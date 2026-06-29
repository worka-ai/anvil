use crate::{formats::hash32, storage::Storage};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::io::ErrorKind;
use std::path::Path;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedIndexProof {
    pub format_version: u16,
    pub index_id: String,
    pub index_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub source_watch_stream_id: String,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub segment_hashes: Vec<String>,
    pub built_by_node: String,
    pub built_at_nanos: i64,
    pub proof_hash: Option<String>,
    pub proof_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DerivedIndexProofWrite {
    pub index_id: String,
    pub index_kind: String,
    pub partition_family: String,
    pub partition_id: String,
    pub source_watch_stream_id: String,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub segment_hashes: Vec<String>,
    pub built_by_node: String,
    pub built_at_nanos: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivedIndexValidity {
    Valid,
    RebuildRequired,
}

impl DerivedIndexProof {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_proof(&self)?;
        let hash = hash_derived_index_proof(&self)?;
        let signature = sign_proof_hash(
            signing_key,
            &hash,
            &[
                &self.index_id,
                &self.partition_id,
                &self.source_manifest_hash,
                &self.generation.to_string(),
            ],
        )?;
        self.proof_hash = Some(hash);
        self.proof_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_proof(self)?;
        let expected_hash = hash_derived_index_proof(self)?;
        if self.proof_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("derived index proof hash mismatch"));
        }
        let expected_signature = sign_proof_hash(
            signing_key,
            &expected_hash,
            &[
                &self.index_id,
                &self.partition_id,
                &self.source_manifest_hash,
                &self.generation.to_string(),
            ],
        )?;
        if self.proof_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("derived index proof signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_derived_index_proof(proof: &DerivedIndexProof) -> Result<String> {
    let mut unsigned = proof.clone();
    unsigned.proof_hash = None;
    unsigned.proof_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn write_derived_index_proof(
    storage: &Storage,
    proof: DerivedIndexProofWrite,
    signing_key: &[u8],
) -> Result<DerivedIndexProof> {
    validate_write(&proof)?;
    let sealed = DerivedIndexProof {
        format_version: 1,
        index_id: proof.index_id,
        index_kind: proof.index_kind,
        partition_family: proof.partition_family,
        partition_id: proof.partition_id,
        source_watch_stream_id: proof.source_watch_stream_id,
        source_cursor: proof.source_cursor,
        source_manifest_hash: proof.source_manifest_hash,
        generation: proof.generation,
        segment_hashes: proof.segment_hashes,
        built_by_node: proof.built_by_node,
        built_at_nanos: proof.built_at_nanos,
        proof_hash: None,
        proof_signature: None,
    }
    .seal(signing_key)?;
    let versioned_path = storage.derived_index_proof_path(
        &sealed.index_id,
        sealed.generation,
        sealed.proof_hash.as_deref().expect("sealed proof has hash"),
    )?;
    write_json_atomically(&versioned_path, &sealed).await?;
    let head_path = storage.derived_index_proof_head_path(&sealed.index_id)?;
    write_json_atomically(&head_path, &sealed).await?;
    Ok(sealed)
}

pub async fn read_latest_derived_index_proof(
    storage: &Storage,
    index_id: &str,
    signing_key: &[u8],
) -> Result<Option<DerivedIndexProof>> {
    let path = storage.derived_index_proof_head_path(index_id)?;
    let Some(proof) = read_json_optional::<DerivedIndexProof>(&path).await? else {
        return Ok(None);
    };
    proof.verify(signing_key)?;
    if proof.index_id != index_id {
        return Err(anyhow!("derived index proof path scope mismatch"));
    }
    Ok(Some(proof))
}

pub fn validate_derived_index_source(
    proof: &DerivedIndexProof,
    required_source_cursor: u128,
    expected_source_manifest_hash: &str,
    min_generation: u64,
    signing_key: &[u8],
) -> Result<DerivedIndexValidity> {
    proof.verify(signing_key)?;
    validate_hex32(
        expected_source_manifest_hash,
        "expected_source_manifest_hash",
    )?;
    if proof.source_manifest_hash != expected_source_manifest_hash
        || proof.source_cursor < required_source_cursor
        || proof.generation < min_generation
    {
        return Ok(DerivedIndexValidity::RebuildRequired);
    }
    Ok(DerivedIndexValidity::Valid)
}

fn validate_write(proof: &DerivedIndexProofWrite) -> Result<()> {
    let unsigned = DerivedIndexProof {
        format_version: 1,
        index_id: proof.index_id.clone(),
        index_kind: proof.index_kind.clone(),
        partition_family: proof.partition_family.clone(),
        partition_id: proof.partition_id.clone(),
        source_watch_stream_id: proof.source_watch_stream_id.clone(),
        source_cursor: proof.source_cursor,
        source_manifest_hash: proof.source_manifest_hash.clone(),
        generation: proof.generation,
        segment_hashes: proof.segment_hashes.clone(),
        built_by_node: proof.built_by_node.clone(),
        built_at_nanos: proof.built_at_nanos,
        proof_hash: None,
        proof_signature: None,
    };
    validate_unsigned_proof(&unsigned)
}

fn validate_unsigned_proof(proof: &DerivedIndexProof) -> Result<()> {
    if proof.format_version != 1 {
        return Err(anyhow!("unsupported derived index proof version"));
    }
    require_safe_component(&proof.index_id, "index_id")?;
    require_safe_component(&proof.index_kind, "index_kind")?;
    require_nonempty(&proof.partition_family, "partition_family")?;
    validate_hex32(&proof.partition_id, "partition_id")?;
    require_safe_component(&proof.source_watch_stream_id, "source_watch_stream_id")?;
    validate_hex32(&proof.source_manifest_hash, "source_manifest_hash")?;
    if proof.generation == 0 {
        return Err(anyhow!("derived index proof generation must be nonzero"));
    }
    if proof.segment_hashes.is_empty() {
        return Err(anyhow!("derived index proof must include segment hashes"));
    }
    for segment_hash in &proof.segment_hashes {
        validate_hex32(segment_hash, "segment_hash")?;
    }
    require_nonempty(&proof.built_by_node, "built_by_node")?;
    if proof.built_at_nanos < 0 {
        return Err(anyhow!("derived index proof timestamp must be nonnegative"));
    }
    Ok(())
}

fn sign_proof_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("derived index proof signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"derived_index_proof");
    mac.update(b"\0");
    mac.update(hash.as_bytes());
    for part in scope_parts {
        mac.update(b"\0");
        mac.update(part.as_bytes());
    }
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

async fn write_json_atomically(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = path.with_extension(format!("json.tmp-{}", uuid::Uuid::new_v4().simple()));
    tokio::fs::write(&tmp, serde_json::to_vec_pretty(value)?)
        .await
        .with_context(|| format!("write temporary derived index proof {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish derived index proof {}", path.display()))?;
    Ok(())
}

async fn read_json_optional<T>(path: &Path) -> Result<Option<T>>
where
    T: DeserializeOwned,
{
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err).with_context(|| format!("read {}", path.display())),
    };
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    require_nonempty(value, field)?;
    if value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    const KEY: &[u8] = b"derived index proof signing key";

    #[tokio::test]
    async fn derived_index_proof_writes_version_and_head() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let proof = write_derived_index_proof(&storage, proof(7, 42, hex::encode([8; 32])), KEY)
            .await
            .unwrap();
        assert_eq!(proof.generation, 7);
        assert_eq!(proof.source_cursor, 42);
        let proof_hash = proof.proof_hash.as_deref().unwrap();
        let versioned = storage
            .derived_index_proof_path("full-text-alpha", 7, proof_hash)
            .unwrap();
        assert!(versioned.ends_with(format!(
            "_anvil/index/proofs/full-text-alpha/generation-00000000000000000007-{proof_hash}.json"
        )));
        assert_eq!(
            read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
                .await
                .unwrap()
                .unwrap(),
            proof
        );
    }

    #[tokio::test]
    async fn derived_index_source_validation_requires_cursor_manifest_and_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let manifest_hash = hex::encode([8; 32]);
        let proof = write_derived_index_proof(&storage, proof(7, 42, manifest_hash.clone()), KEY)
            .await
            .unwrap();
        assert_eq!(
            validate_derived_index_source(&proof, 42, &manifest_hash, 7, KEY).unwrap(),
            DerivedIndexValidity::Valid
        );
        assert_eq!(
            validate_derived_index_source(&proof, 43, &manifest_hash, 7, KEY).unwrap(),
            DerivedIndexValidity::RebuildRequired
        );
        assert_eq!(
            validate_derived_index_source(&proof, 42, &hex::encode([9; 32]), 7, KEY).unwrap(),
            DerivedIndexValidity::RebuildRequired
        );
        assert_eq!(
            validate_derived_index_source(&proof, 42, &manifest_hash, 8, KEY).unwrap(),
            DerivedIndexValidity::RebuildRequired
        );
    }

    #[tokio::test]
    async fn derived_index_proof_rejects_tamper_and_unsafe_inputs() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        write_derived_index_proof(&storage, proof(7, 42, hex::encode([8; 32])), KEY)
            .await
            .unwrap();
        let stored = read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
            .await
            .unwrap()
            .unwrap();
        let path = storage
            .derived_index_proof_head_path("full-text-alpha")
            .unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["source_cursor"] = serde_json::json!(999);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
                .await
                .is_err()
        );
        assert!(storage.derived_index_proof_head_path("../escape").is_err());
        let mut invalid = proof(7, 42, hex::encode([8; 32]));
        invalid.segment_hashes = Vec::new();
        assert!(
            write_derived_index_proof(&storage, invalid, KEY)
                .await
                .is_err()
        );
        assert!(stored.verify(KEY).is_ok());
    }

    fn proof(
        generation: u64,
        source_cursor: u128,
        source_manifest_hash: String,
    ) -> DerivedIndexProofWrite {
        DerivedIndexProofWrite {
            index_id: "full-text-alpha".to_string(),
            index_kind: "full_text".to_string(),
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([2; 32]),
            source_watch_stream_id: "object-prefix".to_string(),
            source_cursor,
            source_manifest_hash,
            generation,
            segment_hashes: vec![hex::encode([3; 32]), hex::encode([4; 32])],
            built_by_node: "node-a".to_string(),
            built_at_nanos: 1000,
        }
    }
}
