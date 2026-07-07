use crate::{
    core_store::{
        CompareAndSwapRef, CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
        WriteLogicalFileRequest,
    },
    formats::hash32,
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;
const DERIVED_INDEX_PROOF_REF_PREFIX: &str = "derived_index_proof:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
    write_derived_index_proof_ref(
        storage,
        &versioned_proof_ref_name(
            &sealed.index_id,
            sealed.generation,
            sealed.proof_hash.as_deref().expect("sealed proof has hash"),
        )?,
        &sealed,
        true,
    )
    .await?;
    write_derived_index_proof_ref(
        storage,
        &head_proof_ref_name(&sealed.index_id)?,
        &sealed,
        false,
    )
    .await?;
    Ok(sealed)
}

pub async fn read_latest_derived_index_proof(
    storage: &Storage,
    index_id: &str,
    signing_key: &[u8],
) -> Result<Option<DerivedIndexProof>> {
    let Some(proof) =
        read_derived_index_proof_ref(storage, &head_proof_ref_name(index_id)?).await?
    else {
        return Ok(None);
    };
    proof.verify(signing_key)?;
    if proof.index_id != index_id {
        return Err(anyhow!("derived index proof ref scope mismatch"));
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
        || value.contains(':')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        return Err(anyhow!("{field} is not a safe path component"));
    }
    Ok(())
}

async fn write_derived_index_proof_ref(
    storage: &Storage,
    ref_name: &str,
    proof: &DerivedIndexProof,
    require_absent: bool,
) -> Result<()> {
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "derived_index".to_string(),
            generation: proof.generation,
            logical_file_id: ref_name.to_string(),
            source: serde_json::to_vec_pretty(proof)?,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "derived-index-proof:{}:{}",
                proof.index_id, proof.generation
            ),
            region_id: "local".to_string(),
        })
        .await?;
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.to_string(),
            expected_generation: None,
            expected_target: None,
            require_absent,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await?;
    Ok(())
}

async fn read_derived_index_proof_ref(
    storage: &Storage,
    ref_name: &str,
) -> Result<Option<DerivedIndexProof>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(ref_name).await? else {
        return Ok(None);
    };
    let object_ref = decode_core_object_ref_target(&ref_value.target)?;
    let bytes = store.get_blob(GetBlob { object_ref }).await?;
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn head_proof_ref_name(index_id: &str) -> Result<String> {
    require_safe_component(index_id, "index_id")?;
    Ok(format!(
        "{DERIVED_INDEX_PROOF_REF_PREFIX}index:{index_id}:head"
    ))
}

fn versioned_proof_ref_name(index_id: &str, generation: u64, proof_hash: &str) -> Result<String> {
    require_safe_component(index_id, "index_id")?;
    if generation == 0 {
        return Err(anyhow!("derived index proof generation must be nonzero"));
    }
    validate_hex32(proof_hash, "proof_hash")?;
    Ok(format!(
        "{DERIVED_INDEX_PROOF_REF_PREFIX}index:{index_id}:generation:{generation:020}:hash:{proof_hash}"
    ))
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core_store::PutBlob;
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
        let store = CoreStore::new(storage.clone()).await.unwrap();
        assert!(
            store
                .read_ref(&versioned_proof_ref_name("full-text-alpha", 7, proof_hash).unwrap())
                .await
                .unwrap()
                .is_some()
        );
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
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let head = store
            .read_ref(&head_proof_ref_name("full-text-alpha").unwrap())
            .await
            .unwrap()
            .unwrap();
        let object_ref = decode_core_object_ref_target(&head.target).unwrap();
        let mut value: serde_json::Value =
            serde_json::from_slice(&store.get_blob(GetBlob { object_ref }).await.unwrap()).unwrap();
        value["source_cursor"] = serde_json::json!(999);
        let tampered = store
            .put_blob(PutBlob {
                logical_name: "derived-index-proof-tamper".to_string(),
                bytes: serde_json::to_vec_pretty(&value).unwrap(),
                boundary_values: Vec::new(),
                region_id: "local".to_string(),
                mutation_id: "derived-index-proof-tamper".to_string(),
            })
            .await
            .unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: head_proof_ref_name("full-text-alpha").unwrap(),
                expected_generation: Some(head.generation),
                expected_target: Some(head.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&tampered).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert!(
            read_latest_derived_index_proof(&storage, "full-text-alpha", KEY)
                .await
                .is_err()
        );
        assert!(head_proof_ref_name("../escape").is_err());
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
