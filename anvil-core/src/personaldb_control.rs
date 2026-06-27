use crate::formats::hash32;
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbGroupManifest {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub schema_hash: String,
    pub genesis_hash: String,
    pub created_at: String,
    pub created_by: String,
    pub consistency_policy: String,
    pub object_layout_version: u16,
    pub active_membership_epoch: u64,
    pub active_policy_epoch: u64,
    pub current_row_index_generation: u64,
    pub current_projection_generation: u64,
    pub manifest_hash: Option<String>,
    pub manifest_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbSnapshotManifest {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub log_hash: String,
    pub state_hash: String,
    pub schema_hash: String,
    pub snapshot_object_key: String,
    pub snapshot_object_hash: String,
    pub source_segment_start: u64,
    pub source_segment_end: u64,
    pub row_index_generation: u64,
    pub created_at: String,
    pub created_by_node: String,
    pub manifest_hash: Option<String>,
    pub manifest_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbCommitCertificate {
    pub format_version: u16,
    pub tenant_id: String,
    pub database_id: String,
    pub log_index: u64,
    pub previous_log_hash: String,
    pub entry_hash: String,
    pub changeset_payload_hash: String,
    pub verified_envelope_hash: String,
    pub client_log_epoch: u64,
    pub membership_epoch: u64,
    pub policy_epoch: u64,
    pub leader_replica_id: String,
    pub voter_acks_hash: String,
    pub authz_revision: u64,
    pub witness_node_id: String,
    pub witnessed_at: String,
    pub certificate_hash: Option<String>,
    pub witness_signature: Option<String>,
}

impl PersonalDbGroupManifest {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_group_manifest_unsigned(&self)?;
        let hash = hash_group_manifest(&self)?;
        let signature = sign_control_hash(
            signing_key,
            "personaldb_group_manifest",
            &hash,
            &[&self.tenant_id, &self.database_id],
        )?;
        self.manifest_hash = Some(hash);
        self.manifest_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_group_manifest_unsigned(self)?;
        let expected_hash = hash_group_manifest(self)?;
        if self.manifest_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb group manifest hash mismatch"));
        }
        let expected_signature = sign_control_hash(
            signing_key,
            "personaldb_group_manifest",
            &expected_hash,
            &[&self.tenant_id, &self.database_id],
        )?;
        if self.manifest_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb group manifest signature mismatch"));
        }
        Ok(())
    }
}

impl PersonalDbSnapshotManifest {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_snapshot_manifest_unsigned(&self)?;
        let hash = hash_snapshot_manifest(&self)?;
        let signature = sign_control_hash(
            signing_key,
            "personaldb_snapshot_manifest",
            &hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        self.manifest_hash = Some(hash);
        self.manifest_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_snapshot_manifest_unsigned(self)?;
        let expected_hash = hash_snapshot_manifest(self)?;
        if self.manifest_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb snapshot manifest hash mismatch"));
        }
        let expected_signature = sign_control_hash(
            signing_key,
            "personaldb_snapshot_manifest",
            &expected_hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        if self.manifest_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb snapshot manifest signature mismatch"));
        }
        Ok(())
    }
}

impl PersonalDbCommitCertificate {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_commit_certificate_unsigned(&self)?;
        let hash = hash_commit_certificate(&self)?;
        let signature = sign_control_hash(
            signing_key,
            "personaldb_commit_certificate",
            &hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        self.certificate_hash = Some(hash);
        self.witness_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_commit_certificate_unsigned(self)?;
        let expected_hash = hash_commit_certificate(self)?;
        if self.certificate_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("personaldb commit certificate hash mismatch"));
        }
        let expected_signature = sign_control_hash(
            signing_key,
            "personaldb_commit_certificate",
            &expected_hash,
            &[
                &self.tenant_id,
                &self.database_id,
                &self.log_index.to_string(),
            ],
        )?;
        if self.witness_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("personaldb commit certificate signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_group_manifest(manifest: &PersonalDbGroupManifest) -> Result<String> {
    let mut unsigned = manifest.clone();
    unsigned.manifest_hash = None;
    unsigned.manifest_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub fn hash_snapshot_manifest(manifest: &PersonalDbSnapshotManifest) -> Result<String> {
    let mut unsigned = manifest.clone();
    unsigned.manifest_hash = None;
    unsigned.manifest_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub fn hash_commit_certificate(certificate: &PersonalDbCommitCertificate) -> Result<String> {
    let mut unsigned = certificate.clone();
    unsigned.certificate_hash = None;
    unsigned.witness_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

fn validate_group_manifest_unsigned(manifest: &PersonalDbGroupManifest) -> Result<()> {
    if manifest.format_version != 1 {
        return Err(anyhow!("unsupported personaldb group manifest version"));
    }
    if manifest.consistency_policy != "StrictWitnessed" {
        return Err(anyhow!("unsupported personaldb consistency policy"));
    }
    validate_hex32(&manifest.schema_hash, "schema_hash")?;
    validate_hex32(&manifest.genesis_hash, "genesis_hash")?;
    require_nonempty(&manifest.tenant_id, "tenant_id")?;
    require_nonempty(&manifest.database_id, "database_id")?;
    require_nonempty(&manifest.created_by, "created_by")?;
    Ok(())
}

fn validate_snapshot_manifest_unsigned(manifest: &PersonalDbSnapshotManifest) -> Result<()> {
    if manifest.format_version != 1 {
        return Err(anyhow!("unsupported personaldb snapshot manifest version"));
    }
    validate_hex32(&manifest.log_hash, "log_hash")?;
    validate_hex32(&manifest.state_hash, "state_hash")?;
    validate_hex32(&manifest.schema_hash, "schema_hash")?;
    validate_hex32(&manifest.snapshot_object_hash, "snapshot_object_hash")?;
    require_nonempty(&manifest.tenant_id, "tenant_id")?;
    require_nonempty(&manifest.database_id, "database_id")?;
    require_nonempty(&manifest.snapshot_object_key, "snapshot_object_key")?;
    require_nonempty(&manifest.created_by_node, "created_by_node")?;
    if manifest.source_segment_start > manifest.source_segment_end {
        return Err(anyhow!("snapshot source segment range is invalid"));
    }
    Ok(())
}

fn validate_commit_certificate_unsigned(certificate: &PersonalDbCommitCertificate) -> Result<()> {
    if certificate.format_version != 1 {
        return Err(anyhow!("unsupported personaldb commit certificate version"));
    }
    validate_hex32(&certificate.previous_log_hash, "previous_log_hash")?;
    validate_hex32(&certificate.entry_hash, "entry_hash")?;
    validate_hex32(
        &certificate.changeset_payload_hash,
        "changeset_payload_hash",
    )?;
    validate_hex32(
        &certificate.verified_envelope_hash,
        "verified_envelope_hash",
    )?;
    validate_hex32(&certificate.voter_acks_hash, "voter_acks_hash")?;
    require_nonempty(&certificate.tenant_id, "tenant_id")?;
    require_nonempty(&certificate.database_id, "database_id")?;
    require_nonempty(&certificate.leader_replica_id, "leader_replica_id")?;
    require_nonempty(&certificate.witness_node_id, "witness_node_id")?;
    Ok(())
}

fn sign_control_hash(
    signing_key: &[u8],
    domain: &str,
    hash: &str,
    scope_parts: &[&str],
) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("personaldb signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(domain.as_bytes());
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
        return Err(anyhow!("{field} must be 32 bytes encoded as hex"));
    }
    Ok(())
}

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &[u8] = b"personaldb control signing key";

    #[test]
    fn group_manifest_seal_verify_and_tamper_reject() {
        let manifest = sample_group_manifest().seal(KEY).unwrap();
        manifest.verify(KEY).unwrap();
        assert_eq!(manifest.manifest_hash.as_deref().unwrap().len(), 64);
        assert!(!manifest.manifest_signature.as_deref().unwrap().is_empty());

        let mut tampered = manifest;
        tampered.active_policy_epoch += 1;
        assert!(tampered.verify(KEY).is_err());
    }

    #[test]
    fn snapshot_manifest_seal_verify_and_tamper_reject() {
        let manifest = sample_snapshot_manifest().seal(KEY).unwrap();
        manifest.verify(KEY).unwrap();
        let mut tampered = manifest;
        tampered.snapshot_object_hash = hex::encode([7; 32]);
        assert!(tampered.verify(KEY).is_err());
    }

    #[test]
    fn commit_certificate_seal_verify_and_tamper_reject() {
        let certificate = sample_commit_certificate().seal(KEY).unwrap();
        certificate.verify(KEY).unwrap();
        assert_eq!(certificate.certificate_hash.as_deref().unwrap().len(), 64);

        let mut tampered = certificate;
        tampered.authz_revision += 1;
        assert!(tampered.verify(KEY).is_err());
    }

    #[test]
    fn group_manifest_rejects_unsupported_policy() {
        let mut manifest = sample_group_manifest();
        manifest.consistency_policy = "EventuallyAccepted".to_string();
        assert!(manifest.seal(KEY).is_err());
    }

    fn sample_group_manifest() -> PersonalDbGroupManifest {
        PersonalDbGroupManifest {
            format_version: 1,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            schema_hash: hex::encode([1; 32]),
            genesis_hash: hex::encode([2; 32]),
            created_at: "2026-06-20T00:00:00.000000000Z".to_string(),
            created_by: "principal".to_string(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 1,
            active_policy_epoch: 1,
            current_row_index_generation: 0,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        }
    }

    fn sample_snapshot_manifest() -> PersonalDbSnapshotManifest {
        PersonalDbSnapshotManifest {
            format_version: 1,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            log_index: 1000,
            log_hash: hex::encode([1; 32]),
            state_hash: hex::encode([2; 32]),
            schema_hash: hex::encode([3; 32]),
            snapshot_object_key: "_anvil/personaldb/tenants/tenant/groups/db/snapshots/objects/00000000000000001000-state.sqlite.zst".to_string(),
            snapshot_object_hash: hex::encode([4; 32]),
            source_segment_start: 1,
            source_segment_end: 1000,
            row_index_generation: 12,
            created_at: "2026-06-20T00:00:00.000000000Z".to_string(),
            created_by_node: "node".to_string(),
            manifest_hash: None,
            manifest_signature: None,
        }
    }

    fn sample_commit_certificate() -> PersonalDbCommitCertificate {
        PersonalDbCommitCertificate {
            format_version: 1,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            log_index: 1,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode([2; 32]),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 1,
            policy_epoch: 1,
            leader_replica_id: "replica".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 1,
            witness_node_id: "node".to_string(),
            witnessed_at: "2026-06-20T00:00:00.000000000Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
