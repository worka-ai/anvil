use crate::formats::{Hash32, hash32};
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_CHANGESET_SIZE_BYTES: usize = 16 * 1024 * 1024;
pub const HARD_MAX_CHANGESET_SIZE_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SubmitPersonalDbChangeset {
    pub tenant_id: i64,
    pub database_id: String,
    pub principal: String,
    pub session_token: String,
    pub request_id: String,
    pub idempotency_key: String,
    pub base_log_index: u64,
    pub base_log_hash: String,
    pub client_log_epoch: u64,
    pub membership_epoch: u64,
    pub policy_epoch: u64,
    pub leader_replica_id: String,
    pub voter_acks: Vec<PersonalDbVoterAck>,
    pub changeset_payload_hash: String,
    pub changeset_bytes: Vec<u8>,
    pub client_debug_metadata: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PersonalDbVoterAck {
    pub replica_id: String,
    pub log_index: u64,
    pub log_hash: String,
    pub signature: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedPersonalDbChangeset {
    pub request: SubmitPersonalDbChangeset,
    pub changeset_payload_hash: Hash32,
    pub voter_acks_hash: Hash32,
}

pub fn validate_submit_personaldb_changeset(
    mut request: SubmitPersonalDbChangeset,
    configured_max_changeset_size: usize,
) -> Result<ValidatedPersonalDbChangeset> {
    validate_configured_limit(configured_max_changeset_size)?;
    require_nonempty(&request.database_id, "database_id")?;
    require_nonempty(&request.principal, "principal")?;
    require_nonempty(&request.session_token, "session_token")?;
    require_nonempty(&request.request_id, "request_id")?;
    require_nonempty(&request.idempotency_key, "idempotency_key")?;
    require_nonempty(&request.leader_replica_id, "leader_replica_id")?;
    validate_hex32(&request.base_log_hash, "base_log_hash")?;
    if request.client_log_epoch == 0 || request.membership_epoch == 0 || request.policy_epoch == 0 {
        return Err(anyhow!("personaldb submit epochs must be nonzero"));
    }
    if request.changeset_bytes.is_empty() {
        return Err(anyhow!("personaldb changeset must not be empty"));
    }
    if request.changeset_bytes.len() > configured_max_changeset_size {
        return Err(anyhow!(
            "personaldb changeset exceeds configured maximum size"
        ));
    }
    let actual_payload_hash = hash32(&request.changeset_bytes);
    if request.changeset_payload_hash != hex::encode(actual_payload_hash) {
        return Err(anyhow!("personaldb changeset payload hash mismatch"));
    }
    canonicalize_voter_acks(&mut request.voter_acks);
    validate_voter_acks(&request.voter_acks)?;
    let voter_acks_hash = hash32(&serde_json::to_vec(&request.voter_acks)?);
    Ok(ValidatedPersonalDbChangeset {
        request,
        changeset_payload_hash: actual_payload_hash,
        voter_acks_hash,
    })
}

pub fn default_max_changeset_size() -> usize {
    DEFAULT_MAX_CHANGESET_SIZE_BYTES
}

pub fn validate_configured_limit(limit: usize) -> Result<()> {
    if limit == 0 {
        return Err(anyhow!("personaldb changeset size limit must be nonzero"));
    }
    if limit > HARD_MAX_CHANGESET_SIZE_BYTES {
        return Err(anyhow!(
            "personaldb changeset size limit exceeds hard maximum"
        ));
    }
    Ok(())
}

fn canonicalize_voter_acks(acks: &mut Vec<PersonalDbVoterAck>) {
    acks.sort();
}

fn validate_voter_acks(acks: &[PersonalDbVoterAck]) -> Result<()> {
    if acks.is_empty() {
        return Err(anyhow!(
            "personaldb submit must include voter acknowledgements"
        ));
    }
    for pair in acks.windows(2) {
        if pair[0].replica_id == pair[1].replica_id {
            return Err(anyhow!("duplicate personaldb voter acknowledgement"));
        }
    }
    for ack in acks {
        require_nonempty(&ack.replica_id, "voter_ack.replica_id")?;
        validate_hex32(&ack.log_hash, "voter_ack.log_hash")?;
        require_nonempty(&ack.signature, "voter_ack.signature")?;
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
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

    #[test]
    fn submit_changeset_validation_accepts_canonical_request() {
        let request = sample_request();
        let validated =
            validate_submit_personaldb_changeset(request, DEFAULT_MAX_CHANGESET_SIZE_BYTES)
                .unwrap();
        assert_eq!(
            validated.changeset_payload_hash,
            hash32(b"sqlite changeset bytes")
        );
        assert_eq!(validated.request.voter_acks[0].replica_id, "replica-a");
        assert_eq!(validated.request.voter_acks[1].replica_id, "replica-b");
        assert_ne!(validated.voter_acks_hash, [0; 32]);
    }

    #[test]
    fn submit_changeset_validation_rejects_hash_mismatch_and_size_excess() {
        let mut bad_hash = sample_request();
        bad_hash.changeset_payload_hash = hex::encode([9; 32]);
        assert!(
            validate_submit_personaldb_changeset(bad_hash, DEFAULT_MAX_CHANGESET_SIZE_BYTES)
                .is_err()
        );

        let too_large = sample_request();
        assert!(validate_submit_personaldb_changeset(too_large, 4).is_err());
        assert!(validate_configured_limit(HARD_MAX_CHANGESET_SIZE_BYTES + 1).is_err());
    }

    #[test]
    fn submit_changeset_validation_rejects_empty_fields_and_duplicate_acks() {
        let mut missing = sample_request();
        missing.principal.clear();
        assert!(
            validate_submit_personaldb_changeset(missing, DEFAULT_MAX_CHANGESET_SIZE_BYTES)
                .is_err()
        );

        let mut duplicate = sample_request();
        duplicate.voter_acks[1].replica_id = duplicate.voter_acks[0].replica_id.clone();
        assert!(
            validate_submit_personaldb_changeset(duplicate, DEFAULT_MAX_CHANGESET_SIZE_BYTES)
                .is_err()
        );
    }

    #[test]
    fn voter_ack_hash_is_stable_across_input_order() {
        let left = sample_request();
        let mut right = sample_request();
        right.voter_acks.reverse();
        let left =
            validate_submit_personaldb_changeset(left, DEFAULT_MAX_CHANGESET_SIZE_BYTES).unwrap();
        let right =
            validate_submit_personaldb_changeset(right, DEFAULT_MAX_CHANGESET_SIZE_BYTES).unwrap();
        assert_eq!(left.voter_acks_hash, right.voter_acks_hash);
        assert_eq!(left.request.voter_acks, right.request.voter_acks);
    }

    fn sample_request() -> SubmitPersonalDbChangeset {
        SubmitPersonalDbChangeset {
            tenant_id: 7,
            database_id: "db-alpha".to_string(),
            principal: "principal-a".to_string(),
            session_token: "session-token".to_string(),
            request_id: "request-1".to_string(),
            idempotency_key: "idem-1".to_string(),
            base_log_index: 41,
            base_log_hash: hex::encode([1; 32]),
            client_log_epoch: 2,
            membership_epoch: 3,
            policy_epoch: 4,
            leader_replica_id: "leader-a".to_string(),
            voter_acks: vec![
                PersonalDbVoterAck {
                    replica_id: "replica-b".to_string(),
                    log_index: 42,
                    log_hash: hex::encode([2; 32]),
                    signature: "sig-b".to_string(),
                },
                PersonalDbVoterAck {
                    replica_id: "replica-a".to_string(),
                    log_index: 42,
                    log_hash: hex::encode([2; 32]),
                    signature: "sig-a".to_string(),
                },
            ],
            changeset_payload_hash: hex::encode(hash32(b"sqlite changeset bytes")),
            changeset_bytes: b"sqlite changeset bytes".to_vec(),
            client_debug_metadata: Some(serde_json::json!({"trace": "client-supplied"})),
        }
    }
}
