use crate::{
    error_codes::AnvilErrorCode,
    formats::{JournalFrame, hash32},
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::Sha256;
use std::{fmt, io::ErrorKind, path::Path};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PartitionOwnerStatus {
    Recovering,
    Ready,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionOwnerState {
    pub format_version: u16,
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
    pub recovery_epoch: u64,
    pub status: PartitionOwnerStatus,
    pub recovered_through_sequence: u64,
    pub recovered_manifest_hash: String,
    pub updated_at_nanos: i64,
    pub owner_hash: Option<String>,
    pub owner_signature: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionRecoveryAcquire {
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub recovered_through_sequence: u64,
    pub recovered_manifest_hash: String,
    pub now_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionWritePermit {
    pub partition_family: String,
    pub partition_id: String,
    pub owner_node_id: String,
    pub fence_token: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FenceRejection {
    pub code: AnvilErrorCode,
    pub reason: &'static str,
}

impl fmt::Display for FenceRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.reason)
    }
}

impl std::error::Error for FenceRejection {}

impl PartitionOwnerState {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_owner(&self)?;
        let hash = hash_partition_owner(&self)?;
        let signature = sign_owner_hash(
            signing_key,
            &hash,
            &[
                &self.partition_family,
                &self.partition_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        self.owner_hash = Some(hash);
        self.owner_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_owner(self)?;
        let expected_hash = hash_partition_owner(self)?;
        if self.owner_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("partition owner hash mismatch"));
        }
        let expected_signature = sign_owner_hash(
            signing_key,
            &expected_hash,
            &[
                &self.partition_family,
                &self.partition_id,
                &self.owner_node_id,
                &self.fence_token.to_string(),
            ],
        )?;
        if self.owner_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("partition owner signature mismatch"));
        }
        Ok(())
    }

    pub fn write_permit(&self) -> Result<PartitionWritePermit, FenceRejection> {
        if self.status != PartitionOwnerStatus::Ready {
            return Err(FenceRejection {
                code: AnvilErrorCode::PartitionNotOwned,
                reason: "partition owner has not completed recovery",
            });
        }
        Ok(PartitionWritePermit {
            partition_family: self.partition_family.clone(),
            partition_id: self.partition_id.clone(),
            owner_node_id: self.owner_node_id.clone(),
            fence_token: self.fence_token,
        })
    }
}

pub fn hash_partition_owner(owner: &PartitionOwnerState) -> Result<String> {
    let mut unsigned = owner.clone();
    unsigned.owner_hash = None;
    unsigned.owner_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub async fn acquire_partition_recovery(
    storage: &Storage,
    request: PartitionRecoveryAcquire,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    validate_recovery_acquire(&request)?;
    let existing = read_partition_owner(
        storage,
        &request.partition_family,
        &request.partition_id,
        signing_key,
    )
    .await?;
    let fence_token = existing
        .as_ref()
        .map(|state| state.fence_token.saturating_add(1))
        .unwrap_or(1);
    let recovery_epoch = existing
        .as_ref()
        .map(|state| state.recovery_epoch.saturating_add(1))
        .unwrap_or(1);
    let state = PartitionOwnerState {
        format_version: 1,
        partition_family: request.partition_family,
        partition_id: request.partition_id,
        owner_node_id: request.owner_node_id,
        fence_token,
        recovery_epoch,
        status: PartitionOwnerStatus::Recovering,
        recovered_through_sequence: request.recovered_through_sequence,
        recovered_manifest_hash: request.recovered_manifest_hash,
        updated_at_nanos: request.now_nanos,
        owner_hash: None,
        owner_signature: None,
    }
    .seal(signing_key)?;
    write_partition_owner(storage, &state).await?;
    Ok(state)
}

pub async fn publish_partition_ready(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    owner_node_id: &str,
    fence_token: u64,
    recovered_through_sequence: u64,
    recovered_manifest_hash: &str,
    now_nanos: i64,
    signing_key: &[u8],
) -> Result<PartitionOwnerState> {
    let Some(mut state) =
        read_partition_owner(storage, partition_family, partition_id, signing_key).await?
    else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        }
        .into());
    };
    validate_write_permit_for_state(
        &state,
        &PartitionWritePermit {
            partition_family: partition_family.to_string(),
            partition_id: partition_id.to_string(),
            owner_node_id: owner_node_id.to_string(),
            fence_token,
        },
        false,
    )?;
    validate_hex32(recovered_manifest_hash, "recovered manifest hash")?;
    if now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    state.status = PartitionOwnerStatus::Ready;
    state.recovered_through_sequence = recovered_through_sequence;
    state.recovered_manifest_hash = recovered_manifest_hash.to_string();
    state.updated_at_nanos = now_nanos;
    state = state.seal(signing_key)?;
    write_partition_owner(storage, &state).await?;
    Ok(state)
}

pub async fn validate_partition_write(
    storage: &Storage,
    permit: &PartitionWritePermit,
    signing_key: &[u8],
) -> Result<(), FenceRejection> {
    let owner = read_partition_owner(
        storage,
        &permit.partition_family,
        &permit.partition_id,
        signing_key,
    )
    .await
    .map_err(|_| FenceRejection {
        code: AnvilErrorCode::PartitionNotOwned,
        reason: "partition owner state cannot be read",
    })?;
    let Some(owner) = owner else {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner state is absent",
        });
    };
    validate_write_permit_for_state(&owner, permit, true)
}

pub fn validate_write_permit_for_state(
    owner: &PartitionOwnerState,
    permit: &PartitionWritePermit,
    require_ready: bool,
) -> Result<(), FenceRejection> {
    if owner.partition_family != permit.partition_family
        || owner.partition_id != permit.partition_id
    {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "write permit targets a different partition",
        });
    }
    if require_ready && owner.status != PartitionOwnerStatus::Ready {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "partition owner has not completed recovery",
        });
    }
    if owner.owner_node_id != permit.owner_node_id {
        return Err(FenceRejection {
            code: AnvilErrorCode::PartitionNotOwned,
            reason: "write permit owner is not current",
        });
    }
    if owner.fence_token != permit.fence_token {
        return Err(FenceRejection {
            code: AnvilErrorCode::StaleFenceToken,
            reason: "write permit fence token is stale",
        });
    }
    Ok(())
}

pub async fn read_partition_owner(
    storage: &Storage,
    partition_family: &str,
    partition_id: &str,
    signing_key: &[u8],
) -> Result<Option<PartitionOwnerState>> {
    let path = storage.partition_owner_path(partition_family, partition_id)?;
    let Some(owner) = read_json_optional::<PartitionOwnerState>(&path).await? else {
        return Ok(None);
    };
    owner.verify(signing_key)?;
    if owner.partition_family != partition_family || owner.partition_id != partition_id {
        return Err(anyhow!("partition owner path scope mismatch"));
    }
    Ok(Some(owner))
}

pub fn frames_for_recovered_fence(
    frames: &[JournalFrame],
    manifest_checkpoint_sequence: u64,
    current_fence_token: u64,
) -> Vec<JournalFrame> {
    frames
        .iter()
        .filter(|frame| {
            frame.partition_sequence > manifest_checkpoint_sequence
                && frame.fence_token == current_fence_token
        })
        .cloned()
        .collect()
}

pub fn reject_stale_frames_after_checkpoint(
    frames: &[JournalFrame],
    manifest_checkpoint_sequence: u64,
    current_fence_token: u64,
) -> Result<(), FenceRejection> {
    if frames.iter().any(|frame| {
        frame.partition_sequence > manifest_checkpoint_sequence
            && frame.fence_token != current_fence_token
    }) {
        return Err(FenceRejection {
            code: AnvilErrorCode::StaleFenceToken,
            reason: "journal contains stale-fence records after manifest checkpoint",
        });
    }
    Ok(())
}

async fn write_partition_owner(storage: &Storage, owner: &PartitionOwnerState) -> Result<()> {
    let path = storage.partition_owner_path(&owner.partition_family, &owner.partition_id)?;
    write_json_atomically(&path, owner).await
}

fn validate_recovery_acquire(request: &PartitionRecoveryAcquire) -> Result<()> {
    require_nonempty(&request.partition_family, "partition family")?;
    validate_hex32(&request.partition_id, "partition id")?;
    require_nonempty(&request.owner_node_id, "owner node id")?;
    validate_hex32(&request.recovered_manifest_hash, "recovered manifest hash")?;
    if request.now_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_owner(owner: &PartitionOwnerState) -> Result<()> {
    if owner.format_version != 1 {
        return Err(anyhow!("unsupported partition owner version"));
    }
    require_nonempty(&owner.partition_family, "partition family")?;
    validate_hex32(&owner.partition_id, "partition id")?;
    require_nonempty(&owner.owner_node_id, "owner node id")?;
    validate_hex32(&owner.recovered_manifest_hash, "recovered manifest hash")?;
    if owner.fence_token == 0 || owner.recovery_epoch == 0 {
        return Err(anyhow!("partition owner fence and epoch must be nonzero"));
    }
    if owner.updated_at_nanos < 0 {
        return Err(anyhow!("partition owner timestamp must be nonnegative"));
    }
    Ok(())
}

fn sign_owner_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("partition owner signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"partition_owner");
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
        .with_context(|| format!("write temporary partition owner {}", tmp.display()))?;
    tokio::fs::rename(&tmp, path)
        .await
        .with_context(|| format!("publish partition owner {}", path.display()))?;
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
    use crate::formats::JournalRecordKind;
    use tempfile::tempdir;

    const KEY: &[u8] = b"partition owner signing key";

    #[tokio::test]
    async fn recovery_acquire_blocks_writes_until_owner_ready() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let recovering = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
            .await
            .unwrap();
        assert_eq!(recovering.fence_token, 1);
        assert_eq!(recovering.status, PartitionOwnerStatus::Recovering);

        let permit = PartitionWritePermit {
            partition_family: recovering.partition_family.clone(),
            partition_id: recovering.partition_id.clone(),
            owner_node_id: "node-a".to_string(),
            fence_token: recovering.fence_token,
        };
        let rejected = validate_partition_write(&storage, &permit, KEY)
            .await
            .unwrap_err();
        assert_eq!(rejected.code, AnvilErrorCode::PartitionNotOwned);

        let ready = publish_partition_ready(
            &storage,
            &recovering.partition_family,
            &recovering.partition_id,
            "node-a",
            recovering.fence_token,
            77,
            &hex::encode([9; 32]),
            200,
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(ready.status, PartitionOwnerStatus::Ready);
        assert_eq!(ready.recovered_through_sequence, 77);
        validate_partition_write(&storage, &ready.write_permit().unwrap(), KEY)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn owner_handoff_rejects_stale_fence_token() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
            .await
            .unwrap();
        let first = publish_partition_ready(
            &storage,
            &first.partition_family,
            &first.partition_id,
            "node-a",
            first.fence_token,
            10,
            &hex::encode([3; 32]),
            150,
            KEY,
        )
        .await
        .unwrap();
        let stale_permit = first.write_permit().unwrap();

        let second = acquire_partition_recovery(&storage, acquire("node-b", 300), KEY)
            .await
            .unwrap();
        assert_eq!(second.fence_token, first.fence_token + 1);
        let stale_rejection = validate_partition_write(&storage, &stale_permit, KEY)
            .await
            .unwrap_err();
        assert_eq!(stale_rejection.code, AnvilErrorCode::PartitionNotOwned);

        let second = publish_partition_ready(
            &storage,
            &second.partition_family,
            &second.partition_id,
            "node-b",
            second.fence_token,
            20,
            &hex::encode([4; 32]),
            350,
            KEY,
        )
        .await
        .unwrap();
        validate_partition_write(&storage, &second.write_permit().unwrap(), KEY)
            .await
            .unwrap();

        let mut stale_same_owner = second.write_permit().unwrap();
        stale_same_owner.fence_token -= 1;
        let stale_rejection = validate_partition_write(&storage, &stale_same_owner, KEY)
            .await
            .unwrap_err();
        assert_eq!(stale_rejection.code, AnvilErrorCode::StaleFenceToken);
    }

    #[test]
    fn recovery_replay_keeps_current_fence_after_manifest_checkpoint() {
        let stale_before = frame(9, 1, [0; 32]);
        let current_after = frame(11, 2, stale_before.record_hash);
        let stale_after = frame(12, 1, current_after.record_hash);
        let frames = vec![stale_before, current_after.clone(), stale_after];

        assert_eq!(
            frames_for_recovered_fence(&frames, 10, 2),
            vec![current_after]
        );
        let rejection = reject_stale_frames_after_checkpoint(&frames, 10, 2).unwrap_err();
        assert_eq!(rejection.code, AnvilErrorCode::StaleFenceToken);
        reject_stale_frames_after_checkpoint(&frames[..2], 10, 2).unwrap();
    }

    #[tokio::test]
    async fn partition_owner_state_is_signed_and_path_scoped() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
            .await
            .unwrap();
        let path = storage
            .partition_owner_path(&owner.partition_family, &owner.partition_id)
            .unwrap();
        assert!(path.ends_with(format!(
            "_anvil/control/partition-owners/object_metadata/{}.json",
            owner.partition_id
        )));

        let mut value: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&path).await.unwrap()).unwrap();
        value["fence_token"] = serde_json::json!(99);
        tokio::fs::write(&path, serde_json::to_vec_pretty(&value).unwrap())
            .await
            .unwrap();
        assert!(
            read_partition_owner(&storage, &owner.partition_family, &owner.partition_id, KEY)
                .await
                .is_err()
        );
        assert!(
            storage
                .partition_owner_path("../escape", &owner.partition_id)
                .is_err()
        );
    }

    fn acquire(owner_node_id: &str, now_nanos: i64) -> PartitionRecoveryAcquire {
        PartitionRecoveryAcquire {
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([7; 32]),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos,
        }
    }

    fn frame(sequence: u64, fence_token: u64, previous_hash: [u8; 32]) -> JournalFrame {
        JournalFrame::new(
            JournalRecordKind::ObjectVersion,
            sequence,
            fence_token,
            [sequence as u8; 16],
            [fence_token as u8; 32],
            previous_hash,
            vec![sequence as u8, fence_token as u8],
        )
    }
}
