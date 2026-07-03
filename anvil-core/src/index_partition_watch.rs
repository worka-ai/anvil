use crate::{
    formats::{FileFamily, Hash32, hash32, watch::WatchRecord},
    partition_fence::{
        OWNERSHIP_EXPIRED, OWNERSHIP_NOT_FOUND, OWNERSHIP_OWNER_MISMATCH, OWNERSHIP_STALE_FENCE,
        OwnershipResource, OwnershipResourceKind, read_ownership_fence,
    },
    storage::Storage,
    watch_log::{DecodedWatchLog, WatchLogHeader, decode_watch_log},
};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

const INDEX_PARTITION_FAMILY: u16 = 7;
const INDEX_PARTITION_RECORD_KIND: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IndexPartitionWatchPayload {
    pub index_id: String,
    pub index_kind: String,
    pub event_type: String,
    pub generation: u64,
    pub source_cursor: u128,
    pub source_manifest_hash: String,
    pub proof_hash: String,
    pub segment_hashes: Vec<String>,
    pub emitted_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPartitionWatchEvent {
    pub cursor: u128,
    pub mutation_id: [u8; 16],
    pub authz_revision: u64,
    pub index_generation: u64,
    pub payload: IndexPartitionWatchPayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexPartitionWatchWriteAuthority {
    pub owner_node_id: String,
    pub fence: u64,
    pub resource_id: String,
}

pub async fn append_index_partition_watch_record(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    cursor: u128,
    mutation_id: [u8; 16],
    authz_revision: u64,
    payload: IndexPartitionWatchPayload,
    authority: IndexPartitionWatchWriteAuthority,
    signing_key: &[u8],
) -> Result<PathBuf> {
    validate_payload(partition_id, &payload)?;
    validate_write_authority(
        storage,
        tenant_id,
        bucket_id,
        partition_id,
        &payload,
        &authority,
        signing_key,
    )
    .await?;
    let path = storage.index_partition_watch_path(
        tenant_id,
        bucket_id,
        &payload.index_id,
        partition_id,
    )?;
    ensure_watch_header(tenant_id, bucket_id, &payload.index_id, partition_id, &path).await?;
    ensure_cursor_is_monotonic(&path, cursor).await?;

    let record = WatchRecord::new(
        cursor,
        INDEX_PARTITION_FAMILY,
        watch_partition_id(tenant_id, bucket_id, &payload.index_id, partition_id),
        mutation_id,
        INDEX_PARTITION_RECORD_KIND,
        authz_revision,
        payload.generation,
        0,
        serde_json::to_vec(&payload)?,
    );
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&record.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub fn index_partition_watch_resource_id(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> String {
    format!(
        "watch/index_partition/tenant/{tenant_id}/bucket/{bucket_id}/index/{index_id}/partition/{partition_id}"
    )
}

pub async fn list_index_partition_watch_events(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    after_cursor: u128,
    limit: usize,
) -> Result<Vec<IndexPartitionWatchEvent>> {
    let path = storage.index_partition_watch_path(tenant_id, bucket_id, index_id, partition_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    let expected_partition = watch_partition_id(tenant_id, bucket_id, index_id, partition_id);
    let mut events = Vec::new();
    for record in decoded.records {
        if record.cursor <= after_cursor {
            continue;
        }
        if record.partition_family != INDEX_PARTITION_FAMILY
            || record.record_kind != INDEX_PARTITION_RECORD_KIND
            || record.partition_id != expected_partition
        {
            continue;
        }
        let payload: IndexPartitionWatchPayload = serde_json::from_slice(&record.payload)?;
        if payload.index_id != index_id {
            return Err(anyhow!("index partition watch payload scope mismatch"));
        }
        validate_payload(partition_id, &payload)?;
        events.push(IndexPartitionWatchEvent {
            cursor: record.cursor,
            mutation_id: record.mutation_id,
            authz_revision: record.authz_revision,
            index_generation: record.index_generation,
            payload,
        });
        if limit > 0 && events.len() >= limit {
            break;
        }
    }
    Ok(events)
}

async fn validate_write_authority(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    payload: &IndexPartitionWatchPayload,
    authority: &IndexPartitionWatchWriteAuthority,
    signing_key: &[u8],
) -> Result<()> {
    if authority.fence == 0 {
        return Err(anyhow!("index partition watch write fence must be nonzero"));
    }
    let expected_resource_id =
        index_partition_watch_resource_id(tenant_id, bucket_id, &payload.index_id, partition_id);
    if authority.resource_id != expected_resource_id {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: index partition watch authority resource mismatch"
        ));
    }
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::WatchPartition,
        resource_id: authority.resource_id.clone(),
    };
    let Some(record) = read_ownership_fence(storage, 0, &resource, signing_key).await? else {
        return Err(anyhow!(
            "{OWNERSHIP_NOT_FOUND}: index partition watch ownership fence is absent"
        ));
    };
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("index partition watch timestamp overflow"))?;
    if !record.is_active_unexpired(now_nanos) {
        return Err(anyhow!(
            "{OWNERSHIP_EXPIRED}: index partition watch ownership fence is not active"
        ));
    }
    if record.owner.principal_id != authority.owner_node_id
        || record.owner.actor_instance_id != authority.owner_node_id
    {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: index partition watch ownership fence owner mismatch"
        ));
    }
    if record.fence != authority.fence {
        return Err(anyhow!(
            "{OWNERSHIP_STALE_FENCE}: index partition watch ownership fence token mismatch"
        ));
    }
    Ok(())
}

pub async fn latest_index_partition_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Result<Option<u128>> {
    let path = storage.index_partition_watch_path(tenant_id, bucket_id, index_id, partition_id)?;
    let decoded = read_watch_or_empty(&path).await?;
    let expected_partition = watch_partition_id(tenant_id, bucket_id, index_id, partition_id);
    Ok(decoded
        .records
        .into_iter()
        .filter(|record| {
            record.partition_family == INDEX_PARTITION_FAMILY
                && record.record_kind == INDEX_PARTITION_RECORD_KIND
                && record.partition_id == expected_partition
        })
        .map(|record| record.cursor)
        .max())
}

async fn ensure_watch_header(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
    path: &PathBuf,
) -> Result<()> {
    if tokio::fs::metadata(path).await.is_ok() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let header = WatchLogHeader {
        tenant_id: tenant_id.to_string(),
        bucket_id: bucket_id.to_string(),
        watch_stream: "index_partition".to_string(),
        partition_family: "index_partition".to_string(),
        partition_id: hex::encode(watch_partition_id(
            tenant_id,
            bucket_id,
            index_id,
            partition_id,
        )),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        codec: "none".to_string(),
    };
    let envelope = crate::formats::BinaryEnvelopeHeader::new(
        FileFamily::WatchSegment,
        0,
        0,
        serde_json::to_vec(&header)?,
    );
    match tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
    {
        Ok(mut file) => {
            file.write_all(&envelope.encode()).await?;
            file.sync_data().await?;
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(err) => Err(err)
            .with_context(|| format!("create index partition watch file {}", path.display())),
    }
}

async fn ensure_cursor_is_monotonic(path: &PathBuf, cursor: u128) -> Result<()> {
    let decoded = read_watch_or_empty(path).await?;
    if let Some(latest) = decoded.records.iter().map(|record| record.cursor).max() {
        if cursor <= latest {
            return Err(anyhow!("index partition watch cursor must be monotonic"));
        }
    }
    Ok(())
}

async fn read_watch_or_empty(path: &PathBuf) -> Result<DecodedWatchLog> {
    match tokio::fs::read(path).await {
        Ok(bytes) => decode_watch_log(&bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(DecodedWatchLog {
            header: WatchLogHeader {
                tenant_id: String::new(),
                bucket_id: String::new(),
                watch_stream: "index_partition".to_string(),
                partition_family: "index_partition".to_string(),
                partition_id: String::new(),
                created_at: String::new(),
                codec: "none".to_string(),
            },
            records: Vec::new(),
        }),
        Err(err) => {
            Err(err).with_context(|| format!("read index partition watch file {}", path.display()))
        }
    }
}

fn validate_payload(partition_id: &str, payload: &IndexPartitionWatchPayload) -> Result<()> {
    require_nonempty(&payload.index_id, "index_id")?;
    require_nonempty(&payload.index_kind, "index_kind")?;
    require_nonempty(&payload.event_type, "event_type")?;
    validate_hex32(partition_id, "partition_id")?;
    if payload.generation == 0 {
        return Err(anyhow!("index partition watch generation must be nonzero"));
    }
    validate_hex32(&payload.source_manifest_hash, "source_manifest_hash")?;
    validate_hex32(&payload.proof_hash, "proof_hash")?;
    if payload.segment_hashes.is_empty() {
        return Err(anyhow!("index partition watch requires segment hashes"));
    }
    for segment_hash in &payload.segment_hashes {
        validate_hex32(segment_hash, "segment_hash")?;
    }
    require_nonempty(&payload.emitted_at, "emitted_at")?;
    Ok(())
}

fn watch_partition_id(
    tenant_id: i64,
    bucket_id: i64,
    index_id: &str,
    partition_id: &str,
) -> Hash32 {
    hash32(
        format!("tenant:{tenant_id}:bucket:{bucket_id}:index:{index_id}:partition:{partition_id}")
            .as_bytes(),
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        AcquireOwnership, ForceExpireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal,
        OwnershipResource, OwnershipResourceKind, acquire_ownership, force_expire_ownership,
    };
    use crate::storage::Storage;
    use tempfile::tempdir;

    #[tokio::test]
    async fn index_partition_watch_appends_lists_and_tracks_latest_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let first_payload = payload(1);
        let first_authority = authority(&storage, 3, 9, &partition_id, &first_payload).await;
        let first_path = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            10,
            [1; 16],
            7,
            first_payload,
            first_authority,
            KEY,
        )
        .await
        .unwrap();
        let second_payload = payload(2);
        let second_authority = authority(&storage, 3, 9, &partition_id, &second_payload).await;
        let second_path = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            12,
            [2; 16],
            8,
            second_payload,
            second_authority,
            KEY,
        )
        .await
        .unwrap();
        assert_eq!(first_path, second_path);
        assert!(first_path.ends_with(format!(
            "_anvil/watch/index/tenant-3/bucket-9/indexes/full-text-alpha/partitions/{partition_id}.anwatch"
        )));

        let events = list_index_partition_watch_events(
            &storage,
            3,
            9,
            "full-text-alpha",
            &partition_id,
            10,
            10,
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].cursor, 12);
        assert_eq!(events[0].index_generation, 2);
        assert_eq!(events[0].authz_revision, 8);
        assert_eq!(events[0].payload.generation, 2);
        assert_eq!(
            latest_index_partition_watch_cursor(&storage, 3, 9, "full-text-alpha", &partition_id)
                .await
                .unwrap(),
            Some(12)
        );
    }

    #[tokio::test]
    async fn index_partition_watch_rejects_non_monotonic_and_invalid_payloads() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let first_payload = payload(1);
        let first_authority = authority(&storage, 3, 9, &partition_id, &first_payload).await;
        append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            10,
            [1; 16],
            7,
            first_payload,
            first_authority,
            KEY,
        )
        .await
        .unwrap();
        let second_payload = payload(2);
        let second_authority = authority(&storage, 3, 9, &partition_id, &second_payload).await;
        assert!(
            append_index_partition_watch_record(
                &storage,
                3,
                9,
                &partition_id,
                10,
                [2; 16],
                7,
                second_payload,
                second_authority,
                KEY,
            )
            .await
            .is_err()
        );
        let mut invalid = payload(3);
        invalid.segment_hashes.clear();
        let invalid_authority = IndexPartitionWatchWriteAuthority {
            owner_node_id: "node-a".to_string(),
            fence: 1,
            resource_id: index_partition_watch_resource_id(3, 9, "full-text-alpha", &partition_id),
        };
        assert!(
            append_index_partition_watch_record(
                &storage,
                3,
                9,
                &partition_id,
                11,
                [3; 16],
                7,
                invalid,
                invalid_authority,
                KEY,
            )
            .await
            .is_err()
        );
        assert!(
            storage
                .index_partition_watch_path(3, 9, "../index", &partition_id)
                .is_err()
        );
    }

    #[tokio::test]
    async fn index_partition_watch_limit_zero_means_unlimited() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        for generation in 1..=3 {
            let next_payload = payload(generation);
            let next_authority = authority(&storage, 3, 9, &partition_id, &next_payload).await;
            append_index_partition_watch_record(
                &storage,
                3,
                9,
                &partition_id,
                generation as u128,
                [generation as u8; 16],
                7,
                next_payload,
                next_authority,
                KEY,
            )
            .await
            .unwrap();
        }
        let events = list_index_partition_watch_events(
            &storage,
            3,
            9,
            "full-text-alpha",
            &partition_id,
            0,
            0,
        )
        .await
        .unwrap();
        assert_eq!(events.len(), 3);
    }

    #[tokio::test]
    async fn index_partition_watch_rejects_stale_or_mismatched_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let partition_id = hex::encode([5; 32]);
        let next_payload = payload(1);
        let valid = authority(&storage, 3, 9, &partition_id, &next_payload).await;
        let stale = IndexPartitionWatchWriteAuthority {
            fence: valid.fence.saturating_add(1),
            ..valid.clone()
        };
        let err = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            1,
            [1; 16],
            7,
            next_payload.clone(),
            stale,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("StaleFence"));

        let wrong_resource = IndexPartitionWatchWriteAuthority {
            resource_id: "watch/index_partition/wrong".to_string(),
            ..valid
        };
        let err = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            1,
            [1; 16],
            7,
            next_payload,
            wrong_resource,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("OwnershipOwnerMismatch"));

        let next_payload = payload(2);
        let stale_after_failover = authority(&storage, 3, 9, &partition_id, &next_payload).await;
        replace_index_partition_watch_owner(&storage, &stale_after_failover).await;
        let err = append_index_partition_watch_record(
            &storage,
            3,
            9,
            &partition_id,
            2,
            [2; 16],
            7,
            next_payload,
            stale_after_failover,
            KEY,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("OwnershipOwnerMismatch"));
    }

    fn payload(generation: u64) -> IndexPartitionWatchPayload {
        IndexPartitionWatchPayload {
            index_id: "full-text-alpha".to_string(),
            index_kind: "full_text".to_string(),
            event_type: "partition_published".to_string(),
            generation,
            source_cursor: 40 + u128::from(generation),
            source_manifest_hash: hex::encode([6; 32]),
            proof_hash: hex::encode([7; 32]),
            segment_hashes: vec![hex::encode([8; 32])],
            emitted_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        }
    }

    const KEY: &[u8] = b"index partition watch signing key";

    async fn authority(
        storage: &Storage,
        tenant_id: i64,
        bucket_id: i64,
        partition_id: &str,
        payload: &IndexPartitionWatchPayload,
    ) -> IndexPartitionWatchWriteAuthority {
        let resource_id = index_partition_watch_resource_id(
            tenant_id,
            bucket_id,
            &payload.index_id,
            partition_id,
        );
        let outcome = acquire_ownership(
            storage,
            AcquireOwnership {
                request_id: format!("test-index-watch-{resource_id}"),
                idempotency_key: format!("test-index-watch-{resource_id}"),
                resource: OwnershipResource {
                    resource_kind: OwnershipResourceKind::WatchPartition,
                    resource_id: resource_id.clone(),
                },
                owner: OwnershipPrincipal {
                    tenant_id: 0,
                    principal_kind: "node".to_string(),
                    principal_id: "node-a".to_string(),
                    actor_instance_id: "node-a".to_string(),
                    display_name: "node-a".to_string(),
                    region: "test-region".to_string(),
                    cell: "default".to_string(),
                },
                now_nanos: chrono::Utc::now().timestamp_nanos_opt().unwrap(),
                ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                    .unwrap()
                    .saturating_mul(1_000_000),
            },
            KEY,
        )
        .await
        .unwrap();
        IndexPartitionWatchWriteAuthority {
            owner_node_id: "node-a".to_string(),
            fence: outcome.record.fence,
            resource_id,
        }
    }

    async fn replace_index_partition_watch_owner(
        storage: &Storage,
        stale_authority: &IndexPartitionWatchWriteAuthority,
    ) {
        let resource = OwnershipResource {
            resource_kind: OwnershipResourceKind::WatchPartition,
            resource_id: stale_authority.resource_id.clone(),
        };
        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        force_expire_ownership(
            storage,
            ForceExpireOwnership {
                request_id: format!("test-index-watch-expire-{}", stale_authority.resource_id),
                idempotency_key: format!("test-index-watch-expire-{}", stale_authority.resource_id),
                resource: resource.clone(),
                admin: OwnershipPrincipal::node("admin-node"),
                reason: "test ownership failover".to_string(),
                now_nanos,
            },
            KEY,
        )
        .await
        .unwrap();
        acquire_ownership(
            storage,
            AcquireOwnership {
                request_id: format!(
                    "test-index-watch-replacement-{}",
                    stale_authority.resource_id
                ),
                idempotency_key: format!(
                    "test-index-watch-replacement-{}",
                    stale_authority.resource_id
                ),
                resource,
                owner: OwnershipPrincipal::node("node-b"),
                now_nanos: now_nanos.saturating_add(1),
                ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                    .unwrap()
                    .saturating_mul(1_000_000),
            },
            KEY,
        )
        .await
        .unwrap();
    }
}
