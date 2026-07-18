use crate::{
    core_store::{
        CF_MATERIALISATION, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
        TABLE_MATERIALISATION_CURSOR_ROW, TABLE_WATCH_CHECKPOINT_ROW,
        commit_coremeta_batch_for_storage, core_meta_committed_row_common, core_meta_root_key_hash,
        core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
    partition_fence::{
        OWNERSHIP_EXPIRED, OWNERSHIP_NOT_FOUND, OWNERSHIP_OWNER_MISMATCH, OWNERSHIP_STALE_FENCE,
        OwnershipResource, OwnershipResourceKind, read_ownership_fence,
    },
    storage::Storage,
};
use anyhow::{Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use prost::Message;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WatchCheckpoint {
    pub format_version: u16,
    pub watch_stream_id: String,
    pub partition_family: String,
    pub partition_id: String,
    pub consumer_id: String,
    pub cursor: u128,
    pub source_cursor_high: u128,
    pub lag_record_count_hint: u64,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub updated_by_node: String,
    pub updated_at_nanos: i64,
    pub checkpoint_hash: Option<String>,
    pub checkpoint_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct WatchCheckpointProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    watch_stream_id: String,
    #[prost(string, tag = "3")]
    partition_family: String,
    #[prost(string, tag = "4")]
    partition_id: String,
    #[prost(string, tag = "5")]
    consumer_id: String,
    #[prost(string, tag = "6")]
    cursor: String,
    #[prost(string, tag = "7")]
    source_cursor_high: String,
    #[prost(uint64, tag = "8")]
    lag_record_count_hint: u64,
    #[prost(string, tag = "9")]
    source_manifest_hash: String,
    #[prost(uint64, tag = "10")]
    generation: u64,
    #[prost(string, tag = "11")]
    updated_by_node: String,
    #[prost(int64, tag = "12")]
    updated_at_nanos: i64,
    #[prost(string, optional, tag = "13")]
    checkpoint_hash: Option<String>,
    #[prost(string, optional, tag = "14")]
    checkpoint_signature: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct WatchCheckpointRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    checkpoint_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchCheckpointUpdate {
    pub watch_stream_id: String,
    pub partition_family: String,
    pub partition_id: String,
    pub consumer_id: String,
    pub cursor: u128,
    pub source_cursor_high: u128,
    pub lag_record_count_hint: u64,
    pub source_manifest_hash: String,
    pub generation: u64,
    pub updated_by_node: String,
    pub updated_at_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchCheckpointWriteAuthority {
    pub owner_node_id: String,
    pub fence: u64,
    pub resource_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchCheckpointLagRecord {
    pub watch_stream_id: String,
    pub partition_family: String,
    pub partition_id: String,
    pub consumer_id: String,
    pub applied_cursor: u128,
    pub source_cursor_high: u128,
    pub lag_record_count_hint: u64,
    pub checkpoint_generation: u64,
    pub checkpoint_hash: String,
    pub updated_at_nanos: i64,
}

#[derive(Clone, PartialEq, Message)]
struct WatchCheckpointLagRecordProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    watch_stream_id: String,
    #[prost(string, tag = "4")]
    partition_family: String,
    #[prost(string, tag = "5")]
    partition_id: String,
    #[prost(string, tag = "6")]
    consumer_id: String,
    #[prost(string, tag = "7")]
    applied_cursor: String,
    #[prost(string, tag = "8")]
    source_cursor_high: String,
    #[prost(uint64, tag = "9")]
    lag_record_count_hint: u64,
    #[prost(uint64, tag = "10")]
    checkpoint_generation: u64,
    #[prost(string, tag = "11")]
    checkpoint_hash: String,
    #[prost(int64, tag = "12")]
    updated_at_nanos: i64,
}

impl WatchCheckpoint {
    pub fn seal(mut self, signing_key: &[u8]) -> Result<Self> {
        validate_unsigned_checkpoint(&self)?;
        let hash = hash_watch_checkpoint(&self)?;
        let signature = sign_checkpoint_hash(
            signing_key,
            &hash,
            &[
                &self.watch_stream_id,
                &self.partition_id,
                &self.consumer_id,
                &self.cursor.to_string(),
            ],
        )?;
        self.checkpoint_hash = Some(hash);
        self.checkpoint_signature = Some(signature);
        Ok(self)
    }

    pub fn verify(&self, signing_key: &[u8]) -> Result<()> {
        validate_unsigned_checkpoint(self)?;
        let expected_hash = hash_watch_checkpoint(self)?;
        if self.checkpoint_hash.as_deref() != Some(expected_hash.as_str()) {
            return Err(anyhow!("watch checkpoint hash mismatch"));
        }
        let expected_signature = sign_checkpoint_hash(
            signing_key,
            &expected_hash,
            &[
                &self.watch_stream_id,
                &self.partition_id,
                &self.consumer_id,
                &self.cursor.to_string(),
            ],
        )?;
        if self.checkpoint_signature.as_deref() != Some(expected_signature.as_str()) {
            return Err(anyhow!("watch checkpoint signature mismatch"));
        }
        Ok(())
    }
}

pub fn hash_watch_checkpoint(checkpoint: &WatchCheckpoint) -> Result<String> {
    let mut unsigned = checkpoint.clone();
    unsigned.checkpoint_hash = None;
    unsigned.checkpoint_signature = None;
    Ok(hex::encode(hash32(&encode_watch_checkpoint(&unsigned))))
}

pub async fn checkpoint_watch_consumer(
    storage: &Storage,
    update: WatchCheckpointUpdate,
    authority: WatchCheckpointWriteAuthority,
    signing_key: &[u8],
) -> Result<WatchCheckpoint> {
    validate_update(&update)?;
    validate_write_authority(storage, &update, &authority, signing_key).await?;
    let existing = read_watch_checkpoint(
        storage,
        &update.watch_stream_id,
        &update.consumer_id,
        signing_key,
    )
    .await?;
    if let Some(existing) = existing.as_ref() {
        if existing.cursor > update.cursor {
            return Err(anyhow!("watch checkpoint cursor cannot move backwards"));
        }
        if existing.source_cursor_high > update.source_cursor_high {
            return Err(anyhow!(
                "watch checkpoint source cursor high cannot move backwards"
            ));
        }
        if existing.generation > update.generation {
            return Err(anyhow!("watch checkpoint generation cannot move backwards"));
        }
        if existing.partition_family != update.partition_family
            || existing.partition_id != update.partition_id
        {
            return Err(anyhow!("watch checkpoint stream partition cannot change"));
        }
        if existing.cursor == update.cursor
            && existing.source_manifest_hash != update.source_manifest_hash
        {
            return Err(anyhow!(
                "ControlStreamDivergence: watch checkpoint digest differs for already applied cursor"
            ));
        }
    }

    let checkpoint = WatchCheckpoint {
        format_version: 1,
        watch_stream_id: update.watch_stream_id,
        partition_family: update.partition_family,
        partition_id: update.partition_id,
        consumer_id: update.consumer_id,
        cursor: update.cursor,
        source_cursor_high: update.source_cursor_high,
        lag_record_count_hint: update.lag_record_count_hint,
        source_manifest_hash: update.source_manifest_hash,
        generation: update.generation,
        updated_by_node: update.updated_by_node,
        updated_at_nanos: update.updated_at_nanos,
        checkpoint_hash: None,
        checkpoint_signature: None,
    }
    .seal(signing_key)?;
    write_watch_checkpoint(storage, &checkpoint).await?;
    write_watch_checkpoint_lag_record(storage, &checkpoint).await?;
    Ok(checkpoint)
}

pub fn watch_checkpoint_resource_id(
    watch_stream_id: &str,
    partition_id: &str,
    consumer_id: &str,
) -> String {
    format!("watch/{watch_stream_id}/partition/{partition_id}/consumer/{consumer_id}")
}

pub async fn read_watch_checkpoint(
    storage: &Storage,
    watch_stream_id: &str,
    consumer_id: &str,
    signing_key: &[u8],
) -> Result<Option<WatchCheckpoint>> {
    require_safe_component(watch_stream_id, "watch_stream_id")?;
    require_safe_component(consumer_id, "consumer_id")?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let Some(bytes) = meta.get(
        CF_MATERIALISATION,
        TABLE_WATCH_CHECKPOINT_ROW,
        &watch_checkpoint_tuple_key(watch_stream_id, consumer_id)?,
    )?
    else {
        return Ok(None);
    };
    let checkpoint = decode_watch_checkpoint_row(&bytes)?;
    checkpoint.verify(signing_key)?;
    if checkpoint.watch_stream_id != watch_stream_id || checkpoint.consumer_id != consumer_id {
        return Err(anyhow!("watch checkpoint path scope mismatch"));
    }
    Ok(Some(checkpoint))
}

pub fn read_watch_checkpoint_lag_record(
    storage: &Storage,
    watch_stream_id: &str,
    consumer_id: &str,
) -> Result<Option<WatchCheckpointLagRecord>> {
    require_safe_component(watch_stream_id, "watch_stream_id")?;
    require_safe_component(consumer_id, "consumer_id")?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    meta.get(
        CF_MATERIALISATION,
        TABLE_MATERIALISATION_CURSOR_ROW,
        &watch_checkpoint_lag_tuple_key(watch_stream_id, consumer_id)?,
    )?
    .map(|payload| decode_watch_checkpoint_lag_record(&payload))
    .transpose()
}

async fn validate_write_authority(
    storage: &Storage,
    update: &WatchCheckpointUpdate,
    authority: &WatchCheckpointWriteAuthority,
    signing_key: &[u8],
) -> Result<()> {
    if authority.fence == 0 {
        return Err(anyhow!("watch checkpoint write fence must be nonzero"));
    }
    if authority.owner_node_id != update.updated_by_node {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: watch checkpoint writer node mismatch"
        ));
    }
    let expected_resource_id = watch_checkpoint_resource_id(
        &update.watch_stream_id,
        &update.partition_id,
        &update.consumer_id,
    );
    if authority.resource_id != expected_resource_id {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: watch checkpoint authority resource mismatch"
        ));
    }
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::WatchPartition,
        resource_id: authority.resource_id.clone(),
    };
    let Some(record) = read_ownership_fence(storage, 0, &resource, signing_key).await? else {
        return Err(anyhow!(
            "{OWNERSHIP_NOT_FOUND}: watch checkpoint ownership fence is absent"
        ));
    };
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("watch checkpoint timestamp overflow"))?;
    if !record.is_active_unexpired(now_nanos) {
        return Err(anyhow!(
            "{OWNERSHIP_EXPIRED}: watch checkpoint ownership fence is not active"
        ));
    }
    if record.owner.principal_id != authority.owner_node_id
        || record.owner.actor_instance_id != authority.owner_node_id
    {
        return Err(anyhow!(
            "{OWNERSHIP_OWNER_MISMATCH}: watch checkpoint ownership fence owner mismatch"
        ));
    }
    if record.fence != authority.fence {
        return Err(anyhow!(
            "{OWNERSHIP_STALE_FENCE}: watch checkpoint ownership fence token mismatch"
        ));
    }
    Ok(())
}

async fn write_watch_checkpoint_lag_record(
    storage: &Storage,
    checkpoint: &WatchCheckpoint,
) -> Result<()> {
    let checkpoint_hash = checkpoint
        .checkpoint_hash
        .clone()
        .ok_or_else(|| anyhow!("sealed watch checkpoint is missing checkpoint hash"))?;
    let record = WatchCheckpointLagRecord {
        watch_stream_id: checkpoint.watch_stream_id.clone(),
        partition_family: checkpoint.partition_family.clone(),
        partition_id: checkpoint.partition_id.clone(),
        consumer_id: checkpoint.consumer_id.clone(),
        applied_cursor: checkpoint.cursor,
        source_cursor_high: checkpoint.source_cursor_high,
        lag_record_count_hint: checkpoint.lag_record_count_hint,
        checkpoint_generation: checkpoint.generation,
        checkpoint_hash,
        updated_at_nanos: checkpoint.updated_at_nanos,
    };
    let payload = encode_watch_checkpoint_lag_record(&record)?;
    let tuple_key = watch_checkpoint_lag_tuple_key(&record.watch_stream_id, &record.consumer_id)?;
    let op = CoreMetaBatchOp {
        cf: CF_MATERIALISATION,
        table_id: TABLE_MATERIALISATION_CURSOR_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "watch-checkpoint-lag:{}:{}:{}",
            record.watch_stream_id, record.consumer_id, record.checkpoint_generation
        ),
        &[op],
    )
    .await?;
    Ok(())
}

async fn write_watch_checkpoint(storage: &Storage, checkpoint: &WatchCheckpoint) -> Result<()> {
    let payload = encode_watch_checkpoint_row(checkpoint)?;
    let tuple_key =
        watch_checkpoint_tuple_key(&checkpoint.watch_stream_id, &checkpoint.consumer_id)?;
    let op = CoreMetaBatchOp {
        cf: CF_MATERIALISATION,
        table_id: TABLE_WATCH_CHECKPOINT_ROW,
        tuple_key: &tuple_key,
        common: None,
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    commit_coremeta_batch_for_storage(
        storage,
        &format!(
            "watch-checkpoint:{}:{}:{}",
            checkpoint.watch_stream_id, checkpoint.consumer_id, checkpoint.generation
        ),
        &[op],
    )
    .await?;
    Ok(())
}

fn validate_update(update: &WatchCheckpointUpdate) -> Result<()> {
    require_safe_component(&update.watch_stream_id, "watch_stream_id")?;
    require_safe_component(&update.partition_family, "partition_family")?;
    validate_hex32(&update.partition_id, "partition_id")?;
    require_safe_component(&update.consumer_id, "consumer_id")?;
    validate_hex32(&update.source_manifest_hash, "source_manifest_hash")?;
    require_nonempty(&update.updated_by_node, "updated_by_node")?;
    if update.source_cursor_high < update.cursor {
        return Err(anyhow!(
            "watch checkpoint source_cursor_high must be at or after applied cursor"
        ));
    }
    if update.generation == 0 {
        return Err(anyhow!("watch checkpoint generation must be nonzero"));
    }
    if update.updated_at_nanos < 0 {
        return Err(anyhow!("watch checkpoint timestamp must be nonnegative"));
    }
    Ok(())
}

fn validate_unsigned_checkpoint(checkpoint: &WatchCheckpoint) -> Result<()> {
    if checkpoint.format_version != 1 {
        return Err(anyhow!("unsupported watch checkpoint version"));
    }
    let update = WatchCheckpointUpdate {
        watch_stream_id: checkpoint.watch_stream_id.clone(),
        partition_family: checkpoint.partition_family.clone(),
        partition_id: checkpoint.partition_id.clone(),
        consumer_id: checkpoint.consumer_id.clone(),
        cursor: checkpoint.cursor,
        source_cursor_high: checkpoint.source_cursor_high,
        lag_record_count_hint: checkpoint.lag_record_count_hint,
        source_manifest_hash: checkpoint.source_manifest_hash.clone(),
        generation: checkpoint.generation,
        updated_by_node: checkpoint.updated_by_node.clone(),
        updated_at_nanos: checkpoint.updated_at_nanos,
    };
    validate_update(&update)
}

fn sign_checkpoint_hash(signing_key: &[u8], hash: &str, scope_parts: &[&str]) -> Result<String> {
    if signing_key.is_empty() {
        return Err(anyhow!("watch checkpoint signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(signing_key)?;
    mac.update(b"watch_checkpoint");
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
    if value.is_empty() {
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
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

fn encode_watch_checkpoint(checkpoint: &WatchCheckpoint) -> Vec<u8> {
    encode_deterministic_proto(&WatchCheckpointProto {
        format_version: u32::from(checkpoint.format_version),
        watch_stream_id: checkpoint.watch_stream_id.clone(),
        partition_family: checkpoint.partition_family.clone(),
        partition_id: checkpoint.partition_id.clone(),
        consumer_id: checkpoint.consumer_id.clone(),
        cursor: checkpoint.cursor.to_string(),
        source_cursor_high: checkpoint.source_cursor_high.to_string(),
        lag_record_count_hint: checkpoint.lag_record_count_hint,
        source_manifest_hash: checkpoint.source_manifest_hash.clone(),
        generation: checkpoint.generation,
        updated_by_node: checkpoint.updated_by_node.clone(),
        updated_at_nanos: checkpoint.updated_at_nanos,
        checkpoint_hash: checkpoint.checkpoint_hash.clone(),
        checkpoint_signature: checkpoint.checkpoint_signature.clone(),
    })
}

fn encode_watch_checkpoint_row(checkpoint: &WatchCheckpoint) -> Result<Vec<u8>> {
    validate_unsigned_checkpoint(checkpoint)?;
    let expected_hash = hash_watch_checkpoint(checkpoint)?;
    if checkpoint.checkpoint_hash.as_deref() != Some(expected_hash.as_str()) {
        return Err(anyhow!("watch checkpoint row hash mismatch"));
    }
    if checkpoint.checkpoint_signature.is_none() {
        return Err(anyhow!("watch checkpoint row requires sealed checkpoint"));
    }
    let checkpoint_hash = checkpoint
        .checkpoint_hash
        .clone()
        .ok_or_else(|| anyhow!("sealed watch checkpoint is missing checkpoint hash"))?;
    Ok(encode_deterministic_proto(&WatchCheckpointRowProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(&format!(
                "watch-checkpoint/{}/{}",
                checkpoint.watch_stream_id, checkpoint.consumer_id
            )),
            checkpoint.generation,
            checkpoint_hash,
            checkpoint.updated_at_nanos.max(0) as u64,
        )),
        schema: "anvil.coremeta.watch_checkpoint.v1".to_string(),
        checkpoint_bytes: encode_watch_checkpoint(checkpoint),
    }))
}

fn encode_watch_checkpoint_lag_record(record: &WatchCheckpointLagRecord) -> Result<Vec<u8>> {
    validate_lag_record(record)?;
    Ok(encode_deterministic_proto(&WatchCheckpointLagRecordProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash(&format!(
                "watch-checkpoint/{}/{}",
                record.watch_stream_id, record.consumer_id
            )),
            record.checkpoint_generation,
            record.checkpoint_hash.clone(),
            record.updated_at_nanos.max(0) as u64,
        )),
        schema: "anvil.coremeta.watch_checkpoint_lag.v1".to_string(),
        watch_stream_id: record.watch_stream_id.clone(),
        partition_family: record.partition_family.clone(),
        partition_id: record.partition_id.clone(),
        consumer_id: record.consumer_id.clone(),
        applied_cursor: record.applied_cursor.to_string(),
        source_cursor_high: record.source_cursor_high.to_string(),
        lag_record_count_hint: record.lag_record_count_hint,
        checkpoint_generation: record.checkpoint_generation,
        checkpoint_hash: record.checkpoint_hash.clone(),
        updated_at_nanos: record.updated_at_nanos,
    }))
}

fn decode_watch_checkpoint(bytes: &[u8]) -> Result<WatchCheckpoint> {
    let proto =
        decode_deterministic_proto::<WatchCheckpointProto>(bytes, "watch checkpoint payload")?;
    Ok(WatchCheckpoint {
        format_version: proto
            .format_version
            .try_into()
            .map_err(|_| anyhow!("watch checkpoint format_version overflow"))?,
        watch_stream_id: proto.watch_stream_id,
        partition_family: proto.partition_family,
        partition_id: proto.partition_id,
        consumer_id: proto.consumer_id,
        cursor: proto
            .cursor
            .parse()
            .map_err(|_| anyhow!("watch checkpoint cursor is not u128"))?,
        source_cursor_high: proto
            .source_cursor_high
            .parse()
            .map_err(|_| anyhow!("watch checkpoint source_cursor_high is not u128"))?,
        lag_record_count_hint: proto.lag_record_count_hint,
        source_manifest_hash: proto.source_manifest_hash,
        generation: proto.generation,
        updated_by_node: proto.updated_by_node,
        updated_at_nanos: proto.updated_at_nanos,
        checkpoint_hash: proto.checkpoint_hash,
        checkpoint_signature: proto.checkpoint_signature,
    })
}

fn decode_watch_checkpoint_row(bytes: &[u8]) -> Result<WatchCheckpoint> {
    let row = decode_deterministic_proto::<WatchCheckpointRowProto>(bytes, "watch checkpoint row")?;
    if row.schema != "anvil.coremeta.watch_checkpoint.v1" {
        return Err(anyhow!("watch checkpoint row has invalid schema"));
    }
    row.common
        .as_ref()
        .ok_or_else(|| anyhow!("watch checkpoint row missing CoreMeta common"))?;
    decode_watch_checkpoint(&row.checkpoint_bytes)
}

fn decode_watch_checkpoint_lag_record(bytes: &[u8]) -> Result<WatchCheckpointLagRecord> {
    let proto = decode_deterministic_proto::<WatchCheckpointLagRecordProto>(
        bytes,
        "watch checkpoint lag record",
    )?;
    if proto.schema != "anvil.coremeta.watch_checkpoint_lag.v1" {
        return Err(anyhow!("watch checkpoint lag record has invalid schema"));
    }
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("watch checkpoint lag row missing CoreMeta common"))?;
    let record = WatchCheckpointLagRecord {
        watch_stream_id: proto.watch_stream_id,
        partition_family: proto.partition_family,
        partition_id: proto.partition_id,
        consumer_id: proto.consumer_id,
        applied_cursor: proto
            .applied_cursor
            .parse()
            .map_err(|_| anyhow!("watch checkpoint lag applied_cursor is not u128"))?,
        source_cursor_high: proto
            .source_cursor_high
            .parse()
            .map_err(|_| anyhow!("watch checkpoint lag source_cursor_high is not u128"))?,
        lag_record_count_hint: proto.lag_record_count_hint,
        checkpoint_generation: proto.checkpoint_generation,
        checkpoint_hash: proto.checkpoint_hash,
        updated_at_nanos: proto.updated_at_nanos,
    };
    validate_lag_record(&record)?;
    Ok(record)
}

fn watch_checkpoint_tuple_key(watch_stream_id: &str, consumer_id: &str) -> Result<Vec<u8>> {
    require_safe_component(watch_stream_id, "watch_stream_id")?;
    require_safe_component(consumer_id, "consumer_id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("watch-checkpoint"),
        CoreMetaTuplePart::Utf8(watch_stream_id),
        CoreMetaTuplePart::Utf8(consumer_id),
    ])
}

fn watch_checkpoint_lag_tuple_key(watch_stream_id: &str, consumer_id: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("watch_checkpoint_lag"),
        CoreMetaTuplePart::Utf8(watch_stream_id),
        CoreMetaTuplePart::Utf8(consumer_id),
    ])
}

fn validate_lag_record(record: &WatchCheckpointLagRecord) -> Result<()> {
    require_safe_component(&record.watch_stream_id, "watch_stream_id")?;
    require_safe_component(&record.consumer_id, "consumer_id")?;
    require_nonempty(&record.partition_family, "partition_family")?;
    validate_hex32(&record.partition_id, "partition_id")?;
    validate_hex32(&record.checkpoint_hash, "checkpoint_hash")?;
    if record.source_cursor_high < record.applied_cursor {
        return Err(anyhow!(
            "watch checkpoint lag source cursor precedes applied cursor"
        ));
    }
    if record.checkpoint_generation == 0 {
        return Err(anyhow!("watch checkpoint lag generation must be nonzero"));
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
    use tempfile::tempdir;

    const KEY: &[u8] = b"watch checkpoint signing key";

    #[tokio::test]
    async fn watch_checkpoint_writes_reads_and_advances_cursor() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first_update = update(40, 1);
        let first_authority = authority(&storage, &first_update).await;
        let first = checkpoint_watch_consumer(&storage, first_update, first_authority, KEY)
            .await
            .unwrap();
        assert_eq!(first.cursor, 40);
        assert_eq!(first.generation, 1);
        assert!(first.checkpoint_hash.as_deref().unwrap().len() == 64);
        assert!(watch_checkpoint_tuple_key("object-prefix", "full-text-builder").is_ok());

        let second_update = update(75, 2);
        let second_authority = authority(&storage, &second_update).await;
        let second = checkpoint_watch_consumer(&storage, second_update, second_authority, KEY)
            .await
            .unwrap();
        assert_eq!(second.cursor, 75);
        assert_eq!(second.generation, 2);
        assert_eq!(
            read_watch_checkpoint(&storage, "object-prefix", "full-text-builder", KEY)
                .await
                .unwrap()
                .unwrap(),
            second
        );
        let lag = read_watch_checkpoint_lag_record(&storage, "object-prefix", "full-text-builder")
            .unwrap()
            .unwrap();
        assert_eq!(lag.applied_cursor, 75);
        assert_eq!(lag.source_cursor_high, 80);
        assert_eq!(lag.lag_record_count_hint, 5);
    }

    #[tokio::test]
    async fn watch_checkpoint_rejects_backwards_progress_and_partition_changes() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = update(40, 3);
        let first_authority = authority(&storage, &first).await;
        checkpoint_watch_consumer(&storage, first, first_authority, KEY)
            .await
            .unwrap();
        let backwards_cursor = update(39, 4);
        let backwards_cursor_authority = authority(&storage, &backwards_cursor).await;
        assert!(
            checkpoint_watch_consumer(&storage, backwards_cursor, backwards_cursor_authority, KEY)
                .await
                .is_err()
        );
        let backwards_generation = update(41, 2);
        let backwards_generation_authority = authority(&storage, &backwards_generation).await;
        assert!(
            checkpoint_watch_consumer(
                &storage,
                backwards_generation,
                backwards_generation_authority,
                KEY
            )
            .await
            .is_err()
        );
        let mut changed_partition = update(41, 4);
        changed_partition.partition_id = hex::encode([2; 32]);
        let changed_partition_authority = authority(&storage, &changed_partition).await;
        assert!(
            checkpoint_watch_consumer(
                &storage,
                changed_partition,
                changed_partition_authority,
                KEY
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn watch_checkpoint_rejects_tamper_invalid_inputs_and_unsafe_paths() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first = update(40, 1);
        let first_authority = authority(&storage, &first).await;
        checkpoint_watch_consumer(&storage, first, first_authority, KEY)
            .await
            .unwrap();
        let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
        let tuple_key = watch_checkpoint_tuple_key("object-prefix", "full-text-builder").unwrap();
        let mut row = decode_deterministic_proto::<WatchCheckpointRowProto>(
            &meta
                .get(CF_MATERIALISATION, TABLE_WATCH_CHECKPOINT_ROW, &tuple_key)
                .unwrap()
                .unwrap(),
            "watch checkpoint row",
        )
        .unwrap();
        let mut value = decode_deterministic_proto::<WatchCheckpointProto>(
            &row.checkpoint_bytes,
            "watch checkpoint",
        )
        .unwrap();
        value.cursor = "41".to_string();
        row.checkpoint_bytes = encode_deterministic_proto(&value);
        meta.put(
            CF_MATERIALISATION,
            TABLE_WATCH_CHECKPOINT_ROW,
            &tuple_key,
            &encode_deterministic_proto(&row),
        )
        .unwrap();
        assert!(
            read_watch_checkpoint(&storage, "object-prefix", "full-text-builder", KEY)
                .await
                .is_err()
        );
        assert!(watch_checkpoint_tuple_key("../escape", "consumer").is_err());
        assert!(watch_checkpoint_tuple_key("stream", "../escape").is_err());
        let mut invalid = update(1, 1);
        invalid.source_manifest_hash = "not-hex".to_string();
        let invalid_authority = WatchCheckpointWriteAuthority {
            owner_node_id: "node-a".to_string(),
            fence: 1,
            resource_id: watch_checkpoint_resource_id(
                &invalid.watch_stream_id,
                &invalid.partition_id,
                &invalid.consumer_id,
            ),
        };
        assert!(
            checkpoint_watch_consumer(&storage, invalid, invalid_authority, KEY)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn watch_checkpoint_rejects_stale_or_mismatched_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let first_update = update(40, 1);
        let valid = authority(&storage, &first_update).await;

        let stale = WatchCheckpointWriteAuthority {
            fence: valid.fence.saturating_add(1),
            ..valid.clone()
        };
        let err = checkpoint_watch_consumer(&storage, first_update.clone(), stale, KEY)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("StaleFence"));

        let wrong_owner = WatchCheckpointWriteAuthority {
            owner_node_id: "node-b".to_string(),
            ..valid
        };
        let err = checkpoint_watch_consumer(&storage, first_update, wrong_owner, KEY)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("OwnershipOwnerMismatch"));

        let next_update = update(41, 2);
        let stale_after_failover = authority(&storage, &next_update).await;
        replace_watch_checkpoint_owner(&storage, &stale_after_failover).await;
        let err = checkpoint_watch_consumer(&storage, next_update, stale_after_failover, KEY)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("OwnershipOwnerMismatch"));
    }

    fn update(cursor: u128, generation: u64) -> WatchCheckpointUpdate {
        WatchCheckpointUpdate {
            watch_stream_id: "object-prefix".to_string(),
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode([1; 32]),
            consumer_id: "full-text-builder".to_string(),
            cursor,
            source_cursor_high: cursor.saturating_add(5),
            lag_record_count_hint: 5,
            source_manifest_hash: hex::encode([9; 32]),
            generation,
            updated_by_node: "node-a".to_string(),
            updated_at_nanos: 1000 + i64::try_from(cursor).unwrap(),
        }
    }

    async fn authority(
        storage: &Storage,
        update: &WatchCheckpointUpdate,
    ) -> WatchCheckpointWriteAuthority {
        let resource_id = watch_checkpoint_resource_id(
            &update.watch_stream_id,
            &update.partition_id,
            &update.consumer_id,
        );
        let outcome = acquire_ownership(
            storage,
            AcquireOwnership {
                request_id: format!("test-watch-checkpoint-{resource_id}"),
                idempotency_key: format!("test-watch-checkpoint-{resource_id}"),
                resource: OwnershipResource {
                    resource_kind: OwnershipResourceKind::WatchPartition,
                    resource_id: resource_id.clone(),
                },
                owner: OwnershipPrincipal {
                    tenant_id: 0,
                    principal_kind: "node".to_string(),
                    principal_id: update.updated_by_node.clone(),
                    actor_instance_id: update.updated_by_node.clone(),
                    display_name: update.updated_by_node.clone(),
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
        WatchCheckpointWriteAuthority {
            owner_node_id: update.updated_by_node.clone(),
            fence: outcome.record.fence,
            resource_id,
        }
    }

    async fn replace_watch_checkpoint_owner(
        storage: &Storage,
        stale_authority: &WatchCheckpointWriteAuthority,
    ) {
        let resource = OwnershipResource {
            resource_kind: OwnershipResourceKind::WatchPartition,
            resource_id: stale_authority.resource_id.clone(),
        };
        let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();
        force_expire_ownership(
            storage,
            ForceExpireOwnership {
                request_id: format!(
                    "test-watch-checkpoint-expire-{}",
                    stale_authority.resource_id
                ),
                idempotency_key: format!(
                    "test-watch-checkpoint-expire-{}",
                    stale_authority.resource_id
                ),
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
                    "test-watch-checkpoint-replacement-{}",
                    stale_authority.resource_id
                ),
                idempotency_key: format!(
                    "test-watch-checkpoint-replacement-{}",
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
