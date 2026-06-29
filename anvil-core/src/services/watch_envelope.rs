use crate::anvil_api::WatchEventEnvelope;
use serde::Serialize;

pub(crate) struct WatchEnvelopeParts {
    pub(crate) watch_stream_id: &'static str,
    pub(crate) partition_family: &'static str,
    pub(crate) partition_id: String,
    pub(crate) cursor: u128,
    pub(crate) mutation_id: String,
    pub(crate) record_kind: String,
    pub(crate) object_ref: String,
    pub(crate) authz_revision: u64,
    pub(crate) index_generation: u64,
    pub(crate) personaldb_log_index: u64,
    pub(crate) payload_hash: String,
    pub(crate) emitted_at: String,
}

pub(crate) fn envelope(parts: WatchEnvelopeParts) -> WatchEventEnvelope {
    let (cursor_low, cursor_high) = split_cursor(parts.cursor);
    WatchEventEnvelope {
        watch_stream_id: parts.watch_stream_id.to_string(),
        partition_family: parts.partition_family.to_string(),
        partition_id: parts.partition_id,
        cursor_low,
        cursor_high,
        mutation_id: parts.mutation_id,
        record_kind: parts.record_kind,
        object_ref: parts.object_ref,
        authz_revision: parts.authz_revision,
        index_generation: parts.index_generation,
        personaldb_log_index: parts.personaldb_log_index,
        payload_hash: parts.payload_hash,
        emitted_at: parts.emitted_at,
    }
}

pub(crate) fn payload_hash<T: Serialize>(payload: &T) -> String {
    let bytes = serde_json::to_vec(payload).unwrap_or_default();
    blake3::hash(&bytes).to_hex().to_string()
}

pub(crate) fn split_cursor(cursor: u128) -> (u64, u64) {
    let low = (cursor & u128::from(u64::MAX)) as u64;
    let high = (cursor >> 64) as u64;
    (low, high)
}

pub(crate) fn uuid_from_bytes(bytes: [u8; 16]) -> String {
    uuid::Uuid::from_bytes(bytes).to_string()
}
