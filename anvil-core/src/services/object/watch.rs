use super::*;

pub(super) fn watch_event_response(
    event: &crate::persistence::ObjectWatchEvent,
) -> Option<WatchPrefixResponse> {
    let cursor = u64::try_from(event.id).ok()?;
    let created_at = event.created_at.to_string();
    Some(WatchPrefixResponse {
        cursor,
        bucket_name: event.bucket_name.clone(),
        object_key: event.key.clone(),
        event_type: event.event_type.clone(),
        version_id: event
            .version_id
            .map(|version_id| version_id.to_string())
            .unwrap_or_default(),
        etag: event.etag.clone().unwrap_or_default(),
        size: event.size,
        is_delete_marker: event.is_delete_marker,
        created_at: created_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "object_prefix",
            partition_family: "object_metadata",
            partition_id: event.bucket_id.to_string(),
            cursor: event.id as u128,
            mutation_id: event.mutation_id.to_string(),
            record_kind: event.event_type.clone(),
            object_ref: format!("{}/{}", event.bucket_name, event.key),
            authz_revision: 0,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash: event.payload_hash.clone(),
            emitted_at: created_at,
        })),
    })
}
