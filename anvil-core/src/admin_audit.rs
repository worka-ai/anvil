use crate::{
    core_store::{AppendStreamRecord, CoreStore, ReadStream},
    storage::Storage,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub const ADMIN_AUDIT_EVENT_SCHEMA: &str = "anvil.admin.audit_event.v1";
const ADMIN_AUDIT_STREAM_ID: &str = "admin_audit:global";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AdminAuditEvent {
    pub schema: String,
    pub audit_event_id: String,
    pub request_id: String,
    pub principal_id: String,
    pub resource_id: String,
    pub action: String,
    pub audit_reason: String,
    pub created_at: String,
    pub details_json: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditEventFilter<'a> {
    pub principal_id: Option<&'a str>,
    pub resource_id: Option<&'a str>,
    pub action: Option<&'a str>,
}

pub async fn append_audit_event(storage: &Storage, event: &AdminAuditEvent) -> Result<()> {
    CoreStore::new(storage.clone())
        .await?
        .append_stream(AppendStreamRecord {
            stream_id: ADMIN_AUDIT_STREAM_ID.to_string(),
            partition_id: "global".to_string(),
            record_kind: "admin_audit_event".to_string(),
            payload: serde_json::to_vec(event)?,
            fence: None,
            transaction_id: None,
            idempotency_key: Some(event.audit_event_id.clone()),
        })
        .await?;
    Ok(())
}

pub async fn list_audit_events(
    storage: &Storage,
    filter: AuditEventFilter<'_>,
) -> Result<Vec<AdminAuditEvent>> {
    let mut out = Vec::new();
    for record in CoreStore::new(storage.clone())
        .await?
        .read_stream(ReadStream {
            stream_id: ADMIN_AUDIT_STREAM_ID.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?
    {
        let event: AdminAuditEvent = serde_json::from_slice(&record.payload)?;
        if matches_filter(&event, &filter) {
            out.push(event);
        }
    }
    out.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then(left.audit_event_id.cmp(&right.audit_event_id))
    });
    Ok(out)
}

pub fn audit_event_position(event: &AdminAuditEvent) -> String {
    format!("{}:{}", event.created_at, event.audit_event_id)
}

pub fn audit_event_revision_generation(event: &AdminAuditEvent) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-admin-audit-event-revision-v1");
    update_hash_part(&mut hasher, event.schema.as_bytes());
    update_hash_part(&mut hasher, event.audit_event_id.as_bytes());
    update_hash_part(&mut hasher, event.request_id.as_bytes());
    update_hash_part(&mut hasher, event.principal_id.as_bytes());
    update_hash_part(&mut hasher, event.resource_id.as_bytes());
    update_hash_part(&mut hasher, event.action.as_bytes());
    update_hash_part(&mut hasher, event.audit_reason.as_bytes());
    update_hash_part(&mut hasher, event.created_at.as_bytes());
    update_hash_part(&mut hasher, event.details_json.as_bytes());
    let digest = hasher.finalize();
    u64::from_le_bytes(digest.as_bytes()[0..8].try_into().expect("blake3 digest"))
}

fn matches_filter(event: &AdminAuditEvent, filter: &AuditEventFilter<'_>) -> bool {
    filter
        .principal_id
        .is_none_or(|principal_id| event.principal_id == principal_id)
        && filter
            .resource_id
            .is_none_or(|resource_id| event.resource_id == resource_id)
        && filter.action.is_none_or(|action| event.action == action)
}

fn update_hash_part(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_le_bytes());
    hasher.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::Storage;
    use tempfile::tempdir;

    fn event(id: &str, principal: &str, resource: &str, action: &str) -> AdminAuditEvent {
        AdminAuditEvent {
            schema: ADMIN_AUDIT_EVENT_SCHEMA.to_string(),
            audit_event_id: id.to_string(),
            request_id: format!("req-{id}"),
            principal_id: principal.to_string(),
            resource_id: resource.to_string(),
            action: action.to_string(),
            audit_reason: "test".to_string(),
            created_at: "2026-07-02T20:00:00Z".to_string(),
            details_json: "{}".to_string(),
        }
    }

    #[tokio::test]
    async fn audit_events_are_durable_and_filterable() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        append_audit_event(&storage, &event("audit-a", "admin-a", "bucket-a", "create"))
            .await
            .unwrap();
        append_audit_event(&storage, &event("audit-b", "admin-b", "bucket-b", "delete"))
            .await
            .unwrap();

        let all = list_audit_events(&storage, AuditEventFilter::default())
            .await
            .unwrap();
        assert_eq!(all.len(), 2);

        let filtered = list_audit_events(
            &storage,
            AuditEventFilter {
                principal_id: Some("admin-a"),
                resource_id: Some("bucket-a"),
                action: Some("create"),
            },
        )
        .await
        .unwrap();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].audit_event_id, "audit-a");
    }
}
