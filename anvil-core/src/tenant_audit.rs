use crate::{
    core_store::{AppendStreamRecord, CoreStore, ReadStream},
    storage::Storage,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};

pub const TENANT_AUDIT_EVENT_SCHEMA: &str = "anvil.tenant.audit_event.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TenantAuditEvent {
    pub schema: String,
    pub audit_event_id: String,
    pub request_id: String,
    pub tenant_id: i64,
    pub principal_id: String,
    pub resource_id: String,
    pub action: String,
    pub created_at: String,
    pub details_json: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TenantAuditEventFilter<'a> {
    pub principal_id: Option<&'a str>,
    pub resource_id: Option<&'a str>,
    pub action: Option<&'a str>,
}

pub async fn append_tenant_audit_event(storage: &Storage, event: &TenantAuditEvent) -> Result<()> {
    CoreStore::new(storage.clone())
        .await?
        .append_stream(AppendStreamRecord {
            stream_id: tenant_audit_stream_id(event.tenant_id),
            partition_id: format!("tenant:{}", event.tenant_id),
            record_kind: "tenant_audit_event".to_string(),
            payload: serde_json::to_vec(event)?,
            fence: None,
            transaction_id: None,
            idempotency_key: Some(event.audit_event_id.clone()),
        })
        .await?;
    Ok(())
}

pub async fn list_tenant_audit_events(
    storage: &Storage,
    tenant_id: i64,
    filter: TenantAuditEventFilter<'_>,
) -> Result<Vec<TenantAuditEvent>> {
    let mut events = Vec::new();
    for record in CoreStore::new(storage.clone())
        .await?
        .read_stream(ReadStream {
            stream_id: tenant_audit_stream_id(tenant_id),
            after_sequence: 0,
            limit: 0,
        })
        .await?
    {
        let event: TenantAuditEvent = serde_json::from_slice(&record.payload)?;
        if event.tenant_id == tenant_id && matches_filter(&event, &filter) {
            events.push(event);
        }
    }
    events.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then(left.audit_event_id.cmp(&right.audit_event_id))
    });
    Ok(events)
}

pub fn audit_event_position(event: &TenantAuditEvent) -> String {
    format!("{}:{}", event.created_at, event.audit_event_id)
}

pub fn audit_event_revision_generation(event: &TenantAuditEvent) -> u64 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil-tenant-audit-event-revision-v1");
    for part in [
        event.schema.as_bytes(),
        event.audit_event_id.as_bytes(),
        event.request_id.as_bytes(),
        &event.tenant_id.to_le_bytes(),
        event.principal_id.as_bytes(),
        event.resource_id.as_bytes(),
        event.action.as_bytes(),
        event.created_at.as_bytes(),
        event.details_json.as_bytes(),
    ] {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part);
    }
    u64::from_le_bytes(
        hasher.finalize().as_bytes()[0..8]
            .try_into()
            .expect("blake3 digest"),
    )
}

pub fn collection_revision<'a>(events: impl IntoIterator<Item = &'a TenantAuditEvent>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"tenant-audit-collection-revision-v1");
    for event in events {
        let position = audit_event_position(event);
        hasher.update(&(position.len() as u64).to_le_bytes());
        hasher.update(position.as_bytes());
        hasher.update(&audit_event_revision_generation(event).to_le_bytes());
    }
    hex::encode(hasher.finalize().as_bytes())
}

fn tenant_audit_stream_id(tenant_id: i64) -> String {
    format!("tenant_audit:{tenant_id}")
}

fn matches_filter(event: &TenantAuditEvent, filter: &TenantAuditEventFilter<'_>) -> bool {
    filter
        .principal_id
        .is_none_or(|value| event.principal_id == value)
        && filter
            .resource_id
            .is_none_or(|value| event.resource_id == value)
        && filter.action.is_none_or(|value| event.action == value)
}
