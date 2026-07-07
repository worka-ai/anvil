use crate::core_store::{
    CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition, CoreStore, ReadStream,
};
use crate::formats::{Hash32, JournalFrame, JournalRecordKind, hash32, validate_journal_chain};
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{App, AppDetails, Tenant};
use crate::storage::Storage;
use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", rename_all = "snake_case")]
enum ControlEventBody {
    RegionUpsert {
        name: String,
    },
    TenantUpsert {
        id: i64,
        name: String,
    },
    AppCreate {
        id: i64,
        tenant_id: i64,
        name: String,
        client_id: String,
        client_secret_encrypted: Vec<u8>,
    },
    AppSecretUpdate {
        app_id: i64,
        client_secret_encrypted: Vec<u8>,
    },
    AppDelete {
        app_id: i64,
    },
    AppPolicyGrant {
        app_id: i64,
        resource: String,
        action: String,
    },
    AppPolicyRevoke {
        app_id: i64,
        resource: String,
        action: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ControlState {
    next_id: i64,
    regions: BTreeSet<String>,
    tenants: BTreeMap<i64, Tenant>,
    apps: BTreeMap<i64, StoredControlApp>,
    app_policies: BTreeSet<StoredControlPolicy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredControlApp {
    id: i64,
    tenant_id: i64,
    name: String,
    client_id: String,
    client_secret_encrypted: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StoredControlPolicy {
    app_id: i64,
    resource: String,
    action: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredAppPolicy {
    pub app_id: i64,
    pub resource: String,
    pub action: String,
}

impl ControlState {
    fn allocate_id(&self) -> i64 {
        self.next_id.saturating_add(1)
    }

    pub fn regions(&self) -> Vec<String> {
        self.regions.iter().cloned().collect()
    }

    pub fn tenants(&self) -> Vec<Tenant> {
        self.tenants.values().cloned().collect()
    }

    pub fn tenant_by_name(&self, name: &str) -> Option<Tenant> {
        self.tenants
            .values()
            .find(|tenant| tenant.name == name)
            .cloned()
    }

    pub fn app_by_name(&self, name: &str) -> Option<App> {
        self.apps
            .values()
            .find(|app| app.name == name)
            .map(app_record)
    }

    pub fn app_by_id(&self, id: i64) -> Option<App> {
        self.apps.get(&id).map(app_record)
    }

    pub fn apps_for_tenant(&self, tenant_id: i64) -> Vec<App> {
        self.apps
            .values()
            .filter(|app| app.tenant_id == tenant_id)
            .map(app_record)
            .collect()
    }

    pub fn app_details_by_client_id(&self, client_id: &str) -> Option<AppDetails> {
        self.apps
            .values()
            .find(|app| app.client_id == client_id)
            .map(|app| AppDetails {
                id: app.id,
                tenant_id: app.tenant_id,
                client_secret_encrypted: app.client_secret_encrypted.clone(),
            })
    }

    pub fn policies_for_app(&self, app_id: i64) -> Vec<String> {
        self.app_policies
            .iter()
            .filter(|policy| policy.app_id == app_id)
            .map(|policy| format!("{}|{}", policy.action, policy.resource))
            .collect()
    }

    pub fn policy_records_for_app(&self, app_id: i64) -> Vec<StoredAppPolicy> {
        self.app_policies
            .iter()
            .filter(|policy| policy.app_id == app_id)
            .map(|policy| StoredAppPolicy {
                app_id: policy.app_id,
                resource: policy.resource.clone(),
                action: policy.action.clone(),
            })
            .collect()
    }

    pub fn policy_summaries(&self) -> Vec<String> {
        let mut policies = self
            .app_policies
            .iter()
            .map(|policy| format!("{}:{}", policy.action, policy.resource))
            .collect::<Vec<_>>();
        policies.sort();
        policies.dedup();
        policies
    }
}

pub async fn read_control_state(storage: &Storage) -> Result<ControlState> {
    let frames = read_control_journal_frames(storage).await?;
    let mut state = ControlState::default();
    for frame in frames {
        if frame.record_kind != JournalRecordKind::ControlPlane {
            continue;
        }
        let body: ControlEventBody = serde_json::from_slice(&frame.body)?;
        apply_event(&mut state, body);
    }
    Ok(state)
}

#[cfg(test)]
async fn create_region(storage: &Storage, name: &str) -> Result<bool> {
    create_region_inner(storage, name, 0, None).await
}

pub(crate) async fn create_region_with_permit(
    storage: &Storage,
    name: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<bool> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_region_inner(
        storage,
        name,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_region_inner(
    storage: &Storage,
    name: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<bool> {
    require_nonempty(name, "region")?;
    let state = read_control_state(storage).await?;
    if state.regions.contains(name) {
        return Ok(false);
    }
    append_control_event(
        storage,
        ControlEventBody::RegionUpsert {
            name: name.to_string(),
        },
        region_key_hash(name),
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(true)
}

#[cfg(test)]
async fn create_tenant(storage: &Storage, name: &str) -> Result<Tenant> {
    create_tenant_inner(storage, name, 0, None).await
}

pub(crate) async fn create_tenant_with_permit(
    storage: &Storage,
    name: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<Tenant> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_tenant_inner(
        storage,
        name,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_tenant_inner(
    storage: &Storage,
    name: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<Tenant> {
    require_nonempty(name, "tenant")?;
    let state = read_control_state(storage).await?;
    if let Some(existing) = state.tenant_by_name(name) {
        return Ok(existing);
    }
    let tenant = Tenant {
        id: state.allocate_id(),
        name: name.to_string(),
    };
    append_control_event(
        storage,
        ControlEventBody::TenantUpsert {
            id: tenant.id,
            name: tenant.name.clone(),
        },
        tenant_key_hash(&tenant.name),
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(tenant)
}

#[cfg(test)]
async fn create_app(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    client_id: &str,
    encrypted_secret: &[u8],
) -> Result<App> {
    create_app_inner(
        storage,
        tenant_id,
        name,
        client_id,
        encrypted_secret,
        0,
        None,
    )
    .await
}

pub(crate) async fn create_app_with_permit(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    client_id: &str,
    encrypted_secret: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<App> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    create_app_inner(
        storage,
        tenant_id,
        name,
        client_id,
        encrypted_secret,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn create_app_inner(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    client_id: &str,
    encrypted_secret: &[u8],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<App> {
    require_nonempty(name, "app")?;
    require_nonempty(client_id, "client_id")?;
    let state = read_control_state(storage).await?;
    if state
        .apps
        .values()
        .any(|app| app.tenant_id == tenant_id && app.name == name)
    {
        return Err(anyhow!("app already exists"));
    }
    let app = App {
        id: state.allocate_id(),
        name: name.to_string(),
        client_id: client_id.to_string(),
    };
    append_control_event(
        storage,
        ControlEventBody::AppCreate {
            id: app.id,
            tenant_id,
            name: app.name.clone(),
            client_id: app.client_id.clone(),
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        app_key_hash(tenant_id, name),
        fence_token,
        partition_precondition,
    )
    .await?;
    Ok(app)
}

#[cfg(test)]
async fn update_app_secret(storage: &Storage, app_id: i64, encrypted_secret: &[u8]) -> Result<()> {
    update_app_secret_inner(storage, app_id, encrypted_secret, 0, None).await
}

pub(crate) async fn update_app_secret_with_permit(
    storage: &Storage,
    app_id: i64,
    encrypted_secret: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    update_app_secret_inner(
        storage,
        app_id,
        encrypted_secret,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn update_app_secret_inner(
    storage: &Storage,
    app_id: i64,
    encrypted_secret: &[u8],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let state = read_control_state(storage).await?;
    if !state.apps.contains_key(&app_id) {
        return Err(anyhow!("app not found"));
    }
    append_control_event(
        storage,
        ControlEventBody::AppSecretUpdate {
            app_id,
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        app_id_key_hash(app_id),
        fence_token,
        partition_precondition,
    )
    .await
}

pub(crate) async fn delete_app_with_permit(
    storage: &Storage,
    app_id: i64,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    delete_app_inner(
        storage,
        app_id,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn delete_app_inner(
    storage: &Storage,
    app_id: i64,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let state = read_control_state(storage).await?;
    if !state.apps.contains_key(&app_id) {
        return Err(anyhow!("app not found"));
    }
    append_control_event(
        storage,
        ControlEventBody::AppDelete { app_id },
        app_id_key_hash(app_id),
        fence_token,
        partition_precondition,
    )
    .await
}

#[cfg(test)]
async fn grant_policy(storage: &Storage, app_id: i64, resource: &str, action: &str) -> Result<()> {
    grant_policy_inner(storage, app_id, resource, action, 0, None).await
}

pub(crate) async fn grant_policy_with_permit(
    storage: &Storage,
    app_id: i64,
    resource: &str,
    action: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    grant_policy_inner(
        storage,
        app_id,
        resource,
        action,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn grant_policy_inner(
    storage: &Storage,
    app_id: i64,
    resource: &str,
    action: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let state = read_control_state(storage).await?;
    if !state.apps.contains_key(&app_id) {
        return Err(anyhow!("app not found"));
    }
    append_control_event(
        storage,
        ControlEventBody::AppPolicyGrant {
            app_id,
            resource: resource.to_string(),
            action: action.to_string(),
        },
        policy_key_hash(app_id, resource, action),
        fence_token,
        partition_precondition,
    )
    .await
}

#[cfg(test)]
async fn revoke_policy(storage: &Storage, app_id: i64, resource: &str, action: &str) -> Result<()> {
    revoke_policy_inner(storage, app_id, resource, action, 0, None).await
}

pub(crate) async fn revoke_policy_with_permit(
    storage: &Storage,
    app_id: i64,
    resource: &str,
    action: &str,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    let partition_precondition =
        control_write_precondition(storage, permit, partition_owner_signing_key).await?;
    revoke_policy_inner(
        storage,
        app_id,
        resource,
        action,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn revoke_policy_inner(
    storage: &Storage,
    app_id: i64,
    resource: &str,
    action: &str,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    append_control_event(
        storage,
        ControlEventBody::AppPolicyRevoke {
            app_id,
            resource: resource.to_string(),
            action: action.to_string(),
        },
        policy_key_hash(app_id, resource, action),
        fence_token,
        partition_precondition,
    )
    .await
}

async fn append_control_event(
    storage: &Storage,
    event: ControlEventBody,
    key_hash: Hash32,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let previous = read_control_journal_frames_from_store(&core_store)
        .await
        .unwrap_or_default();
    let sequence = previous
        .last()
        .map(|frame| frame.partition_sequence + 1)
        .unwrap_or(1);
    let previous_hash = previous
        .last()
        .map(|frame| frame.record_hash)
        .unwrap_or([0; 32]);
    let mutation_id = uuid::Uuid::new_v4();
    let frame = JournalFrame::new(
        JournalRecordKind::ControlPlane,
        sequence,
        fence_token,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&event)?,
    );
    let partition_id = hex::encode(control_partition_id());
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("control-plane:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: control_partition_principal(),
            preconditions: partition_precondition.into_iter().collect(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id: control_plane_stream_id(),
                record_kind: "control_plane".to_string(),
                payload: frame.encode(),
                idempotency_key: Some(format!("control-plane:{mutation_id}")),
            }],
        })
        .await?;
    Ok(())
}

async fn read_control_journal_frames(storage: &Storage) -> Result<Vec<JournalFrame>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_control_journal_frames_from_store(&core_store).await
}

async fn read_control_journal_frames_from_store(
    core_store: &CoreStore,
) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: control_plane_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "control_plane" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

fn apply_event(state: &mut ControlState, event: ControlEventBody) {
    match event {
        ControlEventBody::RegionUpsert { name } => {
            state.regions.insert(name);
        }
        ControlEventBody::TenantUpsert { id, name } => {
            state.next_id = state.next_id.max(id);
            state.tenants.insert(id, Tenant { id, name });
        }
        ControlEventBody::AppCreate {
            id,
            tenant_id,
            name,
            client_id,
            client_secret_encrypted,
        } => {
            state.next_id = state.next_id.max(id);
            state.apps.insert(
                id,
                StoredControlApp {
                    id,
                    tenant_id,
                    name,
                    client_id,
                    client_secret_encrypted,
                },
            );
        }
        ControlEventBody::AppSecretUpdate {
            app_id,
            client_secret_encrypted,
        } => {
            if let Some(app) = state.apps.get_mut(&app_id) {
                app.client_secret_encrypted = client_secret_encrypted;
            }
        }
        ControlEventBody::AppDelete { app_id } => {
            state.apps.remove(&app_id);
            state.app_policies.retain(|policy| policy.app_id != app_id);
        }
        ControlEventBody::AppPolicyGrant {
            app_id,
            resource,
            action,
        } => {
            state.app_policies.insert(StoredControlPolicy {
                app_id,
                resource,
                action,
            });
        }
        ControlEventBody::AppPolicyRevoke {
            app_id,
            resource,
            action,
        } => {
            state.app_policies.remove(&StoredControlPolicy {
                app_id,
                resource,
                action,
            });
        }
    }
}

fn app_record(app: &StoredControlApp) -> App {
    App {
        id: app.id,
        name: app.name.clone(),
        client_id: app.client_id.clone(),
    }
}

fn region_key_hash(name: &str) -> Hash32 {
    hash32(format!("region\0{name}").as_bytes())
}

fn tenant_key_hash(name: &str) -> Hash32 {
    hash32(format!("tenant\0{name}").as_bytes())
}

fn app_key_hash(tenant_id: i64, name: &str) -> Hash32 {
    hash32(format!("app\0{tenant_id}\0{name}").as_bytes())
}

fn app_id_key_hash(app_id: i64) -> Hash32 {
    hash32(format!("app\0{app_id}").as_bytes())
}

fn policy_key_hash(app_id: i64, resource: &str, action: &str) -> Hash32 {
    hash32(format!("policy\0{app_id}\0{resource}\0{action}").as_bytes())
}

pub fn control_partition_id() -> Hash32 {
    hash32(b"control_plane/global")
}

fn control_plane_stream_id() -> String {
    "control_plane:global".to_string()
}

fn control_partition_principal() -> String {
    "partition-owner:control_plane:global".to_string()
}

#[cfg(test)]
pub(crate) async fn read_control_frame_fences_for_test(storage: &Storage) -> Result<Vec<u64>> {
    Ok(read_control_journal_frames(storage)
        .await?
        .into_iter()
        .map(|frame| frame.fence_token)
        .collect())
}

async fn control_write_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    require_control_permit(permit)?;
    Ok(partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?)
}

fn require_control_permit(permit: &PartitionWritePermit) -> Result<()> {
    if permit.partition_family != "control_plane"
        || permit.partition_id != hex::encode(control_partition_id())
    {
        anyhow::bail!("control-plane write permit targets a different partition");
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
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use tempfile::tempdir;

    const KEY: &[u8] = b"control-plane partition owner key";

    #[tokio::test]
    async fn control_journal_replays_regions_tenants_apps_and_policies() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();

        assert!(create_region(&storage, "local").await.unwrap());
        assert!(!create_region(&storage, "local").await.unwrap());
        let tenant = create_tenant(&storage, "default").await.unwrap();
        let same_tenant = create_tenant(&storage, "default").await.unwrap();
        assert_eq!(tenant.id, same_tenant.id);

        let app = create_app(&storage, tenant.id, "demo", "client-a", b"secret-a")
            .await
            .unwrap();
        grant_policy(&storage, app.id, "*", "*").await.unwrap();
        grant_policy(&storage, app.id, "bucket-a", "bucket:create")
            .await
            .unwrap();
        update_app_secret(&storage, app.id, b"secret-b")
            .await
            .unwrap();
        revoke_policy(&storage, app.id, "bucket-a", "bucket:create")
            .await
            .unwrap();

        let replayed = read_control_state(&storage).await.unwrap();
        assert_eq!(replayed.regions(), vec!["local".to_string()]);
        assert_eq!(replayed.tenant_by_name("default").unwrap().id, tenant.id);
        assert_eq!(replayed.app_by_name("demo").unwrap().id, app.id);
        assert_eq!(
            replayed.app_details_by_client_id("client-a").unwrap().id,
            app.id
        );
        assert_eq!(
            replayed
                .app_details_by_client_id("client-a")
                .unwrap()
                .client_secret_encrypted,
            b"secret-b".to_vec()
        );
        assert_eq!(replayed.policies_for_app(app.id), vec!["*|*".to_string()]);
        let frames = read_control_journal_frames(&storage).await.unwrap();
        assert_eq!(frames.len(), 7);
    }

    #[tokio::test]
    async fn control_journal_rejects_duplicate_apps() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tenant = create_tenant(&storage, "default").await.unwrap();
        create_app(&storage, tenant.id, "demo", "client-a", b"secret-a")
            .await
            .unwrap();
        assert!(
            create_app(&storage, tenant.id, "demo", "client-b", b"secret-b")
                .await
                .is_err()
        );
    }

    #[tokio::test]
    pub(crate) async fn control_journal_with_permit_writes_fenced_frames_and_header() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let permit = owner.write_permit().unwrap();

        assert!(
            create_region_with_permit(&storage, "local", &permit, KEY)
                .await
                .unwrap()
        );
        let tenant = create_tenant_with_permit(&storage, "default", &permit, KEY)
            .await
            .unwrap();
        let app = create_app_with_permit(
            &storage,
            tenant.id,
            "demo",
            "client-a",
            b"secret-a",
            &permit,
            KEY,
        )
        .await
        .unwrap();
        grant_policy_with_permit(&storage, app.id, "*", "*", &permit, KEY)
            .await
            .unwrap();
        update_app_secret_with_permit(&storage, app.id, b"secret-b", &permit, KEY)
            .await
            .unwrap();
        revoke_policy_with_permit(&storage, app.id, "*", "*", &permit, KEY)
            .await
            .unwrap();

        let frames = read_control_journal_frames(&storage).await.unwrap();
        assert_eq!(frames.len(), 6);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );
    }

    #[tokio::test]
    pub(crate) async fn control_journal_with_permit_rejects_stale_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_region_with_permit(&storage, "local", &stale_permit, KEY)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("write permit owner is not current")
        );
    }

    #[tokio::test]
    pub(crate) async fn control_journal_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let owner = ready_owner(&storage, "node-a").await;
        let stale_permit = owner.write_permit().unwrap();
        let stale_precondition = partition_write_ref_precondition(&storage, &stale_permit, KEY)
            .await
            .unwrap();
        let newer = ready_owner(&storage, "node-b").await;
        assert!(newer.fence_token > stale_permit.fence_token);

        let err = create_region_inner(
            &storage,
            "local",
            stale_permit.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("generation mismatch") || message.contains("target mismatch"),
            "unexpected stale precondition error: {message}"
        );

        assert!(
            create_region_with_permit(&storage, "local", &newer.write_permit().unwrap(), KEY)
                .await
                .unwrap()
        );
    }

    async fn ready_owner(
        storage: &Storage,
        owner_node_id: &str,
    ) -> crate::partition_fence::PartitionOwnerState {
        let family = "control_plane".to_string();
        let id = hex::encode(control_partition_id());
        let recovering = acquire_partition_recovery(
            storage,
            PartitionRecoveryAcquire {
                partition_family: family.clone(),
                partition_id: id.clone(),
                owner_node_id: owner_node_id.to_string(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos: 100,
            },
            KEY,
        )
        .await
        .unwrap();
        publish_partition_ready(
            storage,
            &family,
            &id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([1; 32]),
            200,
            KEY,
        )
        .await
        .unwrap()
    }
}
