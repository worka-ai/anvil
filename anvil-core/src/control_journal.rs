use crate::formats::{
    BinaryEnvelopeHeader, COMMON_HEADER_LEN, FileFamily, Hash32, JournalFrame, JournalRecordKind,
    hash32, validate_journal_chain,
};
use crate::persistence::{App, AppDetails, Tenant};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Serialize)]
struct ControlJournalHeader<'a> {
    partition_family: &'static str,
    partition_id: &'static str,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

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
    let frames = read_control_journal_frames_at_path(&storage.control_journal_path()).await?;
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

pub async fn create_region(storage: &Storage, name: &str) -> Result<bool> {
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
    )
    .await?;
    Ok(true)
}

pub async fn create_tenant(storage: &Storage, name: &str) -> Result<Tenant> {
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
    )
    .await?;
    Ok(tenant)
}

pub async fn create_app(
    storage: &Storage,
    tenant_id: i64,
    name: &str,
    client_id: &str,
    encrypted_secret: &[u8],
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
    )
    .await?;
    Ok(app)
}

pub async fn update_app_secret(
    storage: &Storage,
    app_id: i64,
    encrypted_secret: &[u8],
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
    )
    .await
}

pub async fn grant_policy(
    storage: &Storage,
    app_id: i64,
    resource: &str,
    action: &str,
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
    )
    .await
}

pub async fn revoke_policy(
    storage: &Storage,
    app_id: i64,
    resource: &str,
    action: &str,
) -> Result<()> {
    append_control_event(
        storage,
        ControlEventBody::AppPolicyRevoke {
            app_id,
            resource: resource.to_string(),
            action: action.to_string(),
        },
        policy_key_hash(app_id, resource, action),
    )
    .await
}

async fn append_control_event(
    storage: &Storage,
    event: ControlEventBody,
    key_hash: Hash32,
) -> Result<()> {
    let path = storage.control_journal_path();
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    ensure_journal_header(&path).await?;
    let previous = read_control_journal_frames_at_path(path.as_path())
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
        0,
        *mutation_id.as_bytes(),
        key_hash,
        previous_hash,
        serde_json::to_vec(&event)?,
    );
    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await
        .with_context(|| format!("open control journal {}", path.display()))?;
    file.write_all(&frame.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn ensure_journal_header(path: &Path) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = chrono::Utc::now().to_rfc3339();
    let header_json = serde_json::to_vec(&ControlJournalHeader {
        partition_family: "control_plane",
        partition_id: "global",
        fence_token: 0,
        first_sequence: 1,
        created_at: &created_at,
        codec: "none",
    })?;
    let header = BinaryEnvelopeHeader::new(FileFamily::MetadataJournal, 0, 0, header_json);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .with_context(|| format!("create control journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

async fn read_control_journal_frames_at_path(path: &Path) -> Result<Vec<JournalFrame>> {
    if tokio::fs::metadata(path).await.is_err() {
        return Ok(Vec::new());
    }
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("read control journal {}", path.display()))?;
    decode_journal_file(&bytes)
}

fn decode_journal_file(bytes: &[u8]) -> Result<Vec<JournalFrame>> {
    let header = BinaryEnvelopeHeader::decode(bytes)?;
    if header.family != FileFamily::MetadataJournal {
        anyhow::bail!("control journal has wrong file family");
    }
    let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            anyhow::bail!("truncated control journal frame length");
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("invalid control journal frame length"))?;
        if input.len() < frame_end {
            anyhow::bail!("truncated control journal frame");
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
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

fn require_nonempty(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty() {
        return Err(anyhow!("{field} must not be empty"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
        assert!(
            storage
                .control_journal_path()
                .ends_with("_anvil/meta/control.anjournal")
        );
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
}
