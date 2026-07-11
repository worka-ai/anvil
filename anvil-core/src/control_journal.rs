use crate::core_store::{
    CF_MESH, CoreMetaTuplePart, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreStore, TABLE_CONTROL_CURRENT_ROW, core_meta_committed_row_common, core_meta_root_key_hash,
    core_meta_tuple_key,
};
use crate::formats::{Hash32, hash32};
use crate::partition_fence::{PartitionWritePermit, partition_write_precondition};
use crate::persistence::{App, AppDetails, Tenant};
use crate::storage::Storage;
use anyhow::{Result, anyhow, bail};
use prost::{Message, Oneof};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};

const CONTROL_EVENT_SCHEMA: &str = "anvil.control.event.v1";
const CONTROL_CURRENT_SCHEMA: &str = "anvil.control.current.v1";
const CONTROL_CURRENT_TARGET_MAX_BYTES: usize = 32 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ControlCurrentRecord {
    IdAllocator {
        max_allocated_id: i64,
    },
    Region {
        name: String,
        active: bool,
    },
    Tenant {
        id: i64,
        name: String,
        active: bool,
    },
    App {
        id: i64,
        tenant_id: i64,
        name: String,
        client_id: String,
        client_secret_encrypted: Vec<u8>,
        active: bool,
    },
}

#[derive(Debug, Clone, Default)]
pub struct ControlState {
    next_id: i64,
    regions: BTreeSet<String>,
    tenants: BTreeMap<i64, Tenant>,
    apps: BTreeMap<i64, StoredControlApp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredControlApp {
    id: i64,
    tenant_id: i64,
    name: String,
    client_id: String,
    client_secret_encrypted: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ControlEventProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    emitted_at: String,
    #[prost(uint64, tag = "3")]
    fence_token: u64,
    #[prost(string, tag = "4")]
    mutation_id: String,
    #[prost(oneof = "control_event_proto::Event", tags = "10, 11, 12, 13, 14")]
    event: Option<control_event_proto::Event>,
}

mod control_event_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Event {
        #[prost(message, tag = "10")]
        RegionUpsert(super::RegionUpsertProto),
        #[prost(message, tag = "11")]
        TenantUpsert(super::TenantUpsertProto),
        #[prost(message, tag = "12")]
        AppCreate(super::AppCreateProto),
        #[prost(message, tag = "13")]
        AppSecretUpdate(super::AppSecretUpdateProto),
        #[prost(message, tag = "14")]
        AppDelete(super::AppDeleteProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct ControlCurrentProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(oneof = "control_current_proto::Record", tags = "10, 11, 12, 13")]
    record: Option<control_current_proto::Record>,
}

mod control_current_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Record {
        #[prost(message, tag = "10")]
        IdAllocator(super::IdAllocatorCurrentProto),
        #[prost(message, tag = "11")]
        Region(super::RegionCurrentProto),
        #[prost(message, tag = "12")]
        Tenant(super::TenantCurrentProto),
        #[prost(message, tag = "13")]
        App(super::AppCurrentProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct RegionUpsertProto {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct TenantUpsertProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    name: String,
}

#[derive(Clone, PartialEq, Message)]
struct AppCreateProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    name: String,
    #[prost(string, tag = "4")]
    client_id: String,
    #[prost(bytes, tag = "5")]
    client_secret_encrypted: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct AppSecretUpdateProto {
    #[prost(int64, tag = "1")]
    app_id: i64,
    #[prost(bytes, tag = "2")]
    client_secret_encrypted: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct AppDeleteProto {
    #[prost(int64, tag = "1")]
    app_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct IdAllocatorCurrentProto {
    #[prost(int64, tag = "1")]
    max_allocated_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct RegionCurrentProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(bool, tag = "2")]
    active: bool,
}

#[derive(Clone, PartialEq, Message)]
struct TenantCurrentProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(bool, tag = "3")]
    active: bool,
}

#[derive(Clone, PartialEq, Message)]
struct AppCurrentProto {
    #[prost(int64, tag = "1")]
    id: i64,
    #[prost(int64, tag = "2")]
    tenant_id: i64,
    #[prost(string, tag = "3")]
    name: String,
    #[prost(string, tag = "4")]
    client_id: String,
    #[prost(bytes, tag = "5")]
    client_secret_encrypted: Vec<u8>,
    #[prost(bool, tag = "6")]
    active: bool,
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
}

pub async fn read_control_state(storage: &Storage) -> Result<ControlState> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_control_state_from_coremeta_rows(&core_store)
}

fn read_control_state_from_coremeta_rows(core_store: &CoreStore) -> Result<ControlState> {
    let mut state = ControlState::default();

    if let Some(value) = core_store.read_coremeta_row(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &id_allocator_tuple_key()?,
    )? {
        match decode_control_current_row(&value)? {
            ControlCurrentRecord::IdAllocator { max_allocated_id } => {
                state.next_id = max_allocated_id;
            }
            _ => bail!("control id allocator row contains a different record type"),
        }
    }

    for row in core_store.scan_coremeta_prefix(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &region_tuple_prefix()?,
    )? {
        match decode_control_current_row(&row.payload)? {
            ControlCurrentRecord::Region { name, active } => {
                if active {
                    state.regions.insert(name);
                }
            }
            _ => bail!("control region row contains a different record type"),
        }
    }

    for row in core_store.scan_coremeta_prefix(
        CF_MESH,
        TABLE_CONTROL_CURRENT_ROW,
        &tenant_tuple_prefix()?,
    )? {
        match decode_control_current_row(&row.payload)? {
            ControlCurrentRecord::Tenant { id, name, active } => {
                state.next_id = state.next_id.max(id);
                if active {
                    state.tenants.insert(id, Tenant { id, name });
                }
            }
            _ => bail!("control tenant row contains a different record type"),
        }
    }

    for row in
        core_store.scan_coremeta_prefix(CF_MESH, TABLE_CONTROL_CURRENT_ROW, &app_tuple_prefix()?)?
    {
        match decode_control_current_row(&row.payload)? {
            ControlCurrentRecord::App {
                id,
                tenant_id,
                name,
                client_id,
                client_secret_encrypted,
                active,
            } => {
                state.next_id = state.next_id.max(id);
                if active {
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
            }
            _ => bail!("control app row contains a different record type"),
        }
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
        vec![ControlCurrentRecord::Region {
            name: name.to_string(),
            active: true,
        }],
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
        vec![
            ControlCurrentRecord::IdAllocator {
                max_allocated_id: tenant.id,
            },
            ControlCurrentRecord::Tenant {
                id: tenant.id,
                name: tenant.name.clone(),
                active: true,
            },
        ],
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
        vec![
            ControlCurrentRecord::IdAllocator {
                max_allocated_id: app.id,
            },
            ControlCurrentRecord::App {
                id: app.id,
                tenant_id,
                name: app.name.clone(),
                client_id: app.client_id.clone(),
                client_secret_encrypted: encrypted_secret.to_vec(),
                active: true,
            },
        ],
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
    let existing = state
        .apps
        .get(&app_id)
        .ok_or_else(|| anyhow!("app not found"))?;
    append_control_event(
        storage,
        ControlEventBody::AppSecretUpdate {
            app_id,
            client_secret_encrypted: encrypted_secret.to_vec(),
        },
        vec![ControlCurrentRecord::App {
            id: existing.id,
            tenant_id: existing.tenant_id,
            name: existing.name.clone(),
            client_id: existing.client_id.clone(),
            client_secret_encrypted: encrypted_secret.to_vec(),
            active: true,
        }],
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
        vec![ControlCurrentRecord::App {
            id: app_id,
            tenant_id: 0,
            name: String::new(),
            client_id: String::new(),
            client_secret_encrypted: Vec::new(),
            active: false,
        }],
        fence_token,
        partition_precondition,
    )
    .await
}

async fn append_control_event(
    storage: &Storage,
    event: ControlEventBody,
    current_updates: Vec<ControlCurrentRecord>,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let mutation_id = uuid::Uuid::new_v4();
    let row_generation = current_unix_nanos();
    let payload = encode_control_event_body(&event, fence_token, mutation_id)?;
    let partition_id = hex::encode(control_partition_id());
    let mut operations = current_updates
        .into_iter()
        .map(|record| control_current_update(record, &mutation_id.to_string(), row_generation))
        .collect::<Result<Vec<_>>>()?;
    operations.push(CoreMutationOperation::StreamAppend {
        partition_id: partition_id.clone(),
        stream_id: control_plane_stream_id(),
        record_kind: "control_plane".to_string(),
        payload,
        idempotency_key: Some(format!("control-plane:{mutation_id}")),
    });
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("control-plane:{mutation_id}"),
            scope_partition: partition_id,
            committed_by_principal: control_partition_principal(),
            preconditions: partition_precondition.into_iter().collect(),
            operations,
        })
        .await?;
    Ok(())
}

#[cfg(test)]
#[cfg(test)]
async fn read_control_journal_bodies(storage: &Storage) -> Result<Vec<ControlEventBody>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_control_journal_bodies_from_store(&core_store).await
}

#[cfg(test)]
async fn read_control_journal_bodies_from_store(
    core_store: &CoreStore,
) -> Result<Vec<ControlEventBody>> {
    let records = core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: control_plane_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut bodies = Vec::new();
    for record in records {
        if record.record_kind != "control_plane" {
            continue;
        }
        bodies.push(decode_control_event_body(&record.payload)?);
    }
    Ok(bodies)
}

fn app_record(app: &StoredControlApp) -> App {
    App {
        id: app.id,
        name: app.name.clone(),
        client_id: app.client_id.clone(),
    }
}

fn control_current_update(
    record: ControlCurrentRecord,
    mutation_id: &str,
    row_generation: u64,
) -> Result<CoreMutationOperation> {
    Ok(CoreMutationOperation::CoreMetaPut {
        partition_id: hex::encode(control_partition_id()),
        cf: CF_MESH.to_string(),
        table_id: TABLE_CONTROL_CURRENT_ROW,
        tuple_key: control_current_tuple_key(&record)?,
        payload: encode_control_current_row(&record, mutation_id, row_generation)?,
    })
}

fn control_current_tuple_key(record: &ControlCurrentRecord) -> Result<Vec<u8>> {
    match record {
        ControlCurrentRecord::IdAllocator { .. } => id_allocator_tuple_key(),
        ControlCurrentRecord::Region { name, .. } => region_tuple_key(name),
        ControlCurrentRecord::Tenant { id, .. } => tenant_tuple_key(*id),
        ControlCurrentRecord::App { id, .. } => app_tuple_key(*id),
    }
}

fn encode_control_event_body(
    event: &ControlEventBody,
    fence_token: u64,
    mutation_id: uuid::Uuid,
) -> Result<Vec<u8>> {
    let proto = ControlEventProto {
        schema: CONTROL_EVENT_SCHEMA.to_string(),
        emitted_at: chrono::Utc::now().to_rfc3339(),
        fence_token,
        mutation_id: mutation_id.to_string(),
        event: Some(match event {
            ControlEventBody::RegionUpsert { name } => {
                control_event_proto::Event::RegionUpsert(RegionUpsertProto { name: name.clone() })
            }
            ControlEventBody::TenantUpsert { id, name } => {
                control_event_proto::Event::TenantUpsert(TenantUpsertProto {
                    id: *id,
                    name: name.clone(),
                })
            }
            ControlEventBody::AppCreate {
                id,
                tenant_id,
                name,
                client_id,
                client_secret_encrypted,
            } => control_event_proto::Event::AppCreate(AppCreateProto {
                id: *id,
                tenant_id: *tenant_id,
                name: name.clone(),
                client_id: client_id.clone(),
                client_secret_encrypted: client_secret_encrypted.clone(),
            }),
            ControlEventBody::AppSecretUpdate {
                app_id,
                client_secret_encrypted,
            } => control_event_proto::Event::AppSecretUpdate(AppSecretUpdateProto {
                app_id: *app_id,
                client_secret_encrypted: client_secret_encrypted.clone(),
            }),
            ControlEventBody::AppDelete { app_id } => {
                control_event_proto::Event::AppDelete(AppDeleteProto { app_id: *app_id })
            }
        }),
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    ensure_deterministic_control_proto(&proto, &bytes, "control event")?;
    if bytes.len() > CONTROL_CURRENT_TARGET_MAX_BYTES {
        bail!(
            "control event protobuf is {} bytes, exceeding {} bytes",
            bytes.len(),
            CONTROL_CURRENT_TARGET_MAX_BYTES
        );
    }
    Ok(bytes)
}

#[cfg(test)]
fn decode_control_event_body(bytes: &[u8]) -> Result<ControlEventBody> {
    let proto = ControlEventProto::decode(bytes)?;
    ensure_deterministic_control_proto(&proto, bytes, "control event")?;
    if proto.schema != CONTROL_EVENT_SCHEMA {
        bail!("control event protobuf has invalid schema");
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("control event protobuf has invalid mutation id"))?;
    match proto
        .event
        .ok_or_else(|| anyhow!("control event protobuf is missing event"))?
    {
        control_event_proto::Event::RegionUpsert(value) => {
            Ok(ControlEventBody::RegionUpsert { name: value.name })
        }
        control_event_proto::Event::TenantUpsert(value) => Ok(ControlEventBody::TenantUpsert {
            id: value.id,
            name: value.name,
        }),
        control_event_proto::Event::AppCreate(value) => Ok(ControlEventBody::AppCreate {
            id: value.id,
            tenant_id: value.tenant_id,
            name: value.name,
            client_id: value.client_id,
            client_secret_encrypted: value.client_secret_encrypted,
        }),
        control_event_proto::Event::AppSecretUpdate(value) => {
            Ok(ControlEventBody::AppSecretUpdate {
                app_id: value.app_id,
                client_secret_encrypted: value.client_secret_encrypted,
            })
        }
        control_event_proto::Event::AppDelete(value) => Ok(ControlEventBody::AppDelete {
            app_id: value.app_id,
        }),
    }
}

#[cfg(test)]
#[cfg(test)]
fn decode_control_event_body_fence(bytes: &[u8]) -> Result<u64> {
    let proto = ControlEventProto::decode(bytes)?;
    ensure_deterministic_control_proto(&proto, bytes, "control event")?;
    if proto.schema != CONTROL_EVENT_SCHEMA {
        bail!("control event protobuf has invalid schema");
    }
    let _mutation_id = uuid::Uuid::parse_str(&proto.mutation_id)
        .map_err(|_| anyhow!("control event protobuf has invalid mutation id"))?;
    Ok(proto.fence_token)
}

fn ensure_deterministic_control_proto(
    message: &impl Message,
    bytes: &[u8],
    label: &str,
) -> Result<()> {
    let mut canonical = Vec::with_capacity(message.encoded_len());
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} protobuf is not deterministic");
    }
    Ok(())
}

fn encode_control_current_row(
    record: &ControlCurrentRecord,
    mutation_id: &str,
    row_generation: u64,
) -> Result<Vec<u8>> {
    let proto = ControlCurrentProto {
        common: Some(core_meta_committed_row_common(
            control_current_realm_id(record),
            control_current_root_key_hash(record),
            row_generation,
            mutation_id.to_string(),
            row_generation,
        )),
        schema: CONTROL_CURRENT_SCHEMA.to_string(),
        record: Some(match record {
            ControlCurrentRecord::IdAllocator { max_allocated_id } => {
                control_current_proto::Record::IdAllocator(IdAllocatorCurrentProto {
                    max_allocated_id: *max_allocated_id,
                })
            }
            ControlCurrentRecord::Region { name, active } => {
                control_current_proto::Record::Region(RegionCurrentProto {
                    name: name.clone(),
                    active: *active,
                })
            }
            ControlCurrentRecord::Tenant { id, name, active } => {
                control_current_proto::Record::Tenant(TenantCurrentProto {
                    id: *id,
                    name: name.clone(),
                    active: *active,
                })
            }
            ControlCurrentRecord::App {
                id,
                tenant_id,
                name,
                client_id,
                client_secret_encrypted,
                active,
            } => control_current_proto::Record::App(AppCurrentProto {
                id: *id,
                tenant_id: *tenant_id,
                name: name.clone(),
                client_id: client_id.clone(),
                client_secret_encrypted: client_secret_encrypted.clone(),
                active: *active,
            }),
        }),
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    ensure_deterministic_control_proto(&proto, &bytes, "control current")?;
    if bytes.len() > CONTROL_CURRENT_TARGET_MAX_BYTES {
        bail!(
            "control current protobuf is {} bytes, exceeding {} bytes",
            bytes.len(),
            CONTROL_CURRENT_TARGET_MAX_BYTES
        );
    }
    Ok(bytes)
}

fn decode_control_current_row(bytes: &[u8]) -> Result<ControlCurrentRecord> {
    if bytes.len() > CONTROL_CURRENT_TARGET_MAX_BYTES {
        bail!(
            "control current protobuf is {} bytes, exceeding {} bytes",
            bytes.len(),
            CONTROL_CURRENT_TARGET_MAX_BYTES
        );
    }
    let proto = ControlCurrentProto::decode(bytes)?;
    ensure_deterministic_control_proto(&proto, bytes, "control current")?;
    if proto.schema != CONTROL_CURRENT_SCHEMA {
        bail!("control current protobuf has invalid schema");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("control current row missing CoreMeta common"))?;
    if common.root_key_hash.is_empty() {
        bail!("control current row missing root hash");
    }
    match proto
        .record
        .ok_or_else(|| anyhow!("control current protobuf is missing record"))?
    {
        control_current_proto::Record::IdAllocator(value) => {
            Ok(ControlCurrentRecord::IdAllocator {
                max_allocated_id: value.max_allocated_id,
            })
        }
        control_current_proto::Record::Region(value) => Ok(ControlCurrentRecord::Region {
            name: value.name,
            active: value.active,
        }),
        control_current_proto::Record::Tenant(value) => Ok(ControlCurrentRecord::Tenant {
            id: value.id,
            name: value.name,
            active: value.active,
        }),
        control_current_proto::Record::App(value) => Ok(ControlCurrentRecord::App {
            id: value.id,
            tenant_id: value.tenant_id,
            name: value.name,
            client_id: value.client_id,
            client_secret_encrypted: value.client_secret_encrypted,
            active: value.active,
        }),
    }
}

fn stable_suffix(bytes: &[u8]) -> String {
    hex::encode(hash32(bytes))
}

fn id_allocator_tuple_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("id-allocator"),
    ])
}

fn region_tuple_key(name: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("region"),
        CoreMetaTuplePart::Hash(&stable_suffix(name.as_bytes())),
        CoreMetaTuplePart::Utf8(name),
    ])
}

fn region_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("region"),
    ])
}

fn tenant_tuple_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("tenant"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn tenant_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("tenant"),
    ])
}

fn app_tuple_key(id: i64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("app"),
        CoreMetaTuplePart::I64(id),
    ])
}

fn app_tuple_prefix() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("control-current"),
        CoreMetaTuplePart::Utf8("app"),
    ])
}

fn control_current_realm_id(record: &ControlCurrentRecord) -> String {
    match record {
        ControlCurrentRecord::IdAllocator { .. } | ControlCurrentRecord::Region { .. } => {
            "system".to_string()
        }
        ControlCurrentRecord::Tenant { id, .. } => format!("tenant/{id}"),
        ControlCurrentRecord::App { tenant_id, .. } => format!("tenant/{tenant_id}"),
    }
}

fn control_current_root_key_hash(record: &ControlCurrentRecord) -> String {
    match record {
        ControlCurrentRecord::IdAllocator { .. } => core_meta_root_key_hash("control/id-allocator"),
        ControlCurrentRecord::Region { .. } => core_meta_root_key_hash("control/regions"),
        ControlCurrentRecord::Tenant { id, .. } => {
            core_meta_root_key_hash(&format!("control/tenant/{id}"))
        }
        ControlCurrentRecord::App { id, .. } => {
            core_meta_root_key_hash(&format!("control/app/{id}"))
        }
    }
}

fn current_unix_nanos() -> u64 {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    now.as_secs()
        .saturating_mul(1_000_000_000)
        .saturating_add(u64::from(now.subsec_nanos()))
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
    let core_store = CoreStore::new(storage.clone()).await?;
    Ok(core_store
        .read_stream(crate::core_store::ReadStream {
            stream_id: control_plane_stream_id(),
            after_sequence: 0,
            limit: 0,
        })
        .await?
        .into_iter()
        .filter(|record| record.record_kind == "control_plane")
        .map(|record| decode_control_event_body_fence(&record.payload))
        .collect::<Result<Vec<_>>>()?)
}

async fn control_write_precondition(
    storage: &Storage,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<CoreMutationPrecondition> {
    require_control_permit(permit)?;
    Ok(partition_write_precondition(storage, permit, partition_owner_signing_key).await?)
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
    async fn control_state_reads_current_rows_and_keeps_history_for_watch() {
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
        update_app_secret(&storage, app.id, b"secret-b")
            .await
            .unwrap();

        let current = read_control_state(&storage).await.unwrap();
        assert_eq!(current.regions(), vec!["local".to_string()]);
        assert_eq!(current.tenant_by_name("default").unwrap().id, tenant.id);
        assert_eq!(current.app_by_name("demo").unwrap().id, app.id);
        assert_eq!(
            current.app_details_by_client_id("client-a").unwrap().id,
            app.id
        );
        assert_eq!(
            current
                .app_details_by_client_id("client-a")
                .unwrap()
                .client_secret_encrypted,
            b"secret-b".to_vec()
        );
        let bodies = read_control_journal_bodies(&storage).await.unwrap();
        assert_eq!(bodies.len(), 4);
        assert!(matches!(bodies[0], ControlEventBody::RegionUpsert { .. }));
    }

    #[tokio::test]
    async fn control_current_state_does_not_replay_control_history_stream() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let event = ControlEventBody::TenantUpsert {
            id: 41,
            name: "history-only".to_string(),
        };
        let mutation_id = uuid::Uuid::new_v4();
        let payload = encode_control_event_body(&event, 0, mutation_id).unwrap();
        core_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: format!("history-only:{mutation_id}"),
                scope_partition: hex::encode(control_partition_id()),
                committed_by_principal: control_partition_principal(),
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: hex::encode(control_partition_id()),
                    stream_id: control_plane_stream_id(),
                    record_kind: "control_plane".to_string(),
                    payload,
                    idempotency_key: Some(format!("history-only:{mutation_id}")),
                }],
            })
            .await
            .unwrap();

        assert_eq!(
            read_control_journal_bodies(&storage).await.unwrap().len(),
            1
        );
        let state = read_control_state(&storage).await.unwrap();
        assert!(state.tenants().is_empty());
        assert!(state.tenant_by_name("history-only").is_none());
    }

    #[tokio::test]
    async fn control_current_rows_are_sufficient_without_control_history_stream() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let core_store = CoreStore::new(storage.clone()).await.unwrap();
        let tenant = Tenant {
            id: 1,
            name: "default".to_string(),
        };
        let app = StoredControlApp {
            id: 2,
            tenant_id: tenant.id,
            name: "demo".to_string(),
            client_id: "client-a".to_string(),
            client_secret_encrypted: b"secret-a".to_vec(),
        };
        core_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: "current-row-seed".to_string(),
                scope_partition: hex::encode(control_partition_id()),
                committed_by_principal: control_partition_principal(),
                preconditions: Vec::new(),
                operations: vec![
                    ControlCurrentRecord::IdAllocator {
                        max_allocated_id: app.id,
                    },
                    ControlCurrentRecord::Region {
                        name: "local".to_string(),
                        active: true,
                    },
                    ControlCurrentRecord::Tenant {
                        id: tenant.id,
                        name: tenant.name.clone(),
                        active: true,
                    },
                    ControlCurrentRecord::App {
                        id: app.id,
                        tenant_id: app.tenant_id,
                        name: app.name.clone(),
                        client_id: app.client_id.clone(),
                        client_secret_encrypted: app.client_secret_encrypted.clone(),
                        active: true,
                    },
                ]
                .into_iter()
                .map(|record| {
                    control_current_update(record, "current-row-seed", current_unix_nanos())
                })
                .collect::<Result<Vec<_>>>()
                .unwrap(),
            })
            .await
            .unwrap();

        assert!(
            read_control_journal_bodies(&storage)
                .await
                .unwrap()
                .is_empty()
        );
        let state = read_control_state(&storage).await.unwrap();
        assert_eq!(state.regions(), vec!["local".to_string()]);
        let loaded_tenant = state.tenant_by_name("default").unwrap();
        assert_eq!(loaded_tenant.id, tenant.id);
        assert_eq!(loaded_tenant.name, tenant.name);
        assert_eq!(state.app_by_id(app.id).unwrap().client_id, app.client_id);
        assert_eq!(state.allocate_id(), 3);
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
    pub(crate) async fn control_journal_with_permit_writes_fenced_payloads_and_current_rows() {
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
        update_app_secret_with_permit(&storage, app.id, b"secret-b", &permit, KEY)
            .await
            .unwrap();

        let fences = read_control_frame_fences_for_test(&storage).await.unwrap();
        assert_eq!(fences.len(), 4);
        assert!(fences.iter().all(|fence| *fence == permit.fence_token));
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
        let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
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
