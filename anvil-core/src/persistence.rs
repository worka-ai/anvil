use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
    sync::Arc,
};
use tokio::sync::{RwLock, mpsc::Sender};

use crate::{
    authz_journal,
    bucket_journal::{self, BucketJournalMutation},
    cache::MetadataCache,
    cluster::MetadataEvent,
    config::Config,
    control_journal, index_journal, model_journal,
    storage::Storage,
    task_journal,
};

#[derive(Debug, Clone)]
pub struct Persistence {
    state_path: PathBuf,
    storage: Storage,
    state: Arc<RwLock<NativeState>>,
    cache: MetadataCache,
    event_publisher: Option<Sender<MetadataEvent>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct NativeState {
    next_id: i64,
    objects: Vec<Object>,
    multipart_uploads: Vec<MultipartUpload>,
    multipart_parts: Vec<MultipartUploadPart>,
    object_events: Vec<ObjectWatchEvent>,
    append_streams: Vec<AppendStream>,
    append_records: Vec<AppendStreamRecord>,
    manifests: Vec<ManifestRecord>,
    index_diagnostics: Vec<IndexDiagnostic>,
    hf_keys: Vec<HfKey>,
    hf_ingestions: Vec<HfIngestion>,
    hf_items: Vec<HfIngestionItem>,
}

impl NativeState {
    fn allocate_id(&mut self) -> i64 {
        self.next_id += 1;
        self.next_id
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ManifestRecord {
    tenant_id: i64,
    bucket_id: i64,
    object_key: String,
    revision: i64,
    manifest_hash: String,
    manifest: JsonValue,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HfKey {
    id: i64,
    name: String,
    token_encrypted: Vec<u8>,
    note: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HfIngestion {
    id: i64,
    key_id: i64,
    tenant_id: i64,
    requester_app_id: i64,
    repo: String,
    revision: String,
    target_bucket: String,
    target_region: String,
    target_prefix: String,
    include_globs: Vec<String>,
    exclude_globs: Vec<String>,
    state: crate::tasks::HFIngestionState,
    error: Option<String>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct HfIngestionItem {
    id: i64,
    ingestion_id: i64,
    path: String,
    size: Option<i64>,
    etag: Option<String>,
    state: crate::tasks::HFIngestionItemState,
    error: Option<String>,
    created_at: DateTime<Utc>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tenant {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct App {
    pub id: i64,
    pub name: String,
    pub client_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub region: String,
    pub created_at: DateTime<Utc>,
    pub is_public_read: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketMetadataEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub event_type: String,
    pub bucket_metadata: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Object {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub key: String,
    pub content_hash: String,
    pub size: i64,
    pub etag: String,
    pub content_type: Option<String>,
    pub version_id: uuid::Uuid,
    pub mutation_id: uuid::Uuid,
    pub index_policy_snapshot: String,
    pub user_metadata_hash: String,
    pub authz_revision: i64,
    pub record_hash: String,
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub storage_class: Option<i16>,
    pub user_meta: Option<JsonValue>,
    pub shard_map: Option<JsonValue>,
    pub inline_payload: Option<Vec<u8>>,
    pub checksum: Option<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub object: Object,
    pub is_delete_marker: bool,
    pub is_latest: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersionsPage {
    pub versions: Vec<ObjectVersion>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_version_id_marker: Option<uuid::Uuid>,
}

struct ObjectVersionRecordHashInput<'a> {
    tenant_id: i64,
    bucket_id: i64,
    key: &'a str,
    version_id: uuid::Uuid,
    mutation_id: uuid::Uuid,
    content_hash: &'a str,
    size: i64,
    etag: &'a str,
    content_type: Option<&'a str>,
    user_metadata_hash: &'a str,
    index_policy_snapshot: &'a str,
    authz_revision: i64,
    delete_marker: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub key: String,
    pub upload_id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub aborted_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadsPage {
    pub uploads: Vec<MultipartUpload>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<uuid::Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUploadPart {
    pub id: i64,
    pub upload_id: i64,
    pub part_number: i32,
    pub content_hash: String,
    pub size: i64,
    pub etag: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartPartsPage {
    pub parts: Vec<MultipartUploadPart>,
    pub is_truncated: bool,
    pub next_part_number_marker: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectWatchEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub key: String,
    pub event_type: String,
    pub version_id: Option<uuid::Uuid>,
    pub etag: Option<String>,
    pub size: i64,
    pub is_delete_marker: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendStream {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub stream_key: String,
    pub stream_id: uuid::Uuid,
    pub created_at: DateTime<Utc>,
    pub sealed_at: Option<DateTime<Utc>>,
    pub segment_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppendStreamRecord {
    pub id: i64,
    pub stream_id: i64,
    pub record_sequence: i64,
    pub payload_hash: String,
    pub payload_size: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCasResult {
    pub revision: i64,
    pub manifest_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzTupleRecord {
    pub revision: i64,
    pub tenant_id: i64,
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub operation: String,
    pub written_by: String,
    pub reason: String,
    pub record_hash: String,
    pub written_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinition {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub name: String,
    pub kind: String,
    pub selector: JsonValue,
    pub extractor: JsonValue,
    pub authorization_mode: String,
    pub build_policy: JsonValue,
    pub enabled: bool,
    pub version: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDefinitionEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub index_id: i64,
    pub index_name: String,
    pub event_type: String,
    pub index_version: i64,
    pub definition: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDiagnostic {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub index_id: Option<i64>,
    pub index_name: String,
    pub object_key: String,
    pub version_id: Option<uuid::Uuid>,
    pub severity: String,
    pub code: String,
    pub message: String,
    pub details: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppDetails {
    pub id: i64,
    pub client_secret_encrypted: Vec<u8>,
    pub tenant_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminUser {
    pub id: i64,
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub is_active: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdminRole {
    pub id: i32,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRecord {
    pub id: i64,
    pub task_type: crate::tasks::TaskType,
    pub payload: JsonValue,
    pub priority: i32,
    pub status: crate::tasks::TaskStatus,
    pub attempts: i32,
    pub last_error: Option<String>,
    pub scheduled_at: DateTime<Utc>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct HfIngestionJob {
    pub key_id: i64,
    pub tenant_id: i64,
    pub requester_app_id: i64,
    pub repo: String,
    pub revision: String,
    pub target_bucket: String,
    pub target_region: String,
    pub target_prefix: String,
    pub include_globs: Vec<String>,
    pub exclude_globs: Vec<String>,
}

fn object_version_record_hash(input: ObjectVersionRecordHashInput<'_>) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&input.tenant_id.to_le_bytes());
    hasher.update(&input.bucket_id.to_le_bytes());
    hasher.update(input.key.as_bytes());
    hasher.update(input.version_id.as_bytes());
    hasher.update(input.mutation_id.as_bytes());
    hasher.update(input.content_hash.as_bytes());
    hasher.update(&input.size.to_le_bytes());
    hasher.update(input.etag.as_bytes());
    if let Some(content_type) = input.content_type {
        hasher.update(content_type.as_bytes());
    }
    hasher.update(&[0]);
    hasher.update(input.user_metadata_hash.as_bytes());
    hasher.update(input.index_policy_snapshot.as_bytes());
    hasher.update(&input.authz_revision.to_le_bytes());
    hasher.update(&[u8::from(input.delete_marker)]);
    hasher.finalize().to_hex().to_string()
}

fn user_metadata_hash(user_meta: Option<&JsonValue>) -> String {
    let Some(user_meta) = user_meta else {
        return blake3::hash(&[]).to_hex().to_string();
    };
    blake3::hash(&canonical_json_bytes(user_meta))
        .to_hex()
        .to_string()
}

fn canonical_json_bytes(value: &JsonValue) -> Vec<u8> {
    match value {
        JsonValue::Null => b"null".to_vec(),
        JsonValue::Bool(value) => {
            if *value {
                b"true".to_vec()
            } else {
                b"false".to_vec()
            }
        }
        JsonValue::Number(value) => value.to_string().into_bytes(),
        JsonValue::String(value) => serde_json::to_vec(value).unwrap_or_default(),
        JsonValue::Array(values) => {
            let mut out = vec![b'['];
            for (idx, value) in values.iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&canonical_json_bytes(value));
            }
            out.push(b']');
            out
        }
        JsonValue::Object(values) => {
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            let mut out = vec![b'{'];
            for (idx, key) in keys.into_iter().enumerate() {
                if idx > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(&serde_json::to_vec(key).unwrap_or_default());
                out.push(b':');
                out.extend_from_slice(&canonical_json_bytes(&values[key]));
            }
            out.push(b'}');
            out
        }
    }
}

impl Persistence {
    pub fn new(config: &Config, event_publisher: Option<Sender<MetadataEvent>>) -> Result<Self> {
        let storage = Storage::new_at_sync(&config.storage_path)?;
        let state_path = PathBuf::from(&config.storage_path)
            .join("_anvil")
            .join("meta")
            .join("native-state.json");
        if let Some(parent) = state_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let state = if state_path.exists() {
            let bytes = std::fs::read(&state_path)?;
            if bytes.is_empty() {
                NativeState::default()
            } else {
                serde_json::from_slice(&bytes)?
            }
        } else {
            NativeState::default()
        };
        Ok(Self {
            state_path,
            storage,
            state: Arc::new(RwLock::new(state)),
            cache: MetadataCache::new(config),
            event_publisher,
        })
    }

    async fn persist_state(&self, state: NativeState) -> Result<()> {
        if let Some(parent) = self.state_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = self.state_path.with_extension("json.tmp");
        let bytes = serde_json::to_vec_pretty(&state)?;
        tokio::fs::write(&tmp, bytes).await?;
        tokio::fs::rename(&tmp, &self.state_path).await?;
        Ok(())
    }

    async fn persist_after_write(&self, state: &NativeState) -> Result<()> {
        self.persist_state(state.clone()).await
    }

    async fn refresh_from_disk(&self) -> Result<()> {
        let bytes = match tokio::fs::read(&self.state_path).await {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        if bytes.is_empty() {
            return Ok(());
        }
        *self.state.write().await = serde_json::from_slice(&bytes)?;
        Ok(())
    }

    async fn publish_event(&self, event: MetadataEvent) {
        if let Some(sender) = &self.event_publisher {
            let _ = sender.send(event).await;
        }
    }

    pub fn cache(&self) -> &MetadataCache {
        &self.cache
    }

    pub async fn get_admin_user_by_username(&self, username: &str) -> Result<Option<AdminUser>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .admin_user_by_username(username))
    }

    pub async fn get_admin_user_by_id(&self, id: i64) -> Result<Option<AdminUser>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .admin_user_by_id(id))
    }

    pub async fn get_roles_for_admin_user(&self, user_id: i64) -> Result<Vec<String>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .roles_for_admin_user(user_id))
    }

    pub async fn create_admin_user(
        &self,
        username: &str,
        email: &str,
        password_hash: &str,
        role_names: &[String],
    ) -> Result<AdminUser> {
        control_journal::create_admin_user(
            &self.storage,
            username,
            email,
            password_hash,
            role_names,
        )
        .await
    }

    pub async fn update_admin_user(
        &self,
        user_id: i64,
        username: &str,
        email: &str,
        password_hash: Option<&str>,
        is_active: bool,
        role_names: &[String],
    ) -> Result<()> {
        control_journal::update_admin_user(
            &self.storage,
            user_id,
            username,
            email,
            password_hash,
            is_active,
            role_names,
        )
        .await
    }

    pub async fn delete_admin_user(&self, user_id: i64) -> Result<()> {
        control_journal::delete_admin_user(&self.storage, user_id).await
    }

    pub async fn list_admin_users(&self) -> Result<Vec<AdminUser>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .admin_users())
    }

    pub async fn create_admin_role(&self, name: &str) -> Result<()> {
        control_journal::create_admin_role(&self.storage, name).await
    }

    pub async fn list_admin_roles(&self) -> Result<Vec<String>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .admin_roles())
    }

    pub async fn get_admin_role_by_id(&self, id: i32) -> Result<Option<AdminRole>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .admin_role_by_id(id))
    }

    pub async fn update_admin_role(&self, id: i32, name: &str) -> Result<()> {
        control_journal::update_admin_role(&self.storage, id, name).await
    }

    pub async fn delete_admin_role(&self, id: i32) -> Result<()> {
        control_journal::delete_admin_role(&self.storage, id).await
    }

    pub async fn list_policies(&self) -> Result<Vec<String>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .policy_summaries())
    }

    pub async fn create_model_artifact(
        &self,
        artifact_id: &str,
        bucket_id: i64,
        key: &str,
        manifest: &crate::anvil_api::ModelManifest,
    ) -> Result<()> {
        model_journal::create_model_artifact(&self.storage, artifact_id, bucket_id, key, manifest)
            .await
    }

    pub async fn create_model_tensors(
        &self,
        artifact_id: &str,
        tensors: &[crate::anvil_api::TensorIndexRow],
    ) -> Result<()> {
        model_journal::create_model_tensors(&self.storage, artifact_id, tensors).await
    }

    pub async fn list_tensors(
        &self,
        artifact_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<crate::anvil_api::TensorIndexRow>> {
        model_journal::list_tensors(&self.storage, artifact_id, limit, offset).await
    }

    pub async fn get_tensor_metadata(
        &self,
        artifact_id: &str,
        tensor_name: &str,
    ) -> Result<Option<crate::anvil_api::TensorIndexRow>> {
        model_journal::get_tensor_metadata(&self.storage, artifact_id, tensor_name).await
    }

    pub async fn get_model_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<crate::anvil_api::ModelManifest>> {
        model_journal::get_model_artifact(&self.storage, artifact_id).await
    }

    pub async fn get_tensor_metadata_recursive(
        &self,
        artifact_id: &str,
        tensor_name: &str,
    ) -> Result<Option<crate::anvil_api::TensorIndexRow>> {
        let mut current = artifact_id.to_string();
        let mut seen = HashSet::new();
        while seen.insert(current.clone()) {
            if let Some(tensor) = self.get_tensor_metadata(&current, tensor_name).await? {
                return Ok(Some(tensor));
            }
            let Some(manifest) = self.get_model_artifact(&current).await? else {
                break;
            };
            if manifest.base_artifact_id.is_empty() {
                break;
            }
            current = manifest.base_artifact_id;
        }
        Ok(None)
    }

    pub async fn create_region(&self, name: &str) -> Result<bool> {
        control_journal::create_region(&self.storage, name).await
    }

    pub async fn list_regions(&self) -> Result<Vec<String>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .regions())
    }

    pub async fn get_tenant_by_name(&self, name: &str) -> Result<Option<Tenant>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .tenant_by_name(name))
    }

    pub async fn list_tenants(&self) -> Result<Vec<Tenant>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .tenants())
    }

    pub async fn get_app_by_client_id(&self, client_id: &str) -> Result<Option<AppDetails>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .app_details_by_client_id(client_id))
    }

    pub async fn get_policies_for_app(&self, app_id: i64) -> Result<Vec<String>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .policies_for_app(app_id))
    }

    pub async fn create_tenant(&self, name: &str, _api_key: &str) -> Result<Tenant> {
        control_journal::create_tenant(&self.storage, name).await
    }

    pub async fn create_app(
        &self,
        tenant_id: i64,
        name: &str,
        client_id: &str,
        encrypted_secret: &[u8],
    ) -> Result<App> {
        control_journal::create_app(&self.storage, tenant_id, name, client_id, encrypted_secret)
            .await
    }

    pub async fn get_app_by_id(&self, id: i64) -> Result<Option<App>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .app_by_id(id))
    }

    pub async fn get_app_by_name(&self, name: &str) -> Result<Option<App>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .app_by_name(name))
    }

    pub async fn list_apps_for_tenant(&self, tenant_id: i64) -> Result<Vec<App>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .apps_for_tenant(tenant_id))
    }

    pub async fn update_app_secret(&self, app_id: i64, new_encrypted_secret: &[u8]) -> Result<()> {
        control_journal::update_app_secret(&self.storage, app_id, new_encrypted_secret).await
    }

    pub async fn grant_policy(&self, app_id: i64, resource: &str, action: &str) -> Result<()> {
        control_journal::grant_policy(&self.storage, app_id, resource, action).await
    }

    pub async fn revoke_policy(&self, app_id: i64, resource: &str, action: &str) -> Result<()> {
        control_journal::revoke_policy(&self.storage, app_id, resource, action).await
    }

    pub async fn create_bucket(
        &self,
        tenant_id: i64,
        name: &str,
        region: &str,
    ) -> Result<Bucket, tonic::Status> {
        if bucket_journal::read_current_bucket(&self.storage, tenant_id, name)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .is_some()
        {
            return Err(tonic::Status::already_exists(
                "A bucket with that name already exists.",
            ));
        }
        let bucket = Bucket {
            id: bucket_journal::next_bucket_id(&self.storage)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
            tenant_id,
            name: name.to_string(),
            region: region.to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        bucket_journal::append_bucket_mutation(
            &self.storage,
            &bucket,
            BucketJournalMutation::Create,
        )
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        self.cache
            .insert_bucket(tenant_id, name.to_string(), bucket.clone())
            .await;
        self.publish_event(MetadataEvent::BucketUpdated {
            tenant_id,
            name: name.to_string(),
        })
        .await;
        Ok(bucket)
    }

    pub async fn get_bucket_by_name(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        if let Some(bucket) = self.cache.get_bucket(tenant_id, name).await {
            return Ok(Some(bucket));
        }
        let bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, name).await?;
        if let Some(bucket) = bucket.clone() {
            self.cache
                .insert_bucket(tenant_id, name.to_string(), bucket)
                .await;
        }
        Ok(bucket)
    }

    pub async fn get_public_bucket_by_name(&self, name: &str) -> Result<Option<Bucket>> {
        bucket_journal::read_public_bucket_by_name(&self.storage, name).await
    }

    pub async fn set_bucket_public_access(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        is_public: bool,
    ) -> Result<Bucket> {
        let mut out = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        out.is_public_read = is_public;
        bucket_journal::append_bucket_mutation(&self.storage, &out, BucketJournalMutation::Update)
            .await?;
        self.cache.invalidate_bucket(tenant_id, bucket_name).await;
        Ok(out)
    }

    pub async fn set_bucket_public_access_by_name(
        &self,
        bucket_name: &str,
        is_public: bool,
    ) -> Result<Bucket> {
        let mut out = bucket_journal::read_current_bucket_by_name(&self.storage, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        out.is_public_read = is_public;
        bucket_journal::append_bucket_mutation(&self.storage, &out, BucketJournalMutation::Update)
            .await?;
        self.cache
            .invalidate_bucket(out.tenant_id, bucket_name)
            .await;
        Ok(out)
    }

    pub async fn soft_delete_bucket(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        let deleted = bucket_journal::read_current_bucket(&self.storage, tenant_id, name).await?;
        if let Some(bucket) = &deleted {
            bucket_journal::append_bucket_mutation(
                &self.storage,
                bucket,
                BucketJournalMutation::Delete,
            )
            .await?;
        }
        self.cache.invalidate_bucket(tenant_id, name).await;
        Ok(deleted)
    }

    pub async fn bucket_has_retained_objects_or_uploads(&self, bucket_id: i64) -> Result<bool> {
        let state = self.state.read().await;
        Ok(state.objects.iter().any(|o| o.bucket_id == bucket_id)
            || state.multipart_uploads.iter().any(|u| {
                u.bucket_id == bucket_id && u.completed_at.is_none() && u.aborted_at.is_none()
            }))
    }

    pub async fn hard_delete_bucket_if_empty(&self, bucket_id: i64) -> Result<bool> {
        let state = self.state.read().await;
        if state.objects.iter().any(|o| o.bucket_id == bucket_id)
            || state.multipart_uploads.iter().any(|u| {
                u.bucket_id == bucket_id && u.completed_at.is_none() && u.aborted_at.is_none()
            })
        {
            return Ok(false);
        }
        Ok(true)
    }

    pub async fn create_bucket_metadata_event(
        &self,
        tenant_id: i64,
        bucket: &Bucket,
        event_type: &str,
        bucket_metadata: JsonValue,
    ) -> Result<BucketMetadataEvent> {
        bucket_journal::latest_bucket_metadata_event(&self.storage, tenant_id, &bucket.name)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "bucket metadata event not found after {event_type}: {}",
                    bucket_metadata
                )
            })
    }

    pub async fn list_bucket_metadata_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<BucketMetadataEvent>> {
        bucket_journal::list_bucket_metadata_events_by_bucket_id(
            &self.storage,
            tenant_id,
            bucket_id,
            after_cursor,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }

    pub async fn list_buckets_for_tenant(&self, tenant_id: i64) -> Result<Vec<Bucket>> {
        let mut buckets = bucket_journal::read_current_buckets(&self.storage, tenant_id).await?;
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    pub async fn active_index_policy_snapshot_hash(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<String> {
        let defs = index_journal::read_current_index_definitions(
            &self.storage,
            tenant_id,
            bucket_id,
            false,
        )
        .await?;
        Ok(blake3::hash(&serde_json::to_vec(
            &defs
                .iter()
                .map(|d| (&d.name, &d.kind, d.version))
                .collect::<Vec<_>>(),
        )?)
        .to_hex()
        .to_string())
    }

    pub async fn latest_authz_revision(&self, tenant_id: i64) -> Result<i64> {
        authz_journal::latest_authz_revision(&self.storage, tenant_id).await
    }

    pub async fn create_object(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        content_hash: &str,
        size: i64,
        etag: &str,
        content_type: Option<&str>,
        user_meta: Option<JsonValue>,
        shard_map: Option<JsonValue>,
        inline_payload: Option<Vec<u8>>,
    ) -> Result<Object> {
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let index_policy_snapshot = self
            .active_index_policy_snapshot_hash(tenant_id, bucket_id)
            .await?;
        let user_metadata_hash = user_metadata_hash(user_meta.as_ref());
        let authz_revision = self.latest_authz_revision(tenant_id).await?;
        let record_hash = object_version_record_hash(ObjectVersionRecordHashInput {
            tenant_id,
            bucket_id,
            key,
            version_id,
            mutation_id,
            content_hash,
            size,
            etag,
            content_type,
            user_metadata_hash: &user_metadata_hash,
            index_policy_snapshot: &index_policy_snapshot,
            authz_revision,
            delete_marker: false,
        });
        let mut state = self.state.write().await;
        let object = Object {
            id: state.allocate_id(),
            tenant_id,
            bucket_id,
            key: key.to_string(),
            content_hash: content_hash.to_string(),
            size,
            etag: etag.to_string(),
            content_type: content_type.map(ToOwned::to_owned),
            version_id,
            mutation_id,
            index_policy_snapshot,
            user_metadata_hash,
            authz_revision,
            record_hash,
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: None,
            user_meta,
            shard_map,
            inline_payload,
            checksum: None,
        };
        state.objects.push(object.clone());
        self.persist_after_write(&state).await?;
        Ok(object)
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let mut versions = self
            .state
            .read()
            .await
            .objects
            .iter()
            .filter(|o| o.bucket_id == bucket_id && o.key == key)
            .cloned()
            .collect::<Vec<_>>();
        versions.sort_by(|a, b| {
            a.created_at
                .cmp(&b.created_at)
                .then_with(|| a.id.cmp(&b.id))
        });
        Ok(versions.last().filter(|o| o.deleted_at.is_none()).cloned())
    }

    pub async fn get_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        Ok(self
            .state
            .read()
            .await
            .objects
            .iter()
            .find(|o| o.bucket_id == bucket_id && o.key == key && o.version_id == version_id)
            .cloned())
    }

    pub async fn list_objects(
        &self,
        bucket_id: i64,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>)> {
        let state = self.state.read().await;
        let mut latest: BTreeMap<String, Object> = BTreeMap::new();
        for object in state.objects.iter().filter(|o| {
            o.bucket_id == bucket_id
                && o.key.starts_with(prefix)
                && o.key.as_str() > start_after
                && !crate::validation::is_reserved_internal_key(&o.key)
        }) {
            latest
                .entry(object.key.clone())
                .and_modify(|existing| {
                    if (object.created_at, object.id) > (existing.created_at, existing.id) {
                        *existing = object.clone();
                    }
                })
                .or_insert_with(|| object.clone());
        }
        let mut objects = latest
            .into_values()
            .filter(|o| o.deleted_at.is_none())
            .collect::<Vec<_>>();
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        let limit = if limit == 0 {
            1000
        } else {
            limit.max(1) as usize
        };
        if delimiter.is_empty() {
            objects.truncate(limit);
            return Ok((objects, Vec::new()));
        }
        let mut object_by_key = BTreeMap::<String, Object>::new();
        let mut prefixes = BTreeMap::<String, ()>::new();
        for object in objects {
            let suffix = &object.key[prefix.len()..];
            if let Some(pos) = suffix.find(delimiter) {
                prefixes.insert(
                    format!("{}{}", prefix, &suffix[..pos + delimiter.len()]),
                    (),
                );
            } else {
                object_by_key.insert(object.key.clone(), object);
            }
        }
        let mut merged = object_by_key
            .keys()
            .cloned()
            .map(|k| (k, false))
            .chain(prefixes.keys().cloned().map(|p| (p, true)))
            .collect::<Vec<_>>();
        merged.sort();
        let mut out_objects = Vec::new();
        let mut out_prefixes = Vec::new();
        for (key, is_prefix) in merged.into_iter().take(limit) {
            if is_prefix {
                out_prefixes.push(key);
            } else if let Some(object) = object_by_key.remove(&key) {
                out_objects.push(object);
            }
        }
        Ok((out_objects, out_prefixes))
    }

    pub async fn soft_delete_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let mut state = self.state.write().await;
        let Some(base) = state
            .objects
            .iter()
            .filter(|o| o.bucket_id == bucket_id && o.key == key && o.deleted_at.is_none())
            .max_by(|a, b| {
                a.created_at
                    .cmp(&b.created_at)
                    .then_with(|| a.id.cmp(&b.id))
            })
            .cloned()
        else {
            return Ok(None);
        };
        let now = Utc::now();
        let object = Object {
            id: state.allocate_id(),
            mutation_id: uuid::Uuid::new_v4(),
            version_id: uuid::Uuid::new_v4(),
            content_hash: String::new(),
            size: 0,
            etag: String::new(),
            created_at: now,
            deleted_at: Some(now),
            ..base
        };
        state.objects.push(object.clone());
        self.persist_after_write(&state).await?;
        Ok(Some(object))
    }

    pub async fn delete_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let mut state = self.state.write().await;
        let Some(pos) = state
            .objects
            .iter()
            .position(|o| o.bucket_id == bucket_id && o.key == key && o.version_id == version_id)
        else {
            return Ok(None);
        };
        let mut object = state.objects.remove(pos);
        object.deleted_at = Some(Utc::now());
        self.persist_after_write(&state).await?;
        Ok(Some(object))
    }

    pub async fn list_object_versions(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage> {
        let state = self.state.read().await;
        let mut by_key: BTreeMap<String, Vec<Object>> = BTreeMap::new();
        for object in state
            .objects
            .iter()
            .filter(|o| o.bucket_id == bucket_id && o.key.starts_with(prefix))
        {
            by_key
                .entry(object.key.clone())
                .or_default()
                .push(object.clone());
        }
        let mut flat = Vec::new();
        for versions in by_key.values_mut() {
            versions.sort_by(|a, b| {
                b.created_at
                    .cmp(&a.created_at)
                    .then_with(|| b.id.cmp(&a.id))
            });
            for (idx, object) in versions.iter().enumerate() {
                flat.push(ObjectVersion {
                    object: object.clone(),
                    is_delete_marker: object.deleted_at.is_some(),
                    is_latest: idx == 0,
                });
            }
        }
        flat.sort_by(|a, b| {
            a.object
                .key
                .cmp(&b.object.key)
                .then_with(|| b.object.created_at.cmp(&a.object.created_at))
        });
        if !key_marker.is_empty() {
            let mut past_marker = version_id_marker.is_none();
            flat.retain(|v| {
                if v.object.key.as_str() < key_marker {
                    return false;
                }
                if v.object.key.as_str() > key_marker {
                    return true;
                }
                if let Some(marker) = version_id_marker {
                    if past_marker {
                        return true;
                    }
                    if v.object.version_id == marker {
                        past_marker = true;
                    }
                    return false;
                }
                true
            });
        }
        let limit = if limit == 0 {
            1000
        } else {
            limit.max(1) as usize
        };
        let is_truncated = flat.len() > limit;
        if is_truncated {
            flat.truncate(limit);
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            flat.last()
                .map(|v| (Some(v.object.key.clone()), Some(v.object.version_id)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        Ok(ObjectVersionsPage {
            versions: flat,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    pub async fn create_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
    ) -> Result<MultipartUpload> {
        let mut state = self.state.write().await;
        let upload = MultipartUpload {
            id: state.allocate_id(),
            tenant_id,
            bucket_id,
            key: key.to_string(),
            upload_id: uuid::Uuid::new_v4(),
            created_at: Utc::now(),
            completed_at: None,
            aborted_at: None,
        };
        state.multipart_uploads.push(upload.clone());
        self.persist_after_write(&state).await?;
        Ok(upload)
    }

    pub async fn get_active_multipart_upload(
        &self,
        _tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<Option<MultipartUpload>> {
        Ok(self
            .state
            .read()
            .await
            .multipart_uploads
            .iter()
            .find(|u| {
                u.bucket_id == bucket_id
                    && u.key == key
                    && u.upload_id == upload_id
                    && u.completed_at.is_none()
                    && u.aborted_at.is_none()
            })
            .cloned())
    }

    pub async fn upsert_multipart_part(
        &self,
        upload_row_id: i64,
        part_number: i32,
        content_hash: &str,
        size: i64,
        etag: &str,
    ) -> Result<MultipartUploadPart> {
        let mut state = self.state.write().await;
        if let Some(part) = state
            .multipart_parts
            .iter_mut()
            .find(|p| p.upload_id == upload_row_id && p.part_number == part_number)
        {
            part.content_hash = content_hash.to_string();
            part.size = size;
            part.etag = etag.to_string();
            part.created_at = Utc::now();
            let out = part.clone();
            self.persist_after_write(&state).await?;
            return Ok(out);
        }
        let part = MultipartUploadPart {
            id: state.allocate_id(),
            upload_id: upload_row_id,
            part_number,
            content_hash: content_hash.to_string(),
            size,
            etag: etag.to_string(),
            created_at: Utc::now(),
        };
        state.multipart_parts.push(part.clone());
        self.persist_after_write(&state).await?;
        Ok(part)
    }

    pub async fn list_multipart_parts(
        &self,
        upload_row_id: i64,
    ) -> Result<Vec<MultipartUploadPart>> {
        let mut parts = self
            .state
            .read()
            .await
            .multipart_parts
            .iter()
            .filter(|p| p.upload_id == upload_row_id)
            .cloned()
            .collect::<Vec<_>>();
        parts.sort_by_key(|p| p.part_number);
        Ok(parts)
    }

    pub async fn list_multipart_parts_page(
        &self,
        upload_row_id: i64,
        part_number_marker: i32,
        limit: i32,
    ) -> Result<MultipartPartsPage> {
        let mut parts = self
            .list_multipart_parts(upload_row_id)
            .await?
            .into_iter()
            .filter(|p| p.part_number > part_number_marker)
            .collect::<Vec<_>>();
        let limit = if limit == 0 {
            1000
        } else {
            limit.max(1) as usize
        };
        let is_truncated = parts.len() > limit;
        if is_truncated {
            parts.truncate(limit);
        }
        let next_part_number_marker = if is_truncated {
            parts.last().map(|p| p.part_number)
        } else {
            None
        };
        Ok(MultipartPartsPage {
            parts,
            is_truncated,
            next_part_number_marker,
        })
    }

    pub async fn list_active_multipart_uploads(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<MultipartUploadsPage> {
        let mut uploads = self
            .state
            .read()
            .await
            .multipart_uploads
            .iter()
            .filter(|u| {
                u.bucket_id == bucket_id
                    && u.key.starts_with(prefix)
                    && u.completed_at.is_none()
                    && u.aborted_at.is_none()
            })
            .cloned()
            .collect::<Vec<_>>();
        uploads.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });
        if !key_marker.is_empty() {
            let mut past_marker = upload_id_marker.is_none();
            uploads.retain(|u| {
                if u.key.as_str() < key_marker {
                    return false;
                }
                if u.key.as_str() > key_marker {
                    return true;
                }
                if let Some(marker) = upload_id_marker {
                    if past_marker {
                        return true;
                    }
                    if u.upload_id == marker {
                        past_marker = true;
                    }
                    return false;
                }
                true
            });
        }
        let limit = if limit == 0 {
            1000
        } else {
            limit.max(1) as usize
        };
        let is_truncated = uploads.len() > limit;
        if is_truncated {
            uploads.truncate(limit);
        }
        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            uploads
                .last()
                .map(|u| (Some(u.key.clone()), Some(u.upload_id)))
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        Ok(MultipartUploadsPage {
            uploads,
            is_truncated,
            next_key_marker,
            next_upload_id_marker,
        })
    }

    pub async fn complete_multipart_upload(&self, upload_row_id: i64) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(upload) = state
            .multipart_uploads
            .iter_mut()
            .find(|u| u.id == upload_row_id)
        {
            upload.completed_at = Some(Utc::now());
        }
        self.persist_after_write(&state).await
    }

    pub async fn abort_multipart_upload(
        &self,
        _tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<bool> {
        let mut state = self.state.write().await;
        let mut updated = false;
        if let Some(upload) = state.multipart_uploads.iter_mut().find(|u| {
            u.bucket_id == bucket_id
                && u.key == key
                && u.upload_id == upload_id
                && u.completed_at.is_none()
                && u.aborted_at.is_none()
        }) {
            upload.aborted_at = Some(Utc::now());
            updated = true;
        }
        if updated {
            self.persist_after_write(&state).await?;
        }
        Ok(updated)
    }

    pub async fn create_object_watch_event(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        object: &Object,
        event_type: &str,
        is_delete_marker: bool,
    ) -> Result<ObjectWatchEvent> {
        let mut state = self.state.write().await;
        let event = ObjectWatchEvent {
            id: state.allocate_id(),
            tenant_id,
            bucket_id,
            bucket_name: bucket_name.to_string(),
            key: object.key.clone(),
            event_type: event_type.to_string(),
            version_id: Some(object.version_id),
            etag: Some(object.etag.clone()),
            size: object.size,
            is_delete_marker,
            created_at: Utc::now(),
        };
        state.object_events.push(event.clone());
        self.persist_after_write(&state).await?;
        Ok(event)
    }

    pub async fn latest_object_watch_cursor(&self, tenant_id: i64, bucket_id: i64) -> Result<i64> {
        Ok(self
            .state
            .read()
            .await
            .object_events
            .iter()
            .filter(|e| e.tenant_id == tenant_id && e.bucket_id == bucket_id)
            .map(|e| e.id)
            .max()
            .unwrap_or(0))
    }

    pub async fn list_object_watch_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        prefix: &str,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<ObjectWatchEvent>> {
        let mut events = self
            .state
            .read()
            .await
            .object_events
            .iter()
            .filter(|e| {
                e.tenant_id == tenant_id
                    && e.bucket_id == bucket_id
                    && e.key.starts_with(prefix)
                    && e.id > after_cursor
            })
            .cloned()
            .collect::<Vec<_>>();
        events.sort_by_key(|e| e.id);
        events.truncate(if limit == 0 {
            1000
        } else {
            limit.max(1) as usize
        });
        Ok(events)
    }

    pub async fn create_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        stream_key: &str,
    ) -> Result<AppendStream> {
        let mut state = self.state.write().await;
        let stream = AppendStream {
            id: state.allocate_id(),
            tenant_id,
            bucket_id,
            bucket_name: bucket_name.to_string(),
            stream_key: stream_key.to_string(),
            stream_id: uuid::Uuid::new_v4(),
            created_at: Utc::now(),
            sealed_at: None,
            segment_hash: None,
        };
        state.append_streams.push(stream.clone());
        self.persist_after_write(&state).await?;
        Ok(stream)
    }

    pub async fn get_active_append_stream(
        &self,
        _tenant_id: i64,
        bucket_id: i64,
        stream_key: &str,
        stream_id: uuid::Uuid,
    ) -> Result<Option<AppendStream>> {
        Ok(self
            .state
            .read()
            .await
            .append_streams
            .iter()
            .find(|s| {
                s.bucket_id == bucket_id
                    && s.stream_key == stream_key
                    && s.stream_id == stream_id
                    && s.sealed_at.is_none()
            })
            .cloned())
    }

    pub async fn append_stream_record(
        &self,
        stream_row_id: i64,
        payload_hash: &str,
        payload_size: i64,
    ) -> Result<AppendStreamRecord> {
        let mut state = self.state.write().await;
        let next_seq = state
            .append_records
            .iter()
            .filter(|r| r.stream_id == stream_row_id)
            .map(|r| r.record_sequence)
            .max()
            .unwrap_or(0)
            + 1;
        let record = AppendStreamRecord {
            id: state.allocate_id(),
            stream_id: stream_row_id,
            record_sequence: next_seq,
            payload_hash: payload_hash.to_string(),
            payload_size,
            created_at: Utc::now(),
        };
        state.append_records.push(record.clone());
        self.persist_after_write(&state).await?;
        Ok(record)
    }

    pub async fn list_append_stream_records(
        &self,
        stream_row_id: i64,
    ) -> Result<Vec<AppendStreamRecord>> {
        let mut records = self
            .state
            .read()
            .await
            .append_records
            .iter()
            .filter(|r| r.stream_id == stream_row_id)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|r| r.record_sequence);
        Ok(records)
    }

    pub async fn seal_append_stream(&self, stream_row_id: i64, segment_hash: &str) -> Result<bool> {
        let mut state = self.state.write().await;
        let mut sealed = false;
        if let Some(stream) = state
            .append_streams
            .iter_mut()
            .find(|s| s.id == stream_row_id && s.sealed_at.is_none())
        {
            stream.sealed_at = Some(Utc::now());
            stream.segment_hash = Some(segment_hash.to_string());
            sealed = true;
        }
        if sealed {
            self.persist_after_write(&state).await?;
        }
        Ok(sealed)
    }

    pub async fn compare_and_swap_manifest(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        _bucket_name: &str,
        object_key: &str,
        expected_revision: i64,
        manifest: JsonValue,
        manifest_hash: &str,
    ) -> Result<Option<ManifestCasResult>> {
        let mut state = self.state.write().await;
        let current = state
            .manifests
            .iter()
            .filter(|m| {
                m.tenant_id == tenant_id && m.bucket_id == bucket_id && m.object_key == object_key
            })
            .map(|m| m.revision)
            .max()
            .unwrap_or(0);
        if expected_revision != current {
            return Ok(None);
        }
        let revision = current + 1;
        state.manifests.push(ManifestRecord {
            tenant_id,
            bucket_id,
            object_key: object_key.to_string(),
            revision,
            manifest_hash: manifest_hash.to_string(),
            manifest,
            updated_at: Utc::now(),
        });
        self.persist_after_write(&state).await?;
        Ok(Some(ManifestCasResult {
            revision,
            manifest_hash: manifest_hash.to_string(),
        }))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn write_authz_tuple(
        &self,
        tenant_id: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        caveat_hash: &str,
        operation: &str,
        written_by: &str,
        reason: &str,
    ) -> Result<AuthzTupleRecord> {
        authz_journal::write_authz_tuple(
            &self.storage,
            authz_journal::AuthzTupleWrite {
                tenant_id,
                namespace,
                object_id,
                relation,
                subject_kind,
                subject_id,
                caveat_hash,
                operation,
                written_by,
                reason,
            },
        )
        .await
    }

    pub async fn check_authz_tuple(
        &self,
        tenant_id: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        caveat_hash: &str,
    ) -> Result<Option<AuthzTupleRecord>> {
        authz_journal::check_authz_tuple(
            &self.storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
        )
        .await
    }

    pub async fn check_authz_tuple_at_revision(
        &self,
        tenant_id: i64,
        namespace: &str,
        object_id: &str,
        relation: &str,
        subject_kind: &str,
        subject_id: &str,
        caveat_hash: &str,
        revision: i64,
    ) -> Result<Option<AuthzTupleRecord>> {
        authz_journal::check_authz_tuple_at_revision(
            &self.storage,
            tenant_id,
            namespace,
            object_id,
            relation,
            subject_kind,
            subject_id,
            caveat_hash,
            revision,
        )
        .await
    }

    pub async fn list_authz_tuple_log(
        &self,
        tenant_id: i64,
        after_revision: i64,
        namespace: &str,
        limit: i32,
    ) -> Result<Vec<AuthzTupleRecord>> {
        authz_journal::list_authz_tuple_log(
            &self.storage,
            tenant_id,
            after_revision,
            namespace,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
        kind: &str,
        selector: JsonValue,
        extractor: JsonValue,
        authorization_mode: &str,
        build_policy: JsonValue,
    ) -> Result<IndexDefinition> {
        let now = Utc::now();
        Ok(IndexDefinition {
            id: index_journal::next_index_definition_id(&self.storage, tenant_id, bucket_id)
                .await?,
            tenant_id,
            bucket_id,
            name: name.to_string(),
            kind: kind.to_string(),
            selector,
            extractor,
            authorization_mode: authorization_mode.to_string(),
            build_policy,
            enabled: true,
            version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn update_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
        selector: JsonValue,
        extractor: JsonValue,
        authorization_mode: &str,
        build_policy: JsonValue,
    ) -> Result<Option<IndexDefinition>> {
        let Some(mut index) =
            index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
                .await?
        else {
            return Ok(None);
        };
        index.selector = selector;
        index.extractor = extractor;
        index.authorization_mode = authorization_mode.to_string();
        index.build_policy = build_policy;
        index.version += 1;
        index.updated_at = Utc::now();
        Ok(Some(index))
    }

    pub async fn get_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
            .await
    }

    pub async fn disable_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        let Some(mut index) =
            index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
                .await?
        else {
            return Ok(None);
        };
        index.enabled = false;
        index.version += 1;
        index.updated_at = Utc::now();
        Ok(Some(index))
    }

    pub async fn drop_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
            .await
    }

    pub async fn list_index_definitions(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        include_disabled: bool,
    ) -> Result<Vec<IndexDefinition>> {
        index_journal::read_current_index_definitions(
            &self.storage,
            tenant_id,
            bucket_id,
            include_disabled,
        )
        .await
    }

    pub async fn create_index_definition_event(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        index: &IndexDefinition,
        event_type: &str,
    ) -> Result<IndexDefinitionEvent> {
        let event = IndexDefinitionEvent {
            id: index_journal::read_index_definition_events(
                &self.storage,
                tenant_id,
                bucket_id,
                0,
                0,
            )
            .await?
            .into_iter()
            .map(|event| event.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("index definition cursor overflow"))?,
            tenant_id,
            bucket_id,
            bucket_name: bucket_name.to_string(),
            index_id: index.id,
            index_name: index.name.clone(),
            event_type: event_type.to_string(),
            index_version: index.version,
            definition: serde_json::json!({
                "index_id": index.id,
                "bucket_name": bucket_name,
                "name": index.name,
                "kind": index.kind,
                "selector_json": index.selector.to_string(),
                "extractor_json": index.extractor.to_string(),
                "authorization_mode": index.authorization_mode,
                "build_policy_json": index.build_policy.to_string(),
                "enabled": index.enabled,
                "version": index.version,
                "created_at": index.created_at.to_rfc3339(),
                "updated_at": index.updated_at.to_rfc3339(),
            }),
            created_at: Utc::now(),
        };
        index_journal::append_index_definition_event(&self.storage, &event).await?;
        Ok(event)
    }

    pub async fn list_index_definition_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<IndexDefinitionEvent>> {
        index_journal::read_index_definition_events(
            &self.storage,
            tenant_id,
            bucket_id,
            after_cursor,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }

    pub async fn create_index_diagnostic(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        index_id: Option<i64>,
        index_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        severity: &str,
        code: &str,
        message: &str,
        details: JsonValue,
    ) -> Result<IndexDiagnostic> {
        let mut state = self.state.write().await;
        let diagnostic = IndexDiagnostic {
            id: state.allocate_id(),
            tenant_id,
            bucket_id,
            bucket_name: bucket_name.to_string(),
            index_id,
            index_name: index_name.to_string(),
            object_key: object_key.to_string(),
            version_id,
            severity: severity.to_string(),
            code: code.to_string(),
            message: message.to_string(),
            details,
            created_at: Utc::now(),
        };
        state.index_diagnostics.push(diagnostic.clone());
        self.persist_after_write(&state).await?;
        Ok(diagnostic)
    }

    pub async fn list_index_diagnostics(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        index_name: &str,
        severity: &str,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<IndexDiagnostic>> {
        let mut diagnostics = self
            .state
            .read()
            .await
            .index_diagnostics
            .iter()
            .filter(|d| {
                d.tenant_id == tenant_id
                    && d.bucket_id == bucket_id
                    && d.id > after_cursor
                    && (index_name.is_empty() || d.index_name == index_name)
                    && (severity.is_empty() || d.severity == severity)
            })
            .cloned()
            .collect::<Vec<_>>();
        diagnostics.sort_by_key(|d| d.id);
        diagnostics.truncate(if limit == 0 {
            1000
        } else {
            limit.max(1) as usize
        });
        Ok(diagnostics)
    }

    pub async fn hard_delete_object(&self, object_id: i64) -> Result<()> {
        let mut state = self.state.write().await;
        state.objects.retain(|o| o.id != object_id);
        self.persist_after_write(&state).await
    }

    pub async fn enqueue_task(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<()> {
        task_journal::enqueue_task(&self.storage, task_type, payload, priority).await
    }

    pub async fn claim_pending_tasks(&self, limit: i64) -> Result<Vec<TaskRecord>> {
        task_journal::claim_pending_tasks(&self.storage, limit).await
    }

    pub async fn list_tasks(&self) -> Result<Vec<TaskRecord>> {
        task_journal::list_tasks(&self.storage).await
    }

    pub async fn update_task_status(
        &self,
        task_id: i64,
        status: crate::tasks::TaskStatus,
    ) -> Result<()> {
        task_journal::update_task_status(&self.storage, task_id, status).await
    }

    pub async fn fail_task(&self, task_id: i64, error: &str) -> Result<()> {
        task_journal::fail_task(&self.storage, task_id, error).await
    }

    pub async fn hf_create_key(
        &self,
        name: &str,
        token_encrypted: &[u8],
        note: Option<&str>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        if state.hf_keys.iter().any(|k| k.name == name) {
            return Err(anyhow!("hugging face key already exists"));
        }
        let now = Utc::now();
        let key = HfKey {
            id: state.allocate_id(),
            name: name.to_string(),
            token_encrypted: token_encrypted.to_vec(),
            note: note.map(ToOwned::to_owned),
            created_at: now,
            updated_at: now,
        };
        state.hf_keys.push(key);
        self.persist_after_write(&state).await
    }

    pub async fn hf_delete_key(&self, name: &str) -> Result<u64> {
        let mut state = self.state.write().await;
        let before = state.hf_keys.len();
        state.hf_keys.retain(|k| k.name != name);
        let deleted = before - state.hf_keys.len();
        if deleted > 0 {
            self.persist_after_write(&state).await?;
        }
        Ok(deleted as u64)
    }

    pub async fn hf_get_key_encrypted(&self, name: &str) -> Result<Option<(i64, Vec<u8>)>> {
        Ok(self
            .state
            .read()
            .await
            .hf_keys
            .iter()
            .find(|k| k.name == name)
            .map(|k| (k.id, k.token_encrypted.clone())))
    }

    pub async fn hf_get_key_encrypted_by_id(&self, id: i64) -> Result<Option<Vec<u8>>> {
        Ok(self
            .state
            .read()
            .await
            .hf_keys
            .iter()
            .find(|key| key.id == id)
            .map(|key| key.token_encrypted.clone()))
    }

    pub async fn hf_list_keys(
        &self,
    ) -> Result<Vec<(String, Option<String>, DateTime<Utc>, DateTime<Utc>)>> {
        let mut keys = self
            .state
            .read()
            .await
            .hf_keys
            .iter()
            .map(|k| (k.name.clone(), k.note.clone(), k.created_at, k.updated_at))
            .collect::<Vec<_>>();
        keys.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(keys)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn hf_create_ingestion(
        &self,
        key_id: i64,
        tenant_id: i64,
        requester_app_id: i64,
        repo: &str,
        revision: Option<&str>,
        target_bucket: &str,
        target_region: &str,
        target_prefix: Option<&str>,
        include_globs: &[String],
        exclude_globs: &[String],
    ) -> Result<i64> {
        let mut state = self.state.write().await;
        let id = state.allocate_id();
        state.hf_ingestions.push(HfIngestion {
            id,
            key_id,
            tenant_id,
            requester_app_id,
            repo: repo.to_string(),
            revision: revision.unwrap_or("main").to_string(),
            target_bucket: target_bucket.to_string(),
            target_region: target_region.to_string(),
            target_prefix: target_prefix.unwrap_or_default().to_string(),
            include_globs: include_globs.to_vec(),
            exclude_globs: exclude_globs.to_vec(),
            state: crate::tasks::HFIngestionState::Queued,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        });
        self.persist_after_write(&state).await?;
        Ok(id)
    }

    pub async fn hf_get_ingestion_job(&self, id: i64) -> Result<Option<HfIngestionJob>> {
        Ok(self
            .state
            .read()
            .await
            .hf_ingestions
            .iter()
            .find(|h| h.id == id)
            .map(|h| HfIngestionJob {
                key_id: h.key_id,
                tenant_id: h.tenant_id,
                requester_app_id: h.requester_app_id,
                repo: h.repo.clone(),
                revision: h.revision.clone(),
                target_bucket: h.target_bucket.clone(),
                target_region: h.target_region.clone(),
                target_prefix: h.target_prefix.clone(),
                include_globs: h.include_globs.clone(),
                exclude_globs: h.exclude_globs.clone(),
            }))
    }

    pub async fn hf_update_ingestion_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionState,
        error: Option<&str>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(job) = state.hf_ingestions.iter_mut().find(|h| h.id == id) {
            job.state = state_value;
            job.error = error.map(ToOwned::to_owned);
            if state_value == crate::tasks::HFIngestionState::Running && job.started_at.is_none() {
                job.started_at = Some(Utc::now());
            }
            if matches!(
                state_value,
                crate::tasks::HFIngestionState::Completed
                    | crate::tasks::HFIngestionState::Failed
                    | crate::tasks::HFIngestionState::Canceled
            ) {
                job.finished_at = Some(Utc::now());
            }
        }
        self.persist_after_write(&state).await
    }

    pub async fn hf_cancel_ingestion(&self, id: i64) -> Result<u64> {
        let mut state = self.state.write().await;
        let mut updated = 0;
        if let Some(job) = state.hf_ingestions.iter_mut().find(|h| {
            h.id == id
                && matches!(
                    h.state,
                    crate::tasks::HFIngestionState::Queued
                        | crate::tasks::HFIngestionState::Running
                )
        }) {
            job.state = crate::tasks::HFIngestionState::Canceled;
            job.finished_at = Some(Utc::now());
            updated = 1;
        }
        if updated > 0 {
            self.persist_after_write(&state).await?;
        }
        Ok(updated)
    }

    pub async fn hf_add_item(
        &self,
        ingestion_id: i64,
        path: &str,
        size: Option<i64>,
        etag: Option<&str>,
    ) -> Result<i64> {
        let mut state = self.state.write().await;
        if let Some(item) = state
            .hf_items
            .iter_mut()
            .find(|i| i.ingestion_id == ingestion_id && i.path == path)
        {
            item.size = size;
            item.etag = etag.map(ToOwned::to_owned);
            let id = item.id;
            self.persist_after_write(&state).await?;
            return Ok(id);
        }
        let id = state.allocate_id();
        state.hf_items.push(HfIngestionItem {
            id,
            ingestion_id,
            path: path.to_string(),
            size,
            etag: etag.map(ToOwned::to_owned),
            state: crate::tasks::HFIngestionItemState::Queued,
            error: None,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
        });
        self.persist_after_write(&state).await?;
        Ok(id)
    }

    pub async fn hf_update_item_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionItemState,
        error: Option<&str>,
    ) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(item) = state.hf_items.iter_mut().find(|i| i.id == id) {
            item.state = state_value;
            item.error = error.map(ToOwned::to_owned);
            if state_value == crate::tasks::HFIngestionItemState::Downloading
                && item.started_at.is_none()
            {
                item.started_at = Some(Utc::now());
            }
            if matches!(
                state_value,
                crate::tasks::HFIngestionItemState::Stored
                    | crate::tasks::HFIngestionItemState::Failed
                    | crate::tasks::HFIngestionItemState::Skipped
            ) {
                item.finished_at = Some(Utc::now());
            }
        }
        self.persist_after_write(&state).await
    }

    pub async fn hf_update_item_success(&self, id: i64, size: i64, etag: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if let Some(item) = state.hf_items.iter_mut().find(|i| i.id == id) {
            item.state = crate::tasks::HFIngestionItemState::Stored;
            item.size = Some(size);
            item.etag = Some(etag.to_string());
            item.finished_at = Some(Utc::now());
        }
        self.persist_after_write(&state).await
    }

    pub async fn hf_get_ingestion_items(
        &self,
        ingestion_id: i64,
    ) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
        Ok(self
            .state
            .read()
            .await
            .hf_items
            .iter()
            .filter(|i| {
                i.ingestion_id == ingestion_id
                    && i.state == crate::tasks::HFIngestionItemState::Stored
            })
            .map(|i| (i.path.clone(), i.size, i.etag.clone(), i.finished_at))
            .collect())
    }

    pub async fn hf_get_all_items_for_prefix(
        &self,
        tenant_id: i64,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
        let state = self.state.read().await;
        let ingestion_ids = state
            .hf_ingestions
            .iter()
            .filter(|h| {
                h.tenant_id == tenant_id && h.target_bucket == bucket && h.target_prefix == prefix
            })
            .map(|h| h.id)
            .collect::<HashSet<_>>();
        Ok(state
            .hf_items
            .iter()
            .filter(|i| {
                ingestion_ids.contains(&i.ingestion_id)
                    && i.state == crate::tasks::HFIngestionItemState::Stored
            })
            .map(|i| (i.path.clone(), i.size, i.etag.clone(), i.finished_at))
            .collect())
    }

    pub async fn hf_status_summary(
        &self,
        id: i64,
    ) -> Result<(
        String,
        i64,
        i64,
        i64,
        i64,
        Option<String>,
        Option<DateTime<Utc>>,
        Option<DateTime<Utc>>,
        DateTime<Utc>,
    )> {
        let state = self.state.read().await;
        let job = state
            .hf_ingestions
            .iter()
            .find(|h| h.id == id)
            .ok_or_else(|| anyhow!("ingestion not found"))?;
        let queued = state
            .hf_items
            .iter()
            .filter(|i| {
                i.ingestion_id == id && i.state == crate::tasks::HFIngestionItemState::Queued
            })
            .count() as i64;
        let downloading = state
            .hf_items
            .iter()
            .filter(|i| {
                i.ingestion_id == id && i.state == crate::tasks::HFIngestionItemState::Downloading
            })
            .count() as i64;
        let stored = state
            .hf_items
            .iter()
            .filter(|i| {
                i.ingestion_id == id && i.state == crate::tasks::HFIngestionItemState::Stored
            })
            .count() as i64;
        let failed = state
            .hf_items
            .iter()
            .filter(|i| {
                i.ingestion_id == id && i.state == crate::tasks::HFIngestionItemState::Failed
            })
            .count() as i64;
        let state_text = if job.state == crate::tasks::HFIngestionState::Running
            && queued == 0
            && downloading == 0
            && (stored > 0 || failed > 0)
        {
            "completed".to_string()
        } else {
            job.state.as_str().to_string()
        };
        Ok((
            state_text,
            queued,
            downloading,
            stored,
            failed,
            job.error.clone(),
            job.started_at,
            job.finished_at,
            job.created_at,
        ))
    }
}
