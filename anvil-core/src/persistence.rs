use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use tokio::sync::mpsc::Sender;

use crate::{
    append_journal, authz_journal, authz_repair,
    bucket_journal::{self, BucketJournalMutation},
    cache::MetadataCache,
    cluster::MetadataEvent,
    config::Config,
    control_journal, directory_repair, hf_journal, index_builder, index_diagnostic_journal,
    index_journal, index_repair, manifest_journal, metadata_journal, model_journal,
    multipart_journal,
    partition_fence::{
        PartitionOwnerStatus, PartitionRecoveryAcquire, PartitionWritePermit,
        acquire_partition_recovery, publish_partition_ready, read_partition_owner,
    },
    personaldb_repair, repair_finding,
    storage::Storage,
    task_journal, task_lease, watch_checkpoint, watch_log,
};

#[derive(Debug, Clone)]
pub struct Persistence {
    storage: Storage,
    cache: MetadataCache,
    event_publisher: Option<Sender<MetadataEvent>>,
    owner_node_id: String,
    partition_owner_signing_key: Vec<u8>,
    personaldb_signing_key: Vec<u8>,
    object_metadata_compaction_frame_threshold: u64,
    object_metadata_compaction_bytes_threshold: u64,
    task_lease_ttl_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HfKey {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) token_encrypted: Vec<u8>,
    pub(crate) note: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HfIngestion {
    pub(crate) id: i64,
    pub(crate) key_id: i64,
    pub(crate) tenant_id: i64,
    pub(crate) requester_app_id: i64,
    pub(crate) repo: String,
    pub(crate) revision: String,
    pub(crate) target_bucket: String,
    pub(crate) target_region: String,
    pub(crate) target_prefix: String,
    pub(crate) include_globs: Vec<String>,
    pub(crate) exclude_globs: Vec<String>,
    pub(crate) state: crate::tasks::HFIngestionState,
    pub(crate) error: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct HfIngestionItem {
    pub(crate) id: i64,
    pub(crate) ingestion_id: i64,
    pub(crate) path: String,
    pub(crate) size: Option<i64>,
    pub(crate) etag: Option<String>,
    pub(crate) state: crate::tasks::HFIngestionItemState,
    pub(crate) error: Option<String>,
    pub(crate) created_at: DateTime<Utc>,
    pub(crate) started_at: Option<DateTime<Utc>>,
    pub(crate) finished_at: Option<DateTime<Utc>>,
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
    pub mutation_id: uuid::Uuid,
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
pub struct MultipartUploadMutation {
    pub upload: MultipartUpload,
    pub receipt: MetadataMutationReceipt,
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
pub struct MultipartUploadPartMutation {
    pub part: MultipartUploadPart,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartCompletionMutation {
    pub completed: bool,
    pub receipt: Option<MetadataMutationReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartAbortMutation {
    pub aborted: bool,
    pub receipt: Option<MetadataMutationReceipt>,
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
    pub mutation_id: uuid::Uuid,
    pub payload_hash: String,
    pub etag: Option<String>,
    pub size: i64,
    pub is_delete_marker: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataMutationReceipt {
    pub mutation_id: uuid::Uuid,
    pub payload_hash: String,
    pub record_hash: String,
    pub watch_cursor: u64,
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
pub struct AppendStreamMutation {
    pub stream: AppendStream,
    pub receipt: MetadataMutationReceipt,
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
pub struct AppendStreamRecordMutation {
    pub record: AppendStreamRecord,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SealAppendStreamMutation {
    pub sealed: bool,
    pub receipt: Option<MetadataMutationReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestCasResult {
    pub revision: i64,
    pub manifest_hash: String,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzTupleRecord {
    pub revision: i64,
    #[serde(default)]
    pub revision_ordinal: u32,
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
    pub mutation_id: uuid::Uuid,
    pub record_hash: String,
    pub written_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct AuthzTupleBatchMutation {
    pub namespace: String,
    pub object_id: String,
    pub relation: String,
    pub subject_kind: String,
    pub subject_id: String,
    pub caveat_hash: String,
    pub operation: String,
    pub reason: String,
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
    pub mutation_id: uuid::Uuid,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct TaskLeaseTarget {
    partition_family: String,
    partition_id: String,
    source_cursor: u128,
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
        Ok(Self {
            storage: Storage::new_at_sync(&config.storage_path)?,
            cache: MetadataCache::new(config),
            event_publisher,
            owner_node_id: persistence_owner_node_id(config),
            partition_owner_signing_key: hex::decode(&config.anvil_secret_encryption_key)?,
            personaldb_signing_key: config.anvil_secret_encryption_key.as_bytes().to_vec(),
            object_metadata_compaction_frame_threshold: config
                .object_metadata_compaction_frame_threshold,
            object_metadata_compaction_bytes_threshold: config
                .object_metadata_compaction_bytes_threshold,
            task_lease_ttl_secs: if config.task_lease_ttl_secs == 0 {
                300
            } else {
                config.task_lease_ttl_secs
            },
        })
    }

    async fn publish_event(&self, event: MetadataEvent) {
        if let Some(sender) = &self.event_publisher {
            let _ = sender.send(event).await;
        }
    }

    pub fn cache(&self) -> &MetadataCache {
        &self.cache
    }

    async fn global_write_permit(
        &self,
        partition_family: &str,
        partition_id: String,
    ) -> Result<PartitionWritePermit> {
        if self.partition_owner_signing_key.is_empty() {
            return Err(anyhow!("partition owner signing key must not be empty"));
        }
        if let Some(owner) = read_partition_owner(
            &self.storage,
            partition_family,
            &partition_id,
            &self.partition_owner_signing_key,
        )
        .await?
        {
            if owner.status == PartitionOwnerStatus::Ready
                && owner.owner_node_id == self.owner_node_id
            {
                return owner.write_permit().map_err(Into::into);
            }
        }

        let now_nanos = Utc::now()
            .timestamp_nanos_opt()
            .ok_or_else(|| anyhow!("partition owner timestamp overflow"))?;
        let recovering = acquire_partition_recovery(
            &self.storage,
            PartitionRecoveryAcquire {
                partition_family: partition_family.to_string(),
                partition_id: partition_id.clone(),
                owner_node_id: self.owner_node_id.clone(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await?;
        let ready = publish_partition_ready(
            &self.storage,
            partition_family,
            &partition_id,
            &self.owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([0; 32]),
            now_nanos.saturating_add(1),
            &self.partition_owner_signing_key,
        )
        .await?;
        ready.write_permit().map_err(Into::into)
    }

    async fn control_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "control_plane",
            hex::encode(control_journal::control_partition_id()),
        )
        .await
    }

    async fn task_queue_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "task_queue",
            hex::encode(task_journal::task_queue_partition_id()),
        )
        .await
    }

    async fn model_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "model_metadata",
            hex::encode(model_journal::model_partition_id()),
        )
        .await
    }

    async fn hf_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit("hf_metadata", hex::encode(hf_journal::hf_partition_id()))
            .await
    }

    async fn bucket_tenant_write_permit(&self, tenant_id: i64) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "bucket_metadata",
            hex::encode(bucket_journal::tenant_bucket_partition_id(tenant_id)),
        )
        .await
    }

    async fn bucket_global_write_permit(&self) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "bucket_metadata",
            hex::encode(bucket_journal::global_bucket_partition_id()),
        )
        .await
    }

    async fn object_metadata_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "object_metadata",
            hex::encode(metadata_journal::object_metadata_partition_id(
                tenant_id, bucket_id,
            )),
        )
        .await
    }

    async fn multipart_metadata_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "multipart_metadata",
            hex::encode(multipart_journal::multipart_metadata_partition_id(
                tenant_id, bucket_id,
            )),
        )
        .await
    }

    async fn append_metadata_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "append_metadata",
            hex::encode(append_journal::append_metadata_partition_id(
                tenant_id, bucket_id,
            )),
        )
        .await
    }

    async fn manifest_cas_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "manifest_cas",
            hex::encode(manifest_journal::manifest_cas_partition_id(
                tenant_id, bucket_id,
            )),
        )
        .await
    }

    async fn authz_write_permit(&self, tenant_id: i64) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "authz_tuple",
            hex::encode(authz_journal::authz_partition_id(tenant_id)),
        )
        .await
    }

    async fn repair_write_permit(
        &self,
        scope_kind: &str,
        scope_id: &str,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "repair",
            hex::encode(crate::formats::hash32(
                format!("repair\0{scope_kind}\0{scope_id}").as_bytes(),
            )),
        )
        .await
    }

    async fn index_definition_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "index_definition",
            hex::encode(index_journal::index_definition_partition_id(
                tenant_id, bucket_id,
            )),
        )
        .await
    }

    async fn index_diagnostic_write_permit(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<PartitionWritePermit> {
        self.global_write_permit(
            "index_diagnostic",
            hex::encode(index_diagnostic_journal::index_diagnostic_partition_id(
                tenant_id, bucket_id,
            )),
        )
        .await
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
        let permit = self.control_write_permit().await?;
        control_journal::create_admin_user_with_permit(
            &self.storage,
            username,
            email,
            password_hash,
            role_names,
            &permit,
            &self.partition_owner_signing_key,
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
        let permit = self.control_write_permit().await?;
        control_journal::update_admin_user_with_permit(
            &self.storage,
            user_id,
            username,
            email,
            password_hash,
            is_active,
            role_names,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn delete_admin_user(&self, user_id: i64) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::delete_admin_user_with_permit(
            &self.storage,
            user_id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_admin_users(&self) -> Result<Vec<AdminUser>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .admin_users())
    }

    pub async fn create_admin_role(&self, name: &str) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::create_admin_role_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let permit = self.control_write_permit().await?;
        control_journal::update_admin_role_with_permit(
            &self.storage,
            id,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn delete_admin_role(&self, id: i32) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::delete_admin_role_with_permit(
            &self.storage,
            id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let permit = self.model_write_permit().await?;
        model_journal::create_model_artifact_with_permit(
            &self.storage,
            artifact_id,
            bucket_id,
            key,
            manifest,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn create_model_tensors(
        &self,
        artifact_id: &str,
        tensors: &[crate::anvil_api::TensorIndexRow],
    ) -> Result<()> {
        let permit = self.model_write_permit().await?;
        model_journal::create_model_tensors_with_permit(
            &self.storage,
            artifact_id,
            tensors,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let permit = self.control_write_permit().await?;
        control_journal::create_region_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let permit = self.control_write_permit().await?;
        control_journal::create_tenant_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn create_app(
        &self,
        tenant_id: i64,
        name: &str,
        client_id: &str,
        encrypted_secret: &[u8],
    ) -> Result<App> {
        let permit = self.control_write_permit().await?;
        control_journal::create_app_with_permit(
            &self.storage,
            tenant_id,
            name,
            client_id,
            encrypted_secret,
            &permit,
            &self.partition_owner_signing_key,
        )
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
        let permit = self.control_write_permit().await?;
        control_journal::update_app_secret_with_permit(
            &self.storage,
            app_id,
            new_encrypted_secret,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn grant_policy(&self, app_id: i64, resource: &str, action: &str) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::grant_policy_with_permit(
            &self.storage,
            app_id,
            resource,
            action,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn revoke_policy(&self, app_id: i64, resource: &str, action: &str) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::revoke_policy_with_permit(
            &self.storage,
            app_id,
            resource,
            action,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let tenant_permit = self
            .bucket_tenant_write_permit(tenant_id)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        let global_permit = self
            .bucket_global_write_permit()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        bucket_journal::append_bucket_mutation_with_permits(
            &self.storage,
            &bucket,
            BucketJournalMutation::Create,
            &tenant_permit,
            &global_permit,
            &self.partition_owner_signing_key,
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
        let tenant_permit = self.bucket_tenant_write_permit(out.tenant_id).await?;
        let global_permit = self.bucket_global_write_permit().await?;
        bucket_journal::append_bucket_mutation_with_permits(
            &self.storage,
            &out,
            BucketJournalMutation::Update,
            &tenant_permit,
            &global_permit,
            &self.partition_owner_signing_key,
        )
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
        let tenant_permit = self.bucket_tenant_write_permit(out.tenant_id).await?;
        let global_permit = self.bucket_global_write_permit().await?;
        bucket_journal::append_bucket_mutation_with_permits(
            &self.storage,
            &out,
            BucketJournalMutation::Update,
            &tenant_permit,
            &global_permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.cache
            .invalidate_bucket(out.tenant_id, bucket_name)
            .await;
        Ok(out)
    }

    pub async fn soft_delete_bucket(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        let deleted = bucket_journal::read_current_bucket(&self.storage, tenant_id, name).await?;
        if let Some(bucket) = &deleted {
            let tenant_permit = self.bucket_tenant_write_permit(bucket.tenant_id).await?;
            let global_permit = self.bucket_global_write_permit().await?;
            bucket_journal::append_bucket_mutation_with_permits(
                &self.storage,
                bucket,
                BucketJournalMutation::Delete,
                &tenant_permit,
                &global_permit,
                &self.partition_owner_signing_key,
            )
            .await?;
        }
        self.cache.invalidate_bucket(tenant_id, name).await;
        Ok(deleted)
    }

    pub async fn bucket_has_retained_objects_or_uploads(&self, bucket_id: i64) -> Result<bool> {
        let has_objects = if let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        {
            !metadata_journal::read_object_versions(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
                "",
                "",
                None,
                1,
            )
            .await?
            .versions
            .is_empty()
        } else {
            false
        };
        if has_objects {
            return Ok(true);
        }
        multipart_journal::has_active_multipart_upload(&self.storage, bucket_id).await
    }

    pub async fn hard_delete_bucket_if_empty(&self, bucket_id: i64) -> Result<bool> {
        if self
            .bucket_has_retained_objects_or_uploads(bucket_id)
            .await?
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
        let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("bucket does not belong to tenant"));
        }
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
        let object = Object {
            id: metadata_journal::next_object_id(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
            )
            .await?,
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
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        metadata_journal::append_object_mutation_with_permit(
            &self.storage,
            &bucket,
            &object,
            metadata_journal::ObjectJournalMutation::Put,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.enqueue_index_builds_for_bucket(&bucket).await?;
        self.enqueue_object_metadata_compaction_if_due(&bucket)
            .await?;
        Ok(object)
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
        )
        .await
    }

    pub async fn get_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        metadata_journal::read_object_version(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
            version_id,
        )
        .await
    }

    pub async fn get_object_version_by_id(
        &self,
        bucket_id: i64,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        metadata_journal::read_object_version_by_id(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            version_id,
        )
        .await
    }

    pub async fn list_current_directory_objects(&self, bucket: &Bucket) -> Result<Vec<Object>> {
        metadata_journal::read_current_directory_objects(
            &self.storage,
            bucket,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_objects(
        &self,
        bucket_id: i64,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>)> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok((Vec::new(), Vec::new()));
        };
        let listing = metadata_journal::list_current_objects(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            prefix,
            start_after,
            limit,
            delimiter,
        )
        .await?;
        Ok((listing.objects, listing.common_prefixes))
    }

    pub async fn soft_delete_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let Some(base) = metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
        )
        .await?
        else {
            return Ok(None);
        };
        let now = Utc::now();
        let object = Object {
            id: metadata_journal::next_object_id(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
            )
            .await?,
            mutation_id: uuid::Uuid::new_v4(),
            version_id: uuid::Uuid::new_v4(),
            content_hash: String::new(),
            size: 0,
            etag: String::new(),
            created_at: now,
            deleted_at: Some(now),
            ..base
        };
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        metadata_journal::append_object_mutation_with_permit(
            &self.storage,
            &bucket,
            &object,
            metadata_journal::ObjectJournalMutation::DeleteMarker,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.enqueue_index_builds_for_bucket(&bucket).await?;
        self.enqueue_object_metadata_compaction_if_due(&bucket)
            .await?;
        Ok(Some(object))
    }

    pub async fn delete_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let Some(mut object) = metadata_journal::read_object_version(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            key,
            version_id,
        )
        .await?
        else {
            return Ok(None);
        };
        object.id = metadata_journal::next_object_id(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        object.mutation_id = uuid::Uuid::new_v4();
        object.deleted_at = Some(Utc::now());
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        metadata_journal::append_object_mutation_with_permit(
            &self.storage,
            &bucket,
            &object,
            metadata_journal::ObjectJournalMutation::DeleteVersion,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.enqueue_index_builds_for_bucket(&bucket).await?;
        self.enqueue_object_metadata_compaction_if_due(&bucket)
            .await?;
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
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(ObjectVersionsPage {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        };
        metadata_journal::read_object_versions(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            prefix,
            key_marker,
            version_id_marker,
            limit,
        )
        .await
    }

    pub async fn compact_object_metadata(
        &self,
        bucket_id: i64,
    ) -> Result<Option<metadata_journal::SealedObjectMetadataSegments>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        let journal_path = self
            .storage
            .metadata_journal_path(bucket.tenant_id, bucket.id);
        if tokio::fs::metadata(&journal_path).await.is_err() {
            return Ok(None);
        }
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        metadata_journal::seal_object_journal_segments_with_permit(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
        .map(Some)
    }

    async fn enqueue_object_metadata_compaction_if_due(&self, bucket: &Bucket) -> Result<()> {
        let stats = metadata_journal::active_object_journal_stats(
            &self.storage,
            bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        let frame_due = self.object_metadata_compaction_frame_threshold > 0
            && stats.uncompacted_frame_count >= self.object_metadata_compaction_frame_threshold;
        let bytes_due = self.object_metadata_compaction_bytes_threshold > 0
            && stats.uncompacted_encoded_bytes >= self.object_metadata_compaction_bytes_threshold;
        if !frame_due && !bytes_due {
            return Ok(());
        }

        self.enqueue_task_if_absent(
            crate::tasks::TaskType::ObjectMetadataCompaction,
            serde_json::json!({ "bucket_id": bucket.id }),
            50,
        )
        .await?;
        Ok(())
    }

    pub async fn create_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
    ) -> Result<MultipartUploadMutation> {
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::create_multipart_upload_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn get_active_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<Option<MultipartUpload>> {
        multipart_journal::get_active_multipart_upload(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            upload_id,
        )
        .await
    }

    pub async fn upsert_multipart_part(
        &self,
        upload_row_id: i64,
        part_number: i32,
        content_hash: &str,
        size: i64,
        etag: &str,
    ) -> Result<MultipartUploadPartMutation> {
        let (tenant_id, bucket_id) =
            multipart_journal::find_multipart_upload_partition(&self.storage, upload_row_id)
                .await?
                .ok_or_else(|| anyhow!("multipart upload not found"))?;
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::upsert_multipart_part_with_permit(
            &self.storage,
            upload_row_id,
            part_number,
            content_hash,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_multipart_parts(
        &self,
        upload_row_id: i64,
    ) -> Result<Vec<MultipartUploadPart>> {
        multipart_journal::list_multipart_parts(&self.storage, upload_row_id).await
    }

    pub async fn list_multipart_parts_page(
        &self,
        upload_row_id: i64,
        part_number_marker: i32,
        limit: i32,
    ) -> Result<MultipartPartsPage> {
        multipart_journal::list_multipart_parts_page(
            &self.storage,
            upload_row_id,
            part_number_marker,
            limit,
        )
        .await
    }

    pub async fn list_active_multipart_uploads(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<MultipartUploadsPage> {
        multipart_journal::list_active_multipart_uploads(
            &self.storage,
            bucket_id,
            prefix,
            key_marker,
            upload_id_marker,
            limit,
        )
        .await
    }

    pub async fn complete_multipart_upload(
        &self,
        upload_row_id: i64,
    ) -> Result<MultipartCompletionMutation> {
        let Some((tenant_id, bucket_id)) =
            multipart_journal::find_multipart_upload_partition(&self.storage, upload_row_id)
                .await?
        else {
            return Ok(MultipartCompletionMutation {
                completed: false,
                receipt: None,
            });
        };
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::complete_multipart_upload_with_permit(
            &self.storage,
            upload_row_id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn abort_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<MultipartAbortMutation> {
        let permit = self
            .multipart_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        multipart_journal::abort_multipart_upload_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            key,
            upload_id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let id = self
            .latest_object_watch_cursor(tenant_id, bucket_id)
            .await?
            .checked_add(1)
            .ok_or_else(|| anyhow!("object watch cursor overflow"))?;
        Ok(ObjectWatchEvent {
            id,
            tenant_id,
            bucket_id,
            bucket_name: bucket_name.to_string(),
            key: object.key.clone(),
            event_type: event_type.to_string(),
            version_id: Some(object.version_id),
            mutation_id: object.mutation_id,
            payload_hash: object.content_hash.clone(),
            etag: Some(object.etag.clone()),
            size: object.size,
            is_delete_marker,
            created_at: Utc::now(),
        })
    }

    pub async fn latest_object_watch_cursor(&self, tenant_id: i64, bucket_id: i64) -> Result<i64> {
        Ok(
            watch_log::list_object_watch_events(&self.storage, tenant_id, bucket_id, "", 0, 0)
                .await?
                .into_iter()
                .map(|event| event.id)
                .max()
                .unwrap_or(0),
        )
    }

    pub async fn list_object_watch_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        prefix: &str,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<ObjectWatchEvent>> {
        watch_log::list_object_watch_events(
            &self.storage,
            tenant_id,
            bucket_id,
            prefix,
            after_cursor,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }

    pub async fn create_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        stream_key: &str,
    ) -> Result<AppendStreamMutation> {
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::create_append_stream_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            bucket_name,
            stream_key,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn get_active_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream_key: &str,
        stream_id: uuid::Uuid,
    ) -> Result<Option<AppendStream>> {
        append_journal::get_active_append_stream(
            &self.storage,
            tenant_id,
            bucket_id,
            stream_key,
            stream_id,
        )
        .await
    }

    pub async fn append_stream_record(
        &self,
        stream_row_id: i64,
        payload_hash: &str,
        payload_size: i64,
    ) -> Result<AppendStreamRecordMutation> {
        let (tenant_id, bucket_id) =
            append_journal::find_append_stream_partition(&self.storage, stream_row_id)
                .await?
                .ok_or_else(|| anyhow!("append stream not found"))?;
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::append_stream_record_with_permit(
            &self.storage,
            stream_row_id,
            payload_hash,
            payload_size,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_append_stream_records(
        &self,
        stream_row_id: i64,
    ) -> Result<Vec<AppendStreamRecord>> {
        append_journal::list_append_stream_records(&self.storage, stream_row_id).await
    }

    pub async fn seal_append_stream(
        &self,
        stream_row_id: i64,
        segment_hash: &str,
    ) -> Result<SealAppendStreamMutation> {
        let Some((tenant_id, bucket_id)) =
            append_journal::find_append_stream_partition(&self.storage, stream_row_id).await?
        else {
            return Ok(SealAppendStreamMutation {
                sealed: false,
                receipt: None,
            });
        };
        let permit = self
            .append_metadata_write_permit(tenant_id, bucket_id)
            .await?;
        append_journal::seal_append_stream_with_permit(
            &self.storage,
            stream_row_id,
            segment_hash,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let permit = self.manifest_cas_write_permit(tenant_id, bucket_id).await?;
        manifest_journal::compare_and_swap_manifest_with_permit(
            &self.storage,
            tenant_id,
            bucket_id,
            object_key,
            expected_revision,
            manifest,
            manifest_hash,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        let permit = self.authz_write_permit(tenant_id).await?;
        authz_journal::write_authz_tuple_with_permit(
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
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn write_authz_tuple_batch(
        &self,
        tenant_id: i64,
        mutations: Vec<AuthzTupleBatchMutation>,
        written_by: &str,
    ) -> Result<Vec<AuthzTupleRecord>> {
        let permit = self.authz_write_permit(tenant_id).await?;
        let writes = mutations
            .iter()
            .map(|mutation| authz_journal::AuthzTupleWrite {
                tenant_id,
                namespace: mutation.namespace.as_str(),
                object_id: mutation.object_id.as_str(),
                relation: mutation.relation.as_str(),
                subject_kind: mutation.subject_kind.as_str(),
                subject_id: mutation.subject_id.as_str(),
                caveat_hash: mutation.caveat_hash.as_str(),
                operation: mutation.operation.as_str(),
                written_by,
                reason: mutation.reason.as_str(),
            })
            .collect();
        authz_journal::write_authz_tuple_batch_with_permit(
            &self.storage,
            writes,
            &permit,
            &self.partition_owner_signing_key,
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
            mutation_id: uuid::Uuid::new_v4(),
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
        let permit = self
            .index_definition_write_permit(tenant_id, bucket_id)
            .await?;
        index_journal::append_index_definition_event_with_permit(
            &self.storage,
            &event,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
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

    pub async fn enqueue_index_build_for_index(
        &self,
        bucket: &Bucket,
        index: &IndexDefinition,
    ) -> Result<bool> {
        if !index.enabled || !matches!(index.kind.as_str(), "full_text" | "vector" | "hybrid") {
            return Ok(false);
        }
        let stats = metadata_journal::active_object_journal_stats(
            &self.storage,
            bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        let source_cursor = index_repair::source_cursor_from_stats(stats);
        if source_cursor == 0 {
            return Ok(false);
        }
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let checkpoint = watch_checkpoint::read_watch_checkpoint(
            &self.storage,
            "object_metadata",
            &index_storage_id,
            &self.partition_owner_signing_key,
        )
        .await?;
        let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
            &self.storage,
            bucket,
            &self.partition_owner_signing_key,
            source_cursor,
        )
        .await?;
        let latest_proof = crate::derived_index_proof::read_latest_derived_index_proof(
            &self.storage,
            &index_storage_id,
            &self.partition_owner_signing_key,
        )
        .await
        .ok()
        .flatten();
        let catch_up_plan = crate::derived_index_catchup::plan_derived_index_catch_up(
            crate::derived_index_catchup::DerivedIndexCatchUpInput {
                index_id: index_storage_id.clone(),
                consumer_id: index_storage_id.clone(),
                watch_stream_id: "object_metadata".to_string(),
                checkpoint_cursor: checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.cursor)
                    .unwrap_or(0),
                retained_start_cursor: u128::from(stats.compacted_through_sequence),
                latest_cursor: source_cursor,
                manifest_checkpoint_cursor: u128::from(stats.compacted_through_sequence),
                source_manifest_hash: source_manifest_hash.clone(),
                required_source_cursor: source_cursor,
                min_generation: index.version.max(1) as u64,
                latest_proof,
            },
            &self.partition_owner_signing_key,
        )?;
        if matches!(
            catch_up_plan,
            crate::derived_index_catchup::DerivedIndexCatchUpPlan::UpToDate { .. }
        ) {
            return Ok(false);
        }
        self.enqueue_index_build_task(
            serde_json::json!({
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "index_id": index.id,
                "index_version": index.version,
                "source_cursor": source_cursor,
                "source_manifest_hash": source_manifest_hash,
                "catch_up_plan": catch_up_plan,
            }),
            40,
        )
        .await
    }

    pub async fn enqueue_index_builds_for_bucket(&self, bucket: &Bucket) -> Result<usize> {
        let indexes = index_journal::read_current_index_definitions(
            &self.storage,
            bucket.tenant_id,
            bucket.id,
            false,
        )
        .await?;
        let mut scheduled = 0usize;
        for index in indexes {
            if self.enqueue_index_build_for_index(bucket, &index).await? {
                scheduled = scheduled.saturating_add(1);
            }
        }
        Ok(scheduled)
    }

    pub async fn build_index_task(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        index_id: i64,
        index_version: i64,
        source_cursor: u128,
    ) -> Result<Option<index_builder::IndexBuildOutcome>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("index build bucket tenant mismatch"));
        }
        let Some(index) = index_journal::read_current_index_definitions(
            &self.storage,
            tenant_id,
            bucket_id,
            true,
        )
        .await?
        .into_iter()
        .find(|index| index.id == index_id) else {
            return Ok(None);
        };
        if !index.enabled || index.version != index_version {
            return Ok(None);
        }
        let outcome = match index.kind.as_str() {
            "full_text" => {
                index_builder::build_full_text_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                )
                .await?
            }
            "vector" => {
                index_builder::build_vector_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                )
                .await?
            }
            "hybrid" => {
                index_builder::build_hybrid_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                )
                .await?
            }
            _ => return Ok(None),
        };
        for diagnostic in &outcome.diagnostics {
            self.create_index_diagnostic(
                tenant_id,
                bucket_id,
                &bucket.name,
                Some(index.id),
                &index.name,
                &diagnostic.object_key,
                diagnostic.version_id,
                &diagnostic.severity,
                &diagnostic.code,
                &diagnostic.message,
                diagnostic.details.clone(),
            )
            .await?;
        }
        Ok(Some(outcome))
    }

    pub async fn repair_index_from_base_journal(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        index_name: &str,
        rebuild: bool,
    ) -> Result<index_repair::IndexRepairReport> {
        let bucket = self
            .get_bucket_by_name(tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        let index = self
            .get_index_definition(tenant_id, bucket.id, index_name)
            .await?
            .filter(|index| index.enabled)
            .ok_or_else(|| anyhow!("index definition not found"))?;
        if !matches!(index.kind.as_str(), "full_text" | "vector" | "hybrid") {
            return Err(anyhow!(
                "index kind does not have a repairable derived index"
            ));
        }

        let stats = metadata_journal::active_object_journal_stats(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        let source_cursor = index_repair::source_cursor_from_stats(stats);
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let source_manifest_hash = if source_cursor == 0 {
            String::new()
        } else {
            metadata_journal::object_metadata_source_checkpoint_hash(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
                source_cursor,
            )
            .await?
        };

        let mut status = index_repair::assess_derived_index(
            &self.storage,
            &index,
            &index_storage_id,
            source_cursor,
            &source_manifest_hash,
            &self.partition_owner_signing_key,
        )
        .await?;
        let mut build = None;
        let mut finding = None;

        if let index_repair::IndexRepairStatus::NeedsRepair(reason) = status.clone() {
            let permit = self
                .object_metadata_write_permit(bucket.tenant_id, bucket.id)
                .await?;
            if rebuild {
                build = self
                    .build_index_task(tenant_id, bucket.id, index.id, index.version, source_cursor)
                    .await?;
                status = index_repair::IndexRepairStatus::Rebuilt(reason.clone());
            }

            let finding_status = if rebuild {
                repair_finding::RepairFindingStatus::RebuiltDerivedIndex
            } else {
                repair_finding::RepairFindingStatus::Open
            };
            let write = index_repair::repair_finding_write(
                &bucket,
                &index,
                &index_storage_id,
                source_cursor,
                &source_manifest_hash,
                &reason,
                finding_status,
                permit.fence_token,
            )?;
            finding = Some(
                repair_finding::write_repair_finding(
                    &self.storage,
                    write,
                    &self.partition_owner_signing_key,
                )
                .await?,
            );
        }

        Ok(index_repair::IndexRepairReport {
            status,
            bucket_name: bucket.name,
            index_name: index.name,
            index_storage_id,
            source_cursor,
            finding,
            build,
        })
    }

    pub async fn repair_directory_index(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        rebuild: bool,
    ) -> Result<directory_repair::DirectoryIndexRepairReport> {
        let bucket = self
            .get_bucket_by_name(tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        directory_repair::repair_directory_index(
            &self.storage,
            &bucket,
            rebuild,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_repair_findings(
        &self,
        scope_kind: &str,
        scope_id: &str,
        limit: usize,
    ) -> Result<Vec<repair_finding::RepairFinding>> {
        let mut findings = repair_finding::list_repair_findings(
            &self.storage,
            scope_kind,
            scope_id,
            &self.partition_owner_signing_key,
        )
        .await?;
        if limit > 0 && findings.len() > limit {
            findings.truncate(limit);
        }
        Ok(findings)
    }

    pub async fn repair_authz_derived_userset_index(
        &self,
        tenant_id: i64,
        derived_index_id: &str,
        rebuild: bool,
    ) -> Result<authz_repair::AuthzDerivedIndexRepairReport> {
        let permit = self.authz_write_permit(tenant_id).await?;
        authz_repair::repair_authz_derived_userset_index(
            &self.storage,
            tenant_id,
            derived_index_id,
            rebuild,
            permit.fence_token,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn repair_personaldb_log_chain(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<personaldb_repair::PersonalDbLogChainRepairReport> {
        let scope_id = format!("tenant-{tenant_id}-database-{database_id}");
        let permit = self.repair_write_permit("personaldb", &scope_id).await?;
        personaldb_repair::repair_personaldb_log_chain(
            &self.storage,
            tenant_id,
            database_id,
            permit.fence_token,
            &self.personaldb_signing_key,
            &self.partition_owner_signing_key,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
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
        let permit = self
            .index_diagnostic_write_permit(tenant_id, bucket_id)
            .await?;
        index_diagnostic_journal::write_index_diagnostic_with_permit(
            &self.storage,
            IndexDiagnostic {
                id: 0,
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
            },
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
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
        index_diagnostic_journal::read_index_diagnostics(
            &self.storage,
            tenant_id,
            bucket_id,
            index_name,
            severity,
            after_cursor,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }

    pub async fn hard_delete_object(&self, _object_id: i64) -> Result<()> {
        // Object metadata is append-only in the native journal. Physical shard cleanup
        // must not erase the metadata history needed for watches, indexes, and audit.
        Ok(())
    }

    pub async fn enqueue_task(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<()> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::enqueue_task_with_permit(
            &self.storage,
            task_type,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn enqueue_task_if_absent(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<bool> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::enqueue_task_if_absent_with_permit(
            &self.storage,
            task_type,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    async fn enqueue_index_build_task(&self, payload: JsonValue, priority: i32) -> Result<bool> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::enqueue_index_build_task_with_permit(
            &self.storage,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn acquire_task_execution_lease(
        &self,
        task: &TaskRecord,
    ) -> Result<task_lease::TaskLease> {
        let target = self.task_lease_target(task).await?;
        let now_nanos = current_time_nanos()?;
        let ttl_nanos = self.task_lease_ttl_nanos()?;
        task_lease::acquire_task_lease(
            &self.storage,
            task_lease::TaskLeaseAcquire {
                task_id: task_lease_id(task.id)?,
                task_kind: task.task_type.as_str().to_string(),
                partition_family: target.partition_family,
                partition_id: target.partition_id,
                owner: task_lease::TaskLeaseOwner::node(self.owner_node_id.clone()),
                source_cursor: target.source_cursor,
                now_nanos,
                ttl_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn checkpoint_task_execution_lease(
        &self,
        lease: &task_lease::TaskLease,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease(
            &self.storage,
            &lease.task_id,
            &task_lease::TaskLeaseOwner::node(self.owner_node_id.clone()),
            lease.fence_token,
            checkpoint_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn acquire_named_task_lease(
        &self,
        request: task_lease::TaskLeaseAcquire,
    ) -> Result<task_lease::TaskLease> {
        task_lease::acquire_task_lease(&self.storage, request, &self.partition_owner_signing_key)
            .await
    }

    pub async fn checkpoint_named_task_lease(
        &self,
        task_id: &str,
        owner: &task_lease::TaskLeaseOwner,
        fence_token: u64,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease(
            &self.storage,
            task_id,
            owner,
            fence_token,
            checkpoint_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn commit_named_task_lease(
        &self,
        task_id: &str,
        owner: &task_lease::TaskLeaseOwner,
        fence_token: u64,
        committed_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::commit_task_lease(
            &self.storage,
            task_id,
            owner,
            fence_token,
            committed_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_named_task_lease(
        &self,
        tenant_id: i64,
        task_id: &str,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::read_task_lease(
            &self.storage,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn force_release_named_task_lease(
        &self,
        tenant_id: i64,
        task_id: &str,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::force_release_task_lease(
            &self.storage,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_task_execution_lease(
        &self,
        task_id: i64,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::read_task_lease(
            &self.storage,
            0,
            &task_lease_id(task_id)?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    async fn task_lease_target(&self, task: &TaskRecord) -> Result<TaskLeaseTarget> {
        match task.task_type {
            crate::tasks::TaskType::ObjectMetadataCompaction => {
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id)
                    .await?
                    .ok_or_else(|| anyhow!("object metadata compaction bucket not found"))?;
                let stats = metadata_journal::active_object_journal_stats(
                    &self.storage,
                    &bucket,
                    &self.partition_owner_signing_key,
                )
                .await?;
                Ok(TaskLeaseTarget {
                    partition_family: "object_metadata".to_string(),
                    partition_id: hex::encode(metadata_journal::object_metadata_partition_id(
                        bucket.tenant_id,
                        bucket.id,
                    )),
                    source_cursor: u128::from(stats.last_sequence),
                })
            }
            crate::tasks::TaskType::IndexBuild => {
                let tenant_id = task_payload_i64(task, "tenant_id")?;
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let index_id = task_payload_i64(task, "index_id")?;
                let source_cursor = task_payload_u128(task, "source_cursor")?;
                Ok(TaskLeaseTarget {
                    partition_family: "index".to_string(),
                    partition_id: hex::encode(crate::formats::hash32(
                        format!("tenant/{tenant_id}/bucket/{bucket_id}/index/{index_id}")
                            .as_bytes(),
                    )),
                    source_cursor,
                })
            }
            _ => Ok(TaskLeaseTarget {
                partition_family: "task_queue".to_string(),
                partition_id: hex::encode(task_journal::task_queue_partition_id()),
                source_cursor: task.id.max(0) as u128,
            }),
        }
    }

    fn task_lease_ttl_nanos(&self) -> Result<i64> {
        if self.task_lease_ttl_secs == 0 {
            return Err(anyhow!("task lease ttl must be nonzero"));
        }
        let ttl = self
            .task_lease_ttl_secs
            .checked_mul(1_000_000_000)
            .ok_or_else(|| anyhow!("task lease ttl overflow"))?;
        i64::try_from(ttl).map_err(|_| anyhow!("task lease ttl cannot fit i64 nanoseconds"))
    }

    pub async fn claim_pending_tasks(&self, limit: i64) -> Result<Vec<TaskRecord>> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::claim_pending_tasks_with_permit(
            &self.storage,
            limit,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_tasks(&self) -> Result<Vec<TaskRecord>> {
        task_journal::list_tasks(&self.storage).await
    }

    pub async fn update_task_status(
        &self,
        task_id: i64,
        status: crate::tasks::TaskStatus,
    ) -> Result<()> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::update_task_status_with_permit(
            &self.storage,
            task_id,
            status,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn fail_task(&self, task_id: i64, error: &str) -> Result<()> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::fail_task_with_permit(
            &self.storage,
            task_id,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_create_key(
        &self,
        name: &str,
        token_encrypted: &[u8],
        note: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::create_key_with_permit(
            &self.storage,
            name,
            token_encrypted,
            note,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_delete_key(&self, name: &str) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::delete_key_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_key_encrypted(&self, name: &str) -> Result<Option<(i64, Vec<u8>)>> {
        hf_journal::get_key_encrypted(&self.storage, name).await
    }

    pub async fn hf_get_key_encrypted_by_id(&self, id: i64) -> Result<Option<Vec<u8>>> {
        hf_journal::get_key_encrypted_by_id(&self.storage, id).await
    }

    pub async fn hf_list_keys(
        &self,
    ) -> Result<Vec<(String, Option<String>, DateTime<Utc>, DateTime<Utc>)>> {
        hf_journal::list_keys(&self.storage).await
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
        let permit = self.hf_write_permit().await?;
        hf_journal::create_ingestion_with_permit(
            &self.storage,
            key_id,
            tenant_id,
            requester_app_id,
            repo,
            revision,
            target_bucket,
            target_region,
            target_prefix,
            include_globs,
            exclude_globs,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_ingestion_job(&self, id: i64) -> Result<Option<HfIngestionJob>> {
        hf_journal::get_ingestion_job(&self.storage, id).await
    }

    pub async fn hf_update_ingestion_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionState,
        error: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_ingestion_state_with_permit(
            &self.storage,
            id,
            state_value,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_cancel_ingestion(&self, id: i64) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::cancel_ingestion_with_permit(
            &self.storage,
            id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_add_item(
        &self,
        ingestion_id: i64,
        path: &str,
        size: Option<i64>,
        etag: Option<&str>,
    ) -> Result<i64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::add_item_with_permit(
            &self.storage,
            ingestion_id,
            path,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_update_item_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionItemState,
        error: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_item_state_with_permit(
            &self.storage,
            id,
            state_value,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_update_item_success(&self, id: i64, size: i64, etag: &str) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_item_success_with_permit(
            &self.storage,
            id,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_ingestion_items(
        &self,
        ingestion_id: i64,
    ) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
        hf_journal::get_ingestion_items(&self.storage, ingestion_id).await
    }

    pub async fn hf_get_all_items_for_prefix(
        &self,
        tenant_id: i64,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
        hf_journal::get_all_items_for_prefix(&self.storage, tenant_id, bucket, prefix).await
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
        hf_journal::status_summary(&self.storage, id).await
    }
}

fn persistence_owner_node_id(config: &Config) -> String {
    if !config.public_api_addr.is_empty() {
        return config.public_api_addr.clone();
    }
    if !config.api_listen_addr.is_empty() {
        return config.api_listen_addr.clone();
    }
    if !config.region.is_empty() {
        return config.region.clone();
    }
    "local-anvil-node".to_string()
}

fn current_time_nanos() -> Result<i64> {
    Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))
}

fn task_lease_id(task_id: i64) -> Result<String> {
    if task_id <= 0 {
        return Err(anyhow!("task id must be positive"));
    }
    Ok(format!("task-{task_id}"))
}

fn task_payload_i64(task: &TaskRecord, field: &'static str) -> Result<i64> {
    task.payload
        .get(field)
        .and_then(JsonValue::as_i64)
        .ok_or_else(|| anyhow!("task {} payload must include integer {field}", task.id))
}

fn task_payload_u128(task: &TaskRecord, field: &'static str) -> Result<u128> {
    task.payload
        .get(field)
        .and_then(JsonValue::as_u64)
        .map(u128::from)
        .ok_or_else(|| {
            anyhow!(
                "task {} payload must include unsigned integer {field}",
                task.id
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::{BinaryEnvelopeHeader, COMMON_HEADER_LEN, JournalFrame};
    use serde_json::json;
    use tempfile::tempdir;

    fn test_config(storage_path: &std::path::Path) -> Config {
        Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "test-region".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            ..Config::default()
        }
    }

    fn model_manifest() -> crate::anvil_api::ModelManifest {
        crate::anvil_api::ModelManifest {
            schema_version: "1".to_string(),
            artifact_id: "artifact-a".to_string(),
            name: "artifact-a".to_string(),
            format: "test".to_string(),
            components: Vec::new(),
            base_artifact_id: String::new(),
            delta_artifact_ids: Vec::new(),
            signatures: Vec::new(),
            merkle_root: "abc".to_string(),
            meta: std::collections::HashMap::new(),
        }
    }

    async fn assert_journal_is_fenced(path: impl AsRef<std::path::Path>) {
        let bytes = tokio::fs::read(path).await.unwrap();
        let header = BinaryEnvelopeHeader::decode(&bytes).unwrap();
        let header_json: serde_json::Value = serde_json::from_slice(&header.header_json).unwrap();
        let header_fence = header_json["fence_token"].as_u64().unwrap();
        assert!(header_fence > 0);

        let mut input = &bytes[COMMON_HEADER_LEN + header.header_json.len()..];
        let mut frames = Vec::new();
        while !input.is_empty() {
            let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
            let frame_end = 4 + frame_len;
            frames.push(JournalFrame::decode(&input[..frame_end]).unwrap());
            input = &input[frame_end..];
        }
        assert!(!frames.is_empty());
        assert!(frames.iter().all(|frame| frame.fence_token == header_fence));
    }

    #[tokio::test]
    async fn persistence_replays_anvil_owned_state_after_fresh_instance() {
        let temp = tempdir().unwrap();
        let first_config = test_config(temp.path());
        let persistence = Persistence::new(&first_config, None).unwrap();

        persistence.create_region("local").await.unwrap();
        let tenant = persistence
            .create_tenant("tenant-a", "unused")
            .await
            .unwrap();
        let app = persistence
            .create_app(tenant.id, "app-a", "client-a", b"encrypted-secret")
            .await
            .unwrap();
        persistence
            .grant_policy(app.id, "bucket:docs", "read")
            .await
            .unwrap();

        let bucket = persistence
            .create_bucket(tenant.id, "docs", "local")
            .await
            .unwrap();
        let object = persistence
            .create_object(
                tenant.id,
                bucket.id,
                "project/a.txt",
                "payload-hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                Some(json!({"label": "alpha"})),
                None,
                Some(b"hello world".to_vec()),
            )
            .await
            .unwrap();
        persistence
            .create_object(
                tenant.id,
                bucket.id,
                "project/nested/b.txt",
                "payload-hash-b",
                12,
                "etag-b",
                Some("text/plain"),
                None,
                None,
                Some(b"hello again".to_vec()),
            )
            .await
            .unwrap();

        let upload = persistence
            .create_multipart_upload(tenant.id, bucket.id, "uploads/large.bin")
            .await
            .unwrap()
            .upload;
        persistence
            .upsert_multipart_part(upload.id, 1, "part-hash-a", 4, "part-etag-a")
            .await
            .unwrap();

        let append_stream = persistence
            .create_append_stream(tenant.id, bucket.id, &bucket.name, "events")
            .await
            .unwrap()
            .stream;
        persistence
            .append_stream_record(append_stream.id, "event-payload-hash", 42)
            .await
            .unwrap();

        let manifest = persistence
            .compare_and_swap_manifest(
                tenant.id,
                bucket.id,
                &bucket.name,
                "manifests/current.json",
                0,
                json!({"generation": 1}),
                "manifest-hash-a",
            )
            .await
            .unwrap()
            .unwrap();

        let index = persistence
            .create_index_definition(
                tenant.id,
                bucket.id,
                "body",
                "full_text",
                json!({"prefix": "project/"}),
                json!({"field": "body"}),
                "inherit",
                json!({"mode": "watch"}),
            )
            .await
            .unwrap();
        persistence
            .create_index_definition_event(tenant.id, bucket.id, &bucket.name, &index, "create")
            .await
            .unwrap();
        persistence
            .create_index_diagnostic(
                tenant.id,
                bucket.id,
                &bucket.name,
                Some(index.id),
                &index.name,
                &object.key,
                Some(object.version_id),
                "warning",
                "diagnostic-alpha",
                "synthetic diagnostic for replay coverage",
                json!({"source": "test"}),
            )
            .await
            .unwrap();

        let authz = persistence
            .write_authz_tuple(
                tenant.id,
                "document",
                &object.key,
                "reader",
                "user",
                "user-a",
                "",
                "add",
                "test",
                "grant reader",
            )
            .await
            .unwrap();
        persistence
            .enqueue_task(
                crate::tasks::TaskType::DeleteBucket,
                json!({"bucket_id": bucket.id}),
                5,
            )
            .await
            .unwrap();
        persistence
            .create_model_artifact("artifact-a", tenant.id, "models/a", &model_manifest())
            .await
            .unwrap();
        persistence
            .hf_create_key("primary", b"secret", Some("note"))
            .await
            .unwrap();

        drop(persistence);

        let restarted_config = Config {
            public_api_addr: "test-node-after-restart".to_string(),
            ..first_config
        };
        let replayed = Persistence::new(&restarted_config, None).unwrap();

        assert!(
            replayed
                .list_regions()
                .await
                .unwrap()
                .contains(&"local".to_string())
        );
        assert_eq!(
            replayed
                .get_tenant_by_name("tenant-a")
                .await
                .unwrap()
                .unwrap()
                .id,
            tenant.id
        );
        assert_eq!(
            replayed
                .get_app_by_client_id("client-a")
                .await
                .unwrap()
                .unwrap()
                .id,
            app.id
        );
        assert_eq!(
            replayed.get_policies_for_app(app.id).await.unwrap(),
            vec!["read|bucket:docs".to_string()]
        );
        assert_eq!(
            replayed
                .get_bucket_by_name(tenant.id, "docs")
                .await
                .unwrap()
                .unwrap()
                .id,
            bucket.id
        );

        let replayed_object = replayed
            .get_object(bucket.id, "project/a.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed_object.version_id, object.version_id);
        assert_eq!(
            replayed_object.inline_payload.as_deref(),
            Some(&b"hello world"[..])
        );
        assert_eq!(replayed_object.user_meta.unwrap()["label"], "alpha");

        let (objects, common_prefixes) = replayed
            .list_objects(bucket.id, "project/", "", 100, "/")
            .await
            .unwrap();
        assert_eq!(
            objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["project/a.txt"]
        );
        assert_eq!(common_prefixes, vec!["project/nested/".to_string()]);
        assert_eq!(
            replayed
                .list_object_versions(bucket.id, "project/", "", None, 100)
                .await
                .unwrap()
                .versions
                .len(),
            2
        );

        assert_eq!(
            replayed
                .get_active_multipart_upload(
                    tenant.id,
                    bucket.id,
                    "uploads/large.bin",
                    upload.upload_id
                )
                .await
                .unwrap()
                .unwrap()
                .id,
            upload.id
        );
        assert_eq!(
            replayed
                .list_multipart_parts(upload.id)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            replayed
                .list_append_stream_records(append_stream.id)
                .await
                .unwrap()
                .len(),
            1
        );

        let second_manifest = replayed
            .compare_and_swap_manifest(
                tenant.id,
                bucket.id,
                &bucket.name,
                "manifests/current.json",
                manifest.revision,
                json!({"generation": 2}),
                "manifest-hash-b",
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second_manifest.revision, manifest.revision + 1);

        assert_eq!(
            replayed
                .list_index_definitions(tenant.id, bucket.id, false)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            replayed
                .list_index_definition_events(tenant.id, bucket.id, 0, 100)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            replayed
                .list_index_diagnostics(tenant.id, bucket.id, &index.name, "", 0, 100)
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            replayed
                .check_authz_tuple(
                    tenant.id,
                    "document",
                    &object.key,
                    "reader",
                    "user",
                    "user-a",
                    "",
                )
                .await
                .unwrap()
                .unwrap()
                .revision,
            authz.revision
        );
        assert_eq!(replayed.list_tasks().await.unwrap().len(), 1);
        assert!(
            replayed
                .get_model_artifact("artifact-a")
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(replayed.hf_list_keys().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn persistence_compacts_object_metadata_and_restarts_from_manifest() {
        let temp = tempdir().unwrap();
        let first_config = test_config(temp.path());
        let persistence = Persistence::new(&first_config, None).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "compact-bucket", "local")
            .await
            .unwrap();
        let first = persistence
            .create_object(
                1,
                bucket.id,
                "docs/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                Some(json!({"label": "a"})),
                None,
                Some(b"alpha".to_vec()),
            )
            .await
            .unwrap();
        persistence
            .create_object(
                1,
                bucket.id,
                "docs/nested/b.txt",
                "hash-b",
                12,
                "etag-b",
                Some("text/plain"),
                None,
                None,
                Some(b"bravo".to_vec()),
            )
            .await
            .unwrap();

        let sealed = persistence
            .compact_object_metadata(bucket.id)
            .await
            .unwrap()
            .expect("object metadata journal should compact");
        assert_eq!(sealed.metadata_record_count, 2);
        assert_eq!(sealed.directory_record_count, 2);

        drop(persistence);
        let restarted_config = Config {
            public_api_addr: "test-node-after-compaction".to_string(),
            ..first_config
        };
        let restarted = Persistence::new(&restarted_config, None).unwrap();

        let replayed = restarted
            .get_object(bucket.id, "docs/a.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed.version_id, first.version_id);
        assert_eq!(replayed.inline_payload.as_deref(), Some(&b"alpha"[..]));
        assert_eq!(replayed.user_meta.unwrap()["label"], "a");

        let (objects, common_prefixes) = restarted
            .list_objects(bucket.id, "docs/", "", 100, "/")
            .await
            .unwrap();
        assert_eq!(
            objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/a.txt"]
        );
        assert_eq!(common_prefixes, vec!["docs/nested/".to_string()]);
        assert_eq!(
            restarted
                .list_object_versions(bucket.id, "docs/", "", None, 100)
                .await
                .unwrap()
                .versions
                .len(),
            2
        );

        let replacement = restarted
            .create_object(
                1,
                bucket.id,
                "docs/a.txt",
                "hash-c",
                13,
                "etag-c",
                Some("text/plain"),
                None,
                None,
                Some(b"charlie".to_vec()),
            )
            .await
            .unwrap();
        let (objects_after_append, _) = restarted
            .list_objects(bucket.id, "docs/", "", 100, "/")
            .await
            .unwrap();
        assert_eq!(objects_after_append[0].version_id, replacement.version_id);
        assert_eq!(objects_after_append[0].content_hash, "hash-c");
        assert_eq!(
            restarted
                .list_object_versions(bucket.id, "docs/a.txt", "", None, 100)
                .await
                .unwrap()
                .versions
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn persistence_schedules_deduplicated_object_metadata_compaction_tasks() {
        let temp = tempdir().unwrap();
        let config = Config {
            object_metadata_compaction_frame_threshold: 2,
            object_metadata_compaction_bytes_threshold: 0,
            ..test_config(temp.path())
        };
        let persistence = Persistence::new(&config, None).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "scheduled-compact-bucket", "local")
            .await
            .unwrap();
        persistence
            .create_object(
                1,
                bucket.id,
                "objects/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                Some(b"alpha".to_vec()),
            )
            .await
            .unwrap();

        let tasks = persistence.list_tasks().await.unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(
            tasks[0].task_type,
            crate::tasks::TaskType::ObjectMetadataCompaction
        );
        assert_eq!(tasks[0].payload, json!({ "bucket_id": bucket.id }));

        persistence
            .create_object(
                1,
                bucket.id,
                "objects/b.txt",
                "hash-b",
                12,
                "etag-b",
                Some("text/plain"),
                None,
                None,
                Some(b"bravo".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            persistence.list_tasks().await.unwrap().len(),
            1,
            "live compaction task should be deduplicated per bucket"
        );

        let claimed = persistence.claim_pending_tasks(1).await.unwrap();
        persistence
            .compact_object_metadata(bucket.id)
            .await
            .unwrap();
        persistence
            .update_task_status(claimed[0].id, crate::tasks::TaskStatus::Completed)
            .await
            .unwrap();

        persistence
            .create_object(
                1,
                bucket.id,
                "objects/c.txt",
                "hash-c",
                13,
                "etag-c",
                Some("text/plain"),
                None,
                None,
                Some(b"charlie".to_vec()),
            )
            .await
            .unwrap();
        assert_eq!(
            persistence.list_tasks().await.unwrap().len(),
            2,
            "new post-compaction journal frames should schedule a new task"
        );
    }

    #[tokio::test]
    async fn persistence_task_execution_lease_targets_object_metadata_partition() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let persistence = Persistence::new(&config, None).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "lease-target-bucket", "local")
            .await
            .unwrap();
        persistence
            .create_object(
                1,
                bucket.id,
                "objects/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                Some(b"alpha".to_vec()),
            )
            .await
            .unwrap();

        let now = Utc::now();
        let task = TaskRecord {
            id: 77,
            task_type: crate::tasks::TaskType::ObjectMetadataCompaction,
            payload: json!({ "bucket_id": bucket.id }),
            priority: 0,
            status: crate::tasks::TaskStatus::Running,
            attempts: 1,
            last_error: None,
            scheduled_at: now,
            created_at: now,
            updated_at: now,
        };
        let lease = persistence
            .acquire_task_execution_lease(&task)
            .await
            .unwrap();
        assert_eq!(lease.task_id, "task-77");
        assert_eq!(lease.task_kind, "OBJECT_METADATA_COMPACTION");
        assert_eq!(lease.partition_family, "object_metadata");
        assert_eq!(
            lease.partition_id,
            hex::encode(metadata_journal::object_metadata_partition_id(1, bucket.id))
        );
        assert!(
            lease.source_cursor >= 2,
            "object PUT should create object-version and directory frames"
        );

        let read_back = persistence
            .read_task_execution_lease(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(read_back, lease);

        let competing_config = Config {
            public_api_addr: "other-worker-node".to_string(),
            ..config
        };
        let competing = Persistence::new(&competing_config, None).unwrap();
        let err = competing
            .acquire_task_execution_lease(&task)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("owned by another active node"));

        let checkpointed = persistence
            .checkpoint_task_execution_lease(&lease, lease.source_cursor)
            .await
            .unwrap();
        assert_eq!(checkpointed.checkpoint_cursor, lease.source_cursor);
    }

    #[tokio::test]
    async fn persistence_global_journal_writes_use_current_fence_tokens() {
        let temp = tempdir().unwrap();
        let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "bucket-a", "local")
            .await
            .unwrap();
        let object = persistence
            .create_object(
                1,
                bucket.id,
                "objects/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        persistence
            .soft_delete_object(bucket.id, &object.key)
            .await
            .unwrap();
        let upload = persistence
            .create_multipart_upload(1, bucket.id, "objects/large.bin")
            .await
            .unwrap()
            .upload;
        persistence
            .upsert_multipart_part(upload.id, 1, "part-hash", 12, "part-etag")
            .await
            .unwrap();
        persistence
            .complete_multipart_upload(upload.id)
            .await
            .unwrap();
        let stream = persistence
            .create_append_stream(1, bucket.id, &bucket.name, "stream-a")
            .await
            .unwrap()
            .stream;
        persistence
            .append_stream_record(stream.id, "payload-hash", 13)
            .await
            .unwrap();
        persistence
            .seal_append_stream(stream.id, "segment-hash")
            .await
            .unwrap();
        persistence
            .compare_and_swap_manifest(
                1,
                bucket.id,
                &bucket.name,
                "manifest.json",
                0,
                json!({"version": 1}),
                "manifest-hash",
            )
            .await
            .unwrap();
        let index = persistence
            .create_index_definition(
                1,
                bucket.id,
                "body",
                "full_text",
                json!({"prefix": "objects/"}),
                json!({"field": "body"}),
                "inherit",
                json!({"mode": "sync"}),
            )
            .await
            .unwrap();
        persistence
            .create_index_definition_event(1, bucket.id, &bucket.name, &index, "create")
            .await
            .unwrap();
        persistence
            .create_index_diagnostic(
                1,
                bucket.id,
                &bucket.name,
                Some(index.id),
                &index.name,
                &object.key,
                Some(object.version_id),
                "warning",
                "test-warning",
                "diagnostic",
                json!({"source": "test"}),
            )
            .await
            .unwrap();
        persistence
            .write_authz_tuple(
                1,
                "object",
                &object.key,
                "reader",
                "user",
                "user-a",
                "",
                "add",
                "test",
                "test grant",
            )
            .await
            .unwrap();
        persistence
            .enqueue_task(
                crate::tasks::TaskType::DeleteBucket,
                json!({"bucket_id": 7}),
                1,
            )
            .await
            .unwrap();
        persistence
            .create_model_artifact("artifact-a", 1, "models/a", &model_manifest())
            .await
            .unwrap();
        persistence
            .hf_create_key("primary", b"secret", Some("note"))
            .await
            .unwrap();

        assert_journal_is_fenced(persistence.storage.control_journal_path()).await;
        assert_journal_is_fenced(persistence.storage.task_queue_journal_path()).await;
        assert_journal_is_fenced(persistence.storage.model_metadata_journal_path()).await;
        assert_journal_is_fenced(persistence.storage.hf_journal_path()).await;
        assert_journal_is_fenced(persistence.storage.bucket_metadata_journal_path(1)).await;
        assert_journal_is_fenced(persistence.storage.global_bucket_metadata_journal_path()).await;
        assert_journal_is_fenced(persistence.storage.metadata_journal_path(1, bucket.id)).await;
        assert_journal_is_fenced(persistence.storage.multipart_journal_path(1, bucket.id)).await;
        assert_journal_is_fenced(persistence.storage.append_journal_path(1, bucket.id)).await;
        assert_journal_is_fenced(persistence.storage.manifest_cas_journal_path(1, bucket.id)).await;
        assert_journal_is_fenced(
            persistence
                .storage
                .index_definition_journal_path(1, bucket.id),
        )
        .await;
        assert_journal_is_fenced(
            persistence
                .storage
                .index_diagnostic_journal_path(1, bucket.id),
        )
        .await;
        assert_journal_is_fenced(persistence.storage.authz_tuple_journal_path(1)).await;
    }
}
