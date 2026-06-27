use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::Value as JsonValue;
use tokio_postgres::Row;

use crate::cache::MetadataCache;
use crate::cluster::MetadataEvent;
use tokio::sync::mpsc::Sender;

#[derive(Debug, Clone)]
pub struct Persistence {
    global_pool: Pool,
    regional_pool: Pool,
    cache: MetadataCache,
    event_publisher: Option<Sender<MetadataEvent>>,
}

// Structs that map to our database tables
#[derive(Debug, Clone, serde::Serialize)]
pub struct Tenant {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, serde::Serialize)]
pub struct App {
    pub id: i64,
    pub name: String,
    pub client_id: String,
}

#[derive(Debug, Clone)]
pub struct Bucket {
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub region: String,
    pub created_at: DateTime<Utc>,
    pub is_public_read: bool,
}

#[derive(Debug, Clone)]
pub struct BucketMetadataEvent {
    pub id: i64,
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub bucket_name: String,
    pub event_type: String,
    pub bucket_metadata: JsonValue,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct ObjectVersion {
    pub object: Object,
    pub is_delete_marker: bool,
    pub is_latest: bool,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct MultipartUploadsPage {
    pub uploads: Vec<MultipartUpload>,
    pub is_truncated: bool,
    pub next_key_marker: Option<String>,
    pub next_upload_id_marker: Option<uuid::Uuid>,
}

#[derive(Debug, Clone)]
pub struct MultipartUploadPart {
    pub id: i64,
    pub upload_id: i64,
    pub part_number: i32,
    pub content_hash: String,
    pub size: i64,
    pub etag: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct AppendStreamRecord {
    pub id: i64,
    pub stream_id: i64,
    pub record_sequence: i64,
    pub payload_hash: String,
    pub payload_size: i64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ManifestCasResult {
    pub revision: i64,
    pub manifest_hash: String,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

// Manual row-to-struct mapping
impl From<Row> for Tenant {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            name: row.get("name"),
        }
    }
}

impl From<Row> for App {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            name: row.get("name"),
            client_id: row.get("client_id"),
        }
    }
}

impl From<Row> for Bucket {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            name: row.get("name"),
            region: row.get("region"),
            created_at: row.get("created_at"),
            is_public_read: row.get("is_public_read"),
        }
    }
}

impl From<Row> for BucketMetadataEvent {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            bucket_name: row.get("bucket_name"),
            event_type: row.get("event_type"),
            bucket_metadata: row.get("bucket_metadata"),
            created_at: row.get("created_at"),
        }
    }
}

impl From<Row> for Object {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            key: row.get("key"),
            content_hash: row.get("content_hash"),
            size: row.get("size"),
            etag: row.get("etag"),
            content_type: row.get("content_type"),
            version_id: row.get("version_id"),
            mutation_id: row.get("mutation_id"),
            index_policy_snapshot: row.get("index_policy_snapshot"),
            user_metadata_hash: row.get("user_metadata_hash"),
            authz_revision: row.get("authz_revision"),
            record_hash: row.get("record_hash"),
            created_at: row.get("created_at"),
            deleted_at: row.get("deleted_at"),
            storage_class: row.get("storage_class"),
            user_meta: row.get("user_meta"),
            shard_map: row.get("shard_map"),
            inline_payload: row.get("inline_payload"),
            checksum: row.get("checksum"),
        }
    }
}

impl From<Row> for MultipartUpload {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            key: row.get("key"),
            upload_id: row.get("upload_id"),
            created_at: row.get("created_at"),
            completed_at: row.get("completed_at"),
            aborted_at: row.get("aborted_at"),
        }
    }
}

impl From<Row> for MultipartUploadPart {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            upload_id: row.get("upload_id"),
            part_number: row.get("part_number"),
            content_hash: row.get("content_hash"),
            size: row.get("size"),
            etag: row.get("etag"),
            created_at: row.get("created_at"),
        }
    }
}

impl From<Row> for ObjectWatchEvent {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            bucket_name: row.get("bucket_name"),
            key: row.get("key"),
            event_type: row.get("event_type"),
            version_id: row.get("version_id"),
            etag: row.get("etag"),
            size: row.get("size"),
            is_delete_marker: row.get("is_delete_marker"),
            created_at: row.get("created_at"),
        }
    }
}

impl From<Row> for AppendStream {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            bucket_name: row.get("bucket_name"),
            stream_key: row.get("stream_key"),
            stream_id: row.get("stream_id"),
            created_at: row.get("created_at"),
            sealed_at: row.get("sealed_at"),
            segment_hash: row.get("segment_hash"),
        }
    }
}

impl From<Row> for AppendStreamRecord {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            stream_id: row.get("stream_id"),
            record_sequence: row.get("record_sequence"),
            payload_hash: row.get("payload_hash"),
            payload_size: row.get("payload_size"),
            created_at: row.get("created_at"),
        }
    }
}

impl From<Row> for AuthzTupleRecord {
    fn from(row: Row) -> Self {
        Self {
            revision: row.get("revision"),
            tenant_id: row.get("tenant_id"),
            namespace: row.get("namespace"),
            object_id: row.get("object_id"),
            relation: row.get("relation"),
            subject_kind: row.get("subject_kind"),
            subject_id: row.get("subject_id"),
            caveat_hash: row.get("caveat_hash"),
            operation: row.get("operation"),
            written_by: row.get("written_by"),
            reason: row.get("reason"),
            record_hash: row.get("record_hash"),
            written_at: row.get("written_at"),
        }
    }
}

impl From<Row> for IndexDefinition {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            name: row.get("name"),
            kind: row.get("kind"),
            selector: row.get("selector"),
            extractor: row.get("extractor"),
            authorization_mode: row.get("authorization_mode"),
            build_policy: row.get("build_policy"),
            enabled: row.get("enabled"),
            version: row.get("version"),
            created_at: row.get("created_at"),
            updated_at: row.get("updated_at"),
        }
    }
}

impl From<Row> for IndexDefinitionEvent {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            bucket_name: row.get("bucket_name"),
            index_id: row.get("index_id"),
            index_name: row.get("index_name"),
            event_type: row.get("event_type"),
            index_version: row.get("index_version"),
            definition: row.get("definition"),
            created_at: row.get("created_at"),
        }
    }
}

impl From<Row> for IndexDiagnostic {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            tenant_id: row.get("tenant_id"),
            bucket_id: row.get("bucket_id"),
            bucket_name: row.get("bucket_name"),
            index_id: row.get("index_id"),
            index_name: row.get("index_name"),
            object_key: row.get("object_key"),
            version_id: row.get("version_id"),
            severity: row.get("severity"),
            code: row.get("code"),
            message: row.get("message"),
            details: row.get("details"),
            created_at: row.get("created_at"),
        }
    }
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

pub struct AppDetails {
    pub id: i64,
    pub client_secret_encrypted: Vec<u8>,
    pub tenant_id: i64,
}

#[derive(Debug, serde::Serialize)]
pub struct AdminUser {
    pub id: i64,
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub is_active: bool,
}

#[derive(Debug, serde::Serialize)]
pub struct AdminRole {
    pub id: i32,
    pub name: String,
}

impl From<Row> for AppDetails {
    fn from(row: Row) -> Self {
        Self {
            id: row.get("id"),
            client_secret_encrypted: row.get("client_secret_encrypted"),
            tenant_id: row.get("tenant_id"),
        }
    }
}

impl Persistence {
    pub fn new(
        global_pool: Pool,
        regional_pool: Pool,
        event_publisher: Option<Sender<MetadataEvent>>,
        config: &crate::config::Config,
    ) -> Self {
        Self {
            global_pool,
            regional_pool,
            cache: MetadataCache::new(config),
            event_publisher,
        }
    }

    async fn publish_event(&self, event: MetadataEvent) {
        if let Some(publisher) = &self.event_publisher {
            if let Err(e) = publisher.send(event).await {
                tracing::warn!("Failed to publish metadata event: {}", e);
            }
        }
    }

    pub async fn get_admin_user_by_username(&self, username: &str) -> Result<Option<AdminUser>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt("SELECT id, username, email, password_hash, is_active FROM admin_users WHERE username = $1", &[&username])
            .await?;
        Ok(row.map(|r| AdminUser {
            id: r.get("id"),
            username: r.get("username"),
            email: r.get("email"),
            password_hash: r.get("password_hash"),
            is_active: r.get("is_active"),
        }))
    }

    pub async fn get_admin_user_by_id(&self, id: i64) -> Result<Option<AdminUser>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt("SELECT id, username, email, password_hash, is_active FROM admin_users WHERE id = $1", &[&id])
            .await?;
        Ok(row.map(|r| AdminUser {
            id: r.get("id"),
            username: r.get("username"),
            email: r.get("email"),
            password_hash: r.get("password_hash"),
            is_active: r.get("is_active"),
        }))
    }

    pub async fn get_roles_for_admin_user(&self, user_id: i64) -> Result<Vec<String>> {
        let client = self.global_pool.get().await?;
        let rows = client.query(
            "SELECT r.name FROM admin_roles r JOIN admin_user_roles ur ON r.id = ur.role_id WHERE ur.user_id = $1",
            &[&user_id],
        ).await?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
    }

    pub fn get_global_pool(&self) -> &Pool {
        &self.global_pool
    }

    pub fn cache(&self) -> &MetadataCache {
        &self.cache
    }

    pub async fn create_admin_user(
        &self,
        username: &str,
        email: &str,
        password_hash: &str,
        role: &str,
    ) -> Result<()> {
        let mut client = self.global_pool.get().await?;
        let tx = client.transaction().await?;

        let user_id: i64 = tx.query_one(
            "INSERT INTO admin_users (username, email, password_hash) VALUES ($1, $2, $3) RETURNING id",
            &[&username, &email, &password_hash],
        ).await?.get(0);

        let role_id: i32 = tx
            .query_one("SELECT id FROM admin_roles WHERE name = $1", &[&role])
            .await?
            .get(0);

        tx.execute(
            "INSERT INTO admin_user_roles (user_id, role_id) VALUES ($1, $2)",
            &[&user_id, &role_id],
        )
        .await?;

        tx.commit().await?;
        Ok(())
    }

    pub async fn update_admin_user(
        &self,
        user_id: i64,
        email: Option<String>,
        password_hash: Option<String>,
        role: Option<String>,
        is_active: Option<bool>,
    ) -> Result<()> {
        let client = self.global_pool.get().await?;
        let mut query_parts = Vec::new();
        let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync + Send>> = Vec::new();
        let mut param_idx = 1;

        if let Some(e) = email {
            query_parts.push(format!("email = ${}", param_idx));
            params.push(Box::new(e));
            param_idx += 1;
        }
        if let Some(p) = password_hash {
            query_parts.push(format!("password_hash = ${}", param_idx));
            params.push(Box::new(p));
            param_idx += 1;
        }
        if let Some(a) = is_active {
            query_parts.push(format!("is_active = ${}", param_idx));
            params.push(Box::new(a));
            param_idx += 1;
        }

        if query_parts.is_empty() {
            // Nothing to update
            return Ok(());
        }

        let query = format!(
            "UPDATE admin_users SET {} WHERE id = ${}",
            query_parts.join(", "),
            param_idx
        );
        params.push(Box::new(user_id));

        let param_refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params
            .iter()
            .map(|p| p.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync))
            .collect();
        client.execute(&query, &param_refs).await?;

        if let Some(r) = role {
            let role_id: i32 = client
                .query_one("SELECT id FROM admin_roles WHERE name = $1", &[&r])
                .await?
                .get(0);
            client
                .execute(
                    "UPDATE admin_user_roles SET role_id = $1 WHERE user_id = $2",
                    &[&role_id, &user_id],
                )
                .await?;
        }

        Ok(())
    }

    pub async fn delete_admin_user(&self, user_id: i64) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute("DELETE FROM admin_users WHERE id = $1", &[&user_id])
            .await?;
        Ok(())
    }

    pub async fn list_admin_users(&self) -> Result<Vec<AdminUser>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT id, username, email, password_hash, is_active FROM admin_users",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| AdminUser {
                id: r.get("id"),
                username: r.get("username"),
                email: r.get("email"),
                password_hash: r.get("password_hash"),
                is_active: r.get("is_active"),
            })
            .collect())
    }

    pub async fn create_admin_role(&self, name: &str) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute("INSERT INTO admin_roles (name) VALUES ($1)", &[&name])
            .await?;
        Ok(())
    }

    pub async fn list_admin_roles(&self) -> Result<Vec<String>> {
        let client = self.global_pool.get().await?;
        let rows = client.query("SELECT name FROM admin_roles", &[]).await?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
    }

    pub async fn get_admin_role_by_id(&self, id: i32) -> Result<Option<AdminRole>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt("SELECT id, name FROM admin_roles WHERE id = $1", &[&id])
            .await?;
        Ok(row.map(|r| AdminRole {
            id: r.get("id"),
            name: r.get("name"),
        }))
    }

    pub async fn update_admin_role(&self, id: i32, name: &str) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "UPDATE admin_roles SET name = $1 WHERE id = $2",
                &[&name, &id],
            )
            .await?;
        Ok(())
    }

    pub async fn delete_admin_role(&self, id: i32) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute("DELETE FROM admin_roles WHERE id = $1", &[&id])
            .await?;
        Ok(())
    }

    pub async fn list_policies(&self) -> Result<Vec<String>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query("SELECT resource, action FROM policies", &[])
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| {
                format!(
                    "{}:{}",
                    r.get::<_, String>("action"),
                    r.get::<_, String>("resource")
                )
            })
            .collect())
    }

    // --- Model Registry Methods ---

    pub async fn create_model_artifact(
        &self,
        artifact_id: &str,
        bucket_id: i64,
        key: &str,
        manifest: &crate::anvil_api::ModelManifest,
    ) -> Result<()> {
        let client = self.regional_pool.get().await?;
        let manifest_json = serde_json::to_value(manifest)?;
        client
            .execute(
                "INSERT INTO model_artifacts (artifact_id, bucket_id, key, manifest) VALUES ($1, $2, $3, $4)",
                &[&artifact_id, &bucket_id, &key, &manifest_json],
            )
            .await?;
        Ok(())
    }

    pub async fn create_model_tensors(
        &self,
        artifact_id: &str,
        tensors: &[crate::anvil_api::TensorIndexRow],
    ) -> Result<()> {
        if tensors.is_empty() {
            return Ok(());
        }
        let client = self.regional_pool.get().await?;
        let sink = client.copy_in("COPY model_tensors (artifact_id, tensor_name, file_path, file_offset, byte_length, dtype, shape, layout, block_bytes, blocks) FROM STDIN").await?;

        use bytes::Bytes;
        use futures_util::SinkExt;
        use std::pin::pin;

        let mut writer = pin!(sink);

        for tensor in tensors {
            let shape_array = format!(
                "{{{}}}",
                tensor
                    .shape
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            let blocks_json = serde_json::to_string(&tensor.blocks)?;

            let row_string = format!(
                "{}	{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                artifact_id,
                tensor.tensor_name,
                tensor.file_path,
                tensor.file_offset,
                tensor.byte_length,
                tensor.dtype,
                shape_array,
                tensor.layout,
                tensor.block_bytes,
                blocks_json
            );
            writer.send(Bytes::from(row_string)).await?;
        }
        writer.close().await?;
        Ok(())
    }

    pub async fn list_tensors(
        &self,
        artifact_id: &str,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<crate::anvil_api::TensorIndexRow>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                "SELECT tensor_name, file_path, file_offset, byte_length, dtype, shape, layout, block_bytes, blocks FROM model_tensors WHERE artifact_id = $1 ORDER BY tensor_name LIMIT $2 OFFSET $3",
                &[&artifact_id, &limit, &offset],
            )
            .await?;

        let tensors = rows
            .into_iter()
            .map(|row| {
                let shape: Vec<i32> = row.get("shape");
                let shape_u32: Vec<u32> = shape.into_iter().map(|i| i as u32).collect();
                let file_offset: i64 = row.get("file_offset");
                let byte_length: i64 = row.get("byte_length");
                let dtype_str: String = row.get("dtype");
                let block_bytes: i32 = row.get("block_bytes");
                crate::anvil_api::TensorIndexRow {
                    tensor_name: row.get("tensor_name"),
                    file_path: row.get("file_path"),
                    file_offset: file_offset as u64,
                    byte_length: byte_length as u64,
                    dtype: dtype_str.parse::<i32>().unwrap_or(0),
                    shape: shape_u32,
                    layout: row.get("layout"),
                    block_bytes: block_bytes as u32,
                    blocks: serde_json::from_value(row.get("blocks")).unwrap_or_default(),
                }
            })
            .collect();
        Ok(tensors)
    }

    pub async fn get_tensor_metadata(
        &self,
        artifact_id: &str,
        tensor_name: &str,
    ) -> Result<Option<crate::anvil_api::TensorIndexRow>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                "SELECT tensor_name, file_path, file_offset, byte_length, dtype, shape, layout, block_bytes, blocks FROM model_tensors WHERE artifact_id = $1 AND tensor_name = $2",
                &[&artifact_id, &tensor_name],
            )
            .await?;

        Ok(row.map(|row| {
            let shape: Vec<i32> = row.get("shape");
            let shape_u32: Vec<u32> = shape.into_iter().map(|i| i as u32).collect();
            let file_offset: i64 = row.get("file_offset");
            let byte_length: i64 = row.get("byte_length");
            let dtype_str: String = row.get("dtype");
            let block_bytes: i32 = row.get("block_bytes");
            crate::anvil_api::TensorIndexRow {
                tensor_name: row.get("tensor_name"),
                file_path: row.get("file_path"),
                file_offset: file_offset as u64,
                byte_length: byte_length as u64,
                dtype: dtype_str.parse::<i32>().unwrap_or(0),
                shape: shape_u32,
                layout: row.get("layout"),
                block_bytes: block_bytes as u32,
                blocks: serde_json::from_value(row.get("blocks")).unwrap_or_default(),
            }
        }))
    }

    pub async fn get_model_artifact(
        &self,
        artifact_id: &str,
    ) -> Result<Option<crate::anvil_api::ModelManifest>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                "SELECT manifest FROM model_artifacts WHERE artifact_id = $1",
                &[&artifact_id],
            )
            .await?;

        match row {
            Some(row) => {
                let manifest_json: serde_json::Value = row.get("manifest");
                let manifest: crate::anvil_api::ModelManifest =
                    serde_json::from_value(manifest_json)?;
                Ok(Some(manifest))
            }
            None => Ok(None),
        }
    }

    pub async fn get_tensor_metadata_recursive(
        &self,
        artifact_id: &str,
        tensor_name: &str,
    ) -> Result<Option<crate::anvil_api::TensorIndexRow>> {
        // 1. Try to find the tensor in the current artifact.
        if let Some(tensor) = self.get_tensor_metadata(artifact_id, tensor_name).await? {
            return Ok(Some(tensor));
        }

        // 2. If not found, get the current artifact's manifest to find its base.
        if let Some(manifest) = self.get_model_artifact(artifact_id).await? {
            if !manifest.base_artifact_id.is_empty() {
                // 3. If it has a base, recurse.
                return Box::pin(
                    self.get_tensor_metadata_recursive(&manifest.base_artifact_id, tensor_name),
                )
                .await;
            }
        }

        // 4. If we've reached the end of the chain, it's not found.
        Ok(None)
    }

    // --- Global Methods ---

    pub async fn create_region(&self, name: &str) -> Result<bool> {
        let client = self.global_pool.get().await?;
        let n = client
            .execute(
                "INSERT INTO regions (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
                &[&name],
            )
            .await?;
        Ok(n == 1)
    }

    pub async fn list_regions(&self) -> Result<Vec<String>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query("SELECT name FROM regions ORDER BY name", &[])
            .await?;
        Ok(rows.into_iter().map(|r| r.get("name")).collect())
    }

    pub async fn get_tenant_by_name(&self, name: &str) -> Result<Option<Tenant>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt("SELECT id, name FROM tenants WHERE name = $1", &[&name])
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_tenants(&self) -> Result<Vec<Tenant>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query("SELECT id, name FROM tenants ORDER BY name", &[])
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn get_app_by_client_id(&self, client_id: &str) -> Result<Option<AppDetails>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
                "SELECT id, client_secret_encrypted, tenant_id FROM apps WHERE client_id = $1",
                &[&client_id],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn get_policies_for_app(&self, app_id: i64) -> Result<Vec<String>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT resource, action FROM policies WHERE app_id = $1",
                &[&app_id],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                format!(
                    "{}|{}",
                    row.get::<_, String>("action"),
                    row.get::<_, String>("resource")
                )
            })
            .collect())
    }

    pub async fn create_tenant(&self, name: &str, api_key: &str) -> Result<Tenant> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                "INSERT INTO tenants (name, api_key) VALUES ($1, $2) RETURNING id, name",
                &[&name, &api_key],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn create_app(
        &self,
        tenant_id: i64,
        name: &str,
        client_id: &str,
        client_secret_encrypted: &[u8],
    ) -> Result<App> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                "INSERT INTO apps (tenant_id, name, client_id, client_secret_encrypted) VALUES ($1, $2, $3, $4) RETURNING id, name, client_id",
                &[&tenant_id, &name, &client_id, &client_secret_encrypted],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_app_by_id(&self, id: i64) -> Result<Option<App>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt("SELECT id, name, client_id FROM apps WHERE id = $1", &[&id])
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn get_app_by_name(&self, name: &str) -> Result<Option<App>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
                "SELECT id, name, client_id FROM apps WHERE name = $1",
                &[&name],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_apps_for_tenant(&self, tenant_id: i64) -> Result<Vec<App>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT id, name, client_id FROM apps WHERE tenant_id = $1 ORDER BY name",
                &[&tenant_id],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn update_app_secret(&self, app_id: i64, new_encrypted_secret: &[u8]) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "UPDATE apps SET client_secret_encrypted = $1 WHERE id = $2",
                &[&new_encrypted_secret, &app_id],
            )
            .await?;
        Ok(())
    }

    pub async fn grant_policy(&self, app_id: i64, resource: &str, action: &str) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "INSERT INTO policies (app_id, resource, action) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
                &[&app_id, &resource, &action],
            )
            .await?;
        Ok(())
    }

    pub async fn revoke_policy(&self, app_id: i64, resource: &str, action: &str) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "DELETE FROM policies WHERE app_id = $1 AND resource = $2 AND action = $3",
                &[&app_id, &resource, &action],
            )
            .await?;
        Ok(())
    }

    pub async fn create_bucket(
        &self,
        tenant_id: i64,
        name: &str,
        region: &str,
    ) -> Result<Bucket, tonic::Status> {
        tracing::debug!(
            "[Persistence] ENTERING create_bucket: tenant_id={}, name={}, region={}",
            tenant_id,
            name,
            region
        );
        let client = self
            .global_pool
            .get()
            .await
            .map_err(|e| tonic::Status::internal(format!("Failed to get DB client: {}", e)))?;
        let result = client
            .query_one(
                "INSERT INTO buckets (tenant_id, name, region) VALUES ($1, $2, $3) RETURNING *",
                &[&tenant_id, &name, &region],
            )
            .await;

        match result {
            Ok(row) => {
                tracing::debug!("[Persistence] EXITING create_bucket: success");
                let bucket: Bucket = row.into();
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
            Err(e) => {
                tracing::debug!("[Persistence] EXITING create_bucket: error");
                if let Some(db_err) = e.as_db_error() {
                    tracing::error!(?db_err, "Database error on create_bucket");
                    if db_err.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION {
                        return Err(tonic::Status::already_exists(
                            "A bucket with that name already exists.",
                        ));
                    }
                }
                Err(tonic::Status::internal(e.to_string()))
            }
        }
    }

    pub async fn get_bucket_by_name(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        // Check cache first
        if let Some(bucket) = self.cache.get_bucket(tenant_id, name).await {
            return Ok(Some(bucket));
        }

        let client = self.global_pool.get().await?;
        // Removed region constraint
        let row = client
            .query_opt(
            "SELECT id, name, region, created_at, is_public_read, tenant_id FROM buckets WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL",
                &[&tenant_id, &name],
            )
            .await?;

        if let Some(row) = row {
            let bucket: Bucket = row.into();
            self.cache
                .insert_bucket(tenant_id, name.to_string(), bucket.clone())
                .await;
            Ok(Some(bucket))
        } else {
            Ok(None)
        }
    }

    pub async fn get_public_bucket_by_name(&self, name: &str) -> Result<Option<Bucket>> {
        if let Some(bucket) = self.cache.get_bucket_by_name_only(name).await {
            if bucket.is_public_read {
                return Ok(Some(bucket));
            }
            // If cached but not public, return None (effectively hiding it)
            return Ok(None);
        }

        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
                "SELECT * FROM buckets WHERE name = $1 AND deleted_at IS NULL",
                &[&name],
            )
            .await?;

        if let Some(row) = row {
            let bucket: Bucket = row.into();
            // We cache it regardless of public status so we don't hit DB repeatedly for non-public buckets?
            // Or only if public?
            // My `buckets_by_name` cache is generic. It's better to cache it.
            self.cache
                .insert_bucket(bucket.tenant_id, name.to_string(), bucket.clone())
                .await;

            if bucket.is_public_read {
                Ok(Some(bucket))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn set_bucket_public_access(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        is_public: bool,
    ) -> Result<Bucket> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                "UPDATE buckets SET is_public_read = $1 WHERE tenant_id = $2 AND name = $3 RETURNING *",
                &[&is_public, &tenant_id, &bucket_name],
            )
            .await?;

        let bucket: Bucket = row.into();
        self.cache.invalidate_bucket(tenant_id, bucket_name).await;
        self.publish_event(MetadataEvent::BucketUpdated {
            tenant_id,
            name: bucket_name.to_string(),
        })
        .await;

        Ok(bucket)
    }

    pub async fn set_bucket_public_access_by_name(
        &self,
        bucket_name: &str,
        is_public: bool,
    ) -> Result<Bucket> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                "UPDATE buckets SET is_public_read = $1 WHERE name = $2 RETURNING *",
                &[&is_public, &bucket_name],
            )
            .await?;

        let bucket: Bucket = row.into();
        self.cache
            .invalidate_bucket(bucket.tenant_id, bucket_name)
            .await;
        self.publish_event(MetadataEvent::BucketUpdated {
            tenant_id: bucket.tenant_id,
            name: bucket_name.to_string(),
        })
        .await;

        Ok(bucket)
    }

    pub async fn soft_delete_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<Option<Bucket>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
                r#"UPDATE buckets SET deleted_at = now() WHERE tenant_id = $1 AND name = $2 AND deleted_at IS NULL RETURNING *"#,
                &[&tenant_id, &bucket_name],
            )
            .await?;

        if row.is_some() {
            self.cache.invalidate_bucket(tenant_id, bucket_name).await;
            self.publish_event(MetadataEvent::BucketUpdated {
                tenant_id,
                name: bucket_name.to_string(),
            })
            .await;
        }

        Ok(row.map(Into::into))
    }

    pub async fn bucket_has_retained_objects_or_uploads(&self, bucket_id: i64) -> Result<bool> {
        let client = self.regional_pool.get().await?;
        let object_exists = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM objects WHERE bucket_id = $1)",
                &[&bucket_id],
            )
            .await?
            .get::<_, bool>(0);
        if object_exists {
            return Ok(true);
        }

        let active_upload_exists = client
            .query_one(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM multipart_uploads
                    WHERE bucket_id = $1 AND completed_at IS NULL AND aborted_at IS NULL
                )"#,
                &[&bucket_id],
            )
            .await?
            .get::<_, bool>(0);
        Ok(active_upload_exists)
    }

    pub async fn create_bucket_metadata_event(
        &self,
        tenant_id: i64,
        bucket: &Bucket,
        event_type: &str,
        bucket_metadata: JsonValue,
    ) -> Result<BucketMetadataEvent> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO bucket_metadata_events
                    (tenant_id, bucket_id, bucket_name, event_type, bucket_metadata)
                VALUES ($1, $2, $3, $4, $5)
                RETURNING *"#,
                &[
                    &tenant_id,
                    &bucket.id,
                    &bucket.name,
                    &event_type,
                    &bucket_metadata,
                ],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn list_bucket_metadata_events(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<BucketMetadataEvent>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM bucket_metadata_events
                WHERE tenant_id = $1
                  AND ($2 = '' OR bucket_name = $2)
                  AND id > $3
                ORDER BY id
                LIMIT $4"#,
                &[
                    &tenant_id,
                    &bucket_name,
                    &after_cursor,
                    &(if limit == 0 { 1000 } else { limit } as i64),
                ],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_buckets_for_tenant(&self, tenant_id: i64) -> Result<Vec<Bucket>> {
        tracing::debug!(
            "[Persistence] ENTERING list_buckets_for_tenant: tenant_id={}",
            tenant_id
        );
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT * FROM buckets WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY name",
                &[&tenant_id],
            )
            .await?;
        let buckets: Vec<Bucket> = rows.into_iter().map(Into::into).collect();
        tracing::debug!(
            "[Persistence] EXITING list_buckets_for_tenant, found {} buckets",
            buckets.len()
        );
        Ok(buckets)
    }

    // --- Regional Methods ---

    pub async fn active_index_policy_snapshot_hash(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<String> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT name, kind, selector, extractor, authorization_mode, build_policy, version
                FROM index_definitions
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND enabled
                ORDER BY name, id"#,
                &[&tenant_id, &bucket_id],
            )
            .await?;

        let mut hasher = blake3::Hasher::new();
        for row in rows {
            let name: String = row.get("name");
            let kind: String = row.get("kind");
            let selector: JsonValue = row.get("selector");
            let extractor: JsonValue = row.get("extractor");
            let authorization_mode: String = row.get("authorization_mode");
            let build_policy: JsonValue = row.get("build_policy");
            let version: i64 = row.get("version");
            hasher.update(name.as_bytes());
            hasher.update(&[0]);
            hasher.update(kind.as_bytes());
            hasher.update(&[0]);
            hasher.update(selector.to_string().as_bytes());
            hasher.update(&[0]);
            hasher.update(extractor.to_string().as_bytes());
            hasher.update(&[0]);
            hasher.update(authorization_mode.as_bytes());
            hasher.update(&[0]);
            hasher.update(build_policy.to_string().as_bytes());
            hasher.update(&[0]);
            hasher.update(&version.to_le_bytes());
        }
        Ok(hasher.finalize().to_hex().to_string())
    }

    pub async fn latest_authz_revision(&self, tenant_id: i64) -> Result<i64> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                "SELECT COALESCE(MAX(revision), 0)::BIGINT AS revision FROM authz_tuple_log WHERE tenant_id = $1",
                &[&tenant_id],
            )
            .await?;
        Ok(row.get("revision"))
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO objects
                    (tenant_id, bucket_id, key, content_hash, size, etag, version_id, mutation_id,
                     content_type, user_meta, shard_map, inline_payload, index_policy_snapshot, user_metadata_hash,
                     authz_revision, record_hash)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                RETURNING *;"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &key,
                    &content_hash,
                    &size,
                    &etag,
                    &version_id,
                    &mutation_id,
                    &content_type,
                    &user_meta,
                    &shard_map,
                    &inline_payload,
                    &index_policy_snapshot,
                    &user_metadata_hash,
                    &authz_revision,
                    &record_hash,
                ],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"SELECT * FROM objects WHERE bucket_id = $1 AND key = $2 ORDER BY created_at DESC, id DESC LIMIT 1"#,
                &[&bucket_id, &key],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let object: Object = row.into();
        if object.deleted_at.is_some() {
            return Ok(None);
        }
        Ok(Some(object))
    }

    pub async fn get_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"SELECT * FROM objects WHERE bucket_id = $1 AND key = $2 AND version_id = $3"#,
                &[&bucket_id, &key, &version_id],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    /// List objects and (optionally) "common prefixes" (aka pseudo-folders).
    ///
    /// - When `delimiter` is empty: returns up to `limit` objects whose `key`
    ///   starts with `prefix` and are lexicographically `> start_after`.
    /// - When `delimiter` is non-empty: returns up to `limit` entries across the
    ///   **merged, lexicographic** stream of:
    ///     • objects that are the first-level children under `prefix` (no further delimiter),
    ///     • common prefixes representing deeper descendants at that first level.
    ///   The function still returns `(objects, common_prefixes)` separately, but the
    ///   single `limit` applies to the merged stream (i.e., total returned =
    ///   `objects.len() + common_prefixes.len() <= limit`).
    ///
    /// Notes:
    /// - Avoids `ltree` cast errors by trimming/cleaning trailing slashes/dots,
    ///   removing empty segments, and mapping invalid label characters.
    /// - Uses `key_ltree <@ prefix_ltree` for proper descendant matching.
    /// - Orders deterministically, and applies `LIMIT` after interleaving.
    /// - Objects fetched by key are re-ordered by `key`.
    pub async fn list_objects(
        &self,
        bucket_id: i64,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>)> {
        use regex::Regex;

        // Helper: map an arbitrary key segment to a valid ltree label.
        // Must mirror whatever you used when populating `objects.key_ltree`.
        // Here we use a conservative mapping: A-Za-z0-9_ only; others -> "_".
        fn ltree_labelize(seg: &str) -> String {
            // If your ingestion uses a different normalization, replace this to match it.
            let mut out = String::with_capacity(seg.len());
            for (i, ch) in seg.chars().enumerate() {
                let valid = ch.is_ascii_alphanumeric() || ch == '_';
                if i == 0 {
                    // label must start with alpha (ltree requirement). If not, prefix with 'x'
                    if ch.is_ascii_alphabetic() {
                        out.push(ch.to_ascii_lowercase());
                    } else if valid {
                        out.push('x');
                        out.push(ch.to_ascii_lowercase());
                    } else {
                        out.push('x');
                        out.push('_');
                    }
                } else {
                    out.push(if valid { ch.to_ascii_lowercase() } else { '_' });
                }
            }
            if out.is_empty() { "x".to_owned() } else { out }
        }

        // Normalize `prefix` into an ltree dot-path that is safe to cast.
        // - trim leading/trailing delimiters ('/')
        // - collapse multiple slashes
        // - drop empty segments
        // - ltree-labelize each segment
        // IMPORTANT: this must match how you built `key_ltree` at write time.
        let slash_re = Regex::new(r"/+").unwrap();
        let cleaned_prefix_slash = slash_re
            .replace_all(prefix.trim_matches('/'), "/")
            .to_string();

        let prefix_segments: Vec<String> = cleaned_prefix_slash
            .split('/')
            .filter(|s| !s.is_empty())
            .map(ltree_labelize)
            .collect();

        let prefix_dot = prefix_segments.join(".");

        // Fast path: no delimiter => simple ordered list of objects.
        if delimiter.is_empty() {
            let client = self.regional_pool.get().await?;
            let rows = client
                .query(
                    r#"
                    SELECT id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, mutation_id, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash, created_at, storage_class, user_meta, shard_map, inline_payload, checksum, deleted_at, key_ltree
                    FROM (
                      SELECT DISTINCT ON (key)
                        id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, mutation_id, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash, created_at, storage_class, user_meta, shard_map, inline_payload, checksum, deleted_at, key_ltree
                      FROM objects
                      WHERE bucket_id = $1 AND key > $2 AND left(key, length($3)) = $3
                        AND key !~ '^_anvil/(meta|index|authz|watch|personaldb|git|tmp)(/|$)'
                      ORDER BY key, created_at DESC, id DESC
                    ) latest
                    WHERE deleted_at IS NULL
                    ORDER BY key
                    LIMIT $4"#,
                    &[
                        &bucket_id,
                        &start_after,
                        &prefix,
                        &(limit as i64),
                    ],
                )
                .await?;
            let objects = rows.into_iter().map(Into::into).collect();
            return Ok((objects, vec![]));
        }

        // Delimiter path: interleave first-level objects and prefixes and apply a single LIMIT.
        let client = self.regional_pool.get().await?;

        // We keep $4 as TEXT; cast to ltree with NULLIF in SQL to avoid "Unexpected end of input".
        // When empty, treat as the root (nlevel = 0) and skip the <@ check.
        let rows = client
            .query(
                r#"
            WITH
            params AS (
              SELECT
                $1::bigint AS bucket_id,
                $2::text   AS start_after,
                $3::int8   AS lim,
                NULLIF($4::text, '')::ltree AS prefix_ltree,
                $5::text AS prefix_text
            ),
            lvl AS (
              SELECT COALESCE(nlevel(prefix_ltree), 0) AS p FROM params
            ),
            relevant AS (
              SELECT o.key, o.key_ltree
              FROM (
                SELECT DISTINCT ON (key) key, key_ltree, deleted_at, bucket_id
                FROM objects
                WHERE bucket_id = $1
                  AND key !~ '^_anvil/(meta|index|authz|watch|personaldb|git|tmp)(/|$)'
                ORDER BY key, created_at DESC, id DESC
              ) o, params p
              WHERE o.bucket_id = p.bucket_id
                AND o.deleted_at IS NULL
                AND o.key > p.start_after
                AND left(o.key, length(p.prefix_text)) = p.prefix_text
                AND (p.prefix_ltree IS NULL OR o.key_ltree <@ p.prefix_ltree)
            ),
            children AS (
              SELECT
                key,
                key_ltree,
                subpath(
                  key_ltree,
                  0,
                  (SELECT p FROM lvl) + 1
                ) AS child_path,
                nlevel(key_ltree) AS lvl
              FROM relevant
            ),
            grouped AS (
              SELECT
                child_path,
                MIN(key) AS min_key,
                BOOL_OR(nlevel(key_ltree) > nlevel(child_path)) AS has_descendants_below,
                COUNT(*) FILTER (WHERE key_ltree = child_path) AS exact_object_count
              FROM children
              GROUP BY child_path
            ),
            -- Build a unified, lexicographically sorted stream of rows, then LIMIT.
            stream AS (
              -- Common prefixes: return only those whose first visible key is > start_after
              SELECT
                ltree2text(g.child_path) AS sort_key,
                NULL::text               AS object_key,
                TRUE                     AS is_prefix
              FROM grouped g, params p
              WHERE g.has_descendants_below
                AND g.min_key > p.start_after

              UNION ALL

              -- Objects that are exactly first-level children (no deeper slash beyond prefix)
              SELECT
                ltree2text(c.child_path) AS sort_key,
                c.key                    AS object_key,
                FALSE                    AS is_prefix
              FROM children c
              WHERE c.key_ltree = c.child_path
            )
            SELECT sort_key, object_key, is_prefix
            FROM stream
            ORDER BY sort_key, is_prefix DESC  -- object (false) before prefix (true) for same sort_key
            LIMIT (SELECT lim FROM params)
            "#,
                &[&bucket_id, &start_after, &(limit as i64), &prefix_dot, &prefix],
            )
            .await?;

        // Split the unified stream into object keys vs prefixes (preserving order).
        let mut object_keys: Vec<String> = Vec::new();
        let mut common_prefixes: Vec<String> = Vec::new();

        for row in &rows {
            let sort_key: String = row.get("sort_key"); // dot path
            let is_prefix: bool = row.get("is_prefix");
            let slash_path = sort_key.replace('.', "/");

            if is_prefix {
                // Convert to caller's delimiter at the very end.
                let mut pref = if delimiter == "/" {
                    format!("{}/", slash_path)
                } else {
                    // Replace slashes with requested delimiter and append delimiter once.
                    let replaced = if slash_path.is_empty() {
                        String::new()
                    } else {
                        slash_path.replace('/', delimiter)
                    };
                    format!("{}{}", replaced, delimiter)
                };
                // Ensure it still starts with the provided (string) prefix for nice UX
                // (only when using non-'/' delimiters this might differ). This is optional:
                if !prefix.is_empty() && !pref.starts_with(prefix) && delimiter == "/" {
                    // For safety; usually unnecessary if keys are consistent.
                    pref = format!("{}/", prefix.trim_end_matches('/'));
                }
                common_prefixes.push(pref);
            } else {
                let key: String = row.get("object_key");
                object_keys.push(key);
            }
        }

        // Fetch object rows (if any) with deterministic ordering.
        let objects = if !object_keys.is_empty() {
            let rows = client
                .query(
                    r#"
                    SELECT id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, mutation_id, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash, created_at, storage_class, user_meta, shard_map, inline_payload, checksum, deleted_at, key_ltree
                    FROM (
                      SELECT DISTINCT ON (key)
                        id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, mutation_id, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash, created_at, storage_class, user_meta, shard_map, inline_payload, checksum, deleted_at, key_ltree
                      FROM objects
                      WHERE bucket_id = $1 AND key = ANY($2)
                        AND key !~ '^_anvil/(meta|index|authz|watch|personaldb|git|tmp)(/|$)'
                      ORDER BY key, created_at DESC, id DESC
                    ) latest
                    WHERE deleted_at IS NULL
                    ORDER BY key"#,
                    &[&bucket_id, &object_keys],
                )
                .await?;
            rows.into_iter().map(Into::into).collect()
        } else {
            Vec::new()
        };

        Ok((objects, common_prefixes))
    }

    pub async fn soft_delete_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let source = client
            .query_opt(
                r#"
                SELECT *
                FROM objects
                WHERE bucket_id = $1 AND key = $2
                ORDER BY created_at DESC, id DESC
                LIMIT 1"#,
                &[&bucket_id, &key],
            )
            .await?;
        let Some(source) = source else {
            return Ok(None);
        };
        let source: Object = source.into();
        let version_id = uuid::Uuid::new_v4();
        let mutation_id = uuid::Uuid::new_v4();
        let index_policy_snapshot = self
            .active_index_policy_snapshot_hash(source.tenant_id, bucket_id)
            .await?;
        let authz_revision = self.latest_authz_revision(source.tenant_id).await?;
        let user_metadata_hash = user_metadata_hash(None);
        let record_hash = object_version_record_hash(ObjectVersionRecordHashInput {
            tenant_id: source.tenant_id,
            bucket_id,
            key,
            version_id,
            mutation_id,
            content_hash: "",
            size: 0,
            etag: "",
            content_type: None,
            user_metadata_hash: &user_metadata_hash,
            index_policy_snapshot: &index_policy_snapshot,
            authz_revision,
            delete_marker: true,
        });
        let row = client
            .query_opt(
                r#"
                INSERT INTO objects
                    (tenant_id, bucket_id, key, content_hash, size, etag, version_id, mutation_id,
                     deleted_at, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash)
                VALUES ($1, $2, $3, '', 0, '', $4, $5, now(), $6, $7, $8, $9)
                RETURNING *"#,
                &[
                    &source.tenant_id,
                    &bucket_id,
                    &key,
                    &version_id,
                    &mutation_id,
                    &index_policy_snapshot,
                    &user_metadata_hash,
                    &authz_revision,
                    &record_hash,
                ],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn delete_object_version(
        &self,
        bucket_id: i64,
        key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                DELETE FROM objects
                WHERE bucket_id = $1 AND key = $2 AND version_id = $3
                RETURNING *"#,
                &[&bucket_id, &key, &version_id],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_object_versions(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage> {
        let client = self.regional_pool.get().await?;
        let limit = limit.max(1) as i64;
        let fetch_limit = limit + 1;
        let rows = if let Some(version_id_marker) = version_id_marker {
            let marker = client
                .query_opt(
                    r#"
                    SELECT id, created_at
                    FROM objects
                    WHERE bucket_id = $1 AND key = $2 AND version_id = $3"#,
                    &[&bucket_id, &key_marker, &version_id_marker],
                )
                .await?;
            let Some(marker) = marker else {
                return Ok(ObjectVersionsPage {
                    versions: Vec::new(),
                    is_truncated: false,
                    next_key_marker: None,
                    next_version_id_marker: None,
                });
            };
            let marker_id: i64 = marker.get("id");
            let marker_created_at: DateTime<Utc> = marker.get("created_at");
            client
                .query(
                    r#"
                    WITH ranked AS (
                      SELECT
                        id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, mutation_id, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash, created_at, storage_class, user_meta, shard_map, inline_payload, checksum, deleted_at, key_ltree,
                        row_number() OVER (PARTITION BY key ORDER BY created_at DESC, id DESC) = 1 AS is_latest
                      FROM objects
                      WHERE bucket_id = $1 AND left(key, length($2)) = $2
                        AND (
                          key > $3
                          OR (
                            key = $3
                            AND (
                              created_at < $4
                              OR (created_at = $4 AND id < $5)
                            )
                          )
                        )
                        AND key !~ '^_anvil/(meta|index|authz|watch|personaldb|git|tmp)(/|$)'
                    )
                    SELECT *
                    FROM ranked
                    ORDER BY key, created_at DESC, id DESC
                    LIMIT $6"#,
                    &[
                        &bucket_id,
                        &prefix,
                        &key_marker,
                        &marker_created_at,
                        &marker_id,
                        &fetch_limit,
                    ],
                )
                .await?
        } else {
            client
                .query(
                    r#"
                    WITH ranked AS (
                      SELECT
                        id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, mutation_id, index_policy_snapshot, user_metadata_hash, authz_revision, record_hash, created_at, storage_class, user_meta, shard_map, inline_payload, checksum, deleted_at, key_ltree,
                        row_number() OVER (PARTITION BY key ORDER BY created_at DESC, id DESC) = 1 AS is_latest
                      FROM objects
                      WHERE bucket_id = $1 AND key > $2 AND left(key, length($3)) = $3
                        AND key !~ '^_anvil/(meta|index|authz|watch|personaldb|git|tmp)(/|$)'
                    )
                    SELECT *
                    FROM ranked
                    ORDER BY key, created_at DESC, id DESC
                    LIMIT $4"#,
                    &[&bucket_id, &key_marker, &prefix, &fetch_limit],
                )
                .await?
        };

        let mut versions: Vec<ObjectVersion> = rows
            .into_iter()
            .map(|row| {
                let is_latest: bool = row.get("is_latest");
                let object: Object = row.into();
                ObjectVersion {
                    is_delete_marker: object.deleted_at.is_some(),
                    is_latest,
                    object,
                }
            })
            .collect();
        let is_truncated = versions.len() > limit as usize;
        if is_truncated {
            versions.truncate(limit as usize);
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            versions
                .last()
                .map(|version| {
                    (
                        Some(version.object.key.clone()),
                        Some(version.object.version_id),
                    )
                })
                .unwrap_or((None, None))
        } else {
            (None, None)
        };
        Ok(ObjectVersionsPage {
            versions,
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO multipart_uploads (tenant_id, bucket_id, key, upload_id)
                VALUES ($1, $2, $3, gen_random_uuid())
                RETURNING *"#,
                &[&tenant_id, &bucket_id, &key],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_active_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<Option<MultipartUpload>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                SELECT *
                FROM multipart_uploads
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND key = $3
                  AND upload_id = $4
                  AND completed_at IS NULL
                  AND aborted_at IS NULL"#,
                &[&tenant_id, &bucket_id, &key, &upload_id],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn upsert_multipart_part(
        &self,
        upload_row_id: i64,
        part_number: i32,
        content_hash: &str,
        size: i64,
        etag: &str,
    ) -> Result<MultipartUploadPart> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO multipart_upload_parts
                    (upload_id, part_number, content_hash, size, etag)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (upload_id, part_number)
                DO UPDATE SET
                    content_hash = EXCLUDED.content_hash,
                    size = EXCLUDED.size,
                    etag = EXCLUDED.etag,
                    created_at = now()
                RETURNING *"#,
                &[&upload_row_id, &part_number, &content_hash, &size, &etag],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn list_multipart_parts(
        &self,
        upload_row_id: i64,
    ) -> Result<Vec<MultipartUploadPart>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM multipart_upload_parts
                WHERE upload_id = $1
                ORDER BY part_number"#,
                &[&upload_row_id],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn list_active_multipart_uploads(
        &self,
        bucket_id: i64,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<MultipartUploadsPage> {
        let client = self.regional_pool.get().await?;
        let requested_limit = if limit <= 0 { 1000 } else { limit.min(1000) } as i64;
        let marker_text = upload_id_marker
            .filter(|_| !key_marker.is_empty())
            .map(|marker| marker.to_string())
            .unwrap_or_default();
        let rows = client
            .query(
                r#"
                SELECT *
                FROM multipart_uploads
                WHERE bucket_id = $1
                  AND left(key, length($2)) = $2
                  AND (
                      $3 = ''
                      OR key > $3
                      OR (key = $3 AND ($4 = '' OR upload_id::text > $4))
                  )
                  AND completed_at IS NULL
                  AND aborted_at IS NULL
                ORDER BY key, upload_id
                LIMIT $5"#,
                &[
                    &bucket_id,
                    &prefix,
                    &key_marker,
                    &marker_text,
                    &(requested_limit + 1),
                ],
            )
            .await?;
        let mut uploads: Vec<MultipartUpload> = rows.into_iter().map(Into::into).collect();
        let is_truncated = uploads.len() as i64 > requested_limit;
        if is_truncated {
            uploads.truncate(requested_limit as usize);
        }
        let (next_key_marker, next_upload_id_marker) = if is_truncated {
            uploads
                .last()
                .map(|upload| (Some(upload.key.clone()), Some(upload.upload_id)))
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
        let client = self.regional_pool.get().await?;
        client
            .execute(
                r#"
                UPDATE multipart_uploads
                SET completed_at = now()
                WHERE id = $1
                  AND completed_at IS NULL
                  AND aborted_at IS NULL"#,
                &[&upload_row_id],
            )
            .await?;
        Ok(())
    }

    pub async fn abort_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        upload_id: uuid::Uuid,
    ) -> Result<bool> {
        let client = self.regional_pool.get().await?;
        let changed = client
            .execute(
                r#"
                UPDATE multipart_uploads
                SET aborted_at = now()
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND key = $3
                  AND upload_id = $4
                  AND completed_at IS NULL
                  AND aborted_at IS NULL"#,
                &[&tenant_id, &bucket_id, &key, &upload_id],
            )
            .await?;
        Ok(changed > 0)
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO object_watch_events
                    (tenant_id, bucket_id, bucket_name, key, event_type, version_id, etag, size, is_delete_marker)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                RETURNING *"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &bucket_name,
                    &object.key,
                    &event_type,
                    &object.version_id,
                    &object.etag,
                    &object.size,
                    &is_delete_marker,
                ],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn latest_object_watch_cursor(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        version_id: uuid::Uuid,
    ) -> Result<Option<i64>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                SELECT id
                FROM object_watch_events
                WHERE tenant_id = $1 AND bucket_id = $2 AND version_id = $3
                ORDER BY id DESC
                LIMIT 1"#,
                &[&tenant_id, &bucket_id, &version_id],
            )
            .await?;
        Ok(row.map(|row| row.get("id")))
    }

    pub async fn list_object_watch_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        prefix: &str,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<ObjectWatchEvent>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM object_watch_events
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND left(key, length($3)) = $3
                  AND id > $4
                ORDER BY id
                LIMIT $5"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &prefix,
                    &after_cursor,
                    &(if limit == 0 { 1000 } else { limit } as i64),
                ],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn create_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        stream_key: &str,
    ) -> Result<AppendStream> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO append_streams
                    (tenant_id, bucket_id, bucket_name, stream_key, stream_id)
                VALUES ($1, $2, $3, $4, gen_random_uuid())
                RETURNING *"#,
                &[&tenant_id, &bucket_id, &bucket_name, &stream_key],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_active_append_stream(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        stream_key: &str,
        stream_id: uuid::Uuid,
    ) -> Result<Option<AppendStream>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                SELECT *
                FROM append_streams
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND stream_key = $3
                  AND stream_id = $4
                  AND sealed_at IS NULL"#,
                &[&tenant_id, &bucket_id, &stream_key, &stream_id],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn append_stream_record(
        &self,
        append_stream_row_id: i64,
        payload_hash: &str,
        payload_size: i64,
    ) -> Result<AppendStreamRecord> {
        let mut client = self.regional_pool.get().await?;
        let tx = client.transaction().await?;
        let sequence: i64 = tx
            .query_one(
                r#"
                SELECT COALESCE(MAX(record_sequence), 0) + 1
                FROM append_stream_records
                WHERE stream_id = $1"#,
                &[&append_stream_row_id],
            )
            .await?
            .get(0);
        let row = tx
            .query_one(
                r#"
                INSERT INTO append_stream_records
                    (stream_id, record_sequence, payload_hash, payload_size)
                VALUES ($1, $2, $3, $4)
                RETURNING *"#,
                &[
                    &append_stream_row_id,
                    &sequence,
                    &payload_hash,
                    &payload_size,
                ],
            )
            .await?;
        tx.commit().await?;
        Ok(row.into())
    }

    pub async fn list_append_stream_records(
        &self,
        append_stream_row_id: i64,
    ) -> Result<Vec<AppendStreamRecord>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM append_stream_records
                WHERE stream_id = $1
                ORDER BY record_sequence"#,
                &[&append_stream_row_id],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn seal_append_stream(
        &self,
        append_stream_row_id: i64,
        segment_hash: &str,
    ) -> Result<bool> {
        let client = self.regional_pool.get().await?;
        let changed = client
            .execute(
                r#"
                UPDATE append_streams
                SET sealed_at = now(), segment_hash = $2
                WHERE id = $1 AND sealed_at IS NULL"#,
                &[&append_stream_row_id, &segment_hash],
            )
            .await?;
        Ok(changed > 0)
    }

    pub async fn compare_and_swap_manifest(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        manifest_key: &str,
        expected_revision: i64,
        manifest_json: JsonValue,
        manifest_hash: &str,
    ) -> Result<Option<ManifestCasResult>> {
        let mut client = self.regional_pool.get().await?;
        let tx = client.transaction().await?;
        let existing = tx
            .query_opt(
                r#"
                SELECT revision
                FROM object_manifests
                WHERE bucket_id = $1 AND manifest_key = $2
                FOR UPDATE"#,
                &[&bucket_id, &manifest_key],
            )
            .await?;

        let next_revision = match existing {
            Some(row) => {
                let current_revision: i64 = row.get("revision");
                if current_revision != expected_revision {
                    tx.rollback().await?;
                    return Ok(None);
                }
                let next_revision = current_revision + 1;
                tx.execute(
                    r#"
                    UPDATE object_manifests
                    SET revision = $3,
                        manifest_json = $4,
                        manifest_hash = $5,
                        updated_at = now()
                    WHERE bucket_id = $1 AND manifest_key = $2"#,
                    &[
                        &bucket_id,
                        &manifest_key,
                        &next_revision,
                        &manifest_json,
                        &manifest_hash,
                    ],
                )
                .await?;
                next_revision
            }
            None => {
                if expected_revision != 0 {
                    tx.rollback().await?;
                    return Ok(None);
                }
                tx.execute(
                    r#"
                    INSERT INTO object_manifests
                        (tenant_id, bucket_id, bucket_name, manifest_key, revision, manifest_json, manifest_hash)
                    VALUES ($1, $2, $3, $4, 1, $5, $6)"#,
                    &[
                        &tenant_id,
                        &bucket_id,
                        &bucket_name,
                        &manifest_key,
                        &manifest_json,
                        &manifest_hash,
                    ],
                )
                .await?;
                1
            }
        };

        tx.commit().await?;
        Ok(Some(ManifestCasResult {
            revision: next_revision,
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
        record_hash: &str,
    ) -> Result<AuthzTupleRecord> {
        let mut client = self.regional_pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_one(
                r#"
                INSERT INTO authz_tuple_log
                    (tenant_id, namespace, object_id, relation, subject_kind, subject_id,
                     caveat_hash, operation, written_by, reason, record_hash)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING *"#,
                &[
                    &tenant_id,
                    &namespace,
                    &object_id,
                    &relation,
                    &subject_kind,
                    &subject_id,
                    &caveat_hash,
                    &operation,
                    &written_by,
                    &reason,
                    &record_hash,
                ],
            )
            .await?;
        let record: AuthzTupleRecord = row.into();
        tx.execute(
            r#"
            INSERT INTO authz_current_tuples
                (tenant_id, namespace, object_id, relation, subject_kind, subject_id, caveat_hash,
                 operation, revision, written_by, reason, record_hash, written_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            ON CONFLICT (tenant_id, namespace, object_id, relation, subject_kind, subject_id, caveat_hash)
            DO UPDATE SET
                operation = EXCLUDED.operation,
                revision = EXCLUDED.revision,
                written_by = EXCLUDED.written_by,
                reason = EXCLUDED.reason,
                record_hash = EXCLUDED.record_hash,
                written_at = EXCLUDED.written_at"#,
            &[
                &record.tenant_id,
                &record.namespace,
                &record.object_id,
                &record.relation,
                &record.subject_kind,
                &record.subject_id,
                &record.caveat_hash,
                &record.operation,
                &record.revision,
                &record.written_by,
                &record.reason,
                &record.record_hash,
                &record.written_at,
            ],
        )
        .await?;
        tx.commit().await?;
        Ok(record)
    }

    #[allow(clippy::too_many_arguments)]
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                SELECT *
                FROM authz_current_tuples
                WHERE tenant_id = $1
                  AND namespace = $2
                  AND object_id = $3
                  AND relation = $4
                  AND subject_kind = $5
                  AND subject_id = $6
                  AND caveat_hash = $7"#,
                &[
                    &tenant_id,
                    &namespace,
                    &object_id,
                    &relation,
                    &subject_kind,
                    &subject_id,
                    &caveat_hash,
                ],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_authz_tuple_log(
        &self,
        tenant_id: i64,
        after_revision: i64,
        namespace: &str,
        limit: i32,
    ) -> Result<Vec<AuthzTupleRecord>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM authz_tuple_log
                WHERE tenant_id = $1
                  AND revision > $2
                  AND ($3 = '' OR namespace = $3)
                ORDER BY revision
                LIMIT $4"#,
                &[
                    &tenant_id,
                    &after_revision,
                    &namespace,
                    &(if limit == 0 { 1000 } else { limit } as i64),
                ],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO index_definitions
                    (tenant_id, bucket_id, name, kind, selector, extractor, authorization_mode, build_policy)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &name,
                    &kind,
                    &selector,
                    &extractor,
                    &authorization_mode,
                    &build_policy,
                ],
            )
            .await?;
        Ok(row.into())
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                UPDATE index_definitions
                SET selector = $4,
                    extractor = $5,
                    authorization_mode = $6,
                    build_policy = $7,
                    version = version + 1,
                    updated_at = now()
                WHERE tenant_id = $1 AND bucket_id = $2 AND name = $3
                RETURNING *"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &name,
                    &selector,
                    &extractor,
                    &authorization_mode,
                    &build_policy,
                ],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn get_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                SELECT *
                FROM index_definitions
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND name = $3"#,
                &[&tenant_id, &bucket_id, &name],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn disable_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
                UPDATE index_definitions
                SET enabled = false,
                    version = version + 1,
                    updated_at = now()
                WHERE tenant_id = $1 AND bucket_id = $2 AND name = $3
                RETURNING *"#,
                &[&tenant_id, &bucket_id, &name],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn drop_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                "DELETE FROM index_definitions WHERE tenant_id = $1 AND bucket_id = $2 AND name = $3 RETURNING *",
                &[&tenant_id, &bucket_id, &name],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_index_definitions(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        include_disabled: bool,
    ) -> Result<Vec<IndexDefinition>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM index_definitions
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND ($3 OR enabled)
                ORDER BY name"#,
                &[&tenant_id, &bucket_id, &include_disabled],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn create_index_definition_event(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        index: &IndexDefinition,
        event_type: &str,
        definition: JsonValue,
    ) -> Result<IndexDefinitionEvent> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO index_definition_events
                    (tenant_id, bucket_id, bucket_name, index_id, index_name, event_type, index_version, definition)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &bucket_name,
                    &index.id,
                    &index.name,
                    &event_type,
                    &index.version,
                    &definition,
                ],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn list_index_definition_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<IndexDefinitionEvent>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM index_definition_events
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND id > $3
                ORDER BY id
                LIMIT $4"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &after_cursor,
                    &(if limit == 0 { 1000 } else { limit } as i64),
                ],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
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
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
                INSERT INTO index_diagnostics
                    (tenant_id, bucket_id, bucket_name, index_id, index_name, object_key,
                     version_id, severity, code, message, details)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                RETURNING *"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &bucket_name,
                    &index_id,
                    &index_name,
                    &object_key,
                    &version_id,
                    &severity,
                    &code,
                    &message,
                    &details,
                ],
            )
            .await?;
        Ok(row.into())
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
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
                SELECT *
                FROM index_diagnostics
                WHERE tenant_id = $1
                  AND bucket_id = $2
                  AND ($3 = '' OR index_name = $3)
                  AND ($4 = '' OR severity = $4)
                  AND id > $5
                ORDER BY id
                LIMIT $6"#,
                &[
                    &tenant_id,
                    &bucket_id,
                    &index_name,
                    &severity,
                    &after_cursor,
                    &(if limit == 0 { 1000 } else { limit } as i64),
                ],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn hard_delete_object(&self, object_id: i64) -> Result<()> {
        let client = self.regional_pool.get().await?;
        client
            .execute("DELETE FROM objects WHERE id = $1", &[&object_id])
            .await?;
        Ok(())
    }

    // --- Task Queue Methods ---

    pub async fn enqueue_task(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "INSERT INTO tasks (task_type, payload, priority) VALUES ($1, $2, $3)",
                &[&task_type, &payload, &priority],
            )
            .await?;
        Ok(())
    }

    pub async fn fetch_pending_tasks_for_update(&self, limit: i64) -> Result<Vec<Row>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                r#"SELECT id, task_type::text, payload, attempts FROM tasks WHERE status = 'pending'::task_status AND scheduled_at <= now() ORDER BY priority ASC, created_at ASC LIMIT $1 FOR UPDATE SKIP LOCKED"#,
                &[&limit],
            )
            .await?;
        Ok(rows)
    }

    pub async fn update_task_status(
        &self,
        task_id: i64,
        status: crate::tasks::TaskStatus,
    ) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "UPDATE tasks SET status = $1, updated_at = now() WHERE id = $2",
                &[&status, &task_id],
            )
            .await?;
        Ok(())
    }

    pub async fn fail_task(&self, task_id: i64, error: &str) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                r#"UPDATE tasks SET status = $1, last_error = $2, attempts = attempts + 1, scheduled_at = now() + (attempts * attempts * 10 * interval '1 second'), updated_at = now() WHERE id = $3"#,
                &[&crate::tasks::TaskStatus::Failed, &error, &task_id],
            )
            .await?;
        Ok(())
    }

    // ---- Hugging Face Keys ----
    pub async fn hf_create_key(
        &self,
        name: &str,
        token_encrypted: &[u8],
        note: Option<&str>,
    ) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "INSERT INTO huggingface_keys (name, token_encrypted, note) VALUES ($1,$2,$3)",
                &[&name, &token_encrypted, &note],
            )
            .await?;
        Ok(())
    }

    pub async fn hf_delete_key(&self, name: &str) -> Result<u64> {
        let client = self.global_pool.get().await?;
        let n = client
            .execute("DELETE FROM huggingface_keys WHERE name=$1", &[&name])
            .await?;
        Ok(n)
    }

    pub async fn hf_get_key_encrypted(&self, name: &str) -> Result<Option<(i64, Vec<u8>)>> {
        let client = self.global_pool.get().await?;
        if let Some(row) = client
            .query_opt(
                "SELECT id, token_encrypted FROM huggingface_keys WHERE name=$1",
                &[&name],
            )
            .await?
        {
            let id: i64 = row.get(0);
            let token: Vec<u8> = row.get(1);
            Ok(Some((id, token)))
        } else {
            Ok(None)
        }
    }

    pub async fn hf_list_keys(
        &self,
    ) -> Result<
        Vec<(
            String,
            Option<String>,
            chrono::DateTime<chrono::Utc>,
            chrono::DateTime<chrono::Utc>,
        )>,
    > {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT name, note, created_at, updated_at FROM huggingface_keys ORDER BY name",
                &[],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect())
    }

    // ---- Hugging Face Ingestion ----
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
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                r#"INSERT INTO hf_ingestions (key_id, tenant_id, requester_app_id, repo, revision, target_bucket, target_region, target_prefix, include_globs, exclude_globs) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING id"#,
                &[
                    &key_id,
                    &tenant_id,
                    &requester_app_id,
                    &repo,
                    &revision,
                    &target_bucket,
                    &target_region,
                    &target_prefix,
                    &include_globs,
                    &exclude_globs,
                ],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn hf_update_ingestion_state(
        &self,
        id: i64,
        state: crate::tasks::HFIngestionState,
        error: Option<&str>,
    ) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                r#"UPDATE hf_ingestions SET state=$2, error=$3, started_at=CASE WHEN $2='running'::hf_ingestion_state AND started_at IS NULL THEN now() ELSE started_at END, finished_at=CASE WHEN $2 IN ('completed'::hf_ingestion_state,'failed'::hf_ingestion_state,'canceled'::hf_ingestion_state) THEN now() ELSE finished_at END WHERE id=$1"#,
                &[&id, &state, &error],
            )
            .await?;
        Ok(())
    }

    pub async fn hf_cancel_ingestion(&self, id: i64) -> Result<u64> {
        let client = self.global_pool.get().await?;
        let n = client
            .execute(
                "UPDATE hf_ingestions SET state=$2::hf_ingestion_state WHERE id=$1 AND state IN ('queued'::hf_ingestion_state,'running'::hf_ingestion_state)",
                &[&id, &crate::tasks::HFIngestionState::Canceled],
            )
            .await?;
        Ok(n)
    }

    pub async fn hf_add_item(
        &self,
        ingestion_id: i64,
        path: &str,
        size: Option<i64>,
        etag: Option<&str>,
    ) -> Result<i64> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                r#"INSERT INTO hf_ingestion_items (ingestion_id, path, size, etag) VALUES ($1, $2, $3, $4) ON CONFLICT (ingestion_id, path) DO UPDATE SET size = EXCLUDED.size RETURNING id"#,
                &[&ingestion_id, &path, &size, &etag],
            )
            .await?;
        Ok(row.get(0))
    }

    pub async fn hf_update_item_state(
        &self,
        id: i64,
        state: crate::tasks::HFIngestionItemState,
        error: Option<&str>,
    ) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                r#"UPDATE hf_ingestion_items SET state=$2, error=$3, started_at=CASE WHEN $2='downloading'::hf_item_state AND started_at IS NULL THEN now() ELSE started_at END, finished_at=CASE WHEN $2 IN ('stored'::hf_item_state,'failed'::hf_item_state,'skipped'::hf_item_state) THEN now() ELSE finished_at END WHERE id=$1"#,
                &[&id, &state, &error],
            )
            .await?;
        Ok(())
    }

    pub async fn hf_update_item_success(&self, id: i64, size: i64, etag: &str) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                r#"UPDATE hf_ingestion_items SET state='stored'::hf_item_state, size=$2, etag=$3, finished_at=now() WHERE id=$1"#,
                &[&id, &size, &etag],
            )
            .await?;
        Ok(())
    }

    pub async fn hf_get_ingestion_items(
        &self,
        ingestion_id: i64,
    ) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT path, size, etag, finished_at FROM hf_ingestion_items WHERE ingestion_id=$1 AND state='stored'::hf_item_state",
                &[&ingestion_id],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
            .collect())
    }

    pub async fn hf_get_all_items_for_prefix(
        &self,
        tenant_id: i64,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, Option<i64>, Option<String>, Option<DateTime<Utc>>)>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                r#"
            SELECT i.path, i.size, i.etag, i.finished_at
            FROM hf_ingestion_items i
            JOIN hf_ingestions h ON i.ingestion_id = h.id
            WHERE h.tenant_id = $1
              AND h.target_bucket = $2
              AND COALESCE(h.target_prefix, '') = $3
              AND i.state = 'stored'::hf_item_state
            ORDER BY i.finished_at ASC
            "#,
                &[&tenant_id, &bucket, &prefix],
            )
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3)))
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
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        chrono::DateTime<chrono::Utc>,
    )> {
        let client = self.global_pool.get().await?;
        let job = client
            .query_one(
                r#"SELECT state::text, error, created_at, started_at, finished_at FROM hf_ingestions WHERE id=$1"#,
                &[&id],
            )
            .await?;
        let mut state: String = job.get(0);
        let err: Option<String> = job.get(1);
        let created_at: chrono::DateTime<chrono::Utc> = job.get(2);
        let started_at: Option<chrono::DateTime<chrono::Utc>> = job.get(3);
        let finished_at: Option<chrono::DateTime<chrono::Utc>> = job.get(4);
        let counts = client
            .query_one(
                r#"SELECT COUNT(*) FILTER (WHERE state::text='queued') AS queued, COUNT(*) FILTER (WHERE state::text='downloading') AS downloading, COUNT(*) FILTER (WHERE state::text='stored') AS stored, COUNT(*) FILTER (WHERE state::text='failed') AS failed FROM hf_ingestion_items WHERE ingestion_id=$1"#,
                &[&id],
            )
            .await?;

        let queued: i64 = counts.get(0);
        let downloading: i64 = counts.get(1);
        let stored: i64 = counts.get(2);
        let failed: i64 = counts.get(3);

        if state == "running" && queued == 0 && downloading == 0 && (stored > 0 || failed > 0) {
            state = "completed".to_string();
        }

        Ok((
            state,
            queued,
            downloading,
            stored,
            failed,
            err,
            started_at,
            finished_at,
            created_at,
        ))
    }
}
