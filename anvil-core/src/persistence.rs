use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Pool;
use serde_json::Value as JsonValue;
use tokio_postgres::Row;

#[derive(Debug, Clone)]
pub struct Persistence {
    global_pool: Pool,
    regional_pool: Pool,
}

// Structs that map to our database tables
#[derive(Debug, serde::Serialize)]
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

#[derive(Debug)]
pub struct Bucket {
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub region: String,
    pub created_at: DateTime<Utc>,
    pub is_public_read: bool,
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
    pub created_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub storage_class: Option<i16>,
    pub user_meta: Option<JsonValue>,
    pub shard_map: Option<JsonValue>,
    pub checksum: Option<Vec<u8>>,
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
            created_at: row.get("created_at"),
            deleted_at: row.get("deleted_at"),
            storage_class: row.get("storage_class"),
            user_meta: row.get("user_meta"),
            shard_map: row.get("shard_map"),
            checksum: row.get("checksum"),
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
    pub fn new(global_pool: Pool, regional_pool: Pool) -> Self {
        Self {
            global_pool,
            regional_pool,
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
                Ok(row.into())
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

    pub async fn get_bucket_by_name(
        &self,
        tenant_id: i64,
        name: &str,
        region: &str,
    ) -> Result<Option<Bucket>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
            "SELECT id, name, region, created_at, is_public_read, tenant_id FROM buckets WHERE tenant_id = $1 AND name = $2 AND region = $3 AND deleted_at IS NULL",
                &[&tenant_id, &name, &region],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn get_public_bucket_by_name(&self, name: &str) -> Result<Option<Bucket>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
                "SELECT * FROM buckets WHERE name = $1 AND is_public_read = true AND deleted_at IS NULL",
                &[&name],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn set_bucket_public_access(&self, bucket_name: &str, is_public: bool) -> Result<()> {
        let client = self.global_pool.get().await?;
        client
            .execute(
                "UPDATE buckets SET is_public_read = $1 WHERE name = $2",
                &[&is_public, &bucket_name],
            )
            .await?;
        Ok(())
    }

    pub async fn soft_delete_bucket(&self, bucket_name: &str) -> Result<Option<Bucket>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
                r#"UPDATE buckets SET deleted_at = now() WHERE name = $1 AND deleted_at IS NULL RETURNING *"#,
                &[&bucket_name],
            )
            .await?;
        Ok(row.map(Into::into))
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

    pub async fn create_object(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        key: &str,
        content_hash: &str,
        size: i64,
        etag: &str,
        shard_map: Option<JsonValue>,
    ) -> Result<Object> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"INSERT INTO objects (tenant_id, bucket_id, key, content_hash, size, etag, version_id, shard_map) VALUES ($1, $2, $3, $4, $5, $6, gen_random_uuid(), $7) RETURNING *;"#,
                &[&tenant_id, &bucket_id, &key, &content_hash, &size, &etag, &shard_map],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"SELECT * FROM objects WHERE bucket_id = $1 AND key = $2 AND deleted_at IS NULL ORDER BY created_at DESC LIMIT 1"#,
                &[&bucket_id, &key],
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
                    r#"SELECT id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, created_at, storage_class, user_meta, shard_map, checksum, deleted_at, key_ltree FROM objects WHERE bucket_id = $1 AND deleted_at IS NULL AND key > $2 AND key LIKE $3 ORDER BY key LIMIT $4"#,
                    &[
                        &bucket_id,
                        &start_after,
                        &format!(r#"{}%"#, prefix),
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
                NULLIF($4::text, '')::ltree AS prefix_ltree
            ),
            lvl AS (
              SELECT COALESCE(nlevel(prefix_ltree), 0) AS p FROM params
            ),
            relevant AS (
              SELECT o.key, o.key_ltree
              FROM objects o, params p
              WHERE o.bucket_id = p.bucket_id
                AND o.deleted_at IS NULL
                AND o.key > p.start_after
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
                &[&bucket_id, &start_after, &(limit as i64), &prefix_dot],
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
                    r#"SELECT id, tenant_id, bucket_id, key, content_hash, size, etag, content_type, version_id, created_at, storage_class, user_meta, shard_map, checksum, deleted_at, key_ltree FROM objects WHERE bucket_id = $1 AND deleted_at IS NULL AND key = ANY($2) ORDER BY key"#,
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
        let row = client
            .query_opt(
                r#"UPDATE objects SET deleted_at = now() WHERE bucket_id = $1 AND key = $2 AND deleted_at IS NULL RETURNING *"#,
                &[&bucket_id, &key],
            )
            .await?;
        Ok(row.map(Into::into))
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
