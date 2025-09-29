use anyhow::Result;
use deadpool_postgres::Pool;
use time::OffsetDateTime;
use serde_json::Value as JsonValue;
use tokio_postgres::Row;

#[derive(Debug, Clone)]
pub struct Persistence {
    global_pool: Pool,
    regional_pool: Pool,
}

// Structs that map to our database tables
#[derive(Debug)]
pub struct Tenant {
    pub id: i64,
    pub name: String,
}

#[derive(Debug)]
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
    pub created_at: OffsetDateTime,
    pub is_public_read: bool,
}

#[derive(Debug)]
pub struct Object {
    pub id: i64,
    pub bucket_id: i64,
    pub key: String,
    pub content_hash: String,
    pub size: i64,
    pub etag: String,
    pub content_type: Option<String>,
    pub version_id: uuid::Uuid,
    pub created_at: OffsetDateTime,
    pub storage_class: Option<i16>,
    pub user_meta: Option<JsonValue>,
    pub shard_map: Option<JsonValue>,
    pub checksum: Option<Vec<u8>>,
}

// Manual row-to-struct mapping
impl From<Row> for Tenant {
    fn from(row: Row) -> Self {
        Self { id: row.get("id"), name: row.get("name") }
    }
}

impl From<Row> for App {
    fn from(row: Row) -> Self {
        Self { id: row.get("id"), name: row.get("name"), client_id: row.get("client_id") }
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
            bucket_id: row.get("bucket_id"),
            key: row.get("key"),
            content_hash: row.get("content_hash"),
            size: row.get("size"),
            etag: row.get("etag"),
            content_type: row.get("content_type"),
            version_id: row.get("version_id"),
            created_at: row.get("created_at"),
            storage_class: row.get("storage_class"),
            user_meta: row.get("user_meta"),
            shard_map: row.get("shard_map"),
            checksum: row.get("checksum"),
        }
    }
}

pub struct AppDetails {
    pub id: i64,
    pub client_secret_hash: String,
}

impl From<Row> for AppDetails {
    fn from(row: Row) -> Self {
        Self { id: row.get("id"), client_secret_hash: row.get("client_secret_hash") }
    }
}

impl Persistence {
    pub fn new(global_pool: Pool, regional_pool: Pool) -> Self {
        Self { global_pool, regional_pool }
    }

    pub fn get_global_pool(&self) -> &Pool {
        &self.global_pool
    }

    // --- Global Methods ---

    pub async fn get_tenant_by_name(&self, name: &str) -> Result<Option<Tenant>> {
        let client = self.global_pool.get().await?;
        let row = client.query_opt("SELECT id, name FROM tenants WHERE name = $1", &[&name]).await?;
        Ok(row.map(Into::into))
    }

    pub async fn get_app_by_client_id(&self, client_id: &str) -> Result<Option<AppDetails>> {
        let client = self.global_pool.get().await?;
        let row = client.query_opt("SELECT id, client_secret_hash FROM apps WHERE client_id = $1", &[&client_id]).await?;
        Ok(row.map(Into::into))
    }

    pub async fn get_policies_for_app(&self, app_id: i64) -> Result<Vec<String>> {
        let client = self.global_pool.get().await?;
        let rows = client.query("SELECT resource, action FROM policies WHERE app_id = $1", &[&app_id]).await?;
        Ok(rows.into_iter().map(|row| format!("{}:{}", row.get::<_, String>("action"), row.get::<_, String>("resource"))).collect())
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

    pub async fn create_app(&self, tenant_id: i64, name: &str, client_id: &str, client_secret_hash: &str) -> Result<App> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                "INSERT INTO apps (tenant_id, name, client_id, client_secret_hash) VALUES ($1, $2, $3, $4) RETURNING id, name, client_id",
                &[&tenant_id, &name, &client_id, &client_secret_hash],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_app_by_name(&self, name: &str) -> Result<Option<App>> {
        let client = self.global_pool.get().await?;
        let row = client.query_opt("SELECT id, name, client_id FROM apps WHERE name = $1", &[&name]).await?;
        Ok(row.map(Into::into))
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

    pub async fn create_bucket(&self, tenant_id: i64, name: &str, region: &str) -> Result<Bucket> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_one(
                "INSERT INTO buckets (tenant_id, name, region) VALUES ($1, $2, $3) RETURNING *",
                &[&tenant_id, &name, &region],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_bucket_by_name(&self, tenant_id: i64, name: &str, region: &str) -> Result<Option<Bucket>> {
        let client = self.global_pool.get().await?;
        let row = client
            .query_opt(
            "SELECT id, name, region, created_at, is_public_read, tenant_id FROM buckets WHERE tenant_id = $1 AND name = $2 AND region = $3 AND deleted_at IS NULL",
                &[&tenant_id, &name, &region],
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
                "UPDATE buckets SET deleted_at = now() WHERE name = $1 AND deleted_at IS NULL RETURNING *",
                &[&bucket_name],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_buckets_for_tenant(&self, tenant_id: i64) -> Result<Vec<Bucket>> {
        let client = self.global_pool.get().await?;
        let rows = client
            .query(
                "SELECT * FROM buckets WHERE tenant_id = $1 AND deleted_at IS NULL ORDER BY name",
                &[&tenant_id],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    // --- Regional Methods ---

    pub async fn create_object(
        &self,
        bucket_id: i64,
        key: &str,
        content_hash: &str,
        size: i64,
        etag: &str,
    ) -> Result<Object> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_one(
                r#"
            INSERT INTO objects (bucket_id, key, content_hash, size, etag, version_id)
            VALUES ($1, $2, $3, $4, $5, gen_random_uuid())
            RETURNING *;
            "#,
                &[&bucket_id, &key, &content_hash, &size, &etag],
            )
            .await?;
        Ok(row.into())
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
            SELECT *
            FROM objects
            WHERE bucket_id = $1 AND key = $2 AND deleted_at IS NULL
            ORDER BY created_at DESC LIMIT 1
            "#,
                &[&bucket_id, &key],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    pub async fn list_objects(
        &self,
        bucket_id: i64,
        prefix: &str,
        start_after: &str,
        limit: i32,
    ) -> Result<Vec<Object>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
            SELECT * FROM objects
            WHERE bucket_id = $1 AND key > $2 AND key LIKE $3 AND deleted_at IS NULL
            ORDER BY key
            LIMIT $4
            "#,
                &[&bucket_id, &start_after, &format!("{}%", prefix), &(limit as i64)],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    pub async fn soft_delete_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        let client = self.regional_pool.get().await?;
        let row = client
            .query_opt(
                r#"
            UPDATE objects
            SET deleted_at = now()
            WHERE bucket_id = $1 AND key = $2 AND deleted_at IS NULL
            RETURNING *
            "#,
                &[&bucket_id, &key],
            )
            .await?;
        Ok(row.map(Into::into))
    }

    // --- Task Queue Methods ---

    pub async fn enqueue_task(&self, task_type: &str, payload: JsonValue, priority: i32) -> Result<()> {
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
                r#"
            SELECT id, task_type, payload, attempts FROM tasks
            WHERE status = 'pending' AND scheduled_at <= now()
            ORDER BY priority ASC, created_at ASC
            LIMIT $1
            FOR UPDATE SKIP LOCKED
            "#,
                &[&limit],
            )
            .await?;
        Ok(rows)
    }

    pub async fn update_task_status(&self, task_id: i64, status: &str) -> Result<()> {
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
                r#"
            UPDATE tasks
            SET
                status = 'failed',
                last_error = $1,
                attempts = attempts + 1,
                -- Exponential backoff: 10s, 40s, 90s, etc.
                scheduled_at = now() + (attempts * attempts * 10 * interval '1 second'),
                updated_at = now()
            WHERE id = $2
            "#,
                &[&error, &task_id],
            )
            .await?;
        Ok(())
    }
}
