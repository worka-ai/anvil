use anyhow::Result;
use deadpool_postgres::Pool;
use serde_json::Value as JsonValue;
use time::OffsetDateTime;
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
pub struct Bucket {
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub region: String,
    pub created_at: OffsetDateTime,
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
        Self {
            id: row.get("id"),
            name: row.get("name"),
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

impl Persistence {
    pub fn new(global_pool: Pool, regional_pool: Pool) -> Self {
        Self {
            global_pool,
            regional_pool,
        }
    }

    // --- Global Methods ---

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
                "SELECT * FROM buckets WHERE tenant_id = $1 AND name = $2 AND region = $3",
                &[&tenant_id, &name, &region],
            )
            .await?;
        Ok(row.map(Into::into))
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
            WHERE bucket_id = $1 AND key = $2
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
        _prefix: &str,
        _start_after: &str,
        limit: i32,
    ) -> Result<Vec<Object>> {
        let client = self.regional_pool.get().await?;
        let rows = client
            .query(
                r#"
            SELECT * FROM objects
            WHERE bucket_id = $1
            ORDER BY key
            LIMIT $2
            "#,
                &[&bucket_id, &(limit as i64)],
            )
            .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}
