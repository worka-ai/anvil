use anyhow::Result;
use sqlx::{FromRow, PgPool};
use time::OffsetDateTime;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub struct Persistence {
    global_pool: PgPool,
    regional_pool: PgPool,
}

// Structs that map to our database tables
#[derive(Debug, FromRow)]
pub struct Tenant {
    pub id: i64,
    pub name: String,
}

#[derive(Debug, FromRow)]
pub struct Bucket {
    pub id: i64,
    pub tenant_id: i64,
    pub name: String,
    pub region: String,
    pub created_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
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

impl Persistence {
    pub fn new(global_pool: PgPool, regional_pool: PgPool) -> Self {
        Self { global_pool, regional_pool }
    }

    // --- Global Methods ---

    pub async fn create_tenant(&self, name: &str, api_key: &str) -> Result<Tenant> {
        sqlx::query_as!(
            Tenant,
            "INSERT INTO tenants (name, api_key) VALUES ($1, $2) RETURNING id, name",
            name,
            api_key
        )
        .fetch_one(&self.global_pool)
        .await.map_err(Into::into)
    }

    pub async fn create_bucket(&self, tenant_id: i64, name: &str, region: &str) -> Result<Bucket> {
        sqlx::query_as!(
            Bucket,
            "INSERT INTO buckets (tenant_id, name, region) VALUES ($1, $2, $3) RETURNING *",
            tenant_id,
            name,
            region
        )
        .fetch_one(&self.global_pool)
        .await.map_err(Into::into)
    }

    pub async fn get_bucket_by_name(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        sqlx::query_as!(
            Bucket,
            "SELECT * FROM buckets WHERE tenant_id = $1 AND name = $2",
            tenant_id,
            name
        )
        .fetch_optional(&self.global_pool)
        .await.map_err(Into::into)
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
        sqlx::query_as!(
            Object,
            r#"
            INSERT INTO objects (bucket_id, key, content_hash, size, etag)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING 
                id, bucket_id, key, content_hash, size, etag, 
                content_type, version_id, created_at, storage_class, 
                user_meta, shard_map, checksum
            "#,
            bucket_id,
            key,
            content_hash,
            size,
            etag
        )
        .fetch_one(&self.regional_pool)
        .await.map_err(Into::into)
    }

    pub async fn get_object(&self, bucket_id: i64, key: &str) -> Result<Option<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT 
                id, bucket_id, key, content_hash, size, etag, 
                content_type, version_id, created_at, storage_class, 
                user_meta, shard_map, checksum 
            FROM objects 
            WHERE bucket_id = $1 AND key = $2 
            ORDER BY created_at DESC LIMIT 1
            "#,
            bucket_id,
            key
        )
            .fetch_optional(&self.regional_pool)
            .await.map_err(Into::into)
    }
    pub async fn list_objects(
        &self,
        bucket_id: i64,
        prefix: &str,
        start_after: &str,
        limit: i32,
    ) -> Result<Vec<Object>> {
        sqlx::query_as!(
            Object,
            r#"
            SELECT 
                id, bucket_id, key, content_hash, size, etag, 
                content_type, version_id, created_at, storage_class, 
                user_meta, shard_map, checksum
            FROM objects
            WHERE bucket_id = $1 AND key > $2 AND key LIKE $3
            ORDER BY key
            LIMIT $4
            "#,
            bucket_id,
            start_after,
            format!("{}%", prefix),
            limit as i64
        )
        .fetch_all(&self.regional_pool)
        .await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy::dotenv;
    use std::env;

    async fn setup() -> Persistence {
        dotenv().ok();
        let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set for tests");
        let pool = PgPool::connect(&db_url).await.unwrap();

        // For this test, we can treat the single DB as both global and regional
        sqlx::migrate!("./migrations_global").run(&pool).await.unwrap();
        sqlx::migrate!("./migrations_regional").run(&pool).await.unwrap();

        // Ensure the region exists before creating a bucket in it
        sqlx::query("INSERT INTO regions (name) VALUES ('test-region') ON CONFLICT (name) DO NOTHING")
            .execute(&pool)
            .await
            .unwrap();

        Persistence::new(pool.clone(), pool)
    }

    #[tokio::test]
    async fn test_create_tenant_and_bucket() {
        let p = setup().await;

        // Create a tenant
        let tenant_name = format!("test-tenant-persist-{}", uuid::Uuid::new_v4());
        let tenant = p.create_tenant(&tenant_name, "test-key").await.unwrap();
        assert_eq!(tenant.name, tenant_name);

        // Create a bucket for that tenant
        let bucket_name = "test-bucket";
        let bucket = p
            .create_bucket(tenant.id, bucket_name, "test-region")
            .await
            .unwrap();
        assert_eq!(bucket.name, bucket_name);
        assert_eq!(bucket.tenant_id, tenant.id);
    }
}
