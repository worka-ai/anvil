use crate::persistence::{Bucket, Tenant};
use moka::future::Cache;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MetadataCache {
    // (tenant_id, bucket_name) -> Bucket
    buckets: Cache<(i64, String), Bucket>,

    // api_key -> Tenant
    tenants: Cache<String, Tenant>,
}

impl MetadataCache {
    pub fn new(config: &crate::config::Config) -> Self {
        let ttl = Duration::from_secs(config.metadata_cache_ttl_secs);
        Self {
            buckets: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(ttl)
                .build(),
            tenants: Cache::builder()
                .max_capacity(5_000)
                .time_to_live(ttl * 2)
                .build(),
        }
    }

    pub async fn get_bucket(&self, tenant_id: i64, name: &str) -> Option<Bucket> {
        self.buckets.get(&(tenant_id, name.to_string())).await
    }

    pub async fn insert_bucket(&self, tenant_id: i64, name: String, bucket: Bucket) {
        self.buckets.insert((tenant_id, name), bucket).await;
        self.buckets.run_pending_tasks().await;
    }

    pub async fn invalidate_bucket(&self, tenant_id: i64, name: &str) {
        self.buckets.remove(&(tenant_id, name.to_string())).await;
        self.buckets.run_pending_tasks().await;
    }

    pub async fn get_tenant(&self, api_key: &str) -> Option<Tenant> {
        self.tenants.get(api_key).await
    }

    pub async fn insert_tenant(&self, api_key: String, tenant: Tenant) {
        self.tenants.insert(api_key, tenant).await;
    }

    pub async fn invalidate_tenant(&self, api_key: &str) {
        self.tenants.invalidate(api_key).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use chrono::Utc;

    fn bucket(name: &str) -> Bucket {
        Bucket {
            id: 1,
            tenant_id: 7,
            name: name.to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        }
    }

    #[tokio::test]
    async fn bucket_invalidation_removes_all_bucket_lookup_entries() {
        let cache = MetadataCache::new(&Config {
            metadata_cache_ttl_secs: 300,
            ..Config::default()
        });
        cache
            .insert_bucket(7, "deleted".to_string(), bucket("deleted"))
            .await;

        assert!(cache.get_bucket(7, "deleted").await.is_some());

        cache.invalidate_bucket(7, "deleted").await;

        assert!(cache.get_bucket(7, "deleted").await.is_none());
    }
}
