use crate::persistence::{Bucket, Tenant};
use moka::future::Cache;
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct MetadataCache {
    // (tenant_id, bucket_name) -> Bucket
    buckets: Cache<(i64, String), Bucket>,
    // bucket_name -> Bucket (for public/S3 lookups without tenant_id context initially)
    // This might need to handle conflicts if bucket names aren't globally unique, but 
    // for S3 compat they should be. Assuming global uniqueness for now.
    buckets_by_name: Cache<String, Bucket>,
    
    // api_key -> Tenant
    tenants: Cache<String, Tenant>,
    
    // (app_id, resource, action) -> bool (authorized)
    // Or perhaps cache the list of policies? 
    // Let's cache the policies list for an app as that's what `get_policies_for_app` returns.
    // app_id -> Vec<String> (policies)
    app_policies: Cache<i64, Vec<String>>,
}

impl MetadataCache {
    pub fn new(config: &crate::config::Config) -> Self {
        let ttl = Duration::from_secs(config.metadata_cache_ttl_secs);
        Self {
            buckets: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(ttl) 
                .build(),
            buckets_by_name: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(ttl)
                .build(),
            tenants: Cache::builder()
                .max_capacity(5_000)
                .time_to_live(ttl * 2) 
                .build(),
            app_policies: Cache::builder()
                .max_capacity(5_000)
                .time_to_live(ttl)
                .build(),
        }
    }

    pub async fn get_bucket(&self, tenant_id: i64, name: &str) -> Option<Bucket> {
        self.buckets.get(&(tenant_id, name.to_string())).await
    }

    pub async fn insert_bucket(&self, tenant_id: i64, name: String, bucket: Bucket) {
        self.buckets.insert((tenant_id, name.clone()), bucket.clone()).await;
        self.buckets_by_name.insert(name, bucket).await;
    }

    pub async fn invalidate_bucket(&self, tenant_id: i64, name: &str) {
        self.buckets.invalidate(&(tenant_id, name.to_string())).await;
        self.buckets_by_name.invalidate(name).await;
    }
    
    // For when we only know the name (e.g. deleting by name, or cross-tenant lookup if allowed)
    pub async fn get_bucket_by_name_only(&self, name: &str) -> Option<Bucket> {
        self.buckets_by_name.get(name).await
    }

    pub async fn invalidate_bucket_by_name(&self, name: &str) {
        self.buckets_by_name.invalidate(name).await;
        // Note: We can't easily invalidate the (tenant_id, name) key without scanning 
        // or knowing the tenant_id. This is a trade-off. 
        // For strict consistency, the caller should provide tenant_id if possible.
        // However, P2P events usually contain enough info.
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

    pub async fn get_app_policies(&self, app_id: i64) -> Option<Vec<String>> {
        self.app_policies.get(&app_id).await
    }

    pub async fn insert_app_policies(&self, app_id: i64, policies: Vec<String>) {
        self.app_policies.insert(app_id, policies).await;
    }

    pub async fn invalidate_app_policies(&self, app_id: i64) {
        self.app_policies.invalidate(&app_id).await;
    }
}


