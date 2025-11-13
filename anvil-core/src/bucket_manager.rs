use crate::{
    auth,
    persistence::{Bucket, Persistence},
    tasks::TaskType,
    validation,
};
use tonic::Status;

#[derive(Debug, Clone)]
pub struct BucketManager {
    db: Persistence,
}

impl BucketManager {
    pub fn new(db: Persistence) -> Self {
        Self { db }
    }

    pub async fn create_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        region: &str,
        scopes: &[String],
    ) -> Result<(), Status> {
        println!("[manager] ENTERING create_bucket for bucket: {}", bucket_name);
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if !validation::is_valid_region_name(region) {
            return Err(Status::invalid_argument("Invalid region name"));
        }
        let resource = format!("bucket:{}", bucket_name);
        if !auth::is_authorized(&format!("write:{}", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        println!("[manager] Calling DB to create bucket: {}", bucket_name);
        self.db
            .create_bucket(tenant_id, bucket_name, region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        println!("[manager] EXITING create_bucket for bucket: {}", bucket_name);
        Ok(())
    }

    pub async fn delete_bucket(&self, bucket_name: &str, scopes: &[String]) -> Result<(), Status> {
        let resource = format!("bucket:{}", bucket_name);
        if !auth::is_authorized(&format!("write:{}", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        // Soft-delete the bucket
        let bucket = self
            .db
            .soft_delete_bucket(bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        // Enqueue a task for physical deletion
        let payload = serde_json::json!({ "bucket_id": bucket.id });
        self.db
            .enqueue_task(TaskType::DeleteBucket, payload, 100)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }

    pub async fn list_buckets(
        &self,
        tenant_id: i64,
        scopes: &[String],
    ) -> Result<Vec<Bucket>, Status> {
        println!("[manager] ENTERING list_buckets for tenant: {}", tenant_id);
        if !auth::is_authorized("read:bucket:*", scopes) {
            return Err(Status::permission_denied(
                "Permission denied to list buckets",
            ));
        }

        println!("[manager] Calling DB to list buckets for tenant: {}", tenant_id);
        let buckets = self
            .db
            .list_buckets_for_tenant(tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        println!("[manager] EXITING list_buckets, found {} buckets", buckets.len());
        Ok(buckets)
    }

    pub async fn set_bucket_public_access(
        &self,
        bucket_name: &str,
        is_public: bool,
        scopes: &[String],
    ) -> Result<(), Status> {
        let resource = format!("bucket:{}", bucket_name);
        if !auth::is_authorized(&format!("write:{}:policy", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        self.db
            .set_bucket_public_access(bucket_name, is_public)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }
}
