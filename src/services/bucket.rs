use crate::anvil_api::*;
use crate::{auth, AppState};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::bucket_service_server::BucketService;
use crate::anvil_api::internal_anvil_service_server::InternalAnvilService;
use crate::anvil_api::object_service_server::ObjectService;

#[tonic::async_trait]
impl BucketService for AppState {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let resource = format!("bucket:{}", req.bucket_name);
        if !auth::is_authorized(&format!("write:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        println!("gRPC - Create Bucket: {:?}", req);

        let tenant_id = claims.tenant_id;

        let db_result = self
            .db
            .create_bucket(tenant_id, &req.bucket_name, &req.region)
            .await;

        println!("DB create_bucket result: {:?}", db_result);

        db_result.map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?.clone();
        let req = request.into_inner();

        let resource = format!("bucket:{}", req.bucket_name);
        if !auth::is_authorized(&format!("write:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        // Soft-delete the bucket
        let bucket = self
            .db
            .soft_delete_bucket(&req.bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        // Enqueue a task for physical deletion
        let payload = serde_json::json!({ "bucket_id": bucket.id });
            self.db.enqueue_task(crate::tasks::TaskType::DeleteBucket, payload, 100).await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteBucketResponse {}))
    }

    async fn list_buckets(
        &self,
        request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        // For now, we assume the tenant is derived from the app, which is linked to the claims.
        let tenant_id = claims.tenant_id;

        if !auth::is_authorized("read:bucket:*", &claims.scopes) {
            return Err(Status::permission_denied("Permission denied to list buckets"));
        }

        let buckets = self
            .db
            .list_buckets_for_tenant(tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let response_buckets = buckets
            .into_iter()
            .map(|b| crate::anvil_api::Bucket {
                name: b.name,
                creation_date: b.created_at.to_string(),
            })
            .collect();

        Ok(Response::new(ListBucketsResponse { buckets: response_buckets }))
    }

    async fn get_bucket_policy(
        &self,
        _request: Request<GetBucketPolicyRequest>,
    ) -> Result<Response<GetBucketPolicyResponse>, Status> {
        todo!()
    }

    async fn put_bucket_policy(
        &self,
        _request: Request<PutBucketPolicyRequest>,
    ) -> Result<Response<PutBucketPolicyResponse>, Status> {
        todo!()
    }
}
