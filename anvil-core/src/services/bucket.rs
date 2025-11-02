use crate::anvil_api::bucket_service_server::BucketService;
use crate::anvil_api::*;
use crate::{AppState, auth};
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl BucketService for AppState {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        tracing::info!("[BucketService] ENTERING create_bucket. Metadata: {:?}", request.metadata());

        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        tracing::info!("[BucketService] Claims successfully extracted. Tenant ID: {}", claims.tenant_id);

        let req = request.get_ref();

        self.bucket_manager
            .create_bucket(
                claims.tenant_id,
                &req.bucket_name,
                &req.region,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        self.bucket_manager
            .delete_bucket(&req.bucket_name, &claims.scopes)
            .await?;

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

        let buckets = self
            .bucket_manager
            .list_buckets(claims.tenant_id, &claims.scopes)
            .await?;

        let response_buckets = buckets
            .into_iter()
            .map(|b| crate::anvil_api::Bucket {
                name: b.name,
                creation_date: b.created_at.to_string(),
            })
            .collect();

        Ok(Response::new(ListBucketsResponse {
            buckets: response_buckets,
        }))
    }

    async fn get_bucket_policy(
        &self,
        _request: Request<GetBucketPolicyRequest>,
    ) -> Result<Response<GetBucketPolicyResponse>, Status> {
        todo!()
    }

    async fn put_bucket_policy(
        &self,
        request: Request<PutBucketPolicyRequest>,
    ) -> Result<Response<PutBucketPolicyResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        // A bit of a hack: we only support is_public_read for now.
        let policy: serde_json::Value = serde_json::from_str(&req.policy_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid policy JSON: {}", e)))?;
        let is_public_read = policy["is_public_read"].as_bool().unwrap_or(false);

        self.bucket_manager
            .set_bucket_public_access(&req.bucket_name, is_public_read, &claims.scopes)
            .await?;

        Ok(Response::new(PutBucketPolicyResponse {}))
    }
}
