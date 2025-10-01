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
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        self.bucket_manager
            .create_bucket(claims.tenant_id, &req.bucket_name, &req.region, &claims.scopes)
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
        _request: Request<PutBucketPolicyRequest>,
    ) -> Result<Response<PutBucketPolicyResponse>, Status> {
        todo!()
    }
}
