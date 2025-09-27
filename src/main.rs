use tonic::{Request, Response, Status};
use tonic::transport::Server;
use crate::anvil_api::bucket_service_server::{BucketService, BucketServiceServer};
use crate::anvil_api::{CreateBucketRequest, CreateBucketResponse, DeleteBucketRequest, DeleteBucketResponse, GetBucketPolicyRequest, GetBucketPolicyResponse, ListBucketsRequest, ListBucketsResponse, PutBucketPolicyRequest, PutBucketPolicyResponse};

pub mod anvil_api {
    tonic::include_proto!("anvil");
}

#[derive(Default)]
pub struct BucketServiceAPI {}

#[tonic::async_trait]
impl BucketService for BucketServiceAPI {
    async fn create_bucket(&self, request: Request<CreateBucketRequest>) -> Result<Response<CreateBucketResponse>, Status> {
        todo!()
    }

    async fn delete_bucket(&self, request: Request<DeleteBucketRequest>) -> Result<Response<DeleteBucketResponse>, Status> {
        todo!()
    }

    async fn list_buckets(&self, request: Request<ListBucketsRequest>) -> Result<Response<ListBucketsResponse>, Status> {
        todo!()
    }

    async fn get_bucket_policy(&self, request: Request<GetBucketPolicyRequest>) -> Result<Response<GetBucketPolicyResponse>, Status> {
        todo!()
    }

    async fn put_bucket_policy(&self, request: Request<PutBucketPolicyRequest>) -> Result<Response<PutBucketPolicyResponse>, Status> {
        todo!()
    }
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse().unwrap();
    let bucket_service_api = BucketServiceAPI::default();

    println!("Listening on {addr}");

    Server::builder()
        .add_service(BucketServiceServer::new(bucket_service_api))
        .serve(addr)
        .await?;

    Ok(())
}
