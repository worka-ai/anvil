use crate::anvil_api::bucket_service_server::BucketService;
use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::AppState;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl BucketService for AppState {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        println!("gRPC - Create Bucket: {:?}", request.into_inner());
        // Logic to create a bucket will go here
        todo!()
    }

    async fn delete_bucket(
        &self,
        _request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        todo!()
    }

    async fn list_buckets(
        &self,
        _request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        todo!()
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

#[tonic::async_trait]
impl ObjectService for AppState {
    type GetObjectStream = std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<GetObjectResponse, Status>> + Send>>;

    async fn put_object(
        &self,
        request: Request<tonic::Streaming<PutObjectRequest>>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let mut stream = request.into_inner();

        let mut data = Vec::new();
        let mut object_key = String::new();
        let mut bucket_name = String::new();

        if let Some(Ok(first_chunk)) = stream.next().await {
            if let Some(put_object_request::Data::Metadata(metadata)) = first_chunk.data {
                object_key = metadata.object_key;
                bucket_name = metadata.bucket_name;
            } else {
                return Err(Status::invalid_argument("First chunk must be metadata"));
            }
        } else {
            return Err(Status::invalid_argument("Empty stream"));
        }

        while let Some(Ok(chunk)) = stream.next().await {
            if let Some(put_object_request::Data::Chunk(bytes)) = chunk.data {
                data.extend_from_slice(&bytes);
            } else {
                return Err(Status::invalid_argument("Subsequent chunks must be data"));
            }
        }

        // For now, we'll assume a single tenant.
        // In Phase 5, we'll get this from the request's auth token.
        let tenant_id = 1;

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, &bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        let content_hash = self
            .storage
            .store(&data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let object = self
            .db
            .create_object(bucket.id, &object_key, &content_hash, data.len() as i64, &content_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
        }))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let req = request.into_inner();

        // For now, we'll assume a single tenant.
        let tenant_id = 1;

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, &req.bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        let object = self
            .db
            .get_object(bucket.id, &req.object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        let data = self
            .storage
            .retrieve(&object.content_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let info = ObjectInfo {
                content_type: object.content_type.unwrap_or_default(),
                content_length: object.size,
            };

            tx.send(Ok(GetObjectResponse {
                data: Some(get_object_response::Data::Metadata(info)),
            }))
            .await
            .unwrap();

            for chunk in data.chunks(1024 * 1024) { // 1MB chunks
                tx.send(Ok(GetObjectResponse {
                    data: Some(get_object_response::Data::Chunk(chunk.to_vec())),
                }))
                .await
                .unwrap();
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::GetObjectStream))
    }

    async fn delete_object(
        &self,
        _request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        todo!()
    }

    async fn head_object(
        &self,
        _request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        todo!()
    }

    async fn list_objects(
        &self,
        _request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        todo!()
    }

    async fn initiate_multipart_upload(
        &self,
        _request: Request<InitiateMultipartRequest>,
    ) -> Result<Response<InitiateMultipartResponse>, Status> {
        todo!()
    }

    async fn complete_multipart_upload(
        &self,
        _request: Request<CompleteMultipartRequest>,
    ) -> Result<Response<CompleteMultipartResponse>, Status> {
        todo!()
    }
}
