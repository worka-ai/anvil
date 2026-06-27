use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::{AppState, auth};
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl ObjectService for AppState {
    type GetObjectStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<GetObjectResponse, Status>> + Send>,
    >;

    async fn put_object(
        &self,
        request: Request<tonic::Streaming<PutObjectRequest>>,
    ) -> Result<Response<PutObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let mut stream = request.into_inner();

        let (bucket_name, object_key) = match stream.next().await {
            Some(Ok(chunk)) => match chunk.data {
                Some(put_object_request::Data::Metadata(meta)) => {
                    (meta.bucket_name, meta.object_key)
                }
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            _ => return Err(Status::invalid_argument("Empty stream")),
        };

        let data_stream = stream.map(|chunk_result| match chunk_result {
            Ok(chunk) => match chunk.data {
                Some(put_object_request::Data::Chunk(bytes)) => Ok(bytes),
                _ => Ok(vec![]), // Or handle as an error, but must be Vec<u8>
            },
            Err(e) => Err(e),
        });

        let object = self
            .object_manager
            .put_object(
                claims.tenant_id,
                &bucket_name,
                &object_key,
                &claims.scopes,
                data_stream,
            )
            .await?;

        Ok(Response::new(PutObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
        }))
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<Self::GetObjectStream>, Status> {
        let claims = request.extensions().get::<auth::Claims>().cloned();
        let req = request.into_inner();

        let (object, mut data_stream) = self
            .object_manager
            .get_object(
                claims,
                req.bucket_name,
                req.object_key,
                parse_optional_version_id(req.version_id.as_deref())?,
            )
            .await?;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            let info = ObjectInfo {
                content_type: object.content_type.clone().unwrap_or_default(),
                content_length: object.size,
            };
            if tx
                .send(Ok(GetObjectResponse {
                    data: Some(get_object_response::Data::Metadata(info)),
                }))
                .await
                .is_err()
            {
                return; // Client disconnected
            }

            while let Some(Ok(chunk)) = data_stream.next().await {
                if tx
                    .send(Ok(GetObjectResponse {
                        data: Some(get_object_response::Data::Chunk(chunk.to_vec())),
                    }))
                    .await
                    .is_err()
                {
                    break; // Client disconnected
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::GetObjectStream
        ))
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        self.object_manager
            .delete_object(
                claims.tenant_id,
                &req.bucket_name,
                &req.object_key,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(DeleteObjectResponse {}))
    }

    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let object = self
            .object_manager
            .head_object(
                Some(claims.clone()),
                &req.bucket_name,
                &req.object_key,
                parse_optional_version_id(req.version_id.as_deref())?,
            )
            .await?;

        Ok(Response::new(HeadObjectResponse {
            etag: object.etag,
            size: object.size,
            last_modified: object.created_at.to_string(),
        }))
    }

    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let (objects, common_prefixes) = self
            .object_manager
            .list_objects(
                Some(claims.clone()),
                &req.bucket_name,
                &req.prefix,
                &req.start_after,
                req.max_keys,
                &req.delimiter,
            )
            .await?;

        let response_objects = objects
            .into_iter()
            .map(|o| crate::anvil_api::ObjectSummary {
                key: o.key,
                size: o.size,
                last_modified: o.created_at.to_string(),
                etag: o.etag,
            })
            .collect();

        Ok(Response::new(ListObjectsResponse {
            objects: response_objects,
            common_prefixes,
        }))
    }

    async fn list_object_versions(
        &self,
        request: Request<ListObjectVersionsRequest>,
    ) -> Result<Response<ListObjectVersionsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let versions = self
            .object_manager
            .list_object_versions(
                Some(claims.clone()),
                &req.bucket_name,
                &req.prefix,
                &req.key_marker,
                req.max_keys,
            )
            .await?
            .into_iter()
            .map(|version| {
                let object = version.object;
                crate::anvil_api::ObjectVersionSummary {
                    key: object.key,
                    version_id: object.version_id.to_string(),
                    size: object.size,
                    last_modified: object.created_at.to_string(),
                    etag: object.etag,
                    is_delete_marker: version.is_delete_marker,
                    is_latest: version.is_latest,
                }
            })
            .collect();

        Ok(Response::new(ListObjectVersionsResponse { versions }))
    }

    async fn copy_object(
        &self,
        request: Request<CopyObjectRequest>,
    ) -> Result<Response<CopyObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();

        let object = self
            .object_manager
            .copy_object(
                claims,
                &req.source_bucket_name,
                &req.source_object_key,
                parse_optional_version_id(req.source_version_id.as_deref())?,
                &req.destination_bucket_name,
                &req.destination_object_key,
            )
            .await?;

        Ok(Response::new(CopyObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
        }))
    }

    async fn compose_object(
        &self,
        request: Request<ComposeObjectRequest>,
    ) -> Result<Response<ComposeObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();

        let mut sources = Vec::with_capacity(req.sources.len());
        for source in req.sources {
            sources.push(crate::object_manager::ComposeSource {
                bucket_name: source.bucket_name,
                object_key: source.object_key,
                version_id: parse_optional_version_id(source.version_id.as_deref())?,
            });
        }

        let object = self
            .object_manager
            .compose_object(
                claims,
                sources,
                &req.destination_bucket_name,
                &req.destination_object_key,
            )
            .await?;

        Ok(Response::new(ComposeObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
        }))
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

fn parse_optional_version_id(value: Option<&str>) -> Result<Option<uuid::Uuid>, Status> {
    value
        .filter(|value| !value.is_empty())
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|_| Status::invalid_argument("Invalid version_id"))
}
