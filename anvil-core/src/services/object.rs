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
    type WatchPrefixStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchPrefixResponse, Status>> + Send>,
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
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision: u64::try_from(object.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: object.index_policy_snapshot,
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
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            record_hash: object.record_hash,
            authz_revision: u64::try_from(object.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: object.index_policy_snapshot,
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

    async fn patch_json_object(
        &self,
        request: Request<PatchJsonObjectRequest>,
    ) -> Result<Response<PatchJsonObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();

        let object = self
            .object_manager
            .patch_json_object(
                claims,
                &req.bucket_name,
                &req.object_key,
                parse_optional_version_id(req.base_version_id.as_deref())?,
                &req.merge_patch_json,
            )
            .await?;

        Ok(Response::new(PatchJsonObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
        }))
    }

    async fn compare_and_swap_manifest(
        &self,
        request: Request<CompareAndSwapManifestRequest>,
    ) -> Result<Response<CompareAndSwapManifestResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let result = self
            .object_manager
            .compare_and_swap_manifest(
                claims.tenant_id,
                &req.bucket_name,
                &req.manifest_key,
                req.expected_revision,
                &req.manifest_json,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(CompareAndSwapManifestResponse {
            revision: result.revision,
            manifest_hash: result.manifest_hash,
        }))
    }

    async fn watch_prefix(
        &self,
        request: Request<WatchPrefixRequest>,
    ) -> Result<Response<Self::WatchPrefixStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let tenant_id = claims.tenant_id;
        let prefix = req.prefix.clone();
        let (bucket_id, snapshot, mut live) = self
            .object_manager
            .watch_prefix_snapshot(claims, &req.bucket_name, &req.prefix, req.after_cursor)
            .await?;

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = req.after_cursor;
            for event in snapshot {
                if let Some(response) = watch_event_response(&event) {
                    last_cursor = last_cursor.max(response.cursor);
                    if tx.send(Ok(response)).await.is_err() {
                        return;
                    }
                }
            }

            loop {
                match live.recv().await {
                    Ok(event) => {
                        if event.tenant_id != tenant_id
                            || event.bucket_id != bucket_id
                            || !event.key.starts_with(&prefix)
                        {
                            continue;
                        }
                        let Some(response) = watch_event_response(&event) else {
                            continue;
                        };
                        if response.cursor <= last_cursor {
                            continue;
                        }
                        last_cursor = response.cursor;
                        if tx.send(Ok(response)).await.is_err() {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::data_loss(
                                "Watch cursor fell behind retained live event window",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchPrefixStream
        ))
    }

    async fn create_append_stream(
        &self,
        request: Request<CreateAppendStreamRequest>,
    ) -> Result<Response<CreateAppendStreamResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let stream_id = self
            .object_manager
            .create_append_stream(
                claims.tenant_id,
                &req.bucket_name,
                &req.stream_key,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(CreateAppendStreamResponse {
            stream_id: stream_id.to_string(),
        }))
    }

    async fn append_stream_record(
        &self,
        request: Request<AppendStreamRecordRequest>,
    ) -> Result<Response<AppendStreamRecordResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let stream_id = uuid::Uuid::parse_str(&req.stream_id)
            .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
        let record = self
            .object_manager
            .append_stream_record(
                claims.tenant_id,
                &req.bucket_name,
                &req.stream_key,
                stream_id,
                req.payload,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(AppendStreamRecordResponse {
            record_sequence: record.record_sequence,
            payload_hash: record.payload_hash,
            payload_size: record.payload_size,
        }))
    }

    async fn seal_append_stream_segment(
        &self,
        request: Request<SealAppendStreamSegmentRequest>,
    ) -> Result<Response<SealAppendStreamSegmentResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let stream_id = uuid::Uuid::parse_str(&req.stream_id)
            .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
        let sealed = self
            .object_manager
            .seal_append_stream_segment(
                claims.tenant_id,
                &req.bucket_name,
                &req.stream_key,
                stream_id,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(SealAppendStreamSegmentResponse {
            record_count: sealed.record_count,
            segment_hash: sealed.segment_hash,
        }))
    }

    async fn initiate_multipart_upload(
        &self,
        request: Request<InitiateMultipartRequest>,
    ) -> Result<Response<InitiateMultipartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();

        let upload_id = self
            .object_manager
            .initiate_multipart_upload(
                claims.tenant_id,
                &req.bucket_name,
                &req.object_key,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(InitiateMultipartResponse {
            upload_id: upload_id.to_string(),
        }))
    }

    async fn upload_part(
        &self,
        request: Request<tonic::Streaming<UploadPartRequest>>,
    ) -> Result<Response<UploadPartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;

        let mut stream = request.into_inner();
        let metadata = match stream.next().await {
            Some(Ok(chunk)) => match chunk.data {
                Some(upload_part_request::Data::Metadata(metadata)) => metadata,
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            Some(Err(status)) => return Err(status),
            None => return Err(Status::invalid_argument("Empty stream")),
        };

        let upload_id = uuid::Uuid::parse_str(&metadata.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;
        let data_stream = stream.map(|chunk_result| match chunk_result {
            Ok(chunk) => match chunk.data {
                Some(upload_part_request::Data::Chunk(bytes)) => Ok(bytes),
                _ => Ok(vec![]),
            },
            Err(e) => Err(e),
        });

        let etag = self
            .object_manager
            .upload_part(
                claims.tenant_id,
                &metadata.bucket_name,
                &metadata.object_key,
                upload_id,
                metadata.part_number,
                &claims.scopes,
                data_stream,
            )
            .await?;

        Ok(Response::new(UploadPartResponse { etag }))
    }

    async fn complete_multipart_upload(
        &self,
        request: Request<CompleteMultipartRequest>,
    ) -> Result<Response<CompleteMultipartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let upload_id = uuid::Uuid::parse_str(&req.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;
        let parts = req
            .parts
            .into_iter()
            .map(|part| crate::object_manager::CompleteMultipartPart {
                part_number: part.part_number,
                etag: part.etag,
            })
            .collect();

        let object = self
            .object_manager
            .complete_multipart_upload(
                claims.tenant_id,
                &req.bucket_name,
                &req.object_key,
                upload_id,
                parts,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(CompleteMultipartResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
        }))
    }

    async fn abort_multipart_upload(
        &self,
        request: Request<AbortMultipartRequest>,
    ) -> Result<Response<AbortMultipartResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let upload_id = uuid::Uuid::parse_str(&req.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;

        self.object_manager
            .abort_multipart_upload(
                claims.tenant_id,
                &req.bucket_name,
                &req.object_key,
                upload_id,
                &claims.scopes,
            )
            .await?;

        Ok(Response::new(AbortMultipartResponse {}))
    }
}

fn parse_optional_version_id(value: Option<&str>) -> Result<Option<uuid::Uuid>, Status> {
    value
        .filter(|value| !value.is_empty())
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|_| Status::invalid_argument("Invalid version_id"))
}

fn watch_event_response(
    event: &crate::persistence::ObjectWatchEvent,
) -> Option<WatchPrefixResponse> {
    Some(WatchPrefixResponse {
        cursor: u64::try_from(event.id).ok()?,
        bucket_name: event.bucket_name.clone(),
        object_key: event.key.clone(),
        event_type: event.event_type.clone(),
        version_id: event
            .version_id
            .map(|version_id| version_id.to_string())
            .unwrap_or_default(),
        etag: event.etag.clone().unwrap_or_default(),
        size: event.size,
        is_delete_marker: event.is_delete_marker,
        created_at: event.created_at.to_string(),
    })
}
