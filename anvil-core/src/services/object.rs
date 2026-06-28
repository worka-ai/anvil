use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::object_manager::ObjectWriteOptions;
use crate::permissions::AnvilAction;
use crate::{AppState, auth, authz_journal, bucket_journal, watch_log};
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

        let (bucket_name, object_key, mutation_context) = match stream.next().await {
            Some(Ok(chunk)) => match chunk.data {
                Some(put_object_request::Data::Metadata(meta)) => {
                    (meta.bucket_name, meta.object_key, meta.mutation_context)
                }
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            _ => return Err(Status::invalid_argument("Empty stream")),
        };
        validate_native_mutation_context(self, &claims, &bucket_name, mutation_context.as_ref())
            .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &bucket_name,
            &object_key,
            mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

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
                ObjectWriteOptions::default(),
            )
            .await?;
        let watch_cursor = object_watch_cursor(self, &object).await?;

        Ok(Response::new(PutObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision: u64::try_from(object.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: object.index_policy_snapshot,
            watch_cursor,
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

            while let Some(chunk_result) = data_stream.next().await {
                let chunk = match chunk_result {
                    Ok(chunk) => chunk,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                };
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
        validate_native_mutation_context(
            self,
            claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectDelete,
        )
        .await?;

        let deleted =
            if let Some(version_id) = parse_optional_version_id(req.version_id.as_deref())? {
                self.object_manager
                    .delete_object_version(
                        claims.tenant_id,
                        &req.bucket_name,
                        &req.object_key,
                        version_id,
                        &claims.scopes,
                    )
                    .await?
            } else {
                self.object_manager
                    .delete_object(
                        claims.tenant_id,
                        &req.bucket_name,
                        &req.object_key,
                        &claims.scopes,
                    )
                    .await?
            };
        let watch_cursor = object_watch_cursor(self, &deleted).await?;

        Ok(Response::new(DeleteObjectResponse {
            version_id: deleted.version_id.to_string(),
            mutation_id: deleted.mutation_id.to_string(),
            payload_hash: deleted.content_hash,
            record_hash: deleted.record_hash,
            authz_revision: u64::try_from(deleted.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: deleted.index_policy_snapshot,
            watch_cursor,
            delete_marker: deleted.deleted_at.is_some(),
        }))
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
                &req.version_id_marker,
                req.max_keys,
            )
            .await?;
        let response_versions = versions
            .versions
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

        Ok(Response::new(ListObjectVersionsResponse {
            versions: response_versions,
            is_truncated: versions.is_truncated,
            next_key_marker: versions.next_key_marker.unwrap_or_default(),
            next_version_id_marker: versions
                .next_version_id_marker
                .map(|marker| marker.to_string())
                .unwrap_or_default(),
        }))
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.destination_bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.destination_bucket_name,
            &req.destination_object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

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
        let watch_cursor = object_watch_cursor(self, &object).await?;
        let authz_revision = object_authz_revision(&object)?;

        Ok(Response::new(CopyObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.destination_bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.destination_bucket_name,
            &req.destination_object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

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
        let watch_cursor = object_watch_cursor(self, &object).await?;
        let authz_revision = object_authz_revision(&object)?;

        Ok(Response::new(ComposeObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

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
        let watch_cursor = object_watch_cursor(self, &object).await?;
        let authz_revision = object_authz_revision(&object)?;

        Ok(Response::new(PatchJsonObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.manifest_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
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
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(CompareAndSwapManifestResponse {
            revision: result.revision,
            manifest_hash: result.manifest_hash.clone(),
            version_id: result.revision.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.manifest_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.stream_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        let result = self
            .object_manager
            .create_append_stream(
                claims.tenant_id,
                &req.bucket_name,
                &req.stream_key,
                &claims.scopes,
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(CreateAppendStreamResponse {
            stream_id: result.stream_id.to_string(),
            version_id: result.stream_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.stream_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
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
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(AppendStreamRecordResponse {
            record_sequence: record.record_sequence,
            payload_hash: record.payload_hash,
            payload_size: record.payload_size,
            version_id: record.record_sequence.to_string(),
            mutation_id: record.receipt.mutation_id.to_string(),
            record_hash: record.receipt.record_hash,
            authz_revision,
            watch_cursor: record.receipt.watch_cursor,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.stream_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        let version_id = req.stream_id.clone();
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
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(SealAppendStreamSegmentResponse {
            record_count: sealed.record_count,
            segment_hash: sealed.segment_hash.clone(),
            version_id,
            mutation_id: sealed.receipt.mutation_id.to_string(),
            payload_hash: sealed.segment_hash,
            record_hash: sealed.receipt.record_hash,
            authz_revision,
            watch_cursor: sealed.receipt.watch_cursor,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

        let result = self
            .object_manager
            .initiate_multipart_upload(
                claims.tenant_id,
                &req.bucket_name,
                &req.object_key,
                &claims.scopes,
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(InitiateMultipartResponse {
            upload_id: result.upload_id.to_string(),
            version_id: result.upload_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
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
        validate_native_mutation_context(
            self,
            &claims,
            &metadata.bucket_name,
            metadata.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &metadata.bucket_name,
            &metadata.object_key,
            metadata.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;

        let part_version_id = metadata.part_number.to_string();
        let upload_id = uuid::Uuid::parse_str(&metadata.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;
        let data_stream = stream.map(|chunk_result| match chunk_result {
            Ok(chunk) => match chunk.data {
                Some(upload_part_request::Data::Chunk(bytes)) => Ok(bytes),
                _ => Ok(vec![]),
            },
            Err(e) => Err(e),
        });

        let result = self
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
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(UploadPartResponse {
            etag: result.etag,
            version_id: part_version_id,
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        }))
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
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
        let watch_cursor = object_watch_cursor(self, &object).await?;
        let authz_revision = object_authz_revision(&object)?;

        Ok(Response::new(CompleteMultipartResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
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
        validate_native_mutation_context(
            self,
            &claims,
            &req.bucket_name,
            req.mutation_context.as_ref(),
        )
        .await?;
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        let upload_id = uuid::Uuid::parse_str(&req.upload_id)
            .map_err(|_| Status::invalid_argument("Invalid upload_id"))?;

        let result = self
            .object_manager
            .abort_multipart_upload(
                claims.tenant_id,
                &req.bucket_name,
                &req.object_key,
                upload_id,
                &claims.scopes,
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        Ok(Response::new(AbortMultipartResponse {
            version_id: result.upload_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        }))
    }
}

async fn validate_native_mutation_context(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    context: Option<&NativeMutationContext>,
) -> Result<(), Status> {
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    if context.tenant_id != claims.tenant_id {
        return Err(Status::permission_denied("Native mutation tenant mismatch"));
    }
    if context.principal != claims.sub {
        return Err(Status::permission_denied(
            "Native mutation principal mismatch",
        ));
    }
    require_native_context_field("request_id", &context.request_id)?;
    require_native_context_field("precondition", &context.precondition)?;
    require_native_context_field("idempotency_key", &context.idempotency_key)?;
    if context.bucket_id <= 0 {
        return Err(Status::invalid_argument(
            "Native mutation bucket_id is required",
        ));
    }

    let bucket = bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket_name)
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .ok_or_else(|| Status::not_found("BucketNotFound"))?;
    if bucket.id != context.bucket_id {
        return Err(Status::permission_denied("Native mutation bucket mismatch"));
    }

    if let Some(required_revision) = parse_authz_zookie(&context.authz_zookie_optional)? {
        let latest = authz_journal::latest_authz_revision(&state.storage, claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if latest < required_revision {
            return Err(Status::failed_precondition("AuthzRevisionUnavailable"));
        }
    }

    Ok(())
}

enum NativeMutationPrecondition<'a> {
    None,
    Exists,
    NotExists,
    Version(uuid::Uuid),
    Etag(&'a str),
}

async fn enforce_native_mutation_precondition(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
    object_key: &str,
    context: Option<&NativeMutationContext>,
    action: AnvilAction,
) -> Result<(), Status> {
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    let precondition = parse_native_mutation_precondition(&context.precondition)?;
    if matches!(precondition, NativeMutationPrecondition::None) {
        return Ok(());
    }

    let current = state
        .object_manager
        .current_object_for_mutation_precondition(
            claims.tenant_id,
            bucket_name,
            object_key,
            &claims.scopes,
            action,
        )
        .await?;
    let current = current
        .as_ref()
        .filter(|object| object.deleted_at.is_none());

    let satisfied = match precondition {
        NativeMutationPrecondition::None => true,
        NativeMutationPrecondition::Exists => current.is_some(),
        NativeMutationPrecondition::NotExists => current.is_none(),
        NativeMutationPrecondition::Version(expected) => current
            .map(|object| object.version_id == expected)
            .unwrap_or(false),
        NativeMutationPrecondition::Etag(expected) => current
            .map(|object| etag_matches(&object.etag, expected))
            .unwrap_or(false),
    };
    if !satisfied {
        return Err(Status::failed_precondition(
            "Native mutation precondition failed",
        ));
    }
    Ok(())
}

fn parse_native_mutation_precondition(
    value: &str,
) -> Result<NativeMutationPrecondition<'_>, Status> {
    let value = value.trim();
    if value.eq_ignore_ascii_case("none") {
        return Ok(NativeMutationPrecondition::None);
    }
    if value.eq_ignore_ascii_case("exists") {
        return Ok(NativeMutationPrecondition::Exists);
    }
    if value.eq_ignore_ascii_case("not_exists")
        || value.eq_ignore_ascii_case("not-exists")
        || value.eq_ignore_ascii_case("absent")
    {
        return Ok(NativeMutationPrecondition::NotExists);
    }
    if let Some(version) = value.strip_prefix("version:") {
        let version = uuid::Uuid::parse_str(version.trim()).map_err(|_| {
            Status::invalid_argument("Invalid native mutation version precondition")
        })?;
        return Ok(NativeMutationPrecondition::Version(version));
    }
    if let Some(etag) = value.strip_prefix("etag:") {
        let etag = etag.trim();
        if etag.is_empty() {
            return Err(Status::invalid_argument(
                "Invalid native mutation etag precondition",
            ));
        }
        return Ok(NativeMutationPrecondition::Etag(etag));
    }
    Err(Status::invalid_argument(
        "Unsupported native mutation precondition",
    ))
}

fn etag_matches(actual: &str, expected: &str) -> bool {
    actual == expected || trim_etag_quotes(actual) == trim_etag_quotes(expected)
}

fn trim_etag_quotes(value: &str) -> &str {
    value.trim().trim_matches('"')
}

fn require_native_context_field(name: &str, value: &str) -> Result<(), Status> {
    if value.trim().is_empty() {
        return Err(Status::invalid_argument(format!(
            "Native mutation {name} is required"
        )));
    }
    Ok(())
}

fn parse_authz_zookie(value: &str) -> Result<Option<i64>, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    let revision = value
        .strip_prefix("authz:")
        .unwrap_or(value)
        .parse::<i64>()
        .map_err(|_| Status::invalid_argument("Invalid authz_zookie_optional"))?;
    if revision < 0 {
        return Err(Status::invalid_argument("Invalid authz_zookie_optional"));
    }
    Ok(Some(revision))
}

async fn latest_authz_revision(state: &AppState, tenant_id: i64) -> Result<u64, Status> {
    let revision = authz_journal::latest_authz_revision(&state.storage, tenant_id)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    u64::try_from(revision).map_err(|_| Status::internal("Invalid authz revision"))
}

fn parse_optional_version_id(value: Option<&str>) -> Result<Option<uuid::Uuid>, Status> {
    value
        .filter(|value| !value.is_empty())
        .map(uuid::Uuid::parse_str)
        .transpose()
        .map_err(|_| Status::invalid_argument("Invalid version_id"))
}

async fn object_watch_cursor(
    state: &AppState,
    object: &crate::persistence::Object,
) -> Result<u64, Status> {
    let cursor = watch_log::latest_object_watch_cursor(
        &state.storage,
        object.tenant_id,
        object.bucket_id,
        object.version_id,
    )
    .await
    .map_err(|e| Status::internal(e.to_string()))?
    .ok_or_else(|| Status::internal("Object mutation watch event not found"))?;
    u64::try_from(cursor).map_err(|_| Status::internal("Invalid object watch cursor"))
}

fn object_authz_revision(object: &crate::persistence::Object) -> Result<u64, Status> {
    u64::try_from(object.authz_revision).map_err(|_| Status::internal("Invalid authz revision"))
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
