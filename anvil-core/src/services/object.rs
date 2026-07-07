use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::mesh_lifecycle::{CreateHostAliasDescriptor, LifecycleError};
use crate::native_idempotency::{self, NativeIdempotencyTarget};
use crate::object_links;
use crate::object_manager::ObjectWriteOptions;
use crate::permissions::AnvilAction;
use crate::routing::{
    self, HostAliasDescriptor as CoreHostAliasDescriptor, HostAliasState as CoreHostAliasState,
    RoutingConfig,
};
use crate::{
    AppState, auth, authz_journal, bucket_journal,
    services::watch_envelope::{self, WatchEnvelopeParts},
    task_lease, watch_log,
};
use futures_util::StreamExt;
use serde::{Serialize, de::DeserializeOwned};
use tokio::sync::OwnedMutexGuard;
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
    type TailAppendStreamStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<TailAppendStreamResponse, Status>> + Send>,
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

        let (bucket_name, object_key, mutation_context, content_type, user_metadata) =
            match stream.next().await {
                Some(Ok(chunk)) => match chunk.data {
                    Some(put_object_request::Data::Metadata(meta)) => (
                        meta.bucket_name,
                        meta.object_key,
                        meta.mutation_context,
                        meta.content_type,
                        parse_user_metadata_json(&meta.user_metadata_json)?,
                    ),
                    _ => return Err(Status::invalid_argument("First chunk must be metadata")),
                },
                _ => return Err(Status::invalid_argument("Empty stream")),
            };
        validate_native_mutation_context(self, &claims, &bucket_name, mutation_context.as_ref())
            .await?;
        let target = NativeIdempotencyTarget::new("PutObject", &bucket_name, &object_key);
        let (attempt, replay) = begin_native_mutation::<PutObjectResponse>(
            self,
            mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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
                ObjectWriteOptions {
                    content_type,
                    user_metadata,
                },
            )
            .await?;
        let watch_cursor = object_watch_cursor(self, &object).await?;

        let response = PutObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision: u64::try_from(object.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: object.index_policy_snapshot,
            watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
                version_id: object.version_id.to_string(),
                user_metadata_json: json_object_string(object.user_meta.as_ref()),
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
        let target =
            NativeIdempotencyTarget::new("DeleteObject", &req.bucket_name, &req.object_key)
                .with_parameters(serde_json::json!({
                    "version_id": req.version_id.as_deref().unwrap_or("")
                }));
        let (attempt, replay) = begin_native_mutation::<DeleteObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectDelete,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = DeleteObjectResponse {
            version_id: deleted.version_id.to_string(),
            mutation_id: deleted.mutation_id.to_string(),
            payload_hash: deleted.content_hash,
            record_hash: deleted.record_hash,
            authz_revision: u64::try_from(deleted.authz_revision)
                .map_err(|_| Status::internal("Invalid authz revision"))?,
            index_policy_snapshot: deleted.index_policy_snapshot,
            watch_cursor,
            delete_marker: deleted.deleted_at.is_some(),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
            content_type: object.content_type.unwrap_or_default(),
            user_metadata_json: json_object_string(object.user_meta.as_ref()),
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
                content_type: o.content_type.unwrap_or_default(),
                user_metadata_json: json_object_string(o.user_meta.as_ref()),
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
                    content_type: object.content_type.unwrap_or_default(),
                    user_metadata_json: json_object_string(object.user_meta.as_ref()),
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
        let target = NativeIdempotencyTarget::new(
            "CopyObject",
            &req.destination_bucket_name,
            &req.destination_object_key,
        )
        .with_parameters(serde_json::json!({
            "source_bucket_name": req.source_bucket_name.clone(),
            "source_object_key": req.source_object_key.clone(),
            "source_version_id": req.source_version_id.as_deref().unwrap_or("")
        }));
        let (attempt, replay) = begin_native_mutation::<CopyObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = CopyObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target_sources = req
            .sources
            .iter()
            .map(|source| {
                serde_json::json!({
                    "bucket_name": source.bucket_name.clone(),
                    "object_key": source.object_key.clone(),
                    "version_id": source.version_id.as_deref().unwrap_or("")
                })
            })
            .collect::<Vec<_>>();
        let target = NativeIdempotencyTarget::new(
            "ComposeObject",
            &req.destination_bucket_name,
            &req.destination_object_key,
        )
        .with_parameters(serde_json::json!({ "sources": target_sources }));
        let (attempt, replay) = begin_native_mutation::<ComposeObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = ComposeObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target = NativeIdempotencyTarget::new(
            "PatchJsonObject",
            &req.bucket_name,
            &req.object_key,
        )
        .with_parameters(serde_json::json!({
            "base_version_id": req.base_version_id.as_deref().unwrap_or(""),
            "merge_patch_hash": blake3::hash(req.merge_patch_json.as_bytes()).to_hex().to_string()
        }));
        let (attempt, replay) = begin_native_mutation::<PatchJsonObjectResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.object_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;

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

        let response = PatchJsonObjectResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            last_modified: object.created_at.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target = NativeIdempotencyTarget::new(
            "CompareAndSwapManifest",
            &req.bucket_name,
            &req.manifest_key,
        )
        .with_parameters(serde_json::json!({
            "expected_revision": req.expected_revision,
            "manifest_hash": blake3::hash(req.manifest_json.as_bytes()).to_hex().to_string()
        }));
        let (attempt, replay) = begin_native_mutation::<CompareAndSwapManifestResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.manifest_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;
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

        let response = CompareAndSwapManifestResponse {
            revision: result.revision,
            manifest_hash: result.manifest_hash.clone(),
            version_id: result.revision.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.manifest_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target =
            NativeIdempotencyTarget::new("CreateAppendStream", &req.bucket_name, &req.stream_key);
        let (attempt, replay) = begin_native_mutation::<CreateAppendStreamResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = CreateAppendStreamResponse {
            stream_id: result.stream_id.to_string(),
            version_id: result.stream_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target =
            NativeIdempotencyTarget::new("AppendStreamRecord", &req.bucket_name, &req.stream_key)
                .with_parameters(serde_json::json!({
                    "stream_id": req.stream_id.clone(),
                    "payload_hash": blake3::hash(&req.payload).to_hex().to_string()
                }));
        let (attempt, replay) = begin_native_mutation::<AppendStreamRecordResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.stream_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;
        let stream_id = uuid::Uuid::parse_str(&req.stream_id)
            .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
        let user_metadata = parse_user_metadata_json(&req.user_metadata_json)?;
        let record = self
            .object_manager
            .append_stream_record(
                claims.tenant_id,
                &req.bucket_name,
                &req.stream_key,
                stream_id,
                req.payload,
                req.content_type,
                user_metadata,
                &claims.scopes,
            )
            .await?;
        let authz_revision = latest_authz_revision(self, claims.tenant_id).await?;

        let response = AppendStreamRecordResponse {
            record_sequence: record.record_sequence,
            payload_hash: record.payload_hash,
            payload_size: record.payload_size,
            version_id: record.record_sequence.to_string(),
            mutation_id: record.receipt.mutation_id.to_string(),
            record_hash: record.receipt.record_hash,
            authz_revision,
            watch_cursor: record.receipt.watch_cursor,
            content_type: record.content_type.unwrap_or_default(),
            user_metadata_json: json_object_string(record.user_metadata.as_ref()),
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn read_append_stream(
        &self,
        request: Request<ReadAppendStreamRequest>,
    ) -> Result<Response<ReadAppendStreamResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let stream_id = uuid::Uuid::parse_str(&req.stream_id)
            .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
        let records = self
            .object_manager
            .read_append_stream_records(
                claims,
                &req.bucket_name,
                &req.stream_key,
                stream_id,
                req.after_sequence,
                req.limit,
                req.include_payload,
            )
            .await?;
        let next_after_sequence = records
            .last()
            .map(|record| record.record_sequence)
            .unwrap_or(req.after_sequence);
        let records = records.into_iter().map(append_stream_record_info).collect();
        Ok(Response::new(ReadAppendStreamResponse {
            records,
            next_after_sequence,
            is_end: true,
        }))
    }

    async fn tail_append_stream(
        &self,
        request: Request<TailAppendStreamRequest>,
    ) -> Result<Response<Self::TailAppendStreamStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let stream_id = uuid::Uuid::parse_str(&req.stream_id)
            .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
        let (tx, rx) = mpsc::channel(32);
        let state = self.clone();
        let poll_interval =
            std::time::Duration::from_millis(u64::from(req.poll_interval_ms).clamp(100, 30_000));
        tokio::spawn(async move {
            let mut after_sequence = req.from_sequence.saturating_sub(1);
            loop {
                let records = state
                    .object_manager
                    .read_append_stream_records(
                        claims.clone(),
                        &req.bucket_name,
                        &req.stream_key,
                        stream_id,
                        after_sequence,
                        100,
                        req.include_payload,
                    )
                    .await;
                match records {
                    Ok(records) if records.is_empty() => {
                        tokio::time::sleep(poll_interval).await;
                    }
                    Ok(records) => {
                        for record in records {
                            after_sequence = record.record_sequence;
                            if tx
                                .send(Ok(TailAppendStreamResponse {
                                    record: Some(append_stream_record_info(record)),
                                }))
                                .await
                                .is_err()
                            {
                                return;
                            }
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
        });
        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::TailAppendStreamStream
        ))
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
        let target = NativeIdempotencyTarget::new(
            "SealAppendStreamSegment",
            &req.bucket_name,
            &req.stream_key,
        )
        .with_parameters(serde_json::json!({ "stream_id": req.stream_id.clone() }));
        let (attempt, replay) = begin_native_mutation::<SealAppendStreamSegmentResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        enforce_native_mutation_precondition(
            self,
            &claims,
            &req.bucket_name,
            &req.stream_key,
            req.mutation_context.as_ref(),
            AnvilAction::ObjectWrite,
        )
        .await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;
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

        let response = SealAppendStreamSegmentResponse {
            record_count: sealed.record_count,
            segment_hash: sealed.segment_hash.clone(),
            version_id,
            mutation_id: sealed.receipt.mutation_id.to_string(),
            payload_hash: sealed.segment_hash,
            record_hash: sealed.receipt.record_hash,
            authz_revision,
            watch_cursor: sealed.receipt.watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn mutation_batch(
        &self,
        request: Request<MutationBatchRequest>,
    ) -> Result<Response<MutationBatchResponse>, Status> {
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
        if req.operations.is_empty() {
            return Err(Status::invalid_argument(
                "MutationBatch requires at least one operation",
            ));
        }
        validate_mutation_batch_operations(&req)?;
        validate_mutation_batch_authorization(&claims, &req)?;
        let operation_digest = mutation_batch_digest(&req)?;
        let context = req
            .mutation_context
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
        let target =
            NativeIdempotencyTarget::new("MutationBatch", &req.bucket_name, "mutation_batch")
                .with_parameters(serde_json::json!({ "request_digest": operation_digest }));
        let _idempotency_guard = acquire_native_mutation_lock(self, context).await?;
        let replay = native_idempotency::load_response::<MutationBatchResponse>(
            &self.storage,
            context,
            &target,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
        let _operation_guards =
            acquire_mutation_batch_operation_locks(self, claims.tenant_id, &req).await?;
        enforce_write_precondition(self, &claims, req.precondition.as_ref()).await?;

        let mut receipts = Vec::with_capacity(req.operations.len());
        let mut max_watch_cursor = 0_u64;
        for operation in req.operations {
            let Some(op) = operation.op else {
                return Err(Status::invalid_argument(
                    "MutationBatch operation is missing op",
                ));
            };
            match op {
                mutation_batch_operation::Op::PutObject(op) => {
                    let object = self
                        .object_manager
                        .put_object(
                            claims.tenant_id,
                            &req.bucket_name,
                            &op.object_key,
                            &claims.scopes,
                            futures_util::stream::iter(vec![Ok(op.payload)]),
                            ObjectWriteOptions {
                                content_type: op.content_type,
                                user_metadata: parse_user_metadata_json(&op.user_metadata_json)?,
                            },
                        )
                        .await?;
                    let watch_cursor = object_watch_cursor(self, &object).await?;
                    max_watch_cursor = max_watch_cursor.max(watch_cursor);
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "put_object".to_string(),
                        object_key: object.key,
                        version_id: object.version_id.to_string(),
                        mutation_id: object.mutation_id.to_string(),
                        payload_hash: object.content_hash,
                        record_hash: object.record_hash,
                        append_record_sequence: 0,
                        manifest_revision: 0,
                        lease_fence_token: 0,
                    });
                }
                mutation_batch_operation::Op::PatchJsonObject(op) => {
                    let object = self
                        .object_manager
                        .patch_json_object(
                            claims.clone(),
                            &req.bucket_name,
                            &op.object_key,
                            parse_optional_version_id(op.base_version_id.as_deref())?,
                            &op.merge_patch_json,
                        )
                        .await?;
                    let watch_cursor = object_watch_cursor(self, &object).await?;
                    max_watch_cursor = max_watch_cursor.max(watch_cursor);
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "patch_json_object".to_string(),
                        object_key: object.key,
                        version_id: object.version_id.to_string(),
                        mutation_id: object.mutation_id.to_string(),
                        payload_hash: object.content_hash,
                        record_hash: object.record_hash,
                        append_record_sequence: 0,
                        manifest_revision: 0,
                        lease_fence_token: 0,
                    });
                }
                mutation_batch_operation::Op::DeleteObject(op) => {
                    let deleted = if let Some(version_id) =
                        parse_optional_version_id(op.version_id.as_deref())?
                    {
                        self.object_manager
                            .delete_object_version(
                                claims.tenant_id,
                                &req.bucket_name,
                                &op.object_key,
                                version_id,
                                &claims.scopes,
                            )
                            .await?
                    } else {
                        self.object_manager
                            .delete_object(
                                claims.tenant_id,
                                &req.bucket_name,
                                &op.object_key,
                                &claims.scopes,
                            )
                            .await?
                    };
                    let watch_cursor = object_watch_cursor(self, &deleted).await?;
                    max_watch_cursor = max_watch_cursor.max(watch_cursor);
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "delete_object".to_string(),
                        object_key: deleted.key,
                        version_id: deleted.version_id.to_string(),
                        mutation_id: deleted.mutation_id.to_string(),
                        payload_hash: deleted.content_hash,
                        record_hash: deleted.record_hash,
                        append_record_sequence: 0,
                        manifest_revision: 0,
                        lease_fence_token: 0,
                    });
                }
                mutation_batch_operation::Op::AppendStreamRecord(op) => {
                    let stream_id = uuid::Uuid::parse_str(&op.stream_id)
                        .map_err(|_| Status::invalid_argument("Invalid stream_id"))?;
                    let record = self
                        .object_manager
                        .append_stream_record(
                            claims.tenant_id,
                            &req.bucket_name,
                            &op.stream_key,
                            stream_id,
                            op.payload,
                            op.content_type,
                            parse_user_metadata_json(&op.user_metadata_json)?,
                            &claims.scopes,
                        )
                        .await?;
                    max_watch_cursor = max_watch_cursor.max(record.receipt.watch_cursor);
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "append_stream_record".to_string(),
                        object_key: op.stream_key,
                        version_id: record.record_sequence.to_string(),
                        mutation_id: record.receipt.mutation_id.to_string(),
                        payload_hash: record.payload_hash,
                        record_hash: record.receipt.record_hash,
                        append_record_sequence: record.record_sequence,
                        manifest_revision: 0,
                        lease_fence_token: 0,
                    });
                }
                mutation_batch_operation::Op::CheckpointTaskLease(op) => {
                    let owner = lease_owner_from_claims(&claims);
                    let lease = self
                        .persistence
                        .checkpoint_named_task_lease(
                            &op.task_id,
                            &owner,
                            op.fence_token,
                            join_u128(op.checkpoint_cursor_low, op.checkpoint_cursor_high),
                        )
                        .await
                        .map_err(lease_error_status)?;
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "checkpoint_task_lease".to_string(),
                        object_key: op.task_id,
                        version_id: lease.lease_epoch.to_string(),
                        mutation_id: String::new(),
                        payload_hash: String::new(),
                        record_hash: lease.lease_hash.unwrap_or_default(),
                        append_record_sequence: 0,
                        manifest_revision: 0,
                        lease_fence_token: lease.fence_token,
                    });
                }
                mutation_batch_operation::Op::CommitTaskLease(op) => {
                    let owner = lease_owner_from_claims(&claims);
                    let lease = self
                        .persistence
                        .commit_named_task_lease(
                            &op.task_id,
                            &owner,
                            op.fence_token,
                            join_u128(op.committed_cursor_low, op.committed_cursor_high),
                        )
                        .await
                        .map_err(lease_error_status)?;
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "commit_task_lease".to_string(),
                        object_key: op.task_id,
                        version_id: lease.lease_epoch.to_string(),
                        mutation_id: String::new(),
                        payload_hash: String::new(),
                        record_hash: lease.lease_hash.unwrap_or_default(),
                        append_record_sequence: 0,
                        manifest_revision: 0,
                        lease_fence_token: lease.fence_token,
                    });
                }
                mutation_batch_operation::Op::CompareAndSwapManifest(op) => {
                    let result = self
                        .object_manager
                        .compare_and_swap_manifest(
                            claims.tenant_id,
                            &req.bucket_name,
                            &op.manifest_key,
                            op.expected_revision,
                            &op.manifest_json,
                            &claims.scopes,
                        )
                        .await?;
                    max_watch_cursor = max_watch_cursor.max(result.receipt.watch_cursor);
                    receipts.push(MutationBatchOperationReceipt {
                        operation: "compare_and_swap_manifest".to_string(),
                        object_key: op.manifest_key,
                        version_id: result.revision.to_string(),
                        mutation_id: result.receipt.mutation_id.to_string(),
                        payload_hash: result.manifest_hash,
                        record_hash: result.receipt.record_hash,
                        append_record_sequence: 0,
                        manifest_revision: result.revision,
                        lease_fence_token: 0,
                    });
                }
            }
        }

        let response = MutationBatchResponse {
            batch_id: operation_digest,
            operation_receipts: receipts,
            watch_cursor: max_watch_cursor,
            mutation_id: context.request_id.clone(),
        };
        native_idempotency::store_response(&self.storage, context, &target, &response).await?;
        Ok(Response::new(response))
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
        let target = NativeIdempotencyTarget::new(
            "InitiateMultipartUpload",
            &req.bucket_name,
            &req.object_key,
        );
        let (attempt, replay) = begin_native_mutation::<InitiateMultipartResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = InitiateMultipartResponse {
            upload_id: result.upload_id.to_string(),
            version_id: result.upload_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target =
            NativeIdempotencyTarget::new("UploadPart", &metadata.bucket_name, &metadata.object_key)
                .with_parameters(serde_json::json!({
                    "upload_id": metadata.upload_id.clone(),
                    "part_number": metadata.part_number
                }));
        let (attempt, replay) = begin_native_mutation::<UploadPartResponse>(
            self,
            metadata.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = UploadPartResponse {
            etag: result.etag,
            version_id: part_version_id,
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target_parts = req
            .parts
            .iter()
            .map(|part| serde_json::json!({"part_number": part.part_number, "etag": part.etag.clone()}))
            .collect::<Vec<_>>();
        let target = NativeIdempotencyTarget::new(
            "CompleteMultipartUpload",
            &req.bucket_name,
            &req.object_key,
        )
        .with_parameters(serde_json::json!({
            "upload_id": req.upload_id.clone(),
            "parts": target_parts
        }));
        let (attempt, replay) = begin_native_mutation::<CompleteMultipartResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = CompleteMultipartResponse {
            etag: object.etag,
            version_id: object.version_id.to_string(),
            mutation_id: object.mutation_id.to_string(),
            payload_hash: object.content_hash,
            record_hash: object.record_hash,
            authz_revision,
            watch_cursor,
            index_policy_snapshot: object.index_policy_snapshot,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
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
        let target =
            NativeIdempotencyTarget::new("AbortMultipartUpload", &req.bucket_name, &req.object_key)
                .with_parameters(serde_json::json!({ "upload_id": req.upload_id.clone() }));
        let (attempt, replay) = begin_native_mutation::<AbortMultipartResponse>(
            self,
            req.mutation_context.as_ref(),
            &target,
            &claims.scopes,
            AnvilAction::ObjectWrite,
        )
        .await?;
        if let Some(response) = replay {
            return Ok(Response::new(response));
        }
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

        let response = AbortMultipartResponse {
            version_id: result.upload_id.to_string(),
            mutation_id: result.receipt.mutation_id.to_string(),
            payload_hash: result.receipt.payload_hash,
            record_hash: result.receipt.record_hash,
            authz_revision,
            watch_cursor: result.receipt.watch_cursor,
        };
        complete_native_mutation(self, &attempt, &target, &response).await?;
        Ok(Response::new(response))
    }

    async fn create_object_link(
        &self,
        request: Request<CreateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_tenant_locator(&claims, &req.tenant_id)?;
        let context = public_link_context(req.context.as_ref(), true)?;
        require_object_link_scope(
            &claims,
            &req.bucket_name,
            &req.link_key,
            AnvilAction::ObjectWrite,
        )?;
        let bucket = public_link_bucket(self, &claims, &req.bucket_name).await?;
        let resolution = object_link_resolution_from_proto(req.resolution)?;
        let target_version = parse_optional_uuid("target_version", req.target_version)?;
        let mutation = self
            .persistence
            .put_object_link(object_links::PutObjectLinkRequest {
                tenant_id: bucket.tenant_id,
                bucket_id: bucket.id,
                link_key: req.link_key,
                target_key: req.target_key,
                target_version,
                resolution,
                expected_generation: None,
                create_only: true,
                allow_dangling: req.allow_dangling,
                idempotency_key: context.idempotency_key.clone(),
                created_by: format!("app:{}", claims.sub),
            })
            .await
            .map_err(object_link_status)?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &context.request_id,
            format!("{}/{}", bucket.name, mutation.descriptor.link_key),
            "object_link.create",
            serde_json::json!({
                "target_key": mutation.descriptor.target_key.clone(),
                "generation": mutation.descriptor.generation
            }),
        )
        .await?;

        Ok(Response::new(ObjectLinkResponse {
            request_id: context.request_id.clone(),
            link: Some(object_link_descriptor_to_proto(mutation.descriptor)),
            audit_event_id,
        }))
    }

    async fn update_object_link(
        &self,
        request: Request<UpdateObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_tenant_locator(&claims, &req.tenant_id)?;
        let context = public_link_context(req.context.as_ref(), false)?;
        require_object_link_scope(
            &claims,
            &req.bucket_name,
            &req.link_key,
            AnvilAction::ObjectWrite,
        )?;
        let bucket = public_link_bucket(self, &claims, &req.bucket_name).await?;
        let resolution = object_link_resolution_from_proto(req.resolution)?;
        let target_version = parse_optional_uuid("target_version", req.target_version)?;
        let mutation = self
            .persistence
            .put_object_link(object_links::PutObjectLinkRequest {
                tenant_id: bucket.tenant_id,
                bucket_id: bucket.id,
                link_key: req.link_key,
                target_key: req.target_key,
                target_version,
                resolution,
                expected_generation: Some(context.expected_generation),
                create_only: false,
                allow_dangling: req.allow_dangling,
                idempotency_key: context.idempotency_key.clone(),
                created_by: format!("app:{}", claims.sub),
            })
            .await
            .map_err(object_link_status)?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &context.request_id,
            format!("{}/{}", bucket.name, mutation.descriptor.link_key),
            "object_link.update",
            serde_json::json!({
                "target_key": mutation.descriptor.target_key.clone(),
                "generation": mutation.descriptor.generation
            }),
        )
        .await?;

        Ok(Response::new(ObjectLinkResponse {
            request_id: context.request_id.clone(),
            link: Some(object_link_descriptor_to_proto(mutation.descriptor)),
            audit_event_id,
        }))
    }

    async fn delete_object_link(
        &self,
        request: Request<DeleteObjectLinkRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_tenant_locator(&claims, &req.tenant_id)?;
        let context = public_link_context(req.context.as_ref(), false)?;
        require_object_link_scope(
            &claims,
            &req.bucket_name,
            &req.link_key,
            AnvilAction::ObjectDelete,
        )?;
        let bucket = public_link_bucket(self, &claims, &req.bucket_name).await?;
        let deleted = self
            .persistence
            .delete_object_link(object_links::DeleteObjectLinkRequest {
                tenant_id: bucket.tenant_id,
                bucket_id: bucket.id,
                link_key: req.link_key,
                expected_generation: context.expected_generation,
                idempotency_key: context.idempotency_key.clone(),
            })
            .await
            .map_err(object_link_status)?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &context.request_id,
            format!("{}/{}", bucket.name, deleted.link_key),
            "object_link.delete",
            serde_json::json!({ "generation": deleted.generation }),
        )
        .await?;

        Ok(Response::new(MutationResponse {
            request_id: context.request_id.clone(),
            resource_id: deleted.link_key,
            generation: deleted.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn read_object_link(
        &self,
        request: Request<ReadObjectLinkRequest>,
    ) -> Result<Response<ObjectLinkResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_tenant_locator(&claims, &req.tenant_id)?;
        require_object_link_scope(
            &claims,
            &req.bucket_name,
            &req.link_key,
            AnvilAction::ObjectRead,
        )?;
        let bucket = public_link_bucket(self, &claims, &req.bucket_name).await?;
        let descriptor = self
            .persistence
            .get_object_link(bucket.id, &req.link_key)
            .await
            .map_err(object_link_status)?
            .ok_or_else(|| Status::not_found("Object link not found"))?;

        Ok(Response::new(ObjectLinkResponse {
            request_id: req.request_id,
            link: Some(object_link_descriptor_to_proto(descriptor)),
            audit_event_id: String::new(),
        }))
    }

    async fn list_object_links(
        &self,
        request: Request<ListObjectLinksRequest>,
    ) -> Result<Response<ListObjectLinksResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_tenant_locator(&claims, &req.tenant_id)?;
        let bucket = public_link_bucket(self, &claims, &req.bucket_name).await?;
        if !auth::is_authorized(
            AnvilAction::ObjectList,
            &format!("{}/{}", bucket.name, req.prefix),
            &claims.scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }
        let mut links = self
            .persistence
            .list_object_links(bucket.id, none_if_empty(&req.prefix))
            .await
            .map_err(object_link_status)?;
        links.retain(|link| {
            auth::is_authorized(
                AnvilAction::ObjectRead,
                &format!("{}/{}", bucket.name, link.link_key),
                &claims.scopes,
            )
        });
        links.sort_by(|a, b| a.link_key.cmp(&b.link_key));
        let limit = page_limit(req.page.as_ref());
        let has_more = links.len() > limit;
        if has_more {
            links.truncate(limit);
        }

        Ok(Response::new(ListObjectLinksResponse {
            page: Some(PageResponse {
                next_cursor: String::new(),
                has_more,
            }),
            links: links
                .into_iter()
                .map(object_link_descriptor_to_proto)
                .collect(),
        }))
    }

    async fn create_host_alias(
        &self,
        request: Request<CreateHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_public_tenant_locator(&claims, &req.tenant_id)?;
        let context = public_link_context(req.context.as_ref(), true)?;
        let bucket = public_host_alias_bucket(self, &claims, &req.bucket_name).await?;
        require_bucket_scope(&claims, &bucket.name, AnvilAction::BucketWrite)?;

        let region = if req.region.trim().is_empty() {
            bucket.region.clone()
        } else {
            req.region
        };
        let routing_config = public_routing_config_for_region(self, &region).await?;
        let host_alias = self
            .persistence
            .create_host_alias_descriptor(
                &routing_config,
                CreateHostAliasDescriptor {
                    hostname: req.hostname,
                    tenant_id: claims.tenant_id.to_string(),
                    bucket_name: bucket.name,
                    region,
                    prefix: req.prefix,
                },
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &context.request_id,
            format!("host_alias:{}", host_alias.hostname),
            "host_alias.create",
            serde_json::json!({
                "bucket_name": host_alias.bucket_name.clone(),
                "region": host_alias.region.clone(),
                "prefix": host_alias.prefix.clone()
            }),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id: context.request_id.clone(),
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn verify_host_alias(
        &self,
        request: Request<VerifyHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let context = public_link_context(req.context.as_ref(), false)?;
        let current = public_host_alias_descriptor(self, &claims, &req.hostname).await?;
        require_bucket_scope(&claims, &current.bucket_name, AnvilAction::BucketWrite)?;
        let expected_challenge = host_alias_verification_challenge(&current);
        if req.observed_challenge.trim() != expected_challenge {
            return Err(Status::failed_precondition(
                "Host alias verification challenge did not match",
            ));
        }
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &current.hostname,
                context.expected_generation,
                CoreHostAliasState::Active,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &context.request_id,
            format!("host_alias:{}", host_alias.hostname),
            "host_alias.verify",
            serde_json::json!({ "generation": host_alias.generation }),
        )
        .await?;

        Ok(Response::new(HostAliasResponse {
            request_id: context.request_id.clone(),
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id,
        }))
    }

    async fn delete_host_alias(
        &self,
        request: Request<DeleteHostAliasRequest>,
    ) -> Result<Response<MutationResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let context = public_link_context(req.context.as_ref(), false)?;
        let current = public_host_alias_descriptor(self, &claims, &req.hostname).await?;
        require_bucket_scope(&claims, &current.bucket_name, AnvilAction::BucketWrite)?;
        let host_alias = self
            .persistence
            .transition_host_alias_descriptor(
                &current.hostname,
                context.expected_generation,
                CoreHostAliasState::Deleted,
            )
            .await
            .map_err(lifecycle_status)?;
        let audit_event_id = crate::services::audit::record_tenant_audit_event(
            self,
            &claims,
            &context.request_id,
            format!("host_alias:{}", host_alias.hostname),
            "host_alias.delete",
            serde_json::json!({ "generation": host_alias.generation }),
        )
        .await?;

        Ok(Response::new(MutationResponse {
            request_id: context.request_id.clone(),
            resource_id: host_alias.hostname,
            generation: host_alias.generation,
            audit_event_id,
            idempotent_replay: false,
        }))
    }

    async fn read_host_alias(
        &self,
        request: Request<ReadHostAliasRequest>,
    ) -> Result<Response<HostAliasResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let host_alias = public_host_alias_descriptor(self, &claims, &req.hostname).await?;
        require_bucket_scope(&claims, &host_alias.bucket_name, AnvilAction::BucketRead)?;

        Ok(Response::new(HostAliasResponse {
            request_id: req.request_id,
            host_alias: Some(host_alias_descriptor_to_proto(host_alias)),
            audit_event_id: String::new(),
        }))
    }

    async fn list_host_aliases(
        &self,
        request: Request<ListHostAliasesRequest>,
    ) -> Result<Response<ListHostAliasesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let tenant_id = claims.tenant_id.to_string();
        let mut host_aliases = self
            .persistence
            .list_host_alias_descriptors(none_if_empty(&req.region))
            .await
            .map_err(lifecycle_status)?
            .into_iter()
            .filter(|alias| alias.tenant_id == tenant_id)
            .filter(|alias| {
                auth::is_authorized(AnvilAction::BucketRead, &alias.bucket_name, &claims.scopes)
            })
            .collect::<Vec<_>>();
        host_aliases.sort_by(|left, right| left.hostname.cmp(&right.hostname));
        let limit = page_limit(req.page.as_ref());
        let has_more = host_aliases.len() > limit;
        if has_more {
            host_aliases.truncate(limit);
        }

        Ok(Response::new(ListHostAliasesResponse {
            page: Some(PageResponse {
                next_cursor: String::new(),
                has_more,
            }),
            host_aliases: host_aliases
                .into_iter()
                .map(host_alias_descriptor_to_proto)
                .collect(),
        }))
    }
}

fn public_link_context(
    context: Option<&PublicMutationContext>,
    create: bool,
) -> Result<&PublicMutationContext, Status> {
    let context = context.ok_or_else(|| Status::invalid_argument("Missing mutation context"))?;
    if context.request_id.trim().is_empty() {
        return Err(Status::invalid_argument("request_id is required"));
    }
    if context.idempotency_key.trim().is_empty() {
        return Err(Status::invalid_argument("idempotency_key is required"));
    }
    if !create && context.expected_generation == 0 {
        return Err(Status::invalid_argument("expected_generation is required"));
    }
    Ok(context)
}

fn validate_public_tenant_locator(claims: &auth::Claims, tenant_id: &str) -> Result<(), Status> {
    let tenant_id = tenant_id.trim();
    if tenant_id.is_empty() || tenant_id == claims.tenant_id.to_string() {
        return Ok(());
    }
    Err(Status::permission_denied(
        "Request tenant_id does not match authenticated tenant",
    ))
}

async fn public_link_bucket(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
) -> Result<crate::persistence::Bucket, Status> {
    if bucket_name.trim().is_empty() {
        return Err(Status::invalid_argument("bucket_name is required"));
    }
    state
        .persistence
        .get_bucket_by_name(claims.tenant_id, bucket_name)
        .await
        .map_err(|err| Status::internal(err.to_string()))?
        .ok_or_else(|| Status::not_found("Bucket not found"))
}

fn require_object_link_scope(
    claims: &auth::Claims,
    bucket_name: &str,
    link_key: &str,
    action: AnvilAction,
) -> Result<(), Status> {
    if crate::validation::is_reserved_internal_key(link_key) {
        return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
    }
    if !auth::is_authorized(action, &format!("{bucket_name}/{link_key}"), &claims.scopes) {
        return Err(Status::permission_denied("Permission denied"));
    }
    Ok(())
}

async fn public_host_alias_bucket(
    state: &AppState,
    claims: &auth::Claims,
    bucket_name: &str,
) -> Result<crate::persistence::Bucket, Status> {
    public_link_bucket(state, claims, bucket_name).await
}

async fn public_host_alias_descriptor(
    state: &AppState,
    claims: &auth::Claims,
    hostname: &str,
) -> Result<CoreHostAliasDescriptor, Status> {
    let descriptor = state
        .persistence
        .get_host_alias_descriptor(hostname)
        .await
        .map_err(lifecycle_status)?
        .ok_or_else(|| Status::not_found("Host alias not found"))?;
    if descriptor.tenant_id != claims.tenant_id.to_string() {
        return Err(Status::not_found("Host alias not found"));
    }
    Ok(descriptor)
}

fn require_bucket_scope(
    claims: &auth::Claims,
    bucket_name: &str,
    action: AnvilAction,
) -> Result<(), Status> {
    if !auth::is_authorized(action, bucket_name, &claims.scopes) {
        return Err(Status::permission_denied("Permission denied"));
    }
    Ok(())
}

async fn public_routing_config_for_region(
    state: &AppState,
    region_name: &str,
) -> Result<RoutingConfig, Status> {
    let region_name = region_name.trim();
    if region_name.is_empty() {
        return Err(Status::invalid_argument("region is required"));
    }
    let region = state
        .persistence
        .list_region_descriptors()
        .await
        .map_err(lifecycle_status)?
        .into_iter()
        .find(|region| region.region == region_name)
        .ok_or_else(|| Status::not_found("Region not found"))?;
    let base_domain =
        public_base_domain_from_region_suffix(&region.region, &region.virtual_host_suffix)?;
    RoutingConfig::new(base_domain).map_err(|err| Status::invalid_argument(err.to_string()))
}

fn public_base_domain_from_region_suffix(
    region: &str,
    virtual_host_suffix: &str,
) -> Result<String, Status> {
    let suffix = routing::normalize_alias_hostname(virtual_host_suffix)
        .map_err(|err| Status::invalid_argument(err.to_string()))?;
    let region_prefix = format!(
        "{}.",
        region.trim().trim_end_matches('.').to_ascii_lowercase()
    );
    Ok(suffix
        .strip_prefix(&region_prefix)
        .unwrap_or(&suffix)
        .to_string())
}

fn parse_optional_uuid(
    field_name: &'static str,
    value: String,
) -> Result<Option<uuid::Uuid>, Status> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<uuid::Uuid>()
        .map(Some)
        .map_err(|_| Status::invalid_argument(format!("Invalid {field_name}")))
}

fn page_limit(page: Option<&PageRequest>) -> usize {
    let requested = page.map(|page| page.limit).unwrap_or(100);
    if requested == 0 {
        100
    } else {
        requested.clamp(1, 1000) as usize
    }
}

fn object_link_status(err: object_links::ObjectLinkError) -> Status {
    match err {
        object_links::ObjectLinkError::InvalidLinkKey
        | object_links::ObjectLinkError::InvalidTargetKey
        | object_links::ObjectLinkError::MissingExpectedGeneration => {
            Status::invalid_argument(err.to_string())
        }
        object_links::ObjectLinkError::AlreadyExists => Status::already_exists(err.to_string()),
        object_links::ObjectLinkError::BucketNotFound | object_links::ObjectLinkError::NotFound => {
            Status::not_found(err.to_string())
        }
        object_links::ObjectLinkError::BucketTenantMismatch => {
            Status::not_found("Bucket not found")
        }
        object_links::ObjectLinkError::GenerationConflict { .. } => {
            Status::aborted(err.to_string())
        }
        object_links::ObjectLinkError::ExistingObjectIsNotLink
        | object_links::ObjectLinkError::DanglingObjectLink
        | object_links::ObjectLinkError::TargetNotBlob
        | object_links::ObjectLinkError::LinkLoop
        | object_links::ObjectLinkError::LinkDepthExceeded => {
            Status::failed_precondition(err.to_string())
        }
        object_links::ObjectLinkError::Internal(_) => Status::internal(err.to_string()),
    }
}

fn object_link_resolution_from_proto(
    value: i32,
) -> Result<object_links::ObjectLinkResolution, Status> {
    match value {
        1 => Ok(object_links::ObjectLinkResolution::Follow),
        2 => Ok(object_links::ObjectLinkResolution::Redirect),
        _ => Err(Status::invalid_argument("Invalid object link resolution")),
    }
}

fn object_link_resolution_to_proto(value: object_links::ObjectLinkResolution) -> i32 {
    match value {
        object_links::ObjectLinkResolution::Follow => 1,
        object_links::ObjectLinkResolution::Redirect => 2,
    }
}

fn object_link_descriptor_to_proto(
    value: object_links::ObjectLinkDescriptor,
) -> crate::anvil_api::ObjectLinkDescriptor {
    crate::anvil_api::ObjectLinkDescriptor {
        schema: value.schema,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        link_key: value.link_key,
        target_key: value.target_key,
        target_version: value.target_version.unwrap_or_default(),
        resolution: object_link_resolution_to_proto(value.resolution),
        created_at: value
            .created_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        updated_at: value
            .updated_at
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        created_by: value.created_by,
        generation: value.generation,
    }
}

fn lifecycle_status(err: LifecycleError) -> Status {
    match err {
        LifecycleError::InvalidArgument(message) => Status::invalid_argument(message),
        LifecycleError::AlreadyExists { .. } => Status::already_exists(err.to_string()),
        LifecycleError::NotFound { .. } => Status::not_found(err.to_string()),
        LifecycleError::GenerationConflict { .. } => Status::aborted(err.to_string()),
        LifecycleError::LifecycleTransitionDenied { .. }
        | LifecycleError::ActivationCheckpointNotReached { .. } => {
            Status::failed_precondition(err.to_string())
        }
        LifecycleError::Io(_) | LifecycleError::Json(_) | LifecycleError::Other(_) => {
            Status::internal(err.to_string())
        }
    }
}

fn host_alias_state_to_proto(value: CoreHostAliasState) -> i32 {
    match value {
        CoreHostAliasState::PendingVerification => 1,
        CoreHostAliasState::Active => 2,
        CoreHostAliasState::Suspended => 3,
        CoreHostAliasState::Deleted => 4,
    }
}

fn host_alias_descriptor_to_proto(
    value: CoreHostAliasDescriptor,
) -> crate::anvil_api::HostAliasDescriptor {
    let verification_challenge = host_alias_verification_challenge(&value);
    crate::anvil_api::HostAliasDescriptor {
        schema: value.schema,
        hostname: value.hostname,
        tenant_id: value.tenant_id,
        bucket_name: value.bucket_name,
        region: value.region,
        prefix: value.prefix,
        state: host_alias_state_to_proto(value.state),
        created_at: value.created_at,
        updated_at: value.updated_at,
        generation: value.generation,
        verification_challenge,
    }
}

fn host_alias_verification_challenge(value: &CoreHostAliasDescriptor) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(value.hostname.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.tenant_id.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.bucket_name.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.region.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.prefix.as_bytes());
    format!("anvil-host-alias={}", hasher.finalize().to_hex())
}

fn none_if_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() { None } else { Some(value) }
}

struct NativeMutationAttempt<'a> {
    context: &'a NativeMutationContext,
    _idempotency_guard: OwnedMutexGuard<()>,
    _target_guard: OwnedMutexGuard<()>,
}

async fn begin_native_mutation<'a, T>(
    state: &AppState,
    context: Option<&'a NativeMutationContext>,
    target: &NativeIdempotencyTarget,
    scopes: &[String],
    action: AnvilAction,
) -> Result<(NativeMutationAttempt<'a>, Option<T>), Status>
where
    T: DeserializeOwned,
{
    let context =
        context.ok_or_else(|| Status::invalid_argument("Missing native mutation context"))?;
    validate_native_mutation_target_authorization(target, scopes, action)?;
    let idempotency_guard = acquire_native_mutation_lock(state, context).await?;
    let target_guard = acquire_native_target_lock(state, context, target).await?;
    let replay = native_idempotency::load_response(&state.storage, context, target).await?;
    Ok((
        NativeMutationAttempt {
            context,
            _idempotency_guard: idempotency_guard,
            _target_guard: target_guard,
        },
        replay,
    ))
}

fn validate_native_mutation_target_authorization(
    target: &NativeIdempotencyTarget,
    scopes: &[String],
    action: AnvilAction,
) -> Result<(), Status> {
    if !crate::validation::is_valid_bucket_name(&target.bucket_name) {
        return Err(Status::invalid_argument("Invalid bucket name"));
    }
    if crate::validation::is_reserved_internal_key(&target.object_key) {
        return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
    }
    if !crate::validation::is_valid_object_key(&target.object_key) {
        return Err(Status::invalid_argument("Invalid object key"));
    }
    if !auth::is_authorized(
        action,
        &format!("{}/{}", target.bucket_name, target.object_key),
        scopes,
    ) {
        return Err(Status::permission_denied("Permission denied"));
    }
    Ok(())
}

async fn complete_native_mutation<T>(
    state: &AppState,
    attempt: &NativeMutationAttempt<'_>,
    target: &NativeIdempotencyTarget,
    response: &T,
) -> Result<(), Status>
where
    T: Serialize,
{
    native_idempotency::store_response(&state.storage, attempt.context, target, response).await
}

async fn acquire_native_mutation_lock(
    state: &AppState,
    context: &NativeMutationContext,
) -> Result<OwnedMutexGuard<()>, Status> {
    acquire_native_lock_key(state, native_mutation_lock_key(context)).await
}

async fn acquire_native_target_lock(
    state: &AppState,
    context: &NativeMutationContext,
    target: &NativeIdempotencyTarget,
) -> Result<OwnedMutexGuard<()>, Status> {
    acquire_native_lock_key(
        state,
        native_target_lock_key(context.tenant_id, &target.bucket_name, &target.object_key),
    )
    .await
}

async fn acquire_native_lock_key(
    state: &AppState,
    lock_key: String,
) -> Result<OwnedMutexGuard<()>, Status> {
    let lock = {
        let mut locks = state.native_mutation_locks.lock().await;
        locks
            .entry(lock_key)
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    };
    Ok(lock.lock_owned().await)
}

fn native_mutation_lock_key(context: &NativeMutationContext) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&context.tenant_id.to_le_bytes());
    hasher.update(&context.bucket_id.to_le_bytes());
    hasher.update(context.principal.as_bytes());
    hasher.update(&[0]);
    hasher.update(context.idempotency_key.as_bytes());
    hasher.finalize().to_hex().to_string()
}

fn native_target_lock_key(tenant_id: i64, bucket_name: &str, object_key: &str) -> String {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"native-target");
    hasher.update(&tenant_id.to_le_bytes());
    hasher.update(bucket_name.as_bytes());
    hasher.update(&[0]);
    hasher.update(object_key.as_bytes());
    hasher.finalize().to_hex().to_string()
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

fn parse_user_metadata_json(value: &str) -> Result<Option<serde_json::Value>, Status> {
    if value.trim().is_empty() {
        return Ok(None);
    }
    let parsed: serde_json::Value = serde_json::from_str(value)
        .map_err(|e| Status::invalid_argument(format!("Invalid user_metadata_json: {e}")))?;
    if !parsed.is_object() {
        return Err(Status::invalid_argument(
            "user_metadata_json must be a JSON object",
        ));
    }
    Ok(Some(parsed))
}

fn json_object_string(value: Option<&serde_json::Value>) -> String {
    value
        .map(|value| value.to_string())
        .unwrap_or_else(|| "{}".to_string())
}

fn append_stream_record_info(
    record: crate::object_manager::AppendStreamRecordRead,
) -> AppendStreamRecordInfo {
    AppendStreamRecordInfo {
        record_sequence: record.record_sequence,
        payload_hash: record.payload_hash,
        payload_size: record.payload_size,
        created_at: record.created_at.to_rfc3339(),
        content_type: record.content_type.unwrap_or_default(),
        user_metadata_json: json_object_string(record.user_metadata.as_ref()),
        payload: record.payload.unwrap_or_default(),
    }
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

async fn enforce_write_precondition(
    state: &AppState,
    claims: &auth::Claims,
    precondition: Option<&WritePrecondition>,
) -> Result<(), Status> {
    let Some(precondition) = precondition else {
        return Ok(());
    };
    for object_precondition in &precondition.object_versions {
        if object_precondition.bucket_name.trim().is_empty()
            || object_precondition.object_key.trim().is_empty()
        {
            return Err(Status::invalid_argument(
                "ObjectVersionPrecondition requires bucket_name and object_key",
            ));
        }
        let expected_version_id =
            parse_optional_version_id(object_precondition.expected_version_id.as_deref())?;
        let head = state
            .object_manager
            .head_object(
                Some(claims.clone()),
                &object_precondition.bucket_name,
                &object_precondition.object_key,
                None,
            )
            .await;
        match (
            object_precondition.must_not_exist,
            expected_version_id,
            head,
        ) {
            (true, _, Ok(_)) => {
                return Err(Status::failed_precondition(
                    "ObjectVersionPreconditionFailed",
                ));
            }
            (true, _, Err(status)) if status.code() == tonic::Code::NotFound => {}
            (true, _, Err(status)) => return Err(status),
            (false, Some(expected), Ok(object)) if object.version_id == expected => {}
            (false, Some(_), Ok(_)) => {
                return Err(Status::failed_precondition(
                    "ObjectVersionPreconditionFailed",
                ));
            }
            (false, Some(_), Err(status)) if status.code() == tonic::Code::NotFound => {
                return Err(Status::failed_precondition(
                    "ObjectVersionPreconditionFailed",
                ));
            }
            (false, Some(_), Err(status)) => return Err(status),
            (false, None, _) => {
                return Err(Status::invalid_argument(
                    "ObjectVersionPrecondition requires expected_version_id or must_not_exist",
                ));
            }
        }
    }

    if let Some(lease_fence) = precondition.lease_fence.as_ref() {
        validate_task_lease_id(&lease_fence.task_id)?;
        let lease = state
            .persistence
            .read_named_task_lease(claims.tenant_id, &lease_fence.task_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition(task_lease::LEASE_EXPIRED))?;
        let owner = lease_owner_from_claims(claims);
        if !lease.owner.same_security_owner(&owner) {
            return Err(Status::permission_denied(task_lease::LEASE_OWNER_MISMATCH));
        }
        if lease.fence_token != lease_fence.fence_token {
            return Err(Status::failed_precondition(task_lease::STALE_FENCE));
        }
        if lease.expires_at_nanos <= current_time_nanos()? {
            return Err(Status::failed_precondition(task_lease::LEASE_EXPIRED));
        }
    }

    Ok(())
}

fn validate_mutation_batch_operations(req: &MutationBatchRequest) -> Result<(), Status> {
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            return Err(Status::invalid_argument(
                "MutationBatch operation is missing op",
            ));
        };
        match op {
            mutation_batch_operation::Op::PutObject(op) if op.object_key.trim().is_empty() => {
                return Err(Status::invalid_argument(
                    "put_object.object_key is required",
                ));
            }
            mutation_batch_operation::Op::PatchJsonObject(op)
                if op.object_key.trim().is_empty() =>
            {
                return Err(Status::invalid_argument(
                    "patch_json_object.object_key is required",
                ));
            }
            mutation_batch_operation::Op::DeleteObject(op) if op.object_key.trim().is_empty() => {
                return Err(Status::invalid_argument(
                    "delete_object.object_key is required",
                ));
            }
            mutation_batch_operation::Op::AppendStreamRecord(op)
                if op.stream_key.trim().is_empty() || op.stream_id.trim().is_empty() =>
            {
                return Err(Status::invalid_argument(
                    "append_stream_record stream_key and stream_id are required",
                ));
            }
            mutation_batch_operation::Op::CheckpointTaskLease(op)
                if op.task_id.trim().is_empty() || op.fence_token == 0 =>
            {
                return Err(Status::invalid_argument(
                    "task lease batch operation requires task_id and fence_token",
                ));
            }
            mutation_batch_operation::Op::CommitTaskLease(op)
                if op.task_id.trim().is_empty() || op.fence_token == 0 =>
            {
                return Err(Status::invalid_argument(
                    "task lease batch operation requires task_id and fence_token",
                ));
            }
            mutation_batch_operation::Op::CompareAndSwapManifest(op)
                if op.manifest_key.trim().is_empty() =>
            {
                return Err(Status::invalid_argument(
                    "compare_and_swap_manifest.manifest_key is required",
                ));
            }
            _ => {}
        }
    }
    Ok(())
}

fn validate_mutation_batch_authorization(
    claims: &auth::Claims,
    req: &MutationBatchRequest,
) -> Result<(), Status> {
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            continue;
        };
        match op {
            mutation_batch_operation::Op::CheckpointTaskLease(op) => {
                if !auth::is_authorized(
                    AnvilAction::CoordinationLeaseWrite,
                    &task_lease_resource(&op.task_id),
                    &claims.scopes,
                ) {
                    return Err(Status::permission_denied("Permission denied"));
                }
            }
            mutation_batch_operation::Op::CommitTaskLease(op) => {
                if !auth::is_authorized(
                    AnvilAction::CoordinationLeaseWrite,
                    &task_lease_resource(&op.task_id),
                    &claims.scopes,
                ) {
                    return Err(Status::permission_denied("Permission denied"));
                }
            }
            _ => {}
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct MutationBatchDigestInput<'a> {
    precondition: Option<&'a WritePrecondition>,
    operations: &'a [MutationBatchOperation],
}

fn mutation_batch_digest(req: &MutationBatchRequest) -> Result<String, Status> {
    let input = MutationBatchDigestInput {
        precondition: req.precondition.as_ref(),
        operations: &req.operations,
    };
    Ok(blake3::hash(
        &serde_json::to_vec(&input)
            .map_err(|e| Status::internal(format!("Serialize mutation batch: {e}")))?,
    )
    .to_hex()
    .to_string())
}

fn task_lease_resource(task_id: &str) -> String {
    format!("task_lease/{task_id}")
}

async fn acquire_mutation_batch_operation_locks(
    state: &AppState,
    tenant_id: i64,
    req: &MutationBatchRequest,
) -> Result<Vec<OwnedMutexGuard<()>>, Status> {
    let mut keys = Vec::new();
    for operation in &req.operations {
        let Some(op) = operation.op.as_ref() else {
            continue;
        };
        let key = match op {
            mutation_batch_operation::Op::PutObject(op) => op.object_key.as_str(),
            mutation_batch_operation::Op::PatchJsonObject(op) => op.object_key.as_str(),
            mutation_batch_operation::Op::DeleteObject(op) => op.object_key.as_str(),
            mutation_batch_operation::Op::AppendStreamRecord(op) => op.stream_key.as_str(),
            mutation_batch_operation::Op::CompareAndSwapManifest(op) => op.manifest_key.as_str(),
            mutation_batch_operation::Op::CheckpointTaskLease(op) => {
                keys.push(native_target_lock_key(
                    tenant_id,
                    &req.bucket_name,
                    &format!("_task_lease/{}", op.task_id),
                ));
                continue;
            }
            mutation_batch_operation::Op::CommitTaskLease(op) => {
                keys.push(native_target_lock_key(
                    tenant_id,
                    &req.bucket_name,
                    &format!("_task_lease/{}", op.task_id),
                ));
                continue;
            }
        };
        keys.push(native_target_lock_key(tenant_id, &req.bucket_name, key));
    }
    keys.sort();
    keys.dedup();

    let mut guards = Vec::with_capacity(keys.len());
    for key in keys {
        guards.push(acquire_native_lock_key(state, key).await?);
    }
    Ok(guards)
}

fn lease_owner_from_claims(claims: &auth::Claims) -> task_lease::TaskLeaseOwner {
    let actor_instance_id = claims.jti.clone().unwrap_or_else(|| claims.sub.clone());
    task_lease::TaskLeaseOwner {
        tenant_id: claims.tenant_id,
        principal_kind: "app".to_string(),
        principal_id: claims.sub.clone(),
        actor_instance_id,
        display_name: claims.sub.clone(),
    }
}

fn current_time_nanos() -> Result<i64, Status> {
    chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| Status::internal("current time exceeds supported range"))
}

fn join_u128(low: u64, high: u64) -> u128 {
    ((high as u128) << 64) | low as u128
}

fn lease_error_status(error: anyhow::Error) -> Status {
    let message = error.to_string();
    if message.contains(task_lease::LEASE_HELD) {
        Status::failed_precondition(task_lease::LEASE_HELD)
    } else if message.contains(task_lease::LEASE_EXPIRED) {
        Status::failed_precondition(task_lease::LEASE_EXPIRED)
    } else if message.contains(task_lease::STALE_FENCE) {
        Status::failed_precondition(task_lease::STALE_FENCE)
    } else if message.contains(task_lease::LEASE_OWNER_MISMATCH) {
        Status::permission_denied(task_lease::LEASE_OWNER_MISMATCH)
    } else if message.contains(task_lease::LEASE_CAS_CONFLICT) {
        Status::aborted(task_lease::LEASE_CAS_CONFLICT)
    } else {
        Status::failed_precondition(message)
    }
}

fn validate_task_lease_id(value: &str) -> Result<(), Status> {
    if value.trim().is_empty()
        || value.len() > 256
        || value.chars().any(|ch| ch.is_control())
        || value.contains("..")
        || value.starts_with('/')
    {
        return Err(Status::invalid_argument("Invalid task_id"));
    }
    Ok(())
}

fn watch_event_response(
    event: &crate::persistence::ObjectWatchEvent,
) -> Option<WatchPrefixResponse> {
    let cursor = u64::try_from(event.id).ok()?;
    let created_at = event.created_at.to_string();
    Some(WatchPrefixResponse {
        cursor,
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
        created_at: created_at.clone(),
        envelope: Some(watch_envelope::envelope(WatchEnvelopeParts {
            watch_stream_id: "object_prefix",
            partition_family: "object_metadata",
            partition_id: event.bucket_id.to_string(),
            cursor: event.id as u128,
            mutation_id: event.mutation_id.to_string(),
            record_kind: event.event_type.clone(),
            object_ref: format!("{}/{}", event.bucket_name, event.key),
            authz_revision: 0,
            index_generation: 0,
            personaldb_log_index: 0,
            payload_hash: event.payload_hash.clone(),
            emitted_at: created_at,
        })),
    })
}
