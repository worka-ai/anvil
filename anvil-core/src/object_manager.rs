use crate::{
    access_control, auth, bucket_journal,
    core_store::{CoreObjectRef, CoreStore, GetBlob, PutBlob},
    metadata_journal, object_links,
    observability::{
        OBJECT_READ_LATENCY, OBJECT_WRITE_LATENCY, Observability, PREFIX_LIST_LATENCY,
        RESERVED_NAMESPACE_REJECTION_COUNT,
    },
    permissions::AnvilAction,
    persistence::{
        Bucket, MetadataMutationReceipt, Object, ObjectVersion, ObjectVersionsPage,
        ObjectWatchEvent, Persistence,
    },
    routing::{self, CrossRegionRoutingPolicy},
    storage::Storage,
    validation, watch_log,
};
use futures_util::{Stream, StreamExt};
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tracing::info;

#[derive(Debug, Clone)]
pub struct ObjectManager {
    persistence: Persistence,
    storage: Storage,
    core_store: CoreStore,
    region: String,
    cross_region_routing_policy: CrossRegionRoutingPolicy,
    signing_key: Vec<u8>,
    watch_tx: broadcast::Sender<ObjectWatchEvent>,
    observability: Observability,
}

#[derive(Debug, Clone)]
pub struct ComposeSource {
    pub bucket_name: String,
    pub object_key: String,
    pub version_id: Option<uuid::Uuid>,
}

#[derive(Debug, Clone)]
pub struct CompleteMultipartPart {
    pub part_number: i32,
    pub etag: String,
}

#[derive(Debug, Clone)]
pub struct InitiateMultipartUploadResult {
    pub upload_id: uuid::Uuid,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct UploadPartResult {
    pub etag: String,
    pub payload_hash: String,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct AbortMultipartUploadResult {
    pub upload_id: uuid::Uuid,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone, Default)]
pub struct ObjectWriteOptions {
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectLinkReadMode {
    Follow,
    Metadata,
}

pub struct ObjectReadResult {
    pub object: Object,
    pub stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
    pub followed_link: Option<object_links::FollowedObjectLink>,
}

#[derive(Debug, Clone)]
pub struct AppendStreamRecordRead {
    pub record_sequence: u64,
    pub payload_hash: String,
    pub payload_size: i64,
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub payload: Option<Vec<u8>>,
}

struct ComposeStreamState {
    manager: ObjectManager,
    claims: auth::Claims,
    sources: std::vec::IntoIter<ComposeSource>,
    current: Option<Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>>,
}

#[derive(Debug, Clone)]
pub struct ObjectHeadResult {
    pub object: Object,
    pub followed_link: Option<object_links::FollowedObjectLink>,
}

#[derive(Debug, Clone)]
pub struct AppendStreamRecordResult {
    pub record_sequence: u64,
    pub payload_hash: String,
    pub payload_size: i64,
    pub content_type: Option<String>,
    pub user_metadata: Option<JsonValue>,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct CreateAppendStreamResult {
    pub stream_id: uuid::Uuid,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct SealAppendStreamResult {
    pub record_count: u64,
    pub segment_hash: String,
    pub receipt: MetadataMutationReceipt,
}

#[derive(Debug, Clone)]
pub struct ManifestCasResult {
    pub revision: u64,
    pub manifest_hash: String,
    pub receipt: MetadataMutationReceipt,
}

impl ObjectManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        persistence: Persistence,
        storage: Storage,
        core_store: CoreStore,
        region: String,
        cross_region_routing_policy: CrossRegionRoutingPolicy,
        signing_key: Vec<u8>,
        watch_tx: broadcast::Sender<ObjectWatchEvent>,
        observability: Observability,
    ) -> Self {
        Self {
            persistence,
            storage,
            core_store,
            region,
            cross_region_routing_policy,
            signing_key,
            watch_tx,
            observability,
        }
    }

    fn record_reserved_namespace_rejection(&self, operation: &'static str) {
        self.observability.increment_counter(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("api", "native"), ("operation", operation)],
        );
    }

    pub async fn put_object(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
        data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin,
        options: ObjectWriteOptions,
    ) -> Result<Object, Status> {
        let _latency = self
            .observability
            .latency_guard(OBJECT_WRITE_LATENCY, &[("api", "native")]);
        info!(
            tenant_id,
            bucket_name,
            object_key,
            ?scopes,
            "put_object called"
        );
        let total_start = std::time::Instant::now();

        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            self.record_reserved_namespace_rejection("put_object");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let authorized = auth::is_authorized(
            AnvilAction::ObjectWrite,
            &format!("{}/{}", bucket_name, object_key),
            scopes,
        );

        if !authorized {
            return Err(Status::permission_denied("Permission denied"));
        }
        let step_start = std::time::Instant::now();
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        crate::emit_test_timing(
            "object_manager.put_object get_tenant_bucket",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let (temp_path, total_bytes, _legacy_hash) = self
            .storage
            .stream_to_temp_file(data_stream)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "object_manager.put_object stream_to_temp_file",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let io_start = Instant::now();
        let payload = tokio::fs::read(&temp_path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::perf::record_io_duration(
            "object_manager",
            "read_temp_payload",
            &temp_path,
            payload.len() as u64,
            io_start.elapsed(),
        );
        let io_start = Instant::now();
        tokio::fs::remove_file(&temp_path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::perf::record_io_duration(
            "object_manager",
            "remove_temp_payload",
            &temp_path,
            total_bytes as u64,
            io_start.elapsed(),
        );
        crate::emit_test_timing(
            "object_manager.put_object read_and_remove_temp_file",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let object_ref = self
            .core_store
            .put_blob(PutBlob {
                logical_name: format!(
                    "tenant:{tenant_id}/bucket:{}/object:{}",
                    bucket.name, object_key
                ),
                bytes: payload,
                region_id: self.region.clone(),
                mutation_id: uuid::Uuid::new_v4().to_string(),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "object_manager.put_object core_store_put_blob",
            step_start.elapsed(),
        );
        let content_hash = object_ref.hash.clone();
        let shard_map_json = Some(core_object_ref_to_shard_map(&object_ref));

        let step_start = std::time::Instant::now();
        let object = self
            .persistence
            .create_object(
                tenant_id,
                bucket.id,
                object_key,
                &content_hash,
                total_bytes as i64,
                &content_hash,
                options.content_type.as_deref(),
                options.user_metadata,
                shard_map_json,
                None,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "object_manager.put_object persistence_create_object",
            step_start.elapsed(),
        );

        let step_start = std::time::Instant::now();
        self.publish_object_watch_event(tenant_id, &bucket, &object, "put", false)
            .await?;
        crate::emit_test_timing(
            "object_manager.put_object publish_object_watch_event",
            step_start.elapsed(),
        );
        crate::emit_test_timing("object_manager.put_object total", total_start.elapsed());

        Ok(object)
    }

    pub async fn initiate_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<InitiateMultipartUploadResult, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;

        let mutation = self
            .persistence
            .create_multipart_upload(tenant_id, bucket.id, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(InitiateMultipartUploadResult {
            upload_id: mutation.upload.upload_id,
            receipt: mutation.receipt,
        })
    }

    pub async fn upload_part(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        part_number: i32,
        scopes: &[String],
        data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin,
    ) -> Result<UploadPartResult, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        validate_multipart_part_number(part_number)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = self
            .persistence
            .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;

        let (temp_path, bytes, _legacy_content_hash) = self
            .storage
            .stream_to_temp_file(data_stream)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let io_start = Instant::now();
        let payload = tokio::fs::read(&temp_path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::perf::record_io_duration(
            "object_manager",
            "read_temp_multipart_part",
            &temp_path,
            payload.len() as u64,
            io_start.elapsed(),
        );
        let io_start = Instant::now();
        tokio::fs::remove_file(&temp_path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        crate::perf::record_io_duration(
            "object_manager",
            "remove_temp_multipart_part",
            &temp_path,
            bytes as u64,
            io_start.elapsed(),
        );
        let object_ref = self
            .core_store
            .put_blob(PutBlob {
                logical_name: format!(
                    "tenant:{tenant_id}/bucket:{}/multipart:{upload_id}/part:{part_number}",
                    bucket.name
                ),
                bytes: payload,
                region_id: self.region.clone(),
                mutation_id: uuid::Uuid::new_v4().to_string(),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let content_hash = object_ref.hash.clone();

        let mutation = self
            .persistence
            .upsert_multipart_part(
                upload.id,
                part_number,
                object_ref,
                bytes as i64,
                &content_hash,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(UploadPartResult {
            etag: mutation.part.etag,
            payload_hash: content_hash,
            receipt: mutation.receipt,
        })
    }

    pub async fn complete_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        parts: Vec<CompleteMultipartPart>,
        scopes: &[String],
    ) -> Result<Object, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        if parts.is_empty() {
            return Err(Status::invalid_argument(
                "CompleteMultipartUpload requires at least one part",
            ));
        }
        for part in &parts {
            validate_multipart_part_number(part.part_number)?;
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = self
            .persistence
            .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        let stored_parts = self
            .persistence
            .list_multipart_parts(upload.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut ordered_part_refs = Vec::with_capacity(parts.len());
        for expected in parts {
            let stored = stored_parts
                .iter()
                .find(|part| part.part_number == expected.part_number)
                .ok_or_else(|| {
                    Status::invalid_argument("Complete request references missing part")
                })?;
            if trim_s3_etag(&stored.etag) != trim_s3_etag(&expected.etag) {
                return Err(Status::invalid_argument(
                    "Complete request part ETag mismatch",
                ));
            }
            ordered_part_refs.push(stored.object_ref.clone());
        }

        let core_store = self.core_store.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for object_ref in ordered_part_refs {
                let bytes = match core_store.get_blob(GetBlob { object_ref }).await {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                        return;
                    }
                };
                for chunk in bytes.chunks(1024 * 64) {
                    if tx.send(Ok(chunk.to_vec())).await.is_err() {
                        return;
                    }
                }
            }
        });

        let object = self
            .put_object(
                tenant_id,
                bucket_name,
                object_key,
                scopes,
                ReceiverStream::new(rx),
                ObjectWriteOptions::default(),
            )
            .await?;

        let completion = self
            .persistence
            .complete_multipart_upload(upload.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !completion.completed {
            return Err(Status::not_found("Multipart upload not found"));
        }

        Ok(object)
    }

    pub async fn abort_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        scopes: &[String],
    ) -> Result<AbortMultipartUploadResult, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let mutation = self
            .persistence
            .abort_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if let Some(receipt) = mutation.receipt {
            Ok(AbortMultipartUploadResult { upload_id, receipt })
        } else {
            Err(Status::not_found("Multipart upload not found"))
        }
    }

    pub async fn list_multipart_parts(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        part_number_marker: i32,
        limit: i32,
        scopes: &[String],
    ) -> Result<crate::persistence::MultipartPartsPage, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = self
            .persistence
            .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        self.persistence
            .list_multipart_parts_page(upload.id, part_number_marker, limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn list_multipart_uploads(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        upload_id_marker: Option<uuid::Uuid>,
        limit: i32,
        scopes: &[String],
    ) -> Result<crate::persistence::MultipartUploadsPage, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }
        if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        self.persistence
            .list_active_multipart_uploads(bucket.id, prefix, key_marker, upload_id_marker, limit)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn watch_prefix_snapshot(
        &self,
        claims: auth::Claims,
        bucket_name: &str,
        prefix: &str,
        after_cursor: u64,
    ) -> Result<
        (
            i64,
            Vec<ObjectWatchEvent>,
            broadcast::Receiver<ObjectWatchEvent>,
        ),
        Status,
    > {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }
        if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        let live = self.watch_tx.subscribe();
        let after_cursor = i64::try_from(after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let snapshot = watch_log::list_object_watch_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            prefix,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        Ok((bucket.id, snapshot, live))
    }

    pub async fn create_append_stream(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        stream_key: &str,
        scopes: &[String],
    ) -> Result<CreateAppendStreamResult, Status> {
        self.validate_write_request(bucket_name, stream_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let mutation = self
            .persistence
            .create_append_stream(tenant_id, bucket.id, &bucket.name, stream_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(CreateAppendStreamResult {
            stream_id: mutation.stream.stream_id,
            receipt: mutation.receipt,
        })
    }

    pub async fn append_stream_record(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        stream_key: &str,
        stream_id: uuid::Uuid,
        payload: Vec<u8>,
        content_type: Option<String>,
        user_metadata: Option<JsonValue>,
        scopes: &[String],
    ) -> Result<AppendStreamRecordResult, Status> {
        self.validate_write_request(bucket_name, stream_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let stream = self
            .persistence
            .get_active_append_stream(tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;

        let payload_size = payload.len() as i64;
        let object_ref = self
            .core_store
            .put_blob(PutBlob {
                logical_name: format!(
                    "tenant:{tenant_id}/bucket:{}/append:{stream_key}/record:{}",
                    bucket.name,
                    uuid::Uuid::new_v4()
                ),
                bytes: payload,
                region_id: self.region.clone(),
                mutation_id: uuid::Uuid::new_v4().to_string(),
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let payload_hash = object_ref.hash.clone();
        let mutation = self
            .persistence
            .append_stream_record(
                stream.id,
                object_ref,
                payload_size,
                content_type.clone(),
                user_metadata.clone(),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(AppendStreamRecordResult {
            record_sequence: u64::try_from(mutation.record.record_sequence)
                .map_err(|_| Status::internal("Invalid record sequence"))?,
            payload_hash,
            payload_size,
            content_type,
            user_metadata,
            receipt: mutation.receipt,
        })
    }

    pub async fn seal_append_stream_segment(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        stream_key: &str,
        stream_id: uuid::Uuid,
        scopes: &[String],
    ) -> Result<SealAppendStreamResult, Status> {
        self.validate_write_request(bucket_name, stream_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let stream = self
            .persistence
            .get_active_append_stream(tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let records = self
            .persistence
            .list_append_stream_records(stream.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if records.is_empty() {
            return Err(Status::failed_precondition(
                "Append stream has no records to seal",
            ));
        }

        let mut hasher = blake3::Hasher::new();
        for record in &records {
            hasher.update(&record.record_sequence.to_le_bytes());
            hasher.update(record.payload_hash.as_bytes());
            hasher.update(&record.payload_size.to_le_bytes());
        }
        let segment_hash = hasher.finalize().to_hex().to_string();
        let sealed = self
            .persistence
            .seal_append_stream(stream.id, &segment_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let Some(receipt) = sealed.receipt else {
            return Err(Status::failed_precondition(
                "Append stream is already sealed",
            ));
        };

        Ok(SealAppendStreamResult {
            record_count: records.len() as u64,
            segment_hash,
            receipt,
        })
    }

    pub async fn read_append_stream_records(
        &self,
        claims: auth::Claims,
        bucket_name: &str,
        stream_key: &str,
        stream_id: uuid::Uuid,
        after_sequence: u64,
        limit: u32,
        include_payload: bool,
    ) -> Result<Vec<AppendStreamRecordRead>, Status> {
        if !auth::is_authorized(AnvilAction::ObjectRead, bucket_name, &claims.scopes)
            && !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes)
        {
            return Err(Status::permission_denied("Permission denied"));
        }
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        let stream = self
            .persistence
            .get_active_append_stream(claims.tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let mut records = self
            .persistence
            .list_append_stream_records(stream.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .filter(|record| u64::try_from(record.record_sequence).unwrap_or(0) > after_sequence)
            .collect::<Vec<_>>();
        records.sort_by_key(|record| record.record_sequence);
        let limit = if limit == 0 { 100 } else { limit.min(1000) } as usize;
        records.truncate(limit);

        let mut out = Vec::with_capacity(records.len());
        for record in records {
            let payload = if include_payload {
                Some(
                    self.core_store
                        .get_blob(GetBlob {
                            object_ref: record.payload_object_ref.clone(),
                        })
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?,
                )
            } else {
                None
            };
            out.push(AppendStreamRecordRead {
                record_sequence: u64::try_from(record.record_sequence)
                    .map_err(|_| Status::internal("Invalid append record sequence"))?,
                payload_hash: record.payload_hash,
                payload_size: record.payload_size,
                content_type: record.content_type,
                user_metadata: record.user_meta,
                created_at: record.created_at,
                payload,
            });
        }
        Ok(out)
    }

    pub async fn compare_and_swap_manifest(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        manifest_key: &str,
        expected_revision: u64,
        manifest_json: &str,
        scopes: &[String],
    ) -> Result<ManifestCasResult, Status> {
        self.validate_write_request(bucket_name, manifest_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let expected_revision = i64::try_from(expected_revision)
            .map_err(|_| Status::invalid_argument("expected_revision exceeds supported range"))?;
        let manifest: JsonValue = serde_json::from_str(manifest_json)
            .map_err(|e| Status::invalid_argument(format!("Invalid manifest JSON: {}", e)))?;
        let manifest_bytes = serde_json::to_vec(&manifest)
            .map_err(|e| Status::internal(format!("Failed to encode manifest JSON: {}", e)))?;
        let manifest_hash = blake3::hash(&manifest_bytes).to_hex().to_string();

        let result = self
            .persistence
            .compare_and_swap_manifest(
                tenant_id,
                bucket.id,
                &bucket.name,
                manifest_key,
                expected_revision,
                manifest,
                &manifest_hash,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition("Manifest revision mismatch"))?;

        Ok(ManifestCasResult {
            revision: u64::try_from(result.revision)
                .map_err(|_| Status::internal("Invalid manifest revision"))?,
            manifest_hash: result.manifest_hash,
            receipt: result.receipt,
        })
    }

    pub async fn get_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: String,
        object_key: String,
        version_id: Option<uuid::Uuid>,
    ) -> Result<
        (
            Object,
            Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
        ),
        Status,
    > {
        let result = self
            .get_object_with_link_mode(
                claims,
                bucket_name,
                object_key,
                version_id,
                ObjectLinkReadMode::Follow,
            )
            .await?;
        Ok((result.object, result.stream))
    }

    pub async fn get_object_with_link_mode(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: String,
        object_key: String,
        version_id: Option<uuid::Uuid>,
        link_mode: ObjectLinkReadMode,
    ) -> Result<ObjectReadResult, Status> {
        self.get_object_with_link_mode_for_tenant(
            claims,
            None,
            bucket_name,
            object_key,
            version_id,
            link_mode,
        )
        .await
    }

    pub async fn get_object_with_link_mode_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: String,
        object_key: String,
        version_id: Option<uuid::Uuid>,
        link_mode: ObjectLinkReadMode,
    ) -> Result<ObjectReadResult, Status> {
        let _latency = self
            .observability
            .latency_guard(OBJECT_READ_LATENCY, &[("api", "native")]);
        if !validation::is_valid_bucket_name(&bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(&object_key) {
            self.record_reserved_namespace_rejection("get_object");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(&object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, &bucket_name)
            .await?;

        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !self
                .object_read_allowed(claims, &bucket_name, &object_key, None)
                .await?
            {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let mut object = match version_id {
            Some(version_id) => {
                let object = metadata_journal::read_object_version(
                    &self.storage,
                    &bucket,
                    &self.signing_key,
                    &object_key,
                    version_id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.signing_key,
                &object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?,
        };
        let mut followed_link = None;
        if version_id.is_none() && object.kind == object_links::ObjectEntryKind::Link {
            if link_mode == ObjectLinkReadMode::Metadata {
                return Err(Status::failed_precondition("ObjectLinkMetadataRead"));
            }
            let (target, link) = self
                .resolve_followed_link(&bucket, object, claims.as_ref())
                .await?;
            object = target;
            followed_link = Some(link);
        }

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();
        let object_clone = object.clone();

        tokio::spawn(async move {
            let Some(object_ref) = object_clone
                .shard_map
                .as_ref()
                .and_then(core_object_ref_from_shard_map)
            else {
                let _ = tx
                    .send(Err(Status::not_found(
                        "Object data unavailable: object is not CoreStore-backed",
                    )))
                    .await;
                return;
            };

            match app_state.core_store.get_blob(GetBlob { object_ref }).await {
                Ok(full_data) => {
                    for chunk in full_data.chunks(1024 * 64) {
                        if tx.send(Ok(chunk.to_vec())).await.is_err() {
                            return;
                        }
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(Status::not_found(error.to_string()))).await;
                }
            }
        });

        Ok(ObjectReadResult {
            object,
            stream: Box::pin(ReceiverStream::new(rx)),
            followed_link,
        })
    }

    pub async fn delete_object(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<Object, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            self.record_reserved_namespace_rejection("delete_object");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        if !auth::is_authorized(
            AnvilAction::ObjectDelete,
            &format!("{}/{}", bucket_name, object_key),
            scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;

        let delete_marker = self
            .persistence
            .soft_delete_object(bucket.id, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        self.publish_object_watch_event(tenant_id, &bucket, &delete_marker, "delete", true)
            .await?;

        Ok(delete_marker)
    }

    pub async fn delete_object_version(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        version_id: uuid::Uuid,
        scopes: &[String],
    ) -> Result<Object, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        if !auth::is_authorized(
            AnvilAction::ObjectDelete,
            &format!("{}/{}", bucket_name, object_key),
            scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
        }

        let deleted = self
            .persistence
            .delete_object_version(bucket.id, object_key, version_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object version not found"))?;

        self.publish_object_watch_event(
            tenant_id,
            &bucket,
            &deleted,
            "delete_version",
            deleted.deleted_at.is_some(),
        )
        .await?;

        Ok(deleted)
    }

    pub async fn head_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
    ) -> Result<Object, Status> {
        Ok(self
            .head_object_with_link_mode(
                claims,
                bucket_name,
                object_key,
                version_id,
                ObjectLinkReadMode::Follow,
            )
            .await?
            .object)
    }

    pub async fn head_object_with_link_mode(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        link_mode: ObjectLinkReadMode,
    ) -> Result<ObjectHeadResult, Status> {
        self.head_object_with_link_mode_for_tenant(
            claims,
            None,
            bucket_name,
            object_key,
            version_id,
            link_mode,
        )
        .await
    }

    pub async fn head_object_with_link_mode_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        link_mode: ObjectLinkReadMode,
    ) -> Result<ObjectHeadResult, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;

        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !self
                .object_read_allowed(claims, bucket_name, object_key, None)
                .await?
            {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let mut object = match version_id {
            Some(version_id) => {
                let object = metadata_journal::read_object_version(
                    &self.storage,
                    &bucket,
                    &self.signing_key,
                    object_key,
                    version_id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.signing_key,
                object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?,
        };
        let mut followed_link = None;
        if version_id.is_none() && object.kind == object_links::ObjectEntryKind::Link {
            if link_mode == ObjectLinkReadMode::Metadata {
                return Err(Status::failed_precondition("ObjectLinkMetadataRead"));
            }
            let (target, link) = self
                .resolve_followed_link(&bucket, object, claims.as_ref())
                .await?;
            object = target;
            followed_link = Some(link);
        }
        Ok(ObjectHeadResult {
            object,
            followed_link,
        })
    }

    pub async fn read_object_link(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
    ) -> Result<object_links::ObjectLinkDescriptor, Status> {
        self.read_object_link_for_tenant(claims, None, bucket_name, object_key, version_id)
            .await
    }

    pub async fn read_object_link_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
    ) -> Result<object_links::ObjectLinkDescriptor, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !self
                .object_read_allowed(claims, bucket_name, object_key, None)
                .await?
            {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let object = match version_id {
            Some(version_id) => metadata_journal::read_object_version(
                &self.storage,
                &bucket,
                &self.signing_key,
                object_key,
                version_id,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object link not found"))?,
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.signing_key,
                object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object link not found"))?,
        };
        if object.deleted_at.is_some() || object.kind != object_links::ObjectEntryKind::Link {
            return Err(Status::not_found("Object link not found"));
        }
        object_links::link_descriptor(&bucket.name, &object)
            .ok_or_else(|| Status::internal("Object link descriptor missing"))
    }

    pub async fn list_objects(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>), Status> {
        self.list_objects_for_tenant(
            claims,
            None,
            bucket_name,
            prefix,
            start_after,
            limit,
            delimiter,
        )
        .await
    }

    pub async fn list_objects_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>), Status> {
        let _latency = self
            .observability
            .latency_guard(PREFIX_LIST_LATENCY, &[("api", "native")]);
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            self.record_reserved_namespace_rejection("list_objects");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }

        // Allow public buckets to bypass auth; otherwise require appropriate scope
        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let mut objects = metadata_journal::read_current_directory_objects(
            &self.storage,
            &bucket,
            &self.signing_key,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        objects.retain(|object| {
            object.key.starts_with(prefix)
                && object.key.as_str() > start_after
                && !validation::is_reserved_internal_key(&object.key)
        });
        objects.sort_by(|left, right| left.key.cmp(&right.key));

        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .expect("private bucket listing has claims after authorization");
            objects = self
                .filter_objects_visible_to_reader(claims, bucket_name, objects, None)
                .await?;
        }

        let listing =
            visible_object_listing(objects, prefix, normalized_list_limit(limit), delimiter);
        Ok((listing.objects, listing.common_prefixes))
    }

    pub async fn list_object_versions(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        version_id_marker: &str,
        limit: i32,
    ) -> Result<crate::persistence::ObjectVersionsPage, Status> {
        self.list_object_versions_for_tenant(
            claims,
            None,
            bucket_name,
            prefix,
            key_marker,
            version_id_marker,
            limit,
        )
        .await
    }

    pub async fn list_object_versions_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        version_id_marker: &str,
        limit: i32,
    ) -> Result<crate::persistence::ObjectVersionsPage, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }
        if !key_marker.is_empty() && !validation::is_valid_object_key(key_marker) {
            return Err(Status::invalid_argument("Invalid key marker"));
        }
        let version_id_marker = if version_id_marker.is_empty() {
            None
        } else if key_marker.is_empty() {
            return Err(Status::invalid_argument(
                "version id marker requires key marker",
            ));
        } else {
            Some(
                uuid::Uuid::parse_str(version_id_marker)
                    .map_err(|_| Status::invalid_argument("Invalid version id marker"))?,
            )
        };

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        if bucket.is_public_read {
            return metadata_journal::read_object_versions(
                &self.storage,
                &bucket,
                &self.signing_key,
                prefix,
                key_marker,
                version_id_marker,
                normalized_list_limit(limit),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()));
        }

        let claims = claims
            .as_ref()
            .expect("private bucket version listing has claims after authorization");
        self.list_visible_object_versions(
            claims,
            bucket_name,
            &bucket,
            prefix,
            key_marker,
            version_id_marker,
            normalized_list_limit(limit),
        )
        .await
    }

    pub async fn current_object_for_write_precondition(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<Option<Object>, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        metadata_journal::read_current_object(&self.storage, &bucket, &self.signing_key, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn current_object_for_mutation_precondition(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
        action: AnvilAction,
    ) -> Result<Option<Object>, Status> {
        self.validate_object_request(bucket_name, object_key, scopes, action)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        metadata_journal::read_current_object(&self.storage, &bucket, &self.signing_key, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn copy_object(
        &self,
        claims: auth::Claims,
        source_bucket_name: &str,
        source_object_key: &str,
        source_version_id: Option<uuid::Uuid>,
        destination_bucket_name: &str,
        destination_object_key: &str,
    ) -> Result<Object, Status> {
        self.validate_write_request(
            destination_bucket_name,
            destination_object_key,
            &claims.scopes,
        )?;
        let source_object = self
            .head_object(
                Some(claims.clone()),
                source_bucket_name,
                source_object_key,
                source_version_id,
            )
            .await?;
        let destination_bucket = self
            .get_tenant_bucket(claims.tenant_id, destination_bucket_name)
            .await?;

        let copied = self
            .persistence
            .create_object(
                claims.tenant_id,
                destination_bucket.id,
                destination_object_key,
                &source_object.content_hash,
                source_object.size,
                &source_object.etag,
                source_object.content_type.as_deref(),
                source_object.user_meta,
                source_object.shard_map,
                None,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.publish_object_watch_event(
            claims.tenant_id,
            &destination_bucket,
            &copied,
            "copy",
            false,
        )
        .await?;

        Ok(copied)
    }

    pub async fn compose_object(
        &self,
        claims: auth::Claims,
        sources: Vec<ComposeSource>,
        destination_bucket_name: &str,
        destination_object_key: &str,
    ) -> Result<Object, Status> {
        if sources.is_empty() {
            return Err(Status::invalid_argument(
                "ComposeObject requires at least one source",
            ));
        }

        let state = ComposeStreamState {
            manager: self.clone(),
            claims: claims.clone(),
            sources: sources.into_iter(),
            current: None,
        };
        let composed_stream = Box::pin(futures_util::stream::try_unfold(
            state,
            |mut state| async move {
                loop {
                    if let Some(current) = state.current.as_mut() {
                        match current.next().await {
                            Some(Ok(chunk)) => return Ok(Some((chunk, state))),
                            Some(Err(status)) => return Err(status),
                            None => {
                                state.current = None;
                                continue;
                            }
                        }
                    }

                    let Some(source) = state.sources.next() else {
                        return Ok(None);
                    };
                    let (_object, stream) = state
                        .manager
                        .get_object(
                            Some(state.claims.clone()),
                            source.bucket_name,
                            source.object_key,
                            source.version_id,
                        )
                        .await?;
                    state.current = Some(stream);
                }
            },
        ));

        self.put_object(
            claims.tenant_id,
            destination_bucket_name,
            destination_object_key,
            &claims.scopes,
            composed_stream,
            ObjectWriteOptions::default(),
        )
        .await
    }

    pub async fn patch_json_object(
        &self,
        claims: auth::Claims,
        bucket_name: &str,
        object_key: &str,
        base_version_id: Option<uuid::Uuid>,
        merge_patch_json: &str,
    ) -> Result<Object, Status> {
        let (_source_object, source_stream) = self
            .get_object(
                Some(claims.clone()),
                bucket_name.to_string(),
                object_key.to_string(),
                base_version_id,
            )
            .await?;

        let source_bytes = collect_stream_bytes(source_stream).await?;
        let mut document: JsonValue = serde_json::from_slice(&source_bytes)
            .map_err(|e| Status::invalid_argument(format!("Object is not valid JSON: {}", e)))?;
        let patch: JsonValue = serde_json::from_str(merge_patch_json)
            .map_err(|e| Status::invalid_argument(format!("Patch is not valid JSON: {}", e)))?;

        apply_json_merge_patch(&mut document, patch);
        let patched_bytes = serde_json::to_vec(&document)
            .map_err(|e| Status::internal(format!("Failed to serialize patched JSON: {}", e)))?;

        self.put_object(
            claims.tenant_id,
            bucket_name,
            object_key,
            &claims.scopes,
            tokio_stream::iter(vec![Ok(patched_bytes)]),
            ObjectWriteOptions {
                content_type: Some("application/json".to_string()),
                user_metadata: None,
            },
        )
        .await
    }

    async fn get_authorized_bucket(
        &self,
        claims: Option<&auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        if let (Some(claims), Some(route_tenant_id)) = (claims, route_tenant_id)
            && claims.tenant_id != route_tenant_id
        {
            return Err(Status::permission_denied(
                "Credentials are not valid for routed tenant",
            ));
        }

        let tenant_id = route_tenant_id.or_else(|| claims.map(|claims| claims.tenant_id));
        if let Some(tenant_id) = tenant_id
            && let Some(locator) = self
                .persistence
                .get_mesh_bucket_locator(tenant_id, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            && locator.status != crate::mesh_directory::BucketLocatorStatus::Deleted
            && locator.home_region.as_str() != self.region.as_str()
        {
            return Err(self.remote_bucket_status(locator.home_region.as_str()));
        }

        let bucket = match tenant_id {
            Some(tenant_id) => {
                bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Bucket not found for this tenant"))
            }
            None => bucket_journal::read_public_bucket_by_name(&self.storage, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Public bucket not found")),
        }?;

        if bucket.region != self.region {
            return Err(self.remote_bucket_status(&bucket.region));
        }

        Ok(bucket)
    }

    fn remote_bucket_status(&self, bucket_region: &str) -> Status {
        let action = routing::remote_bucket_routing_action(self.cross_region_routing_policy, false);
        let (code, message, action_name) = match action {
            routing::RemoteBucketRoutingAction::Redirect => (
                tonic::Code::FailedPrecondition,
                format!("Bucket is in region {bucket_region}; redirect required"),
                "redirect",
            ),
            routing::RemoteBucketRoutingAction::Proxy => (
                tonic::Code::Unavailable,
                format!("Bucket is in region {bucket_region}; native proxy is unavailable"),
                "proxy_unavailable",
            ),
            routing::RemoteBucketRoutingAction::RejectLocalOnly => (
                tonic::Code::FailedPrecondition,
                format!("Bucket is in region {bucket_region}; cross-region routing is disabled"),
                "local_only",
            ),
            routing::RemoteBucketRoutingAction::ProxyUnavailable => (
                tonic::Code::Unavailable,
                format!("Bucket is in region {bucket_region}; cross-region proxy is unavailable"),
                "proxy_unavailable",
            ),
        };
        let mut status = Status::new(code, message);
        if let Ok(value) = MetadataValue::try_from(bucket_region) {
            status.metadata_mut().insert("x-anvil-bucket-region", value);
        }
        if let Ok(value) = MetadataValue::try_from(action_name) {
            status
                .metadata_mut()
                .insert("x-anvil-cross-region-action", value);
        }
        status
    }

    async fn resolve_followed_link(
        &self,
        bucket: &Bucket,
        initial_link: Object,
        claims: Option<&auth::Claims>,
    ) -> Result<(Object, object_links::FollowedObjectLink), Status> {
        let initial_descriptor = object_links::link_descriptor(&bucket.name, &initial_link)
            .ok_or_else(|| Status::internal("Object link descriptor missing"))?;
        if initial_descriptor.resolution != object_links::ObjectLinkResolution::Follow {
            return Err(Status::failed_precondition("ObjectLinkRedirectRequired"));
        }

        let mut current_link = initial_link;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..object_links::MAX_LINK_RESOLUTION_DEPTH {
            let Some(link) = current_link.link.clone() else {
                return Err(Status::failed_precondition("InvalidObjectLink"));
            };
            let seen_key = format!("{}:{}", current_link.key, current_link.version_id);
            if !seen.insert(seen_key) {
                return Err(Status::failed_precondition("ObjectLinkLoop"));
            }
            if !bucket.is_public_read {
                let claims =
                    claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
                if !self
                    .object_read_allowed(claims, &bucket.name, &link.target_key, None)
                    .await?
                {
                    return Err(Status::permission_denied("Permission denied"));
                }
            }

            let target = match link.target_version {
                Some(version_id) => {
                    metadata_journal::read_object_version(
                        &self.storage,
                        bucket,
                        &self.signing_key,
                        &link.target_key,
                        version_id,
                    )
                    .await
                }
                None => {
                    metadata_journal::read_current_object(
                        &self.storage,
                        bucket,
                        &self.signing_key,
                        &link.target_key,
                    )
                    .await
                }
            }
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition("DanglingObjectLink"))?;
            if target.deleted_at.is_some() {
                return Err(Status::failed_precondition("DanglingObjectLink"));
            }
            match target.kind {
                object_links::ObjectEntryKind::Blob => {
                    let response_etag = object_links::followed_link_etag(&current_link, &target)
                        .ok_or_else(|| Status::internal("Object link ETag missing"))?;
                    let mut served = target;
                    served.etag = response_etag.clone();
                    let followed = object_links::FollowedObjectLink {
                        descriptor: initial_descriptor,
                        response_etag,
                        target_version: served.version_id,
                    };
                    return Ok((served, followed));
                }
                object_links::ObjectEntryKind::Link => {
                    current_link = target;
                }
            }
        }
        Err(Status::failed_precondition("ObjectLinkDepthExceeded"))
    }

    async fn object_read_allowed(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        authz_revision: Option<i64>,
    ) -> Result<bool, Status> {
        let object_resource = format!("{bucket_name}/{object_key}");
        access_control::scope_or_relationship_allows(
            &self.storage,
            claims,
            AnvilAction::ObjectRead,
            &object_resource,
            "object",
            &object_resource,
            "reader",
            authz_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))
    }

    async fn filter_objects_visible_to_reader(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        objects: Vec<Object>,
        authz_revision: Option<i64>,
    ) -> Result<Vec<Object>, Status> {
        let mut visible = Vec::new();
        for object in objects {
            if self
                .object_read_allowed(claims, bucket_name, &object.key, authz_revision)
                .await?
            {
                visible.push(object);
            }
        }
        Ok(visible)
    }

    async fn list_visible_object_versions(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage, Status> {
        let requested_limit = normalized_list_limit(limit).max(1) as usize;
        let visible_target = requested_limit.saturating_add(1);
        let page_limit = i32::try_from(visible_target.max(100)).unwrap_or(i32::MAX);
        let mut visible = Vec::<ObjectVersion>::new();
        let mut current_key_marker = key_marker.to_string();
        let mut current_version_marker = version_id_marker;

        loop {
            let page = metadata_journal::read_object_versions(
                &self.storage,
                bucket,
                &self.signing_key,
                prefix,
                &current_key_marker,
                current_version_marker,
                page_limit,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            for version in page.versions {
                if self
                    .object_read_allowed(claims, bucket_name, &version.object.key, None)
                    .await?
                {
                    visible.push(version);
                    if visible.len() >= visible_target {
                        break;
                    }
                }
            }

            if visible.len() >= visible_target || !page.is_truncated {
                break;
            }

            let Some(next_key_marker) = page.next_key_marker else {
                break;
            };
            current_key_marker = next_key_marker;
            current_version_marker = page.next_version_id_marker;
        }

        let is_truncated = visible.len() > requested_limit;
        if is_truncated {
            visible.truncate(requested_limit);
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            visible
                .last()
                .map(|version| {
                    (
                        Some(version.object.key.clone()),
                        Some(version.object.version_id),
                    )
                })
                .unwrap_or((None, None))
        } else {
            (None, None)
        };

        Ok(ObjectVersionsPage {
            versions: visible,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    async fn publish_object_watch_event(
        &self,
        tenant_id: i64,
        bucket: &Bucket,
        object: &Object,
        event_type: &str,
        is_delete_marker: bool,
    ) -> Result<(), Status> {
        let event = self
            .persistence
            .create_object_watch_event(
                tenant_id,
                bucket.id,
                &bucket.name,
                object,
                event_type,
                is_delete_marker,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        watch_log::append_object_watch_record(&self.storage, bucket, object, &event)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.watch_tx.send(event);
        Ok(())
    }

    fn validate_write_request(
        &self,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<(), Status> {
        self.validate_object_request(bucket_name, object_key, scopes, AnvilAction::ObjectWrite)
    }

    fn validate_object_request(
        &self,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
        action: AnvilAction,
    ) -> Result<(), Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }
        if !auth::is_authorized(action, &format!("{}/{}", bucket_name, object_key), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
        Ok(())
    }

    async fn get_tenant_bucket(&self, tenant_id: i64, bucket_name: &str) -> Result<Bucket, Status> {
        if let Some(locator) = self
            .persistence
            .get_mesh_bucket_locator(tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            && locator.status != crate::mesh_directory::BucketLocatorStatus::Deleted
            && locator.home_region.as_str() != self.region.as_str()
        {
            return Err(self.remote_bucket_status(locator.home_region.as_str()));
        }

        let bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        if bucket.region != self.region {
            return Err(self.remote_bucket_status(&bucket.region));
        }

        Ok(bucket)
    }
}

fn normalized_list_limit(limit: i32) -> i32 {
    if limit <= 0 { 1000 } else { limit }
}

fn visible_object_listing(
    objects: Vec<Object>,
    prefix: &str,
    limit: i32,
    delimiter: &str,
) -> metadata_journal::NativeObjectListing {
    let limit = limit.max(1) as usize;
    if delimiter.is_empty() {
        return metadata_journal::NativeObjectListing {
            objects: objects.into_iter().take(limit).collect(),
            common_prefixes: Vec::new(),
        };
    }

    enum ListingEntry {
        Object(Object),
        CommonPrefix(String),
    }

    let mut merged = std::collections::BTreeMap::<String, ListingEntry>::new();
    for object in objects {
        let suffix = &object.key[prefix.len()..];
        if let Some(position) = suffix.find(delimiter) {
            let common_prefix = format!("{}{}", prefix, &suffix[..position + delimiter.len()]);
            merged
                .entry(common_prefix.clone())
                .or_insert(ListingEntry::CommonPrefix(common_prefix));
        } else {
            merged.insert(object.key.clone(), ListingEntry::Object(object));
        }
        if merged.len() >= limit {
            break;
        }
    }

    let mut listing = metadata_journal::NativeObjectListing {
        objects: Vec::new(),
        common_prefixes: Vec::new(),
    };
    for (_, entry) in merged.into_iter().take(limit) {
        match entry {
            ListingEntry::Object(object) => listing.objects.push(object),
            ListingEntry::CommonPrefix(common_prefix) => {
                listing.common_prefixes.push(common_prefix)
            }
        }
    }
    listing
}

async fn collect_stream_bytes(
    mut stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
) -> Result<Vec<u8>, Status> {
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk?);
    }
    Ok(bytes)
}

fn apply_json_merge_patch(target: &mut JsonValue, patch: JsonValue) {
    match patch {
        JsonValue::Object(patch_object) => {
            if !target.is_object() {
                *target = JsonValue::Object(serde_json::Map::new());
            }
            let target_object = target.as_object_mut().expect("target set to object");
            for (key, value) in patch_object {
                if value.is_null() {
                    target_object.remove(&key);
                } else {
                    apply_json_merge_patch(
                        target_object.entry(key).or_insert(JsonValue::Null),
                        value,
                    );
                }
            }
        }
        replacement => {
            *target = replacement;
        }
    }
}

fn validate_multipart_part_number(part_number: i32) -> Result<(), Status> {
    if (1..=10_000).contains(&part_number) {
        Ok(())
    } else {
        Err(Status::invalid_argument(
            "Multipart part number must be between 1 and 10000",
        ))
    }
}

fn core_object_ref_to_shard_map(object_ref: &CoreObjectRef) -> JsonValue {
    serde_json::json!({
        "schema": "anvil.core.object_ref.v1",
        "hash": object_ref.hash,
        "logical_size": object_ref.logical_size,
        "manifest_ref": object_ref.manifest_ref,
    })
}

fn core_object_ref_from_shard_map(value: &JsonValue) -> Option<CoreObjectRef> {
    if value.get("schema")?.as_str()? != "anvil.core.object_ref.v1" {
        return None;
    }
    Some(CoreObjectRef {
        hash: value.get("hash")?.as_str()?.to_string(),
        logical_size: value.get("logical_size")?.as_u64()?,
        manifest_ref: value.get("manifest_ref")?.as_str()?.to_string(),
    })
}

fn trim_s3_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
}
