use crate::{
    access_control, auth, bucket_journal,
    core_store::{
        CoreBoundarySchema, CoreBoundarySource, CoreBoundaryValue, CoreByteRange, CoreObjectRef,
        CoreStore, GetBlob, GetBlobRange, PutBlob,
    },
    error_codes::AnvilErrorCode,
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
use anyhow::{Result as AnyhowResult, anyhow, bail};
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
    pub range_start: u64,
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

    async fn object_write_boundary_values(
        &self,
        bucket_name: &str,
        object_key: &str,
        content_type: Option<&str>,
        user_metadata: Option<&JsonValue>,
        payload: &[u8],
    ) -> Result<Vec<CoreBoundaryValue>, Status> {
        let Some(schema) = self
            .core_store
            .read_boundary_schema(bucket_name)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
        else {
            return Ok(Vec::new());
        };
        extract_object_boundary_values(&schema, object_key, content_type, user_metadata, payload)
            .map_err(|error| Status::invalid_argument(error.to_string()))
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
        let boundary_values = self
            .object_write_boundary_values(
                &bucket.name,
                object_key,
                options.content_type.as_deref(),
                options.user_metadata.as_ref(),
                &payload,
            )
            .await?;
        let step_start = std::time::Instant::now();
        let object_ref = self
            .core_store
            .put_blob(PutBlob {
                logical_name: format!(
                    "tenant:{tenant_id}/bucket:{}/object:{}",
                    bucket.name, object_key
                ),
                bytes: payload,
                boundary_values,
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
                boundary_values: Vec::new(),
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
                boundary_values: Vec::new(),
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
}

mod read;

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
    let mut value = serde_json::to_value(object_ref).unwrap_or(JsonValue::Null);
    if let JsonValue::Object(map) = &mut value {
        map.insert(
            "schema".to_string(),
            JsonValue::String("anvil.core.object_ref.v1".to_string()),
        );
    }
    value
}

fn core_object_ref_from_shard_map(value: &JsonValue) -> Option<CoreObjectRef> {
    if value.get("schema")?.as_str()? != "anvil.core.object_ref.v1" {
        return None;
    }
    serde_json::from_value(value.clone()).ok()
}

fn extract_object_boundary_values(
    schema: &CoreBoundarySchema,
    object_key: &str,
    content_type: Option<&str>,
    user_metadata: Option<&JsonValue>,
    payload: &[u8],
) -> AnyhowResult<Vec<CoreBoundaryValue>> {
    let mut values = Vec::new();
    for dimension in &schema.dimensions {
        let (source_kind, raw_value) = match &dimension.source {
            CoreBoundarySource::UserMetadataJsonPointer { pointer } => (
                "user_metadata_json_pointer",
                user_metadata
                    .and_then(|metadata| metadata.pointer(pointer))
                    .cloned(),
            ),
            CoreBoundarySource::PathTemplate { template } => (
                "path_template",
                extract_path_template_capture(template, object_key, &dimension.name),
            ),
            CoreBoundarySource::BodyJsonPointer {
                pointer,
                max_body_bytes,
            } => {
                if !content_type.is_some_and(is_json_content_type) {
                    bail!(
                        "{}: boundary dimension {} requires JSON content type",
                        AnvilErrorCode::BoundaryExtractorUnsupportedContentType.as_str(),
                        dimension.name
                    );
                }
                if payload.len() as u64 > *max_body_bytes {
                    bail!(
                        "{}: boundary dimension {} body exceeds {} bytes",
                        AnvilErrorCode::BoundaryExtractorBodyTooLarge.as_str(),
                        dimension.name,
                        max_body_bytes
                    );
                }
                let body: JsonValue = serde_json::from_slice(payload).map_err(|error| {
                    anyhow!(
                        "{}: boundary dimension {} body is not valid JSON: {error}",
                        AnvilErrorCode::BoundaryTypeMismatch.as_str(),
                        dimension.name
                    )
                })?;
                ("body_json_pointer", body.pointer(pointer).cloned())
            }
        };

        let Some(raw_value) = raw_value else {
            if dimension.required {
                bail!(
                    "{}: required boundary dimension {} is missing",
                    AnvilErrorCode::BoundaryRequiredMissing.as_str(),
                    dimension.name
                );
            }
            continue;
        };
        let value = normalise_boundary_value(&dimension.value_type, &raw_value)
            .map_err(|error| anyhow!("{} for dimension {}", error, dimension.name))?;
        values.push(CoreBoundaryValue {
            schema_generation: schema.generation,
            name: dimension.name.clone(),
            value_type: dimension.value_type.clone(),
            value,
            categories: dimension.categories.clone(),
            source_kind: source_kind.to_string(),
            required: dimension.required,
        });
    }
    Ok(values)
}

fn is_json_content_type(content_type: &str) -> bool {
    let content_type = content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .to_ascii_lowercase();
    content_type == "application/json" || content_type.ends_with("+json")
}

fn extract_path_template_capture(
    template: &str,
    object_key: &str,
    capture_name: &str,
) -> Option<JsonValue> {
    let template_segments = template
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let object_segments = object_key
        .trim_start_matches('/')
        .split('/')
        .collect::<Vec<_>>();
    let mut captures = serde_json::Map::new();
    let mut object_index = 0usize;
    for segment in template_segments {
        if segment == "**" {
            break;
        }
        let object_segment = object_segments.get(object_index)?;
        object_index += 1;
        if let Some(capture) = segment
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
        {
            let name = capture.split(':').next().unwrap_or(capture);
            captures.insert(
                name.to_string(),
                JsonValue::String((*object_segment).to_string()),
            );
        } else if segment != *object_segment {
            return None;
        }
    }
    captures.remove(capture_name)
}

fn normalise_boundary_value(value_type: &str, value: &JsonValue) -> AnyhowResult<String> {
    match value_type {
        "string" => value.as_str().map(str::to_string).ok_or_else(|| {
            anyhow!(
                "{}: expected string boundary value",
                AnvilErrorCode::BoundaryTypeMismatch.as_str()
            )
        }),
        "uuid" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected uuid string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let uuid = uuid::Uuid::parse_str(value).map_err(|_| {
                anyhow!(
                    "{}: expected canonical uuid boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(uuid.to_string())
        }
        "u64" => value
            .as_u64()
            .map(|value| value.to_string())
            .or_else(|| {
                value
                    .as_str()?
                    .parse::<u64>()
                    .ok()
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| {
                anyhow!(
                    "{}: expected u64 boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            }),
        "i64" => value
            .as_i64()
            .map(|value| value.to_string())
            .or_else(|| {
                value
                    .as_str()?
                    .parse::<i64>()
                    .ok()
                    .map(|value| value.to_string())
            })
            .ok_or_else(|| {
                anyhow!(
                    "{}: expected i64 boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            }),
        "date" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected date string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let date = chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").map_err(|_| {
                anyhow!(
                    "{}: expected YYYY-MM-DD boundary date",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(date.to_string())
        }
        "timestamp" => {
            let value = value.as_str().ok_or_else(|| {
                anyhow!(
                    "{}: expected timestamp string boundary value",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            let timestamp = chrono::DateTime::parse_from_rfc3339(value).map_err(|_| {
                anyhow!(
                    "{}: expected RFC3339 boundary timestamp",
                    AnvilErrorCode::BoundaryTypeMismatch.as_str()
                )
            })?;
            Ok(timestamp.to_rfc3339())
        }
        _ => bail!("unsupported boundary value type {value_type}"),
    }
}

fn trim_s3_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
}

#[cfg(test)]
mod tests;
