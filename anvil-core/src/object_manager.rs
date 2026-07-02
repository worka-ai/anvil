use crate::{
    access_control,
    anvil_api::{
        CommitShardRequest, GetShardRequest, PutShardRequest, internal_anvil_service_client,
    },
    auth, bucket_journal,
    cluster::ClusterState,
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
    placement::PlacementManager,
    routing::{self, CrossRegionRoutingPolicy},
    sharding::ShardManager,
    storage::{ExternalChunkManifest, Storage},
    validation, watch_log,
};
use futures_util::{Stream, StreamExt};
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tracing::info;

const INLINE_PAYLOAD_MAX_BYTES: i64 = 64 * 1024;

#[derive(Debug, Clone, Default)]
struct CommittedPayload {
    inline_payload: Option<Vec<u8>>,
    chunk_manifest: Option<ExternalChunkManifest>,
}

#[derive(Debug, Clone)]
pub struct ObjectManager {
    persistence: Persistence,
    placer: PlacementManager,
    cluster: ClusterState,
    sharder: ShardManager,
    storage: Storage,
    region: String,
    cross_region_routing_policy: CrossRegionRoutingPolicy,
    jwt_manager: Arc<auth::JwtManager>,
    encryption_key: Vec<u8>,
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
pub struct ObjectHeadResult {
    pub object: Object,
    pub followed_link: Option<object_links::FollowedObjectLink>,
}

async fn commit_temp_payload(
    storage: &Storage,
    temp_path: &std::path::Path,
    total_bytes: i64,
    content_hash: &str,
) -> Result<CommittedPayload, Status> {
    if total_bytes <= INLINE_PAYLOAD_MAX_BYTES {
        let payload = tokio::fs::read(temp_path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        tokio::fs::remove_file(temp_path)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        return Ok(CommittedPayload {
            inline_payload: Some(payload),
            chunk_manifest: None,
        });
    }

    info!("Committing external object chunks");
    let chunk_manifest = storage
        .commit_external_chunks(temp_path, content_hash)
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
    Ok(CommittedPayload {
        inline_payload: None,
        chunk_manifest: Some(chunk_manifest),
    })
}

#[derive(Debug, Clone)]
pub struct AppendStreamRecordResult {
    pub record_sequence: u64,
    pub payload_hash: String,
    pub payload_size: i64,
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
        placer: PlacementManager,
        cluster: ClusterState,
        sharder: ShardManager,
        storage: Storage,
        region: String,
        cross_region_routing_policy: CrossRegionRoutingPolicy,
        jwt_manager: Arc<auth::JwtManager>,
        anvil_secret_encryption_key: String,
        watch_tx: broadcast::Sender<ObjectWatchEvent>,
        observability: Observability,
    ) -> Self {
        let encryption_key = hex::decode(anvil_secret_encryption_key)
            .expect("ANVIL_SECRET_ENCRYPTION_KEY must be a valid hex string");
        Self {
            persistence,
            placer,
            cluster,
            sharder,
            storage,
            region,
            cross_region_routing_policy,
            jwt_manager,
            encryption_key,
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
        mut data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin,
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
        let nodes = self
            .placer
            .calculate_placement(object_key, &self.cluster, self.sharder.total_shards())
            .await;

        let total_bytes;
        let content_hash;
        let mut committed_payload = CommittedPayload::default();

        if nodes.len() < self.sharder.total_shards() {
            if nodes.len() >= 1 {
                // Single-node case: stream to a whole file.
                let (temp_path, bytes, hash) = self
                    .storage
                    .stream_to_temp_file(data_stream)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                total_bytes = bytes;
                content_hash = hash;
                committed_payload =
                    commit_temp_payload(&self.storage, &temp_path, total_bytes, &content_hash)
                        .await?;
            } else {
                // No peers known; fallback to single-node path as well
                let (temp_path, bytes, hash) = self
                    .storage
                    .stream_to_temp_file(data_stream)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                total_bytes = bytes;
                content_hash = hash;
                committed_payload =
                    commit_temp_payload(&self.storage, &temp_path, total_bytes, &content_hash)
                        .await?;
            }
        } else {
            // Distributed case: stream and erasure code stripes.
            let mut overall_hasher = blake3::Hasher::new();
            let mut bytes_so_far = 0;
            let upload_id = uuid::Uuid::new_v4().to_string();

            let stripe_size = 1024 * 64; // 64KB per shard in a stripe
            let data_shards_count = self.sharder.data_shards();
            let stripe_buffer_size = stripe_size * data_shards_count;
            let mut stripe_buffer = Vec::with_capacity(stripe_buffer_size);

            let mut clients = Vec::new();
            let cluster_map = self.cluster.read().await;
            for peer_id in &nodes {
                let peer_info = cluster_map.get(peer_id).ok_or_else(|| {
                    Status::internal("Placement selected a peer that is not in the cluster state")
                })?;
                let addr = peer_info.grpc_addr.clone();
                let endpoint = if addr.starts_with("http://") || addr.starts_with("https://") {
                    addr
                } else {
                    format!("http://{}", addr)
                };
                let client =
                    internal_anvil_service_client::InternalAnvilServiceClient::connect(endpoint)
                        .await
                        .map_err(|e| Status::unavailable(e.to_string()))?;
                clients.push(client);
            }

            while let Some(chunk_result) = data_stream.next().await {
                let chunk = chunk_result?;
                overall_hasher.update(&chunk);
                bytes_so_far += chunk.len();
                stripe_buffer.extend_from_slice(&chunk);

                while stripe_buffer.len() >= stripe_buffer_size {
                    let stripe_data = stripe_buffer
                        .drain(..stripe_buffer_size)
                        .collect::<Vec<_>>();
                    self.send_stripe(&clients, &upload_id, stripe_data, stripe_size)
                        .await?;
                }
            }

            if !stripe_buffer.is_empty() {
                stripe_buffer.resize(stripe_buffer_size, 0);
                self.send_stripe(&clients, &upload_id, stripe_buffer, stripe_size)
                    .await?;
            }

            total_bytes = bytes_so_far as i64;
            content_hash = overall_hasher.finalize().to_hex().to_string();

            let mut futures = Vec::new();
            for (i, client) in clients.into_iter().enumerate() {
                let scope = format!("internal:commit_shard|{}/{}", content_hash, i);
                let token = self
                    .jwt_manager
                    .mint_token("internal".to_string(), vec![scope], 0)
                    .map_err(|e| Status::internal(e.to_string()))?;

                let mut client = client.clone();
                let request = CommitShardRequest {
                    upload_id: upload_id.clone(),
                    shard_index: i as u32,
                    final_object_hash: content_hash.clone(),
                };
                let mut req = tonic::Request::new(request);
                req.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {}", token).parse().unwrap(),
                );
                futures.push(async move { client.commit_shard(req).await });
            }
            futures::future::try_join_all(futures)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let shard_map_json = if nodes.len() > 1 {
            let peer_ids: Vec<String> = nodes.iter().map(|p| p.to_base58()).collect();
            Some(serde_json::json!(peer_ids))
        } else {
            committed_payload
                .chunk_manifest
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|e| Status::internal(e.to_string()))?
        };

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
                committed_payload.inline_payload,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.publish_object_watch_event(tenant_id, &bucket, &object, "put", false)
            .await?;

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

        let (temp_path, bytes, content_hash) = self
            .storage
            .stream_to_temp_file(data_stream)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        self.storage
            .commit_whole_object(&temp_path, &content_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let mutation = self
            .persistence
            .upsert_multipart_part(
                upload.id,
                part_number,
                &content_hash,
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

        let mut ordered_hashes = Vec::with_capacity(parts.len());
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
            ordered_hashes.push(stored.content_hash.clone());
        }

        let storage = self.storage.clone();
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            for content_hash in ordered_hashes {
                let bytes = match storage.retrieve_whole_object(&content_hash).await {
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

        let payload_hash = blake3::hash(&payload).to_hex().to_string();
        self.storage
            .store_whole_object(&payload_hash, &payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let mutation = self
            .persistence
            .append_stream_record(stream.id, &payload_hash, payload.len() as i64)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(AppendStreamRecordResult {
            record_sequence: u64::try_from(mutation.record.record_sequence)
                .map_err(|_| Status::internal("Invalid record sequence"))?,
            payload_hash,
            payload_size: payload.len() as i64,
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

    async fn send_stripe(
        &self,
        clients: &[internal_anvil_service_client::InternalAnvilServiceClient<
            tonic::transport::Channel,
        >],
        upload_id: &str,
        stripe_data: Vec<u8>,
        stripe_size: usize,
    ) -> Result<(), Status> {
        let mut shards: Vec<Vec<u8>> = stripe_data
            .chunks(stripe_size)
            .map(|c| c.to_vec())
            .collect();
        shards.resize(self.sharder.total_shards(), vec![0; stripe_size]);
        self.sharder
            .encode(&mut shards, &self.encryption_key)
            .map_err(|e| Status::internal(e.to_string()))?;

        let mut futures = Vec::new();
        for (i, shard_data) in shards.into_iter().enumerate() {
            let scope = format!("internal:put_shard|{}/{}", upload_id, i);
            let token = self
                .jwt_manager
                .mint_token("internal".to_string(), vec![scope], 0)
                .map_err(|e| Status::internal(e.to_string()))?;

            let request = PutShardRequest {
                upload_id: upload_id.to_string(),
                shard_index: i as u32,
                data: shard_data,
            };

            let mut client = clients[i].clone();
            let request_stream = tokio_stream::iter(vec![request]);
            let mut req = tonic::Request::new(request_stream);
            req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );

            futures.push(async move { client.put_shard(req).await });
        }

        futures::future::try_join_all(futures)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
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
                    &self.encryption_key,
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
                &self.encryption_key,
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

        if let Some(inline_payload) = object.inline_payload.clone() {
            let chunks = inline_payload
                .chunks(1024 * 64)
                .map(|chunk| Ok(chunk.to_vec()))
                .collect::<Vec<Result<Vec<u8>, Status>>>();
            return Ok(ObjectReadResult {
                object,
                stream: Box::pin(futures_util::stream::iter(chunks)),
                followed_link,
            });
        }

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();
        let object_clone = object.clone();

        tokio::spawn(async move {
            if let Some(manifest) = object_clone
                .shard_map
                .as_ref()
                .and_then(external_chunk_manifest_from_shard_map)
            {
                for expected_index in 0..manifest.chunks.len() {
                    let record = &manifest.chunks[expected_index];
                    if record.chunk_index != expected_index as u64 {
                        let _ = tx
                            .send(Err(Status::internal(
                                "Object data unavailable: invalid external chunk order",
                            )))
                            .await;
                        return;
                    }
                    let chunk = match app_state
                        .storage
                        .retrieve_external_chunk(&record.storage_ref)
                        .await
                    {
                        Ok(chunk) => chunk,
                        Err(error) => {
                            let _ = tx.send(Err(Status::not_found(error.to_string()))).await;
                            return;
                        }
                    };
                    if chunk.len() as u64 != record.plaintext_length {
                        let _ = tx
                            .send(Err(Status::internal(
                                "Object data unavailable: external chunk length mismatch",
                            )))
                            .await;
                        return;
                    }
                    let actual_hash = blake3::hash(&chunk).to_hex().to_string();
                    if actual_hash != record.payload_chunk_hash
                        || actual_hash != record.storage_chunk_hash
                    {
                        let _ = tx
                            .send(Err(Status::internal(
                                "Object data unavailable: external chunk hash mismatch",
                            )))
                            .await;
                        return;
                    }
                    for part in chunk.chunks(1024 * 64) {
                        if tx.send(Ok(part.to_vec())).await.is_err() {
                            return;
                        }
                    }
                }
                return;
            }

            // Prefer whole-object if available
            if let Ok(full_data) = app_state
                .storage
                .retrieve_whole_object(&object_clone.content_hash)
                .await
            {
                for chunk in full_data.chunks(1024 * 64) {
                    if tx.send(Ok(chunk.to_vec())).await.is_err() {
                        break;
                    }
                }
                return;
            }

            // Strict shard map usage
            let shard_map_peer_ids: Vec<libp2p::PeerId> = match object_clone.shard_map.as_ref() {
                Some(map_json) => {
                    let peer_strs: Vec<String> = match serde_json::from_value(map_json.clone()) {
                        Ok(v) => v,
                        Err(_) => {
                            let _ = tx
                                .send(Err(Status::not_found(
                                    "Object data unavailable: invalid shard map",
                                )))
                                .await;
                            return;
                        }
                    };
                    let mut ids = Vec::with_capacity(peer_strs.len());
                    for s in peer_strs {
                        match s.parse() {
                            Ok(pid) => ids.push(pid),
                            Err(_) => {
                                let _ = tx
                                    .send(Err(Status::not_found(
                                        "Object data unavailable: bad peer id in shard map",
                                    )))
                                    .await;
                                return;
                            }
                        }
                    }
                    ids
                }
                None => {
                    let _ = tx
                        .send(Err(Status::not_found("Object data unavailable")))
                        .await;
                    return;
                }
            };

            let total_shards = app_state.sharder.total_shards();
            if shard_map_peer_ids.len() < total_shards {
                let _ = tx
                    .send(Err(Status::not_found(
                        "Object data unavailable: incomplete shard map",
                    )))
                    .await;
                return;
            }

            let mut shards = Vec::with_capacity(total_shards);
            for i in 0..total_shards {
                let shard_data = app_state
                    .storage
                    .retrieve_shard(&object_clone.content_hash, i as u32)
                    .await
                    .ok();
                shards.push(shard_data);
            }

            let mut missing_shards_futures = Vec::new();
            for i in 0..total_shards {
                if shards[i].is_none() {
                    let peer_id = &shard_map_peer_ids[i];
                    let cluster_map = app_state.cluster.read().await;
                    if let Some(peer_info) = cluster_map.get(peer_id) {
                        let grpc_addr = peer_info.grpc_addr.clone();
                        let object_hash = object_clone.content_hash.clone();
                        let jwt_manager = app_state.jwt_manager.clone();
                        missing_shards_futures.push(async move {
                            let endpoint = if grpc_addr.starts_with("http://")
                                || grpc_addr.starts_with("https://")
                            {
                                grpc_addr
                            } else {
                                format!("http://{}", grpc_addr)
                            };
                            let mut client =
                                internal_anvil_service_client::InternalAnvilServiceClient::connect(
                                    endpoint,
                                )
                                .await
                                .map_err(|e| {
                                    Status::internal(format!("Failed to connect to peer: {}", e))
                                })?;
                            let scope = format!("internal:get_shard|{}/{}", object_hash, i);
                            let token = jwt_manager
                                .mint_token("internal".to_string(), vec![scope], 0)
                                .map_err(|e| Status::internal(e.to_string()))?;
                            let mut req = tonic::Request::new(GetShardRequest {
                                object_hash: object_hash.clone(),
                                shard_index: i as u32,
                            });
                            req.metadata_mut().insert(
                                "authorization",
                                format!("Bearer {}", token).parse().unwrap(),
                            );
                            let mut stream = client.get_shard(req).await?.into_inner();
                            let mut shard_data = Vec::new();
                            while let Some(chunk_res) = stream.next().await {
                                let chunk = chunk_res?;
                                shard_data.extend_from_slice(&chunk.data);
                            }
                            Ok((i, shard_data))
                        });
                    } else {
                        let _ = tx
                            .send(Err(Status::not_found(
                                "Object data unavailable: peer missing from cluster",
                            )))
                            .await;
                        return;
                    }
                }
            }

            let results: Vec<Result<(usize, Vec<u8>), Status>> =
                futures::future::join_all(missing_shards_futures).await;
            for result in results {
                match result {
                    Ok((index, data)) => shards[index] = Some(data),
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }

            if app_state
                .sharder
                .reconstruct(&mut shards, &app_state.encryption_key)
                .is_err()
            {
                let _ = tx
                    .send(Err(Status::internal("Failed to reconstruct object data")))
                    .await;
                return;
            }

            // Stream reconstructed data in 64KB chunks
            let mut full_data = Vec::new();
            let data_shards = &shards[..app_state.sharder.data_shards()];
            for data_shard_opt in data_shards {
                if let Some(sd) = data_shard_opt {
                    full_data.extend_from_slice(sd);
                } else {
                    let _ = tx
                        .send(Err(Status::internal("Failed to reconstruct data")))
                        .await;
                    return;
                }
            }
            full_data.truncate(object_clone.size as usize);
            for chunk in full_data.chunks(1024 * 64) {
                if tx.send(Ok(chunk.to_vec())).await.is_err() {
                    break;
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
                    &self.encryption_key,
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
                &self.encryption_key,
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
                &self.encryption_key,
                object_key,
                version_id,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object link not found"))?,
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.encryption_key,
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
            &self.encryption_key,
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
                &self.encryption_key,
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
        metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.encryption_key,
            object_key,
        )
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
        metadata_journal::read_current_object(
            &self.storage,
            &bucket,
            &self.encryption_key,
            object_key,
        )
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
        let (source_object, source_stream) = self
            .get_object(
                Some(claims.clone()),
                source_bucket_name.to_string(),
                source_object_key.to_string(),
                source_version_id,
            )
            .await?;

        self.put_object(
            claims.tenant_id,
            destination_bucket_name,
            destination_object_key,
            &claims.scopes,
            source_stream,
            ObjectWriteOptions {
                content_type: source_object.content_type,
                user_metadata: source_object.user_meta,
            },
        )
        .await
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

        let manager = self.clone();
        let source_claims = claims.clone();
        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            for source in sources {
                let source_stream = match manager
                    .get_object(
                        Some(source_claims.clone()),
                        source.bucket_name,
                        source.object_key,
                        source.version_id,
                    )
                    .await
                {
                    Ok((_object, stream)) => stream,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };

                futures_util::pin_mut!(source_stream);
                while let Some(chunk) = source_stream.next().await {
                    if tx.send(chunk).await.is_err() {
                        return;
                    }
                }
            }
        });

        self.put_object(
            claims.tenant_id,
            destination_bucket_name,
            destination_object_key,
            &claims.scopes,
            ReceiverStream::new(rx),
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
                        &self.encryption_key,
                        &link.target_key,
                        version_id,
                    )
                    .await
                }
                None => {
                    metadata_journal::read_current_object(
                        &self.storage,
                        bucket,
                        &self.encryption_key,
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
                &self.encryption_key,
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
        let bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
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

fn external_chunk_manifest_from_shard_map(value: &JsonValue) -> Option<ExternalChunkManifest> {
    let manifest = serde_json::from_value::<ExternalChunkManifest>(value.clone()).ok()?;
    (manifest.kind == "external_chunks_v1").then_some(manifest)
}

fn trim_s3_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
}
