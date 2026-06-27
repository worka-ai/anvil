use crate::{
    anvil_api::{
        CommitShardRequest, GetShardRequest, PutShardRequest, internal_anvil_service_client,
    },
    auth,
    cluster::ClusterState,
    permissions::AnvilAction,
    persistence::{
        Bucket, MultipartUploadPart, Object, ObjectVersion, ObjectWatchEvent, Persistence,
    },
    placement::PlacementManager,
    sharding::ShardManager,
    storage::Storage,
    validation,
};
use futures_util::{Stream, StreamExt};
use serde_json::Value as JsonValue;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;
use tracing::info;

#[derive(Debug, Clone)]
pub struct ObjectManager {
    db: Persistence,
    placer: PlacementManager,
    cluster: ClusterState,
    sharder: ShardManager,
    storage: Storage,
    region: String,
    jwt_manager: Arc<auth::JwtManager>,
    encryption_key: Vec<u8>,
    watch_tx: broadcast::Sender<ObjectWatchEvent>,
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
pub struct AppendStreamRecordResult {
    pub record_sequence: u64,
    pub payload_hash: String,
    pub payload_size: i64,
}

#[derive(Debug, Clone)]
pub struct SealAppendStreamResult {
    pub record_count: u64,
    pub segment_hash: String,
}

impl ObjectManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Persistence,
        placer: PlacementManager,
        cluster: ClusterState,
        sharder: ShardManager,
        storage: Storage,
        region: String,
        jwt_manager: Arc<auth::JwtManager>,
        anvil_secret_encryption_key: String,
        watch_tx: broadcast::Sender<ObjectWatchEvent>,
    ) -> Self {
        let encryption_key = hex::decode(anvil_secret_encryption_key)
            .expect("ANVIL_SECRET_ENCRYPTION_KEY must be a valid hex string");
        Self {
            db,
            placer,
            cluster,
            sharder,
            storage,
            region,
            jwt_manager,
            encryption_key,
            watch_tx,
        }
    }

    pub async fn put_object(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
        mut data_stream: impl Stream<Item = Result<Vec<u8>, Status>> + Unpin,
    ) -> Result<Object, Status> {
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
                info!("Committing whole object");
                self.storage
                    .commit_whole_object(&temp_path, &content_hash)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            } else {
                // No peers known; fallback to single-node path as well
                let (temp_path, bytes, hash) = self
                    .storage
                    .stream_to_temp_file(data_stream)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
                total_bytes = bytes;
                content_hash = hash;
                self.storage
                    .commit_whole_object(&temp_path, &content_hash)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
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

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
        }
        let shard_map_json = if nodes.len() > 1 {
            let peer_ids: Vec<String> = nodes.iter().map(|p| p.to_base58()).collect();
            Some(serde_json::json!(peer_ids))
        } else {
            None
        };

        let object = self
            .db
            .create_object(
                tenant_id,
                bucket.id,
                object_key,
                &content_hash,
                total_bytes as i64,
                &content_hash,
                shard_map_json,
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
    ) -> Result<uuid::Uuid, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;

        let upload = self
            .db
            .create_multipart_upload(tenant_id, bucket.id, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(upload.upload_id)
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
    ) -> Result<String, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        validate_multipart_part_number(part_number)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = self
            .db
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

        let part = self
            .db
            .upsert_multipart_part(
                upload.id,
                part_number,
                &content_hash,
                bytes as i64,
                &content_hash,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(part.etag)
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
            .db
            .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        let stored_parts = self
            .db
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
            )
            .await?;

        self.db
            .complete_multipart_upload(upload.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(object)
    }

    pub async fn abort_multipart_upload(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        upload_id: uuid::Uuid,
        scopes: &[String],
    ) -> Result<(), Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let aborted = self
            .db
            .abort_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if aborted {
            Ok(())
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
        scopes: &[String],
    ) -> Result<Vec<MultipartUploadPart>, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let upload = self
            .db
            .get_active_multipart_upload(tenant_id, bucket.id, object_key, upload_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Multipart upload not found"))?;
        self.db
            .list_multipart_parts(upload.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn list_multipart_uploads(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        limit: i32,
        scopes: &[String],
    ) -> Result<Vec<crate::persistence::MultipartUpload>, Status> {
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
        self.db
            .list_active_multipart_uploads(bucket.id, prefix, key_marker, limit)
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
        let snapshot = self
            .db
            .list_object_watch_events(claims.tenant_id, bucket.id, prefix, after_cursor, 1000)
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
    ) -> Result<uuid::Uuid, Status> {
        self.validate_write_request(bucket_name, stream_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        let stream = self
            .db
            .create_append_stream(tenant_id, bucket.id, &bucket.name, stream_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(stream.stream_id)
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
            .db
            .get_active_append_stream(tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;

        let payload_hash = blake3::hash(&payload).to_hex().to_string();
        self.storage
            .store_whole_object(&payload_hash, &payload)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let record = self
            .db
            .append_stream_record(stream.id, &payload_hash, payload.len() as i64)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(AppendStreamRecordResult {
            record_sequence: u64::try_from(record.record_sequence)
                .map_err(|_| Status::internal("Invalid record sequence"))?,
            payload_hash,
            payload_size: payload.len() as i64,
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
            .db
            .get_active_append_stream(tenant_id, bucket.id, stream_key, stream_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Append stream not found"))?;
        let records = self
            .db
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
            .db
            .seal_append_stream(stream.id, &segment_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if !sealed {
            return Err(Status::failed_precondition(
                "Append stream is already sealed",
            ));
        }

        Ok(SealAppendStreamResult {
            record_count: records.len() as u64,
            segment_hash,
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
        if !validation::is_valid_bucket_name(&bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(&object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(&object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), &bucket_name)
            .await?;

        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(
                AnvilAction::ObjectRead,
                &format!("{}/{}", bucket_name, object_key),
                &claims.scopes,
            ) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let object = match version_id {
            Some(version_id) => {
                let object = self
                    .db
                    .get_object_version(bucket.id, &object_key, version_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => self
                .db
                .get_object(bucket.id, &object_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object not found"))?,
        };

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();
        let object_clone = object.clone();

        tokio::spawn(async move {
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

        Ok((object, Box::pin(ReceiverStream::new(rx))))
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

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
        }

        let delete_marker = self
            .db
            .soft_delete_object(bucket.id, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        self.publish_object_watch_event(tenant_id, &bucket, &delete_marker, "delete", true)
            .await?;

        Ok(delete_marker)
    }

    pub async fn head_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
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

        // Allow public buckets to bypass auth; otherwise require appropriate scope
        let bucket = self
            .get_authorized_bucket(claims.as_ref(), bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(
                AnvilAction::ObjectRead,
                &format!("{}/{}", bucket_name, object_key),
                &claims.scopes,
            ) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        match version_id {
            Some(version_id) => {
                let object = self
                    .db
                    .get_object_version(bucket.id, object_key, version_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                Ok(object)
            }
            None => self
                .db
                .get_object(bucket.id, object_key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object not found")),
        }
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
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }

        // Allow public buckets to bypass auth; otherwise require appropriate scope
        let bucket = self
            .get_authorized_bucket(claims.as_ref(), bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        self.db
            .list_objects(
                bucket.id,
                prefix,
                start_after,
                if limit == 0 { 1000 } else { limit },
                delimiter,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn list_object_versions(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        limit: i32,
    ) -> Result<Vec<ObjectVersion>, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        self.db
            .list_object_versions(
                bucket.id,
                prefix,
                key_marker,
                if limit == 0 { 1000 } else { limit },
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
        let (_source_object, source_stream) = self
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
        )
        .await
    }

    async fn get_authorized_bucket(
        &self,
        claims: Option<&auth::Claims>,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        let bucket = match claims {
            Some(c) => self
                .db
                .get_bucket_by_name(c.tenant_id, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found for this tenant")),
            None => self
                .db
                .get_public_bucket_by_name(bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Public bucket not found")),
        }?;

        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
        }

        Ok(bucket)
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
            .db
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
        let _ = self.watch_tx.send(event);
        Ok(())
    }

    fn validate_write_request(
        &self,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
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
        if !auth::is_authorized(
            AnvilAction::ObjectWrite,
            &format!("{}/{}", bucket_name, object_key),
            scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }
        Ok(())
    }

    async fn get_tenant_bucket(&self, tenant_id: i64, bucket_name: &str) -> Result<Bucket, Status> {
        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name)
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

fn trim_s3_etag(value: &str) -> &str {
    value.trim().trim_matches('"')
}
