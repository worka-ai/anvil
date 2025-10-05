use crate::{
    anvil_api::{internal_anvil_service_client, CommitShardRequest, PutShardRequest},
    auth,
    cluster::ClusterState,
    persistence::{Bucket, Object, Persistence},
    placement::PlacementManager,
    sharding::ShardManager,
    storage::Storage,
    tasks::TaskType,
    validation,
};
use futures_util::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Status;

#[derive(Clone)]
pub struct ObjectManager {
    db: Persistence,
    placer: PlacementManager,
    cluster: ClusterState,
    sharder: ShardManager,
    storage: Storage,
    region: String,
    jwt_manager: Arc<auth::JwtManager>,
    encryption_key: Vec<u8>,
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
        worka_secret_encryption_key: String,
    ) -> Self {
        let encryption_key = hex::decode(worka_secret_encryption_key).expect("WORKA_SECRET_ENCRYPTION_KEY must be a valid hex string");
        Self {
            db,
            placer,
            cluster,
            sharder,
            storage,
            region,
            jwt_manager,
            encryption_key,
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
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let resource = format!("bucket:{}/{}", bucket_name, object_key);
        if !auth::is_authorized(&format!("write:{}", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let nodes = self
            .placer
            .calculate_placement(object_key, &self.cluster, self.sharder.total_shards())
            .await;

        let mut overall_hasher = blake3::Hasher::new();
        let mut buffer = Vec::new();

        while let Some(Ok(chunk)) = data_stream.next().await {
            buffer.extend_from_slice(&chunk);
            overall_hasher.update(&chunk);
        }
        let total_bytes = buffer.len();
        let content_hash = overall_hasher.finalize().to_hex().to_string();

        if nodes.len() < self.sharder.total_shards() {
            if nodes.len() == 1 {
                self.storage
                    .store_whole_object(&content_hash, &buffer)
                    .await.map_err(|e| Status::internal(e.to_string()))?;
            } else {
                return Err(Status::unavailable("Not enough nodes to store object"));
            }
        } else {
            let upload_id = uuid::Uuid::new_v4().to_string();
            let stripe_size = 1024 * 64; // 64KB per shard in a stripe
            let data_shards_count = self.sharder.data_shards();

            let mut clients = Vec::new();
            let cluster_map = self.cluster.read().await;
            for peer_id in &nodes {
                let peer_info = cluster_map.get(peer_id).ok_or_else(|| {
                    Status::internal("Placement selected a peer that is not in the cluster state")
                })?;
                let client = internal_anvil_service_client::InternalAnvilServiceClient::connect(
                    peer_info.grpc_addr.clone(),
                )
                .await.map_err(|e| Status::unavailable(e.to_string()))?;
                clients.push(client);
            }

            let mut temp_buffer = buffer.clone();
            while temp_buffer.len() >= stripe_size * data_shards_count {
                let stripe_data = temp_buffer
                    .drain(..stripe_size * data_shards_count)
                    .collect::<Vec<_>>();
                let mut shards: Vec<Vec<u8>> = stripe_data
                    .chunks(stripe_size)
                    .map(|c| c.to_vec())
                    .collect();
                shards.resize(self.sharder.total_shards(), vec![0; stripe_size]);
                self.sharder.encode(&mut shards, &self.encryption_key).map_err(|e| Status::internal(e.to_string()))?;

                let mut futures = Vec::new();
                for (i, shard_data) in shards.into_iter().enumerate() {
                    let scope = format!("internal:put_shard:{}/{}", upload_id, i);
                    let token = self
                        .jwt_manager
                        .mint_token("internal".to_string(), vec![scope], 0)
                        .map_err(|e| Status::internal(e.to_string()))?;

                    let request = PutShardRequest {
                        upload_id: upload_id.clone(),
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
                futures::future::try_join_all(futures).await.map_err(|e| Status::internal(e.to_string()))?;
            }

            if !temp_buffer.is_empty() {
                let final_stripe_size = stripe_size * data_shards_count;
                temp_buffer.resize(final_stripe_size, 0);

                let mut shards: Vec<Vec<u8>> = temp_buffer
                    .chunks(stripe_size)
                    .map(|c| c.to_vec())
                    .collect();
                shards.resize(self.sharder.total_shards(), vec![0; stripe_size]);
                self.sharder.encode(&mut shards, &self.encryption_key).map_err(|e| Status::internal(e.to_string()))?;

                let mut futures = Vec::new();
                for (i, shard_data) in shards.into_iter().enumerate() {
                    let scope = format!("internal:put_shard:{}/{}", upload_id, i);
                    let token = self
                        .jwt_manager
                        .mint_token("internal".to_string(), vec![scope], 0)
                        .map_err(|e| Status::internal(e.to_string()))?;

                    let request = PutShardRequest {
                        upload_id: upload_id.clone(),
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
                futures::future::try_join_all(futures).await.map_err(|e| Status::internal(e.to_string()))?;
            }

            let mut futures = Vec::new();
            for (i, client) in clients.into_iter().enumerate() {
                let scope = format!("internal:commit_shard:{}/{}", content_hash, i);
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
            futures::future::try_join_all(futures).await.map_err(|e| Status::internal(e.to_string()))?;
        }

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name, &self.region)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
        let object = self
            .db
            .create_object(
                tenant_id,
                bucket.id,
                object_key,
                &content_hash,
                total_bytes as i64,
                &content_hash,
            )
            .await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(object)
    }

    pub async fn get_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: String,
        object_key: String,
    ) -> Result<(
        Object,
        Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
    ), Status> {
        let bucket = self
            .get_authorized_bucket(claims.as_ref(), &bucket_name)
            .await?;

        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
            let resource = format!("bucket:{}/{}", bucket_name, object_key);
            if !auth::is_authorized(&format!("read:{}", resource), &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let object = self
            .db
            .get_object(bucket.id, &object_key)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();
        let object_clone = object.clone();

        tokio::spawn(async move {
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
            } else {
                let total_shards = app_state.sharder.total_shards();
                let mut shards = Vec::with_capacity(total_shards);
                for i in 0..total_shards {
                    let shard_data = app_state
                        .storage
                        .retrieve_shard(&object_clone.content_hash, i as u32)
                        .await
                        .ok();
                    shards.push(shard_data);
                }

                // TODO: Fetch missing shards from other peers.
                // The current implementation only tries to reconstruct from local shards.

                if app_state.sharder.reconstruct(&mut shards, &app_state.encryption_key).is_ok() {
                    let mut full_data = Vec::new();
                    let data_shards = &shards[..app_state.sharder.data_shards()];
                    for data_shard_opt in data_shards {
                        if let Some(shard_data) = data_shard_opt {
                            full_data.extend_from_slice(shard_data);
                        } else {
                            let _ = tx
                                .send(Err(Status::internal(
                                    "Failed to reconstruct data: a data shard was missing after successful reconstruction call."
                                )))
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
                } else {
                    let _ = tx
                        .send(Err(Status::internal(
                            "Failed to reconstruct data from shards."
                        )))
                        .await;
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
    ) -> Result<(), Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let resource = format!("bucket:{}/{}", bucket_name, object_key);
        if !auth::is_authorized(&format!("write:{}", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name, &self.region)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        let object = self
            .db
            .soft_delete_object(bucket.id, object_key)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        let payload = serde_json::json!({
            "content_hash": object.content_hash,
            "region": self.region,
            "shard_map": object.shard_map,
        });
        self.db
            .enqueue_task(TaskType::DeleteObject, payload, 100)
            .await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(())
    }

    pub async fn head_object(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<Object, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let resource = format!("bucket:{}/{}", bucket_name, object_key);
        if !auth::is_authorized(&format!("read:{}", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name, &self.region)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        self.db
            .get_object(bucket.id, object_key)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))
    }

    pub async fn list_objects(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
        scopes: &[String],
    ) -> Result<(Vec<Object>, Vec<String>), Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }

        let resource = format!("bucket:{}", bucket_name);
        if !auth::is_authorized(&format!("read:{}", resource), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name, &self.region)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        self.db
            .list_objects(
                bucket.id,
                prefix,
                start_after,
                if limit == 0 { 1000 } else { limit },
                delimiter,
            )
            .await.map_err(|e| Status::internal(e.to_string()))
    }

    async fn get_authorized_bucket(
        &self,
        claims: Option<&auth::Claims>,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        match claims {
            Some(c) => self
                .db
                .get_bucket_by_name(c.tenant_id, bucket_name, &self.region)
                .await.map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Bucket not found for this tenant")),
            None => self
                .db
                .get_public_bucket_by_name(bucket_name)
                .await.map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Public bucket not found")),
        }
    }
}