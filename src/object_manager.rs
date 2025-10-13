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

        let total_bytes; 
        let content_hash;

        if nodes.len() < self.sharder.total_shards() {
            if nodes.len() == 1 {
                // Single-node case: stream to a whole file.
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
            } else {
                return Err(Status::unavailable("Not enough nodes to store object with durability"));
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
                let client = internal_anvil_service_client::InternalAnvilServiceClient::connect(
                    peer_info.grpc_addr.clone(),
                )
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
                    let stripe_data = stripe_buffer.drain(..stripe_buffer_size).collect::<Vec<_>>();
                    self.send_stripe(&clients, &upload_id, stripe_data, stripe_size).await?;
                }
            }
    
            if !stripe_buffer.is_empty() {
                stripe_buffer.resize(stripe_buffer_size, 0);
                self.send_stripe(&clients, &upload_id, stripe_buffer, stripe_size).await?;
            }
    
            total_bytes = bytes_so_far as i64;
            content_hash = overall_hasher.finalize().to_hex().to_string();
    
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
                req.metadata_mut()
                    .insert("authorization", format!("Bearer {}", token).parse().unwrap());
                futures.push(async move { client.commit_shard(req).await });
            }
            futures::future::try_join_all(futures)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, bucket_name, &self.region)
            .await.map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;
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
            .await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(object)
    }

    async fn send_stripe(
        &self,
        clients: &[internal_anvil_service_client::InternalAnvilServiceClient<tonic::transport::Channel>],
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
            let scope = format!("internal:put_shard:{}/{}", upload_id, i);
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
            req.metadata_mut()
                .insert("authorization", format!("Bearer {}", token).parse().unwrap());

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

                let placement = app_state
                    .placer
                    .calculate_placement(&object_clone.key, &app_state.cluster, total_shards)
                    .await;

                let mut missing_shards_futures = Vec::new();

                for i in 0..total_shards {
                    if shards[i].is_none() {
                        let peer_id = placement.get(i).ok_or_else(|| {
                            Status::internal("Placement did not return enough peers for reconstruction")
                        })?;

                        let cluster_map = app_state.cluster.read().await;
                        if let Some(peer_info) = cluster_map.get(peer_id) {
                            let grpc_addr = peer_info.grpc_addr.clone();
                            let object_hash = object_clone.content_hash.clone();
                            let jwt_manager = app_state.jwt_manager.clone();

                            missing_shards_futures.push(async move {
                                let mut client = internal_anvil_service_client::InternalAnvilServiceClient::connect(grpc_addr)
                                    .await
                                    .map_err(|e| Status::internal(format!("Failed to connect to peer: {}", e)))?;

                                let scope = format!("internal:get_shard:{}/{}", object_hash, i);
                                let token = jwt_manager.mint_token("internal".to_string(), vec![scope], 0)
                                    .map_err(|e| Status::internal(e.to_string()))?;

                                let mut req = tonic::Request::new(GetShardRequest {
                                    object_hash: object_hash.clone(),
                                    shard_index: i as u32,
                                });
                                req.metadata_mut().insert("authorization", format!("Bearer {}", token).parse().unwrap());

                                let mut stream = client.get_shard(req).await?.into_inner();
                                let mut shard_data = Vec::new();
                                while let Some(Ok(chunk)) = stream.next().await {
                                    shard_data.extend_from_slice(&chunk.data);
                                }

                                Ok((i, shard_data))
                            });
                        }
                    }
                }

                let results = futures::future::join_all(missing_shards_futures).await;
                for result in results {
                    if let Ok(Ok((index, data))) = result {
                        shards[index] = Some(data);
                    }
                }

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
            "object_id": object.id,
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