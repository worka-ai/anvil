use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::tasks::TaskType;
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

        // 1. Get metadata and generate Upload ID
        let (bucket_name, object_key) = match stream.next().await {
            Some(Ok(chunk)) => match chunk.data {
                Some(put_object_request::Data::Metadata(meta)) => {
                    (meta.bucket_name, meta.object_key)
                }
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            _ => return Err(Status::invalid_argument("Empty stream")),
        };

        let resource = format!("bucket:{}/{}", bucket_name, object_key);
        if !auth::is_authorized(&format!("write:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        // 2. Determine placement
        let nodes = self
            .placer
            .calculate_placement(&object_key, &self.cluster, self.sharder.total_shards())
            .await;

        // 4. Stream data, shard it, and distribute it
        let mut overall_hasher = blake3::Hasher::new();
        let mut buffer = Vec::new();

        while let Some(Ok(chunk)) = stream.next().await {
            if let Some(put_object_request::Data::Chunk(bytes)) = chunk.data {
                buffer.extend_from_slice(&bytes);
                overall_hasher.update(&bytes);
            }
        }
        let total_bytes = buffer.len();
        let content_hash = overall_hasher.finalize().to_hex().to_string();

        if nodes.len() < self.sharder.total_shards() {
            if nodes.len() == 1 {
                // Single node mode: store whole object locally
                self.storage
                    .store_whole_object(&content_hash, &buffer)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?;
            } else {
                return Err(Status::unavailable("Not enough nodes to store object"));
            }
        } else {
            // Sharding mode for cluster
            let upload_id = uuid::Uuid::new_v4().to_string();
            let stripe_size = 1024 * 64; // 64KB per shard in a stripe
            let data_shards_count = self.sharder.data_shards();

            // Create clients for each target node
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
                .map_err(|e| {
                    Status::internal(format!("Failed to connect to peer {}: {}", peer_id, e))
                })?;
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
                self.sharder
                    .encode(&mut shards)
                    .map_err(|e| Status::internal(e.to_string()))?;

                let mut futures = Vec::new();
                for (i, shard_data) in shards.into_iter().enumerate() {
                    let scope = format!("internal:put_shard:{}/{}", upload_id, i);
                    let token = self
                        .jwt_manager
                        .mint_token("internal".to_string(), vec![scope], 0)
                        .unwrap();

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
                futures::future::try_join_all(futures)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to send shard: {}", e)))?;
            }

            // Handle final partial stripe
            if !temp_buffer.is_empty() {
                let final_stripe_size = stripe_size * data_shards_count;
                temp_buffer.resize(final_stripe_size, 0);

                let mut shards: Vec<Vec<u8>> = temp_buffer
                    .chunks(stripe_size)
                    .map(|c| c.to_vec())
                    .collect();
                shards.resize(self.sharder.total_shards(), vec![0; stripe_size]);
                self.sharder
                    .encode(&mut shards)
                    .map_err(|e| Status::internal(e.to_string()))?;

                let mut futures = Vec::new();
                for (i, shard_data) in shards.into_iter().enumerate() {
                    let scope = format!("internal:put_shard:{}/{}", upload_id, i);
                    let token = self
                        .jwt_manager
                        .mint_token("internal".to_string(), vec![scope], 0)
                        .unwrap();

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
                futures::future::try_join_all(futures)
                    .await
                    .map_err(|e| Status::internal(format!("Failed to send final shard: {}", e)))?;
            }

            // Commit the shards
            let mut futures = Vec::new();
            for (i, client) in clients.into_iter().enumerate() {
                let scope = format!("internal:commit_shard:{}/{}", content_hash, i);
                let token = self
                    .jwt_manager
                    .mint_token("internal".to_string(), vec![scope], 0)
                    .unwrap();

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
                .map_err(|e| Status::internal(format!("Failed to commit shard: {}", e)))?;
        }

        // Commit metadata to DB
        let bucket = self
            .db
            .get_bucket_by_name(claims.tenant_id, &bucket_name, &self.region)
            .await
            .unwrap()
            .unwrap();
        let object = self
            .db
            .create_object(
                claims.tenant_id,
                bucket.id,
                &object_key,
                &content_hash,
                total_bytes as i64,
                &content_hash,
            )
            .await
            .unwrap();

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

        // 1. Look up bucket, handling anonymous vs authenticated access differently.
        let bucket = match &claims {
            Some(c) => {
                // Authenticated user: look up bucket by tenant and name.
                self.db
                    .get_bucket_by_name(c.tenant_id, &req.bucket_name, &self.region)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Bucket not found"))?
            }
            None => {
                // Anonymous user: look for a bucket that is explicitly public.
                self.db
                    .get_public_bucket_by_name(&req.bucket_name)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Bucket not found"))?
            }
        };

        // 2. Authorization Check
        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| {
                //we know it is an authorisation error but best practice says we shouldn't reveal its existence at all
                Status::not_found("Bucket not found")
            })?;
            let resource = format!("bucket:{}/{}", req.bucket_name, req.object_key);
            if !auth::is_authorized(&format!("read:{}", resource), &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let object = self
            .db
            .get_object(bucket.id, &req.object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();

        tokio::spawn(async move {
            // 2. Send metadata header
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

            // 3. On-the-fly reconstruction and streaming
            // Check if a whole object exists first (single-node case)
            if let Ok(full_data) = app_state
                .storage
                .retrieve_whole_object(&object.content_hash)
                .await
            {
                // Stream the result back in chunks
                for chunk in full_data.chunks(1024 * 64) {
                    if tx
                        .send(Ok(GetObjectResponse {
                            data: Some(get_object_response::Data::Chunk(chunk.to_vec())),
                        }))
                        .await
                        .is_err()
                    {
                        // Client disconnected
                        break;
                    }
                }
            } else {
                // Otherwise, proceed with shard reconstruction
                let total_shards = app_state.sharder.total_shards();
                let mut shards = Vec::with_capacity(total_shards);
                for i in 0..total_shards {
                    let shard_data = app_state
                        .storage
                        .retrieve_shard(&object.content_hash, i as u32)
                        .await
                        .ok();
                    shards.push(shard_data);
                }

                if app_state.sharder.reconstruct(&mut shards).is_ok() {
                    let mut full_data = Vec::new();
                    let data_shards = &shards[..app_state.sharder.data_shards()];
                    for data_shard_opt in data_shards {
                        if let Some(shard_data) = data_shard_opt {
                            full_data.extend_from_slice(shard_data);
                        } else {
                            let _ = tx.send(Err(Status::internal("Failed to reconstruct data: a data shard was missing after successful reconstruction call."))).await;
                            return;
                        }
                    }

                    // Truncate to the original object size to remove padding
                    full_data.truncate(object.size as usize);

                    // Stream the result back in chunks
                    for chunk in full_data.chunks(1024 * 64) {
                        // 64KB chunks
                        if tx
                            .send(Ok(GetObjectResponse {
                                data: Some(get_object_response::Data::Chunk(chunk.to_vec())),
                            }))
                            .await
                            .is_err()
                        {
                            // Client disconnected
                            break;
                        }
                    }
                } else {
                    let _ = tx
                        .send(Err(Status::internal(
                            "Failed to reconstruct data from shards.",
                        )))
                        .await;
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
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();
        let req = request.into_inner();

        let resource = format!("bucket:{}/{}", req.bucket_name, req.object_key);
        if !auth::is_authorized(&format!("write:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let tenant_id = claims.tenant_id;
        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, &req.bucket_name, &self.region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        // Soft-delete the object
        let object = self
            .db
            .soft_delete_object(bucket.id, &req.object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        // Enqueue a task for physical deletion
        let payload = serde_json::json!({
            "content_hash": object.content_hash,
            "region": self.region,
            "shard_map": object.shard_map,
        });
        self.db
            .enqueue_task(TaskType::DeleteObject, payload, 100)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(DeleteObjectResponse {}))
    }
    async fn head_object(
        &self,
        request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();
        let req = request.into_inner();

        let resource = format!("bucket:{}/{}", req.bucket_name, req.object_key);
        if !auth::is_authorized(&format!("read:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let tenant_id = claims.tenant_id;
        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, &req.bucket_name, &self.region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        let object = self
            .db
            .get_object(bucket.id, &req.object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

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
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();

        let resource = format!("bucket:{}", req.bucket_name);
        if !auth::is_authorized(&format!("read:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self
            .db
            .get_bucket_by_name(claims.tenant_id, &req.bucket_name, &self.region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        let (objects, common_prefixes) = self
            .db
            .list_objects(
                bucket.id,
                &req.prefix,
                &req.start_after,
                if req.max_keys == 0 {
                    1000
                } else {
                    req.max_keys
                },
                &req.delimiter,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

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
