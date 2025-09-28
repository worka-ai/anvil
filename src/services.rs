use crate::anvil_api::auth_service_server::AuthService;
use crate::anvil_api::bucket_service_server::BucketService;
use crate::anvil_api::internal_anvil_service_server::InternalAnvilService;
use crate::anvil_api::object_service_server::ObjectService;
use crate::anvil_api::*;
use crate::AppState;
use futures_util::StreamExt;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::auth;

#[tonic::async_trait]
impl AuthService for AppState {
    async fn get_access_token(
        &self,
        request: Request<GetAccessTokenRequest>,
    ) -> Result<Response<GetAccessTokenResponse>, Status> {
        let req = request.into_inner();

        // 1. Verify credentials
        let app_details = self
            .db
            .get_app_by_client_id(&req.client_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::unauthenticated("Invalid client ID"))?;

        if !auth::verify_secret(&req.client_secret, &app_details.client_secret_hash) {
            return Err(Status::unauthenticated("Invalid client secret"));
        }

        let allowed_scopes = self
            .db
            .get_policies_for_app(app_details.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let approved_scopes = if req.scopes.is_empty() {
            allowed_scopes
        } else {
            req.scopes
                .into_iter()
                .filter(|requested_scope| auth::is_authorized(requested_scope, &allowed_scopes))
                .collect()
        };

        if approved_scopes.is_empty() {
            return Err(Status::permission_denied("App has no assigned policies"));
        }

        // 3. Mint token
        let token = self
            .jwt_manager
            .mint_token(app_details.id.to_string(), approved_scopes)
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GetAccessTokenResponse {
            access_token: token,
            expires_in: 3600,
        }))
    }

    async fn grant_access(
        &self,
        request: Request<GrantAccessRequest>,
    ) -> Result<Response<GrantAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        if !auth::is_authorized(&format!("grant:{}", req.resource), &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to grant access to this resource",
            ));
        }

        let app = self
            .db
            .get_app_by_name(&req.grantee_app_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Grantee app not found"))?;
        self.db
            .grant_policy(app.id, &req.resource, &req.action)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(GrantAccessResponse {}))
    }

    async fn revoke_access(
        &self,
        _request: Request<RevokeAccessRequest>,
    ) -> Result<Response<RevokeAccessResponse>, Status> {
        // Implementation would be similar to grant_access
        todo!()
    }

    async fn set_public_access(
        &self,
        request: Request<SetPublicAccessRequest>,
    ) -> Result<Response<SetPublicAccessResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let resource = format!("bucket:{}", req.bucket);
        if !auth::is_authorized(&format!("grant:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied(
                "Permission denied to modify public access on this bucket",
            ));
        }

        self.db
            .set_bucket_public_access(&req.bucket, req.allow_public_read)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(SetPublicAccessResponse {}))
    }
}

#[tonic::async_trait]
impl InternalAnvilService for AppState {
    type GetShardStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<GetShardResponse, Status>> + Send>>;

    async fn put_shard(
        &self,
        request: Request<tonic::Streaming<PutShardRequest>>,
    ) -> Result<Response<PutShardResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?.clone();

        let mut stream = request.into_inner();

        let (upload_id, shard_index, first_chunk_data) = if let Some(Ok(chunk)) = stream.next().await
        {
            (chunk.upload_id, chunk.shard_index, chunk.data)
        } else {
            return Err(Status::invalid_argument("Empty stream"));
        };

        let resource = format!("{}/{}", upload_id, shard_index);
        if !auth::is_authorized(&format!("internal:put_shard:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let mut data = first_chunk_data;
        while let Some(Ok(chunk)) = stream.next().await {
            data.extend_from_slice(&chunk.data);
        }

        self.storage
            .store_temp_shard(&upload_id, shard_index, &data)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(PutShardResponse {}))
    }

    async fn commit_shard(
        &self,
        request: Request<CommitShardRequest>,
    ) -> Result<Response<CommitShardResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?.clone();
        let req = request.into_inner();

        let resource = format!("{}/{}", req.final_object_hash, req.shard_index);
        if !auth::is_authorized(&format!("internal:commit_shard:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        self.storage
            .commit_shard(&req.upload_id, req.shard_index, &req.final_object_hash)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CommitShardResponse {}))
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<Self::GetShardStream>, Status> {
        let req = request.into_inner();
        let data = self
            .storage
            .retrieve_shard(&req.object_hash, req.shard_index)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let (tx, rx) = mpsc::channel(4);

        tokio::spawn(async move {
            for chunk in data.chunks(1024 * 1024) {
                // 1MB chunks
                tx.send(Ok(GetShardResponse { data: chunk.to_vec() }))
                    .await
                    .unwrap();
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(
            Box::pin(output_stream) as Self::GetShardStream
        ))
    }
}

#[tonic::async_trait]
impl BucketService for AppState {
    async fn create_bucket(
        &self,
        request: Request<CreateBucketRequest>,
    ) -> Result<Response<CreateBucketResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.get_ref();

        let resource = format!("bucket:{}", req.bucket_name);
        if !auth::is_authorized(&format!("write:{}", resource), &claims.scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }

        println!("gRPC - Create Bucket: {:?}", req);

        let tenant_id = 1; // Placeholder

        let db_result = self
            .db
            .create_bucket(tenant_id, &req.bucket_name, &req.region)
            .await;

        println!("DB create_bucket result: {:?}", db_result);

        db_result.map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(CreateBucketResponse {}))
    }

    async fn delete_bucket(
        &self,
        _request: Request<DeleteBucketRequest>,
    ) -> Result<Response<DeleteBucketResponse>, Status> {
        todo!()
    }

    async fn list_buckets(
        &self,
        _request: Request<ListBucketsRequest>,
    ) -> Result<Response<ListBucketsResponse>, Status> {
        todo!()
    }

    async fn get_bucket_policy(
        &self,
        _request: Request<GetBucketPolicyRequest>,
    ) -> Result<Response<GetBucketPolicyResponse>, Status> {
        todo!()
    }

    async fn put_bucket_policy(
        &self,
        _request: Request<PutBucketPolicyRequest>,
    ) -> Result<Response<PutBucketPolicyResponse>, Status> {
        todo!()
    }
}

#[tonic::async_trait]
impl ObjectService for AppState {
    type GetObjectStream =
        std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<GetObjectResponse, Status>> + Send>>;

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
                Some(put_object_request::Data::Metadata(meta)) => (meta.bucket_name, meta.object_key),
                _ => return Err(Status::invalid_argument("First chunk must be metadata")),
            },
            _ => return Err(Status::invalid_argument("Empty stream")),
        };
        let upload_id = uuid::Uuid::new_v4().to_string();

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
                        .mint_token("internal".to_string(), vec![scope])
                        .unwrap();

                    let request = PutShardRequest {
                        upload_id: upload_id.clone(),
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
                        .mint_token("internal".to_string(), vec![scope])
                        .unwrap();

                    let request = PutShardRequest {
                        upload_id: upload_id.clone(),
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
                    .map_err(|e| Status::internal(format!("Failed to send final shard: {}", e)))?;
            }

            // Commit the shards
            let mut futures = Vec::new();
            for (i, client) in clients.into_iter().enumerate() {
                let scope = format!("internal:commit_shard:{}/{}", content_hash, i);
                let token = self
                    .jwt_manager
                    .mint_token("internal".to_string(), vec![scope])
                    .unwrap();

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
                .map_err(|e| Status::internal(format!("Failed to commit shard: {}", e)))?;
        }

        // Commit metadata to DB
        let bucket = self
            .db
            .get_bucket_by_name(1, &bucket_name, &self.region)
            .await
            .unwrap()
            .unwrap();
        let object = self
            .db
            .create_object(
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

        // 1. Look up object metadata
        let tenant_id = 1; // Placeholder
        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, &req.bucket_name, &self.region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        // 2. Authorization Check
        if !bucket.is_public_read {
            let claims = claims.ok_or_else(|| Status::unauthenticated("Missing claims"))?;
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
        _request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>, Status> {
        todo!()
    }

    async fn head_object(
        &self,
        _request: Request<HeadObjectRequest>,
    ) -> Result<Response<HeadObjectResponse>, Status> {
        todo!()
    }
    async fn list_objects(
        &self,
        request: Request<ListObjectsRequest>,
    ) -> Result<Response<ListObjectsResponse>, Status> {
        let req = request.into_inner();
        let tenant_id = 1; // Placeholder

        let bucket = self
            .db
            .get_bucket_by_name(tenant_id, &req.bucket_name, &self.region)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        let objects = self
            .db
            .list_objects(
                bucket.id,
                &req.prefix,
                &req.start_after,
                std::cmp::max(req.max_keys, 1),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        let summaries = objects
            .into_iter()
            .map(|o| ObjectSummary {
                key: o.key,
                size: o.size,
                etag: o.etag,
                last_modified: o.created_at.to_string(),
            })
            .collect();

        Ok(Response::new(ListObjectsResponse {
            objects: summaries,
            common_prefixes: vec![], // Delimiter logic not yet implemented
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
