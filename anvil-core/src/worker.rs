use crate::anvil_api::DeleteShardRequest;
use crate::anvil_api::internal_anvil_service_client::InternalAnvilServiceClient;
use crate::auth::JwtManager;
use crate::cluster::ClusterState;
use crate::object_manager::ObjectManager;
use crate::persistence::Persistence;
use crate::tasks::{HFIngestionItemState, HFIngestionState, TaskStatus, TaskType};
use crate::persistence::Object;
use anyhow::{Result, anyhow};
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::convert::Infallible;
use std::boxed::Box;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::Row;
use tonic::Status;
use tracing::{debug, error, info, warn};
use futures_util::{StreamExt, Stream};

#[derive(Debug)]
struct Task {
    id: i64,
    task_type: TaskType,
    payload: JsonValue,
    _attempts: i32,
}

impl TryFrom<Row> for Task {
    type Error = anyhow::Error;

    fn try_from(row: Row) -> Result<Self, Self::Error> {
        let task_type_str: &str = row.get("task_type");
        let task_type = match task_type_str {
            "DELETE_OBJECT" => TaskType::DeleteObject,
            "DELETE_BUCKET" => TaskType::DeleteBucket,
            "REBALANCE_SHARD" => TaskType::RebalanceShard,
            "HF_INGESTION" => TaskType::HFIngestion,
            _ => return Err(anyhow!("Unknown task type: {}", task_type_str)),
        };

        Ok(Self {
            id: row.get("id"),
            task_type,
            payload: row.get("payload"),
            _attempts: row.get("attempts"),
        })
    }
}

#[derive(Deserialize)]
struct DeleteObjectPayload {
    object_id: i64,
    content_hash: String,
    shard_map: Option<Vec<String>>,
}

pub async fn run(
    persistence: Persistence,
    cluster_state: ClusterState,
    jwt_manager: Arc<JwtManager>,
    object_manager: ObjectManager,
) -> Result<()> {
    loop {
        let tasks = match persistence.fetch_pending_tasks_for_update(10).await {
            Ok(rows) => rows
                .into_iter()
                .map(Task::try_from)
                .collect::<Result<Vec<_>>>()?,
            Err(e) => {
                error!("Failed to fetch tasks: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if tasks.is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }

        for task in tasks {
            let p = persistence.clone();
            let cs = cluster_state.clone();
            let jm = jwt_manager.clone();
            let om = object_manager.clone();
            tokio::spawn(async move {
                if let Err(e) = p.update_task_status(task.id, TaskStatus::Running).await {
                    error!("Failed to mark task {} as running: {}", task.id, e);
                    return;
                }

                let result = match task.task_type {
                    TaskType::DeleteObject => handle_delete_object(&p, &cs, &jm, &task).await,
                    TaskType::HFIngestion => handle_hf_ingestion(&p, &om, &task).await,
                    _ => {
                        warn!("Unhandled task type: {:?}", task.task_type);
                        Ok(())
                    }
                };

                if let Err(e) = result {
                    error!("Task {} failed: {:?}", task.id, e);
                    if let Err(fail_err) = p.fail_task(task.id, &e.to_string()).await {
                        error!("Failed to mark task {} as failed: {:?}", task.id, fail_err);
                    }
                } else {
                    if let Err(complete_err) =
                        p.update_task_status(task.id, TaskStatus::Completed).await
                    {
                        error!(
                            "Failed to mark task {} as completed: {}",
                            task.id, complete_err
                        );
                    }
                }
            });
        }
    }
}

async fn handle_hf_ingestion(
    persistence: &Persistence,
    object_manager: &ObjectManager,
    task: &Task,
) -> anyhow::Result<()> {
    use globset::{Glob, GlobSetBuilder};
    use hf_hub::{Repo, RepoType, api::sync::ApiBuilder};

    let ingestion_id: i64 = task
        .payload
        .get("ingestion_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("missing ingestion_id"))?;

    // Wrap the main logic in a closure to ensure we can catch errors and update the final status.
    let result =
        async {
            info!(
                ingestion_id,
                "Starting ingestion task."
            );

            persistence
                .hf_update_ingestion_state(ingestion_id, HFIngestionState::Running, None)
                .await?;

            let client = persistence.get_global_pool().get().await?;
            let job = client
                .query_one(
                    "SELECT key_id, tenant_id, requester_app_id, repo, COALESCE(revision,'main'), target_bucket, target_region, COALESCE(target_prefix,''), include_globs, exclude_globs FROM hf_ingestions WHERE id=$1",
                    &[&ingestion_id],
                )
                .await?;
            let key_id: i64 = job.get(0);
            let tenant_id: i64 = job.get(1);
            let _requester_app_id: i64 = job.get(2);
            let repo_str: String = job.get(3);
            let revision: String = job.get(4);
            let target_bucket: String = job.get(5);
            let _target_region: String = job.get(6);
            let target_prefix: String = job.get(7);
            let include_globs: Vec<String> = job.get(8);
            let exclude_globs: Vec<String> = job.get(9);
            info!(
                repo = %repo_str,
                revision = %revision,
                "Fetched job details."
            );

            let row = client
                .query_one(
                    "SELECT token_encrypted FROM huggingface_keys WHERE id=$1",
                    &[&key_id],
                )
                .await?;
            let token_encrypted: Vec<u8> = row.get(0);
            let enc_key_hex = std::env::var("ANVIL_SECRET_ENCRYPTION_KEY").unwrap_or_default();
            if enc_key_hex.is_empty() {
                anyhow::bail!("missing encryption key in worker");
            }
            let enc_key = hex::decode(enc_key_hex)?;
            let token_bytes = crate::crypto::decrypt(&token_encrypted, &enc_key)?;
            let token = String::from_utf8(token_bytes)?;
            debug!("Decrypted token.");

            let cache_dir = tempfile::tempdir()?;
            let api = ApiBuilder::new()
                .with_cache_dir(cache_dir.path().to_path_buf())
                .with_token(Some(token))
                .build()?;

            // --- Blocking File Listing ---
            info!("Getting repo file list (blocking)...");
            let repo_details = (repo_str.clone(), revision.clone());
            let api_clone = api.clone();
            let siblings = tokio::task::spawn_blocking(move || {
                let repo = Repo::with_revision(repo_details.0, RepoType::Model, repo_details.1);
                let repo_client = api_clone.repo(repo);
                repo_client.info().map(|info| info.siblings)
            })
            .await??;
            info!(
                num_files = siblings.len(),
                "Got files from repo."
            );
            // --- End Blocking ---

            let mut inc_builder = GlobSetBuilder::new();
            if include_globs.is_empty() {
                inc_builder.add(Glob::new("**/*")?);
            } else {
                for g in include_globs {
                    inc_builder.add(Glob::new(&g)?);
                }
            }
            let include = inc_builder.build()?;
            let mut exc_builder = GlobSetBuilder::new();
            for g in exclude_globs {
                exc_builder.add(Glob::new(&g)?);
            }
            let exclude = exc_builder.build()?;

            'outer: for e in siblings {
                let path = e.rfilename.clone();
                debug!(path = %path, "Processing file");
                let path_buf = std::path::PathBuf::from(path.clone());
                if !include.is_match(path_buf.as_path()) {
                    continue;
                }
                if exclude.is_match(path_buf.as_path()) {
                    continue;
                }
                let size = None; // hf-hub RepoSibling does not include size; will be known after download
                let item_id = persistence
                    .hf_add_item(ingestion_id, &path, size, None)
                    .await?;
                persistence
                    .hf_update_item_state(item_id, HFIngestionItemState::Downloading, None)
                    .await?;
                debug!(item_id, "Item state set to downloading.");

                if let Ok(bucket_opt) =
                    persistence.get_bucket_by_name(tenant_id, &target_bucket).await
                {
                    if let Some(bucket) = bucket_opt {
                        if let Ok(obj_opt) = persistence.get_object(bucket.id, &path).await {
                            if obj_opt.is_some() {
                                info!(path = %path, "Skipping existing file");
                                persistence
                                    .hf_update_item_state(
                                        item_id,
                                        HFIngestionItemState::Skipped,
                                        None,
                                    )
                                    .await?;
                                continue 'outer;
                            }
                        }
                    }
                }

                // --- Blocking File Download ---
                info!(
                    file = %e.rfilename,
                    "Downloading file (blocking)..."
                );
                let repo_details_clone = (repo_str.clone(), revision.clone());
                let api_clone_2 = api.clone();
                let filename = e.rfilename.clone();
                let local_path_buf;
                    info!("Downloading from Hugging Face");
                    local_path_buf = tokio::task::spawn_blocking(move || {
                        let repo = Repo::with_revision(
                            repo_details_clone.0,
                            RepoType::Model,
                            repo_details_clone.1,
                        );
                        let repo_client = api_clone_2.repo(repo);
                        repo_client.get(&filename)
                    })
                    .await??;

                let local_path = &local_path_buf;
                debug!(path = ?local_path, "Downloaded to");
                // --- End Blocking ---

                let _bucket = persistence
                    .get_bucket_by_name(tenant_id, &target_bucket)
                    .await?
                    .ok_or_else(|| anyhow!("target bucket not found"))?;
                let full_key = if target_prefix.is_empty() {
                    path.clone()
                } else {
                    format!(
                        "{}/{}",
                        target_prefix.trim_end_matches('/'),
                        path
                    )
                };

                info!(
                    bucket = %target_bucket,
                    key = %full_key,
                    "Uploading to Anvil"
                );
                let make_reader = || async {
                    let f = tokio::fs::File::open(&local_path).await;
                    f.map(|file| {
                        use futures_util::StreamExt as _;
                        use tokio_util::io::ReaderStream;
                        ReaderStream::new(file).map(|r: Result<bytes::Bytes, std::io::Error>| {
                            r.map(|b| b.to_vec())
                                .map_err(|e| tonic::Status::internal(e.to_string()))
                        })
                    })
                };

                let mut reader = make_reader().await?;
                let scopes = vec!["object:write|*".to_string()];
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    info!("Putting object, attempt {}", attempt);
                    let res = object_manager
                        .put_object(tenant_id, &target_bucket, &full_key, &scopes, reader)
                        .await;
                    match res {
                                                        Ok(obj) => {
                                                            info!(key = %full_key, "Upload successful");
                                                            persistence.hf_update_item_success(item_id, obj.size, &obj.etag).await?;
                                                            break;
                                                        }                        Err(e) if attempt < 3 => {
                            warn!(
                                attempt,
                                key = %full_key,
                                error = %e.to_string(),
                                "Upload attempt failed. Retrying..."
                            );
                            let jitter = (rand::random::<u64>() % 200) as u64;
                            tokio::time::sleep(std::time::Duration::from_millis(
                                500 * attempt as u64 + jitter,
                            ))
                            .await;
                            reader = make_reader().await?;
                            continue;
                        }
                        Err(e) => {
                            error!(
                                key = %full_key,
                                error = %e,
                                "Upload failed permanently"
                            );
                            return Err(anyhow::anyhow!(e.to_string()));
                        }
                    }
                }

            }

            info!(
                ingestion_id,
                "Ingestion task completed successfully."
            );

            // --- Generate and upload anvil-index.json ---
            let index_key = if target_prefix.is_empty() {
                "anvil-index.json".to_string()
            } else {
                format!("{}/anvil-index.json", target_prefix.trim_end_matches('/'))
            };

            let mut file_map = HashMap::new();

            // Try to load existing index to merge
            let claims = crate::auth::Claims {
                sub: "internal-worker".to_string(),
                exp: (chrono::Utc::now().timestamp() + 3600) as usize,
                scopes: vec!["object:read|*".to_string()],
                tenant_id: tenant_id,
            };

            if let Ok((_, mut stream)) = object_manager.get_object(Some(claims), target_bucket.clone(), index_key.clone()).await {
                let mut data = Vec::new();
                while let Some(chunk_res) = stream.next().await {
                    if let Ok(chunk) = chunk_res {
                        data.extend_from_slice(&chunk);
                    }
                }
                if let Ok(val) = serde_json::from_slice::<serde_json::Value>(&data) {
                    if let Some(files) = val.get("files").and_then(|f| f.as_object()) {
                        for (k, v) in files {
                            file_map.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            let items = persistence.hf_get_ingestion_items(ingestion_id).await?;
            for (path, size_opt, etag_opt, finished_at_opt) in items {
                let mut meta = json!({});
                if let Some(s) = size_opt {
                    meta["size"] = json!(s);
                }
                if let Some(e) = etag_opt {
                    meta["etag"] = json!(e);
                }
                if let Some(f) = finished_at_opt {
                    meta["last_modified"] = json!(f.to_rfc3339());
                }
                file_map.insert(path, meta);
            }

            let mut total_bytes = 0;
            for meta in file_map.values() {
                if let Some(s) = meta.get("size").and_then(|v| v.as_i64()) {
                    total_bytes += s;
                }
            }

            let index_json = json!({
                "meta": {
                    "source_repo": repo_str,
                    "revision": revision,
                    "generated_at": chrono::Utc::now().to_rfc3339(),
                    "total_files": file_map.len(),
                    "total_bytes": total_bytes
                },
                "files": file_map,
            });

            let index_content_data = serde_json::to_vec_pretty(&index_json)?;
            info!(index_key = %index_key, "Uploading anvil-index.json");

            // Upload index file, using retry logic adapted from above for robustness
            let mut attempt = 0;
            loop {
                attempt += 1;
                info!("Putting anvil-index.json, attempt {}", attempt);
                let current_index_content = index_content_data.clone();
                let index_stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>> = Box::pin(
                    futures_util::stream::once(async move {
                        Ok(current_index_content)
                    })
                    .map(|item: Result<Vec<u8>, Infallible>| {
                        item.map_err(|e| match e {})
                    })
                );

                let res: Result<Object, Status> = object_manager.put_object(
                    tenant_id,
                    &target_bucket,
                    &index_key,
                    &vec!["object:write|*".to_string()], // Scopes
                    index_stream,
                ).await;
                match res {
                    Ok(_) => {
                        info!(key = %index_key, "anvil-index.json upload successful");
                        break;
                    }
                    Err(e) if attempt < 3 => {
                        warn!(
                            attempt,
                            key = %index_key,
                            error = %e.to_string(),
                            "anvil-index.json upload attempt failed. Retrying..."
                        );
                        let jitter = (rand::random::<u64>() % 200) as u64;
                        tokio::time::sleep(std::time::Duration::from_millis(
                            500 * attempt as u64 + jitter,
                        ))
                        .await;
                        continue;
                    }
                    Err(e) => {
                        error!(
                            key = %index_key,
                            error = %e,
                            "anvil-index.json upload failed permanently"
                        );
                        return Err(anyhow::anyhow!(e.to_string()));
                    }
                }
            }
            // --- End anvil-index.json upload ---

            info!(ingestion_id, "Updating ingestion state to completed.");
            persistence
                .hf_update_ingestion_state(ingestion_id, HFIngestionState::Completed, None)
                .await?;
            info!(ingestion_id, "Ingestion state set to completed.");

            Ok::<(), anyhow::Error>(())
        }
        .await;

    if let Err(e) = &result {
        error!(ingestion_id, error = %e, "HF Ingestion task failed");
    }
    result
}

async fn handle_delete_object(
    persistence: &Persistence,
    cluster_state: &ClusterState,
    jwt_manager: &Arc<JwtManager>,
    task: &Task,
) -> Result<()> {
    let payload: DeleteObjectPayload = serde_json::from_value(task.payload.clone())?;

    if let Some(shard_map_peers) = payload.shard_map {
        let cluster_map = cluster_state.read().await;
        let mut futures = Vec::new();

        for (i, peer_id_str) in shard_map_peers.iter().enumerate() {
            let peer_id: libp2p::PeerId = peer_id_str.parse()?;
            if let Some(peer_info) = cluster_map.get(&peer_id) {
                let grpc_addr = peer_info.grpc_addr.clone();
                let content_hash = payload.content_hash.clone();
                let token = jwt_manager.mint_token(
                    "internal-worker".to_string(),
                    vec![format!("internal:delete_shard:{}/{}", content_hash, i)],
                    0, // System-level task, no tenant
                )?;

                futures.push(async move {
                    let endpoint =
                        if grpc_addr.starts_with("http://") || grpc_addr.starts_with("https://") {
                            grpc_addr
                        } else {
                            format!("http://{}", grpc_addr)
                        };
                    let mut client = InternalAnvilServiceClient::connect(endpoint)
                        .await
                        .map_err(|e| Status::internal(e.to_string()))?;
                    let mut req = tonic::Request::new(DeleteShardRequest {
                        object_hash: content_hash,
                        shard_index: i as u32,
                    });
                    req.metadata_mut().insert(
                        "authorization",
                        format!("Bearer {}", token).parse().unwrap(),
                    );
                    client.delete_shard(req).await
                });
            }
        }
        // We proceed even if some shard deletions fail. The object metadata will be gone,
        // so the shards become orphaned and can be garbage collected later.
        let _ = futures::future::join_all(futures).await;
    }

    // Finally, hard delete the object from the database.
    persistence.hard_delete_object(payload.object_id).await?;

    info!(
        "Successfully processed DeleteObject task for object {}",
        payload.object_id
    );
    Ok(())
}
