use crate::anvil_api::DeleteShardRequest;
use crate::anvil_api::internal_anvil_service_client::InternalAnvilServiceClient;
use crate::auth::JwtManager;
use crate::cluster::ClusterState;
use crate::object_manager::ObjectManager;
use crate::persistence::Persistence;
use crate::tasks::{HFIngestionItemState, HFIngestionState, TaskStatus, TaskType};
use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::Row;
use tonic::Status;
use tracing::{error, info};

#[derive(Debug)]
struct Task {
    id: i64,
    task_type: TaskType,
    payload: JsonValue,
    attempts: i32,
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
            attempts: row.get("attempts"),
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
                println!("Failed to fetch tasks: {}", e);
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
                    println!("Failed to mark task {} as running: {}", task.id, e);
                    return;
                }

                let result = match task.task_type {
                    TaskType::DeleteObject => handle_delete_object(&p, &cs, &jm, &task).await,
                    TaskType::HFIngestion => handle_hf_ingestion(&p, &om, &task).await,
                    _ => {
                        println!("Unhandled task type: {:?}", task.task_type);
                        Ok(())
                    }
                };

                if let Err(e) = result {
                    println!("Task {} failed: {:?}", task.id, e);
                    if let Err(fail_err) = p.fail_task(task.id, &e.to_string()).await {
                        println!("Failed to mark task {} as failed: {:?}", task.id, fail_err);
                    }
                } else {
                    if let Err(complete_err) = 
                        p.update_task_status(task.id, TaskStatus::Completed).await
                    {
                        println!(
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
    use hf_hub::{api::sync::Api, Repo, RepoType};
    use std::fs::File;
    use std::io::Read;

    let ingestion_id: i64 = task
        .payload
        .get("ingestion_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("missing ingestion_id"))?;

    // Wrap the main logic in a closure to ensure we can catch errors and update the final status.
    let result =
        async {
            println!(
                "[HF_INGESTION] Starting ingestion task for id: {}",
                ingestion_id
            );

            persistence
                .hf_update_ingestion_state(ingestion_id, HFIngestionState::Running, None)
                .await?;

            let client = persistence.get_global_pool().get().await?;
            let job = client
                .query_one(
                    "SELECT key_id, repo, COALESCE(revision,'main'), target_bucket, COALESCE(target_prefix,''), include_globs, exclude_globs FROM hf_ingestions WHERE id=$1",
                    &[&ingestion_id],
                )
                .await?;
            let key_id: i64 = job.get(0);
            let repo_str: String = job.get(1);
            let revision: String = job.get(2);
            let target_bucket: String = job.get(3);
            let target_prefix: String = job.get(4);
            let include_globs: Vec<String> = job.get(5);
            let exclude_globs: Vec<String> = job.get(6);
            println!(
                "[HF_INGESTION] Fetched job details for repo: {}, revision: {}",
                repo_str,
                revision
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
            println!("[HF_INGESTION] Decrypted token.");

            unsafe {
                std::env::set_var("HF_TOKEN", token);
            }
            let api = Api::new()?;

            // --- Blocking File Listing ---
            println!("[HF_INGESTION] Getting repo file list (blocking)...");
            let repo_details = (repo_str.clone(), revision.clone());
            let api_clone = api.clone();
            let siblings = tokio::task::spawn_blocking(move || {
                let repo = Repo::with_revision(repo_details.0, RepoType::Model, repo_details.1);
                let repo_client = api_clone.repo(repo);
                repo_client.info().map(|info| info.siblings)
            })
            .await??;
            println!(
                "[HF_INGESTION] Got {} files from repo.",
                siblings.len()
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
                println!("[HF_INGESTION] Processing file: {}", path);
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
                println!("[HF_INGESTION] Item {} state set to downloading.", item_id);

                if let Ok(bucket_opt) =
                    persistence.get_public_bucket_by_name(&target_bucket).await
                {
                    if let Some(bucket) = bucket_opt {
                        if let Ok(obj_opt) = persistence.get_object(bucket.id, &path).await {
                            if obj_opt.is_some() {
                                println!("[HF_INGESTION] Skipping existing file: {}", path);
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
                println!(
                    "[HF_INGESTION] Downloading file (blocking): {}",
                    e.rfilename
                );
                let repo_details_clone = (repo_str.clone(), revision.clone());
                let api_clone_2 = api.clone();
                let filename = e.rfilename.clone();
                let local_path = tokio::task::spawn_blocking(move || {
                    let repo = Repo::with_revision(
                        repo_details_clone.0,
                        RepoType::Model,
                        repo_details_clone.1,
                    );
                    let repo_client = api_clone_2.repo(repo);
                    repo_client.get(&filename)
                })
                .await??;
                println!("[HF_INGESTION] Downloaded to: {:?}", local_path);
                // --- End Blocking ---

                let bucket = persistence
                    .get_public_bucket_by_name(&target_bucket)
                    .await?
                    .ok_or_else(|| anyhow!("target bucket not found"))?;
                let tenant_id = bucket.tenant_id;
                let full_key = if target_prefix.is_empty() {
                    path.clone()
                } else {
                    format!(
                        "{}/{}",
                        target_prefix.trim_end_matches('/'),
                        path
                    )
                };

                println!(
                    "[HF_INGESTION] Uploading to Anvil: bucket={}, key={}",
                    target_bucket,
                    full_key
                );
                let mut make_reader = || async {
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
                let scopes = vec![format!("write:bucket:{}/{}", target_bucket, full_key)];
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    let res = object_manager
                        .put_object(tenant_id, &target_bucket, &full_key, &scopes, reader)
                        .await;
                    match res {
                        Ok(_obj) => {
                            println!("[HF_INGESTION] Upload successful for key: {}", full_key);
                            break;
                        }
                        Err(e) if attempt < 3 => {
                            println!(
                                "[HF_INGESTION] Upload attempt {} failed for key: {}. Retrying...",
                                attempt, full_key
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
                            println!(
                                "[HF_INGESTION] Upload failed permanently for key: {}. Error: {}",
                                full_key, e
                            );
                            return Err(anyhow::anyhow!(e.to_string()));
                        }
                    }
                }
                persistence
                    .hf_update_item_state(item_id, HFIngestionItemState::Stored, None)
                    .await?;
                println!("[HF_INGESTION] Item {} state set to stored.", item_id);
            }

            println!(
                "[HF_INGESTION] Ingestion task {} completed successfully.",
                ingestion_id
            );
            persistence
                .hf_update_ingestion_state(ingestion_id, HFIngestionState::Completed, None)
                .await?;

            Ok::<(), anyhow::Error>(())
        }
        .await;

    if let Err(e) = result {
        panic!("[HF_INGESTION] Worker task failed with error: {:?}", e);
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
                    let endpoint = if grpc_addr.starts_with("http://") || grpc_addr.starts_with("https://") {
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

