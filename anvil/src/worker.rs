use crate::anvil_api::DeleteShardRequest;
use crate::anvil_api::internal_anvil_service_client::InternalAnvilServiceClient;
use crate::auth::JwtManager;
use crate::object_manager::ObjectManager;
use crate::cluster::ClusterState;
use crate::persistence::Persistence;
use crate::tasks::TaskType;
use anyhow::{Result, anyhow};
use serde::Deserialize;
use serde_json::Value as JsonValue;
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
            _ => return Err(anyhow!("Unknown task type")),
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
                if let Err(e) = p.update_task_status(task.id, "running").await {
                    error!("Failed to mark task {} as running: {}", task.id, e);
                    return;
                }

                let result = match task.task_type {
                    TaskType::DeleteObject => handle_delete_object(&p, &cs, &jm, &task).await,
                    TaskType::HFIngestion => handle_hf_ingestion(&p, &om, &task).await,
                    _ => { info!("Unhandled task type: {:?}", task.task_type); Ok(()) }
                };

                if let Err(e) = result {
                    error!("Task {} failed: {}", task.id, e);
                    if let Err(fail_err) = p.fail_task(task.id, &e.to_string()).await {
                        error!("Failed to mark task {} as failed: {}", task.id, fail_err);
                    }
                } else {
                    if let Err(complete_err) = p.update_task_status(task.id, "completed").await {
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

async fn handle_hf_ingestion(persistence: &Persistence, object_manager: &ObjectManager, task: &Task) -> anyhow::Result<()> {
    use hf_hub::{api::sync::ApiBuilder, Repo, RepoType};
    use globset::{Glob, GlobSetBuilder};
    use std::fs::File;
    use std::io::Read;

    let ingestion_id: i64 = task
        .payload
        .get("ingestion_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("missing ingestion_id"))?;

    persistence
        .hf_update_ingestion_state(ingestion_id, "running", None)
        .await?;

    let client = persistence.get_global_pool().get().await?;
    let job = client
        .query_one(
            "SELECT key_id, repo, COALESCE(revision,'main'), target_bucket, COALESCE(target_prefix,''), include_globs, exclude_globs FROM hf_ingestions WHERE id=$1",
            &[&ingestion_id],
        )
        .await?;
    let key_id: i64 = job.get(0);
    let repo: String = job.get(1);
    let revision: String = job.get(2);
    let target_bucket: String = job.get(3);
    let target_prefix: String = job.get(4);
    let include_globs: Vec<String> = job.get(5);
    let exclude_globs: Vec<String> = job.get(6);

    let row = client
        .query_one("SELECT token_encrypted FROM huggingface_keys WHERE id=$1", &[&key_id])
        .await?;
    let token_encrypted: Vec<u8> = row.get(0);
    let enc_key = std::env::var("ANVIL_SECRET_ENCRYPTION_KEY").unwrap_or_default();
    if enc_key.is_empty() {
        persistence
            .hf_update_ingestion_state(ingestion_id, "failed", Some("missing encryption key in worker"))
            .await?;
        anyhow::bail!("missing encryption key in worker");
    }
    let token_bytes = crate::crypto::decrypt(&token_encrypted, enc_key.as_bytes())?;
    let token = String::from_utf8(token_bytes)?;

    let api = ApiBuilder::new().with_token(Some(token)).build()?;
    let repo = Repo::with_revision(repo, RepoType::Model, revision);
    let repo_client = api.repo(repo);

    let mut inc_builder = GlobSetBuilder::new();
    if include_globs.is_empty() { inc_builder.add(Glob::new("**/*")?); } else { for g in include_globs { inc_builder.add(Glob::new(&g)?); } }
    let include = inc_builder.build()?;
    let mut exc_builder = GlobSetBuilder::new();
    for g in exclude_globs { exc_builder.add(Glob::new(&g)?); }
    let exclude = exc_builder.build()?;

    // List files in repo (hf-hub 0.3): use repo_client.get on index and iterate entries via walk
    let info = repo_client.info()?; // RepoInfo { siblings, sha }
    'outer: for e in info.siblings {
        let path = e.rfilename.clone();
        let path = std::path::PathBuf::from(path);
        if !include.is_match(path.as_path()) { continue; }
        if exclude.is_match(path.as_path()) { continue; }
        let size = None; // hf-hub RepoSibling does not include size; will be known after download
        let item_id = persistence
            .hf_add_item(ingestion_id, &path.to_string_lossy(), size, None)
            .await?;
        persistence
            .hf_update_item_state(item_id, "downloading", None)
            .await?;

        // Skip if object exists with same key (size check not available here; best-effort skip)
        // Use list with prefix == full key to detect existence
        if let Ok(bucket_opt) = persistence.get_public_bucket_by_name(&target_bucket).await {
            if let Some(bucket) = bucket_opt {
                if let Ok(obj_opt) = persistence.get_object(bucket.id, &path.to_string_lossy()).await {
                    if obj_opt.is_some() { continue 'outer; }
                }
            }
        }

        let local = repo_client.get(path.to_string_lossy().as_ref())?;
        // Determine tenant and construct object key
        let bucket = persistence
            .get_public_bucket_by_name(&target_bucket)
            .await?
            .ok_or_else(|| anyhow::anyhow!("target bucket not found"))?;
        let tenant_id = bucket.tenant_id;
        let full_key = if target_prefix.is_empty() { path.to_string_lossy().to_string() } else { format!("{}/{}", target_prefix.trim_end_matches('/'), path.to_string_lossy()) };

        // Build a stream from the local file
        let file = tokio::fs::File::open(&local).await?;
        use tokio_util::io::ReaderStream;
        use futures_util::StreamExt as _;
        let mut make_reader = || async {
            let f = tokio::fs::File::open(&local).await;
            f.map(|file| ReaderStream::new(file).map(|r: Result<bytes::Bytes, std::io::Error>| r.map(|b| b.to_vec()).map_err(|e| tonic::Status::internal(e.to_string()))))
        };
        let mut reader = make_reader().await?;
        // Internal write scope: bypass external policy in worker context
        let scopes = vec![format!("write:bucket:{}/{}", target_bucket, full_key)];
        // Retry upload with simple backoff
        let mut attempt = 0;
        loop {
            attempt += 1;
            let res = object_manager
                .put_object(tenant_id, &target_bucket, &full_key, &scopes, reader)
                .await;
            match res {
                Ok(_obj) => break,
                Err(e) if attempt < 3 => {
                    // jittered backoff: 500ms * attempt + 0-200ms
                    let jitter = (rand::random::<u64>() % 200) as u64;
                    tokio::time::sleep(std::time::Duration::from_millis(500 * attempt as u64 + jitter)).await;
                    // Recreate reader for retry
                    reader = make_reader().await?;
                    continue;
                }
                Err(e) => return Err(anyhow::anyhow!(e.to_string())),
            }
        }
        persistence
            .hf_update_item_state(item_id, "stored", None)
            .await?;
    }

    persistence
        .hf_update_ingestion_state(ingestion_id, "completed", None)
        .await?;
    Ok(())
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
                    let mut client = InternalAnvilServiceClient::connect(grpc_addr)
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
