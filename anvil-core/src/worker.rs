use crate::auth::JwtManager;
use crate::cluster::ClusterState;
use crate::crypto::EncryptionKeyring;
use crate::object_manager::ObjectManager;
use crate::persistence::Object;
use crate::persistence::Persistence;
use crate::tasks::{HFIngestionItemState, HFIngestionState, TaskStatus, TaskType};
use anyhow::{Result, anyhow};
use futures_util::{Stream, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::boxed::Box;
use std::collections::HashMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tonic::Status;
use tracing::{debug, error, info, warn};

type Task = crate::persistence::TaskRecord;

#[derive(Deserialize)]
struct DeleteObjectPayload {
    object_id: i64,
}

#[derive(Deserialize)]
struct DeleteBucketPayload {
    bucket_id: i64,
}

#[derive(Deserialize)]
struct ObjectMetadataCompactionPayload {
    bucket_id: i64,
}

#[derive(Deserialize)]
struct IndexBuildPayload {
    tenant_id: i64,
    bucket_id: i64,
    index_id: i64,
    index_version: i64,
    source_cursor: u128,
}

pub async fn run(
    persistence: Persistence,
    cluster_state: ClusterState,
    jwt_manager: Arc<JwtManager>,
    object_manager: ObjectManager,
    keyring: Arc<EncryptionKeyring>,
) -> Result<()> {
    let task_notify = persistence.task_notify();
    loop {
        let tasks = match persistence.claim_pending_tasks(10).await {
            Ok(tasks) => tasks,
            Err(e) => {
                error!("Failed to fetch tasks: {}", e);
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };

        if tasks.is_empty() {
            tokio::select! {
                _ = task_notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(500)) => {}
            }
            continue;
        }

        for task in tasks {
            let p = persistence.clone();
            let cs = cluster_state.clone();
            let jm = jwt_manager.clone();
            let om = object_manager.clone();
            let keyring = keyring.clone();
            tokio::spawn(async move {
                let result = execute_task_with_lease(&p, &cs, &jm, &om, &task, &keyring).await;

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

async fn execute_task_with_lease(
    persistence: &Persistence,
    _cluster_state: &ClusterState,
    _jwt_manager: &Arc<JwtManager>,
    object_manager: &ObjectManager,
    task: &Task,
    keyring: &Arc<EncryptionKeyring>,
) -> anyhow::Result<()> {
    let lease = persistence.acquire_task_execution_lease(task).await?;
    match task.task_type {
        TaskType::DeleteObject => handle_delete_object(persistence, task).await?,
        TaskType::DeleteBucket => handle_delete_bucket(persistence, task).await?,
        TaskType::ObjectMetadataCompaction => {
            handle_object_metadata_compaction(persistence, task).await?
        }
        TaskType::IndexBuild => handle_index_build(persistence, task).await?,
        TaskType::HFIngestion => {
            handle_hf_ingestion(persistence, object_manager, task, keyring).await?
        }
        _ => {
            warn!("Unhandled task type: {:?}", task.task_type);
        }
    }
    persistence
        .checkpoint_task_execution_lease(&lease, lease.source_cursor)
        .await?;
    Ok(())
}

async fn handle_index_build(persistence: &Persistence, task: &Task) -> anyhow::Result<()> {
    let payload: IndexBuildPayload = serde_json::from_value(task.payload.clone())?;
    match persistence
        .build_index_task(
            payload.tenant_id,
            payload.bucket_id,
            payload.index_id,
            payload.index_version,
            payload.source_cursor,
        )
        .await?
    {
        Some(outcome) => {
            info!(
                index_id = payload.index_id,
                index_storage_id = %outcome.index_storage_id,
                index_kind = %outcome.index_kind,
                generation = outcome.generation,
                item_count = outcome.item_count,
                source_cursor = outcome.source_cursor,
                segment_hashes = ?outcome.segment_hashes,
                diagnostic_count = outcome.diagnostics.len(),
                "Index build task completed"
            );
        }
        None => {
            info!(
                index_id = payload.index_id,
                index_version = payload.index_version,
                "Index build task skipped because the index is absent, disabled, stale, or unsupported"
            );
        }
    }
    Ok(())
}

async fn handle_object_metadata_compaction(
    persistence: &Persistence,
    task: &Task,
) -> anyhow::Result<()> {
    let payload: ObjectMetadataCompactionPayload = serde_json::from_value(task.payload.clone())?;
    let Some(sealed) = persistence
        .compact_object_metadata(payload.bucket_id)
        .await?
    else {
        info!(
            bucket_id = payload.bucket_id,
            "Object metadata compaction skipped; bucket or journal did not exist"
        );
        return Ok(());
    };
    info!(
        bucket_id = payload.bucket_id,
        generation = sealed.generation,
        metadata_records = sealed.metadata_record_count,
        directory_records = sealed.directory_record_count,
        manifest_hash = %sealed.manifest_hash,
        "Object metadata compaction sealed partition"
    );
    Ok(())
}

async fn handle_hf_ingestion(
    persistence: &Persistence,
    object_manager: &ObjectManager,
    task: &Task,
    keyring: &EncryptionKeyring,
) -> anyhow::Result<()> {
    use globset::{Glob, GlobSetBuilder};
    use hf_hub::{Repo, RepoType, api::sync::ApiBuilder};

    let ingestion_id: i64 = task
        .payload
        .get("ingestion_id")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| anyhow!("missing ingestion_id"))?;

    // Wrap the main logic in a closure to ensure we can catch errors and update the final status.
    let result = async {
        info!(ingestion_id, "Starting ingestion task.");

        persistence
            .hf_update_ingestion_state(ingestion_id, HFIngestionState::Running, None)
            .await?;

        let job = persistence
            .hf_get_ingestion_job(ingestion_id)
            .await?
            .ok_or_else(|| anyhow!("ingestion job not found"))?;
        let key_id = job.key_id;
        let tenant_id = job.tenant_id;
        let _requester_app_id = job.requester_app_id;
        let repo_str = job.repo;
        let revision = job.revision;
        let target_bucket = job.target_bucket;
        let _target_region = job.target_region;
        let target_prefix = job.target_prefix;
        let include_globs = job.include_globs;
        let exclude_globs = job.exclude_globs;
        info!(
            repo = %repo_str,
            revision = %revision,
            "Fetched job details."
        );

        let token_encrypted = persistence
            .hf_get_key_encrypted_by_id(key_id)
            .await?
            .ok_or_else(|| anyhow!("hugging face key not found"))?;
        let token_bytes = keyring.decrypt(&token_encrypted)?;
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
        info!(num_files = siblings.len(), "Got files from repo.");
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

            if let Ok(bucket_opt) = persistence
                .get_bucket_by_name(tenant_id, &target_bucket)
                .await
            {
                if let Some(bucket) = bucket_opt {
                    if let Ok(obj_opt) = persistence.get_object(bucket.id, &path).await {
                        if obj_opt.is_some() {
                            info!(path = %path, "Skipping existing file");
                            persistence
                                .hf_update_item_state(item_id, HFIngestionItemState::Skipped, None)
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
                format!("{}/{}", target_prefix.trim_end_matches('/'), path)
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
                    .put_object(
                        tenant_id,
                        &target_bucket,
                        &full_key,
                        &scopes,
                        reader,
                        crate::object_manager::ObjectWriteOptions::default(),
                    )
                    .await;
                match res {
                    Ok(obj) => {
                        info!(key = %full_key, "Upload successful");
                        persistence
                            .hf_update_item_success(item_id, obj.size, &obj.etag)
                            .await?;
                        break;
                    }
                    Err(e) if attempt < 3 => {
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

        info!(ingestion_id, "Ingestion task completed successfully.");

        // --- Generate and upload anvil-index.json ---
        let index_key = if target_prefix.is_empty() {
            "anvil-index.json".to_string()
        } else {
            format!("{}/anvil-index.json", target_prefix.trim_end_matches('/'))
        };

        let mut file_map = HashMap::new();

        // Fetch ALL items for this target (from past and current jobs) to build a complete index
        let all_items = persistence
            .hf_get_all_items_for_prefix(tenant_id, &target_bucket, &target_prefix)
            .await?;

        for (path, size_opt, etag_opt, finished_at_opt) in all_items {
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
            // Insert will overwrite existing entries, so later jobs (ordered by finished_at) win.
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
            let index_stream: Pin<
                Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>,
            > = Box::pin(
                futures_util::stream::once(async move { Ok(current_index_content) })
                    .map(|item: Result<Vec<u8>, Infallible>| item.map_err(|e| match e {})),
            );

            let res: Result<Object, Status> = object_manager
                .put_object(
                    tenant_id,
                    &target_bucket,
                    &index_key,
                    &vec!["object:write|*".to_string()], // Scopes
                    index_stream,
                    crate::object_manager::ObjectWriteOptions {
                        content_type: Some("application/json".to_string()),
                        user_metadata: None,
                    },
                )
                .await;
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

async fn handle_delete_object(persistence: &Persistence, task: &Task) -> Result<()> {
    let payload: DeleteObjectPayload = serde_json::from_value(task.payload.clone())?;

    // Finally, hard delete the object metadata.
    persistence.hard_delete_object(payload.object_id).await?;

    info!(
        "Successfully processed DeleteObject task for object {}",
        payload.object_id
    );
    Ok(())
}

async fn handle_delete_bucket(persistence: &Persistence, task: &Task) -> Result<()> {
    let payload: DeleteBucketPayload = serde_json::from_value(task.payload.clone())?;
    let deleted = persistence
        .hard_delete_bucket_if_empty(payload.bucket_id)
        .await?;

    if deleted {
        info!(
            "Successfully processed DeleteBucket task for bucket {}",
            payload.bucket_id
        );
    } else {
        info!(
            "DeleteBucket task for bucket {} was already applied",
            payload.bucket_id
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, storage::Storage};
    use chrono::Utc;
    use std::collections::HashMap;
    use tempfile::tempdir;
    use tokio::sync::{RwLock, broadcast};

    fn test_config(storage_path: &std::path::Path) -> Config {
        Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "worker-test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "test-region".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            ..Config::default()
        }
    }

    #[tokio::test]
    async fn object_metadata_compaction_task_seals_manifest() {
        let temp = tempdir().unwrap();
        let config = test_config(temp.path());
        let persistence = Persistence::new(&config, None).unwrap();

        persistence.create_region("local").await.unwrap();
        let bucket = persistence
            .create_bucket(1, "task-compact-bucket", "local")
            .await
            .unwrap();
        let object = persistence
            .create_object(
                1,
                bucket.id,
                "docs/a.txt",
                "hash-a",
                11,
                "etag-a",
                Some("text/plain"),
                None,
                None,
                None,
            )
            .await
            .unwrap();

        let now = Utc::now();
        let task = Task {
            id: 1,
            task_type: TaskType::ObjectMetadataCompaction,
            payload: json!({ "bucket_id": bucket.id }),
            priority: 0,
            status: TaskStatus::Running,
            attempts: 1,
            last_error: None,
            scheduled_at: now,
            created_at: now,
            updated_at: now,
        };
        let storage = Storage::new_at_sync(&config.storage_path).unwrap();
        let core_store = crate::core_store::CoreStore::new(storage.clone())
            .await
            .unwrap();
        let cluster_state: ClusterState = Arc::new(RwLock::new(HashMap::new()));
        let jwt_manager = Arc::new(JwtManager::new(config.jwt_secret.clone()));
        let (watch_tx, _watch_rx) = broadcast::channel(16);
        let object_manager = ObjectManager::new(
            persistence.clone(),
            storage.clone(),
            core_store,
            config.region.clone(),
            config.cross_region_routing_policy,
            hex::decode(&config.anvil_secret_encryption_key).unwrap(),
            watch_tx,
            crate::observability::Observability::default(),
        );
        let keyring = Arc::new(config.secret_keyring().unwrap());
        execute_task_with_lease(
            &persistence,
            &cluster_state,
            &jwt_manager,
            &object_manager,
            &task,
            &keyring,
        )
        .await
        .unwrap();

        assert!(
            crate::metadata_journal::read_latest_partition_manifest(
                &storage,
                &bucket,
                &hex::decode(&config.anvil_secret_encryption_key).unwrap()
            )
            .await
            .unwrap()
            .is_some()
        );
        let replayed = persistence
            .get_object(bucket.id, "docs/a.txt")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replayed.version_id, object.version_id);
        let lease = persistence
            .read_task_execution_lease(task.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.partition_family, "object_metadata");
        assert_eq!(lease.checkpoint_cursor, lease.source_cursor);
    }
}
