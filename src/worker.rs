use crate::anvil_api::internal_anvil_service_client::InternalAnvilServiceClient;
use crate::anvil_api::DeleteShardRequest;
use crate::auth::JwtManager;
use crate::cluster::ClusterState;
use crate::persistence::Persistence;
use crate::tasks::TaskType;
use anyhow::{anyhow, Result};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use std::time::Duration;
use tokio_postgres::Row;
use tonic::Status;

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
) -> Result<()> {
    loop {
        let tasks = match persistence.fetch_pending_tasks_for_update(10).await {
            Ok(rows) => rows.into_iter().map(Task::try_from).collect::<Result<Vec<_>>>()?,
            Err(e) => {
                eprintln!("Failed to fetch tasks: {}", e);
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
            tokio::spawn(async move {
                if let Err(e) = p.update_task_status(task.id, "running").await {
                    eprintln!("Failed to mark task {} as running: {}", task.id, e);
                    return;
                }

                let result = match task.task_type {
                    TaskType::DeleteObject => handle_delete_object(&p, &cs, &jm, &task).await,
                    _ => {
                        println!("Unhandled task type: {:?}", task.task_type);
                        Ok(())
                    }
                };

                if let Err(e) = result {
                    eprintln!("Task {} failed: {}", task.id, e);
                    if let Err(fail_err) = p.fail_task(task.id, &e.to_string()).await {
                        eprintln!("Failed to mark task {} as failed: {}", task.id, fail_err);
                    }
                } else {
                    if let Err(complete_err) = p.update_task_status(task.id, "completed").await {
                        eprintln!(
                            "Failed to mark task {} as completed: {}",
                            task.id,
                            complete_err
                        );
                    }
                }
            });
        }
    }
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
                    vec![format!(
                        "internal:delete_shard:{}/{}",
                        content_hash,
                        i
                    )],
                    0, // System-level task, no tenant
                )?;

                futures.push(async move {
                    let mut client = InternalAnvilServiceClient::connect(grpc_addr).await.map_err(|e| Status::internal(e.to_string()))?;
                    let mut req = tonic::Request::new(DeleteShardRequest {
                        object_hash: content_hash,
                        shard_index: i as u32,
                    });
                    req.metadata_mut()
                        .insert("authorization", format!("Bearer {}", token).parse().unwrap());
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

    println!("Successfully processed DeleteObject task for object {}", payload.object_id);
    Ok(())
}
