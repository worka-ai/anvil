use super::*;
use std::time::Duration;

#[tokio::test]
async fn core_store_startup_discards_stale_process_locks() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let stale_lock = storage
        .core_store_staging_path()
        .join("locks")
        .join("stream")
        .join("stale.lock");
    std::fs::create_dir_all(stale_lock.parent().unwrap()).unwrap();
    std::fs::write(&stale_lock, []).unwrap();

    let store = CoreStore::new(storage).await.unwrap();

    assert!(!stale_lock.exists());
    let live_lock = store.acquire_named_lock("stream", "live").await.unwrap();
    drop(live_lock);
}

#[tokio::test]
async fn explicit_transaction_stage_waits_for_write_lock_before_named_locks() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let root = "tenant:t/bucket:explicit-lock-order";
    let principal = "principal:explicit-lock-order";
    let transaction = store
        .begin_explicit_transaction(CoreBeginTransaction {
            idempotency_key: "explicit-lock-order".to_string(),
            root_anchor_key: root.to_string(),
            root_key_hash: CoreStore::root_key_hash_for_anchor(root),
            scope_partition: root.to_string(),
            ttl_ms: 60_000,
            purpose: "verify explicit transaction lock order".to_string(),
            principal: principal.to_string(),
            preconditions_hash: ZERO_HASH.to_string(),
        })
        .await
        .unwrap();

    let write_guard = store.write_lock.lock().await;
    let stage_store = store.clone();
    let transaction_id = transaction.transaction_id.clone();
    let stage = tokio::spawn(async move {
        stage_store
            .stage_explicit_transaction_batch(CoreMutationBatch {
                transaction_id,
                scope_partition: root.to_string(),
                committed_by_principal: principal.to_string(),
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: root.to_string(),
                    stream_id: "explicit-lock-order-stream".to_string(),
                    record_kind: "test.explicit-lock-order".to_string(),
                    payload: b"lock-order".to_vec(),
                    idempotency_key: Some("explicit-lock-order-event".to_string()),
                }],
            })
            .await
    });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        count_files_with_extension(&storage.core_store_staging_path().join("locks"), "lock"),
        0,
        "a stage waiting for the process write lock must not hold named locks"
    );

    drop(write_guard);
    tokio::time::timeout(Duration::from_secs(5), stage)
        .await
        .expect("stage should continue after the process write lock is released")
        .expect("stage task should not panic")
        .expect("stage should succeed");
}

#[tokio::test]
async fn admitted_mutation_finalisation_survives_caller_cancellation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let transaction_id = "cancelled-caller-mutation";
    let stream_id = "object_metadata:t:b:cancelled-caller";
    let blocker = store
        .acquire_named_lock("stream", CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap();
    let commit_store = store.clone();
    let commit = tokio::spawn(async move {
        commit_store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: transaction_id.to_string(),
                scope_partition: "tenant:t/bucket:b".to_string(),
                committed_by_principal: "principal:cancellation-test".to_string(),
                preconditions: Vec::new(),
                operations: vec![CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: stream_id.to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"cancelled-caller"}"#.to_vec(),
                    idempotency_key: Some("cancelled-caller-event".to_string()),
                }],
            })
            .await
    });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if !read_test_pending_mutation_records(&store).await.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("mutation should reach its durable admission boundary");

    commit.abort();
    let _ = commit.await;
    drop(blocker);

    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let transaction = store.read_transaction(transaction_id).await.unwrap();
            if transaction
                .as_ref()
                .is_some_and(|transaction| transaction.state == CoreTransactionState::Committed)
                && read_test_pending_mutation_records(&store).await.is_empty()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("detached finalisation should commit and checkpoint the admitted mutation");

    let records = store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
}
