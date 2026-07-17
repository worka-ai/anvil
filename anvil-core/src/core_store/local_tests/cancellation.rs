use super::*;
use std::time::Duration;

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
