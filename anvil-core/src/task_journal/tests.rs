use super::*;
use serde_json::json;
use tempfile::tempdir;

const KEY: &[u8] = b"task queue partition owner key";

#[tokio::test]
async fn task_journal_claims_and_reads_corestore_current_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    enqueue_task(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
    )
    .await
    .unwrap();
    enqueue_task(
        &storage,
        TaskType::DeleteObject,
        json!({"object_id": 9}),
        10,
    )
    .await
    .unwrap();

    let claimed = claim_pending_tasks(&storage, 1).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].id, 2);
    assert_eq!(claimed[0].status, TaskStatus::Running);

    fail_task(&storage, claimed[0].id, "boom").await.unwrap();
    update_task_status(&storage, 1, TaskStatus::Completed)
        .await
        .unwrap();

    let tasks = list_tasks(&storage).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].status, TaskStatus::Completed);
    assert_eq!(tasks[1].status, TaskStatus::Failed);
    assert_eq!(tasks[1].attempts, 1);
    assert_eq!(tasks[1].last_error.as_deref(), Some("boom"));
}

#[tokio::test]
async fn task_live_state_reads_coremeta_current_rows_without_audit_payloads() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let now = Utc::now();
    let task = TaskRecord {
        id: 42,
        task_type: TaskType::ObjectMetadataCompaction,
        payload: json!({"bucket_id": 7}),
        priority: 50,
        status: TaskStatus::Pending,
        attempts: 0,
        last_error: None,
        scheduled_at: now,
        created_at: now,
        updated_at: now,
    };
    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    write_task_current_row(
        &storage,
        &meta,
        &TaskCurrentCoreMetaRow {
            task,
            generation: 1,
            transaction_id: "task-current-row-only-42".to_string(),
            created_at_unix_nanos: current_unix_nanos().unwrap(),
        },
        None,
    )
    .await
    .unwrap();

    assert!(read_task_journal_bodies(&storage).await.unwrap().is_empty());
    let tasks = list_tasks(&storage).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].id, 42);
    assert_eq!(tasks[0].payload, json!({"bucket_id": 7}));
}

#[tokio::test]
async fn task_live_state_does_not_replay_tampered_audit_payload() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();

    enqueue_task(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
    )
    .await
    .unwrap();

    CoreStore::new(storage.clone())
        .await
        .unwrap()
        .corrupt_stream_record_payload_for_test(&task_queue_stream_id(), 1)
        .unwrap();

    let tasks = list_tasks(&storage).await.unwrap();
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task_type, TaskType::DeleteBucket);

    let err = read_task_journal_bodies(&storage)
        .await
        .expect_err("tampered task queue audit history must still fail closed");
    assert!(!err.to_string().is_empty());
}

#[tokio::test]
pub(crate) async fn task_journal_with_permit_writes_fenced_protobuf_payloads() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();

    enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let claimed = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap();
    update_task_status_with_permit(&storage, claimed[0].id, TaskStatus::Completed, &permit, KEY)
        .await
        .unwrap();

    let fences = read_task_journal_payload_fences(&storage).await.unwrap();
    assert_eq!(fences.len(), 3);
    assert!(fences.iter().all(|fence| *fence == permit.fence_token));
}

#[tokio::test]
pub(crate) async fn task_journal_deduplicates_live_tasks_but_allows_new_after_completion() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();
    let payload = json!({"bucket_id": 7});

    assert!(
        enqueue_task_if_absent_with_permit(
            &storage,
            TaskType::ObjectMetadataCompaction,
            payload.clone(),
            50,
            &permit,
            KEY,
        )
        .await
        .unwrap()
    );
    assert!(
        !enqueue_task_if_absent_with_permit(
            &storage,
            TaskType::ObjectMetadataCompaction,
            payload.clone(),
            50,
            &permit,
            KEY,
        )
        .await
        .unwrap()
    );
    assert_eq!(list_tasks(&storage).await.unwrap().len(), 1);

    let claimed = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap();
    update_task_status_with_permit(&storage, claimed[0].id, TaskStatus::Completed, &permit, KEY)
        .await
        .unwrap();
    assert!(
        enqueue_task_if_absent_with_permit(
            &storage,
            TaskType::ObjectMetadataCompaction,
            payload,
            50,
            &permit,
            KEY,
        )
        .await
        .unwrap()
    );
    assert_eq!(list_tasks(&storage).await.unwrap().len(), 2);
}

#[tokio::test]
async fn task_journal_supersedes_pending_index_build_tasks_by_index_identity() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();
    let payload_cursor_2 = json!({
        "tenant_id": 1,
        "bucket_id": 7,
        "index_id": 9,
        "index_version": 3,
        "source_cursor": 2,
    });
    let payload_cursor_5 = json!({
        "tenant_id": 1,
        "bucket_id": 7,
        "index_id": 9,
        "index_version": 3,
        "source_cursor": 5,
    });

    assert!(
        enqueue_index_build_task_with_permit(&storage, payload_cursor_2, 40, &permit, KEY,)
            .await
            .unwrap()
    );
    assert!(
        enqueue_index_build_task_with_permit(&storage, payload_cursor_5, 40, &permit, KEY,)
            .await
            .unwrap(),
        "newer cursor should supersede the pending task with admitted task events"
    );
    let tasks = list_tasks(&storage).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].status, TaskStatus::Completed);
    assert_eq!(tasks[0].payload["source_cursor"], json!(2));
    assert_eq!(tasks[1].status, TaskStatus::Pending);
    assert_eq!(tasks[1].payload["source_cursor"], json!(5));

    assert!(
        !enqueue_index_build_task_with_permit(
            &storage,
            json!({
                "tenant_id": 1,
                "bucket_id": 7,
                "index_id": 9,
                "index_version": 3,
                "source_cursor": 4,
            }),
            40,
            &permit,
            KEY,
        )
        .await
        .unwrap(),
        "older cursor is already covered by the pending build"
    );

    let claimed = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].payload["source_cursor"], json!(5));

    for body in read_task_journal_bodies(&storage).await.unwrap() {
        assert!(
            matches!(
                body,
                TaskJournalBody::Enqueued { .. }
                    | TaskJournalBody::Claimed { .. }
                    | TaskJournalBody::StatusUpdated { .. }
                    | TaskJournalBody::Failed { .. }
            ),
            "unexpected task queue event: {body:?}"
        );
    }
}

#[tokio::test]
async fn task_journal_serializes_running_and_followup_index_builds() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();
    let payload_cursor_2 = json!({
        "tenant_id": 1,
        "bucket_id": 7,
        "index_id": 9,
        "index_version": 3,
        "source_cursor": 2,
    });
    let payload_cursor_8 = json!({
        "tenant_id": 1,
        "bucket_id": 7,
        "index_id": 9,
        "index_version": 3,
        "source_cursor": 8,
    });

    enqueue_index_build_task_with_permit(&storage, payload_cursor_2, 40, &permit, KEY)
        .await
        .unwrap();
    let first_claim = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].payload["source_cursor"], json!(2));

    enqueue_index_build_task_with_permit(&storage, payload_cursor_8, 40, &permit, KEY)
        .await
        .unwrap();
    let blocked_by_running = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert!(
        blocked_by_running.is_empty(),
        "pending follow-up for the same index must not run beside an active build"
    );

    update_task_status_with_permit(
        &storage,
        first_claim[0].id,
        TaskStatus::Completed,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let followup = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(followup.len(), 1);
    assert_eq!(followup[0].payload["source_cursor"], json!(8));
}

#[tokio::test]
async fn task_journal_coalesces_pending_authz_materialization_by_tenant() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();

    assert!(
        enqueue_authz_materialization_task_with_permit(
            &storage,
            json!({"tenant_id": 42, "target_revision": 10}),
            30,
            &permit,
            KEY,
        )
        .await
        .unwrap()
    );
    assert!(
        enqueue_authz_materialization_task_with_permit(
            &storage,
            json!({"tenant_id": 42, "target_revision": 15}),
            30,
            &permit,
            KEY,
        )
        .await
        .unwrap(),
        "newer authz materialization should supersede the older pending task"
    );
    assert!(
        !enqueue_authz_materialization_task_with_permit(
            &storage,
            json!({"tenant_id": 42, "target_revision": 14}),
            30,
            &permit,
            KEY,
        )
        .await
        .unwrap(),
        "older requested revision is already covered by the pending task"
    );

    let tasks = list_tasks(&storage).await.unwrap();
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[0].status, TaskStatus::Completed);
    assert_eq!(tasks[0].payload["target_revision"], json!(10));
    assert_eq!(tasks[1].status, TaskStatus::Pending);
    assert_eq!(tasks[1].payload["target_revision"], json!(15));
}

#[tokio::test]
async fn task_journal_serializes_running_and_followup_authz_materialization() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();

    enqueue_authz_materialization_task_with_permit(
        &storage,
        json!({"tenant_id": 42, "target_revision": 10}),
        30,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let first_claim = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(first_claim.len(), 1);
    assert_eq!(first_claim[0].payload["target_revision"], json!(10));

    enqueue_authz_materialization_task_with_permit(
        &storage,
        json!({"tenant_id": 42, "target_revision": 20}),
        30,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let blocked_by_running = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert!(
        blocked_by_running.is_empty(),
        "pending follow-up for the same tenant must not run beside active materialization"
    );

    update_task_status_with_permit(
        &storage,
        first_claim[0].id,
        TaskStatus::Completed,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let followup = claim_pending_tasks_with_permit(&storage, 10, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(followup.len(), 1);
    assert_eq!(followup[0].payload["target_revision"], json!(20));
}

#[tokio::test]
pub(crate) async fn task_journal_reclaims_failed_tasks_after_retry_delay() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let permit = owner.write_permit().unwrap();

    enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &permit,
        KEY,
    )
    .await
    .unwrap();
    let first_claim = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap();
    fail_task_with_permit(&storage, first_claim[0].id, "try again", &permit, KEY)
        .await
        .unwrap();
    let not_ready = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap();
    assert!(not_ready.is_empty());

    let mut state = read_task_queue_state(&storage).await.unwrap();
    let task = state.tasks.get_mut(&first_claim[0].id).unwrap();
    task.scheduled_at = Utc::now() - chrono::Duration::seconds(1);
    let partition_precondition = partition_write_precondition(&storage, &permit, KEY)
        .await
        .unwrap();
    append_task_event(
        &storage,
        TaskJournalBody::Failed {
            task_id: task.id,
            error: task.last_error.clone().unwrap(),
            attempts: task.attempts,
            scheduled_at: task.scheduled_at,
            updated_at: Utc::now(),
        },
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
    .unwrap();

    let retried = claim_pending_tasks_with_permit(&storage, 1, &permit, KEY)
        .await
        .unwrap();
    assert_eq!(retried.len(), 1);
    assert_eq!(retried[0].id, first_claim[0].id);
    assert_eq!(retried[0].status, TaskStatus::Running);
    assert_eq!(retried[0].attempts, 1);
}

#[tokio::test]
pub(crate) async fn task_journal_with_permit_rejects_stale_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let stale_permit = owner.write_permit().unwrap();
    let newer = ready_owner(&storage, "node-b").await;
    assert!(newer.fence_token > stale_permit.fence_token);

    let err = enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &stale_permit,
        KEY,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("write permit owner is not current")
    );
}

#[tokio::test]
pub(crate) async fn task_journal_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, "node-a").await;
    let stale_permit = owner.write_permit().unwrap();
    enqueue_task_with_permit(
        &storage,
        TaskType::DeleteBucket,
        json!({"bucket_id": 7}),
        100,
        &stale_permit,
        KEY,
    )
    .await
    .unwrap();
    let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
        .await
        .unwrap();
    let newer = ready_owner(&storage, "node-b").await;
    assert!(newer.fence_token > stale_permit.fence_token);

    let err = append_task_event(
        &storage,
        TaskJournalBody::StatusUpdated {
            task_id: 1,
            status: TaskStatus::Completed,
            updated_at: Utc::now(),
        },
        stale_permit.fence_token,
        Some(stale_precondition),
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string().contains("target mismatch")
            || err.to_string().contains("generation mismatch"),
        "unexpected error: {err:?}"
    );
}

async fn ready_owner(
    storage: &Storage,
    owner_node_id: &str,
) -> crate::partition_fence::PartitionOwnerState {
    let family = "task_queue".to_string();
    let id = hex::encode(task_queue_partition_id());
    crate::partition_fence::ready_partition_owner_for_test(
        storage,
        family,
        id,
        owner_node_id,
        0,
        hex::encode([0; 32]),
        hex::encode([1; 32]),
        KEY,
    )
    .await
}
