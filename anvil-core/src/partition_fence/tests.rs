use super::*;
use crate::core_store::PutBlob;
use crate::formats::JournalRecordKind;
use tempfile::tempdir;

const KEY: &[u8] = b"partition owner signing key";

#[tokio::test]
async fn recovery_acquire_blocks_writes_until_owner_ready() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let recovering = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    assert_eq!(recovering.fence_token, 1);
    assert_eq!(recovering.status, PartitionOwnerStatus::Recovering);

    let permit = PartitionWritePermit {
        partition_family: recovering.partition_family.clone(),
        partition_id: recovering.partition_id.clone(),
        owner_node_id: "node-a".to_string(),
        fence_token: recovering.fence_token,
    };
    let rejected = validate_partition_write(&storage, &permit, KEY)
        .await
        .unwrap_err();
    assert_eq!(rejected.code, AnvilErrorCode::PartitionNotOwned);

    let ready = publish_partition_ready(
        &storage,
        &recovering.partition_family,
        &recovering.partition_id,
        "node-a",
        recovering.fence_token,
        77,
        &hex::encode([9; 32]),
        200,
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(ready.status, PartitionOwnerStatus::Ready);
    assert_eq!(ready.recovered_through_sequence, 77);
    validate_partition_write(&storage, &ready.write_permit().unwrap(), KEY)
        .await
        .unwrap();
}

#[tokio::test]
async fn owner_handoff_rejects_stale_fence_token() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    let first = publish_partition_ready(
        &storage,
        &first.partition_family,
        &first.partition_id,
        "node-a",
        first.fence_token,
        10,
        &hex::encode([3; 32]),
        150,
        KEY,
    )
    .await
    .unwrap();
    let stale_permit = first.write_permit().unwrap();

    let second = acquire_partition_recovery(&storage, acquire("node-b", 300), KEY)
        .await
        .unwrap();
    assert_eq!(second.fence_token, first.fence_token + 1);
    let stale_rejection = validate_partition_write(&storage, &stale_permit, KEY)
        .await
        .unwrap_err();
    assert_eq!(stale_rejection.code, AnvilErrorCode::PartitionNotOwned);

    let second = publish_partition_ready(
        &storage,
        &second.partition_family,
        &second.partition_id,
        "node-b",
        second.fence_token,
        20,
        &hex::encode([4; 32]),
        350,
        KEY,
    )
    .await
    .unwrap();
    validate_partition_write(&storage, &second.write_permit().unwrap(), KEY)
        .await
        .unwrap();

    let mut stale_same_owner = second.write_permit().unwrap();
    stale_same_owner.fence_token -= 1;
    let stale_rejection = validate_partition_write(&storage, &stale_same_owner, KEY)
        .await
        .unwrap_err();
    assert_eq!(stale_rejection.code, AnvilErrorCode::StaleFenceToken);
}

#[test]
fn recovery_replay_keeps_current_fence_after_manifest_checkpoint() {
    let stale_before = frame(9, 1, [0; 32]);
    let current_after = frame(11, 2, stale_before.record_hash);
    let stale_after = frame(12, 1, current_after.record_hash);
    let frames = vec![stale_before, current_after.clone(), stale_after];

    assert_eq!(
        frames_for_recovered_fence(&frames, 10, 2),
        vec![current_after]
    );
    let rejection = reject_stale_frames_after_checkpoint(&frames, 10, 2).unwrap_err();
    assert_eq!(rejection.code, AnvilErrorCode::StaleFenceToken);
    reject_stale_frames_after_checkpoint(&frames[..2], 10, 2).unwrap();
}

#[tokio::test]
async fn partition_owner_state_is_signed_and_path_scoped() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = acquire_partition_recovery(&storage, acquire("node-a", 100), KEY)
        .await
        .unwrap();
    let (ref_value, _) =
        read_partition_owner_state(&storage, &owner.partition_family, &owner.partition_id, KEY)
            .await
            .unwrap()
            .unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let object_ref = decode_core_object_ref_target(&ref_value.target).unwrap();
    let mut value: serde_json::Value =
        serde_json::from_slice(&store.get_blob(GetBlob { object_ref }).await.unwrap()).unwrap();
    value["fence_token"] = serde_json::json!(99);
    let tampered = store
        .put_blob(PutBlob {
            logical_name: "partition-owner-tamper".to_string(),
            bytes: serde_json::to_vec_pretty(&value).unwrap(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "partition-owner-tamper".to_string(),
        })
        .await
        .unwrap();
    store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: partition_owner_ref_name(&owner.partition_family, &owner.partition_id)
                .unwrap(),
            expected_generation: Some(ref_value.generation),
            expected_target: Some(ref_value.target),
            require_absent: false,
            require_present: true,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&tampered).unwrap(),
            transaction_id: None,
        })
        .await
        .unwrap();
    assert!(
        read_partition_owner(&storage, &owner.partition_family, &owner.partition_id, KEY)
            .await
            .is_err()
    );
    assert!(partition_owner_ref_name("../escape", &owner.partition_id).is_err());
}

#[tokio::test]
async fn ownership_label_is_not_security_identity() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner_a = principal("app-a", "token-a", "node-shared");
    let owner_b = principal("app-b", "token-b", "node-shared");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner_a.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    assert_eq!(first.owner.display_name, "node-shared");
    assert_eq!(first.owner.principal_id, "app-a");
    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "renew-b".to_string(),
                resource: ownership_resource(),
                owner: owner_b.clone(),
                current_fence: first.fence,
                now_nanos: 200,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );
    assert!(
        release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-b".to_string(),
                idempotency_key: "release-b".to_string(),
                resource: ownership_resource(),
                owner: owner_b,
                current_fence: first.fence,
                administrative_force: false,
                now_nanos: 250,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );
}

#[tokio::test]
async fn expired_ownership_can_be_acquired_and_increments_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let first = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-a", "token-a", "node-a"),
            100,
            50,
            "acquire-a",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let second = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-b", "token-b", "node-b"),
            200,
            50,
            "acquire-b",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;

    assert_eq!(second.fence, first.fence + 1);
    assert_eq!(second.owner.principal_id, "app-b");
    assert_eq!(second.state, OwnershipFenceState::Active);
}

#[tokio::test]
async fn ownership_operations_reject_stale_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;
    let stale_fence = first.fence + 1;

    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "renew-stale".to_string(),
                resource: ownership_resource(),
                owner: owner.clone(),
                current_fence: stale_fence,
                now_nanos: 200,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );
    assert!(
        transfer_ownership(
            &storage,
            TransferOwnership {
                request_id: "transfer-stale".to_string(),
                idempotency_key: "transfer-stale".to_string(),
                resource: ownership_resource(),
                current_owner: owner.clone(),
                new_owner: principal("app-b", "token-b", "node-b"),
                current_fence: stale_fence,
                now_nanos: 220,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );
    assert!(
        release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-stale".to_string(),
                idempotency_key: "release-stale".to_string(),
                resource: ownership_resource(),
                owner,
                current_fence: stale_fence,
                administrative_force: false,
                now_nanos: 240,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );
}

#[tokio::test]
async fn concurrent_ownership_acquires_have_one_winner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let mut tasks = Vec::new();
    for idx in 0..16 {
        let storage = storage.clone();
        tasks.push(tokio::spawn(async move {
            acquire_ownership(
                &storage,
                ownership_acquire(
                    principal(
                        format!("app-{idx}"),
                        format!("token-{idx}"),
                        format!("node-{idx}"),
                    ),
                    100,
                    500,
                    format!("acquire-{idx}"),
                ),
                KEY,
            )
            .await
        }));
    }

    let mut successes = 0;
    let mut held = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(_) => successes += 1,
            Err(err) if err.to_string().contains(OWNERSHIP_HELD) => held += 1,
            Err(err) => panic!("unexpected ownership error: {err}"),
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(held, 15);
}

#[tokio::test]
async fn force_expire_increments_fence_and_blocks_stale_owner() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let expired = force_expire_ownership(
        &storage,
        ForceExpireOwnership {
            request_id: "force-expire".to_string(),
            idempotency_key: "force-expire".to_string(),
            resource: ownership_resource(),
            admin: principal("admin", "admin-token", "admin"),
            reason: "test failover".to_string(),
            now_nanos: 200,
        },
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(expired.state, OwnershipFenceState::Expired);
    assert_eq!(expired.fence, first.fence + 1);

    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "stale-renew".to_string(),
                resource: ownership_resource(),
                owner,
                current_fence: first.fence,
                now_nanos: 220,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_STALE_FENCE)
    );

    let replacement = acquire_ownership(
        &storage,
        ownership_acquire(
            principal("app-b", "token-b", "node-b"),
            250,
            500,
            "acquire-b",
        ),
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(replacement.fence, expired.fence + 1);
}

#[tokio::test]
async fn transfer_moves_to_explicit_target_identity_and_is_idempotent() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let new_owner = principal("app-b", "token-b", "node-b");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner.clone(), 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    let transferred = transfer_ownership(
        &storage,
        TransferOwnership {
            request_id: "transfer".to_string(),
            idempotency_key: "transfer-key".to_string(),
            resource: ownership_resource(),
            current_owner: owner.clone(),
            new_owner: new_owner.clone(),
            current_fence: first.fence,
            now_nanos: 200,
            ttl_nanos: 500,
        },
        KEY,
    )
    .await
    .unwrap();
    assert_eq!(transferred.record.fence, first.fence + 1);
    assert!(transferred.record.owner.same_security_owner(&new_owner));
    assert!(!transferred.record.owner.same_security_owner(&owner));
    assert!(!transferred.idempotent_replay);

    let replay = transfer_ownership(
        &storage,
        TransferOwnership {
            request_id: "transfer-replay".to_string(),
            idempotency_key: "transfer-key".to_string(),
            resource: ownership_resource(),
            current_owner: owner.clone(),
            new_owner,
            current_fence: first.fence,
            now_nanos: 220,
            ttl_nanos: 500,
        },
        KEY,
    )
    .await
    .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(replay.record.fence, transferred.record.fence);

    assert!(
        renew_ownership(
            &storage,
            RenewOwnership {
                request_id: "old-owner-renew".to_string(),
                resource: ownership_resource(),
                owner,
                current_fence: transferred.record.fence,
                now_nanos: 230,
                ttl_nanos: 500,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );
}

#[tokio::test]
async fn release_requires_owner_and_fence_unless_force() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = principal("app-a", "token-a", "node-a");
    let other = principal("app-b", "token-b", "node-b");
    let first = acquire_ownership(
        &storage,
        ownership_acquire(owner, 100, 500, "acquire-a"),
        KEY,
    )
    .await
    .unwrap()
    .record;

    assert!(
        release_ownership(
            &storage,
            ReleaseOwnership {
                request_id: "release-other".to_string(),
                idempotency_key: "release-other".to_string(),
                resource: ownership_resource(),
                owner: other.clone(),
                current_fence: first.fence,
                administrative_force: false,
                now_nanos: 200,
            },
            KEY,
        )
        .await
        .unwrap_err()
        .to_string()
        .contains(OWNERSHIP_OWNER_MISMATCH)
    );

    let released = release_ownership(
        &storage,
        ReleaseOwnership {
            request_id: "release-force".to_string(),
            idempotency_key: "release-force".to_string(),
            resource: ownership_resource(),
            owner: other,
            current_fence: 0,
            administrative_force: true,
            now_nanos: 220,
        },
        KEY,
    )
    .await
    .unwrap()
    .record;
    assert_eq!(released.state, OwnershipFenceState::Released);
    assert_eq!(released.fence, first.fence + 1);
}

fn acquire(owner_node_id: &str, now_nanos: i64) -> PartitionRecoveryAcquire {
    PartitionRecoveryAcquire {
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode([7; 32]),
        owner_node_id: owner_node_id.to_string(),
        recovered_through_sequence: 0,
        recovered_manifest_hash: hex::encode([0; 32]),
        now_nanos,
    }
}

fn ownership_acquire(
    owner: OwnershipPrincipal,
    now_nanos: i64,
    ttl_nanos: i64,
    idempotency_key: impl Into<String>,
) -> AcquireOwnership {
    AcquireOwnership {
        request_id: format!("req-{}", now_nanos),
        idempotency_key: idempotency_key.into(),
        resource: ownership_resource(),
        owner,
        now_nanos,
        ttl_nanos,
    }
}

fn ownership_resource() -> OwnershipResource {
    OwnershipResource {
        resource_kind: OwnershipResourceKind::BucketPrimary,
        resource_id: "tenant-acme/releases".to_string(),
    }
}

fn principal(
    principal_id: impl Into<String>,
    actor_instance_id: impl Into<String>,
    display_name: impl Into<String>,
) -> OwnershipPrincipal {
    OwnershipPrincipal {
        tenant_id: 1,
        principal_kind: "app".to_string(),
        principal_id: principal_id.into(),
        actor_instance_id: actor_instance_id.into(),
        display_name: display_name.into(),
        region: "eu-west-1".to_string(),
        cell: "cell-a".to_string(),
    }
}

fn frame(sequence: u64, fence_token: u64, previous_hash: [u8; 32]) -> JournalFrame {
    JournalFrame::new(
        JournalRecordKind::ObjectVersion,
        sequence,
        fence_token,
        [sequence as u8; 16],
        [fence_token as u8; 32],
        previous_hash,
        vec![sequence as u8, fence_token as u8],
    )
}
