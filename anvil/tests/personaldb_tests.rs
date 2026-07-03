use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::personal_db_service_client::PersonalDbServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    CreatePersonalDbGroupRequest, CreatePersonalDbProjectionRequest, GetPersonalDbGroupRequest,
    GetPersonalDbProjectionRequest, PersonalDbCatchUpRequest, PersonalDbVoterAck,
    RepairPersonalDbLogChainRequest, SubmitPersonalDbChangesetRequest, WatchPersonalDbGroupRequest,
    WatchPersonalDbProjectionRequest, WriteAuthzTupleRequest,
};
use anvil::anvil_personaldb_sqlite_changeset::iterate_changeset;
use anvil::formats::hash32;
use anvil::partition_fence::{
    AcquireOwnership, ForceExpireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal,
    OwnershipResource, OwnershipResourceKind, acquire_ownership, force_expire_ownership,
    read_ownership_fence, read_partition_owner,
};
use anvil::personaldb_envelope::{
    PersonalDbEnvelopeDerivationInput, derive_verified_mutation_envelope,
};
use anvil::personaldb_projection::{
    ColumnMapping, ProjectionDefinition, ProjectionResourceBinding, RowFilter, TableMapping,
    WriteBackPolicy,
};
use anvil::personaldb_row_index::read_personaldb_row_index;
use anvil::personaldb_watch::{
    PersonalDbGroupWatchPayload, PersonalDbProjectionWatchPayload,
    append_personaldb_group_watch_record, append_personaldb_projection_watch_record,
};
use anvil_test_utils::*;
use futures_util::StreamExt;
use rusqlite::{Connection, session::Session};
use std::time::Duration;
use tonic::{Code, Request};

const PERSONALDB_TEST_SCHEMA_SQL: &str = "CREATE TABLE items(
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL,
    payload BLOB
);";

const PERSONALDB_PROJECTION_TEST_SCHEMA_SQL: &str = "CREATE TABLE items_projection(
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL
);";

fn personaldb_test_schema_hash() -> String {
    hex::encode(hash32(PERSONALDB_TEST_SCHEMA_SQL.as_bytes()))
}

fn personaldb_projection_test_schema_hash() -> String {
    hex::encode(hash32(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL.as_bytes()))
}

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

#[tokio::test]
async fn personaldb_group_create_get_and_catch_up_are_native_api_backed() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let schema_hash = personaldb_test_schema_hash();
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));

    let created = client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: schema_hash.clone(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let manifest = created.manifest.expect("group manifest");
    assert_eq!(manifest.tenant_id, "1");
    assert_eq!(manifest.database_id, database_id);
    assert_eq!(manifest.schema_hash, schema_hash);
    assert_eq!(manifest.genesis_hash, genesis_hash);
    assert_eq!(manifest.consistency_policy, "StrictWitnessed");
    assert!(!manifest.manifest_hash.is_empty());
    assert!(!manifest.manifest_signature.is_empty());

    let head = created.committed_head.expect("committed head");
    assert_eq!(head.log_index, 0);
    assert_eq!(head.log_hash, genesis_hash);
    assert_eq!(head.segment_path, "");
    assert_eq!(head.policy_epoch, 1);
    assert_eq!(head.membership_epoch, 1);
    assert!(!head.head_hash.is_empty());
    assert!(!head.head_signature.is_empty());

    let fetched = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.manifest.unwrap().database_id, database_id);
    assert_eq!(fetched.committed_head.unwrap().log_hash, genesis_hash);

    let caught_up = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash.clone(),
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert!(caught_up.entries.is_empty());
    assert_eq!(caught_up.committed_head.unwrap().log_hash, genesis_hash);

    let divergent = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id,
                principal: "test-app".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: hex::encode([9; 32]),
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(divergent.snapshot_required);
    assert_eq!(divergent.snapshot_reason, "divergent_replica");
}

#[tokio::test]
async fn personaldb_group_creation_requires_current_rfc_ownership_fence() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::PersonalDbGroup,
        resource_id: format!("tenant/1/personaldb/{database_id}"),
    };
    let now_nanos = chrono::Utc::now().timestamp_nanos_opt().unwrap();

    acquire_ownership(
        &cluster.states[0].storage,
        AcquireOwnership {
            request_id: "other-node-personaldb-owner".to_string(),
            idempotency_key: "other-node-personaldb-owner".to_string(),
            resource: resource.clone(),
            owner: OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: "other-node".to_string(),
                actor_instance_id: "other-node".to_string(),
                display_name: "other-node".to_string(),
                region: "test-region-1".to_string(),
                cell: "default".to_string(),
            },
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap()
                .saturating_mul(1_000_000),
        },
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .unwrap();

    let err = client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: hex::encode(hash32(format!("genesis:{database_id}").as_bytes())),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message().contains("OwnershipHeld"),
        "unexpected error: {err}"
    );

    let fence = read_ownership_fence(
        &cluster.states[0].storage,
        0,
        &resource,
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .unwrap()
    .expect("conflicting owner fence remains durable");
    assert_eq!(fence.owner.principal_id, "other-node");
}

#[tokio::test]
async fn personaldb_submit_commits_and_is_available_to_catch_up_and_watch() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "reader-app".to_string(),
            vec![format!("personaldb:read|tenant-1/{database_id}")],
            1,
        )
        .unwrap();
    let permission_denied = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &limited_token),
            &limited_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(permission_denied.code(), Code::PermissionDenied);

    let malformed = client
        .submit_personal_db_changeset(authorized(
            malformed_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), Code::InvalidArgument);

    let session_mismatch = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, "not-the-bearer-token"),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(session_mismatch.code(), Code::Unauthenticated);

    let commit_only_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "test-app".to_string(),
            vec![format!("personaldb:commit|tenant-1/{database_id}")],
            1,
        )
        .unwrap();
    let row_permission_denied = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &commit_only_token),
            &commit_only_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(row_permission_denied.code(), Code::PermissionDenied);

    let committed = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.log_index, 1);
    assert_eq!(committed.changeset_payload_hash.len(), 64);
    assert_eq!(committed.verified_envelope_hash.len(), 64);
    assert_eq!(committed.certificate_hash.len(), 64);
    assert_eq!(committed.watch_cursor_low, 1);
    assert_eq!(committed.watch_cursor_high, 0);
    assert_eq!(committed.certificate.as_ref().unwrap().log_index, 1);
    assert_eq!(committed.committed_head.as_ref().unwrap().log_index, 1);
    assert_eq!(
        committed
            .committed_head
            .as_ref()
            .unwrap()
            .row_index_generation,
        1
    );

    let row_index_path = cluster.states[0]
        .storage
        .personaldb_row_index_path(1, &database_id, 1, &committed.log_hash)
        .unwrap();
    let row_index = read_personaldb_row_index(row_index_path).await.unwrap();
    assert_eq!(row_index.header.generation, 1);
    assert_eq!(row_index.records.len(), 1);
    assert_eq!(row_index.records[0].database_id, database_id.as_bytes());

    let stale_base = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_base.code(), Code::FailedPrecondition);

    let fetched = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(fetched.committed_head.unwrap().log_index, 1);

    let caught_up = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert_eq!(caught_up.entries.len(), 1);
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
    assert_eq!(
        caught_up.entries[0].changeset_bytes,
        sqlite_insert_changeset()
    );
    assert_eq!(
        caught_up.entries[0].certificate.as_ref().unwrap().log_index,
        1
    );

    let watch = client
        .watch_personal_db_group(authorized(
            WatchPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut stream = watch.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.database_id, database_id);
    assert_eq!(event.event_type, "commit");
    assert_eq!(event.log_index, 1);
    assert_eq!(event.log_hash, committed.log_hash);
    let envelope = event
        .envelope
        .as_ref()
        .expect("PersonalDB group watch envelope");
    assert_eq!(envelope.watch_stream_id, "personaldb_group");
    assert_eq!(envelope.partition_family, "personaldb_group");
    assert_eq!(envelope.cursor_low, event.cursor_low);
    assert_eq!(envelope.personaldb_log_index, event.log_index);
    assert_eq!(envelope.authz_revision, event.authz_revision);
    assert_eq!(envelope.record_kind, "personaldb_group");
    assert!(!envelope.payload_hash.is_empty());
}

#[tokio::test]
async fn personaldb_repair_verifies_log_chain_and_reports_missing_payload() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut personaldb = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut repair = RepairServiceClient::connect(grpc_addr).await.unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = create_group(&mut personaldb, &token, &database_id).await;
    let committed = personaldb
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let healthy = repair
        .repair_personal_db_log_chain(authorized(
            RepairPersonalDbLogChainRequest {
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(healthy.status, "up_to_date");
    assert_eq!(healthy.committed_log_index, 1);
    assert_eq!(healthy.verified_log_index, 1);
    assert_eq!(healthy.committed_log_hash, committed.log_hash);
    assert!(healthy.finding.is_none());

    let payload_path = cluster.states[0]
        .storage
        .personaldb_changeset_payload_by_index_path(
            1,
            &database_id,
            committed.log_index,
            &committed.changeset_payload_hash,
        )
        .unwrap();
    tokio::fs::remove_file(payload_path).await.unwrap();

    let report = repair
        .repair_personal_db_log_chain(authorized(
            RepairPersonalDbLogChainRequest {
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "needs_review");
    assert_eq!(report.reason, "PersonalDbChangesetPayloadMissing");
    assert_eq!(report.committed_log_index, 1);
    assert_eq!(report.verified_log_index, 0);
    let finding = report.finding.expect("repair finding");
    assert_eq!(finding.scope_kind, "personaldb");
    assert_eq!(finding.scope_id, format!("tenant-1-database-{database_id}"));
    assert_eq!(finding.status, "RequiresOperatorReview");
    assert_eq!(finding.proposed_action, "VerifyOnly");
}

#[tokio::test]
async fn personaldb_concurrent_same_base_submits_publish_one_witness_commit() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut setup_client = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = create_group(&mut setup_client, &token, &database_id).await;

    let mut first_client = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut second_client = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let first = first_client.submit_personal_db_changeset(authorized(
        submit_request(
            &database_id,
            &genesis_hash,
            &token,
            sqlite_insert_changeset_with_item(1, "alpha", &[1_u8, 2, 3]),
        ),
        &token,
    ));
    let second = second_client.submit_personal_db_changeset(authorized(
        submit_request(
            &database_id,
            &genesis_hash,
            &token,
            sqlite_insert_changeset_with_item(2, "beta", &[4_u8, 5, 6]),
        ),
        &token,
    ));

    let (first, second) = tokio::join!(first, second);
    let mut successes = Vec::new();
    let mut failures = Vec::new();
    for result in [first, second] {
        match result {
            Ok(response) => successes.push(response.into_inner()),
            Err(status) => failures.push(status),
        }
    }

    assert_eq!(
        successes.len(),
        1,
        "only one same-base submit can publish a witnessed commit"
    );
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].code(), Code::FailedPrecondition);
    assert_eq!(successes[0].log_index, 1);

    let fetched = setup_client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let committed_head = fetched.committed_head.unwrap();
    assert_eq!(committed_head.log_index, 1);
    assert_eq!(committed_head.log_hash, successes[0].log_hash);

    let caught_up = setup_client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert_eq!(
        caught_up.entries.len(),
        1,
        "canonical log must not contain duplicate witness commits"
    );
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().entry_hash,
        successes[0].log_hash
    );
}

#[tokio::test]
async fn personaldb_group_owner_handoff_after_force_expiry_commits_once_across_nodes() {
    let mut cluster = TestCluster::new(&["test-region-1", "test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(20)).await;

    let token = cluster.token.clone();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let mut node_a = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut node_b = PersonalDbServiceClient::connect(cluster.grpc_addrs[1].clone())
        .await
        .unwrap();

    let genesis_hash = create_group(&mut node_a, &token, &database_id).await;
    let first = node_a
        .submit_personal_db_changeset(authorized(
            submit_request(
                &database_id,
                &genesis_hash,
                &token,
                sqlite_insert_changeset_with_item(1, "alpha", &[1_u8, 2, 3]),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.log_index, 1);

    let partition_id = personaldb_group_partition_id_for_test(1, &database_id);
    let first_owner = read_partition_owner(
        &cluster.states[0].storage,
        "personaldb_group",
        &partition_id,
        cluster.states[0]
            .config
            .anvil_secret_encryption_key
            .as_bytes(),
    )
    .await
    .unwrap()
    .expect("first commit writes partition owner");
    assert_eq!(first_owner.owner_node_id, cluster.states[0].config.node_id);

    let ownership_resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::PersonalDbGroup,
        resource_id: format!("tenant/1/personaldb/{database_id}"),
    };
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .expect("valid timestamp");
    force_expire_ownership(
        &cluster.states[0].storage,
        ForceExpireOwnership {
            request_id: format!("force-expire-personaldb-{database_id}"),
            idempotency_key: format!("force-expire-personaldb-{database_id}"),
            resource: ownership_resource,
            admin: OwnershipPrincipal {
                tenant_id: 0,
                principal_kind: "node".to_string(),
                principal_id: cluster.states[0].config.node_id.clone(),
                actor_instance_id: cluster.states[0].config.node_id.clone(),
                display_name: cluster.states[0].config.node_id.clone(),
                region: if cluster.states[0].config.region.is_empty() {
                    "default".to_string()
                } else {
                    cluster.states[0].config.region.clone()
                },
                cell: if cluster.states[0].config.cell_id.is_empty() {
                    "default".to_string()
                } else {
                    cluster.states[0].config.cell_id.clone()
                },
            },
            reason: "personaldb owner handoff test".to_string(),
            now_nanos,
        },
        &hex::decode(&cluster.states[0].config.anvil_secret_encryption_key).unwrap(),
    )
    .await
    .expect("force expiry allows owner handoff");

    let second = node_b
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &database_id,
                first.log_index,
                &first.log_hash,
                &token,
                sqlite_insert_changeset_with_item(2, "beta", &[4_u8, 5, 6]),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.log_index, 2);

    let second_owner = read_partition_owner(
        &cluster.states[0].storage,
        "personaldb_group",
        &partition_id,
        cluster.states[0]
            .config
            .anvil_secret_encryption_key
            .as_bytes(),
    )
    .await
    .unwrap()
    .expect("second commit publishes new partition owner");
    assert_eq!(second_owner.owner_node_id, cluster.states[1].config.node_id);
    assert_eq!(second_owner.fence_token, first_owner.fence_token + 1);

    let stale_base = node_a
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &database_id,
                first.log_index,
                &first.log_hash,
                &token,
                sqlite_insert_changeset_with_item(3, "gamma", &[7_u8, 8, 9]),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(stale_base.code(), Code::FailedPrecondition);

    let caught_up = node_b
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-owner-transfer".to_string(),
                have_log_index: 0,
                have_log_hash: genesis_hash,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!caught_up.snapshot_required);
    assert_eq!(
        caught_up.entries.len(),
        2,
        "owner handoff must not duplicate witnessed commits"
    );
    assert_eq!(
        caught_up.entries[0].log_record.as_ref().unwrap().entry_hash,
        first.log_hash
    );
    assert_eq!(
        caught_up.entries[1].log_record.as_ref().unwrap().entry_hash,
        second.log_hash
    );
}

#[tokio::test]
async fn personaldb_row_mutation_can_be_authorized_by_relationship_tuple() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut personaldb = PersonalDbServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(grpc_addr).await.unwrap();

    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    personaldb
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let commit_only_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "test-app".to_string(),
            vec![format!("personaldb:commit|tenant-1/{database_id}")],
            1,
        )
        .unwrap();
    let changeset_bytes = sqlite_insert_changeset();
    let denied = personaldb
        .submit_personal_db_changeset(authorized(
            submit_request(
                &database_id,
                &genesis_hash,
                &commit_only_token,
                changeset_bytes.clone(),
            ),
            &commit_only_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(denied.code(), Code::PermissionDenied);

    let changes = iterate_changeset(&changeset_bytes).unwrap();
    let envelope = derive_verified_mutation_envelope(PersonalDbEnvelopeDerivationInput {
        tenant_id: 1,
        database_id: &database_id,
        principal: "test-app",
        base_log_index: 0,
        proposed_log_index: 1,
        changeset_payload_hash: hash32(&changeset_bytes),
        schema_hash: &personaldb_test_schema_hash(),
        policy_epoch: 1,
        authz_revision: 1,
        changes: &changes,
        updated_at_nanos: 1,
    })
    .unwrap();
    let effect = envelope
        .table_effects
        .first()
        .expect("insert changeset should derive one effect");
    let binding = &effect.source_resource_binding;
    let resource = format!(
        "tenant-1/{}/{}/{}",
        database_id, binding.resource_type, binding.resource_id
    );
    let permission = effect
        .required_permissions
        .first()
        .expect("effect should require a row mutation permission")
        .clone();

    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: "personaldb_row".to_string(),
                object_id: resource,
                relation: permission,
                subject_kind: "app".to_string(),
                subject_id: "test-app".to_string(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "test".to_string(),
                scope: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let committed = personaldb
        .submit_personal_db_changeset(authorized(
            submit_request(
                &database_id,
                &genesis_hash,
                &commit_only_token,
                changeset_bytes,
            ),
            &commit_only_token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.log_index, 1);
    assert_eq!(committed.verified_envelope_hash.len(), 64);
}

#[tokio::test]
async fn personaldb_submit_builds_snapshot_when_threshold_is_reached() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    std::sync::Arc::make_mut(&mut cluster.states[0].config).personaldb_snapshot_entry_threshold = 1;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.clone(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let committed = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&database_id, &genesis_hash, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let divergent = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-a".to_string(),
                have_log_index: 0,
                have_log_hash: hex::encode([9; 32]),
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    assert!(divergent.snapshot_required);
    assert_eq!(divergent.snapshot_reason, "divergent_replica");
    let snapshots_head = divergent.snapshots_head.expect("snapshots head");
    assert_eq!(snapshots_head.latest_snapshot_log_index, 1);
    assert_eq!(snapshots_head.latest_snapshot_log_hash, committed.log_hash);
    let manifest_path = cluster.states[0]
        .storage
        .resolve_relative_storage_path(&snapshots_head.latest_snapshot_manifest_path)
        .unwrap();
    assert!(manifest_path.exists());
}

#[tokio::test]
async fn personaldb_projection_definition_create_get_and_watch_are_native_api_backed() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    create_group(&mut client, &token, &source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition(&projection_database_id, &source_database_id);
    let created = client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let created_definition: ProjectionDefinition =
        serde_json::from_str(&created.projection_definition_json).unwrap();
    created_definition.verify().unwrap();
    assert_eq!(created_definition.database_id, projection_database_id);
    assert_eq!(
        created_definition.source_database_ids,
        vec![source_database_id.clone()]
    );
    assert!(created_definition.definition_hash.as_ref().unwrap().len() == 64);

    let fetched = client
        .get_personal_db_projection(authorized(
            GetPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_id: "projection-items".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let fetched_definition: ProjectionDefinition =
        serde_json::from_str(&fetched.projection_definition_json).unwrap();
    assert_eq!(fetched_definition, created_definition);

    let payload = PersonalDbProjectionWatchPayload {
        database_id: projection_database_id.clone(),
        projection_id: "projection-items".to_string(),
        event_type: "projection_committed".to_string(),
        source_database_id: source_database_id.clone(),
        source_log_index: 1,
        source_log_hash: hex::encode([1; 32]),
        projection_log_index: 1,
        projection_log_hash: hex::encode([2; 32]),
        definition_hash: created_definition.definition_hash.unwrap(),
        emitted_at: "2026-06-28T00:00:00Z".to_string(),
    };
    append_personaldb_projection_watch_record(
        &cluster.states[0].storage,
        1,
        &projection_database_id,
        "projection-items",
        77,
        *uuid::Uuid::new_v4().as_bytes(),
        12,
        payload,
    )
    .await
    .unwrap();
    let watch = client
        .watch_personal_db_projection(authorized(
            WatchPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_id: "projection-items".to_string(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut stream = watch.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.cursor_low, 77);
    assert_eq!(event.cursor_high, 0);
    assert_eq!(event.database_id, projection_database_id);
    assert_eq!(event.projection_id, "projection-items");
    assert_eq!(event.source_database_id, source_database_id);
    assert_eq!(event.authz_revision, 12);
    let envelope = event
        .envelope
        .as_ref()
        .expect("PersonalDB projection watch envelope");
    assert_eq!(envelope.watch_stream_id, "personaldb_projection");
    assert_eq!(envelope.partition_family, "personaldb_projection");
    assert_eq!(envelope.cursor_low, event.cursor_low);
    assert_eq!(envelope.personaldb_log_index, event.projection_log_index);
    assert_eq!(envelope.authz_revision, event.authz_revision);
    assert_eq!(envelope.record_kind, "personaldb_projection");
    assert!(!envelope.payload_hash.is_empty());
}

#[tokio::test]
async fn personaldb_source_commit_builds_projection_group_and_watch_event() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition(&projection_database_id, &source_database_id);
    let created = client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let created_definition: ProjectionDefinition =
        serde_json::from_str(&created.projection_definition_json).unwrap();

    let source_commit = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&source_database_id, &source_genesis, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_commit.log_index, 1);

    let projected = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-projection".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!projected.snapshot_required);
    assert_eq!(projected.entries.len(), 1);
    assert_eq!(
        projected.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
    let projection_db = Connection::open_in_memory().unwrap();
    projection_db
        .execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    projection_db
        .apply_strm(
            &mut std::io::Cursor::new(&projected.entries[0].changeset_bytes),
            None::<fn(&str) -> bool>,
            |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
        )
        .unwrap();
    let name: String = projection_db
        .query_row(
            "SELECT name FROM items_projection WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(name, "alpha");

    let watch = client
        .watch_personal_db_projection(authorized(
            WatchPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_id: "projection-items".to_string(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let mut stream = watch.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.database_id, projection_database_id);
    assert_eq!(event.projection_id, "projection-items");
    assert_eq!(event.event_type, "projection_committed");
    assert_eq!(event.source_database_id, source_database_id);
    assert_eq!(event.source_log_index, 1);
    assert_eq!(event.source_log_hash, source_commit.log_hash);
    assert_eq!(event.projection_log_index, 1);
    assert_eq!(
        event.definition_hash,
        created_definition.definition_hash.unwrap()
    );
}

#[tokio::test]
async fn personaldb_projection_resource_relation_filter_uses_authz_index() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition =
        projection_definition_with_resource_filter(&projection_database_id, &source_database_id);
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();

    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: "personaldb_row".to_string(),
                object_id: format!("tenant-1/{source_database_id}/items/1"),
                relation: "viewer".to_string(),
                subject_kind: "app".to_string(),
                subject_id: "scope-primary".to_string(),
                caveat_hash: String::new(),
                operation: "add".to_string(),
                reason: "allow projection target".to_string(),
                scope: None,
            },
            &token,
        ))
        .await
        .unwrap();

    let source_commit = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&source_database_id, &source_genesis, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_commit.log_index, 1);

    let projected = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-projection-authz".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(!projected.snapshot_required);
    assert_eq!(projected.entries.len(), 1);
    assert_eq!(
        projected.entries[0].log_record.as_ref().unwrap().log_index,
        1
    );
}

#[tokio::test]
async fn personaldb_projection_group_submit_rejects_direct_writeback_when_policy_denies() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition(&projection_database_id, &source_database_id);
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();
    client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&source_database_id, &source_genesis, &token),
            &token,
        ))
        .await
        .unwrap();

    let projection_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");
    assert_eq!(projection_head_before.log_index, 1);

    let err = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &projection_database_id,
                projection_head_before.log_index,
                &projection_head_before.log_hash,
                &token,
                sqlite_projection_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    let projection_head_after = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");
    assert_eq!(
        projection_head_after.log_index,
        projection_head_before.log_index
    );
    assert_eq!(
        projection_head_after.log_hash,
        projection_head_before.log_hash
    );
}

#[tokio::test]
async fn personaldb_projection_writeback_updates_source_and_rebuilds_projection_when_allowed() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition =
        projection_definition_allowing_name_writeback(&projection_database_id, &source_database_id);
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();
    let source_insert = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&source_database_id, &source_genesis, &token),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();

    let projection_head = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");
    assert_eq!(projection_head.log_index, 1);

    let source_writeback = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &projection_database_id,
                projection_head.log_index,
                &projection_head.log_hash,
                &token,
                sqlite_projection_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_insert.log_index, 1);
    assert_eq!(source_writeback.log_index, 2);

    let source_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: source_database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-source".to_string(),
                have_log_index: 0,
                have_log_hash: source_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_catchup.entries.len(), 2);
    let source_db = Connection::open_in_memory().unwrap();
    source_db.execute_batch(PERSONALDB_TEST_SCHEMA_SQL).unwrap();
    for entry in &source_catchup.entries {
        source_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let source_name: String = source_db
        .query_row("SELECT name FROM items WHERE id = 1", [], |row| row.get(0))
        .unwrap();
    assert_eq!(source_name, "beta");

    let projection_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: projection_database_id,
                principal: "test-app".to_string(),
                replica_id: "replica-projection".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection_catchup.entries.len(), 2);
    assert_eq!(
        projection_catchup.entries[1]
            .log_record
            .as_ref()
            .unwrap()
            .log_index,
        2
    );
    let projection_db = Connection::open_in_memory().unwrap();
    projection_db
        .execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    for entry in &projection_catchup.entries {
        projection_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let projection_name: String = projection_db
        .query_row(
            "SELECT name FROM items_projection WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projection_name, "beta");
}

#[tokio::test]
async fn personaldb_projection_writeback_insert_and_delete_round_trip_through_source_group() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let projection_genesis = create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_allowing_id_name_writeback(
        &projection_database_id,
        &source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();

    let inserted = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &projection_database_id,
                0,
                &projection_genesis,
                &token,
                sqlite_projection_insert_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(inserted.log_index, 1);

    let projection_head_after_insert = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head after insert");
    assert_eq!(projection_head_after_insert.log_index, 1);

    let deleted = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &projection_database_id,
                projection_head_after_insert.log_index,
                &projection_head_after_insert.log_hash,
                &token,
                sqlite_projection_delete_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.log_index, 2);

    let source_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: source_database_id.clone(),
                principal: "test-app".to_string(),
                replica_id: "replica-source-insert-delete".to_string(),
                have_log_index: 0,
                have_log_hash: source_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source_catchup.entries.len(), 2);
    let source_db = Connection::open_in_memory().unwrap();
    source_db.execute_batch(PERSONALDB_TEST_SCHEMA_SQL).unwrap();
    for entry in &source_catchup.entries {
        source_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let source_count: i64 = source_db
        .query_row("SELECT COUNT(*) FROM items WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(source_count, 0);

    let projection_catchup = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: 1,
                database_id: projection_database_id,
                principal: "test-app".to_string(),
                replica_id: "replica-projection-insert-delete".to_string(),
                have_log_index: 0,
                have_log_hash: projection_genesis,
                max_entries: 10,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(projection_catchup.entries.len(), 2);
    let projection_db = Connection::open_in_memory().unwrap();
    projection_db
        .execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    for entry in &projection_catchup.entries {
        projection_db
            .apply_strm(
                &mut std::io::Cursor::new(&entry.changeset_bytes),
                None::<fn(&str) -> bool>,
                |_conflict, _item| rusqlite::session::ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .unwrap();
    }
    let projection_count: i64 = projection_db
        .query_row(
            "SELECT COUNT(*) FROM items_projection WHERE id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(projection_count, 0);
}

#[tokio::test]
async fn personaldb_projection_writeback_rejects_protected_column_mutation() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition =
        projection_definition_allowing_name_writeback(&projection_database_id, &source_database_id);
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();
    client
        .submit_personal_db_changeset(authorized(
            valid_submit_request(&source_database_id, &source_genesis, &token),
            &token,
        ))
        .await
        .unwrap();

    let source_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: source_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("source committed head");
    let projection_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");

    let err = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &projection_database_id,
                projection_head_before.log_index,
                &projection_head_before.log_hash,
                &token,
                sqlite_projection_id_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    let source_head_after = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: source_database_id,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("source committed head");
    assert_eq!(source_head_after.log_index, source_head_before.log_index);
    assert_eq!(source_head_after.log_hash, source_head_before.log_hash);
}

#[tokio::test]
async fn personaldb_projection_writeback_rejects_ambiguous_source_binding() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let first_source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let second_source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let first_source_genesis = create_group(&mut client, &token, &first_source_database_id).await;
    let second_source_genesis = create_group(&mut client, &token, &second_source_database_id).await;
    create_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
    )
    .await;

    let definition = projection_definition_with_ambiguous_writeback(
        &projection_database_id,
        &first_source_database_id,
        &second_source_database_id,
    );
    client
        .create_personal_db_projection(authorized(
            CreatePersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
            },
            &token,
        ))
        .await
        .unwrap();

    let projection_head_before = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .expect("projection committed head");

    let err = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base(
                &projection_database_id,
                projection_head_before.log_index,
                &projection_head_before.log_hash,
                &token,
                sqlite_projection_update_changeset(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(
        err.message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    for (database_id, genesis_hash) in [
        (&first_source_database_id, &first_source_genesis),
        (&second_source_database_id, &second_source_genesis),
    ] {
        let source_head = client
            .get_personal_db_group(authorized(
                GetPersonalDbGroupRequest {
                    tenant_id: 1,
                    database_id: database_id.to_string(),
                },
                &token,
            ))
            .await
            .unwrap()
            .into_inner()
            .committed_head
            .expect("source committed head");
        assert_eq!(source_head.log_index, 0);
        assert_eq!(&source_head.log_hash, genesis_hash);
    }
}

#[tokio::test]
async fn personaldb_api_rejects_cross_tenant_request_scope() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let err = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 999,
                database_id: "db-alpha".to_string(),
            },
            &cluster.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn personaldb_group_watch_streams_reserved_internal_events_through_native_api() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let payload = PersonalDbGroupWatchPayload {
        database_id: database_id.clone(),
        event_type: "commit".to_string(),
        log_index: 7,
        log_hash: hex::encode([7; 32]),
        changeset_payload_hash: hex::encode([8; 32]),
        certificate_hash: hex::encode([9; 32]),
        committed_head_hash: hex::encode([10; 32]),
        emitted_at: "2026-06-28T00:00:00Z".to_string(),
    };
    append_personaldb_group_watch_record(
        &cluster.states[0].storage,
        1,
        &database_id,
        42,
        *uuid::Uuid::new_v4().as_bytes(),
        11,
        payload,
    )
    .await
    .unwrap();

    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let response = client
        .watch_personal_db_group(authorized(
            WatchPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: database_id.clone(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &cluster.token,
        ))
        .await
        .unwrap();
    let mut stream = response.into_inner();
    let event = stream.next().await.unwrap().unwrap();
    assert_eq!(event.database_id, database_id);
    assert_eq!(event.cursor_low, 42);
    assert_eq!(event.cursor_high, 0);
    assert_eq!(event.event_type, "commit");
    assert_eq!(event.log_index, 7);
    assert_eq!(event.authz_revision, 11);
}

fn valid_submit_request(
    database_id: &str,
    genesis_hash: &str,
    session_token: &str,
) -> SubmitPersonalDbChangesetRequest {
    let changeset_bytes = sqlite_insert_changeset();
    submit_request(database_id, genesis_hash, session_token, changeset_bytes)
}

async fn create_group(
    client: &mut PersonalDbServiceClient<tonic::transport::Channel>,
    token: &str,
    database_id: &str,
) -> String {
    create_group_with_schema(
        client,
        token,
        database_id,
        PERSONALDB_TEST_SCHEMA_SQL,
        &personaldb_test_schema_hash(),
    )
    .await
}

async fn create_group_with_schema(
    client: &mut PersonalDbServiceClient<tonic::transport::Channel>,
    token: &str,
    database_id: &str,
    schema_sql: &str,
    schema_hash: &str,
) -> String {
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.to_string(),
                schema_hash: schema_hash.to_string(),
                genesis_hash: genesis_hash.clone(),
                schema_sql: schema_sql.to_string(),
            },
            token,
        ))
        .await
        .unwrap();
    genesis_hash
}

fn personaldb_group_partition_id_for_test(tenant_id: i64, database_id: &str) -> String {
    hex::encode(hash32(
        format!("personaldb_group\0{tenant_id}\0{database_id}").as_bytes(),
    ))
}

fn projection_definition(
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    ProjectionDefinition {
        format_version: 1,
        tenant_id: "1".to_string(),
        database_id: projection_database_id.to_string(),
        projection_id: "projection-items".to_string(),
        source_database_ids: vec![source_database_id.to_string()],
        target_database_id: projection_database_id.to_string(),
        target_actor_or_scope: "scope-primary".to_string(),
        table_mappings: vec![TableMapping {
            source_database_id: source_database_id.to_string(),
            source_table: "items".to_string(),
            target_table: "items_projection".to_string(),
        }],
        column_mappings: vec![
            ColumnMapping {
                source_table: "items".to_string(),
                source_column: "id".to_string(),
                target_table: "items_projection".to_string(),
                target_column: "id".to_string(),
            },
            ColumnMapping {
                source_table: "items".to_string(),
                source_column: "name".to_string(),
                target_table: "items_projection".to_string(),
                target_column: "name".to_string(),
            },
        ],
        row_filters: vec![RowFilter::FieldEqualsLiteral {
            table: "items".to_string(),
            field: "id".to_string(),
            literal: "1".to_string(),
        }],
        resource_bindings: vec![ProjectionResourceBinding {
            source_table: "items".to_string(),
            primary_key_column: "id".to_string(),
            resource_type: "items".to_string(),
            resource_id_column: "id".to_string(),
            parent_resource_id_column: None,
        }],
        writeback_policy: WriteBackPolicy::Deny,
        definition_hash: None,
    }
}

fn projection_definition_allowing_name_writeback(
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition = projection_definition(projection_database_id, source_database_id);
    definition.writeback_policy = WriteBackPolicy::AllowMappedColumns {
        protected_columns: vec!["id".to_string()],
        allowed_columns: vec!["name".to_string()],
    };
    definition
}

fn projection_definition_with_resource_filter(
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition = projection_definition(projection_database_id, source_database_id);
    definition.row_filters = vec![RowFilter::ResourceRelationAllows {
        table: "items".to_string(),
        resource_id_field: "id".to_string(),
        relation: "viewer".to_string(),
    }];
    definition
}

fn projection_definition_allowing_id_name_writeback(
    projection_database_id: &str,
    source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition = projection_definition(projection_database_id, source_database_id);
    definition.writeback_policy = WriteBackPolicy::AllowMappedColumns {
        protected_columns: Vec::new(),
        allowed_columns: vec!["id".to_string(), "name".to_string()],
    };
    definition
}

fn projection_definition_with_ambiguous_writeback(
    projection_database_id: &str,
    first_source_database_id: &str,
    second_source_database_id: &str,
) -> ProjectionDefinition {
    let mut definition = projection_definition_allowing_name_writeback(
        projection_database_id,
        first_source_database_id,
    );
    definition
        .source_database_ids
        .push(second_source_database_id.to_string());
    definition.table_mappings.push(TableMapping {
        source_database_id: second_source_database_id.to_string(),
        source_table: "items".to_string(),
        target_table: "items_projection".to_string(),
    });
    definition
}

fn malformed_submit_request(
    database_id: &str,
    genesis_hash: &str,
    session_token: &str,
) -> SubmitPersonalDbChangesetRequest {
    submit_request(
        database_id,
        genesis_hash,
        session_token,
        b"not a sqlite changeset".to_vec(),
    )
}

fn submit_request(
    database_id: &str,
    genesis_hash: &str,
    session_token: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    submit_request_at_base(database_id, 0, genesis_hash, session_token, changeset_bytes)
}

fn submit_request_at_base(
    database_id: &str,
    base_log_index: u64,
    base_log_hash: &str,
    session_token: &str,
    changeset_bytes: Vec<u8>,
) -> SubmitPersonalDbChangesetRequest {
    SubmitPersonalDbChangesetRequest {
        tenant_id: 1,
        database_id: database_id.to_string(),
        principal: "test-app".to_string(),
        session_token: session_token.to_string(),
        request_id: format!("request-{base_log_index}"),
        idempotency_key: format!("idem-{base_log_index}"),
        base_log_index,
        base_log_hash: base_log_hash.to_string(),
        client_log_epoch: base_log_index.saturating_add(1),
        membership_epoch: 1,
        policy_epoch: 1,
        leader_replica_id: "leader-a".to_string(),
        voter_acks: vec![PersonalDbVoterAck {
            replica_id: "replica-a".to_string(),
            log_index: base_log_index.saturating_add(1),
            log_hash: hex::encode([8; 32]),
            signature: "signature-a".to_string(),
        }],
        changeset_payload_hash: hex::encode(hash32(&changeset_bytes)),
        changeset_bytes,
        client_debug_metadata_json: String::new(),
    }
}

fn sqlite_insert_changeset() -> Vec<u8> {
    sqlite_insert_changeset_with_item(1, "alpha", &[1_u8, 2, 3])
}

fn sqlite_insert_changeset_with_item(id: i64, name: &str, payload: &[u8]) -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE items(
            id INTEGER PRIMARY KEY NOT NULL,
            name TEXT NOT NULL,
            payload BLOB
        );",
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute(
        "INSERT INTO items (id, name, payload) VALUES (?1, ?2, ?3)",
        rusqlite::params![id, name, payload],
    )
    .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_update_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("UPDATE items_projection SET name = 'beta' WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_insert_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_delete_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("DELETE FROM items_projection WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}

fn sqlite_projection_id_update_changeset() -> Vec<u8> {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(PERSONALDB_PROJECTION_TEST_SCHEMA_SQL)
        .unwrap();
    db.execute(
        "INSERT INTO items_projection (id, name) VALUES (1, 'alpha')",
        [],
    )
    .unwrap();
    let mut session = Session::new(&db).unwrap();
    session.attach::<&str>(None).unwrap();
    db.execute("UPDATE items_projection SET id = 2 WHERE id = 1", [])
        .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}
