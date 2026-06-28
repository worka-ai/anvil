use anvil::anvil_api::personal_db_service_client::PersonalDbServiceClient;
use anvil::anvil_api::{
    CreatePersonalDbGroupRequest, CreatePersonalDbProjectionRequest, GetPersonalDbGroupRequest,
    GetPersonalDbProjectionRequest, PersonalDbCatchUpRequest, PersonalDbVoterAck,
    SubmitPersonalDbChangesetRequest, WatchPersonalDbGroupRequest,
    WatchPersonalDbProjectionRequest,
};
use anvil::formats::hash32;
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

fn personaldb_test_schema_hash() -> String {
    hex::encode(hash32(PERSONALDB_TEST_SCHEMA_SQL.as_bytes()))
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
    create_group(&mut client, &token, &projection_database_id).await;

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
) {
    let genesis_hash = hex::encode(hash32(format!("genesis:{database_id}").as_bytes()));
    client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: database_id.to_string(),
                schema_hash: personaldb_test_schema_hash(),
                genesis_hash,
                schema_sql: PERSONALDB_TEST_SCHEMA_SQL.to_string(),
            },
            token,
        ))
        .await
        .unwrap();
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
        row_filters: vec![RowFilter::NotDeleted {
            table: "items".to_string(),
            deleted_field: "deleted_at".to_string(),
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
    SubmitPersonalDbChangesetRequest {
        tenant_id: 1,
        database_id: database_id.to_string(),
        principal: "test-app".to_string(),
        session_token: session_token.to_string(),
        request_id: "request-1".to_string(),
        idempotency_key: "idem-1".to_string(),
        base_log_index: 0,
        base_log_hash: genesis_hash.to_string(),
        client_log_epoch: 1,
        membership_epoch: 1,
        policy_epoch: 1,
        leader_replica_id: "leader-a".to_string(),
        voter_acks: vec![PersonalDbVoterAck {
            replica_id: "replica-a".to_string(),
            log_index: 1,
            log_hash: hex::encode([8; 32]),
            signature: "signature-a".to_string(),
        }],
        changeset_payload_hash: hex::encode(hash32(&changeset_bytes)),
        changeset_bytes,
        client_debug_metadata_json: String::new(),
    }
}

fn sqlite_insert_changeset() -> Vec<u8> {
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
        "INSERT INTO items (id, name, payload) VALUES (1, 'alpha', ?1)",
        [vec![1_u8, 2, 3]],
    )
    .unwrap();
    let mut output = Vec::new();
    session.changeset_strm(&mut output).unwrap();
    assert!(!output.is_empty());
    output
}
