use super::*;

#[tokio::test]
async fn personaldb_projection_group_is_born_with_immutable_definition() {
    let cluster = shared_default_test_cluster().await;
    let token = cluster.token.clone();
    let mut client = PersonalDbServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    create_group(&mut client, &token, &source_database_id).await;
    let definition = projection_definition(&projection_database_id, &source_database_id);
    create_projection_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
        &definition,
    )
    .await;

    let group = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: 1,
                database_id: projection_database_id.clone(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let manifest = group.manifest.expect("projection manifest");
    assert_eq!(manifest.format_version, 3);
    assert_eq!(manifest.proposer_signature_purpose, "projection-proposer");
    assert_eq!(manifest.current_projection_generation, 1);
    assert_eq!(manifest.active_policy_epoch, 1);
    assert_eq!(
        manifest.projection_source_database_ids,
        vec![source_database_id.clone()]
    );
    assert!(
        manifest
            .projection_definition_ref
            .contains("projection-items")
    );
    assert_eq!(manifest.projection_definition_hash.len(), 64);
    assert!(manifest.genesis_authorization_revision.is_some());
    let builder_policy: KeyTrustPolicy =
        serde_json::from_str(&manifest.projection_builder_key_policy_json).unwrap();
    assert_eq!(builder_policy.purpose, SignaturePurpose::ProjectionBuilder);

    let fetched = client
        .get_personal_db_projection(authorized(
            GetPersonalDbProjectionRequest {
                tenant_id: 1,
                database_id: projection_database_id,
                projection_id: "projection-items".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let fetched_definition: ProjectionDefinition =
        serde_json::from_str(&fetched.projection_definition_json).unwrap();
    fetched_definition.verify().unwrap();
    assert_eq!(
        fetched_definition.definition_hash.as_deref(),
        Some(manifest.projection_definition_hash.as_str())
    );
    assert_eq!(fetched_definition.writeback_policy, WriteBackPolicy::Deny);
}

#[tokio::test]
async fn personaldb_source_commit_builds_projection_group_and_watch_event() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-projection").await;
    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let definition = projection_definition_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    let projection_genesis = create_projection_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
        &definition,
    )
    .await;

    let source_commit = client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let projected = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id.clone(),
                principal: actor.app_id.clone(),
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
    assert_eq!(projected.entries.len(), 1);
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
                tenant_id: actor.tenant_id,
                database_id: projection_database_id,
                projection_id: "projection-items".to_string(),
                after_cursor_low: 0,
                after_cursor_high: 0,
            },
            &token,
        ))
        .await
        .unwrap();
    let event = watch.into_inner().next().await.unwrap().unwrap();
    assert_eq!(event.source_database_id, source_database_id);
    assert_eq!(event.source_log_hash, source_commit.log_hash);
    assert_eq!(event.projection_log_index, 1);
}

#[tokio::test]
async fn personaldb_projection_resource_relation_filter_uses_authz_index() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-projection-authz").await;
    grant_default_authz_tuple_writer(&cluster, &actor).await;
    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let mut auth_client = AuthServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let source_genesis = create_group(&mut client, &token, &source_database_id).await;
    let definition = projection_definition_with_resource_filter_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    let projection_genesis = create_projection_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
        &definition,
    )
    .await;

    auth_client
        .write_authz_tuple(authorized(
            WriteAuthzTupleRequest {
                namespace: "personaldb_row".to_string(),
                object_id: format!("tenant-{}/{source_database_id}/items/1", actor.tenant_id),
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
    client
        .submit_personal_db_changeset(authorized(
            valid_submit_request_for_actor(&actor, &source_database_id, &source_genesis),
            &token,
        ))
        .await
        .unwrap();
    let projected = client
        .catch_up_personal_db(authorized(
            PersonalDbCatchUpRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id,
                principal: actor.app_id.clone(),
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
    assert_eq!(projected.entries.len(), 1);
}

#[tokio::test]
async fn personaldb_projection_owner_submit_rejects_before_changeset_parsing() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-projection-deny").await;
    let token = actor.token.clone();
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    create_group(&mut client, &token, &source_database_id).await;
    let definition = projection_definition_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    let projection_genesis = create_projection_group_with_schema(
        &mut client,
        &token,
        &projection_database_id,
        PERSONALDB_PROJECTION_TEST_SCHEMA_SQL,
        &personaldb_projection_test_schema_hash(),
        &definition,
    )
    .await;

    let error = client
        .submit_personal_db_changeset(authorized(
            submit_request_at_base_for_actor(
                &actor,
                &projection_database_id,
                0,
                &projection_genesis,
                b"not-a-sqlite-changeset".to_vec(),
            ),
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("PersonalDbProjectionWriteBackRejected")
    );

    let head = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id,
                database_id: projection_database_id,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .committed_head
        .unwrap();
    assert_eq!(head.log_index, 0);
    assert_eq!(head.log_hash, projection_genesis);
}

#[tokio::test]
async fn personaldb_projection_genesis_rejects_writeback_policy_other_than_deny() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-projection-policy").await;
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let source_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    let projection_database_id = format!("db-{}", uuid::Uuid::new_v4().simple());
    create_group(&mut client, &actor.token, &source_database_id).await;
    let definition = projection_definition_allowing_name_writeback_for_tenant(
        actor.tenant_id,
        &projection_database_id,
        &source_database_id,
    );
    let genesis_hash = hex::encode(hash32(
        format!("genesis:{projection_database_id}").as_bytes(),
    ));
    let error = client
        .create_personal_db_group(authorized(
            CreatePersonalDbGroupRequest {
                database_id: projection_database_id.clone(),
                schema_hash: personaldb_projection_test_schema_hash(),
                genesis_hash,
                schema_sql: PERSONALDB_PROJECTION_TEST_SCHEMA_SQL.to_string(),
                proposer_signature_purpose: SignaturePurpose::ProjectionProposer
                    .as_str()
                    .to_string(),
                policy_epoch: 1,
                projection_definition_json: serde_json::to_string(&definition).unwrap(),
                projection_builder_key_policy_json: projection_builder_policy_json(
                    &projection_database_id,
                ),
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("ManifestInvalid"));
    assert!(error.message().contains("Deny write-back policy"));
}

#[tokio::test]
async fn personaldb_api_rejects_cross_tenant_request_scope() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_personaldb_test_actor(&cluster, "personaldb-cross-tenant").await;
    let mut client = PersonalDbServiceClient::connect(actor.grpc_addr.clone())
        .await
        .unwrap();
    let error = client
        .get_personal_db_group(authorized(
            GetPersonalDbGroupRequest {
                tenant_id: actor.tenant_id + 1,
                database_id: "db-alpha".to_string(),
            },
            &actor.token,
        ))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::PermissionDenied);
}
