use super::*;
use crate::partition_fence::{
    PartitionRecoveryAcquire, acquire_partition_recovery, force_expire_partition_owner_for_node,
    publish_partition_ready,
};
use chrono::Utc;
use tempfile::tempdir;

const PARTITION_OWNER_KEY: &[u8] = b"authorization tuple partition owner signing key";

fn record(revision: i64, operation: &str) -> AuthzTupleRecord {
    AuthzTupleRecord {
        revision,
        revision_ordinal: 0,
        tenant_id: 42,
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        written_by: "tester".to_string(),
        reason: "test".to_string(),
        mutation_id: uuid::Uuid::new_v4(),
        record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
        written_at: Utc::now(),
    }
}

fn tuple(
    revision: i64,
    namespace: &str,
    object_id: &str,
    relation: &str,
    subject_kind: &str,
    subject_id: &str,
    operation: &str,
) -> AuthzTupleRecord {
    AuthzTupleRecord {
            revision,
            revision_ordinal: 0,
            tenant_id: 42,
            namespace: namespace.to_string(),
            object_id: object_id.to_string(),
            relation: relation.to_string(),
            subject_kind: subject_kind.to_string(),
            subject_id: subject_id.to_string(),
            caveat_hash: String::new(),
            operation: operation.to_string(),
            written_by: "tester".to_string(),
            reason: "test".to_string(),
            mutation_id: uuid::Uuid::new_v4(),
            record_hash: hex::encode(hash32(
                format!(
                    "record-{revision}-{namespace}-{object_id}-{relation}-{subject_kind}-{subject_id}-{operation}"
                )
                .as_bytes(),
            )),
            written_at: Utc::now(),
        }
}

async fn ready_authz_permit(
    storage: &Storage,
    tenant_id: i64,
    owner_node_id: &str,
) -> PartitionWritePermit {
    let request = PartitionRecoveryAcquire {
        partition_family: "authz_tuple".to_string(),
        partition_id: hex::encode(authz_partition_id(tenant_id)),
        owner_node_id: owner_node_id.to_string(),
        recovered_through_sequence: 0,
        recovered_manifest_hash: hex::encode([0; 32]),
        now_nanos: 100,
    };
    let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    publish_partition_ready(
        storage,
        &recovering.partition_family,
        &recovering.partition_id,
        owner_node_id,
        recovering.fence_token,
        0,
        &hex::encode([3; 32]),
        200,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap()
    .write_permit()
    .unwrap()
}

async fn append_authz_record_without_segment(
    storage: &Storage,
    record: &AuthzTupleRecord,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let partition_id = hex::encode(authz_partition_id(record.tenant_id));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("authz-tuple-unmaterialized:{}", record.mutation_id),
            scope_partition: partition_id.clone(),
            committed_by_principal: authz_partition_principal(record.tenant_id),
            preconditions: Vec::new(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id: authz_tuple_stream_id(record.tenant_id),
                record_kind: AUTHZ_TUPLE_RECORD_KIND.to_string(),
                payload: encode_authz_tuple_journal_body(record, 0)?,
                idempotency_key: Some(format!("authz-tuple-unmaterialized:{}", record.mutation_id)),
            }],
        })
        .await?;
    write_authz_tuple_records_to_current_rows(storage, std::slice::from_ref(record)).await
}

#[tokio::test]
async fn authz_journal_recovers_latest_exact_and_watch_ranges() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    test_append_authz_tuple_record_unfenced(&storage, &record(1, "add"))
        .await
        .unwrap();
    test_append_authz_tuple_record_unfenced(&storage, &record(2, "remove"))
        .await
        .unwrap();

    assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 2);
    assert_eq!(
        check_authz_tuple(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
        .unwrap()
        .operation,
        "remove"
    );
    assert_eq!(
        check_authz_tuple_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 1
        )
        .await
        .unwrap()
        .unwrap()
        .operation,
        "add"
    );
    let watched = list_authz_tuple_log(&storage, 42, 0, "document", 10)
        .await
        .unwrap();
    assert_eq!(watched.len(), 2);
    assert_eq!(watched[1].revision, 2);
}

#[tokio::test]
async fn latest_authz_revision_uses_the_journal_head_not_a_tuple_scan() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let record = record(1, "add");
    test_append_authz_tuple_record_unfenced(&storage, &record)
        .await
        .unwrap();

    let meta = CoreMetaStore::open(storage.core_store_meta_path()).unwrap();
    meta.delete(
        CF_AUTHZ,
        TABLE_AUTHZ_TUPLE_PAGE_ROW,
        &authz_tuple_current_row_key(&record).unwrap(),
    )
    .unwrap();

    assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 1);
}

#[tokio::test]
async fn missing_authz_segments_materialize_only_the_requested_revision() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for revision in 1..=3 {
        append_authz_record_without_segment(&storage, &record(revision, "add"))
            .await
            .unwrap();
    }

    assert_eq!(latest_authz_revision(&storage, 42).await.unwrap(), 3);
    for revision in 1..=3 {
        assert!(
            authz_segment::existing_authz_tuple_segment_ref(&storage, 42, revision)
                .unwrap()
                .is_none()
        );
    }

    let segment = authz_segment::ensure_authz_tuple_segment_at_revision(&storage, 42, 3)
        .await
        .unwrap()
        .expect("requested authorization segment");
    assert_eq!(segment.header.generation, 3);
    assert_eq!(segment.records.len(), 3);
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 1)
            .unwrap()
            .is_none()
    );
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 2)
            .unwrap()
            .is_none()
    );
    assert!(
        authz_segment::existing_authz_tuple_segment_ref(&storage, 42, 3)
            .unwrap()
            .is_some()
    );
}

#[test]
fn caveat_hash_validation_accepts_empty_or_hex32_only() {
    validate_optional_caveat_hash("").unwrap();
    validate_optional_caveat_hash(&hex::encode([7; 32])).unwrap();
    validate_optional_caveat_hash("not-hex32").unwrap_err();
    validate_optional_caveat_hash(&hex::encode([7; 31])).unwrap_err();
}

#[tokio::test]
async fn authz_resolves_direct_and_nested_userset_tuples() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    assert!(
        resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 3
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "bob", "", 3
        )
        .await
        .unwrap()
    );
    assert!(
        resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "bob", ""
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn authz_userset_removal_and_cycles_do_not_grant_access() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
        tuple(
            4,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "remove",
        ),
        tuple(
            5,
            "group",
            "a",
            "member",
            "userset",
            "group/b#member",
            "add",
        ),
        tuple(
            6,
            "group",
            "b",
            "member",
            "userset",
            "group/a#member",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    assert!(
        resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 3
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_current_permission(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", ""
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_permission_at_revision(
            &storage, 42, "document", "alpha", "viewer", "user", "alice", "", 4
        )
        .await
        .unwrap()
    );
    assert!(
        !resolve_permission_at_revision(
            &storage, 42, "group", "a", "member", "user", "alice", "", 6
        )
        .await
        .unwrap()
    );
}

#[tokio::test]
async fn authz_bound_schema_inherit_computed_and_tuple_to_userset_rules_are_enforced() {
    use crate::anvil_api::{AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema};
    use crate::authz_scope::encode_realm_namespace;

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let realm_id = "realm_a";
    let document_ns = encode_realm_namespace(realm_id, "document");
    let folder_ns = encode_realm_namespace(realm_id, "folder");
    let tenant_ns = encode_realm_namespace(realm_id, "tenant");
    let group_ns = encode_realm_namespace(realm_id, "group");
    let schema = crate::authz_realm_schema::put_schema_revision(
        &storage,
        42,
        "workspace-authz",
        vec![
            AuthzNamespaceSchema {
                namespace: "document".to_string(),
                relations: vec![
                    AuthzRelationSchema {
                        relation: "viewer".to_string(),
                        rules: vec![
                            AuthzRelationRule {
                                kind: "inherit".to_string(),
                                relation: "editor".to_string(),
                                tuple_relation: String::new(),
                                target_relation: String::new(),
                            },
                            AuthzRelationRule {
                                kind: "computed".to_string(),
                                relation: String::new(),
                                tuple_relation: "parent_folder".to_string(),
                                target_relation: "viewer".to_string(),
                            },
                            AuthzRelationRule {
                                kind: "tuple_to_userset".to_string(),
                                relation: String::new(),
                                tuple_relation: "shared_group".to_string(),
                                target_relation: "member".to_string(),
                            },
                        ],
                    },
                    AuthzRelationSchema {
                        relation: "editor".to_string(),
                        rules: vec![],
                    },
                ],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            AuthzNamespaceSchema {
                namespace: "folder".to_string(),
                relations: vec![AuthzRelationSchema {
                    relation: "viewer".to_string(),
                    rules: vec![AuthzRelationRule {
                        kind: "computed".to_string(),
                        relation: String::new(),
                        tuple_relation: "parent_tenant".to_string(),
                        target_relation: "member".to_string(),
                    }],
                }],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            AuthzNamespaceSchema {
                namespace: "tenant".to_string(),
                relations: vec![AuthzRelationSchema {
                    relation: "member".to_string(),
                    rules: vec![],
                }],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
            AuthzNamespaceSchema {
                namespace: "group".to_string(),
                relations: vec![AuthzRelationSchema {
                    relation: "member".to_string(),
                    rules: vec![],
                }],
                schema_json: String::new(),
                schema_hash: String::new(),
                schema_version: 0,
                authz_revision: 0,
                applied_at: String::new(),
            },
        ],
        1,
        "tester",
        "test schema",
    )
    .await
    .unwrap();
    crate::authz_realm_schema::bind_schema(
        &storage,
        42,
        realm_id,
        schema.schema_ref,
        None,
        2,
        "tester",
        "bind schema",
    )
    .await
    .unwrap();

    for record in [
        tuple(
            1,
            &document_ns,
            "alpha",
            "editor",
            "user",
            "direct-editor",
            "add",
        ),
        tuple(
            2,
            &tenant_ns,
            "acme",
            "member",
            "user",
            "tenant-member",
            "add",
        ),
        tuple(
            3,
            &folder_ns,
            "platform",
            "parent_tenant",
            "tenant",
            "acme",
            "add",
        ),
        tuple(
            4,
            &document_ns,
            "alpha",
            "parent_folder",
            "folder",
            "platform",
            "add",
        ),
        tuple(
            5,
            &group_ns,
            "engineering",
            "member",
            "user",
            "group-member",
            "add",
        ),
        tuple(
            6,
            &document_ns,
            "alpha",
            "shared_group",
            "group",
            "engineering",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    for subject in ["direct-editor", "tenant-member", "group-member"] {
        assert!(
            resolve_permission_at_revision(
                &storage,
                42,
                &document_ns,
                "alpha",
                "viewer",
                "user",
                subject,
                "",
                6,
            )
            .await
            .unwrap(),
            "{subject} should be granted by the bound schema"
        );
    }
    assert_eq!(
        list_current_authz_objects_at_revision(
            &storage,
            42,
            &document_ns,
            "viewer",
            "user",
            "tenant-member",
            "",
            6,
        )
        .await
        .unwrap(),
        vec!["alpha".to_string()]
    );
    assert_eq!(
        list_current_authz_subjects_at_revision(
            &storage,
            42,
            &document_ns,
            "alpha",
            "viewer",
            Some("user"),
            6,
        )
        .await
        .unwrap(),
        vec![
            AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "direct-editor".to_string(),
                caveat_hash: String::new(),
            },
            AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "group-member".to_string(),
                caveat_hash: String::new(),
            },
            AuthzSubjectRef {
                subject_kind: "user".to_string(),
                subject_id: "tenant-member".to_string(),
                caveat_hash: String::new(),
            },
        ]
    );
}

#[tokio::test]
async fn authz_schema_writes_materialize_segment_schema_tables() {
    use crate::anvil_api::{AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema};
    use crate::authz_scope::encode_realm_namespace;

    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let schema = crate::authz_realm_schema::put_schema_revision(
        &storage,
        42,
        "workspace-authz",
        vec![AuthzNamespaceSchema {
            namespace: "document".to_string(),
            relations: vec![AuthzRelationSchema {
                relation: "viewer".to_string(),
                rules: vec![AuthzRelationRule {
                    kind: "inherit".to_string(),
                    relation: "editor".to_string(),
                    tuple_relation: String::new(),
                    target_relation: String::new(),
                }],
            }],
            schema_json: String::new(),
            schema_hash: String::new(),
            schema_version: 0,
            authz_revision: 0,
            applied_at: String::new(),
        }],
        10,
        "tester",
        "put schema",
    )
    .await
    .unwrap();

    let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.generation, 10);
    assert!(segment.records.is_empty());
    assert!(segment.schema_descriptors.iter().any(|row| {
        row.realm_id.is_empty()
            && row.namespace == "document"
            && row.schema_id == "workspace-authz"
            && row.schema_revision == 1
    }));
    assert!(segment.relation_rules.iter().any(|row| {
        row.realm_id.is_empty()
            && row.namespace == "document"
            && row.relation == "viewer"
            && row.rule_kind == "inherit"
            && row.inherited_relation == "editor"
    }));

    crate::authz_realm_schema::bind_schema(
        &storage,
        42,
        "workspace-a",
        schema.schema_ref,
        None,
        11,
        "tester",
        "bind schema",
    )
    .await
    .unwrap();
    let bound_segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    let bound_namespace = encode_realm_namespace("workspace-a", "document");
    assert_eq!(bound_segment.header.generation, 11);
    assert!(bound_segment.schema_descriptors.iter().any(|row| {
        row.realm_id == "workspace-a"
            && row.namespace == bound_namespace
            && row.schema_id == "workspace-authz"
            && row.binding_generation == 1
    }));
    assert!(bound_segment.relation_rules.iter().any(|row| {
        row.realm_id == "workspace-a"
            && row.namespace == bound_namespace
            && row.relation == "viewer"
            && row.rule_kind == "inherit"
            && row.inherited_relation == "editor"
    }));
    assert!(
        bound_segment
            .revision_checkpoints
            .iter()
            .any(|row| row.revision == 11)
    );
}

#[tokio::test]
async fn authz_current_tuple_reads_filter_active_adds_only() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "document", "alpha", "viewer", "user", "alice", "add"),
        tuple(2, "document", "beta", "viewer", "user", "alice", "add"),
        tuple(3, "document", "beta", "viewer", "user", "alice", "remove"),
        tuple(4, "document", "alpha", "editor", "user", "bob", "add"),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    let active_viewers = read_current_authz_tuples_at_revision(
        &storage,
        42,
        AuthzTupleFilter {
            namespace: Some("document".to_string()),
            relation: Some("viewer".to_string()),
            subject_kind: Some("user".to_string()),
            subject_id: Some("alice".to_string()),
            caveat_hash: Some(String::new()),
            ..AuthzTupleFilter::default()
        },
        4,
    )
    .await
    .unwrap();
    assert_eq!(active_viewers.len(), 1);
    assert_eq!(active_viewers[0].object_id, "alpha");

    let historical_viewers = read_current_authz_tuples_at_revision(
        &storage,
        42,
        AuthzTupleFilter {
            namespace: Some("document".to_string()),
            relation: Some("viewer".to_string()),
            subject_kind: Some("user".to_string()),
            subject_id: Some("alice".to_string()),
            caveat_hash: Some(String::new()),
            ..AuthzTupleFilter::default()
        },
        2,
    )
    .await
    .unwrap();
    assert_eq!(
        historical_viewers
            .iter()
            .map(|record| record.object_id.as_str())
            .collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );

    assert_eq!(
        list_current_authz_objects_at_revision(
            &storage, 42, "document", "viewer", "user", "alice", "", 4
        )
        .await
        .unwrap(),
        vec!["alpha".to_string()]
    );
    assert_eq!(
        list_current_authz_subjects_at_revision(
            &storage,
            42,
            "document",
            "alpha",
            "editor",
            Some("user"),
            4
        )
        .await
        .unwrap(),
        vec![AuthzSubjectRef {
            subject_kind: "user".to_string(),
            subject_id: "bob".to_string(),
            caveat_hash: String::new(),
        }]
    );
}

#[tokio::test]
async fn authz_tuple_writes_materialize_userset_and_reverse_lookup_segments() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple(1, "group", "engineering", "member", "user", "alice", "add"),
        tuple(
            2,
            "folder",
            "platform",
            "viewer",
            "userset",
            "group/engineering#member",
            "add",
        ),
        tuple(
            3,
            "document",
            "alpha",
            "viewer",
            "userset",
            "folder/platform#viewer",
            "add",
        ),
    ] {
        test_append_authz_tuple_record_unfenced(&storage, &record)
            .await
            .unwrap();
    }

    authz_segment::ensure_authz_tuple_segment_at_revision(&storage, 42, 3)
        .await
        .unwrap()
        .unwrap();

    let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.generation, 3);
    assert_eq!(segment.records.len(), 3);
    assert_eq!(
        segment
            .revision_checkpoints
            .last()
            .expect("latest revision checkpoint")
            .tuple_record_count,
        3
    );
    assert!(segment.userset_edges.iter().any(|row| {
        row.namespace == "document"
            && row.object_id == "alpha"
            && row.relation == "viewer"
            && row.subject_kind == "user"
            && row.subject_id == "alice"
            && row.source == "derived_userset"
    }));
    assert!(segment.list_objects.iter().any(|row| {
        row.namespace == "document"
            && row.relation == "viewer"
            && row.subject_kind == "user"
            && row.subject_id == "alice"
            && row.object_id == "alpha"
    }));
    assert!(segment.list_subjects.iter().any(|row| {
        row.namespace == "document"
            && row.object_id == "alpha"
            && row.relation == "viewer"
            && row.subject_kind == "user"
            && row.subject_id == "alice"
    }));
}

#[tokio::test]
async fn authz_journal_permit_sets_payload_and_segment_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let permit = ready_authz_permit(&storage, 42, "node-a").await;

    append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();

    let fences = read_authz_journal_payload_fences(&storage, 42)
        .await
        .unwrap();
    assert_eq!(fences, vec![permit.fence_token]);

    authz_segment::ensure_authz_tuple_segment_at_revision(&storage, 42, 1)
        .await
        .unwrap()
        .unwrap();

    let segment = authz_segment::read_latest_authz_tuple_segment(&storage, 42)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.source_fence_token, permit.fence_token);
    assert_eq!(segment.revision_checkpoints.len(), 1);
    assert_eq!(
        segment.revision_checkpoints[0].source_fence_token,
        permit.fence_token
    );
}

#[tokio::test]
async fn authz_journal_rejects_stale_partition_permit() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let stale = ready_authz_permit(&storage, 42, "node-a").await;
    force_expire_partition_owner_for_node(
        &storage,
        &stale.partition_family,
        &stale.partition_id,
        "node-a",
        250,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap()
    .unwrap();
    let fresh = ready_authz_permit(&storage, 42, "node-b").await;
    assert!(fresh.fence_token > stale.fence_token);

    let rejected = append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &stale,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(rejected.to_string().contains("PartitionNotOwned"));

    append_authz_tuple_record_with_permit(&storage, &record(1, "add"), &fresh, PARTITION_OWNER_KEY)
        .await
        .unwrap();
}

#[tokio::test]
async fn authz_journal_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let stale = ready_authz_permit(&storage, 42, "node-a").await;
    let stale_precondition = partition_write_precondition(&storage, &stale, PARTITION_OWNER_KEY)
        .await
        .unwrap();
    force_expire_partition_owner_for_node(
        &storage,
        &stale.partition_family,
        &stale.partition_id,
        "node-a",
        250,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap()
    .unwrap();
    let fresh = ready_authz_permit(&storage, 42, "node-b").await;
    assert!(fresh.fence_token > stale.fence_token);

    let rejected = append_authz_tuple_record_inner(
        &storage,
        &record(1, "add"),
        stale.fence_token,
        Some(stale_precondition),
    )
    .await
    .unwrap_err();
    assert!(
        rejected.to_string().contains("target mismatch")
            || rejected.to_string().contains("generation mismatch"),
        "unexpected error: {rejected:?}"
    );
}

#[tokio::test]
async fn authz_journal_rejects_wrong_partition_scope_before_write() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let valid = ready_authz_permit(&storage, 42, "node-a").await;

    let wrong_family = PartitionWritePermit {
        partition_family: "object_metadata".to_string(),
        partition_id: valid.partition_id.clone(),
        owner_node_id: valid.owner_node_id.clone(),
        fence_token: valid.fence_token,
    };
    let rejected = append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &wrong_family,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("does not target this authorization tuple partition")
    );

    let wrong_tenant_partition = PartitionWritePermit {
        partition_family: valid.partition_family.clone(),
        partition_id: hex::encode(authz_partition_id(43)),
        owner_node_id: valid.owner_node_id,
        fence_token: valid.fence_token,
    };
    let rejected = append_authz_tuple_record_with_permit(
        &storage,
        &record(1, "add"),
        &wrong_tenant_partition,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap_err();
    assert!(
        rejected
            .to_string()
            .contains("does not target this authorization tuple partition")
    );
    assert!(
        read_authz_journal_payload_fences(&storage, 42)
            .await
            .unwrap()
            .is_empty(),
        "wrong-scope internal authz writes must fail before stream creation"
    );
}

#[tokio::test]
pub(crate) async fn authz_write_with_permit_allocates_revision_under_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let permit = ready_authz_permit(&storage, 42, "node-a").await;

    let written = write_authz_tuple_with_permit(
        &storage,
        AuthzTupleWrite {
            tenant_id: 42,
            namespace: "document",
            object_id: "beta",
            relation: "editor",
            subject_kind: "user",
            subject_id: "bob",
            caveat_hash: "",
            operation: "add",
            written_by: "tester",
            reason: "test",
        },
        &permit,
        PARTITION_OWNER_KEY,
    )
    .await
    .unwrap();
    assert_eq!(written.revision, 1);
    let fences = read_authz_journal_payload_fences(&storage, 42)
        .await
        .unwrap();
    assert_eq!(fences[0], permit.fence_token);
}
