use super::*;
use chrono::Utc;
use tempfile::tempdir;

fn record(revision: i64, operation: &str) -> AuthzTupleRecord {
    AuthzTupleRecord {
        revision,
        revision_ordinal: 0,
        tenant_id: 7,
        namespace: "document".to_string(),
        object_id: "alpha".to_string(),
        relation: "viewer".to_string(),
        subject_kind: "user".to_string(),
        subject_id: "alice".to_string(),
        caveat_hash: String::new(),
        operation: operation.to_string(),
        written_by: "node".to_string(),
        reason: "test".to_string(),
        mutation_id: uuid::Uuid::new_v4(),
        record_hash: hex::encode(hash32(format!("record-{revision}").as_bytes())),
        written_at: Utc::now(),
    }
}

fn tuple_record(revision: i64, object_id: &str, subject_id: &str) -> AuthzTupleRecord {
    AuthzTupleRecord {
        object_id: object_id.to_string(),
        subject_id: subject_id.to_string(),
        ..record(revision, "add")
    }
}

fn test_hash(ch: char) -> String {
    format!("blake3:{}", ch.to_string().repeat(64))
}

#[tokio::test]
async fn authz_tuple_segment_uses_exact_binary_records() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let records = vec![record(2, "remove"), record(1, "add")];
    let segment_ref = write_authz_tuple_segment(&storage, 7, &records)
        .await
        .unwrap();
    assert_eq!(
        segment_ref,
        "authz_tuple_segment:tenant:7:generation:00000000000000000002"
    );

    let decoded = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decoded.header.partition_family, "authz_tuple");
    assert_eq!(decoded.records.len(), 2);
    assert_eq!(decoded.records[0].revision, 1);
    assert_eq!(decoded.records[1].operation, "remove");

    let latest = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.records.len(), 2);
}

#[tokio::test]
async fn authz_tuple_segment_candidate_reader_returns_revision_scoped_doc_ids() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let records = vec![
        tuple_record(1, "alpha", "alice"),
        tuple_record(2, "beta", "bob"),
        tuple_record(3, "gamma", "alice"),
    ];
    write_authz_tuple_segment(&storage, 7, &records)
        .await
        .unwrap();

    let scope = CandidateSetScope {
        root_key_hash: test_hash('0'),
        root_generation: 9,
        index_id: "index:documents".to_string(),
        index_generation: 4,
        authz_realm_id: "tenant:7".to_string(),
        authz_scope_hash: test_hash('1'),
        authz_object_namespace: "document".to_string(),
        authz_relation: "viewer".to_string(),
        authz_principal_hash: test_hash('2'),
        authz_revision: 3,
        boundary_schema_generation_hash: test_hash('3'),
        predicate_hash: test_hash('4'),
        order_hash: test_hash('5'),
    };
    let reader = AuthzSegmentCandidateReader::new(storage.clone(), 7);
    let partition_id = 44;
    let request = AuthzCandidateRequest {
        authz_scope: "tenant:7".to_string(),
        candidate_scope: scope,
        partition_id,
        subject: "user:alice".to_string(),
        relation: "viewer".to_string(),
        object_namespace: "document".to_string(),
        revision: 3,
        system_revision: 0,
        root_generation: 9,
    };

    let candidates = reader.candidate_set(request.clone()).await.unwrap();
    assert!(
        candidates.contains_doc_id(
            ObjectAuthzKey::realm_object("document", "alpha").doc_id(partition_id)
        )
    );
    assert!(
        candidates.contains_doc_id(
            ObjectAuthzKey::realm_object("document", "gamma").doc_id(partition_id)
        )
    );
    assert!(
        !candidates
            .contains_doc_id(ObjectAuthzKey::realm_object("document", "beta").doc_id(partition_id))
    );

    let decisions = reader
        .verify_page(
            request.clone(),
            vec![
                ObjectAuthzKey::realm_object("document", "alpha"),
                ObjectAuthzKey::realm_object("document", "beta"),
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        decisions
            .iter()
            .map(|decision| decision.allowed)
            .collect::<Vec<_>>(),
        vec![true, false]
    );

    let mut stale = request;
    stale.revision = 4;
    assert!(reader.candidate_set(stale).await.is_err());
}

#[tokio::test]
async fn authz_candidate_reader_lazy_catches_up_deferred_tuple_segments() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    for record in [
        tuple_record(1, "alpha", "alice"),
        tuple_record(2, "beta", "bob"),
        tuple_record(3, "gamma", "alice"),
    ] {
        crate::authz_journal::append_authz_tuple_record(&storage, &record)
            .await
            .unwrap();
    }

    assert!(
        read_latest_authz_tuple_segment(&storage, 7)
            .await
            .unwrap()
            .is_none(),
        "tuple writes must not synchronously materialize authz writer segments"
    );
    assert!(
        crate::authz_userset_index::read_derived_userset_index(
            &storage,
            7,
            crate::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID,
        )
        .await
        .unwrap()
        .is_none(),
        "tuple writes must coalesce userset materialization instead of rewriting a snapshot"
    );

    let scope = CandidateSetScope {
        root_key_hash: test_hash('0'),
        root_generation: 9,
        index_id: "index:documents".to_string(),
        index_generation: 4,
        authz_realm_id: "tenant:7".to_string(),
        authz_scope_hash: test_hash('1'),
        authz_object_namespace: "document".to_string(),
        authz_relation: "viewer".to_string(),
        authz_principal_hash: test_hash('2'),
        authz_revision: 3,
        boundary_schema_generation_hash: test_hash('3'),
        predicate_hash: test_hash('4'),
        order_hash: test_hash('5'),
    };
    let request = AuthzCandidateRequest {
        authz_scope: "tenant:7".to_string(),
        candidate_scope: scope,
        partition_id: 44,
        subject: "user:alice".to_string(),
        relation: "viewer".to_string(),
        object_namespace: "document".to_string(),
        revision: 3,
        system_revision: 0,
        root_generation: 9,
    };
    let reader = AuthzSegmentCandidateReader::new(storage.clone(), 7);

    let candidates = reader.candidate_set(request.clone()).await.unwrap();
    assert!(
        candidates.contains_doc_id(ObjectAuthzKey::realm_object("document", "alpha").doc_id(44))
    );
    assert!(
        candidates.contains_doc_id(ObjectAuthzKey::realm_object("document", "gamma").doc_id(44))
    );
    assert!(
        !candidates.contains_doc_id(ObjectAuthzKey::realm_object("document", "beta").doc_id(44))
    );

    let segment = read_latest_authz_tuple_segment(&storage, 7)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(segment.header.generation, 3);

    let mut historical_request = request.clone();
    historical_request.revision = 2;
    historical_request.candidate_scope.authz_revision = 2;
    let historical = reader.candidate_set(historical_request).await.unwrap();
    assert!(
        historical.contains_doc_id(
            ObjectAuthzKey::realm_object("document", "alpha").doc_id(44)
        )
    );
    assert!(
        !historical.contains_doc_id(
            ObjectAuthzKey::realm_object("document", "gamma").doc_id(44)
        )
    );

    let mut stale_request = request;
    stale_request.revision = 4;
    assert!(reader.candidate_set(stale_request).await.is_err());
}
