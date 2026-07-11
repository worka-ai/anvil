use std::fs;
use std::path::Path;

fn repo_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

#[test]
fn query_planning_defines_shared_candidate_set_contract() {
    let source = repo_file("anvil-core/src/query_planner.rs");
    for expected in [
        "pub struct CoreDocId",
        "pub struct CandidateSetScope",
        "boundary_schema_generation_hash",
        "pub enum CandidateSetKind",
        "AllWithinPartition",
        "Bitmap",
        "SortedDocIdRanges",
        "OrderedTuples",
        "authz_scope_hash",
        "authz_object_namespace",
        "authz_relation",
        "authz_principal_hash",
        "pub trait AuthzCandidateReader",
        "pub trait IndexCandidateReader",
        "pub struct QueryPlanMetrics",
        "full_scan_forbidden_count",
    ] {
        assert!(source.contains(expected), "missing {expected}");
    }
}

#[test]
fn page_tokens_bind_boundary_schema_generation() {
    let source = format!(
        "{}\n{}\n{}",
        repo_file("anvil-core/src/services/index/query.rs"),
        repo_file("anvil-core/src/services/index/query_page_token.rs"),
        repo_file("anvil-core/src/services/index/operations.rs")
    );
    assert!(source.contains("boundary_schema_generation_hash"));
    assert!(source.contains("root_generation"));
    assert!(source.contains("root_key_hash"));
    assert!(source.contains("index_generation"));
    assert!(source.contains("authz_realm_id"));
    assert!(source.contains("authz_revision"));
    assert!(source.contains("predicate_hash"));
    assert!(source.contains("order_hash"));
    assert!(source.contains("anvil.query.root_key.v1"));
    assert!(source.contains("anvil.query.page_token_scope.v1"));
    assert!(source.contains("boundary_predicates_json"));
    assert!(source.contains("boundary_predicates_hash"));
    assert!(source.contains("PageTokenScopeMismatch"));
    assert!(source.contains("token.validate(&binding)"));
    assert!(source.contains("format!(\"blake3:{}\", blake3::hash(value.as_bytes()).to_hex())"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.root_key_hash"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.predicate_hash"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.order_hash"));
    assert!(source.contains("ensure_algorithm_prefixed_hash(&self.authz_scope_hash"));
    assert!(
        source
            .contains("update_mac_part(&mut mac, self.boundary_schema_generation_hash.as_bytes())")
    );

    let operations = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/operations.rs"),
        repo_file("anvil-core/src/services/index/query_candidates.rs")
    );
    assert!(operations.contains("index_page_token_boundary_hash"));
    assert!(operations.contains("read_boundary_schema_generation_hash"));
    assert!(operations.contains("anvil.query.boundary_schema_generation_hash.v1"));
}

#[test]
fn query_planner_forbids_unbounded_index_candidates_before_range_plans() {
    let planner = repo_file("anvil-core/src/query_planner.rs");
    assert!(planner.contains("CandidateSetKind::AllWithinPartition"));
    assert!(planner.contains("bail!(\"IndexCapabilityMissing\")"));
    assert!(planner.contains("AuthzCandidateSetStale"));
    assert!(planner.contains("payload_ranges_planned"));
    assert!(planner.contains("payload_bytes_planned"));
}

#[test]
fn query_services_reject_late_or_legacy_authz_filters() {
    let query = repo_file("anvil-core/src/services/index/query.rs");
    let operations = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/operations.rs"),
        repo_file("anvil-core/src/services/index/query_candidates.rs")
    );

    assert!(query.contains("QuerySpec degraded full-scan fallback is forbidden"));
    assert!(query.contains("typed_json_index_has_field"));
    assert!(query.contains("requires_object_authorization"));
    assert!(query.contains("effective_authorization_mode"));
    assert!(query.contains("authz_label_filter_for_index_candidate_set"));
    assert!(!query.contains("object_read_scope_patterns_for_bucket"));
    assert!(!query.contains("object_key_prefixes"));
    assert!(operations.contains("AuthzRevisionLagging"));
    assert!(!query.contains("collect_object_scope"));

    assert!(operations.contains("Status::failed_precondition(\"IndexCapabilityMissing\")"));
    assert!(operations.contains("query_hit_visible("));
    assert!(operations.contains("system_realm_relationship_allows("));
    assert!(!operations.contains("list_objects(bucket.id, \"\", \"\", i32::MAX"));
    assert!(operations.contains("selected.requires_object_authorization()"));
    assert!(operations.contains("selected.effective_authorization_mode()"));
    assert!(operations.contains("authz_label_filter_for_index_candidate_set("));
    assert!(!operations.contains("map(|filter| &filter.authorized_labels)"));
    assert!(!operations.contains("AuthzPermissionSetTooLargeForPrefixScope"));
}

#[test]
fn boundary_predicates_use_typed_candidates_not_score_index_scans() {
    let query = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/query.rs"),
        repo_file("anvil-core/src/services/index/query_boundary.rs")
    );
    for expected in [
        "pub(super) struct BoundaryPredicate",
        "fn parse_boundary_predicate_item",
        "fn boundary_predicates_from_range_node",
        "matches_row",
        "matches_metadata",
        "ensure_no_direct_boundary_predicates",
        "Status::failed_precondition(\"IndexCapabilityMissing\")",
    ] {
        assert!(query.contains(expected), "missing {expected}");
    }

    let operations = format!(
        "{}\n{}",
        repo_file("anvil-core/src/services/index/operations.rs"),
        repo_file("anvil-core/src/services/index/query_candidates.rs")
    );
    assert!(operations.contains("BoundaryPredicate::parse_list(&req.boundary_predicates_json)"));
    assert!(operations.contains("typed_json_predicate_hash(&req, &authz_scope)"));
    assert!(operations.contains("predicate.matches_row(&typed)"));
    assert!(operations.contains("predicate.matches_metadata(metadata)"));
    assert!(operations.contains("ensure_no_direct_boundary_predicates(&req)?;"));
}

#[test]
fn query_scope_hashes_are_authz_aware_and_fail_closed() {
    let query = repo_file("anvil-core/src/services/index/query.rs");
    let token = repo_file("anvil-core/src/services/index/query_page_token.rs");
    let candidates = repo_file("anvil-core/src/services/index/query_candidates.rs");
    let planner = repo_file("anvil-core/src/query_planner.rs");

    for expected in [
        "authz_aware_query_scope_hash",
        "\"schema\": \"anvil.query.scope_hash.v1\"",
        "\"principal_hash\": authz_scope.principal_hash",
        "\"scope_hash\": authz_scope.scope_hash",
        "stable_json_hash_checked(",
        "Status::invalid_argument(format!(\"Invalid {field_name}: {e}\"))",
    ] {
        assert!(query.contains(expected), "missing {expected}");
    }

    for expected in [
        "validate_scope_hashes",
        "ensure_algorithm_prefixed_hash(&self.query_hash",
        "&self.boundary_schema_generation_hash",
        "\"schema\": \"anvil.query.page_token_scope.v1\"",
        "\"authz_scope_hash\": authz_scope.scope_hash",
        "\"caller_principal_hash\": caller_principal_hash",
    ] {
        assert!(token.contains(expected), "missing {expected}");
    }

    for expected in [
        "root_key_hash: authz_aware_query_scope_hash(",
        "authz_scope_hash: authz_scope.scope_hash.clone()",
        "authz_object_namespace: authz_scope.object_namespace.clone()",
        "authz_relation: authz_scope.relation.clone()",
        "authz_principal_hash: authz_scope.principal_hash.clone()",
    ] {
        assert!(candidates.contains(expected), "missing {expected}");
    }

    for expected in [
        "pub fn validate(&self) -> Result<()>",
        "ensure_algorithm_prefixed_hash(&self.root_key_hash",
        "ensure_algorithm_prefixed_hash(&self.authz_scope_hash",
        "ensure_algorithm_prefixed_hash(&self.authz_principal_hash",
        "self.scope.validate()?",
        "other.scope.validate()?",
    ] {
        assert!(planner.contains(expected), "missing {expected}");
    }
}

#[test]
fn typed_json_queries_use_materialised_value_index_candidates() {
    let segment = repo_file("anvil-core/src/typed_field_segment.rs");
    for expected in [
        "TABLE_TYPED_FIELD_VALUE_INDEX",
        "TABLE_TYPED_ROW_BY_ORDINAL",
        "pub struct TypedFieldValueIndexEntry",
        "pub value_index: Vec<TypedFieldValueIndexEntry>",
        "fn typed_value_index_key",
        "fn decode_typed_field_value_index",
        "pub fn encode_json_value_for_typed_index",
        "pub struct TypedFieldValueIndexLookup",
        "read_typed_field_value_index_entries",
        "typed_value_index_key_prefix",
        "read_typed_field_rows_by_ordinals",
        "read_typed_field_segment_rows_by_ordinals",
        "typed_row_ordinal_key",
        "typed_rows_by_ordinal_rows",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }

    let candidates = repo_file("anvil-core/src/services/index/query_candidates.rs");
    for expected in [
        "typed_json_value_index_ordinals",
        "typed_json_predicate_ordinals",
        "typed_json_value_index_lookups_for_predicate",
        "typed_json_predicate_ordinals_from_entries",
        "encoded_string_prefix",
        "\"prefix\"",
        "plan_loaded_typed_json_candidates",
        "plan_loaded_metadata_backed_candidates",
        "materialized_index_ordinals",
        "encoded_typed_predicate_value",
        "entry.row_ordinal",
    ] {
        assert!(candidates.contains(expected), "missing {expected}");
    }

    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    assert!(operations.contains("typed_json_candidate_ordinals_from_value_index"));
    assert!(operations.contains("metadata_candidate_ordinals_from_value_index"));
    assert!(operations.contains("read_typed_field_rows_by_ordinals"));
    assert!(
        operations.contains("return Err(Status::failed_precondition(\"IndexCapabilityMissing\"));")
    );
    assert!(!operations.contains("read_latest_typed_field_segment"));
}

#[test]
fn full_text_queries_have_term_addressed_postings_range_path() {
    let segment = repo_file("anvil-core/src/full_text_segment.rs");
    for expected in [
        "TABLE_FULL_TEXT_POSTINGS_BY_TERM",
        "fn postings_by_term_rows",
        "read_latest_full_text_segment_terms",
        "read_full_text_segment_terms",
        "RangeAddressedWriterSegment::open",
        "read_table_pages_matching_key_prefix(dictionary_entry",
        "read_table_pages_matching_key_prefix(postings_entry",
        "decode_postings(&posting_row.value)",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }

    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    assert!(operations.contains("read_latest_full_text_segment_terms"));
    assert!(operations.contains("tokenize_text(&req.query_text"));
}

#[test]
fn vector_queries_have_hnsw_range_reader_path() {
    let segment = repo_file("anvil-core/src/vector_segment.rs");
    for expected in [
        "TABLE_VECTOR_ENTRY_BY_ID",
        "TABLE_VECTOR_HNSW_BY_NODE",
        "fn vector_entry_rows",
        "fn hnsw_adjacency_rows",
        "fn hnsw_adjacency_key",
        "fn decode_hnsw_neighbors",
        "fn decode_hnsw_entrypoints",
        "fn encode_vector_entry_row",
        "fn decode_vector_entry_row",
        "read_vector_segment_header",
        "query_latest_vector_segment_ranges",
        "query_vector_segment_ranges",
        "RangeVectorEntryReader",
        "RangeVectorGraphReader",
        "query_hnsw_graph_with_range_reader",
        "read_table_pages_matching_key_prefix(self.entry_table",
        "graph_adjacency_by_layer",
    ] {
        assert!(segment.contains(expected), "missing {expected}");
    }

    let operations = repo_file("anvil-core/src/services/index/operations.rs");
    assert!(operations.contains("read_vector_segment_header"));
    assert!(operations.contains("query_vector_segment_ranges"));
    assert!(!operations.contains("search_query::query_vector_segment("));
}

#[test]
fn writer_segment_range_reader_exposes_table_page_ranges() {
    let range_reader = repo_file("anvil-core/src/writer_segment_range.rs");
    for expected in [
        "RangeAddressedWriterSegment",
        "read_body_table_directory",
        "read_table_pages_matching_key_prefix",
        "read_exact_body_directory_len",
        "read_exact_table_page_directory_len",
        "get_blob_range(GetBlobRange",
        "CoreByteRange",
        "WriterSegmentFixedHeader::decode",
    ] {
        assert!(range_reader.contains(expected), "missing {expected}");
    }

    let table = repo_file("anvil-core/src/formats/table.rs");
    for expected in [
        "pub struct WriterBodyTableDirectory",
        "pub struct WriterBodyTableDirectoryEntry",
        "pub struct TablePageRange",
        "decode_writer_body_table_directory",
        "decode_writer_body_table_page_ranges",
        "decode_table_page_rows",
    ] {
        assert!(table.contains(expected), "missing {expected}");
    }
}
