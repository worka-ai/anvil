use std::fs;
use std::path::{Path, PathBuf};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf()
}

fn production_source(relative: &str) -> String {
    let path = workspace_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn assert_contains_all(label: &str, source: &str, terms: &[&str]) {
    let missing = terms
        .iter()
        .copied()
        .filter(|term| !source.contains(term))
        .collect::<Vec<_>>();
    assert!(
        missing.is_empty(),
        "{label} missing required terms: {missing:#?}"
    );
}

fn assert_contains_none(label: &str, source: &str, terms: &[&str]) {
    let present = terms
        .iter()
        .copied()
        .filter(|term| source.contains(term))
        .collect::<Vec<_>>();
    assert!(
        present.is_empty(),
        "{label} contains forbidden terms: {present:#?}"
    );
}

#[test]
fn rfc_0007_service_control_tokens_and_digests_are_protobuf_not_json() {
    let admin_cursor = production_source("anvil-core/src/services/admin_cursor.rs");
    let audit = production_source("anvil-core/src/services/audit.rs");
    let auth_helpers = production_source("anvil-core/src/services/auth/helpers.rs");
    let batch_helpers = production_source("anvil-core/src/services/object/batch_helpers.rs");
    let internal_proxy = production_source("anvil-core/src/services/internal_proxy.rs");
    let s3_proxy = production_source("anvil/src/s3_gateway/proxy.rs");

    assert_contains_all(
        "admin cursor deterministic protobuf token",
        &admin_cursor,
        &[
            "AdminListCursorTokenProto",
            "decode_deterministic_proto::<AdminListCursorTokenProto>",
            "encode_deterministic_proto(&admin_list_cursor_to_proto",
        ],
    );
    assert_contains_none(
        "admin cursor JSON control token",
        &admin_cursor,
        &[
            "serde_json::from_slice(&bytes)",
            "serde_json::to_vec(&token)",
            "Serialize, Deserialize",
        ],
    );

    assert_contains_all(
        "tenant audit deterministic protobuf cursor",
        &audit,
        &[
            "TenantAuditCursorProto",
            "decode_deterministic_proto::<TenantAuditCursorProto>",
            "encode_deterministic_proto(&tenant_audit_cursor_to_proto",
        ],
    );
    assert_contains_none(
        "tenant audit JSON cursor token",
        &audit,
        &[
            "serde_json::from_slice(&bytes)",
            "serde_json::to_vec(&token)",
        ],
    );

    assert_contains_all(
        "authz deterministic protobuf page token",
        &auth_helpers,
        &[
            "AuthzPageTokenProto",
            "decode_deterministic_proto::<AuthzPageTokenProto>",
            "encode_deterministic_proto(&authz_page_token_to_proto",
        ],
    );
    assert_contains_none(
        "authz JSON page token",
        &auth_helpers,
        &[
            "serde_json::from_slice(&bytes)",
            "serde_json::to_vec(&token)",
            "#[derive(Debug, Clone, Serialize, Deserialize)]",
        ],
    );

    assert_contains_all(
        "mutation batch deterministic protobuf digest input",
        &batch_helpers,
        &[
            "MutationBatchDigestInputProto",
            "encode_deterministic_proto(&input)",
        ],
    );
    assert_contains_none(
        "mutation batch JSON digest input",
        &batch_helpers,
        &["MutationBatchDigestInput<'a>", "serde_json::to_vec(&input)"],
    );

    assert_contains_all(
        "internal proxy deterministic protobuf authz context",
        &internal_proxy,
        &[
            "ProxyAuthzContextProto",
            "encode_proxy_authz_context",
            "decode_deterministic_proto::<ProxyAuthzContextProto>",
        ],
    );
    assert_contains_none(
        "internal proxy JSON authz context",
        &internal_proxy,
        &[
            "serde_json::from_slice::<auth::Claims>",
            "serde_json::to_vec(&claims(",
            "serde_json::to_vec(claims)",
        ],
    );
    assert_contains_none(
        "S3 proxy JSON authz context",
        &s3_proxy,
        &["serde_json::to_vec(claims)"],
    );
}

#[test]
fn rfc_0007_manifest_cas_uses_corestore_stream_payloads_and_current_rows() {
    let source = production_source("anvil-core/src/manifest_journal.rs");

    assert_contains_all(
        "manifest CAS RFC 0007 path",
        &source,
        &[
            "ManifestBodyProto",
            "ManifestCurrentRowProto",
            "TABLE_MANIFEST_CAS_CURRENT_ROW",
            "encode_manifest_body(&body, fence_token, mutation_id)",
            "decode_manifest_body(&record.payload)",
            "CoreMetaStore::open(storage.core_store_meta_path())",
            "CoreMutationPrecondition::CoreMetaRow",
            "write_manifest_current_row",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "manifest CAS legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_frames",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_index_diagnostics_use_direct_protobuf_stream_payloads() {
    let source = production_source("anvil-core/src/index_diagnostic_journal.rs");

    assert_contains_all(
        "index diagnostic RFC 0007 path",
        &source,
        &[
            "IndexDiagnosticBodyProto",
            "fence_token",
            "mutation_id",
            "encode_index_diagnostic_body(diagnostic, fence_token, mutation_id)",
            "decode_index_diagnostic_body(&record.payload)",
            "decode_index_diagnostic_body_fence(&record.payload)",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "index diagnostic legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_index_diagnostic_frames",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_index_definitions_use_stream_payloads_and_coremeta_current_rows() {
    let source = format!(
        "{}\n{}",
        production_source("anvil-core/src/index_journal.rs"),
        production_source("anvil-core/src/index_coremeta.rs")
    );

    assert_contains_all(
        "index definition RFC 0007 path",
        &source,
        &[
            "IndexEventBodyProto",
            "IndexDefinitionCurrentCoreMetaRecord",
            "IndexDefinitionStateCoreMetaRecord",
            "fence_token",
            "mutation_id",
            "INDEX_DEFINITION_RECORD_KIND",
            "encode_index_event_body(event, fence_token)",
            "decode_index_event_body(&record.payload)",
            "read_index_journal_bodies",
            "write_index_current_coremeta_rows",
            "write_index_definition_current_coremeta_record",
            "read_index_definition_current_coremeta_record",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "index definition legacy wrapper/current-row bypass path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_index_journal_frames",
            "serde_json::to_vec(&event)",
            "serde_json::from_slice(&frame.body)",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_hf_metadata_uses_direct_protobuf_stream_payloads() {
    let source = production_source("anvil-core/src/hf_journal.rs");

    assert_contains_all(
        "HF metadata RFC 0007 path",
        &source,
        &[
            "HfJournalBodyProto",
            "fence_token",
            "mutation_id",
            "encode_hf_body(&body, guard.fence_token, mutation_id)",
            "decode_hf_body(&record.payload)",
            "decode_hf_body_fence(&record.payload)",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "HF metadata legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_hf_journal_frames",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_model_metadata_uses_direct_protobuf_stream_payloads() {
    let source = production_source("anvil-core/src/model_journal.rs");

    assert_contains_all(
        "model metadata RFC 0007 path",
        &source,
        &[
            "ModelEventBodyProto",
            "ModelArtifactUpsertProto",
            "ModelTensorsReplaceProto",
            "fence_token",
            "mutation_id",
            "encode_model_event_body(&event, fence_token, mutation_id)",
            "decode_model_event_body(&record.payload)",
            "decode_model_event_body_fence(&record.payload)",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "model metadata legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_model_journal_frames",
            "serde_json::to_vec(&event)",
            "serde_json::from_slice(&frame.body)",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_bucket_metadata_uses_direct_protobuf_stream_payloads_and_current_refs() {
    let source = production_source("anvil-core/src/bucket_journal.rs");

    assert_contains_all(
        "bucket metadata RFC 0007 path",
        &source,
        &[
            "BucketJournalBodyProto",
            "fence_token",
            "mutation_id",
            "encode_bucket_journal_body",
            "decode_bucket_journal_body(&record.payload)",
            "read_current_bucket_for_tenant_row",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "bucket metadata legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_bucket_journal_frames",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_object_metadata_uses_direct_protobuf_stream_payloads_and_current_refs() {
    let source = format!(
        "{}\n{}",
        production_source("anvil-core/src/metadata_journal.rs"),
        production_source("anvil-core/src/metadata_journal/helpers.rs")
    );

    assert_contains_all(
        "object metadata RFC 0007 path",
        &source,
        &[
            "ObjectMetadataBodyProto",
            "ObjectLinkTargetProto",
            "ObjectMetadataRecord",
            "fence_token",
            "mutation_id",
            "encode_object_version_body(&object_body)",
            "encode_directory_entry_body(&directory_body)",
            "decode_object_metadata_body_proto(&record.payload)",
            "read_all_metadata_journal_records",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "object metadata legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_all_metadata_journal_frames",
            "read_metadata_journal_frames",
            "frame.encode()",
            "frame.body",
            "serde_json::to_vec(&ObjectVersionBody",
            "serde_json::to_vec(&DirectoryEntryBody",
            "serde_json::from_slice(&record.value)",
            "serde_json::from_slice(&frame.body)",
        ],
    );
}

#[test]
fn rfc_0007_task_queue_uses_direct_protobuf_stream_payloads_and_current_rows() {
    let source = production_source("anvil-core/src/task_journal.rs");

    assert_contains_all(
        "task queue RFC 0007 path",
        &source,
        &[
            "TaskJournalBodyProto",
            "TaskCurrentRowProto",
            "fence_token",
            "mutation_id",
            "TASK_QUEUE_AUDIT_RECORD_KIND",
            "encode_task_journal_body(&event, fence_token, mutation_id)",
            "decode_task_journal_body(&record.payload)",
            "decode_task_journal_body_fence(&record.payload)",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "task queue legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_task_journal_frames",
            "serde_json::to_vec(&event)",
            "serde_json::from_slice(&frame.body)",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_legacy_journal_frame_format_is_removed_from_core_sources() {
    let source = format!(
        "{}\n{}",
        production_source("anvil-core/src/formats.rs"),
        production_source("anvil-core/src/partition_fence.rs")
    );

    assert_contains_none(
        "legacy journal frame core support",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "MetadataJournal",
            "ANVJRN1",
        ],
    );
}

#[test]
fn rfc_0007_append_metadata_uses_direct_protobuf_stream_payloads() {
    let source = production_source("anvil-core/src/append_journal.rs");

    assert_contains_all(
        "append metadata RFC 0007 path",
        &source,
        &[
            "AppendBodyProto",
            "AppendStreamProto",
            "AppendStreamRecordProto",
            "CoreObjectRefProto",
            "fence_token",
            "mutation_id",
            "encode_append_body(&body, fence_token, mutation_id)",
            "decode_append_body(&record.payload)",
            "decode_append_body_fence(&record.payload)",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "append metadata legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_frames",
            "serde_json::to_vec(&AppendBody",
            "serde_json::from_slice(&frame.body)",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_control_plane_uses_direct_protobuf_stream_payloads_and_current_refs() {
    let source = production_source("anvil-core/src/control_journal.rs");

    assert_contains_all(
        "control plane RFC 0007 path",
        &source,
        &[
            "ControlEventProto",
            "ControlCurrentProto",
            "fence_token",
            "mutation_id",
            "encode_control_event_body(&event, fence_token, mutation_id)",
            "decode_control_event_body(&record.payload)",
            "decode_control_event_body_fence(&record.payload)",
            "read_control_journal_bodies",
            "CoreMutationOperation::StreamAppend",
        ],
    );
    assert_contains_none(
        "control plane legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_control_journal_frames",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_multipart_metadata_uses_direct_protobuf_stream_payloads_and_current_rows() {
    let source = production_source("anvil-core/src/multipart_journal.rs");

    assert_contains_all(
        "multipart metadata RFC 0007 path",
        &source,
        &[
            "MultipartEventProto",
            "MultipartUploadCurrentRowProto",
            "MultipartPartCurrentRowProto",
            "TABLE_MULTIPART_UPLOAD_CURRENT_ROW",
            "TABLE_MULTIPART_PART_CURRENT_ROW",
            "fence_token",
            "mutation_id",
            "encode_multipart_event(",
            "decode_multipart_event(&record.payload)",
            "decode_multipart_event_fence(&record.payload)",
            "read_events_from_store",
            "read_state_from_current_rows",
            "multipart_current_row_operations",
            "CoreMutationPrecondition::CoreMetaRow",
            "CoreTransactionUpdate::StreamAppend",
            "CoreMutationOperation::StreamAppend",
            "CoreMutationOperation::CoreMetaPut",
        ],
    );
    assert_contains_none(
        "multipart metadata legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_frames_from_store",
            "frame.encode()",
            "frame.body",
            "read_state_from_current_refs",
        ],
    );
}

#[test]
fn rfc_0007_authz_tuples_use_direct_protobuf_stream_payloads_and_coremeta_rows() {
    let source = production_source("anvil-core/src/authz_journal.rs");

    assert_contains_all(
        "authz tuple RFC 0007 path",
        &source,
        &[
            "AuthzTupleJournalBodyProto",
            "AuthzTupleBatchJournalBodyProto",
            "AuthzTupleCurrentRowProto",
            "fence_token",
            "mutation_id",
            "encode_authz_tuple_journal_body(record, fence_token)",
            "decode_authz_tuple_journal_body(&stream_record.payload)",
            "decode_authz_tuple_journal_body_fence(&record.payload)",
            "CoreMutationOperation::StreamAppend",
            "CoreMetaStore::open(storage.core_store_meta_path())",
        ],
    );
    assert_contains_none(
        "authz tuple legacy wrapper path",
        &source,
        &[
            "JournalFrame",
            "JournalRecordKind",
            "validate_journal_chain",
            "read_authz_journal_frames",
            "frame.encode()",
            "frame.body",
        ],
    );
}

#[test]
fn rfc_0007_pending_mutation_admission_targets_are_typed_protobuf_not_json() {
    let pending = production_source("anvil-core/src/core_store/pending_mutation.rs");
    let admission = production_source("anvil-core/src/core_store/local_admission.rs");
    let transactions = production_source("anvil-core/src/core_store/local_transactions.rs");
    let stream = production_source("anvil-core/src/core_store/local_stream_control.rs");
    let blob = production_source("anvil-core/src/core_store/local_init_blob.rs");

    assert_contains_all(
        "pending mutation typed target protobuf",
        &pending,
        &[
            "pub(super) enum CorePendingMutationTarget",
            "struct CorePendingMutationTargetProto",
            "CoreObjectPutTargetProto",
            "CoreStreamAppendTargetProto",
            "CoreMutationBatchTargetProto",
            "CorePendingMutationFinalisationResultProto",
            "precondition_fingerprints",
        ],
    );
    assert_contains_none(
        "pending mutation JSON control records",
        &pending,
        &[
            "CoreJsonValueProto",
            "json_to_proto",
            "json_from_proto",
            "target: serde_json::Value",
            "preconditions: serde_json::Value",
            "result: Option<serde_json::Value>",
        ],
    );
    for (label, source) in [
        ("admission", admission.as_str()),
        ("transactions", transactions.as_str()),
        ("stream", stream.as_str()),
        ("blob", blob.as_str()),
    ] {
        assert_contains_none(
            label,
            source,
            &[
                "target: serde_json::Value",
                "serde_json::json!({",
                "json_required_string(&record.target",
                "json_optional_string(&record.target",
                "json_required_u64(&record.target",
                "json_optional_u64(&record.target",
                "json_required_bool(&record.target",
            ],
        );
    }
}

#[test]
fn rfc_0007_genesis_and_root_records_are_typed_protobuf_not_json() {
    let local_roots = production_source("anvil-core/src/core_store/local_roots.rs");
    let root_proto = production_source("anvil-core/src/core_store/root_proto.rs");
    let local = production_source("anvil-core/src/core_store/local.rs");

    assert_contains_all(
        "genesis typed protobuf",
        &local_roots,
        &[
            "CoreGenesisMeshControlSegmentProto",
            "CoreGenesisAuthzReservedSchemaSegmentProto",
            "CoreGenesisConfigProto",
            "encode_genesis_mesh_control_segment",
            "encode_genesis_authz_reserved_schema_segment",
        ],
    );
    assert_contains_all(
        "genesis typed partition model",
        &local,
        &["pub(super) struct CoreGenesisPartition"],
    );
    for (label, source) in [
        ("local roots", local_roots.as_str()),
        ("root proto", root_proto.as_str()),
    ] {
        assert_contains_none(
            label,
            source,
            &[
                "canonical_json_bytes(&serde_json::json!",
                "serde_json::json!({",
                "initial_partition_map: Vec<serde_json::Value>",
                "fn json_required_string",
                "fn json_required_u64",
            ],
        );
    }
}

#[test]
fn rfc_0007_system_realm_bootstrap_marker_is_protobuf_logical_file() {
    let source = production_source("anvil-core/src/system_realm.rs");
    assert_contains_all(
        "system realm bootstrap marker protobuf",
        &source,
        &[
            "struct BootstrapMarkerProto",
            "fn encode_bootstrap_marker",
            "encode_deterministic_proto(&BootstrapMarkerProto::from(marker))",
            "marker_bytes: encode_bootstrap_marker(marker)",
        ],
    );
    assert_contains_none(
        "system realm bootstrap marker legacy JSON",
        &source,
        &[
            "source: serde_json::to_vec_pretty(&marker)?",
            "BootstrapMarker: Deserialize",
        ],
    );
}

#[test]
fn rfc_0007_tenant_audit_payloads_are_deterministic_protobuf() {
    let source = production_source("anvil-core/src/tenant_audit.rs");

    assert_contains_all(
        "tenant audit deterministic protobuf payloads",
        &source,
        &[
            "struct TenantAuditEventProto",
            "fn encode_tenant_audit_event",
            "fn decode_tenant_audit_event",
            "payload: encode_tenant_audit_event(event)",
            "decode_tenant_audit_event(&record.payload)",
            "encode_deterministic_proto(&TenantAuditEventProto",
            "decode_deterministic_proto::<TenantAuditEventProto>",
        ],
    );
    assert_contains_none(
        "tenant audit JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(event)",
            "serde_json::to_vec(&event)",
            "serde_json::from_slice(&record.payload)",
        ],
    );
}

#[test]
fn rfc_0007_personaldb_watch_payloads_are_deterministic_protobuf() {
    let source = production_source("anvil-core/src/personaldb_watch.rs");

    assert_contains_all(
        "personaldb watch deterministic protobuf payloads",
        &source,
        &[
            "struct PersonalDbGroupWatchPayloadProto",
            "struct PersonalDbProjectionWatchPayloadProto",
            "fn encode_group_watch_payload",
            "fn decode_group_watch_payload",
            "fn encode_projection_watch_payload",
            "fn decode_projection_watch_payload",
            "encode_group_watch_payload(&payload)",
            "decode_group_watch_payload(&record.payload)",
            "encode_projection_watch_payload(&payload)",
            "decode_projection_watch_payload(&record.payload)",
        ],
    );
    assert_contains_none(
        "personaldb watch JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(&payload)",
            "serde_json::from_slice(&record.payload)",
            "serde_json::to_vec(&PersonalDbGroupWatchPayload",
            "serde_json::to_vec(&PersonalDbProjectionWatchPayload",
        ],
    );
}

#[test]
fn rfc_0007_index_partition_watch_payloads_are_deterministic_protobuf() {
    let source = production_source("anvil-core/src/index_partition_watch.rs");

    assert_contains_all(
        "index partition watch deterministic protobuf payloads",
        &source,
        &[
            "struct IndexPartitionWatchPayloadProto",
            "fn encode_index_partition_watch_payload",
            "fn decode_index_partition_watch_payload",
            "encode_index_partition_watch_payload(&payload)",
            "decode_index_partition_watch_payload(&record.payload)",
            "source_cursor: payload.source_cursor.to_string()",
            "decode_deterministic_proto::<IndexPartitionWatchPayloadProto>",
        ],
    );
    assert_contains_none(
        "index partition watch JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(&payload)",
            "serde_json::from_slice(&record.payload)",
        ],
    );
}

#[test]
fn rfc_0007_watch_checkpoints_are_deterministic_protobuf_logical_files() {
    let source = production_source("anvil-core/src/watch_checkpoint.rs");

    assert_contains_all(
        "watch checkpoint deterministic protobuf logical files",
        &source,
        &[
            "struct WatchCheckpointProto",
            "fn encode_watch_checkpoint",
            "fn decode_watch_checkpoint",
            "checkpoint_bytes: encode_watch_checkpoint(checkpoint)",
            "decode_watch_checkpoint(&row.checkpoint_bytes)",
            "hash32(&encode_watch_checkpoint(&unsigned))",
            "decode_deterministic_proto::<WatchCheckpointProto>",
        ],
    );
    assert_contains_none(
        "watch checkpoint JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(&unsigned)",
            "serde_json::to_vec(checkpoint)",
            "serde_json::to_vec(&checkpoint)",
            "serde_json::from_slice(&bytes)",
            "let checkpoint: WatchCheckpoint = serde_json::from_slice",
        ],
    );
}

#[test]
fn rfc_0007_index_segments_publish_coremeta_rows() {
    let coremeta = production_source("anvil-core/src/index_coremeta.rs");
    let typed = production_source("anvil-core/src/typed_field_segment.rs");
    let full_text = production_source("anvil-core/src/full_text_segment.rs");
    let vector = production_source("anvil-core/src/vector_segment.rs");

    assert_contains_all(
        "index segment CoreMeta row schema",
        &coremeta,
        &[
            "IndexSegmentCoreMetaRecordProto",
            "CF_INDEX_ROWS",
            "TABLE_INDEX_ROW",
            "write_index_segment_coremeta_record",
            "latest_index_segment_coremeta_record",
            "segment_authz_scope_hash",
            "decode_deterministic_proto::<IndexSegmentCoreMetaRecordProto>",
        ],
    );
    assert_contains_all(
        "typed/path/metadata segment CoreMeta publication",
        &typed,
        &[
            "write_index_segment_coremeta_record",
            "IndexSegmentCoreMetaRecord",
            "typed_segment_index_kind",
            "latest_index_segment_coremeta_record",
        ],
    );
    assert_contains_all(
        "full-text segment CoreMeta publication",
        &full_text,
        &[
            "write_index_segment_coremeta_record",
            "IndexSegmentCoreMetaRecord",
            "list_index_segment_coremeta_records",
            "latest_index_segment_coremeta_record",
        ],
    );
    assert_contains_all(
        "vector segment CoreMeta publication",
        &vector,
        &[
            "write_index_segment_coremeta_record",
            "IndexSegmentCoreMetaRecord",
            "list_index_segment_coremeta_records",
            "latest_index_segment_coremeta_record",
        ],
    );
    assert_contains_none(
        "index segment latest lookup bypasses CoreMeta",
        &format!("{typed}\n{full_text}\n{vector}"),
        &[],
    );
}

#[test]
fn rfc_0007_query_tokens_and_plans_bind_authz_scope() {
    let query = format!(
        "{}\n{}",
        production_source("anvil-core/src/services/index/query.rs"),
        production_source("anvil-core/src/services/index/query_page_token.rs")
    );
    let operations = production_source("anvil-core/src/services/index/operations.rs");

    assert_contains_all(
        "index query authz scope token binding",
        &query,
        &[
            "struct QueryAuthzScope",
            "scope_hash",
            "authz_scope_hash",
            "object_namespace",
            "relation",
            "stable_prefixed_json_hash",
            "IndexPageTokenBinding",
        ],
    );
    assert_contains_all(
        "index query paths carry authz scope",
        &operations,
        &[
            "QueryAuthzScope::for_bucket",
            "\"authz_scope\": authz_scope.trace_json()",
            "&authz_scope",
            "query_permission_filter",
            "query_hit_visible",
        ],
    );
}

#[test]
fn rfc_0007_watch_checkpoint_lag_is_coremeta_protobuf() {
    let source = production_source("anvil-core/src/watch_checkpoint.rs");

    assert_contains_all(
        "watch checkpoint lag CoreMeta row",
        &source,
        &[
            "struct WatchCheckpointLagRecordProto",
            "CF_MATERIALISATION",
            "TABLE_MATERIALISATION_CURSOR_ROW",
            "write_watch_checkpoint_lag_record",
            "read_watch_checkpoint_lag_record",
            "encode_watch_checkpoint_lag_record",
            "decode_watch_checkpoint_lag_record",
            "source_cursor_high",
            "lag_record_count_hint",
        ],
    );
    assert_contains_none(
        "watch checkpoint lag JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(&record)",
            "serde_json::from_slice(&payload)",
        ],
    );
}

#[test]
fn rfc_0007_authz_namespace_watch_payloads_are_deterministic_protobuf() {
    let source = production_source("anvil-core/src/authz_namespace_watch.rs");

    assert_contains_all(
        "authz namespace watch deterministic protobuf payloads",
        &source,
        &[
            "struct AuthzNamespaceWatchPayloadProto",
            "fn encode_authz_namespace_watch_payload",
            "fn decode_authz_namespace_watch_payload",
            "encode_authz_namespace_watch_payload(&payload)",
            "decode_authz_namespace_watch_payload(&record.payload)",
            "AuthzNamespaceWatchPayloadProto {",
            "decode_deterministic_proto::<AuthzNamespaceWatchPayloadProto>",
        ],
    );
    assert_contains_none(
        "authz namespace watch JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(&payload)",
            "serde_json::from_slice(&record.payload)",
        ],
    );
}

#[test]
fn rfc_0007_git_source_watch_payloads_are_deterministic_protobuf() {
    let source = production_source("anvil-core/src/git_source_watch.rs");

    assert_contains_all(
        "git source watch deterministic protobuf payloads",
        &source,
        &[
            "struct GitSourceWatchPayloadProto",
            "fn encode_git_source_watch_payload",
            "fn decode_git_source_watch_payload",
            "encode_git_source_watch_payload(&payload)",
            "decode_git_source_watch_payload(&record.payload)",
            "encode_deterministic_proto(&GitSourceWatchPayloadProto",
            "decode_deterministic_proto::<GitSourceWatchPayloadProto>",
        ],
    );
    assert_contains_none(
        "git source watch JSON durable payloads",
        &source,
        &[
            "serde_json::to_vec(&payload)",
            "serde_json::from_slice(&record.payload)",
        ],
    );
}

#[test]
fn rfc_0007_cluster_gossip_uses_deterministic_protobuf_control_messages() {
    let source = production_source("anvil-core/src/cluster.rs");

    assert_contains_all(
        "cluster gossip deterministic protobuf control messages",
        &source,
        &[
            "struct ClusterMessageProto",
            "struct MetadataEventProto",
            "fn encode_cluster_message",
            "fn decode_cluster_message",
            "fn encode_metadata_event",
            "fn decode_metadata_event",
            "encode_deterministic_proto(&ClusterMessageProto",
            "decode_deterministic_proto::<ClusterMessageProto>",
            "encode_deterministic_proto(&MetadataEventProto",
            "decode_deterministic_proto::<MetadataEventProto>",
        ],
    );
    assert_contains_none(
        "cluster gossip JSON control message transport",
        &source,
        &[
            "serde_json::to_vec(&message)",
            "serde_json::to_vec(&event)",
            "serde_json::from_slice::<ClusterMessage>",
            "serde_json::from_slice::<MetadataEvent>",
        ],
    );
}

#[test]
fn rfc_0007_mesh_control_payloads_are_protobuf_with_operator_json_only_at_boundary() {
    let control = production_source("anvil-core/src/mesh_control_stream.rs");
    let directory = production_source("anvil-core/src/mesh_directory/record_proto.rs");
    let lifecycle = production_source("anvil-core/src/mesh_lifecycle/record_proto.rs");

    assert_contains_all(
        "mesh control stream protobuf frame and checkpoint records",
        &control,
        &[
            "struct ControlFrameHeaderProto",
            "struct ControlCheckpointProto",
            "payload_proto",
            "decode_control_mutation_header",
            "encode_control_checkpoint_proto",
            "decode_control_checkpoint_proto",
            "ControlRecordDigest::blake3(&record.frame.payload_proto)",
        ],
    );
    assert_contains_none(
        "mesh control projection digest durable JSON canonicalisation",
        &control,
        &["serde_json::to_vec(&value)?"],
    );
    assert_contains_all(
        "mesh routing descriptor protobuf records",
        &directory,
        &[
            "TenantNameDescriptorProto",
            "TenantLocatorDescriptorProto",
            "BucketLocatorDescriptorProto",
            "HostAliasDescriptorProto",
            "DESCRIPTOR_FILE_EXTENSION: &str = \".pb\"",
            "encode_routing_payload_proto",
            "decode_routing_payload_proto",
        ],
    );
    assert_contains_all(
        "mesh lifecycle descriptor protobuf records",
        &lifecycle,
        &[
            "MeshLifecycleStateProto",
            "RegionDescriptorProto",
            "CellDescriptorProto",
            "NodeDescriptorProto",
            "encode_lifecycle_control_payload",
            "decode_lifecycle_control_payload",
        ],
    );
}

#[test]
fn rfc_0007_admission_commit_certificates_preserve_coremeta_evidence() {
    let source = production_source("anvil-core/src/core_store/pending_mutation.rs");

    assert_contains_all(
        "admission commit certificate CoreMeta evidence support",
        &source,
        &[
            "CoreAdmissionCommitCertificate",
            "metadata_replica_node_ids: Vec<String>",
            "admission_attempt_id_with_metadata_replicas",
            "core_meta_commit_certificate_hash",
            "certificate_persist_receipt_hashes",
            "CORE_META_ADMISSION_PROFILE",
            "#[prost(string, repeated, tag = \"10\")]",
            "#[prost(string, repeated, tag = \"9\")]",
        ],
    );
    let admission = production_source("anvil-core/src/core_store/local_admission.rs");
    assert_contains_all(
        "CoreMeta evidence rows are persisted before and after certificate construction",
        &admission,
        &[
            "commit_coremeta_batch_by_embedded_roots",
            "metadata_commit.metadata_replica_node_ids",
            "local_pending_mutation_commit_certificate_bytes",
            "verify_local_pending_mutation_commit_certificate",
        ],
    );
    assert_contains_none(
        "CoreMeta evidence unsupported bypass",
        &(source + &admission),
        &["do not support CoreMeta evidence yet"],
    );
}
