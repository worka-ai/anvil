use std::{fs, path::PathBuf};

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf()
}

fn workspace_file(path: &str) -> String {
    fs::read_to_string(workspace_root().join(path))
        .unwrap_or_else(|err| panic!("read {path}: {err}"))
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
fn internal_corestore_protocols_are_defined_in_public_proto_contract() {
    let proto = workspace_file("anvil-core/proto/anvil.proto");
    let client_proto = workspace_file("clients/rust/proto/anvil.proto");
    assert_eq!(proto, client_proto, "client proto must match core proto");

    assert_contains_all(
        "internal CoreStore services",
        &proto,
        &[
            "service BlockStoreInternal",
            "service RootRegisterInternal",
            "service CoreMetaReplicationInternal",
            "service AntiEntropyInternal",
            "service CrossRegionProxyInternal",
            "message InternalRequestHeader",
            "message CoreMetaPrepareReceipt",
            "message CoreMetaCommitCertificate",
            "message CoreMetaCertificatePersistReceipt",
            "message PutShardRequest",
            "message GetShardRequest",
            "message RepairShardRequest",
            "message CompareAndSwapRootRequest",
        ],
    );
}

#[test]
fn native_gateway_stream_registry_boundary_and_mesh_services_are_declared() {
    let proto = workspace_file("anvil-core/proto/anvil.proto");
    assert_contains_all(
        "RFC 0007 native service surface",
        &proto,
        &[
            "service StreamService",
            "rpc AppendRecord(AppendStreamRecordRequest) returns (AppendStreamRecordResponse);",
            "service RegistryService",
            "rpc PutPackageBlob(PutPackageBlobRequest) returns (WriteResponse);",
            "rpc PutPackageVersion(PutPackageVersionRequest) returns (WriteResponse);",
            "rpc PutRegistryRef(PutRegistryRefRequest) returns (WriteResponse);",
            "message PutPackageBlobRequest",
            "message PackageVersion",
            "service MeshControlService",
            "rpc PutRegion(PutRegionRequest) returns (WriteResponse);",
            "rpc GetPartitionMap(GetPartitionMapRequest) returns (PartitionMap);",
            "rpc StartBoundaryMigration(StartBoundaryMigrationRequest) returns (WriteResponse);",
            "message BoundaryMigrationStatus",
            "message ReadConsistency",
            "ReadConsistency consistency = 5;",
            "ReadConsistency consistency = 6;",
            "ReadConsistency consistency = 7;",
            "string page_token = 7;",
            "string page_token = 8;",
        ],
    );
}

#[test]
fn internal_block_coremeta_and_root_services_are_registered_without_string_dispatch() {
    let services = workspace_file("anvil-core/src/services/mod.rs");
    let internal = workspace_file("anvil-core/src/services/corestore_internal.rs");

    assert_contains_all(
        "registered CoreStore internal services",
        &services,
        &[
            "BlockStoreInternalServer::with_interceptor",
            "CoreMetaReplicationInternalServer::with_interceptor",
            "RootRegisterInternalServer::with_interceptor",
            "AntiEntropyInternalServer::with_interceptor",
            "CrossRegionProxyInternalServer::with_interceptor",
            "RegistryServiceServer::with_interceptor",
            "StreamServiceServer::with_interceptor",
            "MeshControlServiceServer::with_interceptor",
        ],
    );
    assert_contains_all(
        "CoreStore internal service implementation",
        &internal,
        &[
            "impl BlockStoreInternal for AppState",
            "impl CoreMetaReplicationInternal for AppState",
            "impl RootRegisterInternal for AppState",
            "impl AntiEntropyInternal for AppState",
            "impl CrossRegionProxyInternal for AppState",
            "put_internal_shard(CoreInternalPutShard",
            "coremeta_commit_evidence_encoded_row(",
            "write_coremeta_encoded_rows(&rows)",
            "catch_up_coremeta_rows(",
            "compare_and_swap_internal_root_anchor(",
            "validate_commit_evidence_with_verifier(",
            "ensure_internal_node_request",
            "system realm manage_nodes relation required",
        ],
    );
    assert_contains_none(
        "CoreStore internal service placeholders",
        &internal,
        &[
            "unimplemented!",
            "todo!",
            "Status::unimplemented",
            "placeholder",
            "fake",
            "compat",
        ],
    );
}

#[test]
fn coremeta_quorum_certificate_code_matches_rfc_protocol() {
    let source = workspace_file("anvil-core/src/core_store/coremeta_quorum.rs");
    assert_contains_all(
        "CoreMeta quorum/certificate implementation",
        &source,
        &[
            "CORE_META_DEFAULT_REPLICA_COUNT",
            "CORE_META_DEFAULT_QUORUM",
            "CoreMetaPendingBatchInput",
            "CoreMetaPrepareReceipt",
            "CoreMetaCommitCertificate",
            "CoreMetaCertificatePersistReceipt",
            "pending_batch_hash(",
            "committed_batch_hash(",
            "prepare_receipt_payload_hash(",
            "build_commit_certificate(",
            "validate_commit_evidence(",
            "certificate_persist_quorum",
            "signed_payload_hash",
            "signature must not be empty",
        ],
    );
    assert_contains_none(
        "CoreMeta quorum unsupported encodings",
        &source,
        &["serde_json", "cbor", "CBOR"],
    );
}

#[test]
fn storage_profiles_separate_metadata_quorum_from_erasure_coded_byte_profile() {
    let source = workspace_file("anvil-core/src/core_store/storage_profile.rs");
    assert_contains_all(
        "storage profile catalogue",
        &source,
        &[
            "CoreStorageClass",
            "CoreMetadataProfile",
            "CoreByteStorageProfile",
            "CoreInlinePayloadPolicy",
            "metadata-r3-q2",
            "standard-r3-ec4-2",
            "low-latency-replicated",
            "write_publish_threshold",
            "max_raw_payload_bytes",
            "absolute_encoded_record_max_bytes",
        ],
    );
}

#[test]
fn root_register_persists_through_coremeta_not_sidecar_files() {
    let roots = workspace_file("anvil-core/src/core_store/local_roots_layout.rs");
    let storage = workspace_file("anvil-core/src/storage.rs");
    assert_contains_all(
        "root register CoreMeta path",
        &roots,
        &[
            "CF_ROOT_CACHE",
            "TABLE_ROOT_CACHE_ROW",
            "root_anchor_generation_key",
            "root_cache_key",
            "commit_coremeta_batch_by_embedded_roots",
        ],
    );
    assert_contains_none(
        "root register file sidecar path",
        &format!("{roots}\n{storage}"),
        &[
            "core_store_root_register_path",
            "write_root_anchor_register_file",
            "read_root_anchor_generation_from_register",
            ".anr",
            "blocks/register",
        ],
    );
}
