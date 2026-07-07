use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anvil::core_store::{
    AcquireFence, AppendStreamRecord, AuthzScopeRef, CORE_QUORUM_PROFILE_SCHEMA,
    CORE_ROOT_CATALOG_SCHEMA, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreQuorumProfile, CoreRootCatalog, CoreRootPartition, CoreStore, CoreTransactionState,
    GetBlob, PutBlob, ReadStream, SealStreamSegment, WatchRequest,
};
use anvil::gateway_store::{
    GatewayMountMatchKind, GatewayMountRecord, GatewayMountState, put_gateway_mount_record,
    resolve_gateway_mount,
};
use anvil::storage::Storage;

#[derive(Debug)]
struct DurableFeatureFamily {
    name: &'static str,
    source_files: &'static [&'static str],
    required_terms: &'static [&'static str],
    corestore_terms: &'static [&'static str],
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("anvil crate has workspace parent")
        .to_path_buf()
}

fn read_workspace_file(relative: &str) -> String {
    let path = workspace_root().join(relative);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn production_source(relative: &str) -> String {
    strip_cfg_test_modules(&read_workspace_file(relative))
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut out = String::new();
    let mut pending_cfg_test = false;
    let mut skipping_test_module = false;
    let mut depth: i32 = 0;

    for line in source.lines() {
        let trimmed = line.trim_start();

        if skipping_test_module {
            depth += brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }

        if pending_cfg_test && trimmed.starts_with("mod tests") {
            skipping_test_module = true;
            depth = brace_delta(line);
            if depth <= 0 {
                skipping_test_module = false;
                depth = 0;
            }
            pending_cfg_test = false;
            continue;
        }

        if pending_cfg_test {
            out.push_str("#[cfg(test)]\n");
            pending_cfg_test = false;
        }

        out.push_str(line);
        out.push('\n');
    }

    out
}

fn brace_delta(line: &str) -> i32 {
    let opens = line.as_bytes().iter().filter(|byte| **byte == b'{').count() as i32;
    let closes = line.as_bytes().iter().filter(|byte| **byte == b'}').count() as i32;
    opens - closes
}

#[test]
fn rfc_0006_no_durable_bypass_feature_families_are_corestore_backed() {
    let families = [
        DurableFeatureFamily {
            name: "object_payload",
            source_files: &["anvil-core/src/object_manager.rs"],
            required_terms: &["put_object", "PutBlob", "core_object_ref_to_shard_map"],
            corestore_terms: &["CoreStore", ".put_blob"],
        },
        DurableFeatureFamily {
            name: "object_metadata",
            source_files: &["anvil-core/src/metadata_journal.rs"],
            required_terms: &["JournalFrame", "append_object_mutation"],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "bucket_metadata",
            source_files: &["anvil-core/src/bucket_journal.rs"],
            required_terms: &["BucketJournalBody", "append_bucket_mutation"],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "object_link",
            source_files: &[
                "anvil-core/src/object_links.rs",
                "anvil-core/src/persistence.rs",
                "anvil-core/src/metadata_journal.rs",
            ],
            required_terms: &[
                "put_object_link",
                "ObjectEntryKind::Link",
                "append_object_mutation_with_permit",
            ],
            corestore_terms: &["CoreStore", "CoreMutationBatch", ".commit_mutation_batch"],
        },
        DurableFeatureFamily {
            name: "append_stream",
            source_files: &["anvil-core/src/append_journal.rs"],
            required_terms: &["AppendStreamRecord", "append_stream_record"],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationBatch",
                ".commit_mutation_batch",
                ".read_stream",
            ],
        },
        DurableFeatureFamily {
            name: "task_lease",
            source_files: &["anvil-core/src/task_lease.rs"],
            required_terms: &["TaskLease", "TaskLeaseOwner", "fence_token"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "authz_schema",
            source_files: &[
                "anvil-core/src/authz_schema.rs",
                "anvil-core/src/authz_realm_schema.rs",
            ],
            required_terms: &["AuthzNamespaceSchemaRecord", "write_authz_namespace_schema"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "authz_tuple",
            source_files: &["anvil-core/src/authz_journal.rs"],
            required_terms: &["AuthzTuple", "write_authz_tuple", "append_authz_tuple"],
            corestore_terms: &[
                "CoreStore",
                "CoreMutationBatch",
                ".commit_mutation_batch",
                ".read_stream",
            ],
        },
        DurableFeatureFamily {
            name: "authz_derived_index",
            source_files: &["anvil-core/src/authz_userset_index.rs"],
            required_terms: &["AuthzDerivedUsersetIndex", "write_derived_userset_index"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "path_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/typed_field_segment.rs",
            ],
            required_terms: &[
                "build_metadata_backed_index",
                "path",
                "write_typed_field_segment",
            ],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "typed_field_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/typed_field_segment.rs",
            ],
            required_terms: &[
                "build_typed_json_index",
                "write_typed_field_segment",
                "EncodedTypedValue",
            ],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "full_text_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/full_text_segment.rs",
            ],
            required_terms: &[
                "build_full_text_index",
                "write_full_text_segment",
                "FullTextSegment",
            ],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "vector_index",
            source_files: &[
                "anvil-core/src/index_builder.rs",
                "anvil-core/src/vector_segment.rs",
            ],
            required_terms: &[
                "build_vector_index",
                "write_vector_segment",
                "VectorSegment",
            ],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "package_repository",
            source_files: &["anvil-core/src/gateway_store.rs"],
            required_terms: &["GatewayRepositoryRecord", "create_gateway_repository"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "package_blob",
            source_files: &["anvil-core/src/gateway_store.rs"],
            required_terms: &["GatewayBlobRecord", "put_gateway_blob"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "gateway_mount",
            source_files: &["anvil-core/src/gateway_store.rs"],
            required_terms: &["GatewayMountRecord", "resolve_gateway_mount"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "mesh_route",
            source_files: &["anvil-core/src/mesh_directory.rs"],
            required_terms: &["BucketLocatorDescriptor", "append_control_mutation"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "node_lifecycle",
            source_files: &["anvil-core/src/mesh_lifecycle.rs"],
            required_terms: &["NodeDescriptor", "NodeDrainDescriptor"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "region_lifecycle",
            source_files: &["anvil-core/src/mesh_lifecycle.rs"],
            required_terms: &["RegionDescriptor", "region_drain"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "embedded_database_snapshot",
            source_files: &[
                "anvil-core/src/personaldb_snapshot_store.rs",
                "anvil-core/src/personaldb_snapshot_builder.rs",
            ],
            required_terms: &["write_personaldb_snapshot", "personaldb_snapshot_object"],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "embedded_database_changeset",
            source_files: &[
                "anvil-core/src/personaldb_commit_store.rs",
                "anvil-core/src/personaldb_segment.rs",
            ],
            required_terms: &[
                "write_personaldb_changeset_payload",
                "personaldb_changeset_payload",
            ],
            corestore_terms: &["CoreStore", ".put_blob", ".compare_and_swap_ref"],
        },
        DurableFeatureFamily {
            name: "audit_record",
            source_files: &["anvil-core/src/admin_audit.rs"],
            required_terms: &["AdminAuditEvent", "append_audit_event"],
            corestore_terms: &["CoreStore", ".append_stream", ".read_stream"],
        },
    ];

    let expected_names: BTreeSet<_> = [
        "object_payload",
        "object_metadata",
        "bucket_metadata",
        "object_link",
        "append_stream",
        "task_lease",
        "authz_schema",
        "authz_tuple",
        "authz_derived_index",
        "path_index",
        "typed_field_index",
        "full_text_index",
        "vector_index",
        "package_repository",
        "package_blob",
        "gateway_mount",
        "mesh_route",
        "node_lifecycle",
        "region_lifecycle",
        "embedded_database_snapshot",
        "embedded_database_changeset",
        "audit_record",
    ]
    .into_iter()
    .collect();
    let actual_names: BTreeSet<_> = families.iter().map(|family| family.name).collect();
    assert_eq!(
        actual_names, expected_names,
        "RFC 21.1 feature family coverage changed"
    );

    let mut failures = BTreeMap::new();
    for family in families {
        let combined = family
            .source_files
            .iter()
            .map(|file| production_source(file))
            .collect::<Vec<_>>()
            .join("\n");

        let missing_required: Vec<_> = family
            .required_terms
            .iter()
            .copied()
            .filter(|term| !combined.contains(term))
            .collect();
        let missing_corestore: Vec<_> = family
            .corestore_terms
            .iter()
            .copied()
            .filter(|term| !combined.contains(term))
            .collect();

        if !missing_required.is_empty() || !missing_corestore.is_empty() {
            failures.insert(family.name, (missing_required, missing_corestore));
        }
    }

    assert!(
        failures.is_empty(),
        "durable feature families must persist through CoreStore APIs; failures={failures:#?}"
    );
}

#[test]
fn rfc_0006_local_storage_guard_prevents_authoritative_feature_file_writes() {
    let source_root = workspace_root().join("anvil-core/src");
    let mut files = Vec::new();
    collect_rs_files(&source_root, &mut files);

    let allowed_direct_io = BTreeSet::from([
        // CoreStore owns local staging, shard, manifest, ref, watch and lock files.
        "anvil-core/src/core_store/local.rs",
        // Storage owns transient upload staging only; object payloads are moved into CoreStore.
        "anvil-core/src/storage.rs",
        // Node identity/keypair are operator bootstrap files, not durable feature state.
        "anvil-core/src/cluster_identity.rs",
        // System realm bootstrap writes the first-admin credential file for the operator once;
        // system realm state itself is still committed through CoreStore-authz paths.
        "anvil-core/src/system_realm.rs",
        // Snapshot builder uses SQLite scratch files before writing snapshots through CoreStore.
        "anvil-core/src/personaldb_snapshot_builder.rs",
    ]);

    let forbidden_write_patterns = [
        "tokio::fs::write(",
        "std::fs::write(",
        "fs::write(",
        "tokio::fs::File::create(",
        "fs::File::create(",
        "File::create(",
        "OpenOptions::new(",
        "tokio::fs::rename(",
        "fs::rename(",
    ];

    let mut violations = Vec::new();
    for path in files {
        let relative = path
            .strip_prefix(workspace_root())
            .expect("source path is under workspace")
            .to_string_lossy()
            .replace('\\', "/");
        let source = strip_cfg_test_modules(
            &fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {relative}: {error}")),
        );

        for (line_index, line) in source.lines().enumerate() {
            if forbidden_write_patterns
                .iter()
                .any(|pattern| line.contains(pattern))
                && !allowed_direct_io.contains(relative.as_str())
            {
                violations.push(format!("{}:{}: {}", relative, line_index + 1, line.trim()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "authoritative durable feature code must not write local files outside CoreStore-owned staging/cache/shard/scratch paths:\n{}",
        violations.join("\n")
    );
}

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in
        fs::read_dir(dir).unwrap_or_else(|error| panic!("read dir {}: {error}", dir.display()))
    {
        let entry = entry.expect("read directory entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn rfc_0006_protected_writers_use_commit_time_partition_preconditions() {
    let protected_writers = [
        "anvil-core/src/append_journal.rs",
        "anvil-core/src/authz_journal.rs",
        "anvil-core/src/bucket_journal.rs",
        "anvil-core/src/control_journal.rs",
        "anvil-core/src/hf_journal.rs",
        "anvil-core/src/index_diagnostic_journal.rs",
        "anvil-core/src/index_journal.rs",
        "anvil-core/src/manifest_journal.rs",
        "anvil-core/src/metadata_journal.rs",
        "anvil-core/src/model_journal.rs",
        "anvil-core/src/multipart_journal.rs",
        "anvil-core/src/task_journal.rs",
        "anvil-core/src/mesh_directory.rs",
        "anvil-core/src/mesh_lifecycle.rs",
        "anvil-core/src/services/personaldb.rs",
    ];

    for relative in protected_writers {
        let source = production_source(relative);
        assert!(
            !source.contains("validate_partition_write("),
            "{relative} must not prevalidate a partition write and then perform a separate visible write"
        );
        assert!(
            source.contains("partition_write_ref_precondition(")
                || source.contains("personaldb_group_write_precondition("),
            "{relative} must derive a CoreRef precondition from the current partition owner"
        );
    }

    for relative in [
        "anvil-core/src/append_journal.rs",
        "anvil-core/src/authz_journal.rs",
        "anvil-core/src/bucket_journal.rs",
        "anvil-core/src/control_journal.rs",
        "anvil-core/src/hf_journal.rs",
        "anvil-core/src/index_diagnostic_journal.rs",
        "anvil-core/src/index_journal.rs",
        "anvil-core/src/manifest_journal.rs",
        "anvil-core/src/metadata_journal.rs",
        "anvil-core/src/model_journal.rs",
        "anvil-core/src/multipart_journal.rs",
        "anvil-core/src/task_journal.rs",
        "anvil-core/src/mesh_control_stream.rs",
        "anvil-core/src/personaldb_heads.rs",
    ] {
        let source = production_source(relative);
        assert!(
            source.contains("CoreMutationBatch") && source.contains(".commit_mutation_batch"),
            "{relative} must make protected visible writes through CoreMutationBatch"
        );
    }
}

#[tokio::test]
async fn rfc_0006_root_catalog_is_signed_generationed_and_recoverable() {
    const KEY: &[u8] = b"rfc-0006-root-catalog-key";
    const ZERO_HASH: &str =
        "sha256:0000000000000000000000000000000000000000000000000000000000000000";

    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root_segment = store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0006/system/mesh/root-segment/head".to_string(),
            bytes: br#"{"refs":[],"streams":[]}"#.to_vec(),
            region_id: "local".to_string(),
            mutation_id: "rfc-0006-root-segment-1".to_string(),
        })
        .await
        .unwrap();
    let embedded_manifest = store.read_object_manifest(&root_segment).await.unwrap();

    let catalog = CoreRootCatalog {
        schema: CORE_ROOT_CATALOG_SCHEMA.to_string(),
        mesh_id: "mesh-rfc0006".to_string(),
        generation: 1,
        previous_hash: ZERO_HASH.to_string(),
        root_partitions: vec![CoreRootPartition {
            partition_id: "core.root.refs.0".to_string(),
            owner_node_id: "node-rfc0006".to_string(),
            fence: 1,
            placement_group: "root-pg-0".to_string(),
            embedded_head_segment_manifest: embedded_manifest,
        }],
        placement_catalog_ref: "core.ref:/system/placement/current".to_string(),
        stream_directory_ref: "core.ref:/system/streams/current".to_string(),
        ref_directory_ref: "core.ref:/system/refs/current".to_string(),
        authz_system_realm_ref: "core.ref:/system/authz/realm/current".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        signed_by: "node-rfc0006".to_string(),
        signature: String::new(),
    };

    let receipt = store
        .commit_root_catalog(catalog.clone(), KEY)
        .await
        .expect("genesis root catalog commits");
    assert_eq!(receipt.generation, 1);
    assert!(receipt.catalog_hash.starts_with("sha256:"));

    let latest = store
        .read_latest_root_catalog("mesh-rfc0006", KEY)
        .await
        .unwrap()
        .expect("latest root catalog");
    assert_eq!(latest.generation, 1);
    assert_eq!(latest.signed_by, "node-rfc0006");
    assert!(
        store
            .read_latest_root_catalog("mesh-rfc0006", b"wrong-key")
            .await
            .is_err(),
        "root catalog signatures must reject the wrong key"
    );
    assert!(
        store.commit_root_catalog(catalog, KEY).await.is_err(),
        "stale root catalog generations must be rejected"
    );
    assert_eq!(
        store
            .list_root_catalog_history("mesh-rfc0006")
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn rfc_0006_quorum_profile_requires_intersection_and_monotonic_epochs() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    let genesis = CoreQuorumProfile {
        schema: CORE_QUORUM_PROFILE_SCHEMA.to_string(),
        placement_group: "pg-rfc0006".to_string(),
        replica_count: 5,
        write_quorum: 3,
        read_quorum: 3,
        fence_quorum: 3,
        epoch: 1,
    };
    let receipt = store
        .commit_quorum_profile(genesis.clone())
        .await
        .expect("intersecting genesis quorum profile commits");
    assert_eq!(receipt.placement_group, "pg-rfc0006");
    assert_eq!(receipt.epoch, 1);
    assert!(receipt.profile_hash.starts_with("sha256:"));

    let latest = store
        .read_latest_quorum_profile("pg-rfc0006")
        .await
        .unwrap()
        .expect("latest quorum profile");
    assert_eq!(latest, genesis);

    let mut stale = latest.clone();
    stale.epoch = 1;
    assert!(
        store.commit_quorum_profile(stale).await.is_err(),
        "a new active epoch must immediately follow the current committed epoch"
    );

    let non_intersecting = CoreQuorumProfile {
        schema: CORE_QUORUM_PROFILE_SCHEMA.to_string(),
        placement_group: "pg-rfc0006-bad".to_string(),
        replica_count: 5,
        write_quorum: 2,
        read_quorum: 2,
        fence_quorum: 2,
        epoch: 1,
    };
    let err = store
        .commit_quorum_profile(non_intersecting)
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("do not intersect"),
        "non-intersecting read/write/fence quorums must be rejected: {err:?}"
    );

    let next = CoreQuorumProfile { epoch: 2, ..latest };
    store
        .commit_quorum_profile(next)
        .await
        .expect("next quorum profile epoch commits");
    let history = store
        .list_quorum_profile_history("pg-rfc0006")
        .await
        .unwrap();
    assert_eq!(history.len(), 2);
    assert_eq!(history[0].epoch, 1);
    assert_eq!(history[1].epoch, 2);
}

#[tokio::test]
async fn rfc_0006_corestore_transactions_gate_ref_stream_and_watch_visibility() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0006/tenant:t/bucket:b/object:alpha/payload".to_string(),
            bytes: br#"{"name":"alpha"}"#.to_vec(),
            region_id: "local".to_string(),
            mutation_id: "payload-alpha".to_string(),
        })
        .await
        .unwrap();
    let target = format!("core-object-ref:{}", object_ref.hash);

    let receipt = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-visible".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:rfc0006".to_string(),
            preconditions: vec![CoreMutationPrecondition::Ref {
                ref_name: "tenant/t/bucket/b/object/alpha/current".to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: true,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
            }],
            operations: vec![
                CoreMutationOperation::RefUpdate {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    ref_name: "tenant/t/bucket/b/object/alpha/current".to_string(),
                    new_target: target.clone(),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: "tenant:t/bucket:b".to_string(),
                    stream_id: "object_metadata:t:b".to_string(),
                    record_kind: "object.put".to_string(),
                    payload: br#"{"object":"alpha"}"#.to_vec(),
                    idempotency_key: Some("object-alpha-put".to_string()),
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(receipt.visible_updates.len(), 2);
    assert_eq!(
        store
            .read_transaction("txn-rfc0006-visible")
            .await
            .unwrap()
            .expect("transaction")
            .state,
        CoreTransactionState::Committed
    );
    let current = store
        .read_ref("tenant/t/bucket/b/object/alpha/current")
        .await
        .unwrap()
        .expect("current ref");
    assert_eq!(current.target, target);
    assert_eq!(
        store.get_blob(GetBlob { object_ref }).await.unwrap(),
        br#"{"name":"alpha"}"#
    );
    assert_eq!(
        store
            .read_stream(ReadStream {
                stream_id: "object_metadata:t:b".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .watch(WatchRequest {
                stream_prefix: "object_metadata:t".to_string(),
                after_cursor: None,
                limit: 10,
            })
            .await
            .unwrap()
            .len(),
        1
    );

    let failed = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-failed".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:rfc0006".to_string(),
            preconditions: vec![CoreMutationPrecondition::Ref {
                ref_name: "tenant/t/bucket/b/object/alpha/current".to_string(),
                expected_generation: Some(999),
                expected_target: None,
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
            }],
            operations: vec![CoreMutationOperation::RefUpdate {
                partition_id: "tenant:t/bucket:b".to_string(),
                ref_name: "tenant/t/bucket/b/object/alpha/current".to_string(),
                new_target: "core-object-ref:should-not-be-visible".to_string(),
            }],
        })
        .await;
    assert!(failed.is_err());
    assert!(
        store
            .read_transaction("txn-rfc0006-failed")
            .await
            .unwrap()
            .is_none(),
        "failed mutation batches must not publish transaction records"
    );
    assert_eq!(
        store
            .read_ref("tenant/t/bucket/b/object/alpha/current")
            .await
            .unwrap()
            .expect("current ref")
            .target,
        current.target
    );
}

#[tokio::test]
async fn rfc_0006_coreobject_manifests_are_quorum_replicated_control_records() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "mesh:rfc0006/tenant:t/bucket:b/object:manifest/payload".to_string(),
            bytes: br#"{"manifest":"quorum"}"#.to_vec(),
            region_id: "local".to_string(),
            mutation_id: "payload-manifest-quorum".to_string(),
        })
        .await
        .unwrap();
    assert!(
        object_ref.manifest_ref.starts_with("core-manifest-sha256:"),
        "manifest_ref must be a CoreStore logical manifest reference, not a local file path"
    );
    assert!(
        !tmp.path().join("_core").join("manifests").exists(),
        "manifests must not use a single-copy top-level local manifest directory"
    );

    let manifest_hash = object_ref
        .manifest_ref
        .strip_prefix("core-manifest-sha256:")
        .unwrap();
    let manifest_file = format!("{manifest_hash}.json");
    let manifest_path = |index: usize| {
        storage
            .core_store_replica_path(&format!("local-control-node-{index}"))
            .join("manifests")
            .join("sha256")
            .join(&manifest_hash[0..2])
            .join(&manifest_file)
    };
    for index in 1..=5 {
        assert!(
            manifest_path(index).exists(),
            "manifest replica {index} should exist"
        );
    }

    for index in 1..=2 {
        fs::remove_file(manifest_path(index)).unwrap();
    }
    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    assert_eq!(manifest.object_hash, object_ref.hash);

    fs::remove_file(manifest_path(3)).unwrap();
    assert!(
        store.read_object_manifest(&object_ref).await.is_err(),
        "manifest reads must fail closed without read quorum"
    );
}

#[tokio::test]
async fn rfc_0006_gateway_mounts_resolve_scope_before_route_handling() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let mount = GatewayMountRecord {
        schema: "anvil.gateway.mount.v1".to_string(),
        mount_id: "docker-west".to_string(),
        gateway: "docker".to_string(),
        hosts: vec!["registry.example.test".to_string()],
        path_prefixes: vec!["/".to_string(), "/v2/".to_string()],
        mesh_id: "mesh-a".to_string(),
        region: "eu-west-1".to_string(),
        anvil_storage_tenant_id: "storage-tenant-a".to_string(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: "storage-tenant-a".to_string(),
            authz_realm_id: "realm-a".to_string(),
        },
        tenant_id: "tenant-a".to_string(),
        registry_instance_id: "registry-a".to_string(),
        default_bucket: "packages".to_string(),
        repository_prefix: String::new(),
        state: GatewayMountState::Active,
        generation: 0,
        record_hash: String::new(),
    };
    assert_eq!(
        put_gateway_mount_record(&storage, mount, None)
            .await
            .unwrap(),
        1
    );

    let exact = resolve_gateway_mount(&storage, "registry.example.test", "/v2/team/api/tags/list")
        .await
        .unwrap()
        .expect("exact host mount");
    assert_eq!(exact.match_kind, GatewayMountMatchKind::ExactHostAlias);
    assert_eq!(exact.matched_path_prefix, "/v2/");
    assert_eq!(exact.record.authz_scope.authz_realm_id, "realm-a");

    let virtual_host = resolve_gateway_mount(
        &storage,
        "registry-a.tenant-a.eu-west-1.anvil-storage.com",
        "/v2/team/api/manifests/latest",
    )
    .await
    .unwrap()
    .expect("virtual host mount");
    assert_eq!(
        virtual_host.match_kind,
        GatewayMountMatchKind::VirtualHostRegional
    );
    assert_eq!(virtual_host.record.authz_scope.authz_realm_id, "realm-a");

    let path_style = resolve_gateway_mount(
        &storage,
        "eu-west-1.anvil-storage.com",
        "/tenant-a/_gateway/docker/registry-a/v2/team/api/blobs/sha256:abc",
    )
    .await
    .unwrap()
    .expect("path-style mount");
    assert_eq!(
        path_style.match_kind,
        GatewayMountMatchKind::PathStyleRegional
    );
    assert_eq!(
        path_style.matched_path_prefix,
        "/tenant-a/_gateway/docker/registry-a/"
    );

    assert!(
        resolve_gateway_mount(
            &storage,
            "eu-west-1.anvil-storage.com",
            "/tenant-a/_gateway/npm/registry-a/package",
        )
        .await
        .unwrap()
        .is_none(),
        "gateway kind and authz scope must come from the mount, not caller-supplied path text"
    );
}

#[tokio::test]
async fn rfc_0006_corestore_streams_are_chained_and_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    let first = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.created".to_string(),
            payload: br#"{"step":1}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-created-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(first.sequence, 1);
    assert!(!first.idempotent_replay);

    let replay = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.created".to_string(),
            payload: br#"{"step":1}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-created-1".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(replay.sequence, first.sequence);
    assert_eq!(replay.event_hash, first.event_hash);
    assert!(replay.idempotent_replay);

    let conflicting_replay = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.created".to_string(),
            payload: br#"{"step":99}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-created-1".to_string()),
        })
        .await;
    assert!(
        conflicting_replay.is_err(),
        "same idempotency key with different bytes must fail"
    );

    let second = store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "audit.updated".to_string(),
            payload: br#"{"step":2}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("audit-updated-2".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(second.sequence, 2);

    let records = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/audit".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].event_hash, first.event_hash);
    assert_eq!(records[1].previous_event_hash, records[0].event_hash);
    assert_eq!(records[1].event_hash, second.event_hash);
    assert_ne!(
        records[1].previous_event_hash, records[1].event_hash,
        "a stream record must chain to the prior event, not itself"
    );

    let watched = store
        .watch(WatchRequest {
            stream_prefix: "tenant:t/bucket:b".to_string(),
            after_cursor: Some(first.cursor),
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(watched.len(), 1);
    assert_eq!(watched[0].cursor, second.cursor);
    assert_eq!(watched[0].previous_event_hash, records[0].event_hash);
    assert_eq!(watched[0].event_hash, records[1].event_hash);
    assert_eq!(watched[0].event_type, "audit.updated");
    assert_eq!(watched[0].transaction_id, None);
    assert_eq!(watched[0].payload_hash, records[1].payload_hash);
}

#[tokio::test]
async fn rfc_0006_sealed_stream_segments_use_binary_frame_and_stream_remains_open() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.created".to_string(),
            payload: br#"{"event":1}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("event-1".to_string()),
        })
        .await
        .unwrap();
    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.updated".to_string(),
            payload: br#"{"event":2}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("event-2".to_string()),
        })
        .await
        .unwrap();

    let sealed = store
        .seal_stream_segment(SealStreamSegment {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            through_sequence: Some(2),
            segment_kind: "audit".to_string(),
            mutation_id: "seal-events-1".to_string(),
        })
        .await
        .unwrap();
    let segment_bytes = store
        .get_blob(GetBlob {
            object_ref: sealed.object_ref.clone(),
        })
        .await
        .unwrap();
    assert!(
        segment_bytes.starts_with(b"ANSEG001"),
        "sealed stream segment must use the RFC binary frame magic"
    );
    let decoded = store.read_stream_segment(&sealed).await.unwrap();
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded[0].record_kind, "event.created");
    assert_eq!(decoded[1].record_kind, "event.updated");

    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.after_seal".to_string(),
            payload: br#"{"event":3}"#.to_vec(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("event-3".to_string()),
        })
        .await
        .unwrap();
    let records = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/events".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(
        records.len(),
        3,
        "sealing must not close the logical stream"
    );
    assert_eq!(records[2].record_kind, "event.after_seal");
}

#[tokio::test]
async fn rfc_0006_fenced_mutations_use_authenticated_principal_not_request_owner_text() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let fence = store
        .acquire_fence(AcquireFence {
            fence_name: "tenant:t/bucket:b/object:secure".to_string(),
            authenticated_principal: "principal:legitimate-worker".to_string(),
            ttl_ms: 60_000,
        })
        .await
        .unwrap();

    let stale_owner = store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-wrong-principal".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:impersonator".to_string(),
            preconditions: vec![CoreMutationPrecondition::Fence {
                fence_name: "tenant:t/bucket:b/object:secure".to_string(),
                fence_token: fence.fence_token,
            }],
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: "object_metadata:t:b:secure".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"secure"}"#.to_vec(),
                idempotency_key: None,
            }],
        })
        .await;
    assert!(
        stale_owner.is_err(),
        "fenced mutation must derive owner from committed_by_principal and reject impersonation"
    );
    assert!(
        store
            .read_stream(ReadStream {
                stream_id: "object_metadata:t:b:secure".to_string(),
                after_sequence: 0,
                limit: 10,
            })
            .await
            .unwrap()
            .is_empty(),
        "failed fenced mutation must not publish protected stream records"
    );

    store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: "txn-rfc0006-right-principal".to_string(),
            scope_partition: "tenant:t/bucket:b".to_string(),
            committed_by_principal: "principal:legitimate-worker".to_string(),
            preconditions: vec![CoreMutationPrecondition::Fence {
                fence_name: "tenant:t/bucket:b/object:secure".to_string(),
                fence_token: fence.fence_token,
            }],
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id: "tenant:t/bucket:b".to_string(),
                stream_id: "object_metadata:t:b:secure".to_string(),
                record_kind: "object.put".to_string(),
                payload: br#"{"object":"secure"}"#.to_vec(),
                idempotency_key: None,
            }],
        })
        .await
        .unwrap();
}
