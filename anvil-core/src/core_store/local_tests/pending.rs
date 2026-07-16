use super::local_stream_control::control_record_proto::encode_object_manifest_record;
use super::*;
use crate::core_store::meta::{CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW};

#[tokio::test]
async fn core_store_pending_mutation_records_never_inline_large_payloads_before_finalisation() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let bytes = vec![b'x'; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 1];
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:large"),
            "large-payload-admission".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();

    let pending_mutation_records = read_test_pending_mutation_records(&store).await;
    assert_eq!(pending_mutation_records.len(), 1);
    assert!(
        pending_mutation_records[0].1.is_empty(),
        "large payloads must never be embedded in RocksDB admission payloads"
    );
    let landed = pending_mutation_records[0].0.landed_bytes.first().unwrap();
    assert_eq!(landed.length, bytes.len() as u64);
    assert!(
        storage
            .resolve_relative_storage_path(&landed.relative_path)
            .unwrap()
            .exists(),
        "large payload bytes must land outside the pending mutation and be referenced by hash/length"
    );
    store
        .verify_landed_bytes_ref_row(
            &landed.landing_id,
            "large-payload-admission",
            &landed.sha256,
            landed.length,
            &[],
        )
        .unwrap();
    assert!(
        !storage
            .core_store_landed_bytes_path()
            .join("sha256")
            .join(landed.sha256.trim_start_matches("sha256:")[0..2].to_string())
            .join(format!(
                "{}.meta",
                landed.sha256.trim_start_matches("sha256:")
            ))
            .exists(),
        "landed bytes metadata must live in RocksDB/CoreMeta, not sidecar files"
    );
}

#[tokio::test]
async fn core_store_landed_bytes_are_removed_only_after_last_coremeta_reference() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let bytes = vec![b'd'; CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES + 8];

    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:dedup-a"),
            "dedup-admission-a".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:dedup-b"),
            "dedup-admission-b".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();

    let records = read_test_pending_mutation_records(&store).await;
    assert_eq!(records.len(), 2);
    let first_landed = records[0].0.landed_bytes[0].clone();
    let landed_path = storage
        .resolve_relative_storage_path(&first_landed.relative_path)
        .unwrap();

    store
        .remove_finalised_landed_bytes(&records[0].0)
        .await
        .unwrap();
    assert!(landed_path.exists());

    store
        .remove_finalised_landed_bytes(&records[1].0)
        .await
        .unwrap();
    assert!(!landed_path.exists());
}

#[tokio::test]
async fn corestore_rocksdb_records_never_inline_large_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let stream_id = "tenant:t/bucket:b/stream:large-payload".to_string();
    let payload = vec![0x5a; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES * 8];

    store
        .append_stream(AppendStreamRecord {
            stream_id: stream_id.clone(),
            partition_id: "partition:large-payload".to_string(),
            record_kind: "object.put".to_string(),
            payload: payload.clone(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("large-stream-payload".to_string()),
        })
        .await
        .unwrap();

    let rows = store
        .meta
        .scan_prefix(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_prefix(&stream_id),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0].payload.len() <= CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        "stream record metadata row must stay bounded and payload-free"
    );
    let row = super::local_stream_control::control_record_proto::decode_stream_record_index_row(
        &rows[0].payload,
    )
    .unwrap();
    assert_eq!(row.payload_len, payload.len() as u64);
    assert_eq!(row.payload_hash, format!("sha256:{}", sha256_hex(&payload)));
    assert_eq!(row.inline_payload, None);
    assert!(
        row.payload_locator
            .as_ref()
            .is_some_and(|locator| locator.manifest_length > 0)
    );
    assert!(
        serde_json::from_slice::<serde_json::Value>(&rows[0].payload).is_err(),
        "stream record metadata row must use protobuf, not JSON"
    );

    let read = store
        .read_stream(ReadStream {
            stream_id,
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(read.len(), 1);
    assert_eq!(read[0].payload, payload);

    let object_payload = vec![0xa5; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES * 8];
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:large-payload".to_string(),
            bytes: object_payload.clone(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "large-object-payload".to_string(),
        })
        .await
        .unwrap();
    assert!(object_ref.manifest_ref.starts_with("core-manifest-sha256:"));
    assert_eq!(object_ref.logical_size, object_payload.len() as u64);
    assert!(
        !object_ref.placements.is_empty(),
        "large object bytes must produce CoreStore shard placement metadata"
    );
    for placement in &object_ref.placements {
        assert!(
            store
                .shard_path(
                    &placement.node_id,
                    &object_ref.encoding.block_id,
                    placement.shard_index,
                )
                .exists(),
            "large object shard must exist for placement {placement:?}"
        );
    }
    assert_eq!(
        store
            .get_blob(GetBlob {
                object_ref: object_ref.clone()
            })
            .await
            .unwrap(),
        object_payload
    );
    let object_rows = store
        .meta
        .scan_prefix(CF_OBJECT_VERSIONS, TABLE_OBJECT_VERSION_META_ROW, b"")
        .unwrap();
    assert!(!object_rows.is_empty());
    assert!(
        object_rows
            .iter()
            .all(|row| row.payload.len() <= CORE_META_MAX_VALUE_BYTES),
        "object metadata rows must remain bounded manifests/locators"
    );
    let inline_rows = store
        .meta
        .scan_prefix(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, b"")
        .unwrap();
    assert!(
        inline_rows.iter().all(|row| row.payload != object_payload),
        "large object bytes must not be copied into inline RocksDB payload rows"
    );
}

#[tokio::test]
async fn core_store_landed_bytes_existing_file_must_match_hash() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let bytes = b"expected landed bytes".to_vec();
    let hash = sha256_hex(&bytes);
    let final_path = store.landed_bytes_path(&hash);
    fs::create_dir_all(final_path.parent().unwrap())
        .await
        .unwrap();
    fs::write(&final_path, vec![0x55; bytes.len()])
        .await
        .unwrap();

    assert!(
        store
            .admit_core_mutation(
                "stream.append",
                "stream",
                test_stream_append_target(
                    "tenant:t/bucket:b/corrupt-landed",
                    "tenant:t/bucket:b",
                    "event.created",
                ),
                "corrupt-existing-landed".to_string(),
                None,
                CorePendingMutationPayload::Landed(&bytes),
                Vec::new(),
            )
            .await
            .unwrap_err()
            .to_string()
            .contains("existing hash mismatch"),
        "admission must verify existing landed bytes by hash before referencing them from pending mutation"
    );
}

#[tokio::test]
async fn core_store_pending_mutation_records_include_boundary_values() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            test_object_put_target("tenant:t/bucket:b/object:bounded"),
            "bounded-payload-admission".to_string(),
            None,
            CorePendingMutationPayload::Landed(b"bounded"),
            vec![CoreBoundaryValue {
                schema_generation: 2,
                name: "customer_tenant".to_string(),
                value_type: "uuid".to_string(),
                value: "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a".to_string(),
                categories: vec!["security_realm".to_string()],
                source_kind: "user_metadata_json_pointer".to_string(),
                required: true,
                max_values_per_block: 1,
                placement_affinity: "none".to_string(),
                compaction_scope: "none".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
            }],
        )
        .await
        .unwrap();

    let pending_mutation_records = read_test_pending_mutation_records(&store).await;
    assert_eq!(pending_mutation_records.len(), 1);
    assert_eq!(pending_mutation_records[0].0.boundary_values.len(), 1);
    assert_eq!(
        pending_mutation_records[0].0.boundary_values[0].name,
        "customer_tenant"
    );
    assert_eq!(
        pending_mutation_records[0].0.boundary_values[0].value,
        "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a"
    );
    let landed = pending_mutation_records[0].0.landed_bytes.first().unwrap();
    let stored = store
        .meta
        .get(
            CF_MATERIALISATION,
            TABLE_LANDED_BYTE_REF_ROW,
            &meta_tuple_key(&[b"landed-byte", landed.landing_id.as_bytes()]),
        )
        .unwrap()
        .expect("landed byte metadata row");
    let stored = decode_landed_byte_ref_row(&stored).unwrap();
    assert_eq!(stored.boundary_values[0].name, "customer_tenant");
}

#[tokio::test]
async fn core_store_pending_mutation_header_uses_deterministic_protobuf() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/canonical-pending_mutation",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "protobuf-pending_mutation-row".to_string(),
            Some("canonical-idempotency".to_string()),
            CorePendingMutationPayload::Inline(br#"{"ok":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();

    let row = store
        .meta
        .get(
            CF_TRANSACTIONS,
            TABLE_PENDING_MUTATION_ROW,
            &admission_record_key(1),
        )
        .unwrap()
        .expect("pending mutation row");
    assert!(decode_stored_pending_mutation_row(&row).is_ok());
    assert!(serde_json::from_slice::<serde_json::Value>(&row).is_err());
}

#[tokio::test]
async fn core_store_admission_records_are_not_file_backed() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/partial-pending_mutation",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "partial-tail-pending_mutation".to_string(),
            None,
            CorePendingMutationPayload::Inline(br#"{"ok":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();

    let records = store
        .read_pending_mutation_records_with_payload()
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert!(
        !store.admission_root().join("pending_mutation").exists(),
        "admission state must live in CoreStore metadata, not a sidecar pending mutation file"
    );
}

#[tokio::test]
async fn core_store_pending_mutation_admission_writes_signed_commit_certificate() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/certified-pending_mutation",
                "tenant:t/bucket:b",
                "event.created",
            ),
            "certified-pending_mutation-admission".to_string(),
            Some("certified-idempotency".to_string()),
            CorePendingMutationPayload::Landed(br#"{"ok":true}"#),
            Vec::new(),
        )
        .await
        .unwrap();

    let pending_mutation_hash_input = encode_pending_mutation_hash_input(&record, &[]).unwrap();
    let certificate = store
        .verify_local_pending_mutation_commit_certificate(&record, &pending_mutation_hash_input)
        .await
        .unwrap();
    assert_eq!(
        certificate.local_receipt.local_metadata_fsync_sequence,
        LOCAL_SHARD_FSYNC_SEQUENCE
    );
    assert_eq!(certificate.local_receipt.landed_byte_hashes.len(), 1);
    assert_eq!(certificate.local_receipt.descriptor_hashes.len(), 1);
    assert!(
        certificate
            .local_receipt
            .signed_payload_hash
            .starts_with("sha256:")
    );
    assert!(certificate.signed_payload_hash.starts_with("sha256:"));
    assert!(!certificate.local_receipt.source_signature.is_empty());
    assert!(!certificate.source_signature.is_empty());

    let certificate_bytes = store
        .meta
        .get(
            CF_TRANSACTIONS,
            TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW,
            &admission_certificate_key(record.sequence),
        )
        .unwrap()
        .expect("pending mutation certificate row");
    assert!(decode_admission_commit_certificate(&certificate_bytes).is_ok());
    assert!(serde_json::from_slice::<serde_json::Value>(&certificate_bytes).is_err());
}

#[tokio::test]
async fn core_store_recovery_rejects_uncertified_pending_mutation_record() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    write_test_pending_mutation_records(
        &store,
        vec![test_pending_mutation_record(
            "uncertified-pending_mutation-record",
            unix_timestamp_nanos(),
            1,
        )],
    )
    .await;
    store.unregister_process_instance_for_tests();
    drop(store);

    assert!(
        CoreStore::new(storage)
            .await
            .unwrap_err()
            .to_string()
            .contains("admission commit certificate"),
        "recovery must not replay a pending mutation that lacks committed admission evidence"
    );
}

#[tokio::test]
async fn core_store_object_manifest_includes_boundary_values() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let boundary_value = CoreBoundaryValue {
        schema_generation: 2,
        name: "customer_tenant".to_string(),
        value_type: "uuid".to_string(),
        value: "8e4b4477-99d8-4f4b-89db-876d2c7f0c6a".to_string(),
        categories: vec!["security_realm".to_string()],
        source_kind: "user_metadata_json_pointer".to_string(),
        required: true,
        max_values_per_block: 1,
        placement_affinity: "none".to_string(),
        compaction_scope: "none".to_string(),
        shared_ranges_allowed: false,
        shared_record_kinds: Vec::new(),
    };
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:bounded".to_string(),
            bytes: vec![0x62; 80 * 1024],
            boundary_values: vec![boundary_value.clone()],
            region_id: "local".to_string(),
            mutation_id: "bounded-manifest".to_string(),
        })
        .await
        .unwrap();

    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    assert_eq!(manifest.boundary_values, vec![boundary_value]);
    for placement in &manifest.placements {
        assert_ne!(placement.written_at_unix_nanos, 0);
        assert!(placement.signed_payload_hash.starts_with("sha256:"));
        assert_eq!(placement.signature_algorithm, "ed25519-libp2p");
        assert!(!placement.receipt_signature.is_empty());
    }

    let mut bad_manifest = manifest.clone();
    bad_manifest.placements[0].receipt_signature[0] ^= 0x01;
    store
        .meta
        .put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_manifest_meta_key(&object_ref),
            &encode_object_manifest_record(&bad_manifest).unwrap(),
        )
        .unwrap();
    assert!(
        store
            .get_blob(GetBlob {
                object_ref: object_ref.clone()
            })
            .await
            .unwrap_err()
            .to_string()
            .contains("signature verification failed")
    );
}

#[tokio::test]
async fn core_store_recovers_unfinalised_put_blob_pending_mutation_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let bytes = b"recover object from pending_mutation".to_vec();
    let logical_name = "tenant:t/bucket:b/object:recovered";
    let payload_hash = format!("sha256:{}", sha256_hex(&bytes));
    store
        .admit_core_mutation(
            "object.put",
            "object_blob",
            CorePendingMutationTarget::ObjectPut {
                logical_name: logical_name.to_string(),
                region_id: "local".to_string(),
                erasure_profile_id: LOCAL_ERASURE_PROFILE_ID.to_string(),
                encryption: "none".to_string(),
                block_plain_hash: payload_hash.clone(),
                object_hash: payload_hash.clone(),
                object_logical_size: bytes.len() as u64,
                compression: none_compression_descriptor(&bytes),
                writer_generation: 0_u64,
                block_ordinal: 0_u64,
            },
            "recover-object-from-pending_mutation".to_string(),
            None,
            CorePendingMutationPayload::Landed(&bytes),
            Vec::new(),
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let object_ref =
        test_object_ref_for_payload(&recovered, logical_name, &bytes, LOCAL_EC_4_2_PROFILE);
    assert_eq!(
        recovered
            .get_blob(GetBlob {
                object_ref: object_ref.clone()
            })
            .await
            .unwrap(),
        bytes
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint recovered object pending mutation rows"
    );
    assert_eq!(recovered.admission_landed_bytes().await.unwrap(), 0);
}

#[tokio::test]
async fn core_store_recovers_unfinalised_stream_append_pending_mutation_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let payload = br#"{"event":"recover"}"#.to_vec();
    let idempotency_key = "recover-stream-idempotency";
    store
        .admit_core_mutation(
            "stream.append",
            "stream",
            test_stream_append_target(
                "tenant:t/bucket:b/recovered-stream",
                "tenant:t/bucket:b",
                "event.recovered",
            ),
            "recover-stream-from-pending_mutation".to_string(),
            Some(idempotency_key.to_string()),
            CorePendingMutationPayload::Inline(&payload),
            Vec::new(),
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let records = recovered
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/recovered-stream".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_kind, "event.recovered");
    assert_eq!(records[0].payload, payload);
    let expected_idempotency_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
    assert_eq!(
        records[0].idempotency_key_hash.as_deref(),
        Some(expected_idempotency_hash.as_str())
    );
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint recovered stream pending mutation rows"
    );
}

#[tokio::test]
async fn core_store_recovers_unfinalised_mutation_batch_pending_mutation_on_startup() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let batch = CoreMutationBatch {
        transaction_id: "recover-mutation-batch".to_string(),
        scope_partition: "tenant:t/bucket:b".to_string(),
        committed_by_principal: "principal:recovery".to_string(),
        preconditions: Vec::new(),
        operations: vec![CoreMutationOperation::StreamAppend {
            partition_id: "tenant:t/bucket:b".to_string(),
            stream_id: "object_metadata:t:b:batch-recovered".to_string(),
            record_kind: "object.put".to_string(),
            payload: br#"{"object":"batch-recovered"}"#.to_vec(),
            idempotency_key: Some("batch-recovered-event".to_string()),
        }],
    };
    store
        .admit_core_mutation(
            "mutation.batch",
            "core_control",
            CorePendingMutationTarget::MutationBatch {
                transaction_id: batch.transaction_id.clone(),
                scope_partition: batch.scope_partition.clone(),
                operation_count: batch.operations.len() as u64,
            },
            batch.transaction_id.clone(),
            Some(batch.transaction_id.clone()),
            CorePendingMutationPayload::Inline(&encode_core_mutation_batch(&batch).unwrap()),
            Vec::new(),
        )
        .await
        .unwrap();
    store.unregister_process_instance_for_tests();
    drop(store);

    let recovered = CoreStore::new(storage).await.unwrap();
    let transaction = recovered
        .read_transaction("recover-mutation-batch")
        .await
        .unwrap()
        .expect("recovered transaction");
    assert_eq!(transaction.state, CoreTransactionState::Committed);
    let records = recovered
        .read_stream(ReadStream {
            stream_id: "object_metadata:t:b:batch-recovered".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].record_kind, "object.put");
    assert!(
        read_test_pending_mutation_records(&recovered)
            .await
            .is_empty(),
        "startup recovery must checkpoint recovered mutation batch pending mutation rows"
    );
}

#[tokio::test]
async fn core_store_admission_rejects_when_pending_mutation_hard_limit_would_be_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .meta
        .put(
            CF_TRANSACTIONS,
            TABLE_PENDING_MUTATION_ROW,
            &admission_record_key(1),
            &encode_stored_pending_mutation_row(
                &test_pending_mutation_record("capacity-row", 1, 1),
                b"capacity-payload",
            )
            .unwrap(),
        )
        .unwrap();
    store
        .meta
        .put(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &admission_sequence_key(),
            &encode_materialisation_cursor_row(1).unwrap(),
        )
        .unwrap();

    let err = store
        .enforce_admission_capacity_with_limits(
            16,
            0,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 32,
                pending_mutation_hard_limit_bytes: 40,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 1024,
                landed_bytes_hard_limit_bytes: 2048,
            },
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains(AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str())
    );
}

#[tokio::test]
async fn core_store_admission_rejects_when_landed_hard_limit_would_be_exceeded() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let landed_dir = store
        .admission_landed_bytes_root()
        .join("sha256")
        .join("aa");
    fs::create_dir_all(&landed_dir).await.unwrap();
    fs::write(landed_dir.join("aa-existing.landed"), vec![0_u8; 64])
        .await
        .unwrap();

    let err = store
        .enforce_admission_capacity_with_limits(
            0,
            64,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 1024,
                pending_mutation_hard_limit_bytes: 2048,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 96,
                landed_bytes_hard_limit_bytes: 100,
            },
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains(AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str())
    );
}

#[tokio::test]
async fn core_store_admission_rejects_when_pending_mutation_materialisation_lag_is_too_old() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    write_test_pending_mutation_records(
        &store,
        vec![test_pending_mutation_record(
            "old-lag-mutation",
            unix_timestamp_nanos().saturating_sub(301_000_000_000),
            1,
        )],
    )
    .await;

    let err = store
        .enforce_admission_capacity_with_limits(
            0,
            0,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 1024 * 1024,
                pending_mutation_hard_limit_bytes: 2 * 1024 * 1024,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 1024 * 1024,
                landed_bytes_hard_limit_bytes: 2 * 1024 * 1024,
            },
        )
        .await
        .unwrap_err();

    assert!(
        err.to_string()
            .contains(AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str())
    );
}

#[tokio::test]
async fn core_store_admission_lag_ignores_finalised_pending_mutation_records() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = test_pending_mutation_record(
        "old-finalised-mutation",
        unix_timestamp_nanos().saturating_sub(301_000_000_000),
        1,
    );
    write_test_pending_mutation_records(&store, vec![record.clone()]).await;
    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();

    store
        .enforce_admission_capacity_with_limits(
            0,
            0,
            CoreAdmissionCapacityLimits {
                pending_mutation_soft_limit_rows: 1_000_000,
                pending_mutation_hard_limit_rows: 2_000_000,
                pending_mutation_soft_limit_bytes: 1024 * 1024,
                pending_mutation_hard_limit_bytes: 2 * 1024 * 1024,
                pending_mutation_soft_lag_seconds: 60,
                pending_mutation_hard_lag_seconds: 300,
                landed_bytes_soft_limit_bytes: 1024 * 1024,
                landed_bytes_hard_limit_bytes: 2 * 1024 * 1024,
            },
        )
        .await
        .unwrap();
    assert!(
        read_test_pending_mutation_records(&store).await.is_empty(),
        "a fully finalised admission prefix must be checkpointed out of RocksDB metadata"
    );
    assert_eq!(store.next_core_mutation_sequence().await.unwrap(), 2);
}

#[tokio::test]
async fn core_store_pending_mutation_finalisation_is_idempotent_for_same_record() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let record = test_pending_mutation_record("same-finalisation", unix_timestamp_nanos(), 1);

    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();
    store
        .mark_pending_mutation_finalised_unlocked(&record, "committed")
        .await
        .unwrap();

    let finalisations = store
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap()
        .into_iter()
        .filter(|record| record.record_kind == CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND)
        .count();
    assert_eq!(finalisations, 1);

    let conflicting =
        test_pending_mutation_record("different-finalisation", unix_timestamp_nanos(), 1);
    assert!(
        store
            .mark_pending_mutation_finalised_unlocked(&conflicting, "committed")
            .await
            .is_err(),
        "same pending mutation node/epoch/sequence with a different mutation must fail closed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pending_mutation_finalisations_publish_one_contiguous_root_stream() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage).await.unwrap());
    let mut pending = Vec::new();
    for index in 0..8 {
        pending.push(
            store
                .admit_core_mutation(
                    "test.concurrent",
                    "core_control",
                    test_mutation_target(),
                    format!("concurrent-finalisation-{index}"),
                    Some(format!("concurrent-finalisation-key-{index}")),
                    CorePendingMutationPayload::Inline(b"concurrent-finalisation"),
                    Vec::new(),
                )
                .await
                .unwrap(),
        );
    }

    let barrier = Arc::new(tokio::sync::Barrier::new(pending.len()));
    let tasks = pending.into_iter().map(|record| {
        let store = store.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store
                .mark_pending_mutation_finalised_unlocked(&record, "committed")
                .await
        })
    });
    for result in futures_util::future::join_all(tasks).await {
        result.unwrap().unwrap();
    }

    let records = store
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap();
    assert_eq!(records.len(), 8);
    assert_eq!(
        records
            .iter()
            .map(|record| record.sequence)
            .collect::<Vec<_>>(),
        (1..=8).collect::<Vec<_>>()
    );
    assert_eq!(
        store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .expect("concurrent finalisation root anchor")
            .root_generation,
        8
    );
}

#[tokio::test]
async fn core_store_pending_mutation_checkpoint_preserves_high_watermark_when_prefix_is_unfinalised()
 {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let first = test_pending_mutation_record("unfinalised-prefix", unix_timestamp_nanos(), 1);
    let second = test_pending_mutation_record("finalised-after-gap", unix_timestamp_nanos(), 2);
    write_test_pending_mutation_records(&store, vec![first, second.clone()]).await;
    store
        .mark_pending_mutation_finalised_unlocked(&second, "committed")
        .await
        .unwrap();

    let pending_mutation_records = read_test_pending_mutation_records(&store).await;
    assert_eq!(
        pending_mutation_records.len(),
        1,
        "checkpointing may remove independently finalised pending mutation rows once the high watermark is persisted"
    );
    assert_eq!(
        store.next_core_mutation_sequence().await.unwrap(),
        3,
        "pending mutation sequence allocation must not reuse a finalised sequence that remains after an unfinalised prefix"
    );
}
