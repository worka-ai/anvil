use super::*;

#[tokio::test]
async fn core_store_deduplicates_large_content_across_logical_names_and_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let payload = vec![7_u8; 80 * 1024];

    let first = store
        .put_blob(PutBlob {
            logical_name: "mesh:test/tenant:a/bucket:data/object:one".to_string(),
            bytes: payload.clone(),
            boundary_values: vec![CoreBoundaryValue {
                schema_generation: 1,
                name: "customer_tenant".to_string(),
                value_type: "string".to_string(),
                value: "tenant-a".to_string(),
                categories: vec!["storage_partition".to_string(), "query_prune".to_string()],
                source_kind: "user_metadata".to_string(),
                required: true,
                max_values_per_block: 1,
                placement_affinity: "none".to_string(),
                compaction_scope: "none".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
            }],
            region_id: "local".to_string(),
            mutation_id: "dedupe-mut-1".to_string(),
        })
        .await
        .unwrap();
    let second = store
        .put_blob(PutBlob {
            logical_name: "mesh:test/tenant:b/bucket:data/object:two".to_string(),
            bytes: payload.clone(),
            boundary_values: vec![CoreBoundaryValue {
                schema_generation: 1,
                name: "customer_tenant".to_string(),
                value_type: "string".to_string(),
                value: "tenant-b".to_string(),
                categories: vec!["storage_partition".to_string(), "query_prune".to_string()],
                source_kind: "user_metadata".to_string(),
                required: true,
                max_values_per_block: 1,
                placement_affinity: "none".to_string(),
                compaction_scope: "none".to_string(),
                shared_ranges_allowed: false,
                shared_record_kinds: Vec::new(),
            }],
            region_id: "local".to_string(),
            mutation_id: "dedupe-mut-2".to_string(),
        })
        .await
        .unwrap();

    assert_eq!(first.hash, second.hash);
    assert_eq!(first.encoding.block_id, second.encoding.block_id);
    assert_eq!(
        store.get_blob(GetBlob { object_ref: first }).await.unwrap(),
        payload
    );
    assert_eq!(
        store
            .get_blob(GetBlob { object_ref: second })
            .await
            .unwrap(),
        payload
    );
}
#[tokio::test]
async fn core_store_put_blob_writes_erasure_shards_and_reconstructs_missing_data() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let payload = vec![0x41; 80 * 1024];
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "mesh:test/tenant:t/bucket:b/object:a".to_string(),
            bytes: payload.clone(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "mut-1".to_string(),
        })
        .await
        .unwrap();
    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    assert_eq!(manifest.encoding.profile_id, LOCAL_ERASURE_PROFILE_ID);
    assert_eq!(manifest.encoding.data_shards, LOCAL_DATA_SHARDS as u16);
    assert_eq!(manifest.encoding.parity_shards, LOCAL_PARITY_SHARDS as u16);
    assert_eq!(
        manifest.encoding.minimum_read_shards,
        LOCAL_DATA_SHARDS as u16
    );
    assert_eq!(
        manifest.encoding.minimum_write_ack_shards,
        (LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS) as u16
    );
    assert_eq!(manifest.encoding.placement_scope, "region");
    assert_eq!(manifest.encoding.repair_priority, "normal");
    assert_eq!(
        manifest.placements.len(),
        LOCAL_DATA_SHARDS + LOCAL_PARITY_SHARDS
    );
    for placement in &manifest.placements {
        assert_eq!(placement.region_id, "local");
        assert!(
            placement.cell_id.starts_with("local-cell-"),
            "placements must carry a cell failure-domain identity"
        );
        assert_eq!(placement.placement_epoch, LOCAL_PLACEMENT_EPOCH);
        assert_eq!(placement.fsync_sequence, LOCAL_SHARD_FSYNC_SEQUENCE);
        let path = store.shard_path(
            &placement.node_id,
            &object_ref.encoding.block_id,
            placement.shard_index,
        );
        assert!(
            path.starts_with(
                storage
                    .core_store_root_path()
                    .join("blocks")
                    .join("local-cache")
                    .join(LOCAL_ERASURE_SET_ID)
                    .join(&placement.node_id)
            ),
            "shards must be placed under the RFC CoreStore block cache"
        );
        assert_eq!(
            path.extension().and_then(|value| value.to_str()),
            Some("anb")
        );
        assert!(
            path.exists(),
            "replica shard must exist at {}",
            path.display()
        );
        let shard_file = tokio::fs::read(&path).await.unwrap();
        assert!(
            shard_file.starts_with(CORE_BLOCK_SHARD_MAGIC),
            "physical shard files must use the RFC block-shard container"
        );
        let header_len_offset = CORE_BLOCK_SHARD_MAGIC.len() + 2;
        let header_len = u32::from_le_bytes(
            shard_file[header_len_offset..header_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let header_start = header_len_offset + 4;
        let header_bytes = &shard_file[header_start..header_start + header_len];
        assert!(
            BlockShardHeaderProto::decode(header_bytes).is_ok(),
            "block shard header must be deterministic protobuf"
        );
        assert!(serde_json::from_slice::<serde_json::Value>(header_bytes).is_err());
        let expected_block_id = object_ref.encoding.block_id.clone();
        let boundary_summary_hash = boundary_summary_hash(&manifest.boundary_values).unwrap();
        let boundary_values_b64 = encode_boundary_values_b64(&manifest.boundary_values).unwrap();
        let payload = read_block_shard_file(
            &path,
            BlockShardExpectation {
                block_id: &expected_block_id,
                shard_index: placement.shard_index,
                erasure_profile_id: LOCAL_ERASURE_PROFILE_ID,
                placement_epoch: placement.placement_epoch,
                payload_hash: &placement.shard_hash,
                payload_len: placement.stored_size,
                boundary_summary_hash: Some(&boundary_summary_hash),
                boundary_values_b64: Some(&boundary_values_b64),
            },
            "test_read_block_shard",
        )
        .await
        .unwrap();
        assert_eq!(payload.len() as u64, placement.stored_size);
        assert!(
            read_block_shard_file(
                &path,
                BlockShardExpectation {
                    block_id: &expected_block_id,
                    shard_index: placement.shard_index,
                    erasure_profile_id: LOCAL_ERASURE_PROFILE_ID,
                    placement_epoch: placement.placement_epoch + 1,
                    payload_hash: &placement.shard_hash,
                    payload_len: placement.stored_size,
                    boundary_summary_hash: Some(&boundary_summary_hash),
                    boundary_values_b64: Some(&boundary_values_b64),
                },
                "test_read_block_shard_stale_epoch",
            )
            .await
            .is_err(),
            "block shard validation must reject stale placement epochs"
        );
    }

    for placement in manifest.placements.iter().take(LOCAL_PARITY_SHARDS) {
        let path = store.shard_path(
            &placement.node_id,
            &object_ref.encoding.block_id,
            placement.shard_index,
        );
        tokio::fs::remove_file(path).await.unwrap();
    }

    let reconstructed = store
        .get_blob(GetBlob {
            object_ref: object_ref.clone(),
        })
        .await
        .unwrap();
    assert_eq!(reconstructed, payload);
}

#[tokio::test]
async fn core_store_get_blob_fails_when_too_many_erasure_shards_are_missing() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: "mesh:test/tenant:t/bucket:b/object:a".to_string(),
            bytes: vec![0x42; 80 * 1024],
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "mut-1".to_string(),
        })
        .await
        .unwrap();
    let manifest = store.read_object_manifest(&object_ref).await.unwrap();
    for placement in manifest.placements.iter().take(LOCAL_PARITY_SHARDS + 1) {
        let path = store.shard_path(
            &placement.node_id,
            &object_ref.encoding.block_id,
            placement.shard_index,
        );
        tokio::fs::remove_file(path).await.unwrap();
    }

    let err = store.get_blob(GetBlob { object_ref }).await.unwrap_err();
    assert!(
        err.to_string().contains("has only"),
        "unexpected error: {err:?}"
    );
}

#[tokio::test]
async fn core_store_streams_are_gap_free_hash_chained_and_idempotent() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let first = store
        .append_stream(AppendStreamRecord {
            stream_id: "object_metadata:tenant:b".to_string(),
            partition_id: "partition-1".to_string(),
            record_kind: "object.put".to_string(),
            payload: br#"{"key":"a"}"#.to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("idem-1".to_string()),
        })
        .await
        .unwrap();
    let replay = store
        .append_stream(AppendStreamRecord {
            stream_id: "object_metadata:tenant:b".to_string(),
            partition_id: "partition-1".to_string(),
            record_kind: "object.put".to_string(),
            payload: br#"{"key":"a"}"#.to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("idem-1".to_string()),
        })
        .await
        .unwrap();
    assert!(replay.idempotent_replay);
    assert_eq!(first.sequence, replay.sequence);

    let second = store
        .append_stream(AppendStreamRecord {
            stream_id: "object_metadata:tenant:b".to_string(),
            partition_id: "partition-1".to_string(),
            record_kind: "object.delete".to_string(),
            payload: br#"{"key":"a"}"#.to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("idem-2".to_string()),
        })
        .await
        .unwrap();
    assert_eq!(second.sequence, 2);
    let records = store
        .read_stream(ReadStream {
            stream_id: "object_metadata:tenant:b".to_string(),
            after_sequence: 0,
            limit: 10,
        })
        .await
        .unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1].previous_event_hash, records[0].event_hash);
    let stream_ids = store
        .list_stream_ids("object_metadata:")
        .await
        .expect("list stream ids");
    assert_eq!(stream_ids, vec!["object_metadata:tenant:b".to_string()]);
    assert!(
        read_test_pending_mutation_records(&store).await.is_empty(),
        "finalised stream appends must be checkpointed out of RocksDB admission metadata"
    );
}

#[tokio::test]
async fn core_store_read_stream_page_uses_corestore_stream_metadata() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    for sequence in 1..=3 {
        store
            .append_stream(AppendStreamRecord {
                stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
                partition_id: "tenant:t/bucket:b".to_string(),
                record_kind: format!("event.{sequence}"),
                payload: format!(r#"{{"sequence":{sequence}}}"#).into_bytes(),
                content_type: None,
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some(format!("event-{sequence}")),
            })
            .await
            .unwrap();
    }

    let raw_records = store
        .read_raw_stream("tenant:t/bucket:b/ranged-stream")
        .await
        .unwrap();
    assert_eq!(raw_records.len(), 3);
    assert_eq!(raw_records[2].record_kind, "event.3");

    let page = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
            after_sequence: 0,
            limit: 2,
        })
        .await
        .unwrap();
    assert_eq!(page.len(), 2);
    assert_eq!(page[0].record_kind, "event.1");
    assert_eq!(page[1].record_kind, "event.2");

    let full = store
        .read_stream(ReadStream {
            stream_id: "tenant:t/bucket:b/ranged-stream".to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await
        .unwrap();
    assert_eq!(full.len(), 3);
}

#[tokio::test]
async fn core_store_transaction_stream_is_root_anchored() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();

    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/root-anchor-proof".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.root_anchor_proof".to_string(),
            payload: br#"{"ok":true}"#.to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("root-anchor-proof".to_string()),
        })
        .await
        .unwrap();

    let transaction_records = store
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap();
    assert!(
        transaction_records
            .iter()
            .any(|record| record.record_kind == CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND),
        "CoreStore transaction stream must replay through root-anchor metadata"
    );

    let root_key_hash = root_key_hash(core_transaction_root_anchor_key());
    assert!(
        count_root_cache_generations(&store, &root_key_hash) >= 2,
        "CoreStore root anchors must be committed as CoreMeta generation rows"
    );
    assert_eq!(
        count_files_with_extension(
            &tmp.path().join("corestore").join("blocks").join("register"),
            "anr"
        ),
        0,
        "CoreStore must not create root-anchor sidecar shard files"
    );

    drop(store);
    let recovered = CoreStore::new(storage).await.unwrap();
    let latest_anchor = recovered
        .read_latest_root_anchor(core_transaction_root_anchor_key())
        .await
        .unwrap()
        .expect("latest transaction root anchor");
    let transaction_manifest_locator = latest_anchor
        .transaction_manifest
        .clone()
        .expect("root anchor transaction manifest locator");
    assert!(is_inline_manifest_body_locator(
        &transaction_manifest_locator
    ));
    assert!(transaction_manifest_locator.block_locators.is_empty());
    let transaction_manifest_bytes = recovered
        .read_inline_manifest_body(&transaction_manifest_locator)
        .unwrap();
    assert!(transaction_manifest_bytes.starts_with(CORE_TRANSACTION_MANIFEST_MAGIC));
    let header_len_offset = CORE_TRANSACTION_MANIFEST_MAGIC.len() + 2;
    let header_len = u32::from_le_bytes(
        transaction_manifest_bytes[header_len_offset..header_len_offset + 4]
            .try_into()
            .unwrap(),
    ) as usize;
    let body_len_offset = header_len_offset + 4;
    let body_len = u64::from_le_bytes(
        transaction_manifest_bytes[body_len_offset..body_len_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    let header_start = body_len_offset + 8;
    let body_start = header_start + header_len;
    let header_bytes = &transaction_manifest_bytes[header_start..body_start];
    let body_bytes = &transaction_manifest_bytes[body_start..body_start + body_len];
    assert!(decode_transaction_manifest_proto(header_bytes, body_bytes).is_ok());
    assert!(serde_json::from_slice::<serde_json::Value>(header_bytes).is_err());
    let transaction_manifest = decode_transaction_manifest_record(&transaction_manifest_bytes)
        .expect("decode transaction manifest frame");
    assert_eq!(
        transaction_manifest.post_root_generation,
        latest_anchor.root_generation
    );
    assert_eq!(transaction_manifest.logical_manifests.len(), 1);
    let records = recovered
        .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
        .await
        .unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.record_kind == CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND),
        "CoreStore must recover transaction stream records from the latest root anchor"
    );
}

#[tokio::test]
async fn core_store_bootstraps_system_root_anchor_once() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage.clone()).await.unwrap();
    let root_key_hash = root_key_hash(core_transaction_root_anchor_key());
    assert_eq!(
        count_root_cache_generations(&store, &root_key_hash),
        1,
        "startup bootstrap must write exactly one CoreMeta genesis root generation"
    );
    assert_eq!(
        count_files_with_extension(
            &tmp.path().join("corestore").join("blocks").join("register"),
            "anr"
        ),
        0,
        "startup bootstrap must not write root-anchor sidecar shard files"
    );
    let genesis = store
        .read_latest_root_anchor(core_transaction_root_anchor_key())
        .await
        .unwrap()
        .expect("genesis root anchor");
    assert_eq!(genesis.root_generation, 0);
    assert!(genesis.transaction_manifest.is_none());
    assert!(genesis.checkpoint_manifest.is_none());
    assert_eq!(genesis.publisher_node_id, "genesis");
    assert_eq!(genesis.publisher_epoch, 0);
    assert_eq!(genesis.partition_owner_fence, 0);
    assert_eq!(genesis.created_at_unix_nanos, 0);
    assert_eq!(genesis.mutation_first.as_deref(), Some("genesis"));
    assert_eq!(genesis.mutation_last.as_deref(), Some("genesis"));
    let genesis_bundle = genesis
        .genesis_bundle
        .as_ref()
        .expect("genesis root anchor must embed genesis bundle");
    validate_core_genesis_bundle(genesis_bundle, core_transaction_root_anchor_key()).unwrap();

    drop(store);
    let reopened = CoreStore::new(storage).await.unwrap();
    assert_eq!(
        count_root_cache_generations(&reopened, &root_key_hash),
        1,
        "bootstrap must be idempotent after restart"
    );
    assert!(
        reopened
            .read_direct_stream_records(CORE_TRANSACTION_STREAM_ID)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn core_store_root_anchor_rejects_conflicting_or_skipped_generations() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let genesis = store
        .read_latest_root_anchor(core_transaction_root_anchor_key())
        .await
        .unwrap()
        .expect("genesis root anchor");

    store
        .write_root_anchor_generation(&genesis)
        .await
        .expect("same root generation and bytes are idempotent");

    let mut conflict = genesis.clone();
    conflict.created_at_unix_nanos = conflict.created_at_unix_nanos.saturating_add(1);
    assert!(
        store.write_root_anchor_generation(&conflict).await.is_err(),
        "same root generation with different bytes must fail create-new CAS"
    );

    let mut missing_manifest = genesis.clone();
    missing_manifest.root_generation = 1;
    missing_manifest.previous_root_hash = hash_root_anchor_record(&genesis).unwrap();
    assert!(
        store
            .write_root_anchor_generation(&missing_manifest)
            .await
            .unwrap_err()
            .to_string()
            .contains("transaction manifest"),
        "non-genesis roots must not be published without transaction evidence"
    );

    let mut skipped = genesis.clone();
    skipped.root_generation = 2;
    skipped.previous_root_hash = hash_root_anchor_record(&genesis).unwrap();
    assert!(
        store.write_root_anchor_generation(&skipped).await.is_err(),
        "root anchor publication must not skip generations"
    );

    let root_key_hash = root_key_hash(core_transaction_root_anchor_key());
    let genesis_row = store
        .read_committed_root_anchor_generation(&root_key_hash, 0)
        .await
        .unwrap()
        .expect("genesis root generation row");
    assert_eq!(genesis_row.root_generation, 0);
    assert_eq!(
        count_root_cache_generations(&store, &root_key_hash),
        1,
        "failed root publications must not create extra CoreMeta generation rows"
    );
}

#[tokio::test]
async fn core_store_root_anchor_uses_coremeta_rows_not_shard_files() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    let root_key_hash = root_key_hash(core_transaction_root_anchor_key());

    let rows = store
        .meta
        .scan_prefix(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_prefix(&root_key_hash),
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let anchor = decode_root_cache_row(&rows[0].payload).unwrap();
    assert_eq!(anchor.root_generation, 0);
    assert_eq!(anchor.root_key_hash, root_key_hash);
    assert_eq!(
        count_files_with_extension(
            &tmp.path().join("corestore").join("blocks").join("register"),
            "anr"
        ),
        0,
        "CoreStore root anchors must not be persisted through root-anchor sidecar files"
    );
}

#[tokio::test]
async fn core_store_root_anchor_has_single_concurrent_winner() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = Arc::new(CoreStore::new(storage).await.unwrap());
    let locator_a = store
        .write_logical_bytes_direct(
            "core_control",
            "lf_root_cas_a".to_string(),
            1,
            b"root cas contender a".to_vec(),
            "root_cas_a".to_string(),
            "local".to_string(),
        )
        .await
        .unwrap();
    let locator_b = store
        .write_logical_bytes_direct(
            "core_control",
            "lf_root_cas_b".to_string(),
            1,
            b"root cas contender b".to_vec(),
            "root_cas_b".to_string(),
            "local".to_string(),
        )
        .await
        .unwrap();

    let current = store
        .read_latest_root_anchor(core_transaction_root_anchor_key())
        .await
        .unwrap()
        .expect("current root anchor after locator writes");
    let previous_root_hash = hash_root_anchor_record(&current).unwrap();
    let next_generation = current.root_generation + 1;
    let root_key_hash_value = root_key_hash(core_transaction_root_anchor_key());
    let evidence_payload = encode_materialisation_cursor_row(next_generation).unwrap();
    let evidence_key_a =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("root-cas-evidence-a")]).unwrap();
    let evidence_a = store
        .commit_coremeta_batch_for_root(
            &root_key_hash_value,
            current.root_generation,
            next_generation,
            "root-cas-evidence-a",
            &[CoreMetaBatchOp {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: &evidence_key_a,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&evidence_payload),
            }],
        )
        .await
        .unwrap();
    let evidence_key_b =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("root-cas-evidence-b")]).unwrap();
    let evidence_b = store
        .commit_coremeta_batch_for_root(
            &root_key_hash_value,
            current.root_generation,
            next_generation,
            "root-cas-evidence-b",
            &[CoreMetaBatchOp {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: &evidence_key_b,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&evidence_payload),
            }],
        )
        .await
        .unwrap();

    let anchor = |mutation_id: &str,
                  locator: CoreManifestLocator,
                  certificate_hash: &str,
                  receipt_hashes: &[String]|
     -> CoreRootAnchorRecord {
        CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: core_transaction_root_anchor_key().to_string(),
            root_key_hash: root_key_hash_value.clone(),
            root_generation: next_generation,
            previous_root_hash: previous_root_hash.clone(),
            transaction_manifest: Some(locator),
            checkpoint_manifest: None,
            core_meta_commit_certificate_hash: Some(certificate_hash.to_string()),
            certificate_persist_receipt_hashes: receipt_hashes.to_vec(),
            publisher_node_id: CORE_PENDING_MUTATION_NODE_ID.to_string(),
            publisher_epoch: LOCAL_PLACEMENT_EPOCH,
            partition_owner_fence: LOCAL_PLACEMENT_EPOCH,
            created_at_unix_nanos: unix_timestamp_nanos(),
            root_state: "committed".to_string(),
            mutation_first: Some(mutation_id.to_string()),
            mutation_last: Some(mutation_id.to_string()),
            writer_families: vec!["core_control".to_string()],
            manifest_count: 1,
            final_block_count: 1,
            genesis_bundle: None,
        }
    };
    let anchor_a = anchor(
        "root-cas-a",
        locator_a,
        &evidence_a.certificate_hash,
        &evidence_a.certificate_persist_receipt_hashes,
    );
    let anchor_b = anchor(
        "root-cas-b",
        locator_b,
        &evidence_b.certificate_hash,
        &evidence_b.certificate_persist_receipt_hashes,
    );
    let mut invalid_non_genesis = anchor_a.clone();
    invalid_non_genesis.genesis_bundle =
        Some(build_core_genesis_bundle(core_transaction_root_anchor_key()).unwrap());
    assert!(
        validate_root_anchor_record(&invalid_non_genesis)
            .unwrap_err()
            .to_string()
            .contains("genesis bundle"),
        "non-genesis roots must not carry the embedded genesis bundle"
    );
    let barrier = Arc::new(tokio::sync::Barrier::new(2));

    let task_a = {
        let store = store.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store.write_root_anchor_generation_local(&anchor_a).await
        })
    };
    let task_b = {
        let store = store.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            barrier.wait().await;
            store.write_root_anchor_generation_local(&anchor_b).await
        })
    };
    let results = vec![task_a.await.unwrap(), task_b.await.unwrap()];
    assert_eq!(
        results.iter().filter(|result| result.is_ok()).count(),
        1,
        "root anchor create-new CAS must produce exactly one winner"
    );
    assert_eq!(
        results.iter().filter(|result| result.is_err()).count(),
        1,
        "root anchor create-new CAS must reject the loser"
    );

    let latest = store
        .read_latest_root_anchor(core_transaction_root_anchor_key())
        .await
        .unwrap()
        .expect("winner root anchor");
    assert_eq!(latest.root_generation, next_generation);
    assert!(matches!(
        latest.mutation_first.as_deref(),
        Some("root-cas-a") | Some("root-cas-b")
    ));
}

#[tokio::test]
async fn core_store_root_discovery_requires_previous_hash_chain() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();
    store
        .append_stream(AppendStreamRecord {
            stream_id: "tenant:t/bucket:b/root-chain".to_string(),
            partition_id: "tenant:t/bucket:b".to_string(),
            record_kind: "event.created".to_string(),
            payload: b"root chain".to_vec(),
            content_type: None,
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("root-chain-event".to_string()),
        })
        .await
        .unwrap();
    let root_key_hash = root_key_hash(core_transaction_root_anchor_key());
    assert!(
        store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .unwrap()
            .root_generation
            > 0
    );

    store
        .meta
        .delete(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_key(&root_key_hash, 0),
        )
        .unwrap();
    assert!(
        store
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await
            .unwrap()
            .is_none(),
        "root discovery must not serve a higher generation whose previous_root_hash chain cannot be verified"
    );
}
