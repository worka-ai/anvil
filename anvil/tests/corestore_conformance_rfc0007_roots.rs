use std::{
    fs,
    path::{Path, PathBuf},
};

use anvil::core_store::{
    AppendStreamRecord, CF_INLINE_PAYLOADS, CoreMetaStore, CoreStore, GetBlob, PutBlob, ReadStream,
};
use anvil::storage::Storage;

fn count_files_with_extension(root: &Path, extension: &str) -> usize {
    let Ok(entries) = fs::read_dir(root) else {
        return 0;
    };
    let mut count = 0usize;
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            count += count_files_with_extension(&path, extension);
        } else if path.extension().is_some_and(|actual| actual == extension) {
            count += 1;
        }
    }
    count
}

fn collect_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_files(&path, out);
        } else {
            out.push(path);
        }
    }
}

fn path_has_component_sequence(path: &Path, components: &[&str]) -> bool {
    let actual = path
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => part.to_str(),
            _ => None,
        })
        .collect::<Vec<_>>();

    actual
        .windows(components.len())
        .any(|window| window == components)
}

fn legacy_stream_sidecar_violations(root: &Path) -> Vec<String> {
    let mut files = Vec::new();
    collect_files(root, &mut files);

    files
        .into_iter()
        .filter_map(|path| {
            let legacy_extension = format!("{}{}", "an", "stream");
            let has_legacy_extension = path
                .extension()
                .is_some_and(|extension| extension == legacy_extension.as_str());
            let has_legacy_data_dir = path_has_component_sequence(&path, &["streams", "data"]);
            if !has_legacy_extension && !has_legacy_data_dir {
                return None;
            }

            Some(
                path.strip_prefix(root)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .replace('\\', "/"),
            )
        })
        .collect()
}

#[tokio::test]
async fn rfc_0007_large_compressible_payloads_do_not_inline_into_rocksdb() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let meta_path = storage.core_store_meta_path();
    let store = CoreStore::new(storage).await.unwrap();

    let small_payload = vec![0x11; 1024];
    let small_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:inline-small".to_string(),
            bytes: small_payload.clone(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "inline-small".to_string(),
        })
        .await
        .unwrap();
    assert!(
        small_ref.placements.is_empty(),
        "bounded tiny payloads should use CoreMeta inline rows"
    );
    assert_eq!(
        store
            .get_blob(GetBlob {
                object_ref: small_ref.clone(),
            })
            .await
            .unwrap(),
        small_payload
    );

    let large_payload = vec![0x22; 5 * 1024 * 1024];
    let large_ref = store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object/large-compressible".to_string(),
            bytes: large_payload.clone(),
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "large-compressible".to_string(),
        })
        .await
        .unwrap();
    assert!(
        !large_ref.placements.is_empty(),
        "large raw payloads must use the erasure-coded byte pipeline even when they compress well"
    );
    assert_ne!(
        large_ref.encoding.placement_scope, "coremeta-inline",
        "large raw payloads must not become CoreMeta inline payloads after compression"
    );
    assert_eq!(large_ref.encoding.profile_id, "ec-4-2");
    assert_eq!(
        store
            .get_blob(GetBlob {
                object_ref: large_ref,
            })
            .await
            .unwrap(),
        large_payload
    );

    let meta = CoreMetaStore::open(meta_path).unwrap();
    let rows = meta.scan_all_encoded_rows().unwrap();
    let oversized = rows
        .iter()
        .filter(|row| row.value_envelope.len() > 64 * 1024)
        .map(|row| (row.cf.clone(), row.value_envelope.len()))
        .collect::<Vec<_>>();
    assert!(
        oversized.is_empty(),
        "CoreMeta RocksDB values must stay under 64 KiB: {oversized:#?}"
    );
    assert!(
        rows.iter().any(|row| row.cf == CF_INLINE_PAYLOADS),
        "small payload path should use the dedicated inline payload column family"
    );

    let block_files =
        count_files_with_extension(&tmp.path().join("corestore").join("blocks"), "anb");
    assert!(
        block_files >= 6,
        "large payload should publish erasure-coded block shards, found {block_files}"
    );
}

#[tokio::test]
async fn rfc_0007_core_transaction_stream_is_root_anchor_backed() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    store
        .append_stream(AppendStreamRecord {
            stream_id: "object_metadata:tenant:root-anchor-proof".to_string(),
            partition_id: "tenant:root-anchor-proof".to_string(),
            record_kind: "object_metadata.put".to_string(),
            payload: br#"{"object":"proof"}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("root-anchor-proof".to_string()),
        })
        .await
        .unwrap();

    let transaction_records = store
        .read_stream(ReadStream {
            stream_id: "core_transactions".to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await
        .unwrap();
    assert!(
        transaction_records
            .iter()
            .any(|record| record.record_kind == "core_pending_mutation.finalisation"),
        "CoreStore transaction stream must replay through root-anchor-backed metadata"
    );
    assert_eq!(
        count_files_with_extension(
            &tmp.path().join("corestore").join("blocks").join("register"),
            "anr"
        ),
        0,
        "RFC 0007 root anchors must not use root-anchor sidecar shard files"
    );
}

#[tokio::test]
async fn rfc_0007_corestore_data_dir_has_no_final_json_or_legacy_sidecars() {
    let tmp = tempfile::tempdir().unwrap();
    let storage = Storage::new_at(tmp.path()).await.unwrap();
    let store = CoreStore::new(storage).await.unwrap();

    store
        .put_blob(PutBlob {
            logical_name: "tenant:t/bucket:b/object:durable-scan".to_string(),
            bytes: vec![0x42; 96 * 1024],
            boundary_values: Vec::new(),
            region_id: "local".to_string(),
            mutation_id: "durable-scan-object".to_string(),
        })
        .await
        .unwrap();
    store
        .append_stream(AppendStreamRecord {
            stream_id: "object_metadata:tenant:durable-scan".to_string(),
            partition_id: "tenant:durable-scan".to_string(),
            record_kind: "object_metadata.put".to_string(),
            payload: br#"{"payload":"application-json-is-allowed"}"#.to_vec(),
            content_type: Some("application/json".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: None,
            idempotency_key: Some("durable-scan-stream".to_string()),
        })
        .await
        .unwrap();
    drop(store);

    let mut files = Vec::new();
    collect_files(tmp.path(), &mut files);
    let forbidden_extensions = [
        "json",
        "jsonl",
        "journal",
        "manifest",
        "sidecar",
        "anjournal",
        "anstream",
        "anroot",
    ];
    let mut violations = legacy_stream_sidecar_violations(tmp.path());
    violations.extend(files.into_iter().filter_map(|path| {
        let extension = path.extension().and_then(|value| value.to_str())?;
        if !forbidden_extensions.contains(&extension) {
            return None;
        }
        Some(
            path.strip_prefix(tmp.path())
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/"),
        )
    }));
    violations.sort();
    violations.dedup();
    assert!(
        violations.is_empty(),
        "CoreStore data directory must not contain final JSON/custom journal sidecars: {violations:#?}"
    );
}
