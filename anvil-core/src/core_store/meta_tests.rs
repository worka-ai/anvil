use super::*;

fn test_tuple_key(part: &[u8]) -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&1u16.to_le_bytes());
    key.push(0x05);
    key.push(0);
    key.extend_from_slice(&(part.len() as u16).to_le_bytes());
    key.extend_from_slice(part);
    key
}

#[test]
fn stream_record_rows_reject_large_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let payload = vec![0x42; CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES + 1];
    let err = store
        .put(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &test_tuple_key(b"stream-record-row"),
            &payload,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn object_version_rows_reject_object_sized_payloads() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let payload = vec![0x99; CORE_META_MAX_VALUE_BYTES + 1];
    let err = store
        .put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &test_tuple_key(b"object-version-row"),
            &payload,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn inline_payload_rows_use_dedicated_cap_before_rocksdb_compression() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let payload = vec![0x11; CORE_META_MAX_INLINE_PAYLOAD_BYTES];
    let key = test_tuple_key(b"inline-ok");

    store.put_inline_payload(&key, &payload).unwrap();
    assert_eq!(store.get_inline_payload(&key).unwrap().unwrap(), payload);

    let oversized = vec![0x22; CORE_META_MAX_INLINE_PAYLOAD_BYTES + 1];
    let err = store
        .put_inline_payload(&test_tuple_key(b"inline-too-large"), &oversized)
        .unwrap_err();
    assert!(
        err.to_string().contains("exceeding"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn inline_payload_table_rejects_direct_large_payload_puts() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let payload = vec![0x33; CORE_META_MAX_INLINE_PAYLOAD_BYTES + 1];
    let err = store
        .put(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            &test_tuple_key(b"direct-inline-too-large"),
            &payload,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("inline payload") || err.to_string().contains("decode"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn table_specific_schema_markers_reject_wrong_payload_family() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let payload =
        encode_core_meta_inline_payload_row(b"not an object version", local_committed_row_common())
            .unwrap();
    let err = store
        .put(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &test_tuple_key(b"wrong-object-version-family"),
            &payload,
        )
        .unwrap_err();
    assert!(
        err.to_string().contains("payload schema"),
        "unexpected error: {err:#}"
    );
}

#[test]
fn reverse_range_scan_returns_rows_from_end_to_start() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let keys = [
        test_tuple_key(b"range-a"),
        test_tuple_key(b"range-b"),
        test_tuple_key(b"range-c"),
    ];
    for (index, key) in keys.iter().enumerate() {
        store
            .put_inline_payload(key, format!("payload-{index}").as_bytes())
            .unwrap();
    }

    let records = store
        .scan_range_reverse(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            &keys[0],
            &keys[2],
            keys.len(),
        )
        .unwrap();
    let tuple_keys = records
        .iter()
        .map(|record| core_meta_record_tuple_key(&record.key).unwrap().to_vec())
        .collect::<Vec<_>>();

    assert_eq!(
        tuple_keys,
        vec![keys[2].clone(), keys[1].clone(), keys[0].clone()]
    );
}
