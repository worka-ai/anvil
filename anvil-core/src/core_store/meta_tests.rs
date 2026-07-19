use super::*;

fn test_tuple_key(part: &[u8]) -> Vec<u8> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Raw(part)]).unwrap()
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
        .scan_range_reverse_inclusive(
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

#[test]
fn prefix_scan_returns_only_physical_descendants() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let matching_prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("matching")]).unwrap();

    for index in 0..32_u64 {
        let matching_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("matching"),
            CoreMetaTuplePart::U64(index),
        ])
        .unwrap();
        store
            .put_inline_payload(&matching_key, format!("matching-{index}").as_bytes())
            .unwrap();

        let unrelated_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("unrelated"),
            CoreMetaTuplePart::U64(index),
        ])
        .unwrap();
        store
            .put_inline_payload(&unrelated_key, format!("unrelated-{index}").as_bytes())
            .unwrap();
    }

    let records = store
        .scan_prefix(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            &matching_prefix,
        )
        .unwrap();
    assert_eq!(records.len(), 32);
    assert!(records.iter().all(|record| {
        core_meta_record_tuple_key(&record.key)
            .unwrap()
            .starts_with(&matching_prefix)
    }));
}

#[test]
fn prefix_pages_seek_after_the_last_tuple_key() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("paged")]).unwrap();
    let keys = (0..5_u64)
        .map(|index| {
            core_meta_tuple_key(&[
                CoreMetaTuplePart::Utf8("paged"),
                CoreMetaTuplePart::U64(index),
            ])
            .unwrap()
        })
        .collect::<Vec<_>>();
    for (index, key) in keys.iter().enumerate() {
        store
            .put_inline_payload(key, format!("payload-{index}").as_bytes())
            .unwrap();
    }

    let first = store
        .scan_prefix_page(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            &prefix,
            None,
            2,
        )
        .unwrap();
    assert_eq!(first.len(), 2);
    let first_keys = first
        .iter()
        .map(|record| core_meta_record_tuple_key(&record.key).unwrap().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(first_keys, keys[..2]);

    let second = store
        .scan_prefix_page(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            &prefix,
            Some(&first_keys[1]),
            2,
        )
        .unwrap();
    let second_keys = second
        .iter()
        .map(|record| core_meta_record_tuple_key(&record.key).unwrap().to_vec())
        .collect::<Vec<_>>();
    assert_eq!(second_keys, keys[2..4]);
}

#[test]
fn coremeta_scans_reject_unbounded_limits_and_foreign_positions() {
    let tmp = tempfile::tempdir().unwrap();
    let store = CoreMetaStore::open(tmp.path()).unwrap();
    let prefix = core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("paged")]).unwrap();
    let foreign =
        core_meta_tuple_key(&[CoreMetaTuplePart::Utf8("other"), CoreMetaTuplePart::U64(1)])
            .unwrap();

    assert!(
        store
            .scan_prefix_page(
                CF_INLINE_PAYLOADS,
                TABLE_INLINE_PAYLOAD_ROW,
                &prefix,
                None,
                0,
            )
            .is_err()
    );
    assert!(
        store
            .scan_prefix_page(
                CF_INLINE_PAYLOADS,
                TABLE_INLINE_PAYLOAD_ROW,
                &prefix,
                None,
                CORE_META_MAX_SCAN_PAGE_ROWS + 1,
            )
            .is_err()
    );
    assert!(
        store
            .scan_prefix_page(
                CF_INLINE_PAYLOADS,
                TABLE_INLINE_PAYLOAD_ROW,
                &prefix,
                Some(&foreign),
                1,
            )
            .is_err()
    );
    assert!(
        store
            .scan_range_reverse_inclusive(
                CF_INLINE_PAYLOADS,
                TABLE_INLINE_PAYLOAD_ROW,
                &prefix,
                &prefix,
                0,
            )
            .is_err()
    );
}

#[test]
fn nonempty_store_without_current_format_marker_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let descriptors = column_families()
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, cf_options(name)))
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&options, tmp.path(), descriptors).unwrap();
        let cf = db.cf_handle(CF_META_VERSION).unwrap();
        db.put_cf(&cf, b"superseded-physical-key", b"superseded-value")
            .unwrap();
    }

    let error = CoreMetaStore::open(tmp.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("delete and recreate this pre-release store")
    );
}
