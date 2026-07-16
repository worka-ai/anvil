use super::*;
use tempfile::tempdir;

const KEY: &[u8] = b"multipart journal partition owner key";

#[tokio::test]
async fn multipart_current_rows_drive_upload_parts_and_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = create_multipart_upload(&storage, 1, 2, "obj")
        .await
        .unwrap()
        .upload;
    upsert_multipart_part(
        &storage,
        upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
    )
    .await
    .unwrap();
    upsert_multipart_part(
        &storage,
        upload.id,
        1,
        payload_ref("hash-b", 11),
        11,
        "etag-b",
    )
    .await
    .unwrap();
    assert_eq!(
        list_multipart_parts(&storage, upload.id).await.unwrap()[0].etag,
        "etag-b"
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        complete_multipart_upload(&storage, upload.id)
            .await
            .unwrap()
            .completed
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn multipart_current_rows_work_without_history_stream_records() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = test_upload(7, 1, 2, "obj-no-history");
    let part = test_part(upload.id, 2, "hash-no-history", 12, "etag-no-history");

    write_current_rows_for_test(&storage, &upload, std::slice::from_ref(&part))
        .await
        .unwrap();

    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    assert!(
        read_events_from_store(&core_store, &multipart_metadata_stream_id(1, 2))
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj-no-history", upload.upload_id)
            .await
            .unwrap()
            .is_some()
    );
    let parts = list_multipart_parts(&storage, upload.id).await.unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].etag, "etag-no-history");
}

#[tokio::test]
async fn multipart_current_rows_ignore_tampered_history_stream() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = create_multipart_upload(&storage, 1, 2, "obj")
        .await
        .unwrap()
        .upload;
    upsert_multipart_part(
        &storage,
        upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
    )
    .await
    .unwrap();

    let core_store = CoreStore::new(storage.clone()).await.unwrap();
    core_store
        .corrupt_stream_record_payload_for_test(&multipart_metadata_stream_id(1, 2), 1)
        .unwrap();
    assert!(
        read_events_from_store(&core_store, &multipart_metadata_stream_id(1, 2))
            .await
            .is_err()
    );

    assert!(
        get_active_multipart_upload(&storage, 1, 2, "obj", upload.upload_id)
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        list_multipart_parts(&storage, upload.id).await.unwrap()[0].etag,
        "etag-a"
    );
    assert!(has_active_multipart_upload(&storage, 2).await.unwrap());
}

#[tokio::test]
async fn multipart_history_alone_does_not_create_current_state() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let upload = test_upload(1, 1, 2, "audit-only");
    append_history_only_for_test(
        &storage,
        upload.tenant_id,
        upload.bucket_id,
        MultipartMutationKind::CreateUpload,
        Some(upload.clone()),
        None,
    )
    .await
    .unwrap();

    assert!(
        get_active_multipart_upload(&storage, 1, 2, "audit-only", upload.upload_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        list_active_multipart_uploads(&storage, 2, "", "", None, 1000)
            .await
            .unwrap()
            .uploads
            .is_empty()
    );
    assert!(
        find_multipart_upload_partition(&storage, upload.id)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
pub(crate) async fn multipart_journal_with_permit_writes_fenced_payloads() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, 1, 2, "node-a").await;
    let permit = owner.write_permit().unwrap();

    let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &permit, KEY)
        .await
        .unwrap();
    upsert_multipart_part_with_permit(
        &storage,
        upload.upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
        &permit,
        KEY,
    )
    .await
    .unwrap();
    complete_multipart_upload_with_permit(&storage, upload.upload.id, &permit, KEY)
        .await
        .unwrap();

    let fences = read_multipart_frame_fences_for_test(&storage, 1, 2)
        .await
        .unwrap();
    assert_eq!(fences.len(), 3);
    assert!(fences.iter().all(|fence| *fence == permit.fence_token));
}

#[tokio::test]
pub(crate) async fn multipart_journal_with_permit_rejects_stale_fence() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, 1, 2, "node-a").await;
    let stale_permit = owner.write_permit().unwrap();
    let upload = create_multipart_upload_with_permit(&storage, 1, 2, "obj", &stale_permit, KEY)
        .await
        .unwrap();
    let newer = ready_owner(&storage, 1, 2, "node-b").await;
    assert!(newer.fence_token > stale_permit.fence_token);

    let err = upsert_multipart_part_with_permit(
        &storage,
        upload.upload.id,
        1,
        payload_ref("hash-a", 10),
        10,
        "etag-a",
        &stale_permit,
        KEY,
    )
    .await
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("write permit owner is not current")
    );
}

#[tokio::test]
pub(crate) async fn multipart_journal_batch_rejects_stale_partition_precondition() {
    let temp = tempdir().unwrap();
    let storage = Storage::new_at(temp.path()).await.unwrap();
    let owner = ready_owner(&storage, 1, 2, "node-a").await;
    let stale_permit = owner.write_permit().unwrap();
    let stale_precondition = partition_write_precondition(&storage, &stale_permit, KEY)
        .await
        .unwrap();
    let newer = ready_owner(&storage, 1, 2, "node-b").await;
    assert!(newer.fence_token > stale_permit.fence_token);

    let err = create_multipart_upload_inner(
        &storage,
        1,
        2,
        "obj",
        stale_permit.fence_token,
        Some(stale_precondition),
        None,
    )
    .await
    .unwrap_err();
    let message = err.to_string();
    assert!(
        message.contains("generation mismatch") || message.contains("target mismatch"),
        "unexpected stale precondition error: {message}"
    );

    create_multipart_upload_with_permit(&storage, 1, 2, "obj", &newer.write_permit().unwrap(), KEY)
        .await
        .unwrap();
}

async fn ready_owner(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    owner_node_id: &str,
) -> crate::partition_fence::PartitionOwnerState {
    let family = "multipart_metadata".to_string();
    let id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    crate::partition_fence::ready_partition_owner_for_test(
        storage,
        family,
        id,
        owner_node_id,
        0,
        hex::encode([0; 32]),
        hex::encode([1; 32]),
        KEY,
    )
    .await
}

async fn write_current_rows_for_test(
    storage: &Storage,
    upload: &MultipartUpload,
    parts: &[MultipartUploadPart],
) -> Result<()> {
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let transaction_id = format!("multipart-current-rows-test:{}", uuid::Uuid::new_v4());
    let upload_row = MultipartUploadCurrentRow {
        upload: upload.clone(),
        generation: 1,
        transaction_id: transaction_id.clone(),
        created_at_unix_nanos: current_unix_nanos()?,
    };
    meta.put(
        CF_OBJECT_HEADS,
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW,
        &multipart_upload_row_key(upload.tenant_id, upload.bucket_id, upload.id)?,
        &encode_upload_current_row(&upload_row)?,
    )?;
    for part in parts {
        let part_row = MultipartPartCurrentRow {
            tenant_id: upload.tenant_id,
            bucket_id: upload.bucket_id,
            part: part.clone(),
            generation: 1,
            transaction_id: transaction_id.clone(),
            created_at_unix_nanos: current_unix_nanos()?,
        };
        meta.put(
            CF_OBJECT_HEADS,
            TABLE_MULTIPART_PART_CURRENT_ROW,
            &multipart_part_row_key(
                upload.tenant_id,
                upload.bucket_id,
                part.upload_id,
                part.part_number,
            )?,
            &encode_part_current_row(&part_row)?,
        )?;
    }
    Ok(())
}

async fn append_history_only_for_test(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    event: MultipartMutationKind,
    upload: Option<MultipartUpload>,
    part: Option<MultipartUploadPart>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = multipart_metadata_stream_id(tenant_id, bucket_id);
    let mutation_id = uuid::Uuid::new_v4();
    let payload = encode_multipart_event(event, upload.as_ref(), part.as_ref(), 0, mutation_id)?;
    let partition_id = hex::encode(multipart_metadata_partition_id(tenant_id, bucket_id));
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!("multipart-history-only-test:{mutation_id}"),
            scope_partition: partition_id.clone(),
            committed_by_principal: multipart_metadata_partition_principal(tenant_id, bucket_id),
            preconditions: Vec::new(),
            operations: vec![CoreMutationOperation::StreamAppend {
                partition_id,
                stream_id,
                record_kind: "multipart_metadata".to_string(),
                payload,
                idempotency_key: Some(format!("multipart-history-only-test:{mutation_id}")),
            }],
        })
        .await?;
    Ok(())
}

fn test_upload(id: i64, tenant_id: i64, bucket_id: i64, key: &str) -> MultipartUpload {
    MultipartUpload {
        id,
        tenant_id,
        bucket_id,
        key: key.to_string(),
        upload_id: uuid::Uuid::new_v4(),
        created_at: Utc::now(),
        completed_at: None,
        aborted_at: None,
    }
}

fn test_part(
    upload_id: i64,
    part_number: i32,
    hash_label: &str,
    size: i64,
    etag: &str,
) -> MultipartUploadPart {
    MultipartUploadPart {
        id: i64::from(part_number),
        upload_id,
        part_number,
        content_hash: payload_ref(hash_label, size as u64).hash,
        object_ref: payload_ref(hash_label, size as u64),
        size,
        etag: etag.to_string(),
        created_at: Utc::now(),
    }
}

fn payload_ref(label: &str, logical_size: u64) -> CoreObjectRef {
    CoreObjectRef::test_unlocated(
        format!(
            "sha256:{}",
            hex::encode(blake3::hash(label.as_bytes()).as_bytes())
        ),
        logical_size,
        format!("manifest:{label}"),
    )
}
