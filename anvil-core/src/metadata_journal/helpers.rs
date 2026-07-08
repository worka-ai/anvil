use super::*;

pub(super) fn version_sorts_after_marker(
    order: usize,
    body: &ObjectVersionBody,
    marker_order: usize,
    marker_body: &ObjectVersionBody,
) -> Result<bool> {
    let created_at = parse_body_timestamp(&body.created_at)?;
    let marker_created_at = parse_body_timestamp(&marker_body.created_at)?;
    Ok(created_at < marker_created_at || (created_at == marker_created_at && order < marker_order))
}

pub(super) async fn write_segment_file(
    storage: &Storage,
    bucket: &Bucket,
    generation: u64,
    family: FileFamily,
    header: SegmentHeader,
    records: &[SegmentRecord],
) -> Result<WrittenSegment> {
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(family, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let body = SegmentBody::from_uncompressed_records(records)?.encode();
    let (first_record_hash, last_record_hash) = segment_record_hash_bounds(records);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        records.len() as u64,
        first_record_hash,
        last_record_hash,
    );
    let file_hash = hex::encode(footer.file_hash);
    let ref_name = metadata_segment_ref_name(bucket, generation, family, &file_hash)?;
    let mut bytes = Vec::with_capacity(encoded_header.len() + body.len() + COMMON_FOOTER_LEN);
    bytes.extend_from_slice(&encoded_header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "object_metadata".to_string(),
            generation,
            logical_file_id: ref_name.clone(),
            source: bytes,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "metadata-segment:{}:{}:{}",
                bucket.tenant_id, bucket.id, generation
            ),
            region_id: "local".to_string(),
        })
        .await?;
    let new_target = encode_core_object_ref_target(&object_ref)?;
    match store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: new_target.clone(),
            transaction_id: None,
        })
        .await
    {
        Ok(_) => {}
        Err(error) if error.to_string().contains("must be absent") => {
            let existing = store
                .read_ref(&ref_name)
                .await?
                .ok_or_else(|| anyhow!("metadata segment ref disappeared after CAS conflict"))?;
            if existing.target != new_target {
                return Err(error);
            }
        }
        Err(error) => return Err(error),
    }
    Ok(WrittenSegment {
        family,
        ref_name,
        record_count: records.len() as u64,
        file_hash,
    })
}

pub(super) async fn write_partition_manifest(
    storage: &Storage,
    bucket: &Bucket,
    generation: u64,
    frames: &[JournalFrame],
    segments: &[WrittenSegment],
    manifest_signing_key: &[u8],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<(PartitionManifest, String)> {
    if manifest_signing_key.is_empty() {
        return Err(anyhow!("partition manifest signing key must not be empty"));
    }
    let last_record_hash = frames
        .last()
        .map(|frame| hex::encode(frame.record_hash))
        .ok_or_else(|| anyhow!("partition manifest requires at least one journal frame"))?;
    let journal_ref = ManifestJournalRef {
        path: format!(
            "corestream:{}",
            object_metadata_stream_id(bucket.tenant_id, bucket.id)
        ),
        first_sequence: frames
            .first()
            .map(|frame| frame.partition_sequence)
            .unwrap_or(0),
        last_sequence: generation,
        last_record_hash: last_record_hash.clone(),
    };
    let segment_refs = segments
        .iter()
        .map(|segment| {
            Ok(ManifestSegmentRef {
                family: file_family_name(segment.family).to_string(),
                path: format!("{MANIFEST_SEGMENT_REF_PREFIX}{}", segment.ref_name),
                generation,
                record_count: segment.record_count,
                file_hash: segment.file_hash.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut manifest = PartitionManifest {
        format_version: 1,
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
        generation,
        fence_token,
        sealed_journals: vec![journal_ref],
        active_journal: None,
        segments: segment_refs,
        compacted_through_sequence: generation,
        last_record_hash,
        published_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        manifest_hash: None,
        manifest_signature: None,
    };
    let manifest_hash = compute_manifest_hash(&manifest)?;
    let manifest_signature = sign_manifest(&manifest_hash, &manifest, manifest_signing_key)?;
    manifest.manifest_hash = Some(manifest_hash.clone());
    manifest.manifest_signature = Some(manifest_signature);
    let encoded = serde_json::to_vec_pretty(&manifest)?;
    let manifest_ref = metadata_manifest_ref_name(bucket)?;
    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .write_logical_file_ref(WriteLogicalFileRequest {
            writer_family: "object_metadata".to_string(),
            generation,
            logical_file_id: manifest_ref.clone(),
            source: encoded,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!(
                "metadata-manifest:{}:{}:{}",
                bucket.tenant_id, bucket.id, generation
            ),
            region_id: "local".to_string(),
        })
        .await?;
    let new_target = encode_core_object_ref_target(&object_ref)?;
    if let Some(precondition) = partition_precondition {
        store
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id: format!(
                    "metadata-manifest:{}:{}:{}:{}",
                    bucket.tenant_id, bucket.id, generation, manifest_hash
                ),
                scope_partition: manifest.partition_id.clone(),
                committed_by_principal: object_metadata_partition_principal(bucket),
                preconditions: vec![precondition],
                operations: vec![CoreMutationOperation::RefUpdate {
                    partition_id: manifest.partition_id.clone(),
                    ref_name: manifest_ref.clone(),
                    new_target,
                }],
            })
            .await?;
    } else {
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: manifest_ref.clone(),
                expected_generation: None,
                expected_target: None,
                require_absent: false,
                require_present: false,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target,
                transaction_id: None,
            })
            .await?;
    }
    Ok((manifest, manifest_ref))
}

pub fn decode_partition_manifest(
    input: &[u8],
    manifest_signing_key: &[u8],
) -> Result<PartitionManifest> {
    let manifest: PartitionManifest = serde_json::from_slice(input)?;
    verify_partition_manifest(&manifest, manifest_signing_key)?;
    Ok(manifest)
}

pub(crate) async fn read_latest_partition_manifest(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Option<PartitionManifest>> {
    let store = CoreStore::new(storage.clone()).await?;
    let Some(ref_value) = store.read_ref(&metadata_manifest_ref_name(bucket)?).await? else {
        return Ok(None);
    };
    let bytes = store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await?;
    Ok(Some(decode_partition_manifest(
        &bytes,
        manifest_signing_key,
    )?))
}

pub(super) async fn partition_manifest_exists(storage: &Storage, bucket: &Bucket) -> Result<bool> {
    Ok(CoreStore::new(storage.clone())
        .await?
        .read_ref(&metadata_manifest_ref_name(bucket)?)
        .await?
        .is_some())
}

pub(super) async fn read_manifest_segment(
    storage: &Storage,
    segment: &ManifestSegmentRef,
) -> Result<Vec<u8>> {
    let ref_name = segment
        .path
        .strip_prefix(MANIFEST_SEGMENT_REF_PREFIX)
        .ok_or_else(|| anyhow!("partition segment manifest entry is not a CoreStore ref"))?;
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(ref_name)
        .await?
        .ok_or_else(|| anyhow!("partition segment ref is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await
}

#[cfg(test)]
pub(super) async fn read_core_ref_uri_payload(storage: &Storage, ref_uri: &str) -> Result<Vec<u8>> {
    let ref_name = ref_uri
        .strip_prefix(MANIFEST_SEGMENT_REF_PREFIX)
        .unwrap_or(ref_uri);
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(ref_name)
        .await?
        .ok_or_else(|| anyhow!("CoreStore ref is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await
}

pub fn verify_partition_manifest(
    manifest: &PartitionManifest,
    manifest_signing_key: &[u8],
) -> Result<()> {
    let expected_hash = compute_manifest_hash(manifest)?;
    if manifest.manifest_hash.as_deref() != Some(expected_hash.as_str()) {
        return Err(anyhow!("partition manifest hash mismatch"));
    }
    let expected_signature = sign_manifest(&expected_hash, manifest, manifest_signing_key)?;
    if manifest.manifest_signature.as_deref() != Some(expected_signature.as_str()) {
        return Err(anyhow!("partition manifest signature mismatch"));
    }
    Ok(())
}

pub(super) fn compute_manifest_hash(manifest: &PartitionManifest) -> Result<String> {
    let mut unsigned = manifest.clone();
    unsigned.manifest_hash = None;
    unsigned.manifest_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

pub(super) fn sign_manifest(
    manifest_hash: &str,
    manifest: &PartitionManifest,
    manifest_signing_key: &[u8],
) -> Result<String> {
    if manifest_signing_key.is_empty() {
        return Err(anyhow!("partition manifest signing key must not be empty"));
    }
    let mut mac = HmacSha256::new_from_slice(manifest_signing_key)?;
    mac.update(manifest_hash.as_bytes());
    mac.update(b"\0");
    mac.update(manifest.partition_id.as_bytes());
    mac.update(b"\0");
    mac.update(&manifest.generation.to_le_bytes());
    mac.update(&manifest.fence_token.to_le_bytes());
    Ok(base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes()))
}

pub(super) fn file_family_name(family: FileFamily) -> &'static str {
    match family {
        FileFamily::MetadataJournal => "metadata_journal",
        FileFamily::MetadataSegment => "metadata_segment",
        FileFamily::DirectorySegment => "directory_segment",
        FileFamily::FullTextSegment => "full_text_segment",
        FileFamily::VectorSegment => "vector_segment",
        FileFamily::AuthzTupleSegment => "authz_tuple_segment",
        FileFamily::WatchSegment => "watch_segment",
        FileFamily::PersonalDbLogSegment => "personaldb_log_segment",
        FileFamily::PersonalDbRowIndex => "personaldb_row_index",
        FileFamily::GitSourceIndex => "git_source_index",
        FileFamily::TypedFieldSegment => "typed_field_segment",
    }
}

pub(super) fn file_family_from_manifest_name(name: &str) -> Result<FileFamily> {
    match name {
        "metadata_segment" => Ok(FileFamily::MetadataSegment),
        "directory_segment" => Ok(FileFamily::DirectorySegment),
        other => Err(anyhow!(
            "unsupported segment family in partition manifest: {other}"
        )),
    }
}

pub(super) fn decode_segment_body_records(body: &SegmentBody) -> Result<Vec<SegmentRecord>> {
    let mut records = Vec::new();
    for block in &body.data_blocks {
        records.extend(block.decode_uncompressed_records()?);
    }
    Ok(records)
}

pub(super) fn object_from_body(body: &ObjectVersionBody) -> Result<Object> {
    Ok(Object {
        id: body.id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        key: body.object_key.clone(),
        kind: body.kind,
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        version_id: uuid::Uuid::parse_str(&body.version_id)?,
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        record_hash: body.record_hash.clone(),
        created_at: parse_body_timestamp(&body.created_at)?,
        deleted_at: body
            .deleted_at
            .as_deref()
            .map(parse_body_timestamp)
            .transpose()?,
        storage_class: body.storage_class,
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        checksum: body.checksum.clone(),
        link: body.link.clone(),
    })
}

pub(super) fn object_from_directory_body(body: &DirectoryEntryBody) -> Result<Object> {
    Ok(Object {
        id: body.id,
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        key: body.object_key.clone(),
        kind: body.kind,
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        version_id: uuid::Uuid::parse_str(&body.version_id)?,
        mutation_id: uuid::Uuid::parse_str(&body.mutation_id)?,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        record_hash: body.record_hash.clone(),
        created_at: parse_body_timestamp(&body.created_at)?,
        deleted_at: body
            .deleted_at
            .as_deref()
            .map(parse_body_timestamp)
            .transpose()?,
        storage_class: body.storage_class,
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        checksum: None,
        link: body.link.clone(),
    })
}

pub(super) fn parse_body_timestamp(value: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)?.with_timezone(&chrono::Utc))
}

pub(super) fn segment_header(
    bucket: &Bucket,
    generation: u64,
    partition_family: &'static str,
    key_order: &'static str,
) -> SegmentHeader {
    SegmentHeader {
        tenant_id: bucket.tenant_id.to_string(),
        bucket_id: bucket.id.to_string(),
        partition_family,
        partition_id: hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
        generation,
        key_order,
        compression: "none",
        block_size_uncompressed: 64 * 1024,
        bloom_bits_per_key: 0,
    }
}

pub(super) fn segment_record_hash_bounds(records: &[SegmentRecord]) -> (Hash32, Hash32) {
    let first = records
        .first()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    let last = records
        .last()
        .map(|record| hash32(&record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

pub(super) fn metadata_segment_key(body: &ObjectVersionBody) -> Vec<u8> {
    format!(
        "tenant/{}/bucket/{}/object/{}/version/{}",
        body.tenant_id, body.bucket_id, body.object_key, body.version_id
    )
    .into_bytes()
}

pub(super) fn directory_segment_key(body: &DirectoryEntryBody) -> Vec<u8> {
    format!(
        "tenant/{}/bucket/{}/directory/{}",
        body.tenant_id, body.bucket_id, body.object_key
    )
    .into_bytes()
}

pub(super) fn metadata_segment_ref_name(
    bucket: &Bucket,
    generation: u64,
    family: FileFamily,
    file_hash: &str,
) -> Result<String> {
    validate_hex32(file_hash, "metadata segment file hash")?;
    let prefix = match family {
        FileFamily::MetadataSegment => METADATA_SEGMENT_REF_PREFIX,
        FileFamily::DirectorySegment => DIRECTORY_SEGMENT_REF_PREFIX,
        _ => return Err(anyhow!("unsupported object metadata segment family")),
    };
    Ok(format!(
        "{prefix}tenant:{}:bucket:{}:generation:{generation:020}:hash:{file_hash}",
        bucket.tenant_id, bucket.id
    ))
}

pub(super) fn metadata_manifest_ref_name(bucket: &Bucket) -> Result<String> {
    Ok(format!(
        "{METADATA_MANIFEST_REF_PREFIX}tenant:{}:bucket:{}",
        bucket.tenant_id, bucket.id
    ))
}

pub(super) fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

pub(super) fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

pub(super) fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(encoded)?,
    )?)
}

pub async fn active_object_journal_stats(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<ActiveObjectJournalStats> {
    let mut compacted_through_sequence = 0;
    if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        compacted_through_sequence = manifest.compacted_through_sequence;
    }

    let frames = read_all_metadata_journal_frames(storage, bucket).await?;
    let mut stats = ActiveObjectJournalStats {
        last_sequence: frames
            .last()
            .map(|frame| frame.partition_sequence)
            .unwrap_or(0),
        compacted_through_sequence,
        ..ActiveObjectJournalStats::default()
    };
    for frame in frames {
        if frame.partition_sequence <= compacted_through_sequence {
            continue;
        }
        stats.uncompacted_frame_count = stats.uncompacted_frame_count.saturating_add(1);
        stats.uncompacted_encoded_bytes = stats
            .uncompacted_encoded_bytes
            .saturating_add(frame.encode().len() as u64);
    }
    Ok(stats)
}

pub async fn object_metadata_source_checkpoint_hash(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: u128,
) -> Result<String> {
    let max_sequence = u64::try_from(max_sequence)
        .map_err(|_| anyhow!("object metadata source cursor exceeds u64 sequence range"))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.object_metadata.source_checkpoint.v1");
    hasher.update(&bucket.tenant_id.to_le_bytes());
    hasher.update(&bucket.id.to_le_bytes());
    hasher.update(&max_sequence.to_le_bytes());

    let mut compacted_through_sequence = 0u64;
    if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        compacted_through_sequence = manifest.compacted_through_sequence;
        if compacted_through_sequence > max_sequence {
            return Err(anyhow!(
                "object metadata source cursor is older than manifest checkpoint"
            ));
        }
        hasher.update(manifest.manifest_hash.as_deref().unwrap_or("").as_bytes());
    } else {
        hasher.update(&[0; 32]);
    }

    for frame in read_all_metadata_journal_frames(storage, bucket).await? {
        if frame.partition_sequence <= compacted_through_sequence
            || frame.partition_sequence > max_sequence
        {
            continue;
        }
        hasher.update(&frame.partition_sequence.to_le_bytes());
        hasher.update(&frame.record_hash);
    }

    Ok(hasher.finalize().to_hex().to_string())
}

pub(super) async fn read_manifest_journal_ref_frames(
    storage: &Storage,
    journal_ref: &ManifestJournalRef,
) -> Result<Vec<JournalFrame>> {
    let stream_id = journal_ref
        .path
        .strip_prefix("corestream:")
        .ok_or_else(|| anyhow!("object metadata manifest journal ref must use corestream:"))?;
    let core_store = CoreStore::new(storage.clone()).await?;
    read_metadata_journal_frames_from_store(&core_store, stream_id).await
}

pub(super) async fn read_metadata_journal_frames_from_store(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<JournalFrame>> {
    let records = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.to_string(),
            after_sequence: 0,
            limit: 0,
        })
        .await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "object_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

pub(super) async fn read_raw_metadata_journal_frames_from_store(
    core_store: &CoreStore,
    stream_id: &str,
) -> Result<Vec<JournalFrame>> {
    let records = core_store.read_raw_stream(stream_id).await?;
    let mut frames = Vec::new();
    for record in records {
        if record.record_kind != "object_metadata" {
            continue;
        }
        frames.push(JournalFrame::decode(&record.payload)?);
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

pub(super) fn object_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

pub(super) fn object_metadata_partition_principal(bucket: &Bucket) -> String {
    format!(
        "partition-owner:object_metadata:{}:{}",
        bucket.tenant_id, bucket.id
    )
}

pub(super) fn current_object_ref_name(bucket: &Bucket, object_key: &str) -> String {
    let key_hash = hex::encode(hash32(object_key.as_bytes()));
    format!(
        "{CURRENT_OBJECT_REF_PREFIX}tenant:{}:bucket:{}:key:{key_hash}",
        bucket.tenant_id, bucket.id
    )
}

pub(super) fn current_object_ref_target(stream_id: &str, frame: &JournalFrame) -> String {
    format!(
        "corestream:{stream_id}:sequence:{}:hash:{}",
        frame.partition_sequence,
        hex::encode(frame.record_hash)
    )
}

pub(super) fn parse_current_object_ref_target(target: &str) -> Result<(String, u64, String)> {
    let rest = target
        .strip_prefix("corestream:")
        .ok_or_else(|| anyhow!("current object ref target must use corestream scheme"))?;
    let (stream_id, rest) = rest
        .split_once(":sequence:")
        .ok_or_else(|| anyhow!("current object ref target is missing sequence"))?;
    let (sequence, frame_hash) = rest
        .split_once(":hash:")
        .ok_or_else(|| anyhow!("current object ref target is missing hash"))?;
    validate_hex32(frame_hash, "current object ref frame hash")?;
    Ok((
        stream_id.to_string(),
        sequence.parse()?,
        frame_hash.to_string(),
    ))
}

#[cfg(test)]
pub(crate) async fn read_object_metadata_frame_fences_for_test(
    storage: &Storage,
    bucket: &Bucket,
) -> Result<Vec<u64>> {
    Ok(read_all_metadata_journal_frames(storage, bucket)
        .await?
        .into_iter()
        .map(|frame| frame.fence_token)
        .collect())
}

pub fn object_metadata_partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&tenant_id.to_le_bytes());
    bytes.extend_from_slice(&bucket_id.to_le_bytes());
    hash32(&bytes)
}

pub(super) fn require_object_metadata_permit(
    bucket: &Bucket,
    permit: &PartitionWritePermit,
) -> Result<()> {
    let expected_partition_id =
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    if permit.partition_family != "object_metadata" || permit.partition_id != expected_partition_id
    {
        return Err(anyhow!(
            "partition write permit does not target this object metadata partition"
        ));
    }
    Ok(())
}

pub(super) fn object_version_key_hash(bucket: &Bucket, object: &Object) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            bucket.tenant_id, bucket.id, object.key, object.version_id
        )
        .as_bytes(),
    )
}

pub(super) fn directory_key_hash(bucket: &Bucket, object: &Object) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/directory/{}",
            bucket.tenant_id, bucket.id, object.key
        )
        .as_bytes(),
    )
}
