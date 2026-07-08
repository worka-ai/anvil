use crate::core_store::{
    CompareAndSwapRef, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob, ReadStream,
    WriteLogicalFileRequest, is_stream_head_mismatch,
};
use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    FormatError, Hash32, JournalFrame, JournalRecordKind, hash32,
    segment::{SegmentBody, SegmentRecord},
    validate_journal_chain,
};
use crate::object_links;
use crate::partition_fence::{PartitionWritePermit, partition_write_ref_precondition};
use crate::persistence::{Bucket, Object, ObjectVersion, ObjectVersionsPage};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
#[cfg(test)]
use sha2::Digest;
use sha2::Sha256;

const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";
const MANIFEST_SEGMENT_REF_PREFIX: &str = "coreref:";
const METADATA_SEGMENT_REF_PREFIX: &str = "metadata_segment:";
const DIRECTORY_SEGMENT_REF_PREFIX: &str = "directory_segment:";
const METADATA_MANIFEST_REF_PREFIX: &str = "metadata_manifest:";
const CURRENT_OBJECT_REF_PREFIX: &str = "object_current:";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectJournalMutation {
    Put,
    DeleteMarker,
    DeleteVersion,
}

impl ObjectJournalMutation {
    fn event_name(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::DeleteMarker => "delete_marker",
            Self::DeleteVersion => "delete_version",
        }
    }

    fn object_record_kind(self) -> JournalRecordKind {
        match self {
            Self::Put | Self::DeleteVersion => JournalRecordKind::ObjectVersion,
            Self::DeleteMarker => JournalRecordKind::DeleteMarker,
        }
    }

    fn is_delete_marker(self) -> bool {
        matches!(self, Self::DeleteMarker)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ObjectVersionBody {
    #[serde(default)]
    id: i64,
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    object_key: String,
    event: String,
    #[serde(default)]
    kind: object_links::ObjectEntryKind,
    version_id: String,
    mutation_id: String,
    content_hash: String,
    size: i64,
    etag: String,
    content_type: Option<String>,
    user_metadata_hash: String,
    authz_revision: i64,
    index_policy_snapshot: String,
    record_hash: String,
    storage_class: Option<i16>,
    #[serde(default)]
    user_meta: Option<serde_json::Value>,
    #[serde(default)]
    shard_map: Option<serde_json::Value>,
    #[serde(default)]
    checksum: Option<Vec<u8>>,
    #[serde(default)]
    link: Option<object_links::ObjectLinkTarget>,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct DirectoryEntryBody {
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    object_key: String,
    event: String,
    #[serde(default)]
    kind: object_links::ObjectEntryKind,
    #[serde(default)]
    id: i64,
    version_id: String,
    mutation_id: String,
    #[serde(default)]
    content_hash: String,
    size: i64,
    etag: String,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    user_metadata_hash: String,
    #[serde(default)]
    authz_revision: i64,
    #[serde(default)]
    index_policy_snapshot: String,
    #[serde(default)]
    record_hash: String,
    #[serde(default)]
    storage_class: Option<i16>,
    #[serde(default)]
    user_meta: Option<serde_json::Value>,
    #[serde(default)]
    shard_map: Option<serde_json::Value>,
    #[serde(default)]
    link: Option<object_links::ObjectLinkTarget>,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

#[cfg(test)]
async fn append_object_mutation(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
) -> Result<()> {
    append_object_mutation_inner(storage, bucket, object, mutation, 0, None).await
}

pub(crate) async fn append_object_mutation_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<()> {
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    append_object_mutation_inner(
        storage,
        bucket,
        object,
        mutation,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn append_object_mutation_inner(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    const MAX_STREAM_HEAD_RETRIES: usize = 64;

    for attempt in 0..MAX_STREAM_HEAD_RETRIES {
        let result = append_object_mutation_inner_once(
            storage,
            bucket,
            object,
            mutation,
            fence_token,
            partition_precondition.clone(),
        )
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(error)
                if is_stream_head_mismatch(&error) && attempt + 1 < MAX_STREAM_HEAD_RETRIES =>
            {
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }

    unreachable!("metadata journal stream-head retry loop always returns")
}

async fn append_object_mutation_inner_once(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<()> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let raw_stream_head = core_store.raw_stream_head(&stream_id).await?;
    let frames = read_raw_metadata_journal_frames_from_store(&core_store, &stream_id)
        .await
        .unwrap_or_default();
    let previous_hash = frames
        .last()
        .map(|frame| frame.record_hash)
        .unwrap_or([0; 32]);
    let next_sequence = frames
        .last()
        .map(|frame| frame.partition_sequence + 1)
        .unwrap_or(1);

    let object_body = serde_json::to_vec(&ObjectVersionBody {
        id: object.id,
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        bucket_name: bucket.name.clone(),
        object_key: object.key.clone(),
        event: mutation.event_name().to_string(),
        kind: object.kind,
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        content_hash: object.content_hash.clone(),
        size: object.size,
        etag: object.etag.clone(),
        content_type: object.content_type.clone(),
        user_metadata_hash: object.user_metadata_hash.clone(),
        authz_revision: object.authz_revision,
        index_policy_snapshot: object.index_policy_snapshot.clone(),
        record_hash: object.record_hash.clone(),
        storage_class: object.storage_class,
        user_meta: object.user_meta.clone(),
        shard_map: object.shard_map.clone(),
        checksum: object.checksum.clone(),
        link: object.link.clone(),
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|ts| ts.to_rfc3339()),
    })?;
    let object_frame = JournalFrame::new(
        mutation.object_record_kind(),
        next_sequence,
        fence_token,
        *object.mutation_id.as_bytes(),
        object_version_key_hash(bucket, object),
        previous_hash,
        object_body,
    );

    let directory_body = serde_json::to_vec(&DirectoryEntryBody {
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        bucket_name: bucket.name.clone(),
        object_key: object.key.clone(),
        event: mutation.event_name().to_string(),
        kind: object.kind,
        id: object.id,
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        content_hash: object.content_hash.clone(),
        size: object.size,
        etag: object.etag.clone(),
        content_type: object.content_type.clone(),
        user_metadata_hash: object.user_metadata_hash.clone(),
        authz_revision: object.authz_revision,
        index_policy_snapshot: object.index_policy_snapshot.clone(),
        record_hash: object.record_hash.clone(),
        storage_class: object.storage_class,
        user_meta: object.user_meta.clone(),
        shard_map: object.shard_map.clone(),
        link: object.link.clone(),
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|ts| ts.to_rfc3339()),
    })?;
    let directory_frame = JournalFrame::new(
        JournalRecordKind::DirectoryEntry,
        next_sequence + 1,
        fence_token,
        *object.mutation_id.as_bytes(),
        directory_key_hash(bucket, object),
        object_frame.record_hash,
        directory_body,
    );

    let mut updated_frames = frames;
    updated_frames.push(object_frame.clone());
    updated_frames.push(directory_frame.clone());
    validate_journal_chain(&updated_frames)?;

    let partition_id = hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    let mut preconditions = partition_precondition.into_iter().collect::<Vec<_>>();
    preconditions.push(CoreMutationPrecondition::StreamHead {
        stream_id: stream_id.clone(),
        expected_last_sequence: raw_stream_head.0,
        expected_last_event_hash: raw_stream_head.1,
    });
    core_store
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id: format!(
                "object-metadata:{}:{}",
                object.mutation_id,
                mutation.event_name()
            ),
            scope_partition: partition_id.clone(),
            committed_by_principal: object_metadata_partition_principal(bucket),
            preconditions,
            operations: vec![
                CoreMutationOperation::StreamAppend {
                    partition_id: partition_id.clone(),
                    stream_id: stream_id.clone(),
                    record_kind: "object_metadata".to_string(),
                    payload: object_frame.encode(),
                    idempotency_key: Some(format!(
                        "object-metadata:{}:{}:object",
                        object.mutation_id,
                        mutation.event_name()
                    )),
                },
                CoreMutationOperation::StreamAppend {
                    partition_id: partition_id.clone(),
                    stream_id: stream_id.clone(),
                    record_kind: "object_metadata".to_string(),
                    payload: directory_frame.encode(),
                    idempotency_key: Some(format!(
                        "object-metadata:{}:{}:directory",
                        object.mutation_id,
                        mutation.event_name()
                    )),
                },
                CoreMutationOperation::RefUpdate {
                    partition_id,
                    ref_name: current_object_ref_name(bucket, &object.key),
                    new_target: current_object_ref_target(&stream_id, &directory_frame),
                },
            ],
        })
        .await?;
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedObjectMetadataSegments {
    pub generation: u64,
    pub metadata_ref: String,
    pub directory_ref: String,
    pub metadata_record_count: usize,
    pub directory_record_count: usize,
    pub manifest_ref: String,
    pub manifest_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredObjectMetadataPartition {
    pub manifest: PartitionManifest,
    pub metadata_records: Vec<SegmentRecord>,
    pub directory_records: Vec<SegmentRecord>,
}

#[derive(Debug, Clone)]
pub struct NativeObjectListing {
    pub objects: Vec<Object>,
    pub common_prefixes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryIndexSnapshot {
    pub entry_count: usize,
    pub snapshot_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryIndexComparison {
    pub source_cursor: u128,
    pub expected: DirectoryIndexSnapshot,
    pub actual: DirectoryIndexSnapshot,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActiveObjectJournalStats {
    pub uncompacted_frame_count: u64,
    pub uncompacted_encoded_bytes: u64,
    pub last_sequence: u64,
    pub compacted_through_sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionManifest {
    pub format_version: u16,
    pub partition_family: String,
    pub partition_id: String,
    pub generation: u64,
    pub fence_token: u64,
    pub sealed_journals: Vec<ManifestJournalRef>,
    pub active_journal: Option<ManifestJournalRef>,
    pub segments: Vec<ManifestSegmentRef>,
    pub compacted_through_sequence: u64,
    pub last_record_hash: String,
    pub published_at: String,
    pub manifest_hash: Option<String>,
    pub manifest_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestJournalRef {
    pub path: String,
    pub first_sequence: u64,
    pub last_sequence: u64,
    pub last_record_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestSegmentRef {
    pub family: String,
    pub path: String,
    pub generation: u64,
    pub record_count: u64,
    pub file_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct WrittenSegment {
    family: FileFamily,
    ref_name: String,
    record_count: u64,
    file_hash: String,
}

#[derive(Debug, Serialize)]
struct SegmentHeader {
    tenant_id: String,
    bucket_id: String,
    partition_family: &'static str,
    partition_id: String,
    generation: u64,
    key_order: &'static str,
    compression: &'static str,
    block_size_uncompressed: u32,
    bloom_bits_per_key: u8,
}

#[cfg(test)]
async fn seal_object_journal_segments(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    seal_object_journal_segments_inner(storage, bucket, manifest_signing_key, 0, None).await
}

pub(crate) async fn seal_object_journal_segments_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    seal_object_journal_segments_inner(
        storage,
        bucket,
        manifest_signing_key,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await
}

async fn seal_object_journal_segments_inner(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    fence_token: u64,
    partition_precondition: Option<CoreMutationPrecondition>,
) -> Result<SealedObjectMetadataSegments> {
    let frames = read_all_metadata_journal_frames(storage, bucket).await?;
    let generation = frames
        .last()
        .map(|frame| frame.partition_sequence)
        .ok_or_else(|| anyhow!("metadata journal has no frames to seal"))?;

    let mut metadata_records = Vec::new();
    let mut directory_latest = std::collections::BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for frame in &frames {
        match frame.record_kind {
            JournalRecordKind::ObjectVersion | JournalRecordKind::DeleteMarker => {
                let body: ObjectVersionBody = serde_json::from_slice(&frame.body)?;
                metadata_records.push(SegmentRecord::new(
                    metadata_segment_key(&body),
                    frame.body.clone(),
                ));
            }
            JournalRecordKind::DirectoryEntry => {
                let body: DirectoryEntryBody = serde_json::from_slice(&frame.body)?;
                directory_latest.insert(directory_segment_key(&body), frame.body.clone());
            }
            _ => {}
        }
    }
    metadata_records.sort_by(|left, right| left.key.cmp(&right.key));
    let directory_records = directory_latest
        .into_iter()
        .map(|(key, value)| SegmentRecord::new(key, value))
        .collect::<Vec<_>>();

    let metadata_segment = write_segment_file(
        storage,
        bucket,
        generation,
        FileFamily::MetadataSegment,
        segment_header(
            bucket,
            generation,
            "object_metadata",
            "tenant_bucket_key_version",
        ),
        &metadata_records,
    )
    .await?;
    let directory_segment = write_segment_file(
        storage,
        bucket,
        generation,
        FileFamily::DirectorySegment,
        segment_header(bucket, generation, "directory", "tenant_bucket_prefix_key"),
        &directory_records,
    )
    .await?;
    let (manifest, manifest_ref) = write_partition_manifest(
        storage,
        bucket,
        generation,
        &frames,
        &[metadata_segment, directory_segment],
        manifest_signing_key,
        fence_token,
        partition_precondition,
    )
    .await?;

    Ok(SealedObjectMetadataSegments {
        generation,
        metadata_ref: manifest.segments[0].path.clone(),
        directory_ref: manifest.segments[1].path.clone(),
        metadata_record_count: metadata_records.len(),
        directory_record_count: directory_records.len(),
        manifest_ref,
        manifest_hash: manifest
            .manifest_hash
            .clone()
            .ok_or_else(|| anyhow!("partition manifest hash was not set"))?,
    })
}

pub fn decode_segment_file(input: &[u8], expected_family: FileFamily) -> Result<SegmentBody> {
    let (body, _) = decode_segment_file_with_footer(input, expected_family)?;
    Ok(body)
}

fn decode_segment_file_with_footer(
    input: &[u8],
    expected_family: FileFamily,
) -> Result<(SegmentBody, BinaryFileFooter)> {
    let header = BinaryEnvelopeHeader::decode(input)?;
    if header.family != expected_family {
        return Err(anyhow!("segment file family mismatch"));
    }
    if input.len() < COMMON_FOOTER_LEN {
        return Err(FormatError::TooShort {
            context: "segment file footer",
            needed: COMMON_FOOTER_LEN,
            actual: input.len(),
        }
        .into());
    }
    let header_len = COMMON_HEADER_LEN
        .checked_add(header.header_json.len())
        .ok_or_else(|| anyhow!("segment header length overflow"))?;
    let footer_start = input
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("segment footer offset underflow"))?;
    let body = &input[header_len..footer_start];
    let footer = BinaryFileFooter::decode(&input[footer_start..])?;
    footer.verify(&input[..header_len], body)?;
    let body = SegmentBody::decode(body)?;
    Ok((body, footer))
}

pub async fn recover_object_metadata_partition(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<RecoveredObjectMetadataPartition> {
    let manifest = read_latest_partition_manifest(storage, bucket, manifest_signing_key)
        .await?
        .ok_or_else(|| anyhow!("object metadata partition manifest is missing"))?;
    let expected_partition_id =
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    if manifest.partition_family != "object_metadata" {
        return Err(anyhow!("partition manifest family mismatch"));
    }
    if manifest.partition_id != expected_partition_id {
        return Err(anyhow!("partition manifest id mismatch"));
    }

    let mut metadata_records = Vec::new();
    let mut directory_latest = std::collections::BTreeMap::<Vec<u8>, SegmentRecord>::new();
    for segment in &manifest.segments {
        let family = file_family_from_manifest_name(&segment.family)?;
        let bytes = read_manifest_segment(storage, segment).await?;
        let (body, footer) = decode_segment_file_with_footer(&bytes, family)?;
        if hex::encode(footer.file_hash) != segment.file_hash {
            return Err(anyhow!("partition segment file hash mismatch"));
        }
        if footer.record_count != segment.record_count {
            return Err(anyhow!("partition segment record count mismatch"));
        }

        let mut records = decode_segment_body_records(&body)?;
        match family {
            FileFamily::MetadataSegment => metadata_records.append(&mut records),
            FileFamily::DirectorySegment => {
                for record in records {
                    directory_latest.insert(record.key.clone(), record);
                }
            }
            _ => {
                return Err(anyhow!(
                    "unexpected segment family in object metadata manifest"
                ));
            }
        }
    }

    if let Some(active_journal) = &manifest.active_journal {
        let frames = read_manifest_journal_ref_frames(storage, active_journal).await?;
        let first = frames
            .first()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty journal"))?;
        let last = frames
            .last()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty journal"))?;
        if first.partition_sequence != active_journal.first_sequence
            || last.partition_sequence != active_journal.last_sequence
            || hex::encode(last.record_hash) != active_journal.last_record_hash
        {
            return Err(anyhow!("active journal manifest reference mismatch"));
        }
        for frame in frames {
            match frame.record_kind {
                JournalRecordKind::ObjectVersion | JournalRecordKind::DeleteMarker => {
                    let body: ObjectVersionBody = serde_json::from_slice(&frame.body)?;
                    metadata_records.push(SegmentRecord::new(
                        metadata_segment_key(&body),
                        frame.body.clone(),
                    ));
                }
                JournalRecordKind::DirectoryEntry => {
                    let body: DirectoryEntryBody = serde_json::from_slice(&frame.body)?;
                    let record = SegmentRecord::new(directory_segment_key(&body), frame.body);
                    directory_latest.insert(record.key.clone(), record);
                }
                _ => {}
            }
        }
    }

    metadata_records.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(RecoveredObjectMetadataPartition {
        manifest,
        metadata_records,
        directory_records: directory_latest.into_values().collect(),
    })
}

async fn recover_object_directory_partition(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<(
    PartitionManifest,
    std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>,
)> {
    let manifest = read_latest_partition_manifest(storage, bucket, manifest_signing_key)
        .await?
        .ok_or_else(|| anyhow!("object metadata partition manifest is missing"))?;
    let expected_partition_id =
        hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
    if manifest.partition_family != "object_metadata" {
        return Err(anyhow!("partition manifest family mismatch"));
    }
    if manifest.partition_id != expected_partition_id {
        return Err(anyhow!("partition manifest id mismatch"));
    }

    let mut directory_latest = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    for segment in &manifest.segments {
        let family = file_family_from_manifest_name(&segment.family)?;
        if family != FileFamily::DirectorySegment {
            continue;
        }
        let bytes = read_manifest_segment(storage, segment).await?;
        let (body, footer) = decode_segment_file_with_footer(&bytes, FileFamily::DirectorySegment)?;
        if hex::encode(footer.file_hash) != segment.file_hash {
            return Err(anyhow!("directory segment file hash mismatch"));
        }
        if footer.record_count != segment.record_count {
            return Err(anyhow!("directory segment record count mismatch"));
        }
        for record in decode_segment_body_records(&body)? {
            let entry: DirectoryEntryBody = serde_json::from_slice(&record.value)?;
            directory_latest.insert(record.key, entry);
        }
    }

    if let Some(active_journal) = &manifest.active_journal {
        let frames = read_manifest_journal_ref_frames(storage, active_journal).await?;
        let first = frames
            .first()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty journal"))?;
        let last = frames
            .last()
            .ok_or_else(|| anyhow!("active journal manifest entry points at an empty journal"))?;
        if first.partition_sequence != active_journal.first_sequence
            || last.partition_sequence != active_journal.last_sequence
            || hex::encode(last.record_hash) != active_journal.last_record_hash
        {
            return Err(anyhow!("active journal manifest reference mismatch"));
        }
        for frame in frames {
            if frame.record_kind == JournalRecordKind::DirectoryEntry {
                let body: DirectoryEntryBody = serde_json::from_slice(&frame.body)?;
                directory_latest.insert(directory_segment_key(&body), body);
            }
        }
    }

    Ok((manifest, directory_latest))
}

pub async fn next_object_id(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<i64> {
    let max_id = read_object_version_bodies(storage, bucket, manifest_signing_key)
        .await?
        .into_iter()
        .map(|(_, body)| body.id)
        .max()
        .unwrap_or(0);
    max_id
        .checked_add(1)
        .ok_or_else(|| anyhow!("object id overflow"))
}

pub async fn read_current_object(
    storage: &Storage,
    bucket: &Bucket,
    _manifest_signing_key: &[u8],
    object_key: &str,
) -> Result<Option<Object>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    let Some(current_ref) = core_store
        .read_ref(&current_object_ref_name(bucket, object_key))
        .await?
    else {
        return Ok(None);
    };
    let (stream_id, sequence, expected_frame_hash) =
        parse_current_object_ref_target(&current_ref.target)?;
    let Some(record) = core_store
        .read_stream(ReadStream {
            stream_id: stream_id.clone(),
            after_sequence: sequence.saturating_sub(1),
            limit: 1,
        })
        .await?
        .into_iter()
        .next()
    else {
        return Err(anyhow!(
            "current object ref points at missing metadata stream record"
        ));
    };
    if record.stream_id != stream_id || record.sequence != sequence {
        return Err(anyhow!("current object ref stream cursor mismatch"));
    }
    let frame = JournalFrame::decode(&record.payload)?;
    if frame.record_kind != JournalRecordKind::DirectoryEntry {
        return Err(anyhow!(
            "current object ref target is not a directory metadata frame"
        ));
    }
    if hex::encode(frame.record_hash) != expected_frame_hash {
        return Err(anyhow!("current object ref target frame hash mismatch"));
    }
    let body: DirectoryEntryBody = serde_json::from_slice(&frame.body)?;
    if body.tenant_id != bucket.tenant_id
        || body.bucket_id != bucket.id
        || body.bucket_name != bucket.name
        || body.object_key != object_key
    {
        return Err(anyhow!("current object ref target scope mismatch"));
    }
    if body.delete_marker || body.deleted_at.is_some() {
        return Ok(None);
    }
    Ok(Some(object_from_directory_body(&body)?))
}

pub async fn read_object_version(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    object_key: &str,
    version_id: uuid::Uuid,
) -> Result<Option<Object>> {
    let body_records = read_object_version_bodies(storage, bucket, manifest_signing_key).await?;
    let mut version_records = body_records
        .into_iter()
        .filter(|(_, body)| {
            body.object_key == object_key && body.version_id == version_id.to_string()
        })
        .collect::<Vec<_>>();
    sort_versions_for_key(&mut version_records);

    let mut selected = None;
    for (_, body) in version_records {
        if body.event == "delete_version" {
            selected = None;
        } else {
            selected = Some(body);
        }
    }

    selected.as_ref().map(object_from_body).transpose()
}

pub async fn read_object_version_by_id(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    version_id: uuid::Uuid,
) -> Result<Option<Object>> {
    let body_records = read_object_version_bodies(storage, bucket, manifest_signing_key).await?;
    Ok(body_records
        .into_iter()
        .find(|(_, body)| body.version_id == version_id.to_string())
        .map(|(_, body)| object_from_body(&body))
        .transpose()?)
}

pub async fn list_current_objects(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    prefix: &str,
    start_after: &str,
    limit: i32,
    delimiter: &str,
) -> Result<NativeObjectListing> {
    let mut objects = read_current_directory_objects(storage, bucket, manifest_signing_key).await?;
    objects.retain(|object| {
        object.key.starts_with(prefix)
            && object.key.as_str() > start_after
            && !crate::validation::is_reserved_internal_key(&object.key)
    });
    objects.sort_by(|left, right| left.key.cmp(&right.key));

    let limit = limit.max(1) as usize;
    if delimiter.is_empty() {
        objects.truncate(limit);
        return Ok(NativeObjectListing {
            objects,
            common_prefixes: Vec::new(),
        });
    }

    enum ListingEntry {
        Object(Object),
        CommonPrefix(String),
    }

    let mut merged = std::collections::BTreeMap::<String, ListingEntry>::new();
    for object in objects {
        let suffix = &object.key[prefix.len()..];
        if let Some(position) = suffix.find(delimiter) {
            let common_prefix = format!("{}{}", prefix, &suffix[..position + delimiter.len()]);
            merged
                .entry(common_prefix.clone())
                .or_insert(ListingEntry::CommonPrefix(common_prefix));
        } else {
            merged.insert(object.key.clone(), ListingEntry::Object(object));
        }
        if merged.len() >= limit {
            break;
        }
    }

    let mut listing = NativeObjectListing {
        objects: Vec::new(),
        common_prefixes: Vec::new(),
    };
    for (_, entry) in merged.into_iter().take(limit) {
        match entry {
            ListingEntry::Object(object) => listing.objects.push(object),
            ListingEntry::CommonPrefix(common_prefix) => {
                listing.common_prefixes.push(common_prefix)
            }
        }
    }
    Ok(listing)
}

pub(crate) async fn read_current_directory_objects(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<Object>> {
    let mut directory_records = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    let mut compacted_through_sequence = 0u64;

    if partition_manifest_exists(storage, bucket).await? {
        let (manifest, recovered_directory) =
            recover_object_directory_partition(storage, bucket, manifest_signing_key)
                .await
                .context("recover object directory partition from CoreStore manifest")?;
        compacted_through_sequence = manifest.compacted_through_sequence;
        directory_records.extend(recovered_directory);
    }

    for frame in read_all_metadata_journal_frames(storage, bucket).await? {
        if frame.partition_sequence <= compacted_through_sequence {
            continue;
        }
        if frame.record_kind == JournalRecordKind::DirectoryEntry {
            let body: DirectoryEntryBody = serde_json::from_slice(&frame.body)?;
            directory_records.insert(directory_segment_key(&body), body);
        }
    }

    let mut current = Vec::new();
    for body in directory_records.into_values() {
        if body.delete_marker || body.deleted_at.is_some() {
            continue;
        }
        current.push(object_from_directory_body(&body)?);
    }
    current.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(current)
}

pub async fn read_current_objects(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<Object>> {
    let body_records = read_object_version_bodies(storage, bucket, manifest_signing_key).await?;
    current_objects_from_version_bodies(body_records)
}

pub async fn read_current_objects_through_sequence(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: u128,
) -> Result<Vec<Object>> {
    if max_sequence == 0 {
        return Ok(Vec::new());
    }
    let body_records = read_object_version_bodies_through_sequence(
        storage,
        bucket,
        manifest_signing_key,
        max_sequence,
    )
    .await?;
    current_objects_from_version_bodies(body_records)
}

pub async fn compare_directory_index_to_metadata(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<DirectoryIndexComparison> {
    let stats = active_object_journal_stats(storage, bucket, manifest_signing_key).await?;
    let source_cursor = u128::from(stats.last_sequence.max(stats.compacted_through_sequence));
    Ok(DirectoryIndexComparison {
        source_cursor,
        expected: expected_directory_index_snapshot_from_metadata(
            storage,
            bucket,
            manifest_signing_key,
        )
        .await?,
        actual: current_directory_index_snapshot_from_index(storage, bucket, manifest_signing_key)
            .await?,
    })
}

pub async fn expected_directory_index_snapshot_from_metadata(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<DirectoryIndexSnapshot> {
    let expected_entries =
        expected_directory_entries_from_metadata(storage, bucket, manifest_signing_key).await?;
    directory_index_snapshot(&expected_entries)
}

pub async fn current_directory_index_snapshot_from_index(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<DirectoryIndexSnapshot> {
    let actual_entries =
        current_directory_entries_from_index(storage, bucket, manifest_signing_key).await?;
    directory_index_snapshot(&actual_entries)
}

pub async fn rebuild_directory_index_from_metadata_with_permit(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    permit: &PartitionWritePermit,
    partition_owner_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    require_object_metadata_permit(bucket, permit)?;
    let partition_precondition =
        partition_write_ref_precondition(storage, permit, partition_owner_signing_key).await?;
    let body_records =
        read_object_version_bodies_from_metadata_only(storage, bucket, manifest_signing_key)
            .await?;
    let frames = read_all_metadata_journal_frames(storage, bucket).await?;
    let generation = frames
        .last()
        .map(|frame| frame.partition_sequence)
        .ok_or_else(|| anyhow!("metadata journal has no frames to rebuild directory index"))?;

    let mut metadata_records = body_records
        .iter()
        .map(|(_, body)| {
            Ok(SegmentRecord::new(
                metadata_segment_key(body),
                serde_json::to_vec(body)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    metadata_records.sort_by(|left, right| left.key.cmp(&right.key));

    let directory_entries = directory_entries_from_object_version_bodies(body_records)?;
    let directory_records = directory_entries
        .into_iter()
        .map(|(key, body)| Ok(SegmentRecord::new(key, serde_json::to_vec(&body)?)))
        .collect::<Result<Vec<_>>>()?;

    let metadata_segment = write_segment_file(
        storage,
        bucket,
        generation,
        FileFamily::MetadataSegment,
        segment_header(
            bucket,
            generation,
            "object_metadata",
            "tenant_bucket_key_version",
        ),
        &metadata_records,
    )
    .await?;
    let directory_segment = write_segment_file(
        storage,
        bucket,
        generation,
        FileFamily::DirectorySegment,
        segment_header(bucket, generation, "directory", "tenant_bucket_prefix_key"),
        &directory_records,
    )
    .await?;
    let (manifest, manifest_ref) = write_partition_manifest(
        storage,
        bucket,
        generation,
        &frames,
        &[metadata_segment, directory_segment],
        manifest_signing_key,
        permit.fence_token,
        Some(partition_precondition),
    )
    .await?;

    Ok(SealedObjectMetadataSegments {
        generation,
        metadata_ref: manifest.segments[0].path.clone(),
        directory_ref: manifest.segments[1].path.clone(),
        metadata_record_count: metadata_records.len(),
        directory_record_count: directory_records.len(),
        manifest_ref,
        manifest_hash: manifest
            .manifest_hash
            .clone()
            .ok_or_else(|| anyhow!("partition manifest hash was not set"))?,
    })
}

fn current_objects_from_version_bodies(
    body_records: Vec<(usize, ObjectVersionBody)>,
) -> Result<Vec<Object>> {
    let mut versions_by_key = object_versions_by_key(body_records);

    let mut current = Vec::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key(versions);
        if let Some((_, body)) = versions.last() {
            if !body.delete_marker && body.deleted_at.is_none() {
                current.push(object_from_body(body)?);
            }
        }
    }
    current.sort_by(|left, right| left.key.cmp(&right.key));
    Ok(current)
}

pub async fn read_object_versions(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    prefix: &str,
    key_marker: &str,
    version_id_marker: Option<uuid::Uuid>,
    limit: i32,
) -> Result<ObjectVersionsPage> {
    let body_records = read_object_version_bodies(storage, bucket, manifest_signing_key).await?;
    let mut versions_by_key = object_versions_by_key(body_records);
    let marker = if let Some(version_id_marker) = version_id_marker {
        let marker = versions_by_key
            .get(key_marker)
            .and_then(|versions| {
                versions
                    .iter()
                    .find(|(_, body)| body.version_id == version_id_marker.to_string())
            })
            .cloned();
        let Some(marker) = marker else {
            return Ok(ObjectVersionsPage {
                versions: Vec::new(),
                is_truncated: false,
                next_key_marker: None,
                next_version_id_marker: None,
            });
        };
        Some(marker)
    } else {
        None
    };

    let mut flattened = Vec::<(usize, ObjectVersionBody, bool)>::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key_descending(versions);
        for (index, (order, body)) in versions.iter().enumerate() {
            flattened.push((*order, body.clone(), index == 0));
        }
    }
    flattened.sort_by(|(left_order, left, _), (right_order, right, _)| {
        left.object_key
            .cmp(&right.object_key)
            .then_with(|| {
                parse_body_timestamp(&right.created_at)
                    .ok()
                    .cmp(&parse_body_timestamp(&left.created_at).ok())
            })
            .then_with(|| right_order.cmp(left_order))
    });

    let mut selected = Vec::new();
    for (order, body, is_latest) in flattened {
        if !body.object_key.starts_with(prefix)
            || crate::validation::is_reserved_internal_key(&body.object_key)
        {
            continue;
        }
        if let Some((marker_order, marker_body)) = marker.as_ref() {
            if body.object_key.as_str() < key_marker {
                continue;
            }
            if body.object_key == key_marker
                && !version_sorts_after_marker(order, &body, *marker_order, marker_body)?
            {
                continue;
            }
        } else if body.object_key.as_str() <= key_marker {
            continue;
        }

        selected.push(ObjectVersion {
            is_delete_marker: body.delete_marker || body.deleted_at.is_some(),
            is_latest,
            object: object_from_body(&body)?,
        });
    }

    let limit = limit.max(1) as usize;
    let is_truncated = selected.len() > limit;
    if is_truncated {
        selected.truncate(limit);
    }
    let (next_key_marker, next_version_id_marker) = if is_truncated {
        selected
            .last()
            .map(|version| {
                (
                    Some(version.object.key.clone()),
                    Some(version.object.version_id),
                )
            })
            .unwrap_or((None, None))
    } else {
        (None, None)
    };
    Ok(ObjectVersionsPage {
        versions: selected,
        is_truncated,
        next_key_marker,
        next_version_id_marker,
    })
}

async fn read_object_version_bodies(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    read_object_version_bodies_inner(storage, bucket, manifest_signing_key, None).await
}

async fn read_object_version_bodies_through_sequence(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: u128,
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    let max_sequence = u64::try_from(max_sequence)
        .map_err(|_| anyhow!("object metadata source cursor exceeds u64 sequence range"))?;
    read_object_version_bodies_inner(storage, bucket, manifest_signing_key, Some(max_sequence))
        .await
}

async fn read_object_version_bodies_inner(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
    max_sequence: Option<u64>,
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    let mut body_records = Vec::<(usize, ObjectVersionBody)>::new();
    let mut order = 0usize;
    let mut compacted_through_sequence = 0u64;

    if partition_manifest_exists(storage, bucket).await? {
        let recovered = recover_object_metadata_partition(storage, bucket, manifest_signing_key)
            .await
            .context("recover object metadata partition from CoreStore manifest")?;
        compacted_through_sequence = recovered.manifest.compacted_through_sequence;
        if let Some(max_sequence) = max_sequence {
            if compacted_through_sequence > max_sequence {
                return Err(anyhow!(
                    "object metadata source cursor is older than manifest checkpoint"
                ));
            }
        }
        for record in recovered.metadata_records {
            let body: ObjectVersionBody = serde_json::from_slice(&record.value)?;
            body_records.push((order, body));
            order += 1;
        }
    }

    for frame in read_all_metadata_journal_frames(storage, bucket).await? {
        if frame.partition_sequence <= compacted_through_sequence {
            continue;
        }
        if max_sequence.is_some_and(|max_sequence| frame.partition_sequence > max_sequence) {
            continue;
        }
        if matches!(
            frame.record_kind,
            JournalRecordKind::ObjectVersion | JournalRecordKind::DeleteMarker
        ) {
            let body: ObjectVersionBody = serde_json::from_slice(&frame.body)?;
            body_records.push((order, body));
            order += 1;
        }
    }
    Ok(body_records)
}

async fn read_object_version_bodies_from_metadata_only(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<Vec<(usize, ObjectVersionBody)>> {
    let mut body_records = Vec::<(usize, ObjectVersionBody)>::new();
    let mut order = 0usize;
    let mut compacted_through_sequence = 0u64;

    if let Some(manifest) =
        read_latest_partition_manifest(storage, bucket, manifest_signing_key).await?
    {
        let expected_partition_id =
            hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id));
        if manifest.partition_family != "object_metadata" {
            return Err(anyhow!("partition manifest family mismatch"));
        }
        if manifest.partition_id != expected_partition_id {
            return Err(anyhow!("partition manifest id mismatch"));
        }
        compacted_through_sequence = manifest.compacted_through_sequence;
        for segment in &manifest.segments {
            let family = file_family_from_manifest_name(&segment.family)?;
            if family != FileFamily::MetadataSegment {
                continue;
            }
            let bytes = read_manifest_segment(storage, segment).await?;
            let (body, footer) =
                decode_segment_file_with_footer(&bytes, FileFamily::MetadataSegment)?;
            if hex::encode(footer.file_hash) != segment.file_hash {
                return Err(anyhow!("metadata segment file hash mismatch"));
            }
            if footer.record_count != segment.record_count {
                return Err(anyhow!("metadata segment record count mismatch"));
            }
            for record in decode_segment_body_records(&body)? {
                let body: ObjectVersionBody = serde_json::from_slice(&record.value)?;
                body_records.push((order, body));
                order += 1;
            }
        }
    }

    for frame in read_all_metadata_journal_frames(storage, bucket).await? {
        if frame.partition_sequence <= compacted_through_sequence {
            continue;
        }
        if matches!(
            frame.record_kind,
            JournalRecordKind::ObjectVersion | JournalRecordKind::DeleteMarker
        ) {
            let body: ObjectVersionBody = serde_json::from_slice(&frame.body)?;
            body_records.push((order, body));
            order += 1;
        }
    }
    Ok(body_records)
}

async fn current_directory_entries_from_index(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    let mut directory_records = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    let mut compacted_through_sequence = 0u64;

    if partition_manifest_exists(storage, bucket).await? {
        let (manifest, recovered_directory) =
            recover_object_directory_partition(storage, bucket, manifest_signing_key)
                .await
                .context("recover object directory partition from CoreStore manifest")?;
        compacted_through_sequence = manifest.compacted_through_sequence;
        directory_records.extend(recovered_directory);
    }

    for frame in read_all_metadata_journal_frames(storage, bucket).await? {
        if frame.partition_sequence <= compacted_through_sequence {
            continue;
        }
        if frame.record_kind == JournalRecordKind::DirectoryEntry {
            let body: DirectoryEntryBody = serde_json::from_slice(&frame.body)?;
            directory_records.insert(directory_segment_key(&body), body);
        }
    }
    Ok(directory_records)
}

async fn expected_directory_entries_from_metadata(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    directory_entries_from_object_version_bodies(
        read_object_version_bodies_from_metadata_only(storage, bucket, manifest_signing_key)
            .await?,
    )
}

fn directory_entries_from_object_version_bodies(
    body_records: Vec<(usize, ObjectVersionBody)>,
) -> Result<std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>> {
    let mut versions_by_key = object_versions_by_key(body_records);
    let mut entries = std::collections::BTreeMap::<Vec<u8>, DirectoryEntryBody>::new();
    for versions in versions_by_key.values_mut() {
        sort_versions_for_key(versions);
        if let Some((_, body)) = versions.last() {
            let directory = directory_entry_from_object_version_body(body);
            entries.insert(directory_segment_key(&directory), directory);
        }
    }
    Ok(entries)
}

fn directory_entry_from_object_version_body(body: &ObjectVersionBody) -> DirectoryEntryBody {
    DirectoryEntryBody {
        tenant_id: body.tenant_id,
        bucket_id: body.bucket_id,
        bucket_name: body.bucket_name.clone(),
        object_key: body.object_key.clone(),
        event: body.event.clone(),
        kind: body.kind,
        id: body.id,
        version_id: body.version_id.clone(),
        mutation_id: body.mutation_id.clone(),
        content_hash: body.content_hash.clone(),
        size: body.size,
        etag: body.etag.clone(),
        content_type: body.content_type.clone(),
        user_metadata_hash: body.user_metadata_hash.clone(),
        authz_revision: body.authz_revision,
        index_policy_snapshot: body.index_policy_snapshot.clone(),
        record_hash: body.record_hash.clone(),
        storage_class: body.storage_class,
        user_meta: body.user_meta.clone(),
        shard_map: body.shard_map.clone(),
        link: body.link.clone(),
        delete_marker: body.delete_marker,
        created_at: body.created_at.clone(),
        deleted_at: body.deleted_at.clone(),
    }
}

fn directory_index_snapshot(
    entries: &std::collections::BTreeMap<Vec<u8>, DirectoryEntryBody>,
) -> Result<DirectoryIndexSnapshot> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.directory_index.snapshot.v1");
    for (key, body) in entries {
        hasher.update(&(key.len() as u64).to_le_bytes());
        hasher.update(key);
        let body = serde_json::to_vec(body)?;
        hasher.update(&(body.len() as u64).to_le_bytes());
        hasher.update(&body);
    }
    Ok(DirectoryIndexSnapshot {
        entry_count: entries.len(),
        snapshot_hash: hasher.finalize().to_hex().to_string(),
    })
}

async fn read_all_metadata_journal_frames(
    storage: &Storage,
    bucket: &Bucket,
) -> Result<Vec<JournalFrame>> {
    let core_store = CoreStore::new(storage.clone()).await?;
    read_metadata_journal_frames_from_store(
        &core_store,
        &object_metadata_stream_id(bucket.tenant_id, bucket.id),
    )
    .await
}

fn object_versions_by_key(
    body_records: Vec<(usize, ObjectVersionBody)>,
) -> std::collections::BTreeMap<String, Vec<(usize, ObjectVersionBody)>> {
    let mut versions_by_key =
        std::collections::BTreeMap::<String, Vec<(usize, ObjectVersionBody)>>::new();
    for (order, body) in body_records {
        let versions = versions_by_key.entry(body.object_key.clone()).or_default();
        if body.event == "delete_version" {
            versions.retain(|(_, existing)| existing.version_id != body.version_id);
        } else {
            versions.push((order, body));
        }
    }
    versions_by_key
}

fn sort_versions_for_key(versions: &mut [(usize, ObjectVersionBody)]) {
    versions.sort_by(|(left_order, left), (right_order, right)| {
        parse_body_timestamp(&left.created_at)
            .ok()
            .cmp(&parse_body_timestamp(&right.created_at).ok())
            .then_with(|| left_order.cmp(right_order))
    });
}

fn sort_versions_for_key_descending(versions: &mut [(usize, ObjectVersionBody)]) {
    versions.sort_by(|(left_order, left), (right_order, right)| {
        parse_body_timestamp(&right.created_at)
            .ok()
            .cmp(&parse_body_timestamp(&left.created_at).ok())
            .then_with(|| right_order.cmp(left_order))
    });
}

mod helpers;
pub use helpers::*;

#[cfg(test)]
mod tests;
