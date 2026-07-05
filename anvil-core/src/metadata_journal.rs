use crate::core_store::{
    CompareAndSwapRef, CoreMutationBatch, CoreMutationOperation, CoreMutationPrecondition,
    CoreObjectRef, CoreStore, GetBlob, PutBlob, ReadStream,
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
    let core_store = CoreStore::new(storage.clone()).await?;
    let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
    let frames = read_metadata_journal_frames_from_store(&core_store, &stream_id)
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
    let preconditions = partition_precondition.into_iter().collect();
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

fn version_sorts_after_marker(
    order: usize,
    body: &ObjectVersionBody,
    marker_order: usize,
    marker_body: &ObjectVersionBody,
) -> Result<bool> {
    let created_at = parse_body_timestamp(&body.created_at)?;
    let marker_created_at = parse_body_timestamp(&marker_body.created_at)?;
    Ok(created_at < marker_created_at || (created_at == marker_created_at && order < marker_order))
}

async fn write_segment_file(
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
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes,
            region_id: "local".to_string(),
            mutation_id: format!(
                "metadata-segment:{}:{}:{}",
                bucket.tenant_id, bucket.id, generation
            ),
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

async fn write_partition_manifest(
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
        .put_blob(PutBlob {
            logical_name: manifest_ref.clone(),
            bytes: encoded,
            region_id: "local".to_string(),
            mutation_id: format!(
                "metadata-manifest:{}:{}:{}",
                bucket.tenant_id, bucket.id, generation
            ),
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

async fn partition_manifest_exists(storage: &Storage, bucket: &Bucket) -> Result<bool> {
    Ok(CoreStore::new(storage.clone())
        .await?
        .read_ref(&metadata_manifest_ref_name(bucket)?)
        .await?
        .is_some())
}

async fn read_manifest_segment(storage: &Storage, segment: &ManifestSegmentRef) -> Result<Vec<u8>> {
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
async fn read_core_ref_uri_payload(storage: &Storage, ref_uri: &str) -> Result<Vec<u8>> {
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

fn compute_manifest_hash(manifest: &PartitionManifest) -> Result<String> {
    let mut unsigned = manifest.clone();
    unsigned.manifest_hash = None;
    unsigned.manifest_signature = None;
    Ok(hex::encode(hash32(&serde_json::to_vec(&unsigned)?)))
}

fn sign_manifest(
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

fn file_family_name(family: FileFamily) -> &'static str {
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

fn file_family_from_manifest_name(name: &str) -> Result<FileFamily> {
    match name {
        "metadata_segment" => Ok(FileFamily::MetadataSegment),
        "directory_segment" => Ok(FileFamily::DirectorySegment),
        other => Err(anyhow!(
            "unsupported segment family in partition manifest: {other}"
        )),
    }
}

fn decode_segment_body_records(body: &SegmentBody) -> Result<Vec<SegmentRecord>> {
    let mut records = Vec::new();
    for block in &body.data_blocks {
        records.extend(block.decode_uncompressed_records()?);
    }
    Ok(records)
}

fn object_from_body(body: &ObjectVersionBody) -> Result<Object> {
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

fn object_from_directory_body(body: &DirectoryEntryBody) -> Result<Object> {
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

fn parse_body_timestamp(value: &str) -> Result<chrono::DateTime<chrono::Utc>> {
    Ok(chrono::DateTime::parse_from_rfc3339(value)?.with_timezone(&chrono::Utc))
}

fn segment_header(
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

fn segment_record_hash_bounds(records: &[SegmentRecord]) -> (Hash32, Hash32) {
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

fn metadata_segment_key(body: &ObjectVersionBody) -> Vec<u8> {
    format!(
        "tenant/{}/bucket/{}/object/{}/version/{}",
        body.tenant_id, body.bucket_id, body.object_key, body.version_id
    )
    .into_bytes()
}

fn directory_segment_key(body: &DirectoryEntryBody) -> Vec<u8> {
    format!(
        "tenant/{}/bucket/{}/directory/{}",
        body.tenant_id, body.bucket_id, body.object_key
    )
    .into_bytes()
}

fn metadata_segment_ref_name(
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

fn metadata_manifest_ref_name(bucket: &Bucket) -> Result<String> {
    Ok(format!(
        "{METADATA_MANIFEST_REF_PREFIX}tenant:{}:bucket:{}",
        bucket.tenant_id, bucket.id
    ))
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
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

async fn read_manifest_journal_ref_frames(
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

async fn read_metadata_journal_frames_from_store(
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

fn object_metadata_stream_id(tenant_id: i64, bucket_id: i64) -> String {
    format!("object_metadata:tenant:{tenant_id}:bucket:{bucket_id}")
}

fn object_metadata_partition_principal(bucket: &Bucket) -> String {
    format!(
        "partition-owner:object_metadata:{}:{}",
        bucket.tenant_id, bucket.id
    )
}

fn current_object_ref_name(bucket: &Bucket, object_key: &str) -> String {
    let key_hash = hex::encode(hash32(object_key.as_bytes()));
    format!(
        "{CURRENT_OBJECT_REF_PREFIX}tenant:{}:bucket:{}:key:{key_hash}",
        bucket.tenant_id, bucket.id
    )
}

fn current_object_ref_target(stream_id: &str, frame: &JournalFrame) -> String {
    format!(
        "corestream:{stream_id}:sequence:{}:hash:{}",
        frame.partition_sequence,
        hex::encode(frame.record_hash)
    )
}

fn parse_current_object_ref_target(target: &str) -> Result<(String, u64, String)> {
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

fn require_object_metadata_permit(bucket: &Bucket, permit: &PartitionWritePermit) -> Result<()> {
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

fn object_version_key_hash(bucket: &Bucket, object: &Object) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/object/{}/version/{}",
            bucket.tenant_id, bucket.id, object.key, object.version_id
        )
        .as_bytes(),
    )
}

fn directory_key_hash(bucket: &Bucket, object: &Object) -> Hash32 {
    hash32(
        format!(
            "tenant/{}/bucket/{}/directory/{}",
            bucket.tenant_id, bucket.id, object.key
        )
        .as_bytes(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
    };
    use chrono::Utc;
    use tempfile::tempdir;

    const PARTITION_OWNER_KEY: &[u8] = b"object metadata partition owner signing key";

    fn sample_bucket() -> Bucket {
        Bucket {
            id: 7,
            tenant_id: 3,
            name: "journal-bucket".to_string(),
            region: "test-region".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        }
    }

    fn sample_object(id: i64, key: &str, delete_marker: bool) -> Object {
        Object {
            id,
            tenant_id: 3,
            bucket_id: 7,
            key: key.to_string(),
            kind: object_links::ObjectEntryKind::Blob,
            content_hash: format!("hash-{id}"),
            size: 42,
            etag: format!("etag-{id}"),
            content_type: Some("text/plain".to_string()),
            version_id: uuid::Uuid::new_v4(),
            mutation_id: uuid::Uuid::new_v4(),
            index_policy_snapshot: "snapshot".to_string(),
            user_metadata_hash: "metadata-hash".to_string(),
            authz_revision: 11,
            record_hash: format!("record-{id}"),
            created_at: Utc::now(),
            deleted_at: delete_marker.then(Utc::now),
            storage_class: None,
            user_meta: None,
            shard_map: None,
            checksum: None,
            link: None,
        }
    }

    async fn ready_object_metadata_permit(
        storage: &Storage,
        bucket: &Bucket,
        owner_node_id: &str,
    ) -> PartitionWritePermit {
        let request = PartitionRecoveryAcquire {
            partition_family: "object_metadata".to_string(),
            partition_id: hex::encode(object_metadata_partition_id(bucket.tenant_id, bucket.id)),
            owner_node_id: owner_node_id.to_string(),
            recovered_through_sequence: 0,
            recovered_manifest_hash: hex::encode([0; 32]),
            now_nanos: 100,
        };
        let recovering = acquire_partition_recovery(storage, request, PARTITION_OWNER_KEY)
            .await
            .unwrap();
        publish_partition_ready(
            storage,
            &recovering.partition_family,
            &recovering.partition_id,
            owner_node_id,
            recovering.fence_token,
            0,
            &hex::encode([1; 32]),
            200,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap()
        .write_permit()
        .unwrap()
    }

    #[tokio::test]
    async fn append_object_mutation_writes_chained_metadata_and_directory_frames() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt", false);
        let second = sample_object(2, "docs/b.txt", true);

        append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(
            &storage,
            &bucket,
            &second,
            ObjectJournalMutation::DeleteMarker,
        )
        .await
        .unwrap();

        let frames = read_all_metadata_journal_frames(&storage, &bucket)
            .await
            .unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].record_kind, JournalRecordKind::ObjectVersion);
        assert_eq!(frames[1].record_kind, JournalRecordKind::DirectoryEntry);
        assert_eq!(frames[2].record_kind, JournalRecordKind::DeleteMarker);
        assert_eq!(frames[3].record_kind, JournalRecordKind::DirectoryEntry);
        assert_eq!(frames[1].previous_record_hash, frames[0].record_hash);
        assert_eq!(frames[2].previous_record_hash, frames[1].record_hash);
        validate_journal_chain(&frames).unwrap();

        let current = read_current_objects(&storage, &bucket, b"unused without manifest")
            .await
            .unwrap();
        assert_eq!(current.len(), 1);
        assert_eq!(current[0].key, first.key);
        assert_eq!(current[0].content_hash, first.content_hash);

        let current_through_directory_frame =
            read_current_objects_through_sequence(&storage, &bucket, b"unused without manifest", 2)
                .await
                .unwrap();
        assert_eq!(current_through_directory_frame.len(), 1);
        assert_eq!(
            current_through_directory_frame[0].content_hash,
            first.content_hash
        );
    }

    #[tokio::test]
    async fn object_metadata_write_permit_sets_frame_and_manifest_fence() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
        let object = sample_object(1, "docs/fenced.txt", false);

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &object,
            ObjectJournalMutation::Put,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        let frames = read_all_metadata_journal_frames(&storage, &bucket)
            .await
            .unwrap();
        assert_eq!(frames.len(), 2);
        assert!(
            frames
                .iter()
                .all(|frame| frame.fence_token == permit.fence_token)
        );

        let manifest_key = b"manifest signing key";
        let sealed = seal_object_journal_segments_with_permit(
            &storage,
            &bucket,
            manifest_key,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        assert_eq!(sealed.generation, 2);
        let manifest = read_latest_partition_manifest(&storage, &bucket, manifest_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manifest.fence_token, permit.fence_token);
    }

    #[tokio::test]
    async fn object_metadata_write_rejects_stale_partition_permit() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let stale_permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
        let fresh_permit = ready_object_metadata_permit(&storage, &bucket, "node-b").await;
        assert_eq!(fresh_permit.fence_token, stale_permit.fence_token + 1);

        let rejected = append_object_mutation_with_permit(
            &storage,
            &bucket,
            &sample_object(1, "docs/stale.txt", false),
            ObjectJournalMutation::Put,
            &stale_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap_err();
        assert!(rejected.to_string().contains("PartitionNotOwned"));

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &sample_object(2, "docs/fresh.txt", false),
            ObjectJournalMutation::Put,
            &fresh_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn object_metadata_corestore_batch_rejects_stale_partition_precondition() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let stale_permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
        let stale_precondition = crate::partition_fence::partition_write_ref_precondition(
            &storage,
            &stale_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        let fresh_permit = ready_object_metadata_permit(&storage, &bucket, "node-b").await;
        assert_eq!(fresh_permit.fence_token, stale_permit.fence_token + 1);

        let rejected = append_object_mutation_inner(
            &storage,
            &bucket,
            &sample_object(1, "docs/stale-precondition.txt", false),
            ObjectJournalMutation::Put,
            stale_permit.fence_token,
            Some(stale_precondition),
        )
        .await
        .unwrap_err();
        assert!(
            rejected.to_string().contains("target mismatch")
                || rejected.to_string().contains("generation mismatch"),
            "unexpected error: {rejected:?}"
        );

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &sample_object(2, "docs/fresh-precondition.txt", false),
            ObjectJournalMutation::Put,
            &fresh_permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn object_metadata_mutation_updates_current_object_coreref_in_same_batch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
        let key = "docs/current-ref.txt";
        let first = sample_object(1, key, false);

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &first,
            ObjectJournalMutation::Put,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let ref_name = current_object_ref_name(&bucket, key);
        let first_ref = store
            .read_ref(&ref_name)
            .await
            .unwrap()
            .expect("current object ref is published");
        assert_eq!(first_ref.generation, 1);
        assert!(first_ref.target.contains("sequence:2"));

        let second = sample_object(2, key, true);
        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &second,
            ObjectJournalMutation::DeleteMarker,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        let second_ref = store
            .read_ref(&ref_name)
            .await
            .unwrap()
            .expect("current object ref is updated");
        assert_eq!(second_ref.generation, 2);
        assert_ne!(second_ref.target, first_ref.target);
        assert!(second_ref.target.contains("sequence:4"));
    }

    #[tokio::test]
    async fn read_current_object_uses_current_object_coreref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
        let key = "docs/ref-driven-read.txt";
        let first = sample_object(1, key, false);
        let second = sample_object(2, key, false);

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &first,
            ObjectJournalMutation::Put,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let ref_name = current_object_ref_name(&bucket, key);
        let first_ref = store.read_ref(&ref_name).await.unwrap().unwrap();

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &second,
            ObjectJournalMutation::Put,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        assert_eq!(
            read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
                .await
                .unwrap()
                .unwrap()
                .content_hash,
            second.content_hash
        );

        let latest_ref = store.read_ref(&ref_name).await.unwrap().unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name,
                expected_generation: Some(latest_ref.generation),
                expected_target: Some(latest_ref.target),
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: first_ref.target,
                transaction_id: None,
            })
            .await
            .unwrap();

        assert_eq!(
            read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
                .await
                .unwrap()
                .unwrap()
                .content_hash,
            first.content_hash
        );
    }

    #[tokio::test]
    async fn read_current_object_returns_none_for_current_delete_marker_ref() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let permit = ready_object_metadata_permit(&storage, &bucket, "node-a").await;
        let key = "docs/deleted-current.txt";

        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &sample_object(1, key, false),
            ObjectJournalMutation::Put,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();
        append_object_mutation_with_permit(
            &storage,
            &bucket,
            &sample_object(2, key, true),
            ObjectJournalMutation::DeleteMarker,
            &permit,
            PARTITION_OWNER_KEY,
        )
        .await
        .unwrap();

        assert!(
            read_current_object(&storage, &bucket, PARTITION_OWNER_KEY, key)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn read_object_version_returns_exact_version_and_delete_marker() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let object = sample_object(1, "docs/a.txt", false);
        let delete_marker = sample_object(2, "docs/a.txt", true);

        append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(
            &storage,
            &bucket,
            &delete_marker,
            ObjectJournalMutation::DeleteMarker,
        )
        .await
        .unwrap();

        let read = read_object_version(
            &storage,
            &bucket,
            b"unused without manifest",
            &object.key,
            object.version_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read.version_id, object.version_id);
        assert!(read.deleted_at.is_none());

        let read_marker = read_object_version(
            &storage,
            &bucket,
            b"unused without manifest",
            &delete_marker.key,
            delete_marker.version_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(read_marker.version_id, delete_marker.version_id);
        assert!(read_marker.deleted_at.is_some());
    }

    #[tokio::test]
    async fn read_object_version_hides_explicitly_deleted_version_after_seal() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let object = sample_object(1, "docs/a.txt", false);

        append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
            .await
            .unwrap();
        let signing_key = b"manifest signing key";
        seal_object_journal_segments(&storage, &bucket, signing_key)
            .await
            .unwrap();

        let before_delete = read_object_version(
            &storage,
            &bucket,
            signing_key,
            &object.key,
            object.version_id,
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(before_delete.version_id, object.version_id);

        append_object_mutation(
            &storage,
            &bucket,
            &object,
            ObjectJournalMutation::DeleteVersion,
        )
        .await
        .unwrap();

        let after_delete = read_object_version(
            &storage,
            &bucket,
            signing_key,
            &object.key,
            object.version_id,
        )
        .await
        .unwrap();
        assert!(after_delete.is_none());
    }

    #[tokio::test]
    async fn seal_object_journal_segments_writes_metadata_and_directory_segments() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt", false);
        let second = sample_object(2, "docs/a.txt", false);
        let third = sample_object(3, "docs/b.txt", false);

        append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(&storage, &bucket, &second, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(&storage, &bucket, &third, ObjectJournalMutation::Put)
            .await
            .unwrap();

        let signing_key = b"manifest signing key";
        let sealed = seal_object_journal_segments(&storage, &bucket, signing_key)
            .await
            .unwrap();
        assert_eq!(sealed.generation, 6);
        assert_eq!(sealed.metadata_record_count, 3);
        assert_eq!(sealed.directory_record_count, 2);
        assert_eq!(
            sealed.manifest_ref,
            metadata_manifest_ref_name(&bucket).unwrap()
        );

        let manifest = read_latest_partition_manifest(&storage, &bucket, signing_key)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manifest.generation, sealed.generation);
        assert_eq!(
            manifest.manifest_hash.as_deref(),
            Some(sealed.manifest_hash.as_str())
        );
        assert_eq!(manifest.sealed_journals.len(), 1);
        assert_eq!(manifest.segments.len(), 2);
        assert_eq!(manifest.segments[0].family, "metadata_segment");
        assert_eq!(manifest.segments[1].family, "directory_segment");
        assert!(manifest.active_journal.is_none());

        let mut tampered_manifest = manifest.clone();
        tampered_manifest.generation += 1;
        assert!(verify_partition_manifest(&tampered_manifest, signing_key).is_err());

        let recovered = recover_object_metadata_partition(&storage, &bucket, signing_key)
            .await
            .unwrap();
        assert_eq!(recovered.manifest.generation, sealed.generation);
        assert_eq!(recovered.metadata_records.len(), 3);
        assert_eq!(recovered.directory_records.len(), 2);
        assert!(
            read_core_ref_uri_payload(&storage, "../escape.anseg")
                .await
                .is_err()
        );
        let current = read_current_objects(&storage, &bucket, signing_key)
            .await
            .unwrap();
        assert_eq!(current.len(), 2);
        assert_eq!(current[0].key, second.key);
        assert_eq!(current[0].version_id, second.version_id);
        let listed = list_current_objects(&storage, &bucket, signing_key, "docs/", "", 10, "/")
            .await
            .unwrap();
        assert_eq!(listed.objects.len(), 2);
        assert!(listed.common_prefixes.is_empty());
        let versions = read_object_versions(&storage, &bucket, signing_key, "docs/", "", None, 10)
            .await
            .unwrap();
        assert_eq!(versions.versions.len(), 3);
        assert_eq!(versions.versions[0].object.version_id, second.version_id);
        assert!(versions.versions[0].is_latest);
        assert_eq!(versions.versions[1].object.version_id, first.version_id);
        assert!(!versions.versions[1].is_latest);
        let next_versions = read_object_versions(
            &storage,
            &bucket,
            signing_key,
            "docs/",
            "docs/a.txt",
            Some(second.version_id),
            10,
        )
        .await
        .unwrap();
        assert_eq!(
            next_versions.versions[0].object.version_id,
            first.version_id
        );

        let metadata_bytes = read_core_ref_uri_payload(&storage, &sealed.metadata_ref)
            .await
            .unwrap();
        let metadata_body =
            decode_segment_file(&metadata_bytes, FileFamily::MetadataSegment).unwrap();
        let metadata_records = metadata_body.data_blocks[0]
            .decode_uncompressed_records()
            .unwrap();
        assert_eq!(metadata_records.len(), 3);
        assert!(
            metadata_records
                .windows(2)
                .all(|pair| pair[0].key <= pair[1].key)
        );

        let directory_bytes = read_core_ref_uri_payload(&storage, &sealed.directory_ref)
            .await
            .unwrap();
        let directory_body =
            decode_segment_file(&directory_bytes, FileFamily::DirectorySegment).unwrap();
        let directory_records = directory_body.data_blocks[0]
            .decode_uncompressed_records()
            .unwrap();
        assert_eq!(directory_records.len(), 2);
        let latest_a: DirectoryEntryBody =
            serde_json::from_slice(&directory_records[0].value).unwrap();
        assert_eq!(latest_a.version_id, second.version_id.to_string());

        let mut corrupted_metadata = read_core_ref_uri_payload(&storage, &sealed.metadata_ref)
            .await
            .unwrap();
        let body_byte = corrupted_metadata.len() - COMMON_FOOTER_LEN - 1;
        corrupted_metadata[body_byte] ^= 1;
        let ref_name = sealed
            .metadata_ref
            .strip_prefix(MANIFEST_SEGMENT_REF_PREFIX)
            .unwrap();
        let store = CoreStore::new(storage.clone()).await.unwrap();
        let object_ref = store
            .put_blob(PutBlob {
                logical_name: ref_name.to_string(),
                bytes: corrupted_metadata,
                region_id: "local".to_string(),
                mutation_id: "corrupt-metadata-segment-test".to_string(),
            })
            .await
            .unwrap();
        store
            .compare_and_swap_ref(CompareAndSwapRef {
                ref_name: ref_name.to_string(),
                expected_generation: None,
                expected_target: None,
                require_absent: false,
                require_present: true,
                fence: None,
                authz_revision: None,
                source_watch_cursor: None,
                new_target: encode_core_object_ref_target(&object_ref).unwrap(),
                transaction_id: None,
            })
            .await
            .unwrap();
        assert!(
            recover_object_metadata_partition(&storage, &bucket, signing_key)
                .await
                .is_err()
        );
        assert!(
            read_current_objects(&storage, &bucket, signing_key)
                .await
                .is_err()
        );
        let directory_listing =
            list_current_objects(&storage, &bucket, signing_key, "docs/", "", 10, "/")
                .await
                .unwrap();
        assert_eq!(
            directory_listing
                .objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/a.txt", "docs/b.txt"]
        );
        assert_eq!(directory_listing.objects[0].version_id, second.version_id);
    }

    #[tokio::test]
    async fn prefix_list_uses_directory_segment_plus_active_directory_journal() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt", false);
        let second = sample_object(2, "docs/b.txt", false);
        let nested = sample_object(3, "docs/nested/c.txt", false);

        append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(&storage, &bucket, &second, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(&storage, &bucket, &nested, ObjectJournalMutation::Put)
            .await
            .unwrap();

        let signing_key = b"manifest signing key";
        seal_object_journal_segments(&storage, &bucket, signing_key)
            .await
            .unwrap();

        let replacement = sample_object(4, "docs/a.txt", false);
        let delete_nested = sample_object(5, "docs/nested/c.txt", true);
        append_object_mutation(&storage, &bucket, &replacement, ObjectJournalMutation::Put)
            .await
            .unwrap();
        append_object_mutation(
            &storage,
            &bucket,
            &delete_nested,
            ObjectJournalMutation::DeleteMarker,
        )
        .await
        .unwrap();

        let listing = list_current_objects(&storage, &bucket, signing_key, "docs/", "", 10, "/")
            .await
            .unwrap();
        assert_eq!(
            listing
                .objects
                .iter()
                .map(|object| object.key.as_str())
                .collect::<Vec<_>>(),
            vec!["docs/a.txt", "docs/b.txt"]
        );
        assert_eq!(listing.objects[0].version_id, replacement.version_id);
        assert_eq!(listing.objects[0].content_hash, replacement.content_hash);
        assert!(listing.common_prefixes.is_empty());

        let nested_listing =
            list_current_objects(&storage, &bucket, signing_key, "docs/nested/", "", 10, "/")
                .await
                .unwrap();
        assert!(nested_listing.objects.is_empty());
    }

    #[tokio::test]
    async fn object_metadata_stream_rejects_corrupted_appended_frame() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let object = sample_object(1, "docs/a.txt", false);
        append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
            .await
            .unwrap();

        let stream_id = object_metadata_stream_id(bucket.tenant_id, bucket.id);
        let mut hasher = Sha256::new();
        hasher.update(stream_id.as_bytes());
        let file_name = format!("{}.jsonl", hex::encode(hasher.finalize()));
        for index in 1..=3 {
            let stream_path = storage
                .core_store_replica_path(&format!("local-control-node-{index}"))
                .join("streams")
                .join("data")
                .join(&file_name);
            let mut bytes = tokio::fs::read(&stream_path).await.unwrap();
            let last = bytes.len() - 33;
            bytes[last] ^= 1;
            tokio::fs::write(&stream_path, bytes).await.unwrap();
        }
        assert!(
            read_all_metadata_journal_frames(&storage, &bucket)
                .await
                .is_err()
        );
    }
}
