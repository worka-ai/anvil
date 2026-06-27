use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    FormatError, Hash32, JournalFrame, JournalRecordKind, hash32,
    segment::{SegmentBody, SegmentRecord},
    validate_journal_chain,
};
use crate::persistence::{Bucket, Object};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

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

#[derive(Debug, Serialize)]
struct MetadataJournalHeader<'a> {
    tenant_id: String,
    bucket_id: String,
    partition_family: &'static str,
    partition_id: String,
    fence_token: u64,
    first_sequence: u64,
    created_at: &'a str,
    codec: &'static str,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ObjectVersionBody {
    tenant_id: i64,
    bucket_id: i64,
    bucket_name: String,
    object_key: String,
    event: String,
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
    version_id: String,
    mutation_id: String,
    size: i64,
    etag: String,
    delete_marker: bool,
    created_at: String,
    deleted_at: Option<String>,
}

pub async fn append_object_mutation(
    storage: &Storage,
    bucket: &Bucket,
    object: &Object,
    mutation: ObjectJournalMutation,
) -> Result<PathBuf> {
    let path = storage.metadata_journal_path(bucket.tenant_id, bucket.id);
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    ensure_journal_header(&path, bucket).await?;
    let existing = tokio::fs::read(&path).await?;
    let (header_len, frames) = decode_journal_file(&existing)?;
    let previous_hash = frames
        .last()
        .map(|frame| frame.record_hash)
        .unwrap_or([0; 32]);
    let next_sequence = frames
        .last()
        .map(|frame| frame.partition_sequence + 1)
        .unwrap_or(1);

    let object_body = serde_json::to_vec(&ObjectVersionBody {
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        bucket_name: bucket.name.clone(),
        object_key: object.key.clone(),
        event: mutation.event_name().to_string(),
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
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|ts| ts.to_rfc3339()),
    })?;
    let object_frame = JournalFrame::new(
        mutation.object_record_kind(),
        next_sequence,
        0,
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
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        size: object.size,
        etag: object.etag.clone(),
        delete_marker: mutation.is_delete_marker(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object.deleted_at.map(|ts| ts.to_rfc3339()),
    })?;
    let directory_frame = JournalFrame::new(
        JournalRecordKind::DirectoryEntry,
        next_sequence + 1,
        0,
        *object.mutation_id.as_bytes(),
        directory_key_hash(bucket, object),
        object_frame.record_hash,
        directory_body,
    );

    let mut updated_frames = frames;
    updated_frames.push(object_frame.clone());
    updated_frames.push(directory_frame.clone());
    validate_journal_chain(&updated_frames)?;

    let mut file = tokio::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .await?;
    file.write_all(&object_frame.encode()).await?;
    file.write_all(&directory_frame.encode()).await?;
    file.sync_data().await?;

    debug_assert!(header_len <= existing.len());
    Ok(path)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedObjectMetadataSegments {
    pub generation: u64,
    pub metadata_path: PathBuf,
    pub directory_path: PathBuf,
    pub metadata_record_count: usize,
    pub directory_record_count: usize,
    pub manifest_path: PathBuf,
    pub manifest_hash: String,
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
    path: PathBuf,
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

pub async fn seal_object_journal_segments(
    storage: &Storage,
    bucket: &Bucket,
    manifest_signing_key: &[u8],
) -> Result<SealedObjectMetadataSegments> {
    let journal_path = storage.metadata_journal_path(bucket.tenant_id, bucket.id);
    let journal_bytes = tokio::fs::read(&journal_path)
        .await
        .with_context(|| format!("read metadata journal {}", journal_path.display()))?;
    let (_, frames) = decode_journal_file(&journal_bytes)?;
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

    let metadata_path = storage.metadata_segment_path(bucket.tenant_id, bucket.id, generation);
    let directory_path = storage.directory_segment_path(bucket.tenant_id, bucket.id, generation);

    let metadata_segment = write_segment_file(
        &metadata_path,
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
        &directory_path,
        FileFamily::DirectorySegment,
        segment_header(bucket, generation, "directory", "tenant_bucket_prefix_key"),
        &directory_records,
    )
    .await?;
    let manifest_path = storage.metadata_manifest_path(bucket.tenant_id, bucket.id);
    let manifest = write_partition_manifest(
        storage,
        bucket,
        generation,
        &frames,
        &[metadata_segment, directory_segment],
        manifest_signing_key,
        &manifest_path,
    )
    .await?;

    Ok(SealedObjectMetadataSegments {
        generation,
        metadata_path,
        directory_path,
        metadata_record_count: metadata_records.len(),
        directory_record_count: directory_records.len(),
        manifest_path,
        manifest_hash: manifest
            .manifest_hash
            .clone()
            .ok_or_else(|| anyhow!("partition manifest hash was not set"))?,
    })
}

pub fn decode_segment_file(input: &[u8], expected_family: FileFamily) -> Result<SegmentBody> {
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
    SegmentBody::decode(body).map_err(Into::into)
}

async fn write_segment_file(
    path: &Path,
    family: FileFamily,
    header: SegmentHeader,
    records: &[SegmentRecord],
) -> Result<WrittenSegment> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
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
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await?;
    file.write_all(&encoded_header).await?;
    file.write_all(&body).await?;
    file.write_all(&footer.encode()).await?;
    file.sync_data().await?;
    Ok(WrittenSegment {
        family,
        path: path.to_path_buf(),
        record_count: records.len() as u64,
        file_hash: hex::encode(footer.file_hash),
    })
}

async fn write_partition_manifest(
    storage: &Storage,
    bucket: &Bucket,
    generation: u64,
    frames: &[JournalFrame],
    segments: &[WrittenSegment],
    manifest_signing_key: &[u8],
    manifest_path: &Path,
) -> Result<PartitionManifest> {
    if manifest_signing_key.is_empty() {
        return Err(anyhow!("partition manifest signing key must not be empty"));
    }
    if let Some(parent) = manifest_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let last_record_hash = frames
        .last()
        .map(|frame| hex::encode(frame.record_hash))
        .ok_or_else(|| anyhow!("partition manifest requires at least one journal frame"))?;
    let journal_ref = ManifestJournalRef {
        path: storage
            .relative_storage_path(&storage.metadata_journal_path(bucket.tenant_id, bucket.id))?,
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
                path: storage.relative_storage_path(&segment.path)?,
                generation,
                record_count: segment.record_count,
                file_hash: segment.file_hash.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let mut manifest = PartitionManifest {
        format_version: 1,
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode(partition_id(bucket.tenant_id, bucket.id)),
        generation,
        fence_token: 0,
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
    manifest.manifest_hash = Some(manifest_hash);
    manifest.manifest_signature = Some(manifest_signature);
    let encoded = serde_json::to_vec_pretty(&manifest)?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(manifest_path)
        .await?;
    file.write_all(&encoded).await?;
    file.sync_data().await?;
    Ok(manifest)
}

pub fn decode_partition_manifest(
    input: &[u8],
    manifest_signing_key: &[u8],
) -> Result<PartitionManifest> {
    let manifest: PartitionManifest = serde_json::from_slice(input)?;
    verify_partition_manifest(&manifest, manifest_signing_key)?;
    Ok(manifest)
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
    }
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
        partition_id: hex::encode(partition_id(bucket.tenant_id, bucket.id)),
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

pub fn decode_journal_file(input: &[u8]) -> Result<(usize, Vec<JournalFrame>)> {
    let header = BinaryEnvelopeHeader::decode(input)?;
    if header.family != FileFamily::MetadataJournal {
        return Err(anyhow!("not a metadata journal file"));
    }
    let header_len = COMMON_HEADER_LEN
        .checked_add(header.header_json.len())
        .ok_or_else(|| anyhow!("metadata journal header length overflow"))?;
    let frames = decode_frames(&input[header_len..])?;
    Ok((header_len, frames))
}

fn decode_frames(mut input: &[u8]) -> Result<Vec<JournalFrame>> {
    let mut frames = Vec::new();
    while !input.is_empty() {
        if input.len() < 4 {
            return Err(FormatError::TooShort {
                context: "journal frame length",
                needed: 4,
                actual: input.len(),
            }
            .into());
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        let frame_end = 4usize
            .checked_add(frame_len)
            .ok_or_else(|| anyhow!("journal frame length overflow"))?;
        if input.len() < frame_end {
            return Err(FormatError::TooShort {
                context: "journal frame",
                needed: frame_end,
                actual: input.len(),
            }
            .into());
        }
        frames.push(JournalFrame::decode(&input[..frame_end])?);
        input = &input[frame_end..];
    }
    validate_journal_chain(&frames)?;
    Ok(frames)
}

async fn ensure_journal_header(path: &Path, bucket: &Bucket) -> Result<()> {
    if tokio::fs::try_exists(path).await? {
        return Ok(());
    }
    let created_at = bucket.created_at.to_rfc3339();
    let header_json = serde_json::to_vec(&MetadataJournalHeader {
        tenant_id: bucket.tenant_id.to_string(),
        bucket_id: bucket.id.to_string(),
        partition_family: "object_metadata",
        partition_id: hex::encode(partition_id(bucket.tenant_id, bucket.id)),
        fence_token: 0,
        first_sequence: 1,
        created_at: &created_at,
        codec: "none",
    })?;
    let header = BinaryEnvelopeHeader::new(FileFamily::MetadataJournal, 0, 0, header_json);
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await
        .with_context(|| format!("create metadata journal {}", path.display()))?;
    file.write_all(&header.encode()).await?;
    file.sync_data().await?;
    Ok(())
}

fn partition_id(tenant_id: i64, bucket_id: i64) -> Hash32 {
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(&tenant_id.to_le_bytes());
    bytes.extend_from_slice(&bucket_id.to_le_bytes());
    hash32(&bytes)
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
    use chrono::Utc;
    use tempfile::tempdir;

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
            inline_payload: None,
            checksum: None,
        }
    }

    #[tokio::test]
    async fn append_object_mutation_writes_chained_metadata_and_directory_frames() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let first = sample_object(1, "docs/a.txt", false);
        let second = sample_object(2, "docs/b.txt", true);

        let path = append_object_mutation(&storage, &bucket, &first, ObjectJournalMutation::Put)
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

        assert_eq!(
            path,
            storage.metadata_journal_path(bucket.tenant_id, bucket.id)
        );
        let bytes = tokio::fs::read(&path).await.unwrap();
        let (_, frames) = decode_journal_file(&bytes).unwrap();
        assert_eq!(frames.len(), 4);
        assert_eq!(frames[0].record_kind, JournalRecordKind::ObjectVersion);
        assert_eq!(frames[1].record_kind, JournalRecordKind::DirectoryEntry);
        assert_eq!(frames[2].record_kind, JournalRecordKind::DeleteMarker);
        assert_eq!(frames[3].record_kind, JournalRecordKind::DirectoryEntry);
        assert_eq!(frames[1].previous_record_hash, frames[0].record_hash);
        assert_eq!(frames[2].previous_record_hash, frames[1].record_hash);
        validate_journal_chain(&frames).unwrap();
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
            sealed.manifest_path,
            storage.metadata_manifest_path(bucket.tenant_id, bucket.id)
        );

        let manifest_bytes = tokio::fs::read(&sealed.manifest_path).await.unwrap();
        let manifest = decode_partition_manifest(&manifest_bytes, signing_key).unwrap();
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

        let metadata_bytes = tokio::fs::read(&sealed.metadata_path).await.unwrap();
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

        let directory_bytes = tokio::fs::read(&sealed.directory_path).await.unwrap();
        let directory_body =
            decode_segment_file(&directory_bytes, FileFamily::DirectorySegment).unwrap();
        let directory_records = directory_body.data_blocks[0]
            .decode_uncompressed_records()
            .unwrap();
        assert_eq!(directory_records.len(), 2);
        let latest_a: DirectoryEntryBody =
            serde_json::from_slice(&directory_records[0].value).unwrap();
        assert_eq!(latest_a.version_id, second.version_id.to_string());
    }

    #[tokio::test]
    async fn decode_journal_file_rejects_corrupted_appended_frame() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let bucket = sample_bucket();
        let object = sample_object(1, "docs/a.txt", false);
        let path = append_object_mutation(&storage, &bucket, &object, ObjectJournalMutation::Put)
            .await
            .unwrap();

        let mut bytes = tokio::fs::read(&path).await.unwrap();
        let last = bytes.len() - 33;
        bytes[last] ^= 1;
        assert!(decode_journal_file(&bytes).is_err());
    }
}
