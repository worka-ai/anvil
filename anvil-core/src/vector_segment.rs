use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32, hash32,
    vector::{
        HnswGraph, VECTOR_BODY_HEADER_LEN, VECTOR_RECORD_LEN, VectorBodyHeader, VectorMetric,
        VectorModality, VectorPayload, VectorRecord,
    },
};
use crate::storage::Storage;
use crate::vector_hnsw::{build_hnsw_graph_for_entries, validate_hnsw_graph};
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorSegmentHeader {
    pub index_id: String,
    pub generation: u64,
    pub dimension: u16,
    pub metric: String,
    pub embedding_model: String,
    pub modality: String,
    pub hnsw_m: u16,
    pub hnsw_ef_construction: u16,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSegmentEntry {
    pub record: VectorRecord,
    pub payload: VectorPayload,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedVectorSegment {
    pub header: VectorSegmentHeader,
    pub body_header: VectorBodyHeader,
    pub entries: Vec<VectorSegmentEntry>,
    pub hnsw_graph: HnswGraph,
    pub deleted_bitset: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct VectorSegmentWrite<'a> {
    pub index_id: &'a str,
    pub generation: u64,
    pub dimension: u16,
    pub metric: VectorMetric,
    pub embedding_model: &'a str,
    pub modality: VectorModality,
    pub hnsw_m: u16,
    pub hnsw_ef_construction: u16,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub entries: &'a [VectorSegmentEntry],
    pub deleted_bitset: &'a [u8],
}

pub async fn write_vector_segment(
    storage: &Storage,
    input: VectorSegmentWrite<'_>,
) -> Result<PathBuf> {
    let mut entries = input.entries.to_vec();
    validate_entries(input.dimension, input.metric, input.modality, &entries)?;
    entries.sort_by_key(|entry| entry.record.vector_id);
    validate_deleted_bitset(input.deleted_bitset, entries.len())?;
    let hnsw_graph = build_hnsw_graph_for_entries(
        &entries,
        input.metric,
        input.hnsw_m,
        input.hnsw_ef_construction,
    )?;
    let body = encode_vector_body(&mut entries, &hnsw_graph, input.deleted_bitset)?;
    let segment_hash = hash32(&body);
    let path = storage.vector_segment_path(
        input.index_id,
        input.generation,
        &hex::encode(segment_hash),
    )?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let header = VectorSegmentHeader {
        index_id: input.index_id.to_string(),
        generation: input.generation,
        dimension: input.dimension,
        metric: input.metric.as_name().to_string(),
        embedding_model: input.embedding_model.to_string(),
        modality: input.modality.as_name().to_string(),
        hnsw_m: input.hnsw_m,
        hnsw_ef_construction: input.hnsw_ef_construction,
        source_cursor: input.source_cursor,
        authz_revision: input.authz_revision,
        codec: "none".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::VectorSegment, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let (first_hash, last_hash) = record_hash_bounds(&entries);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        entries.len() as u64,
        first_hash,
        last_hash,
    );

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("vector segment path has no file name"))?;
    let tmp_path = path.with_file_name(format!(".{file_name}.tmp-{}", uuid::Uuid::new_v4()));
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .truncate(true)
        .open(&tmp_path)
        .await
        .with_context(|| format!("create vector segment temp {}", tmp_path.display()))?;
    file.write_all(&encoded_header).await?;
    file.write_all(&body).await?;
    file.write_all(&footer.encode()).await?;
    file.sync_data().await?;
    drop(file);
    if tokio::fs::try_exists(&path).await? {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Ok(path);
    }
    tokio::fs::rename(&tmp_path, &path)
        .await
        .with_context(|| format!("publish vector segment {}", path.display()))?;
    Ok(path)
}

pub async fn read_vector_segment(path: impl Into<PathBuf>) -> Result<DecodedVectorSegment> {
    let path = path.into();
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read vector segment {}", path.display()))?;
    decode_vector_segment(&bytes)
}

pub async fn read_latest_vector_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedVectorSegment>> {
    let Some(path) = latest_vector_segment_path(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_vector_segment(path).await?))
}

pub async fn latest_vector_segment_path(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<PathBuf>> {
    latest_segment_path(storage.vector_segment_dir(index_id)?, ".anvec").await
}

pub fn decode_vector_segment(bytes: &[u8]) -> Result<DecodedVectorSegment> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::VectorSegment {
        return Err(anyhow!("vector segment file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("vector segment is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("vector segment header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("vector segment footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("vector segment body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body)?;
    let header: VectorSegmentHeader = serde_json::from_slice(&envelope.header_json)?;
    let metric = VectorMetric::from_name(&header.metric)?;
    let modality = VectorModality::from_name(&header.modality)?;
    let decoded = decode_vector_body(body, header.dimension)?;
    validate_entries(header.dimension, metric, modality, &decoded.entries)?;
    validate_hnsw_graph(&decoded.hnsw_graph, &decoded.entries, header.hnsw_m)?;
    validate_deleted_bitset(&decoded.deleted_bitset, decoded.entries.len())?;
    Ok(DecodedVectorSegment { header, ..decoded })
}

fn encode_vector_body(
    entries: &mut [VectorSegmentEntry],
    graph: &HnswGraph,
    deleted_bitset: &[u8],
) -> Result<Vec<u8>> {
    let vector_table_offset = VECTOR_BODY_HEADER_LEN as u64;
    let vector_payload_offset =
        vector_table_offset + (entries.len() as u64 * VECTOR_RECORD_LEN as u64);
    let mut payload_bytes = Vec::new();
    for entry in entries.iter_mut() {
        entry.record.vector_payload_offset = payload_bytes.len() as u64;
        payload_bytes.extend_from_slice(&entry.payload.encode()?);
    }
    let graph_bytes = graph.encode();
    let hnsw_graph_offset = vector_payload_offset + payload_bytes.len() as u64;
    let deleted_bitset_offset = hnsw_graph_offset + graph_bytes.len() as u64;
    let body_header = VectorBodyHeader {
        vector_count: entries.len() as u64,
        vector_table_offset,
        vector_payload_offset,
        hnsw_graph_offset,
        deleted_bitset_offset,
    };
    let mut out = Vec::with_capacity(deleted_bitset_offset as usize + deleted_bitset.len());
    out.extend_from_slice(&body_header.encode());
    for entry in entries {
        out.extend_from_slice(&entry.record.encode());
    }
    out.extend_from_slice(&payload_bytes);
    out.extend_from_slice(&graph_bytes);
    out.extend_from_slice(deleted_bitset);
    Ok(out)
}

fn decode_vector_body(body: &[u8], dimension: u16) -> Result<DecodedVectorSegment> {
    let body_header = VectorBodyHeader::decode(body)?;
    let vector_table_offset = usize::try_from(body_header.vector_table_offset)
        .context("vector table offset exceeds usize")?;
    let vector_payload_offset = usize::try_from(body_header.vector_payload_offset)
        .context("vector payload offset exceeds usize")?;
    let hnsw_graph_offset = usize::try_from(body_header.hnsw_graph_offset)
        .context("hnsw graph offset exceeds usize")?;
    let deleted_bitset_offset = usize::try_from(body_header.deleted_bitset_offset)
        .context("deleted bitset offset exceeds usize")?;
    if vector_table_offset != VECTOR_BODY_HEADER_LEN
        || vector_payload_offset < vector_table_offset
        || hnsw_graph_offset < vector_payload_offset
        || deleted_bitset_offset < hnsw_graph_offset
        || deleted_bitset_offset > body.len()
    {
        return Err(anyhow!("vector segment offsets are invalid"));
    }
    let record_count =
        usize::try_from(body_header.vector_count).context("vector count exceeds usize")?;
    let table_len = record_count
        .checked_mul(VECTOR_RECORD_LEN)
        .ok_or_else(|| anyhow!("vector table length overflow"))?;
    if vector_table_offset + table_len != vector_payload_offset {
        return Err(anyhow!(
            "vector table offset range does not match vector count"
        ));
    }
    let mut records = Vec::with_capacity(record_count);
    let mut cursor = vector_table_offset;
    for _ in 0..record_count {
        let (record, used) = VectorRecord::decode(&body[cursor..vector_payload_offset])?;
        records.push(record);
        cursor += used;
    }
    let mut payload_offsets = records
        .iter()
        .map(|record| record.vector_payload_offset as usize)
        .collect::<Vec<_>>();
    payload_offsets.push(hnsw_graph_offset - vector_payload_offset);
    let mut entries = Vec::with_capacity(records.len());
    for (idx, record) in records.into_iter().enumerate() {
        let start = vector_payload_offset
            .checked_add(payload_offsets[idx])
            .ok_or_else(|| anyhow!("vector payload start overflow"))?;
        let end = vector_payload_offset
            .checked_add(payload_offsets[idx + 1])
            .ok_or_else(|| anyhow!("vector payload end overflow"))?;
        if start > end || end > hnsw_graph_offset {
            return Err(anyhow!("vector payload offset exceeds payload block"));
        }
        entries.push(VectorSegmentEntry {
            record,
            payload: VectorPayload::decode(&body[start..end], dimension)?,
        });
    }
    let hnsw_graph = HnswGraph::decode(&body[hnsw_graph_offset..deleted_bitset_offset])?;
    let deleted_bitset = body[deleted_bitset_offset..].to_vec();
    Ok(DecodedVectorSegment {
        header: VectorSegmentHeader {
            index_id: String::new(),
            generation: 0,
            dimension,
            metric: String::new(),
            embedding_model: String::new(),
            modality: String::new(),
            hnsw_m: 0,
            hnsw_ef_construction: 0,
            source_cursor: 0,
            authz_revision: 0,
            codec: String::new(),
            created_at: String::new(),
        },
        body_header,
        entries,
        hnsw_graph,
        deleted_bitset,
    })
}

fn validate_entries(
    dimension: u16,
    metric: VectorMetric,
    modality: VectorModality,
    entries: &[VectorSegmentEntry],
) -> Result<()> {
    for entry in entries {
        if entry.payload.dimension != dimension
            || entry.record.dimension != dimension
            || entry.record.metric != metric as u8
            || entry.record.modality != modality as u8
        {
            return Err(anyhow!("vector entry does not match segment header"));
        }
    }
    Ok(())
}

fn validate_deleted_bitset(deleted_bitset: &[u8], entry_count: usize) -> Result<()> {
    let expected_len = entry_count.div_ceil(8);
    if deleted_bitset.len() != expected_len {
        return Err(anyhow!(
            "vector deleted bitset length does not match entry count"
        ));
    }
    Ok(())
}

fn record_hash_bounds(entries: &[VectorSegmentEntry]) -> (Hash32, Hash32) {
    let first = entries
        .first()
        .map(|entry| hash32(&entry.record.encode()))
        .unwrap_or([0; 32]);
    let last = entries
        .last()
        .map(|entry| hash32(&entry.record.encode()))
        .unwrap_or([0; 32]);
    (first, last)
}

async fn latest_segment_path(dir: PathBuf, suffix: &str) -> Result<Option<PathBuf>> {
    let mut entries = match tokio::fs::read_dir(&dir).await {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };
    let mut latest: Option<(u64, PathBuf)> = None;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.ends_with(suffix) {
            continue;
        }
        let Some(generation) = name
            .strip_prefix("generation-")
            .and_then(|rest| rest.split('-').next())
            .and_then(|value| value.parse::<u64>().ok())
        else {
            continue;
        };
        match latest {
            Some((current, _)) if generation <= current => {}
            _ => latest = Some((generation, path)),
        }
    }
    Ok(latest.map(|(_, path)| path))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(vector_id: u64, values: Vec<f32>) -> VectorSegmentEntry {
        VectorSegmentEntry {
            record: VectorRecord {
                vector_id,
                object_version_id: [vector_id as u8; 16],
                chunk_id: vector_id as u32,
                modality: VectorModality::Text as u8,
                metric: VectorMetric::Cosine as u8,
                dimension: 3,
                vector_payload_offset: 0,
                source_start: vector_id * 100,
                source_len: 20,
                authz_label_hash: [7; 32],
                metadata_filter_bits: 0,
            },
            payload: VectorPayload {
                dimension: 3,
                values,
            },
        }
    }

    #[tokio::test]
    async fn vector_segment_round_trips_payloads_and_graph() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let entries = vec![entry(2, vec![0.0, 1.0, 0.0]), entry(1, vec![1.0, 0.0, 0.0])];
        let path = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_model: "embedding-v1",
                modality: VectorModality::Text,
                hnsw_m: 32,
                hnsw_ef_construction: 200,
                source_cursor: 88,
                authz_revision: 9,
                entries: &entries,
                deleted_bitset: &[0],
            },
        )
        .await
        .unwrap();
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".anvec"))
        );

        let decoded = read_vector_segment(path).await.unwrap();
        assert_eq!(decoded.header.index_id, "vector-alpha");
        assert_eq!(decoded.header.dimension, 3);
        assert_eq!(decoded.header.metric, "cosine");
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].record.vector_id, 1);
        assert_eq!(decoded.entries[1].record.vector_id, 2);
        assert_eq!(decoded.hnsw_graph.node_count, 2);
        assert!(!decoded.hnsw_graph.layers.is_empty());
        assert_eq!(decoded.deleted_bitset, vec![0]);
    }

    #[tokio::test]
    async fn vector_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let path = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_model: "embedding-v1",
                modality: VectorModality::Text,
                hnsw_m: 32,
                hnsw_ef_construction: 200,
                source_cursor: 88,
                authz_revision: 9,
                entries: &[entry(1, vec![1.0, 0.0, 0.0])],
                deleted_bitset: &[0],
            },
        )
        .await
        .unwrap();
        let mut bytes = tokio::fs::read(path).await.unwrap();
        bytes[COMMON_HEADER_LEN + 1] ^= 1;
        assert!(decode_vector_segment(&bytes).is_err());
    }

    #[tokio::test]
    async fn vector_segment_rejects_deleted_bitset_length_mismatch() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let err = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_model: "embedding-v1",
                modality: VectorModality::Text,
                hnsw_m: 32,
                hnsw_ef_construction: 200,
                source_cursor: 88,
                authz_revision: 9,
                entries: &[entry(1, vec![1.0, 0.0, 0.0])],
                deleted_bitset: &[],
            },
        )
        .await
        .unwrap_err();

        assert!(
            err.to_string()
                .contains("vector deleted bitset length does not match entry count")
        );
    }

    #[tokio::test]
    async fn latest_vector_segment_selects_highest_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let entries = [entry(1, vec![1.0, 0.0, 0.0])];
        for generation in [1, 3, 2] {
            write_vector_segment(
                &storage,
                VectorSegmentWrite {
                    index_id: "vector-alpha",
                    generation,
                    dimension: 3,
                    metric: VectorMetric::Cosine,
                    embedding_model: "embedding-v1",
                    modality: VectorModality::Text,
                    hnsw_m: 32,
                    hnsw_ef_construction: 200,
                    source_cursor: generation,
                    authz_revision: 0,
                    entries: &entries,
                    deleted_bitset: &[0],
                },
            )
            .await
            .unwrap();
        }
        let latest = read_latest_vector_segment(&storage, "vector-alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.header.generation, 3);
        assert!(
            latest_vector_segment_path(&storage, "../escape")
                .await
                .is_err()
        );
    }
}
