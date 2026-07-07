use crate::{
    core_store::{
        CompareAndSwapRef, CoreObjectRef, CorePipelinePolicy, CoreStore, CoreTraceContext, GetBlob,
        WriteLogicalFileRequest, core_object_ref_from_logical_file_manifest,
    },
    formats::{
        BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
        Hash32, hash32,
        vector::{
            HnswGraph, VECTOR_BODY_HEADER_LEN, VECTOR_RECORD_LEN, VectorBodyHeader, VectorMetric,
            VectorModality, VectorPayload, VectorRecord,
        },
    },
    storage::Storage,
    vector_hnsw::{build_hnsw_graph_for_entries, validate_hnsw_graph},
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};

const VECTOR_SEGMENT_REF_PREFIX: &str = "vector_segment:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";
const VECTOR_ANN_KIND_HNSW_V1: u16 = 1;
const VECTOR_BLOCK_LEN_LEN: usize = 8;
const VECTOR_BLOCK_COUNT_LEN: usize = 8;
const VECTOR_BLOCK_CRC_LEN: usize = 4;
const VECTOR_ANN_KIND_LEN: usize = 2;
const VECTOR_ANN_LEN_LEN: usize = 8;
const VECTOR_ANN_CRC_LEN: usize = 4;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorSegmentHeader {
    pub schema: String,
    pub index_id: String,
    pub definition_hash: String,
    pub generation: u64,
    pub dimension: u16,
    pub metric: String,
    pub algorithm: String,
    pub embedding_provider: String,
    pub embedding_model: String,
    pub embedding_model_version: Option<String>,
    pub embedding_normalisation: String,
    pub embedding_chunking_hash: String,
    pub extractor_definition_hash: String,
    pub embedding_provenance_hash: String,
    pub modality: String,
    pub vector_count: u64,
    pub hnsw_m: u16,
    pub hnsw_ef_construction: u16,
    pub ann_format_hash: String,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSegmentEntry {
    pub source_id_binary: Vec<u8>,
    pub source_generation: u64,
    pub labels: Vec<u64>,
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
    pub definition_hash: &'a str,
    pub generation: u64,
    pub dimension: u16,
    pub metric: VectorMetric,
    pub embedding_provider: &'a str,
    pub embedding_model: &'a str,
    pub embedding_model_version: Option<&'a str>,
    pub embedding_normalisation: &'a str,
    pub embedding_chunking_hash: &'a str,
    pub extractor_definition_hash: &'a str,
    pub embedding_provenance_hash: &'a str,
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
) -> Result<String> {
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
    let ref_name =
        vector_segment_ref_name(input.index_id, input.generation, &hex::encode(segment_hash))?;

    let header = VectorSegmentHeader {
        schema: "anvil.index.vector_segment_header.v1".to_string(),
        index_id: input.index_id.to_string(),
        definition_hash: input.definition_hash.to_string(),
        generation: input.generation,
        dimension: input.dimension,
        metric: input.metric.as_name().to_string(),
        algorithm: "hnsw".to_string(),
        embedding_provider: input.embedding_provider.to_string(),
        embedding_model: input.embedding_model.to_string(),
        embedding_model_version: input.embedding_model_version.map(str::to_string),
        embedding_normalisation: input.embedding_normalisation.to_string(),
        embedding_chunking_hash: input.embedding_chunking_hash.to_string(),
        extractor_definition_hash: input.extractor_definition_hash.to_string(),
        embedding_provenance_hash: input.embedding_provenance_hash.to_string(),
        modality: input.modality.as_name().to_string(),
        vector_count: entries.len() as u64,
        hnsw_m: input.hnsw_m,
        hnsw_ef_construction: input.hnsw_ef_construction,
        ann_format_hash: hnsw_ann_format_hash(),
        source_cursor: input.source_cursor,
        authz_revision: input.authz_revision,
        codec: "f32_le_v1".to_string(),
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

    let mut bytes = Vec::with_capacity(encoded_header.len() + body.len() + COMMON_FOOTER_LEN);
    bytes.extend_from_slice(&encoded_header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());

    let store = CoreStore::new(storage.clone()).await?;
    let manifest = store
        .write_logical_file(WriteLogicalFileRequest {
            writer_family: "vector".to_string(),
            generation: input.generation,
            logical_file_id: ref_name.clone(),
            source: bytes,
            range_hints: Vec::new(),
            pipeline_policy: CorePipelinePolicy::default(),
            trace_context: CoreTraceContext::default(),
            boundary_values: Vec::new(),
            mutation_id: format!("vector-segment:{}:{}", input.index_id, input.generation),
            region_id: "local".to_string(),
        })
        .await?;
    let object_ref = core_object_ref_from_logical_file_manifest(&manifest);
    if let Err(error) = store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await
    {
        // The ref name is derived from the deterministic segment body hash. If a
        // concurrent builder published the same segment first, the ref is already
        // valid and this write is complete.
        if read_vector_segment(storage, &ref_name).await.is_ok() {
            return Ok(ref_name);
        }
        return Err(error);
    }
    Ok(ref_name)
}

pub async fn read_vector_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedVectorSegment> {
    let bytes = read_vector_segment_bytes(storage, segment_ref).await?;
    decode_vector_segment(&bytes)
}

pub async fn read_vector_segment_bytes(storage: &Storage, segment_ref: &str) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(segment_ref)
        .await?
        .ok_or_else(|| anyhow!("vector segment ref is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await
}

pub async fn read_latest_vector_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedVectorSegment>> {
    let Some(segment_ref) = latest_vector_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_vector_segment(storage, &segment_ref).await?))
}

pub async fn latest_vector_segment_ref(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<String>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut refs = store
        .list_ref_names(&vector_segment_ref_prefix(index_id)?)
        .await?;
    refs.sort_by_key(|value| generation_from_ref(value).unwrap_or(0));
    Ok(refs.pop())
}

pub(crate) async fn vector_segment_hash_exists(
    storage: &Storage,
    index_id: &str,
    generation: u64,
    expected_segment_hash: &str,
) -> Result<bool> {
    validate_hex32(expected_segment_hash, "vector expected segment hash")?;
    let store = CoreStore::new(storage.clone()).await?;
    let refs = store
        .list_ref_names(&vector_segment_ref_prefix(index_id)?)
        .await?;
    for segment_ref in refs {
        if generation_from_ref(&segment_ref) != Some(generation) {
            continue;
        }
        let bytes = read_vector_segment_bytes(storage, &segment_ref).await?;
        if blake3::hash(&bytes).to_hex().as_str() == expected_segment_hash {
            return Ok(true);
        }
    }
    Ok(false)
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
    validate_vector_segment_header(&header)?;
    let metric = VectorMetric::from_name(&header.metric)?;
    let modality = VectorModality::from_name(&header.modality)?;
    let decoded = decode_vector_body(body, header.dimension)?;
    if header.vector_count != decoded.entries.len() as u64 {
        return Err(anyhow!(
            "vector segment header count does not match decoded entries"
        ));
    }
    validate_entries(header.dimension, metric, modality, &decoded.entries)?;
    validate_hnsw_graph(&decoded.hnsw_graph, &decoded.entries, header.hnsw_m)?;
    validate_deleted_bitset(&decoded.deleted_bitset, decoded.entries.len())?;
    Ok(DecodedVectorSegment { header, ..decoded })
}

fn validate_vector_segment_header(header: &VectorSegmentHeader) -> Result<()> {
    if header.schema != "anvil.index.vector_segment_header.v1" {
        return Err(anyhow!("vector segment header schema is unsupported"));
    }
    if header.definition_hash.is_empty() {
        return Err(anyhow!("vector segment definition hash is missing"));
    }
    if header.algorithm != "hnsw" {
        return Err(anyhow!("vector segment algorithm is unsupported"));
    }
    if header.ann_format_hash != hnsw_ann_format_hash() {
        return Err(anyhow!("vector segment ann format hash is unsupported"));
    }
    if header.codec != "f32_le_v1" {
        return Err(anyhow!("vector segment codec is unsupported"));
    }
    Ok(())
}

fn encode_vector_body(
    entries: &mut [VectorSegmentEntry],
    graph: &HnswGraph,
    deleted_bitset: &[u8],
) -> Result<Vec<u8>> {
    let source_table_bytes = encode_source_table(entries)?;
    let record_table_offset = VECTOR_BODY_HEADER_LEN as u64 + source_table_bytes.len() as u64;
    let vector_blocks_offset =
        record_table_offset + (entries.len() as u64 * VECTOR_RECORD_LEN as u64);
    let vector_block_bytes = encode_vector_blocks(entries)?;
    let ann_blocks_offset = vector_blocks_offset + vector_block_bytes.len() as u64;
    let ann_block_bytes = encode_hnsw_ann_block(graph);
    let deleted_bitset_offset = ann_blocks_offset + ann_block_bytes.len() as u64;
    let body_header = VectorBodyHeader {
        vector_count: entries.len() as u64,
        record_table_offset,
        vector_blocks_offset,
        ann_blocks_offset,
        deleted_bitset_offset,
    };
    let mut out = Vec::with_capacity(deleted_bitset_offset as usize + deleted_bitset.len());
    out.extend_from_slice(&body_header.encode());
    out.extend_from_slice(&source_table_bytes);
    for entry in entries {
        out.extend_from_slice(&entry.record.encode());
    }
    out.extend_from_slice(&vector_block_bytes);
    out.extend_from_slice(&ann_block_bytes);
    out.extend_from_slice(deleted_bitset);
    Ok(out)
}

fn decode_vector_body(body: &[u8], dimension: u16) -> Result<DecodedVectorSegment> {
    let body_header = VectorBodyHeader::decode(body)?;
    let record_table_offset = usize::try_from(body_header.record_table_offset)
        .context("vector record table offset exceeds usize")?;
    let vector_blocks_offset = usize::try_from(body_header.vector_blocks_offset)
        .context("vector blocks offset exceeds usize")?;
    let ann_blocks_offset = usize::try_from(body_header.ann_blocks_offset)
        .context("ann blocks offset exceeds usize")?;
    let deleted_bitset_offset = usize::try_from(body_header.deleted_bitset_offset)
        .context("deleted bitset offset exceeds usize")?;
    if record_table_offset < VECTOR_BODY_HEADER_LEN
        || vector_blocks_offset < record_table_offset
        || ann_blocks_offset < vector_blocks_offset
        || deleted_bitset_offset < ann_blocks_offset
        || deleted_bitset_offset > body.len()
    {
        return Err(anyhow!("vector segment offsets are invalid"));
    }
    let record_count =
        usize::try_from(body_header.vector_count).context("vector count exceeds usize")?;
    let source_entries = decode_source_table(
        &body[VECTOR_BODY_HEADER_LEN..record_table_offset],
        record_count,
    )?;
    let table_len = record_count
        .checked_mul(VECTOR_RECORD_LEN)
        .ok_or_else(|| anyhow!("vector table length overflow"))?;
    if record_table_offset + table_len != vector_blocks_offset {
        return Err(anyhow!(
            "vector table offset range does not match vector count"
        ));
    }
    let mut records = Vec::with_capacity(record_count);
    let mut cursor = record_table_offset;
    for _ in 0..record_count {
        let (record, used) = VectorRecord::decode(&body[cursor..vector_blocks_offset])?;
        records.push(record);
        cursor += used;
    }
    let payloads = decode_vector_blocks(
        &body[vector_blocks_offset..ann_blocks_offset],
        record_count,
        dimension,
    )?;
    let mut entries = Vec::with_capacity(records.len());
    for (idx, record) in records.into_iter().enumerate() {
        let source = source_entries
            .get(idx)
            .ok_or_else(|| anyhow!("vector source table entry is missing"))?;
        entries.push(VectorSegmentEntry {
            source_id_binary: source.source_id_binary.clone(),
            source_generation: source.source_generation,
            labels: source.labels.clone(),
            record,
            payload: payloads
                .get(idx)
                .ok_or_else(|| anyhow!("vector payload block entry is missing"))?
                .clone(),
        });
    }
    let hnsw_graph = decode_hnsw_ann_block(&body[ann_blocks_offset..deleted_bitset_offset])?;
    let deleted_bitset = body[deleted_bitset_offset..].to_vec();
    Ok(DecodedVectorSegment {
        header: VectorSegmentHeader {
            schema: String::new(),
            index_id: String::new(),
            definition_hash: String::new(),
            generation: 0,
            dimension,
            metric: String::new(),
            algorithm: String::new(),
            embedding_provider: String::new(),
            embedding_model_version: None,
            embedding_normalisation: String::new(),
            embedding_chunking_hash: String::new(),
            extractor_definition_hash: String::new(),
            embedding_provenance_hash: String::new(),
            embedding_model: String::new(),
            modality: String::new(),
            vector_count: 0,
            hnsw_m: 0,
            hnsw_ef_construction: 0,
            ann_format_hash: String::new(),
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

fn encode_vector_blocks(entries: &mut [VectorSegmentEntry]) -> Result<Vec<u8>> {
    let mut raw_vectors = Vec::new();
    for (ordinal, entry) in entries.iter_mut().enumerate() {
        let vector_offset = ordinal
            .checked_mul(entry.payload.dimension as usize)
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| anyhow!("vector block offset overflow"))?;
        entry.record.vector_payload_offset =
            u64::try_from(vector_offset).context("vector block offset exceeds u64")?;
        raw_vectors.extend_from_slice(&entry.payload.encode()?);
    }

    let vector_count = entries.len() as u64;
    let block_len = VECTOR_BLOCK_COUNT_LEN
        .checked_add(raw_vectors.len())
        .and_then(|value| value.checked_add(VECTOR_BLOCK_CRC_LEN))
        .ok_or_else(|| anyhow!("vector block length overflow"))?;
    let mut crc_input = Vec::with_capacity(VECTOR_BLOCK_COUNT_LEN + raw_vectors.len());
    crc_input.extend_from_slice(&vector_count.to_le_bytes());
    crc_input.extend_from_slice(&raw_vectors);

    let mut out = Vec::with_capacity(VECTOR_BLOCK_LEN_LEN + block_len);
    out.extend_from_slice(&(block_len as u64).to_le_bytes());
    out.extend_from_slice(&vector_count.to_le_bytes());
    out.extend_from_slice(&raw_vectors);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    Ok(out)
}

fn decode_vector_blocks(
    input: &[u8],
    expected_count: usize,
    dimension: u16,
) -> Result<Vec<VectorPayload>> {
    if input.len() < VECTOR_BLOCK_LEN_LEN + VECTOR_BLOCK_COUNT_LEN + VECTOR_BLOCK_CRC_LEN {
        return Err(anyhow!("vector block is truncated"));
    }
    let block_len = u64::from_le_bytes(input[0..8].try_into().unwrap());
    let block_len = usize::try_from(block_len).context("vector block length exceeds usize")?;
    if block_len + VECTOR_BLOCK_LEN_LEN != input.len() {
        return Err(anyhow!("vector block length does not match payload"));
    }

    let vector_count = u64::from_le_bytes(input[8..16].try_into().unwrap());
    let vector_count = usize::try_from(vector_count).context("vector block count exceeds usize")?;
    if vector_count != expected_count {
        return Err(anyhow!("vector block count does not match segment count"));
    }
    let raw_vectors_start = VECTOR_BLOCK_LEN_LEN + VECTOR_BLOCK_COUNT_LEN;
    let raw_vectors_end = input
        .len()
        .checked_sub(VECTOR_BLOCK_CRC_LEN)
        .ok_or_else(|| anyhow!("vector block is shorter than crc"))?;
    let actual_crc = u32::from_le_bytes(input[raw_vectors_end..].try_into().unwrap());
    if crc32c(&input[VECTOR_BLOCK_LEN_LEN..raw_vectors_end]) != actual_crc {
        return Err(anyhow!("vector block crc32c mismatch"));
    }

    let vector_bytes = usize::from(dimension)
        .checked_mul(4)
        .ok_or_else(|| anyhow!("vector dimension byte length overflow"))?;
    let raw_vectors = &input[raw_vectors_start..raw_vectors_end];
    if raw_vectors.len()
        != expected_count
            .checked_mul(vector_bytes)
            .ok_or_else(|| anyhow!("vector block raw length overflow"))?
    {
        return Err(anyhow!(
            "vector block raw vector length does not match dimension"
        ));
    }

    raw_vectors
        .chunks_exact(vector_bytes)
        .map(|chunk| VectorPayload::decode(chunk, dimension).map_err(anyhow::Error::from))
        .collect()
}

fn encode_hnsw_ann_block(graph: &HnswGraph) -> Vec<u8> {
    let ann_bytes = graph.encode();
    let mut crc_input =
        Vec::with_capacity(VECTOR_ANN_KIND_LEN + VECTOR_ANN_LEN_LEN + ann_bytes.len());
    crc_input.extend_from_slice(&VECTOR_ANN_KIND_HNSW_V1.to_le_bytes());
    crc_input.extend_from_slice(&(ann_bytes.len() as u64).to_le_bytes());
    crc_input.extend_from_slice(&ann_bytes);

    let mut out = Vec::with_capacity(crc_input.len() + VECTOR_ANN_CRC_LEN);
    out.extend_from_slice(&crc_input);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    out
}

fn decode_hnsw_ann_block(input: &[u8]) -> Result<HnswGraph> {
    if input.len() < VECTOR_ANN_KIND_LEN + VECTOR_ANN_LEN_LEN + VECTOR_ANN_CRC_LEN {
        return Err(anyhow!("ann block is truncated"));
    }
    let ann_kind = u16::from_le_bytes(input[0..2].try_into().unwrap());
    if ann_kind != VECTOR_ANN_KIND_HNSW_V1 {
        return Err(anyhow!("unsupported vector ann kind {ann_kind}"));
    }
    let ann_len = u64::from_le_bytes(input[2..10].try_into().unwrap());
    let ann_len = usize::try_from(ann_len).context("ann block length exceeds usize")?;
    let ann_start = VECTOR_ANN_KIND_LEN + VECTOR_ANN_LEN_LEN;
    let ann_end = ann_start
        .checked_add(ann_len)
        .ok_or_else(|| anyhow!("ann block length overflow"))?;
    if ann_end + VECTOR_ANN_CRC_LEN != input.len() {
        return Err(anyhow!("ann block length does not match payload"));
    }
    let expected_crc = u32::from_le_bytes(input[ann_end..].try_into().unwrap());
    if crc32c(&input[..ann_end]) != expected_crc {
        return Err(anyhow!("ann block crc32c mismatch"));
    }
    Ok(HnswGraph::decode(&input[ann_start..ann_end])?)
}

fn hnsw_ann_format_hash() -> String {
    let description = b"anvil.vector.ann.hnsw_v1:ann_kind:u16le,ann_len:u64le,hnsw:layer_count:u16le,node_count:u64le,layers[layer_index:u16le,node_count:u64le,node[vector_id:u64le,neighbor_count:u16le,neighbors:u64le*]],crc32c:u32le";
    format!("blake3:{}", blake3::hash(description).to_hex())
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            let mask = 0u32.wrapping_sub(crc & 1);
            crc = (crc >> 1) ^ (0x82f6_3b78 & mask);
        }
    }
    !crc
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct VectorSourceEntry {
    source_id_binary: Vec<u8>,
    source_generation: u64,
    labels: Vec<u64>,
}

fn encode_source_table(entries: &[VectorSegmentEntry]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u64).to_le_bytes());
    for entry in entries {
        let source_len = u32::try_from(entry.source_id_binary.len())
            .context("vector source id binary length exceeds u32")?;
        out.extend_from_slice(&source_len.to_le_bytes());
        out.extend_from_slice(&entry.source_id_binary);
        out.extend_from_slice(&entry.source_generation.to_le_bytes());
        out.extend_from_slice(&entry.record.chunk_id.to_le_bytes());
        let label_count =
            u32::try_from(entry.labels.len()).context("vector label count exceeds u32")?;
        out.extend_from_slice(&label_count.to_le_bytes());
        for label in &entry.labels {
            out.extend_from_slice(&label.to_le_bytes());
        }
    }
    Ok(out)
}

fn decode_source_table(input: &[u8], expected_count: usize) -> Result<Vec<VectorSourceEntry>> {
    if input.len() < 8 {
        return Err(anyhow!("vector source table is shorter than count"));
    }
    let count = u64::from_le_bytes(input[0..8].try_into().unwrap());
    let count = usize::try_from(count).context("vector source count exceeds usize")?;
    if count != expected_count {
        return Err(anyhow!(
            "vector source table count does not match vector count"
        ));
    }
    let mut entries = Vec::with_capacity(count);
    let mut cursor = 8usize;
    for _ in 0..count {
        if input.len().saturating_sub(cursor) < 4 {
            return Err(anyhow!("vector source entry missing source id length"));
        }
        let source_len = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let source_len = usize::try_from(source_len).context("source id length exceeds usize")?;
        if input.len().saturating_sub(cursor) < source_len + 8 + 4 + 4 {
            return Err(anyhow!("vector source entry is truncated"));
        }
        let source_id_binary = input[cursor..cursor + source_len].to_vec();
        cursor += source_len;
        let source_generation = u64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let _chunk_ordinal = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let label_count = u32::from_le_bytes(input[cursor..cursor + 4].try_into().unwrap());
        cursor += 4;
        let label_count = usize::try_from(label_count).context("label count exceeds usize")?;
        let label_bytes = label_count
            .checked_mul(8)
            .ok_or_else(|| anyhow!("label byte length overflow"))?;
        if input.len().saturating_sub(cursor) < label_bytes {
            return Err(anyhow!("vector source labels are truncated"));
        }
        let mut labels = Vec::with_capacity(label_count);
        for _ in 0..label_count {
            labels.push(u64::from_le_bytes(
                input[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        }
        entries.push(VectorSourceEntry {
            source_id_binary,
            source_generation,
            labels,
        });
    }
    if cursor != input.len() {
        return Err(anyhow!("vector source table has trailing bytes"));
    }
    Ok(entries)
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
        if entry.source_id_binary.is_empty() {
            return Err(anyhow!("vector entry source id is empty"));
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

fn vector_segment_ref_prefix(index_id: &str) -> Result<String> {
    require_safe_component(index_id, "vector index id")?;
    Ok(format!("{VECTOR_SEGMENT_REF_PREFIX}index:{index_id}:"))
}

fn vector_segment_ref_name(index_id: &str, generation: u64, segment_hash: &str) -> Result<String> {
    validate_hex32(segment_hash, "vector segment hash")?;
    Ok(format!(
        "{}generation:{generation:020}:hash:{segment_hash}",
        vector_segment_ref_prefix(index_id)?
    ))
}

fn generation_from_ref(ref_name: &str) -> Option<u64> {
    ref_name
        .rsplit_once(":generation:")?
        .1
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
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
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn entry(vector_id: u64, values: Vec<f32>) -> VectorSegmentEntry {
        VectorSegmentEntry {
            source_id_binary: vec![vector_id as u8, 0xaa],
            source_generation: vector_id * 10,
            labels: if vector_id == 1 { vec![42] } else { Vec::new() },
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
        let segment_ref = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                definition_hash: "blake3:test-definition",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_provider: "test_only",
                embedding_model_version: None,
                embedding_normalisation: "unit_l2",
                embedding_chunking_hash: "blake3:test-chunking",
                extractor_definition_hash: "blake3:test-extractor",
                embedding_provenance_hash: "blake3:test-provenance",
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
        assert!(segment_ref.starts_with(VECTOR_SEGMENT_REF_PREFIX));

        let decoded = read_vector_segment(&storage, &segment_ref).await.unwrap();
        assert_eq!(decoded.header.index_id, "vector-alpha");
        assert_eq!(
            decoded.header.schema,
            "anvil.index.vector_segment_header.v1"
        );
        assert_eq!(decoded.header.definition_hash, "blake3:test-definition");
        assert_eq!(decoded.header.dimension, 3);
        assert_eq!(decoded.header.metric, "cosine");
        assert_eq!(decoded.header.algorithm, "hnsw");
        assert_eq!(decoded.header.embedding_provider, "test_only");
        assert_eq!(decoded.header.embedding_model, "embedding-v1");
        assert_eq!(decoded.header.embedding_normalisation, "unit_l2");
        assert_eq!(
            decoded.header.embedding_chunking_hash,
            "blake3:test-chunking"
        );
        assert_eq!(
            decoded.header.extractor_definition_hash,
            "blake3:test-extractor"
        );
        assert_eq!(
            decoded.header.embedding_provenance_hash,
            "blake3:test-provenance"
        );
        assert_eq!(decoded.header.vector_count, 2);
        assert_eq!(decoded.header.ann_format_hash, hnsw_ann_format_hash());
        assert_eq!(decoded.header.codec, "f32_le_v1");
        assert_eq!(decoded.entries.len(), 2);
        assert_eq!(decoded.entries[0].record.vector_id, 1);
        assert_eq!(decoded.entries[1].record.vector_id, 2);
        assert_eq!(decoded.entries[0].source_id_binary, vec![1, 0xaa]);
        assert_eq!(decoded.entries[0].source_generation, 10);
        assert_eq!(decoded.entries[0].labels, vec![42]);
        assert_eq!(decoded.entries[1].source_id_binary, vec![2, 0xaa]);
        assert_eq!(decoded.entries[1].source_generation, 20);
        assert!(decoded.entries[1].labels.is_empty());
        assert!(decoded.body_header.record_table_offset > VECTOR_BODY_HEADER_LEN as u64);
        assert!(decoded.body_header.vector_blocks_offset > decoded.body_header.record_table_offset);
        assert!(decoded.body_header.ann_blocks_offset > decoded.body_header.vector_blocks_offset);
        assert_eq!(decoded.hnsw_graph.node_count, 2);
        assert!(!decoded.hnsw_graph.layers.is_empty());
        assert_eq!(decoded.deleted_bitset, vec![0]);
    }

    #[test]
    fn vector_body_rejects_corrupt_vector_block_crc32c() {
        let mut entries = vec![entry(1, vec![1.0, 0.0, 0.0])];
        let graph = build_hnsw_graph_for_entries(&entries, VectorMetric::Cosine, 32, 200).unwrap();
        let mut body = encode_vector_body(&mut entries, &graph, &[0]).unwrap();
        let body_header = VectorBodyHeader::decode(&body).unwrap();
        let crc_byte = usize::try_from(body_header.ann_blocks_offset).unwrap() - 1;
        body[crc_byte] ^= 1;

        assert!(
            decode_vector_body(&body, 3)
                .unwrap_err()
                .to_string()
                .contains("vector block crc32c mismatch")
        );
    }

    #[test]
    fn vector_body_rejects_corrupt_ann_block_crc32c() {
        let mut entries = vec![entry(1, vec![1.0, 0.0, 0.0])];
        let graph = build_hnsw_graph_for_entries(&entries, VectorMetric::Cosine, 32, 200).unwrap();
        let mut body = encode_vector_body(&mut entries, &graph, &[0]).unwrap();
        let body_header = VectorBodyHeader::decode(&body).unwrap();
        let crc_byte = usize::try_from(body_header.deleted_bitset_offset).unwrap() - 1;
        body[crc_byte] ^= 1;

        assert!(
            decode_vector_body(&body, 3)
                .unwrap_err()
                .to_string()
                .contains("ann block crc32c mismatch")
        );
    }

    #[tokio::test]
    async fn vector_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let segment_ref = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-alpha",
                definition_hash: "blake3:test-definition",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_provider: "test_only",
                embedding_model_version: None,
                embedding_normalisation: "unit_l2",
                embedding_chunking_hash: "blake3:test-chunking",
                extractor_definition_hash: "blake3:test-extractor",
                embedding_provenance_hash: "blake3:test-provenance",
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
        let mut bytes = read_vector_segment_bytes(&storage, &segment_ref)
            .await
            .unwrap();
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
                definition_hash: "blake3:test-definition",
                generation: 6,
                dimension: 3,
                metric: VectorMetric::Cosine,
                embedding_provider: "test_only",
                embedding_model_version: None,
                embedding_normalisation: "unit_l2",
                embedding_chunking_hash: "blake3:test-chunking",
                extractor_definition_hash: "blake3:test-extractor",
                embedding_provenance_hash: "blake3:test-provenance",
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
                    definition_hash: "blake3:test-definition",
                    generation,
                    dimension: 3,
                    metric: VectorMetric::Cosine,
                    embedding_provider: "test_only",
                    embedding_model: "embedding-v1",
                    embedding_model_version: None,
                    embedding_normalisation: "unit_l2",
                    embedding_chunking_hash: "blake3:test-chunking",
                    extractor_definition_hash: "blake3:test-extractor",
                    embedding_provenance_hash: "blake3:test-provenance",
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
            latest_vector_segment_ref(&storage, "../escape")
                .await
                .is_err()
        );
    }
}
