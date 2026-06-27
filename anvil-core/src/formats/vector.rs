use super::{FormatError, Hash32};
use std::convert::TryInto;

pub const VECTOR_BODY_HEADER_LEN: usize = 8 + 8 + 8 + 8 + 8;
pub const VECTOR_RECORD_LEN: usize = 8 + 16 + 4 + 1 + 1 + 2 + 8 + 8 + 4 + 32 + 8;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorBodyHeader {
    pub vector_count: u64,
    pub vector_table_offset: u64,
    pub vector_payload_offset: u64,
    pub hnsw_graph_offset: u64,
    pub deleted_bitset_offset: u64,
}

impl VectorBodyHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(VECTOR_BODY_HEADER_LEN);
        out.extend_from_slice(&self.vector_count.to_le_bytes());
        out.extend_from_slice(&self.vector_table_offset.to_le_bytes());
        out.extend_from_slice(&self.vector_payload_offset.to_le_bytes());
        out.extend_from_slice(&self.hnsw_graph_offset.to_le_bytes());
        out.extend_from_slice(&self.deleted_bitset_offset.to_le_bytes());
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < VECTOR_BODY_HEADER_LEN {
            return Err(FormatError::TooShort {
                context: "vector body header",
                needed: VECTOR_BODY_HEADER_LEN,
                actual: input.len(),
            });
        }
        Ok(Self {
            vector_count: u64::from_le_bytes(input[0..8].try_into().unwrap()),
            vector_table_offset: u64::from_le_bytes(input[8..16].try_into().unwrap()),
            vector_payload_offset: u64::from_le_bytes(input[16..24].try_into().unwrap()),
            hnsw_graph_offset: u64::from_le_bytes(input[24..32].try_into().unwrap()),
            deleted_bitset_offset: u64::from_le_bytes(input[32..40].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorRecord {
    pub vector_id: u64,
    pub object_version_id: [u8; 16],
    pub chunk_id: u32,
    pub modality: u8,
    pub metric: u8,
    pub dimension: u16,
    pub vector_payload_offset: u64,
    pub source_start: u64,
    pub source_len: u32,
    pub authz_label_hash: Hash32,
    pub metadata_filter_bits: u64,
}

impl VectorRecord {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(VECTOR_RECORD_LEN);
        out.extend_from_slice(&self.vector_id.to_le_bytes());
        out.extend_from_slice(&self.object_version_id);
        out.extend_from_slice(&self.chunk_id.to_le_bytes());
        out.push(self.modality);
        out.push(self.metric);
        out.extend_from_slice(&self.dimension.to_le_bytes());
        out.extend_from_slice(&self.vector_payload_offset.to_le_bytes());
        out.extend_from_slice(&self.source_start.to_le_bytes());
        out.extend_from_slice(&self.source_len.to_le_bytes());
        out.extend_from_slice(&self.authz_label_hash);
        out.extend_from_slice(&self.metadata_filter_bits.to_le_bytes());
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < VECTOR_RECORD_LEN {
            return Err(FormatError::TooShort {
                context: "vector record",
                needed: VECTOR_RECORD_LEN,
                actual: input.len(),
            });
        }
        Ok((
            Self {
                vector_id: u64::from_le_bytes(input[0..8].try_into().unwrap()),
                object_version_id: input[8..24].try_into().unwrap(),
                chunk_id: u32::from_le_bytes(input[24..28].try_into().unwrap()),
                modality: input[28],
                metric: input[29],
                dimension: u16::from_le_bytes(input[30..32].try_into().unwrap()),
                vector_payload_offset: u64::from_le_bytes(input[32..40].try_into().unwrap()),
                source_start: u64::from_le_bytes(input[40..48].try_into().unwrap()),
                source_len: u32::from_le_bytes(input[48..52].try_into().unwrap()),
                authz_label_hash: input[52..84].try_into().unwrap(),
                metadata_filter_bits: u64::from_le_bytes(input[84..92].try_into().unwrap()),
            },
            VECTOR_RECORD_LEN,
        ))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorPayload {
    pub dimension: u16,
    pub values: Vec<f32>,
}

impl VectorPayload {
    pub fn encode(&self) -> Result<Vec<u8>, FormatError> {
        if self.values.len() != self.dimension as usize {
            return Err(FormatError::InvalidDeclaredLength {
                context: "vector payload dimension",
            });
        }
        let mut out = Vec::with_capacity(self.values.len() * 4);
        for value in &self.values {
            out.extend_from_slice(&value.to_le_bytes());
        }
        Ok(out)
    }

    pub fn decode(input: &[u8], dimension: u16) -> Result<Self, FormatError> {
        let expected_len = dimension as usize * 4;
        if input.len() != expected_len {
            return Err(FormatError::InvalidDeclaredLength {
                context: "vector payload length",
            });
        }
        let mut values = Vec::with_capacity(dimension as usize);
        for chunk in input.chunks_exact(4) {
            values.push(f32::from_le_bytes(chunk.try_into().unwrap()));
        }
        Ok(Self { dimension, values })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HnswGraph {
    pub node_count: u64,
    pub layers: Vec<LayerBlock>,
}

impl HnswGraph {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.layers.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.node_count.to_le_bytes());
        for layer in &self.layers {
            out.extend_from_slice(&layer.encode());
        }
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < 10 {
            return Err(FormatError::TooShort {
                context: "hnsw graph",
                needed: 10,
                actual: input.len(),
            });
        }
        let layer_count = u16::from_le_bytes(input[0..2].try_into().unwrap()) as usize;
        let node_count = u64::from_le_bytes(input[2..10].try_into().unwrap());
        let mut cursor = 10;
        let mut layers = Vec::with_capacity(layer_count);
        for _ in 0..layer_count {
            let (layer, used) = LayerBlock::decode(&input[cursor..])?;
            layers.push(layer);
            cursor += used;
        }
        if cursor != input.len() {
            return Err(FormatError::InvalidDeclaredLength {
                context: "hnsw graph trailing bytes",
            });
        }
        Ok(Self { node_count, layers })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LayerBlock {
    pub layer_index: u16,
    pub node_adjacencies: Vec<NodeAdjacency>,
}

impl LayerBlock {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&self.layer_index.to_le_bytes());
        out.extend_from_slice(&(self.node_adjacencies.len() as u64).to_le_bytes());
        for adjacency in &self.node_adjacencies {
            out.extend_from_slice(&adjacency.encode());
        }
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < 10 {
            return Err(FormatError::TooShort {
                context: "hnsw layer block",
                needed: 10,
                actual: input.len(),
            });
        }
        let layer_index = u16::from_le_bytes(input[0..2].try_into().unwrap());
        let node_adjacency_count = u64::from_le_bytes(input[2..10].try_into().unwrap()) as usize;
        let mut cursor = 10;
        let mut node_adjacencies = Vec::with_capacity(node_adjacency_count);
        for _ in 0..node_adjacency_count {
            let (adjacency, used) = NodeAdjacency::decode(&input[cursor..])?;
            node_adjacencies.push(adjacency);
            cursor += used;
        }
        Ok((
            Self {
                layer_index,
                node_adjacencies,
            },
            cursor,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeAdjacency {
    pub vector_id: u64,
    pub neighbors: Vec<u64>,
}

impl NodeAdjacency {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 2 + self.neighbors.len() * 8);
        out.extend_from_slice(&self.vector_id.to_le_bytes());
        out.extend_from_slice(&(self.neighbors.len() as u16).to_le_bytes());
        for neighbor in &self.neighbors {
            out.extend_from_slice(&neighbor.to_le_bytes());
        }
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < 10 {
            return Err(FormatError::TooShort {
                context: "hnsw node adjacency",
                needed: 10,
                actual: input.len(),
            });
        }
        let neighbor_count = u16::from_le_bytes(input[8..10].try_into().unwrap()) as usize;
        let neighbor_bytes =
            neighbor_count
                .checked_mul(8)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "hnsw neighbors",
                })?;
        let record_end =
            10usize
                .checked_add(neighbor_bytes)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "hnsw node adjacency",
                })?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "hnsw neighbors",
                needed: record_end,
                actual: input.len(),
            });
        }
        let mut neighbors = Vec::with_capacity(neighbor_count);
        let mut cursor = 10;
        for _ in 0..neighbor_count {
            neighbors.push(u64::from_le_bytes(
                input[cursor..cursor + 8].try_into().unwrap(),
            ));
            cursor += 8;
        }
        Ok((
            Self {
                vector_id: u64::from_le_bytes(input[0..8].try_into().unwrap()),
                neighbors,
            },
            record_end,
        ))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VectorMetric {
    Cosine = 1,
    Dot = 2,
    L2 = 3,
}

impl VectorMetric {
    pub fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            1 => Ok(Self::Cosine),
            2 => Ok(Self::Dot),
            3 => Ok(Self::L2),
            other => Err(FormatError::UnsupportedVectorMetric(other)),
        }
    }

    pub fn from_name(value: &str) -> Result<Self, FormatError> {
        match value {
            "cosine" => Ok(Self::Cosine),
            "dot" => Ok(Self::Dot),
            "l2" => Ok(Self::L2),
            _ => Err(FormatError::InvalidVectorIndexDefinition { field: "metric" }),
        }
    }

    pub fn as_name(self) -> &'static str {
        match self {
            Self::Cosine => "cosine",
            Self::Dot => "dot",
            Self::L2 => "l2",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum VectorModality {
    Text = 1,
    Image = 2,
    Audio = 3,
    Video = 4,
}

impl VectorModality {
    pub fn from_u8(value: u8) -> Result<Self, FormatError> {
        match value {
            1 => Ok(Self::Text),
            2 => Ok(Self::Image),
            3 => Ok(Self::Audio),
            4 => Ok(Self::Video),
            other => Err(FormatError::UnsupportedVectorModality(other)),
        }
    }

    pub fn from_name(value: &str) -> Result<Self, FormatError> {
        match value {
            "text" => Ok(Self::Text),
            "image" => Ok(Self::Image),
            "audio" => Ok(Self::Audio),
            "video" => Ok(Self::Video),
            _ => Err(FormatError::InvalidVectorIndexDefinition { field: "modality" }),
        }
    }

    pub fn as_name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Image => "image",
            Self::Audio => "audio",
            Self::Video => "video",
        }
    }
}

pub const DEFAULT_HNSW_M: u16 = 32;
pub const DEFAULT_HNSW_EF_CONSTRUCTION: u16 = 200;
pub const DEFAULT_HNSW_EF_SEARCH: u16 = 80;

#[derive(Debug, Clone, PartialEq)]
pub struct VectorIndexDefinition {
    pub dimension: u16,
    pub metric: VectorMetric,
    pub modality: VectorModality,
    pub embedding_model: String,
    pub chunking: serde_json::Value,
    pub hnsw_m: u16,
    pub hnsw_ef_construction: u16,
    pub hnsw_ef_search_default: u16,
}

impl VectorIndexDefinition {
    pub fn from_json(value: &serde_json::Value) -> Result<Self, FormatError> {
        let object = value
            .as_object()
            .ok_or(FormatError::InvalidVectorIndexDefinition { field: "root" })?;
        let dimension = required_u16(object, "dimension")?;
        if dimension == 0 {
            return Err(FormatError::InvalidVectorIndexDefinition { field: "dimension" });
        }
        let metric = VectorMetric::from_name(required_str(object, "metric")?)?;
        let modality = VectorModality::from_name(required_str(object, "modality")?)?;
        let embedding_model = required_str(object, "embedding_model")?.to_string();
        if embedding_model.trim().is_empty() {
            return Err(FormatError::InvalidVectorIndexDefinition {
                field: "embedding_model",
            });
        }
        let chunking = object
            .get("chunking")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or(FormatError::InvalidVectorIndexDefinition { field: "chunking" })?;
        let hnsw_m = optional_u16(object, "hnsw_m", DEFAULT_HNSW_M)?;
        let hnsw_ef_construction =
            optional_u16(object, "hnsw_ef_construction", DEFAULT_HNSW_EF_CONSTRUCTION)?;
        let hnsw_ef_search_default =
            optional_u16(object, "hnsw_ef_search_default", DEFAULT_HNSW_EF_SEARCH)?;
        if hnsw_m == 0 {
            return Err(FormatError::InvalidVectorIndexDefinition { field: "hnsw_m" });
        }
        if hnsw_ef_construction == 0 {
            return Err(FormatError::InvalidVectorIndexDefinition {
                field: "hnsw_ef_construction",
            });
        }
        if hnsw_ef_search_default == 0 {
            return Err(FormatError::InvalidVectorIndexDefinition {
                field: "hnsw_ef_search_default",
            });
        }
        Ok(Self {
            dimension,
            metric,
            modality,
            embedding_model,
            chunking,
            hnsw_m,
            hnsw_ef_construction,
            hnsw_ef_search_default,
        })
    }
}

fn required_str<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<&'a str, FormatError> {
    object
        .get(field)
        .and_then(serde_json::Value::as_str)
        .ok_or(FormatError::InvalidVectorIndexDefinition { field })
}

fn required_u16(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<u16, FormatError> {
    let value = object
        .get(field)
        .and_then(serde_json::Value::as_u64)
        .ok_or(FormatError::InvalidVectorIndexDefinition { field })?;
    u16::try_from(value).map_err(|_| FormatError::InvalidVectorIndexDefinition { field })
}

fn optional_u16(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
    default: u16,
) -> Result<u16, FormatError> {
    match object.get(field) {
        Some(value) => {
            let value = value
                .as_u64()
                .ok_or(FormatError::InvalidVectorIndexDefinition { field })?;
            u16::try_from(value).map_err(|_| FormatError::InvalidVectorIndexDefinition { field })
        }
        None => Ok(default),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchCandidate {
    pub record: VectorRecord,
    pub values: Vec<f32>,
    pub authorized: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchResult {
    pub vector_id: u64,
    pub score: f32,
    pub object_version_id: [u8; 16],
    pub chunk_id: u32,
    pub source_start: u64,
    pub source_len: u32,
}

pub fn vector_score(
    query: &[f32],
    candidate: &[f32],
    metric: VectorMetric,
) -> Result<f32, FormatError> {
    if query.is_empty() || query.len() != candidate.len() {
        return Err(FormatError::InvalidDeclaredLength {
            context: "vector query dimension",
        });
    }

    match metric {
        VectorMetric::Dot => Ok(dot_product(query, candidate)),
        VectorMetric::Cosine => {
            let query_norm = dot_product(query, query).sqrt();
            let candidate_norm = dot_product(candidate, candidate).sqrt();
            if query_norm == 0.0 || candidate_norm == 0.0 {
                return Ok(0.0);
            }
            Ok(dot_product(query, candidate) / (query_norm * candidate_norm))
        }
        VectorMetric::L2 => {
            let distance = query
                .iter()
                .zip(candidate.iter())
                .map(|(left, right)| {
                    let delta = left - right;
                    delta * delta
                })
                .sum::<f32>()
                .sqrt();
            Ok(-distance)
        }
    }
}

pub fn select_authorized_vector_results(
    query: &[f32],
    candidates: &[VectorSearchCandidate],
    metric: VectorMetric,
    result_count: usize,
) -> Result<Vec<VectorSearchResult>, FormatError> {
    if result_count == 0 {
        return Ok(Vec::new());
    }

    let max_candidate_count = result_count.saturating_mul(20).max(result_count);
    let mut scored = candidates
        .iter()
        .map(|candidate| {
            Ok((
                vector_score(query, &candidate.values, metric)?,
                candidate.authorized,
                candidate,
            ))
        })
        .collect::<Result<Vec<_>, FormatError>>()?;
    scored.sort_by(|left, right| {
        right
            .0
            .partial_cmp(&left.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.2.record.vector_id.cmp(&right.2.record.vector_id))
    });

    Ok(scored
        .into_iter()
        .take(max_candidate_count)
        .filter(|(_, authorized, _)| *authorized)
        .take(result_count)
        .map(|(score, _, candidate)| VectorSearchResult {
            vector_id: candidate.record.vector_id,
            score,
            object_version_id: candidate.record.object_version_id,
            chunk_id: candidate.record.chunk_id,
            source_start: candidate.record.source_start,
            source_len: candidate.record.source_len,
        })
        .collect())
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn vector_body_header_round_trip() {
        let header = VectorBodyHeader {
            vector_count: 10,
            vector_table_offset: 40,
            vector_payload_offset: 400,
            hnsw_graph_offset: 800,
            deleted_bitset_offset: 1200,
        };
        assert_eq!(VectorBodyHeader::decode(&header.encode()).unwrap(), header);
    }

    #[test]
    fn vector_record_round_trip_uses_fixed_layout() {
        let record = VectorRecord {
            vector_id: 1,
            object_version_id: [2; 16],
            chunk_id: 3,
            modality: 4,
            metric: 5,
            dimension: 3,
            vector_payload_offset: 100,
            source_start: 200,
            source_len: 50,
            authz_label_hash: [6; 32],
            metadata_filter_bits: 0b1010,
        };
        let encoded = record.encode();
        let (decoded, used) = VectorRecord::decode(&encoded).unwrap();
        assert_eq!(used, VECTOR_RECORD_LEN);
        assert_eq!(decoded, record);
    }

    #[test]
    fn vector_payload_round_trip_requires_exact_dimension() {
        let payload = VectorPayload {
            dimension: 3,
            values: vec![0.1, 0.2, 0.3],
        };
        let encoded = payload.encode().unwrap();
        assert_eq!(VectorPayload::decode(&encoded, 3).unwrap(), payload);
        assert_eq!(
            VectorPayload::decode(&encoded, 2).unwrap_err(),
            FormatError::InvalidDeclaredLength {
                context: "vector payload length"
            }
        );
    }

    #[test]
    fn hnsw_graph_round_trip_preserves_layer_adjacency() {
        let graph = HnswGraph {
            node_count: 3,
            layers: vec![LayerBlock {
                layer_index: 0,
                node_adjacencies: vec![
                    NodeAdjacency {
                        vector_id: 1,
                        neighbors: vec![2, 3],
                    },
                    NodeAdjacency {
                        vector_id: 2,
                        neighbors: vec![1],
                    },
                ],
            }],
        };
        assert_eq!(HnswGraph::decode(&graph.encode()).unwrap(), graph);
    }

    #[test]
    fn vector_metric_and_modality_decode_supported_values() {
        assert_eq!(VectorMetric::from_u8(1).unwrap(), VectorMetric::Cosine);
        assert_eq!(VectorMetric::from_u8(2).unwrap(), VectorMetric::Dot);
        assert_eq!(VectorMetric::from_u8(3).unwrap(), VectorMetric::L2);
        assert_eq!(
            VectorMetric::from_name("cosine").unwrap(),
            VectorMetric::Cosine
        );
        assert_eq!(VectorMetric::from_name("dot").unwrap(), VectorMetric::Dot);
        assert_eq!(VectorMetric::from_name("l2").unwrap(), VectorMetric::L2);
        assert_eq!(VectorMetric::L2.as_name(), "l2");
        assert_eq!(VectorModality::from_u8(1).unwrap(), VectorModality::Text);
        assert_eq!(VectorModality::from_u8(2).unwrap(), VectorModality::Image);
        assert_eq!(VectorModality::from_u8(3).unwrap(), VectorModality::Audio);
        assert_eq!(VectorModality::from_u8(4).unwrap(), VectorModality::Video);
        assert_eq!(
            VectorModality::from_name("text").unwrap(),
            VectorModality::Text
        );
        assert_eq!(
            VectorModality::from_name("image").unwrap(),
            VectorModality::Image
        );
        assert_eq!(
            VectorModality::from_name("audio").unwrap(),
            VectorModality::Audio
        );
        assert_eq!(
            VectorModality::from_name("video").unwrap(),
            VectorModality::Video
        );
        assert_eq!(VectorModality::Video.as_name(), "video");
        assert_eq!(
            VectorMetric::from_u8(99).unwrap_err(),
            FormatError::UnsupportedVectorMetric(99)
        );
        assert_eq!(
            VectorModality::from_u8(99).unwrap_err(),
            FormatError::UnsupportedVectorModality(99)
        );
    }

    #[test]
    fn vector_index_definition_parses_required_shape_and_defaults() {
        let definition = VectorIndexDefinition::from_json(&serde_json::json!({
            "dimension": 768,
            "metric": "cosine",
            "modality": "text",
            "embedding_model": "text-embedding-v1",
            "chunking": {
                "kind": "tokens",
                "max_tokens": 512,
                "overlap_tokens": 64
            }
        }))
        .unwrap();

        assert_eq!(definition.dimension, 768);
        assert_eq!(definition.metric, VectorMetric::Cosine);
        assert_eq!(definition.modality, VectorModality::Text);
        assert_eq!(definition.embedding_model, "text-embedding-v1");
        assert_eq!(definition.chunking["kind"], "tokens");
        assert_eq!(definition.hnsw_m, DEFAULT_HNSW_M);
        assert_eq!(
            definition.hnsw_ef_construction,
            DEFAULT_HNSW_EF_CONSTRUCTION
        );
        assert_eq!(definition.hnsw_ef_search_default, DEFAULT_HNSW_EF_SEARCH);
    }

    #[test]
    fn vector_index_definition_accepts_all_modalities_metrics_and_explicit_hnsw() {
        for (metric, expected_metric) in [
            ("cosine", VectorMetric::Cosine),
            ("dot", VectorMetric::Dot),
            ("l2", VectorMetric::L2),
        ] {
            for (modality, expected_modality) in [
                ("text", VectorModality::Text),
                ("image", VectorModality::Image),
                ("audio", VectorModality::Audio),
                ("video", VectorModality::Video),
            ] {
                let definition = VectorIndexDefinition::from_json(&serde_json::json!({
                    "dimension": 1024,
                    "metric": metric,
                    "modality": modality,
                    "embedding_model": format!("{modality}-embedding-v1"),
                    "chunking": {"kind": "fixed_bytes", "max_bytes": 65536},
                    "hnsw_m": 48,
                    "hnsw_ef_construction": 320,
                    "hnsw_ef_search_default": 96
                }))
                .unwrap();
                assert_eq!(definition.metric, expected_metric);
                assert_eq!(definition.modality, expected_modality);
                assert_eq!(definition.hnsw_m, 48);
                assert_eq!(definition.hnsw_ef_construction, 320);
                assert_eq!(definition.hnsw_ef_search_default, 96);
            }
        }
    }

    #[test]
    fn vector_index_definition_rejects_invalid_shapes() {
        for (field, value) in [
            ("root", serde_json::json!("not an object")),
            (
                "dimension",
                serde_json::json!({
                    "dimension": 0,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {}
                }),
            ),
            (
                "metric",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "manhattan",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {}
                }),
            ),
            (
                "modality",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "cosine",
                    "modality": "binary",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {}
                }),
            ),
            (
                "embedding_model",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "   ",
                    "chunking": {}
                }),
            ),
            (
                "chunking",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": "none"
                }),
            ),
            (
                "hnsw_m",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {},
                    "hnsw_m": 0
                }),
            ),
            (
                "hnsw_ef_construction",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {},
                    "hnsw_ef_construction": 0
                }),
            ),
            (
                "hnsw_ef_search_default",
                serde_json::json!({
                    "dimension": 1,
                    "metric": "cosine",
                    "modality": "text",
                    "embedding_model": "text-embedding-v1",
                    "chunking": {},
                    "hnsw_ef_search_default": 0
                }),
            ),
        ] {
            assert_eq!(
                VectorIndexDefinition::from_json(&value).unwrap_err(),
                FormatError::InvalidVectorIndexDefinition { field }
            );
        }
    }

    #[test]
    fn vector_score_supports_cosine_dot_and_l2() {
        let query = [1.0, 0.0, 0.0];
        let same = [1.0, 0.0, 0.0];
        let orthogonal = [0.0, 1.0, 0.0];

        assert_eq!(
            vector_score(&query, &same, VectorMetric::Cosine).unwrap(),
            1.0
        );
        assert_eq!(
            vector_score(&query, &orthogonal, VectorMetric::Dot).unwrap(),
            0.0
        );
        assert!(vector_score(&query, &orthogonal, VectorMetric::L2).unwrap() < 0.0);
        assert_eq!(
            vector_score(&query, &[1.0, 0.0], VectorMetric::Cosine).unwrap_err(),
            FormatError::InvalidDeclaredLength {
                context: "vector query dimension"
            }
        );
    }

    #[test]
    fn vector_result_selection_applies_authorization_after_scoring() {
        let query = [1.0, 0.0];
        let candidates = vec![
            candidate(1, [1.0, 0.0], false),
            candidate(2, [0.9, 0.1], true),
            candidate(3, [0.0, 1.0], true),
        ];

        let results =
            select_authorized_vector_results(&query, &candidates, VectorMetric::Cosine, 2).unwrap();
        assert_eq!(
            results
                .iter()
                .map(|result| result.vector_id)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(results[0].score > results[1].score);
    }

    #[test]
    fn vector_result_selection_respects_candidate_multiplier_limit() {
        let query = [1.0, 0.0];
        let mut candidates = (0..20)
            .map(|idx| candidate(idx + 1, [1.0 - idx as f32 * 0.001, 0.0], false))
            .collect::<Vec<_>>();
        candidates.push(candidate(100, [0.1, 0.0], true));

        let results =
            select_authorized_vector_results(&query, &candidates, VectorMetric::Dot, 1).unwrap();
        assert!(results.is_empty());
    }

    fn candidate(vector_id: u64, values: [f32; 2], authorized: bool) -> VectorSearchCandidate {
        VectorSearchCandidate {
            record: VectorRecord {
                vector_id,
                object_version_id: [vector_id as u8; 16],
                chunk_id: vector_id as u32,
                modality: VectorModality::Text as u8,
                metric: VectorMetric::Cosine as u8,
                dimension: 2,
                vector_payload_offset: 0,
                source_start: vector_id * 10,
                source_len: 10,
                authz_label_hash: [0; 32],
                metadata_filter_bits: 0,
            },
            values: values.to_vec(),
            authorized,
        }
    }
}
