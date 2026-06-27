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
}
