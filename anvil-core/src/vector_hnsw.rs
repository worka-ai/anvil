use crate::formats::{
    FormatError, Hash32,
    vector::{
        HnswGraph, LayerBlock, NodeAdjacency, VectorMetric, VectorSearchResult, vector_score,
    },
};
use crate::vector_segment::{DecodedVectorSegment, VectorSegmentEntry};
use hnsw_rs::{anndists::dist::distances::Distance, hnsw::Hnsw};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_MAX_LAYER: usize = 16;
const DEFAULT_EF_SEARCH: usize = 80;
const MAX_HNSW_CONNECTIONS: usize = 256;
const MAX_AUTHZ_CANDIDATE_MULTIPLIER: usize = 20;

pub trait VectorIndexEngine {
    fn query_segment(
        &self,
        segment: &DecodedVectorSegment,
        query: &[f32],
        metric: VectorMetric,
        authorized_labels: Option<&BTreeSet<Hash32>>,
        limit: usize,
    ) -> Result<Vec<VectorSearchResult>, FormatError>;
}

pub fn build_hnsw_graph_for_entries(
    entries: &[VectorSegmentEntry],
    metric: VectorMetric,
    hnsw_m: u16,
    hnsw_ef_construction: u16,
) -> Result<HnswGraph, FormatError> {
    if entries.is_empty() {
        return Ok(HnswGraph {
            node_count: 0,
            layers: Vec::new(),
        });
    }
    if usize::from(hnsw_m) > MAX_HNSW_CONNECTIONS {
        return Err(FormatError::InvalidDeclaredLength { context: "hnsw_m" });
    }

    match metric {
        VectorMetric::Cosine => {
            build_graph_with_distance::<AnvilCosineDistance>(entries, hnsw_m, hnsw_ef_construction)
        }
        VectorMetric::Dot => {
            build_graph_with_distance::<AnvilDotDistance>(entries, hnsw_m, hnsw_ef_construction)
        }
        VectorMetric::L2 => {
            build_graph_with_distance::<AnvilL2Distance>(entries, hnsw_m, hnsw_ef_construction)
        }
    }
}

pub fn validate_hnsw_graph(
    graph: &HnswGraph,
    entries: &[VectorSegmentEntry],
    hnsw_m: u16,
) -> Result<(), FormatError> {
    if graph.node_count != entries.len() as u64 {
        return Err(FormatError::InvalidDeclaredLength {
            context: "hnsw graph node count",
        });
    }
    if usize::from(hnsw_m) > MAX_HNSW_CONNECTIONS {
        return Err(FormatError::InvalidDeclaredLength { context: "hnsw_m" });
    }

    let known_ids = entries
        .iter()
        .map(|entry| entry.record.vector_id)
        .collect::<BTreeSet<_>>();
    let mut seen_layers = BTreeSet::new();
    for layer in &graph.layers {
        if !seen_layers.insert(layer.layer_index) {
            return Err(FormatError::InvalidDeclaredLength {
                context: "hnsw duplicate layer",
            });
        }
        let mut seen_nodes = BTreeSet::new();
        for adjacency in &layer.node_adjacencies {
            if !known_ids.contains(&adjacency.vector_id) {
                return Err(FormatError::InvalidDeclaredLength {
                    context: "hnsw unknown node",
                });
            }
            if !seen_nodes.insert(adjacency.vector_id) {
                return Err(FormatError::InvalidDeclaredLength {
                    context: "hnsw duplicate node",
                });
            }
            if adjacency.neighbors.len() > usize::from(hnsw_m) {
                return Err(FormatError::InvalidDeclaredLength {
                    context: "hnsw neighbor count",
                });
            }
            let mut seen_neighbors = BTreeSet::new();
            for neighbor in &adjacency.neighbors {
                if *neighbor == adjacency.vector_id {
                    return Err(FormatError::InvalidDeclaredLength {
                        context: "hnsw self neighbor",
                    });
                }
                if !known_ids.contains(neighbor) {
                    return Err(FormatError::InvalidDeclaredLength {
                        context: "hnsw unknown neighbor",
                    });
                }
                if !seen_neighbors.insert(*neighbor) {
                    return Err(FormatError::InvalidDeclaredLength {
                        context: "hnsw duplicate neighbor",
                    });
                }
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswRsVectorIndexEngine {
    max_candidate_multiplier: usize,
    max_layer: usize,
    ef_search_floor: usize,
}

impl Default for HnswRsVectorIndexEngine {
    fn default() -> Self {
        Self {
            max_candidate_multiplier: MAX_AUTHZ_CANDIDATE_MULTIPLIER,
            max_layer: DEFAULT_MAX_LAYER,
            ef_search_floor: DEFAULT_EF_SEARCH,
        }
    }
}

impl VectorIndexEngine for HnswRsVectorIndexEngine {
    fn query_segment(
        &self,
        segment: &DecodedVectorSegment,
        query: &[f32],
        metric: VectorMetric,
        authorized_labels: Option<&BTreeSet<Hash32>>,
        limit: usize,
    ) -> Result<Vec<VectorSearchResult>, FormatError> {
        if limit == 0 || segment.entries.is_empty() {
            return Ok(Vec::new());
        }
        if query.len() != segment.header.dimension as usize || query.is_empty() {
            return Err(FormatError::InvalidDeclaredLength {
                context: "vector query dimension",
            });
        }
        if segment.header.hnsw_m as usize > MAX_HNSW_CONNECTIONS {
            return Err(FormatError::InvalidDeclaredLength { context: "hnsw_m" });
        }

        match metric {
            VectorMetric::Cosine => self.query_with_distance::<AnvilCosineDistance>(
                segment,
                query,
                metric,
                authorized_labels,
                limit,
            ),
            VectorMetric::Dot => self.query_with_distance::<AnvilDotDistance>(
                segment,
                query,
                metric,
                authorized_labels,
                limit,
            ),
            VectorMetric::L2 => self.query_with_distance::<AnvilL2Distance>(
                segment,
                query,
                metric,
                authorized_labels,
                limit,
            ),
        }
    }
}

impl HnswRsVectorIndexEngine {
    fn query_with_distance<D>(
        &self,
        segment: &DecodedVectorSegment,
        query: &[f32],
        metric: VectorMetric,
        authorized_labels: Option<&BTreeSet<Hash32>>,
        limit: usize,
    ) -> Result<Vec<VectorSearchResult>, FormatError>
    where
        D: Distance<f32> + Default + Send + Sync,
    {
        let candidate_count = limit
            .saturating_mul(self.max_candidate_multiplier)
            .max(limit)
            .min(segment.entries.len());
        let max_connections = usize::from(segment.header.hnsw_m).max(1);
        let ef_construction = usize::from(segment.header.hnsw_ef_construction).max(max_connections);
        let hnsw = Hnsw::<f32, D>::new(
            max_connections,
            segment.entries.len(),
            self.max_layer,
            ef_construction,
            D::default(),
        );
        for (idx, entry) in segment.entries.iter().enumerate() {
            hnsw.insert((&entry.payload.values, idx));
        }

        let ef_search = self
            .ef_search_floor
            .max(candidate_count)
            .max(max_connections);
        let neighbours = hnsw.search(query, candidate_count, ef_search);
        let mut seen = vec![false; segment.entries.len()];
        let mut candidate_indexes = Vec::with_capacity(candidate_count);
        for neighbour in neighbours {
            if neighbour.d_id < segment.entries.len() && !seen[neighbour.d_id] {
                seen[neighbour.d_id] = true;
                candidate_indexes.push(neighbour.d_id);
            }
        }
        if candidate_indexes.len() < candidate_count {
            let mut missing = segment
                .entries
                .iter()
                .enumerate()
                .filter(|(idx, _)| !seen[*idx])
                .map(|(idx, entry)| Ok((vector_score(query, &entry.payload.values, metric)?, idx)))
                .collect::<Result<Vec<_>, FormatError>>()?;
            missing.sort_by(|left, right| {
                right
                    .0
                    .partial_cmp(&left.0)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| {
                        segment.entries[left.1]
                            .record
                            .vector_id
                            .cmp(&segment.entries[right.1].record.vector_id)
                    })
            });
            candidate_indexes.extend(
                missing
                    .into_iter()
                    .take(candidate_count - candidate_indexes.len())
                    .map(|(_, idx)| idx),
            );
        }
        let mut results = Vec::with_capacity(limit.min(candidate_indexes.len()));
        for idx in candidate_indexes {
            let entry = &segment.entries[idx];
            if !is_authorized(entry.record.authz_label_hash, authorized_labels) {
                continue;
            }
            results.push(result_from_entry(query, metric, entry)?);
        }
        results.sort_by(|left, right| {
            right
                .score
                .partial_cmp(&left.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.vector_id.cmp(&right.vector_id))
        });
        results.truncate(limit);
        Ok(results)
    }
}

fn build_graph_with_distance<D>(
    entries: &[VectorSegmentEntry],
    hnsw_m: u16,
    hnsw_ef_construction: u16,
) -> Result<HnswGraph, FormatError>
where
    D: Distance<f32> + Default + Send + Sync,
{
    let max_connections = usize::from(hnsw_m).max(1);
    let ef_construction = usize::from(hnsw_ef_construction).max(max_connections);
    let hnsw = Hnsw::<f32, D>::new(
        max_connections,
        entries.len(),
        DEFAULT_MAX_LAYER,
        ef_construction,
        D::default(),
    );
    for (idx, entry) in entries.iter().enumerate() {
        hnsw.insert((&entry.payload.values, idx));
    }
    let graph = graph_from_hnsw(&hnsw, entries)?;
    validate_hnsw_graph(&graph, entries, hnsw_m)?;
    Ok(graph)
}

fn graph_from_hnsw<D>(
    hnsw: &Hnsw<'_, f32, D>,
    entries: &[VectorSegmentEntry],
) -> Result<HnswGraph, FormatError>
where
    D: Distance<f32> + Send + Sync,
{
    let vector_ids = entries
        .iter()
        .map(|entry| entry.record.vector_id)
        .collect::<Vec<_>>();
    let mut layers = Vec::new();
    for layer_index in 0..=usize::from(hnsw.get_max_level_observed()) {
        let mut nodes_by_id = BTreeMap::new();
        for point in hnsw.get_point_indexation().get_layer_iterator(layer_index) {
            let origin_id = point.get_origin_id();
            let vector_id =
                *vector_ids
                    .get(origin_id)
                    .ok_or(FormatError::InvalidDeclaredLength {
                        context: "hnsw origin id",
                    })?;
            let neighborhoods = point.get_neighborhood_id();
            let neighbors_for_layer =
                neighborhoods
                    .get(layer_index)
                    .ok_or(FormatError::InvalidDeclaredLength {
                        context: "hnsw layer",
                    })?;
            let mut neighbors = neighbors_for_layer
                .iter()
                .map(|neighbor| {
                    vector_ids.get(neighbor.d_id).copied().ok_or(
                        FormatError::InvalidDeclaredLength {
                            context: "hnsw neighbor id",
                        },
                    )
                })
                .collect::<Result<Vec<_>, FormatError>>()?;
            neighbors.sort_unstable();
            neighbors.dedup();
            nodes_by_id.insert(
                vector_id,
                NodeAdjacency {
                    vector_id,
                    neighbors,
                },
            );
        }
        if !nodes_by_id.is_empty() {
            layers.push(LayerBlock {
                layer_index: layer_index as u16,
                node_adjacencies: nodes_by_id.into_values().collect(),
            });
        }
    }
    Ok(HnswGraph {
        node_count: entries.len() as u64,
        layers,
    })
}

#[derive(Default, Debug, Clone, Copy)]
struct AnvilL2Distance;

impl Distance<f32> for AnvilL2Distance {
    fn eval(&self, left: &[f32], right: &[f32]) -> f32 {
        left.iter()
            .zip(right.iter())
            .map(|(left, right)| {
                let delta = left - right;
                delta * delta
            })
            .sum::<f32>()
            .sqrt()
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct AnvilCosineDistance;

impl Distance<f32> for AnvilCosineDistance {
    fn eval(&self, left: &[f32], right: &[f32]) -> f32 {
        let left_norm = dot_product(left, left).sqrt();
        let right_norm = dot_product(right, right).sqrt();
        if left_norm == 0.0 || right_norm == 0.0 {
            return 1.0;
        }
        1.0 - (dot_product(left, right) / (left_norm * right_norm))
    }
}

#[derive(Default, Debug, Clone, Copy)]
struct AnvilDotDistance;

impl Distance<f32> for AnvilDotDistance {
    fn eval(&self, left: &[f32], right: &[f32]) -> f32 {
        let dot = dot_product(left, right);
        if dot >= 80.0 {
            0.0
        } else if dot <= -80.0 {
            1.0
        } else {
            1.0 / (1.0 + dot.exp())
        }
    }
}

fn result_from_entry(
    query: &[f32],
    metric: VectorMetric,
    entry: &VectorSegmentEntry,
) -> Result<VectorSearchResult, FormatError> {
    Ok(VectorSearchResult {
        vector_id: entry.record.vector_id,
        score: vector_score(query, &entry.payload.values, metric)?,
        object_version_id: entry.record.object_version_id,
        chunk_id: entry.record.chunk_id,
        source_start: entry.record.source_start,
        source_len: entry.record.source_len,
    })
}

fn dot_product(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum()
}

fn is_authorized(label: Hash32, authorized_labels: Option<&BTreeSet<Hash32>>) -> bool {
    authorized_labels.is_none_or(|labels| labels.contains(&label))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::vector::{
        HnswGraph, LayerBlock, NodeAdjacency, VectorModality, VectorPayload, VectorRecord,
    };
    use crate::vector_segment::{DecodedVectorSegment, VectorSegmentHeader};

    #[test]
    fn hnsw_query_filters_authorized_candidates_after_candidate_expansion() {
        let allowed = [1; 32];
        let denied = [2; 32];
        let segment = segment_with_entries(vec![
            entry(1, [1.0, 0.0], denied),
            entry(2, [0.95, 0.05], denied),
            entry(3, [0.9, 0.1], allowed),
            entry(4, [0.0, 1.0], allowed),
        ]);
        let mut labels = BTreeSet::new();
        labels.insert(allowed);

        let results = HnswRsVectorIndexEngine::default()
            .query_segment(
                &segment,
                &[1.0, 0.0],
                VectorMetric::Cosine,
                Some(&labels),
                1,
            )
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].vector_id, 3);
    }

    #[test]
    fn hnsw_query_supports_l2_and_dot_metrics() {
        let allowed = [1; 32];
        let segment = segment_with_entries(vec![
            entry(1, [0.0, 1.0], allowed),
            entry(2, [1.0, 0.0], allowed),
            entry(3, [0.5, 0.5], allowed),
        ]);
        let engine = HnswRsVectorIndexEngine::default();

        let l2 = engine
            .query_segment(&segment, &[0.0, 1.0], VectorMetric::L2, None, 1)
            .unwrap();
        assert_eq!(l2[0].vector_id, 1);

        let dot = engine
            .query_segment(&segment, &[1.0, 0.0], VectorMetric::Dot, None, 1)
            .unwrap();
        assert_eq!(dot[0].vector_id, 2);
    }

    #[test]
    fn generated_hnsw_graph_uses_segment_vector_ids() {
        let allowed = [1; 32];
        let entries = vec![
            entry(100, [1.0, 0.0], allowed),
            entry(200, [0.5, 0.5], allowed),
            entry(300, [0.0, 1.0], allowed),
        ];

        let graph = build_hnsw_graph_for_entries(&entries, VectorMetric::Cosine, 16, 80).unwrap();

        assert_eq!(graph.node_count, 3);
        assert!(!graph.layers.is_empty());
        let known_ids = entries
            .iter()
            .map(|entry| entry.record.vector_id)
            .collect::<BTreeSet<_>>();
        for layer in &graph.layers {
            for node in &layer.node_adjacencies {
                assert!(known_ids.contains(&node.vector_id));
                for neighbor in &node.neighbors {
                    assert!(known_ids.contains(neighbor));
                }
            }
        }
    }

    #[test]
    fn hnsw_graph_validation_rejects_malformed_adjacency() {
        let allowed = [1; 32];
        let entries = vec![entry(1, [1.0, 0.0], allowed), entry(2, [0.0, 1.0], allowed)];
        let unknown_neighbor = HnswGraph {
            node_count: 2,
            layers: vec![LayerBlock {
                layer_index: 0,
                node_adjacencies: vec![NodeAdjacency {
                    vector_id: 1,
                    neighbors: vec![3],
                }],
            }],
        };
        assert!(validate_hnsw_graph(&unknown_neighbor, &entries, 16).is_err());

        let duplicate_layer = HnswGraph {
            node_count: 2,
            layers: vec![
                LayerBlock {
                    layer_index: 0,
                    node_adjacencies: Vec::new(),
                },
                LayerBlock {
                    layer_index: 0,
                    node_adjacencies: Vec::new(),
                },
            ],
        };
        assert!(validate_hnsw_graph(&duplicate_layer, &entries, 16).is_err());
    }

    fn segment_with_entries(entries: Vec<VectorSegmentEntry>) -> DecodedVectorSegment {
        DecodedVectorSegment {
            header: VectorSegmentHeader {
                index_id: "vectors".to_string(),
                generation: 1,
                dimension: 2,
                metric: "cosine".to_string(),
                embedding_model: "test-model".to_string(),
                modality: "text".to_string(),
                hnsw_m: 16,
                hnsw_ef_construction: 80,
                source_cursor: 0,
                authz_revision: 0,
                codec: "none".to_string(),
                created_at: "2026-06-28T00:00:00.000000000Z".to_string(),
            },
            body_header: crate::formats::vector::VectorBodyHeader {
                vector_count: entries.len() as u64,
                vector_table_offset: 0,
                vector_payload_offset: 0,
                hnsw_graph_offset: 0,
                deleted_bitset_offset: 0,
            },
            entries,
            hnsw_graph: HnswGraph {
                node_count: 0,
                layers: Vec::new(),
            },
            deleted_bitset: Vec::new(),
        }
    }

    fn entry(vector_id: u64, values: [f32; 2], authz_label_hash: Hash32) -> VectorSegmentEntry {
        VectorSegmentEntry {
            record: VectorRecord {
                vector_id,
                object_version_id: [vector_id as u8; 16],
                chunk_id: 0,
                modality: VectorModality::Text as u8,
                metric: VectorMetric::Cosine as u8,
                dimension: 2,
                vector_payload_offset: 0,
                source_start: 0,
                source_len: 0,
                authz_label_hash,
                metadata_filter_bits: 0,
            },
            payload: VectorPayload {
                dimension: 2,
                values: values.to_vec(),
            },
        }
    }
}
