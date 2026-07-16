use super::*;

#[derive(Debug, Clone)]
pub(super) struct HybridAccum {
    pub(super) object_version_id: [u8; 16],
    pub(super) object_key: String,
    pub(super) text_score: f32,
    pub(super) vector_score: f32,
    pub(super) score: f32,
    pub(super) normalized_text_score: f32,
    pub(super) normalized_vector_score: f32,
    pub(super) freshness_score: f32,
    pub(super) document_id: u64,
    pub(super) field_id: u32,
    pub(super) vector_id: u64,
    pub(super) chunk_id: u32,
    pub(super) source_start: u64,
    pub(super) source_len: u32,
}

impl HybridAccum {
    pub(super) fn new(object_version_id: [u8; 16]) -> Self {
        Self {
            object_version_id,
            object_key: String::new(),
            text_score: 0.0,
            vector_score: 0.0,
            score: 0.0,
            normalized_text_score: 0.0,
            normalized_vector_score: 0.0,
            freshness_score: 0.0,
            document_id: 0,
            field_id: 0,
            vector_id: 0,
            chunk_id: 0,
            source_start: 0,
            source_len: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct HybridCandidate {
    pub(super) item: HybridAccum,
    pub(super) object_ref: QueryObjectRef,
}

pub(super) fn score_hybrid_candidates(
    candidates: &mut [HybridCandidate],
    has_text: bool,
    has_vector: bool,
    text_weight: f32,
    vector_weight: f32,
    freshness_weight: f32,
) {
    let max_text_score = candidates
        .iter()
        .map(|candidate| candidate.item.text_score.max(0.0))
        .fold(0.0_f32, f32::max);
    let max_vector_score = candidates
        .iter()
        .map(|candidate| candidate.item.vector_score.max(0.0))
        .fold(0.0_f32, f32::max);
    let (min_created_at, max_created_at) =
        candidates
            .iter()
            .fold((i64::MAX, i64::MIN), |(min_seen, max_seen), candidate| {
                (
                    min_seen.min(candidate.object_ref.created_at_nanos),
                    max_seen.max(candidate.object_ref.created_at_nanos),
                )
            });
    let created_range = max_created_at.saturating_sub(min_created_at);

    for candidate in candidates {
        candidate.item.normalized_text_score = if has_text && max_text_score > f32::EPSILON {
            candidate.item.text_score.max(0.0) / max_text_score
        } else {
            0.0
        };
        candidate.item.normalized_vector_score = if has_vector && max_vector_score > f32::EPSILON {
            candidate.item.vector_score.max(0.0) / max_vector_score
        } else {
            0.0
        };
        candidate.item.freshness_score = if freshness_weight > 0.0 {
            if created_range <= 0 {
                1.0
            } else {
                candidate
                    .object_ref
                    .created_at_nanos
                    .saturating_sub(min_created_at) as f32
                    / created_range as f32
            }
        } else {
            0.0
        };
        candidate.item.score = candidate.item.normalized_text_score.mul_add(
            text_weight,
            candidate.item.normalized_vector_score * vector_weight,
        ) + candidate.item.freshness_score * freshness_weight;
    }
}
