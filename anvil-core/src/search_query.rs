use crate::formats::{
    Hash32,
    full_text::{
        Bm25Config, Bm25FieldStats, FullTextQueryError, Posting, TokenizerConfig, bm25_score,
        decode_postings, evaluate_phrase_query, tokenize_text,
    },
    vector::{VectorMetric, VectorSearchResult},
};
use crate::full_text_segment::DecodedFullTextSegment;
use crate::vector_hnsw::{HnswRsVectorIndexEngine, VectorIndexEngine};
use crate::vector_segment::DecodedVectorSegment;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq)]
pub struct FullTextSearchHit {
    pub document_id: u64,
    pub field_id: u16,
    pub object_version_id: [u8; 16],
    pub authz_label_hash: Hash32,
    pub score: f32,
    pub matched_terms: u32,
}

#[derive(Debug, Clone)]
pub struct FullTextSegmentQuery<'a> {
    pub query: &'a str,
    pub tokenizer: &'a TokenizerConfig,
    pub positions_enabled: bool,
    pub phrase: bool,
    pub bm25: Bm25Config,
    pub authorized_labels: Option<&'a BTreeSet<Hash32>>,
    pub limit: usize,
}

pub fn query_full_text_segment(
    segment: &DecodedFullTextSegment,
    query: FullTextSegmentQuery<'_>,
) -> Result<Vec<FullTextSearchHit>, FullTextQueryError> {
    if query.limit == 0 {
        return Ok(Vec::new());
    }
    let query_terms = tokenize_text(query.query, query.tokenizer)
        .into_iter()
        .map(|token| token.term.into_bytes())
        .collect::<Vec<_>>();
    if query_terms.is_empty() {
        return Err(FullTextQueryError::EmptyPhrase);
    }

    let postings_by_term = query_terms
        .iter()
        .map(|term| postings_for_term(segment, term))
        .collect::<Vec<_>>();
    if query.phrase {
        let borrowed = postings_by_term
            .iter()
            .map(Vec::as_slice)
            .collect::<Vec<_>>();
        let phrase_matches = evaluate_phrase_query(&borrowed, query.positions_enabled)?;
        let mut hits = phrase_matches
            .into_iter()
            .filter(|matched| is_authorized(matched.authz_label_hash, query.authorized_labels))
            .map(|matched| FullTextSearchHit {
                document_id: matched.document_id,
                field_id: matched.field_id,
                object_version_id: matched.object_version_id,
                authz_label_hash: matched.authz_label_hash,
                score: query_terms.len() as f32,
                matched_terms: query_terms.len().min(u32::MAX as usize) as u32,
            })
            .collect::<Vec<_>>();
        sort_hits(&mut hits);
        hits.truncate(query.limit);
        return Ok(hits);
    }

    let document_count = segment
        .postings
        .iter()
        .map(document_key)
        .collect::<BTreeSet<_>>()
        .len()
        .min(u32::MAX as usize) as u32;
    let stats = Bm25FieldStats {
        document_count,
        average_field_length: average_matched_field_length(&segment.postings),
    };
    let mut by_document = BTreeMap::<(u64, u16, [u8; 16], Hash32), FullTextSearchHit>::new();
    for (term_idx, postings) in postings_by_term.iter().enumerate() {
        let Some(term) = segment
            .terms
            .iter()
            .find(|term| term.term_utf8 == query_terms[term_idx])
        else {
            continue;
        };
        for posting in postings {
            if !is_authorized(posting.authz_label_hash, query.authorized_labels) {
                continue;
            }
            let key = document_key(posting);
            let score = bm25_score(
                posting.term_frequency,
                term.doc_frequency,
                posting.term_frequency.max(1) as u32,
                stats,
                query.bm25,
            );
            let entry = by_document.entry(key).or_insert_with(|| FullTextSearchHit {
                document_id: posting.document_id,
                field_id: posting.field_id,
                object_version_id: posting.object_version_id,
                authz_label_hash: posting.authz_label_hash,
                score: 0.0,
                matched_terms: 0,
            });
            entry.score += score;
            entry.matched_terms = entry.matched_terms.saturating_add(1);
        }
    }
    let mut hits = by_document.into_values().collect::<Vec<_>>();
    sort_hits(&mut hits);
    hits.truncate(query.limit);
    Ok(hits)
}

pub fn query_vector_segment(
    segment: &DecodedVectorSegment,
    query: &[f32],
    metric: VectorMetric,
    authorized_labels: Option<&BTreeSet<Hash32>>,
    limit: usize,
) -> Result<Vec<VectorSearchResult>, crate::formats::FormatError> {
    HnswRsVectorIndexEngine::default().query_segment(
        segment,
        query,
        metric,
        authorized_labels,
        limit,
    )
}

fn postings_for_term(segment: &DecodedFullTextSegment, term_utf8: &[u8]) -> Vec<Posting> {
    let Some(term) = segment
        .terms
        .iter()
        .find(|term| term.term_utf8 == term_utf8)
    else {
        return Vec::new();
    };
    let start = term.postings_offset as usize;
    let end = start.saturating_add(term.postings_len as usize);
    if end > segment.postings_bytes.len() {
        return Vec::new();
    }
    decode_postings(&segment.postings_bytes[start..end]).unwrap_or_default()
}

fn document_key(posting: &Posting) -> (u64, u16, [u8; 16], Hash32) {
    (
        posting.document_id,
        posting.field_id,
        posting.object_version_id,
        posting.authz_label_hash,
    )
}

fn average_matched_field_length(postings: &[Posting]) -> f32 {
    if postings.is_empty() {
        return 1.0;
    }
    let total = postings.iter().fold(0u64, |sum, posting| {
        sum.saturating_add(posting.term_frequency.max(1) as u64)
    });
    (total as f32 / postings.len() as f32).max(1.0)
}

fn is_authorized(label: Hash32, authorized_labels: Option<&BTreeSet<Hash32>>) -> bool {
    authorized_labels.is_none_or(|labels| labels.contains(&label))
}

fn sort_hits(hits: &mut [FullTextSearchHit]) {
    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| right.matched_terms.cmp(&left.matched_terms))
            .then_with(|| left.document_id.cmp(&right.document_id))
            .then_with(|| left.field_id.cmp(&right.field_id))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::full_text::{FullTextDocument, build_full_text_postings};
    use crate::formats::vector::{VectorModality, VectorPayload, VectorRecord};
    use crate::full_text_segment::{
        FullTextSegmentWrite, read_full_text_segment, write_full_text_segment,
    };
    use crate::storage::Storage;
    use crate::vector_segment::{
        VectorSegmentEntry, VectorSegmentWrite, read_vector_segment, write_vector_segment,
    };
    use tempfile::tempdir;

    #[tokio::test]
    async fn full_text_query_reads_segment_and_filters_by_authorized_label() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tokenizer = TokenizerConfig::default();
        let allowed = [1; 32];
        let denied = [2; 32];
        let built = build_full_text_postings(
            &[
                FullTextDocument {
                    document_id: 1,
                    field_id: 1,
                    object_version_id: [1; 16],
                    authz_label_hash: allowed,
                    text: "alpha beta beta",
                },
                FullTextDocument {
                    document_id: 2,
                    field_id: 1,
                    object_version_id: [2; 16],
                    authz_label_hash: denied,
                    text: "alpha beta beta beta",
                },
            ],
            &tokenizer,
        );
        let segment_ref = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "query-index",
                generation: 1,
                tokenizer: serde_json::json!({}),
                scorer: serde_json::json!({"kind": "bm25"}),
                source_cursor: 1,
                authz_revision: 1,
                built_postings: &built,
                document_table: b"",
            },
        )
        .await
        .unwrap();
        let segment = read_full_text_segment(&storage, &segment_ref)
            .await
            .unwrap();
        let authorized = BTreeSet::from([allowed]);

        let hits = query_full_text_segment(
            &segment,
            FullTextSegmentQuery {
                query: "alpha beta",
                tokenizer: &tokenizer,
                positions_enabled: true,
                phrase: false,
                bm25: Bm25Config::default(),
                authorized_labels: Some(&authorized),
                limit: 10,
            },
        )
        .unwrap();

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].document_id, 1);
        assert!(hits[0].score > 0.0);
    }

    #[tokio::test]
    async fn full_text_phrase_query_requires_positions_and_filters_results() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let tokenizer = TokenizerConfig::default();
        let allowed = [1; 32];
        let built = build_full_text_postings(
            &[
                FullTextDocument {
                    document_id: 1,
                    field_id: 1,
                    object_version_id: [1; 16],
                    authz_label_hash: allowed,
                    text: "quick brown fox",
                },
                FullTextDocument {
                    document_id: 2,
                    field_id: 1,
                    object_version_id: [2; 16],
                    authz_label_hash: allowed,
                    text: "quick blue brown",
                },
            ],
            &tokenizer,
        );
        let segment_ref = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "phrase-index",
                generation: 1,
                tokenizer: serde_json::json!({}),
                scorer: serde_json::json!({"kind": "bm25"}),
                source_cursor: 1,
                authz_revision: 1,
                built_postings: &built,
                document_table: b"",
            },
        )
        .await
        .unwrap();
        let segment = read_full_text_segment(&storage, &segment_ref)
            .await
            .unwrap();
        let hits = query_full_text_segment(
            &segment,
            FullTextSegmentQuery {
                query: "quick brown",
                tokenizer: &tokenizer,
                positions_enabled: true,
                phrase: true,
                bm25: Bm25Config::default(),
                authorized_labels: None,
                limit: 10,
            },
        )
        .unwrap();
        assert_eq!(
            hits.iter().map(|hit| hit.document_id).collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            query_full_text_segment(
                &segment,
                FullTextSegmentQuery {
                    positions_enabled: false,
                    ..FullTextSegmentQuery {
                        query: "quick brown",
                        tokenizer: &tokenizer,
                        positions_enabled: true,
                        phrase: true,
                        bm25: Bm25Config::default(),
                        authorized_labels: None,
                        limit: 10,
                    }
                },
            ),
            Err(FullTextQueryError::PositionsDisabled)
        );
    }

    #[tokio::test]
    async fn vector_query_reads_segment_scores_and_filters_by_authorized_label() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let allowed = [1; 32];
        let denied = [2; 32];
        let entries = vec![
            vector_entry(1, allowed, vec![1.0, 0.0]),
            vector_entry(2, denied, vec![0.99, 0.0]),
            vector_entry(3, allowed, vec![0.0, 1.0]),
        ];
        let segment_ref = write_vector_segment(
            &storage,
            VectorSegmentWrite {
                index_id: "vector-query",
                definition_hash: "blake3:test-definition",
                generation: 1,
                dimension: 2,
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
                source_cursor: 1,
                authz_revision: 1,
                entries: &entries,
                deleted_bitset: &[0],
            },
        )
        .await
        .unwrap();
        let segment = read_vector_segment(&storage, &segment_ref).await.unwrap();
        let authorized = BTreeSet::from([allowed]);
        let hits = query_vector_segment(
            &segment,
            &[1.0, 0.0],
            VectorMetric::Cosine,
            Some(&authorized),
            2,
        )
        .unwrap();

        assert_eq!(
            hits.iter().map(|hit| hit.vector_id).collect::<Vec<_>>(),
            vec![1, 3]
        );
    }

    #[tokio::test]
    async fn vector_query_round_trips_all_required_modalities() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let allowed = [9; 32];

        for (idx, modality) in [
            VectorModality::Text,
            VectorModality::Image,
            VectorModality::Audio,
            VectorModality::Video,
        ]
        .into_iter()
        .enumerate()
        {
            let entries = vec![
                vector_entry_with_modality(1, allowed, vec![1.0, 0.0], modality),
                vector_entry_with_modality(2, allowed, vec![0.0, 1.0], modality),
            ];
            let segment_ref = write_vector_segment(
                &storage,
                VectorSegmentWrite {
                    index_id: &format!("vector-modality-{idx}"),
                    definition_hash: "blake3:test-definition",
                    generation: 1,
                    dimension: 2,
                    metric: VectorMetric::Cosine,
                    embedding_provider: "test_only",
                    embedding_model: "embedding-v1",
                    embedding_model_version: None,
                    embedding_normalisation: "unit_l2",
                    embedding_chunking_hash: "blake3:test-chunking",
                    extractor_definition_hash: "blake3:test-extractor",
                    embedding_provenance_hash: "blake3:test-provenance",
                    modality,
                    hnsw_m: 32,
                    hnsw_ef_construction: 200,
                    source_cursor: 1,
                    authz_revision: 1,
                    entries: &entries,
                    deleted_bitset: &[0],
                },
            )
            .await
            .unwrap();

            let segment = read_vector_segment(&storage, &segment_ref).await.unwrap();
            let hits =
                query_vector_segment(&segment, &[1.0, 0.0], VectorMetric::Cosine, None, 1).unwrap();
            assert_eq!(segment.header.modality, modality.as_name());
            assert_eq!(hits[0].vector_id, 1);
        }
    }

    fn vector_entry(
        vector_id: u64,
        authz_label_hash: Hash32,
        values: Vec<f32>,
    ) -> VectorSegmentEntry {
        vector_entry_with_modality(vector_id, authz_label_hash, values, VectorModality::Text)
    }

    fn vector_entry_with_modality(
        vector_id: u64,
        authz_label_hash: Hash32,
        values: Vec<f32>,
        modality: VectorModality,
    ) -> VectorSegmentEntry {
        VectorSegmentEntry {
            source_id_binary: vec![vector_id as u8],
            source_generation: vector_id,
            labels: Vec::new(),
            record: VectorRecord {
                vector_id,
                object_version_id: [vector_id as u8; 16],
                chunk_id: vector_id as u32,
                modality: modality as u8,
                metric: VectorMetric::Cosine as u8,
                dimension: 2,
                vector_payload_offset: 0,
                source_start: vector_id * 10,
                source_len: 10,
                authz_label_hash,
                metadata_filter_bits: 0,
            },
            payload: VectorPayload {
                dimension: 2,
                values,
            },
        }
    }
}
