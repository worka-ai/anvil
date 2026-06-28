use crate::formats::{
    full_text::FullTextDocument,
    full_text::FullTextIndexDefinition,
    hash32,
    vector::{VectorIndexDefinition, VectorMetric, VectorPayload, VectorRecord},
};
use crate::full_text_segment::{self, FullTextSegmentWrite};
use crate::index_journal;
use crate::index_partition_watch::{self, IndexPartitionWatchPayload};
use crate::metadata_journal;
use crate::persistence::{Bucket, IndexDefinition, Object};
use crate::storage::{ExternalChunkManifest, Storage};
use crate::vector_segment::{self, VectorSegmentEntry, VectorSegmentWrite};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexBuildOutcome {
    pub index_storage_id: String,
    pub index_kind: String,
    pub generation: u64,
    pub item_count: usize,
    pub source_cursor: u128,
    pub segment_hashes: Vec<String>,
    pub diagnostics: Vec<IndexBuildDiagnostic>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexBuildDiagnostic {
    pub object_key: String,
    pub version_id: Option<uuid::Uuid>,
    pub severity: String,
    pub code: String,
    pub message: String,
    pub details: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
struct DocumentTableRow<'a> {
    document_id: u64,
    field_id: u16,
    object_key: &'a str,
    version_id: String,
}

#[derive(Debug, Clone)]
struct OwnedFullTextDocument {
    document_id: u64,
    field_id: u16,
    object_version_id: [u8; 16],
    authz_label_hash: [u8; 32],
    authz_revision: i64,
    object_key: String,
    text: String,
}

#[derive(Debug, Clone)]
struct OwnedVectorDocument {
    vector_id: u64,
    object_version_id: [u8; 16],
    chunk_id: u32,
    source_start: u64,
    source_len: u32,
    authz_label_hash: [u8; 32],
    authz_revision: i64,
    metadata_filter_bits: u64,
    values: Vec<f32>,
}

pub async fn build_full_text_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
) -> Result<IndexBuildOutcome> {
    if index.kind != "full_text" {
        return Err(anyhow!("index build only supports full_text indexes"));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let definition = FullTextIndexDefinition::from_json(&index.build_policy)
        .map_err(|error| anyhow!("invalid full text index definition: {error}"))?;
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation =
        full_text_segment::read_latest_full_text_segment(storage, &index_storage_id)
            .await?
            .map(|segment| segment.header.generation)
            .unwrap_or(0);
    let generation = latest_generation
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects =
        metadata_journal::read_current_objects(storage, bucket, partition_owner_signing_key)
            .await?;
    let mut owned_documents = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let Some(payload) = read_object_payload(storage, &object).await? else {
            continue;
        };
        let Ok(payload_text) = String::from_utf8(payload) else {
            continue;
        };
        let extracted = extract_text_fields(&index.extractor, &object, &payload_text);
        for (field_idx, text) in extracted.into_iter().enumerate() {
            if text.trim().is_empty() {
                continue;
            }
            let field_id = u16::try_from(field_idx.saturating_add(1)).unwrap_or(u16::MAX);
            owned_documents.push(OwnedFullTextDocument {
                document_id: object.id.max(0) as u64,
                field_id,
                object_version_id: *object.version_id.as_bytes(),
                authz_label_hash: object_authz_label_hash(bucket, &object),
                authz_revision: object.authz_revision,
                object_key: object.key.clone(),
                text,
            });
        }
    }

    let borrowed_documents = owned_documents
        .iter()
        .map(|document| FullTextDocument {
            document_id: document.document_id,
            field_id: document.field_id,
            object_version_id: document.object_version_id,
            authz_label_hash: document.authz_label_hash,
            text: document.text.as_str(),
        })
        .collect::<Vec<_>>();
    let built = crate::formats::full_text::build_full_text_postings(
        &borrowed_documents,
        &definition.tokenizer,
    );
    let document_table = serde_json::to_vec(
        &owned_documents
            .iter()
            .map(|document| DocumentTableRow {
                document_id: document.document_id,
                field_id: document.field_id,
                object_key: &document.object_key,
                version_id: uuid::Uuid::from_bytes(document.object_version_id).to_string(),
            })
            .collect::<Vec<_>>(),
    )?;
    let segment_path = full_text_segment::write_full_text_segment(
        storage,
        FullTextSegmentWrite {
            index_id: &index_storage_id,
            generation,
            tokenizer: serde_json::to_value(&index.build_policy)?,
            scorer: serde_json::json!({ "kind": "bm25" }),
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_documents(&owned_documents),
            built_postings: &built,
            document_table: &document_table,
        },
    )
    .await?;
    let segment_bytes = tokio::fs::read(&segment_path).await.with_context(|| {
        format!(
            "read generated full text segment {}",
            segment_path.display()
        )
    })?;
    let segment_hash = blake3::hash(&segment_bytes).to_hex().to_string();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let proof_hash = blake3::hash(
        format!(
            "full_text:{}:{}:{}:{}",
            index_storage_id, generation, source_cursor, segment_hash
        )
        .as_bytes(),
    )
    .to_hex()
    .to_string();
    let watch_cursor = next_index_watch_cursor(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
    )
    .await?;
    index_partition_watch::append_index_partition_watch_record(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        watch_cursor,
        *uuid::Uuid::new_v4().as_bytes(),
        latest_authz_revision_for_documents(&owned_documents),
        IndexPartitionWatchPayload {
            index_id: index_storage_id.clone(),
            index_kind: index.kind.clone(),
            event_type: "segment_built".to_string(),
            generation,
            source_cursor,
            source_manifest_hash: hex::encode(hash32(
                format!("bucket:{}:cursor:{}", bucket.id, source_cursor).as_bytes(),
            )),
            proof_hash,
            segment_hashes: vec![segment_hash.clone()],
            emitted_at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: "full_text".to_string(),
        generation,
        item_count: owned_documents.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics: Vec::new(),
    })
}

pub async fn build_vector_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
) -> Result<IndexBuildOutcome> {
    if index.kind != "vector" {
        return Err(anyhow!("index build only supports vector indexes"));
    }
    build_vector_index_with_policy(
        storage,
        bucket,
        index,
        &index.build_policy,
        &index.extractor,
        partition_owner_signing_key,
        source_cursor,
        "vector",
    )
    .await
}

pub async fn build_hybrid_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
) -> Result<IndexBuildOutcome> {
    if index.kind != "hybrid" {
        return Err(anyhow!("index build only supports hybrid indexes"));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let full_text_policy = index
        .build_policy
        .get("full_text")
        .ok_or_else(|| anyhow!("hybrid index build policy is missing full_text"))?;
    let vector_policy = index
        .build_policy
        .get("vector")
        .ok_or_else(|| anyhow!("hybrid index build policy is missing vector"))?;
    let text_extractor = index.extractor.get("text").unwrap_or(&index.extractor);
    let vector_extractor = index.extractor.get("vector").unwrap_or(&index.extractor);

    let text_index = IndexDefinition {
        kind: "full_text".to_string(),
        build_policy: full_text_policy.clone(),
        extractor: text_extractor.clone(),
        ..index.clone()
    };
    let text_outcome = build_full_text_index(
        storage,
        bucket,
        &text_index,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let vector_outcome = build_vector_index_with_policy(
        storage,
        bucket,
        index,
        vector_policy,
        vector_extractor,
        partition_owner_signing_key,
        source_cursor,
        "hybrid",
    )
    .await?;

    let mut segment_hashes = text_outcome.segment_hashes;
    segment_hashes.extend(vector_outcome.segment_hashes);
    let mut diagnostics = text_outcome.diagnostics;
    diagnostics.extend(vector_outcome.diagnostics);
    Ok(IndexBuildOutcome {
        index_storage_id: vector_outcome.index_storage_id,
        index_kind: "hybrid".to_string(),
        generation: text_outcome.generation.max(vector_outcome.generation),
        item_count: text_outcome
            .item_count
            .saturating_add(vector_outcome.item_count),
        source_cursor,
        segment_hashes,
        diagnostics,
    })
}

async fn build_vector_index_with_policy(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    build_policy: &JsonValue,
    extractor: &JsonValue,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    outcome_kind: &str,
) -> Result<IndexBuildOutcome> {
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let definition = VectorIndexDefinition::from_json(build_policy)
        .map_err(|error| anyhow!("invalid vector index definition: {error}"))?;
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation = vector_segment::read_latest_vector_segment(storage, &index_storage_id)
        .await?
        .map(|segment| segment.header.generation)
        .unwrap_or(0);
    let generation = latest_generation
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects =
        metadata_journal::read_current_objects(storage, bucket, partition_owner_signing_key)
            .await?;
    let mut vector_documents = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let Some(payload) = read_object_payload(storage, &object).await? else {
            continue;
        };
        let extraction = extract_vectors(extractor, &payload, &definition);
        let diagnostic_count = extraction.diagnostics.len();
        for diagnostic in extraction.diagnostics {
            diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "error".to_string(),
                code: diagnostic.code,
                message: diagnostic.message,
                details: diagnostic.details,
            });
        }
        if extraction.vectors.is_empty() {
            if diagnostic_count == 0 {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: object.key.clone(),
                    version_id: Some(object.version_id),
                    severity: "warning".to_string(),
                    code: "VectorExtractionEmpty".to_string(),
                    message: "vector extractor produced no vectors for object version".to_string(),
                    details: serde_json::json!({
                        "content_type": object.content_type,
                        "extractor": extractor,
                    }),
                });
            }
            continue;
        }
        for vector in extraction.vectors {
            vector_documents.push(OwnedVectorDocument {
                vector_id: vector_documents.len().saturating_add(1) as u64,
                object_version_id: *object.version_id.as_bytes(),
                chunk_id: vector.chunk_id,
                source_start: vector.source_start,
                source_len: vector.source_len,
                authz_label_hash: object_authz_label_hash(bucket, &object),
                authz_revision: object.authz_revision,
                metadata_filter_bits: 0,
                values: vector.values,
            });
        }
    }

    let entries = vector_documents
        .iter()
        .map(|document| VectorSegmentEntry {
            record: VectorRecord {
                vector_id: document.vector_id,
                object_version_id: document.object_version_id,
                chunk_id: document.chunk_id,
                modality: definition.modality as u8,
                metric: definition.metric as u8,
                dimension: definition.dimension,
                vector_payload_offset: 0,
                source_start: document.source_start,
                source_len: document.source_len,
                authz_label_hash: document.authz_label_hash,
                metadata_filter_bits: document.metadata_filter_bits,
            },
            payload: VectorPayload {
                dimension: definition.dimension,
                values: document.values.clone(),
            },
        })
        .collect::<Vec<_>>();
    let deleted_bitset = vec![0; entries.len().div_ceil(8)];
    let segment_path = vector_segment::write_vector_segment(
        storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            generation,
            dimension: definition.dimension,
            metric: definition.metric,
            embedding_model: &definition.embedding_model,
            modality: definition.modality,
            hnsw_m: definition.hnsw_m,
            hnsw_ef_construction: definition.hnsw_ef_construction,
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_vectors(&vector_documents),
            entries: &entries,
            deleted_bitset: &deleted_bitset,
        },
    )
    .await?;
    let segment_bytes = tokio::fs::read(&segment_path)
        .await
        .with_context(|| format!("read generated vector segment {}", segment_path.display()))?;
    let segment_hash = blake3::hash(&segment_bytes).to_hex().to_string();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let proof_hash = blake3::hash(
        format!(
            "vector:{}:{}:{}:{}",
            index_storage_id, generation, source_cursor, segment_hash
        )
        .as_bytes(),
    )
    .to_hex()
    .to_string();
    let watch_cursor = next_index_watch_cursor(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
    )
    .await?;
    index_partition_watch::append_index_partition_watch_record(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        watch_cursor,
        *uuid::Uuid::new_v4().as_bytes(),
        latest_authz_revision_for_vectors(&vector_documents),
        IndexPartitionWatchPayload {
            index_id: index_storage_id.clone(),
            index_kind: outcome_kind.to_string(),
            event_type: "segment_built".to_string(),
            generation,
            source_cursor,
            source_manifest_hash: hex::encode(hash32(
                format!("bucket:{}:cursor:{}", bucket.id, source_cursor).as_bytes(),
            )),
            proof_hash,
            segment_hashes: vec![segment_hash.clone()],
            emitted_at: chrono::Utc::now().to_rfc3339(),
        },
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: outcome_kind.to_string(),
        generation,
        item_count: entries.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics,
    })
}

async fn next_index_watch_cursor(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_storage_id: &str,
    partition_id: &str,
    preferred_cursor: u128,
) -> Result<u128> {
    let latest = index_partition_watch::latest_index_partition_watch_cursor(
        storage,
        tenant_id,
        bucket_id,
        index_storage_id,
        partition_id,
    )
    .await?
    .unwrap_or(0);
    Ok(preferred_cursor.max(latest.saturating_add(1)))
}

#[derive(Debug, Clone)]
struct ExtractedVector {
    chunk_id: u32,
    source_start: u64,
    source_len: u32,
    values: Vec<f32>,
}

#[derive(Debug, Clone)]
struct VectorExtractionDiagnostic {
    code: String,
    message: String,
    details: JsonValue,
}

#[derive(Debug, Clone)]
struct VectorExtraction {
    vectors: Vec<ExtractedVector>,
    diagnostics: Vec<VectorExtractionDiagnostic>,
}

#[derive(Debug, Deserialize)]
struct JsonVectorRecord {
    vector: Option<Vec<f32>>,
    values: Option<Vec<f32>>,
    embedding: Option<Vec<f32>>,
    chunk_id: Option<u32>,
    source_start: Option<u64>,
    source_len: Option<u32>,
}

fn extract_vectors(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let source = extractor
        .get("source")
        .and_then(JsonValue::as_str)
        .unwrap_or("object_body_utf8");
    match source {
        "object_body_json_vector" | "object_body_json" | "json_vector" => {
            extract_json_vectors(extractor, payload, definition)
        }
        "object_body_f32_le" | "f32_le" => extract_f32_le_vectors(payload, definition),
        "object_body_utf8" | "utf8" | "body" => {
            extract_deterministic_embedding(payload, definition)
        }
        _ => VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "UnsupportedVectorExtractor".to_string(),
                message: format!("unsupported vector extractor source `{source}`"),
                details: serde_json::json!({ "source": source }),
            }],
        },
    }
}

fn extract_json_vectors(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let Ok(json) = serde_json::from_slice::<JsonValue>(payload) else {
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "VectorJsonDecodeFailed".to_string(),
                message: "object body is not valid JSON for vector extraction".to_string(),
                details: JsonValue::Null,
            }],
        };
    };
    let selected = extractor
        .get("json_pointer")
        .or_else(|| extractor.get("vector_pointer"))
        .or_else(|| extractor.get("pointer"))
        .and_then(JsonValue::as_str)
        .and_then(|pointer| json.pointer(pointer))
        .unwrap_or(&json);
    vectors_from_json_value(selected, definition)
}

fn vectors_from_json_value(
    value: &JsonValue,
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    if let Some(vector) = parse_json_vector(value) {
        return vector_extraction_from_vectors(
            vec![ExtractedVector {
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                values: vector,
            }],
            definition,
        );
    }
    if let Ok(record) = serde_json::from_value::<JsonVectorRecord>(value.clone()) {
        if let Some(values) = record.vector.or(record.values).or(record.embedding) {
            return vector_extraction_from_vectors(
                vec![ExtractedVector {
                    chunk_id: record.chunk_id.unwrap_or(0),
                    source_start: record.source_start.unwrap_or(0),
                    source_len: record.source_len.unwrap_or(0),
                    values,
                }],
                definition,
            );
        }
    }
    if let Some(array) = value
        .as_object()
        .and_then(|object| object.get("vectors"))
        .and_then(JsonValue::as_array)
        .or_else(|| value.as_array())
    {
        let mut vectors = Vec::new();
        let mut diagnostics = Vec::new();
        for (idx, item) in array.iter().enumerate() {
            if let Some(values) = parse_json_vector(item) {
                vectors.push(ExtractedVector {
                    chunk_id: u32::try_from(idx).unwrap_or(u32::MAX),
                    source_start: 0,
                    source_len: 0,
                    values,
                });
                continue;
            }
            match serde_json::from_value::<JsonVectorRecord>(item.clone()) {
                Ok(record) => {
                    if let Some(values) = record.vector.or(record.values).or(record.embedding) {
                        vectors.push(ExtractedVector {
                            chunk_id: record
                                .chunk_id
                                .unwrap_or_else(|| u32::try_from(idx).unwrap_or(u32::MAX)),
                            source_start: record.source_start.unwrap_or(0),
                            source_len: record.source_len.unwrap_or(0),
                            values,
                        });
                    }
                }
                Err(error) => diagnostics.push(VectorExtractionDiagnostic {
                    code: "VectorJsonEntryDecodeFailed".to_string(),
                    message: "JSON vector entry could not be decoded".to_string(),
                    details: serde_json::json!({ "entry_index": idx, "error": error.to_string() }),
                }),
            }
        }
        let mut extraction = vector_extraction_from_vectors(vectors, definition);
        extraction.diagnostics.extend(diagnostics);
        return extraction;
    }
    VectorExtraction {
        vectors: Vec::new(),
        diagnostics: vec![VectorExtractionDiagnostic {
            code: "VectorJsonShapeUnsupported".to_string(),
            message: "JSON payload does not contain a vector or vector record".to_string(),
            details: JsonValue::Null,
        }],
    }
}

fn parse_json_vector(value: &JsonValue) -> Option<Vec<f32>> {
    let array = value.as_array()?;
    let mut values = Vec::with_capacity(array.len());
    for item in array {
        values.push(item.as_f64()? as f32);
    }
    Some(values)
}

fn extract_f32_le_vectors(payload: &[u8], definition: &VectorIndexDefinition) -> VectorExtraction {
    if !payload.len().is_multiple_of(4) {
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "VectorPayloadLengthInvalid".to_string(),
                message: "raw f32 vector payload length is not divisible by four".to_string(),
                details: serde_json::json!({ "byte_len": payload.len() }),
            }],
        };
    }
    let values = payload
        .chunks_exact(4)
        .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
        .collect::<Vec<_>>();
    vector_extraction_from_vectors(
        vec![ExtractedVector {
            chunk_id: 0,
            source_start: 0,
            source_len: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            values,
        }],
        definition,
    )
}

fn extract_deterministic_embedding(
    payload: &[u8],
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let mut values = Vec::with_capacity(usize::from(definition.dimension));
    let mut counter = 0u64;
    while values.len() < usize::from(definition.dimension) {
        let mut hasher = blake3::Hasher::new();
        hasher.update(definition.embedding_model.as_bytes());
        hasher.update(definition.modality.as_name().as_bytes());
        hasher.update(payload);
        hasher.update(&counter.to_le_bytes());
        let digest = hasher.finalize();
        for chunk in digest.as_bytes().chunks_exact(4) {
            if values.len() == usize::from(definition.dimension) {
                break;
            }
            let raw = u32::from_le_bytes(chunk.try_into().unwrap());
            let normalized = (raw as f32 / u32::MAX as f32) * 2.0 - 1.0;
            values.push(normalized);
        }
        counter = counter.saturating_add(1);
    }
    normalize_vector(&mut values, definition.metric);
    vector_extraction_from_vectors(
        vec![ExtractedVector {
            chunk_id: 0,
            source_start: 0,
            source_len: u32::try_from(payload.len()).unwrap_or(u32::MAX),
            values,
        }],
        definition,
    )
}

fn vector_extraction_from_vectors(
    vectors: Vec<ExtractedVector>,
    definition: &VectorIndexDefinition,
) -> VectorExtraction {
    let mut accepted = Vec::new();
    let mut diagnostics = Vec::new();
    for (idx, vector) in vectors.into_iter().enumerate() {
        if vector.values.len() != usize::from(definition.dimension) {
            diagnostics.push(VectorExtractionDiagnostic {
                code: "VectorDimensionMismatch".to_string(),
                message: "extracted vector dimension does not match index definition".to_string(),
                details: serde_json::json!({
                    "vector_index": idx,
                    "expected_dimension": definition.dimension,
                    "actual_dimension": vector.values.len(),
                }),
            });
            continue;
        }
        accepted.push(vector);
    }
    VectorExtraction {
        vectors: accepted,
        diagnostics,
    }
}

fn normalize_vector(values: &mut [f32], metric: VectorMetric) {
    if metric != VectorMetric::Cosine {
        return;
    }
    let norm = values.iter().map(|value| value * value).sum::<f32>().sqrt();
    if norm <= f32::EPSILON {
        return;
    }
    for value in values {
        *value /= norm;
    }
}

fn selector_matches(selector: &JsonValue, object: &Object) -> bool {
    if selector.is_null() {
        return true;
    }
    let Some(selector) = selector.as_object() else {
        return true;
    };
    if let Some(prefix) = selector.get("prefix").and_then(JsonValue::as_str) {
        if !object.key.starts_with(prefix) {
            return false;
        }
    }
    if let Some(content_type) = selector.get("content_type").and_then(JsonValue::as_str) {
        if object.content_type.as_deref() != Some(content_type) {
            return false;
        }
    }
    true
}

fn extract_text_fields(extractor: &JsonValue, object: &Object, payload_text: &str) -> Vec<String> {
    if let Some(fields) = extractor.get("fields").and_then(JsonValue::as_array) {
        return fields
            .iter()
            .filter_map(|field| {
                let source = field
                    .get("source")
                    .and_then(JsonValue::as_str)
                    .unwrap_or("object_body_utf8");
                extract_text_source(source, object, payload_text)
            })
            .collect();
    }
    if let Some(source) = extractor.get("source").and_then(JsonValue::as_str) {
        return extract_text_source(source, object, payload_text)
            .into_iter()
            .collect();
    }
    if extractor.get("encoding").and_then(JsonValue::as_str) == Some("utf8") {
        return vec![payload_text.to_string()];
    }
    vec![payload_text.to_string()]
}

fn extract_text_source(source: &str, object: &Object, payload_text: &str) -> Option<String> {
    match source {
        "object_body_utf8" | "utf8" | "body" => Some(payload_text.to_string()),
        "object_key" | "key" => Some(object.key.clone()),
        "content_type" => object.content_type.clone(),
        _ => None,
    }
}

async fn read_object_payload(storage: &Storage, object: &Object) -> Result<Option<Vec<u8>>> {
    if let Some(inline) = object.inline_payload.clone() {
        return Ok(Some(inline));
    }
    if let Some(manifest) = object
        .shard_map
        .as_ref()
        .and_then(|value| serde_json::from_value::<ExternalChunkManifest>(value.clone()).ok())
        .filter(|manifest| manifest.kind == "external_chunks_v1")
    {
        let mut bytes = Vec::new();
        for (expected_idx, chunk) in manifest.chunks.iter().enumerate() {
            if chunk.chunk_index != expected_idx as u64 {
                return Err(anyhow!("external chunk manifest order mismatch"));
            }
            let data = storage.retrieve_external_chunk(&chunk.storage_ref).await?;
            if data.len() as u64 != chunk.plaintext_length {
                return Err(anyhow!("external chunk length mismatch"));
            }
            let actual_hash = blake3::hash(&data).to_hex().to_string();
            if actual_hash != chunk.payload_chunk_hash || actual_hash != chunk.storage_chunk_hash {
                return Err(anyhow!("external chunk hash mismatch"));
            }
            bytes.extend_from_slice(&data);
        }
        return Ok(Some(bytes));
    }
    match storage.retrieve_whole_object(&object.content_hash).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(_) => Ok(None),
    }
}

fn object_authz_label_hash(bucket: &Bucket, object: &Object) -> [u8; 32] {
    hash32(
        format!(
            "tenant:{}:bucket:{}:object:{}:authz:{}",
            bucket.tenant_id, bucket.id, object.key, object.authz_revision
        )
        .as_bytes(),
    )
}

fn latest_authz_revision_for_documents(documents: &[OwnedFullTextDocument]) -> u64 {
    documents
        .iter()
        .filter_map(|document| u64::try_from(document.authz_revision).ok())
        .max()
        .unwrap_or(0)
}

fn latest_authz_revision_for_vectors(documents: &[OwnedVectorDocument]) -> u64 {
    documents
        .iter()
        .filter_map(|document| u64::try_from(document.authz_revision).ok())
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn selector_matches_prefix_and_content_type() {
        let object = object("docs/a.txt", Some("text/plain"));
        assert!(selector_matches(
            &serde_json::json!({"prefix": "docs/"}),
            &object
        ));
        assert!(!selector_matches(
            &serde_json::json!({"prefix": "logs/"}),
            &object
        ));
        assert!(selector_matches(
            &serde_json::json!({"content_type": "text/plain"}),
            &object
        ));
    }

    #[test]
    fn extractor_supports_body_key_and_content_type_fields() {
        let object = object("docs/a.txt", Some("text/plain"));
        let fields = extract_text_fields(
            &serde_json::json!({
                "fields": [
                    {"source": "object_body_utf8"},
                    {"source": "object_key"},
                    {"source": "content_type"}
                ]
            }),
            &object,
            "alpha body",
        );
        assert_eq!(fields, vec!["alpha body", "docs/a.txt", "text/plain"]);
    }

    fn object(key: &str, content_type: Option<&str>) -> Object {
        Object {
            id: 1,
            tenant_id: 1,
            bucket_id: 1,
            key: key.to_string(),
            content_hash: hex::encode([1; 32]),
            size: 10,
            etag: "etag".to_string(),
            content_type: content_type.map(ToOwned::to_owned),
            version_id: uuid::Uuid::from_bytes([1; 16]),
            mutation_id: uuid::Uuid::from_bytes([2; 16]),
            index_policy_snapshot: String::new(),
            user_metadata_hash: String::new(),
            authz_revision: 0,
            record_hash: String::new(),
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: None,
            user_meta: None,
            shard_map: None,
            inline_payload: None,
            checksum: None,
        }
    }
}
