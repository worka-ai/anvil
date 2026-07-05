use crate::core_store::{AuthzScopeRef, CoreObjectRef, CoreStore, GetBlob, SourceId, SourceKind};
use crate::embedding_provider::{EmbeddingProviderRegistry, TEST_ONLY_EMBEDDING_PROVIDER};
use crate::formats::{
    full_text::FullTextDocument,
    full_text::FullTextIndexDefinition,
    hash32,
    vector::{VectorIndexDefinition, VectorMetric, VectorPayload, VectorRecord},
};
use crate::full_text_segment::{self, FullTextSegmentWrite};
use crate::index_journal;
use crate::index_partition_watch::{self, IndexPartitionWatchPayload};
use crate::media_extraction::{
    DerivedAssetPolicy, DerivedOutputKind, MediaExtractionRequest, MediaObjectRef,
    execute_media_extraction,
};
use crate::metadata_journal;
use crate::partition_fence::{
    AcquireOwnership, MAX_OWNERSHIP_LEASE_MS, OwnershipPrincipal, OwnershipResource,
    OwnershipResourceKind, RenewOwnership, acquire_ownership, read_ownership_fence,
    renew_ownership,
};
use crate::persistence::{AppendStream, AppendStreamRecord, Bucket, IndexDefinition, Object};
use crate::storage::Storage;
use crate::typed_field_segment::{
    self, TypedFieldSegmentRow, TypedFieldSegmentWrite, encode_row_values, source_id_binary,
};
use crate::vector_segment::{self, VectorSegmentEntry, VectorSegmentWrite};
use crate::{
    derived_index_proof::{self, DerivedIndexProofWrite},
    watch_checkpoint::{self, WatchCheckpointUpdate, WatchCheckpointWriteAuthority},
};
use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;

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
struct ExtractedTextField {
    text: String,
}

#[derive(Debug, Clone)]
struct TextExtractionDiagnostic {
    code: String,
    message: String,
    details: JsonValue,
}

#[derive(Debug, Clone)]
struct TextExtraction {
    fields: Vec<ExtractedTextField>,
    diagnostics: Vec<TextExtractionDiagnostic>,
}

#[derive(Debug, Clone)]
struct OwnedVectorDocument {
    vector_id: u64,
    source_id_binary: Vec<u8>,
    source_generation: u64,
    object_version_id: [u8; 16],
    chunk_id: u32,
    source_start: u64,
    source_len: u32,
    authz_label_hash: [u8; 32],
    authz_revision: i64,
    metadata_filter_bits: u64,
    values: Vec<f32>,
}

#[derive(Debug, Clone)]
struct TypedJsonBuildDefinition {
    source_kind: String,
    fields: Vec<TypedJsonBuildField>,
}

#[derive(Debug, Clone)]
struct TypedJsonBuildField {
    name: String,
    extractor: String,
    required: bool,
}

pub async fn build_full_text_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
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
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    ensure_index_build_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut owned_documents = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let payload = read_object_payload(&core_store, &object).await?;
        let extracted = extract_text_fields(&index.extractor, &object, &payload);
        let diagnostic_count = extracted.diagnostics.len();
        for diagnostic in extracted.diagnostics {
            diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "warning".to_string(),
                code: diagnostic.code,
                message: diagnostic.message,
                details: diagnostic.details,
            });
        }
        if extracted.fields.is_empty() && diagnostic_count == 0 {
            diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "warning".to_string(),
                code: "TextExtractionEmpty".to_string(),
                message: "text extractor produced no fields for object version".to_string(),
                details: serde_json::json!({ "extractor": index.extractor }),
            });
        }
        for (field_idx, field) in extracted.fields.into_iter().enumerate() {
            if field.text.trim().is_empty() {
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
                text: field.text,
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
    let segment_ref = full_text_segment::write_full_text_segment(
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
    let segment_bytes =
        full_text_segment::read_full_text_segment_bytes(storage, &segment_ref).await?;
    let segment_hash = blake3::hash(&segment_bytes).to_hex().to_string();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        &index.kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let watch_cursor = next_index_watch_cursor(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: index.kind.clone(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: chrono::Utc::now().to_rfc3339(),
    };
    let watch_authority = acquire_index_partition_watch_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        &watch_payload,
        builder_node_id,
        partition_owner_signing_key,
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
        watch_payload,
        watch_authority,
        partition_owner_signing_key,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: "full_text".to_string(),
        generation,
        item_count: owned_documents.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics,
    })
}

pub async fn build_typed_json_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
) -> Result<IndexBuildOutcome> {
    if index.kind != "typed_json" {
        return Err(anyhow!("index build only supports typed_json indexes"));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let definition = parse_typed_json_build_definition(index)?;
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation =
        typed_field_segment::read_latest_typed_field_segment(storage, &index_storage_id)
            .await?
            .map(|segment| segment.header.generation)
            .unwrap_or(0);
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    ensure_index_build_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let core_store = CoreStore::new(storage.clone()).await?;
    let (rows, diagnostics) = match definition.source_kind.as_str() {
        "object_current" => {
            build_typed_json_object_rows(
                storage,
                bucket,
                index,
                &definition,
                &core_store,
                partition_owner_signing_key,
                source_cursor,
            )
            .await?
        }
        "append_record" => {
            build_typed_json_append_rows(bucket, index, &definition, &core_store, source_cursor)
                .await?
        }
        _ => return Err(anyhow!("unsupported typed_json source kind")),
    };

    let field_names = definition
        .fields
        .iter()
        .map(|field| field.name.clone())
        .collect::<Vec<_>>();
    let definition_hash = blake3::hash(index.build_policy.to_string().as_bytes())
        .to_hex()
        .to_string();
    let segment_ref = typed_field_segment::write_typed_field_segment(
        storage,
        TypedFieldSegmentWrite {
            index_id: &index_storage_id,
            generation,
            source_kind: &definition.source_kind,
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_typed_rows(&rows),
            definition_hash: &definition_hash,
            field_names: &field_names,
            rows: &rows,
        },
    )
    .await?;
    let segment_bytes =
        typed_field_segment::read_typed_field_segment_bytes(storage, &segment_ref).await?;
    let segment_hash = blake3::hash(&segment_bytes).to_hex().to_string();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = typed_json_source_manifest_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
        &definition.source_kind,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        &index.kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let watch_cursor = next_index_watch_cursor(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: index.kind.clone(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: chrono::Utc::now().to_rfc3339(),
    };
    let watch_authority = acquire_index_partition_watch_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        &watch_payload,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    index_partition_watch::append_index_partition_watch_record(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        watch_cursor,
        *uuid::Uuid::new_v4().as_bytes(),
        latest_authz_revision_for_typed_rows(&rows),
        watch_payload,
        watch_authority,
        partition_owner_signing_key,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: index.kind.clone(),
        generation,
        item_count: rows.len(),
        source_cursor,
        segment_hashes: vec![segment_hash],
        diagnostics,
    })
}

pub async fn build_metadata_backed_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
) -> Result<IndexBuildOutcome> {
    if !matches!(index.kind.as_str(), "path" | "metadata_filter") {
        return Err(anyhow!(
            "index build only supports path and metadata_filter indexes"
        ));
    }
    if !index.enabled {
        return Err(anyhow!("index build requires an enabled index"));
    }
    let index_storage_id =
        index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
    let latest_generation =
        typed_field_segment::read_latest_typed_field_segment(storage, &index_storage_id)
            .await?
            .map(|segment| segment.header.generation)
            .unwrap_or(0);
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    ensure_index_build_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let mut rows = Vec::new();
    let mut field_names = BTreeMap::<String, ()>::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let row = metadata_backed_row_from_object(bucket, &object)?;
        for field in row.values.keys() {
            field_names.insert(field.clone(), ());
        }
        rows.push(row);
    }
    let field_names = field_names.into_keys().collect::<Vec<_>>();
    let definition_hash = blake3::hash(
        serde_json::json!({
            "kind": index.kind,
            "selector": index.selector,
            "extractor": index.extractor,
            "build_policy": index.build_policy,
        })
        .to_string()
        .as_bytes(),
    )
    .to_hex()
    .to_string();
    let segment_ref = typed_field_segment::write_typed_field_segment(
        storage,
        TypedFieldSegmentWrite {
            index_id: &index_storage_id,
            generation,
            source_kind: "object_metadata",
            source_cursor: u64::try_from(source_cursor).unwrap_or(u64::MAX),
            authz_revision: latest_authz_revision_for_typed_rows(&rows),
            definition_hash: &definition_hash,
            field_names: &field_names,
            rows: &rows,
        },
    )
    .await?;
    let segment_bytes =
        typed_field_segment::read_typed_field_segment_bytes(storage, &segment_ref).await?;
    let segment_hash = blake3::hash(&segment_bytes).to_hex().to_string();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        &index.kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let watch_cursor = next_index_watch_cursor(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: index.kind.clone(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: chrono::Utc::now().to_rfc3339(),
    };
    let watch_authority = acquire_index_partition_watch_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        &watch_payload,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    index_partition_watch::append_index_partition_watch_record(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        watch_cursor,
        *uuid::Uuid::new_v4().as_bytes(),
        latest_authz_revision_for_typed_rows(&rows),
        watch_payload,
        watch_authority,
        partition_owner_signing_key,
    )
    .await?;

    Ok(IndexBuildOutcome {
        index_storage_id,
        index_kind: index.kind.clone(),
        generation,
        item_count: rows.len(),
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
    builder_node_id: &str,
    embedding_providers: &EmbeddingProviderRegistry,
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
        builder_node_id,
        "vector",
        embedding_providers,
    )
    .await
}

pub async fn build_hybrid_index(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    embedding_providers: &EmbeddingProviderRegistry,
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
        builder_node_id,
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
        builder_node_id,
        "hybrid",
        embedding_providers,
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
    _extractor: &JsonValue,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    builder_node_id: &str,
    outcome_kind: &str,
    embedding_providers: &EmbeddingProviderRegistry,
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
    let latest_checkpoint_generation =
        latest_index_checkpoint_generation(storage, &index_storage_id, partition_owner_signing_key)
            .await?;
    ensure_index_build_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let generation = latest_generation
        .max(latest_checkpoint_generation)
        .saturating_add(1)
        .max(u64::try_from(source_cursor).unwrap_or(u64::MAX))
        .max(1);

    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let core_store = CoreStore::new(storage.clone()).await?;
    let mut vector_documents = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let payload = read_object_payload(&core_store, &object).await?;
        let extraction = extract_vectors(
            &definition.extractor,
            &payload,
            &definition,
            embedding_providers,
        )
        .await;
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
                        "extractor": &definition.extractor,
                    }),
                });
            }
            continue;
        }
        for vector in extraction.vectors {
            vector_documents.push(OwnedVectorDocument {
                vector_id: vector_documents.len().saturating_add(1) as u64,
                source_id_binary: source_id_binary(&object_current_source_id(bucket, &object))?,
                source_generation: object.id.max(0) as u64,
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
            source_id_binary: document.source_id_binary.clone(),
            source_generation: document.source_generation,
            labels: if document.metadata_filter_bits == 0 {
                Vec::new()
            } else {
                vec![document.metadata_filter_bits]
            },
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
    let definition_hash = blake3::hash(build_policy.to_string().as_bytes())
        .to_hex()
        .to_string();
    let segment_ref = vector_segment::write_vector_segment(
        storage,
        VectorSegmentWrite {
            index_id: &index_storage_id,
            definition_hash: &definition_hash,
            generation,
            dimension: definition.dimension,
            metric: definition.metric,
            embedding_provider: &definition.embedding_provider,
            embedding_model: &definition.embedding_model,
            embedding_model_version: definition.embedding_model_version.as_deref(),
            embedding_normalisation: &definition.normalisation,
            embedding_chunking_hash: &definition.chunking_hash,
            extractor_definition_hash: &definition.extractor_hash,
            embedding_provenance_hash: &definition.provenance_hash,
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
    let segment_bytes = vector_segment::read_vector_segment_bytes(storage, &segment_ref).await?;
    let segment_hash = blake3::hash(&segment_bytes).to_hex().to_string();
    let partition_id = hex::encode(hash32(index_storage_id.as_bytes()));
    let source_manifest_hash = metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let proof = publish_index_build_proof_and_checkpoint(
        storage,
        bucket,
        &index_storage_id,
        outcome_kind,
        &partition_id,
        source_cursor,
        &source_manifest_hash,
        generation,
        &[segment_hash.clone()],
        builder_node_id,
        partition_owner_signing_key,
    )
    .await?;
    let watch_cursor = next_index_watch_cursor(
        storage,
        index.tenant_id,
        index.bucket_id,
        &index_storage_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
    )
    .await?;
    let watch_payload = IndexPartitionWatchPayload {
        index_id: index_storage_id.clone(),
        index_kind: outcome_kind.to_string(),
        event_type: "segment_built".to_string(),
        generation,
        source_cursor,
        source_manifest_hash,
        proof_hash: proof
            .proof_hash
            .clone()
            .ok_or_else(|| anyhow!("derived index proof was not sealed"))?,
        segment_hashes: vec![segment_hash.clone()],
        emitted_at: chrono::Utc::now().to_rfc3339(),
    };
    let watch_authority = acquire_index_partition_watch_authority(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        &watch_payload,
        builder_node_id,
        partition_owner_signing_key,
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
        watch_payload,
        watch_authority,
        partition_owner_signing_key,
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

#[allow(clippy::too_many_arguments)]
async fn publish_index_build_proof_and_checkpoint(
    storage: &Storage,
    bucket: &Bucket,
    index_storage_id: &str,
    index_kind: &str,
    index_partition_id: &str,
    source_cursor: u128,
    source_manifest_hash: &str,
    generation: u64,
    segment_hashes: &[String],
    builder_node_id: &str,
    signing_key: &[u8],
) -> Result<derived_index_proof::DerivedIndexProof> {
    let built_at_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))?;
    let proof = derived_index_proof::write_derived_index_proof(
        storage,
        DerivedIndexProofWrite {
            index_id: index_storage_id.to_string(),
            index_kind: index_kind.to_string(),
            partition_family: "index".to_string(),
            partition_id: index_partition_id.to_string(),
            source_watch_stream_id: "object_metadata".to_string(),
            source_cursor,
            source_manifest_hash: source_manifest_hash.to_string(),
            generation,
            segment_hashes: segment_hashes.to_vec(),
            built_by_node: builder_node_id.to_string(),
            built_at_nanos,
        },
        signing_key,
    )
    .await?;
    let checkpoint_update = WatchCheckpointUpdate {
        watch_stream_id: "object_metadata".to_string(),
        partition_family: "object_metadata".to_string(),
        partition_id: hex::encode(metadata_journal::object_metadata_partition_id(
            bucket.tenant_id,
            bucket.id,
        )),
        consumer_id: index_storage_id.to_string(),
        cursor: source_cursor,
        source_manifest_hash: source_manifest_hash.to_string(),
        generation,
        updated_by_node: builder_node_id.to_string(),
        updated_at_nanos: built_at_nanos,
    };
    let checkpoint_authority = acquire_watch_checkpoint_authority(
        storage,
        &checkpoint_update,
        builder_node_id,
        signing_key,
    )
    .await?;
    watch_checkpoint::checkpoint_watch_consumer(
        storage,
        checkpoint_update,
        checkpoint_authority,
        signing_key,
    )
    .await?;
    Ok(proof)
}

async fn latest_index_checkpoint_generation(
    storage: &Storage,
    index_storage_id: &str,
    signing_key: &[u8],
) -> Result<u64> {
    Ok(watch_checkpoint::read_watch_checkpoint(
        storage,
        "object_metadata",
        index_storage_id,
        signing_key,
    )
    .await?
    .map(|checkpoint| checkpoint.generation)
    .unwrap_or(0))
}

async fn ensure_index_build_authority(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    index_storage_id: &str,
    builder_node_id: &str,
    signing_key: &[u8],
) -> Result<()> {
    let resource = OwnershipResource {
        resource_kind: OwnershipResourceKind::IndexPartition,
        resource_id: format!(
            "tenant/{tenant_id}/bucket/{bucket_id}/index_build/{index_storage_id}"
        ),
    };
    let owner = OwnershipPrincipal::node(builder_node_id);
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))?;
    let ttl_nanos = i64::try_from(MAX_OWNERSHIP_LEASE_MS)
        .unwrap_or(i64::MAX)
        .saturating_mul(1_000_000);

    if let Some(record) =
        read_ownership_fence(storage, owner.tenant_id, &resource, signing_key).await?
        && record.owner.same_security_owner(&owner)
        && record.is_active_unexpired(now_nanos)
    {
        renew_ownership(
            storage,
            RenewOwnership {
                request_id: format!("index-build-renew-{}", resource.resource_id),
                resource,
                owner,
                current_fence: record.fence,
                now_nanos,
                ttl_nanos,
            },
            signing_key,
        )
        .await?;
        return Ok(());
    }

    acquire_ownership(
        storage,
        AcquireOwnership {
            request_id: format!("index-build-acquire-{}", resource.resource_id),
            idempotency_key: format!("index-build-owner-{}", resource.resource_id),
            resource,
            owner,
            now_nanos,
            ttl_nanos,
        },
        signing_key,
    )
    .await?;
    Ok(())
}

async fn acquire_watch_checkpoint_authority(
    storage: &Storage,
    update: &WatchCheckpointUpdate,
    builder_node_id: &str,
    signing_key: &[u8],
) -> Result<WatchCheckpointWriteAuthority> {
    let resource_id = watch_checkpoint::watch_checkpoint_resource_id(
        &update.watch_stream_id,
        &update.partition_id,
        &update.consumer_id,
    );
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))?;
    let outcome = acquire_ownership(
        storage,
        AcquireOwnership {
            request_id: format!("watch-checkpoint-{resource_id}-{builder_node_id}"),
            idempotency_key: format!("watch-checkpoint-{resource_id}-{builder_node_id}"),
            resource: OwnershipResource {
                resource_kind: OwnershipResourceKind::WatchPartition,
                resource_id: resource_id.clone(),
            },
            owner: OwnershipPrincipal::node(builder_node_id),
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000),
        },
        signing_key,
    )
    .await?;
    Ok(WatchCheckpointWriteAuthority {
        owner_node_id: builder_node_id.to_string(),
        fence: outcome.record.fence,
        resource_id,
    })
}

#[allow(clippy::too_many_arguments)]
async fn acquire_index_partition_watch_authority(
    storage: &Storage,
    tenant_id: i64,
    bucket_id: i64,
    partition_id: &str,
    payload: &IndexPartitionWatchPayload,
    builder_node_id: &str,
    signing_key: &[u8],
) -> Result<index_partition_watch::IndexPartitionWatchWriteAuthority> {
    let resource_id = index_partition_watch::index_partition_watch_resource_id(
        tenant_id,
        bucket_id,
        &payload.index_id,
        partition_id,
    );
    let now_nanos = chrono::Utc::now()
        .timestamp_nanos_opt()
        .ok_or_else(|| anyhow!("timestamp cannot be represented in nanoseconds"))?;
    let outcome = acquire_ownership(
        storage,
        AcquireOwnership {
            request_id: format!("index-watch-{resource_id}-{builder_node_id}"),
            idempotency_key: format!("index-watch-{resource_id}-{builder_node_id}"),
            resource: OwnershipResource {
                resource_kind: OwnershipResourceKind::WatchPartition,
                resource_id: resource_id.clone(),
            },
            owner: OwnershipPrincipal::node(builder_node_id),
            now_nanos,
            ttl_nanos: i64::try_from(MAX_OWNERSHIP_LEASE_MS)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000_000),
        },
        signing_key,
    )
    .await?;
    Ok(index_partition_watch::IndexPartitionWatchWriteAuthority {
        owner_node_id: builder_node_id.to_string(),
        fence: outcome.record.fence,
        resource_id,
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

async fn extract_vectors(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
    embedding_providers: &EmbeddingProviderRegistry,
) -> VectorExtraction {
    let kind = extractor
        .get("kind")
        .and_then(JsonValue::as_str)
        .unwrap_or("object_body_utf8");
    match kind {
        "object_body_json_vector" | "object_body_json" | "json_vector" => {
            extract_json_vectors(extractor, payload, definition)
        }
        "object_body_f32_le" | "f32_le" => extract_f32_le_vectors(payload, definition),
        "object_body_utf8" | "utf8" | "body" => {
            extract_provider_embedding(extractor, payload, definition, embedding_providers).await
        }
        _ => VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "UnsupportedVectorExtractor".to_string(),
                message: format!("unsupported vector extractor kind `{kind}`"),
                details: serde_json::json!({ "kind": kind }),
            }],
        },
    }
}

async fn extract_provider_embedding(
    extractor: &JsonValue,
    payload: &[u8],
    definition: &VectorIndexDefinition,
    embedding_providers: &EmbeddingProviderRegistry,
) -> VectorExtraction {
    if definition.embedding_provider == TEST_ONLY_EMBEDDING_PROVIDER {
        if embedding_providers.is_test_only_allowed() {
            return extract_test_only_embedding(payload, definition);
        }
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "TestOnlyEmbeddingProviderDisabled".to_string(),
                message: "test_only vector embedding provider is disabled for this server"
                    .to_string(),
                details: serde_json::json!({ "provider": "test_only" }),
            }],
        };
    }

    let response = match embedding_providers
        .embed_text(definition, extractor, payload)
        .await
    {
        Ok(response) => response,
        Err(error) => {
            return VectorExtraction {
                vectors: Vec::new(),
                diagnostics: vec![VectorExtractionDiagnostic {
                    code: if embedding_providers.has_provider(&definition.embedding_provider) {
                        "EmbeddingProviderFailed"
                    } else {
                        "EmbeddingProviderNotConfigured"
                    }
                    .to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "provider": definition.embedding_provider }),
                }],
            };
        }
    };
    if let (Some(expected), Some(actual)) = (
        definition.embedding_model_version.as_deref(),
        response.model_version.as_deref(),
    ) && expected != actual
    {
        return VectorExtraction {
            vectors: Vec::new(),
            diagnostics: vec![VectorExtractionDiagnostic {
                code: "EmbeddingProviderModelVersionMismatch".to_string(),
                message: "embedding provider returned a different model version than the index definition".to_string(),
                details: serde_json::json!({
                    "provider": definition.embedding_provider,
                    "expected_model_version": expected,
                    "actual_model_version": actual,
                }),
            }],
        };
    }
    vector_extraction_from_vectors(
        response
            .vectors
            .into_iter()
            .map(|vector| ExtractedVector {
                chunk_id: vector.chunk_id.unwrap_or(0),
                source_start: vector.source_start.unwrap_or(0),
                source_len: vector
                    .source_len
                    .unwrap_or_else(|| u32::try_from(payload.len()).unwrap_or(u32::MAX)),
                values: vector.values,
            })
            .collect(),
        definition,
    )
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

fn extract_test_only_embedding(
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

fn metadata_backed_row_from_object(
    bucket: &Bucket,
    object: &Object,
) -> Result<TypedFieldSegmentRow> {
    let values = object
        .user_meta
        .as_ref()
        .and_then(JsonValue::as_object)
        .map(|metadata| {
            metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    Ok(TypedFieldSegmentRow {
        object_key: object.key.clone(),
        object_version_id: object.version_id.to_string(),
        source_identity: format!("{}#{}", object.key, object.version_id),
        encoded_values: encode_row_values(&values)?,
        source_id_binary: source_id_binary(&object_current_source_id(bucket, object))?,
        value_flags: 0,
        values,
        authz_label_hash: hex::encode(object_authz_label_hash(bucket, object)),
        authz_revision: u64::try_from(object.authz_revision).unwrap_or(0),
    })
}

async fn build_typed_json_object_rows(
    storage: &Storage,
    bucket: &Bucket,
    index: &IndexDefinition,
    definition: &TypedJsonBuildDefinition,
    core_store: &CoreStore,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
) -> Result<(Vec<TypedFieldSegmentRow>, Vec<IndexBuildDiagnostic>)> {
    let objects = metadata_journal::read_current_objects_through_sequence(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await?;
    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    for object in objects {
        if object.deleted_at.is_some() || !selector_matches(&index.selector, &object) {
            continue;
        }
        let payload = match read_object_payload(core_store, &object).await {
            Ok(payload) => payload,
            Err(error) => {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: object.key.clone(),
                    version_id: Some(object.version_id),
                    severity: "error".to_string(),
                    code: "TypedJsonObjectPayloadUnavailable".to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "source_kind": definition.source_kind }),
                });
                continue;
            }
        };
        let json = match serde_json::from_slice::<JsonValue>(&payload) {
            Ok(json) => json,
            Err(error) => {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: object.key.clone(),
                    version_id: Some(object.version_id),
                    severity: "error".to_string(),
                    code: "TypedJsonObjectInvalid".to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "content_type": object.content_type }),
                });
                continue;
            }
        };
        match typed_json_row_from_object(bucket, definition, &object, &json) {
            Ok(row) => rows.push(row),
            Err(error) => diagnostics.push(IndexBuildDiagnostic {
                object_key: object.key.clone(),
                version_id: Some(object.version_id),
                severity: "error".to_string(),
                code: "TypedJsonRowExtractionFailed".to_string(),
                message: error.to_string(),
                details: serde_json::json!({ "fields": index.build_policy.get("fields") }),
            }),
        }
    }
    Ok((rows, diagnostics))
}

async fn build_typed_json_append_rows(
    bucket: &Bucket,
    index: &IndexDefinition,
    definition: &TypedJsonBuildDefinition,
    core_store: &CoreStore,
    source_cursor: u128,
) -> Result<(Vec<TypedFieldSegmentRow>, Vec<IndexBuildDiagnostic>)> {
    let records = crate::append_journal::list_append_stream_records_for_bucket(
        core_store.storage(),
        bucket.tenant_id,
        bucket.id,
    )
    .await?;
    let mut rows = Vec::new();
    let mut diagnostics = Vec::new();
    for (stream, record) in records {
        if (record.id.max(0) as u128) > source_cursor {
            continue;
        }
        if !selector_matches_append(&index.selector, &stream, &record) {
            continue;
        }
        let payload = match core_store
            .get_blob(GetBlob {
                object_ref: record.payload_object_ref.clone(),
            })
            .await
        {
            Ok(payload) => payload,
            Err(error) => {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: stream.stream_key.clone(),
                    version_id: None,
                    severity: "error".to_string(),
                    code: "TypedJsonAppendPayloadUnavailable".to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "record_sequence": record.record_sequence }),
                });
                continue;
            }
        };
        let json = match serde_json::from_slice::<JsonValue>(&payload) {
            Ok(json) => json,
            Err(error) => {
                diagnostics.push(IndexBuildDiagnostic {
                    object_key: stream.stream_key.clone(),
                    version_id: None,
                    severity: "error".to_string(),
                    code: "TypedJsonAppendPayloadInvalid".to_string(),
                    message: error.to_string(),
                    details: serde_json::json!({ "content_type": record.content_type }),
                });
                continue;
            }
        };
        match typed_json_row_from_append_record(bucket, definition, &stream, &record, &json) {
            Ok(row) => rows.push(row),
            Err(error) => diagnostics.push(IndexBuildDiagnostic {
                object_key: stream.stream_key.clone(),
                version_id: None,
                severity: "error".to_string(),
                code: "TypedJsonAppendRowExtractionFailed".to_string(),
                message: error.to_string(),
                details: serde_json::json!({ "fields": index.build_policy.get("fields") }),
            }),
        }
    }
    Ok((rows, diagnostics))
}

fn selector_matches_append(
    selector: &JsonValue,
    stream: &AppendStream,
    record: &AppendStreamRecord,
) -> bool {
    if selector.is_null() {
        return true;
    }
    let Some(selector) = selector.as_object() else {
        return true;
    };
    if let Some(prefix) = selector.get("prefix").and_then(JsonValue::as_str)
        && !stream.stream_key.starts_with(prefix)
    {
        return false;
    }
    if let Some(content_type) = selector.get("content_type").and_then(JsonValue::as_str)
        && record.content_type.as_deref() != Some(content_type)
    {
        return false;
    }
    true
}

async fn typed_json_source_manifest_hash(
    storage: &Storage,
    bucket: &Bucket,
    partition_owner_signing_key: &[u8],
    source_cursor: u128,
    source_kind: &str,
) -> Result<String> {
    if source_kind == "append_record" {
        return Ok(blake3::hash(
            format!(
                "append_record:{}:{}:{}",
                bucket.tenant_id, bucket.id, source_cursor
            )
            .as_bytes(),
        )
        .to_hex()
        .to_string());
    }
    metadata_journal::object_metadata_source_checkpoint_hash(
        storage,
        bucket,
        partition_owner_signing_key,
        source_cursor,
    )
    .await
}

fn parse_typed_json_build_definition(index: &IndexDefinition) -> Result<TypedJsonBuildDefinition> {
    let source_kind = json_optional_string_field(&index.build_policy, "source_kind")
        .or_else(|| json_optional_string_field(&index.build_policy, "source"))
        .unwrap_or_else(|| "object_current".to_string());
    let fields_json = index
        .build_policy
        .get("fields")
        .or_else(|| index.extractor.get("fields"))
        .ok_or_else(|| anyhow!("typed_json index requires fields"))?;
    let JsonValue::Array(field_values) = fields_json else {
        return Err(anyhow!("typed_json fields must be an array"));
    };
    let mut fields = Vec::with_capacity(field_values.len());
    for value in field_values {
        let name = json_optional_string_field(value, "name")
            .ok_or_else(|| anyhow!("typed_json field requires name"))?;
        let extractor = json_optional_string_field(value, "extractor")
            .or_else(|| json_optional_string_field(value, "json_pointer"))
            .ok_or_else(|| anyhow!("typed_json field requires extractor"))?;
        validate_typed_json_extractor(&source_kind, &extractor)?;
        fields.push(TypedJsonBuildField {
            name,
            extractor,
            required: value
                .get("required")
                .and_then(JsonValue::as_bool)
                .unwrap_or(false),
        });
    }
    Ok(TypedJsonBuildDefinition {
        source_kind,
        fields,
    })
}

fn typed_json_row_from_object(
    bucket: &Bucket,
    definition: &TypedJsonBuildDefinition,
    object: &Object,
    json: &JsonValue,
) -> Result<TypedFieldSegmentRow> {
    let mut values = serde_json::Map::new();
    for field in &definition.fields {
        let value = match field.extractor.as_str() {
            "object_key" => JsonValue::String(object.key.clone()),
            "object_content_type" => object
                .content_type
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            "created_at" => JsonValue::String(object.created_at.to_rfc3339()),
            extractor if extractor.starts_with("object_body_json_pointer:") => json
                .pointer(extractor.trim_start_matches("object_body_json_pointer:"))
                .cloned()
                .unwrap_or(JsonValue::Null),
            extractor if extractor.starts_with("object_user_metadata_json_pointer:") => object
                .user_meta
                .as_ref()
                .and_then(|metadata| {
                    metadata
                        .pointer(extractor.trim_start_matches("object_user_metadata_json_pointer:"))
                })
                .cloned()
                .unwrap_or(JsonValue::Null),
            pointer if pointer.starts_with('/') => {
                json.pointer(pointer).cloned().unwrap_or(JsonValue::Null)
            }
            _ => JsonValue::Null,
        };
        if value.is_null() && field.required {
            return Err(anyhow!("typed_json required field missing: {}", field.name));
        }
        values.insert(field.name.clone(), value);
    }
    let values = values.into_iter().collect();
    Ok(TypedFieldSegmentRow {
        object_key: object.key.clone(),
        object_version_id: object.version_id.to_string(),
        source_identity: format!("{}#{}", object.key, object.version_id),
        encoded_values: encode_row_values(&values)?,
        source_id_binary: source_id_binary(&object_current_source_id(bucket, object))?,
        value_flags: 0,
        values,
        authz_label_hash: hex::encode(object_authz_label_hash(bucket, object)),
        authz_revision: u64::try_from(object.authz_revision).unwrap_or(0),
    })
}

fn typed_json_row_from_append_record(
    bucket: &Bucket,
    definition: &TypedJsonBuildDefinition,
    stream: &AppendStream,
    record: &AppendStreamRecord,
    json: &JsonValue,
) -> Result<TypedFieldSegmentRow> {
    let mut values = serde_json::Map::new();
    for field in &definition.fields {
        let value = match field.extractor.as_str() {
            "append_stream_key" => JsonValue::String(stream.stream_key.clone()),
            "append_record_sequence" => JsonValue::Number(record.record_sequence.into()),
            "append_content_type" => record
                .content_type
                .clone()
                .map(JsonValue::String)
                .unwrap_or(JsonValue::Null),
            "created_at" => JsonValue::String(record.created_at.to_rfc3339()),
            extractor if extractor.starts_with("append_payload_json_pointer:") => json
                .pointer(extractor.trim_start_matches("append_payload_json_pointer:"))
                .cloned()
                .unwrap_or(JsonValue::Null),
            extractor if extractor.starts_with("append_user_metadata_json_pointer:") => record
                .user_meta
                .as_ref()
                .and_then(|metadata| {
                    metadata
                        .pointer(extractor.trim_start_matches("append_user_metadata_json_pointer:"))
                })
                .cloned()
                .unwrap_or(JsonValue::Null),
            pointer if pointer.starts_with('/') => {
                json.pointer(pointer).cloned().unwrap_or(JsonValue::Null)
            }
            _ => JsonValue::Null,
        };
        if value.is_null() && field.required {
            return Err(anyhow!("typed_json required field missing: {}", field.name));
        }
        values.insert(field.name.clone(), value);
    }
    let values = values.into_iter().collect();
    let source_identity = format!("{}#{}", stream.stream_key, record.record_sequence);
    Ok(TypedFieldSegmentRow {
        object_key: stream.stream_key.clone(),
        object_version_id: record.record_sequence.to_string(),
        source_identity,
        encoded_values: encode_row_values(&values)?,
        source_id_binary: source_id_binary(&append_record_source_id(bucket, stream, record))?,
        value_flags: 0,
        values,
        authz_label_hash: hex::encode(hash32(
            format!(
                "tenant:{}:bucket:{}:append:{}:record:{}",
                bucket.tenant_id, bucket.id, stream.stream_key, record.record_sequence
            )
            .as_bytes(),
        )),
        authz_revision: 0,
    })
}

fn object_current_source_id(bucket: &Bucket, object: &Object) -> SourceId {
    let storage_tenant = bucket.tenant_id.to_string();
    SourceId {
        schema: "anvil.query.source_id.v1".to_string(),
        mesh_id: "local-mesh".to_string(),
        anvil_storage_tenant_id: storage_tenant.clone(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: storage_tenant,
            authz_realm_id: format!("tenant:{}", bucket.tenant_id),
        },
        kind: SourceKind::ObjectCurrent,
        resource_namespace: "anvil_object".to_string(),
        resource_id: format!("{}/{}/{}", bucket.tenant_id, bucket.name, object.key),
        generation: object.id.max(0) as u64,
        tombstone: object.deleted_at.is_some(),
        variant: BTreeMap::from([
            ("bucket_id".to_string(), bucket.id.to_string()),
            ("version_id".to_string(), object.version_id.to_string()),
        ]),
    }
}

fn append_record_source_id(
    bucket: &Bucket,
    stream: &AppendStream,
    record: &AppendStreamRecord,
) -> SourceId {
    let storage_tenant = bucket.tenant_id.to_string();
    SourceId {
        schema: "anvil.query.source_id.v1".to_string(),
        mesh_id: "local-mesh".to_string(),
        anvil_storage_tenant_id: storage_tenant.clone(),
        authz_scope: AuthzScopeRef {
            anvil_storage_tenant_id: storage_tenant,
            authz_realm_id: format!("tenant:{}", bucket.tenant_id),
        },
        kind: SourceKind::AppendRecord,
        resource_namespace: "anvil_append_record".to_string(),
        resource_id: format!(
            "{}/{}/{}/{}",
            bucket.tenant_id, bucket.name, stream.stream_key, record.record_sequence
        ),
        generation: record.id.max(0) as u64,
        tombstone: false,
        variant: BTreeMap::from([
            ("bucket_id".to_string(), bucket.id.to_string()),
            ("stream_id".to_string(), stream.stream_id.to_string()),
            (
                "record_sequence".to_string(),
                record.record_sequence.to_string(),
            ),
        ]),
    }
}

fn validate_typed_json_extractor(source_kind: &str, extractor: &str) -> Result<()> {
    let pointer_valid = |value: &str| value.starts_with('/');
    match (source_kind, extractor) {
        (_, "created_at") => Ok(()),
        ("object_current" | "object_version", "object_key" | "object_content_type") => Ok(()),
        ("object_current" | "object_version", value) if pointer_valid(value) => Ok(()),
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_body_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("object_current" | "object_version", value)
            if value
                .strip_prefix("object_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        (
            "append_record",
            "append_stream_key" | "append_record_sequence" | "append_content_type",
        ) => Ok(()),
        ("append_record", value) if pointer_valid(value) => Ok(()),
        ("append_record", value)
            if value
                .strip_prefix("append_payload_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        ("append_record", value)
            if value
                .strip_prefix("append_user_metadata_json_pointer:")
                .is_some_and(pointer_valid) =>
        {
            Ok(())
        }
        _ => Err(anyhow!("invalid typed_json field extractor")),
    }
}

fn json_optional_string_field(value: &JsonValue, name: &str) -> Option<String> {
    value
        .get(name)
        .and_then(JsonValue::as_str)
        .map(ToOwned::to_owned)
}

fn extract_text_fields(extractor: &JsonValue, object: &Object, payload: &[u8]) -> TextExtraction {
    let mut fields = Vec::new();
    let mut diagnostics = Vec::new();
    if let Some(field_specs) = extractor.get("fields").and_then(JsonValue::as_array) {
        for (idx, field) in field_specs.iter().enumerate() {
            let source = field
                .get("source")
                .and_then(JsonValue::as_str)
                .unwrap_or("object_body_utf8");
            match extract_text_source(source, field, object, payload) {
                Ok(Some(text)) => fields.push(ExtractedTextField { text }),
                Ok(None) => {}
                Err(diagnostic) => diagnostics.push(TextExtractionDiagnostic {
                    details: merge_details(
                        diagnostic.details,
                        serde_json::json!({ "field_index": idx }),
                    ),
                    ..diagnostic
                }),
            }
        }
        return TextExtraction {
            fields,
            diagnostics,
        };
    }
    if let Some(source) = extractor.get("source").and_then(JsonValue::as_str) {
        match extract_text_source(source, extractor, object, payload) {
            Ok(Some(text)) => fields.push(ExtractedTextField { text }),
            Ok(None) => {}
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
        return TextExtraction {
            fields,
            diagnostics,
        };
    }
    if extractor.get("encoding").and_then(JsonValue::as_str) == Some("utf8") {
        match decode_utf8_text(payload) {
            Ok(text) => fields.push(ExtractedTextField { text }),
            Err(diagnostic) => diagnostics.push(diagnostic),
        }
        return TextExtraction {
            fields,
            diagnostics,
        };
    }
    match decode_utf8_text(payload) {
        Ok(text) => fields.push(ExtractedTextField { text }),
        Err(diagnostic) => diagnostics.push(diagnostic),
    }
    TextExtraction {
        fields,
        diagnostics,
    }
}

fn extract_text_source(
    source: &str,
    extractor: &JsonValue,
    object: &Object,
    payload: &[u8],
) -> Result<Option<String>, TextExtractionDiagnostic> {
    match source {
        "object_body_utf8" | "utf8" | "body" | "git_blob_text" => {
            decode_utf8_text(payload).map(Some)
        }
        "object_key" | "key" => Ok(Some(object.key.clone())),
        "content_type" => Ok(object.content_type.clone()),
        "json_pointer" => {
            let payload_text = decode_utf8_text(payload)?;
            extract_json_pointer_text(extractor, &payload_text)
        }
        "metadata_field" => extract_metadata_field_text(extractor, object),
        "media_transcript" => extract_media_transcript_text(object, payload),
        "personaldb_table_column" => extract_personaldb_table_column_text(extractor, payload),
        other => Err(TextExtractionDiagnostic {
            code: "UnsupportedTextExtractor".to_string(),
            message: format!("unsupported text extractor source `{other}`"),
            details: serde_json::json!({ "source": other }),
        }),
    }
}

fn decode_utf8_text(payload: &[u8]) -> Result<String, TextExtractionDiagnostic> {
    String::from_utf8(payload.to_vec()).map_err(|error| TextExtractionDiagnostic {
        code: "TextPayloadNotUtf8".to_string(),
        message: "object body is not valid UTF-8 for text extraction".to_string(),
        details: serde_json::json!({ "error": error.to_string() }),
    })
}

fn extract_media_transcript_text(
    object: &Object,
    payload: &[u8],
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let content_type = object
        .content_type
        .as_deref()
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "MediaContentTypeMissing".to_string(),
            message: "media_transcript text extractor requires an object content type".to_string(),
            details: serde_json::json!({ "object_key": object.key.clone() }),
        })?;
    let extraction = execute_media_extraction(
        MediaExtractionRequest {
            object: MediaObjectRef {
                tenant_id: object.tenant_id,
                bucket_id: object.bucket_id,
                object_key: object.key.clone(),
                version_id: object.version_id.to_string(),
                content_hash: object.content_hash.clone(),
                size_bytes: u64::try_from(payload.len()).unwrap_or(u64::MAX),
            },
            content_type: content_type.to_string(),
            asset_policy: DerivedAssetPolicy::InternalOnly,
        },
        payload,
    )
    .map_err(|error| TextExtractionDiagnostic {
        code: "MediaTranscriptExtractionFailed".to_string(),
        message: error.to_string(),
        details: serde_json::json!({ "content_type": content_type }),
    })?;
    extraction
        .outputs
        .into_iter()
        .find(|output| output.kind == DerivedOutputKind::TextTranscript)
        .map(|output| {
            String::from_utf8(output.bytes).map_err(|error| TextExtractionDiagnostic {
                code: "MediaTranscriptNotUtf8".to_string(),
                message: "media transcript output is not valid UTF-8".to_string(),
                details: serde_json::json!({ "error": error.to_string() }),
            })
        })
        .transpose()
}

fn extract_json_pointer_text(
    extractor: &JsonValue,
    payload_text: &str,
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let pointer = extractor
        .get("json_pointer")
        .or_else(|| extractor.get("pointer"))
        .or_else(|| extractor.get("path"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "JsonPointerMissing".to_string(),
            message: "json_pointer text extractor requires a JSON pointer".to_string(),
            details: serde_json::json!({ "extractor": extractor }),
        })?;
    let body = serde_json::from_str::<JsonValue>(payload_text).map_err(|error| {
        TextExtractionDiagnostic {
            code: "JsonPointerDecodeFailed".to_string(),
            message: "object body is not valid JSON for json_pointer text extraction".to_string(),
            details: serde_json::json!({ "pointer": pointer, "error": error.to_string() }),
        }
    })?;
    let Some(value) = body.pointer(pointer) else {
        return Err(TextExtractionDiagnostic {
            code: "JsonPointerNotFound".to_string(),
            message: "JSON pointer did not match a value in the object body".to_string(),
            details: serde_json::json!({ "pointer": pointer }),
        });
    };
    Ok(json_value_to_text(value))
}

fn extract_personaldb_table_column_text(
    extractor: &JsonValue,
    payload: &[u8],
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let column = extractor
        .get("column")
        .or_else(|| extractor.get("column_name"))
        .or_else(|| extractor.get("field"))
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "PersonalDbTableColumnMissing".to_string(),
            message: "personaldb_table_column text extractor requires a column name".to_string(),
            details: serde_json::json!({ "extractor": extractor }),
        })?;
    let payload_text = decode_utf8_text(payload)?;
    let row = serde_json::from_str::<JsonValue>(&payload_text).map_err(|error| {
        TextExtractionDiagnostic {
            code: "PersonalDbTableColumnDecodeFailed".to_string(),
            message: "object body is not valid JSON for personaldb_table_column extraction"
                .to_string(),
            details: serde_json::json!({ "column": column, "error": error.to_string() }),
        }
    })?;
    if let Some(expected_table) = extractor
        .get("table")
        .or_else(|| extractor.get("table_name"))
        .and_then(JsonValue::as_str)
        .filter(|value| !value.trim().is_empty())
        && !personaldb_table_matches(&row, expected_table)
    {
        return Ok(None);
    }
    let Some(value) = personaldb_column_value(&row, column) else {
        return Err(TextExtractionDiagnostic {
            code: "PersonalDbTableColumnNotFound".to_string(),
            message: "personaldb_table_column did not match a value in the row payload".to_string(),
            details: serde_json::json!({ "column": column }),
        });
    };
    Ok(json_value_to_text(value))
}

fn personaldb_table_matches(row: &JsonValue, expected_table: &str) -> bool {
    row.get("table_name")
        .or_else(|| row.get("table"))
        .and_then(JsonValue::as_str)
        == Some(expected_table)
}

fn personaldb_column_value<'a>(row: &'a JsonValue, column: &str) -> Option<&'a JsonValue> {
    if column.starts_with('/') {
        return row.pointer(column);
    }
    row.get("columns")
        .and_then(|columns| columns.get(column))
        .or_else(|| row.get("row").and_then(|row| row.get(column)))
        .or_else(|| row.get("new_values").and_then(|values| values.get(column)))
        .or_else(|| row.get("values").and_then(|values| values.get(column)))
        .or_else(|| row.get(column))
}

fn extract_metadata_field_text(
    extractor: &JsonValue,
    object: &Object,
) -> Result<Option<String>, TextExtractionDiagnostic> {
    let field = extractor
        .get("field")
        .or_else(|| extractor.get("metadata_field"))
        .or_else(|| extractor.get("key"))
        .or_else(|| extractor.get("path"))
        .and_then(JsonValue::as_str)
        .ok_or_else(|| TextExtractionDiagnostic {
            code: "MetadataFieldMissing".to_string(),
            message: "metadata_field text extractor requires a field name".to_string(),
            details: serde_json::json!({ "extractor": extractor }),
        })?;
    let Some(metadata) = object.user_meta.as_ref() else {
        return Err(TextExtractionDiagnostic {
            code: "MetadataFieldNotFound".to_string(),
            message: "object has no user metadata for metadata_field text extraction".to_string(),
            details: serde_json::json!({ "field": field }),
        });
    };
    let value = if field.starts_with('/') {
        metadata.pointer(field)
    } else {
        metadata.get(field)
    };
    let Some(value) = value else {
        return Err(TextExtractionDiagnostic {
            code: "MetadataFieldNotFound".to_string(),
            message: "metadata field did not match a value in object user metadata".to_string(),
            details: serde_json::json!({ "field": field }),
        });
    };
    Ok(json_value_to_text(value))
}

fn json_value_to_text(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::String(value) => Some(value.clone()),
        JsonValue::Number(value) => Some(value.to_string()),
        JsonValue::Bool(value) => Some(value.to_string()),
        JsonValue::Array(_) | JsonValue::Object(_) => Some(value.to_string()),
    }
}

fn merge_details(left: JsonValue, right: JsonValue) -> JsonValue {
    let mut merged = serde_json::Map::new();
    if let JsonValue::Object(values) = left {
        merged.extend(values);
    } else if !left.is_null() {
        merged.insert("details".to_string(), left);
    }
    if let JsonValue::Object(values) = right {
        merged.extend(values);
    }
    JsonValue::Object(merged)
}

async fn read_object_payload(core_store: &CoreStore, object: &Object) -> Result<Vec<u8>> {
    let object_ref = core_object_ref_from_shard_map(object).ok_or_else(|| {
        anyhow!(
            "object {} version {} is not CoreStore-backed",
            object.key,
            object.version_id
        )
    })?;
    core_store
        .get_blob(GetBlob { object_ref })
        .await
        .with_context(|| format!("read CoreStore payload for {}", object.key))
}

fn core_object_ref_from_shard_map(object: &Object) -> Option<CoreObjectRef> {
    let value = object.shard_map.as_ref()?;
    if value.get("schema")?.as_str()? != "anvil.core.object_ref.v1" {
        return None;
    }
    Some(CoreObjectRef {
        hash: value.get("hash")?.as_str()?.to_string(),
        logical_size: value.get("logical_size")?.as_u64()?,
        manifest_ref: value.get("manifest_ref")?.as_str()?.to_string(),
    })
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

fn latest_authz_revision_for_typed_rows(rows: &[TypedFieldSegmentRow]) -> u64 {
    rows.iter().map(|row| row.authz_revision).max().unwrap_or(0)
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

    #[tokio::test]
    async fn vector_text_extraction_uses_only_explicit_test_provider() {
        let production_definition =
            VectorIndexDefinition::from_json(&test_vector_definition("configured_provider", 4))
                .unwrap();
        let test_registry = EmbeddingProviderRegistry::for_tests(true);

        let missing_provider = extract_vectors(
            &production_definition.extractor,
            b"hello",
            &production_definition,
            &test_registry,
        )
        .await;
        assert!(missing_provider.vectors.is_empty());
        assert_eq!(
            missing_provider.diagnostics[0].code,
            "EmbeddingProviderNotConfigured"
        );

        let definition =
            VectorIndexDefinition::from_json(&test_vector_definition("test_only", 4)).unwrap();

        let disabled = extract_vectors(
            &definition.extractor,
            b"hello",
            &definition,
            &EmbeddingProviderRegistry::for_tests(false),
        )
        .await;
        assert!(disabled.vectors.is_empty());
        assert_eq!(
            disabled.diagnostics[0].code,
            "TestOnlyEmbeddingProviderDisabled"
        );

        let enabled =
            extract_vectors(&definition.extractor, b"hello", &definition, &test_registry).await;
        assert_eq!(enabled.vectors.len(), 1);
        assert_eq!(enabled.vectors[0].values.len(), 4);
    }

    #[tokio::test]
    async fn vector_text_extraction_uses_configured_command_provider() {
        let mut definition_value = test_vector_definition("configured_provider", 4);
        definition_value["embedding"]["model_version"] = serde_json::json!("v1");
        let definition = VectorIndexDefinition::from_json(&definition_value).unwrap();
        let config = crate::config::Config {
            vector_embedding_providers_json: serde_json::json!({
                "providers": [{
                    "name": "configured_provider",
                    "kind": "command_json",
                    "command": "/bin/sh",
                    "args": [
                        "-c",
                        "cat >/dev/null; printf '%s' '{\"model_version\":\"v1\",\"vectors\":[{\"values\":[0.5,0.5,0.5,0.5],\"chunk_id\":7,\"source_start\":1,\"source_len\":5}]}'"
                    ],
                    "timeout_ms": 5000
                }]
            })
            .to_string(),
            ..crate::config::Config::default()
        };
        let registry = EmbeddingProviderRegistry::from_config(&config).unwrap();

        let extraction =
            extract_vectors(&definition.extractor, b"hello", &definition, &registry).await;

        assert!(
            extraction.diagnostics.is_empty(),
            "{:?}",
            extraction.diagnostics
        );
        assert_eq!(extraction.vectors.len(), 1);
        assert_eq!(extraction.vectors[0].chunk_id, 7);
        assert_eq!(extraction.vectors[0].source_start, 1);
        assert_eq!(extraction.vectors[0].source_len, 5);
        assert_eq!(extraction.vectors[0].values, vec![0.5, 0.5, 0.5, 0.5]);
    }

    fn test_vector_definition(provider: &str, dimension: u16) -> JsonValue {
        serde_json::json!({
            "schema": crate::formats::vector::VECTOR_INDEX_SCHEMA,
            "source": {"kind": "object_current", "prefix": "docs/"},
            "extractor": {"kind": "object_body_utf8"},
            "embedding": {
                "provider": provider,
                "model": "test-text-embedding",
                "dimension": dimension,
                "modality": "text",
                "normalisation": "unit_l2",
                "chunking": {"strategy": "whole_object"}
            },
            "ann": {
                "algorithm": "hnsw",
                "metric": "cosine"
            }
        })
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
            b"alpha body",
        );
        assert_eq!(
            fields
                .fields
                .into_iter()
                .map(|field| field.text)
                .collect::<Vec<_>>(),
            vec!["alpha body", "docs/a.txt", "text/plain"]
        );
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn extractor_supports_json_pointer_and_metadata_fields() {
        let mut object = object("docs/a.json", Some("application/json"));
        object.user_meta = Some(serde_json::json!({
            "owner": "alice",
            "nested": {"department": "legal"}
        }));
        let fields = extract_text_fields(
            &serde_json::json!({
                "fields": [
                    {"source": "json_pointer", "pointer": "/summary"},
                    {"source": "metadata_field", "field": "owner"},
                    {"source": "metadata_field", "field": "/nested/department"}
                ]
            }),
            &object,
            br#"{"summary":"lease renewal due","ignored":true}"#,
        );
        assert_eq!(
            fields
                .fields
                .into_iter()
                .map(|field| field.text)
                .collect::<Vec<_>>(),
            vec!["lease renewal due", "alice", "legal"]
        );
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn extractor_reports_missing_json_pointer() {
        let object = object("docs/a.json", Some("application/json"));
        let fields = extract_text_fields(
            &serde_json::json!({"source": "json_pointer", "pointer": "/missing"}),
            &object,
            br#"{"summary":"present"}"#,
        );
        assert!(fields.fields.is_empty());
        assert_eq!(fields.diagnostics[0].code, "JsonPointerNotFound");
    }

    #[test]
    fn extractor_supports_media_transcript_for_binary_payloads() {
        let object = object("media/audio/a.bin", Some("audio/mpeg"));
        let fields = extract_text_fields(
            &serde_json::json!({"source": "media_transcript"}),
            &object,
            b"\x00\x01audio payload",
        );
        assert_eq!(fields.fields.len(), 1);
        assert!(fields.fields[0].text.contains("Audio media object"));
        assert!(fields.fields[0].text.contains("media/audio/a.bin"));
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn extractor_supports_personaldb_table_column_rows() {
        let object = object("rows/items/1.json", Some("application/json"));
        let fields = extract_text_fields(
            &serde_json::json!({
                "source": "personaldb_table_column",
                "table": "items",
                "column": "name"
            }),
            &object,
            br#"{"table_name":"items","columns":{"id":1,"name":"alpha repair order"}}"#,
        );
        assert_eq!(fields.fields[0].text, "alpha repair order");
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn extractor_skips_non_matching_personaldb_table_column_rows() {
        let object = object("rows/items/1.json", Some("application/json"));
        let fields = extract_text_fields(
            &serde_json::json!({
                "source": "personaldb_table_column",
                "table": "invoices",
                "column": "name"
            }),
            &object,
            br#"{"table_name":"items","columns":{"id":1,"name":"alpha repair order"}}"#,
        );
        assert!(fields.fields.is_empty());
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn extractor_reports_missing_personaldb_table_column() {
        let object = object("rows/items/1.json", Some("application/json"));
        let fields = extract_text_fields(
            &serde_json::json!({
                "source": "personaldb_table_column",
                "column": "name"
            }),
            &object,
            br#"{"table_name":"items","columns":{"id":1}}"#,
        );
        assert!(fields.fields.is_empty());
        assert_eq!(fields.diagnostics[0].code, "PersonalDbTableColumnNotFound");
    }

    #[test]
    fn extractor_supports_personaldb_table_column_json_pointer() {
        let object = object("rows/items/1.json", Some("application/json"));
        let fields = extract_text_fields(
            &serde_json::json!({
                "source": "personaldb_table_column",
                "table": "items",
                "column": "/new_values/name"
            }),
            &object,
            br#"{"table":"items","new_values":{"id":1,"name":"beta inspection note"}}"#,
        );
        assert_eq!(fields.fields[0].text, "beta inspection note");
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn extractor_supports_git_blob_text_as_utf8_payload() {
        let object = object("src/lib.rs", Some("text/plain"));
        let fields = extract_text_fields(
            &serde_json::json!({"source": "git_blob_text"}),
            &object,
            b"fn main() {}",
        );
        assert_eq!(fields.fields[0].text, "fn main() {}");
        assert!(fields.diagnostics.is_empty());
    }

    #[test]
    fn typed_json_row_extracts_body_metadata_and_source_id() {
        let mut object = object("queue/item-1.json", Some("application/json"));
        object.user_meta = Some(serde_json::json!({"owner": "alice"}));
        object.authz_revision = 12;
        let bucket = Bucket {
            id: 1,
            tenant_id: 7,
            name: "jobs".to_string(),
            region: "local".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        let index = index_definition(serde_json::json!({
            "source_kind": "object_current",
            "fields": [
                {"name": "state", "extractor": "/state"},
                {"name": "priority", "extractor": "object_body_json_pointer:/priority"},
                {"name": "owner", "extractor": "object_user_metadata_json_pointer:/owner"},
                {"name": "object_key", "extractor": "object_key"}
            ]
        }));
        let definition = parse_typed_json_build_definition(&index).unwrap();
        let row = typed_json_row_from_object(
            &bucket,
            &definition,
            &object,
            &serde_json::json!({"state": "pending", "priority": 10}),
        )
        .unwrap();

        assert_eq!(row.values["state"], "pending");
        assert_eq!(row.values["priority"], 10);
        assert_eq!(row.values["owner"], "alice");
        assert_eq!(row.values["object_key"], "queue/item-1.json");
        assert_eq!(row.authz_revision, 12);
        assert!(!row.source_id_binary.is_empty());
        assert!(row.encoded_values.contains_key("priority"));
    }

    #[test]
    fn typed_json_required_field_missing_fails_extraction() {
        let object = object("queue/item-1.json", Some("application/json"));
        let bucket = Bucket {
            id: 1,
            tenant_id: 7,
            name: "jobs".to_string(),
            region: "local".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        let index = index_definition(serde_json::json!({
            "source_kind": "object_current",
            "fields": [
                {"name": "state", "extractor": "/state", "required": true}
            ]
        }));
        let definition = parse_typed_json_build_definition(&index).unwrap();
        let err = typed_json_row_from_object(&bucket, &definition, &object, &serde_json::json!({}))
            .unwrap_err();
        assert!(err.to_string().contains("required field missing"));
    }

    #[test]
    fn typed_json_append_row_extracts_payload_and_metadata() {
        let bucket = Bucket {
            id: 1,
            tenant_id: 7,
            name: "events".to_string(),
            region: "local".to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        let stream = AppendStream {
            id: 3,
            tenant_id: 7,
            bucket_id: 1,
            bucket_name: "events".to_string(),
            stream_key: "audit".to_string(),
            stream_id: uuid::Uuid::from_bytes([3; 16]),
            created_at: Utc::now(),
            sealed_at: None,
            segment_hash: None,
        };
        let record = AppendStreamRecord {
            id: 4,
            stream_id: stream.id,
            record_sequence: 2,
            payload_hash: format!("sha256:{}", hex::encode([4u8; 32])),
            payload_object_ref: CoreObjectRef {
                hash: format!("sha256:{}", hex::encode([4u8; 32])),
                logical_size: 64,
                manifest_ref: "manifest:event".to_string(),
            },
            payload_size: 64,
            content_type: Some("application/json".to_string()),
            user_meta: Some(serde_json::json!({"actor": "alice"})),
            created_at: Utc::now(),
        };
        let index = index_definition(serde_json::json!({
            "source_kind": "append_record",
            "fields": [
                {"name": "stream", "extractor": "append_stream_key"},
                {"name": "sequence", "extractor": "append_record_sequence"},
                {"name": "state", "extractor": "append_payload_json_pointer:/state"},
                {"name": "actor", "extractor": "append_user_metadata_json_pointer:/actor"}
            ]
        }));
        let definition = parse_typed_json_build_definition(&index).unwrap();
        let row = typed_json_row_from_append_record(
            &bucket,
            &definition,
            &stream,
            &record,
            &serde_json::json!({"state": "sent"}),
        )
        .unwrap();

        assert_eq!(row.object_key, "audit");
        assert_eq!(row.values["stream"], "audit");
        assert_eq!(row.values["sequence"], 2);
        assert_eq!(row.values["state"], "sent");
        assert_eq!(row.values["actor"], "alice");
        assert!(!row.source_id_binary.is_empty());
    }

    fn object(key: &str, content_type: Option<&str>) -> Object {
        Object {
            id: 1,
            tenant_id: 1,
            bucket_id: 1,
            key: key.to_string(),
            kind: crate::object_links::ObjectEntryKind::Blob,
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
            checksum: None,
            link: None,
        }
    }

    fn index_definition(build_policy: JsonValue) -> IndexDefinition {
        IndexDefinition {
            id: 1,
            tenant_id: 7,
            bucket_id: 1,
            name: "typed".to_string(),
            kind: "typed_json".to_string(),
            selector: JsonValue::Null,
            extractor: JsonValue::Null,
            authorization_mode: "inherit_object".to_string(),
            build_policy,
            enabled: true,
            version: 1,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }
}
