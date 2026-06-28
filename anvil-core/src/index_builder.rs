use crate::formats::{full_text::FullTextDocument, full_text::FullTextIndexDefinition, hash32};
use crate::full_text_segment::{self, FullTextSegmentWrite};
use crate::index_journal;
use crate::index_partition_watch::{self, IndexPartitionWatchPayload};
use crate::metadata_journal;
use crate::persistence::{Bucket, IndexDefinition, Object};
use crate::storage::{ExternalChunkManifest, Storage};
use anyhow::{Context, Result, anyhow};
use serde::Serialize;
use serde_json::Value as JsonValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexBuildOutcome {
    pub index_storage_id: String,
    pub generation: u64,
    pub document_count: usize,
    pub source_cursor: u128,
    pub segment_hash: String,
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
    index_partition_watch::append_index_partition_watch_record(
        storage,
        index.tenant_id,
        index.bucket_id,
        &partition_id,
        source_cursor.max(u128::from(generation)),
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
        generation,
        document_count: owned_documents.len(),
        source_cursor,
        segment_hash,
    })
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
