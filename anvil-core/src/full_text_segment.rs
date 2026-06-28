use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32,
    full_text::{BuiltFullTextPostings, FullTextBodyHeader, Posting, TermEntry, decode_postings},
    hash32,
};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FullTextSegmentHeader {
    pub index_id: String,
    pub generation: u64,
    pub tokenizer: serde_json::Value,
    pub scorer: serde_json::Value,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub dictionary_bytes_len: u64,
    pub term_count: u64,
    pub postings_bytes_len: u64,
    pub posting_count: u64,
    pub codec: String,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedFullTextSegment {
    pub header: FullTextSegmentHeader,
    pub body_header: FullTextBodyHeader,
    pub terms: Vec<TermEntry>,
    pub postings: Vec<Posting>,
    pub postings_bytes: Vec<u8>,
    pub document_table: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FullTextSegmentWrite<'a> {
    pub index_id: &'a str,
    pub generation: u64,
    pub tokenizer: serde_json::Value,
    pub scorer: serde_json::Value,
    pub source_cursor: u64,
    pub authz_revision: u64,
    pub built_postings: &'a BuiltFullTextPostings,
    pub document_table: &'a [u8],
}

pub async fn write_full_text_segment(
    storage: &Storage,
    input: FullTextSegmentWrite<'_>,
) -> Result<PathBuf> {
    let encoded_terms = encode_terms(&input.built_postings.terms);
    let postings_bytes = zstd::stream::encode_all(&input.built_postings.postings_bytes[..], 3)
        .context("compress full text postings block")?;
    let body = encode_full_text_body(&encoded_terms, &postings_bytes, input.document_table)?;
    let segment_hash = hash32(&body);
    let path = storage.full_text_segment_path(
        input.index_id,
        input.generation,
        &hex::encode(segment_hash),
    )?;
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }

    let header = FullTextSegmentHeader {
        index_id: input.index_id.to_string(),
        generation: input.generation,
        tokenizer: input.tokenizer,
        scorer: input.scorer,
        source_cursor: input.source_cursor,
        authz_revision: input.authz_revision,
        dictionary_bytes_len: encoded_terms.len() as u64,
        term_count: input.built_postings.terms.len() as u64,
        postings_bytes_len: postings_bytes.len() as u64,
        posting_count: input.built_postings.postings.len() as u64,
        codec: "zstd".to_string(),
        created_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
    };
    let header_json = serde_json::to_vec(&header)?;
    let envelope = BinaryEnvelopeHeader::new(FileFamily::FullTextSegment, 0, 0, header_json);
    let encoded_header = envelope.encode();
    let (first_hash, last_hash) = term_hash_bounds(&input.built_postings.terms);
    let footer = BinaryFileFooter::new(
        &encoded_header,
        &body,
        input.built_postings.terms.len() as u64,
        first_hash,
        last_hash,
    );

    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .await
        .with_context(|| format!("create full text segment {}", path.display()))?;
    file.write_all(&encoded_header).await?;
    file.write_all(&body).await?;
    file.write_all(&footer.encode()).await?;
    file.sync_data().await?;
    Ok(path)
}

pub async fn read_full_text_segment(path: impl Into<PathBuf>) -> Result<DecodedFullTextSegment> {
    let path = path.into();
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read full text segment {}", path.display()))?;
    decode_full_text_segment(&bytes)
}

pub async fn read_latest_full_text_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedFullTextSegment>> {
    let Some(path) = latest_full_text_segment_path(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_full_text_segment(path).await?))
}

pub async fn latest_full_text_segment_path(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<PathBuf>> {
    latest_segment_path(storage.full_text_segment_dir(index_id)?, ".anfts").await
}

pub fn decode_full_text_segment(bytes: &[u8]) -> Result<DecodedFullTextSegment> {
    let envelope = BinaryEnvelopeHeader::decode(bytes)?;
    if envelope.family != FileFamily::FullTextSegment {
        return Err(anyhow!("full text segment file family mismatch"));
    }
    if bytes.len() < COMMON_FOOTER_LEN {
        return Err(anyhow!("full text segment is shorter than footer"));
    }
    let header_end = COMMON_HEADER_LEN
        .checked_add(envelope.header_json.len())
        .ok_or_else(|| anyhow!("full text segment header length overflow"))?;
    let footer_start = bytes
        .len()
        .checked_sub(COMMON_FOOTER_LEN)
        .ok_or_else(|| anyhow!("full text segment footer length overflow"))?;
    if footer_start < header_end {
        return Err(anyhow!("full text segment body overlaps header"));
    }
    let encoded_header = &bytes[..header_end];
    let body = &bytes[header_end..footer_start];
    let footer = BinaryFileFooter::decode(&bytes[footer_start..])?;
    footer.verify(encoded_header, body)?;
    let header: FullTextSegmentHeader = serde_json::from_slice(&envelope.header_json)?;
    decode_full_text_body(header, body)
}

fn encode_full_text_body(
    dictionary_bytes: &[u8],
    postings_bytes: &[u8],
    document_table: &[u8],
) -> Result<Vec<u8>> {
    let document_table_offset = FullTextBodyHeader::encode(&FullTextBodyHeader {
        dictionary_block_count: 1,
        postings_block_count: 1,
        document_table_offset: 0,
    })
    .len()
    .checked_add(dictionary_bytes.len())
    .and_then(|value| value.checked_add(postings_bytes.len()))
    .ok_or_else(|| anyhow!("full text body offset overflow"))?;
    let body_header = FullTextBodyHeader {
        dictionary_block_count: 1,
        postings_block_count: 1,
        document_table_offset: document_table_offset as u64,
    };
    let mut out = Vec::with_capacity(document_table_offset + document_table.len());
    out.extend_from_slice(&body_header.encode());
    out.extend_from_slice(dictionary_bytes);
    out.extend_from_slice(postings_bytes);
    out.extend_from_slice(document_table);
    Ok(out)
}

fn decode_full_text_body(
    header: FullTextSegmentHeader,
    body: &[u8],
) -> Result<DecodedFullTextSegment> {
    let body_header = FullTextBodyHeader::decode(body)?;
    let document_table_offset = usize::try_from(body_header.document_table_offset)
        .context("full text document table offset exceeds usize")?;
    if document_table_offset > body.len() {
        return Err(anyhow!(
            "full text document table offset exceeds body length"
        ));
    }
    let dictionary_start = crate::formats::full_text::FULL_TEXT_BODY_HEADER_LEN;
    let dictionary_len = usize::try_from(header.dictionary_bytes_len)
        .context("full text dictionary length exceeds usize")?;
    let postings_len = usize::try_from(header.postings_bytes_len)
        .context("full text postings length exceeds usize")?;
    let dictionary_end = dictionary_start
        .checked_add(dictionary_len)
        .ok_or_else(|| anyhow!("full text dictionary end overflow"))?;
    let postings_end = dictionary_end
        .checked_add(postings_len)
        .ok_or_else(|| anyhow!("full text postings end overflow"))?;
    if postings_end != document_table_offset {
        return Err(anyhow!(
            "full text header lengths do not match document table offset"
        ));
    }
    if postings_end > body.len() {
        return Err(anyhow!("full text postings exceed body length"));
    }
    let terms = decode_terms(&body[dictionary_start..dictionary_end], header.term_count)?;
    let encoded_postings_bytes = &body[dictionary_end..postings_end];
    let postings_bytes = match header.codec.as_str() {
        "zstd" => zstd::stream::decode_all(encoded_postings_bytes)
            .context("decompress full text postings block")?,
        other => return Err(anyhow!("unsupported full text postings codec {other}")),
    };
    let postings = decode_postings(&postings_bytes)?;
    if postings.len() as u64 != header.posting_count {
        return Err(anyhow!("full text posting count does not match header"));
    }
    let document_table = body[document_table_offset..].to_vec();
    Ok(DecodedFullTextSegment {
        header,
        body_header,
        terms,
        postings,
        postings_bytes,
        document_table,
    })
}

fn encode_terms(terms: &[TermEntry]) -> Vec<u8> {
    let len = terms.iter().map(|term| term.encode().len()).sum();
    let mut out = Vec::with_capacity(len);
    for term in terms {
        out.extend_from_slice(&term.encode());
    }
    out
}

fn decode_terms(mut input: &[u8], expected_count: u64) -> Result<Vec<TermEntry>> {
    let mut terms = Vec::with_capacity(expected_count as usize);
    for _ in 0..expected_count {
        let (term, used) = TermEntry::decode(input)?;
        terms.push(term);
        input = &input[used..];
    }
    if !input.is_empty() {
        return Err(anyhow!("full text dictionary has trailing bytes"));
    }
    Ok(terms)
}

fn term_hash_bounds(terms: &[TermEntry]) -> (Hash32, Hash32) {
    let first = terms
        .first()
        .map(|term| hash32(&term.encode()))
        .unwrap_or([0; 32]);
    let last = terms
        .last()
        .map(|term| hash32(&term.encode()))
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
    use crate::formats::full_text::{FullTextDocument, TokenizerConfig, build_full_text_postings};
    use tempfile::tempdir;

    #[tokio::test]
    async fn full_text_segment_round_trips_built_postings() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let config = TokenizerConfig::default();
        let built = build_full_text_postings(
            &[
                FullTextDocument {
                    document_id: 1,
                    field_id: 1,
                    object_version_id: [1; 16],
                    authz_label_hash: [2; 32],
                    text: "Alpha beta alpha",
                },
                FullTextDocument {
                    document_id: 2,
                    field_id: 1,
                    object_version_id: [3; 16],
                    authz_label_hash: [4; 32],
                    text: "beta gamma",
                },
            ],
            &config,
        );
        let document_table = br#"{"documents":[1,2]}"#;

        let path = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "index-alpha",
                generation: 5,
                tokenizer: serde_json::json!({"language": "simple"}),
                scorer: serde_json::json!({"kind": "bm25"}),
                source_cursor: 44,
                authz_revision: 7,
                built_postings: &built,
                document_table,
            },
        )
        .await
        .unwrap();
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".anfts"))
        );

        let decoded = read_full_text_segment(path).await.unwrap();
        assert_eq!(decoded.header.index_id, "index-alpha");
        assert_eq!(decoded.header.source_cursor, 44);
        assert_eq!(decoded.header.authz_revision, 7);
        assert_eq!(decoded.header.codec, "zstd");
        let expected_compressed = zstd::stream::encode_all(&built.postings_bytes[..], 3).unwrap();
        assert_ne!(
            expected_compressed, built.postings_bytes,
            "test fixture should prove on-disk postings are compressed bytes"
        );
        assert_eq!(
            decoded.header.postings_bytes_len,
            expected_compressed.len() as u64
        );
        assert_eq!(decoded.terms, built.terms);
        assert_eq!(decoded.postings, built.postings);
        assert_eq!(decoded.postings_bytes, built.postings_bytes);
        assert_eq!(decoded.document_table, document_table);
    }

    #[tokio::test]
    async fn full_text_segment_footer_protects_body() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let built = build_full_text_postings(&[], &TokenizerConfig::default());
        let path = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "index-alpha",
                generation: 5,
                tokenizer: serde_json::json!({}),
                scorer: serde_json::json!({}),
                source_cursor: 0,
                authz_revision: 0,
                built_postings: &built,
                document_table: b"",
            },
        )
        .await
        .unwrap();
        let mut bytes = tokio::fs::read(path).await.unwrap();
        bytes[COMMON_HEADER_LEN + 1] ^= 1;
        assert!(decode_full_text_segment(&bytes).is_err());
    }

    #[tokio::test]
    async fn latest_full_text_segment_selects_highest_generation() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let built = build_full_text_postings(&[], &TokenizerConfig::default());
        for generation in [1, 3, 2] {
            write_full_text_segment(
                &storage,
                FullTextSegmentWrite {
                    index_id: "index-alpha",
                    generation,
                    tokenizer: serde_json::json!({}),
                    scorer: serde_json::json!({}),
                    source_cursor: generation,
                    authz_revision: 0,
                    built_postings: &built,
                    document_table: b"",
                },
            )
            .await
            .unwrap();
        }
        let latest = read_latest_full_text_segment(&storage, "index-alpha")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.header.generation, 3);
        assert!(
            latest_full_text_segment_path(&storage, "../escape")
                .await
                .is_err()
        );
    }
}
