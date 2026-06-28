use crate::formats::{
    BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
    Hash32,
    full_text::{BuiltFullTextPostings, FullTextBodyHeader, Posting, TermEntry, decode_postings},
    hash32,
};
use crate::storage::Storage;
use anyhow::{Context, Result, anyhow};
use serde::{Deserialize, Serialize};
use std::{convert::TryInto, path::PathBuf};
use tokio::io::AsyncWriteExt;

const FULL_TEXT_POSTINGS_BLOCK_MAGIC: &[u8; 8] = b"ANVFTSPB";
const FULL_TEXT_POSTINGS_BLOCK_VERSION: u16 = 1;
const FULL_TEXT_POSTINGS_SKIP_STRIDE: u32 = 128;
const FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN: usize = 8 + 2 + 2 + 4 + 8 + 8 + 4;
const FULL_TEXT_SKIP_ENTRY_LEN: usize = 8 + 8 + 8 + 2 + 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextSkipEntry {
    pub posting_index: u64,
    pub postings_offset: u64,
    pub document_id: u64,
    pub field_id: u16,
    pub object_version_id: [u8; 16],
}

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
    pub posting_skips: Vec<FullTextSkipEntry>,
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
    let encoded_postings_block = encode_postings_block(
        &input.built_postings.postings,
        &input.built_postings.postings_bytes,
    )?;
    let postings_bytes = zstd::stream::encode_all(&encoded_postings_block[..], 3)
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
    let encoded_postings_block = match header.codec.as_str() {
        "zstd" => zstd::stream::decode_all(encoded_postings_bytes)
            .context("decompress full text postings block")?,
        other => return Err(anyhow!("unsupported full text postings codec {other}")),
    };
    let (postings_bytes, posting_skips) =
        decode_postings_block(&encoded_postings_block, header.posting_count)?;
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
        posting_skips,
        postings_bytes,
        document_table,
    })
}

fn encode_postings_block(postings: &[Posting], raw_postings_bytes: &[u8]) -> Result<Vec<u8>> {
    let skip_entries = build_skip_entries(postings, raw_postings_bytes)?;
    let skip_bytes_len = skip_entries
        .len()
        .checked_mul(FULL_TEXT_SKIP_ENTRY_LEN)
        .ok_or_else(|| anyhow!("full text skip table length overflow"))?;
    let capacity = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN
        .checked_add(skip_bytes_len)
        .and_then(|value| value.checked_add(raw_postings_bytes.len()))
        .ok_or_else(|| anyhow!("full text postings block length overflow"))?;
    let mut out = Vec::with_capacity(capacity);
    out.extend_from_slice(FULL_TEXT_POSTINGS_BLOCK_MAGIC);
    out.extend_from_slice(&FULL_TEXT_POSTINGS_BLOCK_VERSION.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.extend_from_slice(&FULL_TEXT_POSTINGS_SKIP_STRIDE.to_le_bytes());
    out.extend_from_slice(&(postings.len() as u64).to_le_bytes());
    out.extend_from_slice(&(raw_postings_bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(&(skip_entries.len() as u32).to_le_bytes());
    for entry in &skip_entries {
        out.extend_from_slice(&entry.posting_index.to_le_bytes());
        out.extend_from_slice(&entry.postings_offset.to_le_bytes());
        out.extend_from_slice(&entry.document_id.to_le_bytes());
        out.extend_from_slice(&entry.field_id.to_le_bytes());
        out.extend_from_slice(&entry.object_version_id);
    }
    out.extend_from_slice(raw_postings_bytes);
    Ok(out)
}

fn build_skip_entries(
    postings: &[Posting],
    raw_postings_bytes: &[u8],
) -> Result<Vec<FullTextSkipEntry>> {
    let mut skips = Vec::new();
    let mut cursor = 0usize;
    for (idx, posting) in postings.iter().enumerate() {
        let encoded = posting.encode();
        let end = cursor
            .checked_add(encoded.len())
            .ok_or_else(|| anyhow!("full text posting offset overflow"))?;
        if raw_postings_bytes.get(cursor..end) != Some(encoded.as_slice()) {
            return Err(anyhow!(
                "full text raw postings bytes do not match posting entries"
            ));
        }
        if idx % FULL_TEXT_POSTINGS_SKIP_STRIDE as usize == 0 {
            skips.push(FullTextSkipEntry {
                posting_index: idx as u64,
                postings_offset: cursor as u64,
                document_id: posting.document_id,
                field_id: posting.field_id,
                object_version_id: posting.object_version_id,
            });
        }
        cursor = end;
    }
    if cursor != raw_postings_bytes.len() {
        return Err(anyhow!(
            "full text raw postings bytes contain trailing data"
        ));
    }
    Ok(skips)
}

fn decode_postings_block(
    input: &[u8],
    expected_posting_count: u64,
) -> Result<(Vec<u8>, Vec<FullTextSkipEntry>)> {
    if input.len() < FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN {
        return Err(anyhow!("full text postings block is shorter than header"));
    }
    if &input[0..8] != FULL_TEXT_POSTINGS_BLOCK_MAGIC {
        return Err(anyhow!("full text postings block magic mismatch"));
    }
    let version = u16::from_le_bytes(input[8..10].try_into().unwrap());
    if version != FULL_TEXT_POSTINGS_BLOCK_VERSION {
        return Err(anyhow!(
            "unsupported full text postings block version {version}"
        ));
    }
    let reserved = u16::from_le_bytes(input[10..12].try_into().unwrap());
    if reserved != 0 {
        return Err(anyhow!(
            "full text postings block reserved bytes are non-zero"
        ));
    }
    let skip_stride = u32::from_le_bytes(input[12..16].try_into().unwrap());
    if skip_stride != FULL_TEXT_POSTINGS_SKIP_STRIDE {
        return Err(anyhow!(
            "unsupported full text postings skip stride {skip_stride}"
        ));
    }
    let posting_count = u64::from_le_bytes(input[16..24].try_into().unwrap());
    if posting_count != expected_posting_count {
        return Err(anyhow!(
            "full text postings block count does not match segment header"
        ));
    }
    let raw_postings_len = usize::try_from(u64::from_le_bytes(input[24..32].try_into().unwrap()))
        .context("full text raw postings length exceeds usize")?;
    let skip_count = usize::try_from(u32::from_le_bytes(input[32..36].try_into().unwrap()))
        .context("full text skip count exceeds usize")?;
    let skip_bytes_len = skip_count
        .checked_mul(FULL_TEXT_SKIP_ENTRY_LEN)
        .ok_or_else(|| anyhow!("full text skip table length overflow"))?;
    let raw_start = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN
        .checked_add(skip_bytes_len)
        .ok_or_else(|| anyhow!("full text postings block raw start overflow"))?;
    let raw_end = raw_start
        .checked_add(raw_postings_len)
        .ok_or_else(|| anyhow!("full text postings block raw end overflow"))?;
    if raw_end != input.len() {
        return Err(anyhow!(
            "full text postings block lengths do not match payload length"
        ));
    }

    let mut skips = Vec::with_capacity(skip_count);
    let mut cursor = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN;
    for _ in 0..skip_count {
        skips.push(FullTextSkipEntry {
            posting_index: u64::from_le_bytes(input[cursor..cursor + 8].try_into().unwrap()),
            postings_offset: u64::from_le_bytes(input[cursor + 8..cursor + 16].try_into().unwrap()),
            document_id: u64::from_le_bytes(input[cursor + 16..cursor + 24].try_into().unwrap()),
            field_id: u16::from_le_bytes(input[cursor + 24..cursor + 26].try_into().unwrap()),
            object_version_id: input[cursor + 26..cursor + 42].try_into().unwrap(),
        });
        cursor += FULL_TEXT_SKIP_ENTRY_LEN;
    }
    let raw_postings = input[raw_start..raw_end].to_vec();
    validate_skip_entries(&raw_postings, &skips, posting_count)?;
    Ok((raw_postings, skips))
}

fn validate_skip_entries(
    raw_postings_bytes: &[u8],
    skips: &[FullTextSkipEntry],
    posting_count: u64,
) -> Result<()> {
    let expected_skip_count = if posting_count == 0 {
        0
    } else {
        ((posting_count - 1) / FULL_TEXT_POSTINGS_SKIP_STRIDE as u64) + 1
    };
    if skips.len() as u64 != expected_skip_count {
        return Err(anyhow!("full text skip count does not match posting count"));
    }

    let mut next_skip = 0usize;
    let mut cursor = 0usize;
    let mut posting_index = 0u64;
    while cursor < raw_postings_bytes.len() {
        let (posting, used) = Posting::decode(&raw_postings_bytes[cursor..])
            .map_err(|err| anyhow!("decode full text posting for skip validation: {err}"))?;
        if posting_index % FULL_TEXT_POSTINGS_SKIP_STRIDE as u64 == 0 {
            let Some(skip) = skips.get(next_skip) else {
                return Err(anyhow!("full text skip table ended early"));
            };
            if skip.posting_index != posting_index
                || skip.postings_offset != cursor as u64
                || skip.document_id != posting.document_id
                || skip.field_id != posting.field_id
                || skip.object_version_id != posting.object_version_id
            {
                return Err(anyhow!(
                    "full text skip entry does not match posting at index {posting_index}"
                ));
            }
            next_skip += 1;
        }
        cursor = cursor
            .checked_add(used)
            .ok_or_else(|| anyhow!("full text posting cursor overflow"))?;
        posting_index += 1;
    }
    if posting_index != posting_count {
        return Err(anyhow!(
            "full text decoded posting count does not match block header"
        ));
    }
    if next_skip != skips.len() {
        return Err(anyhow!("full text skip table contains trailing entries"));
    }
    Ok(())
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
        let expected_block = encode_postings_block(&built.postings, &built.postings_bytes).unwrap();
        let expected_compressed = zstd::stream::encode_all(&expected_block[..], 3).unwrap();
        assert_ne!(
            expected_compressed, expected_block,
            "test fixture should prove on-disk postings are compressed bytes"
        );
        assert_eq!(
            decoded.header.postings_bytes_len,
            expected_compressed.len() as u64
        );
        assert_eq!(decoded.terms, built.terms);
        assert_eq!(decoded.postings, built.postings);
        assert_eq!(
            decoded.posting_skips,
            build_skip_entries(&built.postings, &built.postings_bytes).unwrap()
        );
        assert_eq!(decoded.postings_bytes, built.postings_bytes);
        assert_eq!(decoded.document_table, document_table);
    }

    #[tokio::test]
    async fn full_text_segment_writes_skip_data_every_128_postings() {
        let temp = tempdir().unwrap();
        let storage = Storage::new_at(temp.path()).await.unwrap();
        let config = TokenizerConfig::default();
        let documents = (0..260)
            .map(|idx| FullTextDocument {
                document_id: idx + 1,
                field_id: 1,
                object_version_id: [idx as u8; 16],
                authz_label_hash: [9; 32],
                text: "shared",
            })
            .collect::<Vec<_>>();
        let built = build_full_text_postings(&documents, &config);

        let path = write_full_text_segment(
            &storage,
            FullTextSegmentWrite {
                index_id: "index-shared",
                generation: 1,
                tokenizer: serde_json::json!({"language": "simple"}),
                scorer: serde_json::json!({"kind": "bm25"}),
                source_cursor: 260,
                authz_revision: 11,
                built_postings: &built,
                document_table: b"",
            },
        )
        .await
        .unwrap();

        let decoded = read_full_text_segment(path).await.unwrap();
        assert_eq!(decoded.postings.len(), 260);
        assert_eq!(decoded.postings, built.postings);
        assert_eq!(decoded.postings_bytes, built.postings_bytes);
        assert_eq!(
            decoded
                .posting_skips
                .iter()
                .map(|entry| entry.posting_index)
                .collect::<Vec<_>>(),
            vec![0, 128, 256]
        );

        let mut cursor = 0u64;
        let mut expected_offsets = Vec::new();
        for (idx, posting) in built.postings.iter().enumerate() {
            if idx % FULL_TEXT_POSTINGS_SKIP_STRIDE as usize == 0 {
                expected_offsets.push(cursor);
            }
            cursor += posting.encode().len() as u64;
        }
        assert_eq!(
            decoded
                .posting_skips
                .iter()
                .map(|entry| entry.postings_offset)
                .collect::<Vec<_>>(),
            expected_offsets
        );
    }

    #[test]
    fn full_text_postings_block_rejects_corrupt_skip_data() {
        let documents = (0..130)
            .map(|idx| FullTextDocument {
                document_id: idx + 1,
                field_id: 1,
                object_version_id: [idx as u8; 16],
                authz_label_hash: [8; 32],
                text: "shared",
            })
            .collect::<Vec<_>>();
        let built = build_full_text_postings(&documents, &TokenizerConfig::default());
        let mut encoded = encode_postings_block(&built.postings, &built.postings_bytes).unwrap();

        let second_skip_offset = FULL_TEXT_POSTINGS_BLOCK_HEADER_LEN + FULL_TEXT_SKIP_ENTRY_LEN + 8;
        encoded[second_skip_offset] ^= 1;

        assert!(
            decode_postings_block(&encoded, built.postings.len() as u64)
                .unwrap_err()
                .to_string()
                .contains("skip entry does not match")
        );
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
