use crate::{
    core_store::{CompareAndSwapRef, CoreObjectRef, CoreStore, GetBlob, PutBlob},
    formats::{
        BinaryEnvelopeHeader, BinaryFileFooter, COMMON_FOOTER_LEN, COMMON_HEADER_LEN, FileFamily,
        Hash32,
        full_text::{
            BuiltFullTextPostings, FullTextBodyHeader, Posting, TermEntry, decode_postings,
        },
        hash32,
    },
    storage::Storage,
};
use anyhow::{Context, Result, anyhow};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use std::convert::TryInto;

const FULL_TEXT_SEGMENT_REF_PREFIX: &str = "full_text_segment:";
const CORE_OBJECT_REF_TARGET_PREFIX: &str = "core-object-ref:";

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
) -> Result<String> {
    let encoded_terms = encode_terms(&input.built_postings.terms);
    let encoded_postings_block = encode_postings_block(
        &input.built_postings.postings,
        &input.built_postings.postings_bytes,
    )?;
    let postings_bytes = zstd::stream::encode_all(&encoded_postings_block[..], 3)
        .context("compress full text postings block")?;
    let body = encode_full_text_body(&encoded_terms, &postings_bytes, input.document_table)?;
    let segment_hash = hash32(&body);
    let ref_name =
        full_text_segment_ref_name(input.index_id, input.generation, &hex::encode(segment_hash))?;

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

    let mut bytes = Vec::with_capacity(encoded_header.len() + body.len() + COMMON_FOOTER_LEN);
    bytes.extend_from_slice(&encoded_header);
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&footer.encode());

    let store = CoreStore::new(storage.clone()).await?;
    let object_ref = store
        .put_blob(PutBlob {
            logical_name: ref_name.clone(),
            bytes,
            region_id: "local".to_string(),
            mutation_id: format!("full-text-segment:{}:{}", input.index_id, input.generation),
        })
        .await?;
    if let Err(error) = store
        .compare_and_swap_ref(CompareAndSwapRef {
            ref_name: ref_name.clone(),
            expected_generation: None,
            expected_target: None,
            require_absent: true,
            require_present: false,
            fence: None,
            authz_revision: None,
            source_watch_cursor: None,
            new_target: encode_core_object_ref_target(&object_ref)?,
            transaction_id: None,
        })
        .await
    {
        // The ref name is derived from the deterministic segment body hash. If a
        // concurrent builder published the same segment first, the ref is already
        // valid and this write is complete.
        if read_full_text_segment(storage, &ref_name).await.is_ok() {
            return Ok(ref_name);
        }
        return Err(error);
    }
    Ok(ref_name)
}

pub async fn read_full_text_segment(
    storage: &Storage,
    segment_ref: &str,
) -> Result<DecodedFullTextSegment> {
    let bytes = read_full_text_segment_bytes(storage, segment_ref).await?;
    decode_full_text_segment(&bytes)
}

pub async fn read_full_text_segment_bytes(storage: &Storage, segment_ref: &str) -> Result<Vec<u8>> {
    let store = CoreStore::new(storage.clone()).await?;
    let ref_value = store
        .read_ref(segment_ref)
        .await?
        .ok_or_else(|| anyhow!("full text segment ref is missing"))?;
    store
        .get_blob(GetBlob {
            object_ref: decode_core_object_ref_target(&ref_value.target)?,
        })
        .await
}

pub async fn read_latest_full_text_segment(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<DecodedFullTextSegment>> {
    let Some(segment_ref) = latest_full_text_segment_ref(storage, index_id).await? else {
        return Ok(None);
    };
    Ok(Some(read_full_text_segment(storage, &segment_ref).await?))
}

pub async fn latest_full_text_segment_ref(
    storage: &Storage,
    index_id: &str,
) -> Result<Option<String>> {
    let store = CoreStore::new(storage.clone()).await?;
    let mut refs = store
        .list_ref_names(&full_text_segment_ref_prefix(index_id)?)
        .await?;
    refs.sort_by_key(|value| generation_from_ref(value).unwrap_or(0));
    Ok(refs.pop())
}

pub(crate) async fn full_text_segment_hash_exists(
    storage: &Storage,
    index_id: &str,
    generation: u64,
    expected_segment_hash: &str,
) -> Result<bool> {
    validate_hex32(expected_segment_hash, "full text expected segment hash")?;
    let store = CoreStore::new(storage.clone()).await?;
    let refs = store
        .list_ref_names(&full_text_segment_ref_prefix(index_id)?)
        .await?;
    for segment_ref in refs {
        if generation_from_ref(&segment_ref) != Some(generation) {
            continue;
        }
        let bytes = read_full_text_segment_bytes(storage, &segment_ref).await?;
        if blake3::hash(&bytes).to_hex().as_str() == expected_segment_hash {
            return Ok(true);
        }
    }
    Ok(false)
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

fn full_text_segment_ref_prefix(index_id: &str) -> Result<String> {
    require_safe_component(index_id, "full text index id")?;
    Ok(format!("{FULL_TEXT_SEGMENT_REF_PREFIX}index:{index_id}:"))
}

fn full_text_segment_ref_name(
    index_id: &str,
    generation: u64,
    segment_hash: &str,
) -> Result<String> {
    validate_hex32(segment_hash, "full text segment hash")?;
    Ok(format!(
        "{}generation:{generation:020}:hash:{segment_hash}",
        full_text_segment_ref_prefix(index_id)?
    ))
}

fn generation_from_ref(ref_name: &str) -> Option<u64> {
    ref_name
        .rsplit_once(":generation:")?
        .1
        .split(':')
        .next()?
        .parse()
        .ok()
}

fn require_safe_component(value: &str, field: &'static str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.contains(':')
        || value.chars().any(char::is_control)
    {
        return Err(anyhow!("{field} is not a safe component"));
    }
    Ok(())
}

fn validate_hex32(value: &str, field: &'static str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(anyhow!("{field} must be hex32"));
    }
    Ok(())
}

fn encode_core_object_ref_target(object_ref: &CoreObjectRef) -> Result<String> {
    Ok(format!(
        "{CORE_OBJECT_REF_TARGET_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(object_ref)?)
    ))
}

fn decode_core_object_ref_target(target: &str) -> Result<CoreObjectRef> {
    let encoded = target
        .strip_prefix(CORE_OBJECT_REF_TARGET_PREFIX)
        .ok_or_else(|| anyhow!("CoreStore ref target is not a CoreObjectRef"))?;
    Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(encoded)?)?)
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

        let segment_ref = write_full_text_segment(
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
        assert!(segment_ref.starts_with(FULL_TEXT_SEGMENT_REF_PREFIX));

        let decoded = read_full_text_segment(&storage, &segment_ref)
            .await
            .unwrap();
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

        let segment_ref = write_full_text_segment(
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

        let decoded = read_full_text_segment(&storage, &segment_ref)
            .await
            .unwrap();
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
        let segment_ref = write_full_text_segment(
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
        let mut bytes = read_full_text_segment_bytes(&storage, &segment_ref)
            .await
            .unwrap();
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
            latest_full_text_segment_ref(&storage, "../escape")
                .await
                .is_err()
        );
    }
}
