use super::{FormatError, Hash32, hash32};
use std::convert::TryInto;

pub const FULL_TEXT_BODY_HEADER_LEN: usize = 16;
const TERM_ENTRY_FIXED_LEN: usize = 32 + 2 + 4 + 8 + 4;
const POSTING_FIXED_LEN: usize = 8 + 2 + 2 + 16 + 32 + 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextBodyHeader {
    pub dictionary_block_count: u32,
    pub postings_block_count: u32,
    pub document_table_offset: u64,
}

impl FullTextBodyHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(FULL_TEXT_BODY_HEADER_LEN);
        out.extend_from_slice(&self.dictionary_block_count.to_le_bytes());
        out.extend_from_slice(&self.postings_block_count.to_le_bytes());
        out.extend_from_slice(&self.document_table_offset.to_le_bytes());
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < FULL_TEXT_BODY_HEADER_LEN {
            return Err(FormatError::TooShort {
                context: "full text body header",
                needed: FULL_TEXT_BODY_HEADER_LEN,
                actual: input.len(),
            });
        }
        Ok(Self {
            dictionary_block_count: u32::from_le_bytes(input[0..4].try_into().unwrap()),
            postings_block_count: u32::from_le_bytes(input[4..8].try_into().unwrap()),
            document_table_offset: u64::from_le_bytes(input[8..16].try_into().unwrap()),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TermEntry {
    pub term_hash: Hash32,
    pub term_utf8: Vec<u8>,
    pub doc_frequency: u32,
    pub postings_offset: u64,
    pub postings_len: u32,
}

impl TermEntry {
    pub fn new(
        term_utf8: Vec<u8>,
        doc_frequency: u32,
        postings_offset: u64,
        postings_len: u32,
    ) -> Self {
        Self {
            term_hash: hash32(&term_utf8),
            term_utf8,
            doc_frequency,
            postings_offset,
            postings_len,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(TERM_ENTRY_FIXED_LEN + self.term_utf8.len());
        out.extend_from_slice(&self.term_hash);
        out.extend_from_slice(&(self.term_utf8.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.term_utf8);
        out.extend_from_slice(&self.doc_frequency.to_le_bytes());
        out.extend_from_slice(&self.postings_offset.to_le_bytes());
        out.extend_from_slice(&self.postings_len.to_le_bytes());
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < TERM_ENTRY_FIXED_LEN {
            return Err(FormatError::TooShort {
                context: "full text term entry",
                needed: TERM_ENTRY_FIXED_LEN,
                actual: input.len(),
            });
        }
        let term_len = u16::from_le_bytes(input[32..34].try_into().unwrap()) as usize;
        let term_start: usize = 34;
        let doc_frequency_offset =
            term_start
                .checked_add(term_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "full text term bytes",
                })?;
        let record_end = doc_frequency_offset + 4 + 8 + 4;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "full text term entry bytes",
                needed: record_end,
                actual: input.len(),
            });
        }
        let term_utf8 = input[term_start..doc_frequency_offset].to_vec();
        let term_hash = input[0..32].try_into().unwrap();
        if hash32(&term_utf8) != term_hash {
            return Err(FormatError::HashMismatch {
                context: "full text term",
            });
        }
        Ok((
            Self {
                term_hash,
                term_utf8,
                doc_frequency: u32::from_le_bytes(
                    input[doc_frequency_offset..doc_frequency_offset + 4]
                        .try_into()
                        .unwrap(),
                ),
                postings_offset: u64::from_le_bytes(
                    input[doc_frequency_offset + 4..doc_frequency_offset + 12]
                        .try_into()
                        .unwrap(),
                ),
                postings_len: u32::from_le_bytes(
                    input[doc_frequency_offset + 12..doc_frequency_offset + 16]
                        .try_into()
                        .unwrap(),
                ),
            },
            record_end,
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    pub document_id: u64,
    pub field_id: u16,
    pub term_frequency: u16,
    pub object_version_id: [u8; 16],
    pub authz_label_hash: Hash32,
    pub delta_positions: Vec<u32>,
}

impl Posting {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(POSTING_FIXED_LEN + self.delta_positions.len() * 4);
        out.extend_from_slice(&self.document_id.to_le_bytes());
        out.extend_from_slice(&self.field_id.to_le_bytes());
        out.extend_from_slice(&self.term_frequency.to_le_bytes());
        out.extend_from_slice(&self.object_version_id);
        out.extend_from_slice(&self.authz_label_hash);
        out.extend_from_slice(&(self.delta_positions.len() as u16).to_le_bytes());
        for position in &self.delta_positions {
            out.extend_from_slice(&position.to_le_bytes());
        }
        out
    }

    pub fn decode(input: &[u8]) -> Result<(Self, usize), FormatError> {
        if input.len() < POSTING_FIXED_LEN {
            return Err(FormatError::TooShort {
                context: "full text posting",
                needed: POSTING_FIXED_LEN,
                actual: input.len(),
            });
        }
        let position_count = u16::from_le_bytes(input[60..62].try_into().unwrap()) as usize;
        let positions_len =
            position_count
                .checked_mul(4)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "full text posting positions",
                })?;
        let record_end = POSTING_FIXED_LEN.checked_add(positions_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "full text posting",
            },
        )?;
        if input.len() < record_end {
            return Err(FormatError::TooShort {
                context: "full text posting positions",
                needed: record_end,
                actual: input.len(),
            });
        }
        let mut delta_positions = Vec::with_capacity(position_count);
        let mut cursor = POSTING_FIXED_LEN;
        for _ in 0..position_count {
            delta_positions.push(u32::from_le_bytes(
                input[cursor..cursor + 4].try_into().unwrap(),
            ));
            cursor += 4;
        }
        Ok((
            Self {
                document_id: u64::from_le_bytes(input[0..8].try_into().unwrap()),
                field_id: u16::from_le_bytes(input[8..10].try_into().unwrap()),
                term_frequency: u16::from_le_bytes(input[10..12].try_into().unwrap()),
                object_version_id: input[12..28].try_into().unwrap(),
                authz_label_hash: input[28..60].try_into().unwrap(),
                delta_positions,
            },
            record_end,
        ))
    }
}

pub fn decode_postings(input: &[u8]) -> Result<Vec<Posting>, FormatError> {
    let mut postings = Vec::new();
    let mut cursor = 0usize;
    while cursor < input.len() {
        let (posting, used) = Posting::decode(&input[cursor..])?;
        if used == 0 {
            return Err(FormatError::InvalidDeclaredLength {
                context: "full text posting",
            });
        }
        cursor += used;
        postings.push(posting);
    }
    Ok(postings)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenizerConfig {
    pub max_token_chars: usize,
    pub lowercase: bool,
    pub normalize_nfkc: bool,
    pub record_original_ranges: bool,
}

impl Default for TokenizerConfig {
    fn default() -> Self {
        Self {
            max_token_chars: 128,
            lowercase: true,
            normalize_nfkc: true,
            record_original_ranges: true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextIndexDefinition {
    pub positions_enabled: bool,
    pub language: String,
    pub tokenizer: TokenizerConfig,
    pub stop_words_enabled: bool,
    pub stemming: Option<String>,
    pub require_index_success: bool,
}

impl FullTextIndexDefinition {
    pub fn from_json(value: &serde_json::Value) -> Result<Self, FormatError> {
        let object = value
            .as_object()
            .ok_or(FormatError::InvalidFullTextIndexDefinition { field: "root" })?;
        let positions_enabled = optional_bool(object, "positions", true)?;
        let language = optional_str(object, "language", "simple")?.to_string();
        if language.trim().is_empty() {
            return Err(FormatError::InvalidFullTextIndexDefinition { field: "language" });
        }
        let max_token_chars = optional_usize(object, "max_token_chars", 128)?;
        if max_token_chars == 0 || max_token_chars > 128 {
            return Err(FormatError::InvalidFullTextIndexDefinition {
                field: "max_token_chars",
            });
        }
        let lowercase = optional_bool(object, "lowercase", true)?;
        let normalize_nfkc = optional_bool(object, "normalize_nfkc", true)?;
        let record_original_ranges = optional_bool(object, "record_original_ranges", true)?;
        let stop_words_enabled = optional_bool(object, "stop_words_enabled", false)?;
        let stemming = optional_string(object, "stemming")?;
        if stemming.as_deref().is_some_and(str::is_empty) {
            return Err(FormatError::InvalidFullTextIndexDefinition { field: "stemming" });
        }
        let require_index_success = optional_bool(object, "require_index_success", false)?;
        Ok(Self {
            positions_enabled,
            language,
            tokenizer: TokenizerConfig {
                max_token_chars,
                lowercase,
                normalize_nfkc,
                record_original_ranges,
            },
            stop_words_enabled,
            stemming,
            require_index_success,
        })
    }
}

fn optional_bool(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
    default: bool,
) -> Result<bool, FormatError> {
    match object.get(field) {
        Some(value) => value
            .as_bool()
            .ok_or(FormatError::InvalidFullTextIndexDefinition { field }),
        None => Ok(default),
    }
}

fn optional_str<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    field: &'static str,
    default: &'a str,
) -> Result<&'a str, FormatError> {
    match object.get(field) {
        Some(value) => value
            .as_str()
            .ok_or(FormatError::InvalidFullTextIndexDefinition { field }),
        None => Ok(default),
    }
}

fn optional_string(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
) -> Result<Option<String>, FormatError> {
    match object.get(field) {
        Some(value) => value
            .as_str()
            .map(|value| Some(value.to_string()))
            .ok_or(FormatError::InvalidFullTextIndexDefinition { field }),
        None => Ok(None),
    }
}

fn optional_usize(
    object: &serde_json::Map<String, serde_json::Value>,
    field: &'static str,
    default: usize,
) -> Result<usize, FormatError> {
    match object.get(field) {
        Some(value) => {
            let value = value
                .as_u64()
                .ok_or(FormatError::InvalidFullTextIndexDefinition { field })?;
            usize::try_from(value)
                .map_err(|_| FormatError::InvalidFullTextIndexDefinition { field })
        }
        None => Ok(default),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub term: String,
    pub position: u32,
    pub normalized_byte_start: usize,
    pub normalized_byte_end: usize,
    pub original_byte_start: usize,
    pub original_byte_end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedCharSpan {
    normalized_start: usize,
    normalized_end: usize,
    original_start: usize,
    original_end: usize,
}

pub fn tokenize_text(input: &str, config: &TokenizerConfig) -> Vec<Token> {
    use unicode_segmentation::UnicodeSegmentation;

    let (normalized, spans) = normalize_for_full_text(input, config);
    let mut tokens = Vec::new();
    let mut position = 0u32;

    for (start, segment) in normalized.split_word_bound_indices() {
        if !is_index_word_segment(segment) {
            continue;
        }
        let end = start + segment.len();
        push_token(
            &normalized,
            &spans,
            start,
            end,
            segment.chars().count(),
            &mut position,
            &mut tokens,
            config,
        );
    }
    tokens
}

fn normalize_for_full_text(
    input: &str,
    config: &TokenizerConfig,
) -> (String, Vec<NormalizedCharSpan>) {
    use unicode_normalization::UnicodeNormalization;
    use unicode_segmentation::UnicodeSegmentation;

    let mut normalized = String::with_capacity(input.len());
    let mut spans = Vec::new();

    for (original_start, grapheme) in input.grapheme_indices(true) {
        let original_end = original_start + grapheme.len();
        let source = if config.normalize_nfkc {
            grapheme.nfkc().collect::<String>()
        } else {
            grapheme.to_string()
        };
        let folded = if config.lowercase {
            caseless::default_case_fold_str(&source)
        } else {
            source
        };
        for folded_ch in folded.chars() {
            let normalized_start = normalized.len();
            normalized.push(folded_ch);
            let normalized_end = normalized.len();
            spans.push(NormalizedCharSpan {
                normalized_start,
                normalized_end,
                original_start,
                original_end,
            });
        }
    }

    (normalized, spans)
}

fn is_index_word_segment(segment: &str) -> bool {
    segment.chars().any(char::is_alphanumeric)
}

fn push_token(
    normalized: &str,
    spans: &[NormalizedCharSpan],
    start: usize,
    end: usize,
    char_count: usize,
    position: &mut u32,
    tokens: &mut Vec<Token>,
    config: &TokenizerConfig,
) {
    if char_count == 0 {
        return;
    }
    let current_position = *position;
    *position = position.saturating_add(1);
    if char_count > config.max_token_chars {
        return;
    }
    let (original_byte_start, original_byte_end) = if config.record_original_ranges {
        normalized_range_to_original(spans, start, end).unwrap_or((0, 0))
    } else {
        (0, 0)
    };
    tokens.push(Token {
        term: normalized[start..end].to_string(),
        position: current_position,
        normalized_byte_start: start,
        normalized_byte_end: end,
        original_byte_start,
        original_byte_end,
    });
}

fn normalized_range_to_original(
    spans: &[NormalizedCharSpan],
    start: usize,
    end: usize,
) -> Option<(usize, usize)> {
    let mut original_start = None;
    let mut original_end = None;
    for span in spans {
        if span.normalized_end <= start || span.normalized_start >= end {
            continue;
        }
        original_start = Some(original_start.map_or(span.original_start, |value: usize| {
            value.min(span.original_start)
        }));
        original_end = Some(original_end.map_or(span.original_end, |value: usize| {
            value.max(span.original_end)
        }));
    }
    Some((original_start?, original_end?))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullTextDocument<'a> {
    pub document_id: u64,
    pub field_id: u16,
    pub object_version_id: [u8; 16],
    pub authz_label_hash: Hash32,
    pub text: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuiltFullTextPostings {
    pub terms: Vec<TermEntry>,
    pub postings: Vec<Posting>,
    pub postings_bytes: Vec<u8>,
}

pub fn build_full_text_postings(
    documents: &[FullTextDocument<'_>],
    config: &TokenizerConfig,
) -> BuiltFullTextPostings {
    use std::collections::BTreeMap;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct PostingKey {
        document_id: u64,
        field_id: u16,
        object_version_id: [u8; 16],
        authz_label_hash: Hash32,
    }

    let mut by_term: BTreeMap<Vec<u8>, BTreeMap<PostingKey, Vec<u32>>> = BTreeMap::new();
    for document in documents {
        for token in tokenize_text(document.text, config) {
            let key = PostingKey {
                document_id: document.document_id,
                field_id: document.field_id,
                object_version_id: document.object_version_id,
                authz_label_hash: document.authz_label_hash,
            };
            by_term
                .entry(token.term.into_bytes())
                .or_default()
                .entry(key)
                .or_default()
                .push(token.position);
        }
    }

    let mut terms = Vec::with_capacity(by_term.len());
    let mut postings = Vec::new();
    let mut postings_bytes = Vec::new();

    for (term_utf8, postings_for_term) in by_term {
        let postings_offset = postings_bytes.len() as u64;
        let start_len = postings_bytes.len();
        let doc_frequency = postings_for_term.len().min(u32::MAX as usize) as u32;
        for (key, positions) in postings_for_term {
            let delta_positions = delta_encode_positions(&positions);
            let posting = Posting {
                document_id: key.document_id,
                field_id: key.field_id,
                term_frequency: positions.len().min(u16::MAX as usize) as u16,
                object_version_id: key.object_version_id,
                authz_label_hash: key.authz_label_hash,
                delta_positions,
            };
            postings_bytes.extend_from_slice(&posting.encode());
            postings.push(posting);
        }
        let postings_len = (postings_bytes.len() - start_len) as u32;
        terms.push(TermEntry::new(
            term_utf8,
            doc_frequency,
            postings_offset,
            postings_len,
        ));
    }

    BuiltFullTextPostings {
        terms,
        postings,
        postings_bytes,
    }
}

fn delta_encode_positions(positions: &[u32]) -> Vec<u32> {
    let mut previous = 0u32;
    positions
        .iter()
        .enumerate()
        .map(|(idx, position)| {
            let delta = if idx == 0 {
                *position
            } else {
                position.saturating_sub(previous)
            };
            previous = *position;
            delta
        })
        .collect()
}

pub fn delta_decode_positions(delta_positions: &[u32]) -> Vec<u32> {
    let mut current = 0u32;
    delta_positions
        .iter()
        .enumerate()
        .map(|(idx, delta)| {
            current = if idx == 0 {
                *delta
            } else {
                current.saturating_add(*delta)
            };
            current
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FullTextQueryError {
    EmptyPhrase,
    PositionsDisabled,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct PhraseMatch {
    pub document_id: u64,
    pub field_id: u16,
    pub object_version_id: [u8; 16],
    pub authz_label_hash: Hash32,
}

pub fn evaluate_phrase_query(
    postings_by_term: &[&[Posting]],
    positions_enabled: bool,
) -> Result<Vec<PhraseMatch>, FullTextQueryError> {
    use std::collections::{BTreeMap, BTreeSet};

    if postings_by_term.is_empty() {
        return Err(FullTextQueryError::EmptyPhrase);
    }
    if !positions_enabled {
        return Err(FullTextQueryError::PositionsDisabled);
    }

    let mut term_maps: Vec<BTreeMap<PhraseMatch, BTreeSet<u32>>> =
        Vec::with_capacity(postings_by_term.len());
    for postings in postings_by_term {
        let mut by_document = BTreeMap::new();
        for posting in *postings {
            if posting.term_frequency > 0 && posting.delta_positions.is_empty() {
                return Err(FullTextQueryError::PositionsDisabled);
            }
            by_document.insert(
                PhraseMatch {
                    document_id: posting.document_id,
                    field_id: posting.field_id,
                    object_version_id: posting.object_version_id,
                    authz_label_hash: posting.authz_label_hash,
                },
                delta_decode_positions(&posting.delta_positions)
                    .into_iter()
                    .collect(),
            );
        }
        term_maps.push(by_document);
    }

    let Some(first_term) = term_maps.first() else {
        return Err(FullTextQueryError::EmptyPhrase);
    };
    let mut matches = Vec::new();
    'candidate: for (document, first_positions) in first_term {
        for term_map in term_maps.iter().skip(1) {
            if !term_map.contains_key(document) {
                continue 'candidate;
            }
        }
        'start_position: for start_position in first_positions {
            for (term_index, term_map) in term_maps.iter().enumerate().skip(1) {
                let expected = start_position.saturating_add(term_index as u32);
                if !term_map
                    .get(document)
                    .is_some_and(|positions| positions.contains(&expected))
                {
                    continue 'start_position;
                }
            }
            matches.push(document.clone());
            continue 'candidate;
        }
    }
    Ok(matches)
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25Config {
    pub k1: f32,
    pub b: f32,
}

impl Default for Bm25Config {
    fn default() -> Self {
        Self { k1: 1.2, b: 0.75 }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bm25FieldStats {
    pub document_count: u32,
    pub average_field_length: f32,
}

impl Bm25FieldStats {
    pub fn from_field_lengths(lengths: &[u32]) -> Self {
        if lengths.is_empty() {
            return Self {
                document_count: 0,
                average_field_length: 0.0,
            };
        }
        let total = lengths
            .iter()
            .fold(0u64, |sum, length| sum.saturating_add(*length as u64));
        Self {
            document_count: lengths.len().min(u32::MAX as usize) as u32,
            average_field_length: total as f32 / lengths.len() as f32,
        }
    }
}

pub fn bm25_score(
    term_frequency: u16,
    document_frequency: u32,
    field_length: u32,
    stats: Bm25FieldStats,
    config: Bm25Config,
) -> f32 {
    if term_frequency == 0
        || document_frequency == 0
        || stats.document_count == 0
        || stats.average_field_length <= 0.0
    {
        return 0.0;
    }

    let document_count = stats.document_count as f32;
    let document_frequency = document_frequency.min(stats.document_count) as f32;
    let idf = (1.0 + (document_count - document_frequency + 0.5) / (document_frequency + 0.5)).ln();
    let tf = term_frequency as f32;
    let field_length = field_length as f32;
    let denominator =
        tf + config.k1 * (1.0 - config.b + config.b * (field_length / stats.average_field_length));
    idf * ((tf * (config.k1 + 1.0)) / denominator)
}

pub fn tokenized_field_lengths(
    documents: &[FullTextDocument<'_>],
    config: &TokenizerConfig,
) -> Vec<u32> {
    documents
        .iter()
        .map(|document| {
            tokenize_text(document.text, config)
                .len()
                .min(u32::MAX as usize) as u32
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_text_body_header_round_trip() {
        let header = FullTextBodyHeader {
            dictionary_block_count: 2,
            postings_block_count: 3,
            document_table_offset: 4096,
        };
        assert_eq!(
            FullTextBodyHeader::decode(&header.encode()).unwrap(),
            header
        );
    }

    #[test]
    fn term_entry_round_trip_checks_term_hash() {
        let entry = TermEntry::new(b"tenant".to_vec(), 4, 128, 64);
        let encoded = entry.encode();
        let (decoded, used) = TermEntry::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, entry);

        let mut corrupted = encoded;
        corrupted[34] ^= 1;
        assert_eq!(
            TermEntry::decode(&corrupted).unwrap_err(),
            FormatError::HashMismatch {
                context: "full text term"
            }
        );
    }

    #[test]
    fn posting_round_trip_preserves_position_deltas() {
        let posting = Posting {
            document_id: 7,
            field_id: 2,
            term_frequency: 3,
            object_version_id: [9; 16],
            authz_label_hash: [5; 32],
            delta_positions: vec![1, 3, 8],
        };
        let encoded = posting.encode();
        let (decoded, used) = Posting::decode(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        assert_eq!(decoded, posting);
    }

    #[test]
    fn tokenizer_nfkc_normalizes_lowercases_and_keeps_ranges() {
        let tokens = tokenize_text("Ａcme Café, 世界!", &TokenizerConfig::default());
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.term.as_str())
                .collect::<Vec<_>>(),
            vec!["acme", "café", "世", "界"]
        );
        assert_eq!(tokens[0].position, 0);
        assert_eq!(tokens[1].position, 1);
        assert_eq!(tokens[2].position, 2);
        assert_eq!(tokens[3].position, 3);
        assert_eq!(
            &"Ａcme Café, 世界!"[tokens[0].original_byte_start..tokens[0].original_byte_end],
            "Ａcme"
        );
        assert_eq!(
            &"Ａcme Café, 世界!"[tokens[2].original_byte_start..tokens[2].original_byte_end],
            "世"
        );
    }

    #[test]
    fn tokenizer_uses_unicode_case_folding() {
        let tokens = tokenize_text("Straße Teſt spiﬃest", &TokenizerConfig::default());
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.term.as_str())
                .collect::<Vec<_>>(),
            vec!["strasse", "test", "spiffiest"]
        );
    }

    #[test]
    fn tokenizer_normalizes_grapheme_clusters_before_tokenizing() {
        let input = "Cafe\u{301} resume\u{301}";
        let tokens = tokenize_text(input, &TokenizerConfig::default());
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.term.as_str())
                .collect::<Vec<_>>(),
            vec!["café", "resumé"]
        );
        assert_eq!(
            &input[tokens[0].original_byte_start..tokens[0].original_byte_end],
            "Cafe\u{301}"
        );
    }

    #[test]
    fn tokenizer_uses_unicode_word_boundaries() {
        let tokens = tokenize_text(
            "can't_stop 123.45 email@example.com",
            &TokenizerConfig::default(),
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.term.as_str())
                .collect::<Vec<_>>(),
            vec!["can't_stop", "123.45", "email", "example.com"]
        );
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.position)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
    }

    #[test]
    fn tokenizer_rejects_tokens_above_configured_length() {
        let config = TokenizerConfig {
            max_token_chars: 3,
            ..TokenizerConfig::default()
        };
        let tokens = tokenize_text("one three two", &config);
        assert_eq!(
            tokens
                .iter()
                .map(|token| token.term.as_str())
                .collect::<Vec<_>>(),
            vec!["one", "two"]
        );
        assert_eq!(tokens[1].position, 2);
    }

    #[test]
    fn full_text_index_definition_parses_defaults() {
        let definition = FullTextIndexDefinition::from_json(&serde_json::json!({})).unwrap();

        assert!(definition.positions_enabled);
        assert_eq!(definition.language, "simple");
        assert_eq!(definition.tokenizer, TokenizerConfig::default());
        assert!(!definition.stop_words_enabled);
        assert_eq!(definition.stemming, None);
        assert!(!definition.require_index_success);
    }

    #[test]
    fn full_text_index_definition_parses_explicit_policy() {
        let definition = FullTextIndexDefinition::from_json(&serde_json::json!({
            "positions": false,
            "language": "en",
            "max_token_chars": 64,
            "lowercase": false,
            "normalize_nfkc": false,
            "record_original_ranges": false,
            "stop_words_enabled": true,
            "stemming": "porter",
            "require_index_success": true
        }))
        .unwrap();

        assert!(!definition.positions_enabled);
        assert_eq!(definition.language, "en");
        assert_eq!(definition.tokenizer.max_token_chars, 64);
        assert!(!definition.tokenizer.lowercase);
        assert!(!definition.tokenizer.normalize_nfkc);
        assert!(!definition.tokenizer.record_original_ranges);
        assert!(definition.stop_words_enabled);
        assert_eq!(definition.stemming.as_deref(), Some("porter"));
        assert!(definition.require_index_success);
    }

    #[test]
    fn full_text_index_definition_rejects_invalid_shapes() {
        for (field, value) in [
            ("root", serde_json::json!("not an object")),
            ("positions", serde_json::json!({"positions": "yes"})),
            ("language", serde_json::json!({"language": ""})),
            ("max_token_chars", serde_json::json!({"max_token_chars": 0})),
            (
                "max_token_chars",
                serde_json::json!({"max_token_chars": 129}),
            ),
            ("lowercase", serde_json::json!({"lowercase": "true"})),
            (
                "normalize_nfkc",
                serde_json::json!({"normalize_nfkc": "true"}),
            ),
            (
                "record_original_ranges",
                serde_json::json!({"record_original_ranges": "true"}),
            ),
            (
                "stop_words_enabled",
                serde_json::json!({"stop_words_enabled": "false"}),
            ),
            ("stemming", serde_json::json!({"stemming": ""})),
            (
                "require_index_success",
                serde_json::json!({"require_index_success": "false"}),
            ),
        ] {
            assert_eq!(
                FullTextIndexDefinition::from_json(&value).unwrap_err(),
                FormatError::InvalidFullTextIndexDefinition { field }
            );
        }
    }

    #[test]
    fn postings_builder_groups_terms_by_document_and_delta_encodes_positions() {
        let config = TokenizerConfig::default();
        let built = build_full_text_postings(
            &[
                FullTextDocument {
                    document_id: 10,
                    field_id: 1,
                    object_version_id: [1; 16],
                    authz_label_hash: [2; 32],
                    text: "Alpha beta alpha",
                },
                FullTextDocument {
                    document_id: 11,
                    field_id: 1,
                    object_version_id: [3; 16],
                    authz_label_hash: [4; 32],
                    text: "beta gamma",
                },
            ],
            &config,
        );

        assert_eq!(
            built
                .terms
                .iter()
                .map(|term| std::str::from_utf8(&term.term_utf8).unwrap())
                .collect::<Vec<_>>(),
            vec!["alpha", "beta", "gamma"]
        );
        assert_eq!(built.terms[0].doc_frequency, 1);
        assert_eq!(built.terms[1].doc_frequency, 2);
        assert_eq!(built.postings.len(), 4);

        let alpha = &built.postings[0];
        assert_eq!(alpha.document_id, 10);
        assert_eq!(alpha.term_frequency, 2);
        assert_eq!(alpha.delta_positions, vec![0, 2]);

        let beta_first = &built.postings[1];
        let beta_second = &built.postings[2];
        assert_eq!(beta_first.document_id, 10);
        assert_eq!(beta_second.document_id, 11);

        let alpha_postings_len = built.terms[0].postings_len as usize;
        let (decoded, used) = Posting::decode(&built.postings_bytes[..alpha_postings_len]).unwrap();
        assert_eq!(used, alpha_postings_len);
        assert_eq!(decoded, *alpha);
    }

    #[test]
    fn decode_postings_decodes_concatenated_posting_block() {
        let postings = vec![
            Posting {
                document_id: 1,
                field_id: 1,
                term_frequency: 1,
                object_version_id: [1; 16],
                authz_label_hash: [1; 32],
                delta_positions: vec![0],
            },
            Posting {
                document_id: 2,
                field_id: 1,
                term_frequency: 2,
                object_version_id: [2; 16],
                authz_label_hash: [2; 32],
                delta_positions: vec![1, 3],
            },
        ];
        let mut encoded = Vec::new();
        for posting in &postings {
            encoded.extend_from_slice(&posting.encode());
        }
        assert_eq!(decode_postings(&encoded).unwrap(), postings);
    }

    #[test]
    fn phrase_query_matches_only_adjacent_terms_in_same_document_field() {
        let config = TokenizerConfig::default();
        let built = build_full_text_postings(
            &[
                FullTextDocument {
                    document_id: 1,
                    field_id: 1,
                    object_version_id: [1; 16],
                    authz_label_hash: [1; 32],
                    text: "the quick brown fox",
                },
                FullTextDocument {
                    document_id: 2,
                    field_id: 1,
                    object_version_id: [2; 16],
                    authz_label_hash: [2; 32],
                    text: "quick blue brown",
                },
                FullTextDocument {
                    document_id: 3,
                    field_id: 2,
                    object_version_id: [3; 16],
                    authz_label_hash: [3; 32],
                    text: "quick brown",
                },
            ],
            &config,
        );
        let quick = postings_for_term(&built, "quick");
        let brown = postings_for_term(&built, "brown");

        let matches = evaluate_phrase_query(&[&quick, &brown], true).unwrap();
        assert_eq!(
            matches
                .iter()
                .map(|matched| (matched.document_id, matched.field_id))
                .collect::<Vec<_>>(),
            vec![(1, 1), (3, 2)]
        );
    }

    #[test]
    fn phrase_query_rejects_when_positions_are_disabled() {
        let postings = vec![Posting {
            document_id: 1,
            field_id: 1,
            term_frequency: 1,
            object_version_id: [1; 16],
            authz_label_hash: [1; 32],
            delta_positions: Vec::new(),
        }];

        assert_eq!(
            evaluate_phrase_query(&[&postings], false),
            Err(FullTextQueryError::PositionsDisabled)
        );
        assert_eq!(
            evaluate_phrase_query(&[&postings], true),
            Err(FullTextQueryError::PositionsDisabled)
        );
    }

    #[test]
    fn bm25_field_stats_are_derived_from_tokenized_lengths() {
        let config = TokenizerConfig::default();
        let documents = [
            FullTextDocument {
                document_id: 1,
                field_id: 1,
                object_version_id: [1; 16],
                authz_label_hash: [1; 32],
                text: "short field",
            },
            FullTextDocument {
                document_id: 2,
                field_id: 1,
                object_version_id: [2; 16],
                authz_label_hash: [2; 32],
                text: "longer field with more tokens",
            },
        ];
        let lengths = tokenized_field_lengths(&documents, &config);
        assert_eq!(lengths, vec![2, 5]);
        let stats = Bm25FieldStats::from_field_lengths(&lengths);
        assert_eq!(stats.document_count, 2);
        assert_eq!(stats.average_field_length, 3.5);
    }

    #[test]
    fn bm25_score_increases_with_frequency_and_rarity() {
        let stats = Bm25FieldStats {
            document_count: 100,
            average_field_length: 10.0,
        };
        let common_once = bm25_score(1, 80, 10, stats, Bm25Config::default());
        let common_twice = bm25_score(2, 80, 10, stats, Bm25Config::default());
        let rare_once = bm25_score(1, 5, 10, stats, Bm25Config::default());

        assert!(common_twice > common_once);
        assert!(rare_once > common_once);
        assert_eq!(bm25_score(0, 5, 10, stats, Bm25Config::default()), 0.0);
    }

    fn postings_for_term(built: &BuiltFullTextPostings, term: &str) -> Vec<Posting> {
        let entry = built
            .terms
            .iter()
            .find(|entry| entry.term_utf8 == term.as_bytes())
            .expect("term entry");
        let start = entry.postings_offset as usize;
        let end = start + entry.postings_len as usize;
        decode_postings(&built.postings_bytes[start..end]).unwrap()
    }
}
