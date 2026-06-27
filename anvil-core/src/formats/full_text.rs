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
    let (normalized, spans) = normalize_for_full_text(input, config);
    let mut tokens = Vec::new();
    let mut current_start = None;
    let mut current_chars = 0usize;
    let mut position = 0u32;

    for (offset, ch) in normalized.char_indices() {
        if is_index_token_char(ch) {
            if current_start.is_none() {
                current_start = Some(offset);
                current_chars = 0;
            }
            current_chars += 1;
            continue;
        }

        if let Some(start) = current_start.take() {
            push_token(
                &normalized,
                &spans,
                start,
                offset,
                current_chars,
                &mut position,
                &mut tokens,
                config,
            );
        }
    }

    if let Some(start) = current_start {
        push_token(
            &normalized,
            &spans,
            start,
            normalized.len(),
            current_chars,
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

    let mut normalized = String::with_capacity(input.len());
    let mut spans = Vec::new();

    for (original_start, ch) in input.char_indices() {
        let original_end = original_start + ch.len_utf8();
        let source = if config.normalize_nfkc {
            ch.to_string().nfkc().collect::<String>()
        } else {
            ch.to_string()
        };
        for normalized_ch in source.chars() {
            let folded = if config.lowercase {
                normalized_ch.to_lowercase().collect::<String>()
            } else {
                normalized_ch.to_string()
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
    }

    (normalized, spans)
}

fn is_index_token_char(ch: char) -> bool {
    ch.is_alphanumeric() || ch == '_'
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
            vec!["acme", "café", "世界"]
        );
        assert_eq!(tokens[0].position, 0);
        assert_eq!(tokens[1].position, 1);
        assert_eq!(tokens[2].position, 2);
        assert_eq!(
            &"Ａcme Café, 世界!"[tokens[0].original_byte_start..tokens[0].original_byte_end],
            "Ａcme"
        );
        assert_eq!(
            &"Ａcme Café, 世界!"[tokens[2].original_byte_start..tokens[2].original_byte_end],
            "世界"
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
}
