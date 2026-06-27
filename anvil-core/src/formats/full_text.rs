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
}
