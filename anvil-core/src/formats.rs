pub mod authz;
pub mod full_text;
pub mod git;
pub mod hybrid;
pub mod personaldb;
pub mod segment;
pub mod vector;
pub mod watch;

use std::convert::TryInto;

pub const FORMAT_MAJOR_VERSION: u16 = 1;
pub const COMMON_HEADER_LEN: usize = 56;
pub const COMMON_FOOTER_LEN: usize = 152;
pub const FOOTER_MAGIC: &[u8; 8] = b"ANVFOOT1";

pub type Hash32 = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FileFamily {
    MetadataJournal = 1,
    MetadataSegment = 2,
    DirectorySegment = 3,
    FullTextSegment = 4,
    VectorSegment = 5,
    AuthzTupleSegment = 6,
    WatchSegment = 7,
    PersonalDbLogSegment = 8,
    PersonalDbRowIndex = 9,
    GitSourceIndex = 10,
}

impl FileFamily {
    pub fn from_u16(value: u16) -> Result<Self, FormatError> {
        match value {
            1 => Ok(Self::MetadataJournal),
            2 => Ok(Self::MetadataSegment),
            3 => Ok(Self::DirectorySegment),
            4 => Ok(Self::FullTextSegment),
            5 => Ok(Self::VectorSegment),
            6 => Ok(Self::AuthzTupleSegment),
            7 => Ok(Self::WatchSegment),
            8 => Ok(Self::PersonalDbLogSegment),
            9 => Ok(Self::PersonalDbRowIndex),
            10 => Ok(Self::GitSourceIndex),
            other => Err(FormatError::UnsupportedFamily(other)),
        }
    }

    pub fn expected_magic(self) -> &'static [u8; 8] {
        match self {
            Self::MetadataJournal => b"ANVJRN1\0",
            Self::MetadataSegment => b"ANVSEG1\0",
            Self::DirectorySegment => b"ANVDIR1\0",
            Self::FullTextSegment => b"ANVFTS1\0",
            Self::VectorSegment => b"ANVVEC1\0",
            Self::AuthzTupleSegment => b"ANVAUTH1",
            Self::WatchSegment => b"ANVWAT1\0",
            Self::PersonalDbLogSegment => b"ANVPDB1\0",
            Self::PersonalDbRowIndex => b"ANVROW1\0",
            Self::GitSourceIndex => b"ANVGIT1\0",
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FormatError {
    #[error("buffer is too short for {context}: need at least {needed} bytes, got {actual}")]
    TooShort {
        context: &'static str,
        needed: usize,
        actual: usize,
    },
    #[error("invalid magic for {context}")]
    InvalidMagic { context: &'static str },
    #[error("unsupported major version {0}")]
    UnsupportedMajorVersion(u16),
    #[error("unsupported file family {0}")]
    UnsupportedFamily(u16),
    #[error("unsupported block codec {0}")]
    UnsupportedCodec(u8),
    #[error("unsupported operation {0}")]
    UnsupportedOperation(u8),
    #[error("unsupported vector metric {0}")]
    UnsupportedVectorMetric(u8),
    #[error("unsupported vector modality {0}")]
    UnsupportedVectorModality(u8),
    #[error("invalid vector index definition field {field}")]
    InvalidVectorIndexDefinition { field: &'static str },
    #[error("invalid full text index definition field {field}")]
    InvalidFullTextIndexDefinition { field: &'static str },
    #[error("file family does not match magic")]
    FamilyMagicMismatch,
    #[error("declared length is invalid for {context}")]
    InvalidDeclaredLength { context: &'static str },
    #[error("{context} has invalid fixed length: expected {expected} bytes, got {actual}")]
    InvalidFixedLength {
        context: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("hash mismatch for {context}")]
    HashMismatch { context: &'static str },
    #[error("journal sequence did not increase")]
    NonIncreasingJournalSequence,
    #[error("journal previous hash does not match prior frame")]
    JournalPreviousHashMismatch,
    #[error("log index is not contiguous")]
    NonContiguousLogIndex,
    #[error("log previous hash does not match prior entry")]
    LogPreviousHashMismatch,
    #[error("records are not sorted by key")]
    RecordsNotSorted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryEnvelopeHeader {
    pub magic: [u8; 8],
    pub major: u16,
    pub minor: u16,
    pub family: FileFamily,
    pub flags: u16,
    pub header_json: Vec<u8>,
}

impl BinaryEnvelopeHeader {
    pub fn new(family: FileFamily, minor: u16, flags: u16, header_json: Vec<u8>) -> Self {
        Self {
            magic: *family.expected_magic(),
            major: FORMAT_MAJOR_VERSION,
            minor,
            family,
            flags,
            header_json,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COMMON_HEADER_LEN + self.header_json.len());
        out.extend_from_slice(&self.magic);
        out.extend_from_slice(&self.major.to_le_bytes());
        out.extend_from_slice(&self.minor.to_le_bytes());
        out.extend_from_slice(&(self.family as u16).to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&(self.header_json.len() as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
        out.extend_from_slice(hash32(&self.header_json).as_ref());
        out.extend_from_slice(&self.header_json);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < COMMON_HEADER_LEN {
            return Err(FormatError::TooShort {
                context: "binary envelope header",
                needed: COMMON_HEADER_LEN,
                actual: input.len(),
            });
        }
        let magic: [u8; 8] = input[0..8].try_into().unwrap();
        let major = u16::from_le_bytes(input[8..10].try_into().unwrap());
        if major != FORMAT_MAJOR_VERSION {
            return Err(FormatError::UnsupportedMajorVersion(major));
        }
        let minor = u16::from_le_bytes(input[10..12].try_into().unwrap());
        let family = FileFamily::from_u16(u16::from_le_bytes(input[12..14].try_into().unwrap()))?;
        if &magic != family.expected_magic() {
            return Err(FormatError::FamilyMagicMismatch);
        }
        let flags = u16::from_le_bytes(input[14..16].try_into().unwrap());
        let header_len = u32::from_le_bytes(input[16..20].try_into().unwrap()) as usize;
        let header_end = COMMON_HEADER_LEN.checked_add(header_len).ok_or(
            FormatError::InvalidDeclaredLength {
                context: "binary envelope header",
            },
        )?;
        if input.len() < header_end {
            return Err(FormatError::TooShort {
                context: "binary envelope header json",
                needed: header_end,
                actual: input.len(),
            });
        }
        let expected_hash: Hash32 = input[24..56].try_into().unwrap();
        let header_json = input[COMMON_HEADER_LEN..header_end].to_vec();
        if hash32(&header_json) != expected_hash {
            return Err(FormatError::HashMismatch {
                context: "binary envelope header json",
            });
        }
        Ok(Self {
            magic,
            major,
            minor,
            family,
            flags,
            header_json,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryFileFooter {
    pub body_len: u64,
    pub record_count: u64,
    pub first_record_hash: Hash32,
    pub last_record_hash: Hash32,
    pub body_hash: Hash32,
    pub file_hash: Hash32,
}

impl BinaryFileFooter {
    pub fn new(
        encoded_header: &[u8],
        body: &[u8],
        record_count: u64,
        first_record_hash: Hash32,
        last_record_hash: Hash32,
    ) -> Self {
        let body_hash = hash32(body);
        let mut footer_without_file_hash = Vec::with_capacity(COMMON_FOOTER_LEN - 32);
        footer_without_file_hash.extend_from_slice(FOOTER_MAGIC);
        footer_without_file_hash.extend_from_slice(&(body.len() as u64).to_le_bytes());
        footer_without_file_hash.extend_from_slice(&record_count.to_le_bytes());
        footer_without_file_hash.extend_from_slice(&first_record_hash);
        footer_without_file_hash.extend_from_slice(&last_record_hash);
        footer_without_file_hash.extend_from_slice(&body_hash);

        let mut file_hash_input =
            Vec::with_capacity(encoded_header.len() + body.len() + footer_without_file_hash.len());
        file_hash_input.extend_from_slice(encoded_header);
        file_hash_input.extend_from_slice(body);
        file_hash_input.extend_from_slice(&footer_without_file_hash);

        Self {
            body_len: body.len() as u64,
            record_count,
            first_record_hash,
            last_record_hash,
            body_hash,
            file_hash: hash32(&file_hash_input),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(COMMON_FOOTER_LEN);
        out.extend_from_slice(FOOTER_MAGIC);
        out.extend_from_slice(&self.body_len.to_le_bytes());
        out.extend_from_slice(&self.record_count.to_le_bytes());
        out.extend_from_slice(&self.first_record_hash);
        out.extend_from_slice(&self.last_record_hash);
        out.extend_from_slice(&self.body_hash);
        out.extend_from_slice(&self.file_hash);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < COMMON_FOOTER_LEN {
            return Err(FormatError::TooShort {
                context: "binary file footer",
                needed: COMMON_FOOTER_LEN,
                actual: input.len(),
            });
        }
        if &input[0..8] != FOOTER_MAGIC {
            return Err(FormatError::InvalidMagic {
                context: "binary file footer",
            });
        }
        Ok(Self {
            body_len: u64::from_le_bytes(input[8..16].try_into().unwrap()),
            record_count: u64::from_le_bytes(input[16..24].try_into().unwrap()),
            first_record_hash: input[24..56].try_into().unwrap(),
            last_record_hash: input[56..88].try_into().unwrap(),
            body_hash: input[88..120].try_into().unwrap(),
            file_hash: input[120..152].try_into().unwrap(),
        })
    }

    pub fn verify(&self, encoded_header: &[u8], body: &[u8]) -> Result<(), FormatError> {
        if self.body_len != body.len() as u64 {
            return Err(FormatError::InvalidDeclaredLength {
                context: "binary file footer body_len",
            });
        }
        if self.body_hash != hash32(body) {
            return Err(FormatError::HashMismatch {
                context: "binary file footer body",
            });
        }
        let expected = Self::new(
            encoded_header,
            body,
            self.record_count,
            self.first_record_hash,
            self.last_record_hash,
        );
        if self.file_hash != expected.file_hash {
            return Err(FormatError::HashMismatch {
                context: "binary file footer file",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum JournalRecordKind {
    ObjectVersion = 1,
    DirectoryEntry = 2,
    DeleteMarker = 3,
    IndexDefinition = 4,
    AuthzTuple = 5,
    WatchEvent = 6,
    PersonalDbControl = 7,
    TaskCheckpoint = 8,
    BucketMetadata = 9,
    ControlPlane = 10,
    TaskQueue = 11,
    ModelMetadata = 12,
    IndexDiagnostic = 13,
    MultipartMetadata = 14,
    AppendMetadata = 15,
    ManifestCas = 16,
    HfMetadata = 17,
    AuthzTupleBatch = 18,
}

impl JournalRecordKind {
    fn from_u16(value: u16) -> Result<Self, FormatError> {
        match value {
            1 => Ok(Self::ObjectVersion),
            2 => Ok(Self::DirectoryEntry),
            3 => Ok(Self::DeleteMarker),
            4 => Ok(Self::IndexDefinition),
            5 => Ok(Self::AuthzTuple),
            6 => Ok(Self::WatchEvent),
            7 => Ok(Self::PersonalDbControl),
            8 => Ok(Self::TaskCheckpoint),
            9 => Ok(Self::BucketMetadata),
            10 => Ok(Self::ControlPlane),
            11 => Ok(Self::TaskQueue),
            12 => Ok(Self::ModelMetadata),
            13 => Ok(Self::IndexDiagnostic),
            14 => Ok(Self::MultipartMetadata),
            15 => Ok(Self::AppendMetadata),
            16 => Ok(Self::ManifestCas),
            17 => Ok(Self::HfMetadata),
            18 => Ok(Self::AuthzTupleBatch),
            other => Err(FormatError::UnsupportedFamily(other)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalFrame {
    pub record_version: u16,
    pub record_kind: JournalRecordKind,
    pub flags: u32,
    pub partition_sequence: u64,
    pub fence_token: u64,
    pub mutation_id: [u8; 16],
    pub key_hash: Hash32,
    pub previous_record_hash: Hash32,
    pub body: Vec<u8>,
    pub record_hash: Hash32,
}

impl JournalFrame {
    pub fn new(
        record_kind: JournalRecordKind,
        partition_sequence: u64,
        fence_token: u64,
        mutation_id: [u8; 16],
        key_hash: Hash32,
        previous_record_hash: Hash32,
        body: Vec<u8>,
    ) -> Self {
        let mut frame = Self {
            record_version: 1,
            record_kind,
            flags: 0,
            partition_sequence,
            fence_token,
            mutation_id,
            key_hash,
            previous_record_hash,
            body,
            record_hash: [0; 32],
        };
        frame.record_hash = hash32(&frame.bytes_without_len_and_hash());
        frame
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut body = self.bytes_without_len_and_hash();
        body.extend_from_slice(&self.record_hash);
        let frame_len = body.len() as u32;
        let mut out = Vec::with_capacity(4 + body.len());
        out.extend_from_slice(&frame_len.to_le_bytes());
        out.extend_from_slice(&body);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < 4 {
            return Err(FormatError::TooShort {
                context: "journal frame length",
                needed: 4,
                actual: input.len(),
            });
        }
        let frame_len = u32::from_le_bytes(input[0..4].try_into().unwrap()) as usize;
        if frame_len < 172 {
            return Err(FormatError::InvalidDeclaredLength {
                context: "journal frame",
            });
        }
        let frame_end =
            4usize
                .checked_add(frame_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "journal frame",
                })?;
        if input.len() < frame_end {
            return Err(FormatError::TooShort {
                context: "journal frame",
                needed: frame_end,
                actual: input.len(),
            });
        }
        let frame = &input[4..frame_end];
        let record_hash: Hash32 = frame[frame.len() - 32..].try_into().unwrap();
        let hash_input = &frame[..frame.len() - 32];
        if hash32(hash_input) != record_hash {
            return Err(FormatError::HashMismatch {
                context: "journal frame record",
            });
        }

        let body_len_offset = 2 + 2 + 4 + 8 + 8 + 16 + 32 + 32 + 32;
        let body_len = u32::from_le_bytes(
            hash_input[body_len_offset..body_len_offset + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let body_start = body_len_offset + 4;
        let body_end =
            body_start
                .checked_add(body_len)
                .ok_or(FormatError::InvalidDeclaredLength {
                    context: "journal frame body",
                })?;
        if body_end != hash_input.len() {
            return Err(FormatError::InvalidDeclaredLength {
                context: "journal frame body",
            });
        }
        let body = hash_input[body_start..body_end].to_vec();
        let expected_body_hash: Hash32 = hash_input[104..136].try_into().unwrap();
        if hash32(&body) != expected_body_hash {
            return Err(FormatError::HashMismatch {
                context: "journal frame body",
            });
        }
        Ok(Self {
            record_version: u16::from_le_bytes(hash_input[0..2].try_into().unwrap()),
            record_kind: JournalRecordKind::from_u16(u16::from_le_bytes(
                hash_input[2..4].try_into().unwrap(),
            ))?,
            flags: u32::from_le_bytes(hash_input[4..8].try_into().unwrap()),
            partition_sequence: u64::from_le_bytes(hash_input[8..16].try_into().unwrap()),
            fence_token: u64::from_le_bytes(hash_input[16..24].try_into().unwrap()),
            mutation_id: hash_input[24..40].try_into().unwrap(),
            key_hash: hash_input[40..72].try_into().unwrap(),
            previous_record_hash: hash_input[72..104].try_into().unwrap(),
            body,
            record_hash,
        })
    }

    fn bytes_without_len_and_hash(&self) -> Vec<u8> {
        let body_hash = hash32(&self.body);
        let mut out = Vec::with_capacity(140 + self.body.len());
        out.extend_from_slice(&self.record_version.to_le_bytes());
        out.extend_from_slice(&(self.record_kind as u16).to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.partition_sequence.to_le_bytes());
        out.extend_from_slice(&self.fence_token.to_le_bytes());
        out.extend_from_slice(&self.mutation_id);
        out.extend_from_slice(&self.key_hash);
        out.extend_from_slice(&self.previous_record_hash);
        out.extend_from_slice(&body_hash);
        out.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.body);
        out
    }
}

pub fn validate_journal_chain(frames: &[JournalFrame]) -> Result<(), FormatError> {
    let mut last_sequence = None;
    let mut last_hash = [0u8; 32];
    for frame in frames {
        if let Some(previous_sequence) = last_sequence {
            if frame.partition_sequence <= previous_sequence {
                return Err(FormatError::NonIncreasingJournalSequence);
            }
            if frame.previous_record_hash != last_hash {
                return Err(FormatError::JournalPreviousHashMismatch);
            }
        }
        last_sequence = Some(frame.partition_sequence);
        last_hash = frame.record_hash;
    }
    Ok(())
}

pub fn hash32(bytes: &[u8]) -> Hash32 {
    *blake3::hash(bytes).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_hash(seed: u8) -> Hash32 {
        [seed; 32]
    }

    #[test]
    fn envelope_round_trip_and_hash_validation() {
        let header = BinaryEnvelopeHeader::new(
            FileFamily::MetadataJournal,
            0,
            0,
            br#"{"tenant_id":"tenant","codec":"none"}"#.to_vec(),
        );
        let encoded = header.encode();
        let decoded = BinaryEnvelopeHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);

        let mut corrupted = encoded;
        let last = corrupted.len() - 1;
        corrupted[last] ^= 0x01;
        assert_eq!(
            BinaryEnvelopeHeader::decode(&corrupted).unwrap_err(),
            FormatError::HashMismatch {
                context: "binary envelope header json"
            }
        );
    }

    #[test]
    fn footer_verifies_complete_file_hash() {
        let header = BinaryEnvelopeHeader::new(FileFamily::MetadataSegment, 0, 0, b"{}".to_vec());
        let encoded_header = header.encode();
        let body = b"segment body";
        let footer =
            BinaryFileFooter::new(&encoded_header, body, 2, sample_hash(1), sample_hash(2));
        let encoded_footer = footer.encode();
        let decoded = BinaryFileFooter::decode(&encoded_footer).unwrap();
        decoded.verify(&encoded_header, body).unwrap();
        assert_eq!(decoded, footer);

        let bad_body = b"segment bodz";
        assert_eq!(
            decoded.verify(&encoded_header, bad_body).unwrap_err(),
            FormatError::HashMismatch {
                context: "binary file footer body"
            }
        );
    }

    #[test]
    fn journal_frame_round_trip_and_chain_validation() {
        let first = JournalFrame::new(
            JournalRecordKind::ObjectVersion,
            1,
            7,
            [9; 16],
            sample_hash(3),
            [0; 32],
            br#"{"key":"a"}"#.to_vec(),
        );
        let second = JournalFrame::new(
            JournalRecordKind::DirectoryEntry,
            2,
            7,
            [10; 16],
            sample_hash(4),
            first.record_hash,
            br#"{"key":"b"}"#.to_vec(),
        );

        let decoded = JournalFrame::decode(&first.encode()).unwrap();
        assert_eq!(decoded, first);
        validate_journal_chain(&[first.clone(), second.clone()]).unwrap();

        let invalid = JournalFrame {
            previous_record_hash: sample_hash(99),
            ..second
        };
        assert_eq!(
            validate_journal_chain(&[first, invalid]).unwrap_err(),
            FormatError::JournalPreviousHashMismatch
        );
    }
}
