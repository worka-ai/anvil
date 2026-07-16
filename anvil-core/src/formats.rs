pub mod authz;
pub mod full_text;
pub mod git;
pub mod hybrid;
pub mod personaldb;
pub mod segment;
pub mod table;
pub mod vector;
pub mod watch;
pub mod writer;

use std::collections::BTreeMap;
use std::convert::TryInto;

use prost::Message;

pub const FORMAT_MAJOR_VERSION: u16 = 1;
pub const WRITER_SEGMENT_FIXED_HEADER_LEN: usize = 36;
pub const SEGMENT_HASH_LEN: usize = 32;

pub type Hash32 = [u8; 32];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum FileFamily {
    MetadataSegment = 2,
    DirectorySegment = 3,
    FullTextSegment = 4,
    VectorSegment = 5,
    AuthzTupleSegment = 6,
    WatchSegment = 7,
    PersonalDbLogSegment = 8,
    PersonalDbRowIndex = 9,
    GitSourceIndex = 10,
    TypedFieldSegment = 11,
    RegistrySegment = 12,
    MeshControlSegment = 13,
}

impl FileFamily {
    pub fn from_u16(value: u16) -> Result<Self, FormatError> {
        match value {
            2 => Ok(Self::MetadataSegment),
            3 => Ok(Self::DirectorySegment),
            4 => Ok(Self::FullTextSegment),
            5 => Ok(Self::VectorSegment),
            6 => Ok(Self::AuthzTupleSegment),
            7 => Ok(Self::WatchSegment),
            8 => Ok(Self::PersonalDbLogSegment),
            9 => Ok(Self::PersonalDbRowIndex),
            10 => Ok(Self::GitSourceIndex),
            11 => Ok(Self::TypedFieldSegment),
            12 => Ok(Self::RegistrySegment),
            13 => Ok(Self::MeshControlSegment),
            other => Err(FormatError::UnsupportedFamily(other)),
        }
    }

    pub fn expected_magic(self) -> &'static [u8; 8] {
        match self {
            Self::MetadataSegment => b"ANOBJM1\0",
            Self::DirectorySegment => b"ANTDIR1\0",
            Self::FullTextSegment => b"ANFTSG1\0",
            Self::VectorSegment => b"ANVECG1\0",
            Self::AuthzTupleSegment => b"ANAUTH1\0",
            Self::WatchSegment => b"ANVWAT1\0",
            Self::PersonalDbLogSegment => b"ANPDB1\0\0",
            Self::PersonalDbRowIndex => b"ANVROW1\0",
            Self::GitSourceIndex => b"ANVGIT1\0",
            Self::TypedFieldSegment => b"ANTIDX1\0",
            Self::RegistrySegment => b"ANREG1\0\0",
            Self::MeshControlSegment => b"ANMESH1\0",
        }
    }

    pub fn from_magic(magic: &[u8; 8]) -> Result<Self, FormatError> {
        for family in [
            Self::MetadataSegment,
            Self::DirectorySegment,
            Self::FullTextSegment,
            Self::VectorSegment,
            Self::AuthzTupleSegment,
            Self::WatchSegment,
            Self::PersonalDbLogSegment,
            Self::PersonalDbRowIndex,
            Self::GitSourceIndex,
            Self::TypedFieldSegment,
            Self::RegistrySegment,
            Self::MeshControlSegment,
        ] {
            if family.expected_magic() == magic {
                return Ok(family);
            }
        }
        Err(FormatError::InvalidMagic {
            context: "writer segment",
        })
    }

    pub fn writer_family_name(self) -> &'static str {
        match self {
            Self::MetadataSegment => "object_blob",
            Self::DirectorySegment => "object_blob",
            Self::FullTextSegment => "full_text",
            Self::VectorSegment => "vector",
            Self::AuthzTupleSegment => "authz",
            Self::WatchSegment => "stream",
            Self::PersonalDbLogSegment => "personaldb",
            Self::PersonalDbRowIndex => "personaldb_row_index",
            Self::GitSourceIndex => "git_source",
            Self::TypedFieldSegment => "typed_field",
            Self::RegistrySegment => "registry",
            Self::MeshControlSegment => "mesh_control",
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
    #[error("invalid protobuf for {context}")]
    InvalidProtobuf { context: &'static str },
    #[error("missing writer segment header field {field}")]
    MissingHeaderField { field: &'static str },
    #[error("invalid writer segment header field {field}")]
    InvalidHeaderField { field: &'static str },
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
    #[error("range index is not sorted or has overlapping logical ranges")]
    InvalidRangeIndexOrder,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriterSegmentHeaderFieldProto {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, optional, tag = "2")]
    pub string_value: Option<String>,
    #[prost(string, repeated, tag = "3")]
    pub string_values: Vec<String>,
    #[prost(uint64, optional, tag = "4")]
    pub uint64_value: Option<u64>,
    #[prost(bytes = "vec", optional, tag = "5")]
    pub bytes_value: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WriterSegmentHeaderProto {
    #[prost(string, tag = "1")]
    pub schema: String,
    #[prost(string, tag = "2")]
    pub logical_file_id: String,
    #[prost(string, tag = "3")]
    pub writer_family: String,
    #[prost(uint64, tag = "4")]
    pub writer_generation: u64,
    #[prost(bytes = "vec", optional, tag = "5")]
    pub min_record_key: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "6")]
    pub max_record_key: Option<Vec<u8>>,
    #[prost(uint64, tag = "7")]
    pub created_at_unix_nanos: u64,
    #[prost(message, repeated, tag = "8")]
    pub fields: Vec<WriterSegmentHeaderFieldProto>,
}

#[derive(Clone, PartialEq, Message)]
struct WriterSegmentTrailerProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    record_count: u64,
    #[prost(bytes = "vec", tag = "3")]
    first_record_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    last_record_hash: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    body_hash: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryValueRef {
    pub dimension_id: u32,
    pub value_type: u8,
    pub value_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeIndexEntry {
    pub logical_start: u64,
    pub logical_end: u64,
    pub record_count: u64,
    pub boundary_values: Vec<BoundaryValueRef>,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub stats_ref: Vec<u8>,
}

impl RangeIndexEntry {
    pub fn covering_body(
        body_len: usize,
        record_count: u64,
        min_key: impl Into<Vec<u8>>,
        max_key: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            logical_start: 0,
            logical_end: body_len as u64,
            record_count,
            boundary_values: Vec::new(),
            min_key: min_key.into(),
            max_key: max_key.into(),
            stats_ref: Vec::new(),
        }
    }
}

pub fn single_body_range_index(
    body_len: usize,
    record_count: u64,
    min_key: impl Into<Vec<u8>>,
    max_key: impl Into<Vec<u8>>,
) -> Result<Vec<u8>, FormatError> {
    if body_len == 0 {
        return Ok(Vec::new());
    }
    encode_range_index(&[RangeIndexEntry::covering_body(
        body_len,
        record_count,
        min_key,
        max_key,
    )])
}

pub fn encode_range_index(entries: &[RangeIndexEntry]) -> Result<Vec<u8>, FormatError> {
    let mut previous_end = None;
    let mut out = Vec::new();
    for entry in entries {
        if entry.logical_start > entry.logical_end {
            return Err(FormatError::InvalidRangeIndexOrder);
        }
        if let Some(previous_end) = previous_end {
            if entry.logical_start < previous_end {
                return Err(FormatError::InvalidRangeIndexOrder);
            }
        }
        previous_end = Some(entry.logical_end);
        out.extend_from_slice(&entry.logical_start.to_le_bytes());
        out.extend_from_slice(&entry.logical_end.to_le_bytes());
        out.extend_from_slice(&entry.record_count.to_le_bytes());
        write_u16_len(
            entry.boundary_values.len(),
            &mut out,
            "range index boundary values",
        )?;
        for value in &entry.boundary_values {
            out.extend_from_slice(&value.dimension_id.to_le_bytes());
            out.push(value.value_type);
            write_uleb128(value.value_bytes.len() as u64, &mut out);
            out.extend_from_slice(&value.value_bytes);
        }
        write_u16_len(entry.min_key.len(), &mut out, "range index min key")?;
        out.extend_from_slice(&entry.min_key);
        write_u16_len(entry.max_key.len(), &mut out, "range index max key")?;
        out.extend_from_slice(&entry.max_key);
        write_u16_len(entry.stats_ref.len(), &mut out, "range index stats ref")?;
        out.extend_from_slice(&entry.stats_ref);
    }
    Ok(out)
}

pub fn decode_range_index(mut input: &[u8]) -> Result<Vec<RangeIndexEntry>, FormatError> {
    let mut entries = Vec::new();
    let mut previous_end = None;
    while !input.is_empty() {
        if input.len() < 26 {
            return Err(FormatError::TooShort {
                context: "range index entry",
                needed: 26,
                actual: input.len(),
            });
        }
        let logical_start = read_u64_le(&mut input, "range index logical_start")?;
        let logical_end = read_u64_le(&mut input, "range index logical_end")?;
        let record_count = read_u64_le(&mut input, "range index record_count")?;
        if logical_start > logical_end {
            return Err(FormatError::InvalidRangeIndexOrder);
        }
        if let Some(previous_end) = previous_end {
            if logical_start < previous_end {
                return Err(FormatError::InvalidRangeIndexOrder);
            }
        }
        previous_end = Some(logical_end);
        let boundary_value_count = read_u16_le(&mut input, "range index boundary_value_count")?;
        let mut boundary_values = Vec::with_capacity(boundary_value_count as usize);
        for _ in 0..boundary_value_count {
            let dimension_id = read_u32_le(&mut input, "range index boundary dimension_id")?;
            let value_type = read_u8(&mut input, "range index boundary value_type")?;
            let value_len = read_uleb128(&mut input, "range index boundary value_len")?;
            let value_bytes = take_len(&mut input, value_len, "range index boundary value")?;
            boundary_values.push(BoundaryValueRef {
                dimension_id,
                value_type,
                value_bytes,
            });
        }
        let min_key_len = read_u16_le(&mut input, "range index min_key_len")? as usize;
        let min_key = take_len(&mut input, min_key_len, "range index min_key")?;
        let max_key_len = read_u16_le(&mut input, "range index max_key_len")? as usize;
        let max_key = take_len(&mut input, max_key_len, "range index max_key")?;
        let stats_ref_len = read_u16_le(&mut input, "range index stats_ref_len")? as usize;
        let stats_ref = take_len(&mut input, stats_ref_len, "range index stats_ref")?;
        entries.push(RangeIndexEntry {
            logical_start,
            logical_end,
            record_count,
            boundary_values,
            min_key,
            max_key,
            stats_ref,
        });
    }
    Ok(entries)
}

pub fn header_field_string(
    name: &'static str,
    value: impl Into<String>,
) -> WriterSegmentHeaderFieldProto {
    WriterSegmentHeaderFieldProto {
        name: name.to_string(),
        string_value: Some(value.into()),
        string_values: Vec::new(),
        uint64_value: None,
        bytes_value: None,
    }
}

pub fn header_field_strings(
    name: &'static str,
    values: impl IntoIterator<Item = String>,
) -> WriterSegmentHeaderFieldProto {
    WriterSegmentHeaderFieldProto {
        name: name.to_string(),
        string_value: None,
        string_values: values.into_iter().collect(),
        uint64_value: None,
        bytes_value: None,
    }
}

pub fn header_field_u64(name: &'static str, value: u64) -> WriterSegmentHeaderFieldProto {
    WriterSegmentHeaderFieldProto {
        name: name.to_string(),
        string_value: None,
        string_values: Vec::new(),
        uint64_value: Some(value),
        bytes_value: None,
    }
}

pub fn header_field_bytes(
    name: &'static str,
    value: impl Into<Vec<u8>>,
) -> WriterSegmentHeaderFieldProto {
    WriterSegmentHeaderFieldProto {
        name: name.to_string(),
        string_value: None,
        string_values: Vec::new(),
        uint64_value: None,
        bytes_value: Some(value.into()),
    }
}

pub fn encode_writer_segment_header(
    schema: &'static str,
    logical_file_id: &str,
    family: FileFamily,
    writer_generation: u64,
    min_record_key: Option<Vec<u8>>,
    max_record_key: Option<Vec<u8>>,
    created_at_unix_nanos: u64,
    mut fields: Vec<WriterSegmentHeaderFieldProto>,
) -> Vec<u8> {
    fields.sort_by(|left, right| left.name.cmp(&right.name));
    let header = WriterSegmentHeaderProto {
        schema: schema.to_string(),
        logical_file_id: logical_file_id.to_string(),
        writer_family: family.writer_family_name().to_string(),
        writer_generation,
        min_record_key,
        max_record_key,
        created_at_unix_nanos,
        fields,
    };
    encode_proto(&header)
}

pub fn decode_writer_segment_header(input: &[u8]) -> Result<WriterSegmentHeaderProto, FormatError> {
    let header =
        WriterSegmentHeaderProto::decode(input).map_err(|_| FormatError::InvalidProtobuf {
            context: "writer segment header",
        })?;
    validate_writer_segment_header(&header)?;
    if canonical_writer_segment_header_bytes(&header) != input {
        return Err(FormatError::InvalidProtobuf {
            context: "canonical writer segment header",
        });
    }
    Ok(header)
}

fn encode_proto(message: &impl Message) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message
        .encode(&mut bytes)
        .expect("encoding a protobuf message into Vec cannot fail");
    bytes
}

fn canonical_writer_segment_header_bytes(header: &WriterSegmentHeaderProto) -> Vec<u8> {
    let mut header = header.clone();
    header
        .fields
        .sort_by(|left, right| left.name.cmp(&right.name));
    encode_proto(&header)
}

fn validate_writer_segment_header(header: &WriterSegmentHeaderProto) -> Result<(), FormatError> {
    if header.schema.is_empty() {
        return Err(FormatError::InvalidHeaderField { field: "schema" });
    }
    if header.logical_file_id.is_empty() {
        return Err(FormatError::InvalidHeaderField {
            field: "logical_file_id",
        });
    }
    if header.writer_family.is_empty() {
        return Err(FormatError::InvalidHeaderField {
            field: "writer_family",
        });
    }
    let mut previous: Option<&str> = None;
    for field in &header.fields {
        if field.name.is_empty() || previous.is_some_and(|previous| previous >= field.name.as_str())
        {
            return Err(FormatError::InvalidHeaderField { field: "fields" });
        }
        previous = Some(field.name.as_str());
    }
    Ok(())
}

fn header_field_map<'a>(
    header: &'a WriterSegmentHeaderProto,
) -> BTreeMap<&'a str, &'a WriterSegmentHeaderFieldProto> {
    header
        .fields
        .iter()
        .map(|field| (field.name.as_str(), field))
        .collect()
}

pub fn required_header_string(
    header: &WriterSegmentHeaderProto,
    name: &'static str,
) -> Result<String, FormatError> {
    let fields = header_field_map(header);
    let field = fields
        .get(name)
        .ok_or(FormatError::MissingHeaderField { field: name })?;
    field
        .string_value
        .clone()
        .ok_or(FormatError::InvalidHeaderField { field: name })
}

pub fn optional_header_string(
    header: &WriterSegmentHeaderProto,
    name: &'static str,
) -> Result<Option<String>, FormatError> {
    let fields = header_field_map(header);
    Ok(fields
        .get(name)
        .and_then(|field| field.string_value.clone()))
}

pub fn required_header_strings(
    header: &WriterSegmentHeaderProto,
    name: &'static str,
) -> Result<Vec<String>, FormatError> {
    let fields = header_field_map(header);
    let field = fields
        .get(name)
        .ok_or(FormatError::MissingHeaderField { field: name })?;
    Ok(field.string_values.clone())
}

pub fn required_header_u64(
    header: &WriterSegmentHeaderProto,
    name: &'static str,
) -> Result<u64, FormatError> {
    let fields = header_field_map(header);
    let field = fields
        .get(name)
        .ok_or(FormatError::MissingHeaderField { field: name })?;
    field
        .uint64_value
        .ok_or(FormatError::InvalidHeaderField { field: name })
}

pub fn required_header_bytes(
    header: &WriterSegmentHeaderProto,
    name: &'static str,
) -> Result<Vec<u8>, FormatError> {
    let fields = header_field_map(header);
    let field = fields
        .get(name)
        .ok_or(FormatError::MissingHeaderField { field: name })?;
    field
        .bytes_value
        .clone()
        .ok_or(FormatError::InvalidHeaderField { field: name })
}

pub fn unix_nanos_from_rfc3339(value: &str) -> u64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|dt| dt.timestamp_nanos_opt())
        .and_then(|nanos| u64::try_from(nanos).ok())
        .unwrap_or(0)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterSegmentFixedHeader {
    pub magic: [u8; 8],
    pub version: u16,
    pub family: FileFamily,
    pub flags: u16,
    pub header_proto: Vec<u8>,
    pub body_len: u64,
    pub range_index_len: u64,
    pub trailer_len: u32,
}

impl WriterSegmentFixedHeader {
    pub fn new(
        family: FileFamily,
        flags: u16,
        header_proto: Vec<u8>,
        body_len: u64,
        range_index_len: u64,
        trailer_len: u32,
    ) -> Self {
        Self {
            magic: *family.expected_magic(),
            version: FORMAT_MAJOR_VERSION,
            family,
            flags,
            header_proto,
            body_len,
            range_index_len,
            trailer_len,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(WRITER_SEGMENT_FIXED_HEADER_LEN + self.header_proto.len());
        out.extend_from_slice(&self.magic);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&(self.header_proto.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.body_len.to_le_bytes());
        out.extend_from_slice(&self.range_index_len.to_le_bytes());
        out.extend_from_slice(&self.trailer_len.to_le_bytes());
        out.extend_from_slice(&self.header_proto);
        out
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < WRITER_SEGMENT_FIXED_HEADER_LEN {
            return Err(FormatError::TooShort {
                context: "binary envelope header",
                needed: WRITER_SEGMENT_FIXED_HEADER_LEN,
                actual: input.len(),
            });
        }
        let magic: [u8; 8] = input[0..8].try_into().unwrap();
        let version = u16::from_le_bytes(input[8..10].try_into().unwrap());
        if version != FORMAT_MAJOR_VERSION {
            return Err(FormatError::UnsupportedMajorVersion(version));
        }
        let flags = u16::from_le_bytes(input[10..12].try_into().unwrap());
        let header_len = u32::from_le_bytes(input[12..16].try_into().unwrap()) as usize;
        let body_len = u64::from_le_bytes(input[16..24].try_into().unwrap());
        let range_index_len = u64::from_le_bytes(input[24..32].try_into().unwrap());
        let trailer_len = u32::from_le_bytes(input[32..36].try_into().unwrap());
        let family = FileFamily::from_magic(&magic)?;
        let header_end = WRITER_SEGMENT_FIXED_HEADER_LEN
            .checked_add(header_len)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "binary envelope header",
            })?;
        if input.len() < header_end {
            return Err(FormatError::TooShort {
                context: "writer segment header proto",
                needed: header_end,
                actual: input.len(),
            });
        }
        let header_proto = input[WRITER_SEGMENT_FIXED_HEADER_LEN..header_end].to_vec();
        decode_writer_segment_header(&header_proto)?;
        Ok(Self {
            magic,
            version,
            family,
            flags,
            header_proto,
            body_len,
            range_index_len,
            trailer_len,
        })
    }

    pub fn encoded_len(&self) -> usize {
        WRITER_SEGMENT_FIXED_HEADER_LEN + self.header_proto.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterSegmentTrailer {
    pub body_len: u64,
    pub record_count: u64,
    pub first_record_hash: Hash32,
    pub last_record_hash: Hash32,
    pub body_hash: Hash32,
    pub file_hash: Hash32,
}

impl WriterSegmentTrailer {
    fn new(
        body: &[u8],
        record_count: u64,
        first_record_hash: Hash32,
        last_record_hash: Hash32,
    ) -> Self {
        let body_hash = hash32(body);
        Self {
            body_len: body.len() as u64,
            record_count,
            first_record_hash,
            last_record_hash,
            body_hash,
            file_hash: [0; 32],
        }
    }

    fn trailer_proto(&self) -> Vec<u8> {
        let trailer = WriterSegmentTrailerProto {
            schema: "anvil.writer_segment.trailer.v1".to_string(),
            record_count: self.record_count,
            first_record_hash: self.first_record_hash.to_vec(),
            last_record_hash: self.last_record_hash.to_vec(),
            body_hash: self.body_hash.to_vec(),
        };
        encode_proto(&trailer)
    }

    pub fn decode(input: &[u8]) -> Result<Self, FormatError> {
        if input.len() < SEGMENT_HASH_LEN {
            return Err(FormatError::TooShort {
                context: "writer segment trailer",
                needed: SEGMENT_HASH_LEN,
                actual: input.len(),
            });
        }
        let trailer_end = input.len() - SEGMENT_HASH_LEN;
        let proto = WriterSegmentTrailerProto::decode(&input[..trailer_end]).map_err(|_| {
            FormatError::InvalidProtobuf {
                context: "writer segment trailer",
            }
        })?;
        if proto.schema != "anvil.writer_segment.trailer.v1" {
            return Err(FormatError::InvalidProtobuf {
                context: "writer segment trailer schema",
            });
        }
        if encode_proto(&proto).as_slice() != &input[..trailer_end] {
            return Err(FormatError::InvalidProtobuf {
                context: "canonical writer segment trailer",
            });
        }
        let first_record_hash = hash_from_proto_vec(proto.first_record_hash, "first_record_hash")?;
        let last_record_hash = hash_from_proto_vec(proto.last_record_hash, "last_record_hash")?;
        let body_hash = hash_from_proto_vec(proto.body_hash, "body_hash")?;
        Ok(Self {
            body_len: 0,
            record_count: proto.record_count,
            first_record_hash,
            last_record_hash,
            body_hash,
            file_hash: input[trailer_end..].try_into().unwrap(),
        })
    }

    pub fn verify(
        &self,
        encoded_header: &[u8],
        body: &[u8],
        range_index: &[u8],
        trailer_proto: &[u8],
    ) -> Result<(), FormatError> {
        if self.body_len != body.len() as u64 {
            return Err(FormatError::InvalidDeclaredLength {
                context: "writer segment trailer body_len",
            });
        }
        if self.body_hash != hash32(body) {
            return Err(FormatError::HashMismatch {
                context: "writer segment body",
            });
        }
        let mut segment_hash_input = Vec::with_capacity(
            encoded_header.len() + body.len() + range_index.len() + trailer_proto.len(),
        );
        segment_hash_input.extend_from_slice(encoded_header);
        segment_hash_input.extend_from_slice(body);
        segment_hash_input.extend_from_slice(range_index);
        segment_hash_input.extend_from_slice(trailer_proto);
        if self.file_hash != hash32(&segment_hash_input) {
            return Err(FormatError::HashMismatch {
                context: "writer segment hash",
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedWriterSegment {
    pub bytes: Vec<u8>,
    pub family: FileFamily,
    pub segment_hash: Hash32,
    pub body_hash: Hash32,
    pub record_count: u64,
    pub first_record_hash: Hash32,
    pub last_record_hash: Hash32,
}

#[derive(Debug)]
pub struct DecodedWriterSegment<'a> {
    pub envelope: WriterSegmentFixedHeader,
    pub header: WriterSegmentHeaderProto,
    pub body: &'a [u8],
    pub range_index: &'a [u8],
    pub trailer_proto: &'a [u8],
    pub footer: WriterSegmentTrailer,
}

pub fn encode_writer_segment(
    family: FileFamily,
    flags: u16,
    header_proto: Vec<u8>,
    body: &[u8],
    range_index: &[u8],
    record_count: u64,
    first_record_hash: Hash32,
    last_record_hash: Hash32,
) -> Result<EncodedWriterSegment, FormatError> {
    decode_writer_segment_header(&header_proto)?;
    let footer = WriterSegmentTrailer::new(body, record_count, first_record_hash, last_record_hash);
    let trailer_proto = footer.trailer_proto();
    let envelope = WriterSegmentFixedHeader::new(
        family,
        flags,
        header_proto,
        body.len() as u64,
        range_index.len() as u64,
        u32::try_from(trailer_proto.len()).map_err(|_| FormatError::InvalidDeclaredLength {
            context: "writer segment trailer",
        })?,
    );
    let encoded_header = envelope.encode();
    let mut segment_hash_input = Vec::with_capacity(
        encoded_header.len() + body.len() + range_index.len() + trailer_proto.len(),
    );
    segment_hash_input.extend_from_slice(&encoded_header);
    segment_hash_input.extend_from_slice(body);
    segment_hash_input.extend_from_slice(range_index);
    segment_hash_input.extend_from_slice(&trailer_proto);
    let segment_hash = hash32(&segment_hash_input);
    let mut bytes = segment_hash_input;
    bytes.extend_from_slice(&segment_hash);
    Ok(EncodedWriterSegment {
        bytes,
        family,
        segment_hash,
        body_hash: footer.body_hash,
        record_count,
        first_record_hash,
        last_record_hash,
    })
}

pub fn decode_writer_segment<'a>(
    input: &'a [u8],
    expected_family: FileFamily,
) -> Result<DecodedWriterSegment<'a>, FormatError> {
    let envelope = WriterSegmentFixedHeader::decode(input)?;
    if envelope.family != expected_family {
        return Err(FormatError::FamilyMagicMismatch);
    }
    let header = decode_writer_segment_header(&envelope.header_proto)?;
    if header.writer_family != expected_family.writer_family_name() {
        return Err(FormatError::InvalidHeaderField {
            field: "writer_family",
        });
    }
    let header_end = envelope.encoded_len();
    let body_len =
        usize::try_from(envelope.body_len).map_err(|_| FormatError::InvalidDeclaredLength {
            context: "writer segment body",
        })?;
    let range_index_len = usize::try_from(envelope.range_index_len).map_err(|_| {
        FormatError::InvalidDeclaredLength {
            context: "writer segment range index",
        }
    })?;
    let trailer_len = envelope.trailer_len as usize;
    let body_end = header_end
        .checked_add(body_len)
        .ok_or(FormatError::InvalidDeclaredLength {
            context: "writer segment body",
        })?;
    let range_index_end =
        body_end
            .checked_add(range_index_len)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "writer segment range index",
            })?;
    let trailer_end =
        range_index_end
            .checked_add(trailer_len)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "writer segment trailer",
            })?;
    let expected_len =
        trailer_end
            .checked_add(SEGMENT_HASH_LEN)
            .ok_or(FormatError::InvalidDeclaredLength {
                context: "writer segment hash",
            })?;
    if input.len() != expected_len {
        return Err(FormatError::InvalidDeclaredLength {
            context: "writer segment",
        });
    }
    let body = &input[header_end..body_end];
    let range_index = &input[body_end..range_index_end];
    let trailer_proto = &input[range_index_end..trailer_end];
    if !range_index.is_empty() {
        decode_range_index(range_index)?;
    }
    let footer = WriterSegmentTrailer::decode(&input[range_index_end..])?;
    let mut footer = footer;
    footer.body_len = envelope.body_len;
    footer.verify(&input[..header_end], body, range_index, trailer_proto)?;
    Ok(DecodedWriterSegment {
        envelope,
        header,
        body,
        range_index,
        trailer_proto,
        footer,
    })
}

fn hash_from_proto_vec(value: Vec<u8>, context: &'static str) -> Result<Hash32, FormatError> {
    if value.len() != 32 {
        return Err(FormatError::InvalidFixedLength {
            context,
            expected: 32,
            actual: value.len(),
        });
    }
    Ok(value.try_into().unwrap())
}

fn write_u16_len(len: usize, out: &mut Vec<u8>, context: &'static str) -> Result<(), FormatError> {
    let len = u16::try_from(len).map_err(|_| FormatError::InvalidDeclaredLength { context })?;
    out.extend_from_slice(&len.to_le_bytes());
    Ok(())
}

fn write_uleb128(mut value: u64, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn read_uleb128(input: &mut &[u8], context: &'static str) -> Result<usize, FormatError> {
    let mut result = 0u64;
    for shift in (0..=63).step_by(7) {
        let byte = read_u8(input, context)?;
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return usize::try_from(result)
                .map_err(|_| FormatError::InvalidDeclaredLength { context });
        }
    }
    Err(FormatError::InvalidDeclaredLength { context })
}

fn read_u8(input: &mut &[u8], context: &'static str) -> Result<u8, FormatError> {
    if input.is_empty() {
        return Err(FormatError::TooShort {
            context,
            needed: 1,
            actual: 0,
        });
    }
    let value = input[0];
    *input = &input[1..];
    Ok(value)
}

fn read_u16_le(input: &mut &[u8], context: &'static str) -> Result<u16, FormatError> {
    let bytes = take_array::<2>(input, context)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32_le(input: &mut &[u8], context: &'static str) -> Result<u32, FormatError> {
    let bytes = take_array::<4>(input, context)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_le(input: &mut &[u8], context: &'static str) -> Result<u64, FormatError> {
    let bytes = take_array::<8>(input, context)?;
    Ok(u64::from_le_bytes(bytes))
}

fn take_array<const N: usize>(
    input: &mut &[u8],
    context: &'static str,
) -> Result<[u8; N], FormatError> {
    if input.len() < N {
        return Err(FormatError::TooShort {
            context,
            needed: N,
            actual: input.len(),
        });
    }
    let (head, tail) = input.split_at(N);
    *input = tail;
    Ok(head.try_into().unwrap())
}

fn take_len(input: &mut &[u8], len: usize, context: &'static str) -> Result<Vec<u8>, FormatError> {
    if input.len() < len {
        return Err(FormatError::TooShort {
            context,
            needed: len,
            actual: input.len(),
        });
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head.to_vec())
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
    fn file_family_magic_matches_corestore_rfc() {
        assert_eq!(FileFamily::MetadataSegment.expected_magic(), b"ANOBJM1\0");
        assert_eq!(FileFamily::DirectorySegment.expected_magic(), b"ANTDIR1\0");
        assert_eq!(FileFamily::FullTextSegment.expected_magic(), b"ANFTSG1\0");
        assert_eq!(FileFamily::VectorSegment.expected_magic(), b"ANVECG1\0");
        assert_eq!(FileFamily::AuthzTupleSegment.expected_magic(), b"ANAUTH1\0");
        assert_eq!(
            FileFamily::PersonalDbLogSegment.expected_magic(),
            b"ANPDB1\0\0"
        );
        assert_eq!(FileFamily::TypedFieldSegment.expected_magic(), b"ANTIDX1\0");
        assert_eq!(FileFamily::RegistrySegment.expected_magic(), b"ANREG1\0\0");
        assert_eq!(
            FileFamily::MeshControlSegment.expected_magic(),
            b"ANMESH1\0"
        );
    }

    #[test]
    fn envelope_round_trip_and_hash_validation() {
        let header_proto = encode_writer_segment_header(
            "anvil.test.segment.v1",
            "test-logical-file",
            FileFamily::MetadataSegment,
            0,
            None,
            None,
            1,
            vec![header_field_string("tenant_id", "tenant")],
        );
        let header =
            WriterSegmentFixedHeader::new(FileFamily::MetadataSegment, 0, header_proto, 12, 0, 8);
        let encoded = header.encode();
        let decoded = WriterSegmentFixedHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);

        let mut corrupted = encoded;
        corrupted[12] = corrupted[12].wrapping_add(64);
        assert!(matches!(
            WriterSegmentFixedHeader::decode(&corrupted).unwrap_err(),
            FormatError::TooShort { .. } | FormatError::InvalidDeclaredLength { .. }
        ));
    }

    #[test]
    fn range_index_round_trips_and_rejects_overlap() {
        let entries = vec![
            RangeIndexEntry {
                logical_start: 0,
                logical_end: 64,
                record_count: 2,
                boundary_values: vec![BoundaryValueRef {
                    dimension_id: 7,
                    value_type: 1,
                    value_bytes: b"tenant-a".to_vec(),
                }],
                min_key: b"a".to_vec(),
                max_key: b"m".to_vec(),
                stats_ref: b"stats-1".to_vec(),
            },
            RangeIndexEntry {
                logical_start: 64,
                logical_end: 128,
                record_count: 3,
                boundary_values: Vec::new(),
                min_key: b"n".to_vec(),
                max_key: b"z".to_vec(),
                stats_ref: Vec::new(),
            },
        ];

        let encoded = encode_range_index(&entries).unwrap();
        assert_eq!(decode_range_index(&encoded).unwrap(), entries);

        let overlapping = vec![
            RangeIndexEntry::covering_body(64, 1, b"a".to_vec(), b"b".to_vec()),
            RangeIndexEntry {
                logical_start: 63,
                logical_end: 96,
                record_count: 1,
                boundary_values: Vec::new(),
                min_key: b"c".to_vec(),
                max_key: b"d".to_vec(),
                stats_ref: Vec::new(),
            },
        ];
        assert_eq!(
            encode_range_index(&overlapping).unwrap_err(),
            FormatError::InvalidRangeIndexOrder
        );
    }

    #[test]
    fn footer_verifies_complete_file_hash() {
        let header_proto = encode_writer_segment_header(
            "anvil.test.segment.v1",
            "test-logical-file",
            FileFamily::MetadataSegment,
            0,
            None,
            None,
            1,
            Vec::new(),
        );
        let body = b"segment body";
        let range_index =
            single_body_range_index(body.len(), 2, sample_hash(1), sample_hash(2)).unwrap();
        let encoded = encode_writer_segment(
            FileFamily::MetadataSegment,
            0,
            header_proto,
            body,
            &range_index,
            2,
            sample_hash(1),
            sample_hash(2),
        )
        .unwrap();
        let decoded = decode_writer_segment(&encoded.bytes, FileFamily::MetadataSegment).unwrap();
        assert_eq!(decoded.body, body);
        assert_eq!(
            decode_range_index(decoded.range_index).unwrap(),
            vec![RangeIndexEntry::covering_body(
                body.len(),
                2,
                sample_hash(1),
                sample_hash(2)
            )]
        );
        assert_eq!(decoded.footer.record_count, 2);
        assert_eq!(decoded.footer.file_hash, encoded.segment_hash);

        let mut bad_body = encoded.bytes.clone();
        let body_offset = decoded.envelope.encoded_len();
        bad_body[body_offset] ^= 0x01;
        assert_eq!(
            decode_writer_segment(&bad_body, FileFamily::MetadataSegment).unwrap_err(),
            FormatError::HashMismatch {
                context: "writer segment body"
            }
        );
    }
}
