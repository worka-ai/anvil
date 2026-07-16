use super::stream_event_hash_proto::encode_stream_event_hash_input;
use super::types::{SealStreamSegment, StreamRecord};
use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use prost::Message;
use sha2::{Digest, Sha256};

pub(super) const CORE_STREAM_SEGMENT_MAGIC: &[u8; 8] = b"ANSTRM\n\0";
const CORE_STREAM_SPARSE_INDEX_MAGIC: &[u8; 8] = b"ANSSIX1\0";
const CORE_STREAM_SEGMENT_VERSION: u16 = 1;
const CORE_STREAM_SEGMENT_HEADER_SCHEMA: &str = "anvil.stream.segment.v1";
const CORE_STREAM_RECORD_HEADER_SCHEMA: &str = "anvil.stream.record.v1";
const CORE_STREAM_SEGMENT_TRAILER_SCHEMA: &str = "anvil.stream.segment_trailer.v1";
const ZERO_HASH: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Clone, PartialEq, Message)]
struct StreamSegmentHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    realm_id: String,
    #[prost(string, tag = "3")]
    stream_id: String,
    #[prost(string, tag = "4")]
    segment_id: String,
    #[prost(uint64, tag = "5")]
    first_sequence: u64,
    #[prost(uint64, tag = "6")]
    writer_epoch: u64,
    #[prost(string, tag = "7")]
    writer_node_id: String,
    #[prost(uint64, tag = "8")]
    created_at_unix_nanos: u64,
    #[prost(string, tag = "9")]
    authz_scope_hash: String,
    #[prost(string, tag = "10")]
    partition_id: String,
    #[prost(uint64, tag = "11")]
    last_sequence: u64,
    #[prost(string, tag = "12")]
    source_family: String,
    #[prost(string, tag = "13")]
    created_at: String,
    #[prost(string, tag = "14")]
    sealed_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct StreamRecordHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    idempotency_key_hash: String,
    #[prost(string, tag = "3")]
    payload_hash: String,
    #[prost(string, tag = "4")]
    payload_content_type: String,
    #[prost(message, repeated, tag = "5")]
    boundary_values: Vec<PendingBoundaryValueProto>,
    #[prost(string, repeated, tag = "6")]
    index_hint_hashes: Vec<String>,
    #[prost(string, tag = "10")]
    stream_id: String,
    #[prost(uint64, tag = "11")]
    sequence: u64,
    #[prost(string, tag = "12")]
    record_kind: String,
    #[prost(string, tag = "13")]
    mutation_id: String,
    #[prost(string, tag = "14")]
    previous_event_hash: String,
    #[prost(string, tag = "15")]
    event_hash: String,
    #[prost(string, tag = "16")]
    transaction_id: String,
    #[prost(string, tag = "17")]
    created_at: String,
    #[prost(string, tag = "18")]
    user_metadata_json: String,
}

#[derive(Clone, PartialEq, Message)]
struct PendingBoundaryValueProto {
    #[prost(string, tag = "1")]
    dimension: String,
    #[prost(string, tag = "2")]
    value_type: String,
    #[prost(bytes = "vec", tag = "3")]
    encoded_value: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct StreamSegmentTrailerProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(uint64, tag = "2")]
    last_sequence: u64,
    #[prost(uint64, tag = "3")]
    record_count: u64,
    #[prost(string, tag = "4")]
    sparse_index_hash: String,
    #[prost(uint64, tag = "5")]
    sealed_at_unix_nanos: u64,
    #[prost(string, tag = "6")]
    segment_payload_hash: String,
    #[prost(string, tag = "10")]
    stream_id: String,
    #[prost(string, tag = "11")]
    partition_id: String,
    #[prost(string, tag = "12")]
    segment_id: String,
    #[prost(uint64, tag = "13")]
    first_sequence: u64,
    #[prost(string, tag = "14")]
    sealed_at: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StreamSparseIndexEntry {
    first_sequence: u64,
    first_timestamp_nanos: i64,
    record_ordinal: u32,
    byte_offset: u64,
}

pub(super) fn encode_stream_segment(
    input: &SealStreamSegment,
    records: &[StreamRecord],
    segment_id: &str,
    first_sequence: u64,
    last_sequence: u64,
) -> Result<Vec<u8>> {
    let sealed_at = now_rfc3339();
    let created_at = records
        .first()
        .map(|record| record.created_at.clone())
        .unwrap_or_else(|| sealed_at.clone());
    let header = StreamSegmentHeaderProto {
        schema: CORE_STREAM_SEGMENT_HEADER_SCHEMA.to_string(),
        realm_id: input.partition_id.clone(),
        stream_id: input.stream_id.clone(),
        segment_id: segment_id.to_string(),
        first_sequence,
        writer_epoch: 0,
        writer_node_id: "local-node".to_string(),
        created_at_unix_nanos: parse_stream_record_timestamp_nanos(&created_at)
            .try_into()
            .unwrap_or_default(),
        authz_scope_hash: String::new(),
        partition_id: input.partition_id.clone(),
        last_sequence,
        source_family: input.segment_kind.clone(),
        created_at,
        sealed_at: sealed_at.clone(),
    };

    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_STREAM_SEGMENT_MAGIC);
    bytes.extend_from_slice(&CORE_STREAM_SEGMENT_VERSION.to_le_bytes());
    let header_proto = encode_deterministic_proto(&header)?;
    write_u32_le(&mut bytes, header_proto.len())?;
    bytes.extend_from_slice(&header_proto);
    bytes.extend_from_slice(&(records.len() as u64).to_le_bytes());

    let mut index_entries = Vec::with_capacity(records.len());
    for (ordinal, record) in records.iter().enumerate() {
        let record_offset = bytes.len() as u64;
        let timestamp_nanos = parse_stream_record_timestamp_nanos(&record.created_at);
        let record_header = StreamRecordHeaderProto {
            schema: CORE_STREAM_RECORD_HEADER_SCHEMA.to_string(),
            idempotency_key_hash: record.idempotency_key_hash.clone().unwrap_or_default(),
            payload_hash: record.payload_hash.clone(),
            payload_content_type: record.content_type.clone().unwrap_or_default(),
            boundary_values: Vec::new(),
            index_hint_hashes: Vec::new(),
            stream_id: record.stream_id.clone(),
            sequence: record.sequence,
            record_kind: record.record_kind.clone(),
            mutation_id: record
                .transaction_id
                .clone()
                .unwrap_or_else(|| record.cursor.clone()),
            previous_event_hash: record.previous_event_hash.clone(),
            event_hash: record.event_hash.clone(),
            transaction_id: record.transaction_id.clone().unwrap_or_default(),
            created_at: record.created_at.clone(),
            user_metadata_json: record.user_metadata_json.clone(),
        };
        let record_header_proto = encode_deterministic_proto(&record_header)?;
        bytes.extend_from_slice(&record.sequence.to_le_bytes());
        bytes.extend_from_slice(&timestamp_nanos.to_le_bytes());
        write_u32_le(&mut bytes, record_header_proto.len())?;
        bytes.extend_from_slice(&(record.payload.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&record_header_proto);
        bytes.extend_from_slice(&record.payload);
        let record_frame_end = bytes.len();
        let actual_crc = crc32c(&bytes[record_offset as usize..record_frame_end]);
        bytes.extend_from_slice(&actual_crc.to_le_bytes());
        index_entries.push(StreamSparseIndexEntry {
            first_sequence: record.sequence,
            first_timestamp_nanos: timestamp_nanos,
            record_ordinal: ordinal as u32,
            byte_offset: record_offset,
        });
    }

    let index_bytes = encode_stream_sparse_index(&index_entries)?;
    bytes.extend_from_slice(&(index_bytes.len() as u64).to_le_bytes());
    bytes.extend_from_slice(&index_bytes);

    let segment_payload_hash = format!("sha256:{}", sha256_hex(&bytes));
    let trailer = StreamSegmentTrailerProto {
        schema: CORE_STREAM_SEGMENT_TRAILER_SCHEMA.to_string(),
        last_sequence,
        record_count: records.len() as u64,
        sparse_index_hash: format!("sha256:{}", sha256_hex(&index_bytes)),
        sealed_at_unix_nanos: parse_stream_record_timestamp_nanos(&sealed_at)
            .try_into()
            .unwrap_or_default(),
        segment_payload_hash,
        stream_id: input.stream_id.clone(),
        partition_id: input.partition_id.clone(),
        segment_id: segment_id.to_string(),
        first_sequence,
        sealed_at,
    };
    let trailer_proto = encode_deterministic_proto(&trailer)?;
    write_u32_le(&mut bytes, trailer_proto.len())?;
    bytes.extend_from_slice(&trailer_proto);
    let segment_hash = Sha256::digest(&bytes);
    bytes.extend_from_slice(&segment_hash);
    Ok(bytes)
}

pub(super) fn decode_stream_segment(bytes: &[u8]) -> Result<Vec<StreamRecord>> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_STREAM_SEGMENT_MAGIC.len())?;
    if magic != CORE_STREAM_SEGMENT_MAGIC {
        bail!("CoreStore stream segment has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_STREAM_SEGMENT_VERSION {
        bail!("CoreStore stream segment has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let header_proto = read_exact(bytes, &mut offset, header_len)?;
    let header: StreamSegmentHeaderProto =
        decode_deterministic_proto(header_proto, "stream segment header")?;
    if header.schema != CORE_STREAM_SEGMENT_HEADER_SCHEMA {
        bail!("CoreStore stream segment header has invalid schema");
    }
    let record_count = read_u64_le(bytes, &mut offset)?;
    let mut records = Vec::with_capacity(record_count as usize);
    for _ in 0..record_count {
        let record_frame_start = offset;
        let frame_sequence = read_u64_le(bytes, &mut offset)?;
        let frame_timestamp_nanos = read_i64_le(bytes, &mut offset)?;
        let record_header_len = read_u32_le(bytes, &mut offset)? as usize;
        let payload_len = read_u64_le(bytes, &mut offset)? as usize;
        let record_header_proto = read_exact(bytes, &mut offset, record_header_len)?;
        let record_header: StreamRecordHeaderProto =
            decode_deterministic_proto(record_header_proto, "stream record header")?;
        if record_header.schema != CORE_STREAM_RECORD_HEADER_SCHEMA {
            bail!("CoreStore stream segment record header has invalid schema");
        }
        let payload = read_exact(bytes, &mut offset, payload_len)?.to_vec();
        let record_frame_end = offset;
        let expected_crc = read_u32_le(bytes, &mut offset)?;
        let actual_crc = crc32c(&bytes[record_frame_start..record_frame_end]);
        if actual_crc != expected_crc {
            bail!("CoreStore stream segment record checksum mismatch");
        }
        if record_header.stream_id != header.stream_id {
            bail!("CoreStore stream segment record stream_id mismatch");
        }
        if record_header.sequence != frame_sequence {
            bail!("CoreStore stream segment record frame sequence mismatch");
        }
        if parse_stream_record_timestamp_nanos(&record_header.created_at) != frame_timestamp_nanos {
            bail!("CoreStore stream segment record frame timestamp mismatch");
        }
        let record = StreamRecord {
            schema: "anvil.core.watch_event.v1".to_string(),
            stream_id: record_header.stream_id,
            partition_id: header.partition_id.clone(),
            sequence: record_header.sequence,
            cursor: format!("{}:{:020}", header.stream_id, record_header.sequence),
            previous_event_hash: record_header.previous_event_hash,
            event_hash: record_header.event_hash,
            record_kind: record_header.record_kind,
            payload_hash: record_header.payload_hash,
            payload,
            content_type: non_empty_string(record_header.payload_content_type),
            user_metadata_json: if record_header.user_metadata_json.is_empty() {
                "{}".to_string()
            } else {
                record_header.user_metadata_json
            },
            transaction_id: non_empty_string(record_header.transaction_id),
            idempotency_key_hash: non_empty_string(record_header.idempotency_key_hash),
            created_at: record_header.created_at,
        };
        verify_stream_record(records.last(), &record)?;
        records.push(record);
    }
    let index_len = read_u64_le(bytes, &mut offset)? as usize;
    let index_bytes = read_exact(bytes, &mut offset, index_len)?;
    validate_stream_sparse_index(index_bytes, &records)?;
    let trailer_len_start = offset;
    let trailer_len = read_u32_le(bytes, &mut offset)? as usize;
    let trailer_proto = read_exact(bytes, &mut offset, trailer_len)?;
    let trailer: StreamSegmentTrailerProto =
        decode_deterministic_proto(trailer_proto, "stream segment trailer")?;
    if trailer.schema != CORE_STREAM_SEGMENT_TRAILER_SCHEMA {
        bail!("CoreStore stream segment trailer has invalid schema");
    }
    let sparse_index_hash = format!("sha256:{}", sha256_hex(index_bytes));
    if trailer.sparse_index_hash != sparse_index_hash {
        bail!("CoreStore stream segment trailer sparse index hash mismatch");
    }
    if trailer.stream_id != header.stream_id
        || trailer.partition_id != header.partition_id
        || trailer.segment_id != header.segment_id
        || trailer.first_sequence != header.first_sequence
        || trailer.last_sequence != header.last_sequence
        || trailer.record_count != record_count
    {
        bail!("CoreStore stream segment trailer scope mismatch");
    }
    let segment_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore stream segment has trailing bytes");
    }
    let actual_segment_hash = Sha256::digest(&bytes[..trailer_len_start + 4 + trailer_len]);
    let actual_segment_hash: &[u8] = actual_segment_hash.as_ref();
    if segment_hash != actual_segment_hash {
        bail!("CoreStore stream segment hash mismatch");
    }
    if records
        .first()
        .map(|record| record.sequence)
        .unwrap_or_default()
        != header.first_sequence
    {
        bail!("CoreStore stream segment header first_sequence mismatch");
    }
    if records
        .last()
        .map(|record| record.sequence)
        .unwrap_or_default()
        != header.last_sequence
    {
        bail!("CoreStore stream segment header last_sequence mismatch");
    }
    Ok(records)
}

fn encode_deterministic_proto<M>(message: &M) -> Result<Vec<u8>>
where
    M: Message + Default + PartialEq,
{
    let bytes = message.encode_to_vec();
    let decoded = M::decode(bytes.as_slice())?;
    if decoded != *message || decoded.encode_to_vec() != bytes {
        bail!("CoreStore stream segment protobuf did not round-trip deterministically");
    }
    Ok(bytes)
}

fn decode_deterministic_proto<M>(bytes: &[u8], label: &str) -> Result<M>
where
    M: Message + Default + PartialEq,
{
    let decoded = M::decode(bytes).with_context(|| format!("decode CoreStore {label} protobuf"))?;
    if decoded.encode_to_vec() != bytes {
        bail!("CoreStore {label} protobuf is not deterministic canonical encoding");
    }
    Ok(decoded)
}

fn verify_stream_record(previous: Option<&StreamRecord>, record: &StreamRecord) -> Result<()> {
    let previous_sequence = previous.map(|prev| prev.sequence).unwrap_or(0);
    let expected_previous = previous
        .map(|prev| prev.event_hash.clone())
        .unwrap_or_else(|| ZERO_HASH.to_string());
    verify_stream_record_after_head(
        &record.stream_id,
        previous_sequence,
        &expected_previous,
        record,
    )
}

fn verify_stream_record_after_head(
    stream_id: &str,
    previous_sequence: u64,
    previous_event_hash: &str,
    record: &StreamRecord,
) -> Result<()> {
    if record.stream_id != stream_id {
        bail!("CoreStore stream record metadata row has invalid scope");
    }
    let expected_sequence = previous_sequence
        .checked_add(1)
        .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))?;
    if record.sequence != expected_sequence {
        bail!(
            "CoreStore stream {} sequence mismatch: expected {}, got {}",
            stream_id,
            expected_sequence,
            record.sequence
        );
    }
    let expected_cursor = format!("{stream_id}:{expected_sequence:020}");
    if record.cursor != expected_cursor {
        bail!("CoreStore stream {} cursor mismatch", record.stream_id);
    }
    if record.previous_event_hash != previous_event_hash {
        bail!("CoreStore stream {} hash chain mismatch", record.stream_id);
    }
    let expected_payload_hash = format!("sha256:{}", sha256_hex(&record.payload));
    if record.payload_hash != expected_payload_hash {
        bail!(
            "CoreStore stream {} payload hash mismatch",
            record.stream_id
        );
    }
    let expected_hash = format!("sha256:{}", sha256_hex(&event_hash_input(record)?));
    if record.event_hash != expected_hash {
        bail!("CoreStore stream {} event hash mismatch", record.stream_id);
    }
    Ok(())
}

fn event_hash_input(record: &StreamRecord) -> Result<Vec<u8>> {
    encode_stream_event_hash_input(record)
}

fn encode_stream_sparse_index(entries: &[StreamSparseIndexEntry]) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(CORE_STREAM_SPARSE_INDEX_MAGIC);
    write_u32_le(&mut bytes, entries.len())?;
    for entry in entries {
        bytes.extend_from_slice(&entry.first_sequence.to_le_bytes());
        bytes.extend_from_slice(&entry.record_ordinal.to_le_bytes());
        bytes.extend_from_slice(&entry.byte_offset.to_le_bytes());
    }
    write_u32_le(&mut bytes, entries.len())?;
    for entry in entries {
        bytes.extend_from_slice(&entry.first_timestamp_nanos.to_le_bytes());
        bytes.extend_from_slice(&entry.record_ordinal.to_le_bytes());
        bytes.extend_from_slice(&entry.byte_offset.to_le_bytes());
    }
    let crc = crc32c(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn validate_stream_sparse_index(bytes: &[u8], records: &[StreamRecord]) -> Result<()> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_STREAM_SPARSE_INDEX_MAGIC.len())?;
    if magic != CORE_STREAM_SPARSE_INDEX_MAGIC {
        bail!("CoreStore stream sparse index has invalid magic");
    }
    let sequence_count = read_u32_le(bytes, &mut offset)? as usize;
    if sequence_count != records.len() {
        bail!("CoreStore stream sparse index sequence count mismatch");
    }
    let mut previous_sequence = None;
    for ordinal in 0..sequence_count {
        let first_sequence = read_u64_le(bytes, &mut offset)?;
        let record_ordinal = read_u32_le(bytes, &mut offset)?;
        let _byte_offset = read_u64_le(bytes, &mut offset)?;
        if record_ordinal != ordinal as u32 {
            bail!("CoreStore stream sparse index ordinal mismatch");
        }
        if Some(first_sequence) <= previous_sequence {
            bail!("CoreStore stream sparse index sequence entries are not sorted");
        }
        if records
            .get(ordinal)
            .map(|record| record.sequence)
            .unwrap_or_default()
            != first_sequence
        {
            bail!("CoreStore stream sparse index sequence does not match record");
        }
        previous_sequence = Some(first_sequence);
    }
    let timestamp_count = read_u32_le(bytes, &mut offset)? as usize;
    if timestamp_count != records.len() {
        bail!("CoreStore stream sparse index timestamp count mismatch");
    }
    let mut previous_timestamp = None;
    for ordinal in 0..timestamp_count {
        let timestamp = read_i64_le(bytes, &mut offset)?;
        let record_ordinal = read_u32_le(bytes, &mut offset)?;
        let _byte_offset = read_u64_le(bytes, &mut offset)?;
        if record_ordinal != ordinal as u32 {
            bail!("CoreStore stream sparse timestamp ordinal mismatch");
        }
        if previous_timestamp.is_some_and(|previous| timestamp < previous) {
            bail!("CoreStore stream sparse timestamp entries are not sorted");
        }
        previous_timestamp = Some(timestamp);
    }
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    if offset != bytes.len() {
        bail!("CoreStore stream sparse index has trailing bytes");
    }
    let actual_crc = crc32c(&bytes[..bytes.len() - 4]);
    if actual_crc != expected_crc {
        bail!("CoreStore stream sparse index checksum mismatch");
    }
    Ok(())
}

fn parse_stream_record_timestamp_nanos(value: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(value)
        .ok()
        .and_then(|value| value.timestamp_nanos_opt())
        .unwrap_or_default()
}

fn non_empty_string(value: String) -> Option<String> {
    if value.is_empty() { None } else { Some(value) }
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn write_u32_le(out: &mut Vec<u8>, value: usize) -> Result<()> {
    let value = u32::try_from(value).map_err(|_| anyhow!("CoreStore frame length exceeds u32"))?;
    out.extend_from_slice(&value.to_le_bytes());
    Ok(())
}

fn read_exact<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> Result<&'a [u8]> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| anyhow!("CoreStore frame offset overflow"))?;
    if end > bytes.len() {
        bail!("CoreStore frame ended unexpectedly");
    }
    let slice = &bytes[*offset..end];
    *offset = end;
    Ok(slice)
}

fn read_u16_le(bytes: &[u8], offset: &mut usize) -> Result<u16> {
    let raw = read_exact(bytes, offset, 2)?;
    Ok(u16::from_le_bytes(raw.try_into()?))
}

fn read_u32_le(bytes: &[u8], offset: &mut usize) -> Result<u32> {
    let raw = read_exact(bytes, offset, 4)?;
    Ok(u32::from_le_bytes(raw.try_into()?))
}

fn read_u64_le(bytes: &[u8], offset: &mut usize) -> Result<u64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(u64::from_le_bytes(raw.try_into()?))
}

fn read_i64_le(bytes: &[u8], offset: &mut usize) -> Result<i64> {
    let raw = read_exact(bytes, offset, 8)?;
    Ok(i64::from_le_bytes(raw.try_into()?))
}

fn crc32c(bytes: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0x82f6_3b78;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(
        stream_id: &str,
        partition_id: &str,
        sequence: u64,
        previous_event_hash: String,
        payload: Vec<u8>,
    ) -> StreamRecord {
        let created_at = format!("2026-07-08T00:00:0{}+00:00", sequence);
        let payload_hash = format!("sha256:{}", sha256_hex(&payload));
        let mut record = StreamRecord {
            schema: "anvil.core.watch_event.v1".to_string(),
            stream_id: stream_id.to_string(),
            partition_id: partition_id.to_string(),
            sequence,
            cursor: format!("{stream_id}:{sequence:020}"),
            previous_event_hash,
            event_hash: String::new(),
            record_kind: "test".to_string(),
            payload_hash,
            payload,
            content_type: None,
            user_metadata_json: "{}".to_string(),
            transaction_id: None,
            idempotency_key_hash: Some(format!("sha256:key-{sequence}")),
            created_at,
        };
        record.event_hash = format!("sha256:{}", sha256_hex(&event_hash_input(&record).unwrap()));
        record
    }

    #[test]
    fn stream_segment_headers_and_trailers_are_protobuf_not_json() {
        let first = record(
            "stream-a",
            "partition-a",
            1,
            ZERO_HASH.to_string(),
            b"one".to_vec(),
        );
        let second = record(
            "stream-a",
            "partition-a",
            2,
            first.event_hash.clone(),
            b"two".to_vec(),
        );
        let input = SealStreamSegment {
            stream_id: "stream-a".to_string(),
            partition_id: "partition-a".to_string(),
            through_sequence: Some(2),
            segment_kind: "unit".to_string(),
            mutation_id: "mutation-a".to_string(),
        };
        let bytes =
            encode_stream_segment(&input, &[first.clone(), second.clone()], "seg-a", 1, 2).unwrap();

        let mut offset = CORE_STREAM_SEGMENT_MAGIC.len() + 2;
        let header_len = read_u32_le(&bytes, &mut offset).unwrap() as usize;
        let header_bytes = read_exact(&bytes, &mut offset, header_len).unwrap();
        assert!(serde_json::from_slice::<serde_json::Value>(header_bytes).is_err());
        let decoded_header: StreamSegmentHeaderProto =
            decode_deterministic_proto(header_bytes, "stream segment header").unwrap();
        assert_eq!(decoded_header.schema, CORE_STREAM_SEGMENT_HEADER_SCHEMA);

        let decoded = decode_stream_segment(&bytes).unwrap();
        assert_eq!(decoded, vec![first, second]);
    }
}
