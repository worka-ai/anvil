use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use prost::Message;
use sha2::{Digest, Sha256};
use std::path::PathBuf;

use crate::formats::writer::WriterFamily;

use super::types::CoreBoundaryValue;

pub(super) const CORE_BLOCK_SHARD_MAGIC: &[u8; 8] = b"ANBLK\n\0\0";
const CORE_BLOCK_SHARD_VERSION: u16 = 1;
const CORE_BLOCK_SHARD_HEADER_SCHEMA: &str = "anvil.core.block_shard.v1";

#[derive(Debug, Clone)]
pub(super) struct BlockShardHeaderInput {
    pub(super) block_id: String,
    pub(super) erasure_set_id: String,
    pub(super) shard_index: u16,
    pub(super) erasure_profile_id: String,
    pub(super) logical_file_id: String,
    pub(super) logical_offset: u64,
    pub(super) logical_length: u64,
    pub(super) payload_plain_hash: String,
    pub(super) payload_stored_hash: String,
    pub(super) compression: String,
    pub(super) encryption: String,
    pub(super) placement_epoch: u64,
    pub(super) boundary_summary_hash: String,
    pub(super) boundary_values_b64: String,
    pub(super) writer_family: String,
    pub(super) created_by_mutation_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct BlockShardHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    pub(super) block_id: String,
    #[prost(string, tag = "3")]
    pub(super) erasure_set_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    pub(super) erasure_profile_id: String,
    #[prost(string, tag = "6")]
    pub(super) logical_file_id: String,
    #[prost(uint64, tag = "7")]
    pub(super) logical_offset: u64,
    #[prost(uint64, tag = "8")]
    pub(super) logical_length: u64,
    #[prost(string, tag = "9")]
    pub(super) payload_plain_hash: String,
    #[prost(string, tag = "10")]
    pub(super) payload_stored_hash: String,
    #[prost(string, tag = "11")]
    pub(super) compression: String,
    #[prost(string, tag = "12")]
    pub(super) encryption: String,
    #[prost(uint64, tag = "13")]
    pub(super) placement_epoch: u64,
    #[prost(string, tag = "14")]
    pub(super) boundary_summary_hash: String,
    #[prost(string, tag = "15")]
    pub(super) boundary_values_b64: String,
    #[prost(string, tag = "16")]
    pub(super) writer_family: String,
    #[prost(string, tag = "17")]
    pub(super) created_by_mutation_id: String,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BlockShardExpectation<'a> {
    pub(super) block_id: &'a str,
    pub(super) shard_index: u16,
    pub(super) erasure_profile_id: &'a str,
    pub(super) placement_epoch: u64,
    pub(super) payload_hash: &'a str,
    pub(super) payload_len: u64,
    pub(super) boundary_summary_hash: Option<&'a str>,
    pub(super) boundary_values_b64: Option<&'a str>,
}

pub(super) struct ShardReceiptPayloadInput<'a> {
    pub(super) block_id: &'a str,
    pub(super) shard_index: u16,
    pub(super) erasure_profile: &'a str,
    pub(super) node_id: &'a str,
    pub(super) region_id: &'a str,
    pub(super) cell_id: &'a str,
    pub(super) placement_epoch: u64,
    pub(super) shard_length: u64,
    pub(super) shard_hash: &'a str,
    pub(super) fsync_sequence: u64,
    pub(super) written_at_unix_nanos: u64,
    pub(super) boundary_summary_hash: &'a str,
}

#[derive(Clone, PartialEq, Message)]
struct BoundaryValuesProto {
    #[prost(message, repeated, tag = "1")]
    values: Vec<BoundaryValueProto>,
}

#[derive(Clone, PartialEq, Message)]
struct BoundaryValueProto {
    #[prost(uint64, tag = "1")]
    schema_generation: u64,
    #[prost(string, tag = "2")]
    name: String,
    #[prost(string, tag = "3")]
    value_type: String,
    #[prost(string, tag = "4")]
    value: String,
    #[prost(string, repeated, tag = "5")]
    categories: Vec<String>,
    #[prost(string, tag = "6")]
    source_kind: String,
    #[prost(bool, tag = "7")]
    required: bool,
    #[prost(uint32, tag = "8")]
    max_values_per_block: u32,
    #[prost(string, tag = "9")]
    compaction_scope: String,
    #[prost(bool, tag = "10")]
    shared_ranges_allowed: bool,
    #[prost(string, repeated, tag = "11")]
    shared_record_kinds: Vec<String>,
    #[prost(string, tag = "12")]
    placement_affinity: String,
}

#[derive(Clone, PartialEq, Message)]
struct ShardReceiptPayloadProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    block_id: String,
    #[prost(string, tag = "3")]
    shard_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    erasure_profile: String,
    #[prost(string, tag = "6")]
    node_id: String,
    #[prost(string, tag = "7")]
    region_id: String,
    #[prost(string, tag = "8")]
    cell_id: String,
    #[prost(uint64, tag = "9")]
    placement_epoch: u64,
    #[prost(uint64, tag = "10")]
    shard_length: u64,
    #[prost(string, tag = "11")]
    shard_hash: String,
    #[prost(uint64, tag = "12")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "13")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "14")]
    boundary_summary_hash: String,
}

pub(super) fn shard_receipt_payload_hash(input: ShardReceiptPayloadInput<'_>) -> String {
    let shard_id = format!("{}:{}", input.block_id, input.shard_index);
    let proto = ShardReceiptPayloadProto {
        schema: "anvil.core.shard_receipt.v1".to_string(),
        block_id: input.block_id.to_string(),
        shard_id,
        shard_index: u32::from(input.shard_index),
        erasure_profile: input.erasure_profile.to_string(),
        node_id: input.node_id.to_string(),
        region_id: input.region_id.to_string(),
        cell_id: input.cell_id.to_string(),
        placement_epoch: input.placement_epoch,
        shard_length: input.shard_length,
        shard_hash: input.shard_hash.to_string(),
        fsync_sequence: input.fsync_sequence,
        written_at_unix_nanos: input.written_at_unix_nanos,
        boundary_summary_hash: input.boundary_summary_hash.to_string(),
    };
    let bytes = proto.encode_to_vec();
    descriptor_hash(&["anvil.shard.receipt.v1", &hex::encode(bytes)])
}

pub(super) fn encode_block_shard_file(
    header: BlockShardHeaderInput,
    payload: &[u8],
) -> Result<Vec<u8>> {
    let header_proto = encode_block_shard_header_proto(header)?;
    let mut out = Vec::with_capacity(
        CORE_BLOCK_SHARD_MAGIC.len() + 2 + 4 + header_proto.len() + 8 + payload.len() + 4 + 32,
    );
    out.extend_from_slice(CORE_BLOCK_SHARD_MAGIC);
    out.extend_from_slice(&CORE_BLOCK_SHARD_VERSION.to_le_bytes());
    write_u32_le(&mut out, header_proto.len())?;
    out.extend_from_slice(&header_proto);
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(payload);
    let mut crc_input = Vec::with_capacity(header_proto.len() + payload.len());
    crc_input.extend_from_slice(&header_proto);
    crc_input.extend_from_slice(payload);
    out.extend_from_slice(&crc32c(&crc_input).to_le_bytes());
    let file_hash = Sha256::digest(&out);
    out.extend_from_slice(file_hash.as_ref());
    Ok(out)
}

pub(super) async fn read_block_shard_file(
    path: &PathBuf,
    expectation: BlockShardExpectation<'_>,
    operation: &'static str,
) -> Result<Vec<u8>> {
    let bytes = tokio::fs::read(path)
        .await
        .with_context(|| format!("{operation}: read CoreStore block shard {}", path.display()))?;
    let (header, payload) = decode_block_shard_file(&bytes)?;
    validate_block_shard_header(&header, expectation)?;
    let actual_hash = format!("sha256:{}", sha256_hex(&payload));
    if actual_hash != expectation.payload_hash {
        bail!(
            "CoreStore block shard payload hash mismatch: expected {}, got {}",
            expectation.payload_hash,
            actual_hash
        );
    }
    if expectation.payload_len > 0 && payload.len() as u64 != expectation.payload_len {
        bail!("CoreStore block shard payload length mismatch");
    }
    Ok(payload)
}

fn decode_block_shard_file(bytes: &[u8]) -> Result<(BlockShardHeaderProto, Vec<u8>)> {
    let mut offset = 0usize;
    let magic = read_exact(bytes, &mut offset, CORE_BLOCK_SHARD_MAGIC.len())?;
    if magic != CORE_BLOCK_SHARD_MAGIC {
        bail!("CoreStore block shard has invalid magic");
    }
    let version = read_u16_le(bytes, &mut offset)?;
    if version != CORE_BLOCK_SHARD_VERSION {
        bail!("CoreStore block shard has unsupported version {version}");
    }
    let header_len = read_u32_le(bytes, &mut offset)? as usize;
    let header_proto = read_exact(bytes, &mut offset, header_len)?;
    let header = decode_block_shard_header_proto(header_proto)?;
    let payload_len = read_u64_le(bytes, &mut offset)? as usize;
    let payload = read_exact(bytes, &mut offset, payload_len)?.to_vec();
    let expected_crc = read_u32_le(bytes, &mut offset)?;
    let mut crc_input = Vec::with_capacity(header_proto.len() + payload.len());
    crc_input.extend_from_slice(header_proto);
    crc_input.extend_from_slice(&payload);
    if crc32c(&crc_input) != expected_crc {
        bail!("CoreStore block shard checksum mismatch");
    }
    let file_hash_start = offset;
    let expected_file_hash = read_exact(bytes, &mut offset, 32)?;
    if offset != bytes.len() {
        bail!("CoreStore block shard has trailing bytes");
    }
    let actual_file_hash = Sha256::digest(&bytes[..file_hash_start]);
    let actual_file_hash: &[u8] = actual_file_hash.as_ref();
    if expected_file_hash != actual_file_hash {
        bail!("CoreStore block shard file hash mismatch");
    }
    Ok((header, payload))
}

fn encode_block_shard_header_proto(header: BlockShardHeaderInput) -> Result<Vec<u8>> {
    let proto = BlockShardHeaderProto {
        schema: CORE_BLOCK_SHARD_HEADER_SCHEMA.to_string(),
        block_id: header.block_id,
        erasure_set_id: header.erasure_set_id,
        shard_index: u32::from(header.shard_index),
        erasure_profile_id: header.erasure_profile_id,
        logical_file_id: header.logical_file_id,
        logical_offset: header.logical_offset,
        logical_length: header.logical_length,
        payload_plain_hash: header.payload_plain_hash,
        payload_stored_hash: header.payload_stored_hash,
        compression: header.compression,
        encryption: header.encryption,
        placement_epoch: header.placement_epoch,
        boundary_summary_hash: header.boundary_summary_hash,
        boundary_values_b64: header.boundary_values_b64,
        writer_family: header.writer_family,
        created_by_mutation_id: header.created_by_mutation_id,
    };
    let mut bytes = Vec::new();
    proto.encode(&mut bytes)?;
    Ok(bytes)
}

fn decode_block_shard_header_proto(bytes: &[u8]) -> Result<BlockShardHeaderProto> {
    let header = BlockShardHeaderProto::decode(bytes)?;
    let mut canonical = Vec::new();
    header.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore block shard header is not deterministic protobuf");
    }
    Ok(header)
}

fn validate_block_shard_header(
    header: &BlockShardHeaderProto,
    expectation: BlockShardExpectation<'_>,
) -> Result<()> {
    if header.schema != CORE_BLOCK_SHARD_HEADER_SCHEMA {
        bail!("CoreStore block shard header has invalid schema");
    }
    if header.block_id != expectation.block_id {
        bail!(
            "CoreStore block shard header block_id mismatch: expected {}, got {}",
            expectation.block_id,
            header.block_id
        );
    }
    if header.shard_index != u32::from(expectation.shard_index) {
        bail!(
            "CoreStore block shard header shard_index mismatch: expected {}, got {}",
            expectation.shard_index,
            header.shard_index
        );
    }
    if header.erasure_profile_id != expectation.erasure_profile_id {
        bail!(
            "CoreStore block shard header erasure_profile_id mismatch: expected {}, got {}",
            expectation.erasure_profile_id,
            header.erasure_profile_id
        );
    }
    validate_shard_logical_file_id(&header.logical_file_id)?;
    if WriterFamily::from_name(&header.writer_family).is_none() {
        bail!(
            "CoreStore block shard header has unknown writer_family {}",
            header.writer_family
        );
    }
    if header.placement_epoch != expectation.placement_epoch {
        bail!(
            "CoreStore block shard header placement_epoch mismatch: expected {}, got {}",
            expectation.placement_epoch,
            header.placement_epoch
        );
    }
    if header.payload_stored_hash != expectation.payload_hash {
        bail!(
            "CoreStore block shard header payload_stored_hash mismatch: expected {}, got {}",
            expectation.payload_hash,
            header.payload_stored_hash
        );
    }
    if header.payload_plain_hash != expectation.payload_hash {
        bail!(
            "CoreStore block shard header payload_plain_hash mismatch: expected {}, got {}",
            expectation.payload_hash,
            header.payload_plain_hash
        );
    }
    validate_boundary_summary_fields(&header.boundary_summary_hash, &header.boundary_values_b64)?;
    if let Some(expected) = expectation.boundary_summary_hash
        && header.boundary_summary_hash != expected
    {
        bail!(
            "CoreStore block shard header boundary_summary_hash mismatch: expected {}, got {}",
            expected,
            header.boundary_summary_hash
        );
    }
    if let Some(expected) = expectation.boundary_values_b64
        && header.boundary_values_b64 != expected
    {
        bail!("CoreStore block shard header boundary_values_b64 mismatch");
    }
    if expectation.payload_len > 0 && header.logical_length != expectation.payload_len {
        bail!(
            "CoreStore block shard header logical_length mismatch: expected {}, got {}",
            expectation.payload_len,
            header.logical_length
        );
    }
    Ok(())
}

fn validate_shard_logical_file_id(value: &str) -> Result<()> {
    let Some(hash) = value.strip_prefix("lf_") else {
        bail!("CoreStore block shard logical_file_id must use canonical lf_ prefix");
    };
    if hash.len() != 64 || !hash.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!(
            "CoreStore block shard logical_file_id must be lf_ followed by a 64 byte hex digest: got {value:?} (digest chars {})",
            hash.len()
        );
    }
    Ok(())
}

pub(super) fn boundary_summary_hash(boundary_values: &[CoreBoundaryValue]) -> Result<String> {
    Ok(format!(
        "sha256:{}",
        sha256_hex(&encode_boundary_values_record(boundary_values)?)
    ))
}

pub(super) fn encode_boundary_values_b64(boundary_values: &[CoreBoundaryValue]) -> Result<String> {
    Ok(URL_SAFE_NO_PAD.encode(encode_boundary_values_record(boundary_values)?))
}

pub(super) fn validate_boundary_summary_fields(
    boundary_summary_hash: &str,
    boundary_values_b64: &str,
) -> Result<()> {
    validate_sha256_hash(boundary_summary_hash, "block shard boundary summary hash")?;
    let bytes = if boundary_values_b64.is_empty() {
        Vec::new()
    } else {
        URL_SAFE_NO_PAD
            .decode(boundary_values_b64)
            .context("decode CoreStore block shard boundary values")?
    };
    let decoded = BoundaryValuesProto::decode(bytes.as_slice())?;
    let canonical = decoded.encode_to_vec();
    if canonical != bytes {
        bail!("CoreStore block shard boundary values are not deterministic protobuf");
    }
    let actual = format!("sha256:{}", sha256_hex(&canonical));
    if actual != boundary_summary_hash {
        bail!("CoreStore block shard boundary summary hash mismatch");
    }
    Ok(())
}

pub(super) fn encode_boundary_values_record(
    boundary_values: &[CoreBoundaryValue],
) -> Result<Vec<u8>> {
    let proto = BoundaryValuesProto {
        values: boundary_values
            .iter()
            .map(|value| BoundaryValueProto {
                schema_generation: value.schema_generation,
                name: value.name.clone(),
                value_type: value.value_type.clone(),
                value: value.value.clone(),
                categories: value.categories.clone(),
                source_kind: value.source_kind.clone(),
                required: value.required,
                max_values_per_block: value.max_values_per_block,
                placement_affinity: value.placement_affinity.clone(),
                compaction_scope: value.compaction_scope.clone(),
                shared_ranges_allowed: value.shared_ranges_allowed,
                shared_record_kinds: value.shared_record_kinds.clone(),
            })
            .collect(),
    };
    let bytes = proto.encode_to_vec();
    let decoded = BoundaryValuesProto::decode(bytes.as_slice())?;
    if decoded.encode_to_vec() != bytes {
        bail!("CoreStore block shard boundary values are not deterministic protobuf");
    }
    Ok(bytes)
}

fn descriptor_hash(parts: &[&str]) -> String {
    let mut hasher = Sha256::new();
    for part in parts {
        hasher.update((part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn validate_sha256_hash(hash: &str, label: &str) -> Result<()> {
    let Some(value) = hash.strip_prefix("sha256:") else {
        bail!("CoreStore {label} must have sha256: prefix");
    };
    if value.len() != 64 || !value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        bail!("CoreStore {label} must be a sha256 hash");
    }
    Ok(())
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
