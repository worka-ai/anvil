use anyhow::{Result, anyhow, bail};
use prost::Message;

use super::local::{CoreGenesisBundle, CoreGenesisPartition, CoreRootAnchorRecord};
use super::types::{
    CoreBlockLocator, CoreCompressionDescriptor, CoreEncryptionDescriptor, CoreManifestLocator,
    CoreManifestRef, CoreShardReceiptSummary,
};

fn encode_det<M: Message>(message: &M) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(message.encoded_len());
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_det<M: Message>(message: &M, bytes: &[u8], label: &str) -> Result<()> {
    if encode_det(message)? != bytes {
        bail!("CoreStore {label} is not deterministic protobuf");
    }
    Ok(())
}

#[derive(Clone, PartialEq, Message)]
struct RootAnchorHeaderProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    root_anchor_key: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    root_generation: u64,
    #[prost(string, tag = "5")]
    previous_root_hash: String,
    #[prost(message, optional, tag = "6")]
    transaction_manifest: Option<CoreManifestLocatorProto>,
    #[prost(message, optional, tag = "7")]
    checkpoint_manifest: Option<CoreManifestLocatorProto>,
    #[prost(string, tag = "8")]
    publisher_node_id: String,
    #[prost(uint64, tag = "9")]
    publisher_epoch: u64,
    #[prost(uint64, tag = "10")]
    partition_owner_fence: u64,
    #[prost(uint64, tag = "11")]
    created_at_unix_nanos: u64,
    #[prost(string, optional, tag = "12")]
    core_meta_commit_certificate_hash: Option<String>,
    #[prost(string, repeated, tag = "13")]
    certificate_persist_receipt_hashes: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
struct RootAnchorBodyProto {
    #[prost(string, tag = "1")]
    root_state: String,
    #[prost(string, optional, tag = "2")]
    mutation_first: Option<String>,
    #[prost(string, optional, tag = "3")]
    mutation_last: Option<String>,
    #[prost(string, repeated, tag = "4")]
    writer_families: Vec<String>,
    #[prost(uint64, tag = "5")]
    manifest_count: u64,
    #[prost(uint64, tag = "6")]
    final_block_count: u64,
    #[prost(message, optional, tag = "7")]
    genesis_bundle: Option<CoreGenesisBundleProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreGenesisBundleProto {
    #[prost(string, tag = "1")]
    schema: String,
    #[prost(string, tag = "2")]
    genesis_config_hash: String,
    #[prost(bytes, tag = "3")]
    mesh_control_segment: Vec<u8>,
    #[prost(bytes, tag = "4")]
    authz_reserved_schema_segment: Vec<u8>,
    #[prost(string, repeated, tag = "5")]
    initial_root_keys: Vec<String>,
    #[prost(message, repeated, tag = "6")]
    initial_partition_map: Vec<CoreGenesisPartitionProto>,
    #[prost(uint64, tag = "7")]
    created_at_unix_nanos: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreGenesisPartitionProto {
    #[prost(string, tag = "1")]
    root_anchor_key: String,
    #[prost(uint64, tag = "2")]
    root_partition_id: u64,
    #[prost(string, tag = "3")]
    owner_node_id: String,
    #[prost(uint64, tag = "4")]
    owner_epoch: u64,
    #[prost(uint64, tag = "5")]
    owner_fence: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreManifestLocatorProto {
    #[prost(message, optional, tag = "1")]
    manifest_ref: Option<CoreManifestRefProto>,
    #[prost(string, tag = "2")]
    manifest_encoding: String,
    #[prost(uint64, tag = "3")]
    manifest_length: u64,
    #[prost(string, tag = "4")]
    manifest_hash: String,
    #[prost(message, repeated, tag = "5")]
    block_locators: Vec<CoreBlockLocatorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreManifestRefProto {
    #[prost(string, tag = "1")]
    logical_file_id: String,
    #[prost(string, tag = "2")]
    writer_family: String,
    #[prost(uint64, tag = "3")]
    writer_generation: u64,
    #[prost(string, tag = "4")]
    manifest_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreBlockLocatorProto {
    #[prost(uint64, tag = "1")]
    logical_start: u64,
    #[prost(uint64, tag = "2")]
    logical_end: u64,
    #[prost(string, tag = "3")]
    block_id: String,
    #[prost(string, tag = "4")]
    codec_id: String,
    #[prost(uint32, tag = "5")]
    data_shards: u32,
    #[prost(uint32, tag = "6")]
    parity_shards: u32,
    #[prost(uint64, tag = "7")]
    plaintext_block_len: u64,
    #[prost(uint64, tag = "8")]
    shard_payload_len: u64,
    #[prost(uint64, tag = "9")]
    padding_len: u64,
    #[prost(string, tag = "10")]
    block_plain_hash: String,
    #[prost(string, tag = "11")]
    block_encoded_hash: String,
    #[prost(message, optional, tag = "12")]
    compression: Option<CoreCompressionDescriptorProto>,
    #[prost(message, optional, tag = "13")]
    encryption: Option<CoreEncryptionDescriptorProto>,
    #[prost(string, tag = "14")]
    erasure_profile_id: String,
    #[prost(uint64, tag = "15")]
    placement_epoch: u64,
    #[prost(message, repeated, tag = "16")]
    shard_receipts: Vec<CoreShardReceiptSummaryProto>,
    #[prost(string, tag = "17")]
    boundary_summary_hash: String,
    #[prost(string, tag = "18")]
    boundary_values_b64: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreShardReceiptSummaryProto {
    #[prost(string, tag = "1")]
    node_id: String,
    #[prost(string, tag = "2")]
    region_id: String,
    #[prost(string, tag = "3")]
    cell_id: String,
    #[prost(uint32, tag = "4")]
    shard_index: u32,
    #[prost(string, tag = "5")]
    shard_hash: String,
    #[prost(uint64, tag = "6")]
    shard_length: u64,
    #[prost(uint64, tag = "7")]
    fsync_sequence: u64,
    #[prost(uint64, tag = "8")]
    written_at_unix_nanos: u64,
    #[prost(string, tag = "9")]
    signed_payload_hash: String,
    #[prost(string, tag = "10")]
    signature_algorithm: String,
    #[prost(bytes, tag = "11")]
    receipt_signature: Vec<u8>,
    #[prost(string, tag = "12")]
    boundary_summary_hash: String,
    #[prost(string, tag = "13")]
    boundary_values_b64: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreCompressionDescriptorProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(uint32, tag = "2")]
    level: u32,
    #[prost(uint64, tag = "3")]
    uncompressed_length: u64,
    #[prost(uint64, tag = "4")]
    compressed_length: u64,
    #[prost(string, tag = "5")]
    dictionary_id: String,
    #[prost(string, tag = "6")]
    descriptor_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreEncryptionDescriptorProto {
    #[prost(string, tag = "1")]
    algorithm: String,
    #[prost(string, tag = "2")]
    key_id: String,
    #[prost(bytes, tag = "3")]
    nonce: Vec<u8>,
    #[prost(string, tag = "4")]
    aad_hash: String,
    #[prost(string, tag = "5")]
    plaintext_hash: String,
    #[prost(string, tag = "6")]
    ciphertext_hash: String,
    #[prost(string, tag = "7")]
    descriptor_hash: String,
}

pub(super) fn encode_root_anchor_proto(
    anchor: &CoreRootAnchorRecord,
) -> Result<(Vec<u8>, Vec<u8>)> {
    let header = RootAnchorHeaderProto {
        schema: anchor.schema.clone(),
        root_anchor_key: anchor.root_anchor_key.clone(),
        root_key_hash: anchor.root_key_hash.clone(),
        root_generation: anchor.root_generation,
        previous_root_hash: anchor.previous_root_hash.clone(),
        transaction_manifest: anchor
            .transaction_manifest
            .as_ref()
            .map(manifest_locator_to_proto),
        checkpoint_manifest: anchor
            .checkpoint_manifest
            .as_ref()
            .map(manifest_locator_to_proto),
        publisher_node_id: anchor.publisher_node_id.clone(),
        publisher_epoch: anchor.publisher_epoch,
        partition_owner_fence: anchor.partition_owner_fence,
        created_at_unix_nanos: anchor.created_at_unix_nanos,
        core_meta_commit_certificate_hash: anchor.core_meta_commit_certificate_hash.clone(),
        certificate_persist_receipt_hashes: anchor.certificate_persist_receipt_hashes.clone(),
    };
    let body = RootAnchorBodyProto {
        root_state: anchor.root_state.clone(),
        mutation_first: anchor.mutation_first.clone(),
        mutation_last: anchor.mutation_last.clone(),
        writer_families: anchor.writer_families.clone(),
        manifest_count: anchor.manifest_count,
        final_block_count: anchor.final_block_count,
        genesis_bundle: anchor
            .genesis_bundle
            .as_ref()
            .map(genesis_bundle_to_proto)
            .transpose()?,
    };
    Ok((encode_det(&header)?, encode_det(&body)?))
}

pub(super) fn decode_root_anchor_proto(
    header_bytes: &[u8],
    body_bytes: &[u8],
) -> Result<CoreRootAnchorRecord> {
    let header = RootAnchorHeaderProto::decode(header_bytes)?;
    ensure_det(&header, header_bytes, "root anchor header")?;
    let body = RootAnchorBodyProto::decode(body_bytes)?;
    ensure_det(&body, body_bytes, "root anchor body")?;
    Ok(CoreRootAnchorRecord {
        schema: header.schema,
        root_anchor_key: header.root_anchor_key,
        root_key_hash: header.root_key_hash,
        root_generation: header.root_generation,
        previous_root_hash: header.previous_root_hash,
        transaction_manifest: header
            .transaction_manifest
            .map(manifest_locator_from_proto)
            .transpose()?,
        checkpoint_manifest: header
            .checkpoint_manifest
            .map(manifest_locator_from_proto)
            .transpose()?,
        publisher_node_id: header.publisher_node_id,
        publisher_epoch: header.publisher_epoch,
        partition_owner_fence: header.partition_owner_fence,
        created_at_unix_nanos: header.created_at_unix_nanos,
        core_meta_commit_certificate_hash: header.core_meta_commit_certificate_hash,
        certificate_persist_receipt_hashes: header.certificate_persist_receipt_hashes,
        root_state: body.root_state,
        mutation_first: body.mutation_first,
        mutation_last: body.mutation_last,
        writer_families: body.writer_families,
        manifest_count: body.manifest_count,
        final_block_count: body.final_block_count,
        genesis_bundle: body
            .genesis_bundle
            .map(genesis_bundle_from_proto)
            .transpose()?,
    })
}

fn manifest_locator_to_proto(value: &CoreManifestLocator) -> CoreManifestLocatorProto {
    CoreManifestLocatorProto {
        manifest_ref: Some(CoreManifestRefProto {
            logical_file_id: value.manifest_ref.logical_file_id.clone(),
            writer_family: value.manifest_ref.writer_family.clone(),
            writer_generation: value.manifest_ref.writer_generation,
            manifest_hash: value.manifest_ref.manifest_hash.clone(),
        }),
        manifest_encoding: value.manifest_encoding.clone(),
        manifest_length: value.manifest_length,
        manifest_hash: value.manifest_hash.clone(),
        block_locators: value
            .block_locators
            .iter()
            .map(block_locator_to_proto)
            .collect(),
    }
}

fn manifest_locator_from_proto(value: CoreManifestLocatorProto) -> Result<CoreManifestLocator> {
    let manifest_ref = value
        .manifest_ref
        .ok_or_else(|| anyhow!("CoreStore manifest locator is missing manifest_ref"))?;
    Ok(CoreManifestLocator {
        manifest_ref: CoreManifestRef {
            logical_file_id: manifest_ref.logical_file_id,
            writer_family: manifest_ref.writer_family,
            writer_generation: manifest_ref.writer_generation,
            manifest_hash: manifest_ref.manifest_hash,
        },
        manifest_encoding: value.manifest_encoding,
        manifest_length: value.manifest_length,
        manifest_hash: value.manifest_hash,
        block_locators: value
            .block_locators
            .into_iter()
            .map(block_locator_from_proto)
            .collect::<Result<Vec<_>>>()?,
    })
}

fn block_locator_to_proto(value: &CoreBlockLocator) -> CoreBlockLocatorProto {
    CoreBlockLocatorProto {
        logical_start: value.logical_start,
        logical_end: value.logical_end,
        block_id: value.block_id.clone(),
        codec_id: value.codec_id.clone(),
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
        plaintext_block_len: value.plaintext_block_len,
        shard_payload_len: value.shard_payload_len,
        padding_len: value.padding_len,
        block_plain_hash: value.block_plain_hash.clone(),
        block_encoded_hash: value.block_encoded_hash.clone(),
        compression: Some(compression_descriptor_to_proto(&value.compression)),
        encryption: Some(encryption_descriptor_to_proto(&value.encryption)),
        erasure_profile_id: value.erasure_profile_id.clone(),
        placement_epoch: value.placement_epoch,
        boundary_summary_hash: value.boundary_summary_hash.clone(),
        boundary_values_b64: value.boundary_values_b64.clone(),
        shard_receipts: value
            .shard_receipts
            .iter()
            .map(shard_receipt_to_proto)
            .collect(),
    }
}

fn block_locator_from_proto(value: CoreBlockLocatorProto) -> Result<CoreBlockLocator> {
    Ok(CoreBlockLocator {
        logical_start: value.logical_start,
        logical_end: value.logical_end,
        block_id: value.block_id,
        codec_id: value.codec_id,
        data_shards: value.data_shards,
        parity_shards: value.parity_shards,
        plaintext_block_len: value.plaintext_block_len,
        shard_payload_len: value.shard_payload_len,
        padding_len: value.padding_len,
        block_plain_hash: value.block_plain_hash,
        block_encoded_hash: value.block_encoded_hash,
        compression: compression_descriptor_from_proto(
            value.compression.ok_or_else(|| {
                anyhow!("CoreStore manifest block locator is missing compression")
            })?,
        ),
        encryption: encryption_descriptor_from_proto(
            value
                .encryption
                .ok_or_else(|| anyhow!("CoreStore manifest block locator is missing encryption"))?,
        ),
        erasure_profile_id: value.erasure_profile_id,
        placement_epoch: value.placement_epoch,
        boundary_summary_hash: value.boundary_summary_hash,
        boundary_values_b64: value.boundary_values_b64,
        shard_receipts: value
            .shard_receipts
            .into_iter()
            .map(shard_receipt_from_proto)
            .collect(),
    })
}

fn shard_receipt_to_proto(value: &CoreShardReceiptSummary) -> CoreShardReceiptSummaryProto {
    CoreShardReceiptSummaryProto {
        node_id: value.node_id.clone(),
        region_id: value.region_id.clone(),
        cell_id: value.cell_id.clone(),
        shard_index: value.shard_index,
        shard_hash: value.shard_hash.clone(),
        shard_length: value.shard_length,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash.clone(),
        signature_algorithm: value.signature_algorithm.clone(),
        receipt_signature: value.receipt_signature.clone(),
        boundary_summary_hash: value.boundary_summary_hash.clone(),
        boundary_values_b64: value.boundary_values_b64.clone(),
    }
}

fn shard_receipt_from_proto(value: CoreShardReceiptSummaryProto) -> CoreShardReceiptSummary {
    CoreShardReceiptSummary {
        node_id: value.node_id,
        region_id: value.region_id,
        cell_id: value.cell_id,
        shard_index: value.shard_index,
        shard_hash: value.shard_hash,
        shard_length: value.shard_length,
        fsync_sequence: value.fsync_sequence,
        written_at_unix_nanos: value.written_at_unix_nanos,
        signed_payload_hash: value.signed_payload_hash,
        signature_algorithm: value.signature_algorithm,
        receipt_signature: value.receipt_signature,
        boundary_summary_hash: value.boundary_summary_hash,
        boundary_values_b64: value.boundary_values_b64,
    }
}

fn compression_descriptor_to_proto(
    value: &CoreCompressionDescriptor,
) -> CoreCompressionDescriptorProto {
    CoreCompressionDescriptorProto {
        algorithm: value.algorithm.clone(),
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn compression_descriptor_from_proto(
    value: CoreCompressionDescriptorProto,
) -> CoreCompressionDescriptor {
    CoreCompressionDescriptor {
        algorithm: value.algorithm,
        level: value.level,
        uncompressed_length: value.uncompressed_length,
        compressed_length: value.compressed_length,
        dictionary_id: value.dictionary_id,
        descriptor_hash: value.descriptor_hash,
    }
}

fn encryption_descriptor_to_proto(
    value: &CoreEncryptionDescriptor,
) -> CoreEncryptionDescriptorProto {
    CoreEncryptionDescriptorProto {
        algorithm: value.algorithm.clone(),
        key_id: value.key_id.clone(),
        nonce: value.nonce.clone(),
        aad_hash: value.aad_hash.clone(),
        plaintext_hash: value.plaintext_hash.clone(),
        ciphertext_hash: value.ciphertext_hash.clone(),
        descriptor_hash: value.descriptor_hash.clone(),
    }
}

fn encryption_descriptor_from_proto(
    value: CoreEncryptionDescriptorProto,
) -> CoreEncryptionDescriptor {
    CoreEncryptionDescriptor {
        algorithm: value.algorithm,
        key_id: value.key_id,
        nonce: value.nonce,
        aad_hash: value.aad_hash,
        plaintext_hash: value.plaintext_hash,
        ciphertext_hash: value.ciphertext_hash,
        descriptor_hash: value.descriptor_hash,
    }
}

fn genesis_bundle_to_proto(value: &CoreGenesisBundle) -> Result<CoreGenesisBundleProto> {
    Ok(CoreGenesisBundleProto {
        schema: value.schema.clone(),
        genesis_config_hash: value.genesis_config_hash.clone(),
        mesh_control_segment: value.mesh_control_segment.clone(),
        authz_reserved_schema_segment: value.authz_reserved_schema_segment.clone(),
        initial_root_keys: value.initial_root_keys.clone(),
        initial_partition_map: value
            .initial_partition_map
            .iter()
            .map(genesis_partition_to_proto)
            .collect(),
        created_at_unix_nanos: value.created_at_unix_nanos,
    })
}

fn genesis_bundle_from_proto(value: CoreGenesisBundleProto) -> Result<CoreGenesisBundle> {
    Ok(CoreGenesisBundle {
        schema: value.schema,
        genesis_config_hash: value.genesis_config_hash,
        mesh_control_segment: value.mesh_control_segment,
        authz_reserved_schema_segment: value.authz_reserved_schema_segment,
        initial_root_keys: value.initial_root_keys,
        initial_partition_map: value
            .initial_partition_map
            .into_iter()
            .map(genesis_partition_from_proto)
            .collect(),
        created_at_unix_nanos: value.created_at_unix_nanos,
    })
}

fn genesis_partition_to_proto(value: &CoreGenesisPartition) -> CoreGenesisPartitionProto {
    CoreGenesisPartitionProto {
        root_anchor_key: value.root_anchor_key.clone(),
        root_partition_id: value.root_partition_id,
        owner_node_id: value.owner_node_id.clone(),
        owner_epoch: value.owner_epoch,
        owner_fence: value.owner_fence,
    }
}

fn genesis_partition_from_proto(value: CoreGenesisPartitionProto) -> CoreGenesisPartition {
    CoreGenesisPartition {
        root_anchor_key: value.root_anchor_key,
        root_partition_id: value.root_partition_id,
        owner_node_id: value.owner_node_id,
        owner_epoch: value.owner_epoch,
        owner_fence: value.owner_fence,
    }
}
