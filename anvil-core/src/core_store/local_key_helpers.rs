use super::*;
use crate::core_store::{
    CoreMetaRowCommonProto, CoreMetaVisibilityState, core_meta_committed_row_common,
    core_meta_root_key_hash, decode_deterministic_proto, encode_deterministic_proto,
};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct NodeSigningKeypairRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(bytes, tag = "2")]
    keypair_protobuf: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct NodeReceiptSigningPublicKeyRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    node_id: String,
    #[prost(bytes, tag = "3")]
    public_key_protobuf: Vec<u8>,
    #[prost(string, tag = "4")]
    public_key_hash: String,
    #[prost(uint64, tag = "5")]
    updated_at_unix_nanos: u64,
}

pub(super) fn admission_record_key(sequence: u64) -> Vec<u8> {
    meta_tuple_key(&[b"admission-record", &sequence.to_be_bytes()])
}

pub(super) fn admission_record_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"admission-record"])
}

pub(super) fn admission_certificate_key(sequence: u64) -> Vec<u8> {
    meta_tuple_key(&[b"admission-certificate", &sequence.to_be_bytes()])
}

pub(super) fn admission_finalisation_key(key: &CorePendingMutationKey) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-finalisation",
        key.node_id.as_bytes(),
        &key.mutation_epoch.to_be_bytes(),
        &key.mutation_sequence.to_be_bytes(),
    ])
}

pub(super) fn admission_finalisation_record_key(key: &CorePendingMutationKey) -> Vec<u8> {
    meta_tuple_key(&[
        b"admission-finalisation-record",
        key.node_id.as_bytes(),
        &key.mutation_epoch.to_be_bytes(),
        &key.mutation_sequence.to_be_bytes(),
    ])
}

pub(super) fn admission_finalisation_prefix() -> Vec<u8> {
    meta_tuple_key(&[b"admission-finalisation"])
}

pub(super) fn admission_sequence_key() -> Vec<u8> {
    meta_tuple_key(&[b"admission-sequence"])
}

pub(super) fn object_manifest_meta_key(object_ref: &CoreObjectRef) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-manifest",
        object_ref.manifest_ref.as_bytes(),
        object_ref.encoding.block_id.as_bytes(),
    ])
}

pub(super) fn inline_payload_meta_key(object_ref: &CoreObjectRef) -> Vec<u8> {
    meta_tuple_key(&[
        b"inline-payload",
        object_ref.hash.as_bytes(),
        &object_ref.logical_size.to_be_bytes(),
        object_ref.manifest_ref.as_bytes(),
    ])
}

pub(super) fn is_inline_object_ref(object_ref: &CoreObjectRef) -> bool {
    object_ref.encoding.profile_id == LOCAL_INLINE_PAYLOAD_PROFILE_ID
        && object_ref.encoding.placement_scope == "coremeta-inline"
}

pub(super) fn local_inline_payload_block_id(hash_hex: &str) -> String {
    format!("{LOCAL_INLINE_PAYLOAD_BLOCK_PREFIX}:{hash_hex}")
}

pub(super) fn root_cache_key(root_anchor_key: &str) -> Vec<u8> {
    meta_tuple_key(&[b"root-anchor", root_anchor_key.as_bytes()])
}

pub(super) fn root_anchor_generation_key(root_key_hash: &str, generation: u64) -> Vec<u8> {
    meta_tuple_key(&[
        b"root-anchor-generation",
        root_key_hash.as_bytes(),
        &generation.to_be_bytes(),
    ])
}

pub(super) fn root_anchor_generation_prefix(root_key_hash: &str) -> Vec<u8> {
    meta_tuple_key(&[b"root-anchor-generation", root_key_hash.as_bytes()])
}

pub(super) fn stream_head_key(stream_id: &str) -> Vec<u8> {
    meta_tuple_key(&[b"stream-head", stream_id.as_bytes()])
}

pub(super) fn stream_record_key(stream_id: &str, sequence: u64) -> Vec<u8> {
    meta_tuple_key(&[
        b"stream-record",
        stream_id.as_bytes(),
        &sequence.to_be_bytes(),
    ])
}

pub(super) fn stream_record_prefix(stream_id: &str) -> Vec<u8> {
    meta_tuple_key(&[b"stream-record", stream_id.as_bytes()])
}

pub(super) fn node_signing_keypair_key() -> Vec<u8> {
    meta_tuple_key(&[b"node-signing-keypair"])
}

pub(super) fn node_receipt_signing_public_key_key(node_id: &str) -> Vec<u8> {
    meta_tuple_key(&[b"node-receipt-signing-public-key", node_id.as_bytes()])
}

pub(super) fn load_or_create_node_signing_keypair(
    meta: &CoreMetaStore,
) -> Result<identity::Keypair> {
    let key = node_signing_keypair_key();
    if let Some(bytes) = meta.get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &key)? {
        let row = decode_node_signing_keypair_row(&bytes)?;
        return identity::Keypair::from_protobuf_encoding(&row.keypair_protobuf)
            .context("CoreStore node signing keypair metadata is not a valid libp2p keypair");
    }

    let keypair = identity::Keypair::generate_ed25519();
    let row = NodeSigningKeypairRowProto {
        common: Some(core_meta_committed_row_common(
            "system/corestore",
            core_meta_root_key_hash("node-signing-keypair"),
            1,
            "node-signing-keypair",
            u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default(),
        )),
        keypair_protobuf: keypair.to_protobuf_encoding()?,
    };
    let bytes = encode_deterministic_proto(&row);
    meta.write_local_committed_batch(&[CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_NODE_SIGNING_KEYPAIR_ROW,
        tuple_key: &key,
        common: row.common.clone(),
        kind: CoreMetaBatchOpKind::Put(&bytes),
    }])?;
    Ok(keypair)
}

pub(super) fn node_receipt_signing_public_key_hash(public_key_protobuf: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"anvil.corestore.node_receipt_signing_public_key.v1");
    hasher.update([0]);
    hasher.update(public_key_protobuf);
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

pub(super) fn store_node_receipt_signing_public_key(
    meta: &CoreMetaStore,
    node_id: &str,
    public_key_protobuf: &[u8],
) -> Result<String> {
    validate_logical_id(node_id, "node receipt signing public key node id")?;
    let public_key = identity::PublicKey::try_decode_protobuf(public_key_protobuf)
        .context("node receipt signing public key protobuf is invalid")?;
    let canonical_public_key = public_key.encode_protobuf();
    if canonical_public_key != public_key_protobuf {
        bail!("node receipt signing public key protobuf is not canonical");
    }
    let public_key_hash = node_receipt_signing_public_key_hash(public_key_protobuf);
    let updated_at_unix_nanos =
        u64::try_from(Utc::now().timestamp_nanos_opt().unwrap_or_default()).unwrap_or_default();
    let row = NodeReceiptSigningPublicKeyRowProto {
        common: Some(core_meta_committed_row_common(
            "system/corestore",
            core_meta_root_key_hash(&format!("node-receipt-signing-public-key/{node_id}")),
            1,
            node_id.to_string(),
            updated_at_unix_nanos,
        )),
        node_id: node_id.to_string(),
        public_key_protobuf: public_key_protobuf.to_vec(),
        public_key_hash: public_key_hash.clone(),
        updated_at_unix_nanos,
    };
    let key = node_receipt_signing_public_key_key(node_id);
    let bytes = encode_deterministic_proto(&row);
    meta.write_local_committed_batch(&[CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_NODE_SIGNING_KEYPAIR_ROW,
        tuple_key: &key,
        common: row.common.clone(),
        kind: CoreMetaBatchOpKind::Put(&bytes),
    }])?;
    Ok(public_key_hash)
}

pub(super) fn load_node_receipt_signing_public_key(
    meta: &CoreMetaStore,
    node_id: &str,
) -> Result<Option<identity::PublicKey>> {
    validate_logical_id(node_id, "node receipt signing public key node id")?;
    let Some(bytes) = meta.get(
        CF_MESH,
        TABLE_NODE_SIGNING_KEYPAIR_ROW,
        &node_receipt_signing_public_key_key(node_id),
    )?
    else {
        return Ok(None);
    };
    let row = decode_node_receipt_signing_public_key_row(node_id, &bytes)?;
    Ok(Some(
        identity::PublicKey::try_decode_protobuf(&row.public_key_protobuf)
            .context("stored node receipt signing public key protobuf is invalid")?,
    ))
}

fn decode_node_signing_keypair_row(bytes: &[u8]) -> Result<NodeSigningKeypairRowProto> {
    let row = decode_deterministic_proto::<NodeSigningKeypairRowProto>(
        bytes,
        "node signing keypair CoreMeta row",
    )?;
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("node signing keypair row missing CoreMeta common"))?;
    if common.realm_id != "system/corestore" {
        bail!("node signing keypair row realm mismatch");
    }
    if common.root_key_hash != core_meta_root_key_hash("node-signing-keypair") {
        bail!("node signing keypair row root mismatch");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("node signing keypair row is not committed");
    }
    if row.keypair_protobuf.is_empty() {
        bail!("node signing keypair row payload is empty");
    }
    Ok(row)
}

fn decode_node_receipt_signing_public_key_row(
    expected_node_id: &str,
    bytes: &[u8],
) -> Result<NodeReceiptSigningPublicKeyRowProto> {
    let row = decode_deterministic_proto::<NodeReceiptSigningPublicKeyRowProto>(
        bytes,
        "node receipt signing public key CoreMeta row",
    )?;
    let common = row
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("node receipt signing public key row missing CoreMeta common"))?;
    if common.realm_id != "system/corestore" {
        bail!("node receipt signing public key row realm mismatch");
    }
    if common.root_key_hash
        != core_meta_root_key_hash(&format!(
            "node-receipt-signing-public-key/{expected_node_id}"
        ))
    {
        bail!("node receipt signing public key row root mismatch");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("node receipt signing public key row is not committed");
    }
    if row.node_id != expected_node_id {
        bail!("node receipt signing public key row node id mismatch");
    }
    if row.public_key_protobuf.is_empty() {
        bail!("node receipt signing public key row payload is empty");
    }
    if row.public_key_hash != node_receipt_signing_public_key_hash(&row.public_key_protobuf) {
        bail!("node receipt signing public key row hash mismatch");
    }
    Ok(row)
}

pub(super) fn meta_tuple_key(parts: &[&[u8]]) -> Vec<u8> {
    let part_count =
        u16::try_from(parts.len()).expect("CoreStore metadata tuple exceeds u16 parts");
    let mut key = Vec::new();
    key.extend_from_slice(&part_count.to_le_bytes());
    for part in parts {
        push_meta_tuple_raw_bytes(&mut key, part);
    }
    key
}

pub(super) fn meta_tuple_utf8(parts: &[&str]) -> Vec<u8> {
    let part_count =
        u16::try_from(parts.len()).expect("CoreStore metadata tuple exceeds u16 parts");
    let mut key = Vec::new();
    key.extend_from_slice(&part_count.to_le_bytes());
    for part in parts {
        push_meta_tuple_part(&mut key, 0x01, part.as_bytes());
    }
    key
}

pub(super) fn meta_tuple_u64(parts: &[u64]) -> Vec<u8> {
    let part_count =
        u16::try_from(parts.len()).expect("CoreStore metadata tuple exceeds u16 parts");
    let mut key = Vec::new();
    key.extend_from_slice(&part_count.to_le_bytes());
    for part in parts {
        push_meta_tuple_part(&mut key, 0x02, &part.to_be_bytes());
    }
    key
}

fn push_meta_tuple_raw_bytes(key: &mut Vec<u8>, part: &[u8]) {
    push_meta_tuple_part(key, 0x05, part);
}

fn push_meta_tuple_part(key: &mut Vec<u8>, kind: u8, part: &[u8]) {
    let len = u16::try_from(part.len()).expect("CoreStore metadata tuple field exceeds u16");
    key.push(kind);
    key.push(0);
    key.extend_from_slice(&len.to_le_bytes());
    key.extend_from_slice(part);
}

pub(super) fn decode_u64_le(bytes: &[u8], label: &str) -> Result<u64> {
    let array: [u8; 8] = bytes
        .try_into()
        .map_err(|_| anyhow!("CoreStore {label} must be 8 bytes"))?;
    Ok(u64::from_le_bytes(array))
}

pub(super) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

pub(super) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}
