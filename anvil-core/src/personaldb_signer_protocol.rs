use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    personaldb_control::{
        PersonalDbCommitCertificate, PersonalDbGroupManifest, PersonalDbSnapshotManifest,
        validate_commit_certificate_unsigned, validate_group_manifest_unsigned,
        validate_snapshot_manifest_unsigned,
    },
    personaldb_heads::{PersonalDbCommittedHead, validate_committed_head_unsigned},
    personaldb_signing::{signature_envelope_from_proto, signature_envelope_to_proto},
};
use anyhow::{Context, Result, anyhow, bail};
use personaldb_protocol::{
    ProtocolSignable, SignatureEnvelopeV1, SignatureMetadata, SigningPayload,
};
use prost::Message;
use std::io::{Error as IoError, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const PERSONALDB_SIGNER_PROTOCOL_VERSION: u32 = 1;
pub const MAX_PERSONALDB_SIGNER_FRAME_BYTES: usize = 64 * 1024;
const MAX_PERSONALDB_SIGNER_STRING_BYTES: usize = 4 * 1024;
const MAX_PERSONALDB_SIGNER_ERROR_CODE_BYTES: usize = 64;
const MAX_PERSONALDB_SIGNER_ERROR_MESSAGE_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbSigningObject {
    GroupManifest(PersonalDbGroupManifest),
    SnapshotManifest(PersonalDbSnapshotManifest),
    CommitCertificate(PersonalDbCommitCertificate),
    CommittedHead(PersonalDbCommittedHead),
}

impl PersonalDbSigningObject {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::GroupManifest(manifest) => validate_group_manifest_unsigned(manifest),
            Self::SnapshotManifest(manifest) => validate_snapshot_manifest_unsigned(manifest),
            Self::CommitCertificate(certificate) => {
                validate_commit_certificate_unsigned(certificate)
            }
            Self::CommittedHead(head) => validate_committed_head_unsigned(head),
        }?;
        self.validate_string_bounds()
    }

    pub fn metadata(&self) -> SignatureMetadata {
        self.signature_metadata()
    }

    fn validate_string_bounds(&self) -> Result<()> {
        let fields: &[(&str, &str)] = match self {
            Self::GroupManifest(manifest) => &[
                ("tenant_id", &manifest.tenant_id),
                ("database_id", &manifest.database_id),
                ("schema_hash", &manifest.schema_hash),
                ("genesis_hash", &manifest.genesis_hash),
                ("created_at", &manifest.created_at),
                ("created_by", &manifest.created_by),
                ("consistency_policy", &manifest.consistency_policy),
            ],
            Self::SnapshotManifest(manifest) => &[
                ("tenant_id", &manifest.tenant_id),
                ("database_id", &manifest.database_id),
                ("log_hash", &manifest.log_hash),
                ("state_hash", &manifest.state_hash),
                ("schema_hash", &manifest.schema_hash),
                ("snapshot_object_key", &manifest.snapshot_object_key),
                ("snapshot_object_hash", &manifest.snapshot_object_hash),
                ("created_at", &manifest.created_at),
                ("created_by_node", &manifest.created_by_node),
            ],
            Self::CommitCertificate(certificate) => &[
                ("tenant_id", &certificate.tenant_id),
                ("database_id", &certificate.database_id),
                ("previous_log_hash", &certificate.previous_log_hash),
                ("entry_hash", &certificate.entry_hash),
                (
                    "changeset_payload_hash",
                    &certificate.changeset_payload_hash,
                ),
                (
                    "verified_envelope_hash",
                    &certificate.verified_envelope_hash,
                ),
                ("leader_replica_id", &certificate.leader_replica_id),
                ("voter_acks_hash", &certificate.voter_acks_hash),
                ("witness_node_id", &certificate.witness_node_id),
                ("witnessed_at", &certificate.witnessed_at),
            ],
            Self::CommittedHead(head) => &[
                ("tenant_id", &head.tenant_id),
                ("database_id", &head.database_id),
                ("log_hash", &head.log_hash),
                ("segment_ref", &head.segment_ref),
                ("schema_hash", &head.schema_hash),
                ("updated_at", &head.updated_at),
                ("updated_by_node", &head.updated_by_node),
            ],
        };
        for (name, value) in fields {
            if value.len() > MAX_PERSONALDB_SIGNER_STRING_BYTES {
                bail!("PersonalDB signer field {name} exceeds the protocol bound");
            }
        }
        Ok(())
    }
}

impl ProtocolSignable for PersonalDbSigningObject {
    fn signature_metadata(&self) -> SignatureMetadata {
        match self {
            Self::GroupManifest(object) => object.signature_metadata(),
            Self::SnapshotManifest(object) => object.signature_metadata(),
            Self::CommitCertificate(object) => object.signature_metadata(),
            Self::CommittedHead(object) => object.signature_metadata(),
        }
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        match self {
            Self::GroupManifest(object) => object.signing_payload(),
            Self::SnapshotManifest(object) => object.signing_payload(),
            Self::CommitCertificate(object) => object.signing_payload(),
            Self::CommittedHead(object) => object.signing_payload(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum PersonalDbSignerErrorCode {
    InvalidRequest = 1,
    WrongPurpose = 2,
    SigningFailed = 3,
    Internal = 4,
}

impl PersonalDbSignerErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid-request",
            Self::WrongPurpose => "wrong-purpose",
            Self::SigningFailed => "signing-failed",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersonalDbSignerResponse {
    Signature(SignatureEnvelopeV1),
    Rejected { code: String, message: String },
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSignerRequestV1 {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(oneof = "personal_db_signer_request_v1::Object", tags = "2, 3, 4, 5")]
    object: Option<personal_db_signer_request_v1::Object>,
}

mod personal_db_signer_request_v1 {
    use super::{CommitCertificateV1, CommittedHeadV1, GroupManifestV1, SnapshotManifestV1};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Object {
        #[prost(message, tag = "2")]
        GroupManifest(GroupManifestV1),
        #[prost(message, tag = "3")]
        SnapshotManifest(SnapshotManifestV1),
        #[prost(message, tag = "4")]
        CommitCertificate(CommitCertificateV1),
        #[prost(message, tag = "5")]
        CommittedHead(CommittedHeadV1),
    }
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSignerResponseV1 {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(oneof = "personal_db_signer_response_v1::Result", tags = "2, 3")]
    result: Option<personal_db_signer_response_v1::Result>,
}

mod personal_db_signer_response_v1 {
    use super::{PersonalDbSignerErrorV1, WireSignatureEnvelopeV1};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Result {
        #[prost(message, tag = "2")]
        Signature(WireSignatureEnvelopeV1),
        #[prost(message, tag = "3")]
        Error(PersonalDbSignerErrorV1),
    }
}

#[derive(Clone, PartialEq, Message)]
struct PersonalDbSignerErrorV1 {
    #[prost(string, tag = "1")]
    code: String,
    #[prost(string, tag = "2")]
    message: String,
}

#[derive(Clone, PartialEq, Message)]
struct GroupManifestV1 {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(string, tag = "4")]
    schema_hash: String,
    #[prost(string, tag = "5")]
    genesis_hash: String,
    #[prost(string, tag = "6")]
    created_at: String,
    #[prost(string, tag = "7")]
    created_by: String,
    #[prost(string, tag = "8")]
    consistency_policy: String,
    #[prost(uint32, tag = "9")]
    object_layout_version: u32,
    #[prost(uint64, tag = "10")]
    active_membership_epoch: u64,
    #[prost(uint64, tag = "11")]
    active_policy_epoch: u64,
    #[prost(uint64, tag = "12")]
    current_row_index_generation: u64,
    #[prost(uint64, tag = "13")]
    current_projection_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct SnapshotManifestV1 {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    log_index: u64,
    #[prost(string, tag = "5")]
    log_hash: String,
    #[prost(string, tag = "6")]
    state_hash: String,
    #[prost(string, tag = "7")]
    schema_hash: String,
    #[prost(string, tag = "8")]
    snapshot_object_key: String,
    #[prost(string, tag = "9")]
    snapshot_object_hash: String,
    #[prost(uint64, tag = "10")]
    source_segment_start: u64,
    #[prost(uint64, tag = "11")]
    source_segment_end: u64,
    #[prost(uint64, tag = "12")]
    row_index_generation: u64,
    #[prost(string, tag = "13")]
    created_at: String,
    #[prost(string, tag = "14")]
    created_by_node: String,
}

#[derive(Clone, PartialEq, Message)]
struct CommitCertificateV1 {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    log_index: u64,
    #[prost(string, tag = "5")]
    previous_log_hash: String,
    #[prost(string, tag = "6")]
    entry_hash: String,
    #[prost(string, tag = "7")]
    changeset_payload_hash: String,
    #[prost(string, tag = "8")]
    verified_envelope_hash: String,
    #[prost(uint64, tag = "9")]
    client_log_epoch: u64,
    #[prost(uint64, tag = "10")]
    membership_epoch: u64,
    #[prost(uint64, tag = "11")]
    policy_epoch: u64,
    #[prost(string, tag = "12")]
    leader_replica_id: String,
    #[prost(string, tag = "13")]
    voter_acks_hash: String,
    #[prost(uint64, tag = "14")]
    authz_revision: u64,
    #[prost(string, tag = "15")]
    witness_node_id: String,
    #[prost(string, tag = "16")]
    witnessed_at: String,
}

#[derive(Clone, PartialEq, Message)]
struct CommittedHeadV1 {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(uint64, tag = "4")]
    log_index: u64,
    #[prost(string, tag = "5")]
    log_hash: String,
    #[prost(string, tag = "6")]
    segment_ref: String,
    #[prost(uint64, tag = "7")]
    row_index_generation: u64,
    #[prost(uint64, tag = "8")]
    policy_epoch: u64,
    #[prost(uint64, tag = "9")]
    membership_epoch: u64,
    #[prost(string, tag = "10")]
    schema_hash: String,
    #[prost(string, tag = "11")]
    updated_at: String,
    #[prost(string, tag = "12")]
    updated_by_node: String,
}

pub fn encode_signer_request(object: &PersonalDbSigningObject) -> Result<Vec<u8>> {
    object.validate()?;
    let request = PersonalDbSignerRequestV1 {
        format_version: PERSONALDB_SIGNER_PROTOCOL_VERSION,
        object: Some(object_to_proto(object)),
    };
    encode_bounded(&request, "PersonalDB signer request")
}

pub fn decode_signer_request(encoded: &[u8]) -> Result<PersonalDbSigningObject> {
    let request = decode_canonical_bounded::<PersonalDbSignerRequestV1>(
        encoded,
        "PersonalDB signer request",
    )?;
    if request.format_version != PERSONALDB_SIGNER_PROTOCOL_VERSION {
        bail!("unsupported PersonalDB signer request version");
    }
    let object = match request
        .object
        .ok_or_else(|| anyhow!("PersonalDB signer request object is missing"))?
    {
        personal_db_signer_request_v1::Object::GroupManifest(proto) => {
            PersonalDbSigningObject::GroupManifest(group_manifest_from_proto(proto)?)
        }
        personal_db_signer_request_v1::Object::SnapshotManifest(proto) => {
            PersonalDbSigningObject::SnapshotManifest(snapshot_manifest_from_proto(proto)?)
        }
        personal_db_signer_request_v1::Object::CommitCertificate(proto) => {
            PersonalDbSigningObject::CommitCertificate(commit_certificate_from_proto(proto)?)
        }
        personal_db_signer_request_v1::Object::CommittedHead(proto) => {
            PersonalDbSigningObject::CommittedHead(committed_head_from_proto(proto)?)
        }
    };
    object.validate()?;
    Ok(object)
}

pub fn encode_signer_success(envelope: &SignatureEnvelopeV1) -> Result<Vec<u8>> {
    envelope.validate_shape()?;
    encode_bounded(
        &PersonalDbSignerResponseV1 {
            format_version: PERSONALDB_SIGNER_PROTOCOL_VERSION,
            result: Some(personal_db_signer_response_v1::Result::Signature(
                signature_envelope_to_proto(envelope),
            )),
        },
        "PersonalDB signer response",
    )
}

pub fn encode_signer_error(
    code: PersonalDbSignerErrorCode,
    message: impl AsRef<str>,
) -> Result<Vec<u8>> {
    let message = message.as_ref();
    if message.len() > MAX_PERSONALDB_SIGNER_ERROR_MESSAGE_BYTES {
        bail!("PersonalDB signer error message exceeds the protocol bound");
    }
    encode_bounded(
        &PersonalDbSignerResponseV1 {
            format_version: PERSONALDB_SIGNER_PROTOCOL_VERSION,
            result: Some(personal_db_signer_response_v1::Result::Error(
                PersonalDbSignerErrorV1 {
                    code: code.as_str().to_string(),
                    message: message.to_string(),
                },
            )),
        },
        "PersonalDB signer response",
    )
}

pub fn decode_signer_response(encoded: &[u8]) -> Result<PersonalDbSignerResponse> {
    let response = decode_canonical_bounded::<PersonalDbSignerResponseV1>(
        encoded,
        "PersonalDB signer response",
    )?;
    if response.format_version != PERSONALDB_SIGNER_PROTOCOL_VERSION {
        bail!("unsupported PersonalDB signer response version");
    }
    match response
        .result
        .ok_or_else(|| anyhow!("PersonalDB signer response result is missing"))?
    {
        personal_db_signer_response_v1::Result::Signature(proto) => {
            Ok(PersonalDbSignerResponse::Signature(
                signature_envelope_from_proto(proto)
                    .context("decode PersonalDB signer response envelope")?,
            ))
        }
        personal_db_signer_response_v1::Result::Error(error) => {
            if error.code.is_empty()
                || error.code.len() > MAX_PERSONALDB_SIGNER_ERROR_CODE_BYTES
                || error.message.len() > MAX_PERSONALDB_SIGNER_ERROR_MESSAGE_BYTES
            {
                bail!("PersonalDB signer error response exceeds the protocol bound");
            }
            Ok(PersonalDbSignerResponse::Rejected {
                code: error.code,
                message: error.message,
            })
        }
    }
}

#[cfg(test)]
pub(crate) fn encode_test_signer_response(envelope: WireSignatureEnvelopeV1) -> Result<Vec<u8>> {
    encode_bounded(
        &PersonalDbSignerResponseV1 {
            format_version: PERSONALDB_SIGNER_PROTOCOL_VERSION,
            result: Some(personal_db_signer_response_v1::Result::Signature(envelope)),
        },
        "PersonalDB signer response",
    )
}

pub async fn read_bounded_frame(reader: &mut (impl AsyncRead + Unpin)) -> std::io::Result<Vec<u8>> {
    let length = reader.read_u32().await? as usize;
    if length == 0 || length > MAX_PERSONALDB_SIGNER_FRAME_BYTES {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            "PersonalDB signer frame length is outside the protocol bound",
        ));
    }
    let mut bytes = vec![0; length];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}

pub async fn write_bounded_frame(
    writer: &mut (impl AsyncWrite + Unpin),
    bytes: &[u8],
) -> std::io::Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_PERSONALDB_SIGNER_FRAME_BYTES {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "PersonalDB signer frame length is outside the protocol bound",
        ));
    }
    writer.write_u32(bytes.len() as u32).await?;
    writer.write_all(bytes).await?;
    writer.flush().await
}

fn encode_bounded(message: &impl Message, name: &'static str) -> Result<Vec<u8>> {
    let encoded = message.encode_to_vec();
    if encoded.is_empty() || encoded.len() > MAX_PERSONALDB_SIGNER_FRAME_BYTES {
        bail!("{name} exceeds the protocol frame bound");
    }
    Ok(encoded)
}

fn decode_canonical_bounded<M>(encoded: &[u8], name: &'static str) -> Result<M>
where
    M: Message + Default,
{
    if encoded.is_empty() || encoded.len() > MAX_PERSONALDB_SIGNER_FRAME_BYTES {
        bail!("{name} is outside the protocol frame bound");
    }
    let message = M::decode(encoded).with_context(|| format!("decode {name} protobuf"))?;
    if message.encode_to_vec() != encoded {
        bail!("{name} protobuf is not canonical");
    }
    Ok(message)
}

fn object_to_proto(object: &PersonalDbSigningObject) -> personal_db_signer_request_v1::Object {
    match object {
        PersonalDbSigningObject::GroupManifest(manifest) => {
            personal_db_signer_request_v1::Object::GroupManifest(GroupManifestV1 {
                format_version: u32::from(manifest.format_version),
                tenant_id: manifest.tenant_id.clone(),
                database_id: manifest.database_id.clone(),
                schema_hash: manifest.schema_hash.clone(),
                genesis_hash: manifest.genesis_hash.clone(),
                created_at: manifest.created_at.clone(),
                created_by: manifest.created_by.clone(),
                consistency_policy: manifest.consistency_policy.clone(),
                object_layout_version: u32::from(manifest.object_layout_version),
                active_membership_epoch: manifest.active_membership_epoch,
                active_policy_epoch: manifest.active_policy_epoch,
                current_row_index_generation: manifest.current_row_index_generation,
                current_projection_generation: manifest.current_projection_generation,
            })
        }
        PersonalDbSigningObject::SnapshotManifest(manifest) => {
            personal_db_signer_request_v1::Object::SnapshotManifest(SnapshotManifestV1 {
                format_version: u32::from(manifest.format_version),
                tenant_id: manifest.tenant_id.clone(),
                database_id: manifest.database_id.clone(),
                log_index: manifest.log_index,
                log_hash: manifest.log_hash.clone(),
                state_hash: manifest.state_hash.clone(),
                schema_hash: manifest.schema_hash.clone(),
                snapshot_object_key: manifest.snapshot_object_key.clone(),
                snapshot_object_hash: manifest.snapshot_object_hash.clone(),
                source_segment_start: manifest.source_segment_start,
                source_segment_end: manifest.source_segment_end,
                row_index_generation: manifest.row_index_generation,
                created_at: manifest.created_at.clone(),
                created_by_node: manifest.created_by_node.clone(),
            })
        }
        PersonalDbSigningObject::CommitCertificate(certificate) => {
            personal_db_signer_request_v1::Object::CommitCertificate(CommitCertificateV1 {
                format_version: u32::from(certificate.format_version),
                tenant_id: certificate.tenant_id.clone(),
                database_id: certificate.database_id.clone(),
                log_index: certificate.log_index,
                previous_log_hash: certificate.previous_log_hash.clone(),
                entry_hash: certificate.entry_hash.clone(),
                changeset_payload_hash: certificate.changeset_payload_hash.clone(),
                verified_envelope_hash: certificate.verified_envelope_hash.clone(),
                client_log_epoch: certificate.client_log_epoch,
                membership_epoch: certificate.membership_epoch,
                policy_epoch: certificate.policy_epoch,
                leader_replica_id: certificate.leader_replica_id.clone(),
                voter_acks_hash: certificate.voter_acks_hash.clone(),
                authz_revision: certificate.authz_revision,
                witness_node_id: certificate.witness_node_id.clone(),
                witnessed_at: certificate.witnessed_at.clone(),
            })
        }
        PersonalDbSigningObject::CommittedHead(head) => {
            personal_db_signer_request_v1::Object::CommittedHead(CommittedHeadV1 {
                format_version: u32::from(head.format_version),
                tenant_id: head.tenant_id.clone(),
                database_id: head.database_id.clone(),
                log_index: head.log_index,
                log_hash: head.log_hash.clone(),
                segment_ref: head.segment_ref.clone(),
                row_index_generation: head.row_index_generation,
                policy_epoch: head.policy_epoch,
                membership_epoch: head.membership_epoch,
                schema_hash: head.schema_hash.clone(),
                updated_at: head.updated_at.clone(),
                updated_by_node: head.updated_by_node.clone(),
            })
        }
    }
}

fn group_manifest_from_proto(proto: GroupManifestV1) -> Result<PersonalDbGroupManifest> {
    Ok(PersonalDbGroupManifest {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("PersonalDB group manifest version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        schema_hash: proto.schema_hash,
        genesis_hash: proto.genesis_hash,
        created_at: proto.created_at,
        created_by: proto.created_by,
        consistency_policy: proto.consistency_policy,
        object_layout_version: u16::try_from(proto.object_layout_version)
            .map_err(|_| anyhow!("PersonalDB group manifest layout version exceeds u16"))?,
        active_membership_epoch: proto.active_membership_epoch,
        active_policy_epoch: proto.active_policy_epoch,
        current_row_index_generation: proto.current_row_index_generation,
        current_projection_generation: proto.current_projection_generation,
        manifest_hash: None,
        manifest_signature: None,
    })
}

fn snapshot_manifest_from_proto(proto: SnapshotManifestV1) -> Result<PersonalDbSnapshotManifest> {
    Ok(PersonalDbSnapshotManifest {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("PersonalDB snapshot manifest version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        log_index: proto.log_index,
        log_hash: proto.log_hash,
        state_hash: proto.state_hash,
        schema_hash: proto.schema_hash,
        snapshot_object_key: proto.snapshot_object_key,
        snapshot_object_hash: proto.snapshot_object_hash,
        source_segment_start: proto.source_segment_start,
        source_segment_end: proto.source_segment_end,
        row_index_generation: proto.row_index_generation,
        created_at: proto.created_at,
        created_by_node: proto.created_by_node,
        manifest_hash: None,
        manifest_signature: None,
    })
}

fn commit_certificate_from_proto(
    proto: CommitCertificateV1,
) -> Result<PersonalDbCommitCertificate> {
    Ok(PersonalDbCommitCertificate {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("PersonalDB commit certificate version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        log_index: proto.log_index,
        previous_log_hash: proto.previous_log_hash,
        entry_hash: proto.entry_hash,
        changeset_payload_hash: proto.changeset_payload_hash,
        verified_envelope_hash: proto.verified_envelope_hash,
        client_log_epoch: proto.client_log_epoch,
        membership_epoch: proto.membership_epoch,
        policy_epoch: proto.policy_epoch,
        leader_replica_id: proto.leader_replica_id,
        voter_acks_hash: proto.voter_acks_hash,
        authz_revision: proto.authz_revision,
        witness_node_id: proto.witness_node_id,
        witnessed_at: proto.witnessed_at,
        certificate_hash: None,
        witness_signature: None,
    })
}

fn committed_head_from_proto(proto: CommittedHeadV1) -> Result<PersonalDbCommittedHead> {
    Ok(PersonalDbCommittedHead {
        format_version: u16::try_from(proto.format_version)
            .map_err(|_| anyhow!("PersonalDB committed head version exceeds u16"))?,
        tenant_id: proto.tenant_id,
        database_id: proto.database_id,
        log_index: proto.log_index,
        log_hash: proto.log_hash,
        segment_ref: proto.segment_ref,
        row_index_generation: proto.row_index_generation,
        policy_epoch: proto.policy_epoch,
        membership_epoch: proto.membership_epoch,
        schema_hash: proto.schema_hash,
        updated_at: proto.updated_at,
        updated_by_node: proto.updated_by_node,
        head_hash: None,
        head_signature: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_request_round_trips_canonically() {
        let object = PersonalDbSigningObject::CommitCertificate(PersonalDbCommitCertificate {
            format_version: 2,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            log_index: 7,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode([2; 32]),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 2,
            policy_epoch: 3,
            leader_replica_id: "leader".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 5,
            witness_node_id: "witness".to_string(),
            witnessed_at: "2026-07-17T00:00:00Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        });

        let encoded = encode_signer_request(&object).unwrap();
        assert_eq!(decode_signer_request(&encoded).unwrap(), object);

        let mut noncanonical = encoded;
        noncanonical.extend([0x98, 0x06, 0x00]);
        assert!(
            decode_signer_request(&noncanonical)
                .unwrap_err()
                .to_string()
                .contains("not canonical")
        );
    }

    #[test]
    fn typed_request_enforces_string_and_frame_bounds() {
        let object = PersonalDbSigningObject::GroupManifest(PersonalDbGroupManifest {
            format_version: 2,
            tenant_id: "a".repeat(MAX_PERSONALDB_SIGNER_STRING_BYTES + 1),
            database_id: "db".to_string(),
            schema_hash: hex::encode([1; 32]),
            genesis_hash: hex::encode([2; 32]),
            created_at: "2026-07-17T00:00:00Z".to_string(),
            created_by: "creator".to_string(),
            consistency_policy: "StrictWitnessed".to_string(),
            object_layout_version: 1,
            active_membership_epoch: 1,
            active_policy_epoch: 1,
            current_row_index_generation: 0,
            current_projection_generation: 0,
            manifest_hash: None,
            manifest_signature: None,
        });
        assert!(
            encode_signer_request(&object)
                .unwrap_err()
                .to_string()
                .contains("protocol bound")
        );
    }
}
