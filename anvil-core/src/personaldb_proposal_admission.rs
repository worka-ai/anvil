use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    core_store::{
        CF_PERSONALDB, CoreMetaRowCommonProto, CoreMetaStore, CoreMetaTuplePart, CoreMutationBatch,
        CoreMutationBatchReceipt, CoreMutationOperation, CoreMutationPrecondition, CoreStore,
        CoreTransactionState, TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
        TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
        core_meta_committed_row_common, core_meta_payload_digest, core_meta_tuple_key,
        decode_deterministic_proto, encode_deterministic_proto,
    },
    formats::hash32,
    partition_fence::{
        PartitionOwnerState, PartitionWritePermit, partition_write_precondition,
        read_partition_owner, validate_write_permit_for_state,
    },
    personaldb_commit_store::{decode_commit_certificate, encode_commit_certificate},
    personaldb_control::{PersonalDbCommitCertificate, validate_commit_certificate_unsigned},
    personaldb_coremeta::{
        personaldb_partition_id, personaldb_realm_id, personaldb_root_key_hash,
        tenant_id_from_realm,
    },
    personaldb_heads::{
        PersonalDbCommittedHead, decode_committed_head, encode_committed_head,
        personaldb_committed_head_precondition, read_personaldb_committed_head,
        validate_committed_head_unsigned,
    },
    personaldb_signing::{
        PersonalDbProtocolKeyring, signature_envelope_from_proto, signature_envelope_to_proto,
    },
    personaldb_signing_object::PersonalDbSigningObject,
    storage::Storage,
};
use anyhow::{Context, Result, anyhow, bail};
use personaldb_protocol::{
    DatabaseId, KeyGeneration, KeyId, ProtocolSignable, ProtocolSigner, PublicKeyStatus,
    PublicKeyTrustRecord, PublicKeyTrustStore, SignatureDomain, SignatureEnvelopeV1,
    SignatureMetadata, SignaturePurpose, SignatureScope, SigningPayload,
};
use prost::Message;
use sha2::{Digest, Sha256};

const FORMAT_VERSION_V1: u32 = 1;
const MAX_ADMISSION_LIFETIME_SECONDS: i64 = 5 * 60;
const MAX_TEXT_BYTES: usize = 4 * 1024;
const MAX_SIGNING_OBJECT_BYTES: usize = 64 * 1024;

const CLAIM_HASH_DOMAIN: &[u8] = b"anvil-proposal-idempotency-claim-v1\0";
const RESERVATION_ID_DOMAIN: &[u8] = b"anvil-proposal-admission-reservation-id-v1\0";
const RESERVATION_HASH_DOMAIN: &[u8] = b"anvil-proposal-admission-reservation-v1\0";
const SLOT_HASH_DOMAIN: &[u8] = b"anvil-proposal-admission-slot-v1\0";
const UNSIGNED_CERTIFICATE_HASH_DOMAIN: &[u8] = b"anvil-unsigned-commit-certificate-v2\0";
const HEAD_TEMPLATE_HASH_DOMAIN: &[u8] = b"anvil-witness-head-template-v1\0";
const UNSIGNED_HEAD_HASH_DOMAIN: &[u8] = b"anvil-unsigned-committed-head-v2\0";
const CANDIDATE_HASH_DOMAIN: &[u8] = b"anvil-witness-signing-candidate-v1\0";
const RECEIPT_HASH_DOMAIN: &[u8] = b"anvil-witness-dual-signing-receipt-v1\0";
const SIGNED_CERTIFICATE_HASH_DOMAIN: &[u8] = b"personaldb-commit-certificate-v2\0";
const SIGNED_HEAD_HASH_DOMAIN: &[u8] = b"personaldb-committed-head-v2\0";
const SIGNED_ADMISSION_HASH_DOMAIN: &[u8] = b"personaldb-proposal-admission-v1\0";

const CLAIM_KEY_PREFIX: &str = "proposal-idempotency-claim";
const SLOT_KEY_PREFIX: &str = "proposal-slot";
const RESERVATION_KEY_PREFIX: &str = "proposal-reservation";
const CANDIDATE_KEY_PREFIX: &str = "witness-candidate";
const RECEIPT_KEY_PREFIX: &str = "witness-signing";

pub const PERSONALDB_GROUP_PARTITION_FAMILY: &str = "personaldb_group";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalIdempotencyClaimIdentityV1 {
    pub format_version: u32,
    pub tenant_id: String,
    pub application_id: String,
    pub operation_id: String,
    pub request_id: String,
    pub database_id: String,
    pub client_proposal_hash_sha256: [u8; 32],
    pub changeset_payload_hash_sha256: [u8; 32],
    pub workflow_id: String,
    pub fencing_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ProposalAdmissionReservationStateV1 {
    Unspecified = 0,
    Reserved = 1,
    Signing = 2,
    Committed = 3,
    Revoked = 4,
    Expired = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, prost::Enumeration)]
#[repr(i32)]
pub enum ProposalAdmissionSlotStateV1 {
    Unspecified = 0,
    Reserved = 1,
    Signed = 2,
    Committed = 3,
    ReleasedUnsigned = 4,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionSlotV1 {
    pub format_version: u32,
    pub database_id: String,
    pub next_log_index: u64,
    pub expected_previous_log_hash_sha256: [u8; 32],
    pub placement_epoch: u64,
    pub client_log_epoch: u64,
    pub fencing_generation: u64,
    pub slot_revision: u64,
    pub reservation_id: String,
    pub state: ProposalAdmissionSlotStateV1,
    pub witness_dual_signing_receipt_sha256: Option<[u8; 32]>,
    pub terminal_committed_head_sha256: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionReservationIdentityV1 {
    pub format_version: u32,
    pub reservation_id: String,
    pub database_id: String,
    pub group_kind: String,
    pub proposer_id: String,
    pub client_proposal_hash_sha256: [u8; 32],
    pub changeset_payload_hash_sha256: [u8; 32],
    pub expected_previous_log_index: u64,
    pub expected_previous_log_hash_sha256: [u8; 32],
    pub membership_revision: u64,
    pub placement_epoch: u64,
    pub client_log_epoch: u64,
    pub workflow_id: String,
    pub fencing_generation: u64,
    pub leader_lease_id: String,
    pub leader_lease_revision: u64,
    pub authorization_receipt_sha256: [u8; 32],
    pub authorization_revision: u64,
    pub idempotency_claim_sha256: [u8; 32],
    pub issued_at_unix_seconds: i64,
    pub expires_at_unix_seconds: i64,
    pub selected_voter_ids: Vec<String>,
    pub primary_server_id: String,
    pub proposal_admission_key_id: String,
    pub proposal_admission_generation: u64,
    pub witness_key_id: String,
    pub witness_key_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionReservationV1 {
    pub identity: ProposalAdmissionReservationIdentityV1,
    pub reservation_revision: u64,
    pub state: ProposalAdmissionReservationStateV1,
    pub candidate_entry_core_sha256: Option<[u8; 32]>,
    pub candidate_unsigned_certificate_sha256: Option<[u8; 32]>,
    pub candidate_head_template_sha256: Option<[u8; 32]>,
    pub candidate_unsigned_committed_head_sha256: Option<[u8; 32]>,
    pub witness_dual_signing_receipt_sha256: Option<[u8; 32]>,
    pub terminal_commit_certificate_sha256: Option<[u8; 32]>,
    pub terminal_committed_head_sha256: Option<[u8; 32]>,
    pub witness_signing_candidate_sha256: Option<[u8; 32]>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessSigningCandidateV1 {
    pub format_version: u32,
    pub reservation_id: String,
    pub signing_reservation_revision: u64,
    pub database_id: String,
    pub next_log_index: u64,
    pub client_log_epoch: u64,
    pub fencing_generation: u64,
    pub witness_key_id: String,
    pub witness_key_generation: u64,
    pub unsigned_commit_certificate: Vec<u8>,
    pub head_template: Vec<u8>,
    pub candidate_entry_core_sha256: [u8; 32],
    pub candidate_unsigned_certificate_sha256: [u8; 32],
    pub candidate_head_template_sha256: [u8; 32],
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WitnessDualSigningReceiptV1 {
    pub format_version: u32,
    pub reservation_id: String,
    pub signing_reservation_revision: u64,
    pub candidate_unsigned_certificate_sha256: [u8; 32],
    pub candidate_head_template_sha256: [u8; 32],
    pub witness_key_id: String,
    pub witness_key_generation: u64,
    pub certificate_signature_sha256: [u8; 32],
    pub signed_commit_certificate_sha256: [u8; 32],
    pub unsigned_committed_head_sha256: [u8; 32],
    pub head_signature_sha256: [u8; 32],
    pub signed_committed_head_sha256: [u8; 32],
    pub signed_at_unix_seconds: i64,
    pub signed_commit_certificate: Vec<u8>,
    pub signed_committed_head: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposalAdmissionV1 {
    pub format_version: u32,
    pub database_id: String,
    pub group_kind: String,
    pub proposer_id: String,
    pub client_proposal_hash_sha256: [u8; 32],
    pub workflow_id: String,
    pub fencing_generation: u64,
    pub authorization_revision: u64,
    pub idempotency_claim_sha256: [u8; 32],
    pub issued_at_unix_seconds: i64,
    pub expires_at_unix_seconds: i64,
    pub reservation_id: String,
    pub reservation_revision: u64,
    pub reservation_identity_sha256: [u8; 32],
    pub authorization_receipt_sha256: [u8; 32],
    pub selected_voter_ids: Vec<String>,
    pub primary_server_id: String,
    pub proposal_admission_key_id: String,
    pub proposal_admission_generation: u64,
    pub witness_key_id: String,
    pub witness_key_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignedProposalAdmissionV1 {
    pub admission: ProposalAdmissionV1,
    pub proposal_admission_signature: SignatureEnvelopeV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredProposalAdmissionReservationV1 {
    pub tenant_id: i64,
    pub reservation: ProposalAdmissionReservationV1,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginWitnessSigningV1 {
    pub tenant_id: i64,
    pub reservation_id: String,
    pub expected_reservation_revision: u64,
    pub unsigned_commit_certificate: PersonalDbCommitCertificate,
    pub head_template: PersonalDbCommittedHead,
    pub created_at_unix_seconds: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SignCertificateAndHeadV1 {
    pub reservation_id: String,
    pub signing_reservation_revision: u64,
}

pub struct PersonalDbAdmissionAuthority<'a> {
    pub storage: &'a Storage,
    pub trust_store: &'a PublicKeyTrustStore,
    pub write_permit: &'a PartitionWritePermit,
    pub partition_owner_signing_key: &'a [u8],
    pub now_unix_seconds: i64,
}

#[derive(Clone, PartialEq, Message)]
struct ProposalIdempotencyClaimIdentityProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    tenant_id: String,
    #[prost(string, tag = "3")]
    application_id: String,
    #[prost(string, tag = "4")]
    operation_id: String,
    #[prost(string, tag = "5")]
    request_id: String,
    #[prost(string, tag = "6")]
    database_id: String,
    #[prost(bytes = "vec", tag = "7")]
    client_proposal_hash_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    changeset_payload_hash_sha256: Vec<u8>,
    #[prost(string, tag = "9")]
    workflow_id: String,
    #[prost(uint64, tag = "10")]
    fencing_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ProposalAdmissionSlotProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    database_id: String,
    #[prost(uint64, tag = "3")]
    next_log_index: u64,
    #[prost(bytes = "vec", tag = "4")]
    expected_previous_log_hash_sha256: Vec<u8>,
    #[prost(uint64, tag = "5")]
    placement_epoch: u64,
    #[prost(uint64, tag = "6")]
    client_log_epoch: u64,
    #[prost(uint64, tag = "7")]
    fencing_generation: u64,
    #[prost(uint64, tag = "8")]
    slot_revision: u64,
    #[prost(string, tag = "9")]
    reservation_id: String,
    #[prost(enumeration = "ProposalAdmissionSlotStateV1", tag = "10")]
    state: i32,
    #[prost(bytes = "vec", optional, tag = "11")]
    witness_dual_signing_receipt_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "12")]
    terminal_committed_head_sha256: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct ProposalAdmissionReservationIdentityProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    reservation_id: String,
    #[prost(string, tag = "3")]
    database_id: String,
    #[prost(string, tag = "4")]
    group_kind: String,
    #[prost(string, tag = "5")]
    proposer_id: String,
    #[prost(bytes = "vec", tag = "6")]
    client_proposal_hash_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "7")]
    changeset_payload_hash_sha256: Vec<u8>,
    #[prost(uint64, tag = "8")]
    expected_previous_log_index: u64,
    #[prost(bytes = "vec", tag = "9")]
    expected_previous_log_hash_sha256: Vec<u8>,
    #[prost(uint64, tag = "10")]
    membership_revision: u64,
    #[prost(uint64, tag = "11")]
    placement_epoch: u64,
    #[prost(uint64, tag = "12")]
    client_log_epoch: u64,
    #[prost(string, tag = "13")]
    workflow_id: String,
    #[prost(uint64, tag = "14")]
    fencing_generation: u64,
    #[prost(string, tag = "15")]
    leader_lease_id: String,
    #[prost(uint64, tag = "16")]
    leader_lease_revision: u64,
    #[prost(bytes = "vec", tag = "17")]
    authorization_receipt_sha256: Vec<u8>,
    #[prost(uint64, tag = "18")]
    authorization_revision: u64,
    #[prost(bytes = "vec", tag = "19")]
    idempotency_claim_sha256: Vec<u8>,
    #[prost(int64, tag = "20")]
    issued_at_unix_seconds: i64,
    #[prost(int64, tag = "21")]
    expires_at_unix_seconds: i64,
    #[prost(string, repeated, tag = "22")]
    selected_voter_ids: Vec<String>,
    #[prost(string, tag = "23")]
    primary_server_id: String,
    #[prost(string, tag = "24")]
    proposal_admission_key_id: String,
    #[prost(uint64, tag = "25")]
    proposal_admission_generation: u64,
    #[prost(string, tag = "26")]
    witness_key_id: String,
    #[prost(uint64, tag = "27")]
    witness_key_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct ProposalAdmissionReservationProto {
    #[prost(message, optional, tag = "1")]
    identity: Option<ProposalAdmissionReservationIdentityProto>,
    #[prost(uint64, tag = "2")]
    reservation_revision: u64,
    #[prost(enumeration = "ProposalAdmissionReservationStateV1", tag = "3")]
    state: i32,
    #[prost(bytes = "vec", optional, tag = "4")]
    candidate_entry_core_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "5")]
    candidate_unsigned_certificate_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "6")]
    candidate_head_template_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "7")]
    candidate_unsigned_committed_head_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "8")]
    witness_dual_signing_receipt_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "9")]
    terminal_commit_certificate_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "10")]
    terminal_committed_head_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "11")]
    witness_signing_candidate_sha256: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct WitnessSigningCandidateProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    reservation_id: String,
    #[prost(uint64, tag = "3")]
    signing_reservation_revision: u64,
    #[prost(string, tag = "4")]
    database_id: String,
    #[prost(uint64, tag = "5")]
    next_log_index: u64,
    #[prost(uint64, tag = "6")]
    client_log_epoch: u64,
    #[prost(uint64, tag = "7")]
    fencing_generation: u64,
    #[prost(string, tag = "8")]
    witness_key_id: String,
    #[prost(uint64, tag = "9")]
    witness_key_generation: u64,
    #[prost(bytes = "vec", tag = "10")]
    unsigned_commit_certificate: Vec<u8>,
    #[prost(bytes = "vec", tag = "11")]
    head_template: Vec<u8>,
    #[prost(bytes = "vec", tag = "12")]
    candidate_entry_core_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "13")]
    candidate_unsigned_certificate_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "14")]
    candidate_head_template_sha256: Vec<u8>,
    #[prost(int64, tag = "15")]
    created_at_unix_seconds: i64,
}

#[derive(Clone, PartialEq, Message)]
struct WitnessDualSigningReceiptProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    reservation_id: String,
    #[prost(uint64, tag = "3")]
    signing_reservation_revision: u64,
    #[prost(bytes = "vec", tag = "4")]
    candidate_unsigned_certificate_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    candidate_head_template_sha256: Vec<u8>,
    #[prost(string, tag = "6")]
    witness_key_id: String,
    #[prost(uint64, tag = "7")]
    witness_key_generation: u64,
    #[prost(bytes = "vec", tag = "8")]
    certificate_signature_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "9")]
    signed_commit_certificate_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    unsigned_committed_head_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "11")]
    head_signature_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "12")]
    signed_committed_head_sha256: Vec<u8>,
    #[prost(int64, tag = "13")]
    signed_at_unix_seconds: i64,
    #[prost(bytes = "vec", tag = "14")]
    signed_commit_certificate: Vec<u8>,
    #[prost(bytes = "vec", tag = "15")]
    signed_committed_head: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct ProposalAdmissionProto {
    #[prost(uint32, tag = "1")]
    format_version: u32,
    #[prost(string, tag = "2")]
    database_id: String,
    #[prost(string, tag = "3")]
    group_kind: String,
    #[prost(string, tag = "4")]
    proposer_id: String,
    #[prost(bytes = "vec", tag = "5")]
    client_proposal_hash_sha256: Vec<u8>,
    #[prost(string, tag = "6")]
    workflow_id: String,
    #[prost(uint64, tag = "7")]
    fencing_generation: u64,
    #[prost(uint64, tag = "8")]
    authorization_revision: u64,
    #[prost(bytes = "vec", tag = "9")]
    idempotency_claim_sha256: Vec<u8>,
    #[prost(int64, tag = "10")]
    issued_at_unix_seconds: i64,
    #[prost(int64, tag = "11")]
    expires_at_unix_seconds: i64,
    #[prost(string, tag = "12")]
    reservation_id: String,
    #[prost(uint64, tag = "13")]
    reservation_revision: u64,
    #[prost(bytes = "vec", tag = "14")]
    reservation_identity_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "15")]
    authorization_receipt_sha256: Vec<u8>,
    #[prost(string, repeated, tag = "16")]
    selected_voter_ids: Vec<String>,
    #[prost(string, tag = "17")]
    primary_server_id: String,
    #[prost(string, tag = "18")]
    proposal_admission_key_id: String,
    #[prost(uint64, tag = "19")]
    proposal_admission_generation: u64,
    #[prost(string, tag = "20")]
    witness_key_id: String,
    #[prost(uint64, tag = "21")]
    witness_key_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
struct SignedProposalAdmissionProto {
    #[prost(message, optional, tag = "1")]
    admission: Option<ProposalAdmissionProto>,
    #[prost(message, optional, tag = "2")]
    proposal_admission_signature: Option<WireSignatureEnvelopeV1>,
}

#[derive(Clone, PartialEq, Message)]
struct ClaimRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    claim: Option<ProposalIdempotencyClaimIdentityProto>,
}

#[derive(Clone, PartialEq, Message)]
struct SlotRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    slot: Option<ProposalAdmissionSlotProto>,
}

#[derive(Clone, PartialEq, Message)]
struct ReservationRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    reservation: Option<ProposalAdmissionReservationProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CandidateRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    candidate: Option<WitnessSigningCandidateProto>,
}

#[derive(Clone, PartialEq, Message)]
struct ReceiptRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    receipt: Option<WitnessDualSigningReceiptProto>,
}

impl ProposalIdempotencyClaimIdentityV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_claim(self)?;
        Ok(domain_hash(
            CLAIM_HASH_DOMAIN,
            &encode_deterministic_proto(&claim_to_proto(self)),
        ))
    }
}

impl ProposalAdmissionReservationIdentityV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_reservation_identity_shape(self)?;
        Ok(domain_hash(
            RESERVATION_HASH_DOMAIN,
            &encode_deterministic_proto(&reservation_identity_to_proto(self)),
        ))
    }
}

impl ProposalAdmissionSlotV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_slot(self)?;
        Ok(domain_hash(
            SLOT_HASH_DOMAIN,
            &encode_deterministic_proto(&slot_to_proto(self)),
        ))
    }
}

impl WitnessSigningCandidateV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_candidate(self)?;
        Ok(domain_hash(
            CANDIDATE_HASH_DOMAIN,
            &encode_deterministic_proto(&candidate_to_proto(self)),
        ))
    }
}

impl WitnessDualSigningReceiptV1 {
    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        validate_receipt_shape(self)?;
        Ok(domain_hash(
            RECEIPT_HASH_DOMAIN,
            &encode_deterministic_proto(&receipt_to_proto(self)),
        ))
    }
}

impl ProposalAdmissionV1 {
    pub fn encode_deterministic(&self) -> Result<Vec<u8>> {
        validate_admission_shape(self)?;
        Ok(encode_deterministic_proto(&admission_to_proto(self)))
    }
}

impl SignedProposalAdmissionV1 {
    pub fn encode_deterministic(&self) -> Result<Vec<u8>> {
        self.proposal_admission_signature.validate_shape()?;
        Ok(encode_deterministic_proto(&SignedProposalAdmissionProto {
            admission: Some(admission_to_proto(&self.admission)),
            proposal_admission_signature: Some(signature_envelope_to_proto(
                &self.proposal_admission_signature,
            )),
        }))
    }

    pub fn decode_deterministic(bytes: &[u8]) -> Result<Self> {
        let proto = decode_deterministic_proto::<SignedProposalAdmissionProto>(
            bytes,
            "signed proposal admission",
        )?;
        let admission = admission_from_proto(
            proto
                .admission
                .ok_or_else(|| anyhow!("signed proposal admission body missing"))?,
        )?;
        validate_admission_shape(&admission)?;
        let proposal_admission_signature = signature_envelope_from_proto(
            proto
                .proposal_admission_signature
                .ok_or_else(|| anyhow!("signed proposal admission signature missing"))?,
        )?;
        Ok(Self {
            admission,
            proposal_admission_signature,
        })
    }

    pub fn hash_sha256(&self) -> Result<[u8; 32]> {
        Ok(domain_hash(
            SIGNED_ADMISSION_HASH_DOMAIN,
            &self.encode_deterministic()?,
        ))
    }

    pub fn verify(
        &self,
        reservation: &ProposalAdmissionReservationV1,
        trust_store: &PublicKeyTrustStore,
    ) -> Result<()> {
        let expected = admission_from_reservation(reservation)?;
        if self.admission != expected {
            bail!("signed proposal admission does not match its reservation");
        }
        let signable = ProposalAdmissionSignable {
            admission: &self.admission,
            log_index: next_log_index(&reservation.identity)?,
        };
        trust_store.verify(&signable, &self.proposal_admission_signature)?;
        Ok(())
    }
}

struct ProposalAdmissionSignable<'a> {
    admission: &'a ProposalAdmissionV1,
    log_index: u64,
}

impl ProtocolSignable for ProposalAdmissionSignable<'_> {
    fn signature_metadata(&self) -> SignatureMetadata {
        SignatureMetadata::for_domain(
            SignaturePurpose::ProposalAdmission,
            SignatureDomain::ProposalAdmission,
            self.log_index,
        )
        .with_scope(SignatureScope::for_database_group(
            DatabaseId::new(&self.admission.database_id),
            self.admission.database_id.clone(),
        ))
        .requiring_key_generation(
            KeyGeneration::new(self.admission.proposal_admission_generation)
                .expect("validated proposal admission key generation"),
        )
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        SigningPayload::Sha256Digest(
            Sha256::digest(encode_deterministic_proto(&admission_to_proto(
                self.admission,
            )))
            .into(),
        )
    }
}

struct RequiredGenerationSignable<'a> {
    object: &'a dyn ProtocolSignable,
    generation: KeyGeneration,
}

impl ProtocolSignable for RequiredGenerationSignable<'_> {
    fn signature_metadata(&self) -> SignatureMetadata {
        self.object
            .signature_metadata()
            .requiring_key_generation(self.generation)
    }

    fn signing_payload(&self) -> SigningPayload<'_> {
        self.object.signing_payload()
    }
}

enum WitnessSigningAuthority<'a> {
    ProtocolSigner(&'a dyn ProtocolSigner),
    Keyring(&'a PersonalDbProtocolKeyring),
}

impl WitnessSigningAuthority<'_> {
    fn validate_bound_signer(
        &self,
        expected_key_id: &str,
        expected_generation: u64,
        database_id: &str,
        log_index: u64,
    ) -> Result<()> {
        match self {
            Self::ProtocolSigner(signer) => validate_bound_signer(
                signer.trust_record(),
                SignaturePurpose::Witness,
                expected_key_id,
                expected_generation,
                database_id,
                log_index,
            ),
            Self::Keyring(keyring) => {
                let record = keyring.trust_record_for_purpose(SignaturePurpose::Witness)?;
                validate_bound_signer(
                    record,
                    SignaturePurpose::Witness,
                    expected_key_id,
                    expected_generation,
                    database_id,
                    log_index,
                )
            }
        }
    }

    async fn sign_certificate_with_required_generation(
        &self,
        certificate: &PersonalDbCommitCertificate,
        generation: KeyGeneration,
        trust_store: &PublicKeyTrustStore,
    ) -> Result<SignatureEnvelopeV1> {
        let signature = match self {
            Self::ProtocolSigner(signer) => {
                let signable = RequiredGenerationSignable {
                    object: certificate,
                    generation,
                };
                let signature = signer.sign(&signable)?;
                trust_store.verify(&signable, &signature)?;
                return Ok(signature);
            }
            Self::Keyring(keyring) => {
                keyring
                    .sign(PersonalDbSigningObject::CommitCertificate(
                        certificate.clone(),
                    ))
                    .await?
            }
        };
        let signable = RequiredGenerationSignable {
            object: certificate,
            generation,
        };
        trust_store.verify(&signable, &signature)?;
        Ok(signature)
    }

    async fn sign_head_with_required_generation(
        &self,
        head: &PersonalDbCommittedHead,
        generation: KeyGeneration,
        trust_store: &PublicKeyTrustStore,
    ) -> Result<SignatureEnvelopeV1> {
        let signature = match self {
            Self::ProtocolSigner(signer) => {
                let signable = RequiredGenerationSignable {
                    object: head,
                    generation,
                };
                let signature = signer.sign(&signable)?;
                trust_store.verify(&signable, &signature)?;
                return Ok(signature);
            }
            Self::Keyring(keyring) => {
                keyring
                    .sign(PersonalDbSigningObject::CommittedHead(head.clone()))
                    .await?
            }
        };
        let signable = RequiredGenerationSignable {
            object: head,
            generation,
        };
        trust_store.verify(&signable, &signature)?;
        Ok(signature)
    }
}

pub fn sign_proposal_admission(
    reservation: &ProposalAdmissionReservationV1,
    signer: &dyn ProtocolSigner,
    trust_store: &PublicKeyTrustStore,
) -> Result<SignedProposalAdmissionV1> {
    validate_reservation(reservation)?;
    if reservation.state != ProposalAdmissionReservationStateV1::Reserved {
        bail!("proposal admission can be signed only for a reserved reservation");
    }
    validate_bound_signer(
        signer.trust_record(),
        SignaturePurpose::ProposalAdmission,
        &reservation.identity.proposal_admission_key_id,
        reservation.identity.proposal_admission_generation,
        &reservation.identity.database_id,
        next_log_index(&reservation.identity)?,
    )?;
    let admission = admission_from_reservation(reservation)?;
    let signable = ProposalAdmissionSignable {
        admission: &admission,
        log_index: next_log_index(&reservation.identity)?,
    };
    let proposal_admission_signature = signer.sign(&signable)?;
    let signed = SignedProposalAdmissionV1 {
        admission,
        proposal_admission_signature,
    };
    signed.verify(reservation, trust_store)?;
    Ok(signed)
}

pub fn derive_reservation_id(
    database_id: &str,
    idempotency_claim_sha256: [u8; 32],
) -> Result<String> {
    validate_text(database_id, "database_id")?;
    let mut hasher = Sha256::new();
    hasher.update(RESERVATION_ID_DOMAIN);
    hasher.update(database_id.as_bytes());
    hasher.update([0]);
    hasher.update(idempotency_claim_sha256);
    Ok(format!("sha256:{}", hex::encode(hasher.finalize())))
}

pub fn personaldb_group_partition_owner_id(tenant_id: i64, database_id: &str) -> Result<String> {
    validate_tenant_database(tenant_id, database_id)?;
    Ok(hex::encode(hash32(
        format!("{PERSONALDB_GROUP_PARTITION_FAMILY}\0{tenant_id}\0{database_id}").as_bytes(),
    )))
}

pub fn personaldb_group_leader_lease_id(owner: &PartitionOwnerState) -> String {
    format!(
        "partition-owner:{}:{}",
        owner.partition_family, owner.partition_id
    )
}

pub async fn reserve_personaldb_proposal(
    authority: &PersonalDbAdmissionAuthority<'_>,
    claim: ProposalIdempotencyClaimIdentityV1,
    identity: ProposalAdmissionReservationIdentityV1,
) -> Result<ProposalAdmissionReservationV1> {
    validate_claim(&claim)?;
    let tenant_id = parse_claim_tenant_id(&claim)?;
    if identity.database_id != claim.database_id
        || identity.client_proposal_hash_sha256 != claim.client_proposal_hash_sha256
        || identity.changeset_payload_hash_sha256 != claim.changeset_payload_hash_sha256
        || identity.workflow_id != claim.workflow_id
        || identity.fencing_generation != claim.fencing_generation
    {
        bail!("proposal reservation identity does not match its idempotency claim");
    }
    let claim_hash = claim.hash_sha256()?;
    if identity.idempotency_claim_sha256 != claim_hash {
        bail!("proposal reservation idempotency claim hash mismatch");
    }
    let expected_reservation_id = derive_reservation_id(&claim.database_id, claim_hash)?;
    if identity.reservation_id != expected_reservation_id {
        bail!("proposal reservation ID is not canonical");
    }

    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, &identity, &guard)?;
    validate_active_key_binding(
        authority.trust_store,
        &identity.proposal_admission_key_id,
        identity.proposal_admission_generation,
        SignaturePurpose::ProposalAdmission,
        &identity.database_id,
        next_log_index(&identity)?,
    )?;
    validate_active_key_binding(
        authority.trust_store,
        &identity.witness_key_id,
        identity.witness_key_generation,
        SignaturePurpose::Witness,
        &identity.database_id,
        next_log_index(&identity)?,
    )?;

    let reservation = ProposalAdmissionReservationV1 {
        identity,
        reservation_revision: 1,
        state: ProposalAdmissionReservationStateV1::Reserved,
        candidate_entry_core_sha256: None,
        candidate_unsigned_certificate_sha256: None,
        candidate_head_template_sha256: None,
        candidate_unsigned_committed_head_sha256: None,
        witness_dual_signing_receipt_sha256: None,
        terminal_commit_certificate_sha256: None,
        terminal_committed_head_sha256: None,
        witness_signing_candidate_sha256: None,
    };
    validate_reservation(&reservation)?;
    let slot = slot_from_reservation(&reservation)?;

    let claim_key = claim_key(&claim)?;
    let slot_key = slot_key(
        tenant_id,
        &slot.database_id,
        slot.next_log_index,
        slot.client_log_epoch,
    )?;
    let reservation_key = reservation_key(&reservation.identity.reservation_id)?;
    let existing_claim = read_claim_row(authority.storage, &claim_key)?;
    let existing_slot = read_slot_row(authority.storage, &slot_key)?;
    let existing_reservation = read_reservation_row(authority.storage, &reservation_key)?;
    if existing_claim.is_some() || existing_slot.is_some() || existing_reservation.is_some() {
        return exact_reservation_replay(
            tenant_id,
            &claim,
            &slot,
            &reservation,
            existing_claim,
            existing_slot,
            existing_reservation,
        );
    }

    let protocol_hash = reservation.identity.hash_sha256()?;
    let transaction_id = format!("personaldb-reserve:{}", hex::encode(protocol_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &claim.database_id).await?;
    let created_at_unix_nanos = unix_seconds_to_nanos(reservation.identity.issued_at_unix_seconds)?;
    let common = row_common(
        tenant_id,
        &claim.database_id,
        root_generation,
        &transaction_id,
        created_at_unix_nanos,
    );
    let claim_payload = encode_deterministic_proto(&ClaimRowProto {
        common: Some(common.clone()),
        claim: Some(claim_to_proto(&claim)),
    });
    let slot_payload = encode_deterministic_proto(&SlotRowProto {
        common: Some(common.clone()),
        slot: Some(slot_to_proto(&slot)),
    });
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common),
        reservation: Some(reservation_to_proto(&reservation)),
    });
    let preconditions = vec![
        guard.owner_precondition,
        guard.head_precondition,
        absent_precondition(TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW, claim_key.clone()),
        absent_precondition(TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, slot_key.clone()),
        absent_precondition(
            TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
            reservation_key.clone(),
        ),
    ];
    let operations = vec![
        put_operation(
            tenant_id,
            &claim.database_id,
            TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW,
            claim_key,
            claim_payload,
        ),
        put_operation(
            tenant_id,
            &claim.database_id,
            TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
            slot_key,
            slot_payload,
        ),
        put_operation(
            tenant_id,
            &claim.database_id,
            TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
            reservation_key,
            reservation_payload,
        ),
    ];
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &claim.database_id,
        &authority.write_permit.owner_node_id,
        preconditions,
        operations,
    )
    .await?;

    let stored = read_personaldb_proposal_reservation(
        authority.storage,
        &reservation.identity.reservation_id,
    )?
    .ok_or_else(|| anyhow!("proposal reservation commit produced no visible row"))?;
    if stored.tenant_id != tenant_id || stored.reservation != reservation {
        bail!("proposal reservation commit did not round-trip exactly");
    }
    Ok(stored.reservation)
}

pub async fn begin_personaldb_witness_signing(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: BeginWitnessSigningV1,
) -> Result<WitnessSigningCandidateV1> {
    validate_tenant_database(
        request.tenant_id,
        &request.unsigned_commit_certificate.database_id,
    )?;
    if request.created_at_unix_seconds != authority.now_unix_seconds {
        bail!("witness candidate creation time must equal the authority time");
    }
    validate_commit_certificate_unsigned(&request.unsigned_commit_certificate)?;
    validate_committed_head_unsigned(&request.head_template)?;
    require_unsigned_certificate(&request.unsigned_commit_certificate)?;
    require_unsigned_head(&request.head_template)?;

    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    if stored.tenant_id != request.tenant_id {
        bail!("witness candidate tenant does not match reservation");
    }
    let reservation = stored.reservation;
    let signing_revision = request
        .expected_reservation_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation revision overflow"))?;
    let is_initial = reservation.state == ProposalAdmissionReservationStateV1::Reserved
        && reservation.reservation_revision == request.expected_reservation_revision;
    let is_replay = reservation.state == ProposalAdmissionReservationStateV1::Signing
        && reservation.reservation_revision == signing_revision;
    if !is_initial && !is_replay {
        bail!("proposal reservation is not at the expected reserved revision");
    }
    let identity = &reservation.identity;
    let guard = load_group_guard(authority, request.tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, identity, &guard)?;
    validate_candidate_objects(
        request.tenant_id,
        identity,
        &request.unsigned_commit_certificate,
        &request.head_template,
    )?;

    let unsigned_commit_certificate =
        encode_commit_certificate(&request.unsigned_commit_certificate)?;
    let head_template = encode_committed_head(&request.head_template)?;
    ensure_bounded_object(&unsigned_commit_certificate, "unsigned commit certificate")?;
    ensure_bounded_object(&head_template, "witness head template")?;
    let entry_hash = decode_hex32(
        &request.unsigned_commit_certificate.entry_hash,
        "entry_hash",
    )?;
    let candidate_unsigned_certificate_sha256 = domain_hash(
        UNSIGNED_CERTIFICATE_HASH_DOMAIN,
        &unsigned_commit_certificate,
    );
    let candidate_head_template_sha256 = domain_hash(HEAD_TEMPLATE_HASH_DOMAIN, &head_template);
    let candidate = WitnessSigningCandidateV1 {
        format_version: FORMAT_VERSION_V1,
        reservation_id: request.reservation_id.clone(),
        signing_reservation_revision: signing_revision,
        database_id: identity.database_id.clone(),
        next_log_index: next_log_index(identity)?,
        client_log_epoch: identity.client_log_epoch,
        fencing_generation: identity.fencing_generation,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
        unsigned_commit_certificate,
        head_template,
        candidate_entry_core_sha256: entry_hash,
        candidate_unsigned_certificate_sha256,
        candidate_head_template_sha256,
        created_at_unix_seconds: request.created_at_unix_seconds,
    };
    validate_candidate(&candidate)?;
    let candidate_hash = candidate.hash_sha256()?;

    let mut signing_reservation = reservation.clone();
    signing_reservation.reservation_revision = candidate.signing_reservation_revision;
    signing_reservation.state = ProposalAdmissionReservationStateV1::Signing;
    signing_reservation.candidate_entry_core_sha256 = Some(candidate.candidate_entry_core_sha256);
    signing_reservation.candidate_unsigned_certificate_sha256 =
        Some(candidate.candidate_unsigned_certificate_sha256);
    signing_reservation.candidate_head_template_sha256 =
        Some(candidate.candidate_head_template_sha256);
    signing_reservation.witness_signing_candidate_sha256 = Some(candidate_hash);
    validate_reservation(&signing_reservation)?;

    let candidate_key = candidate_key(
        request.tenant_id,
        &candidate.database_id,
        candidate.next_log_index,
        candidate.client_log_epoch,
        &candidate.reservation_id,
    )?;
    let reservation_key = reservation_key(&candidate.reservation_id)?;
    let existing_candidate = read_candidate_row(authority.storage, &candidate_key)?;
    if let Some(existing) = existing_candidate {
        if existing == candidate {
            let current =
                read_personaldb_proposal_reservation(authority.storage, &candidate.reservation_id)?
                    .ok_or_else(|| anyhow!("witness candidate has no reservation"))?;
            if current.tenant_id == request.tenant_id && current.reservation == signing_reservation
            {
                return Ok(existing);
            }
        }
        bail!("altered witness candidate replay");
    }

    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let transaction_id = format!("personaldb-candidate:{}", hex::encode(candidate_hash));
    let root_generation =
        next_group_root_generation(authority.storage, request.tenant_id, &candidate.database_id)
            .await?;
    let common = row_common(
        request.tenant_id,
        &candidate.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(candidate.created_at_unix_seconds)?,
    );
    let candidate_payload = encode_deterministic_proto(&CandidateRowProto {
        common: Some(common.clone()),
        candidate: Some(candidate_to_proto(&candidate)),
    });
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common),
        reservation: Some(reservation_to_proto(&signing_reservation)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        request.tenant_id,
        &candidate.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition,
            guard.head_precondition,
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key.clone(),
                &current_reservation_payload,
            ),
            absent_precondition(
                TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
                candidate_key.clone(),
            ),
        ],
        vec![
            put_operation(
                request.tenant_id,
                &candidate.database_id,
                TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
                candidate_key,
                candidate_payload,
            ),
            put_operation(
                request.tenant_id,
                &candidate.database_id,
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                reservation_payload,
            ),
        ],
    )
    .await?;

    let stored_candidate = read_witness_signing_candidate(
        authority.storage,
        request.tenant_id,
        &candidate.database_id,
        candidate.next_log_index,
        candidate.client_log_epoch,
        &candidate.reservation_id,
    )?
    .ok_or_else(|| anyhow!("witness candidate commit produced no visible row"))?;
    if stored_candidate != candidate {
        bail!("witness candidate commit did not round-trip exactly");
    }
    Ok(stored_candidate)
}

pub async fn sign_personaldb_certificate_and_head(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
    signer: &dyn ProtocolSigner,
) -> Result<WitnessDualSigningReceiptV1> {
    sign_personaldb_certificate_and_head_with_authority(
        authority,
        request,
        WitnessSigningAuthority::ProtocolSigner(signer),
    )
    .await
}

pub async fn sign_personaldb_certificate_and_head_with_keyring(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
    keyring: &PersonalDbProtocolKeyring,
) -> Result<WitnessDualSigningReceiptV1> {
    sign_personaldb_certificate_and_head_with_authority(
        authority,
        request,
        WitnessSigningAuthority::Keyring(keyring),
    )
    .await
}

async fn sign_personaldb_certificate_and_head_with_authority(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
    signer: WitnessSigningAuthority<'_>,
) -> Result<WitnessDualSigningReceiptV1> {
    validate_reservation_id(&request.reservation_id)?;
    if request.signing_reservation_revision == 0 {
        bail!("signing reservation revision must be nonzero");
    }
    if let Some(existing) =
        read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
    {
        validate_receipt_request(&existing, request)?;
        validate_stored_receipt(authority, &existing)?;
        return Ok(existing);
    }

    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    let tenant_id = stored.tenant_id;
    let reservation = stored.reservation;
    if reservation.state != ProposalAdmissionReservationStateV1::Signing
        || reservation.reservation_revision != request.signing_reservation_revision
    {
        bail!("proposal reservation is not at the requested signing revision");
    }
    let identity = &reservation.identity;
    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, identity, &guard)?;
    let candidate = read_witness_signing_candidate(
        authority.storage,
        tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
        &identity.reservation_id,
    )?
    .ok_or_else(|| anyhow!("witness signing candidate not found"))?;
    validate_candidate_against_reservation(&candidate, &reservation)?;
    signer.validate_bound_signer(
        &identity.witness_key_id,
        identity.witness_key_generation,
        &identity.database_id,
        candidate.next_log_index,
    )?;

    let mut certificate = decode_commit_certificate(&candidate.unsigned_commit_certificate)
        .context("decode witness candidate commit certificate")?;
    let mut head = decode_committed_head(&candidate.head_template)
        .context("decode witness candidate committed-head template")?;
    require_unsigned_certificate(&certificate)?;
    require_unsigned_head(&head)?;
    validate_candidate_objects(tenant_id, identity, &certificate, &head)?;

    let generation = KeyGeneration::new(identity.witness_key_generation)?;
    let certificate_signature = signer
        .sign_certificate_with_required_generation(&certificate, generation, authority.trust_store)
        .await?;
    certificate.certificate_hash = Some(crate::personaldb_control::hash_commit_certificate(
        &certificate,
    )?);
    certificate.witness_signature = Some(certificate_signature.clone());
    let signed_commit_certificate = encode_commit_certificate(&certificate)?;

    let head_signature = signer
        .sign_head_with_required_generation(&head, generation, authority.trust_store)
        .await?;
    head.head_hash = Some(crate::personaldb_heads::hash_committed_head(&head)?);
    head.head_signature = Some(head_signature.clone());
    let signed_committed_head = encode_committed_head(&head)?;
    ensure_bounded_object(&signed_commit_certificate, "signed commit certificate")?;
    ensure_bounded_object(&signed_committed_head, "signed committed head")?;

    let receipt = WitnessDualSigningReceiptV1 {
        format_version: FORMAT_VERSION_V1,
        reservation_id: request.reservation_id.clone(),
        signing_reservation_revision: request.signing_reservation_revision,
        candidate_unsigned_certificate_sha256: candidate.candidate_unsigned_certificate_sha256,
        candidate_head_template_sha256: candidate.candidate_head_template_sha256,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
        certificate_signature_sha256: Sha256::digest(certificate_signature.signature.as_bytes())
            .into(),
        signed_commit_certificate_sha256: domain_hash(
            SIGNED_CERTIFICATE_HASH_DOMAIN,
            &signed_commit_certificate,
        ),
        unsigned_committed_head_sha256: domain_hash(
            UNSIGNED_HEAD_HASH_DOMAIN,
            &candidate.head_template,
        ),
        head_signature_sha256: Sha256::digest(head_signature.signature.as_bytes()).into(),
        signed_committed_head_sha256: domain_hash(SIGNED_HEAD_HASH_DOMAIN, &signed_committed_head),
        signed_at_unix_seconds: candidate.created_at_unix_seconds,
        signed_commit_certificate,
        signed_committed_head,
    };
    validate_receipt_against_candidate(&receipt, &candidate)?;
    store_witness_receipt_create_absent(authority, tenant_id, &candidate, &guard, &receipt).await?;
    Ok(
        read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
            .ok_or_else(|| anyhow!("witness receipt commit produced no visible row"))?,
    )
}

pub async fn acknowledge_personaldb_witness_receipt(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
) -> Result<ProposalAdmissionReservationV1> {
    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    let tenant_id = stored.tenant_id;
    let reservation = stored.reservation;
    let identity = &reservation.identity;
    let receipt = read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("witness dual-signing receipt not found"))?;
    validate_receipt_request(&receipt, request)?;
    validate_stored_receipt(authority, &receipt)?;
    let receipt_hash = receipt.hash_sha256()?;
    if reservation.witness_dual_signing_receipt_sha256 == Some(receipt_hash) {
        let slot = read_proposal_admission_slot(
            authority.storage,
            tenant_id,
            &identity.database_id,
            next_log_index(identity)?,
            identity.client_log_epoch,
        )?
        .ok_or_else(|| anyhow!("proposal slot not found"))?;
        if slot.state == ProposalAdmissionSlotStateV1::Signed
            && slot.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
        {
            return Ok(reservation);
        }
        bail!("proposal reservation receipt acknowledgement is incomplete");
    }
    if reservation.state != ProposalAdmissionReservationStateV1::Signing
        || reservation.reservation_revision != request.signing_reservation_revision
        || reservation.witness_dual_signing_receipt_sha256.is_some()
    {
        bail!("proposal reservation cannot acknowledge this witness receipt");
    }

    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    validate_identity_against_guard(authority, identity, &guard)?;
    let slot_key = slot_key(
        tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
    )?;
    let mut slot = read_slot_row(authority.storage, &slot_key)?
        .ok_or_else(|| anyhow!("proposal slot not found"))?;
    if slot.reservation_id != identity.reservation_id
        || slot.state != ProposalAdmissionSlotStateV1::Reserved
    {
        bail!("proposal slot is not reserved for this witness receipt");
    }
    slot.state = ProposalAdmissionSlotStateV1::Signed;
    slot.slot_revision = slot
        .slot_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal slot revision overflow"))?;
    slot.witness_dual_signing_receipt_sha256 = Some(receipt_hash);
    slot.terminal_committed_head_sha256 = Some(receipt.signed_committed_head_sha256);

    let mut acknowledged = reservation.clone();
    acknowledged.candidate_unsigned_committed_head_sha256 =
        Some(receipt.unsigned_committed_head_sha256);
    acknowledged.witness_dual_signing_receipt_sha256 = Some(receipt_hash);
    validate_reservation(&acknowledged)?;
    validate_slot(&slot)?;

    let reservation_key = reservation_key(&identity.reservation_id)?;
    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let current_slot_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
        &slot_key,
    )?
    .ok_or_else(|| anyhow!("proposal slot disappeared"))?;
    let receipt_key = receipt_key(&identity.reservation_id)?;
    let receipt_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
        &receipt_key,
    )?
    .ok_or_else(|| anyhow!("witness receipt disappeared"))?;
    let transaction_id = format!("personaldb-receipt-ack:{}", hex::encode(receipt_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &identity.database_id).await?;
    let common = row_common(
        tenant_id,
        &identity.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(authority.now_unix_seconds)?,
    );
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common.clone()),
        reservation: Some(reservation_to_proto(&acknowledged)),
    });
    let slot_payload = encode_deterministic_proto(&SlotRowProto {
        common: Some(common),
        slot: Some(slot_to_proto(&slot)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &identity.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition,
            guard.head_precondition,
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key.clone(),
                &current_reservation_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key.clone(),
                &current_slot_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
                receipt_key,
                &receipt_payload,
            ),
        ],
        vec![
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                reservation_payload,
            ),
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key,
                slot_payload,
            ),
        ],
    )
    .await?;
    Ok(
        read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
            .ok_or_else(|| anyhow!("acknowledged proposal reservation disappeared"))?
            .reservation,
    )
}

pub async fn commit_personaldb_witnessed_proposal(
    authority: &PersonalDbAdmissionAuthority<'_>,
    request: &SignCertificateAndHeadV1,
) -> Result<ProposalAdmissionReservationV1> {
    let stored = read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("proposal reservation not found"))?;
    let tenant_id = stored.tenant_id;
    let reservation = stored.reservation;
    let identity = &reservation.identity;
    let receipt = read_witness_dual_signing_receipt(authority.storage, &request.reservation_id)?
        .ok_or_else(|| anyhow!("witness dual-signing receipt not found"))?;
    validate_receipt_request(&receipt, request)?;
    validate_stored_receipt(authority, &receipt)?;
    let receipt_hash = receipt.hash_sha256()?;
    let slot_key = slot_key(
        tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
    )?;
    let slot = read_slot_row(authority.storage, &slot_key)?
        .ok_or_else(|| anyhow!("proposal slot not found"))?;
    if reservation.state == ProposalAdmissionReservationStateV1::Committed {
        if reservation.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
            && reservation.terminal_commit_certificate_sha256
                == Some(receipt.signed_commit_certificate_sha256)
            && reservation.terminal_committed_head_sha256
                == Some(receipt.signed_committed_head_sha256)
            && slot.state == ProposalAdmissionSlotStateV1::Committed
            && slot.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
            && slot.terminal_committed_head_sha256 == Some(receipt.signed_committed_head_sha256)
        {
            return Ok(reservation);
        }
        bail!("proposal committed reservation is incomplete");
    }
    if reservation.state != ProposalAdmissionReservationStateV1::Signing
        || reservation.reservation_revision != request.signing_reservation_revision
        || reservation.witness_dual_signing_receipt_sha256 != Some(receipt_hash)
        || slot.reservation_id != identity.reservation_id
        || slot.state != ProposalAdmissionSlotStateV1::Signed
        || slot.witness_dual_signing_receipt_sha256 != Some(receipt_hash)
        || slot.terminal_committed_head_sha256 != Some(receipt.signed_committed_head_sha256)
    {
        bail!("proposal reservation cannot be committed from its current state");
    }

    let guard = load_group_guard(authority, tenant_id, &identity.database_id).await?;
    if identity.fencing_generation != guard.owner.fence_token
        || identity.fencing_generation != authority.write_permit.fence_token
        || identity.primary_server_id != guard.owner.owner_node_id
        || identity.primary_server_id != authority.write_permit.owner_node_id
        || identity.leader_lease_id != personaldb_group_leader_lease_id(&guard.owner)
        || identity.leader_lease_revision != guard.owner.recovery_epoch
    {
        bail!("proposal reservation leader lease is stale at commit finalisation");
    }
    let committed_head = decode_committed_head(&receipt.signed_committed_head)?;
    let current_head_bytes = encode_committed_head(&guard.head)?;
    if guard.head != committed_head
        || domain_hash(SIGNED_HEAD_HASH_DOMAIN, &current_head_bytes)
            != receipt.signed_committed_head_sha256
    {
        bail!("proposal witnessed head is not the current committed head");
    }
    let committed_certificate = decode_commit_certificate(&receipt.signed_commit_certificate)?;
    if domain_hash(
        SIGNED_CERTIFICATE_HASH_DOMAIN,
        &encode_commit_certificate(&committed_certificate)?,
    ) != receipt.signed_commit_certificate_sha256
    {
        bail!("proposal witnessed certificate hash does not match receipt");
    }

    let terminal_revision = request
        .signing_reservation_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation revision overflow"))?;
    let mut committed_reservation = reservation.clone();
    committed_reservation.reservation_revision = terminal_revision;
    committed_reservation.state = ProposalAdmissionReservationStateV1::Committed;
    committed_reservation.terminal_commit_certificate_sha256 =
        Some(receipt.signed_commit_certificate_sha256);
    committed_reservation.terminal_committed_head_sha256 =
        Some(receipt.signed_committed_head_sha256);

    let mut committed_slot = slot.clone();
    committed_slot.state = ProposalAdmissionSlotStateV1::Committed;
    committed_slot.slot_revision = committed_slot
        .slot_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal slot revision overflow"))?;
    validate_reservation(&committed_reservation)?;
    validate_slot(&committed_slot)?;

    let reservation_key = reservation_key(&identity.reservation_id)?;
    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let current_slot_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
        &slot_key,
    )?
    .ok_or_else(|| anyhow!("proposal slot disappeared"))?;
    let receipt_key = receipt_key(&identity.reservation_id)?;
    let receipt_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
        &receipt_key,
    )?
    .ok_or_else(|| anyhow!("witness receipt disappeared"))?;
    let transaction_id = format!("personaldb-admission-commit:{}", hex::encode(receipt_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &identity.database_id).await?;
    let common = row_common(
        tenant_id,
        &identity.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(authority.now_unix_seconds)?,
    );
    let reservation_payload = encode_deterministic_proto(&ReservationRowProto {
        common: Some(common.clone()),
        reservation: Some(reservation_to_proto(&committed_reservation)),
    });
    let slot_payload = encode_deterministic_proto(&SlotRowProto {
        common: Some(common),
        slot: Some(slot_to_proto(&committed_slot)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &identity.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition,
            guard.head_precondition,
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key.clone(),
                &current_reservation_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key.clone(),
                &current_slot_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
                receipt_key,
                &receipt_payload,
            ),
        ],
        vec![
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                reservation_payload,
            ),
            put_operation(
                tenant_id,
                &identity.database_id,
                TABLE_PERSONALDB_PROPOSAL_SLOT_ROW,
                slot_key,
                slot_payload,
            ),
        ],
    )
    .await?;
    Ok(
        read_personaldb_proposal_reservation(authority.storage, &request.reservation_id)?
            .ok_or_else(|| anyhow!("committed proposal reservation disappeared"))?
            .reservation,
    )
}

pub fn read_personaldb_proposal_reservation(
    storage: &Storage,
    reservation_id: &str,
) -> Result<Option<StoredProposalAdmissionReservationV1>> {
    let key = reservation_key(reservation_id)?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, &key)?
    else {
        return Ok(None);
    };
    let (common, reservation) = decode_reservation_row(&payload)?;
    let tenant_id = tenant_id_from_realm(&common.realm_id)?;
    validate_row_scope(&common, tenant_id, &reservation.identity.database_id)?;
    if reservation.identity.reservation_id != reservation_id {
        bail!("proposal reservation exact-key scope mismatch");
    }
    validate_reservation(&reservation)?;
    Ok(Some(StoredProposalAdmissionReservationV1 {
        tenant_id,
        reservation,
    }))
}

pub fn read_proposal_admission_slot(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
) -> Result<Option<ProposalAdmissionSlotV1>> {
    let key = slot_key(tenant_id, database_id, next_log_index, client_log_epoch)?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, &key)? else {
        return Ok(None);
    };
    let (common, slot) = decode_slot_row(&payload)?;
    validate_row_scope(&common, tenant_id, database_id)?;
    if slot.database_id != database_id
        || slot.next_log_index != next_log_index
        || slot.client_log_epoch != client_log_epoch
    {
        bail!("proposal slot exact-key scope mismatch");
    }
    validate_slot(&slot)?;
    Ok(Some(slot))
}

pub fn read_witness_signing_candidate(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
    reservation_id: &str,
) -> Result<Option<WitnessSigningCandidateV1>> {
    let key = candidate_key(
        tenant_id,
        database_id,
        next_log_index,
        client_log_epoch,
        reservation_id,
    )?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, &key)? else {
        return Ok(None);
    };
    let (common, candidate) = decode_candidate_row(&payload)?;
    validate_row_scope(&common, tenant_id, database_id)?;
    if candidate.database_id != database_id
        || candidate.next_log_index != next_log_index
        || candidate.client_log_epoch != client_log_epoch
        || candidate.reservation_id != reservation_id
    {
        bail!("witness candidate exact-key scope mismatch");
    }
    validate_candidate(&candidate)?;
    Ok(Some(candidate))
}

pub fn read_witness_dual_signing_receipt(
    storage: &Storage,
    reservation_id: &str,
) -> Result<Option<WitnessDualSigningReceiptV1>> {
    let key = receipt_key(reservation_id)?;
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_WITNESS_RECEIPT_ROW, &key)? else {
        return Ok(None);
    };
    let (_common, receipt) = decode_receipt_row(&payload)?;
    if receipt.reservation_id != reservation_id {
        bail!("witness receipt exact-key scope mismatch");
    }
    validate_receipt_shape(&receipt)?;
    Ok(Some(receipt))
}

struct GroupGuard {
    owner: PartitionOwnerState,
    head: PersonalDbCommittedHead,
    owner_precondition: CoreMutationPrecondition,
    head_precondition: CoreMutationPrecondition,
}

async fn load_group_guard(
    authority: &PersonalDbAdmissionAuthority<'_>,
    tenant_id: i64,
    database_id: &str,
) -> Result<GroupGuard> {
    validate_tenant_database(tenant_id, database_id)?;
    let expected_partition_id = personaldb_group_partition_owner_id(tenant_id, database_id)?;
    if authority.write_permit.partition_family != PERSONALDB_GROUP_PARTITION_FAMILY
        || authority.write_permit.partition_id != expected_partition_id
    {
        bail!("PersonalDB admission write permit targets a different group");
    }
    let owner = read_partition_owner(
        authority.storage,
        &authority.write_permit.partition_family,
        &authority.write_permit.partition_id,
        authority.partition_owner_signing_key,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB group leader lease is absent"))?;
    validate_write_permit_for_state(&owner, authority.write_permit, true)
        .map_err(|error| anyhow!("{error}"))?;
    let owner_precondition = partition_write_precondition(
        authority.storage,
        authority.write_permit,
        authority.partition_owner_signing_key,
    )
    .await
    .map_err(|error| anyhow!("{error}"))?;
    let head = read_personaldb_committed_head(
        authority.storage,
        tenant_id,
        database_id,
        authority.trust_store,
    )
    .await?
    .ok_or_else(|| anyhow!("PersonalDB group committed head is absent"))?;
    let head_precondition =
        personaldb_committed_head_precondition(authority.storage, tenant_id, database_id)?;
    Ok(GroupGuard {
        owner,
        head,
        owner_precondition,
        head_precondition,
    })
}

fn validate_identity_against_guard(
    authority: &PersonalDbAdmissionAuthority<'_>,
    identity: &ProposalAdmissionReservationIdentityV1,
    guard: &GroupGuard,
) -> Result<()> {
    validate_reservation_identity_shape(identity)?;
    if identity.expected_previous_log_index != guard.head.log_index
        || identity.expected_previous_log_hash_sha256
            != decode_hex32(&guard.head.log_hash, "committed head log_hash")?
    {
        bail!("proposal reservation expected head is stale");
    }
    if identity.membership_revision != guard.head.membership_epoch {
        bail!("proposal reservation membership revision is stale");
    }
    if identity.fencing_generation != guard.owner.fence_token
        || identity.fencing_generation != authority.write_permit.fence_token
    {
        bail!("proposal reservation fencing generation is stale");
    }
    if identity.primary_server_id != guard.owner.owner_node_id
        || identity.primary_server_id != authority.write_permit.owner_node_id
    {
        bail!("proposal reservation primary does not own the group lease");
    }
    if identity.leader_lease_id != personaldb_group_leader_lease_id(&guard.owner)
        || identity.leader_lease_revision != guard.owner.recovery_epoch
    {
        bail!("proposal reservation leader lease is stale");
    }
    if identity.issued_at_unix_seconds > authority.now_unix_seconds
        || identity.expires_at_unix_seconds <= authority.now_unix_seconds
    {
        bail!("proposal reservation is outside its active lifetime");
    }
    Ok(())
}

fn validate_candidate_objects(
    tenant_id: i64,
    identity: &ProposalAdmissionReservationIdentityV1,
    certificate: &PersonalDbCommitCertificate,
    head: &PersonalDbCommittedHead,
) -> Result<()> {
    let tenant = tenant_id.to_string();
    let previous_hash = hex::encode(identity.expected_previous_log_hash_sha256);
    let next_index = next_log_index(identity)?;
    if certificate.tenant_id != tenant
        || certificate.database_id != identity.database_id
        || certificate.log_index != next_index
        || certificate.previous_log_hash != previous_hash
        || certificate.changeset_payload_hash != hex::encode(identity.changeset_payload_hash_sha256)
        || certificate.client_log_epoch != identity.client_log_epoch
        || certificate.membership_epoch != identity.membership_revision
        || certificate.leader_replica_id != identity.primary_server_id
        || certificate.authz_revision != identity.authorization_revision
    {
        bail!("witness candidate certificate does not match reservation");
    }
    if head.tenant_id != tenant
        || head.database_id != identity.database_id
        || head.log_index != next_index
        || head.log_hash != certificate.entry_hash
        || head.membership_epoch != identity.membership_revision
        || head.updated_by_node != identity.primary_server_id
    {
        bail!("witness candidate committed-head template does not match reservation");
    }
    Ok(())
}

fn validate_candidate_against_reservation(
    candidate: &WitnessSigningCandidateV1,
    reservation: &ProposalAdmissionReservationV1,
) -> Result<()> {
    validate_candidate(candidate)?;
    let identity = &reservation.identity;
    if candidate.reservation_id != identity.reservation_id
        || candidate.signing_reservation_revision != reservation.reservation_revision
        || candidate.database_id != identity.database_id
        || candidate.next_log_index != next_log_index(identity)?
        || candidate.client_log_epoch != identity.client_log_epoch
        || candidate.fencing_generation != identity.fencing_generation
        || candidate.witness_key_id != identity.witness_key_id
        || candidate.witness_key_generation != identity.witness_key_generation
        || reservation.candidate_entry_core_sha256 != Some(candidate.candidate_entry_core_sha256)
        || reservation.candidate_unsigned_certificate_sha256
            != Some(candidate.candidate_unsigned_certificate_sha256)
        || reservation.candidate_head_template_sha256
            != Some(candidate.candidate_head_template_sha256)
        || reservation.witness_signing_candidate_sha256 != Some(candidate.hash_sha256()?)
    {
        bail!("witness candidate does not match signing reservation");
    }
    Ok(())
}

fn validate_receipt_request(
    receipt: &WitnessDualSigningReceiptV1,
    request: &SignCertificateAndHeadV1,
) -> Result<()> {
    if receipt.reservation_id != request.reservation_id
        || receipt.signing_reservation_revision != request.signing_reservation_revision
    {
        bail!("altered witness signing replay");
    }
    Ok(())
}

fn validate_stored_receipt(
    authority: &PersonalDbAdmissionAuthority<'_>,
    receipt: &WitnessDualSigningReceiptV1,
) -> Result<()> {
    validate_receipt_shape(receipt)?;
    let stored = read_personaldb_proposal_reservation(authority.storage, &receipt.reservation_id)?
        .ok_or_else(|| anyhow!("witness receipt reservation not found"))?;
    let reservation = stored.reservation;
    let identity = &reservation.identity;
    let receipt_hash = receipt.hash_sha256()?;
    let signing_reservation_matches = reservation.state
        == ProposalAdmissionReservationStateV1::Signing
        && reservation.reservation_revision == receipt.signing_reservation_revision;
    let terminal_revision = receipt
        .signing_reservation_revision
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation revision overflow"))?;
    let committed_reservation_matches = reservation.state
        == ProposalAdmissionReservationStateV1::Committed
        && reservation.reservation_revision == terminal_revision
        && reservation.witness_dual_signing_receipt_sha256 == Some(receipt_hash)
        && reservation.terminal_commit_certificate_sha256
            == Some(receipt.signed_commit_certificate_sha256)
        && reservation.terminal_committed_head_sha256 == Some(receipt.signed_committed_head_sha256);
    if (!signing_reservation_matches && !committed_reservation_matches)
        || identity.witness_key_id != receipt.witness_key_id
        || identity.witness_key_generation != receipt.witness_key_generation
    {
        bail!("stored witness receipt does not match reservation");
    }
    let candidate = read_witness_signing_candidate(
        authority.storage,
        stored.tenant_id,
        &identity.database_id,
        next_log_index(identity)?,
        identity.client_log_epoch,
        &identity.reservation_id,
    )?
    .ok_or_else(|| anyhow!("witness receipt candidate not found"))?;
    validate_receipt_against_candidate(receipt, &candidate)?;
    let certificate = decode_commit_certificate(&receipt.signed_commit_certificate)?;
    let head = decode_committed_head(&receipt.signed_committed_head)?;
    certificate.verify(authority.trust_store)?;
    head.verify(authority.trust_store)?;
    Ok(())
}

fn validate_receipt_against_candidate(
    receipt: &WitnessDualSigningReceiptV1,
    candidate: &WitnessSigningCandidateV1,
) -> Result<()> {
    validate_receipt_shape(receipt)?;
    if receipt.reservation_id != candidate.reservation_id
        || receipt.signing_reservation_revision != candidate.signing_reservation_revision
        || receipt.candidate_unsigned_certificate_sha256
            != candidate.candidate_unsigned_certificate_sha256
        || receipt.candidate_head_template_sha256 != candidate.candidate_head_template_sha256
        || receipt.witness_key_id != candidate.witness_key_id
        || receipt.witness_key_generation != candidate.witness_key_generation
        || receipt.unsigned_committed_head_sha256
            != domain_hash(UNSIGNED_HEAD_HASH_DOMAIN, &candidate.head_template)
        || receipt.signed_commit_certificate_sha256
            != domain_hash(
                SIGNED_CERTIFICATE_HASH_DOMAIN,
                &receipt.signed_commit_certificate,
            )
        || receipt.signed_committed_head_sha256
            != domain_hash(SIGNED_HEAD_HASH_DOMAIN, &receipt.signed_committed_head)
    {
        bail!("witness receipt does not match immutable candidate");
    }
    let certificate = decode_commit_certificate(&receipt.signed_commit_certificate)?;
    let head = decode_committed_head(&receipt.signed_committed_head)?;
    let certificate_signature = certificate
        .witness_signature
        .as_ref()
        .ok_or_else(|| anyhow!("witness receipt certificate signature missing"))?;
    let head_signature = head
        .head_signature
        .as_ref()
        .ok_or_else(|| anyhow!("witness receipt head signature missing"))?;
    let certificate_signature_sha256: [u8; 32] =
        Sha256::digest(certificate_signature.signature.as_bytes()).into();
    let head_signature_sha256: [u8; 32] =
        Sha256::digest(head_signature.signature.as_bytes()).into();
    if receipt.certificate_signature_sha256 != certificate_signature_sha256
        || receipt.head_signature_sha256 != head_signature_sha256
    {
        bail!("witness receipt signature hash mismatch");
    }
    Ok(())
}

async fn store_witness_receipt_create_absent(
    authority: &PersonalDbAdmissionAuthority<'_>,
    tenant_id: i64,
    candidate: &WitnessSigningCandidateV1,
    guard: &GroupGuard,
    receipt: &WitnessDualSigningReceiptV1,
) -> Result<()> {
    validate_receipt_against_candidate(receipt, candidate)?;
    let receipt_key = receipt_key(&receipt.reservation_id)?;
    if let Some(existing) =
        read_witness_dual_signing_receipt(authority.storage, &receipt.reservation_id)?
    {
        if existing == *receipt {
            return Ok(());
        }
        bail!("altered witness receipt replay");
    }
    let reservation_key = reservation_key(&receipt.reservation_id)?;
    let candidate_key = candidate_key(
        tenant_id,
        &candidate.database_id,
        candidate.next_log_index,
        candidate.client_log_epoch,
        &candidate.reservation_id,
    )?;
    let current_reservation_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
        &reservation_key,
    )?
    .ok_or_else(|| anyhow!("proposal reservation disappeared"))?;
    let current_candidate_payload = read_raw_row(
        authority.storage,
        TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
        &candidate_key,
    )?
    .ok_or_else(|| anyhow!("witness candidate disappeared"))?;
    let receipt_hash = receipt.hash_sha256()?;
    let transaction_id = format!("personaldb-witness:{}", hex::encode(receipt_hash));
    let root_generation =
        next_group_root_generation(authority.storage, tenant_id, &candidate.database_id).await?;
    let common = row_common(
        tenant_id,
        &candidate.database_id,
        root_generation,
        &transaction_id,
        unix_seconds_to_nanos(receipt.signed_at_unix_seconds)?,
    );
    let receipt_payload = encode_deterministic_proto(&ReceiptRowProto {
        common: Some(common),
        receipt: Some(receipt_to_proto(receipt)),
    });
    commit_group_batch(
        authority.storage,
        transaction_id,
        tenant_id,
        &candidate.database_id,
        &authority.write_permit.owner_node_id,
        vec![
            guard.owner_precondition.clone(),
            guard.head_precondition.clone(),
            exact_precondition(
                TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW,
                reservation_key,
                &current_reservation_payload,
            ),
            exact_precondition(
                TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW,
                candidate_key,
                &current_candidate_payload,
            ),
            absent_precondition(TABLE_PERSONALDB_WITNESS_RECEIPT_ROW, receipt_key.clone()),
        ],
        vec![put_operation(
            tenant_id,
            &candidate.database_id,
            TABLE_PERSONALDB_WITNESS_RECEIPT_ROW,
            receipt_key,
            receipt_payload,
        )],
    )
    .await
}

fn exact_reservation_replay(
    tenant_id: i64,
    claim: &ProposalIdempotencyClaimIdentityV1,
    slot: &ProposalAdmissionSlotV1,
    reservation: &ProposalAdmissionReservationV1,
    existing_claim: Option<ProposalIdempotencyClaimIdentityV1>,
    existing_slot: Option<ProposalAdmissionSlotV1>,
    existing_reservation: Option<(i64, ProposalAdmissionReservationV1)>,
) -> Result<ProposalAdmissionReservationV1> {
    if existing_claim.as_ref().is_some_and(|value| value != claim) {
        bail!("proposal idempotency claim conflict");
    }
    if existing_slot.as_ref().is_some_and(|value| value != slot) {
        bail!("proposal slot is occupied by another reservation");
    }
    if existing_reservation
        .as_ref()
        .is_some_and(|(stored_tenant, value)| *stored_tenant != tenant_id || value != reservation)
    {
        bail!("altered proposal reservation replay");
    }
    match (existing_claim, existing_slot, existing_reservation) {
        (Some(_), Some(_), Some((_, stored))) => Ok(stored),
        _ => bail!("partial proposal reservation state is a consistency fault"),
    }
}

fn admission_from_reservation(
    reservation: &ProposalAdmissionReservationV1,
) -> Result<ProposalAdmissionV1> {
    validate_reservation(reservation)?;
    let identity = &reservation.identity;
    let admission = ProposalAdmissionV1 {
        format_version: FORMAT_VERSION_V1,
        database_id: identity.database_id.clone(),
        group_kind: identity.group_kind.clone(),
        proposer_id: identity.proposer_id.clone(),
        client_proposal_hash_sha256: identity.client_proposal_hash_sha256,
        workflow_id: identity.workflow_id.clone(),
        fencing_generation: identity.fencing_generation,
        authorization_revision: identity.authorization_revision,
        idempotency_claim_sha256: identity.idempotency_claim_sha256,
        issued_at_unix_seconds: identity.issued_at_unix_seconds,
        expires_at_unix_seconds: identity.expires_at_unix_seconds,
        reservation_id: identity.reservation_id.clone(),
        reservation_revision: reservation.reservation_revision,
        reservation_identity_sha256: identity.hash_sha256()?,
        authorization_receipt_sha256: identity.authorization_receipt_sha256,
        selected_voter_ids: identity.selected_voter_ids.clone(),
        primary_server_id: identity.primary_server_id.clone(),
        proposal_admission_key_id: identity.proposal_admission_key_id.clone(),
        proposal_admission_generation: identity.proposal_admission_generation,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
    };
    validate_admission_shape(&admission)?;
    Ok(admission)
}

fn slot_from_reservation(
    reservation: &ProposalAdmissionReservationV1,
) -> Result<ProposalAdmissionSlotV1> {
    let identity = &reservation.identity;
    let slot = ProposalAdmissionSlotV1 {
        format_version: FORMAT_VERSION_V1,
        database_id: identity.database_id.clone(),
        next_log_index: next_log_index(identity)?,
        expected_previous_log_hash_sha256: identity.expected_previous_log_hash_sha256,
        placement_epoch: identity.placement_epoch,
        client_log_epoch: identity.client_log_epoch,
        fencing_generation: identity.fencing_generation,
        slot_revision: 1,
        reservation_id: identity.reservation_id.clone(),
        state: ProposalAdmissionSlotStateV1::Reserved,
        witness_dual_signing_receipt_sha256: None,
        terminal_committed_head_sha256: None,
    };
    validate_slot(&slot)?;
    Ok(slot)
}

fn validate_claim(claim: &ProposalIdempotencyClaimIdentityV1) -> Result<()> {
    require_version(claim.format_version, "proposal idempotency claim")?;
    validate_text(&claim.tenant_id, "tenant_id")?;
    claim
        .tenant_id
        .parse::<i64>()
        .context("proposal idempotency claim tenant_id must be a nonnegative integer")
        .and_then(|tenant_id| {
            if tenant_id < 0 {
                bail!("proposal idempotency claim tenant_id must be nonnegative")
            }
            Ok(())
        })?;
    for (field, value) in [
        ("application_id", claim.application_id.as_str()),
        ("operation_id", claim.operation_id.as_str()),
        ("request_id", claim.request_id.as_str()),
        ("database_id", claim.database_id.as_str()),
        ("workflow_id", claim.workflow_id.as_str()),
    ] {
        validate_text(value, field)?;
    }
    if claim.fencing_generation == 0 {
        bail!("proposal idempotency claim fencing generation must be nonzero");
    }
    Ok(())
}

fn validate_reservation_identity_shape(
    identity: &ProposalAdmissionReservationIdentityV1,
) -> Result<()> {
    require_version(identity.format_version, "proposal reservation identity")?;
    validate_reservation_id(&identity.reservation_id)?;
    for (field, value) in [
        ("database_id", identity.database_id.as_str()),
        ("group_kind", identity.group_kind.as_str()),
        ("proposer_id", identity.proposer_id.as_str()),
        ("workflow_id", identity.workflow_id.as_str()),
        ("leader_lease_id", identity.leader_lease_id.as_str()),
        ("primary_server_id", identity.primary_server_id.as_str()),
    ] {
        validate_text(value, field)?;
    }
    if !matches!(
        identity.group_kind.as_str(),
        "source" | "projection" | "standalone"
    ) {
        bail!("proposal reservation group_kind is unsupported");
    }
    for (field, value) in [
        ("membership_revision", identity.membership_revision),
        ("placement_epoch", identity.placement_epoch),
        ("client_log_epoch", identity.client_log_epoch),
        ("fencing_generation", identity.fencing_generation),
        ("leader_lease_revision", identity.leader_lease_revision),
        (
            "proposal_admission_generation",
            identity.proposal_admission_generation,
        ),
        ("witness_key_generation", identity.witness_key_generation),
    ] {
        if value == 0 {
            bail!("proposal reservation {field} must be nonzero");
        }
    }
    if identity.issued_at_unix_seconds < 0
        || identity.expires_at_unix_seconds <= identity.issued_at_unix_seconds
        || identity
            .expires_at_unix_seconds
            .saturating_sub(identity.issued_at_unix_seconds)
            > MAX_ADMISSION_LIFETIME_SECONDS
    {
        bail!("proposal reservation lifetime is invalid");
    }
    validate_sorted_unique_text(&identity.selected_voter_ids, "selected_voter_ids")?;
    KeyId::new(identity.proposal_admission_key_id.clone())?;
    KeyGeneration::new(identity.proposal_admission_generation)?;
    KeyId::new(identity.witness_key_id.clone())?;
    KeyGeneration::new(identity.witness_key_generation)?;
    Ok(())
}

fn validate_reservation(reservation: &ProposalAdmissionReservationV1) -> Result<()> {
    validate_reservation_identity_shape(&reservation.identity)?;
    if reservation.reservation_revision == 0
        || reservation.state == ProposalAdmissionReservationStateV1::Unspecified
    {
        bail!("proposal reservation state or revision is invalid");
    }
    match reservation.state {
        ProposalAdmissionReservationStateV1::Reserved => {
            if reservation.reservation_revision != 1
                || reservation.candidate_entry_core_sha256.is_some()
                || reservation.candidate_unsigned_certificate_sha256.is_some()
                || reservation.candidate_head_template_sha256.is_some()
                || reservation
                    .candidate_unsigned_committed_head_sha256
                    .is_some()
                || reservation.witness_dual_signing_receipt_sha256.is_some()
                || reservation.witness_signing_candidate_sha256.is_some()
            {
                bail!("reserved proposal reservation contains signing state");
            }
        }
        ProposalAdmissionReservationStateV1::Signing => {
            if reservation.reservation_revision < 2
                || reservation.candidate_entry_core_sha256.is_none()
                || reservation.candidate_unsigned_certificate_sha256.is_none()
                || reservation.candidate_head_template_sha256.is_none()
                || reservation.witness_signing_candidate_sha256.is_none()
            {
                bail!("signing proposal reservation is missing immutable candidate state");
            }
            if reservation.witness_dual_signing_receipt_sha256.is_some()
                != reservation
                    .candidate_unsigned_committed_head_sha256
                    .is_some()
            {
                bail!("proposal reservation receipt acknowledgement is partial");
            }
        }
        ProposalAdmissionReservationStateV1::Committed => {
            if reservation.witness_dual_signing_receipt_sha256.is_none()
                || reservation.terminal_commit_certificate_sha256.is_none()
                || reservation.terminal_committed_head_sha256.is_none()
            {
                bail!("committed proposal reservation is missing terminal hashes");
            }
        }
        ProposalAdmissionReservationStateV1::Revoked
        | ProposalAdmissionReservationStateV1::Expired => {
            if reservation.witness_dual_signing_receipt_sha256.is_some() {
                bail!("signed proposal reservation cannot be revoked or expired");
            }
        }
        ProposalAdmissionReservationStateV1::Unspecified => {
            bail!("proposal reservation state is unspecified")
        }
    }
    Ok(())
}

fn validate_slot(slot: &ProposalAdmissionSlotV1) -> Result<()> {
    require_version(slot.format_version, "proposal slot")?;
    validate_text(&slot.database_id, "database_id")?;
    validate_reservation_id(&slot.reservation_id)?;
    if slot.next_log_index == 0
        || slot.placement_epoch == 0
        || slot.client_log_epoch == 0
        || slot.fencing_generation == 0
        || slot.slot_revision == 0
        || slot.state == ProposalAdmissionSlotStateV1::Unspecified
    {
        bail!("proposal slot contains a zero state boundary");
    }
    match slot.state {
        ProposalAdmissionSlotStateV1::Reserved => {
            if slot.witness_dual_signing_receipt_sha256.is_some()
                || slot.terminal_committed_head_sha256.is_some()
            {
                bail!("reserved proposal slot contains signed state");
            }
        }
        ProposalAdmissionSlotStateV1::Signed | ProposalAdmissionSlotStateV1::Committed => {
            if slot.witness_dual_signing_receipt_sha256.is_none()
                || slot.terminal_committed_head_sha256.is_none()
            {
                bail!("signed proposal slot is missing receipt state");
            }
        }
        ProposalAdmissionSlotStateV1::ReleasedUnsigned => {
            if slot.witness_dual_signing_receipt_sha256.is_some()
                || slot.terminal_committed_head_sha256.is_some()
            {
                bail!("released proposal slot contains signed state");
            }
        }
        ProposalAdmissionSlotStateV1::Unspecified => bail!("proposal slot state is unspecified"),
    }
    Ok(())
}

fn validate_candidate(candidate: &WitnessSigningCandidateV1) -> Result<()> {
    require_version(candidate.format_version, "witness signing candidate")?;
    validate_reservation_id(&candidate.reservation_id)?;
    validate_text(&candidate.database_id, "database_id")?;
    KeyId::new(candidate.witness_key_id.clone())?;
    KeyGeneration::new(candidate.witness_key_generation)?;
    if candidate.signing_reservation_revision < 2
        || candidate.next_log_index == 0
        || candidate.client_log_epoch == 0
        || candidate.fencing_generation == 0
        || candidate.created_at_unix_seconds < 0
    {
        bail!("witness signing candidate contains an invalid boundary");
    }
    ensure_bounded_object(
        &candidate.unsigned_commit_certificate,
        "unsigned commit certificate",
    )?;
    ensure_bounded_object(&candidate.head_template, "witness head template")?;
    if candidate.candidate_unsigned_certificate_sha256
        != domain_hash(
            UNSIGNED_CERTIFICATE_HASH_DOMAIN,
            &candidate.unsigned_commit_certificate,
        )
        || candidate.candidate_head_template_sha256
            != domain_hash(HEAD_TEMPLATE_HASH_DOMAIN, &candidate.head_template)
    {
        bail!("witness signing candidate component hash mismatch");
    }
    Ok(())
}

fn validate_receipt_shape(receipt: &WitnessDualSigningReceiptV1) -> Result<()> {
    require_version(receipt.format_version, "witness dual-signing receipt")?;
    validate_reservation_id(&receipt.reservation_id)?;
    KeyId::new(receipt.witness_key_id.clone())?;
    KeyGeneration::new(receipt.witness_key_generation)?;
    if receipt.signing_reservation_revision < 2 || receipt.signed_at_unix_seconds < 0 {
        bail!("witness receipt contains an invalid boundary");
    }
    ensure_bounded_object(
        &receipt.signed_commit_certificate,
        "signed commit certificate",
    )?;
    ensure_bounded_object(&receipt.signed_committed_head, "signed committed head")?;
    Ok(())
}

fn validate_admission_shape(admission: &ProposalAdmissionV1) -> Result<()> {
    require_version(admission.format_version, "proposal admission")?;
    for (field, value) in [
        ("database_id", admission.database_id.as_str()),
        ("group_kind", admission.group_kind.as_str()),
        ("proposer_id", admission.proposer_id.as_str()),
        ("workflow_id", admission.workflow_id.as_str()),
        ("primary_server_id", admission.primary_server_id.as_str()),
    ] {
        validate_text(value, field)?;
    }
    validate_reservation_id(&admission.reservation_id)?;
    validate_sorted_unique_text(&admission.selected_voter_ids, "selected_voter_ids")?;
    KeyId::new(admission.proposal_admission_key_id.clone())?;
    KeyGeneration::new(admission.proposal_admission_generation)?;
    KeyId::new(admission.witness_key_id.clone())?;
    KeyGeneration::new(admission.witness_key_generation)?;
    if admission.fencing_generation == 0
        || admission.reservation_revision == 0
        || admission.issued_at_unix_seconds < 0
        || admission.expires_at_unix_seconds <= admission.issued_at_unix_seconds
        || admission
            .expires_at_unix_seconds
            .saturating_sub(admission.issued_at_unix_seconds)
            > MAX_ADMISSION_LIFETIME_SECONDS
    {
        bail!("proposal admission contains an invalid boundary");
    }
    Ok(())
}

fn validate_active_key_binding(
    trust_store: &PublicKeyTrustStore,
    key_id: &str,
    generation: u64,
    purpose: SignaturePurpose,
    database_id: &str,
    log_index: u64,
) -> Result<()> {
    let key_id = KeyId::new(key_id.to_string())?;
    let record = trust_store
        .get(&key_id)
        .ok_or_else(|| anyhow!("bound {purpose} key is absent from the trust store"))?;
    validate_bound_record(record, purpose, generation, database_id, log_index)
}

fn validate_bound_signer(
    record: &PublicKeyTrustRecord,
    purpose: SignaturePurpose,
    key_id: &str,
    generation: u64,
    database_id: &str,
    log_index: u64,
) -> Result<()> {
    if record.key_id.as_str() != key_id {
        bail!("{purpose} signer key does not match reservation");
    }
    validate_bound_record(record, purpose, generation, database_id, log_index)
}

fn validate_bound_record(
    record: &PublicKeyTrustRecord,
    purpose: SignaturePurpose,
    generation: u64,
    database_id: &str,
    log_index: u64,
) -> Result<()> {
    record.validate()?;
    if record.purpose != purpose
        || record.key_generation.get() != generation
        || record.status != PublicKeyStatus::Active
        || log_index < record.valid_from_log_index
        || record
            .valid_until_log_index
            .is_some_and(|until| log_index >= until)
    {
        bail!("bound {purpose} key is not active at the proposal boundary");
    }
    let database = DatabaseId::new(database_id);
    if !record.database_scopes.is_empty() && !record.database_scopes.contains(&database) {
        bail!("bound {purpose} key is outside the database scope");
    }
    if !record.group_scopes.is_empty()
        && !record.group_scopes.iter().any(|group| group == database_id)
    {
        bail!("bound {purpose} key is outside the group scope");
    }
    Ok(())
}

fn require_unsigned_certificate(certificate: &PersonalDbCommitCertificate) -> Result<()> {
    if certificate.certificate_hash.is_some() || certificate.witness_signature.is_some() {
        bail!("witness candidate commit certificate must be unsigned");
    }
    Ok(())
}

fn require_unsigned_head(head: &PersonalDbCommittedHead) -> Result<()> {
    if head.head_hash.is_some() || head.head_signature.is_some() {
        bail!("witness candidate committed-head template must be unsigned");
    }
    Ok(())
}

fn require_version(version: u32, object: &str) -> Result<()> {
    if version != FORMAT_VERSION_V1 {
        bail!("{object} has unsupported format version");
    }
    Ok(())
}

fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        bail!("{field} is empty or outside the canonical text bound");
    }
    Ok(())
}

fn validate_sorted_unique_text(values: &[String], field: &str) -> Result<()> {
    if values.is_empty() {
        bail!("{field} must not be empty");
    }
    for value in values {
        validate_text(value, field)?;
    }
    if values.windows(2).any(|pair| pair[0] >= pair[1]) {
        bail!("{field} must be strictly ascending and unique");
    }
    Ok(())
}

fn validate_reservation_id(value: &str) -> Result<()> {
    KeyId::new(value.to_string())
        .map(|_| ())
        .context("proposal reservation ID must be canonical sha256")
}

fn validate_tenant_database(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        bail!("PersonalDB tenant ID must be nonnegative");
    }
    validate_text(database_id, "database_id")
}

fn parse_claim_tenant_id(claim: &ProposalIdempotencyClaimIdentityV1) -> Result<i64> {
    let tenant_id = claim
        .tenant_id
        .parse::<i64>()
        .context("parse proposal idempotency claim tenant_id")?;
    if tenant_id < 0 {
        bail!("proposal idempotency claim tenant_id must be nonnegative");
    }
    Ok(tenant_id)
}

fn next_log_index(identity: &ProposalAdmissionReservationIdentityV1) -> Result<u64> {
    identity
        .expected_previous_log_index
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation log index overflow"))
}

fn ensure_bounded_object(bytes: &[u8], name: &str) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_SIGNING_OBJECT_BYTES {
        bail!("{name} is outside the signing object bound");
    }
    Ok(())
}

fn decode_hex32(value: &str, field: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(value).with_context(|| format!("decode {field} hex"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("{field} must be exactly 32 bytes"))
}

async fn next_group_root_generation(
    storage: &Storage,
    tenant_id: i64,
    database_id: &str,
) -> Result<u64> {
    let root_key_hash = personaldb_root_key_hash(tenant_id, database_id);
    CoreMetaStore::open(storage.core_store_meta_path())?
        .scan_all_encoded_rows()?
        .into_iter()
        .filter(|row| row.root_key_hash == root_key_hash)
        .map(|row| row.root_generation)
        .max()
        .ok_or_else(|| anyhow!("PersonalDB group root has no committed rows"))?
        .checked_add(1)
        .ok_or_else(|| anyhow!("PersonalDB group root generation overflow"))
}

async fn commit_group_batch(
    storage: &Storage,
    transaction_id: String,
    tenant_id: i64,
    database_id: &str,
    principal: &str,
    preconditions: Vec<CoreMutationPrecondition>,
    operations: Vec<CoreMutationOperation>,
) -> Result<()> {
    let receipt = CoreStore::new(storage.clone())
        .await?
        .commit_mutation_batch(CoreMutationBatch {
            transaction_id,
            scope_partition: personaldb_partition_id(tenant_id, database_id),
            committed_by_principal: principal.to_string(),
            preconditions,
            operations,
        })
        .await?;
    ensure_committed_receipt(&receipt)
}

fn ensure_committed_receipt(receipt: &CoreMutationBatchReceipt) -> Result<()> {
    if receipt.state != CoreTransactionState::Committed {
        bail!(
            "PersonalDB admission CoreStore transaction {} did not commit: {}",
            receipt.transaction_id,
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation failure")
        );
    }
    Ok(())
}

fn put_operation(
    tenant_id: i64,
    database_id: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: personaldb_partition_id(tenant_id, database_id),
        cf: CF_PERSONALDB.to_string(),
        table_id,
        tuple_key,
        payload,
    }
}

fn absent_precondition(table_id: u16, tuple_key: Vec<u8>) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_PERSONALDB.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: None,
        require_absent: true,
        require_present: false,
    }
}

fn exact_precondition(
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: &[u8],
) -> CoreMutationPrecondition {
    CoreMutationPrecondition::CoreMetaRow {
        cf: CF_PERSONALDB.to_string(),
        table_id,
        tuple_key,
        expected_payload_hash: Some(core_meta_payload_digest(table_id, payload)),
        require_absent: false,
        require_present: true,
    }
}

fn read_raw_row(storage: &Storage, table_id: u16, key: &[u8]) -> Result<Option<Vec<u8>>> {
    CoreMetaStore::open(storage.core_store_meta_path())?.get(CF_PERSONALDB, table_id, key)
}

fn read_claim_row(
    storage: &Storage,
    key: &[u8],
) -> Result<Option<ProposalIdempotencyClaimIdentityV1>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_CLAIM_ROW, key)? else {
        return Ok(None);
    };
    let row =
        decode_deterministic_proto::<ClaimRowProto>(&payload, "proposal idempotency claim row")?;
    let claim = claim_from_proto(
        row.claim
            .ok_or_else(|| anyhow!("proposal idempotency claim row missing claim"))?,
    )?;
    let common = row
        .common
        .ok_or_else(|| anyhow!("proposal idempotency claim row missing CoreMeta common"))?;
    validate_row_scope(&common, parse_claim_tenant_id(&claim)?, &claim.database_id)?;
    validate_claim(&claim)?;
    Ok(Some(claim))
}

fn read_slot_row(storage: &Storage, key: &[u8]) -> Result<Option<ProposalAdmissionSlotV1>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_SLOT_ROW, key)? else {
        return Ok(None);
    };
    let (_common, slot) = decode_slot_row(&payload)?;
    validate_slot(&slot)?;
    Ok(Some(slot))
}

fn read_reservation_row(
    storage: &Storage,
    key: &[u8],
) -> Result<Option<(i64, ProposalAdmissionReservationV1)>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_PROPOSAL_RESERVATION_ROW, key)?
    else {
        return Ok(None);
    };
    let (common, reservation) = decode_reservation_row(&payload)?;
    let tenant_id = tenant_id_from_realm(&common.realm_id)?;
    validate_row_scope(&common, tenant_id, &reservation.identity.database_id)?;
    validate_reservation(&reservation)?;
    Ok(Some((tenant_id, reservation)))
}

fn read_candidate_row(storage: &Storage, key: &[u8]) -> Result<Option<WitnessSigningCandidateV1>> {
    let Some(payload) = read_raw_row(storage, TABLE_PERSONALDB_WITNESS_CANDIDATE_ROW, key)? else {
        return Ok(None);
    };
    let (_common, candidate) = decode_candidate_row(&payload)?;
    validate_candidate(&candidate)?;
    Ok(Some(candidate))
}

fn decode_slot_row(payload: &[u8]) -> Result<(CoreMetaRowCommonProto, ProposalAdmissionSlotV1)> {
    let row = decode_deterministic_proto::<SlotRowProto>(payload, "proposal admission slot row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("proposal slot row missing CoreMeta common"))?,
        slot_from_proto(
            row.slot
                .ok_or_else(|| anyhow!("proposal slot row missing slot"))?,
        )?,
    ))
}

fn decode_reservation_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, ProposalAdmissionReservationV1)> {
    let row =
        decode_deterministic_proto::<ReservationRowProto>(payload, "proposal reservation row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("proposal reservation row missing CoreMeta common"))?,
        reservation_from_proto(
            row.reservation
                .ok_or_else(|| anyhow!("proposal reservation row missing reservation"))?,
        )?,
    ))
}

fn decode_candidate_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, WitnessSigningCandidateV1)> {
    let row =
        decode_deterministic_proto::<CandidateRowProto>(payload, "witness signing candidate row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("witness candidate row missing CoreMeta common"))?,
        candidate_from_proto(
            row.candidate
                .ok_or_else(|| anyhow!("witness candidate row missing candidate"))?,
        )?,
    ))
}

fn decode_receipt_row(
    payload: &[u8],
) -> Result<(CoreMetaRowCommonProto, WitnessDualSigningReceiptV1)> {
    let row =
        decode_deterministic_proto::<ReceiptRowProto>(payload, "witness dual-signing receipt row")?;
    Ok((
        row.common
            .ok_or_else(|| anyhow!("witness receipt row missing CoreMeta common"))?,
        receipt_from_proto(
            row.receipt
                .ok_or_else(|| anyhow!("witness receipt row missing receipt"))?,
        )?,
    ))
}

fn row_common(
    tenant_id: i64,
    database_id: &str,
    root_generation: u64,
    transaction_id: &str,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        personaldb_realm_id(tenant_id),
        personaldb_root_key_hash(tenant_id, database_id),
        root_generation,
        transaction_id.to_string(),
        created_at_unix_nanos,
    )
}

fn validate_row_scope(
    common: &CoreMetaRowCommonProto,
    tenant_id: i64,
    database_id: &str,
) -> Result<()> {
    if common.realm_id != personaldb_realm_id(tenant_id)
        || common.root_key_hash != personaldb_root_key_hash(tenant_id, database_id)
        || common.root_generation == 0
    {
        bail!("PersonalDB admission CoreMeta row scope mismatch");
    }
    Ok(())
}

fn claim_key(claim: &ProposalIdempotencyClaimIdentityV1) -> Result<Vec<u8>> {
    let mut hasher = Sha256::new();
    for part in [
        claim.tenant_id.as_bytes(),
        claim.application_id.as_bytes(),
        claim.operation_id.as_bytes(),
        claim.request_id.as_bytes(),
        claim.database_id.as_bytes(),
    ] {
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    let digest = format!("sha256:{}", hex::encode(hasher.finalize()));
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CLAIM_KEY_PREFIX),
        CoreMetaTuplePart::Hash(&digest),
    ])
}

fn slot_key(
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
) -> Result<Vec<u8>> {
    validate_tenant_database(tenant_id, database_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(SLOT_KEY_PREFIX),
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(database_id),
        CoreMetaTuplePart::U64(next_log_index),
        CoreMetaTuplePart::U64(client_log_epoch),
    ])
}

fn reservation_key(reservation_id: &str) -> Result<Vec<u8>> {
    validate_reservation_id(reservation_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(RESERVATION_KEY_PREFIX),
        CoreMetaTuplePart::Hash(reservation_id),
    ])
}

fn candidate_key(
    tenant_id: i64,
    database_id: &str,
    next_log_index: u64,
    client_log_epoch: u64,
    reservation_id: &str,
) -> Result<Vec<u8>> {
    validate_tenant_database(tenant_id, database_id)?;
    validate_reservation_id(reservation_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(CANDIDATE_KEY_PREFIX),
        CoreMetaTuplePart::Utf8(&personaldb_realm_id(tenant_id)),
        CoreMetaTuplePart::Utf8(database_id),
        CoreMetaTuplePart::U64(next_log_index),
        CoreMetaTuplePart::U64(client_log_epoch),
        CoreMetaTuplePart::Hash(reservation_id),
    ])
}

fn receipt_key(reservation_id: &str) -> Result<Vec<u8>> {
    validate_reservation_id(reservation_id)?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(RECEIPT_KEY_PREFIX),
        CoreMetaTuplePart::Hash(reservation_id),
    ])
}

fn domain_hash(domain: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(domain);
    hasher.update(bytes);
    hasher.finalize().into()
}

fn unix_seconds_to_nanos(seconds: i64) -> Result<u64> {
    let seconds = u64::try_from(seconds).context("protocol timestamp must be nonnegative")?;
    seconds
        .checked_mul(1_000_000_000)
        .ok_or_else(|| anyhow!("protocol timestamp nanoseconds overflow"))
}

fn claim_to_proto(
    claim: &ProposalIdempotencyClaimIdentityV1,
) -> ProposalIdempotencyClaimIdentityProto {
    ProposalIdempotencyClaimIdentityProto {
        format_version: claim.format_version,
        tenant_id: claim.tenant_id.clone(),
        application_id: claim.application_id.clone(),
        operation_id: claim.operation_id.clone(),
        request_id: claim.request_id.clone(),
        database_id: claim.database_id.clone(),
        client_proposal_hash_sha256: claim.client_proposal_hash_sha256.to_vec(),
        changeset_payload_hash_sha256: claim.changeset_payload_hash_sha256.to_vec(),
        workflow_id: claim.workflow_id.clone(),
        fencing_generation: claim.fencing_generation,
    }
}

fn claim_from_proto(
    proto: ProposalIdempotencyClaimIdentityProto,
) -> Result<ProposalIdempotencyClaimIdentityV1> {
    Ok(ProposalIdempotencyClaimIdentityV1 {
        format_version: proto.format_version,
        tenant_id: proto.tenant_id,
        application_id: proto.application_id,
        operation_id: proto.operation_id,
        request_id: proto.request_id,
        database_id: proto.database_id,
        client_proposal_hash_sha256: exact32(
            proto.client_proposal_hash_sha256,
            "client_proposal_hash_sha256",
        )?,
        changeset_payload_hash_sha256: exact32(
            proto.changeset_payload_hash_sha256,
            "changeset_payload_hash_sha256",
        )?,
        workflow_id: proto.workflow_id,
        fencing_generation: proto.fencing_generation,
    })
}

fn slot_to_proto(slot: &ProposalAdmissionSlotV1) -> ProposalAdmissionSlotProto {
    ProposalAdmissionSlotProto {
        format_version: slot.format_version,
        database_id: slot.database_id.clone(),
        next_log_index: slot.next_log_index,
        expected_previous_log_hash_sha256: slot.expected_previous_log_hash_sha256.to_vec(),
        placement_epoch: slot.placement_epoch,
        client_log_epoch: slot.client_log_epoch,
        fencing_generation: slot.fencing_generation,
        slot_revision: slot.slot_revision,
        reservation_id: slot.reservation_id.clone(),
        state: slot.state as i32,
        witness_dual_signing_receipt_sha256: optional32_to_vec(
            slot.witness_dual_signing_receipt_sha256,
        ),
        terminal_committed_head_sha256: optional32_to_vec(slot.terminal_committed_head_sha256),
    }
}

fn slot_from_proto(proto: ProposalAdmissionSlotProto) -> Result<ProposalAdmissionSlotV1> {
    Ok(ProposalAdmissionSlotV1 {
        format_version: proto.format_version,
        database_id: proto.database_id,
        next_log_index: proto.next_log_index,
        expected_previous_log_hash_sha256: exact32(
            proto.expected_previous_log_hash_sha256,
            "expected_previous_log_hash_sha256",
        )?,
        placement_epoch: proto.placement_epoch,
        client_log_epoch: proto.client_log_epoch,
        fencing_generation: proto.fencing_generation,
        slot_revision: proto.slot_revision,
        reservation_id: proto.reservation_id,
        state: ProposalAdmissionSlotStateV1::try_from(proto.state)
            .map_err(|_| anyhow!("proposal slot state is unknown"))?,
        witness_dual_signing_receipt_sha256: optional_exact32(
            proto.witness_dual_signing_receipt_sha256,
            "witness_dual_signing_receipt_sha256",
        )?,
        terminal_committed_head_sha256: optional_exact32(
            proto.terminal_committed_head_sha256,
            "terminal_committed_head_sha256",
        )?,
    })
}

fn reservation_identity_to_proto(
    identity: &ProposalAdmissionReservationIdentityV1,
) -> ProposalAdmissionReservationIdentityProto {
    ProposalAdmissionReservationIdentityProto {
        format_version: identity.format_version,
        reservation_id: identity.reservation_id.clone(),
        database_id: identity.database_id.clone(),
        group_kind: identity.group_kind.clone(),
        proposer_id: identity.proposer_id.clone(),
        client_proposal_hash_sha256: identity.client_proposal_hash_sha256.to_vec(),
        changeset_payload_hash_sha256: identity.changeset_payload_hash_sha256.to_vec(),
        expected_previous_log_index: identity.expected_previous_log_index,
        expected_previous_log_hash_sha256: identity.expected_previous_log_hash_sha256.to_vec(),
        membership_revision: identity.membership_revision,
        placement_epoch: identity.placement_epoch,
        client_log_epoch: identity.client_log_epoch,
        workflow_id: identity.workflow_id.clone(),
        fencing_generation: identity.fencing_generation,
        leader_lease_id: identity.leader_lease_id.clone(),
        leader_lease_revision: identity.leader_lease_revision,
        authorization_receipt_sha256: identity.authorization_receipt_sha256.to_vec(),
        authorization_revision: identity.authorization_revision,
        idempotency_claim_sha256: identity.idempotency_claim_sha256.to_vec(),
        issued_at_unix_seconds: identity.issued_at_unix_seconds,
        expires_at_unix_seconds: identity.expires_at_unix_seconds,
        selected_voter_ids: identity.selected_voter_ids.clone(),
        primary_server_id: identity.primary_server_id.clone(),
        proposal_admission_key_id: identity.proposal_admission_key_id.clone(),
        proposal_admission_generation: identity.proposal_admission_generation,
        witness_key_id: identity.witness_key_id.clone(),
        witness_key_generation: identity.witness_key_generation,
    }
}

fn reservation_identity_from_proto(
    proto: ProposalAdmissionReservationIdentityProto,
) -> Result<ProposalAdmissionReservationIdentityV1> {
    Ok(ProposalAdmissionReservationIdentityV1 {
        format_version: proto.format_version,
        reservation_id: proto.reservation_id,
        database_id: proto.database_id,
        group_kind: proto.group_kind,
        proposer_id: proto.proposer_id,
        client_proposal_hash_sha256: exact32(
            proto.client_proposal_hash_sha256,
            "client_proposal_hash_sha256",
        )?,
        changeset_payload_hash_sha256: exact32(
            proto.changeset_payload_hash_sha256,
            "changeset_payload_hash_sha256",
        )?,
        expected_previous_log_index: proto.expected_previous_log_index,
        expected_previous_log_hash_sha256: exact32(
            proto.expected_previous_log_hash_sha256,
            "expected_previous_log_hash_sha256",
        )?,
        membership_revision: proto.membership_revision,
        placement_epoch: proto.placement_epoch,
        client_log_epoch: proto.client_log_epoch,
        workflow_id: proto.workflow_id,
        fencing_generation: proto.fencing_generation,
        leader_lease_id: proto.leader_lease_id,
        leader_lease_revision: proto.leader_lease_revision,
        authorization_receipt_sha256: exact32(
            proto.authorization_receipt_sha256,
            "authorization_receipt_sha256",
        )?,
        authorization_revision: proto.authorization_revision,
        idempotency_claim_sha256: exact32(
            proto.idempotency_claim_sha256,
            "idempotency_claim_sha256",
        )?,
        issued_at_unix_seconds: proto.issued_at_unix_seconds,
        expires_at_unix_seconds: proto.expires_at_unix_seconds,
        selected_voter_ids: proto.selected_voter_ids,
        primary_server_id: proto.primary_server_id,
        proposal_admission_key_id: proto.proposal_admission_key_id,
        proposal_admission_generation: proto.proposal_admission_generation,
        witness_key_id: proto.witness_key_id,
        witness_key_generation: proto.witness_key_generation,
    })
}

fn reservation_to_proto(
    reservation: &ProposalAdmissionReservationV1,
) -> ProposalAdmissionReservationProto {
    ProposalAdmissionReservationProto {
        identity: Some(reservation_identity_to_proto(&reservation.identity)),
        reservation_revision: reservation.reservation_revision,
        state: reservation.state as i32,
        candidate_entry_core_sha256: optional32_to_vec(reservation.candidate_entry_core_sha256),
        candidate_unsigned_certificate_sha256: optional32_to_vec(
            reservation.candidate_unsigned_certificate_sha256,
        ),
        candidate_head_template_sha256: optional32_to_vec(
            reservation.candidate_head_template_sha256,
        ),
        candidate_unsigned_committed_head_sha256: optional32_to_vec(
            reservation.candidate_unsigned_committed_head_sha256,
        ),
        witness_dual_signing_receipt_sha256: optional32_to_vec(
            reservation.witness_dual_signing_receipt_sha256,
        ),
        terminal_commit_certificate_sha256: optional32_to_vec(
            reservation.terminal_commit_certificate_sha256,
        ),
        terminal_committed_head_sha256: optional32_to_vec(
            reservation.terminal_committed_head_sha256,
        ),
        witness_signing_candidate_sha256: optional32_to_vec(
            reservation.witness_signing_candidate_sha256,
        ),
    }
}

fn reservation_from_proto(
    proto: ProposalAdmissionReservationProto,
) -> Result<ProposalAdmissionReservationV1> {
    Ok(ProposalAdmissionReservationV1 {
        identity: reservation_identity_from_proto(
            proto
                .identity
                .ok_or_else(|| anyhow!("proposal reservation identity is missing"))?,
        )?,
        reservation_revision: proto.reservation_revision,
        state: ProposalAdmissionReservationStateV1::try_from(proto.state)
            .map_err(|_| anyhow!("proposal reservation state is unknown"))?,
        candidate_entry_core_sha256: optional_exact32(
            proto.candidate_entry_core_sha256,
            "candidate_entry_core_sha256",
        )?,
        candidate_unsigned_certificate_sha256: optional_exact32(
            proto.candidate_unsigned_certificate_sha256,
            "candidate_unsigned_certificate_sha256",
        )?,
        candidate_head_template_sha256: optional_exact32(
            proto.candidate_head_template_sha256,
            "candidate_head_template_sha256",
        )?,
        candidate_unsigned_committed_head_sha256: optional_exact32(
            proto.candidate_unsigned_committed_head_sha256,
            "candidate_unsigned_committed_head_sha256",
        )?,
        witness_dual_signing_receipt_sha256: optional_exact32(
            proto.witness_dual_signing_receipt_sha256,
            "witness_dual_signing_receipt_sha256",
        )?,
        terminal_commit_certificate_sha256: optional_exact32(
            proto.terminal_commit_certificate_sha256,
            "terminal_commit_certificate_sha256",
        )?,
        terminal_committed_head_sha256: optional_exact32(
            proto.terminal_committed_head_sha256,
            "terminal_committed_head_sha256",
        )?,
        witness_signing_candidate_sha256: optional_exact32(
            proto.witness_signing_candidate_sha256,
            "witness_signing_candidate_sha256",
        )?,
    })
}

fn candidate_to_proto(candidate: &WitnessSigningCandidateV1) -> WitnessSigningCandidateProto {
    WitnessSigningCandidateProto {
        format_version: candidate.format_version,
        reservation_id: candidate.reservation_id.clone(),
        signing_reservation_revision: candidate.signing_reservation_revision,
        database_id: candidate.database_id.clone(),
        next_log_index: candidate.next_log_index,
        client_log_epoch: candidate.client_log_epoch,
        fencing_generation: candidate.fencing_generation,
        witness_key_id: candidate.witness_key_id.clone(),
        witness_key_generation: candidate.witness_key_generation,
        unsigned_commit_certificate: candidate.unsigned_commit_certificate.clone(),
        head_template: candidate.head_template.clone(),
        candidate_entry_core_sha256: candidate.candidate_entry_core_sha256.to_vec(),
        candidate_unsigned_certificate_sha256: candidate
            .candidate_unsigned_certificate_sha256
            .to_vec(),
        candidate_head_template_sha256: candidate.candidate_head_template_sha256.to_vec(),
        created_at_unix_seconds: candidate.created_at_unix_seconds,
    }
}

fn candidate_from_proto(proto: WitnessSigningCandidateProto) -> Result<WitnessSigningCandidateV1> {
    Ok(WitnessSigningCandidateV1 {
        format_version: proto.format_version,
        reservation_id: proto.reservation_id,
        signing_reservation_revision: proto.signing_reservation_revision,
        database_id: proto.database_id,
        next_log_index: proto.next_log_index,
        client_log_epoch: proto.client_log_epoch,
        fencing_generation: proto.fencing_generation,
        witness_key_id: proto.witness_key_id,
        witness_key_generation: proto.witness_key_generation,
        unsigned_commit_certificate: proto.unsigned_commit_certificate,
        head_template: proto.head_template,
        candidate_entry_core_sha256: exact32(
            proto.candidate_entry_core_sha256,
            "candidate_entry_core_sha256",
        )?,
        candidate_unsigned_certificate_sha256: exact32(
            proto.candidate_unsigned_certificate_sha256,
            "candidate_unsigned_certificate_sha256",
        )?,
        candidate_head_template_sha256: exact32(
            proto.candidate_head_template_sha256,
            "candidate_head_template_sha256",
        )?,
        created_at_unix_seconds: proto.created_at_unix_seconds,
    })
}

fn receipt_to_proto(receipt: &WitnessDualSigningReceiptV1) -> WitnessDualSigningReceiptProto {
    WitnessDualSigningReceiptProto {
        format_version: receipt.format_version,
        reservation_id: receipt.reservation_id.clone(),
        signing_reservation_revision: receipt.signing_reservation_revision,
        candidate_unsigned_certificate_sha256: receipt
            .candidate_unsigned_certificate_sha256
            .to_vec(),
        candidate_head_template_sha256: receipt.candidate_head_template_sha256.to_vec(),
        witness_key_id: receipt.witness_key_id.clone(),
        witness_key_generation: receipt.witness_key_generation,
        certificate_signature_sha256: receipt.certificate_signature_sha256.to_vec(),
        signed_commit_certificate_sha256: receipt.signed_commit_certificate_sha256.to_vec(),
        unsigned_committed_head_sha256: receipt.unsigned_committed_head_sha256.to_vec(),
        head_signature_sha256: receipt.head_signature_sha256.to_vec(),
        signed_committed_head_sha256: receipt.signed_committed_head_sha256.to_vec(),
        signed_at_unix_seconds: receipt.signed_at_unix_seconds,
        signed_commit_certificate: receipt.signed_commit_certificate.clone(),
        signed_committed_head: receipt.signed_committed_head.clone(),
    }
}

fn receipt_from_proto(
    proto: WitnessDualSigningReceiptProto,
) -> Result<WitnessDualSigningReceiptV1> {
    Ok(WitnessDualSigningReceiptV1 {
        format_version: proto.format_version,
        reservation_id: proto.reservation_id,
        signing_reservation_revision: proto.signing_reservation_revision,
        candidate_unsigned_certificate_sha256: exact32(
            proto.candidate_unsigned_certificate_sha256,
            "candidate_unsigned_certificate_sha256",
        )?,
        candidate_head_template_sha256: exact32(
            proto.candidate_head_template_sha256,
            "candidate_head_template_sha256",
        )?,
        witness_key_id: proto.witness_key_id,
        witness_key_generation: proto.witness_key_generation,
        certificate_signature_sha256: exact32(
            proto.certificate_signature_sha256,
            "certificate_signature_sha256",
        )?,
        signed_commit_certificate_sha256: exact32(
            proto.signed_commit_certificate_sha256,
            "signed_commit_certificate_sha256",
        )?,
        unsigned_committed_head_sha256: exact32(
            proto.unsigned_committed_head_sha256,
            "unsigned_committed_head_sha256",
        )?,
        head_signature_sha256: exact32(proto.head_signature_sha256, "head_signature_sha256")?,
        signed_committed_head_sha256: exact32(
            proto.signed_committed_head_sha256,
            "signed_committed_head_sha256",
        )?,
        signed_at_unix_seconds: proto.signed_at_unix_seconds,
        signed_commit_certificate: proto.signed_commit_certificate,
        signed_committed_head: proto.signed_committed_head,
    })
}

fn admission_to_proto(admission: &ProposalAdmissionV1) -> ProposalAdmissionProto {
    ProposalAdmissionProto {
        format_version: admission.format_version,
        database_id: admission.database_id.clone(),
        group_kind: admission.group_kind.clone(),
        proposer_id: admission.proposer_id.clone(),
        client_proposal_hash_sha256: admission.client_proposal_hash_sha256.to_vec(),
        workflow_id: admission.workflow_id.clone(),
        fencing_generation: admission.fencing_generation,
        authorization_revision: admission.authorization_revision,
        idempotency_claim_sha256: admission.idempotency_claim_sha256.to_vec(),
        issued_at_unix_seconds: admission.issued_at_unix_seconds,
        expires_at_unix_seconds: admission.expires_at_unix_seconds,
        reservation_id: admission.reservation_id.clone(),
        reservation_revision: admission.reservation_revision,
        reservation_identity_sha256: admission.reservation_identity_sha256.to_vec(),
        authorization_receipt_sha256: admission.authorization_receipt_sha256.to_vec(),
        selected_voter_ids: admission.selected_voter_ids.clone(),
        primary_server_id: admission.primary_server_id.clone(),
        proposal_admission_key_id: admission.proposal_admission_key_id.clone(),
        proposal_admission_generation: admission.proposal_admission_generation,
        witness_key_id: admission.witness_key_id.clone(),
        witness_key_generation: admission.witness_key_generation,
    }
}

fn admission_from_proto(proto: ProposalAdmissionProto) -> Result<ProposalAdmissionV1> {
    Ok(ProposalAdmissionV1 {
        format_version: proto.format_version,
        database_id: proto.database_id,
        group_kind: proto.group_kind,
        proposer_id: proto.proposer_id,
        client_proposal_hash_sha256: exact32(
            proto.client_proposal_hash_sha256,
            "client_proposal_hash_sha256",
        )?,
        workflow_id: proto.workflow_id,
        fencing_generation: proto.fencing_generation,
        authorization_revision: proto.authorization_revision,
        idempotency_claim_sha256: exact32(
            proto.idempotency_claim_sha256,
            "idempotency_claim_sha256",
        )?,
        issued_at_unix_seconds: proto.issued_at_unix_seconds,
        expires_at_unix_seconds: proto.expires_at_unix_seconds,
        reservation_id: proto.reservation_id,
        reservation_revision: proto.reservation_revision,
        reservation_identity_sha256: exact32(
            proto.reservation_identity_sha256,
            "reservation_identity_sha256",
        )?,
        authorization_receipt_sha256: exact32(
            proto.authorization_receipt_sha256,
            "authorization_receipt_sha256",
        )?,
        selected_voter_ids: proto.selected_voter_ids,
        primary_server_id: proto.primary_server_id,
        proposal_admission_key_id: proto.proposal_admission_key_id,
        proposal_admission_generation: proto.proposal_admission_generation,
        witness_key_id: proto.witness_key_id,
        witness_key_generation: proto.witness_key_generation,
    })
}

fn exact32(bytes: Vec<u8>, field: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| anyhow!("{field} must be exactly 32 bytes"))
}

fn optional_exact32(bytes: Option<Vec<u8>>, field: &str) -> Result<Option<[u8; 32]>> {
    bytes.map(|bytes| exact32(bytes, field)).transpose()
}

fn optional32_to_vec(value: Option<[u8; 32]>) -> Option<Vec<u8>> {
    value.map(|bytes| bytes.to_vec())
}

#[cfg(test)]
mod tests;
