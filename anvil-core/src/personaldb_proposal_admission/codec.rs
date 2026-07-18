use super::*;

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProposalIdempotencyClaimIdentityProto {
    #[prost(uint32, tag = "1")]
    pub(super) format_version: u32,
    #[prost(string, tag = "2")]
    pub(super) tenant_id: String,
    #[prost(string, tag = "3")]
    pub(super) application_id: String,
    #[prost(string, tag = "4")]
    pub(super) operation_id: String,
    #[prost(string, tag = "5")]
    pub(super) request_id: String,
    #[prost(string, tag = "6")]
    pub(super) database_id: String,
    #[prost(bytes = "vec", tag = "7")]
    pub(super) client_proposal_hash_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    pub(super) changeset_payload_hash_sha256: Vec<u8>,
    #[prost(string, tag = "9")]
    pub(super) workflow_id: String,
    #[prost(uint64, tag = "10")]
    pub(super) fencing_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProposalAdmissionSlotProto {
    #[prost(uint32, tag = "1")]
    pub(super) format_version: u32,
    #[prost(string, tag = "2")]
    pub(super) database_id: String,
    #[prost(uint64, tag = "3")]
    pub(super) next_log_index: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) expected_previous_log_hash_sha256: Vec<u8>,
    #[prost(uint64, tag = "5")]
    pub(super) placement_epoch: u64,
    #[prost(uint64, tag = "6")]
    pub(super) client_log_epoch: u64,
    #[prost(uint64, tag = "7")]
    pub(super) fencing_generation: u64,
    #[prost(uint64, tag = "8")]
    pub(super) slot_revision: u64,
    #[prost(string, tag = "9")]
    pub(super) reservation_id: String,
    #[prost(enumeration = "ProposalAdmissionSlotStateV1", tag = "10")]
    pub(super) state: i32,
    #[prost(bytes = "vec", optional, tag = "11")]
    pub(super) witness_dual_signing_receipt_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "12")]
    pub(super) terminal_committed_head_sha256: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProposalAdmissionReservationIdentityProto {
    #[prost(uint32, tag = "1")]
    pub(super) format_version: u32,
    #[prost(string, tag = "2")]
    pub(super) reservation_id: String,
    #[prost(string, tag = "3")]
    pub(super) database_id: String,
    #[prost(string, tag = "4")]
    pub(super) group_kind: String,
    #[prost(string, tag = "5")]
    pub(super) proposer_id: String,
    #[prost(bytes = "vec", tag = "6")]
    pub(super) client_proposal_hash_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "7")]
    pub(super) changeset_payload_hash_sha256: Vec<u8>,
    #[prost(uint64, tag = "8")]
    pub(super) expected_previous_log_index: u64,
    #[prost(bytes = "vec", tag = "9")]
    pub(super) expected_previous_log_hash_sha256: Vec<u8>,
    #[prost(uint64, tag = "10")]
    pub(super) membership_revision: u64,
    #[prost(uint64, tag = "11")]
    pub(super) placement_epoch: u64,
    #[prost(uint64, tag = "12")]
    pub(super) client_log_epoch: u64,
    #[prost(string, tag = "13")]
    pub(super) workflow_id: String,
    #[prost(uint64, tag = "14")]
    pub(super) fencing_generation: u64,
    #[prost(string, tag = "15")]
    pub(super) leader_lease_id: String,
    #[prost(uint64, tag = "16")]
    pub(super) leader_lease_revision: u64,
    #[prost(bytes = "vec", tag = "17")]
    pub(super) authorization_receipt_sha256: Vec<u8>,
    #[prost(uint64, tag = "18")]
    pub(super) authorization_revision: u64,
    #[prost(bytes = "vec", tag = "19")]
    pub(super) idempotency_claim_sha256: Vec<u8>,
    #[prost(int64, tag = "20")]
    pub(super) issued_at_unix_seconds: i64,
    #[prost(int64, tag = "21")]
    pub(super) expires_at_unix_seconds: i64,
    #[prost(string, repeated, tag = "22")]
    pub(super) selected_voter_ids: Vec<String>,
    #[prost(string, tag = "23")]
    pub(super) primary_server_id: String,
    #[prost(string, tag = "24")]
    pub(super) proposal_admission_key_id: String,
    #[prost(uint64, tag = "25")]
    pub(super) proposal_admission_generation: u64,
    #[prost(string, tag = "26")]
    pub(super) witness_key_id: String,
    #[prost(uint64, tag = "27")]
    pub(super) witness_key_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProposalAdmissionReservationProto {
    #[prost(message, optional, tag = "1")]
    pub(super) identity: Option<ProposalAdmissionReservationIdentityProto>,
    #[prost(uint64, tag = "2")]
    pub(super) reservation_revision: u64,
    #[prost(enumeration = "ProposalAdmissionReservationStateV1", tag = "3")]
    pub(super) state: i32,
    #[prost(bytes = "vec", optional, tag = "4")]
    pub(super) candidate_entry_core_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "5")]
    pub(super) candidate_unsigned_certificate_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "6")]
    pub(super) candidate_head_template_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "7")]
    pub(super) candidate_unsigned_committed_head_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub(super) witness_dual_signing_receipt_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "9")]
    pub(super) terminal_commit_certificate_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "10")]
    pub(super) terminal_committed_head_sha256: Option<Vec<u8>>,
    #[prost(bytes = "vec", optional, tag = "11")]
    pub(super) witness_signing_candidate_sha256: Option<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct WitnessSigningCandidateProto {
    #[prost(uint32, tag = "1")]
    pub(super) format_version: u32,
    #[prost(string, tag = "2")]
    pub(super) reservation_id: String,
    #[prost(uint64, tag = "3")]
    pub(super) signing_reservation_revision: u64,
    #[prost(string, tag = "4")]
    pub(super) database_id: String,
    #[prost(uint64, tag = "5")]
    pub(super) next_log_index: u64,
    #[prost(uint64, tag = "6")]
    pub(super) client_log_epoch: u64,
    #[prost(uint64, tag = "7")]
    pub(super) fencing_generation: u64,
    #[prost(string, tag = "8")]
    pub(super) witness_key_id: String,
    #[prost(uint64, tag = "9")]
    pub(super) witness_key_generation: u64,
    #[prost(bytes = "vec", tag = "10")]
    pub(super) unsigned_commit_certificate: Vec<u8>,
    #[prost(bytes = "vec", tag = "11")]
    pub(super) head_template: Vec<u8>,
    #[prost(bytes = "vec", tag = "12")]
    pub(super) candidate_entry_core_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "13")]
    pub(super) candidate_unsigned_certificate_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "14")]
    pub(super) candidate_head_template_sha256: Vec<u8>,
    #[prost(int64, tag = "15")]
    pub(super) created_at_unix_seconds: i64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct WitnessDualSigningReceiptProto {
    #[prost(uint32, tag = "1")]
    pub(super) format_version: u32,
    #[prost(string, tag = "2")]
    pub(super) reservation_id: String,
    #[prost(uint64, tag = "3")]
    pub(super) signing_reservation_revision: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub(super) candidate_unsigned_certificate_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub(super) candidate_head_template_sha256: Vec<u8>,
    #[prost(string, tag = "6")]
    pub(super) witness_key_id: String,
    #[prost(uint64, tag = "7")]
    pub(super) witness_key_generation: u64,
    #[prost(bytes = "vec", tag = "8")]
    pub(super) certificate_signature_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "9")]
    pub(super) signed_commit_certificate_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    pub(super) unsigned_committed_head_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "11")]
    pub(super) head_signature_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "12")]
    pub(super) signed_committed_head_sha256: Vec<u8>,
    #[prost(int64, tag = "13")]
    pub(super) signed_at_unix_seconds: i64,
    #[prost(bytes = "vec", tag = "14")]
    pub(super) signed_commit_certificate: Vec<u8>,
    #[prost(bytes = "vec", tag = "15")]
    pub(super) signed_committed_head: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ProposalAdmissionProto {
    #[prost(uint32, tag = "1")]
    pub(super) format_version: u32,
    #[prost(string, tag = "2")]
    pub(super) database_id: String,
    #[prost(string, tag = "3")]
    pub(super) group_kind: String,
    #[prost(string, tag = "4")]
    pub(super) proposer_id: String,
    #[prost(bytes = "vec", tag = "5")]
    pub(super) client_proposal_hash_sha256: Vec<u8>,
    #[prost(string, tag = "6")]
    pub(super) workflow_id: String,
    #[prost(uint64, tag = "7")]
    pub(super) fencing_generation: u64,
    #[prost(uint64, tag = "8")]
    pub(super) authorization_revision: u64,
    #[prost(bytes = "vec", tag = "9")]
    pub(super) idempotency_claim_sha256: Vec<u8>,
    #[prost(int64, tag = "10")]
    pub(super) issued_at_unix_seconds: i64,
    #[prost(int64, tag = "11")]
    pub(super) expires_at_unix_seconds: i64,
    #[prost(string, tag = "12")]
    pub(super) reservation_id: String,
    #[prost(uint64, tag = "13")]
    pub(super) reservation_revision: u64,
    #[prost(bytes = "vec", tag = "14")]
    pub(super) reservation_identity_sha256: Vec<u8>,
    #[prost(bytes = "vec", tag = "15")]
    pub(super) authorization_receipt_sha256: Vec<u8>,
    #[prost(string, repeated, tag = "16")]
    pub(super) selected_voter_ids: Vec<String>,
    #[prost(string, tag = "17")]
    pub(super) primary_server_id: String,
    #[prost(string, tag = "18")]
    pub(super) proposal_admission_key_id: String,
    #[prost(uint64, tag = "19")]
    pub(super) proposal_admission_generation: u64,
    #[prost(string, tag = "20")]
    pub(super) witness_key_id: String,
    #[prost(uint64, tag = "21")]
    pub(super) witness_key_generation: u64,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SignedProposalAdmissionProto {
    #[prost(message, optional, tag = "1")]
    pub(super) admission: Option<ProposalAdmissionProto>,
    #[prost(message, optional, tag = "2")]
    pub(super) proposal_admission_signature: Option<WireSignatureEnvelopeV1>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ClaimRowProto {
    #[prost(message, optional, tag = "1")]
    pub(super) common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    pub(super) claim: Option<ProposalIdempotencyClaimIdentityProto>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct SlotRowProto {
    #[prost(message, optional, tag = "1")]
    pub(super) common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    pub(super) slot: Option<ProposalAdmissionSlotProto>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ReservationRowProto {
    #[prost(message, optional, tag = "1")]
    pub(super) common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    pub(super) reservation: Option<ProposalAdmissionReservationProto>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct CandidateRowProto {
    #[prost(message, optional, tag = "1")]
    pub(super) common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    pub(super) candidate: Option<WitnessSigningCandidateProto>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct ReceiptRowProto {
    #[prost(message, optional, tag = "1")]
    pub(super) common: Option<CoreMetaRowCommonProto>,
    #[prost(message, optional, tag = "2")]
    pub(super) receipt: Option<WitnessDualSigningReceiptProto>,
}

pub(super) fn claim_to_proto(
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

pub(super) fn claim_from_proto(
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

pub(super) fn slot_to_proto(slot: &ProposalAdmissionSlotV1) -> ProposalAdmissionSlotProto {
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

pub(super) fn slot_from_proto(
    proto: ProposalAdmissionSlotProto,
) -> Result<ProposalAdmissionSlotV1> {
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

pub(super) fn reservation_identity_to_proto(
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

pub(super) fn reservation_identity_from_proto(
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

pub(super) fn reservation_to_proto(
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

pub(super) fn reservation_from_proto(
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

pub(super) fn candidate_to_proto(
    candidate: &WitnessSigningCandidateV1,
) -> WitnessSigningCandidateProto {
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

pub(super) fn candidate_from_proto(
    proto: WitnessSigningCandidateProto,
) -> Result<WitnessSigningCandidateV1> {
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

pub(super) fn receipt_to_proto(
    receipt: &WitnessDualSigningReceiptV1,
) -> WitnessDualSigningReceiptProto {
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

pub(super) fn receipt_from_proto(
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

pub(super) fn admission_to_proto(admission: &ProposalAdmissionV1) -> ProposalAdmissionProto {
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

pub(super) fn admission_from_proto(proto: ProposalAdmissionProto) -> Result<ProposalAdmissionV1> {
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

pub(super) fn exact32(bytes: Vec<u8>, field: &str) -> Result<[u8; 32]> {
    bytes
        .try_into()
        .map_err(|_| anyhow!("{field} must be exactly 32 bytes"))
}

pub(super) fn optional_exact32(bytes: Option<Vec<u8>>, field: &str) -> Result<Option<[u8; 32]>> {
    bytes.map(|bytes| exact32(bytes, field)).transpose()
}

pub(super) fn optional32_to_vec(value: Option<[u8; 32]>) -> Option<Vec<u8>> {
    value.map(|bytes| bytes.to_vec())
}
