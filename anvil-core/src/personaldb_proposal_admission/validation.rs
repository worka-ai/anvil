use super::*;

pub(super) fn validate_claim(claim: &ProposalIdempotencyClaimIdentityV1) -> Result<()> {
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

pub(super) fn validate_reservation_identity_shape(
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

pub(super) fn validate_reservation(reservation: &ProposalAdmissionReservationV1) -> Result<()> {
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

pub(super) fn validate_slot(slot: &ProposalAdmissionSlotV1) -> Result<()> {
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

pub(super) fn validate_candidate(candidate: &WitnessSigningCandidateV1) -> Result<()> {
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

pub(super) fn validate_receipt_shape(receipt: &WitnessDualSigningReceiptV1) -> Result<()> {
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

pub(super) fn validate_admission_shape(admission: &ProposalAdmissionV1) -> Result<()> {
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

pub(super) fn validate_active_key_binding(
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

pub(super) fn validate_bound_signer(
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

pub(super) fn validate_bound_record(
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
    if !record.database_scopes.is_empty() && !record.database_scopes.as_slice().contains(&database)
    {
        bail!("bound {purpose} key is outside the database scope");
    }
    let group_id = database_id.to_string();
    if !record.group_scopes.is_empty() && !record.group_scopes.as_slice().contains(&group_id) {
        bail!("bound {purpose} key is outside the group scope");
    }
    Ok(())
}

pub(super) fn require_unsigned_certificate(
    certificate: &PersonalDbCommitCertificate,
) -> Result<()> {
    if certificate.certificate_hash.is_some() || certificate.witness_signature.is_some() {
        bail!("witness candidate commit certificate must be unsigned");
    }
    Ok(())
}

pub(super) fn require_unsigned_head(head: &PersonalDbCommittedHead) -> Result<()> {
    if head.head_hash.is_some() || head.head_signature.is_some() {
        bail!("witness candidate committed-head template must be unsigned");
    }
    Ok(())
}

pub(super) fn require_version(version: u32, object: &str) -> Result<()> {
    if version != FORMAT_VERSION_V1 {
        bail!("{object} has unsupported format version");
    }
    Ok(())
}

pub(super) fn validate_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_TEXT_BYTES
        || value.contains('\0')
        || value.chars().any(char::is_control)
    {
        bail!("{field} is empty or outside the canonical text bound");
    }
    Ok(())
}

pub(super) fn validate_sorted_unique_text(values: &[String], field: &str) -> Result<()> {
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

pub(super) fn validate_reservation_id(value: &str) -> Result<()> {
    KeyId::new(value.to_string())
        .map(|_| ())
        .context("proposal reservation ID must be canonical sha256")
}

pub(super) fn validate_tenant_database(tenant_id: i64, database_id: &str) -> Result<()> {
    if tenant_id < 0 {
        bail!("PersonalDB tenant ID must be nonnegative");
    }
    validate_text(database_id, "database_id")
}

pub(super) fn parse_claim_tenant_id(claim: &ProposalIdempotencyClaimIdentityV1) -> Result<i64> {
    let tenant_id = claim
        .tenant_id
        .parse::<i64>()
        .context("parse proposal idempotency claim tenant_id")?;
    if tenant_id < 0 {
        bail!("proposal idempotency claim tenant_id must be nonnegative");
    }
    Ok(tenant_id)
}

pub(super) fn next_log_index(identity: &ProposalAdmissionReservationIdentityV1) -> Result<u64> {
    identity
        .expected_previous_log_index
        .checked_add(1)
        .ok_or_else(|| anyhow!("proposal reservation log index overflow"))
}

pub(super) fn ensure_bounded_object(bytes: &[u8], name: &str) -> Result<()> {
    if bytes.is_empty() || bytes.len() > MAX_SIGNING_OBJECT_BYTES {
        bail!("{name} is outside the signing object bound");
    }
    Ok(())
}

pub(super) fn decode_hex32(value: &str, field: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(value).with_context(|| format!("decode {field} hex"))?;
    bytes
        .try_into()
        .map_err(|_| anyhow!("{field} must be exactly 32 bytes"))
}
