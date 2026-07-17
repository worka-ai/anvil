use super::*;
use crate::{
    partition_fence::{
        PartitionRecoveryAcquire, acquire_partition_recovery,
        force_expire_partition_owner_for_node, publish_partition_ready,
    },
    personaldb_heads::write_personaldb_committed_head,
    personaldb_signing::PersonalDbProtocolKeyring,
};
use personaldb_protocol::{
    Ed25519ProtocolSigner, KeyTrustPolicy, ProtocolSigner, PublicKeyTrustStore,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tempfile::TempDir;

const TENANT_ID: i64 = 41;
const DATABASE_ID: &str = "group-a";
const OWNER_ID: &str = "node-a";
const OWNER_KEY: &[u8] = b"personaldb-admission-owner-test-key";
const NOW: i64 = 1_800_000_000;

struct Fixture {
    _directory: TempDir,
    storage: Storage,
    permit: PartitionWritePermit,
    owner: PartitionOwnerState,
    keyring: PersonalDbProtocolKeyring,
    proposal_signer: Arc<dyn ProtocolSigner>,
    witness_signer: Arc<dyn ProtocolSigner>,
}

impl Fixture {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let storage = Storage::new_at(directory.path()).await.unwrap();
        let group_signer = signer(0x41, SignaturePurpose::GroupControl);
        let snapshot_signer = signer(0x42, SignaturePurpose::Snapshot);
        let proposal_signer = signer(0x43, SignaturePurpose::ProposalAdmission);
        let witness_signer = signer(0x44, SignaturePurpose::Witness);
        let signers = [
            Arc::clone(&group_signer),
            Arc::clone(&snapshot_signer),
            Arc::clone(&proposal_signer),
            Arc::clone(&witness_signer),
        ];
        let trust_store = PublicKeyTrustStore::from_records(
            signers.iter().map(|signer| signer.trust_record().clone()),
        )
        .unwrap();
        let keyring = PersonalDbProtocolKeyring::new_test_only(trust_store, signers).unwrap();
        let genesis = PersonalDbCommittedHead {
            format_version: 2,
            tenant_id: TENANT_ID.to_string(),
            database_id: DATABASE_ID.to_string(),
            log_index: 0,
            log_hash: hex::encode([0; 32]),
            segment_ref: String::new(),
            row_index_generation: 0,
            policy_epoch: 1,
            membership_epoch: 1,
            schema_hash: hex::encode([9; 32]),
            updated_at: "2027-01-15T08:00:00Z".to_string(),
            updated_by_node: OWNER_ID.to_string(),
            head_hash: None,
            head_signature: None,
        }
        .seal(&keyring)
        .await
        .unwrap();
        write_personaldb_committed_head(
            &storage,
            TENANT_ID,
            DATABASE_ID,
            &genesis,
            keyring.trust_store(),
        )
        .await
        .unwrap();

        let partition_id = personaldb_group_partition_owner_id(TENANT_ID, DATABASE_ID).unwrap();
        let recovering = acquire_partition_recovery(
            &storage,
            PartitionRecoveryAcquire {
                partition_family: PERSONALDB_GROUP_PARTITION_FAMILY.to_string(),
                partition_id: partition_id.clone(),
                owner_node_id: OWNER_ID.to_string(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos: NOW * 1_000_000_000,
            },
            OWNER_KEY,
        )
        .await
        .unwrap();
        let owner = publish_partition_ready(
            &storage,
            PERSONALDB_GROUP_PARTITION_FAMILY,
            &partition_id,
            OWNER_ID,
            recovering.fence_token,
            0,
            &hex::encode([0; 32]),
            NOW * 1_000_000_000 + 1,
            OWNER_KEY,
        )
        .await
        .unwrap();
        let permit = owner.write_permit().unwrap();
        Self {
            _directory: directory,
            storage,
            permit,
            owner,
            keyring,
            proposal_signer,
            witness_signer,
        }
    }

    fn authority(&self) -> PersonalDbAdmissionAuthority<'_> {
        self.authority_for(&self.storage)
    }

    fn authority_for<'a>(&'a self, storage: &'a Storage) -> PersonalDbAdmissionAuthority<'a> {
        PersonalDbAdmissionAuthority {
            storage,
            trust_store: self.keyring.trust_store(),
            write_permit: &self.permit,
            partition_owner_signing_key: OWNER_KEY,
            now_unix_seconds: NOW,
        }
    }

    fn claim_and_identity(
        &self,
        request_id: &str,
    ) -> (
        ProposalIdempotencyClaimIdentityV1,
        ProposalAdmissionReservationIdentityV1,
    ) {
        let claim = ProposalIdempotencyClaimIdentityV1 {
            format_version: 1,
            tenant_id: TENANT_ID.to_string(),
            application_id: "test-app".to_string(),
            operation_id: "submit".to_string(),
            request_id: request_id.to_string(),
            database_id: DATABASE_ID.to_string(),
            client_proposal_hash_sha256: [1; 32],
            changeset_payload_hash_sha256: [2; 32],
            workflow_id: "workflow-1".to_string(),
            fencing_generation: self.owner.fence_token,
        };
        let claim_hash = claim.hash_sha256().unwrap();
        let identity = ProposalAdmissionReservationIdentityV1 {
            format_version: 1,
            reservation_id: derive_reservation_id(DATABASE_ID, claim_hash).unwrap(),
            database_id: DATABASE_ID.to_string(),
            group_kind: "source".to_string(),
            proposer_id: "proposer-1".to_string(),
            client_proposal_hash_sha256: claim.client_proposal_hash_sha256,
            changeset_payload_hash_sha256: claim.changeset_payload_hash_sha256,
            expected_previous_log_index: 0,
            expected_previous_log_hash_sha256: [0; 32],
            membership_revision: 1,
            placement_epoch: 1,
            client_log_epoch: 1,
            workflow_id: claim.workflow_id.clone(),
            fencing_generation: self.owner.fence_token,
            leader_lease_id: personaldb_group_leader_lease_id(&self.owner),
            leader_lease_revision: self.owner.recovery_epoch,
            authorization_receipt_sha256: [3; 32],
            authorization_revision: 1,
            idempotency_claim_sha256: claim_hash,
            issued_at_unix_seconds: NOW,
            expires_at_unix_seconds: NOW + 300,
            selected_voter_ids: vec!["voter-a".to_string(), "voter-b".to_string()],
            primary_server_id: OWNER_ID.to_string(),
            proposal_admission_key_id: self.proposal_signer.trust_record().key_id.to_string(),
            proposal_admission_generation: self.proposal_signer.trust_record().key_generation.get(),
            witness_key_id: self.witness_signer.trust_record().key_id.to_string(),
            witness_key_generation: self.witness_signer.trust_record().key_generation.get(),
        };
        (claim, identity)
    }

    fn candidate_request(
        &self,
        reservation: &ProposalAdmissionReservationV1,
    ) -> BeginWitnessSigningV1 {
        BeginWitnessSigningV1 {
            tenant_id: TENANT_ID,
            reservation_id: reservation.identity.reservation_id.clone(),
            expected_reservation_revision: reservation.reservation_revision,
            unsigned_commit_certificate: PersonalDbCommitCertificate {
                format_version: 2,
                tenant_id: TENANT_ID.to_string(),
                database_id: DATABASE_ID.to_string(),
                log_index: 1,
                previous_log_hash: hex::encode([0; 32]),
                entry_hash: hex::encode([4; 32]),
                changeset_payload_hash: hex::encode([2; 32]),
                verified_envelope_hash: hex::encode([5; 32]),
                client_log_epoch: 1,
                membership_epoch: 1,
                policy_epoch: 1,
                leader_replica_id: OWNER_ID.to_string(),
                voter_acks_hash: hex::encode([6; 32]),
                authz_revision: 1,
                witness_node_id: "witness-1".to_string(),
                witnessed_at: "2027-01-15T08:00:00Z".to_string(),
                certificate_hash: None,
                witness_signature: None,
            },
            head_template: PersonalDbCommittedHead {
                format_version: 2,
                tenant_id: TENANT_ID.to_string(),
                database_id: DATABASE_ID.to_string(),
                log_index: 1,
                log_hash: hex::encode([4; 32]),
                segment_ref: "personaldb_log_segment:0001".to_string(),
                row_index_generation: 1,
                policy_epoch: 1,
                membership_epoch: 1,
                schema_hash: hex::encode([9; 32]),
                updated_at: "2027-01-15T08:00:00Z".to_string(),
                updated_by_node: OWNER_ID.to_string(),
                head_hash: None,
                head_signature: None,
            },
            created_at_unix_seconds: NOW,
        }
    }
}

#[tokio::test]
async fn reservation_exact_replay_and_signed_admission_are_purpose_bound() {
    let fixture = Fixture::new().await;
    let (claim, identity) = fixture.claim_and_identity("request-1");
    let reserved =
        reserve_personaldb_proposal(&fixture.authority(), claim.clone(), identity.clone())
            .await
            .unwrap();
    let replay = reserve_personaldb_proposal(&fixture.authority(), claim, identity)
        .await
        .unwrap();
    assert_eq!(replay, reserved);

    let admission = sign_proposal_admission(
        &reserved,
        fixture.proposal_signer.as_ref(),
        fixture.keyring.trust_store(),
    )
    .unwrap();
    admission
        .verify(&reserved, fixture.keyring.trust_store())
        .unwrap();
    let encoded = admission.encode_deterministic().unwrap();
    assert_eq!(
        SignedProposalAdmissionV1::decode_deterministic(&encoded).unwrap(),
        admission
    );
    let wrong_purpose = sign_proposal_admission(
        &reserved,
        fixture.witness_signer.as_ref(),
        fixture.keyring.trust_store(),
    )
    .unwrap_err();
    assert!(wrong_purpose.to_string().contains("signer key"));
}

#[tokio::test]
async fn stale_fence_is_rejected_before_reservation_write() {
    let fixture = Fixture::new().await;
    let (claim, identity) = fixture.claim_and_identity("stale-fence");
    force_expire_partition_owner_for_node(
        &fixture.storage,
        PERSONALDB_GROUP_PARTITION_FAMILY,
        &fixture.permit.partition_id,
        OWNER_ID,
        NOW * 1_000_000_000 + 2,
        OWNER_KEY,
    )
    .await
    .unwrap();
    let error = reserve_personaldb_proposal(&fixture.authority(), claim, identity)
        .await
        .unwrap_err();
    assert!(
        error.to_string().contains("not current")
            || error.to_string().contains("stale")
            || error.to_string().contains("has not completed recovery")
    );
}

#[tokio::test]
async fn duplicate_slot_and_altered_idempotency_replay_are_rejected() {
    let fixture = Fixture::new().await;
    let (claim, identity) = fixture.claim_and_identity("slot-first");
    reserve_personaldb_proposal(&fixture.authority(), claim.clone(), identity)
        .await
        .unwrap();

    let (second_claim, second_identity) = fixture.claim_and_identity("slot-second");
    let duplicate =
        reserve_personaldb_proposal(&fixture.authority(), second_claim, second_identity)
            .await
            .unwrap_err();
    assert!(duplicate.to_string().contains("slot is occupied"));

    let mut altered_claim = claim;
    altered_claim.client_proposal_hash_sha256 = [8; 32];
    let altered_hash = altered_claim.hash_sha256().unwrap();
    let mut altered_identity = fixture.claim_and_identity("slot-first").1;
    altered_identity.client_proposal_hash_sha256 = [8; 32];
    altered_identity.idempotency_claim_sha256 = altered_hash;
    altered_identity.reservation_id = derive_reservation_id(DATABASE_ID, altered_hash).unwrap();
    let altered =
        reserve_personaldb_proposal(&fixture.authority(), altered_claim, altered_identity)
            .await
            .unwrap_err();
    assert!(altered.to_string().contains("idempotency claim conflict"));
}

#[tokio::test]
async fn altered_candidate_replay_is_rejected() {
    let fixture = Fixture::new().await;
    let (claim, identity) = fixture.claim_and_identity("candidate-replay");
    let reserved = reserve_personaldb_proposal(&fixture.authority(), claim, identity)
        .await
        .unwrap();
    let request = fixture.candidate_request(&reserved);
    let candidate = begin_personaldb_witness_signing(&fixture.authority(), request.clone())
        .await
        .unwrap();
    let exact = begin_personaldb_witness_signing(&fixture.authority(), request.clone())
        .await
        .unwrap();
    assert_eq!(exact, candidate);

    let mut altered = request;
    altered.head_template.schema_hash = hex::encode([7; 32]);
    let error = begin_personaldb_witness_signing(&fixture.authority(), altered)
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("altered witness candidate replay")
    );
}

#[tokio::test]
async fn receipt_survives_crash_window_and_replays_without_signing_again() {
    let fixture = Fixture::new().await;
    let (claim, identity) = fixture.claim_and_identity("receipt-replay");
    let reserved = reserve_personaldb_proposal(&fixture.authority(), claim, identity)
        .await
        .unwrap();
    let candidate = begin_personaldb_witness_signing(
        &fixture.authority(),
        fixture.candidate_request(&reserved),
    )
    .await
    .unwrap();
    let signing_request = SignCertificateAndHeadV1 {
        reservation_id: candidate.reservation_id.clone(),
        signing_reservation_revision: candidate.signing_reservation_revision,
    };
    let signer_storage = Storage::new_at(fixture._directory.path()).await.unwrap();
    let wrong_purpose = sign_personaldb_certificate_and_head(
        &fixture.authority_for(&signer_storage),
        &signing_request,
        fixture.proposal_signer.as_ref(),
    )
    .await
    .unwrap_err();
    assert!(wrong_purpose.to_string().contains("signer key"));

    let count = Arc::new(AtomicUsize::new(0));
    let fail_after_certificate = FailOnSecondSignature {
        inner: Arc::clone(&fixture.witness_signer),
        count: Arc::clone(&count),
    };
    let interrupted = sign_personaldb_certificate_and_head(
        &fixture.authority_for(&signer_storage),
        &signing_request,
        &fail_after_certificate,
    )
    .await
    .unwrap_err();
    assert!(
        interrupted
            .to_string()
            .contains("invalid Ed25519 signature")
    );
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert!(
        read_witness_dual_signing_receipt(&signer_storage, &candidate.reservation_id)
            .unwrap()
            .is_none()
    );

    let counting = CountingSigner {
        inner: Arc::clone(&fixture.witness_signer),
        count: Arc::clone(&count),
    };
    let receipt = sign_personaldb_certificate_and_head(
        &fixture.authority_for(&signer_storage),
        &signing_request,
        &counting,
    )
    .await
    .unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 4);

    let replay_storage = Storage::new_at(fixture._directory.path()).await.unwrap();
    let replay = sign_personaldb_certificate_and_head(
        &fixture.authority_for(&replay_storage),
        &signing_request,
        &counting,
    )
    .await
    .unwrap();
    assert_eq!(replay, receipt);
    assert_eq!(count.load(Ordering::SeqCst), 4);

    let mut altered_request = signing_request.clone();
    altered_request.signing_reservation_revision += 1;
    let altered = sign_personaldb_certificate_and_head(
        &fixture.authority_for(&replay_storage),
        &altered_request,
        &counting,
    )
    .await
    .unwrap_err();
    assert!(
        altered
            .to_string()
            .contains("altered witness signing replay")
    );
    assert_eq!(count.load(Ordering::SeqCst), 4);

    let acknowledged = acknowledge_personaldb_witness_receipt(
        &fixture.authority_for(&replay_storage),
        &signing_request,
    )
    .await
    .unwrap();
    let acknowledged_replay = acknowledge_personaldb_witness_receipt(
        &fixture.authority_for(&replay_storage),
        &signing_request,
    )
    .await
    .unwrap();
    assert_eq!(acknowledged_replay, acknowledged);
    assert_eq!(
        read_proposal_admission_slot(&replay_storage, TENANT_ID, DATABASE_ID, 1, 1,)
            .unwrap()
            .unwrap()
            .state,
        ProposalAdmissionSlotStateV1::Signed
    );
}

struct CountingSigner {
    inner: Arc<dyn ProtocolSigner>,
    count: Arc<AtomicUsize>,
}

impl ProtocolSigner for CountingSigner {
    fn trust_record(&self) -> &PublicKeyTrustRecord {
        self.inner.trust_record()
    }

    fn sign(
        &self,
        signable: &dyn ProtocolSignable,
    ) -> Result<SignatureEnvelopeV1, personaldb_protocol::SignatureError> {
        self.count.fetch_add(1, Ordering::SeqCst);
        self.inner.sign(signable)
    }
}

struct FailOnSecondSignature {
    inner: Arc<dyn ProtocolSigner>,
    count: Arc<AtomicUsize>,
}

impl ProtocolSigner for FailOnSecondSignature {
    fn trust_record(&self) -> &PublicKeyTrustRecord {
        self.inner.trust_record()
    }

    fn sign(
        &self,
        signable: &dyn ProtocolSignable,
    ) -> Result<SignatureEnvelopeV1, personaldb_protocol::SignatureError> {
        let call = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if call == 2 {
            return Err(personaldb_protocol::SignatureError::InvalidSignature);
        }
        self.inner.sign(signable)
    }
}

fn signer(seed: u8, purpose: SignaturePurpose) -> Arc<dyn ProtocolSigner> {
    let mut key = hex::decode("302e020100300506032b657004220420").unwrap();
    key.extend([seed; 32]);
    Arc::new(
        Ed25519ProtocolSigner::from_pkcs8_der(
            &key,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), purpose, 0),
        )
        .unwrap(),
    )
}
