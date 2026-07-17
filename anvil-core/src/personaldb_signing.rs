use crate::anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1;
use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use personaldb_protocol::{
    Ed25519ProtocolSigner, Ed25519PublicKey, KeyGeneration, KeyId, ProtocolSignable,
    ProtocolSigner, PublicKeyStatus, PublicKeyTrustRecord, PublicKeyTrustStore, SignatureAlgorithm,
    SignatureEnvelopeV1, SignaturePurpose,
};
use prost::Message;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Clone)]
pub struct PersonalDbProtocolKeyring {
    trust_store: PublicKeyTrustStore,
    signers: Arc<HashMap<SignaturePurpose, Arc<dyn ProtocolSigner>>>,
}

impl fmt::Debug for PersonalDbProtocolKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let signer_key_ids = self
            .signers
            .iter()
            .map(|(purpose, signer)| (purpose, &signer.trust_record().key_id))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("PersonalDbProtocolKeyring")
            .field("trust_store", &self.trust_store)
            .field("signer_key_ids", &signer_key_ids)
            .finish()
    }
}

impl PersonalDbProtocolKeyring {
    pub fn new(
        trust_store: PublicKeyTrustStore,
        signers: impl IntoIterator<Item = Arc<dyn ProtocolSigner>>,
    ) -> Result<Self> {
        if trust_store.is_empty() {
            bail!("PersonalDB protocol trust store must not be empty");
        }

        let mut by_purpose = HashMap::new();
        for signer in signers {
            let record = signer.trust_record();
            let purpose = record.purpose;
            record.validate()?;
            if record.status != PublicKeyStatus::Active {
                bail!(
                    "PersonalDB {} signer must use an active trust record",
                    purpose
                );
            }
            if trust_store.get(&record.key_id) != Some(record) {
                bail!(
                    "PersonalDB {} signer does not match its trust-store record",
                    purpose
                );
            }
            if by_purpose.insert(purpose, signer).is_some() {
                bail!(
                    "PersonalDB protocol keyring has more than one {} signer",
                    purpose
                );
            }
        }

        for purpose in [
            SignaturePurpose::GroupControl,
            SignaturePurpose::Snapshot,
            SignaturePurpose::Witness,
        ] {
            if !by_purpose.contains_key(&purpose) {
                bail!("PersonalDB protocol keyring is missing the {purpose} signer");
            }
        }

        Ok(Self {
            trust_store,
            signers: Arc::new(by_purpose),
        })
    }

    pub fn trust_store(&self) -> &PublicKeyTrustStore {
        &self.trust_store
    }

    pub(crate) fn sign(&self, signable: &dyn ProtocolSignable) -> Result<SignatureEnvelopeV1> {
        let metadata = signable.signature_metadata();
        let signer = self
            .signers
            .get(&metadata.purpose)
            .ok_or_else(|| anyhow!("PersonalDB {} signer is not configured", metadata.purpose))?;
        let envelope = signer.sign(signable)?;
        self.trust_store.verify(signable, &envelope)?;
        Ok(envelope)
    }

    pub fn from_manifest_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            bail!("PersonalDB protocol keyring manifest path is required");
        }
        let raw = std::fs::read(path).with_context(|| {
            format!(
                "read PersonalDB protocol keyring manifest {}",
                path.display()
            )
        })?;
        let manifest: PersonalDbProtocolKeyringManifestV1 =
            serde_json::from_slice(&raw).context("parse PersonalDB protocol keyring manifest")?;
        if manifest.format_version != 1 {
            bail!("unsupported PersonalDB protocol keyring manifest version");
        }

        let records = manifest
            .trusted_keys
            .into_iter()
            .map(PersonalDbTrustedKeyConfigV1::into_trust_record)
            .collect::<Result<Vec<_>>>()?;
        let trust_store = PublicKeyTrustStore::from_records(records.clone())?;
        let records_by_id = records
            .into_iter()
            .map(|record| (record.key_id.clone(), record))
            .collect::<HashMap<_, _>>();
        let base_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let signers = manifest
            .signers
            .into_iter()
            .map(|config| {
                let record = records_by_id.get(&config.key_id).cloned().ok_or_else(|| {
                    anyhow!(
                        "PersonalDB signer key {} is absent from the trust store",
                        config.key_id
                    )
                })?;
                config.into_signer(base_dir, record)
            })
            .collect::<Result<Vec<_>>>()?;

        Self::new(trust_store, signers)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonalDbProtocolKeyringManifestV1 {
    format_version: u32,
    trusted_keys: Vec<PersonalDbTrustedKeyConfigV1>,
    signers: Vec<PersonalDbSignerConfigV1>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonalDbTrustedKeyConfigV1 {
    format_version: u32,
    signature_algorithm: SignatureAlgorithm,
    key_id: KeyId,
    key_generation: KeyGeneration,
    public_key_b64u: String,
    purpose: SignaturePurpose,
    database_scopes: Vec<personaldb_protocol::DatabaseId>,
    group_scopes: Vec<String>,
    valid_from_log_index: u64,
    valid_until_log_index: Option<u64>,
    status: PublicKeyStatus,
}

impl PersonalDbTrustedKeyConfigV1 {
    fn into_trust_record(self) -> Result<PublicKeyTrustRecord> {
        let public_key = decode_canonical_public_key(&self.public_key_b64u)?;
        let record = PublicKeyTrustRecord {
            format_version: self.format_version,
            signature_algorithm: self.signature_algorithm,
            key_id: self.key_id,
            key_generation: self.key_generation,
            public_key,
            purpose: self.purpose,
            database_scopes: self.database_scopes,
            group_scopes: self.group_scopes,
            valid_from_log_index: self.valid_from_log_index,
            valid_until_log_index: self.valid_until_log_index,
            status: self.status,
        };
        record.validate()?;
        Ok(record)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonalDbSignerConfigV1 {
    key_id: KeyId,
    private_key_pkcs8_path: PathBuf,
}

impl PersonalDbSignerConfigV1 {
    fn into_signer(
        self,
        base_dir: &Path,
        trust_record: PublicKeyTrustRecord,
    ) -> Result<Arc<dyn ProtocolSigner>> {
        let key_path = if self.private_key_pkcs8_path.is_absolute() {
            self.private_key_pkcs8_path
        } else {
            base_dir.join(self.private_key_pkcs8_path)
        };
        validate_private_key_file(&key_path)?;
        let bytes = std::fs::read(&key_path)
            .with_context(|| format!("read PersonalDB {} signing key", trust_record.purpose))?;

        let signer = match Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(
            &bytes,
            trust_record.clone(),
        ) {
            Ok(signer) => signer,
            Err(der_error) => {
                let pem = std::str::from_utf8(&bytes).map_err(|_| der_error.clone())?;
                Ed25519ProtocolSigner::from_pkcs8_pem_with_trust_record(pem, trust_record)
                    .map_err(|_| der_error)?
            }
        };
        Ok(Arc::new(signer))
    }
}

pub(crate) fn signature_envelope_to_proto(
    envelope: &SignatureEnvelopeV1,
) -> WireSignatureEnvelopeV1 {
    WireSignatureEnvelopeV1 {
        format_version: envelope.format_version,
        hash_algorithm: envelope.hash_algorithm as i32,
        signature_algorithm: envelope.signature_algorithm as i32,
        key_id: envelope.key_id.to_string(),
        signature: envelope.signature.as_bytes().to_vec(),
    }
}

pub(crate) fn signature_envelope_from_proto(
    proto: WireSignatureEnvelopeV1,
) -> Result<SignatureEnvelopeV1> {
    SignatureEnvelopeV1::decode_canonical(&proto.encode_to_vec())
        .context("decode canonical PersonalDB signature envelope")
}

fn decode_canonical_public_key(value: &str) -> Result<Ed25519PublicKey> {
    if value.is_empty() || value.contains('=') {
        bail!("PersonalDB Ed25519 public key must use unpadded base64url");
    }
    let decoded = URL_SAFE_NO_PAD
        .decode(value)
        .map_err(|_| anyhow!("PersonalDB Ed25519 public key is not canonical base64url"))?;
    if URL_SAFE_NO_PAD.encode(&decoded) != value {
        bail!("PersonalDB Ed25519 public key is not canonical base64url");
    }
    Ed25519PublicKey::try_from(decoded.as_slice()).map_err(Into::into)
}

fn validate_private_key_file(path: &Path) -> Result<()> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("inspect PersonalDB private key {}", path.display()))?;
    if !metadata.is_file() {
        bail!("PersonalDB private key path must name a regular file");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("PersonalDB private key must not be group- or world-accessible");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use personaldb_protocol::{
        DatabaseId, KeyTrustPolicy, SignatureDomain, SignatureError, SignatureMetadata,
        SignatureScope, SigningPayload,
    };

    #[derive(Debug, Clone)]
    struct TestSignable {
        metadata: SignatureMetadata,
        payload: Vec<u8>,
    }

    impl TestSignable {
        fn witness(log_index: u64) -> Self {
            Self {
                metadata: SignatureMetadata::for_domain(
                    SignaturePurpose::Witness,
                    SignatureDomain::CommitCertificate,
                    log_index,
                ),
                payload: b"anvil canonical PersonalDB witness payload".to_vec(),
            }
        }

        fn at_log_index(mut self, log_index: u64) -> Self {
            self.metadata.log_index = log_index;
            self
        }

        fn with_scope(mut self, scope: SignatureScope) -> Self {
            self.metadata.scope = scope;
            self
        }

        fn requiring_generation(mut self, generation: u64) -> Self {
            self.metadata.required_key_generation = Some(KeyGeneration::new(generation).unwrap());
            self
        }
    }

    impl ProtocolSignable for TestSignable {
        fn signature_metadata(&self) -> SignatureMetadata {
            self.metadata.clone()
        }

        fn signing_payload(&self) -> SigningPayload<'_> {
            SigningPayload::ExactBytes(&self.payload)
        }
    }

    #[test]
    fn wire_envelope_round_trips_canonically_and_rejects_malformed_shape() {
        let signer = signer(
            0x41,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let envelope = signer.sign(&TestSignable::witness(1)).unwrap();
        let wire = signature_envelope_to_proto(&envelope);

        assert_eq!(
            signature_envelope_from_proto(wire.clone()).unwrap(),
            envelope
        );
        assert_eq!(wire.format_version, 1);
        assert_eq!(wire.signature.len(), 64);

        let malformed = WireSignatureEnvelopeV1 {
            signature: vec![0; 63],
            ..wire
        };
        let error = signature_envelope_from_proto(malformed).unwrap_err();
        assert!(format!("{error:#}").contains("64 bytes"));
    }

    #[test]
    fn canonical_store_rejects_wrong_key_and_required_generation() {
        let signer_v1 = signer(
            0x42,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
        );
        let signer_v2 = signer(
            0x43,
            KeyTrustPolicy::new(KeyGeneration::new(2).unwrap(), SignaturePurpose::Witness, 0),
        );
        let object = TestSignable::witness(7);
        let envelope = signer_v1.sign(&object).unwrap();
        let store = PublicKeyTrustStore::from_records([
            signer_v1.trust_record().clone(),
            signer_v2.trust_record().clone(),
        ])
        .unwrap();

        let mut wrong_key = envelope.clone();
        wrong_key.key_id = signer_v2.trust_record().key_id.clone();
        assert!(matches!(
            store.verify(&object, &wrong_key),
            Err(SignatureError::InvalidSignature)
        ));

        let generation_two = object.requiring_generation(2);
        assert!(matches!(
            store.verify(&generation_two, &envelope),
            Err(SignatureError::RequiredKeyGenerationMismatch { .. })
        ));
    }

    #[test]
    fn canonical_store_enforces_exact_database_and_group_scope() {
        let policy =
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0)
                .with_database_scopes(vec![DatabaseId::new("db-a")])
                .with_group_scopes(vec!["group-a".to_string()]);
        let signer = signer(0x44, policy);
        let matching = TestSignable::witness(1).with_scope(SignatureScope::for_database_group(
            DatabaseId::new("db-a"),
            "group-a",
        ));
        let envelope = signer.sign(&matching).unwrap();
        let store = PublicKeyTrustStore::from_records([signer.trust_record().clone()]).unwrap();
        store.verify(&matching, &envelope).unwrap();

        let wrong_database = matching
            .clone()
            .with_scope(SignatureScope::for_database_group(
                DatabaseId::new("db-b"),
                "group-a",
            ));
        assert!(matches!(
            store.verify(&wrong_database, &envelope),
            Err(SignatureError::DatabaseScopeMismatch { .. })
        ));
        let wrong_group = matching.with_scope(SignatureScope::for_database_group(
            DatabaseId::new("db-a"),
            "group-b",
        ));
        assert!(matches!(
            store.verify(&wrong_group, &envelope),
            Err(SignatureError::GroupScopeMismatch { .. })
        ));
    }

    #[test]
    fn canonical_store_preserves_history_across_rotation_and_status_cutoffs() {
        let old = signer(
            0x45,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0)
                .with_valid_until(100)
                .with_status(PublicKeyStatus::Retiring),
        );
        let new = signer(
            0x46,
            KeyTrustPolicy::new(
                KeyGeneration::new(2).unwrap(),
                SignaturePurpose::Witness,
                100,
            ),
        );
        let old_object = TestSignable::witness(99);
        let new_object = TestSignable::witness(100);
        let old_envelope = old.sign(&old_object).unwrap();
        let new_envelope = new.sign(&new_object).unwrap();
        let rotating_store = PublicKeyTrustStore::from_records([
            old.trust_record().clone(),
            new.trust_record().clone(),
        ])
        .unwrap();

        rotating_store.verify(&old_object, &old_envelope).unwrap();
        rotating_store.verify(&new_object, &new_envelope).unwrap();
        assert!(matches!(
            rotating_store.verify(&old_object.clone().at_log_index(100), &old_envelope),
            Err(SignatureError::KeyRetired {
                log_index: 100,
                valid_until: 100,
                ..
            })
        ));
        assert!(matches!(
            rotating_store.verify(&new_object.at_log_index(99), &new_envelope),
            Err(SignatureError::KeyNotYetValid {
                log_index: 99,
                valid_from: 100,
                ..
            })
        ));
        rotating_store.verify(&old_object, &old_envelope).unwrap();

        let revoked = signer(
            0x47,
            KeyTrustPolicy::new(KeyGeneration::new(3).unwrap(), SignaturePurpose::Witness, 0)
                .with_valid_until(50)
                .with_status(PublicKeyStatus::RevokedFuture),
        );
        let revoked_history = TestSignable::witness(49);
        let revoked_envelope = revoked.sign(&revoked_history).unwrap();
        let revoked_store =
            PublicKeyTrustStore::from_records([revoked.trust_record().clone()]).unwrap();
        revoked_store
            .verify(&revoked_history, &revoked_envelope)
            .unwrap();
        assert!(matches!(
            revoked_store.verify(&revoked_history.clone().at_log_index(50), &revoked_envelope),
            Err(SignatureError::KeyRevoked {
                log_index: 50,
                valid_until: 50,
                ..
            })
        ));

        let active = signer(
            0x48,
            KeyTrustPolicy::new(KeyGeneration::new(4).unwrap(), SignaturePurpose::Witness, 0),
        );
        let compromised_history = TestSignable::witness(29);
        let compromised_envelope = active.sign(&compromised_history).unwrap();
        let mut compromised_record = active.trust_record().clone();
        compromised_record.valid_until_log_index = Some(30);
        compromised_record.status = PublicKeyStatus::Compromised;
        let compromised_store = PublicKeyTrustStore::from_records([compromised_record]).unwrap();
        compromised_store
            .verify(&compromised_history, &compromised_envelope)
            .unwrap();
        assert!(matches!(
            compromised_store.verify(&compromised_history.at_log_index(30), &compromised_envelope),
            Err(SignatureError::KeyCompromised {
                log_index: 30,
                valid_until: 30,
                ..
            })
        ));
    }

    #[test]
    fn production_keyring_requires_active_signers_for_every_anvil_purpose() {
        let witness = Arc::new(signer(
            0x49,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0)
                .with_valid_until(10)
                .with_status(PublicKeyStatus::Retiring),
        )) as Arc<dyn ProtocolSigner>;
        let store = PublicKeyTrustStore::from_records([witness.trust_record().clone()]).unwrap();

        assert!(
            PersonalDbProtocolKeyring::new(store, [witness])
                .unwrap_err()
                .to_string()
                .contains("must use an active trust record")
        );
    }

    #[test]
    fn manifest_loader_uses_canonical_trust_records_and_pkcs8_keys() {
        let directory = tempfile::tempdir().unwrap();
        let mut trusted_keys = Vec::new();
        let mut signer_configs = Vec::new();
        for (seed, purpose, filename) in [
            (0x51, SignaturePurpose::GroupControl, "group-control.pk8"),
            (0x52, SignaturePurpose::Snapshot, "snapshot.pk8"),
            (0x53, SignaturePurpose::Witness, "witness.pk8"),
        ] {
            let policy = KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), purpose, 0);
            let signer = signer(seed, policy);
            let record = signer.trust_record();
            let key_path = directory.path().join(filename);
            std::fs::write(&key_path, pkcs8(seed)).unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
                    .unwrap();
            }
            trusted_keys.push(serde_json::json!({
                "format_version": record.format_version,
                "signature_algorithm": record.signature_algorithm,
                "key_id": record.key_id,
                "key_generation": record.key_generation,
                "public_key_b64u": URL_SAFE_NO_PAD.encode(record.public_key.as_bytes()),
                "purpose": record.purpose,
                "database_scopes": record.database_scopes,
                "group_scopes": record.group_scopes,
                "valid_from_log_index": record.valid_from_log_index,
                "valid_until_log_index": record.valid_until_log_index,
                "status": record.status,
            }));
            signer_configs.push(serde_json::json!({
                "key_id": record.key_id,
                "private_key_pkcs8_path": filename,
            }));
        }
        let manifest = serde_json::json!({
            "format_version": 1,
            "trusted_keys": trusted_keys,
            "signers": signer_configs,
        });
        let manifest_path = directory.path().join("keyring.json");
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        let keyring = PersonalDbProtocolKeyring::from_manifest_file(manifest_path).unwrap();
        assert_eq!(keyring.trust_store().len(), 3);
        keyring.sign(&TestSignable::witness(1)).unwrap();
    }

    fn signer(seed: u8, policy: KeyTrustPolicy) -> Ed25519ProtocolSigner {
        Ed25519ProtocolSigner::from_pkcs8_der(&pkcs8(seed), policy).unwrap()
    }

    fn pkcs8(seed: u8) -> Vec<u8> {
        let mut bytes = hex::decode("302e020100300506032b657004220420").unwrap();
        bytes.extend([seed; 32]);
        bytes
    }
}
