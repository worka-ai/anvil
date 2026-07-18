use crate::{
    anvil_api::SignatureEnvelopeV1 as WireSignatureEnvelopeV1,
    personaldb_signer_protocol::{
        PersonalDbSignerResponse, PersonalDbSigningObject, decode_signer_response,
        encode_signer_request, read_bounded_frame, write_bounded_frame,
    },
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use personaldb_protocol::{
    Ed25519PublicKey, KeyGeneration, KeyId, PublicKeyStatus, PublicKeyTrustRecord,
    PublicKeyTrustStore, SignatureAlgorithm, SignatureEnvelopeV1, SignaturePurpose,
};
use prost::Message;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const PERSONALDB_SIGNER_TIMEOUT: Duration = Duration::from_secs(2);
const REQUIRED_PERSONALDB_SIGNER_PURPOSES: [SignaturePurpose; 4] = [
    SignaturePurpose::GroupControl,
    SignaturePurpose::ProposalAdmission,
    SignaturePurpose::Snapshot,
    SignaturePurpose::Witness,
];

#[async_trait]
pub trait PersonalDbSignerProvider: Send + Sync + fmt::Debug {
    fn purpose(&self) -> SignaturePurpose;
    fn key_id(&self) -> &KeyId;

    async fn sign(&self, object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1>;
}

#[derive(Clone)]
pub struct PersonalDbProtocolKeyring {
    trust_store: PublicKeyTrustStore,
    providers: Arc<HashMap<SignaturePurpose, Arc<dyn PersonalDbSignerProvider>>>,
}

impl fmt::Debug for PersonalDbProtocolKeyring {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let signer_key_ids = self
            .providers
            .iter()
            .map(|(purpose, provider)| (purpose, provider.key_id()))
            .collect::<Vec<_>>();
        formatter
            .debug_struct("PersonalDbProtocolKeyring")
            .field("trust_store", &self.trust_store)
            .field("signer_key_ids", &signer_key_ids)
            .finish()
    }
}

impl PersonalDbProtocolKeyring {
    fn from_providers(
        trust_store: PublicKeyTrustStore,
        providers: impl IntoIterator<Item = Arc<dyn PersonalDbSignerProvider>>,
    ) -> Result<Self> {
        if trust_store.is_empty() {
            bail!("PersonalDB protocol trust store must not be empty");
        }

        let mut by_purpose = HashMap::new();
        let mut key_ids = HashSet::new();
        for provider in providers {
            let purpose = provider.purpose();
            let record = trust_store.get(provider.key_id()).ok_or_else(|| {
                anyhow!(
                    "PersonalDB {} signer key {} is absent from the trust store",
                    purpose,
                    provider.key_id()
                )
            })?;
            if record.status != PublicKeyStatus::Active {
                bail!(
                    "PersonalDB {} signer must use an active trust record",
                    purpose
                );
            }
            if record.purpose != purpose {
                bail!(
                    "PersonalDB {} signer key {} is trusted only for {}",
                    purpose,
                    record.key_id,
                    record.purpose
                );
            }
            if !key_ids.insert(record.key_id.clone()) {
                bail!(
                    "PersonalDB signer key {} is shared by more than one purpose",
                    record.key_id
                );
            }
            if by_purpose.insert(purpose, provider).is_some() {
                bail!(
                    "PersonalDB protocol keyring has more than one {} signer",
                    purpose
                );
            }
        }

        for purpose in REQUIRED_PERSONALDB_SIGNER_PURPOSES {
            if !by_purpose.contains_key(&purpose) {
                bail!("PersonalDB protocol keyring is missing the {purpose} signer");
            }
        }

        Ok(Self {
            trust_store,
            providers: Arc::new(by_purpose),
        })
    }

    pub fn trust_store(&self) -> &PublicKeyTrustStore {
        &self.trust_store
    }

    pub(crate) fn trust_record_for_purpose(
        &self,
        purpose: SignaturePurpose,
    ) -> Result<&PublicKeyTrustRecord> {
        let provider = self
            .providers
            .get(&purpose)
            .ok_or_else(|| anyhow!("PersonalDB {purpose} signer is not configured"))?;
        self.trust_store.get(provider.key_id()).ok_or_else(|| {
            anyhow!(
                "PersonalDB {purpose} signer key {} is absent from the trust store",
                provider.key_id()
            )
        })
    }

    pub(crate) async fn sign(
        &self,
        object: PersonalDbSigningObject,
    ) -> Result<SignatureEnvelopeV1> {
        object.validate()?;
        let metadata = object.metadata();
        let provider = self
            .providers
            .get(&metadata.purpose)
            .ok_or_else(|| anyhow!("PersonalDB {} signer is not configured", metadata.purpose))?;
        let envelope = provider.sign(&object).await?;
        if &envelope.key_id != provider.key_id() {
            bail!(
                "PersonalDB {} signer returned key {}, expected {}",
                metadata.purpose,
                envelope.key_id,
                provider.key_id()
            );
        }
        self.trust_store.verify(&object, &envelope)?;
        Ok(envelope)
    }

    pub fn from_manifest_file(path: impl AsRef<Path>) -> Result<Self> {
        let manifest = PersonalDbProtocolSigningManifest::from_file(path)?;
        let providers = manifest
            .endpoints
            .values()
            .cloned()
            .map(|endpoint| {
                Arc::new(UnixPersonalDbSignerProvider { endpoint })
                    as Arc<dyn PersonalDbSignerProvider>
            })
            .collect::<Vec<_>>();
        Self::from_providers(manifest.trust_store, providers)
    }

    #[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
    #[doc(hidden)]
    pub fn new_test_only(
        trust_store: PublicKeyTrustStore,
        signers: impl IntoIterator<Item = Arc<dyn personaldb_protocol::ProtocolSigner>>,
    ) -> Result<Self> {
        let providers = signers
            .into_iter()
            .map(|signer| {
                Arc::new(TestOnlyInProcessSignerProvider { signer })
                    as Arc<dyn PersonalDbSignerProvider>
            })
            .collect::<Vec<_>>();
        Self::from_providers(trust_store, providers)
    }
}

#[derive(Debug, Clone)]
pub struct PersonalDbSignerEndpoint {
    pub purpose: SignaturePurpose,
    pub key_id: KeyId,
    pub socket_path: PathBuf,
}

#[derive(Debug, Clone)]
pub struct PersonalDbProtocolSigningManifest {
    trust_store: PublicKeyTrustStore,
    endpoints: HashMap<SignaturePurpose, PersonalDbSignerEndpoint>,
}

impl PersonalDbProtocolSigningManifest {
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            bail!("PersonalDB protocol signing manifest path is required");
        }
        let raw = std::fs::read(path).with_context(|| {
            format!(
                "read PersonalDB protocol signing manifest {}",
                path.display()
            )
        })?;
        let value: serde_json::Value =
            serde_json::from_slice(&raw).context("parse PersonalDB protocol signing manifest")?;
        if value.get("signers").is_some() {
            bail!(
                "production PersonalDB startup refuses file-backed in-process signers; configure signer_endpoints"
            );
        }
        let manifest: PersonalDbProtocolSigningManifestV1 =
            serde_json::from_value(value).context("parse PersonalDB protocol signing manifest")?;
        if manifest.format_version != 1 {
            bail!("unsupported PersonalDB protocol signing manifest version");
        }

        let records = manifest
            .trusted_keys
            .into_iter()
            .map(PersonalDbTrustedKeyConfigV1::into_trust_record)
            .collect::<Result<Vec<_>>>()?;
        let trust_store = PublicKeyTrustStore::from_records(records)?;
        if trust_store.is_empty() {
            bail!("PersonalDB protocol trust store must not be empty");
        }

        let mut endpoints = HashMap::new();
        let mut socket_paths = HashSet::new();
        let mut key_ids = HashSet::new();
        for config in manifest.signer_endpoints {
            if !REQUIRED_PERSONALDB_SIGNER_PURPOSES.contains(&config.purpose) {
                bail!(
                    "PersonalDB {} is not an Anvil control-object signing purpose",
                    config.purpose
                );
            }
            if !config.socket_path.is_absolute() {
                bail!(
                    "PersonalDB {} signer socket path must be absolute",
                    config.purpose
                );
            }
            if config.socket_path.as_os_str().is_empty()
                || config.socket_path.as_os_str().as_encoded_bytes().len() > 100
            {
                bail!(
                    "PersonalDB {} signer socket path exceeds the Unix socket bound",
                    config.purpose
                );
            }
            let record = trust_store.get(&config.key_id).ok_or_else(|| {
                anyhow!(
                    "PersonalDB {} signer key {} is absent from the trust store",
                    config.purpose,
                    config.key_id
                )
            })?;
            if record.purpose != config.purpose {
                bail!(
                    "PersonalDB {} endpoint uses key {} trusted only for {}",
                    config.purpose,
                    config.key_id,
                    record.purpose
                );
            }
            if record.status != PublicKeyStatus::Active {
                bail!(
                    "PersonalDB {} endpoint must use an active trust record",
                    config.purpose
                );
            }
            if !socket_paths.insert(config.socket_path.clone()) {
                bail!("PersonalDB signer endpoints must use distinct socket paths");
            }
            if !key_ids.insert(config.key_id.clone()) {
                bail!("PersonalDB signer endpoints must use distinct key IDs");
            }
            let endpoint = PersonalDbSignerEndpoint {
                purpose: config.purpose,
                key_id: config.key_id,
                socket_path: config.socket_path,
            };
            if endpoints.insert(endpoint.purpose, endpoint).is_some() {
                bail!(
                    "PersonalDB protocol signing manifest has more than one {} endpoint",
                    config.purpose
                );
            }
        }
        for purpose in REQUIRED_PERSONALDB_SIGNER_PURPOSES {
            if !endpoints.contains_key(&purpose) {
                bail!("PersonalDB protocol signing manifest is missing the {purpose} endpoint");
            }
        }

        Ok(Self {
            trust_store,
            endpoints,
        })
    }

    pub fn trust_store(&self) -> &PublicKeyTrustStore {
        &self.trust_store
    }

    pub fn endpoint(&self, purpose: SignaturePurpose) -> Option<&PersonalDbSignerEndpoint> {
        self.endpoints.get(&purpose)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PersonalDbProtocolSigningManifestV1 {
    format_version: u32,
    trusted_keys: Vec<PersonalDbTrustedKeyConfigV1>,
    signer_endpoints: Vec<PersonalDbSignerEndpointConfigV1>,
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
struct PersonalDbSignerEndpointConfigV1 {
    purpose: SignaturePurpose,
    key_id: KeyId,
    socket_path: PathBuf,
}

#[derive(Clone)]
struct UnixPersonalDbSignerProvider {
    endpoint: PersonalDbSignerEndpoint,
}

impl fmt::Debug for UnixPersonalDbSignerProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnixPersonalDbSignerProvider")
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

#[async_trait]
impl PersonalDbSignerProvider for UnixPersonalDbSignerProvider {
    fn purpose(&self) -> SignaturePurpose {
        self.endpoint.purpose
    }

    fn key_id(&self) -> &KeyId {
        &self.endpoint.key_id
    }

    async fn sign(&self, object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1> {
        let metadata = object.metadata();
        if metadata.purpose != self.endpoint.purpose {
            bail!(
                "PersonalDB {} endpoint cannot sign {} objects",
                self.endpoint.purpose,
                metadata.purpose
            );
        }

        #[cfg(unix)]
        {
            let request = encode_signer_request(object)?;
            let mut stream = tokio::time::timeout(
                PERSONALDB_SIGNER_TIMEOUT,
                tokio::net::UnixStream::connect(&self.endpoint.socket_path),
            )
            .await
            .context("connect to PersonalDB signer timed out")?
            .with_context(|| {
                format!(
                    "connect to PersonalDB {} signer at {}",
                    self.endpoint.purpose,
                    self.endpoint.socket_path.display()
                )
            })?;
            tokio::time::timeout(
                PERSONALDB_SIGNER_TIMEOUT,
                write_bounded_frame(&mut stream, &request),
            )
            .await
            .context("write to PersonalDB signer timed out")??;
            let response =
                tokio::time::timeout(PERSONALDB_SIGNER_TIMEOUT, read_bounded_frame(&mut stream))
                    .await
                    .context("read from PersonalDB signer timed out")??;
            match decode_signer_response(&response)? {
                PersonalDbSignerResponse::Signature(envelope) => Ok(envelope),
                PersonalDbSignerResponse::Rejected { code, message } => {
                    bail!("PersonalDB signer rejected request ({code}): {message}")
                }
            }
        }

        #[cfg(not(unix))]
        {
            let _ = object;
            bail!("PersonalDB production signing requires Unix-domain signer endpoints")
        }
    }
}

#[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
struct TestOnlyInProcessSignerProvider {
    signer: Arc<dyn personaldb_protocol::ProtocolSigner>,
}

#[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
impl fmt::Debug for TestOnlyInProcessSignerProvider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TestOnlyInProcessSignerProvider")
            .field("trust_record", self.signer.trust_record())
            .finish()
    }
}

#[cfg(any(test, feature = "test-in-process-personaldb-signers"))]
#[async_trait]
impl PersonalDbSignerProvider for TestOnlyInProcessSignerProvider {
    fn purpose(&self) -> SignaturePurpose {
        self.signer.trust_record().purpose
    }

    fn key_id(&self) -> &KeyId {
        &self.signer.trust_record().key_id
    }

    async fn sign(&self, object: &PersonalDbSigningObject) -> Result<SignatureEnvelopeV1> {
        self.signer.sign(object).map_err(Into::into)
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

#[cfg(test)]
mod tests {
    use super::*;
    use personaldb_protocol::{
        DatabaseId, Ed25519ProtocolSigner, KeyTrustPolicy, ProtocolSignable, ProtocolSigner,
        PublicKeyEnvelopeV1, SignatureDomain, SignatureError, SignatureMetadata, SignatureScope,
        SigningPayload, signing_preimage,
    };
    use serde::Deserialize;
    use sha2::{Digest as _, Sha256};
    use std::str::FromStr as _;

    const SHARED_SIGNING_VECTOR_JSON: &str =
        include_str!("../tests/fixtures/protocol-signing-v1.json");

    #[derive(Debug, Deserialize)]
    #[serde(deny_unknown_fields)]
    struct SharedSigningVector {
        schema: String,
        id: String,
        private_key_seed_hex: String,
        public_key_hex: String,
        public_key_envelope_hex: String,
        key_id: String,
        purpose: String,
        domain: String,
        signed_payload_version: u32,
        key_generation: u64,
        log_index: u64,
        payload_utf8: String,
        payload_hex: String,
        payload_sha256_hex: String,
        signing_preimage_hex: String,
        signature_hex: String,
        signature_envelope_hex: String,
    }

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
    fn shared_personaldb_signing_vector_crosses_anvil_wire_boundary() {
        let vector: SharedSigningVector = serde_json::from_str(SHARED_SIGNING_VECTOR_JSON).unwrap();
        assert_eq!(vector.schema, "personaldb.protocol-signing-vector.v1");
        assert_eq!(vector.id, "rfc8032-witness-commit-certificate-exact-bytes");
        let purpose = SignaturePurpose::from_str(&vector.purpose).unwrap();
        let domain = SignatureDomain::from_str(&vector.domain).unwrap();
        assert_eq!(
            domain.signed_payload_version(),
            vector.signed_payload_version
        );

        let payload = hex::decode(&vector.payload_hex).unwrap();
        assert_eq!(payload, vector.payload_utf8.as_bytes());
        assert_eq!(
            hex::encode(Sha256::digest(&payload)),
            vector.payload_sha256_hex
        );
        let preimage = signing_preimage(
            domain,
            vector.signed_payload_version,
            SigningPayload::ExactBytes(&payload),
        )
        .unwrap();
        assert_eq!(
            hex::encode(preimage.as_bytes()),
            vector.signing_preimage_hex
        );

        let mut private_key = hex::decode("302e020100300506032b657004220420").unwrap();
        private_key.extend(hex::decode(&vector.private_key_seed_hex).unwrap());
        let signer = Ed25519ProtocolSigner::from_pkcs8_der(
            &private_key,
            KeyTrustPolicy::new(
                KeyGeneration::new(vector.key_generation).unwrap(),
                purpose,
                vector.log_index,
            ),
        )
        .unwrap();
        assert_eq!(signer.trust_record().key_id.as_str(), vector.key_id);
        assert_eq!(
            hex::encode(signer.trust_record().public_key.as_bytes()),
            vector.public_key_hex
        );
        assert_eq!(
            hex::encode(
                PublicKeyEnvelopeV1::for_ed25519(signer.trust_record().public_key)
                    .encode_deterministic()
                    .unwrap()
            ),
            vector.public_key_envelope_hex
        );

        let object = TestSignable {
            metadata: SignatureMetadata::for_domain(purpose, domain, vector.log_index),
            payload,
        };
        let envelope = signer.sign(&object).unwrap();
        assert_eq!(
            hex::encode(envelope.signature.as_bytes()),
            vector.signature_hex
        );
        assert_eq!(
            hex::encode(envelope.encode_deterministic().unwrap()),
            vector.signature_envelope_hex
        );

        let wire = signature_envelope_to_proto(&envelope);
        let decoded = signature_envelope_from_proto(wire).unwrap();
        assert_eq!(decoded, envelope);
        let trust_store =
            PublicKeyTrustStore::from_records([signer.trust_record().clone()]).unwrap();
        trust_store.verify(&object, &decoded).unwrap();
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
    fn test_keyring_requires_active_signers_for_every_anvil_purpose() {
        let witness = Arc::new(signer(
            0x49,
            KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0)
                .with_valid_until(10)
                .with_status(PublicKeyStatus::Retiring),
        )) as Arc<dyn ProtocolSigner>;
        let store = PublicKeyTrustStore::from_records([witness.trust_record().clone()]).unwrap();

        assert!(
            PersonalDbProtocolKeyring::new_test_only(store, [witness])
                .unwrap_err()
                .to_string()
                .contains("must use an active trust record")
        );
    }

    #[test]
    fn production_manifest_loads_public_trust_and_remote_endpoints_only() {
        let directory = tempfile::tempdir().unwrap();
        let manifest_path = write_manifest(directory.path(), valid_manifest(directory.path()));

        let keyring = PersonalDbProtocolKeyring::from_manifest_file(manifest_path).unwrap();
        assert_eq!(keyring.trust_store().len(), 4);
    }

    #[test]
    fn production_manifest_rejects_wrong_purpose_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        let mut manifest = valid_manifest(directory.path());
        let witness_key = endpoint_for_purpose(&manifest, "witness")["key_id"].clone();
        manifest["signer_endpoints"][0]["key_id"] = witness_key;
        let manifest_path = write_manifest(directory.path(), manifest);

        let error = PersonalDbProtocolKeyring::from_manifest_file(manifest_path).unwrap_err();
        assert!(error.to_string().contains("trusted only for witness"));
    }

    #[test]
    fn production_manifest_rejects_shared_endpoint_and_key() {
        let directory = tempfile::tempdir().unwrap();
        let mut shared_endpoint = valid_manifest(directory.path());
        shared_endpoint["signer_endpoints"][1]["socket_path"] =
            shared_endpoint["signer_endpoints"][0]["socket_path"].clone();
        let path = write_manifest(directory.path(), shared_endpoint);
        assert!(
            PersonalDbProtocolKeyring::from_manifest_file(path)
                .unwrap_err()
                .to_string()
                .contains("distinct socket paths")
        );

        let mut shared_key = valid_manifest(directory.path());
        let duplicate = shared_key["signer_endpoints"][0].clone();
        shared_key["signer_endpoints"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({
                "purpose": duplicate["purpose"].clone(),
                "key_id": duplicate["key_id"].clone(),
                "socket_path": directory.path().join("duplicate.sock"),
            }));
        let path = write_manifest(directory.path(), shared_key);
        assert!(
            PersonalDbProtocolKeyring::from_manifest_file(path)
                .unwrap_err()
                .to_string()
                .contains("distinct key IDs")
        );
    }

    #[test]
    fn production_manifest_rejects_missing_purpose_endpoint() {
        let directory = tempfile::tempdir().unwrap();
        let mut manifest = valid_manifest(directory.path());
        manifest["signer_endpoints"]
            .as_array_mut()
            .unwrap()
            .retain(|endpoint| endpoint["purpose"] != "snapshot");
        let path = write_manifest(directory.path(), manifest);

        assert!(
            PersonalDbProtocolKeyring::from_manifest_file(path)
                .unwrap_err()
                .to_string()
                .contains("missing the snapshot endpoint")
        );
    }

    #[test]
    fn production_refuses_file_backed_signer_mode() {
        let directory = tempfile::tempdir().unwrap();
        let mut manifest = valid_manifest(directory.path());
        let endpoints = manifest
            .as_object_mut()
            .unwrap()
            .remove("signer_endpoints")
            .unwrap();
        manifest["signers"] = serde_json::json!([{
            "key_id": endpoints[0]["key_id"].clone(),
            "private_key_pkcs8_path": "witness.pk8"
        }]);
        let path = write_manifest(directory.path(), manifest);

        let error = PersonalDbProtocolKeyring::from_manifest_file(path).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("refuses file-backed in-process signers")
        );
    }

    #[test]
    fn production_manifest_schema_rejects_private_key_paths_and_in_process_modes() {
        let directory = tempfile::tempdir().unwrap();
        let mut cases = Vec::new();

        let mut root_private_key = valid_manifest(directory.path());
        root_private_key["private_key_pkcs8_path"] = serde_json::json!("/run/secrets/witness.pk8");
        cases.push(("root private key", root_private_key));

        let mut trusted_key_private_key = valid_manifest(directory.path());
        trusted_key_private_key["trusted_keys"][0]["private_key_pkcs8_path"] =
            serde_json::json!("/run/secrets/group-control.pk8");
        cases.push(("trusted-key private key", trusted_key_private_key));

        let mut endpoint_private_key = valid_manifest(directory.path());
        endpoint_private_key["signer_endpoints"][0]["private_key_pkcs8_path"] =
            serde_json::json!("/run/secrets/group-control.pk8");
        cases.push(("endpoint private key", endpoint_private_key));

        let mut root_mode = valid_manifest(directory.path());
        root_mode["signer_mode"] = serde_json::json!("in_process");
        cases.push(("root in-process mode", root_mode));

        let mut endpoint_mode = valid_manifest(directory.path());
        endpoint_mode["signer_endpoints"][0]["mode"] = serde_json::json!("in_process");
        cases.push(("endpoint in-process mode", endpoint_mode));

        for (name, manifest) in cases {
            let path = write_manifest(directory.path(), manifest);
            let error = PersonalDbProtocolKeyring::from_manifest_file(path).unwrap_err();
            assert!(
                format!("{error:#}").contains("unknown field"),
                "{name} was not rejected by the closed production manifest schema: {error:#}"
            );
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn unavailable_remote_signer_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let manifest_path = write_manifest(directory.path(), valid_manifest(directory.path()));
        let keyring = PersonalDbProtocolKeyring::from_manifest_file(manifest_path).unwrap();

        let error = keyring
            .sign(PersonalDbSigningObject::CommitCertificate(
                sample_commit_certificate(),
            ))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("connect to PersonalDB witness signer"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn malformed_remote_signature_fails_before_trust_verification() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("witness.sock");
        let mut manifest = valid_manifest(directory.path());
        let witness_endpoint_index = endpoint_index_for_purpose(&manifest, "witness");
        manifest["signer_endpoints"][witness_endpoint_index]["socket_path"] =
            serde_json::json!(socket_path.to_string_lossy());
        let manifest_path = write_manifest(directory.path(), manifest.clone());
        let keyring = PersonalDbProtocolKeyring::from_manifest_file(manifest_path).unwrap();
        let witness_key = endpoint_for_purpose(&manifest, "witness")["key_id"]
            .as_str()
            .unwrap()
            .to_string();

        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_bounded_frame(&mut stream).await.unwrap();
            let response = crate::personaldb_signer_protocol::encode_test_signer_response(
                WireSignatureEnvelopeV1 {
                    format_version: 1,
                    hash_algorithm: 1,
                    signature_algorithm: 1,
                    key_id: witness_key,
                    signature: vec![0; 63],
                },
            )
            .unwrap();
            write_bounded_frame(&mut stream, &response).await.unwrap();
        });

        let error = keyring
            .sign(PersonalDbSigningObject::CommitCertificate(
                sample_commit_certificate(),
            ))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("64 bytes"));
        server.await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn remote_signature_is_verified_by_the_coordinator() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = directory.path().join("witness.sock");
        let mut manifest = valid_manifest(directory.path());
        let witness_endpoint_index = endpoint_index_for_purpose(&manifest, "witness");
        manifest["signer_endpoints"][witness_endpoint_index]["socket_path"] =
            serde_json::json!(socket_path.to_string_lossy());
        let manifest_path = write_manifest(directory.path(), manifest.clone());
        let keyring = PersonalDbProtocolKeyring::from_manifest_file(manifest_path).unwrap();
        let witness_key = endpoint_for_purpose(&manifest, "witness")["key_id"]
            .as_str()
            .unwrap()
            .to_string();

        let listener = tokio::net::UnixListener::bind(&socket_path).unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let _ = read_bounded_frame(&mut stream).await.unwrap();
            let response = crate::personaldb_signer_protocol::encode_test_signer_response(
                WireSignatureEnvelopeV1 {
                    format_version: 1,
                    hash_algorithm: 1,
                    signature_algorithm: 1,
                    key_id: witness_key,
                    signature: vec![0; 64],
                },
            )
            .unwrap();
            write_bounded_frame(&mut stream, &response).await.unwrap();
        });

        let error = keyring
            .sign(PersonalDbSigningObject::CommitCertificate(
                sample_commit_certificate(),
            ))
            .await
            .unwrap_err();
        assert!(format!("{error:#}").contains("invalid Ed25519 signature"));
        server.await.unwrap();
    }

    fn signer(seed: u8, policy: KeyTrustPolicy) -> Ed25519ProtocolSigner {
        Ed25519ProtocolSigner::from_pkcs8_der(&pkcs8(seed), policy).unwrap()
    }

    fn pkcs8(seed: u8) -> Vec<u8> {
        let mut bytes = hex::decode("302e020100300506032b657004220420").unwrap();
        bytes.extend([seed; 32]);
        bytes
    }

    fn valid_manifest(directory: &Path) -> serde_json::Value {
        let mut trusted_keys = Vec::new();
        let mut signer_endpoints = Vec::new();
        for (seed, purpose, socket_name) in [
            (0x51, SignaturePurpose::GroupControl, "group-control.sock"),
            (
                0x52,
                SignaturePurpose::ProposalAdmission,
                "proposal-admission.sock",
            ),
            (0x53, SignaturePurpose::Snapshot, "snapshot.sock"),
            (0x54, SignaturePurpose::Witness, "witness.sock"),
        ] {
            let signer = signer(
                seed,
                KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), purpose, 0),
            );
            let record = signer.trust_record();
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
            signer_endpoints.push(serde_json::json!({
                "purpose": purpose,
                "key_id": record.key_id,
                "socket_path": directory.join(socket_name),
            }));
        }
        serde_json::json!({
            "format_version": 1,
            "trusted_keys": trusted_keys,
            "signer_endpoints": signer_endpoints,
        })
    }

    fn endpoint_index_for_purpose(manifest: &serde_json::Value, purpose: &str) -> usize {
        manifest["signer_endpoints"]
            .as_array()
            .unwrap()
            .iter()
            .position(|endpoint| endpoint["purpose"].as_str() == Some(purpose))
            .unwrap()
    }

    fn endpoint_for_purpose<'a>(
        manifest: &'a serde_json::Value,
        purpose: &str,
    ) -> &'a serde_json::Value {
        &manifest["signer_endpoints"][endpoint_index_for_purpose(manifest, purpose)]
    }

    fn write_manifest(directory: &Path, manifest: serde_json::Value) -> PathBuf {
        let path = directory.join("personaldb-signing.json");
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        path
    }

    fn sample_commit_certificate() -> crate::personaldb_control::PersonalDbCommitCertificate {
        crate::personaldb_control::PersonalDbCommitCertificate {
            format_version: 2,
            tenant_id: "tenant".to_string(),
            database_id: "db".to_string(),
            log_index: 1,
            previous_log_hash: hex::encode([0; 32]),
            entry_hash: hex::encode([1; 32]),
            changeset_payload_hash: hex::encode([2; 32]),
            verified_envelope_hash: hex::encode([3; 32]),
            client_log_epoch: 1,
            membership_epoch: 1,
            policy_epoch: 1,
            leader_replica_id: "replica".to_string(),
            voter_acks_hash: hex::encode([4; 32]),
            authz_revision: 1,
            witness_node_id: "node".to_string(),
            witnessed_at: "2026-07-17T00:00:00Z".to_string(),
            certificate_hash: None,
            witness_signature: None,
        }
    }
}
