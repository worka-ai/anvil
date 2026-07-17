#[cfg(unix)]
mod unix {
    use anvil_core::{
        personaldb_signer_protocol::{
            PersonalDbSignerErrorCode, decode_signer_request, encode_signer_error,
            encode_signer_success, read_bounded_frame, write_bounded_frame,
        },
        personaldb_signing::PersonalDbProtocolSigningManifest,
    };
    use anyhow::{Context, Result, anyhow, bail};
    use clap::Parser;
    use personaldb_protocol::{
        Ed25519ProtocolSigner, KeyId, ProtocolSignable, ProtocolSigner, PublicKeyStatus,
        PublicKeyTrustRecord, SignaturePurpose,
    };
    use std::{
        collections::HashSet,
        fs::FileType,
        os::unix::fs::{FileTypeExt, PermissionsExt},
        path::{Path, PathBuf},
        sync::Arc,
        time::Duration,
    };
    use tokio::net::{UnixListener, UnixStream};
    use zeroize::Zeroizing;

    const SIGNER_IO_TIMEOUT: Duration = Duration::from_secs(2);

    #[derive(Debug, Parser)]
    #[command(
        name = "anvil-signer",
        about = "Purpose-scoped local signer for typed Anvil PersonalDB protocol objects"
    )]
    pub struct SignerConfig {
        #[arg(long, env = "ANVIL_SIGNER_TRUST_MANIFEST_PATH")]
        trust_manifest_path: PathBuf,

        #[arg(long, env = "ANVIL_SIGNER_PURPOSE")]
        purpose: SignaturePurpose,

        #[arg(long, env = "ANVIL_SIGNER_KEY_ID")]
        key_id: KeyId,

        #[arg(long, env = "ANVIL_SIGNER_SOCKET_PATH")]
        socket_path: PathBuf,

        #[arg(long, env = "ANVIL_SIGNER_PRIVATE_KEY_PKCS8_PATH")]
        private_key_pkcs8_path: PathBuf,

        #[arg(
            long,
            env = "ANVIL_SIGNER_ALLOWED_PEER_UIDS",
            value_delimiter = ',',
            num_args = 1..
        )]
        allowed_peer_uid: Vec<u32>,
    }

    pub fn main() -> Result<()> {
        tracing_subscriber::fmt::init();
        let config = SignerConfig::parse();
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?
            .block_on(run(config))
    }

    async fn run(config: SignerConfig) -> Result<()> {
        let allowed_peer_uids = config
            .allowed_peer_uid
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if allowed_peer_uids.is_empty() || allowed_peer_uids.len() != config.allowed_peer_uid.len()
        {
            bail!(
                "PersonalDB signer peer UID allowlist must be nonempty and contain no duplicates"
            );
        }
        if !config.private_key_pkcs8_path.is_absolute() {
            bail!("PersonalDB signer private-key path must be absolute");
        }

        let manifest = PersonalDbProtocolSigningManifest::from_file(&config.trust_manifest_path)?;
        let endpoint = manifest.endpoint(config.purpose).ok_or_else(|| {
            anyhow!(
                "PersonalDB signing manifest has no {} endpoint",
                config.purpose
            )
        })?;
        if endpoint.key_id != config.key_id {
            bail!(
                "PersonalDB {} signer key ID does not match its manifest endpoint",
                config.purpose
            );
        }
        if endpoint.socket_path != config.socket_path {
            bail!(
                "PersonalDB {} signer socket does not match its manifest endpoint",
                config.purpose
            );
        }
        let trust_record = manifest
            .trust_store()
            .get(&config.key_id)
            .cloned()
            .ok_or_else(|| anyhow!("PersonalDB signer key is absent from the trust store"))?;
        validate_role_record(&trust_record, config.purpose)?;
        let signer = Arc::new(load_signer(&config.private_key_pkcs8_path, trust_record)?);
        let listener = bind_role_socket(&config.socket_path)?;
        let _socket_guard = SocketCleanup {
            path: config.socket_path.clone(),
        };
        let allowed_peer_uids = Arc::new(allowed_peer_uids);

        tracing::info!(
            purpose = %config.purpose,
            key_id = %config.key_id,
            socket_path = %config.socket_path.display(),
            "PersonalDB signer ready"
        );
        loop {
            let (stream, _) = listener.accept().await.context("accept signer client")?;
            let signer = Arc::clone(&signer);
            let allowed_peer_uids = Arc::clone(&allowed_peer_uids);
            let purpose = config.purpose;
            tokio::spawn(async move {
                if let Err(error) =
                    handle_connection(stream, purpose, signer, allowed_peer_uids).await
                {
                    tracing::warn!(%error, %purpose, "PersonalDB signer request failed");
                }
            });
        }
    }

    async fn handle_connection(
        mut stream: UnixStream,
        purpose: SignaturePurpose,
        signer: Arc<Ed25519ProtocolSigner>,
        allowed_peer_uids: Arc<HashSet<u32>>,
    ) -> Result<()> {
        let credentials = stream
            .peer_cred()
            .context("read PersonalDB signer peer credentials")?;
        if !allowed_peer_uids.contains(&credentials.uid()) {
            bail!(
                "PersonalDB signer peer UID {} is not allowed",
                credentials.uid()
            );
        }

        let request = tokio::time::timeout(SIGNER_IO_TIMEOUT, read_bounded_frame(&mut stream))
            .await
            .context("read PersonalDB signer request timed out")??;
        let response = process_request(purpose, signer.as_ref(), &request);
        tokio::time::timeout(
            SIGNER_IO_TIMEOUT,
            write_bounded_frame(&mut stream, &response),
        )
        .await
        .context("write PersonalDB signer response timed out")??;
        Ok(())
    }

    fn process_request(
        purpose: SignaturePurpose,
        signer: &Ed25519ProtocolSigner,
        request: &[u8],
    ) -> Vec<u8> {
        let object = match decode_signer_request(request) {
            Ok(object) => object,
            Err(error) => {
                return encode_error(
                    PersonalDbSignerErrorCode::InvalidRequest,
                    format!("{error:#}"),
                );
            }
        };
        let metadata = object.signature_metadata();
        if metadata.purpose != purpose {
            return encode_error(
                PersonalDbSignerErrorCode::WrongPurpose,
                format!(
                    "{} signer cannot sign {} objects",
                    purpose, metadata.purpose
                ),
            );
        }
        match signer.sign(&object) {
            Ok(envelope) => encode_signer_success(&envelope)
                .unwrap_or_else(|error| encode_internal_error(format!("{error:#}"))),
            Err(error) => encode_error(PersonalDbSignerErrorCode::SigningFailed, error.to_string()),
        }
    }

    fn encode_error(code: PersonalDbSignerErrorCode, message: String) -> Vec<u8> {
        let bounded = if message.len() > 1024 {
            "PersonalDB signer rejected request".to_string()
        } else {
            message
        };
        encode_signer_error(code, bounded)
            .unwrap_or_else(|error| encode_internal_error(format!("{error:#}")))
    }

    fn encode_internal_error(message: String) -> Vec<u8> {
        encode_signer_error(
            PersonalDbSignerErrorCode::Internal,
            if message.len() > 1024 {
                "PersonalDB signer internal error"
            } else {
                &message
            },
        )
        .expect("fixed PersonalDB signer internal error response is bounded")
    }

    fn validate_role_record(
        trust_record: &PublicKeyTrustRecord,
        purpose: SignaturePurpose,
    ) -> Result<()> {
        trust_record.validate()?;
        if trust_record.purpose != purpose {
            bail!(
                "PersonalDB signer purpose {} does not match trust-record purpose {}",
                purpose,
                trust_record.purpose
            );
        }
        if trust_record.status != PublicKeyStatus::Active {
            bail!("PersonalDB signer trust record must be active");
        }
        Ok(())
    }

    fn load_signer(
        path: &Path,
        trust_record: PublicKeyTrustRecord,
    ) -> Result<Ed25519ProtocolSigner> {
        validate_private_key_file(path)?;
        // Scrub the file-backed key input on every return path after key construction.
        let bytes = Zeroizing::new(
            std::fs::read(path)
                .with_context(|| format!("read PersonalDB signer key {}", path.display()))?,
        );
        match Ed25519ProtocolSigner::from_pkcs8_der_with_trust_record(
            bytes.as_slice(),
            trust_record.clone(),
        ) {
            Ok(signer) => Ok(signer),
            Err(der_error) => {
                let pem = std::str::from_utf8(bytes.as_slice()).map_err(|_| der_error.clone())?;
                Ed25519ProtocolSigner::from_pkcs8_pem_with_trust_record(pem, trust_record)
                    .map_err(Into::into)
            }
        }
    }

    fn validate_private_key_file(path: &Path) -> Result<()> {
        let metadata = std::fs::symlink_metadata(path)
            .with_context(|| format!("inspect PersonalDB signer key {}", path.display()))?;
        if !metadata.file_type().is_file() {
            bail!("PersonalDB signer private-key path must name a regular, non-symlink file");
        }
        if metadata.permissions().mode() & 0o077 != 0 {
            bail!("PersonalDB signer private key must not be group- or world-accessible");
        }
        Ok(())
    }

    fn bind_role_socket(path: &Path) -> Result<UnixListener> {
        let parent = path
            .parent()
            .ok_or_else(|| anyhow!("PersonalDB signer socket has no parent directory"))?;
        let metadata = std::fs::symlink_metadata(parent).with_context(|| {
            format!(
                "inspect PersonalDB signer socket directory {}",
                parent.display()
            )
        })?;
        if !metadata.file_type().is_dir() {
            bail!("PersonalDB signer socket parent must be a directory");
        }
        if metadata.permissions().mode() & 0o777 != 0o700 {
            bail!("PersonalDB signer socket directory must have mode 0700");
        }
        if let Ok(existing) = std::fs::symlink_metadata(path) {
            reject_non_socket(&existing.file_type())?;
            std::fs::remove_file(path).with_context(|| {
                format!("remove stale PersonalDB signer socket {}", path.display())
            })?;
        }
        let listener = UnixListener::bind(path)
            .with_context(|| format!("bind PersonalDB signer socket {}", path.display()))?;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).with_context(
            || {
                format!(
                    "set PersonalDB signer socket permissions {}",
                    path.display()
                )
            },
        )?;
        Ok(listener)
    }

    fn reject_non_socket(file_type: &FileType) -> Result<()> {
        if !file_type.is_socket() {
            bail!("PersonalDB signer refuses to replace a non-socket path");
        }
        Ok(())
    }

    struct SocketCleanup {
        path: PathBuf,
    }

    impl Drop for SocketCleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use anvil_core::{
            personaldb_control::PersonalDbGroupManifest,
            personaldb_signer_protocol::{
                PersonalDbSignerResponse, PersonalDbSigningObject, decode_signer_response,
                encode_signer_request,
            },
        };
        use personaldb_protocol::{
            KeyGeneration, KeyTrustPolicy, ProtocolSigner, PublicKeyTrustRecord,
        };
        use std::io::Write;

        #[test]
        fn witness_endpoint_rejects_group_control_object() {
            let signer = Ed25519ProtocolSigner::from_pkcs8_der(
                &pkcs8(0x33),
                KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
            )
            .unwrap();
            let object = PersonalDbSigningObject::GroupManifest(PersonalDbGroupManifest {
                format_version: 2,
                tenant_id: "tenant".to_string(),
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
            let request = encode_signer_request(&object).unwrap();

            let response = process_request(SignaturePurpose::Witness, &signer, &request);
            assert_eq!(
                decode_signer_response(&response).unwrap(),
                PersonalDbSignerResponse::Rejected {
                    code: "wrong-purpose".to_string(),
                    message: "witness signer cannot sign group-control objects".to_string(),
                }
            );
        }

        #[test]
        fn loads_der_and_pem_private_key_inputs() {
            let der = pkcs8(0x33);
            let pem = format!(
                "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
                "MC4CAQAwBQYDK2VwBCIEIDMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMz"
            );
            let trust_record = test_trust_record(&der);

            for input in [der, pem.into_bytes()] {
                let mut file = tempfile::NamedTempFile::new().unwrap();
                file.write_all(&input).unwrap();
                file.flush().unwrap();

                let loaded = load_signer(file.path(), trust_record.clone()).unwrap();
                assert_eq!(loaded.trust_record(), &trust_record);
            }
        }

        fn test_trust_record(pkcs8: &[u8]) -> PublicKeyTrustRecord {
            Ed25519ProtocolSigner::from_pkcs8_der(
                pkcs8,
                KeyTrustPolicy::new(KeyGeneration::new(1).unwrap(), SignaturePurpose::Witness, 0),
            )
            .unwrap()
            .trust_record()
            .clone()
        }

        fn pkcs8(seed: u8) -> Vec<u8> {
            let mut bytes = hex::decode("302e020100300506032b657004220420").unwrap();
            bytes.extend([seed; 32]);
            bytes
        }
    }
}

#[cfg(unix)]
fn main() -> anyhow::Result<()> {
    unix::main()
}

#[cfg(not(unix))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("anvil-signer requires Unix-domain sockets")
}
