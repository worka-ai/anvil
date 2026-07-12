use super::*;
use crate::anvil_api::{
    CompareAndSwapRootRequest, PrepareRootRequest, RootPrepareReceipt,
    root_register_internal_client::RootRegisterInternalClient,
};
use anyhow::Context;
use std::collections::BTreeSet;
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

impl CoreStore {
    pub async fn read_internal_root_anchor(
        &self,
        root_anchor_key: &str,
        min_generation: u64,
    ) -> Result<CoreInternalRootAnchorRead> {
        let anchor = self
            .read_latest_root_anchor(root_anchor_key)
            .await?
            .ok_or_else(|| anyhow!("CoreStore root anchor not found"))?;
        if anchor.root_generation < min_generation {
            bail!("CoreStore root anchor generation is below requested minimum");
        }
        let bytes = encode_root_anchor_record(&anchor)?;
        Ok(CoreInternalRootAnchorRead {
            root_key_hash: anchor.root_key_hash,
            generation: anchor.root_generation,
            root_anchor_record: bytes.clone(),
            root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }
    pub async fn read_internal_root_anchor_by_hash(
        &self,
        root_key_hash_value: &str,
        min_generation: u64,
    ) -> Result<CoreInternalRootAnchorRead> {
        validate_hash(root_key_hash_value, "internal root key hash")?;
        let mut latest = None;
        for row in self.meta.scan_prefix(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_prefix(root_key_hash_value),
        )? {
            let anchor = decode_root_cache_row(&row.payload)?;
            if anchor.root_key_hash != root_key_hash_value
                || anchor.root_generation < min_generation
            {
                continue;
            }
            if latest
                .as_ref()
                .is_none_or(|current: &CoreRootAnchorRecord| {
                    anchor.root_generation > current.root_generation
                })
            {
                latest = Some(anchor);
            }
        }
        let Some(anchor) = latest else {
            bail!("CoreStore root anchor not found")
        };
        if !self
            .verify_root_anchor_chain(root_key_hash_value, &anchor.root_anchor_key, &anchor)
            .await?
        {
            bail!("CoreStore root anchor chain verification failed");
        }
        let bytes = encode_root_anchor_record(&anchor)?;
        Ok(CoreInternalRootAnchorRead {
            root_key_hash: anchor.root_key_hash,
            generation: anchor.root_generation,
            root_anchor_record: bytes.clone(),
            root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }

    pub(super) async fn publish_root_anchor_generation(
        &self,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        validate_root_anchor_record(anchor)?;
        if anchor.root_generation == 0 {
            return self.write_root_anchor_generation_local(anchor).await;
        }
        let anchor_bytes = encode_root_anchor_record(anchor)?;
        let expected_generation = anchor.root_generation.saturating_sub(1);
        let expected_root_hash = anchor.previous_root_hash.clone();
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;
        let replicas = self.select_coremeta_replicas(&profile).await?;
        let mut prepare_receipts = Vec::new();
        let mut prepare_errors = Vec::new();
        let prepare_started_at = Instant::now();
        for replica in &replicas {
            let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                self.prepare_root_anchor_locally(
                    replica,
                    anchor,
                    &anchor_bytes,
                    expected_generation,
                    &expected_root_hash,
                )
                .await
            } else {
                self.prepare_root_anchor_remotely(
                    replica,
                    anchor,
                    &anchor_bytes,
                    expected_generation,
                    &expected_root_hash,
                )
                .await
            };
            match result.and_then(|receipt| {
                self.verify_root_prepare_receipt(
                    anchor,
                    expected_generation,
                    &anchor_bytes,
                    &receipt,
                )?;
                Ok(receipt)
            }) {
                Ok(receipt) => prepare_receipts.push(receipt),
                Err(error) => prepare_errors.push(format!("{}: {error}", replica.node_id)),
            }
        }
        if prepare_receipts.len() < profile.prepare_quorum {
            crate::perf::record_root_register_cas_duration(
                "prepare",
                profile.profile_id.as_str(),
                "quorum_failed",
                prepare_started_at.elapsed(),
            );
            crate::perf::record_failover_vote_total("root_prepare", "quorum_failed");
            bail!(
                "CoreStore root prepare quorum was not reached for {}: {}",
                anchor.root_key_hash,
                prepare_errors.join("; ")
            );
        }
        crate::perf::record_root_register_cas_duration(
            "prepare",
            profile.profile_id.as_str(),
            "ok",
            prepare_started_at.elapsed(),
        );
        crate::perf::record_failover_vote_total("root_prepare", "ok");
        self.validate_root_prepare_quorum(
            anchor,
            expected_generation,
            &anchor_bytes,
            &prepare_receipts,
        )?;

        let certificate_hash = anchor
            .core_meta_commit_certificate_hash
            .as_deref()
            .ok_or_else(|| anyhow!("CoreStore root anchor missing CoreMeta commit certificate"))?;
        let evidence = self
            .read_coremeta_commit_evidence(certificate_hash)?
            .ok_or_else(|| anyhow!("CoreStore root anchor CoreMeta commit evidence is missing"))?;
        let certificate = decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
            &evidence.certificate_bytes,
            "CoreMeta commit certificate",
        )?;
        let certificate_persist_receipts = evidence
            .certificate_persist_receipt_bytes
            .iter()
            .map(|bytes| {
                decode_deterministic_proto::<crate::anvil_api::CoreMetaCertificatePersistReceipt>(
                    bytes,
                    "CoreMeta certificate persist receipt",
                )
            })
            .collect::<Result<Vec<_>>>()?;

        let mut write_count = 0usize;
        let mut write_errors = Vec::new();
        let cas_started_at = Instant::now();
        for replica in &replicas {
            let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                self.compare_and_swap_root_anchor_locally(
                    anchor,
                    expected_generation,
                    &expected_root_hash,
                )
                .await
            } else {
                self.compare_and_swap_root_anchor_remotely(
                    replica,
                    anchor,
                    &anchor_bytes,
                    expected_generation,
                    &expected_root_hash,
                    &certificate,
                    &certificate_persist_receipts,
                    &prepare_receipts,
                )
                .await
                .map(|_| ())
            };
            match result {
                Ok(()) => write_count += 1,
                Err(error) => write_errors.push(format!("{}: {error}", replica.node_id)),
            }
        }
        if write_count < profile.certificate_persist_quorum {
            crate::perf::record_root_register_cas_duration(
                "compare_and_swap",
                profile.profile_id.as_str(),
                "quorum_failed",
                cas_started_at.elapsed(),
            );
            crate::perf::record_failover_vote_total("root_compare_and_swap", "quorum_failed");
            bail!(
                "CoreStore root CAS quorum was not reached for {}: {}",
                anchor.root_key_hash,
                write_errors.join("; ")
            );
        }
        crate::perf::record_root_register_cas_duration(
            "compare_and_swap",
            profile.profile_id.as_str(),
            "ok",
            cas_started_at.elapsed(),
        );
        crate::perf::record_failover_vote_total("root_compare_and_swap", "ok");
        Ok(())
    }

    async fn prepare_root_anchor_locally(
        &self,
        replica: &LocalShardPlacement,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        expected_root_hash: &str,
    ) -> Result<RootPrepareReceipt> {
        self.validate_root_cas_precondition(
            &anchor.root_key_hash,
            expected_generation,
            expected_root_hash,
            anchor,
        )
        .await?;
        let new_root_hash = format!("sha256:{}", sha256_hex(anchor_bytes));
        Ok(RootPrepareReceipt {
            replica_node_id: replica.node_id.clone(),
            root_key_hash: anchor.root_key_hash.clone(),
            expected_generation,
            post_generation: anchor.root_generation,
            new_root_hash: new_root_hash.clone(),
            signature: self.sign_internal_core_receipt(&new_root_hash)?,
        })
    }

    async fn prepare_root_anchor_remotely(
        &self,
        replica: &LocalShardPlacement,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        expected_root_hash: &str,
    ) -> Result<RootPrepareReceipt> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "root register remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let endpoint = normalise_grpc_endpoint(&replica.public_api_addr)?;
        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await
            .with_context(|| format!("connect root register replica at {endpoint}"))?;
        let mut client = RootRegisterInternalClient::new(channel);
        let mut request = tonic::Request::new(PrepareRootRequest {
            header: Some(self.internal_request_header("root.prepare")?),
            root_key_hash: anchor.root_key_hash.clone(),
            expected_generation,
            expected_root_hash: expected_root_hash.to_string(),
            new_root_anchor_record: anchor_bytes.to_vec(),
            partition_owner_fence: anchor.partition_owner_fence,
        });
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {bearer}"))
                .context("encode root register internal bearer token")?,
        );
        Ok(client.prepare_root(request).await?.into_inner())
    }

    async fn compare_and_swap_root_anchor_locally(
        &self,
        anchor: &CoreRootAnchorRecord,
        expected_generation: u64,
        expected_root_hash: &str,
    ) -> Result<()> {
        self.validate_root_cas_precondition(
            &anchor.root_key_hash,
            expected_generation,
            expected_root_hash,
            anchor,
        )
        .await?;
        self.write_root_anchor_generation_local(anchor).await
    }

    async fn compare_and_swap_root_anchor_remotely(
        &self,
        replica: &LocalShardPlacement,
        anchor: &CoreRootAnchorRecord,
        anchor_bytes: &[u8],
        expected_generation: u64,
        expected_root_hash: &str,
        certificate: &crate::anvil_api::CoreMetaCommitCertificate,
        certificate_persist_receipts: &[crate::anvil_api::CoreMetaCertificatePersistReceipt],
        prepare_receipts: &[RootPrepareReceipt],
    ) -> Result<crate::anvil_api::RootAnchorWrite> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "root register remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let endpoint = normalise_grpc_endpoint(&replica.public_api_addr)?;
        let channel = Endpoint::from_shared(endpoint.clone())?
            .connect()
            .await
            .with_context(|| format!("connect root register replica at {endpoint}"))?;
        let mut client = RootRegisterInternalClient::new(channel);
        let mut request = tonic::Request::new(CompareAndSwapRootRequest {
            header: Some(self.internal_request_header("root.compare_and_swap")?),
            root_key_hash: anchor.root_key_hash.clone(),
            expected_generation,
            expected_root_hash: expected_root_hash.to_string(),
            new_root_anchor_record: anchor_bytes.to_vec(),
            partition_owner_fence: anchor.partition_owner_fence,
            core_meta_commit_certificate: Some(certificate.clone()),
            core_meta_commit_certificate_hash: certificate.certificate_hash.clone(),
            certificate_persist_receipts: certificate_persist_receipts.to_vec(),
            prepare_receipts: prepare_receipts.to_vec(),
        });
        request.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(format!("Bearer {bearer}"))
                .context("encode root register internal bearer token")?,
        );
        Ok(client.compare_and_swap_root(request).await?.into_inner())
    }

    async fn validate_root_cas_precondition(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        new_anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        if new_anchor.root_key_hash != root_key_hash_value {
            bail!("CoreStore root CAS root key hash mismatch");
        }
        if new_anchor.root_generation != expected_generation.saturating_add(1) {
            bail!("CoreStore root CAS post generation mismatch");
        }
        match self
            .read_internal_root_anchor_by_hash(root_key_hash_value, 0)
            .await
        {
            Ok(current) => {
                let new_anchor_hash = hash_root_anchor_record(new_anchor)?;
                if current.generation == new_anchor.root_generation
                    && current.root_anchor_hash == new_anchor_hash
                {
                    return Ok(());
                }
                if current.generation != expected_generation {
                    bail!("CoreStore root CAS expected generation mismatch");
                }
                if !expected_root_hash.is_empty() && current.root_anchor_hash != expected_root_hash
                {
                    bail!(
                        "CoreStore root CAS expected root hash mismatch: root_key_hash={} generation={} expected={} current={}",
                        root_key_hash_value,
                        expected_generation,
                        expected_root_hash,
                        current.root_anchor_hash
                    );
                }
                if new_anchor.previous_root_hash != current.root_anchor_hash {
                    bail!("CoreStore root CAS previous hash mismatch");
                }
            }
            Err(_) => {
                if expected_generation != 0 || !expected_root_hash.is_empty() {
                    bail!("CoreStore root CAS expected generation missing");
                }
            }
        }
        Ok(())
    }

    fn validate_root_prepare_quorum(
        &self,
        anchor: &CoreRootAnchorRecord,
        expected_generation: u64,
        anchor_bytes: &[u8],
        receipts: &[RootPrepareReceipt],
    ) -> Result<()> {
        let mut replicas = BTreeSet::new();
        for receipt in receipts {
            self.verify_root_prepare_receipt(anchor, expected_generation, anchor_bytes, receipt)?;
            replicas.insert(receipt.replica_node_id.as_str());
        }
        if replicas.len() < self.default_coremeta_quorum_profile()?.prepare_quorum {
            bail!("CoreStore root prepare quorum has duplicate replicas");
        }
        Ok(())
    }

    fn verify_root_prepare_receipt(
        &self,
        anchor: &CoreRootAnchorRecord,
        expected_generation: u64,
        anchor_bytes: &[u8],
        receipt: &RootPrepareReceipt,
    ) -> Result<()> {
        let new_root_hash = format!("sha256:{}", sha256_hex(anchor_bytes));
        if receipt.root_key_hash != anchor.root_key_hash
            || receipt.expected_generation != expected_generation
            || receipt.post_generation != anchor.root_generation
            || receipt.new_root_hash != new_root_hash
        {
            bail!("CoreStore root prepare receipt scope mismatch");
        }
        self.verify_internal_core_receipt_signature(
            &receipt.replica_node_id,
            &receipt.new_root_hash,
            &receipt.signature,
        )
    }

    pub async fn compare_and_swap_internal_root_anchor(
        &self,
        root_key_hash_value: &str,
        expected_generation: u64,
        expected_root_hash: &str,
        new_root_anchor_record: &[u8],
    ) -> Result<CoreInternalRootAnchorRead> {
        validate_hash(root_key_hash_value, "internal root key hash")?;
        if !expected_root_hash.is_empty() {
            validate_hash(expected_root_hash, "internal expected root hash")?;
        }
        let anchor = decode_root_anchor_record(new_root_anchor_record)?;
        if anchor.root_key_hash != root_key_hash_value {
            bail!("CoreStore internal root CAS root key hash mismatch");
        }
        if anchor.root_generation != expected_generation.saturating_add(1) {
            bail!("CoreStore internal root CAS post generation mismatch");
        }
        let current = self
            .read_internal_root_anchor_by_hash(root_key_hash_value, 0)
            .await
            .ok();
        if let Some(current) = current {
            if current.generation != expected_generation {
                bail!("CoreStore internal root CAS expected generation mismatch");
            }
            if !expected_root_hash.is_empty() && current.root_anchor_hash != expected_root_hash {
                bail!("CoreStore internal root CAS expected root hash mismatch");
            }
        } else if expected_generation != 0 {
            bail!("CoreStore internal root CAS missing expected generation");
        }
        self.write_root_anchor_generation_local(&anchor).await?;
        let bytes = encode_root_anchor_record(&anchor)?;
        Ok(CoreInternalRootAnchorRead {
            root_key_hash: anchor.root_key_hash,
            generation: anchor.root_generation,
            root_anchor_record: bytes.clone(),
            root_anchor_hash: format!("sha256:{}", sha256_hex(&bytes)),
        })
    }
}
