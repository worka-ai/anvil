use super::*;
use crate::anvil_api::{
    CoreMetaBatchRequest, CoreMetaCommitCertificate as ApiCoreMetaCommitCertificate,
    CoreMetaPersistCommitRequest, CoreMetaRowMutation,
    core_meta_replication_internal_client::CoreMetaReplicationInternalClient,
};
use crate::mesh_lifecycle::{self, LifecycleState, NodeCapability};
use futures_util::StreamExt;
use tonic::metadata::MetadataValue;

const COREMETA_ROOT_COMMIT_CONCURRENCY: usize = 16;

#[derive(Debug, Clone)]
pub(crate) struct CoreMetaQuorumCommitOutcome {
    pub(crate) root_key_hash: String,
    pub(crate) post_root_generation: u64,
    pub(crate) certificate_hash: String,
    pub(crate) certificate_bytes: Vec<u8>,
    pub(crate) certificate_persist_receipt_hashes: Vec<String>,
    pub(crate) certificate_persist_receipts: Vec<CoreMetaCertificatePersistReceipt>,
    pub(crate) metadata_replica_node_ids: Vec<String>,
}

/// Commit feature-owned CoreMeta rows through CoreStore instead of opening the
/// local RocksDB store directly. Synchronous feature stores should become async
/// at their public write boundary and use this helper so rooted rows get quorum
/// evidence and local rows still pass through the single CoreStore boundary.
pub(crate) async fn commit_coremeta_batch_for_storage(
    storage: &crate::storage::Storage,
    transaction_id: &str,
    ops: &[CoreMetaBatchOp<'_>],
) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
    let store = CoreStore::new(storage.clone()).await?;
    store
        .commit_coremeta_batch_by_embedded_roots(transaction_id, ops)
        .await
}

impl CoreStore {
    pub(crate) async fn commit_coremeta_batch_for_root(
        &self,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        ops: &[CoreMetaBatchOp<'_>],
    ) -> Result<CoreMetaQuorumCommitOutcome> {
        let rows = self.meta.encode_batch_ops(ops)?;
        self.commit_coremeta_encoded_rows_for_root(
            root_key_hash,
            expected_root_generation,
            post_root_generation,
            transaction_id,
            rows,
        )
        .await
    }

    pub(crate) async fn commit_coremeta_batch_by_embedded_roots(
        &self,
        transaction_id: &str,
        ops: &[CoreMetaBatchOp<'_>],
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        validate_logical_id(transaction_id, "CoreMeta quorum transaction id")?;
        let mut local_rows = Vec::new();
        let mut replicated_rows = BTreeMap::<(String, u64), Vec<CoreMetaEncodedOwnedRow>>::new();

        for row in self.meta.encode_batch_ops(ops)? {
            if row.root_key_hash.is_empty() {
                local_rows.push(row);
            } else {
                replicated_rows
                    .entry((row.root_key_hash.clone(), row.root_generation))
                    .or_default()
                    .push(row);
            }
        }

        if !local_rows.is_empty() {
            let borrowed = borrow_encoded_rows(&local_rows);
            self.write_coremeta_encoded_rows(&borrowed)?;
        }

        let outcomes = futures_util::stream::iter(replicated_rows.into_iter().map(
            |((root_key_hash, post_root_generation), rows)| async move {
                let expected_root_generation = post_root_generation.saturating_sub(1);
                self.commit_coremeta_encoded_rows_for_root(
                    &root_key_hash,
                    expected_root_generation,
                    post_root_generation,
                    transaction_id,
                    rows,
                )
                .await
            },
        ))
        .buffer_unordered(COREMETA_ROOT_COMMIT_CONCURRENCY)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        Ok(outcomes)
    }

    pub(crate) async fn commit_coremeta_encoded_rows_for_root(
        &self,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        rows: Vec<CoreMetaEncodedOwnedRow>,
    ) -> Result<CoreMetaQuorumCommitOutcome> {
        validate_hash(root_key_hash, "CoreMeta quorum root key hash")?;
        validate_logical_id(transaction_id, "CoreMeta quorum transaction id")?;
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;
        if rows.is_empty() {
            bail!("CoreMeta quorum batch must contain at least one row");
        }
        for row in &rows {
            if row.root_key_hash != root_key_hash {
                bail!("CoreMeta quorum row root hash does not match batch root");
            }
            if row.root_generation != post_root_generation {
                bail!("CoreMeta quorum row generation does not match batch root generation");
            }
        }
        let row_hashes = encoded_row_hashes(&rows);
        let pending_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
            root_key_hash: root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id: transaction_id.to_string(),
            row_hashes: row_hashes.clone(),
        })?;
        let select_started_at = Instant::now();
        let replicas = self.select_coremeta_replicas(&profile).await?;
        crate::emit_test_timing(
            "coremeta.commit_for_root select_coremeta_replicas",
            select_started_at.elapsed(),
        );
        let prepare_started_at = Instant::now();
        let mut prepare_results = replicas
            .iter()
            .map(|replica| async {
                let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                    self.replicate_coremeta_batch_locally(
                        replica,
                        root_key_hash,
                        expected_root_generation,
                        post_root_generation,
                        transaction_id,
                        &pending_hash,
                        &rows,
                    )
                } else {
                    self.replicate_coremeta_batch_remotely(
                        replica,
                        root_key_hash,
                        expected_root_generation,
                        post_root_generation,
                        transaction_id,
                        &pending_hash,
                        &rows,
                    )
                    .await
                };
                (replica.node_id.clone(), result)
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut prepare_receipts = Vec::new();
        let mut prepare_errors = Vec::new();
        while let Some((node_id, result)) = prepare_results.next().await {
            match result.and_then(|receipt| {
                self.verify_internal_core_receipt_signature(
                    &receipt.replica_node_id,
                    &receipt.signed_payload_hash,
                    &receipt.signature,
                )?;
                Ok(receipt)
            }) {
                Ok(receipt) => prepare_receipts.push(receipt),
                Err(error) => prepare_errors.push(format!("{node_id}: {error}")),
            }
            if prepare_receipts.len() >= profile.prepare_quorum {
                break;
            }
        }
        drop(prepare_results);
        if prepare_receipts.len() < profile.prepare_quorum {
            record_corestore_trace_event("coremeta.replicate_pending", "quorum_failed");
            record_corestore_trace_event("coremeta.quorum_wait", "quorum_failed");
            crate::perf::record_duration(
                "anvil_coremeta_replication_duration_ms",
                &[
                    ("operation", "replicate_pending"),
                    ("root_kind", "root_anchor"),
                    ("profile", profile.profile_id.as_str()),
                    ("status", "quorum_failed"),
                ],
                prepare_started_at.elapsed(),
            );
            crate::perf::record_counter(
                "anvil_coremeta_quorum_total",
                &[
                    ("profile", profile.profile_id.as_str()),
                    ("outcome", "prepare_failed"),
                ],
                1,
            );
            bail!(
                "CoreMeta prepare quorum was not reached for {root_key_hash}: {}",
                prepare_errors.join("; ")
            );
        }
        crate::perf::record_duration(
            "anvil_coremeta_replication_duration_ms",
            &[
                ("operation", "replicate_pending"),
                ("root_kind", "root_anchor"),
                ("profile", profile.profile_id.as_str()),
                ("status", "ok"),
            ],
            prepare_started_at.elapsed(),
        );
        record_corestore_trace_event("coremeta.replicate_pending", "ok");
        record_corestore_trace_event("coremeta.quorum_wait", "prepare_ok");
        crate::emit_test_timing(
            "coremeta.commit_for_root replicate_pending_quorum",
            prepare_started_at.elapsed(),
        );
        crate::perf::record_counter(
            "anvil_coremeta_quorum_total",
            &[
                ("profile", profile.profile_id.as_str()),
                ("outcome", "prepare_ok"),
            ],
            1,
        );
        let certificate = build_commit_certificate(
            &profile,
            root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id.to_string(),
            pending_hash.clone(),
            prepare_receipts,
        )?;
        let committed_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
            root_key_hash: root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id: transaction_id.to_string(),
            pending_batch_hash: pending_hash,
            committed_row_hashes: row_hashes,
        })?;
        let persist_started_at = Instant::now();
        let mut persist_results = replicas
            .iter()
            .map(|replica| async {
                let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                    self.persist_coremeta_certificate_locally(
                        replica,
                        &certificate,
                        &committed_hash,
                        &rows,
                    )
                } else {
                    self.persist_coremeta_certificate_remotely(
                        replica,
                        &certificate,
                        &committed_hash,
                        &rows,
                    )
                    .await
                };
                (replica.node_id.clone(), result)
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut persist_receipts = Vec::new();
        let mut persist_errors = Vec::new();
        while let Some((node_id, result)) = persist_results.next().await {
            match result.and_then(|receipt| {
                self.verify_internal_core_receipt_signature(
                    &receipt.replica_node_id,
                    &receipt.signed_payload_hash,
                    &receipt.signature,
                )?;
                Ok(receipt)
            }) {
                Ok(receipt) => persist_receipts.push(receipt),
                Err(error) => persist_errors.push(format!("{node_id}: {error}")),
            }
            if persist_receipts.len() >= profile.certificate_persist_quorum {
                break;
            }
        }
        drop(persist_results);
        if persist_receipts.len() < profile.certificate_persist_quorum {
            record_corestore_trace_event("coremeta.persist_commit_certificate", "quorum_failed");
            record_corestore_trace_event("coremeta.quorum_wait", "quorum_failed");
            crate::perf::record_duration(
                "anvil_coremeta_replication_duration_ms",
                &[
                    ("operation", "persist_commit_certificate"),
                    ("root_kind", "root_anchor"),
                    ("profile", profile.profile_id.as_str()),
                    ("status", "quorum_failed"),
                ],
                persist_started_at.elapsed(),
            );
            crate::perf::record_counter(
                "anvil_coremeta_quorum_total",
                &[
                    ("profile", profile.profile_id.as_str()),
                    ("outcome", "certificate_persist_failed"),
                ],
                1,
            );
            bail!(
                "CoreMeta certificate persistence quorum was not reached for {root_key_hash}: {}",
                persist_errors.join("; ")
            );
        }
        crate::perf::record_duration(
            "anvil_coremeta_replication_duration_ms",
            &[
                ("operation", "persist_commit_certificate"),
                ("root_kind", "root_anchor"),
                ("profile", profile.profile_id.as_str()),
                ("status", "ok"),
            ],
            persist_started_at.elapsed(),
        );
        record_corestore_trace_event("coremeta.persist_commit_certificate", "ok");
        record_corestore_trace_event("coremeta.quorum_wait", "certificate_persist_ok");
        crate::emit_test_timing(
            "coremeta.commit_for_root persist_commit_certificate_quorum",
            persist_started_at.elapsed(),
        );
        crate::perf::record_counter(
            "anvil_coremeta_quorum_total",
            &[
                ("profile", profile.profile_id.as_str()),
                ("outcome", "certificate_persist_ok"),
            ],
            1,
        );
        validate_commit_evidence_with_verifier(
            &profile,
            &certificate,
            &persist_receipts,
            |node_id, signed_payload_hash, signature| {
                self.verify_internal_core_receipt_signature(node_id, signed_payload_hash, signature)
            },
        )?;
        let certificate_bytes =
            encode_deterministic_proto(&core_commit_certificate_to_api(&certificate));
        let persist_receipt_bytes = persist_receipts
            .iter()
            .map(|receipt| encode_deterministic_proto(&core_persist_receipt_to_api(receipt)))
            .collect::<Vec<_>>();
        let mut persist_receipt_hashes = persist_receipts
            .iter()
            .map(certificate_persist_receipt_payload_hash)
            .collect::<Result<Vec<_>>>()?;
        persist_receipt_hashes.sort();
        persist_receipt_hashes.dedup();
        let evidence_started_at = Instant::now();
        self.persist_coremeta_commit_evidence(
            root_key_hash,
            post_root_generation,
            transaction_id,
            &certificate.certificate_hash,
            &committed_hash,
            certificate_bytes.clone(),
            persist_receipt_hashes.clone(),
            persist_receipt_bytes,
        )?;
        crate::emit_test_timing(
            "coremeta.commit_for_root persist_commit_evidence",
            evidence_started_at.elapsed(),
        );
        Ok(CoreMetaQuorumCommitOutcome {
            root_key_hash: root_key_hash.to_string(),
            post_root_generation,
            certificate_hash: certificate.certificate_hash,
            certificate_bytes,
            certificate_persist_receipt_hashes: persist_receipt_hashes,
            certificate_persist_receipts: persist_receipts,
            metadata_replica_node_ids: replicas
                .iter()
                .take(profile.replica_count)
                .map(|replica| replica.node_id.clone())
                .collect(),
        })
    }

    pub(super) async fn select_coremeta_replicas(
        &self,
        profile: &CoreMetaQuorumProfile,
    ) -> Result<Vec<LocalShardPlacement>> {
        let mut candidates = self.active_coremeta_lifecycle_replicas(profile.prepare_quorum)?;

        if candidates.len() < profile.prepare_quorum {
            // During cluster genesis the mesh lifecycle rows that nominate
            // metadata replicas do not exist yet. Use the local bootstrap
            // control set only until lifecycle projection rows are installed.
            candidates = local_control_node_ids()
                .into_iter()
                .enumerate()
                .map(|(index, node_id)| {
                    let cell_id = format!("local-control-cell-{}", index + 1);
                    LocalShardPlacement {
                        is_local: true,
                        node_id,
                        region_id: self.node_identity.region_id.clone(),
                        cell_id: cell_id.clone(),
                        failure_domain: cell_id,
                        region_weight: 100,
                        cell_weight: 100,
                        public_api_addr: String::new(),
                    }
                })
                .collect::<Vec<_>>();
        }

        if candidates.len() < profile.prepare_quorum {
            bail!(
                "CoreMeta profile {} requires {} active metadata replicas for quorum, got {}",
                profile.profile_id,
                profile.prepare_quorum,
                candidates.len()
            );
        }
        let planned_replicas = candidates.len().min(profile.replica_count);
        Ok(choose_spread_placements(
            LocalErasureProfile {
                id: "metadata-r3-q2",
                codec_id: "logical-coremeta-r3-q2",
                data_shards: planned_replicas,
                parity_shards: 0,
                minimum_read_shards: profile.prepare_quorum,
                minimum_write_ack_shards: profile.prepare_quorum,
                logical_block_target_bytes: 0,
                max_shard_size_bytes: 0,
            },
            candidates,
            &[],
        )?)
    }

    fn active_coremeta_lifecycle_replicas(
        &self,
        prepare_quorum: usize,
    ) -> Result<Vec<LocalShardPlacement>> {
        let nodes = mesh_lifecycle::list_node_projections_with_core_store(self, None, None)?;
        let mut active_candidates = Vec::new();
        for node in nodes {
            if node.mesh_id != self.node_identity.mesh_id {
                continue;
            }
            if node.region != self.node_identity.region_id {
                continue;
            }
            if node.state != LifecycleState::Active {
                continue;
            }
            if !node.capabilities.contains(&NodeCapability::Metadata) {
                continue;
            }
            if node.public_api_addr.trim().is_empty() {
                continue;
            }
            self.register_node_receipt_signing_public_key(
                &node.node_id,
                &node.receipt_signing_public_key_proto,
            )?;
            let is_local = node.node_id == self.node_identity.node_id;
            let placement = LocalShardPlacement {
                is_local,
                node_id: node.node_id,
                region_id: node.region,
                cell_id: node.cell_id.clone(),
                failure_domain: node.cell_id,
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: node.public_api_addr,
            };
            active_candidates.push(placement);
        }
        Self::sort_coremeta_candidates(&mut active_candidates);
        if active_candidates.len() < prepare_quorum {
            return Ok(Vec::new());
        }
        Ok(active_candidates)
    }

    fn sort_coremeta_candidates(candidates: &mut [LocalShardPlacement]) {
        candidates.sort_by(|a, b| {
            b.region_weight
                .cmp(&a.region_weight)
                .then_with(|| b.cell_weight.cmp(&a.cell_weight))
                .then_with(|| a.region_id.cmp(&b.region_id))
                .then_with(|| a.failure_domain.cmp(&b.failure_domain))
                .then_with(|| a.cell_id.cmp(&b.cell_id))
                .then_with(|| a.node_id.cmp(&b.node_id))
        });
    }

    fn replicate_coremeta_batch_locally(
        &self,
        replica: &LocalShardPlacement,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        pending_batch_hash_value: &str,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<CoreMetaPrepareReceipt> {
        self.persist_coremeta_pending_batch_marker(
            root_key_hash,
            expected_root_generation,
            post_root_generation,
            transaction_id,
            pending_batch_hash_value,
            rows.len(),
        )?;
        let mut receipt = CoreMetaPrepareReceipt {
            replica_node_id: replica.node_id.clone(),
            write_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            pending_batch_hash: pending_batch_hash_value.to_string(),
            root_key_hash: root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id: transaction_id.to_string(),
            signed_payload_hash: String::new(),
            signature: Vec::new(),
        };
        receipt.signed_payload_hash = prepare_receipt_payload_hash(&receipt)?;
        receipt.signature = self.sign_internal_core_receipt(&receipt.signed_payload_hash)?;
        Ok(receipt)
    }

    async fn replicate_coremeta_batch_remotely(
        &self,
        replica: &LocalShardPlacement,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        pending_batch_hash_value: &str,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<CoreMetaPrepareReceipt> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreMeta remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let request_body = CoreMetaBatchRequest {
            header: Some(self.internal_request_header("coremeta.replicate_pending_batch")?),
            root_key_hash: root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id: transaction_id.to_string(),
            visibility_state: "pending".to_string(),
            mutations: rows_to_api_mutations(rows),
            pending_batch_hash: pending_batch_hash_value.to_string(),
        };
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreMeta internal bearer token")?;
        let receipt = self
            .internal_grpc_request(
                &replica.public_api_addr,
                "replicate CoreMeta batch",
                move |channel| {
                    let mut client = CoreMetaReplicationInternalClient::new(channel);
                    let mut request = tonic::Request::new(request_body.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .replicate_pending_batch(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            )
            .await
            .with_context(|| format!("replicate CoreMeta batch to {}", replica.node_id))?;
        api_prepare_receipt_to_core(receipt)
    }

    fn persist_coremeta_certificate_locally(
        &self,
        replica: &LocalShardPlacement,
        certificate: &CoreMetaCommitCertificate,
        committed_batch_hash_value: &str,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<CoreMetaCertificatePersistReceipt> {
        let mut receipt = CoreMetaCertificatePersistReceipt {
            replica_node_id: replica.node_id.clone(),
            write_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            certificate_hash: certificate.certificate_hash.clone(),
            committed_batch_hash: committed_batch_hash_value.to_string(),
            root_key_hash: certificate.root_key_hash.clone(),
            post_root_generation: certificate.post_root_generation,
            transaction_id: certificate.transaction_id.clone(),
            signed_payload_hash: String::new(),
            signature: Vec::new(),
        };
        receipt.signed_payload_hash = certificate_persist_receipt_payload_hash(&receipt)?;
        receipt.signature = self.sign_internal_core_receipt(&receipt.signed_payload_hash)?;
        let evidence_row = self.coremeta_commit_evidence_encoded_row(
            &certificate.root_key_hash,
            certificate.post_root_generation,
            &certificate.transaction_id,
            &certificate.certificate_hash,
            committed_batch_hash_value,
            encode_deterministic_proto(&core_commit_certificate_to_api(certificate)),
            vec![certificate_persist_receipt_payload_hash(&receipt)?],
            vec![encode_deterministic_proto(&core_persist_receipt_to_api(
                &receipt,
            ))],
        )?;
        let mut committed_rows = rows.to_vec();
        committed_rows.push(evidence_row);
        let borrowed = borrow_encoded_rows(&committed_rows);
        self.write_coremeta_encoded_rows(&borrowed)?;
        Ok(receipt)
    }

    async fn persist_coremeta_certificate_remotely(
        &self,
        replica: &LocalShardPlacement,
        certificate: &CoreMetaCommitCertificate,
        committed_batch_hash_value: &str,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<CoreMetaCertificatePersistReceipt> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreMeta remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let request_body = CoreMetaPersistCommitRequest {
            header: Some(self.internal_request_header("coremeta.persist_commit_certificate")?),
            commit_certificate: Some(core_commit_certificate_to_api(certificate)),
            committed_rows: rows_to_api_mutations(rows),
            committed_batch_hash: committed_batch_hash_value.to_string(),
        };
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreMeta internal bearer token")?;
        let receipt = self
            .internal_grpc_request(
                &replica.public_api_addr,
                "persist CoreMeta certificate",
                move |channel| {
                    let mut client = CoreMetaReplicationInternalClient::new(channel);
                    let mut request = tonic::Request::new(request_body.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .persist_commit_certificate(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            )
            .await
            .with_context(|| format!("persist CoreMeta certificate on {}", replica.node_id))?;
        api_persist_receipt_to_core(receipt)
    }
}

fn encoded_row_hashes(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<String> {
    let mut hashes = rows
        .iter()
        .map(|row| {
            core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            )
        })
        .collect::<Vec<_>>();
    hashes.sort();
    hashes.dedup();
    hashes
}

fn borrow_encoded_rows(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<CoreMetaEncodedRow<'_>> {
    rows.iter()
        .map(|row| CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        })
        .collect()
}

fn rows_to_api_mutations(rows: &[CoreMetaEncodedOwnedRow]) -> Vec<CoreMetaRowMutation> {
    rows.iter()
        .map(|row| CoreMetaRowMutation {
            column_family: row.cf.clone(),
            core_meta_key: row.core_meta_key.clone(),
            value_envelope: row.value_envelope.clone(),
            row_hash: core_meta_encoded_row_hash_with_delete(
                &row.cf,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            ),
            delete_marker: row.delete_marker,
        })
        .collect()
}

pub(super) fn core_commit_certificate_to_api(
    certificate: &CoreMetaCommitCertificate,
) -> ApiCoreMetaCommitCertificate {
    ApiCoreMetaCommitCertificate {
        root_key_hash: certificate.root_key_hash.clone(),
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id.clone(),
        pending_batch_hash: certificate.pending_batch_hash.clone(),
        prepare_receipts: certificate
            .prepare_receipts
            .iter()
            .map(core_prepare_receipt_to_api)
            .collect(),
        certificate_hash: certificate.certificate_hash.clone(),
    }
}

fn core_prepare_receipt_to_api(
    receipt: &CoreMetaPrepareReceipt,
) -> crate::anvil_api::CoreMetaPrepareReceipt {
    crate::anvil_api::CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signature: receipt.signature.clone(),
    }
}

pub(super) fn api_prepare_receipt_to_core(
    receipt: crate::anvil_api::CoreMetaPrepareReceipt,
) -> Result<CoreMetaPrepareReceipt> {
    let mut core = CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash,
        root_key_hash: receipt.root_key_hash,
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signed_payload_hash: String::new(),
        signature: receipt.signature,
    };
    core.signed_payload_hash = prepare_receipt_payload_hash(&core)?;
    Ok(core)
}

pub(super) fn api_commit_certificate_to_core(
    certificate: crate::anvil_api::CoreMetaCommitCertificate,
) -> Result<CoreMetaCommitCertificate> {
    let prepare_receipts = certificate
        .prepare_receipts
        .into_iter()
        .map(api_prepare_receipt_to_core)
        .collect::<Result<Vec<_>>>()?;
    Ok(CoreMetaCommitCertificate {
        root_key_hash: certificate.root_key_hash,
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id,
        pending_batch_hash: certificate.pending_batch_hash,
        prepare_receipts,
        certificate_hash: certificate.certificate_hash,
    })
}

pub(super) fn api_persist_receipt_to_core(
    receipt: crate::anvil_api::CoreMetaCertificatePersistReceipt,
) -> Result<CoreMetaCertificatePersistReceipt> {
    let mut core = CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash,
        committed_batch_hash: receipt.committed_batch_hash,
        root_key_hash: receipt.root_key_hash,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signed_payload_hash: String::new(),
        signature: receipt.signature,
    };
    core.signed_payload_hash = certificate_persist_receipt_payload_hash(&core)?;
    Ok(core)
}

pub(super) fn core_persist_receipt_to_api(
    receipt: &CoreMetaCertificatePersistReceipt,
) -> crate::anvil_api::CoreMetaCertificatePersistReceipt {
    crate::anvil_api::CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash.clone(),
        committed_batch_hash: receipt.committed_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signature: receipt.signature.clone(),
    }
}

pub(super) fn normalise_grpc_endpoint(addr: &str) -> Result<String> {
    let trimmed = addr.trim();
    if trimmed.is_empty() {
        bail!("CoreMeta replica endpoint must not be empty");
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Ok(trimmed.to_string())
    } else {
        Ok(format!("http://{trimmed}"))
    }
}

fn internal_request_payload_hash(
    operation: &str,
    request_id: &str,
    source_node_id: &str,
    membership_epoch: u64,
) -> String {
    let mut bytes = Vec::new();
    for part in [
        "anvil.internal.request.v1",
        operation,
        request_id,
        source_node_id,
        &membership_epoch.to_string(),
    ] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    format!("sha256:{}", sha256_hex(&bytes))
}
