use super::*;
use crate::anvil_api::{
    CoreMetaBatchGroupRequest, CoreMetaBatchRequest,
    CoreMetaCommitCertificate as ApiCoreMetaCommitCertificate, CoreMetaPersistCommitGroupRequest,
    CoreMetaPersistCommitRequest, CoreMetaRowMutation, CoreMetaStreamRequest,
    core_meta_stream_request, core_meta_stream_response,
};
use crate::mesh_lifecycle::{self, LifecycleState, NodeCapability};
use futures_util::StreamExt;
use std::collections::VecDeque;

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

#[derive(Debug, Clone)]
struct CoreMetaRootCommitInput {
    root_key_hash: String,
    expected_root_generation: u64,
    post_root_generation: u64,
    transaction_id: String,
    rows: Vec<CoreMetaEncodedOwnedRow>,
}

#[derive(Debug, Clone)]
struct PreparedCoreMetaRootCommit {
    input: CoreMetaRootCommitInput,
    pending_batch_hash: String,
    row_hashes: Vec<String>,
}

#[derive(Debug, Clone)]
struct CertifiedCoreMetaRootCommit {
    prepared: PreparedCoreMetaRootCommit,
    certificate: CoreMetaCommitCertificate,
    committed_batch_hash: String,
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
        let mut replicated_rows =
            BTreeMap::<String, BTreeMap<u64, Vec<CoreMetaEncodedOwnedRow>>>::new();

        for row in self.meta.encode_batch_ops(ops)? {
            if row.root_key_hash.is_empty() {
                local_rows.push(row);
            } else {
                replicated_rows
                    .entry(row.root_key_hash.clone())
                    .or_default()
                    .entry(row.root_generation)
                    .or_default()
                    .push(row);
            }
        }

        if !local_rows.is_empty() {
            let borrowed = borrow_encoded_rows(&local_rows);
            self.write_coremeta_encoded_rows(&borrowed)?;
        }

        let mut generations_by_root = replicated_rows
            .into_iter()
            .map(|(root_key_hash, generations)| {
                (
                    root_key_hash,
                    generations.into_iter().collect::<VecDeque<_>>(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut outcomes = Vec::new();

        loop {
            let mut inputs = Vec::new();
            for (root_key_hash, generations) in &mut generations_by_root {
                let Some((post_root_generation, rows)) = generations.pop_front() else {
                    continue;
                };
                inputs.push(CoreMetaRootCommitInput {
                    root_key_hash: root_key_hash.clone(),
                    expected_root_generation: post_root_generation.saturating_sub(1),
                    post_root_generation,
                    transaction_id: transaction_id.to_string(),
                    rows,
                });
            }
            if inputs.is_empty() {
                break;
            }
            outcomes.extend(self.commit_coremeta_encoded_rows_for_roots(inputs).await?);
        }

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
        let mut outcomes = self
            .commit_coremeta_encoded_rows_for_roots(vec![CoreMetaRootCommitInput {
                root_key_hash: root_key_hash.to_string(),
                expected_root_generation,
                post_root_generation,
                transaction_id: transaction_id.to_string(),
                rows,
            }])
            .await?;
        outcomes
            .pop()
            .ok_or_else(|| anyhow!("CoreMeta quorum batch produced no outcome"))
    }

    async fn commit_coremeta_encoded_rows_for_roots(
        &self,
        inputs: Vec<CoreMetaRootCommitInput>,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }
        let profile = self.default_coremeta_quorum_profile()?;
        profile.validate()?;

        let mut prepared = Vec::with_capacity(inputs.len());
        let mut seen_roots = BTreeSet::new();
        for input in inputs {
            validate_hash(&input.root_key_hash, "CoreMeta quorum root key hash")?;
            validate_logical_id(&input.transaction_id, "CoreMeta quorum transaction id")?;
            if input.rows.is_empty() {
                bail!("CoreMeta quorum batch must contain at least one row");
            }
            if !seen_roots.insert(input.root_key_hash.clone()) {
                bail!("CoreMeta quorum group contains the same root more than once");
            }
            for row in &input.rows {
                if row.root_key_hash != input.root_key_hash {
                    bail!("CoreMeta quorum row root hash does not match batch root");
                }
                if row.root_generation != input.post_root_generation {
                    bail!("CoreMeta quorum row generation does not match batch root generation");
                }
            }
            let row_hashes = encoded_row_hashes(&input.rows);
            let pending_batch_hash = pending_batch_hash(&CoreMetaPendingBatchInput {
                root_key_hash: input.root_key_hash.clone(),
                expected_root_generation: input.expected_root_generation,
                post_root_generation: input.post_root_generation,
                transaction_id: input.transaction_id.clone(),
                row_hashes: row_hashes.clone(),
            })?;
            prepared.push(PreparedCoreMetaRootCommit {
                input,
                pending_batch_hash,
                row_hashes,
            });
        }

        let select_started_at = Instant::now();
        let replicas = self.select_coremeta_replicas(&profile).await?;
        crate::emit_test_timing(
            "coremeta.commit_group select_coremeta_replicas",
            select_started_at.elapsed(),
        );

        let prepare_started_at = Instant::now();
        crate::perf::record_counter(
            "anvil_coremeta_commit_group_roots",
            &[("stage", "prepare")],
            prepared.len() as u64,
        );
        let mut prepare_results = replicas
            .iter()
            .map(|replica| async {
                let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                    self.replicate_coremeta_batches_locally(replica, &prepared)
                } else {
                    self.replicate_coremeta_batches_remotely(replica, &prepared)
                        .await
                };
                (replica.node_id.clone(), result)
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut prepare_receipts = BTreeMap::<String, Vec<CoreMetaPrepareReceipt>>::new();
        let mut prepare_errors = Vec::new();
        while let Some((node_id, result)) = prepare_results.next().await {
            match result.and_then(|receipts| {
                self.validate_prepare_receipt_group(&node_id, &prepared, &receipts)?;
                Ok(receipts)
            }) {
                Ok(receipts) => {
                    for receipt in receipts {
                        prepare_receipts
                            .entry(receipt.root_key_hash.clone())
                            .or_default()
                            .push(receipt);
                    }
                }
                Err(error) => prepare_errors.push(format!("{node_id}: {error}")),
            }
            if prepared.iter().all(|root| {
                prepare_receipts
                    .get(&root.input.root_key_hash)
                    .is_some_and(|receipts| receipts.len() >= profile.prepare_quorum)
            }) {
                break;
            }
        }
        drop(prepare_results);
        for root in &prepared {
            let count = prepare_receipts
                .get(&root.input.root_key_hash)
                .map_or(0, Vec::len);
            if count < profile.prepare_quorum {
                record_corestore_trace_event("coremeta.replicate_pending", "quorum_failed");
                record_corestore_trace_event("coremeta.quorum_wait", "quorum_failed");
                crate::perf::record_duration(
                    "anvil_coremeta_replication_duration_ms",
                    &[
                        ("operation", "replicate_pending_group"),
                        ("root_kind", "root_group"),
                        ("profile", profile.profile_id.as_str()),
                        ("status", "quorum_failed"),
                    ],
                    prepare_started_at.elapsed(),
                );
                bail!(
                    "CoreMeta prepare quorum was not reached for {}: {}",
                    root.input.root_key_hash,
                    prepare_errors.join("; ")
                );
            }
        }
        crate::emit_test_timing(
            "coremeta.commit_group replicate_pending_quorum",
            prepare_started_at.elapsed(),
        );
        crate::perf::record_duration(
            "anvil_coremeta_replication_duration_ms",
            &[
                ("operation", "replicate_pending_group"),
                ("root_kind", "root_group"),
                ("profile", profile.profile_id.as_str()),
                ("status", "ok"),
            ],
            prepare_started_at.elapsed(),
        );
        crate::perf::record_counter(
            "anvil_coremeta_quorum_total",
            &[
                ("profile", profile.profile_id.as_str()),
                ("outcome", "prepare_ok"),
            ],
            prepared.len() as u64,
        );

        let mut certified = Vec::with_capacity(prepared.len());
        for root in prepared {
            let root_receipts = prepare_receipts
                .remove(&root.input.root_key_hash)
                .unwrap_or_default();
            let certificate = build_commit_certificate(
                &profile,
                root.input.root_key_hash.clone(),
                root.input.expected_root_generation,
                root.input.post_root_generation,
                root.input.transaction_id.clone(),
                root.pending_batch_hash.clone(),
                root_receipts,
            )?;
            let committed_batch_hash = committed_batch_hash(&CoreMetaCommittedBatchInput {
                root_key_hash: root.input.root_key_hash.clone(),
                expected_root_generation: root.input.expected_root_generation,
                post_root_generation: root.input.post_root_generation,
                transaction_id: root.input.transaction_id.clone(),
                pending_batch_hash: root.pending_batch_hash.clone(),
                committed_row_hashes: root.row_hashes.clone(),
            })?;
            certified.push(CertifiedCoreMetaRootCommit {
                prepared: root,
                certificate,
                committed_batch_hash,
            });
        }

        let persist_started_at = Instant::now();
        crate::perf::record_counter(
            "anvil_coremeta_commit_group_roots",
            &[("stage", "persist")],
            certified.len() as u64,
        );
        let mut persist_results = replicas
            .iter()
            .map(|replica| async {
                let result = if replica.is_local || replica.public_api_addr.trim().is_empty() {
                    self.persist_coremeta_certificates_locally(replica, &certified)
                } else {
                    self.persist_coremeta_certificates_remotely(replica, &certified)
                        .await
                };
                (replica.node_id.clone(), result)
            })
            .collect::<futures_util::stream::FuturesUnordered<_>>();
        let mut persist_receipts =
            BTreeMap::<String, Vec<CoreMetaCertificatePersistReceipt>>::new();
        let mut persist_errors = Vec::new();
        while let Some((node_id, result)) = persist_results.next().await {
            match result.and_then(|receipts| {
                self.validate_persist_receipt_group(&node_id, &certified, &receipts)?;
                Ok(receipts)
            }) {
                Ok(receipts) => {
                    for receipt in receipts {
                        persist_receipts
                            .entry(receipt.root_key_hash.clone())
                            .or_default()
                            .push(receipt);
                    }
                }
                Err(error) => persist_errors.push(format!("{node_id}: {error}")),
            }
            if certified.iter().all(|root| {
                persist_receipts
                    .get(&root.prepared.input.root_key_hash)
                    .is_some_and(|receipts| receipts.len() >= profile.certificate_persist_quorum)
            }) {
                break;
            }
        }
        drop(persist_results);
        for root in &certified {
            let count = persist_receipts
                .get(&root.prepared.input.root_key_hash)
                .map_or(0, Vec::len);
            if count < profile.certificate_persist_quorum {
                record_corestore_trace_event(
                    "coremeta.persist_commit_certificate",
                    "quorum_failed",
                );
                record_corestore_trace_event("coremeta.quorum_wait", "quorum_failed");
                crate::perf::record_duration(
                    "anvil_coremeta_replication_duration_ms",
                    &[
                        ("operation", "persist_commit_group"),
                        ("root_kind", "root_group"),
                        ("profile", profile.profile_id.as_str()),
                        ("status", "quorum_failed"),
                    ],
                    persist_started_at.elapsed(),
                );
                bail!(
                    "CoreMeta certificate persistence quorum was not reached for {}: {}",
                    root.prepared.input.root_key_hash,
                    persist_errors.join("; ")
                );
            }
        }
        crate::emit_test_timing(
            "coremeta.commit_group persist_commit_certificate_quorum",
            persist_started_at.elapsed(),
        );
        crate::perf::record_duration(
            "anvil_coremeta_replication_duration_ms",
            &[
                ("operation", "persist_commit_group"),
                ("root_kind", "root_group"),
                ("profile", profile.profile_id.as_str()),
                ("status", "ok"),
            ],
            persist_started_at.elapsed(),
        );
        crate::perf::record_counter(
            "anvil_coremeta_quorum_total",
            &[
                ("profile", profile.profile_id.as_str()),
                ("outcome", "certificate_persist_ok"),
            ],
            certified.len() as u64,
        );

        let evidence_started_at = Instant::now();
        let mut evidence_rows = Vec::with_capacity(certified.len());
        let mut outcomes = Vec::with_capacity(certified.len());
        for root in certified {
            let receipts = persist_receipts
                .remove(&root.prepared.input.root_key_hash)
                .unwrap_or_default();
            validate_commit_evidence_with_verifier(
                &profile,
                &root.certificate,
                &receipts,
                |node_id, signed_payload_hash, signature| {
                    self.verify_internal_core_receipt_signature(
                        node_id,
                        signed_payload_hash,
                        signature,
                    )
                },
            )?;
            let certificate_bytes =
                encode_deterministic_proto(&core_commit_certificate_to_api(&root.certificate));
            let persist_receipt_bytes = receipts
                .iter()
                .map(|receipt| encode_deterministic_proto(&core_persist_receipt_to_api(receipt)))
                .collect::<Vec<_>>();
            let mut persist_receipt_hashes = receipts
                .iter()
                .map(certificate_persist_receipt_payload_hash)
                .collect::<Result<Vec<_>>>()?;
            persist_receipt_hashes.sort();
            persist_receipt_hashes.dedup();
            evidence_rows.push(self.coremeta_commit_evidence_encoded_row(
                &root.prepared.input.root_key_hash,
                root.prepared.input.post_root_generation,
                &root.prepared.input.transaction_id,
                &root.certificate.certificate_hash,
                &root.committed_batch_hash,
                certificate_bytes.clone(),
                persist_receipt_hashes.clone(),
                persist_receipt_bytes,
            )?);
            outcomes.push(CoreMetaQuorumCommitOutcome {
                root_key_hash: root.prepared.input.root_key_hash,
                post_root_generation: root.prepared.input.post_root_generation,
                certificate_hash: root.certificate.certificate_hash,
                certificate_bytes,
                certificate_persist_receipt_hashes: persist_receipt_hashes,
                certificate_persist_receipts: receipts,
                metadata_replica_node_ids: replicas
                    .iter()
                    .take(profile.replica_count)
                    .map(|replica| replica.node_id.clone())
                    .collect(),
            });
        }
        let borrowed = borrow_encoded_rows(&evidence_rows);
        self.write_coremeta_encoded_rows(&borrowed)?;
        crate::emit_test_timing(
            "coremeta.commit_group persist_commit_evidence",
            evidence_started_at.elapsed(),
        );
        Ok(outcomes)
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

    fn replicate_coremeta_batches_locally(
        &self,
        replica: &LocalShardPlacement,
        roots: &[PreparedCoreMetaRootCommit],
    ) -> Result<Vec<CoreMetaPrepareReceipt>> {
        let marker_rows = roots
            .iter()
            .map(|root| {
                self.coremeta_pending_batch_marker_encoded_row(
                    &root.input.root_key_hash,
                    root.input.expected_root_generation,
                    root.input.post_root_generation,
                    &root.input.transaction_id,
                    &root.pending_batch_hash,
                    root.input.rows.len(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        self.write_coremeta_encoded_rows(&borrow_encoded_rows(&marker_rows))?;
        roots
            .iter()
            .map(|root| {
                let mut receipt = CoreMetaPrepareReceipt {
                    replica_node_id: replica.node_id.clone(),
                    write_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                    pending_batch_hash: root.pending_batch_hash.clone(),
                    root_key_hash: root.input.root_key_hash.clone(),
                    expected_root_generation: root.input.expected_root_generation,
                    post_root_generation: root.input.post_root_generation,
                    transaction_id: root.input.transaction_id.clone(),
                    signed_payload_hash: String::new(),
                    signature: Vec::new(),
                };
                receipt.signed_payload_hash = prepare_receipt_payload_hash(&receipt)?;
                receipt.signature =
                    self.sign_internal_core_receipt(&receipt.signed_payload_hash)?;
                Ok(receipt)
            })
            .collect()
    }

    async fn replicate_coremeta_batches_remotely(
        &self,
        replica: &LocalShardPlacement,
        roots: &[PreparedCoreMetaRootCommit],
    ) -> Result<Vec<CoreMetaPrepareReceipt>> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreMeta remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let request_body = CoreMetaBatchGroupRequest {
            header: Some(self.internal_request_header("coremeta.replicate_pending_batches")?),
            batches: roots
                .iter()
                .map(|root| CoreMetaBatchRequest {
                    header: None,
                    root_key_hash: root.input.root_key_hash.clone(),
                    expected_root_generation: root.input.expected_root_generation,
                    post_root_generation: root.input.post_root_generation,
                    transaction_id: root.input.transaction_id.clone(),
                    visibility_state: "pending".to_string(),
                    mutations: rows_to_api_mutations(&root.input.rows),
                    pending_batch_hash: root.pending_batch_hash.clone(),
                })
                .collect(),
        };
        let response = self
            .coremeta_stream_request(
                &replica.public_api_addr,
                bearer,
                "replicate CoreMeta batches",
                CoreMetaStreamRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    command: Some(core_meta_stream_request::Command::ReplicatePendingBatches(
                        request_body,
                    )),
                },
            )
            .await
            .with_context(|| format!("replicate CoreMeta batches to {}", replica.node_id))?;
        match response.result {
            Some(core_meta_stream_response::Result::PrepareReceipts(group)) => group
                .receipts
                .into_iter()
                .map(api_prepare_receipt_to_core)
                .collect(),
            _ => bail!("CoreMeta stream returned unexpected response for prepare request"),
        }
    }

    fn validate_prepare_receipt_group(
        &self,
        replica_node_id: &str,
        roots: &[PreparedCoreMetaRootCommit],
        receipts: &[CoreMetaPrepareReceipt],
    ) -> Result<()> {
        if receipts.len() != roots.len() {
            bail!("CoreMeta prepare receipt group cardinality mismatch");
        }
        let expected = roots
            .iter()
            .map(|root| (root.input.root_key_hash.as_str(), root))
            .collect::<BTreeMap<_, _>>();
        let mut seen = BTreeSet::new();
        for receipt in receipts {
            let root = expected
                .get(receipt.root_key_hash.as_str())
                .ok_or_else(|| anyhow!("CoreMeta prepare receipt references unknown root"))?;
            if !seen.insert(receipt.root_key_hash.as_str())
                || receipt.replica_node_id != replica_node_id
                || receipt.pending_batch_hash != root.pending_batch_hash
                || receipt.expected_root_generation != root.input.expected_root_generation
                || receipt.post_root_generation != root.input.post_root_generation
                || receipt.transaction_id != root.input.transaction_id
            {
                bail!("CoreMeta prepare receipt does not match its root batch");
            }
            self.verify_internal_core_receipt_signature(
                &receipt.replica_node_id,
                &receipt.signed_payload_hash,
                &receipt.signature,
            )?;
        }
        Ok(())
    }

    fn persist_coremeta_certificates_locally(
        &self,
        replica: &LocalShardPlacement,
        roots: &[CertifiedCoreMetaRootCommit],
    ) -> Result<Vec<CoreMetaCertificatePersistReceipt>> {
        let mut receipts = Vec::with_capacity(roots.len());
        let mut rows = Vec::new();
        for root in roots {
            let mut receipt = CoreMetaCertificatePersistReceipt {
                replica_node_id: replica.node_id.clone(),
                write_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
                certificate_hash: root.certificate.certificate_hash.clone(),
                committed_batch_hash: root.committed_batch_hash.clone(),
                root_key_hash: root.prepared.input.root_key_hash.clone(),
                post_root_generation: root.prepared.input.post_root_generation,
                transaction_id: root.prepared.input.transaction_id.clone(),
                signed_payload_hash: String::new(),
                signature: Vec::new(),
            };
            receipt.signed_payload_hash = certificate_persist_receipt_payload_hash(&receipt)?;
            receipt.signature = self.sign_internal_core_receipt(&receipt.signed_payload_hash)?;
            rows.extend(root.prepared.input.rows.clone());
            rows.push(self.coremeta_commit_evidence_encoded_row(
                &root.prepared.input.root_key_hash,
                root.prepared.input.post_root_generation,
                &root.prepared.input.transaction_id,
                &root.certificate.certificate_hash,
                &root.committed_batch_hash,
                encode_deterministic_proto(&core_commit_certificate_to_api(&root.certificate)),
                vec![certificate_persist_receipt_payload_hash(&receipt)?],
                vec![encode_deterministic_proto(&core_persist_receipt_to_api(
                    &receipt,
                ))],
            )?);
            receipts.push(receipt);
        }
        self.write_coremeta_encoded_rows(&borrow_encoded_rows(&rows))?;
        Ok(receipts)
    }

    async fn persist_coremeta_certificates_remotely(
        &self,
        replica: &LocalShardPlacement,
        roots: &[CertifiedCoreMetaRootCommit],
    ) -> Result<Vec<CoreMetaCertificatePersistReceipt>> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreMeta remote replica {} selected, but no internal bearer token is configured",
                replica.node_id
            )
        })?;
        let request_body = CoreMetaPersistCommitGroupRequest {
            header: Some(self.internal_request_header("coremeta.persist_commit_certificates")?),
            commits: roots
                .iter()
                .map(|root| CoreMetaPersistCommitRequest {
                    header: None,
                    commit_certificate: Some(core_commit_certificate_to_api(&root.certificate)),
                    committed_rows: rows_to_api_mutations(&root.prepared.input.rows),
                    committed_batch_hash: root.committed_batch_hash.clone(),
                })
                .collect(),
        };
        let response = self
            .coremeta_stream_request(
                &replica.public_api_addr,
                bearer,
                "persist CoreMeta certificates",
                CoreMetaStreamRequest {
                    request_id: uuid::Uuid::new_v4().to_string(),
                    command: Some(
                        core_meta_stream_request::Command::PersistCommitCertificates(request_body),
                    ),
                },
            )
            .await
            .with_context(|| format!("persist CoreMeta certificates on {}", replica.node_id))?;
        match response.result {
            Some(core_meta_stream_response::Result::CertificatePersistReceipts(group)) => group
                .receipts
                .into_iter()
                .map(api_persist_receipt_to_core)
                .collect(),
            _ => bail!("CoreMeta stream returned unexpected response for certificate request"),
        }
    }

    fn validate_persist_receipt_group(
        &self,
        replica_node_id: &str,
        roots: &[CertifiedCoreMetaRootCommit],
        receipts: &[CoreMetaCertificatePersistReceipt],
    ) -> Result<()> {
        if receipts.len() != roots.len() {
            bail!("CoreMeta persist receipt group cardinality mismatch");
        }
        let expected = roots
            .iter()
            .map(|root| (root.prepared.input.root_key_hash.as_str(), root))
            .collect::<BTreeMap<_, _>>();
        let mut seen = BTreeSet::new();
        for receipt in receipts {
            let root = expected
                .get(receipt.root_key_hash.as_str())
                .ok_or_else(|| anyhow!("CoreMeta persist receipt references unknown root"))?;
            if !seen.insert(receipt.root_key_hash.as_str())
                || receipt.replica_node_id != replica_node_id
                || receipt.certificate_hash != root.certificate.certificate_hash
                || receipt.committed_batch_hash != root.committed_batch_hash
                || receipt.post_root_generation != root.prepared.input.post_root_generation
                || receipt.transaction_id != root.prepared.input.transaction_id
            {
                bail!("CoreMeta persist receipt does not match its root certificate");
            }
            self.verify_internal_core_receipt_signature(
                &receipt.replica_node_id,
                &receipt.signed_payload_hash,
                &receipt.signature,
            )?;
        }
        Ok(())
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
