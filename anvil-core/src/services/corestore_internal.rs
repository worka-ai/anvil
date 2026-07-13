use crate::anvil_api::anti_entropy_internal_server::AntiEntropyInternal;
use crate::anvil_api::block_store_internal_server::BlockStoreInternal;
use crate::anvil_api::core_meta_replication_internal_server::CoreMetaReplicationInternal;
use crate::anvil_api::cross_region_proxy_internal_server::CrossRegionProxyInternal;
use crate::anvil_api::root_register_internal_server::RootRegisterInternal;
use crate::anvil_api::*;
use crate::core_store::{self, CoreInternalGetShard, CoreInternalPutShard, CoreMetaEncodedRow};
use crate::{AppState, auth, diagnostic_store, system_realm, task_lease};
use futures_util::StreamExt;
use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;
use std::time::Instant;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl BlockStoreInternal for AppState {
    type GetShardStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ShardChunk, Status>> + Send>>;

    async fn put_shard(
        &self,
        request: Request<PutShardRequest>,
    ) -> Result<Response<ShardReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let writer_family = if req.writer_family.trim().is_empty() {
            return Err(Status::invalid_argument("writer_family is required"));
        } else {
            req.writer_family
        };
        let mutation_id = if req.mutation_id.trim().is_empty() {
            request_id_from_header(req.header.as_ref())
        } else {
            req.mutation_id
        };
        let receipt = self
            .core_store
            .put_internal_shard(CoreInternalPutShard {
                logical_file_id: req.logical_file_id,
                block_id: req.block_id,
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_bytes: req.shard_bytes,
                shard_hash: req.shard_hash,
                boundary_summary_hash: req.boundary_summary_hash,
                boundary_values_b64: req.boundary_values_b64,
                writer_family,
                mutation_id,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(shard_receipt_from_core(receipt)))
    }

    async fn get_shard(
        &self,
        request: Request<GetShardRequest>,
    ) -> Result<Response<Self::GetShardStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let range = if req.range_end_exclusive > 0 || req.range_start > 0 {
            Some(core_store::CoreByteRange {
                start: req.range_start,
                end_exclusive: req.range_end_exclusive,
            })
        } else {
            None
        };
        let bytes = self
            .core_store
            .read_internal_shard_range(CoreInternalGetShard {
                block_id: req.block_id.clone(),
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_hash: req.shard_hash,
                boundary_summary_hash: if req.boundary_summary_hash.is_empty() {
                    None
                } else {
                    Some(req.boundary_summary_hash)
                },
                range,
            })
            .await
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ShardChunk {
                    block_id: req.block_id,
                    shard_index: req.shard_index,
                    offset: req.range_start,
                    data: bytes,
                    eof: true,
                }))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn get_shard_receipt(
        &self,
        request: Request<GetShardReceiptRequest>,
    ) -> Result<Response<ShardReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let receipt = self
            .core_store
            .get_internal_shard_receipt(CoreInternalGetShard {
                block_id: req.block_id,
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_hash: req.shard_hash,
                boundary_summary_hash: if req.boundary_summary_hash.is_empty() {
                    None
                } else {
                    Some(req.boundary_summary_hash)
                },
                range: None,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(shard_receipt_from_core(receipt)))
    }

    async fn repair_shard(
        &self,
        request: Request<RepairShardRequest>,
    ) -> Result<Response<ShardReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let writer_family = if req.writer_family.trim().is_empty() {
            return Err(Status::invalid_argument("writer_family is required"));
        } else {
            req.writer_family
        };
        let mutation_id = if req.mutation_id.trim().is_empty() {
            request_id_from_header(req.header.as_ref())
        } else {
            req.mutation_id
        };
        let receipt = self
            .core_store
            .put_internal_shard(CoreInternalPutShard {
                logical_file_id: req.logical_file_id,
                block_id: req.block_id,
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_bytes: req.shard_bytes,
                shard_hash: req.shard_hash,
                boundary_summary_hash: req.boundary_summary_hash,
                boundary_values_b64: req.boundary_values_b64,
                writer_family,
                mutation_id,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(shard_receipt_from_core(receipt)))
    }
}

#[tonic::async_trait]
impl CoreMetaReplicationInternal for AppState {
    type CoreMetaStreamStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<CoreMetaStreamResponse, Status>> + Send>>;

    async fn replicate_pending_batches(
        &self,
        request: Request<CoreMetaBatchGroupRequest>,
    ) -> Result<Response<CoreMetaPrepareReceiptGroup>, Status> {
        let total_started_at = Instant::now();
        let auth_started_at = Instant::now();
        ensure_internal_node_request(self, &request).await?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches authorise",
            auth_started_at.elapsed(),
        );
        let validation_started_at = Instant::now();
        let req = request.into_inner();
        if req.batches.is_empty() {
            return Err(Status::invalid_argument(
                "CoreMeta batch group must not be empty",
            ));
        }
        let mut marker_rows = Vec::with_capacity(req.batches.len());
        let mut roots = BTreeSet::new();
        for batch in &req.batches {
            if !roots.insert(batch.root_key_hash.as_str()) {
                return Err(Status::invalid_argument(
                    "CoreMeta batch group contains a duplicate root",
                ));
            }
            if batch.visibility_state != "pending" {
                return Err(Status::invalid_argument(
                    "CoreMeta batch group entries must have pending visibility",
                ));
            }
            let rows = request_rows_checked(&batch.mutations)?;
            let row_hashes = batch
                .mutations
                .iter()
                .map(|row| row.row_hash.clone())
                .collect();
            let expected_pending_hash =
                core_store::pending_batch_hash(&core_store::CoreMetaPendingBatchInput {
                    root_key_hash: batch.root_key_hash.clone(),
                    expected_root_generation: batch.expected_root_generation,
                    post_root_generation: batch.post_root_generation,
                    transaction_id: batch.transaction_id.clone(),
                    row_hashes,
                })
                .map_err(internal_status)?;
            if batch.pending_batch_hash != expected_pending_hash {
                return Err(Status::failed_precondition(
                    "CoreMeta pending batch hash mismatch",
                ));
            }
            marker_rows.push(
                self.core_store
                    .coremeta_pending_batch_marker_encoded_row(
                        &batch.root_key_hash,
                        batch.expected_root_generation,
                        batch.post_root_generation,
                        &batch.transaction_id,
                        &batch.pending_batch_hash,
                        rows.len(),
                    )
                    .map_err(internal_status)?,
            );
        }
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches validate",
            validation_started_at.elapsed(),
        );
        let borrowed = marker_rows
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        let write_started_at = Instant::now();
        self.core_store
            .write_coremeta_encoded_rows(&borrowed)
            .map_err(internal_status)?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches rocksdb_write",
            write_started_at.elapsed(),
        );
        let receipt_started_at = Instant::now();
        let receipts = req
            .batches
            .iter()
            .map(|batch| local_prepare_receipt(&self.core_store, &self.config.node_id, batch, 1))
            .collect::<Result<Vec<_>, _>>()?;
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches sign_receipts",
            receipt_started_at.elapsed(),
        );
        crate::emit_test_timing(
            "coremeta.internal.replicate_pending_batches total",
            total_started_at.elapsed(),
        );
        Ok(Response::new(CoreMetaPrepareReceiptGroup { receipts }))
    }

    async fn persist_commit_certificates(
        &self,
        request: Request<CoreMetaPersistCommitGroupRequest>,
    ) -> Result<Response<CoreMetaCertificatePersistReceiptGroup>, Status> {
        let total_started_at = Instant::now();
        let auth_started_at = Instant::now();
        ensure_internal_node_request(self, &request).await?;
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates authorise",
            auth_started_at.elapsed(),
        );
        let validation_started_at = Instant::now();
        let req = request.into_inner();
        if req.commits.is_empty() {
            return Err(Status::invalid_argument(
                "CoreMeta commit group must not be empty",
            ));
        }
        let mut receipts = Vec::with_capacity(req.commits.len());
        let mut evidence_rows = Vec::with_capacity(req.commits.len());
        let mut roots = BTreeSet::new();
        for commit in &req.commits {
            let _rows = request_rows_checked(&commit.committed_rows)?;
            let cert = commit
                .commit_certificate
                .as_ref()
                .ok_or_else(|| Status::invalid_argument("commit_certificate is required"))?;
            if !roots.insert(cert.root_key_hash.as_str()) {
                return Err(Status::invalid_argument(
                    "CoreMeta commit group contains a duplicate root",
                ));
            }
            let row_hashes = commit
                .committed_rows
                .iter()
                .map(|row| row.row_hash.clone())
                .collect();
            let expected_committed_hash =
                core_store::committed_batch_hash(&core_store::CoreMetaCommittedBatchInput {
                    root_key_hash: cert.root_key_hash.clone(),
                    expected_root_generation: cert.expected_root_generation,
                    post_root_generation: cert.post_root_generation,
                    transaction_id: cert.transaction_id.clone(),
                    pending_batch_hash: cert.pending_batch_hash.clone(),
                    committed_row_hashes: row_hashes,
                })
                .map_err(internal_status)?;
            if commit.committed_batch_hash != expected_committed_hash {
                return Err(Status::failed_precondition(
                    "CoreMeta committed batch hash mismatch",
                ));
            }
            let core_cert = api_commit_certificate_to_core(cert)?;
            core_store::validate_commit_certificate_with_verifier(
                &self
                    .core_store
                    .default_coremeta_quorum_profile()
                    .map_err(internal_status)?,
                &core_cert,
                |node_id, signed_payload_hash, signature| {
                    self.core_store.verify_internal_core_receipt_signature(
                        node_id,
                        signed_payload_hash,
                        signature,
                    )
                },
            )
            .map_err(internal_status)?;
            let receipt = local_persist_receipt(
                &self.core_store,
                &self.config.node_id,
                cert,
                &commit.committed_batch_hash,
                1,
            )?;
            evidence_rows.push(
                self.core_store
                    .coremeta_commit_evidence_encoded_row(
                        &cert.root_key_hash,
                        cert.post_root_generation,
                        &cert.transaction_id,
                        &cert.certificate_hash,
                        &commit.committed_batch_hash,
                        core_store::encode_deterministic_proto(cert),
                        vec![
                            core_store::certificate_persist_receipt_payload_hash(
                                &api_persist_receipt_to_core(&receipt)?,
                            )
                            .map_err(internal_status)?,
                        ],
                        vec![core_store::encode_deterministic_proto(&receipt)],
                    )
                    .map_err(internal_status)?,
            );
            receipts.push(receipt);
        }
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates validate_and_sign",
            validation_started_at.elapsed(),
        );

        let mut rows = Vec::new();
        for commit in &req.commits {
            rows.extend(request_rows_checked(&commit.committed_rows)?);
        }
        rows.extend(evidence_rows.iter().map(|row| CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        }));
        let write_started_at = Instant::now();
        self.core_store
            .write_coremeta_encoded_rows(&rows)
            .map_err(internal_status)?;
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates rocksdb_write",
            write_started_at.elapsed(),
        );
        crate::emit_test_timing(
            "coremeta.internal.persist_commit_certificates total",
            total_started_at.elapsed(),
        );
        Ok(Response::new(CoreMetaCertificatePersistReceiptGroup {
            receipts,
        }))
    }

    async fn core_meta_stream(
        &self,
        request: Request<tonic::Streaming<CoreMetaStreamRequest>>,
    ) -> Result<Response<Self::CoreMetaStreamStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let state = self.clone();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);

        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                let frame = match item {
                    Ok(frame) => frame,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };
                let request_id = frame.request_id.clone();
                if request_id.trim().is_empty() {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "CoreMeta stream request_id is required",
                        )))
                        .await;
                    return;
                }

                let result = match frame.command {
                    Some(core_meta_stream_request::Command::ReplicatePendingBatches(command)) => {
                        let mut request = Request::new(command);
                        request.extensions_mut().insert(claims.clone());
                        CoreMetaReplicationInternal::replicate_pending_batches(&state, request)
                            .await
                            .map(|response| CoreMetaStreamResponse {
                                request_id,
                                result: Some(core_meta_stream_response::Result::PrepareReceipts(
                                    response.into_inner(),
                                )),
                            })
                    }
                    Some(core_meta_stream_request::Command::PersistCommitCertificates(command)) => {
                        let mut request = Request::new(command);
                        request.extensions_mut().insert(claims.clone());
                        CoreMetaReplicationInternal::persist_commit_certificates(&state, request)
                            .await
                            .map(|response| CoreMetaStreamResponse {
                                request_id,
                                result: Some(
                                    core_meta_stream_response::Result::CertificatePersistReceipts(
                                        response.into_inner(),
                                    ),
                                ),
                            })
                    }
                    None => Err(Status::invalid_argument(
                        "CoreMeta stream command is required",
                    )),
                };

                match result {
                    Ok(response) => {
                        if tx.send(Ok(response)).await.is_err() {
                            return;
                        }
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn abort_pending_batch(
        &self,
        request: Request<CoreMetaAbortRequest>,
    ) -> Result<Response<crate::anvil_api::CoreMetaPrepareReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let batch = CoreMetaBatchRequest {
            header: req.header,
            root_key_hash: req.root_key_hash,
            expected_root_generation: req.expected_root_generation,
            post_root_generation: req.post_root_generation,
            transaction_id: req.transaction_id,
            visibility_state: "aborted".to_string(),
            mutations: Vec::new(),
            pending_batch_hash: req.pending_batch_hash,
        };
        let receipt = local_prepare_receipt(&self.core_store, &self.config.node_id, &batch, 1)?;
        Ok(Response::new(receipt))
    }

    async fn read_rows(
        &self,
        request: Request<CoreMetaReadRowsRequest>,
    ) -> Result<Response<CoreMetaReadRowsResponse>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let rows = self
            .core_store
            .read_coremeta_encoded_rows(&req.column_family, &req.core_meta_keys)
            .map_err(internal_status)?;
        let rows = rows
            .into_iter()
            .map(|row| {
                let row_hash = core_store::core_meta_encoded_row_hash(
                    &row.cf,
                    &row.core_meta_key,
                    &row.value_envelope,
                );
                CoreMetaRowMutation {
                    column_family: row.cf,
                    row_hash,
                    core_meta_key: row.core_meta_key,
                    value_envelope: row.value_envelope,
                    delete_marker: row.delete_marker,
                }
            })
            .collect::<Vec<_>>();
        Ok(Response::new(CoreMetaReadRowsResponse {
            rows,
            root_generation: req.root_generation,
            inventory_hash: format!(
                "sha256:{}",
                core_store::sha256_hex(req.root_key_hash.as_bytes())
            ),
        }))
    }

    type CatchUpPartitionStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<CoreMetaBatchFrame, Status>> + Send>>;

    async fn catch_up_partition(
        &self,
        request: Request<CoreMetaCatchUpRequest>,
    ) -> Result<Response<Self::CatchUpPartitionStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let limit = usize::try_from(req.limit.max(1))
            .map_err(|_| Status::invalid_argument("limit exceeds usize"))?;
        let rows = self
            .core_store
            .catch_up_coremeta_rows(&req.root_key_hash, req.after_generation, limit)
            .map_err(internal_status)?;
        let frames = coremeta_rows_to_frames(&req.root_key_hash, rows);
        let (tx, rx) = mpsc::channel(frames.len().max(1));
        tokio::spawn(async move {
            for frame in frames {
                if tx.send(Ok(frame)).await.is_err() {
                    break;
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn exchange_core_meta_inventory(
        &self,
        request: Request<CoreMetaInventoryRequest>,
    ) -> Result<Response<CoreMetaInventory>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let rows = self
            .core_store
            .coremeta_inventory_rows(
                &req.root_key_hash,
                req.from_generation,
                req.to_generation,
                100_000,
            )
            .map_err(internal_status)?;
        let generation_hashes = inventory_generation_hashes(rows);
        let inventory_hash = hash_string_list("anvil.coremeta.inventory.v1", &generation_hashes);
        Ok(Response::new(CoreMetaInventory {
            root_key_hash: req.root_key_hash.clone(),
            from_generation: req.from_generation,
            to_generation: req.to_generation,
            inventory_hash,
            generation_hashes,
        }))
    }
}

trait InternalHeaderCarrier {
    fn internal_header(&self) -> Option<&InternalRequestHeader>;
    fn internal_operation(&self) -> &'static str;
}

macro_rules! impl_internal_header_carrier {
    ($($ty:ty => $operation:literal),+ $(,)?) => {
        $(
            impl InternalHeaderCarrier for $ty {
                fn internal_header(&self) -> Option<&InternalRequestHeader> {
                    self.header.as_ref()
                }

                fn internal_operation(&self) -> &'static str {
                    $operation
                }
            }
        )+
    };
}

impl_internal_header_carrier!(
    PutShardRequest => "block.put_shard",
    GetShardRequest => "block.get_shard",
    GetShardReceiptRequest => "block.get_shard_receipt",
    RepairShardRequest => "block.repair_shard",
    CoreMetaBatchGroupRequest => "coremeta.replicate_pending_batches",
    CoreMetaPersistCommitGroupRequest => "coremeta.persist_commit_certificates",
    CoreMetaAbortRequest => "coremeta.abort_pending_batch",
    CoreMetaReadRowsRequest => "coremeta.read_rows",
    CoreMetaCatchUpRequest => "coremeta.catch_up_partition",
    CoreMetaInventoryRequest => "coremeta.exchange_inventory",
    ReadRootRequest => "root.read",
    PrepareRootRequest => "root.prepare",
    CompareAndSwapRootRequest => "root.compare_and_swap",
    VoteFailoverRequest => "root.vote_failover",
    ExchangeRootInventoryRequest => "root.exchange_inventory",
    ExchangeInventoryRequest => "anti_entropy.exchange_inventory",
    PublishRepairFindingRequest => "anti_entropy.publish_repair_finding",
    ClaimRepairRequest => "anti_entropy.claim_repair",
    ProxyNativeRequest => "proxy.native",
    ProxyObjectReadRequest => "proxy.object_read",
    ProxyShardRangeRequest => "proxy.shard_range",
);

async fn ensure_internal_node_request<T: InternalHeaderCarrier>(
    state: &AppState,
    request: &Request<T>,
) -> Result<(), Status> {
    let total_started_at = Instant::now();
    let claims = request
        .extensions()
        .get::<auth::Claims>()
        .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
    let relation_started_at = Instant::now();
    let allowed = system_realm::check_admin_relation(
        &state.storage,
        &state.config.mesh_id,
        claims,
        system_realm::SystemAdminRelation::ManageNodes,
    )
    .await
    .map_err(internal_status)?;
    crate::emit_test_timing(
        "coremeta.internal.authorise zanzibar_check",
        relation_started_at.elapsed(),
    );
    if !allowed {
        return Err(Status::permission_denied(
            "system realm manage_nodes relation required",
        ));
    }
    let header = request
        .get_ref()
        .internal_header()
        .ok_or_else(|| Status::unauthenticated("internal request header required"))?;
    if header.source_node_id.trim().is_empty() || header.request_id.trim().is_empty() {
        return Err(Status::unauthenticated(
            "internal request source node and request id are required",
        ));
    }
    if header.signature.is_empty() {
        return Err(Status::unauthenticated(
            "internal request header signature is required",
        ));
    }
    if claims.sub != header.source_node_id {
        return Err(Status::permission_denied(
            "internal request source node must match authenticated principal",
        ));
    }
    let signed_payload_hash = internal_request_payload_hash(
        request.get_ref().internal_operation(),
        &header.request_id,
        &header.source_node_id,
        header.membership_epoch,
    );
    state
        .core_store
        .verify_internal_core_receipt_signature(
            &header.source_node_id,
            &signed_payload_hash,
            &header.signature,
        )
        .map_err(internal_status)?;
    crate::emit_test_timing(
        "coremeta.internal.authorise total",
        total_started_at.elapsed(),
    );
    Ok(())
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
    format!("sha256:{}", core_store::sha256_hex(&bytes))
}

fn validate_root_prepare_receipts(
    state: &AppState,
    root_key_hash: &str,
    expected_generation: u64,
    new_root_anchor_record: &[u8],
    receipts: &[RootPrepareReceipt],
) -> Result<(), Status> {
    if receipts.len() < core_store::CORE_META_DEFAULT_QUORUM {
        return Err(Status::failed_precondition(
            "root prepare quorum not reached",
        ));
    }
    let new_root_hash = format!("sha256:{}", core_store::sha256_hex(new_root_anchor_record));
    let post_generation = expected_generation.saturating_add(1);
    let mut replicas = BTreeSet::new();
    for receipt in receipts {
        if receipt.root_key_hash != root_key_hash
            || receipt.expected_generation != expected_generation
            || receipt.post_generation != post_generation
            || receipt.new_root_hash != new_root_hash
        {
            return Err(Status::failed_precondition(
                "root prepare receipt scope mismatch",
            ));
        }
        state
            .core_store
            .verify_internal_core_receipt_signature(
                &receipt.replica_node_id,
                &receipt.new_root_hash,
                &receipt.signature,
            )
            .map_err(internal_status)?;
        replicas.insert(receipt.replica_node_id.as_str());
    }
    if replicas.len() < core_store::CORE_META_DEFAULT_QUORUM {
        return Err(Status::failed_precondition(
            "root prepare quorum not reached",
        ));
    }
    Ok(())
}

fn request_rows_checked(
    mutations: &[CoreMetaRowMutation],
) -> Result<Vec<CoreMetaEncodedRow<'_>>, Status> {
    mutations
        .iter()
        .map(|row| {
            let actual = core_store::core_meta_encoded_row_hash_with_delete(
                &row.column_family,
                &row.core_meta_key,
                &row.value_envelope,
                row.delete_marker,
            );
            if actual != row.row_hash {
                return Err(Status::invalid_argument("CoreMeta row_hash mismatch"));
            }
            Ok(CoreMetaEncodedRow {
                cf: row.column_family.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
        })
        .collect()
}

fn coremeta_rows_to_frames(
    root_key_hash: &str,
    rows: Vec<core_store::CoreMetaEncodedOwnedRow>,
) -> Vec<CoreMetaBatchFrame> {
    let mut by_generation: BTreeMap<u64, Vec<CoreMetaRowMutation>> = BTreeMap::new();
    for row in rows {
        let row_hash = core_store::core_meta_encoded_row_hash_with_delete(
            &row.cf,
            &row.core_meta_key,
            &row.value_envelope,
            row.delete_marker,
        );
        by_generation
            .entry(row.root_generation)
            .or_default()
            .push(CoreMetaRowMutation {
                column_family: row.cf,
                core_meta_key: row.core_meta_key,
                row_hash,
                value_envelope: row.value_envelope,
                delete_marker: row.delete_marker,
            });
    }
    by_generation
        .into_iter()
        .map(|(generation, rows)| CoreMetaBatchFrame {
            root_key_hash: root_key_hash.to_string(),
            root_generation: generation,
            committed_rows: rows,
            commit_certificate: None,
            certificate_persist_receipts: Vec::new(),
        })
        .collect()
}

fn inventory_generation_hashes(rows: Vec<core_store::CoreMetaInventoryRow>) -> Vec<String> {
    let mut by_generation: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    for row in rows {
        by_generation
            .entry(row.root_generation)
            .or_default()
            .push(format!(
                "{}:{}:{}",
                row.cf,
                hex::encode(row.core_meta_key),
                row.row_hash
            ));
    }
    by_generation
        .into_iter()
        .map(|(generation, mut rows)| {
            rows.sort();
            format!(
                "{generation}:{}",
                hash_string_list("anvil.coremeta.inventory.generation.v1", &rows)
            )
        })
        .collect()
}

fn hash_string_list(domain: &str, values: &[String]) -> String {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(domain.as_bytes());
    for value in values {
        bytes.extend_from_slice(&(value.len() as u64).to_le_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }
    format!("sha256:{}", core_store::sha256_hex(&bytes))
}

fn local_prepare_receipt(
    core_store: &core_store::CoreStore,
    replica_node_id: &str,
    request: &CoreMetaBatchRequest,
    write_sequence: u64,
) -> Result<crate::anvil_api::CoreMetaPrepareReceipt, Status> {
    let mut receipt = core_store::CoreMetaPrepareReceipt {
        replica_node_id: replica_node_id.to_string(),
        write_sequence,
        pending_batch_hash: request.pending_batch_hash.clone(),
        root_key_hash: request.root_key_hash.clone(),
        expected_root_generation: request.expected_root_generation,
        post_root_generation: request.post_root_generation,
        transaction_id: request.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash =
        core_store::prepare_receipt_payload_hash(&receipt).map_err(internal_status)?;
    let signature = core_store
        .sign_internal_core_receipt(&receipt.signed_payload_hash)
        .map_err(internal_status)?;
    Ok(crate::anvil_api::CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash,
        root_key_hash: receipt.root_key_hash,
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signature,
    })
}

fn local_persist_receipt(
    core_store: &core_store::CoreStore,
    replica_node_id: &str,
    certificate: &crate::anvil_api::CoreMetaCommitCertificate,
    committed_batch_hash: &str,
    write_sequence: u64,
) -> Result<crate::anvil_api::CoreMetaCertificatePersistReceipt, Status> {
    let mut receipt = core_store::CoreMetaCertificatePersistReceipt {
        replica_node_id: replica_node_id.to_string(),
        write_sequence,
        certificate_hash: certificate.certificate_hash.clone(),
        committed_batch_hash: committed_batch_hash.to_string(),
        root_key_hash: certificate.root_key_hash.clone(),
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: Vec::new(),
    };
    receipt.signed_payload_hash =
        core_store::certificate_persist_receipt_payload_hash(&receipt).map_err(internal_status)?;
    let signature = core_store
        .sign_internal_core_receipt(&receipt.signed_payload_hash)
        .map_err(internal_status)?;
    Ok(crate::anvil_api::CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id,
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash,
        committed_batch_hash: receipt.committed_batch_hash,
        root_key_hash: receipt.root_key_hash,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id,
        signature,
    })
}

fn api_prepare_receipt_to_core(
    receipt: &crate::anvil_api::CoreMetaPrepareReceipt,
) -> Result<core_store::CoreMetaPrepareReceipt, Status> {
    let mut core = core_store::CoreMetaPrepareReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        pending_batch_hash: receipt.pending_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        expected_root_generation: receipt.expected_root_generation,
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: receipt.signature.clone(),
    };
    core.signed_payload_hash =
        core_store::prepare_receipt_payload_hash(&core).map_err(internal_status)?;
    Ok(core)
}

fn api_commit_certificate_to_core(
    certificate: &crate::anvil_api::CoreMetaCommitCertificate,
) -> Result<core_store::CoreMetaCommitCertificate, Status> {
    let prepare_receipts = certificate
        .prepare_receipts
        .iter()
        .map(api_prepare_receipt_to_core)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(core_store::CoreMetaCommitCertificate {
        root_key_hash: certificate.root_key_hash.clone(),
        expected_root_generation: certificate.expected_root_generation,
        post_root_generation: certificate.post_root_generation,
        transaction_id: certificate.transaction_id.clone(),
        pending_batch_hash: certificate.pending_batch_hash.clone(),
        prepare_receipts,
        certificate_hash: certificate.certificate_hash.clone(),
    })
}

fn api_persist_receipt_to_core(
    receipt: &crate::anvil_api::CoreMetaCertificatePersistReceipt,
) -> Result<core_store::CoreMetaCertificatePersistReceipt, Status> {
    let mut core = core_store::CoreMetaCertificatePersistReceipt {
        replica_node_id: receipt.replica_node_id.clone(),
        write_sequence: receipt.write_sequence,
        certificate_hash: receipt.certificate_hash.clone(),
        committed_batch_hash: receipt.committed_batch_hash.clone(),
        root_key_hash: receipt.root_key_hash.clone(),
        post_root_generation: receipt.post_root_generation,
        transaction_id: receipt.transaction_id.clone(),
        signed_payload_hash: String::new(),
        signature: receipt.signature.clone(),
    };
    core.signed_payload_hash =
        core_store::certificate_persist_receipt_payload_hash(&core).map_err(internal_status)?;
    Ok(core)
}

fn request_id_from_header(header: Option<&InternalRequestHeader>) -> String {
    header
        .map(|header| header.request_id.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string())
}

fn shard_receipt_from_core(receipt: core_store::CoreInternalShardReceipt) -> ShardReceipt {
    ShardReceipt {
        node_id: receipt.node_id,
        block_id: receipt.block_id,
        shard_index: u32::from(receipt.shard_index),
        shard_hash: receipt.shard_hash,
        shard_length: receipt.shard_length,
        fsync_sequence: receipt.fsync_sequence,
        written_at_unix_nanos: receipt.written_at_unix_nanos,
        signed_payload_hash: receipt.signed_payload_hash,
        signature: receipt.signature,
    }
}

fn internal_status(error: impl std::fmt::Display) -> Status {
    Status::internal(format!("{error:#}"))
}

#[tonic::async_trait]
impl RootRegisterInternal for AppState {
    async fn read_root(
        &self,
        request: Request<ReadRootRequest>,
    ) -> Result<Response<RootAnchorRead>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let read = self
            .core_store
            .read_internal_root_anchor_by_hash(&req.root_key_hash, req.min_generation)
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RootAnchorRead {
            root_key_hash: read.root_key_hash,
            generation: read.generation,
            root_anchor_record: read.root_anchor_record,
            root_anchor_hash: read.root_anchor_hash,
        }))
    }

    async fn prepare_root(
        &self,
        request: Request<PrepareRootRequest>,
    ) -> Result<Response<RootPrepareReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let post_generation = req.expected_generation.saturating_add(1);
        let new_root_hash = format!(
            "sha256:{}",
            core_store::sha256_hex(&req.new_root_anchor_record)
        );
        let new_anchor = core_store::decode_root_anchor_record(&req.new_root_anchor_record)
            .map_err(internal_status)?;
        if new_anchor.root_key_hash != req.root_key_hash {
            return Err(Status::failed_precondition("root key hash mismatch"));
        }
        if new_anchor.root_generation != post_generation {
            return Err(Status::failed_precondition("root post generation mismatch"));
        }
        match self
            .core_store
            .read_internal_root_anchor_by_hash(&req.root_key_hash, 0)
            .await
        {
            Ok(current) => {
                if current.generation != req.expected_generation {
                    return Err(Status::failed_precondition(format!(
                        "root expected generation mismatch: root_key_hash={} expected={} current={}",
                        req.root_key_hash, req.expected_generation, current.generation
                    )));
                }
                if !req.expected_root_hash.is_empty()
                    && current.root_anchor_hash != req.expected_root_hash
                {
                    return Err(Status::failed_precondition(format!(
                        "root expected hash mismatch: root_key_hash={} generation={} expected={} current={}",
                        req.root_key_hash,
                        req.expected_generation,
                        req.expected_root_hash,
                        current.root_anchor_hash
                    )));
                }
                if new_anchor.previous_root_hash != current.root_anchor_hash {
                    return Err(Status::failed_precondition("root previous hash mismatch"));
                }
            }
            Err(_) => {
                if req.expected_generation != 0 || !req.expected_root_hash.is_empty() {
                    return Err(Status::failed_precondition(
                        "root expected generation missing",
                    ));
                }
            }
        }
        let signature = self
            .core_store
            .sign_internal_core_receipt(&new_root_hash)
            .map_err(internal_status)?;
        Ok(Response::new(RootPrepareReceipt {
            replica_node_id: self.config.node_id.clone(),
            root_key_hash: req.root_key_hash,
            expected_generation: req.expected_generation,
            post_generation,
            new_root_hash,
            signature,
        }))
    }

    async fn compare_and_swap_root(
        &self,
        request: Request<CompareAndSwapRootRequest>,
    ) -> Result<Response<RootAnchorWrite>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let certificate = req
            .core_meta_commit_certificate
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("core_meta_commit_certificate is required"))?;
        if !req.core_meta_commit_certificate_hash.is_empty()
            && req.core_meta_commit_certificate_hash != certificate.certificate_hash
        {
            return Err(Status::failed_precondition(
                "core_meta_commit_certificate_hash mismatch",
            ));
        }
        let core_certificate = api_commit_certificate_to_core(certificate)?;
        let persist_receipts = req
            .certificate_persist_receipts
            .iter()
            .map(api_persist_receipt_to_core)
            .collect::<Result<Vec<_>, _>>()?;
        core_store::validate_commit_evidence_with_verifier(
            &self
                .core_store
                .default_coremeta_quorum_profile()
                .map_err(internal_status)?,
            &core_certificate,
            &persist_receipts,
            |node_id, signed_payload_hash, signature| {
                self.core_store.verify_internal_core_receipt_signature(
                    node_id,
                    signed_payload_hash,
                    signature,
                )
            },
        )
        .map_err(internal_status)?;
        let committed_batch_hash = persist_receipts
            .first()
            .map(|receipt| receipt.committed_batch_hash.clone())
            .ok_or_else(|| Status::failed_precondition("certificate persist receipts required"))?;
        let persist_receipt_hashes = persist_receipts
            .iter()
            .map(core_store::certificate_persist_receipt_payload_hash)
            .collect::<Result<Vec<_>, _>>()
            .map_err(internal_status)?;
        let persist_receipt_bytes = req
            .certificate_persist_receipts
            .iter()
            .map(core_store::encode_deterministic_proto)
            .collect::<Vec<_>>();
        self.core_store
            .persist_coremeta_commit_evidence(
                &certificate.root_key_hash,
                certificate.post_root_generation,
                &certificate.transaction_id,
                &certificate.certificate_hash,
                &committed_batch_hash,
                core_store::encode_deterministic_proto(certificate),
                persist_receipt_hashes,
                persist_receipt_bytes,
            )
            .map_err(internal_status)?;
        validate_root_prepare_receipts(
            self,
            &req.root_key_hash,
            req.expected_generation,
            &req.new_root_anchor_record,
            &req.prepare_receipts,
        )?;
        let read = self
            .core_store
            .compare_and_swap_internal_root_anchor(
                &req.root_key_hash,
                req.expected_generation,
                &req.expected_root_hash,
                &req.new_root_anchor_record,
            )
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RootAnchorWrite {
            root_key_hash: read.root_key_hash,
            generation: read.generation,
            root_anchor_hash: read.root_anchor_hash,
            prepare_receipts: Vec::new(),
        }))
    }

    async fn vote_failover(
        &self,
        request: Request<VoteFailoverRequest>,
    ) -> Result<Response<FailoverVoteReceipt>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let new_owner_fence = req.observed_owner_fence.saturating_add(1);
        let signed = format!(
            "sha256:{}",
            core_store::sha256_hex(
                format!(
                    "{}:{}:{}",
                    req.root_key_hash, req.candidate_owner_node_id, new_owner_fence
                )
                .as_bytes()
            )
        );
        let signature = self
            .core_store
            .sign_internal_core_receipt(&signed)
            .map_err(internal_status)?;
        Ok(Response::new(FailoverVoteReceipt {
            replica_node_id: self.config.node_id.clone(),
            root_key_hash: req.root_key_hash,
            candidate_owner_node_id: req.candidate_owner_node_id,
            new_owner_fence,
            signature,
        }))
    }

    async fn exchange_root_inventory(
        &self,
        request: Request<ExchangeRootInventoryRequest>,
    ) -> Result<Response<RootInventory>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let read = self
            .core_store
            .read_internal_root_anchor_by_hash(&req.root_key_hash, req.from_generation)
            .await
            .map_err(internal_status)?;
        let inventory_parts = if read.generation <= req.to_generation || req.to_generation == 0 {
            vec![format!(
                "{}:{}:{}",
                read.root_key_hash, read.generation, read.root_anchor_hash
            )]
        } else {
            Vec::new()
        };
        Ok(Response::new(RootInventory {
            root_key_hash: req.root_key_hash.clone(),
            from_generation: req.from_generation,
            to_generation: req.to_generation,
            inventory_hash: hash_string_list("anvil.root.inventory.v1", &inventory_parts),
        }))
    }
}

#[tonic::async_trait]
impl AntiEntropyInternal for AppState {
    async fn exchange_inventory(
        &self,
        request: Request<ExchangeInventoryRequest>,
    ) -> Result<Response<InventoryDiff>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let local_hash = match req.namespace.as_str() {
            "coremeta" => {
                let rows = self
                    .core_store
                    .coremeta_inventory_rows(&req.partition, 0, u64::MAX, 100_000)
                    .map_err(internal_status)?;
                hash_string_list(
                    "anvil.antientropy.coremeta.inventory.v1",
                    &inventory_generation_hashes(rows),
                )
            }
            "root" => {
                let read = self
                    .core_store
                    .read_internal_root_anchor_by_hash(&req.partition, 0)
                    .await
                    .map_err(internal_status)?;
                hash_string_list(
                    "anvil.antientropy.root.inventory.v1",
                    &[format!(
                        "{}:{}:{}",
                        read.root_key_hash, read.generation, read.root_anchor_hash
                    )],
                )
            }
            "shard" => format!(
                "sha256:{}",
                core_store::sha256_hex(req.partition.as_bytes())
            ),
            other => {
                return Err(Status::invalid_argument(format!(
                    "unsupported anti-entropy namespace {other}"
                )));
            }
        };

        let divergent_ids = if req.inventory_hash == local_hash {
            Vec::new()
        } else {
            vec![req.partition]
        };
        Ok(Response::new(InventoryDiff {
            missing_ids: Vec::new(),
            divergent_ids,
            inventory_hash: local_hash,
        }))
    }

    async fn publish_repair_finding(
        &self,
        request: Request<PublishRepairFindingRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        let details: serde_json::Value =
            serde_json::from_str(&req.finding_json).map_err(|error| {
                Status::invalid_argument(format!("finding_json must be valid JSON: {error}"))
            })?;
        let finding_id = details
            .get("finding_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let scope_kind = details
            .get("scope_kind")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("anti_entropy")
            .to_string();
        let scope_id = details
            .get("scope_id")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("cluster")
            .to_string();
        let diagnostic = diagnostic_store::write_diagnostic_object(
            &self.storage,
            diagnostic_store::DiagnosticWrite {
                diagnostic_id: finding_id.clone(),
                scope_kind,
                scope_id,
                source: "anvil.internal.anti_entropy".to_string(),
                severity: diagnostic_store::DiagnosticSeverity::Warning,
                code: "RepairFindingPublished".to_string(),
                message: "repair finding published by internal anti-entropy service".to_string(),
                object_ref: None,
                details,
                created_at_nanos: now_unix_nanos(),
            },
            self.config.anvil_secret_encryption_key.as_bytes(),
        )
        .await
        .map_err(internal_status)?;
        Ok(Response::new(WriteResponse {
            request_id: request_id_from_header(req.header.as_ref()),
            mutation_id: finding_id,
            state: WriteState::Committed as i32,
            root_generation: None,
            transaction_manifest_ref: diagnostic.diagnostic_hash,
            idempotency_outcome: "accepted".to_string(),
            retry_after_hint: None,
            finalisation_error: None,
            saga: None,
        }))
    }

    async fn claim_repair(
        &self,
        request: Request<ClaimRepairRequest>,
    ) -> Result<Response<RepairClaim>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        if req.finding_id.trim().is_empty() {
            return Err(Status::invalid_argument("finding_id is required"));
        }
        let owner = task_lease::TaskLeaseOwner {
            tenant_id: 0,
            principal_kind: "anvil-node".to_string(),
            principal_id: self.config.node_id.clone(),
            actor_instance_id: request_id_from_header(req.header.as_ref()),
            display_name: self.config.node_id.clone(),
        };
        let lease = self
            .persistence
            .acquire_named_task_lease(task_lease::TaskLeaseAcquire {
                task_id: format!("repair:{}", req.finding_id),
                task_kind: "repair".to_string(),
                partition_family: "anti_entropy".to_string(),
                partition_id: core_store::sha256_hex(req.finding_id.as_bytes()),
                owner,
                source_cursor: 0,
                now_nanos: now_unix_nanos(),
                ttl_nanos: 60_000_000_000,
            })
            .await
            .map_err(internal_status)?;
        Ok(Response::new(RepairClaim {
            finding_id: req.finding_id,
            claimant_node_id: self.config.node_id.clone(),
            fence_token: lease.fence_token,
        }))
    }
}

#[tonic::async_trait]
impl CrossRegionProxyInternal for AppState {
    type ProxyObjectReadStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ObjectChunk, Status>> + Send>>;
    type ProxyShardRangeStream =
        Pin<Box<dyn futures_core::Stream<Item = Result<ShardChunk, Status>> + Send>>;

    async fn proxy_native(
        &self,
        request: Request<ProxyNativeRequest>,
    ) -> Result<Response<ProxyNativeResponse>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_local_proxy_target(&self.config.region, &req.target_region_id)?;
        match req.method.as_str() {
            "anvil.internal.ping" => Ok(Response::new(ProxyNativeResponse {
                status_code: 200,
                response_body: format!(
                    "{{\"region\":\"{}\",\"node_id\":\"{}\"}}",
                    self.config.region, self.config.node_id
                )
                .into_bytes(),
                error_code: String::new(),
            })),
            "anvil.internal.root.read_by_hash" => {
                let body = core_store::decode_deterministic_proto::<ReadRootRequest>(
                    &req.request_body,
                    "proxied ReadRootRequest",
                )
                .map_err(internal_status)?;
                let read = self
                    .core_store
                    .read_internal_root_anchor_by_hash(&body.root_key_hash, body.min_generation)
                    .await
                    .map_err(internal_status)?;
                Ok(Response::new(ProxyNativeResponse {
                    status_code: 200,
                    response_body: core_store::encode_deterministic_proto(&RootAnchorRead {
                        root_key_hash: read.root_key_hash,
                        generation: read.generation,
                        root_anchor_record: read.root_anchor_record,
                        root_anchor_hash: read.root_anchor_hash,
                    }),
                    error_code: String::new(),
                }))
            }
            "anvil.internal.coremeta.inventory" => {
                let body = core_store::decode_deterministic_proto::<CoreMetaInventoryRequest>(
                    &req.request_body,
                    "proxied CoreMetaInventoryRequest",
                )
                .map_err(internal_status)?;
                let rows = self
                    .core_store
                    .coremeta_inventory_rows(
                        &body.root_key_hash,
                        body.from_generation,
                        body.to_generation,
                        100_000,
                    )
                    .map_err(internal_status)?;
                let generation_hashes = inventory_generation_hashes(rows);
                let inventory_hash =
                    hash_string_list("anvil.coremeta.inventory.v1", &generation_hashes);
                Ok(Response::new(ProxyNativeResponse {
                    status_code: 200,
                    response_body: core_store::encode_deterministic_proto(&CoreMetaInventory {
                        root_key_hash: body.root_key_hash,
                        from_generation: body.from_generation,
                        to_generation: body.to_generation,
                        inventory_hash,
                        generation_hashes,
                    }),
                    error_code: String::new(),
                }))
            }
            other => Err(Status::invalid_argument(format!(
                "native proxy method is not admitted by this Anvil build: {other}"
            ))),
        }
    }

    async fn proxy_object_read(
        &self,
        request: Request<ProxyObjectReadRequest>,
    ) -> Result<Response<Self::ProxyObjectReadStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_local_proxy_target(&self.config.region, &req.target_region_id)?;
        let range = if req.range_start > 0 || req.range_end_exclusive > 0 {
            Some(core_store::CoreByteRange {
                start: req.range_start,
                end_exclusive: req.range_end_exclusive,
            })
        } else {
            None
        };
        let version_id = if req.version_id.trim().is_empty() {
            None
        } else {
            Some(
                uuid::Uuid::parse_str(&req.version_id)
                    .map_err(|_| Status::invalid_argument("version_id must be a UUID"))?,
            )
        };
        let original_claims =
            crate::services::internal_proxy::decode_proxy_authz_context_bytes(&req.authz_context)?;
        if original_claims.tenant_id != req.tenant_id {
            return Err(Status::permission_denied(
                "proxy tenant does not match authz context",
            ));
        }
        if !req.principal_id.is_empty() && original_claims.sub != req.principal_id {
            return Err(Status::permission_denied(
                "proxy principal does not match authz context",
            ));
        }
        let result = self
            .object_manager
            .get_object_with_link_mode_for_tenant(
                Some(original_claims),
                Some(req.tenant_id),
                req.bucket_name.clone(),
                req.object_key.clone(),
                version_id,
                range,
                crate::object_manager::ObjectLinkReadMode::Follow,
                crate::object_manager::ObjectReadConsistency::Latest,
            )
            .await?;
        let mut stream = result.stream;
        let start_offset = result.range_start;
        let (tx, rx) = mpsc::channel(4);
        tokio::spawn(async move {
            let mut offset = start_offset;
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(data) => {
                        let len = data.len() as u64;
                        if tx
                            .send(Ok(ObjectChunk {
                                offset,
                                data,
                                eof: false,
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                        offset = offset.saturating_add(len);
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                }
            }
            let _ = tx
                .send(Ok(ObjectChunk {
                    offset,
                    data: Vec::new(),
                    eof: true,
                }))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn proxy_shard_range(
        &self,
        request: Request<ProxyShardRangeRequest>,
    ) -> Result<Response<Self::ProxyShardRangeStream>, Status> {
        ensure_internal_node_request(self, &request).await?;
        let req = request.into_inner();
        ensure_local_proxy_target(&self.config.region, &req.target_region_id)?;
        let bytes = self
            .core_store
            .read_internal_shard_range(CoreInternalGetShard {
                block_id: req.block_id.clone(),
                shard_index: u16::try_from(req.shard_index)
                    .map_err(|_| Status::invalid_argument("shard_index exceeds u16"))?,
                erasure_profile_id: req.erasure_profile_id,
                placement_epoch: req.placement_epoch,
                shard_hash: req.shard_hash,
                boundary_summary_hash: if req.boundary_summary_hash.is_empty() {
                    None
                } else {
                    Some(req.boundary_summary_hash)
                },
                range: if req.range_start > 0 || req.range_end_exclusive > 0 {
                    Some(core_store::CoreByteRange {
                        start: req.range_start,
                        end_exclusive: req.range_end_exclusive,
                    })
                } else {
                    None
                },
            })
            .await
            .map_err(internal_status)?;
        let (tx, rx) = mpsc::channel(2);
        tokio::spawn(async move {
            let _ = tx
                .send(Ok(ShardChunk {
                    block_id: req.block_id,
                    shard_index: req.shard_index,
                    offset: req.range_start,
                    data: bytes,
                    eof: true,
                }))
                .await;
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

fn ensure_local_proxy_target(local_region: &str, target_region: &str) -> Result<(), Status> {
    if !target_region.is_empty() && target_region != local_region {
        return Err(Status::unavailable(format!(
            "target region {target_region} is not served by this node"
        )));
    }
    Ok(())
}

fn now_unix_nanos() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(i64::MAX)
}
