use super::*;
use crate::anvil_api::{
    GetShardRequest, InternalRequestHeader, PutShardRequest, ShardReceipt,
    block_store_internal_client::BlockStoreInternalClient,
};
use crate::mesh_lifecycle::{self, LifecycleState, NodeCapability};
use futures_util::StreamExt;
use tonic::metadata::MetadataValue;

impl CoreStore {
    pub(super) async fn plan_publish_shard_placements(
        &self,
        profile: LocalErasureProfile,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<Vec<LocalShardPlacement>> {
        let candidates = self.active_shard_candidates(profile).await?;
        let placements = choose_spread_placements(profile, candidates, boundary_values)?;
        validate_local_publish_placements(profile, &placements)?;
        Ok(placements)
    }

    pub(super) async fn write_shard_to_placement(
        &self,
        input: WriteShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        if input.placement.is_local || input.placement.public_api_addr.trim().is_empty() {
            self.write_local_block_shard(input).await
        } else {
            self.write_remote_block_shard(input).await
        }
    }

    pub(super) async fn read_shard_from_placement(
        &self,
        input: ReadShardFromPlacement<'_>,
    ) -> Result<Vec<u8>> {
        if self
            .shard_path(
                &input.placement.node_id,
                input.block_id,
                input.placement.shard_index,
            )
            .exists()
        {
            return self.read_local_block_shard(input).await;
        }
        let endpoint = self
            .placement_endpoint(&input.placement.node_id)
            .await?
            .or_else(|| {
                if input.placement.node_id == self.node_identity.node_id {
                    Some(self.node_identity.public_api_addr.clone())
                } else {
                    None
                }
            });
        let Some(endpoint) = endpoint else {
            return self.read_local_block_shard(input).await;
        };
        self.read_remote_block_shard(input, &endpoint).await
    }

    async fn active_shard_candidates(
        &self,
        profile: LocalErasureProfile,
    ) -> Result<Vec<LocalShardPlacement>> {
        let mut active = Vec::new();
        for node in mesh_lifecycle::list_node_projections_with_core_store(self, None, None)? {
            if node.mesh_id != self.node_identity.mesh_id {
                continue;
            }
            if node.region != self.node_identity.region_id {
                continue;
            }
            if node.state != LifecycleState::Active {
                continue;
            }
            if !node.capabilities.contains(&NodeCapability::Object) {
                continue;
            }
            if node.public_api_addr.trim().is_empty() {
                continue;
            }
            self.register_node_receipt_signing_public_key(
                &node.node_id,
                &node.receipt_signing_public_key_proto,
            )?;
            let placement = LocalShardPlacement {
                is_local: node.node_id == self.node_identity.node_id,
                node_id: node.node_id,
                region_id: node.region,
                cell_id: node.cell_id.clone(),
                failure_domain: node.cell_id,
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: node.public_api_addr,
            };
            active.push(placement);
        }

        let mut out = if active.len() >= profile.total_shards() {
            active
        } else if active.len() <= 1 {
            plan_local_shard_placements(profile)?
        } else {
            bail!(
                "CoreStore placement for {} requires {} active object nodes, got {}",
                profile.id,
                profile.total_shards(),
                active.len()
            );
        };
        out.sort_by(|a, b| {
            b.region_weight
                .cmp(&a.region_weight)
                .then_with(|| b.cell_weight.cmp(&a.cell_weight))
                .then_with(|| a.region_id.cmp(&b.region_id))
                .then_with(|| a.failure_domain.cmp(&b.failure_domain))
                .then_with(|| a.cell_id.cmp(&b.cell_id))
                .then_with(|| compare_node_ids(&a.node_id, &b.node_id))
        });
        Ok(out)
    }

    pub(super) async fn active_placement_cells(
        &self,
    ) -> Result<BTreeMap<(String, String), LocalPlacementCellInfo>> {
        let mut out = BTreeMap::new();
        for node in mesh_lifecycle::list_node_projections_with_core_store(self, None, None)? {
            if node.mesh_id != self.node_identity.mesh_id
                || node.region != self.node_identity.region_id
                || !matches!(node.state, LifecycleState::Active | LifecycleState::Joining)
                || !node.capabilities.contains(&NodeCapability::Object)
            {
                continue;
            }
            out.insert(
                (node.region.clone(), node.cell_id.clone()),
                LocalPlacementCellInfo {
                    failure_domain: node.cell_id,
                    region_weight: 100,
                    cell_weight: 100,
                },
            );
        }
        if !out.is_empty() {
            return Ok(out);
        }
        out.insert(
            (
                self.node_identity.region_id.clone(),
                self.node_identity.cell_id.clone(),
            ),
            LocalPlacementCellInfo {
                failure_domain: self.node_identity.cell_id.clone(),
                region_weight: 1,
                cell_weight: 1,
            },
        );
        Ok(out)
    }

    async fn write_local_block_shard(
        &self,
        input: WriteShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        let shard_file = encode_block_shard_file(
            BlockShardHeaderInput {
                block_id: input.block_id.to_string(),
                erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
                shard_index: input.shard_index,
                erasure_profile_id: input.profile.id.to_string(),
                logical_file_id: input.logical_file_id.to_string(),
                logical_offset: input.logical_offset,
                logical_length: input.shard.len() as u64,
                payload_plain_hash: input.shard_hash.to_string(),
                payload_stored_hash: input.shard_hash.to_string(),
                compression: "none".to_string(),
                encryption: input.encryption_algorithm.to_string(),
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                boundary_summary_hash: input.boundary_summary_hash.to_string(),
                boundary_values_b64: input.boundary_values_b64.to_string(),
                writer_family: input.writer_family.to_string(),
                created_by_mutation_id: input.mutation_id.to_string(),
            },
            input.shard,
        )?;
        let shard_path =
            self.shard_path(&input.placement.node_id, input.block_id, input.shard_index);
        if let Some(parent) = shard_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let dedupe_started_at = Instant::now();
        let dedupe_hit = read_block_shard_file(
            &shard_path,
            BlockShardExpectation {
                block_id: input.block_id,
                shard_index: input.shard_index,
                erasure_profile_id: input.profile.id,
                placement_epoch: LOCAL_PLACEMENT_EPOCH,
                payload_hash: input.shard_hash,
                payload_len: input.shard.len() as u64,
                boundary_summary_hash: None,
                boundary_values_b64: None,
            },
            "dedupe_existing_block_shard",
        )
        .await
        .is_ok();
        record_byte_pipeline_stage_duration(
            if dedupe_hit {
                "dedupe_hit"
            } else {
                "dedupe_miss"
            },
            input.writer_family,
            "preencoded",
            input.encryption_algorithm,
            input.profile.id,
            dedupe_started_at.elapsed(),
        );
        crate::perf::record_dedupe_hit_ratio(
            input.writer_family,
            input.profile.id,
            if dedupe_hit { 1.0 } else { 0.0 },
        );
        if dedupe_hit {
            record_corestore_trace_event("byte_pipeline.dedupe", "hit");
        } else {
            record_corestore_trace_event("byte_pipeline.dedupe", "miss");
            let block_write_started_at = Instant::now();
            write_file_atomic(&shard_path, &shard_file).await?;
            record_block_write_duration(
                &input.placement.node_id,
                &input.placement.region_id,
                &input.placement.cell_id,
                "shard_write",
                "ok",
                block_write_started_at.elapsed(),
            );
            record_corestore_trace_event("block.shard_write", "ok");
            record_corestore_trace_event("block.shard_fsync", "ok");
        }
        let written_at_unix_nanos = unix_timestamp_nanos();
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: input.block_id,
            shard_index: input.shard_index,
            erasure_profile: input.profile.id,
            node_id: &input.placement.node_id,
            region_id: &input.placement.region_id,
            cell_id: &input.placement.cell_id,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            shard_length: input.shard.len() as u64,
            shard_hash: input.shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: input.boundary_summary_hash,
        });
        let receipt_signature = self.sign_core_receipt(&signed_payload_hash)?;
        Ok(CoreObjectPlacement {
            shard_index: input.shard_index,
            node_id: input.placement.node_id.clone(),
            region_id: input.placement.region_id.clone(),
            cell_id: input.placement.cell_id.clone(),
            shard_hash: input.shard_hash.to_string(),
            stored_size: input.shard.len() as u64,
            generation: 1,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            signed_payload_hash,
            signature_algorithm: "ed25519-libp2p".to_string(),
            receipt_signature,
        })
    }

    async fn write_remote_block_shard(
        &self,
        input: WriteShardToPlacement<'_>,
    ) -> Result<CoreObjectPlacement> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreStore remote shard placement selected {}, but no internal bearer token is configured",
                input.placement.node_id
            )
        })?;
        let request_body = PutShardRequest {
            header: Some(self.internal_request_header("block.put_shard")?),
            logical_file_id: input.logical_file_id.to_string(),
            block_id: input.block_id.to_string(),
            shard_index: u32::from(input.shard_index),
            erasure_profile_id: input.profile.id.to_string(),
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            shard_bytes: input.shard.to_vec(),
            shard_hash: input.shard_hash.to_string(),
            boundary_summary_hash: input.boundary_summary_hash.to_string(),
            boundary_values_b64: input.boundary_values_b64.to_string(),
            writer_family: input.writer_family.to_string(),
            mutation_id: input.mutation_id.to_string(),
        };
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        let receipt = self
            .internal_grpc_request(
                &input.placement.public_api_addr,
                "put CoreStore shard",
                move |channel| {
                    let mut client = BlockStoreInternalClient::new(channel);
                    let mut request = tonic::Request::new(request_body.clone());
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    async move {
                        client
                            .put_shard(request)
                            .await
                            .map(tonic::Response::into_inner)
                    }
                },
            )
            .await
            .with_context(|| {
                format!(
                    "put CoreStore shard {}:{} to {}",
                    input.block_id, input.shard_index, input.placement.node_id
                )
            })?;
        self.placement_from_remote_receipt(input, receipt)
    }

    async fn read_local_block_shard(&self, input: ReadShardFromPlacement<'_>) -> Result<Vec<u8>> {
        let shard_path = self.shard_path(
            &input.placement.node_id,
            input.block_id,
            input.placement.shard_index,
        );
        read_block_shard_file(
            &shard_path,
            BlockShardExpectation {
                block_id: input.block_id,
                shard_index: input.placement.shard_index,
                erasure_profile_id: input.profile.id,
                placement_epoch: input.placement.placement_epoch,
                payload_hash: &input.placement.shard_hash,
                payload_len: input.placement.stored_size,
                boundary_summary_hash: None,
                boundary_values_b64: None,
            },
            input.operation,
        )
        .await
    }

    async fn read_remote_block_shard(
        &self,
        input: ReadShardFromPlacement<'_>,
        endpoint: &str,
    ) -> Result<Vec<u8>> {
        let bearer = self.node_identity.internal_bearer_token.as_deref().ok_or_else(|| {
            anyhow!(
                "CoreStore remote shard read selected {}, but no internal bearer token is configured",
                input.placement.node_id
            )
        })?;
        let (range_start, range_end_exclusive) = input
            .range
            .map(|range| (range.start, range.end_exclusive))
            .unwrap_or((0, 0));
        let block_id = input.block_id.to_string();
        let shard_index = u32::from(input.placement.shard_index);
        let erasure_profile_id = input.profile.id.to_string();
        let placement_epoch = input.placement.placement_epoch;
        let shard_hash = input.placement.shard_hash.clone();
        let boundary_summary_hash = input.boundary_summary_hash.to_string();
        let authorization = MetadataValue::try_from(format!("Bearer {bearer}"))
            .context("encode CoreStore internal bearer token")?;
        let bytes = self
            .internal_grpc_request(endpoint, "get CoreStore shard", move |channel| {
                let block_id = block_id.clone();
                let erasure_profile_id = erasure_profile_id.clone();
                let shard_hash = shard_hash.clone();
                let boundary_summary_hash = boundary_summary_hash.clone();
                let authorization = authorization.clone();
                async move {
                    let mut client = BlockStoreInternalClient::new(channel);
                    let mut request = tonic::Request::new(GetShardRequest {
                        header: Some(self.internal_request_header("block.get_shard").map_err(
                            |err| tonic::Status::internal(format!("build internal header: {err}")),
                        )?),
                        block_id: block_id.clone(),
                        shard_index,
                        range_start,
                        range_end_exclusive,
                        erasure_profile_id,
                        placement_epoch,
                        shard_hash,
                        boundary_summary_hash,
                    });
                    request
                        .metadata_mut()
                        .insert("authorization", authorization.clone());
                    let mut stream = client.get_shard(request).await?.into_inner();
                    let mut bytes = Vec::new();
                    while let Some(chunk) = stream.next().await {
                        let chunk = chunk?;
                        if chunk.block_id != block_id || chunk.shard_index != shard_index {
                            return Err(tonic::Status::internal(
                                "CoreStore remote shard chunk scope mismatch",
                            ));
                        }
                        bytes.extend_from_slice(&chunk.data);
                        if chunk.eof {
                            break;
                        }
                    }
                    Ok(bytes)
                }
            })
            .await
            .with_context(|| {
                format!(
                    "read CoreStore shard {}:{} from {}",
                    input.block_id, input.placement.shard_index, input.placement.node_id
                )
            })?;
        if let Some(range) = input.range {
            if bytes.len() as u64 != range.end_exclusive.saturating_sub(range.start) {
                bail!("CoreStore remote shard range length mismatch");
            }
            return Ok(bytes);
        }
        if bytes.len() as u64 != input.placement.stored_size {
            bail!("CoreStore remote shard length mismatch");
        }
        let actual_hash = format!("sha256:{}", sha256_hex(&bytes));
        if actual_hash != input.placement.shard_hash {
            bail!("CoreStore remote shard hash mismatch");
        }
        Ok(bytes)
    }

    async fn placement_endpoint(&self, node_id: &str) -> Result<Option<String>> {
        let nodes = mesh_lifecycle::list_nodes(&self.storage, None, None)
            .await
            .unwrap_or_default();
        if let Some(node) = nodes.into_iter().find(|node| node.node_id == node_id) {
            self.register_node_receipt_signing_public_key(
                &node.node_id,
                &node.receipt_signing_public_key_proto,
            )?;
            Ok(Some(node.public_api_addr))
        } else {
            Ok(None)
        }
    }

    fn placement_from_remote_receipt(
        &self,
        input: WriteShardToPlacement<'_>,
        receipt: ShardReceipt,
    ) -> Result<CoreObjectPlacement> {
        if receipt.node_id != input.placement.node_id {
            bail!(
                "CoreStore remote shard receipt node mismatch: expected {}, got {}",
                input.placement.node_id,
                receipt.node_id
            );
        }
        if receipt.block_id != input.block_id || receipt.shard_index != u32::from(input.shard_index)
        {
            bail!("CoreStore remote shard receipt scope mismatch");
        }
        let stored_size = receipt.shard_length;
        let expected_signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: input.block_id,
            shard_index: input.shard_index,
            erasure_profile: input.profile.id,
            node_id: &input.placement.node_id,
            region_id: &input.placement.region_id,
            cell_id: &input.placement.cell_id,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            shard_length: stored_size,
            shard_hash: &receipt.shard_hash,
            fsync_sequence: receipt.fsync_sequence,
            written_at_unix_nanos: receipt.written_at_unix_nanos,
            boundary_summary_hash: input.boundary_summary_hash,
        });
        validate_shard_receipt_common(
            &input.placement.node_id,
            &input.placement.region_id,
            &input.placement.cell_id,
            &receipt.shard_hash,
            stored_size,
            receipt.fsync_sequence,
            receipt.written_at_unix_nanos,
            &receipt.signed_payload_hash,
            "ed25519-libp2p",
            &receipt.signature,
            &expected_signed_payload_hash,
        )?;
        self.verify_core_receipt_signature(
            &input.placement.node_id,
            &receipt.signed_payload_hash,
            &receipt.signature,
        )?;
        Ok(CoreObjectPlacement {
            shard_index: input.shard_index,
            node_id: input.placement.node_id.clone(),
            region_id: input.placement.region_id.clone(),
            cell_id: input.placement.cell_id.clone(),
            shard_hash: receipt.shard_hash,
            stored_size,
            generation: 1,
            placement_epoch: LOCAL_PLACEMENT_EPOCH,
            fsync_sequence: receipt.fsync_sequence,
            written_at_unix_nanos: receipt.written_at_unix_nanos,
            signed_payload_hash: receipt.signed_payload_hash,
            signature_algorithm: "ed25519-libp2p".to_string(),
            receipt_signature: receipt.signature,
        })
    }

    pub(super) fn internal_request_header(&self, operation: &str) -> Result<InternalRequestHeader> {
        let request_id = uuid::Uuid::new_v4().simple().to_string();
        let signed_payload_hash = internal_request_payload_hash(
            operation,
            &request_id,
            &self.node_identity.node_id,
            LOCAL_PLACEMENT_EPOCH,
        );
        Ok(InternalRequestHeader {
            request_id,
            trace_id: String::new(),
            source_node_id: self.node_identity.node_id.clone(),
            membership_epoch: LOCAL_PLACEMENT_EPOCH,
            source_node_fence: 0,
            signature: self.sign_internal_core_receipt(&signed_payload_hash)?,
        })
    }
}

#[derive(Clone, Copy)]
pub(super) struct WriteShardToPlacement<'a> {
    pub logical_file_id: &'a str,
    pub block_id: &'a str,
    pub shard_index: u16,
    pub shard: &'a [u8],
    pub shard_hash: &'a str,
    pub logical_offset: u64,
    pub profile: LocalErasureProfile,
    pub placement: &'a LocalShardPlacement,
    pub boundary_summary_hash: &'a str,
    pub boundary_values_b64: &'a str,
    pub mutation_id: &'a str,
    pub encryption_algorithm: &'a str,
    pub writer_family: &'a str,
}

#[derive(Clone, Copy)]
pub(super) struct ReadShardFromPlacement<'a> {
    pub block_id: &'a str,
    pub profile: LocalErasureProfile,
    pub placement: &'a CoreObjectPlacement,
    pub boundary_summary_hash: &'a str,
    pub boundary_values_b64: &'a str,
    pub range: Option<CoreByteRange>,
    pub operation: &'static str,
}

pub(super) fn choose_spread_placements(
    profile: LocalErasureProfile,
    candidates: Vec<LocalShardPlacement>,
    boundary_values: &[CoreBoundaryValue],
) -> Result<Vec<LocalShardPlacement>> {
    let total = profile.total_shards();
    if candidates.len() < total {
        bail!(
            "CoreStore placement for {} requires {} active nodes, got {}",
            profile.id,
            total,
            candidates.len()
        );
    }
    let candidates = boundary_rotated_candidates(candidates, boundary_values);
    let mut by_failure_domain = BTreeMap::<String, Vec<LocalShardPlacement>>::new();
    for candidate in candidates {
        by_failure_domain
            .entry(candidate.failure_domain.clone())
            .or_default()
            .push(candidate);
    }
    for nodes in by_failure_domain.values_mut() {
        nodes.sort_by(|a, b| {
            b.region_weight
                .cmp(&a.region_weight)
                .then_with(|| b.cell_weight.cmp(&a.cell_weight))
                .then_with(|| a.region_id.cmp(&b.region_id))
                .then_with(|| a.cell_id.cmp(&b.cell_id))
                .then_with(|| compare_node_ids(&a.node_id, &b.node_id))
        });
    }
    let mut placements = Vec::with_capacity(total);
    while placements.len() < total {
        let mut made_progress = false;
        let failure_domains = by_failure_domain.keys().cloned().collect::<Vec<_>>();
        for failure_domain in failure_domains {
            if placements.len() == total {
                break;
            }
            if let Some(nodes) = by_failure_domain.get_mut(&failure_domain) {
                if !nodes.is_empty() {
                    placements.push(nodes.remove(0));
                    made_progress = true;
                }
            }
        }
        if !made_progress {
            break;
        }
    }
    if placements.len() != total {
        bail!("CoreStore placement planner exhausted candidates");
    }
    Ok(placements)
}

fn boundary_rotated_candidates(
    mut candidates: Vec<LocalShardPlacement>,
    boundary_values: &[CoreBoundaryValue],
) -> Vec<LocalShardPlacement> {
    if candidates.len() < 2 {
        return candidates;
    }
    let seed_parts = boundary_values
        .iter()
        .filter(|value| {
            (!value.placement_affinity.is_empty() && value.placement_affinity != "none")
                || value
                    .categories
                    .iter()
                    .any(|category| category == "placement_affinity")
        })
        .map(|value| {
            format!(
                "{}\0{}\0{}\0{}",
                value.name, value.value_type, value.value, value.placement_affinity
            )
        })
        .collect::<Vec<_>>();
    if seed_parts.is_empty() {
        return candidates;
    }
    candidates.sort_by(|a, b| {
        b.region_weight
            .cmp(&a.region_weight)
            .then_with(|| b.cell_weight.cmp(&a.cell_weight))
            .then_with(|| a.region_id.cmp(&b.region_id))
            .then_with(|| a.failure_domain.cmp(&b.failure_domain))
            .then_with(|| a.cell_id.cmp(&b.cell_id))
            .then_with(|| compare_node_ids(&a.node_id, &b.node_id))
    });
    let seed = sha256_hex(seed_parts.join("\u{1f}").as_bytes());
    let rotation = usize::from_str_radix(&seed[0..8], 16).unwrap_or(0) % candidates.len();
    candidates.rotate_left(rotation);
    candidates
}

fn compare_node_ids(left: &str, right: &str) -> std::cmp::Ordering {
    match (local_node_ordinal(left), local_node_ordinal(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

fn local_node_ordinal(node_id: &str) -> Option<u64> {
    node_id
        .strip_prefix(LOCAL_NODE_ID_PREFIX)?
        .strip_prefix('-')?
        .parse()
        .ok()
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
