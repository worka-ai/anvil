use super::*;
use anyhow::Context;

impl CoreStore {
    pub async fn put_internal_shard(
        &self,
        request: CoreInternalPutShard,
    ) -> Result<CoreInternalShardReceipt> {
        validate_logical_id(&request.block_id, "internal shard block id")?;
        validate_logical_file_id(&request.logical_file_id, "internal shard logical file id")?;
        validate_writer_family(&request.writer_family, "internal shard writer family")?;
        validate_hash(&request.shard_hash, "internal shard hash")?;
        validate_boundary_summary_fields(
            &request.boundary_summary_hash,
            &request.boundary_values_b64,
        )?;
        let actual_hash = format!("sha256:{}", sha256_hex(&request.shard_bytes));
        if actual_hash != request.shard_hash {
            bail!("CoreStore internal shard hash mismatch");
        }
        let profile = local_erasure_profile(&request.erasure_profile_id)?;
        if usize::from(request.shard_index) >= profile.total_shards() {
            bail!("CoreStore internal shard index exceeds erasure profile shard count");
        }
        let placement = self.internal_shard_placement(profile, request.shard_index);
        let shard_path =
            self.shard_path(&placement.node_id, &request.block_id, request.shard_index);
        let shard_file = encode_block_shard_file(
            BlockShardHeaderInput {
                block_id: request.block_id.clone(),
                erasure_set_id: LOCAL_ERASURE_SET_ID.to_string(),
                shard_index: request.shard_index,
                erasure_profile_id: request.erasure_profile_id.clone(),
                logical_file_id: request.logical_file_id.clone(),
                logical_offset: 0,
                logical_length: request.shard_bytes.len() as u64,
                payload_plain_hash: request.shard_hash.clone(),
                payload_stored_hash: request.shard_hash.clone(),
                compression: "none".to_string(),
                encryption: "none".to_string(),
                placement_epoch: request.placement_epoch,
                boundary_summary_hash: request.boundary_summary_hash.clone(),
                boundary_values_b64: request.boundary_values_b64.clone(),
                writer_family: request.writer_family,
                created_by_mutation_id: request.mutation_id,
            },
            &request.shard_bytes,
        )?;
        write_file_atomic(&shard_path, &shard_file).await?;
        let written_at_unix_nanos = unix_timestamp_nanos();
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: &request.block_id,
            shard_index: request.shard_index,
            erasure_profile: &request.erasure_profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: request.placement_epoch,
            shard_length: request.shard_bytes.len() as u64,
            shard_hash: &request.shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: &request.boundary_summary_hash,
        });
        let signature = self.sign_core_receipt(&signed_payload_hash)?;
        Ok(CoreInternalShardReceipt {
            node_id: placement.node_id,
            region_id: placement.region_id,
            cell_id: placement.cell_id,
            block_id: request.block_id,
            shard_index: request.shard_index,
            shard_hash: request.shard_hash,
            shard_length: request.shard_bytes.len() as u64,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            signed_payload_hash,
            signature,
        })
    }

    pub async fn read_internal_shard_range(
        &self,
        request: CoreInternalGetShard,
    ) -> Result<Vec<u8>> {
        validate_logical_id(&request.block_id, "internal shard block id")?;
        validate_hash(&request.shard_hash, "internal shard hash")?;
        let profile = local_erasure_profile(&request.erasure_profile_id)?;
        if usize::from(request.shard_index) >= profile.total_shards() {
            bail!("CoreStore internal shard index exceeds erasure profile shard count");
        }
        let placement = self.internal_shard_placement(profile, request.shard_index);
        let shard_path =
            self.shard_path(&placement.node_id, &request.block_id, request.shard_index);
        let bytes = read_block_shard_file(
            &shard_path,
            BlockShardExpectation {
                block_id: &request.block_id,
                shard_index: request.shard_index,
                erasure_profile_id: &request.erasure_profile_id,
                placement_epoch: request.placement_epoch,
                payload_hash: &request.shard_hash,
                payload_len: 0,
                boundary_summary_hash: None,
                boundary_values_b64: None,
            },
            "internal_get_shard",
        )
        .await?;
        if let Some(range) = request.range {
            let start =
                usize::try_from(range.start).context("internal shard range start exceeds usize")?;
            let end = usize::try_from(range.end_exclusive)
                .context("internal shard range end exceeds usize")?;
            if start > end || end > bytes.len() {
                bail!("CoreStore internal shard requested range is out of bounds");
            }
            Ok(bytes[start..end].to_vec())
        } else {
            Ok(bytes)
        }
    }

    pub async fn get_internal_shard_receipt(
        &self,
        request: CoreInternalGetShard,
    ) -> Result<CoreInternalShardReceipt> {
        let bytes = self.read_internal_shard_range(request.clone()).await?;
        let profile = local_erasure_profile(&request.erasure_profile_id)?;
        let placement = self.internal_shard_placement(profile, request.shard_index);
        let boundary_summary_hash = request.boundary_summary_hash.unwrap_or_default();
        let written_at_unix_nanos = unix_timestamp_nanos();
        let signed_payload_hash = shard_receipt_payload_hash(ShardReceiptPayloadInput {
            block_id: &request.block_id,
            shard_index: request.shard_index,
            erasure_profile: &request.erasure_profile_id,
            node_id: &placement.node_id,
            region_id: &placement.region_id,
            cell_id: &placement.cell_id,
            placement_epoch: request.placement_epoch,
            shard_length: bytes.len() as u64,
            shard_hash: &request.shard_hash,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            boundary_summary_hash: &boundary_summary_hash,
        });
        let signature = self.sign_core_receipt(&signed_payload_hash)?;
        Ok(CoreInternalShardReceipt {
            node_id: placement.node_id,
            region_id: placement.region_id,
            cell_id: placement.cell_id,
            block_id: request.block_id,
            shard_index: request.shard_index,
            shard_hash: request.shard_hash,
            shard_length: bytes.len() as u64,
            fsync_sequence: LOCAL_SHARD_FSYNC_SEQUENCE,
            written_at_unix_nanos,
            signed_payload_hash,
            signature,
        })
    }
    fn internal_shard_placement(
        &self,
        profile: LocalErasureProfile,
        shard_index: u16,
    ) -> LocalShardPlacement {
        if self.node_identity == CoreStoreNodeIdentity::default() {
            LocalShardPlacement {
                node_id: format!("{LOCAL_NODE_ID_PREFIX}-{}", usize::from(shard_index) + 1),
                region_id: "local".to_string(),
                cell_id: local_cell_id_for_shard(profile, usize::from(shard_index)),
                failure_domain: local_cell_id_for_shard(profile, usize::from(shard_index)),
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: String::new(),
                is_local: true,
            }
        } else {
            LocalShardPlacement {
                node_id: self.node_identity.node_id.clone(),
                region_id: self.node_identity.region_id.clone(),
                cell_id: self.node_identity.cell_id.clone(),
                failure_domain: self.node_identity.cell_id.clone(),
                region_weight: 100,
                cell_weight: 100,
                public_api_addr: self.node_identity.public_api_addr.clone(),
                is_local: true,
            }
        }
    }
}
