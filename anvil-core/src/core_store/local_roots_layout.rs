use super::local_stream_control::control_record_proto::*;
use super::local_tx_rows::{OwnedCoreMetaBatchOp, borrow_owned_coremeta_batch_ops};
use super::*;
use crate::formats::writer::{WriterFamily, canonical_logical_file_id};

impl CoreStore {
    pub(super) async fn ensure_layout(&self) -> Result<()> {
        for path in [
            self.storage.core_store_root_path(),
            self.storage.core_store_staging_path(),
            self.admission_landed_bytes_root(),
        ] {
            let started_at = Instant::now();
            fs::create_dir_all(&path).await?;
            crate::perf::record_io_duration(
                "core_store",
                "ensure_layout_create_dir_all",
                &path,
                0,
                started_at.elapsed(),
            );
        }
        Ok(())
    }

    pub async fn read_object_manifest(
        &self,
        object_ref: &CoreObjectRef,
    ) -> Result<CoreObjectManifest> {
        let single_block_ref = if let Some(logical_manifest) = self
            .logical_file_manifest_from_object_ref(object_ref)
            .await?
        {
            Some(logical_file_manifest_single_block_object_ref(
                &logical_manifest,
            )?)
        } else {
            None
        };
        let object_ref = single_block_ref.as_ref().unwrap_or(object_ref);
        let manifest_hash = decode_manifest_ref(&object_ref.manifest_ref)?;
        let object_hash = strip_sha256_prefix(&object_ref.hash)?;
        if object_hash != manifest_hash {
            bail!("CoreStore object manifest ref/hash mismatch");
        }
        let bytes = self
            .meta
            .get(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &object_manifest_meta_key(object_ref),
            )?
            .ok_or_else(|| {
                anyhow!(
                    "CoreStore object manifest metadata row is missing for {}",
                    object_ref.manifest_ref
                )
            })?;
        let manifest = decode_object_manifest_record(&bytes)?;
        if manifest.schema != CORE_OBJECT_MANIFEST_SCHEMA {
            bail!("CoreStore object manifest metadata row has invalid schema");
        }
        validate_manifest_for_object_ref(&manifest, object_ref, object_hash)?;
        if is_inline_object_ref(object_ref) {
            return Ok(manifest);
        }
        self.manifest_with_present_shard_placements(manifest)
    }

    pub(super) fn manifest_with_present_shard_placements(
        &self,
        mut manifest: CoreObjectManifest,
    ) -> Result<CoreObjectManifest> {
        manifest.placements.retain(|placement| {
            if !is_local_shard_node_id(&placement.node_id) {
                return true;
            }
            self.shard_path(
                &placement.node_id,
                &manifest.encoding.block_id,
                placement.shard_index,
            )
            .exists()
        });
        if manifest.placements.len() < usize::from(manifest.encoding.minimum_read_shards) {
            bail!(
                "CoreStore manifest {} has only {} recorded shard placements, below minimum read quorum {}",
                manifest.object_hash,
                manifest.placements.len(),
                manifest.encoding.minimum_read_shards
            );
        }
        Ok(manifest)
    }

    pub(super) async fn read_object_manifest_for_range(
        &self,
        object_ref: &CoreObjectRef,
        range: &CoreByteRange,
    ) -> Result<CoreObjectManifest> {
        if self
            .logical_file_manifest_from_object_ref(object_ref)
            .await?
            .is_some()
        {
            return self.read_object_manifest(object_ref).await;
        }
        let manifest_hash = decode_manifest_ref(&object_ref.manifest_ref)?;
        let object_hash = strip_sha256_prefix(&object_ref.hash)?;
        if object_hash != manifest_hash {
            bail!("CoreStore object manifest ref/hash mismatch");
        }
        if is_inline_object_ref(object_ref) {
            return self.read_object_manifest(object_ref).await;
        }
        let profile =
            local_erasure_profile(decode_manifest_ref_profile(&object_ref.manifest_ref)?)?;
        let required_indices = required_data_shard_indices_for_range(
            object_ref.logical_size,
            profile.data_shards,
            range,
        )?;
        let manifest = self.read_object_manifest(object_ref).await?;
        let present = manifest
            .placements
            .iter()
            .map(|placement| placement.shard_index)
            .collect::<BTreeSet<_>>();
        let missing = required_indices
            .difference(&present)
            .copied()
            .collect::<Vec<_>>();
        if !missing.is_empty() {
            bail!(
                "CoreStore manifest {} is missing required range shards {:?}",
                object_ref.manifest_ref,
                missing
            );
        }
        Ok(manifest)
    }

    pub(super) async fn verify_embedded_manifest_readable(
        &self,
        manifest: &CoreObjectManifest,
    ) -> Result<()> {
        if manifest.schema != CORE_OBJECT_MANIFEST_SCHEMA {
            bail!("CoreStore embedded root segment manifest has invalid schema");
        }
        let object_ref = object_ref_from_object_manifest(manifest)?;
        let bytes = self
            .get_blob(GetBlob { object_ref })
            .await
            .with_context(|| "read embedded root segment manifest payload")?;
        if bytes.len() as u64 != manifest.logical_size {
            bail!("CoreStore embedded root segment logical size mismatch");
        }
        Ok(())
    }

    pub(super) async fn read_all_stream_records(
        &self,
        stream_id: &str,
    ) -> Result<Vec<StreamRecord>> {
        self.read_stream_records_from_meta(stream_id).await
    }

    pub(super) async fn read_direct_stream_records(
        &self,
        stream_id: &str,
    ) -> Result<Vec<StreamRecord>> {
        if stream_id != CORE_TRANSACTION_STREAM_ID {
            bail!(
                "CoreStore direct stream reads are reserved for the root-anchored transaction stream"
            );
        }
        self.read_core_transaction_stream_records_from_root().await
    }

    pub(super) async fn read_stream_records_after(
        &self,
        stream_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        if stream_id != CORE_TRANSACTION_STREAM_ID {
            let records = self.read_all_stream_records(stream_id).await?;
            let filtered = records
                .into_iter()
                .filter(|record| record.sequence > after_sequence)
                .collect::<Vec<_>>();
            if limit > 0 {
                return Ok(filtered.into_iter().take(limit).collect());
            }
            return Ok(filtered);
        }
        self.read_core_transaction_stream_records_after_from_root(after_sequence, limit)
            .await
    }

    pub(super) async fn write_stream_records(
        &self,
        stream_id: &str,
        records: &[StreamRecord],
    ) -> Result<()> {
        let metadata_commits = self.write_stream_metadata_rows(stream_id, records).await?;
        if stream_id == CORE_TRANSACTION_STREAM_ID {
            self.write_core_transaction_stream_records(records, &metadata_commits)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn write_stream_metadata_rows(
        &self,
        stream_id: &str,
        records: &[StreamRecord],
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        let prepared = self
            .prepare_stream_metadata_rows(stream_id, records)
            .await?;
        if prepared.owned_ops.is_empty() {
            return Ok(Vec::new());
        }
        let ops = borrow_owned_coremeta_batch_ops(&prepared.owned_ops);
        self.commit_coremeta_batch_by_embedded_roots(&prepared.transaction_id, &ops)
            .await
    }

    pub(super) async fn prepare_stream_metadata_rows(
        &self,
        stream_id: &str,
        records: &[StreamRecord],
    ) -> Result<PreparedStreamMetadataWrite> {
        for record in records {
            if record.stream_id != stream_id {
                bail!("CoreStore stream record metadata row has invalid scope");
            }
        }

        let existing_head = self.read_stream_head_from_meta(stream_id)?;
        let (existing_sequence, existing_hash, existing_record_count) = existing_head
            .as_ref()
            .map(|head| {
                (
                    head.last_sequence,
                    head.last_event_hash.clone(),
                    head.record_count,
                )
            })
            .unwrap_or_else(|| (0, ZERO_HASH.to_string(), 0));
        let mut new_records = records
            .iter()
            .filter(|record| record.sequence > existing_sequence)
            .collect::<Vec<_>>();
        new_records.sort_by_key(|record| record.sequence);
        if new_records.is_empty() {
            return Ok(PreparedStreamMetadataWrite {
                transaction_id: String::new(),
                owned_ops: Vec::new(),
            });
        }

        let mut previous_sequence = existing_sequence;
        let mut previous_event_hash = existing_hash;
        for record in &new_records {
            verify_stream_record_after_head(
                stream_id,
                previous_sequence,
                &previous_event_hash,
                record,
            )?;
            previous_sequence = record.sequence;
            previous_event_hash = record.event_hash.clone();
        }

        let mut payloads = Vec::with_capacity(new_records.len() + 1);
        let mut record_keys = Vec::with_capacity(new_records.len());
        for record in new_records {
            let inline_payload = record.payload.clone();
            let mut stored = StoredStreamRecordIndexRow::new(record, Some(inline_payload), None);
            record_keys.push(stream_record_key(stream_id, record.sequence));
            let mut payload = encode_stream_record_index_row(&stored)?;
            if payload.len() > CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES {
                let payload_locator = self.write_stream_record_payload(record).await?;
                stored = StoredStreamRecordIndexRow::new(record, None, Some(payload_locator));
                payload = encode_stream_record_index_row(&stored)?;
                if payload.len() > CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES {
                    bail!(
                        "CoreStore stream record metadata row is {} bytes, exceeding {} bytes",
                        payload.len(),
                        CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES
                    );
                }
            }
            payloads.push(payload);
        }
        let head = CoreStoredStreamHead {
            schema: "anvil.core.stream_head.v1".to_string(),
            stream_id: stream_id.to_string(),
            last_sequence: previous_sequence,
            last_event_hash: previous_event_hash,
            record_count: existing_record_count
                .checked_add(record_keys.len() as u64)
                .ok_or_else(|| anyhow!("CoreStore stream record count overflow"))?,
            updated_at: now_rfc3339(),
        };
        let head_key = stream_head_key(stream_id);
        payloads.push(encode_stream_head_record(&head)?);

        let mut owned_ops = Vec::with_capacity(payloads.len());
        for (idx, key) in record_keys.iter().enumerate() {
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_STREAM_RECORDS,
                table_id: TABLE_STREAM_RECORD_INDEX_ROW,
                tuple_key: key.clone(),
                payload: payloads[idx].clone(),
                common: None,
            });
        }
        owned_ops.push(OwnedCoreMetaBatchOp::Put {
            cf: CF_STREAM_HEADS,
            table_id: TABLE_STREAM_HEAD_ROW,
            tuple_key: head_key,
            payload: payloads
                .last()
                .expect("stream head payload is pushed after record payloads")
                .clone(),
            common: None,
        });
        let transaction_id = records
            .last()
            .and_then(|record| record.transaction_id.clone())
            .unwrap_or_else(|| {
                format!(
                    "stream:{stream_id}:{}:{}",
                    existing_sequence, head.last_sequence
                )
            });
        Ok(PreparedStreamMetadataWrite {
            transaction_id,
            owned_ops,
        })
    }

    pub(super) async fn write_stream_record_payload(
        &self,
        record: &StreamRecord,
    ) -> Result<CoreManifestLocator> {
        let payload_hash = format!("sha256:{}", sha256_hex(&record.payload));
        if payload_hash != record.payload_hash {
            bail!("CoreStore stream record payload hash mismatch before metadata write");
        }
        let payload_hash_hex = strip_sha256_prefix(&payload_hash)?;
        let stream_hash = sha256_hex(record.stream_id.as_bytes());
        self.write_logical_bytes_direct(
            "stream",
            format!(
                "lf_stream_record_payload_{stream_hash}_{:020}_{payload_hash_hex}",
                record.sequence
            ),
            record.sequence,
            record.payload.clone(),
            format!(
                "stream_record_payload_{stream_hash}_{:020}",
                record.sequence
            ),
            "local".to_string(),
        )
        .await
    }

    pub(super) async fn read_stream_records_from_meta(
        &self,
        stream_id: &str,
    ) -> Result<Vec<StreamRecord>> {
        let Some(head_bytes) = self.meta.get(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &stream_head_key(stream_id),
        )?
        else {
            return Ok(Vec::new());
        };
        let head = decode_stream_head_record(&head_bytes)?;
        if head.schema != "anvil.core.stream_head.v1" || head.stream_id != stream_id {
            bail!("CoreStore stream head metadata row has invalid scope");
        }
        let prefix = stream_record_prefix(stream_id);
        let mut records = Vec::new();
        for item in
            self.meta
                .scan_prefix(CF_STREAM_RECORDS, TABLE_STREAM_RECORD_INDEX_ROW, &prefix)?
        {
            let stored = decode_stream_record_index_row(&item.payload)?;
            if stored.stream_id != stream_id {
                bail!("CoreStore stream record metadata row has invalid scope");
            }
            records.push(self.stream_record_from_index_row(stored).await?);
        }
        records.sort_by_key(|record| record.sequence);
        if records.len() as u64 != head.record_count {
            bail!("CoreStore stream head record count does not match indexed rows");
        }
        let (last_sequence, last_event_hash) = stream_head_from_records(&records);
        if last_sequence != head.last_sequence || last_event_hash != head.last_event_hash {
            bail!("CoreStore stream head does not match indexed rows");
        }
        Ok(records)
    }

    pub(super) async fn read_stream_record_from_meta(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StreamRecord>> {
        let Some(bytes) = self.meta.get(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &stream_record_key(stream_id, sequence),
        )?
        else {
            return Ok(None);
        };
        let stored = decode_stream_record_index_row(&bytes)?;
        if stored.stream_id != stream_id || stored.sequence != sequence {
            bail!("CoreStore stream record metadata row has invalid scope");
        }
        self.stream_record_from_index_row(stored).await.map(Some)
    }

    pub(super) async fn read_stream_records_from_meta_range(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        if through_sequence <= after_sequence {
            return Ok(Vec::new());
        }
        let Some(head) = self.read_stream_head_from_meta(stream_id)? else {
            bail!("CoreStore stream {stream_id} is missing metadata head");
        };
        if through_sequence > head.last_sequence {
            bail!(
                "CoreStore stream {stream_id} range exceeds metadata head: through={}, head={}",
                through_sequence,
                head.last_sequence
            );
        }

        let mut last_requested = through_sequence;
        if limit > 0 {
            last_requested = last_requested
                .min(after_sequence.saturating_add(u64::try_from(limit).unwrap_or(u64::MAX)));
        }

        let mut records = Vec::new();
        let mut previous_record: Option<StreamRecord> = None;
        for sequence in after_sequence.saturating_add(1)..=last_requested {
            let Some(record) = self
                .read_stream_record_from_meta(stream_id, sequence)
                .await?
            else {
                bail!("CoreStore stream {stream_id} is missing record {sequence}");
            };
            if record.sequence != sequence {
                bail!("CoreStore stream record metadata row has invalid sequence");
            }
            if sequence == 1 && record.previous_event_hash != ZERO_HASH {
                bail!("CoreStore stream {stream_id} first record previous hash is invalid");
            }
            if let Some(previous) = previous_record.as_ref()
                && record.previous_event_hash != previous.event_hash
            {
                bail!("CoreStore stream {stream_id} range hash chain is invalid");
            }
            previous_record = Some(record.clone());
            records.push(record);
        }
        Ok(records)
    }

    pub(super) async fn stream_record_from_index_row(
        &self,
        row: StoredStreamRecordIndexRow,
    ) -> Result<StreamRecord> {
        validate_stream_record_index_row_metadata(&row.stream_id, &row)?;
        let payload = match (row.inline_payload.as_ref(), row.payload_locator.as_ref()) {
            (Some(inline), None) => inline.clone(),
            (None, Some(locator)) => {
                let manifest = self.read_logical_file_manifest(locator).await?;
                self.read_logical_file_plaintext(&manifest).await?
            }
            (Some(_), Some(_)) => {
                bail!("CoreStore stream record metadata row has both inline payload and locator")
            }
            (None, None) => {
                bail!("CoreStore stream record metadata row has neither inline payload nor locator")
            }
        };
        if payload.len() as u64 != row.payload_len {
            bail!("CoreStore stream record payload length mismatch");
        }
        let payload_hash = format!("sha256:{}", sha256_hex(&payload));
        if payload_hash != row.payload_hash {
            bail!("CoreStore stream record payload hash mismatch");
        }
        let record = StreamRecord {
            schema: CORE_WATCH_EVENT_SCHEMA.to_string(),
            stream_id: row.stream_id,
            partition_id: row.partition_id,
            sequence: row.sequence,
            cursor: row.cursor,
            previous_event_hash: row.previous_event_hash,
            event_hash: row.event_hash,
            record_kind: row.record_kind,
            payload_hash: row.payload_hash,
            payload,
            content_type: row.content_type,
            user_metadata_json: row.user_metadata_json,
            authenticated_principal: row.authenticated_principal,
            transaction_id: row.transaction_id,
            idempotency_key_hash: row.idempotency_key_hash,
            created_at: row.created_at,
        };
        Ok(record)
    }

    pub(super) fn read_stream_head_from_meta(
        &self,
        stream_id: &str,
    ) -> Result<Option<CoreStoredStreamHead>> {
        let Some(bytes) = self.meta.get(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &stream_head_key(stream_id),
        )?
        else {
            return Ok(None);
        };
        let head = decode_stream_head_record(&bytes)?;
        if head.schema != "anvil.core.stream_head.v1" || head.stream_id != stream_id {
            bail!("CoreStore stream head metadata row has invalid scope");
        }
        Ok(Some(head))
    }

    pub(super) async fn read_core_transaction_stream_records_from_root(
        &self,
    ) -> Result<Vec<StreamRecord>> {
        let Some(anchor) = self
            .read_latest_root_anchor(core_transaction_root_anchor_key())
            .await?
        else {
            return Ok(Vec::new());
        };
        let Some(transaction_manifest_locator) = anchor.transaction_manifest else {
            if anchor.root_generation == 0 {
                return Ok(Vec::new());
            }
            bail!("CoreStore non-genesis root anchor is missing transaction manifest");
        };
        validate_manifest_locator(&transaction_manifest_locator)?;
        if !is_inline_manifest_body_locator(&transaction_manifest_locator) {
            bail!("CoreStore transaction root manifest must use bounded inline CoreMeta storage");
        }
        let transaction_manifest_bytes =
            self.read_inline_manifest_body(&transaction_manifest_locator)?;
        let transaction = decode_transaction_manifest_record(&transaction_manifest_bytes)?;
        validate_transaction_manifest_record(&transaction, anchor.root_generation)?;
        if anchor.core_meta_commit_certificate_hash.as_deref()
            != Some(transaction.core_meta_commit_certificate_hash.as_str())
            || anchor.certificate_persist_receipt_hashes
                != transaction.certificate_persist_receipt_hashes
        {
            bail!("CoreStore transaction manifest commit evidence does not match root anchor");
        }
        for locator in &transaction.logical_manifests {
            if !is_inline_manifest_body_locator(locator) {
                bail!("CoreStore transaction checkpoint must use bounded inline CoreMeta storage");
            }
            let checkpoint_bytes = self.read_inline_manifest_body(locator)?;
            let checkpoint = decode_transaction_stream_checkpoint_record(&checkpoint_bytes)?;
            if checkpoint.schema != "anvil.core.transaction_stream_checkpoint.v1"
                || checkpoint.stream_id != CORE_TRANSACTION_STREAM_ID
            {
                bail!("CoreStore transaction stream checkpoint has invalid scope");
            }
            let records = self
                .read_stream_records_from_meta_range(
                    CORE_TRANSACTION_STREAM_ID,
                    0,
                    checkpoint.last_sequence,
                    0,
                )
                .await?;
            let (last_sequence, last_event_hash) = stream_head_from_records(&records);
            if last_sequence != checkpoint.last_sequence
                || last_event_hash != checkpoint.last_event_hash
            {
                bail!("CoreStore transaction stream checkpoint does not match metadata rows");
            }
            return Ok(records);
        }
        bail!("CoreStore transaction manifest is missing checkpoint logical manifests")
    }

    pub(super) async fn read_core_transaction_stream_records_after_from_root(
        &self,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<StreamRecord>> {
        let (_root_generation, visible_sequence) =
            self.core_transaction_stream_root_visibility(false).await?;
        self.read_stream_records_from_meta_range(
            CORE_TRANSACTION_STREAM_ID,
            after_sequence,
            visible_sequence,
            limit,
        )
        .await
    }

    pub(super) async fn core_transaction_stream_root_visibility(
        &self,
        _refresh: bool,
    ) -> Result<(u64, u64)> {
        let root_anchor_key = core_transaction_root_anchor_key();
        let anchor = self.read_latest_root_anchor(root_anchor_key).await?;
        let Some(anchor) = anchor else {
            return Ok((0, 0));
        };
        if anchor.root_generation == 0 {
            return Ok((0, 0));
        }
        let cursor = anchor
            .mutation_last
            .as_deref()
            .ok_or_else(|| anyhow!("CoreStore transaction root anchor is missing mutation_last"))?;
        let sequence = transaction_stream_sequence_from_cursor(cursor)?;
        Ok((anchor.root_generation, sequence))
    }

    pub(super) async fn write_core_transaction_stream_records(
        &self,
        records: &[StreamRecord],
        metadata_commits: &[CoreMetaQuorumCommitOutcome],
    ) -> Result<()> {
        let root_anchor_key = core_transaction_root_anchor_key();
        let current = self.read_latest_root_anchor(root_anchor_key).await?;
        let pre_root_generation = current
            .as_ref()
            .map(|anchor| anchor.root_generation)
            .unwrap_or(0);
        let post_root_generation = pre_root_generation.saturating_add(1);
        let (last_sequence, last_event_hash) = stream_head_from_records(records);
        let checkpoint = CoreTransactionStreamCheckpointRecord {
            schema: "anvil.core.transaction_stream_checkpoint.v1".to_string(),
            stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
            record_count: records.len() as u64,
            last_sequence,
            last_event_hash,
        };
        let checkpoint_bytes = encode_transaction_stream_checkpoint_record(&checkpoint)?;
        let checkpoint_hash = sha256_hex(&checkpoint_bytes);
        let (checkpoint_locator, checkpoint_op) = self.prepare_inline_manifest_body(
            WriterFamily::CoreControl.as_str(),
            canonical_logical_file_id(
                WriterFamily::CoreControl,
                post_root_generation,
                &format!("core_transaction_checkpoint:{post_root_generation:020}"),
                checkpoint_hash.as_bytes(),
            ),
            post_root_generation,
            checkpoint_bytes,
        )?;
        let transaction_root_key_hash = root_key_hash(core_transaction_root_anchor_key());
        let root_metadata_commit = metadata_commits
            .iter()
            .find(|outcome| {
                outcome.root_key_hash == transaction_root_key_hash
                    && outcome.post_root_generation == post_root_generation
            })
            .cloned()
            .or_else(|| None);
        let root_metadata_commit = match root_metadata_commit {
            Some(commit) => commit,
            None => self
                .recommit_existing_core_transaction_stream_metadata(records, post_root_generation)
                .await?
                .into_iter()
                .find(|outcome| {
                    outcome.root_key_hash == transaction_root_key_hash
                        && outcome.post_root_generation == post_root_generation
                })
                .ok_or_else(|| {
                    anyhow!(
                        "CoreStore root publication missing CoreMeta stream metadata commit evidence for generation {post_root_generation}"
                    )
                })?,
        };
        let transaction = CoreTransactionManifestRecord {
            schema: "anvil.core.transaction_manifest.v1".to_string(),
            mutation_ids: records
                .last()
                .map(|record| vec![record.cursor.clone()])
                .unwrap_or_default(),
            idempotency_key_hashes: records
                .last()
                .and_then(|record| record.idempotency_key_hash.clone())
                .into_iter()
                .collect(),
            pre_root_generation,
            post_root_generation,
            logical_manifests: vec![checkpoint_locator],
            core_meta_commit_certificate_hash: root_metadata_commit.certificate_hash.clone(),
            certificate_persist_receipt_hashes: root_metadata_commit
                .certificate_persist_receipt_hashes
                .clone(),
        };
        let transaction_manifest_bytes = encode_transaction_manifest_record(&transaction)?;
        let transaction_manifest_hash = sha256_hex(&transaction_manifest_bytes);
        let (transaction_manifest, transaction_manifest_op) = self.prepare_inline_manifest_body(
            WriterFamily::CoreControl.as_str(),
            canonical_logical_file_id(
                WriterFamily::CoreControl,
                post_root_generation,
                &format!("core_transaction_manifest:{post_root_generation:020}"),
                transaction_manifest_hash.as_bytes(),
            ),
            post_root_generation,
            transaction_manifest_bytes,
        )?;
        let manifest_ops = [checkpoint_op, transaction_manifest_op];
        let manifest_ops = borrow_owned_coremeta_batch_ops(&manifest_ops);
        self.commit_coremeta_batch_by_embedded_roots(
            &format!("core-transaction-manifests:{post_root_generation}"),
            &manifest_ops,
        )
        .await?;
        self.publish_core_transaction_root_anchor(
            records,
            transaction_manifest,
            current.as_ref(),
            post_root_generation,
        )
        .await
    }

    async fn recommit_existing_core_transaction_stream_metadata(
        &self,
        records: &[StreamRecord],
        post_root_generation: u64,
    ) -> Result<Vec<CoreMetaQuorumCommitOutcome>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let Some(head_bytes) = self.meta.get(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &stream_head_key(CORE_TRANSACTION_STREAM_ID),
        )?
        else {
            return Ok(Vec::new());
        };
        let head = decode_stream_head_record(&head_bytes)?;
        if head.stream_id != CORE_TRANSACTION_STREAM_ID {
            bail!("CoreStore transaction stream head metadata row has invalid scope");
        }
        if head.last_sequence != post_root_generation {
            bail!(
                "CoreStore transaction stream head is at generation {}, cannot reconstruct missing root generation {}",
                head.last_sequence,
                post_root_generation
            );
        }

        let mut owned_ops = Vec::with_capacity(records.len() + 1);
        for record in records {
            if record.stream_id != CORE_TRANSACTION_STREAM_ID {
                bail!(
                    "CoreStore transaction root publication received non-transaction stream record"
                );
            }
            if record.sequence > post_root_generation {
                bail!("CoreStore transaction stream record exceeds target root generation");
            }
            let key = stream_record_key(CORE_TRANSACTION_STREAM_ID, record.sequence);
            let Some(record_bytes) =
                self.meta
                    .get(CF_STREAM_RECORDS, TABLE_STREAM_RECORD_INDEX_ROW, &key)?
            else {
                return Ok(Vec::new());
            };
            let stored = decode_stream_record_index_row(&record_bytes)?;
            if stored.stream_id != CORE_TRANSACTION_STREAM_ID
                || stored.sequence != record.sequence
                || stored.event_hash != record.event_hash
            {
                bail!("CoreStore transaction stream record metadata row has invalid scope");
            }
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_STREAM_RECORDS,
                table_id: TABLE_STREAM_RECORD_INDEX_ROW,
                tuple_key: key,
                payload: record_bytes,
                common: None,
            });
        }
        owned_ops.push(OwnedCoreMetaBatchOp::Put {
            cf: CF_STREAM_HEADS,
            table_id: TABLE_STREAM_HEAD_ROW,
            tuple_key: stream_head_key(CORE_TRANSACTION_STREAM_ID),
            payload: head_bytes,
            common: None,
        });
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        self.commit_coremeta_batch_by_embedded_roots(
            &format!("core-transaction-stream-recover:{post_root_generation}"),
            &ops,
        )
        .await
    }

    pub(super) async fn publish_core_transaction_root_anchor(
        &self,
        records: &[StreamRecord],
        transaction_manifest: CoreManifestLocator,
        current: Option<&CoreRootAnchorRecord>,
        root_generation: u64,
    ) -> Result<()> {
        let root_anchor_key = core_transaction_root_anchor_key();
        let root_key_hash = root_key_hash(root_anchor_key);
        let previous_root_hash = current
            .map(hash_root_anchor_record)
            .transpose()?
            .unwrap_or_else(|| ZERO_HASH.to_string());
        let writer_families = records
            .iter()
            .map(|record| {
                if record.stream_id == CORE_TRANSACTION_STREAM_ID {
                    WriterFamily::CoreControl.as_str().to_string()
                } else {
                    record.partition_id.clone()
                }
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let final_block_count = transaction_manifest
            .block_locators
            .iter()
            .map(|block| block.data_shards + block.parity_shards)
            .sum::<u32>() as u64;
        let transaction_manifest_bytes = self.read_inline_manifest_body(&transaction_manifest)?;
        let transaction = decode_transaction_manifest_record(&transaction_manifest_bytes)?;
        validate_transaction_manifest_record(&transaction, root_generation)?;
        let anchor = CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash,
            root_generation,
            previous_root_hash,
            transaction_manifest: Some(transaction_manifest),
            checkpoint_manifest: None,
            core_meta_commit_certificate_hash: Some(
                transaction.core_meta_commit_certificate_hash.clone(),
            ),
            certificate_persist_receipt_hashes: transaction
                .certificate_persist_receipt_hashes
                .clone(),
            publisher_node_id: CORE_PENDING_MUTATION_NODE_ID.to_string(),
            publisher_epoch: LOCAL_PLACEMENT_EPOCH,
            partition_owner_fence: LOCAL_PLACEMENT_EPOCH,
            created_at_unix_nanos: unix_timestamp_nanos(),
            root_state: "committed".to_string(),
            mutation_first: records.first().map(|record| record.cursor.clone()),
            mutation_last: records.last().map(|record| record.cursor.clone()),
            writer_families,
            manifest_count: 1,
            final_block_count,
            genesis_bundle: None,
        };
        self.write_root_anchor_generation(&anchor).await
    }

    pub(super) async fn bootstrap_system_root_anchor(&self) -> Result<()> {
        let root_anchor_key = core_transaction_root_anchor_key();
        if self
            .read_latest_root_anchor(root_anchor_key)
            .await?
            .is_some()
        {
            return Ok(());
        }
        let anchor = CoreRootAnchorRecord {
            schema: "anvil.core.root_anchor.v1".to_string(),
            root_anchor_key: root_anchor_key.to_string(),
            root_key_hash: root_key_hash(root_anchor_key),
            root_generation: 0,
            previous_root_hash: ZERO_HASH.to_string(),
            transaction_manifest: None,
            checkpoint_manifest: None,
            core_meta_commit_certificate_hash: None,
            certificate_persist_receipt_hashes: Vec::new(),
            publisher_node_id: "genesis".to_string(),
            publisher_epoch: 0,
            partition_owner_fence: 0,
            created_at_unix_nanos: 0,
            root_state: "committed".to_string(),
            mutation_first: Some("genesis".to_string()),
            mutation_last: Some("genesis".to_string()),
            writer_families: vec![
                WriterFamily::MeshControl.as_str().to_string(),
                WriterFamily::Authz.as_str().to_string(),
                WriterFamily::CoreControl.as_str().to_string(),
            ],
            manifest_count: 0,
            final_block_count: 0,
            genesis_bundle: Some(build_core_genesis_bundle(root_anchor_key)?),
        };
        self.write_root_anchor_generation(&anchor).await
    }

    pub(super) async fn read_latest_root_anchor(
        &self,
        root_anchor_key: &str,
    ) -> Result<Option<CoreRootAnchorRecord>> {
        let root_key_hash = root_key_hash(root_anchor_key);
        let Some(bytes) = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_cache_key(root_anchor_key),
        )?
        else {
            return Ok(None);
        };
        let anchor = decode_root_cache_row(&bytes)?;
        if anchor.root_anchor_key != root_anchor_key || anchor.root_key_hash != root_key_hash {
            bail!("CoreStore root anchor scope mismatch");
        }
        if self
            .verify_root_anchor_direct_predecessor(&root_key_hash, root_anchor_key, &anchor)
            .await?
        {
            Ok(Some(anchor))
        } else {
            Ok(None)
        }
    }

    pub(super) async fn verify_root_anchor_direct_predecessor(
        &self,
        root_key_hash: &str,
        root_anchor_key: &str,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<bool> {
        if anchor.root_key_hash != root_key_hash || anchor.root_anchor_key != root_anchor_key {
            bail!("CoreStore root anchor scope mismatch");
        }
        if anchor.root_generation == 0 {
            return Ok(anchor.previous_root_hash == ZERO_HASH);
        }

        let Some(previous) = self
            .read_committed_root_anchor_generation(
                root_key_hash,
                anchor.root_generation.saturating_sub(1),
            )
            .await?
        else {
            return Ok(false);
        };
        if previous.root_anchor_key != root_anchor_key || previous.root_key_hash != root_key_hash {
            bail!("CoreStore root anchor predecessor scope mismatch");
        }
        Ok(anchor.previous_root_hash == hash_root_anchor_record(&previous)?)
    }

    pub(super) async fn verify_root_anchor_chain(
        &self,
        root_key_hash: &str,
        root_anchor_key: &str,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<bool> {
        if anchor.root_generation == 0 {
            return Ok(anchor.previous_root_hash == ZERO_HASH);
        }

        let mut expected_child = anchor.clone();
        for generation in (0..anchor.root_generation).rev() {
            let Some(previous) = self
                .read_committed_root_anchor_generation(root_key_hash, generation)
                .await?
            else {
                return Ok(false);
            };
            if previous.root_anchor_key != root_anchor_key {
                bail!("CoreStore root anchor chain key mismatch");
            }
            let previous_hash = hash_root_anchor_record(&previous)?;
            if expected_child.previous_root_hash != previous_hash {
                return Ok(false);
            }
            expected_child = previous;
        }
        Ok(expected_child.previous_root_hash == ZERO_HASH)
    }

    pub(super) async fn read_committed_root_anchor_generation(
        &self,
        root_key_hash: &str,
        generation: u64,
    ) -> Result<Option<CoreRootAnchorRecord>> {
        let Some(bytes) = self.meta.get(
            CF_ROOT_CACHE,
            TABLE_ROOT_CACHE_ROW,
            &root_anchor_generation_key(root_key_hash, generation),
        )?
        else {
            return Ok(None);
        };
        let anchor = decode_root_cache_row(&bytes)?;
        if anchor.root_key_hash != root_key_hash || anchor.root_generation != generation {
            bail!("CoreStore root anchor generation row scope mismatch");
        }
        Ok(Some(anchor))
    }

    pub(super) async fn write_root_anchor_generation(
        &self,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        self.publish_root_anchor_generation(anchor).await
    }

    pub(super) async fn write_root_anchor_generation_local(
        &self,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        let cas_started_at = Instant::now();
        validate_root_anchor_record(anchor)?;
        let anchor_bytes = encode_root_anchor_record(anchor)?;
        let root_anchor_hash = format!("sha256:{}", sha256_hex(&anchor_bytes));
        match self
            .read_latest_root_anchor(&anchor.root_anchor_key)
            .await?
        {
            Some(current) => {
                let current_hash = hash_root_anchor_record(&current)?;
                if anchor.root_generation < current.root_generation {
                    bail!(
                        "CoreStore root anchor rejected stale generation {} below current {}",
                        anchor.root_generation,
                        current.root_generation
                    );
                }
                if anchor.root_generation == current.root_generation {
                    if root_anchor_hash == current_hash {
                        return Ok(());
                    }
                    bail!(
                        "CoreStore root anchor rejected conflicting generation {}",
                        anchor.root_generation
                    );
                }
                if anchor.root_generation != current.root_generation.saturating_add(1) {
                    bail!("CoreStore root anchor generations must be contiguous");
                }
                if anchor.previous_root_hash != current_hash {
                    bail!("CoreStore root anchor previous hash mismatch");
                }
                if anchor.publisher_epoch < current.publisher_epoch
                    || (anchor.publisher_epoch == current.publisher_epoch
                        && anchor.partition_owner_fence < current.partition_owner_fence)
                {
                    bail!("CoreStore root anchor rejected stale owner fence");
                }
            }
            None => {
                if anchor.root_generation != 0 {
                    bail!("CoreStore root anchor first generation must be zero");
                }
                if anchor.previous_root_hash != ZERO_HASH {
                    bail!("CoreStore root anchor genesis previous hash must be zero");
                }
            }
        }
        let row = encode_root_cache_row(anchor)?;
        self.validate_root_anchor_coremeta_commit_evidence(anchor)?;
        let generation_key =
            root_anchor_generation_key(&anchor.root_key_hash, anchor.root_generation);
        let latest_key = root_cache_key(&anchor.root_anchor_key);
        let latest_hash_key = root_cache_hash_key(&anchor.root_key_hash);
        let ops = [
            CoreMetaBatchOp {
                cf: CF_ROOT_CACHE,
                table_id: TABLE_ROOT_CACHE_ROW,
                tuple_key: &generation_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&row),
            },
            CoreMetaBatchOp {
                cf: CF_ROOT_CACHE,
                table_id: TABLE_ROOT_CACHE_ROW,
                tuple_key: &latest_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&row),
            },
            CoreMetaBatchOp {
                cf: CF_ROOT_CACHE,
                table_id: TABLE_ROOT_CACHE_ROW,
                tuple_key: &latest_hash_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&row),
            },
        ];
        let encoded = self.meta.encode_batch_ops(&ops)?;
        let borrowed = encoded
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        self.write_coremeta_encoded_rows(&borrowed)?;
        crate::perf::record_duration(
            "anvil_root_register_cas_duration_ms",
            &[
                ("root_kind", "root_anchor"),
                ("partition_id_hash", anchor.root_key_hash.as_str()),
                ("outcome", "ok"),
            ],
            cas_started_at.elapsed(),
        );
        record_corestore_trace_event("root_register.cas_write", "ok");
        Ok(())
    }

    pub(super) fn validate_root_anchor_coremeta_commit_evidence(
        &self,
        anchor: &CoreRootAnchorRecord,
    ) -> Result<()> {
        if anchor.root_generation == 0 {
            return Ok(());
        }
        let certificate_hash = anchor
            .core_meta_commit_certificate_hash
            .as_deref()
            .ok_or_else(|| {
                anyhow!("CoreStore root anchor is missing CoreMeta commit certificate")
            })?;
        let evidence = self
            .read_coremeta_commit_evidence(certificate_hash)?
            .ok_or_else(|| anyhow!("CoreStore root anchor references missing CoreMeta evidence"))?;
        let api_certificate =
            decode_deterministic_proto::<crate::anvil_api::CoreMetaCommitCertificate>(
                &evidence.certificate_bytes,
                "CoreMeta commit certificate evidence",
            )?;
        if api_certificate.certificate_hash != certificate_hash {
            bail!("CoreStore root anchor CoreMeta certificate hash mismatch");
        }
        let core_certificate = api_commit_certificate_to_core(api_certificate)?;
        let mut persist_receipts =
            Vec::with_capacity(evidence.certificate_persist_receipt_bytes.len());
        let mut persist_receipt_hashes =
            Vec::with_capacity(evidence.certificate_persist_receipt_bytes.len());
        for bytes in &evidence.certificate_persist_receipt_bytes {
            let api_receipt = decode_deterministic_proto::<
                crate::anvil_api::CoreMetaCertificatePersistReceipt,
            >(bytes, "CoreMeta certificate persist receipt evidence")?;
            let core_receipt = api_persist_receipt_to_core(api_receipt)?;
            persist_receipt_hashes.push(certificate_persist_receipt_payload_hash(&core_receipt)?);
            persist_receipts.push(core_receipt);
        }
        persist_receipt_hashes.sort();
        persist_receipt_hashes.dedup();
        let mut anchor_receipt_hashes = anchor.certificate_persist_receipt_hashes.clone();
        anchor_receipt_hashes.sort();
        anchor_receipt_hashes.dedup();
        let mut evidence_receipt_hashes = evidence.certificate_persist_receipt_hashes.clone();
        evidence_receipt_hashes.sort();
        evidence_receipt_hashes.dedup();
        if persist_receipt_hashes != evidence_receipt_hashes
            || persist_receipt_hashes != anchor_receipt_hashes
        {
            bail!("CoreStore root anchor CoreMeta certificate persist receipt mismatch");
        }
        if core_certificate.root_key_hash != anchor.root_key_hash
            || core_certificate.post_root_generation != anchor.root_generation
        {
            bail!(
                "CoreStore root anchor CoreMeta certificate scope mismatch: certificate root {} generation {}, anchor root {} generation {}",
                core_certificate.root_key_hash,
                core_certificate.post_root_generation,
                anchor.root_key_hash,
                anchor.root_generation
            );
        }
        let quorum_profile = self.default_coremeta_quorum_profile()?;
        validate_commit_evidence_with_verifier(
            &quorum_profile,
            &core_certificate,
            &persist_receipts,
            |node_id, signed_payload_hash, signature| {
                self.verify_internal_core_receipt_signature(node_id, signed_payload_hash, signature)
            },
        )?;
        Ok(())
    }

    pub(super) async fn acquire_named_lock(&self, kind: &str, id: &str) -> Result<CoreStoreLock> {
        let lock_path = self
            .storage
            .core_store_staging_path()
            .join("locks")
            .join(kind)
            .join(format!("{}.lock", logical_file_name(id)));
        if let Some(parent) = lock_path.parent() {
            let started_at = Instant::now();
            fs::create_dir_all(parent).await?;
            crate::perf::record_io_duration(
                "core_store",
                "lock_create_dir_all",
                parent,
                0,
                started_at.elapsed(),
            );
        }
        for _ in 0..CORE_PROCESS_LOCK_RETRY_ATTEMPTS {
            let started_at = Instant::now();
            let open_result = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&lock_path)
                .await;
            crate::perf::record_io_duration(
                "core_store",
                "lock_create_new",
                &lock_path,
                0,
                started_at.elapsed(),
            );
            match open_result {
                Ok(_) => return Ok(CoreStoreLock { path: lock_path }),
                Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                    tokio::time::sleep(CORE_PROCESS_LOCK_RETRY_DELAY).await;
                }
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "create CoreStore process write lock {}",
                            lock_path.display()
                        )
                    });
                }
            }
        }
        bail!("CoreStore {kind} {id} lock was not acquired")
    }

    pub(super) fn shard_path(&self, node_id: &str, block_id: &str, shard_index: u16) -> PathBuf {
        let block_path_hash = sha256_hex(block_id.as_bytes());
        let prefix = &block_path_hash[0..2];
        self.storage
            .core_store_local_block_cache_path()
            .join(LOCAL_ERASURE_SET_ID)
            .join(node_id)
            .join("block-id")
            .join(prefix)
            .join(block_path_hash)
            .join(format!("shard-{shard_index:05}-{block_id}.anb"))
    }

    pub(super) fn admission_root(&self) -> PathBuf {
        self.storage.core_store_admission_path()
    }

    pub(super) fn admission_landed_bytes_root(&self) -> PathBuf {
        self.storage.core_store_landed_bytes_path()
    }

    pub(super) fn landed_bytes_path(&self, hash: &str) -> PathBuf {
        self.admission_landed_bytes_root()
            .join("sha256")
            .join(&hash[0..2])
            .join(format!("{hash}.landed"))
    }
}

fn logical_file_manifest_single_block_object_ref(
    manifest: &CoreLogicalFileManifest,
) -> Result<CoreObjectRef> {
    let Some(block) = manifest.blocks.first() else {
        bail!("CoreStore logical file has no blocks");
    };
    if manifest.blocks.len() != 1 {
        bail!("CoreStore multi-block logical file cannot be exposed as a single object manifest");
    }
    object_ref_from_logical_block_ref(block, &manifest.erasure_profile_id)
}

fn stream_coremeta_root_key_hash(stream_id: &str) -> String {
    if stream_id == CORE_TRANSACTION_STREAM_ID {
        root_key_hash(core_transaction_root_anchor_key())
    } else {
        root_key_hash(&format!("stream/{stream_id}"))
    }
}

fn transaction_stream_sequence_from_cursor(cursor: &str) -> Result<u64> {
    let Some(raw_sequence) = cursor
        .strip_prefix(CORE_TRANSACTION_STREAM_ID)
        .and_then(|rest| rest.strip_prefix(':'))
    else {
        bail!("CoreStore transaction root anchor has invalid mutation cursor");
    };
    raw_sequence
        .parse::<u64>()
        .with_context(|| format!("parse CoreStore transaction cursor sequence {cursor}"))
}
