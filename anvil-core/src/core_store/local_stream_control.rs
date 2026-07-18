#[path = "control_record_proto.rs"]
pub(in crate::core_store::local) mod control_record_proto;

use super::local_tx_rows::borrow_owned_coremeta_batch_ops;
use super::*;
use crate::formats::{
    hash32,
    writer::{WriterFamily, canonical_logical_file_id},
};
use control_record_proto::*;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct CoreControlCurrentRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
    #[prost(string, tag = "4")]
    payload_hash: String,
}

impl CoreStore {
    pub async fn put_boundary_schema(
        &self,
        input: PutBoundarySchema,
    ) -> Result<BoundarySchemaReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "put_boundary_schema")],
        );
        validate_logical_id(&input.mutation_id, "boundary schema mutation id")?;
        let mut schema = input.schema;
        if schema.created_at.is_empty() {
            schema.created_at = now_rfc3339();
        }

        let _guard = self.write_lock.lock().await;
        let current_schema = self.read_latest_boundary_schema_unlocked(&schema.bucket)?;
        validate_boundary_schema(&schema, current_schema.as_ref(), input.expected_generation)?;

        let bytes = encode_boundary_schema_record(&schema)?;
        let schema_hash = format!("sha256:{}", sha256_hex(&bytes));
        let schema_key = boundary_schema_coremeta_key(&schema.bucket, schema.generation)?;
        self.commit_coremeta_batch_by_embedded_roots(
            &format!("boundary-schema:{}:{}", schema.bucket, schema.generation),
            &[CoreMetaBatchOp {
                cf: CF_BOUNDARY,
                table_id: TABLE_BOUNDARY_SCHEMA_ROW,
                tuple_key: &schema_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&bytes),
            }],
        )
        .await?;

        Ok(BoundarySchemaReceipt {
            bucket: schema.bucket,
            generation: schema.generation,
            row_generation: schema.generation,
            schema_hash,
        })
    }

    pub async fn put_boundary_schema_in_transaction(
        &self,
        input: PutBoundarySchema,
        transaction_id: &str,
        principal: &str,
    ) -> Result<BoundarySchemaReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "put_boundary_schema_in_transaction")],
        );
        validate_logical_id(&input.mutation_id, "boundary schema mutation id")?;
        validate_logical_id(transaction_id, "boundary schema transaction id")?;
        validate_logical_id(principal, "boundary schema principal")?;
        let mut schema = input.schema;
        if schema.created_at.is_empty() {
            schema.created_at = now_rfc3339();
        }

        let (bytes, schema_hash, schema_key) = {
            let _guard = self.write_lock.lock().await;
            let current_schema = self.read_latest_boundary_schema_unlocked(&schema.bucket)?;
            validate_boundary_schema(&schema, current_schema.as_ref(), input.expected_generation)?;
            let bytes = encode_boundary_schema_record(&schema)?;
            let schema_hash = format!("sha256:{}", sha256_hex(&bytes));
            let schema_key = boundary_schema_coremeta_key(&schema.bucket, schema.generation)?;
            (bytes, schema_hash, schema_key)
        };
        self.stage_coremeta_put_in_transaction(
            transaction_id,
            principal,
            CF_BOUNDARY,
            TABLE_BOUNDARY_SCHEMA_ROW,
            schema_key,
            bytes,
            None,
            true,
            false,
        )
        .await?;

        Ok(BoundarySchemaReceipt {
            bucket: schema.bucket,
            generation: schema.generation,
            row_generation: schema.generation,
            schema_hash,
        })
    }

    pub async fn read_boundary_schema(&self, bucket: &str) -> Result<Option<CoreBoundarySchema>> {
        validate_logical_id(bucket, "boundary schema bucket")?;
        self.read_latest_boundary_schema_unlocked(bucket)
    }

    pub async fn read_boundary_schema_generation(
        &self,
        bucket: &str,
        generation: u64,
    ) -> Result<Option<CoreBoundarySchema>> {
        validate_logical_id(bucket, "boundary schema bucket")?;
        let key = boundary_schema_coremeta_key(bucket, generation)?;
        let Some(bytes) = self
            .meta
            .get(CF_BOUNDARY, TABLE_BOUNDARY_SCHEMA_ROW, &key)?
        else {
            return Ok(None);
        };
        let schema = decode_boundary_schema_record(&bytes)?;
        if schema.schema != CORE_BOUNDARY_SCHEMA_SCHEMA || schema.bucket != bucket {
            bail!("CoreStore boundary schema row has invalid scope");
        }
        if schema.generation != generation {
            bail!("CoreStore boundary schema row generation mismatch");
        }
        Ok(Some(schema))
    }

    pub async fn read_boundary_schema_generation_hash(&self, bucket: &str) -> Result<String> {
        validate_logical_id(bucket, "boundary schema bucket")?;
        let Some(schema) = self.read_latest_boundary_schema_unlocked(bucket)? else {
            return Ok("none:0".to_string());
        };
        let bytes = encode_boundary_schema_record(&schema)?;
        Ok(format!(
            "generation:{}:sha256:{}",
            schema.generation,
            sha256_hex(&bytes)
        ))
    }

    pub async fn put_boundary_values_for_object(
        &self,
        bucket: &str,
        object_ref: &str,
        values: &[CoreBoundaryValue],
    ) -> Result<()> {
        validate_logical_id(bucket, "boundary value bucket")?;
        validate_logical_id(object_ref, "boundary value object ref")?;
        let _guard = self.write_lock.lock().await;
        let mut keys = Vec::with_capacity(values.len());
        let mut rows = Vec::with_capacity(values.len());
        for value in values {
            validate_logical_id(&value.name, "boundary value dimension")?;
            keys.push(boundary_value_coremeta_key(
                bucket, value, object_ref, "object",
            )?);
            rows.push(encode_boundary_value_row(
                bucket, object_ref, "object", value,
            )?);
        }
        let ops = keys
            .iter()
            .zip(rows.iter())
            .map(|(key, row)| CoreMetaBatchOp {
                cf: CF_BOUNDARY,
                table_id: TABLE_BOUNDARY_VALUE_ROW,
                tuple_key: key.as_slice(),
                common: None,
                kind: CoreMetaBatchOpKind::Put(row),
            })
            .collect::<Vec<_>>();
        self.commit_coremeta_batch_by_embedded_roots(
            &Self::boundary_values_transaction_id(bucket, object_ref),
            &ops,
        )
        .await?;
        Ok(())
    }

    fn boundary_values_transaction_id(bucket: &str, object_ref: &str) -> String {
        format!(
            "boundary-values:{bucket}:{}",
            sha256_hex(object_ref.as_bytes())
        )
    }

    fn read_latest_boundary_schema_unlocked(
        &self,
        bucket: &str,
    ) -> Result<Option<CoreBoundarySchema>> {
        validate_logical_id(bucket, "boundary schema bucket")?;
        let prefix = boundary_schema_coremeta_prefix(bucket)?;
        let mut latest = None;
        for row in self
            .meta
            .scan_prefix(CF_BOUNDARY, TABLE_BOUNDARY_SCHEMA_ROW, &prefix)?
        {
            let schema = decode_boundary_schema_record(&row.payload)?;
            if schema.schema != CORE_BOUNDARY_SCHEMA_SCHEMA {
                bail!("CoreStore boundary schema has invalid schema");
            }
            if schema.bucket != bucket {
                bail!("CoreStore boundary schema bucket mismatch");
            }
            if latest
                .as_ref()
                .is_none_or(|current: &CoreBoundarySchema| schema.generation > current.generation)
            {
                latest = Some(schema);
            }
        }
        Ok(latest)
    }

    pub async fn append_stream(&self, input: AppendStreamRecord) -> Result<StreamAppendReceipt> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "append_stream")]);
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(&input.partition_id, "partition id")?;
        let _stream_guard = self.acquire_named_lock("stream", &input.stream_id).await?;
        let _guard = self.write_lock.lock().await;
        if let Some(receipt) = self.stream_idempotent_replay_unlocked(&input).await? {
            return Ok(receipt);
        }
        if let Some(fence) = input.fence.as_ref() {
            self.validate_fence_precondition_unlocked(fence).await?;
        }
        let pending_mutation_payload =
            if input.payload.len() <= CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
                CorePendingMutationPayload::Inline(&input.payload)
            } else {
                CorePendingMutationPayload::Landed(&input.payload)
            };
        let admission = self
            .admit_core_mutation(
                "stream.append",
                "stream",
                CorePendingMutationTarget::StreamAppend {
                    stream_id: input.stream_id.clone(),
                    partition_id: input.partition_id.clone(),
                    record_kind: input.record_kind.clone(),
                    transaction_id: input.transaction_id.clone(),
                },
                input
                    .transaction_id
                    .clone()
                    .unwrap_or_else(|| format!("stream-append:{}", uuid::Uuid::new_v4())),
                input.idempotency_key.clone(),
                pending_mutation_payload,
                Vec::new(),
            )
            .await?;
        match self.append_stream_unlocked(input).await {
            Ok(outcome) => {
                let result = outcome.state_locator.as_ref().map(|locator| {
                    CorePendingMutationFinalisationResult::StreamStateLocator(locator.clone())
                });
                self.mark_pending_mutation_finalised_with_result_unlocked(
                    &admission,
                    "committed",
                    result,
                )
                .await?;
                Ok(outcome.receipt)
            }
            Err(error) => {
                self.mark_pending_mutation_finalised_unlocked(&admission, "aborted")
                    .await
                    .with_context(|| "mark failed CoreStore stream append admission as aborted")?;
                Err(error)
            }
        }
    }

    pub(crate) async fn read_raw_stream(&self, stream_id: &str) -> Result<Vec<StreamRecord>> {
        validate_logical_id(stream_id, "stream id")?;
        self.read_all_stream_records(stream_id).await
    }

    pub(crate) async fn read_raw_stream_record(
        &self,
        stream_id: &str,
        sequence: u64,
        event_hash: &str,
    ) -> Result<Option<StreamRecord>> {
        validate_logical_id(stream_id, "stream id")?;
        Ok(self
            .read_all_stream_records(stream_id)
            .await?
            .into_iter()
            .find(|record| record.sequence == sequence && record.event_hash == event_hash))
    }

    pub(crate) async fn raw_stream_head(&self, stream_id: &str) -> Result<(u64, String)> {
        Ok(self
            .read_stream_head_from_meta(stream_id)?
            .map(|head| (head.last_sequence, head.last_event_hash))
            .unwrap_or_else(|| (0, ZERO_HASH.to_string())))
    }

    pub(crate) fn raw_stream_record_metadata_range(
        &self,
        stream_id: &str,
        after_sequence: u64,
        through_sequence: u64,
        limit: usize,
    ) -> Result<Vec<CoreStreamRecordMetadata>> {
        self.read_stream_record_index_rows_from_meta_range(
            stream_id,
            after_sequence,
            through_sequence,
            limit,
        )
        .map(|rows| {
            rows.into_iter()
                .map(|row| CoreStreamRecordMetadata {
                    sequence: row.sequence,
                    event_hash: row.event_hash,
                    record_kind: row.record_kind,
                    payload_len: row.payload_len,
                })
                .collect()
        })
    }

    pub(crate) async fn visible_stream_record_metadata(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<CoreStreamRecordMetadata>> {
        let Some(row) = self.read_stream_record_index_row_from_meta(stream_id, sequence)? else {
            return Ok(None);
        };
        if !self
            .stream_record_identity_is_visible(
                &row.stream_id,
                row.sequence,
                &row.event_hash,
                row.transaction_id.as_deref(),
            )
            .await?
        {
            return Ok(None);
        }
        Ok(Some(CoreStreamRecordMetadata {
            sequence: row.sequence,
            event_hash: row.event_hash,
            record_kind: row.record_kind,
            payload_len: row.payload_len,
        }))
    }

    pub(crate) async fn visible_stream_head_metadata(
        &self,
        stream_id: &str,
    ) -> Result<Option<CoreStreamRecordMetadata>> {
        let (mut sequence, _) = self.raw_stream_head(stream_id).await?;
        while sequence > 0 {
            if let Some(metadata) = self
                .visible_stream_record_metadata(stream_id, sequence)
                .await?
            {
                return Ok(Some(metadata));
            }
            sequence = sequence.saturating_sub(1);
        }
        Ok(None)
    }

    #[cfg(test)]
    pub(crate) fn corrupt_stream_record_payload_for_test(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<()> {
        let key = stream_record_key(stream_id, sequence);
        let Some(bytes) = self
            .meta
            .get(CF_STREAM_RECORDS, TABLE_STREAM_RECORD_INDEX_ROW, &key)?
        else {
            bail!("CoreStore stream record metadata row not found");
        };
        let mut record = decode_stream_record_index_row(&bytes)?;
        if record.payload_hash.len() < "sha256:".len() + 1 {
            bail!("CoreStore stream record payload hash is invalid");
        }
        let replacement = if record.payload_hash.ends_with('0') {
            '1'
        } else {
            '0'
        };
        record.payload_hash.pop();
        record.payload_hash.push(replacement);
        self.meta.put(
            CF_STREAM_RECORDS,
            TABLE_STREAM_RECORD_INDEX_ROW,
            &key,
            &encode_stream_record_index_row(&record)?,
        )?;
        Ok(())
    }

    pub(super) async fn append_stream_unlocked(
        &self,
        input: AppendStreamRecord,
    ) -> Result<StreamAppendOutcome> {
        let idempotency_key_hash = input
            .idempotency_key
            .as_deref()
            .map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
        self.append_stream_unlocked_with_idempotency_hash(input, idempotency_key_hash)
            .await
    }

    pub(super) async fn append_stream_unlocked_with_idempotency_hash(
        &self,
        input: AppendStreamRecord,
        idempotency_key_hash: Option<String>,
    ) -> Result<StreamAppendOutcome> {
        let prepared = self
            .prepare_stream_append_unlocked_with_idempotency_hash(input, idempotency_key_hash)
            .await?;
        if prepared.metadata.owned_ops.is_empty() {
            return Ok(prepared.outcome);
        }
        let ops = borrow_owned_coremeta_batch_ops(&prepared.metadata.owned_ops);
        let metadata_commits = self
            .commit_coremeta_batch_by_embedded_roots(&prepared.metadata.transaction_id, &ops)
            .await?;
        if let Some(record) = prepared
            .record
            .as_ref()
            .filter(|record| record.stream_id == CORE_TRANSACTION_STREAM_ID)
        {
            self.write_core_transaction_stream_records(
                std::slice::from_ref(record),
                &metadata_commits,
            )
            .await?;
        }
        Ok(prepared.outcome)
    }

    pub(super) async fn prepare_stream_append_unlocked_with_idempotency_hash(
        &self,
        input: AppendStreamRecord,
        idempotency_key_hash: Option<String>,
    ) -> Result<PreparedStreamAppend> {
        if let Some(fence) = input.fence.as_ref() {
            self.validate_fence_precondition_unlocked(fence).await?;
        }
        if let Some(receipt) = self
            .stream_idempotent_replay_by_hash_unlocked(
                &input.stream_id,
                &input.payload,
                idempotency_key_hash.as_deref(),
                input.transaction_id.as_deref(),
            )
            .await?
        {
            return Ok(PreparedStreamAppend {
                outcome: StreamAppendOutcome {
                    receipt,
                    state_locator: None,
                },
                record: None,
                metadata: PreparedStreamMetadataWrite {
                    transaction_id: String::new(),
                    owned_ops: Vec::new(),
                },
            });
        }
        self.prepare_new_stream_append_unlocked_with_idempotency_hash(input, idempotency_key_hash)
            .await
    }

    pub(super) async fn prepare_new_stream_append_unlocked_with_idempotency_hash(
        &self,
        input: AppendStreamRecord,
        idempotency_key_hash: Option<String>,
    ) -> Result<PreparedStreamAppend> {
        let payload_hash = format!("sha256:{}", sha256_hex(&input.payload));
        let head = self.read_stream_head_from_meta(&input.stream_id)?;
        let (last_sequence, previous_event_hash) = head
            .as_ref()
            .map(|head| (head.last_sequence, head.last_event_hash.clone()))
            .unwrap_or_else(|| (0, ZERO_HASH.to_string()));
        let sequence = last_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("CoreStore stream sequence overflow"))?;
        let cursor = format!("{}:{sequence:020}", input.stream_id);
        let mut record = StreamRecord {
            schema: CORE_WATCH_EVENT_SCHEMA.to_string(),
            stream_id: input.stream_id.clone(),
            partition_id: input.partition_id,
            sequence,
            cursor,
            previous_event_hash,
            event_hash: String::new(),
            record_kind: input.record_kind,
            payload_hash,
            payload: input.payload,
            content_type: input.content_type,
            user_metadata_json: input.user_metadata_json,
            transaction_id: input.transaction_id,
            idempotency_key_hash,
            created_at: now_rfc3339(),
        };
        record.event_hash = format!("sha256:{}", sha256_hex(&event_hash_input(&record)?));
        let metadata = self
            .prepare_stream_metadata_rows(&input.stream_id, std::slice::from_ref(&record))
            .await?;
        let receipt = StreamAppendReceipt {
            stream_id: record.stream_id.clone(),
            sequence: record.sequence,
            cursor: record.cursor.clone(),
            event_hash: record.event_hash.clone(),
            idempotent_replay: false,
        };
        Ok(PreparedStreamAppend {
            outcome: StreamAppendOutcome {
                receipt,
                state_locator: None,
            },
            record: Some(record),
            metadata,
        })
    }

    pub(super) async fn stream_idempotent_replay_unlocked(
        &self,
        input: &AppendStreamRecord,
    ) -> Result<Option<StreamAppendReceipt>> {
        let Some(idempotency_key) = input.idempotency_key.as_deref() else {
            return Ok(None);
        };
        let idempotency_key_hash = format!("sha256:{}", sha256_hex(idempotency_key.as_bytes()));
        self.stream_idempotent_replay_by_hash_unlocked(
            &input.stream_id,
            &input.payload,
            Some(&idempotency_key_hash),
            input.transaction_id.as_deref(),
        )
        .await
    }

    pub(super) async fn stream_idempotent_replay_by_hash_unlocked(
        &self,
        stream_id: &str,
        payload: &[u8],
        idempotency_key_hash: Option<&str>,
        transaction_id: Option<&str>,
    ) -> Result<Option<StreamAppendReceipt>> {
        let Some(idempotency_key_hash) = idempotency_key_hash else {
            return Ok(None);
        };
        let payload_hash = format!("sha256:{}", sha256_hex(payload));
        let head = self.read_stream_head_from_meta(stream_id)?;
        if head
            .as_ref()
            .is_some_and(|head| head.idempotency_index_complete)
        {
            let Some(bytes) = self.meta.get(
                CF_STREAM_RECORDS,
                TABLE_STREAM_IDEMPOTENCY_ROW,
                &stream_idempotency_key(stream_id, idempotency_key_hash),
            )?
            else {
                return Ok(None);
            };
            let existing = decode_stream_idempotency_row(&bytes)?;
            if existing.schema != "anvil.core.stream_idempotency.v1"
                || existing.stream_id != stream_id
                || existing.idempotency_key_hash != idempotency_key_hash
            {
                bail!("CoreStore stream idempotency index row has invalid key scope");
            }
            return self
                .stream_idempotent_receipt_from_idempotency_row_unlocked(
                    stream_id,
                    &payload_hash,
                    idempotency_key_hash,
                    transaction_id,
                    existing,
                )
                .await;
        }
        // Streams created before the direct idempotency index retain the
        // historical lookup path. New streams never pay this scan cost.
        let head_sequence = head.map(|head| head.last_sequence);
        if let Some(sequence) = head_sequence
            && let Some(bytes) = self.meta.get(
                CF_STREAM_RECORDS,
                TABLE_STREAM_RECORD_INDEX_ROW,
                &stream_record_key(stream_id, sequence),
            )?
        {
            let existing = decode_stream_record_index_row(&bytes)?;
            validate_stream_record_index_row_metadata(stream_id, &existing)?;
            if let Some(existing) = StoredStreamIdempotencyRow::from_record_index(&existing) {
                if let Some(receipt) = self
                    .stream_idempotent_receipt_from_idempotency_row_unlocked(
                        stream_id,
                        &payload_hash,
                        idempotency_key_hash,
                        transaction_id,
                        existing,
                    )
                    .await?
                {
                    return Ok(Some(receipt));
                }
            }
        }
        const REPLAY_SCAN_BATCH_SIZE: u64 = 256;
        let mut end_sequence = head_sequence.unwrap_or_default().saturating_sub(1);
        while end_sequence > 0 {
            let start_sequence = end_sequence
                .saturating_sub(REPLAY_SCAN_BATCH_SIZE - 1)
                .max(1);
            for item in self.meta.scan_range_reverse(
                CF_STREAM_RECORDS,
                TABLE_STREAM_RECORD_INDEX_ROW,
                &stream_record_key(stream_id, start_sequence),
                &stream_record_key(stream_id, end_sequence),
                REPLAY_SCAN_BATCH_SIZE as usize,
            )? {
                let existing = decode_stream_record_index_row(&item.payload)?;
                validate_stream_record_index_row_metadata(stream_id, &existing)?;
                if let Some(existing) = StoredStreamIdempotencyRow::from_record_index(&existing) {
                    if let Some(receipt) = self
                        .stream_idempotent_receipt_from_idempotency_row_unlocked(
                            stream_id,
                            &payload_hash,
                            idempotency_key_hash,
                            transaction_id,
                            existing,
                        )
                        .await?
                    {
                        return Ok(Some(receipt));
                    }
                }
            }
            if start_sequence == 1 {
                break;
            }
            end_sequence = start_sequence - 1;
        }
        Ok(None)
    }

    async fn stream_idempotent_receipt_from_idempotency_row_unlocked(
        &self,
        stream_id: &str,
        payload_hash: &str,
        idempotency_key_hash: &str,
        transaction_id: Option<&str>,
        existing: StoredStreamIdempotencyRow,
    ) -> Result<Option<StreamAppendReceipt>> {
        if existing.idempotency_key_hash != idempotency_key_hash {
            return Ok(None);
        }
        if let Some(existing_transaction_id) = existing.transaction_id.as_deref()
            && Some(existing_transaction_id) != transaction_id
        {
            let visible = self
                .read_transaction_unlocked(existing_transaction_id)
                .await?
                .is_some_and(|transaction| {
                    transaction.state == CoreTransactionState::Committed
                        && transaction.visible_updates.iter().any(|visible_update| {
                            matches!(
                                visible_update,
                                CoreTransactionUpdate::StreamAppend {
                                    stream_id: visible_stream_id,
                                    visible_sequence,
                                    prepared_record_hash,
                                } if visible_stream_id == &existing.stream_id
                                    && *visible_sequence == existing.sequence
                                    && prepared_record_hash == &existing.event_hash
                            )
                        })
                });
            if !visible {
                return Ok(None);
            }
        }
        if existing.payload_hash != payload_hash {
            bail!(
                "CoreStore stream idempotency conflict for stream {stream_id}: idempotency_key_hash={idempotency_key_hash}, existing_record_kind={}, existing_payload_hash={}, new_payload_hash={payload_hash}",
                existing.record_kind,
                existing.payload_hash
            );
        }
        Ok(Some(StreamAppendReceipt {
            stream_id: existing.stream_id,
            sequence: existing.sequence,
            cursor: existing.cursor,
            event_hash: existing.event_hash,
            idempotent_replay: true,
        }))
    }

    pub async fn read_stream(&self, input: ReadStream) -> Result<Vec<StreamRecord>> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "read_stream")]);
        validate_logical_id(&input.stream_id, "stream id")?;
        let records = self
            .read_stream_records_after(&input.stream_id, input.after_sequence, input.limit)
            .await?;
        self.filter_committed_stream_records(records).await
    }

    pub async fn read_stream_at_generation(
        &self,
        input: ReadStream,
        root_generation: u64,
    ) -> Result<Vec<StreamRecord>> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "read_stream_at_generation")],
        );
        validate_logical_id(&input.stream_id, "stream id")?;
        if root_generation <= input.after_sequence {
            return Ok(Vec::new());
        }
        let records = self
            .read_stream_records_from_meta_range(
                &input.stream_id,
                input.after_sequence,
                root_generation,
                input.limit,
            )
            .await?;
        self.filter_committed_stream_records(records).await
    }

    pub async fn seal_stream_segment(&self, input: SealStreamSegment) -> Result<CoreSegmentRef> {
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(&input.partition_id, "partition id")?;
        let records = self.read_all_stream_records(&input.stream_id).await?;
        if records.is_empty() {
            bail!(
                "CoreStore stream {} has no records to seal",
                input.stream_id
            );
        }
        let through_sequence = input
            .through_sequence
            .unwrap_or_else(|| records.last().map(|record| record.sequence).unwrap_or(0));
        let selected = records
            .into_iter()
            .filter(|record| record.sequence <= through_sequence)
            .collect::<Vec<_>>();
        if selected.is_empty() {
            bail!(
                "CoreStore stream {} has no records at or before sequence {}",
                input.stream_id,
                through_sequence
            );
        }
        if selected
            .iter()
            .any(|record| record.partition_id != input.partition_id)
        {
            bail!(
                "CoreStore stream {} contains records outside partition {}",
                input.stream_id,
                input.partition_id
            );
        }
        let first_sequence = selected.first().map(|record| record.sequence).unwrap_or(0);
        let last_sequence = selected.last().map(|record| record.sequence).unwrap_or(0);
        let segment_id = format!(
            "seg:{}:{first_sequence:020}:{last_sequence:020}:{}",
            input.stream_id,
            sha256_hex(input.mutation_id.as_bytes())
        );
        let segment_bytes = encode_stream_segment(
            &input,
            &selected,
            &segment_id,
            first_sequence,
            last_sequence,
        )?;
        let logical_file_id = canonical_logical_file_id(
            WriterFamily::Stream,
            1,
            &segment_id,
            &hash32(&segment_bytes),
        );
        let segment_write = self
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: WriterFamily::Stream.as_str().to_string(),
                generation: 1,
                logical_file_id,
                source: segment_bytes,
                range_hints: Vec::new(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: input.mutation_id,
                region_id: "local".to_string(),
            })
            .await?;
        let object_ref = core_object_ref_from_logical_file_write(&segment_write);
        Ok(CoreSegmentRef {
            stream_id: input.stream_id,
            partition_id: input.partition_id,
            first_sequence,
            last_sequence,
            record_count: selected.len() as u64,
            segment_kind: input.segment_kind,
            object_ref,
        })
    }

    pub async fn read_stream_segment(&self, segment: &CoreSegmentRef) -> Result<Vec<StreamRecord>> {
        let bytes = self
            .get_blob(GetBlob {
                object_ref: segment.object_ref.clone(),
            })
            .await?;
        let records = decode_stream_segment(&bytes)?;
        if records.len() as u64 != segment.record_count {
            bail!("CoreStore stream segment record_count mismatch");
        }
        if records
            .first()
            .map(|record| record.sequence)
            .unwrap_or_default()
            != segment.first_sequence
        {
            bail!("CoreStore stream segment first_sequence mismatch");
        }
        if records
            .last()
            .map(|record| record.sequence)
            .unwrap_or_default()
            != segment.last_sequence
        {
            bail!("CoreStore stream segment last_sequence mismatch");
        }
        if records
            .iter()
            .any(|record| record.stream_id != segment.stream_id)
        {
            bail!("CoreStore stream segment stream_id mismatch");
        }
        Ok(records)
    }

    pub async fn watch(&self, input: WatchRequest) -> Result<Vec<WatchEvent>> {
        let stream_ids = self.list_stream_ids(&input.stream_prefix).await?;
        let after_cursor = input.after_cursor.as_deref();
        let mut events = Vec::new();
        for stream_id in stream_ids {
            for record in self
                .filter_committed_stream_records(self.read_all_stream_records(&stream_id).await?)
                .await?
            {
                if after_cursor.is_some_and(|cursor| record.cursor.as_str() <= cursor) {
                    continue;
                }
                events.push(WatchEvent {
                    stream_id: record.stream_id,
                    sequence: record.sequence,
                    cursor: record.cursor,
                    previous_event_hash: record.previous_event_hash,
                    event_hash: record.event_hash,
                    event_type: record.record_kind.clone(),
                    record_kind: record.record_kind,
                    payload_hash: record.payload_hash,
                    transaction_id: record.transaction_id,
                    created_at: record.created_at,
                });
            }
        }
        events.sort_by(|left, right| {
            (left.cursor.as_str(), left.stream_id.as_str(), left.sequence).cmp(&(
                right.cursor.as_str(),
                right.stream_id.as_str(),
                right.sequence,
            ))
        });
        if input.limit > 0 && events.len() > input.limit {
            events.truncate(input.limit);
        }
        Ok(events)
    }

    pub async fn acquire_fence(&self, input: AcquireFence) -> Result<FencedPermit> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "acquire_fence")]);
        validate_logical_id(&input.fence_name, "fence name")?;
        validate_logical_id(
            &input.authenticated_principal,
            "fence authenticated principal",
        )?;
        if input.ttl_ms == 0 {
            bail!("CoreStore fence ttl_ms must be nonzero");
        }
        if input.ttl_ms > MAX_CORE_FENCE_TTL_MS {
            bail!(
                "CoreStore fence ttl_ms {} exceeds maximum {}",
                input.ttl_ms,
                MAX_CORE_FENCE_TTL_MS
            );
        }

        let _guard = self.write_lock.lock().await;
        let row_key = core_fence_row_key(&input.fence_name)?;
        let current = self
            .meta
            .get(CF_LEASES_FENCES, TABLE_CORE_FENCE_ROW, &row_key)?;
        let now_ms = Utc::now().timestamp_millis();
        let current_record = match current {
            Some(bytes) => Some(decode_core_fence_record(&decode_control_current_row(
                &bytes,
                CORE_FENCE_SCHEMA,
            )?)?),
            None => None,
        };
        if let Some(record) = current_record.as_ref() {
            if record.expires_at_ms > now_ms
                && record.owner_principal != input.authenticated_principal
            {
                bail!(
                    "CoreStore fence {} is held by another owner",
                    input.fence_name
                );
            }
        }
        let next_token = current_record
            .as_ref()
            .map(|record| record.fence_token.saturating_add(1))
            .unwrap_or(1);
        let record = CoreFenceRecord {
            schema: CORE_FENCE_SCHEMA.to_string(),
            fence_name: input.fence_name.clone(),
            owner_principal: input.authenticated_principal.clone(),
            fence_token: next_token,
            expires_at_ms: now_ms.saturating_add(input.ttl_ms as i64),
            updated_at: now_rfc3339(),
        };
        let payload = encode_control_current_row(
            "system",
            core_meta_root_key_hash(&format!("core-fence/{}", input.fence_name)),
            next_token,
            format!("core-fence:{}:{next_token}", input.fence_name),
            CORE_FENCE_SCHEMA,
            encode_core_fence_record(&record)?,
        );
        self.commit_coremeta_batch_by_embedded_roots(
            &format!("core-fence:{}:{next_token}", input.fence_name),
            &[CoreMetaBatchOp {
                cf: CF_LEASES_FENCES,
                table_id: TABLE_CORE_FENCE_ROW,
                tuple_key: &row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&payload),
            }],
        )
        .await?;
        Ok(FencedPermit {
            fence_name: record.fence_name,
            owner_principal: record.owner_principal,
            fence_token: record.fence_token,
            expires_at_ms: record.expires_at_ms,
        })
    }

    pub async fn release_fence(&self, input: ReleaseFence) -> Result<()> {
        let _perf_guard =
            crate::perf::guard("anvil_core_store_op", &[("operation", "release_fence")]);
        validate_logical_id(&input.fence_name, "fence name")?;
        validate_logical_id(
            &input.authenticated_principal,
            "fence authenticated principal",
        )?;
        let _guard = self.write_lock.lock().await;
        let row_key = core_fence_row_key(&input.fence_name)?;
        let Some(current) = self
            .meta
            .get(CF_LEASES_FENCES, TABLE_CORE_FENCE_ROW, &row_key)?
        else {
            bail!("CoreStore fence {} is not held", input.fence_name);
        };
        let record =
            decode_core_fence_record(&decode_control_current_row(&current, CORE_FENCE_SCHEMA)?)?;
        if record.owner_principal != input.authenticated_principal
            || record.fence_token != input.fence_token
        {
            bail!(
                "CoreStore fence {} release owner/fence mismatch",
                input.fence_name
            );
        }
        let released = CoreFenceRecord {
            schema: CORE_FENCE_SCHEMA.to_string(),
            fence_name: record.fence_name,
            owner_principal: record.owner_principal,
            fence_token: record.fence_token,
            expires_at_ms: Utc::now().timestamp_millis(),
            updated_at: now_rfc3339(),
        };
        let payload = encode_control_current_row(
            "system",
            core_meta_root_key_hash(&format!("core-fence/{}", input.fence_name)),
            input.fence_token,
            format!(
                "core-fence-release:{}:{}",
                input.fence_name, input.fence_token
            ),
            CORE_FENCE_SCHEMA,
            encode_core_fence_record(&released)?,
        );
        self.commit_coremeta_batch_by_embedded_roots(
            &format!(
                "core-fence-release:{}:{}",
                input.fence_name, input.fence_token
            ),
            &[CoreMetaBatchOp {
                cf: CF_LEASES_FENCES,
                table_id: TABLE_CORE_FENCE_ROW,
                tuple_key: &row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&payload),
            }],
        )
        .await?;
        Ok(())
    }

    pub async fn commit_root_catalog(
        &self,
        mut catalog: CoreRootCatalog,
        signing_key: &[u8],
    ) -> Result<CoreRootCatalogReceipt> {
        validate_logical_id(&catalog.mesh_id, "mesh id")?;
        validate_logical_id(&catalog.signed_by, "root catalog signer")?;
        if catalog.schema != CORE_ROOT_CATALOG_SCHEMA {
            bail!("CoreStore root catalog has invalid schema");
        }
        if catalog.root_partitions.is_empty() {
            bail!("CoreStore root catalog must include root partitions");
        }
        let current = self
            .read_latest_root_catalog(&catalog.mesh_id, signing_key)
            .await?;
        match current.as_ref() {
            Some(current) => {
                if catalog.generation <= current.generation {
                    bail!(
                        "CoreStore root catalog generation {} is not newer than current {}",
                        catalog.generation,
                        current.generation
                    );
                }
                let current_hash = hash_root_catalog(current)?;
                if catalog.previous_hash != current_hash {
                    bail!("CoreStore root catalog previous_hash does not match current catalog");
                }
            }
            None => {
                if catalog.generation == 0 {
                    bail!("CoreStore root catalog generation must be nonzero");
                }
                if catalog.previous_hash != ZERO_HASH {
                    bail!("CoreStore genesis root catalog must use the zero previous_hash");
                }
            }
        }
        for partition in &catalog.root_partitions {
            validate_root_partition(partition)?;
            self.verify_embedded_manifest_readable(&partition.embedded_head_segment_manifest)
                .await?;
        }
        catalog.signature = String::new();
        catalog.signature = sign_root_catalog(signing_key, &catalog)?;
        verify_root_catalog(&catalog, signing_key)?;
        let catalog_hash = hash_root_catalog(&catalog)?;
        let catalog_payload = encode_root_catalog_record(&catalog)?;
        let catalog_row_payload = encode_control_current_row(
            format!("mesh/{}", catalog.mesh_id),
            core_meta_root_key_hash(&format!("root-catalog/{}", catalog.mesh_id)),
            catalog.generation,
            format!("root-catalog:{}:{}", catalog.mesh_id, catalog.generation),
            CORE_ROOT_CATALOG_SCHEMA,
            catalog_payload.clone(),
        );
        let catalog_row_key = root_catalog_row_key(&catalog.mesh_id)?;
        self.commit_coremeta_batch_by_embedded_roots(
            &format!("root-catalog:{}:{}", catalog.mesh_id, catalog.generation),
            &[CoreMetaBatchOp {
                cf: CF_MESH,
                table_id: TABLE_ROOT_CATALOG_CURRENT_ROW,
                tuple_key: &catalog_row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&catalog_row_payload),
            }],
        )
        .await?;
        let watch = self
            .append_stream(AppendStreamRecord {
                stream_id: root_catalog_stream_id(&catalog.mesh_id),
                partition_id: "core.root.catalog".to_string(),
                record_kind: "root_catalog.committed".to_string(),
                payload: encode_root_catalog_record(&catalog)?,
                content_type: Some("application/x.anvil.root-catalog".to_string()),
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some(format!(
                    "root-catalog:{}:{}",
                    catalog.mesh_id, catalog.generation
                )),
            })
            .await?;
        Ok(CoreRootCatalogReceipt {
            mesh_id: catalog.mesh_id,
            generation: catalog.generation,
            catalog_hash,
            row_generation: catalog.generation,
            watch_cursor: watch.cursor,
        })
    }

    pub async fn read_latest_root_catalog(
        &self,
        mesh_id: &str,
        signing_key: &[u8],
    ) -> Result<Option<CoreRootCatalog>> {
        validate_logical_id(mesh_id, "mesh id")?;
        let Some(bytes) = self.meta.get(
            CF_MESH,
            TABLE_ROOT_CATALOG_CURRENT_ROW,
            &root_catalog_row_key(mesh_id)?,
        )?
        else {
            return Ok(None);
        };
        let catalog = decode_root_catalog_record(&decode_control_current_row(
            &bytes,
            CORE_ROOT_CATALOG_SCHEMA,
        )?)?;
        verify_root_catalog(&catalog, signing_key)?;
        if catalog.mesh_id != mesh_id {
            bail!("CoreStore root catalog mesh id mismatch");
        }
        Ok(Some(catalog))
    }

    pub async fn list_root_catalog_history(&self, mesh_id: &str) -> Result<Vec<CoreRootCatalog>> {
        validate_logical_id(mesh_id, "mesh id")?;
        let records = self
            .read_stream(ReadStream {
                stream_id: root_catalog_stream_id(mesh_id),
                after_sequence: 0,
                limit: 0,
            })
            .await?;
        let mut catalogs = Vec::new();
        for record in records {
            catalogs.push(decode_root_catalog_record(&record.payload)?);
        }
        Ok(catalogs)
    }

    pub async fn commit_quorum_profile(
        &self,
        profile: CoreQuorumProfile,
    ) -> Result<CoreQuorumProfileReceipt> {
        validate_quorum_profile(&profile)?;
        let current = self
            .read_latest_quorum_profile(&profile.placement_group)
            .await?;
        match current.as_ref() {
            Some(current) => {
                if profile.epoch != current.epoch.saturating_add(1) {
                    bail!(
                        "CoreStore quorum profile epoch {} must immediately follow current epoch {}",
                        profile.epoch,
                        current.epoch
                    );
                }
            }
            None => {
                if profile.epoch == 0 {
                    bail!("CoreStore quorum profile genesis epoch must be nonzero");
                }
            }
        }

        let profile_bytes = encode_quorum_profile_record(&profile)?;
        let profile_hash = format!("sha256:{}", sha256_hex(&profile_bytes));
        let profile_row_payload = encode_control_current_row(
            format!("placement-group/{}", profile.placement_group),
            core_meta_root_key_hash(&format!("quorum-profile/{}", profile.placement_group)),
            profile.epoch,
            format!(
                "quorum-profile:{}:{}",
                profile.placement_group, profile.epoch
            ),
            CORE_QUORUM_PROFILE_SCHEMA,
            profile_bytes.clone(),
        );
        let profile_row_key = quorum_profile_row_key(&profile.placement_group)?;
        self.commit_coremeta_batch_by_embedded_roots(
            &format!(
                "quorum-profile:{}:{}",
                profile.placement_group, profile.epoch
            ),
            &[CoreMetaBatchOp {
                cf: CF_MESH,
                table_id: TABLE_QUORUM_PROFILE_CURRENT_ROW,
                tuple_key: &profile_row_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&profile_row_payload),
            }],
        )
        .await?;
        let watch = self
            .append_stream(AppendStreamRecord {
                stream_id: quorum_profile_stream_id(&profile.placement_group),
                partition_id: "core.quorum.profile".to_string(),
                record_kind: "quorum_profile.committed".to_string(),
                payload: encode_quorum_profile_record(&profile)?,
                content_type: Some("application/x.anvil.quorum-profile".to_string()),
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: None,
                idempotency_key: Some(format!(
                    "quorum-profile:{}:{}",
                    profile.placement_group, profile.epoch
                )),
            })
            .await?;

        Ok(CoreQuorumProfileReceipt {
            placement_group: profile.placement_group,
            epoch: profile.epoch,
            profile_hash,
            row_generation: profile.epoch,
            watch_cursor: watch.cursor,
        })
    }

    pub async fn read_latest_quorum_profile(
        &self,
        placement_group: &str,
    ) -> Result<Option<CoreQuorumProfile>> {
        validate_logical_id(placement_group, "placement group")?;
        let Some(bytes) = self.meta.get(
            CF_MESH,
            TABLE_QUORUM_PROFILE_CURRENT_ROW,
            &quorum_profile_row_key(placement_group)?,
        )?
        else {
            return Ok(None);
        };
        let profile = decode_quorum_profile_record(&decode_control_current_row(
            &bytes,
            CORE_QUORUM_PROFILE_SCHEMA,
        )?)?;
        validate_quorum_profile(&profile)?;
        if profile.placement_group != placement_group {
            bail!("CoreStore quorum profile placement group mismatch");
        }
        Ok(Some(profile))
    }

    pub async fn list_quorum_profile_history(
        &self,
        placement_group: &str,
    ) -> Result<Vec<CoreQuorumProfile>> {
        validate_logical_id(placement_group, "placement group")?;
        let records = self
            .read_stream(ReadStream {
                stream_id: quorum_profile_stream_id(placement_group),
                after_sequence: 0,
                limit: 0,
            })
            .await?;
        let mut profiles = Vec::new();
        for record in records {
            let profile = decode_quorum_profile_record(&record.payload)?;
            validate_quorum_profile(&profile)?;
            if profile.placement_group != placement_group {
                bail!("CoreStore quorum profile stream scope mismatch");
            }
            profiles.push(profile);
        }
        Ok(profiles)
    }
}

pub(super) fn read_core_fence_current_row(
    store: &CoreStore,
    fence_name: &str,
) -> Result<Option<CoreFenceRecord>> {
    let row_key = core_fence_row_key(fence_name)?;
    let Some(bytes) = store
        .meta
        .get(CF_LEASES_FENCES, TABLE_CORE_FENCE_ROW, &row_key)?
    else {
        return Ok(None);
    };
    Ok(Some(decode_core_fence_record(
        &decode_control_current_row(&bytes, CORE_FENCE_SCHEMA)?,
    )?))
}

pub(super) fn core_fence_row_key(fence_name: &str) -> Result<Vec<u8>> {
    validate_logical_id(fence_name, "fence name")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("core-fence"),
        CoreMetaTuplePart::Utf8(fence_name),
    ])
}

fn root_catalog_row_key(mesh_id: &str) -> Result<Vec<u8>> {
    validate_logical_id(mesh_id, "mesh id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("root-catalog"),
        CoreMetaTuplePart::Utf8(mesh_id),
    ])
}

fn quorum_profile_row_key(placement_group: &str) -> Result<Vec<u8>> {
    validate_logical_id(placement_group, "placement group")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("quorum-profile"),
        CoreMetaTuplePart::Utf8(placement_group),
    ])
}

fn encode_control_current_row(
    realm_id: impl Into<String>,
    root_key_hash: String,
    root_generation: u64,
    transaction_id: impl Into<String>,
    schema: &'static str,
    payload: Vec<u8>,
) -> Vec<u8> {
    let payload_hash = format!("sha256:{}", sha256_hex(&payload));
    CoreControlCurrentRowProto {
        common: Some(core_meta_committed_row_common(
            realm_id.into(),
            root_key_hash,
            root_generation,
            transaction_id.into(),
            unix_timestamp_nanos(),
        )),
        schema: schema.to_string(),
        payload,
        payload_hash,
    }
    .encode_to_vec()
}

fn decode_control_current_row(bytes: &[u8], schema: &'static str) -> Result<Vec<u8>> {
    let row = CoreControlCurrentRowProto::decode(bytes)?;
    if row.schema != schema {
        bail!("CoreStore control current row schema mismatch");
    }
    if row.payload_hash != format!("sha256:{}", sha256_hex(&row.payload)) {
        bail!("CoreStore control current row payload hash mismatch");
    }
    Ok(row.payload)
}

fn boundary_schema_coremeta_prefix(bucket: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[CoreMetaTuplePart::Utf8(bucket)])
}

fn boundary_schema_coremeta_key(bucket: &str, generation: u64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(bucket),
        CoreMetaTuplePart::U64(generation),
    ])
}

fn boundary_value_coremeta_key(
    bucket: &str,
    value: &CoreBoundaryValue,
    object_ref: &str,
    range_ref: &str,
) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8(bucket),
        CoreMetaTuplePart::Utf8(&value.name),
        CoreMetaTuplePart::Utf8(&value.value),
        CoreMetaTuplePart::Utf8(object_ref),
        CoreMetaTuplePart::Utf8(range_ref),
    ])
}
