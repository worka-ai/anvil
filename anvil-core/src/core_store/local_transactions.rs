use super::local_stream_control::control_record_proto::{
    decode_stream_head_record, decode_stream_record_index_row,
};
use super::local_tx_rows::{OwnedCoreMetaBatchOp, borrow_owned_coremeta_batch_ops};
use super::*;
use crate::formats::writer::WriterFamily;

fn insert_coremeta_root_lock_from_payload(
    lock_keys: &mut BTreeSet<(String, String)>,
    payload: &[u8],
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    if !common.root_key_hash.is_empty() {
        lock_keys.insert(("coremeta-root".to_string(), common.root_key_hash));
    }
    Ok(())
}

impl CoreStore {
    async fn acquire_batch_locks(&self, batch: &CoreMutationBatch) -> Result<Vec<CoreStoreLock>> {
        let mut acquired_keys = BTreeSet::new();
        for _ in 0..CORE_PROCESS_LOCK_RETRY_ATTEMPTS {
            let lock_keys = self.batch_lock_keys(batch)?;
            let mut guards = Vec::with_capacity(lock_keys.len());
            for (kind, id) in &lock_keys {
                guards.push(self.acquire_named_lock(kind, id).await?);
            }

            // Deletions discover their root from the current row. Recompute while
            // row locks are held so a concurrent writer cannot make us miss a
            // root lock; if the required set grew, reacquire everything in the
            // global sorted order to avoid deadlocks.
            let stable_lock_keys = self.batch_lock_keys(batch)?;
            if stable_lock_keys.is_subset(&lock_keys) {
                return Ok(guards);
            }
            acquired_keys = stable_lock_keys;
        }

        bail!(
            "CoreStore mutation batch locks changed too often while acquiring: {:?}",
            acquired_keys
        )
    }

    fn batch_lock_keys(&self, batch: &CoreMutationBatch) -> Result<BTreeSet<(String, String)>> {
        let mut lock_keys = BTreeSet::new();
        lock_keys.insert(("transaction".to_string(), batch.transaction_id.clone()));
        for precondition in &batch.preconditions {
            match precondition {
                CoreMutationPrecondition::Fence { fence_name, .. } => {
                    lock_keys.insert(("fence".to_string(), fence_name.clone()));
                }
                CoreMutationPrecondition::CoreMetaRow {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                    if let Some(payload) = self.read_coremeta_row(cf, *table_id, tuple_key)? {
                        insert_coremeta_root_lock_from_payload(&mut lock_keys, &payload)?;
                    }
                }
                CoreMutationPrecondition::StreamHead { stream_id, .. } => {
                    lock_keys.insert(("stream".to_string(), stream_id.clone()));
                }
            }
        }
        for operation in &batch.operations {
            match operation {
                CoreMutationOperation::StreamAppend { stream_id, .. } => {
                    lock_keys.insert(("stream".to_string(), stream_id.clone()));
                }
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                    insert_coremeta_root_lock_from_payload(&mut lock_keys, payload)?;
                }
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    Self::insert_coremeta_row_lock(&mut lock_keys, cf, *table_id, tuple_key);
                    if let Some(payload) = self.read_coremeta_row(cf, *table_id, tuple_key)? {
                        insert_coremeta_root_lock_from_payload(&mut lock_keys, &payload)?;
                    }
                }
            }
        }
        Ok(lock_keys)
    }

    fn insert_coremeta_row_lock(
        lock_keys: &mut BTreeSet<(String, String)>,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) {
        lock_keys.insert((
            "coremeta-row".to_string(),
            format!("{cf}:{table_id}:{}", sha256_hex(tuple_key)),
        ));
    }

    pub async fn list_stream_ids(&self, prefix: &str) -> Result<Vec<String>> {
        let mut ids = BTreeSet::new();
        for item in self.meta.scan_prefix(
            CF_STREAM_HEADS,
            TABLE_STREAM_HEAD_ROW,
            &meta_tuple_key(&[b"stream-head"]),
        )? {
            let head = decode_stream_head_record(&item.payload)?;
            if head.schema != "anvil.core.stream_head.v1" {
                bail!("CoreStore stream head metadata row has invalid schema");
            }
            if head.stream_id.starts_with(prefix) && head.record_count > 0 {
                ids.insert(head.stream_id);
            }
        }
        Ok(ids.into_iter().collect())
    }

    pub async fn commit_mutation_batch(
        &self,
        batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        let _perf_guard = crate::perf::guard(
            "anvil_core_store_op",
            &[("operation", "commit_mutation_batch")],
        );
        let total_start = std::time::Instant::now();
        let timing_name = batch.transaction_id.clone();
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore mutation batch must include at least one operation");
        }
        validate_batch_partitions(&batch)?;

        let step_start = std::time::Instant::now();
        let _operation_guards = self.acquire_batch_locks(&batch).await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch acquire_batch_locks tx={timing_name}"),
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        if let Some(transaction) = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
        {
            if matches!(
                transaction.state,
                CoreTransactionState::Committed | CoreTransactionState::FinalisationFailed
            ) {
                return Ok(receipt_from_transaction(&transaction));
            }
            bail!(
                "CoreStore transaction {} already exists with non-implicit state {}",
                batch.transaction_id,
                core_transaction_state_name(transaction.state)
            );
        }
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch read_transaction tx={timing_name}"),
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        self.validate_mutation_preconditions_unlocked(
            &batch.preconditions,
            &batch.committed_by_principal,
            None,
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch validate_preconditions tx={timing_name}"),
            step_start.elapsed(),
        );
        let batch_payload = encode_core_mutation_batch(&batch)?;
        let pending_mutation_payload =
            if batch_payload.len() <= CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
                CorePendingMutationPayload::Inline(&batch_payload)
            } else {
                CorePendingMutationPayload::Landed(&batch_payload)
            };
        let step_start = std::time::Instant::now();
        let admission = self
            .admit_core_mutation(
                "mutation.batch",
                WriterFamily::CoreControl.as_str(),
                CorePendingMutationTarget::MutationBatch {
                    transaction_id: batch.transaction_id.clone(),
                    scope_partition: batch.scope_partition.clone(),
                    operation_count: batch.operations.len() as u64,
                },
                batch.transaction_id.clone(),
                Some(batch.transaction_id.clone()),
                pending_mutation_payload,
                Vec::new(),
            )
            .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch admission tx={timing_name}"),
            step_start.elapsed(),
        );

        let step_start = std::time::Instant::now();
        let mut prepared_coremeta_ops = Vec::new();
        let (visible_updates, finalisation_error) = match self
            .prepare_mutation_batch_operations_unlocked(&batch)
            .await
        {
            Some(Ok((ops, updates))) => {
                prepared_coremeta_ops = ops;
                (updates, None)
            }
            Some(Err(error)) => (Vec::new(), Some(format!("{error:#}"))),
            None => self.apply_mutation_batch_operations_unlocked(&batch).await,
        };
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch operations tx={timing_name}"),
            step_start.elapsed(),
        );

        let transaction_state = if finalisation_error.is_some() {
            CoreTransactionState::FinalisationFailed
        } else {
            CoreTransactionState::Committed
        };
        let transaction_visible_updates = if finalisation_error.is_some() {
            Vec::new()
        } else {
            visible_updates.clone()
        };
        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: transaction_state,
            preconditions_hash: core_mutation_preconditions_hash(&batch.preconditions)?,
            operations_hash: core_mutation_operations_hash(&batch.operations)?,
            visible_updates: transaction_visible_updates.clone(),
            finalisation_error: finalisation_error.clone(),
            committed_at: now_rfc3339(),
            committed_by_principal: batch.committed_by_principal.clone(),
            created_at_unix_nanos: current_unix_nanos_u64()?,
            expires_at_unix_nanos: 0,
            root_anchor_key: batch.scope_partition.clone(),
            root_key_hash: root_key_hash(&batch.scope_partition),
            committed_root_generation: if transaction_state == CoreTransactionState::Committed {
                committed_root_generation_from_updates(&transaction_visible_updates)?
            } else {
                None
            },
            purpose: "implicit_mutation_batch".to_string(),
            failure_evidence: finalisation_error.clone(),
            outcome: core_transaction_state_name(transaction_state).to_string(),
        };
        let step_start = std::time::Instant::now();
        let transaction_ops = self
            .transaction_rows_as_coremeta_ops_unlocked(&transaction, &batch.preconditions)
            .await?;
        prepared_coremeta_ops.extend(transaction_ops);
        self.mark_pending_mutation_finalised_with_result_and_ops_unlocked(
            &admission,
            core_transaction_state_name(transaction_state),
            None,
            prepared_coremeta_ops,
        )
        .await?;
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch write_transaction tx={timing_name}"),
            step_start.elapsed(),
        );
        crate::emit_test_timing(
            format!("core_store.commit_mutation_batch total tx={timing_name}"),
            total_start.elapsed(),
        );

        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id,
            scope_partition: batch.scope_partition,
            state: transaction_state,
            visible_updates: transaction_visible_updates,
            finalisation_error,
        })
    }

    pub(super) async fn recover_admitted_mutation_batch_unlocked(
        &self,
        batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore mutation batch must include at least one operation");
        }
        validate_batch_partitions(&batch)?;

        if let Some(transaction) = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
        {
            if matches!(
                transaction.state,
                CoreTransactionState::Committed | CoreTransactionState::FinalisationFailed
            ) {
                return Ok(receipt_from_transaction(&transaction));
            }
            bail!(
                "CoreStore transaction {} already exists with non-implicit state {}",
                batch.transaction_id,
                core_transaction_state_name(transaction.state)
            );
        }
        let recovered_updates = self
            .inspect_applied_mutation_batch_updates_unlocked(&batch)
            .await?;
        let recovered_count = recovered_updates
            .iter()
            .filter(|update| update.is_some())
            .count();
        let (visible_updates, finalisation_error) = if recovered_count == batch.operations.len() {
            (
                recovered_updates
                    .into_iter()
                    .map(|update| update.expect("all mutation operations were recovered"))
                    .collect(),
                None,
            )
        } else if recovered_count > 0 {
            (
                self.complete_partially_applied_mutation_batch_unlocked(&batch, recovered_updates)
                    .await?,
                None,
            )
        } else {
            self.validate_mutation_preconditions_unlocked(
                &batch.preconditions,
                &batch.committed_by_principal,
                None,
            )
            .await?;
            self.apply_mutation_batch_operations_unlocked(&batch).await
        };

        let transaction_state = if finalisation_error.is_some() {
            CoreTransactionState::FinalisationFailed
        } else {
            CoreTransactionState::Committed
        };
        let transaction_visible_updates = if finalisation_error.is_some() {
            Vec::new()
        } else {
            visible_updates.clone()
        };
        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            state: transaction_state,
            preconditions_hash: core_mutation_preconditions_hash(&batch.preconditions)?,
            operations_hash: core_mutation_operations_hash(&batch.operations)?,
            visible_updates: transaction_visible_updates.clone(),
            finalisation_error: finalisation_error.clone(),
            committed_at: now_rfc3339(),
            committed_by_principal: batch.committed_by_principal.clone(),
            created_at_unix_nanos: current_unix_nanos_u64()?,
            expires_at_unix_nanos: 0,
            root_anchor_key: batch.scope_partition.clone(),
            root_key_hash: root_key_hash(&batch.scope_partition),
            committed_root_generation: if transaction_state == CoreTransactionState::Committed {
                committed_root_generation_from_updates(&transaction_visible_updates)?
            } else {
                None
            },
            purpose: "implicit_mutation_batch_recovery".to_string(),
            failure_evidence: finalisation_error.clone(),
            outcome: core_transaction_state_name(transaction_state).to_string(),
        };
        self.write_transaction_with_staged_rows_unlocked(&transaction, &batch.preconditions)
            .await?;

        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id,
            scope_partition: batch.scope_partition,
            state: transaction_state,
            visible_updates: transaction_visible_updates,
            finalisation_error,
        })
    }

    async fn inspect_applied_mutation_batch_updates_unlocked(
        &self,
        batch: &CoreMutationBatch,
    ) -> Result<Vec<Option<CoreTransactionUpdate>>> {
        let mut updates = Vec::with_capacity(batch.operations.len());
        for operation in &batch.operations {
            match operation {
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => {
                    let idempotency_key_hash = idempotency_key
                        .as_deref()
                        .map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
                    let receipt = self
                        .recover_stream_append_receipt_unlocked(
                            stream_id,
                            payload,
                            idempotency_key_hash.as_deref(),
                            Some(&batch.transaction_id),
                        )
                        .await?;
                    let Some(receipt) = receipt else {
                        updates.push(None);
                        continue;
                    };
                    let record = self
                        .read_stream_record_from_meta(stream_id, receipt.sequence)
                        .await?
                        .ok_or_else(|| {
                            anyhow!(
                                "CoreStore recovered stream record {}:{} is missing",
                                stream_id,
                                receipt.sequence
                            )
                        })?;
                    if record.partition_id != *partition_id
                        || record.record_kind != *record_kind
                        || record.transaction_id.as_deref() != Some(batch.transaction_id.as_str())
                    {
                        bail!(
                            "CoreStore recovered stream operation identity mismatch for transaction {} stream {} sequence {}",
                            batch.transaction_id,
                            stream_id,
                            receipt.sequence
                        );
                    }
                    updates.push(Some(CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: receipt.sequence,
                        prepared_record_hash: receipt.event_hash,
                    }));
                }
                CoreMutationOperation::CoreMetaDelete { .. } => updates.push(None),
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    let canonical_cf = canonical_coremeta_cf_name(cf)?;
                    let Some(current_payload) =
                        self.read_coremeta_row(canonical_cf, *table_id, tuple_key)?
                    else {
                        updates.push(None);
                        continue;
                    };
                    if current_payload.as_slice() != payload.as_slice() {
                        updates.push(None);
                        continue;
                    }
                    let common = core_meta_row_common_from_payload(&current_payload)?;
                    if common.transaction_id != batch.transaction_id {
                        bail!(
                            "CoreStore recovered CoreMeta operation identity mismatch for transaction {} row {}/{}",
                            batch.transaction_id,
                            canonical_cf,
                            table_id
                        );
                    }
                    let Some(previous_payload_hash) = recovered_coremeta_previous_payload_hash(
                        batch,
                        canonical_cf,
                        *table_id,
                        tuple_key,
                    )?
                    else {
                        updates.push(None);
                        continue;
                    };
                    updates.push(Some(CoreTransactionUpdate::CoreMetaPut {
                        cf: canonical_cf.to_string(),
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        previous_payload_hash,
                        payload: payload.clone(),
                        payload_hash: core_meta_payload_digest(*table_id, payload),
                    }));
                }
            }
        }
        Ok(updates)
    }

    async fn complete_partially_applied_mutation_batch_unlocked(
        &self,
        batch: &CoreMutationBatch,
        recovered_updates: Vec<Option<CoreTransactionUpdate>>,
    ) -> Result<Vec<CoreTransactionUpdate>> {
        let missing_operations = batch
            .operations
            .iter()
            .zip(&recovered_updates)
            .filter_map(|(operation, update)| update.is_none().then(|| operation.clone()))
            .collect::<Vec<_>>();
        let mut remaining_preconditions = Vec::new();
        for precondition in &batch.preconditions {
            if !precondition_is_satisfied_by_recovered_operation(
                precondition,
                &batch.operations,
                &recovered_updates,
            )? {
                remaining_preconditions.push(precondition.clone());
            }
        }
        self.validate_mutation_preconditions_unlocked(
            &remaining_preconditions,
            &batch.committed_by_principal,
            None,
        )
        .await?;

        let missing_batch = CoreMutationBatch {
            transaction_id: batch.transaction_id.clone(),
            scope_partition: batch.scope_partition.clone(),
            committed_by_principal: batch.committed_by_principal.clone(),
            preconditions: remaining_preconditions,
            operations: missing_operations,
        };
        let (new_updates, finalisation_error) = self
            .apply_mutation_batch_operations_unlocked(&missing_batch)
            .await;
        if let Some(error) = finalisation_error {
            bail!(
                "CoreStore could not complete partially applied mutation batch {}: {error}",
                batch.transaction_id
            );
        }
        combine_recovered_mutation_updates(recovered_updates, new_updates)
    }

    async fn recover_stream_append_receipt_unlocked(
        &self,
        stream_id: &str,
        payload: &[u8],
        idempotency_key_hash: Option<&str>,
        transaction_id: Option<&str>,
    ) -> Result<Option<StreamAppendReceipt>> {
        if let Some(idempotency_key_hash) = idempotency_key_hash {
            return self
                .stream_idempotent_replay_by_hash_unlocked(
                    stream_id,
                    payload,
                    Some(idempotency_key_hash),
                    transaction_id,
                )
                .await;
        }
        let Some(head) = self.read_stream_head_from_meta(stream_id)? else {
            return Ok(None);
        };
        let payload_hash = format!("sha256:{}", sha256_hex(payload));
        // Non-idempotent pending writes are identified by their transaction.
        // Search from the tail because they are normally the latest record.
        for sequence in (1..=head.last_sequence).rev() {
            let bytes = self
                .meta
                .get(
                    CF_STREAM_RECORDS,
                    TABLE_STREAM_RECORD_INDEX_ROW,
                    &stream_record_key(stream_id, sequence),
                )?
                .ok_or_else(|| {
                    anyhow!("CoreStore stream {stream_id} is missing record {sequence}")
                })?;
            let existing = decode_stream_record_index_row(&bytes)?;
            validate_stream_record_index_row_metadata(stream_id, &existing)?;
            if existing.transaction_id.as_deref() != transaction_id {
                continue;
            }
            if existing.payload_hash != payload_hash {
                bail!(
                    "CoreStore recovered stream payload mismatch for stream {stream_id} transaction {}",
                    transaction_id.unwrap_or("<none>")
                );
            }
            return Ok(Some(StreamAppendReceipt {
                stream_id: existing.stream_id,
                sequence: existing.sequence,
                cursor: existing.cursor,
                event_hash: existing.event_hash,
                idempotent_replay: true,
            }));
        }
        Ok(None)
    }

    async fn apply_mutation_batch_operations_unlocked(
        &self,
        batch: &CoreMutationBatch,
    ) -> (Vec<CoreTransactionUpdate>, Option<String>) {
        let mut visible_updates = Vec::with_capacity(batch.operations.len());
        let mut pending_coremeta_ops = Vec::new();
        let mut pending_coremeta_updates = Vec::new();

        for operation in &batch.operations {
            let operation_result = match operation {
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => {
                    if let Err(error) = self
                        .flush_coremeta_mutation_run_unlocked(
                            &batch.transaction_id,
                            &mut pending_coremeta_ops,
                            &mut pending_coremeta_updates,
                            &mut visible_updates,
                        )
                        .await
                    {
                        return (visible_updates, Some(format!("{error:#}")));
                    }
                    self.append_stream_unlocked(AppendStreamRecord {
                        stream_id: stream_id.clone(),
                        partition_id: partition_id.clone(),
                        record_kind: record_kind.clone(),
                        payload: payload.clone(),
                        content_type: None,
                        user_metadata_json: "{}".to_string(),
                        fence: None,
                        transaction_id: Some(batch.transaction_id.clone()),
                        idempotency_key: idempotency_key.clone(),
                    })
                    .await
                    .map(|outcome| CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: outcome.receipt.sequence,
                        prepared_record_hash: outcome.receipt.event_hash,
                    })
                }
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    match self
                        .prepare_coremeta_put_update_unlocked(cf, *table_id, tuple_key, payload)
                    {
                        Ok((op, update)) => {
                            pending_coremeta_ops.push(op);
                            pending_coremeta_updates.push(update);
                            continue;
                        }
                        Err(error) => Err(error),
                    }
                }
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let row_transaction_id = format!("coremeta-delete:{}", sha256_hex(tuple_key));
                    match self.prepare_coremeta_delete_update_unlocked(
                        cf,
                        *table_id,
                        tuple_key,
                        row_transaction_id,
                    ) {
                        Ok((op, update)) => {
                            pending_coremeta_ops.push(op);
                            pending_coremeta_updates.push(update);
                            continue;
                        }
                        Err(error) => Err(error),
                    }
                }
            };
            match operation_result {
                Ok(update) => visible_updates.push(update),
                Err(error) => return (visible_updates, Some(format!("{error:#}"))),
            }
        }

        if let Err(error) = self
            .flush_coremeta_mutation_run_unlocked(
                &batch.transaction_id,
                &mut pending_coremeta_ops,
                &mut pending_coremeta_updates,
                &mut visible_updates,
            )
            .await
        {
            return (visible_updates, Some(format!("{error:#}")));
        }

        (visible_updates, None)
    }

    async fn prepare_mutation_batch_operations_unlocked(
        &self,
        batch: &CoreMutationBatch,
    ) -> Option<Result<(Vec<OwnedCoreMetaBatchOp>, Vec<CoreTransactionUpdate>)>> {
        let mut stream_ids = BTreeSet::new();
        for operation in &batch.operations {
            if let CoreMutationOperation::StreamAppend { stream_id, .. } = operation
                && (stream_id == CORE_TRANSACTION_STREAM_ID
                    || !stream_ids.insert(stream_id.as_str()))
            {
                return None;
            }
        }

        let mut owned_ops = Vec::new();
        let mut updates = Vec::with_capacity(batch.operations.len());
        for operation in &batch.operations {
            let prepared = match operation {
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => self
                    .prepare_coremeta_put_update_unlocked(cf, *table_id, tuple_key, payload)
                    .map(|(op, update)| (vec![op], update)),
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => self
                    .prepare_coremeta_delete_update_unlocked(
                        cf,
                        *table_id,
                        tuple_key,
                        format!("coremeta-delete:{}", sha256_hex(tuple_key)),
                    )
                    .map(|(op, update)| (vec![op], update)),
                CoreMutationOperation::StreamAppend {
                    stream_id,
                    partition_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => {
                    let idempotency_key_hash = idempotency_key
                        .as_deref()
                        .map(|key| format!("sha256:{}", sha256_hex(key.as_bytes())));
                    self.prepare_stream_append_unlocked_with_idempotency_hash(
                        AppendStreamRecord {
                            stream_id: stream_id.clone(),
                            partition_id: partition_id.clone(),
                            record_kind: record_kind.clone(),
                            payload: payload.clone(),
                            content_type: None,
                            user_metadata_json: "{}".to_string(),
                            fence: None,
                            transaction_id: Some(batch.transaction_id.clone()),
                            idempotency_key: idempotency_key.clone(),
                        },
                        idempotency_key_hash,
                    )
                    .await
                    .map(|prepared| {
                        let receipt = prepared.outcome.receipt;
                        (
                            prepared.metadata.owned_ops,
                            CoreTransactionUpdate::StreamAppend {
                                stream_id: stream_id.clone(),
                                visible_sequence: receipt.sequence,
                                prepared_record_hash: receipt.event_hash,
                            },
                        )
                    })
                }
            };
            match prepared {
                Ok((mut ops, update)) => {
                    owned_ops.append(&mut ops);
                    updates.push(update);
                }
                Err(error) => return Some(Err(error)),
            }
        }
        Some(Ok((owned_ops, updates)))
    }

    async fn flush_coremeta_mutation_run_unlocked(
        &self,
        transaction_id: &str,
        pending_coremeta_ops: &mut Vec<OwnedCoreMetaBatchOp>,
        pending_coremeta_updates: &mut Vec<CoreTransactionUpdate>,
        visible_updates: &mut Vec<CoreTransactionUpdate>,
    ) -> Result<()> {
        if pending_coremeta_ops.is_empty() {
            return Ok(());
        }
        let ops = borrow_owned_coremeta_batch_ops(pending_coremeta_ops);
        self.commit_coremeta_batch_by_embedded_roots(transaction_id, &ops)
            .await?;
        visible_updates.append(pending_coremeta_updates);
        pending_coremeta_ops.clear();
        Ok(())
    }

    pub async fn read_transaction(&self, transaction_id: &str) -> Result<Option<CoreTransaction>> {
        validate_logical_id(transaction_id, "transaction id")?;
        self.read_transaction_unlocked(transaction_id).await
    }

    pub async fn read_explicit_transaction_for_principal(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<CoreTransaction> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let _guard = self.write_lock.lock().await;
        let transaction = self
            .read_transaction_unlocked(transaction_id)
            .await?
            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
        if transaction.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        if transaction.state == CoreTransactionState::Open
            && transaction.expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= transaction.expires_at_unix_nanos
        {
            let expired = transaction_with_state(
                transaction,
                CoreTransactionState::Expired,
                Some("TransactionExpired".to_string()),
            )?;
            self.write_transaction_unlocked(&expired).await?;
            return Ok(expired);
        }
        Ok(transaction)
    }

    pub fn root_key_hash_for_anchor(root_anchor_key: &str) -> String {
        root_key_hash(root_anchor_key)
    }

    pub(crate) async fn infer_explicit_transaction_commit_root_generation(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<u64> {
        let _guard = self.write_lock.lock().await;
        self.infer_explicit_transaction_commit_root_generation_unlocked(transaction)
            .await
    }

    pub async fn begin_explicit_transaction(
        &self,
        input: CoreBeginTransaction,
    ) -> Result<CoreTransaction> {
        validate_logical_id(&input.idempotency_key, "transaction idempotency key")?;
        validate_logical_id(&input.root_anchor_key, "transaction root anchor key")?;
        validate_hash(&input.root_key_hash, "transaction root key hash")?;
        validate_logical_id(&input.scope_partition, "transaction scope partition")?;
        validate_logical_id(&input.principal, "transaction principal")?;
        validate_transaction_scope_fields(
            &input.root_anchor_key,
            &input.root_key_hash,
            &input.scope_partition,
        )?;
        let ttl_ms = input.ttl_ms.clamp(1, 3_600_000);
        let created_at_unix_nanos = current_unix_nanos_u64()?;
        let expires_at_unix_nanos =
            created_at_unix_nanos.saturating_add(ttl_ms.saturating_mul(1_000_000));
        let transaction_id = explicit_transaction_id(
            &input.principal,
            &input.root_key_hash,
            &input.idempotency_key,
        );
        let transaction = CoreTransaction {
            schema: CORE_TRANSACTION_SCHEMA.to_string(),
            transaction_id,
            scope_partition: input.scope_partition,
            state: CoreTransactionState::Open,
            preconditions_hash: input.preconditions_hash,
            operations_hash: ZERO_HASH.to_string(),
            visible_updates: Vec::new(),
            finalisation_error: None,
            committed_at: String::new(),
            committed_by_principal: input.principal,
            created_at_unix_nanos,
            expires_at_unix_nanos,
            root_anchor_key: input.root_anchor_key,
            root_key_hash: input.root_key_hash,
            committed_root_generation: None,
            purpose: input.purpose,
            failure_evidence: None,
            outcome: "open".to_string(),
        };

        let _guard = self.write_lock.lock().await;
        if let Some(existing) = self
            .read_transaction_unlocked(&transaction.transaction_id)
            .await?
        {
            if existing.root_anchor_key == transaction.root_anchor_key
                && existing.root_key_hash == transaction.root_key_hash
                && existing.scope_partition == transaction.scope_partition
                && existing.preconditions_hash == transaction.preconditions_hash
                && existing.committed_by_principal == transaction.committed_by_principal
            {
                return Ok(existing);
            }
            if !is_allowed_transaction_transition(&existing, &transaction) {
                bail!(
                    "CoreStore transaction {} idempotency conflict",
                    transaction.transaction_id
                );
            }
        }
        self.write_transaction_unlocked(&transaction).await?;
        Ok(transaction)
    }

    pub async fn stage_explicit_transaction_batch(
        &self,
        batch: CoreMutationBatch,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(&batch.transaction_id, "transaction id")?;
        validate_logical_id(&batch.scope_partition, "transaction scope partition")?;
        validate_logical_id(&batch.committed_by_principal, "transaction principal")?;
        if batch.operations.is_empty() {
            bail!("CoreStore explicit transaction stage must include at least one operation");
        }
        let _operation_guards = self.acquire_batch_locks(&batch).await?;
        let _guard = self.write_lock.lock().await;
        let mut transaction = self
            .read_transaction_unlocked(&batch.transaction_id)
            .await?
            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
        if transaction.state != CoreTransactionState::Open {
            bail!("TransactionNotOpen");
        }
        if transaction.committed_by_principal != batch.committed_by_principal {
            bail!("TransactionPrincipalMismatch");
        }
        validate_transaction_root_scope(&transaction)?;
        if transaction.scope_partition != batch.scope_partition {
            bail!("TransactionScopeMismatch");
        }
        if transaction.expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= transaction.expires_at_unix_nanos
        {
            let expired = transaction_with_state(
                transaction,
                CoreTransactionState::Expired,
                Some("TransactionExpired".to_string()),
            )?;
            self.write_transaction_unlocked(&expired).await?;
            bail!("TransactionExpired");
        }
        validate_explicit_transaction_scope(&batch, &transaction.scope_partition)?;
        validate_batch_partitions(&batch)?;
        self.validate_mutation_preconditions_unlocked(
            &batch.preconditions,
            &batch.committed_by_principal,
            Some(&batch.transaction_id),
        )
        .await?;

        let mut staged_updates = Vec::with_capacity(batch.operations.len());
        let mut coremeta_batch_overlay = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        for operation in &batch.operations {
            let update = match operation {
                CoreMutationOperation::StreamAppend {
                    partition_id,
                    stream_id,
                    record_kind,
                    payload,
                    idempotency_key,
                } => self
                    .append_stream_unlocked(AppendStreamRecord {
                        stream_id: stream_id.clone(),
                        partition_id: partition_id.clone(),
                        record_kind: record_kind.clone(),
                        payload: payload.clone(),
                        content_type: None,
                        user_metadata_json: "{}".to_string(),
                        fence: None,
                        transaction_id: Some(batch.transaction_id.clone()),
                        idempotency_key: idempotency_key.clone(),
                    })
                    .await
                    .map(|outcome| CoreTransactionUpdate::StreamAppend {
                        stream_id: stream_id.clone(),
                        visible_sequence: outcome.receipt.sequence,
                        prepared_record_hash: outcome.receipt.event_hash,
                    })?,
                CoreMutationOperation::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    ensure_coremeta_payload_in_transaction_scope(payload, &transaction)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let previous_payload = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => payload.clone(),
                        None => self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            &transaction,
                        )?,
                    };
                    let previous_payload_hash = previous_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    let payload_hash = core_meta_payload_digest(*table_id, payload);
                    coremeta_batch_overlay.insert(key, Some(payload.clone()));
                    CoreTransactionUpdate::CoreMetaPut {
                        cf: cf.to_string(),
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        previous_payload_hash,
                        payload: payload.clone(),
                        payload_hash,
                    }
                }
                CoreMutationOperation::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let previous_payload = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => payload.clone(),
                        None => self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            &transaction,
                        )?,
                    };
                    if let Some(payload) = previous_payload.as_ref() {
                        ensure_coremeta_payload_in_transaction_scope(payload, &transaction)?;
                    }
                    let previous_payload_hash = previous_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    coremeta_batch_overlay.insert(key, None);
                    CoreTransactionUpdate::CoreMetaDelete {
                        cf: cf.to_string(),
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        previous_payload_hash,
                    }
                }
            };
            staged_updates.push(update);
        }

        transaction.visible_updates.extend(staged_updates.clone());
        transaction.operations_hash = core_mutation_operations_hash(&batch.operations)?;
        transaction.outcome = "open".to_string();
        self.write_transaction_with_staged_rows_unlocked(&transaction, &batch.preconditions)
            .await?;
        Ok(CoreMutationBatchReceipt {
            transaction_id: batch.transaction_id,
            scope_partition: batch.scope_partition,
            state: CoreTransactionState::Open,
            visible_updates: staged_updates,
            finalisation_error: None,
        })
    }

    pub async fn stage_coremeta_put_in_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        cf: &str,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
        expected_payload_hash: Option<String>,
        require_absent: bool,
        require_present: bool,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        validate_coremeta_operation_payload(cf, table_id, &tuple_key, &payload)?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        validate_transaction_root_scope(&transaction)?;
        let scope_partition = transaction.scope_partition.clone();
        self.stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition,
            committed_by_principal: principal.to_string(),
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash,
                require_absent,
                require_present,
            }],
            operations: vec![CoreMutationOperation::CoreMetaPut {
                partition_id: transaction.root_anchor_key,
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key,
                payload,
            }],
        })
        .await
    }

    pub async fn stage_coremeta_delete_in_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        cf: &str,
        table_id: u16,
        tuple_key: Vec<u8>,
        expected_payload_hash: Option<String>,
        require_present: bool,
    ) -> Result<CoreMutationBatchReceipt> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        validate_coremeta_operation_key(cf, table_id, &tuple_key)?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        validate_transaction_root_scope(&transaction)?;
        let scope_partition = transaction.scope_partition.clone();
        self.stage_explicit_transaction_batch(CoreMutationBatch {
            transaction_id: transaction_id.to_string(),
            scope_partition,
            committed_by_principal: principal.to_string(),
            preconditions: vec![CoreMutationPrecondition::CoreMetaRow {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key: tuple_key.clone(),
                expected_payload_hash,
                require_absent: false,
                require_present,
            }],
            operations: vec![CoreMutationOperation::CoreMetaDelete {
                partition_id: transaction.root_anchor_key,
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id,
                tuple_key,
            }],
        })
        .await
    }

    pub async fn commit_explicit_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
    ) -> Result<CoreTransaction> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let _guard = self.write_lock.lock().await;
        let transaction = self
            .read_transaction_unlocked(transaction_id)
            .await?
            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
        if transaction.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        match transaction.state {
            CoreTransactionState::Committed => return Ok(transaction),
            CoreTransactionState::Open => {}
            CoreTransactionState::RolledBack | CoreTransactionState::Aborted => {
                bail!("TransactionRolledBack");
            }
            CoreTransactionState::Expired => bail!("TransactionExpired"),
            _ => bail!("TransactionNotCommittable"),
        }
        if transaction.expires_at_unix_nanos != 0
            && current_unix_nanos_u64()? >= transaction.expires_at_unix_nanos
        {
            let expired = transaction_with_state(
                transaction,
                CoreTransactionState::Expired,
                Some("TransactionExpired".to_string()),
            )?;
            self.write_transaction_unlocked(&expired).await?;
            bail!("TransactionExpired");
        }
        validate_transaction_root_scope(&transaction)?;
        self.validate_explicit_transaction_commit_unlocked(&transaction)
            .await?;
        let committed = transaction_with_state(transaction, CoreTransactionState::Committed, None)?;
        let committed_transaction = self
            .commit_explicit_transaction_rows_and_coremeta_updates_unlocked(&committed)
            .await?;
        Ok(committed_transaction)
    }

    pub async fn rollback_explicit_transaction(
        &self,
        transaction_id: &str,
        principal: &str,
        reason: &str,
    ) -> Result<CoreTransaction> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let _guard = self.write_lock.lock().await;
        let transaction = self
            .read_transaction_unlocked(transaction_id)
            .await?
            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
        if transaction.committed_by_principal != principal {
            bail!("TransactionPrincipalMismatch");
        }
        match transaction.state {
            CoreTransactionState::RolledBack => return Ok(transaction),
            CoreTransactionState::Committed => bail!("TransactionAlreadyCommitted"),
            _ => {}
        }
        let rolled_back = transaction_with_state(
            transaction,
            CoreTransactionState::RolledBack,
            Some(if reason.trim().is_empty() {
                "RollbackRequested".to_string()
            } else {
                format!("RollbackRequested: {reason}")
            }),
        )?;
        self.write_transaction_unlocked(&rolled_back).await?;
        Ok(rolled_back)
    }

    pub async fn commit_transaction(&self, transaction: CoreTransaction) -> Result<()> {
        if transaction.state != CoreTransactionState::Committed {
            bail!("CoreStore only persists committed transactions through commit_transaction");
        }
        validate_transaction_root_scope(&transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let _guard = self.write_lock.lock().await;
        self.write_transaction_unlocked(&transaction).await
    }

    pub(super) async fn write_transaction_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        if let Some(existing) = self
            .read_transaction_unlocked(&transaction.transaction_id)
            .await?
        {
            if existing.state == transaction.state
                && existing.preconditions_hash == transaction.preconditions_hash
                && existing.operations_hash == transaction.operations_hash
                && existing.visible_updates == transaction.visible_updates
                && existing.finalisation_error == transaction.finalisation_error
                && existing.committed_by_principal == transaction.committed_by_principal
                && existing.root_anchor_key == transaction.root_anchor_key
                && existing.root_key_hash == transaction.root_key_hash
                && existing.scope_partition == transaction.scope_partition
                && existing.failure_evidence == transaction.failure_evidence
                && existing.outcome == transaction.outcome
            {
                return Ok(());
            }
            if !is_allowed_transaction_transition(&existing, transaction) {
                bail!(
                    "CoreStore transaction {} idempotency conflict",
                    transaction.transaction_id
                );
            }
        }
        self.write_transaction_with_staged_rows_unlocked(transaction, &[])
            .await?;
        Ok(())
    }

    pub(super) async fn read_transaction_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransaction>> {
        self.read_transaction_from_rows_unlocked(transaction_id)
            .await
    }
}

fn precondition_is_satisfied_by_recovered_operation(
    precondition: &CoreMutationPrecondition,
    operations: &[CoreMutationOperation],
    recovered_updates: &[Option<CoreTransactionUpdate>],
) -> Result<bool> {
    for (operation, recovered_update) in operations.iter().zip(recovered_updates) {
        if recovered_update.is_none() {
            continue;
        }
        let satisfied = match (precondition, operation) {
            (
                CoreMutationPrecondition::StreamHead { stream_id, .. },
                CoreMutationOperation::StreamAppend {
                    stream_id: operation_stream_id,
                    ..
                },
            ) => stream_id == operation_stream_id,
            (
                CoreMutationPrecondition::CoreMetaRow {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                },
                CoreMutationOperation::CoreMetaPut {
                    cf: operation_cf,
                    table_id: operation_table_id,
                    tuple_key: operation_tuple_key,
                    ..
                }
                | CoreMutationOperation::CoreMetaDelete {
                    cf: operation_cf,
                    table_id: operation_table_id,
                    tuple_key: operation_tuple_key,
                    ..
                },
            ) => {
                canonical_coremeta_cf_name(cf)? == canonical_coremeta_cf_name(operation_cf)?
                    && table_id == operation_table_id
                    && tuple_key == operation_tuple_key
            }
            _ => false,
        };
        if satisfied {
            return Ok(true);
        }
    }
    Ok(false)
}

fn combine_recovered_mutation_updates(
    recovered_updates: Vec<Option<CoreTransactionUpdate>>,
    new_updates: Vec<CoreTransactionUpdate>,
) -> Result<Vec<CoreTransactionUpdate>> {
    let missing_count = recovered_updates
        .iter()
        .filter(|update| update.is_none())
        .count();
    if new_updates.len() != missing_count {
        bail!(
            "CoreStore partially applied mutation recovery produced {} updates for {missing_count} missing operations",
            new_updates.len()
        );
    }
    let mut new_updates = new_updates.into_iter();
    let combined = recovered_updates
        .into_iter()
        .map(|update| {
            update.unwrap_or_else(|| {
                new_updates
                    .next()
                    .expect("missing update count was validated before combining")
            })
        })
        .collect::<Vec<_>>();
    debug_assert!(new_updates.next().is_none());
    Ok(combined)
}

fn recovered_coremeta_previous_payload_hash(
    batch: &CoreMutationBatch,
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
) -> Result<Option<Option<String>>> {
    let mut recovered = None;
    for precondition in &batch.preconditions {
        let CoreMutationPrecondition::CoreMetaRow {
            cf: candidate_cf,
            table_id: candidate_table_id,
            tuple_key: candidate_tuple_key,
            expected_payload_hash,
            require_absent,
            require_present,
        } = precondition
        else {
            continue;
        };
        if canonical_coremeta_cf_name(candidate_cf)? != cf
            || *candidate_table_id != table_id
            || candidate_tuple_key != tuple_key
        {
            continue;
        }
        if *require_absent && *require_present {
            bail!("CoreStore CoreMeta precondition cannot require both absence and presence");
        }
        let previous = if *require_absent {
            Some(None)
        } else {
            expected_payload_hash.clone().map(Some)
        };
        let Some(previous) = previous else {
            return Ok(None);
        };
        if let Some(existing) = recovered.as_ref()
            && existing != &previous
        {
            bail!("CoreStore recovered CoreMeta preconditions conflict");
        }
        recovered = Some(previous);
    }
    Ok(recovered)
}

pub(super) fn transaction_lists_stream_record(
    transaction: &CoreTransaction,
    record: &StreamRecord,
) -> Result<bool> {
    Ok(transaction.visible_updates.iter().any(|visible_update| {
        matches!(
            visible_update,
            CoreTransactionUpdate::StreamAppend {
                stream_id,
                visible_sequence,
                prepared_record_hash,
            } if stream_id == &record.stream_id
                && *visible_sequence == record.sequence
                && prepared_record_hash == &record.event_hash
        )
    }))
}

fn ensure_coremeta_payload_in_transaction_scope(
    payload: &[u8],
    transaction: &CoreTransaction,
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    if !common.root_key_hash.is_empty() && common.root_key_hash != transaction.root_key_hash {
        bail!("TransactionScopeMismatch");
    }
    Ok(())
}

fn validate_explicit_transaction_scope(
    batch: &CoreMutationBatch,
    scope_partition: &str,
) -> Result<()> {
    for operation in &batch.operations {
        let partition_id = match operation {
            CoreMutationOperation::StreamAppend { partition_id, .. }
            | CoreMutationOperation::CoreMetaPut { partition_id, .. }
            | CoreMutationOperation::CoreMetaDelete { partition_id, .. } => partition_id,
        };
        if partition_id != scope_partition {
            bail!("TransactionScopeMismatch");
        }
    }
    Ok(())
}

fn is_allowed_transaction_transition(existing: &CoreTransaction, next: &CoreTransaction) -> bool {
    if existing.transaction_id != next.transaction_id
        || existing.scope_partition != next.scope_partition
        || existing.preconditions_hash != next.preconditions_hash
        || existing.committed_by_principal != next.committed_by_principal
        || existing.created_at_unix_nanos != next.created_at_unix_nanos
        || existing.expires_at_unix_nanos != next.expires_at_unix_nanos
        || existing.root_anchor_key != next.root_anchor_key
        || existing.root_key_hash != next.root_key_hash
    {
        return false;
    }

    match existing.state {
        CoreTransactionState::Open => match next.state {
            CoreTransactionState::Open => {
                has_prefix(&next.visible_updates, &existing.visible_updates)
            }
            CoreTransactionState::Prepared
            | CoreTransactionState::Committed
            | CoreTransactionState::FinalisationFailed
            | CoreTransactionState::Aborted
            | CoreTransactionState::RolledBack
            | CoreTransactionState::Expired
            | CoreTransactionState::Failed => true,
        },
        CoreTransactionState::Prepared => matches!(
            next.state,
            CoreTransactionState::Committed
                | CoreTransactionState::FinalisationFailed
                | CoreTransactionState::Aborted
                | CoreTransactionState::RolledBack
                | CoreTransactionState::Expired
                | CoreTransactionState::Failed
        ),
        CoreTransactionState::Committed
        | CoreTransactionState::FinalisationFailed
        | CoreTransactionState::Aborted
        | CoreTransactionState::RolledBack
        | CoreTransactionState::Expired
        | CoreTransactionState::Failed => false,
    }
}

pub(super) fn validate_core_meta_row_precondition(
    current: Option<&[u8]>,
    cf: &str,
    table_id: u16,
    expected_payload_hash: Option<&str>,
    require_absent: bool,
    require_present: bool,
) -> Result<()> {
    if require_absent && current.is_some() {
        bail!("CoreMeta row {cf}/{table_id:#06x} must be absent");
    }
    if require_present && current.is_none() {
        bail!("CoreMeta row {cf}/{table_id:#06x} must be present");
    }
    if let (Some(expected), Some(payload)) = (expected_payload_hash, current) {
        let actual = core_meta_payload_digest(table_id, payload);
        if actual != expected {
            bail!(
                "CoreMeta row {cf}/{table_id:#06x} target mismatch: payload hash mismatch expected {expected}, got {actual}"
            );
        }
    }
    Ok(())
}

fn has_prefix<T: PartialEq>(value: &[T], prefix: &[T]) -> bool {
    value.len() >= prefix.len() && &value[..prefix.len()] == prefix
}
