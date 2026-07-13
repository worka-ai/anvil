use super::local_transactions::{
    transaction_lists_stream_record, validate_core_meta_row_precondition,
};
use super::local_tx_rows::OwnedCoreMetaBatchOp;
use super::*;

impl CoreStore {
    pub(super) async fn transaction_makes_stream_record_visible(
        &self,
        record: &StreamRecord,
        transaction_id: &str,
    ) -> Result<bool> {
        let Some(transaction) = self.read_transaction_unlocked(transaction_id).await? else {
            return Ok(false);
        };
        if transaction.state != CoreTransactionState::Committed {
            return Ok(false);
        }
        transaction_lists_stream_record(&transaction, record)
    }

    pub(super) async fn filter_committed_stream_records(
        &self,
        records: Vec<StreamRecord>,
    ) -> Result<Vec<StreamRecord>> {
        let mut visible = Vec::with_capacity(records.len());
        for record in records {
            if record.stream_id == CORE_TRANSACTION_STREAM_ID {
                visible.push(record);
                continue;
            }
            if let Some(transaction_id) = record.transaction_id.as_deref()
                && !self
                    .transaction_makes_stream_record_visible(&record, transaction_id)
                    .await?
            {
                continue;
            }
            visible.push(record);
        }
        Ok(visible)
    }

    pub async fn read_coremeta_row_visible_to_transaction(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction_id: &str,
        principal: &str,
    ) -> Result<Option<Vec<u8>>> {
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        validate_coremeta_operation_key(cf, table_id, tuple_key)?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        if transaction.state != CoreTransactionState::Open {
            bail!("TransactionNotOpen");
        }
        self.coremeta_payload_visible_to_transaction_unlocked(cf, table_id, tuple_key, &transaction)
    }

    pub async fn read_stream_visible_to_transaction(
        &self,
        input: ReadStream,
        transaction_id: &str,
        principal: &str,
    ) -> Result<Vec<StreamRecord>> {
        validate_logical_id(&input.stream_id, "stream id")?;
        validate_logical_id(transaction_id, "transaction id")?;
        validate_logical_id(principal, "transaction principal")?;
        let transaction = self
            .read_explicit_transaction_for_principal(transaction_id, principal)
            .await?;
        if transaction.state != CoreTransactionState::Open {
            bail!("TransactionNotOpen");
        }

        let records = self
            .read_stream_records_after(&input.stream_id, input.after_sequence, 0)
            .await?;
        let mut visible = Vec::new();
        for record in records {
            if let Some(record_transaction_id) = record.transaction_id.as_deref() {
                if record_transaction_id == transaction_id
                    && transaction_lists_stream_record(&transaction, &record)?
                {
                    visible.push(record);
                } else if self
                    .transaction_makes_stream_record_visible(&record, record_transaction_id)
                    .await?
                {
                    visible.push(record);
                }
            } else {
                visible.push(record);
            }
        }
        if input.limit > 0 {
            visible.truncate(input.limit);
        }
        Ok(visible)
    }

    pub(super) fn coremeta_payload_visible_to_transaction_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction: &CoreTransaction,
    ) -> Result<Option<Vec<u8>>> {
        let cf = canonical_coremeta_cf_name(cf)?;
        let mut current = self.committed_coremeta_payload_unlocked(cf, table_id, tuple_key)?;
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf: update_cf,
                    table_id: update_table_id,
                    tuple_key: update_tuple_key,
                    payload,
                    ..
                } => {
                    if canonical_coremeta_cf_name(update_cf)? == cf
                        && *update_table_id == table_id
                        && update_tuple_key == tuple_key
                    {
                        current = Some(payload.clone());
                    }
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf: update_cf,
                    table_id: update_table_id,
                    tuple_key: update_tuple_key,
                    ..
                } => {
                    if canonical_coremeta_cf_name(update_cf)? == cf
                        && *update_table_id == table_id
                        && update_tuple_key == tuple_key
                    {
                        current = None;
                    }
                }
                _ => {}
            }
        }
        Ok(current)
    }

    fn committed_coremeta_payload_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let cf = canonical_coremeta_cf_name(cf)?;
        self.read_coremeta_row(cf, table_id, tuple_key)
    }

    pub(super) async fn validate_explicit_transaction_commit_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        self.validate_explicit_transaction_stream_commits_unlocked(transaction)
            .await?;
        self.validate_explicit_transaction_coremeta_commits_unlocked(transaction)
            .await
    }

    pub(super) fn prepare_coremeta_put_update_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        payload: &[u8],
    ) -> Result<(OwnedCoreMetaBatchOp, CoreTransactionUpdate)> {
        validate_coremeta_operation_payload(cf, table_id, tuple_key, payload)?;
        let cf = canonical_coremeta_cf_name(cf)?;
        let previous_payload_hash = self
            .committed_coremeta_payload_unlocked(cf, table_id, tuple_key)?
            .map(|payload| core_meta_payload_digest(table_id, &payload));
        let payload_hash = core_meta_payload_digest(table_id, payload);
        let op = OwnedCoreMetaBatchOp::Put {
            cf,
            table_id,
            tuple_key: tuple_key.to_vec(),
            common: None,
            payload: payload.to_vec(),
        };
        let update = CoreTransactionUpdate::CoreMetaPut {
            cf: cf.to_string(),
            table_id,
            tuple_key: tuple_key.to_vec(),
            previous_payload_hash,
            payload: payload.to_vec(),
            payload_hash,
        };
        Ok((op, update))
    }

    pub(super) fn prepare_coremeta_delete_update_unlocked(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction_id: String,
    ) -> Result<(OwnedCoreMetaBatchOp, CoreTransactionUpdate)> {
        validate_coremeta_operation_key(cf, table_id, tuple_key)?;
        let cf = canonical_coremeta_cf_name(cf)?;
        let current_payload = self.committed_coremeta_payload_unlocked(cf, table_id, tuple_key)?;
        let previous_payload_hash = current_payload
            .as_ref()
            .map(|payload| core_meta_payload_digest(table_id, payload));
        let deleted_at_unix_nanos = current_unix_nanos_u64()?;
        let delete_common = current_payload
            .as_ref()
            .map(|payload| {
                core_meta_row_common_from_payload(payload).map(|common| {
                    core_meta_committed_row_common(
                        common.realm_id,
                        common.root_key_hash,
                        common.root_generation.saturating_add(1),
                        transaction_id.clone(),
                        deleted_at_unix_nanos,
                    )
                })
            })
            .transpose()?;
        let op = OwnedCoreMetaBatchOp::Delete {
            cf,
            table_id,
            tuple_key: tuple_key.to_vec(),
            common: delete_common,
        };
        let update = CoreTransactionUpdate::CoreMetaDelete {
            cf: cf.to_string(),
            table_id,
            tuple_key: tuple_key.to_vec(),
            previous_payload_hash,
        };
        Ok((op, update))
    }

    async fn validate_explicit_transaction_stream_commits_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        for update in &transaction.visible_updates {
            let CoreTransactionUpdate::StreamAppend {
                stream_id,
                visible_sequence,
                prepared_record_hash,
            } = update
            else {
                continue;
            };
            let records = self.read_all_stream_records(stream_id).await?;
            let Some(record) = records.iter().find(|record| {
                record.sequence == *visible_sequence
                    && record.event_hash == *prepared_record_hash
                    && record.transaction_id.as_deref() == Some(transaction.transaction_id.as_str())
            }) else {
                bail!("TransactionConflict: transaction is missing a staged stream record");
            };
            for prior in records
                .iter()
                .filter(|prior| prior.sequence < record.sequence)
            {
                let Some(prior_transaction_id) = prior.transaction_id.as_deref() else {
                    continue;
                };
                if prior_transaction_id == transaction.transaction_id {
                    continue;
                }
                if !self
                    .transaction_makes_stream_record_visible(prior, prior_transaction_id)
                    .await?
                {
                    bail!(
                        "TransactionConflict: transaction would expose a stream record before an uncommitted predecessor"
                    );
                }
            }
        }
        Ok(())
    }

    async fn validate_explicit_transaction_coremeta_commits_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<()> {
        let mut coremeta_visible = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        let mut coremeta_root_generation = None;
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                    payload,
                    payload_hash,
                } => {
                    validate_coremeta_operation_payload(cf, *table_id, tuple_key, payload)?;
                    let actual_hash = core_meta_payload_digest(*table_id, payload);
                    if &actual_hash != payload_hash {
                        bail!("TransactionConflict: staged CoreMeta payload hash mismatch");
                    }
                    let common = validate_committed_coremeta_put_common(transaction, payload)?;
                    match coremeta_root_generation {
                        Some(generation) if generation != common.root_generation => {
                            bail!(
                                "TransactionScopeMismatch: explicit transaction touches multiple root generations"
                            );
                        }
                        Some(_) => {}
                        None => coremeta_root_generation = Some(common.root_generation),
                    }
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let current_payload = match coremeta_visible.get(&key) {
                        Some(payload) => payload.clone(),
                        None => {
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                        }
                    };
                    let current_hash = current_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    if &current_hash != previous_payload_hash {
                        bail!("TransactionConflict: staged CoreMeta put preimage changed");
                    }
                    coremeta_visible.insert(key, Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                } => {
                    validate_coremeta_operation_key(cf, *table_id, tuple_key)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let (from_overlay, current_payload) = match coremeta_visible.get(&key) {
                        Some(payload) => (true, payload.clone()),
                        None => (
                            false,
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?,
                        ),
                    };
                    let current_hash = current_payload
                        .as_ref()
                        .map(|payload| core_meta_payload_digest(*table_id, payload));
                    if &current_hash != previous_payload_hash {
                        bail!("TransactionConflict: staged CoreMeta delete preimage changed");
                    }
                    if let Some(payload) = current_payload.as_ref() {
                        if from_overlay {
                            if let Some(delete_generation) = delete_generation_from_visible_payload(
                                transaction,
                                payload,
                                from_overlay,
                            )? {
                                match coremeta_root_generation {
                                    Some(generation) if generation != delete_generation => {
                                        bail!(
                                            "TransactionScopeMismatch: explicit transaction touches multiple root generations"
                                        );
                                    }
                                    Some(_) => {}
                                    None => coremeta_root_generation = Some(delete_generation),
                                }
                            }
                        } else {
                            validate_delete_visible_payload_scope(transaction, payload)?;
                        }
                    }
                    coremeta_visible.insert(key, None);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) async fn commit_explicit_transaction_rows_and_coremeta_updates_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<CoreTransaction> {
        let committed_root_generation = self
            .infer_explicit_transaction_commit_root_generation_unlocked(transaction)
            .await?;
        let mut committed_transaction = transaction.clone();
        committed_transaction.committed_root_generation = Some(committed_root_generation);
        let mut owned_ops =
            vec![self.transaction_header_as_coremeta_op_unlocked(&committed_transaction)?];
        let mut coremeta_batch_overlay = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    validate_coremeta_operation_payload(cf, *table_id, tuple_key, payload)?;
                    let common = validate_committed_coremeta_put_common(transaction, payload)?;
                    if common.root_generation != committed_root_generation {
                        bail!(
                            "TransactionScopeMismatch: explicit transaction CoreMeta put generation does not match committed root generation"
                        );
                    }
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    owned_ops.push(OwnedCoreMetaBatchOp::Put {
                        cf,
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        payload: payload.clone(),
                        common: None,
                    });
                    coremeta_batch_overlay.insert(key, Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    validate_coremeta_operation_key(cf, *table_id, tuple_key)?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let current_payload = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => payload.clone(),
                        None => {
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                        }
                    };
                    owned_ops.push(OwnedCoreMetaBatchOp::Delete {
                        cf,
                        table_id: *table_id,
                        tuple_key: tuple_key.clone(),
                        common: Some(delete_common_for_committed_transaction(
                            transaction,
                            current_payload.as_deref(),
                            committed_root_generation,
                            current_unix_nanos_u64()?,
                        )?),
                    });
                    coremeta_batch_overlay.insert(key, None);
                }
                _ => {}
            }
        }
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        self.commit_coremeta_batch_for_root(
            &committed_transaction.root_key_hash,
            committed_root_generation.saturating_sub(1),
            committed_root_generation,
            &committed_transaction.transaction_id,
            &ops,
        )
        .await?;
        Ok(committed_transaction)
    }

    async fn infer_explicit_transaction_commit_root_generation_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<u64> {
        let mut coremeta_root_generation = None;
        let mut fallback_delete_generation = None;
        let mut coremeta_batch_overlay = BTreeMap::<(String, u16, Vec<u8>), Option<Vec<u8>>>::new();

        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } => {
                    let common = validate_committed_coremeta_put_common(transaction, payload)?;
                    merge_coremeta_root_generation(
                        &mut coremeta_root_generation,
                        common.root_generation,
                    )?;
                    let cf = canonical_coremeta_cf_name(cf)?;
                    coremeta_batch_overlay.insert(
                        (cf.to_string(), *table_id, tuple_key.clone()),
                        Some(payload.clone()),
                    );
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } => {
                    let cf = canonical_coremeta_cf_name(cf)?;
                    let key = (cf.to_string(), *table_id, tuple_key.clone());
                    let (from_overlay, current_payload) = match coremeta_batch_overlay.get(&key) {
                        Some(payload) => (true, payload.clone()),
                        None => (
                            false,
                            self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?,
                        ),
                    };
                    if let Some(payload) = current_payload.as_ref() {
                        let delete_generation = delete_generation_from_visible_payload(
                            transaction,
                            payload,
                            from_overlay,
                        )?;
                        if from_overlay {
                            if let Some(delete_generation) = delete_generation {
                                merge_coremeta_root_generation(
                                    &mut coremeta_root_generation,
                                    delete_generation,
                                )?;
                            }
                        } else {
                            validate_delete_visible_payload_scope(transaction, payload)?;
                            if let Some(delete_generation) = delete_generation {
                                merge_coremeta_root_generation(
                                    &mut fallback_delete_generation,
                                    delete_generation,
                                )?;
                            }
                        }
                    }
                    coremeta_batch_overlay.insert(key, None);
                }
                _ => {}
            }
        }

        if let Some(generation) = coremeta_root_generation {
            return Ok(generation);
        }
        if let Some(generation) = fallback_delete_generation {
            return Ok(generation);
        }
        if let Some(generation) =
            committed_root_generation_from_updates(&transaction.visible_updates)?
        {
            return Ok(generation);
        }
        if let Some(generation) = transaction
            .committed_root_generation
            .filter(|generation| *generation > 0)
        {
            return Ok(generation);
        }

        let current_root_generation = self
            .read_latest_root_anchor(&transaction.root_anchor_key)
            .await?
            .map(|anchor| anchor.root_generation)
            .unwrap_or(0);
        current_root_generation
            .checked_add(1)
            .filter(|generation| *generation > 0)
            .ok_or_else(|| anyhow!("CoreStore explicit transaction root generation overflow"))
    }

    pub(super) async fn validate_mutation_preconditions_unlocked(
        &self,
        preconditions: &[CoreMutationPrecondition],
        committed_by_principal: &str,
        transaction_id: Option<&str>,
    ) -> Result<()> {
        for precondition in preconditions {
            match precondition {
                CoreMutationPrecondition::Fence {
                    fence_name,
                    fence_token,
                } => {
                    self.validate_fence_precondition_unlocked(&CoreFencePrecondition {
                        fence_name: fence_name.clone(),
                        fence_token: *fence_token,
                        authenticated_principal: committed_by_principal.to_string(),
                    })
                    .await?;
                }
                CoreMutationPrecondition::CoreMetaRow {
                    cf,
                    table_id,
                    tuple_key,
                    expected_payload_hash,
                    require_absent,
                    require_present,
                } => {
                    let current = if let Some(transaction_id) = transaction_id {
                        let transaction = self
                            .read_transaction_unlocked(transaction_id)
                            .await?
                            .ok_or_else(|| anyhow!("TransactionNotFound"))?;
                        self.coremeta_payload_visible_to_transaction_unlocked(
                            cf,
                            *table_id,
                            tuple_key,
                            &transaction,
                        )?
                    } else {
                        self.committed_coremeta_payload_unlocked(cf, *table_id, tuple_key)?
                    };
                    validate_core_meta_row_precondition(
                        current.as_deref(),
                        cf,
                        *table_id,
                        expected_payload_hash.as_deref(),
                        *require_absent,
                        *require_present,
                    )?;
                }
                CoreMutationPrecondition::StreamHead {
                    stream_id,
                    expected_last_sequence,
                    expected_last_event_hash,
                } => {
                    let head = self.read_stream_head_from_meta(stream_id)?;
                    let (actual_sequence, actual_hash) = head
                        .map(|head| (head.last_sequence, head.last_event_hash))
                        .unwrap_or_else(|| (0, ZERO_HASH.to_string()));
                    if actual_sequence != *expected_last_sequence
                        || actual_hash != *expected_last_event_hash
                    {
                        return Err(CoreStoreCommitError::StreamHeadMismatch {
                            stream_id: stream_id.clone(),
                            expected_last_sequence: *expected_last_sequence,
                            expected_last_event_hash: expected_last_event_hash.clone(),
                            actual_sequence,
                            actual_event_hash: actual_hash,
                        }
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    pub(super) async fn validate_source_watch_cursor_unlocked(&self, cursor: &str) -> Result<()> {
        let (stream_id, sequence) = parse_stream_cursor(cursor)?;
        let Some(record) = self
            .read_stream(ReadStream {
                stream_id,
                after_sequence: sequence.saturating_sub(1),
                limit: 1,
            })
            .await?
            .into_iter()
            .next()
        else {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        };
        if record.cursor != cursor {
            bail!("WatchCursorExpired: CoreStore source watch cursor is not retained");
        }
        Ok(())
    }

    pub(super) async fn validate_fence_precondition_unlocked(
        &self,
        precondition: &CoreFencePrecondition,
    ) -> Result<()> {
        validate_logical_id(&precondition.fence_name, "fence name")?;
        validate_logical_id(
            &precondition.authenticated_principal,
            "fence authenticated principal",
        )?;
        let Some(record) = super::local_stream_control::read_core_fence_current_row(
            self,
            &precondition.fence_name,
        )?
        else {
            bail!("CoreStore fence {} is not held", precondition.fence_name);
        };
        if record.owner_principal != precondition.authenticated_principal
            || record.fence_token != precondition.fence_token
            || record.expires_at_ms <= Utc::now().timestamp_millis()
        {
            bail!(
                "CoreStore fence {} precondition failed",
                precondition.fence_name
            );
        }
        Ok(())
    }
}

fn validate_committed_coremeta_put_common(
    transaction: &CoreTransaction,
    payload: &[u8],
) -> Result<CoreMetaRowCommonProto> {
    let common = core_meta_row_common_from_payload(payload)?;
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("TransactionConflict: staged CoreMeta payload must commit as visible");
    }
    if common.root_key_hash != transaction.root_key_hash {
        bail!("TransactionScopeMismatch");
    }
    if common.root_generation == 0 {
        bail!("TransactionConflict: staged CoreMeta payload has no root generation");
    }
    if common.transaction_id != transaction.transaction_id {
        bail!("TransactionScopeMismatch: staged CoreMeta payload transaction id mismatch");
    }
    Ok(common)
}

fn delete_generation_from_visible_payload(
    transaction: &CoreTransaction,
    payload: &[u8],
    from_overlay: bool,
) -> Result<Option<u64>> {
    let common = core_meta_row_common_from_payload(payload)?;
    if common.root_key_hash.is_empty() {
        return Ok(None);
    }
    validate_delete_visible_common_scope(transaction, &common)?;
    if from_overlay {
        Ok(Some(common.root_generation))
    } else {
        common
            .root_generation
            .checked_add(1)
            .map(Some)
            .ok_or_else(|| anyhow!("CoreStore explicit transaction root generation overflow"))
    }
}

fn validate_delete_visible_payload_scope(
    transaction: &CoreTransaction,
    payload: &[u8],
) -> Result<()> {
    let common = core_meta_row_common_from_payload(payload)?;
    validate_delete_visible_common_scope(transaction, &common)
}

fn validate_delete_visible_common_scope(
    transaction: &CoreTransaction,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.root_key_hash.is_empty() {
        return Ok(());
    }
    if common.root_key_hash != transaction.root_key_hash {
        bail!("TransactionScopeMismatch");
    }
    if common.root_generation == 0 {
        bail!("TransactionConflict: staged CoreMeta delete has no root generation");
    }
    Ok(())
}

fn delete_common_for_committed_transaction(
    transaction: &CoreTransaction,
    current_payload: Option<&[u8]>,
    committed_root_generation: u64,
    created_at_unix_nanos: u64,
) -> Result<CoreMetaRowCommonProto> {
    if committed_root_generation == 0 {
        bail!("CoreStore explicit transaction delete must use a non-zero root generation");
    }
    let realm_id = match current_payload {
        Some(payload) => {
            let common = core_meta_row_common_from_payload(payload)?;
            if !common.root_key_hash.is_empty() && common.root_key_hash != transaction.root_key_hash
            {
                bail!("TransactionScopeMismatch");
            }
            if !common.root_key_hash.is_empty() {
                common.realm_id
            } else {
                transaction.committed_by_principal.clone()
            }
        }
        None => transaction.committed_by_principal.clone(),
    };
    Ok(core_meta_committed_row_common(
        realm_id,
        transaction.root_key_hash.clone(),
        committed_root_generation,
        transaction.transaction_id.clone(),
        created_at_unix_nanos,
    ))
}

fn merge_coremeta_root_generation(target: &mut Option<u64>, generation: u64) -> Result<()> {
    if generation == 0 {
        bail!("CoreStore explicit transaction rooted rows must use non-zero generations");
    }
    match target {
        Some(existing) if *existing != generation => {
            bail!(
                "TransactionScopeMismatch: explicit transaction touches multiple root generations"
            )
        }
        Some(_) => {}
        None => *target = Some(generation),
    }
    Ok(())
}
