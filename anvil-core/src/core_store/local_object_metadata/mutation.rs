use super::*;
use std::collections::BTreeMap;

const VERSION_SCAN_PAGE_ROWS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ObjectMetadataProjectionMutation {
    Upsert,
    DeleteVersion,
}

pub(crate) struct ObjectMetadataMutationGuard {
    _guard: super::super::CoreStoreLock,
}

#[derive(Debug)]
pub(crate) struct PreparedObjectMetadataProjection {
    pub(crate) root_generation: u64,
    pub(crate) operations: Vec<CoreMutationOperation>,
}

impl CoreStore {
    pub(crate) async fn acquire_object_metadata_mutation_lock(
        &self,
        bucket: &Bucket,
    ) -> Result<ObjectMetadataMutationGuard> {
        Ok(ObjectMetadataMutationGuard {
            _guard: self
                .acquire_named_lock(
                    "object-metadata-bucket",
                    &object_metadata_bucket_lock_id(bucket),
                )
                .await?,
        })
    }

    pub(crate) async fn prepare_object_metadata_projection(
        &self,
        bucket: &Bucket,
        object: &Object,
        mutation: ObjectMetadataProjectionMutation,
        scope_partition: &str,
        transaction_id: &str,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<PreparedObjectMetadataProjection> {
        validate_object_scope(bucket, object)?;
        if scope_partition != object_metadata_root_anchor_key(bucket.tenant_id, bucket.id) {
            bail!("object metadata mutation scope does not match its CoreMeta root");
        }
        let root_generation = match explicit_transaction {
            Some(transaction) => {
                if transaction.transaction_id != transaction_id
                    || transaction.root_anchor_key != scope_partition
                    || transaction.root_key_hash
                        != object_metadata_root_key_hash(bucket.tenant_id, bucket.id)
                {
                    bail!("object metadata explicit transaction scope mismatch");
                }
                self.infer_explicit_transaction_commit_root_generation(transaction)
                    .await?
            }
            None => {
                self.implicit_root_generation_unlocked(transaction_id, scope_partition, None)
                    .await?
            }
        };
        let operations = match mutation {
            ObjectMetadataProjectionMutation::Upsert => {
                self.prepare_object_metadata_upsert_operations(
                    bucket,
                    object,
                    scope_partition,
                    transaction_id,
                    root_generation,
                    explicit_transaction,
                )
                .await?
            }
            ObjectMetadataProjectionMutation::DeleteVersion => {
                self.prepare_object_metadata_delete_version_operations(
                    bucket,
                    object,
                    scope_partition,
                    transaction_id,
                    root_generation,
                    explicit_transaction,
                )
                .await?
            }
        };
        Ok(PreparedObjectMetadataProjection {
            root_generation,
            operations,
        })
    }

    pub(crate) async fn materialize_object_metadata_ancillary_projections(
        &self,
        bucket: &Bucket,
        object: &Object,
        mutation: ObjectMetadataProjectionMutation,
    ) -> Result<()> {
        let transaction_id = object.mutation_id.to_string();
        let (owned_ops, publications) = match mutation {
            ObjectMetadataProjectionMutation::Upsert => {
                self.payload_reference_put_ops_for_object(bucket, object, &transaction_id)
                    .await?
            }
            ObjectMetadataProjectionMutation::DeleteVersion => {
                self.payload_reference_delete_ops_for_object(bucket, object, &transaction_id)
                    .await?
            }
        };
        if !owned_ops.is_empty() {
            let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
            self.commit_coremeta_root_groups(&transaction_id, &ops, &publications)
                .await?;
        }
        if mutation == ObjectMetadataProjectionMutation::Upsert {
            self.materialize_object_boundary_values(bucket, object)
                .await?;
        }
        Ok(())
    }

    #[cfg(test)]
    pub async fn put_object_metadata(&self, bucket: &Bucket, object: &Object) -> Result<()> {
        let _guard = self.acquire_object_metadata_mutation_lock(bucket).await?;
        let transaction_id = format!("object-metadata-projection:{}", object.mutation_id);
        let scope_partition = object_metadata_root_anchor_key(bucket.tenant_id, bucket.id);
        let prepared = self
            .prepare_object_metadata_projection(
                bucket,
                object,
                ObjectMetadataProjectionMutation::Upsert,
                &scope_partition,
                &transaction_id,
                None,
            )
            .await?;
        let receipt = self
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id,
                root_publications: vec![object_metadata_root_publication(&scope_partition)],
                scope_partition,
                committed_by_principal: format!(
                    "object-metadata-projection:{}:{}",
                    bucket.tenant_id, bucket.id
                ),
                preconditions: Vec::new(),
                operations: prepared.operations,
            })
            .await?;
        require_committed_projection_receipt(&receipt)?;
        self.materialize_object_metadata_ancillary_projections(
            bucket,
            object,
            ObjectMetadataProjectionMutation::Upsert,
        )
        .await
    }

    #[cfg(test)]
    pub async fn delete_object_version_metadata(
        &self,
        bucket: &Bucket,
        object: &Object,
    ) -> Result<()> {
        let _guard = self.acquire_object_metadata_mutation_lock(bucket).await?;
        let transaction_id = format!("object-metadata-projection:{}", object.mutation_id);
        let scope_partition = object_metadata_root_anchor_key(bucket.tenant_id, bucket.id);
        let prepared = self
            .prepare_object_metadata_projection(
                bucket,
                object,
                ObjectMetadataProjectionMutation::DeleteVersion,
                &scope_partition,
                &transaction_id,
                None,
            )
            .await?;
        let receipt = self
            .commit_mutation_batch(CoreMutationBatch {
                transaction_id,
                root_publications: vec![object_metadata_root_publication(&scope_partition)],
                scope_partition,
                committed_by_principal: format!(
                    "object-metadata-projection:{}:{}",
                    bucket.tenant_id, bucket.id
                ),
                preconditions: Vec::new(),
                operations: prepared.operations,
            })
            .await?;
        require_committed_projection_receipt(&receipt)?;
        self.materialize_object_metadata_ancillary_projections(
            bucket,
            object,
            ObjectMetadataProjectionMutation::DeleteVersion,
        )
        .await
    }

    pub async fn next_object_metadata_id(&self, bucket: &Bucket) -> Result<i64> {
        self.next_object_metadata_id_in_transaction(bucket, None)
            .await
    }

    pub(crate) async fn next_object_metadata_id_in_transaction(
        &self,
        bucket: &Bucket,
        transaction_id: Option<&str>,
    ) -> Result<i64> {
        let transaction = match transaction_id {
            Some(transaction_id) => Some(
                self.read_transaction(transaction_id)
                    .await?
                    .ok_or_else(|| anyhow!("TransactionNotFound"))?,
            ),
            None => None,
        };
        let counter_key = object_id_counter_key(bucket);
        let projected_max_id = match self.object_metadata_payload_visible_to_transaction(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
            transaction.as_ref(),
        )? {
            Some(bytes) => decode_object_metadata_counter_for_bucket(&bytes, bucket)?.max_id,
            None => 0,
        };
        let stream_max_id = i64::try_from(
            self.stream_head_sequence(&format!(
                "object_metadata:tenant:{}:bucket:{}",
                bucket.tenant_id, bucket.id
            ))
            .await?,
        )
        .context("object metadata stream sequence exceeds i64")?;
        projected_max_id
            .max(stream_max_id)
            .checked_add(1)
            .ok_or_else(|| anyhow!("object id overflow"))
    }

    async fn prepare_object_metadata_upsert_operations(
        &self,
        bucket: &Bucket,
        object: &Object,
        scope_partition: &str,
        transaction_id: &str,
        root_generation: u64,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<Vec<CoreMutationOperation>> {
        let payload = encode_object_metadata_row_at_generation_for_transaction(
            object,
            root_generation,
            transaction_id,
        )?;
        let counter_payload = explicit_transaction
            .is_none()
            .then(|| {
                self.object_id_counter_payload_at_generation(
                    bucket,
                    object.id,
                    root_generation,
                    transaction_id,
                    explicit_transaction,
                )
            })
            .transpose()?;
        let mut operations = vec![
            put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_key(bucket, &object.key),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_key(bucket, &object.key, object.version_id),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_id_key(bucket, object.version_id),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_key_catalog_key(bucket, object),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_page_key_for_object(bucket, object, root_generation),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_catalog_key(bucket, object, root_generation),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                object_current_history_key(bucket, &object.key, root_generation, object.version_id),
                payload.clone(),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_history_key(bucket, &object.key, object.version_id, root_generation),
                payload.clone(),
            ),
        ];
        if let Some(counter_payload) = counter_payload {
            operations.push(put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_id_counter_key(bucket),
                counter_payload,
            ));
        }
        let current_page_key = object_current_page_key_for_object(bucket, object);
        if object.deleted_at.is_some() {
            operations.push(delete_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                current_page_key,
            ));
        } else {
            operations.push(put_operation(
                scope_partition,
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                current_page_key,
                payload,
            ));
        }
        Ok(operations)
    }

    async fn prepare_object_metadata_delete_version_operations(
        &self,
        bucket: &Bucket,
        deletion: &Object,
        scope_partition: &str,
        transaction_id: &str,
        root_generation: u64,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<Vec<CoreMutationOperation>> {
        if deletion.deleted_at.is_none() {
            bail!("object version deletion projection requires deleted_at");
        }
        let object_key = deletion.key.as_str();
        let version_id = deletion.version_id;
        let current_key = object_current_key(bucket, object_key);
        let version_key = object_version_key(bucket, object_key, version_id);
        let current = self
            .object_metadata_payload_visible_to_transaction(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                &current_key,
                explicit_transaction,
            )?
            .map(|payload| decode_object_metadata_row(&payload))
            .transpose()?;
        let original = self
            .object_metadata_payload_visible_to_transaction(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &version_key,
                explicit_transaction,
            )?
            .map(|payload| decode_object_metadata_row_with_common(&payload))
            .transpose()?
            .ok_or_else(|| anyhow!("CoreStore object version metadata row missing"))?;
        validate_object_scope(bucket, &original.object)?;
        if original.object.key != object_key || original.object.version_id != version_id {
            bail!("CoreStore object version deletion row key mismatch");
        }
        let deleted_is_current = current
            .as_ref()
            .is_some_and(|object| object.key == object_key && object.version_id == version_id);
        let replacement = if deleted_is_current {
            self.latest_object_version_for_key_after_delete_in_transaction(
                bucket,
                object_key,
                version_id,
                explicit_transaction,
            )?
        } else {
            None
        };
        let mut tombstone = deletion.clone();
        tombstone.record_hash = format!("sha256:{}", sha256_hex(tombstone.mutation_id.as_bytes()));
        let tombstone_payload =
            encode_object_metadata_row_at_generation_with_delete_marker_for_transaction(
                &tombstone,
                root_generation,
                false,
                transaction_id,
            )?;
        let replacement_payload = replacement
            .as_ref()
            .map(|object| {
                encode_object_metadata_row_at_generation_for_transaction(
                    object,
                    root_generation,
                    transaction_id,
                )
            })
            .transpose()?;
        let counter_payload = explicit_transaction
            .is_none()
            .then(|| {
                self.object_id_counter_payload_at_generation(
                    bucket,
                    deletion.id,
                    root_generation,
                    transaction_id,
                    explicit_transaction,
                )
            })
            .transpose()?;
        let mut operations = vec![
            delete_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                version_key,
            ),
            delete_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_id_key(bucket, version_id),
            ),
            delete_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_page_key_for_object(
                    bucket,
                    &original.object,
                    original.root_generation,
                ),
            ),
            put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_version_history_key(bucket, object_key, version_id, root_generation),
                tombstone_payload.clone(),
            ),
        ];
        if let Some(counter_payload) = counter_payload {
            operations.push(put_operation(
                scope_partition,
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                object_id_counter_key(bucket),
                counter_payload,
            ));
        }
        if deleted_is_current {
            match (replacement.as_ref(), replacement_payload.as_ref()) {
                (Some(replacement), Some(replacement_payload)) => {
                    operations.extend([
                        put_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            current_key,
                            replacement_payload.clone(),
                        ),
                        if replacement.deleted_at.is_some() {
                            delete_operation(
                                scope_partition,
                                CF_OBJECT_HEADS,
                                TABLE_OBJECT_HEAD_ROW,
                                object_current_page_key_for_object(bucket, replacement),
                            )
                        } else {
                            put_operation(
                                scope_partition,
                                CF_OBJECT_HEADS,
                                TABLE_OBJECT_HEAD_ROW,
                                object_current_page_key_for_object(bucket, replacement),
                                replacement_payload.clone(),
                            )
                        },
                        put_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            object_current_history_key(
                                bucket,
                                object_key,
                                root_generation,
                                replacement.version_id,
                            ),
                            replacement_payload.clone(),
                        ),
                    ]);
                }
                (None, None) => {
                    operations.extend([
                        delete_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            current_key,
                        ),
                        delete_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            object_current_page_key_for_object(bucket, &original.object),
                        ),
                        put_operation(
                            scope_partition,
                            CF_OBJECT_HEADS,
                            TABLE_OBJECT_HEAD_ROW,
                            object_current_history_key(
                                bucket,
                                object_key,
                                root_generation,
                                version_id,
                            ),
                            tombstone_payload,
                        ),
                    ]);
                }
                _ => unreachable!("replacement object and payload are created together"),
            }
        }
        Ok(operations)
    }

    fn object_id_counter_payload_at_generation(
        &self,
        bucket: &Bucket,
        candidate_id: i64,
        root_generation: u64,
        transaction_id: &str,
        explicit_transaction: Option<&CoreTransaction>,
    ) -> Result<Vec<u8>> {
        if candidate_id <= 0 {
            bail!("object metadata id must be positive");
        }
        let counter_key = object_id_counter_key(bucket);
        let current_max = match self.object_metadata_payload_visible_to_transaction(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
            explicit_transaction,
        )? {
            Some(bytes) => decode_object_metadata_counter_for_bucket(&bytes, bucket)?.max_id,
            None => i64::try_from(root_generation.saturating_sub(1))
                .context("object metadata root generation exceeds i64")?,
        };
        let effective_max = candidate_id.max(current_max);
        encode_object_metadata_counter_at_generation(
            bucket,
            effective_max,
            root_generation,
            transaction_id,
        )
    }

    pub(super) async fn current_object_metadata_root_generation(
        &self,
        bucket: &Bucket,
    ) -> Result<u64> {
        let counter_key = object_id_counter_key(bucket);
        let Some(payload) = self.read_coremeta_row(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &counter_key,
        )?
        else {
            return Ok(0);
        };
        let counter = decode_object_metadata_counter_for_bucket(&payload, bucket)?;
        Ok(counter
            .common
            .expect("counter decoder requires CoreMeta common")
            .root_generation)
    }

    fn object_metadata_payload_visible_to_transaction(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        transaction: Option<&CoreTransaction>,
    ) -> Result<Option<Vec<u8>>> {
        match transaction {
            Some(transaction) => self.coremeta_payload_visible_to_transaction_unlocked(
                cf,
                table_id,
                tuple_key,
                transaction,
            ),
            None => self.read_coremeta_row(canonical_coremeta_cf_name(cf)?, table_id, tuple_key),
        }
    }

    fn latest_object_version_for_key_after_delete_in_transaction(
        &self,
        bucket: &Bucket,
        object_key: &str,
        deleted_version_id: uuid::Uuid,
        transaction: Option<&CoreTransaction>,
    ) -> Result<Option<Object>> {
        let Some(transaction) = transaction else {
            return self.latest_object_version_for_key_after_delete(
                bucket,
                object_key,
                deleted_version_id,
            );
        };
        let prefix = object_version_page_prefix(bucket, object_key);
        let mut overlay = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for update in &transaction.visible_updates {
            match update {
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    payload,
                    ..
                } if cf == CF_OBJECT_VERSIONS
                    && *table_id == TABLE_OBJECT_VERSION_META_ROW
                    && tuple_key.starts_with(&prefix) =>
                {
                    overlay.insert(tuple_key.clone(), Some(payload.clone()));
                }
                CoreTransactionUpdate::CoreMetaDelete {
                    cf,
                    table_id,
                    tuple_key,
                    ..
                } if cf == CF_OBJECT_VERSIONS
                    && *table_id == TABLE_OBJECT_VERSION_META_ROW
                    && tuple_key.starts_with(&prefix) =>
                {
                    overlay.insert(tuple_key.clone(), None);
                }
                _ => {}
            }
        }
        let mut best = overlay
            .iter()
            .filter_map(|(key, payload)| payload.as_ref().map(|payload| (key.clone(), payload)))
            .map(|(key, payload)| Ok((key, decode_object_metadata_row_with_common(payload)?)))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|(_, decoded)| decoded.object.version_id != deleted_version_id)
            .min_by(|left, right| left.0.cmp(&right.0));
        let mut after = None;
        loop {
            let rows = self.scan_coremeta_prefix_page(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &prefix,
                after.as_deref(),
                VERSION_SCAN_PAGE_ROWS,
            )?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let tuple_key = core_meta_record_tuple_key(&row.key)?.to_vec();
                let payload = match overlay.get(&tuple_key) {
                    Some(Some(payload)) => payload,
                    Some(None) => continue,
                    None => &row.payload,
                };
                let decoded = decode_object_metadata_row_with_common(payload)?;
                if decoded.object.version_id == deleted_version_id {
                    continue;
                }
                if best.as_ref().is_none_or(|(key, _)| tuple_key < *key) {
                    best = Some((tuple_key, decoded));
                }
                break;
            }
            if best.is_some() || rows.len() < VERSION_SCAN_PAGE_ROWS {
                break;
            }
            after = rows
                .last()
                .map(|row| core_meta_record_tuple_key(&row.key).map(ToOwned::to_owned))
                .transpose()?;
        }
        let Some((_, decoded)) = best else {
            return Ok(None);
        };
        validate_object_scope(bucket, &decoded.object)?;
        Ok(Some(decoded.object))
    }

    async fn materialize_object_boundary_values(
        &self,
        bucket: &Bucket,
        object: &Object,
    ) -> Result<()> {
        let Some(data_target) = object_data_target_from_shard_map(object.shard_map.as_ref())?
        else {
            return Ok(());
        };
        let boundary_values = match &data_target {
            ObjectDataTarget::LogicalFile { locator, .. } => {
                let manifest = self.read_logical_file_manifest(locator).await?;
                manifest_boundary_values(&manifest)
            }
            ObjectDataTarget::ObjectRef { object_ref, .. } => {
                self.read_object_manifest(object_ref).await?.boundary_values
            }
        };
        if !boundary_values.is_empty() {
            let bucket_key = boundary_schema_bucket_key(bucket.tenant_id, &bucket.name);
            self.put_boundary_values_for_object(
                &bucket_key,
                data_target.target_string(),
                &boundary_values,
            )
            .await?;
        }
        Ok(())
    }
}

fn put_operation(
    partition_id: &str,
    cf: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
    payload: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaPut {
        partition_id: partition_id.to_string(),
        cf: cf.to_string(),
        table_id,
        tuple_key,
        payload,
    }
}

fn delete_operation(
    partition_id: &str,
    cf: &str,
    table_id: u16,
    tuple_key: Vec<u8>,
) -> CoreMutationOperation {
    CoreMutationOperation::CoreMetaDelete {
        partition_id: partition_id.to_string(),
        cf: cf.to_string(),
        table_id,
        tuple_key,
    }
}

#[cfg(test)]
fn require_committed_projection_receipt(receipt: &CoreMutationBatchReceipt) -> Result<()> {
    if receipt.state != CoreTransactionState::Committed {
        bail!(
            "object metadata projection mutation did not commit: {}",
            receipt
                .finalisation_error
                .as_deref()
                .unwrap_or("unknown finalisation error")
        );
    }
    Ok(())
}

#[cfg(test)]
fn object_metadata_root_publication(root_anchor_key: &str) -> CoreMutationRootPublication {
    CoreMutationRootPublication {
        root_anchor_key: root_anchor_key.to_string(),
        writer_families: vec![
            WriterFamily::CoreControl.as_str().to_string(),
            WriterFamily::ObjectBlob.as_str().to_string(),
        ],
        transaction_coordinator: true,
    }
}
