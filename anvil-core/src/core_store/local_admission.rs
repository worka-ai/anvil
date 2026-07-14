use super::local_tx_rows::{OwnedCoreMetaBatchOp, borrow_owned_coremeta_batch_ops};
use super::*;
use crate::formats::{
    hash32,
    writer::{WriterFamily, canonical_logical_file_id},
};

impl CoreStore {
    pub(super) async fn admit_core_mutation(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: CorePendingMutationTarget,
        mutation_id: String,
        idempotency_key: Option<String>,
        payload: CorePendingMutationPayload<'_>,
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CorePendingMutationRecord> {
        let admission_started_at = Instant::now();
        let boundary_schema_generation = boundary_values
            .iter()
            .map(|value| value.schema_generation)
            .max()
            .unwrap_or(0)
            .to_string();
        record_corestore_trace_event("admission.validate", "ok");
        record_corestore_trace_event("admission.boundary_extract", "ok");
        validate_writer_family(writer_family, "pending_mutation writer family")?;
        validate_logical_id(&mutation_id, "pending_mutation mutation id")?;
        let mut landed_source_bytes = None;
        let (inline_payload, landed_bytes) = match payload {
            CorePendingMutationPayload::Empty => (Vec::new(), Vec::new()),
            CorePendingMutationPayload::Inline(bytes)
                if bytes.len() <= CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES =>
            {
                (bytes.to_vec(), Vec::new())
            }
            CorePendingMutationPayload::Inline(bytes)
            | CorePendingMutationPayload::Landed(bytes) => {
                landed_source_bytes = Some(bytes);
                let landed = self
                    .land_core_bytes(bytes, &mutation_id, &boundary_values)
                    .await?;
                (Vec::new(), vec![landed])
            }
        };
        let result = self
            .append_core_pending_mutation_record(
                operation_family,
                writer_family,
                target,
                mutation_id,
                idempotency_key,
                landed_bytes,
                &inline_payload,
                boundary_values,
            )
            .await;
        record_admission_duration(
            operation_family,
            writer_family,
            &boundary_schema_generation,
            if result.is_ok() { "ok" } else { "error" },
            admission_started_at.elapsed(),
        );
        let record = result?;
        if let Some(bytes) = landed_source_bytes {
            for landed in &record.landed_bytes {
                let hash = strip_sha256_prefix(&landed.sha256)?.to_string();
                let final_path = self.landed_bytes_path(&hash);
                self.ensure_landed_bytes_file(&final_path, bytes, &hash)
                    .await
                    .with_context(|| {
                        format!(
                            "verify CoreStore landed bytes after pending record commit landing_id={} path={}",
                            landed.landing_id,
                            final_path.display()
                        )
                    })?;
            }
        }
        Ok(record)
    }

    pub(super) async fn append_core_pending_mutation_record(
        &self,
        operation_family: &str,
        writer_family: &str,
        target: CorePendingMutationTarget,
        mutation_id: String,
        idempotency_key: Option<String>,
        landed_bytes: Vec<CorePendingLandedByte>,
        payload: &[u8],
        boundary_values: Vec<CoreBoundaryValue>,
    ) -> Result<CorePendingMutationRecord> {
        if payload.len() > CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES {
            bail!(
                "CoreStore pending mutation payload exceeds {} bytes",
                CORE_PENDING_MUTATION_MAX_INLINE_PAYLOAD_BYTES
            );
        }
        let _pending_mutation_guard = self
            .acquire_named_lock("pending_mutation", "active")
            .await?;
        self.enforce_admission_capacity(0, 0).await?;
        let sequence = self.next_core_mutation_sequence().await?;
        let record = CorePendingMutationRecord {
            schema: CORE_PENDING_MUTATION_RECORD_SCHEMA.to_string(),
            node_id: CORE_PENDING_MUTATION_NODE_ID.to_string(),
            mutation_epoch: CORE_PENDING_MUTATION_EPOCH,
            sequence,
            mutation_id,
            idempotency_key_hash: idempotency_key
                .map(|value| format!("sha256:{}", sha256_hex(value.as_bytes()))),
            anvil_storage_tenant_id: "local".to_string(),
            authz_scope: CorePendingAuthzScope {
                realm_id: "system".to_string(),
                revision: None,
            },
            operation_family: operation_family.to_string(),
            writer_family: writer_family.to_string(),
            target,
            precondition_fingerprints: Vec::new(),
            boundary_values,
            landed_bytes,
            created_at_unix_nanos: unix_timestamp_nanos(),
        };
        let hash_input = encode_pending_mutation_hash_input(&record, payload)?;
        self.enforce_admission_capacity(hash_input.len() as u64, 0)
            .await?;
        let payload_bytes = encode_stored_pending_mutation_row(&record, payload)?;
        let sequence_bytes = encode_materialisation_cursor_row(record.sequence)?;
        let pending_key = admission_record_key(record.sequence);
        let sequence_key = admission_sequence_key();
        let ops = vec![
            CoreMetaBatchOp {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_PENDING_MUTATION_ROW,
                tuple_key: &pending_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&payload_bytes),
            },
            CoreMetaBatchOp {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: &sequence_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&sequence_bytes),
            },
        ];
        let root_key = root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY);
        let metadata_commit = self
            .commit_coremeta_batch_for_root(
                &root_key,
                record.sequence.saturating_sub(1),
                record.sequence,
                &record.mutation_id,
                &ops,
            )
            .await?;
        let certificate_bytes = self.local_pending_mutation_commit_certificate_bytes(
            &record,
            &hash_input,
            metadata_commit.metadata_replica_node_ids,
            metadata_commit.certificate_hash,
            metadata_commit.certificate_persist_receipt_hashes,
        )?;
        let write_batch_started_at = Instant::now();
        let certificate_key = admission_certificate_key(record.sequence);
        let result = self
            .commit_coremeta_batch_by_embedded_roots(
                &record.mutation_id,
                &[CoreMetaBatchOp {
                    cf: CF_TRANSACTIONS,
                    table_id: TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW,
                    tuple_key: &certificate_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&certificate_bytes),
                }],
            )
            .await;
        record_corestore_trace_event(
            "admission.commit_evidence_write_batch",
            if result.is_ok() { "ok" } else { "error" },
        );
        crate::perf::record_duration(
            "anvil_rocksdb_write_batch_duration_ms",
            &[
                ("node_id", CORE_PENDING_MUTATION_NODE_ID),
                ("column_family_group", "admission_commit_evidence"),
                ("fsync_mode", "rocksdb_wal"),
                ("status", if result.is_ok() { "ok" } else { "error" }),
            ],
            write_batch_started_at.elapsed(),
        );
        result?;
        Ok(record)
    }

    pub(super) async fn land_core_bytes(
        &self,
        bytes: &[u8],
        mutation_id: &str,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<CorePendingLandedByte> {
        let hash = sha256_hex(bytes);
        let _landed_guard = self.acquire_named_lock("landed-bytes", &hash).await?;
        let final_path = self.landed_bytes_path(&hash);
        let landing_id = format!("{mutation_id}:{hash}");
        self.ensure_landed_bytes_file(&final_path, bytes, &hash)
            .await?;
        let relative_path = self.storage.relative_storage_path(&final_path)?;
        let created_at_unix_nanos = unix_timestamp_nanos();
        let landed = CorePendingLandedByte {
            sha256: format!("sha256:{hash}"),
            length: bytes.len() as u64,
            landing_id,
            relative_path,
        };
        let stored = CoreStoredLandedByteRef {
            schema: "anvil.core.landed_byte_ref.v1".to_string(),
            landed: landed.clone(),
            mutation_id: mutation_id.to_string(),
            boundary_values: boundary_values.to_vec(),
            created_at_unix_nanos,
        };
        let landed_key = meta_tuple_key(&[b"landed-byte", landed.landing_id.as_bytes()]);
        let landed_row = encode_landed_byte_ref_row(&stored)?;
        self.commit_coremeta_batch_by_embedded_roots(
            mutation_id,
            &[CoreMetaBatchOp {
                cf: CF_MATERIALISATION,
                table_id: TABLE_LANDED_BYTE_REF_ROW,
                tuple_key: &landed_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&landed_row),
            }],
        )
        .await?;
        // A finaliser for another pending mutation with the same content hash can
        // remove a content-addressed landed file between the initial existence
        // check and this mutation's durable reference row. Re-ensuring the file
        // after the row is committed closes that TOCTOU window without changing
        // landed-byte semantics.
        self.ensure_landed_bytes_file(&final_path, bytes, &hash)
            .await
            .with_context(|| {
                format!(
                    "verify CoreStore landed bytes after admission landing_id={} path={}",
                    landed.landing_id,
                    final_path.display()
                )
            })?;
        Ok(landed)
    }

    async fn ensure_landed_bytes_file(
        &self,
        final_path: &std::path::Path,
        bytes: &[u8],
        hash: &str,
    ) -> Result<()> {
        match fs::metadata(final_path).await {
            Ok(metadata) => {
                if metadata.len() != bytes.len() as u64 {
                    bail!("CoreStore landed bytes existing length mismatch");
                }
                let existing = read_file(
                    &final_path.to_path_buf(),
                    "core_store",
                    "landed_file_verify_existing",
                )
                .await?;
                if sha256_hex(&existing) != hash {
                    bail!("CoreStore landed bytes existing hash mismatch");
                }
                Ok(())
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                self.enforce_admission_capacity(0, bytes.len() as u64)
                    .await?;
                if let Some(parent) = final_path.parent() {
                    fs::create_dir_all(parent).await?;
                }
                let tmp_path =
                    final_path.with_extension(format!("landed.{}.tmp", uuid::Uuid::new_v4()));
                let started_at = Instant::now();
                let mut file = fs::File::create(&tmp_path).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_create",
                    &tmp_path,
                    0,
                    started_at.elapsed(),
                );
                let started_at = Instant::now();
                file.write_all(bytes).await?;
                record_landed_bytes_duration(
                    "write",
                    "ok",
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                record_corestore_trace_event("admission.landed_write", "ok");
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_write",
                    &tmp_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                let started_at = Instant::now();
                file.sync_all().await?;
                let elapsed = started_at.elapsed();
                record_landed_bytes_duration("fsync", "ok", bytes.len() as u64, elapsed);
                record_corestore_trace_event("admission.landed_fsync", "ok");
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_sync",
                    &tmp_path,
                    bytes.len() as u64,
                    elapsed,
                );
                crate::perf::record_fsync_duration(
                    "core_store",
                    "landed_bytes",
                    "landed_file_sync",
                    elapsed,
                );
                drop(file);
                let started_at = Instant::now();
                fs::rename(&tmp_path, final_path).await?;
                crate::perf::record_io_duration(
                    "core_store",
                    "landed_file_rename",
                    final_path,
                    bytes.len() as u64,
                    started_at.elapsed(),
                );
                sync_parent_dir(&final_path.to_path_buf(), "landed_file_sync_parent_dir").await?;
                Ok(())
            }
            Err(err) => Err(err).with_context(|| {
                format!("inspect CoreStore landed bytes {}", final_path.display())
            }),
        }
    }

    pub(super) fn local_pending_mutation_commit_certificate_bytes(
        &self,
        record: &CorePendingMutationRecord,
        pending_mutation_hash_input: &[u8],
        metadata_replica_node_ids: Vec<String>,
        core_meta_commit_certificate_hash: String,
        certificate_persist_receipt_hashes: Vec<String>,
    ) -> Result<Vec<u8>> {
        let mut certificate = build_local_pending_mutation_commit_certificate(
            record,
            pending_mutation_hash_input,
            unix_timestamp_nanos(),
            LOCAL_SHARD_FSYNC_SEQUENCE,
            metadata_replica_node_ids,
            core_meta_commit_certificate_hash,
            certificate_persist_receipt_hashes,
        )?;
        certificate.local_receipt.source_signature =
            self.sign_core_receipt(&certificate.local_receipt.signed_payload_hash)?;
        certificate.source_signature = self.sign_core_receipt(&certificate.signed_payload_hash)?;
        validate_local_pending_mutation_commit_certificate(&certificate)?;
        self.verify_core_admission_signature(
            &record.node_id,
            &certificate.local_receipt.signed_payload_hash,
            &certificate.local_receipt.source_signature,
        )?;
        self.verify_core_admission_signature(
            &record.node_id,
            &certificate.signed_payload_hash,
            &certificate.source_signature,
        )?;
        encode_admission_commit_certificate(&certificate)
    }

    pub(super) async fn verify_local_pending_mutation_commit_certificate(
        &self,
        record: &CorePendingMutationRecord,
        pending_mutation_hash_input: &[u8],
    ) -> Result<CoreAdmissionCommitCertificate> {
        let mut bytes = None;
        for _ in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            if let Some(read) = self.meta.get(
                CF_TRANSACTIONS,
                TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW,
                &admission_certificate_key(record.sequence),
            )? {
                bytes = Some(read);
                break;
            }
            tokio::time::sleep(CORE_PROCESS_LOCK_RETRY_DELAY).await;
        }
        let bytes = bytes.ok_or_else(|| {
            anyhow!(
                "read CoreStore admission commit certificate for pending mutation sequence {}",
                record.sequence
            )
        })?;
        let certificate = decode_admission_commit_certificate(&bytes)?;
        validate_local_pending_mutation_commit_certificate(&certificate)?;
        let expected_pending_mutation_hash = domain_hash_bytes(
            "anvil.admission.pending_mutation_hash_input.v1",
            pending_mutation_hash_input,
        );
        if certificate.local_receipt.pending_mutation_hash != expected_pending_mutation_hash {
            bail!(
                "CoreStore admission commit certificate pending mutation hash input hash mismatch"
            );
        }
        let expected_attempt = admission_attempt_id_with_metadata_replicas(
            record,
            certificate.attempt_id.metadata_replica_node_ids.clone(),
        )?;
        if certificate.attempt_id != expected_attempt
            || certificate.local_receipt.attempt_id != expected_attempt
        {
            bail!("CoreStore admission commit certificate attempt id mismatch");
        }
        self.verify_core_admission_signature(
            &record.node_id,
            &certificate.local_receipt.signed_payload_hash,
            &certificate.local_receipt.source_signature,
        )?;
        self.verify_core_admission_signature(
            &record.node_id,
            &certificate.signed_payload_hash,
            &certificate.source_signature,
        )?;
        Ok(certificate)
    }

    pub(super) async fn enforce_admission_capacity(
        &self,
        incoming_pending_mutation_bytes: u64,
        incoming_landed_bytes: u64,
    ) -> Result<()> {
        self.enforce_admission_capacity_with_limits(
            incoming_pending_mutation_bytes,
            incoming_landed_bytes,
            CoreAdmissionCapacityLimits::production(),
        )
        .await
    }

    pub(super) async fn enforce_admission_capacity_with_limits(
        &self,
        incoming_pending_mutation_bytes: u64,
        incoming_landed_bytes: u64,
        limits: CoreAdmissionCapacityLimits,
    ) -> Result<()> {
        let pending_mutation_rows = self.pending_mutation_count().await?;
        let pending_mutation_bytes = self.pending_mutation_bytes().await?;
        let landed_bytes = self.admission_landed_bytes().await?;
        let projected_pending_mutation_rows = pending_mutation_rows.saturating_add(1);
        let projected_pending_mutation_bytes =
            pending_mutation_bytes.saturating_add(incoming_pending_mutation_bytes);
        let projected_landed_bytes = landed_bytes.saturating_add(incoming_landed_bytes);
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_rows",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "current"),
                ("status", "measured"),
            ],
            i64::try_from(pending_mutation_rows).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_rows",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "projected"),
                ("status", "measured"),
            ],
            i64::try_from(projected_pending_mutation_rows).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_bytes",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "current"),
                ("status", "measured"),
            ],
            i64::try_from(pending_mutation_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_rocksdb_pending_bytes",
            &[
                ("kind", "pending_mutations"),
                ("limit_class", "projected"),
                ("status", "measured"),
            ],
            i64::try_from(projected_pending_mutation_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_landed_bytes_pending",
            &[
                ("kind", "landed_bytes"),
                ("limit_class", "current"),
                ("status", "measured"),
            ],
            i64::try_from(landed_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_gauge(
            "anvil_landed_bytes_pending",
            &[
                ("kind", "landed_bytes"),
                ("limit_class", "projected"),
                ("status", "measured"),
            ],
            i64::try_from(projected_landed_bytes).unwrap_or(i64::MAX),
        );
        crate::perf::record_pending_state(
            CORE_PENDING_MUTATION_NODE_ID,
            "pending_mutations",
            pending_mutation_rows,
            pending_mutation_bytes,
            landed_bytes,
        );

        if projected_pending_mutation_rows > limits.pending_mutation_hard_limit_rows {
            bail!(
                "{}: CoreStore admission pending mutation row hard limit exceeded: current={}, projected={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                pending_mutation_rows,
                projected_pending_mutation_rows,
                limits.pending_mutation_hard_limit_rows
            );
        }

        if projected_pending_mutation_bytes > limits.pending_mutation_hard_limit_bytes {
            bail!(
                "{}: CoreStore admission pending mutation byte hard limit exceeded: current={}, incoming={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                pending_mutation_bytes,
                incoming_pending_mutation_bytes,
                limits.pending_mutation_hard_limit_bytes
            );
        }

        if projected_landed_bytes > limits.landed_bytes_hard_limit_bytes {
            bail!(
                "{}: CoreStore landed bytes hard limit exceeded: current={}, incoming={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                landed_bytes,
                incoming_landed_bytes,
                limits.landed_bytes_hard_limit_bytes
            );
        }

        let pending_mutation_lag_seconds = self.admission_materialisation_lag_seconds().await?;
        if let Some(lag_seconds) = pending_mutation_lag_seconds {
            crate::perf::record_gauge(
                "anvil_materialisation_lag_seconds",
                &[("queue", "pending_mutations"), ("status", "measured")],
                i64::try_from(lag_seconds).unwrap_or(i64::MAX),
            );
            crate::perf::record_materialisation_lag_ms(
                "pending_mutations",
                lag_seconds.saturating_mul(1_000),
            );
        }
        if let Some(lag_seconds) = pending_mutation_lag_seconds
            && lag_seconds > limits.pending_mutation_hard_lag_seconds
        {
            bail!(
                "{}: CoreStore pending mutation materialisation lag hard limit exceeded: lag_seconds={}, hard={}",
                AnvilErrorCode::ResourceExhaustedMetadataBacklog.as_str(),
                lag_seconds,
                limits.pending_mutation_hard_lag_seconds
            );
        }

        if projected_pending_mutation_rows > limits.pending_mutation_soft_limit_rows
            || projected_pending_mutation_bytes > limits.pending_mutation_soft_limit_bytes
            || projected_landed_bytes > limits.landed_bytes_soft_limit_bytes
            || pending_mutation_lag_seconds
                .is_some_and(|lag_seconds| lag_seconds > limits.pending_mutation_soft_lag_seconds)
        {
            tokio::time::sleep(CORE_PENDING_MUTATION_SOFT_BACKPRESSURE_DELAY).await;
        }

        Ok(())
    }

    pub(super) async fn pending_mutation_count(&self) -> Result<u64> {
        Ok(self
            .meta
            .scan_prefix(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &admission_record_prefix(),
            )?
            .len() as u64)
    }

    pub(super) async fn pending_mutation_bytes(&self) -> Result<u64> {
        Ok(self
            .meta
            .scan_prefix(
                CF_TRANSACTIONS,
                TABLE_PENDING_MUTATION_ROW,
                &admission_record_prefix(),
            )?
            .into_iter()
            .map(|record| record.payload.len() as u64)
            .sum())
    }

    pub(super) async fn admission_landed_bytes(&self) -> Result<u64> {
        sum_files_with_extension(&self.admission_landed_bytes_root(), &["landed"])
            .await
            .with_context(|| {
                format!(
                    "measure CoreStore admission landed bytes under {}",
                    self.admission_landed_bytes_root().display()
                )
            })
    }

    pub(super) async fn admission_materialisation_lag_seconds(&self) -> Result<Option<u64>> {
        let records = self.read_pending_mutation_records().await?;
        if records.is_empty() {
            return Ok(None);
        }

        let finalised = self.read_pending_mutation_finalisation_keys().await?;
        let oldest_unfinalised = records
            .iter()
            .filter(|record| !finalised.contains(&CorePendingMutationKey::from(*record)))
            .map(|record| record.created_at_unix_nanos)
            .min();

        let Some(oldest_unfinalised) = oldest_unfinalised else {
            return Ok(None);
        };

        let now = unix_timestamp_nanos();
        let lag_nanos = now.saturating_sub(oldest_unfinalised);
        Ok(Some(lag_nanos / 1_000_000_000))
    }

    pub(super) async fn read_pending_mutation_records(
        &self,
    ) -> Result<Vec<CorePendingMutationRecord>> {
        self.read_pending_mutation_records_with_payload()
            .await
            .map(|records| {
                records
                    .into_iter()
                    .map(|(record, _payload)| record)
                    .collect()
            })
    }

    pub(super) async fn read_pending_mutation_records_with_payload(
        &self,
    ) -> Result<Vec<(CorePendingMutationRecord, Vec<u8>)>> {
        let mut out = Vec::new();
        let prefix = admission_record_prefix();
        for item in self
            .meta
            .scan_prefix(CF_TRANSACTIONS, TABLE_PENDING_MUTATION_ROW, &prefix)?
        {
            out.push(decode_stored_pending_mutation_row(&item.payload)?);
        }
        out.sort_by_key(|(record, _)| record.sequence);
        Ok(out)
    }

    pub(super) async fn read_pending_mutation_finalisation_keys(
        &self,
    ) -> Result<BTreeSet<CorePendingMutationKey>> {
        let mut keys = BTreeSet::new();
        for item in self.meta.scan_prefix(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &admission_finalisation_prefix(),
        )? {
            let finalisation = decode_pending_mutation_finalisation_index_row(&item.payload)?;
            if finalisation.schema != CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA {
                bail!("CoreStore pending mutation finalisation index row has invalid schema");
            }
            keys.insert(CorePendingMutationKey {
                node_id: finalisation.node_id,
                mutation_epoch: finalisation.mutation_epoch,
                mutation_sequence: finalisation.mutation_sequence,
            });
        }
        Ok(keys)
    }

    pub(super) fn read_pending_mutation_finalisation_record(
        &self,
        key: &CorePendingMutationKey,
    ) -> Result<Option<CorePendingMutationFinalisationRecord>> {
        self.meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_finalisation_record_key(key),
            )?
            .map(|bytes| decode_pending_mutation_finalisation_record(&bytes))
            .transpose()
    }

    pub(super) async fn mark_pending_mutation_finalised_unlocked(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
    ) -> Result<()> {
        self.mark_pending_mutation_finalised_with_result_unlocked(admission, state, None)
            .await
    }

    pub(super) async fn mark_pending_mutation_finalised_with_result_unlocked(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
        result: Option<CorePendingMutationFinalisationResult>,
    ) -> Result<()> {
        self.mark_pending_mutation_finalised_with_result_and_ops_unlocked(
            admission,
            state,
            result,
            Vec::new(),
        )
        .await
    }

    pub(super) async fn mark_pending_mutation_finalised_with_result_and_ops_unlocked(
        &self,
        admission: &CorePendingMutationRecord,
        state: &str,
        result: Option<CorePendingMutationFinalisationResult>,
        mut preceding_ops: Vec<OwnedCoreMetaBatchOp>,
    ) -> Result<()> {
        let admission_key = CorePendingMutationKey::from(admission);
        let finalisation_lock_id = format!(
            "{}:{}:{}",
            admission_key.node_id, admission_key.mutation_epoch, admission_key.mutation_sequence
        );
        let _finalisation_guard = self
            .acquire_named_lock("pending_mutation_finalisation", &finalisation_lock_id)
            .await?;
        let result_hash = finalisation_result_hash(&result)?;
        let index_key = admission_finalisation_key(&admission_key);
        if let Some(existing_bytes) = self.meta.get(
            CF_MATERIALISATION,
            TABLE_MATERIALISATION_CURSOR_ROW,
            &index_key,
        )? {
            let existing = decode_pending_mutation_finalisation_index_row(&existing_bytes)?;
            if existing.schema != CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA {
                bail!("CoreStore pending mutation finalisation index row has invalid schema");
            }
            if existing.mutation_id == admission.mutation_id
                && existing.state == state
                && existing.result_hash == result_hash
            {
                if !preceding_ops.is_empty() {
                    let ops = borrow_owned_coremeta_batch_ops(&preceding_ops);
                    self.commit_coremeta_batch_by_embedded_roots(&admission.mutation_id, &ops)
                        .await?;
                }
                if let Some(record) =
                    self.read_pending_mutation_finalisation_record(&admission_key)?
                {
                    if record.mutation_id != admission.mutation_id
                        || record.state != state
                        || finalisation_result_hash(&record.result)? != result_hash
                    {
                        bail!(
                            "CoreStore pending mutation finalisation record/index mismatch for sequence {}",
                            admission.sequence
                        );
                    }
                    let payload = encode_pending_mutation_finalisation_record(&record)?;
                    self.append_pending_mutation_finalisation_transaction_record(&record, &payload)
                        .await?;
                }
                self.checkpoint_pending_mutations_unlocked().await?;
                return Ok(());
            }
            bail!(
                "CoreStore pending mutation finalisation conflict for sequence {}: existing mutation/state {}/{}, new mutation/state {}/{}",
                admission.sequence,
                existing.mutation_id,
                existing.state,
                admission.mutation_id,
                state
            );
        }
        let _order_guard = self
            .acquire_pending_mutation_finalisation_turn(admission)
            .await?;
        let finalisation = CorePendingMutationFinalisationRecord {
            schema: CORE_PENDING_MUTATION_FINALISATION_SCHEMA.to_string(),
            node_id: admission.node_id.clone(),
            mutation_epoch: admission.mutation_epoch,
            mutation_sequence: admission.sequence,
            mutation_id: admission.mutation_id.clone(),
            operation_family: admission.operation_family.clone(),
            writer_family: admission.writer_family.clone(),
            target: admission.target.clone(),
            boundary_values: admission.boundary_values.clone(),
            landed_bytes: admission.landed_bytes.clone(),
            state: state.to_string(),
            result,
            finalised_at_unix_nanos: unix_timestamp_nanos(),
        };
        let finalisation_index = CorePendingMutationFinalisationIndexRow {
            schema: CORE_PENDING_MUTATION_FINALISATION_INDEX_SCHEMA.to_string(),
            node_id: admission.node_id.clone(),
            mutation_epoch: admission.mutation_epoch,
            mutation_sequence: admission.sequence,
            mutation_id: admission.mutation_id.clone(),
            state: state.to_string(),
            result_hash,
        };
        let record_key = admission_finalisation_record_key(&admission_key);
        let finalisation_payload = encode_pending_mutation_finalisation_record(&finalisation)?;
        let finalisation_index_payload =
            encode_pending_mutation_finalisation_index_row(&finalisation_index)?;
        preceding_ops.extend([
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: record_key,
                payload: finalisation_payload.clone(),
                common: None,
            },
            OwnedCoreMetaBatchOp::Put {
                cf: CF_MATERIALISATION,
                table_id: TABLE_MATERIALISATION_CURSOR_ROW,
                tuple_key: index_key,
                payload: finalisation_index_payload,
                common: None,
            },
        ]);
        let cleanup_common = core_meta_committed_row_common(
            "system",
            root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
            admission.sequence,
            admission.mutation_id.clone(),
            unix_timestamp_nanos(),
        );
        preceding_ops.extend([
            OwnedCoreMetaBatchOp::Delete {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_PENDING_MUTATION_ROW,
                tuple_key: admission_record_key(admission.sequence),
                common: Some(cleanup_common.clone()),
            },
            OwnedCoreMetaBatchOp::Delete {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW,
                tuple_key: admission_certificate_key(admission.sequence),
                common: Some(cleanup_common.clone()),
            },
        ]);
        for landed in &admission.landed_bytes {
            preceding_ops.push(OwnedCoreMetaBatchOp::Delete {
                cf: CF_MATERIALISATION,
                table_id: TABLE_LANDED_BYTE_REF_ROW,
                tuple_key: meta_tuple_key(&[b"landed-byte", landed.landing_id.as_bytes()]),
                common: Some(cleanup_common.clone()),
            });
        }
        let finalisation_idempotency_key = format!(
            "pending-finalisation:{}:{}:{}",
            finalisation.node_id, finalisation.mutation_epoch, finalisation.mutation_sequence
        );
        let finalisation_idempotency_hash = format!(
            "sha256:{}",
            sha256_hex(finalisation_idempotency_key.as_bytes())
        );
        let _stream_guard = self
            .acquire_named_lock("stream", CORE_TRANSACTION_STREAM_ID)
            .await?;
        let prepared_stream = Box::pin(self.prepare_stream_append_unlocked_with_idempotency_hash(
            AppendStreamRecord {
                stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
                partition_id: "system/core-control".to_string(),
                record_kind: CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND.to_string(),
                payload: finalisation_payload.clone(),
                content_type: Some("application/protobuf".to_string()),
                user_metadata_json: "{}".to_string(),
                fence: None,
                transaction_id: Some(finalisation.mutation_id.clone()),
                idempotency_key: Some(finalisation_idempotency_key),
            },
            Some(finalisation_idempotency_hash),
        ))
        .await?;
        let combine_stream_metadata = prepared_stream
            .record
            .as_ref()
            .is_some_and(|record| record.sequence == admission.sequence);
        let stream_transaction_id = prepared_stream.metadata.transaction_id.clone();
        let mut stream_metadata_ops = prepared_stream.metadata.owned_ops;
        if combine_stream_metadata {
            preceding_ops.append(&mut stream_metadata_ops);
        }
        let finalisation_ops = borrow_owned_coremeta_batch_ops(&preceding_ops);
        let step_started_at = Instant::now();
        let metadata_commits = self
            .commit_coremeta_batch_by_embedded_roots(&admission.mutation_id, &finalisation_ops)
            .await?;
        crate::emit_test_timing(
            "core_store.pending_finalisation write_rows",
            step_started_at.elapsed(),
        );
        let step_started_at = Instant::now();
        if let Some(record) = prepared_stream.record.as_ref() {
            let stream_metadata_commits = if combine_stream_metadata {
                metadata_commits
            } else {
                let ops = borrow_owned_coremeta_batch_ops(&stream_metadata_ops);
                self.commit_coremeta_batch_by_embedded_roots(&stream_transaction_id, &ops)
                    .await?
            };
            self.write_core_transaction_stream_records(
                std::slice::from_ref(record),
                &stream_metadata_commits,
            )
            .await?;
        }
        self.remove_finalised_landed_byte_files_after_metadata_cleanup(admission)
            .await?;
        crate::emit_test_timing(
            "core_store.pending_finalisation append_transaction_record",
            step_started_at.elapsed(),
        );
        Ok(())
    }

    async fn acquire_pending_mutation_finalisation_turn(
        &self,
        admission: &CorePendingMutationRecord,
    ) -> Result<CoreStoreLock> {
        let expected_previous_sequence = admission.sequence.saturating_sub(1);
        for _ in 0..CORE_PROCESS_LOCK_RETRY_ATTEMPTS {
            let guard = self
                .acquire_named_lock("pending_mutation_finalisation_order", "core_transaction")
                .await?;
            let stream_head = self.read_stream_head_from_meta(CORE_TRANSACTION_STREAM_ID)?;
            let current_sequence = stream_head
                .as_ref()
                .map(|head| head.last_sequence)
                .unwrap_or(0);
            if current_sequence == expected_previous_sequence {
                return Ok(guard);
            }
            if current_sequence >= admission.sequence {
                bail!(
                    "CoreStore pending mutation finalisation sequence {} is behind transaction stream head {}",
                    admission.sequence,
                    current_sequence
                );
            }
            drop(guard);
            tokio::time::sleep(CORE_PROCESS_LOCK_RETRY_DELAY).await;
        }
        bail!(
            "CoreStore pending mutation finalisation sequence {} did not reach its transaction stream turn",
            admission.sequence
        )
    }

    async fn remove_finalised_landed_byte_files_after_metadata_cleanup(
        &self,
        admission: &CorePendingMutationRecord,
    ) -> Result<()> {
        for landed in &admission.landed_bytes {
            let landed_hash = strip_sha256_prefix(&landed.sha256)?.to_string();
            let _landed_guard = self
                .acquire_named_lock("landed-bytes", &landed_hash)
                .await?;
            if self.landed_bytes_has_live_references(&landed.relative_path)? {
                continue;
            }
            let landed_path = self
                .storage
                .resolve_relative_storage_path(&landed.relative_path)?;
            match fs::remove_file(&landed_path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!(
                            "remove finalised CoreStore landed bytes {}",
                            landed_path.display()
                        )
                    });
                }
            }
        }
        Ok(())
    }

    async fn append_pending_mutation_finalisation_transaction_record(
        &self,
        finalisation: &CorePendingMutationFinalisationRecord,
        payload: &[u8],
    ) -> Result<()> {
        // Finalisations from unrelated mutations can run concurrently. They
        // still append to one root-anchored stream, so sequence allocation and
        // root publication must share the same cross-process stream lock as a
        // public append.
        let _stream_guard = self
            .acquire_named_lock("stream", CORE_TRANSACTION_STREAM_ID)
            .await?;
        Box::pin(self.append_stream_unlocked(AppendStreamRecord {
            stream_id: CORE_TRANSACTION_STREAM_ID.to_string(),
            partition_id: "system/core-control".to_string(),
            record_kind: CORE_PENDING_MUTATION_FINALISATION_RECORD_KIND.to_string(),
            payload: payload.to_vec(),
            content_type: Some("application/protobuf".to_string()),
            user_metadata_json: "{}".to_string(),
            fence: None,
            transaction_id: Some(finalisation.mutation_id.clone()),
            idempotency_key: Some(format!(
                "pending-finalisation:{}:{}:{}",
                finalisation.node_id, finalisation.mutation_epoch, finalisation.mutation_sequence
            )),
        }))
        .await?;
        Ok(())
    }

    pub(super) async fn checkpoint_pending_mutations_unlocked(&self) -> Result<()> {
        let _pending_mutation_guard = self.acquire_named_lock("admission", "active").await?;
        let records = self.read_pending_mutation_records().await?;
        if records.is_empty() {
            return Ok(());
        }
        let finalised = self.read_pending_mutation_finalisation_keys().await?;
        for record in records {
            let key = CorePendingMutationKey::from(&record);
            if !finalised.contains(&key) {
                continue;
            }
            self.remove_finalised_landed_bytes(&record).await?;
            let pending_key = admission_record_key(record.sequence);
            let certificate_key = admission_certificate_key(record.sequence);
            let common = core_meta_committed_row_common(
                "system",
                root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
                record.sequence,
                record.mutation_id.clone(),
                unix_timestamp_nanos(),
            );
            self.commit_coremeta_batch_by_embedded_roots(
                &record.mutation_id,
                &[
                    CoreMetaBatchOp {
                        cf: CF_TRANSACTIONS,
                        table_id: TABLE_PENDING_MUTATION_ROW,
                        tuple_key: &pending_key,
                        common: Some(common.clone()),
                        kind: CoreMetaBatchOpKind::Delete,
                    },
                    CoreMetaBatchOp {
                        cf: CF_TRANSACTIONS,
                        table_id: TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW,
                        tuple_key: &certificate_key,
                        common: Some(common),
                        kind: CoreMetaBatchOpKind::Delete,
                    },
                ],
            )
            .await?;
        }
        Ok(())
    }

    pub(super) async fn remove_finalised_landed_bytes(
        &self,
        record: &CorePendingMutationRecord,
    ) -> Result<()> {
        for landed in &record.landed_bytes {
            let landed_hash = strip_sha256_prefix(&landed.sha256)?.to_string();
            let _landed_guard = self
                .acquire_named_lock("landed-bytes", &landed_hash)
                .await?;
            let landed_key = meta_tuple_key(&[b"landed-byte", landed.landing_id.as_bytes()]);
            let common = core_meta_committed_row_common(
                "system",
                root_key_hash(CORE_TRANSACTION_ROOT_ANCHOR_KEY),
                record.sequence,
                record.mutation_id.clone(),
                unix_timestamp_nanos(),
            );
            self.commit_coremeta_batch_by_embedded_roots(
                &record.mutation_id,
                &[CoreMetaBatchOp {
                    cf: CF_MATERIALISATION,
                    table_id: TABLE_LANDED_BYTE_REF_ROW,
                    tuple_key: &landed_key,
                    common: Some(common),
                    kind: CoreMetaBatchOpKind::Delete,
                }],
            )
            .await?;
            if self.landed_bytes_has_live_references(&landed.relative_path)? {
                continue;
            }
            let landed_path = self
                .storage
                .resolve_relative_storage_path(&landed.relative_path)?;
            match fs::remove_file(&landed_path).await {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => {
                    return Err(err).with_context(|| {
                        format!(
                            "remove finalised CoreStore landed bytes {}",
                            landed_path.display()
                        )
                    });
                }
            }
        }
        Ok(())
    }

    pub(super) async fn read_landed_bytes(
        &self,
        landed: &CorePendingLandedByte,
    ) -> Result<Vec<u8>> {
        validate_hash(&landed.sha256, "landed bytes hash")?;
        let path = self
            .storage
            .resolve_relative_storage_path(&landed.relative_path)?;
        let bytes = read_file(&path, "core_store", "read_landed_bytes")
            .await
            .with_context(|| {
                format!(
                    "read CoreStore landed bytes landing_id={} relative_path={} resolved_path={}",
                    landed.landing_id,
                    landed.relative_path,
                    path.display()
                )
            })?;
        if bytes.len() as u64 != landed.length {
            bail!("CoreStore landed bytes length mismatch");
        }
        let actual = format!("sha256:{}", sha256_hex(&bytes));
        if actual != landed.sha256 {
            bail!("CoreStore landed bytes hash mismatch");
        }
        self.verify_landed_bytes_ref_row(
            &landed.landing_id,
            landed
                .landing_id
                .rsplit_once(':')
                .map(|(mutation_id, _)| mutation_id)
                .unwrap_or_default(),
            &landed.sha256,
            landed.length,
            &[],
        )?;
        Ok(bytes)
    }

    pub(super) async fn recover_pending_mutations(&self) -> Result<()> {
        let recovery_started_at = Instant::now();
        let _recovery_guard = self
            .acquire_named_lock("pending_mutation", "recovery")
            .await?;
        let _guard = self.write_lock.lock().await;
        let result = async {
            let records = self.read_pending_mutation_records_with_payload().await?;
            let referenced_landed_bytes = referenced_landed_byte_paths(&records);
            self.reconcile_landed_bytes_after_rocksdb_recovery(&referenced_landed_bytes)
                .await?;
            if records.is_empty() {
                return Ok(());
            }
            let finalised = self.read_pending_mutation_finalisation_keys().await?;
            for (record, payload) in records {
                let record_key = CorePendingMutationKey::from(&record);
                if finalised.contains(&record_key) {
                    continue;
                }
                let pending_mutation_hash_input =
                    encode_pending_mutation_hash_input(&record, &payload)?;
                if let Err(error) = self
                    .verify_local_pending_mutation_commit_certificate(
                        &record,
                        &pending_mutation_hash_input,
                    )
                    .await
                {
                    if self
                        .wait_for_pending_mutation_finalisation(&record_key)
                        .await?
                    {
                        continue;
                    }
                    return Err(error);
                }
                let replay = match self
                    .replay_pending_mutation_record_unlocked(&record, &payload)
                    .await
                {
                    Ok(replay) => replay,
                    Err(error) => {
                        if self
                            .wait_for_pending_mutation_finalisation(&record_key)
                            .await?
                        {
                            continue;
                        }
                        return Err(error).with_context(|| {
                            format!(
                                "replay CoreStore pending mutation mutation {} sequence {}",
                                record.mutation_id, record.sequence
                            )
                        });
                    }
                };
                if let Err(error) = self
                    .mark_pending_mutation_finalised_with_result_unlocked(
                        &record,
                        replay.state,
                        replay.result,
                    )
                    .await
                {
                    if self
                        .wait_for_pending_mutation_finalisation(&record_key)
                        .await?
                    {
                        continue;
                    }
                    return Err(error);
                }
            }
            Ok(())
        }
        .await;
        crate::perf::record_recovery_duration(
            "pending_mutations",
            if result.is_ok() { "ok" } else { "error" },
            recovery_started_at.elapsed(),
        );
        result
    }

    pub(super) async fn wait_for_pending_mutation_finalisation(
        &self,
        key: &CorePendingMutationKey,
    ) -> Result<bool> {
        for _ in 0..CORE_CONTROL_READ_RETRY_ATTEMPTS {
            if self
                .read_pending_mutation_finalisation_keys()
                .await?
                .contains(key)
            {
                return Ok(true);
            }
            tokio::time::sleep(CORE_PROCESS_LOCK_RETRY_DELAY).await;
        }
        Ok(false)
    }

    pub(super) async fn replay_pending_mutation_record_unlocked(
        &self,
        record: &CorePendingMutationRecord,
        payload: &[u8],
    ) -> Result<CorePendingMutationReplayOutcome> {
        match &record.target {
            CorePendingMutationTarget::ObjectPut {
                logical_name,
                erasure_profile_id,
                encryption,
                block_plain_hash,
                object_hash,
                object_logical_size,
                compression,
                writer_generation,
                block_ordinal,
                region_id: _,
            } => {
                if record.operation_family != "object.put" {
                    bail!("CoreStore pending mutation target/family mismatch for object.put");
                }
                let profile = local_erasure_profile(erasure_profile_id)?;
                let materialised_bytes =
                    self.pending_mutation_payload_bytes(record, payload).await?;
                let hash = sha256_hex(&materialised_bytes);
                if let Some(landed) = record.landed_bytes.first() {
                    let landed_hash = strip_sha256_prefix(&landed.sha256)?;
                    if landed_hash != hash {
                        bail!("CoreStore pending mutation object.put landed hash mismatch");
                    }
                }
                let writer = WriterFamily::from_name(&record.writer_family).ok_or_else(|| {
                    anyhow!(
                        "CoreStore pending mutation unknown writer family {}",
                        record.writer_family
                    )
                })?;
                let replay_logical_file_id =
                    if super::local_logical_files::is_canonical_logical_file_id(logical_name) {
                        logical_name.clone()
                    } else {
                        canonical_logical_file_id(
                            writer,
                            *writer_generation,
                            logical_name,
                            &hash32(&materialised_bytes),
                        )
                    };
                self.materialise_object_blob_bytes(
                    &replay_logical_file_id,
                    *writer_generation,
                    *block_ordinal,
                    block_plain_hash,
                    &hash,
                    &materialised_bytes,
                    object_hash,
                    *object_logical_size,
                    compression.clone(),
                    &record.boundary_values,
                    &record.mutation_id,
                    profile,
                    encryption,
                    &record.writer_family,
                )
                .await?;
                Ok(CorePendingMutationReplayOutcome {
                    state: "committed",
                    result: None,
                })
            }
            CorePendingMutationTarget::StreamAppend {
                stream_id,
                partition_id,
                record_kind,
                transaction_id,
            } => {
                if record.operation_family != "stream.append" {
                    bail!("CoreStore pending mutation target/family mismatch for stream.append");
                }
                let payload = self.pending_mutation_payload_bytes(record, payload).await?;
                let outcome = self
                    .append_stream_unlocked_with_idempotency_hash(
                        AppendStreamRecord {
                            stream_id: stream_id.clone(),
                            partition_id: partition_id.clone(),
                            record_kind: record_kind.clone(),
                            payload,
                            content_type: None,
                            user_metadata_json: "{}".to_string(),
                            fence: None,
                            transaction_id: transaction_id.clone(),
                            idempotency_key: None,
                        },
                        record.idempotency_key_hash.clone(),
                    )
                    .await?;
                Ok(CorePendingMutationReplayOutcome {
                    state: "committed",
                    result: outcome.state_locator.as_ref().map(|locator| {
                        CorePendingMutationFinalisationResult::StreamStateLocator(locator.clone())
                    }),
                })
            }
            CorePendingMutationTarget::MutationBatch { .. } => {
                if record.operation_family != "mutation.batch" {
                    bail!("CoreStore pending mutation target/family mismatch for mutation.batch");
                }
                let payload = self.pending_mutation_payload_bytes(record, payload).await?;
                let batch = decode_core_mutation_batch(&payload)?;
                let receipt = self.recover_admitted_mutation_batch_unlocked(batch).await?;
                Ok(CorePendingMutationReplayOutcome {
                    state: core_transaction_state_name(receipt.state),
                    result: None,
                })
            }
        }
    }

    async fn reconcile_landed_bytes_after_rocksdb_recovery(
        &self,
        referenced_relative_paths: &BTreeSet<String>,
    ) -> Result<()> {
        let files = collect_landed_byte_files(&self.storage.core_store_landed_bytes_path()).await?;
        for path in files {
            let relative = self.storage.relative_storage_path(&path)?;
            if referenced_relative_paths.contains(&relative) {
                let actual = read_file(&path, "core_store", "landed_recovery_read").await?;
                let expected_hash = format!("sha256:{}", sha256_hex(&actual));
                let expected_len = actual.len() as u64;
                self.verify_referenced_landed_bytes_row(&relative, &expected_hash, expected_len)?;
                continue;
            }
            quarantine_landed_bytes_file(&path).await?;
        }
        Ok(())
    }

    pub(super) async fn pending_mutation_payload_bytes(
        &self,
        record: &CorePendingMutationRecord,
        payload: &[u8],
    ) -> Result<Vec<u8>> {
        if !payload.is_empty() {
            return Ok(payload.to_vec());
        }
        let mut bytes = Vec::new();
        for landed in &record.landed_bytes {
            bytes.extend_from_slice(&self.read_landed_bytes(landed).await?);
        }
        Ok(bytes)
    }

    pub(super) async fn next_core_mutation_sequence(&self) -> Result<u64> {
        let persisted = self
            .meta
            .get(
                CF_MATERIALISATION,
                TABLE_MATERIALISATION_CURSOR_ROW,
                &admission_sequence_key(),
            )?
            .map(|bytes| decode_materialisation_cursor_row(&bytes))
            .transpose()?
            .unwrap_or(0);
        let active = self
            .read_pending_mutation_records()
            .await?
            .into_iter()
            .map(|record| record.sequence)
            .max()
            .unwrap_or(0);
        Ok(persisted.max(active).saturating_add(1))
    }
}

fn referenced_landed_byte_paths(
    records: &[(CorePendingMutationRecord, Vec<u8>)],
) -> BTreeSet<String> {
    records
        .iter()
        .flat_map(|(record, _)| record.landed_bytes.iter())
        .map(|landed| landed.relative_path.clone())
        .collect()
}

impl CoreStore {
    fn landed_bytes_has_live_references(&self, relative_path: &str) -> Result<bool> {
        let rows = self.meta.scan_prefix(
            CF_MATERIALISATION,
            TABLE_LANDED_BYTE_REF_ROW,
            &meta_tuple_key(&[b"landed-byte"]),
        )?;
        for record in rows {
            let row = decode_landed_byte_ref_row(&record.payload)?;
            if row.landed.relative_path == relative_path {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(super) fn verify_landed_bytes_ref_row(
        &self,
        landing_id: &str,
        mutation_id: &str,
        sha256: &str,
        length: u64,
        boundary_values: &[CoreBoundaryValue],
    ) -> Result<()> {
        let landed_key = meta_tuple_key(&[b"landed-byte", landing_id.as_bytes()]);
        let Some(bytes) =
            self.meta
                .get(CF_MATERIALISATION, TABLE_LANDED_BYTE_REF_ROW, &landed_key)?
        else {
            bail!("CoreStore landed byte CoreMeta row is missing");
        };
        let row = decode_landed_byte_ref_row(&bytes)?;
        if !mutation_id.is_empty() && row.mutation_id != mutation_id {
            bail!("CoreStore landed byte CoreMeta mutation id mismatch");
        }
        if row.landed.landing_id != landing_id {
            bail!("CoreStore landed byte CoreMeta landing id mismatch");
        }
        if row.landed.sha256 != sha256 {
            bail!("CoreStore landed byte CoreMeta hash mismatch");
        }
        if row.landed.length != length {
            bail!("CoreStore landed byte CoreMeta length mismatch");
        }
        if !boundary_values.is_empty()
            && boundary_summary_hash(&row.boundary_values)?
                != boundary_summary_hash(boundary_values)?
        {
            bail!("CoreStore landed byte CoreMeta boundary summary mismatch");
        }
        Ok(())
    }

    fn verify_referenced_landed_bytes_row(
        &self,
        relative_path: &str,
        sha256: &str,
        length: u64,
    ) -> Result<()> {
        let rows = self.meta.scan_prefix(
            CF_MATERIALISATION,
            TABLE_LANDED_BYTE_REF_ROW,
            &meta_tuple_key(&[b"landed-byte"]),
        )?;
        for record in rows {
            let row = decode_landed_byte_ref_row(&record.payload)?;
            if row.landed.relative_path != relative_path {
                continue;
            }
            if row.landed.sha256 != sha256 || row.landed.length != length {
                bail!("CoreStore referenced landed bytes failed recovery validation");
            }
            return Ok(());
        }
        bail!("CoreStore referenced landed byte CoreMeta row is missing")
    }
}

async fn collect_landed_byte_files(root: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut pending = vec![root.clone()];
    while let Some(dir) = pending.pop() {
        let mut entries = match fs::read_dir(&dir).await {
            Ok(entries) => entries,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("read landed bytes dir {}", dir.display()));
            }
        };
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                pending.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "landed")
            {
                files.push(path);
            }
        }
    }
    Ok(files)
}

async fn quarantine_landed_bytes_file(path: &PathBuf) -> Result<()> {
    let quarantine_path = path.with_extension(format!(
        "{}.quarantine",
        path.extension()
            .and_then(|extension| extension.to_str())
            .unwrap_or("landed")
    ));
    match fs::rename(path, &quarantine_path).await {
        Ok(()) => {
            sync_parent_dir(&quarantine_path, "landed_quarantine_sync_parent_dir").await?;
            Ok(())
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "quarantine CoreStore landed bytes {} -> {}",
                path.display(),
                quarantine_path.display()
            )
        }),
    }
}
