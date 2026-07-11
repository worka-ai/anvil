use super::*;
use crate::formats::writer::WriterFamily;
use prost::{Message, Oneof};

const CORE_TRANSACTION_HEADER_ROW_SCHEMA: &str = "anvil.core.transaction_header_row.v1";
const CORE_TRANSACTION_STAGED_UPDATE_ROW_SCHEMA: &str =
    "anvil.core.transaction_staged_update_row.v1";
const CORE_TRANSACTION_PRECONDITION_ROW_SCHEMA: &str = "anvil.core.transaction_precondition_row.v1";
// Transaction rows may retain tiny CoreMeta payloads for recovery, but larger
// staged payloads must stay in the CoreStore byte pipeline as locators.
const CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES: usize = 16 * 1024;

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionHeaderRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(string, tag = "4")]
    scope_partition: String,
    #[prost(enumeration = "CoreTransactionHeaderStateProto", tag = "5")]
    state: i32,
    #[prost(string, tag = "6")]
    preconditions_hash: String,
    #[prost(string, tag = "7")]
    operations_hash: String,
    #[prost(uint64, tag = "8")]
    visible_update_count: u64,
    #[prost(uint64, tag = "9")]
    precondition_count: u64,
    #[prost(string, optional, tag = "10")]
    finalisation_error: Option<String>,
    #[prost(string, tag = "11")]
    committed_at: String,
    #[prost(string, tag = "12")]
    committed_by_principal: String,
    #[prost(uint64, tag = "13")]
    created_at_unix_nanos: u64,
    #[prost(uint64, tag = "14")]
    expires_at_unix_nanos: u64,
    #[prost(string, tag = "15")]
    root_anchor_key: String,
    #[prost(string, tag = "16")]
    root_key_hash: String,
    #[prost(uint64, optional, tag = "17")]
    committed_root_generation: Option<u64>,
    #[prost(string, tag = "18")]
    purpose: String,
    #[prost(string, optional, tag = "19")]
    failure_evidence: Option<String>,
    #[prost(string, tag = "20")]
    outcome: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ::prost::Enumeration)]
enum CoreTransactionHeaderStateProto {
    Unspecified = 0,
    Open = 1,
    Prepared = 2,
    Committed = 3,
    FinalisationFailed = 4,
    Aborted = 5,
    RolledBack = 6,
    Expired = 7,
    Failed = 8,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionStagedUpdateRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(uint64, tag = "4")]
    ordinal: u64,
    #[prost(message, optional, tag = "5")]
    update: Option<CoreTransactionUpdateRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionUpdateRowProto {
    #[prost(oneof = "transaction_update_row_proto::Kind", tags = "2, 3, 4")]
    kind: Option<transaction_update_row_proto::Kind>,
}

mod transaction_update_row_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "2")]
        StreamAppend(super::CoreTransactionStreamAppendRowProto),
        #[prost(message, tag = "3")]
        CoreMetaPut(super::CoreTransactionCoreMetaPutRowProto),
        #[prost(message, tag = "4")]
        CoreMetaDelete(super::CoreTransactionCoreMetaDeleteRowProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionStreamAppendRowProto {
    #[prost(string, tag = "1")]
    stream_id: String,
    #[prost(uint64, tag = "2")]
    visible_sequence: u64,
    #[prost(string, tag = "3")]
    prepared_record_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionCoreMetaPutRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    previous_payload_hash: Option<String>,
    #[prost(string, tag = "5")]
    payload_hash: String,
    #[prost(message, optional, tag = "6")]
    payload_ref: Option<CoreTransactionPayloadRefProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionCoreMetaDeleteRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    previous_payload_hash: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionPayloadRefProto {
    #[prost(bytes, optional, tag = "1")]
    inline_payload: Option<Vec<u8>>,
    #[prost(message, optional, tag = "2")]
    locator: Option<CoreMetaLocatorProto>,
    #[prost(string, tag = "3")]
    payload_sha256: String,
    #[prost(uint64, tag = "4")]
    payload_length: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreTransactionPreconditionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    transaction_id: String,
    #[prost(uint64, tag = "4")]
    ordinal: u64,
    #[prost(message, optional, tag = "5")]
    precondition: Option<CoreMutationPreconditionRowProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationPreconditionRowProto {
    #[prost(oneof = "mutation_precondition_row_proto::Kind", tags = "2, 3, 4")]
    kind: Option<mutation_precondition_row_proto::Kind>,
}

mod mutation_precondition_row_proto {
    use super::*;

    #[derive(Clone, PartialEq, Oneof)]
    pub(super) enum Kind {
        #[prost(message, tag = "2")]
        Fence(super::CoreMutationFencePreconditionRowProto),
        #[prost(message, tag = "3")]
        StreamHead(super::CoreMutationStreamHeadPreconditionRowProto),
        #[prost(message, tag = "4")]
        CoreMetaRow(super::CoreMutationCoreMetaRowPreconditionRowProto),
    }
}

#[derive(Clone, PartialEq, Message)]
struct CoreFencePreconditionRowProto {
    #[prost(string, tag = "1")]
    fence_name: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
    #[prost(string, tag = "3")]
    authenticated_principal: String,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationFencePreconditionRowProto {
    #[prost(string, tag = "1")]
    fence_name: String,
    #[prost(uint64, tag = "2")]
    fence_token: u64,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationCoreMetaRowPreconditionRowProto {
    #[prost(string, tag = "1")]
    cf: String,
    #[prost(uint32, tag = "2")]
    table_id: u32,
    #[prost(bytes, tag = "3")]
    tuple_key: Vec<u8>,
    #[prost(string, optional, tag = "4")]
    expected_payload_hash: Option<String>,
    #[prost(bool, tag = "5")]
    require_absent: bool,
    #[prost(bool, tag = "6")]
    require_present: bool,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMutationStreamHeadPreconditionRowProto {
    #[prost(string, tag = "1")]
    stream_id: String,
    #[prost(uint64, tag = "2")]
    expected_last_sequence: u64,
    #[prost(string, tag = "3")]
    expected_last_event_hash: String,
}

pub(super) struct CoreTransactionHeaderRow {
    pub transaction: CoreTransaction,
    pub visible_update_count: u64,
    pub precondition_count: u64,
}

pub(super) enum CoreTransactionPayloadRef {
    Inline {
        payload: Vec<u8>,
        payload_sha256: String,
        payload_length: u64,
    },
    Locator {
        locator: CoreMetaLocatorProto,
        payload_sha256: String,
        payload_length: u64,
    },
}

pub(super) enum CoreTransactionUpdateRow {
    StreamAppend {
        stream_id: String,
        visible_sequence: u64,
        prepared_record_hash: String,
    },
    CoreMetaPut {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        previous_payload_hash: Option<String>,
        payload_hash: String,
        payload_ref: CoreTransactionPayloadRef,
    },
    CoreMetaDelete {
        cf: String,
        table_id: u16,
        tuple_key: Vec<u8>,
        previous_payload_hash: Option<String>,
    },
}

pub(super) struct CoreTransactionStagedUpdateRow {
    pub ordinal: u64,
    pub update: CoreTransactionUpdateRow,
}

pub(super) enum OwnedCoreMetaBatchOp {
    Put {
        cf: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
    },
    Delete {
        cf: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
    },
}

impl CoreStore {
    pub(super) async fn write_transaction_with_staged_rows_unlocked(
        &self,
        transaction: &CoreTransaction,
        new_preconditions: &[CoreMutationPrecondition],
    ) -> Result<()> {
        let owned_ops = self
            .transaction_rows_as_coremeta_ops_unlocked(transaction, new_preconditions)
            .await?;
        let ops = borrow_owned_coremeta_batch_ops(&owned_ops);
        self.commit_coremeta_batch_by_embedded_roots(
            &format!("explicit-transaction:{}", transaction.transaction_id),
            &ops,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn transaction_rows_as_coremeta_ops_unlocked(
        &self,
        transaction: &CoreTransaction,
        new_preconditions: &[CoreMutationPrecondition],
    ) -> Result<Vec<OwnedCoreMetaBatchOp>> {
        validate_transaction_root_scope(transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let existing_header =
            self.read_transaction_header_row_unlocked(&transaction.transaction_id)?;
        let existing_update_count = existing_header
            .as_ref()
            .map(|header| header.visible_update_count)
            .unwrap_or(0);
        let existing_precondition_count = existing_header
            .as_ref()
            .map(|header| header.precondition_count)
            .unwrap_or(0);
        let visible_update_count = u64::try_from(transaction.visible_updates.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
        let new_precondition_count = u64::try_from(new_preconditions.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged preconditions"))?;
        let precondition_count = existing_precondition_count.saturating_add(new_precondition_count);

        let header_key = transaction_header_tuple_key(&transaction.transaction_id)?;
        let header_payload =
            encode_transaction_header_row(transaction, visible_update_count, precondition_count)?;
        let mut owned_ops = vec![OwnedCoreMetaBatchOp::Put {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
            tuple_key: header_key,
            payload: header_payload,
            common: None,
        }];

        let first_new_update = if existing_header.is_some() {
            match self
                .read_transaction_from_rows_unlocked(&transaction.transaction_id)
                .await?
            {
                Some(existing)
                    if transaction_updates_have_prefix(
                        &transaction.visible_updates,
                        &existing.visible_updates,
                    ) =>
                {
                    existing_update_count.min(visible_update_count)
                }
                Some(_) => 0,
                None => 0,
            }
        } else {
            0
        };
        for ordinal in first_new_update..visible_update_count {
            let update = transaction
                .visible_updates
                .get(usize::try_from(ordinal).map_err(|_| {
                    anyhow!("CoreStore transaction staged update ordinal exceeds usize")
                })?)
                .ok_or_else(|| anyhow!("CoreStore transaction staged update is missing"))?;
            let row = self
                .transaction_update_row_from_update(transaction, ordinal, update)
                .await?;
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                tuple_key: transaction_update_tuple_key(&transaction.transaction_id, ordinal)?,
                payload: encode_transaction_update_row(transaction, ordinal, &row)?,
                common: None,
            });
        }

        for (offset, precondition) in new_preconditions.iter().enumerate() {
            let ordinal = existing_precondition_count.saturating_add(offset as u64);
            owned_ops.push(OwnedCoreMetaBatchOp::Put {
                cf: CF_TRANSACTIONS,
                table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
                tuple_key: transaction_precondition_tuple_key(
                    &transaction.transaction_id,
                    ordinal,
                )?,
                payload: encode_transaction_precondition_row(transaction, ordinal, precondition)?,
                common: None,
            });
        }

        Ok(owned_ops)
    }

    pub(super) fn transaction_header_as_coremeta_op_unlocked(
        &self,
        transaction: &CoreTransaction,
    ) -> Result<OwnedCoreMetaBatchOp> {
        validate_transaction_root_scope(transaction)?;
        validate_logical_id(&transaction.transaction_id, "transaction id")?;
        let precondition_count = self
            .read_transaction_header_row_unlocked(&transaction.transaction_id)?
            .map(|header| header.precondition_count)
            .unwrap_or(0);
        let visible_update_count = u64::try_from(transaction.visible_updates.len())
            .map_err(|_| anyhow!("CoreStore transaction has too many staged updates"))?;
        Ok(OwnedCoreMetaBatchOp::Put {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_EXPLICIT_TRANSACTION_ROW,
            tuple_key: transaction_header_tuple_key(&transaction.transaction_id)?,
            payload: encode_transaction_header_row(
                transaction,
                visible_update_count,
                precondition_count,
            )?,
            common: None,
        })
    }

    pub(super) async fn read_transaction_from_rows_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransaction>> {
        validate_logical_id(transaction_id, "transaction id")?;
        let Some(header) = self.read_transaction_header_row_unlocked(transaction_id)? else {
            return Ok(None);
        };
        let update_rows = self
            .read_transaction_update_rows_unlocked(transaction_id)
            .await?;
        let mut updates_by_ordinal = BTreeMap::new();
        for row in update_rows {
            if row.ordinal >= header.visible_update_count {
                continue;
            }
            if updates_by_ordinal.insert(row.ordinal, row).is_some() {
                bail!("CoreStore transaction has duplicate staged update row ordinal");
            }
        }

        let mut visible_updates = Vec::with_capacity(
            usize::try_from(header.visible_update_count)
                .map_err(|_| anyhow!("CoreStore transaction update count exceeds usize"))?,
        );
        for ordinal in 0..header.visible_update_count {
            let row = updates_by_ordinal
                .remove(&ordinal)
                .ok_or_else(|| anyhow!("CoreStore transaction staged update row is missing"))?;
            visible_updates.push(self.transaction_update_from_row(row).await?);
        }

        let mut transaction = header.transaction;
        transaction.visible_updates = visible_updates;
        validate_transaction_root_scope(&transaction)?;
        Ok(Some(transaction))
    }

    pub(super) fn read_transaction_header_row_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Option<CoreTransactionHeaderRow>> {
        let Some(payload) = self.meta.get(
            CF_TRANSACTIONS,
            TABLE_EXPLICIT_TRANSACTION_ROW,
            &transaction_header_tuple_key(transaction_id)?,
        )?
        else {
            return Ok(None);
        };
        Ok(Some(decode_transaction_header_row(
            &payload,
            transaction_id,
        )?))
    }

    async fn read_transaction_update_rows_unlocked(
        &self,
        transaction_id: &str,
    ) -> Result<Vec<CoreTransactionStagedUpdateRow>> {
        self.meta
            .scan_prefix(
                CF_TRANSACTIONS,
                TABLE_EXPLICIT_TRANSACTION_ROW,
                &transaction_update_tuple_prefix(transaction_id)?,
            )?
            .into_iter()
            .map(|record| decode_transaction_update_row(&record.payload, transaction_id))
            .collect()
    }

    async fn transaction_update_row_from_update(
        &self,
        transaction: &CoreTransaction,
        ordinal: u64,
        update: &CoreTransactionUpdate,
    ) -> Result<CoreTransactionUpdateRow> {
        Ok(match update {
            CoreTransactionUpdate::StreamAppend {
                stream_id,
                visible_sequence,
                prepared_record_hash,
            } => CoreTransactionUpdateRow::StreamAppend {
                stream_id: stream_id.clone(),
                visible_sequence: *visible_sequence,
                prepared_record_hash: prepared_record_hash.clone(),
            },
            CoreTransactionUpdate::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
                payload,
                payload_hash,
            } => {
                let actual_hash = core_meta_payload_digest(*table_id, payload);
                if &actual_hash != payload_hash {
                    bail!("CoreStore transaction staged CoreMeta payload hash mismatch");
                }
                CoreTransactionUpdateRow::CoreMetaPut {
                    cf: canonical_coremeta_cf_name(cf)?.to_string(),
                    table_id: *table_id,
                    tuple_key: tuple_key.clone(),
                    previous_payload_hash: previous_payload_hash.clone(),
                    payload_hash: payload_hash.clone(),
                    payload_ref: self
                        .transaction_payload_ref(transaction, ordinal, payload)
                        .await?,
                }
            }
            CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
            } => CoreTransactionUpdateRow::CoreMetaDelete {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: *table_id,
                tuple_key: tuple_key.clone(),
                previous_payload_hash: previous_payload_hash.clone(),
            },
        })
    }

    async fn transaction_payload_ref(
        &self,
        transaction: &CoreTransaction,
        ordinal: u64,
        payload: &[u8],
    ) -> Result<CoreTransactionPayloadRef> {
        let payload_sha256 = format!("sha256:{}", sha256_hex(payload));
        let payload_length = payload.len() as u64;
        if payload.len() <= CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES {
            return Ok(CoreTransactionPayloadRef::Inline {
                payload: payload.to_vec(),
                payload_sha256,
                payload_length,
            });
        }

        let policy = self.pipeline_policy_for_storage_class(None)?;
        let logical_name = format!(
            "transaction-payload:{}:{}:{}",
            transaction.transaction_id,
            ordinal,
            payload_sha256.trim_start_matches("sha256:")
        );
        let write = self
            .write_logical_file_with_locator(WriteLogicalFileRequest {
                writer_family: WriterFamily::CoreControl.as_str().to_string(),
                generation: ordinal,
                logical_file_id: logical_name,
                source: payload.to_vec(),
                range_hints: Vec::new(),
                pipeline_policy: policy,
                trace_context: CoreTraceContext::default(),
                boundary_values: Vec::new(),
                mutation_id: format!(
                    "transaction-payload:{}:{ordinal}",
                    transaction.transaction_id
                ),
                region_id: self.node_identity.region_id.clone(),
            })
            .await?;
        Ok(CoreTransactionPayloadRef::Locator {
            locator: core_meta_locator_from_manifest_locator(&write.locator)?,
            payload_sha256,
            payload_length,
        })
    }

    async fn transaction_update_from_row(
        &self,
        row: CoreTransactionStagedUpdateRow,
    ) -> Result<CoreTransactionUpdate> {
        Ok(match row.update {
            CoreTransactionUpdateRow::StreamAppend {
                stream_id,
                visible_sequence,
                prepared_record_hash,
            } => CoreTransactionUpdate::StreamAppend {
                stream_id,
                visible_sequence,
                prepared_record_hash,
            },
            CoreTransactionUpdateRow::CoreMetaPut {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
                payload_hash,
                payload_ref,
            } => {
                let payload = self.read_transaction_payload_ref(payload_ref).await?;
                validate_coremeta_operation_payload(&cf, table_id, &tuple_key, &payload)?;
                let actual_hash = core_meta_payload_digest(table_id, &payload);
                if actual_hash != payload_hash {
                    bail!("CoreStore transaction staged CoreMeta payload hash mismatch");
                }
                CoreTransactionUpdate::CoreMetaPut {
                    cf,
                    table_id,
                    tuple_key,
                    previous_payload_hash,
                    payload,
                    payload_hash,
                }
            }
            CoreTransactionUpdateRow::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
            } => CoreTransactionUpdate::CoreMetaDelete {
                cf,
                table_id,
                tuple_key,
                previous_payload_hash,
            },
        })
    }

    async fn read_transaction_payload_ref(
        &self,
        payload_ref: CoreTransactionPayloadRef,
    ) -> Result<Vec<u8>> {
        match payload_ref {
            CoreTransactionPayloadRef::Inline {
                payload,
                payload_sha256,
                payload_length,
            } => {
                validate_payload_bytes(&payload, &payload_sha256, payload_length)?;
                Ok(payload)
            }
            CoreTransactionPayloadRef::Locator {
                locator,
                payload_sha256,
                payload_length,
            } => {
                let manifest_locator = core_meta_locator_to_manifest_locator(&locator)?;
                let manifest = self.read_logical_file_manifest(&manifest_locator).await?;
                if manifest.logical_size != payload_length {
                    bail!("CoreStore transaction staged payload locator length mismatch");
                }
                let payload = self
                    .read_logical_range(ReadLogicalRangeRequest {
                        ranges: vec![CoreByteRange {
                            start: 0,
                            end_exclusive: manifest.logical_size,
                        }],
                        manifest,
                        authz_scope: AuthzScopeRef {
                            anvil_storage_tenant_id: "system".to_string(),
                            authz_realm_id: "corestore".to_string(),
                        },
                        expected_boundary: None,
                        prefetch_policy: CorePrefetchPolicy::default(),
                        trace_context: CoreTraceContext::default(),
                    })
                    .await?;
                validate_payload_bytes(&payload, &payload_sha256, payload_length)?;
                Ok(payload)
            }
        }
    }
}

fn encode_transaction_header_row(
    transaction: &CoreTransaction,
    visible_update_count: u64,
    precondition_count: u64,
) -> Result<Vec<u8>> {
    validate_transaction_root_scope(transaction)?;
    validate_logical_id(&transaction.transaction_id, "transaction id")?;
    let root_generation = transaction
        .committed_root_generation
        .unwrap_or_else(|| generation_for_transaction_state(transaction.state));
    encode_message(CoreTransactionHeaderRowProto {
        common: Some(CoreMetaRowCommonProto {
            realm_id: transaction.committed_by_principal.clone(),
            root_key_hash: transaction.root_key_hash.clone(),
            root_generation,
            transaction_id: transaction.transaction_id.clone(),
            visibility_state: visibility_for_transaction_state(transaction.state) as i32,
            created_at_unix_nanos: transaction.created_at_unix_nanos,
            payload_schema_version: 1,
        }),
        schema: CORE_TRANSACTION_HEADER_ROW_SCHEMA.to_string(),
        transaction_id: transaction.transaction_id.clone(),
        scope_partition: transaction.scope_partition.clone(),
        state: transaction_state_to_proto(transaction.state) as i32,
        preconditions_hash: transaction.preconditions_hash.clone(),
        operations_hash: transaction.operations_hash.clone(),
        visible_update_count,
        precondition_count,
        finalisation_error: transaction.finalisation_error.clone(),
        committed_at: transaction.committed_at.clone(),
        committed_by_principal: transaction.committed_by_principal.clone(),
        created_at_unix_nanos: transaction.created_at_unix_nanos,
        expires_at_unix_nanos: transaction.expires_at_unix_nanos,
        root_anchor_key: transaction.root_anchor_key.clone(),
        root_key_hash: transaction.root_key_hash.clone(),
        committed_root_generation: transaction.committed_root_generation,
        purpose: transaction.purpose.clone(),
        failure_evidence: transaction.failure_evidence.clone(),
        outcome: transaction.outcome.clone(),
    })
}

pub(super) fn borrow_owned_coremeta_batch_ops(
    ops: &[OwnedCoreMetaBatchOp],
) -> Vec<CoreMetaBatchOp<'_>> {
    ops.iter()
        .map(|op| match op {
            OwnedCoreMetaBatchOp::Put {
                cf,
                table_id,
                tuple_key,
                payload,
                common,
            } => CoreMetaBatchOp {
                cf,
                table_id: *table_id,
                tuple_key,
                common: common.clone(),
                kind: CoreMetaBatchOpKind::Put(payload),
            },
            OwnedCoreMetaBatchOp::Delete {
                cf,
                table_id,
                tuple_key,
                common,
            } => CoreMetaBatchOp {
                cf,
                table_id: *table_id,
                tuple_key,
                common: common.clone(),
                kind: CoreMetaBatchOpKind::Delete,
            },
        })
        .collect()
}

fn decode_transaction_header_row(
    bytes: &[u8],
    expected_transaction_id: &str,
) -> Result<CoreTransactionHeaderRow> {
    let proto = CoreTransactionHeaderRowProto::decode(bytes)?;
    ensure_message_round_trips(&proto, bytes, "CoreStore transaction header row")?;
    if proto.schema != CORE_TRANSACTION_HEADER_ROW_SCHEMA {
        bail!("CoreStore transaction header row has invalid schema");
    }
    if proto.transaction_id != expected_transaction_id {
        bail!("CoreStore transaction header row scope mismatch");
    }
    let common = proto
        .common
        .clone()
        .ok_or_else(|| anyhow!("CoreStore transaction header row missing CoreMeta common"))?;
    let transaction = CoreTransaction {
        schema: CORE_TRANSACTION_SCHEMA.to_string(),
        transaction_id: proto.transaction_id,
        scope_partition: proto.scope_partition,
        state: transaction_state_from_proto(proto.state)?,
        preconditions_hash: proto.preconditions_hash,
        operations_hash: proto.operations_hash,
        visible_updates: Vec::new(),
        finalisation_error: proto.finalisation_error,
        committed_at: proto.committed_at,
        committed_by_principal: proto.committed_by_principal,
        created_at_unix_nanos: proto.created_at_unix_nanos,
        expires_at_unix_nanos: proto.expires_at_unix_nanos,
        root_anchor_key: proto.root_anchor_key,
        root_key_hash: proto.root_key_hash,
        committed_root_generation: proto.committed_root_generation,
        purpose: proto.purpose,
        failure_evidence: proto.failure_evidence,
        outcome: proto.outcome,
    };
    validate_transaction_root_scope(&transaction)?;
    validate_transaction_header_common(&transaction, &common)?;
    Ok(CoreTransactionHeaderRow {
        transaction,
        visible_update_count: proto.visible_update_count,
        precondition_count: proto.precondition_count,
    })
}

fn encode_transaction_update_row(
    transaction: &CoreTransaction,
    ordinal: u64,
    update: &CoreTransactionUpdateRow,
) -> Result<Vec<u8>> {
    validate_logical_id(&transaction.transaction_id, "transaction id")?;
    encode_message(CoreTransactionStagedUpdateRowProto {
        common: Some(CoreMetaRowCommonProto {
            realm_id: transaction.committed_by_principal.clone(),
            root_key_hash: transaction.root_key_hash.clone(),
            root_generation: transaction
                .committed_root_generation
                .unwrap_or_else(|| generation_for_transaction_state(transaction.state)),
            transaction_id: transaction.transaction_id.clone(),
            visibility_state: visibility_for_transaction_state(transaction.state) as i32,
            created_at_unix_nanos: transaction.created_at_unix_nanos,
            payload_schema_version: 1,
        }),
        schema: CORE_TRANSACTION_STAGED_UPDATE_ROW_SCHEMA.to_string(),
        transaction_id: transaction.transaction_id.clone(),
        ordinal,
        update: Some(transaction_update_to_proto(update)?),
    })
}

fn validate_transaction_header_common(
    transaction: &CoreTransaction,
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    if common.root_key_hash != transaction.root_key_hash
        || common.transaction_id != transaction.transaction_id
    {
        bail!("CoreStore transaction header CoreMeta common scope mismatch");
    }
    if common.visibility_state_enum() != visibility_for_transaction_state(transaction.state) {
        bail!("CoreStore transaction header CoreMeta visibility mismatch");
    }
    if let Some(committed_root_generation) = transaction.committed_root_generation {
        if common.root_generation != committed_root_generation {
            bail!("CoreStore transaction header committed root generation mismatch");
        }
    }
    if common.root_generation == 0 {
        bail!("CoreStore transaction header rooted rows must use a non-zero root generation");
    }
    Ok(())
}

fn decode_transaction_update_row(
    bytes: &[u8],
    expected_transaction_id: &str,
) -> Result<CoreTransactionStagedUpdateRow> {
    let proto = CoreTransactionStagedUpdateRowProto::decode(bytes)?;
    ensure_message_round_trips(&proto, bytes, "CoreStore transaction staged update row")?;
    if proto.schema != CORE_TRANSACTION_STAGED_UPDATE_ROW_SCHEMA {
        bail!("CoreStore transaction staged update row has invalid schema");
    }
    if proto.transaction_id != expected_transaction_id {
        bail!("CoreStore transaction staged update row scope mismatch");
    }
    proto.common.as_ref().ok_or_else(|| {
        anyhow!("CoreStore transaction staged update row missing CoreMeta common")
    })?;
    Ok(CoreTransactionStagedUpdateRow {
        ordinal: proto.ordinal,
        update: transaction_update_from_proto(
            proto
                .update
                .ok_or_else(|| anyhow!("CoreStore transaction staged update row missing update"))?,
        )?,
    })
}

fn encode_transaction_precondition_row(
    transaction: &CoreTransaction,
    ordinal: u64,
    precondition: &CoreMutationPrecondition,
) -> Result<Vec<u8>> {
    validate_logical_id(&transaction.transaction_id, "transaction id")?;
    encode_message(CoreTransactionPreconditionRowProto {
        common: Some(CoreMetaRowCommonProto {
            realm_id: transaction.committed_by_principal.clone(),
            root_key_hash: transaction.root_key_hash.clone(),
            root_generation: transaction
                .committed_root_generation
                .unwrap_or_else(|| generation_for_transaction_state(transaction.state)),
            transaction_id: transaction.transaction_id.clone(),
            visibility_state: visibility_for_transaction_state(transaction.state) as i32,
            created_at_unix_nanos: transaction.created_at_unix_nanos,
            payload_schema_version: 1,
        }),
        schema: CORE_TRANSACTION_PRECONDITION_ROW_SCHEMA.to_string(),
        transaction_id: transaction.transaction_id.clone(),
        ordinal,
        precondition: Some(mutation_precondition_to_proto(precondition)?),
    })
}

fn transaction_update_to_proto(
    update: &CoreTransactionUpdateRow,
) -> Result<CoreTransactionUpdateRowProto> {
    let kind = match update {
        CoreTransactionUpdateRow::StreamAppend {
            stream_id,
            visible_sequence,
            prepared_record_hash,
        } => {
            transaction_update_row_proto::Kind::StreamAppend(CoreTransactionStreamAppendRowProto {
                stream_id: stream_id.clone(),
                visible_sequence: *visible_sequence,
                prepared_record_hash: prepared_record_hash.clone(),
            })
        }
        CoreTransactionUpdateRow::CoreMetaPut {
            cf,
            table_id,
            tuple_key,
            previous_payload_hash,
            payload_hash,
            payload_ref,
        } => transaction_update_row_proto::Kind::CoreMetaPut(CoreTransactionCoreMetaPutRowProto {
            cf: canonical_coremeta_cf_name(cf)?.to_string(),
            table_id: u32::from(*table_id),
            tuple_key: tuple_key.clone(),
            previous_payload_hash: previous_payload_hash.clone(),
            payload_hash: payload_hash.clone(),
            payload_ref: Some(payload_ref_to_proto(payload_ref)?),
        }),
        CoreTransactionUpdateRow::CoreMetaDelete {
            cf,
            table_id,
            tuple_key,
            previous_payload_hash,
        } => transaction_update_row_proto::Kind::CoreMetaDelete(
            CoreTransactionCoreMetaDeleteRowProto {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                previous_payload_hash: previous_payload_hash.clone(),
            },
        ),
    };
    Ok(CoreTransactionUpdateRowProto { kind: Some(kind) })
}

fn transaction_update_from_proto(
    proto: CoreTransactionUpdateRowProto,
) -> Result<CoreTransactionUpdateRow> {
    Ok(
        match proto
            .kind
            .ok_or_else(|| anyhow!("CoreStore transaction staged update is missing kind"))?
        {
            transaction_update_row_proto::Kind::StreamAppend(value) => {
                CoreTransactionUpdateRow::StreamAppend {
                    stream_id: value.stream_id,
                    visible_sequence: value.visible_sequence,
                    prepared_record_hash: value.prepared_record_hash,
                }
            }
            transaction_update_row_proto::Kind::CoreMetaPut(value) => {
                CoreTransactionUpdateRow::CoreMetaPut {
                    cf: canonical_coremeta_cf_name(&value.cf)?.to_string(),
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta put update table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    previous_payload_hash: value.previous_payload_hash,
                    payload_hash: value.payload_hash,
                    payload_ref: payload_ref_from_proto(value.payload_ref.ok_or_else(|| {
                        anyhow!("CoreStore transaction staged CoreMeta put is missing payload ref")
                    })?)?,
                }
            }
            transaction_update_row_proto::Kind::CoreMetaDelete(value) => {
                CoreTransactionUpdateRow::CoreMetaDelete {
                    cf: canonical_coremeta_cf_name(&value.cf)?.to_string(),
                    table_id: u16::try_from(value.table_id)
                        .map_err(|_| anyhow!("CoreMeta delete update table id exceeds u16"))?,
                    tuple_key: value.tuple_key,
                    previous_payload_hash: value.previous_payload_hash,
                }
            }
        },
    )
}

fn payload_ref_to_proto(
    payload_ref: &CoreTransactionPayloadRef,
) -> Result<CoreTransactionPayloadRefProto> {
    match payload_ref {
        CoreTransactionPayloadRef::Inline {
            payload,
            payload_sha256,
            payload_length,
        } => {
            validate_payload_bytes(payload, payload_sha256, *payload_length)?;
            if payload.len() > CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES {
                bail!("CoreStore transaction inline staged payload exceeds row inline limit");
            }
            Ok(CoreTransactionPayloadRefProto {
                inline_payload: Some(payload.clone()),
                locator: None,
                payload_sha256: payload_sha256.clone(),
                payload_length: *payload_length,
            })
        }
        CoreTransactionPayloadRef::Locator {
            locator,
            payload_sha256,
            payload_length,
        } => {
            validate_hash(payload_sha256, "transaction staged payload sha256")?;
            Ok(CoreTransactionPayloadRefProto {
                inline_payload: None,
                locator: Some(locator.clone()),
                payload_sha256: payload_sha256.clone(),
                payload_length: *payload_length,
            })
        }
    }
}

fn payload_ref_from_proto(
    proto: CoreTransactionPayloadRefProto,
) -> Result<CoreTransactionPayloadRef> {
    validate_hash(&proto.payload_sha256, "transaction staged payload sha256")?;
    if let Some(payload) = proto.inline_payload {
        if proto.locator.is_some() {
            bail!("CoreStore transaction payload ref cannot be both inline and locator backed");
        }
        validate_payload_bytes(&payload, &proto.payload_sha256, proto.payload_length)?;
        if payload.len() > CORE_TRANSACTION_STAGED_INLINE_PAYLOAD_BYTES {
            bail!("CoreStore transaction inline staged payload exceeds row inline limit");
        }
        return Ok(CoreTransactionPayloadRef::Inline {
            payload,
            payload_sha256: proto.payload_sha256,
            payload_length: proto.payload_length,
        });
    }
    let locator = proto
        .locator
        .ok_or_else(|| anyhow!("CoreStore transaction payload ref is missing storage"))?;
    Ok(CoreTransactionPayloadRef::Locator {
        locator,
        payload_sha256: proto.payload_sha256,
        payload_length: proto.payload_length,
    })
}

fn mutation_precondition_to_proto(
    precondition: &CoreMutationPrecondition,
) -> Result<CoreMutationPreconditionRowProto> {
    let kind = match precondition {
        CoreMutationPrecondition::Fence {
            fence_name,
            fence_token,
        } => mutation_precondition_row_proto::Kind::Fence(CoreMutationFencePreconditionRowProto {
            fence_name: fence_name.clone(),
            fence_token: *fence_token,
        }),
        CoreMutationPrecondition::CoreMetaRow {
            cf,
            table_id,
            tuple_key,
            expected_payload_hash,
            require_absent,
            require_present,
        } => mutation_precondition_row_proto::Kind::CoreMetaRow(
            CoreMutationCoreMetaRowPreconditionRowProto {
                cf: canonical_coremeta_cf_name(cf)?.to_string(),
                table_id: u32::from(*table_id),
                tuple_key: tuple_key.clone(),
                expected_payload_hash: expected_payload_hash.clone(),
                require_absent: *require_absent,
                require_present: *require_present,
            },
        ),
        CoreMutationPrecondition::StreamHead {
            stream_id,
            expected_last_sequence,
            expected_last_event_hash,
        } => mutation_precondition_row_proto::Kind::StreamHead(
            CoreMutationStreamHeadPreconditionRowProto {
                stream_id: stream_id.clone(),
                expected_last_sequence: *expected_last_sequence,
                expected_last_event_hash: expected_last_event_hash.clone(),
            },
        ),
    };
    Ok(CoreMutationPreconditionRowProto { kind: Some(kind) })
}

fn fence_precondition_to_proto(value: &CoreFencePrecondition) -> CoreFencePreconditionRowProto {
    CoreFencePreconditionRowProto {
        fence_name: value.fence_name.clone(),
        fence_token: value.fence_token,
        authenticated_principal: value.authenticated_principal.clone(),
    }
}

fn transaction_header_tuple_key(transaction_id: &str) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("header"),
    ])
}

fn transaction_update_tuple_prefix(transaction_id: &str) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("update"),
    ])
}

fn transaction_update_tuple_key(transaction_id: &str, ordinal: u64) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("update"),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

fn transaction_precondition_tuple_key(transaction_id: &str, ordinal: u64) -> Result<Vec<u8>> {
    validate_logical_id(transaction_id, "transaction id")?;
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("transaction"),
        CoreMetaTuplePart::Utf8(transaction_id),
        CoreMetaTuplePart::Utf8("precondition"),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

fn transaction_state_to_proto(state: CoreTransactionState) -> CoreTransactionHeaderStateProto {
    match state {
        CoreTransactionState::Open => CoreTransactionHeaderStateProto::Open,
        CoreTransactionState::Prepared => CoreTransactionHeaderStateProto::Prepared,
        CoreTransactionState::Committed => CoreTransactionHeaderStateProto::Committed,
        CoreTransactionState::FinalisationFailed => {
            CoreTransactionHeaderStateProto::FinalisationFailed
        }
        CoreTransactionState::Aborted => CoreTransactionHeaderStateProto::Aborted,
        CoreTransactionState::RolledBack => CoreTransactionHeaderStateProto::RolledBack,
        CoreTransactionState::Expired => CoreTransactionHeaderStateProto::Expired,
        CoreTransactionState::Failed => CoreTransactionHeaderStateProto::Failed,
    }
}

fn transaction_state_from_proto(state: i32) -> Result<CoreTransactionState> {
    Ok(match CoreTransactionHeaderStateProto::try_from(state)? {
        CoreTransactionHeaderStateProto::Open => CoreTransactionState::Open,
        CoreTransactionHeaderStateProto::Prepared => CoreTransactionState::Prepared,
        CoreTransactionHeaderStateProto::Committed => CoreTransactionState::Committed,
        CoreTransactionHeaderStateProto::FinalisationFailed => {
            CoreTransactionState::FinalisationFailed
        }
        CoreTransactionHeaderStateProto::Aborted => CoreTransactionState::Aborted,
        CoreTransactionHeaderStateProto::RolledBack => CoreTransactionState::RolledBack,
        CoreTransactionHeaderStateProto::Expired => CoreTransactionState::Expired,
        CoreTransactionHeaderStateProto::Failed => CoreTransactionState::Failed,
        CoreTransactionHeaderStateProto::Unspecified => {
            bail!("CoreStore transaction state is unset")
        }
    })
}

fn visibility_for_transaction_state(state: CoreTransactionState) -> CoreMetaVisibilityState {
    match state {
        CoreTransactionState::Open | CoreTransactionState::Prepared => {
            CoreMetaVisibilityState::Pending
        }
        CoreTransactionState::Committed => CoreMetaVisibilityState::Committed,
        CoreTransactionState::RolledBack => CoreMetaVisibilityState::RolledBack,
        CoreTransactionState::FinalisationFailed
        | CoreTransactionState::Aborted
        | CoreTransactionState::Expired
        | CoreTransactionState::Failed => CoreMetaVisibilityState::Aborted,
    }
}

fn generation_for_transaction_state(state: CoreTransactionState) -> u64 {
    match state {
        CoreTransactionState::Open | CoreTransactionState::Prepared => 1,
        CoreTransactionState::Committed => 2,
        CoreTransactionState::FinalisationFailed
        | CoreTransactionState::Aborted
        | CoreTransactionState::RolledBack
        | CoreTransactionState::Expired
        | CoreTransactionState::Failed => 3,
    }
}

fn transaction_updates_have_prefix(
    value: &[CoreTransactionUpdate],
    prefix: &[CoreTransactionUpdate],
) -> bool {
    value.len() >= prefix.len() && value.iter().zip(prefix.iter()).all(|(a, b)| a == b)
}

fn validate_payload_bytes(payload: &[u8], expected_sha256: &str, expected_len: u64) -> Result<()> {
    validate_hash(expected_sha256, "transaction staged payload sha256")?;
    if payload.len() as u64 != expected_len {
        bail!("CoreStore transaction staged payload length mismatch");
    }
    let actual = format!("sha256:{}", sha256_hex(payload));
    if actual != expected_sha256 {
        bail!("CoreStore transaction staged payload sha256 mismatch");
    }
    Ok(())
}

fn encode_message(message: impl Message) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_message_round_trips(message: &impl Message, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("{label} is not deterministically encoded");
    }
    Ok(())
}
