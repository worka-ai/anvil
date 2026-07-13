use super::*;
use prost::Message;

fn validate_coremeta_digest(value: &str, label: &str) -> Result<()> {
    let Some((algorithm, digest)) = value.split_once(':') else {
        bail!("{label} must use algorithm:hex encoding");
    };
    if !matches!(algorithm, "sha256" | "blake3")
        || digest.len() != 64
        || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("{label} must be a sha256 or blake3 digest");
    }
    Ok(())
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommitEvidenceRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    certificate_hash: String,
    #[prost(string, tag = "3")]
    committed_batch_hash: String,
    #[prost(bytes, tag = "4")]
    certificate_bytes: Vec<u8>,
    #[prost(string, repeated, tag = "5")]
    certificate_persist_receipt_hashes: Vec<String>,
    #[prost(bytes = "vec", repeated, tag = "6")]
    certificate_persist_receipt_bytes: Vec<Vec<u8>>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaPendingBatchMarkerRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    pending_batch_hash: String,
    #[prost(string, tag = "3")]
    root_key_hash: String,
    #[prost(uint64, tag = "4")]
    expected_root_generation: u64,
    #[prost(uint64, tag = "5")]
    post_root_generation: u64,
    #[prost(string, tag = "6")]
    transaction_id: String,
    #[prost(uint64, tag = "7")]
    core_meta_row_count: u64,
}

#[derive(Debug, Clone)]
pub(super) struct CoreMetaCommitEvidenceRecord {
    pub(super) certificate_hash: String,
    pub(super) committed_batch_hash: String,
    pub(super) certificate_bytes: Vec<u8>,
    pub(super) certificate_persist_receipt_hashes: Vec<String>,
    pub(super) certificate_persist_receipt_bytes: Vec<Vec<u8>>,
}

impl CoreStore {
    pub(crate) fn read_coremeta_row(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let Some(payload) = self.meta.get(cf, table_id, tuple_key)? else {
            return Ok(None);
        };
        if self.coremeta_payload_is_committed_visible(cf, table_id, &payload)? {
            Ok(Some(payload))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn scan_coremeta_prefix(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
    ) -> Result<Vec<CoreMetaRecord>> {
        self.meta
            .scan_prefix(cf, table_id, tuple_prefix)?
            .into_iter()
            .filter_map(|record| {
                match self.coremeta_payload_is_committed_visible(cf, table_id, &record.payload) {
                    Ok(true) => Some(Ok(record)),
                    Ok(false) => None,
                    Err(error) => Some(Err(error)),
                }
            })
            .collect()
    }

    pub fn write_coremeta_encoded_rows(&self, rows: &[CoreMetaEncodedRow<'_>]) -> Result<()> {
        self.meta.write_encoded_rows(rows)
    }

    pub fn export_coremeta_snapshot_rows(&self) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        self.meta.scan_all_encoded_rows()
    }

    /// Export the canonical cluster bootstrap state without copying this
    /// node's private identity, runtime coordination state, or local-only
    /// payload locators into a joining node.
    pub fn export_portable_coremeta_bootstrap_rows(&self) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        Ok(self
            .export_coremeta_snapshot_rows()?
            .into_iter()
            .filter(|row| !is_node_local_bootstrap_row(row))
            .filter(|row| !is_runtime_local_bootstrap_row(row))
            .filter(|row| !contains_local_bootstrap_locator(row))
            .collect())
    }

    pub fn install_coremeta_snapshot_rows(&self, rows: &[CoreMetaEncodedOwnedRow]) -> Result<()> {
        let borrowed = rows
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        self.write_coremeta_encoded_rows(&borrowed)
    }

    pub fn install_portable_coremeta_bootstrap_rows(
        &self,
        rows: &[CoreMetaEncodedOwnedRow],
    ) -> Result<()> {
        if rows.is_empty() {
            bail!("portable CoreMeta bootstrap snapshot must not be empty");
        }
        if rows.iter().any(|row| {
            is_node_local_bootstrap_row(row)
                || is_runtime_local_bootstrap_row(row)
                || contains_local_bootstrap_locator(row)
        }) {
            bail!("portable CoreMeta bootstrap snapshot contains node-local state");
        }
        self.install_coremeta_snapshot_rows(rows)
    }

    fn coremeta_payload_is_committed_visible(
        &self,
        cf: &'static str,
        table_id: u16,
        payload: &[u8],
    ) -> Result<bool> {
        if cf == CF_TRANSACTIONS && table_id == TABLE_EXPLICIT_TRANSACTION_ROW {
            return Ok(true);
        }
        let common = core_meta_row_common_from_payload(payload)?;
        if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
            return Ok(false);
        }
        if common.transaction_id.is_empty() {
            return Ok(true);
        }
        let Some(header) = self.read_transaction_header_row_unlocked(&common.transaction_id)?
        else {
            return Ok(true);
        };
        Ok(header.transaction.state == CoreTransactionState::Committed)
    }

    pub fn read_coremeta_encoded_rows(
        &self,
        cf_name: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        self.meta.get_encoded_rows(cf_name, keys)
    }

    pub fn catch_up_coremeta_rows(
        &self,
        root_key_hash: &str,
        after_generation: u64,
        limit: usize,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        self.meta
            .scan_encoded_rows_for_root(root_key_hash, after_generation, limit)
    }

    pub fn coremeta_inventory_rows(
        &self,
        root_key_hash: &str,
        from_generation: u64,
        to_generation: u64,
        limit: usize,
    ) -> Result<Vec<CoreMetaInventoryRow>> {
        self.meta
            .inventory_rows_for_root(root_key_hash, from_generation, to_generation, limit)
    }

    pub fn persist_coremeta_commit_evidence(
        &self,
        root_key_hash: &str,
        root_generation: u64,
        transaction_id: &str,
        certificate_hash: &str,
        committed_batch_hash: &str,
        certificate_bytes: Vec<u8>,
        mut certificate_persist_receipt_hashes: Vec<String>,
        mut certificate_persist_receipt_bytes: Vec<Vec<u8>>,
    ) -> Result<()> {
        let row = self.coremeta_commit_evidence_encoded_row(
            root_key_hash,
            root_generation,
            transaction_id,
            certificate_hash,
            committed_batch_hash,
            certificate_bytes,
            std::mem::take(&mut certificate_persist_receipt_hashes),
            std::mem::take(&mut certificate_persist_receipt_bytes),
        )?;
        let borrowed = [CoreMetaEncodedRow {
            cf: row.cf.as_str(),
            core_meta_key: &row.core_meta_key,
            value_envelope: &row.value_envelope,
            delete_marker: row.delete_marker,
        }];
        self.write_coremeta_encoded_rows(&borrowed)
    }

    pub(super) fn coremeta_commit_evidence_encoded_row(
        &self,
        root_key_hash: &str,
        root_generation: u64,
        transaction_id: &str,
        certificate_hash: &str,
        committed_batch_hash: &str,
        certificate_bytes: Vec<u8>,
        mut certificate_persist_receipt_hashes: Vec<String>,
        mut certificate_persist_receipt_bytes: Vec<Vec<u8>>,
    ) -> Result<CoreMetaEncodedOwnedRow> {
        certificate_persist_receipt_hashes.sort();
        certificate_persist_receipt_hashes.dedup();
        certificate_persist_receipt_bytes.sort();
        certificate_persist_receipt_bytes.dedup();
        let payload = encode_deterministic_proto(&CoreMetaCommitEvidenceRowProto {
            common: Some(core_meta_committed_row_common(
                "system",
                root_key_hash,
                root_generation,
                transaction_id,
                unix_timestamp_nanos(),
            )),
            certificate_hash: certificate_hash.to_string(),
            committed_batch_hash: committed_batch_hash.to_string(),
            certificate_bytes,
            certificate_persist_receipt_hashes,
            certificate_persist_receipt_bytes,
        });
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("coremeta-commit-evidence"),
            CoreMetaTuplePart::Hash(certificate_hash),
        ])?;
        let ops = [CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }];
        self.meta
            .encode_batch_ops(&ops)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("CoreMeta commit evidence encoding produced no row"))
    }

    pub fn persist_coremeta_committed_rows_with_evidence(
        &self,
        rows: &[CoreMetaEncodedRow<'_>],
        root_key_hash: &str,
        root_generation: u64,
        transaction_id: &str,
        certificate_hash: &str,
        committed_batch_hash: &str,
        certificate_bytes: Vec<u8>,
        certificate_persist_receipt_hashes: Vec<String>,
        certificate_persist_receipt_bytes: Vec<Vec<u8>>,
    ) -> Result<()> {
        let evidence_row = self.coremeta_commit_evidence_encoded_row(
            root_key_hash,
            root_generation,
            transaction_id,
            certificate_hash,
            committed_batch_hash,
            certificate_bytes,
            certificate_persist_receipt_hashes,
            certificate_persist_receipt_bytes,
        )?;
        let mut batch_rows = rows.to_vec();
        batch_rows.push(CoreMetaEncodedRow {
            cf: evidence_row.cf.as_str(),
            core_meta_key: &evidence_row.core_meta_key,
            value_envelope: &evidence_row.value_envelope,
            delete_marker: evidence_row.delete_marker,
        });
        self.write_coremeta_encoded_rows(&batch_rows)
    }

    pub fn persist_coremeta_pending_batch_marker(
        &self,
        root_key_hash: &str,
        expected_root_generation: u64,
        post_root_generation: u64,
        transaction_id: &str,
        pending_batch_hash: &str,
        core_meta_row_count: usize,
    ) -> Result<()> {
        validate_hash(root_key_hash, "CoreMeta pending batch root key hash")?;
        validate_coremeta_digest(pending_batch_hash, "CoreMeta pending batch hash")?;
        validate_logical_id(transaction_id, "CoreMeta pending batch transaction id")?;
        let payload = encode_deterministic_proto(&CoreMetaPendingBatchMarkerRowProto {
            common: Some(core_meta_pending_row_common(
                "system/coremeta-pending",
                root_key_hash,
                post_root_generation,
                transaction_id,
                unix_timestamp_nanos(),
            )),
            pending_batch_hash: pending_batch_hash.to_string(),
            root_key_hash: root_key_hash.to_string(),
            expected_root_generation,
            post_root_generation,
            transaction_id: transaction_id.to_string(),
            core_meta_row_count: core_meta_row_count as u64,
        });
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("coremeta-pending-batch"),
            CoreMetaTuplePart::Hash(root_key_hash),
            CoreMetaTuplePart::U64(post_root_generation),
            CoreMetaTuplePart::Utf8(transaction_id),
            CoreMetaTuplePart::Hash(pending_batch_hash),
        ])?;
        let ops = [CoreMetaBatchOp {
            cf: CF_TRANSACTIONS,
            table_id: TABLE_TRANSACTION_LOCATOR_ROW,
            tuple_key: &tuple_key,
            common: None,
            kind: CoreMetaBatchOpKind::Put(&payload),
        }];
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
        self.write_coremeta_encoded_rows(&borrowed)
    }

    pub(super) fn read_coremeta_commit_evidence(
        &self,
        certificate_hash: &str,
    ) -> Result<Option<CoreMetaCommitEvidenceRecord>> {
        validate_coremeta_digest(
            certificate_hash,
            "CoreMeta commit evidence certificate hash",
        )?;
        let tuple_key = core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("coremeta-commit-evidence"),
            CoreMetaTuplePart::Hash(certificate_hash),
        ])?;
        let Some(payload) = self.meta.get(
            CF_TRANSACTIONS,
            TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW,
            &tuple_key,
        )?
        else {
            return Ok(None);
        };
        let row = decode_deterministic_proto::<CoreMetaCommitEvidenceRowProto>(
            &payload,
            "CoreMeta commit evidence row",
        )?;
        if row.certificate_hash != certificate_hash {
            bail!("CoreMeta commit evidence certificate hash scope mismatch");
        }
        validate_coremeta_digest(&row.committed_batch_hash, "CoreMeta committed batch hash")?;
        if row.certificate_bytes.is_empty() {
            bail!("CoreMeta commit evidence is missing certificate bytes");
        }
        for receipt_hash in &row.certificate_persist_receipt_hashes {
            validate_coremeta_digest(receipt_hash, "CoreMeta certificate persist receipt hash")?;
        }
        if row.certificate_persist_receipt_bytes.len()
            < row.certificate_persist_receipt_hashes.len()
        {
            bail!("CoreMeta commit evidence is missing certificate persistence receipt bytes");
        }
        Ok(Some(CoreMetaCommitEvidenceRecord {
            certificate_hash: row.certificate_hash,
            committed_batch_hash: row.committed_batch_hash,
            certificate_bytes: row.certificate_bytes,
            certificate_persist_receipt_hashes: row.certificate_persist_receipt_hashes,
            certificate_persist_receipt_bytes: row.certificate_persist_receipt_bytes,
        }))
    }
}

fn is_node_local_bootstrap_row(row: &CoreMetaEncodedOwnedRow) -> bool {
    if row.cf != CF_MESH || encoded_coremeta_table_id(row) != Some(TABLE_NODE_SIGNING_KEYPAIR_ROW) {
        return false;
    }
    let Ok(tuple_key) = crate::core_store::core_meta_record_tuple_key(&row.core_meta_key) else {
        return false;
    };
    let local_tuples = [
        core_meta_tuple_key(&[CoreMetaTuplePart::Raw(b"node-signing-keypair")]),
        core_meta_tuple_key(&[
            CoreMetaTuplePart::Utf8("cluster-identity"),
            CoreMetaTuplePart::Utf8("local"),
        ]),
    ];
    local_tuples
        .iter()
        .filter_map(|result| result.as_ref().ok())
        .any(|local_tuple| tuple_key == local_tuple.as_slice())
}

fn is_runtime_local_bootstrap_row(row: &CoreMetaEncodedOwnedRow) -> bool {
    row.cf == CF_LEASES_FENCES
        || matches!(
            (row.cf.as_str(), encoded_coremeta_table_id(row)),
            (CF_MATERIALISATION, Some(TABLE_MATERIALISATION_CURSOR_ROW))
                | (
                    CF_MATERIALISATION,
                    Some(crate::core_store::TABLE_WRITER_SEGMENT_ROW)
                )
        )
}

fn contains_local_bootstrap_locator(row: &CoreMetaEncodedOwnedRow) -> bool {
    if row.cf == CF_ROOT_CACHE {
        return false;
    }
    [b"local-node".as_slice(), b"local-control-node".as_slice()]
        .into_iter()
        .any(|needle| {
            row.value_envelope
                .windows(needle.len())
                .any(|window| window == needle)
        })
}

fn encoded_coremeta_table_id(row: &CoreMetaEncodedOwnedRow) -> Option<u16> {
    (row.core_meta_key.len() >= 3)
        .then(|| u16::from_le_bytes([row.core_meta_key[1], row.core_meta_key[2]]))
}
