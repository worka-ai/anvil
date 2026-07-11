use anyhow::{Context, Result, anyhow, bail};
use blake3::Hasher;
use prost::{Enumeration, Message};
use rocksdb::{
    ColumnFamilyDescriptor, DB, Direction, IteratorMode, Options, WriteBatch, WriteOptions,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex as StdMutex, Weak};
use std::time::Instant;
use unicode_normalization::UnicodeNormalization;

use super::deterministic_proto::sha256_hex;
use super::transaction_manifest_proto::{
    decode_manifest_locator_proto, encode_manifest_locator_proto,
};
use super::types::CoreManifestLocator;

pub const CF_META_VERSION: &str = "cf_meta_version";
pub const CF_ROOT_CACHE: &str = "cf_root_cache";
pub const CF_TRANSACTIONS: &str = "cf_transactions";
pub const CF_OBJECT_HEADS: &str = "cf_object_heads";
pub const CF_OBJECT_VERSIONS: &str = "cf_object_versions";
pub const CF_INLINE_PAYLOADS: &str = "cf_inline_payloads";
pub const CF_STREAM_HEADS: &str = "cf_stream_heads";
pub const CF_STREAM_RECORDS: &str = "cf_stream_records";
pub const CF_INDEX_DEFS: &str = "cf_index_defs";
pub const CF_INDEX_ROWS: &str = "cf_index_rows";
pub const CF_BOUNDARY: &str = "cf_boundary";
pub const CF_AUTHZ: &str = "cf_authz";
pub const CF_PERSONALDB: &str = "cf_personaldb";
pub const CF_REGISTRY: &str = "cf_registry";
pub const CF_MESH: &str = "cf_mesh";
pub const CF_LEASES_FENCES: &str = "cf_leases_fences";
pub const CF_MATERIALISATION: &str = "cf_materialisation";
pub const CF_REFCOUNTS: &str = "cf_refcounts";
pub const CF_OBSERVABILITY: &str = "cf_observability";

pub const TABLE_META_SCHEMA_VERSION_ROW: u16 = 0x8001;
pub const TABLE_ROOT_CACHE_ROW: u16 = 0x8002;
pub const TABLE_TRANSACTION_LOCATOR_ROW: u16 = 0x8003;
pub const TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW: u16 = 0x8004;
pub const TABLE_INLINE_MANIFEST_BODY_ROW: u16 = 0x8005;
pub const TABLE_EXPLICIT_TRANSACTION_ROW: u16 = 0x8006;
pub const TABLE_PENDING_MUTATION_ROW: u16 = 0x8007;
pub const TABLE_NATIVE_IDEMPOTENCY_ROW: u16 = 0x8008;
pub const TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW: u16 = 0x8009;
pub const TABLE_OBJECT_HEAD_ROW: u16 = 0x8101;
pub const TABLE_OBJECT_VERSION_META_ROW: u16 = 0x8102;
pub const TABLE_INLINE_PAYLOAD_ROW: u16 = 0x8103;
pub const TABLE_MANIFEST_CAS_CURRENT_ROW: u16 = 0x8104;
pub const TABLE_MULTIPART_UPLOAD_CURRENT_ROW: u16 = 0x8105;
pub const TABLE_MULTIPART_PART_CURRENT_ROW: u16 = 0x8106;
pub const TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW: u16 = 0x8107;
pub const TABLE_STREAM_HEAD_ROW: u16 = 0x8201;
pub const TABLE_STREAM_RECORD_INDEX_ROW: u16 = 0x8202;
pub const TABLE_INDEX_DEFINITION_ROW: u16 = 0x8301;
pub const TABLE_INDEX_ROW: u16 = 0x8302;
pub const TABLE_DERIVED_INDEX_PROOF_ROW: u16 = 0x8303;
pub const TABLE_BOUNDARY_SCHEMA_ROW: u16 = 0x8401;
pub const TABLE_BOUNDARY_VALUE_ROW: u16 = 0x8402;
pub const TABLE_BOUNDARY_MIGRATION_ROW: u16 = 0x8403;
pub const TABLE_AUTHZ_SCHEMA_ROW: u16 = 0x8501;
pub const TABLE_AUTHZ_TUPLE_PAGE_ROW: u16 = 0x8502;
pub const TABLE_PERSONALDB_GROUP_ROW: u16 = 0x8601;
pub const TABLE_PERSONALDB_DATA_LOCATOR_ROW: u16 = 0x8602;
pub const TABLE_REGISTRY_VERSION_ROW: u16 = 0x8701;
pub const TABLE_REGISTRY_BLOB_LOCATOR_ROW: u16 = 0x8702;
pub const TABLE_GATEWAY_METADATA_ROW: u16 = 0x8703;
pub const TABLE_GIT_SOURCE_MANIFEST_ROW: u16 = 0x8704;
pub const TABLE_MESH_NODE_ROW: u16 = 0x8801;
pub const TABLE_MESH_PARTITION_ROW: u16 = 0x8802;
pub const TABLE_REPAIR_FINDING_ROW: u16 = 0x8803;
pub const TABLE_BUCKET_CURRENT_BY_NAME_ROW: u16 = 0x8804;
pub const TABLE_BUCKET_CURRENT_BY_ID_ROW: u16 = 0x8805;
pub const TABLE_CONTROL_CURRENT_ROW: u16 = 0x8806;
pub const TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW: u16 = 0x8807;
pub const TABLE_ROOT_CATALOG_CURRENT_ROW: u16 = 0x8808;
pub const TABLE_QUORUM_PROFILE_CURRENT_ROW: u16 = 0x8809;
pub const TABLE_OWNERSHIP_FENCE_ROW: u16 = 0x8901;
pub const TABLE_PARTITION_OWNER_ROW: u16 = 0x8902;
pub const TABLE_TASK_LEASE_ROW: u16 = 0x8903;
pub const TABLE_TASK_CURRENT_ROW: u16 = 0x8904;
pub const TABLE_CORE_FENCE_ROW: u16 = 0x8905;
pub const TABLE_MATERIALISATION_CURSOR_ROW: u16 = 0x8a01;
pub const TABLE_WRITER_SEGMENT_ROW: u16 = 0x8a02;
pub const TABLE_WATCH_CHECKPOINT_ROW: u16 = 0x8a03;
pub const TABLE_LANDED_BYTE_REF_ROW: u16 = 0x8b02;
pub const TABLE_REFCOUNT_ROW: u16 = 0x8b01;
pub const TABLE_OBSERVABILITY_CURSOR_ROW: u16 = 0x8c01;
pub const TABLE_DIAGNOSTIC_ROW: u16 = 0x8c02;
pub const TABLE_NODE_SIGNING_KEYPAIR_ROW: u16 = 0x8d01;

const CORE_META_KEY_VERSION: u8 = 1;
const CORE_META_VALUE_SCHEMA_VERSION: u32 = 1;
pub(crate) const CORE_META_MAX_VALUE_BYTES: usize = 64 * 1024;
pub(crate) const CORE_META_MAX_INLINE_PAYLOAD_BYTES: usize = 32 * 1024;
pub(crate) const CORE_META_INLINE_MANIFEST_BODY_MAX_BYTES: usize = 32 * 1024;
pub(crate) const CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES: usize = 16 * 1024;
const CORE_META_OBJECT_VERSION_MAX_PAYLOAD_BYTES: usize = CORE_META_MAX_VALUE_BYTES;

static META_DB_CACHE: LazyLock<StdMutex<BTreeMap<PathBuf, Weak<DB>>>> =
    LazyLock::new(|| StdMutex::new(BTreeMap::new()));

#[derive(Clone)]
pub struct CoreMetaStore {
    db: Arc<DB>,
}

impl std::fmt::Debug for CoreMetaStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CoreMetaStore").finish_non_exhaustive()
    }
}

#[derive(Debug, Clone)]
pub struct CoreMetaEncodedRow<'a> {
    pub cf: &'a str,
    pub core_meta_key: &'a [u8],
    pub value_envelope: &'a [u8],
    pub delete_marker: bool,
}

#[derive(Debug, Clone)]
pub struct CoreMetaEncodedOwnedRow {
    pub cf: String,
    pub core_meta_key: Vec<u8>,
    pub value_envelope: Vec<u8>,
    pub delete_marker: bool,
    pub root_key_hash: String,
    pub root_generation: u64,
    pub visibility_state: CoreMetaVisibilityState,
}

#[derive(Debug, Clone)]
pub struct CoreMetaInventoryRow {
    pub cf: String,
    pub core_meta_key: Vec<u8>,
    pub row_hash: String,
    pub root_key_hash: String,
    pub root_generation: u64,
    pub visibility_state: CoreMetaVisibilityState,
}

pub struct CoreMetaRecord {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CoreMetaTuplePart<'a> {
    Utf8(&'a str),
    U64(u64),
    I64(i64),
    Hash(&'a str),
    Raw(&'a [u8]),
    Bool(bool),
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaValueEnvelope {
    #[prost(uint32, tag = "1")]
    table_id: u32,
    #[prost(uint32, tag = "2")]
    schema_version: u32,
    #[prost(bytes, tag = "3")]
    payload: Vec<u8>,
    #[prost(string, tag = "4")]
    payload_hash: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Enumeration)]
#[repr(i32)]
pub enum CoreMetaVisibilityState {
    Unspecified = 0,
    Pending = 1,
    Committed = 2,
    Aborted = 3,
    RolledBack = 4,
}

#[derive(Clone, PartialEq, Message)]
pub struct CoreMetaRowCommonProto {
    #[prost(string, tag = "1")]
    pub realm_id: String,
    #[prost(string, tag = "2")]
    pub root_key_hash: String,
    #[prost(uint64, tag = "3")]
    pub root_generation: u64,
    #[prost(string, tag = "4")]
    pub transaction_id: String,
    #[prost(enumeration = "CoreMetaVisibilityState", tag = "5")]
    pub visibility_state: i32,
    #[prost(uint64, tag = "6")]
    pub created_at_unix_nanos: u64,
    #[prost(uint32, tag = "7")]
    pub payload_schema_version: u32,
}

impl CoreMetaRowCommonProto {
    pub(crate) fn visibility_state_enum(&self) -> CoreMetaVisibilityState {
        CoreMetaVisibilityState::try_from(self.visibility_state)
            .unwrap_or(CoreMetaVisibilityState::Unspecified)
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct CoreMetaLocatorProto {
    #[prost(string, tag = "1")]
    pub storage_kind: String,
    #[prost(string, tag = "2")]
    pub manifest_hash: String,
    #[prost(string, tag = "3")]
    pub root_key_hash: String,
    #[prost(uint64, tag = "4")]
    pub root_generation: u64,
    #[prost(string, tag = "5")]
    pub locator_hash: String,
    #[prost(bytes, tag = "6")]
    pub encoded_locator: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CoreMetaInlineOrLocatorProto {
    #[prost(bytes, tag = "1")]
    pub inline_payload: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub locator: Option<CoreMetaLocatorProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaCommonPrefixProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
}

#[derive(Clone, PartialEq, Message)]
struct CoreMetaSchemaVersionRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(uint32, tag = "2")]
    core_meta_schema_version: u32,
    #[prost(string, tag = "3")]
    created_by_binary_version: String,
    #[prost(string, tag = "4")]
    minimum_supported_binary_version: String,
    #[prost(string, tag = "5")]
    column_family_set_hash: String,
}

#[derive(Clone, PartialEq, Message)]
struct InlinePayloadRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    payload_hash: String,
    #[prost(uint64, tag = "3")]
    raw_payload_length: u64,
    #[prost(bytes, tag = "4")]
    payload_bytes: Vec<u8>,
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

impl CoreMetaStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if let Some(existing) = META_DB_CACHE
            .lock()
            .expect("CoreStore metadata DB cache lock poisoned")
            .get(&path)
            .and_then(Weak::upgrade)
        {
            let store = Self { db: existing };
            store.ensure_schema_version_row()?;
            return Ok(store);
        }

        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        options.set_max_open_files(512);
        let descriptors = column_families()
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, cf_options(name)))
            .collect::<Vec<_>>();
        let db = Arc::new(
            DB::open_cf_descriptors(&options, &path, descriptors).with_context(|| {
                format!("open CoreStore RocksDB metadata at {}", path.display())
            })?,
        );
        let store = Self { db };
        store.ensure_schema_version_row()?;
        META_DB_CACHE
            .lock()
            .expect("CoreStore metadata DB cache lock poisoned")
            .insert(path, Arc::downgrade(&store.db));
        Ok(store)
    }

    fn ensure_schema_version_row(&self) -> Result<()> {
        let key = core_meta_schema_key();
        if self
            .get(CF_META_VERSION, TABLE_META_SCHEMA_VERSION_ROW, &key)?
            .is_some()
        {
            return Ok(());
        }
        let row = CoreMetaSchemaVersionRowProto {
            common: Some(CoreMetaRowCommonProto {
                realm_id: String::new(),
                root_key_hash: String::new(),
                root_generation: 0,
                transaction_id: String::new(),
                visibility_state: CoreMetaVisibilityState::Committed as i32,
                created_at_unix_nanos: 0,
                payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
            }),
            core_meta_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
            created_by_binary_version: env!("CARGO_PKG_VERSION").to_string(),
            minimum_supported_binary_version: env!("CARGO_PKG_VERSION").to_string(),
            column_family_set_hash: column_family_set_hash(),
        };
        let mut payload = Vec::new();
        row.encode(&mut payload)?;
        self.put(
            CF_META_VERSION,
            TABLE_META_SCHEMA_VERSION_ROW,
            &key,
            &payload,
        )
    }

    pub fn put(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
        payload: &[u8],
    ) -> Result<()> {
        self.put_named(cf, table_id, tuple_key, payload)
    }

    pub fn put_named(
        &self,
        cf: &str,
        table_id: u16,
        tuple_key: &[u8],
        payload: &[u8],
    ) -> Result<()> {
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let value = encode_envelope(cf, table_id, payload)?;
        let bytes = (key.len() + value.len()) as u64;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let started_at = Instant::now();
        self.db
            .put_cf_opt(&cf, key, value, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "put",
            cf_name,
            table_id,
            1,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub fn delete(&self, cf: &'static str, table_id: u16, tuple_key: &[u8]) -> Result<()> {
        self.delete_named(cf, table_id, tuple_key)
    }

    pub fn delete_named(&self, cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<()> {
        validate_meta_payload(cf, table_id, 0)?;
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let bytes = key.len() as u64;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let started_at = Instant::now();
        self.db.delete_cf_opt(&cf, key, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "delete",
            cf_name,
            table_id,
            1,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub fn put_inline_payload(&self, tuple_key: &[u8], raw_payload: &[u8]) -> Result<()> {
        let payload =
            encode_core_meta_inline_payload_row(raw_payload, local_committed_row_common())?;
        self.put(
            CF_INLINE_PAYLOADS,
            TABLE_INLINE_PAYLOAD_ROW,
            tuple_key,
            &payload,
        )
    }

    pub fn get_inline_payload(&self, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(CF_INLINE_PAYLOADS, TABLE_INLINE_PAYLOAD_ROW, tuple_key)?
            .map(|payload| decode_inline_payload_row(&payload))
            .transpose()
    }

    pub fn get(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        self.get_named(cf, table_id, tuple_key)
    }

    pub fn get_named(&self, cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<Option<Vec<u8>>> {
        validate_meta_payload(cf, table_id, 0)?;
        let key = core_meta_key(table_id, 0, tuple_key)?;
        let key_bytes = key.len() as u64;
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let started_at = Instant::now();
        let value = self.db.get_cf(&cf, key)?;
        let bytes = key_bytes + value.as_ref().map(|value| value.len() as u64).unwrap_or(0);
        crate::perf::record_coremeta_duration(
            "get",
            cf_name,
            table_id,
            u64::from(value.is_some()),
            bytes,
            started_at.elapsed(),
        );
        value
            .map(|value| decode_envelope(cf_name, table_id, &value))
            .transpose()
    }

    pub fn scan_prefix(
        &self,
        cf: &'static str,
        table_id: u16,
        tuple_prefix: &[u8],
    ) -> Result<Vec<CoreMetaRecord>> {
        validate_meta_payload(cf, table_id, 0)?;
        let prefix = core_meta_partition_prefix(table_id, 0);
        let cf_name = cf;
        let cf = self.cf(cf_name)?;
        let iter = self
            .db
            .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward));
        let mut records = Vec::new();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;
        let started_at = Instant::now();
        for item in iter {
            let (key, value) = item?;
            if !key.starts_with(&prefix) {
                break;
            }
            scanned = scanned.saturating_add(1);
            bytes = bytes.saturating_add((key.len() + value.len()) as u64);
            let tuple_key = decode_core_meta_tuple_key(&key)?;
            if !core_meta_tuple_key_has_prefix(tuple_key, tuple_prefix)? {
                continue;
            }
            records.push(CoreMetaRecord {
                key: key.to_vec(),
                payload: decode_envelope(cf_name, table_id, &value)?,
            });
        }
        crate::perf::record_coremeta_duration(
            "scan_prefix",
            cf_name,
            table_id,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(records)
    }

    pub fn write_batch(&self, ops: &[CoreMetaBatchOp<'_>]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let mut bytes = 0_u64;
        for op in ops {
            let cf = self.cf(op.cf)?;
            let key = core_meta_key(op.table_id, 0, op.tuple_key)?;
            match op.kind {
                CoreMetaBatchOpKind::Put(payload) => {
                    let common = match op.common.clone() {
                        Some(common) => common,
                        None => common_for_envelope_payload(payload)?,
                    };
                    let value = encode_envelope_with_common(op.cf, op.table_id, payload, common)?;
                    bytes = bytes.saturating_add((key.len() + value.len()) as u64);
                    batch.put_cf(&cf, key, value);
                }
                CoreMetaBatchOpKind::Delete => {
                    validate_meta_payload(op.cf, op.table_id, 0)?;
                    if let Some(common) = op.common.as_ref() {
                        validate_coremeta_common_shape(common)?;
                    }
                    bytes = bytes.saturating_add(key.len() as u64);
                    batch.delete_cf(&cf, key);
                }
            }
        }
        let started_at = Instant::now();
        self.db.write_opt(batch, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "write_batch",
            "multi",
            0,
            ops.len() as u64,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub fn write_local_committed_batch(&self, ops: &[CoreMetaBatchOp<'_>]) -> Result<()> {
        let owned = self.encode_batch_ops(ops)?;
        let borrowed = owned
            .iter()
            .map(|row| CoreMetaEncodedRow {
                cf: row.cf.as_str(),
                core_meta_key: &row.core_meta_key,
                value_envelope: &row.value_envelope,
                delete_marker: row.delete_marker,
            })
            .collect::<Vec<_>>();
        self.write_encoded_rows(&borrowed)
    }

    pub fn encode_batch_ops(
        &self,
        ops: &[CoreMetaBatchOp<'_>],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let mut rows = Vec::with_capacity(ops.len());
        for op in ops {
            let key = core_meta_key(op.table_id, 0, op.tuple_key)?;
            match op.kind {
                CoreMetaBatchOpKind::Put(payload) => {
                    let common = match op.common.clone() {
                        Some(common) => common,
                        None => common_for_envelope_payload(payload)?,
                    };
                    let value_envelope =
                        encode_envelope_with_common(op.cf, op.table_id, payload, common.clone())?;
                    rows.push(CoreMetaEncodedOwnedRow {
                        cf: op.cf.to_string(),
                        core_meta_key: key,
                        value_envelope,
                        delete_marker: false,
                        root_key_hash: common.root_key_hash.clone(),
                        root_generation: common.root_generation,
                        visibility_state: common.visibility_state_enum(),
                    });
                }
                CoreMetaBatchOpKind::Delete => {
                    validate_meta_payload(op.cf, op.table_id, 0)?;
                    let common = op.common.clone().unwrap_or_else(local_committed_row_common);
                    validate_coremeta_common_shape(&common)?;
                    rows.push(CoreMetaEncodedOwnedRow {
                        cf: op.cf.to_string(),
                        core_meta_key: key,
                        value_envelope: Vec::new(),
                        delete_marker: true,
                        root_key_hash: common.root_key_hash.clone(),
                        root_generation: common.root_generation,
                        visibility_state: common.visibility_state_enum(),
                    });
                }
            }
        }
        Ok(rows)
    }

    pub fn write_encoded_rows(&self, rows: &[CoreMetaEncodedRow<'_>]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let mut bytes = 0_u64;
        for row in rows {
            let cf = self.cf(row.cf)?;
            let table_id = decode_core_meta_table_id(row.core_meta_key)?;
            if row.delete_marker {
                if !row.value_envelope.is_empty() {
                    bail!("CoreMeta delete row must not carry a value envelope");
                }
                validate_meta_payload(row.cf, table_id, 0)?;
                bytes = bytes.saturating_add(row.core_meta_key.len() as u64);
                batch.delete_cf(&cf, row.core_meta_key);
                continue;
            }
            let (payload, common) =
                decode_envelope_with_common(row.cf, table_id, row.value_envelope)?;
            let canonical_envelope =
                encode_envelope_with_common(row.cf, table_id, &payload, common)?;
            if canonical_envelope != row.value_envelope {
                bail!("CoreMeta encoded row envelope is not canonical");
            }
            bytes =
                bytes.saturating_add((row.core_meta_key.len() + row.value_envelope.len()) as u64);
            batch.put_cf(&cf, row.core_meta_key, row.value_envelope);
        }
        let started_at = Instant::now();
        self.db.write_opt(batch, &durable_write_options())?;
        crate::perf::record_coremeta_duration(
            "write_encoded_rows",
            "multi",
            0,
            rows.len() as u64,
            bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    pub fn get_encoded_rows(
        &self,
        cf_name: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let cf = self.cf(cf_name)?;
        let mut rows = Vec::new();
        for key in keys {
            let table_id = decode_core_meta_table_id(key)?;
            if let Some(value) = self.db.get_cf(&cf, key)? {
                let (payload, common) = decode_envelope_with_common(cf_name, table_id, &value)?;
                let canonical_envelope =
                    encode_envelope_with_common(cf_name, table_id, &payload, common.clone())?;
                rows.push(CoreMetaEncodedOwnedRow {
                    cf: cf_name.to_string(),
                    core_meta_key: key.clone(),
                    value_envelope: canonical_envelope,
                    delete_marker: false,
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                });
            }
        }
        Ok(rows)
    }

    pub fn scan_all_encoded_rows(&self) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let mut rows = Vec::new();
        let started_at = Instant::now();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;

        for cf_name in column_families() {
            let cf = self.cf(cf_name)?;
            let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
            for item in iter {
                let (key, value) = item?;
                scanned = scanned.saturating_add(1);
                bytes = bytes.saturating_add((key.len() + value.len()) as u64);
                let table_id = decode_core_meta_table_id(&key)?;
                let (payload, common) = decode_envelope_with_common(cf_name, table_id, &value)?;
                let canonical_envelope =
                    encode_envelope_with_common(cf_name, table_id, &payload, common.clone())?;
                rows.push(CoreMetaEncodedOwnedRow {
                    cf: (*cf_name).to_string(),
                    core_meta_key: key.to_vec(),
                    value_envelope: canonical_envelope,
                    delete_marker: false,
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                });
            }
        }

        crate::perf::record_coremeta_duration(
            "scan_all_encoded_rows",
            "multi",
            0,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(rows)
    }

    pub fn scan_encoded_rows_for_root(
        &self,
        root_key_hash: &str,
        after_generation: u64,
        limit: usize,
    ) -> Result<Vec<CoreMetaEncodedOwnedRow>> {
        let mut rows = Vec::new();
        let scan_limit = limit.max(1);
        let started_at = Instant::now();
        let mut scanned = 0_u64;
        let mut bytes = 0_u64;

        for cf_name in column_families() {
            let cf = self.cf(cf_name)?;
            let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
            for item in iter {
                let (key, value) = item?;
                scanned = scanned.saturating_add(1);
                bytes = bytes.saturating_add((key.len() + value.len()) as u64);
                let table_id = decode_core_meta_table_id(&key)?;
                let (payload, common) = decode_envelope_with_common(cf_name, table_id, &value)?;
                if common.root_key_hash != root_key_hash {
                    continue;
                }
                if common.root_generation <= after_generation {
                    continue;
                }
                let canonical_envelope =
                    encode_envelope_with_common(cf_name, table_id, &payload, common.clone())?;
                rows.push(CoreMetaEncodedOwnedRow {
                    cf: (*cf_name).to_string(),
                    core_meta_key: key.to_vec(),
                    value_envelope: canonical_envelope,
                    delete_marker: false,
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                });
                if rows.len() >= scan_limit {
                    crate::perf::record_coremeta_duration(
                        "scan_encoded_rows_for_root",
                        "multi",
                        0,
                        scanned,
                        bytes,
                        started_at.elapsed(),
                    );
                    return Ok(rows);
                }
            }
        }

        crate::perf::record_coremeta_duration(
            "scan_encoded_rows_for_root",
            "multi",
            0,
            scanned,
            bytes,
            started_at.elapsed(),
        );
        Ok(rows)
    }

    pub fn inventory_rows_for_root(
        &self,
        root_key_hash: &str,
        from_generation: u64,
        to_generation: u64,
        limit: usize,
    ) -> Result<Vec<CoreMetaInventoryRow>> {
        let mut rows = Vec::new();
        let scan_limit = limit.max(1);
        for cf_name in column_families() {
            let cf = self.cf(cf_name)?;
            for item in self.db.iterator_cf(&cf, IteratorMode::Start) {
                let (key, value) = item?;
                let table_id = decode_core_meta_table_id(&key)?;
                let (payload, common) = decode_envelope_with_common(cf_name, table_id, &value)?;
                if common.root_key_hash != root_key_hash {
                    continue;
                }
                if common.root_generation < from_generation
                    || common.root_generation > to_generation
                {
                    continue;
                }
                let canonical_envelope =
                    encode_envelope_with_common(cf_name, table_id, &payload, common.clone())?;
                rows.push(CoreMetaInventoryRow {
                    cf: (*cf_name).to_string(),
                    core_meta_key: key.to_vec(),
                    row_hash: super::coremeta_quorum::core_meta_encoded_row_hash(
                        cf_name,
                        &key,
                        &canonical_envelope,
                    ),
                    root_key_hash: common.root_key_hash.clone(),
                    root_generation: common.root_generation,
                    visibility_state: common.visibility_state_enum(),
                });
                if rows.len() >= scan_limit {
                    return Ok(rows);
                }
            }
        }
        Ok(rows)
    }

    fn cf(&self, name: &str) -> Result<&rocksdb::ColumnFamily> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| anyhow!("missing CoreStore RocksDB column family {name}"))
    }
}

pub fn encode_core_meta_inline_payload_row(
    raw_payload: &[u8],
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    if raw_payload.len() > CORE_META_MAX_INLINE_PAYLOAD_BYTES {
        bail!(
            "CoreStore inline payload is {} bytes before RocksDB compression, exceeding {} bytes",
            raw_payload.len(),
            CORE_META_MAX_INLINE_PAYLOAD_BYTES
        );
    }
    let row = InlinePayloadRowProto {
        common: Some(common),
        payload_hash: core_meta_inline_payload_hash(raw_payload),
        raw_payload_length: raw_payload.len() as u64,
        payload_bytes: raw_payload.to_vec(),
    };
    let mut payload = Vec::new();
    row.encode(&mut payload)?;
    Ok(payload)
}

pub fn core_meta_tuple_key(parts: &[CoreMetaTuplePart<'_>]) -> Result<Vec<u8>> {
    let part_count =
        u16::try_from(parts.len()).context("CoreMetaTupleKey part count exceeds u16")?;
    let mut key = Vec::new();
    key.extend_from_slice(&part_count.to_le_bytes());
    for part in parts {
        encode_core_meta_tuple_part(&mut key, *part)?;
    }
    validate_core_meta_tuple_key(&key)?;
    Ok(key)
}

pub fn core_meta_record_tuple_key(encoded_key: &[u8]) -> Result<&[u8]> {
    decode_core_meta_tuple_key(encoded_key)
}

pub fn core_meta_committed_row_common(
    realm_id: impl Into<String>,
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_row_common_with_visibility(
        realm_id,
        root_key_hash,
        root_generation,
        transaction_id,
        created_at_unix_nanos,
        CoreMetaVisibilityState::Committed,
    )
}

pub fn core_meta_pending_row_common(
    realm_id: impl Into<String>,
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    created_at_unix_nanos: u64,
) -> CoreMetaRowCommonProto {
    core_meta_row_common_with_visibility(
        realm_id,
        root_key_hash,
        root_generation,
        transaction_id,
        created_at_unix_nanos,
        CoreMetaVisibilityState::Pending,
    )
}

pub fn core_meta_row_common_with_visibility(
    realm_id: impl Into<String>,
    root_key_hash: impl Into<String>,
    root_generation: u64,
    transaction_id: impl Into<String>,
    created_at_unix_nanos: u64,
    visibility_state: CoreMetaVisibilityState,
) -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: realm_id.into(),
        root_key_hash: root_key_hash.into(),
        root_generation,
        transaction_id: transaction_id.into(),
        visibility_state: visibility_state as i32,
        created_at_unix_nanos,
        payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
    }
}

pub fn core_meta_root_key_hash(root_anchor_key: &str) -> String {
    let mut bytes = Vec::new();
    for part in ["anvil.root.key.v1", root_anchor_key] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    format!("sha256:{}", sha256_hex(&bytes))
}

pub fn core_meta_locator_from_manifest_locator(
    locator: &CoreManifestLocator,
) -> Result<CoreMetaLocatorProto> {
    let encoded_locator = encode_manifest_locator_proto(locator)?;
    Ok(CoreMetaLocatorProto {
        storage_kind: "corestore-blocks".to_string(),
        manifest_hash: locator.manifest_hash.clone(),
        root_key_hash: core_meta_root_key_hash(&locator.manifest_ref.logical_file_id),
        root_generation: locator.manifest_ref.writer_generation,
        locator_hash: format!("sha256:{}", sha256_hex(&encoded_locator)),
        encoded_locator,
    })
}

pub fn core_meta_locator_to_manifest_locator(
    locator: &CoreMetaLocatorProto,
) -> Result<CoreManifestLocator> {
    if locator.storage_kind != "corestore-blocks" {
        bail!(
            "CoreMeta locator storage kind {} is not corestore-blocks",
            locator.storage_kind
        );
    }
    let expected_hash = format!("sha256:{}", sha256_hex(&locator.encoded_locator));
    if locator.locator_hash != expected_hash {
        bail!("CoreMeta locator hash mismatch");
    }
    let decoded = decode_manifest_locator_proto(&locator.encoded_locator)?;
    if decoded.manifest_hash != locator.manifest_hash {
        bail!("CoreMeta locator manifest hash mismatch");
    }
    if locator.root_key_hash != core_meta_root_key_hash(&decoded.manifest_ref.logical_file_id) {
        bail!("CoreMeta locator root key hash mismatch");
    }
    if locator.root_generation != decoded.manifest_ref.writer_generation {
        bail!("CoreMeta locator root generation mismatch");
    }
    Ok(decoded)
}

pub struct CoreMetaBatchOp<'a> {
    pub cf: &'static str,
    pub table_id: u16,
    pub tuple_key: &'a [u8],
    pub common: Option<CoreMetaRowCommonProto>,
    pub kind: CoreMetaBatchOpKind<'a>,
}

pub enum CoreMetaBatchOpKind<'a> {
    Put(&'a [u8]),
    Delete,
}

fn column_families() -> &'static [&'static str] {
    &[
        CF_META_VERSION,
        CF_ROOT_CACHE,
        CF_TRANSACTIONS,
        CF_OBJECT_HEADS,
        CF_OBJECT_VERSIONS,
        CF_INLINE_PAYLOADS,
        CF_STREAM_HEADS,
        CF_STREAM_RECORDS,
        CF_INDEX_DEFS,
        CF_INDEX_ROWS,
        CF_BOUNDARY,
        CF_AUTHZ,
        CF_PERSONALDB,
        CF_REGISTRY,
        CF_MESH,
        CF_LEASES_FENCES,
        CF_MATERIALISATION,
        CF_REFCOUNTS,
        CF_OBSERVABILITY,
    ]
}

pub fn core_meta_column_families() -> &'static [&'static str] {
    column_families()
}

fn cf_options(name: &str) -> Options {
    let mut options = Options::default();
    options.set_compression_type(rocksdb::DBCompressionType::Lz4);
    options.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(11));
    options.optimize_for_point_lookup(64);
    if matches!(
        name,
        CF_STREAM_RECORDS | CF_INDEX_ROWS | CF_OBJECT_VERSIONS | CF_INLINE_PAYLOADS | CF_AUTHZ
    ) {
        options.set_compression_type(rocksdb::DBCompressionType::Zstd);
    }
    options
}

fn durable_write_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options
}

fn core_meta_key(table_id: u16, partition_id: u64, tuple_key: &[u8]) -> Result<Vec<u8>> {
    if tuple_key.len() > u16::MAX as usize {
        bail!("CoreMetaKey tuple key exceeds u16 length");
    }
    validate_core_meta_tuple_key(tuple_key)?;
    let mut key = Vec::with_capacity(13 + tuple_key.len());
    key.push(CORE_META_KEY_VERSION);
    key.extend_from_slice(&table_id.to_le_bytes());
    key.extend_from_slice(&partition_id.to_le_bytes());
    key.extend_from_slice(&(tuple_key.len() as u16).to_le_bytes());
    key.extend_from_slice(tuple_key);
    Ok(key)
}

fn core_meta_partition_prefix(table_id: u16, partition_id: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    key.push(CORE_META_KEY_VERSION);
    key.extend_from_slice(&table_id.to_le_bytes());
    key.extend_from_slice(&partition_id.to_le_bytes());
    key
}

fn decode_core_meta_table_id(key: &[u8]) -> Result<u16> {
    if key.len() < 13 {
        bail!("CoreMetaKey is shorter than fixed header");
    }
    if key[0] != CORE_META_KEY_VERSION {
        bail!("CoreMetaKey has unsupported version {}", key[0]);
    }
    let table_id = u16::from_le_bytes([key[1], key[2]]);
    let _ = decode_core_meta_tuple_key(key)?;
    Ok(table_id)
}

fn decode_core_meta_tuple_key(key: &[u8]) -> Result<&[u8]> {
    if key.len() < 13 {
        bail!("CoreMetaKey is shorter than fixed header");
    }
    if key[0] != CORE_META_KEY_VERSION {
        bail!("CoreMetaKey has unsupported version {}", key[0]);
    }
    let len = u16::from_le_bytes([key[11], key[12]]) as usize;
    let end = 13usize
        .checked_add(len)
        .ok_or_else(|| anyhow!("CoreMetaKey tuple key length overflow"))?;
    if key.len() != end {
        bail!("CoreMetaKey tuple key length mismatch");
    }
    let tuple_key = &key[13..end];
    validate_core_meta_tuple_key(tuple_key)?;
    Ok(tuple_key)
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DecodedCoreMetaTuplePart<'a> {
    kind: u8,
    value: &'a [u8],
}

fn validate_core_meta_tuple_key(tuple_key: &[u8]) -> Result<()> {
    decode_core_meta_tuple_parts(tuple_key).map(|_| ())
}

fn encode_core_meta_tuple_part(key: &mut Vec<u8>, part: CoreMetaTuplePart<'_>) -> Result<()> {
    let (kind, value): (u8, Vec<u8>) = match part {
        CoreMetaTuplePart::Utf8(value) => (0x01, value.as_bytes().to_vec()),
        CoreMetaTuplePart::U64(value) => (0x02, value.to_be_bytes().to_vec()),
        CoreMetaTuplePart::I64(value) => {
            let sortable = (value as u64) ^ (1_u64 << 63);
            (0x03, sortable.to_be_bytes().to_vec())
        }
        CoreMetaTuplePart::Hash(value) => (0x04, normalise_tuple_hash_part(value).into_bytes()),
        CoreMetaTuplePart::Raw(value) => (0x05, value.to_vec()),
        CoreMetaTuplePart::Bool(value) => (0x06, vec![u8::from(value)]),
    };
    let value_len =
        u16::try_from(value.len()).context("CoreMetaTupleKey part value exceeds u16")?;
    key.push(kind);
    key.push(0);
    key.extend_from_slice(&value_len.to_le_bytes());
    key.extend_from_slice(&value);
    Ok(())
}

fn normalise_tuple_hash_part(value: &str) -> String {
    if value.contains(':') {
        return value.to_string();
    }
    if value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return format!("blake3:{value}");
    }
    value.to_string()
}

fn core_meta_tuple_key_has_prefix(tuple_key: &[u8], tuple_prefix: &[u8]) -> Result<bool> {
    if tuple_prefix.is_empty() {
        validate_core_meta_tuple_key(tuple_key)?;
        return Ok(true);
    }
    let key_parts = decode_core_meta_tuple_parts(tuple_key)?;
    let prefix_parts = decode_core_meta_tuple_parts(tuple_prefix)?;
    if prefix_parts.len() > key_parts.len() {
        return Ok(false);
    }
    Ok(key_parts
        .iter()
        .zip(prefix_parts.iter())
        .all(|(part, prefix)| part == prefix))
}

fn decode_core_meta_tuple_parts(tuple_key: &[u8]) -> Result<Vec<DecodedCoreMetaTuplePart<'_>>> {
    if tuple_key.len() < 2 {
        bail!("CoreMetaTupleKey missing part_count");
    }
    let part_count = u16::from_le_bytes([tuple_key[0], tuple_key[1]]) as usize;
    let mut offset = 2usize;
    let mut parts = Vec::with_capacity(part_count);
    for _ in 0..part_count {
        if tuple_key.len().saturating_sub(offset) < 4 {
            bail!("CoreMetaTupleKey part header truncated");
        }
        let kind = tuple_key[offset];
        let flags = tuple_key[offset + 1];
        let value_len = u16::from_le_bytes([tuple_key[offset + 2], tuple_key[offset + 3]]) as usize;
        offset += 4;
        if flags != 0 {
            bail!("CoreMetaTupleKey part has unsupported flags {flags}");
        }
        let end = offset
            .checked_add(value_len)
            .ok_or_else(|| anyhow!("CoreMetaTupleKey part length overflow"))?;
        if end > tuple_key.len() {
            bail!("CoreMetaTupleKey part value truncated");
        }
        validate_core_meta_tuple_part(kind, &tuple_key[offset..end])?;
        parts.push(DecodedCoreMetaTuplePart {
            kind,
            value: &tuple_key[offset..end],
        });
        offset = end;
    }
    if offset != tuple_key.len() {
        bail!("CoreMetaTupleKey has trailing bytes");
    }
    Ok(parts)
}

fn validate_core_meta_tuple_part(kind: u8, value: &[u8]) -> Result<()> {
    match kind {
        0x01 => {
            let s = std::str::from_utf8(value).context("CoreMetaTupleKey utf8 part is invalid")?;
            if s.as_bytes().contains(&0) {
                bail!("CoreMetaTupleKey utf8 part contains NUL");
            }
            if !s.chars().eq(s.nfc()) {
                bail!("CoreMetaTupleKey utf8 part must be NFC-normalized");
            }
        }
        0x02 | 0x03 => {
            if value.len() != 8 {
                bail!("CoreMetaTupleKey integer part must be 8 bytes");
            }
        }
        0x04 => {
            let s = std::str::from_utf8(value).context("CoreMetaTupleKey hash part is invalid")?;
            let Some((algorithm, hex_value)) = s.split_once(':') else {
                bail!("CoreMetaTupleKey hash part must be algorithm:hex ASCII");
            };
            if algorithm.is_empty()
                || hex_value.is_empty()
                || s.bytes().any(|byte| !byte.is_ascii())
                || hex_value.bytes().any(|byte| !byte.is_ascii_hexdigit())
            {
                bail!("CoreMetaTupleKey hash part must be algorithm:hex ASCII");
            }
        }
        0x05 => {}
        0x06 => {
            if !matches!(value, [0] | [1]) {
                bail!("CoreMetaTupleKey bool part must be 0x00 or 0x01");
            }
        }
        other => bail!("CoreMetaTupleKey has unknown part kind {other:#04x}"),
    }
    Ok(())
}

fn local_committed_row_common() -> CoreMetaRowCommonProto {
    CoreMetaRowCommonProto {
        realm_id: String::new(),
        root_key_hash: String::new(),
        root_generation: 0,
        transaction_id: String::new(),
        visibility_state: CoreMetaVisibilityState::Committed as i32,
        created_at_unix_nanos: 0,
        payload_schema_version: CORE_META_VALUE_SCHEMA_VERSION,
    }
}

fn decode_inline_payload_row(payload: &[u8]) -> Result<Vec<u8>> {
    let row = InlinePayloadRowProto::decode(payload)?;
    let mut canonical = Vec::new();
    row.encode(&mut canonical)?;
    if canonical != payload {
        bail!("CoreStore inline payload row is not deterministic protobuf");
    }
    if row.raw_payload_length != row.payload_bytes.len() as u64 {
        bail!("CoreStore inline payload row length mismatch");
    }
    if row.payload_bytes.len() > CORE_META_MAX_INLINE_PAYLOAD_BYTES {
        bail!("CoreStore inline payload row exceeds bounded raw payload size");
    }
    if row.payload_hash != core_meta_inline_payload_hash(&row.payload_bytes) {
        bail!("CoreStore inline payload row hash mismatch");
    }
    Ok(row.payload_bytes)
}

fn extract_row_common_from_payload(payload: &[u8]) -> Result<CoreMetaRowCommonProto> {
    CoreMetaCommonPrefixProto::decode(payload)
        .context("decode CoreMeta row common prefix")?
        .common
        .ok_or_else(|| anyhow!("CoreMeta row payload is missing common field"))
}

pub(crate) fn core_meta_row_common_from_payload(payload: &[u8]) -> Result<CoreMetaRowCommonProto> {
    extract_row_common_from_payload(payload)
}

fn core_meta_inline_payload_hash(payload: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"anvil.coremeta.inline_payload.v1");
    hasher.update(&[0]);
    hasher.update(payload);
    format!("blake3:{}", hasher.finalize().to_hex())
}

fn core_meta_schema_key() -> Vec<u8> {
    let mut key = Vec::new();
    key.extend_from_slice(&1u16.to_le_bytes());
    key.push(0x01);
    key.push(0);
    key.extend_from_slice(&6u16.to_le_bytes());
    key.extend_from_slice(b"schema");
    key
}

fn column_family_set_hash() -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"anvil.coremeta.column_families.v1");
    for name in column_families() {
        hasher.update(&[0]);
        hasher.update(name.as_bytes());
    }
    format!("blake3:{}", hasher.finalize().to_hex())
}

#[derive(Debug, Clone, Copy)]
struct CoreMetaTableSpec {
    cf: &'static str,
    max_payload_bytes: usize,
}

fn table_spec(table_id: u16) -> Result<CoreMetaTableSpec> {
    let spec = match table_id {
        TABLE_META_SCHEMA_VERSION_ROW => CoreMetaTableSpec {
            cf: CF_META_VERSION,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_ROOT_CACHE_ROW => CoreMetaTableSpec {
            cf: CF_ROOT_CACHE,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_TRANSACTION_LOCATOR_ROW
        | TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW
        | TABLE_INLINE_MANIFEST_BODY_ROW
        | TABLE_EXPLICIT_TRANSACTION_ROW
        | TABLE_PENDING_MUTATION_ROW
        | TABLE_NATIVE_IDEMPOTENCY_ROW
        | TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW => CoreMetaTableSpec {
            cf: CF_TRANSACTIONS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_OBJECT_HEAD_ROW
        | TABLE_MANIFEST_CAS_CURRENT_ROW
        | TABLE_MULTIPART_UPLOAD_CURRENT_ROW
        | TABLE_MULTIPART_PART_CURRENT_ROW
        | TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW => CoreMetaTableSpec {
            cf: CF_OBJECT_HEADS,
            max_payload_bytes: CORE_META_OBJECT_VERSION_MAX_PAYLOAD_BYTES,
        },
        TABLE_OBJECT_VERSION_META_ROW => CoreMetaTableSpec {
            cf: CF_OBJECT_VERSIONS,
            max_payload_bytes: CORE_META_OBJECT_VERSION_MAX_PAYLOAD_BYTES,
        },
        TABLE_INLINE_PAYLOAD_ROW => CoreMetaTableSpec {
            cf: CF_INLINE_PAYLOADS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_STREAM_HEAD_ROW => CoreMetaTableSpec {
            cf: CF_STREAM_HEADS,
            max_payload_bytes: CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        },
        TABLE_STREAM_RECORD_INDEX_ROW => CoreMetaTableSpec {
            cf: CF_STREAM_RECORDS,
            max_payload_bytes: CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        },
        TABLE_INDEX_DEFINITION_ROW => CoreMetaTableSpec {
            cf: CF_INDEX_DEFS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_INDEX_ROW | TABLE_DERIVED_INDEX_PROOF_ROW => CoreMetaTableSpec {
            cf: CF_INDEX_ROWS,
            max_payload_bytes: CORE_META_STREAM_RECORD_INDEX_MAX_PAYLOAD_BYTES,
        },
        TABLE_BOUNDARY_SCHEMA_ROW | TABLE_BOUNDARY_VALUE_ROW | TABLE_BOUNDARY_MIGRATION_ROW => {
            CoreMetaTableSpec {
                cf: CF_BOUNDARY,
                max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
            }
        }
        TABLE_AUTHZ_SCHEMA_ROW | TABLE_AUTHZ_TUPLE_PAGE_ROW => CoreMetaTableSpec {
            cf: CF_AUTHZ,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_PERSONALDB_GROUP_ROW | TABLE_PERSONALDB_DATA_LOCATOR_ROW => CoreMetaTableSpec {
            cf: CF_PERSONALDB,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_REGISTRY_VERSION_ROW
        | TABLE_REGISTRY_BLOB_LOCATOR_ROW
        | TABLE_GATEWAY_METADATA_ROW
        | TABLE_GIT_SOURCE_MANIFEST_ROW => CoreMetaTableSpec {
            cf: CF_REGISTRY,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_MESH_NODE_ROW
        | TABLE_MESH_PARTITION_ROW
        | TABLE_REPAIR_FINDING_ROW
        | TABLE_BUCKET_CURRENT_BY_NAME_ROW
        | TABLE_BUCKET_CURRENT_BY_ID_ROW
        | TABLE_CONTROL_CURRENT_ROW
        | TABLE_SYSTEM_BOOTSTRAP_MARKER_ROW
        | TABLE_ROOT_CATALOG_CURRENT_ROW
        | TABLE_QUORUM_PROFILE_CURRENT_ROW
        | TABLE_NODE_SIGNING_KEYPAIR_ROW => CoreMetaTableSpec {
            cf: CF_MESH,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_OWNERSHIP_FENCE_ROW
        | TABLE_PARTITION_OWNER_ROW
        | TABLE_TASK_LEASE_ROW
        | TABLE_TASK_CURRENT_ROW
        | TABLE_CORE_FENCE_ROW => CoreMetaTableSpec {
            cf: CF_LEASES_FENCES,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_MATERIALISATION_CURSOR_ROW
        | TABLE_WRITER_SEGMENT_ROW
        | TABLE_WATCH_CHECKPOINT_ROW
        | TABLE_LANDED_BYTE_REF_ROW => CoreMetaTableSpec {
            cf: CF_MATERIALISATION,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_REFCOUNT_ROW => CoreMetaTableSpec {
            cf: CF_REFCOUNTS,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        TABLE_OBSERVABILITY_CURSOR_ROW | TABLE_DIAGNOSTIC_ROW => CoreMetaTableSpec {
            cf: CF_OBSERVABILITY,
            max_payload_bytes: CORE_META_MAX_VALUE_BYTES,
        },
        _ => bail!("unknown CoreStore metadata table id {table_id:#06x}"),
    };
    Ok(spec)
}

fn validate_meta_payload(cf: &str, table_id: u16, payload_len: usize) -> Result<()> {
    let spec = table_spec(table_id)?;
    if spec.cf != cf {
        bail!(
            "CoreStore metadata table {table_id:#06x} belongs to {}, not {cf}",
            spec.cf
        );
    }
    if payload_len > spec.max_payload_bytes {
        bail!(
            "CoreStore metadata table {table_id:#06x} payload is {payload_len} bytes, exceeding {} bytes",
            spec.max_payload_bytes
        );
    }
    Ok(())
}

pub fn validate_coremeta_operation_key(cf: &str, table_id: u16, tuple_key: &[u8]) -> Result<()> {
    validate_meta_payload(cf, table_id, 0)?;
    let _ = core_meta_key(table_id, 0, tuple_key)?;
    Ok(())
}

pub fn canonical_coremeta_cf_name(cf: &str) -> Result<&'static str> {
    match cf {
        CF_META_VERSION => Ok(CF_META_VERSION),
        CF_ROOT_CACHE => Ok(CF_ROOT_CACHE),
        CF_TRANSACTIONS => Ok(CF_TRANSACTIONS),
        CF_OBJECT_HEADS => Ok(CF_OBJECT_HEADS),
        CF_OBJECT_VERSIONS => Ok(CF_OBJECT_VERSIONS),
        CF_INLINE_PAYLOADS => Ok(CF_INLINE_PAYLOADS),
        CF_STREAM_HEADS => Ok(CF_STREAM_HEADS),
        CF_STREAM_RECORDS => Ok(CF_STREAM_RECORDS),
        CF_INDEX_DEFS => Ok(CF_INDEX_DEFS),
        CF_INDEX_ROWS => Ok(CF_INDEX_ROWS),
        CF_BOUNDARY => Ok(CF_BOUNDARY),
        CF_AUTHZ => Ok(CF_AUTHZ),
        CF_PERSONALDB => Ok(CF_PERSONALDB),
        CF_REGISTRY => Ok(CF_REGISTRY),
        CF_MESH => Ok(CF_MESH),
        CF_LEASES_FENCES => Ok(CF_LEASES_FENCES),
        CF_MATERIALISATION => Ok(CF_MATERIALISATION),
        CF_REFCOUNTS => Ok(CF_REFCOUNTS),
        CF_OBSERVABILITY => Ok(CF_OBSERVABILITY),
        _ => bail!("unknown CoreStore metadata column family {cf}"),
    }
}

pub fn validate_coremeta_operation_payload(
    cf: &str,
    table_id: u16,
    tuple_key: &[u8],
    payload: &[u8],
) -> Result<()> {
    validate_meta_payload(cf, table_id, payload.len())?;
    let _ = core_meta_key(table_id, 0, tuple_key)?;
    validate_table_payload_schema(table_id, payload)?;
    Ok(())
}

fn encode_envelope(cf: &str, table_id: u16, payload: &[u8]) -> Result<Vec<u8>> {
    validate_meta_payload(cf, table_id, payload.len())?;
    let common = common_for_envelope_payload(payload)?;
    encode_envelope_with_common(cf, table_id, payload, common)
}

fn encode_envelope_with_common(
    cf: &str,
    table_id: u16,
    payload: &[u8],
    common: CoreMetaRowCommonProto,
) -> Result<Vec<u8>> {
    validate_meta_payload(cf, table_id, payload.len())?;
    validate_table_payload_schema(table_id, payload)?;
    validate_envelope_common_matches_payload(payload, &common)?;
    let payload_hash = core_meta_payload_hash(table_id, CORE_META_VALUE_SCHEMA_VERSION, payload);
    let envelope = CoreMetaValueEnvelope {
        table_id: u32::from(table_id),
        schema_version: CORE_META_VALUE_SCHEMA_VERSION,
        payload: payload.to_vec(),
        payload_hash,
    };
    let mut bytes = Vec::new();
    envelope.encode(&mut bytes)?;
    if bytes.len() > CORE_META_MAX_VALUE_BYTES {
        bail!(
            "CoreStore metadata table {table_id:#06x} value envelope is {} bytes, exceeding {} bytes",
            bytes.len(),
            CORE_META_MAX_VALUE_BYTES
        );
    }
    Ok(bytes)
}

fn decode_envelope(cf: &str, expected_table_id: u16, bytes: &[u8]) -> Result<Vec<u8>> {
    decode_envelope_with_common(cf, expected_table_id, bytes).map(|(payload, _)| payload)
}

fn decode_envelope_with_common(
    cf: &str,
    expected_table_id: u16,
    bytes: &[u8],
) -> Result<(Vec<u8>, CoreMetaRowCommonProto)> {
    if bytes.len() > CORE_META_MAX_VALUE_BYTES {
        bail!(
            "CoreMetaValueEnvelope is {} bytes, exceeding {} bytes",
            bytes.len(),
            CORE_META_MAX_VALUE_BYTES
        );
    }
    let envelope = CoreMetaValueEnvelope::decode(bytes)?;
    if envelope.table_id != u32::from(expected_table_id) {
        bail!(
            "CoreMetaValueEnvelope table id mismatch: expected {expected_table_id:#06x}, got {:#06x}",
            envelope.table_id
        );
    }
    if envelope.schema_version != CORE_META_VALUE_SCHEMA_VERSION {
        bail!(
            "CoreMetaValueEnvelope unsupported schema version {}",
            envelope.schema_version
        );
    }
    let actual = core_meta_payload_hash(
        expected_table_id,
        envelope.schema_version,
        &envelope.payload,
    );
    if actual != envelope.payload_hash {
        bail!("CoreMetaValueEnvelope payload hash mismatch");
    }
    let common = common_for_envelope_payload(&envelope.payload)?;
    validate_coremeta_common_shape(&common)?;
    validate_meta_payload(cf, expected_table_id, envelope.payload.len())?;
    validate_table_payload_schema(expected_table_id, &envelope.payload)?;
    Ok((envelope.payload, common))
}

fn common_for_envelope_payload(payload: &[u8]) -> Result<CoreMetaRowCommonProto> {
    extract_row_common_from_payload(payload)
}

fn validate_envelope_common_matches_payload(
    payload: &[u8],
    common: &CoreMetaRowCommonProto,
) -> Result<()> {
    let payload_common = extract_row_common_from_payload(payload)?;
    if payload_common != *common {
        bail!("CoreMetaValueEnvelope common metadata does not match payload common metadata");
    }
    Ok(())
}

fn validate_table_payload_schema(table_id: u16, payload: &[u8]) -> Result<()> {
    match table_id {
        TABLE_META_SCHEMA_VERSION_ROW => {
            let row = CoreMetaSchemaVersionRowProto::decode(payload)?;
            let mut canonical = Vec::new();
            row.encode(&mut canonical)?;
            if canonical != payload {
                bail!("CoreMeta schema version row is not deterministic protobuf");
            }
        }
        TABLE_INLINE_PAYLOAD_ROW => {
            let _ = decode_inline_payload_row(payload)?;
        }
        TABLE_TRANSACTION_LOCATOR_ROW => {
            validate_coremeta_pending_batch_marker_row(payload)?;
        }
        TABLE_TRANSACTION_COMMIT_EVIDENCE_ROW => {
            validate_coremeta_commit_evidence_row(payload)?;
        }
        TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW => {
            super::pending_mutation::decode_admission_commit_certificate(payload)?;
        }
        _ => {}
    }
    validate_table_schema_marker(table_id, payload)?;
    Ok(())
}

fn validate_table_schema_marker(table_id: u16, payload: &[u8]) -> Result<()> {
    let Some(allowed) = expected_schema_markers(table_id) else {
        return Ok(());
    };
    let Some(actual) = protobuf_field_two_string(payload)? else {
        bail!("CoreStore metadata table {table_id:#06x} payload is missing schema marker field");
    };
    if !allowed.contains(&actual.as_str()) {
        bail!(
            "CoreStore metadata table {table_id:#06x} payload schema {actual} is not one of {allowed:?}"
        );
    }
    Ok(())
}

fn expected_schema_markers(table_id: u16) -> Option<&'static [&'static str]> {
    match table_id {
        TABLE_INLINE_MANIFEST_BODY_ROW => Some(&["anvil.core.inline_manifest_body.v1"]),
        TABLE_EXPLICIT_TRANSACTION_ROW => Some(&[
            "anvil.core.transaction_header_row.v1",
            "anvil.core.transaction_staged_update_row.v1",
            "anvil.core.transaction_precondition_row.v1",
        ]),
        TABLE_PENDING_MUTATION_ROW => Some(&[
            "anvil.core.pending_mutation_row.v1",
            "anvil.core.pending_mutation_finalisation_index.v1",
        ]),
        TABLE_ADMISSION_COMMIT_CERTIFICATE_ROW => Some(&["anvil.admission.commit_certificate.v1"]),
        TABLE_OBJECT_HEAD_ROW => Some(&["anvil.core.object_metadata.v1"]),
        TABLE_OBJECT_VERSION_META_ROW => Some(&[
            "anvil.core.object_manifest.v1",
            "anvil.core.object_metadata.v1",
            "anvil.core.object_metadata_counter.v1",
        ]),
        TABLE_STREAM_HEAD_ROW => Some(&["anvil.core.stream_head.v1"]),
        TABLE_STREAM_RECORD_INDEX_ROW => Some(&[
            "anvil.core.watch_event.v1",
            "anvil.core.stream_record_index.v1",
        ]),
        TABLE_MANIFEST_CAS_CURRENT_ROW => Some(&["anvil.core.manifest_cas.current_row.v1"]),
        TABLE_MULTIPART_UPLOAD_CURRENT_ROW => Some(&["anvil.multipart.upload_current_row.v1"]),
        TABLE_MULTIPART_PART_CURRENT_ROW => Some(&["anvil.multipart.part_current_row.v1"]),
        TABLE_OBJECT_METADATA_PARTITION_MANIFEST_ROW => {
            Some(&["anvil.coremeta.object_metadata_partition_manifest.v1"])
        }
        TABLE_INDEX_DEFINITION_ROW => Some(&[
            "anvil.coremeta.index_definition_current.v1",
            "anvil.coremeta.index_definition_state.v1",
        ]),
        TABLE_INDEX_ROW => Some(&["anvil.coremeta.index_segment_row.v1"]),
        TABLE_DERIVED_INDEX_PROOF_ROW => Some(&["anvil.coremeta.derived_index_proof.v1"]),
        TABLE_BOUNDARY_SCHEMA_ROW => Some(&["anvil.core.boundary_schema.v1"]),
        TABLE_BOUNDARY_VALUE_ROW => Some(&["anvil.core.boundary_value.v1"]),
        TABLE_BOUNDARY_MIGRATION_ROW => Some(&["anvil.boundary_migration.v1"]),
        TABLE_AUTHZ_SCHEMA_ROW | TABLE_AUTHZ_TUPLE_PAGE_ROW => Some(&[
            "anvil.authz.coremeta_payload_row.v1",
            "anvil.authz.derived_userset_index_row.v1",
        ]),
        TABLE_GATEWAY_METADATA_ROW => Some(&["anvil.gateway.coremeta_record.v1"]),
        TABLE_GIT_SOURCE_MANIFEST_ROW => Some(&["anvil.coremeta.git_source_manifest.v1"]),
        TABLE_MESH_NODE_ROW => Some(&[
            "anvil.coremeta.mesh_lifecycle_projection.v1",
            "anvil.coremeta.mesh_directory_projection.v1",
        ]),
        TABLE_MESH_PARTITION_ROW => Some(&[
            "anvil.coremeta.mesh_lifecycle_projection.v1",
            "anvil.coremeta.mesh_directory_projection.v1",
        ]),
        TABLE_BUCKET_CURRENT_BY_NAME_ROW | TABLE_BUCKET_CURRENT_BY_ID_ROW => {
            Some(&["anvil.core.bucket_current.v1"])
        }
        TABLE_CONTROL_CURRENT_ROW => Some(&["anvil.control.current.v1"]),
        TABLE_ROOT_CATALOG_CURRENT_ROW => {
            Some(&["anvil.control.current.v1", "anvil.core.root_catalog.v1"])
        }
        TABLE_QUORUM_PROFILE_CURRENT_ROW => {
            Some(&["anvil.control.current.v1", "anvil.core.quorum_profile.v1"])
        }
        TABLE_TASK_CURRENT_ROW => Some(&["anvil.core.task_current.v1"]),
        TABLE_CORE_FENCE_ROW => Some(&["anvil.control.current.v1", "anvil.core.fence.v1"]),
        TABLE_MATERIALISATION_CURSOR_ROW => Some(&[
            "anvil.core.materialisation_cursor.v1",
            "anvil.core.pending_mutation_finalisation.v1",
            "anvil.core.pending_mutation_finalisation_index.v1",
            "anvil.coremeta.watch_checkpoint_lag.v1",
        ]),
        TABLE_LANDED_BYTE_REF_ROW => Some(&["anvil.core.landed_byte_ref.v1"]),
        TABLE_WRITER_SEGMENT_ROW => Some(&["anvil.coremeta.writer_segment_locator.v1"]),
        TABLE_WATCH_CHECKPOINT_ROW => Some(&[
            "anvil.coremeta.watch_checkpoint.v1",
            "anvil.coremeta.watch_checkpoint_lag.v1",
        ]),
        _ => None,
    }
}

fn protobuf_field_two_string(payload: &[u8]) -> Result<Option<String>> {
    let mut offset = 0usize;
    while offset < payload.len() {
        let (key, next) = read_proto_varint(payload, offset)?;
        offset = next;
        let field_number = key >> 3;
        let wire_type = key & 0x07;
        match wire_type {
            0 => {
                let (_, next) = read_proto_varint(payload, offset)?;
                offset = next;
            }
            1 => {
                offset = offset
                    .checked_add(8)
                    .ok_or_else(|| anyhow!("protobuf fixed64 field length overflow"))?;
            }
            2 => {
                let (len, next) = read_proto_varint(payload, offset)?;
                offset = next;
                let len = usize::try_from(len).context("protobuf length exceeds usize")?;
                let end = offset
                    .checked_add(len)
                    .ok_or_else(|| anyhow!("protobuf length-delimited field overflow"))?;
                if end > payload.len() {
                    bail!("protobuf length-delimited field is truncated");
                }
                if field_number == 2 {
                    return Ok(Some(
                        std::str::from_utf8(&payload[offset..end])
                            .context("protobuf field 2 schema marker is not UTF-8")?
                            .to_string(),
                    ));
                }
                offset = end;
            }
            5 => {
                offset = offset
                    .checked_add(4)
                    .ok_or_else(|| anyhow!("protobuf fixed32 field length overflow"))?;
            }
            other => bail!("protobuf schema marker scan hit unsupported wire type {other}"),
        }
        if offset > payload.len() {
            bail!("protobuf field exceeds payload length");
        }
    }
    Ok(None)
}

fn read_proto_varint(payload: &[u8], mut offset: usize) -> Result<(u64, usize)> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *payload
            .get(offset)
            .ok_or_else(|| anyhow!("protobuf varint is truncated"))?;
        offset += 1;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, offset));
        }
    }
    bail!("protobuf varint is too long")
}

fn validate_coremeta_commit_evidence_row(payload: &[u8]) -> Result<()> {
    let row = CoreMetaCommitEvidenceRowProto::decode(payload)?;
    let mut canonical = Vec::new();
    row.encode(&mut canonical)?;
    if canonical != payload {
        bail!("CoreMeta commit evidence row is not deterministic protobuf");
    }
    validate_coremeta_hash(
        &row.certificate_hash,
        "CoreMeta commit evidence certificate hash",
    )?;
    validate_coremeta_hash(
        &row.committed_batch_hash,
        "CoreMeta commit evidence committed batch hash",
    )?;
    if row.certificate_bytes.is_empty() {
        bail!("CoreMeta commit evidence row is missing certificate bytes");
    }
    for receipt_hash in &row.certificate_persist_receipt_hashes {
        validate_coremeta_hash(receipt_hash, "CoreMeta certificate persist receipt hash")?;
    }
    if row.certificate_persist_receipt_bytes.len() < row.certificate_persist_receipt_hashes.len() {
        bail!("CoreMeta commit evidence row is missing certificate persistence receipt bytes");
    }
    let common = row
        .common
        .ok_or_else(|| anyhow!("CoreMeta commit evidence row is missing common metadata"))?;
    validate_coremeta_common(&common, CoreMetaVisibilityState::Committed)?;
    Ok(())
}

fn validate_coremeta_pending_batch_marker_row(payload: &[u8]) -> Result<()> {
    let row = CoreMetaPendingBatchMarkerRowProto::decode(payload)?;
    let mut canonical = Vec::new();
    row.encode(&mut canonical)?;
    if canonical != payload {
        bail!("CoreMeta pending batch marker row is not deterministic protobuf");
    }
    validate_coremeta_hash(&row.pending_batch_hash, "CoreMeta pending batch hash")?;
    validate_coremeta_hash(&row.root_key_hash, "CoreMeta pending batch root key hash")?;
    validate_coremeta_logical_id(&row.transaction_id, "CoreMeta pending batch transaction id")?;
    if row.core_meta_row_count == 0 {
        bail!("CoreMeta pending batch marker must reference at least one CoreMeta row");
    }
    if row.post_root_generation <= row.expected_root_generation {
        bail!("CoreMeta pending batch marker root generation must advance");
    }
    let common = row
        .common
        .ok_or_else(|| anyhow!("CoreMeta pending batch marker row is missing common metadata"))?;
    validate_coremeta_common(&common, CoreMetaVisibilityState::Pending)?;
    if common.root_key_hash != row.root_key_hash
        || common.root_generation != row.post_root_generation
        || common.transaction_id != row.transaction_id
    {
        bail!("CoreMeta pending batch marker common metadata does not match row scope");
    }
    Ok(())
}

fn validate_coremeta_common(
    common: &CoreMetaRowCommonProto,
    expected_visibility: CoreMetaVisibilityState,
) -> Result<()> {
    validate_coremeta_common_shape(common)?;
    if common.visibility_state_enum() != expected_visibility {
        bail!("CoreMeta row common visibility state mismatch");
    }
    Ok(())
}

fn validate_coremeta_common_shape(common: &CoreMetaRowCommonProto) -> Result<()> {
    if common.payload_schema_version != CORE_META_VALUE_SCHEMA_VERSION {
        bail!(
            "CoreMeta row common payload schema version {} is not supported",
            common.payload_schema_version
        );
    }
    match common.visibility_state_enum() {
        CoreMetaVisibilityState::Pending
        | CoreMetaVisibilityState::Committed
        | CoreMetaVisibilityState::Aborted
        | CoreMetaVisibilityState::RolledBack => {}
        CoreMetaVisibilityState::Unspecified => {
            bail!("CoreMeta row common visibility state must be specified");
        }
    }
    if !common.root_key_hash.is_empty() {
        validate_coremeta_hash(&common.root_key_hash, "CoreMeta row common root key hash")?;
        if common.root_generation == 0 {
            bail!("CoreMeta row common rooted rows must use a non-zero root generation");
        }
    }
    if !common.transaction_id.is_empty() {
        validate_coremeta_logical_id(&common.transaction_id, "CoreMeta row common transaction id")?;
    }
    if common.realm_id.as_bytes().contains(&0) || !common.realm_id.chars().eq(common.realm_id.nfc())
    {
        bail!("CoreMeta row common realm must be NFC-normalized text without NUL");
    }
    Ok(())
}

fn validate_coremeta_hash(hash: &str, label: &str) -> Result<()> {
    let Some((algorithm, value)) = hash.split_once(':') else {
        bail!("{label} must use algorithm:hex encoding");
    };
    if algorithm.is_empty()
        || value.is_empty()
        || !hash.is_ascii()
        || value.bytes().any(|byte| !byte.is_ascii_hexdigit())
    {
        bail!("{label} must use ASCII algorithm:hex encoding");
    }
    Ok(())
}

fn validate_coremeta_logical_id(value: &str, label: &str) -> Result<()> {
    if value.is_empty() {
        bail!("{label} must not be empty");
    }
    if value.len() > 512 {
        bail!("{label} is too long");
    }
    if value.as_bytes().contains(&0) || !value.chars().eq(value.nfc()) {
        bail!("{label} must be NFC-normalized text without NUL");
    }
    Ok(())
}

pub fn core_meta_payload_digest(table_id: u16, payload: &[u8]) -> String {
    core_meta_payload_hash(table_id, CORE_META_VALUE_SCHEMA_VERSION, payload)
}

fn core_meta_payload_hash(table_id: u16, schema_version: u32, payload: &[u8]) -> String {
    let mut hasher = Hasher::new();
    hasher.update(b"anvil.coremeta.value.v1");
    hasher.update(&[0]);
    hasher.update(&table_id.to_le_bytes());
    hasher.update(&schema_version.to_le_bytes());
    hasher.update(payload);
    format!("blake3:{}", hasher.finalize().to_hex())
}

#[cfg(test)]
#[path = "meta_tests.rs"]
mod tests;
