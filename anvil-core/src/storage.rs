use anyhow::Result;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

const STORAGE_DIR: &str = "anvil-data";
const TEMP_DIR: &str = "tmp";
pub const DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalChunkManifest {
    pub kind: String,
    pub chunk_size: usize,
    pub chunks: Vec<ExternalChunkRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalChunkRecord {
    pub chunk_index: u64,
    pub plaintext_length: u64,
    pub ciphertext_length: u64,
    pub payload_chunk_hash: String,
    pub storage_chunk_hash: String,
    pub compression: String,
    pub base_nonce: String,
    pub mac: String,
    pub storage_ref: String,
}

#[derive(Debug, Clone)]
pub struct Storage {
    storage_path: PathBuf,
    temp_path: PathBuf,
}

impl Storage {
    pub async fn commit_whole_object_from_bytes(
        &self,
        data: &[u8],
        final_object_hash: &str,
    ) -> Result<()> {
        let final_path = self.get_whole_object_path(final_object_hash);
        let mut file = fs::File::create(&final_path).await?;
        file.write_all(data).await?;
        Ok(())
    }
    pub async fn new() -> Result<Self> {
        Self::new_at(Path::new(STORAGE_DIR)).await
    }

    pub async fn new_at(storage_path: impl AsRef<Path>) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let temp_path = storage_path.join(TEMP_DIR);
        fs::create_dir_all(&storage_path).await?;
        fs::create_dir_all(&temp_path).await?;
        Ok(Self {
            storage_path,
            temp_path,
        })
    }

    pub fn new_at_sync(storage_path: impl AsRef<Path>) -> Result<Self> {
        let storage_path = storage_path.as_ref().to_path_buf();
        let temp_path = storage_path.join(TEMP_DIR);
        std::fs::create_dir_all(&storage_path)?;
        std::fs::create_dir_all(&temp_path)?;
        Ok(Self {
            storage_path,
            temp_path,
        })
    }

    pub fn temp_dir_path(&self) -> &Path {
        &self.temp_path
    }

    pub fn control_journal_path(&self) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("control.anjournal")
    }

    pub fn metadata_journal_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("journals")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}.anjournal"))
    }

    pub fn bucket_metadata_journal_path(&self, tenant_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("journals")
            .join(format!("tenant-{tenant_id}"))
            .join("buckets.anjournal")
    }

    pub fn global_bucket_metadata_journal_path(&self) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("journals")
            .join("buckets.anjournal")
    }

    pub fn index_definition_journal_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("index")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join("definitions.anjournal")
    }

    pub fn hf_journal_path(&self) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("hf")
            .join("huggingface.anjournal")
    }

    pub fn append_journal_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("append")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join("streams.anjournal")
    }

    pub async fn append_journal_paths(&self) -> anyhow::Result<Vec<PathBuf>> {
        collect_bucket_partition_journals(
            self.storage_path.join("_anvil").join("append"),
            "streams.anjournal",
        )
        .await
    }

    pub fn manifest_cas_journal_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("manifest")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join("cas.anjournal")
    }

    pub fn multipart_journal_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("multipart")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join("uploads.anjournal")
    }

    pub async fn multipart_journal_paths(&self) -> anyhow::Result<Vec<PathBuf>> {
        collect_bucket_partition_journals(
            self.storage_path.join("_anvil").join("multipart"),
            "uploads.anjournal",
        )
        .await
    }

    pub fn index_diagnostic_journal_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("index")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join("diagnostics.anjournal")
    }

    pub fn task_lease_path(&self, tenant_id: i64, task_id: &str) -> Result<PathBuf> {
        if tenant_id < 0 {
            anyhow::bail!("task lease tenant id must be nonnegative");
        }
        ensure_safe_internal_component(task_id, "task id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("tasks")
            .join("leases")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("{task_id}.json")))
    }

    pub fn task_queue_journal_path(&self) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("tasks")
            .join("tasks.anjournal")
    }

    pub fn partition_owner_path(
        &self,
        partition_family: &str,
        partition_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(partition_family, "partition family")?;
        ensure_hash_hex(partition_id, "partition id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("control")
            .join("partition-owners")
            .join(partition_family)
            .join(format!("{partition_id}.json")))
    }

    pub fn model_metadata_journal_path(&self) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("models")
            .join("models.anjournal")
    }

    pub fn authz_tuple_journal_path(&self, tenant_id: i64) -> PathBuf {
        self.authz_tuple_segment_dir(tenant_id)
            .join("tuples.anjournal")
    }

    pub fn authz_tuple_segment_dir(&self, tenant_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("authz")
            .join(format!("tenant-{tenant_id}"))
    }

    pub fn authz_tuple_segment_path(&self, tenant_id: i64, generation: u64) -> PathBuf {
        self.authz_tuple_segment_dir(tenant_id)
            .join(format!("generation-{generation:020}.anauthz"))
    }

    pub fn authz_derived_userset_index_path(
        &self,
        tenant_id: i64,
        derived_index_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(derived_index_id, "authorization derived userset index id")?;
        Ok(self
            .authz_tuple_segment_dir(tenant_id)
            .join("derived-usersets")
            .join(format!("{derived_index_id}.json")))
    }

    pub fn metadata_segment_path(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        generation: u64,
    ) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("segments")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join(format!("generation-{generation:020}.anseg"))
    }

    pub fn directory_segment_path(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        generation: u64,
    ) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("segments")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join(format!("generation-{generation:020}.andir"))
    }

    pub fn metadata_manifest_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("meta")
            .join("manifests")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}.json"))
    }

    pub fn native_idempotency_record_path(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        record_key_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(record_key_hash, "native idempotency record key hash")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("meta")
            .join("idempotency")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join(format!("{record_key_hash}.json")))
    }

    pub fn object_watch_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("watch")
            .join("object")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}.anwatch"))
    }

    pub fn watch_checkpoint_path(
        &self,
        watch_stream_id: &str,
        consumer_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(watch_stream_id, "watch stream id")?;
        ensure_safe_internal_component(consumer_id, "watch consumer id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("checkpoints")
            .join(watch_stream_id)
            .join(format!("{consumer_id}.json")))
    }

    pub fn diagnostic_source_dir(
        &self,
        scope_kind: &str,
        scope_id: &str,
        source: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(scope_kind, "diagnostic scope kind")?;
        ensure_safe_internal_component(scope_id, "diagnostic scope id")?;
        ensure_safe_internal_component(source, "diagnostic source")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("diagnostics")
            .join(scope_kind)
            .join(scope_id)
            .join(source))
    }

    pub fn diagnostic_object_path(
        &self,
        scope_kind: &str,
        scope_id: &str,
        source: &str,
        diagnostic_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(diagnostic_id, "diagnostic id")?;
        Ok(self
            .diagnostic_source_dir(scope_kind, scope_id, source)?
            .join(format!("{diagnostic_id}.json")))
    }

    pub fn derived_index_proof_dir(&self, index_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(index_id, "derived index id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("index")
            .join("proofs")
            .join(index_id))
    }

    pub fn derived_index_proof_head_path(&self, index_id: &str) -> Result<PathBuf> {
        Ok(self
            .derived_index_proof_dir(index_id)?
            .join("heads")
            .join("latest.json"))
    }

    pub fn derived_index_proof_path(
        &self,
        index_id: &str,
        generation: u64,
        proof_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(proof_hash, "derived index proof hash")?;
        Ok(self
            .derived_index_proof_dir(index_id)?
            .join(format!("generation-{generation:020}-{proof_hash}.json")))
    }

    pub fn repair_finding_dir(&self, scope_kind: &str, scope_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(scope_kind, "repair finding scope kind")?;
        ensure_safe_internal_component(scope_id, "repair finding scope id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("repair")
            .join("findings")
            .join(scope_kind)
            .join(scope_id))
    }

    pub fn repair_finding_path(
        &self,
        scope_kind: &str,
        scope_id: &str,
        finding_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(finding_id, "repair finding id")?;
        Ok(self
            .repair_finding_dir(scope_kind, scope_id)?
            .join(format!("{finding_id}.json")))
    }

    pub fn index_partition_watch_path(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        index_id: &str,
        partition_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(index_id, "index id")?;
        ensure_hash_hex(partition_id, "index partition id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("index")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}"))
            .join("indexes")
            .join(index_id)
            .join("partitions")
            .join(format!("{partition_id}.anwatch")))
    }

    pub fn authz_derived_lag_watch_path(
        &self,
        tenant_id: i64,
        derived_index_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(derived_index_id, "authorization derived index id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("authz-derived-lag")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("{derived_index_id}.anwatch")))
    }

    pub fn authz_namespace_watch_path(&self, tenant_id: i64, namespace: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(namespace, "authorization namespace")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("authz-namespace")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("{namespace}.anwatch")))
    }

    pub fn authz_namespace_schema_path(&self, tenant_id: i64, namespace: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(namespace, "authorization namespace")?;
        Ok(self
            .authz_namespace_schema_dir(tenant_id)
            .join(format!("{namespace}.json")))
    }

    pub fn authz_namespace_schema_dir(&self, tenant_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("authz")
            .join("schemas")
            .join(format!("tenant-{tenant_id}"))
    }

    pub fn authz_schema_revision_path(
        &self,
        tenant_id: i64,
        schema_id: &str,
        revision: u64,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(schema_id, "authorization schema id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("authz")
            .join("v2")
            .join(format!("tenant-{tenant_id}"))
            .join("schemas")
            .join(schema_id)
            .join("revisions")
            .join(format!("{revision:020}.json")))
    }

    pub fn authz_schema_latest_path(&self, tenant_id: i64, schema_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(schema_id, "authorization schema id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("authz")
            .join("v2")
            .join(format!("tenant-{tenant_id}"))
            .join("schemas")
            .join(schema_id)
            .join("latest.json"))
    }

    pub fn authz_schema_binding_path(&self, tenant_id: i64, realm_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(realm_id, "authorization realm id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("authz")
            .join("v2")
            .join(format!("tenant-{tenant_id}"))
            .join("realms")
            .join(realm_id)
            .join("schema-binding.json"))
    }

    pub fn git_source_watch_path(&self, tenant_id: i64, repository_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(repository_id, "git repository id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("git")
            .join(format!("tenant-{tenant_id}"))
            .join("repositories")
            .join(format!("{repository_id}.anwatch")))
    }

    pub fn personaldb_group_watch_path(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(database_id, "personaldb database id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("personaldb")
            .join(format!("tenant-{tenant_id}"))
            .join("groups")
            .join(format!("{database_id}.anwatch")))
    }

    pub fn personaldb_projection_watch_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        projection_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(database_id, "personaldb database id")?;
        ensure_safe_internal_component(projection_id, "personaldb projection id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("watch")
            .join("personaldb")
            .join(format!("tenant-{tenant_id}"))
            .join("groups")
            .join(database_id)
            .join("projections")
            .join(format!("{projection_id}.anwatch")))
    }

    pub fn personaldb_group_dir(&self, tenant_id: i64, database_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(database_id, "personaldb database id")?;
        Ok(self
            .personaldb_tenant_groups_dir(tenant_id)?
            .join(database_id))
    }

    pub fn personaldb_tenant_groups_dir(&self, tenant_id: i64) -> Result<PathBuf> {
        Ok(self
            .storage_path
            .join("_anvil")
            .join("personaldb")
            .join("tenants")
            .join(format!("tenant-{tenant_id}"))
            .join("groups"))
    }

    pub fn personaldb_group_manifest_path(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<PathBuf> {
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("manifest.json"))
    }

    pub fn personaldb_schema_sql_path(&self, tenant_id: i64, database_id: &str) -> Result<PathBuf> {
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("schema.sql"))
    }

    pub fn personaldb_committed_head_path(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<PathBuf> {
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("heads")
            .join("committed.json"))
    }

    pub fn personaldb_snapshots_head_path(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<PathBuf> {
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("heads")
            .join("snapshots.json"))
    }

    pub fn personaldb_snapshot_manifest_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        log_index: u64,
        state_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(state_hash, "personaldb snapshot state hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("snapshots")
            .join("manifests")
            .join(format!("{log_index:020}-{state_hash}.json")))
    }

    pub fn personaldb_snapshot_object_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        log_index: u64,
        state_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(state_hash, "personaldb snapshot state hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("snapshots")
            .join("objects")
            .join(format!("{log_index:020}-{state_hash}.sqlite.zst")))
    }

    pub fn personaldb_projection_manifest_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        projection_id: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(projection_id, "personaldb projection id")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("projections")
            .join(projection_id)
            .join("manifest.json"))
    }

    pub fn personaldb_commit_certificate_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        log_index: u64,
        entry_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(entry_hash, "personaldb commit entry hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("log")
            .join("certificates")
            .join(format!("{log_index:020}-{entry_hash}.certificate.json")))
    }

    pub fn personaldb_changeset_payload_by_index_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        log_index: u64,
        payload_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(payload_hash, "personaldb changeset payload hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("log")
            .join("payloads")
            .join("by-index")
            .join(format!("{log_index:020}-{payload_hash}.sqlite-changeset")))
    }

    pub fn personaldb_changeset_payload_by_hash_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        payload_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(payload_hash, "personaldb changeset payload hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("log")
            .join("payloads")
            .join("by-hash")
            .join(format!("{payload_hash}.sqlite-changeset")))
    }

    pub fn personaldb_log_segment_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        start_log_index: u64,
        end_log_index: u64,
        segment_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(segment_hash, "personaldb log segment hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("log")
            .join("segments")
            .join(format!(
                "{start_log_index:020}-{end_log_index:020}-{segment_hash}.pdbseg"
            )))
    }

    pub fn personaldb_log_segment_dir(&self, tenant_id: i64, database_id: &str) -> Result<PathBuf> {
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("log")
            .join("segments"))
    }

    pub fn personaldb_row_index_path(
        &self,
        tenant_id: i64,
        database_id: &str,
        generation: u64,
        source_hash: &str,
    ) -> Result<PathBuf> {
        ensure_hash_hex(source_hash, "personaldb row index source hash")?;
        Ok(self
            .personaldb_group_dir(tenant_id, database_id)?
            .join("row-index")
            .join(format!("{generation:020}-{source_hash}.rowidx")))
    }

    pub fn git_source_index_path(
        &self,
        tenant_id: i64,
        repository_id: &str,
        generation: u64,
        source_hash: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(repository_id, "git repository id")?;
        ensure_hash_hex(source_hash, "git source index source hash")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("git")
            .join("tenants")
            .join(format!("tenant-{tenant_id}"))
            .join("repositories")
            .join(repository_id)
            .join("indexes")
            .join(format!("generation-{generation:020}-{source_hash}.angit")))
    }

    pub fn git_source_index_dir(&self, tenant_id: i64, repository_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(repository_id, "git repository id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("git")
            .join("tenants")
            .join(format!("tenant-{tenant_id}"))
            .join("repositories")
            .join(repository_id)
            .join("indexes"))
    }

    pub fn git_source_manifest_path(&self, tenant_id: i64, repository_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(repository_id, "git repository id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("git")
            .join("tenants")
            .join(format!("tenant-{tenant_id}"))
            .join("repositories")
            .join(repository_id)
            .join("manifest.json"))
    }

    pub fn full_text_segment_path(
        &self,
        index_id: &str,
        generation: u64,
        segment_hash: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(index_id, "full text index id")?;
        ensure_hash_hex(segment_hash, "full text segment hash")?;
        Ok(self
            .full_text_segment_dir(index_id)?
            .join(format!("generation-{generation:020}-{segment_hash}.anfts")))
    }

    pub fn full_text_segment_dir(&self, index_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(index_id, "full text index id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("index")
            .join("full-text")
            .join(index_id)
            .join("segments"))
    }

    pub fn vector_segment_path(
        &self,
        index_id: &str,
        generation: u64,
        segment_hash: &str,
    ) -> Result<PathBuf> {
        ensure_safe_internal_component(index_id, "vector index id")?;
        ensure_hash_hex(segment_hash, "vector segment hash")?;
        Ok(self
            .vector_segment_dir(index_id)?
            .join(format!("generation-{generation:020}-{segment_hash}.anvec")))
    }

    pub fn vector_segment_dir(&self, index_id: &str) -> Result<PathBuf> {
        ensure_safe_internal_component(index_id, "vector index id")?;
        Ok(self
            .storage_path
            .join("_anvil")
            .join("index")
            .join("vector")
            .join(index_id)
            .join("segments"))
    }

    pub fn relative_storage_path(&self, path: &Path) -> Result<String> {
        let relative = path.strip_prefix(&self.storage_path)?;
        Ok(relative.to_string_lossy().replace('\\', "/"))
    }

    pub fn resolve_relative_storage_path(&self, relative: &str) -> Result<PathBuf> {
        let path = Path::new(relative);
        if path.is_absolute() {
            anyhow::bail!("storage-relative path must not be absolute");
        }

        let mut clean = PathBuf::new();
        for component in path.components() {
            match component {
                std::path::Component::Normal(part) => clean.push(part),
                std::path::Component::CurDir => {}
                _ => anyhow::bail!("storage-relative path must not escape storage root"),
            }
        }
        Ok(self.storage_path.join(clean))
    }

    fn get_shard_path(&self, object_hash: &str, shard_index: u32) -> PathBuf {
        self.storage_path
            .join(format!("{}-{:02}", object_hash, shard_index))
    }

    fn get_whole_object_path(&self, object_hash: &str) -> PathBuf {
        self.storage_path.join(object_hash)
    }

    pub fn external_chunk_path(
        &self,
        object_hash: &str,
        chunk_index: u64,
        chunk_hash: &str,
    ) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("payloads")
            .join("chunks")
            .join(object_hash)
            .join(format!("{chunk_index:020}-{chunk_hash}.chunk"))
    }

    pub async fn store_whole_object(&self, object_hash: &str, data: &[u8]) -> Result<()> {
        let file_path = self.get_whole_object_path(object_hash);
        let mut file = fs::File::create(file_path).await?;
        file.write_all(data).await?;
        Ok(())
    }

    fn get_temp_shard_path(&self, upload_id: &str, shard_index: u32) -> PathBuf {
        self.temp_path
            .join(format!("{}-{:02}", upload_id, shard_index))
    }

    pub async fn store_temp_shard(
        &self,
        upload_id: &str,
        shard_index: u32,
        data: &[u8],
    ) -> Result<()> {
        let file_path = self.get_temp_shard_path(upload_id, shard_index);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .await?;
        file.write_all(data).await?;
        Ok(())
    }

    pub async fn commit_shard(
        &self,
        upload_id: &str,
        shard_index: u32,
        final_object_hash: &str,
    ) -> Result<()> {
        let temp_path = self.get_temp_shard_path(upload_id, shard_index);
        let final_path = self.get_shard_path(final_object_hash, shard_index);
        fs::rename(temp_path, final_path).await?;
        info!("File renamed successfully");
        Ok(())
    }

    pub async fn retrieve_shard(&self, object_hash: &str, shard_index: u32) -> Result<Vec<u8>> {
        let file_path = self.get_shard_path(object_hash, shard_index);
        let data = fs::read(file_path).await?;
        Ok(data)
    }

    pub async fn retrieve_whole_object(&self, object_hash: &str) -> Result<Vec<u8>> {
        let file_path = self.get_whole_object_path(object_hash);
        let data = fs::read(file_path).await?;
        Ok(data)
    }

    pub async fn retrieve_external_chunk(&self, storage_ref: &str) -> Result<Vec<u8>> {
        let path = self.resolve_relative_storage_path(storage_ref)?;
        Ok(fs::read(path).await?)
    }

    pub async fn delete_shard(&self, object_hash: &str, shard_index: u32) -> Result<()> {
        let file_path = self.get_shard_path(object_hash, shard_index);
        fs::remove_file(file_path).await?;
        Ok(())
    }

    fn get_temp_whole_object_path(&self, upload_id: &str) -> PathBuf {
        self.temp_path.join(upload_id)
    }

    pub async fn stream_to_temp_file(
        &self,
        mut data_stream: impl futures_util::Stream<Item = Result<Vec<u8>, tonic::Status>> + Unpin,
    ) -> Result<(PathBuf, i64, String)> {
        info!("stream_to_temp_file called");
        let upload_id = uuid::Uuid::new_v4().to_string();
        let temp_path = self.get_temp_whole_object_path(&upload_id);
        let mut file = fs::File::create(&temp_path).await?;

        let mut overall_hasher = blake3::Hasher::new();
        let mut total_bytes = 0;

        while let Some(chunk_result) = data_stream.next().await {
            let chunk = chunk_result.map_err(|e| anyhow::anyhow!(e.to_string()))?;
            file.write_all(&chunk).await?;
            overall_hasher.update(&chunk);
            total_bytes += chunk.len() as i64;
        }

        let content_hash = overall_hasher.finalize().to_hex().to_string();
        info!(
            ?temp_path,
            total_bytes,
            %content_hash,
            "stream_to_temp_file finished"
        );
        Ok((temp_path, total_bytes, content_hash))
    }

    pub async fn commit_whole_object(
        &self,
        temp_path: &Path,
        final_object_hash: &str,
    ) -> Result<()> {
        let final_path = self.get_whole_object_path(final_object_hash);
        info!(
            temp_path = %temp_path.display(),
            final_path = %final_path.display(),
            "Renaming temporary file to final object path"
        );
        fs::rename(temp_path, final_path).await?;
        info!("File renamed successfully");
        Ok(())
    }

    pub async fn commit_external_chunks(
        &self,
        temp_path: &Path,
        final_object_hash: &str,
    ) -> Result<ExternalChunkManifest> {
        ensure_hash_hex(final_object_hash, "final object hash")?;
        let mut source = fs::File::open(temp_path).await?;
        let mut buffer = vec![0_u8; DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES];
        let mut chunk_index = 0_u64;
        let mut chunks = Vec::new();

        loop {
            let mut filled = 0usize;
            while filled < buffer.len() {
                let read = source.read(&mut buffer[filled..]).await?;
                if read == 0 {
                    break;
                }
                filled += read;
            }
            if filled == 0 {
                break;
            }

            let chunk_bytes = &buffer[..filled];
            let payload_chunk_hash = blake3::hash(chunk_bytes).to_hex().to_string();
            let final_path =
                self.external_chunk_path(final_object_hash, chunk_index, &payload_chunk_hash);
            if let Some(parent) = final_path.parent() {
                fs::create_dir_all(parent).await?;
            }
            if fs::metadata(&final_path).await.is_err() {
                let tmp_path = self.temp_path.join(format!(
                    "chunk-{final_object_hash}-{chunk_index:020}-{payload_chunk_hash}"
                ));
                let mut out = fs::File::create(&tmp_path).await?;
                out.write_all(chunk_bytes).await?;
                out.flush().await?;
                fs::rename(tmp_path, &final_path).await?;
            }
            let storage_ref = self.relative_storage_path(&final_path)?;
            chunks.push(ExternalChunkRecord {
                chunk_index,
                plaintext_length: filled as u64,
                ciphertext_length: filled as u64,
                payload_chunk_hash: payload_chunk_hash.clone(),
                storage_chunk_hash: payload_chunk_hash,
                compression: "none".to_string(),
                base_nonce: String::new(),
                mac: String::new(),
                storage_ref,
            });
            chunk_index = chunk_index
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("external chunk index overflow"))?;
        }

        fs::remove_file(temp_path).await?;
        Ok(ExternalChunkManifest {
            kind: "external_chunks_v1".to_string(),
            chunk_size: DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES,
            chunks,
        })
    }
}

fn ensure_safe_internal_component(value: &str, context: &str) -> Result<()> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
        || value.chars().any(|ch| ch == '\0' || ch.is_control())
    {
        anyhow::bail!("{context} is not a safe path component");
    }
    Ok(())
}

fn ensure_hash_hex(value: &str, context: &str) -> Result<()> {
    if value.len() != 64 || !value.as_bytes().iter().all(|byte| byte.is_ascii_hexdigit()) {
        anyhow::bail!("{context} must be 32 bytes encoded as hex");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_store_and_retrieve_shard() {
        let storage = Storage::new().await.unwrap();
        let upload_id = "test-upload";
        let final_hash = "test-hash";
        let shard_index = 0;
        let data = b"hello world";

        // Store the shard to a temporary location
        storage
            .store_temp_shard(upload_id, shard_index, data)
            .await
            .unwrap();

        // Commit the shard
        storage
            .commit_shard(upload_id, shard_index, final_hash)
            .await
            .unwrap();

        // Retrieve the shard from the final location
        let retrieved_data = storage
            .retrieve_shard(final_hash, shard_index)
            .await
            .unwrap();

        assert_eq!(data.as_ref(), retrieved_data.as_slice());

        // Clean up the test file
        fs::remove_file(storage.get_shard_path(final_hash, shard_index))
            .await
            .unwrap();
    }
}

async fn collect_bucket_partition_journals(
    root: PathBuf,
    file_name: &str,
) -> anyhow::Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    if tokio::fs::metadata(&root).await.is_err() {
        return Ok(paths);
    }
    let mut tenants = tokio::fs::read_dir(&root).await?;
    while let Some(tenant) = tenants.next_entry().await? {
        let mut buckets = match tokio::fs::read_dir(tenant.path()).await {
            Ok(entries) => entries,
            Err(_) => continue,
        };
        while let Some(bucket) = buckets.next_entry().await? {
            let path = bucket.path().join(file_name);
            if tokio::fs::metadata(&path).await.is_ok() {
                paths.push(path);
            }
        }
    }
    paths.sort();
    Ok(paths)
}
