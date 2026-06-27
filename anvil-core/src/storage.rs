use anyhow::Result;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::info;

const STORAGE_DIR: &str = "anvil-data";
const TEMP_DIR: &str = "tmp";

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

    pub fn object_watch_path(&self, tenant_id: i64, bucket_id: i64) -> PathBuf {
        self.storage_path
            .join("_anvil")
            .join("watch")
            .join("object")
            .join(format!("tenant-{tenant_id}"))
            .join(format!("bucket-{bucket_id}.anwatch"))
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
            .storage_path
            .join("_anvil")
            .join("personaldb")
            .join("tenants")
            .join(format!("tenant-{tenant_id}"))
            .join("groups")
            .join(database_id))
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
