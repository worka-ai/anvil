use anyhow::Result;
use futures_util::StreamExt;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

const STORAGE_DIR: &str = "anvil-data";
const TEMP_DIR: &str = "tmp";

#[derive(Debug, Clone)]
pub struct Storage {
    storage_path: PathBuf,
    temp_path: PathBuf,
}

impl Storage {
    pub async fn commit_whole_object_from_bytes(&self, data: &[u8], final_object_hash: &str) -> Result<()> {
        let final_path = self.get_whole_object_path(final_object_hash);
        let mut file = fs::File::create(&final_path).await?;
        file.write_all(data).await?;
        Ok(())
    }
    pub async fn new() -> Result<Self> {
        let storage_path = Path::new(STORAGE_DIR).to_path_buf();
        let temp_path = storage_path.join(TEMP_DIR);
        fs::create_dir_all(&storage_path).await?;
        fs::create_dir_all(&temp_path).await?;
        Ok(Self {
            storage_path,
            temp_path,
        })
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
        Ok((temp_path, total_bytes, content_hash))
    }

    pub async fn commit_whole_object(
        &self,
        temp_path: &Path,
        final_object_hash: &str,
    ) -> Result<()> {
        let final_path = self.get_whole_object_path(final_object_hash);
        fs::rename(temp_path, final_path).await?;
        Ok(())
    }
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
