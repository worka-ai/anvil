use anyhow::Result;
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
    pub async fn new() -> Result<Self> {
        let storage_path = Path::new(STORAGE_DIR).to_path_buf();
        let temp_path = storage_path.join(TEMP_DIR);
        fs::create_dir_all(&storage_path).await?;
        fs::create_dir_all(&temp_path).await?;
        Ok(Self { storage_path, temp_path })
    }

    fn get_shard_path(&self, object_hash: &str, shard_index: u32) -> PathBuf {
        self.storage_path.join(format!("{}-{:02}", object_hash, shard_index))
    }

    fn get_temp_shard_path(&self, upload_id: &str, shard_index: u32) -> PathBuf {
        self.temp_path.join(format!("{}-{:02}", upload_id, shard_index))
    }

    pub async fn store_temp_shard(&self, upload_id: &str, shard_index: u32, data: &[u8]) -> Result<()> {
        let file_path = self.get_temp_shard_path(upload_id, shard_index);
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .await?;
        file.write_all(data).await?;
        Ok(())
    }

    pub async fn commit_shard(&self, upload_id: &str, shard_index: u32, final_object_hash: &str) -> Result<()> {
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
        storage.store_temp_shard(upload_id, shard_index, data).await.unwrap();

        // Commit the shard
        storage.commit_shard(upload_id, shard_index, final_hash).await.unwrap();

        // Retrieve the shard from the final location
        let retrieved_data = storage.retrieve_shard(final_hash, shard_index).await.unwrap();

        assert_eq!(data.as_ref(), retrieved_data.as_slice());

        // Clean up the test file
        fs::remove_file(storage.get_shard_path(final_hash, shard_index)).await.unwrap();
    }
}