use anyhow::Result;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

const STORAGE_DIR: &str = "anvil-data";

#[derive(Debug, Clone)]
pub struct Storage {
    storage_path: PathBuf,
}

impl Storage {
    pub async fn new() -> Result<Self> {
        let storage_path = Path::new(STORAGE_DIR).to_path_buf();
        fs::create_dir_all(&storage_path).await?;
        Ok(Self { storage_path })
    }

    fn get_shard_path(&self, object_hash: &str, shard_index: u32) -> PathBuf {
        self.storage_path.join(format!("{}-{:02}", object_hash, shard_index))
    }

    pub async fn store_shard(&self, object_hash: &str, shard_index: u32, data: &[u8]) -> Result<()> {
        let file_path = self.get_shard_path(object_hash, shard_index);
        let mut file = fs::File::create(file_path).await?;
        file.write_all(data).await?;
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
        let object_hash = "test-hash";
        let shard_index = 0;
        let data = b"hello world";

        // Store the shard
        storage.store_shard(object_hash, shard_index, data).await.unwrap();

        // Retrieve the shard
        let retrieved_data = storage.retrieve_shard(object_hash, shard_index).await.unwrap();

        assert_eq!(data.as_ref(), retrieved_data.as_slice());

        // Clean up the test file
        fs::remove_file(storage.get_shard_path(object_hash, shard_index)).await.unwrap();
    }
}
