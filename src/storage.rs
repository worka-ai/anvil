use anyhow::Result;
use std::path::{Path, PathBuf};
use tokio::fs;

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

    pub async fn store(&self, data: &[u8]) -> Result<String> {
        let hash = blake3::hash(data);
        let hex_hash = hash.to_hex().to_string();
        let file_path = self.storage_path.join(&hex_hash);

        fs::write(file_path, data).await?;

        Ok(hex_hash)
    }

    pub async fn retrieve(&self, hash: &str) -> Result<Vec<u8>> {
        let file_path = self.storage_path.join(hash);
        let data = fs::read(file_path).await?;
        Ok(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let storage = Storage::new().await.unwrap();
        let data = b"hello world";

        // Store the data
        let hash = storage.store(data).await.unwrap();

        // Retrieve the data
        let retrieved_data = storage.retrieve(&hash).await.unwrap();

        assert_eq!(data.as_ref(), retrieved_data.as_slice());

        // Clean up the test file
        fs::remove_file(storage.storage_path.join(hash)).await.unwrap();
    }
}
