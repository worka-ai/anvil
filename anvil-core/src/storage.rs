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

    pub fn core_store_root_path(&self) -> PathBuf {
        self.storage_path.join("_core")
    }

    pub fn core_store_replicas_path(&self) -> PathBuf {
        self.core_store_root_path().join("replicas")
    }

    pub fn core_store_replica_path(&self, node_id: &str) -> PathBuf {
        self.core_store_replicas_path().join(node_id)
    }

    pub fn core_store_staging_path(&self) -> PathBuf {
        self.core_store_root_path().join("staging")
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

    fn get_temp_payload_path(&self, upload_id: &str) -> PathBuf {
        self.temp_path.join(upload_id)
    }

    pub async fn stream_to_temp_file(
        &self,
        mut data_stream: impl futures_util::Stream<Item = Result<Vec<u8>, tonic::Status>> + Unpin,
    ) -> Result<(PathBuf, i64, String)> {
        info!("stream_to_temp_file called");
        let upload_id = uuid::Uuid::new_v4().to_string();
        let temp_path = self.get_temp_payload_path(&upload_id);
        let mut file = fs::File::create(&temp_path).await?;

        let mut overall_hasher = blake3::Hasher::new();
        let mut total_bytes = 0;

        while let Some(chunk_result) = data_stream.next().await {
            let chunk = chunk_result.map_err(|e| anyhow::anyhow!(e.to_string()))?;
            file.write_all(&chunk).await?;
            overall_hasher.update(&chunk);
            total_bytes += chunk.len() as i64;
        }
        file.flush().await?;

        let content_hash = overall_hasher.finalize().to_hex().to_string();
        info!(
            ?temp_path,
            total_bytes,
            %content_hash,
            "stream_to_temp_file finished"
        );
        Ok((temp_path, total_bytes, content_hash))
    }
}
