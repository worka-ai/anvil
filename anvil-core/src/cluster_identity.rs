use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use libp2p::{PeerId, identity};

const NODE_ID_PREFIX: &str = "node_";

pub const DEFAULT_NODE_ID_FILE: &str = "node-id";
pub const DEFAULT_CLUSTER_KEYPAIR_FILE: &str = "cluster-keypair.pb";

pub fn default_node_id_path(storage_path: &str) -> PathBuf {
    Path::new(storage_path).join(DEFAULT_NODE_ID_FILE)
}

pub fn default_cluster_keypair_path(storage_path: &str) -> PathBuf {
    Path::new(storage_path).join(DEFAULT_CLUSTER_KEYPAIR_FILE)
}

pub fn load_or_create_node_id(path: impl AsRef<Path>) -> Result<String> {
    let path = path.as_ref();
    match fs::read_to_string(path) {
        Ok(raw) => validate_node_id_file(path, &raw),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let node_id = generate_node_id();
            persist_new_file(path, node_id.as_bytes()).with_context(|| {
                format!("failed to persist node identity file {}", path.display())
            })?;
            Ok(node_id)
        }
        Err(err) => Err(err)
            .with_context(|| format!("failed to read node identity file {}", path.display())),
    }
}

pub fn load_or_create_cluster_keypair(path: impl AsRef<Path>) -> Result<identity::Keypair> {
    let path = path.as_ref();
    match fs::read(path) {
        Ok(bytes) => parse_cluster_keypair(path, &bytes),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            let keypair = identity::Keypair::generate_ed25519();
            let bytes = keypair.to_protobuf_encoding()?;
            persist_new_file(path, &bytes).with_context(|| {
                format!("failed to persist cluster keypair file {}", path.display())
            })?;
            Ok(keypair)
        }
        Err(err) => Err(err)
            .with_context(|| format!("failed to read cluster keypair file {}", path.display())),
    }
}

pub fn cluster_peer_id(keypair: &identity::Keypair) -> PeerId {
    keypair.public().to_peer_id()
}

fn generate_node_id() -> String {
    format!("{NODE_ID_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

fn validate_node_id_file(path: &Path, raw: &str) -> Result<String> {
    let node_id = raw.trim();
    if node_id.is_empty() {
        bail!("node identity file {} is empty", path.display());
    }
    if node_id
        .chars()
        .any(|ch| ch == '/' || ch == '\0' || ch.is_control())
    {
        bail!(
            "node identity file {} contains an invalid node id",
            path.display()
        );
    }
    Ok(node_id.to_string())
}

fn parse_cluster_keypair(path: &Path, bytes: &[u8]) -> Result<identity::Keypair> {
    if bytes.is_empty() {
        bail!("cluster keypair file {} is empty", path.display());
    }
    identity::Keypair::from_protobuf_encoding(bytes).with_context(|| {
        format!(
            "cluster keypair file {} is not a valid libp2p keypair",
            path.display()
        )
    })
}

fn persist_new_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = parent_dir(path);
    fs::create_dir_all(&parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))?;

    let mut temp = tempfile::NamedTempFile::new_in(&parent)
        .with_context(|| format!("failed to create temporary file in {}", parent.display()))?;
    temp.write_all(bytes)
        .with_context(|| format!("failed to write temporary file for {}", path.display()))?;
    temp.as_file()
        .sync_all()
        .with_context(|| format!("failed to sync temporary file for {}", path.display()))?;
    temp.persist_noclobber(path)
        .map_err(|err| err.error)
        .with_context(|| format!("failed to atomically create {}", path.display()))?;
    sync_parent_dir(&parent);
    Ok(())
}

fn parent_dir(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn sync_parent_dir(parent: &Path) {
    if let Ok(parent_file) = File::open(parent) {
        let _ = parent_file.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn node_identity_missing_file_is_created_and_reloaded() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("nested").join("node-id");

        let first = load_or_create_node_id(&path).unwrap();
        let second = load_or_create_node_id(&path).unwrap();

        assert!(path.exists());
        assert!(first.starts_with(NODE_ID_PREFIX));
        assert_eq!(first, second);
    }

    #[test]
    fn node_identity_empty_file_fails_clearly() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("node-id");
        fs::write(&path, "").unwrap();

        let err = load_or_create_node_id(&path).unwrap_err();

        assert!(err.to_string().contains("node identity file"));
        assert!(err.to_string().contains("is empty"));
    }

    #[test]
    fn node_identity_invalid_file_fails_clearly() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("node-id");
        fs::write(&path, "node/invalid").unwrap();

        let err = load_or_create_node_id(&path).unwrap_err();

        assert!(err.to_string().contains("contains an invalid node id"));
    }

    #[test]
    fn cluster_keypair_missing_file_is_created_and_reloaded() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("cluster").join("cluster-keypair.pb");

        let first = load_or_create_cluster_keypair(&path).unwrap();
        let second = load_or_create_cluster_keypair(&path).unwrap();

        assert!(path.exists());
        assert_eq!(cluster_peer_id(&first), cluster_peer_id(&second));
    }

    #[test]
    fn cluster_keypair_empty_file_fails_clearly() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("cluster-keypair.pb");
        fs::write(&path, "").unwrap();

        let err = load_or_create_cluster_keypair(&path).unwrap_err();

        assert!(err.to_string().contains("cluster keypair file"));
        assert!(err.to_string().contains("is empty"));
    }

    #[test]
    fn cluster_keypair_invalid_file_fails_clearly() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("cluster-keypair.pb");
        fs::write(&path, "not a protobuf keypair").unwrap();

        let err = load_or_create_cluster_keypair(&path).unwrap_err();

        assert!(err.to_string().contains("is not a valid libp2p keypair"));
    }
}
