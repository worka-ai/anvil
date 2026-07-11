use anyhow::{Context, Result, bail};
use libp2p::{PeerId, identity};
use prost::Message;

use crate::core_store::{
    CF_MESH, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart, CoreStore,
    TABLE_NODE_SIGNING_KEYPAIR_ROW, core_meta_committed_row_common, core_meta_root_key_hash,
    core_meta_tuple_key, decode_deterministic_proto, encode_deterministic_proto,
};
use crate::storage::Storage;

const NODE_ID_PREFIX: &str = "node_";
const CLUSTER_IDENTITY_SCHEMA: &str = "anvil.mesh.cluster_identity.v1";

pub struct ClusterIdentity {
    pub node_id: String,
    pub cluster_keypair: identity::Keypair,
}

#[derive(Clone, PartialEq, Message)]
struct ClusterIdentityProto {
    #[prost(message, optional, tag = "1")]
    common: Option<crate::core_store::CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(string, tag = "3")]
    node_id: String,
    #[prost(bytes = "vec", tag = "4")]
    cluster_keypair_protobuf: Vec<u8>,
}

pub fn cluster_peer_id(keypair: &identity::Keypair) -> PeerId {
    keypair.public().to_peer_id()
}

pub async fn load_or_create_cluster_identity(
    storage_path: impl AsRef<std::path::Path>,
) -> Result<ClusterIdentity> {
    let storage = Storage::new_at(storage_path).await?;
    let meta = CoreMetaStore::open(storage.core_store_meta_path())?;
    let key = cluster_identity_key()?;
    if let Some(bytes) = meta.get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &key)? {
        return decode_cluster_identity(&bytes);
    }

    let node_id = generate_node_id();
    let record = ClusterIdentityProto {
        common: Some(core_meta_committed_row_common(
            "system",
            core_meta_root_key_hash("mesh/cluster-identity/local"),
            1,
            node_id.clone(),
            0,
        )),
        schema: CLUSTER_IDENTITY_SCHEMA.to_string(),
        node_id,
        cluster_keypair_protobuf: identity::Keypair::generate_ed25519().to_protobuf_encoding()?,
    };
    validate_node_id(&record.node_id)?;
    parse_cluster_keypair_bytes(&record.cluster_keypair_protobuf)
        .context("generated cluster identity keypair protobuf is invalid")?;
    let payload = encode_deterministic_proto(&record);
    let op = CoreMetaBatchOp {
        cf: CF_MESH,
        table_id: TABLE_NODE_SIGNING_KEYPAIR_ROW,
        tuple_key: &key,
        common: record.common.clone(),
        kind: CoreMetaBatchOpKind::Put(&payload),
    };
    CoreStore::new(storage.clone())
        .await?
        .commit_coremeta_batch_by_embedded_roots(&record.node_id, &[op])
        .await?;
    let stored = meta
        .get(CF_MESH, TABLE_NODE_SIGNING_KEYPAIR_ROW, &key)?
        .context("CoreStore cluster identity row was not readable after write")?;
    decode_cluster_identity(&stored)
}

fn cluster_identity_key() -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("cluster-identity"),
        CoreMetaTuplePart::Utf8("local"),
    ])
}

fn decode_cluster_identity(bytes: &[u8]) -> Result<ClusterIdentity> {
    let record =
        decode_deterministic_proto::<ClusterIdentityProto>(bytes, "cluster identity record")?;
    if record.schema != CLUSTER_IDENTITY_SCHEMA {
        bail!("CoreStore cluster identity row has invalid schema");
    }
    record
        .common
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("CoreStore cluster identity row missing CoreMeta common"))?;
    validate_node_id(&record.node_id)?;
    let cluster_keypair = parse_cluster_keypair_bytes(&record.cluster_keypair_protobuf)
        .context("CoreStore cluster identity keypair is invalid")?;
    Ok(ClusterIdentity {
        node_id: record.node_id,
        cluster_keypair,
    })
}

fn generate_node_id() -> String {
    format!("{NODE_ID_PREFIX}{}", uuid::Uuid::new_v4().simple())
}

fn validate_node_id(node_id: &str) -> Result<()> {
    if node_id.is_empty() {
        bail!("CoreStore cluster identity node id is empty");
    }
    if node_id
        .chars()
        .any(|ch| ch == '/' || ch == '\0' || ch.is_control())
    {
        bail!("CoreStore cluster identity node id contains an invalid character");
    }
    Ok(())
}

fn parse_cluster_keypair_bytes(bytes: &[u8]) -> Result<identity::Keypair> {
    if bytes.is_empty() {
        bail!("CoreStore cluster identity keypair protobuf is empty");
    }
    identity::Keypair::from_protobuf_encoding(bytes)
        .context("CoreStore cluster identity keypair is not a valid libp2p keypair")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn node_identity_is_created_in_coremeta_and_reloaded() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");

        let first = load_or_create_cluster_identity(&storage_path)
            .await
            .unwrap();
        let second = load_or_create_cluster_identity(&storage_path)
            .await
            .unwrap();

        assert!(
            storage_path
                .join("corestore")
                .join("meta")
                .join("rocksdb")
                .exists()
        );
        assert!(first.node_id.starts_with(NODE_ID_PREFIX));
        assert_eq!(first.node_id, second.node_id);
    }

    #[tokio::test]
    async fn cluster_keypair_is_created_in_coremeta_and_reloaded() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");

        let first = load_or_create_cluster_identity(&storage_path)
            .await
            .unwrap();
        let second = load_or_create_cluster_identity(&storage_path)
            .await
            .unwrap();

        assert_eq!(
            cluster_peer_id(&first.cluster_keypair),
            cluster_peer_id(&second.cluster_keypair)
        );
    }

    #[tokio::test]
    async fn node_id_and_cluster_keypair_share_one_coremeta_identity() {
        let temp = tempdir().unwrap();
        let identity = load_or_create_cluster_identity(temp.path().join("node-a"))
            .await
            .unwrap();
        let reloaded = load_or_create_cluster_identity(temp.path().join("node-a"))
            .await
            .unwrap();

        assert_eq!(identity.node_id, reloaded.node_id);
        assert_eq!(
            cluster_peer_id(&identity.cluster_keypair),
            cluster_peer_id(&reloaded.cluster_keypair)
        );
    }
}
