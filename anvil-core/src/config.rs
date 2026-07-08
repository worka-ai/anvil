use clap::Parser;
use std::path::{Path, PathBuf};

use crate::routing::CrossRegionRoutingPolicy;
use anyhow::Result;

/// A distributed storage and compute system.
#[derive(Parser, Debug, Clone, Default)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// The secret key used for signing JWTs.
    #[arg(long, env)]
    pub jwt_secret: String,

    /// Active hex-encoded 32-byte key used for server-side secret encryption.
    #[arg(long, env)]
    pub anvil_secret_encryption_key: String,

    /// Identifier written into new encrypted secret envelopes.
    #[arg(long, env, default_value = "primary")]
    pub anvil_secret_encryption_key_id: String,

    /// Comma-delimited previous secret encryption keys as `key_id:hex`.
    #[arg(long, env, default_value = "")]
    pub anvil_secret_encryption_previous_keys: String,

    /// The address to bind the QUIC peer-to-peer endpoint to.
    #[arg(long, env, default_value = "/ip4/0.0.0.0/udp/7443/quic-v1")]
    pub cluster_listen_addr: String,

    /// The publicly reachable addresses for this node.
    #[arg(long, env, use_value_delimiter = true, value_delimiter = ',')]
    pub public_cluster_addrs: Vec<String>,

    /// The publicly reachable gRPC address for this node.
    #[arg(long, env)]
    pub public_api_addr: String,

    /// The address to bind the main gRPC service to.
    #[arg(long, env, default_value = "0.0.0.0:50051")]
    pub api_listen_addr: String,

    /// The address to bind the administrative gRPC service to.
    #[arg(long, env, default_value = "127.0.0.1:50052")]
    pub admin_listen_addr: String,

    /// Explicitly allow binding the private admin plane to a non-loopback address.
    #[arg(long, env, default_value_t = false)]
    pub allow_public_admin_listener: bool,

    /// Stable mesh identifier for administrative and lifecycle records.
    #[arg(long, env, default_value = "default")]
    pub mesh_id: String,

    /// First-boot system-realm admin app name. Used only when the system realm is absent.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_app_name: String,

    /// File path where the first-boot admin app credential JSON is written.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_credential_output_path: String,

    /// Existing first-boot admin subject kind. Used instead of creating an app.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_subject_kind: String,

    /// Existing first-boot admin subject id. Used instead of creating an app.
    #[arg(long, env, default_value = "")]
    pub bootstrap_system_admin_subject_id: String,

    /// The current region this node is operating in.
    #[arg(long, env)]
    pub region: String,

    /// The current cell this node is operating in.
    #[arg(long, env, default_value = "default")]
    pub cell_id: String,

    /// Region host suffix advertised for virtual-host routing.
    #[arg(long, env, default_value = "")]
    pub public_region_base_domain: String,

    /// Trusted proxy source IPs or CIDR ranges allowed to supply forwarded request metadata.
    #[arg(long, env, use_value_delimiter = true, value_delimiter = ',')]
    pub trusted_proxy_source_ranges: Vec<String>,

    /// Policy for requests whose bucket locator is owned by another region.
    #[arg(long, env, default_value_t = CrossRegionRoutingPolicy::RedirectPreferred)]
    pub cross_region_routing_policy: CrossRegionRoutingPolicy,

    /// Path used by operators to persist this node's stable lifecycle identity.
    /// Defaults to `<storage_path>/node-id` when left empty.
    #[arg(long, env, default_value = "")]
    pub node_id_path: String,

    /// Resolved stable node id loaded from `node_id_path` during startup.
    #[arg(skip)]
    pub node_id: String,

    /// Path used to persist the libp2p keypair backing the cluster identity.
    /// Defaults to `<storage_path>/cluster-keypair.pb` when left empty.
    #[arg(long, env, default_value = "")]
    pub cluster_keypair_path: String,

    /// A list of bootstrap addresses for joining a cluster.
    #[arg(long, env, use_value_delimiter = true, value_delimiter = ',')]
    pub bootstrap_addrs: Vec<String>,

    /// Initialize a new cluster.
    #[arg(long, env, default_value_t = false)]
    pub init_cluster: bool,

    /// Enable mDNS for local peer discovery.
    #[arg(long, env, default_value_t = true)]
    pub enable_mdns: bool,

    /// The shared secret for cluster authentication.
    #[arg(long, env)]
    pub cluster_secret: Option<String>,

    /// TTL for metadata cache entries in seconds.
    #[arg(long, env, default_value_t = 300)]
    pub metadata_cache_ttl_secs: u64,

    /// Directory used for Anvil-owned object bytes, metadata journals, indexes, and manifests.
    #[arg(long, env, default_value = "anvil-data")]
    pub storage_path: String,

    /// PersonalDB entries committed after the latest snapshot before building another snapshot.
    #[arg(long, env, default_value_t = 1024)]
    pub personaldb_snapshot_entry_threshold: u64,

    /// PersonalDB changeset payload bytes committed after the latest snapshot before building another snapshot.
    #[arg(long, env, default_value_t = 64 * 1024 * 1024)]
    pub personaldb_snapshot_payload_bytes_threshold: u64,

    /// Enables the deterministic vector embedding provider used only by test harnesses.
    #[arg(skip)]
    pub allow_test_only_embedding_provider: bool,

    /// JSON object defining named production vector embedding providers.
    #[arg(long, env, default_value = "")]
    pub vector_embedding_providers_json: String,

    /// Uncompacted object metadata journal frames allowed before scheduling compaction.
    #[arg(long, env, default_value_t = 4096)]
    pub object_metadata_compaction_frame_threshold: u64,

    /// Uncompacted object metadata journal bytes allowed before scheduling compaction.
    #[arg(long, env, default_value_t = 64 * 1024 * 1024)]
    pub object_metadata_compaction_bytes_threshold: u64,

    /// Seconds that an in-process background task lease remains valid without renewal.
    #[arg(long, env, default_value_t = 300)]
    pub task_lease_ttl_secs: u64,
}
impl Config {
    #[allow(unused)]
    pub fn from_ref(args: &Self) -> Self {
        let mut me = Self::default();
        args.clone_into(&mut me);
        me
    }

    pub fn resolved_node_id_path(&self) -> PathBuf {
        resolve_identity_path(
            &self.node_id_path,
            &self.storage_path,
            crate::cluster_identity::DEFAULT_NODE_ID_FILE,
        )
    }

    pub fn resolved_cluster_keypair_path(&self) -> PathBuf {
        resolve_identity_path(
            &self.cluster_keypair_path,
            &self.storage_path,
            crate::cluster_identity::DEFAULT_CLUSTER_KEYPAIR_FILE,
        )
    }

    pub fn secret_keyring(&self) -> Result<crate::crypto::EncryptionKeyring> {
        let active_key_id = self.active_encryption_key_id();
        crate::crypto::EncryptionKeyring::from_hex_config(
            active_key_id,
            &self.anvil_secret_encryption_key,
            &self.anvil_secret_encryption_previous_keys,
        )
    }

    pub fn core_pipeline_keyring(&self) -> Result<crate::core_store::CorePipelineKeyring> {
        let active_key_id = self.active_encryption_key_id();
        crate::core_store::CorePipelineKeyring::from_hex_config(
            active_key_id,
            &self.anvil_secret_encryption_key,
            &self.anvil_secret_encryption_previous_keys,
        )
    }

    fn active_encryption_key_id(&self) -> &str {
        if self.anvil_secret_encryption_key_id.trim().is_empty() {
            "primary"
        } else {
            &self.anvil_secret_encryption_key_id
        }
    }

    pub fn validate_admin_listener_bind(&self) -> Result<()> {
        let addr: std::net::SocketAddr = self.admin_listen_addr.parse()?;
        if !self.allow_public_admin_listener && !addr.ip().is_loopback() {
            anyhow::bail!(
                "ADMIN_LISTEN_ADDR={} is not loopback; set ALLOW_PUBLIC_ADMIN_LISTENER=true only when the admin port is protected by private networking",
                self.admin_listen_addr
            );
        }
        Ok(())
    }

    pub fn with_persisted_identity(mut self) -> Result<Self> {
        let node_id_path = self.resolved_node_id_path();
        let cluster_keypair_path = self.resolved_cluster_keypair_path();

        self.node_id = crate::cluster_identity::load_or_create_node_id(&node_id_path)?;
        crate::cluster_identity::load_or_create_cluster_keypair(&cluster_keypair_path)?;
        self.node_id_path = node_id_path.to_string_lossy().into_owned();
        self.cluster_keypair_path = cluster_keypair_path.to_string_lossy().into_owned();

        Ok(self)
    }
}

fn resolve_identity_path(configured_path: &str, storage_path: &str, default_file: &str) -> PathBuf {
    let configured_path = configured_path.trim();
    if configured_path.is_empty() {
        return Path::new(storage_path).join(default_file);
    }
    PathBuf::from(configured_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn required_args() -> [&'static str; 9] {
        [
            "anvil",
            "--jwt-secret",
            "test-secret",
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--public-api-addr",
            "test-node",
            "--region",
            "us-east-1",
        ]
    }

    #[test]
    fn cross_region_routing_policy_defaults_to_redirect_preferred() {
        let config = Config::try_parse_from(required_args()).unwrap();

        assert_eq!(
            config.cross_region_routing_policy,
            CrossRegionRoutingPolicy::RedirectPreferred
        );
    }

    #[test]
    fn cross_region_routing_policy_parses_configured_value() {
        let mut args = required_args().to_vec();
        args.extend(["--cross-region-routing-policy", "local_only"]);
        let config = Config::try_parse_from(args).unwrap();

        assert_eq!(
            config.cross_region_routing_policy,
            CrossRegionRoutingPolicy::LocalOnly
        );
    }

    #[test]
    fn persisted_identity_uses_storage_defaults_and_reloads() {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let config = Config {
            storage_path: storage_path.to_string_lossy().into_owned(),
            ..Config::default()
        };

        let first = config.with_persisted_identity().unwrap();
        let restarted = Config {
            storage_path: storage_path.to_string_lossy().into_owned(),
            ..Config::default()
        }
        .with_persisted_identity()
        .unwrap();

        assert_eq!(first.node_id, restarted.node_id);
        assert_eq!(
            PathBuf::from(&first.node_id_path),
            storage_path.join("node-id")
        );
        assert_eq!(
            PathBuf::from(&first.cluster_keypair_path),
            storage_path.join("cluster-keypair.pb")
        );
        assert!(Path::new(&first.node_id_path).exists());
        assert!(Path::new(&first.cluster_keypair_path).exists());
    }

    #[test]
    fn admin_listener_rejects_public_bind_without_explicit_opt_in() {
        for addr in ["0.0.0.0:50052", "[::]:50052", "192.168.1.10:50052"] {
            let config = Config {
                admin_listen_addr: addr.to_string(),
                allow_public_admin_listener: false,
                ..Config::default()
            };

            assert!(config.validate_admin_listener_bind().is_err(), "{addr}");
        }
    }

    #[test]
    fn admin_listener_allows_loopback_or_explicit_public_opt_in() {
        for addr in ["127.0.0.1:50052", "[::1]:50052"] {
            let config = Config {
                admin_listen_addr: addr.to_string(),
                allow_public_admin_listener: false,
                ..Config::default()
            };

            config.validate_admin_listener_bind().unwrap();
        }

        let config = Config {
            admin_listen_addr: "0.0.0.0:50052".to_string(),
            allow_public_admin_listener: true,
            ..Config::default()
        };
        config.validate_admin_listener_bind().unwrap();
    }
}
