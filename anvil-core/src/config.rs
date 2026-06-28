use clap::Parser;

/// A distributed storage and compute system.
#[derive(Parser, Debug, Clone, Default)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// The secret key used for signing JWTs.
    #[arg(long, env)]
    pub jwt_secret: String,

    /// The secret key used for encrypting data at rest.
    #[arg(long, env)]
    pub anvil_secret_encryption_key: String,

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

    /// The current region this node is operating in.
    #[arg(long, env)]
    pub region: String,

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
}
impl Config {
    #[allow(unused)]
    pub fn from_ref(args: &Self) -> Self {
        let mut me = Self::default();
        args.clone_into(&mut me);
        me
    }
}
