use clap::Parser;

/// A distributed storage and compute system.
#[derive(Parser, Debug, Clone, Default)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// The connection URL for the global metadata database.
    #[arg(long, env)]
    pub global_database_url: String,

    /// The connection URL for the regional metadata database.
    #[arg(long, env)]
    pub regional_database_url: String,

    /// The secret key used for signing JWTs.
    #[arg(long, env)]
    pub jwt_secret: String,

    /// The secret key used for encrypting data at rest.
    #[arg(long, env)]
    pub worka_secret_encryption_key: String,

    /// The address to bind the S3-compatible HTTP gateway to.
    #[arg(long, env, default_value = "0.0.0.0:9000")]
    pub http_bind_addr: String,

    /// The address to bind the QUIC peer-to-peer endpoint to.
    #[arg(long, env, default_value = "/ip4/0.0.0.0/udp/7443/quic-v1")]
    pub quic_bind_addr: String,

    /// The publicly reachable addresses for this node.
    #[arg(long, env, use_value_delimiter = true, value_delimiter = ',')]
    pub public_addrs: Vec<String>,

    /// The publicly reachable gRPC address for this node.
    #[arg(long, env)]
    pub public_grpc_addr: String,

    /// The address to bind the main gRPC service to.
    #[arg(long, env, default_value = "0.0.0.0:50051")]
    pub grpc_bind_addr: String,

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
}
impl Config {
    pub fn from_ref(args:&Self) -> Self {
        let mut me  = Self::default();
        args.clone_into(&mut me);
        me
    }
}
