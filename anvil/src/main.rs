use anvil::run;
use clap::Parser;
use std::net::SocketAddr;
use tracing::info;

use anvil::config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    let addr = config
        .api_listen_addr
        .parse::<SocketAddr>()
        .expect("Invalid gRPC bind address");
    let admin_addr = config
        .admin_listen_addr
        .parse::<SocketAddr>()
        .expect("Invalid admin gRPC bind address");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    let admin_listener = tokio::net::TcpListener::bind(admin_addr).await?;

    info!("Anvil server (gRPC & S3) listening on {}", addr);
    info!("Anvil admin server (gRPC) listening on {}", admin_addr);

    run(listener, admin_listener, config).await?;
    Ok(())
}
