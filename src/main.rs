use anvil::run;
use clap::Parser;
use std::net::SocketAddr;

mod config;
use anvil::config::Config;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let config = Config::parse();

    let addr = config
        .grpc_bind_addr
        .parse::<SocketAddr>()
        .expect("Invalid gRPC bind address");
    let listener = tokio::net::TcpListener::bind(addr).await?;

    println!("Anvil server (gRPC & S3) listening on {}", addr);

    run(listener, config).await?;
    Ok(())
}