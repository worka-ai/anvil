use anvil::run;
use std::env;
use std::net::SocketAddr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let addr = "[::1]:50051".parse::<SocketAddr>()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    // println!("Anvil server (gRPC & S3) listening on {}", grpc_addr);
    let region = env::var("REGION").expect("REGION must be set");
    let regional_db_url = env::var(format!("DATABASE_URL_REGION_{}", region.to_uppercase()))
        .expect("Regional DATABASE_URL must be set");
    let global_db_url = env::var("GLOBAL_DATABASE_URL").expect("Global DATABASE_URL must be set");
    let jwt_secret = env::var("JWT_SECRET").expect("JWT_SECRET must be set");

    run(listener, region, global_db_url, regional_db_url, jwt_secret).await?;
    Ok(())
}
