use anyhow::Result;
use axum::ServiceExt;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use std::str::FromStr;
use tonic::service;
use once_cell::sync::OnceCell;
use tokio_postgres::NoTls;
use tower::ServiceExt as TowerServiceExt;
use tracing::{error, info};

// Re-export the core types for the binary and services to use.
pub use anvil_core::*;

// Modules that remain in the main anvil crate
pub mod s3_gateway;

pub mod s3_auth;

pub mod migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("./migrations_global");
}

pub mod regional_migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("./migrations_regional");
}

pub async fn run(listener: tokio::net::TcpListener, config: anvil_core::config::Config) -> Result<()> {
    // Run migrations first
    run_migrations(
        &config.global_database_url,
        migrations::migrations::runner(),
        "refinery_schema_history_global",
    )
    .await?;
    run_migrations(
        &config.regional_database_url,
        regional_migrations::migrations::runner(),
        "refinery_schema_history_regional",
    )
    .await?;

    let regional_pool = create_pool(&config.regional_database_url)?;
    let global_pool = create_pool(&config.global_database_url)?;
    let state = AppState::new(global_pool, regional_pool, config).await?;
    let swarm = anvil_core::cluster::create_swarm(state.config.clone()).await?;

    // Then start the node
    start_node(listener, state, swarm).await
}

pub async fn start_node(
    listener: tokio::net::TcpListener,
    state: AppState,
    mut swarm: libp2p::Swarm<anvil_core::cluster::ClusterBehaviour>,
) -> Result<()> {
    for addr in &state.config.bootstrap_addrs {
        let multiaddr: libp2p::Multiaddr = addr.parse()?;
        swarm.dial(multiaddr)?;
    }

    let worker_state = state.clone();
    tokio::spawn(async move {
        if let Err(e) = anvil_core::worker::run(
            worker_state.db.clone(),
            worker_state.cluster.clone(),
            worker_state.jwt_manager.clone(),
            worker_state.object_manager.clone(),
        )
        .await
        {
            error!("Worker process failed: {}", e);
        }
    });

    // --- Services ---
    let state_clone = state.clone();
    let auth_interceptor = anvil_core::services::AuthInterceptorFn::new(move |req: tonic::Request<()>| {
        middleware::auth_interceptor(req, &state_clone)
    });

    let mut grpc_router = anvil_core::services::create_grpc_router(state.clone(), auth_interceptor.clone());

    if let Some(ext) = ENTERPRISE_EXTENDER.get() {
        grpc_router = ext(grpc_router, state.clone(), auth_interceptor);
    }

    let grpc_axum = anvil_core::services::create_axum_router(grpc_router);
    let s3_app = s3_gateway::app(state.clone());

    let app = tower::service_fn(move |req: axum::extract::Request| {
        let grpc_router = grpc_axum.clone();
        let s3_router = s3_app.clone();

        async move {
            let content_type = req
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");

            if content_type.starts_with("application/grpc") {
                grpc_router.oneshot(req).await
            } else {
                tracing::info!("[gRPC Mux] Routing to S3 gateway for content-type: {}", content_type);
                s3_router.oneshot(req).await
            }
        }
    });

    let addr = listener.local_addr()?;
    info!("Anvil server (gRPC & S3) listening on {}", addr);

    // Spawn the gossip service to run in the background.
    let gossip_task = tokio::spawn(anvil_core::cluster::run_gossip(
        swarm,
        state.cluster.clone(),
        state.config.public_api_addr.clone(),
        state.config.cluster_secret.clone(),
    ));
    let server_task =
        tokio::spawn(async move { axum::serve(listener, app.into_make_service()).await });

    // Run both tasks concurrently.
    let (server_result, gossip_result) = tokio::join!(server_task, gossip_task);
    server_result??;
    gossip_result??;

    Ok(())
}

pub fn create_pool(db_url: &str) -> Result<Pool> {
    let pg_config = tokio_postgres::Config::from_str(db_url)?;
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = deadpool_postgres::Manager::from_config(pg_config, NoTls, mgr_config);
    Pool::builder(mgr).build().map_err(Into::into)
}

pub async fn run_migrations(
    db_url: &str,
    mut runner: refinery::Runner,
    table_name: &str,
) -> Result<()> {
    let (mut client, connection) = tokio_postgres::connect(db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("connection error: {}", e);
        }
    });
    runner
        .set_migration_table_name(table_name)
        .run_async(&mut client)
        .await?;
    Ok(())
}
static ENTERPRISE_EXTENDER: OnceCell<
    fn(service::Routes, anvil_core::AppState, anvil_core::services::AuthInterceptorFn) -> service::Routes,
> = OnceCell::new();

pub fn register_enterprise_extender(
    f: fn(service::Routes, anvil_core::AppState, anvil_core::services::AuthInterceptorFn) -> service::Routes,
) {
    let _ = ENTERPRISE_EXTENDER.set(f);
}
