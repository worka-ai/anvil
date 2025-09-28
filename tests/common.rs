use anyhow::Result;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use refinery::Runner;
use std::future::Future;
use std::str::FromStr;
use tokio_postgres::NoTls;

pub mod migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("migrations_global");
}

pub mod regional_migrations {
    use refinery_macros::embed_migrations;
    embed_migrations!("migrations_regional");
}

pub fn create_pool(db_url: &str) -> Result<Pool> {
    let pg_config = tokio_postgres::Config::from_str(db_url)?;
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    };
    let mgr = deadpool_postgres::Manager::from_config(pg_config, NoTls, mgr_config);
    Pool::builder(mgr).build().map_err(Into::into)
}

/// A test fixture that creates unique, isolated databases for a single test run
/// and guarantees they are cleaned up afterwards.
#[allow(dead_code)]
pub async fn with_test_dbs<F, Fut>(test_body: F)
where
    F: FnOnce(String, String, String) -> Fut,
    Fut: Future<Output = ()>+  Send + 'static,
{
    dotenvy::dotenv().ok();
    let maint_db_url = std::env::var("MAINTENANCE_DATABASE_URL").expect("MAINTENANCE_DATABASE_URL must be set");
    let (maint_client, connection) = tokio_postgres::connect(&maint_db_url, NoTls)
        .await
        .expect("Failed to connect to maintenance database");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("maintenance connection error: {}", e);
        }
    });

    let suffix = uuid::Uuid::new_v4().to_string().replace('-', "");
    let global_db_name = format!("test_global_{}", suffix);
    let east_db_name = format!("test_east_{}", suffix);
    let west_db_name = format!("test_west_{}", suffix);

    maint_client.execute(&format!("CREATE DATABASE \"{}\"", global_db_name), &[]).await.unwrap();
    maint_client.execute(&format!("CREATE DATABASE \"{}\"", east_db_name), &[]).await.unwrap();
    maint_client.execute(&format!("CREATE DATABASE \"{}\"", west_db_name), &[]).await.unwrap();

    let base_db_url = "postgres://worka:worka@localhost:5432";
    let global_db_url = format!("{}/{}", base_db_url, global_db_name);
    let east_db_url = format!("{}/{}", base_db_url, east_db_name);
    let west_db_url = format!("{}/{}", base_db_url, west_db_name);

    let test_future = test_body(global_db_url.clone(), east_db_url.clone(), west_db_url.clone());
    let result = tokio::spawn(test_future).await;

    // Cleanup
    maint_client.execute(&format!("DROP DATABASE \"{}\" WITH (FORCE)", global_db_name), &[]).await.unwrap();
    maint_client.execute(&format!("DROP DATABASE \"{}\" WITH (FORCE)", east_db_name), &[]).await.unwrap();
    maint_client.execute(&format!("DROP DATABASE \"{}\" WITH (FORCE)", west_db_name), &[]).await.unwrap();

    if let Err(err) = result {
        if err.is_panic() {
            std::panic::resume_unwind(err.into_panic());
        }
    }
}

/// Connects to databases, runs migrations, and returns a fully configured AppState and Swarm.
pub async fn prepare_node_state(
    global_db_url: &str,
    regional_db_url: &str,
    region_name: &str,
) -> Result<(anvil::AppState, libp2p::Swarm<anvil::cluster::ClusterBehaviour>)> {
    let global_pool = create_pool(global_db_url)?;
    let regional_pool = create_pool(regional_db_url)?;

    // Run migrations
    let mut global_client = global_pool.get().await?;
    migrations::migrations::runner()
        .set_migration_table_name("refinery_schema_history_global")
        .run_async(&mut **global_client)
        .await?;

    let mut regional_client = regional_pool.get().await?;
    regional_migrations::migrations::runner()
        .set_migration_table_name(&format!("refinery_schema_history_{}", region_name.to_lowercase()))
        .run_async(&mut **regional_client)
        .await?;

    // Insert global metadata
    global_client
        .execute(
            "INSERT INTO tenants (id, name, api_key) VALUES (1, 'default', 'default-key') ON CONFLICT (id) DO NOTHING",
            &[],
        )
        .await?;
    global_client
        .execute(
            "INSERT INTO regions (name) VALUES ($1) ON CONFLICT (name) DO NOTHING",
            &[&region_name],
        )
        .await?;

    // Create AppState
    let storage = anvil::storage::Storage::new().await?;
    let cluster_state = std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
    let swarm = anvil::cluster::create_swarm().await?;

    let state = anvil::AppState {
        db: anvil::persistence::Persistence::new(global_pool, regional_pool),
        storage,
        cluster: cluster_state.clone(),
        sharder: anvil::sharding::ShardManager::new(),
        placer: anvil::placement::PlacementManager::default(),
        region: region_name.to_string(),
    };

    Ok((state, swarm))
}
