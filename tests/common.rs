use anyhow::Result;
use deadpool_postgres::{ManagerConfig, Pool, RecyclingMethod};
use std::env;
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

pub async fn run_migrations(pool: &Pool) -> Result<()> {
    let mut client = pool.get().await?;
    let client_ref = &mut **client;

    // Manually wipe the schema for a clean test run
    client_ref
        .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        .await?;

    // Apply migrations
    migrations::migrations::runner()
        .set_migration_table_name("refinery_schema_history_global")
        .run_async(client_ref)
        .await?;

    regional_migrations::migrations::runner()
        .set_migration_table_name("refinery_schema_history_regional")
        .run_async(client_ref)
        .await?;

    Ok(())
}
