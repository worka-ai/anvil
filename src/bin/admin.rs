use anvil::persistence::Persistence;
use anvil::{create_pool, run_migrations, migrations};
use clap::{Parser, Subcommand};
use rand::Rng;

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage tenants
    Tenants { #[clap(subcommand)] command: TenantCommands },
    /// Manage apps
    Apps { #[clap(subcommand)] command: AppCommands },
    /// Manage policies
    Policies { #[clap(subcommand)] command: PolicyCommands },
}

#[derive(Subcommand)]
enum TenantCommands {
    /// Create a new tenant
    Create { name: String },
}

#[derive(Subcommand)]
enum AppCommands {
    /// Create a new app for a tenant
    Create { #[clap(long)] tenant_name: String, #[clap(long)] app_name: String },
}

#[derive(Subcommand)]
enum PolicyCommands {
    /// Grant a permission to an app
    Grant {
        #[clap(long)] app_name: String,
        #[clap(long)] action: String,
        #[clap(long)] resource: String,
    },
}

use argon2::{
    password_hash::{
        rand_core::OsRng,
        PasswordHasher, SaltString
    },
    Argon2
};

fn hash_secret(secret: &str) -> String {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    argon2.hash_password(secret.as_bytes(), &salt).unwrap().to_string()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    let global_db_url = std::env::var("GLOBAL_DATABASE_URL")?;
    let global_pool = create_pool(&global_db_url)?;
    let regional_pool = create_pool(&global_db_url)?; // Placeholder

    run_migrations(&global_db_url, migrations::migrations::runner(), "refinery_schema_history_global").await?;

    let persistence = Persistence::new(global_pool, regional_pool);

    match &cli.command {
        Commands::Tenants { command } => match command {
            TenantCommands::Create { name } => {
                let tenant = persistence.create_tenant(name, "admin-created-key").await?;
                println!("Created tenant: {} (ID: {})", tenant.name, tenant.id);
            }
        },
        Commands::Apps { command } => match command {
            AppCommands::Create { tenant_name, app_name } => {
                let tenant = persistence.get_tenant_by_name(tenant_name).await?.expect("Tenant not found");
                let client_id = format!("app_{}", rand::thread_rng().r#gen::<[u8; 16]>().map(|b| format!("{:02x}", b)).join(""));
                let client_secret = rand::thread_rng().r#gen::<[u8; 32]>().map(|b| format!("{:02x}", b)).join("");
                let secret_hash = hash_secret(&client_secret);

                let app = persistence.create_app(tenant.id, app_name, &client_id, &secret_hash).await?;
                println!("Created app: {} (ID: {})", app.name, app.id);
                println!("  Client ID: {}", client_id);
                println!("  Client Secret: {}", client_secret);
            }
        },
        Commands::Policies { command } => match command {
            PolicyCommands::Grant { app_name, action, resource } => {
                let app = persistence.get_app_by_name(app_name).await?.expect("App not found");
                persistence.grant_policy(app.id, resource, action).await?;
                println!("Granted action '{}' on resource '{}' to app '{}'", action, resource, app_name);
            }
        },
    }

    Ok(())
}
