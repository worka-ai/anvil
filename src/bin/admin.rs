use anvil::crypto;
use anvil::persistence::Persistence;
use anvil::{create_pool, migrations, run_migrations};
use clap::{Parser, Subcommand};

// Import the shared config


#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,

    #[clap(flatten)]
    config: SharedConfig,
}

#[derive(Parser, Debug)]
struct SharedConfig {
    #[arg(long, env)]
    pub global_database_url: String,

    #[arg(long, env)]
    pub worka_secret_encryption_key: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage tenants
    Tenants {
        #[clap(subcommand)]
        command: TenantCommands,
    },
    /// Manage apps
    Apps {
        #[clap(subcommand)]
        command: AppCommands,
    },
    /// Manage policies
    Policies {
        #[clap(subcommand)]
        command: PolicyCommands,
    },
}

#[derive(Subcommand)]
enum TenantCommands {
    /// Create a new tenant
    Create { name: String },
}

#[derive(Subcommand)]
enum AppCommands {
    /// Create a new app for a tenant
    Create {
        #[clap(long)]
        tenant_name: String,
        #[clap(long)]
        app_name: String,
    },
}

#[derive(Subcommand)]
enum PolicyCommands {
    /// Grant a permission to an app
    Grant {
        #[clap(long)]
        app_name: String,
        #[clap(long)]
        action: String,
        #[clap(long)]
        resource: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = cli.config;

    let global_pool = create_pool(&config.global_database_url)?;
    // The admin tool only interacts with the global DB, so we can use it as a placeholder for the regional pool.
    let regional_pool = create_pool(&config.global_database_url)?;

    run_migrations(
        &config.global_database_url,
        migrations::migrations::runner(),
        "refinery_schema_history_global",
    )
    .await?;

    let persistence = Persistence::new(global_pool, regional_pool);
    let encryption_key = hex::decode(config.worka_secret_encryption_key)?;

    match &cli.command {
        Commands::Tenants { command } => match command {
            TenantCommands::Create { name } => {
                let tenant = persistence.create_tenant(name, "admin-created-key").await?;
                println!("Created tenant: {} (ID: {})", tenant.name, tenant.id);
            }
        },
        Commands::Apps { command } => match command {
            AppCommands::Create {
                tenant_name,
                app_name,
            } => {
                let tenant = persistence
                    .get_tenant_by_name(tenant_name)
                    .await?
                    .expect("Tenant not found");
                let client_id = format!(
                    "app_{}",
                    rand::random::<[u8; 16]>()
                        .map(|b| format!("{:02x}", b))
                        .join("")
                );
                let client_secret = rand::random::<[u8; 32]>()
                    .map(|b| format!("{:02x}", b))
                    .join("");

                let encrypted_secret = crypto::encrypt(client_secret.as_bytes(), &encryption_key)?;

                let app = persistence
                    .create_app(tenant.id, app_name, &client_id, &encrypted_secret)
                    .await?;
                println!("Created app: {} (ID: {})", app.name, app.id);
                println!("  Client ID: {}", client_id);
                println!("  Client Secret: {}", client_secret);
            }
        },
        Commands::Policies { command } => match command {
            PolicyCommands::Grant {
                app_name,
                action,
                resource,
            } => {
                let app = persistence
                    .get_app_by_name(app_name)
                    .await?
                    .expect("App not found");
                persistence.grant_policy(app.id, resource, action).await?;
                println!(
                    "Granted action '{}' on resource '{}' to app '{}'",
                    action, resource, app_name
                );
            }
        },
    }

    Ok(())
}