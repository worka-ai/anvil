use anvil::crypto;
use anvil::persistence::Persistence;
use anvil::{create_pool, migrations, run_migrations};
use anvil_core::permissions::AnvilAction;
use clap::{Parser, Subcommand};
use tracing::info;

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
    pub anvil_secret_encryption_key: String,
}

#[derive(Subcommand)]
enum Commands {
    /// Manage tenants
    Tenant {
        #[clap(subcommand)]
        command: TenantCommands,
    },
    /// Manage apps
    App {
        #[clap(subcommand)]
        command: AppCommands,
    },
    /// Manage policies
    Policy {
        #[clap(subcommand)]
        command: PolicyCommands,
    },
    /// Manage regions
    Region {
        #[clap(subcommand)]
        command: RegionCommands,
    },
    /// Manage buckets
    Bucket {
        #[clap(subcommand)]
        command: BucketCommands,
    },
    /// Manage admin users
    User {
        #[clap(subcommand)]
        command: UserCommands,
    },
}

#[derive(Subcommand)]
enum TenantCommands {
    /// Create a new tenant
    Create { name: String },
}

#[derive(Subcommand)]
enum UserCommands {
    /// Create a new admin user
    Create {
        #[clap(long)]
        username: String,
        #[clap(long)]
        email: String,
        #[clap(long)]
        password: String,
        #[clap(long)]
        role: String,
    },
}

#[derive(Subcommand)]
enum BucketCommands {
    /// Set the public access status for a bucket
    SetPublicAccess {
        #[clap(long)]
        bucket: String,
        #[clap(long)]
        allow: bool,
    },
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
    /// Reset the client secret for an existing app
    ResetSecret {
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
        action: AnvilAction,
        #[clap(long)]
        resource: String,
    },
}

#[derive(Subcommand)]
enum RegionCommands {
    /// Create a region (idempotent)
    Create { name: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let shared_config = cli.config;

    let mut config = anvil_core::config::Config::default();
    config.global_database_url = shared_config.global_database_url;
    config.anvil_secret_encryption_key = shared_config.anvil_secret_encryption_key;
    // Set a dummy region and public_api_addr, as admin CLI doesn't use them,
    // but Persistence::new needs a full Config.
    config.region = "admin-cli-region".to_string();
    config.public_api_addr = "127.0.0.1:0".to_string();

    let global_pool = create_pool(&config.global_database_url)?;
    // The admin tool only interacts with the global DB, so we can use it as a placeholder for the regional pool.
    let regional_pool = create_pool(&config.global_database_url)?;

    run_migrations(
        &config.global_database_url,
        migrations::migrations::runner(),
        "refinery_schema_history_global",
    )
    .await?;

    let persistence = Persistence::new(global_pool, regional_pool, None, &config);
    let encryption_key = hex::decode(config.anvil_secret_encryption_key)?;

    match &cli.command {
        Commands::Tenant { command } => match command {
            TenantCommands::Create { name } => {
                let tenant = persistence.create_tenant(name, "admin-created-key").await?;
                info!("Created tenant: {} (ID: {})", tenant.name, tenant.id);
            }
        },
        Commands::App { command } => match command {
            AppCommands::Create {
                tenant_name,
                app_name,
            } => {
                println!("Creating app for tenant: {}", tenant_name);
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
                println!("Client ID: {}", client_id);
                println!("Client Secret: {}", client_secret);
            }
            AppCommands::ResetSecret { app_name } => {
                let app = persistence
                    .get_app_by_name(app_name)
                    .await?
                    .expect("App not found");

                let new_client_secret = rand::random::<[u8; 32]>()
                    .map(|b| format!("{:02x}", b))
                    .join("");

                let new_encrypted_secret =
                    crypto::encrypt(new_client_secret.as_bytes(), &encryption_key)?;

                persistence
                    .update_app_secret(app.id, &new_encrypted_secret)
                    .await?;

                println!("Successfully reset secret for app: {}", app.name);
                println!("Client ID: {}", app.client_id);
                println!("Client Secret: {}", new_client_secret);
            }
        },
        Commands::Policy { command } => match command {
            PolicyCommands::Grant {
                app_name,
                action,
                resource,
            } => {
                let app = persistence
                    .get_app_by_name(app_name)
                    .await?
                    .expect("App not found");
                persistence
                    .grant_policy(app.id, resource, &action.to_string())
                    .await?;
                info!(
                    "Granted action '{}' on resource '{}' to app '{}'",
                    action, resource, app_name
                );
            }
        },
        Commands::Region { command } => match command {
            RegionCommands::Create { name } => {
                let created = persistence.create_region(name).await?;
                if created {
                    info!("Created region: {}", name);
                } else {
                    info!("Region already exists: {}", name);
                }
            }
        },
        Commands::Bucket { command } => match command {
            BucketCommands::SetPublicAccess { bucket, allow } => {
                persistence.set_bucket_public_access(bucket, *allow).await?;
                info!(
                    "Set public read access for bucket '{}' to {}",
                    bucket, allow
                );
            }
        },
        Commands::User { command } => match command {
            UserCommands::Create {
                username,
                email,
                password,
                role,
            } => {
                let hashed_password = bcrypt::hash(password, bcrypt::DEFAULT_COST)?;
                persistence
                    .create_admin_user(username, email, &hashed_password, role)
                    .await?;
                info!("Created admin user: {}", username);
            }
        },
    }
    Ok(())
}
