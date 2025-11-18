mod cli;
mod config;
mod context;

use crate::context::Context;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    #[clap(long, global = true)]
    profile: Option<String>,
    #[clap(long, global = true)]
    config: Option<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// Configure CLI profiles
    Configure {
        #[clap(long)]
        name: Option<String>,
        #[clap(long)]
        host: Option<String>,
        #[clap(long)]
        client_id: Option<String>,
        #[clap(long)]
        client_secret: Option<String>,
        #[clap(long)]
        default: bool,
    },
    /// Create a configuration file non-interactively
    StaticConfig {
        #[clap(long)]
        name: String,
        #[clap(long)]
        host: String,
        #[clap(long)]
        client_id: String,
        #[clap(long)]
        client_secret: String,
        #[clap(long)]
        default: bool,
    },
    /// Manage buckets
    Bucket {
        #[clap(subcommand)]
        command: cli::bucket::BucketCommands,
    },
    /// Manage objects
    Object {
        #[clap(subcommand)]
        command: cli::object::ObjectCommands,
    },
    /// Manage authentication and permissions
    Auth {
        #[clap(subcommand)]
        command: cli::auth::AuthCommands,
    },
    /// Hugging Face integration
    Hf {
        #[clap(subcommand)]
        command: cli::hf::HfCommands,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    eprintln!("[anvil-cli] starting v{}", env!("CARGO_PKG_VERSION"));
    eprintln!(
        "[anvil-cli] args: {:?}",
        std::env::args().collect::<Vec<_>>()
    );
    let cli = Cli::parse();

    if let Commands::Configure {
        name,
        host,
        client_id,
        client_secret,
        default,
    } = &cli.command
    {
        cli::configure::handle_configure_command(
            name.clone(),
            host.clone(),
            client_id.clone(),
            client_secret.clone(),
            *default,
            cli.config,
        )?;
        return Ok(());
    }
    if let Commands::StaticConfig {
        name,
        host,
        client_id,
        client_secret,
        default,
    } = &cli.command
    {
        cli::configure::handle_static_config_command(
            name.clone(),
            host.clone(),
            client_id.clone(),
            client_secret.clone(),
            *default,
            cli.config,
        )?;
        return Ok(());
    }

    let ctx = Context::new(cli.profile, cli.config)?;

    match &cli.command {
        Commands::Configure { .. } => { /* handled above */ }
        Commands::StaticConfig { .. } => { /* handled above */ }
        Commands::Bucket { command } => {
            cli::bucket::handle_bucket_command(command, &ctx).await?;
        }
        Commands::Object { command } => {
            cli::object::handle_object_command(command, &ctx).await?;
        }
        Commands::Auth { command } => {
            cli::auth::handle_auth_command(command, &ctx).await?;
        }
        Commands::Hf { command } => {
            cli::hf::handle_hf_command(command, &ctx).await?;
        }
    }

    Ok(())
}
