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
}

#[derive(Subcommand)]
enum Commands {
    /// Configure CLI profiles
    Configure,
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
    let cli = Cli::parse();

    if let Commands::Configure = &cli.command {
        cli::configure::handle_configure_command()?;
        return Ok(());
    }

    let ctx = Context::new(cli.profile)?;

    match &cli.command {
        Commands::Configure => { /* handled above */ }
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
