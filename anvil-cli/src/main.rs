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
    /// Manage relationship authorisation tuples and schemas
    Authz {
        #[clap(subcommand)]
        command: cli::authz::AuthzCommands,
    },
    /// Query tenant audit events
    Audit {
        #[clap(subcommand)]
        command: cli::audit::AuditCommands,
    },
    /// Manage application credentials in the authenticated tenant
    App {
        #[clap(subcommand)]
        command: cli::app::AppCommands,
    },
    /// Manage and query indexes
    Index {
        #[clap(subcommand)]
        command: cli::index::IndexCommands,
    },
    /// Watch Anvil change streams
    Watch {
        #[clap(subcommand)]
        command: cli::watch::WatchCommands,
    },
    /// Manage PersonalDB groups, projections and changesets
    Personaldb {
        #[clap(subcommand)]
        command: cli::personaldb::PersonalDbCommands,
    },
    /// Manage append streams
    Stream {
        #[clap(subcommand)]
        command: cli::stream::StreamCommands,
    },
    /// Manage coordination leases
    Lease {
        #[clap(subcommand)]
        command: cli::lease::LeaseCommands,
    },
    /// Query tenant-scoped diagnostics
    Diagnostics {
        #[clap(subcommand)]
        command: cli::diagnostics::DiagnosticsCommands,
    },
    /// Run tenant-scoped repairs
    Repair {
        #[clap(subcommand)]
        command: cli::repair::RepairCommands,
    },
    /// Manage tenant-owned host aliases
    HostAlias {
        #[clap(subcommand)]
        command: cli::host_alias::HostAliasCommands,
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
        Commands::Authz { command } => {
            cli::authz::handle_authz_command(command, &ctx).await?;
        }
        Commands::Audit { command } => {
            cli::audit::handle_audit_command(command, &ctx).await?;
        }
        Commands::App { command } => {
            cli::app::handle_app_command(command, &ctx).await?;
        }
        Commands::Index { command } => {
            cli::index::handle_index_command(command, &ctx).await?;
        }
        Commands::Watch { command } => {
            cli::watch::handle_watch_command(command, &ctx).await?;
        }
        Commands::Personaldb { command } => {
            cli::personaldb::handle_personaldb_command(command, &ctx).await?;
        }
        Commands::Stream { command } => {
            cli::stream::handle_stream_command(command, &ctx).await?;
        }
        Commands::Lease { command } => {
            cli::lease::handle_lease_command(command, &ctx).await?;
        }
        Commands::Diagnostics { command } => {
            cli::diagnostics::handle_diagnostics_command(command, &ctx).await?;
        }
        Commands::Repair { command } => {
            cli::repair::handle_repair_command(command, &ctx).await?;
        }
        Commands::HostAlias { command } => {
            cli::host_alias::handle_host_alias_command(command, &ctx).await?;
        }
        Commands::Hf { command } => {
            cli::hf::handle_hf_command(command, &ctx).await?;
        }
    }

    Ok(())
}
