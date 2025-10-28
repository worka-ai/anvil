use clap::{Parser, Subcommand};

#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Configure CLI profiles
    Configure,
    /// Manage buckets
    Bucket { #[clap(subcommand)] command: BucketCommands },
    /// Manage objects
    Object { #[clap(subcommand)] command: ObjectCommands },
    /// Manage authentication and permissions
    Auth { #[clap(subcommand)] command: AuthCommands },
}

#[derive(Subcommand)]
enum BucketCommands {
    /// Create a new bucket
    Create { name: String },
    /// Remove a bucket
    Rm { name: String },
    /// List buckets
    Ls,
    /// Set public access for a bucket
    SetPublic { name: String, #[clap(long)] allow: bool },
}

#[derive(Subcommand)]
enum ObjectCommands {
    /// Upload a file to an object
    Put { src: String, dest: String },
    /// Download an object to a file or stdout
    Get { src: String, dest: Option<String> },
    /// Remove an object
    Rm { path: String },
    /// List objects in a bucket
    Ls { path: String },
    /// Show object metadata
    Head { path: String },
}

#[derive(Subcommand)]
enum AuthCommands {
    /// Get a new access token
    GetToken,
    /// Grant a permission to another app
    Grant { app: String, action: String, resource: String },
    /// Revoke a permission from an app
    Revoke { app: String, action: String, resource: String },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Configure => println!("Configure command not implemented yet."),
        Commands::Bucket { command } => match command {
            BucketCommands::Create { name } => println!("bucket create not implemented for {}", name),
            _ => println!("This bucket command is not implemented yet."),
        },
        Commands::Object { .. } => println!("Object commands not implemented yet."),
        Commands::Auth { .. } => println!("Auth commands not implemented yet."),
    }

    Ok(())
}
