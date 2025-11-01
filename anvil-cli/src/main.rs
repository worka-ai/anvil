use anvil::anvil_api as api;
use anvil::anvil_api::{
    hf_ingestion_service_client::HfIngestionServiceClient,
    hugging_face_key_service_client::HuggingFaceKeyServiceClient,
};
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
    Bucket {
        #[clap(subcommand)]
        command: BucketCommands,
    },
    /// Manage objects
    Object {
        #[clap(subcommand)]
        command: ObjectCommands,
    },
    /// Manage authentication and permissions
    Auth {
        #[clap(subcommand)]
        command: AuthCommands,
    },
    /// Hugging Face integration
    Hf {
        #[clap(subcommand)]
        command: HfCommands,
    },
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
    SetPublic {
        name: String,
        #[clap(long)]
        allow: bool,
    },
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
    Grant {
        app: String,
        action: String,
        resource: String,
    },
    /// Revoke a permission from an app
    Revoke {
        app: String,
        action: String,
        resource: String,
    },
}

#[derive(Subcommand)]
enum HfCommands {
    /// Manage keys
    Key {
        #[clap(subcommand)]
        command: HfKeyCommands,
    },
    /// Manage ingestions
    Ingest {
        #[clap(subcommand)]
        command: HfIngestCommands,
    },
}

#[derive(Subcommand)]
enum HfKeyCommands {
    /// Add a named key
    Add {
        #[clap(long)]
        name: String,
        #[clap(long)]
        token: String,
        #[clap(long)]
        note: Option<String>,
    },
    /// List keys
    Ls,
    /// Remove a key
    Rm {
        #[clap(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum HfIngestCommands {
    /// Start an ingestion
    Start {
        #[clap(long)]
        key: String,
        #[clap(long)]
        repo: String,
        #[clap(long)]
        revision: Option<String>,
        #[clap(long)]
        bucket: String,
        #[clap(long)]
        target_region: String,
        #[clap(long)]
        prefix: Option<String>,
        #[clap(long)]
        include: Vec<String>,
        #[clap(long)]
        exclude: Vec<String>,
    },
    /// Get status
    Status {
        #[clap(long)]
        id: String,
    },
    /// Cancel an ingestion
    Cancel {
        #[clap(long)]
        id: String,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match &cli.command {
        Commands::Configure => println!("Configure command not implemented yet."),
        Commands::Bucket { command } => match command {
            BucketCommands::Create { name } => {
                println!("bucket create not implemented for {}", name)
            }
            _ => println!("This bucket command is not implemented yet."),
        },
        Commands::Object { .. } => println!("Object commands not implemented yet."),
        Commands::Auth { .. } => println!("Auth commands not implemented yet."),
        Commands::Hf { command } => {
            // TODO: pull endpoint from config/profile; default to http://127.0.0.1:50051
            let endpoint = std::env::var("ANVIL_ENDPOINT")
                .unwrap_or_else(|_| "http://127.0.0.1:50051".to_string());
            match command {
                HfCommands::Key { command } => {
                    let mut client: HuggingFaceKeyServiceClient<tonic::transport::Channel> =
                        HuggingFaceKeyServiceClient::connect(endpoint.clone()).await?;
                    match command {
                        HfKeyCommands::Add { name, token, note } => {
                            let resp = client
                                .create_key(api::CreateHfKeyRequest {
                                    name: name.clone(),
                                    token: token.clone(),
                                    note: note.clone().unwrap_or_default(),
                                })
                                .await?;
                            println!("created key: {}", resp.into_inner().name);
                        }
                        HfKeyCommands::Ls => {
                            let resp = client.list_keys(api::ListHfKeysRequest {}).await?;
                            for k in resp.into_inner().keys {
                                println!("{}\t{}", k.name, k.updated_at);
                            }
                        }
                        HfKeyCommands::Rm { name } => {
                            client
                                .delete_key(api::DeleteHfKeyRequest { name: name.clone() })
                                .await?;
                            println!("deleted key: {}", name);
                        }
                    }
                }
                HfCommands::Ingest { command } => {
                    let mut client: HfIngestionServiceClient<tonic::transport::Channel> =
                        HfIngestionServiceClient::connect(endpoint.clone()).await?;
                    match command {
                        HfIngestCommands::Start {
                            key,
                            repo,
                            revision,
                            bucket,
                            target_region,
                            prefix,
                            include,
                            exclude,
                        } => {
                            let resp = client
                                .start_ingestion(api::StartHfIngestionRequest {
                                    key_name: key.clone(),
                                    repo: repo.clone(),
                                    revision: revision.clone().unwrap_or_default(),
                                    target_bucket: bucket.clone(),
                                    target_prefix: prefix.clone().unwrap_or_default(),
                                    include_globs: include.clone(),
                                    exclude_globs: exclude.clone(),
                                    target_region: target_region.clone(),
                                })
                                .await?;
                            println!("ingestion id: {}", resp.into_inner().ingestion_id);
                        }
                        HfIngestCommands::Status { id } => {
                            let resp = client
                                .get_ingestion_status(api::GetHfIngestionStatusRequest {
                                    ingestion_id: id.clone(),
                                })
                                .await?;
                            let s = resp.into_inner();
                            println!(
                                "state={} queued={} downloading={} stored={} failed={} error={}",
                                s.state, s.queued, s.downloading, s.stored, s.failed, s.error
                            );
                        }
                        HfIngestCommands::Cancel { id } => {
                            client
                                .cancel_ingestion(api::CancelHfIngestionRequest {
                                    ingestion_id: id.clone(),
                                })
                                .await?;
                            println!("canceled: {}", id);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
