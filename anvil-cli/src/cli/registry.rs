use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::registry_service_client::RegistryServiceClient;
use anyhow::Result;
use clap::Subcommand;
use tokio::fs;
use tonic::Request;

#[derive(Subcommand)]
pub enum RegistryCommands {
    /// Store a content-addressed blob for a package registry namespace.
    PutBlob {
        registry_kind: String,
        namespace: String,
        digest: String,
        file: String,
        #[clap(long, default_value = "application/octet-stream")]
        media_type: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Publish a package version manifest.
    PutVersion {
        registry_kind: String,
        namespace: String,
        package_name: String,
        version: String,
        manifest_json: String,
        #[clap(long = "blob-digest")]
        blob_digests: Vec<String>,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Move or create a registry reference such as latest or a dist-tag.
    PutRef {
        registry_kind: String,
        namespace: String,
        package_name: String,
        ref_name: String,
        target_version: String,
        #[clap(long)]
        transaction_id: Option<String>,
    },
    /// Read a published package version.
    GetVersion {
        registry_kind: String,
        namespace: String,
        package_name: String,
        version: String,
    },
    /// List package versions and references.
    ListVersions {
        registry_kind: String,
        namespace: String,
        package_name: String,
        #[clap(long, default_value_t = 100)]
        limit: u32,
        #[clap(long, default_value = "")]
        page_token: String,
    },
}

pub async fn handle_registry_command(command: &RegistryCommands, ctx: &Context) -> Result<()> {
    let mut client = RegistryServiceClient::connect(ctx.profile.host.clone()).await?;
    match command {
        RegistryCommands::PutBlob {
            registry_kind,
            namespace,
            digest,
            file,
            media_type,
            transaction_id,
        } => {
            let body = fs::read(file).await?;
            let token = ctx.get_bearer_token().await?;
            let mut request = Request::new(api::PutPackageBlobRequest {
                registry_kind: registry_kind.clone(),
                namespace: namespace.clone(),
                digest: digest.clone(),
                inline_body: body,
                media_type: media_type.clone(),
                options: Some(write_options(transaction_id.clone())),
            });
            attach_auth(&mut request, &token)?;
            let response = client.put_package_blob(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        RegistryCommands::PutVersion {
            registry_kind,
            namespace,
            package_name,
            version,
            manifest_json,
            blob_digests,
            transaction_id,
        } => {
            let manifest_json = fs::read_to_string(manifest_json).await?;
            let token = ctx.get_bearer_token().await?;
            let mut request = Request::new(api::PutPackageVersionRequest {
                registry_kind: registry_kind.clone(),
                namespace: namespace.clone(),
                package_name: package_name.clone(),
                version: version.clone(),
                manifest_json,
                blob_digests: blob_digests.clone(),
                options: Some(write_options(transaction_id.clone())),
            });
            attach_auth(&mut request, &token)?;
            let response = client.put_package_version(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        RegistryCommands::PutRef {
            registry_kind,
            namespace,
            package_name,
            ref_name,
            target_version,
            transaction_id,
        } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = Request::new(api::PutRegistryRefRequest {
                registry_kind: registry_kind.clone(),
                namespace: namespace.clone(),
                package_name: package_name.clone(),
                ref_name: ref_name.clone(),
                target_version: target_version.clone(),
                options: Some(write_options(transaction_id.clone())),
            });
            attach_auth(&mut request, &token)?;
            let response = client.put_registry_ref(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        RegistryCommands::GetVersion {
            registry_kind,
            namespace,
            package_name,
            version,
        } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = Request::new(api::GetPackageVersionRequest {
                registry_kind: registry_kind.clone(),
                namespace: namespace.clone(),
                package_name: package_name.clone(),
                version: version.clone(),
                consistency: Some(api::ReadConsistency {
                    mode: Some(api::read_consistency::Mode::Latest(true)),
                }),
            });
            attach_auth(&mut request, &token)?;
            let response = client.get_package_version(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
        RegistryCommands::ListVersions {
            registry_kind,
            namespace,
            package_name,
            limit,
            page_token,
        } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = Request::new(api::ListPackageVersionsRequest {
                registry_kind: registry_kind.clone(),
                namespace: namespace.clone(),
                package_name: package_name.clone(),
                consistency: Some(api::ReadConsistency {
                    mode: Some(api::read_consistency::Mode::Latest(true)),
                }),
                limit: *limit,
                page_token: page_token.clone(),
            });
            attach_auth(&mut request, &token)?;
            let response = client.list_package_versions(request).await?.into_inner();
            println!("{}", serde_json::to_string_pretty(&response)?);
        }
    }
    Ok(())
}

fn write_options(transaction_id: Option<String>) -> api::WriteOptions {
    api::WriteOptions {
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        consistency: if transaction_id.is_some() {
            api::ConsistencyMode::Committed as i32
        } else {
            api::ConsistencyMode::Finalised as i32
        },
        wait_for_finalization: transaction_id.is_none(),
        preconditions: Vec::new(),
        boundary_values: Vec::new(),
        transaction_id,
    }
}

fn attach_auth<T>(request: &mut Request<T>, token: &str) -> Result<()> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse()?);
    Ok(())
}
