use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum BucketCommands {
    /// Create a bucket for a tenant
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        region: String,
    },
    /// Manage public access
    PublicAccess {
        #[clap(subcommand)]
        command: BucketPublicAccessCommands,
    },
}
#[derive(Subcommand)]
pub enum BucketPublicAccessCommands {
    /// Set public read access on a bucket
    Set {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long, action = clap::ArgAction::Set, value_parser = clap::builder::BoolishValueParser::new())]
        allow: bool,
    },
}

pub(super) async fn handle_bucket_command(
    command: &BucketCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        BucketCommands::Create {
            context,
            tenant_id,
            bucket_name,
            region,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "bucket",
                Some(&admin_context),
                None,
                client.create_bucket_admin(with_auth(
                    api::CreateBucketAdminRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        region: region.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        BucketCommands::PublicAccess {
            command:
                BucketPublicAccessCommands::Set {
                    context,
                    tenant_id,
                    bucket_name,
                    allow,
                },
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "bucket",
                Some(&admin_context),
                None,
                client.set_bucket_public_access_admin(with_auth(
                    api::SetBucketPublicAccessAdminRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        allow_public_read: *allow,
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}
