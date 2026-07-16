use super::common::{
    AdminClient, MutationOptions, PageOptions, print_rpc_response, request_id_or_cli, with_auth,
};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum HostAliasCommands {
    /// Create a custom host alias in pending verification state
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        region: String,
        #[clap(long, default_value = "")]
        prefix: String,
    },
    /// Activate a verified host alias
    Activate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
    },
    /// Suspend an active host alias
    Suspend {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
    },
    /// Delete a host alias descriptor
    Delete {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
    },
    /// Read host alias metadata
    Read {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long)]
        hostname: String,
    },
    /// List host aliases
    List {
        #[clap(long)]
        region: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

pub(super) async fn handle_host_alias_command(
    command: &HostAliasCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        HostAliasCommands::Create {
            context,
            hostname,
            tenant_id,
            bucket_name,
            region,
            prefix,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.create_host_alias(with_auth(
                    api::CreateHostAliasAdminRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        region: region.clone(),
                        prefix: prefix.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Activate { context, hostname } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.activate_host_alias(with_auth(
                    api::ActivateHostAliasRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Suspend { context, hostname } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.suspend_host_alias(with_auth(
                    api::SuspendHostAliasRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Delete { context, hostname } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.delete_host_alias(with_auth(
                    api::DeleteHostAliasAdminRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Read {
            request_id,
            hostname,
        } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "host_alias",
                None,
                Some(&request_id),
                client.read_host_alias(with_auth(
                    api::ReadHostAliasRequest {
                        request_id: request_id.clone(),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::List { region, page } => {
            print_rpc_response(
                "host_aliases",
                None,
                None,
                client.list_host_aliases(with_auth(
                    api::ListHostAliasesRequest {
                        region: region.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }

    Ok(())
}
