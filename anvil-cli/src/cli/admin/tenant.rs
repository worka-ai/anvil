use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum TenantCommands {
    /// Create a tenant
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        name: String,
        #[clap(long, default_value = "")]
        home_region: String,
    },
}

pub(super) async fn handle_tenant_command(
    command: &TenantCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        TenantCommands::Create {
            context,
            name,
            home_region,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "tenant",
                Some(&admin_context),
                None,
                client.create_tenant(with_auth(
                    api::CreateTenantRequest {
                        context: Some(admin_context.clone()),
                        name: name.clone(),
                        home_region: home_region.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}
