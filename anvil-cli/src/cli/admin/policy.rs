use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum PolicyCommands {
    /// Grant an application permission scope
    Grant {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
        #[clap(long)]
        action: String,
        #[clap(long)]
        resource: String,
    },
    /// Revoke an application permission scope
    Revoke {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
        #[clap(long)]
        action: String,
        #[clap(long)]
        resource: String,
    },
}

pub(super) async fn handle_policy_command(
    command: &PolicyCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        PolicyCommands::Grant {
            context,
            tenant_id,
            app_name,
            action,
            resource,
        } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "application_policy",
                Some(&admin_context),
                None,
                client.grant_application_policy(with_auth(
                    api::GrantApplicationPolicyRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                        action: action.clone(),
                        resource: resource.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        PolicyCommands::Revoke {
            context,
            tenant_id,
            app_name,
            action,
            resource,
        } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "application_policy",
                Some(&admin_context),
                None,
                client.revoke_application_policy(with_auth(
                    api::RevokeApplicationPolicyRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                        action: action.clone(),
                        resource: resource.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}
