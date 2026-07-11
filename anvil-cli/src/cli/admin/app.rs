use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum AppCommands {
    /// Create an application credential
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
    },
    /// Rotate an application secret
    RotateSecret {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
    },
}

pub(super) async fn handle_app_command(
    command: &AppCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        AppCommands::Create {
            context,
            tenant_id,
            app_name,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "application",
                Some(&admin_context),
                None,
                client.create_application(with_auth(
                    api::CreateApplicationRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        AppCommands::RotateSecret {
            context,
            tenant_id,
            app_name,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "application",
                Some(&admin_context),
                None,
                client.rotate_application_secret(with_auth(
                    api::RotateApplicationSecretRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}
