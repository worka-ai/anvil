use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum SecretEncryptionKeyCommands {
    /// Re-encrypt existing server-side secret envelopes with the active configured key
    Rotate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        dry_run: bool,
    },
}

pub(super) async fn handle_secret_encryption_key_command(
    command: &SecretEncryptionKeyCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        SecretEncryptionKeyCommands::Rotate { context, dry_run } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "secret_encryption_key_rotation",
                Some(&admin_context),
                None,
                client.rotate_secret_encryption_key(with_auth(
                    api::RotateSecretEncryptionKeyRequest {
                        context: Some(admin_context.clone()),
                        dry_run: *dry_run,
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}
