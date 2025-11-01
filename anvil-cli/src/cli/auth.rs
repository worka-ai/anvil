use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum AuthCommands {
    /// Get a new access token
    GetToken {
        #[clap(long)]
        client_id: String,
        #[clap(long)]
        client_secret: String,
    },
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

pub async fn handle_auth_command(command: &AuthCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = AuthServiceClient::connect(ctx.profile.host.clone()).await?;

    match command {
        AuthCommands::GetToken { client_id, client_secret } => {
            let resp = client
                .get_access_token(api::GetAccessTokenRequest {
                    client_id: client_id.clone(),
                    client_secret: client_secret.clone(),
                    scopes: vec![],
                })
                .await?;
            println!("{}", resp.into_inner().access_token);
        }
        AuthCommands::Grant { app, action, resource } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = tonic::Request::new(api::GrantAccessRequest {
                grantee_app_id: app.clone(),
                action: action.clone(),
                resource: resource.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.grant_access(request).await?;
            println!("Permission granted.");
        }
        AuthCommands::Revoke { app, action, resource } => {
            let token = ctx.get_bearer_token().await?;
            let mut request = tonic::Request::new(api::RevokeAccessRequest {
                grantee_app_id: app.clone(),
                action: action.clone(),
                resource: resource.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.revoke_access(request).await?;
            println!("Permission revoked.");
        }
    }

    Ok(())
}
