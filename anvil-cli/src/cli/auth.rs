use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use tonic::transport::Endpoint;
use tokio::time::{timeout, Duration};
use clap::Subcommand;

#[derive(Subcommand)]
pub enum AuthCommands {
    /// Get a new access token
    GetToken {
        #[clap(long)]
        client_id: Option<String>,
        #[clap(long)]
        client_secret: Option<String>,
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
    let endpoint = Endpoint::from_shared(ctx.profile.host.clone())?
        .connect_timeout(Duration::from_secs(5))
        .tcp_nodelay(true);
    let channel = endpoint.connect().await?;
    let mut client = AuthServiceClient::new(channel);

    match command {
        AuthCommands::GetToken { client_id, client_secret } => {
            let (id, secret) = match (client_id.as_ref(), client_secret.as_ref()) {
                (Some(id), Some(secret)) => (id.clone(), secret.clone()),
                _ => (ctx.profile.client_id.clone(), ctx.profile.client_secret.clone()),
            };

            let host = ctx.profile.host.clone();
            eprintln!("[anvil-cli] get-token: sending RPC to {}", host);

            // Build channel on current runtime and perform unary call with a timeout
            let endpoint = Endpoint::from_shared(host)?
                .connect_timeout(Duration::from_secs(5))
                .tcp_nodelay(true);
            let channel = endpoint.connect().await?;
            let mut c = AuthServiceClient::new(channel);
            let resp = timeout(
                Duration::from_secs(5),
                c.get_access_token(api::GetAccessTokenRequest {
                    client_id: id,
                    client_secret: secret,
                    scopes: vec![],
                }),
            )
            .await
            .map_err(|_| anyhow::anyhow!("get-token request timed out"))??;
            let token = resp.into_inner().access_token;
            // Explicitly drop client before printing/exiting to tear down h2 cleanly
            drop(c);
            eprintln!("[anvil-cli] get-token: RPC completed, printing token");
            println!("{}", token);
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
