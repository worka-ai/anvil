use crate::config::{Config, Profile};
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anyhow::{Result, anyhow};
use serde::Deserialize;

pub struct Context {
    pub profile: Profile,
}

impl Context {
    pub fn new(profile_name: Option<String>, config_path: Option<String>) -> Result<Self> {
        let config: Config = match &config_path {
            Some(path) => confy::load_path(path)?,
            None => confy::load("anvil", None)?,
        };

        let profile_name = match profile_name {
            Some(name) => Some(name),
            None => config.default_profile,
        };

        let profile_name = profile_name.ok_or_else(|| {
            anyhow!("No profile specified and no default profile set. Use `anvil configure` to create a profile.")
        })?;

        let mut profile = config
            .profiles
            .get(&profile_name)
            .ok_or_else(|| anyhow!("Profile '{}' not found.", profile_name))?
            .clone();

        // Normalize host to include scheme if missing for tonic URIs
        if !(profile.host.starts_with("http://") || profile.host.starts_with("https://")) {
            profile.host = format!("http://{}", profile.host);
        }

        Ok(Self { profile })
    }

    #[allow(dead_code)]
    pub fn from_host(host: String) -> Self {
        let mut host = host;
        if !(host.starts_with("http://") || host.starts_with("https://")) {
            host = format!("http://{}", host);
        }
        Self {
            profile: Profile {
                name: "inline".to_string(),
                host,
                client_id: String::new(),
                client_secret: String::new(),
            },
        }
    }

    #[allow(dead_code)]
    pub fn admin(
        profile_name: Option<String>,
        config_path: Option<String>,
        host: Option<String>,
    ) -> Result<Self> {
        let host = host.or_else(|| std::env::var("ANVIL_ADMIN_ENDPOINT").ok()).ok_or_else(|| {
            anyhow!("anvil-admin requires --host or ANVIL_ADMIN_ENDPOINT for the private admin listener")
        })?;

        let mut ctx = match Self::new(profile_name, config_path) {
            Ok(ctx) => ctx,
            Err(_) => Self::from_host(host.clone()),
        };
        ctx.profile.host = normalize_host(host);
        Ok(ctx)
    }

    pub async fn get_bearer_token(&self) -> anyhow::Result<String> {
        if let Ok(token) = std::env::var("ANVIL_AUTH_TOKEN") {
            return Ok(token);
        }

        let (client_id, client_secret) = match bootstrap_credential_from_env()? {
            Some(credential) => (credential.client_id, credential.client_secret),
            None => (
                self.profile.client_id.clone(),
                self.profile.client_secret.clone(),
            ),
        };
        let auth_host =
            std::env::var("ANVIL_PUBLIC_ENDPOINT").unwrap_or_else(|_| self.profile.host.clone());
        let mut auth_client = AuthServiceClient::connect(normalize_host(auth_host)).await?;
        let token_res = auth_client
            .get_access_token(api::GetAccessTokenRequest {
                client_id,
                client_secret,
                scopes: vec![],
            })
            .await?
            .into_inner();
        Ok(token_res.access_token)
    }
}

#[derive(Deserialize)]
struct BootstrapCredential {
    client_id: String,
    client_secret: String,
}

fn bootstrap_credential_from_env() -> Result<Option<BootstrapCredential>> {
    let Ok(path) = std::env::var("ANVIL_BOOTSTRAP_CREDENTIAL_FILE") else {
        return Ok(None);
    };
    let raw = std::fs::read_to_string(path)?;
    Ok(Some(serde_json::from_str(&raw)?))
}

fn normalize_host(mut host: String) -> String {
    if !(host.starts_with("http://") || host.starts_with("https://")) {
        host = format!("http://{}", host);
    }
    host
}
