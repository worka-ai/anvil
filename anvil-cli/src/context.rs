use crate::config::{Config, Profile};
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anyhow::{Result, anyhow};

pub struct Context {
    pub profile: Profile,
}

impl Context {
    pub fn new(profile_name: Option<String>, config_path: Option<String>) -> Result<Self> {
        let config: Config = match &config_path {
            Some(path) => confy::load_path(path)?,
            None => confy::load("anvil-cli", None)?,
        };

        let profile_name = match profile_name {
            Some(name) => Some(name),
            None => config.default_profile,
        };

        let profile_name = profile_name.ok_or_else(|| {
            anyhow!("No profile specified and no default profile set. Use `anvil-cli configure` to create a profile.")
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

    pub async fn get_bearer_token(&self) -> anyhow::Result<String> {
        if let Ok(token) = std::env::var("ANVIL_AUTH_TOKEN") {
            return Ok(token);
        }

        let mut auth_client = AuthServiceClient::connect(self.profile.host.clone()).await?;
        let token_res = auth_client
            .get_access_token(api::GetAccessTokenRequest {
                client_id: self.profile.client_id.clone(),
                client_secret: self.profile.client_secret.clone(),
                scopes: vec![],
            })
            .await?
            .into_inner();
        Ok(token_res.access_token)
    }
}
