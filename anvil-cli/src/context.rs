use crate::config::{Config, Profile};
use anyhow::{anyhow, Result};
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;

pub struct Context {
    pub profile: Profile,
}

impl Context {
    pub fn new(profile_name: Option<String>) -> Result<Self> {
        let config: Config = confy::load("anvil-cli", None)?;

        let profile_name = match profile_name {
            Some(name) => Some(name),
            None => config.default_profile,
        };

        let profile_name = profile_name.ok_or_else(|| {
            anyhow!("No profile specified and no default profile set. Use `anvil-cli configure` to create a profile.")
        })?;

        let profile = config
            .profiles
            .get(&profile_name)
            .ok_or_else(|| anyhow!("Profile '{}' not found.", profile_name))?
            .clone();

        Ok(Self { profile })
    }

    pub async fn get_bearer_token(&self) -> anyhow::Result<String> {
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
