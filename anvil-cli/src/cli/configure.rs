use crate::config::{Config, Profile};
use dialoguer::{Confirm, Input};

pub fn handle_configure_command() -> anyhow::Result<()> {
    let mut config: Config = confy::load("anvil-cli", None)?;

    let profile_name: String = Input::new()
        .with_prompt("Profile name")
        .interact_text()?;

    let host: String = Input::new()
        .with_prompt("Anvil host (e.g., http://127.0.0.1:50051)")
        .default("http://127.0.0.1:50051".into())
        .interact_text()?;

    let client_id: String = Input::new().with_prompt("Client ID").interact_text()?;
    let client_secret: String = Input::new()
        .with_prompt("Client Secret")
        .interact_text()?;

    let profile = Profile {
        name: profile_name.clone(),
        host,
        client_id,
        client_secret,
    };

    config.profiles.insert(profile_name.clone(), profile);

    let set_as_default = Confirm::new()
        .with_prompt("Set as default profile?")
        .default(true)
        .interact()?;

    if set_as_default {
        config.default_profile = Some(profile_name.clone());
    }

    confy::store("anvil-cli", None, config)?;

    println!("Profile '{}' saved.", profile_name);

    Ok(())
}
