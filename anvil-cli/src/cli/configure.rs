use crate::config::{Config, Profile};
use dialoguer::{Confirm, Input};

pub fn handle_configure_command(
    name: Option<String>,
    host: Option<String>,
    client_id: Option<String>,
    client_secret: Option<String>,
    default: bool,
    config_path: Option<String>,
) -> anyhow::Result<()> {
    let mut config: Config = match &config_path {
        Some(path) => confy::load_path(path).unwrap_or_default(),
        None => confy::load("anvil-cli", None)?,
    };

    let profile_name = match name {
        Some(n) => n,
        None => Input::new().with_prompt("Profile name").interact_text()?,
    };

    let host = match host {
        Some(h) => h,
        None => Input::new()
            .with_prompt("Anvil host (e.g., http://127.0.0.1:50051)")
            .default("http://127.0.0.1:50051".into())
            .interact_text()?,
    };

    let client_id = match client_id {
        Some(c) => c,
        None => Input::new().with_prompt("Client ID").interact_text()?,
    };

    let client_secret = match client_secret {
        Some(s) => s,
        None => Input::new().with_prompt("Client Secret").interact_text()?,
    };

    let profile = Profile {
        name: profile_name.clone(),
        host,
        client_id,
        client_secret,
    };

    config.profiles.insert(profile_name.clone(), profile);

    let set_as_default = if default {
        true
    } else {
        Confirm::new()
            .with_prompt("Set as default profile?")
            .default(true)
            .interact()?
    };

    if set_as_default {
        config.default_profile = Some(profile_name.clone());
    }

    match &config_path {
        Some(path) => confy::store_path(path, &config)?,
        None => confy::store("anvil-cli", None, &config)?,
    };

    println!("Profile '{}' saved.", profile_name);

    Ok(())
}

pub fn handle_static_config_command(
    name: String,
    host: String,
    client_id: String,
    client_secret: String,
    default: bool,
    config_path: Option<String>,
) -> anyhow::Result<()> {
    let mut config: Config = match &config_path {
        Some(path) => confy::load_path(path).unwrap_or_default(),
        None => confy::load("anvil-cli", None)?,
    };

    let profile = Profile {
        name: name.clone(),
        host,
        client_id,
        client_secret,
    };

    config.profiles.insert(name.clone(), profile);

    if default {
        config.default_profile = Some(name.clone());
    }

    match &config_path {
        Some(path) => {
            confy::store_path(path, &config)?
        }
        None => {
            confy::store("anvil-cli", None, &config)?
        }
    };

    println!("Profile '{}' saved.", name);

    Ok(())
}