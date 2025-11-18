use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tempfile::tempdir;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct MyTestConfig {
    version: String,
    is_test: bool,
}

impl Default for MyTestConfig {
    fn default() -> Self {
        Self {
            version: "0.1.0".to_string(),
            is_test: true,
        }
    }
}

#[test]
fn test_confy_store_and_load_path() {
    // 1. Create a temporary directory.
    let temp_dir = tempdir().expect("Failed to create temp dir");
    let config_path: PathBuf = temp_dir.path().join("my-test-app.toml");

    // 2. Define a simple struct for configuration.
    let my_cfg = MyTestConfig {
        version: "1.2.3".to_string(),
        is_test: false,
    };

    // 3. Use `confy::store_path` to save a configuration file.
    println!("Attempting to store config at: {}", config_path.display());
    confy::store_path(&config_path, &my_cfg).expect("Failed to store config");

    // 4. Use `std::fs` to read the file that `confy` just wrote.
    let file_content = fs::read_to_string(&config_path).expect("Failed to read config file");
    println!(
        "Content of config file:
{}",
        file_content
    );

    // Verify the content is what we expect.
    // Note: The order of fields in a TOML file is not guaranteed.
    assert!(file_content.contains("version = \"1.2.3\""));
    assert!(file_content.contains("is_test = false"));

    // 5. Use `confy::load_path` to load the configuration.
    println!("Attempting to load config from: {}", config_path.display());
    let loaded_cfg: MyTestConfig = confy::load_path(&config_path).expect("Failed to load config");

    // 6. Assert that the loaded configuration matches the original one.
    assert_eq!(my_cfg, loaded_cfg);

    println!("Confy store_path and load_path test passed successfully!");
}
