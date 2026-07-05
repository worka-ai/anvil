use std::{env, path::PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("anvil_descriptor.bin"))
        // .server_mod_attribute("attrs", "#[cfg(feature = \"server\")]")
        // .server_attribute("Echo", "#[derive(PartialEq)]")
        // .client_mod_attribute("attrs", "#[cfg(feature = \"client\")]")
        // .client_attribute("Echo", "#[derive(PartialEq)]")
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile_protos(
            &[manifest_dir.join("proto/anvil.proto")],
            &[manifest_dir.join("proto")],
        )
        .unwrap();
}
