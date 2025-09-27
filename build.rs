use std::{env, path::PathBuf};

fn main() {
    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    tonic_prost_build::configure()
        .file_descriptor_set_path(out_dir.join("anvil_descriptor.bin"))
        // .server_mod_attribute("attrs", "#[cfg(feature = \"server\")]")
        // .server_attribute("Echo", "#[derive(PartialEq)]")
        // .client_mod_attribute("attrs", "#[cfg(feature = \"client\")]")
        // .client_attribute("Echo", "#[derive(PartialEq)]")
        .compile_protos(&["proto/anvil.proto"], &["proto"])
        .unwrap();
}
