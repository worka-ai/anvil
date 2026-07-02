fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .type_attribute(".", "#[allow(dead_code)]")
        .compile_protos(&["proto/anvil.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/anvil.proto");
    Ok(())
}
