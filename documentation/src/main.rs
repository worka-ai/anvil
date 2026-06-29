use anyhow::Result;
use fission::site::build_from_cli;

fn main() -> Result<()> {
    build_from_cli(anvil_documentation::site_app())
}
