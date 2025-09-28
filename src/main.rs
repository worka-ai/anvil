use anvil::run;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    run().await?;
    Ok(())
}
