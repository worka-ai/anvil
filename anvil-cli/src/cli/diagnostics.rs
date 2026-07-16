use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum DiagnosticsCommands {
    List {
        bucket: String,
        index: String,
        #[clap(long, default_value_t = 0)]
        after_cursor: u64,
        #[clap(long, default_value_t = 100)]
        limit: u32,
        #[clap(long, default_value = "")]
        severity: String,
    },
}

pub async fn handle_diagnostics_command(
    command: &DiagnosticsCommands,
    ctx: &Context,
) -> anyhow::Result<()> {
    let mut client = IndexServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        DiagnosticsCommands::List {
            bucket,
            index,
            after_cursor,
            limit,
            severity,
        } => {
            let mut request = tonic::Request::new(api::ListIndexDiagnosticsRequest {
                bucket_name: bucket.clone(),
                index_name: index.clone(),
                after_cursor: *after_cursor,
                limit: *limit,
                severity: severity.clone(),
            });
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            for diagnostic in client
                .list_index_diagnostics(request)
                .await?
                .into_inner()
                .diagnostics
            {
                println!(
                    "{}\t{}\t{}\t{}",
                    diagnostic.cursor, diagnostic.severity, diagnostic.code, diagnostic.message
                );
            }
        }
    }
    Ok(())
}
