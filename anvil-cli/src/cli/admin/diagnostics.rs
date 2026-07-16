use super::common::{AdminClient, PageOptions, print_rpc_response, request_id_or_cli, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum DiagnosticsCommands {
    /// List diagnostics from available administrative diagnostic backends
    List {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long)]
        source: Option<String>,
        #[clap(long)]
        tenant_id: Option<String>,
        #[clap(long)]
        bucket_name: Option<String>,
        #[clap(long)]
        index_name: Option<String>,
        #[clap(long)]
        severity: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

pub(super) async fn handle_diagnostics_command(
    command: &DiagnosticsCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        DiagnosticsCommands::List {
            request_id,
            source,
            tenant_id,
            bucket_name,
            index_name,
            severity,
            page,
        } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "diagnostics",
                None,
                Some(&request_id),
                client.list_diagnostics(with_auth(
                    api::ListDiagnosticsRequest {
                        request_id: request_id.clone(),
                        source: source.clone().unwrap_or_default(),
                        tenant_id: tenant_id.clone().unwrap_or_default(),
                        bucket_name: bucket_name.clone().unwrap_or_default(),
                        index_name: index_name.clone().unwrap_or_default(),
                        severity: severity.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }

    Ok(())
}
