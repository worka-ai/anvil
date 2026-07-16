use super::common::{AdminClient, PageOptions, print_rpc_response, request_id_or_cli, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum AuditCommands {
    /// List audit events from the administrative audit backend
    List {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long)]
        principal_id: Option<String>,
        #[clap(long)]
        resource_id: Option<String>,
        #[clap(long)]
        action: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

pub(super) async fn handle_audit_command(
    command: &AuditCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        AuditCommands::List {
            request_id,
            principal_id,
            resource_id,
            action,
            page,
        } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "audit_events",
                None,
                Some(&request_id),
                client.list_audit_events(with_auth(
                    api::ListAuditEventsRequest {
                        request_id: request_id.clone(),
                        principal_id: principal_id.clone().unwrap_or_default(),
                        resource_id: resource_id.clone().unwrap_or_default(),
                        action: action.clone().unwrap_or_default(),
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
