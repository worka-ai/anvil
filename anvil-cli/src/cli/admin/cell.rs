use super::common::{AdminClient, MutationOptions, PageOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum CellCommands {
    /// Register a cell descriptor
    Register {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
        #[clap(long, default_value_t = 100)]
        placement_weight: u32,
        #[clap(long)]
        failure_domain: String,
    },
    /// Activate a joining or drained cell
    Activate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
    },
    /// Drain an active cell
    Drain {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
    },
    /// Remove a drained cell
    Remove {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
    },
    /// List cell descriptors
    List {
        #[clap(long)]
        region: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

pub(super) async fn handle_cell_command(
    command: &CellCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        CellCommands::Register {
            context,
            region,
            cell_id,
            placement_weight,
            failure_domain,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "cell",
                Some(&admin_context),
                None,
                client.register_cell(with_auth(
                    api::RegisterCellRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                        placement_weight: *placement_weight,
                        failure_domain: failure_domain.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        CellCommands::Activate {
            context,
            region,
            cell_id,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "cell",
                Some(&admin_context),
                None,
                client.activate_cell(with_auth(
                    api::ActivateCellRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        CellCommands::Drain {
            context,
            region,
            cell_id,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "cell",
                Some(&admin_context),
                None,
                client.drain_cell(with_auth(
                    api::DrainCellRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        CellCommands::Remove {
            context,
            region,
            cell_id,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "cell",
                Some(&admin_context),
                None,
                client.remove_cell(with_auth(
                    api::RemoveCellRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        CellCommands::List { region, page } => {
            print_rpc_response(
                "cells",
                None,
                None,
                client.list_cells(with_auth(
                    api::ListCellsRequest {
                        region: region.clone().unwrap_or_default(),
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
