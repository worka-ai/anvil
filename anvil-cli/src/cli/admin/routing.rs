use super::common::{AdminClient, MutationOptions, PageOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::{Subcommand, ValueEnum};

#[derive(Subcommand)]
pub enum RoutingCommands {
    /// List materialised mesh routing records
    List {
        #[clap(long, value_enum)]
        family: Option<RoutingRecordFamilyArg>,
        #[clap(flatten)]
        page: PageOptions,
    },
    /// Repair one materialised mesh routing record from durable source state
    Repair {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long, value_enum)]
        family: RoutingRecordFamilyArg,
        #[clap(long)]
        record_key: String,
    },
}
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RoutingRecordFamilyArg {
    TenantName,
    TenantLocator,
    BucketLocator,
    HostAlias,
}
impl RoutingRecordFamilyArg {
    pub(super) fn to_proto(self) -> i32 {
        match self {
            Self::TenantName => 1,
            Self::TenantLocator => 2,
            Self::BucketLocator => 3,
            Self::HostAlias => 4,
        }
    }
}

pub(super) async fn handle_routing_command(
    command: &RoutingCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        RoutingCommands::List { family, page } => {
            print_rpc_response(
                "routing_records",
                None,
                None,
                client.list_routing_records(with_auth(
                    api::ListRoutingRecordsRequest {
                        family: family.map(RoutingRecordFamilyArg::to_proto).unwrap_or(0),
                        page: page.to_page_request(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        RoutingCommands::Repair {
            context,
            family,
            record_key,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "routing_record",
                Some(&admin_context),
                None,
                client.repair_routing_record(with_auth(
                    api::RepairRoutingRecordRequest {
                        context: Some(admin_context.clone()),
                        family: family.to_proto(),
                        record_key: record_key.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }

    Ok(())
}
