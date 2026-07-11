use super::common::{AdminClient, MutationOptions, print_rpc_response, with_auth};
use anvil::anvil_api as api;
use clap::{Subcommand, ValueEnum};

#[derive(Subcommand)]
pub enum RepairCommands {
    /// Run a repair backend synchronously and return its structured report
    Run {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long, value_enum)]
        repair_kind: RepairKindArg,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: Option<String>,
        #[clap(long)]
        index_name: Option<String>,
        #[clap(long)]
        derived_index_id: Option<String>,
        #[clap(long)]
        database_id: Option<String>,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        rebuild: bool,
    },
}
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RepairKindArg {
    Index,
    DirectoryIndex,
    AuthzDerivedIndex,
    #[value(alias = "personal-db-log-chain", alias = "personal_db_log_chain")]
    PersonaldbLogChain,
    #[value(alias = "mesh-routing-projection", alias = "mesh_routing_projection")]
    MeshRoutingProjection,
}
impl RepairKindArg {
    pub(super) fn to_proto(self) -> i32 {
        match self {
            Self::Index => 1,
            Self::DirectoryIndex => 2,
            Self::AuthzDerivedIndex => 3,
            Self::PersonaldbLogChain => 4,
            Self::MeshRoutingProjection => 5,
        }
    }
}

pub(super) async fn handle_repair_command(
    command: &RepairCommands,
    client: &mut AdminClient,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        RepairCommands::Run {
            context,
            repair_kind,
            tenant_id,
            bucket_name,
            index_name,
            derived_index_id,
            database_id,
            rebuild,
        } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "repair_task",
                Some(&admin_context),
                None,
                client.run_repair(with_auth(
                    api::RunRepairRequest {
                        context: Some(admin_context.clone()),
                        repair_kind: repair_kind.to_proto(),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone().unwrap_or_default(),
                        index_name: index_name.clone().unwrap_or_default(),
                        derived_index_id: derived_index_id.clone().unwrap_or_default(),
                        database_id: database_id.clone().unwrap_or_default(),
                        rebuild: *rebuild,
                    },
                    token,
                )?),
            )
            .await?;
        }
    }

    Ok(())
}
