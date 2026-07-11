use crate::context::Context;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use clap::Subcommand;

#[path = "admin/app.rs"]
mod app;
#[path = "admin/audit.rs"]
mod audit;
#[path = "admin/bucket.rs"]
mod bucket;
#[path = "admin/cell.rs"]
mod cell;
#[path = "admin/common.rs"]
mod common;
#[path = "admin/diagnostics.rs"]
mod diagnostics;
#[path = "admin/host_alias.rs"]
mod host_alias;
#[path = "admin/mesh.rs"]
mod mesh;
#[path = "admin/node.rs"]
mod node;
#[path = "admin/policy.rs"]
mod policy;
#[path = "admin/region.rs"]
mod region;
#[path = "admin/repair.rs"]
mod repair;
#[path = "admin/routing.rs"]
mod routing;
#[path = "admin/secret_encryption_key.rs"]
mod secret_encryption_key;
#[path = "admin/storage_class.rs"]
mod storage_class;
#[path = "admin/tenant.rs"]
mod tenant;

pub use self::app::AppCommands;
pub use self::audit::AuditCommands;
pub use self::bucket::BucketCommands;
pub use self::cell::CellCommands;
pub use self::diagnostics::DiagnosticsCommands;
pub use self::host_alias::HostAliasCommands;
pub use self::mesh::MeshCommands;
pub use self::node::NodeCommands;
pub use self::policy::PolicyCommands;
pub use self::region::RegionCommands;
pub use self::repair::RepairCommands;
pub use self::routing::RoutingCommands;
pub use self::secret_encryption_key::SecretEncryptionKeyCommands;
pub use self::storage_class::StorageClassCommands;
pub use self::tenant::TenantCommands;

use self::app::handle_app_command;
use self::audit::handle_audit_command;
use self::bucket::handle_bucket_command;
use self::cell::handle_cell_command;
use self::diagnostics::handle_diagnostics_command;
use self::host_alias::handle_host_alias_command;
use self::mesh::handle_mesh_command;
use self::node::handle_node_command;
use self::policy::handle_policy_command;
use self::region::handle_region_command;
use self::repair::handle_repair_command;
use self::routing::handle_routing_command;
use self::secret_encryption_key::handle_secret_encryption_key_command;
use self::storage_class::handle_storage_class_command;
use self::tenant::handle_tenant_command;

#[derive(Subcommand)]
pub enum AdminCommands {
    /// Manage tenants
    Tenant {
        #[clap(subcommand)]
        command: TenantCommands,
    },
    /// Manage applications
    App {
        #[clap(subcommand)]
        command: AppCommands,
    },
    /// Manage application policies
    Policy {
        #[clap(subcommand)]
        command: PolicyCommands,
    },
    /// Rotate server-side secret encryption envelopes
    SecretEncryptionKey {
        #[clap(subcommand)]
        command: SecretEncryptionKeyCommands,
    },
    /// Manage buckets through the administrative plane
    Bucket {
        #[clap(subcommand)]
        command: BucketCommands,
    },
    /// Manage mesh regions
    Region {
        #[clap(subcommand)]
        command: RegionCommands,
    },
    /// Manage cells within regions
    Cell {
        #[clap(subcommand)]
        command: CellCommands,
    },
    /// Manage nodes within cells
    Node {
        #[clap(subcommand)]
        command: NodeCommands,
    },
    /// Manage mesh regions, cells, nodes and bucket placement through MeshControlService
    Mesh {
        #[clap(subcommand)]
        command: MeshCommands,
    },
    /// Manage custom host aliases
    HostAlias {
        #[clap(subcommand)]
        command: HostAliasCommands,
    },
    /// Inspect and repair mesh routing records
    Routing {
        #[clap(subcommand)]
        command: RoutingCommands,
    },
    /// Run administrative repair jobs
    Repair {
        #[clap(subcommand)]
        command: RepairCommands,
    },
    /// List administrative diagnostics
    Diagnostics {
        #[clap(subcommand)]
        command: DiagnosticsCommands,
    },
    /// List administrative audit events
    Audit {
        #[clap(subcommand)]
        command: AuditCommands,
    },
    /// Inspect CoreStore storage classes and durability profiles
    StorageClass {
        #[clap(subcommand)]
        command: StorageClassCommands,
    },
}

pub async fn handle_admin_command(command: &AdminCommands, ctx: &Context) -> anyhow::Result<()> {
    let token = ctx.get_bearer_token().await?;
    let mut client = AdminServiceClient::connect(ctx.profile.host.clone()).await?;

    match command {
        AdminCommands::Tenant { command } => {
            handle_tenant_command(command, &mut client, &token).await?
        }
        AdminCommands::App { command } => handle_app_command(command, &mut client, &token).await?,
        AdminCommands::Policy { command } => {
            handle_policy_command(command, &mut client, &token).await?
        }
        AdminCommands::SecretEncryptionKey { command } => {
            handle_secret_encryption_key_command(command, &mut client, &token).await?
        }
        AdminCommands::Bucket { command } => {
            handle_bucket_command(command, &mut client, &token).await?
        }
        AdminCommands::Region { command } => {
            handle_region_command(command, &mut client, &token).await?
        }
        AdminCommands::Cell { command } => {
            handle_cell_command(command, &mut client, &token).await?
        }
        AdminCommands::Node { command } => {
            handle_node_command(command, &mut client, &token).await?
        }
        AdminCommands::Mesh { command } => handle_mesh_command(command, ctx, &token).await?,
        AdminCommands::HostAlias { command } => {
            handle_host_alias_command(command, &mut client, &token).await?
        }
        AdminCommands::Routing { command } => {
            handle_routing_command(command, &mut client, &token).await?
        }
        AdminCommands::Repair { command } => {
            handle_repair_command(command, &mut client, &token).await?
        }
        AdminCommands::Diagnostics { command } => {
            handle_diagnostics_command(command, &mut client, &token).await?
        }
        AdminCommands::Audit { command } => {
            handle_audit_command(command, &mut client, &token).await?
        }
        AdminCommands::StorageClass { command } => {
            handle_storage_class_command(command, &mut client, &token).await?
        }
    }

    Ok(())
}

#[cfg(test)]
#[path = "admin/tests.rs"]
mod tests;
