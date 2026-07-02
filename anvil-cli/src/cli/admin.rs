use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use std::str::FromStr;

#[derive(Subcommand)]
pub enum AdminCommands {
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
    /// Manage object links
    Link {
        #[clap(subcommand)]
        command: LinkCommands,
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
}

#[derive(Subcommand)]
pub enum RegionCommands {
    /// Create a region descriptor
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
        #[clap(long)]
        public_base_url: String,
        #[clap(long)]
        virtual_host_suffix: String,
        #[clap(long, default_value_t = 100)]
        placement_weight: u32,
        #[clap(long)]
        default_cell: Option<String>,
    },
    /// Activate a joining or drained region
    Activate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
    },
    /// Set an active region read-only
    SetReadOnly {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
    },
    /// Drain an active region
    Drain {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
        #[clap(long)]
        default_disposition: RegionDrainDispositionArg,
        #[clap(
            long = "bucket-override",
            value_name = "TENANT_ID:BUCKET_NAME:DISPOSITION:REASON"
        )]
        bucket_overrides: Vec<BucketDrainOverrideArg>,
    },
    /// Remove a drained region
    Remove {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        region: String,
    },
    /// List region descriptors
    List {
        #[clap(flatten)]
        page: PageOptions,
    },
}

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

#[derive(Subcommand)]
pub enum NodeCommands {
    /// Register a node descriptor
    Register {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
        #[clap(long)]
        region: String,
        #[clap(long)]
        cell_id: String,
        #[clap(long)]
        libp2p_peer_id: String,
        #[clap(long)]
        public_api_addr: String,
        #[clap(long = "public-cluster-addr")]
        public_cluster_addrs: Vec<String>,
        #[clap(long = "capability", value_delimiter = ',', required = true)]
        capabilities: Vec<NodeCapabilityArg>,
    },
    /// Activate a joining or drained node
    Activate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
    },
    /// Drain an active node
    Drain {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
        #[clap(long)]
        graceful_timeout_ms: u64,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        force_after_timeout: bool,
    },
    /// Force an active or draining node offline
    ForceOffline {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
    },
    /// Remove a drained node
    Remove {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        node_id: String,
    },
    /// List node descriptors
    List {
        #[clap(long)]
        region: Option<String>,
        #[clap(long)]
        cell_id: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

#[derive(Subcommand)]
pub enum LinkCommands {
    /// Create a symlink-like object link
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        link_key: String,
        #[clap(long)]
        target_key: String,
        #[clap(long)]
        target_version: Option<String>,
        #[clap(long, value_enum, default_value_t = ObjectLinkResolutionArg::Follow)]
        resolution: ObjectLinkResolutionArg,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        allow_dangling: bool,
    },
    /// Update an existing object link
    Update {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        link_key: String,
        #[clap(long)]
        target_key: String,
        #[clap(long)]
        target_version: Option<String>,
        #[clap(long, value_enum, default_value_t = ObjectLinkResolutionArg::Follow)]
        resolution: ObjectLinkResolutionArg,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        allow_dangling: bool,
    },
    /// Delete an object link entry without deleting its target
    Delete {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        link_key: String,
    },
    /// Read object link metadata
    Read {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        link_key: String,
    },
    /// List object links in a bucket
    List {
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        prefix: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

#[derive(Subcommand)]
pub enum HostAliasCommands {
    /// Create a custom host alias in pending verification state
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        region: String,
        #[clap(long, default_value = "")]
        prefix: String,
    },
    /// Activate a verified host alias
    Activate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
    },
    /// Suspend an active host alias
    Suspend {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
    },
    /// Delete a host alias descriptor
    Delete {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        hostname: String,
    },
    /// Read host alias metadata
    Read {
        #[clap(long)]
        request_id: Option<String>,
        #[clap(long)]
        hostname: String,
    },
    /// List host aliases
    List {
        #[clap(long)]
        region: Option<String>,
        #[clap(flatten)]
        page: PageOptions,
    },
}

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

#[derive(Args, Debug, Clone)]
pub struct MutationOptions {
    /// AdminRequestContext.request_id. Defaults to a generated UUID.
    #[clap(long)]
    request_id: Option<String>,
    /// AdminRequestContext.idempotency_key. Defaults to a generated UUID.
    #[clap(long)]
    idempotency_key: Option<String>,
    /// AdminRequestContext.audit_reason. Required for all mutations.
    #[clap(long)]
    audit_reason: String,
    /// AdminRequestContext.expected_generation. Use 0 for create/register requests.
    #[clap(long)]
    expected_generation: u64,
}

impl MutationOptions {
    fn to_context(&self) -> api::AdminRequestContext {
        api::AdminRequestContext {
            request_id: self
                .request_id
                .clone()
                .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::new_v4())),
            idempotency_key: self
                .idempotency_key
                .clone()
                .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
            audit_reason: self.audit_reason.clone(),
            expected_generation: self.expected_generation,
        }
    }
}

#[derive(Args, Debug, Clone, Default)]
pub struct PageOptions {
    #[clap(long)]
    cursor: Option<String>,
    #[clap(long)]
    limit: Option<u32>,
}

impl PageOptions {
    fn to_page_request(&self) -> Option<api::PageRequest> {
        if self.cursor.is_none() && self.limit.is_none() {
            return None;
        }

        Some(api::PageRequest {
            cursor: self.cursor.clone().unwrap_or_default(),
            limit: self.limit.unwrap_or_default(),
        })
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NodeCapabilityArg {
    Object,
    Index,
    #[value(alias = "personal-db", alias = "personal_db")]
    Personaldb,
    Gateway,
    Admin,
}

impl NodeCapabilityArg {
    fn to_proto(self) -> i32 {
        match self {
            Self::Object => 1,
            Self::Index => 2,
            Self::Personaldb => 3,
            Self::Gateway => 4,
            Self::Admin => 5,
        }
    }
}

#[derive(Copy, Clone, Debug, Default, ValueEnum)]
pub enum ObjectLinkResolutionArg {
    #[default]
    Follow,
    Redirect,
}

impl ObjectLinkResolutionArg {
    fn to_proto(self) -> i32 {
        match self {
            Self::Follow => 1,
            Self::Redirect => 2,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RoutingRecordFamilyArg {
    TenantName,
    TenantLocator,
    BucketLocator,
}

impl RoutingRecordFamilyArg {
    fn to_proto(self) -> i32 {
        match self {
            Self::TenantName => 1,
            Self::TenantLocator => 2,
            Self::BucketLocator => 3,
        }
    }
}

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RegionDrainDispositionArg {
    BlockUntilEmpty,
    RemainProxyOnly,
    ReadOnlyUntilRemoved,
    DeleteAfterRetention,
}

impl RegionDrainDispositionArg {
    fn to_proto(self) -> i32 {
        match self {
            Self::BlockUntilEmpty => 1,
            Self::RemainProxyOnly => 2,
            Self::ReadOnlyUntilRemoved => 3,
            Self::DeleteAfterRetention => 4,
        }
    }

    fn parse(value: &str) -> Result<Self, String> {
        match normalize_enum_value(value).as_str() {
            "blockuntilempty" => Ok(Self::BlockUntilEmpty),
            "remainproxyonly" => Ok(Self::RemainProxyOnly),
            "readonlyuntilremoved" => Ok(Self::ReadOnlyUntilRemoved),
            "deleteafterretention" => Ok(Self::DeleteAfterRetention),
            _ => Err(format!("invalid region drain disposition '{value}'")),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BucketDrainOverrideArg {
    tenant_id: String,
    bucket_name: String,
    disposition: RegionDrainDispositionArg,
    reason: String,
}

impl BucketDrainOverrideArg {
    fn to_proto(&self) -> api::BucketDrainOverride {
        api::BucketDrainOverride {
            tenant_id: self.tenant_id.clone(),
            bucket_name: self.bucket_name.clone(),
            disposition: self.disposition.to_proto(),
            reason: self.reason.clone(),
        }
    }
}

impl FromStr for BucketDrainOverrideArg {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.splitn(4, ':');
        let tenant_id = required_part(parts.next(), "tenant_id")?;
        let bucket_name = required_part(parts.next(), "bucket_name")?;
        let disposition = required_part(parts.next(), "disposition")?;
        let reason = required_part(parts.next(), "reason")?;

        Ok(Self {
            tenant_id: tenant_id.to_string(),
            bucket_name: bucket_name.to_string(),
            disposition: RegionDrainDispositionArg::parse(disposition)?,
            reason: reason.to_string(),
        })
    }
}

pub async fn handle_admin_command(command: &AdminCommands, ctx: &Context) -> anyhow::Result<()> {
    let token = ctx.get_bearer_token().await?;
    let mut client = AdminServiceClient::connect(ctx.profile.host.clone()).await?;

    match command {
        AdminCommands::Region { command } => {
            handle_region_command(command, &mut client, &token).await?
        }
        AdminCommands::Cell { command } => {
            handle_cell_command(command, &mut client, &token).await?
        }
        AdminCommands::Node { command } => {
            handle_node_command(command, &mut client, &token).await?
        }
        AdminCommands::Link { command } => {
            handle_link_command(command, &mut client, &token).await?
        }
        AdminCommands::HostAlias { command } => {
            handle_host_alias_command(command, &mut client, &token).await?
        }
        AdminCommands::Routing { command } => {
            handle_routing_command(command, &mut client, &token).await?
        }
    }

    Ok(())
}

async fn handle_region_command(
    command: &RegionCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        RegionCommands::Create {
            context,
            region,
            public_base_url,
            virtual_host_suffix,
            placement_weight,
            default_cell,
        } => {
            let response = client
                .create_region(with_auth(
                    api::CreateRegionRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                        public_base_url: public_base_url.clone(),
                        virtual_host_suffix: virtual_host_suffix.clone(),
                        placement_weight: *placement_weight,
                        default_cell: default_cell.clone().unwrap_or_default(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        RegionCommands::Activate { context, region } => {
            let response = client
                .activate_region(with_auth(
                    api::ActivateRegionRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        RegionCommands::SetReadOnly { context, region } => {
            let response = client
                .set_region_read_only(with_auth(
                    api::SetRegionReadOnlyRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        RegionCommands::Drain {
            context,
            region,
            default_disposition,
            bucket_overrides,
        } => {
            let response = client
                .drain_region(with_auth(
                    api::DrainRegionRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                        default_disposition: default_disposition.to_proto(),
                        bucket_overrides: bucket_overrides
                            .iter()
                            .map(BucketDrainOverrideArg::to_proto)
                            .collect(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        RegionCommands::Remove { context, region } => {
            let response = client
                .remove_region(with_auth(
                    api::RemoveRegionRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        RegionCommands::List { page } => {
            let response = client
                .list_regions(with_auth(
                    api::ListRegionsRequest {
                        page: page.to_page_request(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
    }

    Ok(())
}

async fn handle_cell_command(
    command: &CellCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        CellCommands::Register {
            context,
            region,
            cell_id,
            placement_weight,
        } => {
            let response = client
                .register_cell(with_auth(
                    api::RegisterCellRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                        placement_weight: *placement_weight,
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        CellCommands::Activate {
            context,
            region,
            cell_id,
        } => {
            let response = client
                .activate_cell(with_auth(
                    api::ActivateCellRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        CellCommands::Drain {
            context,
            region,
            cell_id,
        } => {
            let response = client
                .drain_cell(with_auth(
                    api::DrainCellRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        CellCommands::Remove {
            context,
            region,
            cell_id,
        } => {
            let response = client
                .remove_cell(with_auth(
                    api::RemoveCellRequest {
                        context: Some(context.to_context()),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        CellCommands::List { region, page } => {
            let response = client
                .list_cells(with_auth(
                    api::ListCellsRequest {
                        region: region.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
    }

    Ok(())
}

async fn handle_node_command(
    command: &NodeCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        NodeCommands::Register {
            context,
            node_id,
            region,
            cell_id,
            libp2p_peer_id,
            public_api_addr,
            public_cluster_addrs,
            capabilities,
        } => {
            let response = client
                .register_node(with_auth(
                    api::RegisterNodeRequest {
                        context: Some(context.to_context()),
                        node_id: node_id.clone(),
                        region: region.clone(),
                        cell_id: cell_id.clone(),
                        libp2p_peer_id: libp2p_peer_id.clone(),
                        public_cluster_addrs: public_cluster_addrs.clone(),
                        public_api_addr: public_api_addr.clone(),
                        capabilities: capabilities
                            .iter()
                            .map(|capability| capability.to_proto())
                            .collect(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        NodeCommands::Activate { context, node_id } => {
            let response = client
                .activate_node(with_auth(
                    api::ActivateNodeRequest {
                        context: Some(context.to_context()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        NodeCommands::Drain {
            context,
            node_id,
            graceful_timeout_ms,
            force_after_timeout,
        } => {
            let response = client
                .drain_node(with_auth(
                    api::DrainNodeRequest {
                        context: Some(context.to_context()),
                        node_id: node_id.clone(),
                        graceful_timeout_ms: *graceful_timeout_ms,
                        force_after_timeout: *force_after_timeout,
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        NodeCommands::ForceOffline { context, node_id } => {
            let response = client
                .force_offline_node(with_auth(
                    api::ForceOfflineNodeRequest {
                        context: Some(context.to_context()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        NodeCommands::Remove { context, node_id } => {
            let response = client
                .remove_node(with_auth(
                    api::RemoveNodeRequest {
                        context: Some(context.to_context()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        NodeCommands::List {
            region,
            cell_id,
            page,
        } => {
            let response = client
                .list_nodes(with_auth(
                    api::ListNodesRequest {
                        region: region.clone().unwrap_or_default(),
                        cell_id: cell_id.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
    }

    Ok(())
}

async fn handle_link_command(
    command: &LinkCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        LinkCommands::Create {
            context,
            tenant_id,
            bucket_name,
            link_key,
            target_key,
            target_version,
            resolution,
            allow_dangling,
        } => {
            let response = client
                .create_object_link(with_auth(
                    api::CreateObjectLinkRequest {
                        context: Some(context.to_context()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        link_key: link_key.clone(),
                        target_key: target_key.clone(),
                        target_version: target_version.clone().unwrap_or_default(),
                        resolution: resolution.to_proto(),
                        allow_dangling: *allow_dangling,
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        LinkCommands::Update {
            context,
            tenant_id,
            bucket_name,
            link_key,
            target_key,
            target_version,
            resolution,
            allow_dangling,
        } => {
            let response = client
                .update_object_link(with_auth(
                    api::UpdateObjectLinkRequest {
                        context: Some(context.to_context()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        link_key: link_key.clone(),
                        target_key: target_key.clone(),
                        target_version: target_version.clone().unwrap_or_default(),
                        resolution: resolution.to_proto(),
                        allow_dangling: *allow_dangling,
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        LinkCommands::Delete {
            context,
            tenant_id,
            bucket_name,
            link_key,
        } => {
            let response = client
                .delete_object_link(with_auth(
                    api::DeleteObjectLinkRequest {
                        context: Some(context.to_context()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        link_key: link_key.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        LinkCommands::Read {
            request_id,
            tenant_id,
            bucket_name,
            link_key,
        } => {
            let response = client
                .read_object_link(with_auth(
                    api::ReadObjectLinkRequest {
                        request_id: request_id
                            .clone()
                            .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::new_v4())),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        link_key: link_key.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        LinkCommands::List {
            tenant_id,
            bucket_name,
            prefix,
            page,
        } => {
            let response = client
                .list_object_links(with_auth(
                    api::ListObjectLinksRequest {
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        prefix: prefix.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
    }

    Ok(())
}

async fn handle_host_alias_command(
    command: &HostAliasCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        HostAliasCommands::Create {
            context,
            hostname,
            tenant_id,
            bucket_name,
            region,
            prefix,
        } => {
            let response = client
                .create_host_alias(with_auth(
                    api::CreateHostAliasRequest {
                        context: Some(context.to_context()),
                        hostname: hostname.clone(),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        region: region.clone(),
                        prefix: prefix.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        HostAliasCommands::Activate { context, hostname } => {
            let response = client
                .activate_host_alias(with_auth(
                    api::ActivateHostAliasRequest {
                        context: Some(context.to_context()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        HostAliasCommands::Suspend { context, hostname } => {
            let response = client
                .suspend_host_alias(with_auth(
                    api::SuspendHostAliasRequest {
                        context: Some(context.to_context()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        HostAliasCommands::Delete { context, hostname } => {
            let response = client
                .delete_host_alias(with_auth(
                    api::DeleteHostAliasRequest {
                        context: Some(context.to_context()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        HostAliasCommands::Read {
            request_id,
            hostname,
        } => {
            let response = client
                .read_host_alias(with_auth(
                    api::ReadHostAliasRequest {
                        request_id: request_id
                            .clone()
                            .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::new_v4())),
                        hostname: hostname.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        HostAliasCommands::List { region, page } => {
            let response = client
                .list_host_aliases(with_auth(
                    api::ListHostAliasesRequest {
                        region: region.clone().unwrap_or_default(),
                        page: page.to_page_request(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
    }

    Ok(())
}

async fn handle_routing_command(
    command: &RoutingCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        RoutingCommands::List { family, page } => {
            let response = client
                .list_routing_records(with_auth(
                    api::ListRoutingRecordsRequest {
                        family: family.map(RoutingRecordFamilyArg::to_proto).unwrap_or(0),
                        page: page.to_page_request(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
        RoutingCommands::Repair {
            context,
            family,
            record_key,
        } => {
            let response = client
                .repair_routing_record(with_auth(
                    api::RepairRoutingRecordRequest {
                        context: Some(context.to_context()),
                        family: family.to_proto(),
                        record_key: record_key.clone(),
                    },
                    token,
                )?)
                .await?
                .into_inner();
            print_json(&response)?;
        }
    }

    Ok(())
}

fn with_auth<T>(message: T, token: &str) -> anyhow::Result<tonic::Request<T>> {
    let mut request = tonic::Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().map_err(|err| {
            anyhow::anyhow!("failed to build authorization metadata header: {err}")
        })?,
    );
    Ok(request)
}

fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    serde_json::to_writer_pretty(std::io::stdout().lock(), value)?;
    println!();
    Ok(())
}

fn required_part<'a>(part: Option<&'a str>, name: &str) -> Result<&'a str, String> {
    part.filter(|value| !value.is_empty())
        .ok_or_else(|| format!("bucket override is missing {name}"))
}

fn normalize_enum_value(value: &str) -> String {
    value
        .chars()
        .filter(|ch| *ch != '-' && *ch != '_')
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};
    use tempfile::TempDir;
    use tokio::net::TcpStream;
    use tokio::task::JoinHandle;

    #[derive(Parser)]
    struct TestAdminCli {
        #[clap(subcommand)]
        command: AdminCommands,
    }

    struct AdminCliNode {
        admin_url: String,
        state: anvil::AppState,
        handle: JoinHandle<()>,
        _temp: TempDir,
    }

    impl Drop for AdminCliNode {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    async fn spawn_admin_cli_node() -> AdminCliNode {
        let temp = tempfile::tempdir().unwrap();
        let storage_path = temp.path().join("cli-node");
        let public_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let admin_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_addr = public_listener.local_addr().unwrap();
        let admin_addr = admin_listener.local_addr().unwrap();

        let config = anvil::config::Config {
            cluster_secret: Some("cli-test-cluster-secret".to_string()),
            jwt_secret: "cli-test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            cluster_listen_addr: "/ip4/127.0.0.1/udp/0/quic-v1".to_string(),
            public_cluster_addrs: vec![],
            metadata_cache_ttl_secs: 1,
            public_api_addr: format!("http://{public_addr}"),
            api_listen_addr: public_addr.to_string(),
            admin_listen_addr: admin_addr.to_string(),
            mesh_id: "mesh-cli-test".to_string(),
            region: "eu-west-1".to_string(),
            cell_id: "cell-a".to_string(),
            public_region_base_domain: "eu-west-1.anvil-storage.test".to_string(),
            bootstrap_addrs: vec![],
            init_cluster: false,
            enable_mdns: false,
            storage_path: storage_path.to_string_lossy().into_owned(),
            node_id_path: storage_path.join("node-id").to_string_lossy().into_owned(),
            cluster_keypair_path: storage_path
                .join("cluster-keypair.pb")
                .to_string_lossy()
                .into_owned(),
            personaldb_snapshot_entry_threshold: 1024,
            personaldb_snapshot_payload_bytes_threshold: 64 * 1024 * 1024,
            ..anvil::config::Config::default()
        };

        let state = anvil::AppState::new(config, None).await.unwrap();
        let swarm = anvil::cluster::create_swarm(state.config.clone())
            .await
            .unwrap();
        let state_for_handle = state.clone();
        let handle = tokio::spawn(async move {
            let (_tx, rx) = tokio::sync::mpsc::channel(1);
            anvil::start_node_with_admin_listener(
                public_listener,
                Some(admin_listener),
                state_for_handle,
                swarm,
                rx,
            )
            .await
            .unwrap();
        });

        wait_for_tcp_port(admin_addr, Duration::from_secs(5)).await;

        AdminCliNode {
            admin_url: format!("http://{admin_addr}"),
            state,
            handle,
            _temp: temp,
        }
    }

    async fn wait_for_tcp_port(addr: SocketAddr, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if TcpStream::connect(addr).await.is_ok() {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for admin listener on {addr}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    fn mutation_options(label: &str, expected_generation: u64) -> MutationOptions {
        MutationOptions {
            request_id: Some(format!("req-{label}")),
            idempotency_key: Some(format!("idem-{label}")),
            audit_reason: format!("test {label}"),
            expected_generation,
        }
    }

    fn admin_token(node: &AdminCliNode) -> String {
        node.state
            .jwt_manager
            .mint_token(
                "cli-admin-principal".to_string(),
                vec!["anvil_admin:*|anvil_admin:cluster:mesh-cli-test".to_string()],
                0,
            )
            .unwrap()
    }

    #[test]
    fn mutation_options_generate_optional_ids() {
        let context = MutationOptions {
            request_id: None,
            idempotency_key: None,
            audit_reason: "planned maintenance".to_string(),
            expected_generation: 42,
        }
        .to_context();

        assert!(context.request_id.starts_with("cli-"));
        assert!(!context.idempotency_key.is_empty());
        assert_eq!(context.audit_reason, "planned maintenance");
        assert_eq!(context.expected_generation, 42);
    }

    #[test]
    fn mutation_parse_requires_explicit_audit_reason() {
        let result = TestAdminCli::try_parse_from([
            "admin",
            "region",
            "create",
            "--expected-generation",
            "0",
            "--region",
            "eu-west-1",
            "--public-base-url",
            "https://eu-west-1.example.test",
            "--virtual-host-suffix",
            "eu-west-1.example.test",
        ]);
        let Err(error) = result else {
            panic!("expected audit_reason parse failure");
        };

        assert_eq!(
            error.kind(),
            clap::error::ErrorKind::MissingRequiredArgument
        );
    }

    #[test]
    fn mutation_parse_allows_generated_request_ids() {
        let cli = TestAdminCli::try_parse_from([
            "admin",
            "region",
            "create",
            "--audit-reason",
            "bootstrap region",
            "--expected-generation",
            "0",
            "--region",
            "eu-west-1",
            "--public-base-url",
            "https://eu-west-1.example.test",
            "--virtual-host-suffix",
            "eu-west-1.example.test",
        ])
        .unwrap();

        let AdminCommands::Region {
            command: RegionCommands::Create {
                context, region, ..
            },
        } = cli.command
        else {
            panic!("expected region create command");
        };

        assert!(context.request_id.is_none());
        assert!(context.idempotency_key.is_none());
        assert_eq!(context.audit_reason, "bootstrap region");
        assert_eq!(context.expected_generation, 0);
        assert_eq!(region, "eu-west-1");
    }

    #[test]
    fn bucket_override_parses_reason_with_colons() {
        let override_arg: BucketDrainOverrideArg =
            "tenant-a:photos:read-only-until-removed:incident:123"
                .parse()
                .unwrap();

        let proto = override_arg.to_proto();
        assert_eq!(proto.tenant_id, "tenant-a");
        assert_eq!(proto.bucket_name, "photos");
        assert_eq!(proto.disposition, 3);
        assert_eq!(proto.reason, "incident:123");
    }

    #[test]
    fn node_capabilities_map_to_proto_values() {
        assert_eq!(NodeCapabilityArg::Object.to_proto(), 1);
        assert_eq!(NodeCapabilityArg::Index.to_proto(), 2);
        assert_eq!(NodeCapabilityArg::Personaldb.to_proto(), 3);
        assert_eq!(NodeCapabilityArg::Gateway.to_proto(), 4);
        assert_eq!(NodeCapabilityArg::Admin.to_proto(), 5);
    }

    #[test]
    fn object_link_resolution_maps_to_proto_values() {
        assert_eq!(ObjectLinkResolutionArg::Follow.to_proto(), 1);
        assert_eq!(ObjectLinkResolutionArg::Redirect.to_proto(), 2);
    }

    #[test]
    fn link_create_parses_required_context_and_target() {
        let cli = TestAdminCli::try_parse_from([
            "admin",
            "link",
            "create",
            "--audit-reason",
            "publish latest",
            "--expected-generation",
            "0",
            "--tenant-id",
            "tenant-a",
            "--bucket-name",
            "releases",
            "--link-key",
            "latest.exe",
            "--target-key",
            "my-app-v1.exe",
            "--resolution",
            "redirect",
        ])
        .unwrap();

        let AdminCommands::Link {
            command:
                LinkCommands::Create {
                    context,
                    tenant_id,
                    bucket_name,
                    link_key,
                    target_key,
                    resolution,
                    ..
                },
        } = cli.command
        else {
            panic!("expected link create command");
        };

        assert_eq!(context.audit_reason, "publish latest");
        assert_eq!(context.expected_generation, 0);
        assert_eq!(tenant_id, "tenant-a");
        assert_eq!(bucket_name, "releases");
        assert_eq!(link_key, "latest.exe");
        assert_eq!(target_key, "my-app-v1.exe");
        assert_eq!(resolution.to_proto(), 2);
    }

    #[test]
    fn host_alias_activate_requires_expected_generation_and_reason() {
        let cli = TestAdminCli::try_parse_from([
            "admin",
            "host-alias",
            "activate",
            "--audit-reason",
            "dns verified",
            "--expected-generation",
            "7",
            "--hostname",
            "cdn.example.com",
        ])
        .unwrap();

        let AdminCommands::HostAlias {
            command: HostAliasCommands::Activate { context, hostname },
        } = cli.command
        else {
            panic!("expected host-alias activate command");
        };

        assert_eq!(context.audit_reason, "dns verified");
        assert_eq!(context.expected_generation, 7);
        assert_eq!(hostname, "cdn.example.com");
    }

    #[test]
    fn missing_lifecycle_commands_parse_with_mutation_context() {
        let region_cli = TestAdminCli::try_parse_from([
            "admin",
            "region",
            "set-read-only",
            "--audit-reason",
            "maintenance window",
            "--expected-generation",
            "11",
            "--region",
            "eu-west-1",
        ])
        .unwrap();
        let AdminCommands::Region {
            command: RegionCommands::SetReadOnly { context, region },
        } = region_cli.command
        else {
            panic!("expected region set-read-only command");
        };
        assert_eq!(context.audit_reason, "maintenance window");
        assert_eq!(context.expected_generation, 11);
        assert_eq!(region, "eu-west-1");

        let node_cli = TestAdminCli::try_parse_from([
            "admin",
            "node",
            "force-offline",
            "--audit-reason",
            "lost heartbeat",
            "--expected-generation",
            "12",
            "--node-id",
            "node-a",
        ])
        .unwrap();
        let AdminCommands::Node {
            command: NodeCommands::ForceOffline { context, node_id },
        } = node_cli.command
        else {
            panic!("expected node force-offline command");
        };
        assert_eq!(context.audit_reason, "lost heartbeat");
        assert_eq!(context.expected_generation, 12);
        assert_eq!(node_id, "node-a");
    }

    #[test]
    fn routing_commands_parse_family_and_mutation_context() {
        let list_cli = TestAdminCli::try_parse_from([
            "admin",
            "routing",
            "list",
            "--family",
            "bucket-locator",
            "--limit",
            "25",
        ])
        .unwrap();
        let AdminCommands::Routing {
            command: RoutingCommands::List { family, page },
        } = list_cli.command
        else {
            panic!("expected routing list command");
        };
        assert_eq!(family.unwrap().to_proto(), 3);
        assert_eq!(page.limit, Some(25));

        let repair_cli = TestAdminCli::try_parse_from([
            "admin",
            "routing",
            "repair",
            "--audit-reason",
            "rebuild missing locator",
            "--expected-generation",
            "1",
            "--family",
            "tenant-name",
            "--record-key",
            "acme",
        ])
        .unwrap();
        let AdminCommands::Routing {
            command:
                RoutingCommands::Repair {
                    context,
                    family,
                    record_key,
                },
        } = repair_cli.command
        else {
            panic!("expected routing repair command");
        };
        assert_eq!(context.audit_reason, "rebuild missing locator");
        assert_eq!(context.expected_generation, 1);
        assert_eq!(family.to_proto(), 1);
        assert_eq!(record_key, "acme");
    }

    #[tokio::test]
    async fn missing_lifecycle_cli_handlers_call_admin_service_and_persist_state() {
        let node = spawn_admin_cli_node().await;
        let token = admin_token(&node);
        let mut client = AdminServiceClient::connect(node.admin_url.clone())
            .await
            .unwrap();

        handle_region_command(
            &RegionCommands::Create {
                context: mutation_options("cli-create-region", 0),
                region: "eu-west-1".to_string(),
                public_base_url: "https://eu-west-1.anvil-storage.test".to_string(),
                virtual_host_suffix: "eu-west-1.anvil-storage.test".to_string(),
                placement_weight: 100,
                default_cell: Some("cell-a".to_string()),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        handle_cell_command(
            &CellCommands::Register {
                context: mutation_options("cli-register-cell", 0),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                placement_weight: 100,
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let cell = node
            .state
            .persistence
            .list_cell_descriptors(Some("eu-west-1"))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        handle_cell_command(
            &CellCommands::Activate {
                context: mutation_options("cli-activate-cell", cell.generation),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let region = node
            .state
            .persistence
            .list_region_descriptors()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        handle_region_command(
            &RegionCommands::Activate {
                context: mutation_options("cli-activate-region", region.generation),
                region: "eu-west-1".to_string(),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let active_region = node
            .state
            .persistence
            .list_region_descriptors()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        handle_region_command(
            &RegionCommands::SetReadOnly {
                context: mutation_options("cli-set-region-read-only", active_region.generation),
                region: "eu-west-1".to_string(),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let read_only_region = node
            .state
            .persistence
            .list_region_descriptors()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            read_only_region.state,
            anvil::mesh_lifecycle::LifecycleState::ReadOnly
        );

        handle_region_command(
            &RegionCommands::Activate {
                context: mutation_options(
                    "cli-reactivate-read-only-region",
                    read_only_region.generation,
                ),
                region: "eu-west-1".to_string(),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        handle_node_command(
            &NodeCommands::Register {
                context: mutation_options("cli-register-node", 0),
                node_id: "node-a".to_string(),
                region: "eu-west-1".to_string(),
                cell_id: "cell-a".to_string(),
                libp2p_peer_id: "peer-a".to_string(),
                public_api_addr: "http://127.0.0.1:50051".to_string(),
                public_cluster_addrs: vec!["/ip4/127.0.0.1/udp/7443/quic-v1".to_string()],
                capabilities: vec![NodeCapabilityArg::Object, NodeCapabilityArg::Admin],
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let registered_node = node
            .state
            .persistence
            .list_node_descriptors(Some("eu-west-1"), Some("cell-a"))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        handle_node_command(
            &NodeCommands::Activate {
                context: mutation_options("cli-activate-node", registered_node.generation),
                node_id: "node-a".to_string(),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let active_node = node
            .state
            .persistence
            .list_node_descriptors(Some("eu-west-1"), Some("cell-a"))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        handle_node_command(
            &NodeCommands::ForceOffline {
                context: mutation_options("cli-force-offline-node", active_node.generation),
                node_id: "node-a".to_string(),
            },
            &mut client,
            &token,
        )
        .await
        .unwrap();

        let offline_node = node
            .state
            .persistence
            .list_node_descriptors(Some("eu-west-1"), Some("cell-a"))
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        assert_eq!(
            offline_node.state,
            anvil::mesh_lifecycle::LifecycleState::Offline
        );
    }
}
