use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::admin_service_client::AdminServiceClient;
use clap::{Args, Subcommand, ValueEnum};
use serde::Serialize;
use serde_json::{Value, json};
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::str::FromStr;

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
}

#[derive(Subcommand)]
pub enum TenantCommands {
    /// Create a tenant
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        name: String,
        #[clap(long, default_value = "")]
        home_region: String,
    },
}

#[derive(Subcommand)]
pub enum AppCommands {
    /// Create an application credential
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
    },
    /// Rotate an application secret
    RotateSecret {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
    },
}

#[derive(Subcommand)]
pub enum PolicyCommands {
    /// Grant an application permission scope
    Grant {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
        #[clap(long)]
        action: String,
        #[clap(long)]
        resource: String,
    },
    /// Revoke an application permission scope
    Revoke {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        app_name: String,
        #[clap(long)]
        action: String,
        #[clap(long)]
        resource: String,
    },
}

#[derive(Subcommand)]
pub enum SecretEncryptionKeyCommands {
    /// Re-encrypt existing server-side secret envelopes with the active configured key
    Rotate {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        dry_run: bool,
    },
}

#[derive(Subcommand)]
pub enum BucketCommands {
    /// Create a bucket for a tenant
    Create {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long)]
        region: String,
    },
    /// Manage public access
    PublicAccess {
        #[clap(subcommand)]
        command: BucketPublicAccessCommands,
    },
}

#[derive(Subcommand)]
pub enum BucketPublicAccessCommands {
    /// Set public read access on a bucket
    Set {
        #[clap(flatten)]
        context: MutationOptions,
        #[clap(long)]
        tenant_id: String,
        #[clap(long)]
        bucket_name: String,
        #[clap(long, action = clap::ArgAction::Set, value_parser = clap::builder::BoolishValueParser::new())]
        allow: bool,
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
        #[clap(long)]
        activation_checkpoint: PathBuf,
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
    /// AdminRequestContext.expected_generation. Required for update/delete commands; create/register commands default to 0.
    #[clap(long)]
    expected_generation: Option<u64>,
}

impl MutationOptions {
    fn to_create_context(&self) -> anyhow::Result<api::AdminRequestContext> {
        let expected_generation = self.expected_generation.unwrap_or(0);
        if expected_generation != 0 {
            anyhow::bail!(
                "create/register commands must use --expected-generation 0 when supplied"
            );
        }
        Ok(self.context_with_generation(expected_generation))
    }

    fn to_update_context(&self) -> anyhow::Result<api::AdminRequestContext> {
        let Some(expected_generation) = self.expected_generation else {
            anyhow::bail!("--expected-generation is required for update/delete lifecycle commands");
        };
        if expected_generation == 0 {
            anyhow::bail!("update/delete commands must use a non-zero --expected-generation");
        }
        Ok(self.context_with_generation(expected_generation))
    }

    fn to_action_context(&self) -> api::AdminRequestContext {
        self.context_with_generation(self.expected_generation.unwrap_or(0))
    }

    fn context_with_generation(&self, expected_generation: u64) -> api::AdminRequestContext {
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
            expected_generation,
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

#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RoutingRecordFamilyArg {
    TenantName,
    TenantLocator,
    BucketLocator,
    HostAlias,
}

impl RoutingRecordFamilyArg {
    fn to_proto(self) -> i32 {
        match self {
            Self::TenantName => 1,
            Self::TenantLocator => 2,
            Self::BucketLocator => 3,
            Self::HostAlias => 4,
        }
    }
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
    fn to_proto(self) -> i32 {
        match self {
            Self::Index => 1,
            Self::DirectoryIndex => 2,
            Self::AuthzDerivedIndex => 3,
            Self::PersonaldbLogChain => 4,
            Self::MeshRoutingProjection => 5,
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
            expires_at: String::new(),
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
    }

    Ok(())
}

async fn handle_tenant_command(
    command: &TenantCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        TenantCommands::Create {
            context,
            name,
            home_region,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "tenant",
                Some(&admin_context),
                None,
                client.create_tenant(with_auth(
                    api::CreateTenantRequest {
                        context: Some(admin_context.clone()),
                        name: name.clone(),
                        home_region: home_region.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}

async fn handle_app_command(
    command: &AppCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        AppCommands::Create {
            context,
            tenant_id,
            app_name,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "application",
                Some(&admin_context),
                None,
                client.create_application(with_auth(
                    api::CreateApplicationRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        AppCommands::RotateSecret {
            context,
            tenant_id,
            app_name,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "application",
                Some(&admin_context),
                None,
                client.rotate_application_secret(with_auth(
                    api::RotateApplicationSecretRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}

async fn handle_policy_command(
    command: &PolicyCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        PolicyCommands::Grant {
            context,
            tenant_id,
            app_name,
            action,
            resource,
        } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "application_policy",
                Some(&admin_context),
                None,
                client.grant_application_policy(with_auth(
                    api::GrantApplicationPolicyRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                        action: action.clone(),
                        resource: resource.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        PolicyCommands::Revoke {
            context,
            tenant_id,
            app_name,
            action,
            resource,
        } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "application_policy",
                Some(&admin_context),
                None,
                client.revoke_application_policy(with_auth(
                    api::RevokeApplicationPolicyRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        app_name: app_name.clone(),
                        action: action.clone(),
                        resource: resource.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}

async fn handle_secret_encryption_key_command(
    command: &SecretEncryptionKeyCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        SecretEncryptionKeyCommands::Rotate { context, dry_run } => {
            let admin_context = context.to_action_context();
            print_rpc_response(
                "secret_encryption_key_rotation",
                Some(&admin_context),
                None,
                client.rotate_secret_encryption_key(with_auth(
                    api::RotateSecretEncryptionKeyRequest {
                        context: Some(admin_context.clone()),
                        dry_run: *dry_run,
                    },
                    token,
                )?),
            )
            .await?;
        }
    }
    Ok(())
}

async fn handle_bucket_command(
    command: &BucketCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        BucketCommands::Create {
            context,
            tenant_id,
            bucket_name,
            region,
        } => {
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "bucket",
                Some(&admin_context),
                None,
                client.create_bucket_admin(with_auth(
                    api::CreateBucketAdminRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        region: region.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        BucketCommands::PublicAccess {
            command:
                BucketPublicAccessCommands::Set {
                    context,
                    tenant_id,
                    bucket_name,
                    allow,
                },
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "bucket",
                Some(&admin_context),
                None,
                client.set_bucket_public_access_admin(with_auth(
                    api::SetBucketPublicAccessAdminRequest {
                        context: Some(admin_context.clone()),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        allow_public_read: *allow,
                    },
                    token,
                )?),
            )
            .await?;
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
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "region",
                Some(&admin_context),
                None,
                client.create_region(with_auth(
                    api::CreateRegionRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        public_base_url: public_base_url.clone(),
                        virtual_host_suffix: virtual_host_suffix.clone(),
                        placement_weight: *placement_weight,
                        default_cell: default_cell.clone().unwrap_or_default(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        RegionCommands::Activate {
            context,
            region,
            activation_checkpoint,
        } => {
            let activation_checkpoint_json = tokio::fs::read_to_string(activation_checkpoint)
                .await
                .map_err(|err| {
                    anyhow::anyhow!(
                        "failed to read activation checkpoint {}: {err}",
                        activation_checkpoint.display()
                    )
                })?;
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "region",
                Some(&admin_context),
                None,
                client.activate_region(with_auth(
                    api::ActivateRegionRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        activation_checkpoint_json,
                    },
                    token,
                )?),
            )
            .await?;
        }
        RegionCommands::SetReadOnly { context, region } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "region",
                Some(&admin_context),
                None,
                client.set_region_read_only(with_auth(
                    api::SetRegionReadOnlyRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        RegionCommands::Drain {
            context,
            region,
            default_disposition,
            bucket_overrides,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "region",
                Some(&admin_context),
                None,
                client.drain_region(with_auth(
                    api::DrainRegionRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                        default_disposition: default_disposition.to_proto(),
                        bucket_overrides: bucket_overrides
                            .iter()
                            .map(BucketDrainOverrideArg::to_proto)
                            .collect(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        RegionCommands::Remove { context, region } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "region",
                Some(&admin_context),
                None,
                client.remove_region(with_auth(
                    api::RemoveRegionRequest {
                        context: Some(admin_context.clone()),
                        region: region.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        RegionCommands::List { page } => {
            print_rpc_response(
                "regions",
                None,
                None,
                client.list_regions(with_auth(
                    api::ListRegionsRequest {
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
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.register_node(with_auth(
                    api::RegisterNodeRequest {
                        context: Some(admin_context.clone()),
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
                )?),
            )
            .await?;
        }
        NodeCommands::Activate { context, node_id } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.activate_node(with_auth(
                    api::ActivateNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::Drain {
            context,
            node_id,
            graceful_timeout_ms,
            force_after_timeout,
        } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.drain_node(with_auth(
                    api::DrainNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                        graceful_timeout_ms: *graceful_timeout_ms,
                        force_after_timeout: *force_after_timeout,
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::ForceOffline { context, node_id } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.force_offline_node(with_auth(
                    api::ForceOfflineNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::Remove { context, node_id } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "node",
                Some(&admin_context),
                None,
                client.remove_node(with_auth(
                    api::RemoveNodeRequest {
                        context: Some(admin_context.clone()),
                        node_id: node_id.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        NodeCommands::List {
            region,
            cell_id,
            page,
        } => {
            print_rpc_response(
                "nodes",
                None,
                None,
                client.list_nodes(with_auth(
                    api::ListNodesRequest {
                        region: region.clone().unwrap_or_default(),
                        cell_id: cell_id.clone().unwrap_or_default(),
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
            let admin_context = context.to_create_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.create_host_alias(with_auth(
                    api::CreateHostAliasAdminRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                        tenant_id: tenant_id.clone(),
                        bucket_name: bucket_name.clone(),
                        region: region.clone(),
                        prefix: prefix.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Activate { context, hostname } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.activate_host_alias(with_auth(
                    api::ActivateHostAliasRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Suspend { context, hostname } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.suspend_host_alias(with_auth(
                    api::SuspendHostAliasRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Delete { context, hostname } => {
            let admin_context = context.to_update_context()?;
            print_rpc_response(
                "host_alias",
                Some(&admin_context),
                None,
                client.delete_host_alias(with_auth(
                    api::DeleteHostAliasAdminRequest {
                        context: Some(admin_context.clone()),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::Read {
            request_id,
            hostname,
        } => {
            let request_id = request_id_or_cli(request_id);
            print_rpc_response(
                "host_alias",
                None,
                Some(&request_id),
                client.read_host_alias(with_auth(
                    api::ReadHostAliasRequest {
                        request_id: request_id.clone(),
                        hostname: hostname.clone(),
                    },
                    token,
                )?),
            )
            .await?;
        }
        HostAliasCommands::List { region, page } => {
            print_rpc_response(
                "host_aliases",
                None,
                None,
                client.list_host_aliases(with_auth(
                    api::ListHostAliasesRequest {
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

async fn handle_routing_command(
    command: &RoutingCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
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

async fn handle_repair_command(
    command: &RepairCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
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

async fn handle_diagnostics_command(
    command: &DiagnosticsCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
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

async fn handle_audit_command(
    command: &AuditCommands,
    client: &mut AdminServiceClient<tonic::transport::Channel>,
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

fn request_id_or_cli(request_id: &Option<String>) -> String {
    request_id
        .clone()
        .unwrap_or_else(|| format!("cli-{}", uuid::Uuid::new_v4()))
}

async fn print_rpc_response<T, F>(
    resource_type: &'static str,
    context: Option<&api::AdminRequestContext>,
    request_id: Option<&str>,
    rpc: F,
) -> anyhow::Result<()>
where
    T: Serialize,
    F: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
{
    match rpc.await {
        Ok(response) => print_admin_success(resource_type, &response.into_inner(), context),
        Err(status) => {
            print_admin_error(resource_type, context, request_id, &status)?;
            Err(status.into())
        }
    }
}

#[derive(Serialize)]
struct AdminCliJsonOutput {
    schema: &'static str,
    request_id: String,
    ok: bool,
    resource_type: String,
    resource: Option<Value>,
    generation: Option<u64>,
    audit_event_id: String,
    idempotency_key: Option<String>,
    error: Option<AdminCliJsonError>,
}

#[derive(Serialize)]
struct AdminCliJsonError {
    request_id: String,
    code: String,
    message: String,
    resource_id: String,
    current_generation: u64,
}

fn print_admin_success<T: Serialize>(
    resource_type: &'static str,
    value: &T,
    context: Option<&api::AdminRequestContext>,
) -> anyhow::Result<()> {
    let value = serde_json::to_value(value)?;
    let resource = admin_cli_resource(&value);
    let request_id = value
        .get("request_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .or_else(|| context.map(|context| context.request_id.as_str()))
        .unwrap_or_default()
        .to_string();
    let audit_event_id = value
        .get("audit_event_id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let generation = value.get("generation").and_then(Value::as_u64).or_else(|| {
        resource
            .as_ref()
            .and_then(|resource| resource.get("generation"))
            .and_then(Value::as_u64)
    });
    let output = AdminCliJsonOutput {
        schema: "anvil.admin_cli.output.v1",
        request_id,
        ok: true,
        resource_type: resource_type.to_string(),
        resource,
        generation,
        audit_event_id,
        idempotency_key: context.map(|context| context.idempotency_key.clone()),
        error: None,
    };
    print_json(&output)
}

fn print_admin_error(
    resource_type: &'static str,
    context: Option<&api::AdminRequestContext>,
    request_id: Option<&str>,
    status: &tonic::Status,
) -> anyhow::Result<()> {
    let request_id = request_id
        .filter(|value| !value.is_empty())
        .or_else(|| context.map(|context| context.request_id.as_str()))
        .unwrap_or_default()
        .to_string();
    let output = AdminCliJsonOutput {
        schema: "anvil.admin_cli.output.v1",
        request_id: request_id.clone(),
        ok: false,
        resource_type: resource_type.to_string(),
        resource: None,
        generation: None,
        audit_event_id: String::new(),
        idempotency_key: context.map(|context| context.idempotency_key.clone()),
        error: Some(AdminCliJsonError {
            request_id,
            code: format!("{:?}", status.code()),
            message: status.message().to_string(),
            resource_id: String::new(),
            current_generation: 0,
        }),
    };
    print_json(&output)
}

fn admin_cli_resource(value: &Value) -> Option<Value> {
    for field in [
        "tenant",
        "bucket",
        "link",
        "host_alias",
        "region",
        "cell",
        "node",
    ] {
        if let Some(resource) = value.get(field) {
            return Some(resource.clone());
        }
    }
    if let Some(resource_id) = value.get("resource_id") {
        return Some(json!({
            "resource_id": resource_id,
            "generation": value.get("generation").cloned().unwrap_or(Value::Null),
            "idempotent_replay": value
                .get("idempotent_replay")
                .cloned()
                .unwrap_or(Value::Bool(false)),
        }));
    }
    Some(value.clone())
}

fn print_json<T: Serialize>(value: &T) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer_pretty(&mut stdout, value)?;
    writeln!(stdout)?;
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
            bootstrap_system_admin_subject_kind: "app".to_string(),
            bootstrap_system_admin_subject_id: "cli-admin-principal".to_string(),
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
            expected_generation: Some(expected_generation),
        }
    }

    fn admin_token(node: &AdminCliNode) -> String {
        node.state
            .jwt_manager
            .mint_token("cli-admin-principal".to_string(), Vec::new(), 0)
            .unwrap()
    }

    async fn write_activation_checkpoint_from_existing_streams(
        node: &AdminCliNode,
        file_name: &str,
    ) -> PathBuf {
        let path = node._temp.path().join(file_name);
        let mut required_streams = Vec::new();
        let stream_families = anvil::mesh_directory::RoutingRecordFamily::all()
            .into_iter()
            .map(|family| family.stream_family())
            .chain(anvil::mesh_lifecycle::lifecycle_control_stream_families().into_iter());
        for stream_family in stream_families {
            let partitions = anvil::mesh_control_stream::list_control_stream_partitions(
                &node.state.storage,
                stream_family,
            )
            .await
            .unwrap();
            for partition in partitions {
                let log = anvil::mesh_control_stream::read_control_stream_log(
                    &node.state.storage,
                    stream_family,
                    &partition,
                )
                .await
                .unwrap();
                let Some(record) = log.records.last() else {
                    continue;
                };
                anvil::mesh_control_stream::write_control_checkpoint(
                    &node.state.storage,
                    &anvil::mesh_control_stream::ControlCheckpointRecord::new(
                        "mesh-cli-test",
                        "eu-west-1",
                        stream_family,
                        &partition,
                        record.metadata.sequence,
                        record.metadata.record_digest.clone(),
                        "2026-07-02T00:00:00Z",
                    ),
                )
                .await
                .unwrap();
                required_streams.push(serde_json::json!({
                    "stream_family": stream_family,
                    "partition": partition,
                    "sequence": record.metadata.sequence.get(),
                    "digest": record.metadata.record_digest.as_str(),
                }));
            }
        }
        std::fs::write(
            &path,
            serde_json::json!({
                "schema": anvil::mesh_lifecycle::ACTIVATION_CHECKPOINT_SCHEMA,
                "mesh_id": "mesh-cli-test",
                "region": "eu-west-1",
                "created_at": "2026-07-02T00:00:00Z",
                "required_streams": required_streams
            })
            .to_string(),
        )
        .unwrap();
        path
    }

    #[test]
    fn mutation_options_generate_optional_ids() {
        let context = MutationOptions {
            request_id: None,
            idempotency_key: None,
            audit_reason: "planned maintenance".to_string(),
            expected_generation: Some(42),
        }
        .to_action_context();

        assert!(context.request_id.starts_with("cli-"));
        assert!(!context.idempotency_key.is_empty());
        assert_eq!(context.audit_reason, "planned maintenance");
        assert_eq!(context.expected_generation, 42);
    }

    #[test]
    fn mutation_context_helpers_enforce_generation_contract() {
        let create = MutationOptions {
            request_id: Some("req-create".to_string()),
            idempotency_key: Some("idem-create".to_string()),
            audit_reason: "create resource".to_string(),
            expected_generation: None,
        };
        assert_eq!(create.to_create_context().unwrap().expected_generation, 0);
        assert!(create.to_update_context().is_err());

        let update = MutationOptions {
            request_id: Some("req-update".to_string()),
            idempotency_key: Some("idem-update".to_string()),
            audit_reason: "update resource".to_string(),
            expected_generation: Some(7),
        };
        assert_eq!(update.to_update_context().unwrap().expected_generation, 7);
        assert!(update.to_create_context().is_err());
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
        assert_eq!(context.expected_generation, Some(0));
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
        assert_eq!(context.expected_generation, Some(7));
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
        assert_eq!(context.expected_generation, Some(11));
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
        assert_eq!(context.expected_generation, Some(12));
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
        assert_eq!(context.expected_generation, Some(1));
        assert_eq!(family.to_proto(), 1);
        assert_eq!(record_key, "acme");
    }

    #[test]
    fn repair_diagnostics_and_audit_commands_parse() {
        let repair_cli = TestAdminCli::try_parse_from([
            "admin",
            "repair",
            "run",
            "--audit-reason",
            "verify directory",
            "--expected-generation",
            "0",
            "--repair-kind",
            "directory-index",
            "--tenant-id",
            "acme",
            "--bucket-name",
            "releases",
            "--rebuild",
        ])
        .unwrap();
        let AdminCommands::Repair {
            command:
                RepairCommands::Run {
                    context,
                    repair_kind,
                    tenant_id,
                    bucket_name,
                    rebuild,
                    ..
                },
        } = repair_cli.command
        else {
            panic!("expected repair run command");
        };
        assert_eq!(context.audit_reason, "verify directory");
        assert_eq!(context.expected_generation, Some(0));
        assert_eq!(repair_kind.to_proto(), 2);
        assert_eq!(tenant_id, "acme");
        assert_eq!(bucket_name.as_deref(), Some("releases"));
        assert!(rebuild);

        let diagnostics_cli = TestAdminCli::try_parse_from([
            "admin",
            "diagnostics",
            "list",
            "--request-id",
            "req-diag",
            "--source",
            "index",
            "--tenant-id",
            "acme",
            "--bucket-name",
            "releases",
            "--severity",
            "warning",
            "--limit",
            "10",
        ])
        .unwrap();
        let AdminCommands::Diagnostics {
            command:
                DiagnosticsCommands::List {
                    request_id,
                    source,
                    tenant_id,
                    bucket_name,
                    severity,
                    page,
                    ..
                },
        } = diagnostics_cli.command
        else {
            panic!("expected diagnostics list command");
        };
        assert_eq!(request_id.as_deref(), Some("req-diag"));
        assert_eq!(source.as_deref(), Some("index"));
        assert_eq!(tenant_id.as_deref(), Some("acme"));
        assert_eq!(bucket_name.as_deref(), Some("releases"));
        assert_eq!(severity.as_deref(), Some("warning"));
        assert_eq!(page.limit, Some(10));

        let audit_cli = TestAdminCli::try_parse_from([
            "admin",
            "audit",
            "list",
            "--request-id",
            "req-audit",
            "--principal-id",
            "admin-a",
            "--resource-id",
            "bucket/releases",
            "--action",
            "run_repair",
        ])
        .unwrap();
        let AdminCommands::Audit {
            command:
                AuditCommands::List {
                    request_id,
                    principal_id,
                    resource_id,
                    action,
                    ..
                },
        } = audit_cli.command
        else {
            panic!("expected audit list command");
        };
        assert_eq!(request_id.as_deref(), Some("req-audit"));
        assert_eq!(principal_id.as_deref(), Some("admin-a"));
        assert_eq!(resource_id.as_deref(), Some("bucket/releases"));
        assert_eq!(action.as_deref(), Some("run_repair"));
    }

    #[test]
    fn tenant_app_and_bucket_admin_commands_parse() {
        let tenant_cli = TestAdminCli::try_parse_from([
            "admin",
            "tenant",
            "create",
            "--audit-reason",
            "create tenant",
            "--expected-generation",
            "0",
            "--name",
            "acme",
            "--home-region",
            "eu-west-1",
        ])
        .unwrap();
        let AdminCommands::Tenant {
            command:
                TenantCommands::Create {
                    context,
                    name,
                    home_region,
                },
        } = tenant_cli.command
        else {
            panic!("expected tenant create command");
        };
        assert_eq!(context.audit_reason, "create tenant");
        assert_eq!(name, "acme");
        assert_eq!(home_region, "eu-west-1");

        let app_cli = TestAdminCli::try_parse_from([
            "admin",
            "app",
            "rotate-secret",
            "--audit-reason",
            "rotate app",
            "--expected-generation",
            "1",
            "--tenant-id",
            "acme",
            "--app-name",
            "publisher",
        ])
        .unwrap();
        let AdminCommands::App {
            command:
                AppCommands::RotateSecret {
                    context,
                    tenant_id,
                    app_name,
                },
        } = app_cli.command
        else {
            panic!("expected app rotate-secret command");
        };
        assert_eq!(context.expected_generation, Some(1));
        assert_eq!(tenant_id, "acme");
        assert_eq!(app_name, "publisher");

        let bucket_cli = TestAdminCli::try_parse_from([
            "admin",
            "bucket",
            "public-access",
            "set",
            "--audit-reason",
            "publish bucket",
            "--expected-generation",
            "1",
            "--tenant-id",
            "acme",
            "--bucket-name",
            "releases",
            "--allow",
            "true",
        ])
        .unwrap();
        let AdminCommands::Bucket {
            command:
                BucketCommands::PublicAccess {
                    command:
                        BucketPublicAccessCommands::Set {
                            context,
                            tenant_id,
                            bucket_name,
                            allow,
                        },
                },
        } = bucket_cli.command
        else {
            panic!("expected bucket public-access set command");
        };
        assert_eq!(context.audit_reason, "publish bucket");
        assert_eq!(tenant_id, "acme");
        assert_eq!(bucket_name, "releases");
        assert!(allow);
    }

    #[tokio::test]
    async fn admin_repair_diagnostics_and_audit_handlers_return_structured_responses() {
        let node = spawn_admin_cli_node().await;
        let token = admin_token(&node);
        let mut client = AdminServiceClient::connect(node.admin_url.clone())
            .await
            .unwrap();

        client
            .create_tenant(
                with_auth(
                    api::CreateTenantRequest {
                        context: Some(
                            mutation_options("admin-diag-tenant", 0)
                                .to_create_context()
                                .unwrap(),
                        ),
                        name: "acme".to_string(),
                        home_region: "eu-west-1".to_string(),
                    },
                    &token,
                )
                .unwrap(),
            )
            .await
            .unwrap();
        client
            .create_bucket_admin(
                with_auth(
                    api::CreateBucketAdminRequest {
                        context: Some(
                            mutation_options("admin-diag-bucket", 0)
                                .to_create_context()
                                .unwrap(),
                        ),
                        tenant_id: "acme".to_string(),
                        bucket_name: "releases".to_string(),
                        region: "eu-west-1".to_string(),
                    },
                    &token,
                )
                .unwrap(),
            )
            .await
            .unwrap();

        let repair = client
            .run_repair(
                with_auth(
                    api::RunRepairRequest {
                        context: Some(
                            mutation_options("admin-directory-repair", 0).to_action_context(),
                        ),
                        repair_kind: RepairKindArg::DirectoryIndex.to_proto(),
                        tenant_id: "acme".to_string(),
                        bucket_name: "releases".to_string(),
                        index_name: String::new(),
                        derived_index_id: String::new(),
                        database_id: String::new(),
                        rebuild: false,
                    },
                    &token,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(repair.request_id, "req-admin-directory-repair");
        assert_eq!(repair.status, "empty_source");
        assert_eq!(repair.scope_kind, "bucket");
        assert!(repair.findings.is_empty());
        assert!(repair.audit_event_id.contains("req-admin-directory-repair"));

        let diagnostics = client
            .list_diagnostics(
                with_auth(
                    api::ListDiagnosticsRequest {
                        request_id: "req-admin-diagnostics".to_string(),
                        source: "index".to_string(),
                        tenant_id: "acme".to_string(),
                        bucket_name: "releases".to_string(),
                        index_name: String::new(),
                        severity: String::new(),
                        page: Some(api::PageRequest {
                            cursor: String::new(),
                            limit: 5,
                        }),
                    },
                    &token,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(diagnostics.request_id, "req-admin-diagnostics");
        assert_eq!(diagnostics.data_source, "index_diagnostic_journal");
        assert!(diagnostics.diagnostics.is_empty());
        assert!(!diagnostics.page.unwrap().has_more);

        let audit = client
            .list_audit_events(
                with_auth(
                    api::ListAuditEventsRequest {
                        request_id: "req-admin-audit".to_string(),
                        principal_id: String::new(),
                        resource_id: String::new(),
                        action: "admin.repair.run".to_string(),
                        page: Some(api::PageRequest {
                            cursor: String::new(),
                            limit: 5,
                        }),
                    },
                    &token,
                )
                .unwrap(),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(audit.request_id, "req-admin-audit");
        assert_eq!(audit.data_source, "admin_audit_log");
        assert_eq!(audit.events.len(), 1);
        assert_eq!(audit.events[0].request_id, "req-admin-directory-repair");
        assert_eq!(audit.events[0].action, "admin.repair.run");
        assert!(!audit.page.unwrap().has_more);
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

        let region = node
            .state
            .persistence
            .list_region_descriptors()
            .await
            .unwrap()
            .into_iter()
            .next()
            .unwrap();
        let activation_checkpoint =
            write_activation_checkpoint_from_existing_streams(&node, "activate-region.json").await;
        handle_region_command(
            &RegionCommands::Activate {
                context: mutation_options("cli-activate-region", region.generation),
                region: "eu-west-1".to_string(),
                activation_checkpoint: activation_checkpoint.clone(),
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
                activation_checkpoint,
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
