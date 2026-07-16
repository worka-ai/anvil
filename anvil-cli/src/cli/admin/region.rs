use super::common::{
    AdminClient, MutationOptions, PageOptions, normalize_enum_value, print_rpc_response,
    required_part, with_auth,
};
use anvil::anvil_api as api;
use clap::{Subcommand, ValueEnum};
use std::path::PathBuf;
use std::str::FromStr;

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
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum RegionDrainDispositionArg {
    BlockUntilEmpty,
    RemainProxyOnly,
    ReadOnlyUntilRemoved,
    DeleteAfterRetention,
}
impl RegionDrainDispositionArg {
    pub(super) fn to_proto(self) -> i32 {
        match self {
            Self::BlockUntilEmpty => 1,
            Self::RemainProxyOnly => 2,
            Self::ReadOnlyUntilRemoved => 3,
            Self::DeleteAfterRetention => 4,
        }
    }

    pub(super) fn parse(value: &str) -> Result<Self, String> {
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
    pub(super) fn to_proto(&self) -> api::BucketDrainOverride {
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

pub(super) async fn handle_region_command(
    command: &RegionCommands,
    client: &mut AdminClient,
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
