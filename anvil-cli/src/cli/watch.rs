use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::personal_db_service_client::PersonalDbServiceClient;
use clap::Subcommand;
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum WatchCommands {
    Prefix {
        bucket: String,
        prefix: String,
        #[clap(long, default_value_t = 0)]
        after_cursor: u64,
    },
    IndexDefinition {
        bucket: String,
        #[clap(long, default_value_t = 0)]
        after_cursor: u64,
    },
    IndexPartition {
        bucket: String,
        index: String,
        partition_id: String,
        #[clap(long, default_value_t = 0)]
        after_cursor_low: u64,
        #[clap(long, default_value_t = 0)]
        after_cursor_high: u64,
    },
    Authz {
        namespace: String,
        #[clap(long, default_value_t = 0)]
        after_revision: u64,
    },
    #[command(name = "personaldb")]
    PersonalDb {
        database_id: String,
        #[clap(long, default_value_t = 0)]
        after_cursor_low: u64,
        #[clap(long, default_value_t = 0)]
        after_cursor_high: u64,
    },
}

pub async fn handle_watch_command(command: &WatchCommands, ctx: &Context) -> anyhow::Result<()> {
    let token = ctx.get_bearer_token().await?;
    match command {
        WatchCommands::Prefix {
            bucket,
            prefix,
            after_cursor,
        } => {
            let mut client = ObjectServiceClient::connect(ctx.profile.host.clone()).await?;
            let mut request = tonic::Request::new(api::WatchPrefixRequest {
                bucket_name: bucket.clone(),
                prefix: prefix.clone(),
                after_cursor: *after_cursor,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_prefix(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!("{}\t{}\t{}", item.cursor, item.event_type, item.object_key);
            }
        }
        WatchCommands::IndexDefinition {
            bucket,
            after_cursor,
        } => {
            let mut client = IndexServiceClient::connect(ctx.profile.host.clone()).await?;
            let mut request = tonic::Request::new(api::WatchIndexDefinitionRequest {
                bucket_name: bucket.clone(),
                after_cursor: *after_cursor,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_index_definition(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!("{}\t{}", item.cursor, item.event_type);
            }
        }
        WatchCommands::IndexPartition {
            bucket,
            index,
            partition_id,
            after_cursor_low,
            after_cursor_high,
        } => {
            let mut client = IndexServiceClient::connect(ctx.profile.host.clone()).await?;
            let mut request = tonic::Request::new(api::WatchIndexPartitionRequest {
                bucket_name: bucket.clone(),
                index_name: index.clone(),
                partition_id: partition_id.clone(),
                after_cursor_low: *after_cursor_low,
                after_cursor_high: *after_cursor_high,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_index_partition(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!(
                    "{}:{}\t{}",
                    item.cursor_low, item.cursor_high, item.event_type
                );
            }
        }
        WatchCommands::Authz {
            namespace,
            after_revision,
        } => {
            let mut client = AuthServiceClient::connect(ctx.profile.host.clone()).await?;
            let mut request = tonic::Request::new(api::WatchAuthzTupleLogRequest {
                after_revision: *after_revision,
                namespace: namespace.clone(),
                scope: None,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_authz_tuple_log(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!(
                    "{}\t{}\t{}:{}#{}",
                    item.revision, item.operation, item.namespace, item.object_id, item.relation
                );
            }
        }
        WatchCommands::PersonalDb {
            database_id,
            after_cursor_low,
            after_cursor_high,
        } => {
            let claims = crate::cli::object::decode_native_token_claims(&token)?;
            let mut client = PersonalDbServiceClient::connect(ctx.profile.host.clone()).await?;
            let mut request = tonic::Request::new(api::WatchPersonalDbGroupRequest {
                tenant_id: claims.tenant_id,
                database_id: database_id.clone(),
                after_cursor_low: *after_cursor_low,
                after_cursor_high: *after_cursor_high,
            });
            add_auth(&mut request, &token);
            let mut stream = client.watch_personal_db_group(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                println!(
                    "{}:{}\t{}",
                    item.cursor_low, item.cursor_high, item.event_type
                );
            }
        }
    }
    Ok(())
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
