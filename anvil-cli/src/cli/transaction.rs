use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::transaction_service_client::TransactionServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum TransactionCommands {
    /// Begin an explicit single-root transaction.
    Begin {
        #[clap(long)]
        root_anchor_key: String,
        #[clap(long)]
        root_key_hash: String,
        #[clap(long, default_value_t = 60_000)]
        ttl_ms: u64,
        #[clap(long, default_value = "manual")]
        purpose: String,
        #[clap(long)]
        idempotency_key: Option<String>,
    },
    /// Commit an explicit transaction.
    Commit {
        transaction_id: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        wait_for_finalization: bool,
    },
    /// Roll back an explicit transaction.
    Rollback {
        transaction_id: String,
        #[clap(long, default_value = "cli-request")]
        reason: String,
    },
    /// Read explicit transaction status.
    Get { transaction_id: String },
}

pub async fn handle_transaction_command(
    command: &TransactionCommands,
    ctx: &Context,
) -> anyhow::Result<()> {
    let mut client = TransactionServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        TransactionCommands::Begin {
            root_anchor_key,
            root_key_hash,
            ttl_ms,
            purpose,
            idempotency_key,
        } => {
            let mut request = tonic::Request::new(api::BeginTransactionRequest {
                idempotency_key: idempotency_key
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string()),
                scope: Some(api::TransactionScope {
                    root_anchor_key: root_anchor_key.clone(),
                    root_key_hash: root_key_hash.clone(),
                }),
                preconditions: Vec::new(),
                boundary_values: Vec::new(),
                ttl_ms: *ttl_ms,
                purpose: purpose.clone(),
            });
            attach_bearer(&mut request, &token);
            let response = client.begin_transaction(request).await?.into_inner();
            println!("transaction_id={}", response.transaction_id);
            println!("state={}", response.state);
            println!("expires_at_unix_nanos={}", response.expires_at_unix_nanos);
        }
        TransactionCommands::Commit {
            transaction_id,
            wait_for_finalization,
        } => {
            let mut request = tonic::Request::new(api::CommitTransactionRequest {
                transaction_id: transaction_id.clone(),
                consistency: if *wait_for_finalization {
                    api::ConsistencyMode::Finalised as i32
                } else {
                    api::ConsistencyMode::Committed as i32
                },
                wait_for_finalization: *wait_for_finalization,
                final_preconditions: Vec::new(),
            });
            attach_bearer(&mut request, &token);
            let response = client.commit_transaction(request).await?.into_inner();
            println!("mutation_id={}", response.mutation_id);
            println!("state={:?}", api::WriteState::try_from(response.state));
            if let Some(root_generation) = response.root_generation {
                println!("root_generation={root_generation}");
            }
        }
        TransactionCommands::Rollback {
            transaction_id,
            reason,
        } => {
            let mut request = tonic::Request::new(api::RollbackTransactionRequest {
                transaction_id: transaction_id.clone(),
                reason: reason.clone(),
            });
            attach_bearer(&mut request, &token);
            let response = client.rollback_transaction(request).await?.into_inner();
            println!("transaction_id={}", response.transaction_id);
            println!("state={}", response.state);
        }
        TransactionCommands::Get { transaction_id } => {
            let mut request = tonic::Request::new(api::GetTransactionRequest {
                transaction_id: transaction_id.clone(),
            });
            attach_bearer(&mut request, &token);
            let response = client.get_transaction(request).await?.into_inner();
            println!("transaction_id={}", response.transaction_id);
            println!("state={}", response.state);
            println!("root_key_hash={}", response.root_key_hash);
            if let Some(root_generation) = response.committed_root_generation {
                println!("committed_root_generation={root_generation}");
            }
            if let Some(error) = response.error {
                println!("error={} {}", error.code, error.message);
            }
        }
    }
    Ok(())
}

fn attach_bearer<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
