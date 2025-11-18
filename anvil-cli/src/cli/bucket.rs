use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use clap::Subcommand;

#[derive(Subcommand)]
pub enum BucketCommands {
    /// Create a new bucket
    Create { name: String, region: String },
    /// Remove a bucket
    Rm { name: String },
    /// List buckets
    Ls,
    /// Set public access for a bucket
    SetPublic {
        name: String,
        #[clap(long, action = clap::ArgAction::Set)]
        allow: bool,
    },
}

pub async fn handle_bucket_command(command: &BucketCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = BucketServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        BucketCommands::Create { name, region } => {
            let mut request = tonic::Request::new(api::CreateBucketRequest {
                bucket_name: name.clone(),
                region: region.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.create_bucket(request).await?;
            println!("Bucket {} created", name);
        }
        BucketCommands::Rm { name } => {
            let mut request = tonic::Request::new(api::DeleteBucketRequest {
                bucket_name: name.clone(),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.delete_bucket(request).await?;
            println!("Bucket {} deleted", name);
        }
        BucketCommands::Ls => {
            let mut request = tonic::Request::new(api::ListBucketsRequest {});
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let resp = client.list_buckets(request).await?;
            for bucket in resp.into_inner().buckets {
                println!("{}\t{}", bucket.name, bucket.creation_date);
            }
        }
        BucketCommands::SetPublic { name, allow } => {
            let mut request = tonic::Request::new(api::PutBucketPolicyRequest {
                bucket_name: name.clone(),
                policy_json: format!("{{\"is_public_read\": {}}}", allow),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.put_bucket_policy(request).await?;
            println!("Public access for bucket {} set to {}", name, allow);
        }
    }
    Ok(())
}
