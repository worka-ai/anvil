use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use clap::Subcommand;
use tokio_stream::iter;

#[derive(Subcommand)]
pub enum ObjectCommands {
    /// Upload a file to an object
    Put { src: String, dest: String },
    /// Download an object to a file or stdout
    Get { src: String, dest: Option<String> },
    /// Remove an object
    Rm { path: String },
    /// List objects in a bucket
    Ls { path: String },
    /// Show object metadata
    Head { path: String },
}

fn parse_s3_path(path: &str) -> anyhow::Result<(String, String)> {
    let path = path.strip_prefix("s3://").unwrap_or(path);
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid S3 path"));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

pub async fn handle_object_command(command: &ObjectCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = ObjectServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        ObjectCommands::Put { src, dest } => {
            let (bucket, key) = parse_s3_path(dest)?;
            let metadata = api::ObjectMetadata {
                bucket_name: bucket,
                object_key: key,
            };
            let file_chunks = tokio::fs::read(src).await?;
            let chunks = vec![
                api::PutObjectRequest {
                    data: Some(api::put_object_request::Data::Metadata(metadata)),
                },
                api::PutObjectRequest {
                    data: Some(api::put_object_request::Data::Chunk(file_chunks)),
                },
            ];
            let mut request = tonic::Request::new(iter(chunks));
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.put_object(request).await?;
            println!("Uploaded {} to {}", src, dest);
        }
        ObjectCommands::Get { src, dest } => {
            let (bucket, key) = parse_s3_path(src)?;
            let mut request = tonic::Request::new(api::GetObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let mut stream = client.get_object(request).await?.into_inner();

            if let Some(dest_path) = dest {
                let mut file = tokio::fs::File::create(dest_path).await?;
                while let Some(chunk) = stream.message().await? {
                    if let Some(api::get_object_response::Data::Chunk(bytes)) = chunk.data {
                        tokio::io::AsyncWriteExt::write_all(&mut file, &bytes).await?;
                    }
                }
                println!("Downloaded {} to {}", src, dest_path);
            } else {
                while let Some(chunk) = stream.message().await? {
                    if let Some(api::get_object_response::Data::Chunk(bytes)) = chunk.data {
                        print!("{}", String::from_utf8_lossy(&bytes));
                    }
                }
            }
        }
        ObjectCommands::Rm { path } => {
            let (bucket, key) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::DeleteObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            client.delete_object(request).await?;
            println!("Removed {}", path);
        }
        ObjectCommands::Ls { path } => {
            let (bucket, prefix) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::ListObjectsRequest {
                bucket_name: bucket,
                prefix,
                ..Default::default()
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let resp = client.list_objects(request).await?;
            for obj in resp.into_inner().objects {
                println!("{}\t{}\t{}", obj.last_modified, obj.size, obj.key);
            }
        }
        ObjectCommands::Head { path } => {
            let (bucket, key) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::HeadObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let resp = client.head_object(request).await?;
            let obj = resp.into_inner();
            println!(
                "ETag: {}\nSize: {}\nLast Modified: {}",
                obj.etag, obj.size, obj.last_modified
            );
        }
    }

    Ok(())
}
