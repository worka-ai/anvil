use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use clap::Subcommand;
use serde::Deserialize;
use tokio::io::AsyncWriteExt;
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
    /// Manage object links.
    Link {
        #[clap(subcommand)]
        command: ObjectLinkCommands,
    },
}

#[derive(Subcommand)]
pub enum ObjectLinkCommands {
    /// Create a link inside a bucket.
    Create {
        link: String,
        target: String,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        allow_dangling: bool,
        #[clap(long, default_value = "follow")]
        resolution: String,
    },
    /// Update an existing link.
    Update {
        link: String,
        target: String,
        #[clap(long)]
        expected_generation: u64,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        allow_dangling: bool,
        #[clap(long, default_value = "follow")]
        resolution: String,
    },
    /// Delete a link.
    Delete {
        link: String,
        #[clap(long)]
        expected_generation: u64,
    },
    /// Read link metadata.
    Read { link: String },
    /// List links in a bucket.
    List {
        path: String,
        #[clap(long, default_value_t = 100)]
        limit: u32,
    },
}

fn parse_s3_path(path: &str) -> anyhow::Result<(String, String)> {
    let path = path.strip_prefix("s3://").unwrap_or(path);
    let parts: Vec<&str> = path.splitn(2, '/').collect();
    if parts.len() != 2 {
        return Err(anyhow::anyhow!("Invalid S3 path"));
    }
    Ok((parts[0].to_string(), parts[1].to_string()))
}

#[derive(Debug, Deserialize)]
pub(crate) struct NativeTokenClaims {
    pub(crate) sub: String,
    pub(crate) tenant_id: i64,
}

pub(crate) fn decode_native_token_claims(token: &str) -> anyhow::Result<NativeTokenClaims> {
    let payload = token
        .split('.')
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("access token is not a JWT"))?;
    let bytes = URL_SAFE_NO_PAD.decode(payload)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(crate) async fn native_mutation_context(
    ctx: &Context,
    token: &str,
    bucket_name: &str,
    tag: &str,
) -> anyhow::Result<api::NativeMutationContext> {
    let claims = decode_native_token_claims(token)?;
    let mut bucket_client = BucketServiceClient::connect(ctx.profile.host.clone()).await?;
    let mut request = tonic::Request::new(api::ListBucketsRequest {});
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .list_buckets(request)
        .await?
        .into_inner()
        .buckets
        .into_iter()
        .find(|bucket| bucket.name == bucket_name)
        .map(|bucket| bucket.bucket_id)
        .ok_or_else(|| anyhow::anyhow!("bucket '{bucket_name}' not found"))?;

    Ok(api::NativeMutationContext {
        tenant_id: claims.tenant_id,
        bucket_id,
        principal: claims.sub,
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        precondition: "none".to_string(),
        authz_zookie_optional: String::new(),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
    })
}

pub async fn handle_object_command(command: &ObjectCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = ObjectServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;

    match command {
        ObjectCommands::Put { src, dest } => {
            let (bucket, key) = parse_s3_path(dest)?;
            let mutation_context = native_mutation_context(ctx, &token, &bucket, "put").await?;
            let metadata = api::ObjectMetadata {
                bucket_name: bucket,
                object_key: key,
                mutation_context: Some(mutation_context),
                content_type: None,
                user_metadata_json: String::new(),
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
                let mut expected_len = None;
                let mut bytes_written = 0_u64;
                while let Some(chunk) = stream.message().await? {
                    match chunk.data {
                        Some(api::get_object_response::Data::Metadata(info)) => {
                            expected_len = Some(u64::try_from(info.content_length)?);
                        }
                        Some(api::get_object_response::Data::Chunk(bytes)) => {
                            file.write_all(&bytes).await?;
                            bytes_written = bytes_written.saturating_add(bytes.len() as u64);
                        }
                        None => {}
                    }
                }
                file.flush().await?;
                if let Some(expected_len) = expected_len {
                    anyhow::ensure!(
                        bytes_written == expected_len,
                        "downloaded {bytes_written} bytes from {src}, expected {expected_len}"
                    );
                }
                println!("Downloaded {} to {}", src, dest_path);
            } else {
                let mut expected_len = None;
                let mut bytes_written = 0_u64;
                while let Some(chunk) = stream.message().await? {
                    match chunk.data {
                        Some(api::get_object_response::Data::Metadata(info)) => {
                            expected_len = Some(u64::try_from(info.content_length)?);
                        }
                        Some(api::get_object_response::Data::Chunk(bytes)) => {
                            bytes_written = bytes_written.saturating_add(bytes.len() as u64);
                            print!("{}", String::from_utf8_lossy(&bytes));
                        }
                        None => {}
                    }
                }
                if let Some(expected_len) = expected_len {
                    anyhow::ensure!(
                        bytes_written == expected_len,
                        "downloaded {bytes_written} bytes from {src}, expected {expected_len}"
                    );
                }
            }
        }
        ObjectCommands::Rm { path } => {
            let (bucket, key) = parse_s3_path(path)?;
            let mutation_context = native_mutation_context(ctx, &token, &bucket, "rm").await?;
            let mut request = tonic::Request::new(api::DeleteObjectRequest {
                bucket_name: bucket,
                object_key: key,
                version_id: None,
                mutation_context: Some(mutation_context),
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
        ObjectCommands::Link { command } => {
            handle_object_link_command(command, ctx, &mut client, &token).await?;
        }
    }

    Ok(())
}

async fn handle_object_link_command(
    command: &ObjectLinkCommands,
    ctx: &Context,
    client: &mut ObjectServiceClient<tonic::transport::Channel>,
    token: &str,
) -> anyhow::Result<()> {
    match command {
        ObjectLinkCommands::Create {
            link,
            target,
            allow_dangling,
            resolution,
        } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let (target_bucket, target_key) = parse_s3_path(target)?;
            anyhow::ensure!(
                bucket == target_bucket,
                "cross-bucket object links are not supported by the public CLI"
            );
            let mut request = tonic::Request::new(api::CreateObjectLinkRequest {
                context: Some(public_link_context("link-create", 0)),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
                target_key,
                target_version: String::new(),
                resolution: parse_link_resolution(resolution)?,
                allow_dangling: *allow_dangling,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            print_link(client.create_object_link(request).await?.into_inner().link);
        }
        ObjectLinkCommands::Update {
            link,
            target,
            expected_generation,
            allow_dangling,
            resolution,
        } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let (target_bucket, target_key) = parse_s3_path(target)?;
            anyhow::ensure!(
                bucket == target_bucket,
                "cross-bucket object links are not supported by the public CLI"
            );
            let mut request = tonic::Request::new(api::UpdateObjectLinkRequest {
                context: Some(public_link_context("link-update", *expected_generation)),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
                target_key,
                target_version: String::new(),
                resolution: parse_link_resolution(resolution)?,
                allow_dangling: *allow_dangling,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            print_link(client.update_object_link(request).await?.into_inner().link);
        }
        ObjectLinkCommands::Delete {
            link,
            expected_generation,
        } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let mut request = tonic::Request::new(api::DeleteObjectLinkRequest {
                context: Some(public_link_context("link-delete", *expected_generation)),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.delete_object_link(request).await?.into_inner();
            println!(
                "Deleted {} at generation {}",
                response.resource_id, response.generation
            );
        }
        ObjectLinkCommands::Read { link } => {
            let (bucket, link_key) = parse_s3_path(link)?;
            let mut request = tonic::Request::new(api::ReadObjectLinkRequest {
                request_id: format!("link-read-{}", uuid::Uuid::new_v4()),
                tenant_id: String::new(),
                bucket_name: bucket,
                link_key,
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            print_link(client.read_object_link(request).await?.into_inner().link);
        }
        ObjectLinkCommands::List { path, limit } => {
            let (bucket, prefix) = parse_s3_path(path)?;
            let mut request = tonic::Request::new(api::ListObjectLinksRequest {
                tenant_id: String::new(),
                bucket_name: bucket,
                prefix,
                page: Some(api::PageRequest {
                    cursor: String::new(),
                    limit: *limit,
                }),
            });
            request.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", token).parse().unwrap(),
            );
            let response = client.list_object_links(request).await?.into_inner();
            for link in response.links {
                println!(
                    "{} -> {} (generation {})",
                    link.link_key, link.target_key, link.generation
                );
            }
        }
    }

    let _ = ctx;
    Ok(())
}

fn public_link_context(tag: &str, expected_generation: u64) -> api::PublicMutationContext {
    api::PublicMutationContext {
        request_id: format!("{tag}-{}", uuid::Uuid::new_v4()),
        idempotency_key: uuid::Uuid::new_v4().to_string(),
        expected_generation,
    }
}

fn parse_link_resolution(value: &str) -> anyhow::Result<i32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "follow" => Ok(api::ObjectLinkResolution::Follow as i32),
        "redirect" => Ok(api::ObjectLinkResolution::Redirect as i32),
        other => Err(anyhow::anyhow!(
            "invalid link resolution '{other}', expected follow or redirect"
        )),
    }
}

fn print_link(link: Option<api::ObjectLinkDescriptor>) {
    if let Some(link) = link {
        println!(
            "{} -> {}{} (generation {})",
            link.link_key,
            link.target_key,
            if link.target_version.is_empty() {
                String::new()
            } else {
                format!("@{}", link.target_version)
            },
            link.generation
        );
    }
}
