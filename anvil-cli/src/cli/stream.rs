use crate::cli::object::native_mutation_context;
use crate::context::Context;
use anvil::anvil_api as api;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use clap::Subcommand;
use tokio_stream::StreamExt;

#[derive(Subcommand)]
pub enum StreamCommands {
    Create {
        bucket: String,
        stream_key: String,
    },
    Append {
        bucket: String,
        stream_key: String,
        stream_id: String,
        payload: String,
        #[clap(long)]
        content_type: Option<String>,
        #[clap(long, default_value = "{}")]
        user_metadata_json: String,
    },
    Read {
        bucket: String,
        stream_key: String,
        stream_id: String,
        #[clap(long, default_value_t = 0)]
        after_sequence: u64,
        #[clap(long, default_value_t = 100)]
        limit: u32,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        include_payload: bool,
    },
    Tail {
        bucket: String,
        stream_key: String,
        stream_id: String,
        #[clap(long, default_value_t = 0)]
        from_sequence: u64,
        #[clap(long, action = clap::ArgAction::SetTrue)]
        include_payload: bool,
        #[clap(long, default_value_t = 1000)]
        poll_interval_ms: u32,
    },
    SealSegment {
        bucket: String,
        stream_key: String,
        stream_id: String,
    },
}

pub async fn handle_stream_command(command: &StreamCommands, ctx: &Context) -> anyhow::Result<()> {
    let mut client = ObjectServiceClient::connect(ctx.profile.host.clone()).await?;
    let token = ctx.get_bearer_token().await?;
    match command {
        StreamCommands::Create { bucket, stream_key } => {
            let mc = native_mutation_context(ctx, &token, bucket, "stream-create").await?;
            let mut request = tonic::Request::new(api::CreateAppendStreamRequest {
                bucket_name: bucket.clone(),
                stream_key: stream_key.clone(),
                mutation_context: Some(mc),
            });
            add_auth(&mut request, &token);
            let response = client.create_append_stream(request).await?.into_inner();
            println!(
                "stream_id={} version_id={}",
                response.stream_id, response.version_id
            );
        }
        StreamCommands::Append {
            bucket,
            stream_key,
            stream_id,
            payload,
            content_type,
            user_metadata_json,
        } => {
            let mc = native_mutation_context(ctx, &token, bucket, "stream-append").await?;
            let mut request = tonic::Request::new(api::AppendStreamRecordRequest {
                bucket_name: bucket.clone(),
                stream_key: stream_key.clone(),
                stream_id: stream_id.clone(),
                payload: payload.as_bytes().to_vec(),
                mutation_context: Some(mc),
                content_type: content_type.clone(),
                user_metadata_json: user_metadata_json.clone(),
                precondition: None,
            });
            add_auth(&mut request, &token);
            let response = client.append_stream_record(request).await?.into_inner();
            println!(
                "sequence={} hash={}",
                response.record_sequence, response.payload_hash
            );
        }
        StreamCommands::Read {
            bucket,
            stream_key,
            stream_id,
            after_sequence,
            limit,
            include_payload,
        } => {
            let mut request = tonic::Request::new(api::ReadAppendStreamRequest {
                bucket_name: bucket.clone(),
                stream_key: stream_key.clone(),
                stream_id: stream_id.clone(),
                after_sequence: *after_sequence,
                limit: *limit,
                include_payload: *include_payload,
            });
            add_auth(&mut request, &token);
            let response = client.read_append_stream(request).await?.into_inner();
            for record in response.records {
                if *include_payload {
                    println!(
                        "{}\t{}\t{}",
                        record.record_sequence,
                        record.content_type,
                        String::from_utf8_lossy(&record.payload)
                    );
                } else {
                    println!(
                        "{}\t{}\t{}",
                        record.record_sequence, record.payload_size, record.payload_hash
                    );
                }
            }
        }
        StreamCommands::Tail {
            bucket,
            stream_key,
            stream_id,
            from_sequence,
            include_payload,
            poll_interval_ms,
        } => {
            let mut request = tonic::Request::new(api::TailAppendStreamRequest {
                bucket_name: bucket.clone(),
                stream_key: stream_key.clone(),
                stream_id: stream_id.clone(),
                from_sequence: *from_sequence,
                include_payload: *include_payload,
                poll_interval_ms: *poll_interval_ms,
            });
            add_auth(&mut request, &token);
            let mut stream = client.tail_append_stream(request).await?.into_inner();
            while let Some(item) = stream.next().await {
                let item = item?;
                if let Some(record) = item.record {
                    println!("{}\t{}", record.record_sequence, record.payload_hash);
                }
            }
        }
        StreamCommands::SealSegment {
            bucket,
            stream_key,
            stream_id,
        } => {
            let mc = native_mutation_context(ctx, &token, bucket, "stream-seal").await?;
            let mut request = tonic::Request::new(api::SealAppendStreamSegmentRequest {
                bucket_name: bucket.clone(),
                stream_key: stream_key.clone(),
                stream_id: stream_id.clone(),
                mutation_context: Some(mc),
                precondition: None,
            });
            add_auth(&mut request, &token);
            let response = client
                .seal_append_stream_segment(request)
                .await?
                .into_inner();
            println!(
                "records={} segment_hash={}",
                response.record_count, response.segment_hash
            );
        }
    }
    Ok(())
}

fn add_auth<T>(request: &mut tonic::Request<T>, token: &str) {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
}
