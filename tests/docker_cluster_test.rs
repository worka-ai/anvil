use anvil::anvil_api::{
    bucket_service_client::BucketServiceClient, object_service_client::ObjectServiceClient, CreateBucketRequest, GetObjectRequest,
    ObjectMetadata, PutObjectRequest,
};
use futures_util::StreamExt;
use std::process::{Command, Stdio};
use std::time::Duration;
use tonic::Request;


mod common;

struct DockerCluster {
    grpc_addrs: Vec<String>,
}

impl DockerCluster {
    fn new() -> Self {
        Command::new("docker-compose")
            .args(&["up", "-d", "--build"])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            //.spawn()
            .status()
            .expect("Failed to execute docker-compose up");

        // let mut stdin = child.stdin.take().expect("Failed to open stdin");
        // let output = child.wait_with_output().expect("Failed to read stdout");
        // info!("{}", String::from_utf8_lossy(&output.stdout));
        // info!("{}", String::from_utf8_lossy(&output.stderr));
        // assert!(output.status.success());
        //assert!(child.success());

        Self {
            grpc_addrs: vec![
                "http://127.0.0.1:9001".to_string(),
                "http://127.0.0.1:9002".to_string(),
                "http://127.0.0.1:9003".to_string(),
            ],
        }
    }

    async fn wait_for_cluster(&self) {
        println!("Waiting for postgres container to be ready...");
        assert!(
            common::wait_for_port("127.0.0.1:5433".parse().unwrap(), Duration::from_secs(30)).await,
            "Postgres container did not start in time"
        );
        println!("Postgres is ready.");

        println!("Waiting for anvil nodes to be ready...");
        for addr in &self.grpc_addrs {
            let socket_addr = addr.replace("http://", "").parse().unwrap();
            assert!(
                common::wait_for_port(socket_addr, Duration::from_secs(30)).await,
                "gRPC server did not start in time"
            );
        }
        println!("Anvil nodes are ready.");

        // Per user instruction, wait a fixed time for convergence.
        println!("Waiting for cluster to converge...");
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("Convergence wait finished.");
    }
}

impl Drop for DockerCluster {
    fn drop(&mut self) {
        let _ = Command::new("docker-compose")
            .args(&["down", "-v"])
            .status();
    }
}

#[tokio::test]
//#[ignore] // This test requires Docker and is slow, so ignore by default.
async fn test_docker_cluster_e2e() {
    let cluster = DockerCluster::new();
    cluster.wait_for_cluster().await;

    // The admin CLI runs on the host and connects to the global DB via its exposed port.
    let global_db_url = "postgres://worka:worka@localhost:5433/anvil_global";

    // Manually run migrations and create the default tenant before running the test logic.
    anvil::run_migrations(
        global_db_url,
        common::migrations::migrations::runner(),
        "refinery_schema_history_global",
    )
    .await
    .unwrap();
    let global_pool = common::create_pool(global_db_url).unwrap();
    common::create_default_tenant(&global_pool, "DOCKER_TEST").await;

    // Provision resources using the admin CLI
    let token = common::get_auth_token(global_db_url, &cluster.grpc_addrs[0]).await;

    let mut bucket_client = BucketServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let bucket_name = "docker-test-bucket".to_string();
    let mut create_bucket_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "DOCKER_TEST".to_string(),
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    // Connect to a node and perform a put/get test
    let object_key = "docker-test-object".to_string();
    let data = b"hello from docker".to_vec();
    let max_retries = 15;
    let mut delay = Duration::from_secs(2);

    for attempt in 0..max_retries {
        let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[1].clone())
            .await
            .unwrap();

        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: object_key.clone(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    metadata,
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    data.clone(),
                )),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );

        match object_client.put_object(put_req).await {
            Ok(_) => {
                println!("PutObject succeeded on attempt {}.", attempt + 1);
                break; // Success
            }
            Err(status) if status.code() == tonic::Code::Unavailable => {
                if attempt == max_retries - 1 {
                    panic!("PutObject failed after all retries: {}", status);
                }
                println!(
                    "PutObject failed with Unavailable (attempt {}), retrying in {:?}...",
                    attempt + 1,
                    delay
                );
                tokio::time::sleep(delay).await;
                delay *= 2; // Exponential backoff
            }
            Err(e) => {
                panic!("PutObject failed with unexpected error: {}", e);
            }
        }
    }

    // Get the object from a different node
    let mut recovery_client = ObjectServiceClient::connect(cluster.grpc_addrs[2].clone())
        .await
        .unwrap();
    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = recovery_client
        .get_object(get_req)
        .await
        .unwrap()
        .into_inner();

    let mut downloaded_data = Vec::new();
    if let Some(Ok(first_chunk)) = stream.next().await {
        if let Some(anvil::anvil_api::get_object_response::Data::Metadata(_)) = first_chunk.data {
            while let Some(Ok(chunk)) = stream.next().await {
                if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.data
                {
                    downloaded_data.extend_from_slice(&bytes);
                }
            }
        }
    }

    assert_eq!(downloaded_data, data);
}
