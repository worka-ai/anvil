use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{CreateBucketRequest, GetObjectRequest, ObjectMetadata, PutObjectRequest};
use futures::stream::StreamExt;
use std::time::Duration;
use tokio::time::timeout;
use tonic::Request;

use anvil_test_utils::*;

#[tokio::test]
async fn test_distributed_reconstruction_on_node_failure() {
    //let num_nodes = 6;
    let mut cluster = TestCluster::new(&["test-region-1"; 6]).await;
    cluster.start_and_converge(Duration::from_secs(20)).await;

    let primary_addr = cluster.grpc_addrs[0].clone(); // already includes /grpc
    let mut object_client = ObjectServiceClient::connect(primary_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(primary_addr.clone())
        .await
        .unwrap();

    let bucket_name = "reconstruction-bucket".to_string();
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    let object_key = "reconstruction-object".to_string();
    let content = (0..1024 * 256).map(|i| (i % 256) as u8).collect::<Vec<_>>();

    // 1. Put an object, which will be distributed
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
    };
    let mut chunks = vec![PutObjectRequest {
        data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
            metadata,
        )),
    }];
    for chunk in content.chunks(1024 * 64) {
        chunks.push(PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                chunk.to_vec(),
            )),
        });
    }
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", cluster.token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // 2. Stop one of the nodes
    cluster.nodes.remove(1).abort();
    // Wait a moment for the node to die and for gossip to propagate
    tokio::time::sleep(Duration::from_secs(5)).await;

    // 3. Connect to a *different*, live node
    // Recover client on a different node; allow brief retry for readiness
    let mut recovery_client = loop {
        match ObjectServiceClient::connect(cluster.grpc_addrs[2].clone()).await {
            Ok(c) => break c,
            Err(_) => {
                tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                continue;
            }
        }
    };

    let mut stream = {
        let mut attempt = 0;
        loop {
            // 4. Get the object and verify its content
            let mut get_req = Request::new(GetObjectRequest {
                bucket_name: bucket_name.clone(),
                object_key: object_key.clone(),
                version_id: None,
            });
            get_req.metadata_mut().insert(
                "authorization",
                format!("Bearer {}", cluster.token).parse().unwrap(),
            );
            match timeout(Duration::from_secs(10), recovery_client.get_object(get_req)).await {
                Ok(Ok(resp)) => break resp.into_inner(),
                _ => {
                    attempt += 1;
                    if attempt > 8 {
                        panic!("get_object timed out or unavailable");
                    }
                    tokio::time::sleep(Duration::from_millis(150)).await;
                    continue;
                }
            }
        }
    };

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

    assert_eq!(
        downloaded_data, content,
        "Reconstructed data did not match original data"
    );
}