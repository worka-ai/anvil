use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    self, CreateBucketRequest, GetObjectRequest, ObjectMetadata, PutObjectRequest,
};
use futures_util::StreamExt;
use std::path::Path;
use std::time::Duration;
use tokio::fs;

mod common;

#[tokio::test]
async fn test_distributed_put_and_get() {
    let num_nodes = 6;
    let mut cluster = common::TestCluster::new(num_nodes).await;
    cluster.start_and_converge(Duration::from_secs(120)).await;

    let token = cluster.token.clone();
    let client_addr = cluster.grpc_addrs[0].clone();

    let mut bucket_client = BucketServiceClient::connect(client_addr.clone()).await.unwrap();
    let bucket_name = format!("test-bucket-{}", uuid::Uuid::new_v4());
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "TEST_REGION".to_string(),
    });
    create_bucket_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", token).parse().unwrap());
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    let mut object_client = ObjectServiceClient::connect(client_addr).await.unwrap();
    let object_key = "my-distributed-object".to_string();
    let data = (0..1024 * 128).map(|i| (i % 256) as u8).collect::<Vec<_>>();

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
    };
    let mut chunks = vec![PutObjectRequest {
        data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
    }];
    for chunk in data.chunks(1024 * 64) {
        chunks.push(PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(chunk.to_vec())),
        });
    }

    let request_stream = tokio_stream::iter(chunks);
    let mut put_object_req = tonic::Request::new(request_stream);
    put_object_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", token).parse().unwrap());

    let response = object_client
        .put_object(put_object_req)
        .await
        .unwrap()
        .into_inner();
    let object_hash = response.etag;

    let get_request = GetObjectRequest {
        bucket_name,
        object_key,
        version_id: Some(response.version_id),
    };
    let mut get_object_req = tonic::Request::new(get_request);
    get_object_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", token).parse().unwrap());
    let mut response_stream = object_client
        .get_object(get_object_req)
        .await
        .unwrap()
        .into_inner();

    let mut downloaded_data = Vec::new();
    if let Some(Ok(first_chunk)) = response_stream.next().await {
        if let Some(anvil_api::get_object_response::Data::Metadata(_)) = first_chunk.data {
            while let Some(Ok(chunk)) = response_stream.next().await {
                if let Some(anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.data {
                    downloaded_data.extend_from_slice(&bytes);
                }
            }
        }
    }

    assert_eq!(downloaded_data, data);

    tokio::time::sleep(Duration::from_secs(1)).await;

    let mut shards_found = 0;
    for i in 0..num_nodes {
        let shard_path = format!("anvil-data/{}-{:02}", object_hash, i);
        if Path::new(&shard_path).exists() {
            shards_found += 1;
            fs::remove_file(shard_path).await.unwrap();
        }
    }
    assert!(shards_found > 0);
}

#[tokio::test]
async fn test_single_node_put() {
    let mut cluster = common::TestCluster::new(1).await;
    cluster.start_and_converge(Duration::from_secs(15)).await;

    let token = cluster.token.clone();
    let client_addr = cluster.grpc_addrs[0].clone();

    let mut bucket_client = BucketServiceClient::connect(client_addr.clone()).await.unwrap();
    let bucket_name = "single-node-bucket".to_string();
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "TEST_REGION".to_string(),
    });
    create_bucket_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", token).parse().unwrap());
    bucket_client
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    let mut object_client = ObjectServiceClient::connect(client_addr).await.unwrap();
    let object_key = "single-node-object".to_string();
    let data = b"hello world".to_vec();

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(data.clone())),
        },
    ];

    let request_stream = tokio_stream::iter(chunks);

    let mut put_object_req = tonic::Request::new(request_stream);
    put_object_req
        .metadata_mut()
        .insert("authorization", format!("Bearer {}", token).parse().unwrap());

    let result = object_client.put_object(put_object_req).await;
    assert!(result.is_ok());
}

// TODO: This test needs to be rewritten to support a multi-region TestCluster.
// #[tokio::test]
// async fn test_multi_region_list_and_isolation() {
//
// }
