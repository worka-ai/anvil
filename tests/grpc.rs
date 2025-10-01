use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    self, CreateBucketRequest, GetObjectRequest, ListObjectsRequest, ObjectMetadata,
    PutObjectRequest,
};
use futures_util::StreamExt;
use std::path::Path;
use std::time::Duration;
use tokio::fs;
use tonic::Code;

mod common;

#[tokio::test]
async fn test_distributed_put_and_get() {
    let num_nodes = 6;
    let mut cluster = common::TestCluster::new(&["TEST_REGION"; 6]).await;
    cluster.start_and_converge(Duration::from_secs(20)).await;

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
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

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

#[tokio::test]
async fn test_multi_region_list_and_isolation() {
    let mut cluster_east = common::TestCluster::new(&["US_EAST_1"]).await;
    cluster_east.start_and_converge(Duration::from_secs(5)).await;

    let mut cluster_west = common::TestCluster::new(&["EU_WEST_1"]).await;
    cluster_west.start_and_converge(Duration::from_secs(5)).await;

    let token = cluster_east.token.clone();
    let east_client_addr = cluster_east.grpc_addrs[0].clone();
    let west_client_addr = cluster_west.grpc_addrs[0].clone();

    let mut bucket_client_east = BucketServiceClient::connect(east_client_addr.clone()).await.unwrap();
    let mut object_client_east = ObjectServiceClient::connect(east_client_addr).await.unwrap();
    let mut object_client_west = ObjectServiceClient::connect(west_client_addr).await.unwrap();

    let bucket_name = "regional-bucket".to_string();
    let mut create_bucket_req = tonic::Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "US_EAST_1".to_string(),
    });
    create_bucket_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client_east
        .create_bucket(create_bucket_req)
        .await
        .unwrap();

    let object_key = "regional-object".to_string();
    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(
                b"regional data".to_vec(),
            )),
        },
    ];
    let mut put_object_req = tonic::Request::new(tokio_stream::iter(chunks));
    put_object_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client_east.put_object(put_object_req).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;

    let list_req_east = ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    };
    let mut list_req_east_auth = tonic::Request::new(list_req_east);
    list_req_east_auth.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_resp_east = object_client_east
        .list_objects(list_req_east_auth)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_resp_east.objects.len(), 1);
    assert_eq!(list_resp_east.objects[0].key, object_key);

    let list_req_west = ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    };
    let mut list_req_west_auth = tonic::Request::new(list_req_west);
    list_req_west_auth.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_resp_west = object_client_west.list_objects(list_req_west_auth).await;

    assert!(list_resp_west.is_err());
    assert_eq!(list_resp_west.unwrap_err().code(), Code::NotFound);
}