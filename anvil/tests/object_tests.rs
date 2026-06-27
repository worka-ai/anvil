use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    self, ComposeObjectRequest, ComposeObjectSource, CopyObjectRequest, CreateBucketRequest,
    DeleteObjectRequest, GetObjectRequest, HeadObjectRequest, ListObjectVersionsRequest,
    ListObjectsRequest, ObjectMetadata, PutObjectRequest,
};
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

use anvil_test_utils::*;

#[tokio::test]
async fn test_delete_object_creates_delete_marker() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-delete-bucket".to_string();
    let object_key = "test-delete-object".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    // 1. Put an object
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
                b"delete me".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    // 2. Verify it exists
    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.objects.len(), 1);

    // 3. Delete the object
    let mut del_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    del_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.delete_object(del_req).await.unwrap();

    // 4. Verify it is gone from listings (soft deleted)
    let mut list_req_after_delete = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        ..Default::default()
    });
    list_req_after_delete.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res_after_delete = object_client
        .list_objects(list_req_after_delete)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res_after_delete.objects.len(), 0);

    // 5. Verify versions retain the original object plus a latest delete marker.
    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 100,
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let versions = object_client
        .list_object_versions(versions_req)
        .await
        .unwrap()
        .into_inner()
        .versions;
    assert_eq!(versions.len(), 2);
    assert!(versions[0].is_delete_marker);
    assert!(versions[0].is_latest);
    assert!(!versions[1].is_delete_marker);
    assert!(!versions[1].is_latest);
}

#[tokio::test]
async fn test_head_object() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-head-bucket".to_string();
    let object_key = "test-head-object".to_string();
    let content = b"hello head";

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    // 1. Put an object
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
                content.to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let put_res = object_client
        .put_object(put_req)
        .await
        .unwrap()
        .into_inner();

    // 2. Head the object
    let mut head_req = Request::new(HeadObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    head_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let head_res = object_client
        .head_object(head_req)
        .await
        .unwrap()
        .into_inner();

    // 3. Assert metadata is correct
    assert_eq!(head_res.etag, put_res.etag);
    assert_eq!(head_res.size, content.len() as i64);
}

#[tokio::test]
async fn test_copy_object_creates_independent_destination_version() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-copy-bucket".to_string();
    let source_key = "source.txt".to_string();
    let destination_key = "destination.txt".to_string();
    let content = b"copy native object";

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: source_key.clone(),
    };
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                content.to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let put_res = object_client
        .put_object(put_req)
        .await
        .unwrap()
        .into_inner();

    let mut copy_req = Request::new(CopyObjectRequest {
        source_bucket_name: bucket_name.clone(),
        source_object_key: source_key.clone(),
        source_version_id: Some(put_res.version_id.clone()),
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: destination_key.clone(),
    });
    copy_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let copy_res = object_client
        .copy_object(copy_req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(copy_res.etag, put_res.etag);
    assert_ne!(copy_res.version_id, put_res.version_id);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: destination_key.clone(),
        version_id: Some(copy_res.version_id),
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = object_client
        .get_object(get_req)
        .await
        .unwrap()
        .into_inner();
    let mut downloaded = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk.unwrap().data.unwrap() {
            anvil_api::get_object_response::Data::Metadata(metadata) => {
                assert_eq!(metadata.content_length, content.len() as i64);
            }
            anvil_api::get_object_response::Data::Chunk(bytes) => {
                downloaded.extend_from_slice(&bytes);
            }
        }
    }
    assert_eq!(downloaded, content);
}

#[tokio::test]
async fn test_compose_object_concatenates_sources_in_order() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-compose-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let sources = vec![
        ("part-a.txt", b"hello ".to_vec()),
        ("part-b.txt", b"compose".to_vec()),
    ];
    let mut source_versions = Vec::new();
    for (key, content) in &sources {
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: key.to_string(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    metadata,
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    content.clone(),
                )),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let put_res = object_client
            .put_object(put_req)
            .await
            .unwrap()
            .into_inner();
        source_versions.push((key.to_string(), put_res.version_id));
    }

    let mut compose_req = Request::new(ComposeObjectRequest {
        sources: source_versions
            .into_iter()
            .map(|(key, version_id)| ComposeObjectSource {
                bucket_name: bucket_name.clone(),
                object_key: key,
                version_id: Some(version_id),
            })
            .collect(),
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: "composed.txt".to_string(),
    });
    compose_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let compose_res = object_client
        .compose_object(compose_req)
        .await
        .unwrap()
        .into_inner();

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: "composed.txt".to_string(),
        version_id: Some(compose_res.version_id),
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = object_client
        .get_object(get_req)
        .await
        .unwrap()
        .into_inner();
    let mut downloaded = Vec::new();
    while let Some(chunk) = stream.next().await {
        match chunk.unwrap().data.unwrap() {
            anvil_api::get_object_response::Data::Metadata(metadata) => {
                assert_eq!(metadata.content_length, "hello compose".len() as i64);
            }
            anvil_api::get_object_response::Data::Chunk(bytes) => {
                downloaded.extend_from_slice(&bytes);
            }
        }
    }

    assert_eq!(downloaded, b"hello compose");
}

#[tokio::test]
async fn test_list_objects_with_delimiter() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-delimiter-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let keys = vec!["a/b.txt", "a/c.txt", "d.txt"];
    for key in keys {
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: key.to_string(),
        };
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                    metadata,
                )),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    b"...".to_vec(),
                )),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        object_client.put_object(put_req).await.unwrap();
    }

    // Listing with prefix and delimiter
    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "a/".to_string(),
        delimiter: "/".to_string(),
        ..Default::default()
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(list_res.objects.len(), 2);
    let got_under_a: Vec<&str> = list_res.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(got_under_a, vec!["a/b.txt", "a/c.txt"]);
    assert!(list_res.common_prefixes.is_empty());

    // Listing with just a delimiter
    let mut list_req_2 = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        delimiter: "/".to_string(),
        ..Default::default()
    });
    list_req_2.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res_2 = object_client
        .list_objects(list_req_2)
        .await
        .unwrap()
        .into_inner();

    let top_level_objects: Vec<&str> = list_res_2.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(top_level_objects, vec!["d.txt"]);
    assert_eq!(list_res_2.common_prefixes, vec!["a/"]);
}
