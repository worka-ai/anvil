use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{CreateBucketRequest, DeleteObjectRequest, HeadObjectRequest, ListObjectsRequest, ObjectMetadata, PutObjectRequest};
use anvil::tasks::{TaskStatus, TaskType};
use std::time::Duration;
use tonic::Request;

mod common;

#[tokio::test]
async fn test_delete_object_soft_deletes_and_enqueues_task() {
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone()).await.unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone()).await.unwrap();

    let bucket_name = "test-delete-bucket".to_string();
    let object_key = "test-delete-object".to_string();
    
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "TEST_REGION".to_string(),
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
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(metadata)),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(b"delete me".to_vec())),
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
    let list_res = object_client.list_objects(list_req).await.unwrap().into_inner();
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

    // 5. Verify a task was enqueued in the global DB
    let global_pool = cluster.states[0].db.get_global_pool();
    let client = global_pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT task_type, status FROM tasks WHERE payload->>'content_hash' IS NOT NULL",
            &[],
        )
        .await
        .unwrap();
    let task_type: TaskType = row.get("task_type");
    let status: TaskStatus = row.get("status");
    assert_eq!(task_type, TaskType::DeleteObject);
    assert_eq!(status, TaskStatus::Pending);
}

#[tokio::test]
async fn test_head_object() {
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone()).await.unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone()).await.unwrap();

    let bucket_name = "test-head-bucket".to_string();
    let object_key = "test-head-object".to_string();
    let content = b"hello head";
    
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "TEST_REGION".to_string(),
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
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(metadata)),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(content.to_vec())),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let put_res = object_client.put_object(put_req).await.unwrap().into_inner();

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
    let head_res = object_client.head_object(head_req).await.unwrap().into_inner();

    // 3. Assert metadata is correct
    assert_eq!(head_res.etag, put_res.etag);
    assert_eq!(head_res.size, content.len() as i64);
}

#[tokio::test]
async fn test_list_objects_with_delimiter() {
    let mut cluster = common::TestCluster::new(&["TEST_REGION"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone()).await.unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone()).await.unwrap();

    let bucket_name = "test-delimiter-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "TEST_REGION".to_string(),
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
                data: Some(anvil::anvil_api::put_object_request::Data::Metadata(metadata)),
            },
            PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(b"...".to_vec())),
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
    let list_res = object_client.list_objects(list_req).await.unwrap().into_inner();

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
    let list_res_2 = object_client.list_objects(list_req_2).await.unwrap().into_inner();

    let top_level_objects: Vec<&str> = list_res_2.objects.iter().map(|o| o.key.as_str()).collect();
    assert_eq!(top_level_objects, vec!["d.txt"]);
    assert_eq!(list_res_2.common_prefixes, vec!["a/"]);
}
