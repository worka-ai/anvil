use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    AbortMultipartRequest, CreateBucketRequest, DeleteBucketRequest, GetBucketPolicyRequest,
    InitiateMultipartRequest, ListBucketsRequest, ObjectMetadata, PutBucketPolicyRequest,
    PutObjectRequest, WatchBucketMetadataRequest,
};
use anvil::tasks::TaskStatus;
use futures_util::StreamExt;
use std::time::{Duration, Instant};
use tonic::Request;

use anvil_test_utils::*;

#[tokio::test]
async fn test_task_claim_marks_tasks_running_before_execution() {
    let cluster = TestCluster::new(&["test-region-1"]).await;
    let db = &cluster.states[0].db;

    db.enqueue_task(
        anvil::tasks::TaskType::DeleteBucket,
        serde_json::json!({ "bucket_id": 123_i64 }),
        100,
    )
    .await
    .unwrap();

    let claimed = db.claim_pending_tasks(10).await.unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].get::<_, i64>("id"), 1);

    let claimed_again = db.claim_pending_tasks(10).await.unwrap();
    assert!(
        claimed_again.is_empty(),
        "running tasks must not be claimed again"
    );

    let client = db.get_global_pool().get().await.unwrap();
    let status: TaskStatus = client
        .query_one("SELECT status FROM tasks WHERE id = 1", &[])
        .await
        .unwrap()
        .get("status");
    assert_eq!(status, TaskStatus::Running);
}

#[tokio::test]
async fn test_delete_bucket_soft_deletes_and_enqueues_task() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-delete-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    // 1. Verify it exists
    let mut list_req = Request::new(ListBucketsRequest {});
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = bucket_client
        .list_buckets(list_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(list_res.buckets.len(), 1);

    // 2. Delete the bucket
    let mut del_req = Request::new(DeleteBucketRequest {
        bucket_name: bucket_name.clone(),
    });
    del_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.delete_bucket(del_req).await.unwrap();

    // 3. Verify it is gone from listings (soft deleted)
    let mut list_req_after_delete = Request::new(ListBucketsRequest {});
    list_req_after_delete.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res_after_delete = bucket_client
        .list_buckets(list_req_after_delete)
        .await
        .unwrap()
        .into_inner();
    assert!(list_res_after_delete.buckets.is_empty());

    // 4. Verify a task was enqueued in the global DB
    let global_pool = cluster.states[0].db.get_global_pool();
    let client = global_pool.get().await.unwrap();
    let row = client
        .query_one(
            "SELECT task_type, status, payload FROM tasks WHERE payload->>'bucket_id' IS NOT NULL",
            &[],
        )
        .await
        .unwrap();
    let task_type: anvil::tasks::TaskType = row.get("task_type");
    let status: TaskStatus = row.get("status");
    let payload: serde_json::Value = row.get("payload");
    let bucket_id = payload
        .get("bucket_id")
        .and_then(|value| value.as_i64())
        .expect("delete bucket task payload should contain bucket_id");
    assert!(matches!(task_type, anvil::tasks::TaskType::DeleteBucket));
    assert!(matches!(
        status,
        TaskStatus::Pending | TaskStatus::Running | TaskStatus::Completed
    ));

    // 5. The background worker must apply the queued deletion so the bucket
    // name can be reused without leaving a permanently soft-deleted row behind.
    let start = Instant::now();
    loop {
        let row = client
            .query_one(
                "SELECT status FROM tasks WHERE payload->>'bucket_id' = $1",
                &[&bucket_id.to_string()],
            )
            .await
            .unwrap();
        let status: TaskStatus = row.get("status");
        if status == TaskStatus::Completed {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(12),
            "delete bucket task did not complete"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM buckets WHERE id = $1)",
            &[&bucket_id],
        )
        .await
        .unwrap()
        .get(0);
    assert!(!exists, "delete bucket task should hard-delete bucket row");

    let mut recreate_req = Request::new(CreateBucketRequest {
        bucket_name,
        region: "test-region-1".to_string(),
    });
    recreate_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(recreate_req).await.unwrap();
}

#[tokio::test]
async fn test_delete_bucket_rejects_retained_objects() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-delete-nonempty-bucket".to_string();
    let object_key = "object.txt".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key,
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"retained".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    let mut del_req = Request::new(DeleteBucketRequest {
        bucket_name: bucket_name.clone(),
    });
    del_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = bucket_client
        .delete_bucket(del_req)
        .await
        .expect_err("retained object versions should keep bucket non-empty");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Bucket not empty"));
}

#[tokio::test]
async fn test_delete_bucket_rejects_active_multipart_uploads() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-delete-active-multipart-bucket".to_string();
    let object_key = "multipart-object.txt".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let mut initiate_req = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
    });
    initiate_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let upload_id = object_client
        .initiate_multipart_upload(initiate_req)
        .await
        .unwrap()
        .into_inner()
        .upload_id;

    let mut active_delete_req = Request::new(DeleteBucketRequest {
        bucket_name: bucket_name.clone(),
    });
    active_delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let err = bucket_client
        .delete_bucket(active_delete_req)
        .await
        .expect_err("active multipart upload should keep bucket non-empty");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("Bucket not empty"));

    let mut abort_req = Request::new(AbortMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key,
        upload_id,
    });
    abort_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client
        .abort_multipart_upload(abort_req)
        .await
        .unwrap();

    let mut empty_delete_req = Request::new(DeleteBucketRequest { bucket_name });
    empty_delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.delete_bucket(empty_delete_req).await.unwrap();
}

#[tokio::test]
async fn test_list_buckets() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name1 = "list-bucket-1".to_string();
    let bucket_name2 = "list-bucket-2".to_string();

    let mut create_req1 = Request::new(CreateBucketRequest {
        bucket_name: bucket_name1.clone(),
        region: "test-region-1".to_string(),
    });
    create_req1.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req1).await.unwrap();

    let mut create_req2 = Request::new(CreateBucketRequest {
        bucket_name: bucket_name2.clone(),
        region: "test-region-1".to_string(),
    });
    create_req2.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req2).await.unwrap();

    let mut list_req = Request::new(ListBucketsRequest {});
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let list_res = bucket_client
        .list_buckets(list_req)
        .await
        .unwrap()
        .into_inner();

    assert_eq!(list_res.buckets.len(), 2);
    assert!(list_res.buckets.iter().any(|b| b.name == bucket_name1));
    assert!(list_res.buckets.iter().any(|b| b.name == bucket_name2));
}

#[tokio::test]
async fn test_get_bucket_policy_reflects_public_read_flag() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "policy-bucket".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let mut get_req = Request::new(GetBucketPolicyRequest {
        bucket_name: bucket_name.clone(),
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let policy = bucket_client
        .get_bucket_policy(get_req)
        .await
        .unwrap()
        .into_inner()
        .policy_json;
    let policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
    assert_eq!(policy["is_public_read"], false);

    let mut put_req = Request::new(PutBucketPolicyRequest {
        bucket_name: bucket_name.clone(),
        policy_json: serde_json::json!({"is_public_read": true}).to_string(),
    });
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.put_bucket_policy(put_req).await.unwrap();

    let mut get_req = Request::new(GetBucketPolicyRequest { bucket_name });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let policy = bucket_client
        .get_bucket_policy(get_req)
        .await
        .unwrap()
        .into_inner()
        .policy_json;
    let policy: serde_json::Value = serde_json::from_str(&policy).unwrap();
    assert_eq!(policy["is_public_read"], true);
}

#[tokio::test]
async fn test_watch_bucket_metadata_streams_snapshot_events() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "watch-bucket-metadata".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let mut put_req = Request::new(PutBucketPolicyRequest {
        bucket_name: bucket_name.clone(),
        policy_json: serde_json::json!({"is_public_read": true}).to_string(),
    });
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.put_bucket_policy(put_req).await.unwrap();

    let mut del_req = Request::new(DeleteBucketRequest {
        bucket_name: bucket_name.clone(),
    });
    del_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.delete_bucket(del_req).await.unwrap();

    let mut watch_req = Request::new(WatchBucketMetadataRequest {
        bucket_name: bucket_name.clone(),
        after_cursor: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut watch = bucket_client
        .watch_bucket_metadata(watch_req)
        .await
        .unwrap()
        .into_inner();

    let mut events = Vec::new();
    for _ in 0..3 {
        events.push(watch.next().await.unwrap().unwrap());
    }

    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["create", "policy_update", "delete"]
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    assert_eq!(events[0].bucket.as_ref().unwrap().name, bucket_name);
    assert!(events[1].bucket.as_ref().unwrap().is_public_read);
    assert!(events[2].bucket.as_ref().unwrap().deleted);
}
