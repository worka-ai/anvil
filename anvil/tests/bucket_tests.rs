use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, DeleteBucketRequest, GetBucketPolicyRequest, ListBucketsRequest,
    PutBucketPolicyRequest, WatchBucketMetadataRequest,
};
use anvil::tasks::TaskStatus;
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

use anvil_test_utils::*;

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
            "SELECT task_type, status FROM tasks WHERE payload->>'bucket_id' IS NOT NULL",
            &[],
        )
        .await
        .unwrap();
    let task_type: anvil::tasks::TaskType = row.get("task_type");
    let status: TaskStatus = row.get("status");
    assert!(matches!(task_type, anvil::tasks::TaskType::DeleteBucket));
    assert_eq!(status, TaskStatus::Pending);
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
