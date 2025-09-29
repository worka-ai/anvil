use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::{DeleteBucketRequest, ListBucketsRequest};
use anvil::tasks::TaskStatus;
use tonic::Request;

mod common;

#[tokio::test]
async fn test_delete_bucket_soft_deletes_and_enqueues_task() {
    common::with_test_dbs(|global_db_url, regional_db_url, _| async move {
        let (state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
        let token = common::get_auth_token(&global_db_url, &grpc_addr).await;
        let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
            .await
            .unwrap();

        let bucket_name = "test-delete-bucket".to_string();
        common::create_test_bucket(&grpc_addr, &bucket_name, &token).await;

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
        let global_pool = state.db.get_global_pool();
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
    })
    .await;
}

#[tokio::test]
async fn test_list_buckets() {
    common::with_test_dbs(|global_db_url, regional_db_url, _| async move {
        let (_state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
        let token = common::get_auth_token(&global_db_url, &grpc_addr).await;
        let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
            .await
            .unwrap();

        let bucket_name1 = "list-bucket-1".to_string();
        let bucket_name2 = "list-bucket-2".to_string();
        common::create_test_bucket(&grpc_addr, &bucket_name1, &token).await;
        common::create_test_bucket(&grpc_addr, &bucket_name2, &token).await;

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
    })
    .await;
}
