use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{DeleteObjectRequest, ListObjectsRequest, ObjectMetadata, PutObjectRequest};
use tonic::Request;

mod common;

#[tokio::test]
async fn test_delete_object_soft_deletes_and_enqueues_task() {
    common::with_test_dbs(|global_db_url, regional_db_url, _| async move {
        let (state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
        let token = common::get_auth_token(&global_db_url, &grpc_addr).await;
        let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
            .await
            .unwrap();

        let bucket_name = "test-delete-bucket".to_string();
        let object_key = "test-delete-object".to_string();
        common::create_test_bucket(&grpc_addr, &bucket_name, &token).await;

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

        // 5. Verify a task was enqueued in the global DB
        let global_pool = state.db.get_global_pool();
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
    })
    .await;
}

use anvil::anvil_api::HeadObjectRequest;
use anvil::tasks::{TaskStatus, TaskType};

#[tokio::test]
async fn test_head_object() {
    common::with_test_dbs(|global_db_url, regional_db_url, _| async move {
        let (_state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
        let token = common::get_auth_token(&global_db_url, &grpc_addr).await;
        let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
            .await
            .unwrap();

        let bucket_name = "test-head-bucket".to_string();
        let object_key = "test-head-object".to_string();
        let content = b"hello head";
        common::create_test_bucket(&grpc_addr, &bucket_name, &token).await;

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
    })
    .await;
}

#[tokio::test]
async fn test_list_objects_with_delimiter() {
    common::with_test_dbs(|global_db_url, regional_db_url, _| async move {
        let (_state, grpc_addr) = common::start_test_server(&global_db_url, &regional_db_url).await;
        let token = common::get_auth_token(&global_db_url, &grpc_addr).await;
        let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
            .await
            .unwrap();

        let bucket_name = "test-delimiter-bucket".to_string();
        common::create_test_bucket(&grpc_addr, &bucket_name, &token).await;

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

        // Listing with prefix and delimiter should return first-level files under a/ and no common prefixes
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

        // Listing with just a delimiter (no prefix) should return top-level file and one common prefix
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

        let top_level_objects: Vec<&str> =
            list_res_2.objects.iter().map(|o| o.key.as_str()).collect();
        assert_eq!(top_level_objects, vec!["d.txt"]);
        assert_eq!(list_res_2.common_prefixes, vec!["a/"]);
    })
    .await;
}
