use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::repair_service_client::RepairServiceClient;
use anvil::anvil_api::{
    self, AppendStreamRecordRequest, CompareAndSwapManifestRequest, CompleteMultipartPart,
    CompleteMultipartRequest, ComposeObjectRequest, ComposeObjectSource, CopyObjectRequest,
    CreateAppendStreamRequest, CreateBucketRequest, CreateIndexRequest, DeleteObjectRequest,
    GetObjectRequest, HeadObjectRequest, InitiateMultipartRequest, ListObjectVersionsRequest,
    ListObjectsRequest, ObjectMetadata, PatchJsonObjectRequest, PutObjectRequest,
    RepairDirectoryIndexRequest, SealAppendStreamSegmentRequest, UploadPartMetadata,
    UploadPartRequest, WatchPrefixRequest,
};
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

use anvil::storage::{DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES, ExternalChunkManifest};
use anvil_test_utils::*;
use tonic::{Code, Status};

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

fn assert_reserved_namespace_status<T>(result: Result<T, Status>) {
    let err = match result {
        Ok(_) => panic!("reserved namespace operation must fail"),
        Err(err) => err,
    };
    assert_eq!(err.code(), Code::PermissionDenied);
    assert!(
        err.message().contains("UnauthorizedReservedNamespace"),
        "expected UnauthorizedReservedNamespace, got {err:?}"
    );
}

#[tokio::test]
async fn test_repair_rebuilds_missing_directory_segment_from_metadata_journal() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let persistence = &cluster.states[0].persistence;
    let tenant_id = 1;
    let bucket_name = format!("directory-repair-{}", uuid::Uuid::new_v4());
    let bucket = persistence
        .create_bucket(tenant_id, &bucket_name, "test-region-1")
        .await
        .unwrap();
    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/a.txt",
            &hex::encode([41; 32]),
            12,
            "etag-a",
            Some("text/plain"),
            None,
            None,
            Some(b"directory-a".to_vec()),
        )
        .await
        .unwrap();
    persistence
        .create_object(
            tenant_id,
            bucket.id,
            "docs/b.txt",
            &hex::encode([42; 32]),
            12,
            "etag-b",
            Some("text/plain"),
            None,
            None,
            Some(b"directory-b".to_vec()),
        )
        .await
        .unwrap();
    let sealed = persistence
        .compact_object_metadata(bucket.id)
        .await
        .unwrap()
        .expect("object metadata compaction writes directory segment");
    tokio::fs::remove_file(&sealed.directory_path)
        .await
        .expect("remove directory segment to force repair");

    let mut repair_client = RepairServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let report = repair_client
        .repair_directory_index(authorized(
            RepairDirectoryIndexRequest {
                bucket_name: bucket_name.clone(),
                rebuild: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(report.status, "needs_repair");
    assert_eq!(report.reason, "DirectoryIndexInvalid");
    assert_eq!(report.expected_entry_count, 2);
    assert!(report.finding.is_some());

    let rebuilt = repair_client
        .repair_directory_index(authorized(
            RepairDirectoryIndexRequest {
                bucket_name: bucket_name.clone(),
                rebuild: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(rebuilt.status, "rebuilt_directory_index");
    assert_eq!(rebuilt.reason, "DirectoryIndexInvalid");
    assert_eq!(rebuilt.expected_entry_count, 2);
    assert!(!rebuilt.rebuilt_manifest_hash.is_empty());

    let mut object_client = ObjectServiceClient::connect(grpc_addr).await.unwrap();
    let listed = object_client
        .list_objects(authorized(
            ListObjectsRequest {
                bucket_name,
                prefix: "docs/".to_string(),
                start_after: String::new(),
                max_keys: 10,
                delimiter: String::new(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        listed
            .objects
            .iter()
            .map(|object| object.key.as_str())
            .collect::<Vec<_>>(),
        vec!["docs/a.txt", "docs/b.txt"]
    );
}

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
    let put_res = object_client
        .put_object(put_req)
        .await
        .unwrap()
        .into_inner();
    assert!(put_res.watch_cursor > 0);

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
    let delete_res = object_client
        .delete_object(del_req)
        .await
        .unwrap()
        .into_inner();
    assert!(delete_res.watch_cursor > put_res.watch_cursor);
    assert!(delete_res.delete_marker);
    assert!(!delete_res.version_id.is_empty());
    assert!(!delete_res.mutation_id.is_empty());
    assert!(!delete_res.record_hash.is_empty());

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
        version_id_marker: String::new(),
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
async fn test_delete_object_specific_version_removes_only_that_version() {
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

    let bucket_name = "test-delete-specific-version".to_string();
    let object_key = "versioned-object".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let first_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"v1".to_vec(),
            )),
        },
    ];
    let mut first_put_req = Request::new(tokio_stream::iter(first_chunks));
    first_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first_put = object_client
        .put_object(first_put_req)
        .await
        .unwrap()
        .into_inner();

    let second_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"v2".to_vec(),
            )),
        },
    ];
    let mut second_put_req = Request::new(tokio_stream::iter(second_chunks));
    second_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second_put = object_client
        .put_object(second_put_req)
        .await
        .unwrap()
        .into_inner();

    let mut first_page_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 1,
        version_id_marker: String::new(),
    });
    first_page_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first_page = object_client
        .list_object_versions(first_page_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first_page.versions.len(), 1);
    assert_eq!(first_page.versions[0].version_id, second_put.version_id);
    assert!(first_page.is_truncated);
    assert_eq!(first_page.next_key_marker, object_key);
    assert_eq!(first_page.next_version_id_marker, second_put.version_id);

    let mut second_page_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: first_page.next_key_marker,
        max_keys: 1,
        version_id_marker: first_page.next_version_id_marker,
    });
    second_page_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second_page = object_client
        .list_object_versions(second_page_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second_page.versions.len(), 1);
    assert_eq!(second_page.versions[0].version_id, first_put.version_id);
    assert!(!second_page.is_truncated);
    assert!(second_page.next_key_marker.is_empty());
    assert!(second_page.next_version_id_marker.is_empty());

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(first_put.version_id.clone()),
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.delete_object(delete_req).await.unwrap();

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: object_key.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
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
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].version_id, second_put.version_id);
    assert!(versions[0].is_latest);
    assert!(!versions[0].is_delete_marker);
}

#[tokio::test]
async fn test_utf8_object_keys_with_spaces_round_trip() {
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

    let bucket_name = "test-utf8-object-keys".to_string();
    let object_key = "folder/my café document 📄.txt".to_string();
    let payload = b"utf8 object key payload".to_vec();
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
            data: Some(anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(payload.clone())),
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
    assert!(!put_res.version_id.is_empty());

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: "folder/".to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 10,
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
    assert_eq!(list_res.objects[0].key, object_key);

    let literal_prefix = "literal/a%_";
    let literal_key = "literal/a%_object.txt";
    let wildcard_decoy_key = "literal/abc-object.txt";
    for (key, body) in [
        (literal_key, b"literal wildcard key".to_vec()),
        (wildcard_decoy_key, b"decoy key".to_vec()),
    ] {
        let chunks = vec![
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Metadata(
                    ObjectMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: key.to_string(),
                    },
                )),
            },
            PutObjectRequest {
                data: Some(anvil_api::put_object_request::Data::Chunk(body)),
            },
        ];
        let mut put_req = Request::new(tokio_stream::iter(chunks));
        put_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        object_client.put_object(put_req).await.unwrap();
    }

    let mut wildcard_prefix_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: literal_prefix.to_string(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 10,
    });
    wildcard_prefix_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let wildcard_prefix_list = object_client
        .list_objects(wildcard_prefix_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(wildcard_prefix_list.objects.len(), 1);
    assert_eq!(wildcard_prefix_list.objects[0].key, literal_key);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key,
        version_id: None,
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
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            downloaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(downloaded, payload);
}

#[tokio::test]
async fn test_listing_omits_reserved_internal_object_keys() {
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

    let bucket_name = "test-listing-hides-internal".to_string();
    let visible_key = "visible/object.txt".to_string();
    let reserved_key = "_anvil/authz/tuples".to_string();

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
                    object_key: visible_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"visible".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist");
    let bucket_id = bucket.id;
    let tenant_id = bucket.tenant_id;
    cluster.states[0]
        .persistence
        .create_object(
            tenant_id,
            bucket_id,
            &reserved_key,
            "reserved-payload-hash",
            0,
            "reserved-etag",
            None,
            None,
            None,
            Some(Vec::new()),
        )
        .await
        .unwrap();

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: String::new(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let objects = object_client
        .list_objects(list_req)
        .await
        .unwrap()
        .into_inner()
        .objects;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].key, visible_key);

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name,
        prefix: String::new(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
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
    assert_eq!(versions.len(), 1);
    assert_eq!(versions[0].key, visible_key);
}

#[tokio::test]
async fn test_native_object_api_rejects_reserved_internal_namespaces() {
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

    let bucket_name = "test-native-reserved-namespace".to_string();
    let visible_key = "visible/source.json".to_string();
    let reserved_key = "_anvil/authz/native-object-api".to_string();
    let reserved_prefix = "_anvil/authz/".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let visible_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: visible_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                br#"{"ok":true}"#.to_vec(),
            )),
        },
    ];
    let mut visible_put = Request::new(tokio_stream::iter(visible_chunks));
    visible_put.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(visible_put).await.unwrap();

    let reserved_put_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: reserved_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                b"must not persist".to_vec(),
            )),
        },
    ];
    let mut reserved_put = Request::new(tokio_stream::iter(reserved_put_chunks));
    reserved_put.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.put_object(reserved_put).await);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        version_id: None,
    });
    get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.get_object(get_req).await);

    let mut head_req = Request::new(HeadObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        version_id: None,
    });
    head_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.head_object(head_req).await);

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        version_id: None,
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.delete_object(delete_req).await);

    let mut list_req = Request::new(ListObjectsRequest {
        bucket_name: bucket_name.clone(),
        prefix: reserved_prefix.clone(),
        delimiter: String::new(),
        start_after: String::new(),
        max_keys: 100,
    });
    list_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.list_objects(list_req).await);

    let mut versions_req = Request::new(ListObjectVersionsRequest {
        bucket_name: bucket_name.clone(),
        prefix: reserved_prefix.clone(),
        key_marker: String::new(),
        max_keys: 100,
        version_id_marker: String::new(),
    });
    versions_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.list_object_versions(versions_req).await);

    let mut copy_from_reserved = Request::new(CopyObjectRequest {
        source_bucket_name: bucket_name.clone(),
        source_object_key: reserved_key.clone(),
        source_version_id: None,
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: "visible/copied-from-reserved.json".to_string(),
    });
    copy_from_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.copy_object(copy_from_reserved).await);

    let mut copy_to_reserved = Request::new(CopyObjectRequest {
        source_bucket_name: bucket_name.clone(),
        source_object_key: visible_key.clone(),
        source_version_id: None,
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: reserved_key.clone(),
    });
    copy_to_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.copy_object(copy_to_reserved).await);

    let mut compose_to_reserved = Request::new(ComposeObjectRequest {
        sources: vec![ComposeObjectSource {
            bucket_name: bucket_name.clone(),
            object_key: visible_key.clone(),
            version_id: None,
        }],
        destination_bucket_name: bucket_name.clone(),
        destination_object_key: reserved_key.clone(),
    });
    compose_to_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.compose_object(compose_to_reserved).await);

    let mut patch_reserved = Request::new(PatchJsonObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
        base_version_id: None,
        merge_patch_json: r#"{"patched":true}"#.to_string(),
    });
    patch_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.patch_json_object(patch_reserved).await);

    let mut manifest_reserved = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: reserved_key.clone(),
        expected_revision: 0,
        manifest_json: "{}".to_string(),
    });
    manifest_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .compare_and_swap_manifest(manifest_reserved)
            .await,
    );

    let mut multipart_reserved = Request::new(InitiateMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: reserved_key.clone(),
    });
    multipart_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .initiate_multipart_upload(multipart_reserved)
            .await,
    );

    let mut create_append_reserved = Request::new(CreateAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: reserved_key.clone(),
    });
    create_append_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .create_append_stream(create_append_reserved)
            .await,
    );

    let mut append_record_reserved = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: reserved_key.clone(),
        stream_id: uuid::Uuid::new_v4().to_string(),
        payload: b"reserved append payload".to_vec(),
    });
    append_record_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .append_stream_record(append_record_reserved)
            .await,
    );

    let mut seal_append_reserved = Request::new(SealAppendStreamSegmentRequest {
        bucket_name: bucket_name.clone(),
        stream_key: reserved_key.clone(),
        stream_id: uuid::Uuid::new_v4().to_string(),
    });
    seal_append_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(
        object_client
            .seal_append_stream_segment(seal_append_reserved)
            .await,
    );

    let mut watch_reserved = Request::new(WatchPrefixRequest {
        bucket_name,
        prefix: reserved_prefix,
        after_cursor: 0,
    });
    watch_reserved.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert_reserved_namespace_status(object_client.watch_prefix(watch_reserved).await);
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
async fn test_inline_payload_threshold_is_recorded_and_readable() {
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

    let bucket_name = "test-inline-payload-bucket".to_string();
    let inline_key = "inline-64k.bin".to_string();
    let external_key = "external-over-64k.bin".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let inline_content = vec![7_u8; 64 * 1024];
    let inline_chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: inline_key.clone(),
                },
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                inline_content.clone(),
            )),
        },
    ];
    let mut inline_put_req = Request::new(tokio_stream::iter(inline_chunks));
    inline_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let inline_put = object_client
        .put_object(inline_put_req)
        .await
        .unwrap()
        .into_inner();

    let external_content = vec![9_u8; DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES + 123];
    let mut external_chunks = vec![PutObjectRequest {
        data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
            ObjectMetadata {
                bucket_name: bucket_name.clone(),
                object_key: external_key.clone(),
            },
        )),
    }];
    external_chunks.extend(
        external_content
            .chunks(1024 * 1024)
            .map(|chunk| PutObjectRequest {
                data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                    chunk.to_vec(),
                )),
            }),
    );
    let mut external_put_req = Request::new(tokio_stream::iter(external_chunks));
    external_put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let external_put = object_client
        .put_object(external_put_req)
        .await
        .unwrap()
        .into_inner();

    let bucket_id = cluster.states[0]
        .persistence
        .get_bucket_by_name(1, &bucket_name)
        .await
        .unwrap()
        .expect("bucket metadata should exist")
        .id;
    let inline_object = cluster.states[0]
        .persistence
        .get_object_version(
            bucket_id,
            &inline_key,
            uuid::Uuid::parse_str(&inline_put.version_id).unwrap(),
        )
        .await
        .unwrap()
        .expect("inline object version should exist");
    assert_eq!(
        inline_object.inline_payload.as_deref(),
        Some(&inline_content[..])
    );

    let external_object = cluster.states[0]
        .persistence
        .get_object_version(
            bucket_id,
            &external_key,
            uuid::Uuid::parse_str(&external_put.version_id).unwrap(),
        )
        .await
        .unwrap()
        .expect("external object version should exist");
    assert!(external_object.inline_payload.is_none());
    let manifest: ExternalChunkManifest = serde_json::from_value(
        external_object
            .shard_map
            .clone()
            .expect("external object should record chunk manifest"),
    )
    .expect("external chunk manifest should decode");
    assert_eq!(manifest.kind, "external_chunks_v1");
    assert_eq!(manifest.chunk_size, DEFAULT_EXTERNAL_CHUNK_SIZE_BYTES);
    assert_eq!(manifest.chunks.len(), 2);
    assert_eq!(
        manifest
            .chunks
            .iter()
            .map(|chunk| chunk.plaintext_length as usize)
            .sum::<usize>(),
        external_content.len()
    );
    for (idx, record) in manifest.chunks.iter().enumerate() {
        assert_eq!(record.chunk_index, idx as u64);
        assert_eq!(record.compression, "none");
        assert!(record.storage_ref.starts_with("_anvil/payloads/chunks/"));
        let path = cluster.states[0].storage.external_chunk_path(
            &external_object.content_hash,
            record.chunk_index,
            &record.payload_chunk_hash,
        );
        assert!(
            path.exists(),
            "external chunk path should exist: {}",
            path.display()
        );
    }

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: inline_key,
        version_id: Some(inline_put.version_id),
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
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            downloaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(downloaded, inline_content);

    let mut external_get_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key: external_key,
        version_id: Some(external_put.version_id),
    });
    external_get_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut external_stream = object_client
        .get_object(external_get_req)
        .await
        .unwrap()
        .into_inner();
    let mut external_downloaded = Vec::new();
    while let Some(chunk) = external_stream.next().await {
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            external_downloaded.extend_from_slice(&bytes);
        }
    }
    assert_eq!(external_downloaded, external_content);
}

#[tokio::test]
async fn test_object_version_records_index_policy_snapshot_and_mutation_metadata() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "object-policy-snapshot-bucket".to_string();
    let object_key = "docs/policy-snapshot.txt".to_string();
    let content = b"policy snapshot content";

    let mut create_bucket = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_bucket.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_bucket).await.unwrap();

    let mut create_index = Request::new(CreateIndexRequest {
        bucket_name: bucket_name.clone(),
        name: "body-text".to_string(),
        kind: "full_text".to_string(),
        selector_json: serde_json::json!({"selector": "object_body_utf8"}).to_string(),
        extractor_json: serde_json::json!({"encoding": "utf8"}).to_string(),
        authorization_mode: "inherit_object".to_string(),
        build_policy_json: serde_json::json!({"require_index_success": false}).to_string(),
    });
    create_index.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    index_client.create_index(create_index).await.unwrap();

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    let expected_policy_hash = cluster.states[0]
        .persistence
        .active_index_policy_snapshot_hash(claims.tenant_id, bucket.id)
        .await
        .unwrap();

    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                ObjectMetadata {
                    bucket_name: bucket_name.clone(),
                    object_key: object_key.clone(),
                },
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

    assert_eq!(put_res.index_policy_snapshot, expected_policy_hash);
    assert_eq!(put_res.payload_hash, put_res.etag);
    assert!(!put_res.mutation_id.is_empty());
    assert!(!put_res.record_hash.is_empty());

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
    assert_eq!(head_res.version_id, put_res.version_id);
    assert_eq!(head_res.mutation_id, put_res.mutation_id);
    assert_eq!(head_res.record_hash, put_res.record_hash);
    assert_eq!(head_res.index_policy_snapshot, expected_policy_hash);
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
async fn test_private_object_read_denied_before_payload_load() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "test-denied-before-payload-load".to_string();
    let object_key = "private/missing-payload.bin".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let claims = cluster.states[0].jwt_manager.verify_token(&token).unwrap();
    let bucket = cluster.states[0]
        .persistence
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
        .unwrap()
        .expect("bucket exists");
    cluster.states[0]
        .persistence
        .create_object(
            claims.tenant_id,
            bucket.id,
            &object_key,
            &hex::encode([42; 32]),
            999,
            "etag-missing-payload",
            Some("application/octet-stream"),
            None,
            None,
            None,
        )
        .await
        .unwrap();

    let limited_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "limited-object-reader".to_string(),
            vec![format!("object:list|{bucket_name}")],
            claims.tenant_id,
        )
        .unwrap();

    let mut denied_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    denied_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", limited_token).parse().unwrap(),
    );
    let denied = object_client
        .get_object(denied_req)
        .await
        .expect_err("read without object:read scope must be denied before payload load");
    assert_eq!(denied.code(), Code::PermissionDenied);
    assert_eq!(denied.message(), "Permission denied");

    let mut allowed_req = Request::new(GetObjectRequest {
        bucket_name,
        object_key,
        version_id: None,
    });
    allowed_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = object_client
        .get_object(allowed_req)
        .await
        .unwrap()
        .into_inner();
    let metadata = stream.next().await.unwrap().unwrap().data.unwrap();
    match metadata {
        anvil_api::get_object_response::Data::Metadata(metadata) => {
            assert_eq!(metadata.content_length, 999);
        }
        anvil_api::get_object_response::Data::Chunk(_) => panic!("first response must be metadata"),
    }
    let payload_error = stream
        .next()
        .await
        .expect("authorized read should attempt payload load")
        .expect_err("missing payload must be reported to authorized readers");
    assert_eq!(payload_error.code(), Code::NotFound);
    assert!(payload_error.message().contains("Object data unavailable"));
}

#[tokio::test]
async fn test_watch_prefix_streams_snapshot_and_live_events() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut watch_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = "test-watch-bucket".to_string();
    let object_key = "docs/a.txt".to_string();

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
                b"watch me".to_vec(),
            )),
        },
    ];
    let mut put_req = Request::new(tokio_stream::iter(chunks));
    put_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.put_object(put_req).await.unwrap();

    let mut watch_req = Request::new(WatchPrefixRequest {
        bucket_name: bucket_name.clone(),
        prefix: "docs/".to_string(),
        after_cursor: 0,
    });
    watch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let mut stream = watch_client
        .watch_prefix(watch_req)
        .await
        .unwrap()
        .into_inner();

    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.bucket_name, bucket_name);
    assert_eq!(first.object_key, object_key);
    assert_eq!(first.event_type, "put");
    assert!(!first.is_delete_marker);
    let first_cursor = first.cursor;

    let mut delete_req = Request::new(DeleteObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: None,
    });
    delete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    object_client.delete_object(delete_req).await.unwrap();

    let second = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(second.cursor > first_cursor);
    assert_eq!(second.bucket_name, bucket_name);
    assert_eq!(second.object_key, object_key);
    assert_eq!(second.event_type, "delete");
    assert!(second.is_delete_marker);
}

#[tokio::test]
async fn test_append_stream_records_are_ordered_and_sealable() {
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

    let bucket_name = "test-append-bucket".to_string();
    let stream_key = "events/topic-a".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let mut create_stream_req = Request::new(CreateAppendStreamRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
    });
    create_stream_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let stream_id = object_client
        .create_append_stream(create_stream_req)
        .await
        .unwrap()
        .into_inner()
        .stream_id;

    let mut first_req = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        payload: b"first".to_vec(),
    });
    first_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first = object_client
        .append_stream_record(first_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.record_sequence, 1);
    assert_eq!(first.payload_size, 5);
    assert!(!first.payload_hash.is_empty());

    let mut second_req = Request::new(AppendStreamRecordRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
        payload: b"second".to_vec(),
    });
    second_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second = object_client
        .append_stream_record(second_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.record_sequence, 2);

    let mut seal_req = Request::new(SealAppendStreamSegmentRequest {
        bucket_name: bucket_name.clone(),
        stream_key: stream_key.clone(),
        stream_id: stream_id.clone(),
    });
    seal_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let sealed = object_client
        .seal_append_stream_segment(seal_req)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(sealed.record_count, 2);
    assert!(!sealed.segment_hash.is_empty());

    let mut append_after_seal = Request::new(AppendStreamRecordRequest {
        bucket_name,
        stream_key,
        stream_id,
        payload: b"must fail".to_vec(),
    });
    append_after_seal.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert!(
        object_client
            .append_stream_record(append_after_seal)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn test_compare_and_swap_manifest_enforces_expected_revision() {
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

    let bucket_name = "test-manifest-bucket".to_string();
    let manifest_key = "manifests/current.json".to_string();
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: "test-region-1".to_string(),
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(create_req).await.unwrap();

    let mut create_manifest = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: manifest_key.clone(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 1}).to_string(),
    });
    create_manifest.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let first = object_client
        .compare_and_swap_manifest(create_manifest)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first.revision, 1);
    assert!(!first.manifest_hash.is_empty());

    let mut stale_update = Request::new(CompareAndSwapManifestRequest {
        bucket_name: bucket_name.clone(),
        manifest_key: manifest_key.clone(),
        expected_revision: 0,
        manifest_json: serde_json::json!({"generation": 2}).to_string(),
    });
    stale_update.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    assert!(
        object_client
            .compare_and_swap_manifest(stale_update)
            .await
            .is_err()
    );

    let mut valid_update = Request::new(CompareAndSwapManifestRequest {
        bucket_name,
        manifest_key,
        expected_revision: first.revision,
        manifest_json: serde_json::json!({"generation": 2}).to_string(),
    });
    valid_update.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let second = object_client
        .compare_and_swap_manifest(valid_update)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(second.revision, 2);
    assert_ne!(second.manifest_hash, first.manifest_hash);
}

#[tokio::test]
async fn test_multipart_upload_completes_ordered_parts() {
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

    let bucket_name = "test-multipart-bucket".to_string();
    let object_key = "multipart.txt".to_string();
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

    let part_payloads = [(1, b"multi".to_vec()), (2, b"part".to_vec())];
    let mut completed_parts = Vec::new();
    for (part_number, payload) in part_payloads {
        let chunks = vec![
            UploadPartRequest {
                data: Some(anvil_api::upload_part_request::Data::Metadata(
                    UploadPartMetadata {
                        bucket_name: bucket_name.clone(),
                        object_key: object_key.clone(),
                        upload_id: upload_id.clone(),
                        part_number,
                    },
                )),
            },
            UploadPartRequest {
                data: Some(anvil_api::upload_part_request::Data::Chunk(payload)),
            },
        ];
        let mut upload_req = Request::new(tokio_stream::iter(chunks));
        upload_req.metadata_mut().insert(
            "authorization",
            format!("Bearer {}", token).parse().unwrap(),
        );
        let upload_part = object_client
            .upload_part(upload_req)
            .await
            .unwrap()
            .into_inner();
        completed_parts.push(CompleteMultipartPart {
            part_number,
            etag: upload_part.etag,
        });
    }

    let mut complete_req = Request::new(CompleteMultipartRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        upload_id,
        parts: completed_parts,
    });
    complete_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let complete_res = object_client
        .complete_multipart_upload(complete_req)
        .await
        .unwrap()
        .into_inner();

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(complete_res.version_id),
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
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            downloaded.extend_from_slice(&bytes);
        }
    }

    assert_eq!(downloaded, b"multipart");
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
async fn test_patch_json_object_writes_new_merged_version() {
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

    let bucket_name = "test-json-patch-bucket".to_string();
    let object_key = "document.json".to_string();

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
        object_key: object_key.clone(),
    };
    let initial_json = br#"{"title":"old","stats":{"open":2,"closed":1},"remove_me":true}"#;
    let chunks = vec![
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Metadata(
                metadata,
            )),
        },
        PutObjectRequest {
            data: Some(anvil::anvil_api::put_object_request::Data::Chunk(
                initial_json.to_vec(),
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

    let mut patch_req = Request::new(PatchJsonObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        base_version_id: Some(put_res.version_id.clone()),
        merge_patch_json: r#"{"title":"new","stats":{"open":3},"remove_me":null}"#.to_string(),
    });
    patch_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let patch_res = object_client
        .patch_json_object(patch_req)
        .await
        .unwrap()
        .into_inner();

    assert_ne!(patch_res.version_id, put_res.version_id);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(patch_res.version_id),
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
        if let anvil_api::get_object_response::Data::Chunk(bytes) = chunk.unwrap().data.unwrap() {
            downloaded.extend_from_slice(&bytes);
        }
    }

    let patched: serde_json::Value = serde_json::from_slice(&downloaded).unwrap();
    assert_eq!(patched["title"], "new");
    assert_eq!(patched["stats"]["open"], 3);
    assert_eq!(patched["stats"]["closed"], 1);
    assert!(patched.get("remove_me").is_none());
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
