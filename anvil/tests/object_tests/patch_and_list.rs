use super::*;

#[tokio::test]
async fn test_patch_json_object_writes_new_merged_version() {
    let cluster = shared_docker_test_cluster().await;
    let actor =
        create_object_test_actor(&cluster, "patch-json-object-writes-new-merged-version").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("json-patch");
    let object_key = "document.json".to_string();

    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let metadata = ObjectMetadata {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "object-metadata",
        )),
        content_type: None,
        user_metadata_json: String::new(),
        storage_class: None,
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
        mutation_context: Some(native_mutation_context(
            &actor,
            bucket_id,
            "patch-json-object",
        )),
        precondition: None,
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
    assert_native_mutation_response!(patch_res);
    assert!(patch_res.watch_cursor > put_res.watch_cursor);

    let mut get_req = Request::new(GetObjectRequest {
        bucket_name: bucket_name.clone(),
        object_key: object_key.clone(),
        version_id: Some(patch_res.version_id),
        range: None,

        ..Default::default()
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
    let cluster = shared_docker_test_cluster().await;
    let actor = create_object_test_actor(&cluster, "list-objects-with-delimiter").await;

    let grpc_addr = actor.grpc_addr.clone();
    let token = actor.token.clone();
    let mut object_client = ObjectServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();

    let bucket_name = unique_test_name("delimiter");
    let mut create_req = Request::new(CreateBucketRequest {
        bucket_name: bucket_name.clone(),
        region: actor.region.clone(),

        options: None,
    });
    create_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    let bucket_id = bucket_client
        .create_bucket(create_req)
        .await
        .unwrap()
        .into_inner()
        .bucket_id;

    let keys = vec!["a/b.txt", "a/c.txt", "d.txt"];
    for key in keys {
        let metadata = ObjectMetadata {
            bucket_name: bucket_name.clone(),
            object_key: key.to_string(),
            mutation_context: Some(native_mutation_context(
                &actor,
                bucket_id,
                "object-metadata",
            )),
            content_type: None,
            user_metadata_json: String::new(),
            storage_class: None,
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
