use super::*;

#[test]
fn test_s3_public_and_private_access() {
    run_large_s3_gateway_test(Box::pin(run_s3_public_and_private_access()));
}

#[test]
fn test_s3_large_object_ranges_across_docker_cluster() {
    run_large_s3_gateway_test(Box::pin(async {
        let cluster = shared_docker_test_cluster().await;
        let actor = create_docker_app(&cluster, "s3-large-chunks").await;

        let client = s3_client_for_docker_app(&cluster, &actor);
        let bucket_name = unique_test_name("s3-large-chunks");
        let object_key = "large/chunked.bin";
        let object_len = LARGE_OBJECT_RANGE_SPLIT_BYTES + 257;
        let content = (0..object_len)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();

        client
            .create_bucket()
            .bucket(&bucket_name)
            .send()
            .await
            .expect("S3 CreateBucket should succeed");

        client
            .put_object()
            .bucket(&bucket_name)
            .key(object_key)
            .body(ByteStream::from(content.clone()))
            .send()
            .await
            .expect("large S3 PUT should succeed");

        let full_resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(object_key)
            .send()
            .await
            .expect("large S3 GET should succeed");
        let full = full_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(full.as_ref(), content.as_slice());

        let range_start = LARGE_OBJECT_RANGE_SPLIT_BYTES - 8;
        let range_end = LARGE_OBJECT_RANGE_SPLIT_BYTES + 8;
        let range_resp = client
            .get_object()
            .bucket(&bucket_name)
            .key(object_key)
            .range(format!("bytes={range_start}-{range_end}"))
            .send()
            .await
            .expect("S3 range GET across large CoreStore-backed object should succeed");
        assert_eq!(
            range_resp.content_range(),
            Some(format!("bytes {range_start}-{range_end}/{object_len}").as_str())
        );
        let ranged = range_resp.body.collect().await.unwrap().into_bytes();
        assert_eq!(ranged.as_ref(), &content[range_start..=range_end]);
    }));
}

async fn run_s3_public_and_private_access() {
    let cluster = shared_docker_test_cluster().await;
    let actor = create_docker_app(&cluster, "s3-test-app").await;
    let token = actor.token.clone();

    // 1. Create a private and a public bucket
    let private_bucket = unique_test_name("private-s3-bucket");
    let public_bucket = unique_test_name("public-s3-bucket");

    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        actor.grpc_addr.clone(),
    )
    .await
    .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: private_bucket.clone(),
        region: cluster.region.clone(),

        options: None,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: public_bucket.clone(),
        region: cluster.region.clone(),

        options: None,
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    // 2. Set the public bucket to be public
    set_bucket_public_for_docker_app(&actor, &public_bucket).await;

    // 3. Configure AWS S3 client to talk to our local server
    // TestCluster stores gRPC base at /grpc; S3 must hit HTTP root.
    let http_base = actor.grpc_addr.trim_end_matches('/');
    let client = s3_client_for_docker_app(&cluster, &actor);

    let location = client
        .get_bucket_location()
        .bucket(&private_bucket)
        .send()
        .await
        .unwrap();
    assert!(
        format!("{:?}", location.location_constraint()).contains(&cluster.region),
        "bucket location response should include the stored bucket region"
    );

    client
        .put_bucket_versioning()
        .bucket(&private_bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let versioning = client
        .get_bucket_versioning()
        .bucket(&private_bucket)
        .send()
        .await
        .unwrap();
    assert!(
        matches!(versioning.status(), Some(BucketVersioningStatus::Enabled)),
        "bucket versioning should be reported as enabled"
    );

    let deleted_bucket = unique_test_name("delete-s3-bucket");
    client
        .create_bucket()
        .bucket(&deleted_bucket)
        .send()
        .await
        .unwrap();
    client
        .delete_bucket()
        .bucket(&deleted_bucket)
        .send()
        .await
        .unwrap();
    let deleted_head = client.head_bucket().bucket(&deleted_bucket).send().await;
    let deleted_head_debug = format!("{deleted_head:?}");
    assert!(
        deleted_head.is_err()
            && (deleted_head_debug.contains("StatusCode(404)")
                || deleted_head_debug.contains("NotFound")),
        "deleted bucket should no longer be visible: {deleted_head_debug}"
    );

    let active_multipart_bucket = unique_test_name("delete-s3-active-multipart");
    let active_multipart_key = "active-upload.txt";
    client
        .create_bucket()
        .bucket(&active_multipart_bucket)
        .send()
        .await
        .unwrap();
    let active_upload = client
        .create_multipart_upload()
        .bucket(&active_multipart_bucket)
        .key(active_multipart_key)
        .send()
        .await
        .expect("create active multipart upload should succeed");
    let active_upload_id = active_upload
        .upload_id()
        .expect("active upload id")
        .to_string();
    let active_delete = client
        .delete_bucket()
        .bucket(&active_multipart_bucket)
        .send()
        .await;
    assert!(
        format!("{active_delete:?}").contains("BucketNotEmpty"),
        "S3 DeleteBucket must reject active multipart uploads"
    );
    client
        .abort_multipart_upload()
        .bucket(&active_multipart_bucket)
        .key(active_multipart_key)
        .upload_id(&active_upload_id)
        .send()
        .await
        .expect("abort active multipart upload should succeed");
    client
        .delete_bucket()
        .bucket(&active_multipart_bucket)
        .send()
        .await
        .expect("empty bucket should be deletable after aborting multipart upload");

    let unauthenticated_list_buckets = reqwest::get(format!("{}/", http_base)).await.unwrap();
    assert_eq!(unauthenticated_list_buckets.status(), 403);

    let private_key = "private.txt";
    let public_key = "public.txt";
    let private_content = b"this is private content";
    let public_content = b"this is public content";

    // 4. Put an object into each bucket using the S3 client (tests SigV4 auth)
    client
        .put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .content_type("text/plain")
        .metadata("owner", "alice")
        .metadata("purpose", "metadata-test")
        .body(ByteStream::from(private_content.to_vec()))
        .send()
        .await
        .expect("Failed to put private object");

    let head_private = client
        .head_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("HEAD should return object metadata");
    assert_eq!(head_private.content_type(), Some("text/plain"));
    let head_metadata = head_private.metadata().expect("HEAD metadata");
    assert_eq!(
        head_metadata.get("owner").map(String::as_str),
        Some("alice")
    );
    assert_eq!(
        head_metadata.get("purpose").map(String::as_str),
        Some("metadata-test")
    );
    let private_etag = head_private.e_tag().expect("private ETag").to_string();
    client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .if_match(&private_etag)
        .send()
        .await
        .expect("matching If-Match GET should succeed");
    let if_match_mismatch = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .if_match("\"definitely-not-the-current-etag\"")
        .send()
        .await;
    assert!(
        if_match_mismatch.is_err(),
        "mismatched If-Match GET should fail"
    );
    let if_none_match_hit = client
        .head_object()
        .bucket(&private_bucket)
        .key(private_key)
        .if_none_match(&private_etag)
        .send()
        .await;
    assert!(
        if_none_match_hit.is_err(),
        "matching If-None-Match HEAD should return not modified"
    );

    let create_only_key = "create-only.txt";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created once"))
        .send()
        .await
        .expect("If-None-Match create should succeed when object is absent");
    let duplicate_create = client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_none_match("*")
        .body(ByteStream::from_static(b"created twice"))
        .send()
        .await;
    assert!(
        duplicate_create.is_err(),
        "If-None-Match create should reject existing object"
    );

    let create_only_head = client
        .head_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .send()
        .await
        .expect("HEAD should return create-only ETag");
    let create_only_etag = create_only_head
        .e_tag()
        .expect("create-only ETag")
        .to_string();
    client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_match(&create_only_etag)
        .body(ByteStream::from_static(b"updated through If-Match"))
        .send()
        .await
        .expect("matching If-Match PUT should update the object");
    let stale_update = client
        .put_object()
        .bucket(&private_bucket)
        .key(create_only_key)
        .if_match(&create_only_etag)
        .body(ByteStream::from_static(b"stale update"))
        .send()
        .await;
    assert!(
        stale_update.is_err(),
        "stale If-Match PUT should reject the update"
    );

    let utf8_key = "folder/my café document 📄.txt";
    let utf8_content = b"utf8 key over s3";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(utf8_key)
        .body(ByteStream::from_static(utf8_content))
        .send()
        .await
        .expect("put UTF-8 S3 key should succeed");
    let utf8_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(utf8_key)
        .send()
        .await
        .expect("UTF-8 S3 key should be readable");
    let utf8_data = utf8_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(utf8_data.as_ref(), utf8_content);
    let utf8_listing = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("folder/")
        .send()
        .await
        .expect("UTF-8 S3 key should be listable");
    assert!(
        utf8_listing
            .contents()
            .iter()
            .any(|object| object.key() == Some(utf8_key)),
        "list_objects_v2 should include the UTF-8 key"
    );

    let literal_prefix = "literal/a%_";
    let literal_key = "literal/a%_object.txt";
    let wildcard_decoy_key = "literal/abc-object.txt";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(literal_key)
        .body(ByteStream::from_static(b"literal wildcard key"))
        .send()
        .await
        .expect("put literal wildcard-like key should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key(wildcard_decoy_key)
        .body(ByteStream::from_static(b"decoy key"))
        .send()
        .await
        .expect("put wildcard decoy key should succeed");
    let literal_prefix_listing = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix(literal_prefix)
        .send()
        .await
        .expect("literal wildcard-like prefix listing should succeed");
    assert_eq!(literal_prefix_listing.contents().len(), 1);
    assert_eq!(
        literal_prefix_listing.contents()[0].key(),
        Some(literal_key)
    );

    client
        .put_object()
        .bucket(&public_bucket)
        .key(public_key)
        .body(ByteStream::from(public_content.to_vec()))
        .send()
        .await
        .expect("Failed to put public object");

    let delete_nonempty = client.delete_bucket().bucket(&private_bucket).send().await;
    assert!(
        format!("{delete_nonempty:?}").contains("BucketNotEmpty"),
        "S3 DeleteBucket must reject buckets with retained object versions"
    );

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Test Private Access (Success): Use S3 client to get from private bucket
    let resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("Failed to get private object with S3 client");
    assert_eq!(resp.content_type(), Some("text/plain"));
    let get_metadata = resp.metadata().expect("GET metadata");
    assert_eq!(get_metadata.get("owner").map(String::as_str), Some("alice"));
    let data = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), private_content);

    let range_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .range("bytes=5-8")
        .send()
        .await
        .expect("range GET should succeed");
    assert_eq!(range_resp.content_range(), Some("bytes 5-8/23"));
    let range_data = range_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(range_data.as_ref(), b"is p");

    let suffix_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .range("bytes=-7")
        .send()
        .await
        .expect("suffix range GET should succeed");
    assert_eq!(suffix_resp.content_range(), Some("bytes 16-22/23"));
    let suffix_data = suffix_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(suffix_data.as_ref(), b"content");

    let copied_key = "copied-private.txt";
    client
        .copy_object()
        .bucket(&private_bucket)
        .key(copied_key)
        .copy_source(format!("{}/{}", private_bucket, private_key))
        .send()
        .await
        .expect("copy object should succeed");
    let copied_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(copied_key)
        .send()
        .await
        .expect("copied object should be readable");
    let copied_data = copied_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(copied_data.as_ref(), private_content);

    client
        .put_object()
        .bucket(&private_bucket)
        .key("bulk/one.txt")
        .body(ByteStream::from_static(b"one"))
        .send()
        .await
        .expect("put bulk/one.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("bulk/two.txt")
        .body(ByteStream::from_static(b"two"))
        .send()
        .await
        .expect("put bulk/two.txt should succeed");

    let bulk_delete = client
        .delete_objects()
        .bucket(&private_bucket)
        .delete(
            Delete::builder()
                .objects(
                    ObjectIdentifier::builder()
                        .key("bulk/one.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("bulk/two.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("bulk/missing.txt")
                        .build()
                        .unwrap(),
                )
                .objects(
                    ObjectIdentifier::builder()
                        .key("_anvil/authz/bulk-delete")
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("multi-object delete should return a DeleteResult");
    assert_eq!(bulk_delete.deleted().len(), 3);
    assert_eq!(bulk_delete.errors().len(), 1);
    assert_eq!(
        bulk_delete.errors()[0].key(),
        Some("_anvil/authz/bulk-delete")
    );
    assert_eq!(
        bulk_delete.errors()[0].code(),
        Some("UnauthorizedReservedNamespace")
    );

    let bulk_deleted_get = client
        .get_object()
        .bucket(&private_bucket)
        .key("bulk/one.txt")
        .send()
        .await;
    assert!(
        bulk_deleted_get.is_err(),
        "multi-object delete should make bulk/one.txt unreadable"
    );

    client
        .put_object()
        .bucket(&private_bucket)
        .key("page/a.txt")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await
        .expect("put page/a.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("page/b.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .expect("put page/b.txt should succeed");
    let first_page = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .send()
        .await
        .expect("first paged list should succeed");
    assert!(first_page.is_truncated().unwrap_or(false));
    assert_eq!(first_page.contents().len(), 1);
    assert_eq!(first_page.contents()[0].key(), Some("page/a.txt"));
    let second_page = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .continuation_token(first_page.next_continuation_token().expect("next token"))
        .send()
        .await
        .expect("second paged list should succeed");
    assert!(!second_page.is_truncated().unwrap_or(true));
    assert_eq!(second_page.contents().len(), 1);
    assert_eq!(second_page.contents()[0].key(), Some("page/b.txt"));
    let first_v1_page = client
        .list_objects()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .send()
        .await
        .expect("first v1 paged list should succeed");
    assert!(first_v1_page.is_truncated().unwrap_or(false));
    assert_eq!(first_v1_page.contents().len(), 1);
    assert_eq!(first_v1_page.contents()[0].key(), Some("page/a.txt"));
    let second_v1_page = client
        .list_objects()
        .bucket(&private_bucket)
        .prefix("page/")
        .max_keys(1)
        .marker(first_v1_page.next_marker().expect("next marker"))
        .send()
        .await
        .expect("second v1 paged list should succeed");
    assert!(!second_v1_page.is_truncated().unwrap_or(true));
    assert_eq!(second_v1_page.contents().len(), 1);
    assert_eq!(second_v1_page.contents()[0].key(), Some("page/b.txt"));

    client
        .put_object()
        .bucket(&private_bucket)
        .key("tree/root.txt")
        .body(ByteStream::from_static(b"root"))
        .send()
        .await
        .expect("put tree/root.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("tree/a/file.txt")
        .body(ByteStream::from_static(b"a"))
        .send()
        .await
        .expect("put tree/a/file.txt should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key("tree/b/file.txt")
        .body(ByteStream::from_static(b"b"))
        .send()
        .await
        .expect("put tree/b/file.txt should succeed");
    let tree_listing = client
        .list_objects_v2()
        .bucket(&private_bucket)
        .prefix("tree/")
        .delimiter("/")
        .send()
        .await
        .expect("delimiter list should succeed");
    assert_eq!(tree_listing.contents().len(), 1);
    assert_eq!(tree_listing.contents()[0].key(), Some("tree/root.txt"));
    assert_eq!(tree_listing.common_prefixes().len(), 2);
    assert_eq!(tree_listing.common_prefixes()[0].prefix(), Some("tree/a/"));
    assert_eq!(tree_listing.common_prefixes()[1].prefix(), Some("tree/b/"));

    let multipart_key = "multipart-private.txt";
    let multipart = client
        .create_multipart_upload()
        .bucket(&private_bucket)
        .key(multipart_key)
        .send()
        .await
        .expect("create multipart upload should succeed");
    let upload_id = multipart.upload_id().expect("upload id").to_string();
    let part_one = client
        .upload_part()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"multi"))
        .send()
        .await
        .expect("upload multipart part 1 should succeed");
    let part_two = client
        .upload_part()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"part"))
        .send()
        .await
        .expect("upload multipart part 2 should succeed");
    let listed_parts = client
        .list_parts()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .send()
        .await
        .expect("list multipart parts should succeed");
    assert_eq!(listed_parts.parts().len(), 2);
    assert_eq!(listed_parts.parts()[0].part_number(), Some(1));
    assert_eq!(listed_parts.parts()[1].part_number(), Some(2));
    let listed_parts_page_one = client
        .list_parts()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .max_parts(1)
        .send()
        .await
        .expect("list multipart parts first page should succeed");
    assert_eq!(listed_parts_page_one.parts().len(), 1);
    assert_eq!(listed_parts_page_one.parts()[0].part_number(), Some(1));
    assert!(listed_parts_page_one.is_truncated().unwrap_or(false));
    let next_part_number_marker = listed_parts_page_one
        .next_part_number_marker()
        .expect("next part number marker");
    let listed_parts_page_two = client
        .list_parts()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .part_number_marker(next_part_number_marker)
        .max_parts(1)
        .send()
        .await
        .expect("list multipart parts second page should succeed");
    assert_eq!(listed_parts_page_two.parts().len(), 1);
    assert_eq!(listed_parts_page_two.parts()[0].part_number(), Some(2));
    assert!(!listed_parts_page_two.is_truncated().unwrap_or(false));
    client
        .complete_multipart_upload()
        .bucket(&private_bucket)
        .key(multipart_key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .parts(
                    CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part_one.e_tag().expect("part 1 etag"))
                        .build(),
                )
                .parts(
                    CompletedPart::builder()
                        .part_number(2)
                        .e_tag(part_two.e_tag().expect("part 2 etag"))
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("complete multipart upload should succeed");
    let multipart_resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(multipart_key)
        .send()
        .await
        .expect("multipart object should be readable");
    let multipart_data = multipart_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(multipart_data.as_ref(), b"multipart");

    let aborted_key = "aborted-multipart-private.txt";
    let aborted = client
        .create_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .send()
        .await
        .expect("create multipart upload for abort should succeed");
    let aborted_upload_id = aborted.upload_id().expect("abort upload id").to_string();
    let second_aborted = client
        .create_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .send()
        .await
        .expect("create second multipart upload for pagination should succeed");
    let second_aborted_upload_id = second_aborted
        .upload_id()
        .expect("second abort upload id")
        .to_string();
    let active_uploads = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .send()
        .await
        .expect("list multipart uploads should succeed");
    assert_eq!(active_uploads.uploads().len(), 2);
    assert!(active_uploads.uploads().iter().any(|upload| {
        upload
            .upload_id()
            .is_some_and(|upload_id| upload_id == aborted_upload_id)
    }));
    assert!(active_uploads.uploads().iter().any(|upload| {
        upload
            .upload_id()
            .is_some_and(|upload_id| upload_id == second_aborted_upload_id)
    }));
    let active_uploads_page_one = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .max_uploads(1)
        .send()
        .await
        .expect("list multipart uploads first page should succeed");
    assert_eq!(active_uploads_page_one.uploads().len(), 1);
    assert!(active_uploads_page_one.is_truncated().unwrap_or(false));
    let next_key_marker = active_uploads_page_one
        .next_key_marker()
        .expect("next multipart key marker");
    let next_upload_id_marker = active_uploads_page_one
        .next_upload_id_marker()
        .expect("next multipart upload id marker");
    let active_uploads_page_two = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .key_marker(next_key_marker)
        .upload_id_marker(next_upload_id_marker)
        .max_uploads(1)
        .send()
        .await
        .expect("list multipart uploads second page should succeed");
    assert_eq!(active_uploads_page_two.uploads().len(), 1);
    assert!(!active_uploads_page_two.is_truncated().unwrap_or(false));
    client
        .abort_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&aborted_upload_id)
        .send()
        .await
        .expect("abort multipart upload should succeed");
    client
        .abort_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&second_aborted_upload_id)
        .send()
        .await
        .expect("abort second multipart upload should succeed");
    let upload_after_abort = client
        .upload_part()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&aborted_upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"must fail"))
        .send()
        .await;
    assert!(
        upload_after_abort.is_err(),
        "uploading a part after abort must fail"
    );
    let active_uploads_after_abort = client
        .list_multipart_uploads()
        .bucket(&private_bucket)
        .prefix(aborted_key)
        .send()
        .await
        .expect("list multipart uploads after abort should succeed");
    assert!(active_uploads_after_abort.uploads().is_empty());

    // 5b. S3 version listing returns overwritten versions and delete markers.
    client
        .put_object()
        .bucket(&private_bucket)
        .key(private_key)
        .body(ByteStream::from(b"this is private content v2".to_vec()))
        .send()
        .await
        .expect("Failed to overwrite private object");

    let versions_before_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .send()
        .await
        .expect("list object versions should succeed");
    assert_eq!(versions_before_delete.versions().len(), 2);
    assert!(
        versions_before_delete
            .versions()
            .iter()
            .any(|version| version.is_latest().unwrap_or(false))
    );
    let first_versions_page = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .max_keys(1)
        .send()
        .await
        .expect("first paged version listing should succeed");
    assert_eq!(first_versions_page.versions().len(), 1);
    assert!(first_versions_page.is_truncated().unwrap_or(false));
    let next_key_marker = first_versions_page
        .next_key_marker()
        .expect("next key marker")
        .to_string();
    let next_version_id_marker = first_versions_page
        .next_version_id_marker()
        .expect("next version marker")
        .to_string();
    assert_eq!(next_key_marker, private_key);
    let second_versions_page = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .key_marker(next_key_marker)
        .version_id_marker(next_version_id_marker)
        .max_keys(1)
        .send()
        .await
        .expect("second paged version listing should succeed");
    assert_eq!(second_versions_page.versions().len(), 1);
    assert!(!second_versions_page.is_truncated().unwrap_or(true));

    let version_specific_key = "version-specific-delete.txt";
    client
        .put_object()
        .bucket(&private_bucket)
        .key(version_specific_key)
        .body(ByteStream::from_static(b"v1"))
        .send()
        .await
        .expect("put version-specific v1 should succeed");
    client
        .put_object()
        .bucket(&private_bucket)
        .key(version_specific_key)
        .body(ByteStream::from_static(b"v2"))
        .send()
        .await
        .expect("put version-specific v2 should succeed");
    let version_specific_before_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(version_specific_key)
        .send()
        .await
        .expect("list version-specific object versions should succeed");
    let older_version_id = version_specific_before_delete
        .versions()
        .iter()
        .find(|version| !version.is_latest().unwrap_or(false))
        .and_then(|version| version.version_id())
        .expect("older version id")
        .to_string();
    client
        .delete_object()
        .bucket(&private_bucket)
        .key(version_specific_key)
        .version_id(older_version_id)
        .send()
        .await
        .expect("version-specific delete should succeed");
    let version_specific_after_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(version_specific_key)
        .send()
        .await
        .expect("list after version-specific delete should succeed");
    assert_eq!(version_specific_after_delete.versions().len(), 1);
    assert!(version_specific_after_delete.delete_markers().is_empty());

    // 6. Test Public Access (Success): Use reqwest (no auth) to get from public bucket
    let public_url = tenant_routed_public_url(
        http_base,
        docker_actor_tenant_route(&actor),
        &public_bucket,
        public_key,
    );
    let public_resp = reqwest::Client::new()
        .get(&public_url)
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .send()
        .await
        .expect("Failed to make public request");
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // 7. Test Private Access (Failure): Use reqwest (no auth) to get from private bucket
    let private_url = tenant_routed_public_url(
        http_base,
        docker_actor_tenant_route(&actor),
        &private_bucket,
        private_key,
    );
    let private_resp = reqwest::Client::new()
        .get(&private_url)
        .header(reqwest::header::HOST, &cluster.public_region_host)
        .send()
        .await
        .unwrap();
    assert!(
        private_resp.status() == 403 || private_resp.status() == 404,
        "Private bucket should be blocked for anonymous access"
    );

    // 8. Reserved internal namespaces are never readable or writable through S3.
    let reserved_prefixes = [
        "_anvil/meta/",
        "_anvil/index/",
        "_anvil/authz/",
        "_anvil/watch/",
        "_anvil/personaldb/",
        "_anvil/git/",
        "_anvil/tmp/",
    ];
    for reserved_prefix in reserved_prefixes {
        let reserved_key = format!("{reserved_prefix}s3-compat-object");
        let reserved_url = tenant_routed_public_url(
            http_base,
            docker_actor_tenant_route(&actor),
            &public_bucket,
            &reserved_key,
        );

        let reserved_get = reqwest::Client::new()
            .get(&reserved_url)
            .header(reqwest::header::HOST, &cluster.public_region_host)
            .send()
            .await
            .unwrap();
        assert_eq!(reserved_get.status(), 403);
        assert!(
            reserved_get
                .text()
                .await
                .unwrap()
                .contains("UnauthorizedReservedNamespace")
        );

        let reserved_head = reqwest::Client::new()
            .head(&reserved_url)
            .header(reqwest::header::HOST, &cluster.public_region_host)
            .send()
            .await
            .unwrap();
        assert_eq!(reserved_head.status(), 403);

        let reserved_range_get = reqwest::Client::new()
            .get(&reserved_url)
            .header(reqwest::header::HOST, &cluster.public_region_host)
            .header(reqwest::header::RANGE, "bytes=0-1")
            .send()
            .await
            .unwrap();
        assert_eq!(reserved_range_get.status(), 403);
        assert!(
            reserved_range_get
                .text()
                .await
                .unwrap()
                .contains("UnauthorizedReservedNamespace")
        );

        let put_err = client
            .put_object()
            .bucket(&public_bucket)
            .key(&reserved_key)
            .body(ByteStream::from(b"must not be stored".to_vec()))
            .send()
            .await
            .expect_err("reserved namespace PUT must fail");
        assert_reserved_namespace_error(put_err);

        let forged_internal_token_put = reqwest::Client::new()
            .put(format!("{reserved_url}?internal_write_token=caller-forged"))
            .header(reqwest::header::HOST, &cluster.public_region_host)
            .header("x-anvil-internal-write-token", "caller-forged")
            .body("must not be stored")
            .send()
            .await
            .unwrap();
        assert_eq!(forged_internal_token_put.status(), 403);
        assert!(
            forged_internal_token_put
                .text()
                .await
                .unwrap()
                .contains("UnauthorizedReservedNamespace")
        );

        let list_err = client
            .list_objects_v2()
            .bucket(&public_bucket)
            .prefix(reserved_prefix)
            .send()
            .await
            .expect_err("reserved namespace LIST must fail");
        assert_reserved_namespace_error(list_err);

        let list_versions_err = client
            .list_object_versions()
            .bucket(&public_bucket)
            .prefix(reserved_prefix)
            .send()
            .await
            .expect_err("reserved namespace version LIST must fail");
        assert_reserved_namespace_error(list_versions_err);

        // The reserved namespace is not a public object namespace. Failed writes
        // must not materialize an object that later leaks through root listings.
        let root_listing = client
            .list_objects_v2()
            .bucket(&public_bucket)
            .send()
            .await
            .expect("root listing should succeed");
        assert!(
            root_listing
                .contents()
                .iter()
                .all(|object| object.key() != Some(reserved_key.as_str())),
            "S3 LIST must not reveal reserved namespace keys"
        );
        let root_versions = client
            .list_object_versions()
            .bucket(&public_bucket)
            .send()
            .await
            .expect("root version listing should succeed");
        assert!(
            root_versions
                .versions()
                .iter()
                .all(|object| !object.key().unwrap_or_default().starts_with("_anvil/"))
                && root_versions
                    .delete_markers()
                    .iter()
                    .all(|object| !object.key().unwrap_or_default().starts_with("_anvil/")),
            "S3 version LIST must not reveal reserved namespace keys"
        );

        let delete_err = client
            .delete_object()
            .bucket(&public_bucket)
            .key(&reserved_key)
            .send()
            .await
            .expect_err("reserved namespace DELETE must fail");
        assert_reserved_namespace_error(delete_err);

        let copy_from_reserved_err = client
            .copy_object()
            .bucket(&public_bucket)
            .key(format!(
                "copied-from-reserved-{}.txt",
                reserved_prefix.trim_matches('/').replace('/', "-")
            ))
            .copy_source(format!("{}/{}", public_bucket, reserved_key))
            .send()
            .await
            .expect_err("reserved namespace CopyObject source must fail");
        assert_reserved_namespace_error(copy_from_reserved_err);

        let copy_to_reserved_err = client
            .copy_object()
            .bucket(&public_bucket)
            .key(&reserved_key)
            .copy_source(format!("{}/{}", public_bucket, public_key))
            .send()
            .await
            .expect_err("reserved namespace CopyObject destination must fail");
        assert_reserved_namespace_error(copy_to_reserved_err);
    }

    // 9. Normal S3 DELETE remains compatible and idempotent.
    client
        .delete_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("normal S3 delete should succeed");

    let versions_after_delete = client
        .list_object_versions()
        .bucket(&private_bucket)
        .prefix(private_key)
        .send()
        .await
        .expect("list object versions after delete should succeed");
    assert_eq!(versions_after_delete.versions().len(), 2);
    assert_eq!(versions_after_delete.delete_markers().len(), 1);
    assert!(
        versions_after_delete.delete_markers()[0]
            .is_latest()
            .unwrap_or(false),
        "delete marker should be latest after S3 delete"
    );

    let deleted_get = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await;
    assert!(
        deleted_get.is_err(),
        "deleted key must no longer be readable"
    );
}
