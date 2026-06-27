use anvil::anvil_api::auth_service_client::AuthServiceClient;
use anvil::anvil_api::{GetAccessTokenRequest, SetPublicAccessRequest};
use aws_sdk_s3::Client;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use rand::random;
use std::env::temp_dir;
use std::path::PathBuf;
use std::time::Duration;
use tokio::fs;

use anvil_test_utils::*;

fn assert_access_denied(error: impl std::fmt::Debug) {
    let rendered = format!("{error:?}");
    assert!(
        rendered.contains("AccessDenied"),
        "expected AccessDenied error, got {rendered}"
    );
}

// Helper function to create an app, since it's used in auth tests.
fn create_app(global_db_url: &str, app_name: &str) -> (String, String) {
    let admin_args = &["run", "--bin", "admin", "--"];
    let app_output = std::process::Command::new("cargo")
        .args(admin_args.iter().chain(&[
            "--global-database-url",
            global_db_url,
            "--anvil-secret-encryption-key",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "app",
            "create",
            "--tenant-name",
            "default",
            "--app-name",
            app_name,
        ]))
        .output()
        .unwrap();
    assert!(app_output.status.success());
    let creds = String::from_utf8(app_output.stdout).unwrap();
    let client_id = extract_credential(&creds, "Client ID");
    let client_secret = extract_credential(&creds, "Client Secret");
    (client_id, client_secret)
}

// Helper to get a token for specific scopes.
async fn get_token_for_scopes(
    grpc_addr: &str,
    client_id: &str,
    client_secret: &str,
    scopes: Vec<String>,
) -> String {
    let mut auth_client = AuthServiceClient::connect(grpc_addr.to_string())
        .await
        .unwrap();
    auth_client
        .get_access_token(GetAccessTokenRequest {
            client_id: client_id.to_string(),
            client_secret: client_secret.to_string(),
            scopes,
        })
        .await
        .unwrap()
        .into_inner()
        .access_token
}

#[tokio::test]
async fn test_s3_public_and_private_access() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let (client_id, client_secret) = create_app(&cluster.global_db_url, "s3-test-app");

    // Grant wildcard policy to the app before getting a token
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policy",
        "grant",
        "--app-name",
        "s3-test-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let status = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--global-database-url",
                    &cluster.global_db_url,
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ])
                .chain(policy_args.iter()),
        )
        .status()
        .unwrap();
    assert!(status.success());

    // Allow a moment for the policy change to propagate or be read by the server.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &client_id,
        &client_secret,
        vec![
            "bucket:create|*".to_string(),
            "policy:grant|*".to_string(),
            "object:write|*".to_string(),
            "object:read|*".to_string(),
        ],
    )
    .await;

    // 1. Create a private and a public bucket
    let private_bucket = "private-s3-bucket".to_string();
    let public_bucket = "public-s3-bucket".to_string();

    let mut bucket_client = anvil::anvil_api::bucket_service_client::BucketServiceClient::connect(
        cluster.grpc_addrs[0].clone(),
    )
    .await
    .unwrap();
    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: private_bucket.clone(),
        region: "test-region-1".to_string(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    let mut req = tonic::Request::new(anvil::anvil_api::CreateBucketRequest {
        bucket_name: public_bucket.clone(),
        region: "test-region-1".to_string(),
    });
    req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    bucket_client.create_bucket(req).await.unwrap();

    // 2. Set the public bucket to be public
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: public_bucket.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    // 3. Configure AWS S3 client to talk to our local server
    let credentials = aws_sdk_s3::config::Credentials::new(
        &client_id,
        &client_secret,
        None, // session token
        None, // expiry
        "static",
    );

    // TestCluster stores gRPC base at /grpc; S3 must hit HTTP root.
    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region"))
        .endpoint_url(http_base)
        .behavior_version_latest()
        .build();
    let client = Client::from_conf(config);

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
        .body(ByteStream::from(private_content.to_vec()))
        .send()
        .await
        .expect("Failed to put private object");

    client
        .put_object()
        .bucket(&public_bucket)
        .key(public_key)
        .body(ByteStream::from(public_content.to_vec()))
        .send()
        .await
        .expect("Failed to put public object");

    tokio::time::sleep(Duration::from_millis(200)).await;

    // 5. Test Private Access (Success): Use S3 client to get from private bucket
    let resp = client
        .get_object()
        .bucket(&private_bucket)
        .key(private_key)
        .send()
        .await
        .expect("Failed to get private object with S3 client");
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
    client
        .abort_multipart_upload()
        .bucket(&private_bucket)
        .key(aborted_key)
        .upload_id(&aborted_upload_id)
        .send()
        .await
        .expect("abort multipart upload should succeed");
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

    // 6. Test Public Access (Success): Use reqwest (no auth) to get from public bucket
    let public_url = format!("{}/{}/{}", http_base, public_bucket, public_key);
    let public_resp = reqwest::get(&public_url)
        .await
        .expect("Failed to make public request");
    assert_eq!(public_resp.status(), 200);
    let public_data = public_resp.bytes().await.unwrap();
    assert_eq!(public_data.as_ref(), public_content);

    // 7. Test Private Access (Failure): Use reqwest (no auth) to get from private bucket
    let private_url = format!("{}/{}/{}", http_base, private_bucket, private_key);
    let private_resp = reqwest::get(&private_url).await.unwrap();
    assert!(
        private_resp.status() == 403 || private_resp.status() == 404,
        "Private bucket should be blocked for anonymous access"
    );

    // 8. Reserved internal namespaces are never readable or writable through S3.
    let reserved_key = "_anvil/authz/tuples";
    let reserved_url = format!("{}/{}/{}", http_base, public_bucket, reserved_key);

    let reserved_get = reqwest::get(&reserved_url).await.unwrap();
    assert_eq!(reserved_get.status(), 403);
    assert!(reserved_get.text().await.unwrap().contains("AccessDenied"));

    let reserved_head = reqwest::Client::new()
        .head(&reserved_url)
        .send()
        .await
        .unwrap();
    assert_eq!(reserved_head.status(), 403);

    let put_err = client
        .put_object()
        .bucket(&public_bucket)
        .key(reserved_key)
        .body(ByteStream::from(b"must not be stored".to_vec()))
        .send()
        .await
        .expect_err("reserved namespace PUT must fail");
    assert_access_denied(put_err);

    let list_err = client
        .list_objects_v2()
        .bucket(&public_bucket)
        .prefix("_anvil/authz/")
        .send()
        .await
        .expect_err("reserved namespace LIST must fail");
    assert_access_denied(list_err);

    let delete_err = client
        .delete_object()
        .bucket(&public_bucket)
        .key(reserved_key)
        .send()
        .await
        .expect_err("reserved namespace DELETE must fail");
    assert_access_denied(delete_err);

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

#[tokio::test]
async fn test_streaming_upload_decoding() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let (client_id, client_secret) = create_app(&cluster.global_db_url, "streaming-decode-app");

    // Grant wildcard policy to the app
    let admin_args = &["run", "--bin", "admin", "--"];
    let policy_args = &[
        "policy",
        "grant",
        "--app-name",
        "streaming-decode-app",
        "--action",
        "*",
        "--resource",
        "*",
    ];
    let status = std::process::Command::new("cargo")
        .args(
            admin_args
                .iter()
                .chain(&[
                    "--global-database-url",
                    &cluster.global_db_url,
                    "--anvil-secret-encryption-key",
                    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                ])
                .chain(policy_args.iter()),
        )
        .status()
        .unwrap();
    assert!(status.success());

    tokio::time::sleep(Duration::from_secs(1)).await;

    // Configure S3 client
    let credentials =
        aws_sdk_s3::config::Credentials::new(&client_id, &client_secret, None, None, "static");
    let http_base = cluster.grpc_addrs[0].trim_end_matches('/');
    let config = aws_sdk_s3::Config::builder()
        .credentials_provider(credentials)
        .region(aws_sdk_s3::config::Region::new("test-region-1"))
        .endpoint_url(http_base)
        .force_path_style(true)
        .behavior_version_latest()
        .build();
    let client = Client::from_conf(config);

    let bucket_name = format!("streaming-decode-test-{}", uuid::Uuid::new_v4());
    client
        .create_bucket()
        .bucket(&bucket_name)
        .send()
        .await
        .unwrap();

    let object_key = "my-streamed-object.txt";
    let original_content = "This is the content that will be streamed with aws-chunked encoding and should be decoded.";

    // 1. Upload the object using a true stream, which forces aws-chunked encoding.
    let stream = original_content.as_bytes().to_vec();
    let _content_len = stream.len();
    // let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(16);
    // tokio::spawn(async move {
    //     for chunk in stream.into_chunks::<5>() {
    //         tx.send(bytes::Bytes::copy_from_slice(&chunk)).await.unwrap();
    //     }
    // });
    // // turn the receiver into a Body that yields http-body 1.0 Frames
    // let stream = ReceiverStream::new(rx).map(|b| Ok::<hyper::body::Frame<bytes::Bytes>, Infallible>(Frame::data(b)));
    // let body = StreamBody::new(stream);
    let mut file = PathBuf::new();
    file.push(temp_dir());
    file.push(format!("worka-test-streaming-{}", random::<i32>()));
    fs::write(file.as_path(), original_content).await.unwrap();
    let bytestream = ByteStream::read_from()
        .path(file.as_path())
        // Specify the size of the buffer used to read the file (in bytes, default is 4096)
        //.buffer_size(content_len as u64)
        // Specify the length of the file used (skips an additional call to retrieve the size)
        //.length(aws_sdk_s3::primitives::Length::Exact(content_len as i64))
        .build()
        .await
        .expect("valid path");

    client
        .put_object()
        .bucket(&bucket_name)
        .key(object_key)
        //.body(ByteStream::new(SdkBody::from_body_1_x(body)))
        .body(bytestream)
        .send()
        .await
        .expect("Failed to put streaming object");

    // 2. Make the bucket public so we can test with an unauthenticated client.
    let mut auth_client = AuthServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let token = get_token_for_scopes(
        &cluster.grpc_addrs[0],
        &client_id,
        &client_secret,
        vec!["policy:grant|*".to_string()],
    )
    .await;
    let mut public_req = tonic::Request::new(SetPublicAccessRequest {
        bucket: bucket_name.clone(),
        allow_public_read: true,
    });
    public_req.metadata_mut().insert(
        "authorization",
        format!("Bearer {}", token).parse().unwrap(),
    );
    auth_client.set_public_access(public_req).await.unwrap();

    tokio::time::sleep(Duration::from_secs(1)).await;

    // 3. Download the object using a simple HTTP client (reqwest).
    let object_url = format!("{}/{}/{}", http_base, bucket_name, object_key);
    let response = reqwest::get(&object_url).await.unwrap();

    // 4. Verify the response is successful and the body is clean.
    assert_eq!(response.status(), 200, "Expected a successful GET request");
    let downloaded_content = response.text().await.unwrap();

    // This is the critical assertion: the downloaded content must be exactly what we
    // uploaded, with no chunked-encoding metadata.
    assert_eq!(downloaded_content, original_content);
}
