use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{self, CreateBucketRequest, ListObjectsRequest, ObjectMetadata, PutObjectRequest};
use anvil::run;
use tonic::Request;

// Helper function to wait for a port to be available
async fn wait_for_port(port: u16) {
    let addr = format!("127.0.0.1:{}", port);
    for _ in 0..10 {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    panic!("Timed out waiting for port {}", port);
}

#[tokio::test]
async fn test_multi_region_list_and_isolation() {
    dotenvy::dotenv().ok();

    // Set env vars for the test thread
    unsafe {
        std::env::set_var("GLOBAL_DATABASE_URL", std::env::var("DATABASE_URL").unwrap());
        std::env::set_var("DATABASE_URL_REGION_US_EAST_1", std::env::var("DATABASE_URL").unwrap());
        std::env::set_var("DATABASE_URL_REGION_EU_WEST_1", std::env::var("DATABASE_URL").unwrap());
    }

    // 1. Start a node in us-east-1
    tokio::spawn(async {
        unsafe { std::env::set_var("REGION", "US_EAST_1"); }
        run("127.0.0.1:50090".parse().unwrap()).await.unwrap();
    });

    // 2. Start a node in eu-west-1
    tokio::spawn(async {
        unsafe { std::env::set_var("REGION", "EU_WEST_1"); }
        run("127.0.0.1:50091".parse().unwrap()).await.unwrap();
    });

    // 3. Wait for both servers to be ready
    wait_for_port(50090).await;
    wait_for_port(50091).await;

    // 4. Connect to the us-east-1 node
    let mut bucket_client_us = BucketServiceClient::connect("http://127.0.0.1:50090").await.unwrap();
    let mut object_client_us = ObjectServiceClient::connect("http://127.0.0.1:50090").await.unwrap();

    // 5. Create a bucket in us-east-1
    let bucket_name_us = "test-bucket-us".to_string();
    bucket_client_us.create_bucket(CreateBucketRequest { bucket_name: bucket_name_us.clone() }).await.unwrap();

    // 6. Put objects into the us-east-1 bucket
    put_object(&mut object_client_us, &bucket_name_us, "a/b/1.txt").await;
    put_object(&mut object_client_us, &bucket_name_us, "a/b/2.txt").await;

    // 7. List objects with a prefix
    let list_req = ListObjectsRequest {
        bucket_name: bucket_name_us.clone(),
        prefix: "a/b/".to_string(),
        start_after: "".to_string(),
        max_keys: 10,
        delimiter: "".to_string(),
    };
    let response = object_client_us.list_objects(list_req).await.unwrap().into_inner();
    assert_eq!(response.objects.len(), 2);

    // 8. Verify that the objects do NOT appear when listing from the eu-west-1 node
    let mut object_client_eu = ObjectServiceClient::connect("http://127.0.0.1:50091").await.unwrap();
    let list_req_eu = ListObjectsRequest {
        bucket_name: bucket_name_us.clone(),
        prefix: "".to_string(),
        start_after: "".to_string(),
        max_keys: 10,
        delimiter: "".to_string(),
    };
    let err = object_client_eu.list_objects(list_req_eu).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);
}

async fn put_object(client: &mut ObjectServiceClient<tonic::transport::Channel>, bucket: &str, key: &str) {
    let metadata = ObjectMetadata { bucket_name: bucket.to_string(), object_key: key.to_string() };
    let data = b"test data".to_vec();
    let chunks = vec![
        PutObjectRequest { data: Some(anvil_api::put_object_request::Data::Metadata(metadata)) },
        PutObjectRequest { data: Some(anvil_api::put_object_request::Data::Chunk(data)) },
    ];
    let req = Request::new(tokio_stream::iter(chunks));
    client.put_object(req).await.unwrap();
}