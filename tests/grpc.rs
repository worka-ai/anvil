use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{self, GetObjectRequest, ObjectMetadata, PutObjectRequest};
use anvil::persistence::Persistence;
use futures_util::StreamExt;
use sqlx::PgPool;
use std::env;
use tonic::transport::Channel;
use tonic::Request;

// Helper function to set up the database and return a persistence layer
async fn setup_db() -> Persistence {
    dotenvy::dotenv().ok();
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set for tests");
    let pool = PgPool::connect(&db_url).await.unwrap();
    sqlx::migrate!().run(&pool).await.unwrap();
    Persistence::new(pool)
}

// Helper function to connect to the server
async fn get_client() -> ObjectServiceClient<Channel> {
    ObjectServiceClient::connect("http://[::1]:50051").await.unwrap()
}

#[tokio::test]
async fn test_put_and_get_object() {
    // Start the server in a background task
    tokio::spawn(anvil::run());

    // Set up the database and create a tenant and bucket
    let db = setup_db().await;
    let tenant_name = format!("test-tenant-grpc-{}", uuid::Uuid::new_v4());
    let tenant = db.create_tenant(&tenant_name, "test-key").await.unwrap();
    let bucket = db.create_bucket(tenant.id, "test-bucket", "test-region").await.unwrap();

    let mut client = get_client().await;

    let object_key = format!("test-object-{}", uuid::Uuid::new_v4());
    let data = b"hello from the client!".to_vec();

    // --- PutObject --- //
    let metadata = ObjectMetadata {
        bucket_name: bucket.name.clone(),
        object_key: object_key.clone(),
    };

    let mut chunks = vec![
        PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Metadata(metadata)),
        },
    ];
    for chunk in data.chunks(10) {
        chunks.push(PutObjectRequest {
            data: Some(anvil_api::put_object_request::Data::Chunk(chunk.to_vec())),
        });
    }

    let request_stream = tokio_stream::iter(chunks);
    let response = client.put_object(request_stream).await.unwrap().into_inner();

    assert!(!response.etag.is_empty());
    assert!(!response.version_id.is_empty());

    // --- GetObject --- //
    let get_request = GetObjectRequest {
        bucket_name: bucket.name,
        object_key,
        version_id: String::new(), // Get latest
    };

    let mut stream = client.get_object(get_request).await.unwrap().into_inner();

    let mut received_data = Vec::new();
    while let Some(Ok(chunk)) = stream.next().await {
        if let Some(anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.data {
            received_data.extend_from_slice(&bytes);
        }
    }

    assert_eq!(data, received_data);
}