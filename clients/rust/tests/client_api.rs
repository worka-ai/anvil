use anvil_storage_client::{AnvilClient, BearerInterceptor, bearer_metadata, proto};
use tonic::Request;
use tonic::service::Interceptor;
use tonic::transport::Endpoint;

#[test]
fn bearer_metadata_uses_authorization_header_value() {
    let value = bearer_metadata("token-123").expect("bearer metadata should parse");
    assert_eq!(value.to_str().unwrap(), "Bearer token-123");
}

#[test]
fn bearer_interceptor_inserts_authorization_metadata() {
    let mut interceptor = BearerInterceptor::new("token-123").expect("interceptor should build");
    let request = interceptor
        .call(Request::new(()))
        .expect("request should pass");
    assert_eq!(
        request
            .metadata()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap(),
        "Bearer token-123"
    );
}

#[test]
fn generated_proto_exports_core_service_types() {
    let request = proto::CreateBucketRequest {
        bucket_name: "documents".to_string(),
        region: "eu-west-1".to_string(),
    };
    assert_eq!(request.bucket_name, "documents");
}

#[tokio::test]
async fn client_constructs_all_public_service_clients_from_channel() {
    let channel = Endpoint::from_static("http://127.0.0.1:50051").connect_lazy();
    let client = AnvilClient::from_channel_with_bearer(channel, "token-123").unwrap();

    let _auth = client.auth();
    let _internal = client.internal();
    let _buckets = client.buckets();
    let _objects = client.objects();
    let _indexes = client.indexes();
    let _git = client.git_sources();
    let _personaldb = client.personaldb();
    let _repair = client.repair();
    let _keys = client.hugging_face_keys();
    let _hf = client.hf_ingestion();
    let _models = client.models();
}
