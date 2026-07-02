use anvil_storage::{AnvilClient, BearerInterceptor, bearer_metadata, proto};
use tonic::Request;
use tonic::service::Interceptor;
use tonic::transport::Endpoint;

#[test]
fn bearer_metadata_uses_authorization_header_value() {
    let value = bearer_metadata("token-123").expect("bearer metadata should parse");
    assert_eq!(value.to_str().unwrap(), "Bearer token-123");
    assert!(value.is_sensitive());
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
fn bearer_debug_output_redacts_token() {
    let interceptor = BearerInterceptor::new("token-123").expect("interceptor should build");
    let rendered = format!("{interceptor:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("token-123"));
    assert!(!rendered.contains("Bearer token"));
}

#[test]
fn generated_proto_exports_core_service_types() {
    let request = proto::CreateBucketRequest {
        bucket_name: "documents".to_string(),
        region: "eu-west-1".to_string(),
    };
    assert_eq!(request.bucket_name, "documents");

    let scope = proto::AuthzScope {
        anvil_storage_tenant_id: "storage".to_string(),
        authz_realm_id: "realm".to_string(),
    };
    let schema_ref = proto::AuthzSchemaRef {
        schema_id: "default".to_string(),
        schema_revision: 1,
        schema_digest: "digest".to_string(),
    };
    let bind = proto::BindAuthzSchemaRequest {
        scope: Some(scope),
        schema_ref: Some(schema_ref),
        expected_binding_generation: Some(1),
        reason: "test".to_string(),
    };
    assert_eq!(bind.expected_binding_generation, Some(1));
}

#[test]
fn packaged_proto_omits_internal_node_service() {
    let packaged_proto = include_str!("../proto/anvil.proto");
    assert!(!packaged_proto.contains("InternalAnvilService"));
    assert!(!packaged_proto.contains("PutShardRequest"));
    assert!(!packaged_proto.contains("CommitShardRequest"));
    assert!(!packaged_proto.contains("GetShardRequest"));
    assert!(!packaged_proto.contains("DeleteShardRequest"));
}

#[tokio::test]
async fn client_constructs_all_public_service_clients_from_channel() {
    let channel = Endpoint::from_static("http://127.0.0.1:50051").connect_lazy();
    let client = AnvilClient::from_channel_with_bearer(channel, "token-123").unwrap();
    let rendered = format!("{client:?}");
    assert!(rendered.contains("<redacted>"));
    assert!(!rendered.contains("token-123"));

    let _auth = client.auth();
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
