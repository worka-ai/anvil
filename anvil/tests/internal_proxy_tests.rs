use std::time::Duration;

use anvil::anvil_api::internal_proxy_service_client::InternalProxyServiceClient;
use anvil::anvil_api::object_service_client::ObjectServiceClient;
use anvil::anvil_api::{
    GetObjectRequest, ProxyHeader, ProxyRequestChunk, ProxyRequestHeader, proxy_request_chunk,
    proxy_response_chunk,
};
use anvil::auth::Claims;
use anvil_test_utils::TestCluster;
use futures_util::StreamExt;
use tokio_stream::iter;

fn with_bearer<T>(mut request: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    request
        .metadata_mut()
        .insert("authorization", format!("Bearer {token}").parse().unwrap());
    request
}

fn proxy_header(name: &str, value: impl AsRef<[u8]>) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.as_ref().to_vec(),
    }
}

#[tokio::test]
async fn internal_proxy_put_and_get_preserve_original_principal_authority() {
    let mut cluster = TestCluster::new(&["eu-west-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    cluster.create_bucket("proxy-bucket", "eu-west-1").await;

    let original_claims = Claims {
        sub: "test-app".to_string(),
        exp: usize::MAX,
        scopes: vec!["*|*".to_string()],
        tenant_id: 1,
        jti: Some("original-jti".to_string()),
    };
    let internal_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "internal".to_string(),
            vec!["internal:proxy_object|*".to_string()],
            0,
        )
        .unwrap();

    let mut proxy_client = InternalProxyServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let put_header = ProxyRequestHeader {
        request_id: "proxy-put-1".to_string(),
        idempotency_key: "proxy-put-1".to_string(),
        principal_id: original_claims.sub.clone(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: "proxy-bucket".to_string(),
        object_key: "via-proxy.txt".to_string(),
        method: "PUT".to_string(),
        canonical_host: "proxy-bucket.tenant.eu-west-1.anvil-storage.test".to_string(),
        canonical_path: "/via-proxy.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![proxy_header("content-type", "text/plain")],
        authz_context: serde_json::to_vec(&original_claims).unwrap(),
    };
    let put_stream = iter(vec![
        ProxyRequestChunk {
            part: Some(proxy_request_chunk::Part::Header(put_header)),
        },
        ProxyRequestChunk {
            part: Some(proxy_request_chunk::Part::Body(
                b"written through proxy".to_vec(),
            )),
        },
    ]);
    let mut put_response = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(put_stream),
            &internal_token,
        ))
        .await
        .unwrap()
        .into_inner();
    let put_header = put_response.next().await.unwrap().unwrap();
    let proxy_response_chunk::Part::Header(put_response_header) = put_header.part.unwrap() else {
        panic!("proxy put must return a response header first");
    };
    assert_eq!(put_response_header.status, 200);
    assert!(put_response_header.committed);
    assert!(put_response_header.headers.iter().any(|h| h.name == "etag"));
    assert!(put_response.next().await.is_none());

    let get_header = ProxyRequestHeader {
        request_id: "proxy-get-1".to_string(),
        idempotency_key: "".to_string(),
        principal_id: original_claims.sub.clone(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: "proxy-bucket".to_string(),
        object_key: "via-proxy.txt".to_string(),
        method: "GET".to_string(),
        canonical_host: "proxy-bucket.tenant.eu-west-1.anvil-storage.test".to_string(),
        canonical_path: "/via-proxy.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![],
        authz_context: serde_json::to_vec(&original_claims).unwrap(),
    };
    let get_stream = iter(vec![ProxyRequestChunk {
        part: Some(proxy_request_chunk::Part::Header(get_header)),
    }]);
    let mut get_response = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(get_stream),
            &internal_token,
        ))
        .await
        .unwrap()
        .into_inner();
    let first = get_response.next().await.unwrap().unwrap();
    let proxy_response_chunk::Part::Header(get_response_header) = first.part.unwrap() else {
        panic!("proxy get must return a response header first");
    };
    assert_eq!(get_response_header.status, 200);
    assert_eq!(
        get_response_header
            .headers
            .iter()
            .find(|h| h.name == "content-type")
            .map(|h| String::from_utf8_lossy(&h.value).to_string())
            .as_deref(),
        Some("text/plain")
    );
    let mut body = Vec::new();
    while let Some(chunk) = get_response.next().await {
        let chunk = chunk.unwrap();
        if let Some(proxy_response_chunk::Part::Body(bytes)) = chunk.part {
            body.extend_from_slice(&bytes);
        }
    }
    assert_eq!(body, b"written through proxy");

    let mut object_client = ObjectServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let normal_get = object_client
        .get_object(with_bearer(
            tonic::Request::new(GetObjectRequest {
                bucket_name: "proxy-bucket".to_string(),
                object_key: "via-proxy.txt".to_string(),
                version_id: None,
            }),
            &cluster.token,
        ))
        .await
        .unwrap()
        .into_inner();
    tokio::pin!(normal_get);
    let mut normal_body = Vec::new();
    while let Some(chunk) = normal_get.next().await {
        if let Some(anvil::anvil_api::get_object_response::Data::Chunk(bytes)) = chunk.unwrap().data
        {
            normal_body.extend_from_slice(&bytes);
        }
    }
    assert_eq!(normal_body, b"written through proxy");
}

#[tokio::test]
async fn internal_proxy_rejects_mismatched_original_principal() {
    let mut cluster = TestCluster::new(&["eu-west-1"]).await;
    cluster.start_and_converge(Duration::from_secs(10)).await;
    cluster
        .create_bucket("proxy-auth-bucket", "eu-west-1")
        .await;

    let original_claims = Claims {
        sub: "test-app".to_string(),
        exp: usize::MAX,
        scopes: vec!["*|*".to_string()],
        tenant_id: 1,
        jti: None,
    };
    let internal_token = cluster.states[0]
        .jwt_manager
        .mint_token(
            "internal".to_string(),
            vec!["internal:proxy_object|*".to_string()],
            0,
        )
        .unwrap();

    let mut proxy_client = InternalProxyServiceClient::connect(cluster.grpc_addrs[0].clone())
        .await
        .unwrap();
    let get_header = ProxyRequestHeader {
        request_id: "proxy-bad-principal".to_string(),
        idempotency_key: "".to_string(),
        principal_id: "other-app".to_string(),
        tenant_id: original_claims.tenant_id.to_string(),
        bucket_name: "proxy-auth-bucket".to_string(),
        object_key: "missing.txt".to_string(),
        method: "GET".to_string(),
        canonical_host: "proxy-auth-bucket.tenant.eu-west-1.anvil-storage.test".to_string(),
        canonical_path: "/missing.txt".to_string(),
        bucket_locator_generation: 1,
        headers: vec![],
        authz_context: serde_json::to_vec(&original_claims).unwrap(),
    };
    let err = proxy_client
        .proxy_object(with_bearer(
            tonic::Request::new(iter(vec![ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Header(get_header)),
            }])),
            &internal_token,
        ))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
