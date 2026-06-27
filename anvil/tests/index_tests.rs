use anvil::anvil_api::bucket_service_client::BucketServiceClient;
use anvil::anvil_api::index_service_client::IndexServiceClient;
use anvil::anvil_api::{
    CreateBucketRequest, CreateIndexRequest, DisableIndexRequest, DropIndexRequest,
    ListIndexesRequest, UpdateIndexRequest, WatchIndexDefinitionRequest,
};
use anvil_test_utils::*;
use futures_util::StreamExt;
use std::time::Duration;
use tonic::Request;

fn authorized<T>(message: T, token: &str) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {token}").parse().expect("valid token"),
    );
    request
}

#[tokio::test]
async fn test_index_definition_lifecycle() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-definition-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let created = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
                kind: "full_text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/"}).to_string(),
                extractor_json: serde_json::json!({"fields": [{"path": "body", "source": "utf8"}]})
                    .to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: serde_json::json!({"positions": true, "language": "simple"})
                    .to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("created index");

    assert_eq!(created.bucket_name, bucket_name);
    assert_eq!(created.name, "docs-full-text");
    assert_eq!(created.kind, "full_text");
    assert_eq!(created.authorization_mode, "inherit_object");
    assert!(created.enabled);
    assert_eq!(created.version, 1);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&created.selector_json).unwrap()["prefix"],
        "docs/"
    );

    let listed = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name, "docs-full-text");

    let updated = index_client
        .update_index(authorized(
            UpdateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
                selector_json: serde_json::json!({"prefix": "docs/v2/"}).to_string(),
                extractor_json:
                    serde_json::json!({"fields": [{"path": "summary", "source": "utf8"}]})
                        .to_string(),
                authorization_mode: "index_only".to_string(),
                build_policy_json: serde_json::json!({"positions": false, "language": "simple"})
                    .to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("updated index");
    assert_eq!(updated.authorization_mode, "index_only");
    assert_eq!(updated.version, 2);
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&updated.selector_json).unwrap()["prefix"],
        "docs/v2/"
    );

    let disabled = index_client
        .disable_index(authorized(
            DisableIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .index
        .expect("disabled index");
    assert!(!disabled.enabled);
    assert_eq!(disabled.version, 3);

    let active_only = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: false,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert!(active_only.is_empty());

    let with_disabled = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name: bucket_name.clone(),
                include_disabled: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert_eq!(with_disabled.len(), 1);
    assert!(!with_disabled[0].enabled);

    index_client
        .drop_index(authorized(
            DropIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "docs-full-text".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let after_drop = index_client
        .list_indexes(authorized(
            ListIndexesRequest {
                bucket_name,
                include_disabled: true,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner()
        .indexes;
    assert!(after_drop.is_empty());

    let mut watch = index_client
        .watch_index_definition(authorized(
            WatchIndexDefinitionRequest {
                bucket_name: "index-definition-bucket".to_string(),
                after_cursor: 0,
            },
            &token,
        ))
        .await
        .unwrap()
        .into_inner();
    let mut events = Vec::new();
    for _ in 0..4 {
        events.push(watch.next().await.unwrap().unwrap());
    }
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type.as_str())
            .collect::<Vec<_>>(),
        vec!["create", "update", "disable", "drop"]
    );
    assert!(
        events
            .windows(2)
            .all(|pair| pair[0].cursor < pair[1].cursor)
    );
    assert_eq!(events[3].index.as_ref().unwrap().name, "docs-full-text");
}

#[tokio::test]
async fn test_index_definition_rejects_invalid_policy_shape() {
    let mut cluster = TestCluster::new(&["test-region-1"]).await;
    cluster.start_and_converge(Duration::from_secs(5)).await;

    let grpc_addr = cluster.grpc_addrs[0].clone();
    let token = cluster.token.clone();
    let mut bucket_client = BucketServiceClient::connect(grpc_addr.clone())
        .await
        .unwrap();
    let mut index_client = IndexServiceClient::connect(grpc_addr).await.unwrap();

    let bucket_name = "index-validation-bucket".to_string();
    bucket_client
        .create_bucket(authorized(
            CreateBucketRequest {
                bucket_name: bucket_name.clone(),
                region: "test-region-1".to_string(),
            },
            &token,
        ))
        .await
        .unwrap();

    let invalid_kind = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name: bucket_name.clone(),
                name: "invalid-kind".to_string(),
                kind: "unsupported".to_string(),
                selector_json: "{}".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_kind.code(), tonic::Code::InvalidArgument);

    let invalid_json = index_client
        .create_index(authorized(
            CreateIndexRequest {
                bucket_name,
                name: "invalid-json".to_string(),
                kind: "full_text".to_string(),
                selector_json: "{".to_string(),
                extractor_json: "{}".to_string(),
                authorization_mode: "inherit_object".to_string(),
                build_policy_json: "{}".to_string(),
            },
            &token,
        ))
        .await
        .unwrap_err();
    assert_eq!(invalid_json.code(), tonic::Code::InvalidArgument);
}
