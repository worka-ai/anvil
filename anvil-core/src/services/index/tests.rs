use super::*;

#[test]
fn query_filters_match_path_prefix_and_metadata() {
    let req = QueryIndexRequest {
        path_prefix: "docs/active/".to_string(),
        metadata_filters_json: serde_json::json!({
            "tenant": "alpha",
            "/nested/state": "open"
        })
        .to_string(),
        ..Default::default()
    };
    let filters = QueryFilters::from_request(&req).unwrap();
    let object_ref = QueryObjectRef {
        object_version_id: "version-1".to_string(),
        object_key: "docs/active/item.json".to_string(),
        user_meta: Some(serde_json::json!({
            "tenant": "alpha",
            "nested": {"state": "open"}
        })),
        ..Default::default()
    };

    assert!(filters.matches(&object_ref).unwrap());
}

#[test]
fn query_filters_reject_non_matching_metadata_without_leaking_object() {
    let req = QueryIndexRequest {
        metadata_filters_json: serde_json::json!({"tenant": "alpha"}).to_string(),
        ..Default::default()
    };
    let filters = QueryFilters::from_request(&req).unwrap();
    let object_ref = QueryObjectRef {
        object_version_id: "version-1".to_string(),
        object_key: "docs/active/item.json".to_string(),
        user_meta: Some(serde_json::json!({"tenant": "beta"})),
        ..Default::default()
    };

    assert!(!filters.matches(&object_ref).unwrap());
}

#[test]
fn query_filters_reject_invalid_metadata_filter_shape() {
    let req = QueryIndexRequest {
        metadata_filters_json: "[]".to_string(),
        ..Default::default()
    };

    assert!(QueryFilters::from_request(&req).is_err());
}

#[test]
fn hybrid_scoring_normalizes_sources_and_applies_freshness() {
    let mut candidates = vec![
        HybridCandidate {
            item: HybridAccum {
                text_score: 2.0,
                vector_score: 2.0,
                ..HybridAccum::new([1; 16])
            },
            object_ref: QueryObjectRef {
                created_at_nanos: 100,
                ..Default::default()
            },
        },
        HybridCandidate {
            item: HybridAccum {
                text_score: 2.0,
                vector_score: 2.0,
                ..HybridAccum::new([2; 16])
            },
            object_ref: QueryObjectRef {
                created_at_nanos: 200,
                ..Default::default()
            },
        },
    ];

    score_hybrid_candidates(&mut candidates, true, true, 0.55, 0.35, 0.10);

    assert_eq!(candidates[0].item.normalized_text_score, 1.0);
    assert_eq!(candidates[0].item.normalized_vector_score, 1.0);
    assert_eq!(candidates[0].item.freshness_score, 0.0);
    assert_eq!(candidates[1].item.freshness_score, 1.0);
    assert!(candidates[1].item.score > candidates[0].item.score);
}

#[test]
fn hybrid_scoring_disables_freshness_for_single_source_queries() {
    let mut candidates = vec![HybridCandidate {
        item: HybridAccum {
            text_score: 7.0,
            ..HybridAccum::new([1; 16])
        },
        object_ref: QueryObjectRef {
            created_at_nanos: 200,
            ..Default::default()
        },
    }];

    score_hybrid_candidates(&mut candidates, true, false, 1.0, 0.0, 0.0);

    assert_eq!(candidates[0].item.score, 1.0);
    assert_eq!(candidates[0].item.freshness_score, 0.0);
}

#[test]
fn index_page_token_binds_principal_mesh_authz_and_index_inputs() {
    let config = Config {
        mesh_id: "mesh-test".to_string(),
        ..Config::default()
    };
    let claims = auth::Claims {
        sub: "principal-a".to_string(),
        exp: 0,
        scopes: vec!["*|*".to_string()],
        tenant_id: 42,
        jti: Some("token-a".to_string()),
    };
    let binding = IndexPageTokenBinding::single_index(
        &config,
        &claims,
        "typed_json",
        "bucket-a",
        "idx-a",
        7,
        3,
        11,
        "predicate-a".to_string(),
        "order-a".to_string(),
    );
    let signing_key = b"page-token-test-key";
    let encoded = IndexPageToken::for_cursor(
        &binding,
        "source-a".to_string(),
        BTreeMap::from([("field".to_string(), JsonValue::String("v".to_string()))]),
    )
    .encode(signing_key)
    .unwrap();
    let decoded = IndexPageToken::decode(&encoded, signing_key)
        .unwrap()
        .expect("decoded token");
    decoded.validate(&binding).unwrap();
    assert!(
        chrono::DateTime::parse_from_rfc3339(&decoded.expires_at)
            .unwrap()
            .with_timezone(&chrono::Utc)
            > chrono::Utc::now(),
        "RFC 0006 page tokens must carry an authenticated expiry"
    );

    let mut other_principal = claims.clone();
    other_principal.sub = "principal-b".to_string();
    let other_principal_binding = IndexPageTokenBinding::single_index(
        &config,
        &other_principal,
        "typed_json",
        "bucket-a",
        "idx-a",
        7,
        3,
        11,
        "predicate-a".to_string(),
        "order-a".to_string(),
    );
    assert!(decoded.validate(&other_principal_binding).is_err());

    let mut other_mesh = config.clone();
    other_mesh.mesh_id = "mesh-other".to_string();
    let other_mesh_binding = IndexPageTokenBinding::single_index(
        &other_mesh,
        &claims,
        "typed_json",
        "bucket-a",
        "idx-a",
        7,
        3,
        11,
        "predicate-a".to_string(),
        "order-a".to_string(),
    );
    assert!(decoded.validate(&other_mesh_binding).is_err());

    let other_generation_binding = IndexPageTokenBinding::single_index(
        &config,
        &claims,
        "typed_json",
        "bucket-a",
        "idx-a",
        8,
        3,
        11,
        "predicate-a".to_string(),
        "order-a".to_string(),
    );
    assert!(decoded.validate(&other_generation_binding).is_err());

    let expired = IndexPageToken {
        expires_at: (chrono::Utc::now() - chrono::Duration::seconds(1))
            .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        ..IndexPageToken::for_cursor(
            &binding,
            "source-a".to_string(),
            BTreeMap::from([("field".to_string(), JsonValue::String("v".to_string()))]),
        )
    }
    .encode(signing_key)
    .unwrap();
    let decoded_expired = IndexPageToken::decode(&expired, signing_key)
        .unwrap()
        .expect("decoded expired token");
    let expired_err = decoded_expired.validate(&binding).unwrap_err();
    assert_eq!(expired_err.message(), "PageTokenExpired");
}

#[test]
fn vector_definition_rejects_external_extractor() {
    let err = validate_index_definition_shape(
        "vector",
        &test_vector_definition("configured_provider", 4),
        &serde_json::json!({"kind": "object_body_utf8"}),
        &test_config(true),
    )
    .unwrap_err();

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("build_policy_json"));
}

#[test]
fn vector_text_extractor_allows_test_only_only_when_enabled() {
    let policy = test_vector_definition("test_only", 4);
    let extractor = serde_json::json!({});

    assert!(
        validate_index_definition_shape("vector", &policy, &extractor, &test_config(true)).is_ok()
    );
    let err = validate_index_definition_shape("vector", &policy, &extractor, &test_config(false))
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("disabled"));
}

fn test_config(allow_test_only: bool) -> Config {
    Config {
        allow_test_only_embedding_provider: allow_test_only,
        ..Config::default()
    }
}

fn test_vector_definition(provider: &str, dimension: u16) -> serde_json::Value {
    serde_json::json!({
        "schema": crate::formats::vector::VECTOR_INDEX_SCHEMA,
        "source": {"kind": "object_current", "prefix": "docs/"},
        "extractor": {"kind": "object_body_utf8"},
        "embedding": {
            "provider": provider,
            "model": "test-text-embedding",
            "dimension": dimension,
            "modality": "text",
            "normalisation": "unit_l2",
            "chunking": {"strategy": "whole_object"}
        },
        "ann": {
            "algorithm": "hnsw",
            "metric": "cosine"
        }
    })
}
