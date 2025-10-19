use crate::auth::Claims;
use crate::s3_auth::sigv4_auth;
use crate::AppState;
use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, put},
    Router,
};
use futures_util::stream::StreamExt;
use std::collections::HashMap;

pub fn app(state: AppState) -> Router {
    let public = Router::new()
        .route("/", get(health_check))
        .route("/ready", get(readiness_check))
        .with_state(state.clone());

    let s3_routes = Router::new()
        .route("/{bucket}", put(create_bucket).get(list_objects))
        .route("/{bucket}/{*path}", get(get_object).put(put_object).head(head_object))
        .with_state(state.clone())
        .route_layer(middleware::from_fn_with_state(state.clone(), sigv4_auth));

    public.merge(s3_routes)

}
async fn health_check() -> impl IntoResponse {
    (axum::http::StatusCode::OK, "OK")
}

async fn create_bucket(
    State(state): State<AppState>,
    bucket: Path<String>,
    req: Request,
) -> Response {
    // For S3 `CreateBucket`, the region is in the XML body, which we are not parsing here.
    // We will use the node's default region.
    // Also, we need to extract the tenant from the request extensions, set by SigV4.
    // This is a simplified version.
    // Claims may be absent for anonymous; handler will enforce bucket public access
    let claims = req.extensions().get::<Claims>().cloned();
    let claims = match claims {
        Some(c) => c,
        None => {
            return (axum::http::StatusCode::UNAUTHORIZED, "Missing credentials").into_response();
        }
    };
    match state
        .bucket_manager
        .create_bucket(claims.tenant_id, &bucket, &state.region, &claims.scopes)
        .await
    {
        Ok(_) => (axum::http::StatusCode::OK, "").into_response(),
        Err(status) => {
            let (code, message) = match status.code() {
                tonic::Code::AlreadyExists => (axum::http::StatusCode::CONFLICT, status.message()),
                tonic::Code::PermissionDenied => {
                    (axum::http::StatusCode::FORBIDDEN, status.message())
                }
                tonic::Code::InvalidArgument => {
                    (axum::http::StatusCode::BAD_REQUEST, status.message())
                }
                _ => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    status.message(),
                ),
            };
            (code, message.to_string()).into_response()
        }
    }
}

async fn list_objects(
    State(state): State<AppState>,
    bucket: Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let start_after = q
        .get("start-after")
        .or_else(|| q.get("startAfter"))
        .cloned()
        .unwrap_or_default();
    let delimiter = q.get("delimiter").cloned().unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_objects(claims, &bucket, &prefix, &start_after, max_keys, &delimiter)
        .await
    {
        Ok((objects, common_prefixes)) => {
            // Basic ListObjectsV2 XML
            let is_truncated = false; // TODO: support continuation tokens
            let key_count = objects.len() as i32;
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", &*bucket));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str(&format!("  <KeyCount>{}</KeyCount>\n", key_count));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if is_truncated { "true" } else { "false" }
            ));
            for o in objects {
                xml.push_str("  <Contents>\n");
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&o.key)));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    o.created_at.to_string()
                ));
                xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", o.etag));
                xml.push_str(&format!("    <Size>{}</Size>\n", o.size));
                xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                xml.push_str("  </Contents>\n");
            }
            for p in common_prefixes {
                xml.push_str("  <CommonPrefixes>\n");
                xml.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(&p)));
                xml.push_str("  </CommonPrefixes>\n");
            }
            xml.push_str("</ListBucketResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => {
            let code = match status.code() {
                tonic::Code::NotFound => {
                    // For anonymous S3-style list on non-public buckets, return 403
                    if req.extensions().get::<Claims>().is_none() {
                        axum::http::StatusCode::FORBIDDEN
                    } else {
                        axum::http::StatusCode::NOT_FOUND
                    }
                }
                tonic::Code::PermissionDenied => axum::http::StatusCode::FORBIDDEN,
                _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            };
            (code, status.message().to_string()).into_response()
        }
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn readiness_check(State(state): State<AppState>) -> Response {
    // DB readiness: attempt a lightweight operation. If Persistence exposes no ping, rely on pool creation success earlier.
    // Cluster readiness: at least 1 peer known (self included)
    let peers = state.cluster.read().await.len();
    if peers >= 1 {
        (axum::http::StatusCode::OK, "READY").into_response()
    } else {
        let body = serde_json::json!({"status":"not_ready","peers":peers});
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::response::Json(body),
        )
            .into_response()
    }
}

async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    match state.object_manager.get_object(claims, bucket, key).await {
        Ok((object, stream)) => {
            let body = Body::from_stream(stream.map(|r| r.map_err(|e| axum::Error::new(e))));
            Response::builder()
                .status(200)
                .header("Content-Type", object.content_type.unwrap_or_default())
                .header("Content-Length", object.size)
                .header("ETag", object.etag)
                .body(body)
                .unwrap()
        }
        Err(status) => {
            let (code, message) = match status.code() {
                tonic::Code::NotFound => {
                    // For anonymous requests, avoid oracle: map to 403
                    if req.extensions().get::<Claims>().is_none() {
                        (axum::http::StatusCode::FORBIDDEN, status.message())
                    } else {
                        (axum::http::StatusCode::NOT_FOUND, status.message())
                    }
                }
                tonic::Code::PermissionDenied => {
                    (axum::http::StatusCode::FORBIDDEN, status.message())
                }
                _ => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    status.message(),
                ),
            };
            (code, message.to_string()).into_response()
        }
    }
}

async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return (axum::http::StatusCode::UNAUTHORIZED, "Missing credentials").into_response();
        }
    };

    let body_stream = req.into_body().into_data_stream().map(|r| {
        r.map(|chunk| chunk.to_vec())
            .map_err(|e| tonic::Status::internal(e.to_string()))
    });

    match state
        .object_manager
        .put_object(claims.tenant_id, &bucket, &key, &claims.scopes, body_stream)
        .await
    {
        Ok(object) => Response::builder()
            .status(200)
            .header("ETag", object.etag)
            .body(Body::empty())
            .unwrap(),
        Err(status) => {
            let (code, message) = match status.code() {
                tonic::Code::NotFound => (axum::http::StatusCode::NOT_FOUND, status.message()),
                tonic::Code::PermissionDenied => {
                    (axum::http::StatusCode::FORBIDDEN, status.message())
                }
                _ => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    status.message(),
                ),
            };
            (code, message.to_string()).into_response()
        }
    }
}

async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    match state.object_manager.head_object(claims, &bucket, &key).await {
        Ok(object) => Response::builder()
            .status(200)
            .header("Content-Type", object.content_type.unwrap_or_default())
            .header("Content-Length", object.size)
            .header("ETag", object.etag)
            .body(Body::empty())
            .unwrap(),
        Err(status) => {
            let (code, message) = match status.code() {
                tonic::Code::NotFound => {
                    if req.extensions().get::<Claims>().is_none() {
                        (axum::http::StatusCode::FORBIDDEN, status.message())
                    } else {
                        (axum::http::StatusCode::NOT_FOUND, status.message())
                    }
                }
                tonic::Code::PermissionDenied => (
                    axum::http::StatusCode::FORBIDDEN,
                    status.message(),
                ),
                _ => (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    status.message(),
                ),
            };
            (code, message.to_string()).into_response()
        }
    }
}
