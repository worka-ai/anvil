use crate::auth::Claims;
use crate::s3_auth::sigv4_auth;
use crate::AppState;
use axum::{
    body::Body,
    extract::{Path, Request, State},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, put},
    Router,
};
use futures_util::stream::StreamExt;

pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/", get(health_check))
        .route("/{bucket}", put(create_bucket).get(list_objects))
        .route("/{bucket}/{*path}", get(get_object).put(put_object))
        .with_state(state.clone())
        .route_layer(middleware::from_fn_with_state(state.clone(), sigv4_auth))
}

async fn health_check() -> impl IntoResponse {
    (axum::http::StatusCode::OK, "OK")
}

async fn create_bucket(_state: State<AppState>, _bucket: Path<String>) -> Response {
    // For S3 `CreateBucket`, the region is in the XML body, which we are not parsing here.
    // We will use the node's default region.
    // Also, we need to extract the tenant from the request extensions, set by SigV4.
    // This is a simplified version.
    (axum::http::StatusCode::OK, "").into_response()
}

async fn list_objects(_state: State<AppState>, _bucket: Path<String>) -> Response {
    // Implementation would involve parsing query params like prefix, delimiter, etc.
    // and calling state.object_manager.list_objects
    (axum::http::StatusCode::OK, "").into_response()
}

async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    match state
        .object_manager
        .get_object(claims, bucket, key)
        .await
    {
        Ok((object, stream)) => {
            let body = Body::from_stream(stream.map(|r| r.map_err(|e| axum::Error::new(e))));
            Response::builder()
                .status(200)
                .header(
                    "Content-Type",
                    object.content_type.unwrap_or_default(),
                )
                .header("Content-Length", object.size)
                .header("ETag", object.etag)
                .body(body)
                .unwrap()
        }
        Err(status) => {
            let (code, message) = match status.code() {
                tonic::Code::NotFound => (axum::http::StatusCode::NOT_FOUND, status.message()),
                tonic::Code::PermissionDenied => (axum::http::StatusCode::FORBIDDEN, status.message()),
                _ => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, status.message()),
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
    let claims = req
        .extensions()
        .get::<Claims>()
        .cloned()
        .expect("SigV4 middleware should have inserted claims");

    let body_stream = req.into_body().into_data_stream().map(|r| {
        r.map(|chunk| chunk.to_vec())
            .map_err(|e| tonic::Status::internal(e.to_string()))
    });

    match state
        .object_manager
        .put_object(
            claims.tenant_id,
            &bucket,
            &key,
            &claims.scopes,
            body_stream,
        )
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
                tonic::Code::PermissionDenied => (axum::http::StatusCode::FORBIDDEN, status.message()),
                _ => (axum::http::StatusCode::INTERNAL_SERVER_ERROR, status.message()),
            };
            (code, message.to_string()).into_response()
        }
    }
}
