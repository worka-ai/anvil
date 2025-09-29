use crate::s3_auth::sigv4_auth;
use axum::{
    extract::{Path, State},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, put},
    Router,
};

pub fn app(state: crate::AppState) -> Router {
    Router::new()
        .route("/{bucket}", put(create_bucket).get(list_objects))
        .route("/{bucket}/{*path}", get(get_object).put(put_object))
        .with_state(state)
        .route_layer(middleware::from_fn(sigv4_auth))
}

async fn create_bucket(
    State(_state): State<crate::AppState>,
    Path(_bucket): Path<String>,
) -> Response {
    (axum::http::StatusCode::OK, "").into_response()
}

async fn list_objects(
    State(_state): State<crate::AppState>,
    Path(_bucket): Path<String>,
) -> Response {
    (axum::http::StatusCode::OK, "").into_response()
}

async fn get_object(
    State(_state): State<crate::AppState>,
    Path((_bucket, _key)): Path<(String, String)>,
) -> Response {
    (axum::http::StatusCode::OK, "").into_response()
}

async fn put_object(
    State(_state): State<crate::AppState>,
    Path((_bucket, _key)): Path<(String, String)>,
) -> Response {
    (axum::http::StatusCode::OK, "").into_response()
}

