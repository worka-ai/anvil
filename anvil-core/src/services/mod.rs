pub mod auth;
pub mod bucket;
pub mod internal;
pub mod object;
pub mod huggingface;

use crate::anvil_api::{
    auth_service_server::AuthServiceServer,
    bucket_service_server::BucketServiceServer,
    internal_anvil_service_server::InternalAnvilServiceServer,
    hugging_face_key_service_server::HuggingFaceKeyServiceServer,
    hf_ingestion_service_server::HfIngestionServiceServer,
    object_service_server::ObjectServiceServer,
};
use crate::{AppState, middleware};
use tonic::service::Routes;

pub fn create_grpc_router(state: AppState) -> (Routes, impl Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Clone) {
    let state_clone = state.clone();
    let auth_interceptor = move |req| middleware::auth_interceptor(req, &state_clone);

    let grpc_router = tonic::service::Routes::new(AuthServiceServer::with_interceptor(
        state.clone(),
        auth_interceptor.clone(),
    ))
    .add_service(ObjectServiceServer::with_interceptor(
        state.clone(),
        auth_interceptor.clone(),
    ))
    .add_service(BucketServiceServer::with_interceptor(
        state.clone(),
        auth_interceptor.clone(),
    ))
    .add_service(InternalAnvilServiceServer::with_interceptor(
        state.clone(),
        auth_interceptor.clone(),
    ))
    .add_service(HuggingFaceKeyServiceServer::with_interceptor(
        state.clone(),
        auth_interceptor.clone(),
    ))
    .add_service(HfIngestionServiceServer::with_interceptor(
        state.clone(),
        auth_interceptor,
    ));

    let auth_interceptor_clone = move |req| middleware::auth_interceptor(req, &state.clone());

    (grpc_router, auth_interceptor_clone)
}

pub fn create_axum_router(grpc_router: Routes) -> axum::Router {
    grpc_router
        .into_axum_router()
        .route_layer(axum::middleware::from_fn(middleware::save_uri_mw))
        .route_layer(axum::middleware::from_fn(
            |req: axum::extract::Request, next: axum::middleware::Next| async move {
                if req.method() == axum::http::Method::POST {
                    next.run(req).await
                } else {
                    axum::response::Response::builder()
                        .status(axum::http::StatusCode::METHOD_NOT_ALLOWED)
                        .body(axum::body::Body::empty())
                        .unwrap()
                }
            },
        ))
}