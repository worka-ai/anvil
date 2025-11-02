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

// Public trait so other crates can accept and reuse the same interceptor.
pub trait AuthInterceptor: Send + Sync + 'static {
    fn call(&self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>;
}

impl<F> AuthInterceptor for F
where
    F: Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Send + Sync + 'static,
{
    fn call(&self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        (self)(req)
    }
}

#[derive(Clone)]
pub struct AuthInterceptorFn(std::sync::Arc<dyn AuthInterceptor>);

impl AuthInterceptorFn {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> + Send + Sync + 'static,
    {
        Self(std::sync::Arc::new(f))
    }

    pub fn call(&self, req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        self.0.call(req)
    }
}

pub fn create_grpc_router(
    state: AppState,
) -> (Routes, AuthInterceptorFn) {
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

    (grpc_router, AuthInterceptorFn::new(auth_interceptor_clone))
}

pub fn create_axum_router(grpc_router: Routes) -> axum::Router {
    grpc_router
        .into_axum_router()
        .route_layer(axum::middleware::from_fn(middleware::save_uri_mw))
}
