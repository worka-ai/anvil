pub mod auth;
pub mod bucket;
pub mod huggingface;
pub mod internal;
pub mod object;

use crate::anvil_api::{
    auth_service_server::AuthServiceServer, bucket_service_server::BucketServiceServer,
    hf_ingestion_service_server::HfIngestionServiceServer,
    hugging_face_key_service_server::HuggingFaceKeyServiceServer,
    internal_anvil_service_server::InternalAnvilServiceServer,
    object_service_server::ObjectServiceServer,
};
use crate::{AppState, middleware};
use tonic::service::Routes;
use tonic::{Request, Status};

#[derive(Clone)]
pub struct AuthInterceptorFn {
    f: std::sync::Arc<dyn Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync>,
}

impl AuthInterceptorFn {
    pub fn new<F>(f: F) -> Self
    where
        F: Fn(Request<()>) -> Result<Request<()>, Status> + Send + Sync + 'static,
    {
        Self {
            f: std::sync::Arc::new(f),
        }
    }

    pub fn call(&self, req: Request<()>) -> Result<Request<()>, Status> {
        (self.f)(req)
    }
}

pub fn create_grpc_router(state: AppState, auth_interceptor: AuthInterceptorFn) -> Routes {
    // Adapt our handle to a closure Interceptor Tonic accepts
    let auth_closure = {
        let f = auth_interceptor.clone();
        move |req| f.call(req)
    };
    tonic::service::Routes::new(AuthServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(ObjectServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(BucketServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(InternalAnvilServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(HuggingFaceKeyServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(HfIngestionServiceServer::with_interceptor(
        state.clone(),
        auth_closure,
    ))
}

pub fn create_axum_router(grpc_router: Routes) -> axum::Router {
    grpc_router
        .into_axum_router()
        .route_layer(axum::middleware::from_fn(middleware::save_uri_mw))
}
