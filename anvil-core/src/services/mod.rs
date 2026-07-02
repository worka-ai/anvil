pub mod admin;
pub(crate) mod admin_cursor;
pub mod auth;
pub mod bucket;
pub mod coordination;
pub mod git_source;
pub mod huggingface;
pub mod index;
pub mod internal;
pub mod object;
pub mod personaldb;
pub mod repair;
pub(crate) mod watch_envelope;

use crate::anvil_api::{
    admin_service_server::AdminServiceServer, auth_service_server::AuthServiceServer,
    bucket_service_server::BucketServiceServer,
    coordination_service_server::CoordinationServiceServer,
    git_source_service_server::GitSourceServiceServer,
    hf_ingestion_service_server::HfIngestionServiceServer,
    hugging_face_key_service_server::HuggingFaceKeyServiceServer,
    index_service_server::IndexServiceServer,
    internal_anvil_service_server::InternalAnvilServiceServer,
    object_service_server::ObjectServiceServer,
    personal_db_service_server::PersonalDbServiceServer,
    repair_service_server::RepairServiceServer,
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
    .add_service(CoordinationServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(IndexServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(GitSourceServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(PersonalDbServiceServer::with_interceptor(
        state.clone(),
        auth_closure.clone(),
    ))
    .add_service(RepairServiceServer::with_interceptor(
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

pub fn create_admin_grpc_router(state: AppState, auth_interceptor: AuthInterceptorFn) -> Routes {
    let auth_closure = {
        let f = auth_interceptor.clone();
        move |req| f.call(req)
    };
    tonic::service::Routes::new(AdminServiceServer::with_interceptor(state, auth_closure))
}

pub fn create_axum_router(grpc_router: Routes) -> axum::Router {
    grpc_router
        .into_axum_router()
        .route_layer(axum::middleware::from_fn(middleware::request_id_mw))
        .route_layer(axum::middleware::from_fn(middleware::save_uri_mw))
}
