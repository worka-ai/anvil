use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use anvil_core::anvil_api::internal_proxy_service_client::InternalProxyServiceClient;
use anvil_core::anvil_api::{
    ProxyHeader, ProxyRequestChunk, ProxyRequestHeader, ProxyResponseHeader, proxy_request_chunk,
    proxy_response_chunk,
};
use anvil_core::bucket_journal;
use anvil_core::mesh_directory::{BucketLocatorStatus, TenantNameStatus};
use anvil_core::mesh_lifecycle::{LifecycleState, NodeCapability};
use anvil_core::object_links;
use anvil_core::object_manager::{
    ObjectLinkReadMode, ObjectReadConsistency, ObjectWriteOptions, ObjectWriteVisibility,
};
use anvil_core::observability::RESERVED_NAMESPACE_REJECTION_COUNT;
use anvil_core::permissions::AnvilAction;
use anvil_core::persistence::Object;
use anvil_core::routing::{
    self as core_routing, CrossRegionRoutingPolicy, HostAliasDescriptor, ObjectRoute, RouteRequest,
    RouteSource, RoutingConfig, RoutingError,
};
use anvil_core::validation;
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{ConnectInfo, Path, Query, Request, State},
    http::{self, HeaderMap, Uri},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_core::Stream;
use futures_util::stream::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;

mod bucket;
mod guard;
mod multipart;
mod object;
mod preconditions;
mod proxy;
mod routing;
mod util;

#[allow(unused_imports)]
use bucket::*;
#[allow(unused_imports)]
use guard::*;
#[allow(unused_imports)]
use multipart::*;
#[allow(unused_imports)]
use object::*;
#[allow(unused_imports)]
use preconditions::*;
#[allow(unused_imports)]
use proxy::*;
#[allow(unused_imports)]
use routing::*;
#[allow(unused_imports)]
use util::*;

pub fn app(state: AppState) -> Router {
    let public = Router::new()
        .route("/ready", get(readiness_check))
        .with_state(state.clone());

    let s3_routes = Router::new()
        .route("/", get(list_buckets)) // ListBuckets
        .route(
            "/{bucket}",
            put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket)
                .post(post_bucket)
                .get(list_objects),
        )
        .route(
            "/{bucket}/",
            get(list_objects)
                .put(create_bucket)
                .delete(delete_bucket)
                .post(post_bucket)
                .head(head_bucket),
        )
        .route(
            "/{bucket}/{*path}",
            get(get_object)
                .put(put_object)
                .post(post_object)
                .delete(delete_object)
                .head(head_object),
        )
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            reserved_namespace_guard,
        ))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            s3_host_routing,
        ))
        .layer(middleware::from_fn(aws_chunked_decoder))
        .layer(middleware::from_fn_with_state(state.clone(), sigv4_auth))
        .layer(middleware::from_fn_with_state(
            state,
            reserved_namespace_guard,
        ));

    public.merge(s3_routes)
}

#[cfg(test)]
mod tests;
