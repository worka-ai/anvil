use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use anvil_core::anvil_api::internal_proxy_service_client::InternalProxyServiceClient;
use anvil_core::anvil_api::{
    ProxyHeader, ProxyRequestChunk, ProxyRequestHeader, ProxyResponseHeader, proxy_request_chunk,
    proxy_response_chunk,
};
use anvil_core::auth;
use anvil_core::bucket_journal;
use anvil_core::mesh_directory::{BucketLocatorStatus, TenantNameStatus};
use anvil_core::mesh_lifecycle::{LifecycleState, NodeCapability};
use anvil_core::object_links;
use anvil_core::object_manager::{ObjectLinkReadMode, ObjectWriteOptions};
use anvil_core::observability::RESERVED_NAMESPACE_REJECTION_COUNT;
use anvil_core::permissions::AnvilAction;
use anvil_core::persistence::Object;
use anvil_core::routing::{
    self, CrossRegionRoutingPolicy, HostAliasDescriptor, ObjectRoute, RouteRequest, RouteSource,
    RoutingConfig, RoutingError,
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

#[derive(Deserialize)]
struct CreateBucketConfiguration {
    #[serde(rename = "LocationConstraint")]
    location_constraint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BucketVersioningConfigurationXml {
    #[serde(rename = "Status")]
    status: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CompleteMultipartUploadXml {
    #[serde(rename = "Part", default)]
    parts: Vec<CompleteMultipartUploadXmlPart>,
}

#[derive(Debug, Deserialize)]
struct CompleteMultipartUploadXmlPart {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "ETag")]
    etag: String,
}

#[derive(Debug, Deserialize)]
struct DeleteObjectsXml {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteObjectsXmlObject>,
    #[serde(rename = "Quiet")]
    quiet: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DeleteObjectsXmlObject {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
}

fn s3_error(code: &str, message: &str, status: axum::http::StatusCode) -> Response {
    let request_id = new_s3_request_id();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n  <RequestId>{}</RequestId>\n</Error>\n",
        code,
        xml_escape(message),
        request_id
    );
    Response::builder()
        .status(status)
        .header("Content-Type", "application/xml")
        .header("x-amz-request-id", request_id)
        .body(Body::from(body))
        .unwrap()
}

fn new_s3_request_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

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

#[derive(Debug, Clone)]
struct S3HostRoute(ObjectRoute);

async fn s3_host_routing(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let Some(config) = s3_routing_config(&state) else {
        return next.run(req).await;
    };
    let host = match request_host(&req, state.config.as_ref()) {
        Ok(Some(host)) => host,
        Ok(None) => return next.run(req).await,
        Err(err) => return s3_routing_error(err),
    };

    let request = RouteRequest {
        host: &host,
        path: req.uri().path(),
    };
    match routing::parse_object_route(request.clone(), &config, &[]) {
        Ok(route) => {
            if let Err(err) = rewrite_s3_host_route_uri(&mut req, &route) {
                return s3_routing_error(err);
            }
            req.extensions_mut().insert(S3HostRoute(route));
            next.run(req).await
        }
        Err(RoutingError::UnknownHost) => {
            let alias = match active_s3_host_alias(&state, &host).await {
                Ok(Some(alias)) => alias,
                Ok(None) => return next.run(req).await,
                Err(response) => return response,
            };
            match routing::parse_object_route(request, &config, &[alias]) {
                Ok(route) => {
                    if let Err(err) = rewrite_s3_host_route_uri(&mut req, &route) {
                        return s3_routing_error(err);
                    }
                    req.extensions_mut().insert(S3HostRoute(route));
                    next.run(req).await
                }
                Err(RoutingError::UnknownHost) => next.run(req).await,
                Err(err) => s3_routing_error(err),
            }
        }
        Err(err) => s3_routing_error(err),
    }
}

async fn active_s3_host_alias(
    state: &AppState,
    host: &str,
) -> Result<Option<HostAliasDescriptor>, Response> {
    let host = match routing::normalize_alias_hostname(host) {
        Ok(host) => host,
        Err(_) => return Ok(None),
    };
    match state.persistence.get_host_alias_descriptor(&host).await {
        Ok(Some(alias)) if alias.state == routing::HostAliasState::Active => Ok(Some(alias)),
        Ok(_) => Ok(None),
        Err(error) => Err(s3_error(
            "InternalError",
            &format!("Failed to load host alias: {error}"),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

fn s3_routing_config(state: &AppState) -> Option<RoutingConfig> {
    let configured = state.config.public_region_base_domain.trim();
    if configured.is_empty() {
        return None;
    }

    let region_prefix = format!("{}.", state.region);
    let base_domain = configured
        .strip_prefix(&region_prefix)
        .unwrap_or(configured);
    RoutingConfig::new(base_domain).ok()
}

fn request_host(
    req: &Request,
    config: &anvil_core::config::Config,
) -> Result<Option<String>, RoutingError> {
    let Some(raw_authority) = raw_request_authority(req) else {
        return Ok(None);
    };
    let Some(remote_peer) = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|connect_info| connect_info.0.ip())
    else {
        return routing::normalize_alias_hostname(raw_authority).map(Some);
    };
    let trusted_proxies = trusted_proxy_source_ranges(config);
    let forwarded_headers = forwarded_headers(req.headers());
    routing::effective_host(
        raw_authority,
        remote_peer,
        &trusted_proxies,
        &forwarded_headers,
    )
    .map(Some)
}

fn raw_request_authority(req: &Request) -> Option<&str> {
    req.headers()
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| req.uri().authority().map(|authority| authority.as_str()))
}

fn trusted_proxy_source_ranges(config: &anvil_core::config::Config) -> Vec<routing::TrustedProxy> {
    routing::parse_trusted_proxies(&config.trusted_proxy_source_ranges).unwrap_or_default()
}

fn forwarded_headers(headers: &HeaderMap) -> routing::ForwardedHeaders {
    routing::ForwardedHeaders {
        forwarded: header_values(headers, "forwarded"),
        x_forwarded_host: header_values(headers, "x-forwarded-host"),
    }
}

fn header_values(headers: &HeaderMap, name: &'static str) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(str::to_string)
        .collect()
}

fn rewrite_s3_host_route_uri(req: &mut Request, route: &ObjectRoute) -> Result<(), RoutingError> {
    let mut parts = req.uri().clone().into_parts();
    let path = s3_route_rewrite_path(route);
    let path_and_query = match req.uri().query() {
        Some(query) => format!("{path}?{query}"),
        None => path,
    };
    parts.path_and_query = Some(
        path_and_query
            .parse()
            .map_err(|_| RoutingError::InvalidPath)?,
    );
    let uri = Uri::from_parts(parts).map_err(|_| RoutingError::InvalidPath)?;
    *req.uri_mut() = uri;
    Ok(())
}

fn s3_route_rewrite_path(route: &ObjectRoute) -> String {
    let mut path = String::new();
    path.push('/');
    push_percent_encoded_path(&mut path, &route.bucket, true);
    path.push('/');
    push_percent_encoded_path(&mut path, &route.key, false);
    path
}

fn push_percent_encoded_path(out: &mut String, value: &str, encode_slash: bool) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(byte as char)
            }
            b'/' if !encode_slash => out.push(byte as char),
            _ => {
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0f) as usize] as char);
            }
        }
    }
}

fn s3_routing_error(err: RoutingError) -> Response {
    s3_error(
        "InvalidRequest",
        &err.to_string(),
        axum::http::StatusCode::BAD_REQUEST,
    )
}

fn s3_host_route(req: &Request) -> Option<ObjectRoute> {
    req.extensions()
        .get::<S3HostRoute>()
        .map(|route| route.0.clone())
}

fn s3_routed_bucket(req: &Request, fallback_bucket: String) -> String {
    s3_host_route(req)
        .map(|route| route.bucket)
        .unwrap_or(fallback_bucket)
}

fn s3_routed_bucket_key(
    req: &Request,
    fallback_bucket: String,
    fallback_key: String,
) -> (String, String) {
    s3_host_route(req)
        .map(|route| (route.bucket, route.key))
        .unwrap_or((fallback_bucket, fallback_key))
}

fn s3_routed_object(req: &Request) -> Option<(String, String)> {
    s3_host_route(req)
        .filter(|route| !route.key.is_empty())
        .map(|route| (route.bucket, route.key))
}

fn s3_routed_bucket_without_key(req: &Request) -> Option<String> {
    s3_host_route(req)
        .filter(|route| route.key.is_empty())
        .map(|route| route.bucket)
}

#[derive(Debug, Clone)]
struct CheckedS3Route {
    claims: Option<Claims>,
    tenant_id: Option<i64>,
    remote_bucket: Option<RemoteBucketProxyTarget>,
}

#[derive(Debug, Clone)]
struct RemoteBucketProxyTarget {
    region: String,
    bucket_locator_generation: u64,
    endpoint: String,
}

async fn s3_checked_route(
    state: &AppState,
    route: Option<ObjectRoute>,
    claims: Option<Claims>,
) -> Result<CheckedS3Route, Response> {
    let Some(route) = route else {
        return Ok(CheckedS3Route {
            claims,
            tenant_id: None,
            remote_bucket: None,
        });
    };

    let route_tenant_id = match &route.source {
        RouteSource::HostAlias { .. } => route.tenant.parse::<i64>().map_err(|_| {
            s3_error(
                "InvalidRequest",
                "Host alias target tenant id is invalid",
                axum::http::StatusCode::BAD_REQUEST,
            )
        })?,
        RouteSource::PathStyle | RouteSource::VirtualHost => {
            let descriptor = state
                .persistence
                .get_mesh_tenant_name_locator(&route.tenant)
                .await
                .map_err(|error| {
                    s3_error(
                        "InvalidRequest",
                        &format!("Failed to resolve tenant route: {error}"),
                        axum::http::StatusCode::BAD_REQUEST,
                    )
                })?
                .ok_or_else(|| {
                    s3_error(
                        "NoSuchTenant",
                        "The specified tenant does not exist",
                        axum::http::StatusCode::NOT_FOUND,
                    )
                })?;
            if descriptor.status != TenantNameStatus::Active {
                return Err(s3_error(
                    "NoSuchTenant",
                    "The specified tenant does not exist",
                    axum::http::StatusCode::NOT_FOUND,
                ));
            }
            descriptor.tenant_id.as_str().parse::<i64>().map_err(|_| {
                s3_error(
                    "InvalidRequest",
                    "Tenant route resolved to an invalid tenant id",
                    axum::http::StatusCode::BAD_REQUEST,
                )
            })?
        }
    };

    if let Some(claims) = claims.as_ref()
        && route_tenant_id != claims.tenant_id
    {
        return Err(s3_error(
            "AccessDenied",
            "Credentials are not valid for routed tenant",
            axum::http::StatusCode::FORBIDDEN,
        ));
    }

    if let Some(locator) = state
        .persistence
        .get_mesh_bucket_locator(route_tenant_id, &route.bucket)
        .await
        .map_err(|error| {
            s3_error(
                "InvalidRequest",
                &format!("Failed to resolve bucket route: {error}"),
                axum::http::StatusCode::BAD_REQUEST,
            )
        })?
        && locator.status != BucketLocatorStatus::Deleted
        && locator.home_region.as_str() != state.region.as_str()
    {
        let proxy_target = select_remote_bucket_proxy_target(state, locator.home_region.as_str())
            .await
            .map_err(|error| {
                s3_error(
                    "InternalError",
                    &format!("Failed to resolve remote proxy target: {error}"),
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                )
            })?;
        if route.key.is_empty() {
            return Err(s3_remote_bucket_response(
                state.config.cross_region_routing_policy,
                locator.home_region.as_str(),
                proxy_target.is_some(),
            ));
        }
        match routing::remote_bucket_routing_action(
            state.config.cross_region_routing_policy,
            proxy_target.is_some(),
        ) {
            routing::RemoteBucketRoutingAction::Proxy => {
                let endpoint = proxy_target.expect("proxy target checked above");
                return Ok(CheckedS3Route {
                    claims,
                    tenant_id: Some(route_tenant_id),
                    remote_bucket: Some(RemoteBucketProxyTarget {
                        region: locator.home_region.as_str().to_string(),
                        bucket_locator_generation: locator.generation,
                        endpoint,
                    }),
                });
            }
            _ => {
                return Err(s3_remote_bucket_response(
                    state.config.cross_region_routing_policy,
                    locator.home_region.as_str(),
                    proxy_target.is_some(),
                ));
            }
        }
    }

    Ok(CheckedS3Route {
        claims,
        tenant_id: Some(route_tenant_id),
        remote_bucket: None,
    })
}

fn s3_query_map(uri: &Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            query
                .split('&')
                .filter(|pair| !pair.is_empty())
                .map(|pair| {
                    let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
                    (
                        percent_decode_query_component(name),
                        percent_decode_query_component(value),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

async fn reserved_namespace_guard(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    if request_targets_reserved_namespace(&req) {
        state.observability.increment_counter(
            RESERVED_NAMESPACE_REJECTION_COUNT,
            &[("api", "s3"), ("operation", req.method().as_str())],
        );
        return s3_error(
            "UnauthorizedReservedNamespace",
            "UnauthorizedReservedNamespace",
            axum::http::StatusCode::FORBIDDEN,
        );
    }
    next.run(req).await
}

fn request_targets_reserved_namespace(req: &Request) -> bool {
    if let Some(route) = s3_host_route(req) {
        if !route.key.is_empty() && validation::is_reserved_internal_key(&route.key) {
            return true;
        }
    }

    let path = req.uri().path().trim_start_matches('/');
    let mut parts = path.splitn(2, '/');
    let _bucket = parts.next();

    if let Some(object_key) = parts.next() {
        if validation::is_reserved_internal_key(object_key) {
            return true;
        }
    }

    if request_copy_source_targets_reserved_namespace(req.headers()) {
        return true;
    }

    req.uri().query().is_some_and(|query| {
        query.split('&').any(|pair| {
            let mut fields = pair.splitn(2, '=');
            matches!(fields.next(), Some("prefix"))
                && fields
                    .next()
                    .map(percent_decode_query_component)
                    .is_some_and(|prefix| validation::is_reserved_internal_key(&prefix))
        })
    })
}

fn request_copy_source_targets_reserved_namespace(headers: &axum::http::HeaderMap) -> bool {
    let Some(copy_source) = headers
        .get("x-amz-copy-source")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    let copy_source = copy_source.trim_start_matches('/');
    let (path, _) = copy_source.split_once('?').unwrap_or((copy_source, ""));
    let Some((_, key)) = path.split_once('/') else {
        return false;
    };
    let key = percent_decode_path_component(key);
    !key.is_empty() && validation::is_reserved_internal_key(&key)
}

fn percent_decode_query_component(value: &str) -> String {
    let value = value.replace('+', " ");
    percent_decode(value.as_bytes())
}

fn percent_decode(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

async fn list_buckets(State(state): State<AppState>, req: Request) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        let q = s3_query_map(req.uri());
        return Box::pin(list_objects(State(state), Path(bucket), Query(q), req)).await;
    }

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .expect("authenticated delete bucket path supplied claims");

    match state
        .bucket_manager
        .list_buckets(claims.tenant_id, claims.scopes.as_slice())
        .await
    {
        Ok(buckets) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str("  <Owner>\n");
            xml.push_str(&format!("    <ID>{}</ID>\n", claims.tenant_id));
            // DisplayName is not stored, so we'll use tenant_id for now.
            xml.push_str(&format!(
                "    <DisplayName>{}</DisplayName>\n",
                claims.tenant_id
            ));
            xml.push_str("  </Owner>\n");
            xml.push_str("  <Buckets>\n");
            for b in buckets {
                xml.push_str("    <Bucket>\n");
                xml.push_str(&format!("      <Name>{}</Name>\n", xml_escape(&b.name)));
                xml.push_str(&format!(
                    "      <CreationDate>{}</CreationDate>\n",
                    b.created_at.to_rfc3339()
                ));
                xml.push_str("    </Bucket>\n");
            }
            xml.push_str("  </Buckets>\n");
            xml.push_str("</ListAllMyBucketsResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn create_bucket(
    State(state): State<AppState>,
    Path(mut bucket): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    // The S3 `CreateBucket` operation can contain an XML body with the location
    // constraint. We must consume the body for the handler to be matched correctly,
    // even if we don't use the content for now.
    if let Some((bucket, key)) = s3_routed_object(&req) {
        return Box::pin(put_object(State(state), Path((bucket, key)), Query(q), req)).await;
    }
    bucket = s3_routed_bucket(&req, bucket);

    // Claims may be absent for anonymous; handler will enforce bucket public access
    let claims = req.extensions().get::<Claims>().cloned();
    let claims = match claims {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .expect("authenticated create bucket path supplied claims");

    if q.contains_key("versioning") {
        return put_bucket_versioning_response(state, claims, &bucket, req).await;
    }

    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let region = if !bytes.is_empty() {
        if let Ok(config) = quick_xml::de::from_reader::<_, CreateBucketConfiguration>(&bytes[..]) {
            config.location_constraint.unwrap_or(state.region.clone())
        } else {
            state.region.clone()
        }
    } else {
        state.region.clone()
    };

    match state
        .bucket_manager
        .create_bucket(claims.tenant_id, &bucket, &region, &claims.scopes)
        .await
    {
        Ok(_) => (axum::http::StatusCode::OK, "").into_response(),
        Err(status) => match status.code() {
            tonic::Code::AlreadyExists => s3_error(
                "BucketAlreadyExists",
                status.message(),
                axum::http::StatusCode::CONFLICT,
            ),
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::InvalidArgument => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn get_bucket_versioning_response(state: AppState, claims: Claims, bucket: &str) -> Response {
    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        bucket,
        AnvilAction::BucketRead,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket).await {
        Ok(Some(_)) => {
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Status>Enabled</Status>\n</VersioningConfiguration>\n";
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn put_bucket_versioning_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
    req: Request,
) -> Response {
    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        bucket,
        AnvilAction::BucketWrite,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return s3_error(
                "NoSuchBucket",
                "The specified bucket does not exist",
                axum::http::StatusCode::NOT_FOUND,
            );
        }
        Err(e) => {
            return s3_error(
                "InternalError",
                &e.to_string(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    }

    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    if !bytes.is_empty() {
        match quick_xml::de::from_reader::<_, BucketVersioningConfigurationXml>(&bytes[..]) {
            Ok(config) => {
                if config
                    .status
                    .as_deref()
                    .is_some_and(|status| status != "Enabled")
                {
                    return s3_error(
                        "NotImplemented",
                        "Bucket versioning can only be enabled",
                        axum::http::StatusCode::NOT_IMPLEMENTED,
                    );
                }
            }
            Err(e) => {
                return s3_error(
                    "MalformedXML",
                    &format!("Invalid versioning configuration: {e}"),
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        }
    }

    (axum::http::StatusCode::OK, "").into_response()
}

async fn delete_bucket(
    State(state): State<AppState>,
    Path(mut bucket): Path<String>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        let q = s3_query_map(req.uri());
        return Box::pin(delete_object(
            State(state),
            Path((bucket, key)),
            Query(q),
            req,
        ))
        .await;
    }
    bucket = s3_routed_bucket(&req, bucket);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        &bucket,
        AnvilAction::BucketDelete,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match state
        .bucket_manager
        .delete_bucket(claims.tenant_id, &bucket, claims.scopes.as_slice())
        .await
    {
        Ok(_) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::NotFound => s3_error(
                "NoSuchBucket",
                status.message(),
                axum::http::StatusCode::NOT_FOUND,
            ),
            tonic::Code::InvalidArgument => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            tonic::Code::FailedPrecondition => s3_error(
                "BucketNotEmpty",
                status.message(),
                axum::http::StatusCode::CONFLICT,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

fn s3_redirect(region: &str) -> Response {
    let request_id = new_s3_request_id();
    let escaped_region = xml_escape(region);
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>PermanentRedirect</Code>\n  <Message>The bucket is in this region: {escaped_region}. Please use this region to retry the request.</Message>\n  <BucketRegion>{escaped_region}</BucketRegion>\n  <RequestId>{request_id}</RequestId>\n</Error>\n"
    );
    Response::builder()
        .status(axum::http::StatusCode::MOVED_PERMANENTLY)
        .header("Content-Type", "application/xml")
        .header("x-amz-request-id", request_id)
        .header("x-amz-bucket-region", region)
        .body(Body::from(body))
        .unwrap()
}

async fn select_remote_bucket_proxy_target(
    state: &AppState,
    region: &str,
) -> anyhow::Result<Option<String>> {
    let mut nodes = state
        .persistence
        .list_node_descriptors(Some(region), None)
        .await
        .map_err(|error| anyhow::anyhow!(error.to_string()))?;
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    Ok(nodes.into_iter().find_map(|node| {
        let can_proxy = node.state == LifecycleState::Active
            && node
                .capabilities
                .iter()
                .any(|capability| *capability == NodeCapability::Object)
            && !node.public_api_addr.trim().is_empty();
        can_proxy.then(|| normalize_proxy_endpoint(&node.public_api_addr))
    }))
}

fn normalize_proxy_endpoint(endpoint: &str) -> String {
    if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    }
}

fn s3_remote_bucket_response(
    policy: CrossRegionRoutingPolicy,
    region: &str,
    proxy_available: bool,
) -> Response {
    match routing::remote_bucket_routing_action(policy, proxy_available) {
        routing::RemoteBucketRoutingAction::Redirect => s3_redirect(region),
        routing::RemoteBucketRoutingAction::Proxy => add_bucket_region_header(
            s3_error(
                "InternalError",
                "Cross-region proxying was selected for an operation that cannot be proxied",
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
            region,
        ),
        routing::RemoteBucketRoutingAction::RejectLocalOnly => add_bucket_region_header(
            s3_error(
                "InvalidRequest",
                &format!(
                    "Bucket is in region {region}; cross-region routing is disabled by local_only policy"
                ),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            region,
        ),
        routing::RemoteBucketRoutingAction::ProxyUnavailable => add_bucket_region_header(
            s3_error(
                "ServiceUnavailable",
                &format!(
                    "Bucket is in region {region}; cross-region proxying is required by policy but no eligible proxy target is available"
                ),
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
            ),
            region,
        ),
    }
}

async fn s3_object_proxy_response_if_needed(
    state: &AppState,
    checked_route: &CheckedS3Route,
    claims: Option<&Claims>,
    bucket: &str,
    key: &str,
    method: &str,
    headers: &HeaderMap,
    uri: &Uri,
    version_id: Option<uuid::Uuid>,
    body: Option<Body>,
) -> Option<Response> {
    let claims = match claims {
        Some(claims) => claims,
        None => {
            let tenant_id = checked_route.tenant_id?;
            let locator = state
                .persistence
                .get_mesh_bucket_locator(tenant_id, bucket)
                .await
                .ok()
                .flatten()?;
            if locator.status == BucketLocatorStatus::Deleted
                || locator.home_region.as_str() == state.region.as_str()
            {
                return None;
            }
            return Some(s3_remote_bucket_response(
                state.config.cross_region_routing_policy,
                locator.home_region.as_str(),
                false,
            ));
        }
    };

    let target =
        match s3_object_proxy_target_if_needed(state, checked_route, claims, bucket).await? {
            Ok(target) => target,
            Err(response) => return Some(response),
        };
    Some(
        proxy_s3_object_request(
            state, target, claims, bucket, key, method, headers, uri, version_id, body,
        )
        .await,
    )
}

async fn s3_object_proxy_target_if_needed(
    state: &AppState,
    checked_route: &CheckedS3Route,
    claims: &Claims,
    bucket: &str,
) -> Option<Result<RemoteBucketProxyTarget, Response>> {
    match checked_route.remote_bucket.clone() {
        Some(target) => Some(Ok(target)),
        None => match state
            .persistence
            .get_mesh_bucket_locator(claims.tenant_id, bucket)
            .await
        {
            Ok(Some(locator))
                if locator.status != BucketLocatorStatus::Deleted
                    && locator.home_region.as_str() != state.region.as_str() =>
            {
                let proxy_endpoint =
                    match select_remote_bucket_proxy_target(state, locator.home_region.as_str())
                        .await
                    {
                        Ok(endpoint) => endpoint,
                        Err(error) => {
                            return Some(Err(s3_error(
                                "InternalError",
                                &format!("Failed to resolve remote proxy target: {error}"),
                                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                            )));
                        }
                    };
                match routing::remote_bucket_routing_action(
                    state.config.cross_region_routing_policy,
                    proxy_endpoint.is_some(),
                ) {
                    routing::RemoteBucketRoutingAction::Proxy => {
                        Some(Ok(RemoteBucketProxyTarget {
                            region: locator.home_region.as_str().to_string(),
                            bucket_locator_generation: locator.generation,
                            endpoint: proxy_endpoint.expect("proxy target checked above"),
                        }))
                    }
                    _ => {
                        return Some(Err(s3_remote_bucket_response(
                            state.config.cross_region_routing_policy,
                            locator.home_region.as_str(),
                            proxy_endpoint.is_some(),
                        )));
                    }
                }
            }
            Ok(_) => None,
            Err(error) => {
                return Some(Err(s3_error(
                    "InternalError",
                    &format!("Failed to resolve bucket route: {error}"),
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                )));
            }
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn proxy_s3_object_request(
    state: &AppState,
    target: RemoteBucketProxyTarget,
    claims: &Claims,
    bucket: &str,
    key: &str,
    method: &str,
    headers: &HeaderMap,
    uri: &Uri,
    version_id: Option<uuid::Uuid>,
    body: Option<Body>,
) -> Response {
    let request_id = new_s3_request_id();
    let mut proxy_headers = http_headers_to_proxy_headers(headers);
    if let Some(version_id) = version_id {
        proxy_headers.push(proxy_header("x-anvil-version-id", version_id.to_string()));
    }
    let authz_context = match serde_json::to_vec(claims) {
        Ok(authz_context) => authz_context,
        Err(error) => {
            return s3_error(
                "InternalError",
                &format!("Failed to encode proxy authorisation context: {error}"),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    let host = headers
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let header = ProxyRequestHeader {
        request_id: request_id.clone(),
        idempotency_key: headers
            .get("x-anvil-idempotency-key")
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string(),
        principal_id: claims.sub.clone(),
        tenant_id: claims.tenant_id.to_string(),
        bucket_name: bucket.to_string(),
        object_key: key.to_string(),
        method: method.to_ascii_uppercase(),
        canonical_host: host,
        canonical_path: uri
            .path_and_query()
            .map(|path| path.as_str())
            .unwrap_or_else(|| uri.path())
            .to_string(),
        bucket_locator_generation: target.bucket_locator_generation,
        headers: proxy_headers,
        authz_context,
    };

    let token = match state.jwt_manager.mint_token(
        "internal".to_string(),
        vec!["internal:proxy_object|*".to_string()],
        0,
    ) {
        Ok(token) => token,
        Err(error) => {
            return s3_error(
                "InternalError",
                &format!("Failed to mint internal proxy token: {error}"),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };

    let mut client = match InternalProxyServiceClient::connect(target.endpoint.clone()).await {
        Ok(client) => client,
        Err(error) => {
            return add_bucket_region_header(
                s3_error(
                    "ServiceUnavailable",
                    &format!(
                        "Failed to connect to cross-region proxy in {}: {error}",
                        target.region
                    ),
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                &target.region,
            );
        }
    };

    let (tx, rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        if tx
            .send(ProxyRequestChunk {
                part: Some(proxy_request_chunk::Part::Header(header)),
            })
            .await
            .is_err()
        {
            return;
        }
        if let Some(body) = body {
            let mut stream = body.into_data_stream();
            while let Some(chunk) = stream.next().await {
                match chunk {
                    Ok(bytes) => {
                        if tx
                            .send(ProxyRequestChunk {
                                part: Some(proxy_request_chunk::Part::Body(bytes.to_vec())),
                            })
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        tracing::warn!(error = %error, "failed to read S3 proxy request body");
                        break;
                    }
                }
            }
        }
    });

    let mut request = tonic::Request::new(tokio_stream::wrappers::ReceiverStream::new(rx));
    request.metadata_mut().insert(
        "authorization",
        match format!("Bearer {token}").parse() {
            Ok(value) => value,
            Err(_) => {
                return s3_error(
                    "InternalError",
                    "Failed to encode internal proxy token",
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                );
            }
        },
    );

    let response = match client.proxy_object(request).await {
        Ok(response) => response,
        Err(status) => return s3_proxy_status_to_response(status, &target.region),
    };
    let mut stream = response.into_inner();
    let first = match stream.next().await {
        Some(Ok(chunk)) => chunk,
        Some(Err(status)) => return s3_proxy_status_to_response(status, &target.region),
        None => {
            return add_bucket_region_header(
                s3_error(
                    "ServiceUnavailable",
                    "Cross-region proxy returned no response",
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                ),
                &target.region,
            );
        }
    };
    let response_header = match first.part {
        Some(proxy_response_chunk::Part::Header(header)) => header,
        _ => {
            return s3_error(
                "InternalError",
                "Cross-region proxy response did not start with a header",
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    };
    s3_proxy_response_to_http(method, &target.region, headers, response_header, stream)
}

fn http_headers_to_proxy_headers(headers: &HeaderMap) -> Vec<ProxyHeader> {
    headers
        .iter()
        .filter(|(name, _)| *name != http::header::AUTHORIZATION)
        .flat_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|_| proxy_header(name.as_str(), value.as_bytes()))
        })
        .collect()
}

fn proxy_header(name: &str, value: impl AsRef<[u8]>) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.as_ref().to_vec(),
    }
}

fn s3_proxy_status_to_response(status: tonic::Status, region: &str) -> Response {
    let status_code = match status.code() {
        tonic::Code::InvalidArgument => axum::http::StatusCode::BAD_REQUEST,
        tonic::Code::Unauthenticated => axum::http::StatusCode::UNAUTHORIZED,
        tonic::Code::PermissionDenied => axum::http::StatusCode::FORBIDDEN,
        tonic::Code::FailedPrecondition => axum::http::StatusCode::PRECONDITION_FAILED,
        tonic::Code::NotFound => axum::http::StatusCode::NOT_FOUND,
        tonic::Code::Unavailable => axum::http::StatusCode::SERVICE_UNAVAILABLE,
        _ => axum::http::StatusCode::INTERNAL_SERVER_ERROR,
    };
    add_bucket_region_header(
        s3_error("CrossRegionProxyError", status.message(), status_code),
        region,
    )
}

fn s3_proxy_response_to_http(
    method: &str,
    region: &str,
    request_headers: &HeaderMap,
    header: ProxyResponseHeader,
    stream: tonic::Streaming<anvil_core::anvil_api::ProxyResponseChunk>,
) -> Response {
    let mut status = axum::http::StatusCode::from_u16(header.status as u16)
        .unwrap_or(axum::http::StatusCode::BAD_GATEWAY);
    let proxy_headers = header.headers;
    let content_length = proxy_header_value(&proxy_headers, "content-length")
        .and_then(|value| value.parse::<u64>().ok());
    let etag = proxy_header_value(&proxy_headers, "etag");
    let last_modified = proxy_header_value(&proxy_headers, "x-anvil-created-at")
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&chrono::Utc));

    if status.is_success()
        && (method.eq_ignore_ascii_case("GET") || method.eq_ignore_ascii_case("HEAD"))
        && let (Some(etag), Some(last_modified)) = (etag.as_deref(), last_modified)
        && let Some(response) = evaluate_object_preconditions(request_headers, etag, last_modified)
    {
        return response;
    }

    let range = if status == axum::http::StatusCode::OK && method.eq_ignore_ascii_case("GET") {
        match parse_http_range(request_headers, content_length) {
            Ok(Some(requested_range)) => {
                let Some(content_length) = content_length else {
                    return invalid_range_response(0);
                };
                match requested_range.resolve(content_length) {
                    Ok(range) => {
                        status = axum::http::StatusCode::PARTIAL_CONTENT;
                        Some(range)
                    }
                    Err(response) => return response,
                }
            }
            Ok(None) => None,
            Err(response) => return response,
        }
    } else {
        None
    };

    let mut builder = Response::builder().status(status);
    for proxy_header in proxy_headers {
        builder = add_proxy_response_header(builder, proxy_header);
    }
    builder = builder.header("x-amz-bucket-region", region);
    if let Some(last_modified) = last_modified {
        builder = builder.header(
            "Last-Modified",
            httpdate::fmt_http_date(object_last_modified_time(last_modified)),
        );
    }
    if let Some(range) = range {
        builder = builder.header("Content-Length", range.len()).header(
            "Content-Range",
            format!(
                "bytes {}-{}/{}",
                range.start,
                range.end,
                content_length.unwrap_or_default()
            ),
        );
    } else if let Some(content_length) = content_length {
        builder = builder.header("Content-Length", content_length);
    }
    if method.eq_ignore_ascii_case("HEAD") || status == axum::http::StatusCode::NO_CONTENT {
        return builder.body(Body::empty()).unwrap();
    }

    let body_stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + 'static>> =
        Box::pin(stream.filter_map(|chunk| async move {
            match chunk {
                Ok(chunk) => match chunk.part {
                    Some(proxy_response_chunk::Part::Body(bytes)) => Some(Ok(bytes)),
                    Some(proxy_response_chunk::Part::Header(_)) | None => None,
                },
                Err(status) => Some(Err(status)),
            }
        }));
    let body_stream = match range {
        Some(range) => slice_stream_by_range(body_stream, range),
        None => body_stream,
    };
    let body_stream = body_stream.map(|chunk| match chunk {
        Ok(bytes) => Ok(Bytes::from(bytes)),
        Err(status) => Err(axum::Error::new(status)),
    });
    builder.body(Body::from_stream(body_stream)).unwrap()
}

fn proxy_header_value(headers: &[ProxyHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| std::str::from_utf8(&header.value).ok())
        .map(ToOwned::to_owned)
}

fn add_proxy_response_header(
    mut builder: http::response::Builder,
    header: ProxyHeader,
) -> http::response::Builder {
    let name = match header.name.as_str() {
        "x-anvil-version-id" => "x-amz-version-id",
        "content-length" | "x-anvil-created-at" => return builder,
        other => other,
    };
    if let (Ok(name), Ok(value)) = (
        http::header::HeaderName::from_bytes(name.as_bytes()),
        http::HeaderValue::from_bytes(&header.value),
    ) {
        builder = builder.header(name, value);
    }
    builder
}

fn add_bucket_region_header(mut response: Response, region: &str) -> Response {
    if let Ok(value) = http::HeaderValue::from_str(region) {
        response.headers_mut().insert("x-amz-bucket-region", value);
    }
    response
}

fn remote_bucket_region_from_status(status: &tonic::Status) -> Option<String> {
    status
        .metadata()
        .get("x-anvil-bucket-region")
        .and_then(|value| value.to_str().ok())
        .filter(|region| !region.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            status
                .message()
                .strip_prefix("Bucket is in region ")
                .and_then(|rest| {
                    rest.split(';')
                        .next()
                        .map(str::trim)
                        .filter(|region| !region.is_empty())
                        .map(ToOwned::to_owned)
                })
        })
}

fn s3_remote_bucket_response_from_status(
    status: &tonic::Status,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Option<Response> {
    remote_bucket_region_from_status(status)
        .map(|region| s3_remote_bucket_response(cross_region_policy, &region, false))
}

fn s3_unavailable_status_to_response(
    status: &tonic::Status,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    s3_remote_bucket_response_from_status(status, cross_region_policy).unwrap_or_else(|| {
        s3_error(
            "ServiceUnavailable",
            status.message(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
        )
    })
}

async fn s3_remote_bucket_response_for_authorized_claims(
    state: &AppState,
    claims: &Claims,
    bucket: &str,
    action: AnvilAction,
) -> Result<Option<Response>, Response> {
    if !auth::is_authorized(action, bucket, &claims.scopes) {
        return Err(s3_error(
            "AccessDenied",
            "Permission denied",
            axum::http::StatusCode::FORBIDDEN,
        ));
    }

    match state
        .persistence
        .get_mesh_bucket_locator(claims.tenant_id, bucket)
        .await
    {
        Ok(Some(locator))
            if locator.status != BucketLocatorStatus::Deleted
                && locator.home_region.as_str() != state.region.as_str() =>
        {
            Ok(Some(s3_remote_bucket_response(
                state.config.cross_region_routing_policy,
                locator.home_region.as_str(),
                false,
            )))
        }
        Ok(_) => Ok(None),
        Err(error) => Err(s3_error(
            "InternalError",
            &format!("Failed to resolve bucket route: {error}"),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

async fn head_bucket(
    State(state): State<AppState>,
    Path(mut bucket_name): Path<String>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        let q = s3_query_map(req.uri());
        return Box::pin(head_object(
            State(state),
            Path((bucket, key)),
            Query(q),
            req,
        ))
        .await;
    }
    bucket_name = s3_routed_bucket(&req, bucket_name);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials for HEAD request",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .expect("authenticated head bucket path supplied claims");

    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        &bucket_name,
        AnvilAction::BucketRead,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, &bucket_name).await
    {
        Ok(Some(bucket)) => {
            if bucket.region != state.region {
                return s3_remote_bucket_response(
                    state.config.cross_region_routing_policy,
                    &bucket.region,
                    false,
                );
            }
            (axum::http::StatusCode::OK, "").into_response()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn list_objects(
    State(state): State<AppState>,
    bucket: Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        return Box::pin(get_object(State(state), Path((bucket, key)), Query(q), req)).await;
    }
    let Path(bucket) = bucket;
    let bucket = s3_routed_bucket(&req, bucket);
    let checked_route = match s3_checked_route(
        &state,
        s3_host_route(&req),
        req.extensions().get::<Claims>().cloned(),
    )
    .await
    {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route.claims.clone();

    if q.contains_key("versions") {
        let request_is_authenticated = req.extensions().get::<Claims>().is_some();
        return list_object_versions_response(
            state,
            claims,
            checked_route.tenant_id,
            &bucket,
            &q,
            request_is_authenticated,
        )
        .await;
    }

    if q.contains_key("uploads") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        return list_multipart_uploads_response(state, claims, &bucket, &q).await;
    }

    if q.contains_key("versioning") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        return get_bucket_versioning_response(state, claims, &bucket).await;
    }

    if q.contains_key("location") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        return get_bucket_location_response(state, claims, &bucket).await;
    }

    let is_list_v2 = q
        .get("list-type")
        .or_else(|| q.get("listType"))
        .is_some_and(|value| value == "2");
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let continuation_token = q
        .get("continuation-token")
        .or_else(|| q.get("continuationToken"))
        .cloned();
    let marker = q.get("marker").cloned().unwrap_or_default();
    let start_after = if is_list_v2 {
        continuation_token.clone().unwrap_or_else(|| {
            q.get("start-after")
                .or_else(|| q.get("startAfter"))
                .cloned()
                .unwrap_or_default()
        })
    } else {
        marker.clone()
    };
    let delimiter = q.get("delimiter").cloned().unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let fetch_limit = max_keys.saturating_add(1);

    match state
        .object_manager
        .list_objects_for_tenant(
            claims,
            checked_route.tenant_id,
            &bucket,
            &prefix,
            &start_after,
            fetch_limit,
            &delimiter,
        )
        .await
    {
        Ok((objects, common_prefixes)) => {
            let requested_max_keys = if max_keys <= 0 {
                1000
            } else {
                max_keys as usize
            };
            let (entries, is_truncated, next_marker) =
                paginate_list_bucket_entries(objects, common_prefixes, requested_max_keys);
            let key_count = entries.len() as i32;
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", &*bucket));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            if is_list_v2 {
                if let Some(token) = continuation_token {
                    xml.push_str(&format!(
                        "  <ContinuationToken>{}</ContinuationToken>\n",
                        xml_escape(&token)
                    ));
                }
                xml.push_str(&format!("  <KeyCount>{}</KeyCount>\n", key_count));
            } else {
                xml.push_str(&format!("  <Marker>{}</Marker>\n", xml_escape(&marker)));
            }
            if !delimiter.is_empty() {
                xml.push_str(&format!(
                    "  <Delimiter>{}</Delimiter>\n",
                    xml_escape(&delimiter)
                ));
            }
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if is_truncated { "true" } else { "false" }
            ));
            if let Some(token) = next_marker {
                if is_list_v2 {
                    xml.push_str(&format!(
                        "  <NextContinuationToken>{}</NextContinuationToken>\n",
                        xml_escape(&token)
                    ));
                } else {
                    xml.push_str(&format!(
                        "  <NextMarker>{}</NextMarker>\n",
                        xml_escape(&token)
                    ));
                }
            }
            for entry in entries {
                append_list_bucket_entry_xml(&mut xml, entry);
            }
            xml.push_str("</ListBucketResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchBucket",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::Unavailable => {
                s3_unavailable_status_to_response(&status, state.config.cross_region_routing_policy)
            }
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn post_bucket(
    State(state): State<AppState>,
    Path(mut bucket): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some((bucket, key)) = s3_routed_object(&req) {
        return Box::pin(post_object(
            State(state),
            Path((bucket, key)),
            Query(q),
            req,
        ))
        .await;
    }
    bucket = s3_routed_bucket(&req, bucket);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(claims) => claims,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .expect("authenticated post bucket path supplied claims");

    if q.contains_key("delete") {
        let bytes = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return s3_error(
                    "InvalidRequest",
                    &format!("Failed to read DeleteObjects body: {error}"),
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return delete_objects(state, claims, bucket, bytes).await;
    }

    s3_error(
        "InvalidArgument",
        "Unsupported bucket POST operation",
        axum::http::StatusCode::BAD_REQUEST,
    )
}

async fn delete_objects(
    state: AppState,
    claims: Claims,
    bucket: String,
    body: axum::body::Bytes,
) -> Response {
    let request = match quick_xml::de::from_reader::<_, DeleteObjectsXml>(&body[..]) {
        Ok(request) => request,
        Err(error) => {
            return s3_error(
                "MalformedXML",
                &format!("Invalid DeleteObjects body: {error}"),
                axum::http::StatusCode::BAD_REQUEST,
            );
        }
    };

    let quiet = request.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for object in request.objects {
        let key = object.key;
        let requested_version_id = object.version_id;

        if validation::is_reserved_internal_key(&key) {
            errors.push(DeleteObjectError {
                key,
                version_id: requested_version_id,
                code: "UnauthorizedReservedNamespace".to_string(),
                message: "UnauthorizedReservedNamespace".to_string(),
            });
            continue;
        }

        let version_id = match requested_version_id.as_deref() {
            Some("") | None => None,
            Some(version_id) => match uuid::Uuid::parse_str(version_id) {
                Ok(version_id) => Some(version_id),
                Err(_) => {
                    errors.push(DeleteObjectError {
                        key,
                        version_id: requested_version_id,
                        code: "InvalidArgument".to_string(),
                        message: "Invalid versionId".to_string(),
                    });
                    continue;
                }
            },
        };

        let delete_result = if let Some(version_id) = version_id {
            state
                .object_manager
                .delete_object_version(claims.tenant_id, &bucket, &key, version_id, &claims.scopes)
                .await
        } else {
            state
                .object_manager
                .delete_object(claims.tenant_id, &bucket, &key, &claims.scopes)
                .await
        };

        match delete_result {
            Ok(delete_marker) => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key,
                        version_id: requested_version_id,
                        delete_marker: Some(delete_marker.deleted_at.is_some()),
                        delete_marker_version_id: delete_marker
                            .deleted_at
                            .is_some()
                            .then(|| delete_marker.version_id.to_string()),
                    });
                }
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key,
                        version_id: None,
                        delete_marker: None,
                        delete_marker_version_id: None,
                    });
                }
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                errors.push(DeleteObjectError::from_status(
                    key,
                    requested_version_id,
                    status,
                ));
            }
            Err(status) => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                errors.push(DeleteObjectError::from_status(
                    key,
                    requested_version_id,
                    status,
                ));
            }
        }
    }

    delete_objects_result_response(deleted, errors)
}

#[derive(Debug)]
struct DeletedObject {
    key: String,
    version_id: Option<String>,
    delete_marker: Option<bool>,
    delete_marker_version_id: Option<String>,
}

#[derive(Debug)]
struct DeleteObjectError {
    key: String,
    version_id: Option<String>,
    code: String,
    message: String,
}

impl DeleteObjectError {
    fn from_status(key: String, version_id: Option<String>, status: tonic::Status) -> Self {
        let code = match status.code() {
            tonic::Code::PermissionDenied => "AccessDenied",
            tonic::Code::InvalidArgument => "InvalidArgument",
            tonic::Code::NotFound => "NoSuchKey",
            tonic::Code::Unimplemented => "NotImplemented",
            _ => "InternalError",
        };
        Self {
            key,
            version_id,
            code: code.to_string(),
            message: status.message().to_string(),
        }
    }
}

fn delete_objects_result_response(
    deleted: Vec<DeletedObject>,
    errors: Vec<DeleteObjectError>,
) -> Response {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );

    for object in deleted {
        xml.push_str("  <Deleted>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
        if let Some(version_id) = object.version_id {
            xml.push_str(&format!(
                "    <VersionId>{}</VersionId>\n",
                xml_escape(&version_id)
            ));
        }
        if let Some(delete_marker) = object.delete_marker {
            xml.push_str(&format!(
                "    <DeleteMarker>{}</DeleteMarker>\n",
                if delete_marker { "true" } else { "false" }
            ));
        }
        if let Some(version_id) = object.delete_marker_version_id {
            xml.push_str(&format!(
                "    <DeleteMarkerVersionId>{}</DeleteMarkerVersionId>\n",
                xml_escape(&version_id)
            ));
        }
        xml.push_str("  </Deleted>\n");
    }

    for error in errors {
        xml.push_str("  <Error>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&error.key)));
        if let Some(version_id) = error.version_id {
            xml.push_str(&format!(
                "    <VersionId>{}</VersionId>\n",
                xml_escape(&version_id)
            ));
        }
        xml.push_str(&format!("    <Code>{}</Code>\n", xml_escape(&error.code)));
        xml.push_str(&format!(
            "    <Message>{}</Message>\n",
            xml_escape(&error.message)
        ));
        xml.push_str("  </Error>\n");
    }

    xml.push_str("</DeleteResult>\n");
    Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(Body::from(xml))
        .unwrap()
}

async fn get_bucket_location_response(state: AppState, claims: Claims, bucket: &str) -> Response {
    match s3_remote_bucket_response_for_authorized_claims(
        &state,
        &claims,
        bucket,
        AnvilAction::BucketRead,
    )
    .await
    {
        Ok(Some(response)) => return response,
        Ok(None) => {}
        Err(response) => return response,
    }

    match bucket_journal::read_current_bucket(&state.storage, claims.tenant_id, bucket).await {
        Ok(Some(bucket)) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>\n",
                xml_escape(&bucket.region)
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("x-amz-bucket-region", bucket.region)
                .body(Body::from(xml))
                .unwrap()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn list_multipart_uploads_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
    q: &HashMap<String, String>,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let key_marker = q
        .get("key-marker")
        .or_else(|| q.get("keyMarker"))
        .cloned()
        .unwrap_or_default();
    let upload_id_marker = match q
        .get("upload-id-marker")
        .or_else(|| q.get("uploadIdMarker"))
    {
        Some(value) => match uuid::Uuid::parse_str(value) {
            Ok(upload_id) => Some(upload_id),
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid upload-id-marker",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => None,
    };
    let max_uploads: i32 = q
        .get("max-uploads")
        .or_else(|| q.get("maxUploads"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_multipart_uploads(
            claims.tenant_id,
            bucket,
            &prefix,
            &key_marker,
            upload_id_marker,
            max_uploads,
            &claims.scopes,
        )
        .await
    {
        Ok(page) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <KeyMarker>{}</KeyMarker>\n  <UploadIdMarker>{}</UploadIdMarker>\n",
                xml_escape(bucket),
                xml_escape(&key_marker),
                upload_id_marker
                    .map(|marker| marker.to_string())
                    .as_deref()
                    .map(xml_escape)
                    .unwrap_or_default()
            );
            if let Some(next_key_marker) = page.next_key_marker.as_deref() {
                xml.push_str(&format!(
                    "  <NextKeyMarker>{}</NextKeyMarker>\n",
                    xml_escape(next_key_marker)
                ));
            }
            if let Some(next_upload_id_marker) = page.next_upload_id_marker {
                xml.push_str(&format!(
                    "  <NextUploadIdMarker>{next_upload_id_marker}</NextUploadIdMarker>\n"
                ));
            }
            xml.push_str(&format!(
                "  <Delimiter></Delimiter>\n  <Prefix>{}</Prefix>\n  <MaxUploads>{}</MaxUploads>\n  <IsTruncated>{}</IsTruncated>\n",
                xml_escape(&prefix),
                max_uploads,
                if page.is_truncated { "true" } else { "false" }
            ));
            for upload in page.uploads {
                xml.push_str("  <Upload>\n");
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&upload.key)));
                xml.push_str(&format!("    <UploadId>{}</UploadId>\n", upload.upload_id));
                xml.push_str(&format!(
                    "    <Initiated>{}</Initiated>\n",
                    upload.created_at.to_rfc3339()
                ));
                xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                xml.push_str("  </Upload>\n");
            }
            xml.push_str("</ListMultipartUploadsResult>\n");
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn list_object_versions_response(
    state: AppState,
    claims: Option<Claims>,
    route_tenant_id: Option<i64>,
    bucket: &str,
    q: &HashMap<String, String>,
    request_is_authenticated: bool,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let key_marker = q
        .get("key-marker")
        .or_else(|| q.get("keyMarker"))
        .cloned()
        .unwrap_or_default();
    let version_id_marker = q
        .get("version-id-marker")
        .or_else(|| q.get("versionIdMarker"))
        .cloned()
        .unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_object_versions_for_tenant(
            claims,
            route_tenant_id,
            bucket,
            &prefix,
            &key_marker,
            &version_id_marker,
            max_keys,
        )
        .await
    {
        Ok(page) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket)));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str(&format!(
                "  <KeyMarker>{}</KeyMarker>\n",
                xml_escape(&key_marker)
            ));
            xml.push_str(&format!(
                "  <VersionIdMarker>{}</VersionIdMarker>\n",
                xml_escape(&version_id_marker)
            ));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            if let Some(next_key_marker) = page.next_key_marker.as_deref() {
                xml.push_str(&format!(
                    "  <NextKeyMarker>{}</NextKeyMarker>\n",
                    xml_escape(next_key_marker)
                ));
            }
            if let Some(next_version_id_marker) = page.next_version_id_marker {
                xml.push_str(&format!(
                    "  <NextVersionIdMarker>{}</NextVersionIdMarker>\n",
                    next_version_id_marker
                ));
            }
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if page.is_truncated { "true" } else { "false" }
            ));
            for version in page.versions {
                let object = version.object;
                let tag = if version.is_delete_marker {
                    "DeleteMarker"
                } else {
                    "Version"
                };
                xml.push_str(&format!("  <{}>\n", tag));
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
                xml.push_str(&format!(
                    "    <VersionId>{}</VersionId>\n",
                    object.version_id
                ));
                xml.push_str(&format!(
                    "    <IsLatest>{}</IsLatest>\n",
                    if version.is_latest { "true" } else { "false" }
                ));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    object.created_at.to_rfc3339()
                ));
                if !version.is_delete_marker {
                    xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", object.etag));
                    xml.push_str(&format!("    <Size>{}</Size>\n", object.size));
                    xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                }
                xml.push_str(&format!("  </{}>\n", tag));
            }
            xml.push_str("</ListVersionsResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            request_is_authenticated,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

fn s3_status_to_response_for_auth(
    status: tonic::Status,
    request_is_authenticated: bool,
    not_found_code: &str,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::FailedPrecondition => {
            if let Some(response) =
                s3_remote_bucket_response_from_status(&status, cross_region_policy)
            {
                return response;
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => {
            if !request_is_authenticated {
                s3_error(
                    "AccessDenied",
                    status.message(),
                    axum::http::StatusCode::FORBIDDEN,
                )
            } else {
                s3_error(
                    not_found_code,
                    status.message(),
                    axum::http::StatusCode::NOT_FOUND,
                )
            }
        }
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

#[derive(Debug)]
enum ListBucketEntry {
    Object(Object),
    Prefix(String),
}

fn paginate_list_bucket_entries(
    objects: Vec<Object>,
    common_prefixes: Vec<String>,
    requested_max_keys: usize,
) -> (Vec<ListBucketEntry>, bool, Option<String>) {
    let mut entries = objects
        .into_iter()
        .map(ListBucketEntry::Object)
        .chain(common_prefixes.into_iter().map(ListBucketEntry::Prefix))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.marker()
            .cmp(right.marker())
            .then_with(|| left.kind_order().cmp(&right.kind_order()))
    });

    let is_truncated = entries.len() > requested_max_keys;
    if is_truncated {
        entries.truncate(requested_max_keys);
    }
    let next_continuation_token = is_truncated
        .then(|| entries.last().map(|entry| entry.marker().to_string()))
        .flatten();

    (entries, is_truncated, next_continuation_token)
}

impl ListBucketEntry {
    fn marker(&self) -> &str {
        match self {
            Self::Object(object) => &object.key,
            Self::Prefix(prefix) => prefix,
        }
    }

    fn kind_order(&self) -> u8 {
        match self {
            Self::Object(_) => 0,
            Self::Prefix(_) => 1,
        }
    }
}

fn append_list_bucket_entry_xml(xml: &mut String, entry: ListBucketEntry) {
    match entry {
        ListBucketEntry::Object(object) => {
            xml.push_str("  <Contents>\n");
            xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
            xml.push_str(&format!(
                "    <LastModified>{}</LastModified>\n",
                object.created_at.to_rfc3339()
            ));
            xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", object.etag));
            xml.push_str(&format!("    <Size>{}</Size>\n", object.size));
            xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
            xml.push_str("  </Contents>\n");
        }
        ListBucketEntry::Prefix(prefix) => {
            xml.push_str("  <CommonPrefixes>\n");
            xml.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str("  </CommonPrefixes>\n");
        }
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn s3_user_metadata(headers: &axum::http::HeaderMap) -> Option<serde_json::Value> {
    let mut values = serde_json::Map::new();
    for (name, value) in headers {
        let Some(metadata_key) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        let Ok(metadata_value) = value.to_str() else {
            continue;
        };
        values.insert(
            metadata_key.to_string(),
            serde_json::Value::String(metadata_value.to_string()),
        );
    }
    if values.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(values))
    }
}

fn add_s3_user_metadata_headers(
    mut builder: axum::http::response::Builder,
    user_meta: Option<&serde_json::Value>,
) -> axum::http::response::Builder {
    let Some(serde_json::Value::Object(values)) = user_meta else {
        return builder;
    };
    for (key, value) in values {
        if let Some(value) = value.as_str() {
            builder = builder.header(format!("x-amz-meta-{key}"), value);
        }
    }
    builder
}

#[cfg(test)]
mod list_bucket_pagination_tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn list_bucket_pagination_uses_last_returned_entry_as_continuation_token() {
        let (entries, is_truncated, token) = paginate_list_bucket_entries(
            vec![object("page/a.txt"), object("page/b.txt")],
            Vec::new(),
            1,
        );

        assert!(is_truncated);
        assert_eq!(token.as_deref(), Some("page/a.txt"));
        assert_eq!(
            entries
                .iter()
                .map(ListBucketEntry::marker)
                .collect::<Vec<_>>(),
            vec!["page/a.txt"]
        );
    }

    #[test]
    fn list_bucket_pagination_merges_objects_and_common_prefixes_before_truncating() {
        let (entries, is_truncated, token) = paginate_list_bucket_entries(
            vec![object("root/b.txt")],
            vec!["root/a/".to_string(), "root/c/".to_string()],
            2,
        );

        assert!(is_truncated);
        assert_eq!(token.as_deref(), Some("root/b.txt"));
        assert_eq!(
            entries
                .iter()
                .map(ListBucketEntry::marker)
                .collect::<Vec<_>>(),
            vec!["root/a/", "root/b.txt"]
        );
    }

    fn object(key: &str) -> Object {
        Object {
            id: 0,
            tenant_id: 0,
            bucket_id: 0,
            key: key.to_string(),
            kind: object_links::ObjectEntryKind::Blob,
            content_hash: String::new(),
            size: 0,
            etag: String::new(),
            content_type: None,
            version_id: uuid::Uuid::nil(),
            mutation_id: uuid::Uuid::nil(),
            index_policy_snapshot: String::new(),
            user_metadata_hash: String::new(),
            authz_revision: 0,
            record_hash: String::new(),
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: None,
            user_meta: None,
            shard_map: None,
            checksum: None,
            link: None,
        }
    }
}

async fn readiness_check(State(state): State<AppState>) -> Response {
    // Cluster readiness: at least 1 peer known (self included).
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
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(list_objects(State(state), Path(bucket), Query(q), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let checked_route = match s3_checked_route(
        &state,
        s3_host_route(&req),
        req.extensions().get::<Claims>().cloned(),
    )
    .await
    {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route.claims.clone();
    if let Some(upload_id) = q.get("uploadId") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return list_multipart_parts_response(state, claims, bucket, key, upload_id, &q).await;
    }

    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };
    if is_link_metadata_request(req.headers()) {
        return get_object_link_metadata_response(
            state,
            claims,
            checked_route.tenant_id,
            &bucket,
            &key,
            version_id,
        )
        .await;
    }
    if let Some(response) = s3_object_proxy_response_if_needed(
        &state,
        &checked_route,
        checked_route.claims.as_ref(),
        &bucket,
        &key,
        "GET",
        req.headers(),
        req.uri(),
        version_id,
        None,
    )
    .await
    {
        return response;
    }
    let requested_range = match parse_http_range(req.headers(), None) {
        Ok(range) => range,
        Err(response) => return response,
    };

    let response_bucket = bucket.clone();
    let response_key = key.clone();
    match state
        .object_manager
        .get_object_with_link_mode_for_tenant(
            claims,
            checked_route.tenant_id,
            bucket,
            key,
            version_id,
            ObjectLinkReadMode::Follow,
        )
        .await
    {
        Ok(result) => {
            let anvil_core::object_manager::ObjectReadResult {
                object,
                stream,
                followed_link,
            } = result;
            if let Some(response) =
                evaluate_object_preconditions(req.headers(), &object.etag, object.created_at)
            {
                return response;
            }
            let range = match requested_range {
                Some(range_header) => match range_header.resolve(object.size as u64) {
                    Ok(range) => Some(range),
                    Err(response) => return response,
                },
                None => None,
            };
            let (status, content_length, body_stream) = match range {
                Some(range) => (
                    axum::http::StatusCode::PARTIAL_CONTENT,
                    range.len() as i64,
                    slice_stream_by_range(stream, range),
                ),
                None => (axum::http::StatusCode::OK, object.size, stream),
            };
            let mut builder = Response::builder()
                .status(status)
                .header("Content-Type", object.content_type.unwrap_or_default())
                .header("Content-Length", content_length)
                .header("ETag", object.etag)
                .header("Accept-Ranges", "bytes")
                .header("x-amz-version-id", object.version_id.to_string());
            builder = add_followed_link_headers(builder, followed_link.as_ref());
            builder = add_s3_user_metadata_headers(builder, object.user_meta.as_ref());
            if let Some(range) = range {
                builder = builder.header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", range.start, range.end, object.size),
                );
            }
            builder
                .body(Body::from_stream(body_stream.map(move |r| {
                    r.map_err(|e| {
                        tracing::warn!(
                            bucket = %response_bucket,
                            key = %response_key,
                            error = %e,
                            "S3 object body stream failed"
                        );
                        axum::Error::new(e)
                    })
                })))
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchKey",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::Unavailable => {
                s3_unavailable_status_to_response(&status, state.config.cross_region_routing_policy)
            }
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn get_object_link_metadata_response(
    state: AppState,
    claims: Option<Claims>,
    route_tenant_id: Option<i64>,
    bucket: &str,
    key: &str,
    version_id: Option<uuid::Uuid>,
) -> Response {
    match state
        .object_manager
        .read_object_link_for_tenant(claims.clone(), route_tenant_id, bucket, key, version_id)
        .await
    {
        Ok(descriptor) => {
            let body = match serde_json::to_vec(&descriptor) {
                Ok(body) => body,
                Err(error) => {
                    return s3_error(
                        "InternalError",
                        &error.to_string(),
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    );
                }
            };
            let builder = Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("Content-Type", object_links::LINK_METADATA_CONTENT_TYPE)
                .header("Content-Length", body.len())
                .header("ETag", object_links::link_metadata_etag(&descriptor));
            add_link_descriptor_headers(builder, &descriptor)
                .body(Body::from(body))
                .unwrap()
        }
        Err(status) => link_status_to_response(
            status,
            claims.is_some(),
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn head_object_link_metadata_response(
    state: AppState,
    claims: Option<Claims>,
    route_tenant_id: Option<i64>,
    bucket: &str,
    key: &str,
    version_id: Option<uuid::Uuid>,
) -> Response {
    match state
        .object_manager
        .read_object_link_for_tenant(claims.clone(), route_tenant_id, bucket, key, version_id)
        .await
    {
        Ok(descriptor) => {
            let content_length = serde_json::to_vec(&descriptor)
                .map(|body| body.len())
                .unwrap_or(0);
            let builder = Response::builder()
                .status(axum::http::StatusCode::OK)
                .header("Content-Type", object_links::LINK_METADATA_CONTENT_TYPE)
                .header("Content-Length", content_length)
                .header("ETag", object_links::link_metadata_etag(&descriptor));
            add_link_descriptor_headers(builder, &descriptor)
                .body(Body::empty())
                .unwrap()
        }
        Err(status) => link_status_to_response(
            status,
            claims.is_some(),
            state.config.cross_region_routing_policy,
        ),
    }
}

fn is_link_metadata_request(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get("x-anvil-link-mode")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("metadata"))
}

fn add_followed_link_headers(
    builder: axum::http::response::Builder,
    followed_link: Option<&object_links::FollowedObjectLink>,
) -> axum::http::response::Builder {
    let Some(followed_link) = followed_link else {
        return builder;
    };
    add_link_descriptor_headers(builder, &followed_link.descriptor)
}

fn add_link_descriptor_headers(
    builder: axum::http::response::Builder,
    descriptor: &object_links::ObjectLinkDescriptor,
) -> axum::http::response::Builder {
    builder
        .header("x-anvil-object-kind", "link")
        .header("x-anvil-link-key", descriptor.link_key.clone())
        .header("x-anvil-link-generation", descriptor.generation.to_string())
        .header(
            "x-anvil-link-target-version",
            descriptor.target_version.clone().unwrap_or_default(),
        )
}

fn link_status_to_response(
    status: tonic::Status,
    has_claims: bool,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::NotFound => {
            if has_claims {
                s3_error(
                    "NoSuchKey",
                    status.message(),
                    axum::http::StatusCode::NOT_FOUND,
                )
            } else {
                s3_error(
                    "AccessDenied",
                    status.message(),
                    axum::http::StatusCode::FORBIDDEN,
                )
            }
        }
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::FailedPrecondition => s3_error(
            status.message(),
            status.message(),
            axum::http::StatusCode::PRECONDITION_FAILED,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        tonic::Code::Unavailable => s3_unavailable_status_to_response(&status, cross_region_policy),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn list_multipart_parts_response(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    q: &HashMap<String, String>,
) -> Response {
    let part_number_marker: i32 = q
        .get("part-number-marker")
        .or_else(|| q.get("partNumberMarker"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let max_parts: i32 = q
        .get("max-parts")
        .or_else(|| q.get("maxParts"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000);
    match state
        .object_manager
        .list_multipart_parts(
            claims.tenant_id,
            &bucket,
            &key,
            upload_id,
            part_number_marker,
            max_parts,
            &claims.scopes,
        )
        .await
    {
        Ok(page) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n  <PartNumberMarker>{}</PartNumberMarker>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                upload_id,
                part_number_marker
            );
            if let Some(next_part_number_marker) = page.next_part_number_marker {
                xml.push_str(&format!(
                    "  <NextPartNumberMarker>{next_part_number_marker}</NextPartNumberMarker>\n"
                ));
            }
            xml.push_str(&format!(
                "  <MaxParts>{}</MaxParts>\n  <IsTruncated>{}</IsTruncated>\n",
                max_parts,
                if page.is_truncated { "true" } else { "false" }
            ));
            for part in page.parts {
                xml.push_str("  <Part>\n");
                xml.push_str(&format!(
                    "    <PartNumber>{}</PartNumber>\n",
                    part.part_number
                ));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    part.created_at.to_rfc3339()
                ));
                xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", part.etag));
                xml.push_str(&format!("    <Size>{}</Size>\n", part.size));
                xml.push_str("  </Part>\n");
            }
            xml.push_str("</ListPartsResult>\n");
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn put_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(create_bucket(State(state), Path(bucket), Query(q), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .clone()
        .expect("authenticated put object path supplied claims");
    let copy_source = match req.headers().get("x-amz-copy-source") {
        Some(value) => match value.to_str() {
            Ok(value) => Some(value.to_owned()),
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid x-amz-copy-source",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => None,
    };

    if let Some(copy_source) = copy_source {
        return copy_object(state, claims, bucket, key, copy_source, req.headers()).await;
    }

    if let Some(upload_id) = q.get("uploadId") {
        let part_number = match q
            .get("partNumber")
            .and_then(|value| value.parse::<i32>().ok())
        {
            Some(part_number) => part_number,
            None => {
                return s3_error(
                    "InvalidArgument",
                    "Missing or invalid partNumber",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let body_stream = req.into_body().into_data_stream().map(|r| {
            r.map(|chunk| chunk.to_vec())
                .map_err(|e| tonic::Status::internal(e.to_string()))
        });
        return upload_part(
            state,
            claims,
            bucket,
            key,
            upload_id,
            part_number,
            body_stream,
        )
        .await;
    }

    if let Some(proxy_target) =
        s3_object_proxy_target_if_needed(&state, &checked_route, &claims, &bucket).await
    {
        return match proxy_target {
            Ok(proxy_target) => {
                let headers = req.headers().clone();
                let uri = req.uri().clone();
                let body = req.into_body();
                proxy_s3_object_request(
                    &state,
                    proxy_target,
                    &claims,
                    &bucket,
                    &key,
                    "PUT",
                    &headers,
                    &uri,
                    None,
                    Some(body),
                )
                .await
            }
            Err(response) => response,
        };
    }

    if request_has_write_etag_preconditions(req.headers()) {
        let current = match state
            .object_manager
            .current_object_for_write_precondition(claims.tenant_id, &bucket, &key, &claims.scopes)
            .await
        {
            Ok(current) => current,
            Err(status) => {
                return s3_status_to_response_for_auth(
                    status,
                    true,
                    "NoSuchBucket",
                    state.config.cross_region_routing_policy,
                );
            }
        };
        if let Some(response) = evaluate_write_etag_preconditions(
            req.headers(),
            current.as_ref().map(|object| object.etag.as_str()),
        ) {
            return response;
        }
    }

    let options = ObjectWriteOptions {
        content_type: req
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
        user_metadata: s3_user_metadata(req.headers()),
    };
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
            options,
        )
        .await
    {
        Ok(object) => Response::builder()
            .status(200)
            .header("ETag", object.etag)
            .header("x-amz-version-id", object.version_id.to_string())
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => s3_error(
                "NoSuchBucket",
                status.message(),
                axum::http::StatusCode::NOT_FOUND,
            ),
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::Unavailable => {
                s3_unavailable_status_to_response(&status, state.config.cross_region_routing_policy)
            }
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn post_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(post_bucket(State(state), Path(bucket), Query(q), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .expect("authenticated post object path supplied claims");

    if q.contains_key("uploads") {
        return initiate_multipart_upload(state, claims, bucket, key).await;
    }

    if let Some(upload_id) = q.get("uploadId") {
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap_or_default();
        return complete_multipart_upload(state, claims, bucket, key, upload_id, bytes).await;
    }

    s3_error(
        "InvalidArgument",
        "Unsupported POST object operation",
        axum::http::StatusCode::BAD_REQUEST,
    )
}

async fn initiate_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
) -> Response {
    match state
        .object_manager
        .initiate_multipart_upload(claims.tenant_id, &bucket, &key, &claims.scopes)
        .await
    {
        Ok(result) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                result.upload_id
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn upload_part(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    part_number: i32,
    body_stream: impl Stream<Item = Result<Vec<u8>, tonic::Status>> + Unpin,
) -> Response {
    match state
        .object_manager
        .upload_part(
            claims.tenant_id,
            &bucket,
            &key,
            upload_id,
            part_number,
            &claims.scopes,
            body_stream,
        )
        .await
    {
        Ok(result) => Response::builder()
            .status(200)
            .header("ETag", format!("\"{}\"", result.etag))
            .body(Body::empty())
            .unwrap(),
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn complete_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    body: axum::body::Bytes,
) -> Response {
    let completed = match quick_xml::de::from_reader::<_, CompleteMultipartUploadXml>(&body[..]) {
        Ok(completed) => completed,
        Err(error) => {
            return s3_error(
                "MalformedXML",
                &format!("Invalid CompleteMultipartUpload body: {}", error),
                axum::http::StatusCode::BAD_REQUEST,
            );
        }
    };
    let parts = completed
        .parts
        .into_iter()
        .map(|part| anvil_core::object_manager::CompleteMultipartPart {
            part_number: part.part_number,
            etag: part.etag,
        })
        .collect();

    match state
        .object_manager
        .complete_multipart_upload(
            claims.tenant_id,
            &bucket,
            &key,
            upload_id,
            parts,
            &claims.scopes,
        )
        .await
    {
        Ok(object) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Location>/{}/{}</Location>\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>\"{}\"</ETag>\n</CompleteMultipartUploadResult>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                xml_escape(&bucket),
                xml_escape(&key),
                object.etag
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn copy_object(
    state: AppState,
    claims: Claims,
    destination_bucket: String,
    destination_key: String,
    copy_source: String,
    headers: &axum::http::HeaderMap,
) -> Response {
    let (source_bucket, source_key, source_version_id) = match parse_copy_source(&copy_source) {
        Ok(source) => source,
        Err(response) => return response,
    };

    let source_object = match state
        .object_manager
        .head_object(
            Some(claims.clone()),
            &source_bucket,
            &source_key,
            source_version_id,
        )
        .await
    {
        Ok(source) => source,
        Err(status) => {
            return copy_status_to_response(
                status,
                "NoSuchKey",
                state.config.cross_region_routing_policy,
            );
        }
    };

    if let Some(response) =
        evaluate_copy_source_preconditions(headers, &source_object.etag, source_object.created_at)
    {
        return response;
    }

    match state
        .object_manager
        .copy_object(
            claims,
            &source_bucket,
            &source_key,
            source_version_id,
            &destination_bucket,
            &destination_key,
        )
        .await
    {
        Ok(object) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\n  <LastModified>{}</LastModified>\n  <ETag>\"{}\"</ETag>\n</CopyObjectResult>\n",
                object.created_at.to_rfc3339(),
                object.etag
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => copy_status_to_response(
            status,
            "NoSuchBucket",
            state.config.cross_region_routing_policy,
        ),
    }
}

fn parse_copy_source(value: &str) -> Result<(String, String, Option<uuid::Uuid>), Response> {
    let value = value.trim_start_matches('/');
    let (path, query) = value.split_once('?').unwrap_or((value, ""));
    let Some((bucket, key)) = path.split_once('/') else {
        return Err(s3_error(
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            axum::http::StatusCode::BAD_REQUEST,
        ));
    };
    let bucket = percent_decode_path_component(bucket);
    let key = percent_decode_path_component(key);
    if bucket.is_empty() || key.is_empty() {
        return Err(s3_error(
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            axum::http::StatusCode::BAD_REQUEST,
        ));
    }
    let version_id = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(name, value)| {
            (name == "versionId" || name == "version-id")
                .then(|| percent_decode_query_component(value))
        })
        .filter(|value| !value.is_empty())
        .map(|value| {
            uuid::Uuid::parse_str(&value).map_err(|_| {
                s3_error(
                    "InvalidArgument",
                    "Invalid source versionId",
                    axum::http::StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()?;

    Ok((bucket, key, version_id))
}

fn percent_decode_path_component(value: &str) -> String {
    percent_decode(value.as_bytes())
}

fn copy_status_to_response(
    status: tonic::Status,
    not_found_code: &str,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::FailedPrecondition => {
            if let Some(response) =
                s3_remote_bucket_response_from_status(&status, cross_region_policy)
            {
                return response;
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => s3_error(
            not_found_code,
            status.message(),
            axum::http::StatusCode::NOT_FOUND,
        ),
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn delete_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(delete_bucket(State(state), Path(bucket), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let checked_route = match s3_checked_route(&state, s3_host_route(&req), Some(claims)).await {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route
        .claims
        .clone()
        .expect("authenticated delete object path supplied claims");

    if let Some(upload_id) = q.get("uploadId") {
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return abort_multipart_upload(state, claims, bucket, key, upload_id).await;
    }

    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };

    if let Some(response) = s3_object_proxy_response_if_needed(
        &state,
        &checked_route,
        Some(&claims),
        &bucket,
        &key,
        "DELETE",
        req.headers(),
        req.uri(),
        version_id,
        None,
    )
    .await
    {
        return response;
    }

    if let Some(version_id) = version_id {
        return match state
            .object_manager
            .delete_object_version(claims.tenant_id, &bucket, &key, version_id, &claims.scopes)
            .await
        {
            Ok(deleted) => {
                let mut builder = Response::builder()
                    .status(axum::http::StatusCode::NO_CONTENT)
                    .header("x-amz-version-id", deleted.version_id.to_string());
                if deleted.deleted_at.is_some() {
                    builder = builder.header("x-amz-delete-marker", "true");
                }
                builder.body(Body::empty()).unwrap()
            }
            Err(status) => {
                s3_delete_status_to_response(status, state.config.cross_region_routing_policy)
            }
        };
    }

    match state
        .object_manager
        .delete_object(claims.tenant_id, &bucket, &key, &claims.scopes)
        .await
    {
        Ok(delete_marker) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", delete_marker.version_id.to_string())
            .body(Body::empty())
            .unwrap(),
        Err(status) => {
            s3_delete_status_to_response(status, state.config.cross_region_routing_policy)
        }
    }
}

fn s3_delete_status_to_response(
    status: tonic::Status,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::FailedPrecondition => {
            if let Some(response) =
                s3_remote_bucket_response_from_status(&status, cross_region_policy)
            {
                return response;
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn abort_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
) -> Response {
    match state
        .object_manager
        .abort_multipart_upload(claims.tenant_id, &bucket, &key, upload_id, &claims.scopes)
        .await
    {
        Ok(_) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(status) => s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchUpload",
            state.config.cross_region_routing_policy,
        ),
    }
}

async fn head_object(
    State(state): State<AppState>,
    Path((mut bucket, mut key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    if let Some(bucket) = s3_routed_bucket_without_key(&req) {
        return Box::pin(head_bucket(State(state), Path(bucket), req)).await;
    }
    (bucket, key) = s3_routed_bucket_key(&req, bucket, key);

    let checked_route = match s3_checked_route(
        &state,
        s3_host_route(&req),
        req.extensions().get::<Claims>().cloned(),
    )
    .await
    {
        Ok(checked_route) => checked_route,
        Err(response) => return response,
    };
    let claims = checked_route.claims.clone();
    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };
    if is_link_metadata_request(req.headers()) {
        return head_object_link_metadata_response(
            state,
            claims,
            checked_route.tenant_id,
            &bucket,
            &key,
            version_id,
        )
        .await;
    }

    if let Some(response) = s3_object_proxy_response_if_needed(
        &state,
        &checked_route,
        checked_route.claims.as_ref(),
        &bucket,
        &key,
        "HEAD",
        req.headers(),
        req.uri(),
        version_id,
        None,
    )
    .await
    {
        return response;
    }

    match state
        .object_manager
        .head_object_with_link_mode_for_tenant(
            claims,
            checked_route.tenant_id,
            &bucket,
            &key,
            version_id,
            ObjectLinkReadMode::Follow,
        )
        .await
    {
        Ok(result) => {
            let anvil_core::object_manager::ObjectHeadResult {
                object,
                followed_link,
            } = result;
            if let Some(response) =
                evaluate_object_preconditions(req.headers(), &object.etag, object.created_at)
            {
                return response;
            }
            let builder = Response::builder()
                .status(200)
                .header(
                    "Content-Type",
                    object.content_type.clone().unwrap_or_default(),
                )
                .header("Content-Length", object.size)
                .header("ETag", object.etag)
                .header("Accept-Ranges", "bytes")
                .header("x-amz-version-id", object.version_id.to_string());
            let builder = add_followed_link_headers(builder, followed_link.as_ref());
            add_s3_user_metadata_headers(builder, object.user_meta.as_ref())
                .body(Body::empty())
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if let Some(response) = s3_remote_bucket_response_from_status(
                    &status,
                    state.config.cross_region_routing_policy,
                ) {
                    return response;
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchKey",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::Unavailable => {
                s3_unavailable_status_to_response(&status, state.config.cross_region_routing_policy)
            }
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    fn len(self) -> u64 {
        self.end - self.start + 1
    }
}

fn evaluate_object_preconditions(
    headers: &axum::http::HeaderMap,
    current_etag: &str,
    last_modified: chrono::DateTime<chrono::Utc>,
) -> Option<Response> {
    if let Some(value) = headers.get(axum::http::header::IF_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if !etag_condition_matches(value, current_etag) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_UNMODIFIED_SINCE) {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) > condition_time {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_NONE_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if etag_condition_matches(value, current_etag) {
            return Some(not_modified_response(current_etag));
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_MODIFIED_SINCE) {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) <= condition_time {
            return Some(not_modified_response(current_etag));
        }
    }
    None
}

fn request_has_write_etag_preconditions(headers: &axum::http::HeaderMap) -> bool {
    headers.contains_key(axum::http::header::IF_MATCH)
        || headers.contains_key(axum::http::header::IF_NONE_MATCH)
}

fn evaluate_write_etag_preconditions(
    headers: &axum::http::HeaderMap,
    current_etag: Option<&str>,
) -> Option<Response> {
    if let Some(value) = headers.get(axum::http::header::IF_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if !current_etag.is_some_and(|etag| etag_condition_matches(value, etag)) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_NONE_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if current_etag.is_some_and(|etag| etag_condition_matches(value, etag)) {
            return Some(precondition_failed_response());
        }
    }
    None
}

fn evaluate_copy_source_preconditions(
    headers: &axum::http::HeaderMap,
    current_etag: &str,
    last_modified: chrono::DateTime<chrono::Utc>,
) -> Option<Response> {
    if let Some(value) = headers.get("x-amz-copy-source-if-match") {
        let value = value.to_str().unwrap_or_default();
        if !etag_condition_matches(value, current_etag) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get("x-amz-copy-source-if-unmodified-since") {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) > condition_time {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get("x-amz-copy-source-if-none-match") {
        let value = value.to_str().unwrap_or_default();
        if etag_condition_matches(value, current_etag) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get("x-amz-copy-source-if-modified-since") {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) <= condition_time {
            return Some(precondition_failed_response());
        }
    }
    None
}

fn etag_condition_matches(header_value: &str, current_etag: &str) -> bool {
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || normalize_etag(candidate) == current_etag)
}

fn normalize_etag(value: &str) -> &str {
    value
        .strip_prefix("W/")
        .unwrap_or(value)
        .trim()
        .trim_matches('"')
}

fn precondition_failed_response() -> Response {
    s3_error(
        "PreconditionFailed",
        "At least one precondition did not hold",
        axum::http::StatusCode::PRECONDITION_FAILED,
    )
}

fn not_modified_response(current_etag: &str) -> Response {
    Response::builder()
        .status(axum::http::StatusCode::NOT_MODIFIED)
        .header("ETag", current_etag)
        .body(Body::empty())
        .unwrap()
}

fn object_last_modified_time(value: chrono::DateTime<chrono::Utc>) -> std::time::SystemTime {
    let seconds = value.timestamp();
    if seconds <= 0 {
        std::time::UNIX_EPOCH
    } else {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds as u64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestedByteRange {
    FromStart { start: u64, end: Option<u64> },
    Suffix { len: u64 },
}

impl RequestedByteRange {
    fn resolve(self, object_size: u64) -> Result<ByteRange, Response> {
        if object_size == 0 {
            return Err(invalid_range_response(object_size));
        }
        match self {
            Self::FromStart { start, end } => {
                if start >= object_size {
                    return Err(invalid_range_response(object_size));
                }
                let end = end.unwrap_or(object_size - 1).min(object_size - 1);
                if end < start {
                    return Err(invalid_range_response(object_size));
                }
                Ok(ByteRange { start, end })
            }
            Self::Suffix { len } => {
                if len == 0 {
                    return Err(invalid_range_response(object_size));
                }
                let len = len.min(object_size);
                Ok(ByteRange {
                    start: object_size - len,
                    end: object_size - 1,
                })
            }
        }
    }
}

fn parse_http_range(
    headers: &axum::http::HeaderMap,
    object_size: Option<u64>,
) -> Result<Option<RequestedByteRange>, Response> {
    let Some(value) = headers.get(axum::http::header::RANGE) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        s3_error(
            "InvalidRange",
            "Invalid Range header",
            axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
        )
    })?;
    if value.contains(',') {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    }
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    };
    let Some((start, end)) = spec.split_once('-') else {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    };
    if start.is_empty() && end.is_empty() {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    }
    let requested = if start.is_empty() {
        RequestedByteRange::Suffix {
            len: end
                .parse()
                .map_err(|_| invalid_range_response(object_size.unwrap_or(0)))?,
        }
    } else {
        RequestedByteRange::FromStart {
            start: start
                .parse()
                .map_err(|_| invalid_range_response(object_size.unwrap_or(0)))?,
            end: if end.is_empty() {
                None
            } else {
                Some(
                    end.parse()
                        .map_err(|_| invalid_range_response(object_size.unwrap_or(0)))?,
                )
            },
        }
    };
    Ok(Some(requested))
}

fn invalid_range_response(object_size: u64) -> Response {
    let mut response = s3_error(
        "InvalidRange",
        "Invalid Range header",
        axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
    );
    response.headers_mut().insert(
        axum::http::header::CONTENT_RANGE,
        format!("bytes */{}", object_size).parse().unwrap(),
    );
    response
}

fn slice_stream_by_range(
    mut stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + 'static>>,
    range: ByteRange,
) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + 'static>> {
    Box::pin(async_stream::try_stream! {
        let mut offset = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let chunk_len = chunk.len() as u64;
            if chunk_len == 0 {
                continue;
            }
            let chunk_start = offset;
            let chunk_end = offset + chunk_len - 1;
            offset += chunk_len;

            if chunk_end < range.start {
                continue;
            }
            if chunk_start > range.end {
                break;
            }

            let from = range.start.saturating_sub(chunk_start) as usize;
            let to_exclusive = (range.end.min(chunk_end) - chunk_start + 1) as usize;
            yield chunk[from..to_exclusive].to_vec();
        }
    })
}

fn parse_s3_version_id(q: &HashMap<String, String>) -> Result<Option<uuid::Uuid>, Response> {
    q.get("versionId")
        .or_else(|| q.get("version-id"))
        .filter(|value| !value.is_empty())
        .map(|value| {
            uuid::Uuid::parse_str(value).map_err(|_| {
                s3_error(
                    "InvalidArgument",
                    "Invalid versionId",
                    axum::http::StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use anvil_core::{
        mesh_directory::{
            self, BucketId, BucketLocatorDescriptor, BucketName, CellId, MeshControlWriteAuthority,
            MeshId, RegionName, RoutingRecordFamily, TenantId,
        },
        partition_fence::{
            PartitionRecoveryAcquire, acquire_partition_recovery, publish_partition_ready,
        },
    };
    use futures_util::TryStreamExt;
    use tempfile::tempdir;

    fn request(uri: &str) -> Request {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn host_request(host: &str, remote: &str, forwarded_host: Option<&str>) -> Request {
        let mut builder = Request::builder().uri("/object.txt").header("host", host);
        if let Some(forwarded_host) = forwarded_host {
            builder = builder.header("x-forwarded-host", forwarded_host);
        }
        let mut req = builder.body(Body::empty()).unwrap();
        req.extensions_mut().insert(ConnectInfo(SocketAddr::new(
            remote.parse().unwrap(),
            41_000,
        )));
        req
    }

    fn routing_config_with_trusted_ranges(ranges: &[&str]) -> anvil_core::config::Config {
        anvil_core::config::Config {
            trusted_proxy_source_ranges: ranges.iter().map(|range| range.to_string()).collect(),
            ..anvil_core::config::Config::default()
        }
    }

    fn routing_config_with_policy(
        storage_path: &std::path::Path,
        policy: CrossRegionRoutingPolicy,
    ) -> anvil_core::config::Config {
        anvil_core::config::Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "us-east-1".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            cross_region_routing_policy: policy,
            ..anvil_core::config::Config::default()
        }
    }

    async fn seeded_remote_bucket_route(
        policy: CrossRegionRoutingPolicy,
    ) -> (tempfile::TempDir, AppState, Claims, ObjectRoute) {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let state = AppState::new(routing_config_with_policy(&storage_path, policy), None)
            .await
            .unwrap();
        let tenant = state
            .persistence
            .create_tenant("acme", "remote-bucket-test")
            .await
            .unwrap();
        state
            .persistence
            .create_bucket(tenant.id, "releases", "eu-west-1")
            .await
            .unwrap();
        let claims = Claims {
            sub: "test-app".to_string(),
            exp: usize::MAX,
            scopes: Vec::new(),
            tenant_id: tenant.id,
            jti: None,
        };
        let route = ObjectRoute {
            tenant: "acme".to_string(),
            bucket: "releases".to_string(),
            region: "us-east-1".to_string(),
            key: "object.txt".to_string(),
            source: RouteSource::PathStyle,
        };
        (temp, state, claims, route)
    }

    async fn seed_active_proxy_node(state: &AppState, region: &str, endpoint: &str) {
        use anvil_core::mesh_lifecycle::{
            CreateRegionDescriptor, LifecycleState, NodeCapability, RegisterCellDescriptor,
            RegisterNodeDescriptor,
        };

        state
            .persistence
            .create_region_descriptor(CreateRegionDescriptor {
                mesh_id: "default".to_string(),
                region: region.to_string(),
                public_base_url: format!("https://{region}.anvil-storage.test"),
                virtual_host_suffix: format!("{region}.anvil-storage.test"),
                placement_weight: 100,
                default_cell: Some("default".to_string()),
            })
            .await
            .unwrap();
        state
            .persistence
            .register_cell_descriptor(RegisterCellDescriptor {
                mesh_id: "default".to_string(),
                region: region.to_string(),
                cell_id: "default".to_string(),
                placement_weight: 100,
            })
            .await
            .unwrap();
        state
            .persistence
            .transition_cell_descriptor(region, "default", 1, LifecycleState::Active)
            .await
            .unwrap();
        state
            .persistence
            .register_node_descriptor(RegisterNodeDescriptor {
                mesh_id: "default".to_string(),
                node_id: "remote-object-node".to_string(),
                region: region.to_string(),
                cell_id: "default".to_string(),
                libp2p_peer_id: "remote-peer".to_string(),
                public_api_addr: endpoint.to_string(),
                public_cluster_addrs: Vec::new(),
                capabilities: vec![NodeCapability::Object],
            })
            .await
            .unwrap();
        state
            .persistence
            .transition_node_descriptor("remote-object-node", 1, LifecycleState::Active, None)
            .await
            .unwrap();
    }

    async fn seeded_remote_bucket_locator_only(
        policy: CrossRegionRoutingPolicy,
    ) -> (tempfile::TempDir, AppState, Claims, String) {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let state = AppState::new(routing_config_with_policy(&storage_path, policy), None)
            .await
            .unwrap();
        let tenant = state
            .persistence
            .create_tenant("acme", "remote-locator-only-test")
            .await
            .unwrap();
        let bucket_name = BucketName::canonicalize("releases").unwrap();
        let object_prefix = format!("objects/{}/{}/", tenant.id, bucket_name.as_str());
        let locator = BucketLocatorDescriptor::active(
            MeshId::new("default").unwrap(),
            TenantId::new(tenant.id.to_string()).unwrap(),
            bucket_name.clone(),
            BucketId::new("remote-bucket-id").unwrap(),
            RegionName::new("eu-west-1").unwrap(),
            CellId::new("default").unwrap(),
            "regional-primary",
            object_prefix,
            "2026-07-02T00:00:00Z",
        )
        .unwrap();
        let partition = locator.partition();
        let control_partition_id = mesh_directory::control_partition_id(
            RoutingRecordFamily::BucketLocator.stream_family(),
            &partition,
        );
        let signing_key = hex::decode(&state.config.anvil_secret_encryption_key).unwrap();
        let recovering = acquire_partition_recovery(
            &state.storage,
            PartitionRecoveryAcquire {
                partition_family: mesh_directory::CONTROL_PARTITION_FAMILY.to_string(),
                partition_id: control_partition_id,
                owner_node_id: state.config.node_id.clone(),
                recovered_through_sequence: 0,
                recovered_manifest_hash: hex::encode([0; 32]),
                now_nanos: 1,
            },
            &signing_key,
        )
        .await
        .unwrap();
        let ready = publish_partition_ready(
            &state.storage,
            &recovering.partition_family,
            &recovering.partition_id,
            &state.config.node_id,
            recovering.fence_token,
            0,
            &hex::encode([0; 32]),
            2,
            &signing_key,
        )
        .await
        .unwrap();
        mesh_directory::write_bucket_locator(
            &state.storage,
            &locator,
            MeshControlWriteAuthority {
                permit: &ready.write_permit().unwrap(),
                signing_key: &signing_key,
            },
        )
        .await
        .unwrap();

        let claims = Claims {
            sub: "test-app".to_string(),
            exp: usize::MAX,
            scopes: vec!["*|*".to_string()],
            tenant_id: tenant.id,
            jti: None,
        };
        (temp, state, claims, bucket_name.as_str().to_string())
    }

    async fn seeded_local_object_link() -> (tempfile::TempDir, AppState, Claims, String, String) {
        let temp = tempdir().unwrap();
        let storage_path = temp.path().join("storage");
        let state = AppState::new(
            routing_config_with_policy(&storage_path, CrossRegionRoutingPolicy::RedirectPreferred),
            None,
        )
        .await
        .unwrap();
        let tenant = state
            .persistence
            .create_tenant("acme", "local-link-test")
            .await
            .unwrap();
        let bucket = state
            .persistence
            .create_bucket(tenant.id, "releases", "us-east-1")
            .await
            .unwrap();
        state
            .persistence
            .create_object(
                tenant.id,
                bucket.id,
                "versions/app-v1.bin",
                "payload-hash-v1",
                14,
                "etag-v1",
                Some("application/octet-stream"),
                None,
                None,
                Some(b"linked payload".to_vec()),
            )
            .await
            .unwrap();
        state
            .persistence
            .put_object_link(object_links::PutObjectLinkRequest {
                tenant_id: tenant.id,
                bucket_id: bucket.id,
                link_key: "latest.bin".to_string(),
                target_key: "versions/app-v1.bin".to_string(),
                target_version: None,
                resolution: object_links::ObjectLinkResolution::Follow,
                expected_generation: None,
                create_only: true,
                allow_dangling: false,
                idempotency_key: "local-link".to_string(),
                created_by: "principal:test".to_string(),
            })
            .await
            .unwrap();
        let claims = Claims {
            sub: "test-app".to_string(),
            exp: usize::MAX,
            scopes: vec!["*|*".to_string()],
            tenant_id: tenant.id,
            jti: None,
        };
        (temp, state, claims, bucket.name, "latest.bin".to_string())
    }

    async fn response_xml(response: Response) -> String {
        let body = axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap();
        std::str::from_utf8(&body).unwrap().to_string()
    }

    async fn response_body(response: Response) -> Vec<u8> {
        axum::body::to_bytes(response.into_body(), 4096)
            .await
            .unwrap()
            .to_vec()
    }

    fn request_with_copy_source(uri: &str, copy_source: &str) -> Request {
        Request::builder()
            .uri(uri)
            .header("x-amz-copy-source", copy_source)
            .body(Body::empty())
            .unwrap()
    }

    fn range_headers(value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::RANGE, value.parse().unwrap());
        headers
    }

    fn etag_headers(name: axum::http::header::HeaderName, value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(name, value.parse().unwrap());
        headers
    }

    fn x_amz_headers(name: &'static str, value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static(name),
            value.parse().unwrap(),
        );
        headers
    }

    fn http_date_headers(
        name: axum::http::header::HeaderName,
        value: std::time::SystemTime,
    ) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(name, httpdate::fmt_http_date(value).parse().unwrap());
        headers
    }

    #[test]
    fn s3_host_routing_accepts_forwarded_host_only_from_trusted_ranges() {
        let config = routing_config_with_trusted_ranges(&["127.0.0.1/32"]);
        let req = host_request(
            "internal.anvil-storage.test",
            "127.0.0.1",
            Some("Bucket.Default.Test-Region-1.Anvil-Storage.Test"),
        );

        let host = request_host(&req, &config).expect("effective host");

        assert_eq!(
            host.as_deref(),
            Some("bucket.default.test-region-1.anvil-storage.test")
        );
    }

    #[test]
    fn s3_host_routing_ignores_untrusted_forwarded_host() {
        let config = routing_config_with_trusted_ranges(&["10.0.0.0/8"]);
        let req = host_request(
            "internal.anvil-storage.test",
            "127.0.0.1",
            Some("bucket.default.test-region-1.anvil-storage.test"),
        );

        let host = request_host(&req, &config).expect("effective host");

        assert_eq!(host.as_deref(), Some("internal.anvil-storage.test"));
    }

    #[test]
    fn s3_host_routing_rejects_ambiguous_forwarded_host_chains() {
        let config = routing_config_with_trusted_ranges(&["127.0.0.1/32"]);
        let req = host_request(
            "internal.anvil-storage.test",
            "127.0.0.1",
            Some("one.example.test, two.example.test"),
        );

        let err = request_host(&req, &config).unwrap_err();

        assert_eq!(err, RoutingError::AmbiguousForwardedHost);
    }

    #[tokio::test]
    async fn s3_error_responses_include_request_id_in_header_and_xml() {
        let response = s3_error(
            "AccessDenied",
            "denied <unsafe>",
            axum::http::StatusCode::FORBIDDEN,
        );
        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
        let request_id = response
            .headers()
            .get("x-amz-request-id")
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(request_id.len(), 32);
        assert!(request_id.bytes().all(|byte| byte.is_ascii_hexdigit()));

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(xml.contains("<Message>denied &lt;unsafe&gt;</Message>"));
        assert!(xml.contains(&format!("<RequestId>{request_id}</RequestId>")));
    }

    #[tokio::test]
    async fn s3_not_found_errors_do_not_leak_existence_to_unauthenticated_callers() {
        let unauthenticated = s3_status_to_response_for_auth(
            tonic::Status::not_found("missing protected object"),
            false,
            "NoSuchKey",
            CrossRegionRoutingPolicy::RedirectPreferred,
        );
        assert_eq!(unauthenticated.status(), axum::http::StatusCode::FORBIDDEN);
        assert!(unauthenticated.headers().contains_key("x-amz-request-id"));
        let body = axum::body::to_bytes(unauthenticated.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>AccessDenied</Code>"));
        assert!(!xml.contains("NoSuchKey"));

        let authenticated = s3_status_to_response_for_auth(
            tonic::Status::not_found("missing visible object"),
            true,
            "NoSuchKey",
            CrossRegionRoutingPolicy::RedirectPreferred,
        );
        assert_eq!(authenticated.status(), axum::http::StatusCode::NOT_FOUND);
        let body = axum::body::to_bytes(authenticated.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>NoSuchKey</Code>"));
    }

    #[tokio::test]
    async fn remote_bucket_locator_local_only_rejects_cross_region_route() {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::LocalOnly).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>InvalidRequest</Code>"));
        assert!(xml.contains("local_only"));
    }

    #[tokio::test]
    async fn remote_bucket_locator_redirect_preferred_returns_s3_wrong_region_response() {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::RedirectPreferred).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        assert!(response.headers().contains_key("x-amz-request-id"));
        assert!(!response.headers().contains_key("x-anvil-bucket-region"));
        assert!(
            !response
                .headers()
                .contains_key("x-anvil-cross-region-action")
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>PermanentRedirect</Code>"));
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
        assert!(xml.contains("<RequestId>"));
    }

    #[tokio::test]
    async fn remote_bucket_locator_proxy_preferred_redirects_when_proxy_is_absent() {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::ProxyPreferred).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>PermanentRedirect</Code>"));
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
    }

    #[tokio::test]
    async fn remote_bucket_locator_proxy_required_reports_unavailable_without_proxy() {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::ProxyRequired).await;

        let response = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap_err();

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
        assert!(xml.contains("no eligible proxy target is available"));
    }

    #[tokio::test]
    async fn remote_bucket_locator_proxy_required_selects_active_remote_object_node() {
        let (_temp, state, claims, route) =
            seeded_remote_bucket_route(CrossRegionRoutingPolicy::ProxyRequired).await;
        seed_active_proxy_node(&state, "eu-west-1", "127.0.0.1:50091").await;

        let checked = s3_checked_route(&state, Some(route), Some(claims))
            .await
            .unwrap();
        let target = checked
            .remote_bucket
            .expect("proxy_required with active object node must select proxy target");

        assert_eq!(target.region, "eu-west-1");
        assert_eq!(target.endpoint, "http://127.0.0.1:50091");
        assert!(target.bucket_locator_generation > 0);
    }

    #[tokio::test]
    async fn remote_bucket_status_metadata_maps_to_s3_without_private_headers() {
        let mut status =
            tonic::Status::unavailable("Bucket is in region eu-west-1; proxy details hidden");
        status
            .metadata_mut()
            .insert("x-anvil-bucket-region", "eu-west-1".parse().unwrap());
        status.metadata_mut().insert(
            "x-anvil-cross-region-action",
            "proxy_unavailable".parse().unwrap(),
        );

        let response = s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            CrossRegionRoutingPolicy::ProxyRequired,
        );

        assert_eq!(
            response.status(),
            axum::http::StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        assert!(!response.headers().contains_key("x-anvil-bucket-region"));
        assert!(
            !response
                .headers()
                .contains_key("x-anvil-cross-region-action")
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>ServiceUnavailable</Code>"));
        assert!(!xml.contains("proxy details hidden"));
    }

    #[tokio::test]
    async fn remote_bucket_message_parser_strips_internal_suffix_for_redirects() {
        let status =
            tonic::Status::failed_precondition("Bucket is in region eu-west-1; redirect required");

        let response = s3_status_to_response_for_auth(
            status,
            true,
            "NoSuchBucket",
            CrossRegionRoutingPolicy::RedirectPreferred,
        );

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
        assert!(!xml.contains("redirect required"));
    }

    #[tokio::test]
    async fn head_bucket_uses_remote_locator_before_local_bucket_metadata() {
        let (_temp, state, claims, bucket) =
            seeded_remote_bucket_locator_only(CrossRegionRoutingPolicy::RedirectPreferred).await;
        let mut req = Request::builder()
            .uri(format!("/{bucket}"))
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(claims);

        let response = head_bucket(State(state), Path(bucket), req).await;

        assert_eq!(response.status(), axum::http::StatusCode::MOVED_PERMANENTLY);
        assert_eq!(
            response.headers().get("x-amz-bucket-region").unwrap(),
            "eu-west-1"
        );
        let xml = response_xml(response).await;
        assert!(xml.contains("<Code>PermanentRedirect</Code>"));
        assert!(xml.contains("<BucketRegion>eu-west-1</BucketRegion>"));
    }

    #[tokio::test]
    async fn object_link_get_and_head_follow_by_default_with_link_headers() {
        let (_temp, state, claims, bucket, link_key) = seeded_local_object_link().await;
        let mut get_req = Request::builder()
            .uri(format!("/{bucket}/{link_key}"))
            .body(Body::empty())
            .unwrap();
        get_req.extensions_mut().insert(claims.clone());

        let get_response = get_object(
            State(state.clone()),
            Path((bucket.clone(), link_key.clone())),
            Query(HashMap::new()),
            get_req,
        )
        .await;

        assert_eq!(get_response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            get_response.headers().get("x-anvil-object-kind").unwrap(),
            "link"
        );
        assert_eq!(
            get_response.headers().get("x-anvil-link-key").unwrap(),
            "latest.bin"
        );
        assert_eq!(
            get_response
                .headers()
                .get("x-anvil-link-generation")
                .unwrap(),
            "1"
        );
        assert!(
            get_response
                .headers()
                .get("ETag")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("link-follow-")
        );
        assert_eq!(response_body(get_response).await, b"linked payload");

        let mut head_req = Request::builder()
            .method(axum::http::Method::HEAD)
            .uri(format!("/{bucket}/{link_key}"))
            .body(Body::empty())
            .unwrap();
        head_req.extensions_mut().insert(claims);

        let head_response = head_object(
            State(state),
            Path((bucket, link_key)),
            Query(HashMap::new()),
            head_req,
        )
        .await;

        assert_eq!(head_response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            head_response.headers().get("x-anvil-object-kind").unwrap(),
            "link"
        );
        assert_eq!(head_response.headers().get("Content-Length").unwrap(), "14");
        assert!(response_body(head_response).await.is_empty());
    }

    #[tokio::test]
    async fn object_link_metadata_mode_returns_descriptor_json() {
        let (_temp, state, claims, bucket, link_key) = seeded_local_object_link().await;
        let mut req = Request::builder()
            .uri(format!("/{bucket}/{link_key}"))
            .header("x-anvil-link-mode", "metadata")
            .body(Body::empty())
            .unwrap();
        req.extensions_mut().insert(claims);

        let response = get_object(
            State(state),
            Path((bucket, link_key)),
            Query(HashMap::new()),
            req,
        )
        .await;

        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers().get("Content-Type").unwrap(),
            object_links::LINK_METADATA_CONTENT_TYPE
        );
        assert_eq!(
            response.headers().get("x-anvil-object-kind").unwrap(),
            "link"
        );
        let descriptor: serde_json::Value =
            serde_json::from_slice(&response_body(response).await).unwrap();
        assert_eq!(descriptor["schema"], "anvil.object_link.v1");
        assert_eq!(descriptor["link_key"], "latest.bin");
        assert_eq!(descriptor["target_key"], "versions/app-v1.bin");
        assert_eq!(descriptor["resolution"], "follow");
    }

    #[test]
    fn reserved_namespace_guard_detects_object_keys() {
        assert!(request_targets_reserved_namespace(&request(
            "/bucket/_anvil/authz/tuples"
        )));
        assert!(request_targets_reserved_namespace(&request(
            "/bucket/_anvil/personaldb/group"
        )));
        assert!(!request_targets_reserved_namespace(&request(
            "/bucket/customer/_anvil/authz/visible"
        )));
    }

    #[test]
    fn reserved_namespace_guard_detects_list_prefixes() {
        assert!(request_targets_reserved_namespace(&request(
            "/bucket?list-type=2&prefix=_anvil%2Fauthz%2F"
        )));
        assert!(request_targets_reserved_namespace(&request(
            "/bucket?prefix=_anvil/personaldb/"
        )));
        assert!(!request_targets_reserved_namespace(&request(
            "/bucket?prefix=customer%2F_anvil%2Fauthz%2F"
        )));
    }

    #[test]
    fn reserved_namespace_guard_detects_copy_source_keys() {
        assert!(request_targets_reserved_namespace(
            &request_with_copy_source("/bucket/destination", "source/_anvil/authz/tuples")
        ));
        assert!(request_targets_reserved_namespace(
            &request_with_copy_source("/bucket/destination", "/source/_anvil%2Fauthz%2Ftuples")
        ));
        assert!(!request_targets_reserved_namespace(
            &request_with_copy_source(
                "/bucket/destination",
                "source/customer/_anvil/authz/visible"
            )
        ));
        assert!(!request_targets_reserved_namespace(
            &request_with_copy_source("/bucket/destination", "malformed-copy-source")
        ));
    }

    #[test]
    fn range_parser_resolves_standard_and_suffix_ranges() {
        let standard = parse_http_range(&range_headers("bytes=2-5"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .unwrap();
        assert_eq!(standard, ByteRange { start: 2, end: 5 });

        let open_ended = parse_http_range(&range_headers("bytes=7-"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .unwrap();
        assert_eq!(open_ended, ByteRange { start: 7, end: 9 });

        let suffix = parse_http_range(&range_headers("bytes=-4"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .unwrap();
        assert_eq!(suffix, ByteRange { start: 6, end: 9 });
    }

    #[test]
    fn range_parser_rejects_multi_ranges_and_unsatisfied_ranges() {
        assert!(parse_http_range(&range_headers("bytes=0-1,4-5"), Some(10)).is_err());
        assert!(
            parse_http_range(&range_headers("bytes=20-30"), Some(10))
                .unwrap()
                .unwrap()
                .resolve(10)
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_range_error_includes_request_id_and_content_range() {
        let response = invalid_range_response(10);
        assert_eq!(
            response.status(),
            axum::http::StatusCode::RANGE_NOT_SATISFIABLE
        );
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::CONTENT_RANGE)
                .and_then(|value| value.to_str().ok()),
            Some("bytes */10")
        );
        let request_id = response
            .headers()
            .get("x-amz-request-id")
            .expect("S3 invalid range errors must include request id")
            .to_str()
            .unwrap()
            .to_string();
        assert_eq!(request_id.len(), 32);

        let body = axum::body::to_bytes(response.into_body(), 1024)
            .await
            .unwrap();
        let xml = std::str::from_utf8(&body).unwrap();
        assert!(xml.contains("<Code>InvalidRange</Code>"));
        assert!(xml.contains(&format!("<RequestId>{request_id}</RequestId>")));
    }

    #[test]
    fn etag_preconditions_match_strong_weak_and_list_values() {
        assert!(etag_condition_matches("\"abc\"", "abc"));
        assert!(etag_condition_matches("W/\"abc\"", "abc"));
        assert!(etag_condition_matches("\"nope\", \"abc\"", "abc"));
        assert!(etag_condition_matches("*", "abc"));
        assert!(!etag_condition_matches("\"nope\"", "abc"));
    }

    #[test]
    fn etag_preconditions_return_s3_status_responses() {
        let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
        let failed = evaluate_object_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"other\""),
            "abc",
            last_modified,
        )
        .expect("if-match mismatch should fail");
        assert_eq!(failed.status(), axum::http::StatusCode::PRECONDITION_FAILED);

        let not_modified = evaluate_object_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
            "abc",
            last_modified,
        )
        .expect("if-none-match match should return not modified");
        assert_eq!(not_modified.status(), axum::http::StatusCode::NOT_MODIFIED);

        assert!(
            evaluate_object_preconditions(
                &etag_headers(axum::http::header::IF_NONE_MATCH, "\"other\""),
                "abc",
                last_modified,
            )
            .is_none()
        );
    }

    #[test]
    fn write_etag_preconditions_require_existing_match() {
        let failed_missing = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"abc\""),
            None,
        )
        .expect("If-Match without current object should fail");
        assert_eq!(
            failed_missing.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );

        assert!(
            evaluate_write_etag_preconditions(
                &etag_headers(axum::http::header::IF_MATCH, "\"abc\""),
                Some("abc"),
            )
            .is_none()
        );

        let failed_mismatch = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"other\""),
            Some("abc"),
        )
        .expect("If-Match mismatch should fail");
        assert_eq!(
            failed_mismatch.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
    }

    #[test]
    fn write_etag_preconditions_enforce_if_none_match() {
        assert!(
            evaluate_write_etag_preconditions(
                &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
                None,
            )
            .is_none()
        );

        let failed_existing = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
            Some("abc"),
        )
        .expect("matching If-None-Match should fail writes");
        assert_eq!(
            failed_existing.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );

        let failed_star = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "*"),
            Some("abc"),
        )
        .expect("If-None-Match wildcard should fail existing object writes");
        assert_eq!(
            failed_star.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
    }

    #[test]
    fn copy_source_preconditions_return_precondition_failed() {
        let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let exact_second = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let before = exact_second - std::time::Duration::from_secs(1);
        let after = exact_second + std::time::Duration::from_secs(1);

        assert!(
            evaluate_copy_source_preconditions(
                &x_amz_headers("x-amz-copy-source-if-match", "\"abc\""),
                "abc",
                last_modified,
            )
            .is_none()
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers("x-amz-copy-source-if-match", "\"other\""),
                "abc",
                last_modified,
            )
            .expect("source If-Match mismatch should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers("x-amz-copy-source-if-none-match", "\"abc\""),
                "abc",
                last_modified,
            )
            .expect("source If-None-Match hit should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers(
                    "x-amz-copy-source-if-unmodified-since",
                    &httpdate::fmt_http_date(before),
                ),
                "abc",
                last_modified,
            )
            .expect("source If-Unmodified-Since before modification should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers(
                    "x-amz-copy-source-if-modified-since",
                    &httpdate::fmt_http_date(after),
                ),
                "abc",
                last_modified,
            )
            .expect("source If-Modified-Since after modification should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
    }

    #[test]
    fn date_preconditions_compare_against_second_precision_last_modified() {
        let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 999_000_000).unwrap();
        let exact_second = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let before = exact_second - std::time::Duration::from_secs(1);
        let after = exact_second + std::time::Duration::from_secs(1);

        let unmodified_since_before = evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_UNMODIFIED_SINCE, before),
            "abc",
            last_modified,
        )
        .expect("older if-unmodified-since should fail");
        assert_eq!(
            unmodified_since_before.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );

        assert!(
            evaluate_object_preconditions(
                &http_date_headers(axum::http::header::IF_UNMODIFIED_SINCE, exact_second),
                "abc",
                last_modified,
            )
            .is_none()
        );

        let modified_since_exact = evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, exact_second),
            "abc",
            last_modified,
        )
        .expect("equal if-modified-since should be not modified");
        assert_eq!(
            modified_since_exact.status(),
            axum::http::StatusCode::NOT_MODIFIED
        );

        assert!(
            evaluate_object_preconditions(
                &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, before),
                "abc",
                last_modified,
            )
            .is_none()
        );
        assert_eq!(
            evaluate_object_preconditions(
                &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, after),
                "abc",
                last_modified,
            )
            .expect("future if-modified-since should be not modified")
            .status(),
            axum::http::StatusCode::NOT_MODIFIED
        );
    }

    #[tokio::test]
    async fn range_stream_slices_across_chunk_boundaries() {
        let stream = Box::pin(futures_util::stream::iter(vec![
            Ok(b"abc".to_vec()),
            Ok(b"defg".to_vec()),
            Ok(b"hij".to_vec()),
        ]));
        let body = slice_stream_by_range(stream, ByteRange { start: 2, end: 7 })
            .try_concat()
            .await
            .unwrap();
        assert_eq!(body, b"cdefgh");
    }

    #[test]
    fn copy_source_parser_accepts_encoded_bucket_key_and_version() {
        let (bucket, key, version_id) = parse_copy_source(
            "/source-bucket/path%20with%20space/file.txt?versionId=550e8400-e29b-41d4-a716-446655440000",
        )
        .unwrap();
        assert_eq!(bucket, "source-bucket");
        assert_eq!(key, "path with space/file.txt");
        assert_eq!(
            version_id.unwrap(),
            uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
        );
    }

    #[test]
    fn copy_source_parser_rejects_missing_key() {
        assert!(parse_copy_source("/source-bucket").is_err());
    }
}
