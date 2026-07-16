use super::*;

#[derive(Debug, Clone)]
pub(super) struct S3HostRoute(pub(super) ObjectRoute);

pub(super) async fn s3_host_routing(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Response {
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
    match core_routing::parse_object_route(request.clone(), &config, &[]) {
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
            match core_routing::parse_object_route(request, &config, &[alias]) {
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

pub(super) async fn active_s3_host_alias(
    state: &AppState,
    host: &str,
) -> Result<Option<HostAliasDescriptor>, Response> {
    let host = match core_routing::normalize_alias_hostname(host) {
        Ok(host) => host,
        Err(_) => return Ok(None),
    };
    match state.persistence.get_host_alias_descriptor(&host).await {
        Ok(Some(alias)) if alias.state == core_routing::HostAliasState::Active => Ok(Some(alias)),
        Ok(_) => Ok(None),
        Err(error) => Err(s3_error(
            "InternalError",
            &format!("Failed to load host alias: {error}"),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        )),
    }
}

pub(super) fn s3_routing_config(state: &AppState) -> Option<RoutingConfig> {
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

pub(super) fn request_host(
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
        return core_routing::normalize_alias_hostname(raw_authority).map(Some);
    };
    let trusted_proxies = trusted_proxy_source_ranges(config);
    let forwarded_headers = forwarded_headers(req.headers());
    core_routing::effective_host(
        raw_authority,
        remote_peer,
        &trusted_proxies,
        &forwarded_headers,
    )
    .map(Some)
}

pub(super) fn raw_request_authority(req: &Request) -> Option<&str> {
    req.headers()
        .get(http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| req.uri().authority().map(|authority| authority.as_str()))
}

pub(super) fn trusted_proxy_source_ranges(
    config: &anvil_core::config::Config,
) -> Vec<core_routing::TrustedProxy> {
    core_routing::parse_trusted_proxies(&config.trusted_proxy_source_ranges).unwrap_or_default()
}

pub(super) fn forwarded_headers(headers: &HeaderMap) -> core_routing::ForwardedHeaders {
    core_routing::ForwardedHeaders {
        forwarded: header_values(headers, "forwarded"),
        x_forwarded_host: header_values(headers, "x-forwarded-host"),
    }
}

pub(super) fn header_values(headers: &HeaderMap, name: &'static str) -> Vec<String> {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .map(str::to_string)
        .collect()
}

pub(super) fn rewrite_s3_host_route_uri(
    req: &mut Request,
    route: &ObjectRoute,
) -> Result<(), RoutingError> {
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

pub(super) fn s3_route_rewrite_path(route: &ObjectRoute) -> String {
    let mut path = String::new();
    path.push('/');
    push_percent_encoded_path(&mut path, &route.bucket, true);
    path.push('/');
    push_percent_encoded_path(&mut path, &route.key, false);
    path
}

pub(super) fn push_percent_encoded_path(out: &mut String, value: &str, encode_slash: bool) {
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

pub(super) fn s3_routing_error(err: RoutingError) -> Response {
    s3_error(
        "InvalidRequest",
        &err.to_string(),
        axum::http::StatusCode::BAD_REQUEST,
    )
}

pub(super) fn s3_host_route(req: &Request) -> Option<ObjectRoute> {
    req.extensions()
        .get::<S3HostRoute>()
        .map(|route| route.0.clone())
}

pub(super) fn s3_routed_bucket(req: &Request, fallback_bucket: String) -> String {
    s3_host_route(req)
        .map(|route| route.bucket)
        .unwrap_or(fallback_bucket)
}

pub(super) fn s3_routed_bucket_key(
    req: &Request,
    fallback_bucket: String,
    fallback_key: String,
) -> (String, String) {
    s3_host_route(req)
        .map(|route| (route.bucket, route.key))
        .unwrap_or((fallback_bucket, fallback_key))
}

pub(super) fn s3_routed_object(req: &Request) -> Option<(String, String)> {
    s3_host_route(req)
        .filter(|route| !route.key.is_empty())
        .map(|route| (route.bucket, route.key))
}

pub(super) fn s3_routed_bucket_without_key(req: &Request) -> Option<String> {
    s3_host_route(req)
        .filter(|route| route.key.is_empty())
        .map(|route| route.bucket)
}

#[derive(Debug, Clone)]
pub(super) struct CheckedS3Route {
    pub(super) claims: Option<Claims>,
    pub(super) tenant_id: Option<i64>,
    pub(super) remote_bucket: Option<RemoteBucketProxyTarget>,
}

#[derive(Debug, Clone)]
pub(super) struct RemoteBucketProxyTarget {
    pub(super) region: String,
    pub(super) bucket_locator_generation: u64,
    pub(super) endpoint: String,
}

pub(super) async fn s3_checked_route(
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
        match core_routing::remote_bucket_routing_action(
            state.config.cross_region_routing_policy,
            proxy_target.is_some(),
        ) {
            core_routing::RemoteBucketRoutingAction::Proxy => {
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
