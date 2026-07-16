use super::*;

pub(super) async fn reserved_namespace_guard(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    if request_targets_reserved_namespace(&req)
        || request_targets_native_routed_reserved_namespace(&state, &req)
    {
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

pub(super) fn request_targets_reserved_namespace(req: &Request) -> bool {
    if let Some(route) = s3_host_route(req) {
        if !route.key.is_empty() && validation::is_reserved_internal_key(&route.key) {
            return true;
        }
    }

    let path = req.uri().path().trim_start_matches('/');
    if let Some((_, object_key)) = path.split_once('/') {
        let object_key = percent_decode_path_component(object_key);
        if validation::is_reserved_internal_key(&object_key) {
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

pub(super) fn request_targets_native_routed_reserved_namespace(
    state: &AppState,
    req: &Request,
) -> bool {
    let Some(config) = s3_routing_config(state) else {
        return false;
    };
    let host = match request_host(req, state.config.as_ref()) {
        Ok(Some(host)) => host,
        Ok(None) | Err(_) => return false,
    };
    let route = core_routing::parse_object_route(
        RouteRequest {
            host: &host,
            path: req.uri().path(),
        },
        &config,
        &[],
    );
    match route {
        Ok(route) => !route.key.is_empty() && validation::is_reserved_internal_key(&route.key),
        Err(_) => false,
    }
}

pub(super) fn request_copy_source_targets_reserved_namespace(
    headers: &axum::http::HeaderMap,
) -> bool {
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
