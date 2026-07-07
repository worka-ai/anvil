use crate::{AppState, auth::AuthenticatedBearerToken};
use axum::{http::HeaderMap, http::HeaderValue, response::Response};
use http::Uri;
use std::time::Instant;
use tonic::{Request, Status};

pub const ANVIL_REQUEST_ID_HEADER: &str = "x-anvil-request-id";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnvilRequestId(pub String);

pub fn auth_interceptor<T>(mut req: Request<T>, state: &AppState) -> Result<Request<T>, Status> {
    let has_auth = req.metadata().get("authorization").is_some();

    let uri = if let Some(m) = req.extensions().get::<Uri>()
    /*req.extensions().get::<tonic::GrpcMethod>()*/
    {
        format!("{}", m.path())
    } else {
        return Err(Status::unauthenticated(
            "Invalid gRPC request, extension not found",
        ));
    };
    tracing::info!("[auth_interceptor] path={} auth_present={}", uri, has_auth);
    // A list of public routes that do not require authentication.
    const PUBLIC_ROUTES: &[&str] = &[
        "/anvil.AuthService/GetAccessToken",
        "/anvil.ObjectService/GetObject",
    ];
    if PUBLIC_ROUTES.contains(&uri.as_str()) && !has_auth {
        // Public routes may be called anonymously. If a bearer token is
        // supplied we still authenticate it below so service methods can apply
        // tenant-scoped permissions instead of falling back to anonymous reads.
        return Ok(req);
    }

    authenticate_bearer(&mut req, state)?;
    Ok(req)
}

/// Admin-plane authentication boundary. This only authenticates and rejects
/// credentials that are clearly data-plane-only; method code still performs the
/// Zanzibar system-realm relation check for the specific admin operation.
pub fn admin_auth_interceptor<T>(
    mut req: Request<T>,
    state: &AppState,
) -> Result<Request<T>, Status> {
    let authenticated = authenticate_bearer(&mut req, state)?;
    if authenticated.tenant_id != crate::system_realm::SYSTEM_STORAGE_TENANT_ID {
        return Err(Status::permission_denied(
            "Tenant data-plane credentials are not accepted on the admin listener",
        ));
    }
    Ok(req)
}

fn authenticate_bearer<T>(
    req: &mut Request<T>,
    state: &AppState,
) -> Result<crate::auth::Claims, Status> {
    let token = req
        .metadata()
        .get("authorization")
        .ok_or_else(|| Status::unauthenticated("Missing bearer token"))?
        .to_str()
        .map_err(|_| Status::unauthenticated("Invalid token format"))?;
    let token = token
        .strip_prefix("Bearer ")
        .ok_or_else(|| Status::unauthenticated("Invalid token format"))?;

    let bearer_token = token.to_string();
    let claims = state
        .jwt_manager
        .verify_token(&bearer_token)
        .map_err(|_| Status::unauthenticated("Unauthorised, invalid token"))?;

    req.extensions_mut().insert(claims.clone());
    req.extensions_mut()
        .insert(AuthenticatedBearerToken(bearer_token));

    Ok(claims)
}

// This runs on the raw HTTP request before Tonic handles it.
pub async fn save_uri_mw(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    tracing::info!(
        method = %req.method(),
        path = %req.uri().path(),
        headers = ?safe_header_names_for_logging(req.headers()),
        "[axum_mw] received request"
    );

    // Prefer the original (unstripped) URI if we’re nested
    let full_uri: Uri = req
        .extensions()
        .get::<axum::extract::OriginalUri>()
        .map(|o| o.0.clone())
        .unwrap_or_else(|| req.uri().clone());

    req.extensions_mut().insert(full_uri);
    next.run(req).await
}

fn safe_header_names_for_logging(headers: &HeaderMap) -> Vec<String> {
    headers
        .keys()
        .map(|name| name.as_str().to_ascii_lowercase())
        .collect()
}

pub async fn request_id_mw(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let started_at = Instant::now();
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let plane = if path.starts_with("/anvil.AdminService/") {
        "admin"
    } else if path.starts_with("/anvil.") {
        "public-grpc"
    } else {
        "public-http"
    };
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    req.extensions_mut()
        .insert(AnvilRequestId(request_id.clone()));

    let mut response = next.run(req).await;
    let status = response.status().as_u16().to_string();
    crate::perf::record_duration(
        "anvil_request",
        &[
            ("plane", plane),
            ("method", method.as_str()),
            ("path", path.as_str()),
            ("status", status.as_str()),
        ],
        started_at.elapsed(),
    );
    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(ANVIL_REQUEST_ID_HEADER, header_value);
    }
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn logged_headers_include_names_without_secret_values() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer secret-token"),
        );
        headers.insert(
            "x-amz-security-token",
            HeaderValue::from_static("session-secret"),
        );
        headers.insert("x-request-source", HeaderValue::from_static("test"));

        let logged = format!("{:?}", safe_header_names_for_logging(&headers));

        assert!(logged.contains("authorization"));
        assert!(logged.contains("x-amz-security-token"));
        assert!(logged.contains("x-request-source"));
        assert!(!logged.contains("secret-token"));
        assert!(!logged.contains("session-secret"));
        assert!(!logged.contains("Bearer"));
    }
}
