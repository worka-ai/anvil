use crate::{AppState, auth::AuthenticatedBearerToken};
use axum::{http::HeaderValue, response::Response};
use http::Uri;
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
    const PUBLIC_ROUTES: &[&str] = &["/anvil.AuthService/GetAccessToken"];
    if PUBLIC_ROUTES.contains(&uri.as_str()) {
        // This is a public route, so we don't need to check for a token.
        return Ok(req);
    }

    match req.metadata().get("authorization") {
        Some(t) => {
            let token = t
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

            req.extensions_mut().insert(claims);
            req.extensions_mut()
                .insert(AuthenticatedBearerToken(bearer_token));

            Ok(req)
        }
        None => Ok(req),
    }
}

// This runs on the raw HTTP request before Tonic handles it.
pub async fn save_uri_mw(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    tracing::info!(
        "[axum_mw] Received request with headers: {:?}",
        req.headers()
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

pub async fn request_id_mw(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let request_id = uuid::Uuid::new_v4().simple().to_string();
    req.extensions_mut()
        .insert(AnvilRequestId(request_id.clone()));

    let mut response = next.run(req).await;
    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        response
            .headers_mut()
            .insert(ANVIL_REQUEST_ID_HEADER, header_value);
    }
    response
}
