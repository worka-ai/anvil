use crate::{auth, AppState};
use http::Uri;
use tonic::{Request, Status};
use tower::Service;

pub fn auth_interceptor<T>(mut req: Request<T>, state: &AppState) -> Result<Request<T>, Status> {
    let uri = if let Some(m) = req.extensions().get::<Uri>()
    /*req.extensions().get::<tonic::GrpcMethod>()*/
    {
        format!("{}", m.path())
    } else {
        return Err(Status::unauthenticated(
            "Invalid gRPC request, extension not found",
        ));
    };
    // A list of public routes that do not require authentication.
    const PUBLIC_ROUTES: &[&str] = &["/anvil.AuthService/GetAccessToken"];
    if PUBLIC_ROUTES.contains(&uri.as_str()) {
        // This is a public route, so we don't need to check for a token.
        return Ok(req);
    }

    let token = match req.metadata().get("authorization") {
        Some(t) => t
            .to_str()
            .map_err(|_| Status::unauthenticated("Invalid token format"))?,
        None => return Err(Status::unauthenticated("Missing token")),
    };

    let token = token
        .strip_prefix("Bearer ")
        .ok_or_else(|| Status::unauthenticated("Invalid token format"))?;

    let claims = state
        .jwt_manager
        .verify_token(token)
        .map_err(|_| Status::unauthenticated("Unauthorised, invalid token"))?;

    // Check scopes
    let required_scope = match uri.as_str() {
        // BucketService
        "/anvil.BucketService/CreateBucket" => "write:bucket",
        "/anvil.BucketService/DeleteBucket" => "delete:bucket",
        "/anvil.BucketService/ListBuckets" => "read:bucket",
        "/anvil.BucketService/GetBucketPolicy" => "read:bucket:policy",
        "/anvil.BucketService/PutBucketPolicy" => "write:bucket:policy",
        // ObjectService
        "/anvil.ObjectService/PutObject" => "write:object",
        "/anvil.ObjectService/GetObject" => "read:object",
        "/anvil.ObjectService/DeleteObject" => "delete:object",
        "/anvil.ObjectService/HeadObject" => "read:object",
        "/anvil.ObjectService/ListObjects" => "list:object",
        // AuthService
        "/anvil.AuthService/GrantAccess" => "grant:policy",
        "/anvil.AuthService/RevokeAccess" => "revoke:policy",
        _ => "", // Default case, no scope required
    };

    if !required_scope.is_empty() && !auth::is_authorized(required_scope, &claims.scopes) {
        return Err(Status::permission_denied("Insufficient scope"));
    }

    req.extensions_mut().insert(claims);

    Ok(req)
}

// This runs on the raw HTTP request before Tonic handles it.
pub(crate) async fn save_uri_mw(
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    // Prefer the original (unstripped) URI if we’re nested
    let full_uri: Uri = req
        .extensions()
        .get::<axum::extract::OriginalUri>()
        .map(|o| o.0.clone())
        .unwrap_or_else(|| req.uri().clone());

    req.extensions_mut().insert(full_uri);
    next.run(req).await
}
