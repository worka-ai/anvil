use axum::{
    body::Body,
    extract::Request,
    middleware::Next,
    response::Response,
};
use axum_extra::headers::Host;
use axum_extra::TypedHeader;

pub async fn sigv4_auth(req: Request, next: Next) -> Response {
    println!("S3 AUTH: Bypassing SigV4 check for now.");
    next.run(req).await
}
