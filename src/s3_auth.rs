use axum::{extract::Request, middleware::Next, response::Response};

pub async fn sigv4_auth(req: Request, next: Next) -> Response {
    println!("S3 AUTH: Bypassing SigV4 check for now.");
    next.run(req).await
}
