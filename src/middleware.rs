use crate::AppState;
use tonic::{Request, Status};

pub fn auth_interceptor(mut req: Request<()>, state: &AppState) -> Result<Request<()>, Status> {
    if let Some(token) = req.metadata().get("authorization") {
        let token = token.to_str().map_err(|_| Status::unauthenticated("Invalid token format"))?;
        let token = token.strip_prefix("Bearer ").ok_or_else(|| Status::unauthenticated("Invalid token format"))?;

        let claims = state.jwt_manager.verify_token(token)
            .map_err(|_| Status::unauthenticated("Invalid token"))?;

        req.extensions_mut().insert(claims);
        Ok(req)
    } else {
        Err(Status::unauthenticated("Missing auth token"))
    }
}
