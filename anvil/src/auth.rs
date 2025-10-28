use anyhow::Result;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String, // Subject (e.g., app_id)
    pub exp: usize,  // Expiration time
    pub scopes: Vec<String>,
    pub tenant_id: i64,
}

pub struct JwtManager {
    secret: String,
}

impl JwtManager {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }

    pub fn mint_token(
        &self,
        app_id: String,
        scopes: Vec<String>,
        tenant_id: i64,
    ) -> Result<String> {
        let expiration = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::hours(1))
            .expect("valid timestamp")
            .timestamp();

        let claims = Claims {
            sub: app_id,
            exp: expiration as usize,
            scopes,
            tenant_id,
        };

        encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(self.secret.as_ref()),
        )
        .map_err(Into::into)
    }

    pub fn verify_token(&self, token: &str) -> Result<Claims> {
        let result = decode::<Claims>(
            token,
            &DecodingKey::from_secret(self.secret.as_ref()),
            &Validation::default(),
        );

        match result {
            Ok(token_data) => {
                debug!(subject = %token_data.claims.sub, "JWT verified successfully");
                Ok(token_data.claims)
            }
            Err(e) => {
                warn!(error = %e, "JWT verification failed");
                Err(e.into())
            }
        }
    }
}

/// Checks if a required scope is satisfied by the scopes present in a token.
/// Supports wildcards.
pub fn is_authorized(required_scope: &str, token_scopes: &[String]) -> bool {
    debug!(%required_scope, ?token_scopes, "Checking authorization");

    let required_parts: Vec<&str> = required_scope.splitn(2, ':').collect();
    if required_parts.len() != 2 {
        warn!(%required_scope, "Malformed required scope");
        return false;
    }
    let required_action = required_parts[0];
    let required_resource = required_parts[1];

    for scope in token_scopes {
        let parts: Vec<&str> = scope.splitn(2, ':').collect();
        if parts.len() != 2 {
            continue;
        }
        let action = parts[0];
        let resource = parts[1];

        if action == required_action || action == "*" {
            if resource_matches(required_resource, resource) {
                debug!(%required_scope, matched_scope = %scope, "Authorization successful");
                return true;
            }
        }
    }

    debug!(%required_scope, "Authorization failed");
    false
}

fn resource_matches(required: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if pattern.ends_with('*') {
        let base = &pattern[..pattern.len() - 1];
        return required.starts_with(base);
    }
    required == pattern
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_authorized() {
        let token_scopes = vec![
            "read:bucket/folder/*".to_string(),
            "write:bucket/specific/file.txt".to_string(),
            "grant:bucket/*".to_string(),
        ];

        // Exact match
        assert!(is_authorized(
            "write:bucket/specific/file.txt",
            &token_scopes
        ));

        // Wildcard match
        assert!(is_authorized(
            "read:bucket/folder/sub/image.jpg",
            &token_scopes
        ));

        // Grant match
        assert!(is_authorized("grant:bucket/some/path", &token_scopes));

        // Mismatch action
        assert!(!is_authorized(
            "delete:bucket/folder/sub/image.jpg",
            &token_scopes
        ));

        // Mismatch resource
        assert!(!is_authorized(
            "read:another-bucket/folder/file.txt",
            &token_scopes
        ));
    }
}
