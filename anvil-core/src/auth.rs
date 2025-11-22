use crate::permissions::AnvilAction;
use anyhow::Result;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::{debug, info, warn};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String, // Subject (e.g., app_id)
    pub exp: usize,  // Expiration time
    pub scopes: Vec<String>,
    pub tenant_id: i64,
}

#[derive(Debug)]
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

/// Checks if a required action on a resource is satisfied by the scopes present in a token.
///
/// This function uses a new, structured scope format: `action|resource_pattern`.
/// Example: `bucket:write|some-bucket/*`
pub fn is_authorized(
    required_action: AnvilAction,
    required_resource: &str,
    token_scopes: &[String],
) -> bool {
    let required_action_str = required_action.to_string();
    info!(
        %required_action,
        %required_resource,
        ?token_scopes,
        "Checking authorization"
    );

    for scope in token_scopes {
        let parts: Vec<&str> = scope.splitn(2, '|').collect();
        if parts.len() != 2 {
            warn!(malformed_scope = %scope, "Skipping malformed scope in token");
            continue;
        }

        let token_action_str = parts[0];
        let token_resource_pattern = parts[1];

        let token_action = match token_action_str.parse::<AnvilAction>() {
            Ok(action) => action,
            Err(_) => {
                warn!(unknown_action = %token_action_str, "Skipping scope with unknown action");
                continue;
            }
        };

        // Check if the action matches (or is a wildcard)
        if !action_covers_required(&token_action, &required_action) {
            continue;
        }

        // If actions match, check if the resource is covered by the pattern.
        if resource_matches(required_resource, token_resource_pattern) {
            debug!(
                required = %format!("{}|{}", required_action, required_resource),
                matched_scope = %scope,
                "Authorization successful"
            );
            return true;
        }
    }

    debug!(
        required = %format!("{}|{}", required_action, required_resource),
        "Authorization failed"
    );
    false
}

fn action_covers_required(token_action: &AnvilAction, required_action: &AnvilAction) -> bool {
    match token_action {
        AnvilAction::All => true, // Covers everything
        AnvilAction::BucketAll => matches!(
            required_action,
            AnvilAction::BucketCreate
                | AnvilAction::BucketDelete
                | AnvilAction::BucketRead
                | AnvilAction::BucketWrite
                | AnvilAction::BucketList
        ),
        AnvilAction::ObjectAll => matches!(
            required_action,
            AnvilAction::ObjectRead
                | AnvilAction::ObjectWrite
                | AnvilAction::ObjectDelete
                | AnvilAction::ObjectList
        ),
        AnvilAction::HfKeyAll => matches!(
            required_action,
            AnvilAction::HfKeyCreate
                | AnvilAction::HfKeyRead
                | AnvilAction::HfKeyDelete
                | AnvilAction::HfKeyList
        ),
        AnvilAction::HfIngestionAll => matches!(
            required_action,
            AnvilAction::HfIngestionCreate
                | AnvilAction::HfIngestionRead
                | AnvilAction::HfIngestionDelete
        ),
        AnvilAction::PolicyAll => matches!(
            required_action,
            AnvilAction::PolicyGrant | AnvilAction::PolicyRevoke
        ),
        _ => token_action == required_action, // Exact match for specific actions
    }
}

/// Checks if a given resource string matches a pattern.
/// Supports exact matches, prefix matches with a wildcard (`*`), and a global wildcard.
fn resource_matches(required: &str, pattern: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(base) = pattern.strip_suffix('*') {
        return required.starts_with(base);
    }
    required == pattern
}

pub fn try_get_claims_from_extensions(ext: &http::Extensions) -> Option<Claims> {
    ext.get::<crate::auth::Claims>().cloned()
}

pub fn try_get_scopes_from_extensions(ext: &http::Extensions) -> Option<Vec<String>> {
    ext.get::<crate::auth::Claims>()
        .map(|claims| claims.scopes.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::AnvilAction;

    #[test]
    fn test_is_authorized_new_format() {
        let token_scopes = vec![
            "bucket:read|images-bucket".to_string(),
            "bucket:write|images-bucket".to_string(),
            "object:read|images-bucket/users/*".to_string(),
            "object:write|images-bucket/users/123/*".to_string(),
            "object:delete|other-bucket/stuff".to_string(),
            "*|*".to_string(), // Global wildcard
        ];

        // Exact match
        assert!(is_authorized(
            AnvilAction::BucketRead,
            "images-bucket",
            &token_scopes
        ));

        // Prefix match
        assert!(is_authorized(
            AnvilAction::ObjectRead,
            "images-bucket/users/abc",
            &token_scopes
        ));
        assert!(is_authorized(
            AnvilAction::ObjectWrite,
            "images-bucket/users/123/avatar.jpg",
            &token_scopes
        ));

        // Global wildcard action and resource
        assert!(is_authorized(
            AnvilAction::BucketCreate,
            "any-bucket",
            &token_scopes
        ));

        //below the we get unauthorised if wildcard doesn't grant access
        let no_wildcard = token_scopes
            .clone()
            .iter()
            .filter(|v| v != &"*|*")
            .map(|v| v.to_string())
            .collect::<Vec<String>>();

        // Mismatch action
        assert!(!is_authorized(
            AnvilAction::BucketDelete,
            "images-bucket",
            &no_wildcard
        ));

        // Mismatch resource
        assert!(!is_authorized(
            AnvilAction::ObjectRead,
            "other-bucket/users/abc",
            &no_wildcard
        ));

        // Mismatch on prefix rule
        assert!(!is_authorized(
            AnvilAction::ObjectWrite,
            "images-bucket/users/456/avatar.jpg", // 456 does not match 123/*
            &no_wildcard
        ));
    }

    #[test]
    fn test_mint_and_verify_token_success() {
        let jwt_manager = JwtManager::new("test_secret".to_string());
        let app_id = "test_app".to_string();
        let scopes = vec!["scope1".to_string(), "scope2".to_string()];
        let tenant_id = 123;

        let token = jwt_manager
            .mint_token(app_id.clone(), scopes.clone(), tenant_id)
            .unwrap();
        let claims = jwt_manager.verify_token(&token).unwrap();

        assert_eq!(claims.sub, app_id);
        assert_eq!(claims.scopes, scopes);
        assert_eq!(claims.tenant_id, tenant_id);
    }

    #[test]
    fn test_verify_token_invalid_secret() {
        let jwt_manager = JwtManager::new("test_secret".to_string());
        let app_id = "test_app".to_string();
        let scopes = vec!["scope1".to_string()];
        let tenant_id = 123;

        let token = jwt_manager.mint_token(app_id, scopes, tenant_id).unwrap();

        let wrong_jwt_manager = JwtManager::new("wrong_secret".to_string());
        let result = wrong_jwt_manager.verify_token(&token);

        assert!(result.is_err());
    }

    #[test]
    fn test_verify_token_malformed() {
        let jwt_manager = JwtManager::new("test_secret".to_string());
        let malformed_token = "this.is.not.a.jwt";

        let result = jwt_manager.verify_token(malformed_token);

        assert!(result.is_err());
    }
}
