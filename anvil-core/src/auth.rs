use anyhow::Result;
use jsonwebtoken::{DecodingKey, EncodingKey, Header, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

#[derive(Debug, Clone)]
pub struct AuthenticatedBearerToken(pub String);

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub sub: String, // Subject (e.g., app_id)
    pub exp: usize,  // Expiration time
    pub tenant_id: i64,
    #[serde(default)]
    pub jti: Option<String>,
}

#[derive(Debug)]
pub struct JwtManager {
    secret: String,
}

impl JwtManager {
    pub fn new(secret: String) -> Self {
        Self { secret }
    }

    pub fn mint_token(&self, app_id: String, tenant_id: i64) -> Result<String> {
        let expiration = chrono::Utc::now()
            .checked_add_signed(chrono::Duration::hours(1))
            .expect("valid timestamp")
            .timestamp();

        let claims = Claims {
            sub: app_id,
            exp: expiration as usize,
            tenant_id,
            jti: Some(uuid::Uuid::new_v4().to_string()),
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

pub fn try_get_claims_from_extensions(ext: &http::Extensions) -> Option<Claims> {
    ext.get::<crate::auth::Claims>().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minted_tokens_identify_principal_and_storage_tenant_without_scopes() {
        let jwt_manager = JwtManager::new("test_secret".to_string());
        let token = jwt_manager.mint_token("test_app".to_string(), 123).unwrap();
        let claims = jwt_manager.verify_token(&token).unwrap();

        assert_eq!(claims.sub, "test_app");
        assert_eq!(claims.tenant_id, 123);
    }

    #[test]
    fn test_verify_token_invalid_secret() {
        let jwt_manager = JwtManager::new("test_secret".to_string());
        let token = jwt_manager.mint_token("test_app".to_string(), 123).unwrap();

        let wrong_jwt_manager = JwtManager::new("wrong_secret".to_string());
        let result = wrong_jwt_manager.verify_token(&token);

        assert!(result.is_err());
    }
}
