//! JWT authentication and authorization middleware.
//!
//! This module provides:
//! - JWT token validation
//! - Claims extraction
//! - Principal/subject identification
//! - Scope-based authorization

use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Request, State},
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
pub use elephant_auth::Claims;
use elephant_auth::{IssuerMappings, LegacyVerifier, VerifiedCallerContext};
use jsonwebtoken::Algorithm;
use serde::Serialize;

use elephant_policy::Clearance;
use elephant_types::RecordId;

/// JWT-related errors.
#[derive(Debug, Clone, Serialize)]
pub struct AuthError {
    /// Error code (e.g. "`missing_token`").
    pub error: String,
    /// Human-readable error message.
    pub message: String,
}

impl From<elephant_auth::AuthError> for AuthError {
    fn from(error: elephant_auth::AuthError) -> Self {
        Self {
            error: if error == elephant_auth::AuthError::ExpiredToken {
                "expired_token"
            } else {
                "invalid_token"
            }
            .to_string(),
            message: error.to_string(),
        }
    }
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let status = match self.error.as_str() {
            "missing_token" | "invalid_token" | "expired_token" => StatusCode::UNAUTHORIZED,
            "insufficient_scope" => StatusCode::FORBIDDEN,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, Json(self)).into_response()
    }
}

/// Principal information extracted from the request.
/// This is stored in request extensions for downstream handlers.
#[derive(Debug, Clone, Default)]
pub struct Principal {
    /// The authenticated subject ID.
    pub subject: String,
    /// Space the principal has access to.
    pub space_id: Option<String>,
    /// Granted scopes.
    pub scopes: Vec<String>,
    /// Security level.
    pub security_level: i32,
    /// Security labels.
    pub security_labels: Vec<String>,
    /// ABAC clearances for different policies.
    pub clearances: Vec<Clearance>,
    /// Allowlist of subject entities (if restricted).
    pub subject_allowlist: Option<Vec<RecordId>>,
    /// Purpose labels for access.
    pub purpose: Vec<String>,
    /// Bypasses ABAC checks (development only).
    pub bypass_abac: bool,
}

impl From<Claims> for Principal {
    fn from(claims: Claims) -> Self {
        Self {
            subject: claims.sub,
            space_id: claims.space_id,
            scopes: claims.scopes,
            security_level: claims.security_level,
            security_labels: claims.security_labels,
            clearances: claims.clearances,
            subject_allowlist: claims.subject_allowlist,
            purpose: claims.purpose,
            bypass_abac: false,
        }
    }
}

/// JWT authentication configuration.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    /// Secret key for HS256 validation.
    pub secret: String,
    /// Algorithm to use for validation.
    pub algorithm: Algorithm,
    /// Required audience.
    pub audience: Option<String>,
    /// Required issuer.
    pub issuer: Option<String>,
    /// Skip authentication (for development).
    pub skip_auth: bool,
}

impl AuthConfig {
    /// Creates a new auth config from environment variables.
    ///
    /// # Errors
    ///
    /// Returns an error when an explicit JWT algorithm is unsupported.
    pub fn from_env() -> anyhow::Result<Self> {
        let secret = std::env::var("JWT_SECRET").unwrap_or_default();
        let algorithm = Self::configured_algorithm(std::env::var("JWT_ALGORITHM").ok().as_deref())
            .map_err(anyhow::Error::msg)?;
        let audience = std::env::var("JWT_AUDIENCE").ok();
        let issuer = std::env::var("JWT_ISSUER").ok();
        let skip_auth = std::env::var("SKIP_AUTH")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false);

        Ok(Self {
            secret,
            algorithm,
            audience,
            issuer,
            skip_auth,
        })
    }

    fn configured_algorithm(value: Option<&str>) -> Result<Algorithm, &'static str> {
        match value {
            None | Some("HS256") => Ok(Algorithm::HS256),
            Some(_) => Err("JWT_ALGORITHM must be HS256; other algorithms are not supported"),
        }
    }

    /// Rejects unsafe authenticated server configuration before opening a listener.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for a missing or short shared secret, or
    /// an algorithm unsupported by the shared-secret decoder.
    pub fn validate_server_config(&self) -> Result<(), &'static str> {
        if self.skip_auth {
            return Ok(());
        }
        if self.algorithm != Algorithm::HS256 {
            return Err("JWT_ALGORITHM must be HS256; asymmetric keys are not supported");
        }
        if self.secret.trim().len() < 32 {
            return Err("Set JWT_SECRET to a random secret of at least 32 bytes, or explicitly set SKIP_AUTH=true for local development");
        }
        Ok(())
    }

    /// One verifier shared by HTTP and MCP. The result is authentication evidence,
    /// not a current policy decision or operation permit.
    pub(crate) fn verify_token(
        &self,
        token: &str,
        mappings: Option<&IssuerMappings>,
    ) -> Result<VerifiedCallerContext, AuthError> {
        LegacyVerifier {
            secret: &self.secret,
            algorithm: self.algorithm,
            audience: self.audience.as_deref(),
            issuer: self.issuer.as_deref(),
            mappings,
        }
        .verify(token)
        .map_err(AuthError::from)
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            secret: String::new(),
            algorithm: Algorithm::HS256,
            audience: None,
            issuer: None,
            skip_auth: false,
        }
    }
}

/// Shared auth state.
pub type AuthState = Arc<AuthConfig>;

/// JWT authentication middleware.
///
/// Extracts and validates JWT from Authorization header.
/// Stores Principal in request extensions for downstream handlers.
///
/// # Errors
///
/// Returns an `AuthError` if the token is missing, invalid, or expired.
pub async fn jwt_auth(
    State(config): State<AuthState>,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, AuthError> {
    // Skip auth if configured (development mode)
    if config.skip_auth {
        // Allow overriding the space via X-Space-Id header in dev mode
        let space_id = request
            .headers()
            .get("X-Space-Id")
            .and_then(|h| h.to_str().ok())
            .filter(|s| !s.is_empty())
            .map_or_else(|| "default".to_string(), String::from);

        // Insert a default principal for development
        let principal = Principal {
            subject: "dev-user".to_string(),
            space_id: Some(space_id),
            scopes: vec!["*".to_string()],
            security_level: 10,
            security_labels: vec![],
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: Vec::new(),
            bypass_abac: true,
        };
        request.extensions_mut().insert(principal);
        return Ok(next.run(request).await);
    }

    // Extract token from Authorization header
    let auth_header = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok());

    let token = match auth_header {
        Some(h) if h.starts_with("Bearer ") => &h[7..],
        _ => {
            return Err(AuthError {
                error: "missing_token".to_string(),
                message: "Authorization header with Bearer token required".to_string(),
            });
        }
    };

    let mappings = request.extensions().get::<Arc<IssuerMappings>>();
    let verified = config.verify_token(token, mappings.map(AsRef::as_ref))?;
    let principal = Principal::from(verified.claims().clone());
    request.extensions_mut().insert(principal);
    request.extensions_mut().insert(verified);

    Ok(next.run(request).await)
}

/// Scope validation middleware factory.
///
/// Creates middleware that requires specific scopes.
/// Must be applied after `jwt_auth` middleware.
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
pub fn require_scope(
    required_scope: &'static str,
) -> impl Fn(
    Request<Body>,
    Next,
) -> std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<Response, AuthError>> + Send>,
> + Clone
       + Send {
    move |request: Request<Body>, next: Next| {
        Box::pin(async move {
            let principal = request
                .extensions()
                .get::<Principal>()
                .ok_or_else(|| AuthError {
                    error: "missing_principal".to_string(),
                    message: "Authentication required".to_string(),
                })?;

            if !principal
                .scopes
                .iter()
                .any(|s| s == required_scope || s == "*")
            {
                return Err(AuthError {
                    error: "insufficient_scope".to_string(),
                    message: format!("Required scope '{required_scope}' not granted"),
                });
            }

            Ok(next.run(request).await)
        })
    }
}

/// Extract principal from request extensions.
#[allow(dead_code)]
pub fn extract_principal(request: &Request<Body>) -> Option<&Principal> {
    request.extensions().get::<Principal>()
}

/// Common scopes used in the API.
pub mod scopes {
    /// Read entities.
    #[allow(dead_code)]
    pub const ENTITIES_READ: &str = "entities:read";
    /// Write entities.
    #[allow(dead_code)]
    pub const ENTITIES_WRITE: &str = "entities:write";
    /// Read relationships.
    #[allow(dead_code)]
    pub const RELATIONSHIPS_READ: &str = "relationships:read";
    /// Write relationships.
    #[allow(dead_code)]
    pub const RELATIONSHIPS_WRITE: &str = "relationships:write";
    /// Read timeline events.
    #[allow(dead_code)]
    pub const TIMELINE_READ: &str = "timeline:read";
    /// Write timeline events.
    #[allow(dead_code)]
    pub const TIMELINE_WRITE: &str = "timeline:write";
    /// Read documents.
    #[allow(dead_code)]
    pub const DOCS_READ: &str = "docs:read";
    /// Write documents.
    #[allow(dead_code)]
    pub const DOCS_WRITE: &str = "docs:write";
    /// Read facts.
    #[allow(dead_code)]
    pub const FACTS_READ: &str = "facts:read";
    /// Write facts.
    #[allow(dead_code)]
    pub const FACTS_WRITE: &str = "facts:write";
    /// Read sources.
    #[allow(dead_code)]
    pub const SOURCES_READ: &str = "sources:read";
    /// Write sources.
    #[allow(dead_code)]
    pub const SOURCES_WRITE: &str = "sources:write";
    /// Admin operations.
    #[allow(dead_code)]
    pub const ADMIN: &str = "admin";
    /// Full access.
    #[allow(dead_code)]
    pub const FULL_ACCESS: &str = "*";
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use axum::{middleware, routing, Router};
    use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
    use tower::ServiceExt as _;

    fn create_test_token(claims: &Claims, secret: &str) -> String {
        let key = EncodingKey::from_secret(secret.as_bytes());
        encode(&Header::default(), claims, &key).expect("encode token")
    }

    fn create_test_claims() -> Claims {
        Claims {
            sub: "test-user".to_string(),
            exp: chrono::Utc::now().timestamp() + 3600,
            iat: chrono::Utc::now().timestamp(),
            iss: Some("test-issuer".to_string()),
            aud: None,
            space_id: Some("space_a".to_string()),
            scopes: vec!["entities:read".to_string(), "entities:write".to_string()],
            security_level: 2,
            security_labels: vec!["confidential".to_string()],
            clearances: Vec::new(),
            subject_allowlist: None,
            purpose: Vec::new(),
        }
    }

    #[test]
    fn test_claims_has_scope() {
        let claims = create_test_claims();
        assert!(claims.has_scope("entities:read"));
        assert!(claims.has_scope("entities:write"));
        assert!(!claims.has_scope("admin"));
    }

    #[test]
    fn test_claims_has_any_scope() {
        let claims = create_test_claims();
        assert!(claims.has_any_scope(&["entities:read", "admin"]));
        assert!(!claims.has_any_scope(&["admin", "superuser"]));
    }

    #[test]
    fn test_claims_has_all_scopes() {
        let claims = create_test_claims();
        assert!(claims.has_all_scopes(&["entities:read", "entities:write"]));
        assert!(!claims.has_all_scopes(&["entities:read", "admin"]));
    }

    #[test]
    fn test_claims_wildcard_scope() {
        let mut claims = create_test_claims();
        claims.scopes = vec!["*".to_string()];
        assert!(claims.has_scope("entities:read"));
        assert!(claims.has_scope("admin"));
        assert!(claims.has_scope("anything"));
    }

    #[test]
    fn test_principal_from_claims() {
        let claims = create_test_claims();
        let principal = Principal::from(claims.clone());
        assert_eq!(principal.subject, claims.sub);
        assert_eq!(principal.space_id, claims.space_id);
        assert_eq!(principal.scopes, claims.scopes);
    }

    #[test]
    fn test_auth_config_default() {
        let config = AuthConfig::default();
        assert!(!config.skip_auth);
        assert!(matches!(config.algorithm, Algorithm::HS256));
    }

    #[test]
    fn explicit_unsupported_algorithms_do_not_fall_back_to_hs256() {
        assert_eq!(AuthConfig::configured_algorithm(None), Ok(Algorithm::HS256));
        assert_eq!(
            AuthConfig::configured_algorithm(Some("HS256")),
            Ok(Algorithm::HS256)
        );
        for value in ["", "HS384", "HS512", "RS256", "ES256", "EdDSA", "HS265"] {
            assert!(AuthConfig::configured_algorithm(Some(value)).is_err());
        }
    }

    #[test]
    fn authenticated_server_rejects_development_and_missing_secrets() {
        assert!(AuthConfig::default().validate_server_config().is_err());
        for secret in ["", "short", "                                "] {
            let config = AuthConfig {
                secret: secret.to_string(),
                ..AuthConfig::default()
            };
            assert!(config.validate_server_config().is_err());
        }
    }

    #[test]
    fn server_auth_requires_supported_key_type_but_allows_explicit_dev_mode() {
        let mut config = AuthConfig {
            secret: "x".repeat(32),
            ..AuthConfig::default()
        };
        assert!(config.validate_server_config().is_ok());
        config.algorithm = Algorithm::RS256;
        assert!(config.validate_server_config().is_err());
        config.skip_auth = true;
        config.secret.clear();
        assert!(config.validate_server_config().is_ok());
    }

    #[test]
    fn test_create_valid_token() {
        let claims = create_test_claims();
        let token = create_test_token(&claims, "test-secret");
        assert!(!token.is_empty());

        // Decode and verify
        let key = DecodingKey::from_secret(b"test-secret");
        let validation = Validation::new(Algorithm::HS256);
        let decoded = decode::<Claims>(&token, &key, &validation).expect("decode");
        assert_eq!(decoded.claims.sub, "test-user");
    }

    #[test]
    fn test_invalid_signature() {
        let claims = create_test_claims();
        let token = create_test_token(&claims, "test-secret");

        let key = DecodingKey::from_secret(b"wrong-secret");
        let validation = Validation::new(Algorithm::HS256);
        let result = decode::<Claims>(&token, &key, &validation);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_skip_auth_uses_x_space_id_header() {
        let config = Arc::new(AuthConfig {
            skip_auth: true,
            ..AuthConfig::default()
        });

        let app = Router::new()
            .route(
                "/test",
                routing::get(
                    |axum::Extension(p): axum::Extension<Principal>| async move {
                        p.space_id.unwrap_or_default()
                    },
                ),
            )
            .layer(middleware::from_fn_with_state(config, jwt_auth));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("X-Space-Id", "myspace")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "myspace");
    }

    #[tokio::test]
    async fn test_skip_auth_defaults_to_default() {
        let config = Arc::new(AuthConfig {
            skip_auth: true,
            ..AuthConfig::default()
        });

        let app = Router::new()
            .route(
                "/test",
                routing::get(
                    |axum::Extension(p): axum::Extension<Principal>| async move {
                        p.space_id.unwrap_or_default()
                    },
                ),
            )
            .layer(middleware::from_fn_with_state(config, jwt_auth));

        let response = app
            .oneshot(Request::builder().uri("/test").body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "default");
    }

    #[tokio::test]
    async fn test_skip_auth_ignores_empty_header() {
        let config = Arc::new(AuthConfig {
            skip_auth: true,
            ..AuthConfig::default()
        });

        let app = Router::new()
            .route(
                "/test",
                routing::get(
                    |axum::Extension(p): axum::Extension<Principal>| async move {
                        p.space_id.unwrap_or_default()
                    },
                ),
            )
            .layer(middleware::from_fn_with_state(config, jwt_auth));

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/test")
                    .header("X-Space-Id", "")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "default");
    }

    #[test]
    fn test_expired_token() {
        let mut claims = create_test_claims();
        claims.exp = chrono::Utc::now().timestamp() - 3600; // 1 hour ago
        let token = create_test_token(&claims, "test-secret");

        let key = DecodingKey::from_secret(b"test-secret");
        let validation = Validation::new(Algorithm::HS256);
        let result = decode::<Claims>(&token, &key, &validation);
        assert!(result.is_err());
    }
    include!("auth_context_tests.rs");
}

#[cfg(test)]
#[path = "auth_test_support.rs"]
pub(crate) mod test_support;
