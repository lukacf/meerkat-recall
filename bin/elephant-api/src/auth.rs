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
    extract::{FromRequestParts, Path, Request, State},
    http::header,
    middleware::Next,
    response::{IntoResponse, Response},
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
        use crate::public_error::{ApiFailure, ErrorCode};
        let code = match self.error.as_str() {
            "insufficient_scope" => ErrorCode::ForbiddenSpace,
            "missing_token" | "invalid_token" | "expired_token" | "missing_principal" => {
                ErrorCode::Unauthorized
            }
            _ => ErrorCode::Unavailable,
        };
        ApiFailure::new(code, self.message).into_response()
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
    /// Signed grant ceiling, distinct from the space selected for this request.
    pub spaces: Option<Vec<String>>,
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
            spaces: claims.spaces,
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

impl Principal {
    /// Bind an explicitly selected space without widening the signed grant set.
    pub(crate) fn bind_space(&mut self, selected: Option<&str>) -> Result<(), AuthError> {
        let selected = selected.or(self.space_id.as_deref());
        if selected
            .is_some_and(|space| elephant_types::validation::validate_space_id(space).is_err())
        {
            return Err(AuthError {
                error: "insufficient_scope".into(),
                message: "Invalid selected space identifier".into(),
            });
        }
        if self.bypass_abac {
            self.space_id = selected.map(String::from);
            return Ok(());
        }
        if let Some(spaces) = &self.spaces {
            let Some(space) = selected.filter(|space| !space.trim().is_empty()) else {
                return Err(AuthError {
                    error: "insufficient_scope".into(),
                    message: "Select a granted space with the route or X-Space-Id".into(),
                });
            };
            if !spaces.iter().any(|granted| granted == space) {
                return Err(AuthError {
                    error: "insufficient_scope".into(),
                    message: "The selected space is not granted".into(),
                });
            }
        } else if let Some(space) = selected {
            let global_grant_admin = self.space_id.is_none()
                && self.scopes.iter().any(|scope| {
                    matches!(
                        scope.as_str(),
                        "manage:grant" | "write:space_config" | "manage:provider"
                    )
                });
            if self.space_id.as_deref() != Some(space) && !global_grant_admin {
                return Err(AuthError {
                    error: "insufficient_scope".into(),
                    message: "The selected space is not granted".into(),
                });
            }
        }
        self.space_id = selected.map(String::from);
        Ok(())
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
        let secret = load_signing_secret(
            std::env::var("JWT_SECRET").ok().as_deref(),
            std::env::var_os("JWT_SECRET_FILE")
                .as_deref()
                .map(std::path::Path::new),
        )?;
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

fn load_signing_secret(
    secret: Option<&str>,
    path: Option<&std::path::Path>,
) -> anyhow::Result<String> {
    let secret = secret.filter(|value| !value.trim().is_empty());
    anyhow::ensure!(
        secret.is_none() || path.is_none(),
        "Configure only one of JWT_SECRET and JWT_SECRET_FILE"
    );
    if let Some(path) = path {
        return elephant_auth::service_token::read_secret_file(path).map_err(anyhow::Error::from);
    }
    Ok(secret.unwrap_or_default().to_string())
}

/// Shared auth state.
pub type AuthState = Arc<AuthConfig>;

/// Parse an explicitly supplied space header without fallback or normalization.
/// UTF-8 bytes are accepted; portable deployments should prefer ASCII identifiers.
pub(crate) fn selected_space_header(
    headers: &axum::http::HeaderMap,
) -> Result<Option<&str>, AuthError> {
    let invalid = || AuthError {
        error: "insufficient_scope".into(),
        message: "Invalid or ambiguous X-Space-Id".into(),
    };
    let mut values = headers.get_all("X-Space-Id").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(invalid());
    }
    let value = std::str::from_utf8(value.as_bytes()).map_err(|_| invalid())?;
    elephant_types::validation::validate_space_id(value).map_err(|_| invalid())?;
    Ok(Some(value))
}

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
    let (mut parts, body) = request.into_parts();
    let params =
        Path::<std::collections::HashMap<String, String>>::from_request_parts(&mut parts, &())
            .await
            .ok()
            .map(|Path(params)| params)
            .unwrap_or_default();
    request = Request::from_parts(parts, body);
    let path_space = params.get("space").or_else(|| params.get("space_id"));
    let header_space = selected_space_header(request.headers())?;
    if path_space.is_some_and(|path| header_space.is_some_and(|header| path != header)) {
        return Err(AuthError {
            error: "insufficient_scope".into(),
            message: "Route space and X-Space-Id disagree".into(),
        });
    }
    let selected_space = path_space
        .map(String::as_str)
        .or(header_space)
        .map(String::from);
    // Skip auth if configured (development mode)
    if config.skip_auth {
        // Allow overriding the space via X-Space-Id header in dev mode
        let space_id = selected_space;

        // Insert a default principal for development
        let principal = Principal {
            subject: "dev-user".to_string(),
            space_id,
            spaces: None,
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
    let mut principal = Principal::from(verified.claims().clone());
    if principal.spaces.is_none() && principal.space_id.is_none() && selected_space.is_some() {
        let management_path = request
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map(axum::extract::MatchedPath::as_str)
            .is_some_and(|path| {
                matches!(
                    path,
                    "/v1/spaces" | "/v1/spaces/{space_id}" | "/v1/spaces/{space}/grants/{subject}"
                ) || path.starts_with("/v1/spaces/{space}/processing/")
            });
        if !management_path {
            return Err(AuthError {
                error: "insufficient_scope".into(),
                message: "Global management tokens cannot select ordinary data spaces".into(),
            });
        }
    }
    principal.bind_space(selected_space.as_deref())?;
    authorize_bootstrap_route(
        verified.claims(),
        request.method(),
        request
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map(axum::extract::MatchedPath::as_str),
        principal.space_id.as_deref(),
    )?;
    request.extensions_mut().insert(principal);
    request.extensions_mut().insert(verified);

    Ok(next.run(request).await)
}

/// Route admission for a verified explicit bootstrap profile. This admits no
/// ordinary data or work-producing route and does not replace record ABAC.
pub(crate) fn authorize_bootstrap_route(
    claims: &Claims,
    method: &axum::http::Method,
    matched_path: Option<&str>,
    selected_space: Option<&str>,
) -> Result<(), AuthError> {
    let Some(space) = claims.bootstrap_space.as_deref() else {
        return Ok(());
    };
    let admitted = match matched_path {
        Some("/v1/spaces") => matches!(*method, axum::http::Method::GET | axum::http::Method::POST),
        Some("/v1/spaces/{space_id}" | "/v1/spaces/{space}/grants/{subject}") => {
            matches!(*method, axum::http::Method::GET | axum::http::Method::PUT)
        }
        _ => false,
    };
    if selected_space != Some(space) || !admitted {
        return Err(AuthError {
            error: "insufficient_scope".into(),
            message: "Space bootstrap authority is restricted to its named HTTP management routes"
                .into(),
        });
    }
    Ok(())
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
            spaces: None,
            bootstrap_space: None,
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
    async fn test_skip_auth_does_not_invent_a_space() {
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
        assert_eq!(std::str::from_utf8(&body).unwrap(), "");
    }

    #[tokio::test]
    async fn test_skip_auth_rejects_explicit_empty_header() {
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

        assert_eq!(response.status(), axum::http::StatusCode::FORBIDDEN);
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

#[cfg(test)]
mod space_binding_tests {
    use super::*;

    #[test]
    fn explicit_space_headers_are_exact_utf8_and_never_fall_back(
    ) -> Result<(), Box<dyn std::error::Error>> {
        use axum::http::{HeaderMap, HeaderValue};
        let mut headers = HeaderMap::new();
        assert!(selected_space_header(&headers)
            .map_err(|_| "absent header")?
            .is_none());
        headers.insert("X-Space-Id", HeaderValue::from_bytes("研究".as_bytes())?);
        assert_eq!(
            selected_space_header(&headers).map_err(|_| "UTF-8 header")?,
            Some("研究")
        );
        for malformed in [
            b"".as_slice(),
            b" alpha".as_slice(),
            b"alpha ".as_slice(),
            &[0xff],
        ] {
            headers.insert("X-Space-Id", HeaderValue::from_bytes(malformed)?);
            assert!(selected_space_header(&headers).is_err());
        }
        headers.insert("X-Space-Id", HeaderValue::from_static("alpha"));
        headers.append("X-Space-Id", HeaderValue::from_static("beta"));
        assert!(selected_space_header(&headers).is_err());
        Ok(())
    }

    #[test]
    fn signing_secret_sources_cannot_silently_override_one_another() {
        assert!(
            load_signing_secret(Some("configured"), Some(std::path::Path::new("ignored"))).is_err()
        );
        assert_eq!(
            load_signing_secret(Some("configured"), None)
                .ok()
                .as_deref(),
            Some("configured")
        );
    }

    #[test]
    fn one_service_principal_selects_only_explicitly_granted_spaces() {
        let original = Principal {
            subject: "service".into(),
            spaces: Some(vec!["alpha".into(), "beta".into()]),
            ..Principal::default()
        };
        for space in ["alpha", "beta"] {
            let mut principal = original.clone();
            assert!(principal.bind_space(Some(space)).is_ok());
            assert_eq!(principal.space_id.as_deref(), Some(space));
        }
        for selection in [None, Some(""), Some("gamma")] {
            assert!(original.clone().bind_space(selection).is_err());
        }
    }

    #[test]
    fn explicit_empty_ceiling_cannot_fall_back_to_legacy_space() {
        let mut principal = Principal {
            space_id: Some("alpha".into()),
            spaces: Some(vec![]),
            ..Principal::default()
        };
        assert!(principal.bind_space(None).is_err());
        assert!(principal.bind_space(Some("alpha")).is_err());
    }

    #[test]
    fn legacy_single_space_is_preserved_and_cannot_be_overridden() {
        let mut principal = Principal {
            space_id: Some("alpha".into()),
            ..Principal::default()
        };
        assert!(principal.bind_space(None).is_ok());
        assert!(principal.bind_space(Some("beta")).is_err());
        assert_eq!(principal.space_id.as_deref(), Some("alpha"));
    }
}

#[cfg(test)]
#[path = "bootstrap_tests.rs"]
mod bootstrap_tests;
