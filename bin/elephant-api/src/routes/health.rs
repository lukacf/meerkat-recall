//! Health check routes.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use elephant_embeddings::{EmbeddingProvider, FakeEmbeddingProvider};
use elephant_storage::{StorageBackendKind, StorageClient, StorageResult};
use elephant_types::records::base::{Security, SubjectsState};

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;

#[derive(Debug, Default)]
struct CircuitBreakerInner {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

#[derive(Debug, Clone)]
struct CircuitBreaker {
    threshold: u32,
    open_duration: Duration,
    inner: Arc<Mutex<CircuitBreakerInner>>,
}

impl CircuitBreaker {
    fn new(threshold: u32, open_duration: Duration) -> Self {
        Self {
            threshold,
            open_duration,
            inner: Arc::new(Mutex::new(CircuitBreakerInner::default())),
        }
    }

    async fn is_open(&self) -> bool {
        let inner = self.inner.lock().await;
        inner.open_until.is_some_and(|until| Instant::now() < until)
    }

    async fn record_success(&self) {
        let mut inner = self.inner.lock().await;
        inner.consecutive_failures = 0;
        inner.open_until = None;
    }

    async fn record_failure(&self) {
        let mut inner = self.inner.lock().await;
        inner.consecutive_failures = inner.consecutive_failures.saturating_add(1);
        if inner.consecutive_failures >= self.threshold {
            inner.open_until = Some(Instant::now() + self.open_duration);
        }
    }

    async fn status(&self) -> &'static str {
        if self.is_open().await {
            "open"
        } else {
            "closed"
        }
    }
}

/// Health check response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HealthResponse {
    /// Service status.
    pub status: String,
    /// Active storage backend kind.
    pub backend: String,
    /// Storage connectivity.
    pub storage: String,
    /// Legacy compatibility alias for `storage`.
    pub surrealdb: String,
    /// Embedding provider status.
    pub embedding_provider: String,
    /// Circuit breaker statuses.
    pub circuit_breakers: CircuitBreakerStatus,
}

/// Circuit breaker status block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CircuitBreakerStatus {
    /// Status of the storage circuit breaker.
    pub storage: String,
    /// Legacy compatibility alias for `storage`.
    pub surrealdb: String,
    /// Status of the embeddings provider circuit breaker.
    pub embeddings: String,
}

impl HealthResponse {
    /// Returns true if the service is healthy.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.status == "healthy"
    }
}

/// Health check state.
#[derive(Clone)]
pub struct HealthState {
    /// Authorization service.
    pub authz: AuthzState,
    /// Storage client for dependency checks.
    pub storage: Arc<dyn StorageHealth>,
    /// Embedding provider for dependency checks.
    pub embedding_provider: Arc<dyn EmbeddingProvider>,
    /// Circuit breaker for `SurrealDB`.
    surrealdb_breaker: CircuitBreaker,
    /// Circuit breaker for embeddings.
    embeddings_breaker: CircuitBreaker,
}

impl HealthState {
    /// Creates new health state.
    #[must_use]
    pub fn new(storage: StorageClient, authz: AuthzState) -> Self {
        let storage: Arc<dyn StorageHealth> = Arc::new(storage);
        Self::new_with_storage(storage, authz)
    }

    /// Creates new health state with an abstracted storage dependency.
    #[must_use]
    pub fn new_with_storage(storage: Arc<dyn StorageHealth>, authz: AuthzState) -> Self {
        // The provider the deployment actually embeds with, so health reports
        // an unavailable provider instead of a healthy fake one.
        let embedding_provider: Arc<dyn EmbeddingProvider> = if cfg!(test) {
            Arc::new(FakeEmbeddingProvider::default())
        } else {
            elephant_embeddings::provider_for_policy(
                &elephant_embeddings::EmbeddingPolicy::from_env(),
            )
        };

        Self {
            authz,
            storage,
            embedding_provider,
            surrealdb_breaker: CircuitBreaker::new(5, Duration::from_secs(60)),
            embeddings_breaker: CircuitBreaker::new(5, Duration::from_secs(60)),
        }
    }
}

/// Minimal storage interface required for health checks.
#[async_trait::async_trait]
pub trait StorageHealth: Send + Sync {
    async fn health_check(&self) -> StorageResult<()>;

    fn backend(&self) -> StorageBackendKind {
        StorageBackendKind::SurrealDb
    }
}

#[async_trait::async_trait]
impl StorageHealth for StorageClient {
    async fn health_check(&self) -> StorageResult<()> {
        Self::health_check(self).await
    }

    fn backend(&self) -> StorageBackendKind {
        self.backend()
    }
}

type ApiResult<T> = Result<(StatusCode, Json<T>), (StatusCode, Json<ApiError>)>;

/// Health check endpoint (JWT optional).
#[allow(clippy::too_many_lines)]
async fn health(
    State(state): State<Arc<HealthState>>,
    principal: Option<axum::Extension<AuthPrincipal>>,
) -> ApiResult<HealthResponse> {
    if let Some(axum::Extension(principal)) = principal {
        let space_id = principal.space_id.clone().ok_or_else(|| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: "ACCESS_DENIED".to_string(),
                    message: "Access denied: missing space_id".to_string(),
                    details: None,
                }),
            )
        })?;

        let required_scope = principal.scopes.first().cloned().ok_or_else(|| {
            (
                StatusCode::FORBIDDEN,
                Json(ApiError {
                    code: "ACCESS_DENIED".to_string(),
                    message: "Access denied: missing scopes".to_string(),
                    details: None,
                }),
            )
        })?;

        let policy_id = state
            .authz
            .to_abac_principal(&principal)
            .clearances
            .first()
            .map_or_else(|| "default".to_string(), |c| c.policy_id.clone());

        let record = elephant_policy::RecordAttributes {
            space_id,
            security: Security {
                policy_id,
                level: 0,
                labels: Vec::new(),
                handling: Vec::new(),
            },
            subjects: Vec::new(),
            subjects_state: SubjectsState::Complete,
        };

        state
            .authz
            .evaluate(&principal, &record, &required_scope)
            .map_err(|e| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError {
                        code: e.code,
                        message: e.message,
                        details: None,
                    }),
                )
            })?;
    }

    let storage_backend = state.storage.backend();
    let storage_ok = if state.surrealdb_breaker.is_open().await {
        false
    } else {
        state.storage.health_check().await.is_ok()
    };

    if storage_ok {
        state.surrealdb_breaker.record_success().await;
    } else {
        state.surrealdb_breaker.record_failure().await;
    }

    let embedding_ok = if state.embeddings_breaker.is_open().await {
        false
    } else {
        // Avoid external network calls in health checks; empty embed is a cheap provider sanity check.
        state.embedding_provider.embed(&Vec::new()).await.is_ok()
    };

    if embedding_ok {
        state.embeddings_breaker.record_success().await;
    } else {
        state.embeddings_breaker.record_failure().await;
    }

    let response = HealthResponse {
        status: if storage_ok && embedding_ok {
            "healthy".to_string()
        } else {
            "unhealthy".to_string()
        },
        backend: storage_backend.as_str().to_string(),
        storage: if storage_ok {
            "connected".to_string()
        } else {
            "disconnected".to_string()
        },
        surrealdb: if storage_ok {
            "connected".to_string()
        } else {
            "disconnected".to_string()
        },
        embedding_provider: if embedding_ok {
            "healthy".to_string()
        } else {
            "unhealthy".to_string()
        },
        circuit_breakers: CircuitBreakerStatus {
            storage: state.surrealdb_breaker.status().await.to_string(),
            surrealdb: state.surrealdb_breaker.status().await.to_string(),
            embeddings: state.embeddings_breaker.status().await.to_string(),
        },
    };

    let status = if response.is_healthy() {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    Ok((status, Json(response)))
}

/// Returns health routes.
pub fn health_routes() -> Router<Arc<HealthState>> {
    Router::new()
        .route("/v1/health", get(health))
        .route("/health", get(health))
}

#[cfg(test)]
#[path = "health_test.rs"]
mod integration_tests;

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

    #[test]
    fn test_health_response_serde_roundtrip() {
        let response = HealthResponse {
            status: "healthy".to_string(),
            backend: "surreal-db".to_string(),
            storage: "connected".to_string(),
            surrealdb: "connected".to_string(),
            embedding_provider: "healthy".to_string(),
            circuit_breakers: CircuitBreakerStatus {
                storage: "closed".to_string(),
                surrealdb: "closed".to_string(),
                embeddings: "closed".to_string(),
            },
        };
        let json = serde_json::to_string(&response).expect("serialize");
        let decoded: HealthResponse = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded, response);
    }
}
