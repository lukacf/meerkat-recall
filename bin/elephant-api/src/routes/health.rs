//! Process liveness only. Authenticated capability checks live in readiness.

use axum::{routing::get, Router};

/// Returns the public liveness route and its existing alias.
/// Neither endpoint probes storage, workers or a model provider.
pub fn health_routes() -> Router {
    Router::new()
        .route("/v1/health", get(super::readiness::liveness))
        .route("/health", get(super::readiness::liveness))
}

#[cfg(test)]
#[path = "health_test.rs"]
mod tests;
