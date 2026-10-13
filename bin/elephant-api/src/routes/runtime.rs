//! Runtime status routes.

use std::sync::Arc;

use axum::{extract::State, routing::get, Extension, Json, Router};

use crate::runtime::{RuntimeManager, RuntimeStatus};

#[derive(Clone)]
pub struct RuntimeState {
    pub runtime: Arc<RuntimeManager>,
}

impl RuntimeState {
    #[must_use]
    pub const fn new(runtime: Arc<RuntimeManager>) -> Self {
        Self { runtime }
    }
}

async fn get_runtime_status(
    State(state): State<Arc<RuntimeState>>,
    Extension(principal): Extension<crate::auth::Principal>,
) -> Result<Json<RuntimeStatus>, crate::public_error::ApiFailure> {
    super::readiness::authorize_status(&principal)?;
    Ok(Json(state.runtime.get_readiness().await))
}

pub fn runtime_routes() -> Router<Arc<RuntimeState>> {
    Router::new().route("/v1/runtime/status", get(get_runtime_status))
}
