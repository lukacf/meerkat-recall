//! Authorized operator view of selected-space daily budget headroom.

use axum::{extract::State, http::StatusCode, Extension, Json};
use elephant_pipeline::scheduler::budget::{BudgetEnforcer, BudgetInspection};
use serde::Serialize;
use std::sync::Arc;

use super::{space_authorization_error, ApiError, ApiResult, AuthPrincipal, SpaceApiState};
use crate::runtime::RuntimeBudgetConfiguration;

#[derive(Serialize)]
pub(super) struct BudgetResponse {
    space_id: String,
    policy_scope: &'static str,
    enforcement_scope: &'static str,
    accounting_unit: &'static str,
    usage_includes_reservations: bool,
    headroom_is_reservation: bool,
    runtime: RuntimeBudgetConfiguration,
    today: BudgetInspection,
}

pub(super) async fn get_budget(
    State(state): State<Arc<SpaceApiState>>,
    Extension(principal): Extension<AuthPrincipal>,
) -> ApiResult<BudgetResponse> {
    let space_id = super::require_space_id(&principal)?;
    state
        .authz
        .authorize_space_scope(&principal, &space_id, "read:space_config")
        .map_err(space_authorization_error)?;
    let config = state
        .space_config_repo
        .get_by_space_id(&space_id)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Space", &space_id)),
            )
        })?;
    state
        .authz
        .authorize_record_read(&principal, &config, "read:space_config")
        .map_err(space_authorization_error)?;
    let runtime = state
        .runtime
        .as_ref()
        .ok_or_else(unavailable)?
        .budget_configuration()
        .await
        .ok_or_else(unavailable)?;
    let today = BudgetEnforcer::new(
        state.storage.clone(),
        runtime.effective.clone(),
        space_id.clone(),
    )
    .inspect()
    .await
    .map_err(|error| {
        tracing::warn!(%error, "Budget inspection unavailable");
        unavailable()
    })?;
    Ok(Json(BudgetResponse {
        space_id,
        policy_scope: "deployment_policy_applied_per_space_per_utc_day",
        enforcement_scope: "managed_workers_external_worker_settings_not_attested",
        accounting_unit: "abstract_token_units_one_per_million_measured_tokens_not_currency",
        usage_includes_reservations: true,
        headroom_is_reservation: false,
        runtime,
        today,
    }))
}

fn unavailable() -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ApiError {
            code: "NOT_READY".into(),
            message: "Effective budget configuration or usage is unavailable".into(),
            details: None,
        }),
    )
}

#[cfg(test)]
#[path = "spaces_budget_tests.rs"]
mod tests;
