//! API routes.

pub mod assertions;
pub mod docs;
pub mod entities;
pub mod events;
pub mod health;
pub mod identity;
pub(crate) mod ingest_guard;
pub mod knowledge_views;
pub mod predicates;
pub mod relationships;
pub mod runtime;
pub mod sources;
pub mod spaces;
pub mod system;
pub mod timeline;
pub mod truth_slots;
pub mod work_items;

pub use assertions::assertion_routes;
pub use docs::doc_routes;
pub use entities::entity_routes;
pub use events::events_routes;
pub use health::health_routes;
pub use identity::identity_routes;
pub use knowledge_views::knowledge_view_routes;
pub use predicates::predicate_routes;
pub use relationships::rel_routes;
pub use runtime::runtime_routes;
pub use sources::source_routes;
pub use spaces::space_routes;
pub use system::system_routes;
pub use timeline::timeline_routes;
pub use truth_slots::truth_slot_routes;
pub use work_items::work_item_routes;

/// Require an explicit tenant scope before any repository operation.
#[allow(clippy::result_large_err)]
pub(crate) fn require_space_id(
    principal: &crate::auth::Principal,
) -> Result<String, (axum::http::StatusCode, axum::Json<entities::ApiError>)> {
    principal
        .space_id
        .as_ref()
        .filter(|space_id| !space_id.trim().is_empty())
        .cloned()
        .ok_or_else(|| {
            (
                axum::http::StatusCode::FORBIDDEN,
                axum::Json(entities::ApiError {
                    code: "ACCESS_DENIED".to_string(),
                    message: "Access denied: missing space_id".to_string(),
                    details: None,
                }),
            )
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::Principal;
    use axum::http::StatusCode;

    #[test]
    fn require_space_rejects_missing_and_blank_scope_even_with_auth_bypass() {
        for space_id in [None, Some(String::new()), Some(" \t".to_string())] {
            let principal = Principal {
                space_id,
                bypass_abac: true,
                ..Principal::default()
            };
            let result = require_space_id(&principal);
            assert!(matches!(result, Err((StatusCode::FORBIDDEN, _))));
        }
    }

    #[test]
    fn require_space_keeps_exact_tenant_identity() {
        for space_id in ["tenant-alpha", " tenant-alpha "] {
            let principal = Principal {
                space_id: Some(space_id.into()),
                ..Principal::default()
            };
            assert!(matches!(require_space_id(&principal), Ok(space) if space == space_id));
        }
    }
}
