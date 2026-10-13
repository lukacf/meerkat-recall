//! Current service-grant attenuation shared by authenticated REST routes.

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    middleware::Next,
    response::{IntoResponse, Response},
};
use elephant_storage::space_grant_repository::SpaceGrantRepository;
use elephant_types::space_grant::intersect_scopes;

use crate::auth::Principal;
use crate::public_error::{ApiFailure, ErrorCode};
use crate::runtime::RuntimeManager;

#[expect(
    clippy::redundant_pub_crate,
    reason = "Keep the explicit internal visibility boundary."
)]
pub(crate) async fn enforce_space_grant(
    State(runtime): State<Arc<RuntimeManager>>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(principal) = request.extensions().get::<Principal>().cloned() else {
        return ApiFailure::new(ErrorCode::Unauthorized, "Authentication required").into_response();
    };
    if let Some(verified) = request
        .extensions()
        .get::<elephant_auth::VerifiedCallerContext>()
    {
        if verified.claims().bootstrap_space.is_some() {
            if let Err(error) = crate::auth::authorize_bootstrap_route(
                verified.claims(),
                request.method(),
                request
                    .extensions()
                    .get::<axum::extract::MatchedPath>()
                    .map(axum::extract::MatchedPath::as_str),
                principal.space_id.as_deref(),
            ) {
                return error.into_response();
            }
            return next.run(request).await;
        }
    }
    if principal.spaces.is_none() || principal.bypass_abac {
        return next.run(request).await;
    }
    match attenuate(&runtime, principal).await {
        Ok(principal) => {
            request.extensions_mut().insert(principal);
            next.run(request).await
        }
        Err(error) => error.into_response(),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn attenuate(
    runtime: &RuntimeManager,
    mut principal: Principal,
) -> Result<Principal, ApiFailure> {
    let space = principal.space_id.as_deref().ok_or_else(|| {
        ApiFailure::new(
            ErrorCode::ForbiddenSpace,
            "Select an explicitly granted space",
        )
    })?;
    let storage = runtime
        .current_storage()
        .await
        .ok_or_else(|| ApiFailure::new(ErrorCode::NotReady, "Storage is not configured"))?;
    if !storage.recovery_status().ready {
        return Err(ApiFailure::new(
            ErrorCode::NotReady,
            "Current service grants are unavailable during storage recovery",
        ));
    }
    let grant = SpaceGrantRepository::new(storage)
        .get(space, &principal.subject)
        .await?
        .filter(|grant| grant.active)
        .ok_or_else(|| {
            ApiFailure::new(
                ErrorCode::ForbiddenSpace,
                "No active service grant in the selected space",
            )
        })?;
    principal.scopes = intersect_scopes(&principal.scopes, &grant.scopes);
    Ok(principal)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use elephant_storage::{Repository, SpaceConfigRepository, StorageClient};
    use elephant_types::records::SpaceConfig;
    use elephant_types::space_grant::SpaceGrant;
    use elephant_types::{RecordId, TableName};
    use std::collections::BTreeSet;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
    async fn each_request_observes_current_revocation_and_scope_attenuation(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let storage = StorageClient::new_embedded().await?;
        let config: SpaceConfig = serde_json::from_value(serde_json::json!({
            "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"alpha",
            "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"operator",
            "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
            "provenance":{},"subjects":[],"subjects_state":"complete",
            "embedding_provider":"fake","embedding_model":"fixture","embedding_dims":3,
            "purpose":null,"skill":null
        }))?;
        SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await?;
        let runtime = RuntimeManager::new_with_storage_for_tests(storage.clone());
        let principal = Principal {
            subject: "service".into(),
            space_id: Some("alpha".into()),
            spaces: Some(vec!["alpha".into(), "beta".into()]),
            scopes: vec!["*".into()],
            ..Principal::default()
        };
        assert!(attenuate(&runtime, principal.clone()).await.is_err());
        let repo = SpaceGrantRepository::new(storage);
        let grant = repo
            .replace(
                SpaceGrant {
                    space_id: "alpha".into(),
                    subject: "service".into(),
                    generation: String::new(),
                    active: true,
                    scopes: BTreeSet::from(["read:source".into(), "manage:grant".into()]),
                    updated_by: "operator".into(),
                    updated_at: Utc::now(),
                },
                None,
            )
            .await?;
        let permitted = attenuate(&runtime, principal.clone())
            .await
            .map_err(|_| "grant denied")?;
        assert_eq!(permitted.scopes, vec!["read:source"]);
        let mut revoked = grant.clone();
        revoked.active = false;
        repo.replace(revoked, Some(&grant.generation)).await?;
        assert!(attenuate(&runtime, principal).await.is_err());
        Ok(())
    }
}
