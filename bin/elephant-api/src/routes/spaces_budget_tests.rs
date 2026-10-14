#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use axum::{
    body::{to_bytes, Body},
    http::Request,
    middleware, Router,
};
use elephant_storage::space_grant_repository::SpaceGrantRepository;
use elephant_storage::{Repository, SpaceConfigRepository, StorageClient};
use elephant_types::{
    records::{
        base::{BaseFields, SubjectsState},
        SpaceConfig,
    },
    space_grant::SpaceGrant,
    RecordId, TableName,
};
use serde_json::Value;
use tower::ServiceExt;

async fn fixture() -> (StorageClient, Arc<crate::runtime::RuntimeManager>) {
    let storage = StorageClient::new_embedded().await.unwrap();
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "alpha".into(),
        "fixture".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    base.security.level = 2;
    SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base,
            processing: None,
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 3,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    let runtime = Arc::new(crate::runtime::RuntimeManager::new_preconfigured(
        storage.clone(),
    ));
    (storage, runtime)
}

fn principal() -> AuthPrincipal {
    AuthPrincipal {
        subject: "operator".into(),
        space_id: Some("alpha".into()),
        scopes: vec!["read:space_config".into()],
        security_level: 5,
        ..AuthPrincipal::default()
    }
}

fn app(
    storage: StorageClient,
    runtime: Arc<crate::runtime::RuntimeManager>,
    principal: AuthPrincipal,
    configured: bool,
) -> Router {
    let state = SpaceApiState::new(storage, Arc::new(crate::AuthzService::new()));
    let state = if configured {
        state.with_runtime(runtime.clone())
    } else {
        state
    };
    super::super::space_routes()
        .with_state(Arc::new(state))
        .layer(middleware::from_fn_with_state(
            runtime,
            crate::space_grants::enforce_space_grant,
        ))
        .layer(Extension(principal))
}

async fn request(app: Router) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/budget")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body =
        serde_json::from_slice(&to_bytes(response.into_body(), 100_000).await.unwrap()).unwrap();
    (status, body)
}

#[tokio::test]
async fn budget_endpoint_reports_selected_space_and_preserves_scope_record_checks() {
    let (storage, runtime) = fixture().await;
    let (code, body) = request(app(storage.clone(), runtime.clone(), principal(), true)).await;
    assert_eq!(code, StatusCode::OK);
    assert_eq!(body["space_id"], "alpha");
    assert_eq!(body["today"]["graph"]["limit_units"], 50.0);
    assert_eq!(body["today"]["graph"]["remaining_items"], 10_000);
    assert_eq!(body["runtime"]["managed_workers_running"], false);
    assert_eq!(body["headroom_is_reservation"], false);
    for denied in [
        AuthPrincipal {
            scopes: vec!["read:status".into()],
            ..principal()
        },
        AuthPrincipal {
            security_level: 0,
            ..principal()
        },
        AuthPrincipal {
            space_id: None,
            ..principal()
        },
    ] {
        let (code, body) = request(app(storage.clone(), runtime.clone(), denied, true)).await;
        assert_eq!(code, StatusCode::FORBIDDEN);
        assert!(body.get("today").is_none());
    }
    assert_eq!(
        request(app(storage, runtime, principal(), false)).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn budget_endpoint_observes_current_service_grant_attenuation_and_revocation() {
    let (storage, runtime) = fixture().await;
    let caller = AuthPrincipal {
        spaces: Some(vec!["alpha".into()]),
        ..principal()
    };
    let grants = SpaceGrantRepository::new(storage.clone());
    let mut grant = grants
        .replace(
            SpaceGrant {
                space_id: "alpha".into(),
                subject: caller.subject.clone(),
                generation: String::new(),
                active: true,
                scopes: std::iter::once("read:space_config".into()).collect(),
                updated_by: "fixture".into(),
                updated_at: chrono::Utc::now(),
            },
            None,
        )
        .await
        .unwrap();
    let router = app(storage, runtime, caller, true);
    assert_eq!(request(router.clone()).await.0, StatusCode::OK);
    grant.scopes = std::iter::once("read:status".into()).collect();
    let generation = grant.generation.clone();
    grant = grants.replace(grant, Some(&generation)).await.unwrap();
    assert_eq!(request(router.clone()).await.0, StatusCode::FORBIDDEN);
    grant.active = false;
    let generation = grant.generation.clone();
    grants.replace(grant, Some(&generation)).await.unwrap();
    drop(grants);
    assert_eq!(request(router).await.0, StatusCode::FORBIDDEN);
}
