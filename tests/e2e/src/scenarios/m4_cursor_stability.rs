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
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
    Router,
};
use tower::ServiceExt;

use elephant_storage::{EntityRepository, Repository};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::EntityType;
use elephant_types::records::Entity;
use elephant_types::RecordId;

use elephant_api::auth::Principal as AuthPrincipal;
use elephant_api::authz::AuthzService;
use elephant_api::routes::entities::{
    entity_routes, AppState as EntitiesState, EntitySearchResponse,
};

use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_m4_cursor_stability_authorized_total() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let repo = EntityRepository::new(harness.client.clone());
    let space_id = "Space_X";

    // Create 50 team-visible and 50 private entities.
    for i in 0..100 {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, space_id.to_string(), "P_sys".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 1,
            labels: if i < 50 {
                vec!["vis:team".to_string()]
            } else {
                vec!["vis:private".to_string()]
            },
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let entity = Entity::new(base, EntityType::new("person"), format!("Entity {i}"));
        repo.create(entity).await?;
    }

    let authz = Arc::new(AuthzService::new());
    let state = Arc::new(EntitiesState::new(harness.client.clone(), authz));
    let app = entity_routes()
        .layer(middleware::from_fn(mock_auth))
        .with_state(state);

    let request = serde_json::json!({
        "space_id": space_id,
        "limit": 10
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/entities/search")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&request)?))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let result: EntitySearchResponse = serde_json::from_slice(&body)?;

    assert_eq!(result.results.len(), 10);
    assert_eq!(result.total, 50);
    for entity in &result.results {
        assert!(
            entity
                .base
                .security
                .labels
                .contains(&"vis:team".to_string()),
            "CRITICAL: unauthorized label leaked in search results"
        );
    }

    Ok(())
}

async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
    let mut req = req;
    req.extensions_mut().insert(AuthPrincipal {
        subject: "P_admin".to_string(),
        space_id: Some("Space_X".to_string()),
        scopes: vec!["read:entity".to_string()],
        security_level: 2,
        security_labels: vec!["vis:team".to_string()],
        ..AuthPrincipal::default()
    });
    next.run(req).await
}
