//! Exercise the real authenticated route stack and stored grant authority.
#![allow(clippy::unwrap_used, clippy::too_many_lines)]

use super::*;
use axum::{
    body::{to_bytes, Body},
    http::{Method, Request, StatusCode},
    Router,
};
use elephant_storage::{EntityRepository, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::{
    records::{
        base::{BaseFields, Security},
        Entity, EntityType,
    },
    RecordId, TableName,
};
use serde_json::{json, Value};
use tower::ServiceExt as _;

const SECRET: &str = "synthetic-only-http-bootstrap-signing-secret";

fn claims(space: &str, policy: &str, level: i32) -> Claims {
    let now = chrono::Utc::now().timestamp();
    serde_json::from_value(json!({
        "sub":"local-space-bootstrap", "iat":now, "exp":now+900,
        "space_id":space, "spaces":[space], "bootstrap_space":space,
        "scopes":["read:space_config","write:space_config","manage:grant"],
        "clearances":[{"policy_id":policy,"max_level":level,"labels":[],"handling_allow":[]}]
    }))
    .unwrap()
}

fn signed(claims: &Claims) -> String {
    jsonwebtoken::encode(
        &jsonwebtoken::Header::default(),
        claims,
        &jsonwebtoken::EncodingKey::from_secret(SECRET.as_bytes()),
    )
    .unwrap()
}

fn router(storage: StorageClient) -> Router {
    let authz = Arc::new(crate::AuthzService::new());
    let routes = crate::routes::spaces::space_routes()
        .with_state(Arc::new(crate::routes::spaces::SpaceApiState::new(
            storage.clone(),
            authz.clone(),
        )))
        .merge(crate::routes::grants::grant_routes().with_state(Arc::new(
            crate::routes::grants::GrantsState::new(storage.clone(), authz),
        )));
    crate::authenticated_data_routes(
        routes,
        Arc::new(AuthConfig {
            secret: SECRET.into(),
            ..AuthConfig::default()
        }),
        Arc::new(crate::runtime::RuntimeManager::new_with_storage_for_tests(
            storage,
        )),
    )
    .unwrap()
}

async fn call_response(
    app: &Router,
    token: &str,
    method: Method,
    path: &str,
    space: &str,
    body: Value,
) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method(method)
                .uri(path)
                .header("Authorization", format!("Bearer {token}"))
                .header("X-Space-Id", space)
                .header("Content-Type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn call(
    app: &Router,
    token: &str,
    method: Method,
    path: &str,
    space: &str,
    body: Value,
) -> (StatusCode, Value) {
    let response = call_response(app, token, method, path, space, body).await;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 128 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

fn config(space: &str, policy: &str, level: i32) -> Value {
    json!({"name":space,"security":{"policy_id":policy,"level":level,"labels":[],"handling":[]},
        "embedding_provider":"fake","embedding_model":"bootstrap-test","embedding_dims":8})
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn classified_bootstrap_creates_exact_space_and_revocable_service_grant() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let app = router(storage.clone());
    for (space, policy, level) in [
        ("household", "household-policy", 3),
        ("business", "business-policy", 7),
    ] {
        let bootstrap_claims = claims(space, policy, level);
        let bootstrap = elephant_auth::service_token::issue_space_bootstrap(
            SECRET,
            &bootstrap_claims,
            bootstrap_claims.iat,
        )
        .unwrap();
        let (status, created) = call(
            &app,
            &bootstrap,
            Method::POST,
            "/v1/spaces",
            space,
            config(space, policy, level),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{created}");
        assert_eq!(created["security"]["policy_id"], policy);
        assert_eq!(created["security"]["level"], level);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space.into(),
            "fixture".into(),
        );
        base.security = Security {
            policy_id: policy.into(),
            level,
            labels: vec![],
            handling: vec![],
        };
        let entity = Entity::new(
            base,
            EntityType::new("person"),
            format!("classified-{space}-entity"),
        );
        EntityRepository::new(storage.clone())
            .create(entity)
            .await
            .unwrap();
        let mut missing_grant_claims = bootstrap_claims.clone();
        missing_grant_claims.bootstrap_space = None;
        missing_grant_claims.sub = "not-granted".into();
        missing_grant_claims.scopes = vec!["read:entity".into()];
        let missing_grant = elephant_auth::service_token::issue(
            SECRET,
            &missing_grant_claims,
            true,
            missing_grant_claims.iat,
        )
        .unwrap();
        assert_eq!(
            call(
                &app,
                &missing_grant,
                Method::GET,
                &format!("/api/v1/spaces/{space}/entities"),
                space,
                Value::Null
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        let scopes = json!(["read:space_config", "read:entity"]);
        let grant_path = format!("/v1/spaces/{space}/grants/normal-service");
        let (status, grant) = call(
            &app,
            &bootstrap,
            Method::PUT,
            &grant_path,
            space,
            json!({"active":true,"scopes":scopes,"expected_generation":null}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{grant}");
        assert_eq!(grant["space_id"], space);
        let mut ordinary_claims = bootstrap_claims.clone();
        ordinary_claims.bootstrap_space = None;
        ordinary_claims.sub = "normal-service".into();
        ordinary_claims.scopes = vec!["read:space_config".into(), "read:entity".into()];
        let ordinary = elephant_auth::service_token::issue(
            SECRET,
            &ordinary_claims,
            true,
            ordinary_claims.iat,
        )
        .unwrap();
        let data_path = format!("/api/v1/spaces/{space}/entities");
        let (status, entities) =
            call(&app, &ordinary, Method::GET, &data_path, space, Value::Null).await;
        assert_eq!(status, StatusCode::OK, "{entities}");
        assert!(
            entities
                .to_string()
                .contains(&format!("classified-{space}-entity")),
            "{entities}"
        );
        assert_eq!(
            call(
                &app,
                &bootstrap,
                Method::GET,
                &data_path,
                space,
                Value::Null
            )
            .await
            .0,
            StatusCode::FORBIDDEN
        );
        let (status, revoked) = call(
            &app,
            &bootstrap,
            Method::PUT,
            &grant_path,
            space,
            json!({"active":false,"scopes":scopes,"expected_generation":grant["generation"]}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{revoked}");
        assert_eq!(
            call(&app, &ordinary, Method::GET, &data_path, space, Value::Null)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
        // A bootstrap collection read remains selected-space-only.
        let (status, listed) = call(
            &app,
            &bootstrap,
            Method::GET,
            "/v1/spaces",
            space,
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{listed}");
        let list = listed.as_array().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0]["space_id"], space);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original test oracle and fixture lifetimes intact."
)]
async fn bootstrap_never_bypasses_body_space_or_proposed_and_current_clearance() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let app = router(storage.clone());
    let exact_claims = claims("household", "household-policy", 3);
    let exact = signed(&exact_claims);
    let mut global_claims = exact_claims.clone();
    global_claims.bootstrap_space = None;
    global_claims.space_id = None;
    global_claims.spaces = None;
    global_claims.clearances.clear();
    let global =
        elephant_auth::service_token::issue_global_admin(SECRET, &global_claims, global_claims.iat)
            .unwrap();
    assert_eq!(
        call(
            &app,
            &global,
            Method::POST,
            "/v1/spaces",
            "household",
            config("household", "household-policy", 3)
        )
        .await
        .0,
        StatusCode::FORBIDDEN
    );

    for (body, header) in [
        (config("business", "household-policy", 3), "household"),
        (config("household", "household-policy", 4), "household"),
        (config("household", "business-policy", 3), "household"),
        (config("household", "household-policy", 3), "business"),
    ] {
        assert_eq!(
            call(&app, &exact, Method::POST, "/v1/spaces", header, body)
                .await
                .0,
            StatusCode::FORBIDDEN
        );
    }
    assert!(SpaceConfigRepository::new(storage.clone())
        .get_by_space_id("household")
        .await
        .unwrap()
        .is_none());
    assert!(SpaceConfigRepository::new(storage.clone())
        .get_by_space_id("business")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        call(
            &app,
            &exact,
            Method::POST,
            "/v1/spaces",
            "household",
            config("household", "household-policy", 3)
        )
        .await
        .0,
        StatusCode::CREATED
    );
    let weaker = signed(&claims("household", "household-policy", 2));
    for (method, path, body) in [
        (Method::GET, "/v1/spaces/household", Value::Null),
        (
            Method::POST,
            "/v1/spaces",
            config("household", "household-policy", 2),
        ),
        (
            Method::PUT,
            "/v1/spaces/household",
            json!({"purpose":"changed"}),
        ),
        (
            Method::PUT,
            "/v1/spaces/household/grants/new-service",
            json!({"active":true,"scopes":["read:entity"]}),
        ),
    ] {
        assert_eq!(
            call(&app, &weaker, method, path, "household", body).await.0,
            StatusCode::FORBIDDEN
        );
    }
    // Unknown update fields are rejected by Axum's Json extractor before the
    // handler, with its text rejection body rather than the API JSON envelope.
    // Keep the ordinary authorization responses above strictly JSON-decoded.
    let rejected = call_response(
        &app,
        &exact,
        Method::PUT,
        "/v1/spaces/household",
        "household",
        json!({"security":{"policy_id":"household-policy","level":4,"labels":[],"handling":[]}}),
    )
    .await;
    assert_eq!(rejected.status(), StatusCode::UNPROCESSABLE_ENTITY);
    let body = to_bytes(rejected.into_body(), 128 * 1024).await.unwrap();
    let rejection = std::str::from_utf8(&body).unwrap();
    assert!(
        rejection.contains("unknown field `security`"),
        "{rejection}"
    );
    let stored = SpaceConfigRepository::new(storage)
        .get_by_space_id("household")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.base.security.level, 3);
    assert_eq!(stored.purpose, None);
}

#[test]
fn bootstrap_method_and_matched_path_ceiling_rejects_work_and_unknown_routes() {
    let claims = claims("household", "household-policy", 3);
    for (method, path) in [
        (Method::POST, "/v1/spaces/{space}/processing/reindex/start"),
        (
            Method::POST,
            "/v1/spaces/{space}/processing/reindex/{action}",
        ),
        (Method::GET, "/v1/operations/{id}"),
        (Method::DELETE, "/v1/spaces/{space_id}"),
        (Method::POST, "/mcp"),
        (Method::GET, "/new-route"),
    ] {
        assert!(
            authorize_bootstrap_route(&claims, &method, Some(path), Some("household")).is_err()
        );
    }
    assert!(authorize_bootstrap_route(&claims, &Method::POST, None, Some("household")).is_err());
    assert!(authorize_bootstrap_route(
        &claims,
        &Method::POST,
        Some("/v1/spaces"),
        Some("business")
    )
    .is_err());
}
