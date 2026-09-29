//! Elephant API server implementation.
//!
//! This crate provides the HTTP API server for the Elephant knowledge system,
//! including authentication, authorization, and route handlers.

#![allow(missing_docs)]

use std::sync::Arc;

use axum::http::Method;
use axum::{middleware, Router};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tower_http::cors::{Any, CorsLayer};
use tracing::{info, warn};

use elephant_extensions::{enabled_extension_names, load_runtime_reference_registry_from_env};
use elephant_storage::StorageClient;

pub mod auth;
pub mod authz;
pub mod config;
pub mod mcp;
pub mod redaction;
pub mod routes;
pub mod runtime;

pub use auth::{jwt_auth, AuthConfig, AuthState, Principal};
pub use authz::{AuthzService, AuthzState};
pub use redaction::{redact_doc, redact_source};

use routes::assertions::AssertionsState;
use routes::docs::DocsState;
use routes::entities::AppState as EntitiesState;
use routes::events::EventsState;
use routes::health::HealthState;
use routes::identity::IdentityState;
use routes::knowledge_views::KnowledgeViewsState;
use routes::predicates::PredicatesState;
use routes::relationships::RelationshipsState;
use routes::runtime::RuntimeState;
use routes::sources::SourcesState;
use routes::spaces::SpaceApiState;
use routes::system::SystemState;
use routes::timeline::TimelineState;
use routes::truth_slots::TruthSlotsState;
use routes::work_items::WorkItemsState;

/// Runs the API server with an already-constructed storage client.
///
/// # Errors
///
/// Returns an error if state initialization, socket binding, or serving fails.
#[allow(clippy::too_many_lines)]
pub async fn run_api_with_client(
    config: config::ApiConfig,
    auth_config: AuthConfig,
    storage_client: StorageClient,
) -> anyhow::Result<()> {
    run_api_with_client_and_shutdown(config, auth_config, storage_client, None).await
}

/// Runs the API server against a managed runtime that may begin in bootstrap
/// mode without configured storage.
///
/// # Errors
///
/// Returns an error if route state construction, socket binding, or serving
/// fails.
#[allow(clippy::too_many_lines)]
pub async fn run_api_with_managed_runtime(
    config: config::ApiConfig,
    auth_config: AuthConfig,
    runtime: Arc<runtime::RuntimeManager>,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> anyhow::Result<()> {
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let auth_state: AuthState = Arc::new(auth_config.clone());
    let authz_service = Arc::new(authz::AuthzService::new());
    let runtime_state = Arc::new(RuntimeState::new(runtime.clone()));

    let mut app = Router::new()
        .merge(routes::runtime_routes().with_state(runtime_state))
        .merge(mcp::mcp_routes(Arc::new(mcp::McpState::new_managed(
            runtime.clone(),
            auth_config.clone(),
        ))))
        .merge(mcp::mcp_agent_routes(Arc::new(mcp::McpState::new_managed(
            runtime.clone(),
            auth_config.clone(),
        ))))
        .merge(mcp::mcp_maintenance_routes(Arc::new(
            mcp::McpState::new_managed(runtime.clone(), auth_config.clone()),
        )))
        .merge(mcp::mcp_full_routes(Arc::new(mcp::McpState::new_managed(
            runtime.clone(),
            auth_config.clone(),
        ))))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::PATCH,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers(Any),
        );

    let runtime_status = runtime.get_status().await;
    if let Some(storage_client) = runtime.current_storage().await {
        let configured_extensions =
            enabled_extension_names(std::env::var("ELEPHANT_EXTENSIONS").ok().as_deref());
        match load_runtime_reference_registry_from_env() {
            Ok(extension_registry) => {
                if extension_registry.is_empty() {
                    info!("Loaded 0 extension merge rewrite declarations");
                } else {
                    info!(
                        "Loaded {} extension merge rewrite declaration(s)",
                        extension_registry.len()
                    );
                }
            }
            Err(err) if configured_extensions.is_empty() => {
                warn!("Skipping extension merge rewrite declarations: {err}");
            }
            Err(err) => {
                return Err(anyhow::anyhow!(
                    "failed to load extension merge rewrite declarations for configured extensions {configured_extensions:?}: {err}",
                ));
            }
        }

        let tables = storage_client.list_tables().await?;
        info!("Found {} tables in database", tables.len());

        let entity_state = Arc::new(EntitiesState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let rel_state = Arc::new(RelationshipsState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let timeline_state = Arc::new(TimelineState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let identity_state = Arc::new(IdentityState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let system_state = Arc::new(SystemState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let health_state = Arc::new(HealthState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let events_state = Arc::new(EventsState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let assertions_state = Arc::new(AssertionsState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let predicates_state = Arc::new(PredicatesState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let truth_slots_state = Arc::new(TruthSlotsState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let knowledge_views_state = Arc::new(KnowledgeViewsState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let work_items_state = Arc::new(WorkItemsState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));
        let space_state = Arc::new(SpaceApiState::new(
            storage_client.clone(),
            authz_service.clone(),
        ));

        let mut authenticated_routes = Router::new()
            .merge(routes::entity_routes().with_state(entity_state))
            .merge(routes::rel_routes().with_state(rel_state))
            .merge(routes::timeline_routes().with_state(timeline_state))
            .merge(routes::identity_routes().with_state(identity_state))
            .merge(routes::system_routes().with_state(system_state))
            .merge(routes::events_routes().with_state(events_state))
            .merge(routes::assertion_routes().with_state(assertions_state))
            .merge(routes::predicate_routes().with_state(predicates_state))
            .merge(routes::space_routes().with_state(space_state))
            .merge(routes::truth_slot_routes().with_state(truth_slots_state))
            .merge(routes::knowledge_view_routes().with_state(knowledge_views_state))
            .merge(routes::work_item_routes().with_state(work_items_state));

        let docs_state = Arc::new(DocsState::new_with_runtime(
            &storage_client,
            authz_service.clone(),
            Some(runtime.clone()),
        ));
        let sources_state = Arc::new(SourcesState::new_with_runtime(
            storage_client.clone(),
            authz_service.clone(),
            Some(runtime.clone()),
        ));
        authenticated_routes = authenticated_routes
            .merge(routes::doc_routes().with_state(docs_state))
            .merge(routes::source_routes().with_state(sources_state));

        if !runtime_status.semantic_ingest_ready {
            warn!(
                "Semantic ingest is not ready; REST ingest endpoints will return runtime-not-ready"
            );
        }

        authenticated_routes =
            authenticated_routes.layer(middleware::from_fn_with_state(auth_state, jwt_auth));

        app = app
            .merge(routes::health_routes().with_state(health_state))
            .merge(authenticated_routes);
    }

    let addr = config.socket_addr()?;
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);
    let _post_bind_refresh = runtime.clone().schedule_post_bind_readiness_refresh();

    if let Some(mut shutdown_rx) = shutdown_rx {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.changed().await;
            })
            .await?;
    } else {
        axum::serve(listener, app).await?;
    }
    Ok(())
}

/// Runs the API server with an already-constructed storage client and an
/// optional shutdown receiver for graceful embedded supervisor drain.
///
/// # Errors
///
/// Returns an error if extension manifest loading fails for configured
/// extensions, if route state cannot be constructed, or if the underlying
/// server exits with an error.
#[allow(clippy::too_many_lines)]
pub async fn run_api_with_client_and_shutdown(
    config: config::ApiConfig,
    auth_config: AuthConfig,
    storage_client: StorageClient,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> anyhow::Result<()> {
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let auth_state: AuthState = Arc::new(auth_config.clone());
    info!(
        "Auth configuration: skip_auth={}, algorithm={:?}",
        auth_config.skip_auth, auth_config.algorithm
    );

    let authz_service = Arc::new(authz::AuthzService::new());

    let configured_extensions =
        enabled_extension_names(std::env::var("ELEPHANT_EXTENSIONS").ok().as_deref());
    match load_runtime_reference_registry_from_env() {
        Ok(extension_registry) => {
            if extension_registry.is_empty() {
                info!("Loaded 0 extension merge rewrite declarations");
            } else {
                info!(
                    "Loaded {} extension merge rewrite declaration(s)",
                    extension_registry.len()
                );
            }
        }
        Err(err) if configured_extensions.is_empty() => {
            warn!("Skipping extension merge rewrite declarations: {err}");
        }
        Err(err) => {
            return Err(anyhow::anyhow!(
                "failed to load extension merge rewrite declarations for configured extensions {configured_extensions:?}: {err}",
            ));
        }
    }

    let tables = storage_client.list_tables().await?;
    info!("Found {} tables in database", tables.len());

    let entity_state = Arc::new(EntitiesState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let rel_state = Arc::new(RelationshipsState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let timeline_state = Arc::new(TimelineState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let docs_state = Arc::new(DocsState::new(&storage_client, authz_service.clone()));
    let sources_state = Arc::new(SourcesState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let identity_state = Arc::new(IdentityState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let system_state = Arc::new(SystemState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let health_state = Arc::new(HealthState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let events_state = Arc::new(EventsState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let assertions_state = Arc::new(AssertionsState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let predicates_state = Arc::new(PredicatesState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let mcp_state = Arc::new(mcp::McpState::new(
        storage_client.clone(),
        auth_config.clone(),
    ));
    let mcp_agent_state = mcp_state.clone();
    let truth_slots_state = Arc::new(TruthSlotsState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let knowledge_views_state = Arc::new(KnowledgeViewsState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let work_items_state = Arc::new(WorkItemsState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));
    let space_state = Arc::new(SpaceApiState::new(
        storage_client.clone(),
        authz_service.clone(),
    ));

    let authenticated_routes = Router::new()
        .merge(routes::entity_routes().with_state(entity_state))
        .merge(routes::rel_routes().with_state(rel_state))
        .merge(routes::timeline_routes().with_state(timeline_state))
        .merge(routes::doc_routes().with_state(docs_state))
        .merge(routes::source_routes().with_state(sources_state))
        .merge(routes::identity_routes().with_state(identity_state))
        .merge(routes::system_routes().with_state(system_state))
        .merge(routes::events_routes().with_state(events_state))
        .merge(routes::assertion_routes().with_state(assertions_state))
        .merge(routes::predicate_routes().with_state(predicates_state))
        .merge(routes::space_routes().with_state(space_state))
        .merge(routes::truth_slot_routes().with_state(truth_slots_state))
        .merge(routes::knowledge_view_routes().with_state(knowledge_views_state))
        .merge(routes::work_item_routes().with_state(work_items_state))
        .layer(middleware::from_fn_with_state(auth_state, jwt_auth));

    let app = Router::new()
        .merge(routes::health_routes().with_state(health_state))
        .merge(authenticated_routes)
        .merge(mcp::mcp_routes(mcp_state))
        .merge(mcp::mcp_agent_routes(mcp_agent_state.clone()))
        .merge(mcp::mcp_maintenance_routes(mcp_agent_state.clone()))
        .merge(mcp::mcp_full_routes(mcp_agent_state))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods([
                    Method::GET,
                    Method::POST,
                    Method::PUT,
                    Method::PATCH,
                    Method::DELETE,
                    Method::OPTIONS,
                ])
                .allow_headers(Any),
        );

    let addr = config.socket_addr()?;
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);

    if let Some(mut shutdown_rx) = shutdown_rx {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.changed().await;
            })
            .await?;
    } else {
        axum::serve(listener, app).await?;
    }
    Ok(())
}

#[cfg(test)]
mod startup_security_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    // An invalid host ensures a regression cannot leave a listener running.
    // Authentication must fail first, before routing, binding or serving.
    #[tokio::test]
    async fn both_server_entrypoints_reject_missing_signing_secret() {
        let config = config::ApiConfig::default().with_host("invalid-host");
        let storage = StorageClient::new_embedded().await.unwrap();
        let runtime = Arc::new(runtime::RuntimeManager::new_with_storage_for_tests(
            storage.clone(),
        ));
        let managed =
            run_api_with_managed_runtime(config.clone(), AuthConfig::default(), runtime, None)
                .await
                .unwrap_err();
        assert!(managed.to_string().contains("JWT_SECRET"));
        let direct = run_api_with_client_and_shutdown(config, AuthConfig::default(), storage, None)
            .await
            .unwrap_err();
        assert!(direct.to_string().contains("JWT_SECRET"));
    }
}
