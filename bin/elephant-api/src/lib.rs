//! Elephant API server implementation.
//!
//! This crate provides the HTTP API server for the Elephant knowledge system,
//! including authentication, authorization, and route handlers.

// Preserve concrete Send futures in the nested MCP test pipeline.
#![cfg_attr(test, recursion_limit = "256")]
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
mod mcp_output;
mod private_primary_evidence;
pub mod public_error;
pub mod redaction;
pub mod routes;
pub mod runtime;
mod runtime_access;
mod space_grants;

#[cfg(test)]
mod authorization_surface_tests;

#[cfg(test)]
mod managed_bootstrap_routes_tests;

pub use auth::{jwt_auth, AuthConfig, AuthState, Principal};
pub use authz::{AuthzService, AuthzState};
pub use redaction::{redact_doc, redact_source};

use routes::assertions::AssertionsState;
use routes::docs::DocsState;
use routes::entities::AppState as EntitiesState;
use routes::events::EventsState;
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

/// Compose the production REST authentication, runtime and current-grant gates.
///
/// This is also the transport boundary for embedded hosts and independent
/// qualification tests; it neither starts a listener nor duplicates policy logic.
/// State-bound record handlers still perform their own scope and ABAC checks.
///
/// # Errors
/// Rejects invalid server authentication configuration before exposing routes.
pub fn authenticated_data_routes(
    routes: Router,
    auth_state: AuthState,
    runtime: Arc<runtime::RuntimeManager>,
) -> anyhow::Result<Router> {
    auth_state
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    Ok(routes
        .layer(middleware::from_fn_with_state(
            runtime.clone(),
            space_grants::enforce_space_grant,
        ))
        .layer(middleware::from_fn_with_state(
            runtime,
            runtime_access::enforce_runtime_access,
        ))
        .layer(middleware::from_fn_with_state(auth_state, jwt_auth)))
}

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

/// Build the existing data route graph for the currently configured storage.
/// The `RuntimeManager` remains the storage authority; no startup snapshot is retained.
#[allow(clippy::too_many_lines)]
#[expect(
    clippy::needless_pass_by_value,
    clippy::redundant_clone,
    clippy::significant_drop_tightening,
    reason = "Retain route-state owners and clone lifetimes through authenticated graph construction."
)]
fn managed_storage_routes(
    storage_client: StorageClient,
    authz_service: Arc<AuthzService>,
    auth_state: AuthState,
    runtime: Arc<runtime::RuntimeManager>,
) -> anyhow::Result<Router> {
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
    let system_state = Arc::new(
        SystemState::new(storage_client.clone(), authz_service.clone())
            .with_runtime(runtime.clone()),
    );
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
        .merge(routes::grants::grant_routes().with_state(Arc::new(
            routes::grants::GrantsState::new(storage_client.clone(), authz_service.clone()),
        )))
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
        .merge(routes::source_routes().with_state(sources_state))
        .merge(
            routes::public_sources::public_source_routes().with_state(Arc::new(
                routes::public_sources::PublicSourcesState::new(
                    storage_client.clone(),
                    authz_service.clone(),
                    runtime.clone(),
                ),
            )),
        )
        .merge(
            routes::space_status::space_status_routes().with_state(Arc::new(
                routes::space_status::SpaceStatusState::new(
                    storage_client.clone(),
                    runtime.clone(),
                    authz_service.clone(),
                ),
            )),
        )
        .merge(routes::processing::processing_routes().with_state(Arc::new(
            routes::processing::ProcessingState::new(storage_client.clone(), authz_service.clone()),
        )))
        .merge(routes::scoped_search::router(
            storage_client.clone(),
            authz_service.clone(),
        ));

    authenticated_data_routes(authenticated_routes, auth_state, runtime)
}

async fn validate_managed_storage(storage_client: &StorageClient) -> anyhow::Result<()> {
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

    if storage_client.recovery_status().ready {
        let tables = storage_client.list_tables().await?;
        info!("Found {} tables in database", tables.len());
    }

    Ok(())
}

struct ManagedDataRoutes {
    runtime: Arc<runtime::RuntimeManager>,
    auth_state: AuthState,
    authz_service: Arc<AuthzService>,
    initialized: tokio::sync::OnceCell<()>,
}

impl ManagedDataRoutes {
    async fn dispatch(&self, request: axum::extract::Request) -> axum::response::Response {
        use axum::response::IntoResponse as _;
        use tower::ServiceExt as _;

        let Some(storage) = self.runtime.current_storage().await else {
            // Bootstrap exposes only the existing runtime, MCP and health routes.
            return axum::http::StatusCode::NOT_FOUND.into_response();
        };
        if let Err(error) = self
            .initialized
            .get_or_try_init(|| validate_managed_storage(&storage))
            .await
        {
            warn!(%error, "Managed data route initialization failed");
            return public_error::ApiFailure::new(
                public_error::ErrorCode::NotReady,
                "Data routes are not ready",
            )
            .into_response();
        }
        // Route state is bound to this request's current storage. Caching a
        // startup Router would also retain stale storage after reconfiguration.
        let routes = match managed_storage_routes(
            storage,
            self.authz_service.clone(),
            self.auth_state.clone(),
            self.runtime.clone(),
        ) {
            Ok(routes) => routes,
            Err(error) => {
                warn!(%error, "Managed data route configuration failed");
                return public_error::ApiFailure::new(
                    public_error::ErrorCode::NotReady,
                    "Data routes are not ready",
                )
                .into_response();
            }
        };
        match routes.oneshot(request).await {
            Ok(response) => response,
            Err(infallible) => match infallible {},
        }
    }
}

/// Construct the actual managed server router, including bootstrap activation.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original state owner lifetimes and dispatch future layout."
)]
async fn managed_api_router(
    auth_config: AuthConfig,
    runtime: Arc<runtime::RuntimeManager>,
) -> anyhow::Result<Router> {
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let auth_state: AuthState = Arc::new(auth_config.clone());
    let authz_service = Arc::new(authz::AuthzService::new());
    let runtime_state = Arc::new(RuntimeState::new(runtime.clone()));

    let app = Router::new()
        .merge(
            routes::runtime_routes()
                .with_state(runtime_state)
                .layer(middleware::from_fn_with_state(
                    runtime.clone(),
                    space_grants::enforce_space_grant,
                ))
                .layer(middleware::from_fn_with_state(auth_state.clone(), jwt_auth)),
        )
        .merge(
            routes::readiness::readiness_routes()
                .with_state(runtime.clone())
                .layer(middleware::from_fn_with_state(
                    runtime.clone(),
                    space_grants::enforce_space_grant,
                ))
                .layer(middleware::from_fn_with_state(auth_state.clone(), jwt_auth)),
        )
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

    let data = Arc::new(ManagedDataRoutes {
        runtime: runtime.clone(),
        auth_state,
        authz_service,
        initialized: tokio::sync::OnceCell::new(),
    });
    // Preserve configured-startup validation before the listener is bound.
    if let Some(storage) = runtime.current_storage().await {
        data.initialized
            .get_or_try_init(|| validate_managed_storage(&storage))
            .await?;
    }
    Ok(app.merge(routes::health_routes()).fallback(move |request| {
        let data = data.clone();
        async move { data.dispatch(request).await }
    }))
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
    let private_evidence =
        elephant_storage::private_primary_evidence::PrivatePrimaryEvidence::from_environment()?;
    let private_storage = private_primary_evidence::StorageProvider::Managed(runtime.clone());
    let app = managed_api_router(auth_config, runtime.clone()).await?;
    let addr = config.socket_addr()?;
    let listener = TcpListener::bind(addr).await?;
    info!("Listening on {}", addr);
    runtime.start_workers_after_bind().await?;
    let _post_bind_refresh = runtime.clone().schedule_post_bind_readiness_refresh();

    private_primary_evidence::serve(private_evidence, private_storage, async move {
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
    })
    .await
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
#[expect(
    clippy::significant_drop_tightening,
    reason = "Preserve the original state owner lifetimes and dispatch future layout."
)]
pub async fn run_api_with_client_and_shutdown(
    config: config::ApiConfig,
    auth_config: AuthConfig,
    storage_client: StorageClient,
    shutdown_rx: Option<watch::Receiver<bool>>,
) -> anyhow::Result<()> {
    auth_config
        .validate_server_config()
        .map_err(anyhow::Error::msg)?;
    let private_evidence =
        elephant_storage::private_primary_evidence::PrivatePrimaryEvidence::from_environment()?;
    let private_storage = private_primary_evidence::StorageProvider::Fixed(storage_client.clone());
    if storage_client.recovery_status().ready {
        elephant_migrations::verify_schema(&storage_client)
            .await
            .map_err(anyhow::Error::msg)?;
    }
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

    if storage_client.recovery_status().ready {
        let tables = storage_client.list_tables().await?;
        info!("Found {} tables in database", tables.len());
    }

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
    let runtime = Arc::new(runtime::RuntimeManager::new_preconfigured(
        storage_client.clone(),
    ));
    let system_state = Arc::new(
        SystemState::new(storage_client.clone(), authz_service.clone())
            .with_runtime(runtime.clone()),
    );
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
    let mcp_state = Arc::new(mcp::McpState::new_managed(
        runtime.clone(),
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
        .merge(routes::grants::grant_routes().with_state(Arc::new(
            routes::grants::GrantsState::new(storage_client.clone(), authz_service.clone()),
        )))
        .merge(routes::entity_routes().with_state(entity_state))
        .merge(routes::rel_routes().with_state(rel_state))
        .merge(routes::timeline_routes().with_state(timeline_state))
        .merge(routes::doc_routes().with_state(docs_state))
        .merge(routes::source_routes().with_state(sources_state))
        .merge(
            routes::public_sources::public_source_routes().with_state(Arc::new(
                routes::public_sources::PublicSourcesState::new(
                    storage_client.clone(),
                    authz_service.clone(),
                    runtime.clone(),
                ),
            )),
        )
        .merge(
            routes::space_status::space_status_routes().with_state(Arc::new(
                routes::space_status::SpaceStatusState::new(
                    storage_client.clone(),
                    runtime.clone(),
                    authz_service.clone(),
                ),
            )),
        )
        .merge(routes::processing::processing_routes().with_state(Arc::new(
            routes::processing::ProcessingState::new(storage_client.clone(), authz_service.clone()),
        )))
        .merge(routes::scoped_search::router(
            storage_client.clone(),
            authz_service.clone(),
        ))
        .merge(routes::identity_routes().with_state(identity_state))
        .merge(routes::system_routes().with_state(system_state))
        .merge(routes::events_routes().with_state(events_state))
        .merge(routes::assertion_routes().with_state(assertions_state))
        .merge(routes::predicate_routes().with_state(predicates_state))
        .merge(routes::space_routes().with_state(space_state))
        .merge(routes::truth_slot_routes().with_state(truth_slots_state))
        .merge(routes::knowledge_view_routes().with_state(knowledge_views_state))
        .merge(routes::work_item_routes().with_state(work_items_state));
    let authenticated_routes =
        authenticated_data_routes(authenticated_routes, auth_state.clone(), runtime.clone())?;

    let app = Router::new()
        .merge(
            routes::readiness::readiness_routes()
                .with_state(runtime.clone())
                .layer(middleware::from_fn_with_state(
                    runtime,
                    space_grants::enforce_space_grant,
                ))
                .layer(middleware::from_fn_with_state(auth_state, jwt_auth)),
        )
        .merge(routes::health_routes())
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

    private_primary_evidence::serve(private_evidence, private_storage, async move {
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
    })
    .await
}

#[cfg(test)]
mod startup_security_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    // An invalid host ensures a regression cannot leave a listener running.
    // Authentication must fail first, before routing, binding or serving.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test oracle, fixture owners, and future lifetimes intact."
    )]
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
