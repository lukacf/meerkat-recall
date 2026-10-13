//! System API routes.
//!
//! Implements the following endpoints:
//! - POST `/v1/audit/revert/{audit_id}` - Revert changes from an audit log entry
//! - GET `/v1/capabilities` - Get server capabilities

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use elephant_extensions::{enabled_extension_names, load_runtime_reference_registry_from_env};
use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_dispatcher_with_context, ToolContext,
};
use elephant_storage::public_mutation::PreparedAuditRevert;
use elephant_storage::source_intake::{
    prepare_source_audit_revert, IntakeControl, SourceAuditRevertOutcome,
};
use elephant_storage::{
    parse_id, AuditLogRepository, DocRepository, EntityRepository, IdentityCandidateRepository,
    RelRepository, Repository, SourceRepository, StorageBackendKind, StorageClient,
    SystemEventRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::{AuditLog, Doc, Entity, IdentityCandidate, Rel, Source, SystemEvent};

use crate::routes::entities::ApiError;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;

/// System-specific application state.
#[derive(Clone)]
pub struct SystemState {
    /// Shared storage handle for backend-level admin operations.
    pub storage: Option<StorageClient>,
    pub runtime: Option<Arc<crate::runtime::RuntimeManager>>,
    /// Audit log repository.
    pub audit_repo: Arc<dyn Repository<AuditLog> + Send + Sync>,
    /// Entity repository.
    pub entity_repo: Arc<dyn Repository<Entity> + Send + Sync>,
    /// Relationship repository.
    pub rel_repo: Arc<dyn Repository<Rel> + Send + Sync>,
    /// Event repository.
    pub event_repo: Arc<dyn Repository<SystemEvent> + Send + Sync>,
    /// Document repository.
    pub doc_repo: Arc<dyn Repository<Doc> + Send + Sync>,
    /// Source repository.
    pub source_repo: Arc<dyn Repository<Source> + Send + Sync>,
    /// Identity candidate repository.
    pub identity_candidate_repo: Arc<dyn Repository<IdentityCandidate> + Send + Sync>,
    /// Authorization service.
    pub authz: AuthzState,
}

impl SystemState {
    /// Creates new system state from a storage client.
    #[must_use]
    pub fn new(client: StorageClient, authz: AuthzState) -> Self {
        Self::new_with_repos(
            Some(client.clone()),
            Arc::new(AuditLogRepository::new(client.clone())),
            Arc::new(EntityRepository::new(client.clone())),
            Arc::new(RelRepository::new(client.clone())),
            Arc::new(SystemEventRepository::new(client.clone())),
            Arc::new(DocRepository::new(client.clone())),
            Arc::new(SourceRepository::new(client.clone())),
            Arc::new(IdentityCandidateRepository::new(client)),
            authz,
        )
    }

    #[expect(
        clippy::return_self_not_must_use,
        reason = "Retain the existing constructor and builder annotation contract."
    )]
    pub fn with_runtime(mut self, runtime: Arc<crate::runtime::RuntimeManager>) -> Self {
        self.runtime = Some(runtime);
        self
    }

    /// Creates system state from pre-built repositories (used by fast tests).
    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_repos(
        storage: Option<StorageClient>,
        audit_repo: Arc<dyn Repository<AuditLog> + Send + Sync>,
        entity_repo: Arc<dyn Repository<Entity> + Send + Sync>,
        rel_repo: Arc<dyn Repository<Rel> + Send + Sync>,
        event_repo: Arc<dyn Repository<SystemEvent> + Send + Sync>,
        doc_repo: Arc<dyn Repository<Doc> + Send + Sync>,
        source_repo: Arc<dyn Repository<Source> + Send + Sync>,
        identity_candidate_repo: Arc<dyn Repository<IdentityCandidate> + Send + Sync>,
        authz: AuthzState,
    ) -> Self {
        Self {
            storage,
            runtime: None,
            audit_repo,
            entity_repo,
            rel_repo,
            event_repo,
            doc_repo,
            source_repo,
            identity_candidate_repo,
            authz,
        }
    }
}

/// Revert response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertResponse {
    /// Whether the revert was successful.
    pub success: bool,
    /// Message about the revert.
    pub message: String,
    /// The audit log ID that was reverted.
    pub audit_id: String,
}

/// Server capabilities response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapabilitiesResponse {
    /// Active storage backend kind.
    pub backend: String,
    /// Supported policy schemas in this deployment.
    pub policies: Vec<String>,
    /// Installed extensions (app-owned modules).
    pub extensions: Vec<String>,
    /// Supported feature flags.
    pub features: Vec<String>,
}

/// Snapshot import/export request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// Filesystem path on the Elephant host.
    pub path: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub retention_seconds: Option<u64>,
    #[serde(default)]
    pub quiesce_timeout_seconds: Option<u64>,
}

/// Snapshot import/export response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Backend that handled the request.
    pub backend: String,
    /// Target snapshot path.
    pub path: String,
    /// Human-friendly result message.
    pub message: String,
    pub manifest: elephant_storage::snapshot::SnapshotManifest,
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;
const SNAPSHOT_DIR_ENV: &str = "ELEPHANT_SNAPSHOT_DIR";

#[allow(clippy::result_large_err)]
fn authorize_global_system_scope(
    principal: &AuthPrincipal,
    required_scope: &str,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if principal.bypass_abac {
        return Ok(());
    }

    let has_scope = principal
        .scopes
        .iter()
        .any(|scope| scope == "*" || scope == required_scope);
    if !has_scope {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: format!("Access denied: missing required scope `{required_scope}`"),
                details: None,
            }),
        ));
    }

    if principal.spaces.is_some()
        || principal
            .space_id
            .as_deref()
            .is_some_and(|space_id| !space_id.is_empty())
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access denied: snapshot operations require a global principal"
                    .to_string(),
                details: None,
            }),
        ));
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
fn snapshot_storage(state: &SystemState) -> Result<&StorageClient, (StatusCode, Json<ApiError>)> {
    let Some(storage) = state.storage.as_ref() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(
                "Snapshot operations are unavailable in this server state",
            )),
        ));
    };

    Ok(storage)
}

#[allow(clippy::result_large_err)]
fn resolve_snapshot_host_path(
    requested: &str,
    for_import: bool,
) -> Result<PathBuf, (StatusCode, Json<ApiError>)> {
    let requested = requested.trim();
    if requested.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation("Snapshot path is required")),
        ));
    }

    let configured_root = std::env::var_os(SNAPSHOT_DIR_ENV).map_or_else(
        || {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join(".elephant-snapshots")
        },
        PathBuf::from,
    );
    std::fs::create_dir_all(&configured_root).map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(format!(
                "Failed to prepare snapshot directory: {err}"
            ))),
        )
    })?;
    let root = configured_root.canonicalize().map_err(|err| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(format!(
                "Failed to resolve snapshot directory: {err}"
            ))),
        )
    })?;

    let requested_path = PathBuf::from(requested);
    let candidate = if requested_path.is_absolute() {
        requested_path
    } else {
        root.join(requested_path)
    };

    let resolved = if for_import {
        candidate.canonicalize().map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(format!(
                    "Snapshot path is invalid or missing: {err}"
                ))),
            )
        })?
    } else {
        let file_name = candidate.file_name().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(
                    "Snapshot path must include a file name",
                )),
            )
        })?;
        let parent = candidate.parent().unwrap_or_else(|| FsPath::new("."));
        std::fs::create_dir_all(parent).map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(format!(
                    "Failed to create snapshot parent directory: {err}"
                ))),
            )
        })?;
        let parent = parent.canonicalize().map_err(|err| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(format!(
                    "Failed to resolve snapshot parent directory: {err}"
                ))),
            )
        })?;
        parent.join(file_name)
    };

    if !resolved.starts_with(&root) {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError::validation(format!(
                "Snapshot paths must stay inside {}",
                root.display()
            ))),
        ));
    }

    Ok(resolved)
}

const fn write_scope_for_table(table: TableName) -> Option<&'static str> {
    match table {
        TableName::Entity => Some("write:entity"),
        TableName::Rel => Some("write:relationship"),
        TableName::Doc => Some("write:doc"),
        TableName::Source => Some("write:source"),
        TableName::IdentityCandidate | TableName::EntityIdentityKey | TableName::EntityMerge => {
            Some("write:entity")
        }
        // System/internal tables and v0.8 tables - revert not supported
        TableName::SystemEvent
        | TableName::Event
        | TableName::AuditLog
        | TableName::IdMap
        | TableName::RelEvidence
        | TableName::ChunkEmbedding
        | TableName::SpaceConfig
        | TableName::DocRevision
        | TableName::DocNode
        | TableName::Assertion
        | TableName::Attribute
        | TableName::WorkItem
        | TableName::WorkItemArchive
        | TableName::WorkEdge
        | TableName::WorkRun
        | TableName::WorkRunArchive
        | TableName::WorkArtifact
        | TableName::WorkDeadletter
        | TableName::PredicateRegistry
        | TableName::PredicateAlias
        | TableName::ConflictGroup
        | TableName::TruthSlot
        | TableName::IngestDecision
        | TableName::CostLedgerDaily
        | TableName::Freshness
        | TableName::ReprocessJob
        | TableName::KnowledgeView
        | TableName::KnowledgeViewRevision
        | TableName::KnowledgeViewDefinition
        | TableName::KnowledgeViewDependency
        | TableName::SourceWatch
        | TableName::SourceWatchCheck
        | TableName::MediaAsset
        | TableName::DocChunk => None,
    }
}

/// POST `/v1/audit/revert/{audit_id}` - Revert changes from an audit log entry.
///
/// # Errors
///
/// Returns an error if the audit entry is not found, if authorization fails,
/// or if the revert operation fails.
#[allow(clippy::too_many_lines)]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn revert_audit(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(audit_id): Path<String>,
    Query(intake): Query<IntakeControl>,
) -> ApiResult<RevertResponse> {
    let space_id = crate::routes::require_space_id(&principal)?;
    let storage = state.storage.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::internal("Canonical audit storage unavailable")),
        )
    })?;
    let rid = parse_id(&audit_id, TableName::AuditLog).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(error.to_string())),
        )
    })?;
    let audit = state
        .audit_repo
        .get(&space_id, &rid)
        .await
        .map_err(audit_storage_error)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Audit log", &audit_id)),
            )
        })?;
    state
        .authz
        .authorize_record_read(&principal, &audit, "read:audit")
        .map_err(audit_authz_error)?;
    let required_scope = write_scope_for_table(audit.record.table()).ok_or_else(|| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Revert not supported for record type")),
        )
    })?;
    let source_backed = match audit.record.table() {
        TableName::Source => true,
        TableName::Doc => DocRepository::new(storage.clone())
            .get_with_deleted(&space_id, &audit.record)
            .await
            .map_err(audit_storage_error)?
            .is_some_and(|doc| doc.source.is_some()),
        _ => false,
    };
    if source_backed {
        let prepared = prepare_source_audit_revert(&storage, &space_id, &audit, intake)
            .await
            .map_err(audit_storage_error)?;
        for source in
            std::iter::once(&prepared.current_source).chain(prepared.proposed_source.iter())
        {
            state
                .authz
                .authorize_record_read(&principal, source, "read:source")
                .map_err(audit_authz_error)?;
            state
                .authz
                .authorize_record_write(&principal, source, "write:source")
                .map_err(audit_authz_error)?;
        }
        for doc in prepared
            .current_document
            .iter()
            .chain(prepared.proposed_document.iter())
        {
            state
                .authz
                .authorize_record_read(&principal, doc, "read:doc")
                .map_err(audit_authz_error)?;
            state
                .authz
                .authorize_record_write(&principal, doc, "write:doc")
                .map_err(audit_authz_error)?;
        }
        for doc in &prepared.current_documents {
            state
                .authz
                .authorize_record_read(&principal, doc, "read:doc:metadata")
                .map_err(audit_authz_error)?;
            state
                .authz
                .authorize_record_write(&principal, doc, "write:doc")
                .map_err(audit_authz_error)?;
        }
        if let Some(contact) = &prepared.contact {
            for row in contact.authorizations() {
                state
                    .authz
                    .authorize_record_read(&principal, row, row.read_scope())
                    .map_err(audit_authz_error)?;
                state
                    .authz
                    .authorize_record_write(&principal, row, row.write_scope())
                    .map_err(audit_authz_error)?;
            }
        }
        if let SourceAuditRevertOutcome::Accepted(accepted) = prepared
            .accept(&storage, &principal.subject)
            .await
            .map_err(audit_storage_error)?
        {
            if accepted.needs_processing() {
                elephant_pipeline::source_queue::enqueue_accepted_source(&storage, &accepted.source, &accepted.publication, &principal.subject).await
                    .map_err(|error| (StatusCode::SERVICE_UNAVAILABLE, Json(ApiError::internal(format!("Audit inverse accepted as {}; queue handoff pending durable recovery: {error}", accepted.operation.operation_id)))))?;
            }
        }
    } else {
        let prepared = PreparedAuditRevert::prepare(storage, &space_id, &audit)
            .await
            .map_err(audit_storage_error)?;
        for record in [&prepared.current, &prepared.proposed] {
            let base = record.base();
            let attributes = elephant_policy::RecordAttributes {
                space_id: base.space_id.clone(),
                security: base.security.clone(),
                subjects: base.subjects.clone(),
                subjects_state: base.subjects_state,
            };
            state
                .authz
                .evaluate(&principal, &attributes, required_scope)
                .map_err(audit_authz_error)?;
        }
        prepared
            .commit(&principal.subject)
            .await
            .map_err(audit_storage_error)?;
    }
    Ok(Json(RevertResponse {
        success: true,
        message: format!(
            "Reversed {:?} on {} with current authority",
            audit.action, audit.record
        ),
        audit_id,
    }))
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "Retain the owned error adapter used by existing map_err callers."
)]
fn audit_storage_error(error: elephant_storage::StorageError) -> (StatusCode, Json<ApiError>) {
    ApiError::storage(&error)
}
fn audit_authz_error(error: crate::authz::AuthzError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: error.code,
            message: error.message,
            details: None,
        }),
    )
}

/// GET /v1/capabilities - Get server capabilities.
async fn get_capabilities(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
) -> ApiResult<CapabilitiesResponse> {
    // Capabilities are non-sensitive metadata; require an authenticated principal.
    if principal.scopes.is_empty() {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access denied: missing scopes".to_string(),
                details: None,
            }),
        ));
    }

    // policies: best-effort list derived from token clearances.
    let abac_principal = state.authz.to_abac_principal(&principal);
    let mut policies: Vec<String> = abac_principal
        .clearances
        .iter()
        .map(|c| c.policy_id.clone())
        .collect();
    policies.sort();
    policies.dedup();

    // extensions: configured by environment for now.
    let extensions = configured_extensions().map_err(|message| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(message)),
        )
    })?;

    let features = if let Some(storage) = state.storage.as_ref() {
        visible_capability_features(storage, &state.authz, &principal).await?
    } else {
        Vec::new()
    };

    Ok(Json(CapabilitiesResponse {
        backend: state.storage.as_ref().map_or_else(
            || StorageBackendKind::SurrealDb.as_str().to_string(),
            |storage| storage.backend().as_str().to_string(),
        ),
        policies,
        extensions,
        features,
    }))
}

fn configured_extensions() -> Result<Vec<String>, String> {
    let names = enabled_extension_names(std::env::var("ELEPHANT_EXTENSIONS").ok().as_deref());
    if names.is_empty() {
        return Ok(Vec::new());
    }

    load_runtime_reference_registry_from_env()
        .map_err(|e| format!("failed to load extension manifests: {e}"))?;
    Ok(names)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn visible_capability_features(
    storage: &StorageClient,
    authz: &AuthzState,
    principal: &AuthPrincipal,
) -> Result<Vec<String>, (StatusCode, Json<ApiError>)> {
    let dispatcher = create_dispatcher_with_context(ToolContext::with_authz(
        storage.clone(),
        Arc::new(McpAuthzService::new_with_skip_auth(principal.bypass_abac)),
        authz.to_abac_principal(principal),
    ));
    let capabilities = dispatcher
        .execute("get_capabilities", Value::Object(serde_json::Map::new()))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let tools: Vec<String> = capabilities
        .get("data")
        .and_then(|data| data.get("tools"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_string)
        .collect();

    let has = |tool_name: &str| tools.iter().any(|tool| tool == tool_name);
    let mut features = Vec::new();
    if has("search_docs") {
        features.push("vector_search".to_string());
    }
    if has("merge_entities") || has("list_identity_candidates") || has("propose_identity_candidate")
    {
        features.push("identity_resolution".to_string());
    }
    if has("stream_events") {
        features.push("outbox_stream".to_string());
    }
    if has("export_snapshot") || has("import_snapshot") {
        features.push("embedded_snapshots".to_string());
    }

    Ok(features)
}

/// Online export drains admissions and workers before the storage consistency barrier.
async fn export_snapshot(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(request): Json<SnapshotRequest>,
) -> ApiResult<SnapshotResponse> {
    authorize_global_system_scope(&principal, "read:system")?;
    run_snapshot(&state, request, false).await
}

/// Restore remains reachable while a previous restore's durable marker blocks data access.
async fn import_snapshot(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(request): Json<SnapshotRequest>,
) -> ApiResult<SnapshotResponse> {
    authorize_global_system_scope(&principal, "write:system")?;
    run_snapshot(&state, request, true).await
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reviewed route body, guard lifetimes, and public contract intact."
)]
async fn run_snapshot(
    state: &SystemState,
    request: SnapshotRequest,
    restore: bool,
) -> ApiResult<SnapshotResponse> {
    let resolved_path = resolve_snapshot_host_path(&request.path, restore)?;
    let storage = snapshot_storage(state)?;
    let runtime = state.runtime.as_ref().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiError::internal("Snapshot runtime control unavailable")),
        )
    })?;
    let defaults = elephant_storage::snapshot::SnapshotRetention::default();
    let retention = elephant_storage::snapshot::SnapshotRetention {
        retention_seconds: request
            .retention_seconds
            .unwrap_or(defaults.retention_seconds),
        quiesce_timeout_seconds: request
            .quiesce_timeout_seconds
            .unwrap_or(defaults.quiesce_timeout_seconds),
    };
    if !(1..=2_592_000).contains(&retention.retention_seconds)
        || !(1..=60).contains(&retention.quiesce_timeout_seconds)
    {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "Retention must be 1..=2592000 seconds and quiesce timeout 1..=60 seconds",
            )),
        ));
    }
    let name = request.name.unwrap_or_else(|| "manual".into());
    let manifest = runtime
        .snapshot_operation(resolved_path.clone(), name, retention, restore)
        .await
        .map_err(|error| {
            tracing::warn!(error = %error, restore, "Snapshot operation failed");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ApiError::internal(
                    "Snapshot operation unavailable; inspect readiness and operator logs",
                )),
            )
        })?;
    Ok(Json(SnapshotResponse {
        success: true,
        backend: storage.backend().as_str().into(),
        path: resolved_path.display().to_string(),
        message: if restore {
            "Snapshot restored after erasure replay"
        } else {
            "Consistent snapshot exported with bounded retention"
        }
        .into(),
        manifest,
    }))
}

/// Returns system routes.
#[must_use]
#[allow(clippy::double_must_use)]
pub fn system_routes() -> Router<Arc<SystemState>> {
    Router::new()
        .route("/v1/audit/revert/{audit_id}", post(revert_audit))
        .route("/v1/capabilities", get(get_capabilities))
        .route("/v1/system/snapshot/export", post(export_snapshot))
        .route("/v1/system/snapshot/import", post(import_snapshot))
        .route("/v1/system/snapshot/restore", post(import_snapshot))
}

#[cfg(test)]
#[path = "system_test.rs"]
mod integration_tests;

#[cfg(test)]
#[path = "system_source_audit_tests.rs"]
mod source_audit_tests;

#[cfg(test)]
mod tests {
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
    use super::*;
    struct EnvVarGuard {
        key: &'static str,
        previous: Option<std::ffi::OsString>,
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            match &self.previous {
                Some(value) => std::env::set_var(self.key, value),
                None => std::env::remove_var(self.key),
            }
        }
    }

    fn set_env_var_scoped(key: &'static str, value: impl AsRef<std::ffi::OsStr>) -> EnvVarGuard {
        let previous = std::env::var_os(key);
        std::env::set_var(key, value);
        EnvVarGuard { key, previous }
    }

    fn temp_dir(prefix: &str) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[serial_test::serial]
    #[test]
    fn test_capabilities_response_serialize() {
        let response = CapabilitiesResponse {
            backend: "surreal-db".to_string(),
            policies: vec!["default".to_string()],
            extensions: vec!["sampleapp".to_string()],
            features: vec!["vector_search".to_string()],
        };
        let json = serde_json::to_string(&response).expect("serialize");
        assert!(json.contains("policies"));
    }

    #[serial_test::serial]
    #[test]
    fn test_global_system_scope_rejects_space_scoped_principal() {
        let principal = AuthPrincipal {
            space_id: Some("space-a".to_string()),
            scopes: vec!["write:system".to_string()],
            ..AuthPrincipal::default()
        };

        let error = authorize_global_system_scope(&principal, "write:system")
            .expect_err("space-scoped principal should be rejected");
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert!(error
            .1
            .message
            .contains("snapshot operations require a global principal"));
    }

    #[serial_test::serial]
    #[test]
    fn test_global_system_scope_allows_global_principal() {
        let principal = AuthPrincipal {
            scopes: vec!["read:system".to_string()],
            ..AuthPrincipal::default()
        };

        authorize_global_system_scope(&principal, "read:system")
            .expect("global principal should be allowed");
    }

    #[serial_test::serial]
    #[test]
    fn test_global_system_scope_allows_skip_auth_principal() {
        let principal = AuthPrincipal {
            bypass_abac: true,
            space_id: Some("default".to_string()),
            scopes: vec!["*".to_string()],
            ..AuthPrincipal::default()
        };

        authorize_global_system_scope(&principal, "write:system")
            .expect("skip-auth principal should be allowed");
    }

    #[serial_test::serial]
    #[test]
    fn test_resolve_snapshot_host_path_rejects_escape() {
        let root = temp_dir("elephant-snapshot-root");
        let _guard = set_env_var_scoped(SNAPSHOT_DIR_ENV, &root);

        let error =
            resolve_snapshot_host_path("../outside.json", false).expect_err("path should reject");
        assert_eq!(error.0, StatusCode::FORBIDDEN);
    }

    #[serial_test::serial]
    #[test]
    fn test_resolve_snapshot_host_path_allows_relative_paths_within_root() {
        let root = temp_dir("elephant-snapshot-root");
        let _guard = set_env_var_scoped(SNAPSHOT_DIR_ENV, &root);

        let resolved =
            resolve_snapshot_host_path("nested/export.json", false).expect("path should resolve");
        assert!(resolved.starts_with(root.canonicalize().expect("canonical root")));
        assert!(resolved.ends_with(std::path::Path::new("nested/export.json")));
    }

    #[serial_test::serial]
    #[test]
    fn test_configured_extensions_surfaces_manifest_errors() {
        let temp_dir = temp_dir("elephant-system-capabilities");
        std::env::set_var("ELEPHANT_EXTENSIONS", "missingext");
        std::env::set_var("ELEPHANT_EXTENSION_MANIFEST_DIR", &temp_dir);

        let error = configured_extensions().expect_err("missing manifest should fail");
        assert!(error.contains("failed to load extension manifests"));

        std::env::remove_var("ELEPHANT_EXTENSIONS");
        std::env::remove_var("ELEPHANT_EXTENSION_MANIFEST_DIR");
        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[serial_test::serial]
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the original test fixture and route-state owners alive through the complete assertion scope"
    )]
    async fn test_visible_capability_features_follow_principal_scopes() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        let principal = AuthPrincipal {
            space_id: Some("default".to_string()),
            scopes: vec!["read:system_event".to_string()],
            ..AuthPrincipal::default()
        };

        let features = visible_capability_features(
            &storage,
            &Arc::new(crate::authz::AuthzService::new()),
            &principal,
        )
        .await
        .expect("features should resolve");

        assert!(features.contains(&"outbox_stream".to_string()));
        assert!(!features.contains(&"vector_search".to_string()));
        assert!(!features.contains(&"identity_resolution".to_string()));
        assert!(!features.contains(&"embedded_snapshots".to_string()));
    }
}
