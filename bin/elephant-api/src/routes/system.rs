//! System API routes.
//!
//! Implements the following endpoints:
//! - POST `/v1/audit/revert/{audit_id}` - Revert changes from an audit log entry
//! - GET `/v1/capabilities` - Get server capabilities

use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use elephant_extensions::{enabled_extension_names, load_runtime_reference_registry_from_env};
use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_dispatcher_with_context, ToolContext,
};
use elephant_storage::{
    parse_id, AuditLogRepository, DocRepository, EmbeddedRecord, EntityRepository,
    IdentityCandidateRepository, RelRepository, Repository, SourceRepository, StorageBackendKind,
    StorageClient, SystemEventRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::base::BaseFields;
use elephant_types::records::{AuditLog, Doc, Entity, IdentityCandidate, Rel, Source, SystemEvent};
use elephant_types::RecordId;

use crate::routes::entities::ApiError;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;

/// System-specific application state.
#[derive(Clone)]
pub struct SystemState {
    /// Shared storage handle for backend-level admin operations.
    pub storage: Option<StorageClient>,
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

    if principal
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

    if !storage.is_embedded() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(
                "Snapshot import/export is only supported with ELEPHANT_DB=elephant-db",
            )),
        ));
    }

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

async fn apply_revert_record<T>(
    storage: Option<&StorageClient>,
    repo: &(dyn Repository<T> + Send + Sync),
    action: elephant_types::records::AuditAction,
    space_id: &str,
    id: &RecordId,
    record: T,
    record_type: &str,
) -> Result<(), (StatusCode, Json<ApiError>)>
where
    T: EmbeddedRecord + Serialize + DeserializeOwned + Send + Sync,
{
    if matches!(action, elephant_types::records::AuditAction::Delete) {
        if let Some(store) = storage.and_then(StorageClient::embedded_store) {
            store
                .restore_and_replace_existing::<T>(space_id, id, record)
                .await
                .map(|_| ())
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(format!(
                            "Failed to revert deleted {record_type}: {e}"
                        ))),
                    )
                })?;
            return Ok(());
        }
    }

    repo.update(space_id, id, record)
        .await
        .map(|_| ())
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(format!(
                    "Failed to revert {record_type}: {e}"
                ))),
            )
        })
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
async fn revert_audit(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(audit_id): Path<String>,
) -> ApiResult<RevertResponse> {
    let Some(space_id) = principal.space_id.clone() else {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError {
                code: "ACCESS_DENIED".to_string(),
                message: "Access denied: missing space_id".to_string(),
                details: None,
            }),
        ));
    };
    let rid = parse_id(&audit_id, TableName::AuditLog).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    // Get the audit log entry
    match state.audit_repo.get(&space_id, &rid).await {
        Ok(Some(audit_log)) => {
            state
                .authz
                .authorize_record_read(&principal, &audit_log, "read:audit")
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;

            let required_scope =
                write_scope_for_table(audit_log.record.table()).ok_or_else(|| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError::validation("Revert not supported for record type")),
                    )
                })?;

            let snapshot = match audit_log.action {
                elephant_types::records::AuditAction::Create => audit_log.after.as_ref(),
                elephant_types::records::AuditAction::Update
                | elephant_types::records::AuditAction::Delete
                | elephant_types::records::AuditAction::Restore => audit_log.before.as_ref(),
                elephant_types::records::AuditAction::Promote
                | elephant_types::records::AuditAction::Merge
                | elephant_types::records::AuditAction::Purge => None,
            }
            .ok_or_else(|| {
                (
                    StatusCode::FORBIDDEN,
                    Json(ApiError::validation(
                        "Revert not supported for audit action",
                    )),
                )
            })?;

            let target_record_id = audit_log.record.clone();
            let mut snapshot_value = snapshot.clone();
            if let serde_json::Value::Object(ref mut map) = snapshot_value {
                map.entry("id".to_string())
                    .or_insert_with(|| serde_json::Value::String(target_record_id.to_string()));
            }

            let base: BaseFields = serde_json::from_value(snapshot_value.clone()).map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(format!(
                        "Failed to parse audit snapshot for authorization: {e}"
                    ))),
                )
            })?;

            let record_attrs = elephant_policy::RecordAttributes {
                space_id: base.space_id.clone(),
                security: base.security,
                subjects: base.subjects,
                subjects_state: base.subjects_state,
            };
            state
                .authz
                .evaluate(&principal, &record_attrs, required_scope)
                .map_err(|e| {
                    (
                        StatusCode::FORBIDDEN,
                        Json(ApiError {
                            code: e.code,
                            message: e.message,
                            details: None,
                        }),
                    )
                })?;

            let target_space_id = base.space_id;

            // Apply revert semantics (docs/spec.md §18.3).
            match audit_log.action {
                elephant_types::records::AuditAction::Update
                | elephant_types::records::AuditAction::Delete
                | elephant_types::records::AuditAction::Restore => match audit_log.record.table() {
                    TableName::Entity => {
                        let record: Entity = serde_json::from_value(snapshot_value.clone())
                            .map_err(|e| {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ApiError::internal(format!(
                                        "Failed to parse entity snapshot: {e}"
                                    ))),
                                )
                            })?;
                        apply_revert_record::<Entity>(
                            state.storage.as_ref(),
                            state.entity_repo.as_ref(),
                            audit_log.action,
                            &target_space_id,
                            &target_record_id,
                            record,
                            "entity",
                        )
                        .await?;
                    }
                    TableName::Rel => {
                        let record: Rel =
                            serde_json::from_value(snapshot_value.clone()).map_err(|e| {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ApiError::internal(format!(
                                        "Failed to parse relationship snapshot: {e}"
                                    ))),
                                )
                            })?;
                        apply_revert_record::<Rel>(
                            state.storage.as_ref(),
                            state.rel_repo.as_ref(),
                            audit_log.action,
                            &target_space_id,
                            &target_record_id,
                            record,
                            "relationship",
                        )
                        .await?;
                    }
                    TableName::SystemEvent => {
                        let record: SystemEvent = serde_json::from_value(snapshot_value.clone())
                            .map_err(|e| {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ApiError::internal(format!(
                                        "Failed to parse event snapshot: {e}"
                                    ))),
                                )
                            })?;
                        apply_revert_record::<SystemEvent>(
                            state.storage.as_ref(),
                            state.event_repo.as_ref(),
                            audit_log.action,
                            &target_space_id,
                            &target_record_id,
                            record,
                            "event",
                        )
                        .await?;
                    }
                    TableName::Doc => {
                        let record: Doc =
                            serde_json::from_value(snapshot_value.clone()).map_err(|e| {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ApiError::internal(format!(
                                        "Failed to parse doc snapshot: {e}"
                                    ))),
                                )
                            })?;
                        apply_revert_record::<Doc>(
                            state.storage.as_ref(),
                            state.doc_repo.as_ref(),
                            audit_log.action,
                            &target_space_id,
                            &target_record_id,
                            record,
                            "doc",
                        )
                        .await?;
                    }
                    TableName::Source => {
                        let record: Source = serde_json::from_value(snapshot_value.clone())
                            .map_err(|e| {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ApiError::internal(format!(
                                        "Failed to parse source snapshot: {e}"
                                    ))),
                                )
                            })?;
                        apply_revert_record::<Source>(
                            state.storage.as_ref(),
                            state.source_repo.as_ref(),
                            audit_log.action,
                            &target_space_id,
                            &target_record_id,
                            record,
                            "source",
                        )
                        .await?;
                    }
                    TableName::IdentityCandidate => {
                        let record: IdentityCandidate =
                            serde_json::from_value(snapshot_value.clone()).map_err(|e| {
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    Json(ApiError::internal(format!(
                                        "Failed to parse identity_candidate snapshot: {e}"
                                    ))),
                                )
                            })?;
                        apply_revert_record::<IdentityCandidate>(
                            state.storage.as_ref(),
                            state.identity_candidate_repo.as_ref(),
                            audit_log.action,
                            &target_space_id,
                            &target_record_id,
                            record,
                            "identity_candidate",
                        )
                        .await?;
                    }
                    _ => {
                        return Err((
                            StatusCode::FORBIDDEN,
                            Json(ApiError::validation("Revert not supported for record type")),
                        ))
                    }
                },
                elephant_types::records::AuditAction::Create => match audit_log.record.table() {
                    TableName::Entity => state
                        .entity_repo
                        .delete(&target_space_id, &target_record_id)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(format!(
                                    "Failed to revert create (entity delete): {e}"
                                ))),
                            )
                        })?,
                    TableName::Rel => state
                        .rel_repo
                        .delete(&target_space_id, &target_record_id)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(format!(
                                    "Failed to revert create (relationship delete): {e}"
                                ))),
                            )
                        })?,
                    TableName::SystemEvent => state
                        .event_repo
                        .delete(&target_space_id, &target_record_id)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(format!(
                                    "Failed to revert create (event delete): {e}"
                                ))),
                            )
                        })?,
                    TableName::Doc => state
                        .doc_repo
                        .delete(&target_space_id, &target_record_id)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(format!(
                                    "Failed to revert create (doc delete): {e}"
                                ))),
                            )
                        })?,
                    TableName::Source => state
                        .source_repo
                        .delete(&target_space_id, &target_record_id)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(format!(
                                    "Failed to revert create (source delete): {e}"
                                ))),
                            )
                        })?,
                    TableName::IdentityCandidate => state
                        .identity_candidate_repo
                        .delete(&target_space_id, &target_record_id)
                        .await
                        .map_err(|e| {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(ApiError::internal(format!(
                                    "Failed to revert create (identity_candidate delete): {e}"
                                ))),
                            )
                        })?,
                    _ => {
                        return Err((
                            StatusCode::FORBIDDEN,
                            Json(ApiError::validation("Revert not supported for record type")),
                        ))
                    }
                },
                elephant_types::records::AuditAction::Promote
                | elephant_types::records::AuditAction::Merge
                | elephant_types::records::AuditAction::Purge => {
                    return Err((
                        StatusCode::FORBIDDEN,
                        Json(ApiError::validation(
                            "Revert not supported for audit action",
                        )),
                    ))
                }
            }

            Ok(Json(RevertResponse {
                success: true,
                message: format!(
                    "Revert requested for audit log {audit_id}. Action: {:?}, Record: {}",
                    audit_log.action, audit_log.record
                ),
                audit_id,
            }))
        }
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiError::not_found("Audit log", &audit_id)),
        )),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
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

/// POST `/v1/system/snapshot/export` - Export the live embedded snapshot.
async fn export_snapshot(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(request): Json<SnapshotRequest>,
) -> ApiResult<SnapshotResponse> {
    authorize_global_system_scope(&principal, "read:system")?;
    let resolved_path = resolve_snapshot_host_path(&request.path, false)?;
    let storage = snapshot_storage(&state)?;
    storage
        .export_snapshot(resolved_path.clone())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    Ok(Json(SnapshotResponse {
        success: true,
        backend: storage.backend().as_str().to_string(),
        path: resolved_path.display().to_string(),
        message: format!("Exported embedded snapshot to {}", resolved_path.display()),
    }))
}

/// POST `/v1/system/snapshot/import` - Replace the live embedded state from a snapshot.
async fn import_snapshot(
    State(state): State<Arc<SystemState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(request): Json<SnapshotRequest>,
) -> ApiResult<SnapshotResponse> {
    authorize_global_system_scope(&principal, "write:system")?;
    let resolved_path = resolve_snapshot_host_path(&request.path, true)?;
    let storage = snapshot_storage(&state)?;
    storage
        .import_snapshot(resolved_path.clone())
        .await
        .map_err(|e| match e {
            elephant_storage::StorageError::Conflict(message) => {
                (StatusCode::CONFLICT, Json(ApiError::validation(message)))
            }
            other => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(other.to_string())),
            ),
        })?;

    Ok(Json(SnapshotResponse {
        success: true,
        backend: storage.backend().as_str().to_string(),
        path: resolved_path.display().to_string(),
        message: format!(
            "Imported embedded snapshot from {}",
            resolved_path.display()
        ),
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
}

#[cfg(test)]
#[path = "system_test.rs"]
mod integration_tests;

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
