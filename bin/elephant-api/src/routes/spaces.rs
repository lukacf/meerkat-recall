//! Space-scoped convenience routes for Phase 13 scenarios.
//!
//! These endpoints mirror the checklist paths under `/api/v1/spaces/...` and
//! orchestrate document ingestion into work items for live validation.

use std::str::FromStr;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use elephant_pipeline::predicate::PREDICATE_PROPOSAL_ARTIFACT_TYPE;
use elephant_pipeline::predicate::{PredicateProposalData, PredicateProposalWorkflow};
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::reprocess::WORK_KIND_EXECUTE_REPROCESS_JOB;
use elephant_pipeline::scheduler::PLAN_EXTRACTION_KIND;
use elephant_pipeline::scheduler::{execute_build_doc_tree, execute_render_doc, DocTreeConfig};
use elephant_storage::{
    AssertionRepository, AttributeRepository, CostLedgerDailyRepository, DocRepository,
    EntityIdentityKeyRepository, EntityRepository, PredicateAliasRepository,
    PredicateRegistryRepository, RelRepository, Repository, SourceRepository,
    SpaceConfigRepository, StorageClient, TruthSlotRepository, WorkArtifactRepository,
    WorkItemRepository,
};
use elephant_types::ids::{RecordId, TableName};
use elephant_types::records::assertion::Assertion;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::identity::{EntityIdentityKey, IdentityKeyStatus};
use elephant_types::records::predicate::{PredicateAlias, PredicateRegistry, SemanticsStatus};
use elephant_types::records::rel::Rel;
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::truth::{CurrentValue, TruthSlot};
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::records::{Doc, Source, SpaceConfig};
use elephant_types::Link;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;
use crate::routes::require_space_id;

/// Shared state for space-scoped helpers.
#[derive(Clone)]
pub struct SpaceApiState {
    pub storage: StorageClient,
    pub source_repo: SourceRepository,
    pub entity_repo: EntityRepository,
    pub identity_repo: EntityIdentityKeyRepository,
    pub doc_repo: DocRepository,
    pub assertion_repo: AssertionRepository,
    pub work_item_repo: WorkItemRepository,
    pub work_artifact_repo: WorkArtifactRepository,
    pub predicate_repo: PredicateRegistryRepository,
    pub alias_repo: PredicateAliasRepository,
    pub reprocess_job_repo: elephant_storage::ReprocessJobRepository,
    pub rel_repo: RelRepository,
    pub attribute_repo: AttributeRepository,
    pub truth_slot_repo: TruthSlotRepository,
    pub cost_ledger_repo: CostLedgerDailyRepository,
    pub space_config_repo: SpaceConfigRepository,
    pub authz: AuthzState,
}

impl SpaceApiState {
    #[must_use]
    pub fn new(storage: StorageClient, authz: AuthzState) -> Self {
        Self {
            storage: storage.clone(),
            source_repo: SourceRepository::new(storage.clone()),
            entity_repo: EntityRepository::new(storage.clone()),
            identity_repo: EntityIdentityKeyRepository::new(storage.clone()),
            doc_repo: DocRepository::new(storage.clone()),
            assertion_repo: AssertionRepository::new(storage.clone()),
            work_item_repo: WorkItemRepository::new(storage.clone()),
            work_artifact_repo: WorkArtifactRepository::new(storage.clone()),
            predicate_repo: PredicateRegistryRepository::new(storage.clone()),
            alias_repo: PredicateAliasRepository::new(storage.clone()),
            reprocess_job_repo: elephant_storage::ReprocessJobRepository::new(storage.clone()),
            rel_repo: RelRepository::new(storage.clone()),
            attribute_repo: AttributeRepository::new(storage.clone()),
            truth_slot_repo: TruthSlotRepository::new(storage.clone()),
            cost_ledger_repo: CostLedgerDailyRepository::new(storage.clone()),
            space_config_repo: SpaceConfigRepository::new(storage),
            authz,
        }
    }
}

/// Register Phase 13 helper routes.
pub fn space_routes() -> Router<Arc<SpaceApiState>> {
    Router::new()
        .route(
            "/api/v1/spaces/{space}/sources",
            post(create_source_in_space),
        )
        .route(
            "/api/v1/spaces/{space}/entities",
            get(list_entities).post(create_entity),
        )
        .route(
            "/api/v1/spaces/{space}/documents",
            get(list_documents).post(create_document),
        )
        .route("/api/v1/spaces/{space}/documents/{id}", get(get_document))
        .route("/api/v1/spaces/{space}/assertions", get(list_assertions))
        .route(
            "/api/v1/spaces/{space}/entities/{id}/facts",
            get(get_entity_facts),
        )
        .route("/api/v1/work_items", get(list_work_items))
        .route(
            "/api/v1/predicates/proposals",
            get(list_predicate_proposals),
        )
        .route(
            "/api/v1/predicates/proposals/{id}/approve",
            post(approve_predicate_proposal),
        )
        .route("/api/v1/predicates/aliases", get(list_predicate_aliases))
        .route("/api/v1/budget/today", get(get_budget_today))
        // V1 space CRUD routes
        .route("/v1/spaces", get(list_spaces_v1).post(create_space_v1))
        .route(
            "/v1/spaces/{space_id}",
            get(get_space_v1).put(update_space_v1),
        )
}

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<ApiError>)>;

#[derive(Debug, Clone, Deserialize)]
struct ListQuery {
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    include_all: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
struct WorkItemQuery {
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    kind: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct WorkItemListResponse {
    results: Vec<WorkItem>,
    total: usize,
}

#[derive(Debug, Clone, Deserialize)]
struct SpaceDocIngestRequest {
    doc: Doc,
    #[serde(default)]
    source_type: Option<String>,
    #[serde(default)]
    classification: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[allow(clippy::struct_field_names)]
struct SpaceDocIngestResponse {
    doc_id: String,
    revision_id: String,
    work_item_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct PredicateProposalView {
    id: String,
    predicate_id: String,
    aliases: Vec<String>,
    status: String,
}

#[derive(Debug, Clone, Serialize)]
struct PredicateProposalListResponse {
    results: Vec<PredicateProposalView>,
    total: usize,
}

#[derive(Debug, Clone, Serialize)]
struct AliasListResponse {
    results: Vec<PredicateAlias>,
    total: usize,
}

#[derive(Debug, Clone, Serialize)]
struct BudgetTodayResponse {
    date: String,
    spent_graph: f64,
    items_graph: u32,
    runs_graph: u32,
    failures_graph: u32,
}

#[derive(Debug, Clone, Serialize)]
struct AssertionView {
    #[serde(flatten)]
    assertion: Assertion,
    #[serde(skip_serializing_if = "Option::is_none")]
    validity_start: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct EntityFactView {
    predicate_id: String,
    slot_type: String,
    claim_id: String,
    value: serde_json::Value,
    confidence: f32,
}

#[derive(Debug, Clone, Serialize)]
struct EntityFactsResponse {
    facts: Vec<EntityFactView>,
}

#[allow(clippy::result_large_err)]
fn ensure_space(
    principal: &AuthPrincipal,
    space: &str,
) -> Result<String, (StatusCode, Json<ApiError>)> {
    let space_id = require_space_id(principal)?;
    if space_id != space {
        return Err((
            StatusCode::FORBIDDEN,
            Json(ApiError::validation("Access to space denied")),
        ));
    }
    Ok(space_id)
}

fn ensure_policy_defaults(security: &mut Security) {
    if security.policy_id.is_empty() {
        security.policy_id = "default".to_string();
    }
}

fn apply_classification(doc: &mut Doc, classification: Option<&String>) {
    let class = classification.cloned().or_else(|| {
        doc.metadata
            .get("classification")
            .and_then(|v| v.as_str().map(ToString::to_string))
    });

    let level = match class.as_deref() {
        Some("executive") => 5,
        Some("confidential") => 3,
        _ => 0,
    };

    doc.base.security.level = level;
    ensure_policy_defaults(&mut doc.base.security);

    if let Some(classification) = class {
        if doc.metadata.is_null() {
            doc.metadata = serde_json::json!({});
        }
        if let Some(map) = doc.metadata.as_object_mut() {
            map.insert(
                "classification".to_string(),
                serde_json::Value::String(classification),
            );
        }
    }
}

async fn list_work_items(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(query): Query<WorkItemQuery>,
) -> ApiResult<WorkItemListResponse> {
    let space_id = require_space_id(&principal)?;
    let limit = query.limit.unwrap_or(100);

    let items = if let Some(status_raw) = query.status {
        let status = match status_raw.as_str() {
            "completed" => WorkItemStatus::Done,
            other => WorkItemStatus::from_str(other).map_err(|e| {
                (
                    StatusCode::BAD_REQUEST,
                    Json(ApiError::validation(e.to_string())),
                )
            })?,
        };
        if let Some(kind) = query.kind.as_deref() {
            state
                .work_item_repo
                .find_by_status_and_kind(&space_id, status, kind, limit)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?
        } else {
            state
                .work_item_repo
                .find_by_status(&space_id, status)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?
        }
    } else {
        let cursor = query
            .cursor
            .as_ref()
            .and_then(|c| elephant_storage::parse_id(c, TableName::WorkItem).ok());
        state
            .work_item_repo
            .list(&space_id, limit, cursor.as_ref())
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    };

    Ok(Json(WorkItemListResponse {
        total: items.len(),
        results: items,
    }))
}

#[allow(clippy::too_many_lines)]
async fn create_document(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Json(req): Json<SpaceDocIngestRequest>,
) -> Result<(StatusCode, Json<SpaceDocIngestResponse>), (StatusCode, Json<ApiError>)> {
    let space_id = ensure_space(&principal, &space)?;
    let mut doc = req.doc;
    doc.base.space_id.clone_from(&space_id);
    doc.base.created_by.clone_from(&principal.subject);
    ensure_policy_defaults(&mut doc.base.security);
    apply_classification(&mut doc, req.classification.as_ref());

    state
        .authz
        .authorize_record_write(&principal, &doc, "write:doc")
        .map_err(|e| (StatusCode::FORBIDDEN, Json(ApiError::validation(e.message))))?;

    let doc = match state.doc_repo.get(&space_id, &doc.base.id).await {
        Ok(Some(existing)) => {
            let mut updated = doc;
            updated.base.created_at = existing.base.created_at;
            updated.base.updated_at = chrono::Utc::now();
            let update_id = updated.base.id.clone();
            state
                .doc_repo
                .update(&space_id, &update_id, updated)
                .await
                .map_err(|e| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiError::internal(e.to_string())),
                    )
                })?
        }
        _ => state.doc_repo.create(doc).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?,
    };

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));
    let source_type = req.source_type.as_deref().unwrap_or("text/plain");

    let render_result = execute_render_doc(&state.storage, &registry, &doc, Some(source_type))
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let config = DocTreeConfig::default();
    let _ = execute_build_doc_tree(&state.storage, &render_result.revision_id, &config)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let payload = serde_json::json!({
        "revision_id": render_result.revision_id.to_string(),
        "policy_version": "default",
        "extractor_id": "elephant.extractor.agentic",
        "allow_raw_quotes": false,
        "ingest_mode": "graph"
    });

    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::WorkItem),
        space_id.clone(),
        principal.subject.clone(),
    );
    base.security = doc.base.security.clone();
    base.provenance = doc.base.provenance.clone();
    let work_item = WorkItem {
        base,
        kind: PLAN_EXTRACTION_KIND.to_string(),
        payload,
        status: WorkItemStatus::Runnable,
        priority: 50,
        idempotency_key: format!("plan_extraction:{}", render_result.revision_id),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };

    let work_item = state.work_item_repo.create(work_item).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    Ok((
        StatusCode::CREATED,
        Json(SpaceDocIngestResponse {
            doc_id: doc.base.id.to_string(),
            revision_id: render_result.revision_id.to_string(),
            work_item_id: work_item.base.id.to_string(),
        }),
    ))
}

async fn list_documents(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Vec<Doc>> {
    let space_id = ensure_space(&principal, &space)?;
    let limit = query.limit.unwrap_or(100);

    let docs = state
        .doc_repo
        .list(&space_id, limit, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let filtered = state.authz.filter_records(&principal, docs, "read:doc");

    Ok(Json(filtered))
}

async fn get_document(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path((space, id)): Path<(String, String)>,
) -> ApiResult<Doc> {
    let space_id = ensure_space(&principal, &space)?;
    let rid = elephant_storage::parse_id(&id, TableName::Doc).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    match state.doc_repo.get(&space_id, &rid).await {
        Ok(Some(doc)) => {
            state
                .authz
                .authorize_record_read(&principal, &doc, "read:doc")
                .map_err(|e| (StatusCode::FORBIDDEN, Json(ApiError::validation(e.message))))?;
            Ok(Json(doc))
        }
        Ok(None) => Err((StatusCode::NOT_FOUND, Json(ApiError::not_found("Doc", &id)))),
        Err(e) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )),
    }
}

async fn list_entities(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Vec<Entity>> {
    let space_id = ensure_space(&principal, &space)?;
    let limit = query.limit.unwrap_or(200);
    let include_all = query.include_all.unwrap_or(false);
    let entities = state
        .entity_repo
        .list(&space_id, limit, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let mut filtered = state
        .authz
        .filter_records(&principal, entities, "read:entity");
    if !include_all {
        filtered.retain(|entity| entity.entity_type.as_str() == "person");
    }
    Ok(Json(filtered))
}

async fn create_entity(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Json(mut entity): Json<Entity>,
) -> Result<(StatusCode, Json<Entity>), (StatusCode, Json<ApiError>)> {
    let space_id = ensure_space(&principal, &space)?;
    entity.base.space_id.clone_from(&space_id);
    ensure_policy_defaults(&mut entity.base.security);

    state
        .authz
        .authorize_record_write(&principal, &entity, "write:entity")
        .map_err(|e| (StatusCode::FORBIDDEN, Json(ApiError::validation(e.message))))?;

    let created = state.entity_repo.create(entity).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    upsert_identity_keys(&state.identity_repo, &created, &created.identity_keys, None).await?;

    Ok((StatusCode::CREATED, Json(created)))
}

#[derive(Debug, Clone, Deserialize)]
struct ContactMeta {
    name: Option<String>,
    email: Option<String>,
    company: Option<String>,
    job_title: Option<String>,
    profile: Option<String>,
}

fn extract_contact_meta(metadata: &serde_json::Value) -> Option<ContactMeta> {
    let obj = metadata.get("contact")?.as_object()?;
    Some(ContactMeta {
        name: obj
            .get("name")
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
        email: obj
            .get("email")
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
        company: obj
            .get("company")
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
        job_title: obj
            .get("job_title")
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
        profile: obj
            .get("profile")
            .and_then(|v| v.as_str())
            .map(ToString::to_string),
    })
}

async fn create_source_in_space(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Json(mut source): Json<Source>,
) -> Result<(StatusCode, Json<Source>), (StatusCode, Json<ApiError>)> {
    let space_id = ensure_space(&principal, &space)?;
    source.base.space_id.clone_from(&space_id);
    ensure_policy_defaults(&mut source.base.security);

    state
        .authz
        .authorize_record_write(&principal, &source, "write:source")
        .map_err(|e| (StatusCode::FORBIDDEN, Json(ApiError::validation(e.message))))?;

    let created = state.source_repo.create(source).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    if let Some(contact) = extract_contact_meta(&created.metadata) {
        let entity = upsert_contact_entity(&state, &space_id, &contact, &created).await?;
        if let Some(company) = contact.company {
            if let Some(job_title) = contact.job_title {
                ensure_works_at_rel(&state, &entity, &company, &job_title).await?;
            }
        }
    }

    Ok((StatusCode::CREATED, Json(created)))
}

async fn upsert_contact_entity(
    state: &SpaceApiState,
    space_id: &str,
    contact: &ContactMeta,
    source: &Source,
) -> Result<Entity, (StatusCode, Json<ApiError>)> {
    let mut keys = Vec::new();
    if let Some(email) = &contact.email {
        keys.push(email.to_lowercase());
    }
    if let Some(profile) = &contact.profile {
        keys.push(profile.clone());
    }

    let mut existing_entity = None;
    for key in &keys {
        if let Ok(Some(identity)) = state.identity_repo.find_by_key(space_id, key).await {
            if let Ok(Some(entity)) = state.entity_repo.get(space_id, identity.entity.id()).await {
                existing_entity = Some(entity);
                break;
            }
        }
    }

    let entity = if let Some(mut existing) = existing_entity {
        for key in &keys {
            if !existing.identity_keys.contains(key) {
                existing.identity_keys.push(key.clone());
            }
        }
        if let Some(name) = &contact.name {
            if existing.display_name != *name && !existing.aliases.contains(name) {
                existing.aliases.push(name.clone());
            }
        }
        let update_id = existing.base.id.clone();
        state
            .entity_repo
            .update(space_id, &update_id, existing)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
    } else {
        let display_name = contact
            .name
            .clone()
            .or_else(|| contact.email.clone())
            .unwrap_or_else(|| "Unknown".to_string());
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            space_id.to_string(),
            "contact_ingest".to_string(),
        );
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let entity = Entity {
            base,
            entity_type: EntityType::new("person"),
            display_name,
            aliases: Vec::new(),
            summary: None,
            handles: Vec::new(),
            identity_keys: keys.clone(),
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: elephant_types::records::entity::EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::json!({
                "source_type": "contact_import"
            }),
        };
        state.entity_repo.create(entity).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
    };

    upsert_identity_keys(&state.identity_repo, &entity, &keys, Some(source)).await?;

    Ok(entity)
}

async fn upsert_identity_keys(
    repo: &EntityIdentityKeyRepository,
    entity: &Entity,
    keys: &[String],
    source: Option<&Source>,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    for key in keys {
        if let Ok(Some(existing)) = repo.find_by_key(&entity.base.space_id, key).await {
            if existing.entity.id() == &entity.base.id {
                continue;
            }
        }

        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::EntityIdentityKey),
            entity.base.space_id.clone(),
            "identity_upsert".to_string(),
        );
        base.security = entity.base.security.clone();
        base.provenance = entity.base.provenance.clone();
        base.subjects_state = SubjectsState::Complete;

        let evidence = source
            .map(|s| vec![Link::new(s.base.id.clone())])
            .unwrap_or_default();

        let record = EntityIdentityKey {
            base,
            key: key.clone(),
            entity: Link::new(entity.base.id.clone()),
            confidence: 1.0,
            first_seen: Some(Utc::now()),
            last_seen: Some(Utc::now()),
            evidence,
            status: IdentityKeyStatus::Active,
        };

        repo.create(record).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;
    }

    Ok(())
}

async fn ensure_works_at_rel(
    state: &SpaceApiState,
    person: &Entity,
    company: &str,
    _job_title: &str,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let company_entity = if let Ok(Some(entity)) = state
        .entity_repo
        .find_by_display_name(&person.base.space_id, company)
        .await
    {
        entity
    } else {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            person.base.space_id.clone(),
            "contact_ingest".to_string(),
        );
        base.security = person.base.security.clone();
        base.provenance = person.base.provenance.clone();
        base.subjects_state = SubjectsState::Complete;

        let entity = Entity {
            base,
            entity_type: EntityType::new("org"),
            display_name: company.to_string(),
            aliases: Vec::new(),
            summary: None,
            handles: Vec::new(),
            identity_keys: Vec::new(),
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: elephant_types::records::entity::EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::json!({}),
        };

        state.entity_repo.create(entity).await.map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
    };

    if state
        .rel_repo
        .find_by_endpoints_with_deleted(
            &person.base.space_id,
            &person.base.id,
            &company_entity.base.id,
            "works_at",
        )
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .is_some()
    {
        return Ok(());
    }

    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Rel),
        person.base.space_id.clone(),
        "contact_ingest".to_string(),
    );
    base.security = person.base.security.clone();
    base.provenance = person.base.provenance.clone();
    base.subjects_state = SubjectsState::Complete;

    let rel = Rel {
        base,
        subject: Link::new(person.base.id.clone()),
        object: Link::new(company_entity.base.id.clone()),
        rel_type: "works_at".to_string(),
        status: elephant_types::records::rel::ClaimStatus::Approved,
        confidence: 0.95,
        validity_window: None,
        evidence: Vec::new(),
        temporal_audit: None,
    };

    state.rel_repo.create(rel).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    Ok(())
}

async fn list_assertions(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Query(query): Query<ListQuery>,
) -> ApiResult<Vec<AssertionView>> {
    let space_id = ensure_space(&principal, &space)?;
    let limit = query.limit.unwrap_or(200);
    let assertions = state
        .assertion_repo
        .list(&space_id, limit, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let filtered = state
        .authz
        .filter_records(&principal, assertions, "read:assertion");
    let views = filtered
        .into_iter()
        .map(|assertion| AssertionView {
            validity_start: assertion_validity_start(&assertion),
            assertion,
        })
        .collect();
    Ok(Json(views))
}

fn assertion_validity_start(assertion: &Assertion) -> Option<String> {
    if let Some(ts) = assertion.source_event_time {
        return Some(ts.to_rfc3339());
    }

    let value = assertion.object_literal.as_ref()?;
    temporal_string_from_object_literal(value)
}

fn temporal_string_from_object_literal(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(raw) => parse_temporal_string(raw),
        serde_json::Value::Object(map) => {
            let literal_type = map.get("type")?.as_str()?;
            if !matches!(literal_type, "date" | "datetime") {
                return None;
            }
            let raw_value = map.get("value")?.as_str()?;
            parse_temporal_string(raw_value)
        }
        _ => None,
    }
}

fn parse_temporal_string(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if chrono::DateTime::parse_from_rfc3339(trimmed).is_ok() {
        return Some(trimmed.to_string());
    }

    if NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").is_ok() {
        return Some(trimmed.to_string());
    }

    None
}

async fn get_entity_facts(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path((space, id)): Path<(String, String)>,
) -> ApiResult<EntityFactsResponse> {
    let space_id = ensure_space(&principal, &space)?;
    let entity_id = elephant_storage::parse_id(&id, TableName::Entity).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let slots = state
        .truth_slot_repo
        .find_by_subject_entity(&space_id, &entity_id.to_string())
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let mut facts = Vec::new();
    for slot in slots {
        for current in &slot.current_values {
            if current.status != elephant_types::records::rel::ClaimStatus::Approved {
                continue;
            }
            if let Some(view) = build_fact_view(&state, &slot, current).await? {
                facts.push(view);
            }
        }
    }

    Ok(Json(EntityFactsResponse { facts }))
}

async fn build_fact_view(
    state: &SpaceApiState,
    slot: &TruthSlot,
    current: &CurrentValue,
) -> Result<Option<EntityFactView>, (StatusCode, Json<ApiError>)> {
    let claim_id = elephant_storage::parse_id(&current.claim_id, TableName::Rel)
        .or_else(|_| elephant_storage::parse_id(&current.claim_id, TableName::Attribute))
        .map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(e.to_string())),
            )
        })?;

    let value = if slot.slot_type == "attribute" {
        match state
            .attribute_repo
            .get(&slot.base.space_id, &claim_id)
            .await
        {
            Ok(Some(attr)) => serde_json::to_value(&attr.value).unwrap_or(serde_json::Value::Null),
            _ => serde_json::Value::Null,
        }
    } else {
        match state.rel_repo.get(&slot.base.space_id, &claim_id).await {
            Ok(Some(rel)) => serde_json::json!({ "object": rel.object.id().to_string() }),
            _ => serde_json::Value::Null,
        }
    };

    Ok(Some(EntityFactView {
        predicate_id: slot.predicate_id.clone(),
        slot_type: slot.slot_type.clone(),
        claim_id: current.claim_id.clone(),
        value,
        confidence: current.confidence,
    }))
}

async fn list_predicate_proposals(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
) -> ApiResult<PredicateProposalListResponse> {
    let space_id = require_space_id(&principal)?;

    let artifacts = state
        .work_artifact_repo
        .list(&space_id, 200, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let mut results = Vec::new();
    for artifact in artifacts {
        if artifact.artifact_type != PREDICATE_PROPOSAL_ARTIFACT_TYPE {
            continue;
        }
        let proposal: PredicateProposalData = match serde_json::from_value(artifact.data.clone()) {
            Ok(p) => p,
            Err(_) => continue,
        };
        let aliases = proposal
            .proposed_aliases
            .iter()
            .map(|alias| alias.alias.clone())
            .collect();
        results.push(PredicateProposalView {
            id: artifact.base.id.to_string(),
            predicate_id: proposal.predicate_id,
            aliases,
            status: artifact.status.to_string(),
        });
    }

    Ok(Json(PredicateProposalListResponse {
        total: results.len(),
        results,
    }))
}

async fn approve_predicate_proposal(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<PredicateAlias> {
    let space_id = require_space_id(&principal)?;

    let artifact_id = elephant_storage::parse_id(&id, TableName::WorkArtifact).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;

    let workflow = PredicateProposalWorkflow::new(state.storage.clone());
    let artifact = state
        .work_artifact_repo
        .get(&space_id, &artifact_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("WorkArtifact", &id)),
            )
        })?;

    let proposal: PredicateProposalData =
        serde_json::from_value(artifact.data.clone()).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(e.to_string())),
            )
        })?;

    ensure_predicate_registry_entry(
        &state.predicate_repo,
        &space_id,
        &proposal.predicate_id,
        &principal.subject,
    )
    .await?;

    let _ = workflow
        .validate_predicate_proposal(&space_id, &artifact_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let _ = workflow
        .commit_predicate_proposal(&space_id, &artifact_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let alias = state
        .alias_repo
        .find_active_by_raw(&space_id, &proposal.proposed_aliases[0].alias)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found(
                    "PredicateAlias",
                    &proposal.predicate_id,
                )),
            )
        })?;

    enqueue_reprocess_for_predicate(&state, &space_id, &proposal.predicate_id).await?;

    Ok(Json(alias))
}

async fn ensure_predicate_registry_entry(
    repo: &PredicateRegistryRepository,
    space_id: &str,
    predicate_id: &str,
    actor: &str,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    if repo
        .find_by_predicate_id(space_id, predicate_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .is_some()
    {
        return Ok(());
    }

    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::PredicateRegistry),
        space_id.to_string(),
        actor.to_string(),
    );
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    let record = PredicateRegistry {
        base,
        predicate_id: predicate_id.to_string(),
        description: None,
        status: "active".to_string(),
        semantics: None,
        semantics_status: SemanticsStatus::Default,
        first_seen_at: None,
        usage_count: 0,
        last_used_at: None,
    };

    repo.create(record).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    Ok(())
}

async fn enqueue_reprocess_for_predicate(
    state: &SpaceApiState,
    space_id: &str,
    predicate_id: &str,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    let has_assertions = state
        .assertion_repo
        .list(space_id, 500, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .into_iter()
        .filter(|a| a.predicate_raw == predicate_id)
        .any(|_| true);

    if !has_assertions {
        return Ok(());
    }

    let reprocess_job = ReprocessJob {
        base: BaseFields::new(
            RecordId::new_v4(TableName::ReprocessJob),
            space_id.to_string(),
            "predicate_reprocess".to_string(),
        ),
        name: format!("predicate-reprocess-{predicate_id}"),
        selection_criteria: SelectionCriteria {
            predicate_ids: vec![predicate_id.to_string()],
            ..SelectionCriteria::default()
        },
        target_stages: vec![
            ReprocessTargetStage::ReResolve,
            ReprocessTargetStage::ReMaintain,
        ],
        status: ReprocessStatus::Pending,
        progress: ReprocessProgress::default(),
        priority: 50,
        budget_limit_euros: None,
        max_sources_per_day: None,
        started_at: None,
        completed_at: None,
        error: None,
        reason: Some(format!(
            "Predicate alias approved; refresh claims for predicate {predicate_id}"
        )),
    };

    let created_job = state
        .reprocess_job_repo
        .create(reprocess_job)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    let work_item = WorkItem {
        base: BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            space_id.to_string(),
            "predicate_reprocess".to_string(),
        ),
        kind: WORK_KIND_EXECUTE_REPROCESS_JOB.to_string(),
        payload: serde_json::json!({ "job_id": created_job.base.id.to_string() }),
        status: WorkItemStatus::Runnable,
        priority: 50,
        idempotency_key: format!("{WORK_KIND_EXECUTE_REPROCESS_JOB}:{}", created_job.base.id),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };

    state.work_item_repo.create(work_item).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    Ok(())
}

async fn list_predicate_aliases(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> ApiResult<AliasListResponse> {
    let space_id = require_space_id(&principal)?;
    let raw = params.get("raw").map_or("", String::as_str);

    let mut results = Vec::new();
    if !raw.is_empty() {
        if let Some(alias) = state
            .alias_repo
            .find_active_by_raw(&space_id, raw)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiError::internal(e.to_string())),
                )
            })?
        {
            results.push(alias);
        }
    }

    Ok(Json(AliasListResponse {
        total: results.len(),
        results,
    }))
}

async fn get_budget_today(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
) -> ApiResult<BudgetTodayResponse> {
    let space_id = require_space_id(&principal)?;
    let today = chrono::Utc::now().date_naive();
    let totals = state
        .cost_ledger_repo
        .aggregate_daily_spending(&space_id, today)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    Ok(Json(BudgetTodayResponse {
        date: today.format("%Y-%m-%d").to_string(),
        spent_graph: totals.spent_graph,
        items_graph: totals.items_graph,
        runs_graph: totals.runs_graph,
        failures_graph: totals.failures_graph,
    }))
}

// ── V1 Space CRUD handlers ─────────────────────────────────────────────

/// Request body for creating a space.
#[derive(Debug, Clone, Deserialize)]
struct CreateSpaceRequest {
    /// Space name / ID.
    name: String,
    /// Embedding provider (e.g. "fastembed", "openai", "fake").
    #[serde(default = "default_provider")]
    embedding_provider: String,
    /// Embedding model name.
    #[serde(default = "default_model")]
    embedding_model: String,
    /// Embedding vector dimensions.
    #[serde(default = "default_dims")]
    embedding_dims: i32,
    /// Optional purpose describing what kind of knowledge this space stores.
    #[serde(default)]
    purpose: Option<String>,
    /// Optional extraction skill ID (e.g. "extraction/fiction").
    #[serde(default)]
    skill: Option<String>,
}

// A space created without an embedding policy gets the deployment's policy,
// never "fake" (space config overrides the environment for that space).
fn default_provider() -> String {
    elephant_embeddings::EmbeddingPolicy::from_env().provider
}
fn default_model() -> String {
    elephant_embeddings::EmbeddingPolicy::from_env().model
}
fn default_dims() -> i32 {
    i32::try_from(elephant_embeddings::EmbeddingPolicy::from_env().dims).unwrap_or(i32::MAX)
}

/// Request body for updating a space.
#[derive(Debug, Clone, Deserialize)]
#[allow(clippy::struct_field_names)]
struct UpdateSpaceRequest {
    #[serde(default)]
    embedding_provider: Option<String>,
    #[serde(default)]
    embedding_model: Option<String>,
    #[serde(default)]
    embedding_dims: Option<i32>,
    #[serde(default)]
    purpose: Option<String>,
    #[serde(default)]
    skill: Option<String>,
}

/// GET /v1/spaces - List all space configs.
async fn list_spaces_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(_principal): axum::Extension<AuthPrincipal>,
) -> ApiResult<Vec<SpaceConfig>> {
    let configs = state
        .space_config_repo
        .list_all(100, None)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    Ok(Json(configs))
}

/// `GET /v1/spaces/{space_id}` - Get a space config by space ID.
async fn get_space_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(_principal): axum::Extension<AuthPrincipal>,
    Path(space_id): Path<String>,
) -> ApiResult<SpaceConfig> {
    let config = state
        .space_config_repo
        .get_by_space_id(&space_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Space", &space_id)),
            )
        })?;

    Ok(Json(config))
}

/// POST /v1/spaces - Create a new space config.
async fn create_space_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<CreateSpaceRequest>,
) -> Result<(StatusCode, Json<SpaceConfig>), (StatusCode, Json<ApiError>)> {
    // Check if space already exists
    if state
        .space_config_repo
        .get_by_space_id(&req.name)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .is_some()
    {
        return Err((
            StatusCode::CONFLICT,
            Json(ApiError {
                code: "CONFLICT".to_string(),
                message: format!("Space '{}' already exists", req.name),
                details: None,
            }),
        ));
    }

    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        req.name.clone(),
        principal.subject.clone(),
    );
    base.security = Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    };
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    let config = SpaceConfig {
        base,
        embedding_provider: req.embedding_provider,
        embedding_model: req.embedding_model,
        embedding_dims: req.embedding_dims,
        purpose: req.purpose,
        skill: req.skill,
    };

    let created = state.space_config_repo.create(config).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(e.to_string())),
        )
    })?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// `PUT /v1/spaces/{space_id}` - Update a space config.
async fn update_space_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(_principal): axum::Extension<AuthPrincipal>,
    Path(space_id): Path<String>,
    Json(req): Json<UpdateSpaceRequest>,
) -> ApiResult<SpaceConfig> {
    let existing = state
        .space_config_repo
        .get_by_space_id(&space_id)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Space", &space_id)),
            )
        })?;

    let updated = SpaceConfig {
        base: existing.base.clone(),
        embedding_provider: req
            .embedding_provider
            .unwrap_or_else(|| existing.embedding_provider.clone()),
        embedding_model: req
            .embedding_model
            .unwrap_or_else(|| existing.embedding_model.clone()),
        embedding_dims: req.embedding_dims.unwrap_or(existing.embedding_dims),
        purpose: req.purpose.or_else(|| existing.purpose.clone()),
        skill: req.skill.or_else(|| existing.skill.clone()),
    };

    let result = state
        .space_config_repo
        .update(&space_id, &existing.base.id, updated)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiError::internal(e.to_string())),
            )
        })?;

    Ok(Json(result))
}
