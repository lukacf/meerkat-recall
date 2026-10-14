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
use chrono::NaiveDate;
use serde::{Deserialize, Serialize};

use elephant_pipeline::predicate::PredicateProposalData;
use elephant_pipeline::predicate::PREDICATE_PROPOSAL_ARTIFACT_TYPE;
use elephant_pipeline::public_predicate_proposal::{
    PublicPredicateProposal, PublicProposalOptions,
};
use elephant_storage::public_mutation::PublicMutation;
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
use elephant_types::records::entity::Entity;
use elephant_types::records::identity::{EntityIdentityKey, IdentityKeyStatus};
use elephant_types::records::predicate::PredicateAlias;
use elephant_types::records::truth::{CurrentValue, TruthSlot};
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::records::{Doc, SpaceConfig};
use elephant_types::Link;

use crate::auth::Principal as AuthPrincipal;
use crate::authz::AuthzState;
use crate::routes::entities::ApiError;
use crate::routes::require_space_id;

#[path = "spaces_budget.rs"]
mod budget;

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
    runtime: Option<Arc<crate::runtime::RuntimeManager>>,
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
            runtime: None,
        }
    }
    /// Bind the runtime authority used for effective budget inspection.
    #[must_use]
    pub fn with_runtime(mut self, runtime: Arc<crate::runtime::RuntimeManager>) -> Self {
        self.runtime = Some(runtime);
        self
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
        .route("/api/v1/budget", get(budget::get_budget))
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
    intake: elephant_storage::source_intake::IntakeControl,
    #[serde(default)]
    source_type: Option<String>,
    #[serde(default)]
    classification: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[allow(clippy::struct_field_names)]
struct SpaceDocIngestResponse {
    doc_id: String,
    source_id: String,
    /// Physical rendered revision, absent until the accepted input is rendered.
    revision_id: Option<String>,
    work_item_id: Option<String>,
    operation: elephant_types::publication::OperationReceipt,
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

    doc.base.security.level = doc.base.security.level.max(level);
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

    let items = state
        .authz
        .filter_records(&principal, items, "read:work_item");
    Ok(Json(WorkItemListResponse {
        total: items.len(),
        results: items,
    }))
}

#[expect(
    clippy::large_futures,
    reason = "Retain the existing dispatch future layout and ownership across the original await"
)]
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
    if let Some(source_type) = req.source_type {
        let metadata = doc.metadata.as_object_mut().ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation("document metadata must be an object")),
            )
        })?;
        metadata.insert("source_type".into(), serde_json::Value::String(source_type));
    }
    let (source, _) = super::docs::build_manual_source_for_doc(&doc)
        .map_err(|error| (StatusCode::BAD_REQUEST, Json(error)))?;
    let (accepted, work_item_id) = super::source_intake::accept_intake(
        &state.storage,
        &state.authz,
        &principal,
        source,
        Some(&doc),
        req.intake,
        &[],
    )
    .await?;
    let doc = accepted.document.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiError::internal(
                "accepted document has no immutable projection",
            )),
        )
    })?;
    let mapping =
        elephant_storage::source_publication_manifest::SourcePublicationManifestRepository::new(
            state.storage.clone(),
        )
        .source_revision_for_document(&space_id, &doc.base.id)
        .await
        .map_err(super::source_intake::storage_error)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(SpaceDocIngestResponse {
            doc_id: doc.base.id.to_string(),
            source_id: accepted.source.base.id.to_string(),
            revision_id: mapping.map(|mapping| mapping.doc_revision_id.to_string()),
            work_item_id: work_item_id.map(|id| id.to_string()),
            operation: accepted.operation,
        }),
    ))
}

#[cfg(test)]
mod source_intake_tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use elephant_policy::Clearance;
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::{Source, SourceType};
    use std::collections::HashSet;

    async fn fixture() -> (Arc<SpaceApiState>, AuthPrincipal, Doc) {
        let storage = StorageClient::new_embedded().await.unwrap();
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "household".into(),
                    "test".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        let state = Arc::new(SpaceApiState::new(
            storage,
            Arc::new(crate::authz::AuthzService::new()),
        ));
        let principal = AuthPrincipal {
            subject: "tester".into(),
            space_id: Some("household".into()),
            spaces: None,
            scopes: [
                "read:doc:metadata",
                "write:doc",
                "read:source",
                "write:source",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            security_level: 0,
            security_labels: vec![],
            clearances: vec![Clearance {
                policy_id: "default".into(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: vec![],
            bypass_abac: false,
        };
        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "household".into(),
                "test".into(),
            ),
            title: "Retained source input".into(),
            text: "First version".into(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: "request".into(),
            metadata: serde_json::json!({"extraction_purpose":"facts"}),
        };
        (state, principal, doc)
    }

    #[tokio::test]
    #[expect(
        clippy::default_trait_access,
        clippy::large_futures,
        reason = "Retain the original inferred defaults in the source and work-run fixtures; Retain the original test dispatch future layout and await boundaries"
    )]
    async fn document_helper_accepts_original_fence_and_preserves_prior_revision() {
        let (state, principal, doc) = fixture().await;
        let request = SpaceDocIngestRequest {
            doc: doc.clone(),
            intake: Default::default(),
            source_type: Some("text/plain".into()),
            classification: None,
        };
        let (status, Json(first)) = create_document(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Path("household".into()),
            Json(request),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(
            first.revision_id.is_none(),
            "rendered revision must not be fabricated"
        );
        let work_id: RecordId = first.work_item_id.as_ref().unwrap().parse().unwrap();
        let work = state
            .work_item_repo
            .get("household", &work_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            work.payload["_publication"]["operation_id"],
            first.operation.operation_id
        );
        assert_eq!(
            work.payload["_publication"]["generation"],
            first.operation.source_generation
        );
        let old_id: RecordId = first.doc_id.parse().unwrap();
        let mut next = doc;
        next.text = "Second version".into();
        let request = SpaceDocIngestRequest {
            doc: next,
            source_type: Some("text/plain".into()),
            classification: None,
            intake: elephant_storage::source_intake::IntakeControl {
                revision_id: Some("second".into()),
                idempotency_key: Some("second-key".into()),
                expected_generation: Some(first.operation.source_generation),
            },
        };
        let (_, Json(second)) = create_document(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(request),
        )
        .await
        .unwrap();
        assert_ne!(second.doc_id, first.doc_id);
        assert_eq!(
            state
                .doc_repo
                .get("household", &old_id)
                .await
                .unwrap()
                .unwrap()
                .text,
            "First version"
        );
    }

    fn contact_source() -> Source {
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "household".into(),
                "caller".into(),
            ),
            SourceType::Manual,
            "Contact import".into(),
        );
        source.external_id = Some("address-book:alice".into());
        source.uri = Some("private://address-book".into());
        source.metadata = serde_json::json!({"contact":{
            "name":"Alice", "email":"ALICE@example.invalid", "company":"Example Org", "job_title":"Engineer"
        }});
        source
    }

    fn contact_scopes(principal: &mut AuthPrincipal) {
        principal.scopes.extend(
            [
                "read:entity",
                "write:entity",
                "read:relationship",
                "write:relationship",
            ]
            .into_iter()
            .map(str::to_owned),
        );
    }

    #[tokio::test]
    #[expect(
        clippy::default_trait_access,
        clippy::large_futures,
        reason = "Retain the original inferred defaults in the source and work-run fixtures; Retain the original test dispatch future layout and await boundaries"
    )]
    async fn contact_helper_seals_intent_and_queues_original_fence_without_graph_writes() {
        let (state, mut principal, _) = fixture().await;
        contact_scopes(&mut principal);
        let source = contact_source();
        let request = || super::super::sources::SourceWriteRequest {
            source: source.clone(),
            intake: Default::default(),
        };
        let (status, Json(first)) = create_source_in_space(
            State(state.clone()),
            axum::Extension(principal.clone()),
            Path("household".into()),
            Json(request()),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(first["source"]["uri"].is_null());
        assert!(first["source"]["metadata"].get("_manual_doc").is_none());
        assert!(
            first["operation"].get("source").is_none(),
            "operation receipt contains no raw source"
        );
        let operation: elephant_types::publication::OperationReceipt =
            serde_json::from_value(first["operation"].clone()).unwrap();
        let work_id: RecordId = first["work_item_id"].as_str().unwrap().parse().unwrap();
        let work = state
            .work_item_repo
            .get("household", &work_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            work.payload["_publication"]["operation_id"],
            operation.operation_id
        );
        assert_eq!(
            work.payload["_publication"]["generation"],
            operation.source_generation
        );
        assert_eq!(
            work.payload["_publication"]["revision_id"],
            operation.desired_revision.as_ref().unwrap().as_str()
        );
        assert!(state
            .entity_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(state
            .identity_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(state
            .rel_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        let fence: elephant_types::publication::PublicationFence =
            serde_json::from_value(work.payload["_publication"].clone()).unwrap();
        let mut plan = elephant_storage::canonical_commit::CanonicalPlan::default();
        elephant_storage::canonical_commit::CanonicalRepository::new(state.storage.clone())
            .append_accepted_source_effects(&mut plan, "household", &fence)
            .await
            .unwrap();
        assert_eq!(
            plan.effects.len(),
            4,
            "person, key, company and relationship are sealed for whole-source finalization"
        );
        let (_, Json(retry)) = create_source_in_space(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(request()),
        )
        .await
        .unwrap();
        assert_eq!(
            retry["operation"]["operation_id"],
            first["operation"]["operation_id"]
        );
        assert_eq!(
            retry["operation"]["source_generation"],
            first["operation"]["source_generation"]
        );
    }

    #[tokio::test]
    #[expect(
        clippy::default_trait_access,
        clippy::large_futures,
        reason = "Retain the original inferred defaults in the source and work-run fixtures; Retain the original test dispatch future layout and await boundaries"
    )]
    async fn contact_helper_denies_hidden_company_before_acceptance_or_partial_person() {
        let (state, mut principal, _) = fixture().await;
        contact_scopes(&mut principal);
        let mut company = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "household".into(),
                "fixture".into(),
            ),
            EntityType::new("org"),
            "Example Org".into(),
        );
        company.base.security.level = 5;
        let company = state.entity_repo.create(company).await.unwrap();
        let error = create_source_in_space(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(super::super::sources::SourceWriteRequest {
                source: contact_source(),
                intake: Default::default(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert_eq!(
            state
                .entity_repo
                .list("household", 100, None)
                .await
                .unwrap(),
            vec![company]
        );
        assert!(state
            .source_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(state
            .identity_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(state
            .work_item_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(
            elephant_storage::SourcePublicationRepository::new(state.storage.clone())
                .external_head("household", SourceType::Manual, "address-book:alice")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    #[expect(
        clippy::default_trait_access,
        clippy::large_futures,
        reason = "Retain the original inferred defaults in the source and work-run fixtures; Retain the original test dispatch future layout and await boundaries"
    )]
    async fn source_scopes_alone_do_not_grant_contact_mutation() {
        let (state, principal, _) = fixture().await;
        let error = create_source_in_space(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(super::super::sources::SourceWriteRequest {
                source: contact_source(),
                intake: Default::default(),
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert!(state
            .source_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
        assert!(state
            .work_item_repo
            .list("household", 100, None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn manual_entity_and_identity_keys_share_one_canonical_commit() {
        let (state, mut principal, _) = fixture().await;
        principal
            .scopes
            .extend(["read:entity".to_owned(), "write:entity".to_owned()]);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "household".into(),
            "test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        let mut entity = Entity::new(base, EntityType::new("person"), "Independent person".into());
        entity.identity_keys = vec![
            "email:person@example.invalid".into(),
            "profile:person".into(),
        ];
        let (status, Json(created)) = create_entity(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(entity),
        )
        .await
        .unwrap();
        assert_eq!(status, StatusCode::CREATED);
        for key in &created.identity_keys {
            let stored = state
                .identity_repo
                .find_by_key("household", key)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(stored.entity.id(), &created.base.id);
            assert_eq!(stored.base.security, created.base.security);
        }
        let events = elephant_storage::SystemEventRepository::new(state.storage.clone())
            .list("household", 100, None)
            .await
            .unwrap();
        assert!(!events.is_empty());
        assert!(events.iter().all(|event| event.seq > 0));
        assert!(
            state
                .source_repo
                .list("household", 100, None)
                .await
                .unwrap()
                .is_empty(),
            "independent manual entity needs no manufactured source"
        );
    }

    #[tokio::test]
    async fn hidden_identity_key_denies_entity_creation_without_partial_records() {
        let (state, mut principal, _) = fixture().await;
        principal
            .scopes
            .extend(["read:entity".to_owned(), "write:entity".to_owned()]);
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::EntityIdentityKey),
            "household".into(),
            "test".into(),
        );
        base.security.level = 5;
        let key = EntityIdentityKey {
            base,
            key: "private-key".into(),
            entity: Link::new(RecordId::new_v4(TableName::Entity)),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        };
        state.identity_repo.create(key.clone()).await.unwrap();
        let mut entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "household".into(),
                "test".into(),
            ),
            EntityType::new("person"),
            "Denied person".into(),
        );
        entity.identity_keys = vec!["fresh-key".into(), "private-key".into()];
        let id = entity.base.id.clone();
        let error = create_entity(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(entity),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert!(state
            .entity_repo
            .get("household", &id)
            .await
            .unwrap()
            .is_none());
        assert!(state
            .identity_repo
            .find_by_key("household", "fresh-key")
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            state
                .identity_repo
                .find_by_key("household", "private-key")
                .await
                .unwrap()
                .unwrap(),
            key
        );
        assert!(
            elephant_storage::SystemEventRepository::new(state.storage.clone())
                .list("household", 100, None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    #[expect(
        clippy::default_trait_access,
        clippy::large_futures,
        reason = "Retain the original inferred defaults in the source and work-run fixtures; Retain the original test dispatch future layout and await boundaries"
    )]
    async fn document_helper_authorizes_old_doc_before_any_acceptance() {
        let (state, principal, doc) = fixture().await;
        let mut protected = doc.clone();
        protected.base.security.level = 5;
        state.doc_repo.create(protected.clone()).await.unwrap();
        let error = create_document(
            State(state.clone()),
            axum::Extension(principal),
            Path("household".into()),
            Json(SpaceDocIngestRequest {
                doc,
                intake: Default::default(),
                source_type: None,
                classification: None,
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(error.0, StatusCode::FORBIDDEN);
        assert_eq!(
            state
                .doc_repo
                .get("household", &protected.base.id)
                .await
                .unwrap()
                .unwrap(),
            protected
        );
        assert!(state
            .source_repo
            .list("household", 10, None)
            .await
            .unwrap()
            .is_empty());
        assert!(state
            .work_item_repo
            .list("household", 10, None)
            .await
            .unwrap()
            .is_empty());
    }
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

    Ok(Json(
        filtered
            .into_iter()
            .map(|doc| crate::redact_doc(&principal, doc).data)
            .collect(),
    ))
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
            Ok(Json(crate::redact_doc(&principal, doc).data))
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

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain guard, repository and runtime owners through the original response or state transition scope"
)]
async fn create_entity(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Json(mut entity): Json<Entity>,
) -> Result<(StatusCode, Json<Entity>), (StatusCode, Json<ApiError>)> {
    use elephant_storage::canonical_commit::CanonicalRecord;
    use elephant_storage::public_mutation::PublicMutation;
    let space_id = ensure_space(&principal, &space)?;
    entity.base.space_id.clone_from(&space_id);
    ensure_policy_defaults(&mut entity.base.security);
    let mut mutation = PublicMutation::begin(state.storage.clone(), &space_id)
        .await
        .map_err(super::source_intake::storage_error)?;
    let denied = |error: crate::authz::AuthzError| {
        (
            StatusCode::FORBIDDEN,
            Json(ApiError::validation(error.message)),
        )
    };
    state
        .authz
        .authorize_record_read(&principal, &entity, "read:entity")
        .map_err(denied)?;
    state
        .authz
        .authorize_record_write(&principal, &entity, "write:entity")
        .map_err(denied)?;
    let mut keys = entity.identity_keys.clone();
    keys.sort();
    keys.dedup();
    for key in keys {
        if let Some(existing) = state
            .identity_repo
            .find_by_key(&space_id, &key)
            .await
            .map_err(super::source_intake::storage_error)?
        {
            state
                .authz
                .authorize_record_read(&principal, &existing, "read:entity")
                .map_err(denied)?;
            state
                .authz
                .authorize_record_write(&principal, &existing, "write:entity")
                .map_err(denied)?;
            if existing.entity.id() != &entity.base.id || existing.base.deleted_at.is_some() {
                return Err(super::source_intake::storage_error(
                    elephant_storage::StorageError::Conflict(
                        "identity key belongs to another identity; use explicit merge".into(),
                    ),
                ));
            }
            mutation.depend_on(CanonicalRecord::EntityIdentityKey(Box::new(existing)));
            continue;
        }
        let bytes = serde_json::to_vec(&(&space_id, &key)).map_err(|error| {
            (
                StatusCode::BAD_REQUEST,
                Json(ApiError::validation(error.to_string())),
            )
        })?;
        let mut base = entity.base.clone();
        base.id = RecordId::new(
            TableName::EntityIdentityKey,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, &bytes),
        );
        base.created_by.clone_from(&principal.subject);
        let record = EntityIdentityKey {
            base,
            key,
            entity: Link::new(entity.base.id.clone()),
            confidence: 1.0,
            first_seen: Some(entity.base.created_at),
            last_seen: Some(entity.base.updated_at),
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        };
        state
            .authz
            .authorize_record_read(&principal, &record, "read:entity")
            .map_err(denied)?;
        state
            .authz
            .authorize_record_write(&principal, &record, "write:entity")
            .map_err(denied)?;
        mutation
            .stage_create(CanonicalRecord::EntityIdentityKey(Box::new(record)))
            .map_err(super::source_intake::storage_error)?;
    }
    let created = mutation
        .create(entity, &principal.subject)
        .await
        .map_err(super::source_intake::storage_error)?;
    Ok((StatusCode::CREATED, Json(created)))
}

#[expect(
    clippy::large_futures,
    reason = "Retain the existing dispatch future layout and ownership across the original await"
)]
async fn create_source_in_space(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space): Path<String>,
    Json(request): Json<super::sources::SourceWriteRequest>,
) -> Result<(StatusCode, Json<serde_json::Value>), (StatusCode, Json<ApiError>)> {
    let mut source = request.source;
    source.base.space_id = ensure_space(&principal, &space)?;
    source.base.created_by.clone_from(&principal.subject);
    ensure_policy_defaults(&mut source.base.security);
    source.set_requested_ingest_mode(elephant_types::records::ingest::IngestMode::Metadata);
    let (accepted, work_item_id) = super::source_intake::accept_contact_intake(
        &state.storage,
        &state.authz,
        &principal,
        source,
        request.intake,
    )
    .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({
            "source": crate::redaction::redact_source(&principal, accepted.source).data,
            "operation": accepted.operation,
            "work_item_id": work_item_id,
        })),
    ))
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

#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Retain the existing dispatch future layout and ownership across the original await; Retain guard, repository and runtime owners through the original response or state transition scope"
)]
async fn approve_predicate_proposal(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(id): Path<String>,
) -> ApiResult<PredicateAlias> {
    let space_id = require_space_id(&principal)?;
    state
        .authz
        .authorize_space_scope(&principal, &space_id, "write:predicate")
        .map_err(predicate_proposal_auth_error)?;
    let artifact_id = elephant_storage::parse_id(&id, TableName::WorkArtifact).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(e.to_string())),
        )
    })?;
    let mutation = PublicMutation::begin(state.storage.clone(), &space_id)
        .await
        .map_err(|error| ApiError::storage(&error))?;
    let artifact = state
        .work_artifact_repo
        .get(&space_id, &artifact_id)
        .await
        .map_err(|error| ApiError::storage(&error))?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("WorkArtifact", &id)),
            )
        })?;
    authorize_predicate_proposal_record(&state, &principal, &artifact.base)?;
    let observed = PublicPredicateProposal::load(
        state.storage.clone(),
        mutation,
        artifact,
        PublicProposalOptions {
            allow_draft: true,
            ensure_registry: true,
            check_work_run_budget: true,
            confidence: None,
        },
    )
    .await
    .map_err(predicate_proposal_error)?;
    if let Some(registry) = &observed.registry {
        authorize_predicate_proposal_record(&state, &principal, registry)?;
    }
    for alias in &observed.aliases {
        authorize_predicate_proposal_record(&state, &principal, alias)?;
    }
    if let Some(run) = &observed.work_run {
        state
            .authz
            .authorize_record_read(&principal, &run.base, "read:predicate")
            .map_err(predicate_proposal_auth_error)?;
    }
    let prepared = observed
        .prepare(&principal.subject)
        .map_err(predicate_proposal_error)?;
    for base in std::iter::once(&prepared.artifact.base)
        .chain(
            prepared
                .registry_create
                .iter()
                .map(|registry| &registry.base),
        )
        .chain(prepared.aliases.iter().map(|alias| &alias.base))
        .chain(std::iter::once(&prepared.reprocess_job.base))
    {
        state
            .authz
            .authorize_record_write(&principal, base, "write:predicate")
            .map_err(predicate_proposal_auth_error)?;
    }
    prepared
        .commit(&principal.subject)
        .await
        .map(Json)
        .map_err(predicate_proposal_error)
}

#[expect(
    clippy::result_large_err,
    reason = "Retain the shared status and public JSON error representation at the route boundary"
)]
fn authorize_predicate_proposal_record(
    state: &SpaceApiState,
    principal: &AuthPrincipal,
    record: &impl elephant_types::records::HasSecurity,
) -> Result<(), (StatusCode, Json<ApiError>)> {
    state
        .authz
        .authorize_record_read(principal, record, "read:predicate")
        .map_err(predicate_proposal_auth_error)?;
    state
        .authz
        .authorize_record_write(principal, record, "write:predicate")
        .map_err(predicate_proposal_auth_error)
}

fn predicate_proposal_auth_error(error: crate::authz::AuthzError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: error.code,
            message: error.message,
            details: None,
        }),
    )
}

fn predicate_proposal_error(
    error: elephant_pipeline::PipelineError,
) -> (StatusCode, Json<ApiError>) {
    if let elephant_pipeline::PipelineError::StorageError(error) = error {
        return ApiError::storage(&error);
    }
    let status = match error {
        elephant_pipeline::PipelineError::InvalidInput(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(ApiError::internal(error.to_string())))
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
#[serde(deny_unknown_fields)]
struct CreateSpaceRequest {
    #[serde(default)]
    processing: Option<elephant_types::provider_binding::SpaceProcessingConfig>,
    /// Space name / ID.
    name: String,
    /// Initial administration envelope and default for new source envelopes.
    #[serde(default)]
    security: Option<Security>,
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
#[serde(deny_unknown_fields)]
#[allow(clippy::struct_field_names)]
struct UpdateSpaceRequest {
    #[serde(default)]
    processing: Option<elephant_types::provider_binding::SpaceProcessingConfig>,
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
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
) -> ApiResult<Vec<SpaceConfig>> {
    let configs = if principal.bypass_abac {
        state.space_config_repo.list_all(100, None).await
    } else {
        let space_id = require_space_id(&principal)?;
        state
            .authz
            .authorize_space_scope(&principal, &space_id, "read:space_config")
            .map_err(space_authorization_error)?;
        state
            .space_config_repo
            .get_by_space_id(&space_id)
            .await
            .map(|config| config.into_iter().collect())
    }
    .map_err(crate::public_error::legacy_storage_failure)?;
    Ok(Json(state.authz.filter_records(
        &principal,
        configs,
        "read:space_config",
    )))
}

#[expect(
    clippy::result_large_err,
    reason = "Retain the shared status and public JSON error representation at the route boundary"
)]
fn require_provider_admin(principal: &AuthPrincipal) -> Result<(), (StatusCode, Json<ApiError>)> {
    if principal.bypass_abac
        || principal
            .scopes
            .iter()
            .any(|scope| scope == "manage:provider")
    {
        return Ok(());
    }
    Err((
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: "ACCESS_DENIED".into(),
            message: "Processing changes require explicit manage:provider scope".into(),
            details: None,
        }),
    ))
}

fn space_authorization_error(error: crate::authz::AuthzError) -> (StatusCode, Json<ApiError>) {
    (
        StatusCode::FORBIDDEN,
        Json(ApiError {
            code: error.code,
            message: error.message,
            details: None,
        }),
    )
}

/// `GET /v1/spaces/{space_id}` - Get a space config by space ID.
async fn get_space_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space_id): Path<String>,
) -> ApiResult<SpaceConfig> {
    state
        .authz
        .authorize_space_scope(&principal, &space_id, "read:space_config")
        .map_err(space_authorization_error)?;
    let config = state
        .space_config_repo
        .get_by_space_id(&space_id)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Space", &space_id)),
            )
        })?;

    state
        .authz
        .authorize_record_read(&principal, &config, "read:space_config")
        .map_err(space_authorization_error)?;
    Ok(Json(config))
}

/// POST /v1/spaces - Create a new space config.
async fn create_space_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Json(req): Json<CreateSpaceRequest>,
) -> Result<(StatusCode, Json<SpaceConfig>), (StatusCode, Json<ApiError>)> {
    state
        .authz
        .authorize_space_scope(&principal, &req.name, "write:space_config")
        .map_err(space_authorization_error)?;
    state
        .authz
        .authorize_space_scope(&principal, &req.name, "read:space_config")
        .map_err(space_authorization_error)?;
    elephant_types::validation::validate_space_id(&req.name).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(error.to_string())),
        )
    })?;
    if req.processing.is_some() {
        require_provider_admin(&principal)?;
    }
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        req.name.clone(),
        principal.subject.clone(),
    );
    base.security = req.security.unwrap_or_else(|| Security {
        policy_id: "default".to_string(),
        level: 0,
        labels: vec![],
        handling: vec![],
    });
    elephant_types::validation::validate_security(&base.security).map_err(|error| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiError::validation(error.to_string())),
        )
    })?;
    base.provenance = Provenance::default();
    base.subjects_state = SubjectsState::Complete;

    let config = SpaceConfig {
        base,
        embedding_provider: req.embedding_provider,
        embedding_model: req.embedding_model,
        embedding_dims: req.embedding_dims,
        purpose: req.purpose,
        skill: req.skill,
        processing: req.processing,
    };

    state
        .authz
        .authorize_record_write(&principal, &config, "write:space_config")
        .map_err(space_authorization_error)?;
    state
        .authz
        .authorize_record_read(&principal, &config, "read:space_config")
        .map_err(space_authorization_error)?;
    // Both the proposed envelope and an existing record require read authority.
    // An inaccessible existing space must not become an existence oracle.
    if let Some(existing) = state
        .space_config_repo
        .get_by_space_id(&req.name)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?
    {
        state
            .authz
            .authorize_record_read(&principal, &existing, "read:space_config")
            .map_err(space_authorization_error)?;
        return Err((
            StatusCode::CONFLICT,
            Json(ApiError {
                code: "CONFLICT".into(),
                message: format!("Space '{}' already exists", req.name),
                details: None,
            }),
        ));
    }
    let created = state
        .space_config_repo
        .create(config)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?;

    Ok((StatusCode::CREATED, Json(created)))
}

/// `PUT /v1/spaces/{space_id}` - Update a space config.
async fn update_space_v1(
    State(state): State<Arc<SpaceApiState>>,
    axum::Extension(principal): axum::Extension<AuthPrincipal>,
    Path(space_id): Path<String>,
    Json(req): Json<UpdateSpaceRequest>,
) -> ApiResult<SpaceConfig> {
    state
        .authz
        .authorize_space_scope(&principal, &space_id, "write:space_config")
        .map_err(space_authorization_error)?;
    if req.processing.is_some() {
        require_provider_admin(&principal)?;
    }
    let existing = state
        .space_config_repo
        .get_by_space_id(&space_id)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(ApiError::not_found("Space", &space_id)),
            )
        })?;

    state
        .authz
        .authorize_record_write(&principal, &existing, "write:space_config")
        .map_err(space_authorization_error)?;
    state
        .authz
        .authorize_record_read(&principal, &existing, "read:space_config")
        .map_err(space_authorization_error)?;
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
        processing: req.processing.or_else(|| existing.processing.clone()),
    };

    let result = state
        .space_config_repo
        .replace_authorized(&existing, updated)
        .await
        .map_err(crate::public_error::legacy_storage_failure)?;

    Ok(Json(result))
}

#[cfg(test)]
#[path = "spaces_predicate_tests.rs"]
mod guarded_predicate_tests;
