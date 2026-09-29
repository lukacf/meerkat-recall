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

use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::{self, Next},
    response::IntoResponse,
};
use chrono::{TimeZone, Utc};
use std::str::FromStr;
use std::sync::Arc;
use tower::ServiceExt;

use elephant_api::routes::timeline::{timeline_routes, TimelineState};
use elephant_api::{AuthzService, Principal as AuthPrincipal};
use elephant_pipeline::error::PipelineError;
use elephant_pipeline::extraction::{ExtractionContext, ExtractionResult, Extractor, RawAssertion};
use elephant_pipeline::scheduler::execute_extract_assertions;
use elephant_pipeline::scheduler::extraction_work_kinds::ExtractionArtifactData;
use elephant_pipeline::scheduler::promote_assertions_inline;
use elephant_pipeline::truth_maintenance::{
    execute_tm_commit, execute_tm_detect_conflicts, execute_tm_detect_conflicts_from_outbox_events,
    resolve_conflict_group_with_storage, RepositoryTruthMaintenanceStorage, TmContext,
    TriggerEvent,
};
use elephant_storage::{
    ConflictGroupRepository, DocNodeRepository, DocRepository, DocRevisionRepository,
    EventRepository, Repository, SystemEventRepository, WorkArtifactRepository, WorkItemRepository,
    WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::event::EventStatus;
use elephant_types::records::work::{
    Determinism, PluginManifest, RetryPolicy, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::{Link, RecordId};

use crate::Harness;

async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
    let mut req = req;
    req.extensions_mut().insert(AuthPrincipal {
        subject: "scenario-user".to_string(),
        space_id: Some("default".to_string()),
        scopes: vec!["read:event".to_string(), "write:event".to_string()],
        security_level: 10,
        security_labels: vec![],
        ..AuthPrincipal::default()
    });
    next.run(req).await
}

#[derive(Debug)]
struct FixtureExtractor {
    manifest: PluginManifest,
    assertions: Vec<RawAssertion>,
}

impl FixtureExtractor {
    fn new(assertions: Vec<RawAssertion>) -> Self {
        Self {
            manifest: PluginManifest {
                plugin_id: "fixture.extractor.events".to_string(),
                version: "1.0.0".to_string(),
                plugin_type: "extractor".to_string(),
                required_scopes: Vec::new(),
                tool_access: Vec::new(),
                work_kinds: vec!["extract_assertions".to_string()],
                determinism: Determinism::Deterministic,
                max_tokens: None,
                max_wall_time_secs: None,
                input_types: vec!["doc_node".to_string()],
                output_types: vec!["knowledge_candidates".to_string()],
                validator_profile: None,
                config_schema: None,
                health_check: None,
                retry_policy: RetryPolicy::default(),
            },
            assertions,
        }
    }
}

#[async_trait]
impl Extractor for FixtureExtractor {
    fn manifest(&self) -> &PluginManifest {
        &self.manifest
    }

    async fn extract(&self, _ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        Ok(ExtractionResult::new(
            self.assertions.clone(),
            0,
            "fixture-model".to_string(),
        ))
    }
}

fn make_event_assertion(confidence: f64, event_time: chrono::DateTime<Utc>) -> RawAssertion {
    RawAssertion {
        subject_text: "Alice".to_string(),
        predicate_text: "announced".to_string(),
        object_text: Some("Project Phoenix".to_string()),
        object_literal: None,
        evidence_span: EvidenceSpan {
            revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
            node: None,
            start: 0,
            end: 32,
            quote_hash: "fixture-hash".to_string(),
        },
        confidence,
        quote: Some("Alice announced Project Phoenix on stage.".to_string()),
        source_event_time: Some(event_time),
        source_event_time_precision: Some(elephant_types::records::TimePrecision::Exact),
        event_title: Some("Alice announced Project Phoenix".to_string()),
        event_lineage_key: None,
        subject_entity_id: None,
        subject_entity_type: Some("person".to_string()),
        object_entity_id: None,
        object_entity_type: Some("topic".to_string()),
        significance: Some("high".to_string()),
    }
}

async fn create_revision_with_node(
    harness: &Harness,
    space_id: &str,
    actor: &str,
    text: &str,
) -> anyhow::Result<(RecordId, RecordId)> {
    let doc_repo = DocRepository::new(harness.client.clone());
    let revision_repo = DocRevisionRepository::new(harness.client.clone());
    let node_repo = DocNodeRepository::new(harness.client.clone());

    let doc_id = RecordId::new_v4(TableName::Doc);
    let doc = Doc {
        base: BaseFields::new(doc_id.clone(), space_id.to_string(), actor.to_string()),
        title: "Event Notes".to_string(),
        text: text.to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: format!("fixture-{}", uuid::Uuid::new_v4()),
        metadata: serde_json::json!({}),
    };
    doc_repo.create(doc).await?;

    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let revision = DocRevision {
        base: BaseFields::new(revision_id.clone(), space_id.to_string(), actor.to_string()),
        doc: Link::new(doc_id),
        text: text.to_string(),
        text_hash: "fixture-hash".to_string(),
        renderer: RendererMetadata {
            name: "fixture_renderer".to_string(),
            version: "1.0.0".to_string(),
            config_hash: None,
        },
        manifest: None,
        supersedes: None,
        status: RevisionStatus::Active,
        rendered_at: Utc::now(),
    };
    revision_repo.create(revision).await?;

    let node_id = RecordId::new_v4(TableName::DocNode);
    let node = DocNode {
        base: BaseFields::new(node_id.clone(), space_id.to_string(), actor.to_string()),
        revision: Link::new(revision_id.clone()),
        role: NodeRole::Paragraph,
        span: TextSpan::new(0, u32::try_from(text.len()).unwrap_or(u32::MAX)),
        node_key: format!("node:{node_id}"),
        text: None,
        parent: None,
        position: 0,
        depth: 1,
        embedding_model: None,
        collection: None,
    };
    node_repo.create(node).await?;

    Ok((revision_id, node_id))
}

async fn create_work_run(
    harness: &Harness,
    space_id: &str,
    actor: &str,
) -> anyhow::Result<RecordId> {
    let item_repo = WorkItemRepository::new(harness.client.clone());
    let run_repo = WorkRunRepository::new(harness.client.clone());

    let item_id = RecordId::new_v4(TableName::WorkItem);
    let item = WorkItem {
        base: BaseFields::new(item_id.clone(), space_id.to_string(), actor.to_string()),
        kind: "extract_assertions".to_string(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Runnable,
        priority: 5,
        idempotency_key: format!("fixture:{}", uuid::Uuid::new_v4()),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    item_repo.create(item).await?;

    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let run = WorkRun {
        base: BaseFields::new(work_run_id.clone(), space_id.to_string(), actor.to_string()),
        work_item: Link::new(item_id),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: None,
        success: false,
        error: None,
        metrics: WorkRunMetrics::default(),
        worker_id: "fixture-worker".to_string(),
        worker_version: "1.0.0".to_string(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    run_repo.create(run).await?;
    Ok(work_run_id)
}

async fn promote_extraction_artifact(
    storage: &elephant_storage::StorageClient,
    space_id: &str,
    artifact_id: &RecordId,
    work_run_id: &RecordId,
) -> anyhow::Result<()> {
    let artifact_repo = WorkArtifactRepository::new(storage.clone());
    let artifact = artifact_repo
        .get(space_id, artifact_id)
        .await?
        .ok_or_else(|| anyhow::anyhow!("extraction artifact not found: {artifact_id}"))?;
    let data: ExtractionArtifactData = serde_json::from_value(artifact.data)?;
    let revision_id = RecordId::from_str(&data.revision_id)?;
    let node_id = RecordId::from_str(&data.node_id)?;
    let assertions = data
        .assertions
        .iter()
        .map(|assertion| assertion.to_raw(&revision_id, &node_id))
        .collect::<Result<Vec<_>, _>>()?;

    promote_assertions_inline(
        storage,
        space_id,
        &assertions,
        work_run_id,
        &node_id,
        &revision_id,
        "scenario",
    )
    .await?;

    Ok(())
}

#[tokio::test]
async fn scenario_e2e_001_ingest_extract_yields_timeline_events() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "default";
    let actor = "scenario";
    let text = "Alice announced Project Phoenix during the launch event.";
    let event_time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();

    let (revision_id, node_id) = create_revision_with_node(&harness, space_id, actor, text).await?;
    let work_run_id = create_work_run(&harness, space_id, actor).await?;
    let extractor = FixtureExtractor::new(vec![make_event_assertion(0.91, event_time)]);

    let result = execute_extract_assertions(
        &harness.client,
        &extractor,
        &node_id,
        &revision_id,
        &work_run_id,
        space_id,
        actor,
        None,
    )
    .await?;
    promote_extraction_artifact(&harness.client, space_id, &result.artifact_id, &work_run_id)
        .await?;

    let event_repo = EventRepository::new(harness.client.clone());
    let mut events = event_repo.get_recent(space_id, 10).await?;
    assert_eq!(
        events.len(),
        1,
        "Expected one timeline event from extraction"
    );
    assert_eq!(events[0].status, EventStatus::Approved);
    assert_eq!(events[0].time_start, Some(event_time));

    let event_outbox = SystemEventRepository::new(harness.client.clone())
        .get_after_seq(space_id, 0, 20)
        .await?;
    assert!(
        !event_outbox.is_empty(),
        "Expected outbox records after inline promotion"
    );

    let tm_storage = RepositoryTruthMaintenanceStorage::new(harness.client.clone());
    let detect = execute_tm_detect_conflicts_from_outbox_events(&event_outbox, &tm_storage).await?;
    for commit in detect.direct_commits {
        execute_tm_commit(&commit.resolution, &commit.input, space_id, &tm_storage).await?;
    }
    for conflict_group_id in detect.conflict_group_ids {
        let conflict_repo = ConflictGroupRepository::new(harness.client.clone());
        let Some(group) = conflict_repo.get(space_id, &conflict_group_id).await? else {
            continue;
        };
        let (resolution, input) =
            resolve_conflict_group_with_storage(&TmContext::none(), &group, space_id, &tm_storage)
                .await?;
        execute_tm_commit(&resolution, &input, space_id, &tm_storage).await?;
    }

    events = event_repo.get_recent(space_id, 10).await?;
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].status,
        EventStatus::Approved,
        "Single event group should auto-promote to approved"
    );

    // E2E proof: query through timeline API surface, not repository-only checks.
    let authz = Arc::new(AuthzService::new());
    let timeline_state = Arc::new(TimelineState::new(harness.client.clone(), authz));
    let app = timeline_routes()
        .layer(middleware::from_fn(mock_auth))
        .with_state(timeline_state);

    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/timeline/recent?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let api_events: Vec<serde_json::Value> = serde_json::from_slice(&body)?;
    assert!(
        api_events.iter().any(|event| {
            event["title"]
                .as_str()
                .is_some_and(|title| title == "Alice announced Project Phoenix")
        }),
        "Expected approved pipeline event to be queryable via timeline API"
    );

    Ok(())
}

#[tokio::test]
async fn scenario_e2e_002_contradictory_event_candidates_resolve() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = "default";
    let actor = "scenario";
    let text = "Alice announced Project Phoenix during the launch event.";
    let event_time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();

    let (revision_id_a, node_id_a) =
        create_revision_with_node(&harness, space_id, actor, text).await?;
    let (revision_id_b, node_id_b) =
        create_revision_with_node(&harness, space_id, actor, text).await?;
    let work_run_a = create_work_run(&harness, space_id, actor).await?;
    let work_run_b = create_work_run(&harness, space_id, actor).await?;

    let extractor_a = FixtureExtractor::new(vec![make_event_assertion(0.93, event_time)]);
    let extractor_b = FixtureExtractor::new(vec![make_event_assertion(0.51, event_time)]);

    let result_a = execute_extract_assertions(
        &harness.client,
        &extractor_a,
        &node_id_a,
        &revision_id_a,
        &work_run_a,
        space_id,
        actor,
        None,
    )
    .await?;
    promote_extraction_artifact(
        &harness.client,
        space_id,
        &result_a.artifact_id,
        &work_run_a,
    )
    .await?;
    let result_b = execute_extract_assertions(
        &harness.client,
        &extractor_b,
        &node_id_b,
        &revision_id_b,
        &work_run_b,
        space_id,
        actor,
        None,
    )
    .await?;
    promote_extraction_artifact(
        &harness.client,
        space_id,
        &result_b.artifact_id,
        &work_run_b,
    )
    .await?;

    let tm_storage = RepositoryTruthMaintenanceStorage::new(harness.client.clone());
    let event_repo = EventRepository::new(harness.client.clone());
    let seeded_events = event_repo.get_recent(space_id, 10).await?;
    let triggers: Vec<TriggerEvent> = seeded_events
        .iter()
        .map(|event| TriggerEvent::Event(event.base.id.clone()))
        .collect();
    let detect = execute_tm_detect_conflicts(&triggers, space_id, &tm_storage).await?;

    // Value-aware detection: event slots never open conflict groups; the
    // shared slot resolves in a single ranked direct commit.
    assert!(
        detect.conflict_group_ids.is_empty(),
        "event slots must resolve via direct commits, not conflict groups: {:?}",
        detect.conflict_group_ids
    );
    assert_eq!(
        detect.direct_commits.len(),
        1,
        "both candidates share one event slot and one direct commit"
    );
    for commit in detect.direct_commits {
        execute_tm_commit(&commit.resolution, &commit.input, space_id, &tm_storage).await?;
    }

    // The candidates differ in status (0.93 auto-approved at promotion, 0.51
    // stayed Candidate), so this is deterministic ranked supersession — NOT
    // corroboration: both rows stay live, nothing is soft-deleted.
    let events = event_repo.get_recent(space_id, 10).await?;
    assert_eq!(
        events.len(),
        2,
        "both event candidates must stay live after ranked resolution: {events:#?}"
    );
    let winner = events
        .iter()
        .find(|event| (event.confidence - 0.93).abs() < 0.001)
        .unwrap_or_else(|| panic!("high-confidence candidate missing: {events:#?}"));
    let loser = events
        .iter()
        .find(|event| (event.confidence - 0.51).abs() < 0.001)
        .unwrap_or_else(|| panic!("low-confidence candidate missing: {events:#?}"));
    assert_eq!(
        winner.status,
        EventStatus::Approved,
        "highest-ranked candidate must win the event slot: {winner:#?}"
    );
    assert_eq!(
        loser.status,
        EventStatus::Superseded,
        "the losing candidate sits above the reject floor and must be \
         Superseded, not Rejected: {loser:#?}"
    );

    let conflict_repo = ConflictGroupRepository::new(harness.client.clone());
    let unresolved = conflict_repo.find_all_unresolved(space_id, 10).await?;
    assert!(
        unresolved.is_empty(),
        "no unresolved conflict groups may remain for event slots: {unresolved:#?}"
    );

    Ok(())
}
