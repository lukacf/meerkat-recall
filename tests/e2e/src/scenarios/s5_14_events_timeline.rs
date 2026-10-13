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

use crate::grounded_source_fixture::GroundedSourceFixture;
use crate::Harness;
const SPACE: &str = "grounded-events";

async fn mock_auth(req: Request<Body>, next: Next) -> impl IntoResponse {
    let mut req = req;
    req.extensions_mut().insert(AuthPrincipal {
        subject: "scenario-user".to_string(),
        space_id: Some(SPACE.to_string()),
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

    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        use elephant_pipeline::extraction::{verify_assertion_support, AssertionValidator};
        let mut assertions = Vec::new();
        for original in &self.assertions {
            let Some(quote) = original.quote.as_deref() else {
                continue;
            };
            let Some(start) = ctx.node_text.find(quote) else {
                continue;
            };
            let mut raw = original.clone();
            raw.evidence_span = EvidenceSpan {
                revision: Link::new(ctx.revision_id.clone()),
                node: Some(Link::new(ctx.node_id.clone())),
                start: u32::try_from(start).unwrap(),
                end: u32::try_from(start + quote.len()).unwrap(),
                quote_hash: AssertionValidator::hash_quote(quote),
            };
            assertions.push(raw);
        }
        verify_assertion_support(&mut assertions, &ctx.node_text, &EventSupport).await?;
        Ok(ExtractionResult::new(
            assertions,
            0,
            "event-known-answer-v1".into(),
        ))
    }
}

struct EventSupport;
#[async_trait]
impl elephant_pipeline::extraction::AssertionSupportVerifier for EventSupport {
    fn identity(&self) -> &str {
        "dated-announcement-known-answer-v1"
    }
    async fn verify(
        &self,
        candidates: &[elephant_pipeline::extraction::SupportCandidate],
    ) -> Result<Vec<elephant_pipeline::extraction::SupportVerdict>, PipelineError> {
        Ok(candidates
            .iter()
            .map(|candidate| {
                let quoted_time = candidate
                    .quote
                    .strip_prefix("Alice announced Project Phoenix on ")
                    .and_then(|text| text.strip_suffix('.'))
                    .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok());
                let claimed_time = candidate.claim["event_time"]
                    .as_str()
                    .and_then(|time| chrono::DateTime::parse_from_rfc3339(time).ok());
                elephant_pipeline::extraction::SupportVerdict {
                    index: candidate.index,
                    supported: quoted_time.is_some()
                        && quoted_time == claimed_time
                        && candidate.claim["subject"] == "Alice"
                        && candidate.claim["predicate"] == "announced"
                        && candidate.claim["object"] == "Project Phoenix"
                        && candidate.claim["literal"].is_null()
                        && candidate.claim["event_title"] == "Alice announced Project Phoenix",
                }
            })
            .collect())
    }
}

fn make_event_assertion(confidence: f64, event_time: chrono::DateTime<Utc>) -> RawAssertion {
    RawAssertion {
        support_verification: None,
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
        quote: Some(format!(
            "Alice announced Project Phoenix on {}.",
            event_time.to_rfc3339()
        )),
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

#[tokio::test]
async fn scenario_e2e_001_ingest_extract_yields_timeline_events() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space_id = SPACE;
    let event_time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();
    let text = format!(
        "Alice announced Project Phoenix on {}.",
        event_time.to_rfc3339()
    );
    let fixture = GroundedSourceFixture::new(&harness.client, space_id).await?;

    let extractor = FixtureExtractor::new(vec![make_event_assertion(0.91, event_time)]);
    Box::pin(fixture.publish_text(&harness.client, "Dated announcement", &text, &extractor))
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
    let detect = Box::pin(execute_tm_detect_conflicts_from_outbox_events(
        &event_outbox,
        &tm_storage,
    ))
    .await?;
    for commit in detect.direct_commits {
        Box::pin(execute_tm_commit(
            &commit.resolution,
            &commit.input,
            space_id,
            &tm_storage,
        ))
        .await?;
    }
    for conflict_group_id in detect.conflict_group_ids {
        let conflict_repo = ConflictGroupRepository::new(harness.client.clone());
        let Some(group) = conflict_repo.get(space_id, &conflict_group_id).await? else {
            continue;
        };
        let (resolution, input) = Box::pin(resolve_conflict_group_with_storage(
            &TmContext::none(),
            &group,
            space_id,
            &tm_storage,
        ))
        .await?;
        Box::pin(execute_tm_commit(
            &resolution,
            &input,
            space_id,
            &tm_storage,
        ))
        .await?;
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
    let space_id = SPACE;
    let event_time = Utc.with_ymd_and_hms(2024, 1, 15, 9, 0, 0).unwrap();
    let text = format!(
        "Alice announced Project Phoenix on {}.",
        event_time.to_rfc3339()
    );
    let fixture = GroundedSourceFixture::new(&harness.client, space_id).await?;

    let extractor_a = FixtureExtractor::new(vec![make_event_assertion(0.93, event_time)]);
    let extractor_b = FixtureExtractor::new(vec![make_event_assertion(0.51, event_time)]);
    let accepted_a = Box::pin(fixture.publish_text(
        &harness.client,
        "Announcement source A",
        &text,
        &extractor_a,
    ))
    .await?;
    let accepted_b = Box::pin(fixture.publish_text(
        &harness.client,
        "Announcement source B",
        &text,
        &extractor_b,
    ))
    .await?;

    use crate::grounded_source_fixture::{
        fixture_entity_for_revision, fixture_published_revision, merge_fixture_entity_ids,
        FixtureMentionRole,
    };
    let revision_a = fixture_published_revision(&harness.client, &accepted_a).await?;
    let revision_b = fixture_published_revision(&harness.client, &accepted_b).await?;
    // Both the person and project are explicitly the same identities in this
    // known-answer corpus. Preserve two independent source contributions.
    for (mention, role) in [
        ("Alice", FixtureMentionRole::Subject),
        ("Project Phoenix", FixtureMentionRole::Object),
    ] {
        let a = fixture_entity_for_revision(&harness.client, space_id, &revision_a, mention, role)
            .await?;
        let b = fixture_entity_for_revision(&harness.client, space_id, &revision_b, mention, role)
            .await?;
        merge_fixture_entity_ids(&harness.client, space_id, &a, &b).await?;
    }

    let tm_storage = RepositoryTruthMaintenanceStorage::new(harness.client.clone());
    let event_repo = EventRepository::new(harness.client.clone());
    let seeded_events = event_repo.get_recent(space_id, 10).await?;
    assert_eq!(seeded_events.len(), 2, "retain both original source events");
    let triggers: Vec<TriggerEvent> = seeded_events
        .iter()
        .map(|event| TriggerEvent::Event(event.base.id.clone()))
        .collect();

    use elephant_storage::canonical_commit::{CanonicalRepository, PublishedSourceFence};
    use sha2::Digest;

    // Locate the exact direct-maintenance receipt from the original epoch,
    // before detection can change it. This independently encodes the stable
    // receipt identity; it never prepares a new decision or refreshes a proof.
    let canonical = CanonicalRepository::new(harness.client.clone());
    let observed_epoch = canonical.epoch(space_id).await?;
    let receipt_key = format!(
        "maintenance:{:x}",
        sha2::Sha256::digest(serde_json::to_vec(&(
            "execute_tm_detect_conflicts",
            "detection",
            "direct",
            &observed_epoch,
        ))?)
    );
    assert!(canonical.receipt(space_id, &receipt_key).await?.is_none());
    let mut original_request = serde_json::json!({"events": &triggers});
    original_request.sort_all_objects();
    let original_input_hash = format!(
        "{:x}",
        sha2::Sha256::digest(serde_json::to_vec(&original_request)?)
    );
    let detect = Box::pin(execute_tm_detect_conflicts(
        &triggers,
        space_id,
        &tm_storage,
    ))
    .await?;

    // Production detection applies its one ranked event-slot decision inside
    // the detection transaction. No successor queue remains to apply twice.
    assert!(
        detect.conflict_group_ids.is_empty(),
        "event slots must resolve via direct commits, not conflict groups: {:?}",
        detect.conflict_group_ids
    );
    assert_eq!(detect.events_processed, 2);
    assert_eq!(detect.conflicts_detected, 1, "one shared event slot");
    assert_eq!(detect.conflicts_updated, 0);
    assert!(detect.direct_commits.is_empty());
    let receipt = canonical
        .receipt(space_id, &receipt_key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("original detection transaction receipt absent"))?;
    assert_eq!(receipt.space_id, space_id);
    assert_eq!(receipt.commit_id, receipt_key);
    assert_eq!(receipt.input_fingerprint, original_input_hash);
    assert_eq!(
        receipt.replay_output,
        Some(serde_json::json!({"value": serde_json::to_value(&detect)?}))
    );
    let mut original_event_ids = seeded_events
        .iter()
        .map(|event| event.base.id.clone())
        .collect::<Vec<_>>();
    original_event_ids.sort();
    let mut committed_event_ids = receipt
        .effect_ids
        .iter()
        .filter(|id| id.table() == TableName::Event)
        .cloned()
        .collect::<Vec<_>>();
    committed_event_ids.sort();
    assert_eq!(committed_event_ids, original_event_ids);
    let mut original_authority = [&accepted_a, &accepted_b]
        .into_iter()
        .map(|accepted| PublishedSourceFence::Head {
            source_id: accepted.source.base.id.clone(),
            generation: accepted.publication.generation.clone(),
            published_revision: accepted.publication.revision_id.clone(),
        })
        .collect::<Vec<_>>();
    original_authority.sort_by(|a, b| a.source_id().cmp(b.source_id()));
    let mut committed_authority = receipt.maintenance_authority;
    committed_authority.sort_by(|a, b| a.source_id().cmp(b.source_id()));
    assert_eq!(committed_authority, original_authority);
    let original_sources = original_authority
        .iter()
        .map(|fence| fence.source_id().clone())
        .collect::<Vec<_>>();
    let mut committed_sources = receipt.source_ids;
    committed_sources.sort();
    assert_eq!(committed_sources, original_sources);

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
