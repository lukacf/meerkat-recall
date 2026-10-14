//! The retained event expectations through accepted-source publication.
//! Extraction uses an independent exact known-answer oracle; embedding uses
//! the shared loopback transport spy. No simulated finalization or graph writes.

use super::*;
use elephant_pipeline::extraction::{
    verify_assertion_support, AssertionSupportVerifier, AssertionValidator, ExtractionContext,
    ExtractionResult, RawAssertion, SupportCandidate, SupportVerdict,
};
use elephant_storage::source_intake::{AcceptedSourceIntake, IntakeControl, PreparedSourceIntake};
use elephant_types::publication::OperationState;

#[allow(dead_code)]
#[path = "../../../../tests/recall_preview_semantics/provider_spy.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "Keep each control module's spy types and state separate from sibling controls"
)]
mod provider_spy;

const UNDATED: &str = "Alice launched Project Phoenix.";
const NOON: &str = "Alice launched Project Phoenix on 2024-03-10T12:00:00Z.";
const AFTERNOON: &str = "Alice launched Project Phoenix on 2024-03-10T13:00:00Z.";
const TITLE: &str = "Project Phoenix launch";
const LINEAGE: &str = "project-phoenix-launch";

fn time(value: &str) -> chrono::DateTime<Utc> {
    chrono::DateTime::parse_from_rfc3339(value)
        .expect("fixed oracle timestamp")
        .with_timezone(&Utc)
}

struct EventSupport {
    explicit_lineage: bool,
}
#[async_trait::async_trait]
impl AssertionSupportVerifier for EventSupport {
    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "Retain the original controlled extractor identity lifetime signature"
    )]
    fn identity(&self) -> &str {
        "retained-event-known-answer-v1"
    }
    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError> {
        Ok(candidates
            .iter()
            .map(|candidate| {
                let (expected_time, expected_precision) = match candidate.quote.as_str() {
                    UNDATED => (serde_json::Value::Null, serde_json::Value::Null),
                    NOON => (
                        serde_json::json!(time("2024-03-10T12:00:00Z")),
                        serde_json::json!(if self.explicit_lineage {
                            TimePrecision::Day
                        } else {
                            TimePrecision::Exact
                        }),
                    ),
                    AFTERNOON => (
                        serde_json::json!(time("2024-03-10T13:00:00Z")),
                        serde_json::json!(TimePrecision::Exact),
                    ),
                    _ => {
                        return SupportVerdict {
                            index: candidate.index,
                            supported: false,
                        }
                    }
                };
                SupportVerdict {
                    index: candidate.index,
                    supported: candidate.claim["subject"] == "Alice"
                        && candidate.claim["predicate"] == "launched"
                        && candidate.claim["object"] == "Project Phoenix"
                        && candidate.claim["literal"].is_null()
                        && candidate.claim["subject_type"] == "person"
                        && candidate.claim["object_type"] == "topic"
                        && candidate.claim["subject_entity"].is_null()
                        && candidate.claim["object_entity"].is_null()
                        && candidate.claim["event_time"] == expected_time
                        && candidate.claim["event_precision"] == expected_precision
                        && candidate.claim["event_title"] == TITLE
                        && candidate.claim["event_lineage"]
                            == if self.explicit_lineage {
                                serde_json::json!(LINEAGE)
                            } else {
                                serde_json::Value::Null
                            },
                }
            })
            .collect())
    }
}

struct EventExtractor {
    manifest: elephant_types::records::work::PluginManifest,
    explicit_lineage: bool,
}
#[async_trait::async_trait]
impl Extractor for EventExtractor {
    fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
        &self.manifest
    }
    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        let quotes = if self.explicit_lineage {
            [UNDATED, NOON]
        } else {
            [NOON, AFTERNOON]
        };
        let mut assertions = Vec::new();
        for quote in quotes {
            for (start, _) in ctx.node_text.match_indices(quote) {
                let event_time = match quote {
                    NOON => Some(time("2024-03-10T12:00:00Z")),
                    AFTERNOON => Some(time("2024-03-10T13:00:00Z")),
                    _ => None,
                };
                assertions.push(RawAssertion {
                    support_verification: None,
                    subject_text: "Alice".into(),
                    predicate_text: "launched".into(),
                    object_text: Some("Project Phoenix".into()),
                    object_literal: None,
                    evidence_span: EvidenceSpan {
                        revision: Link::new(ctx.revision_id.clone()),
                        node: Some(Link::new(ctx.node_id.clone())),
                        start: u32::try_from(start).expect("bounded fixture"),
                        end: u32::try_from(start + quote.len()).expect("bounded fixture"),
                        quote_hash: AssertionValidator::hash_quote(quote),
                    },
                    confidence: 0.95,
                    quote: Some(quote.into()),
                    source_event_time: event_time,
                    source_event_time_precision: event_time.map(|_| {
                        if self.explicit_lineage {
                            TimePrecision::Day
                        } else {
                            TimePrecision::Exact
                        }
                    }),
                    event_title: Some(TITLE.into()),
                    event_lineage_key: self.explicit_lineage.then(|| LINEAGE.into()),
                    subject_entity_id: None,
                    subject_entity_type: Some("person".into()),
                    object_entity_id: None,
                    object_entity_type: Some("topic".into()),
                    significance: Some("high".into()),
                });
            }
        }
        verify_assertion_support(
            &mut assertions,
            &ctx.node_text,
            &EventSupport {
                explicit_lineage: self.explicit_lineage,
            },
        )
        .await?;
        Ok(ExtractionResult::new(
            assertions,
            0,
            "retained-event-known-answer-v1".into(),
        ))
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
async fn publish(explicit_lineage: bool) -> (StorageClient, AcceptedSourceIntake) {
    let storage = StorageClient::new_memory().await.expect("storage");
    elephant_migrations::apply_schema(&storage)
        .await
        .expect("actual schema");
    let space = if explicit_lineage {
        "event-lineage-space"
    } else {
        "event-lineage-time-space"
    };
    let spy = provider_spy::Spy::start()
        .await
        .expect("loopback embedding spy");
    provider_spy::configure(
        &SpaceConfigRepository::new(storage.clone()),
        space,
        &spy.endpoint,
        8,
    )
    .await
    .expect("explicit governed space");
    let first = if explicit_lineage { UNDATED } else { NOON };
    let second = if explicit_lineage { NOON } else { AFTERNOON };
    // More than two extraction windows: the two actual quotes cannot be
    // accidentally tested only as two candidates in the same structural node.
    let padding = "Archive padding for complete bounded evidence coverage.\n".repeat(480);
    let text = format!("{first}\n\n{padding}\n\n{second}");
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            space.into(),
            "event-oracle".into(),
        ),
        SourceType::Manual,
        AssertionValidator::hash_quote(&text),
    );
    source.external_id = Some(format!("event-oracle:{}", uuid::Uuid::new_v4()));
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: TITLE.into(),
            text,
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: "event-oracle".into(),
            metadata: serde_json::json!({}),
        })
        .expect("immutable source template");
    source.set_requested_ingest_mode(IngestMode::Graph);
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        source,
        IntakeControl {
            revision_id: Some("event-evidence-v1".into()),
            idempotency_key: Some(format!("event-oracle:{}", uuid::Uuid::new_v4())),
            expected_generation: None,
        },
    ))
    .await
    .expect("prepare accepted source");
    let accepted = Box::pin(prepared.accept(&storage))
        .await
        .expect("accept source");
    elephant_pipeline::source_queue::enqueue_accepted_source(
        &storage,
        &accepted.source,
        &accepted.publication,
        "event-oracle",
    )
    .await
    .expect("enqueue original source plan");
    let extractor = EventExtractor {
        manifest: elephant_pipeline::extraction::DefaultExtractor::new()
            .manifest()
            .clone(),
        explicit_lineage,
    };
    elephant_pipeline::provider_dispatch::with_dispatch_credentials(
        std::sync::Arc::new(provider_spy::SpyCredentials),
        Box::pin(async {
            let items = WorkItemRepository::new(storage.clone());
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
            for _ in 0..300 {
                let runnable = Box::pin(super::grounded_stage_tests::ready_grounded_items(
                    &storage, space, deadline,
                ))
                .await;
                if runnable.is_empty() {
                    for status in [
                        WorkItemStatus::Pending,
                        WorkItemStatus::Running,
                        WorkItemStatus::Failed,
                    ] {
                        assert!(
                            items
                                .find_by_status(space, status)
                                .await
                                .expect("terminal work state")
                                .is_empty(),
                            "source pipeline left {status} work"
                        );
                    }
                    return;
                }
                for item in runnable {
                    let _completed_run =
                        Box::pin(super::grounded_stage_tests::dispatch_grounded_step(
                            &storage, &item, &extractor,
                        ))
                        .await;
                }
            }
            panic!("accepted source did not quiesce within fixture bound");
        }),
    )
    .await;
    let requests = spy.requests().expect("captured provider requests");
    assert!(
        !requests.is_empty(),
        "publication requires real embedding dispatch"
    );
    assert!(
        requests
            .iter()
            .all(|request| request.path.ends_with("/embeddings")),
        "fixture must not invoke a paid or simulated generative model"
    );
    let operation =
        elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
            .operation(space, &accepted.operation.operation_id)
            .await
            .expect("operation lookup")
            .expect("operation");
    assert_eq!(operation.state, OperationState::Completed);
    assert_eq!(
        operation.published_revision.as_deref(),
        Some(accepted.publication.revision_id.as_str())
    );
    (storage, accepted)
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep original repository and fixture ownership through the complete control assertions"
)]
pub(super) async fn refines_event_across_nodes() {
    let (storage, accepted) = Box::pin(publish(true)).await;
    let space = &accepted.source.base.space_id;
    let events = EventRepository::new(storage.clone())
        .list_recent(space, 10)
        .await
        .expect("events");
    assert_eq!(
        events.len(),
        1,
        "same immutable source lineage must refine one event"
    );
    let event = &events[0];
    assert_eq!(event.title, TITLE);
    assert_eq!(event.time_start, Some(time("2024-03-10T12:00:00Z")));
    assert_eq!(event.time_precision, TimePrecision::Day);
    assert_eq!(
        event.source_refs,
        vec![Link::new(accepted.source.base.id.clone())]
    );
    let mut nodes = std::collections::BTreeSet::new();
    let mut revisions = std::collections::BTreeSet::new();
    let mut quotes = std::collections::BTreeSet::new();
    let reader = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone());
    for reference in &event.assertion_refs {
        let assertion = AssertionRepository::new(storage.clone())
            .get(space, reference.id())
            .await
            .expect("assertion lookup")
            .expect("current assertion");
        let support = reader
            .evidence(space, &assertion.evidence, &|_| true)
            .await
            .expect("evidence")
            .expect("exact current evidence");
        nodes.insert(
            assertion
                .evidence
                .node
                .as_ref()
                .expect("structural evidence node")
                .id()
                .clone(),
        );
        revisions.insert(assertion.evidence.revision.id().clone());
        quotes.insert(support.quote);
    }
    assert!(
        nodes.len() >= 2,
        "test must span actual distinct structural nodes"
    );
    assert_eq!(
        revisions.len(),
        1,
        "event refinement cannot borrow another source revision"
    );
    assert_eq!(
        quotes,
        std::collections::BTreeSet::from([UNDATED.to_string(), NOON.to_string()])
    );
    let revision = revisions.first().expect("revision");
    let lineage = event.metadata["lineage_key"]
        .as_str()
        .expect("scoped lineage");
    assert!(
        lineage.starts_with(&format!("{}:{revision}:", accepted.source.base.id)),
        "lineage must bind source and immutable revision: {lineage}"
    );
}

pub(super) async fn distinguishes_exact_event_times() {
    let (storage, accepted) = Box::pin(publish(false)).await;
    let events = EventRepository::new(storage)
        .list_recent(&accepted.source.base.space_id, 10)
        .await
        .expect("events");
    assert_eq!(events.len(), 2, "distinct exact event times produce separate events with the same fallback semantic lineage");
    assert_eq!(
        events
            .iter()
            .map(|event| event.time_start.expect("supported time"))
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            time("2024-03-10T12:00:00Z"),
            time("2024-03-10T13:00:00Z")
        ])
    );
    assert!(events
        .iter()
        .all(|event| event.time_precision == TimePrecision::Exact && event.title == TITLE));
    assert!(events
        .iter()
        .all(|event| event.source_refs == vec![Link::new(accepted.source.base.id.clone())]));
    assert_eq!(
        events[0].metadata["lineage_key"],
        events[1].metadata["lineage_key"]
    );
}

#[test]
fn retained_event_fixture_phases_fit_default_stack_budget() {
    // Measure the compiler's actual future types without constructing/polling
    // them: a too-large future must fail this oracle before stack allocation.
    fn bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    for (name, size) in [
        ("publication", bytes(|| publish(true))),
        ("refinement", bytes(refines_event_across_nodes)),
        ("distinct times", bytes(distinguishes_exact_event_times)),
    ] {
        assert!(size <= 128 * 1024, "{name} fixture future is {size} bytes");
    }
}
