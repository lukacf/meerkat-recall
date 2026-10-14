//! Actual sealed contact acceptance and Graph publication through claimed work.
//! The extractor checks one fixed claim; the existing loopback spy supplies only
//! deterministic embedding transport. These are composition, not model-quality,
//! oracles. Run them only in the centrally scheduled coherent contact build.

use super::*;
use elephant_pipeline::extraction::{
    verify_assertion_support, AssertionSupportVerifier, AssertionValidator, ExtractionContext,
    ExtractionResult, RawAssertion, SupportCandidate, SupportVerdict,
};
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::source_contact_intake::ContactRecord;
use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{EntityIdentityKeyRepository, SystemEventRepository};
use elephant_types::publication::OperationState;
use elephant_types::records::base::SubjectsState;
use std::collections::BTreeSet;

#[allow(dead_code)]
#[path = "../../../../tests/recall_preview_semantics/provider_spy.rs"]
#[expect(
    clippy::duplicate_mod,
    reason = "Keep each control module's spy types and state separate from sibling controls"
)]
pub(super) mod provider_spy;

#[path = "../../../../tests/recall_preview/contact_lineage_oracles.rs"]
mod independent_contact_lineage_oracles;

const SPACE: &str = "contact-publication-space";
const ACTOR: &str = "contact-publication-oracle";
const QUOTE: &str = "Aria works at Cedar.";
const CONTACT: &str = "Contact Casey";
const COMPANY: &str = "Orion Labs";
const EMAIL: &str = "casey@example.invalid";

/// Failure-only factual output. Never changes input authority or retries work.
#[expect(
    clippy::items_after_statements,
    clippy::significant_drop_tightening,
    reason = "Keep local diagnostic types and case definitions beside the control phase that uses them; Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn native_survivor_maintenance_support_diagnostic(
    storage: &StorageClient,
    original: [&elephant_types::records::Rel; 2],
) {
    use elephant_storage::canonical_commit::{CanonicalRecord, SourceEffectSupport};

    #[expect(
        clippy::needless_pass_by_value,
        reason = "Keep ownership of the bounded failure diagnostic payload at its original call boundary"
    )]
    fn emit(remaining: &mut usize, label: &str, value: serde_json::Value) {
        let line = format!("native survivor support {label}: {value}\n");
        if line.len() <= *remaining {
            *remaining -= line.len();
            eprint!("{line}");
        } else {
            let exhausted =
                "native survivor support diagnostic exceeds remaining 65536-byte budget\n";
            if exhausted.len() <= *remaining {
                *remaining -= exhausted.len();
                eprint!("{exhausted}");
            }
        }
    }
    fn row(rel: &elephant_types::records::Rel) -> serde_json::Value {
        serde_json::json!({
            "id": rel.base.id,
            "fingerprint": CanonicalRecord::Rel(Box::new(rel.clone())).fingerprint()
                .map_err(|error| error.to_string()),
            "provenance": rel.base.provenance,
        })
    }
    let mut remaining = 65_536usize;
    let relations = elephant_storage::RelRepository::new(storage.clone());
    for original in original {
        let current = Box::pin(relations.get(SPACE, &original.base.id)).await;
        emit(
            &mut remaining,
            "relation",
            serde_json::json!({
                "captured_before_maintenance": row(original),
                "current_after_failure": current.map(|rel| rel.as_ref().map(row))
                    .map_err(|error| error.to_string()),
            }),
        );
    }
    #[derive(serde::Deserialize, serde::Serialize)]
    struct SupportProjection {
        storage_id: String,
        source_id: String,
        source_revision: String,
        record_id: String,
        payload: String,
    }
    let bindings = std::collections::BTreeMap::from([
        ("space".to_owned(), SPACE.to_owned()),
        ("old".to_owned(), original[0].base.id.to_string()),
        ("new".to_owned(), original[1].base.id.to_string()),
    ]);
    let query = "SELECT type::string(id) AS storage_id, source_id, source_revision, record_id, payload FROM source_effect_support WHERE space_id=$space AND record_id IN [$old,$new] LIMIT 33;";
    let mut response = match storage.query_bind_checked(query, bindings).await {
        Ok(response) => response,
        Err(error) => {
            emit(
                &mut remaining,
                "query_error",
                serde_json::json!(error.to_string()),
            );
            return;
        }
    };
    let rows = match response.take::<Vec<SupportProjection>>(0) {
        Ok(rows) if rows.len() <= 32 => rows,
        Ok(rows) => {
            emit(
                &mut remaining,
                "row_bound",
                serde_json::json!({"rows": rows.len(), "maximum": 32}),
            );
            return;
        }
        Err(error) => {
            emit(
                &mut remaining,
                "projection_error",
                serde_json::json!(error.to_string()),
            );
            return;
        }
    };
    let mut sources = BTreeSet::new();
    for raw in rows {
        let decoded = serde_json::from_str::<SourceEffectSupport>(&raw.payload);
        if let Ok(support) = &decoded {
            sources.insert(support.source_id.clone());
        }
        emit(
            &mut remaining,
            "original_row",
            serde_json::json!({
                "storage_id": raw.storage_id,
                "source_id": raw.source_id,
                "source_revision": raw.source_revision,
                "record_id": raw.record_id,
                "decoded_payload": decoded.map_err(|error| error.to_string()),
            }),
        );
    }
    let publications = SourcePublicationRepository::new(storage.clone());
    for source_id in sources {
        let head = Box::pin(publications.head(SPACE, &source_id)).await;
        emit(
            &mut remaining,
            "support_source_head",
            serde_json::json!({
                "source_id": source_id,
                "head": head.map_err(|error| error.to_string()),
            }),
        );
    }
}

struct ContactGraphSupport;

#[async_trait::async_trait]
impl AssertionSupportVerifier for ContactGraphSupport {
    #[expect(
        clippy::unnecessary_literal_bound,
        reason = "Keep this support verifier implementation aligned with the trait identity signature"
    )]
    fn identity(&self) -> &str {
        "contact-graph-known-answer-v1"
    }

    async fn verify(
        &self,
        candidates: &[SupportCandidate],
    ) -> Result<Vec<SupportVerdict>, PipelineError> {
        Ok(candidates
            .iter()
            .map(|candidate| SupportVerdict {
                index: candidate.index,
                supported: candidate.quote == QUOTE
                    && candidate.claim["subject"] == "Aria"
                    && candidate.claim["predicate"] == "works_at"
                    && candidate.claim["object"] == "Cedar"
                    && candidate.claim["literal"].is_null()
                    && candidate.claim["subject_type"] == "person"
                    && candidate.claim["object_type"] == "org"
                    && candidate.claim["subject_entity"].is_null()
                    && candidate.claim["object_entity"].is_null()
                    && candidate.claim["event_time"].is_null()
                    && candidate.claim["event_precision"].is_null()
                    && candidate.claim["event_title"].is_null()
                    && candidate.claim["event_lineage"].is_null(),
            })
            .collect())
    }
}

struct ContactGraphExtractor {
    manifest: elephant_types::records::work::PluginManifest,
}

/// Reuse the same fixed supported claim in the concurrent worker oracle.
pub(super) fn known_graph_extractor() -> impl Extractor {
    ContactGraphExtractor {
        manifest: elephant_pipeline::extraction::DefaultExtractor::new()
            .manifest()
            .clone(),
    }
}

#[async_trait::async_trait]
impl Extractor for ContactGraphExtractor {
    fn manifest(&self) -> &elephant_types::records::work::PluginManifest {
        &self.manifest
    }

    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        let start = ctx.node_text.find(QUOTE).ok_or_else(|| {
            PipelineError::ExtractionError("known contact Graph fixture quote missing".into())
        })?;
        let mut assertions = vec![RawAssertion {
            support_verification: None,
            subject_text: "Aria".into(),
            predicate_text: "works_at".into(),
            object_text: Some("Cedar".into()),
            object_literal: None,
            evidence_span: EvidenceSpan {
                revision: Link::new(ctx.revision_id.clone()),
                node: Some(Link::new(ctx.node_id.clone())),
                start: u32::try_from(start).expect("bounded fixture"),
                end: u32::try_from(start + QUOTE.len()).expect("bounded fixture"),
                quote_hash: AssertionValidator::hash_quote(QUOTE),
            },
            confidence: 0.99,
            quote: Some(QUOTE.into()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".into()),
            object_entity_id: None,
            object_entity_type: Some("org".into()),
            significance: Some("high".into()),
        }];
        verify_assertion_support(&mut assertions, &ctx.node_text, &ContactGraphSupport).await?;
        Ok(ExtractionResult::new(
            assertions,
            0,
            "contact-graph-known-answer-v1".into(),
        ))
    }
}

fn input_source() -> Source {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            ACTOR.into(),
        ),
        SourceType::Manual,
        AssertionValidator::hash_quote(QUOTE),
    );
    source.external_id = Some("contact-graph-original-input".into());
    source.base.security.level = 3;
    source.base.security.labels = vec!["contact-confidential".into()];
    source.base.security.handling = vec!["no_export".into()];
    source.base.subjects_state = SubjectsState::Complete;
    source.metadata = serde_json::json!({"contact": {
        "name": CONTACT,
        "email": "CASEY@example.invalid",
        "company": COMPANY,
        "job_title": "Engineer"
    }});
    source
        .set_manual_doc_template(&ManualDocTemplate {
            doc_id: RecordId::new_v4(TableName::Doc).to_string(),
            title: "Contact and separate supported document".into(),
            text: QUOTE.into(),
            sidecar_text: None,
            media_attachments: vec![],
            rendition_key: format!("contact-graph-fixture:{}", source.base.id),
            metadata: serde_json::json!({}),
        })
        .unwrap();
    source.set_requested_ingest_mode(IngestMode::Graph);
    source
}

fn intake_control() -> IntakeControl {
    IntakeControl {
        revision_id: Some("contact-and-graph-v1".into()),
        idempotency_key: Some("contact-and-graph-original-acceptance".into()),
        expected_generation: None,
    }
}

async fn assert_no_live_graph(storage: &StorageClient) {
    assert!(EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(EntityIdentityKeyRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(AssertionRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(!SystemEventRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .iter()
        .any(|event| event.event_type == "contact.ingested"));
}

fn snapshot<T: serde::Serialize>(rows: impl IntoIterator<Item = T>) -> Vec<String> {
    let mut rows: Vec<_> = rows
        .into_iter()
        .map(|row| {
            let mut value = serde_json::to_value(row).unwrap();
            value.sort_all_objects();
            value.to_string()
        })
        .collect();
    rows.sort();
    rows
}

// This expected-state oracle deliberately does not use the product support
// fingerprint: only the explicitly allowed top-level update clock is ignored.
fn semantic_row<T: serde::Serialize>(record: &T) -> serde_json::Value {
    let mut value = serde_json::to_value(record).unwrap();
    assert!(value
        .as_object_mut()
        .unwrap()
        .remove("updated_at")
        .is_some());
    value
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn accepted_contact_and_graph_publish_together(backend: provider_spy::Backend) {
    let storage = Box::pin(provider_spy::storage(backend)).await.unwrap();
    let spy = provider_spy::Spy::start().await.unwrap();
    Box::pin(provider_spy::configure(
        &SpaceConfigRepository::new(storage.clone()),
        SPACE,
        &spy.endpoint,
        8,
    ))
    .await
    .unwrap();
    let input = input_source();
    let original_security = input.base.security.clone();
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        input.clone(),
        intake_control(),
    ))
    .await
    .unwrap();
    let contact = Box::pin(prepared.prepare_contact(&storage))
        .await
        .unwrap()
        .unwrap();
    // Record only opaque IDs from the actual capability. Expected field values
    // below come from this fixture's input, not from the generated contact plan.
    let contact_ids: BTreeSet<_> = contact
        .authorizations()
        .iter()
        .map(|record| match record {
            ContactRecord::Entity(row) => row.base.id.clone(),
            ContactRecord::EntityIdentityKey(row) => row.base.id.clone(),
            ContactRecord::Rel(row) => row.base.id.clone(),
        })
        .collect();
    assert_eq!(contact_ids.len(), 4);
    assert_no_live_graph(&storage).await;
    // This is the trusted, already-authorized typed seam; D's signed HTTP suite
    // separately proves that the public adapter authorizes all these envelopes.
    let accepted = Box::pin(prepared.accept_with_prepared_contact(&storage, &contact))
        .await
        .unwrap();
    assert_eq!(accepted.operation.state, OperationState::Accepted);
    assert!(accepted.needs_processing());
    assert_no_live_graph(&storage).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    assert!(publications
        .head(SPACE, &accepted.source.base.id)
        .await
        .unwrap()
        .unwrap()
        .published_revision
        .is_none());
    let queue_id = Box::pin(elephant_pipeline::source_queue::enqueue_accepted_source(
        &storage,
        &accepted.source,
        &accepted.publication,
        ACTOR,
    ))
    .await
    .unwrap();
    assert_eq!(
        Box::pin(elephant_pipeline::source_queue::enqueue_accepted_source(
            &storage,
            &accepted.source,
            &accepted.publication,
            ACTOR,
        ))
        .await
        .unwrap(),
        queue_id,
        "duplicate acceptance notification must retain the original work"
    );
    let manifests = SourcePublicationManifestRepository::new(storage.clone());
    let extractor = ContactGraphExtractor {
        manifest: elephant_pipeline::extraction::DefaultExtractor::new()
            .manifest()
            .clone(),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut finalizations = 0;
    Box::pin(
        elephant_pipeline::provider_dispatch::with_dispatch_credentials(
            std::sync::Arc::new(provider_spy::SpyCredentials),
            async {
                let items = WorkItemRepository::new(storage.clone());
                for _ in 0..100 {
                    let runnable = Box::pin(ready_contact_items(&storage, deadline)).await;
                    if runnable.is_empty() {
                        for status in [
                            WorkItemStatus::Pending,
                            WorkItemStatus::Running,
                            WorkItemStatus::Failed,
                        ] {
                            assert!(
                                items
                                    .find_by_status(SPACE, status)
                                    .await
                                    .unwrap()
                                    .is_empty(),
                                "incomplete source work: {status}"
                            );
                        }
                        return;
                    }
                    for item in runnable {
                        if item.kind
                            == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
                        {
                            assert_no_live_graph(&storage).await;
                            let manifest = manifests
                                .get(SPACE, &accepted.publication)
                                .await
                                .unwrap()
                                .unwrap();
                            assert_eq!(manifest.mode, IngestMode::Graph);
                            assert!(!manifest.bundles.is_empty());
                            assert!(!manifest.embeddings.is_empty());
                            assert!(manifests
                                .completion(SPACE, &accepted.publication)
                                .await
                                .unwrap()
                                .is_none());
                        }
                        let completed = Box::pin(dispatch_contact_step(&storage, &item, Some(&extractor))).await;
                        if completed && item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND {
                            finalizations += 1;
                        }
                    }
                }
                panic!("contact source exceeded bounded dispatch count");
            },
        ),
    )
    .await;
    assert_eq!(finalizations, 1);
    let requests = spy.requests().unwrap();
    assert!(
        !requests.is_empty(),
        "real required embedding transport must run"
    );
    assert!(requests
        .iter()
        .all(|request| request.path.ends_with("/embeddings")));

    let operation = publications
        .operation(SPACE, &accepted.operation.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.state, OperationState::Completed);
    assert_eq!(operation.source_generation, accepted.publication.generation);
    assert_eq!(
        operation.published_revision.as_deref(),
        Some(accepted.publication.revision_id.as_str())
    );
    let head = publications
        .head(SPACE, &accepted.source.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(head.operation_id, accepted.operation.operation_id);
    assert_eq!(head.generation, accepted.publication.generation);
    assert_eq!(head.published_revision, head.desired_revision);
    let completed_source = SourceRepository::new(storage.clone())
        .get(SPACE, &accepted.source.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        completed_source.lifecycle_status,
        SourceLifecycleStatus::GraphReady
    );
    assert_eq!(
        completed_source.current_ingest_mode,
        Some(IngestMode::Graph)
    );
    assert_eq!(completed_source.metadata["contact"]["name"], CONTACT);
    let completion = manifests
        .completion(SPACE, &accepted.publication)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completion.publication, accepted.publication);
    let receipt = CanonicalRepository::new(storage.clone())
        .receipt(SPACE, &completion.commit_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        receipt.publication_authority.as_ref(),
        Some(&accepted.publication)
    );
    assert!(contact_ids.iter().all(|id| receipt.effect_ids.contains(id)));

    let entities = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let person = entities
        .iter()
        .find(|entity| entity.display_name == CONTACT)
        .unwrap();
    assert_eq!(person.identity_keys, vec![EMAIL.to_owned()]);
    assert_eq!(person.metadata["source_type"], "contact_import");
    let company = entities
        .iter()
        .find(|entity| entity.display_name == COMPANY)
        .unwrap();
    let graph_person = entities
        .iter()
        .find(|entity| entity.display_name == "Aria")
        .unwrap();
    let graph_company = entities
        .iter()
        .find(|entity| entity.display_name == "Cedar")
        .unwrap();
    assert_ne!(person.base.id, graph_person.base.id);
    assert_ne!(company.base.id, graph_company.base.id);
    let keys = EntityIdentityKeyRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let key = keys.iter().find(|key| key.key == EMAIL).unwrap();
    assert_eq!(key.entity.id(), &person.base.id);
    assert!(key
        .evidence
        .iter()
        .any(|reference| reference.id() == &accepted.source.base.id));
    let rels = RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let contact_rel = rels
        .iter()
        .find(|rel| {
            rel.subject.id() == &person.base.id
                && rel.object.id() == &company.base.id
                && rel.rel_type == "works_at"
        })
        .unwrap();
    assert_eq!(contact_rel.status, ClaimStatus::Approved);
    assert!(
        contact_rel.evidence.is_empty(),
        "structured contact must not fabricate an extraction assertion"
    );
    let graph_rel = rels
        .iter()
        .find(|rel| {
            rel.subject.id() == &graph_person.base.id
                && rel.object.id() == &graph_company.base.id
                && rel.rel_type == "works_at"
        })
        .unwrap();
    assert!(!graph_rel.evidence.is_empty());
    for id in contact_ids.iter().chain([
        &graph_person.base.id,
        &graph_company.base.id,
        &graph_rel.base.id,
    ]) {
        let support = publications.support_for_record(SPACE, id).await.unwrap();
        assert!(support
            .iter()
            .any(|row| row.source_id == accepted.source.base.id
                && row.source_revision == accepted.publication.revision_id
                && row.record_id == *id
                && !row.independent));
    }
    let assertions = AssertionRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    assert_eq!(assertions.len(), 1);
    for base in [
        &person.base,
        &company.base,
        &key.base,
        &contact_rel.base,
        &graph_person.base,
        &graph_company.base,
        &graph_rel.base,
        &assertions[0].base,
    ] {
        assert_eq!(base.security, original_security);
        assert!(base.provenance.sources.contains(&accepted.source.base.id));
    }
    let evidence = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone())
        .evidence(SPACE, &assertions[0].evidence, &|_| true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(evidence.quote, QUOTE);
    assert!(receipt.effect_ids.contains(&assertions[0].base.id));
    let events = SystemEventRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let contact_events: Vec<_> = events
        .iter()
        .filter(|event| event.event_type == "contact.ingested")
        .collect();
    for event in events
        .iter()
        .filter(|event| receipt.outbox_ids.contains(&event.base.id))
    {
        assert_eq!(event.base.security, original_security);
    }
    assert_eq!(contact_events.len(), 4);
    for event in contact_events {
        assert!(receipt.outbox_ids.contains(&event.base.id));
        assert_eq!(event.payload["records"].as_array().unwrap().len(), 1);
    }
    assert!(
        events
            .iter()
            .any(|event| event.event_type != "contact.ingested"
                && receipt.outbox_ids.contains(&event.base.id)),
        "grounded publication outbox must survive the contact prefix"
    );

    let before_replay = (
        snapshot(&entities),
        snapshot(&keys),
        snapshot(&rels),
        snapshot(&assertions),
        snapshot(&events),
    );
    let retry = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        input,
        intake_control(),
    ))
    .await
    .unwrap();
    let original_contact = retry.prepare_contact(&storage).await.unwrap().unwrap();
    let repeated = retry
        .accept_with_prepared_contact(&storage, &original_contact)
        .await
        .unwrap();
    assert_eq!(repeated.publication, accepted.publication);
    assert_eq!(repeated.operation.operation_id, operation.operation_id);
    assert_eq!(repeated.operation.state, OperationState::Completed);
    assert!(!repeated.needs_processing());
    assert_eq!(
        manifests
            .completion(SPACE, &accepted.publication)
            .await
            .unwrap()
            .unwrap(),
        completion
    );
    assert_eq!(spy.requests().unwrap().len(), requests.len());
    assert!(WorkItemRepository::new(storage.clone())
        .find_by_status(SPACE, WorkItemStatus::Runnable)
        .await
        .unwrap()
        .is_empty());
    let after_replay = (
        snapshot(
            EntityRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            EntityIdentityKeyRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            RelRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            AssertionRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            SystemEventRepository::new(storage)
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
    );
    assert_eq!(
        before_replay, after_replay,
        "completed retry must not rewrite canonical rows or outbox"
    );
}

#[tokio::test]
async fn embedded_actual_accepted_contact_and_graph_complete_atomically() {
    accepted_contact_and_graph_publish_together(provider_spy::Backend::ElephantDb).await;
}

#[tokio::test]
async fn surreal_actual_accepted_contact_and_graph_complete_atomically() {
    accepted_contact_and_graph_publish_together(provider_spy::Backend::SurrealMemory).await;
}

// These lifecycle cases reuse the actual accepted queue path. They compare live
// rows before the finalizer and the independent baseline after source removal;
// they never manufacture a head, capability, support row or inverse receipt.
type GraphRows = [Vec<String>; 4];

async fn graph_rows(storage: &StorageClient) -> GraphRows {
    [
        snapshot(
            EntityRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            EntityIdentityKeyRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            RelRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
        snapshot(
            AssertionRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap(),
        ),
    ]
}

/// Runnable queries deliberately exclude future `run_after`. Inspect the bounded
/// owned fixture queue to distinguish delayed original work from lost fanout,
/// then wait for the real production retry deadline without rewriting it.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn ready_contact_items(
    storage: &StorageClient,
    deadline: std::time::Instant,
) -> Vec<WorkItem> {
    let items = WorkItemRepository::new(storage.clone());
    loop {
        assert!(
            std::time::Instant::now() < deadline,
            "contact publication exceeded its wall-clock bound"
        );
        let ready = items
            .find_by_status(SPACE, WorkItemStatus::Runnable)
            .await
            .unwrap();
        if !ready.is_empty() {
            return ready;
        }
        let all = items.list(SPACE, 256, None).await.unwrap();
        assert!(
            all.len() < 256,
            "contact fixture exceeded complete queue inspection bound"
        );
        let due = all
            .iter()
            .filter(|item| item.status == WorkItemStatus::Runnable)
            .filter_map(|item| item.run_after)
            .min();
        let Some(due) = due else { return Vec::new() };
        let wait = (due - Utc::now()).to_std().unwrap_or_default();
        assert!(
            wait <= deadline.saturating_duration_since(std::time::Instant::now()),
            "original delayed work cannot complete within the fixture deadline"
        );
        tokio::time::sleep(wait).await;
    }
}

/// The shared B fixture persists the original failed run and performs the
/// production `PendingDependencies` requeue before returning its original error.
/// A retry is not a successful stage, completed artifact, or source receipt.
async fn dispatch_contact_step(
    storage: &StorageClient,
    item: &WorkItem,
    extractor: Option<&dyn Extractor>,
) -> bool {
    let before = graph_rows(storage).await;
    let run_id = RecordId::new_v4(TableName::WorkRun);
    let config = WorkQueueWorkerConfig {
        // Pending dependencies use the independent five-second local policy,
        // even with a thirty-second transient cap. Never rewrite run_after.
        transient_retry_max_delay_secs: 30,
        ..WorkQueueWorkerConfig::default()
    };
    let result = Box::pin(dispatch_fixture_with_extractor(
        item,
        &run_id,
        storage.clone(),
        &config,
        &TmContext::none(),
        extractor,
    ))
    .await;
    let dispatch_finished = Utc::now();
    match result {
        Ok(_) => true,
        Err(error) => {
            use elephant_pipeline::scheduler::execution_contract::{
                classify_failure, FailureClass,
            };
            assert_eq!(
                classify_failure(&error),
                FailureClass::PendingDependencies,
                "actual contact source stage failed: {error:#}"
            );
            let queued = WorkItemRepository::new(storage.clone())
                .get(SPACE, &item.base.id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(queued.status, WorkItemStatus::Runnable);
            assert_eq!(
                queued.attempts, item.attempts,
                "pending prerequisites consumed the transient failure budget"
            );
            assert!(queued.leased_by.is_none() && queued.lease_expires_at.is_none());
            let run = WorkRunRepository::new(storage.clone())
                .get(SPACE, &run_id)
                .await
                .unwrap()
                .unwrap();
            assert!(!run.success && run.completed_at.is_some() && run.error.is_some());
            assert!(
                queued.run_after.is_some_and(|due| {
                    due >= run.completed_at.unwrap() + chrono::Duration::seconds(5)
                        && due <= dispatch_finished + chrono::Duration::seconds(5)
                }),
                "pending retry left its five-second local-prerequisite window"
            );
            assert_eq!(run.work_item.id(), &item.base.id);
            let original: elephant_types::publication::AttemptFence =
                serde_json::from_value(run.metrics.custom["attempt_fence"].clone()).unwrap();
            assert_eq!(original.space_id, SPACE);
            assert_eq!(original.work_item_id, item.base.id);
            assert!(!original.lease_token.is_empty() && original.generation > 0);
            assert_eq!(
                graph_rows(storage).await,
                before,
                "incomplete finalization changed the published graph"
            );
            false
        }
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn finish_accepted_contact(
    storage: &StorageClient,
    accepted: &elephant_storage::source_intake::AcceptedSourceIntake,
) -> elephant_storage::canonical_commit::CanonicalCommitReceipt {
    let prior_graph = graph_rows(storage).await;
    Box::pin(elephant_pipeline::source_queue::enqueue_accepted_source(
        storage,
        &accepted.source,
        &accepted.publication,
        ACTOR,
    ))
    .await
    .unwrap();
    let extractor = ContactGraphExtractor {
        manifest: elephant_pipeline::extraction::DefaultExtractor::new()
            .manifest()
            .clone(),
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut finalizations = 0;
    Box::pin(elephant_pipeline::provider_dispatch::with_dispatch_credentials(
        std::sync::Arc::new(provider_spy::SpyCredentials),
        async {
            let items = WorkItemRepository::new(storage.clone());
            for _ in 0..100 {
                let runnable = Box::pin(ready_contact_items(storage, deadline)).await;
                if runnable.is_empty() {
                    for status in [WorkItemStatus::Pending, WorkItemStatus::Running, WorkItemStatus::Failed] {
                        assert!(items.find_by_status(SPACE, status).await.unwrap().is_empty(), "unfinished lifecycle source work: {status}");
                    }
                    return;
                }
                for item in runnable {
                    if item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND {
                        assert_eq!(graph_rows(storage).await, prior_graph, "pending replacement must retain the prior graph until one finalizer transaction");
                    }
                    let completed = Box::pin(dispatch_contact_step(storage, &item, Some(&extractor))).await;
                    if completed && item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND {
                        finalizations += 1;
                    }
                }
            }
            panic!("contact lifecycle exceeded bounded queue dispatch");
        },
    )).await;
    assert_eq!(finalizations, 1);
    let operation = SourcePublicationRepository::new(storage.clone())
        .operation(SPACE, &accepted.operation.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(operation.state, OperationState::Completed);
    assert_eq!(operation.source_generation, accepted.publication.generation);
    let completion = SourcePublicationManifestRepository::new(storage.clone())
        .completion(SPACE, &accepted.publication)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completion.publication, accepted.publication);
    CanonicalRepository::new(storage.clone())
        .receipt(SPACE, &completion.commit_id)
        .await
        .unwrap()
        .unwrap()
}

async fn lifecycle_storage(backend: provider_spy::Backend) -> (StorageClient, provider_spy::Spy) {
    let storage = Box::pin(provider_spy::storage(backend)).await.unwrap();
    let spy = provider_spy::Spy::start().await.unwrap();
    Box::pin(provider_spy::configure(
        &SpaceConfigRepository::new(storage.clone()),
        SPACE,
        &spy.endpoint,
        8,
    ))
    .await
    .unwrap();
    (storage, spy)
}

async fn accept_contact_revision(
    storage: &StorageClient,
    source: Source,
    revision: &str,
    generation: Option<String>,
    reactivate: bool,
) -> elephant_storage::source_intake::AcceptedSourceIntake {
    let mut prepared = Box::pin(PreparedSourceIntake::prepare(
        storage,
        source,
        IntakeControl {
            revision_id: Some(revision.into()),
            idempotency_key: Some(format!("contact-lifecycle:{revision}")),
            expected_generation: generation,
        },
    ))
    .await
    .unwrap();
    if reactivate {
        prepared = prepared.reactivate();
    }
    let contact = Box::pin(prepared.prepare_contact(storage))
        .await
        .unwrap()
        .unwrap();
    Box::pin(prepared.accept_with_prepared_contact(storage, &contact))
        .await
        .unwrap()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn lifecycle_request(
    storage: &StorageClient,
    source_id: &RecordId,
    action: &str,
) -> elephant_storage::source_publication::SourceMutation {
    let repository = SourcePublicationRepository::new(storage.clone());
    let observed = repository
        .authorization_snapshot(SPACE, source_id)
        .await
        .unwrap()
        .unwrap();
    let head = repository.head(SPACE, source_id).await.unwrap().unwrap();
    elephant_storage::source_publication::SourceMutation {
        space_id: SPACE.into(),
        source_id: source_id.clone(),
        expected: elephant_types::publication::SourcePrecondition::Generation(head.generation),
        idempotency_key: format!("contact-lifecycle:{action}"),
        request_fingerprint: format!("contact-lifecycle:{action}"),
        actor: ACTOR.into(),
        authorized: observed,
        authorized_document: None,
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn replaced_contact_erasure_restores_independent_baseline(backend: provider_spy::Backend) {
    use elephant_types::records::identity::{EntityIdentityKey, IdentityKeyStatus};
    let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
    let mut person = Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "independent-owner".into(),
        ),
        EntityType::new("person"),
        "Independent Casey".into(),
    );
    person.base.subjects_state = SubjectsState::Complete;
    person.base.security.level = 1;
    person.base.security.labels = vec!["independent-label".into()];
    person.aliases = vec!["Original independent alias".into()];
    person.identity_keys = vec![EMAIL.into()];
    person.metadata = serde_json::json!({"independent_baseline":"retain exactly"});
    person.summary = Some("Independent description".into());
    let person = EntityRepository::new(storage.clone())
        .create(person)
        .await
        .unwrap();
    let mut key_base = person.base.clone();
    key_base.id = RecordId::new_v4(TableName::EntityIdentityKey);
    let key = EntityIdentityKeyRepository::new(storage.clone())
        .create(EntityIdentityKey {
            base: key_base,
            key: EMAIL.into(),
            entity: Link::new(person.base.id.clone()),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        })
        .await
        .unwrap();
    let baseline = graph_rows(&storage).await;
    let mut source = input_source();
    source.metadata["contact"]["name"] = serde_json::json!("Casey revision one");
    let first = Box::pin(accept_contact_revision(
        &storage,
        source.clone(),
        "contact-revision-one",
        None,
        false,
    ))
    .await;
    assert_eq!(graph_rows(&storage).await, baseline);
    Box::pin(finish_accepted_contact(&storage, &first)).await;
    let after_first = EntityRepository::new(storage.clone())
        .get(SPACE, &person.base.id)
        .await
        .unwrap()
        .unwrap();
    assert!(after_first
        .aliases
        .iter()
        .any(|name| name == "Casey revision one"));
    assert_eq!(after_first.metadata, person.metadata);
    assert_eq!(after_first.base.security.level, 3);
    let published_first = graph_rows(&storage).await;
    source.metadata["contact"]["name"] = serde_json::json!("Casey revision two");
    let second = Box::pin(accept_contact_revision(
        &storage,
        source,
        "contact-revision-two",
        Some(first.publication.generation.clone()),
        false,
    ))
    .await;
    assert_eq!(graph_rows(&storage).await, published_first);
    let second_receipt = Box::pin(finish_accepted_contact(&storage, &second)).await;
    assert_ne!(first.publication, second.publication);
    assert!(second_receipt.effect_ids.contains(&person.base.id));
    assert!(second_receipt.effect_ids.contains(&key.base.id));
    let after_second = EntityRepository::new(storage.clone())
        .get(SPACE, &person.base.id)
        .await
        .unwrap()
        .unwrap();
    assert!(after_second
        .aliases
        .iter()
        .any(|name| name == "Casey revision two"));
    assert_eq!(after_second.metadata, person.metadata);
    assert!(!after_second
        .aliases
        .iter()
        .any(|name| name == "Casey revision one"));
    assert!(person
        .aliases
        .iter()
        .all(|name| after_second.aliases.contains(name)));
    let repository = SourcePublicationRepository::new(storage.clone());
    let erased = Box::pin(
        repository.erase_live(
            lifecycle_request(
                &storage,
                &second.source.base.id,
                "erase-independent-contact",
            )
            .await,
        ),
    )
    .await
    .unwrap();
    assert_eq!(erased.state, OperationState::Erased);
    let restored_person = EntityRepository::new(storage.clone())
        .get(SPACE, &person.base.id)
        .await
        .unwrap()
        .unwrap();
    let restored_key = EntityIdentityKeyRepository::new(storage.clone())
        .get(SPACE, &key.base.id)
        .await
        .unwrap()
        .unwrap();
    // Compare all semantic and envelope fields. Only the lifecycle's own update
    // timestamp may advance; the old source's aliases/security/evidence may not.
    assert_eq!(semantic_row(&restored_person), semantic_row(&person),);
    assert_eq!(semantic_row(&restored_key), semantic_row(&key),);
    assert!(!restored_person
        .aliases
        .iter()
        .any(|name| name.starts_with("Casey revision")));
    assert!(restored_person.base.provenance.sources.is_empty());
    assert!(restored_key.evidence.is_empty());
    assert!(RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(AssertionRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    let remaining = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|row| &row.base.id).collect::<Vec<_>>(),
        vec![&person.base.id]
    );
    assert!(repository
        .support_for_record(SPACE, &person.base.id)
        .await
        .unwrap()
        .is_empty());
    assert!(SourceRepository::new(storage.clone())
        .get_with_deleted(SPACE, &second.source.base.id)
        .await
        .unwrap()
        .is_none());
    let requests = spy.requests().unwrap();
    assert!(!requests.is_empty());
    assert!(requests
        .iter()
        .all(|request| request.path.ends_with("/embeddings")));
}

#[expect(
    clippy::items_after_statements,
    reason = "Keep local diagnostic types and case definitions beside the control phase that uses them"
)]
async fn physical_identity_key_count(storage: &StorageClient, id: &RecordId) -> u64 {
    if let Some(store) = storage.embedded_store() {
        return u64::from(
            store
                .get::<elephant_types::records::identity::EntityIdentityKey>(SPACE, id, true)
                .await
                .unwrap()
                .is_some(),
        );
    }
    #[derive(serde::Deserialize)]
    struct PhysicalCount {
        count: u64,
    }
    let mut response = storage
        .query_bind_checked(
            "SELECT count() AS count FROM entity_identity_key WHERE id = $id GROUP ALL;",
            std::collections::BTreeMap::from([(
                "id".to_owned(),
                elephant_storage::surreal::thing_from_record_id(id),
            )]),
        )
        .await
        .unwrap();
    let counts: Vec<PhysicalCount> = response.take(0).unwrap();
    assert!(
        counts.len() <= 1,
        "physical identity aggregate must be unique"
    );
    counts.into_iter().map(|row| row.count).sum()
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn retracted_contact_can_be_explicitly_reaccepted_and_published(
    backend: provider_spy::Backend,
) {
    let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
    let source = input_source();
    let first = Box::pin(accept_contact_revision(
        &storage,
        source.clone(),
        "contact-before-retract",
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(&storage, &first)).await;
    let first_person = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|person| person.display_name == CONTACT)
        .unwrap();
    let first_keys = EntityIdentityKeyRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    assert!(!first_keys.is_empty());
    for key in &first_keys {
        assert_eq!(physical_identity_key_count(&storage, &key.base.id).await, 1);
    }
    let repository = SourcePublicationRepository::new(storage.clone());
    let retracted = Box::pin(
        repository
            .retract(lifecycle_request(&storage, &first.source.base.id, "retract-contact").await),
    )
    .await
    .unwrap();
    assert_eq!(retracted.state, OperationState::Retracted);
    assert!(repository
        .head(SPACE, &first.source.base.id)
        .await
        .unwrap()
        .unwrap()
        .published_revision
        .is_none());
    assert!(
        EntityRepository::new(storage.clone())
            .get_with_deleted(SPACE, &first_person.base.id)
            .await
            .unwrap()
            .is_none(),
        "source-exclusive contact identity must be physically removed, not hidden by a tombstone"
    );
    for key in &first_keys {
        assert_eq!(
            physical_identity_key_count(&storage, &key.base.id).await,
            0,
            "source-exclusive identity key must be physically removed"
        );
    }
    assert!(EntityRepository::new(storage.clone())
        .get(SPACE, &first_person.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(EntityIdentityKeyRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(RelRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    let next = Box::pin(accept_contact_revision(
        &storage,
        source,
        "contact-explicit-restore",
        Some(retracted.source_generation),
        true,
    ))
    .await;
    assert_eq!(next.source.base.id, first.source.base.id);
    assert_ne!(next.publication.generation, first.publication.generation);
    assert!(
        EntityRepository::new(storage.clone())
            .get(SPACE, &first_person.base.id)
            .await
            .unwrap()
            .is_none(),
        "acceptance alone cannot restore contact graph"
    );
    let accepted_head = repository
        .head(SPACE, &next.source.base.id)
        .await
        .unwrap()
        .unwrap();
    assert!(accepted_head.published_revision.is_none());
    assert_eq!(
        accepted_head.desired_revision.as_deref(),
        Some(next.publication.revision_id.as_str())
    );
    let receipt = Box::pin(finish_accepted_contact(&storage, &next)).await;
    let published_head = repository
        .head(SPACE, &next.source.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        published_head.published_revision.as_deref(),
        Some(next.publication.revision_id.as_str()),
        "restored publication must replace the historical progress lookup"
    );
    assert_eq!(published_head.operation_id, next.publication.operation_id);
    let person = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|person| person.display_name == CONTACT)
        .unwrap();
    assert_eq!(person.base.id, first_person.base.id);
    assert_eq!(person.identity_keys, vec![EMAIL.to_owned()]);
    assert!(receipt.effect_ids.contains(&person.base.id));
    assert!(repository
        .support_for_record(SPACE, &person.base.id)
        .await
        .unwrap()
        .iter()
        .any(
            |support| support.source_revision == next.publication.revision_id
                && support.source_id == next.source.base.id
        ));
    let requests = spy.requests().unwrap();
    assert!(!requests.is_empty());
    assert!(requests
        .iter()
        .all(|request| request.path.ends_with("/embeddings")));
}

#[tokio::test]
async fn embedded_contact_replacement_erasure_restores_pre_source_baseline() {
    replaced_contact_erasure_restores_independent_baseline(provider_spy::Backend::ElephantDb).await;
}

#[tokio::test]
async fn surreal_contact_replacement_erasure_restores_pre_source_baseline() {
    replaced_contact_erasure_restores_independent_baseline(provider_spy::Backend::SurrealMemory)
        .await;
}

#[tokio::test]
async fn embedded_contact_retraction_and_explicit_restore_publish_new_acceptance() {
    retracted_contact_can_be_explicitly_reaccepted_and_published(provider_spy::Backend::ElephantDb)
        .await;
}

#[tokio::test]
async fn surreal_contact_retraction_and_explicit_restore_publish_new_acceptance() {
    retracted_contact_can_be_explicitly_reaccepted_and_published(
        provider_spy::Backend::SurrealMemory,
    )
    .await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn published_contact_supports_public_supersession(backend: provider_spy::Backend) {
    let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
    let first = Box::pin(accept_contact_revision(
        &storage,
        input_source(),
        "contact-supersession-old",
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(&storage, &first)).await;
    let old_person = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.display_name == CONTACT)
        .unwrap();
    let relations = RelRepository::new(storage.clone());
    let old = relations
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.subject.id() == &old_person.base.id && row.rel_type == "works_at")
        .unwrap();
    assert!(old.evidence.is_empty());
    let mut input = input_source();
    input.external_id = Some("independent-contact-replacement-input".into());
    input.metadata["contact"] = serde_json::json!({
        "name":"Replacement Rowan", "email":"rowan@example.invalid",
        "company":"Vela Research", "job_title":"Scientist"
    });
    let second = Box::pin(accept_contact_revision(
        &storage,
        input,
        "contact-supersession-new",
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(&storage, &second)).await;
    assert_ne!(first.source.base.id, second.source.base.id);
    let replacement_person = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.display_name == "Replacement Rowan")
        .unwrap();
    let replacement = relations
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.subject.id() == &replacement_person.base.id && row.rel_type == "works_at")
        .unwrap();
    assert!(replacement.evidence.is_empty());
    assert_ne!(old.base.id, replacement.base.id);
    assert_eq!(old.base.security, replacement.base.security);
    assert_eq!(old.base.subjects, replacement.base.subjects);
    assert_eq!(old.base.subjects_state, replacement.base.subjects_state);
    let publications = SourcePublicationRepository::new(storage.clone());
    for accepted in [&first, &second] {
        let head = publications
            .head(SPACE, &accepted.source.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(head.generation, accepted.publication.generation);
        assert_eq!(head.operation_id, accepted.publication.operation_id);
        assert_eq!(
            head.published_revision.as_deref(),
            Some(accepted.publication.revision_id.as_str())
        );
        assert_eq!(head.desired_revision, head.published_revision);
    }
    let requests_before = spy.requests().unwrap().len();
    let result = Box::pin(elephant_pipeline::reprocess::supersede_rel(
        &relations,
        &old.base.id,
        &replacement.base.id,
        SPACE,
    ))
    .await
    .expect("public canonical supersession with original sealed contact proof");
    assert_eq!(result.old_id, old.base.id);
    assert_eq!(result.new_id, replacement.base.id);
    let after = relations.get(SPACE, &old.base.id).await.unwrap().unwrap();
    assert_eq!(after.status, ClaimStatus::Superseded);
    let mut expected = old.clone();
    expected.status = ClaimStatus::Superseded;
    assert_eq!(
        semantic_row(&after),
        semantic_row(&expected),
        "the contact proof permits the requested status change, not new facts or weaker authorization",
    );
    assert_eq!(
        relations
            .get(SPACE, &replacement.base.id)
            .await
            .unwrap()
            .unwrap(),
        replacement
    );
    let audits = elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    assert!(audits
        .iter()
        .any(|audit| audit.record == old.base.id && audit.actor == "reprocess_supersession"));
    let events = SystemEventRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let repeated = Box::pin(elephant_pipeline::reprocess::supersede_rel(
        &relations,
        &old.base.id,
        &replacement.base.id,
        SPACE,
    ))
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(repeated).unwrap(),
        serde_json::to_value(result).unwrap()
    );
    assert_eq!(
        snapshot(audits),
        snapshot(
            elephant_storage::AuditLogRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap()
        )
    );
    assert_eq!(
        snapshot(events),
        snapshot(
            SystemEventRepository::new(storage)
                .list(SPACE, 100, None)
                .await
                .unwrap()
        )
    );
    assert_eq!(
        spy.requests().unwrap().len(),
        requests_before,
        "public supersession requires no model request"
    );
}

#[tokio::test]
async fn embedded_actual_contact_rel_supports_public_canonical_supersession() {
    published_contact_supports_public_supersession(provider_spy::Backend::ElephantDb).await;
}

#[tokio::test]
async fn surreal_actual_contact_rel_supports_public_canonical_supersession() {
    published_contact_supports_public_supersession(provider_spy::Backend::SurrealMemory).await;
}

#[test]
fn contact_oracle_futures_do_not_aggregate_nested_phase_storage_on_the_stack() {
    // Measure the actual returned future TYPE without constructing or polling
    // it. A giant future can overflow while being constructed before an outer
    // Box::pin has a chance to move it to the heap.
    fn future_bytes<F: std::future::Future>(_: impl FnOnce(provider_spy::Backend) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    for (phase, bytes) in [
        (
            "shared contact inverse and retained maintenance",
            future_bytes(|backend| {
                shared_contact_survives_source_withdrawal(
                    backend,
                    SharedContactWithdrawal::LaterSource,
                )
            }),
        ),
        (
            "plain retained reprocess",
            future_bytes(published_plain_contact_metadata_reprocesses_without_contact_authority),
        ),
        (
            "initial contact and Graph publication",
            future_bytes(accepted_contact_and_graph_publish_together),
        ),
        (
            "replacement and erasure",
            future_bytes(replaced_contact_erasure_restores_independent_baseline),
        ),
        (
            "retraction and reactivation",
            future_bytes(retracted_contact_can_be_explicitly_reaccepted_and_published),
        ),
        (
            "public contact supersession",
            future_bytes(published_contact_supports_public_supersession),
        ),
    ] {
        assert!(
            bytes <= 64 * 1024,
            "{phase} future stores {bytes} bytes before polling"
        );
    }
}

struct ClaimedContactReprocess {
    context: elephant_pipeline::reprocess::ReprocessExecutionContext,
    config: elephant_pipeline::reprocess::ReprocessConfig,
    job: elephant_types::records::reprocess::ReprocessJob,
    run: WorkRun,
    attempt: elephant_types::publication::AttemptFence,
    coordinator_id: RecordId,
}

async fn claim_contact_reprocess(
    storage: &StorageClient,
    purpose: &str,
) -> ClaimedContactReprocess {
    use elephant_pipeline::reprocess::{
        create_reprocess_job, ReprocessConfig, ReprocessExecutionContext,
    };
    use elephant_types::records::reprocess::{ReprocessTargetStage, SelectionCriteria};
    let ctx = ReprocessExecutionContext {
        storage: storage.clone(),
        job_repo: ReprocessJobRepository::new(storage.clone()),
        source_repo: SourceRepository::new(storage.clone()),
        doc_repo: DocRepository::new(storage.clone()),
        doc_revision_repo: DocRevisionRepository::new(storage.clone()),
        assertion_repo: AssertionRepository::new(storage.clone()),
        work_run_repo: WorkRunRepository::new(storage.clone()),
    };
    let config = ReprocessConfig::default();
    let job = Box::pin(create_reprocess_job(
        &ctx.job_repo,
        purpose,
        SelectionCriteria {
            source_types: vec!["manual".into()],
            space_ids: vec![SPACE.into()],
            ..Default::default()
        },
        vec![ReprocessTargetStage::ReExtract],
        SPACE,
        ACTOR,
        Some(&config),
    ))
    .await
    .unwrap()
    .job;
    let items = WorkItemRepository::new(storage.clone());
    let coordinator_id = ctx.job_repo.ensure_work(&job).await.unwrap();
    let coordinator = items
        .claim_with_lease(
            SPACE,
            &coordinator_id,
            ACTOR,
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .unwrap()
        .unwrap();
    let attempt =
        elephant_storage::work_attempt_repository::fence_from_claim(&coordinator).unwrap();
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert(
        "attempt_fence".into(),
        serde_json::to_value(&attempt).unwrap(),
    );
    let mut run_base = coordinator.base.clone();
    run_base.id = RecordId::new_v4(TableName::WorkRun);
    let run = ctx
        .work_run_repo
        .create(WorkRun {
            base: run_base,
            work_item: Link::new(coordinator.base.id.clone()),
            attempt: coordinator.attempts,
            started_at: Utc::now(),
            completed_at: None,
            success: false,
            error: None,
            metrics,
            worker_id: attempt.lease_token.clone(),
            worker_version: "actual-retained-contact-oracle".into(),
            model_version: None,
            prompt_id: None,
            config_hash: None,
        })
        .await
        .unwrap();
    ClaimedContactReprocess {
        context: ctx,
        config,
        job,
        run,
        attempt,
        coordinator_id,
    }
}

async fn complete_contact_reprocess_coordinator(
    ctx: &elephant_pipeline::reprocess::ReprocessExecutionContext,
    run_id: &RecordId,
    coordinator_id: &RecordId,
    original_attempt: &elephant_types::publication::AttemptFence,
) {
    let coordinator = WorkItemRepository::new(ctx.storage.clone())
        .get(SPACE, coordinator_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        &elephant_storage::work_attempt_repository::fence_from_claim(&coordinator).unwrap(),
        original_attempt
    );
    let mut run = ctx.work_run_repo.get(SPACE, run_id).await.unwrap().unwrap();
    assert_eq!(run.work_item.id(), coordinator_id);
    assert_eq!(
        run.metrics.custom["attempt_fence"],
        serde_json::to_value(original_attempt).unwrap()
    );
    run.success = true;
    run.completed_at = Some(Utc::now());
    run.error = None;
    ctx.work_run_repo.update(SPACE, run_id, run).await.unwrap();
    let queue = WorkQueue::new(ctx.storage.clone(), WorkQueueConfig::default())
        .for_attempt(&coordinator)
        .unwrap();
    Box::pin(complete_work_item_and_promote(
        &queue,
        &DagExecutor::new(ctx.storage.clone()),
        SPACE,
        coordinator_id,
    ))
    .await
    .unwrap();
    assert_eq!(
        WorkItemRepository::new(ctx.storage.clone())
            .get(SPACE, coordinator_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        WorkItemStatus::Done
    );
}

/// Dispatch actual admitted children while the actual coordinator remains
/// leased. Only the captured source operation, never runnable-queue emptiness,
/// determines publication completion.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn dispatch_reprocess_children(
    storage: &StorageClient,
    publication: &elephant_types::publication::PublicationFence,
    coordinator_id: &RecordId,
    extractor: Option<&dyn Extractor>,
) {
    let repository = SourcePublicationRepository::new(storage.clone());
    let before = graph_rows(storage).await;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    let mut finalizations = 0;
    for _ in 0..100 {
        if repository
            .operation(SPACE, &publication.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state
            == OperationState::Completed
        {
            break;
        }
        let runnable = Box::pin(ready_contact_items(storage, deadline)).await;
        assert!(
            !runnable.is_empty(),
            "pending source lost its durable fanout"
        );
        for item in runnable {
            assert_ne!(&item.base.id, coordinator_id);
            if item.kind == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND {
                assert_eq!(
                    graph_rows(storage).await,
                    before,
                    "pending reprocess retains the whole published graph until atomic finalization"
                );
            }
            let completed = Box::pin(dispatch_contact_step(storage, &item, extractor)).await;
            if completed
                && item.kind
                    == elephant_pipeline::source_finalization::FINALIZE_SOURCE_REVISION_KIND
            {
                finalizations += 1;
            }
        }
    }
    assert_eq!(finalizations, 1);
    assert_eq!(
        repository
            .operation(SPACE, &publication.operation_id)
            .await
            .unwrap()
            .unwrap()
            .state,
        OperationState::Completed
    );
}

/// A plain source may carry contact-shaped metadata without having ever been
/// admitted as structured contact effects. Retained reprocessing must preserve
/// that original class through a real published predecessor, not just an
/// accepted-storage fixture or a metadata-based capability guess.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn published_plain_contact_metadata_reprocesses_without_contact_authority(
    backend: provider_spy::Backend,
) {
    use elephant_pipeline::reprocess::execute_reprocess_job_with_attempt;
    use elephant_types::publication::{PublicationFence, SourceProducerOrigin};
    use elephant_types::records::reprocess::ReprocessStatus;

    let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
    let mut input = input_source();
    input.base.security = elephant_types::records::base::Security::default();
    input.set_requested_ingest_mode(IngestMode::Metadata);
    let prepared = Box::pin(PreparedSourceIntake::prepare(
        &storage,
        input,
        IntakeControl {
            revision_id: Some("published-plain-contact-metadata".into()),
            idempotency_key: Some("published-plain-contact-metadata".into()),
            expected_generation: None,
        },
    ))
    .await
    .unwrap();
    let accepted = Box::pin(prepared.accept(&storage)).await.unwrap();
    let publication = SourcePublicationRepository::new(storage.clone());
    assert!(publication
        .accepted_contact_for_publication(SPACE, &accepted.publication)
        .await
        .unwrap()
        .is_none());
    Box::pin(finish_accepted_contact(&storage, &accepted)).await;
    assert_no_live_graph(&storage).await;
    let original = publication
        .head(SPACE, &accepted.source.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        original.published_revision.as_deref(),
        Some("published-plain-contact-metadata")
    );
    assert_eq!(original.operation_id, accepted.operation.operation_id);

    let ClaimedContactReprocess {
        context: ctx,
        config,
        job,
        run,
        attempt,
        coordinator_id,
    } = Box::pin(claim_contact_reprocess(
        &storage,
        "actual published plain metadata retained reprocess",
    ))
    .await;
    let admitted = Box::pin(execute_reprocess_job_with_attempt(
        &ctx,
        &job.base.id,
        &run.base.id,
        SPACE,
        &config,
        Some(attempt.clone()),
    ))
    .await
    .unwrap();
    assert!(admitted.error.is_none());
    assert_eq!(admitted.pending_operations.len(), 1);
    assert_eq!(admitted.sources_processed, 0);
    let operation_id = admitted.pending_operations[0].clone();
    let operation = publication
        .operation(SPACE, &operation_id)
        .await
        .unwrap()
        .unwrap();
    let successor = PublicationFence {
        source_id: operation.source_id.clone(),
        generation: operation.source_generation.clone(),
        revision_id: operation.desired_revision.clone().unwrap(),
        operation_id: operation_id.clone(),
    };
    assert_ne!(successor, accepted.publication);
    assert!(publication
        .accepted_contact_for_publication(SPACE, &successor)
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        publication
            .source_operation_producer(SPACE, &successor)
            .await
            .unwrap(),
        SourceProducerOrigin::Reprocess { original_attempt: Some(ref saved), .. }
            if saved == &attempt
    ));
    let pending_head = publication
        .head(SPACE, &successor.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending_head.published_revision, original.published_revision);
    assert_eq!(pending_head.generation, successor.generation);
    let retry = Box::pin(execute_reprocess_job_with_attempt(
        &ctx,
        &job.base.id,
        &run.base.id,
        SPACE,
        &config,
        Some(attempt.clone()),
    ))
    .await
    .unwrap();
    assert_eq!(retry.pending_operations, admitted.pending_operations);
    assert_eq!(
        publication.head(SPACE, &successor.source_id).await.unwrap(),
        Some(pending_head)
    );
    assert_no_live_graph(&storage).await;

    Box::pin(dispatch_reprocess_children(
        &storage,
        &successor,
        &coordinator_id,
        None,
    ))
    .await;
    let completed = publication
        .operation(SPACE, &operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completed.state, OperationState::Completed);
    assert_eq!(completed.source_generation, successor.generation);
    assert_eq!(
        completed.published_revision,
        Some(successor.revision_id.clone())
    );
    let completion = SourcePublicationManifestRepository::new(storage.clone())
        .completion(SPACE, &successor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completion.publication, successor);
    assert!(CanonicalRepository::new(storage.clone())
        .receipt(SPACE, &completion.commit_id)
        .await
        .unwrap()
        .is_some());
    let current = ctx
        .source_repo
        .get(SPACE, &successor.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.current_ingest_mode, Some(IngestMode::Metadata));
    assert_eq!(
        current.lifecycle_status,
        elephant_types::records::SourceLifecycleStatus::MetadataReady
    );
    assert_eq!(
        current.metadata["contact"],
        accepted.source.metadata["contact"]
    );
    assert_no_live_graph(&storage).await;
    assert!(publication
        .accepted_contact_for_publication(SPACE, &successor)
        .await
        .unwrap()
        .is_none());
    let finished = Box::pin(execute_reprocess_job_with_attempt(
        &ctx,
        &job.base.id,
        &run.base.id,
        SPACE,
        &config,
        Some(attempt.clone()),
    ))
    .await
    .unwrap();
    assert!(finished.error.is_none());
    assert!(finished.pending_operations.is_empty());
    assert_eq!(finished.sources_processed, 1);
    assert_eq!(
        ctx.job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ReprocessStatus::Completed
    );
    Box::pin(complete_contact_reprocess_coordinator(
        &ctx,
        &run.base.id,
        &coordinator_id,
        &attempt,
    ))
    .await;
    assert!(
        spy.requests().unwrap().is_empty(),
        "Metadata must not invoke an embedding provider"
    );
}

#[tokio::test]
async fn embedded_actual_published_plain_contact_metadata_reprocesses_as_plain() {
    published_plain_contact_metadata_reprocesses_without_contact_authority(
        provider_spy::Backend::ElephantDb,
    )
    .await;
}

#[tokio::test]
async fn surreal_actual_published_plain_contact_metadata_reprocesses_as_plain() {
    published_plain_contact_metadata_reprocesses_without_contact_authority(
        provider_spy::Backend::SurrealMemory,
    )
    .await;
}

/// Raw control rows have no public visibility projection. The existing control
/// key protocol length-delimits (space, actual transaction ID) before `UUIDv5`;
/// querying that exact physical key is independent of private payload decoding.
#[expect(
    clippy::format_collect,
    reason = "Retain the existing length-delimited physical control key construction for this oracle"
)]
fn private_control_key(parts: &[&str]) -> String {
    let identity = parts
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<String>();
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, identity.as_bytes()).to_string()
}

async fn physical_control_count(storage: &StorageClient, table: &str, key: &str) -> usize {
    assert!(matches!(
        table,
        "source_commit_receipt" | "source_contact_intent" | "source_effect_support"
    ));
    if let Some(store) = storage.embedded_store() {
        // Kernel extension row keys include the table prefix. Its page API
        // caps each page at101, so inspect successive complete pages before
        // claiming physical absence rather than checking a truncated first page.
        let full_key = format!("{table}:{key}");
        let mut after: Option<String> = None;
        let mut inspected = 0;
        let mut found = 0;
        loop {
            let rows = store
                .extension_rows_page(table, SPACE, after.as_deref(), 101)
                .await
                .unwrap();
            inspected += rows.len();
            assert!(
                inspected <= 256,
                "physical control fixture exceeded its bound"
            );
            found += rows
                .iter()
                .filter(|(actual, _)| actual == &full_key)
                .count();
            if rows.len() < 101 {
                return found;
            }
            let next = rows.last().map(|(id, _)| id.clone());
            assert_ne!(next, after, "physical control cursor did not advance");
            after = next;
        }
    }
    let mut response = storage
        .query_bind_checked(
            &format!(
                "SELECT count() AS n FROM {table} WHERE id=type::thing($table,$key) GROUP ALL"
            ),
            std::collections::BTreeMap::from([
                ("table", table.to_owned()),
                ("key", key.to_owned()),
            ]),
        )
        .await
        .unwrap();
    let counts: Vec<serde_json::Value> = response.take(0).unwrap();
    counts.first().map_or(0, |row| {
        usize::try_from(row["n"].as_u64().unwrap()).unwrap()
    })
}

async fn physical_canonical_receipt_count(storage: &StorageClient, commit_id: &str) -> usize {
    physical_control_count(
        storage,
        "source_commit_receipt",
        &private_control_key(&[SPACE, commit_id]),
    )
    .await
}

async fn physical_contact_intent_count(
    storage: &StorageClient,
    publication: &elephant_types::publication::PublicationFence,
) -> usize {
    physical_control_count(
        storage,
        "source_contact_intent",
        &private_control_key(&[
            SPACE,
            &publication.source_id.to_string(),
            &publication.generation,
            &publication.operation_id,
            &publication.revision_id,
        ]),
    )
    .await
}

/// Complete bounded fixture snapshots, including terminal queue/artifact states.
/// Equality here detects restaging even when a retry would leave the live graph
/// apparently unchanged. All comparisons retain every serialized field.
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn retained_retry_rows(storage: &StorageClient) -> Vec<Vec<String>> {
    use elephant_types::records::ArtifactStatus;
    let mut rows = graph_rows(storage).await.to_vec();
    rows.push(snapshot(
        SourceRepository::new(storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap(),
    ));
    rows.push(snapshot(
        DocRepository::new(storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap(),
    ));
    rows.push(snapshot(
        SystemEventRepository::new(storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap(),
    ));
    let items = WorkItemRepository::new(storage.clone());
    for status in [
        WorkItemStatus::Pending,
        WorkItemStatus::Runnable,
        WorkItemStatus::Running,
        WorkItemStatus::Done,
        WorkItemStatus::Failed,
        WorkItemStatus::Cancelled,
    ] {
        rows.push(snapshot(items.find_by_status(SPACE, status).await.unwrap()));
    }
    let artifacts = elephant_storage::WorkArtifactRepository::new(storage.clone());
    for status in [
        ArtifactStatus::Draft,
        ArtifactStatus::Validated,
        ArtifactStatus::Committing,
        ArtifactStatus::Committed,
        ArtifactStatus::Rejected,
    ] {
        rows.push(snapshot(
            artifacts.find_by_status(SPACE, status).await.unwrap(),
        ));
    }
    rows
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn surviving_contact_retries_read_only_then_reprocesses(
    storage: &StorageClient,
    accepted: &elephant_storage::source_intake::AcceptedSourceIntake,
    original_input: &Source,
    relation_id: &RecordId,
) {
    // Keep each actual phase in a separately boxed future. The outer type-size
    // check alone does not bound a nested async function's polling frame.
    eprintln!("contact survivor phase: original read-only retry");
    let original_head = Box::pin(surviving_contact_read_only_retry(
        storage,
        accepted,
        original_input,
    ))
    .await;
    eprintln!("contact survivor phase: new admission and exact retry");
    let reprocess = Box::pin(admit_surviving_contact_reprocess(
        storage,
        accepted,
        &original_head,
    ))
    .await;
    eprintln!("contact survivor phase: admitted child publication");
    Box::pin(dispatch_surviving_contact_reprocess(storage, &reprocess)).await;
    eprintln!("contact survivor phase: completed support and coordinator");
    Box::pin(verify_surviving_contact_reprocess(
        storage,
        accepted,
        relation_id,
        reprocess,
    ))
    .await;
    eprintln!("contact survivor phase: original completion retained");
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn surviving_contact_read_only_retry(
    storage: &StorageClient,
    accepted: &elephant_storage::source_intake::AcceptedSourceIntake,
    original_input: &Source,
) -> elephant_types::publication::SourceHead {
    let publication = SourcePublicationRepository::new(storage.clone());
    let descriptor = publication
        .accepted_contact_capability_for_publication(SPACE, &accepted.publication)
        .await
        .unwrap()
        .expect("original independently admitted contact descriptor survives erased before-images");
    assert_eq!(descriptor.publication(), &accepted.publication);
    assert!(!descriptor.digest().is_empty());
    assert_eq!(
        physical_contact_intent_count(storage, &accepted.publication).await,
        0
    );
    assert!(
        matches!(
            publication.accepted_contact_for_publication(SPACE, &accepted.publication).await,
            Err(elephant_storage::StorageError::Conflict(message))
                if message == "accepted_contact_plan_unavailable"
        ),
        "strict getter must report the same explicit unavailable-plan category"
    );
    let original_head = publication
        .head(SPACE, &accepted.source.base.id)
        .await
        .unwrap()
        .unwrap();
    let original_receipt = publication
        .operation(SPACE, &accepted.publication.operation_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(original_receipt.state, OperationState::Completed);
    let immutable = publication
        .revision(
            SPACE,
            &accepted.source.base.id,
            &accepted.publication.revision_id,
        )
        .await
        .unwrap()
        .unwrap();
    let before = Box::pin(retained_retry_rows(storage)).await;
    // This is the shared trusted storage adapter. Transport scope/ABAC denial
    // coverage is separately owned by the public intake adapter tests.
    let retry = Box::pin(PreparedSourceIntake::prepare(
        storage,
        original_input.clone(),
        IntakeControl {
            revision_id: Some("shared-contact-b".into()),
            idempotency_key: Some("contact-lifecycle:shared-contact-b".into()),
            expected_generation: None,
        },
    ))
    .await
    .unwrap();
    assert!(matches!(Box::pin(retry.prepare_contact(storage)).await,
        Err(elephant_storage::StorageError::Conflict(message)) if message == "accepted_contact_plan_unavailable"));
    let receipt_only = Box::pin(retry.read_only_contact_retry(storage))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(receipt_only.operation, original_receipt);
    assert_eq!(receipt_only.publication, accepted.publication);
    assert!(!receipt_only.needs_processing());
    let repeated = Box::pin(retry.accept(storage)).await.unwrap();
    assert_eq!(repeated.operation, original_receipt);
    assert!(!repeated.needs_processing());
    assert_eq!(Box::pin(retained_retry_rows(storage)).await, before);
    assert_eq!(
        publication
            .operation(SPACE, &accepted.publication.operation_id)
            .await
            .unwrap(),
        Some(original_receipt),
        "read-only retry must preserve the stored operation, not only return its old receipt"
    );
    assert_eq!(
        publication
            .head(SPACE, &accepted.source.base.id)
            .await
            .unwrap(),
        Some(original_head.clone())
    );
    assert_eq!(
        serde_json::to_value(
            publication
                .revision(
                    SPACE,
                    &accepted.source.base.id,
                    &accepted.publication.revision_id
                )
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(immutable).unwrap()
    );
    assert_eq!(
        physical_contact_intent_count(storage, &accepted.publication).await,
        0,
        "matching original retry cannot recreate an erased contact plan"
    );
    original_head
}

struct SurvivingContactReprocess {
    claimed: ClaimedContactReprocess,
    successor: elephant_types::publication::PublicationFence,
}

#[test]
fn surviving_contact_child_future_types_fit_the_fixture_stack_budget() {
    use elephant_storage::source_intake::AcceptedSourceIntake;
    use elephant_types::publication::SourceHead;

    // Never construct storage or poll a future here. Static references only
    // select the actual returned types, including the previously unchecked
    // survivor child and each of its concrete awaited phases.
    fn bytes<Arguments, F: std::future::Future>(_: impl FnOnce(Arguments) -> F) -> usize {
        std::mem::size_of::<F>()
    }
    for (phase, size) in [
        (
            "survivor orchestration",
            bytes(
                |(storage, accepted, input, relation): (
                    &'static StorageClient,
                    &'static AcceptedSourceIntake,
                    &'static Source,
                    &'static RecordId,
                )| {
                    surviving_contact_retries_read_only_then_reprocesses(
                        storage, accepted, input, relation,
                    )
                },
            ),
        ),
        (
            "original read-only retry",
            bytes(
                |(storage, accepted, input): (
                    &'static StorageClient,
                    &'static AcceptedSourceIntake,
                    &'static Source,
                )| surviving_contact_read_only_retry(storage, accepted, input),
            ),
        ),
        (
            "new admission and exact retry",
            bytes(
                |(storage, accepted, head): (
                    &'static StorageClient,
                    &'static AcceptedSourceIntake,
                    &'static SourceHead,
                )| admit_surviving_contact_reprocess(storage, accepted, head),
            ),
        ),
        (
            "admitted child publication",
            bytes(
                |(storage, reprocess): (
                    &'static StorageClient,
                    &'static SurvivingContactReprocess,
                )| dispatch_surviving_contact_reprocess(storage, reprocess),
            ),
        ),
        (
            "completed support and coordinator",
            bytes(
                |(storage, accepted, relation, reprocess): (
                    &'static StorageClient,
                    &'static AcceptedSourceIntake,
                    &'static RecordId,
                    SurvivingContactReprocess,
                )| {
                    verify_surviving_contact_reprocess(storage, accepted, relation, reprocess)
                },
            ),
        ),
        (
            "original relational support",
            bytes(
                |(canonical, relation, source): (
                    &'static CanonicalRepository,
                    &'static elephant_types::records::Rel,
                    &'static RecordId,
                )| canonical.accepted_contact_rel_support(SPACE, relation, source),
            ),
        ),
    ] {
        assert!(
            size <= 64 * 1024,
            "survivor {phase} future stores {size} bytes before polling"
        );
    }
}

async fn admit_surviving_contact_reprocess(
    storage: &StorageClient,
    accepted: &elephant_storage::source_intake::AcceptedSourceIntake,
    original_head: &elephant_types::publication::SourceHead,
) -> SurvivingContactReprocess {
    use elephant_pipeline::reprocess::execute_reprocess_job_with_attempt;
    use elephant_types::publication::{PublicationFence, SourceProducerOrigin};
    let publication = SourcePublicationRepository::new(storage.clone());
    let ClaimedContactReprocess {
        context: ctx,
        config,
        job,
        run,
        attempt,
        coordinator_id,
    } = Box::pin(claim_contact_reprocess(
        storage,
        "fresh surviving B after physical A erasure",
    ))
    .await;
    let admitted = Box::pin(execute_reprocess_job_with_attempt(
        &ctx,
        &job.base.id,
        &run.base.id,
        SPACE,
        &config,
        Some(attempt.clone()),
    ))
    .await
    .unwrap();
    assert!(admitted.error.is_none());
    assert_eq!(admitted.sources_processed, 0);
    assert_eq!(admitted.pending_operations.len(), 1);
    let operation = publication
        .operation(SPACE, &admitted.pending_operations[0])
        .await
        .unwrap()
        .unwrap();
    let successor = PublicationFence {
        source_id: operation.source_id.clone(),
        generation: operation.source_generation.clone(),
        revision_id: operation.desired_revision.clone().unwrap(),
        operation_id: operation.operation_id.clone(),
    };
    assert_eq!(successor.source_id, accepted.source.base.id);
    assert_ne!(successor, accepted.publication);
    assert_ne!(successor.operation_id, accepted.publication.operation_id);
    let fresh_contact = publication
        .accepted_contact_for_publication(SPACE, &successor)
        .await
        .unwrap()
        .unwrap();
    assert!(!fresh_contact.authorizations().is_empty());
    let fresh_admission = publication
        .accepted_contact_capability_for_publication(SPACE, &successor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fresh_admission.publication(), &successor);
    assert_eq!(physical_contact_intent_count(storage, &successor).await, 1);
    assert_eq!(
        physical_contact_intent_count(storage, &accepted.publication).await,
        0
    );
    assert!(
        matches!(publication.source_operation_producer(SPACE, &successor).await.unwrap(),
        SourceProducerOrigin::Reprocess { original_attempt: Some(ref saved), .. } if saved == &attempt)
    );
    let pending = publication
        .head(SPACE, &successor.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.published_revision, original_head.published_revision);
    assert_eq!(pending.generation, successor.generation);
    let repeated_admission = Box::pin(execute_reprocess_job_with_attempt(
        &ctx,
        &job.base.id,
        &run.base.id,
        SPACE,
        &config,
        Some(attempt.clone()),
    ))
    .await
    .unwrap();
    assert_eq!(
        repeated_admission.pending_operations,
        admitted.pending_operations
    );
    assert_eq!(
        publication.head(SPACE, &successor.source_id).await.unwrap(),
        Some(pending)
    );
    SurvivingContactReprocess {
        claimed: ClaimedContactReprocess {
            context: ctx,
            config,
            job,
            run,
            attempt,
            coordinator_id,
        },
        successor,
    }
}

async fn dispatch_surviving_contact_reprocess(
    storage: &StorageClient,
    reprocess: &SurvivingContactReprocess,
) {
    let successor = &reprocess.successor;
    let coordinator_id = &reprocess.claimed.coordinator_id;
    let extractor = ContactGraphExtractor {
        manifest: elephant_pipeline::extraction::DefaultExtractor::new()
            .manifest()
            .clone(),
    };
    Box::pin(
        elephant_pipeline::provider_dispatch::with_dispatch_credentials(
            std::sync::Arc::new(provider_spy::SpyCredentials),
            Box::pin(dispatch_reprocess_children(
                storage,
                successor,
                coordinator_id,
                Some(&extractor),
            )),
        ),
    )
    .await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn verify_surviving_contact_reprocess(
    storage: &StorageClient,
    accepted: &elephant_storage::source_intake::AcceptedSourceIntake,
    relation_id: &RecordId,
    reprocess: SurvivingContactReprocess,
) {
    use elephant_pipeline::reprocess::execute_reprocess_job_with_attempt;
    use elephant_types::records::reprocess::ReprocessStatus;
    let SurvivingContactReprocess {
        claimed:
            ClaimedContactReprocess {
                context: ctx,
                config,
                job,
                run,
                attempt,
                coordinator_id,
            },
        successor,
    } = reprocess;
    let completion = SourcePublicationManifestRepository::new(storage.clone())
        .completion(SPACE, &successor)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(completion.publication, successor);
    assert!(CanonicalRepository::new(storage.clone())
        .receipt(SPACE, &completion.commit_id)
        .await
        .unwrap()
        .is_some());
    let current = ctx
        .source_repo
        .get(SPACE, &successor.source_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        current.lifecycle_status,
        elephant_types::records::SourceLifecycleStatus::GraphReady
    );
    assert_eq!(current.current_ingest_mode, Some(IngestMode::Graph));
    assert_eq!(
        current.metadata["contact"],
        accepted.source.metadata["contact"]
    );
    assert_eq!(current.base.security, accepted.source.base.security);
    let rel = RelRepository::new(storage.clone())
        .get(SPACE, relation_id)
        .await
        .unwrap()
        .unwrap();
    eprintln!("contact survivor completion: exact current relational support");
    assert!(Box::pin(
        CanonicalRepository::new(storage.clone()).accepted_contact_rel_support(
            SPACE,
            &rel,
            &successor.source_id
        ),
    )
    .await
    .unwrap());
    // A current positive support proof does not exclude an extra stale row.
    // Check every original-source support before any later maintenance runs.
    let publications = SourcePublicationRepository::new(storage.clone());
    let head = Box::pin(publications.head(SPACE, &successor.source_id))
        .await
        .unwrap()
        .unwrap();
    assert_ne!(successor.revision_id, accepted.publication.revision_id);
    assert_eq!(
        head.published_revision.as_deref(),
        Some(successor.revision_id.as_str())
    );
    let supports: Vec<_> = Box::pin(publications.support_for_record(SPACE, relation_id))
        .await
        .unwrap()
        .into_iter()
        .filter(|support| support.source_id == successor.source_id)
        .collect();
    assert_eq!(
        supports.len(),
        1,
        "actual replacement must retire the original revision's support row"
    );
    let fingerprint = elephant_storage::canonical_commit::source_support_fingerprint(&rel).unwrap();
    assert!(
        supports.iter().all(|support| {
            support.source_revision == successor.revision_id
                && support.record_fingerprint == fingerprint
                && support.last_commit_id.as_deref() == Some(completion.commit_id.as_str())
        }),
        "every retained support must belong to the original completed successor"
    );
    eprintln!("contact survivor completion: original reprocess attempt");
    let finished = Box::pin(execute_reprocess_job_with_attempt(
        &ctx,
        &job.base.id,
        &run.base.id,
        SPACE,
        &config,
        Some(attempt.clone()),
    ))
    .await
    .unwrap();
    assert!(finished.error.is_none());
    assert!(finished.pending_operations.is_empty());
    assert_eq!(finished.sources_processed, 1);
    assert_eq!(
        ctx.job_repo
            .get(SPACE, &job.base.id)
            .await
            .unwrap()
            .unwrap()
            .status,
        ReprocessStatus::Completed
    );
    eprintln!("contact survivor completion: original coordinator lease");
    Box::pin(complete_contact_reprocess_coordinator(
        &ctx,
        &run.base.id,
        &coordinator_id,
        &attempt,
    ))
    .await;
    assert_eq!(
        physical_contact_intent_count(storage, &accepted.publication).await,
        0,
        "fresh independently authorized intent must not resurrect the old erased plan"
    );
}

#[derive(Clone, Copy)]
enum SharedContactWithdrawal {
    LaterSource,
    LaterSourceAfterIndependentStatus,
    OriginalSource,
    OriginalSourceDistinctFields,
}

async fn physical_identity_value_count(storage: &StorageClient, value: &str) -> usize {
    if let Some(store) = storage.embedded_store() {
        return store
            .scan::<elephant_types::records::EntityIdentityKey>()
            .await
            .unwrap()
            .into_iter()
            .filter(|row| row.base.space_id == SPACE && row.key == value)
            .count();
    }
    let mut response = storage.query_bind_checked(
        "SELECT count() AS n FROM entity_identity_key WHERE space_id=$space AND key=$key GROUP ALL",
        std::collections::BTreeMap::from([("space", SPACE.to_owned()), ("key", value.to_owned())]),
    ).await.unwrap();
    let counts: Vec<serde_json::Value> = response.take(0).unwrap();
    counts.first().map_or(0, |row| {
        usize::try_from(row["n"].as_u64().unwrap()).unwrap()
    })
}

#[expect(
    clippy::items_after_statements,
    clippy::significant_drop_tightening,
    reason = "Keep local diagnostic types and case definitions beside the control phase that uses them; Retain the original test fixture and repository lifetimes across the complete control"
)]
async fn shared_contact_survives_source_withdrawal(
    backend: provider_spy::Backend,
    scenario: SharedContactWithdrawal,
) {
    use elephant_storage::public_mutation::PublicMutation;

    let (storage, spy) = Box::pin(lifecycle_storage(backend)).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let canonical = CanonicalRepository::new(storage.clone());
    let relations = RelRepository::new(storage.clone());
    let distinct_fields = matches!(
        scenario,
        SharedContactWithdrawal::OriginalSourceDistinctFields
    );
    let withdraw_original = matches!(
        scenario,
        SharedContactWithdrawal::OriginalSource
            | SharedContactWithdrawal::OriginalSourceDistinctFields
    );
    const ERASED_PROFILE: &str = "https://example.invalid/erased-casey";
    const SURVIVING_PROFILE: &str = "https://example.invalid/surviving-blair";
    const SURVIVING_NAME: &str = "Surviving Blair";
    let mut first_input = input_source();
    if distinct_fields {
        first_input.metadata["contact"]["profile"] = serde_json::json!(ERASED_PROFILE);
    }
    let first = Box::pin(accept_contact_revision(
        &storage,
        first_input,
        "shared-contact-a",
        None,
        false,
    ))
    .await;
    let first_receipt = Box::pin(finish_accepted_contact(&storage, &first)).await;
    let person = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.display_name == CONTACT)
        .unwrap();
    let original_rel = relations
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.subject.id() == &person.base.id && row.rel_type == "works_at")
        .unwrap();
    assert!(original_rel.evidence.is_empty());
    let original_support = publications
        .support_for_record(SPACE, &original_rel.base.id)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.source_id == first.source.base.id)
        .unwrap();
    assert_eq!(
        original_support.last_commit_id.as_deref(),
        Some(first_receipt.commit_id.as_str())
    );
    assert!(canonical
        .accepted_contact_rel_support(SPACE, &original_rel, &first.source.base.id)
        .await
        .unwrap());

    let mut other_input = input_source();
    other_input.external_id = Some("independently-accepted-same-contact-fact".into());
    if distinct_fields {
        other_input.metadata["contact"]["name"] = serde_json::json!(SURVIVING_NAME);
        other_input.metadata["contact"]["profile"] = serde_json::json!(SURVIVING_PROFILE);
    }
    let second = Box::pin(accept_contact_revision(
        &storage,
        other_input.clone(),
        "shared-contact-b",
        None,
        false,
    ))
    .await;
    let second_receipt = Box::pin(finish_accepted_contact(&storage, &second)).await;
    assert_eq!(
        physical_contact_intent_count(&storage, &second.publication).await,
        1
    );
    let second_admission_digest = publications
        .accepted_contact_capability_for_publication(SPACE, &second.publication)
        .await
        .unwrap()
        .unwrap()
        .digest()
        .to_owned();
    assert_ne!(first.source.base.id, second.source.base.id);
    let shared = relations
        .get(SPACE, &original_rel.base.id)
        .await
        .unwrap()
        .unwrap();
    assert!(shared
        .base
        .provenance
        .sources
        .contains(&first.source.base.id));
    assert!(shared
        .base
        .provenance
        .sources
        .contains(&second.source.base.id));
    assert_eq!(
        physical_canonical_receipt_count(&storage, &first_receipt.commit_id).await,
        1
    );
    assert_eq!(
        physical_canonical_receipt_count(&storage, &second_receipt.commit_id).await,
        1
    );
    for receipt in [&first_receipt, &second_receipt] {
        assert!(serde_json::to_value(receipt)
            .unwrap()
            .get("_contact_lineage")
            .is_none());
    }
    for accepted in [&first, &second] {
        assert!(canonical
            .accepted_contact_rel_support(SPACE, &shared, &accepted.source.base.id)
            .await
            .unwrap());
    }

    if distinct_fields {
        let joined_person = EntityRepository::new(storage.clone())
            .get(SPACE, &person.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(joined_person.display_name, CONTACT);
        assert!(joined_person
            .aliases
            .iter()
            .any(|name| name == SURVIVING_NAME));
        assert!(joined_person
            .identity_keys
            .iter()
            .any(|key| key == ERASED_PROFILE));
        assert!(joined_person
            .identity_keys
            .iter()
            .any(|key| key == SURVIVING_PROFILE));
        assert_eq!(
            joined_person.metadata,
            serde_json::json!({"source_type":"contact_import"})
        );
        assert_eq!(
            physical_identity_value_count(&storage, ERASED_PROFILE).await,
            1
        );
        assert_eq!(
            physical_identity_value_count(&storage, SURVIVING_PROFILE).await,
            1
        );
    }

    let independently_edited = matches!(
        scenario,
        SharedContactWithdrawal::LaterSourceAfterIndependentStatus
    );
    let mut independent_audit_support_keys = Vec::new();
    if independently_edited {
        // This is the real public IndependentMutation path, not a forged
        // maintenance decision or a direct raw support-row modification.
        let mutation = Box::pin(PublicMutation::begin(storage.clone(), SPACE))
            .await
            .unwrap();
        let observed = relations
            .get(SPACE, &shared.base.id)
            .await
            .unwrap()
            .unwrap();
        let mut proposed = observed.clone();
        proposed.status = ClaimStatus::Disputed;
        let changed = Box::pin(mutation.replace(
            &observed,
            proposed.clone(),
            false,
            "independent-status-owner",
        ))
        .await
        .unwrap();
        assert_eq!(semantic_row(&changed), semantic_row(&proposed));
        let audits = elephant_storage::AuditLogRepository::new(storage.clone())
            .find_by_record(&shared.base.id)
            .await
            .unwrap()
            .into_iter()
            .filter(|audit| audit.actor == "independent-status-owner")
            .collect::<Vec<_>>();
        assert_eq!(audits.len(), 1, "the actual mutation must create its audit");
        let audit_support = publications
            .support_for_record(SPACE, &audits[0].base.id)
            .await
            .unwrap();
        assert_eq!(
            audit_support.iter().map(|support| support.source_id.clone()).collect::<BTreeSet<_>>(),
            BTreeSet::from([first.source.base.id.clone(), second.source.base.id.clone()]),
            "the audit contains both source histories, even though the status change is independent"
        );
        for support in audit_support {
            let key = private_control_key(&[
                SPACE,
                &support.source_id.to_string(),
                &support.source_revision,
                &support.record_id.to_string(),
            ]);
            assert_eq!(
                physical_control_count(&storage, "source_effect_support", &key).await,
                1
            );
            independent_audit_support_keys.push(key);
        }
        for accepted in [&first, &second] {
            assert!(
                canonical
                    .accepted_contact_rel_support(SPACE, &changed, &accepted.source.base.id)
                    .await
                    .unwrap(),
                "an actual independent status update must preserve proved factual lineage"
            );
        }
    }
    let (withdrawn, surviving) = if withdraw_original {
        (&first, &second)
    } else {
        (&second, &first)
    };
    let survivor_before = publications
        .head(SPACE, &surviving.source.base.id)
        .await
        .unwrap()
        .unwrap();
    let expected = if withdraw_original {
        let mut row = shared.clone();
        row.base
            .provenance
            .sources
            .retain(|source| source != &first.source.base.id);
        row
    } else {
        let mut row = original_rel.clone();
        if independently_edited {
            row.status = ClaimStatus::Disputed;
        }
        row
    };
    let erased = Box::pin(
        publications.erase_live(
            lifecycle_request(
                &storage,
                &withdrawn.source.base.id,
                "erase-shared-contact-contributor",
            )
            .await,
        ),
    )
    .await
    .unwrap();
    assert_eq!(erased.state, OperationState::Erased);
    for key in &independent_audit_support_keys {
        assert_eq!(
            physical_control_count(&storage, "source_effect_support", key).await,
            0,
            "physical audit deletion must remove both sources' support, including surviving A"
        );
    }
    let after = relations
        .get(SPACE, &original_rel.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(semantic_row(&after), semantic_row(&expected),
        "withdrawal must restore the independently expected surviving fact and preserve an actual independent status edit");
    assert_eq!(
        publications
            .head(SPACE, &surviving.source.base.id)
            .await
            .unwrap(),
        Some(survivor_before)
    );
    assert_eq!(physical_canonical_receipt_count(&storage, &second_receipt.commit_id).await, 0,
        "the later receipt's raw before-image must physically disappear when either contributor is erased");
    if withdraw_original {
        assert_eq!(
            physical_canonical_receipt_count(&storage, &first_receipt.commit_id).await,
            0
        );
    }
    let supports = publications
        .support_for_record(SPACE, &after.base.id)
        .await
        .unwrap();
    assert!(!supports
        .iter()
        .any(|row| row.source_id == withdrawn.source.base.id));
    let support = supports
        .iter()
        .find(|row| row.source_id == surviving.source.base.id)
        .unwrap();
    let current_commit = support
        .last_commit_id
        .as_ref()
        .expect("surviving proof requires actual transaction identity");
    assert_ne!(current_commit, &second_receipt.commit_id);
    if matches!(scenario, SharedContactWithdrawal::LaterSource) {
        assert_eq!(
            physical_canonical_receipt_count(&storage, &first_receipt.commit_id).await,
            1,
            "the original independent receipt must survive the later contributor's erasure"
        );
        assert_eq!(
            support, &original_support,
            "inverse must restore the original input ledger, not guess an equal record fingerprint"
        );
    } else {
        assert_ne!(current_commit, &first_receipt.commit_id);
        assert_eq!(
            physical_canonical_receipt_count(&storage, current_commit).await,
            1,
            "surviving lineage requires a physically stored lifecycle continuation"
        );
    }
    assert!(
        canonical
            .accepted_contact_rel_support(SPACE, &after, &surviving.source.base.id)
            .await
            .unwrap(),
        "survivor must still authorize actual contact maintenance after erased receipts disappear"
    );

    if distinct_fields {
        let survivor = EntityRepository::new(storage.clone())
            .get(SPACE, &person.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            survivor.display_name, SURVIVING_NAME,
            "A's original display name cannot shadow B's independently admitted own name"
        );
        assert!(!survivor.aliases.iter().any(|name| name == CONTACT));
        assert_eq!(
            survivor.metadata,
            serde_json::json!({}),
            "A-created contact metadata is not a B contribution"
        );
        assert_eq!(survivor.base.security, second.source.base.security);
        assert_eq!(
            survivor.base.provenance.sources,
            vec![second.source.base.id.clone()]
        );
        assert_eq!(
            survivor
                .identity_keys
                .iter()
                .cloned()
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([EMAIL.to_owned(), SURVIVING_PROFILE.to_owned()])
        );
        assert_eq!(
            physical_identity_value_count(&storage, ERASED_PROFILE).await,
            0,
            "unsupported A-only profile must be physically removed"
        );
        assert_eq!(
            physical_identity_value_count(&storage, SURVIVING_PROFILE).await,
            1
        );
        let keys = EntityIdentityKeyRepository::new(storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap();
        for value in [EMAIL, SURVIVING_PROFILE] {
            let key = keys.iter().find(|row| row.key == value).unwrap();
            assert_eq!(key.entity.id(), &person.base.id);
            assert_eq!(
                key.evidence
                    .iter()
                    .map(Link::id)
                    .cloned()
                    .collect::<Vec<_>>(),
                vec![second.source.base.id.clone()],
                "identity evidence must retain B and physically omit erased A"
            );
            assert_eq!(
                key.base.provenance.sources,
                vec![second.source.base.id.clone()]
            );
        }
    }

    if withdraw_original {
        assert_eq!(
            physical_contact_intent_count(&storage, &second.publication).await,
            0,
            "B historical prepared plan contains A before-images and must physically disappear"
        );
        let surviving_admission = publications
            .accepted_contact_capability_for_publication(SPACE, &second.publication)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(surviving_admission.digest(), second_admission_digest);
        Box::pin(surviving_contact_retries_read_only_then_reprocesses(
            &storage,
            &second,
            &other_input,
            &after.base.id,
        ))
        .await;
        assert_eq!(
            physical_canonical_receipt_count(&storage, &first_receipt.commit_id).await,
            0
        );
        assert_eq!(
            physical_canonical_receipt_count(&storage, &second_receipt.commit_id).await,
            0
        );
    }
    // The fresh B operation legitimately changed publication provenance. Capture
    // that actual proved row for the following status-only maintenance oracle.
    let after = relations.get(SPACE, &after.base.id).await.unwrap().unwrap();

    // Exercise the actual retained maintenance consumer after the inverse, with
    // a separately accepted/published replacement and its own contact proof.
    let mut replacement_input = input_source();
    replacement_input.external_id = Some("shared-contact-independent-replacement".into());
    replacement_input.metadata["contact"] = serde_json::json!({
        "name":"Replacement Rowan", "email":"rowan@example.invalid",
        "company":"Vela Research", "job_title":"Scientist"
    });
    let replacement = Box::pin(accept_contact_revision(
        &storage,
        replacement_input,
        "shared-contact-c",
        None,
        false,
    ))
    .await;
    Box::pin(finish_accepted_contact(&storage, &replacement)).await;
    let replacement_person = EntityRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.display_name == "Replacement Rowan")
        .unwrap();
    let replacement_rel = relations
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .into_iter()
        .find(|row| row.subject.id() == &replacement_person.base.id && row.rel_type == "works_at")
        .unwrap();
    let calls_before = spy.requests().unwrap().len();
    let result = Box::pin(elephant_pipeline::reprocess::supersede_rel(
        &relations,
        &after.base.id,
        &replacement_rel.base.id,
        SPACE,
    ))
    .await;
    if result.is_err() && storage.embedded_store().is_none() {
        Box::pin(native_survivor_maintenance_support_diagnostic(
            &storage,
            [&after, &replacement_rel],
        ))
        .await;
    }
    let result =
        result.expect("actual retained maintenance after source withdrawal and receipt cleanup");
    assert_eq!(result.old_id, after.base.id);
    assert_eq!(result.new_id, replacement_rel.base.id);
    let mut expected_superseded = after;
    expected_superseded.status = ClaimStatus::Superseded;
    let superseded = relations
        .get(SPACE, &expected_superseded.base.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        semantic_row(&superseded),
        semantic_row(&expected_superseded)
    );
    assert_eq!(
        relations
            .get(SPACE, &replacement_rel.base.id)
            .await
            .unwrap(),
        Some(replacement_rel)
    );
    assert_eq!(spy.requests().unwrap().len(), calls_before);
}

#[tokio::test]
async fn embedded_actual_shared_contact_erase_b_restores_original_a_pointer_and_maintenance() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::ElephantDb,
        SharedContactWithdrawal::LaterSource,
    )
    .await;
}
#[tokio::test]
async fn surreal_actual_shared_contact_erase_b_restores_original_a_pointer_and_maintenance() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::SurrealMemory,
        SharedContactWithdrawal::LaterSource,
    )
    .await;
}
#[tokio::test]
async fn embedded_actual_independent_status_survives_contact_erase_b_and_maintenance() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::ElephantDb,
        SharedContactWithdrawal::LaterSourceAfterIndependentStatus,
    )
    .await;
}
#[tokio::test]
async fn surreal_actual_independent_status_survives_contact_erase_b_and_maintenance() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::SurrealMemory,
        SharedContactWithdrawal::LaterSourceAfterIndependentStatus,
    )
    .await;
}
#[tokio::test]
async fn embedded_actual_independently_admitted_b_survives_contact_erase_a_and_maintenance() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::ElephantDb,
        SharedContactWithdrawal::OriginalSource,
    )
    .await;
}
#[tokio::test]
async fn surreal_actual_independently_admitted_b_survives_contact_erase_a_and_maintenance() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::SurrealMemory,
        SharedContactWithdrawal::OriginalSource,
    )
    .await;
}

#[tokio::test]
async fn embedded_actual_surviving_contact_owns_distinct_name_profile_and_evidence() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::ElephantDb,
        SharedContactWithdrawal::OriginalSourceDistinctFields,
    )
    .await;
}

#[tokio::test]
async fn surreal_actual_surviving_contact_owns_distinct_name_profile_and_evidence() {
    shared_contact_survives_source_withdrawal(
        provider_spy::Backend::SurrealMemory,
        SharedContactWithdrawal::OriginalSourceDistinctFields,
    )
    .await;
}
