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

//! S5.19 — Truth adjudication end-to-end.
//!
//! Two sources agree about Alice Andersson's age and disagree about her
//! employer:
//! - Doc A: "Alice Andersson works at King. She has worked there since 2020.
//!   Alice is 35 years old."
//! - Doc B: "Alice Andersson, 35, left King and joined Blizzard Entertainment
//!   in 2024."
//!
//! Verified behavior (docs/research/tm-llm-adjudication-design.md D2-D5):
//! - The age slot corroborates: one live Approved claim, evidence merged from
//!   both docs, the duplicate soft-deleted, no closed validity window.
//! - The works_at predicate opens a rel_set conflict (two distinct objects,
//!   no registered semantics) that the adjudicator resolves: Blizzard current,
//!   King superseded with a verdict-supplied 2024 window end — verdict dates
//!   beat ingest timestamps.
//! - The verdict's proposed semantics persist as `SemanticsStatus::LlmProposed`.
//! - The conflict group memoizes `adjudicated_claim_set_hash` and the audit
//!   trail carries the adjudicator rationale.
//!
//! Two tiers:
//! - `scenario_s5_19_truth_adjudication_deterministic` (CI-safe): claims are
//!   seeded through real inline promotion with fixture assertions and the TM
//!   phase runs with a scripted `FakeAdjudicator`. No env, no network.
//! - `scenario_s5_19_truth_adjudication_real_llm` (`#[ignore]`): real document
//!   ingestion, `AgenticExtractor` extraction, and a real
//!   `AgenticSlotAdjudicator`. Requires `ANTHROPIC_API_KEY`.

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Utc};
use serde_json::Value;

use elephant_mcp::{
    authz::AuthzService as McpAuthzService, create_dispatcher, ToolContext, ToolResponse,
};
use elephant_pipeline::error::PipelineError;
use elephant_pipeline::extraction::{
    AgenticExtractor, AgenticExtractorConfig, AnthropicLlmAdapter, ExtractionContext,
    ExtractionResult, Extractor, RawAssertion,
};
use elephant_pipeline::renderer::default::PassthroughRenderer;
use elephant_pipeline::renderer::RendererRegistry;
use elephant_pipeline::scheduler::extraction_work_kinds::ExtractionArtifactData;
use elephant_pipeline::scheduler::{
    execute_build_doc_tree, execute_extract_assertions, execute_plan_extraction,
    promote_assertions_inline, work_kinds, DocTreeConfig, ExtractionPlanConfig,
};
use elephant_pipeline::truth_maintenance::adjudicator::{
    AdjudicationConfig, AdjudicationDecision, AdjudicationInput, AdjudicationVerdict,
    AgenticSlotAdjudicator, ClaimValue, ProposedSemantics, SlotAdjudicator, SupersededClaim,
};
use elephant_pipeline::truth_maintenance::{
    execute_tm_commit, execute_tm_detect_conflicts_from_outbox_events,
    resolve_conflict_group_with_storage, DetectConflictsResult, RepositoryTruthMaintenanceStorage,
    TmContext,
};
use elephant_policy::{Clearance, Principal};
use elephant_storage::{
    AttributeRepository, AuditLogRepository, ConflictGroupRepository, DocNodeRepository,
    DocRepository, DocRevisionRepository, EntityRepository, PredicateRegistryRepository,
    RelRepository, Repository, SpaceConfigRepository, StorageClient, SystemEventRepository,
    TruthSlotRepository, WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::attribute::{Attribute, Literal};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::predicate::{Cardinality, SemanticsStatus};
use elephant_types::records::rel::{ClaimStatus, Rel};
use elephant_types::records::space::SpaceConfig;
use elephant_types::records::temporal::TemporalPersonality;
use elephant_types::records::truth::{ConflictGroup, ConflictPolicy};
use elephant_types::records::work::{
    Determinism, PluginManifest, RetryPolicy, WorkItem, WorkItemStatus, WorkRun, WorkRunMetrics,
};
use elephant_types::records::Entity;
use elephant_types::{Link, RecordId};

use meerkat::{AgentLlmClient, AgentToolDispatcher, ToolDef};
use meerkat_core::error::ToolError;
use meerkat_core::ops::ToolDispatchOutcome;
use meerkat_core::types::{ToolCallView, ToolResult};

use crate::Harness;

const SPACE_ID: &str = "default";
const ACTOR: &str = "scenario";

const DOC_A_TEXT: &str =
    "Alice Andersson works at King. She has worked there since 2020. Alice is 35 years old.";
const DOC_B_TEXT: &str =
    "Alice Andersson, 35, left King and joined Blizzard Entertainment in 2024.";

const FAKE_RATIONALE: &str =
    "Alice Andersson left King and joined Blizzard Entertainment in 2024; \
     the King employment ended that year.";

// ─────────────────────────────────────────────────────────────────────────────
// Scripted adjudicator (deterministic tier)
// ─────────────────────────────────────────────────────────────────────────────

/// Scripted `SlotAdjudicator` returning a Supersede verdict: the claim whose
/// object display mentions Blizzard wins, every other claim closes at a
/// verdict-supplied 2024 date. Proposes One/StateOverTime/SupersedeByRecency
/// semantics because the predicate has none registered.
#[derive(Debug, Default)]
struct FakeAdjudicator {
    calls: AtomicUsize,
    inputs: Mutex<Vec<AdjudicationInput>>,
}

#[async_trait]
impl SlotAdjudicator for FakeAdjudicator {
    async fn adjudicate(
        &self,
        input: &AdjudicationInput,
    ) -> Result<Option<AdjudicationVerdict>, PipelineError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inputs
            .lock()
            .expect("fake adjudicator inputs lock")
            .push(input.clone());

        let winner = input
            .claims
            .iter()
            .find(|claim| match &claim.value {
                ClaimValue::EntityRef { display, .. } => display.contains("Blizzard"),
                _ => false,
            })
            .ok_or_else(|| {
                PipelineError::InvalidInput(format!(
                    "FakeAdjudicator expected a Blizzard claim, got: {:?}",
                    input.claims
                ))
            })?;

        let ended = Utc
            .with_ymd_and_hms(2024, 3, 15, 0, 0, 0)
            .single()
            .expect("valid verdict date");
        let superseded = input
            .claims
            .iter()
            .filter(|claim| claim.claim_id != winner.claim_id)
            .map(|claim| SupersededClaim {
                claim_id: claim.claim_id.clone(),
                valid_until: Some(ended),
            })
            .collect();

        Ok(Some(AdjudicationVerdict {
            decision: AdjudicationDecision::Supersede,
            winner_id: Some(winner.claim_id.clone()),
            equivalent_groups: Vec::new(),
            superseded,
            winner_valid_from: Some(ended),
            proposed_semantics: Some(ProposedSemantics {
                cardinality: Cardinality::One,
                temporal_model: TemporalPersonality::StateOverTime,
                conflict_policy: ConflictPolicy::SupersedeByRecency,
            }),
            rationale: FAKE_RATIONALE.to_string(),
        }))
    }

    fn id(&self) -> &'static str {
        "fake_s5_19"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared TM phase: outbox events -> detect -> direct commits -> groups
// ─────────────────────────────────────────────────────────────────────────────

struct TmPhaseOutcome {
    detect: DetectConflictsResult,
    resolved_group_ids: Vec<RecordId>,
}

/// Drives the explicit truth-maintenance phase exactly like the pipeline
/// worker: read system events, detect, apply direct commits, then resolve and
/// commit every conflict group with the injected `TmContext`.
async fn run_truth_maintenance_phase(
    client: &StorageClient,
    ctx: &TmContext,
) -> anyhow::Result<TmPhaseOutcome> {
    let event_repo = SystemEventRepository::new(client.clone());
    let outbox_events = event_repo.get_after_seq(SPACE_ID, 0, 500).await?;
    anyhow::ensure!(
        !outbox_events.is_empty(),
        "inline promotion should have written outbox events for truth maintenance"
    );

    let tm_storage = RepositoryTruthMaintenanceStorage::new(client.clone());
    let detect =
        execute_tm_detect_conflicts_from_outbox_events(&outbox_events, &tm_storage).await?;
    eprintln!(
        "[s5_19] TM detect: events={} conflicts={} direct_commits={} groups={}",
        detect.events_processed,
        detect.conflicts_detected,
        detect.direct_commits.len(),
        detect.conflict_group_ids.len()
    );

    for commit in &detect.direct_commits {
        execute_tm_commit(&commit.resolution, &commit.input, SPACE_ID, &tm_storage).await?;
    }

    let conflict_repo = ConflictGroupRepository::new(client.clone());
    let mut seen: HashSet<String> = HashSet::new();
    let mut resolved_group_ids = Vec::new();
    for group_id in &detect.conflict_group_ids {
        if !seen.insert(group_id.to_string()) {
            continue;
        }
        let Some(group) = conflict_repo.get(SPACE_ID, group_id).await? else {
            continue;
        };
        let (resolution, input) =
            resolve_conflict_group_with_storage(ctx, &group, SPACE_ID, &tm_storage).await?;
        eprintln!(
            "[s5_19] resolved group {} ({}): {resolution:?}",
            group_id, group.slot_type
        );
        execute_tm_commit(&resolution, &input, SPACE_ID, &tm_storage).await?;
        resolved_group_ids.push(group_id.clone());
    }

    Ok(TmPhaseOutcome {
        detect,
        resolved_group_ids,
    })
}

// ─────────────────────────────────────────────────────────────────────────────
// Diagnostics and lookups
// ─────────────────────────────────────────────────────────────────────────────

async fn find_entity_by_name(
    client: &StorageClient,
    needle: &str,
) -> anyhow::Result<Option<Entity>> {
    let entities = EntityRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?;
    let needle = needle.to_lowercase();
    Ok(entities
        .into_iter()
        .find(|entity| entity.display_name.to_lowercase().contains(&needle)))
}

async fn dump_graph_state(client: &StorageClient) -> anyhow::Result<()> {
    let entities = EntityRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?;
    eprintln!("[s5_19] entities ({}):", entities.len());
    for entity in &entities {
        eprintln!(
            "[s5_19]   {} '{}' (type={})",
            entity.base.id, entity.display_name, entity.entity_type
        );
    }

    let rels = RelRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?;
    eprintln!("[s5_19] rels ({}):", rels.len());
    for rel in &rels {
        eprintln!(
            "[s5_19]   {} {} --[{}]--> {} status={:?} window={:?}",
            rel.base.id,
            rel.subject.id(),
            rel.rel_type,
            rel.object.id(),
            rel.status,
            rel.validity_window
        );
    }

    let attrs = AttributeRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?;
    eprintln!("[s5_19] attributes ({}):", attrs.len());
    for attr in &attrs {
        eprintln!(
            "[s5_19]   {} {} .{} = {:?} status={:?} evidence={} window={:?}",
            attr.base.id,
            attr.subject.id(),
            attr.attr_type,
            attr.value,
            attr.status,
            attr.evidence.len(),
            attr.validity_window
        );
    }
    Ok(())
}

fn distinct_evidence_revisions(attr: &Attribute) -> usize {
    attr.evidence
        .iter()
        .map(|evidence| evidence.span.revision.id().to_string())
        .collect::<HashSet<_>>()
        .len()
}

/// Matches any literal encoding of the age value 35 (the real extractor
/// chooses the literal type; the deterministic tier seeds `Integer(35)`).
fn is_age_35(value: &Literal) -> bool {
    match value {
        Literal::Integer(v) => *v == 35,
        Literal::Float(v) => (*v - 35.0).abs() < f64::EPSILON,
        Literal::String(v) => v.trim() == "35",
        _ => false,
    }
}

/// Asserts the corroborated age slot: exactly one live Approved claim carrying
/// merged evidence from both revisions, no Disputed sibling, no closed window.
async fn assert_age_corroborated(
    client: &StorageClient,
    alice_id: &RecordId,
    min_distinct_revisions: usize,
) -> anyhow::Result<Attribute> {
    let attrs = AttributeRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?;
    let age_attrs: Vec<&Attribute> = attrs
        .iter()
        .filter(|attr| attr.subject.id() == alice_id)
        .filter(|attr| is_age_35(&attr.value))
        .collect();

    assert_eq!(
        age_attrs.len(),
        1,
        "expected exactly one live age=35 claim after corroboration, got {age_attrs:#?}"
    );
    let winner = age_attrs[0];
    assert_eq!(
        winner.status,
        ClaimStatus::Approved,
        "corroborated age winner must be Approved: {winner:#?}"
    );
    assert!(
        winner.evidence.len() >= 2,
        "corroborated age winner must carry merged evidence from both docs: {winner:#?}"
    );
    assert!(
        distinct_evidence_revisions(winner) >= min_distinct_revisions,
        "merged age evidence must span {min_distinct_revisions} distinct revisions: {winner:#?}"
    );
    assert!(
        winner
            .validity_window
            .as_ref()
            .and_then(|w| w.end)
            .is_none(),
        "a corroborating duplicate never stopped being true; no closed window: {winner:#?}"
    );
    assert!(
        !attrs
            .iter()
            .filter(|attr| attr.subject.id() == alice_id)
            .any(|attr| attr.status == ClaimStatus::Disputed),
        "no attribute claim on the subject may be Disputed: {attrs:#?}"
    );

    // The truth slot mirrors the single corroborated value.
    let slots = TruthSlotRepository::new(client.clone())
        .find_by_subject_entity(SPACE_ID, &alice_id.to_string())
        .await?;
    let age_slot = slots
        .iter()
        .find(|slot| slot.slot_type == "attribute" && slot.predicate_id == winner.attr_type);
    if let Some(slot) = age_slot {
        assert_eq!(
            slot.current_values.len(),
            1,
            "age truth slot must hold exactly the corroborated winner: {slot:#?}"
        );
        assert_eq!(
            slot.current_values[0].status,
            ClaimStatus::Approved,
            "age truth slot current value must be Approved: {slot:#?}"
        );
    } else {
        anyhow::bail!(
            "no truth slot found for subject {alice_id} predicate '{}'; slots: {slots:#?}",
            winner.attr_type
        );
    }

    Ok(winner.clone())
}

/// Asserts the adjudicated employer supersession: Blizzard current, King
/// superseded with a validity window ending in 2024 (the verdict date, not the
/// ingest year).
async fn assert_works_at_adjudicated(
    client: &StorageClient,
    alice_id: &RecordId,
    king_id: &RecordId,
    blizzard_id: &RecordId,
) -> anyhow::Result<(Rel, Rel)> {
    let rels = RelRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?;
    let alice_rels: Vec<&Rel> = rels
        .iter()
        .filter(|rel| rel.subject.id() == alice_id)
        .collect();

    // The adjudicated slot pair shares one predicate with two objects; anchor
    // on the Blizzard rel's rel_type so unrelated predicates the extractor may
    // also emit (e.g. a `previously_worked_at` King rel) don't shadow the pair.
    let blizzard_rel = alice_rels
        .iter()
        .find(|rel| rel.object.id() == blizzard_id)
        .unwrap_or_else(|| panic!("no Alice->Blizzard rel found; rels: {alice_rels:#?}"));
    let king_rel = alice_rels
        .iter()
        .find(|rel| rel.object.id() == king_id && rel.rel_type == blizzard_rel.rel_type)
        .unwrap_or_else(|| {
            panic!(
                "no Alice->King rel with predicate '{}' found; rels: {alice_rels:#?}",
                blizzard_rel.rel_type
            )
        });

    assert_eq!(
        blizzard_rel.status,
        ClaimStatus::Approved,
        "Blizzard rel must be the current Approved employer: {blizzard_rel:#?}"
    );
    assert!(
        blizzard_rel
            .validity_window
            .as_ref()
            .and_then(|w| w.end)
            .is_none(),
        "Blizzard rel must have an open validity window: {blizzard_rel:#?}"
    );

    assert_eq!(
        king_rel.status,
        ClaimStatus::Superseded,
        "King rel must be Superseded after adjudication: {king_rel:#?}"
    );
    let king_end = king_rel
        .validity_window
        .as_ref()
        .and_then(|w| w.end)
        .unwrap_or_else(|| panic!("King rel must have a closed validity window: {king_rel:#?}"));
    assert_eq!(
        king_end.year(),
        2024,
        "King validity must end at the verdict date (2024), not the ingest \
         timestamp; got {king_end} on {king_rel:#?}"
    );

    Ok(((*king_rel).clone(), (*blizzard_rel).clone()))
}

/// Asserts the conflict group memoized the adjudication and that the audit
/// trail carries the adjudicator rationale.
async fn assert_group_adjudicated(
    client: &StorageClient,
    group_id: &RecordId,
    rationale_needle: &str,
) -> anyhow::Result<ConflictGroup> {
    let group = ConflictGroupRepository::new(client.clone())
        .get(SPACE_ID, group_id)
        .await?
        .unwrap_or_else(|| panic!("conflict group {group_id} not found"));

    assert!(
        group.resolved,
        "conflict group must be resolved: {group:#?}"
    );
    assert!(
        group.adjudicated_claim_set_hash.is_some(),
        "adjudicated group must memoize the claim-set hash: {group:#?}"
    );
    assert!(
        group.adjudicated_at.is_some(),
        "adjudicated group must record adjudicated_at: {group:#?}"
    );
    let rationale = group
        .resolution_rationale
        .clone()
        .unwrap_or_else(|| panic!("resolved group must carry a rationale: {group:#?}"));
    assert!(
        rationale.contains(rationale_needle),
        "group rationale must come from the adjudicator verdict; \
         expected substring '{rationale_needle}', got '{rationale}'"
    );

    let audit_entries = AuditLogRepository::new(client.clone())
        .find_by_record(group_id)
        .await?;
    assert!(
        !audit_entries.is_empty(),
        "conflict group resolution must be audit-logged"
    );
    assert!(
        audit_entries.iter().any(|entry| {
            entry
                .after
                .as_ref()
                .map(ToString::to_string)
                .is_some_and(|after| after.contains(rationale_needle))
        }),
        "audit log for the group must carry the adjudicator rationale; entries: {audit_entries:#?}"
    );

    Ok(group)
}

/// Asserts the predicate registry recorded the LLM-proposed semantics.
async fn assert_semantics_llm_proposed(
    client: &StorageClient,
    predicate_id: &str,
    expected_cardinality: Option<Cardinality>,
) -> anyhow::Result<()> {
    let registry = PredicateRegistryRepository::new(client.clone())
        .find_by_predicate_id(SPACE_ID, predicate_id)
        .await?
        .unwrap_or_else(|| panic!("no predicate registry entry for '{predicate_id}'"));

    assert_eq!(
        registry.semantics_status,
        SemanticsStatus::LlmProposed,
        "adjudicator-proposed semantics must persist as LlmProposed: {registry:#?}"
    );
    let semantics = registry.semantics.as_ref().unwrap_or_else(|| {
        panic!("LlmProposed registry entry must carry semantics: {registry:#?}")
    });
    eprintln!(
        "[s5_19] learned semantics for '{predicate_id}': cardinality={} temporal_model={} policy={}",
        semantics.cardinality, semantics.temporal_model, semantics.conflict_policy
    );
    if let Some(expected) = expected_cardinality {
        assert_eq!(
            semantics.cardinality, expected,
            "proposed cardinality must be recorded: {registry:#?}"
        );
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Deterministic tier: fixture promotion + scripted adjudicator
// ─────────────────────────────────────────────────────────────────────────────

async fn create_revision_with_node(
    client: &StorageClient,
    title: &str,
    text: &str,
) -> anyhow::Result<(RecordId, RecordId)> {
    let doc_repo = DocRepository::new(client.clone());
    let revision_repo = DocRevisionRepository::new(client.clone());
    let node_repo = DocNodeRepository::new(client.clone());

    let doc_id = RecordId::new_v4(TableName::Doc);
    let doc = Doc {
        base: BaseFields::new(doc_id.clone(), SPACE_ID.to_string(), ACTOR.to_string()),
        title: title.to_string(),
        text: text.to_string(),
        sidecar_text: None,
        source: None,
        media_attachments: Vec::new(),
        rendition_key: format!("s5-19-{}", uuid::Uuid::new_v4()),
        metadata: serde_json::json!({}),
    };
    doc_repo.create(doc).await?;

    let revision_id = RecordId::new_v4(TableName::DocRevision);
    let revision = DocRevision {
        base: BaseFields::new(revision_id.clone(), SPACE_ID.to_string(), ACTOR.to_string()),
        doc: Link::new(doc_id),
        text: text.to_string(),
        text_hash: format!("s5-19-hash-{}", uuid::Uuid::new_v4()),
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
        base: BaseFields::new(node_id.clone(), SPACE_ID.to_string(), ACTOR.to_string()),
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

async fn create_work_run(client: &StorageClient) -> anyhow::Result<RecordId> {
    let item_repo = WorkItemRepository::new(client.clone());
    let run_repo = WorkRunRepository::new(client.clone());

    let item_id = RecordId::new_v4(TableName::WorkItem);
    let item = WorkItem {
        base: BaseFields::new(item_id.clone(), SPACE_ID.to_string(), ACTOR.to_string()),
        kind: "extract_assertions".to_string(),
        payload: serde_json::json!({}),
        status: WorkItemStatus::Runnable,
        priority: 5,
        idempotency_key: format!("s5-19:{}", uuid::Uuid::new_v4()),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    item_repo.create(item).await?;

    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let run = WorkRun {
        base: BaseFields::new(work_run_id.clone(), SPACE_ID.to_string(), ACTOR.to_string()),
        work_item: Link::new(item_id),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: None,
        success: false,
        error: None,
        metrics: WorkRunMetrics::default(),
        worker_id: "s5-19-worker".to_string(),
        worker_version: "1.0.0".to_string(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    run_repo.create(run).await?;
    Ok(work_run_id)
}

fn fixture_assertion(
    revision_id: &RecordId,
    subject: &str,
    predicate: &str,
    object_text: Option<&str>,
    object_literal: Option<Literal>,
    confidence: f64,
    span: (u32, u32),
) -> RawAssertion {
    let object_entity_type = object_text.map(|_| "organization".to_string());
    RawAssertion {
        subject_text: subject.to_string(),
        predicate_text: predicate.to_string(),
        object_text: object_text.map(ToString::to_string),
        object_literal,
        evidence_span: EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: span.0,
            end: span.1,
            quote_hash: format!("s5-19-{}-{}", predicate, span.0),
        },
        confidence,
        quote: None,
        source_event_time: None,
        source_event_time_precision: None,
        event_title: None,
        event_lineage_key: None,
        subject_entity_id: None,
        subject_entity_type: Some("person".to_string()),
        object_entity_id: None,
        object_entity_type,
        significance: Some("high".to_string()),
    }
}

async fn promote_fixture_doc(
    client: &StorageClient,
    title: &str,
    text: &str,
    assertions: &[RawAssertion],
) -> anyhow::Result<RecordId> {
    let (revision_id, node_id) = create_revision_with_node(client, title, text).await?;
    let work_run_id = create_work_run(client).await?;

    // Rebind the fixture evidence spans onto the freshly created revision.
    let assertions: Vec<RawAssertion> = assertions
        .iter()
        .cloned()
        .map(|mut assertion| {
            assertion.evidence_span.revision = Link::new(revision_id.clone());
            assertion
        })
        .collect();

    promote_assertions_inline(
        client,
        SPACE_ID,
        &assertions,
        &work_run_id,
        &node_id,
        &revision_id,
        ACTOR,
    )
    .await?;
    Ok(revision_id)
}

/// Deterministic sibling of the real-LLM scenario: identical two-doc claim
/// structure seeded through real inline promotion, identical explicit TM phase,
/// with a scripted adjudicator verdict instead of a live model.
#[tokio::test]
async fn scenario_s5_19_truth_adjudication_deterministic() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let client = &harness.client;

    // Doc A: works_at King + age 35.
    let placeholder = RecordId::new_v4(TableName::DocRevision);
    promote_fixture_doc(
        client,
        "HR profile 2020",
        DOC_A_TEXT,
        &[
            fixture_assertion(
                &placeholder,
                "Alice Andersson",
                "works_at",
                Some("King"),
                None,
                0.9,
                (0, 30),
            ),
            fixture_assertion(
                &placeholder,
                "Alice Andersson",
                "age",
                None,
                Some(Literal::Integer(35)),
                0.9,
                (62, 88),
            ),
        ],
    )
    .await?;

    // Doc B: works_at Blizzard Entertainment + age 35.
    promote_fixture_doc(
        client,
        "HR update 2024",
        DOC_B_TEXT,
        &[
            fixture_assertion(
                &placeholder,
                "Alice Andersson",
                "works_at",
                Some("Blizzard Entertainment"),
                None,
                0.88,
                (0, 74),
            ),
            fixture_assertion(
                &placeholder,
                "Alice Andersson",
                "age",
                None,
                Some(Literal::Integer(35)),
                0.85,
                (0, 20),
            ),
        ],
    )
    .await?;

    let alice = find_entity_by_name(client, "alice andersson")
        .await?
        .expect("promotion must create the Alice entity");
    let king = find_entity_by_name(client, "king")
        .await?
        .expect("promotion must create the King entity");
    let blizzard = find_entity_by_name(client, "blizzard")
        .await?
        .expect("promotion must create the Blizzard entity");

    // Capture the pre-TM age claims so the soft-deleted duplicate is provable.
    let age_ids_before: Vec<RecordId> = AttributeRepository::new(client.clone())
        .list(SPACE_ID, 1000, None)
        .await?
        .into_iter()
        .filter(|attr| attr.subject.id() == &alice.base.id)
        .filter(|attr| is_age_35(&attr.value))
        .map(|attr| attr.base.id)
        .collect();
    assert_eq!(
        age_ids_before.len(),
        2,
        "both docs must contribute an age claim before truth maintenance"
    );

    // Explicit TM phase with the scripted adjudicator.
    let fake = Arc::new(FakeAdjudicator::default());
    let ctx = TmContext {
        adjudicator: Some(fake.clone()),
    };
    let outcome = run_truth_maintenance_phase(client, &ctx).await?;

    dump_graph_state(client).await?;

    // (a) Age corroborates without the adjudicator: winner Approved, evidence
    // merged, duplicate soft-deleted, no windows, no conflict group.
    assert_age_corroborated(client, &alice.base.id, 2).await?;
    let attr_repo = AttributeRepository::new(client.clone());
    let mut deleted_dupes = 0;
    for id in &age_ids_before {
        if attr_repo.get(SPACE_ID, id).await?.is_none() {
            let deleted = attr_repo
                .get_with_deleted(SPACE_ID, id)
                .await?
                .unwrap_or_else(|| panic!("merged duplicate {id} must still exist as a row"));
            assert!(
                deleted.base.deleted_at.is_some(),
                "corroborated duplicate must be soft-deleted, not erased: {deleted:#?}"
            );
            assert!(
                deleted
                    .validity_window
                    .as_ref()
                    .and_then(|w| w.end)
                    .is_none(),
                "corroborated duplicate must not get a supersession window: {deleted:#?}"
            );
            deleted_dupes += 1;
        }
    }
    assert_eq!(
        deleted_dupes, 1,
        "exactly one of the two identical age claims merges away"
    );

    // (b) The adjudicated verdict wins the works_at rel_set conflict; verdict
    // dates beat ingest timestamps (2024 window end, ingested today).
    assert_works_at_adjudicated(client, &alice.base.id, &king.base.id, &blizzard.base.id).await?;

    // Exactly one adjudication: the rel_set group. Corroboration never calls
    // the adjudicator.
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "only the works_at value conflict may reach the adjudicator"
    );
    {
        let inputs = fake.inputs.lock().expect("fake adjudicator inputs lock");
        assert_eq!(inputs[0].slot_type, "rel_set", "input: {:#?}", inputs[0]);
        assert_eq!(
            inputs[0].predicate_id, "works_at",
            "input: {:#?}",
            inputs[0]
        );
        assert!(
            inputs[0].semantics.is_none(),
            "works_at has no registered semantics before adjudication: {:#?}",
            inputs[0]
        );
    }

    // (d) The conflict group is resolved with memoized claim-set hash and the
    // rationale is on the audit trail.
    assert_eq!(
        outcome.resolved_group_ids.len(),
        1,
        "exactly one conflict group (works_at rel_set) expected; detect: {:?}",
        outcome.detect.conflict_group_ids
    );
    assert_group_adjudicated(client, &outcome.resolved_group_ids[0], FAKE_RATIONALE).await?;
    assert!(
        ConflictGroupRepository::new(client.clone())
            .find_all_unresolved(SPACE_ID, 10)
            .await?
            .is_empty(),
        "no unresolved conflict groups may remain"
    );

    // (c) The scripted verdict's proposed semantics persist as LlmProposed.
    assert_semantics_llm_proposed(client, "works_at", Some(Cardinality::One)).await?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Real-LLM tier: in-process agentic extraction + real adjudicator
// ─────────────────────────────────────────────────────────────────────────────

fn anthropic_key_present() -> bool {
    std::env::var("ANTHROPIC_API_KEY").is_ok_and(|key| !key.trim().is_empty())
}

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

fn set_env_var_scoped(key: &'static str, value: &str) -> EnvVarGuard {
    let previous = std::env::var_os(key);
    std::env::set_var(key, value);
    EnvVarGuard { key, previous }
}

fn test_principal() -> Principal {
    let mut scopes = HashSet::new();
    scopes.insert("*".to_string());

    Principal {
        space_id: SPACE_ID.to_string(),
        scopes,
        clearances: vec![Clearance {
            policy_id: "default".to_string(),
            max_level: i32::MAX,
            labels: HashSet::new(),
            handling_allow: HashSet::new(),
        }],
        subject_allowlist: None,
        purpose: HashSet::new(),
    }
}

fn employment_space_config() -> SpaceConfig {
    let base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        SPACE_ID.to_string(),
        ACTOR.to_string(),
    );
    SpaceConfig {
        base,
        embedding_provider: "fake".to_string(),
        embedding_model: "test-model".to_string(),
        embedding_dims: 8,
        purpose: Some(
            "Employment history and personal facts about people — who works where, since when, \
             and basic biographical attributes. Use canonical snake_case predicates and reuse \
             the same predicate for the same kind of fact: employment is `works_at`, a \
             person's age is the numeric `age` attribute."
                .to_string(),
        ),
        skill: None,
    }
}

/// Bridge: `elephant_mcp::ToolDispatcher` -> `meerkat::AgentToolDispatcher`,
/// so the extraction agent calls Elephant tools in-process (same pattern as
/// s5_13).
struct ElephantToolBridge {
    dispatcher: elephant_mcp::ToolDispatcher,
    tool_defs: Arc<[Arc<ToolDef>]>,
}

impl ElephantToolBridge {
    fn new(dispatcher: elephant_mcp::ToolDispatcher) -> Self {
        let tool_defs: Arc<[Arc<ToolDef>]> = dispatcher
            .all_tool_meta()
            .into_iter()
            .map(|meta| Arc::new(ToolDef::new(meta.name, meta.description, meta.input_schema)))
            .collect();
        Self {
            dispatcher,
            tool_defs,
        }
    }
}

#[async_trait]
impl AgentToolDispatcher for ElephantToolBridge {
    fn tools(&self) -> Arc<[Arc<ToolDef>]> {
        self.tool_defs.clone()
    }

    async fn dispatch(&self, call: ToolCallView<'_>) -> Result<ToolDispatchOutcome, ToolError> {
        let args: Value =
            serde_json::from_str(call.args.get()).unwrap_or(Value::Object(Default::default()));

        match self.dispatcher.execute(call.name, args).await {
            Ok(result) => {
                let content = serde_json::to_string(&result).unwrap_or_default();
                eprintln!("[s5_19] tool {} ok ({} bytes)", call.name, content.len());
                Ok(ToolDispatchOutcome::from(ToolResult::new(
                    call.id.to_string(),
                    content,
                    false,
                )))
            }
            Err(e) => {
                eprintln!("[s5_19] tool {} ERROR: {e}", call.name);
                Ok(ToolDispatchOutcome::from(ToolResult::new(
                    call.id.to_string(),
                    format!("Tool error: {e}"),
                    true,
                )))
            }
        }
    }
}

struct InProcessAgenticExtractor {
    inner: AgenticExtractor,
    tool_bridge: Arc<ElephantToolBridge>,
    llm: Arc<dyn AgentLlmClient>,
}

#[async_trait]
impl Extractor for InProcessAgenticExtractor {
    fn manifest(&self) -> &PluginManifest {
        self.inner.manifest()
    }

    async fn extract(&self, ctx: ExtractionContext) -> Result<ExtractionResult, PipelineError> {
        self.inner
            .run_agent(ctx, self.tool_bridge.clone(), self.llm.clone())
            .await
    }
}

fn create_in_process_extractor(storage: &StorageClient) -> InProcessAgenticExtractor {
    let model =
        std::env::var("EXTRACTION_MODEL").unwrap_or_else(|_| "claude-sonnet-4-5".to_string());
    eprintln!("[s5_19] extraction model: {model}");

    let authz = Arc::new(McpAuthzService::new_with_skip_auth(true));
    let context = ToolContext::with_authz(storage.clone(), authz, test_principal());
    let dispatcher = elephant_mcp::create_agent_dispatcher_with_context(context);
    let tool_bridge = Arc::new(ElephantToolBridge::new(dispatcher));

    let api_key = std::env::var("ANTHROPIC_API_KEY").unwrap_or_else(|_| "unused".to_string());
    let llm: Arc<dyn AgentLlmClient> = Arc::new(
        AnthropicLlmAdapter::new(api_key, model.clone()).expect("Failed to create LLM adapter"),
    );

    let config =
        AgenticExtractorConfig::new("unused://in-process", model).with_connect_on_init(false);

    InProcessAgenticExtractor {
        inner: AgenticExtractor::new(config),
        tool_bridge,
        llm,
    }
}

/// Promotes a staged extraction artifact through real inline promotion,
/// mirroring the pipeline's commit_artifacts arm (same as s5_14).
async fn promote_extraction_artifact(
    storage: &StorageClient,
    artifact_id: &RecordId,
    work_run_id: &RecordId,
) -> anyhow::Result<()> {
    let artifact_repo = WorkArtifactRepository::new(storage.clone());
    let artifact = artifact_repo
        .get(SPACE_ID, artifact_id)
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
        SPACE_ID,
        &assertions,
        work_run_id,
        &node_id,
        &revision_id,
        ACTOR,
    )
    .await?;
    Ok(())
}

async fn create_pipeline_work_run(
    storage: &StorageClient,
    item_id: &RecordId,
) -> anyhow::Result<RecordId> {
    let repo = WorkRunRepository::new(storage.clone());
    let work_run_id = RecordId::new_v4(TableName::WorkRun);
    let work_run = WorkRun {
        base: BaseFields::new(work_run_id.clone(), SPACE_ID.to_string(), ACTOR.to_string()),
        work_item: Link::new(item_id.clone()),
        attempt: 1,
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        success: true,
        error: None,
        metrics: WorkRunMetrics::default(),
        worker_id: "s5-19-worker".to_string(),
        worker_version: "test".to_string(),
        model_version: None,
        prompt_id: None,
        config_hash: None,
    };
    repo.create(work_run).await?;
    Ok(work_run_id)
}

/// Drives ingestion work items to quiescence: render -> plan -> extract ->
/// inline promotion. TM work kinds stay untouched; the explicit TM phase runs
/// afterwards (nothing enqueues TM items in-process).
async fn drive_ingestion_pipeline(
    storage: StorageClient,
    extractor: &InProcessAgenticExtractor,
    max_steps: usize,
) -> anyhow::Result<()> {
    let item_repo = WorkItemRepository::new(storage.clone());
    let doc_repo = DocRepository::new(storage.clone());
    let dag = elephant_pipeline::scheduler::DagExecutor::new(storage.clone());

    let mut registry = RendererRegistry::new();
    registry.register(Box::new(PassthroughRenderer::new()));

    for step in 0..max_steps {
        let runnable = item_repo
            .find_by_status(SPACE_ID, WorkItemStatus::Runnable)
            .await?;
        if runnable.is_empty() {
            eprintln!("[s5_19] pipeline quiesced after {step} steps");
            return Ok(());
        }

        for item in runnable {
            let item_id = item.base.id.clone();
            let _ = item_repo
                .update_status(SPACE_ID, &item_id, WorkItemStatus::Running)
                .await;
            let work_run_id = create_pipeline_work_run(&storage, &item_id).await?;
            eprintln!("[s5_19] step={step} kind={} id={}", item.kind, item_id);

            match item.kind.as_str() {
                work_kinds::RENDER_DOC_KIND => {
                    let doc_id = item
                        .payload
                        .get("doc_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("render_doc missing doc_id payload"))?;
                    let doc_id = RecordId::from_str(doc_id).map_err(|e| anyhow::anyhow!("{e}"))?;
                    let doc = doc_repo
                        .get(SPACE_ID, &doc_id)
                        .await?
                        .ok_or_else(|| anyhow::anyhow!("doc not found: {doc_id}"))?;

                    let render_result = work_kinds::execute_render_doc(
                        &storage,
                        &registry,
                        &doc,
                        Some("text/plain"),
                    )
                    .await?;
                    execute_build_doc_tree(
                        &storage,
                        &render_result.revision_id,
                        &DocTreeConfig::default(),
                    )
                    .await?;

                    let plan_item = WorkItem {
                        base: BaseFields::new(
                            RecordId::new_v4(TableName::WorkItem),
                            SPACE_ID.to_string(),
                            ACTOR.to_string(),
                        ),
                        kind: work_kinds::PLAN_EXTRACTION_KIND.to_string(),
                        payload: serde_json::json!({
                            "revision_id": render_result.revision_id.to_string(),
                            "policy_version": "default",
                            "extractor_id": "elephant.extractor.agentic",
                            "allow_raw_quotes": false,
                            "ingest_mode": "graph",
                        }),
                        status: WorkItemStatus::Runnable,
                        priority: item.priority,
                        idempotency_key: format!("plan_extraction:{}", render_result.revision_id),
                        attempts: 0,
                        lease_expires_at: None,
                        leased_by: None,
                        run_after: None,
                        last_error: None,
                    };
                    let _ = item_repo.create(plan_item).await?;
                }
                work_kinds::PLAN_EXTRACTION_KIND => {
                    let revision_id = item
                        .payload
                        .get("revision_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("plan_extraction missing revision_id"))?;
                    let revision_id = RecordId::from_str(revision_id)?;

                    let config = ExtractionPlanConfig::default();
                    let result = execute_plan_extraction(
                        &storage,
                        &revision_id,
                        "default",
                        &config,
                        &work_run_id,
                        SPACE_ID,
                        ACTOR,
                    )
                    .await?;

                    for selection in result.plan.node_selections {
                        let extract_item = WorkItem {
                            base: BaseFields::new(
                                RecordId::new_v4(TableName::WorkItem),
                                SPACE_ID.to_string(),
                                ACTOR.to_string(),
                            ),
                            kind: work_kinds::EXTRACT_ASSERTIONS_KIND.to_string(),
                            payload: serde_json::json!({
                                "node_id": selection.node_id.to_string(),
                                "revision_id": revision_id.to_string(),
                                "extractor_id": "elephant.extractor.agentic",
                                "allow_raw_quotes": false,
                                "estimated_tokens": selection.estimated_tokens,
                                "estimated_cost_euros": 0.0,
                                "ingest_mode": "graph",
                            }),
                            status: WorkItemStatus::Runnable,
                            priority: item.priority,
                            idempotency_key: format!(
                                "extract_assertions:{}:{}",
                                revision_id, selection.node_id
                            ),
                            attempts: 0,
                            lease_expires_at: None,
                            leased_by: None,
                            run_after: None,
                            last_error: None,
                        };
                        let _ = item_repo.create(extract_item).await?;
                    }
                }
                work_kinds::EXTRACT_ASSERTIONS_KIND => {
                    let node_id = item
                        .payload
                        .get("node_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("extract_assertions missing node_id"))?;
                    let revision_id = item
                        .payload
                        .get("revision_id")
                        .and_then(|v| v.as_str())
                        .ok_or_else(|| anyhow::anyhow!("extract_assertions missing revision_id"))?;
                    let node_id = RecordId::from_str(node_id)?;
                    let revision_id = RecordId::from_str(revision_id)?;

                    // One real extraction pass per doc node, then inline
                    // promotion so outbox events exist for the TM phase.
                    let result = execute_extract_assertions(
                        &storage,
                        extractor,
                        &node_id,
                        &revision_id,
                        &work_run_id,
                        SPACE_ID,
                        ACTOR,
                        None,
                    )
                    .await?;
                    eprintln!(
                        "[s5_19] extracted {} assertions (artifact={})",
                        result.assertions_extracted, result.artifact_id
                    );
                    promote_extraction_artifact(&storage, &result.artifact_id, &work_run_id)
                        .await?;
                }
                // Source-lifecycle triage arms live in the pipeline binary
                // (BudgetEnforcer, triage plugin, staged-commit executors) and
                // are not exported for tests. `ingest_doc_simple` has already
                // materialized the Doc, and the test enqueues `render_doc`
                // directly with graph mode pinned, so these are no-ops here.
                work_kinds::TRIAGE_SOURCE_KIND
                | work_kinds::INGEST_METADATA_KIND
                | work_kinds::INGEST_SOURCE_STUB_KIND
                | work_kinds::UPGRADE_INGEST_MODE_KIND
                | work_kinds::TM_DETECT_CONFLICTS_KIND
                | work_kinds::TM_RESOLVE_SLOT_KIND
                | work_kinds::TM_COMMIT_KIND
                | work_kinds::TM_RECOMPUTE_KIND
                | work_kinds::EMBED_CHUNKS_KIND
                | work_kinds::BUILD_DOC_TREE_KIND
                | work_kinds::WORK_KIND_FRESHNESS_SWEEP
                | work_kinds::experimental::validate_artifacts::KIND
                | work_kinds::experimental::commit_artifacts::KIND => {}
                other => {
                    eprintln!("[s5_19] WARNING: unhandled work kind: {other}");
                }
            }

            item_repo
                .update_status(SPACE_ID, &item_id, WorkItemStatus::Done)
                .await?;
            let _ = dag.on_item_completed(SPACE_ID, &item_id).await;
        }
    }

    anyhow::bail!("pipeline did not quiesce within {max_steps} steps")
}

#[tokio::test]
#[ignore = "real LLM extraction + adjudication; requires ANTHROPIC_API_KEY"]
async fn scenario_s5_19_truth_adjudication_real_llm() -> anyhow::Result<()> {
    if !anthropic_key_present() {
        eprintln!("skipping: ANTHROPIC_API_KEY not set");
        return Ok(());
    }

    let harness = Harness::new().await?;
    let client = &harness.client;

    SpaceConfigRepository::new(client.clone())
        .create(employment_space_config())
        .await?;

    // Real ingestion of the two sources through the MCP surface.
    // `ingest_doc_simple` persists the Doc immediately and enqueues the
    // source-first triage chain; that chain's executors live in the pipeline
    // binary, so the test pins graph mode by enqueuing `render_doc` per doc
    // directly (the triage/lifecycle items no-op in the drive loop).
    let dispatcher = create_dispatcher(client.clone(), test_principal());
    let item_repo = WorkItemRepository::new(client.clone());
    for (title, text) in [
        ("HR profile 2020", DOC_A_TEXT),
        ("HR update 2024", DOC_B_TEXT),
    ] {
        let response = dispatcher
            .execute(
                "ingest_doc_simple",
                serde_json::json!({ "title": title, "text": text }),
            )
            .await?;
        let decoded: ToolResponse<serde_json::Value> = serde_json::from_value(response)?;
        assert!(decoded.ok, "ingest_doc_simple must succeed for '{title}'");

        let doc = DocRepository::new(client.clone())
            .list(SPACE_ID, 100, None)
            .await?
            .into_iter()
            .find(|doc| doc.title == title)
            .unwrap_or_else(|| panic!("ingest_doc_simple must persist doc '{title}'"));
        let render_item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                SPACE_ID.to_string(),
                ACTOR.to_string(),
            ),
            kind: work_kinds::RENDER_DOC_KIND.to_string(),
            payload: serde_json::json!({
                "doc_id": doc.base.id.to_string(),
                "ingest_mode": "graph",
            }),
            status: WorkItemStatus::Runnable,
            priority: 5,
            idempotency_key: format!("render_doc:{}", doc.base.id),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        item_repo.create(render_item).await?;
    }

    // One extraction pass per doc, promoted inline. The default 24 tool-call
    // budget terminates a thorough model mid-browse before it can emit the
    // final structured output (the direct no-tools path is not an option: its
    // prompt itself instructs calling search_entities); give the loop room.
    let _tool_call_budget_guard = set_env_var_scoped("PIPELINE_MAX_EXTRACT_TOOL_CALLS", "64");
    let extractor = create_in_process_extractor(client);
    drive_ingestion_pipeline(client.clone(), &extractor, 60).await?;
    dump_graph_state(client).await?;

    let alice = find_entity_by_name(client, "alice")
        .await?
        .expect("extraction must produce the Alice Andersson entity");
    let king = find_entity_by_name(client, "king")
        .await?
        .expect("extraction must produce the King entity");
    let blizzard = find_entity_by_name(client, "blizzard")
        .await?
        .expect("extraction must produce the Blizzard Entertainment entity");

    // Explicit TM phase with the REAL adjudicator (one adjudication expected:
    // the works_at rel_set conflict; identical age values corroborate without
    // an LLM call).
    let mut config = AdjudicationConfig::default();
    if let Ok(model) = std::env::var("PIPELINE_TM_MODEL") {
        let trimmed = model.trim();
        if !trimmed.is_empty() {
            config.model = trimmed.to_string();
        }
    }
    eprintln!("[s5_19] adjudication model: {}", config.model);
    let ctx = TmContext {
        adjudicator: Some(Arc::new(AgenticSlotAdjudicator::new(config))),
    };
    let outcome = run_truth_maintenance_phase(client, &ctx).await?;

    dump_graph_state(client).await?;

    // (b) first — the headline proof: works_at adjudicated, Blizzard current,
    // King superseded with a 2024 window end (the verdict date, not the
    // ingest date).
    let (king_rel, blizzard_rel) =
        assert_works_at_adjudicated(client, &alice.base.id, &king.base.id, &blizzard.base.id)
            .await?;
    eprintln!(
        "[s5_19] employer supersession: {} -> {} (King window: {:?})",
        king_rel.rel_type, blizzard_rel.rel_type, king_rel.validity_window
    );

    // (a) Age corroborated: one live Approved claim with evidence from both
    // docs, no Disputed sibling, no closed window.
    assert_age_corroborated(client, &alice.base.id, 2).await?;

    // (d) The conflict group covering the works_at rels is resolved with a
    // memoized adjudication and an audited rationale.
    let conflict_repo = ConflictGroupRepository::new(client.clone());
    let king_claim_id = king_rel.base.id.to_string();
    let mut works_at_group: Option<ConflictGroup> = None;
    for id in &outcome.resolved_group_ids {
        if let Some(group) = conflict_repo.get(SPACE_ID, id).await? {
            if group
                .claim_ids
                .iter()
                .any(|claim_id| claim_id == &king_claim_id)
            {
                works_at_group = Some(group);
                break;
            }
        }
    }
    let works_at_group = works_at_group.unwrap_or_else(|| {
        panic!(
            "no resolved conflict group covers the King rel {king_claim_id}; \
             resolved groups: {:?}",
            outcome.resolved_group_ids
        )
    });
    let rationale = works_at_group
        .resolution_rationale
        .clone()
        .unwrap_or_default();
    anyhow::ensure!(
        !rationale.trim().is_empty(),
        "adjudicated group must carry a verdict rationale: {works_at_group:#?}"
    );
    // Probe the audit trail with a rationale prefix, stopping before any
    // character the audit snapshot would JSON-escape.
    let probe: String = rationale
        .chars()
        .take_while(|c| *c != '"' && *c != '\\' && !c.is_control())
        .take(40)
        .collect();
    anyhow::ensure!(
        probe.len() >= 8,
        "verdict rationale too short/exotic for an audit probe: '{rationale}'"
    );
    assert_group_adjudicated(client, &works_at_group.base.id, &probe).await?;

    // (c) Learned works_at semantics persisted as LlmProposed with the
    // proposed cardinality recorded (the model decides the value; employment
    // is expected to be single-valued, so log it rather than pin it).
    assert_semantics_llm_proposed(client, &blizzard_rel.rel_type, None).await?;

    Ok(())
}
