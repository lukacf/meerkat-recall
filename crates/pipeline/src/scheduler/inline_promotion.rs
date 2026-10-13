//! Inline promotion: deterministic code that takes structured extractor output
//! and prepares entities, rels, attributes, and assertions without live writes.
//! Complete source publication is owned by the manifest finalizer and the
//! canonical storage transaction.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Timelike, Utc};
use sha2::{Digest, Sha256};

use elephant_outbox::{EventOperation, OutboxPayload, RecordRef};
use elephant_storage::{
    AssertionRepository, AttributeRepository, EntityRepository, EventRepository,
    FreshnessRepository, RelRepository, StorageClient,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::{
    Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
};
use elephant_types::records::attribute::Literal;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::entity::{Entity, EntityStatus, EntityType};
use elephant_types::records::event::{Event, EventStatus, TimePrecision};
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel, TimeBasis};
use elephant_types::{Link, RecordId};

use super::grounding::{load_grounding_lineage, same_support_partition, GroundingLineage};
use super::promotion_plan::{
    record_claim_freshness, PlanCapture, PlannedEntities, PlannedEvents, PlannedRels,
    PlannedRepository,
};
use crate::error::PipelineError;
use crate::extraction::RawAssertion;
use crate::freshness::ClaimFreshnessObservation;
use crate::scheduler::promotion_disambiguation::{
    CandidateEntity, DisambiguationRequest, PromotionDisambiguationDecision, PromotionDisambiguator,
};
use elephant_storage::canonical_commit::{CanonicalPlan, CanonicalRecord};

const DEFAULT_EVENT_AUTO_APPROVE_THRESHOLD: f32 = 0.85;
const CONTEXT_SNIPPET_MAX_CHARS: usize = 500;
const EVENT_METADATA_LINEAGE_KEY: &str = "lineage_key";
pub(crate) const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";

/// Result of inline promotion for a batch of assertions.
#[derive(Debug, Clone, Default)]
pub struct PromotionResult {
    /// Number of entities created.
    pub entities_created: usize,
    /// Number of rels created.
    pub rels_created: usize,
    /// Number of attributes created.
    pub attributes_created: usize,
    /// Number of assertions recorded.
    pub assertions_recorded: usize,
    /// Number of assertions skipped due to significance="skip".
    pub assertions_skipped: usize,
    /// Number of outbox events written (for truth maintenance propagation).
    pub outbox_events_written: usize,
    /// Number of event records created.
    pub events_created: usize,
}

/// Configuration seam for inline promotion.
///
/// Constructed by the binary (which reads the environment); library code and
/// tests pass fakes or `Default` explicitly.
#[derive(Debug, Clone, Default)]
pub struct PromotionOptions {
    /// Captured source head and attempt authority. Never inferred from current state.
    pub publication: Option<elephant_storage::canonical_commit::CanonicalPublicationContext>,
    /// Stable identity of the complete publication batch, not an assertion ID.
    pub commit_id: Option<String>,
    /// Optional LLM disambiguator consulted when identity-key entity reuse is
    /// ambiguous or type-conflicting. `None` means deterministic rules only.
    pub disambiguator: Option<Arc<dyn PromotionDisambiguator>>,
}

/// Promotes a batch of extracted assertions with default options
/// (no LLM disambiguation).
///
/// See [`promote_assertions_inline_with_options`].
///
/// # Errors
///
/// Returns an error if entity validation fails due to a database error (not NotFound),
/// or if any record creation fails.
pub async fn promote_assertions_inline(
    storage: &StorageClient,
    space_id: &str,
    assertions: &[RawAssertion],
    work_run_id: &RecordId,
    doc_node_id: &RecordId,
    revision_id: &RecordId,
    actor: &str,
) -> Result<elephant_storage::canonical_commit::CanonicalCommitReceipt, PipelineError> {
    promote_assertions_inline_with_options(
        storage,
        space_id,
        assertions,
        work_run_id,
        doc_node_id,
        revision_id,
        actor,
        &PromotionOptions::default(),
    )
    .await
}

/// Compatibility entrypoint for callers with captured source/attempt authority.
/// Finalizes only the complete durable source manifest; supplied node-local
/// arguments never authorize additional or partial effects. Returns the durable
/// receipt rather than invented per-node counts. New callers should use
/// [`crate::source_finalization::finalize_source_revision`] directly.
#[allow(clippy::too_many_arguments)]
pub async fn promote_assertions_inline_with_options(
    storage: &StorageClient,
    space_id: &str,
    _assertions: &[RawAssertion],
    _work_run_id: &RecordId,
    _doc_node_id: &RecordId,
    _revision_id: &RecordId,
    actor: &str,
    options: &PromotionOptions,
) -> Result<elephant_storage::canonical_commit::CanonicalCommitReceipt, PipelineError> {
    let publication = options.publication.as_ref().ok_or_else(|| {
        PipelineError::ValidationError(
            "Source promotion requires captured publication and attempt fences".into(),
        )
    })?;
    crate::source_finalization::finalize_source_revision(
        storage,
        space_id,
        &publication.source,
        &publication.attempt,
        actor,
        options,
    )
    .await
}

/// One staged extraction group within a complete source publication.
#[derive(Debug, Clone)]
pub struct PromotionGroup {
    /// Supported interpretations; unsupported artifacts must be repaired first.
    pub assertions: Vec<RawAssertion>,
    /// Extraction provenance, separate from the finalizer's fenced attempt.
    pub work_run_id: RecordId,
    /// Immutable structural node identity.
    pub doc_node_id: RecordId,
    /// Immutable rendered revision identity.
    pub revision_id: RecordId,
}

/// Prepare all groups against one epoch and shared overlay; performs no writes.
pub async fn prepare_assertion_groups(
    storage: &StorageClient,
    space_id: &str,
    groups: &[PromotionGroup],
    actor: &str,
    options: &PromotionOptions,
) -> Result<PreparedPromotion, PipelineError> {
    if groups.is_empty() {
        return Err(PipelineError::ValidationError(
            "A source publication needs an explicit extraction group".into(),
        ));
    }
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone());
    let prefix = if let Some(publication) = options.publication.as_ref() {
        canonical
            .prepare_source_replacement(space_id, &publication.source)
            .await?
    } else {
        CanonicalPlan {
            observed_epoch: canonical.epoch(space_id).await?,
            dependencies: Vec::new(),
            effects: Vec::new(),
            outbox: Vec::new(),
            ..CanonicalPlan::default()
        }
    };
    Box::pin(prepare_assertion_groups_with_plan(
        storage, space_id, groups, actor, options, prefix,
    ))
    .await
}

/// Continue the finalizer's original authorized overlay before resolving groups.
///
/// The caller prepares source retirement, then attaches the sealed effects from
/// the original acceptance. This function keeps that epoch, original before
/// images, dependencies, outbox and effects intact. It does not reload contact
/// authority, restart retirement or relax immutable support partitions. Only the
/// canonical owner may publish the returned plan.
pub(crate) async fn prepare_assertion_groups_with_plan(
    storage: &StorageClient,
    space_id: &str,
    groups: &[PromotionGroup],
    actor: &str,
    options: &PromotionOptions,
    prefix: CanonicalPlan,
) -> Result<PreparedPromotion, PipelineError> {
    if groups.is_empty() || prefix.observed_epoch.is_empty() {
        return Err(PipelineError::ValidationError(
            "Promotion needs extraction groups and the original captured plan epoch".into(),
        ));
    }
    if options.publication.as_ref().is_some_and(|publication| {
        publication.attempt.space_id != space_id
            || publication.attempt.work_item_id.table() != TableName::WorkItem
            || publication.attempt.generation == 0
            || publication.attempt.lease_token.is_empty()
            || publication.source.source_id.table() != TableName::Source
            || publication.source.generation.is_empty()
            || publication.source.revision_id.is_empty()
            || publication.source.operation_id.is_empty()
    }) {
        return Err(PipelineError::ValidationError(
            "Promotion prefix requires the original source and same-space attempt fences".into(),
        ));
    }
    if prefix
        .dependencies
        .iter()
        .chain(
            prefix
                .effects
                .iter()
                .flat_map(|effect| effect.before.iter().chain(effect.after.iter())),
        )
        .any(|record| record.base().space_id != space_id)
        || prefix
            .outbox
            .iter()
            .any(|event| event.base.space_id != space_id)
    {
        return Err(PipelineError::ValidationError(
            "Promotion prefix belongs to another space".into(),
        ));
    }
    // Compare the caller's original observation; never replace it with a newer
    // epoch. The canonical owner repeats this check at actual publication.
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone());
    if canonical.epoch(space_id).await? != prefix.observed_epoch {
        // This is a concurrent preparation conflict, before any group can
        // consult a disambiguator. Keep the same typed refusal as retirement;
        // the finalizer alone decides whether the original attempt is refundable.
        return Err(
            elephant_storage::StorageError::Conflict("canonical_epoch_changed".into()).into(),
        );
    }
    let capture = PlanCapture::new(prefix);
    let mut ordered: Vec<_> = groups.iter().collect();
    ordered.sort_by_key(|group| (group.revision_id.to_string(), group.doc_node_id.to_string()));
    let mut result = PromotionResult::default();
    for group in ordered {
        let counts = Box::pin(prepare_group(
            storage,
            space_id,
            &group.assertions,
            &group.work_run_id,
            &group.doc_node_id,
            &group.revision_id,
            actor,
            options,
            &capture,
        ))
        .await?;
        result.entities_created += counts.entities_created;
        result.rels_created += counts.rels_created;
        result.attributes_created += counts.attributes_created;
        result.assertions_recorded += counts.assertions_recorded;
        result.assertions_skipped += counts.assertions_skipped;
        result.outbox_events_written += counts.outbox_events_written;
        result.events_created += counts.events_created;
    }
    let plan = capture.finish()?;
    let sources: std::collections::BTreeSet<_> = plan
        .dependencies
        .iter()
        .filter_map(|record| {
            if let CanonicalRecord::Source(source) = record {
                Some(source.base.id.to_string())
            } else {
                None
            }
        })
        .collect();
    if options.publication.is_none() && sources.len() != 1 {
        return Err(PipelineError::ValidationError(
            "A source publication cannot mix sources".into(),
        ));
    }
    Ok(PreparedPromotion { plan, result })
}

/// Prepare one group without writing live storage, for a complete batch caller.
#[allow(clippy::too_many_arguments)]
pub async fn prepare_assertions_inline_with_options(
    storage: &StorageClient,
    space_id: &str,
    assertions: &[RawAssertion],
    work_run_id: &RecordId,
    doc_node_id: &RecordId,
    revision_id: &RecordId,
    actor: &str,
    options: &PromotionOptions,
) -> Result<PreparedPromotion, PipelineError> {
    prepare_assertion_groups(
        storage,
        space_id,
        &[PromotionGroup {
            assertions: assertions.to_vec(),
            work_run_id: work_run_id.clone(),
            doc_node_id: doc_node_id.clone(),
            revision_id: revision_id.clone(),
        }],
        actor,
        options,
    )
    .await
}

#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
async fn prepare_group(
    storage: &StorageClient,
    space_id: &str,
    assertions: &[RawAssertion],
    work_run_id: &RecordId,
    doc_node_id: &RecordId,
    revision_id: &RecordId,
    actor: &str,
    options: &PromotionOptions,
    capture: &PlanCapture,
) -> Result<PromotionResult, PipelineError> {
    let entity_repo = PlannedRepository::new(EntityRepository::new(storage.clone()), capture);
    let assertion_repo = PlannedRepository::new(AssertionRepository::new(storage.clone()), capture);
    let rel_repo = PlannedRepository::new(RelRepository::new(storage.clone()), capture);
    let attr_repo = PlannedRepository::new(AttributeRepository::new(storage.clone()), capture);
    let event_repo = PlannedRepository::new(EventRepository::new(storage.clone()), capture);
    let freshness_repo = PlannedRepository::new(FreshnessRepository::new(storage.clone()), capture);

    let mut result = PromotionResult::default();
    let mut outbox_records: Vec<RecordRef> = Vec::new();
    let event_auto_approve_threshold = event_auto_approve_threshold();

    let lineage = Box::pin(load_grounding_lineage(
        storage,
        space_id,
        revision_id,
        doc_node_id,
    ))
    .await?;
    if options
        .publication
        .as_ref()
        .is_some_and(|publication| publication.source.source_id != lineage.source.base.id)
    {
        return Err(PipelineError::ValidationError(
            "Extraction group belongs to a different accepted source".into(),
        ));
    }
    lineage.validate_assertions(assertions)?;
    capture.dependency(CanonicalRecord::Source(Box::new(lineage.source.clone())))?;
    capture.dependency(CanonicalRecord::Doc(Box::new(lineage.doc.clone())))?;
    capture.dependency(CanonicalRecord::DocRevision(Box::new(
        lineage.revision.clone(),
    )))?;
    capture.dependency(CanonicalRecord::DocNode(Box::new(lineage.node.clone())))?;
    let doc_protectees = angel_protectees_from_metadata(&lineage.doc.metadata);

    let mut ordered: Vec<_> = assertions.iter().collect();
    ordered.sort_by(|left, right| {
        build_assertion_key(doc_node_id, &left.predicate_text, left)
            .cmp(&build_assertion_key(
                doc_node_id,
                &right.predicate_text,
                right,
            ))
            .then_with(|| right.confidence.total_cmp(&left.confidence))
    });
    for raw in ordered {
        let assertion_key = build_assertion_key(doc_node_id, &raw.predicate_text, raw);
        let event_lineage_key = format!(
            "{}:{}:{}",
            lineage.source.base.id,
            revision_id,
            hex::encode(Sha256::digest(
                serde_json::json!({
                    "subject": raw.subject_text, "predicate": raw.predicate_text,
                    "object": raw.object_text, "literal": raw.object_literal,
                    "extractor_lineage": raw.event_lineage_key,
                })
                .to_string()
                .as_bytes()
            ))
        );

        // The commit receipt, not an assertion's existence, governs replay.
        // Encountering a preexisting semantic assertion outside that receipt is
        // an explicit reconciliation conflict; never mark missing effects done.
        if let Some(existing) = assertion_repo
            .find_by_assertion_key(space_id, &assertion_key)
            .await?
        {
            if capture.has_effect(&existing.base.id)? {
                continue;
            }
            return Err(PipelineError::InvalidState(
                "Assertion exists without this bundle receipt; canonical reconciliation required"
                    .into(),
            ));
        }

        let context_snippet = assertion_context_snippet(raw);
        let disambiguator = options.disambiguator.as_deref();

        // 1. Resolve or create subject entity
        let subject_entity_link = Box::pin(resolve_or_create_entity(
            &entity_repo,
            space_id,
            raw.subject_entity_id.as_deref(),
            &raw.subject_text,
            raw.subject_entity_type.as_deref(),
            &context_snippet,
            disambiguator,
            &doc_protectees,
            &lineage,
            actor,
            &mut result,
            &mut outbox_records,
        ))
        .await?;

        // 2. Resolve or create object entity (only for entity-to-entity, not literals)
        let object_entity_link = if raw.object_literal.is_none() {
            let object_text = raw.object_text.as_deref().unwrap_or("");
            if object_text.is_empty() {
                None
            } else {
                Some(
                    Box::pin(resolve_or_create_entity(
                        &entity_repo,
                        space_id,
                        raw.object_entity_id.as_deref(),
                        object_text,
                        raw.object_entity_type.as_deref(),
                        &context_snippet,
                        disambiguator,
                        &doc_protectees,
                        &lineage,
                        actor,
                        &mut result,
                        &mut outbox_records,
                    ))
                    .await?,
                )
            }
        } else {
            None
        };

        // 3. Create assertion record (always — it's the evidence layer)
        let assertion_id = RecordId::new_v4(TableName::Assertion);
        let mut assertion_base = BaseFields::new(
            assertion_id.clone(),
            space_id.to_string(),
            actor.to_string(),
        );
        assertion_base.subjects.push(subject_entity_link.clone());
        assertion_base.subjects_state = SubjectsState::Complete;
        assertion_base.provenance.work_run_id = Some(work_run_id.clone());
        assertion_base.provenance.run_id = Some(work_run_id.to_string());
        lineage.inherit(&mut assertion_base);

        let now = Utc::now();
        #[allow(clippy::cast_possible_truncation)]
        let confidence_f32 = raw.confidence as f32;

        let assertion = Assertion {
            base: assertion_base,
            subject_text: raw.subject_text.clone(),
            predicate_raw: raw.predicate_text.clone(),
            predicate_id: Some(raw.predicate_text.clone()),
            object_text: raw.object_text.clone(),
            object_literal: raw.object_literal.as_ref().map(|lit| literal_to_json(lit)),
            evidence: EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: Some(Link::new(doc_node_id.clone())),
                start: raw.evidence_span.start,
                end: raw.evidence_span.end,
                quote_hash: raw.evidence_span.quote_hash.clone(),
            },
            source_event_time: raw.source_event_time,
            observed_at: now,
            asserted_at: now,
            assertion_key: assertion_key.clone(),
            resolution_status: ResolutionStatus::Resolved,
            resolution_method: Some("inline_promotion".to_string()),
            resolution_confidence: Some(confidence_f32),
            retry_state: AssertionRetryState::default(),
            subject_entity: Some(subject_entity_link.clone()),
            object_entity: object_entity_link.clone(),
            confidence: confidence_f32,
            subject_entity_type: raw.subject_entity_type.clone(),
            object_entity_type: raw.object_entity_type.clone(),
            significance: raw.significance.clone(),
            lifecycle_tier: "active".to_string(),
        };

        assertion_repo
            .create(assertion)
            .await
            .map_err(PipelineError::StorageError)?;
        result.assertions_recorded += 1;
        outbox_records.push(RecordRef {
            id: assertion_id.clone(),
            table: "assertion".into(),
            op: EventOperation::Create,
        });

        // 4. If significance == "skip", don't promote to rel/attr
        let is_skip = raw
            .significance
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("skip"));
        if is_skip {
            result.assertions_skipped += 1;
            continue;
        }

        // 5. Create a first-class domain event even when time is fuzzy or unknown.
        let event_id = RecordId::new_v4(TableName::Event);
        let mut event_base =
            BaseFields::new(event_id.clone(), space_id.to_string(), actor.to_string());
        event_base.subjects.push(subject_entity_link.clone());
        if let Some(ref object_link) = object_entity_link {
            if object_link.id() != subject_entity_link.id() {
                event_base.subjects.push(object_link.clone());
            }
        }
        event_base.subjects_state = SubjectsState::Complete;
        event_base.provenance.work_run_id = Some(work_run_id.clone());
        event_base.provenance.run_id = Some(work_run_id.to_string());
        lineage.inherit(&mut event_base);

        let assertion_link = Link::new(assertion_id.clone());
        if let Some(mut existing_event) = Box::pin(find_event_for_lineage_refinement(
            &event_repo,
            space_id,
            &event_lineage_key,
            raw,
            &event_base,
        ))
        .await?
        {
            let updated = apply_event_temporal_refinement(
                &mut existing_event,
                raw,
                Some(assertion_link.clone()),
                event_auto_approve_threshold,
            );
            if updated {
                event_repo
                    .update(space_id, &existing_event.base.id, existing_event.clone())
                    .await
                    .map_err(PipelineError::StorageError)?;
                outbox_records.push(RecordRef {
                    id: existing_event.base.id.clone(),
                    table: "event".to_string(),
                    op: EventOperation::Update,
                });
            }
            let event_evidence_times = [raw.source_event_time.unwrap_or(now)];
            Box::pin(record_claim_freshness(
                &freshness_repo,
                &existing_event.base.id,
                "event",
                &existing_event.base,
                ClaimFreshnessObservation {
                    evidence_times: &event_evidence_times,
                    evidence_strength: existing_event.confidence,
                    conflict_severity: flag_score(existing_event.status == EventStatus::Rejected),
                    corroboration_count: existing_event.assertion_refs.len(),
                },
            ))
            .await?;
        } else {
            let event = Event {
                base: event_base,
                title: build_event_title(raw),
                summary: None,
                status: if confidence_f32 >= event_auto_approve_threshold {
                    EventStatus::Approved
                } else {
                    EventStatus::Candidate
                },
                time_start: raw.source_event_time,
                time_end: None,
                time_precision: event_time_precision(raw),
                confidence: confidence_f32,
                time_confidence: confidence_f32,
                assertion_refs: vec![assertion_link.clone()],
                source_refs: vec![Link::new(lineage.source.base.id.clone())],
                metadata: serde_json::json!({
                    "origin": "inline_promotion",
                    EVENT_METADATA_LINEAGE_KEY: event_lineage_key,
                    "time_basis": if raw.source_event_time.is_some() {
                        "source_event_time"
                    } else {
                        "unknown"
                    },
                }),
            };

            let event = event_repo
                .create(event)
                .await
                .map_err(PipelineError::StorageError)?;
            let event_evidence_times = [raw.source_event_time.unwrap_or(now)];
            Box::pin(record_claim_freshness(
                &freshness_repo,
                &event.base.id,
                "event",
                &event.base,
                ClaimFreshnessObservation {
                    evidence_times: &event_evidence_times,
                    evidence_strength: event.confidence,
                    conflict_severity: 0.0,
                    corroboration_count: event.assertion_refs.len(),
                },
            ))
            .await?;
            result.events_created += 1;
            outbox_records.push(RecordRef {
                id: event_id,
                table: "event".to_string(),
                op: EventOperation::Create,
            });
        }

        // 6. Create rel or attribute
        let evidence_ref = EvidenceRef {
            assertion: Link::new(assertion_id.clone()),
            span: EvidenceSpan {
                revision: Link::new(revision_id.clone()),
                node: Some(Link::new(doc_node_id.clone())),
                start: raw.evidence_span.start,
                end: raw.evidence_span.end,
                quote_hash: raw.evidence_span.quote_hash.clone(),
            },
            time_basis: if raw.source_event_time.is_some() {
                TimeBasis::SourceEventTime
            } else {
                TimeBasis::ObservedAt
            },
            time_value: raw.source_event_time.unwrap_or(now),
        };

        if let Some(ref literal) = raw.object_literal {
            // Create attribute
            let attr_id = RecordId::new_v4(TableName::Attribute);
            let mut attr_base =
                BaseFields::new(attr_id.clone(), space_id.to_string(), actor.to_string());
            attr_base.subjects.push(subject_entity_link.clone());
            attr_base.subjects_state = SubjectsState::Complete;
            attr_base.provenance.work_run_id = Some(work_run_id.clone());
            attr_base.provenance.run_id = Some(work_run_id.to_string());
            lineage.inherit(&mut attr_base);

            let attribute = elephant_types::records::attribute::Attribute {
                base: attr_base,
                subject: subject_entity_link.clone(),
                attr_type: raw.predicate_text.clone(),
                value: literal.clone(),
                status: ClaimStatus::Candidate,
                confidence: confidence_f32,
                validity_window: None,
                evidence: vec![evidence_ref.clone()],
                temporal_audit: None,
            };

            let attribute = attr_repo
                .create(attribute)
                .await
                .map_err(PipelineError::StorageError)?;
            let attribute_evidence_times = attribute
                .evidence
                .iter()
                .map(|evidence| evidence.time_value)
                .collect::<Vec<_>>();
            Box::pin(record_claim_freshness(
                &freshness_repo,
                &attribute.base.id,
                "attribute",
                &attribute.base,
                ClaimFreshnessObservation {
                    evidence_times: &attribute_evidence_times,
                    evidence_strength: attribute.confidence,
                    conflict_severity: flag_score(attribute.status == ClaimStatus::Disputed),
                    corroboration_count: attribute.evidence.len(),
                },
            ))
            .await?;
            result.attributes_created += 1;
            outbox_records.push(RecordRef {
                id: attr_id,
                table: "attribute".to_string(),
                op: EventOperation::Create,
            });
        } else if let Some(ref object_link) = object_entity_link {
            // Create rel
            let rel_id = RecordId::new_v4(TableName::Rel);
            let mut rel_base =
                BaseFields::new(rel_id.clone(), space_id.to_string(), actor.to_string());
            rel_base.subjects.push(subject_entity_link.clone());
            rel_base.subjects_state = SubjectsState::Complete;
            rel_base.provenance.work_run_id = Some(work_run_id.clone());
            rel_base.provenance.run_id = Some(work_run_id.to_string());
            lineage.inherit(&mut rel_base);

            let rel = Rel {
                base: rel_base,
                subject: subject_entity_link.clone(),
                object: object_link.clone(),
                rel_type: raw.predicate_text.clone(),
                status: ClaimStatus::Candidate,
                confidence: confidence_f32,
                validity_window: None,
                evidence: vec![evidence_ref.clone()],
                temporal_audit: None,
            };

            let (relation, op) = Box::pin(merge_or_create_rel(
                &rel_repo,
                space_id,
                rel,
                evidence_ref,
                confidence_f32,
            ))
            .await?;
            let relation_evidence_times = relation
                .evidence
                .iter()
                .map(|evidence| evidence.time_value)
                .collect::<Vec<_>>();
            Box::pin(record_claim_freshness(
                &freshness_repo,
                &relation.base.id,
                "rel",
                &relation.base,
                ClaimFreshnessObservation {
                    evidence_times: &relation_evidence_times,
                    evidence_strength: relation.confidence,
                    conflict_severity: flag_score(relation.status == ClaimStatus::Disputed),
                    corroboration_count: relation.evidence.len(),
                },
            ))
            .await?;
            if matches!(op, EventOperation::Create) {
                result.rels_created += 1;
            }
            outbox_records.push(RecordRef {
                id: relation.base.id,
                table: "rel".to_string(),
                op,
            });
        }
    }

    // Write outbox events so truth maintenance and downstream workers fire.
    if !outbox_records.is_empty() {
        let mut payload = OutboxPayload::new(
            space_id.to_string(),
            "inline_promotion.completed".to_string(),
            actor.to_string(),
        );
        payload = payload.with_run_id(work_run_id.to_string());
        for record in outbox_records {
            payload.add_record(record);
        }
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            space_id.to_string(),
            actor.to_string(),
        );
        lineage.inherit(&mut base);
        base.provenance.work_run_id = Some(work_run_id.clone());
        capture.outbox(elephant_types::records::SystemEvent {
            base,
            seq: 0,
            event_type: payload.event_type.clone(),
            payload: serde_json::to_value(payload)
                .map_err(|error| PipelineError::ConfigError(error.to_string()))?,
        })?;
        result.outbox_events_written += 1;
    }

    Ok(result)
}

/// Complete, uncommitted canonical effects produced from verified evidence.
#[derive(Debug, Clone)]
pub struct PreparedPromotion {
    /// Storage-owned transaction input, including all read dependencies.
    pub plan: CanonicalPlan,
    /// Counts that become committed only after the atomic repository succeeds.
    pub result: PromotionResult,
}

fn lineage_entity_base(lineage: &GroundingLineage) -> BaseFields {
    let mut base = lineage.source.base.clone();
    lineage.inherit(&mut base);
    base
}

async fn merge_or_create_rel(
    rel_repo: &PlannedRels,
    space_id: &str,
    rel: Rel,
    evidence_ref: EvidenceRef,
    confidence: f32,
) -> Result<(Rel, EventOperation), PipelineError> {
    let mut existing = rel_repo
        .find_for_slot(space_id, rel.subject.id(), rel.object.id(), &rel.rel_type)
        .await
        .map_err(PipelineError::StorageError)?
        .into_iter()
        .find(|existing| same_support_partition(&existing.base, &rel.base));

    let Some(existing_rel) = existing.as_mut() else {
        let created = rel_repo
            .create(rel)
            .await
            .map_err(PipelineError::StorageError)?;
        return Ok((created, EventOperation::Create));
    };

    let evidence_assertion = evidence_ref.assertion.id().clone();
    if !existing_rel
        .evidence
        .iter()
        .any(|evidence| evidence.assertion.id() == &evidence_assertion)
    {
        existing_rel.evidence.push(evidence_ref);
    }
    existing_rel.confidence = existing_rel.confidence.max(confidence);
    if existing_rel.status == ClaimStatus::Candidate && rel.status == ClaimStatus::Approved {
        existing_rel.status = ClaimStatus::Approved;
    }
    let id = existing_rel.base.id.clone();
    rel_repo
        .update(space_id, &id, existing_rel.clone())
        .await
        .map_err(PipelineError::StorageError)?;
    Ok((existing_rel.clone(), EventOperation::Update))
}

/// Resolves an existing entity by ID, or creates a new one.
///
/// - A provided `entity_id` is an untrusted extraction hint. Reuse requires
///   the same source revision, authorization, compatible type and known name.
/// - Otherwise, identity-key matches are filtered by type compatibility;
///   ambiguous or conflicting matches go to the optional disambiguator
///   (see [`resolve_identity_key_matches`]).
/// - If nothing is reused: creates a new entity.
/// - DB/network errors are propagated (not swallowed).
#[allow(clippy::too_many_arguments)]
async fn resolve_or_create_entity(
    entity_repo: &PlannedEntities,
    space_id: &str,
    entity_id: Option<&str>,
    display_name: &str,
    entity_type_str: Option<&str>,
    context_snippet: &str,
    disambiguator: Option<&dyn PromotionDisambiguator>,
    doc_protectees: &[String],
    lineage: &GroundingLineage,
    actor: &str,
    result: &mut PromotionResult,
    outbox_records: &mut Vec<RecordRef>,
) -> Result<Link<Entity>, PipelineError> {
    let entity_type = entity_type_str
        .map(normalize_entity_type)
        .unwrap_or_else(|| EntityType::new("topic"));

    if let Some(record_id) = entity_id.and_then(|id| RecordId::from_str(id).ok()) {
        if let Some(entity) = entity_repo.get(space_id, &record_id).await? {
            let observed_key = display_name.trim().to_lowercase();
            let known_name = entity.display_name.trim().to_lowercase() == observed_key
                || entity
                    .aliases
                    .iter()
                    .chain(entity.identity_keys.iter())
                    .any(|name| name.trim().to_lowercase() == observed_key);
            if known_name
                && entity_types_compatible(&entity.entity_type, &entity_type)
                && same_support_partition(&entity.base, &lineage_entity_base(lineage))
            {
                return reuse_entity_for_display_name(
                    entity_repo,
                    space_id,
                    entity,
                    display_name,
                    Some(&entity_type),
                    doc_protectees,
                    outbox_records,
                )
                .await;
            }
        }
    }

    // Try to find existing entities by identity key (lowercased display_name)
    let identity_key = display_name.to_lowercase();
    let matches = entity_repo
        .find_by_identity_key_all(space_id, &identity_key)
        .await
        .map_err(PipelineError::StorageError)?
        .into_iter()
        .filter(|entity| same_support_partition(&entity.base, &lineage_entity_base(lineage)))
        .collect::<Vec<_>>();
    if !matches.is_empty() {
        if let Some(link) = resolve_identity_key_matches(
            entity_repo,
            space_id,
            matches,
            display_name,
            &entity_type,
            context_snippet,
            disambiguator,
            doc_protectees,
            outbox_records,
        )
        .await?
        {
            return Ok(link);
        }
        // Guard decided against reuse — create a new entity below.
    }

    let entity_id = RecordId::new_v4(TableName::Entity);
    let mut base = BaseFields::new(entity_id.clone(), space_id.to_string(), actor.to_string());
    lineage.inherit(&mut base);

    let mut metadata = serde_json::Value::Object(serde_json::Map::new());
    union_angel_protectees_into_metadata(&mut metadata, doc_protectees);

    let now = Utc::now();
    let entity = Entity {
        base,
        entity_type,
        display_name: display_name.to_string(),
        aliases: vec![],
        summary: None,
        handles: vec![],
        identity_keys: vec![display_name.to_lowercase()],
        risk_level: None,
        trust_level: None,
        known_safe: false,
        first_seen: Some(now),
        last_seen: Some(now),
        status: EntityStatus::Active,
        redirect_to: None,
        metadata,
    };

    entity_repo
        .create(entity)
        .await
        .map_err(PipelineError::StorageError)?;

    result.entities_created += 1;
    outbox_records.push(RecordRef {
        id: entity_id.clone(),
        table: "entity".to_string(),
        op: EventOperation::Create,
    });
    Ok(Link::new(entity_id))
}

/// Applies the type-compatibility guard to identity-key matches.
///
/// Returns `Ok(Some(link))` when a stored entity is reused, `Ok(None)` when a
/// new entity should be created instead:
/// - exactly one compatible match: reuse it;
/// - two or more compatible matches: ask the disambiguator, else reuse the
///   earliest-created match;
/// - only incompatible matches: ask the disambiguator (it may still reuse —
///   the stored type may be wrong), else create a new entity (never
///   false-merge).
#[allow(clippy::too_many_arguments)]
async fn resolve_identity_key_matches(
    entity_repo: &PlannedEntities,
    space_id: &str,
    matches: Vec<Entity>,
    display_name: &str,
    incoming_type: &EntityType,
    context_snippet: &str,
    disambiguator: Option<&dyn PromotionDisambiguator>,
    doc_protectees: &[String],
    outbox_records: &mut Vec<RecordRef>,
) -> Result<Option<Link<Entity>>, PipelineError> {
    let (mut compatible, mut incompatible): (Vec<Entity>, Vec<Entity>) = matches
        .into_iter()
        .partition(|entity| entity_types_compatible(&entity.entity_type, incoming_type));
    // Do NOT rely on scan order: stable order for deterministic picks/prompts.
    sort_candidates_stable(&mut compatible);
    sort_candidates_stable(&mut incompatible);

    if compatible.is_empty() {
        if let Some(disambiguator) = disambiguator {
            let decision = run_disambiguator(
                disambiguator,
                display_name,
                incoming_type,
                context_snippet,
                &incompatible,
            )
            .await?;
            match decision {
                Some(PromotionDisambiguationDecision::Reuse { entity_id }) => {
                    if let Some(entity) = take_entity_by_id(&mut incompatible, &entity_id) {
                        // LLM overrode the type guard; the stored type may be
                        // wrong, so no type upgrade here.
                        let link = reuse_entity_for_display_name(
                            entity_repo,
                            space_id,
                            entity,
                            display_name,
                            None,
                            doc_protectees,
                            outbox_records,
                        )
                        .await?;
                        return Ok(Some(link));
                    }
                    tracing::warn!(
                        display_name,
                        entity_id,
                        "disambiguator chose an entity id outside the candidate set; creating new entity"
                    );
                }
                Some(PromotionDisambiguationDecision::CreateNew) | None => {}
            }
        }
        tracing::warn!(
            display_name,
            incoming_type = incoming_type.as_str(),
            "identity-key matches have conflicting entity types; creating new entity"
        );
        return Ok(None);
    }

    if compatible.len() > 1 {
        // Asking a model to distinguish identical, information-free records can
        // amplify duplicates on every mention. Reuse the earliest such record;
        // consult the disambiguator when the candidates carry real differences.
        if candidates_are_indistinguishable(&compatible) {
            tracing::warn!(
                display_name,
                candidates = compatible.len(),
                "identity-key matches are indistinguishable (same name and type, no \
                 summary, aliases or handles to separate them); reusing earliest-created \
                 entity without consulting the disambiguator"
            );
        } else if let Some(disambiguator) = disambiguator {
            let decision = run_disambiguator(
                disambiguator,
                display_name,
                incoming_type,
                context_snippet,
                &compatible,
            )
            .await?;
            match decision {
                Some(PromotionDisambiguationDecision::CreateNew) => return Ok(None),
                Some(PromotionDisambiguationDecision::Reuse { entity_id }) => {
                    if let Some(entity) = take_entity_by_id(&mut compatible, &entity_id) {
                        let link = reuse_entity_for_display_name(
                            entity_repo,
                            space_id,
                            entity,
                            display_name,
                            Some(incoming_type),
                            doc_protectees,
                            outbox_records,
                        )
                        .await?;
                        return Ok(Some(link));
                    }
                    tracing::warn!(
                        display_name,
                        entity_id,
                        "disambiguator chose an entity id outside the candidate set; falling back to earliest match"
                    );
                }
                None => {}
            }
        }
        tracing::warn!(
            display_name,
            candidates = compatible.len(),
            "multiple compatible identity-key matches; reusing earliest-created entity"
        );
    }

    let Some(entity) = compatible.into_iter().next() else {
        return Ok(None);
    };
    let link = reuse_entity_for_display_name(
        entity_repo,
        space_id,
        entity,
        display_name,
        Some(incoming_type),
        doc_protectees,
        outbox_records,
    )
    .await?;
    Ok(Some(link))
}

/// Two entity types are compatible when they normalize to the same value or
/// either side is `topic` (the catch-all bucket for unknown types).
pub(crate) fn entity_types_compatible(stored: &EntityType, incoming: &EntityType) -> bool {
    let stored = normalize_entity_type(stored.as_str());
    let incoming = normalize_entity_type(incoming.as_str());
    stored.as_str() == incoming.as_str()
        || stored.as_str() == "topic"
        || incoming.as_str() == "topic"
}

/// Whether identity-key candidates carry anything that could tell them apart.
///
/// A disambiguator can only choose between records that differ. When every candidate
/// has the same display name and type and none carries a summary, alias or handle,
/// there is no information to choose on, and asking produces an arbitrary answer whose
/// CreateNew branch permanently worsens the corpus. See the call site for the measured
/// eight-way duplication this prevents.
pub(crate) fn candidates_are_indistinguishable(candidates: &[Entity]) -> bool {
    let mut iter = candidates.iter();
    let Some(first) = iter.next() else {
        return true;
    };
    if !first.summary.as_deref().unwrap_or("").is_empty()
        || !first.aliases.is_empty()
        || !first.handles.is_empty()
    {
        return false;
    }
    iter.all(|entity| {
        entity.summary.as_deref().unwrap_or("").is_empty()
            && entity.aliases.is_empty()
            && entity.handles.is_empty()
            && entity
                .display_name
                .eq_ignore_ascii_case(&first.display_name)
            && normalize_entity_type(entity.entity_type.as_str()).as_str()
                == normalize_entity_type(first.entity_type.as_str()).as_str()
    })
}

fn sort_candidates_stable(entities: &mut [Entity]) {
    entities.sort_by(|lhs, rhs| {
        lhs.base
            .created_at
            .cmp(&rhs.base.created_at)
            .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
    });
}

fn take_entity_by_id(candidates: &mut Vec<Entity>, entity_id: &str) -> Option<Entity> {
    let index = candidates
        .iter()
        .position(|entity| entity.base.id.to_string() == entity_id)?;
    Some(candidates.remove(index))
}

async fn run_disambiguator(
    disambiguator: &dyn PromotionDisambiguator,
    mention: &str,
    incoming_type: &EntityType,
    context_snippet: &str,
    candidates: &[Entity],
) -> Result<Option<PromotionDisambiguationDecision>, PipelineError> {
    let request = DisambiguationRequest {
        mention: mention.to_string(),
        normalized_type: incoming_type.as_str().to_string(),
        context_snippet: context_snippet.to_string(),
        candidates: candidates
            .iter()
            .map(|entity| CandidateEntity {
                id: entity.base.id.to_string(),
                display_name: entity.display_name.clone(),
                entity_type: entity.entity_type.as_str().to_string(),
                summary: entity.summary.clone(),
            })
            .collect(),
    };
    disambiguator.disambiguate(&request).await
}

/// Builds the source-document context passed to the disambiguator: the
/// extractor's quote when available, else the assertion triple as text.
fn assertion_context_snippet(raw: &RawAssertion) -> String {
    let base = raw
        .quote
        .as_deref()
        .map(str::trim)
        .filter(|quote| !quote.is_empty())
        .map_or_else(
            || {
                let object = raw.object_text.clone().unwrap_or_else(|| {
                    raw.object_literal
                        .as_ref()
                        .map(literal_to_event_title)
                        .unwrap_or_default()
                });
                format!(
                    "{} {} {}",
                    raw.subject_text.trim(),
                    raw.predicate_text.trim(),
                    object.trim()
                )
                .trim()
                .to_string()
            },
            ToString::to_string,
        );
    if base.chars().count() > CONTEXT_SNIPPET_MAX_CHARS {
        base.chars().take(CONTEXT_SNIPPET_MAX_CHARS).collect()
    } else {
        base
    }
}

async fn reuse_entity_for_display_name(
    entity_repo: &PlannedEntities,
    space_id: &str,
    entity: Entity,
    display_name: &str,
    incoming_type: Option<&EntityType>,
    doc_protectees: &[String],
    outbox_records: &mut Vec<RecordRef>,
) -> Result<Link<Entity>, PipelineError> {
    let original_base = entity.base.clone();
    let mut entity = follow_entity_redirect(entity_repo, space_id, entity).await?;
    if !same_support_partition(&entity.base, &original_base) {
        return Err(PipelineError::ValidationError(
            "Entity redirect crosses a support/security partition".into(),
        ));
    }
    let entity_id = entity.base.id.clone();
    let observed_name = display_name.trim();
    let identity_key = observed_name.to_ascii_lowercase();
    let mut updated = false;

    // A specific incoming type is strictly more information than the stored
    // catch-all "topic"; never downgrade or overwrite a specific stored type.
    if let Some(incoming) = incoming_type {
        if entity.entity_type.as_str() == "topic" && incoming.as_str() != "topic" {
            entity.entity_type = incoming.clone();
            updated = true;
        }
    }

    if union_angel_protectees_into_metadata(&mut entity.metadata, doc_protectees) {
        updated = true;
    }

    if !observed_name.is_empty() {
        if !entity.display_name.eq_ignore_ascii_case(observed_name)
            && !entity
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(observed_name))
        {
            entity.aliases.push(observed_name.to_string());
            updated = true;
        }

        if !entity
            .identity_keys
            .iter()
            .any(|key| key.eq_ignore_ascii_case(&identity_key))
        {
            entity.identity_keys.push(identity_key);
            updated = true;
        }

        if should_promote_observed_display_name(&entity, observed_name) {
            if !entity
                .aliases
                .iter()
                .any(|alias| alias.eq_ignore_ascii_case(&entity.display_name))
            {
                entity.aliases.push(entity.display_name.clone());
            }
            entity.display_name = observed_name.to_string();
            updated = true;
        }

        if entity.last_seen.is_none() {
            entity.last_seen = Some(Utc::now());
            updated = true;
        }
    }

    if updated {
        entity_repo
            .update(space_id, &entity_id, entity)
            .await
            .map_err(PipelineError::StorageError)?;
        outbox_records.push(RecordRef {
            id: entity_id.clone(),
            table: "entity".to_string(),
            op: EventOperation::Update,
        });
    }

    Ok(Link::new(entity_id))
}

fn should_promote_observed_display_name(entity: &Entity, observed_name: &str) -> bool {
    let observed = observed_name.trim();
    if observed.is_empty() || observed.contains('@') {
        return false;
    }
    if !entity.entity_type.as_str().eq_ignore_ascii_case("person") {
        return false;
    }
    if !entity.display_name.contains('@') {
        return false;
    }
    observed.chars().any(char::is_whitespace) && observed.chars().any(char::is_alphabetic)
}

async fn follow_entity_redirect(
    entity_repo: &PlannedEntities,
    space_id: &str,
    mut entity: Entity,
) -> Result<Entity, PipelineError> {
    for _ in 0..8 {
        if entity.status != EntityStatus::Merged {
            return Ok(entity);
        }

        let Some(redirect_to) = entity.redirect_to.as_ref() else {
            return Ok(entity);
        };

        match entity_repo
            .get(space_id, redirect_to.id())
            .await
            .map_err(PipelineError::StorageError)?
        {
            Some(next) => entity = next,
            None => return Ok(entity),
        }
    }

    Ok(entity)
}

/// Reads `metadata.angel_protectees` as a list of string ids.
///
/// Missing keys, non-array values, and non-string elements yield an empty /
/// filtered list rather than an error.
pub(crate) fn angel_protectees_from_metadata(metadata: &serde_json::Value) -> Vec<String> {
    metadata
        .get(ANGEL_PROTECTEES_METADATA_KEY)
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Unions `ids` into `metadata.angel_protectees` (deduped + sorted for
/// determinism).
///
/// Returns `true` when the stored value changed so callers can skip
/// persisting no-op unions.
pub(crate) fn union_angel_protectees_into_metadata(
    metadata: &mut serde_json::Value,
    ids: &[String],
) -> bool {
    if ids.is_empty() {
        return false;
    }

    let existing = angel_protectees_from_metadata(metadata);
    let mut union: Vec<String> = existing.iter().chain(ids.iter()).cloned().collect();
    union.sort();
    union.dedup();
    if union == existing {
        return false;
    }

    if !metadata.is_object() {
        *metadata = serde_json::Value::Object(serde_json::Map::new());
    }
    if let Some(map) = metadata.as_object_mut() {
        map.insert(
            ANGEL_PROTECTEES_METADATA_KEY.to_string(),
            serde_json::Value::Array(union.into_iter().map(serde_json::Value::String).collect()),
        );
    }
    true
}

/// Conjoin restrictions only within one policy schema. Distinct policy IDs
/// have no generic ordering and must never be folded with first-wins semantics.
pub(crate) fn escalate_security(
    existing: &mut Security,
    incoming: Option<&Security>,
) -> Result<(), PipelineError> {
    let Some(incoming) = incoming else {
        return Ok(());
    };
    if existing.policy_id != incoming.policy_id {
        return Err(PipelineError::ValidationError(
            "Cannot conjoin different security policy schemas".into(),
        ));
    }
    existing.level = existing.level.max(incoming.level);
    existing.labels.extend(incoming.labels.iter().cloned());
    existing.labels.sort();
    existing.labels.dedup();
    existing.handling.extend(incoming.handling.iter().cloned());
    existing.handling.sort();
    existing.handling.dedup();
    Ok(())
}

/// Builds the idempotency key for an assertion.
fn build_assertion_key(doc_node_id: &RecordId, predicate: &str, raw: &RawAssertion) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        serde_json::json!({
            "node": doc_node_id.to_string(), "predicate": predicate,
            "revision": raw.evidence_span.revision.id().to_string(),
            "start": raw.evidence_span.start, "end": raw.evidence_span.end,
            "quote": raw.evidence_span.quote_hash,
            "subject": raw.subject_text, "object": raw.object_text, "literal": raw.object_literal,
            "subject_type": raw.subject_entity_type, "object_type": raw.object_entity_type,
            "subject_entity": raw.subject_entity_id, "object_entity": raw.object_entity_id,
            "event_time": raw.source_event_time, "event_precision": raw.source_event_time_precision,
            "event_title": raw.event_title, "significance": raw.significance,
        })
        .to_string()
        .as_bytes(),
    );
    format!("grounded:{}", hex::encode(hasher.finalize()))
}

fn time_precision_rank(precision: TimePrecision) -> u8 {
    match precision {
        TimePrecision::Exact => 0,
        TimePrecision::Minute => 1,
        TimePrecision::Hour => 2,
        TimePrecision::Day => 3,
        TimePrecision::Month => 4,
        TimePrecision::Year => 5,
        TimePrecision::Unknown => 6,
    }
}

fn event_times_match_at_precision(
    lhs: DateTime<Utc>,
    rhs: DateTime<Utc>,
    precision: TimePrecision,
) -> bool {
    match precision {
        TimePrecision::Exact => lhs == rhs,
        TimePrecision::Minute => {
            lhs.year() == rhs.year()
                && lhs.month() == rhs.month()
                && lhs.day() == rhs.day()
                && lhs.hour() == rhs.hour()
                && lhs.minute() == rhs.minute()
        }
        TimePrecision::Hour => {
            lhs.year() == rhs.year()
                && lhs.month() == rhs.month()
                && lhs.day() == rhs.day()
                && lhs.hour() == rhs.hour()
        }
        TimePrecision::Day => {
            lhs.year() == rhs.year() && lhs.month() == rhs.month() && lhs.day() == rhs.day()
        }
        TimePrecision::Month => lhs.year() == rhs.year() && lhs.month() == rhs.month(),
        TimePrecision::Year => lhs.year() == rhs.year(),
        TimePrecision::Unknown => true,
    }
}

fn coarser_time_precision(lhs: TimePrecision, rhs: TimePrecision) -> TimePrecision {
    if time_precision_rank(lhs) >= time_precision_rank(rhs) {
        lhs
    } else {
        rhs
    }
}

fn event_is_time_compatible(
    event: &Event,
    raw_time: DateTime<Utc>,
    raw_precision: TimePrecision,
) -> bool {
    let Some(existing_time) = event.time_start else {
        return true;
    };
    if event.time_precision == TimePrecision::Unknown {
        return true;
    }
    let comparison_precision = coarser_time_precision(event.time_precision, raw_precision);
    event_times_match_at_precision(existing_time, raw_time, comparison_precision)
}

async fn find_event_for_lineage_refinement(
    event_repo: &PlannedEvents,
    space_id: &str,
    lineage_key: &str,
    raw: &RawAssertion,
    expected_base: &BaseFields,
) -> Result<Option<Event>, crate::error::PipelineError> {
    let candidates = event_repo
        .find_all_by_lineage_key(space_id, lineage_key)
        .await
        .map_err(PipelineError::StorageError)?
        .into_iter()
        .filter(|event| same_support_partition(&event.base, expected_base))
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Ok(None);
    }

    if let Some(raw_time) = raw.source_event_time {
        let raw_precision = event_time_precision(raw);
        if let Some(same_time) = candidates
            .iter()
            .find(|event| event.time_start == Some(raw_time))
        {
            return Ok(Some(same_time.clone()));
        }
        let mut compatible = candidates
            .into_iter()
            .filter(|event| event_is_time_compatible(event, raw_time, raw_precision))
            .collect::<Vec<_>>();
        compatible.sort_by(|lhs, rhs| {
            lhs.time_start
                .is_none()
                .cmp(&rhs.time_start.is_none())
                .then_with(|| {
                    time_precision_rank(lhs.time_precision)
                        .cmp(&time_precision_rank(rhs.time_precision))
                })
                .then_with(|| rhs.base.updated_at.cmp(&lhs.base.updated_at))
                .then_with(|| rhs.base.created_at.cmp(&lhs.base.created_at))
                .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
        });
        if let Some(best_match) = compatible.into_iter().next() {
            return Ok(Some(best_match));
        }
        return Ok(None);
    }

    if candidates.len() == 1 {
        return Ok(candidates.into_iter().next());
    }

    Ok(candidates
        .into_iter()
        .find(|event| event.time_start.is_none() || event.time_precision == TimePrecision::Unknown))
}

fn apply_event_temporal_refinement(
    event: &mut Event,
    raw: &RawAssertion,
    assertion_ref: Option<Link<Assertion>>,
    auto_approve_threshold: f32,
) -> bool {
    let mut changed = false;

    if let Some(assertion_ref) = assertion_ref {
        if !event
            .assertion_refs
            .iter()
            .any(|existing| existing == &assertion_ref)
        {
            event.assertion_refs.push(assertion_ref);
            changed = true;
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    let raw_confidence = raw.confidence as f32;
    if raw_confidence > event.confidence {
        event.confidence = raw_confidence;
        changed = true;
    }
    let desired_status = if event.confidence >= auto_approve_threshold {
        EventStatus::Approved
    } else {
        EventStatus::Candidate
    };
    if event.status != desired_status {
        event.status = desired_status;
        changed = true;
    }

    if let Some(source_event_time) = raw.source_event_time {
        let new_precision = event_time_precision(raw);
        let should_replace_time = event.time_start.is_none()
            || event.time_precision == TimePrecision::Unknown
            || time_precision_rank(new_precision) < time_precision_rank(event.time_precision)
            || (time_precision_rank(new_precision) == time_precision_rank(event.time_precision)
                && raw_confidence >= event.time_confidence
                && event.time_start != Some(source_event_time));
        if should_replace_time {
            event.time_start = Some(source_event_time);
            event.time_precision = new_precision;
            event.time_confidence = raw_confidence;
            if !event.metadata.is_object() {
                event.metadata = serde_json::json!({});
            }
            if let Some(metadata) = event.metadata.as_object_mut() {
                metadata.insert(
                    "time_basis".to_string(),
                    serde_json::Value::String("source_event_time".to_string()),
                );
            }
            changed = true;
        }
    }

    changed
}

/// Maps LLM-provided entity type strings to valid DB schema types.
///
/// The DB schema constrains entity types to:
/// `person`, `app`, `group`, `topic`, `platform`, `org`, `location`, `device`.
/// LLMs may return other types (e.g. "other", "character", "animal") which need mapping.
pub(crate) fn normalize_entity_type(raw: &str) -> EntityType {
    let lower = raw.to_lowercase();
    match lower.as_str() {
        "person" | "app" | "group" | "topic" | "platform" | "org" | "location" | "device" => {
            EntityType::new(lower)
        }
        "organization" | "company" | "corporation" | "institution" => EntityType::new("org"),
        "place" | "city" | "country" | "region" | "address" => EntityType::new("location"),
        "character" | "animal" | "creature" => EntityType::new("person"),
        "team" | "committee" | "department" => EntityType::new("group"),
        _ => EntityType::new("topic"),
    }
}

/// Converts a `Literal` to a raw JSON value for the assertion's `object_literal` field.
///
/// Returns a plain JSON literal (string/number/bool/array), NOT wrapped in `{type, value}`.
/// The `AssertionRepository` rejects the tagged wrapper shape.
fn literal_to_json(literal: &Literal) -> serde_json::Value {
    match literal {
        Literal::String(s) => serde_json::Value::String(s.clone()),
        Literal::Integer(i) => serde_json::json!(i),
        Literal::Float(f) => serde_json::json!(f),
        Literal::Boolean(b) => serde_json::Value::Bool(*b),
        Literal::Date(d) => serde_json::Value::String(d.clone()),
        Literal::DateTime(dt) => serde_json::Value::String(dt.clone()),
        Literal::Duration(d) => serde_json::Value::String(d.clone()),
        Literal::Json(v) => v.clone(),
    }
}

fn literal_to_event_title(literal: &Literal) -> String {
    match literal {
        Literal::String(s) | Literal::Date(s) | Literal::DateTime(s) | Literal::Duration(s) => {
            s.clone()
        }
        Literal::Integer(i) => i.to_string(),
        Literal::Float(f) => f.to_string(),
        Literal::Boolean(b) => b.to_string(),
        Literal::Json(v) => v.to_string(),
    }
}

fn build_event_title(raw: &RawAssertion) -> String {
    if let Some(title) = raw.event_title.as_deref() {
        let title = title.trim();
        if !title.is_empty() {
            return title.to_string();
        }
    }

    if let Some(quote) = raw.quote.as_deref() {
        let quote = quote.trim();
        if !quote.is_empty() {
            // Keep titles short and readable for timeline surfaces.
            let mut title = quote.replace('\n', " ");
            if title.len() > 120 {
                title.truncate(120);
                title.push_str("...");
            }
            return title;
        }
    }

    let mut parts = Vec::new();
    let subject = raw.subject_text.trim();
    let predicate = raw.predicate_text.trim();
    if !subject.is_empty() {
        parts.push(subject.to_string());
    }
    if !predicate.is_empty() {
        parts.push(predicate.to_string());
    }

    if let Some(object_text) = raw.object_text.as_deref() {
        let object_text = object_text.trim();
        if !object_text.is_empty() {
            parts.push(object_text.to_string());
        }
    } else if let Some(literal) = raw.object_literal.as_ref() {
        let rendered = literal_to_event_title(literal);
        if !rendered.is_empty() {
            parts.push(rendered);
        }
    }

    let title = parts.join(" ");
    if title.is_empty() {
        "event".to_string()
    } else {
        title
    }
}

fn event_time_precision(raw: &RawAssertion) -> TimePrecision {
    raw.source_event_time_precision
        .unwrap_or(TimePrecision::Unknown)
}

fn event_auto_approve_threshold() -> f32 {
    std::env::var("EVENT_AUTO_APPROVE_THRESHOLD")
        .ok()
        .and_then(|raw| raw.parse::<f32>().ok())
        .filter(|value| (0.0..=1.0).contains(value))
        .unwrap_or(DEFAULT_EVENT_AUTO_APPROVE_THRESHOLD)
}

const fn flag_score(flag: bool) -> f32 {
    if flag {
        1.0
    } else {
        0.0
    }
}

#[cfg(test)]
#[path = "inline_promotion_tests.rs"]
mod tests;
