//! Inline promotion: deterministic code that takes structured extractor output
//! and creates entities, rels, attributes, and assertions atomically.
//!
//! This replaces the old multi-stage pipeline (commit -> resolve -> synthesize)
//! with a single pass after extraction.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Datelike, Timelike, Utc};
use sha2::{Digest, Sha256};

use elephant_outbox::{EventOperation, OutboxPayload, OutboxWriter, RecordRef};
use elephant_storage::{
    AssertionRepository, AttributeRepository, DocRepository, DocRevisionRepository,
    EntityRepository, EventRepository, FreshnessRepository, RelRepository, Repository,
    StorageClient,
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

use crate::error::PipelineError;
use crate::extraction::RawAssertion;
use crate::freshness::{record_claim_freshness, ClaimFreshnessObservation};
use crate::scheduler::promotion_disambiguation::{
    CandidateEntity, DisambiguationRequest, PromotionDisambiguationDecision, PromotionDisambiguator,
};

const DEFAULT_EVENT_AUTO_APPROVE_THRESHOLD: f32 = 0.85;
const CONTEXT_SNIPPET_MAX_CHARS: usize = 500;
const EVENT_METADATA_LINEAGE_KEY: &str = "lineage_key";
pub(crate) const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";
/// Trusted document metadata key a host stamps to classify how its own
/// projections may use the document. Elephant never writes it on documents;
/// it only reads it and copies it onto the timeline events it derives.
pub(crate) const HOST_PROJECTION_METADATA_KEY: &str = "homecore_projection";
/// Host projection class for internal review bookkeeping. Such documents keep
/// their assertions (history stays searchable) but never become household
/// timeline events, so they cannot surface as upcoming or pending items.
pub(crate) const HOST_PROJECTION_INTERNAL_REVIEW: &str = "internal_review";

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
) -> Result<PromotionResult, PipelineError> {
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

/// Promotes a batch of extracted assertions into entities, rels/attrs, and assertion records.
///
/// For each assertion:
/// 1. Creates or reuses subject entity
/// 2. Creates or reuses object entity (if entity-to-entity)
/// 3. Creates the assertion record (evidence layer, always)
/// 4. If significance != "skip": creates rel or attribute
///
/// Idempotency: uses `{doc_node_id}:{predicate}:{sha256(subject+object)}` as assertion key.
///
/// # Errors
///
/// Returns an error if entity validation fails due to a database error (not NotFound),
/// or if any record creation fails.
#[allow(clippy::too_many_lines, clippy::too_many_arguments)]
pub async fn promote_assertions_inline_with_options(
    storage: &StorageClient,
    space_id: &str,
    assertions: &[RawAssertion],
    work_run_id: &RecordId,
    doc_node_id: &RecordId,
    revision_id: &RecordId,
    actor: &str,
    options: &PromotionOptions,
) -> Result<PromotionResult, PipelineError> {
    let entity_repo = EntityRepository::new(storage.clone());
    let assertion_repo = AssertionRepository::new(storage.clone());
    let rel_repo = RelRepository::new(storage.clone());
    let attr_repo = AttributeRepository::new(storage.clone());
    let event_repo = EventRepository::new(storage.clone());
    let freshness_repo = FreshnessRepository::new(storage.clone());

    let mut result = PromotionResult::default();
    let mut outbox_records: Vec<RecordRef> = Vec::new();
    let event_auto_approve_threshold = event_auto_approve_threshold();

    // Attribution metadata and classification from the parent doc flow onto the
    // records this batch derives (loaded once per batch; missing doc = no-op).
    let inheritance = load_doc_inheritance(storage, space_id, revision_id).await?;
    let doc_protectees = inheritance.angel_protectees;
    let doc_security = inheritance.security.as_ref();
    let doc_host_projection = inheritance.host_projection;
    let suppress_timeline_events = doc_host_projection
        .as_ref()
        .and_then(serde_json::Value::as_str)
        == Some(HOST_PROJECTION_INTERNAL_REVIEW);

    for raw in assertions {
        let assertion_key = build_assertion_key(doc_node_id, &raw.predicate_text, raw);
        let event_lineage_key = raw
            .event_lineage_key
            .clone()
            .unwrap_or_else(|| assertion_key.clone());

        // Check idempotency — skip if assertion with this key already exists
        if let Some(existing_assertion) = assertion_repo
            .find_by_assertion_key(space_id, &assertion_key)
            .await
            .map_err(PipelineError::StorageError)?
        {
            enrich_existing_assertion_entities(
                &entity_repo,
                space_id,
                &existing_assertion,
                raw,
                &doc_protectees,
                &mut outbox_records,
            )
            .await?;

            // Internal review bookkeeping never refines (and so never revives)
            // a household timeline event.
            if !suppress_timeline_events {
                if let Some(mut existing_event) = find_event_for_lineage_refinement(
                    &event_repo,
                    space_id,
                    &event_lineage_key,
                    raw,
                )
                .await?
                {
                    let updated = apply_event_temporal_refinement(
                        &mut existing_event,
                        raw,
                        Some(Link::new(existing_assertion.base.id.clone())),
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
                    let evidence_times = [raw
                        .source_event_time
                        .unwrap_or(existing_assertion.observed_at)];
                    record_claim_freshness(
                        &freshness_repo,
                        &existing_event.base.id,
                        "event",
                        &existing_event.base,
                        ClaimFreshnessObservation {
                            evidence_times: &evidence_times,
                            evidence_strength: existing_event.confidence,
                            conflict_severity: flag_score(
                                existing_event.status == EventStatus::Rejected,
                            ),
                            corroboration_count: existing_event.assertion_refs.len(),
                        },
                    )
                    .await?;
                }
            }
            continue;
        }

        let context_snippet = assertion_context_snippet(raw);
        let disambiguator = options.disambiguator.as_deref();

        // 1. Resolve or create subject entity
        let subject_entity_link = resolve_or_create_entity(
            &entity_repo,
            space_id,
            raw.subject_entity_id.as_deref(),
            &raw.subject_text,
            raw.subject_entity_type.as_deref(),
            &context_snippet,
            disambiguator,
            &doc_protectees,
            actor,
            &mut result,
            &mut outbox_records,
        )
        .await?;

        // 2. Resolve or create object entity (only for entity-to-entity, not literals)
        let object_entity_link = if raw.object_literal.is_none() {
            let object_text = raw.object_text.as_deref().unwrap_or("");
            if object_text.is_empty() {
                None
            } else {
                Some(
                    resolve_or_create_entity(
                        &entity_repo,
                        space_id,
                        raw.object_entity_id.as_deref(),
                        object_text,
                        raw.object_entity_type.as_deref(),
                        &context_snippet,
                        disambiguator,
                        &doc_protectees,
                        actor,
                        &mut result,
                        &mut outbox_records,
                    )
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
        inherit_security(&mut assertion_base, doc_security);

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

        // 4. If significance == "skip", don't promote to rel/attr
        let is_skip = raw
            .significance
            .as_deref()
            .is_some_and(|s| s.eq_ignore_ascii_case("skip"));
        if is_skip {
            result.assertions_skipped += 1;
            continue;
        }

        // 5. Create a first-class domain event even when time is fuzzy or unknown,
        // unless the document is internal review bookkeeping. That path neither
        // creates nor refines an event, so reprocessing cannot resurrect one an
        // operator rejected or retired.
        if !suppress_timeline_events {
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
            inherit_security(&mut event_base, doc_security);

            let assertion_link = Link::new(assertion_id.clone());
            if let Some(mut existing_event) =
                find_event_for_lineage_refinement(&event_repo, space_id, &event_lineage_key, raw)
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
                record_claim_freshness(
                    &freshness_repo,
                    &existing_event.base.id,
                    "event",
                    &existing_event.base,
                    ClaimFreshnessObservation {
                        evidence_times: &event_evidence_times,
                        evidence_strength: existing_event.confidence,
                        conflict_severity: flag_score(
                            existing_event.status == EventStatus::Rejected,
                        ),
                        corroboration_count: existing_event.assertion_refs.len(),
                    },
                )
                .await?;
            } else {
                let mut event = Event {
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
                    source_refs: Vec::new(),
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
                if let (Some(projection), Some(metadata)) =
                    (&doc_host_projection, event.metadata.as_object_mut())
                {
                    metadata.insert(HOST_PROJECTION_METADATA_KEY.into(), projection.clone());
                }

                let event = event_repo
                    .create(event)
                    .await
                    .map_err(PipelineError::StorageError)?;
                let event_evidence_times = [raw.source_event_time.unwrap_or(now)];
                record_claim_freshness(
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
                )
                .await?;
                result.events_created += 1;
                outbox_records.push(RecordRef {
                    id: event_id,
                    table: "event".to_string(),
                    op: EventOperation::Create,
                });
            }
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
            inherit_security(&mut attr_base, doc_security);

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
            record_claim_freshness(
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
            )
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
            inherit_security(&mut rel_base, doc_security);

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

            let (relation, op) =
                merge_or_create_rel(&rel_repo, space_id, rel, evidence_ref, confidence_f32).await?;
            let relation_evidence_times = relation
                .evidence
                .iter()
                .map(|evidence| evidence.time_value)
                .collect::<Vec<_>>();
            record_claim_freshness(
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
            )
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
        let outbox = OutboxWriter::new(storage.clone());
        let mut payload = OutboxPayload::new(
            space_id.to_string(),
            "inline_promotion.completed".to_string(),
            actor.to_string(),
        );
        payload = payload.with_run_id(work_run_id.to_string());
        for record in outbox_records {
            payload.add_record(record);
        }
        outbox.write(payload).await?;
        result.outbox_events_written += 1;
    }

    Ok(result)
}

async fn merge_or_create_rel(
    rel_repo: &RelRepository,
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
        .next();

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
    // The incoming doc contributes evidence to a rel other docs also feed, so
    // widen the classification instead of adopting the newcomer's verbatim.
    escalate_security(&mut existing_rel.base.security, Some(&rel.base.security));

    let id = existing_rel.base.id.clone();
    rel_repo
        .update(space_id, &id, existing_rel.clone())
        .await
        .map_err(PipelineError::StorageError)?;
    Ok((existing_rel.clone(), EventOperation::Update))
}

/// Resolves an existing entity by ID, or creates a new one.
///
/// - If `entity_id` is provided and found in the same space: reuses it
///   (trusted extraction — exempt from the type guard).
/// - Otherwise, identity-key matches are filtered by type compatibility;
///   ambiguous or conflicting matches go to the optional disambiguator
///   (see [`resolve_identity_key_matches`]).
/// - If nothing is reused: creates a new entity.
/// - DB/network errors are propagated (not swallowed).
#[allow(clippy::too_many_arguments)]
async fn resolve_or_create_entity(
    entity_repo: &EntityRepository,
    space_id: &str,
    entity_id: Option<&str>,
    display_name: &str,
    entity_type_str: Option<&str>,
    context_snippet: &str,
    disambiguator: Option<&dyn PromotionDisambiguator>,
    doc_protectees: &[String],
    actor: &str,
    result: &mut PromotionResult,
    outbox_records: &mut Vec<RecordRef>,
) -> Result<Link<Entity>, PipelineError> {
    // Try to reuse existing entity if ID was provided
    if let Some(id_str) = entity_id {
        if let Ok(record_id) = RecordId::from_str(id_str) {
            match entity_repo.get(space_id, &record_id).await {
                Ok(Some(entity)) => {
                    return reuse_entity_for_display_name(
                        entity_repo,
                        space_id,
                        entity,
                        display_name,
                        None,
                        doc_protectees,
                        outbox_records,
                    )
                    .await;
                }
                Ok(None) => {
                    // NotFound or soft-deleted — fall through
                }
                Err(e) => {
                    // DB error — propagate, don't swallow
                    return Err(PipelineError::StorageError(e));
                }
            }
        }
        // Invalid RecordId format — fall through
    }

    // Map the extractor type to the DB vocabulary up front; the type guard
    // and entity creation both use the normalized form.
    let entity_type = entity_type_str
        .map(|t| normalize_entity_type(t))
        .unwrap_or_else(|| EntityType::new("topic"));

    // Try to find existing entities by identity key (lowercased display_name)
    let identity_key = display_name.to_lowercase();
    let matches = entity_repo
        .find_by_identity_key_all(space_id, &identity_key)
        .await
        .map_err(PipelineError::StorageError)?;
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
    let base = BaseFields::new(entity_id.clone(), space_id.to_string(), actor.to_string());

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
    entity_repo: &EntityRepository,
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
            .await;
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
            .await;
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
fn entity_types_compatible(stored: &EntityType, incoming: &EntityType) -> bool {
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
) -> Option<PromotionDisambiguationDecision> {
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
    match disambiguator.disambiguate(&request).await {
        Ok(decision) => decision,
        Err(error) => {
            tracing::warn!(
                %error,
                mention,
                "promotion disambiguator failed; falling back to deterministic entity-reuse rules"
            );
            None
        }
    }
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

async fn enrich_existing_assertion_entities(
    entity_repo: &EntityRepository,
    space_id: &str,
    assertion: &Assertion,
    raw: &RawAssertion,
    doc_protectees: &[String],
    outbox_records: &mut Vec<RecordRef>,
) -> Result<(), PipelineError> {
    if let Some(subject_entity) = &assertion.subject_entity {
        if let Some(entity) = entity_repo
            .get(space_id, subject_entity.id())
            .await
            .map_err(PipelineError::StorageError)?
        {
            reuse_entity_for_display_name(
                entity_repo,
                space_id,
                entity,
                &raw.subject_text,
                None,
                doc_protectees,
                outbox_records,
            )
            .await?;
        }
    }

    if let (Some(object_entity), Some(object_text)) =
        (&assertion.object_entity, raw.object_text.as_deref())
    {
        if let Some(entity) = entity_repo
            .get(space_id, object_entity.id())
            .await
            .map_err(PipelineError::StorageError)?
        {
            reuse_entity_for_display_name(
                entity_repo,
                space_id,
                entity,
                object_text,
                None,
                doc_protectees,
                outbox_records,
            )
            .await?;
        }
    }

    Ok(())
}

async fn reuse_entity_for_display_name(
    entity_repo: &EntityRepository,
    space_id: &str,
    entity: Entity,
    display_name: &str,
    incoming_type: Option<&EntityType>,
    doc_protectees: &[String],
    outbox_records: &mut Vec<RecordRef>,
) -> Result<Link<Entity>, PipelineError> {
    let mut entity = follow_entity_redirect(entity_repo, space_id, entity).await?;
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
    entity_repo: &EntityRepository,
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

/// Loads the parent doc's `metadata.angel_protectees` for a revision.
///
/// A missing revision or doc is tolerated as "no attribution" so promotion
/// never fails on a lookup that only feeds metadata propagation.
async fn load_doc_inheritance(
    storage: &StorageClient,
    space_id: &str,
    revision_id: &RecordId,
) -> Result<DocInheritance, PipelineError> {
    let revision_repo = DocRevisionRepository::new(storage.clone());
    let Some(revision) = revision_repo
        .get_by_id(revision_id)
        .await
        .map_err(PipelineError::StorageError)?
    else {
        return Ok(DocInheritance::default());
    };

    let doc_repo = DocRepository::new(storage.clone());
    let Some(doc) = doc_repo
        .get(space_id, revision.doc.id())
        .await
        .map_err(PipelineError::StorageError)?
    else {
        return Ok(DocInheritance::default());
    };

    Ok(DocInheritance {
        angel_protectees: angel_protectees_from_metadata(&doc.metadata),
        security: Some(doc.base.security.clone()),
        host_projection: doc
            .metadata
            .get(HOST_PROJECTION_METADATA_KEY)
            .filter(|value| !value.is_null())
            .cloned(),
    })
}

/// Classification and attribution a document passes down to the records derived
/// from it.
#[derive(Debug, Clone, Default)]
struct DocInheritance {
    /// Angel protectees named by the parent doc's metadata.
    angel_protectees: Vec<String>,
    /// The parent doc's security envelope. `None` when the doc or its revision
    /// could not be loaded, in which case derived records keep the baseline
    /// envelope from [`BaseFields::new`] rather than a guessed one.
    security: Option<Security>,
    /// The parent doc's host projection class (`metadata.homecore_projection`);
    /// JSON null counts as absent.
    host_projection: Option<serde_json::Value>,
}

/// Stamps the parent document's classification onto a record derived from it.
///
/// Derived records are only as shareable as the evidence behind them: an
/// assertion extracted from an owner-only, `no_voice` document must not be a
/// household-open, speakable fact. Before this existed, every derived record
/// carried the `BaseFields::new` baseline (`policy_id: "default"`, `level: 0`,
/// no labels, no handling) no matter what the source said, so document-level
/// classification could not gate access to the facts at all.
///
/// A `None` envelope leaves the record untouched. That is the missing-doc path,
/// which for extraction is an invariant violation rather than a normal case
/// (extraction runs *from* a revision), and it keeps fakes in tests on the
/// baseline envelope.
pub(crate) fn inherit_security(base: &mut BaseFields, inherited: Option<&Security>) {
    if let Some(security) = inherited {
        base.security = security.clone();
    }
}

/// Merges an incoming document's classification into a record that several
/// documents contribute to, escalating only.
///
/// Rels are slot-merged across documents ([`merge_or_create_rel`]), so the same
/// rel can carry evidence from a household-open note and an owner-only legal
/// document. Taking the incoming envelope verbatim would let whichever document
/// happened to be extracted last decide the classification, and a later
/// low-sensitivity mention would silently *declassify* the rel. So this widens
/// and never narrows: the level is the max, labels and handling are unioned, and
/// a real `policy_id` displaces the `"default"` placeholder but never another
/// real one.
///
/// Same escalate-only shape as the confidence (`max`) and status
/// (`Candidate` -> `Approved`) merges alongside it.
pub(crate) fn escalate_security(existing: &mut Security, incoming: Option<&Security>) {
    let Some(incoming) = incoming else {
        return;
    };

    existing.level = existing.level.max(incoming.level);

    if existing.policy_id == "default" && incoming.policy_id != "default" {
        existing.policy_id = incoming.policy_id.clone();
    }

    for label in &incoming.labels {
        if !existing.labels.contains(label) {
            existing.labels.push(label.clone());
        }
    }
    existing.labels.sort();

    for caveat in &incoming.handling {
        if !existing.handling.contains(caveat) {
            existing.handling.push(caveat.clone());
        }
    }
    existing.handling.sort();
}

/// Builds the idempotency key for an assertion.
fn build_assertion_key(doc_node_id: &RecordId, predicate: &str, raw: &RawAssertion) -> String {
    let object_part = if let Some(ref text) = raw.object_text {
        text.clone()
    } else if let Some(ref literal) = raw.object_literal {
        format!("{literal:?}")
    } else {
        String::new()
    };

    let mut hasher = Sha256::new();
    hasher.update(raw.subject_text.as_bytes());
    hasher.update(b"|");
    hasher.update(object_part.as_bytes());
    let hash = hex::encode(hasher.finalize());

    format!("{doc_node_id}:{predicate}:{hash}")
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
    event_repo: &EventRepository,
    space_id: &str,
    lineage_key: &str,
    raw: &RawAssertion,
) -> Result<Option<Event>, crate::error::PipelineError> {
    let candidates = event_repo
        .find_all_by_lineage_key(space_id, lineage_key)
        .await
        .map_err(PipelineError::StorageError)?;
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
    // The status here is only an automatic confidence-threshold recompute
    // (Candidate vs Approved). A rejection is a review decision, and more
    // support for the same lineage must not silently overturn it. Keep a
    // rejected event rejected; it still records the support and time
    // refinements so the audit trail stays complete.
    if event.status != EventStatus::Rejected {
        let desired_status = if event.confidence >= auto_approve_threshold {
            EventStatus::Approved
        } else {
            EventStatus::Candidate
        };
        if event.status != desired_status {
            event.status = desired_status;
            changed = true;
        }
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
fn normalize_entity_type(raw: &str) -> EntityType {
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
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]

    use super::*;
    use crate::extraction::RawAssertion;
    use elephant_storage::{SearchCriteria, SearchRepository};
    use elephant_types::records::assertion::EvidenceSpan;
    use elephant_types::records::base::Security;
    use elephant_types::records::doc::Doc;
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};

    const SCHEMA: &str = r"
        DEFINE TABLE entity SCHEMALESS;
        DEFINE TABLE assertion SCHEMALESS;
        DEFINE TABLE rel SCHEMALESS;
        DEFINE TABLE attribute SCHEMALESS;
        DEFINE TABLE event SCHEMALESS;
        DEFINE TABLE doc SCHEMALESS;
        DEFINE TABLE doc_revision SCHEMALESS;
        DEFINE TABLE system_event SCHEMALESS;
        DEFINE INDEX system_event_space_seq ON TABLE system_event FIELDS space_id, seq UNIQUE;
    ";

    fn make_evidence_span() -> EvidenceSpan {
        EvidenceSpan {
            revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
            node: None,
            start: 0,
            end: 25,
            quote_hash: "testhash".to_string(),
        }
    }

    fn make_raw_assertion(subject: &str, predicate: &str, object: &str) -> RawAssertion {
        RawAssertion {
            subject_text: subject.to_string(),
            predicate_text: predicate.to_string(),
            object_text: Some(object.to_string()),
            object_literal: None,
            evidence_span: make_evidence_span(),
            confidence: 0.95,
            quote: Some("test quote".to_string()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: Some("org".to_string()),
            significance: Some("high".to_string()),
        }
    }

    fn make_literal_assertion(subject: &str, predicate: &str, value: &str) -> RawAssertion {
        RawAssertion {
            subject_text: subject.to_string(),
            predicate_text: predicate.to_string(),
            object_text: None,
            object_literal: Some(Literal::String(value.to_string())),
            evidence_span: make_evidence_span(),
            confidence: 0.90,
            quote: Some("literal quote".to_string()),
            source_event_time: None,
            source_event_time_precision: None,
            event_title: None,
            event_lineage_key: None,
            subject_entity_id: None,
            subject_entity_type: Some("person".to_string()),
            object_entity_id: None,
            object_entity_type: None,
            significance: Some("medium".to_string()),
        }
    }

    /// Creates a doc with the given metadata plus an active revision pointing
    /// at it, returning the revision id.
    async fn create_doc_with_revision(
        storage: &StorageClient,
        metadata: serde_json::Value,
    ) -> RecordId {
        create_doc_with_revision_and_security(storage, metadata, None).await
    }

    /// As [`create_doc_with_revision`], with an explicit classification on the
    /// parent doc so inheritance onto derived records can be asserted.
    async fn create_doc_with_revision_and_security(
        storage: &StorageClient,
        metadata: serde_json::Value,
        security: Option<Security>,
    ) -> RecordId {
        let doc_repo = DocRepository::new(storage.clone());
        let revision_repo = DocRevisionRepository::new(storage.clone());

        let doc_id = RecordId::new_v4(TableName::Doc);
        let mut doc_base =
            BaseFields::new(doc_id.clone(), "test_space".to_string(), "test".to_string());
        if let Some(security) = security {
            doc_base.security = security;
        }
        let doc = Doc {
            base: doc_base,
            title: "Test Doc".to_string(),
            text: "test text".to_string(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: format!("rendition-{doc_id}"),
            metadata,
        };
        doc_repo.create(doc).await.expect("create doc");

        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let revision = DocRevision {
            base: BaseFields::new(
                revision_id.clone(),
                "test_space".to_string(),
                "test".to_string(),
            ),
            doc: Link::new(doc_id),
            text: "test text".to_string(),
            text_hash: "hash".to_string(),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        revision_repo
            .create(revision)
            .await
            .expect("create revision");

        revision_id
    }

    #[tokio::test]
    async fn test_promote_creates_entity_for_new_subject() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        // Should create 2 entities (Alice + Acme Corp), 1 rel, 1 assertion
        assert_eq!(result.entities_created, 2);
        assert_eq!(result.rels_created, 1);
        assert_eq!(result.assertions_recorded, 1);
        assert_eq!(result.attributes_created, 0);
    }

    #[tokio::test]
    async fn test_promote_reuses_existing_entity() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        // Pre-create an entity
        let entity_repo = EntityRepository::new(storage.clone());
        let existing_id = RecordId::new_v4(TableName::Entity);
        let entity = Entity {
            base: BaseFields::new(
                existing_id.clone(),
                "test_space".to_string(),
                "test".to_string(),
            ),
            entity_type: EntityType::new("person"),
            display_name: "Alice".to_string(),
            aliases: vec![],
            summary: None,
            handles: vec![],
            identity_keys: vec!["alice".to_string()],
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::Value::Object(serde_json::Map::new()),
        };
        entity_repo.create(entity).await.expect("create entity");

        // Create assertion referencing existing entity
        let mut raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        raw.subject_entity_id = Some(existing_id.to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        // Only 1 entity created (Acme Corp); Alice was reused
        assert_eq!(result.entities_created, 1);
        assert_eq!(result.rels_created, 1);
        assert_eq!(result.assertions_recorded, 1);
    }

    #[tokio::test]
    async fn test_promote_propagates_angel_protectees_from_doc_to_entities() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let entity_repo = EntityRepository::new(storage.clone());
        let work_run_id = RecordId::new_v4(TableName::WorkRun);

        // First doc attributed to child-a creates the entities.
        let revision_a = create_doc_with_revision(
            &storage,
            serde_json::json!({ "angel_protectees": ["child-a"] }),
        )
        .await;
        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_raw_assertion("Alice", "works_at", "Acme Corp")],
            &work_run_id,
            &RecordId::new_v4(TableName::DocNode),
            &revision_a,
            "test_actor",
        )
        .await
        .expect("promote first doc");

        let alice = entity_repo
            .find_by_identity_key_all("test_space", "alice")
            .await
            .expect("find alice")
            .pop()
            .expect("alice exists");
        assert_eq!(
            alice.metadata[ANGEL_PROTECTEES_METADATA_KEY],
            serde_json::json!(["child-a"])
        );

        // Second doc attributed to child-b reuses Alice: ids union, sorted.
        let revision_b = create_doc_with_revision(
            &storage,
            serde_json::json!({ "angel_protectees": ["child-b"] }),
        )
        .await;
        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_raw_assertion("Alice", "mentors", "Bob")],
            &work_run_id,
            &RecordId::new_v4(TableName::DocNode),
            &revision_b,
            "test_actor",
        )
        .await
        .expect("promote second doc");

        let alice = entity_repo
            .find_by_identity_key_all("test_space", "alice")
            .await
            .expect("find alice again")
            .pop()
            .expect("alice still exists");
        assert_eq!(
            alice.metadata[ANGEL_PROTECTEES_METADATA_KEY],
            serde_json::json!(["child-a", "child-b"])
        );

        // The entity created by the second doc only carries child-b.
        let bob = entity_repo
            .find_by_identity_key_all("test_space", "bob")
            .await
            .expect("find bob")
            .pop()
            .expect("bob exists");
        assert_eq!(
            bob.metadata[ANGEL_PROTECTEES_METADATA_KEY],
            serde_json::json!(["child-b"])
        );
    }

    #[tokio::test]
    async fn test_promote_without_doc_protectees_leaves_entity_metadata_unset() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let revision_id =
            create_doc_with_revision(&storage, serde_json::json!({ "other": "value" })).await;
        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_raw_assertion("Alice", "works_at", "Acme Corp")],
            &RecordId::new_v4(TableName::WorkRun),
            &RecordId::new_v4(TableName::DocNode),
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        let entity_repo = EntityRepository::new(storage.clone());
        let alice = entity_repo
            .find_by_identity_key_all("test_space", "alice")
            .await
            .expect("find alice")
            .pop()
            .expect("alice exists");
        assert!(alice.metadata.get(ANGEL_PROTECTEES_METADATA_KEY).is_none());
    }

    fn owner_only_security() -> Security {
        Security {
            policy_id: "household-v1".to_string(),
            level: 2,
            labels: vec!["audience:owner".to_string(), "topic:estate".to_string()],
            handling: vec!["no_voice".to_string()],
        }
    }

    /// An owner-only, `no_voice` document must not yield household-open,
    /// speakable facts. Covers the assertion (evidence layer), the event, and
    /// the attribute in one pass.
    #[tokio::test]
    async fn test_derived_records_inherit_doc_classification() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let revision_id = create_doc_with_revision_and_security(
            &storage,
            serde_json::json!({}),
            Some(owner_only_security()),
        )
        .await;

        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_literal_assertion(
                "Grete Friis estate",
                "has_share",
                "50/50",
            )],
            &RecordId::new_v4(TableName::WorkRun),
            &RecordId::new_v4(TableName::DocNode),
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        let expected = owner_only_security();

        let assertions = AssertionRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .expect("list assertions");
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].base.security, expected);

        let attributes = AttributeRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .expect("list attributes");
        assert_eq!(attributes.len(), 1);
        assert_eq!(attributes[0].base.security, expected);

        let events = EventRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].base.security, expected);
    }

    /// Negative control: without a resolvable parent doc there is nothing to
    /// inherit, so records keep the `BaseFields::new` baseline. Guards against
    /// the inheritance helper inventing a classification.
    #[tokio::test]
    async fn test_promote_without_doc_keeps_baseline_classification() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_literal_assertion("Alice", "has_title", "engineer")],
            &RecordId::new_v4(TableName::WorkRun),
            &RecordId::new_v4(TableName::DocNode),
            &RecordId::new_v4(TableName::DocRevision),
            "test_actor",
        )
        .await
        .expect("promote");

        let assertions = AssertionRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .expect("list assertions");
        assert_eq!(assertions.len(), 1);
        assert_eq!(assertions[0].base.security.policy_id, "default");
        assert_eq!(assertions[0].base.security.level, 0);
        assert!(assertions[0].base.security.labels.is_empty());
        assert!(assertions[0].base.security.handling.is_empty());
    }

    /// A rel is slot-merged across docs, so a later low-sensitivity mention of
    /// the same fact must not declassify it.
    #[tokio::test]
    async fn test_rel_merge_escalates_classification_and_never_downgrades() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let sensitive_revision = create_doc_with_revision_and_security(
            &storage,
            serde_json::json!({}),
            Some(owner_only_security()),
        )
        .await;
        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_raw_assertion("Alice", "works_at", "Acme Corp")],
            &RecordId::new_v4(TableName::WorkRun),
            &RecordId::new_v4(TableName::DocNode),
            &sensitive_revision,
            "test_actor",
        )
        .await
        .expect("promote sensitive");

        // Same slot, this time from a household-open doc carrying one new label.
        let open_revision = create_doc_with_revision_and_security(
            &storage,
            serde_json::json!({}),
            Some(Security {
                policy_id: "household-v1".to_string(),
                level: 0,
                labels: vec!["topic:work".to_string()],
                handling: vec![],
            }),
        )
        .await;
        promote_assertions_inline(
            &storage,
            "test_space",
            &[make_raw_assertion("Alice", "works_at", "Acme Corp")],
            &RecordId::new_v4(TableName::WorkRun),
            &RecordId::new_v4(TableName::DocNode),
            &open_revision,
            "test_actor",
        )
        .await
        .expect("promote open");

        let rels = RelRepository::new(storage.clone())
            .find_by_type("works_at")
            .await
            .expect("find rels");
        assert_eq!(rels.len(), 1, "same slot should merge, not duplicate");
        let security = &rels[0].base.security;

        assert_eq!(
            security.level, 2,
            "must not be declassified to the open doc"
        );
        assert_eq!(security.handling, vec!["no_voice".to_string()]);
        assert_eq!(
            security.labels,
            vec![
                "audience:owner".to_string(),
                "topic:estate".to_string(),
                "topic:work".to_string(),
            ],
            "labels union, sorted"
        );
    }

    #[tokio::test]
    async fn test_promote_creates_rel_for_entity_object() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        assert_eq!(result.rels_created, 1);
        assert_eq!(result.attributes_created, 0);

        // Verify rel exists in the DB
        let rel_repo = RelRepository::new(storage.clone());
        let rels = rel_repo.find_by_type("works_at").await.expect("find rels");
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].rel_type, "works_at");
    }

    #[tokio::test]
    async fn test_promote_creates_attribute_for_literal_object() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_literal_assertion("Alice", "job_title", "CEO");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        assert_eq!(result.entities_created, 1); // Only Alice
        assert_eq!(result.attributes_created, 1);
        assert_eq!(result.rels_created, 0);
        assert_eq!(result.assertions_recorded, 1);
    }

    #[tokio::test]
    async fn test_promote_idempotent() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        // First promotion
        let r1 = promote_assertions_inline(
            &storage,
            "test_space",
            std::slice::from_ref(&raw),
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("first promote");

        assert_eq!(r1.assertions_recorded, 1);
        assert_eq!(r1.entities_created, 2);

        // Second promotion — same assertion, same doc_node_id
        let r2 = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("second promote");

        // Should be skipped due to idempotency
        assert_eq!(r2.assertions_recorded, 0);
        assert_eq!(r2.entities_created, 0);
        assert_eq!(r2.rels_created, 0);
    }

    #[tokio::test]
    async fn test_promote_merges_repeated_relationship_evidence() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let first_doc_node_id = RecordId::new_v4(TableName::DocNode);
        let second_doc_node_id = RecordId::new_v4(TableName::DocNode);

        let first = promote_assertions_inline(
            &storage,
            "test_space",
            std::slice::from_ref(&raw),
            &work_run_id,
            &first_doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("first promote");
        let second = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &second_doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("second promote");

        assert_eq!(first.assertions_recorded, 1);
        assert_eq!(first.rels_created, 1);
        assert_eq!(second.assertions_recorded, 1);
        assert_eq!(second.rels_created, 0);

        let rel_repo = RelRepository::new(storage.clone());
        let rels = rel_repo.find_by_type("works_at").await.expect("find rels");
        assert_eq!(rels.len(), 1);
        assert_eq!(rels[0].evidence.len(), 2);
    }

    #[tokio::test]
    async fn test_promote_skips_low_significance() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut raw = make_raw_assertion("Alice", "mentioned_in", "news article");
        raw.significance = Some("skip".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        // Assertion IS recorded (evidence layer), but NOT promoted to rel/attr
        assert_eq!(result.assertions_recorded, 1);
        assert_eq!(result.assertions_skipped, 1);
        assert_eq!(result.rels_created, 0);
        assert_eq!(result.attributes_created, 0);
        // Entities are still created for the assertion record
        assert_eq!(result.entities_created, 2);
    }

    #[tokio::test]
    async fn test_identity_key_dedup_across_batches() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();

        let work_run_id = RecordId::new_v4(TableName::WorkItem);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        // Batch 1 from chunk A: "Alice works_at Acme"
        let node_a = RecordId::new_v4(TableName::DocNode);
        let batch_a = vec![make_raw_assertion("Alice", "works_at", "Acme")];

        let result_a = promote_assertions_inline(
            &storage,
            "test_space",
            &batch_a,
            &work_run_id,
            &node_a,
            &revision_id,
            "test",
        )
        .await
        .unwrap();

        assert_eq!(
            result_a.entities_created, 2,
            "First batch should create Alice + Acme"
        );

        // Batch 2 from chunk B: "Alice knows Bob"
        let node_b = RecordId::new_v4(TableName::DocNode);
        let batch_b = vec![make_raw_assertion("Alice", "knows", "Bob")];

        let result_b = promote_assertions_inline(
            &storage,
            "test_space",
            &batch_b,
            &work_run_id,
            &node_b,
            &revision_id,
            "test",
        )
        .await
        .unwrap();

        // Alice should be reused (identity key "alice" already exists), only Bob is new
        assert_eq!(
            result_b.entities_created, 1,
            "Second batch should only create Bob, reusing Alice"
        );

        // Verify total entity count in DB
        let entity_repo = EntityRepository::new(storage.clone());
        let all = entity_repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space".to_string())
                    .with_limit(100),
            )
            .await
            .unwrap();
        assert_eq!(
            all.results.len(),
            3,
            "Should have exactly 3 entities: Alice, Acme, Bob"
        );
    }

    #[tokio::test]
    async fn test_identity_key_dedup_case_insensitive() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();

        let work_run_id = RecordId::new_v4(TableName::WorkItem);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        // Batch 1: "Alice Chen" (title case)
        let node_a = RecordId::new_v4(TableName::DocNode);
        let batch_a = vec![make_raw_assertion("Alice Chen", "works_at", "Acme")];

        let result_a = promote_assertions_inline(
            &storage,
            "test_space",
            &batch_a,
            &work_run_id,
            &node_a,
            &revision_id,
            "test",
        )
        .await
        .unwrap();
        assert_eq!(result_a.entities_created, 2);

        // Batch 2: "alice chen" (lowercase) — should reuse
        let node_b = RecordId::new_v4(TableName::DocNode);
        let mut assertion = make_raw_assertion("alice chen", "met", "Bob");
        assertion.object_entity_type = Some("person".to_string());
        let batch_b = vec![assertion];

        let result_b = promote_assertions_inline(
            &storage,
            "test_space",
            &batch_b,
            &work_run_id,
            &node_b,
            &revision_id,
            "test",
        )
        .await
        .unwrap();

        // "alice chen" matches identity key "alice chen" from "Alice Chen" — reuse
        assert_eq!(
            result_b.entities_created, 1,
            "Only Bob should be new, alice chen should reuse Alice Chen"
        );
    }

    #[tokio::test]
    async fn test_reused_entity_is_enriched_with_observed_name() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let entity_id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(
            entity_id.clone(),
            "test_space".to_string(),
            "test".to_string(),
        );
        let mut entity = Entity::new(
            base,
            EntityType::new("person"),
            "alex@example.test".to_string(),
        );
        entity.aliases = vec!["Alex".to_string()];
        entity.identity_keys = vec!["alex".to_string(), "alex@example.test".to_string()];
        entity_repo.create(entity.clone()).await.unwrap();

        let mut outbox_records = Vec::new();
        let link = reuse_entity_for_display_name(
            &entity_repo,
            "test_space",
            entity,
            "Alex Example",
            None,
            &[],
            &mut outbox_records,
        )
        .await
        .unwrap();

        assert_eq!(link.id(), &entity_id);
        let stored = entity_repo
            .get("test_space", &entity_id)
            .await
            .unwrap()
            .unwrap();
        assert!(stored.aliases.iter().any(|alias| alias == "Alex Example"));
        assert!(stored
            .aliases
            .iter()
            .any(|alias| alias == "alex@example.test"));
        assert!(stored.identity_keys.iter().any(|key| key == "alex example"));
        assert_eq!(stored.display_name, "Alex Example");
        assert_eq!(outbox_records.len(), 1);
        assert_eq!(outbox_records[0].op, EventOperation::Update);
    }

    #[tokio::test]
    async fn test_reused_merged_entity_follows_redirect_before_enrichment() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let active_id = RecordId::new_v4(TableName::Entity);
        let active_base = BaseFields::new(
            active_id.clone(),
            "test_space".to_string(),
            "test".to_string(),
        );
        let mut active = Entity::new(
            active_base,
            EntityType::new("person"),
            "alex@example.test".to_string(),
        );
        active.identity_keys = vec!["alex@example.test".to_string()];
        entity_repo.create(active).await.unwrap();

        let merged_id = RecordId::new_v4(TableName::Entity);
        let merged_base = BaseFields::new(
            merged_id.clone(),
            "test_space".to_string(),
            "test".to_string(),
        );
        let mut merged = Entity::new(merged_base, EntityType::new("person"), "Alex".to_string());
        merged.status = EntityStatus::Merged;
        merged.redirect_to = Some(Link::new(active_id.clone()));
        entity_repo.create(merged.clone()).await.unwrap();

        let mut outbox_records = Vec::new();
        let link = reuse_entity_for_display_name(
            &entity_repo,
            "test_space",
            merged,
            "Alex Example",
            None,
            &[],
            &mut outbox_records,
        )
        .await
        .unwrap();

        assert_eq!(link.id(), &active_id);
        let active = entity_repo
            .get("test_space", &active_id)
            .await
            .unwrap()
            .unwrap();
        assert!(active.aliases.iter().any(|alias| alias == "Alex Example"));
        assert!(active.identity_keys.iter().any(|key| key == "alex example"));
        assert_eq!(active.display_name, "Alex Example");
    }

    #[tokio::test]
    async fn test_idempotent_reprocess_enriches_existing_assertion_entity_names() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());
        let assertion_repo = AssertionRepository::new(storage.clone());

        let work_run_id = RecordId::new_v4(TableName::WorkItem);
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);

        let entity_id = RecordId::new_v4(TableName::Entity);
        let base = BaseFields::new(
            entity_id.clone(),
            "test_space".to_string(),
            "test".to_string(),
        );
        let mut entity = Entity::new(
            base,
            EntityType::new("person"),
            "alex@example.test".to_string(),
        );
        entity.aliases = vec!["Alex".to_string()];
        entity.identity_keys = vec!["alex".to_string(), "alex@example.test".to_string()];
        entity_repo.create(entity).await.unwrap();

        let mut raw = make_literal_assertion("Alex Example", "has email", "alex@example.test");
        raw.subject_entity_type = Some("person".to_string());
        let assertion_key = build_assertion_key(&doc_node_id, &raw.predicate_text, &raw);
        let now = Utc::now();
        let mut assertion_base = BaseFields::new(
            RecordId::new_v4(TableName::Assertion),
            "test_space".to_string(),
            "test".to_string(),
        );
        assertion_base.subjects.push(Link::new(entity_id.clone()));
        assertion_base.subjects_state = SubjectsState::Complete;

        assertion_repo
            .create(Assertion {
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
                assertion_key,
                resolution_status: ResolutionStatus::Resolved,
                resolution_method: Some("inline_promotion".to_string()),
                resolution_confidence: Some(raw.confidence as f32),
                retry_state: AssertionRetryState::default(),
                subject_entity: Some(Link::new(entity_id)),
                object_entity: None,
                confidence: raw.confidence as f32,
                subject_entity_type: raw.subject_entity_type.clone(),
                object_entity_type: raw.object_entity_type.clone(),
                significance: raw.significance.clone(),
                lifecycle_tier: "active".to_string(),
            })
            .await
            .unwrap();

        let second = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test",
        )
        .await
        .unwrap();
        assert_eq!(second.entities_created, 0);

        let results = entity_repo
            .search(
                &SearchCriteria::new()
                    .with_space_id("test_space")
                    .with_query("Alex Example"),
            )
            .await
            .unwrap();

        assert_eq!(results.results.len(), 1);
        assert_eq!(results.results[0].display_name, "Alex Example");
        assert!(results.results[0]
            .aliases
            .iter()
            .any(|alias| alias == "Alex Example"));
    }

    #[tokio::test]
    async fn test_promote_writes_outbox_events_for_truth_maintenance() {
        use elephant_storage::SystemEventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        // Should have created entities + rel + written outbox event
        assert_eq!(result.entities_created, 2);
        assert_eq!(result.rels_created, 1);
        assert_eq!(result.outbox_events_written, 1);

        // Verify system_event was written
        let event_repo = SystemEventRepository::new(storage.clone());
        let events = event_repo
            .get_after_seq("test_space", 0, 10)
            .await
            .expect("get events");
        assert_eq!(events.len(), 1, "Should have exactly 1 outbox event");
        assert_eq!(events[0].event_type, "inline_promotion.completed");

        // Verify the event contains the right records for truth maintenance
        let payload: elephant_outbox::OutboxPayload =
            serde_json::from_value(events[0].payload.clone()).expect("parse payload");
        assert_eq!(payload.space_id, "test_space");
        assert_eq!(payload.actor, "test_actor");
        assert!(payload.run_id.is_some());

        // Should have 4 records: 2 entities + 1 event + 1 rel
        assert_eq!(
            payload.records.len(),
            4,
            "Should have 2 entity + 1 event + 1 rel records"
        );

        let entity_records: Vec<_> = payload
            .records
            .iter()
            .filter(|r| r.table == "entity")
            .collect();
        let event_records: Vec<_> = payload
            .records
            .iter()
            .filter(|r| r.table == "event")
            .collect();
        let rel_records: Vec<_> = payload
            .records
            .iter()
            .filter(|r| r.table == "rel")
            .collect();
        assert_eq!(entity_records.len(), 2);
        assert_eq!(event_records.len(), 1);
        assert_eq!(rel_records.len(), 1);

        // Verify truth maintenance can parse these into trigger events
        let triggers = crate::truth_maintenance::trigger_events_from_outbox_payload(&payload);
        assert_eq!(triggers.len(), 4, "Should produce 4 trigger events");
    }

    #[tokio::test]
    async fn test_promote_writes_attribute_outbox_event() {
        use elephant_storage::SystemEventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let raw = make_literal_assertion("Alice", "job_title", "CEO");
        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        assert_eq!(result.entities_created, 1); // Alice only
        assert_eq!(result.attributes_created, 1);
        assert_eq!(result.outbox_events_written, 1);

        // Verify outbox payload contains entity + attribute records
        let event_repo = SystemEventRepository::new(storage.clone());
        let events = event_repo
            .get_after_seq("test_space", 0, 10)
            .await
            .expect("get events");
        let payload: elephant_outbox::OutboxPayload =
            serde_json::from_value(events[0].payload.clone()).expect("parse payload");

        let attr_records: Vec<_> = payload
            .records
            .iter()
            .filter(|r| r.table == "attribute")
            .collect();
        assert_eq!(attr_records.len(), 1, "Should have 1 attribute record");
    }

    #[tokio::test]
    async fn test_promote_no_outbox_for_skipped_assertions() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut raw = make_raw_assertion("Alice", "mentioned_in", "news");
        raw.significance = Some("skip".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        // Skipped assertions still create entities but no rel/attr
        assert_eq!(result.assertions_skipped, 1);
        // Entities ARE created (for the assertion record), so outbox events should fire
        assert_eq!(result.entities_created, 2);
        assert_eq!(result.outbox_events_written, 1);

        // Verify only entity records in outbox (no rel or attribute)
        use elephant_storage::SystemEventRepository;
        let event_repo = SystemEventRepository::new(storage.clone());
        let events = event_repo
            .get_after_seq("test_space", 0, 10)
            .await
            .expect("get events");
        let payload: elephant_outbox::OutboxPayload =
            serde_json::from_value(events[0].payload.clone()).expect("parse payload");

        let rel_count = payload.records.iter().filter(|r| r.table == "rel").count();
        let attr_count = payload
            .records
            .iter()
            .filter(|r| r.table == "attribute")
            .count();
        assert_eq!(
            rel_count, 0,
            "Skipped assertions should not create rel events"
        );
        assert_eq!(
            attr_count, 0,
            "Skipped assertions should not create attr events"
        );
    }

    #[tokio::test]
    async fn test_choke_001_inline_promotion_persists_event_and_outbox_ref() {
        use elephant_storage::{EventRepository, SystemEventRepository};

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut raw = make_raw_assertion("Alice", "announced", "Project Phoenix");
        raw.source_event_time = Some(
            chrono::DateTime::parse_from_rfc3339("2024-01-15T09:00:00Z")
                .expect("parse time")
                .with_timezone(&Utc),
        );
        raw.event_title = Some("Alice announced Project Phoenix".to_string());
        raw.confidence = 0.87;

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        assert_eq!(result.events_created, 1, "Expected exactly one event");

        let event_repo = EventRepository::new(storage.clone());
        let events = event_repo
            .list_recent("test_space", 10)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1, "Expected persisted event");
        let event = &events[0];
        assert_eq!(event.title, "Alice announced Project Phoenix");
        assert_eq!(event.status, EventStatus::Approved);
        assert_eq!(
            event.time_start.expect("time_start"),
            chrono::DateTime::parse_from_rfc3339("2024-01-15T09:00:00Z")
                .expect("parse time")
                .with_timezone(&Utc)
        );
        assert!((event.confidence - 0.87).abs() < f32::EPSILON);
        assert!((event.time_confidence - 0.87).abs() < f32::EPSILON);
        assert_eq!(event.assertion_refs.len(), 1, "Expected assertion evidence");
        assert_eq!(
            event.base.subjects.len(),
            2,
            "Subject + object participants"
        );

        let system_event_repo = SystemEventRepository::new(storage.clone());
        let outbox_events = system_event_repo
            .get_after_seq("test_space", 0, 10)
            .await
            .expect("get outbox events");
        assert_eq!(outbox_events.len(), 1, "Expected one outbox payload");
        let payload: elephant_outbox::OutboxPayload =
            serde_json::from_value(outbox_events[0].payload.clone()).expect("payload");
        assert!(
            payload.records.iter().any(|record| record.table == "event"),
            "Outbox payload must contain event record reference"
        );
    }

    #[tokio::test]
    async fn test_event_below_threshold_stays_candidate() {
        use elephant_storage::EventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut raw = make_raw_assertion("Alice", "announced", "Project Phoenix");
        raw.source_event_time = Some(Utc::now());
        raw.confidence = 0.5;

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        let event_repo = EventRepository::new(storage.clone());
        let events = event_repo
            .list_recent("test_space", 10)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1, "Expected persisted event");
        assert_eq!(events[0].status, EventStatus::Candidate);
    }

    #[tokio::test]
    async fn test_event_prefers_extractor_title_and_precision() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut raw = make_raw_assertion("Alice", "joined", "Acme Corp");
        raw.source_event_time = Some(Utc::now());
        raw.source_event_time_precision = Some(TimePrecision::Month);
        raw.event_title = Some("Board approved acquisition".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        assert_eq!(result.events_created, 1);
        let event_repo = EventRepository::new(storage.clone());
        let events = event_repo
            .list("test_space", 10, None)
            .await
            .expect("events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].title, "Board approved acquisition");
        assert_eq!(events[0].time_precision, TimePrecision::Month);
    }

    #[tokio::test]
    async fn test_event_without_precise_time_is_still_created() {
        use elephant_storage::EventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut raw = make_raw_assertion("Alice", "launched", "Project Phoenix");
        raw.source_event_time = None;
        raw.source_event_time_precision = None;
        raw.event_title = Some("Launch event".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let result = promote_assertions_inline(
            &storage,
            "test_space",
            &[raw],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("promote");

        assert_eq!(
            result.events_created, 1,
            "event should still be materialized"
        );

        let event_repo = EventRepository::new(storage.clone());
        let events = event_repo
            .list_recent("test_space", 10)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert!(
            event.time_start.is_none(),
            "time_start should remain unknown"
        );
        assert_eq!(event.time_precision, TimePrecision::Unknown);
        assert_eq!(
            event.metadata["time_basis"],
            serde_json::Value::String("unknown".to_string())
        );
    }

    #[tokio::test]
    async fn test_event_temporal_refinement_updates_existing_lineage() {
        use elephant_storage::EventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let mut first = make_raw_assertion("Alice", "launched", "Project Phoenix");
        first.source_event_time = None;
        first.source_event_time_precision = None;
        first.event_title = Some("Launch event".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let first_result = promote_assertions_inline(
            &storage,
            "test_space",
            &[first.clone()],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("first promote");
        assert_eq!(first_result.events_created, 1);

        let mut second = first;
        second.source_event_time = Some(
            chrono::DateTime::parse_from_rfc3339("2024-03-10T12:00:00Z")
                .expect("parse time")
                .with_timezone(&Utc),
        );
        second.source_event_time_precision = Some(TimePrecision::Day);

        let second_result = promote_assertions_inline(
            &storage,
            "test_space",
            &[second],
            &RecordId::new_v4(TableName::WorkRun),
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("second promote");
        assert_eq!(
            second_result.events_created, 0,
            "refinement should update existing event instead of creating another"
        );

        let expected_lineage_key = build_assertion_key(
            &doc_node_id,
            "launched",
            &make_raw_assertion("Alice", "launched", "Project Phoenix"),
        );
        let event_repo = EventRepository::new(storage.clone());
        let events = event_repo
            .list_recent("test_space", 10)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(
            event.time_start,
            Some(
                chrono::DateTime::parse_from_rfc3339("2024-03-10T12:00:00Z")
                    .expect("parse time")
                    .with_timezone(&Utc)
            )
        );
        assert_eq!(event.time_precision, TimePrecision::Day);
        assert_eq!(
            event.metadata["time_basis"],
            serde_json::Value::String("source_event_time".to_string())
        );
        assert_eq!(
            event.metadata[EVENT_METADATA_LINEAGE_KEY],
            serde_json::Value::String(expected_lineage_key)
        );
    }

    #[tokio::test]
    async fn test_event_temporal_refinement_reuses_same_timestamp_with_better_precision() {
        use elephant_storage::EventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let timestamp = chrono::DateTime::parse_from_rfc3339("2024-03-10T12:00:00Z")
            .expect("parse time")
            .with_timezone(&Utc);

        let mut first = make_raw_assertion("Alice", "launched", "Project Phoenix");
        first.source_event_time = Some(timestamp);
        first.source_event_time_precision = Some(TimePrecision::Day);
        first.event_title = Some("Launch event".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let first_result = promote_assertions_inline(
            &storage,
            "test_space",
            &[first.clone()],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("first promote");
        assert_eq!(first_result.events_created, 1);

        let mut second = first;
        second.source_event_time_precision = Some(TimePrecision::Minute);

        let second_result = promote_assertions_inline(
            &storage,
            "test_space",
            &[second],
            &RecordId::new_v4(TableName::WorkRun),
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("second promote");
        assert_eq!(
            second_result.events_created, 0,
            "same timestamp lineage should refine instead of duplicating"
        );

        let events = EventRepository::new(storage)
            .list_recent("test_space", 10)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].time_start, Some(timestamp));
        assert_eq!(events[0].time_precision, TimePrecision::Minute);
    }

    #[tokio::test]
    async fn test_event_temporal_refinement_reuses_same_day_with_more_precise_time() {
        use elephant_storage::EventRepository;

        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let day_precision_time = chrono::DateTime::parse_from_rfc3339("2024-03-10T00:00:00Z")
            .expect("parse day time")
            .with_timezone(&Utc);
        let exact_time = chrono::DateTime::parse_from_rfc3339("2024-03-10T12:30:00Z")
            .expect("parse exact time")
            .with_timezone(&Utc);

        let mut first = make_raw_assertion("Alice", "launched", "Project Phoenix");
        first.source_event_time = Some(day_precision_time);
        first.source_event_time_precision = Some(TimePrecision::Day);
        first.event_title = Some("Launch event".to_string());

        let work_run_id = RecordId::new_v4(TableName::WorkRun);
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        let first_result = promote_assertions_inline(
            &storage,
            "test_space",
            &[first.clone()],
            &work_run_id,
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("first promote");
        assert_eq!(first_result.events_created, 1);

        let mut second = first;
        second.source_event_time = Some(exact_time);
        second.source_event_time_precision = Some(TimePrecision::Exact);

        let second_result = promote_assertions_inline(
            &storage,
            "test_space",
            &[second],
            &RecordId::new_v4(TableName::WorkRun),
            &doc_node_id,
            &revision_id,
            "test_actor",
        )
        .await
        .expect("second promote");
        assert_eq!(
            second_result.events_created, 0,
            "same-day lineage should refine instead of duplicating"
        );

        let events = EventRepository::new(storage)
            .list_recent("test_space", 10)
            .await
            .expect("list events");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].time_start, Some(exact_time));
        assert_eq!(events[0].time_precision, TimePrecision::Exact);
    }

    #[derive(Debug, Default)]
    struct FakePromotionDisambiguator {
        decisions:
            std::sync::Mutex<std::collections::VecDeque<Option<PromotionDisambiguationDecision>>>,
        requests: std::sync::Mutex<Vec<DisambiguationRequest>>,
    }

    impl FakePromotionDisambiguator {
        fn scripted(decisions: Vec<Option<PromotionDisambiguationDecision>>) -> Arc<Self> {
            Arc::new(Self {
                decisions: std::sync::Mutex::new(decisions.into()),
                requests: std::sync::Mutex::new(Vec::new()),
            })
        }

        fn options(self: &Arc<Self>) -> PromotionOptions {
            PromotionOptions {
                disambiguator: Some(self.clone() as Arc<dyn PromotionDisambiguator>),
            }
        }

        fn requests(&self) -> Vec<DisambiguationRequest> {
            self.requests.lock().unwrap().clone()
        }
    }

    #[async_trait::async_trait]
    impl PromotionDisambiguator for FakePromotionDisambiguator {
        async fn disambiguate(
            &self,
            input: &DisambiguationRequest,
        ) -> Result<Option<PromotionDisambiguationDecision>, PipelineError> {
            self.requests.lock().unwrap().push(input.clone());
            Ok(self.decisions.lock().unwrap().pop_front().flatten())
        }
    }

    fn make_typed_entity(
        display_name: &str,
        entity_type: &str,
        created_at: DateTime<Utc>,
    ) -> Entity {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "test_space".to_string(),
            "test".to_string(),
        );
        base.created_at = created_at;
        let mut entity = Entity::new(base, EntityType::new(entity_type), display_name.to_string());
        entity.identity_keys = vec![display_name.to_lowercase()];
        entity
    }

    async fn promote_with(
        storage: &StorageClient,
        raw: &RawAssertion,
        doc_node_id: &RecordId,
        options: &PromotionOptions,
    ) -> PromotionResult {
        promote_assertions_inline_with_options(
            storage,
            "test_space",
            std::slice::from_ref(raw),
            &RecordId::new_v4(TableName::WorkRun),
            doc_node_id,
            &RecordId::new_v4(TableName::DocRevision),
            "test_actor",
            options,
        )
        .await
        .expect("promote")
    }

    async fn promoted_subject_entity_id(
        storage: &StorageClient,
        doc_node_id: &RecordId,
        raw: &RawAssertion,
    ) -> RecordId {
        let key = build_assertion_key(doc_node_id, &raw.predicate_text, raw);
        let assertion = AssertionRepository::new(storage.clone())
            .find_by_assertion_key("test_space", &key)
            .await
            .expect("find assertion")
            .expect("assertion exists");
        assertion
            .subject_entity
            .expect("subject entity")
            .id()
            .clone()
    }

    #[tokio::test]
    async fn test_type_guard_same_specific_type_reuses() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let existing = make_typed_entity("Alice", "person", Utc::now());
        let existing_id = existing.base.id.clone();
        entity_repo.create(existing).await.unwrap();

        let raw = make_literal_assertion("Alice", "job_title", "CEO");
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &PromotionOptions::default()).await;

        assert_eq!(result.entities_created, 0, "same specific type must reuse");
        assert_eq!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            existing_id
        );
    }

    #[tokio::test]
    async fn test_type_guard_topic_and_specific_reuse_with_upgrade() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        // Stored topic + incoming specific: reuse and upgrade the stored type.
        let stored_topic = make_typed_entity("Acme", "topic", Utc::now());
        let topic_id = stored_topic.base.id.clone();
        entity_repo.create(stored_topic).await.unwrap();

        let mut raw = make_literal_assertion("Acme", "founded_in", "1999");
        raw.subject_entity_type = Some("org".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &PromotionOptions::default()).await;

        assert_eq!(result.entities_created, 0);
        assert_eq!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            topic_id
        );
        let stored = entity_repo
            .get("test_space", &topic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.entity_type.as_str(),
            "org",
            "topic must be upgraded to the specific incoming type"
        );

        // Stored specific + incoming untyped (topic): reuse, keep specific type.
        let mut untyped = make_literal_assertion("Acme", "employee_count", "500");
        untyped.subject_entity_type = None;
        let second_node = RecordId::new_v4(TableName::DocNode);
        let second = promote_with(
            &storage,
            &untyped,
            &second_node,
            &PromotionOptions::default(),
        )
        .await;

        assert_eq!(second.entities_created, 0);
        assert_eq!(
            promoted_subject_entity_id(&storage, &second_node, &untyped).await,
            topic_id
        );
        let stored = entity_repo
            .get("test_space", &topic_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.entity_type.as_str(), "org", "no downgrade to topic");
    }

    #[tokio::test]
    async fn test_type_guard_specific_conflict_creates_new_without_llm() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let planet = make_typed_entity("Mercury", "location", Utc::now());
        let planet_id = planet.base.id.clone();
        entity_repo.create(planet).await.unwrap();

        let mut raw = make_literal_assertion("Mercury", "role", "messenger god");
        raw.subject_entity_type = Some("person".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &PromotionOptions::default()).await;

        assert_eq!(
            result.entities_created, 1,
            "conflicting specific types must create a new entity, never false-merge"
        );
        let subject_id = promoted_subject_entity_id(&storage, &doc_node_id, &raw).await;
        assert_ne!(subject_id, planet_id);
        let planet = entity_repo
            .get("test_space", &planet_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            planet.entity_type.as_str(),
            "location",
            "stored entity untouched"
        );
    }

    #[tokio::test]
    async fn test_untyped_then_typed_sequence_reuses_and_upgrades() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let mut untyped = make_literal_assertion("Acme", "mentioned", "in passing");
        untyped.subject_entity_type = None;
        let first_node = RecordId::new_v4(TableName::DocNode);
        let first = promote_with(
            &storage,
            &untyped,
            &first_node,
            &PromotionOptions::default(),
        )
        .await;
        assert_eq!(first.entities_created, 1);

        let mut typed = make_literal_assertion("Acme", "founded_in", "1999");
        typed.subject_entity_type = Some("org".to_string());
        let second_node = RecordId::new_v4(TableName::DocNode);
        let second =
            promote_with(&storage, &typed, &second_node, &PromotionOptions::default()).await;

        assert_eq!(
            second.entities_created, 0,
            "typed mention must reuse, not duplicate"
        );
        let matches = entity_repo
            .find_by_identity_key_all("test_space", "acme")
            .await
            .unwrap();
        assert_eq!(matches.len(), 1, "exactly one Acme entity");
        assert_eq!(matches[0].entity_type.as_str(), "org");
    }

    #[tokio::test]
    async fn test_multi_match_disambiguator_chooses_candidate() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let now = Utc::now();
        let mut first = make_typed_entity("Acme Corp", "org", now - chrono::Duration::hours(2));
        first.identity_keys = vec!["acme".to_string()];
        let mut second = make_typed_entity("Acme Inc", "org", now - chrono::Duration::hours(1));
        second.identity_keys = vec!["acme".to_string()];
        let second_id = second.base.id.clone();
        entity_repo.create(first).await.unwrap();
        entity_repo.create(second).await.unwrap();

        let fake = FakePromotionDisambiguator::scripted(vec![Some(
            PromotionDisambiguationDecision::Reuse {
                entity_id: second_id.to_string(),
            },
        )]);
        let mut raw = make_literal_assertion("Acme", "founded_in", "1999");
        raw.subject_entity_type = Some("org".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(result.entities_created, 0);
        assert_eq!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            second_id
        );
        let requests = fake.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].candidates.len(), 2);
        assert_eq!(requests[0].mention, "Acme");
        assert_eq!(requests[0].normalized_type, "org");
    }

    #[tokio::test]
    async fn test_multi_match_no_decision_falls_back_to_earliest_created() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let now = Utc::now();
        // Insert the NEWER entity first so scan/insertion order disagrees
        // with created_at order.
        let mut newer = make_typed_entity("Acme Inc", "org", now);
        newer.identity_keys = vec!["acme".to_string()];
        let mut older = make_typed_entity("Acme Corp", "org", now - chrono::Duration::hours(3));
        older.identity_keys = vec!["acme".to_string()];
        let older_id = older.base.id.clone();
        entity_repo.create(newer).await.unwrap();
        entity_repo.create(older).await.unwrap();

        let fake = FakePromotionDisambiguator::scripted(vec![None]);
        let mut raw = make_literal_assertion("Acme", "founded_in", "1999");
        raw.subject_entity_type = Some("org".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(result.entities_created, 0);
        assert_eq!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            older_id,
            "deterministic fallback must pick the earliest-created match"
        );
    }

    #[tokio::test]
    async fn test_multi_match_disambiguator_create_new() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let now = Utc::now();
        for name in ["Acme Corp", "Acme Inc"] {
            let mut entity = make_typed_entity(name, "org", now);
            entity.identity_keys = vec!["acme".to_string()];
            entity_repo.create(entity).await.unwrap();
        }

        let fake = FakePromotionDisambiguator::scripted(vec![Some(
            PromotionDisambiguationDecision::CreateNew,
        )]);
        let mut raw = make_literal_assertion("Acme", "founded_in", "1999");
        raw.subject_entity_type = Some("org".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(
            result.entities_created, 1,
            "create_new decision must be honored"
        );
        assert_eq!(
            entity_repo
                .find_by_identity_key_all("test_space", "acme")
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn test_indistinguishable_matches_reuse_earliest_without_asking() {
        // Synthetic regression: a CreateNew response must not amplify duplicate
        // records that contain no distinguishing information.
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let now = Utc::now();
        let earliest = now - chrono::Duration::days(2);
        let mut first = make_typed_entity("Morgan Sjöberg", "person", earliest);
        first.identity_keys = vec!["morgan sjöberg".to_string()];
        let earliest_id = first.base.id.clone();
        entity_repo.create(first).await.unwrap();
        let mut second = make_typed_entity("Morgan Sjöberg", "person", now);
        second.identity_keys = vec!["morgan sjöberg".to_string()];
        entity_repo.create(second).await.unwrap();

        // Scripted to CreateNew: the point is that it is never consulted.
        let fake = FakePromotionDisambiguator::scripted(vec![Some(
            PromotionDisambiguationDecision::CreateNew,
        )]);
        let mut raw = make_literal_assertion("Morgan Sjöberg", "works_at", "Northbank Academy");
        raw.subject_entity_type = Some("person".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(
            result.entities_created, 0,
            "a third record with the same identity key deepens the ambiguity it was \
             asked to resolve; it must reuse instead"
        );
        assert!(
            fake.requests().is_empty(),
            "the disambiguator must not be asked to separate records that carry no \
             distinguishing information"
        );
        let stored = entity_repo
            .find_by_identity_key_all("test_space", "morgan sjöberg")
            .await
            .unwrap();
        assert_eq!(stored.len(), 2, "no duplicate was added");
        assert_eq!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            earliest_id,
            "reuse is deterministic: the earliest-created candidate wins"
        );
    }

    #[tokio::test]
    async fn test_distinguishable_matches_still_consult_the_disambiguator() {
        // The legitimate case this must not break: two REAL records that happen to
        // share an identity key. They differ, so the disambiguator has something to
        // decide on and its verdict stands - see
        // test_multi_match_disambiguator_create_new, which covers the CreateNew half.
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let now = Utc::now();
        let mut first = make_typed_entity("Anna Andersson", "person", now);
        first.identity_keys = vec!["anna andersson".to_string()];
        first.summary = Some("Ada's swimming teacher".to_string());
        entity_repo.create(first).await.unwrap();
        let mut second = make_typed_entity("Anna Andersson", "person", now);
        second.identity_keys = vec!["anna andersson".to_string()];
        entity_repo.create(second).await.unwrap();

        let fake = FakePromotionDisambiguator::scripted(vec![None]);
        let mut raw = make_literal_assertion("Anna Andersson", "works_at", "Northbank Academy");
        raw.subject_entity_type = Some("person".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(
            fake.requests().len(),
            1,
            "when the candidates differ, the disambiguator is the right authority"
        );
    }

    #[tokio::test]
    async fn test_incompatible_match_disambiguator_can_override_type_guard() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        // Stored type is wrong: "Mercury" the person was misfiled as location.
        let stored = make_typed_entity("Mercury", "location", Utc::now());
        let stored_id = stored.base.id.clone();
        entity_repo.create(stored).await.unwrap();

        let fake = FakePromotionDisambiguator::scripted(vec![Some(
            PromotionDisambiguationDecision::Reuse {
                entity_id: stored_id.to_string(),
            },
        )]);
        let mut raw = make_literal_assertion("Mercury", "role", "messenger god");
        raw.subject_entity_type = Some("person".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(
            result.entities_created, 0,
            "LLM reuse overrides the type guard"
        );
        assert_eq!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            stored_id
        );
        let stored = entity_repo
            .get("test_space", &stored_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.entity_type.as_str(),
            "location",
            "no type rewrite when reusing across a type conflict"
        );
        assert_eq!(fake.requests().len(), 1);
    }

    #[tokio::test]
    async fn test_incompatible_match_unknown_reuse_id_creates_new() {
        let storage = StorageClient::new_memory().await.unwrap();
        storage.query(SCHEMA).await.unwrap();
        let entity_repo = EntityRepository::new(storage.clone());

        let stored = make_typed_entity("Mercury", "location", Utc::now());
        let stored_id = stored.base.id.clone();
        entity_repo.create(stored).await.unwrap();

        let fake = FakePromotionDisambiguator::scripted(vec![Some(
            PromotionDisambiguationDecision::Reuse {
                entity_id: "entity:not-a-candidate".to_string(),
            },
        )]);
        let mut raw = make_literal_assertion("Mercury", "role", "messenger god");
        raw.subject_entity_type = Some("person".to_string());
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let result = promote_with(&storage, &raw, &doc_node_id, &fake.options()).await;

        assert_eq!(result.entities_created, 1);
        assert_ne!(
            promoted_subject_entity_id(&storage, &doc_node_id, &raw).await,
            stored_id
        );
    }

    #[test]
    fn test_entity_types_compatible_matrix() {
        let person = EntityType::new("person");
        let org = EntityType::new("org");
        let topic = EntityType::new("topic");
        // Legacy/unknown stored values normalize into the vocabulary.
        let company = EntityType::new("company");
        let weird = EntityType::new("something-else");

        assert!(entity_types_compatible(&person, &person));
        assert!(entity_types_compatible(&topic, &person));
        assert!(entity_types_compatible(&person, &topic));
        assert!(entity_types_compatible(&topic, &topic));
        assert!(!entity_types_compatible(&person, &org));
        assert!(entity_types_compatible(&company, &org));
        assert!(entity_types_compatible(&weird, &person));
    }

    #[test]
    fn test_assertion_context_snippet_prefers_quote() {
        let mut raw = make_raw_assertion("Alice", "works_at", "Acme Corp");
        assert_eq!(assertion_context_snippet(&raw), "test quote");

        raw.quote = None;
        assert_eq!(assertion_context_snippet(&raw), "Alice works_at Acme Corp");

        raw.quote = Some("x".repeat(2 * CONTEXT_SNIPPET_MAX_CHARS));
        assert_eq!(
            assertion_context_snippet(&raw).chars().count(),
            CONTEXT_SNIPPET_MAX_CHARS
        );
    }

    fn internal_review_metadata() -> serde_json::Value {
        // Sanitized, metadata-only stand-in for an internal security-review doc.
        serde_json::json!({
            HOST_PROJECTION_METADATA_KEY: HOST_PROJECTION_INTERNAL_REVIEW,
            "kind": "review_bookkeeping",
        })
    }

    async fn promote_once(
        storage: &StorageClient,
        raw: &RawAssertion,
        doc_node_id: &RecordId,
        revision_id: &RecordId,
    ) -> PromotionResult {
        promote_assertions_inline(
            storage,
            "test_space",
            std::slice::from_ref(raw),
            &RecordId::new_v4(TableName::WorkRun),
            doc_node_id,
            revision_id,
            "test_actor",
        )
        .await
        .expect("promote")
    }

    async fn all_events(storage: &StorageClient) -> Vec<Event> {
        EventRepository::new(storage.clone())
            .list_recent("test_space", 50)
            .await
            .expect("list events")
    }

    async fn set_doc_metadata(
        storage: &StorageClient,
        revision_id: &RecordId,
        metadata: serde_json::Value,
    ) {
        let revision = DocRevisionRepository::new(storage.clone())
            .get_by_id(revision_id)
            .await
            .unwrap()
            .unwrap();
        let repo = DocRepository::new(storage.clone());
        let mut doc = repo
            .get("test_space", revision.doc.id())
            .await
            .unwrap()
            .unwrap();
        doc.metadata = metadata;
        repo.update("test_space", &doc.base.id, doc.clone())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_internal_review_doc_records_assertion_but_no_household_event() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        let revision_id = create_doc_with_revision(&storage, internal_review_metadata()).await;
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let mut raw = make_raw_assertion("Reviewer", "scheduled_review", "Security review");
        raw.source_event_time = Some(Utc::now() + chrono::Duration::days(3));
        raw.source_event_time_precision = Some(TimePrecision::Day);

        let first = promote_once(&storage, &raw, &doc_node_id, &revision_id).await;
        assert_eq!(first.assertions_recorded, 1);
        assert_eq!(first.rels_created, 1);
        assert_eq!(first.events_created, 0);
        // Reprocessing the same doc twice still yields no event.
        for _ in 0..2 {
            let again = promote_once(&storage, &raw, &doc_node_id, &revision_id).await;
            assert_eq!(again.events_created, 0);
        }
        assert!(all_events(&storage).await.is_empty());
        let assertions = AssertionRepository::new(storage.clone())
            .list("test_space", 10, None)
            .await
            .expect("list assertions");
        assert_eq!(assertions.len(), 1, "history stays searchable");
    }

    #[tokio::test]
    async fn test_household_doc_event_unchanged_and_projection_copied() {
        for (metadata, has_doc, projection) in [
            (serde_json::json!({}), true, None),
            (serde_json::json!({}), false, None),
            (
                serde_json::json!({ HOST_PROJECTION_METADATA_KEY: null }),
                true,
                None,
            ),
            (
                serde_json::json!({ HOST_PROJECTION_METADATA_KEY: "household" }),
                true,
                Some(serde_json::json!("household")),
            ),
        ] {
            let storage = StorageClient::new_memory().await.expect("storage");
            storage.query(SCHEMA).await.expect("schema");
            let revision_id = if has_doc {
                create_doc_with_revision(&storage, metadata).await
            } else {
                RecordId::new_v4(TableName::DocRevision)
            };
            let raw = make_raw_assertion("Alice", "booked", "Dentist");
            let result = promote_once(
                &storage,
                &raw,
                &RecordId::new_v4(TableName::DocNode),
                &revision_id,
            )
            .await;
            assert_eq!(result.events_created, 1);
            let events = all_events(&storage).await;
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].status, EventStatus::Approved);
            assert_eq!(events[0].metadata["origin"], "inline_promotion");
            assert_eq!(
                events[0]
                    .metadata
                    .get(HOST_PROJECTION_METADATA_KEY)
                    .cloned(),
                projection
            );
        }
    }

    #[tokio::test]
    async fn test_operator_rejected_event_stays_rejected_after_reprocessing() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");
        let revision_id = create_doc_with_revision(&storage, serde_json::json!({})).await;
        let doc_node_id = RecordId::new_v4(TableName::DocNode);
        let mut raw = make_raw_assertion("Alice", "booked", "Dentist");
        raw.confidence = 0.6;
        promote_once(&storage, &raw, &doc_node_id, &revision_id).await;
        let repo = EventRepository::new(storage.clone());
        let mut event = all_events(&storage).await.pop().expect("event");
        assert_eq!(event.status, EventStatus::Candidate);
        event.status = EventStatus::Rejected;
        repo.update("test_space", &event.base.id, event.clone())
            .await
            .unwrap();

        // Same doc again (already-seen path), then stronger support for the
        // same lineage: both used to reset the status by confidence.
        promote_once(&storage, &raw, &doc_node_id, &revision_id).await;
        let mut stronger = raw.clone();
        stronger.confidence = 0.99;
        stronger.event_lineage_key = Some(build_assertion_key(&doc_node_id, "booked", &raw));
        stronger.evidence_span.start = 3;
        promote_once(&storage, &stronger, &doc_node_id, &revision_id).await;

        let events = all_events(&storage).await;
        assert_eq!(events.len(), 1, "no replacement event");
        assert_eq!(events[0].base.id, event.base.id);
        assert_eq!(events[0].status, EventStatus::Rejected);
        assert!((events[0].confidence - 0.99).abs() < f32::EPSILON);
    }

    #[tokio::test]
    async fn test_internal_review_reprocessing_never_refines_or_resurrects_events() {
        for (status, deleted) in [
            (EventStatus::Approved, false),
            (EventStatus::Rejected, false),
            (EventStatus::Approved, true),
        ] {
            let storage = StorageClient::new_memory().await.expect("storage");
            storage.query(SCHEMA).await.expect("schema");
            let revision_id = create_doc_with_revision(&storage, serde_json::json!({})).await;
            let doc_node_id = RecordId::new_v4(TableName::DocNode);
            let raw = make_raw_assertion("Reviewer", "scheduled_review", "Security review");
            promote_once(&storage, &raw, &doc_node_id, &revision_id).await;
            let repo = EventRepository::new(storage.clone());
            let mut event = all_events(&storage).await.pop().expect("event");
            event.status = status;
            let event = repo
                .update("test_space", &event.base.id, event.clone())
                .await
                .unwrap();
            if deleted {
                repo.delete("test_space", &event.base.id).await.unwrap();
            }
            // HomeCore stamps the marker later; reprocess, including a variant
            // that would otherwise create a new event for a retired lineage.
            set_doc_metadata(&storage, &revision_id, internal_review_metadata()).await;
            let mut variant = raw.clone();
            variant.evidence_span.start = 7;
            for raw in [&raw, &raw, &variant] {
                let result = promote_once(&storage, raw, &doc_node_id, &revision_id).await;
                assert_eq!(result.events_created, 0);
            }
            let live = repo.get("test_space", &event.base.id).await.unwrap();
            if deleted {
                assert!(live.is_none());
                assert!(all_events(&storage).await.is_empty());
            } else {
                assert_eq!(live, Some(event));
            }
        }
    }
}
