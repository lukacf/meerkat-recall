//! Re-resolve existing assertions in the maintenance owner's captured plan.
//!
//! No assertion is re-extracted, inserted, or given a new verification receipt.
//! Immutable evidence and original authorization remain inputs to every effect.
//! The caller commits the entire plan (including its job/attempt) exactly once.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::Utc;
use elephant_storage::read_eligibility::{CurrentEvidence, CurrentRead, ReadEligibility};
use elephant_storage::{
    AssertionRepository, AttributeRepository, DocRepository, DocRevisionRepository,
    EntityRepository, EventRepository, FreshnessRepository, RelRepository, Repository,
    StorageClient, WorkArtifactRepository,
};
use elephant_types::records::assertion::{Assertion, ResolutionStatus};
use elephant_types::records::attribute::{Attribute, Literal};
use elephant_types::records::base::{BaseFields, ProvenanceStep};
use elephant_types::records::entity::{Entity, EntityStatus};
use elephant_types::records::event::{Event, EventStatus};
use elephant_types::records::freshness::Freshness;
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel, TimeBasis};
use elephant_types::records::source::Source;
use elephant_types::{Link, RecordId, TableName};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::error::{PipelineError, PipelineResult};
use crate::freshness::{prepare_claim_freshness, ClaimFreshnessObservation};
use crate::maintenance_commit::{
    contains_authorization, PlanRecord, PlanningTruthMaintenanceStorage,
};
use crate::scheduler::extraction_work_kinds::ExtractionArtifactData;
use crate::scheduler::grounding::same_support_partition;
use crate::scheduler::inline_promotion::{
    entity_types_compatible, host_projection_from_metadata,
    host_projection_suppresses_timeline_events, normalize_entity_type,
    HOST_PROJECTION_METADATA_KEY,
};
use crate::scheduler::promotion_disambiguation::{
    CandidateEntity, DisambiguationRequest, PromotionDisambiguationDecision, PromotionDisambiguator,
};
use crate::truth_maintenance::TruthMaintenanceStorage;

const PAGE_SIZE: usize = 256;
const MAX_ROWS_PER_TABLE: usize = 100_000;

#[derive(Clone, Debug)]
pub(crate) struct ReprocessResolutionOptions {
    pub(crate) actor: String,
    pub(crate) work_run_id: RecordId,
    pub(crate) disambiguator: Option<Arc<dyn PromotionDisambiguator>>,
}

impl ReprocessResolutionOptions {
    pub(crate) fn new(actor: impl Into<String>, work_run_id: RecordId) -> Self {
        Self {
            actor: actor.into(),
            work_run_id,
            disambiguator: None,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub(crate) struct ReprocessResolutionResult {
    pub(crate) assertions_reprocessed: u64,
    pub(crate) rels_superseded: u64,
    pub(crate) attributes_superseded: u64,
    pub(crate) pending_conflict_groups: Vec<RecordId>,
    pub(crate) events_superseded: u64,
    pub(crate) entities_created: u64,
}

fn invalid(message: impl Into<String>) -> PipelineError {
    PipelineError::ValidationError(message.into())
}

async fn all_rows<T, R>(repo: &R, space: &str) -> PipelineResult<Vec<T>>
where
    T: PlanRecord + Serialize + DeserializeOwned,
    R: Repository<T>,
{
    let mut result = Vec::new();
    let mut cursor = None;
    loop {
        let page = repo.list(space, PAGE_SIZE, cursor.as_ref()).await?;
        let next = page.last().map(|row| row.base().id.clone());
        if !page.is_empty() && next == cursor {
            return Err(invalid("re-resolution repository cursor did not advance"));
        }
        let done = page.len() < PAGE_SIZE;
        result.extend(page);
        if result.len() > MAX_ROWS_PER_TABLE {
            return Err(PipelineError::BudgetExceeded(
                "re-resolution affected-table scan exceeds 100000 rows; no effects committed"
                    .into(),
            ));
        }
        if done {
            return Ok(result);
        }
        cursor = next;
    }
}

async fn require_current<T: PlanRecord + CurrentRead>(
    planning: &PlanningTruthMaintenanceStorage,
    reader: &ReadEligibility,
    row: T,
) -> PipelineResult<T> {
    planning.observe(&row)?;
    let projected = reader
        .project(row.clone(), &|_| true)
        .await?
        .ok_or_else(|| invalid("re-resolution input lost current source support"))?;
    // Maintenance may not silently turn a partially projected claim into a
    // replacement for a different stored row. Reprepare after its owner cleans it.
    if serde_json::to_value(&projected).map_err(|e| invalid(e.to_string()))?
        != serde_json::to_value(&row).map_err(|e| invalid(e.to_string()))?
    {
        return Err(invalid(
            "re-resolution input has obsolete support references",
        ));
    }
    Ok(row)
}

/// Metadata of the document whose revision grounds `assertion`, observed as a
/// plan input and cached per revision. A missing revision or document yields
/// `None` (no host projection), matching the behavior before projections.
/// The projection is a hint any authorized document writer may set; see
/// [`HOST_PROJECTION_METADATA_KEY`] for the writer contract.
async fn evidence_doc_projection(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    assertion: &Assertion,
    cache: &mut BTreeMap<RecordId, Option<serde_json::Value>>,
) -> PipelineResult<Option<serde_json::Value>> {
    let revision_id = assertion.evidence.revision.id();
    if let Some(cached) = cache.get(revision_id) {
        return Ok(cached.clone());
    }
    let mut metadata = None;
    if let Some(revision) = DocRevisionRepository::new(storage.clone())
        .get_by_id(revision_id)
        .await?
    {
        if let Some(doc) = DocRepository::new(storage.clone())
            .get(space, revision.doc.id())
            .await?
        {
            planning.observe(&doc)?;
            metadata = Some(doc.metadata);
        }
    }
    cache.insert(revision_id.clone(), metadata.clone());
    Ok(metadata)
}

fn evidence_ref(assertion: &Assertion) -> EvidenceRef {
    EvidenceRef {
        assertion: Link::new(assertion.base.id.clone()),
        span: assertion.evidence.clone(),
        time_basis: if assertion.source_event_time.is_some() {
            TimeBasis::SourceEventTime
        } else {
            TimeBasis::ObservedAt
        },
        time_value: assertion.source_event_time.unwrap_or(assertion.observed_at),
    }
}

fn normalize_evidence(evidence: &mut Vec<EvidenceRef>) {
    evidence.sort_by(|a, b| {
        a.assertion
            .id()
            .cmp(b.assertion.id())
            .then_with(|| a.span.revision.id().cmp(b.span.revision.id()))
            .then_with(|| a.span.start.cmp(&b.span.start))
            .then_with(|| a.span.end.cmp(&b.span.end))
            .then_with(|| a.span.quote_hash.cmp(&b.span.quote_hash))
            .then_with(|| a.time_basis.to_string().cmp(&b.time_basis.to_string()))
            .then_with(|| a.time_value.cmp(&b.time_value))
    });
    evidence.dedup();
}

fn predicate(assertion: &Assertion) -> &str {
    assertion
        .predicate_id
        .as_deref()
        .unwrap_or(&assertion.predicate_raw)
}

fn inherit(base: &mut BaseFields, input: &BaseFields) {
    base.security.clone_from(&input.security);
    base.subjects.clone_from(&input.subjects);
    base.subjects_state = input.subjects_state;
    base.provenance.clone_from(&input.provenance);
}

fn add_subjects(base: &mut BaseFields, input: &BaseFields) {
    base.subjects.extend(input.subjects.iter().cloned());
    base.subjects.sort_by(|a, b| a.id().cmp(b.id()));
    base.subjects.dedup_by(|a, b| a.id() == b.id());
}

fn add_entity(base: &mut BaseFields, entity: &Link<Entity>) {
    if !base.subjects.contains(entity) {
        base.subjects.push(entity.clone());
    }
    base.subjects.sort_by(|a, b| a.id().cmp(b.id()));
}

fn prepared_base(
    table: TableName,
    input: &BaseFields,
    options: &ReprocessResolutionOptions,
) -> BaseFields {
    let mut base = BaseFields::new(
        RecordId::new_v4(table),
        input.space_id.clone(),
        options.actor.clone(),
    );
    inherit(&mut base, input);
    base.provenance.work_run_id = Some(options.work_run_id.clone());
    base.provenance.chain.push(ProvenanceStep {
        step: "re_resolve".into(),
        input: input.id.to_string(),
        output: base.id.to_string(),
    });
    base
}

fn resolution_changed(before: &Assertion, after: &Assertion) -> bool {
    before.subject_entity != after.subject_entity
        || before.object_entity != after.object_entity
        || before.resolution_status != after.resolution_status
}

fn name_matches(entity: &Entity, mention: &str) -> bool {
    let key = mention.trim().to_lowercase();
    entity.display_name.trim().to_lowercase() == key
        || entity
            .aliases
            .iter()
            .chain(&entity.identity_keys)
            .any(|candidate| candidate.trim().to_lowercase() == key)
}

async fn disambiguator(
    storage: &StorageClient,
    space: &str,
    configured: &mut Option<Arc<dyn PromotionDisambiguator>>,
) -> PipelineResult<Arc<dyn PromotionDisambiguator>> {
    if let Some(adapter) = configured {
        return Ok(adapter.clone());
    }
    #[cfg(feature = "meerkat-extraction")]
    {
        use crate::scheduler::promotion_disambiguation::{
            AgenticPromotionDisambiguator, PromotionDisambiguationConfig,
        };
        let context = crate::provider_dispatch::ProviderDispatchContext::new(
            elephant_storage::SpaceConfigRepository::new(storage.clone()),
            space,
        );
        let adapter: Arc<dyn PromotionDisambiguator> = Arc::new(
            AgenticPromotionDisambiguator::new(PromotionDisambiguationConfig::default())
                .with_provider_context(context)
                .await?,
        );
        *configured = Some(adapter.clone());
        Ok(adapter)
    }
    #[cfg(not(feature = "meerkat-extraction"))]
    {
        let _ = (storage, space);
        Err(PipelineError::ConfigError(
            "ambiguous re-resolution requires the governed disambiguator feature".into(),
        ))
    }
}

struct Resolver<'a> {
    planning: &'a PlanningTruthMaintenanceStorage,
    storage: &'a StorageClient,
    reader: &'a ReadEligibility,
    space: &'a str,
    options: &'a ReprocessResolutionOptions,
    entities: Vec<Entity>,
    model: Option<Arc<dyn PromotionDisambiguator>>,
    created: u64,
    prepared_entities: BTreeSet<RecordId>,
}

impl Resolver<'_> {
    async fn resolve(
        &mut self,
        assertion: &Assertion,
        evidence: &CurrentEvidence,
        mention: &str,
        type_hint: Option<&str>,
        previous: Option<&Link<Entity>>,
    ) -> PipelineResult<Option<Link<Entity>>> {
        let incoming = normalize_entity_type(type_hint.unwrap_or("topic"));
        let source = evidence
            .source
            .as_ref()
            .ok_or_else(|| invalid("re-resolution requires an immutable source"))?;
        let mut partition = source.base.clone();
        partition.provenance.sources = vec![source.base.id.clone()];
        partition.provenance.chain.push(ProvenanceStep {
            step: "grounded_promotion".into(),
            input: evidence.revision.base.id.to_string(),
            output: String::new(),
        });
        let mut candidates = Vec::new();
        for entity in &self.entities {
            if entity.status != EntityStatus::Active
                || !name_matches(entity, mention)
                || !entity_types_compatible(&entity.entity_type, &incoming)
                || !same_support_partition(&entity.base, &partition)
            {
                continue;
            }
            // Newly prepared entities have no durable support receipt yet.
            let is_new = self.prepared_entities.contains(&entity.base.id);
            if !is_new {
                require_current(self.planning, self.reader, entity.clone()).await?;
            }
            candidates.push(entity.clone());
        }
        candidates.sort_by(|a, b| a.base.id.cmp(&b.base.id));
        candidates.dedup_by(|a, b| a.base.id == b.base.id);
        if let Some(previous) = previous {
            let current = self
                .planning
                .get_entity(self.space, previous.id())
                .await?
                .ok_or_else(|| invalid("existing resolution target is no longer current"))?;
            if !contains_authorization(&assertion.base, &current.base)? {
                return Err(invalid(
                    "existing resolution target requires different authorization",
                ));
            }
            // Canonical identity merges deliberately join source partitions.
            // The current assertion's supported link is authority for that exact
            // existing target, never permission to offer cross-source strangers.
            if !same_support_partition(&current.base, &partition) {
                return Ok(Some(previous.clone()));
            }
            if candidates.len() == 1 && candidates[0].base.id == current.base.id {
                return Ok(Some(previous.clone()));
            }
        }
        if candidates.len() == 1 {
            return Ok(Some(Link::new(candidates[0].base.id.clone())));
        }
        let create = if candidates.is_empty() {
            true
        } else {
            let request = DisambiguationRequest {
                mention: mention.to_owned(),
                normalized_type: incoming.as_str().to_owned(),
                context_snippet: evidence.quote.clone(),
                candidates: candidates
                    .iter()
                    .map(|row| CandidateEntity {
                        id: row.base.id.to_string(),
                        display_name: row.display_name.clone(),
                        entity_type: row.entity_type.as_str().to_owned(),
                        summary: row.summary.clone(),
                    })
                    .collect(),
            };
            let model = disambiguator(self.storage, self.space, &mut self.model).await?;
            match model.disambiguate(&request).await? {
                Some(PromotionDisambiguationDecision::Reuse { entity_id }) => {
                    let candidate = candidates.iter().find(|row| row.base.id.to_string() == entity_id)
                        .ok_or_else(|| invalid("disambiguator returned an entity outside its exact support partition"))?;
                    return Ok(Some(Link::new(candidate.base.id.clone())));
                }
                Some(PromotionDisambiguationDecision::CreateNew) => true,
                // Abstention preserves a current, already grounded identity.
                // It cannot fabricate a new link for an unresolved assertion.
                None => return Ok(previous.cloned()),
            }
        };
        if create {
            let mut base = prepared_base(TableName::Entity, &partition, self.options);
            // Re-resolution provenance retains the exact immutable revision.
            base.provenance.chain.retain(|step| !step.output.is_empty());
            base.provenance.chain.push(ProvenanceStep {
                step: "grounded_promotion".into(),
                input: evidence.revision.base.id.to_string(),
                output: base.id.to_string(),
            });
            let mut entity = Entity::new(base, incoming, mention.to_owned());
            entity.identity_keys.push(mention.trim().to_lowercase());
            let entity = self.planning.stage(None, entity)?;
            let link = Link::new(entity.base.id.clone());
            self.prepared_entities.insert(entity.base.id.clone());
            self.entities.push(entity);
            self.created += 1;
            Ok(Some(link))
        } else {
            Ok(None)
        }
    }
}

/// Existing and replacement assertion snapshots used to partition shared claims.
struct Assertions {
    before: BTreeMap<RecordId, Assertion>,
    after: BTreeMap<RecordId, Assertion>,
    changed: BTreeSet<RecordId>,
}

impl Assertions {
    async fn support(
        &mut self,
        planning: &PlanningTruthMaintenanceStorage,
        space: &str,
        evidence: &EvidenceRef,
    ) -> PipelineResult<(&Assertion, &Assertion)> {
        let id = evidence.assertion.id();
        if !self.before.contains_key(id) {
            let row = planning
                .get_assertion(space, id)
                .await?
                .ok_or_else(|| invalid("shared claim references a missing or stale assertion"))?;
            self.before.insert(id.clone(), row.clone());
            self.after.insert(id.clone(), row);
        }
        let before = self
            .before
            .get(id)
            .ok_or_else(|| invalid("missing assertion snapshot"))?;
        let after = self
            .after
            .get(id)
            .ok_or_else(|| invalid("missing replacement assertion snapshot"))?;
        let time = match evidence.time_basis {
            TimeBasis::SourceEventTime => before.source_event_time,
            TimeBasis::ObservedAt => Some(before.observed_at),
            TimeBasis::AssertedAt => Some(before.asserted_at),
        };
        if time != Some(evidence.time_value) {
            return Err(invalid(
                "claim evidence time differs from its cited assertion",
            ));
        }
        if before.evidence != evidence.span {
            return Err(invalid(
                "shared claim evidence differs from its immutable assertion",
            ));
        }
        Ok((before, after))
    }
}

fn assert_rel_tuple(row: &Rel, assertion: &Assertion) -> PipelineResult<()> {
    if assertion.subject_entity.as_ref() != Some(&row.subject)
        || assertion.object_entity.as_ref() != Some(&row.object)
        || predicate(assertion) != row.rel_type
        || assertion.object_literal.is_some()
    {
        return Err(invalid("relationship tuple differs from cited assertion"));
    }
    Ok(())
}

fn literal_value(value: &Literal) -> serde_json::Value {
    match value {
        Literal::String(s) | Literal::Date(s) | Literal::DateTime(s) | Literal::Duration(s) => {
            serde_json::json!(s)
        }
        Literal::Integer(n) => serde_json::json!(n),
        Literal::Float(n) => serde_json::json!(n),
        Literal::Boolean(b) => serde_json::json!(b),
        Literal::Json(value) => value.clone(),
    }
}

fn assert_attribute_tuple(row: &Attribute, assertion: &Assertion) -> PipelineResult<()> {
    if assertion.subject_entity.as_ref() != Some(&row.subject)
        || assertion.object_literal.as_ref() != Some(&literal_value(&row.value))
        || predicate(assertion) != row.attr_type
        || assertion.object_text.is_some()
    {
        return Err(invalid("attribute tuple differs from cited assertion"));
    }
    Ok(())
}

fn skipped(assertion: &Assertion) -> bool {
    assertion
        .significance
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("skip"))
}

fn resolved(assertion: &Assertion) -> bool {
    assertion.resolution_status == ResolutionStatus::Resolved
}

async fn rewrite_rel(
    planning: &PlanningTruthMaintenanceStorage,
    row: &Rel,
    assertions: &mut Assertions,
    options: &ReprocessResolutionOptions,
) -> PipelineResult<Vec<Rel>> {
    let mut groups: BTreeMap<(RecordId, RecordId), Vec<EvidenceRef>> = BTreeMap::new();
    for evidence in &row.evidence {
        let (before, after) = assertions
            .support(planning, &row.base.space_id, evidence)
            .await?;
        assert_rel_tuple(row, before)?;
        if !resolved(after) {
            continue;
        }
        let (Some(subject), Some(object)) = (&after.subject_entity, &after.object_entity) else {
            return Err(invalid(
                "resolved relationship assertion lacks entity links",
            ));
        };
        groups
            .entry((subject.id().clone(), object.id().clone()))
            .or_default()
            .push(evidence.clone());
    }
    let mut replacements = Vec::new();
    for ((subject, object), evidence) in groups {
        let mut after = row.clone();
        after.base = prepared_base(TableName::Rel, &row.base, options);
        after.subject = Link::new(subject);
        after.object = Link::new(object);
        after.evidence = evidence;
        normalize_evidence(&mut after.evidence);
        after.status = ClaimStatus::Candidate;
        after.validity_window = None;
        after.temporal_audit = None;
        add_entity(&mut after.base, &after.subject);
        add_entity(&mut after.base, &after.object);
        for item in &after.evidence {
            let assertion = assertions
                .after
                .get(item.assertion.id())
                .ok_or_else(|| invalid("missing relation support"))?;
            add_subjects(&mut after.base, &assertion.base);
            if !contains_authorization(&after.base, &assertion.base)? {
                return Err(invalid("relationship support crosses policy partitions"));
            }
        }
        replacements.push(after);
    }
    Ok(replacements)
}

async fn rewrite_attribute(
    planning: &PlanningTruthMaintenanceStorage,
    row: &Attribute,
    assertions: &mut Assertions,
    options: &ReprocessResolutionOptions,
) -> PipelineResult<Vec<Attribute>> {
    let mut groups: BTreeMap<RecordId, Vec<EvidenceRef>> = BTreeMap::new();
    for evidence in &row.evidence {
        let (before, after) = assertions
            .support(planning, &row.base.space_id, evidence)
            .await?;
        assert_attribute_tuple(row, before)?;
        if !resolved(after) {
            continue;
        }
        let subject = after
            .subject_entity
            .as_ref()
            .ok_or_else(|| invalid("resolved literal assertion lacks subject"))?;
        groups
            .entry(subject.id().clone())
            .or_default()
            .push(evidence.clone());
    }
    let mut replacements = Vec::new();
    for (subject, evidence) in groups {
        let mut after = row.clone();
        after.base = prepared_base(TableName::Attribute, &row.base, options);
        after.subject = Link::new(subject);
        after.evidence = evidence;
        normalize_evidence(&mut after.evidence);
        // Preserve the original typed value, including Date/DateTime/Duration.
        after.status = ClaimStatus::Candidate;
        after.validity_window = None;
        after.temporal_audit = None;
        add_entity(&mut after.base, &after.subject);
        for item in &after.evidence {
            let assertion = assertions
                .after
                .get(item.assertion.id())
                .ok_or_else(|| invalid("missing attribute support"))?;
            add_subjects(&mut after.base, &assertion.base);
            if !contains_authorization(&after.base, &assertion.base)? {
                return Err(invalid("attribute support crosses policy partitions"));
            }
        }
        replacements.push(after);
    }
    Ok(replacements)
}

fn stage_freshness(
    planning: &PlanningTruthMaintenanceStorage,
    base: &BaseFields,
    kind: &str,
    evidence: &[EvidenceRef],
    confidence: f32,
) -> PipelineResult<()> {
    if evidence.is_empty() {
        return Err(invalid("cannot synthesize freshness without evidence"));
    }
    let times = evidence
        .iter()
        .map(|item| item.time_value)
        .collect::<Vec<_>>();
    let freshness = prepare_claim_freshness(
        None,
        &base.id,
        kind,
        base,
        ClaimFreshnessObservation {
            evidence_times: &times,
            evidence_strength: confidence,
            conflict_severity: 0.0,
            corroboration_count: evidence.len(),
        },
    );
    planning.stage(None, freshness)?;
    Ok(())
}

async fn original_literal(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    assertion: &Assertion,
) -> PipelineResult<Literal> {
    let run = assertion
        .base
        .provenance
        .work_run_id
        .as_ref()
        .ok_or_else(|| {
            invalid("unmaterialized literal assertion lacks original typed extraction artifact")
        })?;
    let repo = WorkArtifactRepository::new(storage.clone());
    let mut values = Vec::new();
    for artifact in repo.find_by_work_run(run).await? {
        if artifact.base.space_id != assertion.base.space_id {
            return Err(invalid("original extraction artifact crossed space"));
        }
        let Ok(data) = serde_json::from_value::<ExtractionArtifactData>(artifact.data.clone())
        else {
            continue;
        };
        if data.revision_id != assertion.evidence.revision.id().to_string() {
            continue;
        }
        for candidate in &data.assertions {
            if candidate.subject_text == assertion.subject_text
                && candidate.predicate_text == assertion.predicate_raw
                && candidate.object_text == assertion.object_text
                && candidate.object_literal == assertion.object_literal
                && candidate.evidence_start == assertion.evidence.start
                && candidate.evidence_end == assertion.evidence.end
                && candidate.quote_hash.as_deref() == Some(assertion.evidence.quote_hash.as_str())
            {
                let node = assertion
                    .evidence
                    .node
                    .as_ref()
                    .ok_or_else(|| invalid("typed extraction lacks evidence node"))?;
                if data.node_id != node.id().to_string() {
                    return Err(invalid("typed extraction node differs"));
                }
                let raw = candidate.to_raw(assertion.evidence.revision.id(), node.id())?;
                crate::extraction::AssertionValidator::new(true)
                    .validate(&raw, data.revision_text_len)
                    .map_err(|_| {
                        invalid("original typed extraction support verification is invalid")
                    })?;
                let value = candidate
                    .typed_object_literal
                    .clone()
                    .ok_or_else(|| invalid("original extraction did not retain literal type"))?;
                planning.observe(&artifact)?;
                values.push(value);
            }
        }
    }
    let value = values
        .first()
        .cloned()
        .ok_or_else(|| invalid("original typed literal support is unavailable"))?;
    if values.iter().any(|other| other != &value) {
        return Err(invalid("original literal types disagree"));
    }
    Ok(value)
}

/// An event re-resolution keeps instead of replacing (operator rejected, or
/// changed support from an internal review document) must stay at least as
/// restricted as its re-resolved support, or readers could see it without
/// access to that support. Join only the support's subject requirements; the
/// event keeps its identity, status, title, time, support refs and metadata.
/// `None` when the envelope already covers the support (nothing to stage).
fn kept_event_envelope(row: &Event, support: &[Assertion]) -> PipelineResult<Option<Event>> {
    let mut covered = true;
    for assertion in support {
        covered &= contains_authorization(&row.base, &assertion.base)?;
    }
    if covered {
        return Ok(None);
    }
    let mut after = row.clone();
    for assertion in support {
        add_subjects(&mut after.base, &assertion.base);
    }
    for assertion in support {
        if !contains_authorization(&after.base, &assertion.base)? {
            return Err(invalid("event support crosses policy partitions"));
        }
    }
    after.base.updated_at = Utc::now();
    Ok(Some(after))
}

/// Stage re-resolution and every affected graph effect in the caller's overlay.
/// Original epoch/attempt, current source support and the one commit belong to M.
pub(crate) async fn prepare_reprocess(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    sources: &[Source],
    options: &ReprocessResolutionOptions,
) -> PipelineResult<ReprocessResolutionResult> {
    if options.actor.trim().is_empty() || options.work_run_id.table() != TableName::WorkRun {
        return Err(invalid(
            "re-resolution requires actor and original work run",
        ));
    }
    let reader = ReadEligibility::new(storage.clone());
    let mut source_ids = BTreeSet::new();
    for source in sources {
        if source.base.space_id != space || source.base.deleted_at.is_some() {
            return Err(invalid("re-resolution source is missing or foreign"));
        }
        planning.observe(source)?;
        if reader.project(source.clone(), &|_| true).await?.is_none() {
            return Err(invalid("re-resolution source is not published/current"));
        }
        source_ids.insert(source.base.id.clone());
    }
    let mut assertions = Assertions {
        before: BTreeMap::new(),
        after: BTreeMap::new(),
        changed: BTreeSet::new(),
    };
    let mut resolver = Resolver {
        planning,
        storage,
        reader: &reader,
        space,
        options,
        entities: all_rows(&EntityRepository::new(storage.clone()), space).await?,
        model: options.disambiguator.clone(),
        created: 0,
        prepared_entities: BTreeSet::new(),
    };
    let mut result = ReprocessResolutionResult::default();
    for row in all_rows(&AssertionRepository::new(storage.clone()), space).await? {
        if row.resolution_status == ResolutionStatus::Superseded
            || !row
                .base
                .provenance
                .sources
                .iter()
                .any(|id| source_ids.contains(id))
        {
            continue;
        }
        let row = require_current(planning, &reader, row).await?;
        planning.validate_assertion_evidence(&row).await?;
        let evidence = reader
            .evidence(space, &row.evidence, &|_| true)
            .await?
            .ok_or_else(|| invalid("re-resolution exact immutable evidence is unavailable"))?;
        let mut after = row.clone();
        after.subject_entity = resolver
            .resolve(
                &row,
                &evidence,
                &row.subject_text,
                row.subject_entity_type.as_deref(),
                row.subject_entity.as_ref(),
            )
            .await?;
        after.object_entity = if let Some(mention) = row.object_text.as_deref() {
            resolver
                .resolve(
                    &row,
                    &evidence,
                    mention,
                    row.object_entity_type.as_deref(),
                    row.object_entity.as_ref(),
                )
                .await?
        } else {
            None
        };
        after.resolution_status = if after.subject_entity.is_some()
            && (after.object_entity.is_some() || after.object_literal.is_some())
        {
            ResolutionStatus::Resolved
        } else if after.subject_entity.is_some() || after.object_entity.is_some() {
            ResolutionStatus::Partial
        } else {
            ResolutionStatus::Abstained
        };
        if resolution_changed(&row, &after) {
            after.base.updated_at = Utc::now();
            if let Some(link) = &after.subject_entity {
                add_entity(&mut after.base, link);
            }
            if let Some(link) = &after.object_entity {
                add_entity(&mut after.base, link);
            }
            after.resolution_method = Some("re_resolve_current_evidence_v1".into());
            after.resolution_confidence = None;
            after.retry_state.resolution_attempts = after
                .retry_state
                .resolution_attempts
                .checked_add(1)
                .ok_or_else(|| invalid("assertion resolution attempt count overflow"))?;
            after.retry_state.last_resolution_at = Some(Utc::now());
            after.retry_state.abstention_reason =
                (!resolved(&after)).then(|| "governed disambiguator abstained".into());
            planning.stage_assertion(row.clone(), after.clone())?;
            assertions.changed.insert(row.base.id.clone());
        }
        assertions.before.insert(row.base.id.clone(), row);
        assertions.after.insert(after.base.id.clone(), after);
        result.assertions_reprocessed += 1;
    }
    result.entities_created = resolver.created;
    let selected = assertions.after.keys().cloned().collect::<BTreeSet<_>>();
    let mut materialized = BTreeSet::new();
    let mut retired = BTreeSet::new();
    let mut event_materialized = BTreeSet::new();
    let mut invalidated_subjects = BTreeSet::new();
    for row in all_rows(&RelRepository::new(storage.clone()), space).await? {
        if row.status == ClaimStatus::Superseded {
            continue;
        }
        if !row
            .evidence
            .iter()
            .any(|item| selected.contains(item.assertion.id()))
        {
            continue;
        }
        let row = require_current(planning, &reader, row).await?;
        materialized.extend(row.evidence.iter().map(|item| item.assertion.id().clone()));
        if !row
            .evidence
            .iter()
            .any(|item| assertions.changed.contains(item.assertion.id()))
        {
            continue;
        }
        let replacements = rewrite_rel(planning, &row, &mut assertions, options).await?;
        let mut old = row.clone();
        old.status = ClaimStatus::Superseded;
        old.base.deleted_at = Some(Utc::now());
        old.base.updated_at = Utc::now();
        planning.stage(Some(row.clone()), old)?;
        retired.insert(row.base.id.to_string());
        invalidated_subjects.insert(row.subject.id().clone());
        result.rels_superseded += 1;
        for replacement in replacements {
            invalidated_subjects.insert(replacement.subject.id().clone());
            stage_freshness(
                planning,
                &replacement.base,
                "rel",
                &replacement.evidence,
                replacement.confidence,
            )?;
            planning.stage(None, replacement)?;
        }
    }
    for row in all_rows(&AttributeRepository::new(storage.clone()), space).await? {
        if row.status == ClaimStatus::Superseded {
            continue;
        }
        if !row
            .evidence
            .iter()
            .any(|item| selected.contains(item.assertion.id()))
        {
            continue;
        }
        let row = require_current(planning, &reader, row).await?;
        materialized.extend(row.evidence.iter().map(|item| item.assertion.id().clone()));
        if !row
            .evidence
            .iter()
            .any(|item| assertions.changed.contains(item.assertion.id()))
        {
            continue;
        }
        let replacements = rewrite_attribute(planning, &row, &mut assertions, options).await?;
        let mut old = row.clone();
        old.status = ClaimStatus::Superseded;
        old.base.deleted_at = Some(Utc::now());
        old.base.updated_at = Utc::now();
        planning.stage(Some(row.clone()), old)?;
        retired.insert(row.base.id.to_string());
        invalidated_subjects.insert(row.subject.id().clone());
        result.attributes_superseded += 1;
        for replacement in replacements {
            invalidated_subjects.insert(replacement.subject.id().clone());
            stage_freshness(
                planning,
                &replacement.base,
                "attribute",
                &replacement.evidence,
                replacement.confidence,
            )?;
            planning.stage(None, replacement)?;
        }
    }
    // Projection of each grounding document, observed once per revision and
    // shared by the replacement and materialization decisions below.
    let mut doc_projections = BTreeMap::new();
    // Kept (not replaced) events whose subject requirements had to grow, so
    // their freshness projection follows the same envelope.
    let mut kept = BTreeMap::new();
    // A changed participant binding does not change what happened or its exact
    // source time. Keep ALL original event support; never copy union-supported
    // temporal details into subsets which may not independently entail them.
    for row in all_rows(&EventRepository::new(storage.clone()), space).await? {
        if row.status == EventStatus::Superseded
            || !row
                .assertion_refs
                .iter()
                .any(|item| selected.contains(item.id()))
        {
            continue;
        }
        // A rejected event is a review decision about this support. Changed
        // participant bindings are not new evidence, so neither replace it nor
        // materialize a fresh twin for its assertions below. That decision
        // reads this exact row (status and assertion refs), so it is a plan
        // dependency: a concurrent status or support change fails the commit
        // with canonical_dependency_changed and the plan is reprepared.
        let rejected = row.status == EventStatus::Rejected;
        let row = if rejected {
            planning.observe(&row)?;
            row
        } else {
            require_current(planning, &reader, row).await?
        };
        event_materialized.extend(row.assertion_refs.iter().map(|item| item.id().clone()));
        if !row
            .assertion_refs
            .iter()
            .any(|item| assertions.changed.contains(item.id()))
        {
            continue;
        }
        let mut support = Vec::with_capacity(row.assertion_refs.len());
        for reference in &row.assertion_refs {
            support.push(
                if let Some(current) = assertions.after.get(reference.id()) {
                    current.clone()
                } else {
                    planning
                        .get_assertion(space, reference.id())
                        .await?
                        .ok_or_else(|| invalid("event support is no longer current"))?
                },
            );
        }
        // Replacing the event would retire it and publish a fresh Candidate
        // twin, i.e. refine the household timeline from the changed support.
        // Inline promotion never lets an internal review document create or
        // refine an event, so when any changed supporting assertion grounds in
        // one, keep the existing event (no retirement, no replacement). Its
        // assertions stay materialized, so no twin either.
        let mut keep = rejected;
        for assertion in &support {
            if keep {
                break;
            }
            keep = assertions.changed.contains(&assertion.base.id)
                && evidence_doc_projection(
                    planning,
                    storage,
                    space,
                    assertion,
                    &mut doc_projections,
                )
                .await?
                .as_ref()
                .is_some_and(host_projection_suppresses_timeline_events);
        }
        if keep {
            if let Some(after) = kept_event_envelope(&row, &support)? {
                kept.insert(row.base.id.to_string(), after.base.clone());
                planning.stage_restricted(Some(row), after)?;
            }
            continue;
        }
        let mut after = row.clone();
        let mut evidence = Vec::new();
        for assertion in support {
            if !resolved(&assertion) {
                return Err(invalid(
                    "event re-resolution requires all original participants to remain resolved",
                ));
            }
            add_subjects(&mut after.base, &assertion.base);
            if let Some(link) = &assertion.subject_entity {
                add_entity(&mut after.base, link);
            }
            if let Some(link) = &assertion.object_entity {
                add_entity(&mut after.base, link);
            }
            if !contains_authorization(&after.base, &assertion.base)? {
                return Err(invalid("event support crosses policy partitions"));
            }
            evidence.push(evidence_ref(&assertion));
        }
        after.base = prepared_base(TableName::Event, &after.base, options);
        after.status = EventStatus::Candidate;
        let mut old = row.clone();
        old.status = EventStatus::Superseded;
        old.base.deleted_at = Some(Utc::now());
        old.base.updated_at = Utc::now();
        planning.stage(Some(row.clone()), old)?;
        retired.insert(row.base.id.to_string());
        stage_freshness(planning, &after.base, "event", &evidence, after.confidence)?;
        planning.stage(None, after)?;
        result.events_superseded += 1;
    }
    // Previously abstained assertions can become useful without manufacturing
    // another assertion or borrowing another source's claim envelope.
    for id in selected.difference(&materialized) {
        let assertion = assertions
            .after
            .get(id)
            .ok_or_else(|| invalid("selected assertion missing"))?;
        if !resolved(assertion) || skipped(assertion) {
            continue;
        }
        let subject = assertion
            .subject_entity
            .clone()
            .ok_or_else(|| invalid("resolved assertion lacks subject"))?;
        invalidated_subjects.insert(subject.id().clone());
        let evidence = vec![evidence_ref(assertion)];
        if let Some(object) = &assertion.object_entity {
            let mut base = prepared_base(TableName::Rel, &assertion.base, options);
            add_entity(&mut base, object);
            let row = Rel {
                base,
                subject,
                object: object.clone(),
                rel_type: predicate(assertion).into(),
                status: ClaimStatus::Candidate,
                confidence: assertion.confidence,
                validity_window: None,
                evidence,
                temporal_audit: None,
            };
            stage_freshness(planning, &row.base, "rel", &row.evidence, row.confidence)?;
            planning.stage(None, row)?;
        } else if assertion.object_literal.is_some() {
            let value = original_literal(planning, storage, assertion).await?;
            let row = Attribute {
                base: prepared_base(TableName::Attribute, &assertion.base, options),
                subject,
                attr_type: predicate(assertion).into(),
                value,
                status: ClaimStatus::Candidate,
                confidence: assertion.confidence,
                validity_window: None,
                evidence,
                temporal_audit: None,
            };
            stage_freshness(
                planning,
                &row.base,
                "attribute",
                &row.evidence,
                row.confidence,
            )?;
            planning.stage(None, row)?;
        }
    }
    for id in selected.difference(&event_materialized) {
        let assertion = assertions
            .after
            .get(id)
            .ok_or_else(|| invalid("selected event assertion missing"))?;
        if !resolved(assertion) || skipped(assertion) {
            continue;
        }
        // Inline promotion keeps internal review bookkeeping out of the
        // household timeline; re-resolution must not materialize it either.
        let projection =
            evidence_doc_projection(planning, storage, space, assertion, &mut doc_projections)
                .await?;
        if projection
            .as_ref()
            .is_some_and(host_projection_suppresses_timeline_events)
        {
            continue;
        }
        let mut base = prepared_base(TableName::Event, &assertion.base, options);
        if let Some(subject) = &assertion.subject_entity {
            add_entity(&mut base, subject);
        }
        if let Some(object) = &assertion.object_entity {
            add_entity(&mut base, object);
        }
        let object = assertion
            .object_text
            .clone()
            .or_else(|| {
                assertion.object_literal.as_ref().map(|value| {
                    value
                        .as_str()
                        .map_or_else(|| value.to_string(), str::to_owned)
                })
            })
            .ok_or_else(|| invalid("resolved event assertion lacks object"))?;
        let title = format!(
            "{} {} {}",
            assertion.subject_text,
            assertion.predicate_raw.replace('_', " "),
            object
        );
        let source_refs = base
            .provenance
            .sources
            .iter()
            .cloned()
            .map(Link::new)
            .collect();
        let mut event = Event::new(
            base,
            title,
            vec![Link::new(assertion.base.id.clone())],
            source_refs,
        )
        .map_err(|error| invalid(error.to_string()))?;
        event.time_start = assertion.source_event_time;
        // Assertion intentionally retains no source precision. Unknown is
        // conservative; guessing day/second precision would invent evidence.
        event.confidence = assertion.confidence;
        event.time_confidence = assertion.confidence;
        event.metadata = serde_json::json!({"origin":"re_resolve", "time_basis": if event.time_start.is_some() {"source_event_time"} else {"unknown"}});
        if let (Some(projection), Some(metadata)) = (
            projection.as_ref().and_then(host_projection_from_metadata),
            event.metadata.as_object_mut(),
        ) {
            metadata.insert(HOST_PROJECTION_METADATA_KEY.into(), projection.clone());
        }
        stage_freshness(
            planning,
            &event.base,
            "event",
            &[evidence_ref(assertion)],
            event.confidence,
        )?;
        planning.stage(None, event)?;
    }
    for row in all_rows::<Freshness, _>(&FreshnessRepository::new(storage.clone()), space).await? {
        if let Some(envelope) = kept.get(&row.target) {
            planning.observe(&row)?;
            let mut after = row.clone();
            after.base.security.clone_from(&envelope.security);
            after.base.subjects.clone_from(&envelope.subjects);
            after.base.subjects_state = envelope.subjects_state;
            after.base.updated_at = Utc::now();
            planning.stage_restricted(Some(row), after)?;
            continue;
        }
        if !retired.contains(&row.target) {
            continue;
        }
        planning.observe(&row)?;
        let mut after = row.clone();
        after.base.deleted_at = Some(Utc::now());
        after.base.updated_at = Utc::now();
        planning.stage(Some(row), after)?;
    }
    for subject in invalidated_subjects {
        planning
            .mark_slots_stale_by_entity(space, &subject, "assertion identity was re-resolved")
            .await?;
    }
    reader.verify_current().await?;
    Ok(result)
}

#[cfg(test)]
#[path = "reprocess_tests.rs"]
mod tests;
