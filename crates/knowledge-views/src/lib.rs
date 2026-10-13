//! Graph-native derived knowledge projections for Elephant.
//!
//! This crate compiles canonical graph records into immutable, cited views. It
//! deliberately performs no source ingestion and grants no truth authority to
//! generated Markdown.

#![allow(clippy::missing_errors_doc)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use elephant_storage::{
    AssertionRepository, AttributeRepository, EntityRepository, EventRepository,
    FreshnessRepository, KnowledgeViewRepository, RelRepository, Repository, StorageClient,
    StorageError, StorageResult, TruthSlotRepository,
};
use elephant_types::records::base::{BaseFields, Extractor, Provenance, Security, SubjectsState};
use elephant_types::records::{
    Assertion, Attribute, CitationKind, CompilerReceipt, Entity, Event, Freshness, HasSecurity,
    KnowledgeCitation, KnowledgeDependency, KnowledgeStatement, KnowledgeView, KnowledgeViewBlock,
    KnowledgeViewKind, KnowledgeViewPublicationStatus, KnowledgeViewRevision, KnowledgeViewSurface,
    ProjectionFreshness, ProjectionState, Rel, Source, StatementDerivation, StatementVerification,
    TruthSlot, ValidationReceipt, ViewBlockKind,
};
use elephant_types::{Link, RecordId, TableName};
use sha2::{Digest, Sha256};
use uuid::Uuid;

mod graph_views;
mod source;

pub use graph_views::{CompiledGraphView, GraphViewCompiler, PreparedGraphView};
pub use source::{CompiledSourceView, SourceViewCompiler, SourceViewLimits};

/// Stable identity of the deterministic entity-view compiler.
pub const ENTITY_COMPILER_NAME: &str = "elephant.entity_view";
/// Contract version of the deterministic entity-view compiler.
pub const ENTITY_COMPILER_VERSION: u32 = 6;

/// Bounds for one entity projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntityViewLimits {
    /// Maximum outgoing relationships.
    pub outgoing_relations: usize,
    /// Maximum incoming relationships.
    pub incoming_relations: usize,
    /// Maximum literal attributes.
    pub attributes: usize,
    /// Maximum temporal events.
    pub events: usize,
}

impl Default for EntityViewLimits {
    fn default() -> Self {
        Self {
            outgoing_relations: 25,
            incoming_relations: 25,
            attributes: 50,
            events: 25,
        }
    }
}

/// Result of a compile-or-reuse operation.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledEntityView {
    /// Stable view definition.
    pub view: KnowledgeView,
    /// Current immutable revision.
    pub revision: KnowledgeViewRevision,
    /// Whether this call published a new revision.
    pub changed: bool,
}

/// Require every contributing record to fit the complete derived envelope.
/// Policy names are opaque: `default` is not a universal weaker policy.
#[must_use]
pub fn projection_envelope_covers(output: &BaseFields, input: &BaseFields) -> bool {
    output.space_id == input.space_id
        && !input.is_deleted()
        && output.security.policy_id == input.security.policy_id
        && output.security.level >= input.security.level
        && input
            .security
            .labels
            .iter()
            .all(|label| output.security.labels.contains(label))
        && input
            .security
            .handling
            .iter()
            .filter(|flag| flag.as_str() != "handling:subjectless_ok")
            .all(|flag| output.security.handling.contains(flag))
        && (!output
            .security
            .handling
            .iter()
            .any(|flag| flag == "handling:subjectless_ok")
            || input
                .security
                .handling
                .iter()
                .any(|flag| flag == "handling:subjectless_ok"))
        && input
            .subjects
            .iter()
            .all(|subject| output.subjects.contains(subject))
        && match input.subjects_state {
            SubjectsState::Complete => true,
            SubjectsState::Partial => output.subjects_state != SubjectsState::Complete,
            SubjectsState::Unknown => output.subjects_state == SubjectsState::Unknown,
        }
}

/// Validate exact current evidence, support and recorded dependency versions.
/// Used for both cache reuse and newly prepared immutable projections.
pub async fn projection_is_current(
    storage: &StorageClient,
    revision: &KnowledgeViewRevision,
) -> StorageResult<bool> {
    let reader = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone());
    Ok(reader
        .project(revision.clone(), &|base| {
            projection_envelope_covers(&revision.base, base)
        })
        .await?
        .is_some())
}

/// Fence read-only preparation/cache returns against the original read epoch.
/// Verification never initializes a missing epoch or grants space authority.
pub async fn verify_projection_epoch(
    storage: &StorageClient,
    space: &str,
    observed_epoch: &str,
) -> StorageResult<()> {
    if observed_epoch.is_empty()
        || elephant_storage::canonical_commit::CanonicalRepository::new(storage.clone())
            .current_epoch(space)
            .await?
            != observed_epoch
    {
        return Err(StorageError::Conflict(
            "Wiki inputs changed during preparation".into(),
        ));
    }
    Ok(())
}

/// Compiler and publication service for entity-rooted knowledge views.
#[derive(Clone, Debug)]
pub struct EntityViewCompiler {
    storage: StorageClient,
    views: KnowledgeViewRepository,
    entities: EntityRepository,
    relations: RelRepository,
    attributes: AttributeRepository,
    events: EventRepository,
    truth_slots: TruthSlotRepository,
    assertions: AssertionRepository,
    freshness: FreshnessRepository,
}

impl EntityViewCompiler {
    /// Creates a compiler over one Elephant storage client.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            storage: storage.clone(),
            views: KnowledgeViewRepository::new(storage.clone()),
            entities: EntityRepository::new(storage.clone()),
            relations: RelRepository::new(storage.clone()),
            attributes: AttributeRepository::new(storage.clone()),
            events: EventRepository::new(storage.clone()),
            truth_slots: TruthSlotRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage.clone()),
            freshness: FreshnessRepository::new(storage),
        }
    }

    /// Returns the repository used for persisted views.
    #[must_use]
    pub const fn repository(&self) -> &KnowledgeViewRepository {
        &self.views
    }

    /// Looks up the existing security-scoped view for an entity.
    pub async fn find_existing(
        &self,
        space_id: &str,
        entity: &Entity,
    ) -> StorageResult<Option<KnowledgeView>> {
        let fingerprint = security_fingerprint(&entity.base.security)?;
        self.views
            .find_by_key(space_id, &entity_view_key(&entity.base.id, &fingerprint))
            .await
    }

    /// Compiles and atomically publishes an entity view unless its exact input
    /// receipt already matches the current revision.
    pub async fn compile(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limits: EntityViewLimits,
        force: bool,
    ) -> StorageResult<CompiledEntityView> {
        let observed_epoch = Box::pin(
            elephant_storage::canonical_commit::CanonicalRepository::new(self.storage.clone())
                .epoch(space_id),
        )
        .await?;
        // The selection future returns before grounding is constructed; the
        // grounding future returns before publication is constructed. Their
        // compiled poll frames are never retained beneath one another.
        let inputs = Box::pin(self.prepare_material(space_id, entity_id, limits)).await?;
        let prepared =
            Box::pin(self.prepare_revision(space_id, entity_id, force, &observed_epoch, inputs))
                .await?;
        if !prepared.changed {
            return Ok(*prepared);
        }
        let (view, revision) = Box::pin(self.views.publish_at_epoch(
            prepared.view,
            prepared.revision,
            &observed_epoch,
        ))
        .await?;
        Ok(CompiledEntityView {
            view,
            revision,
            changed: true,
        })
    }

    #[allow(clippy::too_many_lines)] // Preserves the original ordered, bounded selection.
    async fn prepare_material(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        limits: EntityViewLimits,
    ) -> StorageResult<Box<EntityViewInputs>> {
        let entity = Box::pin(self.entities.get(space_id, entity_id))
            .await?
            .ok_or_else(|| StorageError::NotFound(entity_id.to_string()))?;
        let security = entity.base.security.clone();
        let fingerprint = security_fingerprint(&security)?;
        let key = entity_view_key(entity_id, &fingerprint);
        let existing = Box::pin(self.views.find_by_key(space_id, &key)).await?;
        // Recompute the bounded selection before cache reuse: an old dependency
        // list cannot prove that no new matching relation or document appeared.

        let mut outgoing = Box::pin(self.relations.find_outgoing(space_id, entity_id)).await?;
        let mut incoming = Box::pin(self.relations.find_incoming(space_id, entity_id)).await?;
        outgoing.retain(|record| record.base.security == security);
        incoming.retain(|record| record.base.security == security);
        sort_relations(&mut outgoing);
        sort_relations(&mut incoming);
        outgoing.truncate(limits.outgoing_relations);
        incoming.truncate(limits.incoming_relations);

        let mut attributes =
            Box::pin(self.attributes.find_by_subject_entity(space_id, entity_id)).await?;
        attributes.retain(|record| record.base.security == security);
        attributes.sort_by(|left, right| {
            left.attr_type
                .cmp(&right.attr_type)
                .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
        });
        attributes.truncate(limits.attributes);

        let mut events = Box::pin(self.events.find_by_subject_entity(
            space_id,
            entity_id,
            limits.events.saturating_mul(2),
        ))
        .await?;
        events.retain(|record| record.base.security == security);
        events.truncate(limits.events);

        let mut truth_slots = Box::pin(
            self.truth_slots
                .find_by_subject_entity(space_id, &entity_id.to_string()),
        )
        .await?;
        truth_slots.retain(|record| record.base.security == security);

        let counterpart_ids = outgoing
            .iter()
            .map(|record| record.object.id().clone())
            .chain(incoming.iter().map(|record| record.subject.id().clone()))
            .collect::<BTreeSet<_>>();
        let mut counterparts = HashMap::new();
        for counterpart_id in counterpart_ids {
            if let Some(counterpart) =
                Box::pin(self.entities.get(space_id, &counterpart_id)).await?
            {
                if counterpart.base.security == security {
                    counterparts.insert(counterpart_id, counterpart);
                }
            }
        }

        let assertion_ids = referenced_assertion_ids(&outgoing, &incoming, &attributes, &events);
        let mut assertions = HashMap::new();
        for assertion_id in assertion_ids {
            if let Some(assertion) = Box::pin(self.assertions.get(space_id, &assertion_id)).await? {
                if assertion.base.security == security {
                    assertions.insert(assertion_id, assertion);
                }
            }
        }

        let claim_ids = outgoing
            .iter()
            .map(|record| record.base.id.to_string())
            .chain(incoming.iter().map(|record| record.base.id.to_string()))
            .chain(attributes.iter().map(|record| record.base.id.to_string()))
            .chain(truth_slots.iter().map(|record| record.base.id.to_string()))
            .chain(events.iter().map(|record| record.base.id.to_string()))
            .chain(std::iter::once(entity.base.id.to_string()))
            .collect::<BTreeSet<_>>();
        let mut freshness_records = HashMap::new();
        for claim_id in claim_ids {
            if let Some(record) =
                Box::pin(self.freshness.find_by_target(space_id, &claim_id)).await?
            {
                if record.base.security == security {
                    freshness_records.insert(claim_id, record);
                }
            }
        }

        let material = EntityViewMaterial {
            entity,
            outgoing,
            incoming,
            attributes,
            events,
            truth_slots,
            counterparts,
            assertions,
            freshness: freshness_records,
            security_fingerprint: fingerprint,
        };
        Ok(Box::new(EntityViewInputs {
            material,
            security,
            key,
            existing,
        }))
    }

    // Move selected inputs between phases without retaining their poll frames.
    #[allow(clippy::boxed_local)]
    async fn prepare_revision(
        &self,
        space_id: &str,
        entity_id: &RecordId,
        force: bool,
        observed_epoch: &str,
        inputs: Box<EntityViewInputs>,
    ) -> StorageResult<Box<CompiledEntityView>> {
        let EntityViewInputs {
            material,
            security,
            key,
            existing,
        } = *inputs;
        let mut draft = build_revision_draft(&material)?;
        let evidence_envelopes = Box::pin(ground_citation_evidence(
            &self.storage,
            space_id,
            &security,
            &mut draft,
            ENTITY_COMPILER_NAME,
            ENTITY_COMPILER_VERSION,
            &[],
        ))
        .await?;

        if !force {
            if let Some(existing_view) = existing.as_ref() {
                if let Some(current) =
                    Box::pin(self.views.get_current_revision(existing_view)).await?
                {
                    let freshness_due = current
                        .freshness
                        .valid_until
                        .is_some_and(|deadline| deadline <= Utc::now());
                    if existing_view.state != ProjectionState::Dirty
                        && (!freshness_due || current.freshness.expired_claims > 0)
                        && current.compiler.dependency_hash == draft.dependency_hash
                        && current.compiler.version == ENTITY_COMPILER_VERSION
                        && Box::pin(projection_is_current(&self.storage, &current)).await?
                    {
                        Box::pin(verify_projection_epoch(
                            &self.storage,
                            space_id,
                            observed_epoch,
                        ))
                        .await?;
                        return Ok(Box::new(CompiledEntityView {
                            view: existing_view.clone(),
                            revision: current,
                            changed: false,
                        }));
                    }
                }
            }
        }

        let compiled = assemble_entity_revision(
            space_id,
            entity_id,
            EntityRevisionAssembly {
                material,
                security,
                key,
                existing,
                draft,
                evidence_envelopes,
            },
        );
        if !Box::pin(projection_is_current(&self.storage, &compiled.revision)).await? {
            return Err(StorageError::Conflict(
                "Projection has invalid or withdrawn evidence".into(),
            ));
        }
        Ok(Box::new(compiled))
    }
}

struct EntityViewInputs {
    material: EntityViewMaterial,
    security: Security,
    key: String,
    existing: Option<KnowledgeView>,
}

struct EntityRevisionAssembly {
    material: EntityViewMaterial,
    security: Security,
    key: String,
    existing: Option<KnowledgeView>,
    draft: RevisionDraft,
    evidence_envelopes: Vec<BaseFields>,
}

// This pure assembly has no storage future. Its frame is gone before the
// original current-evidence validation and epoch-fenced publication are polled.
#[inline(never)]
#[allow(clippy::too_many_lines)]
fn assemble_entity_revision(
    space_id: &str,
    entity_id: &RecordId,
    assembly: EntityRevisionAssembly,
) -> CompiledEntityView {
    let EntityRevisionAssembly {
        material,
        security,
        key,
        existing,
        draft,
        evidence_envelopes,
    } = assembly;
    let now = Utc::now();
    let view_id = existing
        .as_ref()
        .map_or_else(|| deterministic_view_id(&key), |view| view.base.id.clone());
    let revision_number = existing
        .as_ref()
        .map_or(1, |view| view.revision_count.saturating_add(1));
    let revision_id = RecordId::new_v7(TableName::KnowledgeViewRevision);
    let mut scope = ProjectionSubjects::default();
    scope.include(&material.entity);
    for record in material.outgoing.iter().chain(&material.incoming) {
        scope.include(record);
    }
    for record in &material.attributes {
        scope.include(record);
    }
    for record in &material.events {
        scope.include(record);
    }
    for record in &material.truth_slots {
        scope.include(record);
    }
    for record in material.counterparts.values() {
        scope.include(record);
    }
    for record in material.assertions.values() {
        scope.include(record);
    }
    for record in material.freshness.values() {
        scope.include(record);
    }
    for base in &evidence_envelopes {
        scope.include_base(base);
    }
    let (subjects, subjects_state) = scope.finish();
    let mut provenance = projection_provenance(
        &draft.citations,
        &draft.dependency_hash,
        ENTITY_COMPILER_NAME,
        ENTITY_COMPILER_VERSION,
    );
    extend_projection_sources(&mut provenance, &evidence_envelopes);

    let mut view_base = existing.as_ref().map_or_else(
        || {
            let mut base = BaseFields::new(
                view_id.clone(),
                space_id.to_string(),
                ENTITY_COMPILER_NAME.to_string(),
            );
            base.security = security.clone();
            base.subjects.clone_from(&subjects);
            base.subjects_state = subjects_state;
            base.provenance = provenance.clone();
            base
        },
        |view| view.base.clone(),
    );
    view_base.updated_at = now;
    view_base.deleted_at = None;
    view_base.security.clone_from(&security);
    view_base.subjects.clone_from(&subjects);
    view_base.subjects_state = subjects_state;
    view_base.provenance.clone_from(&provenance);

    let state = draft.freshness.state;
    let state_reason = projection_state_reason(&draft.freshness);
    let view = KnowledgeView {
        base: view_base,
        key,
        kind: KnowledgeViewKind::Entity,
        surface: KnowledgeViewSurface::Inspector,
        publication_status: KnowledgeViewPublicationStatus::Published,
        root_record: entity_id.to_string(),
        title: material.entity.display_name.clone(),
        slug: entity_slug(&material.entity),
        security_fingerprint: material.security_fingerprint.clone(),
        current_revision: Some(revision_id.to_string()),
        revision_count: revision_number,
        state,
        state_reason,
        published_at: Some(now),
    };

    let mut revision_base = BaseFields::new(
        revision_id,
        space_id.to_string(),
        ENTITY_COMPILER_NAME.to_string(),
    );
    revision_base.security = security;
    revision_base.subjects = subjects;
    revision_base.subjects_state = subjects_state;
    revision_base.provenance = provenance;
    let revision = KnowledgeViewRevision {
        base: revision_base,
        view_id: view.base.id.to_string(),
        revision: revision_number,
        kind: KnowledgeViewKind::Entity,
        root_record: entity_id.to_string(),
        blocks: draft.blocks,
        rendered_markdown: draft.rendered_markdown,
        citations: draft.citations,
        dependencies: draft.dependencies,
        freshness: draft.freshness,
        validation: draft.validation,
        compiler: CompilerReceipt {
            compiler: ENTITY_COMPILER_NAME.to_string(),
            version: ENTITY_COMPILER_VERSION,
            dependency_hash: draft.dependency_hash,
            policy_fingerprint: material.security_fingerprint,
        },
        compiled_at: now,
        supersedes: existing.and_then(|view| view.current_revision),
    };
    CompiledEntityView {
        view,
        revision,
        changed: true,
    }
}

struct EntityViewMaterial {
    entity: Entity,
    outgoing: Vec<Rel>,
    incoming: Vec<Rel>,
    attributes: Vec<Attribute>,
    events: Vec<Event>,
    truth_slots: Vec<TruthSlot>,
    counterparts: HashMap<RecordId, Entity>,
    assertions: HashMap<RecordId, Assertion>,
    freshness: HashMap<String, Freshness>,
    security_fingerprint: String,
}

struct RevisionDraft {
    blocks: Vec<KnowledgeViewBlock>,
    rendered_markdown: String,
    citations: Vec<KnowledgeCitation>,
    dependencies: Vec<KnowledgeDependency>,
    freshness: ProjectionFreshness,
    validation: ValidationReceipt,
    dependency_hash: String,
}

#[allow(clippy::too_many_lines)] // Keeps the ordered block and citation ledger construction visible.
fn build_revision_draft(material: &EntityViewMaterial) -> StorageResult<RevisionDraft> {
    let mut dependencies = BTreeMap::<String, KnowledgeDependency>::new();
    let mut citations = Vec::new();
    add_dependency(
        &mut dependencies,
        &material.entity.base.id,
        "entity",
        "root",
        Some(material.entity.base.updated_at),
    );
    let overview_citation = push_record_citation(
        &mut citations,
        &material.entity.base.id,
        CitationKind::GraphRecord,
    );

    let mut blocks = vec![supported_block(
        "overview",
        ViewBlockKind::Overview,
        "Overview",
        vec![direct_statement(
            "overview-1",
            overview_markdown(&material.entity, &overview_citation),
            vec![overview_citation],
        )],
    )];

    let current_claims = material
        .truth_slots
        .iter()
        .flat_map(|slot| {
            slot.current_values
                .iter()
                .map(|value| value.claim_id.clone())
        })
        .collect::<BTreeSet<_>>();
    let mut unsupported_claims = 0_u32;

    let mut fact_statements = Vec::new();
    for attribute in &material.attributes {
        add_dependency(
            &mut dependencies,
            &attribute.base.id,
            "attribute",
            "fact",
            Some(attribute.base.updated_at),
        );
        let ids = push_evidence_citations(
            &mut citations,
            &mut dependencies,
            &attribute.base.id,
            &attribute.evidence,
            &material.assertions,
        );
        if attribute.evidence.is_empty() {
            unsupported_claims = unsupported_claims.saturating_add(1);
        }
        let current =
            current_claims.is_empty() || current_claims.contains(&attribute.base.id.to_string());
        fact_statements.push(direct_statement(
            &format!("fact-{}", fact_statements.len() + 1),
            format!(
                "- **{}:** {} _(status: {}, current: {}, confidence: {:.2})_{}",
                escape_markdown(&attribute.attr_type),
                escape_markdown(&literal_text(&attribute.value)),
                attribute.status,
                current,
                attribute.confidence,
                citation_marks(&ids),
            ),
            ids,
        ));
    }
    if !fact_statements.is_empty() {
        blocks.push(supported_block(
            "current-facts",
            ViewBlockKind::CurrentFacts,
            "Current facts",
            fact_statements,
        ));
    }

    let mut relation_statements = Vec::new();
    for (direction, relation, counterpart_id) in material
        .outgoing
        .iter()
        .map(|relation| ("outgoing", relation, relation.object.id()))
        .chain(
            material
                .incoming
                .iter()
                .map(|relation| ("incoming", relation, relation.subject.id())),
        )
    {
        add_dependency(
            &mut dependencies,
            &relation.base.id,
            "rel",
            "relationship",
            Some(relation.base.updated_at),
        );
        if let Some(counterpart) = material.counterparts.get(counterpart_id) {
            add_dependency(
                &mut dependencies,
                &counterpart.base.id,
                "entity",
                "relationship_label",
                Some(counterpart.base.updated_at),
            );
        }
        let ids = push_evidence_citations(
            &mut citations,
            &mut dependencies,
            &relation.base.id,
            &relation.evidence,
            &material.assertions,
        );
        if relation.evidence.is_empty() {
            unsupported_claims = unsupported_claims.saturating_add(1);
        }
        let counterpart = material.counterparts.get(counterpart_id).map_or_else(
            || counterpart_id.to_string(),
            |entity| entity.display_name.clone(),
        );
        let current =
            current_claims.is_empty() || current_claims.contains(&relation.base.id.to_string());
        relation_statements.push(direct_statement(
            &format!("relationship-{}", relation_statements.len() + 1),
            format!(
                "- **{}** {} **{}** _(status: {}, current: {}, confidence: {:.2})_{}",
                escape_markdown(direction),
                escape_markdown(&relation.rel_type),
                escape_markdown(&counterpart),
                relation.status,
                current,
                relation.confidence,
                citation_marks(&ids),
            ),
            ids,
        ));
    }
    if !relation_statements.is_empty() {
        blocks.push(supported_block(
            "relationships",
            ViewBlockKind::Relationships,
            "Relationships",
            relation_statements,
        ));
    }

    let mut event_statements = Vec::new();
    for event in &material.events {
        add_dependency(
            &mut dependencies,
            &event.base.id,
            "event",
            "timeline",
            Some(event.base.updated_at),
        );
        let ids = push_event_citations(
            &mut citations,
            &mut dependencies,
            event,
            &material.assertions,
        );
        if event.assertion_refs.is_empty() && event.source_refs.is_empty() {
            unsupported_claims = unsupported_claims.saturating_add(1);
        }
        let time = event
            .time_start
            .map_or_else(|| "time unknown".to_string(), |value| value.to_rfc3339());
        event_statements.push(direct_statement(
            &format!("event-{}", event_statements.len() + 1),
            format!(
                "- **{}** - {} _(status: {}, confidence: {:.2})_{}",
                escape_markdown(&time),
                escape_markdown(&event.title),
                event.status,
                event.confidence,
                citation_marks(&ids),
            ),
            ids,
        ));
    }
    if !event_statements.is_empty() {
        blocks.push(supported_block(
            "timeline",
            ViewBlockKind::Timeline,
            "Timeline",
            event_statements,
        ));
    }

    let stale_truth_slots = u32::try_from(
        material
            .truth_slots
            .iter()
            .filter(|slot| slot.stale)
            .count(),
    )
    .unwrap_or(u32::MAX);
    let unresolved_conflicts = u32::try_from(
        material
            .truth_slots
            .iter()
            .filter(|slot| slot.conflict_group.is_some())
            .count(),
    )
    .unwrap_or(u32::MAX);
    let mut conflict_statements = Vec::new();
    for slot in material
        .truth_slots
        .iter()
        .filter(|slot| slot.stale || slot.conflict_group.is_some())
    {
        add_dependency(
            &mut dependencies,
            &slot.base.id,
            "truth_slot",
            "truth_state",
            Some(slot.base.updated_at),
        );
        let citation =
            push_record_citation(&mut citations, &slot.base.id, CitationKind::GraphRecord);
        conflict_statements.push(direct_statement(
            &format!("conflict-{}", conflict_statements.len() + 1),
            format!(
                "- **{}** - stale: {}, unresolved conflict: {}{}",
                escape_markdown(&slot.predicate_id),
                slot.stale,
                slot.conflict_group.is_some(),
                citation_marks(std::slice::from_ref(&citation)),
            ),
            vec![citation],
        ));
    }
    if !conflict_statements.is_empty() {
        blocks.push(supported_block(
            "conflicts",
            ViewBlockKind::Conflicts,
            "Conflicts and stale knowledge",
            conflict_statements,
        ));
    }

    for record in material.freshness.values() {
        add_dependency(
            &mut dependencies,
            &record.base.id,
            "freshness",
            "claim_freshness",
            Some(record.base.updated_at),
        );
    }
    for slot in &material.truth_slots {
        add_dependency(
            &mut dependencies,
            &slot.base.id,
            "truth_slot",
            "truth_state",
            Some(slot.base.updated_at),
        );
    }

    let minimum_claim_score = material
        .freshness
        .values()
        .map(|record| record.score)
        .reduce(f32::min);
    let valid_until = material
        .freshness
        .values()
        .filter_map(|record| record.next_review_at)
        .min();
    let expired_claims = u32::try_from(
        material
            .freshness
            .values()
            .filter(|record| {
                record
                    .next_review_at
                    .is_some_and(|deadline| deadline <= Utc::now())
            })
            .count(),
    )
    .unwrap_or(u32::MAX);
    let state = if unsupported_claims > 0
        || stale_truth_slots > 0
        || unresolved_conflicts > 0
        || expired_claims > 0
    {
        ProjectionState::Degraded
    } else {
        ProjectionState::Current
    };
    let freshness = ProjectionFreshness {
        state,
        minimum_claim_score,
        stale_truth_slots,
        unresolved_conflicts,
        unsupported_claims,
        expired_claims,
        selection_truncated: false,
        valid_until,
    };
    blocks.push(supported_block(
        "evidence-status",
        ViewBlockKind::EvidenceStatus,
        "Evidence status",
        vec![
            operational_statement(
                "projection-state",
                format!("- Projection state: **{}**", projection_state_name(state)),
            ),
            operational_statement(
                "unsupported-claims",
                format!("- Unsupported claims: **{unsupported_claims}**"),
            ),
            operational_statement(
                "expired-claim-reviews",
                format!("- Expired claim reviews: **{expired_claims}**"),
            ),
            operational_statement(
                "stale-truth-slots",
                format!("- Stale truth slots: **{stale_truth_slots}**"),
            ),
            operational_statement(
                "unresolved-conflicts",
                format!("- Unresolved conflicts: **{unresolved_conflicts}**"),
            ),
        ],
    ));

    let dependencies = dependencies.into_values().collect::<Vec<_>>();
    let validation = validate_projection(&blocks, &citations, &dependencies, unsupported_claims);
    if !validation.citations_resolve
        || !validation.factual_blocks_cited
        || !validation.statements_supported
        || !validation.prose_is_structured
    {
        return Err(StorageError::Conflict(format!(
            "Knowledge-view validation failed: {}",
            validation.warnings.join("; ")
        )));
    }
    let dependency_hash = dependency_hash(
        &dependencies,
        &material.security_fingerprint,
        ENTITY_COMPILER_NAME,
        ENTITY_COMPILER_VERSION,
    )?;
    let rendered_markdown = render_page(&material.entity.display_name, &blocks, &citations);

    Ok(RevisionDraft {
        blocks,
        rendered_markdown,
        citations,
        dependencies,
        freshness,
        validation,
        dependency_hash,
    })
}

fn sort_relations(relations: &mut [Rel]) {
    relations.sort_by(|left, right| {
        left.rel_type
            .cmp(&right.rel_type)
            .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
    });
}

fn referenced_assertion_ids(
    outgoing: &[Rel],
    incoming: &[Rel],
    attributes: &[Attribute],
    events: &[Event],
) -> BTreeSet<RecordId> {
    outgoing
        .iter()
        .chain(incoming)
        .flat_map(|record| {
            record
                .evidence
                .iter()
                .map(|evidence| evidence.assertion.id().clone())
        })
        .chain(attributes.iter().flat_map(|record| {
            record
                .evidence
                .iter()
                .map(|evidence| evidence.assertion.id().clone())
        }))
        .chain(events.iter().flat_map(|event| {
            event
                .assertion_refs
                .iter()
                .map(|assertion| assertion.id().clone())
        }))
        .collect()
}

fn push_evidence_citations(
    citations: &mut Vec<KnowledgeCitation>,
    dependencies: &mut BTreeMap<String, KnowledgeDependency>,
    claim_id: &RecordId,
    evidence: &[elephant_types::records::EvidenceRef],
    assertions: &HashMap<RecordId, Assertion>,
) -> Vec<String> {
    if evidence.is_empty() {
        return vec![push_record_citation(
            citations,
            claim_id,
            CitationKind::ClaimRecord,
        )];
    }
    evidence
        .iter()
        .map(|reference| {
            let assertion_id = reference.assertion.id();
            let assertion = assertions.get(assertion_id);
            if let Some(assertion) = assertion {
                add_dependency(
                    dependencies,
                    &assertion.base.id,
                    "assertion",
                    "evidence",
                    Some(assertion.base.updated_at),
                );
            } else {
                add_dependency(dependencies, assertion_id, "assertion", "evidence", None);
            }
            add_dependency(
                dependencies,
                reference.span.revision.id(),
                "doc_revision",
                "evidence_span",
                None,
            );
            if let Some(node) = reference.span.node.as_ref() {
                add_dependency(dependencies, node.id(), "doc_node", "evidence_span", None);
            }
            let source_id = assertion.and_then(|item| item.base.provenance.sources.first());
            if let Some(source_id) = source_id {
                add_dependency(dependencies, source_id, "source", "original_source", None);
            }
            let id = format!("c{}", citations.len() + 1);
            citations.push(KnowledgeCitation {
                id: id.clone(),
                kind: CitationKind::EvidenceSpan,
                record_id: claim_id.to_string(),
                assertion_id: Some(assertion_id.to_string()),
                source_id: source_id.map(ToString::to_string),
                evidence_span: Some(reference.span.clone()),
            });
            id
        })
        .collect()
}

fn push_event_citations(
    citations: &mut Vec<KnowledgeCitation>,
    dependencies: &mut BTreeMap<String, KnowledgeDependency>,
    event: &Event,
    assertions: &HashMap<RecordId, Assertion>,
) -> Vec<String> {
    let mut ids = Vec::new();
    for assertion_link in &event.assertion_refs {
        let assertion = assertions.get(assertion_link.id());
        if let Some(assertion) = assertion {
            add_dependency(
                dependencies,
                &assertion.base.id,
                "assertion",
                "event_evidence",
                Some(assertion.base.updated_at),
            );
            let source_id = assertion.base.provenance.sources.first();
            add_dependency(
                dependencies,
                assertion.evidence.revision.id(),
                "doc_revision",
                "event_evidence_span",
                None,
            );
            if let Some(node) = assertion.evidence.node.as_ref() {
                add_dependency(
                    dependencies,
                    node.id(),
                    "doc_node",
                    "event_evidence_span",
                    None,
                );
            }
            if let Some(source_id) = source_id {
                add_dependency(dependencies, source_id, "source", "original_source", None);
            }
            let id = format!("c{}", citations.len() + 1);
            citations.push(KnowledgeCitation {
                id: id.clone(),
                kind: CitationKind::EvidenceSpan,
                record_id: event.base.id.to_string(),
                assertion_id: Some(assertion.base.id.to_string()),
                source_id: source_id.map(ToString::to_string),
                evidence_span: Some(assertion.evidence.clone()),
            });
            ids.push(id);
        }
    }
    for source in &event.source_refs {
        add_dependency(dependencies, source.id(), "source", "event_source", None);
        let id = format!("c{}", citations.len() + 1);
        citations.push(KnowledgeCitation {
            id: id.clone(),
            kind: CitationKind::GraphRecord,
            record_id: event.base.id.to_string(),
            assertion_id: None,
            source_id: Some(source.id().to_string()),
            evidence_span: None,
        });
        ids.push(id);
    }
    if ids.is_empty() {
        ids.push(push_record_citation(
            citations,
            &event.base.id,
            CitationKind::ClaimRecord,
        ));
    }
    ids
}

fn push_record_citation(
    citations: &mut Vec<KnowledgeCitation>,
    record_id: &RecordId,
    kind: CitationKind,
) -> String {
    let id = format!("c{}", citations.len() + 1);
    citations.push(KnowledgeCitation {
        id: id.clone(),
        kind,
        record_id: record_id.to_string(),
        assertion_id: None,
        source_id: None,
        evidence_span: None,
    });
    id
}

fn add_dependency(
    dependencies: &mut BTreeMap<String, KnowledgeDependency>,
    record_id: &RecordId,
    record_type: &str,
    role: &str,
    observed_updated_at: Option<DateTime<Utc>>,
) {
    dependencies
        .entry(record_id.to_string())
        .and_modify(|dependency| {
            if dependency.observed_updated_at < observed_updated_at {
                dependency.observed_updated_at = observed_updated_at;
            }
            if !dependency.role.split(',').any(|item| item == role) {
                dependency.role.push(',');
                dependency.role.push_str(role);
            }
        })
        .or_insert_with(|| KnowledgeDependency {
            record_id: record_id.to_string(),
            record_type: record_type.to_string(),
            role: role.to_string(),
            observed_updated_at,
        });
}

fn direct_statement(id: &str, text: String, citations: Vec<String>) -> KnowledgeStatement {
    KnowledgeStatement {
        id: id.to_string(),
        text,
        derivation: StatementDerivation::Direct,
        verification: StatementVerification::DeterministicRecordRendering,
        citations: dedupe_strings(citations),
    }
}

fn operational_statement(id: &str, text: String) -> KnowledgeStatement {
    KnowledgeStatement {
        id: id.to_string(),
        text,
        derivation: StatementDerivation::Synthesized,
        verification: StatementVerification::DeterministicRecordRendering,
        citations: Vec::new(),
    }
}

fn supported_block(
    id: &str,
    kind: ViewBlockKind,
    heading: &str,
    statements: Vec<KnowledgeStatement>,
) -> KnowledgeViewBlock {
    let citations = dedupe_strings(
        statements
            .iter()
            .flat_map(|statement| statement.citations.iter().cloned())
            .collect(),
    );
    let markdown = render_statements(&statements);
    KnowledgeViewBlock {
        id: id.to_string(),
        kind,
        heading: heading.to_string(),
        markdown,
        statements,
        citations,
    }
}

fn render_statements(statements: &[KnowledgeStatement]) -> String {
    statements
        .iter()
        .map(|statement| statement.text.as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn validate_projection(
    blocks: &[KnowledgeViewBlock],
    citations: &[KnowledgeCitation],
    dependencies: &[KnowledgeDependency],
    unsupported_claims: u32,
) -> ValidationReceipt {
    let citation_ids = citations
        .iter()
        .map(|citation| citation.id.as_str())
        .collect::<BTreeSet<_>>();
    let dependency_ids = dependencies
        .iter()
        .map(|dependency| dependency.record_id.as_str())
        .collect::<BTreeSet<_>>();
    let factual_blocks = blocks
        .iter()
        .filter(|block| block.kind != ViewBlockKind::EvidenceStatus)
        .collect::<Vec<_>>();
    let factual_blocks_cited = factual_blocks.iter().all(|block| {
        !block.citations.is_empty()
            && block
                .citations
                .iter()
                .all(|citation| citation_ids.contains(citation.as_str()))
    });
    let statements_supported = blocks.iter().all(|block| {
        let block_citations = block
            .citations
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let statement_citations = block
            .statements
            .iter()
            .flat_map(|statement| statement.citations.iter().map(String::as_str))
            .collect::<BTreeSet<_>>();
        let citations_required = block.kind != ViewBlockKind::EvidenceStatus;
        !block.statements.is_empty()
            && block_citations == statement_citations
            && block.statements.iter().all(|statement| {
                !statement.text.trim().is_empty()
                    && (!citations_required || !statement.citations.is_empty())
                    && statement
                        .citations
                        .iter()
                        .all(|citation| citation_ids.contains(citation.as_str()))
                    && (statement.derivation != StatementDerivation::VerbatimQuote
                        || statement.citations.iter().any(|id| {
                            citations.iter().any(|citation| {
                                citation.id == *id
                                    && citation.kind == CitationKind::EvidenceSpan
                                    && citation.evidence_span.is_some()
                            })
                        }))
            })
    });
    let prose_is_structured = blocks.iter().all(|block| {
        !block.statements.is_empty()
            && block
                .statements
                .iter()
                .all(|statement| !statement.text.trim().is_empty())
            && block.markdown == render_statements(&block.statements)
    });
    let citations_resolve = citations
        .iter()
        .all(|citation| dependency_ids.contains(citation.record_id.as_str()));
    let mut warnings = Vec::new();
    if !factual_blocks_cited {
        warnings.push("One or more factual blocks have missing citations".to_string());
    }
    if !citations_resolve {
        warnings.push("One or more citations do not resolve to a dependency".to_string());
    }
    if !statements_supported {
        warnings.push(
            "One or more prose statements are empty, unsupported, or citation-inconsistent"
                .to_string(),
        );
    }
    if !prose_is_structured {
        warnings.push("Factual Markdown contains text outside the statement ledger".to_string());
    }
    if unsupported_claims > 0 {
        warnings.push(format!(
            "{unsupported_claims} claim record(s) have no source evidence span"
        ));
    }
    ValidationReceipt {
        citations_resolve,
        factual_blocks_cited,
        statements_supported,
        prose_is_structured,
        factual_block_count: u32::try_from(factual_blocks.len()).unwrap_or(u32::MAX),
        citation_count: u32::try_from(citations.len()).unwrap_or(u32::MAX),
        statement_count: u32::try_from(
            blocks
                .iter()
                .map(|block| block.statements.len())
                .sum::<usize>(),
        )
        .unwrap_or(u32::MAX),
        warnings,
    }
}

fn overview_markdown(entity: &Entity, citation: &str) -> String {
    let mut overview = format!(
        "**{}** has canonical type **{}**.",
        escape_markdown(&entity.display_name),
        escape_markdown(entity.entity_type.as_str()),
    );
    if let Some(summary) = entity
        .summary
        .as_deref()
        .filter(|summary| !summary.trim().is_empty())
    {
        overview.push(' ');
        overview.push_str(&escape_markdown(summary));
    }
    overview.push_str(&citation_marks(&[citation.to_string()]));
    overview
}

fn literal_text(value: &elephant_types::records::attribute::Literal) -> String {
    use elephant_types::records::attribute::Literal;
    match value {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => value.clone(),
        Literal::Integer(value) => value.to_string(),
        Literal::Float(value) => value.to_string(),
        Literal::Boolean(value) => value.to_string(),
        Literal::Json(value) => value.to_string(),
    }
}

fn render_page(
    title: &str,
    blocks: &[KnowledgeViewBlock],
    citations: &[KnowledgeCitation],
) -> String {
    let mut markdown = format!("# {}\n", escape_markdown(title));
    for block in blocks {
        markdown.push_str("\n## ");
        markdown.push_str(&escape_markdown(&block.heading));
        markdown.push_str("\n\n");
        markdown.push_str(&block.markdown);
        markdown.push('\n');
    }
    if !citations.is_empty() {
        markdown.push_str("\n## Evidence references\n\n");
        for citation in citations {
            let _ = write!(
                markdown,
                "[^{}]: {} ({:?})",
                citation.id,
                escape_markdown(&citation.record_id),
                citation.kind,
            );
            if let Some(assertion_id) = citation.assertion_id.as_deref() {
                let _ = write!(markdown, ", assertion {}", escape_markdown(assertion_id));
            }
            if let Some(source_id) = citation.source_id.as_deref() {
                let _ = write!(markdown, ", source {}", escape_markdown(source_id));
            }
            if let Some(span) = citation.evidence_span.as_ref() {
                let _ = write!(
                    markdown,
                    ", revision {}, node {}, bytes {}-{}, quote hash {}",
                    span.revision.id(),
                    span.node
                        .as_ref()
                        .map_or_else(|| "none".to_string(), |node| node.id().to_string()),
                    span.start,
                    span.end,
                    escape_markdown(&span.quote_hash),
                );
            }
            markdown.push('\n');
        }
    }
    markdown
}

fn citation_marks(ids: &[String]) -> String {
    let mut marks = String::new();
    for id in ids {
        let _ = write!(marks, " [^{id}]");
    }
    marks
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    values
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

/// Resolve published semantic source content from an exact live source read.
///
/// Callers retain both envelopes, and persist only the returned source's
/// immutable timestamp as the semantic dependency. Their original epoch and
/// publication authority remain independent guards.
pub async fn published_source_for_projection(
    storage: &StorageClient,
    reader: &elephant_storage::read_eligibility::ReadEligibility,
    original_raw: &Source,
    authorize: &elephant_storage::read_eligibility::ReadAuthorization<'_>,
) -> StorageResult<Source> {
    let current = Box::pin(
        elephant_storage::SourceRepository::new(storage.clone())
            .get(&original_raw.base.space_id, &original_raw.base.id),
    )
    .await?;
    if current.as_ref() != Some(original_raw) {
        return Err(StorageError::Conflict(
            "Projection raw source changed during preparation".into(),
        ));
    }
    Box::pin(reader.project(original_raw.clone(), authorize))
        .await?
        .ok_or_else(|| {
            StorageError::Conflict("Projection source is not currently published".into())
        })
}

fn extend_projection_sources(
    provenance: &mut elephant_types::records::base::Provenance,
    envelopes: &[BaseFields],
) {
    for base in envelopes {
        provenance
            .sources
            .extend(base.provenance.sources.iter().cloned());
        if base.id.table() == TableName::Source {
            provenance.sources.push(base.id.clone());
        }
    }
    provenance.sources.sort();
    provenance.sources.dedup();
}

/// Resolve each cited immutable span through the shared current-read authority.
/// Parent envelopes contribute both subjects and exact dependency versions;
/// graph endpoints alone cannot establish the source's subject completeness.
/// Explicit input lineage also contributes when no span citation was selected.
#[allow(clippy::too_many_lines)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the reader and its captured lineage observations alive until citation dependencies and their hash are assembled."
)]
async fn ground_citation_evidence(
    storage: &StorageClient,
    space_id: &str,
    security: &Security,
    draft: &mut RevisionDraft,
    compiler_name: &str,
    compiler_version: u32,
    source_ids: &[RecordId],
) -> StorageResult<Vec<BaseFields>> {
    let reader = elephant_storage::read_eligibility::ReadEligibility::new(storage.clone());
    let mut envelopes = BTreeMap::new();
    let mut raw_envelopes = BTreeMap::new();
    let authorize = |base: &BaseFields| {
        // Subject requirements are inherited below. The fixed partition must
        // already cover both the current and published parent policies.
        let mut output = base.clone();
        output.security = security.clone();
        base.space_id == space_id && projection_envelope_covers(&output, base)
    };
    for citation in &draft.citations {
        let Some(span) = &citation.evidence_span else {
            continue;
        };
        let evidence = Box::pin(reader.evidence(space_id, span, &authorize))
            .await?
            .ok_or_else(|| {
                StorageError::Conflict(
                    "Projection citation has invalid or out-of-envelope evidence".into(),
                )
            })?;
        let published_source = if let Some(raw) = evidence.source.as_ref() {
            let published = Box::pin(published_source_for_projection(
                storage, &reader, raw, &authorize,
            ))
            .await?;
            if raw_envelopes
                .insert(raw.base.id.clone(), raw.base.clone())
                .is_some_and(|previous| previous != raw.base)
            {
                return Err(StorageError::Conflict(
                    "Projection raw source envelope changed during preparation".into(),
                ));
            }
            Some(published)
        } else {
            None
        };
        for base in std::iter::once(evidence.revision.base)
            .chain(std::iter::once(evidence.doc.base))
            .chain(published_source.map(|source| source.base))
            .chain(evidence.node.map(|node| node.base))
        {
            if envelopes
                .insert(base.id.to_string(), base.clone())
                .is_some_and(|previous| previous != base)
            {
                return Err(StorageError::Conflict(
                    "Projection evidence envelope changed during preparation".into(),
                ));
            }
        }
    }
    for source_id in source_ids {
        if source_id.table() != TableName::Source {
            return Err(StorageError::Conflict(
                "Projection input lineage must name a Source".into(),
            ));
        }
        let raw = Box::pin(
            elephant_storage::SourceRepository::new(storage.clone()).get(space_id, source_id),
        )
        .await?
        .ok_or_else(|| StorageError::Conflict("Projection input Source is absent".into()))?;
        let published = Box::pin(published_source_for_projection(
            storage, &reader, &raw, &authorize,
        ))
        .await?;
        if raw_envelopes
            .insert(raw.base.id.clone(), raw.base.clone())
            .is_some_and(|previous| previous != raw.base)
        {
            return Err(StorageError::Conflict(
                "Projection raw source envelope changed during preparation".into(),
            ));
        }
        if envelopes
            .insert(published.base.id.to_string(), published.base.clone())
            .is_some_and(|previous| previous != published.base)
        {
            return Err(StorageError::Conflict(
                "Projection published source envelope changed during preparation".into(),
            ));
        }
    }
    Box::pin(reader.verify_current()).await?;
    let mut dependencies = draft
        .dependencies
        .iter()
        .cloned()
        .map(|dependency| (dependency.record_id.clone(), dependency))
        .collect::<BTreeMap<_, _>>();
    for base in envelopes.values() {
        if dependencies
            .get(&base.id.to_string())
            .and_then(|dependency| dependency.observed_updated_at)
            .is_some_and(|observed| observed != base.updated_at)
        {
            return Err(StorageError::Conflict(
                "Projection evidence dependency changed during preparation".into(),
            ));
        }
        add_dependency(
            &mut dependencies,
            &base.id,
            base.id.table().as_str(),
            "evidence_lineage",
            Some(base.updated_at),
        );
    }
    draft.dependencies = dependencies.into_values().collect();
    draft.dependency_hash = dependency_hash(
        &draft.dependencies,
        &security_fingerprint(security)?,
        compiler_name,
        compiler_version,
    )?;
    Ok(envelopes
        .into_values()
        .chain(raw_envelopes.into_values())
        .collect())
}

/// Combined authorization requirements of every record contributing to a view.
#[derive(Default)]
struct ProjectionSubjects {
    subjects: BTreeSet<RecordId>,
    incomplete: bool,
}

impl ProjectionSubjects {
    fn include(&mut self, record: &impl HasSecurity) {
        self.extend(record.subjects(), record.subjects_state());
    }

    fn include_base(&mut self, base: &BaseFields) {
        self.extend(base.subjects.clone(), base.subjects_state);
    }

    fn extend(&mut self, subjects: Vec<Link<Entity>>, state: SubjectsState) {
        // An empty subject scope cannot become authorized merely by being
        // combined with a different record that has populated subjects.
        self.incomplete |= state != SubjectsState::Complete || subjects.is_empty();
        self.subjects
            .extend(subjects.into_iter().map(|subject| subject.id().clone()));
    }

    fn finish(self) -> (Vec<Link<Entity>>, SubjectsState) {
        (
            self.subjects.into_iter().map(Link::new).collect(),
            if self.incomplete {
                SubjectsState::Unknown
            } else {
                SubjectsState::Complete
            },
        )
    }
}

/// Checks whether automatically planned navigation carries its full input scope.
#[must_use]
pub fn has_complete_definition_subject_contract(
    definition: &elephant_types::records::KnowledgeViewDefinition,
) -> bool {
    if definition.base.created_by != "elephant.wiki_planner" {
        return true;
    }
    definition
        .base
        .provenance
        .extractor
        .as_ref()
        .is_some_and(|extractor| {
            extractor.name.as_deref() == Some("elephant.wiki_planner")
                && extractor
                    .version
                    .as_deref()
                    .and_then(|version| version.parse::<u32>().ok())
                    .is_some_and(|version| version >= 10)
        })
}

/// Whether a persisted projection carries the complete subject-scope contract.
/// Older immutable revisions must be recompiled before subject-restricted reads.
#[must_use]
pub fn has_complete_subject_contract(base: &BaseFields) -> bool {
    let Some(extractor) = base.provenance.extractor.as_ref() else {
        return false;
    };
    let minimum = match extractor.name.as_deref() {
        Some(ENTITY_COMPILER_NAME) => ENTITY_COMPILER_VERSION,
        Some(source::SOURCE_COMPILER_NAME) => source::SOURCE_COMPILER_VERSION,
        Some(graph_views::GRAPH_COMPILER_NAME) => graph_views::GRAPH_COMPILER_VERSION,
        Some("elephant.llm_article") => 15,
        _ => return false,
    };
    extractor
        .version
        .as_deref()
        .and_then(|version| version.parse::<u32>().ok())
        .is_some_and(|version| version >= minimum)
}

fn projection_provenance(
    citations: &[KnowledgeCitation],
    dependency_hash: &str,
    compiler_name: &str,
    compiler_version: u32,
) -> Provenance {
    let sources = citations
        .iter()
        .filter_map(|citation| citation.source_id.as_deref())
        .filter_map(|source| source.parse::<RecordId>().ok())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Provenance {
        sources,
        extractor: Some(Extractor {
            name: Some(compiler_name.to_string()),
            version: Some(compiler_version.to_string()),
            model: None,
        }),
        config_id: Some(dependency_hash.to_string()),
        ..Provenance::default()
    }
}

fn dependency_hash(
    dependencies: &[KnowledgeDependency],
    security_fingerprint: &str,
    compiler_name: &str,
    compiler_version: u32,
) -> StorageResult<String> {
    let bytes = serde_json::to_vec(&(
        compiler_name,
        compiler_version,
        security_fingerprint,
        dependencies,
    ))
    .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

/// Computes a stable fingerprint for a materialization security envelope.
pub fn security_fingerprint(security: &Security) -> StorageResult<String> {
    let mut normalized = security.clone();
    normalized.labels.sort();
    normalized.labels.dedup();
    normalized.handling.sort();
    normalized.handling.dedup();
    let bytes = serde_json::to_vec(&normalized)
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

/// Builds the deterministic identity key for an entity view.
#[must_use]
pub fn entity_view_key(entity_id: &RecordId, security_fingerprint: &str) -> String {
    format!("entity:{entity_id}:security:{security_fingerprint}")
}

fn deterministic_view_id(key: &str) -> RecordId {
    RecordId::new(
        TableName::KnowledgeView,
        Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()),
    )
}

fn entity_slug(entity: &Entity) -> String {
    stable_slug(&entity.display_name, &entity.base.id, "entity")
}

fn stable_slug(label: &str, id: &RecordId, fallback: &str) -> String {
    let mut slug = String::new();
    let mut last_was_separator = false;
    for character in label.chars().flat_map(char::to_lowercase) {
        if character.is_ascii_alphanumeric() {
            slug.push(character);
            last_was_separator = false;
        } else if !last_was_separator && !slug.is_empty() {
            slug.push('-');
            last_was_separator = true;
        }
    }
    while slug.ends_with('-') {
        let _ = slug.pop();
    }
    if slug.is_empty() {
        slug.push_str(fallback);
    }
    let suffix = id.id().simple().to_string();
    format!("{slug}-{}", &suffix[..8])
}

fn escape_markdown(value: &str) -> String {
    value
        .chars()
        .flat_map(|character| {
            if matches!(
                character,
                '\\' | '`' | '*' | '_' | '{' | '}' | '[' | ']' | '<' | '>' | '#' | '|'
            ) {
                vec!['\\', character]
            } else {
                vec![character]
            }
        })
        .collect()
}

const fn projection_state_name(state: ProjectionState) -> &'static str {
    match state {
        ProjectionState::Current => "current",
        ProjectionState::Dirty => "dirty",
        ProjectionState::Rebuilding => "rebuilding",
        ProjectionState::Degraded => "degraded",
        ProjectionState::Failed => "failed",
    }
}

fn projection_state_reason(freshness: &ProjectionFreshness) -> Option<String> {
    if freshness.state == ProjectionState::Current {
        return None;
    }
    Some(format!(
        "unsupported_claims={}, expired_claims={}, stale_truth_slots={}, unresolved_conflicts={}, selection_truncated={}",
        freshness.unsupported_claims,
        freshness.expired_claims,
        freshness.stale_truth_slots,
        freshness.unresolved_conflicts,
        freshness.selection_truncated,
    ))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::{ClaimStatus, EntityStatus, EntityType};

    #[test]
    fn entity_compilation_phases_do_not_retain_selection_state() {
        fn bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        let join = bytes(
            |(compiler, entity): (&'static EntityViewCompiler, &'static RecordId)| {
                compiler.compile("space", entity, EntityViewLimits::default(), true)
            },
        );
        let material = bytes(
            |(compiler, entity): (&'static EntityViewCompiler, &'static RecordId)| {
                compiler.prepare_material("space", entity, EntityViewLimits::default())
            },
        );
        let revision = bytes(
            |(compiler, entity, inputs): (
                &'static EntityViewCompiler,
                &'static RecordId,
                Box<EntityViewInputs>,
            )| {
                compiler.prepare_revision("space", entity, true, "original-epoch", inputs)
            },
        );
        // Stored-state controls, not a compiled-frame or runtime-stack proof.
        // The original mixed-source erasure scenario keeps its full assertions.
        assert!(join <= 16 * 1024, "entity phase join is {join} bytes");
        assert!(material <= 32 * 1024, "entity material is {material} bytes");
        assert!(revision <= 32 * 1024, "entity revision is {revision} bytes");
    }

    #[test]
    fn citation_grounding_child_future_types_are_bounded() {
        fn bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        // Inspect the actual private futures without constructing a backend,
        // evidence row or runtime. Original evidence regressions remain the
        // behavioral oracle; object size does not bound nested poll frames.
        let phases = [
            (
                "citation grounding",
                bytes(
                    |(storage, security, draft): (
                        &'static StorageClient,
                        &'static Security,
                        &'static mut RevisionDraft,
                    )| {
                        ground_citation_evidence(
                            storage,
                            "space",
                            security,
                            draft,
                            "fixture",
                            1,
                            &[],
                        )
                    },
                ),
            ),
            (
                "published source",
                bytes(
                    |(storage, reader, raw, authorize): (
                        &'static StorageClient,
                        &'static elephant_storage::read_eligibility::ReadEligibility,
                        &'static Source,
                        &'static elephant_storage::read_eligibility::ReadAuthorization<'static>,
                    )| {
                        published_source_for_projection(storage, reader, raw, authorize)
                    },
                ),
            ),
        ];
        for (phase, size) in phases {
            assert!(size <= 32 * 1024, "{phase} future is {size} bytes");
        }
    }

    #[expect(
        clippy::redundant_pub_crate,
        reason = "Keep the explicit crate-only boundary of the fixture shared with sibling test modules."
    )]
    pub(crate) async fn register_space(storage: &StorageClient) {
        let mut value = serde_json::to_value(BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "space".into(),
            "fixture".into(),
        ))
        .expect("space fields");
        value["embedding_provider"] = serde_json::json!("not-dispatched");
        value["embedding_model"] = serde_json::json!("fixture");
        value["embedding_dims"] = serde_json::json!(8);
        value["processing"] = serde_json::Value::Null;
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(
                serde_json::from_value::<elephant_types::records::SpaceConfig>(value)
                    .expect("space config"),
            )
            .await
            .expect("register fixture space");
    }

    fn entity(name: &str) -> Entity {
        Entity {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".to_string(),
                "test".to_string(),
            ),
            entity_type: EntityType::new("person"),
            display_name: name.to_string(),
            aliases: Vec::new(),
            summary: None,
            handles: Vec::new(),
            identity_keys: Vec::new(),
            risk_level: None,
            trust_level: None,
            known_safe: false,
            first_seen: None,
            last_seen: None,
            status: EntityStatus::Active,
            redirect_to: None,
            metadata: serde_json::json!({}),
        }
    }

    #[test]
    fn projection_subject_union_preserves_incomplete_and_empty_contributors() {
        let root = entity("root");
        let other = entity("other");
        let mut scope = ProjectionSubjects::default();
        scope.include(&root);
        let mut evidence = other.base.clone();
        evidence.subjects = vec![Link::new(other.base.id)];
        evidence.subjects_state = SubjectsState::Partial;
        scope.include_base(&evidence);
        let (subjects, state) = scope.finish();
        assert_eq!(subjects.len(), 2);
        assert_eq!(state, SubjectsState::Unknown);
        let mut scope = ProjectionSubjects::default();
        scope.include(&root);
        evidence.subjects.clear();
        evidence.subjects_state = SubjectsState::Complete;
        scope.include_base(&evidence);
        assert_eq!(scope.finish().1, SubjectsState::Unknown);
    }

    #[test]
    fn projection_envelope_requires_full_policy_subject_and_handling_lineage() {
        let mut input = entity("evidence").base;
        input.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
        input.subjects_state = SubjectsState::Partial;
        input.security.policy_id = "family-policy".into();
        input.security.level = 2;
        input.security.labels = vec!["family".into(), "health".into()];
        input.security.handling = vec!["restricted".into()];
        let mut output = input.clone();
        output.id = RecordId::new_v4(TableName::KnowledgeViewRevision);
        output.subjects_state = SubjectsState::Unknown;
        output.security.level = 3;
        output.security.labels.reverse();
        assert!(projection_envelope_covers(&output, &input));
        for field in [
            "policy",
            "level",
            "label",
            "handling",
            "subject",
            "completeness",
            "grant",
        ] {
            let mut invalid = output.clone();
            match field {
                "policy" => invalid.security.policy_id = "default".into(),
                "level" => invalid.security.level = 1,
                "label" => invalid.security.labels.clear(),
                "handling" => invalid.security.handling.clear(),
                "subject" => invalid.subjects.clear(),
                "completeness" => invalid.subjects_state = SubjectsState::Complete,
                "grant" => invalid
                    .security
                    .handling
                    .push("handling:subjectless_ok".into()),
                _ => unreachable!(),
            }
            assert!(
                !projection_envelope_covers(&invalid, &input),
                "widened {field}"
            );
        }
        input.subjects_state = SubjectsState::Unknown;
        output.subjects_state = SubjectsState::Partial;
        assert!(!projection_envelope_covers(&output, &input));
    }

    #[test]
    fn old_projection_subject_contract_is_rejected() {
        for (name, version) in [
            (ENTITY_COMPILER_NAME, ENTITY_COMPILER_VERSION),
            (
                graph_views::GRAPH_COMPILER_NAME,
                graph_views::GRAPH_COMPILER_VERSION,
            ),
        ] {
            let mut base = entity("root").base;
            base.provenance.extractor = Some(Extractor {
                name: Some(name.to_string()),
                version: Some((version - 1).to_string()),
                model: None,
            });
            assert!(!has_complete_subject_contract(&base));
            base.provenance
                .extractor
                .as_mut()
                .expect("extractor")
                .version = Some(version.to_string());
            assert!(has_complete_subject_contract(&base));
        }
    }

    #[test]
    fn security_fingerprint_normalizes_set_fields() {
        let left = Security {
            policy_id: "default".to_string(),
            level: 2,
            labels: vec!["b".to_string(), "a".to_string()],
            handling: vec!["x".to_string()],
        };
        let right = Security {
            labels: vec!["a".to_string(), "b".to_string()],
            ..left.clone()
        };
        assert_eq!(
            security_fingerprint(&left).unwrap(),
            security_fingerprint(&right).unwrap()
        );
    }

    #[test]
    fn entity_slug_is_stable_and_safe() {
        let item = entity("Ada Lovelace / Notes");
        let slug = entity_slug(&item);
        assert!(slug.starts_with("ada-lovelace-notes-"));
        assert!(!slug.contains('/'));
    }

    #[test]
    fn markdown_escape_blocks_structural_injection() {
        assert_eq!(escape_markdown("# title [x]"), "\\# title \\[x\\]");
    }

    #[test]
    fn validation_rejects_factual_prose_outside_statement_ledger() {
        let record_id = RecordId::new_v4(TableName::Entity);
        let citation = KnowledgeCitation {
            id: "c1".to_string(),
            kind: CitationKind::GraphRecord,
            record_id: record_id.to_string(),
            assertion_id: None,
            source_id: None,
            evidence_span: None,
        };
        let mut block = supported_block(
            "overview",
            ViewBlockKind::Overview,
            "Overview",
            vec![direct_statement(
                "overview-1",
                "A supported fact. [^c1]".to_string(),
                vec!["c1".to_string()],
            )],
        );
        block
            .markdown
            .push_str("\nThis fluent sentence has no statement receipt.");
        let dependency = KnowledgeDependency {
            record_id: record_id.to_string(),
            record_type: "entity".to_string(),
            role: "root".to_string(),
            observed_updated_at: None,
        };

        let receipt = validate_projection(&[block], &[citation], &[dependency], 0);

        assert!(!receipt.prose_is_structured);
        assert!(receipt.statements_supported);
    }

    #[test]
    fn validation_rejects_quote_without_immutable_evidence_span() {
        let record_id = RecordId::new_v4(TableName::Entity);
        let citation = KnowledgeCitation {
            id: "c1".to_string(),
            kind: CitationKind::GraphRecord,
            record_id: record_id.to_string(),
            assertion_id: None,
            source_id: None,
            evidence_span: None,
        };
        let block = supported_block(
            "overview",
            ViewBlockKind::Overview,
            "Overview",
            vec![KnowledgeStatement {
                id: "quote-1".to_string(),
                text: "A supposed verbatim quote. [^c1]".to_string(),
                derivation: StatementDerivation::VerbatimQuote,
                verification: StatementVerification::EvidenceEntailment,
                citations: vec!["c1".to_string()],
            }],
        );
        let dependency = KnowledgeDependency {
            record_id: record_id.to_string(),
            record_type: "entity".to_string(),
            role: "root".to_string(),
            observed_updated_at: None,
        };

        let receipt = validate_projection(&[block], &[citation], &[dependency], 0);

        assert!(!receipt.statements_supported);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep original entity/relation/compiler ownership through publication, reuse and revision checks."
    )]
    async fn entity_view_publishes_reuses_and_revises() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        register_space(&storage).await;
        let entities = EntityRepository::new(storage.clone());
        let relations = RelRepository::new(storage.clone());

        let ada = entity("Ada Lovelace");
        let ada_id = ada.base.id.clone();
        let engine = entity("Analytical Engine");
        let engine_id = engine.base.id.clone();
        entities.create(ada).await.expect("create Ada");
        entities.create(engine).await.expect("create engine");

        let relation_id = RecordId::new_v4(TableName::Rel);
        let relation = Rel {
            base: BaseFields::new(relation_id.clone(), "space".to_string(), "test".to_string()),
            subject: Link::new(ada_id.clone()),
            object: Link::new(engine_id),
            rel_type: "worked_on".to_string(),
            status: ClaimStatus::Approved,
            confidence: 0.9,
            validity_window: None,
            evidence: Vec::new(),
            temporal_audit: None,
        };
        relations.create(relation).await.expect("create relation");

        let compiler = EntityViewCompiler::new(storage);
        let first = compiler
            .compile("space", &ada_id, EntityViewLimits::default(), false)
            .await
            .expect("compile first revision");
        assert!(first.changed);
        assert_eq!(first.revision.revision, 1);
        assert_eq!(first.view.state, ProjectionState::Degraded);
        assert!(first.revision.rendered_markdown.contains("worked\\_on"));
        assert!(first.revision.validation.citations_resolve);
        assert!(first.revision.validation.statements_supported);
        assert!(first.revision.validation.prose_is_structured);
        let first_indexes = compiler
            .repository()
            .list_dependency_indexes("space", &first.view.base.id)
            .await
            .expect("list dependency indexes");
        assert_eq!(first_indexes.len(), first.revision.dependencies.len());
        assert!(first_indexes
            .iter()
            .any(|index| index.dependency_record == relation_id.to_string()));

        let reused = compiler
            .compile("space", &ada_id, EntityViewLimits::default(), false)
            .await
            .expect("reuse current revision");
        assert!(!reused.changed);
        assert_eq!(reused.revision.base.id, first.revision.base.id);

        let dirty_count = compiler
            .repository()
            .mark_dirty_for_dependencies(
                "space",
                &[relation_id.to_string()],
                "relationship changed",
            )
            .await
            .expect("mark dependent view dirty");
        assert_eq!(dirty_count, 1);

        let rebuilt = compiler
            .compile("space", &ada_id, EntityViewLimits::default(), false)
            .await
            .expect("rebuild dirty revision");
        assert!(rebuilt.changed);
        assert_eq!(rebuilt.revision.revision, 2);

        let forced = compiler
            .compile("space", &ada_id, EntityViewLimits::default(), true)
            .await
            .expect("force new revision");
        assert!(forced.changed);
        assert_eq!(forced.revision.revision, 3);
        assert_eq!(
            forced.revision.supersedes,
            Some(rebuilt.revision.base.id.to_string())
        );
        let current_indexes = compiler
            .repository()
            .list_dependency_indexes("space", &forced.view.base.id)
            .await
            .expect("list current indexes");
        assert_eq!(current_indexes.len(), forced.revision.dependencies.len());
        assert!(current_indexes
            .iter()
            .all(|index| index.revision.id() == &forced.revision.base.id));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve original repository/compiler cleanup after all changed and unchanged selection assertions."
    )]
    async fn entity_view_recomputes_selection_before_reusing_a_clean_revision() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        register_space(&storage).await;
        let root = entity("Ada");
        let target = entity("Analytical Engine");
        let root_id = root.base.id.clone();
        let target_id = target.base.id.clone();
        let entities = EntityRepository::new(storage.clone());
        entities.create(root).await.expect("root");
        entities.create(target).await.expect("target");
        let compiler = EntityViewCompiler::new(storage.clone());
        let first = compiler
            .compile("space", &root_id, EntityViewLimits::default(), false)
            .await
            .expect("initial view");
        let new_relation_id = RecordId::new_v4(TableName::Rel);
        RelRepository::new(storage)
            .create(Rel {
                base: BaseFields::new(new_relation_id.clone(), "space".into(), "test".into()),
                subject: Link::new(root_id.clone()),
                object: Link::new(target_id),
                rel_type: "worked_on".into(),
                status: ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: Vec::new(),
                temporal_audit: None,
            })
            .await
            .expect("new intrinsic authorized relationship");
        // No explicit dirty notification: the old dependency index never named
        // this newly inserted row and cannot establish selection completeness.
        let next = compiler
            .compile("space", &root_id, EntityViewLimits::default(), false)
            .await
            .expect("updated selection");
        assert!(next.changed);
        assert_ne!(first.revision.base.id, next.revision.base.id);
        assert!(next
            .revision
            .dependencies
            .iter()
            .any(|dependency| dependency.record_id == new_relation_id.to_string()));
        let same = compiler
            .compile("space", &root_id, EntityViewLimits::default(), false)
            .await
            .expect("unchanged selection");
        assert!(!same.changed);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep migrated fixture/repository ownership through the original revision and index assertions."
    )]
    async fn entity_view_round_trips_through_migrated_surreal_store() {
        let storage = StorageClient::new(elephant_storage::StorageConfig {
            url: "mem://".to_string(),
            database: format!("knowledge_view_{}", Uuid::new_v4()),
            ..elephant_storage::StorageConfig::default()
        })
        .await
        .expect("surreal memory storage");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("schema");
        register_space(&storage).await;
        let ada = entity("Ada Lovelace");
        let ada_id = ada.base.id.clone();
        EntityRepository::new(storage.clone())
            .create(ada)
            .await
            .expect("entity");

        let compiled = EntityViewCompiler::new(storage.clone())
            .compile("space", &ada_id, EntityViewLimits::default(), false)
            .await
            .expect("compile");
        let repository = KnowledgeViewRepository::new(storage);
        let loaded = repository
            .get_current_revision(&compiled.view)
            .await
            .expect("load revision")
            .expect("current revision");

        assert_eq!(loaded.base.id, compiled.revision.base.id);
        assert!(loaded.validation.statements_supported);
        assert!(loaded.validation.prose_is_structured);
        let indexes = repository
            .list_dependency_indexes("space", &compiled.view.base.id)
            .await
            .expect("load dependency indexes");
        assert_eq!(indexes.len(), loaded.dependencies.len());
    }
    include!("projection_epoch_counterexamples.rs");
}
