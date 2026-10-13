//! Deterministic topic, community, and saved-query projections.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use chrono::Utc;
use elephant_storage::{
    AssertionRepository, AttributeRepository, EntityRepository, KnowledgeViewDefinitionRepository,
    KnowledgeViewRepository, PreparedKnowledgeViewAuthority, RelRepository, Repository,
    StorageClient, StorageError, StorageResult,
};
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::{
    Assertion, Attribute, CitationKind, ClaimStatus, CompilerReceipt, Entity, KnowledgeCitation,
    KnowledgeDependency, KnowledgeStatement, KnowledgeView, KnowledgeViewBlock,
    KnowledgeViewDefinition, KnowledgeViewPublicationStatus, KnowledgeViewRevision,
    KnowledgeViewSelector, KnowledgeViewSurface, ProjectionFreshness, ProjectionState, Rel,
    ViewBlockKind,
};
use elephant_types::{Link, RecordId, TableName};
use uuid::Uuid;

use super::{
    add_dependency, citation_marks, dependency_hash, direct_statement, escape_markdown,
    operational_statement, projection_provenance, projection_state_reason, push_evidence_citations,
    push_record_citation, render_page, security_fingerprint, stable_slug, supported_block,
    validate_projection, RevisionDraft,
};

/// Stable identity of the graph-view compiler.
pub const GRAPH_COMPILER_NAME: &str = "elephant.graph_view";
/// Contract version of the graph-view compiler.
pub const GRAPH_COMPILER_VERSION: u32 = 6;

/// Result of a graph-view compile-or-reuse operation.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledGraphView {
    /// Stable view definition.
    pub view: KnowledgeView,
    /// Current immutable revision.
    pub revision: KnowledgeViewRevision,
    /// Whether this call published a new revision.
    pub changed: bool,
}

/// Read-only graph context, never an implicit publication receipt.
#[derive(Debug, Clone)]
pub struct PreparedGraphView {
    /// Stable view representation.
    pub view: KnowledgeView,
    /// Exact prepared content and underlying dependencies.
    pub revision: KnowledgeViewRevision,
    /// Original epoch captured before any graph input was read.
    pub observed_epoch: String,
    /// True only when this exact revision already exists in storage.
    pub persisted: bool,
}

#[derive(Clone, Debug)]
struct SelectedMember {
    entity: Entity,
    via_relation: Option<Rel>,
    matching_attributes: Vec<Attribute>,
}

#[derive(Debug)]
struct Selection {
    members: Vec<SelectedMember>,
    truncated: bool,
}

/// Compiler for governed topic, community, and query definitions.
#[derive(Clone, Debug)]
pub struct GraphViewCompiler {
    storage: StorageClient,
    views: KnowledgeViewRepository,
    definitions: KnowledgeViewDefinitionRepository,
    entities: EntityRepository,
    relations: RelRepository,
    attributes: AttributeRepository,
    assertions: AssertionRepository,
    authority: Option<PreparedKnowledgeViewAuthority>,
}

impl GraphViewCompiler {
    /// Creates a direct-library graph-view compiler. Managed invocations must
    /// attach their original prepared authority before any compilation reads.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            storage: storage.clone(),
            views: KnowledgeViewRepository::new(storage.clone()),
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            entities: EntityRepository::new(storage.clone()),
            relations: RelRepository::new(storage.clone()),
            attributes: AttributeRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage),
            authority: None,
        }
    }

    /// Retain the original invocation proof for context reads, cache reuse, and
    /// publication. This does not capture or refresh an attempt or grant.
    #[must_use]
    pub fn with_managed_authority(mut self, authority: PreparedKnowledgeViewAuthority) -> Self {
        self.authority = Some(authority);
        self
    }

    async fn verify_authority(&self, space_id: &str) -> StorageResult<()> {
        if let Some(authority) = &self.authority {
            Box::pin(authority.verify_current_for_space(space_id)).await?;
        }
        Ok(())
    }

    /// Returns the stable-view repository.
    #[must_use]
    pub const fn repository(&self) -> &KnowledgeViewRepository {
        &self.views
    }

    /// Looks up the existing security-scoped projection for a definition.
    pub async fn find_existing(
        &self,
        space_id: &str,
        definition: &KnowledgeViewDefinition,
    ) -> StorageResult<Option<KnowledgeView>> {
        Box::pin(self.verify_authority(space_id)).await?;
        let fingerprint = security_fingerprint(&definition.base.security)?;
        let existing = Box::pin(
            self.views
                .find_by_key(space_id, &graph_view_key(&definition.base.id, &fingerprint)),
        )
        .await?;
        Box::pin(self.verify_authority(space_id)).await?;
        Ok(existing)
    }

    /// Prepare a current graph context without publishing intermediate rows.
    pub async fn prepare_context(
        &self,
        space_id: &str,
        definition_id: &RecordId,
        force: bool,
    ) -> StorageResult<PreparedGraphView> {
        Box::pin(self.verify_authority(space_id)).await?;
        let observed_epoch = Box::pin(
            elephant_storage::canonical_commit::CanonicalRepository::new(self.storage.clone())
                .epoch(space_id),
        )
        .await?;
        let compiled =
            Box::pin(self.prepare_at_epoch(space_id, definition_id, force, &observed_epoch))
                .await?;
        Box::pin(self.verify_authority(space_id)).await?;
        Ok(PreparedGraphView {
            view: compiled.view,
            revision: compiled.revision,
            observed_epoch,
            persisted: !compiled.changed,
        })
    }

    /// Compile and atomically publish a governed graph-view definition.
    pub async fn compile(
        &self,
        space_id: &str,
        definition_id: &RecordId,
        force: bool,
    ) -> StorageResult<CompiledGraphView> {
        let prepared = Box::pin(self.prepare_context(space_id, definition_id, force)).await?;
        if prepared.persisted {
            Box::pin(self.verify_authority(space_id)).await?;
            return Ok(CompiledGraphView {
                view: prepared.view,
                revision: prepared.revision,
                changed: false,
            });
        }
        let (view, revision) = if let Some(authority) = &self.authority {
            Box::pin(self.views.publish_at_epoch_with_authority(
                prepared.view,
                prepared.revision,
                &prepared.observed_epoch,
                authority,
            ))
            .await?
        } else {
            Box::pin(self.views.publish_at_epoch(
                prepared.view,
                prepared.revision,
                &prepared.observed_epoch,
            ))
            .await?
        };
        Ok(CompiledGraphView {
            view,
            revision,
            changed: true,
        })
    }

    #[allow(clippy::too_many_lines)]
    async fn prepare_at_epoch(
        &self,
        space_id: &str,
        definition_id: &RecordId,
        force: bool,
        observed_epoch: &str,
    ) -> StorageResult<CompiledGraphView> {
        let definition = Box::pin(self.definitions.get(space_id, definition_id))
            .await?
            .ok_or_else(|| StorageError::NotFound(definition_id.to_string()))?;
        if !super::has_complete_definition_subject_contract(&definition) {
            return Err(StorageError::DataIntegrityError(
                "Wiki definition requires replanning to establish its subject scope".to_string(),
            ));
        }
        let security = definition.base.security.clone();
        let fingerprint = security_fingerprint(&security)?;
        let key = graph_view_key(definition_id, &fingerprint);
        let existing = Box::pin(self.views.find_by_key(space_id, &key)).await?;
        // Recompute the bounded selection before cache reuse: an old dependency
        // list cannot prove that no new matching relation or document appeared.

        let selection = match &definition.selector {
            KnowledgeViewSelector::Topic {
                topic,
                relation_types,
                max_hops,
                max_members,
            } => {
                Box::pin(self.select_graph(
                    space_id,
                    std::slice::from_ref(topic),
                    relation_types,
                    *max_hops,
                    *max_members,
                    true,
                    &security,
                ))
                .await?
            }
            KnowledgeViewSelector::Community {
                seeds,
                relation_types,
                max_hops,
                max_members,
            } => {
                Box::pin(self.select_graph(
                    space_id,
                    seeds,
                    relation_types,
                    *max_hops,
                    *max_members,
                    false,
                    &security,
                ))
                .await?
            }
            KnowledgeViewSelector::Query {
                entity_types,
                name_contains,
                attributes,
                scan_limit,
                max_members,
            } => {
                Box::pin(self.select_query(
                    space_id,
                    entity_types,
                    name_contains.as_deref(),
                    attributes,
                    *scan_limit,
                    *max_members,
                    &security,
                ))
                .await?
            }
        };
        let assertions = Box::pin(self.load_assertions(space_id, &selection, &security)).await?;
        let mut draft = build_graph_draft(&definition, &selection, &assertions, &fingerprint)?;
        let source_ids = graph_input_sources(&definition, &selection, &assertions);
        let evidence_envelopes = Box::pin(super::ground_citation_evidence(
            &self.storage,
            space_id,
            &security,
            &mut draft,
            GRAPH_COMPILER_NAME,
            GRAPH_COMPILER_VERSION,
            &source_ids,
        ))
        .await?;

        if !force {
            if let Some(existing_view) = existing.as_ref() {
                if let Some(current) =
                    Box::pin(self.views.get_current_revision(existing_view)).await?
                {
                    if existing_view.state != ProjectionState::Dirty
                        && current.compiler.dependency_hash == draft.dependency_hash
                        && current.compiler.version == GRAPH_COMPILER_VERSION
                        && Box::pin(super::projection_is_current(&self.storage, &current)).await?
                    {
                        Box::pin(super::verify_projection_epoch(
                            &self.storage,
                            space_id,
                            observed_epoch,
                        ))
                        .await?;
                        return Ok(CompiledGraphView {
                            view: existing_view.clone(),
                            revision: current,
                            changed: false,
                        });
                    }
                }
            }
        }

        let compiled = assemble_graph_revision(
            space_id,
            definition_id,
            GraphRevisionAssembly {
                definition,
                selection,
                assertions,
                evidence_envelopes,
                existing,
                draft,
                security,
                fingerprint,
                key,
            },
        );
        if !Box::pin(super::projection_is_current(
            &self.storage,
            &compiled.revision,
        ))
        .await?
        {
            return Err(StorageError::Conflict(
                "Graph projection has invalid or withdrawn evidence".into(),
            ));
        }
        Box::pin(super::verify_projection_epoch(
            &self.storage,
            space_id,
            observed_epoch,
        ))
        .await?;
        Ok(compiled)
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_graph(
        &self,
        space_id: &str,
        seeds: &[Link<Entity>],
        relation_types: &[String],
        max_hops: u8,
        max_members: u16,
        _topic: bool,
        security: &elephant_types::records::base::Security,
    ) -> StorageResult<Selection> {
        let mut selected = BTreeMap::new();
        let mut frontier = VecDeque::new();
        let mut ordered_seeds = seeds
            .iter()
            .map(|seed| seed.id().clone())
            .collect::<Vec<_>>();
        ordered_seeds.sort();
        for seed_id in ordered_seeds {
            let seed = Box::pin(self.entities.get(space_id, &seed_id))
                .await?
                .filter(|entity| entity.base.security == *security)
                .ok_or_else(|| StorageError::NotFound(seed_id.to_string()))?;
            frontier.push_back((seed_id.clone(), 0_u8));
            selected.insert(
                seed_id,
                SelectedMember {
                    entity: seed,
                    via_relation: None,
                    matching_attributes: Vec::new(),
                },
            );
        }

        let allowed = relation_types
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let mut truncated = false;
        while let Some((current, distance)) = frontier.pop_front() {
            if distance >= max_hops {
                continue;
            }
            let mut edges = Box::pin(self.relations.find_outgoing(space_id, &current)).await?;
            edges.extend(Box::pin(self.relations.find_incoming(space_id, &current)).await?);
            edges.retain(|edge| {
                edge.base.security == *security
                    && edge.status == ClaimStatus::Approved
                    && (allowed.is_empty() || allowed.contains(edge.rel_type.as_str()))
            });
            edges.sort_by(|left, right| {
                left.rel_type
                    .cmp(&right.rel_type)
                    .then_with(|| left.base.id.cmp(&right.base.id))
            });
            for edge in edges {
                let neighbor_id = if edge.subject.id() == &current {
                    edge.object.id()
                } else {
                    edge.subject.id()
                };
                if selected.contains_key(neighbor_id) {
                    continue;
                }
                if selected.len() >= usize::from(max_members) {
                    truncated = true;
                    break;
                }
                let Some(entity) = Box::pin(self.entities.get(space_id, neighbor_id)).await? else {
                    continue;
                };
                if entity.base.security != *security {
                    continue;
                }
                let neighbor_id = neighbor_id.clone();
                selected.insert(
                    neighbor_id.clone(),
                    SelectedMember {
                        entity,
                        via_relation: Some(edge),
                        matching_attributes: Vec::new(),
                    },
                );
                frontier.push_back((neighbor_id, distance.saturating_add(1)));
            }
            if truncated {
                break;
            }
        }
        Ok(Selection {
            members: selected.into_values().collect(),
            truncated,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn select_query(
        &self,
        space_id: &str,
        entity_types: &[String],
        name_contains: Option<&str>,
        filters: &[elephant_types::records::KnowledgeQueryAttributeFilter],
        scan_limit: u16,
        max_members: u16,
        security: &elephant_types::records::base::Security,
    ) -> StorageResult<Selection> {
        let allowed_types = entity_types
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        let name_contains = name_contains.map(str::to_lowercase);
        let mut cursor = None;
        let mut scanned = 0_usize;
        let mut matching = Vec::new();
        while scanned < usize::from(scan_limit) {
            let page_limit = (usize::from(scan_limit) - scanned).min(250);
            let page = Box::pin(self.entities.list(space_id, page_limit, cursor.as_ref())).await?;
            if page.is_empty() {
                break;
            }
            scanned = scanned.saturating_add(page.len());
            cursor = page.last().map(|entity| entity.base.id.clone());
            let last_page = page.len() < page_limit;
            for entity in page {
                if entity.base.security != *security
                    || (!allowed_types.is_empty()
                        && !allowed_types.contains(entity.entity_type.as_str()))
                    || name_contains.as_ref().is_some_and(|needle| {
                        !entity.display_name.to_lowercase().contains(needle)
                            && !entity
                                .aliases
                                .iter()
                                .any(|alias| alias.to_lowercase().contains(needle))
                    })
                {
                    continue;
                }
                let mut matching_attributes = Vec::new();
                if !filters.is_empty() {
                    let attributes = Box::pin(
                        self.attributes
                            .find_by_subject_entity(space_id, &entity.base.id),
                    )
                    .await?;
                    for filter in filters {
                        let Some(attribute) = attributes.iter().find(|attribute| {
                            attribute.base.security == *security
                                && attribute.status == ClaimStatus::Approved
                                && attribute.attr_type == filter.attr_type
                                && attribute.value == filter.value
                        }) else {
                            matching_attributes.clear();
                            break;
                        };
                        matching_attributes.push(attribute.clone());
                    }
                    if matching_attributes.len() != filters.len() {
                        continue;
                    }
                }
                matching.push(SelectedMember {
                    entity,
                    via_relation: None,
                    matching_attributes,
                });
            }
            if last_page {
                break;
            }
        }
        matching.sort_by(|left, right| {
            left.entity
                .display_name
                .cmp(&right.entity.display_name)
                .then_with(|| left.entity.base.id.cmp(&right.entity.base.id))
        });
        let truncated =
            scanned >= usize::from(scan_limit) || matching.len() > usize::from(max_members);
        matching.truncate(usize::from(max_members));
        Ok(Selection {
            members: matching,
            truncated,
        })
    }

    async fn load_assertions(
        &self,
        space_id: &str,
        selection: &Selection,
        security: &elephant_types::records::base::Security,
    ) -> StorageResult<HashMap<RecordId, Assertion>> {
        let ids = selection
            .members
            .iter()
            .flat_map(|member| {
                member
                    .via_relation
                    .iter()
                    .flat_map(|relation| relation.evidence.iter())
                    .chain(
                        member
                            .matching_attributes
                            .iter()
                            .flat_map(|attribute| attribute.evidence.iter()),
                    )
                    .map(|evidence| evidence.assertion.id().clone())
            })
            .collect::<BTreeSet<_>>();
        let mut assertions = HashMap::new();
        for id in ids {
            if let Some(assertion) = Box::pin(self.assertions.get(space_id, &id)).await? {
                if assertion.base.security == *security {
                    assertions.insert(id, assertion);
                }
            }
        }
        Ok(assertions)
    }
}

// Consume the completed selection before polling final current-evidence and
// epoch checks. Pure revision assembly has no nested storage future or write.
struct GraphRevisionAssembly {
    definition: KnowledgeViewDefinition,
    selection: Selection,
    assertions: HashMap<RecordId, Assertion>,
    evidence_envelopes: Vec<BaseFields>,
    existing: Option<KnowledgeView>,
    draft: RevisionDraft,
    security: Security,
    fingerprint: String,
    key: String,
}

#[inline(never)]
#[allow(clippy::too_many_lines)]
fn assemble_graph_revision(
    space_id: &str,
    definition_id: &RecordId,
    assembly: GraphRevisionAssembly,
) -> CompiledGraphView {
    let GraphRevisionAssembly {
        definition,
        selection,
        assertions,
        evidence_envelopes,
        existing,
        draft,
        security,
        fingerprint,
        key,
    } = assembly;
    let now = Utc::now();
    let view_id = existing.as_ref().map_or_else(
        || {
            RecordId::new(
                TableName::KnowledgeView,
                Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()),
            )
        },
        |view| view.base.id.clone(),
    );
    let revision_number = existing
        .as_ref()
        .map_or(1, |view| view.revision_count.saturating_add(1));
    let revision_id = RecordId::new_v7(TableName::KnowledgeViewRevision);
    let mut scope = super::ProjectionSubjects::default();
    scope.include(&definition);
    for member in &selection.members {
        scope.include(&member.entity);
        if let Some(relation) = &member.via_relation {
            scope.include(relation);
        }
        for attribute in &member.matching_attributes {
            scope.include(attribute);
        }
    }
    for assertion in assertions.values() {
        scope.include(assertion);
    }
    for base in &evidence_envelopes {
        scope.include_base(base);
    }
    let (subjects, mut subjects_state) = scope.finish();
    if selection.truncated && subjects_state == SubjectsState::Complete {
        subjects_state = SubjectsState::Partial;
    }
    let mut provenance = projection_provenance(
        &draft.citations,
        &draft.dependency_hash,
        GRAPH_COMPILER_NAME,
        GRAPH_COMPILER_VERSION,
    );
    super::extend_projection_sources(&mut provenance, &evidence_envelopes);
    provenance
        .sources
        .extend(definition.base.provenance.sources.iter().cloned());
    provenance.sources.sort();
    provenance.sources.dedup();
    let mut view_base = existing.as_ref().map_or_else(
        || {
            let mut base = BaseFields::new(
                view_id.clone(),
                space_id.to_string(),
                GRAPH_COMPILER_NAME.to_string(),
            );
            base.security = security.clone();
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
    let kind = definition.selector.kind();
    let view = KnowledgeView {
        base: view_base,
        key,
        kind,
        surface: KnowledgeViewSurface::Inspector,
        publication_status: KnowledgeViewPublicationStatus::Draft,
        root_record: definition_id.to_string(),
        title: definition.title.clone(),
        slug: stable_slug(&definition.title, definition_id, "view"),
        security_fingerprint: fingerprint.clone(),
        current_revision: Some(revision_id.to_string()),
        revision_count: revision_number,
        state: draft.freshness.state,
        state_reason: projection_state_reason(&draft.freshness),
        published_at: Some(now),
    };
    let mut revision_base = BaseFields::new(
        revision_id,
        space_id.to_string(),
        GRAPH_COMPILER_NAME.to_string(),
    );
    revision_base.security = security;
    revision_base.subjects = subjects;
    revision_base.subjects_state = subjects_state;
    revision_base.provenance = provenance;
    let revision = KnowledgeViewRevision {
        base: revision_base,
        view_id: view.base.id.to_string(),
        revision: revision_number,
        kind,
        root_record: definition_id.to_string(),
        blocks: draft.blocks,
        rendered_markdown: draft.rendered_markdown,
        citations: draft.citations,
        dependencies: draft.dependencies,
        freshness: draft.freshness,
        validation: draft.validation,
        compiler: CompilerReceipt {
            compiler: GRAPH_COMPILER_NAME.to_string(),
            version: GRAPH_COMPILER_VERSION,
            dependency_hash: draft.dependency_hash,
            policy_fingerprint: fingerprint,
        },
        compiled_at: now,
        supersedes: existing.and_then(|view| view.current_revision),
    };
    CompiledGraphView {
        view,
        revision,
        changed: true,
    }
}

// A root or definition can contribute source lineage without a selected
// membership path or span citation. Resolve those original inputs through the
// same current-read authority as cited evidence; provenance alone is not an
// observed semantic dependency or a source authorization envelope.
fn graph_input_sources(
    definition: &KnowledgeViewDefinition,
    selection: &Selection,
    assertions: &HashMap<RecordId, Assertion>,
) -> Vec<RecordId> {
    std::iter::once(&definition.base)
        .chain(selection.members.iter().flat_map(|member| {
            std::iter::once(&member.entity.base)
                .chain(member.via_relation.iter().map(|relation| &relation.base))
                .chain(
                    member
                        .matching_attributes
                        .iter()
                        .map(|attribute| &attribute.base),
                )
        }))
        .chain(assertions.values().map(|assertion| &assertion.base))
        .flat_map(|base| base.provenance.sources.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn build_graph_draft(
    definition: &KnowledgeViewDefinition,
    selection: &Selection,
    assertions: &HashMap<RecordId, Assertion>,
    fingerprint: &str,
) -> StorageResult<RevisionDraft> {
    let mut dependencies = BTreeMap::new();
    let mut citations = Vec::new();
    add_dependency(
        &mut dependencies,
        &definition.base.id,
        "knowledge_view_definition",
        "root",
        Some(definition.base.updated_at),
    );
    add_trigger_dependencies(&mut dependencies, &definition.selector);
    let (mut blocks, definition_citation) =
        graph_intro_blocks(definition, selection, &mut citations);
    let (member_statements, unsupported_claims) = graph_member_statements(
        &definition.selector,
        selection,
        assertions,
        &definition_citation,
        &mut dependencies,
        &mut citations,
    );
    blocks.push(supported_block(
        "members",
        ViewBlockKind::Members,
        "Members",
        member_statements,
    ));

    let state = if unsupported_claims > 0 || selection.truncated {
        ProjectionState::Degraded
    } else {
        ProjectionState::Current
    };
    let freshness = ProjectionFreshness {
        state,
        minimum_claim_score: None,
        stale_truth_slots: 0,
        unresolved_conflicts: 0,
        unsupported_claims,
        expired_claims: 0,
        selection_truncated: selection.truncated,
        valid_until: None,
    };
    blocks.push(supported_block(
        "evidence-status",
        ViewBlockKind::EvidenceStatus,
        "Evidence status",
        vec![
            operational_statement(
                "selected-members",
                format!("- Selected members: **{}**", selection.members.len()),
            ),
            operational_statement(
                "selection-truncated",
                format!(
                    "- Selection truncated by an explicit bound: **{}**",
                    selection.truncated
                ),
            ),
            operational_statement(
                "unsupported-membership-claims",
                format!(
                    "- Membership-path claims without source evidence: **{unsupported_claims}**"
                ),
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
            "Graph knowledge-view validation failed: {}",
            validation.warnings.join("; ")
        )));
    }
    let dependency_hash = dependency_hash(
        &dependencies,
        fingerprint,
        GRAPH_COMPILER_NAME,
        GRAPH_COMPILER_VERSION,
    )?;
    let rendered_markdown = render_page(&definition.title, &blocks, &citations);
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

fn graph_intro_blocks(
    definition: &KnowledgeViewDefinition,
    selection: &Selection,
    citations: &mut Vec<KnowledgeCitation>,
) -> (Vec<KnowledgeViewBlock>, String) {
    let definition_citation =
        push_record_citation(citations, &definition.base.id, CitationKind::GraphRecord);
    let (intro, intro_citations) =
        context_intro(definition, selection, citations, &definition_citation);
    let overview = supported_block(
        "overview",
        ViewBlockKind::Overview,
        "Overview",
        vec![direct_statement(
            "overview-1",
            format!("{intro}{}", citation_marks(&intro_citations)),
            intro_citations,
        )],
    );
    let receipt = supported_block(
        "selection-receipt",
        ViewBlockKind::SelectionReceipt,
        "Selection receipt",
        vec![direct_statement(
            "selection-receipt-1",
            format!(
                "- {}{}",
                selector_description(&definition.selector),
                citation_marks(std::slice::from_ref(&definition_citation)),
            ),
            vec![definition_citation.clone()],
        )],
    );
    (vec![overview, receipt], definition_citation)
}

fn context_intro(
    definition: &KnowledgeViewDefinition,
    selection: &Selection,
    citations: &mut Vec<KnowledgeCitation>,
    definition_citation: &str,
) -> (String, Vec<String>) {
    if let KnowledgeViewSelector::Topic { topic, .. } = &definition.selector {
        if let Some(root) = selection
            .members
            .iter()
            .find(|member| member.entity.base.id == *topic.id())
        {
            let root_citation =
                push_record_citation(citations, &root.entity.base.id, CitationKind::GraphRecord);
            let text = root.entity.summary.as_ref().map_or_else(
                || {
                    format!(
                        "**{}** is {} at the center of this editorial context.",
                        escape_markdown(&root.entity.display_name),
                        humanize_entity_type(root.entity.entity_type.as_str()),
                    )
                },
                |summary| {
                    format!(
                        "**{}** - {}",
                        escape_markdown(&root.entity.display_name),
                        escape_markdown(summary)
                    )
                },
            );
            return (text, vec![root_citation]);
        }
    }

    let subject = match &definition.selector {
        KnowledgeViewSelector::Community { .. } => "connected subjects",
        KnowledgeViewSelector::Query { .. } => "subjects matching its maintained criteria",
        KnowledgeViewSelector::Topic { .. } => "knowledge connected to its central subject",
    };
    (
        format!(
            "**{}** brings together {} currently represented in Elephant.",
            escape_markdown(&definition.title),
            subject,
        ),
        vec![definition_citation.to_string()],
    )
}

fn graph_member_statements(
    selector: &KnowledgeViewSelector,
    selection: &Selection,
    assertions: &HashMap<RecordId, Assertion>,
    definition_citation: &str,
    dependencies: &mut BTreeMap<String, KnowledgeDependency>,
    citations: &mut Vec<KnowledgeCitation>,
) -> (Vec<KnowledgeStatement>, u32) {
    let mut unsupported_claims = 0_u32;
    let mut statements = selection
        .members
        .iter()
        .enumerate()
        .map(|(position, member)| {
            add_dependency(
                dependencies,
                &member.entity.base.id,
                "entity",
                "selected_member",
                Some(member.entity.base.updated_at),
            );
            let mut member_citations = vec![push_record_citation(
                citations,
                &member.entity.base.id,
                CitationKind::GraphRecord,
            )];
            if let Some(relation) = member.via_relation.as_ref() {
                add_dependency(
                    dependencies,
                    &relation.base.id,
                    "rel",
                    "membership_path",
                    Some(relation.base.updated_at),
                );
                member_citations.extend(push_evidence_citations(
                    citations,
                    dependencies,
                    &relation.base.id,
                    &relation.evidence,
                    assertions,
                ));
                unsupported_claims =
                    unsupported_claims.saturating_add(u32::from(relation.evidence.is_empty()));
            } else {
                member_citations.push(definition_citation.to_string());
            }
            for attribute in &member.matching_attributes {
                add_dependency(
                    dependencies,
                    &attribute.base.id,
                    "attribute",
                    "query_match",
                    Some(attribute.base.updated_at),
                );
                member_citations.extend(push_evidence_citations(
                    citations,
                    dependencies,
                    &attribute.base.id,
                    &attribute.evidence,
                    assertions,
                ));
                unsupported_claims =
                    unsupported_claims.saturating_add(u32::from(attribute.evidence.is_empty()));
            }
            member_citations.sort();
            member_citations.dedup();
            let text = member.via_relation.as_ref().map_or_else(
                || {
                    let inclusion = if matches!(selector, KnowledgeViewSelector::Query { .. }) {
                        "matches this context's maintained criteria"
                    } else {
                        "is central to this context"
                    };
                    format!(
                        "- **{}** is {} and {}.{}",
                        escape_markdown(&member.entity.display_name),
                        humanize_entity_type(member.entity.entity_type.as_str()),
                        inclusion,
                        citation_marks(&member_citations),
                    )
                },
                |relation| relationship_statement(relation, member, selection, &member_citations),
            );
            direct_statement(&format!("member-{}", position + 1), text, member_citations)
        })
        .collect::<Vec<_>>();
    if statements.is_empty() {
        statements.push(direct_statement(
            "member-none",
            format!(
                "- No canonical entities matched the governed selector.{}",
                citation_marks(&[definition_citation.to_string()]),
            ),
            vec![definition_citation.to_string()],
        ));
    }
    (statements, unsupported_claims)
}

fn relationship_statement(
    relation: &Rel,
    member: &SelectedMember,
    selection: &Selection,
    citations: &[String],
) -> String {
    let (subject_id, object_id) = (relation.subject.id(), relation.object.id());
    let subject = selection
        .members
        .iter()
        .find(|candidate| candidate.entity.base.id == *subject_id)
        .map_or("another known subject", |candidate| {
            candidate.entity.display_name.as_str()
        });
    let object = selection
        .members
        .iter()
        .find(|candidate| candidate.entity.base.id == *object_id)
        .map_or(member.entity.display_name.as_str(), |candidate| {
            candidate.entity.display_name.as_str()
        });
    let phrase = predicate_phrase(&relation.rel_type);
    format!(
        "- **{}** {} **{}**.{}",
        escape_markdown(subject),
        escape_markdown(&phrase),
        escape_markdown(object),
        citation_marks(citations),
    )
}

fn predicate_phrase(predicate: &str) -> String {
    match predicate {
        "parent_of" => "is a parent of".to_string(),
        "mother_of" => "is the mother of".to_string(),
        "father_of" => "is the father of".to_string(),
        "child_of" => "is a child of".to_string(),
        "sibling_of" => "is a sibling of".to_string(),
        "spouse_of" => "is married to".to_string(),
        "works_at" => "works at".to_string(),
        "lives_in" => "lives in".to_string(),
        "located_in" => "is located in".to_string(),
        "teacher_of" => "is a teacher of".to_string(),
        "attends" => "attends".to_string(),
        "owns" => "owns".to_string(),
        "uses" => "uses".to_string(),
        "likes" => "likes".to_string(),
        other => format!("is connected to through {}", other.replace('_', " ")),
    }
}

fn humanize_entity_type(entity_type: &str) -> String {
    let value = entity_type.replace('_', " ");
    let article = if value
        .chars()
        .next()
        .is_some_and(|first| matches!(first.to_ascii_lowercase(), 'a' | 'e' | 'i' | 'o' | 'u'))
    {
        "an"
    } else {
        "a"
    };
    format!("{article} {value}")
}

fn add_trigger_dependencies(
    dependencies: &mut BTreeMap<String, KnowledgeDependency>,
    selector: &KnowledgeViewSelector,
) {
    let tables: &[&str] = match selector {
        KnowledgeViewSelector::Topic { .. } | KnowledgeViewSelector::Community { .. } => &["rel"],
        KnowledgeViewSelector::Query { attributes, .. } if attributes.is_empty() => &["entity"],
        KnowledgeViewSelector::Query { .. } => &["entity", "attribute"],
    };
    for table in tables {
        let key = format!("trigger:table:{table}");
        dependencies.insert(
            key.clone(),
            KnowledgeDependency {
                record_id: key,
                record_type: "selector_trigger".to_string(),
                role: "potential_membership_change".to_string(),
                observed_updated_at: None,
            },
        );
    }
}

fn selector_description(selector: &KnowledgeViewSelector) -> String {
    match selector {
        KnowledgeViewSelector::Topic {
            topic,
            relation_types,
            max_hops,
            max_members,
        } => format!(
            "Topic root: {}; approved relationship types: {}; maximum hops: {}; maximum members: {}.",
            topic.id(),
            list_or_all(relation_types),
            max_hops,
            max_members
        ),
        KnowledgeViewSelector::Community {
            seeds,
            relation_types,
            max_hops,
            max_members,
        } => format!(
            "Community seeds: {}; approved relationship types: {}; maximum hops: {}; maximum members: {}.",
            seeds.iter().map(|seed| seed.id().to_string()).collect::<Vec<_>>().join(", "),
            list_or_all(relation_types),
            max_hops,
            max_members
        ),
        KnowledgeViewSelector::Query {
            entity_types,
            name_contains,
            attributes,
            scan_limit,
            max_members,
        } => format!(
            "Saved entity query: types {}; name filter {}; approved exact-attribute filters {}; scan limit {}; maximum members {}.",
            list_or_all(entity_types),
            name_contains.as_deref().unwrap_or("all names"),
            attributes.len(),
            scan_limit,
            max_members
        ),
    }
}

fn list_or_all(values: &[String]) -> String {
    if values.is_empty() {
        "all".to_string()
    } else {
        values.join(", ")
    }
}

fn graph_view_key(definition_id: &RecordId, fingerprint: &str) -> String {
    format!("definition:{definition_id}:security:{fingerprint}")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use elephant_types::records::attribute::Literal;
    use elephant_types::records::{
        EntityStatus, EntityType, KnowledgeQueryAttributeFilter, KnowledgeViewKind,
        KnowledgeViewPublicationStatus,
    };

    use super::*;

    #[test]
    fn graph_preparation_child_future_types_are_bounded() {
        fn bytes<A, F: std::future::Future>(_: impl FnOnce(A) -> F) -> usize {
            std::mem::size_of::<F>()
        }
        // Uncalled typed closures measure real compiler methods, including the
        // inner preparation and selection futures. They construct no storage or
        // runtime and make no claim about nested machine poll-frame sizes.
        let phases = [
            (
                "prepare context",
                bytes(
                    |(compiler, id): (&'static GraphViewCompiler, &'static RecordId)| {
                        compiler.prepare_context("space", id, false)
                    },
                ),
            ),
            (
                "prepare at epoch",
                bytes(
                    |(compiler, id): (&'static GraphViewCompiler, &'static RecordId)| {
                        compiler.prepare_at_epoch("space", id, false, "original-epoch")
                    },
                ),
            ),
            (
                "compile",
                bytes(
                    |(compiler, id): (&'static GraphViewCompiler, &'static RecordId)| {
                        compiler.compile("space", id, false)
                    },
                ),
            ),
            (
                "select graph",
                bytes(
                    |(compiler, security): (&'static GraphViewCompiler, &'static Security)| {
                        compiler.select_graph("space", &[], &[], 1, 32, true, security)
                    },
                ),
            ),
            (
                "select query",
                bytes(
                    |(compiler, security): (&'static GraphViewCompiler, &'static Security)| {
                        compiler.select_query("space", &[], None, &[], 32, 32, security)
                    },
                ),
            ),
            (
                "load assertions",
                bytes(
                    |(compiler, selection, security): (
                        &'static GraphViewCompiler,
                        &'static Selection,
                        &'static Security,
                    )| compiler.load_assertions("space", selection, security),
                ),
            ),
        ];
        for (phase, size) in phases {
            assert!(size <= 32 * 1024, "{phase} future is {size} bytes");
        }
    }

    fn entity(name: &str, entity_type: &str) -> Entity {
        Entity {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".to_string(),
                "test".to_string(),
            ),
            entity_type: EntityType::new(entity_type),
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

    #[tokio::test]
    #[expect(
        clippy::too_many_lines,
        clippy::significant_drop_tightening,
        reason = "Keep the complete attempt/space counterexample and original fixture cleanup order together."
    )]
    async fn managed_graph_reads_keep_original_attempt_and_space() {
        use elephant_storage::{canonical_commit::CanonicalRepository, WorkItemRepository};
        use elephant_types::records::{WorkItem, WorkItemStatus};

        for embedded in [true, false] {
            let storage = if embedded {
                StorageClient::new_embedded().await.expect("embedded")
            } else {
                let storage = StorageClient::new_memory().await.expect("native memory");
                elephant_migrations::apply_schema(&storage)
                    .await
                    .expect("schema");
                storage
            };
            crate::tests::register_space(&storage).await;
            let works = WorkItemRepository::new(storage.clone());
            let original = works
                .create(WorkItem {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::WorkItem),
                        "space".into(),
                        "graph-authority-test".into(),
                    ),
                    kind: "wiki_compile".into(),
                    payload: serde_json::json!({}),
                    status: WorkItemStatus::Runnable,
                    priority: 0,
                    idempotency_key: Uuid::new_v4().to_string(),
                    attempts: 0,
                    lease_expires_at: None,
                    leased_by: None,
                    run_after: None,
                    last_error: None,
                })
                .await
                .expect("original work");
            let original = works
                .claim_with_lease(
                    "space",
                    &original.base.id,
                    "original-graph-worker",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("claim")
                .expect("claimed");
            let fence = elephant_storage::work_attempt_repository::fence_from_claim(&original)
                .expect("original fence");
            let views = KnowledgeViewRepository::new(storage.clone());
            let proof = views
                .prepare_managed_publication("space", Some(&fence), None)
                .await
                .expect("original managed proof");
            let compiler =
                GraphViewCompiler::new(storage.clone()).with_managed_authority(proof.clone());
            let missing = KnowledgeViewDefinition {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::KnowledgeViewDefinition),
                    "space".into(),
                    "graph-authority-test".into(),
                ),
                title: "Missing definition read sentinel".into(),
                publication_status: KnowledgeViewPublicationStatus::Draft,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Query {
                    entity_types: Vec::new(),
                    name_contains: None,
                    attributes: Vec::new(),
                    scan_limit: 10,
                    max_members: 10,
                },
            };
            assert!(compiler
                .find_existing("space", &missing)
                .await
                .expect("authorized lookup")
                .is_none());
            assert!(matches!(
                compiler
                    .prepare_context("space", &missing.base.id, false)
                    .await,
                Err(StorageError::NotFound(_))
            ));
            assert_original_authority_refused(
                compiler
                    .prepare_context("different-space", &missing.base.id, false)
                    .await,
            );
            works
                .requeue_with_delay("space", &original.base.id, &fence.lease_token, 0, None)
                .await
                .expect("original guarded release")
                .expect("released");
            let successor = works
                .claim_with_lease(
                    "space",
                    &original.base.id,
                    "successor-graph-worker",
                    Utc::now() + chrono::Duration::minutes(5),
                )
                .await
                .expect("successor claim")
                .expect("successor claimed");
            let successor_fence =
                elephant_storage::work_attempt_repository::fence_from_claim(&successor)
                    .expect("successor fence");
            assert_ne!(fence, successor_fence);
            views
                .prepare_managed_publication("space", Some(&successor_fence), None)
                .await
                .expect("successor itself is valid")
                .verify_current()
                .await
                .expect("live successor");
            let canonical = CanonicalRepository::new(storage);
            let epoch = canonical.epoch("space").await.expect("original read epoch");
            assert_original_authority_refused(proof.verify_current().await);
            assert_original_authority_refused(compiler.find_existing("space", &missing).await);
            assert_original_authority_refused(
                compiler
                    .clone()
                    .prepare_context("space", &missing.base.id, false)
                    .await,
            );
            assert_original_authority_refused(
                compiler.compile("space", &missing.base.id, true).await,
            );
            assert_eq!(
                canonical.epoch("space").await.expect("unchanged epoch"),
                epoch
            );
        }
    }

    #[expect(
        clippy::needless_pass_by_value,
        reason = "Consume the original result at the authority assertion boundary, preserving existing test calls."
    )]
    fn assert_original_authority_refused<T: std::fmt::Debug>(result: StorageResult<T>) {
        assert!(
            matches!(&result, Err(StorageError::Conflict(message))
                if message == "knowledge_view_original_authority_changed"),
            "expected original authority refusal, got {result:?}"
        );
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the repository handle through both draft/publication assertions and original cleanup."
    )]
    async fn assert_inspector_draft(compiler: &GraphViewCompiler, result: &CompiledGraphView) {
        assert_eq!(result.view.kind, KnowledgeViewKind::Topic);
        assert_eq!(result.view.surface, KnowledgeViewSurface::Inspector);
        assert_eq!(
            result.view.publication_status,
            KnowledgeViewPublicationStatus::Draft
        );
        let repository = compiler.repository();
        let published = repository
            .list_views_for_surface(
                "space",
                KnowledgeViewSurface::Inspector,
                KnowledgeViewPublicationStatus::Published,
                10,
            )
            .await
            .expect("published inspector projections");
        let drafts = repository
            .list_views_for_surface(
                "space",
                KnowledgeViewSurface::Inspector,
                KnowledgeViewPublicationStatus::Draft,
                10,
            )
            .await
            .expect("draft inspector projections");
        assert!(published.is_empty());
        assert_eq!(drafts.len(), 1);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::large_futures,
        reason = "Preserve original embedded fixture ownership and the unboxed awaited lineage-test future."
    )]
    async fn embedded_graph_roots_resolve_source_lineage_without_spans() {
        let storage = StorageClient::new_embedded()
            .await
            .expect("embedded storage");
        assert_graph_roots_resolve_source_lineage_without_spans(storage).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::large_futures,
        reason = "Preserve original migrated fixture ownership and the unboxed awaited lineage-test future."
    )]
    async fn surreal_graph_roots_resolve_source_lineage_without_spans() {
        let storage = StorageClient::new(elephant_storage::StorageConfig {
            url: "mem://".into(),
            database: format!("graph_lineage_{}", Uuid::new_v4()),
            ..elephant_storage::StorageConfig::default()
        })
        .await
        .expect("native storage");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("native schema");
        assert_graph_roots_resolve_source_lineage_without_spans(storage).await;
    }

    // These real repository inputs exercise the retained source-first path
    // without preview heads. The original Definition integration regressions
    // separately retain their immutable-publication, raw-normalization and
    // pending-generation oracles on both backends.
    #[allow(clippy::too_many_lines)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep source/compiler handles through the original deletion and no-publication assertions."
    )]
    async fn assert_graph_roots_resolve_source_lineage_without_spans(storage: StorageClient) {
        use elephant_storage::SourceRepository;
        use elephant_types::records::{Source, SourceType};

        crate::tests::register_space(&storage).await;
        let sources = SourceRepository::new(storage.clone());
        let mut original_sources = Vec::new();
        for label in ["definition input", "selected root input"] {
            original_sources.push(
                sources
                    .create(Source::new(
                        BaseFields::new(
                            RecordId::new_v4(TableName::Source),
                            "space".into(),
                            "source-lineage-control".into(),
                        ),
                        SourceType::Manual,
                        label.into(),
                    ))
                    .await
                    .expect("actual original Source"),
            );
        }
        let mut root = entity("Source-backed root", "topic");
        root.base.provenance.sources = vec![original_sources[1].base.id.clone()];
        let root = EntityRepository::new(storage.clone())
            .create(root)
            .await
            .expect("actual selected root");
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::KnowledgeViewDefinition),
            "space".into(),
            "source-lineage-control".into(),
        );
        base.provenance.sources = vec![original_sources[0].base.id.clone()];
        let definition = KnowledgeViewDefinitionRepository::new(storage.clone())
            .upsert(KnowledgeViewDefinition {
                base,
                title: "Original source lineage".into(),
                publication_status: KnowledgeViewPublicationStatus::Draft,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Topic {
                    topic: Link::new(root.base.id),
                    relation_types: Vec::new(),
                    max_hops: 1,
                    max_members: 2,
                },
            })
            .await
            .expect("actual definition");
        let compiler = GraphViewCompiler::new(storage);
        let prepared = compiler
            .prepare_context("space", &definition.base.id, false)
            .await
            .expect("original source-backed context without span citations");
        assert!(!prepared.persisted);
        assert!(prepared
            .revision
            .citations
            .iter()
            .all(|citation| citation.evidence_span.is_none()));
        let source_dependencies = prepared
            .revision
            .dependencies
            .iter()
            .filter(|dependency| dependency.record_type == "source")
            .collect::<Vec<_>>();
        assert_eq!(source_dependencies.len(), original_sources.len());
        for source in &original_sources {
            let dependency = source_dependencies
                .iter()
                .find(|dependency| dependency.record_id == source.base.id.to_string())
                .expect("both definition and selected-root Source dependencies");
            assert_eq!(dependency.observed_updated_at, Some(source.base.updated_at));
            assert!(prepared
                .revision
                .base
                .provenance
                .sources
                .contains(&source.base.id));
        }
        assert_eq!(prepared.revision.compiler.version, GRAPH_COMPILER_VERSION);
        // Neither definition provenance nor a visible selected root can bypass
        // the actual Source envelope when there are no span citations.
        for source in &original_sources {
            let mut denied = source.clone();
            denied.base.security.policy_id = "different-source-policy".into();
            sources
                .update("space", &source.base.id, denied)
                .await
                .expect("changed raw policy");
            let result = compiler
                .prepare_context("space", &definition.base.id, true)
                .await;
            sources
                .update("space", &source.base.id, source.clone())
                .await
                .expect("restore raw source");
            assert!(matches!(result, Err(StorageError::Conflict(message))
                if message == "Projection source is not currently published"));
        }
        sources
            .delete("space", &original_sources[1].base.id)
            .await
            .expect("remove selected-root source");
        assert!(matches!(
            compiler.prepare_context("space", &definition.base.id, true).await,
            Err(StorageError::Conflict(message)) if message == "Projection input Source is absent"
        ));
        assert!(compiler
            .repository()
            .list_views("space", 10)
            .await
            .expect("no implicit publication")
            .is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve original graph fixture ownership and cleanup across traversal/trigger assertions."
    )]
    async fn topic_view_traverses_approved_graph_and_registers_trigger() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        crate::tests::register_space(&storage).await;
        let entities = EntityRepository::new(storage.clone());
        let relations = RelRepository::new(storage.clone());
        let definitions = KnowledgeViewDefinitionRepository::new(storage.clone());
        let topic = entity("Rust", "topic");
        let topic_id = topic.base.id.clone();
        let member = entity("Borrow checker", "concept");
        let member_id = member.base.id.clone();
        entities.create(topic).await.expect("topic");
        entities.create(member).await.expect("member");
        relations
            .create(Rel {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Rel),
                    "space".to_string(),
                    "test".to_string(),
                ),
                subject: Link::new(topic_id.clone()),
                object: Link::new(member_id),
                rel_type: "includes_concept".to_string(),
                status: ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: Vec::new(),
                temporal_audit: None,
            })
            .await
            .expect("relation");
        let definition_id = RecordId::new(TableName::KnowledgeViewDefinition, Uuid::from_u128(1));
        definitions
            .upsert(KnowledgeViewDefinition {
                base: BaseFields::new(
                    definition_id.clone(),
                    "space".to_string(),
                    "test".to_string(),
                ),
                title: "Rust concepts".to_string(),
                publication_status: KnowledgeViewPublicationStatus::Published,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Topic {
                    topic: Link::new(topic_id),
                    relation_types: vec!["includes_concept".to_string()],
                    max_hops: 1,
                    max_members: 10,
                },
            })
            .await
            .expect("definition");
        let compiler = GraphViewCompiler::new(storage);
        let result = compiler
            .compile("space", &definition_id, false)
            .await
            .expect("compile");

        assert_inspector_draft(&compiler, &result).await;
        assert!(result.revision.rendered_markdown.contains("Borrow checker"));
        assert!(result.revision.rendered_markdown.contains("Rust"));
        assert!(!result
            .revision
            .rendered_markdown
            .contains("deterministic projection"));
        assert!(result
            .revision
            .dependencies
            .iter()
            .any(|dependency| dependency.record_id == "trigger:table:rel"));
        assert_eq!(result.revision.freshness.unsupported_claims, 1);
        assert_eq!(
            compiler
                .repository()
                .mark_dirty_for_dependencies(
                    "space",
                    &["trigger:table:rel".to_string()],
                    "new relationship"
                )
                .await
                .expect("dirty"),
            1
        );
    }

    #[test]
    fn common_relationship_predicates_read_as_semantic_prose() {
        assert_eq!(predicate_phrase("parent_of"), "is a parent of");
        assert_eq!(predicate_phrase("works_at"), "works at");
        assert_eq!(
            predicate_phrase("has_unusual_connection"),
            "is connected to through has unusual connection"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve original saved-query fixture ownership through bounds and dependency assertions."
    )]
    async fn saved_query_uses_exact_approved_attributes_and_discloses_bounds() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        crate::tests::register_space(&storage).await;
        let entities = EntityRepository::new(storage.clone());
        let attributes = AttributeRepository::new(storage.clone());
        let definitions = KnowledgeViewDefinitionRepository::new(storage.clone());
        let ada = entity("Ada Lovelace", "person");
        let ada_id = ada.base.id.clone();
        let charles = entity("Charles Babbage", "person");
        entities.create(ada).await.expect("Ada");
        entities.create(charles).await.expect("Charles");
        attributes
            .create(Attribute {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::Attribute),
                    "space".to_string(),
                    "test".to_string(),
                ),
                subject: Link::new(ada_id),
                attr_type: "occupation".to_string(),
                value: Literal::String("mathematician".to_string()),
                status: ClaimStatus::Approved,
                confidence: 1.0,
                validity_window: None,
                evidence: Vec::new(),
                temporal_audit: None,
            })
            .await
            .expect("attribute");
        let definition_id = RecordId::new(TableName::KnowledgeViewDefinition, Uuid::from_u128(1));
        definitions
            .upsert(KnowledgeViewDefinition {
                base: BaseFields::new(
                    definition_id.clone(),
                    "space".to_string(),
                    "test".to_string(),
                ),
                title: "Mathematicians".to_string(),
                publication_status: KnowledgeViewPublicationStatus::Draft,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Query {
                    entity_types: vec!["person".to_string()],
                    name_contains: None,
                    attributes: vec![KnowledgeQueryAttributeFilter {
                        attr_type: "occupation".to_string(),
                        value: Literal::String("mathematician".to_string()),
                    }],
                    scan_limit: 100,
                    max_members: 10,
                },
            })
            .await
            .expect("definition");

        let compiled = GraphViewCompiler::new(storage)
            .compile("space", &definition_id, false)
            .await
            .expect("compile");

        assert_eq!(compiled.view.kind, KnowledgeViewKind::Query);
        assert!(compiled.revision.rendered_markdown.contains("Ada Lovelace"));
        assert!(!compiled
            .revision
            .rendered_markdown
            .contains("Charles Babbage"));
        assert!(compiled
            .revision
            .dependencies
            .iter()
            .any(|dependency| dependency.record_id == "trigger:table:attribute"));
        assert!(compiled.revision.validation.prose_is_structured);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the migrated graph repositories through the complete original round-trip assertions."
    )]
    async fn graph_definition_and_revision_round_trip_through_migrated_surreal_store() {
        let storage = StorageClient::new(elephant_storage::StorageConfig {
            url: "mem://".to_string(),
            database: format!("graph_view_{}", Uuid::new_v4()),
            ..elephant_storage::StorageConfig::default()
        })
        .await
        .expect("surreal storage");
        elephant_migrations::apply_schema(&storage)
            .await
            .expect("schema");
        crate::tests::register_space(&storage).await;
        let topic = entity("Knowledge graphs", "topic");
        let topic_id = topic.base.id.clone();
        EntityRepository::new(storage.clone())
            .create(topic)
            .await
            .expect("topic");
        let definition_id = RecordId::new(TableName::KnowledgeViewDefinition, Uuid::from_u128(1));
        let definitions = KnowledgeViewDefinitionRepository::new(storage.clone());
        definitions
            .upsert(KnowledgeViewDefinition {
                base: BaseFields::new(
                    definition_id.clone(),
                    "space".to_string(),
                    "test".to_string(),
                ),
                title: "Knowledge graphs".to_string(),
                publication_status: KnowledgeViewPublicationStatus::Draft,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Topic {
                    topic: Link::new(topic_id),
                    relation_types: Vec::new(),
                    max_hops: 1,
                    max_members: 10,
                },
            })
            .await
            .expect("definition");
        let second_definition_id =
            RecordId::new(TableName::KnowledgeViewDefinition, Uuid::from_u128(2));
        definitions
            .upsert(KnowledgeViewDefinition {
                base: BaseFields::new(
                    second_definition_id.clone(),
                    "space".to_string(),
                    "test".to_string(),
                ),
                title: "All topics".to_string(),
                publication_status: KnowledgeViewPublicationStatus::Hidden,
                related_articles: Vec::new(),
                navigation: None,
                selector: KnowledgeViewSelector::Query {
                    entity_types: vec!["topic".to_string()],
                    name_contains: None,
                    attributes: Vec::new(),
                    scan_limit: 100,
                    max_members: 10,
                },
            })
            .await
            .expect("second definition");

        let loaded_definition = definitions
            .get("space", &definition_id)
            .await
            .expect("load definition")
            .expect("definition exists");
        assert_eq!(loaded_definition.selector.kind(), KnowledgeViewKind::Topic);
        assert_eq!(
            loaded_definition.publication_status,
            KnowledgeViewPublicationStatus::Draft
        );
        let first_page = definitions
            .list_page("space", 1, None)
            .await
            .expect("first definition page");
        assert_eq!(first_page[0].base.id, definition_id);
        let second_page = definitions
            .list_page("space", 1, Some(&first_page[0].base.id))
            .await
            .expect("second definition page");
        assert_eq!(second_page[0].base.id, second_definition_id);
        let compiled = GraphViewCompiler::new(storage.clone())
            .compile("space", &definition_id, false)
            .await
            .expect("compile");
        assert_eq!(
            compiled.view.publication_status,
            KnowledgeViewPublicationStatus::Draft
        );
        let indexes = KnowledgeViewRepository::new(storage)
            .list_dependency_indexes("space", &compiled.view.base.id)
            .await
            .expect("indexes");
        assert!(indexes
            .iter()
            .any(|index| index.dependency_record == "trigger:table:rel"));
    }
}
