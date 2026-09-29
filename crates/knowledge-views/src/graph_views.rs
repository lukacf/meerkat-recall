//! Deterministic topic, community, and saved-query projections.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use chrono::Utc;
use elephant_storage::{
    AssertionRepository, AttributeRepository, EntityRepository, KnowledgeViewDefinitionRepository,
    KnowledgeViewRepository, RelRepository, Repository, StorageClient, StorageError, StorageResult,
};
use elephant_types::records::base::{BaseFields, SubjectsState};
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
pub const GRAPH_COMPILER_VERSION: u32 = 3;

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
    views: KnowledgeViewRepository,
    definitions: KnowledgeViewDefinitionRepository,
    entities: EntityRepository,
    relations: RelRepository,
    attributes: AttributeRepository,
    assertions: AssertionRepository,
}

impl GraphViewCompiler {
    /// Creates a graph-view compiler.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            views: KnowledgeViewRepository::new(storage.clone()),
            definitions: KnowledgeViewDefinitionRepository::new(storage.clone()),
            entities: EntityRepository::new(storage.clone()),
            relations: RelRepository::new(storage.clone()),
            attributes: AttributeRepository::new(storage.clone()),
            assertions: AssertionRepository::new(storage),
        }
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
        let fingerprint = security_fingerprint(&definition.base.security)?;
        self.views
            .find_by_key(space_id, &graph_view_key(&definition.base.id, &fingerprint))
            .await
    }

    /// Compiles a governed graph-view definition.
    #[allow(clippy::too_many_lines)]
    pub async fn compile(
        &self,
        space_id: &str,
        definition_id: &RecordId,
        force: bool,
    ) -> StorageResult<CompiledGraphView> {
        let definition = self
            .definitions
            .get(space_id, definition_id)
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
        let existing = self.views.find_by_key(space_id, &key).await?;
        if !force {
            if let Some(existing_view) = existing.as_ref() {
                if let Some(current) = self.views.get_current_revision(existing_view).await? {
                    if super::reusable_current_revision(
                        existing_view,
                        &current,
                        GRAPH_COMPILER_VERSION,
                    ) {
                        return Ok(CompiledGraphView {
                            view: existing_view.clone(),
                            revision: current,
                            changed: false,
                        });
                    }
                }
            }
        }
        let selection = match &definition.selector {
            KnowledgeViewSelector::Topic {
                topic,
                relation_types,
                max_hops,
                max_members,
            } => {
                self.select_graph(
                    space_id,
                    std::slice::from_ref(topic),
                    relation_types,
                    *max_hops,
                    *max_members,
                    true,
                    &security,
                )
                .await?
            }
            KnowledgeViewSelector::Community {
                seeds,
                relation_types,
                max_hops,
                max_members,
            } => {
                self.select_graph(
                    space_id,
                    seeds,
                    relation_types,
                    *max_hops,
                    *max_members,
                    false,
                    &security,
                )
                .await?
            }
            KnowledgeViewSelector::Query {
                entity_types,
                name_contains,
                attributes,
                scan_limit,
                max_members,
            } => {
                self.select_query(
                    space_id,
                    entity_types,
                    name_contains.as_deref(),
                    attributes,
                    *scan_limit,
                    *max_members,
                    &security,
                )
                .await?
            }
        };
        let assertions = self
            .load_assertions(space_id, &selection, &security)
            .await?;
        let draft = build_graph_draft(&definition, &selection, &assertions, &fingerprint)?;

        if !force {
            if let Some(existing_view) = existing.as_ref() {
                if let Some(current) = self.views.get_current_revision(existing_view).await? {
                    if existing_view.state != ProjectionState::Dirty
                        && current.compiler.dependency_hash == draft.dependency_hash
                        && current.compiler.version == GRAPH_COMPILER_VERSION
                    {
                        return Ok(CompiledGraphView {
                            view: existing_view.clone(),
                            revision: current,
                            changed: false,
                        });
                    }
                }
            }
        }

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
        let (subjects, mut subjects_state) = scope.finish();
        if selection.truncated {
            subjects_state = SubjectsState::Partial;
        }
        let provenance = projection_provenance(
            &draft.citations,
            &draft.dependency_hash,
            GRAPH_COMPILER_NAME,
            GRAPH_COMPILER_VERSION,
        );
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
        let (view, revision) = self.views.publish(view, revision).await?;
        Ok(CompiledGraphView {
            view,
            revision,
            changed: true,
        })
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
            let seed = self
                .entities
                .get(space_id, &seed_id)
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
            let mut edges = self.relations.find_outgoing(space_id, &current).await?;
            edges.extend(self.relations.find_incoming(space_id, &current).await?);
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
                let Some(entity) = self.entities.get(space_id, neighbor_id).await? else {
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
            let page = self
                .entities
                .list(space_id, page_limit, cursor.as_ref())
                .await?;
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
                    let attributes = self
                        .attributes
                        .find_by_subject_entity(space_id, &entity.base.id)
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
            if let Some(assertion) = self.assertions.get(space_id, &id).await? {
                if assertion.base.security == *security {
                    assertions.insert(id, assertion);
                }
            }
        }
        Ok(assertions)
    }
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
    async fn topic_view_traverses_approved_graph_and_registers_trigger() {
        let storage = StorageClient::new_embedded().await.expect("storage");
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
    async fn saved_query_uses_exact_approved_attributes_and_discloses_bounds() {
        let storage = StorageClient::new_embedded().await.expect("storage");
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
