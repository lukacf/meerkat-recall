//! Validation shared by both knowledge-view publication transactions.
//!
//! This does not own source authority. It uses the current-read projection and
//! support proofs, then retains the caller's original canonical epoch through
//! the atomic write. Every source head, support, envelope and predicate read is
//! fenced by that epoch, including absent selector results.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;

use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::{CitationKind, HasSecurity, KnowledgeViewRevision, SystemEvent};
use elephant_types::{RecordId, TableName};
use serde::Serialize;

use crate::canonical_commit::{source_support_fingerprint, CanonicalRepository};
use crate::read_eligibility::{project_wiki_input, CurrentRead, ReadEligibility};
use crate::source_publication::{conflict, integrity, SourcePublicationRepository};
use crate::{KnowledgeViewRepository, Repository, StorageResult};

/// A derived row must preserve every contributing authorization restriction.
fn covers_envelope(output: &BaseFields, input: &BaseFields) -> bool {
    let state_rank = |state| match state {
        SubjectsState::Complete => 0,
        SubjectsState::Partial => 1,
        SubjectsState::Unknown => 2,
    };
    output.space_id == input.space_id
        && input.deleted_at.is_none()
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
        && state_rank(output.subjects_state) >= state_rank(input.subjects_state)
}

pub(super) fn metadata_dependency(
    dependency: &elephant_types::records::KnowledgeDependency,
) -> bool {
    dependency.observed_updated_at.is_none()
        && match dependency.record_type.as_str() {
            "model" => {
                dependency
                    .role
                    .split('+')
                    .all(|role| matches!(role, "article_writer" | "article_verifier"))
                    && dependency
                        .record_id
                        .strip_prefix("model:")
                        .is_some_and(|id| !id.is_empty())
            }
            "prompt" => {
                dependency
                    .role
                    .split('+')
                    .all(|role| matches!(role, "article_writer_prompt" | "article_verifier_prompt"))
                    && dependency
                        .record_id
                        .strip_prefix("prompt:")
                        .is_some_and(|id| !id.is_empty())
            }
            "selector_trigger" => {
                dependency.role == "potential_membership_change"
                    && matches!(
                        dependency.record_id.as_str(),
                        "trigger:table:entity" | "trigger:table:attribute" | "trigger:table:rel"
                    )
            }
            _ => false,
        }
}

impl KnowledgeViewRepository {
    /// Resolve the published inputs again without advancing or refreshing the
    /// original preparation fence. A provider result cannot authorize new input.
    #[allow(
        clippy::too_many_lines,
        reason = "Keep bounded context traversal and all typed dependency checks under one original read projection and final epoch verification."
    )]
    pub(super) async fn validate_publication_inputs(
        &self,
        revision: &KnowledgeViewRevision,
        observed_epoch: &str,
    ) -> StorageResult<Vec<BaseFields>> {
        if observed_epoch.is_empty()
            || CanonicalRepository::new(self.client.clone())
                .epoch(&revision.base.space_id)
                .await?
                != observed_epoch
        {
            return Err(conflict("knowledge_view_inputs_changed"));
        }
        let reader = ReadEligibility::new(self.client.clone());
        let mut bases = Vec::new();
        let mut visited = BTreeSet::new();
        let mut context_graph = BTreeMap::new();
        let mut pending = revision.dependencies.clone();
        bases.extend(
            self.validate_view_citations(revision, &reader, &revision.base)
                .await?,
        );
        let mut inspected = 0_usize;
        while let Some(dependency) = pending.pop() {
            inspected += 1;
            if inspected > 16_384 || visited.len() > 256 {
                return Err(conflict("knowledge_view_dependency_limit"));
            }
            if metadata_dependency(&dependency) {
                continue;
            }
            let id = RecordId::from_str(&dependency.record_id)
                .map_err(|_| conflict("knowledge_view_invalid_dependency"))?;
            if dependency.record_type != id.table().as_str() {
                return Err(conflict("knowledge_view_dependency_type_changed"));
            }
            macro_rules! validate {
                ($repo:ident) => {{
                    let row = crate::$repo::new(self.client.clone())
                        .get(&revision.base.space_id, &id)
                        .await?
                        .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                    self.validate_view_record(
                        row,
                        &reader,
                        &revision.base,
                        dependency.observed_updated_at,
                    )
                    .await?
                }};
            }
            let base = match id.table() {
                TableName::Source => validate!(SourceRepository),
                TableName::Doc => validate!(DocRepository),
                TableName::DocRevision => validate!(DocRevisionRepository),
                TableName::DocNode => validate!(DocNodeRepository),
                TableName::Entity => validate!(EntityRepository),
                TableName::Assertion => validate!(AssertionRepository),
                TableName::Rel => validate!(RelRepository),
                TableName::Attribute => validate!(AttributeRepository),
                TableName::Event => validate!(EventRepository),
                TableName::TruthSlot => validate!(TruthSlotRepository),
                TableName::Freshness => validate!(FreshnessRepository),
                TableName::SourceWatch => validate!(SourceWatchRepository),
                TableName::KnowledgeViewDefinition => validate!(KnowledgeViewDefinitionRepository),
                TableName::SourceWatchCheck => {
                    let row = crate::SourceWatchRepository::new(self.client.clone())
                        .get_check(&revision.base.space_id, &id)
                        .await?
                        .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                    self.validate_view_record(
                        row,
                        &reader,
                        &revision.base,
                        dependency.observed_updated_at,
                    )
                    .await?
                }
                TableName::KnowledgeView | TableName::KnowledgeViewRevision => {
                    let (base, nested) = if id.table() == TableName::KnowledgeView {
                        let view = self
                            .get_view(&revision.base.space_id, &id)
                            .await?
                            .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                        let nested = self
                            .get_current_revision(&view)
                            .await?
                            .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                        (view.base, nested)
                    } else {
                        let nested = self
                            .get_revision(&revision.base.space_id, &id)
                            .await?
                            .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                        let view_id = RecordId::from_str(&nested.view_id)
                            .map_err(|_| conflict("knowledge_view_invalid_dependency"))?;
                        let view = self
                            .get_view(&revision.base.space_id, &view_id)
                            .await?
                            .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                        if view.current_revision.as_deref() != Some(dependency.record_id.as_str())
                            || !covers_envelope(&revision.base, &view.base)
                        {
                            return Err(conflict("knowledge_view_context_revision_changed"));
                        }
                        bases.push(view.base);
                        (nested.base.clone(), nested)
                    };
                    if !covers_envelope(&revision.base, &base)
                        || dependency
                            .observed_updated_at
                            .is_some_and(|at| at != base.updated_at)
                        || !covers_envelope(&revision.base, &nested.base)
                    {
                        return Err(conflict("knowledge_view_dependency_changed"));
                    }
                    // Bound cycles while retaining every material dependency of
                    // a previously published context projection.
                    if visited.insert(id.clone()) {
                        context_graph.insert(
                            id.clone(),
                            nested
                                .dependencies
                                .iter()
                                .filter_map(|dependency| {
                                    RecordId::from_str(&dependency.record_id).ok()
                                })
                                .filter(|id| {
                                    matches!(
                                        id.table(),
                                        TableName::KnowledgeView | TableName::KnowledgeViewRevision
                                    )
                                })
                                .collect::<BTreeSet<_>>(),
                        );
                        bases.extend(
                            self.validate_view_citations(&nested, &reader, &revision.base)
                                .await?,
                        );
                        pending.extend(nested.dependencies);
                    }
                    base
                }
                _ => return Err(conflict("knowledge_view_unsupported_dependency")),
            };
            bases.push(base);
        }
        validate_context_graph(&context_graph)?;
        reader.verify_current().await?;
        drop(reader);
        Ok(bases)
    }

    async fn validate_view_citations(
        &self,
        revision: &KnowledgeViewRevision,
        reader: &ReadEligibility,
        output: &BaseFields,
    ) -> StorageResult<Vec<BaseFields>> {
        let authorize = |base: &BaseFields| covers_envelope(output, base);
        let dependency_ids = revision
            .dependencies
            .iter()
            .map(|row| row.record_id.as_str())
            .collect::<BTreeSet<_>>();
        let mut bases = Vec::new();
        for citation in &revision.citations {
            if !dependency_ids.contains(citation.record_id.as_str())
                || RecordId::from_str(&citation.record_id).is_err()
            {
                return Err(conflict("knowledge_view_citation_dependency_missing"));
            }
            if citation.kind == CitationKind::EvidenceSpan && citation.evidence_span.is_none() {
                return Err(conflict("knowledge_view_evidence_missing"));
            }
            if let Some(span) = &citation.evidence_span {
                let evidence = reader
                    .evidence(&revision.base.space_id, span, &authorize)
                    .await?
                    .ok_or_else(|| conflict("knowledge_view_evidence_not_current"))?;
                if citation.source_id.as_deref().is_some_and(|id| {
                    evidence
                        .source
                        .as_ref()
                        .map(|source| source.base.id.to_string())
                        .as_deref()
                        != Some(id)
                }) {
                    return Err(conflict("knowledge_view_citation_source_changed"));
                }
                if let Some(assertion_id) = &citation.assertion_id {
                    let id = RecordId::from_str(assertion_id)
                        .map_err(|_| conflict("knowledge_view_invalid_dependency"))?;
                    let assertion = crate::AssertionRepository::new(self.client.clone())
                        .get(&revision.base.space_id, &id)
                        .await?
                        .ok_or_else(|| conflict("knowledge_view_assertion_missing"))?;
                    if assertion.evidence != *span
                        || project_wiki_input(reader, assertion.clone(), &authorize)
                            .await?
                            .is_none()
                    {
                        return Err(conflict("knowledge_view_assertion_evidence_changed"));
                    }
                    bases.push(assertion.base);
                }
                bases.push(evidence.revision.base);
                bases.push(evidence.doc.base);
                if let Some(source) = evidence.source {
                    // Evidence can project the immutable published source while
                    // a newer desired source is pending. Fence the actual live
                    // envelope; eligibility already checked the published one.
                    let live = crate::SourceRepository::new(self.client.clone())
                        .get(&revision.base.space_id, &source.base.id)
                        .await?
                        .ok_or_else(|| conflict("knowledge_view_dependency_missing"))?;
                    if !authorize(&live.base) {
                        return Err(conflict("knowledge_view_dependency_changed"));
                    }
                    bases.push(live.base);
                }
                if let Some(node) = evidence.node {
                    bases.push(node.base);
                }
            }
        }
        Ok(bases)
    }

    // Construct the boxed future at this synchronous boundary: the dependency
    // dispatch must not reserve every typed validation future on its stack.
    #[inline(never)]
    fn validate_view_record<'a, T>(
        &'a self,
        row: T,
        reader: &'a ReadEligibility,
        output: &'a BaseFields,
        observed: Option<chrono::DateTime<chrono::Utc>>,
    ) -> std::pin::Pin<Box<impl std::future::Future<Output = StorageResult<BaseFields>> + 'a>>
    where
        T: CurrentRead + HasSecurity + PartialEq + Serialize + 'a,
    {
        Box::pin(async move {
            let base = row.read_base().clone();
            let authorize = |input: &BaseFields| covers_envelope(output, input);
            let current = if base.id.table() == TableName::Source {
                // Source projection authorizes both the actual live envelope and
                // the immutable accepted envelope. Their timestamps and lifecycle
                // can differ while the published revision remains current, so the
                // exact root-envelope guard used for implicit subjects is not the
                // Source guard. The reader still fences the original live row.
                reader.project(row.clone(), &authorize).await?
            } else {
                project_wiki_input(reader, row.clone(), &authorize).await?
            }
            .ok_or_else(|| conflict("knowledge_view_dependency_not_current"))?;
            if observed.is_some_and(|at| at != current.read_base().updated_at) {
                return Err(conflict("knowledge_view_dependency_changed"));
            }
            if current != row && base.id.table() != TableName::Source {
                let mut stored = serde_json::to_value(&row).map_err(invalid_input)?;
                let mut projected = serde_json::to_value(&current).map_err(invalid_input)?;
                let adornment = base.id.table() == TableName::DocNode
                    && stored.get("text").is_none_or(serde_json::Value::is_null);
                if adornment {
                    stored
                        .as_object_mut()
                        .ok_or_else(|| integrity("invalid node record"))?
                        .remove("text");
                    projected
                        .as_object_mut()
                        .ok_or_else(|| integrity("invalid node record"))?
                        .remove("text");
                }
                if !adornment || stored != projected {
                    return Err(conflict("knowledge_view_dependency_projection_changed"));
                }
            }
            let fingerprint = if base.id.table() == TableName::Source {
                source_support_fingerprint(&current)?
            } else {
                // A projected node may materialize its immutable text lazily; the
                // support receipt certifies the persisted row, not this adornment.
                source_support_fingerprint(&row)?
            };
            let supports = SourcePublicationRepository::new(self.client.clone())
                .support_for_record(&base.space_id, &base.id)
                .await?;
            if supports
                .iter()
                .any(|support| support.record_fingerprint != fingerprint)
            {
                return Err(conflict("knowledge_view_support_content_changed"));
            }
            Ok(base)
        })
    }
}

fn validate_context_graph(graph: &BTreeMap<RecordId, BTreeSet<RecordId>>) -> StorageResult<()> {
    fn visit(
        id: &RecordId,
        graph: &BTreeMap<RecordId, BTreeSet<RecordId>>,
        active: &mut BTreeSet<RecordId>,
        complete: &mut BTreeSet<RecordId>,
    ) -> StorageResult<()> {
        if complete.contains(id) {
            return Ok(());
        }
        if !active.insert(id.clone()) {
            return Err(conflict("knowledge_view_context_cycle"));
        }
        if let Some(edges) = graph.get(id) {
            for edge in edges {
                visit(edge, graph, active, complete)?;
            }
        }
        active.remove(id);
        complete.insert(id.clone());
        Ok(())
    }
    let mut active = BTreeSet::new();
    let mut complete = BTreeSet::new();
    for id in graph.keys() {
        visit(id, graph, &mut active, &mut complete)?;
    }
    Ok(())
}

pub(super) fn publication_event(
    view: &elephant_types::records::KnowledgeView,
    revision: &KnowledgeViewRevision,
) -> SystemEvent {
    let mut base = revision.base.clone();
    base.id = RecordId::new(
        TableName::SystemEvent,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("knowledge-view-published:{}", revision.base.id).as_bytes(),
        ),
    );
    // The immutable revision is the complete receipt. The outbox event exposes
    // only opaque identities and deliberately carries no rendered prose.
    SystemEvent {
        base,
        seq: 0,
        event_type: "knowledge_view.published".into(),
        payload: serde_json::json!({
            "view_id":view.base.id,"revision_id":revision.base.id,
            "records":[{"id":view.base.id,"table":"knowledge_view","op":"update"}],
        }),
    }
}

pub(super) fn input_changed() -> crate::StorageError {
    conflict("knowledge_view_inputs_changed")
}

pub(super) fn invalid_input(error: impl std::fmt::Display) -> crate::StorageError {
    integrity(&error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_cycles_are_rejected_but_shared_dependencies_are_accepted() {
        let a = RecordId::new_v4(TableName::KnowledgeViewRevision);
        let b = RecordId::new_v4(TableName::KnowledgeViewRevision);
        let c = RecordId::new_v4(TableName::KnowledgeViewRevision);
        let mut graph = BTreeMap::from([
            (a.clone(), BTreeSet::from([b.clone(), c.clone()])),
            (b, BTreeSet::from([c.clone()])),
            (c.clone(), BTreeSet::new()),
        ]);
        assert!(validate_context_graph(&graph).is_ok());
        graph.insert(c, BTreeSet::from([a]));
        assert!(validate_context_graph(&graph).is_err());
    }
}
