//! Source-rooted Living Wiki projections.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use chrono::Utc;
use elephant_storage::{
    DocRepository, DocRevisionRepository, KnowledgeViewRepository, Repository, SourceRepository,
    SourceWatchRepository, StorageClient, StorageError, StorageResult,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{
    CitationKind, CompilerReceipt, Doc, DocRevision, EvidenceSpan, KnowledgeCitation,
    KnowledgeStatement, KnowledgeView, KnowledgeViewKind, KnowledgeViewPublicationStatus,
    KnowledgeViewRevision, KnowledgeViewSurface, ProjectionFreshness, ProjectionState, Source,
    SourceType, SourceWatch, SourceWatchCheck, SourceWatchStatus, StatementDerivation,
    StatementVerification, ViewBlockKind,
};
use elephant_types::{RecordId, TableName};
use sha2::{Digest, Sha256};

use super::{
    add_dependency, dependency_hash, deterministic_view_id, direct_statement, escape_markdown,
    projection_provenance, projection_state_reason, render_page, security_fingerprint, stable_slug,
    supported_block, validate_projection, RevisionDraft,
};

/// Stable identity of the deterministic source-view compiler.
pub const SOURCE_COMPILER_NAME: &str = "elephant.source_view";
/// Contract version of the deterministic source-view compiler.
pub const SOURCE_COMPILER_VERSION: u32 = 6;

const SOURCE_QUOTE_MAX_CHARS: usize = 280;

/// Bounds for one source projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceViewLimits {
    /// Maximum documents attached to the source.
    pub documents: usize,
    /// Maximum immutable revisions inspected per document.
    pub revisions_per_document: usize,
}

impl Default for SourceViewLimits {
    fn default() -> Self {
        Self {
            documents: 25,
            revisions_per_document: 10,
        }
    }
}

/// Result of a source compile-or-reuse operation.
#[derive(Debug, Clone, PartialEq)]
pub struct CompiledSourceView {
    /// Stable view definition.
    pub view: KnowledgeView,
    /// Current immutable revision.
    pub revision: KnowledgeViewRevision,
    /// Whether this call published a new revision.
    pub changed: bool,
}

/// Compiler and publication service for source-rooted knowledge views.
#[derive(Clone, Debug)]
pub struct SourceViewCompiler {
    storage: StorageClient,
    views: KnowledgeViewRepository,
    sources: SourceRepository,
    docs: DocRepository,
    revisions: DocRevisionRepository,
    watches: SourceWatchRepository,
}

impl SourceViewCompiler {
    /// Creates a compiler over one Elephant storage client.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            storage: storage.clone(),
            views: KnowledgeViewRepository::new(storage.clone()),
            sources: SourceRepository::new(storage.clone()),
            docs: DocRepository::new(storage.clone()),
            revisions: DocRevisionRepository::new(storage.clone()),
            watches: SourceWatchRepository::new(storage),
        }
    }

    /// Returns the repository used for persisted views.
    #[must_use]
    pub const fn repository(&self) -> &KnowledgeViewRepository {
        &self.views
    }

    /// Looks up the existing security-scoped view for a source.
    pub async fn find_existing(
        &self,
        space_id: &str,
        source: &Source,
    ) -> StorageResult<Option<KnowledgeView>> {
        let fingerprint = security_fingerprint(&source.base.security)?;
        self.views
            .find_by_key(space_id, &source_view_key(&source.base.id, &fingerprint))
            .await
    }

    /// Compiles and atomically publishes a source view unless its exact input
    /// receipt already matches the current revision.
    #[allow(clippy::too_many_lines)] // Coordinates lineage reads and atomic publication.
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the reader's captured lineage observations through final projection checks and epoch publication."
    )]
    pub async fn compile(
        &self,
        space_id: &str,
        source_id: &RecordId,
        limits: SourceViewLimits,
        force: bool,
    ) -> StorageResult<CompiledSourceView> {
        let observed_epoch =
            elephant_storage::canonical_commit::CanonicalRepository::new(self.storage.clone())
                .epoch(space_id)
                .await?;

        let source = self
            .sources
            .get(space_id, source_id)
            .await?
            .ok_or_else(|| StorageError::NotFound(source_id.to_string()))?;
        let reader = elephant_storage::read_eligibility::ReadEligibility::new(self.storage.clone());
        let source = reader
            .project(source, &|base| {
                base.space_id == space_id && !base.is_deleted()
            })
            .await?
            .ok_or_else(|| {
                StorageError::Conflict("Source has no current published projection".into())
            })?;
        let security = source.base.security.clone();
        let fingerprint = security_fingerprint(&security)?;
        let key = source_view_key(source_id, &fingerprint);
        let existing = self.views.find_by_key(space_id, &key).await?;
        // Recompute the bounded selection before cache reuse: an old dependency
        // list cannot prove that no new matching relation or document appeared.

        let mut docs = self
            .docs
            .find_by_source(space_id, source_id, limits.documents)
            .await?;
        docs = reader
            .filter(docs, &|base| {
                base.space_id == space_id && base.security == security && !base.is_deleted()
            })
            .await?;
        docs.sort_by(|left, right| {
            right
                .base
                .created_at
                .cmp(&left.base.created_at)
                .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
        });
        docs.truncate(limits.documents);

        let mut revisions = BTreeMap::new();
        for doc in &docs {
            let mut items = self.revisions.find_by_doc(&doc.base.id).await?;
            items = reader
                .filter(items, &|base| {
                    base.space_id == space_id && base.security == security && !base.is_deleted()
                })
                .await?;
            items.truncate(limits.revisions_per_document);
            revisions.insert(doc.base.id.clone(), items);
        }

        let watch = self
            .watches
            .find_by_source(space_id, source_id)
            .await?
            .filter(|watch| watch.base.security == security);
        let latest_check = if let Some(watch) = watch.as_ref() {
            self.watches
                .list_checks(space_id, &watch.base.id, 1)
                .await?
                .into_iter()
                .find(|check| check.base.security == security)
        } else {
            None
        };

        let draft = build_source_revision_draft(
            &source,
            &docs,
            &revisions,
            watch.as_ref(),
            latest_check.as_ref(),
            &fingerprint,
        )?;
        if !force {
            if let Some(existing_view) = existing.as_ref() {
                if let Some(current) = self.views.get_current_revision(existing_view).await? {
                    let freshness_due = current
                        .freshness
                        .valid_until
                        .is_some_and(|deadline| deadline <= Utc::now());
                    if existing_view.state != ProjectionState::Dirty
                        && (!freshness_due || current.freshness.expired_claims > 0)
                        && current.compiler.dependency_hash == draft.dependency_hash
                        && current.compiler.version == SOURCE_COMPILER_VERSION
                        && super::projection_is_current(&self.storage, &current).await?
                    {
                        super::verify_projection_epoch(&self.storage, space_id, &observed_epoch)
                            .await?;
                        return Ok(CompiledSourceView {
                            view: existing_view.clone(),
                            revision: current,
                            changed: false,
                        });
                    }
                }
            }
        }

        let mut scope = super::ProjectionSubjects::default();
        scope.include(&source);
        for doc in &docs {
            scope.include(doc);
        }
        for revision in revisions.values().flatten() {
            scope.include_base(&revision.base);
        }
        if let Some(watch) = &watch {
            scope.include(watch);
        }
        if let Some(check) = &latest_check {
            scope.include(check);
        }
        let (subjects, subjects_state) = scope.finish();

        let now = Utc::now();
        let view_id = existing
            .as_ref()
            .map_or_else(|| deterministic_view_id(&key), |view| view.base.id.clone());
        let revision_number = existing
            .as_ref()
            .map_or(1, |view| view.revision_count.saturating_add(1));
        let revision_id = RecordId::new_v7(TableName::KnowledgeViewRevision);
        let provenance = projection_provenance(
            &draft.citations,
            &draft.dependency_hash,
            SOURCE_COMPILER_NAME,
            SOURCE_COMPILER_VERSION,
        );

        let mut view_base = existing.as_ref().map_or_else(
            || {
                let mut base = BaseFields::new(
                    view_id.clone(),
                    space_id.to_string(),
                    SOURCE_COMPILER_NAME.to_string(),
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

        let title = source_title(&source, &docs);
        let state = draft.freshness.state;
        let view = KnowledgeView {
            base: view_base,
            key,
            kind: KnowledgeViewKind::Source,
            surface: KnowledgeViewSurface::Evidence,
            publication_status: KnowledgeViewPublicationStatus::Published,
            root_record: source_id.to_string(),
            title: title.clone(),
            slug: stable_slug(&title, source_id, "source"),
            security_fingerprint: fingerprint.clone(),
            current_revision: Some(revision_id.to_string()),
            revision_count: revision_number,
            state,
            state_reason: projection_state_reason(&draft.freshness),
            published_at: Some(now),
        };

        let mut revision_base = BaseFields::new(
            revision_id,
            space_id.to_string(),
            SOURCE_COMPILER_NAME.to_string(),
        );
        revision_base.security = security;
        revision_base.subjects.clone_from(&subjects);
        revision_base.subjects_state = subjects_state;
        revision_base.provenance = provenance;
        let revision = KnowledgeViewRevision {
            base: revision_base,
            view_id: view.base.id.to_string(),
            revision: revision_number,
            kind: KnowledgeViewKind::Source,
            root_record: source_id.to_string(),
            blocks: draft.blocks,
            rendered_markdown: draft.rendered_markdown,
            citations: draft.citations,
            dependencies: draft.dependencies,
            freshness: draft.freshness,
            validation: draft.validation,
            compiler: CompilerReceipt {
                compiler: SOURCE_COMPILER_NAME.to_string(),
                version: SOURCE_COMPILER_VERSION,
                dependency_hash: draft.dependency_hash,
                policy_fingerprint: fingerprint,
            },
            compiled_at: now,
            supersedes: existing.and_then(|view| view.current_revision),
        };
        if !super::projection_is_current(&self.storage, &revision).await? {
            return Err(StorageError::Conflict(
                "Projection has invalid or withdrawn evidence".into(),
            ));
        }
        let (view, revision) = self
            .views
            .publish_at_epoch(view, revision, &observed_epoch)
            .await?;
        Ok(CompiledSourceView {
            view,
            revision,
            changed: true,
        })
    }
}

#[allow(clippy::too_many_lines)] // Keeps the ordered lineage statement receipt construction visible.
fn build_source_revision_draft(
    source: &Source,
    docs: &[Doc],
    revisions: &BTreeMap<RecordId, Vec<DocRevision>>,
    watch: Option<&SourceWatch>,
    latest_check: Option<&SourceWatchCheck>,
    fingerprint: &str,
) -> StorageResult<RevisionDraft> {
    let mut dependencies = BTreeMap::new();
    let mut citations = Vec::new();
    add_dependency(
        &mut dependencies,
        &source.base.id,
        "source",
        "root",
        Some(source.base.updated_at),
    );
    let root_citation = source_citation(&mut citations, &source.base.id);

    let mut overview_statements = vec![direct_statement(
        "source-kind",
        format!(
            "- **Source type:** {}{}",
            source_type_name(source),
            super::citation_marks(std::slice::from_ref(&root_citation)),
        ),
        vec![root_citation.clone()],
    )];
    for (id, label, value) in [
        ("source-uri", "URI", source.uri.as_deref()),
        ("source-path", "Path", source.path.as_deref()),
        (
            "source-external-id",
            "External ID",
            source.external_id.as_deref(),
        ),
    ] {
        if let Some(value) = value {
            overview_statements.push(direct_statement(
                id,
                format!(
                    "- **{}:** {}{}",
                    label,
                    escape_markdown(value),
                    super::citation_marks(std::slice::from_ref(&root_citation)),
                ),
                vec![root_citation.clone()],
            ));
        }
    }
    overview_statements.push(direct_statement(
        "source-hash",
        format!(
            "- **Content hash:** {}{}",
            escape_markdown(&source.hash),
            super::citation_marks(std::slice::from_ref(&root_citation)),
        ),
        vec![root_citation.clone()],
    ));
    let mut blocks = vec![supported_block(
        "overview",
        ViewBlockKind::Overview,
        "Source overview",
        overview_statements,
    )];

    blocks.push(supported_block(
        "source-lifecycle",
        ViewBlockKind::CurrentFacts,
        "Ingest lifecycle",
        vec![direct_statement(
            "source-lifecycle-1",
            format!(
                "- **Lifecycle:** {}; **highest completed ingest mode:** {}{}",
                source.lifecycle_status,
                source
                    .current_ingest_mode
                    .map_or_else(|| "none".to_string(), |mode| mode.to_string()),
                super::citation_marks(std::slice::from_ref(&root_citation)),
            ),
            vec![root_citation],
        )],
    ));

    let mut document_statements = Vec::new();
    let mut source_summary_statements = Vec::new();
    let mut excerpt_statements = Vec::new();
    for doc in docs {
        add_dependency(
            &mut dependencies,
            &doc.base.id,
            "doc",
            "source_document",
            Some(doc.base.updated_at),
        );
        let doc_citation =
            super::push_record_citation(&mut citations, &doc.base.id, CitationKind::GraphRecord);
        let revision_items = revisions
            .get(&doc.base.id)
            .map_or(&[] as &[DocRevision], Vec::as_slice);
        for revision in revision_items {
            add_dependency(
                &mut dependencies,
                &revision.base.id,
                "doc_revision",
                "source_document_revision",
                Some(revision.base.updated_at),
            );
        }
        let mut statement_citations = vec![doc_citation];
        let revision_text = revision_items.first().map_or_else(
            || "no immutable revision".to_string(),
            |revision| {
                let citation = super::push_record_citation(
                    &mut citations,
                    &revision.base.id,
                    CitationKind::GraphRecord,
                );
                statement_citations.push(citation);
                let email_summary = email_envelope_summary(&revision.text);
                if let Some((summary, _has_message_body)) = &email_summary {
                    let summary_citation = push_revision_quote_citation(
                        &mut citations,
                        &source.base.id,
                        &revision.base.id,
                        full_revision_span(revision),
                    );
                    source_summary_statements.push(evidence_synthesis_statement(
                        &format!("source-summary-{}", source_summary_statements.len() + 1),
                        summary,
                        summary_citation,
                    ));
                }
                let include_excerpt = email_summary
                    .as_ref()
                    .is_none_or(|(_summary, has_message_body)| *has_message_body);
                if include_excerpt {
                    if let Some((quote, span)) = revision_excerpt(revision) {
                        let quote_citation = push_revision_quote_citation(
                            &mut citations,
                            &source.base.id,
                            &revision.base.id,
                            span,
                        );
                        excerpt_statements.push(verbatim_quote_statement(
                            &format!("document-excerpt-{}", excerpt_statements.len() + 1),
                            &doc.title,
                            &quote,
                            quote_citation,
                        ));
                    }
                }
                format!(
                    "latest revision {}, status {}, text hash {}",
                    revision.base.id,
                    revision.status,
                    escape_markdown(&revision.text_hash),
                )
            },
        );
        document_statements.push(direct_statement(
            &format!("document-{}", document_statements.len() + 1),
            format!(
                "- **{}** - {} bytes, {} attachment(s), {}{}",
                escape_markdown(&doc.title),
                doc.text.len(),
                doc.media_attachments.len(),
                revision_text,
                super::citation_marks(&statement_citations),
            ),
            statement_citations,
        ));
    }
    if !document_statements.is_empty() {
        blocks.push(supported_block(
            "documents",
            ViewBlockKind::SourceLineage,
            "Documents and immutable revisions",
            document_statements,
        ));
    }
    if !source_summary_statements.is_empty() {
        blocks.push(supported_block(
            "source-summary",
            ViewBlockKind::Overview,
            "What this source says",
            source_summary_statements,
        ));
    }
    if !excerpt_statements.is_empty() {
        blocks.push(supported_block(
            "source-excerpts",
            ViewBlockKind::SourceLineage,
            "Source excerpts",
            excerpt_statements,
        ));
    }

    let now = Utc::now();
    let mut freshness_state = ProjectionState::Current;
    let mut unsupported_claims = 0;
    let mut expired_claims = 0;
    let mut valid_until = None;
    let freshness_statements = if let Some(watch) = watch {
        add_dependency(
            &mut dependencies,
            &watch.base.id,
            "source_watch",
            "remote_freshness_policy",
            Some(watch.base.updated_at),
        );
        let watch_citation =
            super::push_record_citation(&mut citations, &watch.base.id, CitationKind::GraphRecord);
        let mut statement_citations = vec![watch_citation];
        let latest = latest_check.map_or_else(
            || "no completed check".to_string(),
            |check| {
                add_dependency(
                    &mut dependencies,
                    &check.base.id,
                    "source_watch_check",
                    "latest_remote_observation",
                    Some(check.base.updated_at),
                );
                let citation = super::push_record_citation(
                    &mut citations,
                    &check.base.id,
                    CitationKind::GraphRecord,
                );
                statement_citations.push(citation);
                format!("latest outcome {} at {}", check.outcome, check.completed_at)
            },
        );
        valid_until = Some(watch.next_check_at);
        match watch.status {
            SourceWatchStatus::Enabled if watch.next_check_at > now => {}
            SourceWatchStatus::Enabled | SourceWatchStatus::Error => {
                freshness_state = ProjectionState::Degraded;
                expired_claims = 1;
            }
            SourceWatchStatus::Paused | SourceWatchStatus::Unsupported => {
                freshness_state = ProjectionState::Degraded;
                unsupported_claims = 1;
            }
        }
        vec![direct_statement(
            "source-watch-status",
            format!(
                "- **Remote freshness:** watch {}; {}; next check {}{}",
                watch.status,
                latest,
                watch.next_check_at,
                super::citation_marks(&statement_citations),
            ),
            statement_citations,
        )]
    } else {
        if source.type_ == SourceType::Web {
            freshness_state = ProjectionState::Degraded;
            unsupported_claims = 1;
        }
        vec![super::operational_statement(
            "source-watch-unconfigured",
            "- **Remote freshness:** no recurring observation policy is configured".to_string(),
        )]
    };
    blocks.push(supported_block(
        "source-freshness",
        ViewBlockKind::EvidenceStatus,
        "Source freshness",
        freshness_statements,
    ));

    let freshness = ProjectionFreshness {
        state: freshness_state,
        minimum_claim_score: None,
        stale_truth_slots: 0,
        unresolved_conflicts: 0,
        unsupported_claims,
        expired_claims,
        selection_truncated: false,
        valid_until,
    };
    let dependencies = dependencies.into_values().collect::<Vec<_>>();
    let validation = validate_projection(&blocks, &citations, &dependencies, 0);
    if !validation.citations_resolve
        || !validation.factual_blocks_cited
        || !validation.statements_supported
        || !validation.prose_is_structured
    {
        return Err(StorageError::Conflict(format!(
            "Source knowledge-view validation failed: {}",
            validation.warnings.join("; ")
        )));
    }
    let dependency_hash = dependency_hash(
        &dependencies,
        fingerprint,
        SOURCE_COMPILER_NAME,
        SOURCE_COMPILER_VERSION,
    )?;
    let rendered_markdown = render_page(&source_title(source, docs), &blocks, &citations);
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

fn source_citation(citations: &mut Vec<KnowledgeCitation>, source_id: &RecordId) -> String {
    let id = format!("c{}", citations.len() + 1);
    citations.push(KnowledgeCitation {
        id: id.clone(),
        kind: CitationKind::GraphRecord,
        record_id: source_id.to_string(),
        assertion_id: None,
        source_id: Some(source_id.to_string()),
        evidence_span: None,
    });
    id
}

fn source_title(source: &Source, docs: &[Doc]) -> String {
    let descriptive_external_id = source
        .external_id
        .as_deref()
        .filter(|value| value.parse::<RecordId>().is_err());
    descriptive_external_id
        .or(source.uri.as_deref())
        .or(source.path.as_deref())
        .or_else(|| docs.first().map(|doc| doc.title.as_str()))
        .or(source.external_id.as_deref())
        .map_or_else(
            || format!("Source {}", source.base.id.id().simple()),
            ToString::to_string,
        )
}

fn email_envelope_summary(text: &str) -> Option<(String, bool)> {
    let mut fields = BTreeMap::new();
    let mut has_unlabelled_content = false;
    for line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let Some((label, value)) = line.split_once(':') else {
            has_unlabelled_content = true;
            continue;
        };
        let label = label.trim();
        if matches!(
            label,
            "collection"
                | "homecore_kind"
                | "Source"
                | "Account"
                | "Subject"
                | "From"
                | "To"
                | "Date"
                | "Snippet"
        ) {
            fields.insert(label, value.trim());
        } else {
            has_unlabelled_content = true;
        }
    }
    let source = fields.get("Source")?;
    if !source.starts_with("Gmail message ") {
        return None;
    }
    let subject = fields.get("Subject").copied().unwrap_or("(no subject)");
    let sender = fields
        .get("From")
        .map_or("an unknown sender", |value| display_mailbox(value));
    let recipient = fields
        .get("To")
        .map_or("an unknown recipient", |value| display_mailbox(value));
    let date = fields.get("Date").copied().unwrap_or("an unknown date");
    let snippet = fields.get("Snippet").copied().unwrap_or("");
    let mut summary =
        format!("{sender} emailed {recipient} on {date} with the subject \"{subject}\".");
    if !snippet.is_empty() {
        let _ = write!(summary, " The captured snippet is \"{snippet}\".");
    }
    if !has_unlabelled_content {
        summary.push_str(" Elephant did not capture a message body for this source.");
    }
    Some((summary, has_unlabelled_content))
}

fn display_mailbox(value: &str) -> &str {
    value
        .split_once(" <")
        .map_or(value, |(display_name, _address)| display_name)
}

fn full_revision_span(revision: &DocRevision) -> EvidenceSpan {
    let mut hasher = Sha256::new();
    hasher.update(revision.text.as_bytes());
    EvidenceSpan {
        revision: elephant_types::Link::new(revision.base.id.clone()),
        node: None,
        start: 0,
        end: u32::try_from(revision.text.len()).unwrap_or(u32::MAX),
        quote_hash: format!("sha256:{}", hex::encode(hasher.finalize())),
    }
}

fn revision_excerpt(revision: &DocRevision) -> Option<(String, EvidenceSpan)> {
    let mut start = first_non_whitespace_offset(&revision.text)?;
    if let Some(preamble_end) = metadata_preamble_end(&revision.text[start..]) {
        start = start.checked_add(preamble_end)?;
        start = start.checked_add(first_non_whitespace_offset(&revision.text[start..])?)?;
    }
    let remaining = &revision.text[start..];
    let hard_end = remaining
        .char_indices()
        .nth(SOURCE_QUOTE_MAX_CHARS)
        .map_or(remaining.len(), |(offset, _)| offset);
    let candidate = &remaining[..hard_end];
    let natural_end = candidate
        .char_indices()
        .filter_map(|(offset, character)| {
            let end = offset + character.len_utf8();
            (end >= 100 && matches!(character, '.' | '!' | '?' | '\n')).then_some(end)
        })
        .next_back();
    let word_end = candidate
        .char_indices()
        .filter_map(|(offset, character)| {
            (offset >= 220 && character.is_whitespace()).then_some(offset)
        })
        .next_back();
    let end_in_remaining = natural_end.or(word_end).unwrap_or(hard_end);
    let quote = remaining[..end_in_remaining].trim_end();
    if quote.is_empty() {
        return None;
    }
    let end = start.checked_add(quote.len())?;
    let start = u32::try_from(start).ok()?;
    let end = u32::try_from(end).ok()?;
    let mut hasher = Sha256::new();
    hasher.update(quote.as_bytes());
    Some((
        quote.to_string(),
        EvidenceSpan {
            revision: elephant_types::Link::new(revision.base.id.clone()),
            node: None,
            start,
            end,
            quote_hash: format!("sha256:{}", hex::encode(hasher.finalize())),
        },
    ))
}

fn first_non_whitespace_offset(value: &str) -> Option<usize> {
    value
        .char_indices()
        .find_map(|(offset, character)| (!character.is_whitespace()).then_some(offset))
}

fn metadata_preamble_end(value: &str) -> Option<usize> {
    let end = value.find("\n\n")?;
    let preamble = &value[..end];
    let lines = preamble.lines().filter(|line| !line.trim().is_empty());
    let (line_count, labelled_count) = lines.fold((0_usize, 0_usize), |counts, line| {
        (
            counts.0 + 1,
            counts.1 + usize::from(line.split_once(':').is_some()),
        )
    });
    (line_count >= 3 && labelled_count.saturating_mul(2) >= line_count).then_some(end + 2)
}

fn push_revision_quote_citation(
    citations: &mut Vec<KnowledgeCitation>,
    source_id: &RecordId,
    revision_id: &RecordId,
    evidence_span: EvidenceSpan,
) -> String {
    let id = format!("c{}", citations.len() + 1);
    citations.push(KnowledgeCitation {
        id: id.clone(),
        kind: CitationKind::EvidenceSpan,
        record_id: revision_id.to_string(),
        assertion_id: None,
        source_id: Some(source_id.to_string()),
        evidence_span: Some(evidence_span),
    });
    id
}

fn verbatim_quote_statement(
    id: &str,
    document_title: &str,
    quote: &str,
    citation: String,
) -> KnowledgeStatement {
    let escaped_quote = escape_markdown(quote).replace('\n', "\n> ");
    KnowledgeStatement {
        id: id.to_string(),
        text: format!(
            "**{}:**\n\n> {}{}",
            escape_markdown(document_title),
            escaped_quote,
            super::citation_marks(std::slice::from_ref(&citation)),
        ),
        derivation: StatementDerivation::VerbatimQuote,
        verification: StatementVerification::EvidenceEntailment,
        citations: vec![citation],
    }
}

fn evidence_synthesis_statement(id: &str, text: &str, citation: String) -> KnowledgeStatement {
    KnowledgeStatement {
        id: id.to_string(),
        text: format!(
            "{}{}",
            escape_markdown(text),
            super::citation_marks(std::slice::from_ref(&citation)),
        ),
        derivation: StatementDerivation::Synthesized,
        verification: StatementVerification::EvidenceEntailment,
        citations: vec![citation],
    }
}

fn source_type_name(source: &Source) -> String {
    format!("{:?}", source.type_).to_ascii_lowercase()
}

/// Builds the deterministic identity key for a source view.
#[must_use]
pub fn source_view_key(source_id: &RecordId, security_fingerprint: &str) -> String {
    format!("source:{source_id}:security:{security_fingerprint}")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use chrono::{Duration, Utc};
    use elephant_storage::{
        DocRepository, DocRevisionRepository, SourceRepository, SourceWatchRepository,
    };
    use elephant_types::records::doc_revision::RendererMetadata;
    use elephant_types::records::{
        Doc, DocRevision, RevisionStatus, SourceType, SourceWatch, SourceWatchCheck,
        SourceWatchOutcome, SourceWatchStatus,
    };
    use elephant_types::Link;

    use super::*;

    #[tokio::test]
    #[allow(
        clippy::too_many_lines,
        reason = "the test builds and verifies the complete source revision lineage"
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Preserve original source-watch/compiler ownership through complete revision-lineage assertions."
    )]
    async fn source_view_tracks_document_revision_lineage() {
        let storage = StorageClient::new_embedded().await.expect("storage");
        crate::tests::register_space(&storage).await;
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                "space".to_string(),
                "test".to_string(),
            ),
            SourceType::Web,
            hex::encode(Sha256::digest(b"A factual source document.")),
        );
        let source_id = source.base.id.clone();
        SourceRepository::new(storage.clone())
            .create(source)
            .await
            .expect("source");

        let doc = Doc {
            base: BaseFields::new(
                RecordId::new_v4(TableName::Doc),
                "space".to_string(),
                "test".to_string(),
            ),
            title: "Engine notes".to_string(),
            text: "A factual source document.".to_string(),
            sidecar_text: None,
            source: Some(Link::new(source_id.clone())),
            media_attachments: Vec::new(),
            rendition_key: "engine-notes-v1".to_string(),
            metadata: serde_json::json!({}),
        };
        let doc_id = doc.base.id.clone();
        DocRepository::new(storage.clone())
            .create(doc)
            .await
            .expect("doc");
        let revision = DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                "space".to_string(),
                "test".to_string(),
            ),
            doc: Link::new(doc_id),
            text: "A factual source document.".to_string(),
            text_hash: hex::encode(Sha256::digest(b"A factual source document.")),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        let revision_id = revision.base.id.clone();
        DocRevisionRepository::new(storage.clone())
            .create(revision)
            .await
            .expect("revision");

        let now = Utc::now();
        let watch = SourceWatch {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SourceWatch),
                "space".to_string(),
                "test".to_string(),
            ),
            source: Link::new(source_id.clone()),
            requested_uri: "https://example.com/engine-notes".to_string(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: now + Duration::hours(1),
            last_checked_at: Some(now),
            last_changed_at: None,
            etag: Some("\"v1\"".to_string()),
            last_modified: None,
            observed_hash: Some(hex::encode(Sha256::digest(b"A factual source document."))),
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        };
        let watch_repository = SourceWatchRepository::new(storage.clone());
        let watch = watch_repository.create(watch).await.expect("watch");
        let mut check_base = watch.base.clone();
        check_base.id = RecordId::new_v4(TableName::SourceWatchCheck);
        let check = SourceWatchCheck {
            base: check_base,
            watch: Link::new(watch.base.id.clone()),
            source: Link::new(source_id.clone()),
            outcome: SourceWatchOutcome::NotModified,
            started_at: now,
            completed_at: now,
            requested_uri: watch.requested_uri.clone(),
            final_uri: Some(watch.requested_uri.clone()),
            content_type: None,
            etag: watch.etag.clone(),
            last_modified: None,
            prior_hash: watch.observed_hash.clone(),
            observed_hash: watch.observed_hash.clone(),
            content_length: None,
            error: None,
        };
        let (watch, check) = watch_repository
            .record_check(watch, check)
            .await
            .expect("record check");

        let compiler = SourceViewCompiler::new(storage);
        let result = compiler
            .compile("space", &source_id, SourceViewLimits::default(), false)
            .await
            .expect("compile source view");

        assert_eq!(result.view.kind, KnowledgeViewKind::Source);
        assert_eq!(result.view.title, "Engine notes");
        assert!(result.revision.validation.statements_supported);
        assert!(result
            .revision
            .rendered_markdown
            .contains("> A factual source document."));
        assert!(result.revision.citations.iter().any(|citation| {
            citation.kind == CitationKind::EvidenceSpan
                && citation.evidence_span.as_ref().is_some_and(|span| {
                    span.revision.id() == &revision_id && span.start == 0 && span.end == 26
                })
        }));
        assert!(result
            .revision
            .dependencies
            .iter()
            .any(|dependency| dependency.record_id == revision_id.to_string()));
        assert_eq!(result.view.state, ProjectionState::Current);
        assert!(result.revision.dependencies.iter().any(|dependency| {
            dependency.record_id == watch.base.id.to_string()
                && dependency.record_type == "source_watch"
        }));
        assert!(result.revision.dependencies.iter().any(|dependency| {
            dependency.record_id == check.base.id.to_string()
                && dependency.record_type == "source_watch_check"
                && dependency.observed_updated_at == Some(check.base.updated_at)
        }));
        assert!(result
            .revision
            .blocks
            .iter()
            .any(|block| block.kind == ViewBlockKind::SourceLineage));
    }

    #[test]
    fn revision_excerpt_skips_labelled_ingestion_preamble() {
        let mut revision = DocRevision {
            base: BaseFields::new(
                RecordId::new_v4(TableName::DocRevision),
                "space".to_string(),
                "test".to_string(),
            ),
            doc: Link::new(RecordId::new_v4(TableName::Doc)),
            text: "collection: google_email\nSource: Gmail message 123\nSubject: Planning\n\nThe actual message body has useful evidence. Another sentence follows."
                .to_string(),
            text_hash: "sha256:revision".to_string(),
            renderer: RendererMetadata {
                name: "test".to_string(),
                version: "1".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };
        let revision_id = revision.base.id.clone();

        let (quote, span) = revision_excerpt(&revision).expect("excerpt");

        assert_eq!(
            quote,
            "The actual message body has useful evidence. Another sentence follows."
        );
        assert_eq!(
            &revision.text[span.start as usize..span.end as usize],
            quote
        );
        assert_eq!(span.revision.id(), &revision_id);

        revision.text = "Only document content.".to_string();
        let (quote, span) = revision_excerpt(&revision).expect("plain excerpt");
        assert_eq!(quote, "Only document content.");
        assert_eq!(span.start, 0);
        assert_eq!(span.end, 22);
    }

    #[test]
    fn envelope_only_email_summary_states_what_was_and_was_not_captured() {
        let (summary, has_message_body) = email_envelope_summary(
            "collection: google_email\n\
             homecore_kind: source_event\n\
             Source: Gmail message synthetic-message-001\n\
             Account: noah@example.com\n\
             Subject: Meeting\n\
             From: Alice Example <alice@example.com>\n\
             To: Noah Example <noah@example.com>\n\
             Date: Mon, 5 Jan 2026 10:00:00 +0000\n\
             Snippet: notes",
        )
        .expect("email summary");

        assert_eq!(
            summary,
            "Alice Example emailed Noah Example on Mon, 5 Jan 2026 10:00:00 +0000 with the subject \"Meeting\". The captured snippet is \"notes\". Elephant did not capture a message body for this source."
        );
        assert!(!has_message_body);
    }
}
