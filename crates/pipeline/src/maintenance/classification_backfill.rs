//! Backfills the source document's classification onto records that were
//! derived before inheritance existed.
//!
//! Inline promotion stamps the parent doc's security envelope onto every record
//! it derives, but records created before that landed carry the
//! `BaseFields::new` baseline (`policy_id: "default"`, `level: 0`, no labels, no
//! handling) regardless of how their source was classified. That is not a
//! cosmetic gap: assertions extracted from an owner-only, `no_voice` document
//! are indistinguishable from household-open facts, so any access filtering
//! built on the envelope would pass them all through.
//!
//! Re-deriving does NOT fix this. Promotion is idempotent on `assertion_key`:
//! an existing assertion short-circuits into the entity-enrichment path and its
//! security is never revisited. So the repair has to be a direct pass, which is
//! also far cheaper than re-running extraction.
//!
//! Reaching the source document differs per record type:
//! - assertion: `evidence.revision` -> revision's doc
//! - attribute / rel: every `evidence[].span.revision` -> revision's doc
//! - event: every `assertion_refs[]` -> that assertion's revision -> doc
//!
//! Records backed by several documents escalate across all of them (max level,
//! unioned labels and handling), so a rel supported by both a household note and
//! a sealed legal document ends up at the sealed document's level. This reuses
//! the same helpers as the write path so the two cannot drift apart.

use std::collections::HashMap;

use elephant_storage::{
    AssertionRepository, AttributeRepository, DocRepository, DocRevisionRepository,
    EventRepository, RelRepository, Repository, StorageClient,
};
use elephant_types::records::base::Security;
use elephant_types::RecordId;
use tracing::{info, warn};

use crate::error::PipelineError;
use crate::scheduler::inline_promotion::escalate_security;

/// How many records of each kind a single pass will read before stopping.
///
/// The pass is bounded so it cannot starve the sweeps that run after it. Hitting
/// the cap is logged rather than passed over in silence — a truncated repair that
/// reports success reads as "everything is classified" when it is not.
const DEFAULT_SCAN_CAP: usize = 20_000;

/// Page size for walking a table.
const PAGE_SIZE: usize = 200;

/// Outcome of one backfill pass.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClassificationBackfillStats {
    /// Records read across all four kinds.
    pub scanned: usize,
    /// Records whose envelope was rewritten.
    pub updated: usize,
    /// Records still on the baseline whose source classification could not be
    /// resolved, or whose source is itself unclassified. Left untouched.
    pub unresolved: usize,
    /// True if any kind stopped at the scan cap with rows left to read.
    pub capped: bool,
}

impl ClassificationBackfillStats {
    fn merge(&mut self, other: &Self) {
        self.scanned += other.scanned;
        self.updated += other.updated;
        self.unresolved += other.unresolved;
        self.capped |= other.capped;
    }
}

/// True when a record still carries the untouched `BaseFields::new` envelope.
///
/// Anything else has been classified deliberately and must not be overwritten —
/// this pass only repairs records that never received an envelope at all.
fn is_baseline(security: &Security) -> bool {
    security.policy_id == "default"
        && security.level == 0
        && security.labels.is_empty()
        && security.handling.is_empty()
}

/// Resolves and caches the classification of the document behind a revision.
///
/// Backfilling a whole graph re-visits the same handful of documents constantly,
/// so revisions are cached including their misses.
struct DocSecurityResolver {
    revision_repo: DocRevisionRepository,
    doc_repo: DocRepository,
    space_id: String,
    cache: HashMap<String, Option<Security>>,
}

impl DocSecurityResolver {
    fn new(storage: &StorageClient, space_id: &str) -> Self {
        Self {
            revision_repo: DocRevisionRepository::new(storage.clone()),
            doc_repo: DocRepository::new(storage.clone()),
            space_id: space_id.to_string(),
            cache: HashMap::new(),
        }
    }

    /// Returns the source doc's envelope, or `None` when it cannot be resolved
    /// or is itself the baseline (nothing worth propagating).
    async fn security_for_revision(
        &mut self,
        revision_id: &RecordId,
    ) -> Result<Option<Security>, PipelineError> {
        let key = revision_id.to_string();
        if let Some(hit) = self.cache.get(&key) {
            return Ok(hit.clone());
        }

        let resolved = self.load_security(revision_id).await?;
        self.cache.insert(key, resolved.clone());
        Ok(resolved)
    }

    async fn load_security(
        &self,
        revision_id: &RecordId,
    ) -> Result<Option<Security>, PipelineError> {
        let Some(revision) = self
            .revision_repo
            .get_by_id(revision_id)
            .await
            .map_err(PipelineError::StorageError)?
        else {
            return Ok(None);
        };

        let Some(doc) = self
            .doc_repo
            .get(&self.space_id, revision.doc.id())
            .await
            .map_err(PipelineError::StorageError)?
        else {
            return Ok(None);
        };

        if is_baseline(&doc.base.security) {
            return Ok(None);
        }
        Ok(Some(doc.base.security.clone()))
    }
}

/// Folds every contributing document's classification into one envelope.
///
/// Returns `None` when no contributing document had anything to propagate, so
/// callers can leave the record alone rather than writing back a no-op.
fn combine(sources: &[Security]) -> Option<Security> {
    let mut combined: Option<Security> = None;
    for source in sources {
        match combined.as_mut() {
            None => combined = Some(source.clone()),
            Some(existing) => escalate_security(existing, Some(source)),
        }
    }
    combined
}

/// Repairs derived records that never received their source's classification.
///
/// Only records still on the baseline envelope are considered, and a record is
/// written only when the resolved envelope actually differs, so a pass over an
/// already-repaired graph performs reads and no writes.
///
/// # Errors
///
/// Returns an error if a table cannot be read or a record cannot be written.
pub async fn run_classification_backfill_sweep(
    storage: &StorageClient,
    space_id: &str,
) -> Result<ClassificationBackfillStats, PipelineError> {
    run_classification_backfill_sweep_with_cap(storage, space_id, DEFAULT_SCAN_CAP).await
}

/// As [`run_classification_backfill_sweep`], with an explicit scan cap for tests.
///
/// # Errors
///
/// Returns an error if a table cannot be read or a record cannot be written.
pub async fn run_classification_backfill_sweep_with_cap(
    storage: &StorageClient,
    space_id: &str,
    scan_cap: usize,
) -> Result<ClassificationBackfillStats, PipelineError> {
    let mut resolver = DocSecurityResolver::new(storage, space_id);
    let mut stats = ClassificationBackfillStats::default();

    let assertions = backfill_assertions(storage, space_id, &mut resolver, scan_cap).await?;
    stats.merge(&assertions);

    let attributes = backfill_attributes(storage, space_id, &mut resolver, scan_cap).await?;
    stats.merge(&attributes);

    let rels = backfill_rels(storage, space_id, &mut resolver, scan_cap).await?;
    stats.merge(&rels);

    let events = backfill_events(storage, space_id, &mut resolver, scan_cap).await?;
    stats.merge(&events);

    if stats.capped {
        warn!(
            "Classification backfill stopped at the {scan_cap}-record scan cap; \
             records remain unclassified and the next sweep will continue"
        );
    }
    if stats.updated > 0 || stats.unresolved > 0 {
        info!(
            "Classification backfill: {} scanned, {} reclassified, {} unresolved",
            stats.scanned, stats.updated, stats.unresolved
        );
    }

    Ok(stats)
}

async fn backfill_assertions(
    storage: &StorageClient,
    space_id: &str,
    resolver: &mut DocSecurityResolver,
    scan_cap: usize,
) -> Result<ClassificationBackfillStats, PipelineError> {
    let repo = AssertionRepository::new(storage.clone());
    let mut stats = ClassificationBackfillStats::default();
    let mut cursor: Option<RecordId> = None;

    loop {
        let page = repo
            .list(space_id, PAGE_SIZE, cursor.as_ref())
            .await
            .map_err(PipelineError::StorageError)?;
        if page.is_empty() {
            break;
        }

        for mut assertion in page.clone() {
            stats.scanned += 1;
            if !is_baseline(&assertion.base.security) {
                continue;
            }

            let source = resolver
                .security_for_revision(assertion.evidence.revision.id())
                .await?;
            let Some(security) = source else {
                stats.unresolved += 1;
                continue;
            };

            assertion.base.security = security;
            let id = assertion.base.id.clone();
            repo.update(space_id, &id, assertion)
                .await
                .map_err(PipelineError::StorageError)?;
            stats.updated += 1;
        }

        if page.len() < PAGE_SIZE {
            break;
        }
        if stats.scanned >= scan_cap {
            stats.capped = true;
            break;
        }
        cursor = page.last().map(|last| last.base.id.clone());
    }

    Ok(stats)
}

async fn backfill_attributes(
    storage: &StorageClient,
    space_id: &str,
    resolver: &mut DocSecurityResolver,
    scan_cap: usize,
) -> Result<ClassificationBackfillStats, PipelineError> {
    let repo = AttributeRepository::new(storage.clone());
    let mut stats = ClassificationBackfillStats::default();
    let mut cursor: Option<RecordId> = None;

    loop {
        let page = repo
            .list(space_id, PAGE_SIZE, cursor.as_ref())
            .await
            .map_err(PipelineError::StorageError)?;
        if page.is_empty() {
            break;
        }

        for mut attribute in page.clone() {
            stats.scanned += 1;
            if !is_baseline(&attribute.base.security) {
                continue;
            }

            let mut sources = Vec::new();
            for evidence in &attribute.evidence {
                if let Some(security) = resolver
                    .security_for_revision(evidence.span.revision.id())
                    .await?
                {
                    sources.push(security);
                }
            }

            let Some(security) = combine(&sources) else {
                stats.unresolved += 1;
                continue;
            };

            attribute.base.security = security;
            let id = attribute.base.id.clone();
            repo.update(space_id, &id, attribute)
                .await
                .map_err(PipelineError::StorageError)?;
            stats.updated += 1;
        }

        if page.len() < PAGE_SIZE {
            break;
        }
        if stats.scanned >= scan_cap {
            stats.capped = true;
            break;
        }
        cursor = page.last().map(|last| last.base.id.clone());
    }

    Ok(stats)
}

async fn backfill_rels(
    storage: &StorageClient,
    space_id: &str,
    resolver: &mut DocSecurityResolver,
    scan_cap: usize,
) -> Result<ClassificationBackfillStats, PipelineError> {
    let repo = RelRepository::new(storage.clone());
    let mut stats = ClassificationBackfillStats::default();
    let mut cursor: Option<RecordId> = None;

    loop {
        let page = repo
            .list(space_id, PAGE_SIZE, cursor.as_ref())
            .await
            .map_err(PipelineError::StorageError)?;
        if page.is_empty() {
            break;
        }

        for mut rel in page.clone() {
            stats.scanned += 1;
            if !is_baseline(&rel.base.security) {
                continue;
            }

            let mut sources = Vec::new();
            for evidence in &rel.evidence {
                if let Some(security) = resolver
                    .security_for_revision(evidence.span.revision.id())
                    .await?
                {
                    sources.push(security);
                }
            }

            let Some(security) = combine(&sources) else {
                stats.unresolved += 1;
                continue;
            };

            rel.base.security = security;
            let id = rel.base.id.clone();
            repo.update(space_id, &id, rel)
                .await
                .map_err(PipelineError::StorageError)?;
            stats.updated += 1;
        }

        if page.len() < PAGE_SIZE {
            break;
        }
        if stats.scanned >= scan_cap {
            stats.capped = true;
            break;
        }
        cursor = page.last().map(|last| last.base.id.clone());
    }

    Ok(stats)
}

async fn backfill_events(
    storage: &StorageClient,
    space_id: &str,
    resolver: &mut DocSecurityResolver,
    scan_cap: usize,
) -> Result<ClassificationBackfillStats, PipelineError> {
    let repo = EventRepository::new(storage.clone());
    let assertion_repo = AssertionRepository::new(storage.clone());
    let mut stats = ClassificationBackfillStats::default();
    let mut cursor: Option<RecordId> = None;

    loop {
        let page = repo
            .list(space_id, PAGE_SIZE, cursor.as_ref())
            .await
            .map_err(PipelineError::StorageError)?;
        if page.is_empty() {
            break;
        }

        for mut event in page.clone() {
            stats.scanned += 1;
            if !is_baseline(&event.base.security) {
                continue;
            }

            // Events reach their document one hop further out than the other
            // kinds: through the assertions that evidence them.
            let mut sources = Vec::new();
            for assertion_ref in &event.assertion_refs {
                let assertion = assertion_repo
                    .get(space_id, assertion_ref.id())
                    .await
                    .map_err(PipelineError::StorageError)?;
                if let Some(assertion) = assertion {
                    if let Some(security) = resolver
                        .security_for_revision(assertion.evidence.revision.id())
                        .await?
                    {
                        sources.push(security);
                    }
                }
            }

            let Some(security) = combine(&sources) else {
                stats.unresolved += 1;
                continue;
            };

            event.base.security = security;
            let id = event.base.id.clone();
            repo.update(space_id, &id, event)
                .await
                .map_err(PipelineError::StorageError)?;
            stats.updated += 1;
        }

        if page.len() < PAGE_SIZE {
            break;
        }
        if stats.scanned >= scan_cap {
            stats.capped = true;
            break;
        }
        cursor = page.last().map(|last| last.base.id.clone());
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chrono::Utc;
    use elephant_types::ids::TableName;
    use elephant_types::records::assertion::{
        Assertion, AssertionRetryState, EvidenceSpan, ResolutionStatus,
    };
    use elephant_types::records::attribute::{Attribute, Literal};
    use elephant_types::records::base::{BaseFields, SubjectsState};
    use elephant_types::records::doc::Doc;
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    use elephant_types::records::event::Event;
    use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel, TimeBasis};
    use elephant_types::Link;

    const SPACE: &str = "test_space";

    const SCHEMA: &str = r"
        DEFINE TABLE assertion SCHEMALESS;
        DEFINE TABLE rel SCHEMALESS;
        DEFINE TABLE attribute SCHEMALESS;
        DEFINE TABLE event SCHEMALESS;
        DEFINE TABLE entity SCHEMALESS;
        DEFINE TABLE doc SCHEMALESS;
        DEFINE TABLE doc_revision SCHEMALESS;
    ";

    fn base_with(table: TableName, security: Security) -> BaseFields {
        let mut base = BaseFields::new(
            RecordId::new_v4(table),
            SPACE.to_string(),
            "test".to_string(),
        );
        base.security = security;
        base.subjects_state = SubjectsState::Complete;
        base
    }

    /// Creates a doc carrying `security` plus a revision pointing at it.
    async fn seed_doc(storage: &StorageClient, security: Security) -> RecordId {
        let doc_id = RecordId::new_v4(TableName::Doc);
        let mut doc_base = BaseFields::new(doc_id.clone(), SPACE.to_string(), "test".to_string());
        doc_base.security = security;
        DocRepository::new(storage.clone())
            .create(Doc {
                base: doc_base,
                title: "Seeded Doc".to_string(),
                text: "text".to_string(),
                sidecar_text: None,
                source: None,
                media_attachments: vec![],
                rendition_key: format!("rendition-{doc_id}"),
                metadata: serde_json::json!({}),
            })
            .await
            .expect("create doc");

        let revision_id = RecordId::new_v4(TableName::DocRevision);
        DocRevisionRepository::new(storage.clone())
            .create(DocRevision {
                base: BaseFields::new(revision_id.clone(), SPACE.to_string(), "test".to_string()),
                doc: Link::new(doc_id),
                text: "text".to_string(),
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
            })
            .await
            .expect("create revision");

        revision_id
    }

    fn evidence_span(revision_id: &RecordId) -> EvidenceSpan {
        EvidenceSpan {
            revision: Link::new(revision_id.clone()),
            node: None,
            start: 0,
            end: 10,
            quote_hash: "hash".to_string(),
        }
    }

    /// Creates an unclassified assertion evidenced by `revision_id`, as records
    /// derived before inheritance existed look on disk.
    async fn seed_unclassified_assertion(
        storage: &StorageClient,
        revision_id: &RecordId,
        key: &str,
    ) -> RecordId {
        let base = base_with(TableName::Assertion, baseline());
        let id = base.id.clone();
        let now = Utc::now();
        AssertionRepository::new(storage.clone())
            .create(Assertion {
                base,
                subject_text: "Subject".to_string(),
                predicate_raw: "has_value".to_string(),
                predicate_id: Some("has_value".to_string()),
                object_text: None,
                object_literal: Some(serde_json::json!("value")),
                evidence: evidence_span(revision_id),
                source_event_time: None,
                observed_at: now,
                asserted_at: now,
                assertion_key: key.to_string(),
                resolution_status: ResolutionStatus::Resolved,
                resolution_method: Some("inline_promotion".to_string()),
                resolution_confidence: Some(0.9),
                retry_state: AssertionRetryState::default(),
                subject_entity: None,
                object_entity: None,
                confidence: 0.9,
                subject_entity_type: None,
                object_entity_type: None,
                significance: Some("high".to_string()),
                lifecycle_tier: "active".to_string(),
            })
            .await
            .expect("create assertion");
        id
    }

    fn evidence_ref(assertion_id: &RecordId, revision_id: &RecordId) -> EvidenceRef {
        EvidenceRef {
            assertion: Link::new(assertion_id.clone()),
            span: evidence_span(revision_id),
            time_basis: TimeBasis::ObservedAt,
            time_value: Utc::now(),
        }
    }

    /// Every derived kind, unclassified, all tracing back to one classified doc.
    #[tokio::test]
    async fn backfill_reclassifies_every_derived_kind() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let revision_id = seed_doc(&storage, owner_only()).await;
        let assertion_id = seed_unclassified_assertion(&storage, &revision_id, "key-1").await;

        let subject = Link::new(RecordId::new_v4(TableName::Entity));
        let object = Link::new(RecordId::new_v4(TableName::Entity));

        let attr_base = base_with(TableName::Attribute, baseline());
        let attr_id = attr_base.id.clone();
        AttributeRepository::new(storage.clone())
            .create(Attribute {
                base: attr_base,
                subject: subject.clone(),
                attr_type: "has_value".to_string(),
                value: Literal::String("value".to_string()),
                status: ClaimStatus::Candidate,
                confidence: 0.9,
                validity_window: None,
                evidence: vec![evidence_ref(&assertion_id, &revision_id)],
                temporal_audit: None,
            })
            .await
            .expect("create attribute");

        let rel_base = base_with(TableName::Rel, baseline());
        let rel_id = rel_base.id.clone();
        RelRepository::new(storage.clone())
            .create(Rel {
                base: rel_base,
                subject: subject.clone(),
                object,
                rel_type: "relates_to".to_string(),
                status: ClaimStatus::Candidate,
                confidence: 0.9,
                validity_window: None,
                evidence: vec![evidence_ref(&assertion_id, &revision_id)],
                temporal_audit: None,
            })
            .await
            .expect("create rel");

        let event_base = base_with(TableName::Event, baseline());
        let event_id = event_base.id.clone();
        EventRepository::new(storage.clone())
            .create(
                Event::new(
                    event_base,
                    "Seeded event",
                    vec![Link::new(assertion_id.clone())],
                    vec![],
                )
                .expect("event invariants"),
            )
            .await
            .expect("create event");

        let stats = run_classification_backfill_sweep(&storage, SPACE)
            .await
            .expect("backfill");

        assert_eq!(stats.updated, 4, "all four kinds should be repaired");
        assert_eq!(stats.unresolved, 0);
        assert!(!stats.capped);

        let expected = owner_only();
        let assertion = AssertionRepository::new(storage.clone())
            .get(SPACE, &assertion_id)
            .await
            .expect("get assertion")
            .expect("assertion exists");
        assert_eq!(assertion.base.security, expected);

        let attribute = AttributeRepository::new(storage.clone())
            .get(SPACE, &attr_id)
            .await
            .expect("get attribute")
            .expect("attribute exists");
        assert_eq!(attribute.base.security, expected);

        let rel = RelRepository::new(storage.clone())
            .get(SPACE, &rel_id)
            .await
            .expect("get rel")
            .expect("rel exists");
        assert_eq!(rel.base.security, expected);

        let event = EventRepository::new(storage.clone())
            .get(SPACE, &event_id)
            .await
            .expect("get event")
            .expect("event exists");
        assert_eq!(event.base.security, expected);
    }

    /// The pass must converge: a second run writes nothing. Without this the
    /// sweep would rewrite the same rows every 15 minutes forever.
    #[tokio::test]
    async fn backfill_is_idempotent() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let revision_id = seed_doc(&storage, owner_only()).await;
        seed_unclassified_assertion(&storage, &revision_id, "key-1").await;

        let first = run_classification_backfill_sweep(&storage, SPACE)
            .await
            .expect("first pass");
        assert_eq!(first.updated, 1);

        let second = run_classification_backfill_sweep(&storage, SPACE)
            .await
            .expect("second pass");
        assert_eq!(second.updated, 0, "already-classified rows must be skipped");
        assert_eq!(second.scanned, 1, "but they are still read");
    }

    /// A record whose source document is itself unclassified has nothing to
    /// inherit. It must be reported, not silently counted as repaired, and not
    /// written to — otherwise it would churn on every sweep.
    #[tokio::test]
    async fn unclassified_source_leaves_record_untouched() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let revision_id = seed_doc(&storage, baseline()).await;
        let assertion_id = seed_unclassified_assertion(&storage, &revision_id, "key-1").await;

        let stats = run_classification_backfill_sweep(&storage, SPACE)
            .await
            .expect("backfill");
        assert_eq!(stats.updated, 0);
        assert_eq!(stats.unresolved, 1);

        let assertion = AssertionRepository::new(storage.clone())
            .get(SPACE, &assertion_id)
            .await
            .expect("get assertion")
            .expect("assertion exists");
        assert!(is_baseline(&assertion.base.security));
    }

    /// A dangling evidence link must not abort the pass or invent a
    /// classification for the record.
    #[tokio::test]
    async fn missing_source_document_is_reported_not_guessed() {
        let storage = StorageClient::new_memory().await.expect("storage");
        storage.query(SCHEMA).await.expect("schema");

        let orphan_revision = RecordId::new_v4(TableName::DocRevision);
        let assertion_id = seed_unclassified_assertion(&storage, &orphan_revision, "key-1").await;

        let stats = run_classification_backfill_sweep(&storage, SPACE)
            .await
            .expect("backfill should not fail on a dangling link");
        assert_eq!(stats.updated, 0);
        assert_eq!(stats.unresolved, 1);

        let assertion = AssertionRepository::new(storage.clone())
            .get(SPACE, &assertion_id)
            .await
            .expect("get assertion")
            .expect("assertion exists");
        assert!(is_baseline(&assertion.base.security));
    }

    fn baseline() -> Security {
        Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        }
    }

    fn owner_only() -> Security {
        Security {
            policy_id: "household-v1".to_string(),
            level: 2,
            labels: vec!["audience:owner".to_string()],
            handling: vec!["no_voice".to_string()],
        }
    }

    #[test]
    fn baseline_envelope_is_recognised() {
        assert!(is_baseline(&baseline()));
    }

    #[test]
    fn deliberate_classification_is_not_treated_as_baseline() {
        assert!(!is_baseline(&owner_only()));

        // Each field alone is enough to disqualify a record from repair, so a
        // partially classified record is never overwritten.
        let mut level_only = baseline();
        level_only.level = 1;
        assert!(!is_baseline(&level_only));

        let mut labels_only = baseline();
        labels_only.labels = vec!["topic:work".to_string()];
        assert!(!is_baseline(&labels_only));

        let mut handling_only = baseline();
        handling_only.handling = vec!["no_voice".to_string()];
        assert!(!is_baseline(&handling_only));

        let mut policy_only = baseline();
        policy_only.policy_id = "household-v1".to_string();
        assert!(!is_baseline(&policy_only));
    }

    #[test]
    fn combine_returns_none_when_nothing_to_propagate() {
        assert_eq!(combine(&[]), None);
    }

    #[test]
    fn combine_escalates_across_contributing_documents() {
        let open = Security {
            policy_id: "household-v1".to_string(),
            level: 0,
            labels: vec!["topic:work".to_string()],
            handling: vec![],
        };

        // Order must not matter: the sealed document wins either way.
        let sealed_last = combine(&[open.clone(), owner_only()]).expect("combined");
        let sealed_first = combine(&[owner_only(), open]).expect("combined");
        assert_eq!(sealed_last, sealed_first);

        assert_eq!(sealed_last.level, 2);
        assert_eq!(sealed_last.handling, vec!["no_voice".to_string()]);
        assert_eq!(
            sealed_last.labels,
            vec!["audience:owner".to_string(), "topic:work".to_string()]
        );
    }

    #[test]
    fn backfill_is_off_unless_explicitly_enabled() {
        // Guards a deployment hazard: a wholesale-rewrite pass that defaulted on
        // would mutate the graph during a candidate boot and fail certification.
        use crate::maintenance::backfill_flag_is_on;

        assert!(!backfill_flag_is_on(None), "absent flag must mean off");
        for off in ["", "false", "0", "no", "yes", "TRUE", "True"] {
            assert!(!backfill_flag_is_on(Some(off)), "{off:?} must mean off");
        }
        for on in ["true", "1"] {
            assert!(backfill_flag_is_on(Some(on)), "{on:?} must mean on");
        }
    }
}
