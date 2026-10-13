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
//! security is never revisited. This opt-in repair prepares exact document
//! dependencies and classification changes in the sweep's canonical transaction;
//! it does not re-run extraction or publish another source generation.
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
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::Doc;
use elephant_types::RecordId;
use tracing::{info, warn};

use crate::error::PipelineResult;
use crate::maintenance_commit::{PlanRecord, PlanningTruthMaintenanceStorage};
use crate::scheduler::inline_promotion::escalate_security;

const DEFAULT_SCAN_CAP: usize = 20_000;
const PAGE_SIZE: usize = 200;

/// Outcome of one bounded classification repair.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ClassificationBackfillStats {
    /// Records examined across all four kinds, including already classified rows.
    pub scanned: usize,
    /// Records whose full inherited envelope was prepared for replacement.
    pub updated: usize,
    /// Baseline records with missing or incompatible document authority.
    pub unresolved: usize,
    /// At least one table exceeded the explicit scan bound.
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

fn is_baseline(security: &Security) -> bool {
    security.policy_id == "default"
        && security.level == 0
        && security.labels.is_empty()
        && security.handling.is_empty()
}

/// Every actual revision and document joins the original read set, including
/// missing-reference decisions protected by the outer predicate epoch.
struct DocAuthorityResolver<'a> {
    planning: &'a PlanningTruthMaintenanceStorage,
    revision_repo: DocRevisionRepository,
    doc_repo: DocRepository,
    space_id: String,
    cache: HashMap<RecordId, Option<Doc>>,
}

impl<'a> DocAuthorityResolver<'a> {
    fn new(
        planning: &'a PlanningTruthMaintenanceStorage,
        storage: &StorageClient,
        space: &str,
    ) -> Self {
        Self {
            planning,
            revision_repo: DocRevisionRepository::new(storage.clone()),
            doc_repo: DocRepository::new(storage.clone()),
            space_id: space.into(),
            cache: HashMap::new(),
        }
    }

    async fn document(&mut self, revision_id: &RecordId) -> PipelineResult<Option<Doc>> {
        if let Some(cached) = self.cache.get(revision_id) {
            return Ok(cached.clone());
        }
        let revision = self.revision_repo.get(&self.space_id, revision_id).await?;
        let revision = self.planning.current_row(revision, revision_id, false)?;
        let doc = if let Some(revision) = revision {
            let live = self.doc_repo.get(&self.space_id, revision.doc.id()).await?;
            self.planning.current_row(live, revision.doc.id(), false)?
        } else {
            None
        };
        self.cache.insert(revision_id.clone(), doc.clone());
        Ok(doc)
    }

    async fn documents(&mut self, revisions: &[RecordId]) -> PipelineResult<Option<Vec<Doc>>> {
        if revisions.is_empty() {
            return Ok(None);
        }
        let mut docs = Vec::new();
        for revision in revisions {
            let Some(doc) = self.document(revision).await? else {
                return Ok(None);
            };
            if !docs
                .iter()
                .any(|existing: &Doc| existing.base.id == doc.base.id)
            {
                docs.push(doc);
            }
        }
        Ok(Some(docs))
    }
}

/// Opaque policy identifiers cannot be compared or silently substituted.
fn combine(sources: &[Security]) -> PipelineResult<Option<Security>> {
    let Some(first) = sources.first() else {
        return Ok(None);
    };
    let mut combined = first.clone();
    for source in &sources[1..] {
        escalate_security(&mut combined, Some(source))?;
    }
    combined.labels.sort();
    combined.labels.dedup();
    combined.handling.sort();
    combined.handling.dedup();
    Ok(Some(combined))
}

fn inherit_documents(base: &mut BaseFields, docs: &[Doc]) -> PipelineResult<bool> {
    let Some(security) = combine(
        &docs
            .iter()
            .map(|doc| doc.base.security.clone())
            .collect::<Vec<_>>(),
    )?
    else {
        return Ok(false);
    };
    if is_baseline(&security) {
        return Ok(false);
    }
    base.security = security;
    for doc in docs {
        base.subjects.extend(doc.base.subjects.iter().cloned());
        base.subjects_state = match (base.subjects_state, doc.base.subjects_state) {
            (SubjectsState::Unknown, _) | (_, SubjectsState::Unknown) => SubjectsState::Unknown,
            (SubjectsState::Partial, _) | (_, SubjectsState::Partial) => SubjectsState::Partial,
            _ => SubjectsState::Complete,
        };
    }
    base.subjects.sort_by(|a, b| a.id().cmp(b.id()));
    base.subjects.dedup();
    base.updated_at = chrono::Utc::now();
    Ok(true)
}

/// Repair baseline historical derived rows in one canonical transaction.
/// Opt-in scheduling remains the maintenance wrapper's responsibility.
///
/// # Errors
/// Returns an error for unavailable storage, stale dependencies, or invalid
/// source/attempt authority. A failure commits no partial classifications.
pub async fn run_classification_backfill_sweep(
    storage: &StorageClient,
    space_id: &str,
) -> PipelineResult<ClassificationBackfillStats> {
    run_classification_backfill_sweep_with_cap(storage, space_id, DEFAULT_SCAN_CAP).await
}

/// Run the same atomic repair with an explicit per-table scan bound.
///
/// # Errors
/// Returns an error if preparation or the canonical commit is rejected.
pub async fn run_classification_backfill_sweep_with_cap(
    storage: &StorageClient,
    space_id: &str,
    scan_cap: usize,
) -> PipelineResult<ClassificationBackfillStats> {
    let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space_id, None).await?;
    let result = prepare_with_cap(&planning, storage, space_id, scan_cap).await?;
    planning
        .commit(
            "classification_backfill",
            space_id,
            &serde_json::json!({"scan_cap":scan_cap}),
            &result,
        )
        .await?;
    Ok(result)
}

/// Prepare the opt-in repair into the caller's original sweep/attempt plan.
pub(crate) async fn prepare_classification_backfill_sweep(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space_id: &str,
) -> PipelineResult<ClassificationBackfillStats> {
    prepare_with_cap(planning, storage, space_id, DEFAULT_SCAN_CAP).await
}

async fn prepare_with_cap(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space_id: &str,
    scan_cap: usize,
) -> PipelineResult<ClassificationBackfillStats> {
    let mut resolver = DocAuthorityResolver::new(planning, storage, space_id);
    let mut stats = ClassificationBackfillStats::default();
    stats.merge(&backfill_assertions(planning, storage, space_id, &mut resolver, scan_cap).await?);
    stats.merge(&backfill_attributes(planning, storage, space_id, &mut resolver, scan_cap).await?);
    stats.merge(&backfill_rels(planning, storage, space_id, &mut resolver, scan_cap).await?);
    stats.merge(&backfill_events(planning, storage, space_id, &mut resolver, scan_cap).await?);
    if stats.capped {
        warn!(
            scan_cap,
            "Classification repair remains incomplete at the configured scan bound"
        );
    }
    if stats.updated > 0 || stats.unresolved > 0 {
        info!(
            scanned = stats.scanned,
            prepared = stats.updated,
            unresolved = stats.unresolved,
            "Prepared classification repair"
        );
    }
    Ok(stats)
}

async fn rows<T, R>(
    planning: &PlanningTruthMaintenanceStorage,
    repo: &R,
    space: &str,
    cap: usize,
) -> PipelineResult<(Vec<T>, bool)>
where
    T: PlanRecord + serde::de::DeserializeOwned,
    R: Repository<T> + Sync,
{
    let mut live = Vec::new();
    let mut cursor = None;
    let bound = cap.saturating_add(1);
    while live.len() < bound {
        let limit = PAGE_SIZE.min(bound - live.len());
        let page = repo.list(space, limit, cursor.as_ref()).await?;
        let count = page.len();
        if let Some(last) = page.last() {
            cursor = Some(last.base().id.clone());
        }
        live.extend(page);
        if count < limit {
            break;
        }
    }
    let mut rows = planning.current_rows(live, |_| true)?;
    let capped = rows.len() > cap;
    rows.truncate(cap);
    Ok((rows, capped))
}

async fn backfill_assertions(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    resolver: &mut DocAuthorityResolver<'_>,
    cap: usize,
) -> PipelineResult<ClassificationBackfillStats> {
    let (records, capped) = rows(
        planning,
        &AssertionRepository::new(storage.clone()),
        space,
        cap,
    )
    .await?;
    let mut stats = ClassificationBackfillStats {
        scanned: records.len(),
        capped,
        ..ClassificationBackfillStats::default()
    };
    for before in records {
        if !is_baseline(&before.base.security) {
            continue;
        }
        let revisions = vec![before.evidence.revision.id().clone()];
        let Some(docs) = resolver.documents(&revisions).await? else {
            stats.unresolved += 1;
            continue;
        };
        let mut after = before.clone();
        if !inherit_documents(&mut after.base, &docs)? {
            stats.unresolved += 1;
            continue;
        }
        planning.stage_source_classification(before, after, &docs)?;
        stats.updated += 1;
    }
    Ok(stats)
}

async fn backfill_attributes(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    resolver: &mut DocAuthorityResolver<'_>,
    cap: usize,
) -> PipelineResult<ClassificationBackfillStats> {
    let (records, capped) = rows(
        planning,
        &AttributeRepository::new(storage.clone()),
        space,
        cap,
    )
    .await?;
    let mut stats = ClassificationBackfillStats {
        scanned: records.len(),
        capped,
        ..ClassificationBackfillStats::default()
    };
    for before in records {
        if !is_baseline(&before.base.security) {
            continue;
        }
        let revisions = before
            .evidence
            .iter()
            .map(|evidence| evidence.span.revision.id().clone())
            .collect::<Vec<_>>();
        let Some(docs) = resolver.documents(&revisions).await? else {
            stats.unresolved += 1;
            continue;
        };
        let mut after = before.clone();
        if !inherit_documents(&mut after.base, &docs)? {
            stats.unresolved += 1;
            continue;
        }
        planning.stage_source_classification(before, after, &docs)?;
        stats.updated += 1;
    }
    Ok(stats)
}

async fn backfill_rels(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    resolver: &mut DocAuthorityResolver<'_>,
    cap: usize,
) -> PipelineResult<ClassificationBackfillStats> {
    let (records, capped) =
        rows(planning, &RelRepository::new(storage.clone()), space, cap).await?;
    let mut stats = ClassificationBackfillStats {
        scanned: records.len(),
        capped,
        ..ClassificationBackfillStats::default()
    };
    for before in records {
        if !is_baseline(&before.base.security) {
            continue;
        }
        let revisions = before
            .evidence
            .iter()
            .map(|evidence| evidence.span.revision.id().clone())
            .collect::<Vec<_>>();
        let Some(docs) = resolver.documents(&revisions).await? else {
            stats.unresolved += 1;
            continue;
        };
        let mut after = before.clone();
        if !inherit_documents(&mut after.base, &docs)? {
            stats.unresolved += 1;
            continue;
        }
        planning.stage_source_classification(before, after, &docs)?;
        stats.updated += 1;
    }
    Ok(stats)
}

async fn backfill_events(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    resolver: &mut DocAuthorityResolver<'_>,
    cap: usize,
) -> PipelineResult<ClassificationBackfillStats> {
    let (records, capped) =
        rows(planning, &EventRepository::new(storage.clone()), space, cap).await?;
    let mut stats = ClassificationBackfillStats {
        scanned: records.len(),
        capped,
        ..ClassificationBackfillStats::default()
    };
    for before in records {
        if !is_baseline(&before.base.security) {
            continue;
        }
        let mut revisions = Vec::new();
        let mut missing = false;
        let repo = AssertionRepository::new(storage.clone());
        for assertion in &before.assertion_refs {
            let live = repo.get(space, assertion.id()).await?;
            if let Some(row) = planning.current_row(live, assertion.id(), false)? {
                revisions.push(row.evidence.revision.id().clone());
            } else {
                missing = true;
                break;
            }
        }
        if missing {
            stats.unresolved += 1;
            continue;
        }
        let Some(docs) = resolver.documents(&revisions).await? else {
            stats.unresolved += 1;
            continue;
        };
        let mut after = before.clone();
        if !inherit_documents(&mut after.base, &docs)? {
            stats.unresolved += 1;
            continue;
        }
        planning.stage_source_classification(before, after, &docs)?;
        stats.updated += 1;
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

    async fn test_storage(embedded: bool) -> StorageClient {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .expect("store");
        if !embedded {
            storage
                .query_checked(include_str!(
                    "../../../migrations/sql/031_source_publication.surql"
                ))
                .await
                .expect("canonical schema");
        }
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    SPACE.into(),
                    "test".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("space");
        storage
    }

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
        doc_base.security = security.clone();
        doc_base.subjects_state = SubjectsState::Complete;
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
                base: {
                    let mut base =
                        BaseFields::new(revision_id.clone(), SPACE.to_string(), "test".to_string());
                    base.security = security;
                    base.subjects_state = SubjectsState::Complete;
                    base
                },
                doc: Link::new(doc_id),
                text: "text".to_string(),
                text_hash: crate::extraction::AssertionValidator::hash_quote("text"),
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
            end: 4,
            quote_hash: crate::extraction::AssertionValidator::hash_quote("text"),
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
    async fn reclassifies_every_derived_kind(embedded: bool) {
        let storage = test_storage(embedded).await;

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

    #[tokio::test]
    async fn backfill_reclassifies_every_derived_kind() {
        reclassifies_every_derived_kind(false).await;
    }
    #[tokio::test]
    async fn embedded_backfill_reclassifies_every_derived_kind() {
        reclassifies_every_derived_kind(true).await;
    }

    /// The pass must converge: a second run writes nothing. Without this the
    /// sweep would rewrite the same rows every 15 minutes forever.
    #[tokio::test]
    async fn backfill_is_idempotent() {
        let storage = test_storage(false).await;

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
        let storage = test_storage(false).await;

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
        let storage = test_storage(false).await;

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

    async fn prepared_backfill_is_atomic(embedded: bool) {
        let storage = test_storage(embedded).await;
        let revision = seed_doc(&storage, owner_only()).await;
        let assertion_id = seed_unclassified_assertion(&storage, &revision, "prepared").await;
        let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), SPACE, None)
            .await
            .unwrap();
        let result = prepare_classification_backfill_sweep(&planning, &storage, SPACE)
            .await
            .unwrap();
        assert_eq!(result.updated, 1);
        let assertions = AssertionRepository::new(storage.clone());
        assert!(is_baseline(
            &assertions
                .get(SPACE, &assertion_id)
                .await
                .unwrap()
                .unwrap()
                .base
                .security
        ));
        let revision_row = DocRevisionRepository::new(storage.clone())
            .get(SPACE, &revision)
            .await
            .unwrap()
            .unwrap();
        let docs = DocRepository::new(storage.clone());
        let mut doc = docs
            .get(SPACE, revision_row.doc.id())
            .await
            .unwrap()
            .unwrap();
        doc.base
            .security
            .handling
            .push("concurrent restriction".into());
        let id = doc.base.id.clone();
        docs.update(SPACE, &id, doc).await.unwrap();
        assert!(planning
            .commit(
                "classification_backfill",
                SPACE,
                &serde_json::json!({"test":true}),
                &result
            )
            .await
            .is_err());
        assert!(is_baseline(
            &assertions
                .get(SPACE, &assertion_id)
                .await
                .unwrap()
                .unwrap()
                .base
                .security
        ));
        assert!(elephant_storage::SystemEventRepository::new(storage)
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn embedded_backfill_prepares_without_writes_and_rejects_changed_doc() {
        prepared_backfill_is_atomic(true).await;
    }
    #[tokio::test]
    async fn surreal_backfill_prepares_without_writes_and_rejects_changed_doc() {
        prepared_backfill_is_atomic(false).await;
    }

    #[test]
    fn classification_preserves_subject_uncertainty_and_rejects_mixed_policy() {
        let mut base = base_with(TableName::Assertion, baseline());
        let mut doc_base = base_with(TableName::Doc, owner_only());
        doc_base.subjects_state = SubjectsState::Partial;
        doc_base
            .subjects
            .push(Link::new(RecordId::new_v4(TableName::Entity)));
        let doc = Doc {
            base: doc_base,
            title: "classification".into(),
            text: "text".into(),
            sidecar_text: None,
            source: None,
            media_attachments: vec![],
            rendition_key: "test".into(),
            metadata: serde_json::json!({}),
        };
        assert!(inherit_documents(&mut base, std::slice::from_ref(&doc)).unwrap());
        assert_eq!(base.subjects_state, SubjectsState::Partial);
        assert_eq!(base.subjects, doc.base.subjects);
        let mut incompatible = doc.clone();
        incompatible.base.security.policy_id = "unrelated-policy".into();
        let before = base.clone();
        assert!(matches!(
            inherit_documents(&mut base, &[doc, incompatible]),
            Err(crate::error::PipelineError::ValidationError(_))
        ));
        assert_eq!(
            serde_json::to_value(&base).unwrap(),
            serde_json::to_value(before).unwrap()
        );
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
        assert_eq!(combine(&[]).unwrap(), None);
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
        let sealed_last = combine(&[open.clone(), owner_only()])
            .expect("compatible policies")
            .expect("combined");
        let sealed_first = combine(&[owner_only(), open])
            .expect("compatible policies")
            .expect("combined");
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
