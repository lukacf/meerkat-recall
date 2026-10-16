//! Current publication and source lifecycle checks shared by public read paths.
//!
//! This module owns no state. SourcePublicationRepository remains the authority
//! for published revisions and independent support. Callers supply current ABAC
//! evaluation; every contributing envelope is evaluated before content escapes.

use async_trait::async_trait;
use elephant_types::publication::{SourceHead, SourceVisibility};
use elephant_types::records::assertion::EvidenceSpan;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_revision::RevisionStatus;
use elephant_types::records::{
    Assertion, Attribute, Doc, DocNode, DocRevision, Entity, Event, Rel, Source,
};
use elephant_types::TableName;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::canonical_commit::{source_support_fingerprint, SourceEffectSupport};
use crate::source_publication::SourcePublicationRepository;
use crate::source_publication_manifest::SourcePublicationManifestRepository;
use crate::{
    AssertionRepository, DocNodeRepository, DocRepository, DocRevisionRepository, Repository,
    SourceRepository, StorageClient, StorageResult,
};

/// Adapter-provided authorization against the current principal and read scope.
pub type ReadAuthorization<'a> = dyn Fn(&BaseFields) -> bool + Send + Sync + 'a;

#[path = "read_action_disposition.rs"]
mod action_disposition;
#[path = "read_definition.rs"]
mod definition;
#[path = "read_historical_transition.rs"]
mod historical_transition;
#[path = "read_snapshot_graph.rs"]
mod snapshot_graph;

type SourceKey = (String, String);
type ObservedSnapshotEpochs = Arc<Mutex<BTreeMap<String, Option<String>>>>;
type ObservedHeads = Arc<Mutex<BTreeMap<SourceKey, Option<SourceHead>>>>;
type ObservedSources = Arc<Mutex<BTreeMap<SourceKey, BaseFields>>>;
type ObservedDefinitions =
    Arc<Mutex<BTreeMap<SourceKey, elephant_types::records::KnowledgeViewDefinition>>>;

/// Read-only access to canonical publication and immutable lineage.
pub struct ReadEligibility {
    storage: StorageClient,
    observed_snapshot_epochs: ObservedSnapshotEpochs,
    observed_heads: ObservedHeads,
    observed_sources: ObservedSources,
    observed_definitions: ObservedDefinitions,
}

tokio::task_local! {
    static REQUEST_READER: ReadEligibility;
}

/// Scope all independently constructed read adapters to one response fence.
///
/// Source changes after an early page cannot produce mixed-revision output.
/// Use only for read requests; mutations own their own transaction fences.
pub async fn with_read_fence<F: std::future::Future>(
    storage: StorageClient,
    future: F,
) -> StorageResult<F::Output> {
    let reader = ReadEligibility::new(storage);
    REQUEST_READER
        .scope(reader, async {
            let output = future.await;
            let reader = REQUEST_READER.with(|reader| ReadEligibility {
                storage: reader.storage.clone(),
                observed_snapshot_epochs: reader.observed_snapshot_epochs.clone(),
                observed_heads: reader.observed_heads.clone(),
                observed_sources: reader.observed_sources.clone(),
                observed_definitions: reader.observed_definitions.clone(),
            });
            reader.verify_current().await?;
            drop(reader);
            Ok(output)
        })
        .await
}

/// A complete immutable evidence chain. No reference-only partial success.
pub struct CurrentEvidence {
    /// Exact immutable revision.
    pub revision: DocRevision,
    /// Parent document.
    pub doc: Doc,
    /// Current source envelope, if the document is source-backed.
    pub source: Option<Source>,
    /// Cited node.
    pub node: Option<DocNode>,
    /// Validated quote.
    pub quote: String,
}

/// A record whose returned projection can remove obsolete support references.
#[async_trait]
pub trait CurrentRead: Clone + Serialize + Send + Sync {
    /// Root envelope for authorization and source support.
    fn read_base(&self) -> &BaseFields;

    /// Optional action-state response projection. Adapters supply per-table
    /// evidence scopes separately from the ordinary root-read authorization.
    async fn project_action_state(
        &mut self,
        _reader: &ReadEligibility,
        _authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<()> {
        Ok(())
    }

    /// Check current support and remove references that no longer contribute.
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool>;
}

impl ReadEligibility {
    /// Use the same storage snapshot authority as the consuming adapter.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            storage,
            observed_snapshot_epochs: REQUEST_READER
                .try_with(|reader| reader.observed_snapshot_epochs.clone())
                .unwrap_or_default(),
            observed_heads: REQUEST_READER
                .try_with(|reader| reader.observed_heads.clone())
                .unwrap_or_default(),
            observed_sources: REQUEST_READER
                .try_with(|reader| reader.observed_sources.clone())
                .unwrap_or_default(),
            observed_definitions: REQUEST_READER
                .try_with(|reader| reader.observed_definitions.clone())
                .unwrap_or_default(),
        }
    }

    async fn head(
        &self,
        space: &str,
        source: &elephant_types::RecordId,
    ) -> StorageResult<Option<SourceHead>> {
        let current = SourcePublicationRepository::new(self.storage.clone())
            .head(space, source)
            .await?;
        let mut observed = self.observed_heads.lock().map_err(|_| {
            crate::StorageError::Conflict("Read publication fence unavailable".into())
        })?;
        let key = (space.to_owned(), source.to_string());
        if let Some(previous) = observed.get(&key) {
            if previous != &current {
                return Err(crate::StorageError::Conflict(
                    "Source publication changed during read; retry".into(),
                ));
            }
        } else {
            observed.insert(key, current.clone());
        }
        drop(observed);
        Ok(current)
    }

    // Snapshot certificates also depend on independent rows which have no
    // source head. Capture before any dependency read and share across all
    // adapters in the response; the final fence rejects any intervening edit.
    async fn observe_snapshot_epoch(&self, space: &str) -> StorageResult<()> {
        let current = crate::canonical_commit::CanonicalRepository::new(self.storage.clone())
            .observe_epoch(space)
            .await?;
        let mut observed = self
            .observed_snapshot_epochs
            .lock()
            .map_err(|_| crate::StorageError::Conflict("Read snapshot fence unavailable".into()))?;
        if let Some(previous) = observed.get(space) {
            if previous != &current {
                return Err(crate::StorageError::Conflict(
                    "Snapshot dependency changed during read; retry".into(),
                ));
            }
        } else {
            observed.insert(space.to_owned(), current);
        }
        drop(observed);
        Ok(())
    }

    /// Recheck observed publications immediately before returning assembled content.
    pub async fn verify_current(&self) -> StorageResult<()> {
        use std::str::FromStr;
        let observed = self
            .observed_heads
            .lock()
            .map_err(|_| {
                crate::StorageError::Conflict("Read publication fence unavailable".into())
            })?
            .clone();
        for ((space, id), previous) in observed {
            let id = elephant_types::RecordId::from_str(&id).map_err(|_| {
                crate::StorageError::Conflict("Invalid observed source identity".into())
            })?;
            let current = SourcePublicationRepository::new(self.storage.clone())
                .head(&space, &id)
                .await?;
            if current != previous {
                return Err(crate::StorageError::Conflict(
                    "Source publication changed during read; retry".into(),
                ));
            }
        }
        let sources = self
            .observed_sources
            .lock()
            .map_err(|_| crate::StorageError::Conflict("Read source fence unavailable".into()))?
            .clone();
        for ((space, id), base) in sources {
            let id = elephant_types::RecordId::from_str(&id).map_err(|_| {
                crate::StorageError::Conflict("Invalid observed source identity".into())
            })?;
            let current = SourceRepository::new(self.storage.clone())
                .get(&space, &id)
                .await?;
            if current.as_ref().map(|source| &source.base) != Some(&base) {
                return Err(crate::StorageError::Conflict(
                    "Source envelope changed during read; retry".into(),
                ));
            }
        }
        let definitions = self
            .observed_definitions
            .lock()
            .map_err(|_| crate::StorageError::Conflict("Read definition fence unavailable".into()))?
            .clone();
        let canonical = crate::canonical_commit::CanonicalRepository::new(self.storage.clone());
        for definition in definitions.values() {
            if canonical
                .definition_decision_inputs(definition)
                .await?
                .is_none()
            {
                return Err(crate::StorageError::Conflict(
                    "Definition decision or original input changed during read; retry".into(),
                ));
            }
        }
        let epochs = self
            .observed_snapshot_epochs
            .lock()
            .map_err(|_| crate::StorageError::Conflict("Read snapshot fence unavailable".into()))?
            .clone();
        for (space, previous) in epochs {
            if canonical.observe_epoch(&space).await? != previous {
                return Err(crate::StorageError::Conflict(
                    "Snapshot dependency changed during read; retry".into(),
                ));
            }
        }
        Ok(())
    }

    /// Project and authorize one record. A denied record discloses no content.
    ///
    /// Keep composed read futures on the heap: TruthSlot -> Event -> Assertion
    /// traversals otherwise retain several large debug construction frames at
    /// once. The non-inlined constructor returns before dependency polling;
    /// source, authorization and snapshot guards run in their original order.
    #[inline(never)]
    pub fn project<'a, T: CurrentRead + 'a>(
        &'a self,
        mut record: T,
        authorize: &'a ReadAuthorization<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<Option<T>>> + Send + 'a>>
    {
        Box::pin(async move {
            if record.read_base().deleted_at.is_some() || !authorize(record.read_base()) {
                return Ok(None);
            }
            // Verify the exact stored domain row before projection removes evidence
            // or rewrites text. A source head and a record ID alone cannot certify
            // a row that was changed outside the canonical transaction.
            if record.read_base().id.table() != TableName::Source
                && !self
                    .matches_content_support(record.read_base(), &record)
                    .await?
            {
                return Ok(None);
            }
            if crate::canonical_commit::is_derived_graph_table(record.read_base().id.table())
                && record.read_base().id.table() != TableName::KnowledgeViewDefinition
                && !self.base_support(record.read_base(), authorize).await?
            {
                return Ok(None);
            }
            if crate::canonical_commit::is_derived_graph_table(record.read_base().id.table()) {
                for support in self
                    .current_support(&record.read_base().space_id, &record.read_base().id)
                    .await?
                {
                    if let Some(history) = &support.historical_unknown {
                        let receipt =
                            crate::canonical_commit::CanonicalRepository::new(self.storage.clone())
                                .receipt_read_only(&support.space_id, &history.commit_id)
                                .await?;
                        if let Some(publication) = receipt
                            .as_ref()
                            .and_then(|receipt| receipt.publication_authority.as_ref())
                        {
                            if let Some(manifest) =
                                SourcePublicationManifestRepository::new(self.storage.clone())
                                    .get(&support.space_id, publication)
                                    .await?
                            {
                                if manifest
                                    .historical_adoption
                                    .as_ref()
                                    .is_some_and(|proof| proof.snapshot_graph.is_some())
                                    && !self
                                        .snapshot_graph_evidence(record.read_base(), authorize)
                                        .await?
                                {
                                    return Ok(None);
                                }
                            }
                        }
                    }
                }
            }
            let retained = record.retain_current(self, authorize).await?;
            self.verify_current().await?;
            Ok(retained.then_some(record))
        })
    }

    /// Filter a candidate page, preserving ranking among eligible records.
    pub async fn filter<T: CurrentRead>(
        &self,
        records: Vec<T>,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<Vec<T>> {
        let mut result = Vec::with_capacity(records.len());
        for record in records {
            if let Some(record) = self.project(record, authorize).await? {
                result.push(record);
            }
        }
        self.verify_current().await?;
        Ok(result)
    }

    async fn matches_content_support<T: Serialize + Sync>(
        &self,
        base: &BaseFields,
        record: &T,
    ) -> StorageResult<bool> {
        let support = self.current_support(&base.space_id, &base.id).await?;
        support_matches_record(base, record, &support)
    }

    /// Current source visibility, including the fence before physical cleanup.
    pub async fn source(
        &self,
        source: &Source,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        if source.base.deleted_at.is_some() || !authorize(&source.base) {
            return Ok(false);
        }
        {
            let mut observed = self.observed_sources.lock().map_err(|_| {
                crate::StorageError::Conflict("Read source fence unavailable".into())
            })?;
            let key = (source.base.space_id.clone(), source.base.id.to_string());
            if let Some(previous) = observed.get(&key) {
                if previous != &source.base {
                    return Err(crate::StorageError::Conflict(
                        "Source envelope changed during read; retry".into(),
                    ));
                }
            } else {
                observed.insert(key, source.base.clone());
            }
        }
        let head = self.head(&source.base.space_id, &source.base.id).await?;
        Ok(head.is_none_or(|head| head.lifecycle == SourceVisibility::Active))
    }

    /// Read the current parent and enforce source publication on raw documents.
    pub async fn document(
        &self,
        doc: &Doc,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        if doc.base.deleted_at.is_some()
            || !authorize(&doc.base)
            || !self.matches_content_support(&doc.base, doc).await?
        {
            return Ok(false);
        }
        let Some(link) = &doc.source else {
            return self.base_support(&doc.base, authorize).await;
        };
        let Some(source) = SourceRepository::new(self.storage.clone())
            .get(&doc.base.space_id, link.id())
            .await?
        else {
            return Ok(false);
        };
        if !self.source(&source, authorize).await? {
            return Ok(false);
        }
        let Some(head) = self.head(&doc.base.space_id, &source.base.id).await? else {
            // Existing source-first input has no preview head. Its live Source
            // and document remain authoritative; never invent a generation.
            return Ok(true);
        };
        if head.published_revision.is_none() {
            return Ok(false);
        }
        // A renderer may transform raw input. The original completion binds
        // the entire document and revision; equal text alone is not authority.
        for revision in DocRevisionRepository::new(self.storage.clone())
            .find_by_doc(&doc.base.id)
            .await?
        {
            if revision.base.space_id == doc.base.space_id
                && hash_matches(&revision.text, &revision.text_hash)
                && authorize(&revision.base)
                && self
                    .revision(&revision, authorize)
                    .await?
                    .is_some_and(|(published_doc, _)| published_doc == *doc)
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Exact immutable revision eligibility, independent of pending doc renders.
    pub async fn revision(
        &self,
        revision: &DocRevision,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<Option<(Doc, Option<Source>)>> {
        let space = &revision.base.space_id;
        if revision.base.deleted_at.is_some()
            || !authorize(&revision.base)
            || !hash_matches(&revision.text, &revision.text_hash)
            || !self
                .matches_content_support(&revision.base, revision)
                .await?
        {
            return Ok(None);
        }
        let Some(doc) = DocRepository::new(self.storage.clone())
            .get(space, revision.doc.id())
            .await?
        else {
            return Ok(None);
        };
        if doc.base.deleted_at.is_some()
            || !authorize(&doc.base)
            || !self.matches_content_support(&doc.base, &doc).await?
        {
            return Ok(None);
        }
        let Some(link) = &doc.source else {
            // Retained source-less documents use their explicit current render.
            return Ok(
                (revision.status == RevisionStatus::Active && revision.text == doc.text)
                    .then_some((doc, None)),
            );
        };
        let Some(source) = SourceRepository::new(self.storage.clone())
            .get(space, link.id())
            .await?
        else {
            return Ok(None);
        };
        if !self.source(&source, authorize).await? {
            return Ok(None);
        }
        if let Some(head) = self.head(space, &source.base.id).await? {
            let mapped = SourcePublicationManifestRepository::new(self.storage.clone())
                .source_revision_for_document_revision(space, &revision.base.id)
                .await?;
            let Some(mapping) = mapped.filter(|mapping| {
                mapping.space_id == *space
                    && mapping.doc_id == doc.base.id
                    && mapping.doc_revision_id == revision.base.id
                    && mapping.publication.source_id == source.base.id
                    && Some(&mapping.publication.revision_id) == head.published_revision.as_ref()
            }) else {
                return Ok(None);
            };
            let Some(published) = head.published_revision else {
                return Ok(None);
            };
            let Some(accepted) = SourcePublicationRepository::new(self.storage.clone())
                .revision(space, &source.base.id, &published)
                .await?
            else {
                return Ok(None);
            };
            if !authorize(&accepted.source.base) {
                return Ok(None);
            }
            if !SourcePublicationManifestRepository::new(self.storage.clone())
                .completed_document_revision_is_current(&mapping, &doc, revision, &accepted)
                .await?
            {
                return Ok(None);
            }
        } else if revision.status != RevisionStatus::Active || revision.text != doc.text {
            return Ok(None);
        }
        Ok(Some((doc, Some(source))))
    }

    /// Validate exact nonempty UTF-8 bytes, digest, node and all current parents.
    pub async fn evidence(
        &self,
        space: &str,
        span: &EvidenceSpan,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<Option<CurrentEvidence>> {
        if span.start >= span.end {
            return Ok(None);
        }
        let Some(revision) = DocRevisionRepository::new(self.storage.clone())
            .get(space, span.revision.id())
            .await?
        else {
            return Ok(None);
        };
        let Some((doc, source)) = self.revision(&revision, authorize).await? else {
            return Ok(None);
        };
        let Some(quote) = revision
            .text
            .get(span.start as usize..span.end as usize)
            .filter(|text| !text.trim().is_empty())
        else {
            return Ok(None);
        };
        if !hash_matches(quote, &span.quote_hash) {
            return Ok(None);
        }
        let quote = quote.to_owned();
        let node = if let Some(link) = &span.node {
            let Some(node) = DocNodeRepository::new(self.storage.clone())
                .get(space, link.id())
                .await?
            else {
                return Ok(None);
            };
            if node.base.deleted_at.is_some()
                || !authorize(&node.base)
                || !self.matches_content_support(&node.base, &node).await?
                || !evidence_within_accepted_window(&self.storage, space, &revision, &node, span)
                    .await?
            {
                return Ok(None);
            }
            let mut node = node;
            node.text = revision
                .text
                .get(node.span.start as usize..node.span.end as usize)
                .map(str::to_owned);
            Some(node)
        } else {
            None
        };
        self.verify_current().await?;
        Ok(Some(CurrentEvidence {
            revision,
            doc,
            source,
            node,
            quote,
        }))
    }

    /// Envelope prerequisite; this alone does not certify semantic row content.
    ///
    /// Intrinsic/independent records survive source replacement and erasure
    /// after canonical reconciliation removes any obsolete enrichment.
    /// Source-derived records require an eligible ledger entry or verified
    /// pre-preview lineage; missing publication is never silently treated as
    /// independent authority.
    #[inline(never)]
    pub fn base_support<'a>(
        &'a self,
        base: &'a BaseFields,
        authorize: &'a ReadAuthorization<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<bool>> + Send + 'a>> {
        Box::pin(async move {
            if base.deleted_at.is_some() || !authorize(base) {
                return Ok(false);
            }
            let support = self.current_support(&base.space_id, &base.id).await?;
            // Structural filtering must not erase authorization dependencies on
            // intermediate retired sources which contributed original graph bytes.
            let originals = SourcePublicationRepository::new(self.storage.clone())
                .support_for_record(&base.space_id, &base.id)
                .await?;
            for original in &originals {
                if original.historical_unknown.is_some()
                    && self
                        .head(&base.space_id, &original.source_id)
                        .await?
                        .is_some_and(|head| head.lifecycle == SourceVisibility::Retracted)
                    && !self
                        .historical_regrouped_support(original, &originals, Some(authorize))
                        .await?
                {
                    return Ok(false);
                }
            }
            // Aggregate semantic fields cannot be reconstructed from record-level
            // support alone. Until authoritative reconciliation removes a retired
            // provenance edge, preserve no possibly enriched historical payload.
            for id in &base.provenance.sources {
                if id.table() != TableName::Source {
                    return Ok(false);
                }
                let Some(source) = SourceRepository::new(self.storage.clone())
                    .get(&base.space_id, id)
                    .await?
                else {
                    return Ok(false);
                };
                if !self.source(&source, authorize).await? {
                    if source.base.deleted_at.is_some() || !authorize(&source.base) {
                        return Ok(false);
                    }
                    let original = SourcePublicationRepository::new(self.storage.clone())
                        .support_for_record(&base.space_id, &base.id)
                        .await?;
                    let mut replaced = false;
                    for entry in original.iter().filter(|entry| entry.source_id == *id) {
                        if self
                            .historical_regrouped_support(entry, &original, Some(authorize))
                            .await?
                        {
                            replaced = true;
                            break;
                        }
                    }
                    if !replaced {
                        return Ok(false);
                    }
                    continue;
                }
                if let Some(head) = self.head(&base.space_id, id).await? {
                    if !support.iter().any(|entry| {
                        entry.source_id == *id
                            && Some(entry.source_revision.as_str())
                                == head.published_revision.as_deref()
                    }) {
                        return Ok(false);
                    }
                }
            }
            if !support.is_empty() {
                for entry in support {
                    if entry.historical_unknown.is_some()
                        && !self.historical_support_is_current(&entry).await?
                    {
                        return Ok(false);
                    }
                    let Some(source) = SourceRepository::new(self.storage.clone())
                        .get(&base.space_id, &entry.source_id)
                        .await?
                    else {
                        return Ok(false);
                    };
                    if !self.source(&source, authorize).await? {
                        return Ok(false);
                    }
                    let Some(head) = self.head(&base.space_id, &entry.source_id).await? else {
                        return Ok(false);
                    };
                    if head.published_revision.as_deref() != Some(entry.source_revision.as_str()) {
                        return Ok(false);
                    }
                }
                return Ok(true);
            }
            if base.provenance.sources.is_empty() {
                return Ok(base.provenance.extractor.is_none()
                    && base.provenance.work_run_id.is_none()
                    && base.provenance.chain.is_empty());
            }
            self.legacy_source_support(base, authorize).await
        })
    }

    async fn legacy_source_support(
        &self,
        base: &BaseFields,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        // Prior source-first rows have no manifest/ledger. Require live
        // same-space sources, and refuse a preview source whose exact support
        // generation was not recorded.
        for id in &base.provenance.sources {
            if id.table() != TableName::Source {
                return Ok(false);
            }
            let Some(source) = SourceRepository::new(self.storage.clone())
                .get(&base.space_id, id)
                .await?
            else {
                continue;
            };
            if self.source(&source, authorize).await?
                && self.head(&base.space_id, id).await?.is_none()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Retained historical evidence may be superseded only by an actual current
    /// historical certificate for this same source and physical graph record.
    /// Ordinary stale supports and missing/corrupt history still refuse reads.
    #[inline(never)]
    fn current_support<'a>(
        &'a self,
        space: &'a str,
        record: &'a elephant_types::RecordId,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = StorageResult<Vec<SourceEffectSupport>>> + Send + 'a>,
    > {
        Box::pin(async move {
            let all = SourcePublicationRepository::new(self.storage.clone())
                .support_for_record(space, record)
                .await?;
            let mut current = Vec::new();
            for entry in &all {
                let head = self.head(space, &entry.source_id).await?;
                let retired = entry.historical_unknown.is_some()
                    && head.as_ref().is_some_and(|head| {
                        head.published_revision
                            .as_deref()
                            .is_some_and(|revision| revision != entry.source_revision)
                    });
                if !retired {
                    if self.historical_regrouped_support(entry, &all, None).await? {
                        continue;
                    }
                    current.push(entry.clone());
                    continue;
                }
                let replacement = all.iter().find(|candidate| {
                    candidate.source_id == entry.source_id
                        && candidate.record_id == entry.record_id
                        && candidate.historical_unknown.is_some()
                        && head
                            .as_ref()
                            .and_then(|head| head.published_revision.as_deref())
                            == Some(candidate.source_revision.as_str())
                });
                let Some(replacement) = replacement else {
                    return Err(crate::StorageError::DataIntegrityError(
                        "retired support lacks current historical replacement".into(),
                    ));
                };
                if !self.historical_support_is_current(replacement).await?
                    || !self.historical_support_receipt_is_retained(entry).await?
                {
                    return Err(crate::StorageError::DataIntegrityError(
                        "historical replacement support authority differs".into(),
                    ));
                }
            }
            if !all.is_empty() && current.is_empty() {
                return Err(crate::StorageError::DataIntegrityError(
                    "historical support population has no current authority".into(),
                ));
            }
            Ok(current)
        })
    }

    async fn historical_support_receipt_is_retained(
        &self,
        support: &SourceEffectSupport,
    ) -> StorageResult<bool> {
        let Some(history) = &support.historical_unknown else {
            return Ok(false);
        };
        if support.independent
            || support.last_commit_id.as_deref() != Some(history.commit_id.as_str())
        {
            return Ok(false);
        }
        let Some(receipt) = SourcePublicationRepository::new(self.storage.clone())
            .read_control::<crate::canonical_commit::CanonicalCommitReceipt>(
                "source_commit_receipt",
                &crate::source_publication::stable_key(&[&support.space_id, &history.commit_id]),
                &support.space_id,
            )
            .await?
        else {
            return Ok(false);
        };
        let Some(publication) = &receipt.publication_authority else {
            return Ok(false);
        };
        let publication_revision_matches = publication.revision_id == support.source_revision;
        if receipt.space_id != support.space_id
            || receipt.commit_id != history.commit_id
            || publication.source_id != support.source_id
            || !publication_revision_matches
        {
            return Ok(false);
        }
        let Some(manifest) = SourcePublicationManifestRepository::new(self.storage.clone())
            .get(&support.space_id, publication)
            .await?
        else {
            return Ok(false);
        };
        manifest.validate()?;
        let Some(proof) = &manifest.historical_adoption else {
            return Ok(false);
        };
        Ok(proof.commit_id(&manifest) == history.commit_id
            && proof.snapshot_sha256 == history.snapshot_sha256
            && proof.plan_id == history.plan_id
            && proof.graph_fingerprints.get(&support.record_id)
                == Some(&support.record_fingerprint)
            && receipt.historical_import.as_ref()
                == Some(
                    &crate::historical_adoption::HistoricalCommitProvenance::for_manifest(
                        &manifest,
                    )?,
                ))
    }

    async fn historical_support_is_current(
        &self,
        support: &SourceEffectSupport,
    ) -> StorageResult<bool> {
        let Some(history) = &support.historical_unknown else {
            return Ok(true);
        };
        if support.independent
            || support.last_commit_id.as_deref() != Some(history.commit_id.as_str())
        {
            return Ok(false);
        }
        let Some(head) = self.head(&support.space_id, &support.source_id).await? else {
            return Ok(false);
        };
        if head.published_revision.as_deref() != Some(support.source_revision.as_str()) {
            return Ok(false);
        }
        let publication = elephant_types::publication::PublicationFence {
            source_id: support.source_id.clone(),
            generation: head.generation,
            revision_id: support.source_revision.clone(),
            operation_id: head.operation_id,
        };
        let Some(manifest) = SourcePublicationManifestRepository::new(self.storage.clone())
            .get(&support.space_id, &publication)
            .await?
        else {
            return Ok(false);
        };
        manifest.validate()?;
        let Some(proof) = &manifest.historical_adoption else {
            return Ok(false);
        };
        if proof.snapshot_sha256 != history.snapshot_sha256
            || proof.plan_id != history.plan_id
            || proof.commit_id(&manifest) != history.commit_id
            || proof.graph_fingerprints.get(&support.record_id) != Some(&support.record_fingerprint)
        {
            return Ok(false);
        }
        let Some(receipt) = crate::canonical_commit::CanonicalRepository::new(self.storage.clone())
            .receipt_read_only(&support.space_id, &history.commit_id)
            .await?
        else {
            return Ok(false);
        };
        Ok(receipt.publication_authority.as_ref() == Some(&publication)
            && receipt.historical_import.as_ref()
                == Some(
                    &crate::historical_adoption::HistoricalCommitProvenance::for_manifest(
                        &manifest,
                    )?,
                ))
    }

    async fn retain_evidence(
        &self,
        space: &str,
        evidence: &mut Vec<elephant_types::records::rel::EvidenceRef>,
        snapshot_certified: bool,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<()> {
        let mut current = Vec::new();
        for link in evidence.drain(..) {
            let Some(assertion) = AssertionRepository::new(self.storage.clone())
                .get(space, link.assertion.id())
                .await?
            else {
                continue;
            };
            if assertion.evidence != link.span
                || self.project(assertion, authorize).await?.is_none()
            {
                continue;
            }
            if (snapshot_certified && link.span.start == 0 && link.span.end == 0)
                || self.evidence(space, &link.span, authorize).await?.is_some()
            {
                // A snapshot-certified root may retain an ungrounded reference
                // only after its exact linked assertion passed authorization.
                // The public evidence endpoint still returns no empty quote.
                current.push(link);
            }
        }
        *evidence = current;
        Ok(())
    }
}

/// A ledger entry binds the entire domain row. `independent` records the
/// existence of baseline authority; it cannot certify different/enriched bytes.
#[expect(
    clippy::suspicious_operation_groupings,
    reason = "The support ledger record_id binds BaseFields.id; both space and the complete record fingerprint must still match."
)]
fn support_matches_record<T: Serialize>(
    base: &BaseFields,
    record: &T,
    support: &[SourceEffectSupport],
) -> StorageResult<bool> {
    if support.is_empty() {
        return Ok(true);
    } // Eligibility still requires lineage below.
    let fingerprint = source_support_fingerprint(record)?;
    Ok(support.iter().all(|entry| {
        entry.space_id == base.space_id
            && entry.record_id == base.id
            && entry.record_fingerprint == fingerprint
    }))
}

/// Check a physical node or its exact persisted coalesced extraction window.
///
/// This is a range check, not authorization or a semantic content certificate.
/// Callers still validate the quote digest, full row proof and source envelope.
pub async fn evidence_within_accepted_window(
    storage: &StorageClient,
    space: &str,
    revision: &DocRevision,
    node: &DocNode,
    span: &EvidenceSpan,
) -> StorageResult<bool> {
    if !evidence_has_exact_physical_span(space, revision, node, span) {
        return Ok(false);
    }
    let within_node = node.span.start <= span.start && span.end <= node.span.end;
    let repository = SourcePublicationManifestRepository::new(storage.clone());
    let Some(mapping) = repository
        .source_revision_for_document_revision(space, &revision.base.id)
        .await?
    else {
        return Ok(within_node);
    };
    if mapping.space_id != space
        || mapping.doc_id != *revision.doc.id()
        || mapping.doc_revision_id != revision.base.id
    {
        return Ok(false);
    }
    let Some(head) = SourcePublicationRepository::new(storage.clone())
        .head(space, &mapping.publication.source_id)
        .await?
    else {
        return Ok(false);
    };
    if head.lifecycle != SourceVisibility::Active
        || head.published_revision.as_deref() != Some(mapping.publication.revision_id.as_str())
    {
        return Ok(false);
    }
    let Some(manifest) = repository.get(space, &mapping.publication).await? else {
        return Ok(within_node);
    };
    if manifest.space_id != space
        || manifest.publication != mapping.publication
        || !manifest.documents.contains(&mapping)
    {
        return Ok(false);
    }
    // Published windows belong to the current completed manifest, never a
    // historical expectation set replaced by an explicit configuration replan.
    let Some(completion) = repository.completion(space, &mapping.publication).await? else {
        return Ok(false);
    };
    if completion.space_id != space
        || completion.publication != mapping.publication
        || completion.manifest_digest != manifest.digest()?
    {
        return Ok(false);
    }
    if let Some(proof) = &manifest.historical_adoption {
        manifest.validate()?;
        let Some(receipt) = SourcePublicationRepository::new(storage.clone())
            .read_control::<crate::canonical_commit::CanonicalCommitReceipt>(
                "source_commit_receipt",
                &crate::source_publication::stable_key(&[space, &completion.commit_id]),
                space,
            )
            .await?
        else {
            return Ok(false);
        };
        return Ok(completion.commit_id == proof.commit_id(&manifest)
            && receipt.commit_id == completion.commit_id
            && receipt.publication_authority.as_ref() == Some(&mapping.publication)
            && receipt.historical_import.as_ref()
                == Some(
                    &crate::historical_adoption::HistoricalCommitProvenance::for_manifest(
                        &manifest,
                    )?,
                )
            && within_node
            && proof.node_matches(&mapping, node)?);
    }
    drop(repository);
    let selected = manifest
        .bundles
        .iter()
        .filter(|bundle| bundle.node_id == node.base.id && bundle.revision_id == revision.base.id)
        .collect::<Vec<_>>();
    // Retrieval-only rendered nodes have no extraction bundle and retain their
    // physical bounds. An extraction anchor uses only its sealed selected range.
    if selected.is_empty() {
        return Ok(within_node);
    }
    Ok(selected.into_iter().any(|bundle| {
        accepted_bundle_contains(bundle, &revision.base.id, &node.base.id, span)
            && revision
                .text
                .get(bundle.span_start as usize..bundle.span_end as usize)
                .is_some()
    }))
}

fn evidence_has_exact_physical_span(
    space: &str,
    revision: &DocRevision,
    node: &DocNode,
    span: &EvidenceSpan,
) -> bool {
    !(revision.base.space_id != space
        || node.base.space_id != space
        || node.revision.id() != &revision.base.id
        || span.revision.id() != &revision.base.id
        || span.node.as_ref().map(elephant_types::Link::id) != Some(&node.base.id)
        || span.start >= span.end
        || node.span.start >= node.span.end
        || revision
            .text
            .get(span.start as usize..span.end as usize)
            .is_none()
        || revision
            .text
            .get(node.span.start as usize..node.span.end as usize)
            .is_none())
}

fn accepted_bundle_contains(
    bundle: &crate::source_publication_manifest::ExpectedSourceBundle,
    revision_id: &elephant_types::RecordId,
    anchor_id: &elephant_types::RecordId,
    span: &EvidenceSpan,
) -> bool {
    bundle.revision_id == *revision_id
        && bundle.node_id == *anchor_id
        && bundle.node_ids.first() == Some(anchor_id)
        && span.revision.id() == revision_id
        && span.node.as_ref().map(elephant_types::Link::id) == Some(anchor_id)
        && bundle.span_start < bundle.span_end
        && span.start < span.end
        && bundle.span_start <= span.start
        && span.end <= bundle.span_end
}

/// Exact SHA-256 encodings already used by renderers and evidence producers.
#[must_use]
pub fn hash_matches(text: &str, expected: &str) -> bool {
    let expected = expected.strip_prefix("sha256:").unwrap_or(expected);
    expected.len() == 64
        && expected.eq_ignore_ascii_case(&hex::encode(Sha256::digest(text.as_bytes())))
}

macro_rules! intrinsic_record {
    ($($record:ty),* $(,)?) => {$(
        #[async_trait]
        impl CurrentRead for $record {
            fn read_base(&self) -> &BaseFields { &self.base }
            async fn retain_current(&mut self, reader: &ReadEligibility, authorize: &ReadAuthorization<'_>) -> StorageResult<bool> {
                reader.base_support(&self.base, authorize).await
            }
        }
    )*};
}
intrinsic_record!(Entity, elephant_types::records::SystemEvent);

#[async_trait]
impl CurrentRead for Source {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        if !reader.source(self, authorize).await? {
            return Ok(false);
        }
        if let Some(head) = reader.head(&self.base.space_id, &self.base.id).await? {
            let Some(revision) = &head.published_revision else {
                return Ok(false);
            };
            let Some(published) = SourcePublicationRepository::new(reader.storage.clone())
                .revision(&self.base.space_id, &self.base.id, revision)
                .await?
            else {
                return Ok(false);
            };
            if !authorize(&published.source.base) {
                return Ok(false);
            }
            let Some(projected) = SourcePublicationManifestRepository::new(reader.storage.clone())
                .project_published_source(&head, &published)
                .await?
            else {
                return Ok(false);
            };
            *self = projected;
        }
        Ok(true)
    }
}
#[async_trait]
impl CurrentRead for Doc {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        reader.document(self, authorize).await
    }
}
#[async_trait]
impl CurrentRead for DocRevision {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        Ok(reader.revision(self, authorize).await?.is_some())
    }
}
#[async_trait]
impl CurrentRead for DocNode {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        let Some(revision) = DocRevisionRepository::new(reader.storage.clone())
            .get(&self.base.space_id, self.revision.id())
            .await?
        else {
            return Ok(false);
        };
        if reader.revision(&revision, authorize).await?.is_none()
            || self.span.start >= self.span.end
        {
            return Ok(false);
        }
        let Some(text) = revision
            .text
            .get(self.span.start as usize..self.span.end as usize)
        else {
            return Ok(false);
        };
        if self.text.is_some() && self.role != elephant_types::records::doc_node::NodeRole::Summary
        {
            self.text = Some(text.to_owned());
        }
        // Generated summaries remain explicitly typed summaries; provenance
        // evidence always returns the underlying immutable passage instead.
        Ok(true)
    }
}
#[async_trait]
impl CurrentRead for Assertion {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        let evidence = reader
            .evidence(&self.base.space_id, &self.evidence, authorize)
            .await?;
        let Some(evidence) = evidence else {
            // Manual source-less assertions retain separate intrinsic authority;
            // an empty placeholder never produces a grounded evidence quote.
            return Ok(self.evidence.start == 0
                && self.evidence.end == 0
                && self.base.provenance.sources.is_empty()
                && ((self.base.provenance.extractor.is_none()
                    && self.base.provenance.work_run_id.is_none()
                    && self.base.provenance.chain.is_empty())
                    || reader
                        .snapshot_graph_evidence(&self.base, authorize)
                        .await?));
        };
        if let Some(source) = &evidence.source {
            let sources: std::collections::BTreeSet<_> =
                self.base.provenance.sources.iter().collect();
            if sources != std::collections::BTreeSet::from([&source.base.id]) {
                // A snapshot root may cite an independently adopted document.
                // The original assertion stays source-less: its own import
                // authority and exact current document evidence are conjunctive.
                if sources.is_empty()
                    && reader
                        .snapshot_graph_evidence(&self.base, authorize)
                        .await?
                {
                    return Ok(true);
                }
                // Imported source-less evidence retains its original provenance.
                // Exact membership comes from the actual historical commit,
                // never from a fabricated source edge on the assertion itself.
                let support = reader
                    .current_support(&self.base.space_id, &self.base.id)
                    .await?;
                let mut certified = false;
                for entry in support.iter().filter(|entry| {
                    entry.source_id == source.base.id && entry.historical_unknown.is_some()
                }) {
                    if !reader.historical_support_is_current(entry).await? {
                        continue;
                    }
                    let Some(head) = reader.head(&entry.space_id, &entry.source_id).await? else {
                        continue;
                    };
                    let publication = elephant_types::publication::PublicationFence {
                        source_id: entry.source_id.clone(),
                        generation: head.generation,
                        revision_id: entry.source_revision.clone(),
                        operation_id: head.operation_id,
                    };
                    let Some(manifest) =
                        SourcePublicationManifestRepository::new(reader.storage.clone())
                            .get(&entry.space_id, &publication)
                            .await?
                    else {
                        continue;
                    };
                    let Some(proof) = manifest.historical_adoption.as_ref() else {
                        continue;
                    };
                    // Original source rows are retained intact. The grouped
                    // identity is a declared association, not a rewrite of
                    // assertion provenance or a union of security envelopes.
                    certified |= sources
                        .iter()
                        .all(|id| proof.original_sources.contains_key(*id));
                }
                if !certified {
                    return Ok(false);
                }
            }
            if let Some(head) = reader.head(&self.base.space_id, &source.base.id).await? {
                let support = reader
                    .current_support(&self.base.space_id, &self.base.id)
                    .await?;
                if !support.iter().any(|entry| {
                    entry.source_id == source.base.id
                        && Some(entry.source_revision.as_str())
                            == head.published_revision.as_deref()
                }) {
                    return Ok(false);
                }
            }
        } else if !self.base.provenance.sources.is_empty() {
            return Ok(false);
        }
        Ok(true)
    }
}
macro_rules! claim_record {
    ($($record:ty),* $(,)?) => {$(
        #[async_trait]
        impl CurrentRead for $record {
            fn read_base(&self) -> &BaseFields { &self.base }
            async fn retain_current(&mut self, reader: &ReadEligibility, authorize: &ReadAuthorization<'_>) -> StorageResult<bool> {
                let original_len = self.evidence.len();
                let snapshot_certified = self.evidence.iter().any(|link| link.span.start == 0 && link.span.end == 0)
                    && reader.snapshot_graph_evidence(&self.base, authorize).await?;
                reader.retain_evidence(&self.base.space_id, &mut self.evidence, snapshot_certified, authorize).await?;
                if self.evidence.len() != original_len { return Ok(false); }
                if !self.evidence.is_empty() { return Ok(true); }
                reader.base_support(&self.base, authorize).await
            }
        }
    )*};
}
claim_record!(Rel, Attribute);

#[async_trait]
impl CurrentRead for Event {
    async fn project_action_state(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<()> {
        reader.project_action_dispositions(self, authorize).await
    }
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        // A user-provided metadata field cannot impersonate the verified
        // adapter projection, including callers that do not request it.
        if let Some(metadata) = self.metadata.as_object_mut() {
            metadata.remove("recall_action_dispositions");
        }
        let assertion_count = self.assertion_refs.len();
        let source_count = self.source_refs.len();
        let mut assertions = Vec::new();
        for link in &self.assertion_refs {
            if let Some(assertion) = AssertionRepository::new(reader.storage.clone())
                .get(&self.base.space_id, link.id())
                .await?
            {
                if reader.project(assertion, authorize).await?.is_some() {
                    assertions.push(link.clone());
                }
            }
        }
        self.assertion_refs = assertions;
        let mut sources = Vec::new();
        for link in &self.source_refs {
            if let Some(source) = SourceRepository::new(reader.storage.clone())
                .get(&self.base.space_id, link.id())
                .await?
            {
                if reader.source(&source, authorize).await? {
                    sources.push(link.clone());
                }
            }
        }
        self.source_refs = sources;
        if self.assertion_refs.len() != assertion_count || self.source_refs.len() != source_count {
            return Ok(false);
        }
        if !self.assertion_refs.is_empty() {
            return Ok(true);
        }
        reader.base_support(&self.base, authorize).await
    }
}

/// Descriptive compiler inputs never grant evidence or record authority.
fn wiki_descriptor(dependency: &elephant_types::records::KnowledgeDependency) -> bool {
    if dependency.record_type == "selector_trigger" {
        // Membership invalidation markers describe a selector, never a record
        // or an evidence snapshot. Accept only the compiler's exact grammar.
        return dependency.role == "potential_membership_change"
            && dependency.observed_updated_at.is_none()
            && matches!(
                dependency.record_id.as_str(),
                "trigger:table:entity" | "trigger:table:attribute" | "trigger:table:rel"
            );
    }
    let (prefix, allowed) = match dependency.record_type.as_str() {
        "model" => ("model:", ["article_writer", "article_verifier"]),
        "prompt" => (
            "prompt:",
            ["article_writer_prompt", "article_verifier_prompt"],
        ),
        _ => return false,
    };
    // Editorial deduplication joins roles when writer and verifier share a
    // model or prompt version. Every component must remain explicitly known.
    dependency
        .role
        .split('+')
        .all(|role| allowed.contains(&role))
        && dependency.observed_updated_at.is_none()
        && dependency
            .record_id
            .strip_prefix(prefix)
            .is_some_and(|value| {
                !value.is_empty() && value.len() <= 512 && !value.chars().any(char::is_control)
            })
}

/// A typed graph dependency has implicit subjects (entity self, relationship
/// endpoints) that are part of its domain authorization view.
///
/// Apply that view
/// only to the exact captured root; supporting parents keep their own envelope.
pub(crate) async fn project_wiki_input<T: CurrentRead + elephant_types::records::HasSecurity>(
    reader: &ReadEligibility,
    record: T,
    authorize: &ReadAuthorization<'_>,
) -> StorageResult<Option<T>> {
    let original = record.read_base().clone();
    let mut effective = original.clone();
    effective.subjects = record.subjects();
    effective.subjects_state = record.subjects_state();
    let root_allowed = authorize(&effective);
    reader
        .project(record, &|base| {
            if base.id == original.id && base.space_id == original.space_id {
                *base == original && root_allowed
            } else {
                authorize(base)
            }
        })
        .await
}

fn wiki_path_allows(path: &[elephant_types::RecordId], id: &elephant_types::RecordId) -> bool {
    path.len() < 32 && !path.contains(id)
}

fn wiki_source_dependency_matches(
    observed: Option<chrono::DateTime<chrono::Utc>>,
    stored: &Source,
    published: &Source,
) -> bool {
    if observed == Some(published.base.updated_at) {
        return true;
    }
    let mut semantic = stored.clone();
    semantic.base.updated_at = published.base.updated_at;
    semantic.lifecycle_status = published.lifecycle_status;
    semantic.current_ingest_mode = published.current_ingest_mode;
    semantic
        .latest_ingest_decision
        .clone_from(&published.latest_ingest_decision);
    semantic == *published && observed.is_none_or(|time| time == stored.base.updated_at)
}

fn wiki_citation_dependency(
    citation: &elephant_types::records::KnowledgeCitation,
    id: &elephant_types::RecordId,
    captured: &[elephant_types::records::KnowledgeDependency],
) -> elephant_types::records::KnowledgeDependency {
    // A Source citation addresses the same captured published snapshot as its
    // explicit dependency. Losing that observation would compare the live
    // source's normalized metadata or pending input with the accepted snapshot.
    // Without one unambiguous captured timestamp, retain the unbound check.
    let observed_updated_at = if id.table() == TableName::Source {
        let mut matching = captured.iter().filter(|dependency| {
            dependency.record_id == citation.record_id && dependency.record_type == "source"
        });
        matching.next().and_then(|first| {
            first.observed_updated_at.filter(|time| {
                matching.all(|dependency| dependency.observed_updated_at == Some(*time))
            })
        })
    } else {
        None
    };
    elephant_types::records::KnowledgeDependency {
        record_id: citation.record_id.clone(),
        record_type: id.table().as_str().to_owned(),
        role: "citation".into(),
        observed_updated_at,
    }
}

// Each typed equality check occupies its own poll frame rather than duplicating
// all record/projection temporaries inside the complete revision traversal.
async fn wiki_typed_dependency_is_current<T>(
    reader: &ReadEligibility,
    dependency: &elephant_types::records::KnowledgeDependency,
    record: T,
    authorize: &ReadAuthorization<'_>,
) -> StorageResult<bool>
where
    T: CurrentRead + elephant_types::records::HasSecurity + PartialEq,
{
    if dependency
        .observed_updated_at
        .is_some_and(|observed| observed != record.read_base().updated_at)
        || Box::pin(project_wiki_input(reader, record.clone(), authorize))
            .await?
            .is_none_or(|current| current != record)
    {
        return Ok(false);
    }
    Ok(true)
}

#[async_trait]
impl CurrentRead for elephant_types::records::KnowledgeViewRevision {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    #[allow(
        clippy::too_many_lines,
        reason = "The bounded visited-set traversal keeps each revision dependency, authorization and current-source check in one response fence."
    )]
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        use std::str::FromStr;
        let mut pending = vec![(self.clone(), vec![self.base.id.clone()])];
        let mut visited = std::collections::BTreeSet::new();
        let mut remaining = 2048_usize;
        while let Some((revision, path)) = pending.pop() {
            if revision.base.space_id != self.base.space_id
                || revision.base.deleted_at.is_some()
                || !authorize(&revision.base)
                || !Box::pin(reader.matches_content_support(&revision.base, &revision)).await?
            {
                return Ok(false);
            }
            if !visited.insert(revision.base.id.clone()) {
                continue;
            }
            // Prose is inseparable from its local citations. Do not remove a
            // citation and leave the claim-bearing prose behind.
            for citation in &revision.citations {
                if let Some(span) = &citation.evidence_span {
                    let Some(evidence) =
                        Box::pin(reader.evidence(&revision.base.space_id, span, authorize)).await?
                    else {
                        return Ok(false);
                    };
                    if citation.source_id.as_ref().is_some_and(|source_id| {
                        evidence
                            .source
                            .as_ref()
                            .map(|source| source.base.id.to_string())
                            .as_ref()
                            != Some(source_id)
                    }) {
                        return Ok(false);
                    }
                    if let Some(id) = &citation.assertion_id {
                        let Ok(id) = elephant_types::RecordId::from_str(id) else {
                            return Ok(false);
                        };
                        let Some(assertion) = Box::pin(
                            AssertionRepository::new(reader.storage.clone())
                                .get(&revision.base.space_id, &id),
                        )
                        .await?
                        else {
                            return Ok(false);
                        };
                        if assertion.evidence != *span
                            || Box::pin(project_wiki_input(reader, assertion, authorize))
                                .await?
                                .is_none()
                        {
                            return Ok(false);
                        }
                    }
                }
            }
            let mut dependencies = revision.dependencies.clone();
            // Record-only citations also carry semantic input; descriptive model
            // receipts must never substitute for a referenced graph record.
            for citation in &revision.citations {
                let Ok(id) = elephant_types::RecordId::from_str(&citation.record_id) else {
                    return Ok(false);
                };
                dependencies.push(wiki_citation_dependency(
                    citation,
                    &id,
                    &revision.dependencies,
                ));
            }
            for dependency in &dependencies {
                remaining = match remaining.checked_sub(1) {
                    Some(value) => value,
                    None => return Ok(false),
                };
                if wiki_descriptor(dependency) {
                    continue;
                }
                let Ok(id) = elephant_types::RecordId::from_str(&dependency.record_id) else {
                    return Ok(false);
                };
                if dependency.record_type != id.table().as_str() {
                    return Ok(false);
                }
                macro_rules! validate {
                    ($repo:ident) => {{
                        let Some(record) = Box::pin(
                            crate::$repo::new(reader.storage.clone())
                                .get(&revision.base.space_id, &id),
                        )
                        .await?
                        else {
                            return Ok(false);
                        };
                        if !Box::pin(wiki_typed_dependency_is_current(
                            reader, dependency, record, authorize,
                        ))
                        .await?
                        {
                            return Ok(false);
                        }
                    }};
                }
                match id.table() {
                    TableName::Source => {
                        let Some(record) = Box::pin(
                            SourceRepository::new(reader.storage.clone())
                                .get(&revision.base.space_id, &id),
                        )
                        .await?
                        else {
                            return Ok(false);
                        };
                        let Some(published) =
                            Box::pin(reader.project(record.clone(), authorize)).await?
                        else {
                            return Ok(false);
                        };
                        if !wiki_source_dependency_matches(
                            dependency.observed_updated_at,
                            &record,
                            &published,
                        ) {
                            return Ok(false);
                        }
                    }
                    TableName::Doc => validate!(DocRepository),
                    TableName::DocRevision => validate!(DocRevisionRepository),
                    TableName::DocNode => {
                        let Some(record) = Box::pin(
                            crate::DocNodeRepository::new(reader.storage.clone())
                                .get(&revision.base.space_id, &id),
                        )
                        .await?
                        else {
                            return Ok(false);
                        };
                        if dependency
                            .observed_updated_at
                            .is_some_and(|time| time != record.base.updated_at)
                            || Box::pin(reader.project(record, authorize)).await?.is_none()
                        {
                            return Ok(false);
                        }
                        // Safe text adornment comes from the verified immutable
                        // revision; project already checked the stored row proof.
                    }
                    TableName::Entity => validate!(EntityRepository),
                    TableName::Assertion => validate!(AssertionRepository),
                    TableName::Rel => validate!(RelRepository),
                    TableName::Attribute => validate!(AttributeRepository),
                    TableName::Event => validate!(EventRepository),
                    TableName::TruthSlot => validate!(TruthSlotRepository),
                    TableName::Freshness => validate!(FreshnessRepository),
                    TableName::SourceWatch => validate!(SourceWatchRepository),
                    TableName::SourceWatchCheck => {
                        let Some(record) = Box::pin(
                            crate::SourceWatchRepository::new(reader.storage.clone())
                                .get_check(&revision.base.space_id, &id),
                        )
                        .await?
                        else {
                            return Ok(false);
                        };
                        if dependency
                            .observed_updated_at
                            .is_some_and(|observed| observed != record.base.updated_at)
                            || Box::pin(reader.project(record, authorize)).await?.is_none()
                        {
                            return Ok(false);
                        }
                    }
                    TableName::KnowledgeViewDefinition => {
                        validate!(KnowledgeViewDefinitionRepository);
                    }
                    TableName::KnowledgeView | TableName::KnowledgeViewRevision => {
                        let repository =
                            crate::KnowledgeViewRepository::new(reader.storage.clone());
                        let nested = if id.table() == TableName::KnowledgeView {
                            let Some(view) =
                                Box::pin(repository.get_view(&revision.base.space_id, &id)).await?
                            else {
                                return Ok(false);
                            };
                            if view.base.deleted_at.is_some()
                                || !authorize(&view.base)
                                || !Box::pin(reader.matches_content_support(&view.base, &view))
                                    .await?
                                || dependency
                                    .observed_updated_at
                                    .is_some_and(|time| time != view.base.updated_at)
                            {
                                return Ok(false);
                            }
                            let Some(nested) =
                                Box::pin(repository.get_current_revision(&view)).await?
                            else {
                                return Ok(false);
                            };
                            if nested.view_id != view.base.id.to_string() {
                                return Ok(false);
                            }
                            nested
                        } else {
                            let Some(nested) =
                                Box::pin(repository.get_revision(&revision.base.space_id, &id))
                                    .await?
                            else {
                                return Ok(false);
                            };
                            if dependency
                                .observed_updated_at
                                .is_some_and(|time| time != nested.base.updated_at)
                            {
                                return Ok(false);
                            }
                            nested
                        };
                        if !wiki_path_allows(&path, &nested.base.id) {
                            return Ok(false);
                        }
                        let mut nested_path = path.clone();
                        nested_path.push(nested.base.id.clone());
                        drop(repository);
                        pending.push((nested, nested_path));
                    }
                    _ => return Ok(false),
                }
            }
        }
        Ok(true)
    }
}

#[async_trait]
impl CurrentRead for elephant_types::records::TruthSlot {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        use std::str::FromStr;
        let old_len = self.current_values.len();
        let snapshot_certified = self
            .current_values
            .iter()
            .flat_map(|value| &value.evidence)
            .any(|link| link.span.start == 0 && link.span.end == 0)
            && reader
                .snapshot_graph_evidence(&self.base, authorize)
                .await?;
        let mut evidence_changed = false;
        let mut current = Vec::new();
        for mut value in self.current_values.drain(..) {
            let Ok(id) = elephant_types::RecordId::from_str(&value.claim_id) else {
                continue;
            };
            macro_rules! current_claim {
                ($repo:ident) => {
                    match crate::$repo::new(reader.storage.clone())
                        .get(&self.base.space_id, &id)
                        .await?
                    {
                        Some(claim) => reader.project(claim, authorize).await?.is_some(),
                        None => false,
                    }
                };
            }
            let eligible = match id.table() {
                TableName::Rel => current_claim!(RelRepository),
                TableName::Attribute => current_claim!(AttributeRepository),
                TableName::Event => current_claim!(EventRepository),
                _ => false,
            };
            if eligible {
                let before = value.evidence.len();
                reader
                    .retain_evidence(
                        &self.base.space_id,
                        &mut value.evidence,
                        snapshot_certified,
                        authorize,
                    )
                    .await?;
                evidence_changed |= before != value.evidence.len();
                current.push(value);
            }
        }
        if current.len() != old_len || evidence_changed {
            // The old rationale may quote a withdrawn alternative.
            self.rationale = None;
            self.conflict_group = None;
            self.stale = true;
            self.stale_reason = Some("Source support changed".into());
        }
        self.current_values = current;
        Ok(!self.current_values.is_empty())
    }
}

macro_rules! watched_source_record {
    ($($record:ty),* $(,)?) => {$(
        #[async_trait]
        impl CurrentRead for $record {
            fn read_base(&self) -> &BaseFields { &self.base }
            async fn retain_current(&mut self, reader: &ReadEligibility, authorize: &ReadAuthorization<'_>) -> StorageResult<bool> {
                let Some(source) = SourceRepository::new(reader.storage.clone()).get(&self.base.space_id,self.source.id()).await? else {return Ok(false);};
                reader.source(&source,authorize).await
            }
        }
    )*};
}
watched_source_record!(
    elephant_types::records::SourceWatch,
    elephant_types::records::SourceWatchCheck
);

#[async_trait]
impl CurrentRead for elephant_types::records::Freshness {
    fn read_base(&self) -> &BaseFields {
        &self.base
    }
    async fn retain_current(
        &mut self,
        reader: &ReadEligibility,
        authorize: &ReadAuthorization<'_>,
    ) -> StorageResult<bool> {
        use std::str::FromStr;
        let Ok(id) = elephant_types::RecordId::from_str(&self.target) else {
            return Ok(false);
        };
        macro_rules! target {
            ($repo:ident) => {{
                let Some(record) = crate::$repo::new(reader.storage.clone())
                    .get(&self.base.space_id, &id)
                    .await?
                else {
                    return Ok(false);
                };
                let Some(current) = reader.project(record.clone(), authorize).await? else {
                    return Ok(false);
                };
                if current != record {
                    self.rationale = None;
                }
                true
            }};
        }
        Ok(match id.table() {
            TableName::Entity => target!(EntityRepository),
            TableName::Rel => target!(RelRepository),
            TableName::Attribute => target!(AttributeRepository),
            TableName::Event => target!(EventRepository),
            TableName::Assertion => target!(AssertionRepository),
            _ => false,
        })
    }
}

#[cfg(test)]
#[path = "read_eligibility_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "historical_support_read_tests.rs"]
mod historical_support_tests;
