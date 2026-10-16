//! Exact imported snapshot evidence can make a source-less semantic assertion
//! visible without inventing a text span, worker history or original author.
use super::{
    BaseFields, CurrentRead, ReadAuthorization, ReadEligibility,
    SourcePublicationManifestRepository, StorageResult,
};
impl ReadEligibility {
    #[inline(never)]
    pub(super) fn snapshot_graph_evidence<'a>(
        &'a self,
        base: &'a BaseFields,
        authorize: &'a ReadAuthorization<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<bool>> + Send + 'a>> {
        Box::pin(async move {
            self.observe_snapshot_epoch(&base.space_id).await?;
            if !base.provenance.sources.is_empty() || !self.base_support(base, authorize).await? {
                return Ok(false);
            }
            for support in self.current_support(&base.space_id, &base.id).await? {
                if !self.historical_support_is_current(&support).await? {
                    continue;
                }
                let Some(head) = self.head(&base.space_id, &support.source_id).await? else {
                    continue;
                };
                let publication = elephant_types::publication::PublicationFence {
                    source_id: support.source_id.clone(),
                    generation: head.generation,
                    revision_id: support.source_revision.clone(),
                    operation_id: head.operation_id,
                };
                let Some(manifest) = SourcePublicationManifestRepository::new(self.storage.clone())
                    .get(&base.space_id, &publication)
                    .await?
                else {
                    continue;
                };
                manifest.validate()?;
                let Some(snapshot) = manifest
                    .historical_adoption
                    .as_ref()
                    .and_then(|proof| proof.snapshot_graph.as_ref())
                else {
                    continue;
                };
                if snapshot.original_rows_sha256.contains_key(&base.id) {
                    return self
                        .snapshot_reference_closure(base, snapshot, authorize)
                        .await;
                }
            }
            Ok(false)
        })
    }

    /// Bounded, cycle-safe typed closure. Reference authorization is conjunctive;
    /// a retained snapshot fingerprint never authorizes a denied dependency.
    #[inline(never)]
    fn snapshot_reference_closure<'a>(
        &'a self,
        root: &'a BaseFields,
        snapshot: &'a crate::historical_adoption::HistoricalSnapshotGraphProof,
        authorize: &'a ReadAuthorization<'a>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<bool>> + Send + 'a>> {
        Box::pin(async move {
            use crate::canonical_commit::CanonicalRecord;
            use crate::historical_adoption::historical_snapshot_links;
            let mut pending = std::collections::VecDeque::from([root.id.clone()]);
            let mut visited = std::collections::BTreeSet::new();
            let mut bytes = 0usize;
            while let Some(id) = pending.pop_front() {
                if !visited.insert(id.clone()) {
                    continue;
                }
                if visited.len() > 8192 {
                    return Ok(false);
                }
                let Some(record) =
                    CanonicalRecord::load(&self.storage, &root.space_id, &id).await?
                else {
                    return Ok(false);
                };
                let base = record.base();
                if base.space_id != root.space_id || base.deleted_at.is_some() || !authorize(base) {
                    return Ok(false);
                }
                bytes = bytes.saturating_add(
                    serde_json::to_vec(&record)
                        .map_err(|_| {
                            crate::StorageError::DataIntegrityError(
                                "snapshot reference encode".into(),
                            )
                        })?
                        .len(),
                );
                if bytes > 128 * 1024 * 1024 {
                    return Ok(false);
                }
                let supports = self.current_support(&root.space_id, &id).await?;
                let fingerprint = record.fingerprint()?;
                if snapshot.reference_fingerprints.get(&id) != Some(&fingerprint) {
                    return Ok(false);
                }
                if supports.iter().any(|support| {
                    support.record_id != id
                        || support.space_id != root.space_id
                        || support.record_fingerprint != fingerprint
                }) || (crate::canonical_commit::is_derived_graph_table(id.table())
                    && !self.base_support(base, authorize).await?)
                {
                    return Ok(false);
                }
                match &record {
                    CanonicalRecord::Source(source) => {
                        if !self.source(source, authorize).await? {
                            return Ok(false);
                        }
                    }
                    CanonicalRecord::Doc(doc) => {
                        if !self.document(doc, authorize).await? {
                            return Ok(false);
                        }
                    }
                    CanonicalRecord::DocRevision(revision) => {
                        if self.revision(revision, authorize).await?.is_none() {
                            return Ok(false);
                        }
                    }
                    CanonicalRecord::DocNode(node) => {
                        let mut node = node.as_ref().clone();
                        if !node.retain_current(self, authorize).await? {
                            return Ok(false);
                        }
                    }
                    CanonicalRecord::Assertion(assertion) => {
                        if !(assertion.evidence.start == 0 && assertion.evidence.end == 0)
                            && self
                                .evidence(&root.space_id, &assertion.evidence, authorize)
                                .await?
                                .is_none()
                        {
                            return Ok(false);
                        }
                    }
                    _ => (),
                }
                let links = historical_snapshot_links(&record)?;
                if snapshot.typed_references.get(&id) != Some(&links) {
                    return Ok(false);
                }
                if id == root.id && !snapshot.original_rows_sha256.contains_key(&id) {
                    return Ok(false);
                }
                pending.extend(links);
                if pending.len() > 32768 {
                    return Ok(false);
                }
            }
            Ok(true)
        })
    }
}
