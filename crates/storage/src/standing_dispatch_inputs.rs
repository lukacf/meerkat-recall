//! Read-only dispatch input capture under the intake owner's desired-document selector.
//!
//! This module is a child of `source_document_guards`, re-exported by storage. It adds no
//! persisted permission, source operation, or publication authority.

use super::PreparedDocGuards;
use crate::canonical_commit::{source_support_fingerprint, CanonicalRecord, CanonicalRepository};
use crate::source_intake::current_watch_document_guards;
use crate::source_publication::SourcePublicationRepository;
use crate::{Repository, SourceRepository, StorageClient, StorageError, StorageResult};
use elephant_types::publication::{SourceHead, SourceVisibility};
use elephant_types::{RecordId, TableName};
use std::collections::BTreeMap;

/// Exact original current inputs and membership observations. Callers separately
/// authorize every returned input under all applicable original principals.
#[derive(Clone)]
pub struct PreparedStandingDispatchInputs {
    space: String,
    source_id: RecordId,
    epoch: String,
    head: Option<SourceHead>,
    documents: PreparedDocGuards,
    records: BTreeMap<RecordId, CanonicalRecord>,
}

impl std::fmt::Debug for PreparedStandingDispatchInputs {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedStandingDispatchInputs")
            .field("input_count", &self.records.len())
            .finish_non_exhaustive()
    }
}

fn changed() -> StorageError {
    StorageError::Conflict("standing_dispatch_inputs_changed_or_missing".into())
}

impl PreparedStandingDispatchInputs {
    /// Capture the actual Source and current desired-input population.
    ///
    /// The
    /// canonical epoch precedes every read, including membership discovery.
    /// # Errors
    /// Refuses a missing, foreign, erased, retracted, changed or invalid input.
    pub async fn capture(
        client: &StorageClient,
        space: &str,
        source_id: &RecordId,
    ) -> StorageResult<Self> {
        if source_id.table() != TableName::Source {
            return Err(changed());
        }
        let epoch = CanonicalRepository::new(client.clone())
            .current_epoch(space)
            .await?;
        let source = SourceRepository::new(client.clone())
            .get(space, source_id)
            .await?
            .ok_or_else(changed)?;
        if source.base.space_id != space || source.base.deleted_at.is_some() {
            return Err(changed());
        }
        let head = SourcePublicationRepository::new(client.clone())
            .head(space, source_id)
            .await?;
        if head.as_ref().is_some_and(|head| {
            head.space_id != space
                || head.source_id != *source_id
                || head.lifecycle != SourceVisibility::Active
        }) {
            return Err(changed());
        }
        // Reuse the SAME desired-template/closed-manifest/absent-Doc selector
        // used by source intake and Watch registration. Never scan historical
        // documents or replace this original head with a newer observation.
        let (documents, guards) = if let Some(original) = &head {
            current_watch_document_guards(client, original).await?
        } else {
            (
                Vec::new(),
                PreparedDocGuards::from_authorized_documents(space, source_id, None, &[])?,
            )
        };
        let mut prepared = Self {
            space: space.into(),
            source_id: source_id.clone(),
            epoch,
            head,
            documents: guards,
            records: BTreeMap::new(),
        };
        prepared.retain(CanonicalRecord::Source(Box::new(source)))?;
        for document in documents {
            prepared.retain(CanonicalRecord::Doc(Box::new(document)))?;
        }
        if let Some((_, Some(manifest))) = &prepared.documents.manifest {
            let selected =
                crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                    client.clone(),
                )
                .declared_input_records(manifest)
                .await?;
            for record in selected {
                prepared.retain(record)?;
            }
        }
        prepared.verify_current(client).await?;
        Ok(prepared)
    }

    /// Every exact original persisted input, deduplicated without truncation.
    pub fn records(&self) -> impl Iterator<Item = &CanonicalRecord> {
        self.records.values()
    }

    /// The canonical owner reuses these exact observations inside its original
    /// epoch transaction, including absent Docs and original manifest presence.
    pub(crate) const fn document_guards(&self) -> &PreparedDocGuards {
        &self.documents
    }

    fn retain(&mut self, record: CanonicalRecord) -> StorageResult<()> {
        if record.base().space_id != self.space || record.base().deleted_at.is_some() {
            return Err(changed());
        }
        if let Some(previous) = self.records.get(&record.base().id) {
            if source_support_fingerprint(previous)? != source_support_fingerprint(&record)? {
                return Err(changed());
            }
        } else {
            self.records.insert(record.base().id.clone(), record);
        }
        Ok(())
    }

    /// Recheck original rows, head presence/absence, current desired membership,
    /// expected absent Docs, manifest presence/content, and the original epoch.
    ///
    /// This does not mutate storage or refresh any captured authorization.
    /// # Errors
    /// Refuses every changed original observation, including a newly staged Doc.
    pub async fn verify_current(&self, client: &StorageClient) -> StorageResult<()> {
        if SourcePublicationRepository::new(client.clone())
            .head(&self.space, &self.source_id)
            .await?
            != self.head
        {
            return Err(changed());
        }
        if self.head.is_none()
            && crate::private_control_read::global_exists(
                client,
                crate::source_publication::HEAD_TABLE,
                &crate::source_publication::head_key(&self.space, &self.source_id),
            )
            .await?
        {
            return Err(changed());
        }
        for original in self.records.values() {
            let current = CanonicalRecord::load(client, &self.space, &original.base().id)
                .await?
                .ok_or_else(changed)?;
            if source_support_fingerprint(&current)? != source_support_fingerprint(original)? {
                return Err(changed());
            }
        }
        // Absence is global record identity, just as in PreparedDocGuards'
        // same-transaction guard; a foreign row cannot satisfy an absent Doc.
        for id in &self.documents.absent_documents {
            if crate::DocRepository::new(client.clone())
                .exists_globally(id)
                .await?
            {
                return Err(changed());
            }
        }
        if let Some((publication, original)) = &self.documents.manifest {
            let current =
                crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                    client.clone(),
                )
                .get(&self.space, publication)
                .await?;
            if &current != original {
                return Err(changed());
            }
            if original.is_none()
                && crate::private_control_read::global_exists(
                    client,
                    crate::source_publication_manifest::MANIFEST_TABLE,
                    &crate::source_publication_manifest::manifest_key(&self.space, publication),
                )
                .await?
            {
                return Err(changed());
            }
        }
        if CanonicalRepository::new(client.clone())
            .current_epoch(&self.space)
            .await?
            != self.epoch
        {
            return Err(changed());
        }
        Ok(())
    }
}
