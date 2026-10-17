//! Bounded initial bindings retain transfer custody through both full checks.

use super::{
    HistoricalAdoptionReceipt, HistoricalAdoptionRepository, HistoricalInitialCatchupSource,
};
use crate::canonical_commit::CanonicalRepository;
use crate::erasure_journal::ArchiveRegistryEntry;
use crate::source_publication::integrity;
use crate::{StorageClient, StorageResult};
use std::collections::BTreeSet;

/// Maximum receipt count in one all-or-error initial binding batch.
pub const MAX_INITIAL_BINDING_BATCH_RECEIPTS: usize = 256;
/// Maximum total encoded receipt bytes in one initial binding batch.
pub const MAX_INITIAL_BINDING_BATCH_BYTES: usize = 16 * 1024 * 1024;

#[derive(PartialEq, Eq)]
struct Checkpoint {
    journal_identity: String,
    reference_key_fingerprint: String,
    transfer_source: String,
    transfer_started: chrono::DateTime<chrono::Utc>,
    archive_entries: Vec<ArchiveRegistryEntry>,
    archive_heads: Vec<ArchiveRegistryEntry>,
    epoch: String,
}

impl Checkpoint {
    async fn capture(
        client: &StorageClient,
        original: &HistoricalAdoptionReceipt,
    ) -> StorageResult<Self> {
        client.require_historical_transfer(&original.original_snapshot_sha256)?;
        let epoch = CanonicalRepository::new(client.clone())
            .observe_epoch(&original.space_id)
            .await?
            .ok_or_else(|| integrity("initial binding epoch absent"))?;
        // Epoch observation is the last await. Recheck the original lease and
        // capture synchronous marker/journal/archive state only afterward.
        client.require_historical_transfer(&original.original_snapshot_sha256)?;
        let journal = client.erasure_journal()?;
        if !journal.intents()?.is_empty() || journal.restore_marker()?.is_some() {
            return Err(integrity("initial binding maintenance authority changed"));
        }
        let marker = journal
            .transfer_marker()?
            .ok_or_else(|| integrity("initial binding transfer missing"))?;
        let archive_entries = journal.operational_archive_entries()?;
        let archive_heads = journal.operational_archive_heads()?;
        Ok(Self {
            journal_identity: journal.identity().to_owned(),
            reference_key_fingerprint: journal.reference_key_fingerprint().to_owned(),
            transfer_source: marker.source_sha256,
            transfer_started: marker.started_at,
            archive_entries,
            archive_heads,
            epoch,
        })
    }
}

fn validate_batch(
    originals: &[HistoricalAdoptionReceipt],
) -> StorageResult<&HistoricalAdoptionReceipt> {
    let original = originals
        .first()
        .ok_or_else(|| integrity("initial binding batch empty"))?;
    if originals.len() > MAX_INITIAL_BINDING_BATCH_RECEIPTS {
        return Err(integrity("initial binding batch count exceeded"));
    }
    let mut bytes = 0usize;
    let mut sources = BTreeSet::new();
    for item in originals {
        if item.space_id != original.space_id
            || item.original_snapshot_sha256 != original.original_snapshot_sha256
            || !sources.insert(&item.source_id)
        {
            return Err(integrity("initial binding batch identity differs"));
        }
        bytes = bytes
            .checked_add(
                serde_json::to_vec(item)
                    .map_err(|error| integrity(&error.to_string()))?
                    .len(),
            )
            .ok_or_else(|| integrity("initial binding batch size overflow"))?;
        if bytes > MAX_INITIAL_BINDING_BATCH_BYTES {
            return Err(integrity("initial binding batch bytes exceeded"));
        }
    }
    Ok(original)
}

impl HistoricalAdoptionRepository {
    /// Read an ordered, bounded binding batch under the original transfer lease.
    ///
    /// Full archive bytes and native authority are checked at both boundaries.
    /// No binding is returned unless the exact marker, journal, archive entries,
    /// heads and canonical epoch remain unchanged throughout the batch.
    ///
    /// # Errors
    /// Refuses invalid bounds, duplicate sources, changed custody, archive bytes,
    /// native authority, epoch, or any original receipt and source-head mismatch.
    pub async fn initial_catchup_bindings(
        &self,
        originals: &[HistoricalAdoptionReceipt],
    ) -> StorageResult<Vec<HistoricalInitialCatchupSource>> {
        #[cfg(test)]
        let full_verifications = std::sync::atomic::AtomicUsize::new(0);
        Box::pin(self.initial_catchup_bindings_checked(
            originals,
            #[cfg(test)]
            std::future::ready(Ok(())),
            #[cfg(test)]
            &full_verifications,
        ))
        .await
    }

    async fn initial_catchup_bindings_checked(
        &self,
        originals: &[HistoricalAdoptionReceipt],
        #[cfg(test)] before_finish: impl std::future::Future<Output = StorageResult<()>>,
        #[cfg(test)] full_verifications: &std::sync::atomic::AtomicUsize,
    ) -> StorageResult<Vec<HistoricalInitialCatchupSource>> {
        let original = validate_batch(originals)?;
        let checkpoint = Checkpoint::capture(self.client(), original).await?;
        Box::pin(self.client().verify_journal_identity()).await?;
        #[cfg(test)]
        full_verifications.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if Checkpoint::capture(self.client(), original).await? != checkpoint {
            return Err(integrity(
                "initial binding authority changed during verification",
            ));
        }
        let mut bindings = Vec::with_capacity(originals.len());
        for item in originals {
            bindings.push(Box::pin(self.initial_catchup_binding_verified(item)).await?);
        }
        #[cfg(test)]
        before_finish.await?;
        if Checkpoint::capture(self.client(), original).await? != checkpoint {
            return Err(integrity("initial binding authority changed"));
        }
        Box::pin(self.client().verify_journal_identity()).await?;
        #[cfg(test)]
        full_verifications.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if Checkpoint::capture(self.client(), original).await? != checkpoint {
            return Err(integrity(
                "initial binding authority changed during final verification",
            ));
        }
        Ok(bindings)
    }
}

#[cfg(test)]
#[path = "historical_initial_bindings_tests.rs"]
mod tests;
