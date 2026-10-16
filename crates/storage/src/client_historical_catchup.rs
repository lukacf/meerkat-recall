//! Exclusive nonempty-target custody for an explicitly bound historical delta.
//! This is distinct from fresh-target transfer and preserves every erasure intent.
use super::{RecoveryPhase, StorageClient, StorageError, StorageResult};
use crate::canonical_commit::{CanonicalCommitReceipt, CanonicalRepository};
use crate::source_publication::SourcePublicationRepository;
use elephant_types::publication::SourceHead;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[path = "historical_catchup_journal.rs"]
mod journal;
use journal::CatchupMarker;
#[path = "historical_archive_catchup.rs"]
mod archive_catchup;
#[path = "historical_catchup_completion.rs"]
pub mod completion;
#[path = "historical_regeneration_chain.rs"]
mod regeneration_chain;
pub use archive_catchup::HistoricalArchiveCandidate;

/// Exact current adoption and head expected before entering catch-up.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalCatchupSource {
    /// Source identity established by the original adoption.
    pub source_id: elephant_types::RecordId,
    /// Exact original adoption commit ID; this is not a digest.
    pub commit_id: String,
    /// SHA-256 of the typed original canonical commit receipt.
    pub receipt_sha256: String,
    /// Complete original published source head required for admission.
    pub head: SourceHead,
}

/// Input declarations are not maintenance authority.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalCatchupInput {
    /// Version of this serialized declaration.
    pub version: u32,
    /// Identity of the original target recovery journal.
    pub journal_identity: String,
    /// Digest identifying the complete immutable catch-up input.
    pub migration_id: String,
    /// Original snapshot namespace before the declared relocation.
    pub source_space_id: String,
    /// Destination namespace covered by this declaration.
    pub space_id: String,
    /// SHA-256 of the complete original S0 snapshot file.
    pub before_snapshot_sha256: String,
    /// SHA-256 of the complete successor S1 snapshot file.
    pub after_snapshot_sha256: String,
    /// SHA-256 binding the closed offline delta plan.
    pub delta_sha256: String,
    /// SHA-256 of the immutable initial or completed predecessor report.
    pub original_report_sha256: String,
    /// SHA-256 of the closed predecessor SQLite inventory.
    pub baseline_ledger_sha256: String,
    /// Original all-table native row digest, before combining closure proofs.
    pub baseline_native_rows_sha256: String,
    /// Caller acceptance of declared removals and soft deletions in this operation.
    pub accept_deletions: bool,
    /// Exact destination epoch required before this transition.
    pub expected_epoch: String,
    /// Complete number of ordered row batches.
    pub batches: u64,
    /// Digest of the complete ordered S1 publication regeneration plan.
    pub regeneration_plan_sha256: String,
    /// Complete number of publication regeneration or retirement steps.
    pub regeneration_steps: u64,
    /// Exact original adoption receipt and head bindings for the population.
    pub sources: Vec<HistoricalCatchupSource>,
}

/// Non-cloneable and non-serializable; created only inside exclusive custody.
#[derive(Debug)]
pub struct PreparedHistoricalCatchupAuthority {
    client: StorageClient,
    marker: CatchupMarker,
    next_batch: u64,
    last_receipt: Option<String>,
    epoch: String,
    original_epoch: String,
    original_sources: Vec<HistoricalCatchupSource>,
    recovered_regeneration: Vec<CanonicalCommitReceipt>,
    archive_receipt: Option<CanonicalCommitReceipt>,
    retained_operational: tokio::sync::OnceCell<completion::RetainedOperationalBaseline>,
}

/// All declared row batches are durably chained. This is still maintenance
/// custody, not proof of final S1 inventory or a permission to restore readiness.
#[derive(Debug)]
pub struct SealedHistoricalCatchupAuthority {
    inner: PreparedHistoricalCatchupAuthority,
    regeneration_anchor: String,
}
impl SealedHistoricalCatchupAuthority {
    /// Consume sealed batch custody into the complete source-owned S1 plan.
    /// The administrative client never escapes to the command adapter.
    pub async fn prepare_regeneration(
        self,
        input: crate::historical_adoption::HistoricalAdoptionInput,
    ) -> StorageResult<crate::historical_adoption::PreparedHistoricalAuthority> {
        let repository =
            crate::historical_adoption::HistoricalAdoptionRepository::new(self.client()?.clone());
        repository.prepare_catchup_authority(input, self).await
    }
    pub(crate) fn client(&self) -> StorageResult<&StorageClient> {
        self.inner.client()
    }
    pub(crate) fn check_custody(&self) -> StorageResult<()> {
        self.inner.check_custody()
    }
    pub(crate) fn source_space_id(&self) -> &str {
        &self.inner.marker.source_space_id
    }
    pub(crate) fn space_id(&self) -> &str {
        &self.inner.marker.space_id
    }
    pub(crate) fn after_snapshot_sha256(&self) -> &str {
        &self.inner.marker.after_snapshot_sha256
    }
    pub(crate) fn regeneration_binding(&self) -> (&str, &str) {
        (
            &self.inner.marker.authority_binding_sha256,
            &self.regeneration_anchor,
        )
    }
    pub(crate) fn regeneration_plan_binding(&self) -> (&str, u64) {
        (
            &self.inner.marker.regeneration_plan_sha256,
            self.inner.marker.regeneration_steps,
        )
    }
    pub(crate) fn recovered_regeneration(&self) -> &[CanonicalCommitReceipt] {
        &self.inner.recovered_regeneration
    }
    pub(crate) fn regeneration_start_epoch(&self) -> &str {
        self.inner
            .recovered_regeneration
            .first()
            .and_then(|receipt| receipt.replay_output.as_ref())
            .and_then(|state| state.get("before_epoch"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or(&self.inner.epoch)
    }
    pub(crate) fn regeneration_epoch(&self) -> &str {
        &self.inner.epoch
    }
    pub(crate) fn ledger_binding(&self) -> (&str, &str, &str, &str, &str, u64, u64) {
        self.inner.ledger_binding()
    }
    pub(crate) fn complete_after_inventory(
        self,
        final_epoch: String,
        receipt: String,
    ) -> StorageResult<CompletedHistoricalCatchup> {
        self.check_custody()?;
        if !journal::valid_digest(&receipt) {
            return Err(StorageError::Conflict(
                "inventory completion digest invalid".into(),
            ));
        }
        Ok(CompletedHistoricalCatchup {
            marker: self.inner.marker,
            final_epoch,
            receipt,
            baseline: None,
        })
    }
    pub(crate) fn certified_empty_baseline_configuration_id(
        &self,
    ) -> StorageResult<Option<elephant_types::RecordId>> {
        self.inner.certified_empty_baseline_configuration_id()
    }
    pub(crate) fn receipt_source_id(&self) -> StorageResult<elephant_types::RecordId> {
        self.inner
            .receipt_sources()?
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::Conflict("receipt scope absent".into()))
    }
    pub(crate) fn original_sources(&self) -> &[HistoricalCatchupSource] {
        &self.inner.original_sources
    }
}

/// Constructed only after executor receipts and final closure are reconciled.
/// An arbitrary callback cannot claim a successful catch-up.
#[derive(Debug)]
pub struct CompletedHistoricalCatchup {
    marker: CatchupMarker,
    final_epoch: String,
    receipt: String,
    pub(crate) baseline: Option<completion::Baseline>,
}

/// Exact full canonical wrapper; payload equality alone must never authorize
/// writes after native projections or opaque wrapper fields were altered.
async fn exact_receipt(
    client: &StorageClient,
    space: &str,
    commit: &str,
) -> StorageResult<CanonicalCommitReceipt> {
    use crate::historical_adoption::catchup::{full_control, HistoricalControlImage};
    let key = crate::source_publication::stable_key(&[space, commit]);
    let image = full_control(client, "source_commit_receipt", &key, space).await?;
    let receipt: CanonicalCommitReceipt = image.payload()?;
    let source = receipt
        .source_id
        .as_ref()
        .or_else(|| receipt.source_ids.first())
        .ok_or_else(|| StorageError::Conflict("receipt source absent".into()))?;
    if receipt.space_id != space || receipt.commit_id != commit {
        return Err(StorageError::Conflict("receipt identity differs".into()));
    }
    let expected = serde_json::json!({"id":format!("source_commit_receipt:{key}"),"space_id":space,"source_id":source.to_string(),"payload":serde_json::to_string(&receipt).map_err(crate::durability_io::io_error)?,"state":serde_json::to_value(&receipt).map_err(crate::durability_io::io_error)?});
    let matches = match image {
        HistoricalControlImage::Embedded(actual) => actual == expected,
        HistoricalControlImage::Native(actual) => {
            let surrealdb::sql::Value::Object(mut fields) =
                crate::surreal::json_to_surreal(expected)?
            else {
                return Err(StorageError::Conflict("receipt wrapper shape".into()));
            };
            fields.insert(
                "id".into(),
                crate::source_publication::control_thing("source_commit_receipt", &key).into(),
            );
            actual == crate::surreal::stored_native_value(surrealdb::sql::Value::Object(fields))
        }
    };
    if !matches {
        return Err(StorageError::Conflict(
            "full immutable receipt changed".into(),
        ));
    }
    Ok(receipt)
}

impl HistoricalCatchupInput {
    fn validate(&self, client: &StorageClient) -> StorageResult<CatchupMarker> {
        if self.version != 1
            || self.expected_epoch.is_empty()
            || self.source_space_id.is_empty()
            || self.sources.len() > 100_000
        {
            return Err(StorageError::DataIntegrityError(
                "invalid historical catch-up declaration".into(),
            ));
        }
        if self.sources.is_empty() {
            completion::verify_empty_baseline(client.erasure_journal()?, self)?;
        }
        let mut sources = std::collections::BTreeSet::new();
        for source in &self.sources {
            if !sources.insert(&source.source_id)
                || source.source_id.table() != elephant_types::TableName::Source
                || source.head.source_id != source.source_id
                || source.head.space_id != self.space_id
                || source.commit_id.is_empty()
                || !journal::valid_digest(&source.receipt_sha256)
            {
                return Err(StorageError::DataIntegrityError(
                    "invalid catch-up source binding".into(),
                ));
            }
        }
        let bytes = serde_json::to_vec(self).map_err(crate::durability_io::io_error)?;
        if bytes.len() > 64 * 1024 * 1024 {
            return Err(StorageError::DataIntegrityError(
                "catch-up binding byte bound".into(),
            ));
        }
        let marker = CatchupMarker {
            version: 1,
            journal_identity: self.journal_identity.clone(),
            migration_id: self.migration_id.clone(),
            source_space_id: self.source_space_id.clone(),
            space_id: self.space_id.clone(),
            before_snapshot_sha256: self.before_snapshot_sha256.clone(),
            after_snapshot_sha256: self.after_snapshot_sha256.clone(),
            delta_sha256: self.delta_sha256.clone(),
            original_report_sha256: self.original_report_sha256.clone(),
            baseline_ledger_sha256: self.baseline_ledger_sha256.clone(),
            baseline_native_rows_sha256: self.baseline_native_rows_sha256.clone(),
            accept_deletions: self.accept_deletions,
            authority_binding_sha256: hex::encode(Sha256::digest(bytes)),
            batches: self.batches,
            regeneration_plan_sha256: self.regeneration_plan_sha256.clone(),
            regeneration_steps: self.regeneration_steps,
        };
        marker.validate(client.erasure_journal()?)?;
        Ok(marker)
    }

    async fn verify_current(&self, client: &StorageClient) -> StorageResult<()> {
        client.verify_journal_identity().await?;
        self.verify_original_receipts(client).await?;
        if CanonicalRepository::new(client.clone())
            .observe_epoch(&self.space_id)
            .await?
            .as_deref()
            != Some(self.expected_epoch.as_str())
        {
            return Err(StorageError::Conflict(
                "catch-up target epoch changed".into(),
            ));
        }
        for source in &self.sources {
            if SourcePublicationRepository::new(client.clone())
                .head(&self.space_id, &source.source_id)
                .await?
                .as_ref()
                != Some(&source.head)
            {
                return Err(StorageError::Conflict(
                    "catch-up source head changed".into(),
                ));
            }
        }
        if CanonicalRepository::new(client.clone())
            .observe_epoch(&self.space_id)
            .await?
            .as_deref()
            != Some(self.expected_epoch.as_str())
        {
            return Err(StorageError::Conflict(
                "catch-up target changed during binding".into(),
            ));
        }
        Ok(())
    }

    // Callers verify the original journal identity before these exact receipt
    // reads. Pending replay separately closes archive heads before any callback.
    async fn verify_original_receipts(&self, client: &StorageClient) -> StorageResult<()> {
        for source in &self.sources {
            let receipt = exact_receipt(client, &self.space_id, &source.commit_id).await?;
            let provenance = receipt.historical_import.as_ref().ok_or_else(|| {
                StorageError::Conflict("original historical provenance missing".into())
            })?;
            let original_snapshot_matches =
                provenance.snapshot_sha256 == self.before_snapshot_sha256;
            if receipt.space_id != self.space_id
                || receipt.commit_id != source.commit_id
                || !original_snapshot_matches
                || hex::encode(Sha256::digest(
                    serde_json::to_vec(&receipt).map_err(crate::durability_io::io_error)?,
                )) != source.receipt_sha256
            {
                return Err(StorageError::Conflict(
                    "original adoption receipt changed".into(),
                ));
            }
        }
        Ok(())
    }
}

impl PreparedHistoricalCatchupAuthority {
    /// Empty original populations require the completed predecessor's immutable
    /// certificate; the returned configuration identity is never read live.
    fn certified_empty_baseline(&self) -> StorageResult<Option<completion::Baseline>> {
        self.check_custody()?;
        if !self.original_sources.is_empty() {
            return Ok(None);
        }
        let input = HistoricalCatchupInput {
            version: 1,
            journal_identity: self.marker.journal_identity.clone(),
            migration_id: self.marker.migration_id.clone(),
            source_space_id: self.marker.source_space_id.clone(),
            space_id: self.marker.space_id.clone(),
            before_snapshot_sha256: self.marker.before_snapshot_sha256.clone(),
            after_snapshot_sha256: self.marker.after_snapshot_sha256.clone(),
            delta_sha256: self.marker.delta_sha256.clone(),
            original_report_sha256: self.marker.original_report_sha256.clone(),
            baseline_ledger_sha256: self.marker.baseline_ledger_sha256.clone(),
            baseline_native_rows_sha256: self.marker.baseline_native_rows_sha256.clone(),
            accept_deletions: self.marker.accept_deletions,
            expected_epoch: self.original_epoch.clone(),
            batches: self.marker.batches,
            regeneration_plan_sha256: self.marker.regeneration_plan_sha256.clone(),
            regeneration_steps: self.marker.regeneration_steps,
            sources: vec![],
        };
        completion::verify_empty_baseline(self.client.erasure_journal()?, &input).map(Some)
    }
    pub(crate) fn certified_empty_baseline_configuration_id(
        &self,
    ) -> StorageResult<Option<elephant_types::RecordId>> {
        Ok(self
            .certified_empty_baseline()?
            .and_then(|b| b.configuration_id))
    }
    fn receipt_sources(&self) -> StorageResult<Vec<elephant_types::RecordId>> {
        let mut sources = self
            .original_sources
            .iter()
            .map(|s| s.source_id.clone())
            .collect::<Vec<_>>();
        if sources.is_empty() {
            sources.push(
                self.certified_empty_baseline()?
                    .and_then(|b| b.receipt_source_id)
                    .ok_or_else(|| {
                        StorageError::Conflict("certified predecessor receipt scope absent".into())
                    })?,
            );
        }
        sources.sort();
        sources.dedup();
        Ok(sources)
    }
    pub(crate) fn original_population_scope(&self) -> StorageResult<(&str, &str, &str)> {
        self.check_custody()?;
        Ok((
            &self.marker.source_space_id,
            &self.marker.space_id,
            &self.marker.before_snapshot_sha256,
        ))
    }

    /// Seal the exact durable batch chain for Source-owned S1 regeneration.
    /// This cannot clear the pending marker or manufacture a completion token.
    ///
    /// # Errors
    /// Refuses incomplete, altered or stale chains, journal or original receipts.
    pub async fn seal_for_regeneration(self) -> StorageResult<SealedHistoricalCatchupAuthority> {
        self.check_custody()?;
        if self.next_batch != self.marker.batches {
            return Err(StorageError::Conflict(
                "catch-up batches are incomplete".into(),
            ));
        }
        let durable = recover_chain(&self.client, &self.marker, &self.original_epoch).await?;
        if durable.next_batch != self.next_batch
            || durable.last_receipt != self.last_receipt
            || durable.epoch != self.epoch
            || serde_json::to_value(&durable.archive_receipt)
                .map_err(crate::durability_io::io_error)?
                != serde_json::to_value(&self.archive_receipt)
                    .map_err(crate::durability_io::io_error)?
            || serde_json::to_vec(&durable.recovered_regeneration)
                .map_err(crate::durability_io::io_error)?
                != serde_json::to_vec(&self.recovered_regeneration)
                    .map_err(crate::durability_io::io_error)?
            || CanonicalRepository::new(self.client.clone())
                .observe_epoch(&self.marker.space_id)
                .await?
                .as_deref()
                != Some(self.epoch.as_str())
        {
            return Err(StorageError::Conflict(
                "catch-up final batch chain changed".into(),
            ));
        }
        self.client.verify_journal_identity().await?;
        for source in &self.original_sources {
            let receipt =
                exact_receipt(&self.client, &self.marker.space_id, &source.commit_id).await?;
            if receipt.historical_import.as_ref().is_none_or(|original| {
                original.snapshot_sha256 != self.marker.before_snapshot_sha256
            }) || hex::encode(Sha256::digest(
                serde_json::to_vec(&receipt).map_err(crate::durability_io::io_error)?,
            )) != source.receipt_sha256
            {
                return Err(StorageError::Conflict(
                    "catch-up original receipt changed".into(),
                ));
            }
        }
        self.check_custody()?;
        let regeneration_anchor = archive_catchup::regeneration_anchor(
            &self.marker,
            &self.original_epoch,
            self.last_receipt.as_deref(),
            self.archive_receipt.as_ref(),
        )?;
        Ok(SealedHistoricalCatchupAuthority {
            inner: self,
            regeneration_anchor,
        })
    }

    /// Verify an inherited operational row against the exact completed
    /// predecessor inventory. This grants no dispatch or source publication.
    pub(crate) async fn verify_inherited_operational_control(
        &self,
        table: &str,
        key: &str,
        expected: &crate::historical_adoption::catchup::HistoricalControlImage,
    ) -> StorageResult<completion::VerifiedHistoricalControl> {
        self.check_custody()?;
        let baseline = self
            .retained_operational
            .get_or_try_init(|| async {
                completion::retained_operational_baseline(
                    self.client.erasure_journal()?,
                    &self.marker,
                    &self.original_epoch,
                )
            })
            .await?;
        let verified = baseline
            .verify_control(self.client()?, table, key, expected)
            .await?;
        self.check_custody()?;
        Ok(verified)
    }

    /// A preparation index may bind only the complete original authority list.
    pub(crate) fn verify_source_bindings(
        &self,
        bindings: &[HistoricalCatchupSource],
    ) -> StorageResult<()> {
        self.check_custody()?;
        if serde_json::to_value(bindings).map_err(crate::durability_io::io_error)?
            != serde_json::to_value(&self.original_sources)
                .map_err(crate::durability_io::io_error)?
        {
            return Err(StorageError::Conflict(
                "catch-up original source list differs".into(),
            ));
        }
        Ok(())
    }

    /// Durable next ordinal/receipt/epoch, including a successfully recovered prefix.
    #[must_use]
    pub fn next_batch(&self) -> (u64, Option<&str>, &str) {
        (self.next_batch, self.last_receipt.as_deref(), &self.epoch)
    }

    /// Apply one prepared full-image batch and invalidate only affected heads.
    /// No batch publishes partial S1 state or clears the global pending marker.
    ///
    /// # Errors
    /// Refuses changed custody, chain, target rows, source controls or original receipts.
    pub async fn apply_batch(
        &mut self,
        index: &crate::historical_adoption::PreparedHistoricalEffectIndex,
        batch: crate::historical_delta::PreparedDeltaBatch,
    ) -> StorageResult<CanonicalCommitReceipt> {
        Ok(self.apply_batch_with_effects(index, batch).await?.0)
    }

    /// Apply an atomic batch and return its receipt plus verified native inventory effects.
    /// The effects can be replayed into the retained ledger after a process crash.
    pub async fn apply_batch_with_effects(
        &mut self,
        index: &crate::historical_adoption::PreparedHistoricalEffectIndex,
        batch: crate::historical_delta::PreparedDeltaBatch,
    ) -> StorageResult<(
        CanonicalCommitReceipt,
        Option<crate::historical_native_effects::VerifiedHistoricalNativeEffects>,
    )> {
        self.bind_batch(&batch)?;
        let effects = index.prepare_effects(self, &batch).await?;
        let operational =
            crate::historical_delta_operations::prepare(self, &batch, &effects.projected_rows)
                .await?;
        let after_epoch = uuid::Uuid::new_v4().to_string();
        let state = HistoricalCatchupBatchReceipt {
            version: 1,
            authority_binding_sha256: self.marker.authority_binding_sha256.clone(),
            ordinal: self.next_batch,
            batch_sha256: batch.digest.clone(),
            previous_receipt_sha256: self.last_receipt.clone(),
            before_epoch: self.epoch.clone(),
            after_epoch: after_epoch.clone(),
            operational_effects: crate::historical_delta_operations::receipt_effects(&operational),
            retained_controls: batch.input.retained_controls.clone(),
        };
        // Empty live populations retain the actual predecessor's receipt scope.
        // This ID is never converted into live membership or dispatch authority.
        let source_ids = self.receipt_sources()?;
        let receipt = CanonicalCommitReceipt {
            historical_import: None,
            space_id: self.marker.space_id.clone(),
            commit_id: batch_commit_id(&self.marker, self.next_batch),
            input_fingerprint: self.marker.authority_binding_sha256.clone(),
            plan_fingerprint: batch.digest.clone(),
            replay_output: Some(
                serde_json::to_value(state).map_err(crate::durability_io::io_error)?,
            ),
            source_ids,
            publication_authority: None,
            maintenance_authority: Vec::new(),
            source_id: None,
            source_revision: None,
            effect_ids: effects
                .projected_rows
                .iter()
                .map(|row| row.id.clone())
                .chain(
                    effects
                        .projected_embeddings
                        .iter()
                        .map(|row| row.id.clone()),
                )
                .collect(),
            extension_ids: effects
                .controls
                .iter()
                .map(|control| {
                    crate::source_publication::control_thing(control.table, &control.key)
                })
                .chain(operational.iter().map(|control| {
                    crate::source_publication::control_thing(control.table, &control.key)
                }))
                .collect(),
            outbox_ids: Vec::new(),
            committed_at: chrono::Utc::now(),
        };
        let native_effects = crate::historical_delta_storage::commit_batch(
            self,
            &effects,
            &operational,
            &receipt,
            &after_epoch,
        )
        .await?;
        let mut next = ChainPosition {
            next_batch: self.next_batch,
            last_receipt: self.last_receipt.clone(),
            epoch: self.epoch.clone(),
            recovered_regeneration: Vec::new(),
            archive_receipt: None,
        };
        next.extend(&self.marker, &receipt)?;
        self.next_batch = next.next_batch;
        self.last_receipt = next.last_receipt;
        self.epoch = next.epoch;
        Ok((receipt, native_effects))
    }

    /// Return the original batch input position from its validated durable
    /// primary receipt, allowing a fresh S0/S1 reader to reconstruct its digest.
    pub async fn replay_batch_position(
        &self,
        ordinal: u64,
    ) -> StorageResult<(String, Option<String>)> {
        let (_, state) = self.replay_primary(ordinal).await?;
        Ok((state.before_epoch, state.previous_receipt_sha256))
    }

    async fn replay_primary(
        &self,
        ordinal: u64,
    ) -> StorageResult<(CanonicalCommitReceipt, HistoricalCatchupBatchReceipt)> {
        self.check_custody()?;
        if ordinal >= self.next_batch {
            return Err(StorageError::Conflict("batch is not committed".into()));
        }
        let receipt = exact_receipt(
            &self.client,
            &self.marker.space_id,
            &batch_commit_id(&self.marker, ordinal),
        )
        .await?;
        let state: HistoricalCatchupBatchReceipt = serde_json::from_value(
            receipt
                .replay_output
                .clone()
                .ok_or_else(|| StorageError::Conflict("batch replay missing state".into()))?,
        )
        .map_err(crate::durability_io::io_error)?;
        if receipt.commit_id != batch_commit_id(&self.marker, ordinal)
            || receipt.space_id != self.marker.space_id
            || state.ordinal != ordinal
            || state.authority_binding_sha256 != self.marker.authority_binding_sha256
            || receipt.input_fingerprint != self.marker.authority_binding_sha256
            || receipt.plan_fingerprint != state.batch_sha256
            || state.version != 1
            || receipt.historical_import.is_some()
            || receipt.publication_authority.is_some()
            || !receipt.maintenance_authority.is_empty()
            || !receipt.outbox_ids.is_empty()
        {
            return Err(StorageError::Conflict(
                "batch replay primary differs".into(),
            ));
        }
        Ok((receipt, state))
    }

    /// Recover only effects whose complete physical identity set is independently
    /// derived from the original typed batch and original source authority index.
    pub async fn replay_batch_effects(
        &self,
        index: &crate::historical_adoption::PreparedHistoricalEffectIndex,
        batch: crate::historical_delta::PreparedDeltaBatch,
    ) -> StorageResult<crate::historical_native_effects::VerifiedHistoricalNativeEffects> {
        self.check_custody()?;
        let input = &batch.input;
        self.verify_deletion_intent(input)?;
        let (receipt, state) = self.replay_primary(input.batch_ordinal).await?;
        if input.migration_id != self.marker.migration_id
            || input.source_space_id != self.marker.source_space_id
            || input.space_id != self.marker.space_id
            || input.before_snapshot_sha256 != self.marker.before_snapshot_sha256
            || input.after_snapshot_sha256 != self.marker.after_snapshot_sha256
            || input.planner_delta_sha256 != self.marker.delta_sha256
            || input.expected_epoch != state.before_epoch
            || input.previous_batch_receipt != state.previous_receipt_sha256
            || batch.digest != state.batch_sha256
            || input.retained_controls != state.retained_controls
        {
            return Err(StorageError::Conflict(
                "replayed original batch differs".into(),
            ));
        }
        crate::historical_delta_operations::verify_receipt_identities(
            &batch,
            &state.operational_effects,
        )?;
        let mut ids = index.replay_inventory(self, &batch, &receipt).await?;
        ids.extend(crate::historical_delta_operations::planned_identities(
            &batch,
        ));
        // Source index includes the original receipt-bound configuration ID;
        // never reconstruct replay identities from current target configuration.
        ids.push((
            "canonical_epoch".into(),
            crate::source_publication::control_thing("canonical_epoch", &self.marker.space_id),
        ));
        ids.push((
            "source_commit_receipt".into(),
            crate::source_publication::control_thing(
                "source_commit_receipt",
                &crate::source_publication::stable_key(&[
                    &self.marker.space_id,
                    &receipt.commit_id,
                ]),
            ),
        ));
        let mut fences = std::collections::BTreeSet::new();
        for (table, id) in &ids {
            if matches!(
                table.as_str(),
                "doc" | "source_head" | "source_publication_manifest"
            ) {
                let hash = hex::encode(Sha256::digest(id.to_string().as_bytes()));
                fences.insert(format!("{table}:{}", &hash[..3]));
            }
        }
        ids.extend(fences.into_iter().map(|key| {
            (
                "native_read_fence".into(),
                crate::source_publication::control_thing("native_read_fence", &key),
            )
        }));
        ids.sort_by(|a, b| a.1.cmp(&b.1));
        ids.dedup();
        let binding = crate::historical_native_effects::HistoricalNativeEffectBinding::new(
            &self.marker.authority_binding_sha256,
            "batch",
            state.ordinal,
            &receipt,
            &ids,
        )?;
        let source = receipt
            .source_ids
            .first()
            .ok_or_else(|| StorageError::Conflict("batch source absent".into()))?;
        let verified = crate::historical_native_effects::recover_verified(
            &self.client,
            source,
            &binding,
            &receipt,
            &ids,
        )
        .await?;
        self.check_custody()?;
        Ok(verified)
    }

    pub(crate) fn ledger_binding(&self) -> (&str, &str, &str, &str, &str, u64, u64) {
        (
            &self.marker.authority_binding_sha256,
            &self.marker.original_report_sha256,
            &self.marker.baseline_ledger_sha256,
            &self.marker.baseline_native_rows_sha256,
            &self.marker.journal_identity,
            self.marker.batches,
            self.marker.regeneration_steps,
        )
    }
    pub(crate) fn effect_manifest_binding(&self) -> (&str, u64) {
        (&self.marker.authority_binding_sha256, self.next_batch)
    }
    pub(crate) fn client(&self) -> StorageResult<&StorageClient> {
        self.check_custody()?;
        Ok(&self.client)
    }

    pub(crate) fn check_custody(&self) -> StorageResult<()> {
        if !self.client.maintenance
            || self.client.maintenance_lease.is_none()
            || self.client.recovery.status().phase != RecoveryPhase::Restoring
            || journal::read(self.client.erasure_journal()?)?.as_ref() != Some(&self.marker)
        {
            return Err(StorageError::Conflict(
                "original catch-up custody unavailable".into(),
            ));
        }
        Ok(())
    }

    fn verify_deletion_intent(
        &self,
        input: &crate::historical_delta::HistoricalDeltaBatch,
    ) -> StorageResult<()> {
        if !self.marker.accept_deletions
            && (input.rows.iter().any(|row| row.destructive_intent)
                || input.embeddings.iter().any(|row| row.destructive_intent)
                || input
                    .retained_controls
                    .iter()
                    .any(|row| row.before.is_some() && row.after.is_none()))
        {
            return Err(StorageError::Conflict(
                "catch-up deletion intent not supplied".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn bind_batch(
        &self,
        batch: &crate::historical_delta::PreparedDeltaBatch,
    ) -> StorageResult<()> {
        self.check_custody()?;
        let input = &batch.input;
        self.verify_deletion_intent(input)?;
        if input.source_space_id != self.marker.source_space_id
            || input.migration_id != self.marker.migration_id
            || input.space_id != self.marker.space_id
            || input.before_snapshot_sha256 != self.marker.before_snapshot_sha256
            || input.after_snapshot_sha256 != self.marker.after_snapshot_sha256
            || input.planner_delta_sha256 != self.marker.delta_sha256
            || input.batch_ordinal != self.next_batch
            || input.previous_batch_receipt != self.last_receipt
            || input.expected_epoch != self.epoch
            || self.next_batch >= self.marker.batches
            || self.archive_receipt.is_some()
            || !self.recovered_regeneration.is_empty()
        {
            return Err(StorageError::Conflict(
                "catch-up batch does not extend original receipt chain".into(),
            ));
        }
        Ok(())
    }
}

/// Written only inside the all-table row/source/epoch transaction. This payload
/// lives in an ordinary source-aware canonical receipt, never in a side journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalCatchupBatchReceipt {
    pub version: u32,
    pub authority_binding_sha256: String,
    pub ordinal: u64,
    pub batch_sha256: String,
    pub previous_receipt_sha256: Option<String>,
    pub before_epoch: String,
    pub after_epoch: String,
    #[serde(default)]
    pub operational_effects: Vec<crate::historical_delta_operations::OperationalReceiptEffect>,
    #[serde(default)]
    pub retained_controls: Vec<crate::historical_delta::HistoricalRetainedControlDelta>,
}

#[derive(Debug)]
struct ChainPosition {
    next_batch: u64,
    last_receipt: Option<String>,
    epoch: String,
    recovered_regeneration: Vec<CanonicalCommitReceipt>,
    archive_receipt: Option<CanonicalCommitReceipt>,
}

fn batch_commit_id(marker: &CatchupMarker, ordinal: u64) -> String {
    crate::source_publication::stable_key(&[
        "historical-catchup-batch-v1",
        &marker.authority_binding_sha256,
        &ordinal.to_string(),
    ])
}

impl ChainPosition {
    fn extend(
        &mut self,
        marker: &CatchupMarker,
        receipt: &CanonicalCommitReceipt,
    ) -> StorageResult<()> {
        let state: HistoricalCatchupBatchReceipt = serde_json::from_value(
            receipt
                .replay_output
                .clone()
                .ok_or_else(|| StorageError::Conflict("catch-up receipt state missing".into()))?,
        )
        .map_err(crate::durability_io::io_error)?;
        if self.next_batch >= marker.batches
            || self.archive_receipt.is_some()
            || !self.recovered_regeneration.is_empty()
            || receipt.space_id != marker.space_id
            || receipt.commit_id != batch_commit_id(marker, self.next_batch)
            || state.version != 1
            || state.authority_binding_sha256 != marker.authority_binding_sha256
            || state.ordinal != self.next_batch
            || state.previous_receipt_sha256 != self.last_receipt
            || state.before_epoch != self.epoch
            || state.after_epoch.is_empty()
            || state.before_epoch == state.after_epoch
            || !journal::valid_digest(&state.batch_sha256)
            || receipt.plan_fingerprint != state.batch_sha256
            || receipt.input_fingerprint != marker.authority_binding_sha256
        {
            return Err(StorageError::Conflict(
                "catch-up durable receipt chain changed".into(),
            ));
        }
        self.last_receipt = Some(hex::encode(Sha256::digest(
            serde_json::to_vec(receipt).map_err(crate::durability_io::io_error)?,
        )));
        self.next_batch += 1;
        self.epoch = state.after_epoch;
        Ok(())
    }
}

async fn recover_chain(
    client: &StorageClient,
    marker: &CatchupMarker,
    original_epoch: &str,
) -> StorageResult<ChainPosition> {
    let mut state = ChainPosition {
        next_batch: 0,
        last_receipt: None,
        epoch: original_epoch.into(),
        recovered_regeneration: Vec::new(),
        archive_receipt: None,
    };
    let mut gap = false;
    for ordinal in 0..marker.batches {
        let commit_id = batch_commit_id(marker, ordinal);
        let receipt: Option<CanonicalCommitReceipt> =
            SourcePublicationRepository::new(client.clone())
                .read_control(
                    "source_commit_receipt",
                    &crate::source_publication::stable_key(&[&marker.space_id, &commit_id]),
                    &marker.space_id,
                )
                .await?;
        match receipt {
            Some(receipt) if !gap => state.extend(marker, &receipt)?,
            Some(_) => {
                return Err(StorageError::Conflict(
                    "catch-up receipt chain has a gap".into(),
                ))
            }
            None => gap = true,
        }
    }
    archive_catchup::recover(client, marker, &mut state).await?;
    regeneration_chain::recover(client, marker, &mut state).await?;
    if CanonicalRepository::new(client.clone())
        .observe_epoch(&marker.space_id)
        .await?
        .as_deref()
        != Some(state.epoch.as_str())
    {
        return Err(StorageError::Conflict(
            "catch-up target differs from last atomic receipt".into(),
        ));
    }
    Ok(state)
}

/// A storage-produced report receipt. Export never rereads live destination rows.
/// The token cannot be manufactured or deserialized by a CLI caller.
pub struct HistoricalCatchupReport {
    journal: std::path::PathBuf,
    receipt: completion::Receipt,
}
impl HistoricalCatchupReport {
    /// Export the retained report and closed baseline companions without reading live rows.
    /// Existing output names are accepted only when their bytes match exactly.
    pub fn export(
        &self,
        report: &std::path::Path,
        ledger: &std::path::Path,
        bindings: &std::path::Path,
    ) -> StorageResult<()> {
        let journal = crate::erasure_journal::ErasureJournal::open(&self.journal)?;
        completion::export(&journal, &self.receipt, report, ledger, bindings)
    }
}
struct ReportRequest {
    template: serde_json::Value,
    original_report: std::path::PathBuf,
    original_ledger: std::path::PathBuf,
}

impl StorageClient {
    /// Verify the original completed empty population certificate for certification.
    /// This read-only proof survives governed reindex/activation epoch changes;
    /// it grants no catch-up or mutation authority. The caller must separately
    /// hold local native custody and verify live activation and population.
    ///
    /// # Errors
    /// Refuses missing/altered certificates, report/configuration/journal mismatch,
    /// nonempty historical bindings and unavailable or pending recovery.
    pub async fn verify_completed_empty_historical_population(
        &self,
        report_sha256: &str,
        report: &serde_json::Value,
        space: &str,
        configuration_id: &elephant_types::RecordId,
    ) -> StorageResult<()> {
        self.ensure_historical_catchup_backend()?;
        if !self.recovery.status().ready
            || self.recovery.in_activity()
            || journal::read(&self.journal)?.is_some()
            || self.journal.transfer_marker()?.is_some()
            || self.journal.restore_marker()?.is_some()
        {
            return Err(StorageError::Conflict(
                "historical population certificate requires ready native custody".into(),
            ));
        }
        self.verify_journal_identity().await?;
        completion::verify_completed_empty_population(
            &self.journal,
            report_sha256,
            report,
            space,
            configuration_id,
        )
    }

    /// Run a closed delta under exclusive nonempty-target custody.
    /// Errors preserve the durable marker and refuse ordinary work after restart.
    ///
    /// # Errors
    /// Refuses stale journal/receipt/head/epoch, overlapping maintenance and incomplete closure.
    #[allow(
        clippy::significant_drop_tightening,
        reason = "Custody spans the callback and durable final reconciliation."
    )]
    pub async fn with_historical_catchup<F, Fut>(
        &self,
        input: HistoricalCatchupInput,
        action: F,
    ) -> StorageResult<()>
    where
        F: FnOnce(PreparedHistoricalCatchupAuthority) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = StorageResult<CompletedHistoricalCatchup>>
            + Send
            + 'static,
    {
        self.catchup_inner(input, None, action).await.map(|_| ())
    }

    /// Complete a delta and retain its exact successor baseline/report before readiness.
    /// An exact completed retry only validates the final epoch and exports retained bytes.
    pub async fn with_historical_catchup_report<F, Fut>(
        &self,
        input: HistoricalCatchupInput,
        report_template: serde_json::Value,
        original_report: std::path::PathBuf,
        original_ledger: std::path::PathBuf,
        action: F,
    ) -> StorageResult<HistoricalCatchupReport>
    where
        F: FnOnce(PreparedHistoricalCatchupAuthority) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = StorageResult<CompletedHistoricalCatchup>>
            + Send
            + 'static,
    {
        self.catchup_inner(
            input,
            Some(ReportRequest {
                template: report_template,
                original_report,
                original_ledger,
            }),
            action,
        )
        .await?
        .ok_or_else(|| StorageError::Conflict("retained catch-up report absent".into()))
    }

    #[allow(
        clippy::significant_drop_tightening,
        reason = "Custody spans durable completion and report retention."
    )]
    async fn catchup_inner<F, Fut>(
        &self,
        input: HistoricalCatchupInput,
        report: Option<ReportRequest>,
        action: F,
    ) -> StorageResult<Option<HistoricalCatchupReport>>
    where
        F: FnOnce(PreparedHistoricalCatchupAuthority) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = StorageResult<CompletedHistoricalCatchup>>
            + Send
            + 'static,
    {
        let (marker, pending, resuming) = self.prepare_catchup_entry(&input, report.as_ref())?;
        let client = self.clone();
        tokio::spawn(async move {
            let guard = client
                .begin_maintenance(RecoveryPhase::Restoring, std::time::Duration::from_secs(60))
                .await?;
            let exclusive = client.maintenance_client_with_guard(&guard);
            if journal::read(&client.journal)? != pending {
                return Err(StorageError::Conflict(
                    "catch-up marker changed during custody acquisition".into(),
                ));
            }
            if let Some(request) = &report {
                let retained = match completion::read(&client.journal, &marker) {
                    Ok(retained) => retained,
                    Err(error) => {
                        if !resuming {
                            guard.finish();
                        }
                        return Err(error);
                    }
                };
                if let Some(done) = retained {
                    let result = client
                        .recover_completed_catchup(&exclusive, &marker, request, done, resuming)
                        .await;
                    // A completed receipt does not bypass current target/epoch guards.
                    // On a ready retry, refusing a stale report leaves readiness unchanged.
                    if result.is_ok() || !resuming {
                        guard.finish();
                    }
                    return result.map(Some);
                }
            }
            // The original pending marker is the only restart exception. A changed
            // declaration, journal, receipt chain or epoch remains blocked.
            let state = if resuming {
                // Only the exact retained pending marker reaches this branch
                // under exclusive custody. A committed archive primary may be
                // ahead of its registry head after a crash; replay must repair
                // that exact effect chain before the full current-head gate.
                exclusive.verify_journal_anchor_identity().await?;
                input.verify_original_receipts(&exclusive).await?;
                let recovered = recover_chain(&exclusive, &marker, &input.expected_epoch).await?;
                exclusive.verify_journal_identity().await?;
                recovered
            } else {
                if let Err(error) = input.verify_current(&exclusive).await {
                    guard.finish();
                    return Err(error);
                }
                if marker.is_exact_noop() {
                    // Exact S0=S1 declaration plus unchanged adoption/head/epoch:
                    // verified no-op, with no durable pending marker or callback.
                    let retained = client
                        .retain_noop_catchup(&exclusive, &marker, &input, report.as_ref())
                        .await;
                    // No pending marker or DB writes exist on the verified no-op
                    // path, including a failed retained-output publication.
                    guard.finish();
                    return retained.map(|retained| {
                        retained.map(|receipt| HistoricalCatchupReport {
                            journal: client.journal.path().to_owned(),
                            receipt,
                        })
                    });
                }
                journal::begin(&client.journal, &marker)?;
                ChainPosition {
                    next_batch: 0,
                    last_receipt: None,
                    epoch: input.expected_epoch.clone(),
                    recovered_regeneration: Vec::new(),
                    archive_receipt: None,
                }
            };
            let authority = PreparedHistoricalCatchupAuthority {
                client: exclusive.clone(),
                marker: marker.clone(),
                next_batch: state.next_batch,
                last_receipt: state.last_receipt,
                epoch: state.epoch,
                original_epoch: input.expected_epoch.clone(),
                original_sources: input.sources,
                recovered_regeneration: state.recovered_regeneration,
                archive_receipt: state.archive_receipt,
                retained_operational: tokio::sync::OnceCell::new(),
            };
            let completion = action(authority).await?;
            let retained = client
                .retain_completed_catchup(&exclusive, &marker, completion, report)
                .await?;
            journal::finish(&client.journal, &marker)?;
            drop(exclusive);
            guard.finish();
            Ok(retained.map(|receipt| HistoricalCatchupReport {
                journal: client.journal.path().to_owned(),
                receipt,
            }))
        })
        .await
        .map_err(|_| StorageError::QueryError("owned catch-up task failed".into()))?
    }

    fn prepare_catchup_entry(
        &self,
        input: &HistoricalCatchupInput,
        report: Option<&ReportRequest>,
    ) -> StorageResult<(CatchupMarker, Option<CatchupMarker>, bool)> {
        self.ensure_historical_catchup_backend()?;
        let marker = input.validate(self)?;
        if let Some(request) = report {
            completion::validate_template(&marker, &request.template)?;
        }
        let pending = journal::read(&self.journal)?;
        let resuming = pending.as_ref() == Some(&marker);
        if pending.is_some() && !resuming {
            return Err(StorageError::Conflict(
                "catch-up input differs from original pending marker".into(),
            ));
        }
        if self.recovery.in_activity() || (!self.recovery.status().ready && !resuming) {
            return Err(StorageError::Conflict(
                "catch-up requires ready administrative custody".into(),
            ));
        }
        if self.journal.transfer_marker()?.is_some() || self.journal.restore_marker()?.is_some() {
            return Err(StorageError::Conflict("restore pending".into()));
        }
        Ok((marker, pending, resuming))
    }

    async fn recover_completed_catchup(
        &self,
        exclusive: &Self,
        marker: &CatchupMarker,
        request: &ReportRequest,
        done: completion::Receipt,
        resuming: bool,
    ) -> StorageResult<HistoricalCatchupReport> {
        if done.report_template_sha256 != completion::template_sha(&request.template)?
            || CanonicalRepository::new(exclusive.clone())
                .observe_epoch(&marker.space_id)
                .await?
                .as_deref()
                != Some(done.final_epoch.as_str())
        {
            return Err(StorageError::Conflict(
                "completed catch-up target or report input changed".into(),
            ));
        }
        exclusive.verify_journal_identity().await?;
        if let Some(archive) = &done.baseline.operational_archive {
            crate::historical_archive_lifecycle::verify_receipt_current(exclusive, archive).await?;
        }
        // Repair a crash between the two immutable receipt names
        // before allowing the recovered baseline to be exported.
        completion::write(&self.journal, &done)?;
        if resuming {
            journal::finish(&self.journal, marker)?;
        }
        Ok(HistoricalCatchupReport {
            journal: self.journal.path().to_owned(),
            receipt: done,
        })
    }

    async fn retain_noop_catchup(
        &self,
        exclusive: &Self,
        marker: &CatchupMarker,
        input: &HistoricalCatchupInput,
        report: Option<&ReportRequest>,
    ) -> StorageResult<Option<completion::Receipt>> {
        let retained = if let Some(request) = report {
            let baseline = completion::retain_noop_baseline(
                &self.journal,
                input,
                &request.original_report,
                &request.original_ledger,
            )?;
            if let Some(archive) = &baseline.operational_archive {
                crate::historical_archive_lifecycle::verify_receipt_current(exclusive, archive)
                    .await?;
            }
            Some(completion::Receipt {
                marker: marker.clone(),
                final_epoch: input.expected_epoch.clone(),
                inventory_receipt_sha256: marker.authority_binding_sha256.clone(),
                report_template_sha256: completion::template_sha(&request.template)?,
                report: completion::render(request.template.clone(), &baseline)?,
                baseline,
            })
        } else {
            None
        };
        if let Some(done) = &retained {
            completion::write(&self.journal, done)?;
        }
        Ok::<Option<completion::Receipt>, StorageError>(retained)
    }

    async fn retain_completed_catchup(
        &self,
        exclusive: &Self,
        marker: &CatchupMarker,
        completion: CompletedHistoricalCatchup,
        report: Option<ReportRequest>,
    ) -> StorageResult<Option<completion::Receipt>> {
        if completion.marker != *marker
            || !journal::valid_digest(&completion.receipt)
            || CanonicalRepository::new(exclusive.clone())
                .observe_epoch(&marker.space_id)
                .await?
                .as_deref()
                != Some(completion.final_epoch.as_str())
        {
            return Err(StorageError::Conflict(
                "catch-up final receipt/epoch mismatch".into(),
            ));
        }
        exclusive.verify_journal_identity().await?;
        exclusive.persist_restored_state().await?;
        let retained = if let Some(request) = report {
            let baseline = completion
                .baseline
                .ok_or_else(|| StorageError::Conflict("successor baseline absent".into()))?;
            Some(completion::Receipt {
                marker: marker.clone(),
                final_epoch: completion.final_epoch,
                inventory_receipt_sha256: completion.receipt,
                report_template_sha256: completion::template_sha(&request.template)?,
                report: completion::render(request.template, &baseline)?,
                baseline,
            })
        } else {
            None
        };
        if let Some(done) = &retained {
            completion::write(&self.journal, done)?;
        }
        Ok(retained)
    }

    fn ensure_historical_catchup_backend(&self) -> StorageResult<()> {
        if self.embedded_store().is_some() {
            return Err(StorageError::Conflict(
                "complete historical catch-up currently requires native storage".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn historical_catchup_pending(&self) -> StorageResult<bool> {
        Ok(journal::read(&self.journal)?.is_some())
    }
}

#[cfg(test)]
#[path = "historical_catchup_executor_tests.rs"]
mod executor_tests;

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::{Repository, StorageConfig};

    pub(super) fn fixture_marker(client: &StorageClient) -> CatchupMarker {
        CatchupMarker {
            version: 1,
            journal_identity: client.journal.identity().into(),
            migration_id: "a".repeat(64),
            source_space_id: "default".into(),
            space_id: "historical".into(),
            before_snapshot_sha256: "b".repeat(64),
            after_snapshot_sha256: "c".repeat(64),
            delta_sha256: "d".repeat(64),
            original_report_sha256: "a".repeat(64),
            baseline_ledger_sha256: "b".repeat(64),
            baseline_native_rows_sha256: "c".repeat(64),
            accept_deletions: true,
            authority_binding_sha256: "e".repeat(64),
            batches: 1,
            regeneration_plan_sha256: "f".repeat(64),
            regeneration_steps: 2,
        }
    }

    #[tokio::test]
    #[allow(
        clippy::significant_drop_tightening,
        reason = "Original journal and opened clients remain alive to compare restart behavior."
    )]
    async fn interrupted_catchup_blocks_reopened_native_client_and_restore() {
        let directory = tempfile::tempdir().expect("directory");
        let journal_path = directory.path().join("journal");
        let config = StorageConfig::default();
        let client =
            StorageClient::new_with_durability(config.clone(), None, Some(journal_path.clone()))
                .await
                .expect("client");
        let marker = fixture_marker(&client);
        journal::begin(&client.journal, &marker).expect("pending marker");
        client.initialize_recovery().await.expect("blocked handle");
        assert!(!client.recovery_status().ready);
        assert!(client.query_checked("SELECT * FROM entity").await.is_err());
        assert!(client.ensure_no_incomplete_transfer().is_err());
        let reopened = StorageClient::new_with_durability(config, None, Some(journal_path))
            .await
            .expect("administrative handle");
        assert!(!reopened.recovery_status().ready);
        assert!(reopened
            .query_checked("SELECT * FROM entity")
            .await
            .is_err());
        assert_eq!(
            journal::read(&reopened.journal).expect("marker"),
            Some(marker)
        );
    }

    #[tokio::test]
    async fn full_catchup_embedded_rejected_before_marker_or_maintenance() {
        let client = Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded");
        assert!(client.recovery_status().ready);
        let error = client
            .ensure_historical_catchup_backend()
            .expect_err("unsupported whole operation");
        assert!(error.to_string().contains("requires native storage"));
        assert!(client.recovery_status().ready);
        assert!(journal::read(&client.journal)
            .expect("marker read")
            .is_none());
        drop(client);
    }

    #[tokio::test]
    async fn interrupted_catchup_blocks_embedded_normal_admission() {
        let client = Box::pin(StorageClient::new_embedded())
            .await
            .expect("embedded");
        journal::begin(&client.journal, &fixture_marker(&client)).expect("marker");
        client.initialize_recovery().await.expect("blocked handle");
        assert!(!client.recovery_status().ready);
        assert!(crate::WorkItemRepository::new(client.clone())
            .get(
                "historical",
                &elephant_types::RecordId::new_v4(elephant_types::TableName::WorkItem)
            )
            .await
            .is_err());
        assert!(client.ensure_no_incomplete_transfer().is_err());
        drop(client);
    }
    async fn binding_failure_readiness(embedded: bool) {
        let client = if embedded {
            Box::pin(StorageClient::new_embedded()).await
        } else {
            Box::pin(StorageClient::new_memory()).await
        }
        .expect("client");
        client.initialize_journal_identity().await.expect("journal");
        let source_id = elephant_types::RecordId::new_v4(elephant_types::TableName::Source);
        let input = HistoricalCatchupInput {
            version: 1,
            journal_identity: client.journal.identity().into(),
            migration_id: "a".repeat(64),
            source_space_id: "default".into(),
            space_id: "historical".into(),
            before_snapshot_sha256: "b".repeat(64),
            after_snapshot_sha256: "c".repeat(64),
            delta_sha256: "d".repeat(64),
            original_report_sha256: "a".repeat(64),
            baseline_ledger_sha256: "b".repeat(64),
            baseline_native_rows_sha256: "c".repeat(64),
            accept_deletions: true,
            expected_epoch: "original-epoch".into(),
            batches: 1,
            regeneration_plan_sha256: "f".repeat(64),
            regeneration_steps: 2,
            sources: vec![HistoricalCatchupSource {
                source_id: source_id.clone(),
                commit_id: "missing-original-receipt".into(),
                receipt_sha256: "e".repeat(64),
                head: SourceHead {
                    space_id: "historical".into(),
                    source_id,
                    generation: "generation".into(),
                    desired_revision: Some("revision".into()),
                    published_revision: Some("revision".into()),
                    operation_id: "operation".into(),
                    lifecycle: elephant_types::publication::SourceVisibility::Active,
                    updated_at: chrono::Utc::now(),
                },
            }],
        };
        let retained = serde_json::to_vec(&input).expect("input");
        let marker = input.validate(&client).expect("shape");
        let invoked = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let called = invoked.clone();
        assert!(client
            .with_historical_catchup(input, move |_| async move {
                called.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(StorageError::QueryError("unexpected callback".into()))
            })
            .await
            .is_err());
        assert!(!invoked.load(std::sync::atomic::Ordering::SeqCst));
        assert!(client.recovery_status().ready);
        assert!(journal::read(&client.journal).expect("no marker").is_none());
        journal::begin(&client.journal, &marker).expect("original interrupted marker");
        client
            .initialize_recovery()
            .await
            .expect("administrative handle");
        assert!(!client.recovery_status().ready);
        let input = serde_json::from_slice(&retained).expect("exact original input");
        let called = invoked.clone();
        assert!(client
            .with_historical_catchup(input, move |_| async move {
                called.store(true, std::sync::atomic::Ordering::SeqCst);
                Err(StorageError::QueryError("unexpected callback".into()))
            })
            .await
            .is_err());
        assert!(!invoked.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(client.recovery_status().phase, RecoveryPhase::Failed);
        assert_eq!(
            journal::read(&client.journal).expect("original marker"),
            Some(marker)
        );
        drop(client);
    }

    #[tokio::test]
    async fn initial_binding_failure_keeps_ready_and_resume_failure_keeps_failed_embedded() {
        binding_failure_readiness(true).await;
    }
    #[tokio::test]
    async fn initial_binding_failure_keeps_ready_and_resume_failure_keeps_failed_native() {
        binding_failure_readiness(false).await;
    }
}
