//! One durable archive publication between the native row and regeneration phases.
//! Original ledger hashes authorize each control before-image; a public seal never does.
use super::{
    completion, exact_receipt, regeneration_chain, CanonicalCommitReceipt, CatchupMarker,
    ChainPosition, Deserialize, Digest, PreparedHistoricalCatchupAuthority, Serialize, Sha256,
    SourcePublicationRepository, StorageClient, StorageError, StorageResult,
};
use crate::erasure_journal::{ArchiveGenerationState, ArchiveRegistration, ArchiveRegistryEntry};
use crate::historical_archive_lifecycle::{
    native_id, seal_evidence, NativeArchive, OperationalArchiveReceipt, RESERVED_ARCHIVE_CONSUMER,
};
use crate::historical_native_effects::{
    self as effects, HistoricalNativeEffectBinding, VerifiedHistoricalNativeEffects,
};
use crate::historical_operational_archive::{ArchiveBinding, VerifiedArchive};
use crate::source_publication::{control_thing, control_value, stable_key};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use surrealdb::sql::{Thing, Value};
fn refused() -> StorageError {
    StorageError::Conflict("historical archive successor differs".into())
}
fn sha<T: Serialize>(value: &T) -> StorageResult<String> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(value).map_err(crate::durability_io::io_error)?,
    )))
}
fn commit_id(marker: &CatchupMarker) -> String {
    stable_key(&[
        "historical-catchup-archive-v1",
        &marker.authority_binding_sha256,
    ])
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    version: u32,
    authority_binding_sha256: String,
    original_report_sha256: String,
    previous_batch_receipt_sha256: Option<String>,
    before_epoch: String,
    after_epoch: String,
    original: OperationalArchiveReceipt,
    successor: NativeArchive,
}
/// Journal reservation established before successor payload creation.
/// It contains the independently verified original archive, never live work authority.
pub struct HistoricalArchiveCandidate {
    entry: ArchiveRegistryEntry,
    binding: ArchiveBinding,
    path: PathBuf,
    original: OperationalArchiveReceipt,
    verified_original: VerifiedArchive,
}
impl HistoricalArchiveCandidate {
    /// Private registered create-only output path.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Exact successor declaration bound before payload creation.
    #[must_use]
    pub const fn binding(&self) -> &ArchiveBinding {
        &self.binding
    }
    /// Original fully verified immutable archive used for bounded CAS replay.
    #[must_use]
    pub const fn original(&self) -> &VerifiedArchive {
        &self.verified_original
    }
}
fn validate_successor(
    marker: &CatchupMarker,
    original: &OperationalArchiveReceipt,
    binding: &ArchiveBinding,
) -> StorageResult<()> {
    binding.validate()?;
    let successor_snapshot_matches = binding.snapshot_sha256 == marker.after_snapshot_sha256;
    let successor_target_matches = binding.target_space_id == marker.space_id;
    if original.binding.snapshot_sha256 != marker.before_snapshot_sha256
        || original.binding.journal_identity != marker.journal_identity
        || original.binding.source_space_id != marker.source_space_id
        || original.binding.target_space_id != marker.space_id
        || original.seal.redacted
        || !successor_snapshot_matches
        || binding.journal_identity != marker.journal_identity
        || binding.source_space_id != marker.source_space_id
        || !successor_target_matches
        || binding.disposition_sha256 != original.binding.disposition_sha256
        || binding.parent_generation_sha256.as_ref() != Some(&original.seal.file_sha256)
    {
        return Err(refused());
    }
    Ok(())
}
fn registration(
    client: &StorageClient,
    original: &OperationalArchiveReceipt,
    binding: &ArchiveBinding,
) -> StorageResult<ArchiveRegistration> {
    let ns = uuid::Uuid::parse_str(client.erasure_journal()?.identity()).map_err(|_| refused())?;
    let digest = binding.digest()?;
    Ok(ArchiveRegistration {
        lineage_id: original.registration.lineage_id.clone(),
        generation_id: uuid::Uuid::new_v5(
            &ns,
            format!("operational-archive-generation:{digest}").as_bytes(),
        )
        .to_string(),
        parent_generation_id: Some(original.registration.generation_id.clone()),
        binding_sha256: digest,
        snapshot_sha256: binding.snapshot_sha256.clone(),
        journal_id: binding.journal_identity.clone(),
        source_space_id: binding.source_space_id.clone(),
        target_space_id: binding.target_space_id.clone(),
    })
}
fn spaces(progress: &Progress) -> StorageResult<BTreeMap<String, i64>> {
    let mut result = BTreeMap::from([(progress.successor.binding.target_space_id.clone(), 0)]);
    for (binding, seal) in [
        (&progress.original.binding, &progress.original.seal),
        (&progress.successor.binding, &progress.successor.seal),
    ] {
        let mut this_cut = BTreeSet::new();
        for (space, tail) in &seal.event_tails {
            let target = if space == &binding.source_space_id {
                &binding.target_space_id
            } else {
                space
            };
            if tail.rows == 0 || tail.max_seq < 0 || !this_cut.insert(target.clone()) {
                return Err(refused());
            }
            result
                .entry(target.clone())
                .and_modify(|v| *v = (*v).max(tail.max_seq))
                .or_insert(tail.max_seq);
        }
    }
    // A bounded transaction; a huge namespace fanout requires a reviewed phase plan.
    if result.len() > 256 {
        return Err(refused());
    }
    Ok(result)
}
fn identities(marker: &CatchupMarker, progress: &Progress) -> StorageResult<Vec<(String, Thing)>> {
    let mut ids = vec![
        ("outbox_checkpoint".into(), native_id(&marker.space_id)),
        (
            "canonical_epoch".into(),
            control_thing("canonical_epoch", &marker.space_id),
        ),
        (
            "source_commit_receipt".into(),
            control_thing(
                "source_commit_receipt",
                &stable_key(&[&marker.space_id, &commit_id(marker)]),
            ),
        ),
    ];
    for space in spaces(progress)?.keys() {
        for consumer in std::iter::once(crate::event_sequence::RESERVED_CONSUMER).chain(
            crate::outbox_checkpoint_repository::HISTORICAL_CONSUMERS
                .iter()
                .copied(),
        ) {
            ids.push((
                "outbox_checkpoint".into(),
                control_thing(
                    "outbox_checkpoint",
                    &crate::outbox_checkpoint_repository::key(space, consumer),
                ),
            ));
        }
    }
    Ok(ids)
}
fn validate_primary(
    marker: &CatchupMarker,
    receipt: &CanonicalCommitReceipt,
    before_epoch: &str,
    last: Option<&str>,
) -> StorageResult<Progress> {
    let p: Progress = serde_json::from_value(receipt.replay_output.clone().ok_or_else(refused)?)
        .map_err(crate::durability_io::io_error)?;
    validate_successor(marker, &p.original, &p.successor.binding)?;
    if p.version != 1
        || p.authority_binding_sha256 != marker.authority_binding_sha256
        || p.original_report_sha256 != marker.original_report_sha256
        || p.previous_batch_receipt_sha256.as_deref() != last
        || p.before_epoch != before_epoch
        || p.after_epoch == p.before_epoch
        || uuid::Uuid::parse_str(&p.after_epoch).is_err()
        || p.successor.version != 1
        || p.successor.seal.redacted
        || p.successor.seal.binding_sha256 != p.successor.binding.digest()?
        || p.successor.registration.parent_generation_id.as_ref()
            != Some(&p.original.registration.generation_id)
        || p.successor.registration.lineage_id != p.original.registration.lineage_id
        || p.successor.registration.binding_sha256 != p.successor.binding.digest()?
        || receipt.space_id != marker.space_id
        || receipt.commit_id != commit_id(marker)
        || receipt.input_fingerprint != marker.authority_binding_sha256
        || receipt.plan_fingerprint != sha(&p)?
        || receipt.historical_import.is_some()
        || receipt.publication_authority.is_some()
        || !receipt.maintenance_authority.is_empty()
        || receipt.source_ids.len() != 1
        || receipt.source_id.as_ref() != receipt.source_ids.first()
        || receipt.source_revision.is_some()
        || !receipt.effect_ids.is_empty()
        || !receipt.extension_ids.is_empty()
        || !receipt.outbox_ids.is_empty()
    {
        return Err(refused());
    }
    Ok(p)
}
async fn replay(
    client: &StorageClient,
    marker: &CatchupMarker,
    receipt: &CanonicalCommitReceipt,
    p: &Progress,
) -> StorageResult<VerifiedHistoricalNativeEffects> {
    let inventory = identities(marker, p)?;
    let binding = HistoricalNativeEffectBinding::new(
        &marker.authority_binding_sha256,
        "archive",
        0,
        receipt,
        &inventory,
    )?;
    let verified = effects::recover_verified(
        client,
        &receipt.source_ids[0],
        &binding,
        receipt,
        &inventory,
    )
    .await?;
    let expected_registration = registration(client, &p.original, &p.successor.binding)?;
    if p.successor.registration != expected_registration {
        return Err(refused());
    }
    let journal = client.erasure_journal()?;
    let current = journal.operational_archive_entry(&expected_registration.generation_id)?;
    let evidence = seal_evidence(&p.successor.binding, &p.successor.seal)?;
    if current.registration != expected_registration || current.evidence() != Some(&evidence) {
        return Err(refused());
    }
    VerifiedArchive::open(
        &journal.archive_payload_path(&expected_registration.generation_id)?,
        &p.successor.binding,
        &p.successor.seal,
    )?;
    let anchor = verified
        .effects()
        .iter()
        .find(|e| e.id == native_id(&marker.space_id).to_string())
        .ok_or_else(refused)?;
    if anchor.before_sha256.as_ref() != Some(&p.original.native_receipt_sha256) {
        return Err(refused());
    }
    let native_sha = anchor.after_sha256.as_deref().ok_or_else(refused)?;
    // Verify exact current native successor before advancing the external journal.
    let mut actual=client.query_bind_checked("SELECT VALUE $this={id:$id,space_id:$space,consumer:$consumer,archive:$archive} AND crypto::sha256(type::string($this))=$hash FROM ONLY $id",crate::bindings!{"id"=>native_id(&marker.space_id),"space"=>marker.space_id.clone(),"consumer"=>RESERVED_ARCHIVE_CONSUMER,"archive"=>serde_json::to_string(&p.successor).map_err(crate::durability_io::io_error)?,"hash"=>native_sha}).await?;
    let matches: Option<bool> = actual.take(0).map_err(crate::durability_io::io_error)?;
    if matches != Some(true) {
        return Err(refused());
    }
    let sealed = ArchiveRegistryEntry {
        registration: expected_registration,
        state: ArchiveGenerationState::Sealed { evidence },
    };
    journal.commit_operational_archive(&sealed, native_sha)?;
    crate::historical_archive_lifecycle::verify_receipt_current(
        client,
        &OperationalArchiveReceipt {
            registration: p.successor.registration.clone(),
            binding: p.successor.binding.clone(),
            seal: p.successor.seal.clone(),
            native_receipt_sha256: native_sha.into(),
        },
    )
    .await?;
    Ok(verified)
}
fn append_archive_before_images(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    inventory: &[(String, Thing)],
    expected: Vec<Option<String>>,
) -> StorageResult<()> {
    for (i, ((_, id), hash)) in inventory.iter().zip(expected).enumerate() {
        bindings.insert(format!("archive_expected_id_{i}"), id.clone().into());
        bindings.insert(
            format!("archive_expected_sha_{i}"),
            hash.map_or(Value::None, Value::from),
        );
        writeln!(sql,"IF (SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $archive_expected_id_{i})!=$archive_expected_sha_{i} {{ THROW 'archive_catchup_before_image_changed'; }};").map_err(|_|refused())?;
    }
    Ok(())
}

fn append_archive_event_cursors(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    marker: &CatchupMarker,
    p: &Progress,
) -> StorageResult<()> {
    for (i, (space, tail)) in spaces(p)?.iter().enumerate() {
        bindings.insert(format!("archive_space_{i}"), space.clone().into());
        writeln!(sql,"IF array::len(SELECT id FROM system_event WHERE space_id=$archive_space_{i} LIMIT 1)!=0 {{ THROW 'archive_catchup_live_events'; }};").map_err(|_|refused())?;
        let base = crate::event_sequence::append_reserve_sql(
            sql,
            bindings,
            space,
            &format!("archive_floor_{i}"),
            *tail,
            0,
        )?;
        let proof = crate::outbox_checkpoint_repository::HistoricalEventBaseline {
            version: 1,
            snapshot_sha256: p.successor.binding.snapshot_sha256.clone(),
            journal_id: marker.journal_identity.clone(),
            through_seq: *tail,
            disposition: "historical_preserved_not_delivered".into(),
        };
        bindings.insert(
            format!("archive_proof_{i}"),
            crate::surreal::json_to_surreal(
                serde_json::to_value(proof).map_err(crate::durability_io::io_error)?,
            )?,
        );
        for (j, consumer) in crate::outbox_checkpoint_repository::HISTORICAL_CONSUMERS
            .iter()
            .enumerate()
        {
            bindings.insert(
                format!("archive_cursor_id_{i}_{j}"),
                control_thing(
                    "outbox_checkpoint",
                    &crate::outbox_checkpoint_repository::key(space, consumer),
                )
                .into(),
            );
            bindings.insert(format!("archive_cursor_name_{i}_{j}"), (*consumer).into());
            writeln!(sql,"UPSERT $archive_cursor_id_{i}_{j} CONTENT {{space_id:$archive_space_{i},consumer:$archive_cursor_name_{i}_{j},seq:{base},updated_at:time::now(),historical_baseline:object::from_entries(array::concat(array::filter(object::entries($archive_proof_{i}),|$entry|$entry[0]!='through_seq'),[['through_seq',{base}]]))}} RETURN NONE;").map_err(|_|refused())?;
        }
    }
    Ok(())
}

pub(super) async fn recover(
    client: &StorageClient,
    marker: &CatchupMarker,
    chain: &mut ChainPosition,
) -> StorageResult<()> {
    let id = commit_id(marker);
    let found: Option<CanonicalCommitReceipt> = SourcePublicationRepository::new(client.clone())
        .read_control(
            "source_commit_receipt",
            &stable_key(&[&marker.space_id, &id]),
            &marker.space_id,
        )
        .await?;
    if let Some(receipt) = found {
        if chain.next_batch != marker.batches {
            return Err(refused());
        }
        let exact = exact_receipt(client, &marker.space_id, &id).await?;
        if sha(&receipt)? != sha(&exact)? {
            return Err(refused());
        }
        let p = validate_primary(marker, &exact, &chain.epoch, chain.last_receipt.as_deref())?;
        replay(client, marker, &exact, &p).await?;
        chain.epoch = p.after_epoch;
        chain.archive_receipt = Some(exact);
    }
    Ok(())
}
pub(super) fn regeneration_anchor(
    marker: &CatchupMarker,
    epoch: &str,
    last: Option<&str>,
    archive: Option<&CanonicalCommitReceipt>,
) -> StorageResult<String> {
    match archive {
        None => regeneration_chain::anchor(marker, epoch, last),
        Some(receipt) => sha(&(
            "historical-catchup-archive-regeneration-anchor-v1",
            &marker.authority_binding_sha256,
            sha(receipt)?,
        )),
    }
}
impl PreparedHistoricalCatchupAuthority {
    /// Replay a previously committed archive phase using its original immutable receipt.
    /// An absent phase returns `None`; no new write occurs in that case.
    pub async fn replay_archive_effects(
        &self,
        original: &OperationalArchiveReceipt,
        binding: &ArchiveBinding,
    ) -> StorageResult<Option<VerifiedHistoricalNativeEffects>> {
        self.check_custody()?;
        validate_successor(&self.marker, original, binding)?;
        let Some(receipt) = &self.archive_receipt else {
            return Ok(None);
        };
        let p: Progress =
            serde_json::from_value(receipt.replay_output.clone().ok_or_else(refused)?)
                .map_err(crate::durability_io::io_error)?;
        if &p.original != original || &p.successor.binding != binding {
            return Err(refused());
        }
        replay(&self.client, &self.marker, receipt, &p)
            .await
            .map(Some)
    }
    /// Reserve a successor only after exact original report and archive verification.
    /// Pending partial files require registry-owned cleanup at their existing private path.
    pub async fn reserve_archive_successor(
        &self,
        report_path: &Path,
        binding: ArchiveBinding,
    ) -> StorageResult<HistoricalArchiveCandidate> {
        self.check_custody()?;
        if self.next_batch != self.marker.batches
            || self.archive_receipt.is_some()
            || !self.recovered_regeneration.is_empty()
            || completion::file_sha(report_path)? != self.marker.original_report_sha256
        {
            return Err(refused());
        }
        let file = std::fs::File::open(report_path).map_err(crate::durability_io::io_error)?;
        let mut raw = Vec::new();
        file.take(1024 * 1024 + 1)
            .read_to_end(&mut raw)
            .map_err(crate::durability_io::io_error)?;
        if raw.len() > 1024 * 1024 {
            return Err(refused());
        }
        let report = crate::transfer_batch::strict_json(
            std::str::from_utf8(&raw).map_err(crate::durability_io::io_error)?,
        )?;
        let original =
            crate::historical_catchup_ledger::archive_from_report(&report)?.ok_or_else(refused)?;
        if completion::file_sha(report_path)? != self.marker.original_report_sha256 {
            return Err(refused());
        }
        validate_successor(&self.marker, &original, &binding)?;
        crate::historical_archive_lifecycle::verify_receipt_current(&self.client, &original)
            .await?;
        let journal = self.client.erasure_journal()?;
        let verified_original = VerifiedArchive::open(
            &journal.archive_payload_path(&original.registration.generation_id)?,
            &original.binding,
            &original.seal,
        )?;
        let reg = registration(&self.client, &original, &binding)?;
        let entries = journal.operational_archive_entries()?;
        let prior = entries
            .into_iter()
            .find(|e| e.registration.generation_id == reg.generation_id);
        let path = if let Some(entry) = &prior {
            if entry.registration != reg {
                return Err(refused());
            }
            journal.archive_payload_path(&reg.generation_id)?
        } else {
            journal.reserve_operational_archive(reg.clone())?
        };
        let entry = journal.operational_archive_entry(&reg.generation_id)?;
        if !matches!(
            entry.state,
            ArchiveGenerationState::Pending | ArchiveGenerationState::Sealed { .. }
        ) {
            return Err(refused());
        }
        if matches!(entry.state, ArchiveGenerationState::Pending) {
            journal.reset_pending_archive_transfer(&entry, self)?;
        }
        // Sealed candidates resume via their exact seal; the CLI must not rebuild them.
        Ok(HistoricalArchiveCandidate {
            entry,
            binding,
            path,
            original,
            verified_original,
        })
    }
    /// Reopen already sealed bytes after interruption before the native transaction.
    pub fn recover_sealed_archive(
        &self,
        candidate: &HistoricalArchiveCandidate,
    ) -> StorageResult<Option<VerifiedArchive>> {
        self.check_custody()?;
        match &candidate.entry.state {
            ArchiveGenerationState::Sealed { evidence } => Ok(Some(VerifiedArchive::open(
                &candidate.path,
                &candidate.binding,
                &evidence.seal,
            )?)),
            ArchiveGenerationState::Pending => Ok(None),
            _ => Err(refused()),
        }
    }
    /// Commit the archive anchor, historical event cursors and canonical phase atomically.
    /// The journal may still hold the sealed predecessor after this returns. Call
    /// `replay_archive_effects` to reconcile that durable boundary and obtain the
    /// opaque effects for the original ledger before regeneration. A restart
    /// performs the same reconciliation from the immutable native primary.
    pub async fn commit_archive_successor(
        &mut self,
        candidate: HistoricalArchiveCandidate,
        verified: VerifiedArchive,
        ledger: &mut crate::historical_catchup_ledger::HistoricalCatchupLedger,
    ) -> StorageResult<()> {
        self.check_custody()?;
        if self.next_batch != self.marker.batches
            || self.archive_receipt.is_some()
            || !self.recovered_regeneration.is_empty()
            || verified.path() != candidate.path
            || verified.binding() != &candidate.binding
            || verified.seal().redacted
        {
            return Err(refused());
        }
        validate_successor(&self.marker, &candidate.original, verified.binding())?;
        verified.verify_successor(&candidate.verified_original, self.marker.accept_deletions)?;
        crate::historical_archive_lifecycle::verify_receipt_current(
            &self.client,
            &candidate.original,
        )
        .await?;
        self.client.erasure_journal()?.seal_operational_archive(
            &candidate.entry,
            seal_evidence(verified.binding(), verified.seal())?,
        )?;
        let p = Progress {
            version: 1,
            authority_binding_sha256: self.marker.authority_binding_sha256.clone(),
            original_report_sha256: self.marker.original_report_sha256.clone(),
            previous_batch_receipt_sha256: self.last_receipt.clone(),
            before_epoch: self.epoch.clone(),
            after_epoch: uuid::Uuid::new_v4().to_string(),
            original: candidate.original,
            successor: NativeArchive {
                version: 1,
                registration: candidate.entry.registration,
                binding: candidate.binding,
                seal: verified.seal().clone(),
            },
        };
        let source = self
            .receipt_sources()?
            .into_iter()
            .next()
            .ok_or_else(refused)?;
        let primary = CanonicalCommitReceipt {
            historical_import: None,
            space_id: self.marker.space_id.clone(),
            commit_id: commit_id(&self.marker),
            input_fingerprint: self.marker.authority_binding_sha256.clone(),
            plan_fingerprint: sha(&p)?,
            replay_output: Some(serde_json::to_value(&p).map_err(crate::durability_io::io_error)?),
            source_ids: vec![source.clone()],
            publication_authority: None,
            maintenance_authority: vec![],
            source_id: Some(source.clone()),
            source_revision: None,
            effect_ids: vec![],
            extension_ids: vec![],
            outbox_ids: vec![],
            committed_at: chrono::Utc::now(),
        };
        validate_primary(
            &self.marker,
            &primary,
            &self.epoch,
            self.last_receipt.as_deref(),
        )?;
        let inventory = identities(&self.marker, &p)?;
        let expected = ledger.archive_before_images(self, &inventory)?;
        let effect_binding = HistoricalNativeEffectBinding::new(
            &self.marker.authority_binding_sha256,
            "archive",
            0,
            &primary,
            &inventory,
        )?;
        let mut bindings = crate::bindings! {"space"=>self.marker.space_id.clone(),"epoch_id"=>control_thing("canonical_epoch",&self.marker.space_id),"before_epoch"=>self.epoch.clone(),"after_epoch"=>p.after_epoch.clone(),"primary_id"=>control_thing("source_commit_receipt",&stable_key(&[&self.marker.space_id,&primary.commit_id])),"primary"=>control_value(&self.marker.space_id,&source,&primary)?,"archive_id"=>native_id(&self.marker.space_id),"archive_consumer"=>RESERVED_ARCHIVE_CONSUMER,"archive_payload"=>serde_json::to_string(&p.successor).map_err(crate::durability_io::io_error)?};
        let mut sql=String::from("BEGIN TRANSACTION; IF (SELECT VALUE epoch FROM ONLY $epoch_id)!=$before_epoch { THROW 'archive_catchup_epoch_changed'; };\n");
        append_archive_before_images(&mut sql, &mut bindings, &inventory, expected)?;
        let prepared = effects::begin(
            &mut sql,
            &mut bindings,
            &source,
            effect_binding,
            &primary,
            inventory.clone(),
        )?;
        sql.push_str("UPDATE $archive_id CONTENT {space_id:$space,consumer:$archive_consumer,archive:$archive_payload} RETURN NONE;\n");
        append_archive_event_cursors(&mut sql, &mut bindings, &self.marker, &p)?;
        sql.push_str("UPSERT $epoch_id SET space_id=$space,epoch=$after_epoch; CREATE $primary_id CONTENT $primary RETURN NONE; IF (SELECT VALUE epoch FROM ONLY $epoch_id)!=$after_epoch { THROW 'archive_catchup_receipt_epoch_changed'; };\n");
        effects::finish(&mut sql, &prepared)?;
        sql.push_str("COMMIT TRANSACTION;");
        self.check_custody()?;
        self.client.query_bind_checked(&sql, bindings).await?;
        self.epoch = p.after_epoch;
        self.archive_receipt = Some(primary);
        Ok(())
    }
}
