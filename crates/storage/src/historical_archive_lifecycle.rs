//! Journal-owned operational archive custody, distinct from live work authority.
use crate::erasure_journal::{
    ArchiveGenerationState, ArchiveRegistration, ArchiveRegistryEntry, ArchiveSealEvidence,
};
use crate::historical_operational_archive::{ArchiveBinding, ArchiveSeal, VerifiedArchive};
use crate::{StorageClient, StorageError, StorageResult};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

/// Private native archive anchor; it is never a delivery acknowledgment.
pub const RESERVED_ARCHIVE_CONSUMER: &str = "__recall_operational_archive_v1";
const TABLE: &str = "outbox_checkpoint";
fn invalid(message: &str) -> StorageError {
    StorageError::DataIntegrityError(format!("managed archive: {message}"))
}
fn hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|v| v.is_ascii_digit() || (b'a'..=b'f').contains(&v))
}
pub(crate) fn seal_evidence(
    binding: &ArchiveBinding,
    seal: &ArchiveSeal,
) -> StorageResult<ArchiveSealEvidence> {
    let mut digest = Sha256::new();
    digest.update(b"recall-operational-archive-seal-v1\0");
    digest.update(serde_json::to_vec(seal).map_err(|e| invalid(&e.to_string()))?);
    Ok(ArchiveSealEvidence {
        binding: binding.clone(),
        seal: seal.clone(),
        seal_sha256: hex::encode(digest.finalize()),
        file_sha256: seal.file_sha256.clone(),
        bytes: seal.bytes,
    })
}
fn require_initial(client: &StorageClient, binding: &ArchiveBinding) -> StorageResult<()> {
    client.require_historical_transfer(&binding.snapshot_sha256)?;
    binding.validate()?;
    if client.is_embedded() {
        return Err(invalid(
            "managed archive requires native durable inventory custody",
        ));
    }
    if binding.journal_identity != client.erasure_journal()?.identity() {
        return Err(invalid("original journal differs"));
    }
    Ok(())
}
/// A path reserved durably before any original row bytes are created.
#[derive(Debug)]
pub struct RegisteredArchiveCandidate {
    pub(crate) entry: ArchiveRegistryEntry,
    pub(crate) binding: ArchiveBinding,
    pub(crate) path: PathBuf,
}
impl RegisteredArchiveCandidate {
    /// Private create-only destination for the core archive writer.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
    /// Original complete source binding used by that writer.
    #[must_use]
    pub const fn binding(&self) -> &ArchiveBinding {
        &self.binding
    }
    /// Seal independently verified bytes without granting completion or access.
    ///
    /// # Errors
    /// Rejects changed custody, registration, bytes, binding, or artifact identity.
    pub fn seal(
        self,
        client: &StorageClient,
        verified: VerifiedArchive,
    ) -> StorageResult<SealedOperationalArchive> {
        require_initial(client, &self.binding)?;
        if verified.path() != self.path
            || verified.binding() != &self.binding
            || verified.seal().binding_sha256 != self.binding.digest()?
        {
            return Err(invalid("candidate seal binding differs"));
        }
        let entry = client.erasure_journal()?.seal_operational_archive(
            &self.entry,
            seal_evidence(verified.binding(), verified.seal())?,
        )?;
        Ok(SealedOperationalArchive { entry, verified })
    }
}
/// Verified retained bytes under original custody. This cannot be constructed
/// from an event tail or public seal DTO and does not assert target readiness.
pub struct SealedOperationalArchive {
    pub(crate) entry: ArchiveRegistryEntry,
    pub(crate) verified: VerifiedArchive,
}
impl SealedOperationalArchive {
    /// Complete original binding; source and destination spaces stay distinct.
    #[must_use]
    pub const fn binding(&self) -> &ArchiveBinding {
        self.verified.binding()
    }
    /// Verified row populations and original namespace event floors.
    #[must_use]
    pub const fn seal(&self) -> &ArchiveSeal {
        self.verified.seal()
    }
    /// Revalidate live transfer custody and exact registry state before mutation.
    ///
    /// # Errors
    /// Rejects a stale candidate, erased source namespace, or different transfer.
    pub fn validate_custody(&self, client: &StorageClient) -> StorageResult<()> {
        require_initial(client, self.binding())?;
        let current = client
            .erasure_journal()?
            .operational_archive_entry(&self.entry.registration.generation_id)?;
        let same_seal = match (&self.entry.state, &current.state) {
            (
                ArchiveGenerationState::Sealed { evidence: a },
                ArchiveGenerationState::Sealed { evidence: b }
                | ArchiveGenerationState::Committed { evidence: b, .. },
            ) => a == b,
            _ => false,
        };
        if current.registration != self.entry.registration || !same_seal {
            return Err(invalid("sealed generation changed"));
        }
        // Transfer admission already requires an empty erasure journal. Keep the
        // exact requirement at the capability boundary as well.
        if !client.erasure_journal()?.intents()?.is_empty() {
            return Err(invalid(
                "original erasure intent prevents archive activation",
            ));
        }
        Ok(())
    }
}
/// Reserve an initial immutable generation while the destination is unready.
///
/// # Errors
/// Rejects absent transfer custody, foreign journal, previous lineage, or changed
/// original declaration. No archive row is written by this operation.
pub fn reserve_initial_archive(
    client: &StorageClient,
    binding: &ArchiveBinding,
) -> StorageResult<RegisteredArchiveCandidate> {
    require_initial(client, binding)?;
    if binding.parent_generation_sha256.is_some() {
        return Err(invalid("initial archive has predecessor"));
    }
    let journal = client.erasure_journal()?;
    let namespace = uuid::Uuid::parse_str(journal.identity())
        .map_err(|_| invalid("journal identity invalid"))?;
    let binding_sha256 = binding.digest()?;
    let lineage_id = uuid::Uuid::new_v5(
        &namespace,
        format!("operational-archive-lineage:{}", binding.target_space_id).as_bytes(),
    )
    .to_string();
    let generation_id = uuid::Uuid::new_v5(
        &namespace,
        format!("operational-archive-generation:{binding_sha256}").as_bytes(),
    )
    .to_string();
    let registration = ArchiveRegistration {
        lineage_id,
        generation_id: generation_id.clone(),
        parent_generation_id: None,
        binding_sha256,
        snapshot_sha256: binding.snapshot_sha256.clone(),
        journal_id: journal.identity().into(),
        source_space_id: binding.source_space_id.clone(),
        target_space_id: binding.target_space_id.clone(),
    };
    let path = journal.reserve_operational_archive(registration)?;
    let entry = journal.operational_archive_entry(&generation_id)?;
    Ok(RegisteredArchiveCandidate {
        entry,
        binding: binding.clone(),
        path,
    })
}
/// Retained archive identity returned to the exact migration inventory/report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationalArchiveReceipt {
    /// Original immutable registration, including explicit namespace projection.
    pub registration: ArchiveRegistration,
    /// Complete original source declaration.
    pub binding: ArchiveBinding,
    /// Exact verified retained artifact evidence.
    pub seal: ArchiveSeal,
    /// Full native control-row digest, including its original wrapper.
    pub native_receipt_sha256: String,
}
/// A native anchor plus only its exact guarded inventory effect.
#[derive(Debug)]
pub struct CommittedOperationalArchive {
    /// Serializable custody receipt; never a live work or delivery completion.
    pub receipt: OperationalArchiveReceipt,
    /// Exact native control mutation to reconcile with the destination inventory.
    pub effect: crate::historical_adoption::HistoricalRowEffect,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NativeArchive {
    pub(crate) version: u32,
    pub(crate) registration: ArchiveRegistration,
    pub(crate) binding: ArchiveBinding,
    pub(crate) seal: ArchiveSeal,
}
pub(crate) fn native_id(space: &str) -> surrealdb::sql::Thing {
    surrealdb::sql::Thing::from((
        TABLE,
        surrealdb::sql::Id::String(crate::outbox_checkpoint_repository::key(
            space,
            RESERVED_ARCHIVE_CONSUMER,
        )),
    ))
}
impl SealedOperationalArchive {
    /// Bind the verified generation to one exact native control under custody.
    /// The caller must include the returned effect in full reconciliation and
    /// fsync its report/ledger before the surrounding transfer releases Ready.
    ///
    /// # Errors
    /// Refuses epoch drift, any preexisting different anchor, or stale seal state.
    pub async fn commit(
        &self,
        client: &StorageClient,
        expected_epoch: &str,
    ) -> StorageResult<CommittedOperationalArchive> {
        self.validate_custody(client)?;
        if expected_epoch.is_empty() {
            return Err(invalid("missing original epoch"));
        }
        let space = &self.binding().target_space_id;
        let archive = NativeArchive {
            version: 1,
            registration: self.entry.registration.clone(),
            binding: self.binding().clone(),
            seal: self.seal().clone(),
        };
        let payload = serde_json::to_string(&archive).map_err(|e| invalid(&e.to_string()))?;
        let sql="BEGIN TRANSACTION;
LET $epoch = SELECT VALUE epoch FROM ONLY $epoch_id;
IF $epoch != $expected_epoch { THROW 'archive_epoch_changed'; };
LET $before = SELECT * FROM ONLY $id;
LET $after = { id:$id, space_id:$space, consumer:$consumer, archive:$archive };
IF $before != NONE AND $before != $after { THROW 'archive_native_anchor_changed'; };
IF $before = NONE { CREATE $id CONTENT $after RETURN NONE; };
LET $actual = SELECT * FROM ONLY $id;
IF $actual != $after { THROW 'archive_native_anchor_shape'; };
RETURN {table:'outbox_checkpoint',id:type::string($id),before_sha256:NONE,after_sha256:crypto::sha256(type::string($actual))};
COMMIT TRANSACTION;";
        let mut response=client.query_bind_checked(sql,crate::bindings! {
            "id"=>native_id(space), "epoch_id"=>surrealdb::sql::Thing::from(("canonical_epoch",space.as_str())),
            "space"=>space.clone(),"consumer"=>RESERVED_ARCHIVE_CONSUMER,"archive"=>payload,"expected_epoch"=>expected_epoch
        }).await?;
        let result = response
            .num_statements()
            .checked_sub(1)
            .ok_or_else(|| invalid("native archive result missing"))?;
        let effect: Option<crate::historical_adoption::HistoricalRowEffect> =
            response.take(result).map_err(|e| invalid(&e.to_string()))?;
        let effect = effect.ok_or_else(|| invalid("native archive effect missing"))?;
        if effect.table != TABLE
            || effect.id != native_id(space).to_string()
            || !hash(&effect.after_sha256)
        {
            return Err(invalid("native archive effect differs"));
        }
        client
            .erasure_journal()?
            .commit_operational_archive(&self.entry, &effect.after_sha256)?;
        Ok(CommittedOperationalArchive {
            receipt: OperationalArchiveReceipt {
                registration: self.entry.registration.clone(),
                binding: self.binding().clone(),
                seal: self.seal().clone(),
                native_receipt_sha256: effect.after_sha256.clone(),
            },
            effect,
        })
    }
}

/// Read-only startup check of complete managed archive heads. A valid older
/// registry cannot hide a newer native cut, and a new registry cannot validate
/// against a database restored before that cut. Pending generations grant no
/// readiness or payload access and are not mistaken for native commitments.
///
/// # Errors
/// Rejects mismatched native wrapper/registration/seal, missing committed head,
/// unregistered native anchor, or an archive-enabled embedded restore.
pub(crate) async fn verify_native_archive_heads(client: &StorageClient) -> StorageResult<()> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct NativeRow {
        space_id: String,
        consumer: String,
        archive: String,
    }
    #[derive(Deserialize)]
    struct Projected {
        id: String,
        space_id: String,
        consumer: String,
        archive: String,
        native_sha256: String,
    }
    let journal = client.erasure_journal()?;
    let heads = journal.operational_archive_heads()?;
    journal.verify_current_archive_files()?;
    if client.is_embedded() {
        if !heads.is_empty() {
            return Err(invalid(
                "native archive cannot restore into embedded backend",
            ));
        }
        return Ok(());
    }
    let mut response=client.query_bind_checked(
        "SELECT type::string(id) AS id,space_id,consumer,archive,crypto::sha256(type::string($this)) AS native_sha256 FROM outbox_checkpoint WHERE consumer=$consumer;",
        crate::bindings! {"consumer"=>RESERVED_ARCHIVE_CONSUMER}).await?;
    let rows: Vec<Projected> = response.take(0).map_err(|e| invalid(&e.to_string()))?;
    if rows.len() != heads.len() {
        return Err(invalid("native/archive generation population differs"));
    }
    let mut seen = std::collections::BTreeSet::new();
    for row in rows {
        let original: NativeArchive = serde_json::from_str(&row.archive)
            .map_err(|_| invalid("native archive payload malformed"))?;
        let entry = heads
            .iter()
            .find(|e| e.registration.generation_id == original.registration.generation_id)
            .ok_or_else(|| invalid("native archive generation not retained"))?;
        let (ArchiveGenerationState::Committed {
            evidence,
            native_receipt_sha256: native_receipt,
        }
        | ArchiveGenerationState::Redacting {
            evidence,
            native_receipt_sha256: native_receipt,
            ..
        }
        | ArchiveGenerationState::Redacted {
            evidence,
            native_receipt_sha256: native_receipt,
            ..
        }) = &entry.state
        else {
            return Err(invalid("native archive registry state differs"));
        };
        if !seen.insert(entry.registration.lineage_id.clone())
            || original.version != 1
            || original.registration != entry.registration
            || original.binding.digest()? != entry.registration.binding_sha256
            || original.binding.journal_identity != journal.identity()
            || original.binding.source_space_id != entry.registration.source_space_id
            || original.binding.target_space_id != entry.registration.target_space_id
            || original.binding.snapshot_sha256 != entry.registration.snapshot_sha256
            || seal_evidence(&original.binding, &original.seal)? != *evidence
            || row.native_sha256 != *native_receipt
            || row.id != native_id(&entry.registration.target_space_id).to_string()
            || row.space_id != entry.registration.target_space_id
            || row.consumer != RESERVED_ARCHIVE_CONSUMER
        {
            return Err(invalid("native archive current binding differs"));
        }
        // Compare the complete stored native image, including any unexpected
        // added field. Typed SELECT projection alone must not hide corruption.
        let expected = NativeRow {
            space_id: row.space_id,
            consumer: row.consumer,
            archive: row.archive,
        };
        let mut query=client.query_bind_checked(
            "SELECT VALUE $this = {id:$id,space_id:$space,consumer:$consumer,archive:$archive} FROM ONLY $id;",
            crate::bindings! {"id"=>native_id(&expected.space_id),"space"=>expected.space_id,"consumer"=>expected.consumer,"archive"=>expected.archive}).await?;
        let matches: Option<bool> = query.take(0).map_err(|e| invalid(&e.to_string()))?;
        if matches != Some(true) {
            return Err(invalid("native archive wrapper differs"));
        }
    }
    Ok(())
}

/// Reconstruct the exact sealed capability after interruption. A different
/// original binding or artifact cannot repair the native/journal boundary.
///
/// # Errors
/// Requires the original live transfer, registered sealed/committed bytes and
/// exact complete archive verification. It never removes the unready marker.
pub fn recover_initial_archive(
    client: &StorageClient,
    binding: &ArchiveBinding,
) -> StorageResult<SealedOperationalArchive> {
    require_initial(client, binding)?;
    let journal = client.erasure_journal()?;
    let digest = binding.digest()?;
    let entries = journal.operational_archive_entries()?;
    let entry = entries
        .into_iter()
        .find(|e| e.registration.binding_sha256 == digest)
        .ok_or_else(|| invalid("original sealed generation not registered"))?;
    let evidence = match &entry.state {
        ArchiveGenerationState::Sealed { evidence }
        | ArchiveGenerationState::Committed { evidence, .. } => evidence.clone(),
        _ => return Err(invalid("original candidate is not sealed")),
    };
    if evidence.binding != *binding {
        return Err(invalid("original binding changed"));
    }
    let verified = VerifiedArchive::open(
        &journal.archive_payload_path(&entry.registration.generation_id)?,
        binding,
        &evidence.seal,
    )?;
    // Retain the immutable sealed predecessor for idempotent commit replay. The
    // registry helper accepts only the exact already-committed successor.
    let sealed = ArchiveRegistryEntry {
        registration: entry.registration,
        state: ArchiveGenerationState::Sealed { evidence },
    };
    let result = SealedOperationalArchive {
        entry: sealed,
        verified,
    };
    result.validate_custody(client)?;
    Ok(result)
}

/// Verify the exact current managed receipt against both original registry and
/// native authority before completion or administrative archive access.
///
/// # Errors
/// Rejects stale, erased, foreign, malformed, or fabricated receipt authority.
pub async fn verify_receipt_current(
    client: &StorageClient,
    receipt: &OperationalArchiveReceipt,
) -> StorageResult<()> {
    verify_native_archive_heads(client).await?;
    let journal = client.erasure_journal()?;
    let entry = journal.operational_archive_entry(&receipt.registration.generation_id)?;
    if entry.registration != receipt.registration
        || !journal.operational_archive_heads()?.contains(&entry)
        || entry.evidence() != Some(&seal_evidence(&receipt.binding, &receipt.seal)?)
        || entry.native_receipt_sha256() != Some(receipt.native_receipt_sha256.as_str())
    {
        return Err(invalid("current receipt differs"));
    }
    let resolved = journal.resolve_operational_archive_access(&entry)?;
    let evidence = resolved
        .evidence()
        .ok_or_else(|| invalid("resolved archive unsealed"))?;
    let archive = VerifiedArchive::open(
        &journal.archive_payload_path(&resolved.registration.generation_id)?,
        &evidence.binding,
        &evidence.seal,
    )?;
    crate::source_lifecycle::verify_archive_erasure_access(journal, &archive)?;
    Ok(())
}

/// Return the exact native source-cut receipt for the selected archive lineage.
/// Redaction preserves this identity while access resolves to verified survivors.
///
/// # Errors
/// Rejects inconsistent native/registry heads or more than one matching lineage.
pub async fn current_operational_archive_receipt(
    client: &StorageClient,
    space: &str,
) -> StorageResult<Option<OperationalArchiveReceipt>> {
    verify_native_archive_heads(client).await?;
    let entries = client.erasure_journal()?.operational_archive_heads()?;
    let mut matches = entries
        .iter()
        .filter(|entry| entry.registration.target_space_id == space);
    let Some(entry) = matches.next() else {
        return Ok(None);
    };
    if matches.next().is_some() {
        return Err(invalid("duplicate namespace archive lineage"));
    }
    let evidence = entry
        .evidence()
        .ok_or_else(|| invalid("archive head unsealed"))?;
    Ok(Some(OperationalArchiveReceipt {
        registration: entry.registration.clone(),
        binding: evidence.binding.clone(),
        seal: evidence.seal.clone(),
        native_receipt_sha256: entry
            .native_receipt_sha256()
            .ok_or_else(|| invalid("archive head uncommitted"))?
            .into(),
    }))
}

/// A synchronous reader borrowed only while ordinary storage admission remains
/// held. Its paths and construction are private; pages contain original bytes.
pub struct ManagedArchiveRead {
    original: OperationalArchiveReceipt,
    resolved: OperationalArchiveReceipt,
    archive: VerifiedArchive,
}
impl ManagedArchiveRead {
    /// Original source-cut receipt, still bound to the exact native anchor.
    #[must_use]
    pub const fn original_receipt(&self) -> &OperationalArchiveReceipt {
        &self.original
    }
    /// Resolved surviving generation. Its native digest identifies the original
    /// cut; redaction authority comes from the retained journal chain.
    #[must_use]
    pub const fn receipt(&self) -> &OperationalArchiveReceipt {
        &self.resolved
    }
    /// Exact declaration of the resolved surviving artifact.
    #[must_use]
    pub const fn binding(&self) -> &ArchiveBinding {
        self.archive.binding()
    }
    /// Exact surviving table populations and original lexical inventory digest.
    #[must_use]
    pub const fn seal(&self) -> &ArchiveSeal {
        self.archive.seal()
    }
    /// Return one bounded page while the caller retains the read callback.
    ///
    /// # Errors
    /// Rejects unknown table, invalid limit/cursor, or changed physical bytes.
    pub fn page(
        &self,
        table: &str,
        after: Option<&str>,
        limit: usize,
    ) -> StorageResult<Vec<crate::historical_operational_archive::ArchiveRow>> {
        self.archive.page(table, after, limit)
    }
}
/// Read current archive survivors under admission custody.
///
/// Resolve the original report and hold admission across the entire synchronous
/// administrative callback. Physical erasure cannot start between validation
/// and returning original payload bytes.
///
/// # Errors
/// Rejects unavailable storage, wrong receipt, incomplete redaction, missing or
/// changed bytes, or surviving rows referencing any original erasure intent.
pub async fn with_operational_archive_read<T>(
    client: &StorageClient,
    receipt: &OperationalArchiveReceipt,
    action: impl FnOnce(&ManagedArchiveRead) -> StorageResult<T>,
) -> StorageResult<T> {
    client
        .with_activity(async {
            verify_receipt_current(client, receipt).await?;
            let journal = client.erasure_journal()?;
            let original =
                journal.operational_archive_entry(&receipt.registration.generation_id)?;
            let resolved = journal.resolve_operational_archive_access(&original)?;
            let evidence = resolved
                .evidence()
                .ok_or_else(|| invalid("resolved archive unsealed"))?;
            let archive = VerifiedArchive::open(
                &journal.archive_payload_path(&resolved.registration.generation_id)?,
                &evidence.binding,
                &evidence.seal,
            )?;
            let reader = ManagedArchiveRead {
                original: receipt.clone(),
                resolved: OperationalArchiveReceipt {
                    registration: resolved.registration.clone(),
                    binding: evidence.binding.clone(),
                    seal: evidence.seal.clone(),
                    native_receipt_sha256: resolved
                        .native_receipt_sha256()
                        .ok_or_else(|| invalid("resolved archive uncommitted"))?
                        .into(),
                },
                archive,
            };
            action(&reader)
        })
        .await?
}

#[path = "historical_archive_backup.rs"]
pub(crate) mod backup;

// This exact current-only shape is also used by native history preservation.
// It does not authorize older versions or operational dispatch.
pub(crate) fn verify_retained_native_anchor(
    journal: &crate::erasure_journal::ErasureJournal,
    space: &str,
    id: &surrealdb::sql::Thing,
    fields: &surrealdb::sql::Object,
) -> StorageResult<()> {
    use surrealdb::sql::Value;
    let Value::Strand(encoded) = fields
        .get("archive")
        .ok_or_else(|| invalid("retained archive payload absent"))?
    else {
        return Err(invalid("retained archive payload type"));
    };
    let payload: NativeArchive = serde_json::from_str(encoded.as_str())
        .map_err(|_| invalid("retained archive payload malformed"))?;
    let entry = journal.operational_archive_entry(&payload.registration.generation_id)?;
    if fields.len() != 4
        || id != &native_id(space)
        || fields.get("id") != Some(&Value::Thing(id.clone()))
        || fields.get("space_id") != Some(&Value::from(space))
        || fields.get("consumer") != Some(&Value::from(RESERVED_ARCHIVE_CONSUMER))
        || payload.version != 1
        || payload.registration != entry.registration
        || entry.registration.target_space_id != space
        || entry.evidence() != Some(&seal_evidence(&payload.binding, &payload.seal)?)
        || serde_json::to_string(&payload).map_err(|e| invalid(&e.to_string()))? != encoded.as_str()
        || !journal.operational_archive_heads()?.contains(&entry)
        || entry.native_receipt_sha256()
            != Some(
                hex::encode(Sha256::digest(
                    Value::Object(fields.clone()).to_string().as_bytes(),
                ))
                .as_str(),
            )
    {
        return Err(invalid("retained archive native image differs"));
    }
    Ok(())
}
