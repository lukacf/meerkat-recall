//! The exact original archive inventory joins the native erasure fixed point.
use super::{
    conflict, integrity, BTreeMap, BTreeSet, ErasureIntent, InventoryRow, LifecyclePlan, RecordId,
    StorageResult, Thing,
};
#[cfg(test)]
use super::{BaseFields, TableName, Utc, Uuid};
use crate::erasure_journal::{ArchiveGenerationState, ArchiveRegistryEntry};
use crate::historical_operational_archive::{ArchiveRow, VerifiedArchive, MAX_PAGE_ROWS};
#[cfg(test)]
use std::str::FromStr;

const MAX_PHYSICAL_ROWS: u64 = 8_000_000;
const MAX_SELECTED: usize = 2_000_000;

pub(super) struct RegisteredGeneration {
    entry: ArchiveRegistryEntry,
    archive: VerifiedArchive,
}
#[derive(Default)]
pub(super) struct ArchiveSelection {
    // Generation + exact original table/id/hash. All original bytes stay in the
    // immutable artifact until a durable original intent permits redaction.
    rows: BTreeMap<String, BTreeMap<(String, String), String>>,
    identities: BTreeSet<RecordId>,
    affected_lineages: BTreeSet<String>,
}
impl ArchiveSelection {
    pub(super) fn identities(&self) -> impl Iterator<Item = Thing> + '_ {
        self.identities
            .iter()
            .map(crate::surreal::thing_from_record_id)
    }
}
pub(super) async fn open_registered(
    client: &crate::StorageClient,
) -> StorageResult<Vec<RegisteredGeneration>> {
    crate::historical_archive_lifecycle::verify_native_archive_heads(client).await?;
    let journal = client.erasure_journal()?;
    // Complete only an already prepared removal under this exact original
    // erasure reservation. Missing originals otherwise remain fail-closed.
    for entry in journal.operational_archive_entries()? {
        if let ArchiveGenerationState::Redacting {
            intent_id,
            prepared: Some(_),
            ..
        } = &entry.state
        {
            if crate::client::current_erasure_reservation(journal.identity()).as_deref()
                == Some(intent_id.as_str())
            {
                journal.finish_archive_redaction(&entry)?;
            }
            // A different original intent retains its own custody. Its exact
            // prepared successor is already validated by journal open; scan
            // that survivor below and let its own replay finish the unlink.
        }
    }
    let entries = journal.operational_archive_entries()?;
    let mut result = Vec::new();
    for entry in &entries {
        if matches!(
            entry.state,
            ArchiveGenerationState::Redacted { .. }
                | ArchiveGenerationState::Redacting {
                    prepared: Some(_),
                    ..
                }
        ) {
            continue;
        }
        if matches!(
            entry.state,
            ArchiveGenerationState::Pending | ArchiveGenerationState::Sealed { .. }
        ) {
            // A registered partial successor is replayed from the still-retained
            // original and its durable identity memberships, never read as data.
            let parent = entry
                .registration
                .parent_generation_id
                .as_ref()
                .and_then(|id| entries.iter().find(|e| &e.registration.generation_id == id));
            if parent.is_some_and(|e| matches!(&e.state, ArchiveGenerationState::Redacting { intent_id, prepared: None, .. }
                if crate::client::current_erasure_reservation(journal.identity()).as_deref() == Some(intent_id.as_str()))) {
                continue;
            }
            return Err(integrity("uncommitted archive prevents physical erasure"));
        }
        let evidence = entry
            .evidence()
            .ok_or_else(|| integrity("unsealed archive prevents physical erasure"))?;
        let archive = VerifiedArchive::open(
            &journal.archive_payload_path(&entry.registration.generation_id)?,
            &evidence.binding,
            &evidence.seal,
        )?;
        result.push(RegisteredGeneration {
            entry: entry.clone(),
            archive,
        });
    }
    Ok(result)
}
fn inventory_row(
    original: &ArchiveRow,
    binding: &crate::historical_operational_archive::ArchiveBinding,
    space: &str,
) -> StorageResult<Option<InventoryRow>> {
    let metadata = original.metadata()?;
    let projected = if metadata.original_space == binding.source_space_id {
        &binding.target_space_id
    } else {
        &metadata.original_space
    };
    if projected != space {
        return Ok(None);
    }
    let lexical =
        std::str::from_utf8(&original.lexical).map_err(|_| integrity("archive row is not UTF8"))?;
    let mut record = crate::transfer_batch::TransferRecord::from_json(&original.table, lexical)?;
    record.relocate_space(&binding.source_space_id, &binding.target_space_id)?;
    let id = record.id().clone();
    // Full original JSON supplies legacy recursive exact-string references;
    // typed native projection supplies known native identity/token semantics.
    // Neither is replaced by the incomplete direct-reference acceleration index.
    Ok(Some(InventoryRow {
        table: original.table.clone(),
        key: original.id.clone(),
        native_id: Some(crate::surreal::thing_from_record_id(&id)),
        record_id: Some(id),
        value: crate::transfer_batch::strict_json(lexical)?,
        native_value: Some(record.native()?),
    }))
}
pub(super) fn extend_selection(
    archives: &[RegisteredGeneration],
    journal: &crate::erasure_journal::ErasureJournal,
    space: &str,
    historical_membership: &BTreeSet<String>,
    plan: &mut LifecyclePlan,
) -> StorageResult<bool> {
    let mut changed = false;
    let mut scanned = 0_u64;
    for generation in archives {
        for table in generation.archive.seal().table_counts.keys() {
            let mut cursor = None;
            loop {
                let rows = generation
                    .archive
                    .page(table, cursor.as_deref(), MAX_PAGE_ROWS)?;
                if rows.is_empty() {
                    break;
                }
                for original in &rows {
                    scanned = scanned
                        .checked_add(1)
                        .ok_or_else(|| integrity("archive closure count overflow"))?;
                    if scanned > MAX_PHYSICAL_ROWS {
                        return Err(integrity("archive closure physical row bound"));
                    }
                    let Some(row) = inventory_row(original, generation.archive.binding(), space)?
                    else {
                        continue;
                    };
                    if !row.references_erased_identity(
                        journal,
                        space,
                        plan,
                        historical_membership,
                    )? {
                        continue;
                    }
                    if row.table == "cost_ledger_daily" {
                        return Err(conflict(
                            "source_erasure_requires_shared_aggregate_reconciliation",
                        ));
                    }
                    let selections = plan
                        .archive_deletes
                        .rows
                        .entry(generation.entry.registration.generation_id.clone())
                        .or_default();
                    let key = (original.table.clone(), original.id.clone());
                    if let Some(prior) = selections.get(&key) {
                        if prior != &original.lexical_sha256 {
                            return Err(integrity("archive selected row changed"));
                        }
                        continue;
                    }
                    selections.insert(key, original.lexical_sha256.clone());
                    plan.archive_deletes
                        .affected_lineages
                        .insert(generation.entry.registration.lineage_id.clone());
                    let id = row
                        .record_id
                        .as_ref()
                        .ok_or_else(|| integrity("archive typed identity absent"))?;
                    plan.archive_deletes.identities.insert(id.clone());
                    if plan.archive_deletes.identities.len() > MAX_SELECTED {
                        return Err(integrity("archive selected identity bound"));
                    }
                    changed |= plan.removed.insert(id.id());
                    changed |= plan.removed_keys.insert(row.reference_key());
                    changed |= plan.removed_identity_tokens.insert(
                        crate::reference_fence::reference_identity_token(
                            journal.identity(),
                            space,
                            journal.reference_key_bytes(),
                            &row.physical_identity()?,
                        )?,
                    );
                }
                cursor = rows.last().map(|row| row.id.clone());
            }
        }
    }
    Ok(changed)
}

// Reuse the native recursive identity semantics for every actual row namespace.
// An old but correctly hashed registry therefore cannot re-enable erased rows.
pub(super) fn verify_erasure_access(
    journal: &crate::erasure_journal::ErasureJournal,
    archive: &VerifiedArchive,
) -> StorageResult<()> {
    let mut restrictions = BTreeMap::<String, (LifecyclePlan, BTreeSet<String>)>::new();
    for intent in journal.intents()? {
        let (plan, memberships) = restrictions.entry(intent.space_id.clone()).or_default();
        plan.removed.insert(intent.source_id.id());
        plan.removed_keys.insert(intent.source_id.to_string());
        memberships.extend(journal.reference_membership(&intent)?);
    }
    if restrictions.is_empty() {
        return Ok(());
    }
    let mut scanned = 0_u64;
    for table in archive.seal().table_counts.keys() {
        let mut cursor = None;
        loop {
            let rows = archive.page(table, cursor.as_deref(), MAX_PAGE_ROWS)?;
            if rows.is_empty() {
                break;
            }
            for original in &rows {
                scanned += 1;
                if scanned > MAX_PHYSICAL_ROWS {
                    return Err(integrity("archive access physical row bound"));
                }
                let metadata = original.metadata()?;
                let space = if metadata.original_space == archive.binding().source_space_id {
                    &archive.binding().target_space_id
                } else {
                    &metadata.original_space
                };
                let Some((plan, memberships)) = restrictions.get(space) else {
                    continue;
                };
                let row = inventory_row(original, archive.binding(), space)?
                    .ok_or_else(|| integrity("archive row namespace changed"))?;
                if row.references_erased_identity(journal, space, plan, memberships)? {
                    return Err(integrity(
                        "archive contains an erased identity or dependency",
                    ));
                }
            }
            cursor = rows.last().map(|r| r.id.clone());
        }
    }
    Ok(())
}

// Hash/census is independent of the redaction writer. Include explicit empty
// tables and all surviving original lexical hashes, including foreign spaces.
fn survivor_proof(
    archive: &VerifiedArchive,
    selections: &BTreeMap<(String, String), String>,
    intent: &ErasureIntent,
) -> StorageResult<(String, String)> {
    use sha2::{Digest, Sha256};
    let mut inventory = crate::historical_operational_archive::ArchiveInventoryDigest::default();
    let mut removed = Sha256::new();
    removed.update(b"recall-operational-archive-erasure-selection-v1\0");
    removed.update(
        serde_json::to_vec(&(
            archive.binding().digest()?,
            &archive.seal().file_sha256,
            &intent.intent_id,
        ))
        .map_err(|e| integrity(&e.to_string()))?,
    );
    let mut seen = 0_usize;
    let mut physical = 0_u64;
    for (table, count) in &archive.seal().table_counts {
        let removing = selections.keys().filter(|(t, _)| t == table).count() as u64;
        inventory.table(
            table,
            count
                .checked_sub(removing)
                .ok_or_else(|| integrity("archive removal count differs"))?,
        )?;
        let mut cursor = None;
        loop {
            let rows = archive.page(table, cursor.as_deref(), MAX_PAGE_ROWS)?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                physical += 1;
                if physical > MAX_PHYSICAL_ROWS {
                    return Err(integrity("archive survivor physical bound"));
                }
                if let Some(expected) = selections.get(&(table.clone(), row.id.clone())) {
                    if expected != &row.lexical_sha256 {
                        return Err(integrity("archive removal original differs"));
                    }
                    removed.update(
                        serde_json::to_vec(&(table, &row.id, expected))
                            .map_err(|e| integrity(&e.to_string()))?,
                    );
                    seen += 1;
                } else {
                    inventory.row(table, &row.id, &row.lexical_sha256, row.lexical.len())?;
                }
            }
            cursor = rows.last().map(|row| row.id.clone());
        }
    }
    if seen != selections.len() {
        return Err(integrity("archive removal identity absent"));
    }
    Ok((hex::encode(removed.finalize()), inventory.finish()?))
}

fn applied_in_ancestry(
    entries: &[ArchiveRegistryEntry],
    entry: &ArchiveRegistryEntry,
    intent: &ErasureIntent,
) -> StorageResult<bool> {
    let indexed: BTreeMap<_, _> = entries
        .iter()
        .map(|e| (&e.registration.generation_id, e))
        .collect();
    let mut current = entry;
    let mut seen = BTreeSet::new();
    while let Some(parent_id) = &current.registration.parent_generation_id {
        if !seen.insert(&current.registration.generation_id) || seen.len() > 4096 {
            return Err(integrity("archive completed-intent ancestry cycle/bound"));
        }
        let parent = indexed
            .get(parent_id)
            .ok_or_else(|| integrity("archive completed-intent ancestor absent"))?;
        let applied = match &parent.state {
            ArchiveGenerationState::Redacted {
                intent_id,
                replacement_generation_id,
                ..
            } => {
                intent_id == &intent.intent_id
                    && replacement_generation_id == &current.registration.generation_id
            }
            ArchiveGenerationState::Redacting {
                intent_id,
                prepared: Some(proof),
                ..
            } => {
                intent_id == &intent.intent_id
                    && proof.replacement_generation_id == current.registration.generation_id
            }
            _ => false,
        };
        if applied {
            return Ok(true);
        }
        current = parent;
    }
    Ok(false)
}

pub(super) async fn redact_selected(
    client: &crate::StorageClient,
    intent: &ErasureIntent,
    selections: &ArchiveSelection,
) -> StorageResult<()> {
    let journal = client.erasure_journal()?;
    if crate::client::current_erasure_reservation(journal.identity()).as_deref()
        != Some(intent.intent_id.as_str())
        || !journal.intents()?.contains(intent)
    {
        return Err(integrity(
            "archive redaction lacks original erasure custody",
        ));
    }
    crate::historical_archive_lifecycle::verify_native_archive_heads(client).await?;
    let entries = journal.operational_archive_entries()?;
    let mut lineages = selections.affected_lineages.clone();
    for entry in &entries {
        if matches!(&entry.state, ArchiveGenerationState::Redacting { intent_id, .. } | ArchiveGenerationState::Redacted { intent_id, .. } if intent_id == &intent.intent_id)
        {
            lineages.insert(entry.registration.lineage_id.clone());
        }
    }
    // Snapshot this complete population before making replacements. Every
    // retained generation in an affected lineage is rewritten, even one with no
    // visible selected row: a prior logical DELETE can leave SQLite freelist bytes.
    for entry in &entries {
        if !lineages.contains(&entry.registration.lineage_id)
            || matches!(
                entry.state,
                ArchiveGenerationState::Pending
                    | ArchiveGenerationState::Sealed { .. }
                    | ArchiveGenerationState::Redacted { .. }
            )
        {
            continue;
        }
        let newly_selected = selections
            .rows
            .get(&entry.registration.generation_id)
            .is_some_and(|rows| !rows.is_empty());
        if !newly_selected && applied_in_ancestry(&entries, entry, intent)? {
            continue;
        }
        redact_generation(journal, intent, selections, entry)?;
    }
    crate::historical_archive_lifecycle::verify_native_archive_heads(client).await?;
    Ok(())
}

fn redact_generation(
    journal: &crate::erasure_journal::ErasureJournal,
    intent: &ErasureIntent,
    selections: &ArchiveSelection,
    entry: &ArchiveRegistryEntry,
) -> StorageResult<()> {
    let original = match &entry.state {
        ArchiveGenerationState::Committed { .. } => {
            journal.deny_operational_archive_for_intent(entry, intent)?
        }
        ArchiveGenerationState::Redacting { intent_id, .. } if intent_id == &intent.intent_id => {
            entry.clone()
        }
        _ => return Err(integrity("archive redaction belongs to another intent")),
    };
    if matches!(
        original.state,
        ArchiveGenerationState::Redacting {
            prepared: Some(_),
            ..
        }
    ) {
        journal.finish_archive_redaction(&original)?;
        return Ok(());
    }
    let evidence = original
        .evidence()
        .ok_or_else(|| integrity("archive redaction original unsealed"))?;
    let archive = VerifiedArchive::open(
        &journal.archive_payload_path(&original.registration.generation_id)?,
        &evidence.binding,
        &evidence.seal,
    )?;
    let empty = BTreeMap::new();
    let selected = selections
        .rows
        .get(&original.registration.generation_id)
        .unwrap_or(&empty);
    let (removed_identity_sha256, preserved_inventory_sha256) =
        survivor_proof(&archive, selected, intent)?;
    let mut binding = archive.binding().clone();
    binding.parent_generation_sha256 = Some(archive.seal().file_sha256.clone());
    let (candidate, path) = journal.reserve_archive_redaction(&original, intent, binding)?;
    let successor = match &candidate.state {
        ArchiveGenerationState::Pending => {
            journal.reset_pending_archive_redaction(&candidate, intent)?;
            let verified = crate::historical_operational_archive::ArchiveBuilder::redact(
                &path,
                &archive,
                |row, _| match selected.get(&(row.table.clone(), row.id.clone())) {
                    Some(hash) if hash == &row.lexical_sha256 => Ok(true),
                    Some(_) => Err(integrity("archive redaction row changed")),
                    None => Ok(false),
                },
            )?;
            if verified.seal().ordered_inventory_sha256 != preserved_inventory_sha256 {
                return Err(integrity("archive survivors differ"));
            }
            journal.seal_operational_archive(
                &candidate,
                crate::historical_archive_lifecycle::seal_evidence(
                    verified.binding(),
                    verified.seal(),
                )?,
            )?
        }
        ArchiveGenerationState::Sealed { evidence } => {
            let verified = VerifiedArchive::open(&path, &evidence.binding, &evidence.seal)?;
            if !verified.seal().redacted
                || verified.seal().ordered_inventory_sha256 != preserved_inventory_sha256
            {
                return Err(integrity("archive recovered survivors differ"));
            }
            candidate.clone()
        }
        _ => return Err(integrity("archive redaction successor disposition differs")),
    };
    let prepared = journal.prepare_archive_redaction(
        &original,
        &successor,
        crate::erasure_journal::ArchiveRedactionProof {
            replacement_generation_id: successor.registration.generation_id.clone(),
            removed_identity_sha256,
            preserved_inventory_sha256,
        },
    )?;
    journal.finish_archive_redaction(&prepared)?;
    Ok(())
}

#[cfg(test)]
#[path = "source_lifecycle_archive_tests.rs"]
mod tests;
