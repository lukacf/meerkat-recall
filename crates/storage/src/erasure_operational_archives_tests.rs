use super::*;
use crate::historical_operational_archive::{
    ArchiveBinding, ArchiveBuilder, ArchiveRow, VerifiedArchive,
};
use elephant_types::records::{AuditAction, AuditLog, BaseFields};
use elephant_types::{RecordId, TableName};

fn row(name: &str, space: &str, source: &RecordId, marker: &str) -> StorageResult<ArchiveRow> {
    let record = AuditLog {
        base: BaseFields::new(
            RecordId::new(
                TableName::AuditLog,
                uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, name.as_bytes()),
            ),
            space.into(),
            "archive-lifecycle-test".into(),
        ),
        action: AuditAction::Update,
        actor: "fixture".into(),
        record: RecordId::new_v4(TableName::Entity),
        before: None,
        after: Some(serde_json::json!({"nested":{"source":source.to_string()},"retained":marker})),
        timestamp: chrono::Utc::now(),
        reason: None,
    };
    let lexical = serde_json::to_vec_pretty(&record).map_err(io_error)?;
    Ok(ArchiveRow {
        table: "audit_log".into(),
        id: record.base.id.to_string(),
        lexical_sha256: hex::encode(Sha256::digest(&lexical)),
        lexical,
    })
}
fn committed(journal: &ErasureJournal, rows: &[ArchiveRow]) -> StorageResult<ArchiveRegistryEntry> {
    let binding = ArchiveBinding {
        version: 1,
        snapshot_sha256: "1".repeat(64),
        spool_sha256: "2".repeat(64),
        plan_id: "3".repeat(64),
        source_space_id: "default".into(),
        target_space_id: "household".into(),
        journal_identity: journal.identity().into(),
        disposition_sha256: ArchiveBinding::disposition_digest(),
        parent_generation_sha256: None,
        original_table_declarations: BTreeMap::from([
            ("audit_log".into(), rows.len() as u64),
            ("work_edge".into(), 0),
        ]),
    };
    let generation = uuid::Uuid::new_v4().to_string();
    let registration = ArchiveRegistration {
        lineage_id: uuid::Uuid::new_v4().to_string(),
        generation_id: generation.clone(),
        parent_generation_id: None,
        binding_sha256: binding.digest()?,
        snapshot_sha256: binding.snapshot_sha256.clone(),
        journal_id: journal.identity().into(),
        source_space_id: binding.source_space_id.clone(),
        target_space_id: binding.target_space_id.clone(),
    };
    let path = journal.reserve_operational_archive(registration)?;
    let pending = journal.operational_archive_entry(&generation)?;
    let mut writer = ArchiveBuilder::create(&path, binding)?;
    writer.append(rows)?;
    let archive = writer.seal()?;
    let sealed = journal.seal_operational_archive(
        &pending,
        crate::historical_archive_lifecycle::seal_evidence(archive.binding(), archive.seal())?,
    )?;
    journal.commit_operational_archive(&sealed, &"a".repeat(64))
}
fn prepared(
    journal: &ErasureJournal,
    original: &ArchiveRegistryEntry,
    intent: &super::super::ErasureIntent,
    remove: &str,
) -> StorageResult<(ArchiveRegistryEntry, ArchiveRegistryEntry)> {
    let denied = journal.deny_operational_archive_for_intent(original, intent)?;
    let evidence = original
        .evidence()
        .ok_or_else(|| io_error("fixture evidence"))?;
    let archive = VerifiedArchive::open(
        &journal.archive_payload_path(&original.registration.generation_id)?,
        &evidence.binding,
        &evidence.seal,
    )?;
    let mut binding = archive.binding().clone();
    binding.parent_generation_sha256 = Some(archive.seal().file_sha256.clone());
    let (pending, path) = journal.reserve_archive_redaction(&denied, intent, binding)?;
    let replacement = ArchiveBuilder::redact(&path, &archive, |row, _| Ok(row.id == remove))?;
    let sealed = journal.seal_operational_archive(
        &pending,
        crate::historical_archive_lifecycle::seal_evidence(
            replacement.binding(),
            replacement.seal(),
        )?,
    )?;
    let prepared = journal.prepare_archive_redaction(
        &denied,
        &sealed,
        ArchiveRedactionProof {
            replacement_generation_id: sealed.registration.generation_id.clone(),
            removed_identity_sha256: "b".repeat(64),
            preserved_inventory_sha256: replacement.seal().ordered_inventory_sha256.clone(),
        },
    )?;
    Ok((
        prepared,
        journal.operational_archive_entry(&sealed.registration.generation_id)?,
    ))
}
#[test]
fn unlink_interruption_resolves_exact_successor_and_preserves_foreign_namespace(
) -> StorageResult<()> {
    let temp = tempfile::tempdir().map_err(io_error)?;
    let root = temp.path().join("journal");
    let journal = ErasureJournal::open(&root)?;
    let source = RecordId::new_v4(TableName::Source);
    let removed = row(
        "removed",
        "default",
        &source,
        "unique-erased-marker-8fdc0491",
    )?;
    let foreign = row("foreign", "other-space", &source, "foreign bytes retained")?;
    let original = committed(&journal, &[removed.clone(), foreign.clone()])?;
    let intent = journal.record_intent("household", &source)?;
    let (pending, replacement) = prepared(&journal, &original, &intent, &removed.id)?;
    assert!(journal
        .resolve_operational_archive_access(&pending)
        .is_err());
    let old = journal.archive_payload_path(&original.registration.generation_id)?;
    // Exact crash boundary: original unlink occurred, directory fsync and final
    // registry selection did not. Reopen must retain a repairable journal.
    fs::remove_file(&old).map_err(io_error)?;
    let recovered = ErasureJournal::open(&root)?;
    let redacted = recovered.finish_archive_redaction(&pending)?;
    let current = recovered.resolve_operational_archive_access(&redacted)?;
    assert_eq!(current, replacement);
    assert_eq!(recovered.operational_archive_heads()?, vec![redacted]);
    let evidence = current
        .evidence()
        .ok_or_else(|| io_error("fixture current"))?;
    let archive = VerifiedArchive::open(
        &recovered.archive_payload_path(&current.registration.generation_id)?,
        &evidence.binding,
        &evidence.seal,
    )?;
    assert!(archive.row("audit_log", &removed.id)?.is_none());
    assert_eq!(
        archive.row("audit_log", &foreign.id)?.map(|r| r.lexical),
        Some(foreign.lexical)
    );
    crate::source_lifecycle::verify_archive_erasure_access(&recovered, &archive)?;
    assert!(!fs::read(archive.path())
        .map_err(io_error)?
        .windows(b"unique-erased-marker-8fdc0491".len())
        .any(|w| w == b"unique-erased-marker-8fdc0491"));
    // Same physical source UUID in another original namespace has its own intent.
    recovered.record_intent("other-space", &source)?;
    assert!(crate::source_lifecycle::verify_archive_erasure_access(&recovered, &archive).is_err());
    ErasureJournal::open(&root)?;
    Ok(())
}
#[test]
fn startup_rejects_tampered_committed_payload_and_unprepared_missing_original() -> StorageResult<()>
{
    for missing in [false, true] {
        let temp = tempfile::tempdir().map_err(io_error)?;
        let root = temp.path().join("journal");
        let journal = ErasureJournal::open(&root)?;
        let source = RecordId::new_v4(TableName::Source);
        let original = committed(&journal, &[row("one", "default", &source, "original")?])?;
        let path = journal.archive_payload_path(&original.registration.generation_id)?;
        if missing {
            let intent = journal.record_intent("household", &source)?;
            journal.deny_operational_archive_for_intent(&original, &intent)?;
            fs::remove_file(path).map_err(io_error)?;
        } else {
            let mut bytes = fs::read(&path).map_err(io_error)?;
            let last = bytes.last_mut().ok_or_else(|| io_error("empty fixture"))?;
            *last ^= 1;
            fs::write(path, bytes).map_err(io_error)?;
        }
        assert!(ErasureJournal::open(&root).is_err());
    }
    Ok(())
}
#[test]
fn restored_pre_redaction_registry_never_authorizes_erased_original_rows() -> StorageResult<()> {
    let temp = tempfile::tempdir().map_err(io_error)?;
    let journal = ErasureJournal::open(temp.path().join("journal"))?;
    let source = RecordId::new_v4(TableName::Source);
    let original = committed(&journal, &[row("nested", "default", &source, "private")?])?;
    let selected = journal
        .operational_archive_registry_sha256()?
        .ok_or_else(|| io_error("fixture index"))?;
    journal.record_intent("household", &source)?;
    // A valid old selected registry can still exist, but original independently
    // retained intents/memberships apply before every administrative exposure.
    journal.with_operational_archive_writer(|| Ok(((), selected)))?;
    let current = journal.resolve_operational_archive_access(&original)?;
    let evidence = current
        .evidence()
        .ok_or_else(|| io_error("fixture evidence"))?;
    let archive = VerifiedArchive::open(
        &journal.archive_payload_path(&current.registration.generation_id)?,
        &evidence.binding,
        &evidence.seal,
    )?;
    assert!(crate::source_lifecycle::verify_archive_erasure_access(&journal, &archive).is_err());
    Ok(())
}

#[test]
fn explicit_transfer_discard_removes_only_registered_generations_and_resumes() -> StorageResult<()>
{
    let temp = tempfile::tempdir().map_err(io_error)?;
    let root = temp.path().join("journal");
    let journal = ErasureJournal::open(&root)?;
    let source = RecordId::new_v4(TableName::Source);
    let original = committed(
        &journal,
        &[row("one", "default", &source, "discard-target-payload")?],
    )?;
    let external = temp.path().join("operator-original.json");
    fs::write(&external, b"original input must remain").map_err(io_error)?;
    journal.begin_transfer(&"1".repeat(64))?;
    assert!(journal
        .discard_transfer_archive_payloads(&"2".repeat(64))
        .is_err());
    journal.discard_transfer_archive_payloads(&"1".repeat(64))?;
    assert!(!journal
        .archive_payload_path(&original.registration.generation_id)?
        .exists());
    // Crash before native clear/registry completion leaves exact transfer marker
    // and registry removal intent, so an administrative reopen remains possible.
    let reopened = ErasureJournal::open(&root)?;
    reopened.discard_transfer_archive_payloads(&"1".repeat(64))?;
    reopened.finish_transfer_archive_discard(&"1".repeat(64))?;
    reopened.finish_transfer()?;
    assert!(reopened.operational_archive_entries()?.is_empty());
    assert!(reopened.operational_archive_heads()?.is_empty());
    assert_eq!(
        fs::read(external).map_err(io_error)?,
        b"original input must remain"
    );
    ErasureJournal::open(&root)?;
    Ok(())
}
