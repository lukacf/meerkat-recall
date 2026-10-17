use super::*;
use crate::erasure_journal::{ArchiveRedactionProof, ArchiveRegistration, ErasureJournal};
use crate::historical_operational_archive::{ArchiveBinding, ArchiveBuilder};
use elephant_types::records::{AuditAction, AuditLog};
use sha2::{Digest, Sha256};

fn row(name: &str, space: &str, dependency: &RecordId, marker: &str) -> StorageResult<ArchiveRow> {
    let value = AuditLog {
        base: BaseFields::new(
            RecordId::new(
                TableName::AuditLog,
                Uuid::new_v5(&Uuid::NAMESPACE_OID, name.as_bytes()),
            ),
            space.into(),
            "archive-closure-fixture".into(),
        ),
        action: AuditAction::Update,
        actor: "fixture".into(),
        record: RecordId::new_v4(TableName::Entity),
        before: None,
        after: Some(
            serde_json::json!({"private":{"dependency":dependency.to_string()},"marker":marker}),
        ),
        timestamp: Utc::now(),
        reason: None,
    };
    let lexical = serde_json::to_vec_pretty(&value).map_err(|e| integrity(&e.to_string()))?;
    Ok(ArchiveRow {
        table: "audit_log".into(),
        id: value.base.id.to_string(),
        lexical_sha256: hex::encode(Sha256::digest(&lexical)),
        lexical,
    })
}
fn register(journal: &ErasureJournal, rows: &[ArchiveRow]) -> StorageResult<RegisteredGeneration> {
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
        original_table_declarations: BTreeMap::from([("audit_log".into(), rows.len() as u64)]),
    };
    let generation = Uuid::new_v4().to_string();
    let reg = ArchiveRegistration {
        lineage_id: Uuid::new_v4().to_string(),
        generation_id: generation.clone(),
        parent_generation_id: None,
        binding_sha256: binding.digest()?,
        snapshot_sha256: binding.snapshot_sha256.clone(),
        journal_id: journal.identity().into(),
        source_space_id: binding.source_space_id.clone(),
        target_space_id: binding.target_space_id.clone(),
    };
    let path = journal.reserve_operational_archive(reg)?;
    let mut writer = ArchiveBuilder::create(&path, binding)?;
    writer.append(rows)?;
    let archive = writer.seal()?;
    let entry = journal.seal_operational_archive(
        &journal.operational_archive_entry(&generation)?,
        crate::historical_archive_lifecycle::seal_evidence(archive.binding(), archive.seal())?,
    )?;
    let entry = journal.commit_operational_archive(&entry, &"a".repeat(64))?;
    Ok(RegisteredGeneration { entry, archive })
}
fn redact(
    journal: &ErasureJournal,
    original: &RegisteredGeneration,
    intent: &ErasureIntent,
    remove: &str,
) -> StorageResult<RegisteredGeneration> {
    let denied = journal.deny_operational_archive_for_intent(&original.entry, intent)?;
    let mut binding = original.archive.binding().clone();
    binding.parent_generation_sha256 = Some(original.archive.seal().file_sha256.clone());
    let (pending, path) = journal.reserve_archive_redaction(&denied, intent, binding)?;
    let selected = match original.archive.row("audit_log", remove)? {
        Some(row) => BTreeMap::from([((row.table, row.id), row.lexical_sha256)]),
        None => BTreeMap::new(),
    };
    let (removed_identity_sha256, preserved_inventory_sha256) =
        survivor_proof(&original.archive, &selected, intent)?;
    let archive = ArchiveBuilder::redact(&path, &original.archive, |row, _| Ok(row.id == remove))?;
    assert_eq!(
        archive.seal().ordered_inventory_sha256,
        preserved_inventory_sha256
    );
    let sealed = journal.seal_operational_archive(
        &pending,
        crate::historical_archive_lifecycle::seal_evidence(archive.binding(), archive.seal())?,
    )?;
    let prepared = journal.prepare_archive_redaction(
        &denied,
        &sealed,
        ArchiveRedactionProof {
            replacement_generation_id: sealed.registration.generation_id.clone(),
            removed_identity_sha256,
            preserved_inventory_sha256,
        },
    )?;
    journal.finish_archive_redaction(&prepared)?;
    Ok(RegisteredGeneration {
        entry: journal.operational_archive_entry(&sealed.registration.generation_id)?,
        archive,
    })
}
#[test]
fn archive_native_archive_fixed_point_uses_nested_original_references_and_space(
) -> StorageResult<()> {
    let dir = tempfile::tempdir().map_err(|e| integrity(&e.to_string()))?;
    let journal = ErasureJournal::open(dir.path().join("journal"))?;
    let source = RecordId::new_v4(TableName::Source);
    let first = row("first", "default", &source, "archive selected from source")?;
    let first_id = RecordId::from_str(&first.id).map_err(|e| integrity(&e.to_string()))?;
    let native_id = RecordId::new_v4(TableName::Doc);
    let second = row(
        "second",
        "default",
        &native_id,
        "archive selected from native",
    )?;
    let foreign = row(
        "foreign",
        "unrelated",
        &source,
        "foreign namespace retained",
    )?;
    let generation = register(&journal, &[first, second, foreign.clone()])?;
    let mut plan = LifecyclePlan::default();
    plan.removed.insert(source.id());
    let archives = vec![generation];
    assert!(extend_selection(
        &archives,
        &journal,
        "household",
        &BTreeSet::new(),
        &mut plan
    )?);
    assert!(plan.archive_deletes.identities.contains(&first_id));
    // Native dependency exists only through the now selected archive identity.
    let native = InventoryRow {
        table: "doc".into(),
        key: native_id.to_string(),
        record_id: Some(native_id.clone()),
        native_id: Some(crate::surreal::thing_from_record_id(&native_id)),
        native_value: None,
        value: serde_json::json!({"id":native_id,"space_id":"household","provenance":{"work_run_id":first_id}}),
    };
    assert!(native.references_erased_identity(&journal, "household", &plan, &BTreeSet::new())?);
    plan.removed.insert(native_id.id());
    assert!(extend_selection(
        &archives,
        &journal,
        "household",
        &BTreeSet::new(),
        &mut plan
    )?);
    assert_eq!(
        plan.archive_deletes
            .rows
            .values()
            .map(BTreeMap::len)
            .sum::<usize>(),
        2
    );
    assert!(!extend_selection(
        &archives,
        &journal,
        "household",
        &BTreeSet::new(),
        &mut plan
    )?);
    assert!(!plan
        .archive_deletes
        .identities
        .contains(&RecordId::from_str(&foreign.id).map_err(|e| integrity(&e.to_string()))?));
    Ok(())
}
#[test]
fn two_erasure_intents_resolve_once_and_replay_ancestry_remains_stable() -> StorageResult<()> {
    let dir = tempfile::tempdir().map_err(|e| integrity(&e.to_string()))?;
    let root = dir.path().join("journal");
    let journal = ErasureJournal::open(&root)?;
    let a = RecordId::new_v4(TableName::Source);
    let b = RecordId::new_v4(TableName::Source);
    let row_a = row("a", "default", &a, "ERASE-A-MARKER")?;
    let row_b = row("b", "default", &b, "ERASE-B-MARKER")?;
    let keep = row(
        "keep",
        "default",
        &RecordId::new_v4(TableName::Source),
        "surviving-original",
    )?;
    let original = register(&journal, &[row_a.clone(), row_b.clone(), keep.clone()])?;
    let intent_a = journal.record_intent("household", &a)?;
    let after_a = redact(&journal, &original, &intent_a, &row_a.id)?;
    let intent_b = journal.record_intent("household", &b)?;
    let after_b = redact(&journal, &after_a, &intent_b, &row_b.id)?;
    let before = journal.operational_archive_entries()?;
    let before_bytes =
        std::fs::read(after_b.archive.path()).map_err(|e| integrity(&e.to_string()))?;
    for _ in 0..3 {
        let reopened = ErasureJournal::open(&root)?;
        let entries = reopened.operational_archive_entries()?;
        assert!(applied_in_ancestry(&entries, &after_b.entry, &intent_a)?);
        assert!(applied_in_ancestry(&entries, &after_b.entry, &intent_b)?);
        let head = reopened
            .operational_archive_heads()?
            .pop()
            .ok_or_else(|| integrity("fixture head"))?;
        assert_eq!(
            reopened.resolve_operational_archive_access(&head)?,
            after_b.entry
        );
        verify_erasure_access(&reopened, &after_b.archive)?;
        assert_eq!(entries, before);
        assert_eq!(
            std::fs::read(after_b.archive.path()).map_err(|e| integrity(&e.to_string()))?,
            before_bytes
        );
    }
    assert_eq!(
        after_b
            .archive
            .row("audit_log", &keep.id)?
            .map(|r| r.lexical),
        Some(keep.lexical)
    );
    for marker in [b"ERASE-A-MARKER", b"ERASE-B-MARKER"] {
        assert!(!before_bytes.windows(marker.len()).any(|w| w == marker));
    }
    Ok(())
}
