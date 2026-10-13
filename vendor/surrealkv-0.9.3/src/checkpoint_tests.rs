use super::*;
use crate::{Mode, Options};
use std::time::Duration;
use tempdir::TempDir;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(10)
}

fn store(path: &Path, versions: bool) -> Result<Store> {
    let mut options = Options::new();
    options.dir = path.to_path_buf();
    options.enable_versions = versions;
    Store::new(options)
}

fn put(db: &Store, key: &[u8], value: &[u8], timestamp: u64) -> Result<()> {
    let mut tx = db.begin()?;
    tx.set_at_ts(key, value, timestamp)?;
    tx.commit()
}

fn stream(db: &mut Store) -> Result<(CheckpointSummary, Vec<Vec<u8>>)> {
    let mut records = Vec::new();
    let summary = db.visit_checkpoint_records(deadline(), |row| {
        records.push(row.to_vec());
        Ok(())
    })?;
    Ok((summary, records))
}

#[test]
fn checkpoint_preserves_versions_and_soft_tombstones_without_hard_deleted_bytes() -> Result<()> {
    for versions in [false, true] {
        let root = TempDir::new("skv-physical-copy")?;
        let mut original = store(&root.path().join("original"), versions)?;
        let erased = b"unique-erased-source-value-551af08e";
        put(&original, b"erased", erased, 1)?;
        put(
            &original,
            b"independent",
            b"first surviving historical value",
            2,
        )?;
        put(
            &original,
            b"independent",
            b"second surviving historical value",
            3,
        )?;
        put(&original, b"soft", b"retained lifecycle history", 4)?;
        {
            let mut tx = original.begin()?;
            tx.delete(b"erased")?;
            tx.soft_delete(b"soft")?;
            tx.commit()?;
        }
        let expected = stream(&mut original)?;
        let target = root.path().join("target");
        fs::create_dir(&target)?;
        assert_eq!(original.write_checkpoint(&target, deadline())?, expected.0);
        let mut copied = store(&target, versions)?;
        assert_eq!(stream(&mut copied)?, expected);
        {
            let mut tx = copied.begin_with_mode(Mode::ReadOnly)?;
            assert_eq!(tx.get(b"erased")?, None);
            assert_eq!(tx.get(b"soft")?, None);
            let history = tx.get_all_versions(b"independent")?;
            assert_eq!(history.len(), if versions { 2 } else { 1 });
        }
        copied.close()?;
        for entry in fs::read_dir(target.join("clog"))? {
            let bytes = fs::read(entry?.path())?;
            assert!(!bytes.windows(erased.len()).any(|window| window == erased));
        }
        original.close()?;
    }
    Ok(())
}

#[test]
fn checkpoint_shared_key_rewrite_preserves_only_authorized_surviving_value() -> Result<()> {
    let root = TempDir::new("skv-shared-copy")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"shared", b"source A and B", 1)?;
    put(&original, b"unrelated", b"unrelated old history", 2)?;
    put(&original, b"unrelated", b"unrelated new history", 3)?;
    {
        let mut tx = original.begin()?;
        tx.delete(b"shared")?;
        tx.commit()?;
    }
    put(&original, b"shared", b"source B only", 4)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    original.write_checkpoint(&target, deadline())?;
    let copied = store(&target, true)?;
    let mut tx = copied.begin_with_mode(Mode::ReadOnly)?;
    assert_eq!(tx.get(b"shared")?, Some(b"source B only".to_vec()));
    assert_eq!(tx.get_all_versions(b"shared")?.len(), 1);
    assert_eq!(tx.get_all_versions(b"unrelated")?.len(), 2);
    Ok(())
}

#[test]
fn empty_generation_retains_clock_after_newest_key_is_hard_deleted() -> Result<()> {
    let root = TempDir::new("skv-clock-copy")?;
    let mut original = store(&root.path().join("original"), true)?;
    for timestamp in 1..=4 {
        put(&original, b"removed", b"removed bytes", timestamp)?;
    }
    {
        let mut tx = original.begin()?;
        tx.delete(b"removed")?;
        tx.commit()?;
    }
    let expected = stream(&mut original)?.0;
    assert_eq!(expected.records, 0);
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    original.write_checkpoint(&target, deadline())?;
    let mut copied = store(&target, true)?;
    assert_eq!(stream(&mut copied)?.0, expected);
    put(&copied, b"after", b"new value", 10)?;
    assert!(copied.core.indexer.read().version() > expected.high_water);
    copied.close()?;
    drop(copied);
    let reopened = store(&target, true)?;
    assert!(reopened.core.indexer.read().version() > expected.high_water);
    Ok(())
}

#[test]
fn active_original_snapshot_refuses_copy_before_creating_any_files() -> Result<()> {
    let root = TempDir::new("skv-snapshot-copy")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"key", b"original", 1)?;
    let mut retained = original.begin_with_mode(Mode::ReadOnly)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    assert!(original.write_checkpoint(&target, deadline()).is_err());
    assert!(fs::read_dir(&target)?.next().is_none());
    assert_eq!(retained.get(b"key")?, Some(b"original".to_vec()));
    drop(retained);
    assert!(original.write_checkpoint(&target, deadline()).is_ok());
    Ok(())
}

#[test]
fn expired_or_nonempty_destination_never_overwrites_original_material() -> Result<()> {
    let root = TempDir::new("skv-copy-refusal")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"key", b"original", 1)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    assert!(original.write_checkpoint(&target, Instant::now()).is_err());
    assert!(fs::read_dir(&target)?.next().is_none());
    fs::write(target.join("retained"), b"do not overwrite")?;
    assert!(original.write_checkpoint(&target, deadline()).is_err());
    assert_eq!(fs::read(target.join("retained"))?, b"do not overwrite");
    assert_eq!(original.begin()?.get(b"key")?, Some(b"original".to_vec()));
    Ok(())
}

#[test]
fn damaged_checkpoint_clock_refuses_reopen_instead_of_reusing_versions() -> Result<()> {
    let root = TempDir::new("skv-clock-refusal")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"key", b"value", 1)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    original.write_checkpoint(&target, deadline())?;
    let mut bytes = fs::read(target.join(CLOCK_FILE))?;
    bytes[15] ^= 1;
    fs::write(target.join(CLOCK_FILE), bytes)?;
    assert!(store(&target, true).is_err());
    Ok(())
}

#[test]
fn failed_stream_observer_cannot_change_the_source_generation() -> Result<()> {
    let root = TempDir::new("skv-stream-refusal")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"key", b"value", 1)?;
    let before = stream(&mut original)?;
    assert!(original
        .visit_checkpoint_records(deadline(), |_| Err(Error::Abort))
        .is_err());
    assert_eq!(stream(&mut original)?, before);
    Ok(())
}

#[test]
fn selected_version_copy_preserves_independent_history_and_removes_historical_only_source(
) -> Result<()> {
    let root = TempDir::new("skv-selective-history")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"shared", b"B-only-original", 1)?;
    put(&original, b"shared", b"A+B-mixed-history", 2)?;
    put(&original, b"shared", b"B-only-current", 3)?;
    put(&original, b"historical-only", b"A-only-now-hidden", 4)?;
    {
        let mut tx = original.begin()?;
        tx.soft_delete(b"historical-only")?;
        tx.commit()?;
    }
    // Literal independent fixture authority; production obtains its decision
    // from accepted source/support state, never from these test sentinels.
    let retain = |record: &super::CheckpointRecord<'_>| -> Result<CheckpointDecision> {
        Ok(if record.key == b"historical-only" {
            CheckpointDecision::Drop
        } else if record.key == b"shared" && record.timestamp == 2 {
            CheckpointDecision::Tombstone
        } else {
            CheckpointDecision::Keep
        })
    };
    let before = stream(&mut original)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    original.write_selected_checkpoint(&target, deadline(), retain)?;
    assert_eq!(stream(&mut original)?, before);
    let copied = store(&target, true)?;
    let mut tx = copied.begin_with_mode(Mode::ReadOnly)?;
    assert_eq!(
        tx.get_all_versions(b"shared")?,
        vec![
            (b"B-only-original".to_vec(), 1),
            (b"B-only-current".to_vec(), 3)
        ]
    );
    assert_eq!(
        tx.get_at_version(b"shared", 1)?,
        Some(b"B-only-original".to_vec())
    );
    assert_eq!(tx.get_at_version(b"shared", 2)?, None);
    assert!(tx.get_all_versions(b"historical-only")?.is_empty());
    assert_eq!(tx.get(b"shared")?, Some(b"B-only-current".to_vec()));
    Ok(())
}

#[test]
fn unknown_version_authority_leaves_original_history_unchanged() -> Result<()> {
    let root = TempDir::new("skv-selective-refusal")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"shared", b"B-only-original", 1)?;
    put(&original, b"shared", b"A+B-mixed-history", 2)?;
    let before = stream(&mut original)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    assert!(original
        .write_selected_checkpoint(&target, deadline(), |record| {
            if record.timestamp == 2 {
                Err(Error::Abort)
            } else {
                Ok(CheckpointDecision::Keep)
            }
        })
        .is_err());
    assert_eq!(stream(&mut original)?, before);
    Ok(())
}

#[test]
fn raw_inventory_marks_exact_current_version_and_retains_hidden_history() -> Result<()> {
    let root = TempDir::new("skv-history-inventory")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"shared", b"independent old", 10)?;
    put(&original, b"shared", b"independent current", 20)?;
    put(&original, b"hidden", b"historical only", 30)?;
    {
        let mut tx = original.begin()?;
        tx.soft_delete(b"hidden")?;
        tx.commit()?;
    }
    let before = stream(&mut original)?;
    let mut observations = Vec::new();
    original.visit_checkpoint_versions(deadline(), |row| {
        observations.push((
            row.key.to_vec(),
            row.value.to_vec(),
            row.timestamp,
            row.current,
            row.tombstone,
        ));
        Ok(())
    })?;
    assert_eq!(observations.len(), 4);
    assert!(observations
        .iter()
        .any(|row| row.0 == b"shared" && row.2 == 10 && !row.3 && !row.4));
    assert!(observations
        .iter()
        .any(|row| row.0 == b"shared" && row.2 == 20 && row.3 && !row.4));
    assert!(observations
        .iter()
        .any(|row| row.0 == b"hidden" && row.2 == 30 && !row.3 && !row.4));
    assert!(observations
        .iter()
        .any(|row| row.0 == b"hidden" && row.1.is_empty() && row.3 && row.4));
    assert_eq!(stream(&mut original)?, before);
    Ok(())
}

#[test]
fn erased_historical_payload_is_absent_from_log_and_gap_survives_reopen() -> Result<()> {
    let root = TempDir::new("skv-history-gap")?;
    let mut original = store(&root.path().join("original"), true)?;
    let erased = b"erased-contribution-18491f3d0ebf";
    put(&original, b"shared", b"B only before", 10)?;
    put(&original, b"shared", erased, 20)?;
    put(&original, b"shared", b"B only after", 30)?;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    let summary = original.write_selected_checkpoint(&target, deadline(), |row| {
        Ok(if row.timestamp == 20 {
            CheckpointDecision::Tombstone
        } else {
            CheckpointDecision::Keep
        })
    })?;
    assert_eq!(summary.records, 3);
    for entry in fs::read_dir(target.join("clog"))? {
        let bytes = fs::read(entry?.path())?;
        assert!(!bytes.windows(erased.len()).any(|window| window == erased));
    }
    for _ in 0..2 {
        let copied = store(&target, true)?;
        {
            let tx = copied.begin_with_mode(Mode::ReadOnly)?;
            assert_eq!(
                tx.get_at_version(b"shared", 10)?,
                Some(b"B only before".to_vec())
            );
            assert_eq!(tx.get_at_version(b"shared", 20)?, None);
            assert_eq!(tx.get_at_version(b"shared", 29)?, None);
            assert_eq!(
                tx.get_at_version(b"shared", 30)?,
                Some(b"B only after".to_vec())
            );
        }
        copied.close()?;
    }
    Ok(())
}

#[test]
fn selecting_no_surviving_versions_preserves_original_clock() -> Result<()> {
    let root = TempDir::new("skv-history-empty")?;
    let mut original = store(&root.path().join("original"), true)?;
    put(&original, b"erased", b"erased old", 10)?;
    put(&original, b"erased", b"erased new", 20)?;
    let before = stream(&mut original)?.0;
    let target = root.path().join("target");
    fs::create_dir(&target)?;
    let copied_summary = original
        .write_selected_checkpoint(&target, deadline(), |_| Ok(CheckpointDecision::Drop))?;
    assert_eq!(copied_summary.records, 0);
    assert_eq!(copied_summary.high_water, before.high_water);
    let mut copied = store(&target, true)?;
    assert_eq!(stream(&mut copied)?.0, copied_summary);
    put(&copied, b"next", b"independent", 30)?;
    assert!(stream(&mut copied)?.0.high_water > before.high_water);
    assert_eq!(stream(&mut original)?.0, before);
    Ok(())
}
