//! Bounded source-side accounting while exact archive rows are streamed once.
//! This proof verifies source coverage; only the journal lifecycle can authorize
//! a sealed archive as managed storage or ready publication.

use super::source_spool::{RawRow, SourceSpool};
use anyhow::{ensure, Context};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

/// Immutable producer declaration; runtime recomputes this from original rows.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ArchiveSourceInventory {
    pub format: String,
    pub disposition_sha256: String,
    pub tables: BTreeMap<String, u64>,
    pub rows: u64,
    pub lexical_bytes: u64,
    pub ordered_inventory_sha256: String,
    pub runtime_verified: bool,
}
impl ArchiveSourceInventory {
    pub fn validate(&self, spool: &SourceSpool) -> anyhow::Result<()> {
        ensure!(self.format == "recall-operational-archive-source-inventory-v1"
            && !self.runtime_verified
            && self.disposition_sha256 == elephant_storage::historical_operational_archive::ArchiveBinding::disposition_digest()
            && self.tables == spool.archive_tables(), "archive source declaration differs");
        let rows = self
            .tables
            .values()
            .try_fold(0_u64, |total, n| total.checked_add(*n))
            .context("archive count overflow")?;
        ensure!(
            self.rows == rows
                && self.ordered_inventory_sha256.len() == 64
                && self
                    .ordered_inventory_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "archive source population or digest malformed"
        );
        Ok(())
    }
    pub fn verify(&self, totals: &ArchiveCopyTotals) -> anyhow::Result<()> {
        ensure!(
            self.tables == totals.tables
                && self.lexical_bytes == totals.lexical_bytes
                && self.ordered_inventory_sha256 == totals.ordered_inventory_sha256,
            "original lexical archive inventory differs from immutable plan"
        );
        Ok(())
    }
}

pub(super) struct ArchiveCopyCensus {
    expected: BTreeMap<String, u64>,
    counts: BTreeMap<String, u64>,
    previous: BTreeMap<String, String>,
    lexical_bytes: u64,
    hash: Sha256,
    table_index: usize,
}

impl ArchiveCopyCensus {
    pub fn new(spool: &SourceSpool) -> Self {
        let expected = spool.archive_tables();
        Self {
            counts: expected.keys().map(|table| (table.clone(), 0)).collect(),
            expected,
            previous: BTreeMap::new(),
            lexical_bytes: 0,
            hash: Sha256::new_with_prefix(b"recall-operational-archive-rows-v1\0"),
            table_index: 0,
        }
    }

    /// Archive inventory digest order is table then original identity, matching
    /// the independent sealed artifact verifier, including declared empty tables.
    pub fn begin_table(&mut self, table: &str) -> anyhow::Result<()> {
        let (expected, count) = self
            .expected
            .iter()
            .nth(self.table_index)
            .context("extra archive table")?;
        ensure!(expected == table, "archive table order differs");
        if self.table_index > 0 {
            let prior = self
                .expected
                .keys()
                .nth(self.table_index - 1)
                .context("prior archive table")?;
            ensure!(
                self.counts[prior] == self.expected[prior],
                "previous archive table incomplete"
            );
        }
        self.hash.update(serde_json::to_vec(&(table, *count))?);
        self.table_index += 1;
        Ok(())
    }

    /// Accept a whole page only after its private archive write succeeded.
    /// A failed write never advances the census and cannot be called complete.
    pub fn accepted_page(&mut self, table: &str, rows: &[RawRow]) -> anyhow::Result<()> {
        ensure!(
            self.table_index > 0
                && self
                    .expected
                    .keys()
                    .nth(self.table_index - 1)
                    .map(String::as_str)
                    == Some(table),
            "archive page is outside current table"
        );
        let expected = *self
            .expected
            .get(table)
            .context("undeclared archive table")?;
        let current = *self
            .counts
            .get(table)
            .context("archive census table missing")?;
        let next = current
            .checked_add(u64::try_from(rows.len())?)
            .context("archive row count overflow")?;
        ensure!(
            next <= expected,
            "archive row count exceeds source declaration"
        );
        let mut previous = self.previous.get(table).map(String::as_str);
        let mut bytes = self.lexical_bytes;
        for row in rows {
            ensure!(
                previous.is_none_or(|p| p < row.id.as_str()),
                "archive source identity duplicate or out of order"
            );
            ensure!(
                row.id
                    .split_once(':')
                    .is_some_and(|(prefix, id)| prefix == table && !id.is_empty()),
                "archive source table/identity mismatch"
            );
            bytes = bytes
                .checked_add(u64::try_from(row.lexical.len())?)
                .context("archive source byte count overflow")?;
            previous = Some(&row.id);
        }
        let mut next_hash = self.hash.clone();
        for row in rows {
            next_hash.update(serde_json::to_vec(&(
                table,
                &row.id,
                hex::encode(Sha256::digest(row.lexical.as_bytes())),
                row.lexical.len(),
            ))?);
        }
        self.hash = next_hash;
        if let Some(last) = rows.last() {
            self.previous.insert(table.to_owned(), last.id.clone());
        }
        self.counts.insert(table.to_owned(), next);
        self.lexical_bytes = bytes;
        Ok(())
    }

    /// Exact source declarations must equal the independently sealed archive
    /// populations; a file hash by itself does not establish complete coverage.
    pub fn finish(
        self,
        sealed_counts: &BTreeMap<String, u64>,
        sealed_digest: &str,
    ) -> anyhow::Result<ArchiveCopyTotals> {
        ensure!(
            self.counts == self.expected
                && *sealed_counts == self.expected
                && self.table_index == self.expected.len(),
            "archive sealed population differs from complete source cut"
        );
        let ordered_inventory_sha256 = hex::encode(self.hash.finalize());
        ensure!(
            ordered_inventory_sha256 == sealed_digest,
            "archive sealed lexical inventory differs from original source rows"
        );
        Ok(ArchiveCopyTotals {
            ordered_inventory_sha256,
            tables: self.counts,
            lexical_bytes: self.lexical_bytes,
            table_phases: BTreeMap::new(),
            seal_seconds: 0.0,
        })
    }
}

pub(super) struct ArchiveCopyTotals {
    pub ordered_inventory_sha256: String,
    pub tables: BTreeMap<String, u64>,
    pub lexical_bytes: u64,
    pub table_phases: BTreeMap<String, ArchiveTablePhase>,
    pub seal_seconds: f64,
}

#[derive(Default, serde::Serialize)]
pub(super) struct ArchiveTablePhase {
    rows: u64,
    original_lexical_bytes: u64,
    pages: u64,
    source_page_seconds: f64,
    archive_write_seconds: f64,
    independent_census_seconds: f64,
    seconds: f64,
}

/// Identical source lexical digest framing for both storage dispositions. A row
/// leaving the live native path must still enter this original source digest.
pub(super) fn hash_source_row(hash: &mut Sha256, table: &str, row: &RawRow) -> anyhow::Result<()> {
    hash.update(serde_json::to_vec(&serde_json::json!([
        table,
        row.id,
        hex::encode(Sha256::digest(row.lexical.as_bytes()))
    ]))?);
    hash.update(b"\n");
    Ok(())
}

/// Reserve original journal custody before creating any archived payload.
pub(super) fn prepare_initial(
    spool: &SourceSpool,
    dst: &elephant_storage::StorageClient,
) -> anyhow::Result<
    Option<(
        elephant_storage::historical_archive_lifecycle::SealedOperationalArchive,
        ArchiveCopyTotals,
    )>,
> {
    use elephant_storage::historical_archive_lifecycle::reserve_initial_archive;
    use elephant_storage::historical_operational_archive::ArchiveBinding;
    if spool.operational_disposition == super::archive_disposition::OperationalDisposition::Native {
        return Ok(None);
    }
    let binding = ArchiveBinding {
        version: 1,
        snapshot_sha256: spool.source_sha256.clone(),
        spool_sha256: spool.inventory_sha256.clone(),
        plan_id: spool.plan_id.clone(),
        source_space_id: spool.space_id.clone(),
        target_space_id: spool.target_space_id.clone(),
        journal_identity: dst.erasure_journal()?.identity().to_owned(),
        disposition_sha256: ArchiveBinding::disposition_digest(),
        parent_generation_sha256: None,
        original_table_declarations: spool.tables.clone(),
    };
    let pending = reserve_initial_archive(dst, &binding)?;
    let (verified, totals) = materialize(spool, pending.path(), binding)?;
    let sealed = pending.seal(dst, verified)?;
    Ok(Some((sealed, totals)))
}

/// Materialize exact original rows only at a path already reserved by the
/// journal lifecycle. The returned evidence is still pending publication.
pub(super) fn materialize(
    spool: &SourceSpool,
    reserved_path: &std::path::Path,
    binding: elephant_storage::historical_operational_archive::ArchiveBinding,
) -> anyhow::Result<(
    elephant_storage::historical_operational_archive::VerifiedArchive,
    ArchiveCopyTotals,
)> {
    use elephant_storage::historical_operational_archive::{ArchiveBuilder, ArchiveRow};
    ensure!(
        spool.operational_disposition
            == super::archive_disposition::OperationalDisposition::ManagedArchiveV1,
        "archive materialization requires explicit disposition"
    );
    binding.validate()?;
    ensure!(
        binding.snapshot_sha256 == spool.source_sha256
            && binding.spool_sha256 == spool.inventory_sha256
            && binding.plan_id == spool.plan_id
            && binding.source_space_id == spool.space_id
            && binding.target_space_id == spool.target_space_id
            && binding.original_table_declarations == spool.tables
            && binding.parent_generation_sha256.is_none(),
        "initial archive binding differs from original source cut"
    );
    spool.verify_unchanged()?;
    let mut writer = ArchiveBuilder::create(reserved_path, binding)?;
    let mut census = ArchiveCopyCensus::new(spool);
    let mut table_phases = BTreeMap::new();
    for table in spool.archive_tables().keys() {
        census.begin_table(table)?;
        let table_started = std::time::Instant::now();
        let mut metrics = ArchiveTablePhase::default();
        let mut after = None;
        loop {
            let started = std::time::Instant::now();
            let page = spool.page(table, after.as_deref())?;
            metrics.source_page_seconds += started.elapsed().as_secs_f64();
            if page.is_empty() {
                break;
            }
            let rows = page
                .iter()
                .map(|row| ArchiveRow {
                    table: table.clone(),
                    id: row.id.clone(),
                    lexical: row.lexical.as_bytes().to_vec(),
                    lexical_sha256: hex::encode(Sha256::digest(row.lexical.as_bytes())),
                })
                .collect::<Vec<_>>();
            let started = std::time::Instant::now();
            writer.append(&rows)?;
            metrics.archive_write_seconds += started.elapsed().as_secs_f64();
            let started = std::time::Instant::now();
            census.accepted_page(table, &page)?;
            metrics.independent_census_seconds += started.elapsed().as_secs_f64();
            metrics.pages += 1;
            metrics.rows += page.len() as u64;
            metrics.original_lexical_bytes +=
                page.iter().map(|row| row.lexical.len() as u64).sum::<u64>();
            after = page.last().map(|row| row.id.clone());
        }
        metrics.seconds = table_started.elapsed().as_secs_f64();
        table_phases.insert(table.clone(), metrics);
    }
    let seal_started = std::time::Instant::now();
    let verified = writer.seal()?;
    let seal_seconds = seal_started.elapsed().as_secs_f64();
    let mut totals = census.finish(
        &verified.seal().table_counts,
        &verified.seal().ordered_inventory_sha256,
    )?;
    totals.table_phases = table_phases;
    totals.seal_seconds = seal_seconds;
    spool
        .archive_inventory
        .as_ref()
        .context("archive inventory missing")?
        .verify(&totals)?;
    spool.verify_unchanged()?;
    Ok((verified, totals))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn census() -> ArchiveCopyCensus {
        ArchiveCopyCensus {
            expected: BTreeMap::from([("audit_log".into(), 2), ("work_edge".into(), 0)]),
            counts: BTreeMap::from([("audit_log".into(), 0), ("work_edge".into(), 0)]),
            previous: BTreeMap::new(),
            lexical_bytes: 0,
            hash: Sha256::new_with_prefix(b"recall-operational-archive-rows-v1\0"),
            table_index: 0,
        }
    }
    fn row(key: &str) -> RawRow {
        RawRow {
            id: format!("audit_log:{key}"),
            lexical: format!("{{\"id\":\"audit_log:{key}\"}}"),
            original_sha256: "a".repeat(64),
        }
    }
    fn complete() -> anyhow::Result<ArchiveCopyCensus> {
        let mut c = census();
        c.begin_table("audit_log")?;
        c.accepted_page("audit_log", &[row("a"), row("b")])?;
        c.begin_table("work_edge")?;
        Ok(c)
    }
    #[test]
    fn complete_census_requires_all_rows_and_exact_independent_sealed_digest() -> anyhow::Result<()>
    {
        let expected = census().expected;
        let digest = hex::encode(complete()?.hash.finalize());
        let mut c = census();
        c.begin_table("audit_log")?;
        c.accepted_page("audit_log", &[row("a")])?;
        assert!(c.begin_table("work_edge").is_err());
        assert!(c.finish(&expected, &digest).is_err());
        let total = complete()?.finish(&expected, &digest)?;
        assert_eq!(total.tables, expected);
        assert!(total.lexical_bytes > 0);
        assert!(complete()?
            .finish(&BTreeMap::from([("audit_log".into(), 2)]), &digest)
            .is_err());
        assert!(complete()?.finish(&expected, &"0".repeat(64)).is_err());
        let mut c = census();
        c.begin_table("audit_log")?;
        let mut changed = row("b");
        changed.lexical.push(' ');
        c.accepted_page("audit_log", &[row("a"), changed])?;
        c.begin_table("work_edge")?;
        assert!(c.finish(&expected, &digest).is_err());
        Ok(())
    }
    #[test]
    fn duplicate_cross_page_and_wrong_table_are_refused_without_census_advance(
    ) -> anyhow::Result<()> {
        let mut c = census();
        assert!(c.begin_table("work_edge").is_err());
        c.begin_table("audit_log")?;
        c.accepted_page("audit_log", &[row("a")])?;
        assert!(c.accepted_page("audit_log", &[row("a")]).is_err());
        assert_eq!(c.counts["audit_log"], 1);
        assert!(c.accepted_page("work_edge", &[row("b")]).is_err());
        assert!(c.accepted_page("doc", &[]).is_err());
        assert!(c.accepted_page("audit_log", &[row("b"), row("c")]).is_err());
        assert_eq!(c.counts["audit_log"], 1);
        c.accepted_page("audit_log", &[row("b")])?;
        c.begin_table("work_edge")?;
        c.finish(
            &census().expected,
            &hex::encode(complete()?.hash.finalize()),
        )?;
        Ok(())
    }
}
