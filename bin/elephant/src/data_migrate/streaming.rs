//! Bounded source-spool transfer and disk-backed exact reconciliation.

use super::source_spool::{self, SourceSpool, TypedRow};
use anyhow::{ensure, Context};
use elephant_storage::canonical_extension::{
    NativeArray, NativeExtensionValue as Native, NativeQueryValue, NativeRecordIdentity as Thing,
    NativeRecordKey as Id, NativeRecordKeyRange as IdRange,
};
use elephant_storage::transfer_batch::{import_batch, import_embedding_batch, TransferRecord};
use elephant_storage::{Repository, SpaceConfigRepository, StorageClient};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::Path;

pub(super) struct Fingerprint {
    pub(super) cursor: Native,
    pub(super) id: String,
    pub(super) sha256: String,
}

pub(super) async fn fingerprints(
    dst: &StorageClient,
    table: &str,
    cursor: Option<Native>,
) -> anyhow::Result<Vec<Fingerprint>> {
    use std::ops::Bound;
    let lower = match &cursor {
        None => Bound::Unbounded,
        Some(Native::Thing(previous)) => {
            ensure!(previous.tb == table, "fingerprint cursor has wrong table");
            ensure!(
                !matches!(previous.id, Id::Range(_) | Id::Generate(_)),
                "fingerprint cursor is not a stored record identity"
            );
            Bound::Excluded(previous.id.clone())
        }
        Some(_) => anyhow::bail!("fingerprint cursor is not a native record"),
    };
    // Scalar native IDs have the same physical and semantic order. Complex
    // IDs do not: array prefixes and nested numeric variants are counterexamples.
    // A complex cursor always retains the original semantic ORDER query.
    let scalar_cursor = cursor.as_ref().is_none_or(
        |value| matches!(value, Native::Thing(thing) if scalar_fingerprint_id(&thing.id)),
    );
    let range = Native::Thing(Thing::from((
        table.to_owned(),
        Id::Range(Box::new(IdRange {
            beg: lower,
            end: Bound::Unbounded,
        })),
    )));
    let mut ordered_fallback = !scalar_cursor;
    let mut values = if scalar_cursor {
        let mut response = dst.query_bind_checked(
            "SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM $range LIMIT 256",
            BTreeMap::from([("range".to_owned(), range)]),
        ).await?;
        let raw: NativeQueryValue = response.take(0)?;
        let Native::Array(values) = raw.into_inner() else {
            anyhow::bail!("fingerprint page is not a native array");
        };
        values
    } else {
        ordered_fingerprint_page(dst, table, cursor.clone()).await?
    };
    // Do not emit even the scalar prefix of a page that crosses into complex
    // IDs. Requery the entire page from the original cursor in the old order.
    if scalar_cursor
        && values.0.iter().any(|value| {
            matches!(value, Native::Object(fields)
            if matches!(fields.get("cursor"), Some(Native::Thing(thing))
                if !scalar_fingerprint_id(&thing.id)))
        })
    {
        ordered_fallback = true;
        values = ordered_fingerprint_page(dst, table, cursor.clone()).await?;
    }
    let mut rows = Vec::with_capacity(values.len());
    let mut previous = cursor;
    for value in values.0 {
        let Native::Object(mut fields) = value else {
            anyhow::bail!("fingerprint row is not a native object");
        };
        let cursor = fields
            .remove("cursor")
            .context("native fingerprint cursor missing")?;
        let Native::Thing(current) = &cursor else {
            anyhow::bail!("fingerprint cursor is not a native record");
        };
        ensure!(current.tb == table, "fingerprint result has wrong table");
        if !ordered_fallback {
            if let Some(Native::Thing(prior)) = &previous {
                ensure!(
                    current > prior,
                    "fingerprint cursor did not strictly advance"
                );
            }
        }
        previous = Some(cursor.clone());
        let Some(Native::Strand(id)) = fields.remove("id") else {
            anyhow::bail!("fingerprint id is not a string");
        };
        let Some(Native::Strand(sha256)) = fields.remove("sha256") else {
            anyhow::bail!("fingerprint digest is not a string");
        };
        ensure!(fields.is_empty(), "unexpected fingerprint fields");
        rows.push(Fingerprint {
            cursor,
            id: id.0,
            sha256: sha256.0,
        });
    }
    ensure!(rows.len() <= 256, "fingerprint page exceeds bound");
    Ok(rows)
}

const fn scalar_fingerprint_id(id: &Id) -> bool {
    matches!(id, Id::Number(_) | Id::String(_) | Id::Uuid(_))
}

async fn ordered_fingerprint_page(
    dst: &StorageClient,
    table: &str,
    cursor: Option<Native>,
) -> anyhow::Result<NativeArray> {
    let predicate = if cursor.is_some() {
        " WHERE id > $after"
    } else {
        ""
    };
    let sql = format!("SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM type::table($table){predicate} ORDER BY cursor LIMIT 256");
    let mut response = dst
        .query_bind_checked(
            &sql,
            BTreeMap::from([
                ("table".to_owned(), Native::from(table.to_owned())),
                ("after".to_owned(), cursor.unwrap_or(Native::None)),
            ]),
        )
        .await?;
    let raw: NativeQueryValue = response.take(0)?;
    let Native::Array(values) = raw.into_inner() else {
        anyhow::bail!("fingerprint page is not a native array");
    };
    Ok(values)
}

async fn capture(dst: &StorageClient, db: &mut Connection, table: &str) -> anyhow::Result<u64> {
    db.execute("DELETE FROM expected WHERE t=?", [table])?;
    let mut cursor = None;
    let mut count = 0_u64;
    loop {
        let page = fingerprints(dst, table, cursor.clone()).await?;
        if page.is_empty() {
            break;
        }
        let tx = db.transaction()?;
        for row in &page {
            tx.execute(
                "INSERT INTO expected VALUES (?,?,?,?)",
                params![table, row.id, row.sha256, count],
            )?;
            count += 1;
        }
        tx.commit()?;
        let next = page.last().context("nonempty page")?.cursor.clone();
        ensure!(
            cursor.as_ref() != Some(&next),
            "fingerprint cursor did not advance"
        );
        cursor = Some(next);
    }
    Ok(count)
}

#[expect(
    clippy::needless_pass_by_ref_mut,
    reason = "The transfer future must remain Send across native page awaits: an exclusive borrow requires Connection: Send, while a shared borrow would require unsupported Connection: Sync."
)]
pub(super) async fn reconcile(
    dst: &StorageClient,
    db: &mut Connection,
    tables: &BTreeSet<String>,
) -> anyhow::Result<String> {
    ensure!(
        dst.list_tables()
            .await?
            .into_iter()
            .collect::<BTreeSet<_>>()
            == *tables,
        "destination table inventory changed"
    );
    let mut digest = Sha256::new();
    for table in tables {
        digest.update(serde_json::to_vec(&serde_json::json!(["table", table]))?);
        digest.update(b"\n");
        let mut cursor = None;
        let mut count = 0_u64;
        loop {
            let page = fingerprints(dst, table, cursor.clone()).await?;
            if page.is_empty() {
                break;
            }
            {
                // Return the statement to the connection cache before the next
                // async native page read; SQLite statements are not Send.
                let mut expected_row =
                    db.prepare_cached("SELECT k,h FROM expected WHERE t=? AND k=?")?;
                for row in &page {
                    let expected: (String, String) = expected_row
                        .query_row(params![table, row.id], |r| Ok((r.get(0)?, r.get(1)?)))?;
                    ensure!(
                        expected == (row.id.clone(), row.sha256.clone()),
                        "destination row changed in {table}"
                    );
                    digest.update(serde_json::to_vec(&serde_json::json!([
                        table, row.id, row.sha256
                    ]))?);
                    digest.update(b"\n");
                    count += 1;
                }
            }
            let next = page.last().context("nonempty page")?.cursor.clone();
            ensure!(
                cursor.as_ref() != Some(&next),
                "fingerprint cursor did not advance"
            );
            cursor = Some(next);
        }
        let expected: u64 =
            db.query_row("SELECT count(*) FROM expected WHERE t=?", [table], |r| {
                r.get(0)
            })?;
        ensure!(expected == count, "missing destination row in {table}");
    }
    Ok(hex::encode(digest.finalize()))
}

/// Apply only exact transaction-reported native row changes. Existing hashes
/// must agree with the already verified copy inventory; absence is explicit.
pub(super) fn apply_effects<'a>(
    db: &mut Connection,
    tables: &mut BTreeSet<String>,
    effects: impl IntoIterator<Item = (&'a str, &'a str, Option<&'a str>, Option<&'a str>)>,
    allowed: &[&str],
) -> anyhow::Result<()> {
    let tx = db.transaction()?;
    let mut changed_tables = BTreeSet::new();
    let mut seen = BTreeSet::new();
    for (table, id, before, after) in effects {
        ensure!(
            allowed.contains(&table) && id.starts_with(&format!("{table}:")),
            "unapproved generated control identity"
        );
        ensure!(seen.insert((table, id)), "duplicate transaction effect");
        let valid = |s: &str| {
            s.len() == 64
                && s.bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        };
        ensure!(
            after.is_none_or(valid)
                && before.is_none_or(valid)
                && (before.is_some() || after.is_some()),
            "invalid native effect digest"
        );
        let actual: Option<String> = tx
            .query_row(
                "SELECT h FROM expected WHERE t=? AND k=?",
                params![table, id],
                |row| row.get(0),
            )
            .optional()?;
        ensure!(
            actual.as_deref() == before,
            "generated effect before-image differs in {table}"
        );
        if after.is_none() {
            ensure!(
                tx.execute("DELETE FROM expected WHERE t=? AND k=?", params![table, id])? == 1,
                "effect deletion missing"
            );
        } else if actual.is_some() {
            ensure!(
                tx.execute(
                    "UPDATE expected SET h=? WHERE t=? AND k=?",
                    params![after, table, id]
                )? == 1,
                "effect update missing"
            );
        } else {
            let next: u64 = tx.query_row(
                "SELECT coalesce(max(seq)+1,0) FROM expected WHERE t=?",
                [table],
                |row| row.get(0),
            )?;
            tx.execute(
                "INSERT INTO expected VALUES (?,?,?,?)",
                params![table, id, after, next],
            )?;
        }
        changed_tables.insert(table.to_owned());
    }
    tx.commit()?;
    tables.extend(changed_tables);
    Ok(())
}

/// The fresh copy creates only Doc physical fences. Derive the exact finite
/// bucket set from original typed identities, validate every native row shape,
/// and approve those identities only. No arbitrary control rows are captured.
async fn seal_copy_physical_fences(
    spool: &SourceSpool,
    dst: &StorageClient,
    checks: &mut Connection,
    tables: &mut BTreeSet<String>,
) -> anyhow::Result<()> {
    #[derive(Deserialize)]
    struct Row {
        id: String,
        sha256: String,
        valid: bool,
    }
    let expected = {
        let source = source_spool::readonly(&spool.path)?;
        let mut query = source.prepare("SELECT k FROM rows WHERE t='doc' ORDER BY k")?;
        let mut expected = BTreeSet::new();
        for key in query.query_map([], |row| row.get::<_, String>(0))? {
            let id: elephant_types::RecordId = key?.parse()?;
            ensure!(
                id.table() == elephant_types::TableName::Doc,
                "doc spool identity differs"
            );
            let native = elephant_storage::surreal::thing_from_record_id(&id);
            let bucket = hex::encode(Sha256::digest(native.to_string().as_bytes()));
            let mut fence = native;
            fence.tb = "native_read_fence".into();
            fence.id = format!("doc:{}", &bucket[..3]).into();
            expected.insert(fence.to_string());
        }
        expected
    };
    ensure!(expected.len() <= 4096, "physical copy bucket limit");

    let mut response=dst.query_checked("SELECT type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256,fn::recall_physical_read_fence_valid($this) AS valid FROM native_read_fence LIMIT 4097").await?;
    let rows: Vec<Row> = response.take(0)?;
    ensure!(
        rows.len() == expected.len()
            && rows.iter().all(|r| r.valid)
            && rows.iter().map(|r| r.id.clone()).collect::<BTreeSet<_>>() == expected,
        "copy physical fence inventory differs"
    );
    apply_effects(
        checks,
        tables,
        rows.iter().map(|r| {
            (
                "native_read_fence",
                r.id.as_str(),
                None,
                Some(r.sha256.as_str()),
            )
        }),
        &["native_read_fence"],
    )?;
    Ok(())
}

pub(super) async fn run(args: super::DataMigrateArgs) -> anyhow::Result<()> {
    let plan = args
        .preservation_plan
        .as_ref()
        .context("preservation plan required")?;
    let report = args
        .report
        .as_ref()
        .context("bounded transfer requires report")?;
    ensure!(
        std::fs::symlink_metadata(report).is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
        "report must be a new path"
    );
    let scratch = report.with_extension("transfer.sqlite");
    let spool = SourceSpool::open(plan, &scratch)?;
    let config = args
        .historical_config
        .as_ref()
        .context("bounded historical transfer requires --historical-config")?;
    let adoption = super::historical_adopt::prepare(
        &spool,
        plan,
        config,
        &report.with_extension("adoption.sqlite"),
    )?;
    let receipts = report.with_extension("adoption.jsonl");
    ensure!(
        std::fs::symlink_metadata(&receipts)
            .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
        "adoption receipt output must be new"
    );
    let original_digest = spool.source_sha256.clone();
    run_with_connection(
        args.target.connect().await?,
        spool,
        scratch,
        report,
        adoption,
        receipts,
        &original_digest,
    )
    .await
}

async fn run_with_connection(
    dst: StorageClient,
    spool: SourceSpool,
    scratch: std::path::PathBuf,
    report: &Path,
    adoption: super::historical_adopt::PreparedAdoption,
    receipts: std::path::PathBuf,
    original_digest: &str,
) -> anyhow::Result<()> {
    let transfer = async {
        super::ensure_empty_target(&dst).await?;
        let report = report.to_path_buf();
        dst.with_transfer_barrier(original_digest, move |exclusive| async move {
            Box::pin(copy(
                &spool, &exclusive, &scratch, &report, adoption, &receipts,
            ))
            .await
            .map_err(|e| {
                elephant_storage::StorageError::DataIntegrityError(format!(
                    "bounded transfer incomplete: {e:#}"
                ))
            })
        })
        .await?;
        Ok::<(), anyhow::Error>(())
    }
    .await;
    // An incomplete transfer keeps its original marker even when close succeeds.
    // Closing is awaited on both outcomes, before main can drop the runtime.
    super::finish_owned_connection(dst, transfer).await?;
    println!("bounded transfer completed; source_sha256={original_digest}");
    Ok(())
}

pub(super) async fn copy(
    spool: &SourceSpool,
    dst: &StorageClient,
    scratch: &Path,
    report_path: &Path,
    adoption: super::historical_adopt::PreparedAdoption,
    receipts_path: &Path,
) -> anyhow::Result<()> {
    let transfer_started = std::time::Instant::now();
    let mut phase_seconds = BTreeMap::new();
    let mut phase_started = std::time::Instant::now();
    elephant_migrations::apply_schema(dst)
        .await
        .map_err(|e| anyhow::anyhow!("apply schema: {e}"))?;
    let mut checks = Connection::open(scratch)?;
    checks.execute_batch("PRAGMA cache_size=-8192; PRAGMA synchronous=FULL;")?;
    let mut tables = dst
        .list_tables()
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    for table in &tables {
        capture(dst, &mut checks, table).await?;
    }
    let bootstrap = adoption.bootstrap(dst).await?;
    apply_effects(
        &mut checks,
        &mut tables,
        bootstrap.iter().map(|e| {
            (
                e.table.as_str(),
                e.id.as_str(),
                e.before_sha256.as_deref(),
                Some(e.after_sha256.as_str()),
            )
        }),
        &["space_config", "canonical_epoch", "source_erasure_guard"],
    )?;
    let config_effect = bootstrap
        .iter()
        .find(|e| e.table == "space_config")
        .context("bootstrap configuration effect missing")?;
    let mut response = dst
        .query_bind_checked(
            "SELECT * FROM ONLY type::thing($id)",
            BTreeMap::from([("id".to_owned(), Native::from(config_effect.id.clone()))]),
        )
        .await?;
    let config_raw: NativeQueryValue = response.take(0)?;
    let config_before = config_raw.into_inner();
    ensure!(
        matches!(config_before, Native::Object(_)),
        "bootstrap configuration missing or malformed"
    );
    let mut copy_visibility_writes = 0_i64;
    phase_seconds.insert("schema_bootstrap", phase_started.elapsed().as_secs_f64());
    phase_started = std::time::Instant::now();
    // No live operational row is omitted until the independent archive census
    // and journal-registered seal have both verified the complete source cut.
    let archive = super::archive_copy::prepare_initial(spool, dst)?;
    phase_seconds.insert("operational_archive", phase_started.elapsed().as_secs_f64());
    phase_started = std::time::Instant::now();
    let mut counts = BTreeMap::new();
    let mut table_phases = BTreeMap::new();
    let mut source_hash = Sha256::new();
    for (table, count) in &spool.tables {
        source_hash.update(serde_json::to_vec(&serde_json::json!([
            "table", table, count
        ]))?);
        source_hash.update(b"\n");
    }
    for receipt in &spool.retained_controls {
        source_hash.update(serde_json::to_vec(&serde_json::json!([
            "retained_control",
            receipt
        ]))?);
        source_hash.update(b"\n");
    }
    let mut entity_vectors = 0_u64;
    let manager = elephant_embeddings::PartitionManager::new();
    // Exact same graph dependency order as the reviewed scalar transfer.
    for table in super::SUPPORTED_TABLES {
        if *table == "id_map" {
            continue;
        }
        let table_started = std::time::Instant::now();
        let mut spool_seconds = 0.0_f64;
        let mut project_seconds = 0.0_f64;
        let mut write_seconds = 0.0_f64;
        let mut lexical_bytes = 0_u64;
        let mut pages = 0_u64;
        let mut cursor = None;
        let mut written = 0_u64;
        let mut partitions = BTreeMap::<String, u64>::new();
        let archived = spool.operational_disposition.archives(table);
        ensure!(
            !archived || archive.is_some(),
            "archive source lacks a registered seal"
        );
        loop {
            let phase = std::time::Instant::now();
            let page = spool.page(table, cursor.as_deref())?;
            spool_seconds += phase.elapsed().as_secs_f64();
            if page.is_empty() {
                break;
            }
            pages += 1;
            let mut ordinary = Vec::new();
            let mut embeddings = Vec::new();
            let phase = std::time::Instant::now();
            for row in &page {
                lexical_bytes = lexical_bytes
                    .checked_add(row.lexical.len() as u64)
                    .context("table lexical byte count overflow")?;
                super::archive_copy::hash_source_row(&mut source_hash, table, row)?;
                if archived {
                    continue;
                }
                match spool.projected(table, &row.lexical)? {
                    TypedRow::Ordinary(record) => {
                        if record.base().space_id == spool.target_space_id
                            && matches!(
                                record,
                                TransferRecord::Entity(_)
                                    | TransferRecord::Source(_)
                                    | TransferRecord::Doc(_)
                                    | TransferRecord::DocRevision(_)
                                    | TransferRecord::DocNode(_)
                            )
                        {
                            copy_visibility_writes = copy_visibility_writes
                                .checked_add(1)
                                .context("copy visibility count overflow")?;
                        }
                        ordinary.push(record);
                    }
                    TypedRow::Space(record) => {
                        let expected = serde_json::to_value(&record)?;
                        let stored = SpaceConfigRepository::new(dst.clone())
                            .create(record)
                            .await?;
                        ensure!(
                            serde_json::to_value(stored)? == expected,
                            "space config transfer changed payload"
                        );
                    }
                    TypedRow::Embedding(record) => embeddings.push(record),
                    TypedRow::Receipt(_) => anyhow::bail!("receipt in tenant table"),
                }
            }
            project_seconds += phase.elapsed().as_secs_f64();
            let phase = std::time::Instant::now();
            if !embeddings.is_empty() {
                for partition in
                    import_embedding_batch(dst, &spool.source_sha256, &manager, &embeddings).await?
                {
                    *partitions.entry(partition).or_default() += 1;
                }
            }
            if !ordinary.is_empty() {
                import_batch(dst, &spool.source_sha256, &ordinary).await?;
                for record in &ordinary {
                    if let TransferRecord::Entity(entity) = record {
                        if let Some((vector, model, hash)) = super::entity_embedding(entity)? {
                            super::attach_entity_embedding(dst, entity, vector, model, hash)
                                .await?;
                            entity_vectors += 1;
                            if entity.base.space_id == spool.target_space_id {
                                copy_visibility_writes = copy_visibility_writes
                                    .checked_add(1)
                                    .context("embedding visibility count overflow")?;
                            }
                        }
                    }
                }
            }
            write_seconds += phase.elapsed().as_secs_f64();
            written += page.len() as u64;
            cursor = page.last().map(|r| r.id.clone());
        }
        ensure!(
            written == *spool.tables.get(*table).unwrap_or(&0),
            "source table count changed"
        );
        let reconciling = std::time::Instant::now();
        let native_written = if archived { 0 } else { written };
        if archived {
            ensure!(
                archive
                    .as_ref()
                    .context("archive seal absent")?
                    .1
                    .tables
                    .get(*table)
                    .copied()
                    .unwrap_or(0)
                    == written,
                "source rows differ from sealed archive census"
            );
            if tables.contains(*table) {
                ensure!(
                    capture(dst, &mut checks, table).await? == 0,
                    "archived operational rows unexpectedly exist in native target"
                );
            }
        } else if *table == "chunk_embedding" {
            for (partition, count) in partitions {
                ensure!(
                    capture(dst, &mut checks, &partition).await? == count,
                    "partition inventory differs"
                );
                tables.insert(partition);
            }
        } else if *table == "space_config" {
            ensure!(
                written == 0,
                "bootstrap requires original space_config zero"
            );
        } else if written > 0 || tables.contains(*table) {
            ensure!(
                capture(dst, &mut checks, table).await? == written,
                "typed table count differs"
            );
            tables.insert((*table).into());
        }
        table_phases.insert(
            (*table).to_owned(),
            serde_json::json!({
                "rows":written,"native_rows":native_written,"archived_rows":if archived { written } else { 0 },
                "original_lexical_bytes":lexical_bytes,"pages":pages,
                "seconds":table_started.elapsed().as_secs_f64(),"spool_page_seconds":spool_seconds,
                "project_and_hash_seconds":project_seconds,"typed_write_seconds":write_seconds,
                "native_capture_seconds":reconciling.elapsed().as_secs_f64()
            }),
        );
        counts.insert((*table).to_owned(), native_written);
        if archived {
            println!("{table}: {written} rows retained in managed archive");
        } else {
            println!("{table}: {written} rows");
        }
    }
    // Migration033 advances only index_generation and updated_at on the exact
    // bootstrap configuration for each original indexed-content write.
    let mut response = dst.query_bind_checked(
        "LET $actual=SELECT * FROM ONLY type::thing($id); IF $actual=NONE OR crypto::sha256(type::string($before))!=$before_sha OR object::keys($actual)!=object::keys($before) OR object::keys($actual.processing)!=object::keys($before.processing) OR $actual.processing.index_generation!=$before.processing.index_generation+$writes OR !type::is::datetime($actual.updated_at) { THROW 'copy configuration differs'; }; FOR $key IN object::keys($before) { IF $key NOT IN ['processing','updated_at'] AND $actual[$key]!=$before[$key] { THROW 'copy configuration authority changed'; }; }; FOR $key IN object::keys($before.processing) { IF $key!='index_generation' AND $actual.processing[$key]!=$before.processing[$key] { THROW 'copy processing authority changed'; }; }; RETURN crypto::sha256(type::string($actual));",
        BTreeMap::from([
            ("id".to_owned(), Native::from(config_effect.id.clone())),
            ("before".to_owned(), config_before),
            ("before_sha".to_owned(), Native::from(config_effect.after_sha256.clone())),
            ("writes".to_owned(), Native::from(copy_visibility_writes)),
        ]),
    ).await?;
    let config_after: Option<String> = response.take(response.num_statements() - 1)?;
    let config_after = config_after.context("copy configuration hash missing")?;
    apply_effects(
        &mut checks,
        &mut tables,
        [(
            "space_config",
            config_effect.id.as_str(),
            Some(config_effect.after_sha256.as_str()),
            Some(config_after.as_str()),
        )],
        &["space_config"],
    )?;
    // Validate and seal only identified destination-owned controls.
    let mut controls = BTreeMap::from([
        ("canonical_epoch".into(), Vec::new()),
        ("source_erasure_guard".into(), Vec::new()),
    ]);
    super::seal_control_spaces(&spool.projected_spaces(), dst, &mut controls).await?;
    for table in controls.keys() {
        capture(dst, &mut checks, table).await?;
        tables.insert(table.clone());
    }
    seal_copy_physical_fences(spool, dst, &mut checks, &mut tables).await?;
    let copied_destination_hash = reconcile(dst, &mut checks, &tables).await?;
    phase_seconds.insert("copy_and_reconcile", phase_started.elapsed().as_secs_f64());
    phase_started = std::time::Instant::now();
    let adoption_report = Box::pin(adoption.adopt(dst, receipts_path, |effects| {
        apply_effects(
            &mut checks,
            &mut tables,
            effects.iter().map(|e| {
                (
                    e.table.as_str(),
                    e.id.as_str(),
                    e.before_sha256.as_deref(),
                    Some(e.after_sha256.as_str()),
                )
            }),
            &[
                "doc",
                "source",
                "source_head",
                "source_identity",
                "source_revision",
                "source_operation",
                "source_authorization",
                "source_publication_manifest",
                "source_publication_completion",
                "source_commit_receipt",
                "source_effect_support",
                "source_document_mapping",
                "source_revision_mapping",
                "space_config",
                "canonical_epoch",
                "source_erasure_guard",
                "native_read_fence",
            ],
        )
    }))
    .await?;
    phase_seconds.insert("historical_adoption", phase_started.elapsed().as_secs_f64());
    phase_started = std::time::Instant::now();
    let held = if archive.is_some() {
        // Original status is retained, including running work. Archived rows
        // have no native dispatch presence or producer-origin claims.
        spool.historical_work_counts.clone()
    } else {
        let mut held = BTreeMap::from([
            ("work_item".to_owned(), 0_u64),
            ("work_item_archive".to_owned(), 0_u64),
            ("running".to_owned(), 0_u64),
        ]);
        for table in ["work_item", "work_item_archive"] {
            let mut cursor = None;
            loop {
                let page = spool.page(table, cursor.as_deref())?;
                if page.is_empty() {
                    break;
                }
                let mut input = Vec::with_capacity(page.len());
                for row in &page {
                    let original = TransferRecord::from_json(table, &row.lexical)?;
                    let source_space_id = original.base().space_id.clone();
                    let target_space_id = if source_space_id == spool.space_id {
                        spool.target_space_id.clone()
                    } else {
                        source_space_id.clone()
                    };
                    let running = match &original {
                        TransferRecord::WorkItem(w) => {
                            w.status == elephant_types::records::WorkItemStatus::Running
                        }
                        TransferRecord::WorkItemArchive(w) => {
                            w.record.status == elephant_types::records::WorkItemStatus::Running
                        }
                        _ => false,
                    };
                    *held.get_mut(table).context("held work table")? += 1;
                    if running {
                        *held.get_mut("running").context("running count")? += 1;
                    }
                    input.push(
                        elephant_storage::work_producer_repository::HistoricalWorkHoldInput {
                            original,
                            original_row_sha256: row.original_sha256.clone(),
                            source_space_id,
                            target_space_id,
                        },
                    );
                }
                let held_spaces = input
                    .iter()
                    .map(|row| row.target_space_id.clone())
                    .collect::<BTreeSet<_>>();
                let registration =
                    elephant_storage::work_producer_repository::WorkProducerRepository::new(
                        dst.clone(),
                    )
                    .register_historical_holds(&spool.source_sha256, &input)
                    .await?;
                ensure!(
                    registration.payload_sha256.len() == page.len()
                        && registration.native_effects.len() == page.len() + held_spaces.len(),
                    "historical hold inventory incomplete"
                );
                apply_effects(
                    &mut checks,
                    &mut tables,
                    registration.native_effects.iter().map(|e| {
                        (
                            e.table.as_str(),
                            e.id.as_str(),
                            e.before_sha256.as_deref(),
                            Some(e.after_sha256.as_str()),
                        )
                    }),
                    &["work_producer_origin", "source_erasure_guard"],
                )?;
                cursor = page.last().map(|r| r.id.clone());
            }
        }
        held
    };
    ensure!(
        held == spool.historical_work_counts,
        "historical hold count differs"
    );
    let outbox = elephant_storage::outbox_checkpoint_repository::OutboxCheckpointRepository::new(
        dst.clone(),
    );
    let mut event_baselines = BTreeMap::new();
    for space in spool.projected_spaces() {
        let registration = if let Some((sealed, _)) = &archive {
            outbox
                .preserve_archived_events_with_effects(&space, sealed)
                .await?
        } else {
            outbox
                .preserve_historical_events_with_effects(&space, &spool.source_sha256)
                .await?
        };
        ensure!(
            registration.native_effects.len() == 3,
            "historical event inventory incomplete"
        );
        apply_effects(
            &mut checks,
            &mut tables,
            registration.native_effects.iter().map(|e| {
                (
                    e.table.as_str(),
                    e.id.as_str(),
                    e.before_sha256.as_deref(),
                    Some(e.after_sha256.as_str()),
                )
            }),
            &["outbox_checkpoint"],
        )?;
        event_baselines.insert(space, registration.baselines);
    }
    phase_seconds.insert(
        "work_hold_and_event_baseline",
        phase_started.elapsed().as_secs_f64(),
    );
    phase_started = std::time::Instant::now();
    let canonical = elephant_storage::canonical_commit::CanonicalRepository::new(dst.clone());
    let operational_archive = if let Some((sealed, _)) = &archive {
        let epoch = canonical
            .observe_epoch(&spool.target_space_id)
            .await?
            .context("archive commit epoch absent")?;
        let committed = sealed.commit(dst, &epoch).await?;
        let effect = &committed.effect;
        apply_effects(
            &mut checks,
            &mut tables,
            [(
                effect.table.as_str(),
                effect.id.as_str(),
                effect.before_sha256.as_deref(),
                Some(effect.after_sha256.as_str()),
            )],
            &["outbox_checkpoint"],
        )?;
        Some(committed.receipt)
    } else {
        None
    };
    let destination_hash = reconcile(dst, &mut checks, &tables).await?;
    spool.verify_unchanged()?;
    phase_seconds.insert("final_reconcile", phase_started.elapsed().as_secs_f64());
    phase_seconds.insert(
        "transfer_before_report_fsync",
        transfer_started.elapsed().as_secs_f64(),
    );
    let baseline_epoch = canonical
        .observe_epoch(&spool.target_space_id)
        .await?
        .context("initial catch-up epoch absent")?;
    let binding_started = std::time::Instant::now();
    let bindings_path = report_path.with_extension("catchup-bindings.jsonl");
    let (bindings_sha256, bindings_count) =
        super::historical_adopt::write_initial_catchup_bindings(
            dst,
            receipts_path,
            &adoption_report.receipts_sha256,
            &bindings_path,
            adoption_report.groups,
        )
        .await?;
    ensure!(
        canonical
            .observe_epoch(&spool.target_space_id)
            .await?
            .as_deref()
            == Some(baseline_epoch.as_str()),
        "initial catch-up epoch changed"
    );
    phase_seconds.insert(
        "initial_catchup_bindings",
        binding_started.elapsed().as_secs_f64(),
    );
    drop(canonical);
    dst.verify_journal_identity().await?;
    let journal_identity = dst.erasure_journal()?.identity().to_owned();
    checks.close().map_err(|(_, e)| e)?;
    std::fs::File::open(scratch)?.sync_all()?;
    let baseline_ledger_sha256 = source_spool::file_sha(scratch)?;
    let retained_source_counts = spool
        .tables
        .iter()
        .filter(|(table, _)| table.as_str() == "migration_history")
        .map(|(table, count)| (table.clone(), *count))
        .collect();
    let archived_tables = archive
        .as_ref()
        .map(|(_, totals)| totals.tables.clone())
        .unwrap_or_default();
    let source_accounting = spool.operational_disposition.account(
        &spool.tables,
        &counts,
        &archived_tables,
        &retained_source_counts,
    )?;
    let historical_work_holds = if operational_archive.is_some() {
        serde_json::json!({"mode":"managed_archive_inert","verified":true,
            "rows":held,"native_holds":0,"original_runtime_requirements":spool.runtime_requirements})
    } else {
        serde_json::json!({"mode":"native_historical_holds","verified":true,
            "rows":held,"original_runtime_requirements":spool.runtime_requirements})
    };
    // New aggregate includes bindings, final journal verification and the closed
    // ledger. Preserve the earlier historical metric; neither includes report
    // serialization/fsync or the outer physical persist, marker clear and close.
    phase_seconds.insert(
        "transfer_before_report_encoding",
        transfer_started.elapsed().as_secs_f64(),
    );
    let mut report = serde_json::json!({"operational_disposition":spool.operational_disposition,
        "archived_tables":archived_tables,
        "archive_lexical_bytes":archive.as_ref().map(|(_, totals)| totals.lexical_bytes),
        "archive_table_phases":archive.as_ref().map(|(_, totals)| &totals.table_phases),
        "archive_seal_seconds":archive.as_ref().map(|(_, totals)| totals.seal_seconds),
        "archive_ordered_inventory_sha256":archive.as_ref().map(|(_, totals)| &totals.ordered_inventory_sha256),
        "source_accounting":source_accounting,"catchup_baseline":{
        "operational_disposition":spool.operational_disposition,"format":"recall-initial-catchup-bindings-v1","bindings_sha256":bindings_sha256,"bindings_count":bindings_count,"expected_epoch":baseline_epoch,"journal_identity":journal_identity,"expected_ledger_sha256":baseline_ledger_sha256,"expected_tables":tables,"native_rows_sha256":destination_hash},"phase_seconds":phase_seconds,"table_phases":table_phases,"format":"recall-legacy-transfer-v2","state":"initial_history_reconciled_vector_coverage_pending","adoption":adoption_report,"copied_destination_native_rows_sha256":copied_destination_hash,"historical_work_holds":historical_work_holds,"historical_event_baselines":event_baselines,"retained_export_notes":spool.export_notes,"media_preservation":spool.media_preservation,"source_space_id":spool.space_id,"target_space_id":spool.target_space_id,"source_sha256":spool.source_sha256,"plan_id":spool.plan_id,"inventory_sha256":spool.inventory_sha256,"source_tables":spool.tables,"copied_tables":counts,"entity_name_embedding":entity_vectors,"retained_source_controls":{"migration_history":spool.retained_controls},"source_lexical_rows_sha256":hex::encode(source_hash.finalize()),"destination_native_rows_sha256":destination_hash,"hash_domains":{"source":"declared table/count, retained control and typed table/id/original-lexical-sha256 ordered JSON lines v1","destination":"native table/id/type-string-sha256 ordered JSON lines v1"}});
    if let Some(receipt) = operational_archive {
        report["operational_archive"] = serde_json::to_value(&receipt)?;
        report["catchup_baseline"]["operational_archive"] = serde_json::to_value(&receipt)?;
    }
    let mut output = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(report_path)?;
    output.write_all(&serde_json::to_vec_pretty(&report)?)?;
    output.sync_all()?;
    #[cfg(unix)]
    std::fs::File::open(
        report_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new(".")),
    )?
    .sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_types::records::{AuditAction, AuditLog, BaseFields};
    use elephant_types::{RecordId, TableName};

    #[test]
    fn explicit_native_effects_require_exact_before_images_and_allowed_ids() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));").unwrap();
        let mut tables = BTreeSet::new();
        let a = "a".repeat(64);
        let b = "b".repeat(64);
        apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:z", None, Some(a.as_str()))],
            ["probe"].as_slice(),
        )
        .unwrap();
        apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:a", None, Some(b.as_str()))],
            ["probe"].as_slice(),
        )
        .unwrap();
        assert!(apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:z", Some(b.as_str()), Some(b.as_str()))],
            &["probe"]
        )
        .is_err());
        assert!(apply_effects(
            &mut db,
            &mut tables,
            [
                ("probe", "probe:x", None, Some(a.as_str())),
                ("probe", "probe:z", None, Some(b.as_str()))
            ],
            &["probe"]
        )
        .is_err());
        let count: u64 = db
            .query_row("SELECT count(*) FROM expected", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 2);
        assert!(apply_effects(
            &mut db,
            &mut tables,
            [("other", "other:x", None, Some(a.as_str()))],
            &["probe"]
        )
        .is_err());
        apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:z", Some(a.as_str()), Some(b.as_str()))],
            &["probe"],
        )
        .unwrap();
        assert!(apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:z", Some(a.as_str()), None)],
            &["probe"]
        )
        .is_err());
        apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:z", Some(b.as_str()), None)],
            &["probe"],
        )
        .unwrap();
        assert!(apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:z", Some(b.as_str()), None)],
            &["probe"]
        )
        .is_err());
        assert!(apply_effects(
            &mut db,
            &mut tables,
            [("probe", "probe:missing", None, None)],
            &["probe"]
        )
        .is_err());
        let count: u64 = db
            .query_row("SELECT count(*) FROM expected", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
        assert!(tables.contains("probe"));
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "The original fixture client remains the native datastore owner through every query, reconciliation assertion and transfer result; retain its original lifetime."
    )]
    #[tokio::test]
    async fn paged_native_reconciliation_detects_changed_added_and_missing_rows() {
        use std::fmt::Write as _;
        let dst = StorageClient::new_memory().await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let mut checks = Connection::open(directory.path().join("fingerprints.sqlite")).unwrap();
        checks.execute_batch("CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));").unwrap();
        for first in (0..513).step_by(256) {
            let mut query = String::from("BEGIN TRANSACTION;");
            let mut bindings = BTreeMap::new();
            for i in first..(first + 256).min(513) {
                write!(
                    query,
                    "CREATE type::thing('probe',$id_{i}) SET value=$value_{i};"
                )
                .unwrap();
                bindings.insert(format!("id_{i}"), Native::from(format!("row{i:04}")));
                bindings.insert(format!("value_{i}"), Native::from(i64::from(i)));
            }
            query.push_str("COMMIT TRANSACTION;");
            dst.query_bind_checked(&query, bindings).await.unwrap();
        }
        assert_eq!(capture(&dst, &mut checks, "probe").await.unwrap(), 513);
        let tables = BTreeSet::from(["probe".to_owned()]);
        let original = reconcile(&dst, &mut checks, &tables).await.unwrap();
        dst.query_checked("UPDATE probe:row0256 SET value=9999")
            .await
            .unwrap();
        assert!(reconcile(&dst, &mut checks, &tables).await.is_err());
        dst.query_checked("UPDATE probe:row0256 SET value=256")
            .await
            .unwrap();
        assert_eq!(
            reconcile(&dst, &mut checks, &tables).await.unwrap(),
            original
        );
        dst.query_checked("CREATE probe:zzextra SET value=1")
            .await
            .unwrap();
        assert!(reconcile(&dst, &mut checks, &tables).await.is_err());
        dst.query_checked("DELETE probe:zzextra; DELETE probe:row0512")
            .await
            .unwrap();
        assert!(reconcile(&dst, &mut checks, &tables).await.is_err());
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "The same native fixture owner spans original ORDER readback, mixed-ID traversal, mutation rejection and final empty-page verification."
    )]
    #[expect(
        clippy::literal_string_with_formatting_args,
        reason = "The query contains a native SurrealQL object literal; its braces are original database syntax, not Rust formatting placeholders."
    )]
    #[tokio::test]
    async fn native_range_pages_preserve_mixed_id_order_hashes_and_complete_inventory() {
        use elephant_storage::canonical_extension::{NativeNumber, NativeObject as Object};
        use std::ops::Bound;
        #[derive(Deserialize)]
        struct OldRow {
            id: String,
            sha256: String,
        }
        let dst = StorageClient::new_memory().await.unwrap();
        let mut ids = Vec::new();
        for i in -70..70 {
            ids.push(Id::Number(i));
        }
        for i in 0..140 {
            ids.push(Id::String(format!("escaped space 'é {i:04}")));
            ids.push(Id::Uuid(uuid::Uuid::from_u128(i + 1).into()));
        }
        for i in 0..41 {
            ids.push(Id::Array(
                vec![Native::from(i), Native::from("part")].into(),
            ));
            ids.push(Id::Object(Object::from(BTreeMap::<String, Native>::from(
                [
                    ("number".into(), Native::from(i)),
                    ("text".into(), Native::from("nested")),
                ],
            ))));
        }
        // These IDs deliberately disagree between physical storekey ordering
        // and semantic ORDER: prefixes, numeric variants and nested containers.
        let decimal = Native::Number(NativeNumber::Decimal("0.25".parse().unwrap()));
        ids.extend([
            Id::Array(vec![Native::from(1)].into()),
            Id::Array(vec![Native::from(1), Native::from(2)].into()),
            Id::Array(vec![Native::from(100)].into()),
            Id::Array(vec![Native::from(1.5_f64)].into()),
            Id::Array(vec![decimal].into()),
            Id::Array(vec![Native::Array(vec![Native::from(1)].into())].into()),
            Id::Array(vec![Native::Array(vec![Native::from(1), Native::from(2)].into())].into()),
            Id::Object(Object::from(BTreeMap::<String, Native>::from([(
                "nested".into(),
                Native::Array(vec![Native::from(1)].into()),
            )]))),
            Id::Object(Object::from(BTreeMap::<String, Native>::from([(
                "number".into(),
                Native::from(100),
            )]))),
            Id::Object(Object::from(BTreeMap::<String, Native>::from([(
                "number".into(),
                Native::from(1.5_f64),
            )]))),
        ]);
        assert_eq!(ids.len(), 512);
        // Insert reverse order so the expected traversal cannot borrow insertion order.
        for (index, id) in ids.iter().rev().enumerate() {
            dst.query_bind_checked(
                "CREATE $id SET payload=$payload,unknown_extra={kept:NULL,nested:[1,'value']}",
                BTreeMap::<String, Native>::from([
                    (
                        "id".into(),
                        Native::Thing(Thing::from(("mixed_probe", id.clone()))),
                    ),
                    (
                        "payload".into(),
                        Native::from(i64::try_from(index).unwrap()),
                    ),
                ]),
            )
            .await
            .unwrap();
        }

        let mut response = dst.query_checked("SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM mixed_probe ORDER BY cursor").await.unwrap();
        let old: Vec<OldRow> = response.take(0).unwrap();
        let mut actual = Vec::new();
        let mut cursor = None;
        let mut page_lengths = Vec::new();
        loop {
            let page = fingerprints(&dst, "mixed_probe", cursor.clone())
                .await
                .unwrap();
            page_lengths.push(page.len());
            if page.is_empty() {
                break;
            }
            cursor = Some(page.last().unwrap().cursor.clone());
            actual.extend(page.into_iter().map(|row| (row.id, row.sha256)));
        }
        assert_eq!(page_lengths, vec![256, 256, 0]);
        assert_eq!(
            actual,
            old.iter()
                .map(|row| (row.id.clone(), row.sha256.clone()))
                .collect::<Vec<_>>()
        );
        // Explicitly start at scalar/complex boundaries and complex cursors,
        // including the semantic prefix and nested numeric counterexamples.
        for id in [
            Id::Uuid(uuid::Uuid::from_u128(140).into()),
            Id::Array(vec![Native::from(1)].into()),
            Id::Array(vec![Native::from(1.5_f64)].into()),
            Id::Object(Object::from(BTreeMap::<String, Native>::from([(
                "number".into(),
                Native::from(1.5_f64),
            )]))),
        ] {
            let cursor = Native::Thing(Thing::from(("mixed_probe", id)));
            let mut response = dst.query_bind_checked(
                "SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM mixed_probe WHERE id > $after ORDER BY cursor LIMIT 256",
                BTreeMap::<String, Native>::from([("after".into(), cursor.clone())]),
            ).await.unwrap();
            let expected: Vec<OldRow> = response.take(0).unwrap();
            let page = fingerprints(&dst, "mixed_probe", Some(cursor))
                .await
                .unwrap();
            assert_eq!(
                page.into_iter()
                    .map(|row| (row.id, row.sha256))
                    .collect::<Vec<_>>(),
                expected
                    .into_iter()
                    .map(|row| (row.id, row.sha256))
                    .collect::<Vec<_>>()
            );
        }
        let mut digest = Sha256::new();
        digest.update(serde_json::to_vec(&serde_json::json!(["table", "mixed_probe"])).unwrap());
        digest.update(b"\n");
        for row in &old {
            digest.update(
                serde_json::to_vec(&serde_json::json!(["mixed_probe", row.id, row.sha256]))
                    .unwrap(),
            );
            digest.update(b"\n");
        }
        let expected_digest = hex::encode(digest.finalize());
        let directory = tempfile::tempdir().unwrap();
        let mut checks = Connection::open(directory.path().join("fingerprints.sqlite")).unwrap();
        checks.execute_batch("CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));").unwrap();
        assert_eq!(
            capture(&dst, &mut checks, "mixed_probe").await.unwrap(),
            512
        );
        let tables = BTreeSet::from(["mixed_probe".to_owned()]);
        assert_eq!(
            reconcile(&dst, &mut checks, &tables).await.unwrap(),
            expected_digest
        );
        let range = Native::Thing(Thing::from((
            "mixed_probe",
            Id::Range(Box::new(IdRange {
                beg: Bound::Unbounded,
                end: Bound::Unbounded,
            })),
        )));
        let mut response = dst.query_bind_checked("SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM $range LIMIT 256 EXPLAIN",BTreeMap::<String, Native>::from([("range".into(),range)])).await.unwrap();
        let explain: NativeQueryValue = response.take(0).unwrap();
        let explain = explain.into_inner().to_string();
        assert!(explain.contains("Iterate Range"), "{explain}");
        assert!(!explain.contains("Iterate Table"), "{explain}");
        let changed = Native::Thing(Thing::from(("mixed_probe", ids[256].clone())));
        dst.query_bind_checked(
            "UPDATE $id SET unknown_late={changed:true}",
            BTreeMap::<String, Native>::from([("id".into(), changed)]),
        )
        .await
        .unwrap();
        assert!(reconcile(&dst, &mut checks, &tables).await.is_err());
        assert!(
            fingerprints(&dst, "mixed_probe", Some(Native::from("not-record")))
                .await
                .is_err()
        );
        assert!(fingerprints(
            &dst,
            "mixed_probe",
            Some(Native::Thing(Thing::from(("foreign", Id::Number(1)))))
        )
        .await
        .is_err());
        // Distinct physical IDs can compare equal semantically. The fallback
        // must preserve the old page, including both rows, without adding a
        // strict-advance refusal. Existing equal-key page-boundary behavior is
        // deliberately unchanged by this optimization.
        for id in [
            Id::Array(vec![Native::from(1)].into()),
            Id::Array(vec![Native::from(1.0_f64)].into()),
        ] {
            dst.query_bind_checked(
                "CREATE $id SET payload='equal-sort-key'",
                BTreeMap::<String, Native>::from([(
                    "id".into(),
                    Native::Thing(Thing::from(("equal_probe", id))),
                )]),
            )
            .await
            .unwrap();
        }
        let mut response = dst.query_checked("SELECT id AS cursor,type::string(id) AS id,crypto::sha256(type::string($this)) AS sha256 FROM equal_probe ORDER BY cursor LIMIT 256").await.unwrap();
        let expected: Vec<OldRow> = response.take(0).unwrap();
        assert_eq!(expected.len(), 2);
        let page = fingerprints(&dst, "equal_probe", None).await.unwrap();
        assert_eq!(
            page.into_iter()
                .map(|row| (row.id, row.sha256))
                .collect::<Vec<_>>(),
            expected
                .into_iter()
                .map(|row| (row.id, row.sha256))
                .collect::<Vec<_>>()
        );
        dst.query_checked("DEFINE TABLE empty_probe SCHEMALESS")
            .await
            .unwrap();
        assert!(fingerprints(&dst, "empty_probe", None)
            .await
            .unwrap()
            .is_empty());
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "The original fixture client remains the native datastore owner through every query, reconciliation assertion and transfer result; retain its original lifetime."
    )]
    #[tokio::test]
    async fn typed_batch_full_schema_retains_original_native_fields() {
        let dst = StorageClient::new_memory().await.unwrap();
        let digest = "a".repeat(64);
        let bound = digest.clone();
        dst.with_transfer_barrier(&digest, move |exclusive| async move {
            elephant_migrations::apply_schema(&exclusive)
                .await
                .map_err(elephant_storage::StorageError::DataIntegrityError)?;
            let base = BaseFields::new(
                RecordId::new_v4(TableName::AuditLog),
                "synthetic".into(),
                "original".into(),
            );
            let row = AuditLog {
                timestamp: base.created_at,
                base,
                action: AuditAction::Create,
                actor: "original".into(),
                record: RecordId::new_v4(TableName::Doc),
                before: None,
                after: Some(
                    serde_json::json!({"integral_float":1.0,"integer":9_007_199_254_740_993_i64}),
                ),
                reason: Some("exact original".into()),
            };
            import_batch(&exclusive, &bound, &[TransferRecord::AuditLog(row.clone())]).await?;
            assert_eq!(
                elephant_storage::AuditLogRepository::new(exclusive.clone())
                    .get("synthetic", &row.base.id)
                    .await?,
                Some(row)
            );
            exclusive.initialize_journal_identity().await?;
            Ok(())
        })
        .await
        .unwrap();
    }
}

#[cfg(test)]
#[path = "streaming_integration_tests.rs"]
pub(super) mod integration_tests;

#[cfg(test)]
#[path = "archive_integration_tests.rs"]
mod archive_integration_tests;

#[cfg(test)]
#[path = "archive_lifecycle_integration_tests.rs"]
mod archive_lifecycle_integration_tests;

#[cfg(test)]
#[path = "legacy_assertion_literal_integration_tests.rs"]
pub(super) mod legacy_assertion_literal_integration_tests;
