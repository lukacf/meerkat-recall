//! Read-only original lexical rows from the shared offline preservation plan.
//! No whole snapshot copy or in-memory source database is constructed.

use anyhow::{ensure, Context};
use elephant_storage::transfer_batch::{
    strict_json, validate_original_representation, TransferRecord, MAX_BATCH_ROWS, MAX_ROW_BYTES,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::io::Read;
use std::path::{Path, PathBuf};

const MAX_PLAN_BYTES: u64 = 1024 * 1024;
// Observed legacy schema declarations without a current typed copy adapter.
// Their absence is preserved as inventory evidence, never as permission to
// discard a populated table.
const LEGACY_EMPTY_TABLES: &[&str] = &[
    "knowledge_view",
    "knowledge_view_definition",
    "knowledge_view_dependency",
    "knowledge_view_revision",
    "rel_evidence",
    "source_watch",
    "source_watch_check",
];

pub(super) struct SourceSpool {
    pub path: PathBuf,
    pub operational_disposition: super::archive_disposition::OperationalDisposition,
    pub archive_inventory: Option<super::archive_copy::ArchiveSourceInventory>,
    pub inventory_sha256: String,
    pub source_sha256: String,
    pub plan_id: String,
    pub space_id: String,
    pub target_space_id: String,
    pub tables: BTreeMap<String, u64>,
    pub spaces: BTreeSet<String>,
    pub retained_controls: Vec<serde_json::Value>,
    pub export_notes: serde_json::Value,
    pub media_preservation: serde_json::Value,
    pub historical_work_counts: BTreeMap<String, u64>,
    pub runtime_requirements: Vec<serde_json::Value>,
}

pub(super) struct RawRow {
    pub id: String,
    pub lexical: String,
    pub original_sha256: String,
}

pub(super) enum TypedRow {
    Ordinary(TransferRecord),
    Space(elephant_types::records::SpaceConfig),
    Embedding(elephant_types::records::NodeEmbedding),
    Receipt(serde_json::Value),
}

pub(super) fn file_sha(path: &Path) -> anyhow::Result<String> {
    let mut input = std::fs::File::open(path)?;
    let mut buffer = vec![0_u8; 1024 * 1024].into_boxed_slice();
    let mut sha = Sha256::new();
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        sha.update(&buffer[..n]);
    }
    Ok(hex::encode(sha.finalize()))
}

fn closed_sqlite(path: &Path) -> anyhow::Result<()> {
    ensure!(
        std::fs::symlink_metadata(path)?.file_type().is_file(),
        "spool must be a closed regular SQLite file"
    );
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = path.as_os_str().to_os_string();
        sidecar.push(suffix);
        ensure!(
            std::fs::symlink_metadata(PathBuf::from(sidecar))
                .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound),
            "closed SQLite artifact has a sidecar: {suffix}"
        );
    }
    let mut header = [0_u8; 20];
    std::fs::File::open(path)?.read_exact(&mut header)?;
    ensure!(
        &header[..16] == b"SQLite format 3\0" && header[18] == 1 && header[19] == 1,
        "SQLite artifact must be checkpointed in rollback-journal mode"
    );
    Ok(())
}

/// The inventory hash covers this closed main file only. Immutable mode also
/// prevents a sidecar created between checks from influencing any query.
pub(super) fn readonly(path: &Path) -> anyhow::Result<Connection> {
    closed_sqlite(path)?;
    let canonical = path.canonicalize()?;
    let mut uri = String::from("file:");
    for byte in canonical
        .to_str()
        .context("SQLite path must be UTF-8")?
        .bytes()
    {
        if byte.is_ascii_alphanumeric() || b"/-._~".contains(&byte) {
            uri.push(char::from(byte));
        } else {
            use std::fmt::Write as _;
            write!(uri, "%{byte:02X}")?;
        }
    }
    uri.push_str("?mode=ro&immutable=1");
    let db = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_NO_MUTEX
            | OpenFlags::SQLITE_OPEN_URI,
    )?;
    db.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF; PRAGMA cache_size=-8192;")?;
    let mode: String = db.query_row("PRAGMA journal_mode", [], |r| r.get(0))?;
    ensure!(mode == "delete", "SQLite artifact journal mode differs");
    closed_sqlite(path)?;
    Ok(db)
}

fn digest(value: &serde_json::Value) -> anyhow::Result<String> {
    let s = value.as_str().context("missing digest")?;
    ensure!(
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid digest"
    );
    Ok(s.into())
}

const fn spool_page_sql(continuation: bool) -> &'static str {
    if continuation {
        "SELECT r.k,length(x.lexical),x.lexical,x.lexical_sha256,r.h,r.space FROM rows r JOIN raw_rows x ON x.t=r.t AND x.k=r.k WHERE r.t=? AND r.k>? ORDER BY r.k LIMIT ?"
    } else {
        "SELECT r.k,length(x.lexical),x.lexical,x.lexical_sha256,r.h,r.space FROM rows r JOIN raw_rows x ON x.t=r.t AND x.k=r.k WHERE r.t=? ORDER BY r.k LIMIT ?"
    }
}

impl SourceSpool {
    /// Validate all lexical payloads and cross-row constraints before target open.
    pub fn open(plan_path: &Path, scratch: &Path) -> anyhow::Result<Self> {
        let plan_file = std::fs::File::open(plan_path)?;
        ensure!(
            plan_file.metadata()?.len() <= MAX_PLAN_BYTES,
            "plan exceeds bound"
        );
        let mut raw = String::new();
        plan_file
            .take(MAX_PLAN_BYTES + 1)
            .read_to_string(&mut raw)?;
        ensure!(raw.len() as u64 <= MAX_PLAN_BYTES, "plan exceeds bound");
        let plan = strict_json(&raw)?;
        ensure!(
            plan["format"] == "recall-preservation-plan-v1"
                && plan["spool_schema"] == "recall-original-lexical-row-spool-v1",
            "unsupported plan/spool format"
        );
        ensure!(
            plan["state"] == "offline_inventory_only" && plan["publication_certified"] == false,
            "plan claims runtime authority"
        );
        ensure!(
            plan["canonical_hash_contract"]["id"] == "homecore-python-json-v1",
            "unknown original hash domain"
        );
        ensure!(
            plan["snapshot_header"]["backend"] == "elephant-db"
                && plan["snapshot_header"]["version"].as_u64() == Some(1),
            "unsupported original snapshot header"
        );
        chrono::DateTime::parse_from_rfc3339(
            plan["snapshot_header"]["exported_at"]
                .as_str()
                .context("snapshot timestamp missing")?,
        )?;
        let path = plan_path
            .parent()
            .context("plan parent")?
            .join("inventory.sqlite");
        ensure!(
            std::fs::symlink_metadata(&path)?.file_type().is_file(),
            "spool must be an owned regular file"
        );
        let inventory_sha256 = digest(&plan["inventory_sha256"])?;
        ensure!(file_sha(&path)? == inventory_sha256, "spool hash differs");
        let source_sha256 = digest(&plan["snapshot_sha256"])?;
        let plan_id = digest(&plan["plan_id"])?;
        let space_id = plan["space_id"]
            .as_str()
            .filter(|s| !s.trim().is_empty())
            .context("plan space missing")?
            .to_owned();
        let target_space_id =
            if plan.get("source_space_id").is_some() || plan.get("target_space_id").is_some() {
                ensure!(
                    plan["source_space_id"].as_str() == Some(space_id.as_str()),
                    "original plan scope differs"
                );
                plan["target_space_id"]
                    .as_str()
                    .filter(|s| !s.trim().is_empty())
                    .context("explicit target space missing")?
                    .to_owned()
            } else {
                // Older inventory-only plans carry identity mapping. The adoption
                // hook separately requires an explicit source/target declaration.
                space_id.clone()
            };
        if let Some(blockers) = plan.get("runtime_blockers") {
            ensure!(
                blockers.as_array().is_some_and(Vec::is_empty),
                "plan has unresolved runtime blockers"
            );
        }
        let operational_disposition =
            super::archive_disposition::OperationalDisposition::from_plan(&plan)?;
        let tables: BTreeMap<String, u64> = serde_json::from_value(plan["tables"].clone())?;
        ensure!(tables.len() <= 128, "too many table declarations");
        for (table, count) in &tables {
            ensure!(
                super::SUPPORTED_TABLES.contains(&table.as_str())
                    || table == "migration_history"
                    || (LEGACY_EMPTY_TABLES.contains(&table.as_str()) && *count == 0),
                "unsupported spool table {table}"
            );
            ensure!(
                table != "id_map" || *count == 0,
                "id_map has no typed transfer path"
            );
        }
        let db = readonly(&path)?;
        for key in [
            "format",
            "spool_schema",
            "canonical_hash_contract",
            "snapshot_header",
            "snapshot_sha256",
            "plan_id",
            "space_id",
            "tables",
        ] {
            let text: String =
                db.query_row("SELECT v FROM metadata WHERE k=?", [key], |r| r.get(0))?;
            ensure!(
                strict_json(&text)? == plan[key],
                "plan metadata differs: {key}"
            );
        }
        for key in [
            "source_space_id",
            "target_space_id",
            "operational_disposition",
            "operational_archive_inventory",
            "runtime_blockers",
            "runtime_requirements",
            "export_notes",
            "media_preservation",
        ] {
            if plan.get(key).is_some() {
                let text: String =
                    db.query_row("SELECT v FROM metadata WHERE k=?", [key], |r| r.get(0))?;
                ensure!(
                    strict_json(&text)? == plan[key],
                    "plan metadata differs: {key}"
                );
            }
        }
        for key in ["operational_disposition", "operational_archive_inventory"] {
            let stored: Option<String> = db
                .query_row("SELECT v FROM metadata WHERE k=?", [key], |r| r.get(0))
                .optional()?;
            let stored = stored.as_deref().map(strict_json).transpose()?;
            ensure!(
                stored.as_ref() == plan.get(key),
                "archive metadata presence or value differs: {key}"
            );
        }
        if space_id != target_space_id {
            let collisions: u64 = db.query_row(
                "SELECT count(*) FROM rows WHERE space=?",
                [&target_space_id],
                |r| r.get(0),
            )?;
            ensure!(
                collisions == 0,
                "original target-space rows would merge with relocated source"
            );
            // This index contains lexical candidates, never grants authority.
            // A cross-space match is a conservative refusal requiring explicit
            // review, not a reason to infer or rewrite an arbitrary string.
            let cross: bool = db.query_row("SELECT EXISTS(SELECT 1 FROM reference_candidates c JOIN rows owner ON owner.t=c.row_table AND owner.k=c.row_id JOIN rows target ON target.t=c.ref_table AND target.k=c.ref_id WHERE (owner.space=? AND target.space!=?) OR (owner.space!=? AND target.space=?))", params![space_id,space_id,space_id,space_id], |r|r.get(0))?;
            ensure!(
                !cross,
                "space relocation has a cross-space reference candidate"
            );
        }
        // Declared zero tables are evidence too; there may be no undeclared rows.
        let mut actual = tables
            .keys()
            .map(|k| (k.clone(), 0_u64))
            .collect::<BTreeMap<_, _>>();
        let mut query = db.prepare("SELECT t,count(*) FROM rows GROUP BY t ORDER BY t")?;
        for item in query.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?)))? {
            let (table, count) = item?;
            ensure!(actual.contains_key(&table), "undeclared spool table");
            actual.insert(table, count);
        }
        ensure!(actual == tables, "spool table counts differ");
        let missing: u64 = db.query_row("SELECT count(*) FROM rows r LEFT JOIN raw_rows x ON x.t=r.t AND x.k=r.k WHERE x.k IS NULL", [], |r|r.get(0))?;
        let extra: u64 = db.query_row("SELECT count(*) FROM raw_rows x LEFT JOIN rows r ON x.t=r.t AND x.k=r.k WHERE r.k IS NULL", [], |r|r.get(0))?;
        ensure!(missing == 0 && extra == 0, "lexical inventory incomplete");
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        options.open(scratch)?.sync_all()?;
        let mut checks = Connection::open(scratch)?;
        checks.execute_batch("PRAGMA cache_size=-8192; CREATE TABLE embedding_keys(partition TEXT,node TEXT,PRIMARY KEY(partition,node)); CREATE TABLE expected(t TEXT,k TEXT,h TEXT,seq INTEGER,PRIMARY KEY(t,k),UNIQUE(t,seq));")?;
        let mut spool = Self {
            path,
            operational_disposition,
            archive_inventory: plan
                .get("operational_archive_inventory")
                .filter(|value| !value.is_null())
                .cloned()
                .map(serde_json::from_value)
                .transpose()?,
            inventory_sha256,
            source_sha256,
            plan_id,
            space_id,
            target_space_id,
            tables,
            spaces: BTreeSet::new(),
            retained_controls: Vec::new(),
            export_notes: plan
                .get("export_notes")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            media_preservation: plan
                .get("media_preservation")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
            historical_work_counts: BTreeMap::from([
                ("work_item".into(), 0),
                ("work_item_archive".into(), 0),
                ("running".into(), 0),
            ]),
            runtime_requirements: serde_json::from_value(
                plan.get("runtime_requirements")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!([])),
            )?,
        };
        match (spool.operational_disposition, &spool.archive_inventory) {
            (super::archive_disposition::OperationalDisposition::Native, None) => {}
            (super::archive_disposition::OperationalDisposition::ManagedArchiveV1, Some(proof)) => {
                proof.validate(&spool)?;
            }
            _ => anyhow::bail!("archive inventory does not match explicit disposition"),
        }
        let mut archive_census = spool
            .archive_inventory
            .as_ref()
            .map(|_| super::archive_copy::ArchiveCopyCensus::new(&spool));
        let mut inline_media_rows = 0_u64;
        let mut inline_media_bytes = 0_u64;
        for table in spool.tables.keys() {
            let archived = spool.operational_disposition.archives(table);
            if archived {
                archive_census
                    .as_mut()
                    .context("archive census missing")?
                    .begin_table(table)?;
            }
            let mut cursor = None;
            loop {
                let rows = spool.page(table, cursor.as_deref())?;
                if rows.is_empty() {
                    break;
                }
                let tx = checks.transaction()?;
                for row in &rows {
                    let value = strict_json(&row.lexical)?;
                    if table == "work_item" || table == "work_item_archive" {
                        *spool
                            .historical_work_counts
                            .get_mut(table)
                            .context("known work table")? += 1;
                        let status = if table == "work_item" {
                            &value["status"]
                        } else {
                            &value["record"]["status"]
                        };
                        if status == "running" {
                            *spool
                                .historical_work_counts
                                .get_mut("running")
                                .context("running count")? += 1;
                        }
                    }
                    match typed(table, &row.lexical)? {
                        TypedRow::Ordinary(record) => {
                            if let TransferRecord::MediaAsset(media) = &record {
                                ensure!(
                                    media.payload_base64.is_some(),
                                    "external media availability is unverified"
                                );
                                digest(&serde_json::Value::String(media.sha256.clone()))?;
                                inline_media_rows = inline_media_rows
                                    .checked_add(1)
                                    .context("media row count overflow")?;
                                inline_media_bytes = inline_media_bytes
                                    .checked_add(media.size_bytes)
                                    .context("media byte count overflow")?;
                            }
                            if let TransferRecord::Entity(entity) = &record {
                                super::entity_embedding(entity)?;
                            }
                            if let TransferRecord::Event(event) = &record {
                                for link in &event.base.subjects {
                                    let present:u64=db.query_row("SELECT count(*) FROM rows WHERE t='entity' AND k=? AND space=? AND deleted=0",params![link.id().to_string(),event.base.space_id],|r|r.get(0))?;
                                    ensure!(
                                        present == 1,
                                        "missing/wrong-space/deleted event subject"
                                    );
                                }
                            }
                            let hot = match &record {
                                TransferRecord::WorkItemArchive(a) => {
                                    Some(a.original_id.to_string())
                                }
                                TransferRecord::WorkRunArchive(a) => {
                                    Some(a.original_id.to_string())
                                }
                                _ => None,
                            };
                            if let Some(hot) = hot {
                                let hot_table =
                                    hot.split(':').next().context("archive identity")?;
                                let count: u64 = db.query_row(
                                    "SELECT count(*) FROM rows WHERE t=? AND k=?",
                                    params![hot_table, hot],
                                    |r| r.get(0),
                                )?;
                                ensure!(count == 0, "hot/cold source identity collision");
                            }
                        }
                        TypedRow::Embedding(e) => {
                            tx.execute(
                                "INSERT INTO embedding_keys VALUES (?,?)",
                                params![super::embedding_partition(&e)?, e.node.id().to_string()],
                            )?;
                        }
                        TypedRow::Receipt(receipt) => spool.retained_controls.push(receipt),
                        TypedRow::Space(_) => {}
                    }
                    if table != "migration_history" {
                        spool
                            .spaces
                            .insert(value["space_id"].as_str().context("space missing")?.into());
                    }
                }
                tx.commit()?;
                if archived {
                    archive_census
                        .as_mut()
                        .context("archive census missing")?
                        .accepted_page(table, &rows)?;
                }
                cursor = rows.last().map(|r| r.id.clone());
            }
        }
        ensure!(
            spool.retained_controls.len() <= 1,
            "duplicate source maintenance receipt"
        );
        spool.validate_media_preservation(inline_media_rows, inline_media_bytes)?;
        if let Some(census) = archive_census {
            let proof = spool
                .archive_inventory
                .as_ref()
                .context("archive inventory missing")?;
            let totals = census.finish(&proof.tables, &proof.ordered_inventory_sha256)?;
            proof.verify(&totals)?;
        }
        spool.validate_requirements()?;
        checks.close().map_err(|(_, e)| e)?;
        spool.verify_unchanged()?;
        Ok(spool)
    }

    /// The planner validates inline base64 length and SHA in its original pass.
    /// Bind that proof to this exact closed inventory and independently match its
    /// row/byte totals; original payload bytes still undergo exact native copy.
    fn validate_media_preservation(&self, rows: u64, bytes: u64) -> anyhow::Result<()> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Proof {
            rows: u64,
            inline_rows: u64,
            external_rows: u64,
            decoded_inline_bytes: u64,
            declared_external_bytes: u64,
            inline_size_sha256_verified: bool,
            self_contained: bool,
            external_availability_verified: bool,
        }
        if self.media_preservation.is_null() {
            ensure!(rows == 0, "media preservation proof is missing");
            return Ok(());
        }

        let proof: Proof = serde_json::from_value(self.media_preservation.clone())?;
        ensure!(
            proof.rows == rows
                && proof.inline_rows == rows
                && proof.decoded_inline_bytes == bytes
                && proof.external_rows == 0
                && proof.declared_external_bytes == 0
                && proof.inline_size_sha256_verified
                && proof.self_contained
                && !proof.external_availability_verified,
            "media preservation proof differs or external availability is unresolved"
        );
        Ok(())
    }

    fn validate_requirements(&self) -> anyhow::Result<()> {
        if self.operational_disposition
            == super::archive_disposition::OperationalDisposition::ManagedArchiveV1
        {
            ensure!(
                self.runtime_requirements.len() == 1,
                "archive requirement missing, extra or duplicated"
            );
            let requirement = &self.runtime_requirements[0];
            let keys = requirement
                .as_object()
                .context("archive requirement must be an object")?
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            ensure!(
                keys == BTreeSet::from([
                    "kind",
                    "snapshot_sha256",
                    "source_inventory",
                    "historical_work",
                    "disposition",
                    "verified",
                    "must_complete"
                ]),
                "archive requirement shape differs"
            );
            ensure!(
                requirement["kind"] == "managed_operational_archive_v1"
                    && requirement["snapshot_sha256"].as_str() == Some(self.source_sha256.as_str())
                    && requirement["source_inventory"]
                        == serde_json::to_value(
                            self.archive_inventory
                                .as_ref()
                                .context("archive inventory missing")?
                        )?
                    && requirement["historical_work"]
                        == serde_json::to_value(&self.historical_work_counts)?
                    && requirement["disposition"] == "archive_inert"
                    && requirement["verified"] == false
                    && requirement["must_complete"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty()),
                "archive requirement differs from original source cut"
            );
            return Ok(());
        }
        let work = self.historical_work_counts["work_item"]
            + self.historical_work_counts["work_item_archive"];
        ensure!(
            self.runtime_requirements.len() == usize::from(work > 0),
            "historical work requirement missing, extra, or duplicated"
        );
        if let Some(requirement) = self.runtime_requirements.first() {
            let keys = requirement
                .as_object()
                .context("runtime requirement must be an object")?
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>();
            ensure!(
                keys == BTreeSet::from([
                    "kind",
                    "snapshot_sha256",
                    "rows",
                    "original_payload_unchanged",
                    "verified",
                    "must_complete"
                ]),
                "unknown runtime requirement shape"
            );
            ensure!(
                requirement["kind"] == "historical_work_hold_v1"
                    && requirement["snapshot_sha256"].as_str() == Some(self.source_sha256.as_str())
                    && requirement["rows"] == serde_json::to_value(&self.historical_work_counts)?
                    && requirement["original_payload_unchanged"] == true
                    && requirement["verified"] == false
                    && requirement["must_complete"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty()),
                "runtime hold requirement differs from original rows"
            );
        }
        Ok(())
    }

    pub fn verify_unchanged(&self) -> anyhow::Result<()> {
        closed_sqlite(&self.path)?;
        ensure!(
            file_sha(&self.path)? == self.inventory_sha256,
            "immutable spool changed"
        );
        closed_sqlite(&self.path)?;
        Ok(())
    }

    pub fn projected_spaces(&self) -> BTreeSet<String> {
        self.spaces
            .iter()
            .map(|space| {
                if space == &self.space_id {
                    self.target_space_id.clone()
                } else {
                    space.clone()
                }
            })
            .collect()
    }

    pub fn projected(&self, table: &str, raw: &str) -> anyhow::Result<TypedRow> {
        let mut record = typed(table, raw)?;
        match &mut record {
            TypedRow::Ordinary(record) => {
                record.relocate_space(&self.space_id, &self.target_space_id)?;
            }
            TypedRow::Space(record) => {
                if record.base.space_id == self.space_id {
                    record.base.space_id.clone_from(&self.target_space_id);
                }
            }
            TypedRow::Embedding(record) => {
                if record.base.space_id == self.space_id {
                    record.base.space_id.clone_from(&self.target_space_id);
                }
            }
            TypedRow::Receipt(_) => {}
        }
        Ok(record)
    }

    /// Preserve exactly the operational declarations from this source cut.
    /// Absent table declarations remain absent, even for supported tables.
    pub fn archive_tables(&self) -> BTreeMap<String, u64> {
        self.operational_disposition
            .declared_archive_tables(&self.tables)
    }

    /// Keyset page bounded by both raw bytes and rows. A single oversized row
    /// fails before allocation; nothing silently falls back to whole-table scan.
    pub fn page(&self, table: &str, after: Option<&str>) -> anyhow::Result<Vec<RawRow>> {
        let db = readonly(&self.path)?;
        let mut stmt = db.prepare(spool_page_sql(after.is_some()))?;
        let mut rows = if let Some(after) = after {
            stmt.query(params![table, after, MAX_BATCH_ROWS as u64])?
        } else {
            stmt.query(params![table, MAX_BATCH_ROWS as u64])?
        };
        let mut page = Vec::new();
        let mut bytes = 0usize;
        while let Some(row) = rows.next()? {
            let size: usize = row.get(1)?;
            ensure!(
                size <= MAX_ROW_BYTES,
                "single spool row exceeds bounded native transfer size"
            );
            if !page.is_empty() && bytes + size > 8 * 1024 * 1024 {
                break;
            }
            let lexical: Vec<u8> = row.get(2)?;
            ensure!(lexical.len() == size, "lexical length changed");
            let sha: String = row.get(3)?;
            ensure!(
                hex::encode(Sha256::digest(&lexical)) == sha,
                "lexical hash differs"
            );
            let lexical = String::from_utf8(lexical)?;
            let parsed = strict_json(&lexical)?;
            let id: String = row.get(0)?;
            let space: Option<String> = row.get(5)?;
            ensure!(
                parsed["id"].as_str() == Some(id.as_str()),
                "spool row identity differs"
            );
            if table != "migration_history" {
                ensure!(
                    parsed["space_id"].as_str() == space.as_deref()
                        && space.as_deref().is_some_and(|s| !s.trim().is_empty()),
                    "spool row space differs"
                );
            }
            let original_sha256: String = row.get(4)?;
            digest(&serde_json::Value::String(original_sha256.clone()))?;
            page.push(RawRow {
                id,
                lexical,
                original_sha256,
            });
            bytes += size;
        }
        closed_sqlite(&self.path)?;
        Ok(page)
    }
}

pub(super) fn typed(table: &str, raw: &str) -> anyhow::Result<TypedRow> {
    let mut value = strict_json(raw)?;
    match table {
        "migration_history" => {
            super::validate_legacy_purge_receipt(&value)?;
            Ok(TypedRow::Receipt(value))
        }
        "space_config" => {
            let record: elephant_types::records::SpaceConfig =
                serde_json::from_value(value.clone())?;
            validate_original_representation(table, &value, &serde_json::to_value(&record)?)?;
            ensure!(
                record.base.id.table() == elephant_types::TableName::SpaceConfig
                    && !record.base.space_id.is_empty(),
                "space config identity invalid"
            );
            Ok(TypedRow::Space(record))
        }
        "chunk_embedding" => {
            let exact_vector = super::exact_f32_vector(&value["vector"])?;
            let record: elephant_types::records::NodeEmbedding =
                serde_json::from_value(value.clone())?;
            ensure!(
                record
                    .vector
                    .iter()
                    .map(|v| v.to_bits())
                    .eq(exact_vector.iter().map(|v| v.to_bits())),
                "typed embedding changed exact vector"
            );
            value["vector"] = serde_json::to_value(&exact_vector)?;
            validate_original_representation(table, &value, &serde_json::to_value(&record)?)?;
            ensure!(
                record.base.id.table() == elephant_types::TableName::ChunkEmbedding
                    && !record.base.space_id.is_empty(),
                "embedding identity invalid"
            );
            ensure!(
                record.dims > 0
                    && record.vector.len() == usize::try_from(record.dims)?
                    && !record.model.trim().is_empty()
                    && record.vector.iter().all(|x| x.is_finite()),
                "invalid embedding dimensions/model/vector"
            );
            ensure!(
                record
                    .vector
                    .iter()
                    .map(|x| x * x)
                    .sum::<f32>()
                    .sqrt()
                    .is_finite(),
                "embedding norm overflow"
            );
            super::embedding_partition(&record)?;
            Ok(TypedRow::Embedding(record))
        }
        _ => Ok(TypedRow::Ordinary(TransferRecord::from_json(table, raw)?)),
    }
}

#[cfg(test)]
#[path = "source_spool_tests.rs"]
mod tests;
