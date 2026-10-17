//! Trusted local inspection and exact-byte export of managed operational history.
//! Storage resolves current journal custody; CLI input never supplies an archive path.
use anyhow::{ensure, Context};
use clap::{Args, Subcommand};
use elephant_storage::historical_archive_lifecycle::OperationalArchiveReceipt;
use elephant_storage::historical_operational_archive::{ArchiveRow, OPERATIONAL_TABLES};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Subcommand, Debug)]
pub enum MigrationArchiveCommand {
    /// Inspect current managed historical counts, outside ordinary search/work APIs.
    Inspect(ArchiveTarget),
    /// Export a table's exact original bytes to a new private external-copy directory.
    Export(ArchiveExportArgs),
}
#[derive(Args, Debug, Clone)]
pub struct ArchiveTarget {
    #[command(flatten)]
    pub target: crate::data_migrate::TransferTarget,
    /// Original completed migration report, before any journal-authorized redaction.
    #[arg(long)]
    pub report: PathBuf,
    /// Exact SHA256 of the immutable original report.
    #[arg(long)]
    pub report_sha256: String,
}
#[derive(Args, Debug, Clone)]
pub struct ArchiveExportArgs {
    #[command(flatten)]
    pub archive: ArchiveTarget,
    /// One declared table in the fixed operational archive class.
    #[arg(long)]
    pub table: String,
    /// New directory containing rows.bin, rows.jsonl, and completion manifest.json.
    #[arg(long)]
    pub output: PathBuf,
}
struct OriginalReport {
    sha256: String,
    receipt: OperationalArchiveReceipt,
}
fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn report_input(path: &Path, expected: &str) -> anyhow::Result<OriginalReport> {
    ensure!(
        valid_digest(expected),
        "report SHA256 must be lowercase hexadecimal"
    );
    ensure!(
        std::fs::symlink_metadata(path)?.file_type().is_file(),
        "report must be a regular file"
    );
    let file = File::open(path)?;
    ensure!(
        file.metadata()?.len() <= 2 * 1024 * 1024,
        "report exceeds byte bound"
    );
    let mut bytes = Vec::new();
    file.take(2 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() <= 2 * 1024 * 1024,
        "report grew beyond byte bound"
    );
    let sha256 = hex::encode(Sha256::digest(&bytes));
    ensure!(sha256 == expected, "original report hash differs");
    let value = elephant_storage::transfer_batch::strict_json(std::str::from_utf8(&bytes)?)?;
    ensure!(
        elephant_storage::historical_catchup_ledger::valid_baseline_report(&value),
        "completed migration report required"
    );
    ensure!(
        value["operational_disposition"] == "managed_archive_v1"
            && value["catchup_baseline"]["operational_disposition"] == "managed_archive_v1"
            && value["operational_archive"].is_object()
            && value["operational_archive"] == value["catchup_baseline"]["operational_archive"],
        "report archive mode or retained receipt differs"
    );
    let receipt: OperationalArchiveReceipt =
        serde_json::from_value(value["operational_archive"].clone())?;
    receipt.binding.validate()?;
    ensure!(
        value["source_sha256"].as_str() == Some(receipt.binding.snapshot_sha256.as_str())
            && value["inventory_sha256"].as_str() == Some(receipt.binding.spool_sha256.as_str())
            && value["plan_id"].as_str() == Some(receipt.binding.plan_id.as_str())
            && value["source_space_id"].as_str() == Some(receipt.binding.source_space_id.as_str())
            && value["target_space_id"].as_str() == Some(receipt.binding.target_space_id.as_str())
            && value["catchup_baseline"]["journal_identity"].as_str()
                == Some(receipt.binding.journal_identity.as_str()),
        "report source/namespace/journal differs from original archive"
    );
    Ok(OriginalReport { sha256, receipt })
}
fn file_new(path: &Path) -> anyhow::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    Ok(options.open(path)?)
}
fn sync_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}
fn private_new_directory(path: &Path) -> anyhow::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let parent = std::fs::canonicalize(parent)?;
    ensure!(
        std::fs::metadata(&parent)?.is_dir(),
        "export parent is not a directory"
    );
    let name = path.file_name().context("export directory name required")?;
    let output = parent.join(name);
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&output)?;
    sync_directory(&parent)?;
    Ok(output)
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExportIndexRow {
    table: String,
    id: String,
    offset: u64,
    bytes: u64,
    lexical_sha256: String,
}
/// The manifest is the completion marker. An interrupted output remains a
/// clearly incomplete private directory and is never overwritten or resumed.
struct ExportWriter {
    directory: PathBuf,
    data: File,
    index: File,
    data_hash: Sha256,
    index_hash: Sha256,
    table: String,
    expected_rows: u64,
    rows: u64,
    bytes: u64,
    prior: Option<String>,
}
impl ExportWriter {
    fn create(path: &Path, table: &str, expected_rows: u64) -> anyhow::Result<Self> {
        ensure!(
            OPERATIONAL_TABLES.contains(&table),
            "table is outside managed operational class"
        );
        let directory = private_new_directory(path)?;
        let data = file_new(&directory.join("rows.bin"))?;
        let index = file_new(&directory.join("rows.jsonl"))?;
        Ok(Self {
            directory,
            data,
            index,
            data_hash: Sha256::new(),
            index_hash: Sha256::new(),
            table: table.to_owned(),
            expected_rows,
            rows: 0,
            bytes: 0,
            prior: None,
        })
    }
    fn append(&mut self, page: &[ArchiveRow]) -> anyhow::Result<()> {
        ensure!(
            page.len() <= elephant_storage::historical_operational_archive::MAX_PAGE_ROWS,
            "archive export page exceeds row bound"
        );
        let mut page_bytes = 0usize;
        for row in page {
            page_bytes = page_bytes
                .checked_add(row.lexical.len())
                .context("page byte overflow")?;
            ensure!(
                page_bytes <= elephant_storage::historical_operational_archive::MAX_PAGE_BYTES,
                "archive export page exceeds byte bound"
            );
            ensure!(
                self.rows < self.expected_rows
                    && row.table == self.table
                    && row
                        .id
                        .split_once(':')
                        .is_some_and(|(table, id)| table == self.table && !id.is_empty())
                    && self.prior.as_ref().is_none_or(|id| id < &row.id)
                    && row.lexical_sha256 == hex::encode(Sha256::digest(&row.lexical)),
                "archive export row/count/order/hash differs"
            );
            let size = u64::try_from(row.lexical.len())?;
            let next_bytes = self
                .bytes
                .checked_add(size)
                .context("export size overflow")?;
            let index = ExportIndexRow {
                table: row.table.clone(),
                id: row.id.clone(),
                offset: self.bytes,
                bytes: size,
                lexical_sha256: row.lexical_sha256.clone(),
            };
            let mut index_bytes = serde_json::to_vec(&index)?;
            index_bytes.push(b'\n');
            self.data.write_all(&row.lexical)?;
            self.index.write_all(&index_bytes)?;
            self.data_hash.update(&row.lexical);
            self.index_hash.update(&index_bytes);
            self.bytes = next_bytes;
            self.rows += 1;
            self.prior = Some(row.id.clone());
        }
        Ok(())
    }
    fn finish(
        self,
        original: &OriginalReport,
        current: &OperationalArchiveReceipt,
    ) -> anyhow::Result<serde_json::Value> {
        ensure!(
            self.rows == self.expected_rows,
            "archive export population incomplete"
        );
        self.data.sync_all()?;
        self.index.sync_all()?;
        let manifest = serde_json::json!({
            "format":"recall-operational-archive-external-export-v1","state":"complete",
            "custody":"operator_controlled_external_copy","managed_erasure_applies":false,
            "original_report_sha256":original.sha256,"original_archive":original.receipt,
            "resolved_archive":current,"table":self.table,"rows":self.rows,
            "data":{"file":"rows.bin","bytes":self.bytes,"sha256":hex::encode(self.data_hash.finalize()),
                "representation":"concatenated exact original lexical bytes; offsets and lengths in rows.jsonl"},
            "index":{"file":"rows.jsonl","sha256":hex::encode(self.index_hash.finalize())},
            "ordinary_search":false,"work_dispatch":false
        });
        // Publish a complete, already-synced marker without replacing any file.
        // Failed serialization/writes leave only an explicitly pending marker.
        let pending = self.directory.join("manifest.pending.json");
        let mut file = file_new(&pending)?;
        file.write_all(&serde_json::to_vec_pretty(&manifest)?)?;
        file.sync_all()?;
        std::fs::hard_link(&pending, self.directory.join("manifest.json"))?;
        std::fs::remove_file(pending)?;
        sync_directory(&self.directory)?;
        Ok(manifest)
    }
}

fn inspect(
    reader: &elephant_storage::historical_archive_lifecycle::ManagedArchiveRead,
    original: &OriginalReport,
) -> serde_json::Value {
    serde_json::json!({
        "format":"recall-managed-operational-archive-inspection-v1",
        "original_report_sha256":original.sha256,
        "original_archive":reader.original_receipt(),
        "resolved_archive":reader.receipt(),
        "surviving_tables":reader.seal().table_counts,
        "original_table_declarations":reader.binding().original_table_declarations,
        "ordinary_search":false,"work_dispatch":false
    })
}
fn export(
    reader: &elephant_storage::historical_archive_lifecycle::ManagedArchiveRead,
    original: &OriginalReport,
    args: &ArchiveExportArgs,
) -> anyhow::Result<serde_json::Value> {
    let count = *reader
        .seal()
        .table_counts
        .get(&args.table)
        .context("table was not declared in the managed archive")?;
    let mut writer = ExportWriter::create(&args.output, &args.table, count)?;
    let mut cursor = None;
    loop {
        let page = reader.page(
            &args.table,
            cursor.as_deref(),
            elephant_storage::historical_operational_archive::MAX_PAGE_ROWS,
        )?;
        if page.is_empty() {
            break;
        }
        writer.append(&page)?;
        cursor = page.last().map(|row| row.id.clone());
    }
    writer.finish(original, reader.receipt())
}
fn storage_error(error: &anyhow::Error) -> elephant_storage::StorageError {
    elephant_storage::StorageError::DataIntegrityError(format!("archive administration: {error:#}"))
}
pub async fn run(command: MigrationArchiveCommand) -> anyhow::Result<()> {
    let result = run_inner(command, None).await?;
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}
pub async fn run_inner(
    command: MigrationArchiveCommand,
    opened: Option<elephant_storage::StorageClient>,
) -> anyhow::Result<serde_json::Value> {
    let args = match &command {
        MigrationArchiveCommand::Inspect(args) => args,
        MigrationArchiveCommand::Export(args) => &args.archive,
    };
    let original = report_input(&args.report, &args.report_sha256)?;
    let owns_connection = opened.is_none();
    let client = if let Some(client) = opened {
        client
    } else {
        let path = args
            .target
            .url
            .as_deref()
            .context("explicit existing local native target required")?
            .strip_prefix("rocksdb://")
            .context("archive administration requires local native custody")?;
        ensure!(
            Path::new(path).is_absolute() && std::fs::symlink_metadata(path)?.file_type().is_dir(),
            "existing absolute native directory required"
        );
        args.target.clone().connect().await?
    };
    let result = read_with_client(&client, &command, &original).await;
    if owns_connection {
        crate::data_migrate::finish_owned_connection(client, result).await
    } else {
        result
    }
}

async fn read_with_client(
    client: &elephant_storage::StorageClient,
    command: &MigrationArchiveCommand,
    original: &OriginalReport,
) -> anyhow::Result<serde_json::Value> {
    client.verify_journal_identity().await?;
    ensure!(
        client.erasure_journal()?.identity() == original.receipt.binding.journal_identity,
        "original archive journal identity differs"
    );
    elephant_storage::historical_archive_lifecycle::with_operational_archive_read(
        client,
        &original.receipt,
        |reader| match command {
            MigrationArchiveCommand::Inspect(_) => Ok(inspect(reader, original)),
            MigrationArchiveCommand::Export(args) => {
                export(reader, original, args).map_err(|error| storage_error(&error))
            }
        },
    )
    .await
    .map_err(anyhow::Error::from)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_storage::erasure_journal::ArchiveRegistration;
    use elephant_storage::historical_operational_archive::{ArchiveBinding, ArchiveSeal};
    use std::collections::BTreeMap;

    fn original() -> OriginalReport {
        let binding = ArchiveBinding {
            version: 1,
            snapshot_sha256: "a".repeat(64),
            spool_sha256: "b".repeat(64),
            plan_id: "c".repeat(64),
            source_space_id: "default".into(),
            target_space_id: "household".into(),
            journal_identity: "journal".into(),
            disposition_sha256: ArchiveBinding::disposition_digest(),
            parent_generation_sha256: None,
            original_table_declarations: BTreeMap::from([
                ("work_item".into(), 2),
                ("work_edge".into(), 0),
            ]),
        };
        let registration = ArchiveRegistration {
            lineage_id: uuid::Uuid::new_v4().to_string(),
            generation_id: uuid::Uuid::new_v4().to_string(),
            parent_generation_id: None,
            binding_sha256: binding.digest().unwrap(),
            snapshot_sha256: binding.snapshot_sha256.clone(),
            journal_id: binding.journal_identity.clone(),
            source_space_id: binding.source_space_id.clone(),
            target_space_id: binding.target_space_id.clone(),
        };
        let seal = ArchiveSeal {
            binding_sha256: binding.digest().unwrap(),
            file_sha256: "d".repeat(64),
            ordered_inventory_sha256: "e".repeat(64),
            bytes: 4096,
            table_counts: binding.archived_tables(),
            event_tails: BTreeMap::new(),
            changes_sha256: "f".repeat(64),
            redacted: false,
        };
        OriginalReport {
            sha256: "9".repeat(64),
            receipt: OperationalArchiveReceipt {
                registration,
                binding,
                seal,
                native_receipt_sha256: "8".repeat(64),
            },
        }
    }
    fn row(id: &str, lexical: &[u8]) -> ArchiveRow {
        ArchiveRow {
            table: "work_item".into(),
            id: format!("work_item:{id}"),
            lexical: lexical.to_vec(),
            lexical_sha256: hex::encode(Sha256::digest(lexical)),
        }
    }
    #[test]
    fn external_export_preserves_multiline_and_trailing_bytes_with_reconstruction_index() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("copy");
        let original = original();
        let rows = [
            row("a", b"{\n  \"id\": \"work_item:a\", \"raw\": [1, 2]\n}\n"),
            row("b", b"{\"id\":\"work_item:b\"} \r\n"),
        ];
        let mut writer = ExportWriter::create(&output, "work_item", 2).unwrap();
        writer.append(&rows[..1]).unwrap();
        writer.append(&rows[1..]).unwrap();
        let manifest = writer.finish(&original, &original.receipt).unwrap();
        let bytes = std::fs::read(output.join("rows.bin")).unwrap();
        let index = std::fs::read(output.join("rows.jsonl")).unwrap();
        let parsed: Vec<ExportIndexRow> = std::str::from_utf8(&index)
            .unwrap()
            .lines()
            .map(|s| serde_json::from_str(s).unwrap())
            .collect();
        assert_eq!(
            bytes,
            rows.iter()
                .flat_map(|r| r.lexical.iter().copied())
                .collect::<Vec<_>>()
        );
        for (row, entry) in rows.iter().zip(parsed) {
            let offset = usize::try_from(entry.offset).unwrap();
            let end = offset + usize::try_from(entry.bytes).unwrap();
            assert_eq!(&bytes[offset..end], row.lexical);
            assert_eq!(entry.id, row.id);
            assert_eq!(entry.lexical_sha256, row.lexical_sha256);
        }
        assert_eq!(
            manifest["data"]["sha256"],
            hex::encode(Sha256::digest(&bytes))
        );
        assert_eq!(
            manifest["index"]["sha256"],
            hex::encode(Sha256::digest(&index))
        );
        assert_eq!(manifest["managed_erasure_applies"], false);
        assert_eq!(manifest["custody"], "operator_controlled_external_copy");
        assert!(ExportWriter::create(&output, "work_item", 2).is_err());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&output).unwrap().permissions().mode() & 0o777,
                0o700
            );
            for name in ["rows.bin", "rows.jsonl", "manifest.json"] {
                assert_eq!(
                    std::fs::metadata(output.join(name))
                        .unwrap()
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
    }
    #[test]
    fn incomplete_invalid_or_extra_export_never_publishes_completion() {
        let original = original();
        for kind in ["missing", "hash", "table", "order", "extra", "page"] {
            let root = tempfile::tempdir().unwrap();
            let output = root.path().join("copy");
            let mut writer = ExportWriter::create(&output, "work_item", 2).unwrap();
            let mut rows = vec![row("a", b"{}"), row("b", b"{}")];
            match kind {
                "missing" => {
                    rows.pop();
                }
                "hash" => rows[1].lexical.push(b' '),
                "table" => rows[1].table = "system_event".into(),
                "order" => rows[1] = rows[0].clone(),
                "extra" => rows.push(row("c", b"{}")),
                "page" => rows = vec![row("a", b"{}"); 257],
                _ => unreachable!(),
            }
            let result = writer
                .append(&rows)
                .and_then(|()| writer.finish(&original, &original.receipt));
            assert!(result.is_err(), "{kind}");
            assert!(!output.join("manifest.json").exists(), "{kind}");
        }
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("copy");
        assert!(ExportWriter::create(&output, "doc", 0).is_err());
        assert!(!output.exists());
        let empty = ExportWriter::create(&output, "work_edge", 0)
            .unwrap()
            .finish(&original, &original.receipt)
            .unwrap();
        assert_eq!(empty["rows"], 0);
        assert_eq!(empty["data"]["sha256"], hex::encode(Sha256::digest([])));
    }
    #[test]
    fn completion_marker_never_overwrites_an_existing_path() {
        let root = tempfile::tempdir().unwrap();
        let output = root.path().join("copy");
        let original = original();
        let writer = ExportWriter::create(&output, "work_edge", 0).unwrap();
        std::fs::write(output.join("manifest.json"), b"existing").unwrap();
        assert!(writer.finish(&original, &original.receipt).is_err());
        assert_eq!(
            std::fs::read(output.join("manifest.json")).unwrap(),
            b"existing"
        );
        assert!(output.join("manifest.pending.json").exists());
    }
    fn report_value() -> serde_json::Value {
        let original = original();
        let binding = &original.receipt.binding;
        serde_json::json!({"format":"recall-legacy-transfer-v2","state":"initial_history_reconciled_vector_coverage_pending",
            "source_sha256":binding.snapshot_sha256,"inventory_sha256":binding.spool_sha256,"plan_id":binding.plan_id,
            "source_space_id":binding.source_space_id,"target_space_id":binding.target_space_id,
            "operational_disposition":"managed_archive_v1","operational_archive":original.receipt,
            "catchup_baseline":{"journal_identity":binding.journal_identity,"operational_disposition":"managed_archive_v1","operational_archive":original.receipt}})
    }
    #[test]
    fn report_input_requires_exact_complete_original_binding_and_rejects_duplicates() {
        for kind in [
            "valid",
            "hash",
            "mode",
            "receipt",
            "journal",
            "namespace",
            "plan",
            "inventory",
            "pending",
            "duplicate",
        ] {
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("report.json");
            let mut value = report_value();
            match kind {
                "mode" => value["operational_disposition"] = "native".into(),
                "receipt" => {
                    value["catchup_baseline"]["operational_archive"]["native_receipt_sha256"] =
                        "0".repeat(64).into();
                }
                "journal" => value["catchup_baseline"]["journal_identity"] = "other".into(),
                "namespace" => value["source_space_id"] = "other".into(),
                "plan" => value["plan_id"] = "0".repeat(64).into(),
                "inventory" => value["inventory_sha256"] = "0".repeat(64).into(),
                "pending" => value["state"] = "pending".into(),
                _ => {}
            }
            let mut bytes = serde_json::to_vec_pretty(&value).unwrap();
            if kind == "duplicate" {
                bytes.splice(1..1, b"\"format\":\"duplicate\",".iter().copied());
            }
            std::fs::write(&path, &bytes).unwrap();
            let hash = if kind == "hash" {
                "0".repeat(64)
            } else {
                hex::encode(Sha256::digest(&bytes))
            };
            assert_eq!(
                report_input(&path, &hash).is_ok(),
                kind == "valid",
                "{kind}"
            );
        }
    }
}
