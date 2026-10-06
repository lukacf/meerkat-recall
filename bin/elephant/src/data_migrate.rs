//! One-time data migration: embedded snapshot -> `SurrealDB` backend.
//!
//! Reads an elephant-db JSON snapshot into an in-memory [`EmbeddedStore`]
//! (complete, including soft-deleted rows, via `scan`) and writes every row
//! through the TYPED repository layer against a surreal-backed
//! [`StorageClient`], so record ids, datetimes, and soft-delete state come
//! out exactly as the production surreal queries expect
//! (`base_fields_to_object` preserves `created_at`/`updated_at`/`deleted_at`
//! verbatim). Chunk embeddings go through `store_embeddings_bulk`, which
//! owns the per-partition DDL. Entity name embeddings are re-attached in a
//! second pass because on surreal they are first-class columns written only
//! by `set_name_embedding`.
//!
//! The target must be EMPTY (fresh rocksdb path or empty database); the tool
//! refuses a non-empty target rather than guessing about merges.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::Args;
use elephant_embeddings::PartitionManager;
use elephant_storage::{
    AssertionRepository, AttributeRepository, AuditLogRepository, ConflictGroupRepository,
    CostLedgerDailyRepository, DocNodeRepository, DocRepository, DocRevisionRepository,
    EmbeddedStore, EntityIdentityKeyRepository, EntityMergeRepository, EntityRepository,
    EventRepository, FreshnessRepository, IdentityCandidateRepository, IngestDecisionRepository,
    MediaAssetRepository, NodeEmbeddingRepository, PredicateAliasRepository,
    PredicateRegistryRepository, RelRepository, Repository, ReprocessJobRepository,
    SourceRepository, SpaceConfigRepository, StorageBackendKind, StorageClient, StorageConfig,
    SystemEventRepository, TruthSlotRepository, WorkArtifactRepository, WorkDeadletterRepository,
    WorkEdgeRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::records::{
    Assertion, Attribute, AuditLog, ConflictGroup, CostLedgerDaily, Doc, DocNode, DocRevision,
    Entity, EntityIdentityKey, EntityMerge, Event, Freshness, IdMap, IdentityCandidate,
    IngestDecision, MediaAsset, NodeEmbedding, PredicateAlias, PredicateRegistry, Rel,
    ReprocessJob, Source, SpaceConfig, SystemEvent, TruthSlot, WorkArtifact, WorkDeadletter,
    WorkEdge, WorkItem, WorkRun,
};

const EMBEDDING_CHUNK: usize = 500;

// Every nonempty source table must have an explicit copy path below. New
// typed tables must fail closed just like opaque extension tables.
const SUPPORTED_TABLES: &[&str] = &[
    "space_config",
    "entity",
    "source",
    "cost_ledger_daily",
    "ingest_decision",
    "doc",
    "doc_revision",
    "doc_node",
    "media_asset",
    "chunk_embedding",
    "entity_identity_key",
    "identity_candidate",
    "entity_merge",
    "predicate_registry",
    "predicate_alias",
    "assertion",
    "attribute",
    "rel",
    "conflict_group",
    "truth_slot",
    "event",
    "freshness",
    "reprocess_job",
    "work_item",
    "work_edge",
    "work_run",
    "work_item_archive",
    "work_run_archive",
    "work_artifact",
    "work_deadletter",
    "system_event",
    "audit_log",
    "id_map",
];

#[derive(Args, Debug, Clone)]
pub struct DataMigrateArgs {
    /// Path to the elephant-db JSON snapshot to migrate.
    #[arg(long)]
    pub snapshot: PathBuf,
    /// Target `SurrealDB` URL (e.g. `rocksdb:///abs/path`). Defaults to `SURREALDB_URL`.
    #[arg(long)]
    pub url: Option<String>,
    /// Target namespace. Defaults to `SURREALDB_NS` or "elephant".
    #[arg(long)]
    pub namespace: Option<String>,
    /// Target database. Defaults to `SURREALDB_DATABASE` or "elephant".
    #[arg(long)]
    pub database: Option<String>,
    /// Write a JSON report to a new file; existing paths are never overwritten.
    #[arg(long)]
    pub report: Option<PathBuf>,
}

/// `SurrealDB`'s serializer refuses NUL bytes in strings; the embedded store
/// tolerated them (live corpus: a source row with an embedded \0 aborted the
/// first migration run). Strip them recursively and count touched rows -
/// NUL in text is corruption, not content.
fn strip_nul(value: &mut serde_json::Value, touched: &mut bool) {
    match value {
        serde_json::Value::String(s) => {
            if s.contains('\0') {
                *s = s.replace('\0', "");
                *touched = true;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                strip_nul(item, touched);
            }
        }
        serde_json::Value::Object(map) => {
            for (_, item) in map.iter_mut() {
                strip_nul(item, touched);
            }
        }
        _ => {}
    }
}

fn sanitize_row<T: serde::Serialize + serde::de::DeserializeOwned>(
    row: T,
    label: &str,
    sanitized: &mut usize,
) -> anyhow::Result<T> {
    let mut value = serde_json::to_value(&row).with_context(|| format!("serialize {label}"))?;
    let mut touched = false;
    strip_nul(&mut value, &mut touched);
    if !touched {
        return Ok(row);
    }
    *sanitized += 1;
    serde_json::from_value(value).with_context(|| format!("reparse sanitized {label}"))
}

macro_rules! copy_table {
    ($src:expr, $dst_repo:expr, $ty:ty, $label:expr, $report:expr) => {{
        let rows: Vec<$ty> = $src
            .scan::<$ty>()
            .await
            .with_context(|| format!("scan {} from snapshot", $label))?;
        let total = rows.len();
        let mut written = 0usize;
        let mut sanitized = 0usize;
        for row in rows {
            let row = sanitize_row(row, $label, &mut sanitized)?;
            $dst_repo
                .create(row)
                .await
                .with_context(|| format!("create {} row {}/{}", $label, written + 1, total))?;
            written += 1;
            if written.is_multiple_of(2000) {
                println!("  {}: {written}/{total}", $label);
            }
        }
        if sanitized > 0 {
            println!("{}: {written} rows ({sanitized} NUL-sanitized)", $label);
        } else {
            println!("{}: {written} rows", $label);
        }
        $report.push(($label, written));
        written
    }};
}

pub async fn run(args: DataMigrateArgs) -> anyhow::Result<()> {
    let url = args
        .url
        .or_else(|| std::env::var("SURREALDB_URL").ok())
        .context("target URL required (--url or SURREALDB_URL)")?;
    let namespace = args
        .namespace
        .or_else(|| std::env::var("SURREALDB_NS").ok())
        .unwrap_or_else(|| "elephant".to_string());
    let database = args
        .database
        .or_else(|| std::env::var("SURREALDB_DATABASE").ok())
        .unwrap_or_else(|| "elephant".to_string());

    println!("Loading snapshot {} ...", args.snapshot.display());
    let src = EmbeddedStore::from_snapshot(&args.snapshot)
        .await
        .context("load snapshot into embedded store")?;

    let dst_config = StorageConfig {
        backend: StorageBackendKind::SurrealDb,
        url: url.clone(),
        namespace,
        database,
        ..StorageConfig::default()
    };
    let dst = StorageClient::new(dst_config.with_env_timeouts()?)
        .await
        .context("connect target surreal backend")?;

    preflight(&src, &dst, args.report.as_deref()).await?;
    copy_snapshot(&src, &dst, args.report.as_deref())
        .await
        .context("migration failed after preflight; the target may contain a partial copy, and the input snapshot is unchanged")
}

async fn preflight(
    src: &EmbeddedStore,
    dst: &StorageClient,
    report: Option<&Path>,
) -> anyhow::Result<()> {
    let unsupported_tables: Vec<String> = src
        .persisted_table_names()
        .await
        .context("enumerate snapshot tables")?
        .into_iter()
        .filter(|table| !SUPPORTED_TABLES.contains(&table.as_str()))
        .collect();
    if !unsupported_tables.is_empty() {
        bail!("unsupported snapshot tables: {unsupported_tables:?}; no target schema or data was written");
    }
    if let Some(path) = report {
        match std::fs::symlink_metadata(path) {
            Ok(_) => bail!(
                "report path already exists: {}; refusing to overwrite it",
                path.display()
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("check report output path"),
        }
    }
    ensure_empty_target(dst).await
}

#[derive(serde::Deserialize)]
struct DatabaseInfo {
    tables: std::collections::BTreeMap<String, String>,
}

async fn ensure_empty_target(dst: &StorageClient) -> anyhow::Result<()> {
    // Inspect every table, including archives and caller-defined tables. An
    // empty SELECT on doc alone says nothing about the rest of the database.
    let mut response = dst
        .query_checked("INFO FOR DB")
        .await
        .context("cannot verify target table metadata")?;
    let mut metadata: Vec<DatabaseInfo> =
        response.take(0).context("invalid target table metadata")?;
    if metadata.len() != 1 {
        bail!("missing or ambiguous target table metadata");
    }
    let info = metadata.pop().context("missing target table metadata")?;
    for table in info.tables.keys() {
        let mut response = dst
            .query_bind_checked(
                "RETURN array::len((SELECT VALUE id FROM type::table($table) LIMIT 1))",
                vec![("table".to_owned(), table.clone())],
            )
            .await
            .with_context(|| format!("cannot verify target table {table}"))?;
        let occupied: Option<u64> = response
            .take(0)
            .with_context(|| format!("invalid occupancy result for target table {table}"))?;
        match occupied {
            Some(0) => {}
            Some(_) => bail!("target table {table} contains records; refusing a non-empty target"),
            None => bail!("missing occupancy result for target table {table}"),
        }
    }
    Ok(())
}

async fn copy_snapshot(
    src: &EmbeddedStore,
    dst: &StorageClient,
    report_path: Option<&Path>,
) -> anyhow::Result<()> {
    println!("Applying target schema ...");
    elephant_migrations::apply_schema(dst)
        .await
        .map_err(|e| anyhow::anyhow!("apply schema: {e}"))?;

    let mut report: Vec<(&str, usize)> = Vec::new();

    // Entity id set for reference repair: events may carry subject links to
    // entities that no longer exist anywhere in the snapshot (dangling links
    // the embedded backend never validated; the surreal EventRepository
    // refuses them). Strip those links rather than dropping the events.
    let known_entities: std::collections::HashSet<String> = src
        .scan::<Entity>()
        .await
        .context("scan entities for reference set")?
        .into_iter()
        .map(|e| e.base.id.to_string())
        .collect();

    // Copy order per the reference graph (schemaless target: ordering is for
    // logical consistency, not constraint satisfaction).
    copy_table!(
        src,
        SpaceConfigRepository::new(dst.clone()),
        SpaceConfig,
        "space_config",
        report
    );
    copy_table!(
        src,
        EntityRepository::new(dst.clone()),
        Entity,
        "entity",
        report
    );
    copy_table!(
        src,
        SourceRepository::new(dst.clone()),
        Source,
        "source",
        report
    );
    copy_table!(
        src,
        CostLedgerDailyRepository::new(dst.clone()),
        CostLedgerDaily,
        "cost_ledger_daily",
        report
    );
    copy_table!(
        src,
        IngestDecisionRepository::new(dst.clone()),
        IngestDecision,
        "ingest_decision",
        report
    );
    copy_table!(src, DocRepository::new(dst.clone()), Doc, "doc", report);
    copy_table!(
        src,
        DocRevisionRepository::new(dst.clone()),
        DocRevision,
        "doc_revision",
        report
    );
    copy_table!(
        src,
        DocNodeRepository::new(dst.clone()),
        DocNode,
        "doc_node",
        report
    );
    copy_table!(
        src,
        MediaAssetRepository::new(dst.clone()),
        MediaAsset,
        "media_asset",
        report
    );

    // Chunk embeddings: bulk path owns partition DDL.
    {
        let rows: Vec<NodeEmbedding> = src
            .scan::<NodeEmbedding>()
            .await
            .context("scan chunk_embedding from snapshot")?;
        let total = rows.len();
        let repo = NodeEmbeddingRepository::new(dst.clone(), PartitionManager::new());
        let mut written = 0usize;
        for chunk in rows.chunks(EMBEDDING_CHUNK) {
            repo.store_embeddings_bulk(chunk.to_vec())
                .await
                .map_err(|e| anyhow::anyhow!("bulk embeddings at {written}/{total}: {e}"))?;
            written += chunk.len();
            if written % 10_000 < EMBEDDING_CHUNK {
                println!("  chunk_embedding: {written}/{total}");
            }
        }
        println!("chunk_embedding: {written} rows");
        report.push(("chunk_embedding", written));
    }

    copy_table!(
        src,
        EntityIdentityKeyRepository::new(dst.clone()),
        EntityIdentityKey,
        "entity_identity_key",
        report
    );
    copy_table!(
        src,
        IdentityCandidateRepository::new(dst.clone()),
        IdentityCandidate,
        "identity_candidate",
        report
    );
    copy_table!(
        src,
        EntityMergeRepository::new(dst.clone()),
        EntityMerge,
        "entity_merge",
        report
    );
    copy_table!(
        src,
        PredicateRegistryRepository::new(dst.clone()),
        PredicateRegistry,
        "predicate_registry",
        report
    );
    copy_table!(
        src,
        PredicateAliasRepository::new(dst.clone()),
        PredicateAlias,
        "predicate_alias",
        report
    );
    copy_table!(
        src,
        AssertionRepository::new(dst.clone()),
        Assertion,
        "assertion",
        report
    );
    copy_table!(
        src,
        AttributeRepository::new(dst.clone()),
        Attribute,
        "attribute",
        report
    );
    copy_table!(src, RelRepository::new(dst.clone()), Rel, "rel", report);
    copy_table!(
        src,
        ConflictGroupRepository::new(dst.clone()),
        ConflictGroup,
        "conflict_group",
        report
    );
    copy_table!(
        src,
        TruthSlotRepository::new(dst.clone()),
        TruthSlot,
        "truth_slot",
        report
    );
    {
        let rows: Vec<Event> = src.scan::<Event>().await.context("scan event")?;
        let total = rows.len();
        let repo = EventRepository::new(dst.clone());
        let mut written = 0usize;
        let mut sanitized = 0usize;
        let mut repaired = 0usize;
        for row in rows {
            let mut row = sanitize_row(row, "event", &mut sanitized)?;
            let before = row.base.subjects.len();
            row.base
                .subjects
                .retain(|link| known_entities.contains(&link.id().to_string()));
            if row.base.subjects.len() != before {
                repaired += 1;
            }
            repo.create(row)
                .await
                .with_context(|| format!("create event row {}/{}", written + 1, total))?;
            written += 1;
            if written.is_multiple_of(2000) {
                println!("  event: {written}/{total}");
            }
        }
        println!("event: {written} rows ({repaired} dangling-subject repairs)");
        report.push(("event", written));
    }
    copy_table!(
        src,
        FreshnessRepository::new(dst.clone()),
        Freshness,
        "freshness",
        report
    );
    copy_table!(
        src,
        ReprocessJobRepository::new(dst.clone()),
        ReprocessJob,
        "reprocess_job",
        report
    );
    copy_table!(
        src,
        WorkItemRepository::new(dst.clone()),
        WorkItem,
        "work_item",
        report
    );
    copy_table!(
        src,
        WorkEdgeRepository::new(dst.clone()),
        WorkEdge,
        "work_edge",
        report
    );
    copy_table!(
        src,
        WorkRunRepository::new(dst.clone()),
        WorkRun,
        "work_run",
        report
    );
    let item_archives = src
        .scan::<elephant_types::records::WorkItemArchive>()
        .await?;
    report.push(("work_item_archive", item_archives.len()));
    for archive in item_archives {
        elephant_storage::work_retention::import_item_archive(&dst, archive).await?;
    }
    let run_archives = src
        .scan::<elephant_types::records::WorkRunArchive>()
        .await?;
    report.push(("work_run_archive", run_archives.len()));
    for archive in run_archives {
        elephant_storage::work_retention::import_run_archive(&dst, archive).await?;
    }
    copy_table!(
        src,
        WorkArtifactRepository::new(dst.clone()),
        WorkArtifact,
        "work_artifact",
        report
    );
    copy_table!(
        src,
        WorkDeadletterRepository::new(dst.clone()),
        WorkDeadletter,
        "work_deadletter",
        report
    );
    copy_table!(
        src,
        SystemEventRepository::new(dst.clone()),
        SystemEvent,
        "system_event",
        report
    );
    copy_table!(
        src,
        AuditLogRepository::new(dst.clone()),
        AuditLog,
        "audit_log",
        report
    );

    // Entity name embeddings: first-class surreal columns, written only by
    // set_name_embedding; the embedded rows carry them in metadata.
    {
        let entities: Vec<Entity> = src.scan::<Entity>().await.context("re-scan entities")?;
        let repo = EntityRepository::new(dst.clone());
        let mut attached = 0usize;
        for entity in &entities {
            let metadata = &entity.metadata;
            let (Some(embedding), Some(model), Some(hash)) = (
                metadata.get("name_embedding").and_then(|v| v.as_array()),
                metadata
                    .get("name_embedding_model")
                    .and_then(|v| v.as_str()),
                metadata
                    .get("name_embedding_text_hash")
                    .and_then(|v| v.as_str()),
            ) else {
                continue;
            };
            // Embeddings are f32 by contract; the snapshot stores JSON numbers.
            #[allow(clippy::cast_possible_truncation)]
            let vector: Vec<f32> = embedding
                .iter()
                .filter_map(|v| v.as_f64().map(|f| f as f32))
                .collect();
            if vector.is_empty() {
                continue;
            }
            repo.set_name_embedding(&entity.base.space_id, &entity.base.id, &vector, model, hash)
                .await
                .map_err(|e| anyhow::anyhow!("set_name_embedding for {}: {e}", entity.base.id))?;
            attached += 1;
        }
        println!("entity name embeddings attached: {attached}");
        report.push(("entity_name_embedding", attached));
    }

    // id_map has no repository; it IS a typed embedded table, so copy it
    // raw into the schemaless target rather than silently dropping it.
    {
        let rows: Vec<IdMap> = src.scan::<IdMap>().await.context("scan id_map")?;
        if !rows.is_empty() {
            println!(
                "id_map: copying {} rows raw (no repository exists)",
                rows.len()
            );
            for row in &rows {
                let id = row
                    .base
                    .id
                    .to_string()
                    .split_once(':')
                    .map_or_else(|| row.base.id.to_string(), |(_, key)| key.to_string());
                dst.query_bind_checked(
                    "CREATE type::thing('id_map', $id) CONTENT $data",
                    vec![
                        ("id".to_string(), serde_json::json!(id)),
                        (
                            "data".to_string(),
                            serde_json::to_value(row).context("serialize id_map row")?,
                        ),
                    ],
                )
                .await
                .map_err(|e| anyhow::anyhow!("raw id_map copy: {e}"))?;
            }
            report.push(("id_map(raw)", rows.len()));
        }
    }
    println!("\n=== MIGRATION REPORT ===");
    for (label, count) in &report {
        println!("{label}: {count}");
    }
    if let Some(path) = report_path {
        let tables: serde_json::Map<String, serde_json::Value> = report
            .iter()
            .map(|(label, count)| ((*label).to_string(), serde_json::json!(count)))
            .collect();
        // create_new also refuses symlink/hard-link aliases of the snapshot
        // and closes the race after the preflight existence check.
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("create report {}", path.display()))?;
        output
            .write_all(
                &serde_json::to_vec_pretty(&serde_json::json!({ "tables": tables }))
                    .context("serialize report")?,
            )
            .with_context(|| format!("write report to {}", path.display()))?;
        println!("report written: {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[tokio::test]
    async fn preflight_checks_every_target_table_without_writing_schema() {
        for table in ["entity", "work_item_archive", "custom_records"] {
            let dst = StorageClient::new_memory().await.unwrap();
            dst.query_checked(&format!("CREATE {table}:existing SET value = 'preserve'"))
                .await
                .unwrap();
            let before = dst.list_tables().await.unwrap();
            let error = preflight(&EmbeddedStore::new(), &dst, None)
                .await
                .unwrap_err();
            assert!(error.to_string().contains(table), "{error:#}");
            assert_eq!(dst.list_tables().await.unwrap(), before);
            let mut response = dst
                .query_checked(&format!("SELECT VALUE value FROM {table}:existing"))
                .await
                .unwrap();
            assert_eq!(response.take::<Vec<String>>(0).unwrap(), vec!["preserve"]);
        }
    }

    #[tokio::test]
    async fn unsupported_extensions_are_rejected_before_target_writes() {
        let src = EmbeddedStore::new();
        src.mutate(|tx| {
            tx.upsert_extension_row(
                "custom_records",
                serde_json::json!({"id":"custom_records:one","space_id":"space","value":"preserve"}),
            )?;
            Ok(())
        })
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("source.json");
        src.export_snapshot(&snapshot).await.unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        let src = EmbeddedStore::from_snapshot(&snapshot).await.unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        assert!(preflight(&src, &dst, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("custom_records"));
        assert!(dst.list_tables().await.unwrap().is_empty());
        assert_eq!(std::fs::read(snapshot).unwrap(), before);
    }

    #[tokio::test]
    async fn embedding_snapshot_accepts_rebuilt_indexes_but_refuses_opaque_rows() {
        use elephant_types::ids::TableName;
        use elephant_types::records::BaseFields;
        use elephant_types::{Link, RecordId};

        let src = EmbeddedStore::new();
        let embedding = NodeEmbedding {
            base: BaseFields::new(
                RecordId::new_v4(TableName::ChunkEmbedding),
                "space".into(),
                "fixture".into(),
            ),
            node: Link::new(RecordId::new_v4(TableName::DocNode)),
            model: "test-model".into(),
            dims: 3,
            vector: vec![1.0, 0.0, 0.0],
        };
        src.mutate(|tx| {
            tx.create(embedding.clone())?;
            Ok(())
        })
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("source.json");
        src.export_snapshot(&snapshot).await.unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        let src = EmbeddedStore::from_snapshot(&snapshot).await.unwrap();
        assert_eq!(
            src.persisted_table_names().await.unwrap(),
            vec!["chunk_embedding"]
        );
        let partition = src
            .table_names()
            .await
            .unwrap()
            .into_iter()
            .find(|name| name != "chunk_embedding")
            .unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        preflight(&src, &dst, None).await.unwrap();
        assert!(dst.list_tables().await.unwrap().is_empty());
        assert_eq!(src.scan::<NodeEmbedding>().await.unwrap(), vec![embedding]);
        assert_eq!(std::fs::read(snapshot).unwrap(), before);

        // A real opaque table with the same spelling as a derived partition
        // must still be rejected; partition name prefixes grant no exemption.
        src.mutate(|tx| {
            tx.upsert_extension_row(
                &partition,
                serde_json::json!({
                    "id": format!("{partition}:one"), "space_id": "space", "value": "preserve"
                }),
            )?;
            Ok(())
        })
        .await
        .unwrap();
        assert!(preflight(&src, &dst, None)
            .await
            .unwrap_err()
            .to_string()
            .contains(&partition));
        assert!(dst.list_tables().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unsupported_typed_tables_are_rejected_before_target_writes() {
        use elephant_types::ids::TableName;
        use elephant_types::records::{BaseFields, SourceWatch, SourceWatchStatus};
        use elephant_types::{Link, RecordId};

        let src = EmbeddedStore::new();
        let watch = SourceWatch {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SourceWatch),
                "space".into(),
                "fixture".into(),
            ),
            source: Link::new(RecordId::new_v4(TableName::Source)),
            requested_uri: "https://example.test/source.txt".into(),
            status: SourceWatchStatus::Enabled,
            interval_seconds: 3600,
            next_check_at: chrono::Utc::now(),
            last_checked_at: None,
            last_changed_at: None,
            etag: None,
            last_modified: None,
            observed_hash: None,
            consecutive_failures: 0,
            last_error: None,
            latest_check: None,
        };
        src.mutate(|tx| {
            tx.create(watch.clone())?;
            Ok(())
        })
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("source.json");
        src.export_snapshot(&snapshot).await.unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        let src = EmbeddedStore::from_snapshot(&snapshot).await.unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        let error = preflight(&src, &dst, None).await.unwrap_err();
        assert!(error.to_string().contains("source_watch"), "{error:#}");
        assert!(dst.list_tables().await.unwrap().is_empty());
        assert_eq!(src.scan::<SourceWatch>().await.unwrap(), vec![watch]);
        assert_eq!(std::fs::read(snapshot).unwrap(), before);
    }

    #[tokio::test]
    async fn unverifiable_target_and_existing_report_are_refused() {
        let src = EmbeddedStore::new();
        let unsupported = StorageClient::new_embedded().await.unwrap();
        assert!(preflight(&src, &unsupported, None).await.is_err());
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("source.json");
        src.export_snapshot(&snapshot).await.unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        assert!(preflight(&src, &dst, Some(&snapshot)).await.is_err());
        assert!(dst.list_tables().await.unwrap().is_empty());
        assert_eq!(std::fs::read(snapshot).unwrap(), before);
    }

    #[tokio::test]
    async fn isolated_empty_target_preserves_entity_identity_and_security() {
        use elephant_types::ids::TableName;
        use elephant_types::records::{BaseFields, EntityType};
        use elephant_types::RecordId;
        let src = EmbeddedStore::new();
        let mut entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".into(),
                "fixture".into(),
            ),
            EntityType::new("person"),
            "Migration fixture".into(),
        );
        entity.base.security.level = 3;
        entity.base.security.labels = vec!["restricted".into()];
        src.mutate(|tx| {
            tx.create(entity.clone())?;
            Ok(())
        })
        .await
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let snapshot = dir.path().join("source.json");
        src.export_snapshot(&snapshot).await.unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        let src = EmbeddedStore::from_snapshot(&snapshot).await.unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        preflight(&src, &dst, None).await.unwrap();
        assert!(dst.list_tables().await.unwrap().is_empty());
        copy_snapshot(&src, &dst, None).await.unwrap();
        let actual = EntityRepository::new(dst)
            .get("space", &entity.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(entity).unwrap()
        );
        assert_eq!(std::fs::read(snapshot).unwrap(), before);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dangling_report_symlink_is_refused_before_target_writes() {
        let dir = tempfile::tempdir().unwrap();
        let report = dir.path().join("report.json");
        let missing = dir.path().join("missing.json");
        std::os::unix::fs::symlink(&missing, &report).unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        let error = preflight(&EmbeddedStore::new(), &dst, Some(&report))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("report path already exists"));
        assert!(dst.list_tables().await.unwrap().is_empty());
        assert_eq!(std::fs::read_link(report).unwrap(), missing);
    }

    #[test]
    fn strip_nul_scrubs_nested_strings_and_reports_touch() {
        let mut value = serde_json::json!({
            "clean": "text",
            "dirty": "a\u{0}b",
            "nested": {"deep": ["x\u{0}", 7, {"more": "\u{0}"}]},
        });
        let mut touched = false;
        strip_nul(&mut value, &mut touched);
        assert!(touched);
        assert_eq!(value["dirty"], "ab");
        assert_eq!(value["nested"]["deep"][0], "x");
        assert_eq!(value["nested"]["deep"][2]["more"], "");

        let mut clean = serde_json::json!({"a": [1, "b"]});
        let mut touched = false;
        strip_nul(&mut clean, &mut touched);
        assert!(!touched);
    }
}
