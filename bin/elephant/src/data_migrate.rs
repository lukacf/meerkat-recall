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

use std::path::PathBuf;

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
    /// Write a machine-readable JSON report of per-table row counts here.
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

    // Refuse a non-empty target: this is a one-time migration, not a merge.
    let existing = dst
        .query_checked("SELECT count() FROM doc GROUP ALL")
        .await
        .ok()
        .and_then(|mut r| r.take::<Option<serde_json::Value>>(0).ok().flatten());
    if let Some(value) = existing {
        let count = value.get("count").and_then(serde_json::Value::as_u64);
        if count.unwrap_or(0) > 0 {
            bail!("target already contains documents; refusing to migrate into a non-empty store");
        }
    }

    println!("Applying schema to {url} ...");
    elephant_migrations::apply_schema(&dst)
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
    // Extension tables have NO surreal surface at all: enumerate loudly.
    let extension_tables = src
        .mutate(|tx| Ok(tx.extension_table_names()))
        .await
        .unwrap_or_default();
    if !extension_tables.is_empty() {
        println!(
            "WARNING: snapshot carries extension tables with NO surreal migration path: {extension_tables:?}"
        );
    }

    println!("\n=== MIGRATION REPORT ===");
    for (label, count) in &report {
        println!("{label}: {count}");
    }
    if let Some(path) = &args.report {
        let tables: serde_json::Map<String, serde_json::Value> = report
            .iter()
            .map(|(label, count)| ((*label).to_string(), serde_json::json!(count)))
            .collect();
        std::fs::write(
            path,
            serde_json::to_vec_pretty(&serde_json::json!({ "tables": tables }))
                .context("serialize report")?,
        )
        .with_context(|| format!("write report to {}", path.display()))?;
        println!("report written: {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::strip_nul;

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
