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

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context};
use clap::Args;
use elephant_embeddings::{level_bucket, PartitionKey, PartitionManager};
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
    #[command(flatten)]
    pub target: TransferTarget,
    /// Write a JSON report to a new file; existing paths are never overwritten.
    #[arg(long)]
    pub report: Option<PathBuf>,
}

/// Destructive recovery is deliberately a separate explicit command.
#[derive(Args, Debug, Clone)]
pub struct DataMigrateDiscardArgs {
    #[command(flatten)]
    pub target: TransferTarget,
    /// SHA256 of the original transfer input; must match the durable pending entry.
    #[arg(long)]
    pub source_sha256: String,
    /// Explicitly authorize erasing the partial destination, never the input snapshot.
    #[arg(long, required = true)]
    pub discard_target: bool,
}

#[derive(Args, Debug, Clone)]
pub struct TransferTarget {
    /// Target `SurrealDB` URL (e.g. `rocksdb:///abs/path`). Defaults to `SURREALDB_URL`.
    #[arg(long)]
    pub url: Option<String>,
    /// Target namespace. Defaults to `SURREALDB_NAMESPACE`, legacy `SURREALDB_NS`, then "elephant".
    #[arg(long)]
    pub namespace: Option<String>,
    /// Target database. Defaults to `SURREALDB_DATABASE` or "elephant".
    #[arg(long)]
    pub database: Option<String>,
}

impl TransferTarget {
    async fn connect(self) -> anyhow::Result<StorageClient> {
        let config = StorageConfig {
            backend: StorageBackendKind::SurrealDb,
            url: self
                .url
                .or_else(|| std::env::var("SURREALDB_URL").ok())
                .context("target URL required (--url or SURREALDB_URL)")?,
            username: std::env::var("SURREALDB_USER").unwrap_or_else(|_| "root".into()),
            password: std::env::var("SURREALDB_PASS").unwrap_or_else(|_| "root".into()),
            namespace: self
                .namespace
                .or_else(|| std::env::var("SURREALDB_NAMESPACE").ok())
                .or_else(|| std::env::var("SURREALDB_NS").ok())
                .unwrap_or_else(|| "elephant".into()),
            database: self
                .database
                .or_else(|| std::env::var("SURREALDB_DATABASE").ok())
                .unwrap_or_else(|| "elephant".into()),
            ..StorageConfig::default()
        }
        .with_env_timeouts()?;
        StorageClient::new(config)
            .await
            .context("connect explicit target backend")
    }
}

pub async fn discard(args: DataMigrateDiscardArgs) -> anyhow::Result<()> {
    anyhow::ensure!(args.discard_target, "--discard-target is required");
    anyhow::ensure!(
        args.source_sha256.len() == 64
            && args
                .source_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()),
        "--source-sha256 must be the 64-hex original input digest"
    );
    args.target
        .connect()
        .await?
        .discard_incomplete_transfer(&args.source_sha256.to_ascii_lowercase())
        .await
        .context("target was not discarded; incomplete transfer identity must match")?;
    println!("incomplete target discarded; source snapshot untouched");
    Ok(())
}

/// Refuse transformations of immutable text, identifiers, metadata or evidence.
fn contains_nul(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::String(value) => value.contains('\0'),
        serde_json::Value::Array(items) => items.iter().any(contains_nul),
        serde_json::Value::Object(items) => items
            .iter()
            .any(|(key, value)| key.contains('\0') || contains_nul(value)),
        _ => false,
    }
}

fn validate_row<T: serde::Serialize>(row: &T, label: &str) -> anyhow::Result<()> {
    let value = serde_json::to_value(row).with_context(|| format!("serialize {label}"))?;
    anyhow::ensure!(
        !contains_nul(&value),
        "{label} contains NUL; refusing an immutable-content transformation"
    );
    Ok(())
}

fn entity_embedding(entity: &Entity) -> anyhow::Result<Option<(Vec<f32>, String, String)>> {
    let fields = [
        "name_embedding",
        "name_embedding_model",
        "name_embedding_text_hash",
    ];
    if fields
        .iter()
        .all(|field| entity.metadata.get(field).is_none())
    {
        anyhow::ensure!(
            entity.metadata.get("name_embedding_dims").is_none(),
            "entity has dimensions without a vector"
        );
        return Ok(None);
    }
    let array = entity
        .metadata
        .get(fields[0])
        .and_then(serde_json::Value::as_array)
        .context("entity name embedding must have vector, model and text hash together")?;
    let model = entity
        .metadata
        .get(fields[1])
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .context("entity name embedding model missing")?;
    let hash = entity
        .metadata
        .get(fields[2])
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .context("entity name embedding text hash missing")?;
    anyhow::ensure!(!array.is_empty(), "entity name embedding vector is empty");
    let mut vector = Vec::with_capacity(array.len());
    for value in array {
        let number = value
            .as_f64()
            .context("entity name embedding contains a nonnumeric element")?;
        anyhow::ensure!(
            number.is_finite() && number.abs() <= f64::from(f32::MAX),
            "entity name embedding is not a finite f32 vector"
        );
        #[allow(clippy::cast_possible_truncation)]
        vector.push(number as f32);
    }
    if let Some(dimensions) = entity.metadata.get("name_embedding_dims") {
        anyhow::ensure!(
            dimensions.as_u64() == Some(u64::try_from(vector.len())?),
            "entity name embedding dimensions disagree with vector length"
        );
    }
    Ok(Some((vector, model.to_owned(), hash.to_owned())))
}

async fn validate_typed_table<T: elephant_storage::embedded::EmbeddedRecord>(
    src: &EmbeddedStore,
    tables: &BTreeMap<String, Vec<serde_json::Value>>,
) -> anyhow::Result<()> {
    let Some(raw) = tables.get(T::TABLE.as_str()) else {
        return Ok(());
    };
    let rows = src.scan::<T>().await?;
    anyhow::ensure!(rows.len() == raw.len(), "typed source inventory differs");
    let by_id: BTreeMap<_, _> = rows
        .iter()
        .map(|row| Ok((row.base().id.to_string(), serde_json::to_value(row)?)))
        .collect::<anyhow::Result<_>>()?;
    for row in raw {
        let id = row
            .get("id")
            .and_then(serde_json::Value::as_str)
            .context("source identity missing")?;
        let mut original = row.clone();
        // The kernel's optional compact vector spelling roundtrips through f32.
        // Only that documented numeric representation mapping is permitted.
        if T::TABLE == elephant_types::TableName::ChunkEmbedding {
            let vector: Vec<f32> =
                serde_json::from_value(original.get("vector").cloned().context("vector missing")?)?;
            original["vector"] = serde_json::to_value(vector)?;
        }
        anyhow::ensure!(
            by_id.get(id) == Some(&original),
            "source row {id} would lose fields or change during typed conversion; transfer refused"
        );
    }
    Ok(())
}

fn embedding_partition(source: &NodeEmbedding) -> anyhow::Result<String> {
    Ok(PartitionKey::new(
        source.base.security.policy_id.clone(),
        level_bucket(source.base.security.level),
        source.model.clone(),
        source.vector.len(),
    )?
    .table_name())
}

/// Inspect every persisted row before opening the target mutation window.
async fn validate_source(src: &EmbeddedStore) -> anyhow::Result<()> {
    let raw: serde_json::Value = serde_json::from_slice(&src.export_snapshot_bytes().await?)?;
    anyhow::ensure!(
        !contains_nul(&raw),
        "snapshot contains NUL; no target writes attempted"
    );
    let tables = source_inventory(src).await?;
    validate_typed_table::<SpaceConfig>(src, &tables).await?;
    validate_typed_table::<Entity>(src, &tables).await?;
    validate_typed_table::<Source>(src, &tables).await?;
    validate_typed_table::<CostLedgerDaily>(src, &tables).await?;
    validate_typed_table::<IngestDecision>(src, &tables).await?;
    validate_typed_table::<Doc>(src, &tables).await?;
    validate_typed_table::<DocRevision>(src, &tables).await?;
    validate_typed_table::<DocNode>(src, &tables).await?;
    validate_typed_table::<MediaAsset>(src, &tables).await?;
    validate_typed_table::<NodeEmbedding>(src, &tables).await?;
    validate_typed_table::<EntityIdentityKey>(src, &tables).await?;
    validate_typed_table::<IdentityCandidate>(src, &tables).await?;
    validate_typed_table::<EntityMerge>(src, &tables).await?;
    validate_typed_table::<PredicateRegistry>(src, &tables).await?;
    validate_typed_table::<PredicateAlias>(src, &tables).await?;
    validate_typed_table::<Assertion>(src, &tables).await?;
    validate_typed_table::<Attribute>(src, &tables).await?;
    validate_typed_table::<Rel>(src, &tables).await?;
    validate_typed_table::<ConflictGroup>(src, &tables).await?;
    validate_typed_table::<TruthSlot>(src, &tables).await?;
    validate_typed_table::<Event>(src, &tables).await?;
    validate_typed_table::<Freshness>(src, &tables).await?;
    validate_typed_table::<ReprocessJob>(src, &tables).await?;
    validate_typed_table::<WorkItem>(src, &tables).await?;
    validate_typed_table::<WorkEdge>(src, &tables).await?;
    validate_typed_table::<WorkRun>(src, &tables).await?;
    validate_typed_table::<WorkArtifact>(src, &tables).await?;
    validate_typed_table::<WorkDeadletter>(src, &tables).await?;
    validate_typed_table::<SystemEvent>(src, &tables).await?;
    validate_typed_table::<AuditLog>(src, &tables).await?;
    validate_typed_table::<IdMap>(src, &tables).await?;
    validate_typed_table::<elephant_types::records::WorkItemArchive>(src, &tables).await?;
    validate_typed_table::<elephant_types::records::WorkRunArchive>(src, &tables).await?;
    let entities = src.scan::<Entity>().await?;
    let known: std::collections::HashSet<_> = entities
        .iter()
        .map(|entity| (entity.base.space_id.clone(), entity.base.id.clone()))
        .collect();
    for entity in entities {
        entity_embedding(&entity)?;
    }
    for event in src.scan::<Event>().await? {
        anyhow::ensure!(
            event
                .base
                .subjects
                .iter()
                .all(|link| known.contains(&(event.base.space_id.clone(), link.id().clone()))),
            "event {} has missing or wrong-space subjects; no links are silently removed",
            event.base.id
        );
    }
    for archive in src
        .scan::<elephant_types::records::WorkItemArchive>()
        .await?
    {
        let mut base = archive.record.base.clone();
        base.id = archive.base.id.clone();
        anyhow::ensure!(
            archive.base == base
                && archive.original_id == archive.record.base.id
                && archive.base.id.id() == archive.original_id.id(),
            "archived item has noncanonical identity/envelope; transfer refused"
        );
    }
    for archive in src
        .scan::<elephant_types::records::WorkRunArchive>()
        .await?
    {
        let mut base = archive.record.base.clone();
        base.id = archive.base.id.clone();
        anyhow::ensure!(
            archive.base == base
                && archive.original_id == archive.record.base.id
                && archive.base.id.id() == archive.original_id.id(),
            "archived run has noncanonical identity/envelope; transfer refused"
        );
    }
    anyhow::ensure!(
        src.scan::<IdMap>().await?.is_empty(),
        "id_map has no typed preservation/readback path; transfer refused before mutation"
    );
    let mut embedding_keys = std::collections::HashSet::new();
    for embedding in src.scan::<NodeEmbedding>().await? {
        anyhow::ensure!(
            embedding.dims > 0
                && usize::try_from(embedding.dims)? == embedding.vector.len()
                && !embedding.model.trim().is_empty()
                && embedding.vector.iter().all(|value| value.is_finite()),
            "embedding {} has malformed dimensions/model/vector",
            embedding.base.id
        );
        let norm = embedding
            .vector
            .iter()
            .map(|value| value * value)
            .sum::<f32>()
            .sqrt();
        anyhow::ensure!(
            norm.is_finite(),
            "embedding norm overflows target representation"
        );
        anyhow::ensure!(
            embedding_keys.insert((
                embedding_partition(&embedding)?,
                embedding.node.id().clone()
            )),
            "multiple embedding rows map to one destination partition/node; transfer refused"
        );
    }
    Ok(())
}

fn validate_transfer_input(path: &Path) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(!path.is_dir(), "preview named snapshots require journal-aware snapshot restore; backend transfer is unsupported");
    let bytes = std::fs::read(path)?;
    let header: serde_json::Value = serde_json::from_slice(&bytes)?;
    anyhow::ensure!(header.get("format").is_none(), "preview live/named snapshot envelopes require the original journal and snapshot restore; backend transfer refused before target mutation");
    Ok(bytes)
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct RowFingerprint {
    id: String,
    sha256: String,
}

async fn target_fingerprints(
    dst: &StorageClient,
    table: &str,
) -> anyhow::Result<Vec<RowFingerprint>> {
    let mut response = dst.query_bind_checked(
        "SELECT type::string(id) AS id, crypto::sha256(type::string($this)) AS sha256 FROM type::table($table) ORDER BY id",
        BTreeMap::from([("table".to_owned(), table.to_owned())]),
    ).await?;
    let mut rows: Vec<RowFingerprint> = response.take(0)?;
    rows.sort_by(|left, right| left.id.cmp(&right.id));
    Ok(rows)
}

async fn source_inventory(
    src: &EmbeddedStore,
) -> anyhow::Result<BTreeMap<String, Vec<serde_json::Value>>> {
    let raw: serde_json::Value = serde_json::from_slice(&src.export_snapshot_bytes().await?)?;
    let mut tables: BTreeMap<String, Vec<serde_json::Value>> = serde_json::from_value(
        raw.get("tables")
            .cloned()
            .context("snapshot table inventory missing")?,
    )?;
    for rows in tables.values_mut() {
        rows.sort_by_key(|row| {
            row.get("id")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("")
                .to_owned()
        });
    }
    Ok(tables)
}

/// Native write events advance these destination-owned control rows while
/// copying historical payload. Validate their identity and source-space range
/// before sealing their final fingerprints; never copy source control authority.
#[expect(
    clippy::checked_conversions,
    clippy::items_after_statements,
    reason = "retain destination control validation order and its exact persisted revision bound"
)]
async fn seal_generated_transfer_controls(
    src: &EmbeddedStore,
    dst: &StorageClient,
    fingerprints: &mut BTreeMap<String, Vec<RowFingerprint>>,
) -> anyhow::Result<()> {
    let spaces: std::collections::BTreeSet<String> = source_inventory(src)
        .await?
        .values()
        .flatten()
        .filter_map(|row| row.get("space_id").and_then(serde_json::Value::as_str))
        .map(str::to_owned)
        .collect();
    dst.verify_journal_identity().await?;
    #[derive(serde::Deserialize)]
    #[expect(
        clippy::struct_field_names,
        reason = "retain the native epoch projection field names at the storage boundary"
    )]
    struct Epoch {
        space_id: String,
        epoch: String,
        id_matches_space: bool,
    }
    let mut response = dst
        .query_checked("SELECT space_id, epoch, id = type::thing('canonical_epoch', space_id) AS id_matches_space FROM canonical_epoch")
        .await?;
    let epochs: Vec<Epoch> = response.take(0)?;
    let mut seen = std::collections::BTreeSet::new();
    for row in epochs {
        anyhow::ensure!(
            spaces.contains(&row.space_id)
                && seen.insert(row.space_id)
                && row.id_matches_space
                && uuid::Uuid::parse_str(&row.epoch).is_ok(),
            "unexpected destination canonical epoch during transfer"
        );
    }
    #[derive(serde::Deserialize)]
    struct Guard {
        space_id: String,
        journal_id: String,
        key_fingerprint: String,
        version: u32,
        membership_count: u64,
        revision: u64,
        reserved: bool,
        id_matches_space: bool,
    }
    let mut response = dst.query_checked("SELECT space_id, journal_id, key_fingerprint, version, membership_count, revision, erasure_reservation != NONE AS reserved, id = type::thing('source_erasure_guard', space_id) AS id_matches_space FROM source_erasure_guard").await?;
    let guards: Vec<Guard> = response.take(0)?;
    seen.clear();
    let journal = dst.erasure_journal()?;
    for row in guards {
        anyhow::ensure!(
            spaces.contains(&row.space_id)
                && seen.insert(row.space_id)
                && row.id_matches_space
                && row.journal_id == journal.identity()
                && row.key_fingerprint == journal.reference_key_fingerprint()
                && row.version == 1
                && row.membership_count == 0
                && row.revision > 0
                && row.revision <= i64::MAX as u64
                && !row.reserved,
            "unexpected destination erasure guard during transfer"
        );
    }
    let mut generated = BTreeMap::new();
    for table in ["canonical_epoch", "source_erasure_guard"] {
        anyhow::ensure!(
            fingerprints.contains_key(table),
            "generated transfer control table was absent from initialized schema"
        );
        generated.insert(table.into(), target_fingerprints(dst, table).await?);
    }
    fingerprints.extend(generated);
    Ok(())
}

async fn reconcile_destination(
    src: &EmbeddedStore,
    dst: &StorageClient,
    expected: &BTreeMap<String, Vec<RowFingerprint>>,
) -> anyhow::Result<()> {
    for (table, fingerprints) in expected {
        anyhow::ensure!(
            target_fingerprints(dst, table).await? == *fingerprints,
            "destination rows changed after copy: {table}"
        );
    }
    for table in dst.list_tables().await? {
        anyhow::ensure!(
            expected.contains_key(&table),
            "unexpected destination table after transfer: {table}"
        );
    }
    for (table, rows) in source_inventory(src).await? {
        if table == "chunk_embedding" {
            continue;
        } // Independently checked using the explicit partition/node mapping.
        let mut expected_ids: Vec<String> = rows
            .iter()
            .map(|row| {
                row.get("id")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .context("source row has no identity")
            })
            .collect::<anyhow::Result<_>>()?;
        let mut response = dst
            .query_bind_checked(
                "SELECT VALUE id FROM type::table($table)",
                BTreeMap::from([("table".to_owned(), table.clone())]),
            )
            .await?;
        let actual: Vec<elephant_types::RecordId> = response.take(0)?;
        let mut actual_ids: Vec<String> = actual.into_iter().map(|id| id.to_string()).collect();
        expected_ids.sort();
        actual_ids.sort();
        anyhow::ensure!(
            actual_ids == expected_ids,
            "destination inventory differs for {table}"
        );
    }
    Ok(())
}

#[derive(serde::Deserialize)]
struct ArchiveMetadata {
    original_id: elephant_types::RecordId,
    archived_at: chrono::DateTime<chrono::Utc>,
}
async fn verify_archive_metadata(
    dst: &StorageClient,
    id: &elephant_types::RecordId,
    original: &elephant_types::RecordId,
    archived_at: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    let mut response = dst
        .query_bind_checked(
            "SELECT original_id, archived_at FROM ONLY $id",
            BTreeMap::<String, elephant_storage::canonical_extension::NativeExtensionValue>::from(
                [(
                    "id".to_owned(),
                    elephant_storage::surreal::thing_from_record_id(id).into(),
                )],
            ),
        )
        .await?;
    let actual: Option<ArchiveMetadata> = response.take(0)?;
    let actual = actual.context("archive metadata missing")?;
    anyhow::ensure!(
        actual.original_id == *original && actual.archived_at == archived_at,
        "archive identity/time changed"
    );
    Ok(())
}

#[derive(serde::Deserialize)]
#[expect(
    clippy::struct_field_names,
    reason = "retain the exact native embedding projection field names"
)]
struct EntityVector {
    name_embedding: Vec<f32>,
    name_embedding_model: String,
    name_embedding_text_hash: String,
}
async fn attach_entity_embedding(
    dst: &StorageClient,
    entity: &Entity,
    vector: Vec<f32>,
    model: String,
    hash: String,
) -> anyhow::Result<()> {
    dst.query_bind_checked(
        "UPDATE $id SET name_embedding=$vector, name_embedding_model=$model, name_embedding_text_hash=$hash WHERE space_id=$space",
        BTreeMap::<String, elephant_storage::canonical_extension::NativeExtensionValue>::from([("id".to_owned(), elephant_storage::surreal::thing_from_record_id(&entity.base.id).into()), ("space".to_owned(), entity.base.space_id.clone().into()), ("vector".to_owned(), vector.clone().into()), ("model".to_owned(), model.clone().into()), ("hash".to_owned(), hash.clone().into())]),
    ).await?;
    let mut response = dst.query_bind_checked("SELECT name_embedding, name_embedding_model, name_embedding_text_hash FROM $id WHERE space_id=$space", BTreeMap::<String, elephant_storage::canonical_extension::NativeExtensionValue>::from([("id".to_owned(), elephant_storage::surreal::thing_from_record_id(&entity.base.id).into()), ("space".to_owned(), entity.base.space_id.clone().into())])).await?;
    let actual: Vec<EntityVector> = response.take(0)?;
    anyhow::ensure!(
        actual.len() == 1
            && actual[0].name_embedding == vector
            && actual[0].name_embedding_model == model
            && actual[0].name_embedding_text_hash == hash,
        "entity embedding did not roundtrip exactly"
    );
    Ok(())
}

// Clean storage proxy; native timestamps/links, no serde flatten or JSON intermediary.
#[derive(serde::Deserialize)]
struct EmbeddingTransferRow {
    space_id: String,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
    created_by: String,
    deleted_at: Option<chrono::DateTime<chrono::Utc>>,
    security: elephant_types::records::base::Security,
    provenance: elephant_types::records::base::Provenance,
    subjects: Vec<elephant_types::Link<Entity>>,
    subjects_state: elephant_types::records::base::SubjectsState,
    node: elephant_types::Link<DocNode>,
    model: String,
    dims: f64,
    vector: Vec<f32>,
}

#[expect(
    clippy::float_cmp,
    reason = "dimensions must roundtrip exactly; an approximate comparison would accept changed metadata"
)]
async fn preserve_embedding(dst: &StorageClient, source: &NodeEmbedding) -> anyhow::Result<String> {
    let partition = embedding_partition(source)?;
    let mut id = elephant_storage::surreal::thing_from_record_id(source.node.id());
    id.tb = partition.clone();
    // Normal embedding writes use wall-clock updated_at. A transfer is not a
    // new model execution, so restore the original timestamp explicitly.
    dst.query_bind_checked(
        "UPDATE $id SET updated_at=$at",
        BTreeMap::<String, elephant_storage::canonical_extension::NativeExtensionValue>::from([
            ("id".to_owned(), id.clone().into()),
            (
                "at".to_owned(),
                elephant_storage::surreal::datetime_from_chrono(source.base.updated_at).into(),
            ),
        ]),
    )
    .await?;
    let mut response = dst.query_bind_checked("SELECT space_id,created_at,updated_at,created_by,deleted_at,security,provenance,subjects,subjects_state,node,model,dims,vector FROM ONLY $id", BTreeMap::<String, elephant_storage::canonical_extension::NativeExtensionValue>::from([("id".to_owned(), id.into())])).await?;
    let row: Option<EmbeddingTransferRow> = response.take(0)?;
    let row = row.context("embedding partition row missing")?;
    anyhow::ensure!(
        row.dims == f64::from(source.dims),
        "embedding dimensions changed"
    );
    let actual = NodeEmbedding {
        base: elephant_types::records::BaseFields {
            id: source.base.id.clone(),
            space_id: row.space_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
            created_by: row.created_by,
            deleted_at: row.deleted_at,
            security: row.security,
            provenance: row.provenance,
            subjects: row.subjects,
            subjects_state: row.subjects_state,
        },
        node: row.node,
        model: row.model,
        dims: source.dims,
        vector: row.vector,
    };
    anyhow::ensure!(
        actual == *source,
        "embedding partition mapping changed content/envelope"
    );
    Ok(partition)
}

macro_rules! copy_table {
    ($src:expr, $dst_repo:expr, $ty:ty, $label:expr, $report:expr, $fingerprints:expr, $client:expr) => {{
        let rows: Vec<$ty> = $src
            .scan::<$ty>()
            .await
            .with_context(|| format!("scan {} from snapshot", $label))?;
        let total = rows.len();
        let mut written = 0usize;
        for row in rows {
            validate_row(&row, $label)?;
            let expected = serde_json::to_value(&row)?;
            let stored = $dst_repo
                .create(row)
                .await
                .with_context(|| format!("create {} row {}/{}", $label, written + 1, total))?;
            anyhow::ensure!(
                serde_json::to_value(stored)? == expected,
                "{} changed during typed storage conversion; transfer incomplete",
                $label
            );
            written += 1;
            if written.is_multiple_of(2000) {
                println!("  {}: {written}/{total}", $label);
            }
        }
        println!("{}: {written} rows", $label);
        $report.push(($label, written));
        $fingerprints.insert(
            $label.to_string(),
            target_fingerprints($client, $label).await?,
        );
        written
    }};
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "retain the destination handle through the original transfer barrier and completion scope"
)]
pub async fn run(args: DataMigrateArgs) -> anyhow::Result<()> {
    let source_bytes = validate_transfer_input(&args.snapshot)?;
    let source_digest = hex::encode(Sha256::digest(&source_bytes));
    println!("Loading snapshot {} ...", args.snapshot.display());
    let src = EmbeddedStore::new();
    src.import_snapshot_bytes(&source_bytes)
        .await
        .context("load fixed input bytes into embedded store")?;

    validate_source(&src).await?;

    let dst = args.target.connect().await?;

    preflight(&src, &dst, args.report.as_deref()).await?;
    let report_path = args.report;
    let receipt_digest = source_digest.clone();
    dst.with_transfer_barrier(&source_digest, move |privileged| async move {
        copy_snapshot(&src, &privileged, report_path.as_deref(), &receipt_digest).await
            .map_err(|error| elephant_storage::StorageError::DataIntegrityError(format!("transfer reconciliation failed: {error:#}")))
    }).await.context("transfer failed; input unchanged; an incomplete transfer marker blocks readiness and requires explicit digest-matched target discard")?;
    println!("transfer completed; source_sha256={source_digest}");
    Ok(())
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
    validate_source(src).await?;
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
                BTreeMap::from([("table".to_owned(), table.clone())]),
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
    anyhow::ensure!(
        info.tables.is_empty(),
        "target schema is not fresh; transfer requires a new database"
    );
    Ok(())
}

#[expect(
    clippy::large_stack_frames,
    clippy::significant_drop_tightening,
    reason = "retain repository drop scopes and the original transfer future and stack layout"
)]
async fn copy_snapshot(
    src: &EmbeddedStore,
    dst: &StorageClient,
    report_path: Option<&Path>,
    source_digest: &str,
) -> anyhow::Result<()> {
    println!("Applying target schema ...");
    elephant_migrations::apply_schema(dst)
        .await
        .map_err(|e| anyhow::anyhow!("apply schema: {e}"))?;

    let mut report: Vec<(&str, usize)> = Vec::new();
    let mut fingerprints: BTreeMap<String, Vec<RowFingerprint>> = BTreeMap::new();
    for table in dst.list_tables().await? {
        fingerprints.insert(table.clone(), target_fingerprints(dst, &table).await?);
    }

    // Copy order per the reference graph (schemaless target: ordering is for
    // logical consistency, not constraint satisfaction).
    copy_table!(
        src,
        SpaceConfigRepository::new(dst.clone()),
        SpaceConfig,
        "space_config",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        EntityRepository::new(dst.clone()),
        Entity,
        "entity",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        SourceRepository::new(dst.clone()),
        Source,
        "source",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        CostLedgerDailyRepository::new(dst.clone()),
        CostLedgerDaily,
        "cost_ledger_daily",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        IngestDecisionRepository::new(dst.clone()),
        IngestDecision,
        "ingest_decision",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        DocRepository::new(dst.clone()),
        Doc,
        "doc",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        DocRevisionRepository::new(dst.clone()),
        DocRevision,
        "doc_revision",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        DocNodeRepository::new(dst.clone()),
        DocNode,
        "doc_node",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        MediaAssetRepository::new(dst.clone()),
        MediaAsset,
        "media_asset",
        report,
        fingerprints,
        dst
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
        let mut partitions = BTreeMap::<String, usize>::new();
        for embedding in &rows {
            *partitions
                .entry(preserve_embedding(dst, embedding).await?)
                .or_default() += 1;
        }
        for (partition, expected_count) in partitions {
            let actual = target_fingerprints(dst, &partition).await?;
            anyhow::ensure!(
                actual.len() == expected_count,
                "embedding partition inventory differs"
            );
            fingerprints.insert(partition, actual);
        }
        println!("chunk_embedding: {written} rows");
        report.push(("chunk_embedding", written));
    }

    copy_table!(
        src,
        EntityIdentityKeyRepository::new(dst.clone()),
        EntityIdentityKey,
        "entity_identity_key",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        IdentityCandidateRepository::new(dst.clone()),
        IdentityCandidate,
        "identity_candidate",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        EntityMergeRepository::new(dst.clone()),
        EntityMerge,
        "entity_merge",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        PredicateRegistryRepository::new(dst.clone()),
        PredicateRegistry,
        "predicate_registry",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        PredicateAliasRepository::new(dst.clone()),
        PredicateAlias,
        "predicate_alias",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        AssertionRepository::new(dst.clone()),
        Assertion,
        "assertion",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        AttributeRepository::new(dst.clone()),
        Attribute,
        "attribute",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        RelRepository::new(dst.clone()),
        Rel,
        "rel",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        ConflictGroupRepository::new(dst.clone()),
        ConflictGroup,
        "conflict_group",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        TruthSlotRepository::new(dst.clone()),
        TruthSlot,
        "truth_slot",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        EventRepository::new(dst.clone()),
        Event,
        "event",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        FreshnessRepository::new(dst.clone()),
        Freshness,
        "freshness",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        ReprocessJobRepository::new(dst.clone()),
        ReprocessJob,
        "reprocess_job",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        WorkItemRepository::new(dst.clone()),
        WorkItem,
        "work_item",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        WorkEdgeRepository::new(dst.clone()),
        WorkEdge,
        "work_edge",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        WorkRunRepository::new(dst.clone()),
        WorkRun,
        "work_run",
        report,
        fingerprints,
        dst
    );
    let item_archives = src
        .scan::<elephant_types::records::WorkItemArchive>()
        .await?;
    report.push(("work_item_archive", item_archives.len()));
    for archive in item_archives {
        let expected = serde_json::to_value(&archive.record)?;
        elephant_storage::work_retention::import_item_archive(dst, archive.clone()).await?;
        let actual = WorkItemRepository::new(dst.clone())
            .get(&archive.base.space_id, &archive.original_id)
            .await?
            .context("archived item missing after transfer")?;
        anyhow::ensure!(
            serde_json::to_value(actual)? == expected,
            "archived item content changed"
        );
        verify_archive_metadata(
            dst,
            &archive.base.id,
            &archive.original_id,
            archive.archived_at,
        )
        .await?;
    }
    let run_archives = src
        .scan::<elephant_types::records::WorkRunArchive>()
        .await?;
    report.push(("work_run_archive", run_archives.len()));
    for archive in run_archives {
        let expected = serde_json::to_value(&archive.record)?;
        elephant_storage::work_retention::import_run_archive(dst, archive.clone()).await?;
        let actual = WorkRunRepository::new(dst.clone())
            .get(&archive.base.space_id, &archive.original_id)
            .await?
            .context("archived run missing after transfer")?;
        anyhow::ensure!(
            serde_json::to_value(actual)? == expected,
            "archived run content changed"
        );
        verify_archive_metadata(
            dst,
            &archive.base.id,
            &archive.original_id,
            archive.archived_at,
        )
        .await?;
    }
    copy_table!(
        src,
        WorkArtifactRepository::new(dst.clone()),
        WorkArtifact,
        "work_artifact",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        WorkDeadletterRepository::new(dst.clone()),
        WorkDeadletter,
        "work_deadletter",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        SystemEventRepository::new(dst.clone()),
        SystemEvent,
        "system_event",
        report,
        fingerprints,
        dst
    );
    copy_table!(
        src,
        AuditLogRepository::new(dst.clone()),
        AuditLog,
        "audit_log",
        report,
        fingerprints,
        dst
    );

    // Entity name embeddings: first-class surreal columns, written only by
    // the embedded rows carry them in metadata. Transfer includes deleted rows.
    {
        let entities: Vec<Entity> = src.scan::<Entity>().await.context("re-scan entities")?;
        let mut attached = 0usize;
        for entity in &entities {
            let Some((vector, model, hash)) = entity_embedding(entity)? else {
                continue;
            };
            attach_entity_embedding(dst, entity, vector, model, hash).await?;
            attached += 1;
        }
        println!("entity name embeddings attached: {attached}");
        report.push(("entity_name_embedding", attached));
    }

    for table in ["work_item_archive", "work_run_archive"] {
        fingerprints.insert(table.to_owned(), target_fingerprints(dst, table).await?);
    }
    // Entity name vectors are an explicit backend representation mapping, so
    // record the final entity row fingerprints after attachment.
    fingerprints.insert("entity".into(), target_fingerprints(dst, "entity").await?);
    seal_generated_transfer_controls(src, dst, &mut fingerprints).await?;
    reconcile_destination(src, dst, &fingerprints).await?;
    let source_inventory = source_inventory(src).await?;
    let source_rows_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&source_inventory)?));
    let destination_rows_sha256 = hex::encode(Sha256::digest(serde_json::to_vec(&fingerprints)?));
    println!("\n=== VERIFIED TRANSFER REPORT ===");
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
                &serde_json::to_vec_pretty(&serde_json::json!({ "format":"recall-legacy-transfer-v1", "state":"reconciled", "source_sha256":source_digest, "tables": tables, "source_rows_sha256":source_rows_sha256, "destination_rows_sha256":destination_rows_sha256, "destination":fingerprints }))
                    .context("serialize report")?,
            )
            .with_context(|| format!("write report to {}", path.display()))?;
        output.sync_all().context("persist transfer report")?;
        #[cfg(unix)]
        std::fs::File::open(
            path.parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new(".")),
        )?
        .sync_all()
        .context("persist report directory entry")?;
        println!("report written: {}", path.display());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    // The retained converter accepts historical plain snapshots. Current
    // kernel writes add canonical_epoch authority, which production preflight
    // must continue to refuse. Remove it only when constructing a historical
    // fixture, before that fixture becomes the immutable transfer input.
    async fn legacy_snapshot_bytes(src: &EmbeddedStore) -> Vec<u8> {
        let mut raw: serde_json::Value =
            serde_json::from_slice(&src.export_snapshot_bytes().await.unwrap()).unwrap();
        raw["tables"]
            .as_object_mut()
            .unwrap()
            .remove("canonical_epoch");
        serde_json::to_vec(&raw).unwrap()
    }

    async fn legacy_fixture(src: &EmbeddedStore) -> EmbeddedStore {
        let fixture = EmbeddedStore::new();
        fixture
            .import_snapshot_bytes(&legacy_snapshot_bytes(src).await)
            .await
            .unwrap();
        fixture
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn current_canonical_epoch_snapshot_is_refused_without_rewriting_source_or_target() {
        use elephant_types::records::{BaseFields, EntityType};
        use elephant_types::{RecordId, TableName};

        let src = EmbeddedStore::new();
        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".into(),
                "fixture".into(),
            ),
            EntityType::new("topic"),
            "Current authority cannot be silently discarded".into(),
        );
        src.create(entity.clone()).await.unwrap();
        let directory = tempfile::tempdir().unwrap();
        let snapshot = directory.path().join("current.json");
        src.export_snapshot(&snapshot).await.unwrap();
        let original = std::fs::read(&snapshot).unwrap();
        let src = EmbeddedStore::from_snapshot(&snapshot).await.unwrap();
        assert!(src
            .persisted_table_names()
            .await
            .unwrap()
            .iter()
            .any(|table| table == "canonical_epoch"));
        let dst = StorageClient::new_memory().await.unwrap();

        let error = preflight(&src, &dst, None).await.unwrap_err();
        assert!(error.to_string().contains("canonical_epoch"), "{error:#}");
        assert!(dst.list_tables().await.unwrap().is_empty());
        assert_eq!(src.scan::<Entity>().await.unwrap(), vec![entity]);
        assert_eq!(std::fs::read(snapshot).unwrap(), original);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn transfer_control_sealing_refuses_unrelated_space_and_foreign_journal() {
        use elephant_types::records::{BaseFields, EntityType};
        use elephant_types::{RecordId, TableName};

        let src = EmbeddedStore::new();
        let entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".into(),
                "fixture".into(),
            ),
            EntityType::new("topic"),
            "Original historical row".into(),
        );
        src.create(entity.clone()).await.unwrap();
        let src = legacy_fixture(&src).await;
        let dst = StorageClient::new_memory().await.unwrap();
        elephant_migrations::apply_schema(&dst).await.unwrap();
        let mut fingerprints = BTreeMap::new();
        for table in dst.list_tables().await.unwrap() {
            fingerprints.insert(
                table.clone(),
                target_fingerprints(&dst, &table).await.unwrap(),
            );
        }
        EntityRepository::new(dst.clone())
            .create(entity)
            .await
            .unwrap();
        seal_generated_transfer_controls(&src, &dst, &mut fingerprints)
            .await
            .unwrap();
        let sealed = fingerprints.clone();

        dst.query_checked(
            "CREATE canonical_epoch:foreign SET space_id='foreign', epoch=<string>rand::uuid()",
        )
        .await
        .unwrap();
        assert!(
            seal_generated_transfer_controls(&src, &dst, &mut fingerprints)
                .await
                .unwrap_err()
                .to_string()
                .contains("unexpected destination canonical epoch")
        );
        assert_eq!(
            fingerprints, sealed,
            "failed validation must not bless new fingerprints"
        );
        dst.query_checked("DELETE canonical_epoch:foreign; UPDATE source_erasure_guard SET journal_id='foreign-journal'")
            .await.unwrap();
        assert!(
            seal_generated_transfer_controls(&src, &dst, &mut fingerprints)
                .await
                .unwrap_err()
                .to_string()
                .contains("unexpected destination erasure guard")
        );
        assert_eq!(fingerprints, sealed);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
        std::fs::write(&snapshot, legacy_snapshot_bytes(&src).await).unwrap();
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
        std::fs::write(&snapshot, legacy_snapshot_bytes(&src).await).unwrap();
        let before = std::fs::read(&snapshot).unwrap();
        let src = EmbeddedStore::from_snapshot(&snapshot).await.unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        preflight(&src, &dst, None).await.unwrap();
        assert!(dst.list_tables().await.unwrap().is_empty());
        let digest = hex::encode(Sha256::digest(&before));
        let report_path = dir.path().join("receipt.json");
        let output = report_path.clone();
        let receipt_digest = digest.clone();
        dst.with_transfer_barrier(&digest, move |exclusive| async move {
            copy_snapshot(&src, &exclusive, Some(&output), &receipt_digest)
                .await
                .map_err(|error| {
                    elephant_storage::StorageError::DataIntegrityError(error.to_string())
                })
        })
        .await
        .unwrap();
        assert!(dst.recovery_status().ready);
        let report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(report_path).unwrap()).unwrap();
        assert_eq!(report["source_sha256"], digest);
        assert_eq!(report["state"], "reconciled");
        assert_eq!(report["tables"]["entity"], 1);
        assert_eq!(report["destination"]["entity"].as_array().unwrap().len(), 1);
        assert_eq!(
            report["destination"]["canonical_epoch"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            report["destination"]["source_erasure_guard"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        dst.verify_journal_identity().await.unwrap();
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
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
    fn nul_is_refused_without_changing_any_bytes() {
        let value = serde_json::json!({"dirty":"a\u{0}b","nested":[{"value":"c\u{0}d"}]});
        let before = serde_json::to_vec(&value).unwrap();
        assert!(validate_row(&value, "fixture").is_err());
        assert_eq!(serde_json::to_vec(&value).unwrap(), before);
        assert!(validate_row(&serde_json::json!({"clean":"text"}), "fixture").is_ok());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn malformed_entity_vectors_are_refused_before_target_mutation() {
        use elephant_types::records::{BaseFields, EntityType};
        use elephant_types::{RecordId, TableName};
        for metadata in [
            serde_json::json!({"name_embedding":[1.0]}),
            serde_json::json!({"name_embedding":[1.0,"bad"],"name_embedding_model":"m","name_embedding_text_hash":"h"}),
            serde_json::json!({"name_embedding":[],"name_embedding_model":"m","name_embedding_text_hash":"h"}),
            serde_json::json!({"name_embedding":[1.0],"name_embedding_model":"m","name_embedding_text_hash":"h","name_embedding_dims":2}),
        ] {
            let src = EmbeddedStore::new();
            let mut entity = Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "space".into(),
                    "fixture".into(),
                ),
                EntityType::new("topic"),
                "Fixture".into(),
            );
            entity.metadata = metadata;
            src.create(entity).await.unwrap();
            let dst = StorageClient::new_memory().await.unwrap();
            assert!(preflight(&src, &dst, None).await.is_err());
            assert!(dst.list_tables().await.unwrap().is_empty());
        }
    }

    #[test]
    fn preview_snapshot_envelopes_are_not_legacy_transfer_inputs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("live.json");
        std::fs::write(
            &path,
            br#"{"format":"recall-live-v1","data":"{}","sha256":"fixture"}"#,
        )
        .unwrap();
        assert!(validate_transfer_input(&path).is_err());
        assert!(validate_transfer_input(dir.path()).is_err());
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn unknown_typed_fields_are_refused_before_target_writes() {
        use elephant_types::records::{BaseFields, EntityType};
        use elephant_types::{RecordId, TableName};
        let src = EmbeddedStore::new();
        src.create(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "space".into(),
                "fixture".into(),
            ),
            EntityType::new("topic"),
            "Original".into(),
        ))
        .await
        .unwrap();
        let mut raw: serde_json::Value =
            serde_json::from_slice(&legacy_snapshot_bytes(&src).await).unwrap();
        raw["tables"]["entity"][0]["must_not_disappear"] = serde_json::json!("evidence");
        src.import_snapshot_bytes(&serde_json::to_vec(&raw).unwrap())
            .await
            .unwrap();
        let dst = StorageClient::new_memory().await.unwrap();
        assert!(preflight(&src, &dst, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("would lose fields"));
        assert!(dst.list_tables().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn duplicate_embedding_destination_is_refused() {
        use elephant_types::records::BaseFields;
        use elephant_types::{Link, RecordId, TableName};
        let src = EmbeddedStore::new();
        let node = Link::new(RecordId::new_v4(TableName::DocNode));
        for _ in 0..2 {
            src.create(NodeEmbedding {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::ChunkEmbedding),
                    "space".into(),
                    "fixture".into(),
                ),
                node: node.clone(),
                model: "fixture".into(),
                dims: 3,
                vector: vec![1.0, 0.0, 0.0],
            })
            .await
            .unwrap();
        }
        let src = legacy_fixture(&src).await;
        let dst = StorageClient::new_memory().await.unwrap();
        assert!(preflight(&src, &dst, None)
            .await
            .unwrap_err()
            .to_string()
            .contains("map to one destination"));
        assert!(dst.list_tables().await.unwrap().is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn reconciliation_detects_content_change_and_extra_table() {
        let dst = StorageClient::new_memory().await.unwrap();
        dst.query_checked("CREATE transfer_probe:one SET evidence='original'")
            .await
            .unwrap();
        let expected = BTreeMap::<String, Vec<RowFingerprint>>::from([(
            "transfer_probe".to_owned(),
            target_fingerprints(&dst, "transfer_probe").await.unwrap(),
        )]);
        reconcile_destination(&EmbeddedStore::new(), &dst, &expected)
            .await
            .unwrap();
        dst.query_checked("UPDATE transfer_probe:one SET evidence='changed'")
            .await
            .unwrap();
        assert!(
            reconcile_destination(&EmbeddedStore::new(), &dst, &expected)
                .await
                .unwrap_err()
                .to_string()
                .contains("rows changed")
        );
        dst.query_checked("UPDATE transfer_probe:one SET evidence='original'; CREATE extra_probe:one SET value=true").await.unwrap();
        assert!(
            reconcile_destination(&EmbeddedStore::new(), &dst, &expected)
                .await
                .unwrap_err()
                .to_string()
                .contains("unexpected destination table")
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "retain destination ownership and drop order through all original scenario assertions"
    )]
    async fn missing_report_parent_keeps_copied_target_unready() {
        let dir = tempfile::tempdir().unwrap();
        let output = dir.path().join("absent").join("receipt.json");
        let dst = StorageClient::new_memory().await.unwrap();
        let digest = "d".repeat(64);
        let result = dst
            .with_transfer_barrier(&digest, move |exclusive| async move {
                copy_snapshot(
                    &EmbeddedStore::new(),
                    &exclusive,
                    Some(&output),
                    &"d".repeat(64),
                )
                .await
                .map_err(|error| {
                    elephant_storage::StorageError::DataIntegrityError(error.to_string())
                })
            })
            .await;
        assert!(result.is_err());
        assert!(!dst.recovery_status().ready);
        assert!(dst.query_checked("RETURN 1").await.is_err());
        assert!(dst
            .discard_incomplete_transfer(&"e".repeat(64))
            .await
            .is_err());
        dst.discard_incomplete_transfer(&digest).await.unwrap();
        assert!(dst.recovery_status().ready);
        assert!(dst.list_tables().await.unwrap().is_empty());
    }
}
