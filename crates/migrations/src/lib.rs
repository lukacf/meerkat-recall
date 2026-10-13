//! Database schema for Elephant.
//!
//! This crate provides the `SurrealQL` schema definitions and a simple
//! function to apply them to a fresh database.

use elephant_storage::StorageClient;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tracing::info;

/// Schema SQL files in application order.
pub mod schema {
    /// Core tables and fields.
    pub const TABLES: &str = include_str!("../sql/001_init.surql");
    /// Indexes for efficient querying.
    pub const INDEXES: &str = include_str!("../sql/002_indexes.surql");
    /// Schema fixes for nested object fields.
    pub const FLEXIBLE_NESTED_OBJECTS: &str =
        include_str!("../sql/003_flexible_nested_objects.surql");
    /// Space configuration table.
    pub const SPACE_CONFIG: &str = include_str!("../sql/004_space_config.surql");
    /// Entity embedding support (vector search).
    pub const ENTITY_EMBEDDING: &str = include_str!("../sql/005_entity_embedding.surql");
    /// Add purpose field to space config.
    pub const SPACE_PURPOSE: &str = include_str!("../sql/006_add_space_purpose.surql");
    /// Drop legacy `doc_chunk` table.
    pub const DROP_DOC_CHUNK: &str = include_str!("../sql/007_drop_doc_chunk.surql");
    /// Add skill field to space config.
    pub const SPACE_SKILL: &str = include_str!("../sql/008_add_space_skill.surql");
    /// Freeform entity type (remove hardcoded enum).
    pub const FREEFORM_ENTITY_TYPE: &str = include_str!("../sql/009_freeform_entity_type.surql");
    /// Add first-class domain event table.
    pub const EVENT_TABLE: &str = include_str!("../sql/010_add_event.surql");
    /// Add BM25 search indexes for document lexical search.
    pub const DOC_SEARCH: &str = include_str!("../sql/011_doc_search.surql");
    /// Add BM25 search indexes for assertion lexical search.
    pub const ASSERTION_SEARCH: &str = include_str!("../sql/012_assertion_search.surql");
    /// Add multimodal document fields for media-backed docs.
    pub const DOC_MEDIA_FIELDS: &str = include_str!("../sql/013_doc_media_fields.surql");
    /// Ensure active `doc_revision` table/manifest support exists under applied schema.
    pub const DOC_REVISION_TABLE: &str = include_str!("../sql/014_doc_revision_table.surql");
    /// Add inline payload field for managed media assets.
    pub const MEDIA_ASSET_PAYLOADS: &str = include_str!("../sql/015_media_asset_payloads.surql");
    /// Truth maintenance LLM adjudication: widened slot types, memo fields,
    /// LLM-proposed semantics status.
    pub const TM_ADJUDICATION: &str = include_str!("../sql/016_tm_adjudication.surql");
    /// Schemaless parity with the embedded engine: `SCHEMAFULL` definitions had
    /// drifted from the Rust structs and `SurrealDB` silently strips undefined
    /// fields (issue #28).
    pub const SCHEMALESS_TABLES: &str = include_str!("../sql/017_schemaless_tables.surql");
    /// Index the relationship endpoint fields (`in`/`out`) — every entity
    /// read scanned the whole rel table without them (issue #31).
    pub const REL_ENDPOINT_INDEXES: &str = include_str!("../sql/018_rel_endpoint_indexes.surql");
    /// Index `media_asset` and `doc_revision` scan fields — both tables carry
    /// heavy rows (inline media payloads, full document texts) and had no
    /// indexes at all (issue #34).
    pub const MEDIA_PERF_INDEXES: &str = include_str!("../sql/019_media_perf_indexes.surql");
    /// Make `entity` embeddings writable: drop the never-written 3072-dim
    /// ASSERT and fixed-dimension index, add the staleness hash field.
    pub const ENTITY_EMBEDDING_WRITES: &str =
        include_str!("../sql/020_entity_embedding_writes.surql");
    /// First-class stable knowledge views and immutable compiled revisions.
    pub const KNOWLEDGE_VIEWS: &str = include_str!("../sql/021_knowledge_views.surql");
    /// Durable recurring source refresh policies and immutable check receipts.
    pub const SOURCE_WATCHES: &str = include_str!("../sql/022_source_watches.surql");
    /// Reverse dependency edges for indexed knowledge-view invalidation.
    pub const KNOWLEDGE_VIEW_DEPENDENCY_INDEX: &str =
        include_str!("../sql/023_knowledge_view_dependency_index.surql");
    /// Governed typed definitions for topic, community, and query views.
    pub const KNOWLEDGE_VIEW_DEFINITIONS: &str =
        include_str!("../sql/024_knowledge_view_definitions.surql");
    /// Composite indexes for graph-scale knowledge-view compilation reads.
    pub const KNOWLEDGE_VIEW_READ_INDEXES: &str =
        include_str!("../sql/025_knowledge_view_read_indexes.surql");
    /// Resolve source-derived assertions by immutable document revision.
    pub const ASSERTION_REVISION_INDEX: &str =
        include_str!("../sql/026_assertion_revision_index.surql");
    /// Explicit wiki, evidence, and inspector surfaces with editorial state.
    pub const KNOWLEDGE_VIEW_SURFACES: &str =
        include_str!("../sql/027_knowledge_view_surfaces.surql");
    /// Indexed queue selection, idempotency, source lookup, and execution joins.
    pub const WORK_QUEUE_INDEXES: &str = include_str!("../sql/028_work_queue_indexes.surql");
    /// Durable cold work and execution history.
    pub const WORK_HISTORY_ARCHIVES: &str = include_str!("../sql/029_work_history_archives.surql");
    /// Bound daily budget aggregation to its space, date, and ingest mode.
    pub const COST_LEDGER_DAILY_INDEXES: &str =
        include_str!("../sql/030_cost_ledger_daily_indexes.surql");
    /// Authoritative source revisions, receipts, publication and erasure fences.
    pub const SOURCE_PUBLICATION: &str = include_str!("../sql/031_source_publication.surql");
    /// Durable work attempts and recovery checkpoints.
    pub const WORK_RECOVERY: &str = include_str!("../sql/032_work_recovery.surql");
    /// Approved per-space processing and immutable model profiles.
    pub const SPACE_PROCESSING: &str = include_str!("../sql/033_space_processing.surql");
    /// Durable storage journal identity anchor.
    pub const STORAGE_DURABILITY: &str = include_str!("../sql/034_storage_durability.surql");
    /// Exact whole-source publication manifest and completion coverage.
    pub const SOURCE_PUBLICATION_MANIFEST: &str =
        include_str!("../sql/035_source_publication_manifest.surql");
    /// Current per-space service grants and revocation generations.
    pub const SPACE_GRANTS: &str = include_str!("../sql/036_space_grants.surql");
    /// Immutable superseded manifests for unpublished configuration replans.
    pub const SOURCE_PUBLICATION_MANIFEST_HISTORY: &str =
        include_str!("../sql/037_source_publication_manifest_history.surql");
    /// Invalidate staged index coverage when source eligibility changes.
    pub const SOURCE_INDEX_VISIBILITY: &str =
        include_str!("../sql/038_source_index_visibility.surql");
    /// Private original-journal reference membership and per-space erasure reservations.
    pub const PRIVATE_REFERENCE_FENCE: &str =
        include_str!("../sql/039_private_reference_fence.surql");
    /// Sealed original source-contact authority, erased with its physical source.
    pub const SOURCE_CONTACT_INTENT: &str = include_str!("../sql/040_source_contact_intent.surql");
    /// Private original work-producer provenance, physically erased with source data.
    pub const WORK_PRODUCER_ORIGIN: &str = include_str!("../sql/041_work_producer_origin.surql");
    /// Private recurring watch delegation and original authority ceiling.
    pub const WATCH_DELEGATION: &str = include_str!("../sql/042_watch_delegation.surql");
    /// Bounded native physical-identity writer fences, including absent reads.
    pub const NATIVE_READ_FENCE: &str = include_str!("../sql/043_native_read_fence.surql");
    /// Semantic source-head epoch changes, excluding private progress nonces.
    pub const SOURCE_HEAD_SEMANTIC_EPOCH: &str =
        include_str!("../sql/044_source_head_semantic_epoch.surql");
    /// Keep readiness/recovery read nonces outside canonical semantic epochs.
    pub const CANONICAL_READ_SEMANTIC_EPOCH: &str =
        include_str!("../sql/045_canonical_read_semantic_epoch.surql");
}

/// Applies all schema SQL to the database.
///
/// This is intended for fresh databases. It runs all schema files in order.
///
/// # Errors
///
/// Returns an error if any schema statement fails.
pub async fn apply_schema(client: &StorageClient) -> Result<(), String> {
    if client.is_embedded() {
        return Ok(());
    }
    let files = schema_files();
    let applied = schema_receipts(client).await?;
    validate_receipts(files, &applied, false)?;
    // An optional maintenance receipt never establishes schema initialization.
    if !files.iter().any(|(name, _)| applied.contains_key(*name))
        && !client
            .list_tables()
            .await
            .map_err(|e| e.to_string())?
            .is_empty()
    {
        return Err("database has no verified initialization history; initialize a fresh database explicitly".into());
    }
    apply_files(client, files, &applied).await?;
    client
        .initialize_journal_identity()
        .await
        .map_err(|error| error.to_string())?;
    verify_schema(client).await
}

/// Read-only startup check. Serve never creates or updates schema.
///
/// # Errors
/// Returns an error if storage recovery is incomplete, schema receipts are
/// missing or invalid, or the durable journal identity cannot be verified.
pub async fn verify_schema(client: &StorageClient) -> Result<(), String> {
    client.health_check().await.map_err(|error| {
        format!("storage recovery must complete before schema verification: {error}")
    })?;
    if client.is_embedded() {
        return Ok(());
    }
    validate_receipts(schema_files(), &schema_receipts(client).await?, true)?;
    client
        .verify_journal_identity()
        .await
        .map_err(|error| error.to_string())
}

const fn schema_files() -> &'static [(&'static str, &'static str)] {
    &[
        ("tables", schema::TABLES),
        ("indexes", schema::INDEXES),
        ("flexible_nested_objects", schema::FLEXIBLE_NESTED_OBJECTS),
        ("space_config", schema::SPACE_CONFIG),
        ("entity_embedding", schema::ENTITY_EMBEDDING),
        ("space_purpose", schema::SPACE_PURPOSE),
        ("drop_doc_chunk", schema::DROP_DOC_CHUNK),
        ("space_skill", schema::SPACE_SKILL),
        ("freeform_entity_type", schema::FREEFORM_ENTITY_TYPE),
        ("event_table", schema::EVENT_TABLE),
        ("doc_search", schema::DOC_SEARCH),
        ("assertion_search", schema::ASSERTION_SEARCH),
        ("doc_media_fields", schema::DOC_MEDIA_FIELDS),
        ("doc_revision_table", schema::DOC_REVISION_TABLE),
        ("media_asset_payloads", schema::MEDIA_ASSET_PAYLOADS),
        ("tm_adjudication", schema::TM_ADJUDICATION),
        ("schemaless_tables", schema::SCHEMALESS_TABLES),
        ("rel_endpoint_indexes", schema::REL_ENDPOINT_INDEXES),
        ("media_perf_indexes", schema::MEDIA_PERF_INDEXES),
        ("entity_embedding_writes", schema::ENTITY_EMBEDDING_WRITES),
        ("knowledge_views", schema::KNOWLEDGE_VIEWS),
        ("source_watches", schema::SOURCE_WATCHES),
        (
            "knowledge_view_dependency_index",
            schema::KNOWLEDGE_VIEW_DEPENDENCY_INDEX,
        ),
        (
            "knowledge_view_definitions",
            schema::KNOWLEDGE_VIEW_DEFINITIONS,
        ),
        (
            "knowledge_view_read_indexes",
            schema::KNOWLEDGE_VIEW_READ_INDEXES,
        ),
        ("assertion_revision_index", schema::ASSERTION_REVISION_INDEX),
        ("knowledge_view_surfaces", schema::KNOWLEDGE_VIEW_SURFACES),
        ("work_queue_indexes", schema::WORK_QUEUE_INDEXES),
        ("work_history_archives", schema::WORK_HISTORY_ARCHIVES),
        (
            "cost_ledger_daily_indexes",
            schema::COST_LEDGER_DAILY_INDEXES,
        ),
        ("source_publication", schema::SOURCE_PUBLICATION),
        ("work_recovery", schema::WORK_RECOVERY),
        ("space_processing", schema::SPACE_PROCESSING),
        ("storage_durability", schema::STORAGE_DURABILITY),
        (
            "source_publication_manifest",
            schema::SOURCE_PUBLICATION_MANIFEST,
        ),
        ("space_grants", schema::SPACE_GRANTS),
        (
            "source_publication_manifest_history",
            schema::SOURCE_PUBLICATION_MANIFEST_HISTORY,
        ),
        ("source_index_visibility", schema::SOURCE_INDEX_VISIBILITY),
        ("private_reference_fence", schema::PRIVATE_REFERENCE_FENCE),
        ("source_contact_intent", schema::SOURCE_CONTACT_INTENT),
        ("work_producer_origin", schema::WORK_PRODUCER_ORIGIN),
        ("watch_delegation", schema::WATCH_DELEGATION),
        ("native_read_fence", schema::NATIVE_READ_FENCE),
        (
            "source_head_semantic_epoch",
            schema::SOURCE_HEAD_SEMANTIC_EPOCH,
        ),
        (
            "canonical_read_semantic_epoch",
            schema::CANONICAL_READ_SEMANTIC_EPOCH,
        ),
    ]
}

fn checksum(sql: &str) -> String {
    format!("{:x}", Sha256::digest(sql.as_bytes()))
}

#[derive(serde::Deserialize)]
struct SchemaReceipt {
    name: String,
    checksum: String,
}

async fn schema_receipts(client: &StorageClient) -> Result<BTreeMap<String, String>, String> {
    if !client
        .list_tables()
        .await
        .map_err(|e| e.to_string())?
        .iter()
        .any(|name| name == "migration_history")
    {
        return Ok(BTreeMap::new());
    }
    let mut response = client
        .query_checked("SELECT name, checksum FROM migration_history")
        .await
        .map_err(|e| e.to_string())?;
    let receipts: Vec<SchemaReceipt> = response.take(0).map_err(|e| e.to_string())?;
    let mut result = BTreeMap::new();
    for receipt in receipts {
        if result.insert(receipt.name, receipt.checksum).is_some() {
            return Err("duplicate schema initialization receipt".into());
        }
    }
    Ok(result)
}

fn validate_receipts(
    files: &[(&str, &str)],
    applied: &BTreeMap<String, String>,
    complete: bool,
) -> Result<(), String> {
    let mut missing = false;
    for (name, sql) in files {
        match applied.get(*name) {
            Some(stored) if *stored == checksum(sql) && !missing => {}
            Some(_) => {
                return Err(format!(
                    "schema receipt mismatch or noncontiguous history: {name}"
                ))
            }
            None if complete => {
                return Err(format!(
                    "schema initialization incomplete: {name}; run explicit migrate/init"
                ))
            }
            None => missing = true,
        }
    }
    for (name, stored) in applied {
        if files.iter().any(|(expected, _)| name == expected) {
            continue;
        }
        // `migrate` runs this retained repair after applying the schema. Its
        // exact receipt may coexist with DDL receipts, but supplies no DDL and
        // must not hide another migration name or a changed repair checksum.
        if name == elephant_storage::fake_embedding_repair::RECEIPT_NAME {
            if stored != elephant_storage::fake_embedding_repair::RECEIPT_CHECKSUM {
                return Err(format!("maintenance receipt checksum mismatch: {name}"));
            }
            continue;
        }
        return Err(
            "unknown schema receipt; database belongs to a different schema version".into(),
        );
    }
    Ok(())
}

async fn apply_files(
    client: &StorageClient,
    files: &[(&str, &str)],
    applied: &BTreeMap<String, String>,
) -> Result<(), String> {
    for (name, sql) in files {
        if applied.contains_key(*name) {
            continue;
        }
        info!("Applying schema: {name}");
        // Definitions and their receipt share one transaction. Any statement failure
        // aborts the group, and Response::check also inspects errors before COMMIT.
        let batch = format!("BEGIN TRANSACTION;\n{sql}\nCREATE type::thing('migration_history', $schema_name) SET name = $schema_name, checksum = $schema_checksum, applied_at = time::now();\nCOMMIT TRANSACTION;");
        client
            .query_bind_checked(
                &batch,
                serde_json::json!({"schema_name": name, "schema_checksum": checksum(sql)}),
            )
            .await
            .map_err(|e| format!("Failed to apply {name}: {e}"))?;
    }
    info!("Schema applied successfully");
    Ok(())
}

#[cfg(all(test, unix))]
mod native_schema_profile_tests;

#[cfg(test)]
mod source_head_epoch_tests;

#[cfg(test)]
mod canonical_read_epoch_tests;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::const_is_empty, clippy::expect_used)]

    use chrono::Utc;
    use elephant_storage::{
        ConflictGroupRepository, DocRepository, DocRevisionRepository, PredicateRegistryRepository,
        Repository, StorageClient, StorageConfig, TruthSlotRepository,
    };
    use elephant_types::ids::{RecordId, TableName};
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::doc_revision::{
        DocRevision, RenderManifest, RenderManifestAttachment, RenderManifestSpanBinding,
        RenderManifestTimeRange, RendererMetadata, RevisionStatus,
    };
    use elephant_types::records::predicate::{PredicateRegistry, SemanticsStatus};
    use elephant_types::records::truth::{ConflictGroup, TruthSlot};
    use elephant_types::records::{Doc, DocMediaAttachment, DocMediaKind, DocMediaLocation};
    use elephant_types::Link;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through every read-only startup and incomplete-receipt assertion."
    )]
    async fn startup_verification_is_read_only_and_requires_completed_receipts() {
        let client = StorageClient::new_memory().await.unwrap();
        assert!(super::verify_schema(&client).await.is_err());
        assert!(client.list_tables().await.unwrap().is_empty());
        super::apply_schema(&client).await.unwrap();
        super::verify_schema(&client).await.unwrap();
        let original = super::schema_receipts(&client).await.unwrap();
        super::apply_schema(&client).await.unwrap();
        assert_eq!(super::schema_receipts(&client).await.unwrap(), original);
        client
            .query_checked("UPDATE migration_history SET checksum='wrong' WHERE name='tables'")
            .await
            .unwrap();
        assert!(super::verify_schema(&client).await.is_err());
        assert!(super::apply_schema(&client).await.is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through repeat migration and damaged-receipt immutability assertions."
    )]
    async fn migrate_repair_receipt_allows_read_only_startup_and_repeat_migration() {
        use elephant_storage::fake_embedding_repair::{
            purge_fake_embeddings, RECEIPT_CHECKSUM, RECEIPT_NAME,
        };

        let client = StorageClient::new_memory().await.unwrap();
        super::apply_schema(&client).await.unwrap();
        // Exercise the same schema -> repair -> startup sequence as the real
        // migrate command and subsequent serve, with the actual repair writer.
        let repair = purge_fake_embeddings(&client).await.unwrap();
        assert!(!repair.skipped);
        let completed = super::schema_receipts(&client).await.unwrap();
        assert_eq!(completed.get(RECEIPT_NAME).unwrap(), RECEIPT_CHECKSUM);
        assert_eq!(completed.len(), super::schema_files().len() + 1);

        super::verify_schema(&client).await.unwrap();
        assert_eq!(super::schema_receipts(&client).await.unwrap(), completed);
        super::apply_schema(&client).await.unwrap();
        assert!(purge_fake_embeddings(&client).await.unwrap().skipped);
        super::verify_schema(&client).await.unwrap();
        assert_eq!(super::schema_receipts(&client).await.unwrap(), completed);

        client
            .query_checked(&format!(
                "UPDATE migration_history:{RECEIPT_NAME} SET checksum = 'wrong'"
            ))
            .await
            .unwrap();
        let damaged = super::schema_receipts(&client).await.unwrap();
        assert!(super::verify_schema(&client)
            .await
            .unwrap_err()
            .contains("maintenance receipt checksum mismatch"));
        assert!(super::apply_schema(&client).await.is_err());
        assert_eq!(super::schema_receipts(&client).await.unwrap(), damaged);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through the maintenance-only rejection and final table-inventory assertion."
    )]
    async fn maintenance_receipt_alone_cannot_initialize_an_existing_database() {
        use elephant_storage::fake_embedding_repair::{RECEIPT_CHECKSUM, RECEIPT_NAME};

        let client = StorageClient::new_memory().await.unwrap();
        client
            .query_checked(&format!(
                "DEFINE TABLE migration_history SCHEMALESS; \
                 CREATE migration_history:{RECEIPT_NAME} SET name = '{RECEIPT_NAME}', \
                 checksum = '{RECEIPT_CHECKSUM}', applied_at = time::now();"
            ))
            .await
            .unwrap();
        let before = super::schema_receipts(&client).await.unwrap();
        assert!(super::verify_schema(&client).await.is_err());
        assert!(super::apply_schema(&client)
            .await
            .unwrap_err()
            .contains("no verified initialization history"));
        assert_eq!(super::schema_receipts(&client).await.unwrap(), before);
        assert_eq!(client.list_tables().await.unwrap(), ["migration_history"]);
    }

    #[test]
    fn maintenance_receipt_does_not_relax_schema_history_validation() {
        use elephant_storage::fake_embedding_repair::{RECEIPT_CHECKSUM, RECEIPT_NAME};
        use std::collections::BTreeMap;

        let files = super::schema_files();
        let mut complete: BTreeMap<_, _> = files
            .iter()
            .map(|(name, sql)| ((*name).to_owned(), super::checksum(sql)))
            .collect();
        complete.insert(RECEIPT_NAME.to_owned(), RECEIPT_CHECKSUM.to_owned());
        for require_complete in [false, true] {
            assert!(super::validate_receipts(files, &complete, require_complete).is_ok());
            let mut unknown = complete.clone();
            unknown.insert("future_unknown_repair".into(), RECEIPT_CHECKSUM.into());
            assert!(super::validate_receipts(files, &unknown, require_complete).is_err());
            let mut damaged = complete.clone();
            damaged.insert("tables".into(), "wrong".into());
            assert!(super::validate_receipts(files, &damaged, require_complete).is_err());
            let mut noncontiguous = complete.clone();
            noncontiguous.remove("indexes");
            assert!(super::validate_receipts(files, &noncontiguous, require_complete).is_err());
            let mut wrong_repair = complete.clone();
            wrong_repair.insert(RECEIPT_NAME.into(), "wrong".into());
            assert!(super::validate_receipts(files, &wrong_repair, require_complete).is_err());
        }
        let mut prefix = BTreeMap::from([
            ("tables".to_owned(), super::checksum(files[0].1)),
            (RECEIPT_NAME.to_owned(), RECEIPT_CHECKSUM.to_owned()),
        ]);
        assert!(super::validate_receipts(files, &prefix, false).is_ok());
        assert!(super::validate_receipts(files, &prefix, true).is_err());
        prefix.remove("tables");
        assert!(super::validate_receipts(files, &prefix, true).is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through failed-group rollback and final schema verification."
    )]
    async fn statement_failure_rolls_back_schema_group_and_receipt() {
        let client = StorageClient::new_memory().await.unwrap();
        super::apply_schema(&client).await.unwrap();
        let invalid = [(
            "injected_failure",
            "DEFINE TABLE initialization_probe SCHEMALESS; THROW 'statement failure';",
        )];
        assert!(
            super::apply_files(&client, &invalid, &std::collections::BTreeMap::new())
                .await
                .is_err()
        );
        assert!(!client
            .list_tables()
            .await
            .unwrap()
            .iter()
            .any(|table| table == "initialization_probe"));
        assert!(!super::schema_receipts(&client)
            .await
            .unwrap()
            .contains_key("injected_failure"));
        super::verify_schema(&client).await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through explicit history upgrade and prior-receipt preservation assertions."
    )]
    async fn manifest_history_requires_explicit_upgrade_and_preserves_prior_receipts() {
        let client = StorageClient::new_memory().await.unwrap();
        let files = super::schema_files();
        let history_index = files
            .iter()
            .position(|(name, _)| *name == "source_publication_manifest_history")
            .unwrap();
        super::apply_files(
            &client,
            &files[..history_index],
            &std::collections::BTreeMap::new(),
        )
        .await
        .unwrap();
        // This is an interrupted fresh migration prefix, before current journal
        // initialization. A fingerprint-less established anchor is not upgraded.
        let before = super::schema_receipts(&client).await.unwrap();
        assert!(super::verify_schema(&client)
            .await
            .unwrap_err()
            .contains("source_publication_manifest_history"));
        assert_eq!(super::schema_receipts(&client).await.unwrap(), before);
        assert!(!client
            .list_tables()
            .await
            .unwrap()
            .iter()
            .any(|table| table == "source_publication_manifest_history"));

        super::apply_schema(&client).await.unwrap();
        super::verify_schema(&client).await.unwrap();
        let after = super::schema_receipts(&client).await.unwrap();
        for (name, checksum) in before {
            assert_eq!(after.get(&name), Some(&checksum));
        }
        assert!(after.contains_key("source_publication_manifest_history"));
        assert!(after.contains_key("source_index_visibility"));
        assert!(client
            .list_tables()
            .await
            .unwrap()
            .iter()
            .any(|table| table == "source_publication_manifest_history"));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through private-reference upgrade and unchanged-history assertions."
    )]
    async fn private_reference_fence_requires_explicit_upgrade_without_rewriting_history(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_memory().await?;
        let files = super::schema_files();
        let index = files
            .iter()
            .position(|(name, _)| *name == "private_reference_fence")
            .ok_or("missing private-reference migration")?;
        assert_eq!(files[index - 1].0, "source_index_visibility");
        super::apply_files(&client, &files[..index], &std::collections::BTreeMap::new()).await?;
        // The new fingerprint field belongs to 039. Leave this interrupted
        // fresh initialization unanchored until explicit migration completes.
        let before = super::schema_receipts(&client).await?;
        let refusal = super::verify_schema(&client).await;
        assert!(matches!(refusal, Err(message) if message.contains("private_reference_fence")));
        assert_eq!(super::schema_receipts(&client).await?, before);
        assert!(!client.list_tables().await?.iter().any(|table| {
            matches!(
                table.as_str(),
                "source_erasure_guard" | "source_erasure_membership"
            )
        }));

        super::apply_schema(&client).await?;
        super::verify_schema(&client).await?;
        let after = super::schema_receipts(&client).await?;
        for (name, checksum) in before {
            assert_eq!(after.get(&name), Some(&checksum));
        }
        assert!(after.contains_key("private_reference_fence"));
        let tables = client.list_tables().await?;
        assert!(tables.iter().any(|table| table == "source_erasure_guard"));
        assert!(tables
            .iter()
            .any(|table| table == "source_erasure_membership"));
        // Repeating explicit migrate must preserve the completed receipt chain.
        super::apply_schema(&client).await?;
        assert_eq!(super::schema_receipts(&client).await?, after);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through ordered contact-intent upgrade and final schema assertions."
    )]
    async fn source_contact_intent_requires_explicit_upgrade_after_private_reference_fence(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_memory().await?;
        let files = super::schema_files();
        let index = files
            .iter()
            .position(|(name, _)| *name == "source_contact_intent")
            .ok_or("missing contact-intent migration")?;
        assert_eq!(files[index - 1].0, "private_reference_fence");
        super::apply_files(&client, &files[..index], &std::collections::BTreeMap::new()).await?;
        client.initialize_journal_identity().await?;
        let before = super::schema_receipts(&client).await?;
        let tables_before = client
            .list_tables()
            .await?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert!(before.contains_key("private_reference_fence"));
        assert!(!tables_before
            .iter()
            .any(|table| table == "source_contact_intent"));

        let refusal = super::verify_schema(&client).await;
        assert!(matches!(refusal, Err(message) if message.contains("source_contact_intent")));
        assert_eq!(super::schema_receipts(&client).await?, before);
        assert_eq!(
            client
                .list_tables()
                .await?
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            tables_before
        );

        super::apply_schema(&client).await?;
        super::verify_schema(&client).await?;
        let after = super::schema_receipts(&client).await?;
        for (name, checksum) in before {
            assert_eq!(after.get(&name), Some(&checksum));
        }
        assert!(after.contains_key("source_contact_intent"));
        assert!(client
            .list_tables()
            .await?
            .iter()
            .any(|table| table == "source_contact_intent"));
        super::apply_schema(&client).await?;
        assert_eq!(super::schema_receipts(&client).await?, after);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::range_plus_one,
        reason = "Retain the original client lifetime and exact exclusive-prefix range in the ordered standing/read-fence receipt oracle."
    )]
    async fn standing_and_physical_read_fences_require_each_explicit_schema_receipt(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_memory().await?;
        let files = super::schema_files();
        let index = files
            .iter()
            .position(|(name, _)| *name == "watch_delegation")
            .ok_or("missing standing watch migration")?;
        assert_eq!(files[index - 1].0, "work_producer_origin");
        assert_eq!(files[index + 1].0, "native_read_fence");
        super::apply_files(&client, &files[..index], &std::collections::BTreeMap::new()).await?;
        client.initialize_journal_identity().await?;

        // Readiness must refuse each missing ordered receipt without installing
        // the missing table or changing any existing receipt or table inventory.
        for (offset, required) in [(0, "watch_delegation"), (1, "native_read_fence")] {
            let before = super::schema_receipts(&client).await?;
            let tables_before = client
                .list_tables()
                .await?
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>();
            assert!(!before.contains_key(required));
            assert!(!tables_before.contains(required));
            let refusal = super::verify_schema(&client).await;
            assert!(matches!(refusal, Err(message) if message.contains(required)));
            assert_eq!(super::schema_receipts(&client).await?, before);
            assert_eq!(
                client
                    .list_tables()
                    .await?
                    .into_iter()
                    .collect::<std::collections::BTreeSet<_>>(),
                tables_before
            );

            super::apply_files(&client, &files[..index + offset + 1], &before).await?;
            let after = super::schema_receipts(&client).await?;
            for (name, checksum) in before {
                assert_eq!(after.get(&name), Some(&checksum));
            }
            assert!(after.contains_key(required));
            assert!(client
                .list_tables()
                .await?
                .iter()
                .any(|table| table == required));
        }
        super::apply_schema(&client).await?;
        super::verify_schema(&client).await?;
        let complete = super::schema_receipts(&client).await?;
        super::apply_schema(&client).await?;
        assert_eq!(super::schema_receipts(&client).await?, complete);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through producer-origin upgrade and final table and receipt assertions."
    )]
    async fn work_producer_origin_requires_explicit_upgrade_after_contact_intent(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_memory().await?;
        let files = super::schema_files();
        let index = files
            .iter()
            .position(|(name, _)| *name == "work_producer_origin")
            .ok_or("missing work-producer migration")?;
        assert_eq!(files[index - 1].0, "source_contact_intent");
        super::apply_files(&client, &files[..index], &std::collections::BTreeMap::new()).await?;
        client.initialize_journal_identity().await?;
        let before = super::schema_receipts(&client).await?;
        let tables_before = client
            .list_tables()
            .await?
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert!(before.contains_key("source_contact_intent"));
        assert!(!tables_before
            .iter()
            .any(|table| table == "work_producer_origin"));

        let refusal = super::verify_schema(&client).await;
        assert!(matches!(refusal, Err(message) if message.contains("work_producer_origin")));
        assert_eq!(super::schema_receipts(&client).await?, before);
        assert_eq!(
            client
                .list_tables()
                .await?
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
            tables_before
        );

        super::apply_schema(&client).await?;
        super::verify_schema(&client).await?;
        let after = super::schema_receipts(&client).await?;
        for (name, checksum) in before {
            assert_eq!(after.get(&name), Some(&checksum));
        }
        assert!(after.contains_key("work_producer_origin"));
        assert!(client
            .list_tables()
            .await?
            .iter()
            .any(|table| table == "work_producer_origin"));
        super::apply_schema(&client).await?;
        assert_eq!(super::schema_receipts(&client).await?, after);
        Ok(())
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime through established-journal refusal and unchanged identity assertions."
    )]
    async fn fingerprint_less_established_journal_is_not_silently_upgraded(
    ) -> Result<(), Box<dyn std::error::Error>> {
        #[derive(Debug, PartialEq, Eq, serde::Deserialize)]
        struct Anchor {
            journal_id: String,
            reference_key_fingerprint: Option<String>,
        }
        let client = StorageClient::new_memory().await?;
        let files = super::schema_files();
        let index = files
            .iter()
            .position(|(name, _)| *name == "private_reference_fence")
            .ok_or("missing private-reference migration")?;
        super::apply_files(&client, &files[..index], &std::collections::BTreeMap::new()).await?;
        // Reproduce the historical 034 anchor shape without pretending its
        // absent fingerprint identifies the current retained private key.
        let journal_id = client.erasure_journal()?.identity().to_owned();
        client
            .query_bind_checked(
                "CREATE recall_durability:journal SET journal_id=$journal_id",
                serde_json::json!({"journal_id":journal_id}),
            )
            .await?;
        let mut response = client
            .query_checked(
                "SELECT journal_id, reference_key_fingerprint FROM recall_durability:journal",
            )
            .await?;
        let before: Vec<Anchor> = response.take(0)?;
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].journal_id, journal_id);
        assert_eq!(before[0].reference_key_fingerprint, None);

        // Explicit DDL may advance verified schema receipts; it must never
        // overwrite this ambiguous established identity with a new key.
        assert!(super::apply_schema(&client).await.is_err());
        assert!(client.initialize_journal_identity().await.is_err());
        assert!(super::verify_schema(&client).await.is_err());
        let mut response = client
            .query_checked(
                "SELECT journal_id, reference_key_fingerprint FROM recall_durability:journal",
            )
            .await?;
        let after: Vec<Anchor> = response.take(0)?;
        assert_eq!(after, before);
        Ok(())
    }

    #[test]
    fn test_schema_files_included() {
        assert!(!super::schema::TABLES.is_empty());
        assert!(!super::schema::INDEXES.is_empty());
        assert!(!super::schema::FLEXIBLE_NESTED_OBJECTS.is_empty());
        assert!(!super::schema::SPACE_CONFIG.is_empty());
        assert!(!super::schema::ENTITY_EMBEDDING.is_empty());
        assert!(!super::schema::EVENT_TABLE.is_empty());
        assert!(!super::schema::DOC_SEARCH.is_empty());
        assert!(!super::schema::ASSERTION_SEARCH.is_empty());
        assert!(!super::schema::DOC_MEDIA_FIELDS.is_empty());
        assert!(!super::schema::DOC_REVISION_TABLE.is_empty());
        assert!(!super::schema::MEDIA_ASSET_PAYLOADS.is_empty());
        assert!(!super::schema::TM_ADJUDICATION.is_empty());
        assert!(!super::schema::MEDIA_PERF_INDEXES.is_empty());
        assert!(!super::schema::ENTITY_EMBEDDING_WRITES.is_empty());
        assert!(!super::schema::KNOWLEDGE_VIEWS.is_empty());
        assert!(!super::schema::SOURCE_WATCHES.is_empty());
        assert!(!super::schema::KNOWLEDGE_VIEW_DEPENDENCY_INDEX.is_empty());
        assert!(!super::schema::KNOWLEDGE_VIEW_DEFINITIONS.is_empty());
        assert!(!super::schema::KNOWLEDGE_VIEW_READ_INDEXES.is_empty());
        assert!(!super::schema::ASSERTION_REVISION_INDEX.is_empty());
        assert!(!super::schema::KNOWLEDGE_VIEW_SURFACES.is_empty());
        assert!(!super::schema::WORK_QUEUE_INDEXES.is_empty());
        assert!(!super::schema::WORK_HISTORY_ARCHIVES.is_empty());
        assert!(!super::schema::COST_LEDGER_DAILY_INDEXES.is_empty());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original client lifetime and exact final daily-capacity index schema assertions."
    )]
    async fn applied_schema_includes_daily_budget_capacity_index() {
        let client = StorageClient::new_memory().await.expect("Surreal memory");
        super::apply_schema(&client)
            .await
            .expect("apply full schema");
        let mut response = client
            .query_checked("INFO FOR TABLE cost_ledger_daily")
            .await
            .expect("ledger table schema");
        let info: Option<serde_json::Value> = response.take(0).expect("table info");
        let info = info.expect("ledger table exists");
        let definition = info["indexes"]["cost_ledger_daily_space_date_mode"]
            .as_str()
            .expect("daily budget index was applied");
        assert!(definition.contains("FIELDS space_id, date, mode"));
    }

    fn tm_test_base(table: TableName) -> BaseFields {
        BaseFields {
            id: RecordId::new_v4(table),
            space_id: "default".to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            created_by: "tester".to_string(),
            deleted_at: None,
            provenance: Provenance::default(),
            security: Security::default(),
            subjects: Vec::new(),
            subjects_state: SubjectsState::Unknown,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain original client and repository lifetimes through conflict-group, truth-slot and predicate-schema roundtrip assertions."
    )]
    async fn test_tm_adjudication_migration_widens_schema() {
        let client = StorageClient::new(StorageConfig {
            url: "mem://".to_string(),
            ..StorageConfig::default()
        })
        .await
        .expect("create surreal memory client");

        super::apply_schema(&client).await.expect("apply schema");

        let group_repo = ConflictGroupRepository::new(client.clone());
        let adjudicated_at = Utc::now();
        let group = ConflictGroup {
            base: tm_test_base(TableName::ConflictGroup),
            slot_key: "entity:1:__rel_set__:works_at:hash".to_string(),
            slot_type: "rel_set".to_string(),
            claim_ids: vec!["rel:1".to_string(), "rel:2".to_string()],
            resolved: false,
            resolution_rationale: None,
            detected_at: Utc::now(),
            resolved_at: None,
            adjudicated_claim_set_hash: Some("sha256:claims".to_string()),
            adjudicated_at: Some(adjudicated_at),
        };
        let created = group_repo
            .create(group.clone())
            .await
            .expect("create rel_set conflict group");
        assert_eq!(created.slot_type, "rel_set");
        assert_eq!(
            created.adjudicated_claim_set_hash,
            Some("sha256:claims".to_string())
        );
        assert!(created.adjudicated_at.is_some());

        // The widened ASSERT still rejects unknown slot types.
        let mut invalid = group;
        invalid.base.id = RecordId::new_v4(TableName::ConflictGroup);
        invalid.slot_type = "bogus".to_string();
        assert!(group_repo.create(invalid).await.is_err());

        let slot_repo = TruthSlotRepository::new(client.clone());
        let slot = TruthSlot {
            base: tm_test_base(TableName::TruthSlot),
            slot_key: "entity:1:__rel_set__:works_at:hash".to_string(),
            slot_type: "rel_set".to_string(),
            subject_entity: "entity:1".to_string(),
            predicate_id: "works_at".to_string(),
            current_values: Vec::new(),
            conflict_group: None,
            confidence: 0.0,
            rationale: None,
            semantics_version: 0,
            alias_version: 0,
            stale: false,
            stale_reason: None,
            computed_at: Utc::now(),
            security_hash: "hash".to_string(),
        };
        let created_slot = slot_repo
            .create(slot)
            .await
            .expect("create rel_set truth slot");
        assert_eq!(created_slot.slot_type, "rel_set");

        let registry_repo = PredicateRegistryRepository::new(client);
        let registry = PredicateRegistry {
            base: tm_test_base(TableName::PredicateRegistry),
            predicate_id: "works_at".to_string(),
            description: Some("proposed by adjudicator".to_string()),
            status: "active".to_string(),
            semantics: None,
            semantics_status: SemanticsStatus::LlmProposed,
            first_seen_at: Some(Utc::now()),
            usage_count: 0,
            last_used_at: None,
        };
        let created_registry = registry_repo
            .create(registry)
            .await
            .expect("create llm_proposed predicate registry");
        assert_eq!(
            created_registry.semantics_status,
            SemanticsStatus::LlmProposed
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain original client and document-repository lifetimes through exact multimodal-field roundtrip assertions."
    )]
    async fn test_apply_schema_preserves_multimodal_doc_fields() {
        let client = StorageClient::new(StorageConfig {
            url: "mem://".to_string(),
            ..StorageConfig::default()
        })
        .await
        .expect("create surreal memory client");

        super::apply_schema(&client).await.expect("apply schema");

        let repo = DocRepository::new(client);
        let doc = Doc {
            base: BaseFields {
                id: RecordId::new_v4(TableName::Doc),
                space_id: "default".to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                created_by: "tester".to_string(),
                deleted_at: None,
                provenance: Provenance::default(),
                security: Security::default(),
                subjects: Vec::new(),
                subjects_state: SubjectsState::Unknown,
            },
            title: "Multimodal".to_string(),
            text: String::new(),
            sidecar_text: Some("Observed screenshot".to_string()),
            source: None,
            media_attachments: vec![DocMediaAttachment {
                attachment_id: "att-1".to_string(),
                kind: DocMediaKind::Image,
                media_type: "image/jpeg".to_string(),
                location: DocMediaLocation::Path {
                    path: "/tmp/example.jpg".to_string(),
                },
                label: Some("example.jpg".to_string()),
                size_bytes: Some(512),
                duration_ms: None,
                sha256: Some("bead".repeat(16)),
            }],
            rendition_key: "multimodal-rendition".to_string(),
            metadata: serde_json::json!({}),
        };

        let created = repo.create(doc.clone()).await.expect("create doc");
        assert_eq!(created.sidecar_text, doc.sidecar_text);
        assert_eq!(created.media_attachments, doc.media_attachments);

        let loaded = repo
            .get("default", &doc.base.id)
            .await
            .expect("load doc")
            .expect("doc exists");
        assert_eq!(loaded.sidecar_text, doc.sidecar_text);
        assert_eq!(loaded.media_attachments, doc.media_attachments);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain original client and revision-repository lifetimes through the exact manifest roundtrip assertion."
    )]
    async fn test_apply_schema_preserves_doc_revision_manifest() {
        let client = StorageClient::new(StorageConfig {
            url: "mem://".to_string(),
            ..StorageConfig::default()
        })
        .await
        .expect("create surreal memory client");

        super::apply_schema(&client).await.expect("apply schema");

        let repo = DocRevisionRepository::new(client);
        let revision = DocRevision {
            base: BaseFields {
                id: RecordId::new_v4(TableName::DocRevision),
                space_id: "default".to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                created_by: "tester".to_string(),
                deleted_at: None,
                provenance: Provenance::default(),
                security: Security::default(),
                subjects: Vec::new(),
                subjects_state: SubjectsState::Unknown,
            },
            doc: Link::new(RecordId::new_v4(TableName::Doc)),
            text: "00:00 Intro\n00:15 Demo".to_string(),
            text_hash: "abc123".to_string(),
            renderer: RendererMetadata {
                name: "multimodal".to_string(),
                version: "1.0.0".to_string(),
                config_hash: Some("cfg".to_string()),
            },
            manifest: Some(RenderManifest {
                attachments: vec![RenderManifestAttachment {
                    attachment_id: "att-1".to_string(),
                    kind: DocMediaKind::Video,
                    media_type: "video/mp4".to_string(),
                    location: elephant_types::records::doc::DocMediaLocation::ManagedAsset {
                        asset_id: "media_asset:test".to_string(),
                    },
                    label: Some("segment.mp4".to_string()),
                    size_bytes: Some(2048),
                    duration_ms: Some(60_000),
                    sha256: Some("abc123".to_string()),
                }],
                span_bindings: vec![RenderManifestSpanBinding {
                    attachment_id: "att-1".to_string(),
                    text_start: 0,
                    text_end: 9,
                    time_range: Some(RenderManifestTimeRange {
                        start_ms: Some(0),
                        end_ms: Some(15_000),
                    }),
                }],
            }),
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        };

        let created = repo
            .create(revision.clone())
            .await
            .expect("create revision");
        assert_eq!(created.manifest, revision.manifest);

        let loaded = repo
            .get("default", &revision.base.id)
            .await
            .expect("load revision")
            .expect("revision exists");
        assert_eq!(loaded.manifest, revision.manifest);
    }

    /// Issue #28 regression: fields the runtime writes must round-trip through
    /// a MIGRATED store. Before migration 017 the SCHEMAFULL `source` table
    /// silently stripped `lifecycle_status` (never defined in the schema), so triage's
    /// "Triaged" update vanished and the ingest pipeline stalled with no error
    /// anywhere. Every other test suite runs schemaless and could not see this.
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original source-repository lifetime through migrated-store update and final roundtrip parity assertions."
    )]
    async fn test_migrated_store_update_roundtrip_parity() {
        use elephant_storage::SourceRepository;
        use elephant_types::records::source::{Source, SourceLifecycleStatus, SourceType};

        let client = StorageClient::new(StorageConfig {
            url: "mem://".to_string(),
            database: "parity_test".to_string(),
            ..StorageConfig::default()
        })
        .await
        .expect("create surreal memory client");

        super::apply_schema(&client).await.expect("apply schema");

        let repo = SourceRepository::new(client.clone());
        let id = RecordId::new_v4(TableName::Source);
        let source = Source {
            base: BaseFields {
                id: id.clone(),
                space_id: "default".to_string(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                created_by: "tester".to_string(),
                deleted_at: None,
                provenance: Provenance::default(),
                security: Security::default(),
                subjects: Vec::new(),
                subjects_state: SubjectsState::Unknown,
            },
            type_: SourceType::DeviceFrame,
            lifecycle_status: SourceLifecycleStatus::Discovered,
            current_ingest_mode: None,
            latest_ingest_decision: None,
            external_id: None,
            uri: Some("angel://test/frame/1".to_string()),
            path: None,
            parent: None,
            range: None,
            timestamp: None,
            author: None,
            channel: None,
            hash: "sha256:test".to_string(),
            metadata: serde_json::json!({}),
        };

        repo.create(source.clone()).await.expect("create source");

        // The exact mutation triage performs.
        let mut triaged = repo
            .get("default", &id)
            .await
            .expect("get source")
            .expect("source exists");
        triaged.lifecycle_status = SourceLifecycleStatus::Triaged;
        repo.update("default", &id, triaged)
            .await
            .expect("update source");

        let reloaded = repo
            .get("default", &id)
            .await
            .expect("reload source")
            .expect("source still exists");
        assert_eq!(
            reloaded.lifecycle_status,
            SourceLifecycleStatus::Triaged,
            "lifecycle_status must survive a write to a migrated store — \
             if this fails, a SCHEMAFULL table is silently stripping fields (#28)"
        );
    }
}
