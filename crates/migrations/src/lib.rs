//! Database schema for Elephant.
//!
//! This crate provides the `SurrealQL` schema definitions and a simple
//! function to apply them to a fresh database.

use elephant_storage::StorageClient;
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
}

/// Applies all schema SQL to the database.
///
/// This is intended for fresh databases. It runs all schema files in order.
///
/// # Errors
///
/// Returns an error if any schema statement fails.
pub async fn apply_schema(client: &StorageClient) -> Result<(), String> {
    let schemas = [
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
    ];

    for (name, sql) in schemas {
        info!("Applying schema: {name}");
        // Historical migrations tolerate already-defined schema statements.
        // Capacity indexes are idempotent and must surface backfill failures;
        // silently missing one restores the production full-table-scan hazard.
        let result = if matches!(
            name,
            "work_queue_indexes" | "work_history_archives" | "cost_ledger_daily_indexes"
        ) {
            client.query_checked(sql).await
        } else {
            client.query(sql).await
        };
        result.map_err(|e| format!("Failed to apply {name}: {e}"))?;
    }

    info!("Schema applied successfully");
    Ok(())
}

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
