//! Record ID types and link references for Elephant.
//!
//! This module provides the core ID types used throughout Elephant:
//! - `RecordId`: A typed wrapper around `SurrealDB` record IDs
//! - `Link<T>`: A reference to another record
//! - `TableName`: Enum of all core table names

use derive_more::{Display, From, Into};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use uuid::Uuid;

use crate::error::{TypeError, TypeResult};

/// Names of all core tables in Elephant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TableName {
    /// Entity table (people, apps, groups, etc.)
    Entity,
    /// Relationship edge table
    Rel,
    /// Document table
    Doc,
    /// Managed media asset table
    MediaAsset,
    /// Document chunk table
    DocChunk,
    /// Chunk embedding table
    ChunkEmbedding,
    /// Entity identity key table
    EntityIdentityKey,
    /// System event (outbox) table
    SystemEvent,
    /// Domain event table
    Event,
    /// Source provenance table
    Source,
    /// Audit log table
    AuditLog,
    /// Legacy ID map table
    IdMap,
    /// Identity candidate table
    IdentityCandidate,
    /// Relationship evidence table
    RelEvidence,
    /// Entity merge lineage table
    EntityMerge,
    /// Per-space configuration table
    SpaceConfig,

    // ─────────────────────────────────────────────────────────────────────────
    // v0.8 Ingestion-to-Graph tables (spec §2-§17)
    // ─────────────────────────────────────────────────────────────────────────
    /// Document revision table (v0.8 §7)
    DocRevision,
    /// Document node/chunk table (v0.8 §7)
    DocNode,
    /// Raw assertion table (v0.8 §8)
    Assertion,
    /// Attribute table (v0.8 §9)
    Attribute,
    /// Work item table (v0.8 §2)
    WorkItem,
    /// Cold terminal work history.
    WorkItemArchive,
    /// Work dependency edge table (v0.8 §2)
    WorkEdge,
    /// Work run execution table (v0.8 §2)
    WorkRun,
    /// Cold completed execution history.
    WorkRunArchive,
    /// Work artifact table (v0.8 §2)
    WorkArtifact,
    /// Work dead letter table (v0.8 §2)
    WorkDeadletter,
    /// Predicate registry table (v0.8 §12)
    PredicateRegistry,
    /// Predicate alias table (v0.8 §12)
    PredicateAlias,
    /// Conflict group table (v0.8 §12)
    ConflictGroup,
    /// Truth slot table (v0.8 §12)
    TruthSlot,
    /// Ingest decision table (v0.8 §6)
    IngestDecision,
    /// Cost ledger daily table (v0.8 §17)
    CostLedgerDaily,
    /// Freshness score table (v0.8 §13)
    Freshness,
    /// Reprocess job table (v0.8 §16)
    ReprocessJob,
    /// Stable definitions for derived knowledge views.
    KnowledgeView,
    /// Immutable compiled revisions of derived knowledge views.
    KnowledgeViewRevision,
    /// Governed typed definitions for graph-derived knowledge views.
    KnowledgeViewDefinition,
    /// Current reverse dependency edge for indexed view invalidation.
    KnowledgeViewDependency,
    /// Durable recurring refresh policy for a remote source.
    SourceWatch,
    /// Immutable receipt for one remote source observation.
    SourceWatchCheck,
}

impl TableName {
    /// Returns the `SurrealDB` table name string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Entity => "entity",
            Self::Rel => "rel",
            Self::Doc => "doc",
            Self::MediaAsset => "media_asset",
            Self::DocChunk => "doc_chunk",
            Self::ChunkEmbedding => "chunk_embedding",
            Self::EntityIdentityKey => "entity_identity_key",
            Self::SystemEvent => "system_event",
            Self::Event => "event",
            Self::Source => "source",
            Self::AuditLog => "audit_log",
            Self::IdMap => "id_map",
            Self::IdentityCandidate => "identity_candidate",
            Self::RelEvidence => "rel_evidence",
            Self::EntityMerge => "entity_merge",
            Self::SpaceConfig => "space_config",
            // v0.8 tables
            Self::DocRevision => "doc_revision",
            Self::DocNode => "doc_node",
            Self::Assertion => "assertion",
            Self::Attribute => "attribute",
            Self::WorkItem => "work_item",
            Self::WorkItemArchive => "work_item_archive",
            Self::WorkEdge => "work_edge",
            Self::WorkRun => "work_run",
            Self::WorkRunArchive => "work_run_archive",
            Self::WorkArtifact => "work_artifact",
            Self::WorkDeadletter => "work_deadletter",
            Self::PredicateRegistry => "predicate_registry",
            Self::PredicateAlias => "predicate_alias",
            Self::ConflictGroup => "conflict_group",
            Self::TruthSlot => "truth_slot",
            Self::IngestDecision => "ingest_decision",
            Self::CostLedgerDaily => "cost_ledger_daily",
            Self::Freshness => "freshness",
            Self::ReprocessJob => "reprocess_job",
            Self::KnowledgeView => "knowledge_view",
            Self::KnowledgeViewRevision => "knowledge_view_revision",
            Self::KnowledgeViewDefinition => "knowledge_view_definition",
            Self::KnowledgeViewDependency => "knowledge_view_dependency",
            Self::SourceWatch => "source_watch",
            Self::SourceWatchCheck => "source_watch_check",
        }
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl FromStr for TableName {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "entity" => Ok(Self::Entity),
            "rel" => Ok(Self::Rel),
            "doc" => Ok(Self::Doc),
            "media_asset" => Ok(Self::MediaAsset),
            "doc_chunk" => Ok(Self::DocChunk),
            "chunk_embedding" => Ok(Self::ChunkEmbedding),
            "entity_identity_key" => Ok(Self::EntityIdentityKey),
            "system_event" => Ok(Self::SystemEvent),
            "event" => Ok(Self::Event),
            "source" => Ok(Self::Source),
            "audit_log" => Ok(Self::AuditLog),
            "id_map" => Ok(Self::IdMap),
            "identity_candidate" => Ok(Self::IdentityCandidate),
            "rel_evidence" => Ok(Self::RelEvidence),
            "entity_merge" => Ok(Self::EntityMerge),
            "space_config" => Ok(Self::SpaceConfig),
            // v0.8 tables
            "doc_revision" => Ok(Self::DocRevision),
            "doc_node" => Ok(Self::DocNode),
            "assertion" => Ok(Self::Assertion),
            "attribute" => Ok(Self::Attribute),
            "work_item" => Ok(Self::WorkItem),
            "work_item_archive" => Ok(Self::WorkItemArchive),
            "work_edge" => Ok(Self::WorkEdge),
            "work_run" => Ok(Self::WorkRun),
            "work_run_archive" => Ok(Self::WorkRunArchive),
            "work_artifact" => Ok(Self::WorkArtifact),
            "work_deadletter" => Ok(Self::WorkDeadletter),
            "predicate_registry" => Ok(Self::PredicateRegistry),
            "predicate_alias" => Ok(Self::PredicateAlias),
            "conflict_group" => Ok(Self::ConflictGroup),
            "truth_slot" => Ok(Self::TruthSlot),
            "ingest_decision" => Ok(Self::IngestDecision),
            "cost_ledger_daily" => Ok(Self::CostLedgerDaily),
            "freshness" => Ok(Self::Freshness),
            "reprocess_job" => Ok(Self::ReprocessJob),
            "knowledge_view" => Ok(Self::KnowledgeView),
            "knowledge_view_revision" => Ok(Self::KnowledgeViewRevision),
            "knowledge_view_definition" => Ok(Self::KnowledgeViewDefinition),
            "knowledge_view_dependency" => Ok(Self::KnowledgeViewDependency),
            "source_watch" => Ok(Self::SourceWatch),
            "source_watch_check" => Ok(Self::SourceWatchCheck),
            _ => Err(TypeError::InvalidTableName(s.to_string())),
        }
    }
}

/// Helper to handle various ID formats from `SurrealDB` natively.
enum AnyId {
    Str(String),
}

impl<'de> serde::Deserialize<'de> for AnyId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, EnumAccess, MapAccess, VariantAccess, Visitor};
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = AnyId;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(
                    "a string id, raw UUID bytes, a {String: ...}/{Uuid: ...} map, or an enum like String(\"...\")",
                )
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(AnyId::Str(v.to_owned()))
            }
            fn visit_string<E: de::Error>(self, v: String) -> Result<Self::Value, E> {
                Ok(AnyId::Str(v))
            }
            fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                AnyId::deserialize(deserializer)
            }
            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<Self::Value, E> {
                let uuid_val =
                    uuid::Uuid::from_slice(v).map_err(|e| de::Error::custom(e.to_string()))?;
                Ok(AnyId::Str(uuid_val.to_string()))
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut bytes = Vec::new();
                while let Some(b) = seq.next_element::<u8>()? {
                    bytes.push(b);
                }
                if bytes.len() == 16 {
                    let uuid_val = uuid::Uuid::from_slice(&bytes)
                        .map_err(|e| de::Error::custom(e.to_string()))?;
                    Ok(AnyId::Str(uuid_val.to_string()))
                } else {
                    Err(de::Error::custom("Invalid UUID byte length"))
                }
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let key: Option<String> = map.next_key()?;
                if let Some(k) = key {
                    match k.as_str() {
                        "String" => {
                            let s: String = map.next_value()?;
                            return Ok(AnyId::Str(s));
                        }
                        _ if k.eq_ignore_ascii_case("Uuid") || k.contains("Uuid") => {
                            return map.next_value::<AnyId>();
                        }
                        _ => {
                            if let Ok(Some(s)) = map.next_value::<Option<String>>() {
                                return Ok(AnyId::Str(s));
                            }
                        }
                    }
                }
                Err(de::Error::custom("unsupported id map shape"))
            }
            fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Self::Value, A::Error> {
                let (variant, v): (String, A::Variant) = data.variant()?;
                if variant.eq_ignore_ascii_case("String") {
                    let inner = v.newtype_variant::<String>()?;
                    return Ok(AnyId::Str(inner));
                }
                if variant.eq_ignore_ascii_case("Uuid") || variant.contains("Uuid") {
                    return v.newtype_variant::<AnyId>();
                }
                v.unit_variant()?;
                Ok(AnyId::Str(variant))
            }
        }
        deserializer.deserialize_any(V)
    }
}

/// A unique identifier for a record in `SurrealDB`.
///
/// Format: `table:uuid` (e.g., `entity:550e8400-e29b-41d4-a716-446655440000`)
///
/// Serializes to/from a string in "table:uuid" format for `SurrealDB` compatibility.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Display, From, Into)]
#[display("{table}:{id}")]
pub struct RecordId {
    table: TableName,
    id: Uuid,
}

impl Serialize for RecordId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RecordId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};

        struct RecordIdVisitor;

        impl<'de> Visitor<'de> for RecordIdVisitor {
            type Value = RecordId;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a record ID string like 'entity:uuid' or a record object with 'id' and 'table' fields")
            }

            fn visit_str<E>(self, value: &str) -> Result<RecordId, E>
            where
                E: de::Error,
            {
                RecordId::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_map<M>(self, mut map: M) -> Result<RecordId, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut id_val: Option<String> = None;
                let mut table_val: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "id" => {
                            let any_id: AnyId = map.next_value()?;
                            match any_id {
                                AnyId::Str(s) => id_val = Some(s),
                            }
                        }
                        "table" | "tb" => {
                            table_val = Some(map.next_value()?);
                        }
                        _ => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let id_str = id_val.ok_or_else(|| de::Error::missing_field("id"))?;
                let table_str = table_val.ok_or_else(|| de::Error::missing_field("table"))?;

                let table = TableName::from_str(&table_str)
                    .map_err(|e| de::Error::custom(e.to_string()))?;
                let id = Uuid::parse_str(&id_str)
                    .map_err(|e| de::Error::custom(format!("Invalid UUID: {e}")))?;

                Ok(RecordId::new(table, id))
            }
        }

        deserializer.deserialize_any(RecordIdVisitor)
    }
}

impl RecordId {
    /// Creates a new record ID.
    #[must_use]
    pub const fn new(table: TableName, id: Uuid) -> Self {
        Self { table, id }
    }

    /// Creates a new record ID with a random UUID v4.
    #[must_use]
    pub fn new_v4(table: TableName) -> Self {
        Self {
            table,
            id: Uuid::new_v4(),
        }
    }

    /// Creates a new record ID with a time-ordered UUID v7.
    #[must_use]
    pub fn new_v7(table: TableName) -> Self {
        Self {
            table,
            id: Uuid::now_v7(),
        }
    }

    /// Returns the table name.
    #[must_use]
    pub const fn table(&self) -> TableName {
        self.table
    }

    /// Returns the UUID portion of the ID.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Converts to `SurrealDB` record ID string format.
    #[must_use]
    pub fn to_surreal_string(&self) -> String {
        format!("{}:{}", self.table, self.id)
    }
}

impl FromStr for RecordId {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(TypeError::InvalidRecordId(s.to_string()));
        }

        let table = TableName::from_str(parts[0])?;
        let id_part = parts[1]
            .strip_prefix('⟨')
            .and_then(|inner| inner.strip_suffix('⟩'))
            .unwrap_or(parts[1]);

        let id = Uuid::parse_str(id_part)?;

        Ok(Self { table, id })
    }
}

/// A typed link/reference to another record.
///
/// This is used for foreign key relationships between records.
/// The generic parameter `T` indicates the expected record type for type safety.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Link<T> {
    id: RecordId,
    _marker: std::marker::PhantomData<T>,
}

impl<T> Serialize for Link<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.id.serialize(serializer)
    }
}

impl<'de, T> Deserialize<'de> for Link<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, MapAccess, Visitor};
        use std::fmt;

        struct LinkVisitor<T>(std::marker::PhantomData<T>);

        impl<'de, T> Visitor<'de> for LinkVisitor<T> {
            type Value = Link<T>;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a record ID string or native SurrealDB ID object")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
                RecordId::from_str(value)
                    .map(Link::new)
                    .map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut id_val: Option<String> = None;
                let mut table_val: Option<String> = None;

                while let Some(key) = map.next_key::<String>()? {
                    match key.as_str() {
                        "id" => {
                            let any_id: AnyId = map.next_value()?;
                            match any_id {
                                AnyId::Str(s) => id_val = Some(s),
                            }
                        }
                        "table" | "tb" => {
                            table_val = Some(map.next_value()?);
                        }
                        _ => {
                            let _: de::IgnoredAny = map.next_value()?;
                        }
                    }
                }

                let id_str = id_val.ok_or_else(|| de::Error::missing_field("id"))?;
                let table_str = table_val.ok_or_else(|| de::Error::missing_field("table"))?;

                let table = TableName::from_str(&table_str)
                    .map_err(|e| de::Error::custom(e.to_string()))?;
                let id = uuid::Uuid::parse_str(&id_str)
                    .map_err(|e| de::Error::custom(format!("Invalid UUID: {e}")))?;

                Ok(Link::new(RecordId::new(table, id)))
            }
        }

        deserializer.deserialize_any(LinkVisitor(std::marker::PhantomData))
    }
}

impl<T> Link<T> {
    /// Creates a new link from a record ID.
    #[must_use]
    pub const fn new(id: RecordId) -> Self {
        Self {
            id,
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns the underlying record ID.
    #[must_use]
    pub const fn id(&self) -> &RecordId {
        &self.id
    }

    /// Converts to the underlying record ID, consuming self.
    #[must_use]
    pub const fn into_id(self) -> RecordId {
        self.id
    }
}

impl<T> fmt::Display for Link<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl<T> From<RecordId> for Link<T> {
    fn from(id: RecordId) -> Self {
        Self::new(id)
    }
}

/// Parses a link from a string, validating the table matches the expected type.
///
/// # Errors
///
/// Returns an error if the string is not a valid record ID or if the table
/// does not match `expected_table`.
pub fn parse_link<T>(s: &str, expected_table: TableName) -> TypeResult<Link<T>> {
    let id = RecordId::from_str(s)?;
    if id.table() != expected_table {
        return Err(TypeError::InvalidRecordId(format!(
            "Expected table '{}', got '{}'",
            expected_table,
            id.table()
        )));
    }
    Ok(Link::new(id))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    #[test]
    fn test_table_name_roundtrip() {
        for table in [
            TableName::Entity,
            TableName::Rel,
            TableName::Doc,
            TableName::DocChunk,
            TableName::Event,
        ] {
            let s = table.as_str();
            assert!(matches!(TableName::from_str(s), Ok(parsed) if parsed == table));
        }
    }

    #[test]
    fn test_record_id_roundtrip() {
        let id = RecordId::new_v4(TableName::Entity);
        let s = id.to_string();
        assert!(matches!(RecordId::from_str(&s), Ok(parsed) if parsed == id));
    }

    #[test]
    fn test_record_id_format() {
        let expected = Uuid::from_u128(0x550e_8400_e29b_41d4_a716_4466_5544_0000);
        let uuid = Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").ok();
        assert_eq!(uuid, Some(expected));
        let uuid = expected;
        let id = RecordId::new(TableName::Entity, uuid);
        assert_eq!(
            id.to_string(),
            "entity:550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn test_link_creation() {
        let id = RecordId::new_v4(TableName::Entity);
        let link: Link<()> = Link::new(id.clone());
        assert_eq!(link.id(), &id);
    }

    #[test]
    fn test_invalid_table_name() {
        let result = TableName::from_str("invalid_table");
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_record_id() {
        let result = RecordId::from_str("invalid");
        assert!(result.is_err());

        let result = RecordId::from_str("entity:not-a-uuid");
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn record_id_roundtrip(table_idx in 0usize..32, uuid_bytes in any::<[u8; 16]>()) {
            let tables = [
                // Legacy tables
                TableName::Entity,
                TableName::Rel,
                TableName::Doc,
                TableName::DocChunk,
                TableName::ChunkEmbedding,
                TableName::EntityIdentityKey,
                TableName::SystemEvent,
                TableName::Event,
                TableName::Source,
                TableName::AuditLog,
                TableName::IdMap,
                TableName::IdentityCandidate,
                TableName::RelEvidence,
                TableName::EntityMerge,
                TableName::SpaceConfig,
                // v0.8 tables
                TableName::DocRevision,
                TableName::DocNode,
                TableName::Assertion,
                TableName::Attribute,
                TableName::WorkItem,
                TableName::WorkEdge,
                TableName::WorkRun,
                TableName::WorkArtifact,
                TableName::WorkDeadletter,
                TableName::PredicateRegistry,
                TableName::PredicateAlias,
                TableName::ConflictGroup,
                TableName::TruthSlot,
                TableName::IngestDecision,
                TableName::CostLedgerDaily,
                TableName::Freshness,
                TableName::ReprocessJob,
            ];
            let table = tables[table_idx];
            let uuid = Uuid::from_bytes(uuid_bytes);
            let id = RecordId::new(table, uuid);
            let s = id.to_string();
            let parsed = RecordId::from_str(&s);
            prop_assert!(matches!(parsed, Ok(parsed) if parsed == id));
        }
    }
}
