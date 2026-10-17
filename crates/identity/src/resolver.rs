//! Identity resolver for executing entity merges.
//!
//! This module handles:
//! - Extension reference declarations
//! - Reference rewriting during merges
//! - Merge execution with transaction support

use chrono::{DateTime, Utc};
use elephant_storage::{
    surreal::thing_from_record_id, EmbeddedMutation, StorageClient, StorageError,
};
use elephant_types::ids::TableName;
#[cfg(test)]
use elephant_types::records::{entity::EntityStatus, Entity};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use crate::error::{IdentityError, IdentityResult};
use crate::merge::{MergeLineage, MergeStrategy};

/// Cardinality of a reference field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceCardinality {
    /// Single reference (record link).
    #[default]
    One,
    /// Multiple references (array of record links).
    Many,
}

/// Merge behavior for a reference field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnMergeBehavior {
    /// Rewrite stored references during merge.
    #[default]
    Rewrite,
    /// Leave references intact, resolve redirects on read.
    Canonicalize,
}

/// Declaration of an entity reference in an extension table.
///
/// Extensions declare their entity references so Elephant can rewrite
/// them during merge operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferenceDeclaration {
    /// Table containing the reference.
    pub table: String,
    /// Field path to the reference (dot-separated for nested fields).
    pub field_path: String,
    /// Cardinality of the reference.
    #[serde(default)]
    pub cardinality: ReferenceCardinality,
    /// Merge behavior.
    #[serde(default)]
    pub on_merge: OnMergeBehavior,
    /// Whether the field is required.
    #[serde(default)]
    pub required: bool,
}

impl ReferenceDeclaration {
    /// Creates a new reference declaration.
    #[must_use]
    pub fn new(table: impl Into<String>, field_path: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            field_path: field_path.into(),
            cardinality: ReferenceCardinality::One,
            on_merge: OnMergeBehavior::Rewrite,
            required: false,
        }
    }

    /// Sets the cardinality to many.
    #[must_use]
    pub fn with_many(mut self) -> Self {
        self.cardinality = ReferenceCardinality::Many;
        // Default for many is rewrite
        if self.on_merge == OnMergeBehavior::Canonicalize {
            self.on_merge = OnMergeBehavior::Rewrite;
        }
        self
    }

    /// Sets the `on_merge` behavior.
    #[must_use]
    pub const fn with_on_merge(mut self, behavior: OnMergeBehavior) -> Self {
        self.on_merge = behavior;
        self
    }

    /// Sets the field as required.
    #[must_use]
    pub const fn required(mut self) -> Self {
        self.required = true;
        self
    }

    /// Returns true if this reference should be rewritten during merge.
    #[must_use]
    pub fn should_rewrite(&self) -> bool {
        self.on_merge == OnMergeBehavior::Rewrite
    }
}

/// Registry of extension reference declarations.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReferenceRegistry {
    /// Declarations indexed by table name.
    declarations: HashMap<String, Vec<ReferenceDeclaration>>,
    /// All extension-owned tables declared by manifests, including tables that
    /// currently have no rewrite declarations.
    owned_tables: HashSet<String>,
}

impl ReferenceRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a reference declaration.
    pub fn register(&mut self, decl: ReferenceDeclaration) {
        let _ = self.owned_tables.insert(decl.table.clone());
        self.declarations
            .entry(decl.table.clone())
            .or_default()
            .push(decl);
    }

    /// Registers an extension-owned table even if it has no rewrite
    /// declarations.
    pub fn register_table(&mut self, table: impl Into<String>) {
        let _ = self.owned_tables.insert(table.into());
    }

    /// Returns all declarations for a table.
    #[must_use]
    pub fn get_table_refs(&self, table: &str) -> &[ReferenceDeclaration] {
        self.declarations.get(table).map_or(&[], Vec::as_slice)
    }

    /// Returns all declarations that should be rewritten.
    pub fn rewrite_refs(&self) -> impl Iterator<Item = &ReferenceDeclaration> {
        self.declarations
            .values()
            .flatten()
            .filter(|d| d.should_rewrite())
    }

    /// Returns extension-owned tables that currently have no rewrite
    /// declarations.
    pub fn owned_tables_without_rewrite_declarations(&self) -> impl Iterator<Item = &str> {
        self.owned_tables
            .iter()
            .filter(|table| {
                self.get_table_refs(table)
                    .iter()
                    .all(|decl| !decl.should_rewrite())
            })
            .map(String::as_str)
    }

    /// Returns all tables with registered references.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.owned_tables.iter().map(String::as_str)
    }

    /// Returns true when the table is declared as extension-owned.
    #[must_use]
    pub fn owns_table(&self, table: &str) -> bool {
        self.owned_tables.contains(table)
    }

    /// Returns the total number of declarations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.declarations.values().map(Vec::len).sum()
    }

    /// Returns true if the registry is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.declarations.is_empty() && self.owned_tables.is_empty()
    }
}

/// Core tables that have entity references and should always be rewritten.
pub const CORE_ENTITY_REF_TABLES: &[(&str, &str)] = &[
    ("rel", "in"),
    ("rel", "out"),
    ("rel", "subjects"),
    ("attribute", "subject"),
    ("attribute", "subjects"),
    ("assertion", "subject_entity"),
    ("assertion", "object_entity"),
    ("assertion", "subjects"),
    ("fact", "subject"),
    ("entity_identity_key", "entity"),
    ("doc", "subjects"),
    ("event", "subjects"),
    ("system_event", "subjects"),
];

/// Request to merge two entities.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MergeRequest {
    /// First entity to merge.
    pub entity_a: RecordId,
    /// Second entity to merge.
    pub entity_b: RecordId,
    /// Strategy to use.
    pub strategy: MergeStrategy,
    /// Actor performing the merge.
    pub actor: String,
    /// Optional reason for the merge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Optional run ID for batch operations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

impl MergeRequest {
    /// Creates a new merge request.
    #[must_use]
    pub fn new(
        entity_a: RecordId,
        entity_b: RecordId,
        strategy: MergeStrategy,
        actor: impl Into<String>,
    ) -> Self {
        Self {
            entity_a,
            entity_b,
            strategy,
            actor: actor.into(),
            reason: None,
            run_id: None,
        }
    }

    /// Sets the reason for the merge.
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Sets the run ID for batch operations.
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Determines the survivor and merged entities based on strategy.
    #[must_use]
    pub fn resolve_survivor(&self) -> (RecordId, Vec<RecordId>) {
        match self.strategy {
            MergeStrategy::KeepA => (self.entity_a.clone(), vec![self.entity_b.clone()]),
            MergeStrategy::KeepB => (self.entity_b.clone(), vec![self.entity_a.clone()]),
            MergeStrategy::Combine => {
                // For combine, both become merged - caller must create new entity
                (
                    self.entity_a.clone(), // Placeholder, will be replaced
                    vec![self.entity_a.clone(), self.entity_b.clone()],
                )
            }
        }
    }

    /// Creates a lineage record for this merge.
    #[must_use]
    pub fn to_lineage(&self, survivor: &RecordId) -> Vec<MergeLineage> {
        let merged = match self.strategy {
            MergeStrategy::KeepA => vec![self.entity_b.clone()],
            MergeStrategy::KeepB => vec![self.entity_a.clone()],
            MergeStrategy::Combine => vec![self.entity_a.clone(), self.entity_b.clone()],
        };

        merged
            .into_iter()
            .map(|from| MergeLineage {
                from,
                into: survivor.clone(),
                strategy: self.strategy,
                decided_by: self.actor.clone(),
                run_id: self.run_id.clone(),
            })
            .collect()
    }
}

/// Validates that entities are mergeable.
///
/// # Errors
///
/// Returns an error if the entities are the same or if either record is not an entity.
pub fn validate_merge(entity_a: &RecordId, entity_b: &RecordId) -> IdentityResult<()> {
    // Entities must be different
    if entity_a == entity_b {
        return Err(IdentityError::MergeConflict(
            "Cannot merge entity with itself".to_string(),
        ));
    }

    // Both must be entity records
    if entity_a.table() != TableName::Entity || entity_b.table() != TableName::Entity {
        return Err(IdentityError::InvalidStrategy(
            "Can only merge entity records".to_string(),
        ));
    }

    Ok(())
}

/// Escapes a string for use in `SurrealQL` single-quoted strings.
///
/// Replaces single quotes with escaped single quotes.
#[must_use]
fn escape_surql_string(s: &str) -> String {
    s.replace('\\', "\\\\").replace('\'', "\\'")
}

/// Validates that an identifier is safe for use in `SurrealQL`.
///
/// Identifiers must be alphanumeric with underscores, and not start with a digit.
///
/// # Errors
///
/// Returns an error if the identifier is empty or contains invalid characters.
fn validate_identifier(s: &str) -> crate::IdentityResult<()> {
    if s.is_empty() {
        return Err(crate::IdentityError::InvalidStrategy(
            "Identifier cannot be empty".to_string(),
        ));
    }

    let first = s.chars().next().ok_or_else(|| {
        crate::IdentityError::InvalidStrategy("Identifier cannot be empty".to_string())
    })?;
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(crate::IdentityError::InvalidStrategy(format!(
            "Identifier must start with letter or underscore: {s}"
        )));
    }

    for ch in s.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' && ch != '.' {
            return Err(crate::IdentityError::InvalidStrategy(format!(
                "Invalid character '{ch}' in identifier: {s}"
            )));
        }
    }

    Ok(())
}

fn validate_field_name_segment(s: &str) -> crate::IdentityResult<()> {
    if s.is_empty() {
        return Err(crate::IdentityError::InvalidStrategy(
            "Identifier cannot be empty".to_string(),
        ));
    }

    let first = s.chars().next().ok_or_else(|| {
        crate::IdentityError::InvalidStrategy("Identifier cannot be empty".to_string())
    })?;
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(crate::IdentityError::InvalidStrategy(format!(
            "Identifier must start with letter or underscore: {s}"
        )));
    }

    for ch in s.chars() {
        if !ch.is_ascii_alphanumeric() && ch != '_' {
            return Err(crate::IdentityError::InvalidStrategy(format!(
                "Invalid character '{ch}' in identifier: {s}"
            )));
        }
    }

    Ok(())
}

/// Generates `SurrealQL` for rewriting a reference field.
///
/// Returns the UPDATE query to rewrite references from `old_id` to `new_id`.
///
/// # Errors
///
/// Returns an error if the table or field path contains invalid characters.
pub fn generate_rewrite_query(
    decl: &ReferenceDeclaration,
    old_id: &RecordId,
    new_id: &RecordId,
) -> crate::IdentityResult<String> {
    // Validate identifiers to prevent injection
    validate_identifier(&decl.table)?;
    let _ = parse_field_path_segments(&decl.field_path)?;

    let table = &decl.table;
    let field = &decl.field_path;
    // Use type::thing to safely reference record IDs (handles hyphenated UUIDs).
    let old_str = format!("type::thing('{}', '{}')", old_id.table(), old_id.id());
    let new_str = format!("type::thing('{}', '{}')", new_id.table(), new_id.id());

    let query = match decl.cardinality {
        ReferenceCardinality::One => {
            // Single reference: UPDATE table SET field = new WHERE field = old
            format!("UPDATE {table} SET {field} = {new_str} WHERE {field} = {old_str}")
        }
        ReferenceCardinality::Many => {
            // Array reference: replace + dedupe with array::distinct
            format!(
                "UPDATE {table} SET {field} = array::distinct(array::concat(array::complement({field}, [{old_str}]), [{new_str}])) WHERE {field} CONTAINS {old_str}"
            )
        }
    };

    Ok(query)
}

/// Generates `SurrealQL` for rewriting a reference field within a single
/// logical Elephant space.
///
/// # Errors
///
/// Returns an error if the table or field path contains invalid characters.
pub fn generate_scoped_rewrite_query(
    decl: &ReferenceDeclaration,
    space_id: &str,
    old_id: &RecordId,
    new_id: &RecordId,
) -> crate::IdentityResult<String> {
    validate_identifier(&decl.table)?;
    let _ = parse_field_path_segments(&decl.field_path)?;

    let table = &decl.table;
    let field = &decl.field_path;
    let escaped_space_id = escape_surql_string(space_id);
    let old_str = format!("type::thing('{}', '{}')", old_id.table(), old_id.id());
    let new_str = format!("type::thing('{}', '{}')", new_id.table(), new_id.id());

    let query = match decl.cardinality {
        ReferenceCardinality::One => format!(
            "UPDATE {table} SET {field} = {new_str} WHERE space_id = '{escaped_space_id}' AND {field} = {old_str}"
        ),
        ReferenceCardinality::Many => format!(
            "UPDATE {table} SET {field} = array::distinct(array::concat(array::complement({field}, [{old_str}]), [{new_str}])) WHERE space_id = '{escaped_space_id}' AND {field} CONTAINS {old_str}"
        ),
    };

    Ok(query)
}

/// Generates `SurrealQL` for creating an `id_map` entry.
///
/// The `source_system` is properly escaped to prevent SQL injection.
/// `RecordIds` are used directly without quotes as they produce safe `SurrealDB` record references.
#[must_use]
pub fn generate_id_map_entry(
    space_id: &RecordId,
    legacy_id: &RecordId,
    canonical_id: &RecordId,
    source_system: &str,
) -> String {
    // Escape source_system to prevent SQL injection
    let escaped_source = escape_surql_string(source_system);
    // RecordId::to_string() produces safe record references (e.g., entity:uuid)
    // No quotes needed around RecordIds - SurrealDB accepts them directly
    format!(
        "CREATE id_map SET space_id = {space_id}, source_system = '{escaped_source}', legacy_id = {legacy_id}, canonical_id = {canonical_id}"
    )
}

/// Generates `SurrealQL` for marking an entity as merged.
#[must_use]
pub fn generate_merge_tombstone(entity_id: &RecordId, survivor_id: &RecordId) -> String {
    format!("UPDATE {entity_id} SET status = 'merged', redirect_to = {survivor_id}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PathSegment {
    Field(String),
    Each(String),
}

fn parse_field_path_segments(path: &str) -> IdentityResult<Vec<PathSegment>> {
    if path.is_empty() {
        return Err(IdentityError::InvalidStrategy(
            "Field path cannot be empty".to_string(),
        ));
    }
    path.split('.')
        .map(|segment| {
            if let Some(name) = segment.strip_suffix("[]") {
                validate_field_name_segment(name)?;
                Ok(PathSegment::Each(name.to_string()))
            } else {
                validate_field_name_segment(segment)?;
                Ok(PathSegment::Field(segment.to_string()))
            }
        })
        .collect()
}

fn parse_entity_record_id(value: &Value) -> Option<RecordId> {
    serde_json::from_value::<RecordId>(value.clone())
        .ok()
        .filter(|id| id.table() == TableName::Entity)
}

fn replacement_value_preserving_shape(original: &Value, survivor: &RecordId) -> Value {
    match original {
        Value::Object(map) => {
            let mut replacement = map.clone();
            if replacement.contains_key("tb") {
                replacement.insert("tb".to_string(), Value::String("entity".to_string()));
            }
            if replacement.contains_key("table") {
                replacement.insert("table".to_string(), Value::String("entity".to_string()));
            }
            if replacement.contains_key("id") {
                replacement.insert("id".to_string(), Value::String(survivor.id().to_string()));
            } else {
                replacement.insert("id".to_string(), Value::String(survivor.to_string()));
            }
            Value::Object(replacement)
        }
        _ => serde_json::to_value(survivor).unwrap_or_else(|_| Value::String(survivor.to_string())),
    }
}

fn rewrite_leaf_value(
    value: &mut Value,
    decl: &ReferenceDeclaration,
    merged_ids: &[RecordId],
    survivor_id: &RecordId,
) -> bool {
    match decl.cardinality {
        ReferenceCardinality::One => {
            let Some(current) = parse_entity_record_id(value) else {
                return false;
            };
            if !merged_ids.iter().any(|id| id == &current) {
                return false;
            }
            *value = replacement_value_preserving_shape(value, survivor_id);
            true
        }
        ReferenceCardinality::Many => {
            let Value::Array(items) = value else {
                return false;
            };

            let mut changed = false;
            for item in items.iter_mut() {
                let Some(current) = parse_entity_record_id(item) else {
                    continue;
                };
                if merged_ids.iter().any(|id| id == &current) {
                    *item = replacement_value_preserving_shape(item, survivor_id);
                    changed = true;
                }
            }

            let mut seen = HashSet::new();
            let original_len = items.len();
            items.retain(|item| {
                parse_entity_record_id(item).is_none_or(|id| seen.insert(id.to_string()))
            });
            changed || items.len() != original_len
        }
    }
}

fn rewrite_segments(
    value: &mut Value,
    segments: &[PathSegment],
    decl: &ReferenceDeclaration,
    merged_ids: &[RecordId],
    survivor_id: &RecordId,
) -> IdentityResult<bool> {
    if segments.is_empty() {
        return Ok(rewrite_leaf_value(value, decl, merged_ids, survivor_id));
    }

    match &segments[0] {
        PathSegment::Field(name) => {
            let Value::Object(map) = value else {
                return Ok(false);
            };
            let Some(next) = map.get_mut(name) else {
                return Ok(false);
            };
            rewrite_segments(next, &segments[1..], decl, merged_ids, survivor_id)
        }
        PathSegment::Each(name) => {
            let Value::Object(map) = value else {
                return Ok(false);
            };
            let Some(Value::Array(items)) = map.get_mut(name) else {
                return Ok(false);
            };
            let mut changed = false;
            for item in items.iter_mut() {
                changed |= rewrite_segments(item, &segments[1..], decl, merged_ids, survivor_id)?;
            }
            Ok(changed)
        }
    }
}

fn is_builtin_storage_table(table_name: &str) -> bool {
    TableName::from_str(table_name).is_ok()
        || table_name.starts_with("emb__")
        || table_name.starts_with("source_")
        || table_name.starts_with("canonical_")
        || table_name.starts_with("work_")
        || table_name.starts_with("erasure_")
        || matches!(
            table_name,
            "migration_history"
                | "recall_durability"
                | "fact"
                | "publication_schema"
                | "provider_call_receipt"
                | "outbox_checkpoint"
                | "outbox_sequence"
                | "space_grant"
                | "restore_barrier"
                | "entity_profile_embedding"
                // Private core controls are not consumer reference tables.
                // A merge must neither rewrite the payload-free physical fence
                // nor relabel a standing delegation's immutable authority.
                | "native_read_fence"
                | "watch_delegation"
        )
}

async fn ensure_declared_extension_tables(
    storage: &StorageClient,
    registry: &ReferenceRegistry,
) -> IdentityResult<()> {
    let undeclared =
        find_undeclared_extension_table(storage.list_tables().await?.into_iter(), registry);

    if let Some(table_name) = undeclared {
        return Err(IdentityError::MergeConflict(format!(
            "Extension table `{table_name}` exists in storage but is not declared by the loaded extension manifests"
        )));
    }

    Ok(())
}

fn find_undeclared_extension_table<I>(
    table_names: I,
    registry: &ReferenceRegistry,
) -> Option<String>
where
    I: IntoIterator<Item = String>,
{
    table_names
        .into_iter()
        .filter(|table_name| !is_builtin_storage_table(table_name))
        .find(|table_name| !registry.owns_table(table_name))
}
/// Rewrites declared entity references in an opaque JSON row for embedded
/// extension-table merge handling.
///
/// # Errors
///
/// Returns an error if the declaration contains an invalid field path.
pub fn rewrite_declared_entity_refs_in_json(
    row: &mut Value,
    decl: &ReferenceDeclaration,
    merged_ids: &[RecordId],
    survivor_id: &RecordId,
) -> IdentityResult<bool> {
    if !decl.should_rewrite() {
        return Ok(false);
    }
    let segments = parse_field_path_segments(&decl.field_path)?;
    rewrite_segments(row, &segments, decl, merged_ids, survivor_id)
}

/// Rewrites extension-owned rows inside one embedded transaction according to
/// the loaded reference registry.
///
/// # Errors
/// Returns an error if a declared field path is invalid.
pub fn rewrite_extension_rows_in_mutation(
    mutation: &mut EmbeddedMutation<'_>,
    space_id: &str,
    registry: &ReferenceRegistry,
    merged_ids: &[RecordId],
    survivor_id: &RecordId,
) -> IdentityResult<usize> {
    if let Some(table_name) =
        find_undeclared_extension_table(mutation.extension_table_names(), registry)
    {
        return Err(IdentityError::MergeConflict(format!(
            "Extension table `{table_name}` is present in embedded state but not declared by the loaded extension manifests"
        )));
    }

    let mut rewritten_rows = 0usize;
    for table_name in registry.owned_tables_without_rewrite_declarations() {
        for (row_key, row) in mutation
            .scan_extension_rows(table_name)
            .map_err(StorageError::from)?
        {
            let Some(row_space_id) = row.get("space_id").and_then(Value::as_str) else {
                return Err(IdentityError::MergeConflict(format!(
                    "Extension row `{table_name}:{row_key}` is missing a string `space_id` and cannot be safely rewritten during merge"
                )));
            };
            if row_space_id == space_id {
                return Err(IdentityError::MergeConflict(format!(
                    "Extension table `{table_name}` contains rows in space `{space_id}` but has no rewrite declarations; declare entity refs before merging"
                )));
            }
        }
    }

    for table_name in registry.tables() {
        let declarations: Vec<_> = registry
            .get_table_refs(table_name)
            .iter()
            .filter(|decl| decl.should_rewrite())
            .cloned()
            .collect();
        if declarations.is_empty() {
            continue;
        }

        for (row_key, mut row) in mutation
            .scan_extension_rows(table_name)
            .map_err(StorageError::from)?
        {
            let Some(row_space_id) = row.get("space_id").and_then(Value::as_str) else {
                return Err(IdentityError::MergeConflict(format!(
                    "Extension row `{table_name}:{row_key}` is missing a string `space_id` and cannot be safely rewritten during merge"
                )));
            };
            if row_space_id != space_id {
                continue;
            }

            let mut changed = false;
            for decl in &declarations {
                changed |=
                    rewrite_declared_entity_refs_in_json(&mut row, decl, merged_ids, survivor_id)?;
            }
            if changed {
                mutation
                    .replace_extension_row(table_name, &row_key, row)
                    .map_err(StorageError::from)?;
                rewritten_rows += 1;
            }
        }
    }

    Ok(rewritten_rows)
}

/// Metadata key carrying Angel protectee attribution on docs and entities.
#[cfg(test)]
const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";

/// Reads `metadata.angel_protectees` as a list of string ids.
///
/// Missing keys, non-array values, and non-string elements yield an empty /
/// filtered list rather than an error.
#[cfg(test)]
fn angel_protectees_from_metadata(metadata: &Value) -> Vec<String> {
    metadata
        .get(ANGEL_PROTECTEES_METADATA_KEY)
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Unions `ids` into `metadata.angel_protectees` (deduped + sorted for
/// determinism).
///
/// Returns `true` when the stored value changed so callers can skip
/// persisting no-op unions.
#[cfg(test)]
fn union_angel_protectees_into_metadata(metadata: &mut Value, ids: &[String]) -> bool {
    if ids.is_empty() {
        return false;
    }

    let existing = angel_protectees_from_metadata(metadata);
    let mut union: Vec<String> = existing.iter().chain(ids.iter()).cloned().collect();
    union.sort();
    union.dedup();
    if union == existing {
        return false;
    }

    if !metadata.is_object() {
        *metadata = Value::Object(serde_json::Map::new());
    }
    if let Some(map) = metadata.as_object_mut() {
        map.insert(
            ANGEL_PROTECTEES_METADATA_KEY.to_string(),
            Value::Array(union.into_iter().map(Value::String).collect()),
        );
    }
    true
}

/// Prepare a complete explicit equivalence without mutating canonical records.
/// The caller authorizes all old/proposed envelopes before committing.
///
/// # Errors
/// Returns an error for missing identities, undeclared or malformed references,
/// incomplete authorization envelopes, or failed storage reads.
pub async fn prepare_entity_merge(
    storage: &StorageClient,
    space_id: &str,
    entity_a: &RecordId,
    entity_b: &RecordId,
    selection: elephant_storage::entity_merge_repository::MergeSelection,
    actor: &str,
    registry: &ReferenceRegistry,
) -> IdentityResult<elephant_storage::entity_merge_repository::PreparedEntityMerge> {
    prepare_entity_merge_group(
        storage,
        space_id,
        entity_a,
        std::slice::from_ref(entity_b),
        selection,
        actor,
        registry,
    )
    .await
}

/// Prepare an equivalence over an existing maintenance overlay, without a
/// second epoch or commit.
///
/// Install registry epoch guards before capturing the
/// outer plan. Consume the returned bundle with `into_prepared_plan` and let the
/// original caller commit with its original attempt.
///
/// # Errors
/// Returns an error for stale original snapshots, invalid references, or
/// unrepresentable authorization envelopes.
#[expect(
    clippy::too_many_arguments,
    reason = "Keep the merge API explicit about the original plan, registry, actor, and record selection."
)]
pub async fn prepare_entity_merge_on_plan(
    storage: &StorageClient,
    space_id: &str,
    entity_a: &RecordId,
    entity_b: &RecordId,
    selection: elephant_storage::entity_merge_repository::MergeSelection,
    actor: &str,
    registry: &ReferenceRegistry,
    plan: &elephant_storage::canonical_commit::CanonicalPlan,
) -> IdentityResult<elephant_storage::entity_merge_repository::PreparedEntityMerge> {
    prepare_entity_merge_group_on_plan(
        storage,
        space_id,
        entity_a,
        std::slice::from_ref(entity_b),
        selection,
        actor,
        registry,
        Some(plan),
    )
    .await
}

async fn prepare_entity_merge_group(
    storage: &StorageClient,
    space_id: &str,
    entity_a: &RecordId,
    others: &[RecordId],
    selection: elephant_storage::entity_merge_repository::MergeSelection,
    actor: &str,
    registry: &ReferenceRegistry,
) -> IdentityResult<elephant_storage::entity_merge_repository::PreparedEntityMerge> {
    prepare_entity_merge_group_on_plan(
        storage, space_id, entity_a, others, selection, actor, registry, None,
    )
    .await
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep the original plan and registry explicit alongside the merge selection and actor."
)]
async fn prepare_entity_merge_group_on_plan(
    storage: &StorageClient,
    space_id: &str,
    entity_a: &RecordId,
    others: &[RecordId],
    selection: elephant_storage::entity_merge_repository::MergeSelection,
    actor: &str,
    registry: &ReferenceRegistry,
    prior_plan: Option<&elephant_storage::canonical_commit::CanonicalPlan>,
) -> IdentityResult<elephant_storage::entity_merge_repository::PreparedEntityMerge> {
    use elephant_storage::canonical_extension::{
        load_extension_rows, CanonicalExtensionEffect, NativeExtensionValue,
    };
    let mut tables: Vec<_> = registry.tables().map(str::to_owned).collect();
    tables.sort();
    // Shared sweep callers install registry guards before capturing the outer
    // epoch. Preparation over that plan must remain read-only, including DDL.
    if prior_plan.is_none() {
        elephant_storage::canonical_commit::ensure_extension_epoch_guards(storage, &tables).await?;
    }
    let mut prepared = elephant_storage::EntityMergeRepository::new(storage.clone())
        .prepare_merge_group_on_plan(space_id, entity_a, others, selection, actor, prior_plan)
        .await?;
    ensure_declared_extension_tables(storage, registry).await?;
    let mut effects = Vec::new();
    for table in tables {
        let declarations = registry.get_table_refs(&table);
        let live = load_extension_rows(storage, &table).await?;
        let rows = extension_rows_on_plan(&table, live, prior_plan)?;
        for before in rows {
            let NativeExtensionValue::Object(object) = &before else {
                return Err(IdentityError::MergeConflict(
                    "extension row must be an object".into(),
                ));
            };
            let Some(NativeExtensionValue::Strand(row_space)) = object.get("space_id") else {
                return Err(IdentityError::MergeConflict(
                    "extension row lacks its owning space".into(),
                ));
            };
            if row_space.0 != space_id {
                continue;
            }
            if declarations.is_empty() {
                return Err(IdentityError::MergeConflict(format!(
                    "extension table {table} has no declared identity references"
                )));
            }
            let Some(NativeExtensionValue::Thing(id)) = object.get("id") else {
                return Err(IdentityError::MergeConflict(
                    "extension row lacks native identity".into(),
                ));
            };
            let id = id.clone();
            let mut after = before.clone();
            let mut changed = false;
            for declaration in declarations {
                if declaration.should_rewrite() {
                    changed |= rewrite_declared_entity_refs_native(
                        &mut after,
                        declaration,
                        &prepared.outcome().merged,
                        &prepared.outcome().survivor.base.id,
                    )?;
                }
            }
            if changed {
                effects.push(CanonicalExtensionEffect {
                    id,
                    before: Some(before),
                    after: Some(after),
                });
            }
        }
    }
    prepared.attach_extensions(effects).await?;
    Ok(prepared)
}

fn extension_rows_on_plan(
    table: &str,
    live: Vec<elephant_storage::canonical_extension::NativeExtensionValue>,
    prior: Option<&elephant_storage::canonical_commit::CanonicalPlan>,
) -> IdentityResult<Vec<elephant_storage::canonical_extension::NativeExtensionValue>> {
    use elephant_storage::canonical_extension::NativeExtensionValue as Native;
    let mut rows = std::collections::BTreeMap::new();
    for row in live {
        let Native::Object(object) = &row else {
            return Err(IdentityError::MergeConflict(
                "extension row must be an object".into(),
            ));
        };
        let Some(Native::Thing(id)) = object.get("id") else {
            return Err(IdentityError::MergeConflict(
                "extension row lacks native identity".into(),
            ));
        };
        rows.insert(id.to_string(), row);
    }
    if let Some(plan) = prior {
        for effect in plan
            .extension_effects
            .iter()
            .filter(|effect| effect.id.tb == table)
        {
            let current = rows.remove(&effect.id.to_string());
            if current != effect.before {
                return Err(IdentityError::MergeConflict(
                    "extension changed from original maintenance snapshot".into(),
                ));
            }
            if let Some(after) = &effect.after {
                rows.insert(effect.id.to_string(), after.clone());
            }
        }
    }
    Ok(rows.into_values().collect())
}

/// Trusted direct callers use the same canonical bundle as public adapters.
/// Public transports must use `prepare_entity_merge` and preflight every row.
///
/// # Errors
/// Returns an error when preparation fails, an observed input changes, source
/// support is no longer eligible, or the atomic canonical transaction fails.
pub async fn apply_entity_merge_rewrites(
    storage: &StorageClient,
    space_id: &str,
    survivor_id: &RecordId,
    merged_entity_ids: &[RecordId],
    _now: DateTime<Utc>,
    registry: &ReferenceRegistry,
) -> IdentityResult<()> {
    if merged_entity_ids.is_empty() {
        return Ok(());
    }
    prepare_entity_merge_group(
        storage,
        space_id,
        survivor_id,
        merged_entity_ids,
        elephant_storage::entity_merge_repository::MergeSelection::KeepA,
        "identity.explicit_merge",
        registry,
    )
    .await?
    .commit()
    .await?;
    Ok(())
}

#[expect(
    clippy::too_many_lines,
    reason = "Keep the native reference shapes and recursive rewrite rules together in the prepared merge."
)]
fn rewrite_declared_entity_refs_native(
    row: &mut elephant_storage::canonical_extension::NativeExtensionValue,
    declaration: &ReferenceDeclaration,
    merged: &[RecordId],
    survivor: &RecordId,
) -> IdentityResult<bool> {
    use elephant_storage::canonical_extension::NativeExtensionValue as Native;
    fn one(value: &mut Native, merged: &[RecordId], survivor: &RecordId) -> IdentityResult<bool> {
        let current = match value {
            Native::Thing(id) if id.tb == "entity" => {
                elephant_storage::surreal::record_id_from_thing(id)
                    .map_err(|error| IdentityError::MergeConflict(error.to_string()))?
            }
            Native::Strand(id) => {
                id.0.parse::<RecordId>()
                    .map_err(|error| IdentityError::MergeConflict(error.to_string()))?
            }
            Native::Object(object) => {
                let Some(Native::Strand(table)) = object.get("tb").or_else(|| object.get("table"))
                else {
                    return Err(IdentityError::MergeConflict(
                        "entity reference object lacks a table".into(),
                    ));
                };
                let Some(Native::Strand(id)) = object.get("id") else {
                    return Err(IdentityError::MergeConflict(
                        "entity reference object lacks an identity".into(),
                    ));
                };
                if table.0 != "entity" {
                    return Err(IdentityError::MergeConflict(
                        "entity reference object names a different table".into(),
                    ));
                }
                elephant_storage::parse_id(&id.0, TableName::Entity)?
            }
            Native::None | Native::Null => return Ok(false),
            _ => {
                return Err(IdentityError::MergeConflict(
                    "declared entity reference has an invalid native shape".into(),
                ))
            }
        };
        if current.table() != TableName::Entity {
            return Err(IdentityError::MergeConflict(
                "declared entity reference names a different table".into(),
            ));
        }
        if !merged.contains(&current) {
            return Ok(false);
        }
        match value {
            Native::Object(object) => {
                let qualified =
                    matches!(object.get("id"), Some(Native::Strand(id)) if id.0.contains(':'));
                object.insert(
                    "id".into(),
                    Native::from(if qualified {
                        survivor.to_string()
                    } else {
                        survivor.id().to_string()
                    }),
                );
            }
            Native::Thing(_) => *value = Native::Thing(thing_from_record_id(survivor)),
            _ => *value = Native::from(survivor.to_string()),
        }
        Ok(true)
    }
    #[expect(
        clippy::mutable_key_type,
        reason = "Cloned native values stay immutable in this set; regex ordering compares pattern text, not cache state."
    )]
    fn leaf(
        value: &mut Native,
        many: bool,
        merged: &[RecordId],
        survivor: &RecordId,
    ) -> IdentityResult<bool> {
        if !many {
            return one(value, merged, survivor);
        }
        if matches!(value, Native::None | Native::Null) {
            return Ok(false);
        }
        let Native::Array(items) = value else {
            return Err(IdentityError::MergeConflict(
                "declared reference array has an invalid shape".into(),
            ));
        };
        let mut changed = false;
        for value in items.iter_mut() {
            changed |= one(value, merged, survivor)?;
        }
        let mut seen = std::collections::BTreeSet::new();
        let old_len = items.len();
        items.0.retain(|value| seen.insert(value.clone()));
        Ok(changed || old_len != items.len())
    }
    fn walk(
        value: &mut Native,
        path: &[PathSegment],
        declaration: &ReferenceDeclaration,
        merged: &[RecordId],
        survivor: &RecordId,
    ) -> IdentityResult<bool> {
        let Some(segment) = path.first() else {
            return leaf(
                value,
                declaration.cardinality == ReferenceCardinality::Many,
                merged,
                survivor,
            );
        };
        if matches!(value, Native::None | Native::Null) && !declaration.required {
            return Ok(false);
        }
        let Native::Object(object) = value else {
            return Err(IdentityError::MergeConflict(
                "declared reference path traverses a non-object".into(),
            ));
        };
        let (name, repeated) = match segment {
            PathSegment::Field(name) => (name, false),
            PathSegment::Each(name) => (name, true),
        };
        let Some(next) = object.get_mut(name) else {
            return if declaration.required {
                Err(IdentityError::MergeConflict(
                    "required extension reference is absent".into(),
                ))
            } else {
                Ok(false)
            };
        };
        if !repeated {
            return walk(next, &path[1..], declaration, merged, survivor);
        }
        if path.len() == 1 {
            return leaf(next, true, merged, survivor);
        }
        let Native::Array(items) = next else {
            return Err(IdentityError::MergeConflict(
                "repeated reference path is not an array".into(),
            ));
        };
        let mut changed = false;
        for item in items.iter_mut() {
            changed |= walk(item, &path[1..], declaration, merged, survivor)?;
        }
        Ok(changed)
    }
    walk(
        row,
        &parse_field_path_segments(&declaration.field_path)?,
        declaration,
        merged,
        survivor,
    )
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
    use elephant_storage::{EmbeddedStore, Repository};
    use elephant_types::ids::TableName;

    async fn register_merge_space(storage: &StorageClient, space: &str) {
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: elephant_types::records::base::BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    space.into(),
                    "fixture".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
    }

    #[test]
    fn test_reference_declaration() {
        let decl = ReferenceDeclaration::new("sampleapp_todo", "related_to")
            .with_many()
            .required();

        assert_eq!(decl.table, "sampleapp_todo");
        assert_eq!(decl.field_path, "related_to");
        assert_eq!(decl.cardinality, ReferenceCardinality::Many);
        assert!(decl.should_rewrite());
        assert!(decl.required);
    }

    #[test]
    fn test_reference_declaration_canonicalize() {
        let decl = ReferenceDeclaration::new("sampleapp_conversation", "participant")
            .with_on_merge(OnMergeBehavior::Canonicalize);

        assert!(!decl.should_rewrite());
    }

    #[test]
    fn test_reference_registry() {
        let mut registry = ReferenceRegistry::new();
        assert!(registry.is_empty());

        registry.register_table("misc");
        registry.register(ReferenceDeclaration::new("todo", "entity"));
        registry.register(ReferenceDeclaration::new("todo", "participants").with_many());
        registry.register(ReferenceDeclaration::new("alert", "subject"));

        assert_eq!(registry.len(), 3);
        assert_eq!(registry.get_table_refs("todo").len(), 2);
        assert_eq!(registry.get_table_refs("alert").len(), 1);
        assert_eq!(registry.get_table_refs("unknown").len(), 0);
        assert!(registry.owns_table("misc"));
        assert!(registry.owns_table("todo"));
    }

    #[test]
    fn test_rewrite_refs_filter() {
        let mut registry = ReferenceRegistry::new();

        registry.register(ReferenceDeclaration::new("todo", "entity"));
        registry.register(
            ReferenceDeclaration::new("conv", "participant")
                .with_on_merge(OnMergeBehavior::Canonicalize),
        );

        let rewrite_count = registry.rewrite_refs().count();
        assert_eq!(rewrite_count, 1);
    }

    #[test]
    fn test_merge_request() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);

        let request = MergeRequest::new(a.clone(), b.clone(), MergeStrategy::KeepA, "test_actor")
            .with_reason("Same person")
            .with_run_id("batch_001");

        assert_eq!(request.actor, "test_actor");
        assert_eq!(request.reason, Some("Same person".to_string()));
        assert_eq!(request.run_id, Some("batch_001".to_string()));

        let (survivor, merged) = request.resolve_survivor();
        assert_eq!(survivor, a);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], b);
    }

    #[test]
    fn test_merge_request_keep_b() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);

        let request = MergeRequest::new(a.clone(), b.clone(), MergeStrategy::KeepB, "actor");

        let (survivor, merged) = request.resolve_survivor();
        assert_eq!(survivor, b);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], a);
    }

    #[test]
    fn test_merge_request_combine() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);

        let request = MergeRequest::new(a.clone(), b.clone(), MergeStrategy::Combine, "actor");

        let (_survivor, merged) = request.resolve_survivor();
        assert_eq!(merged.len(), 2);
        assert!(merged.contains(&a));
        assert!(merged.contains(&b));
    }

    #[test]
    fn test_to_lineage() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);

        let request = MergeRequest::new(a.clone(), b.clone(), MergeStrategy::KeepA, "actor");

        let lineages = request.to_lineage(&a);
        assert_eq!(lineages.len(), 1);
        assert_eq!(lineages[0].from, b);
        assert_eq!(lineages[0].into, a);
        assert_eq!(lineages[0].strategy, MergeStrategy::KeepA);
    }

    #[test]
    fn test_validate_merge_same_entity() {
        let a = RecordId::new_v4(TableName::Entity);

        let result = validate_merge(&a, &a);
        assert!(result.is_err());
        assert!(matches!(result, Err(IdentityError::MergeConflict(_))));
    }

    #[test]
    fn test_validate_merge_non_entity() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Rel);

        let result = validate_merge(&a, &b);
        assert!(result.is_err());
        assert!(matches!(result, Err(IdentityError::InvalidStrategy(_))));
    }

    #[test]
    fn test_validate_merge_success() {
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);

        let result = validate_merge(&a, &b);
        assert!(result.is_ok());
    }

    #[test]
    fn test_generate_rewrite_query_one() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("rel", "subject");

        let query = generate_rewrite_query(&decl, &old_id, &new_id).expect("valid query");

        assert!(query.contains("UPDATE rel"));
        assert!(query.contains("SET subject ="));
        assert!(query.contains("WHERE subject ="));
    }

    #[test]
    fn test_generate_rewrite_query_many() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("rel", "subjects").with_many();

        let query = generate_rewrite_query(&decl, &old_id, &new_id).expect("valid query");

        assert!(query.contains("UPDATE rel"));
        assert!(query.contains("array::distinct"));
        assert!(query.contains("CONTAINS"));
    }

    #[test]
    fn test_generate_scoped_rewrite_query_adds_space_filter() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("sampleapp_todo", "owner");

        let query =
            generate_scoped_rewrite_query(&decl, "space-a", &old_id, &new_id).expect("query");

        assert!(query.contains("space_id = 'space-a'"));
        assert!(query.contains("owner ="));
    }

    #[test]
    fn test_generate_rewrite_query_invalid_table() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("'; DROP TABLE users; --", "field");

        let result = generate_rewrite_query(&decl, &old_id, &new_id);
        assert!(result.is_err(), "SQL injection attempt should be rejected");
    }

    #[test]
    fn test_generate_rewrite_query_invalid_field() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("rel", "field; DROP TABLE");

        let result = generate_rewrite_query(&decl, &old_id, &new_id);
        assert!(result.is_err(), "SQL injection attempt should be rejected");
    }

    #[test]
    fn test_find_undeclared_extension_table_rejects_unknown_table() {
        let undeclared = find_undeclared_extension_table(
            vec![
                "entity".to_string(),
                "migration_history".to_string(),
                "sampleapp_todo".to_string(),
            ],
            &ReferenceRegistry::new(),
        );
        assert_eq!(undeclared.as_deref(), Some("sampleapp_todo"));
    }

    #[test]
    fn test_find_undeclared_extension_table_ignores_embedding_partitions() {
        let undeclared = find_undeclared_extension_table(
            vec!["emb__default__L0__3072".to_string()],
            &ReferenceRegistry::new(),
        );
        assert!(undeclared.is_none());
    }

    #[test]
    fn private_core_controls_do_not_admit_similarly_named_extension_tables() {
        let registry = ReferenceRegistry::new();
        assert!(find_undeclared_extension_table(
            ["native_read_fence".into(), "watch_delegation".into()],
            &registry,
        )
        .is_none());
        for unknown in [
            "native_read_fence_custom",
            "watch_delegation_custom",
            "native_custom",
            "watch_custom",
            "sampleapp_todo",
        ] {
            assert_eq!(
                find_undeclared_extension_table(
                    [
                        "native_read_fence".into(),
                        "watch_delegation".into(),
                        unknown.into()
                    ],
                    &registry,
                ),
                Some(unknown.into()),
            );
        }
    }

    #[test]
    fn test_rewrite_declared_entity_refs_in_json_scalar() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let mut row = serde_json::json!({ "owner": old_id.to_string() });

        let changed = rewrite_declared_entity_refs_in_json(
            &mut row,
            &ReferenceDeclaration::new("sampleapp_todo", "owner"),
            std::slice::from_ref(&old_id),
            &survivor_id,
        )
        .unwrap();

        assert!(changed);
        assert_eq!(row["owner"], Value::String(survivor_id.to_string()));
    }

    #[test]
    fn test_rewrite_declared_entity_refs_in_json_many_dedupes() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let mut row = serde_json::json!({
            "secondary": [old_id.to_string(), survivor_id.to_string()]
        });

        let changed = rewrite_declared_entity_refs_in_json(
            &mut row,
            &ReferenceDeclaration::new("sampleapp_todo", "secondary").with_many(),
            std::slice::from_ref(&old_id),
            &survivor_id,
        )
        .unwrap();

        assert!(changed);
        assert_eq!(
            row["secondary"],
            serde_json::json!([survivor_id.to_string()])
        );
    }

    #[test]
    fn test_rewrite_declared_entity_refs_in_json_nested_array_path() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let mut row = serde_json::json!({
            "data": {
                "refs": [
                    { "entity": old_id.to_string() },
                    { "entity": survivor_id.to_string() }
                ]
            }
        });

        let changed = rewrite_declared_entity_refs_in_json(
            &mut row,
            &ReferenceDeclaration::new("sampleapp_note", "data.refs[].entity"),
            std::slice::from_ref(&old_id),
            &survivor_id,
        )
        .unwrap();

        assert!(changed);
        assert_eq!(
            row["data"]["refs"][0]["entity"],
            Value::String(survivor_id.to_string())
        );
    }

    #[test]
    fn test_rewrite_declared_entity_refs_in_json_skips_canonicalize() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let mut row = serde_json::json!({ "owner": old_id.to_string() });

        let changed = rewrite_declared_entity_refs_in_json(
            &mut row,
            &ReferenceDeclaration::new("sampleapp_todo", "owner")
                .with_on_merge(OnMergeBehavior::Canonicalize),
            std::slice::from_ref(&old_id),
            &survivor_id,
        )
        .unwrap();

        assert!(!changed);
        assert_eq!(row["owner"], Value::String(old_id.to_string()));
    }

    #[test]
    fn test_rewrite_extension_rows_in_mutation_rejects_owned_tables_without_rewrite_declarations() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let store = EmbeddedStore::new();
        let mut registry = ReferenceRegistry::new();
        registry.register(ReferenceDeclaration::new("sampleapp_todo", "owner"));
        registry.register_table("sampleapp_misc");

        let error = tokio_test::block_on(store.mutate(|mutation| {
            mutation.upsert_extension_row(
                "sampleapp_todo",
                serde_json::json!({
                    "id": "sampleapp_todo:todo-1",
                    "space_id": "space",
                    "owner": old_id.to_string()
                }),
            )?;
            mutation.upsert_extension_row(
                "sampleapp_misc",
                serde_json::json!({
                    "id": "sampleapp_misc:misc-1",
                    "space_id": "space",
                    "note": format!("keep {}", old_id)
                }),
            )?;

            rewrite_extension_rows_in_mutation(
                mutation,
                "space",
                &registry,
                std::slice::from_ref(&old_id),
                &survivor_id,
            )
            .map_err(|err| StorageError::QueryError(err.to_string()))?;
            Ok::<_, StorageError>(())
        }))
        .expect_err("owned extension tables without rewrite declarations should fail fast");
        assert!(error.to_string().contains("has no rewrite declarations"));
    }

    #[test]
    fn test_rewrite_extension_rows_in_mutation_rejects_undeclared_tables() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let store = EmbeddedStore::new();
        let mut registry = ReferenceRegistry::new();
        registry.register(ReferenceDeclaration::new("sampleapp_todo", "owner"));

        let error = tokio_test::block_on(store.mutate(|mutation| {
            mutation.upsert_extension_row(
                "sampleapp_misc",
                serde_json::json!({
                    "id": "sampleapp_misc:misc-1",
                    "space_id": "space",
                    "note": format!("keep {old_id}")
                }),
            )?;

            rewrite_extension_rows_in_mutation(
                mutation,
                "space",
                &registry,
                std::slice::from_ref(&old_id),
                &survivor_id,
            )
            .map_err(|err| StorageError::QueryError(err.to_string()))?;
            Ok::<_, StorageError>(())
        }))
        .expect_err("undeclared extension tables should fail fast");

        assert!(error
            .to_string()
            .contains("not declared by the loaded extension manifests"));
    }

    #[test]
    fn test_rewrite_extension_rows_in_mutation_respects_space_id() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let survivor_id = RecordId::new_v4(TableName::Entity);
        let store = EmbeddedStore::new();
        let mut registry = ReferenceRegistry::new();
        registry.register(ReferenceDeclaration::new("sampleapp_todo", "owner"));

        tokio_test::block_on(store.mutate(|mutation| {
            mutation.upsert_extension_row(
                "sampleapp_todo",
                serde_json::json!({
                    "id": "sampleapp_todo:todo-space-a",
                    "space_id": "space-a",
                    "owner": old_id.to_string()
                }),
            )?;
            mutation.upsert_extension_row(
                "sampleapp_todo",
                serde_json::json!({
                    "id": "sampleapp_todo:todo-space-b",
                    "space_id": "space-b",
                    "owner": old_id.to_string()
                }),
            )?;

            let rewritten = rewrite_extension_rows_in_mutation(
                mutation,
                "space-a",
                &registry,
                std::slice::from_ref(&old_id),
                &survivor_id,
            )
            .map_err(|err| StorageError::QueryError(err.to_string()))?;
            assert_eq!(rewritten, 1);
            Ok::<_, StorageError>(())
        }))
        .unwrap();

        let rows = tokio_test::block_on(store.mutate(|mutation| {
            mutation
                .scan_extension_rows("sampleapp_todo")
                .map_err(Into::into)
        }))
        .unwrap();
        assert_eq!(rows.len(), 2);

        let mut owners: HashMap<String, Value> = rows
            .into_iter()
            .map(|(_, row)| {
                (
                    row["space_id"].as_str().unwrap_or_default().to_string(),
                    row["owner"].clone(),
                )
            })
            .collect();

        assert_eq!(
            owners.remove("space-a"),
            Some(Value::String(survivor_id.to_string()))
        );
        assert_eq!(
            owners.remove("space-b"),
            Some(Value::String(old_id.to_string()))
        );
    }

    #[test]
    fn test_generate_rewrite_query_nested_field() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("rel", "nested.field.path");

        let query = generate_rewrite_query(&decl, &old_id, &new_id).expect("valid query");
        assert!(query.contains("nested.field.path"));
    }

    #[test]
    fn test_generate_rewrite_query_nested_array_field() {
        let old_id = RecordId::new_v4(TableName::Entity);
        let new_id = RecordId::new_v4(TableName::Entity);
        let decl = ReferenceDeclaration::new("sampleapp_note", "data.refs[].entity");

        let query = generate_rewrite_query(&decl, &old_id, &new_id).expect("valid query");
        assert!(query.contains("data.refs[].entity"));
    }

    #[test]
    fn test_generate_merge_tombstone() {
        let entity = RecordId::new_v4(TableName::Entity);
        let survivor = RecordId::new_v4(TableName::Entity);

        let query = generate_merge_tombstone(&entity, &survivor);

        assert!(query.contains("UPDATE"));
        assert!(query.contains("status = 'merged'"));
        assert!(query.contains("redirect_to ="));
    }

    #[test]
    fn test_union_angel_protectees_into_metadata_dedups_sorts_and_skips_noops() {
        let mut metadata = serde_json::json!({ "angel_protectees": ["child-b"] });

        assert!(union_angel_protectees_into_metadata(
            &mut metadata,
            &["child-a".to_string(), "child-b".to_string()]
        ));
        assert_eq!(
            metadata["angel_protectees"],
            serde_json::json!(["child-a", "child-b"])
        );

        // Already-present ids are a no-op (no churn).
        assert!(!union_angel_protectees_into_metadata(
            &mut metadata,
            &["child-a".to_string()]
        ));
        assert!(!union_angel_protectees_into_metadata(&mut metadata, &[]));
    }

    #[test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original embedded storage fixture lifetime through union rewrites and final survivor and merged-entity assertions."
    )]
    fn test_apply_entity_merge_rewrites_embedded_unions_angel_protectees() {
        use elephant_types::records::base::BaseFields;
        use elephant_types::records::entity::EntityType;

        fn make_entity(display_name: &str, protectees: &[&str]) -> Entity {
            let id = RecordId::new_v4(TableName::Entity);
            let metadata = if protectees.is_empty() {
                Value::Object(serde_json::Map::new())
            } else {
                serde_json::json!({ "angel_protectees": protectees })
            };
            Entity {
                base: BaseFields::new(id, "space".to_string(), "test".to_string()),
                entity_type: EntityType::new("person"),
                display_name: display_name.to_string(),
                aliases: vec![],
                summary: None,
                handles: vec![],
                identity_keys: vec![display_name.to_lowercase()],
                risk_level: None,
                trust_level: None,
                known_safe: false,
                first_seen: None,
                last_seen: None,
                status: EntityStatus::Active,
                redirect_to: None,
                metadata,
            }
        }

        let storage = tokio_test::block_on(StorageClient::new_embedded()).unwrap();
        tokio_test::block_on(register_merge_space(&storage, "space"));
        let store = storage.embedded_store().unwrap();
        let survivor = make_entity("Alice Chen", &["child-a"]);
        let merged = make_entity("Alice", &["child-b", "child-a"]);
        let survivor_id = survivor.base.id.clone();
        let merged_id = merged.base.id.clone();

        tokio_test::block_on(async {
            store.create(survivor).await?;
            store.create(merged).await?;
            Ok::<_, StorageError>(())
        })
        .unwrap();

        tokio_test::block_on(apply_entity_merge_rewrites(
            &storage,
            "space",
            &survivor_id,
            std::slice::from_ref(&merged_id),
            Utc::now(),
            &ReferenceRegistry::new(),
        ))
        .unwrap();

        let survivor_after =
            tokio_test::block_on(store.get::<Entity>("space", &survivor_id, false))
                .unwrap()
                .expect("survivor exists");
        assert_eq!(
            survivor_after.metadata["angel_protectees"],
            serde_json::json!(["child-a", "child-b"])
        );

        let merged_after = tokio_test::block_on(store.get::<Entity>("space", &merged_id, false))
            .unwrap()
            .expect("merged entity exists");
        assert_eq!(merged_after.status, EntityStatus::Merged);
    }

    #[test]
    fn test_core_entity_ref_tables() {
        // Verify the constant is set up correctly
        assert!(!CORE_ENTITY_REF_TABLES.is_empty());
        assert!(CORE_ENTITY_REF_TABLES.iter().any(|(t, _)| *t == "rel"));
        assert!(CORE_ENTITY_REF_TABLES
            .iter()
            .any(|(t, f)| *t == "attribute" && *f == "subject"));
    }

    #[test]
    fn test_generate_id_map_escapes_source_system() {
        let space_id = RecordId::new_v4(TableName::Entity);
        let legacy_id = RecordId::new_v4(TableName::Entity);
        let canonical_id = RecordId::new_v4(TableName::Entity);

        // Try SQL injection via source_system
        let query = generate_id_map_entry(
            &space_id,
            &legacy_id,
            &canonical_id,
            "test'; DROP TABLE id_map; --",
        );

        // The single quote should be escaped - look for the escaped pattern
        assert!(
            query.contains("test\\'; DROP"),
            "Single quote should be escaped in: {query}"
        );
        // Verify the unescaped injection pattern is not present
        // (look for the SQL breaking pattern of closing quote followed by semicolon)
        assert!(
            !query.contains("test'; DROP"),
            "Unescaped injection should not appear"
        );
    }

    #[test]
    fn test_escape_surql_string() {
        assert_eq!(escape_surql_string("normal"), "normal");
        assert_eq!(escape_surql_string("it's"), "it\\'s");
        assert_eq!(escape_surql_string("back\\slash"), "back\\\\slash");
        assert_eq!(escape_surql_string("both'and\\"), "both\\'and\\\\");
    }

    #[test]
    fn test_validate_identifier_valid() {
        assert!(validate_identifier("valid_name").is_ok());
        assert!(validate_identifier("_private").is_ok());
        assert!(validate_identifier("CamelCase").is_ok());
        assert!(validate_identifier("with123numbers").is_ok());
        assert!(validate_identifier("nested.path").is_ok());
    }

    #[test]
    fn test_validate_identifier_invalid() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("123starts_with_number").is_err());
        assert!(validate_identifier("has spaces").is_err());
        assert!(validate_identifier("semi;colon").is_err());
        assert!(validate_identifier("quote'mark").is_err());
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain original storage, repository and prepared-merge lifetimes through authorization, commit and all native extension post-commit assertions on both backends."
    )]
    async fn actual_extension_merge(embedded: bool) {
        use elephant_storage::canonical_extension::{
            load_extension_rows, NativeExtensionValue as Native,
        };
        use elephant_storage::{EntityRepository, StorageConfig};
        use elephant_types::records::{base::BaseFields, entity::EntityType};
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new(StorageConfig::default()).await
        }
        .unwrap();
        if !embedded {
            storage
                .query_checked(concat!(
                    include_str!("../../migrations/sql/031_source_publication.surql"),
                    "\n",
                    include_str!("../../migrations/sql/046_source_support_index_fence.surql")
                ))
                .await
                .unwrap();
        }
        register_merge_space(&storage, "home").await;
        let entities = EntityRepository::new(storage.clone());
        let make = |name: &str| {
            Entity::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Entity),
                    "home".into(),
                    "test".into(),
                ),
                EntityType::new("person"),
                name.into(),
            )
        };
        let a = entities.create(make("A")).await.unwrap();
        let b = entities.create(make("B")).await.unwrap();
        let row = serde_json::json!({"id":"sampleapp_note:note-1","space_id":"home",
            "security":a.base.security,"subjects":[],"subjects_state":"complete",
            "owner":b.base.id,"nested":{"related":[b.base.id,a.base.id],"untouched":{"empty":null}},"type":"consumer-kind"});
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| {
                    tx.upsert_extension_row("sampleapp_note", row.clone())?;
                    Ok(())
                })
                .await
                .unwrap();
        } else {
            let mut native = elephant_storage::surreal::json_to_surreal(row).unwrap();
            let Native::Object(object) = &mut native else {
                unreachable!()
            };
            object.remove("id");
            object.insert(
                "owner".into(),
                Native::Thing(thing_from_record_id(&b.base.id)),
            );
            storage
                .query_bind_checked(
                    "CREATE type::thing('sampleapp_note', 'note-1') CONTENT $row;",
                    std::collections::BTreeMap::<String, Native>::from([("row".into(), native)]),
                )
                .await
                .unwrap();
        }
        let before = load_extension_rows(&storage, "sampleapp_note")
            .await
            .unwrap()
            .remove(0);
        let mut registry = ReferenceRegistry::new();
        registry.register(ReferenceDeclaration::new("sampleapp_note", "owner"));
        registry
            .register(ReferenceDeclaration::new("sampleapp_note", "nested.related[]").with_many());
        let prepared = prepare_entity_merge(
            &storage,
            "home",
            &a.base.id,
            &b.base.id,
            elephant_storage::entity_merge_repository::MergeSelection::KeepA,
            "test",
            &registry,
        )
        .await
        .unwrap();
        assert!(prepared
            .authorizations()
            .unwrap()
            .iter()
            .any(|row| row.id.contains("note-1")));
        prepared.commit().await.unwrap();
        let after = load_extension_rows(&storage, "sampleapp_note")
            .await
            .unwrap()
            .remove(0);
        let Native::Object(before) = before else {
            unreachable!()
        };
        let Native::Object(after) = after else {
            unreachable!()
        };
        assert_eq!(after.get("type"), before.get("type"));
        let Native::Object(nested) = after.get("nested").unwrap() else {
            unreachable!()
        };
        let Native::Object(original_nested) = before.get("nested").unwrap() else {
            unreachable!()
        };
        assert_eq!(nested.get("untouched"), original_nested.get("untouched"));
        assert_eq!(
            nested.get("related"),
            Some(&Native::Array(
                vec![Native::from(a.base.id.to_string())].into()
            ))
        );
        if embedded {
            assert_eq!(
                after.get("owner"),
                Some(&Native::from(a.base.id.to_string()))
            );
        } else {
            assert_eq!(
                after.get("owner"),
                Some(&Native::Thing(thing_from_record_id(&a.base.id)))
            );
        }
    }

    #[test]
    fn native_reference_preparation_reads_prior_effects_and_keeps_before_images() {
        use elephant_storage::canonical_commit::CanonicalPlan;
        use elephant_storage::canonical_extension::{
            CanonicalExtensionEffect, NativeExtensionValue as Native,
        };
        let a = RecordId::new_v4(TableName::Entity);
        let b = RecordId::new_v4(TableName::Entity);
        let mut before = elephant_storage::surreal::json_to_surreal(serde_json::json!({
            "space_id":"home", "owner":b, "untouched":"original"
        }))
        .unwrap();
        let id = elephant_storage::surreal::json_to_surreal(serde_json::json!("note-1")).unwrap();
        // Obtain the exact native Thing without adding a direct driver dependency.
        let native_id = thing_from_record_id(&RecordId::new_v4(TableName::Entity));
        let mut native_id = native_id;
        native_id.tb = "sampleapp_note".into();
        let Native::Object(object) = &mut before else {
            unreachable!()
        };
        object.insert("id".into(), Native::Thing(native_id.clone()));
        object.insert("opaque".into(), id);
        let mut pending = before.clone();
        let Native::Object(object) = &mut pending else {
            unreachable!()
        };
        object.insert("untouched".into(), Native::from("prepared earlier"));
        let prior = CanonicalPlan {
            extension_effects: vec![CanonicalExtensionEffect {
                id: native_id,
                before: Some(before.clone()),
                after: Some(pending),
            }],
            ..CanonicalPlan::default()
        };
        let mut selected =
            extension_rows_on_plan("sampleapp_note", vec![before.clone()], Some(&prior)).unwrap();
        let declaration = ReferenceDeclaration::new("sampleapp_note", "owner");
        assert!(
            rewrite_declared_entity_refs_native(&mut selected[0], &declaration, &[b], &a).unwrap()
        );
        let Native::Object(after) = &selected[0] else {
            unreachable!()
        };
        assert_eq!(
            after.get("untouched"),
            Some(&Native::from("prepared earlier"))
        );
        assert_eq!(after.get("owner"), Some(&Native::from(a.to_string())));
        assert_eq!(prior.extension_effects[0].before.as_ref(), Some(&before));
        assert!(extension_rows_on_plan("sampleapp_note", vec![], Some(&prior)).is_err());
    }

    #[test]
    fn embedded_actual_canonical_extension_merge() {
        tokio_test::block_on(actual_extension_merge(true));
    }
    #[test]
    fn surreal_actual_canonical_extension_merge() {
        tokio_test::block_on(actual_extension_merge(false));
    }
}
