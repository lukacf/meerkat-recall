//! Identity resolver for executing entity merges.
//!
//! This module handles:
//! - Extension reference declarations
//! - Reference rewriting during merges
//! - Merge execution with transaction support

use chrono::{DateTime, Utc};
use elephant_storage::{
    surreal::thing_from_record_id, EmbeddedMutation, EmbeddedStore, EntityRepository, Repository,
    StorageClient, StorageError, TransactionExt,
};
use elephant_types::ids::TableName;
use elephant_types::records::assertion::Assertion;
use elephant_types::records::attribute::Attribute;
use elephant_types::records::doc::Doc;
use elephant_types::records::entity::EntityStatus;
use elephant_types::records::event::Event;
use elephant_types::records::identity::EntityIdentityKey;
use elephant_types::records::{Entity, Rel, SystemEvent};
use elephant_types::{Link, RecordId};
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

fn repoint_entity_links(
    subjects: &mut Vec<Link<Entity>>,
    merged_ids: &[RecordId],
    survivor_id: &RecordId,
) -> bool {
    let mut changed = false;
    for subject in subjects.iter_mut() {
        if merged_ids.iter().any(|merged_id| subject.id() == merged_id) {
            *subject = Link::new(survivor_id.clone());
            changed = true;
        }
    }

    if changed {
        subjects.sort_by_key(|subject| subject.id().to_string());
        subjects.dedup_by(|lhs, rhs| lhs.id() == rhs.id());
    }

    changed
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
        || matches!(table_name, "migration_history" | "fact")
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

async fn ensure_declared_extension_rows_have_space_id(
    storage: &StorageClient,
    registry: &ReferenceRegistry,
) -> IdentityResult<()> {
    for table_name in registry.tables() {
        let mut response = storage
            .query(&format!(
                "SELECT id FROM {table_name} WHERE space_id IS NONE LIMIT 1"
            ))
            .await?;
        let rows: Vec<Value> = response.take(0).map_err(|e| {
            IdentityError::StorageError(StorageError::QueryError(format!(
                "Failed to inspect extension table `{table_name}` for missing space_id: {e}"
            )))
        })?;
        if !rows.is_empty() {
            return Err(IdentityError::MergeConflict(format!(
                "Extension table `{table_name}` contains rows without `space_id` and cannot be safely rewritten during merge"
            )));
        }
    }

    Ok(())
}

async fn ensure_declared_extension_rows_are_rewriteable(
    storage: &StorageClient,
    registry: &ReferenceRegistry,
    space_id: &str,
) -> IdentityResult<()> {
    let escaped_space_id = escape_surql_string(space_id);

    for table_name in registry.owned_tables_without_rewrite_declarations() {
        let mut response = storage
            .query(&format!(
                "SELECT id FROM {table_name} WHERE space_id = '{escaped_space_id}' LIMIT 1"
            ))
            .await?;
        let rows: Vec<Value> = response.take(0).map_err(|e| {
            IdentityError::StorageError(StorageError::QueryError(format!(
                "Failed to inspect extension table `{table_name}` for undeclared entity rewrites: {e}"
            )))
        })?;
        if !rows.is_empty() {
            return Err(IdentityError::MergeConflict(format!(
                "Extension table `{table_name}` contains rows in space `{space_id}` but has no rewrite declarations; declare entity refs before merging"
            )));
        }
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
    if let Some(table_name) = mutation
        .extension_table_names()
        .into_iter()
        .find(|table_name| !registry.owns_table(table_name))
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
const ANGEL_PROTECTEES_METADATA_KEY: &str = "angel_protectees";

/// Reads `metadata.angel_protectees` as a list of string ids.
///
/// Missing keys, non-array values, and non-string elements yield an empty /
/// filtered list rather than an error.
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

#[allow(clippy::too_many_lines)]
async fn apply_entity_merge_rewrites_embedded(
    store: &EmbeddedStore,
    space_id: &str,
    survivor_id: &RecordId,
    merged_entity_ids: &[RecordId],
    now: DateTime<Utc>,
    registry: &ReferenceRegistry,
) -> IdentityResult<()> {
    let survivor_id = survivor_id.clone();
    let merged_entity_ids = merged_entity_ids.to_vec();
    let registry = registry.clone();
    store
        .mutate(move |mutation| {
            for mut key in mutation.scan::<EntityIdentityKey>()? {
                if key.base.space_id != space_id
                    || key.base.deleted_at.is_some()
                    || !merged_entity_ids.iter().any(|id| key.entity.id() == id)
                {
                    continue;
                }
                key.entity = Link::new(survivor_id.clone());
                key.base.updated_at = now;
                mutation.replace(key)?;
            }

            for mut rel in mutation.scan::<Rel>()? {
                if rel.base.space_id != space_id {
                    continue;
                }
                let changed_subject = merged_entity_ids.iter().any(|id| rel.subject.id() == id);
                if changed_subject {
                    rel.subject = Link::new(survivor_id.clone());
                }
                let changed_object = merged_entity_ids.iter().any(|id| rel.object.id() == id);
                if changed_object {
                    rel.object = Link::new(survivor_id.clone());
                }
                let changed_subjects =
                    repoint_entity_links(&mut rel.base.subjects, &merged_entity_ids, &survivor_id);
                if changed_subject || changed_object || changed_subjects {
                    rel.base.updated_at = now;
                    mutation.replace(rel)?;
                }
            }

            for mut attribute in mutation.scan::<Attribute>()? {
                if attribute.base.space_id != space_id {
                    continue;
                }
                let changed_subject = merged_entity_ids
                    .iter()
                    .any(|id| attribute.subject.id() == id);
                if changed_subject {
                    attribute.subject = Link::new(survivor_id.clone());
                }
                let changed_subjects = repoint_entity_links(
                    &mut attribute.base.subjects,
                    &merged_entity_ids,
                    &survivor_id,
                );
                if changed_subject || changed_subjects {
                    attribute.base.updated_at = now;
                    mutation.replace(attribute)?;
                }
            }

            for mut assertion in mutation.scan::<Assertion>()? {
                if assertion.base.space_id != space_id {
                    continue;
                }
                let changed_subject = assertion
                    .subject_entity
                    .as_ref()
                    .is_some_and(|link| merged_entity_ids.iter().any(|id| link.id() == id));
                if changed_subject {
                    assertion.subject_entity = Some(Link::new(survivor_id.clone()));
                }
                let changed_object = assertion
                    .object_entity
                    .as_ref()
                    .is_some_and(|link| merged_entity_ids.iter().any(|id| link.id() == id));
                if changed_object {
                    assertion.object_entity = Some(Link::new(survivor_id.clone()));
                }
                let changed_subjects = repoint_entity_links(
                    &mut assertion.base.subjects,
                    &merged_entity_ids,
                    &survivor_id,
                );
                if changed_subject || changed_object || changed_subjects {
                    assertion.base.updated_at = now;
                    mutation.replace(assertion)?;
                }
            }

            for mut doc in mutation.scan::<Doc>()? {
                if doc.base.space_id != space_id {
                    continue;
                }
                if repoint_entity_links(&mut doc.base.subjects, &merged_entity_ids, &survivor_id) {
                    doc.base.updated_at = now;
                    mutation.replace(doc)?;
                }
            }

            for mut event in mutation.scan::<Event>()? {
                if event.base.space_id != space_id {
                    continue;
                }
                if repoint_entity_links(&mut event.base.subjects, &merged_entity_ids, &survivor_id)
                {
                    event.base.updated_at = now;
                    mutation.replace(event)?;
                }
            }

            for mut event in mutation.scan::<SystemEvent>()? {
                if event.base.space_id != space_id {
                    continue;
                }
                if repoint_entity_links(&mut event.base.subjects, &merged_entity_ids, &survivor_id)
                {
                    event.base.updated_at = now;
                    mutation.replace(event)?;
                }
            }

            rewrite_extension_rows_in_mutation(
                mutation,
                space_id,
                &registry,
                &merged_entity_ids,
                &survivor_id,
            )
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

            let mut merged_protectees: Vec<String> = Vec::new();
            for merged_id in &merged_entity_ids {
                let Some(mut merged_entity) = mutation.get::<Entity>(space_id, merged_id, true)?
                else {
                    continue;
                };
                merged_protectees.extend(angel_protectees_from_metadata(&merged_entity.metadata));
                merged_entity.status = EntityStatus::Merged;
                merged_entity.redirect_to = Some(Link::new(survivor_id.clone()));
                merged_entity.base.updated_at = now;
                mutation.replace(merged_entity)?;
            }

            // Attribution survives the merge: union the merged entities'
            // angel_protectees into the survivor's metadata.
            if !merged_protectees.is_empty() {
                if let Some(mut survivor) = mutation.get::<Entity>(space_id, &survivor_id, true)? {
                    if union_angel_protectees_into_metadata(
                        &mut survivor.metadata,
                        &merged_protectees,
                    ) {
                        survivor.base.updated_at = now;
                        mutation.replace(survivor)?;
                    }
                }
            }

            Ok(())
        })
        .await?;
    Ok(())
}

/// Applies the core and extension entity-reference rewrites for a merge across
/// the configured storage backend.
///
/// # Errors
///
/// Returns an error if the storage write path fails or an extension manifest
/// declaration is invalid for the active backend.
pub async fn apply_entity_merge_rewrites(
    storage: &StorageClient,
    space_id: &str,
    survivor_id: &RecordId,
    merged_entity_ids: &[RecordId],
    now: DateTime<Utc>,
    registry: &ReferenceRegistry,
) -> IdentityResult<()> {
    if let Some(store) = storage.embedded_store() {
        return apply_entity_merge_rewrites_embedded(
            &store,
            space_id,
            survivor_id,
            merged_entity_ids,
            now,
            registry,
        )
        .await;
    }

    ensure_declared_extension_tables(storage, registry).await?;
    ensure_declared_extension_rows_have_space_id(storage, registry).await?;
    ensure_declared_extension_rows_are_rewriteable(storage, registry, space_id).await?;

    // Attribution survives the merge: read the merged entities' and
    // survivor's angel_protectees up front so the rewrite transaction can
    // union them onto the survivor.
    let entity_repo = EntityRepository::new(storage.clone());
    let mut merged_protectees: Vec<String> = Vec::new();
    for merged_id in merged_entity_ids {
        if let Some(merged_entity) = entity_repo.get(space_id, merged_id).await? {
            merged_protectees.extend(angel_protectees_from_metadata(&merged_entity.metadata));
        }
    }
    let survivor_protectees_update = if merged_protectees.is_empty() {
        None
    } else {
        match entity_repo.get(space_id, survivor_id).await? {
            Some(survivor) => {
                let mut metadata = survivor.metadata;
                union_angel_protectees_into_metadata(&mut metadata, &merged_protectees)
                    .then(|| angel_protectees_from_metadata(&metadata))
            }
            None => None,
        }
    };

    let survivor_thing = thing_from_record_id(survivor_id);
    let mut tx = storage.transaction();
    let escaped_space_id = escape_surql_string(space_id);

    for merged_id in merged_entity_ids {
        let merged_id_str = merged_id.to_string();
        tx = tx.add_statement(format!(
            "UPDATE entity_identity_key SET entity = {survivor_thing}, updated_at = time::now() WHERE space_id = '{escaped_space_id}' AND string::concat(meta::tb(entity), ':', meta::id(entity)) = '{merged_id_str}' AND deleted_at IS NONE"
        ));
    }

    for merged_id in merged_entity_ids {
        let new_thing = thing_from_record_id(survivor_id);

        for (table, field) in CORE_ENTITY_REF_TABLES {
            if *table == "entity_identity_key" {
                continue;
            }
            let mut decl = ReferenceDeclaration::new(*table, *field);
            if *field == "subjects" {
                decl = decl.with_many();
            }
            tx = tx.add_statement(generate_scoped_rewrite_query(
                &decl,
                space_id,
                merged_id,
                survivor_id,
            )?);
        }

        for decl in registry.rewrite_refs() {
            tx = tx.add_statement(generate_scoped_rewrite_query(
                decl,
                space_id,
                merged_id,
                survivor_id,
            )?);
        }

        let merged_thing = thing_from_record_id(merged_id);
        tx = tx.add_statement(format!(
            "UPDATE {merged_thing} SET status = 'merged', redirect_to = {new_thing}, updated_at = time::now()"
        ));
    }

    if let Some(protectees) = survivor_protectees_update {
        let values = protectees
            .iter()
            .map(|id| format!("'{}'", escape_surql_string(id)))
            .collect::<Vec<_>>()
            .join(", ");
        tx = tx.add_statement(format!(
            "UPDATE {survivor_thing} SET metadata.{ANGEL_PROTECTEES_METADATA_KEY} = [{values}], updated_at = time::now()"
        ));
    }

    let _ = tx.execute().await?;
    Ok(())
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
    use elephant_types::ids::TableName;

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

        let store = EmbeddedStore::new();
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

        tokio_test::block_on(apply_entity_merge_rewrites_embedded(
            &store,
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
}
