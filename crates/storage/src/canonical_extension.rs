//! Native extension effects prepared for the canonical transaction.
//!
//! Extension declarations select the fields that may be rewritten. This module
//! preserves native database values and supplies exact before-image checks; it
//! never opens a separate write transaction or grants source authority.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use surrealdb::sql::{Thing, Value};

/// Native database values retained by registry-aware merge preparation.
pub use surrealdb::sql::Value as NativeExtensionValue;

use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};

/// One declared extension row's complete native before/after image.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalExtensionEffect {
    /// Native extension identity; core and control tables are forbidden.
    pub id: Thing,
    /// Exact observed row; absent for a create-only effect.
    pub before: Option<Value>,
    /// Proposed row; absent for physical deletion.
    pub after: Option<Value>,
}

/// Reject identifiers which could name core/control state or inject SQL.
pub fn validate_extension_table(table: &str) -> StorageResult<()> {
    if table.is_empty()
        || !table
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || elephant_types::TableName::from_str(table).is_ok()
        || table.starts_with("emb__")
        || table.starts_with("source_")
        || table.starts_with("canonical_")
        || table.starts_with("work_")
        || table.starts_with("erasure_")
        || matches!(
            table,
            "migration_history"
                | "native_read_fence"
                | "fact"
                | "entity_profile_embedding"
                | "restore_barrier"
                | "publication_schema"
                | "provider_call_receipt"
                | "outbox_checkpoint"
                | "space_grant"
                | "watch_delegation"
                | "erasure_journal_anchor"
                | "recall_durability"
        )
    {
        return Err(integrity("invalid extension table"));
    }
    Ok(())
}

pub(crate) fn validate_extension_effect(
    space: &str,
    effect: &CanonicalExtensionEffect,
) -> StorageResult<()> {
    validate_extension_table(&effect.id.tb)?;
    if space.trim().is_empty() || (effect.before.is_none() && effect.after.is_none()) {
        return Err(integrity("empty extension effect or space"));
    }
    for value in [&effect.before, &effect.after].into_iter().flatten() {
        validate_row(space, &effect.id, value)?;
    }
    Ok(())
}

fn validate_row(space: &str, id: &Thing, value: &Value) -> StorageResult<()> {
    let Value::Object(row) = value else {
        return Err(integrity("extension record must be a native object"));
    };
    if row.get("id") != Some(&Value::Thing(id.clone()))
        || row.get("space_id") != Some(&Value::from(space))
    {
        return Err(integrity("extension identity or owning space changed"));
    }
    Ok(())
}

/// Read native rows without flattening Things, datetimes or decimals through JSON.
/// Embedded extension storage itself is JSON; only its top-level id is normalized.
pub async fn load_extension_rows(client: &StorageClient, table: &str) -> StorageResult<Vec<Value>> {
    validate_extension_table(table)?;
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| {
                tx.scan_extension_rows(table)?
                    .into_iter()
                    .map(|(_, row)| embedded_native_row(table, row))
                    .collect()
            })
            .await;
    }
    let mut response = client
        .query_checked(&format!("SELECT * FROM {table};"))
        .await?;
    let raw: surrealdb::Value = response
        .take(0)
        .map_err(|error| StorageError::QueryError(error.to_string()))?;
    match raw.into_inner() {
        Value::Array(rows) => Ok(rows.0),
        _ => Err(integrity("extension scan did not return an array")),
    }
}

/// Resolve one exact native row for inverse preparation.
pub async fn load_extension_record(
    client: &StorageClient,
    space: &str,
    id: &Thing,
) -> StorageResult<Option<Value>> {
    validate_extension_table(&id.tb)?;
    if let Some(store) = client.embedded_store() {
        return store
            .mutate(|tx| load_extension_record_tx(tx, space, id))
            .await;
    }
    let mut response = client
        .query_bind_checked(
            "SELECT * FROM ONLY $extension_id;",
            crate::bindings! { "extension_id" => id.clone() },
        )
        .await?;
    let raw: surrealdb::Value = response
        .take(0)
        .map_err(|error| StorageError::QueryError(error.to_string()))?;
    let value = raw.into_inner();
    if matches!(value, Value::None | Value::Null) {
        return Ok(None);
    }
    validate_row(space, id, &value)?;
    Ok(Some(value))
}

pub(crate) fn load_extension_record_tx(
    tx: &EmbeddedMutation<'_>,
    space: &str,
    id: &Thing,
) -> StorageResult<Option<Value>> {
    validate_extension_table(&id.tb)?;
    for (_, row) in tx.scan_extension_rows(&id.tb)? {
        let native = embedded_native_row(&id.tb, row)?;
        if let Value::Object(object) = &native {
            if object.get("id") == Some(&Value::Thing(id.clone())) {
                validate_row(space, id, &native)?;
                return Ok(Some(native));
            }
        }
    }
    Ok(None)
}

pub(crate) fn apply_extension_effects_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    effects: &[CanonicalExtensionEffect],
) -> StorageResult<()> {
    for effect in effects {
        validate_extension_effect(space, effect)?;
        if load_extension_record_tx(tx, space, &effect.id)? != effect.before {
            return Err(conflict("extension record changed during preparation"));
        }
        let mut observed = None;
        if effect.before.is_some() {
            for (key, row) in tx.scan_extension_rows(&effect.id.tb)? {
                if Some(&embedded_native_row(&effect.id.tb, row.clone())?) == effect.before.as_ref()
                {
                    observed = Some((key, row));
                    break;
                }
            }
        }
        match (&effect.after, observed) {
            (Some(after), Some((key, before))) => {
                let mut json = embedded_json_value(after)?;
                let original_id = before
                    .get("id")
                    .ok_or_else(|| integrity("extension before-image lacks identity"))?;
                json.as_object_mut()
                    .ok_or_else(|| integrity("extension after-image is not an object"))?
                    .insert("id".into(), original_id.clone());
                tx.replace_extension_row(&effect.id.tb, &key, json)?;
            }
            (Some(after), None) if effect.before.is_none() => {
                tx.upsert_extension_row(&effect.id.tb, embedded_json_value(after)?)?;
            }
            (None, Some((key, _))) => tx.hard_delete_extension_row(&effect.id.tb, &key, space)?,
            _ => return Err(conflict("extension effect lost its exact before-image")),
        }
    }
    Ok(())
}

pub(crate) fn append_extension_effects_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    effects: &[CanonicalExtensionEffect],
) -> StorageResult<()> {
    for (index, effect) in effects.iter().enumerate() {
        validate_extension_effect(space, effect)?;
        let prefix = format!("canonical_extension_{index}");
        bindings.insert(format!("{prefix}_id"), Value::Thing(effect.id.clone()));
        bindings.insert(
            format!("{prefix}_before"),
            effect.before.clone().unwrap_or(Value::None),
        );
        bindings.insert(
            format!("{prefix}_after"),
            effect.after.clone().unwrap_or(Value::None),
        );
        writeln!(sql, "\nLET ${prefix}_current = SELECT * FROM ONLY ${prefix}_id;\nIF ${prefix}_current != ${prefix}_before {{ THROW 'publication:extension_before_changed'; }};").map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        if effect.after.is_some() {
            let verb = if effect.before.is_none() {
                "CREATE"
            } else {
                "UPDATE"
            };
            writeln!(sql, "{verb} ${prefix}_id CONTENT ${prefix}_after;")
                .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        } else {
            writeln!(sql, "DELETE ${prefix}_id;")
                .map_err(|error| crate::StorageError::DataIntegrityError(error.to_string()))?;
        }
    }
    Ok(())
}

/// Recursively undo a derived change while preserving independent nested edits.
///
/// An array with concurrent changes has no declared set semantics here and is
/// rejected instead of guessing which elements or positions belong to a source.
pub fn inverse_extension_effect(
    current: Option<Value>,
    effect: &CanonicalExtensionEffect,
) -> StorageResult<Option<Value>> {
    match (&effect.before, &effect.after, current) {
        (None, Some(after), Some(current)) if &current != after => Err(conflict(
            "changed extension creation has no independent baseline",
        )),
        (None, Some(_), _) | (Some(_), Some(_), None) => Ok(None),
        (Some(before), None, None) => Ok(Some(before.clone())),
        (Some(_), None, Some(_)) => Err(conflict("deleted extension identity was recreated")),
        (Some(before), Some(after), Some(current)) => {
            inverse_native_field(Some(before), Some(after), Some(&current))
        }
        (None, None, _) => Err(integrity("empty extension inverse")),
    }
}

fn inverse_native_field(
    before: Option<&Value>,
    after: Option<&Value>,
    current: Option<&Value>,
) -> StorageResult<Option<Value>> {
    if before == after {
        return Ok(current.cloned());
    }
    if current == after {
        return Ok(before.cloned());
    }
    if let Some(Value::Object(current)) = current {
        if matches!(before, Some(Value::Object(_))) || matches!(after, Some(Value::Object(_))) {
            fn objects(value: Option<&Value>) -> StorageResult<Option<&surrealdb::sql::Object>> {
                match value {
                    Some(Value::Object(object)) => Ok(Some(object)),
                    None | Some(Value::None | Value::Null) => Ok(None),
                    _ => Err(conflict(
                        "extension inverse has an ambiguous field type change",
                    )),
                }
            }
            let before = objects(before)?;
            let after = objects(after)?;
            let mut result = current.clone();
            let keys: BTreeSet<_> = before
                .into_iter()
                .chain(after)
                .flat_map(|object| object.keys())
                .cloned()
                .collect();
            for key in keys {
                match inverse_native_field(
                    before.and_then(|object| object.get(&key)),
                    after.and_then(|object| object.get(&key)),
                    current.get(&key),
                )? {
                    Some(value) => {
                        result.insert(key, value);
                    }
                    None => {
                        result.remove(&key);
                    }
                }
            }
            return Ok(Some(Value::Object(result)));
        }
    }
    if matches!(after, Some(Value::Array(_))) && matches!(current, Some(Value::Array(_))) {
        return Err(conflict(
            "concurrent extension array changes require declared set semantics",
        ));
    }
    Ok(current.cloned())
}

/// Embedded JSON is its native storage representation; never use this bridge on
/// SurrealDB reads or effects because that would erase native field types.
///
/// Normalize an opaque embedded row identity without changing its physical key.
/// Lifecycle inventory and permanent-reference guards share this exact parser.
pub(crate) fn embedded_extension_identity(
    table: &str,
    row: &serde_json::Value,
) -> StorageResult<Thing> {
    validate_extension_table(table)?;
    embedded_physical_identity(table, row)
}

/// Trusted physical inventory identity, including core and private control rows.
///
/// This does not authorize extension writes. Domain UUID rows are retabled to
/// their actual physical table (for example a node stored as chunk_embedding).
pub(crate) fn embedded_physical_identity(
    table: &str,
    row: &serde_json::Value,
) -> StorageResult<Thing> {
    if let Ok(physical_table) = elephant_types::TableName::from_str(table) {
        let value = row
            .get("id")
            .ok_or_else(|| integrity("typed physical row lacks identity"))?;
        let domain: elephant_types::RecordId = serde_json::from_value(value.clone())
            .map_err(|error| integrity(&format!("invalid typed physical identity: {error}")))?;
        return Ok(crate::surreal::thing_from_record_id(
            &elephant_types::RecordId::new(physical_table, domain.id()),
        ));
    }
    let id = match row.get("id") {
        Some(serde_json::Value::String(id)) => {
            if let Some((declared, key)) = id.split_once(':') {
                if declared != table {
                    return Err(integrity("extension row names a different table"));
                }
                surrealdb::sql::thing(id).unwrap_or_else(|_| Thing::from((table, key)))
            } else {
                Thing::from((table, id.as_str()))
            }
        }
        Some(serde_json::Value::Number(id)) => Thing::from((
            table,
            surrealdb::sql::Id::Number(
                id.as_i64()
                    .ok_or_else(|| integrity("extension numeric identity is not an integer"))?,
            ),
        )),
        Some(serde_json::Value::Object(id)) => {
            if let Some(declared) = id.get("tb").or_else(|| id.get("table")) {
                if declared.as_str() != Some(table) {
                    return Err(integrity("extension row names a different table"));
                }
            }
            match id.get("id") {
                Some(serde_json::Value::String(key)) => Thing::from((table, key.as_str())),
                Some(serde_json::Value::Number(key)) => Thing::from((
                    table,
                    surrealdb::sql::Id::Number(key.as_i64().ok_or_else(|| {
                        integrity("extension numeric identity is not an integer")
                    })?),
                )),
                _ => return Err(integrity("unsupported embedded extension identity")),
            }
        }
        _ => return Err(integrity("embedded extension row lacks identity")),
    };
    Ok(id)
}

fn embedded_native_row(table: &str, row: serde_json::Value) -> StorageResult<Value> {
    let id = embedded_extension_identity(table, &row)?;
    let mut native = crate::surreal::json_to_surreal(row)?;
    let Value::Object(object) = &mut native else {
        return Err(integrity("embedded extension row is not an object"));
    };
    object.insert("id".into(), Value::Thing(id));
    Ok(native)
}

fn embedded_json_value(value: &Value) -> StorageResult<serde_json::Value> {
    match value {
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), embedded_json_value(value)?)))
            .collect::<StorageResult<serde_json::Map<_, _>>>()
            .map(serde_json::Value::Object),
        Value::Array(array) => array
            .iter()
            .map(embedded_json_value)
            .collect::<StorageResult<Vec<_>>>()
            .map(serde_json::Value::Array),
        Value::Thing(id) => Ok(serde_json::Value::String(id.to_string())),
        Value::None | Value::Null | Value::Bool(_) | Value::Strand(_) | Value::Number(_) => {
            crate::surreal::surreal_to_json(value.clone())
        }
        _ => Err(integrity(
            "native extension type cannot be flattened into embedded JSON",
        )),
    }
}

fn integrity(message: &str) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}
fn conflict(message: &str) -> StorageError {
    StorageError::Conflict(message.into())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use surrealdb::sql::Object;

    fn row(name: &str) -> Value {
        Value::Object(Object::from(BTreeMap::<String, Value>::from([
            ("id".into(), Thing::from(("sampleapp_note", "one")).into()),
            ("space_id".into(), Value::from("home")),
            (
                "data".into(),
                Value::Object(Object::from(BTreeMap::<String, Value>::from([(
                    "name".into(),
                    Value::from(name),
                )]))),
            ),
        ])))
    }

    #[test]
    fn immutable_watch_authority_is_not_a_consumer_extension() {
        for table in [
            "watch_delegation",
            "watch_delegation_custom",
            "watch_custom",
        ] {
            let id = Thing::from((table, "original"));
            let value = Value::Object(Object::from(BTreeMap::<String, Value>::from([
                ("id".into(), Value::Thing(id.clone())),
                ("space_id".into(), Value::from("home")),
                ("payload".into(), Value::from("original authority")),
            ])));
            let allowed = table != "watch_delegation";
            assert_eq!(validate_extension_table(table).is_ok(), allowed);
            for (before, after) in [
                (None, Some(value.clone())),
                (Some(value.clone()), Some(value.clone())),
                (Some(value.clone()), None),
            ] {
                assert_eq!(
                    validate_extension_effect(
                        "home",
                        &CanonicalExtensionEffect {
                            id: id.clone(),
                            before,
                            after,
                        },
                    )
                    .is_ok(),
                    allowed,
                    "extension effect boundary for {table}",
                );
            }
        }
    }

    #[expect(
        clippy::items_after_statements,
        reason = "Keep the local fixture helper beside the original scenario phase that uses it."
    )]
    async fn transactional_extensions(embedded: bool) {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new(crate::StorageConfig::default()).await
        }
        .unwrap();
        let first = CanonicalExtensionEffect {
            id: Thing::from(("sampleapp_note", "one")),
            before: None,
            after: Some(row("before")),
        };
        async fn apply(
            storage: &StorageClient,
            effects: &[CanonicalExtensionEffect],
        ) -> StorageResult<()> {
            if let Some(store) = storage.embedded_store() {
                return store
                    .mutate(|tx| apply_extension_effects_tx(tx, "home", effects))
                    .await;
            }
            let mut sql = "BEGIN TRANSACTION;".to_owned();
            let mut bindings = BTreeMap::new();
            append_extension_effects_sql(&mut sql, &mut bindings, "home", effects)?;
            sql.push_str("COMMIT TRANSACTION;");
            storage.query_bind_checked(&sql, bindings).await?;
            Ok(())
        }
        apply(&storage, std::slice::from_ref(&first)).await.unwrap();
        assert_eq!(
            load_extension_record(&storage, "home", &first.id)
                .await
                .unwrap(),
            first.after
        );
        let update = CanonicalExtensionEffect {
            id: first.id.clone(),
            before: first.after.clone(),
            after: Some(row("after")),
        };
        // The first effect is valid. The second conflicts; the whole transaction
        // must roll back, including the already staged replacement.
        assert!(apply(&storage, &[update.clone(), first.clone()])
            .await
            .is_err());
        assert_eq!(
            load_extension_record(&storage, "home", &first.id)
                .await
                .unwrap(),
            first.after
        );
        apply(&storage, std::slice::from_ref(&update))
            .await
            .unwrap();
        assert_eq!(
            load_extension_record(&storage, "home", &first.id)
                .await
                .unwrap(),
            update.after
        );
        let delete = CanonicalExtensionEffect {
            id: first.id.clone(),
            before: update.after,
            after: None,
        };
        apply(&storage, &[delete]).await.unwrap();
        assert!(load_extension_record(&storage, "home", &first.id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn embedded_extension_effects_are_atomic_and_exact() {
        transactional_extensions(true).await;
    }
    #[tokio::test]
    async fn surreal_extension_effects_are_atomic_and_exact() {
        transactional_extensions(false).await;
    }

    #[test]
    fn physical_identity_handles_core_tables_without_authorizing_extension_writes() {
        let node = elephant_types::RecordId::new_v4(elephant_types::TableName::DocNode);
        let physical =
            embedded_physical_identity("chunk_embedding", &serde_json::json!({"id":node})).unwrap();
        assert_eq!(physical.tb, "chunk_embedding");
        assert_eq!(
            crate::surreal::record_id_from_thing(&physical)
                .unwrap()
                .id(),
            node.id()
        );
        let nested = embedded_physical_identity(
            "doc",
            &serde_json::json!({"id":{"table":"doc_node","id":node.id().to_string()}}),
        )
        .unwrap();
        assert_eq!(nested.tb, "doc");
        let control = serde_json::json!({"id":"source_revision:private-key"});
        assert_eq!(
            embedded_physical_identity("source_revision", &control)
                .unwrap()
                .tb,
            "source_revision"
        );
        assert!(embedded_extension_identity("source_revision", &control).is_err());
        assert!(embedded_extension_identity("doc", &serde_json::json!({"id":node})).is_err());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn embedded_replacement_preserves_opaque_identity_representation() {
        let storage = StorageClient::new_embedded().await.unwrap();
        let store = storage.embedded_store().unwrap();
        for original_id in [
            serde_json::json!("sampleapp_note:todo-1"),
            serde_json::json!({"tb":"sampleapp_note","id":42}),
            serde_json::json!("unqualified-key"),
        ] {
            let original = serde_json::json!({"id":original_id,"space_id":"home","name":"before"});
            store
                .mutate(|tx| {
                    tx.upsert_extension_row("sampleapp_note", original.clone())?;
                    Ok(())
                })
                .await
                .unwrap();
            let before = embedded_native_row("sampleapp_note", original.clone()).unwrap();
            let Value::Object(mut after) = before.clone() else {
                unreachable!()
            };
            let Value::Thing(id) = after.get("id").unwrap().clone() else {
                unreachable!()
            };
            after.insert("name".into(), Value::from("after"));
            let effect = CanonicalExtensionEffect {
                id,
                before: Some(before),
                after: Some(Value::Object(after)),
            };
            store
                .mutate(|tx| apply_extension_effects_tx(tx, "home", &[effect]))
                .await
                .unwrap();
            let rows = store
                .mutate(|tx| Ok(tx.scan_extension_rows("sampleapp_note")?))
                .await
                .unwrap();
            let selected: Vec<_> = rows
                .iter()
                .filter(|(_, row)| row.get("id") == Some(&original_id))
                .collect();
            assert_eq!(selected.len(), 1);
            assert_eq!(selected[0].1["name"], "after");
        }
        assert_eq!(
            load_extension_rows(&storage, "sampleapp_note")
                .await
                .unwrap()
                .len(),
            3
        );
    }

    #[test]
    fn nested_inverse_preserves_native_values_and_independent_siblings() {
        let before = row("baseline");
        let after = row("source-derived");
        let mut current = after.clone();
        let Value::Object(object) = &mut current else {
            unreachable!()
        };
        let Value::Object(data) = object.get_mut("data").unwrap() else {
            unreachable!()
        };
        let when = Value::Datetime(surrealdb::sql::Datetime::from(chrono::Utc::now()));
        data.insert("independent".into(), when.clone());
        let restored = inverse_extension_effect(
            Some(current),
            &CanonicalExtensionEffect {
                id: Thing::from(("sampleapp_note", "one")),
                before: Some(before),
                after: Some(after),
            },
        )
        .unwrap()
        .unwrap();
        let Value::Object(object) = restored else {
            unreachable!()
        };
        let Value::Object(data) = object.get("data").unwrap() else {
            unreachable!()
        };
        assert_eq!(data.get("name"), Some(&Value::from("baseline")));
        assert_eq!(data.get("independent"), Some(&when));
    }

    #[test]
    fn inverse_rejects_mixed_object_type_change_without_leaking_derived_fields() {
        let before = Value::from("scalar baseline");
        let after =
            crate::surreal::json_to_surreal(serde_json::json!({"secret":"source"})).unwrap();
        let current = crate::surreal::json_to_surreal(
            serde_json::json!({"secret":"source","independent":"keep"}),
        )
        .unwrap();
        assert!(matches!(
            inverse_native_field(Some(&before), Some(&after), Some(&current)),
            Err(StorageError::Conflict(_))
        ));
    }

    #[test]
    fn inverse_restores_deleted_object_leaves_beside_independent_recreation() {
        let before =
            crate::surreal::json_to_surreal(serde_json::json!({"baseline":"keep"})).unwrap();
        let current =
            crate::surreal::json_to_surreal(serde_json::json!({"independent":"also keep"}))
                .unwrap();
        let expected = crate::surreal::json_to_surreal(
            serde_json::json!({"baseline":"keep","independent":"also keep"}),
        )
        .unwrap();
        assert_eq!(
            inverse_native_field(Some(&before), None, Some(&current)).unwrap(),
            Some(expected)
        );
    }

    #[test]
    fn opaque_array_inverse_fails_closed_on_concurrent_change() {
        let values = |items: Vec<&str>| Value::Array(items.into_iter().map(Value::from).collect());
        assert!(inverse_native_field(
            Some(&values(vec!["a"])),
            Some(&values(vec!["a", "b"])),
            Some(&values(vec!["a", "b", "c"]))
        )
        .is_err());
    }

    #[test]
    fn rejects_control_tables_foreign_spaces_and_identity_changes() {
        for table in [
            "entity",
            "source_head",
            "canonical_epoch",
            "emb__private",
            "x;DELETE source",
        ] {
            assert!(validate_extension_table(table).is_err());
        }
        let effect = CanonicalExtensionEffect {
            id: Thing::from(("sampleapp_note", "one")),
            before: Some(row("before")),
            after: Some(row("after")),
        };
        assert!(validate_extension_effect("home", &effect).is_ok());
        assert!(validate_extension_effect("other", &effect).is_err());
    }
}
