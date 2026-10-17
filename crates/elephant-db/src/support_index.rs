//! Derived lookup only: persisted support payloads remain the authority.

use std::collections::{BTreeSet, HashMap};

use serde_json::Value;

use crate::{ElephantDbError, ElephantDbState, Result};

#[derive(Debug, Clone, Default)]
pub struct SourceSupportIndex {
    records: HashMap<(String, String), BTreeSet<String>>,
    malformed_by_space: HashMap<String, BTreeSet<String>>,
    malformed_without_space: BTreeSet<String>,
}

impl SourceSupportIndex {
    fn update(&mut self, key: &str, row: &Value, insert: bool) {
        let space = row
            .get("space_id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let record = row.get("record_id").and_then(Value::as_str);
        let payload = row
            .get("payload")
            .and_then(Value::as_str)
            .and_then(|text| serde_json::from_str::<Value>(text).ok());
        let payload_space = payload
            .as_ref()
            .and_then(|value| value.get("space_id"))
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty());
        let payload_record = payload
            .as_ref()
            .and_then(|value| value.get("record_id"))
            .and_then(Value::as_str);
        let change = |bucket: &mut BTreeSet<String>| {
            if insert {
                bucket.insert(key.to_owned());
            } else {
                bucket.remove(key);
            }
        };
        match (space, record) {
            (Some(space), Some(record))
                if payload_space == Some(space)
                    && payload_record == Some(record)
                    && record.parse::<elephant_types::RecordId>().is_ok() =>
            {
                change(
                    self.records
                        .entry((space.into(), record.into()))
                        .or_default(),
                );
            }
            _ => {
                // A corrupt projection must not disappear from an exact lookup.
                // Refuse both identifiable scopes, or every scope if neither is known.
                for space in space.into_iter().chain(payload_space) {
                    change(self.malformed_by_space.entry(space.into()).or_default());
                }
                if space.is_none() && payload_space.is_none() {
                    change(&mut self.malformed_without_space);
                }
            }
        }
    }

    fn replace(&mut self, key: &str, before: Option<&Value>, after: Option<&Value>) {
        if let Some(before) = before {
            self.update(key, before, false);
        }
        if let Some(after) = after {
            self.update(key, after, true);
        }
    }

    pub(super) fn keys(&self, space: &str, record: &str, limit: usize) -> Result<Vec<&String>> {
        if !self.malformed_without_space.is_empty()
            || self
                .malformed_by_space
                .get(space)
                .is_some_and(|rows| !rows.is_empty())
        {
            return Err(ElephantDbError::DataIntegrity(
                "source support identity projection missing".into(),
            ));
        }
        let Some(keys) = self.records.get(&(space.into(), record.into())) else {
            return Ok(Vec::new());
        };
        if keys.len() > limit.min(4096) {
            return Err(ElephantDbError::DataIntegrity(
                "source support record exceeds lookup bound".into(),
            ));
        }
        Ok(keys.iter().collect())
    }
}

impl ElephantDbState {
    pub(super) fn refresh_source_support_index(
        &mut self,
        table: &str,
        key: &str,
        before: Option<&Value>,
    ) {
        if table == "source_effect_support" {
            let after = self
                .extension_tables
                .get(table)
                .and_then(|rows| rows.get(key));
            self.indexes.source_support.replace(key, before, after);
        }
    }

    pub(super) fn rebuild_source_support_index(&mut self) {
        let mut index = SourceSupportIndex::default();
        if let Some(rows) = self.extension_tables.get("source_effect_support") {
            for (key, row) in rows {
                index.replace(key, None, Some(row));
            }
        }
        self.indexes.source_support = index;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use crate::{ElephantDb, KernelMutation};
    use serde_json::json;

    fn row(key: &str, space: &str, record: &str) -> Value {
        json!({"id":format!("source_effect_support:{key}"),"space_id":space,"record_id":record,"payload":json!({"space_id":space,"record_id":record}).to_string()})
    }

    #[test]
    fn support_index_preserves_exact_rows_across_replace_delete_and_rollback() {
        let db = ElephantDb::new();
        let original = row("a", "s", "doc_node:00000000-0000-4000-8000-000000000001");
        db.tx(|tx| {
            tx.upsert_extension_row("source_effect_support", original.clone())?;
            tx.upsert_extension_row(
                "source_effect_support",
                row(
                    "foreign",
                    "other",
                    "doc_node:00000000-0000-4000-8000-000000000001",
                ),
            )?;
            tx.upsert_extension_row(
                "source_effect_support",
                row(
                    "unrelated",
                    "s",
                    "doc_node:00000000-0000-4000-8000-000000000002",
                ),
            )?;
            Ok::<(), ElephantDbError>(())
        })
        .unwrap();
        assert_eq!(
            db.source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
                .unwrap(),
            vec![("source_effect_support:a".into(), original.clone())]
        );
        let rolled_back: Result<()> = db.tx(|tx| {
            tx.replace_extension_row(
                "source_effect_support",
                "source_effect_support:a",
                row("a", "s", "doc_node:00000000-0000-4000-8000-000000000003"),
            )?;
            Err(ElephantDbError::Conflict("rollback".into()))
        });
        assert!(rolled_back.is_err());
        assert!(db
            .source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000003", 1)
            .unwrap()
            .is_empty());
        assert_eq!(
            db.source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
                .unwrap()[0]
                .1,
            original
        );
        db.tx(|tx| {
            tx.hard_delete_extension_row("source_effect_support", "source_effect_support:a", "s")
        })
        .unwrap();
        assert!(db
            .source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn support_index_recovery_and_snapshot_rebuild_do_not_lose_membership() {
        let db = ElephantDb::new();
        let original = row("a", "s", "doc_node:00000000-0000-4000-8000-000000000001");
        db.apply_recovery_batch(&[KernelMutation::Extension {
            table: "source_effect_support".into(),
            key: "source_effect_support:a".into(),
            value: Some(original.clone()),
        }])
        .unwrap();
        assert_eq!(
            db.source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
                .unwrap()[0]
                .1,
            original
        );
        let snapshot = db.export_snapshot_bytes().unwrap();
        let restored = ElephantDb::new();
        restored.import_snapshot_bytes(&snapshot).unwrap();
        assert_eq!(
            restored
                .source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
                .unwrap(),
            db.source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
                .unwrap()
        );
        restored
            .apply_recovery_batch(&[KernelMutation::Extension {
                table: "source_effect_support".into(),
                key: "source_effect_support:a".into(),
                value: None,
            }])
            .unwrap();
        assert!(restored
            .source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn support_index_refuses_overflow_and_missing_projection_in_requested_space() {
        let db = ElephantDb::new();
        db.tx(|tx| {
            for key in ["a", "b"] {
                tx.upsert_extension_row(
                    "source_effect_support",
                    row(key, "s", "doc_node:00000000-0000-4000-8000-000000000001"),
                )?;
            }
            Ok::<(), ElephantDbError>(())
        })
        .unwrap();
        assert!(db
            .source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 1)
            .is_err());
        db.tx(|tx| {
            tx.upsert_extension_row(
                "source_effect_support",
                json!({"id":"source_effect_support:broken","space_id":"bad","payload":"{}"}),
            )
        })
        .unwrap();
        assert_eq!(
            db.source_support_rows("s", "doc_node:00000000-0000-4000-8000-000000000001", 2)
                .unwrap()
                .len(),
            2
        );
        assert!(db
            .source_support_rows("bad", "doc_node:00000000-0000-4000-8000-000000000001", 2)
            .is_err());
    }
}
