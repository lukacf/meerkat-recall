//! Transaction deltas for a storage-owned, fsynced live recovery log.
//!
//! The kernel has no filesystem policy. The storage owner durably acknowledges
//! the complete delta before the write lock is released or the caller sees success.

use std::collections::BTreeMap;

use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};

use crate::{
    extension_row_key_from_value, ElephantDb, ElephantDbError, ElephantDbState, Result, UndoEntry,
};

/// A final physical row value in one atomic committed transaction.
/// Typed physical tables remain explicit: a vector may reuse a node's record ID.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum KernelMutation {
    /// A typed kernel table row; `None` means physical deletion.
    Typed {
        /// Physical table, independent of aliases in the row ID.
        table: TableName,
        /// Exact row key.
        id: RecordId,
        /// Complete final row or a deletion marker.
        value: Option<serde_json::Value>,
    },
    /// An opaque control or dynamic-vector row.
    Extension {
        /// Exact extension table name.
        table: String,
        /// Full physical row key.
        key: String,
        /// Complete final row or a deletion marker.
        value: Option<serde_json::Value>,
    },
}

pub fn committed_delta(state: &ElephantDbState, undo: &[UndoEntry]) -> Vec<KernelMutation> {
    let mut mutations = BTreeMap::new();
    for entry in undo {
        match entry {
            UndoEntry::TableRow { table, id, .. } => {
                let value = state
                    .tables
                    .get(table)
                    .and_then(|rows| rows.get(id))
                    .cloned();
                mutations.insert(
                    (false, table.as_str().to_owned(), id.to_string()),
                    KernelMutation::Typed {
                        table: *table,
                        id: id.clone(),
                        value,
                    },
                );
            }
            UndoEntry::ExtensionRow {
                table_name,
                row_key,
                ..
            } => {
                let value = state
                    .extension_tables
                    .get(table_name)
                    .and_then(|rows| rows.get(row_key))
                    .cloned();
                mutations.insert(
                    (true, table_name.clone(), row_key.clone()),
                    KernelMutation::Extension {
                        table: table_name.clone(),
                        key: row_key.clone(),
                        value,
                    },
                );
            }
        }
    }
    mutations.into_values().collect()
}

impl ElephantDb {
    /// Applies one checksum-verified, committed live-log frame during startup.
    ///
    /// This is a recovery primitive, not ordinary ingestion or backup import.
    /// The storage owner must hold exclusive recovery custody and replay the
    /// independent erasure journal before admitting readers. Running work is
    /// retained for the queue's lease recovery. No durable callback runs here.
    pub fn apply_recovery_batch(&self, mutations: &[KernelMutation]) -> Result<()> {
        self.tx(|tx| {
            for mutation in mutations {
                match mutation {
                    KernelMutation::Typed { table, id, value } => {
                        if let Some(value) = value {
                            let encoded_id = value.get("id").ok_or_else(|| {
                                ElephantDbError::DataIntegrity(
                                    "durable typed row missing id".into(),
                                )
                            })?;
                            let encoded_id: RecordId = serde_json::from_value(encoded_id.clone())
                                .map_err(|error| {
                                ElephantDbError::Serialization(error.to_string())
                            })?;
                            if encoded_id != *id
                                || value
                                    .get("space_id")
                                    .and_then(serde_json::Value::as_str)
                                    .is_none_or(str::is_empty)
                            {
                                return Err(ElephantDbError::DataIntegrity(
                                    "durable typed row identity mismatch".into(),
                                ));
                            }
                        }
                        let rows = tx.state.tables.entry(*table).or_default();
                        let previous = if let Some(value) = value {
                            rows.insert(id.clone(), value.clone())
                        } else {
                            rows.remove(id)
                        };
                        tx.dirty_tables.insert(*table);
                        tx.undo_log.push(UndoEntry::TableRow {
                            table: *table,
                            id: id.clone(),
                            previous,
                        });
                    }
                    KernelMutation::Extension { table, key, value } => {
                        if let Some(value) = value {
                            if extension_row_key_from_value(table, value)? != *key {
                                return Err(ElephantDbError::DataIntegrity(
                                    "durable extension row identity mismatch".into(),
                                ));
                            }
                        }
                        let rows = tx.state.extension_tables.entry(table.clone()).or_default();
                        let previous = if let Some(value) = value {
                            rows.insert(key.clone(), value.clone())
                        } else {
                            rows.remove(key)
                        };
                        tx.undo_log.push(UndoEntry::ExtensionRow {
                            table_name: table.clone(),
                            row_key: key.clone(),
                            previous,
                        });
                    }
                }
            }
            tx.state.validate_unique_constraints()?;
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use elephant_types::records::{
        base::BaseFields,
        entity::{Entity, EntityType},
        WorkItem, WorkItemStatus,
    };
    use std::sync::{Arc, Mutex};

    fn entity() -> Entity {
        Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "named-a".into(),
                "test".into(),
            ),
            EntityType::new("project"),
            "durable content".into(),
        )
    }

    #[test]
    fn persistence_failure_rolls_back_effect_and_epoch_before_visibility() {
        let db = ElephantDb::new();
        let row = entity();
        let result=db.tx_with_commit::<_,ElephantDbError,_,_>(|tx|tx.create(row.clone()),|delta| {
            assert!(delta.iter().any(|change|matches!(change,KernelMutation::Typed{table:TableName::Entity,..})));
            assert!(delta.iter().any(|change|matches!(change,KernelMutation::Extension{table,..} if table=="canonical_epoch")));
            Err(ElephantDbError::Snapshot("injected fsync failure".into()))
        });
        assert!(result.is_err());
        assert!(db
            .get::<Entity>("named-a", &row.base.id, true)
            .expect("row")
            .is_none());
        assert!(db
            .scan_extension_rows("canonical_epoch")
            .expect("epochs")
            .is_empty());
    }

    #[test]
    fn acknowledged_delta_recovers_whole_transaction_and_deduplicates_updates() {
        let db = ElephantDb::new();
        let row = entity();
        let log = Arc::new(Mutex::new(Vec::new()));
        let output = log.clone();
        db.tx_with_commit::<_, ElephantDbError, _, _>(
            |tx| {
                let mut created = tx.create(row.clone())?;
                created.display_name = "final committed content".into();
                tx.replace(created)
            },
            move |delta| {
                *output.lock().expect("log") = delta.to_vec();
                Ok(())
            },
        )
        .expect("durable acknowledgement");
        let delta = log.lock().expect("log").clone();
        assert_eq!(
            delta
                .iter()
                .filter(|mutation| matches!(
                    mutation,
                    KernelMutation::Typed {
                        table: TableName::Entity,
                        ..
                    }
                ))
                .count(),
            1
        );
        let reopened = ElephantDb::new();
        reopened.apply_recovery_batch(&delta).expect("replay");
        assert_eq!(
            reopened
                .get::<Entity>("named-a", &row.base.id, false)
                .expect("restored")
                .expect("row")
                .display_name,
            "final committed content"
        );
        reopened
            .apply_recovery_batch(&delta)
            .expect("idempotent frame replay");
        assert_eq!(reopened.scan::<Entity>().expect("records").len(), 1);
    }

    #[test]
    fn live_log_recovers_running_lease_while_backup_import_stays_guarded() {
        let db = ElephantDb::new();
        let log = Arc::new(Mutex::new(Vec::new()));
        let output = log.clone();
        let item = WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "named-a".into(),
                "test".into(),
            ),
            kind: "test".into(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Running,
            priority: 0,
            idempotency_key: "live-running".into(),
            attempts: 1,
            lease_expires_at: Some(chrono::Utc::now() + chrono::Duration::minutes(1)),
            leased_by: Some("original-lease".into()),
            run_after: None,
            last_error: None,
        };
        db.tx_with_commit::<_, ElephantDbError, _, _>(
            |tx| tx.create(item.clone()),
            move |delta| {
                *output.lock().expect("log") = delta.to_vec();
                Ok(())
            },
        )
        .expect("acknowledged claim");
        assert!(ElephantDb::from_snapshot_bytes(
            &db.export_snapshot_bytes().expect("raw snapshot")
        )
        .is_err());
        let checkpoint_recovered = ElephantDb::from_recovery_snapshot_bytes(
            &db.export_snapshot_bytes().expect("private checkpoint"),
        )
        .expect("trusted checkpoint replay");
        assert_eq!(
            checkpoint_recovered
                .get::<WorkItem>("named-a", &item.base.id, false)
                .expect("checkpoint work")
                .expect("present")
                .status,
            WorkItemStatus::Running
        );
        let recovered = ElephantDb::new();
        recovered
            .apply_recovery_batch(&log.lock().expect("log"))
            .expect("live replay");
        assert_eq!(
            recovered
                .get::<WorkItem>("named-a", &item.base.id, false)
                .expect("work")
                .expect("present")
                .status,
            WorkItemStatus::Running
        );
    }
}
