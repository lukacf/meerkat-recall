//! Retention policy enforcement worker.
//!
//! Periodically cleans up old records based on retention policies:
//! - Soft-deleted records older than 30 days are permanently deleted
//! - Audit logs older than 30 days are permanently deleted
//! - System events older than 30 days are permanently deleted

use crate::error::PipelineResult;
use chrono::Utc;
use elephant_storage::{EmbeddedRecord, EmbeddedStore, StorageClient, StorageError};
use elephant_types::records::rel::Rel;
use elephant_types::records::{AuditLog, Doc, Entity, Source, SystemEvent};
use std::time::Duration;
use tracing::info;

/// Worker responsible for enforcing data retention policies.
pub struct RetentionWorker {
    client: StorageClient,
    retention_period: Duration,
}

impl RetentionWorker {
    /// Creates a new retention worker.
    #[must_use]
    pub const fn new(client: StorageClient, retention_period: Duration) -> Self {
        Self {
            client,
            retention_period,
        }
    }

    /// Processes retention cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if database operations fail.
    pub async fn process_cleanup(&self) -> PipelineResult<RetentionResult> {
        let mut total_deleted = 0;

        // 1. Permanently delete soft-deleted records older than retention period
        // For simplicity, we query common tables
        let tables = ["doc", "doc_chunk", "entity", "fact", "rel", "source"];
        for table in tables {
            let deleted = self.cleanup_soft_deleted(table).await?;
            total_deleted += deleted;
        }

        // 2. Cleanup audit logs
        let audit_deleted = self.cleanup_audit_logs().await?;
        total_deleted += audit_deleted;

        // 3. Cleanup system events
        let event_deleted = self.cleanup_system_events().await?;
        total_deleted += event_deleted;

        Ok(RetentionResult { total_deleted })
    }

    /// Archives a bounded batch independently of the legacy cleanup families.
    ///
    /// # Errors
    /// Returns an error if copying terminal work to durable history fails.
    pub async fn process_work_history(
        &self,
        batch_size: usize,
    ) -> PipelineResult<elephant_storage::work_retention::WorkRetentionResult> {
        Ok(elephant_storage::work_retention::archive_terminal_work(
            &self.client,
            retention_cutoff(self.retention_period)?,
            batch_size,
        )
        .await?)
    }

    async fn cleanup_soft_deleted(&self, table: &str) -> PipelineResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let cutoff = retention_cutoff(self.retention_period)?;
            let deleted = match table {
                "doc" => cleanup_soft_deleted_records::<Doc>(store.as_ref(), cutoff).await?,
                "entity" => cleanup_soft_deleted_records::<Entity>(store.as_ref(), cutoff).await?,
                "rel" => cleanup_soft_deleted_records::<Rel>(store.as_ref(), cutoff).await?,
                "source" => cleanup_soft_deleted_records::<Source>(store.as_ref(), cutoff).await?,
                "doc_chunk" | "fact" => 0,
                _ => 0,
            };
            if deleted > 0 {
                info!(
                    "Cleaned up {} soft-deleted records from table {}",
                    deleted, table
                );
            }
            return Ok(deleted);
        }

        // The cutoff is computed here and cast in the query: binding a duration
        // as a string breaks datetime arithmetic on SurrealDB servers
        // ("Cannot perform subtraction with '<datetime>' and '2592000s'").
        let cutoff = retention_cutoff(self.retention_period)?;
        let query = format!(
            "DELETE {table} WHERE deleted_at IS NOT NONE AND deleted_at < <datetime> $cutoff RETURN BEFORE"
        );
        let mut response = self
            .client
            .query_bind(&query, serde_json::json!({ "cutoff": cutoff.to_rfc3339() }))
            .await?;

        let deleted = parse_deleted_count(
            &mut response,
            &format!("Failed to parse retention delete for {table}"),
        )?;
        if deleted > 0 {
            info!(
                "Cleaned up {} soft-deleted records from table {}",
                deleted, table
            );
        }
        Ok(deleted)
    }

    async fn cleanup_audit_logs(&self) -> PipelineResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let cutoff = retention_cutoff(self.retention_period)?;
            let deleted = cleanup_created_before::<AuditLog>(store.as_ref(), cutoff).await?;
            if deleted > 0 {
                info!("Cleaned up {} old audit log entries", deleted);
            }
            return Ok(deleted);
        }

        let cutoff = retention_cutoff(self.retention_period)?;
        let query = "DELETE audit_log WHERE created_at < <datetime> $cutoff RETURN BEFORE";
        let mut response = self
            .client
            .query_bind(query, serde_json::json!({ "cutoff": cutoff.to_rfc3339() }))
            .await?;

        let deleted =
            parse_deleted_count(&mut response, "Failed to parse retention audit log delete")?;
        if deleted > 0 {
            info!("Cleaned up {} old audit log entries", deleted);
        }
        Ok(deleted)
    }

    async fn cleanup_system_events(&self) -> PipelineResult<usize> {
        if let Some(store) = self.client.embedded_store() {
            let cutoff = retention_cutoff(self.retention_period)?;
            let deleted = cleanup_created_before::<SystemEvent>(store.as_ref(), cutoff).await?;
            if deleted > 0 {
                info!("Cleaned up {} old system events", deleted);
            }
            return Ok(deleted);
        }

        let cutoff = retention_cutoff(self.retention_period)?;
        let query = "DELETE system_event WHERE created_at < <datetime> $cutoff RETURN BEFORE";
        let mut response = self
            .client
            .query_bind(query, serde_json::json!({ "cutoff": cutoff.to_rfc3339() }))
            .await?;

        let deleted = parse_deleted_count(
            &mut response,
            "Failed to parse retention system event delete",
        )?;
        if deleted > 0 {
            info!("Cleaned up {} old system events", deleted);
        }
        Ok(deleted)
    }
}

/// Result of a retention cleanup run.
pub struct RetentionResult {
    /// Total number of records deleted.
    pub total_deleted: usize,
}

fn parse_deleted_count(response: &mut surrealdb::Response, context: &str) -> PipelineResult<usize> {
    // `RETURN BEFORE` yields full records with SurrealDB-native values
    // (datetimes, record ids); only the count matters here.
    let deleted: Vec<serde::de::IgnoredAny> = response
        .take(0)
        .map_err(|e| StorageError::QueryError(format!("{context}: {e}")))?;
    Ok(deleted.len())
}

fn retention_cutoff(retention_period: Duration) -> Result<chrono::DateTime<Utc>, StorageError> {
    let retention = chrono::Duration::from_std(retention_period).map_err(|e| {
        StorageError::QueryError(format!(
            "Invalid retention period for embedded cleanup: {e}"
        ))
    })?;
    Ok(Utc::now() - retention)
}

async fn cleanup_soft_deleted_records<T>(
    store: &EmbeddedStore,
    cutoff: chrono::DateTime<Utc>,
) -> Result<usize, StorageError>
where
    T: EmbeddedRecord,
{
    store
        .mutate(|mutation| {
            let rows = mutation.scan::<T>()?;
            let ids: Vec<_> = rows
                .into_iter()
                .filter_map(|row| {
                    row.base()
                        .deleted_at
                        .filter(|deleted_at| *deleted_at < cutoff)
                        .map(|_| (row.base().space_id.clone(), row.base().id.clone()))
                })
                .collect();
            let total = ids.len();
            for (space_id, id) in ids {
                mutation.hard_delete::<T>(&space_id, &id)?;
            }
            Ok(total)
        })
        .await
}

async fn cleanup_created_before<T>(
    store: &EmbeddedStore,
    cutoff: chrono::DateTime<Utc>,
) -> Result<usize, StorageError>
where
    T: EmbeddedRecord,
{
    store
        .mutate(|mutation| {
            let rows = mutation.scan::<T>()?;
            let ids: Vec<_> = rows
                .into_iter()
                .filter(|row| row.base().created_at < cutoff)
                .map(|row| (row.base().space_id.clone(), row.base().id.clone()))
                .collect();
            let total = ids.len();
            for (space_id, id) in ids {
                mutation.hard_delete::<T>(&space_id, &id)?;
            }
            Ok(total)
        })
        .await
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use crate::error::PipelineError;

    #[tokio::test]
    async fn test_retention_parse_errors_surface() -> Result<(), Box<dyn std::error::Error>> {
        let client = StorageClient::new_memory().await?;
        let mut response = client.query("THROW 'boom'").await?;

        let err = parse_deleted_count(&mut response, "retention parse").unwrap_err();
        assert!(matches!(err, PipelineError::StorageError(_)));
        Ok(())
    }

    async fn count_rows(client: &StorageClient, table: &str) -> usize {
        let mut response = client
            .query(&format!("SELECT id FROM {table}"))
            .await
            .expect("count query");
        let rows: Vec<serde::de::IgnoredAny> = response.take(0).expect("count rows");
        rows.len()
    }

    #[tokio::test]
    async fn test_retention_cleanup_runs_against_surreal_backend() {
        // Regression test for the live retention query: the cutoff must be a
        // bound datetime, not a duration string (SurrealDB rejects
        // `datetime - '2592000s'` with a subtraction type error).
        let client = StorageClient::new_memory().await.expect("storage");
        client
            .query(
                "DEFINE TABLE doc SCHEMALESS; DEFINE TABLE audit_log SCHEMALESS; DEFINE TABLE system_event SCHEMALESS;",
            )
            .await
            .expect("define tables")
            .check()
            .expect("define tables succeed");
        client
            .query(
                "CREATE doc:old_deleted SET space_id = 'default', deleted_at = <datetime> '2020-01-01T00:00:00Z';
                 CREATE doc:fresh_deleted SET space_id = 'default', deleted_at = time::now();
                 CREATE doc:live SET space_id = 'default';
                 CREATE audit_log:old SET space_id = 'default', created_at = <datetime> '2020-01-01T00:00:00Z';
                 CREATE audit_log:fresh SET space_id = 'default', created_at = time::now();
                 CREATE system_event:old SET space_id = 'default', created_at = <datetime> '2020-01-01T00:00:00Z';
                 CREATE system_event:fresh SET space_id = 'default', created_at = time::now();",
            )
            .await
            .expect("seed rows")
            .check()
            .expect("seed rows succeed");

        let worker = RetentionWorker::new(client.clone(), Duration::from_secs(30 * 24 * 3600));
        let result = worker.process_cleanup().await.expect("cleanup");

        assert_eq!(
            result.total_deleted, 3,
            "old doc + audit_log + system_event"
        );
        assert_eq!(count_rows(&client, "doc").await, 2);
        assert_eq!(count_rows(&client, "audit_log").await, 1);
        assert_eq!(count_rows(&client, "system_event").await, 1);
    }
    #[tokio::test]
    async fn failed_legacy_cleanup_does_not_block_work_history() {
        use elephant_storage::{Repository, WorkItemRepository};
        use elephant_types::records::{BaseFields, WorkItem, WorkItemStatus};
        use elephant_types::{RecordId, TableName};
        let client = StorageClient::new_memory().await.expect("storage");
        client.query_checked(
            "CREATE doc:old SET deleted_at = <datetime>'2020-01-01T00:00:00Z'; DEFINE EVENT reject_retention ON TABLE doc WHEN $event = 'DELETE' THEN { THROW 'legacy_cleanup_rejected'; };"
        ).await.expect("seed failing legacy cleanup");
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkItem),
            "space".to_string(),
            "test".to_string(),
        );
        base.updated_at = Utc::now() - chrono::Duration::days(60);
        let item = WorkItem {
            idempotency_key: base.id.to_string(),
            base,
            kind: "extract_assertions".to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Done,
            priority: 0,
            attempts: 1,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let items = WorkItemRepository::new(client.clone());
        items
            .create(item.clone())
            .await
            .expect("seed completed job");
        let worker = RetentionWorker::new(client.clone(), Duration::from_secs(30 * 24 * 3600));
        let error = worker
            .process_cleanup()
            .await
            .err()
            .expect("legacy deletion fails");
        assert!(
            error.to_string().contains("legacy_cleanup_rejected"),
            "{error}"
        );
        let result = worker
            .process_work_history(1)
            .await
            .expect("independent work archive");
        assert_eq!(result.items_archived, 1);
        assert_eq!(count_rows(&client, "work_item").await, 0);
        assert_eq!(count_rows(&client, "work_item_archive").await, 1);
        assert_eq!(
            items
                .get("space", &item.base.id)
                .await
                .expect("retained history"),
            Some(item)
        );
    }
}
