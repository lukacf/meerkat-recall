//! Audit log repository implementation for `SurrealDB`.

#![allow(clippy::items_after_statements)]

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::entity::Entity;
use elephant_types::records::{AuditAction, AuditLog};

use elephant_types::{Link, RecordId, TableName};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::require::required_field;

pub(crate) const AUDIT_LOG_PROJECTION: &str = r"
    id,
    space_id,
    created_at,
    updated_at,
    created_by,
    deleted_at,
    security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE [] END,
        extractor: provenance.extractor,
        run_id: provenance.run_id,
        work_run_id: provenance.work_run_id,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects,
    subjects_state,
    action,
    actor,
    record,
    before,
    after,
    timestamp,
    reason
";

/// Database-only proxy for `AuditLog` to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct AuditLogDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<elephant_types::records::base::Provenance>,
    security: Option<elephant_types::records::base::Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<elephant_types::records::base::SubjectsState>,
    action: Option<AuditAction>,
    actor: Option<String>,
    record: Option<surrealdb::sql::Thing>,
    before: Option<serde_json::Value>,
    after: Option<serde_json::Value>,
    timestamp: Option<chrono::DateTime<Utc>>,
    reason: Option<String>,
}

impl TryFrom<AuditLogDb> for AuditLog {
    type Error = StorageError;

    fn try_from(db: AuditLogDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: elephant_types::records::base::BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required_field(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required_field(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required_field(db.provenance, "provenance")?,
                security: required_field(db.security, "security")?,
                subjects: required_field(db.subjects, "subjects")?,
                subjects_state: required_field(db.subjects_state, "subjects_state")?,
            },
            action: required_field(db.action, "action")?,
            actor: required_field(db.actor, "actor")?,
            record: crate::surreal::record_id_from_thing(
                &db.record
                    .ok_or_else(|| StorageError::QueryError("Missing record ID".to_string()))?,
            )
            .map_err(|e| StorageError::QueryError(e.to_string()))?,
            before: db.before,
            after: db.after,
            timestamp: db
                .timestamp
                .ok_or_else(|| StorageError::QueryError("Missing timestamp".to_string()))?,
            reason: db.reason,
        })
    }
}

/// Audit log repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct AuditLogRepository {
    client: StorageClient,
}

impl AuditLogRepository {
    /// Creates a new audit log repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }
}

#[async_trait]
impl Repository<AuditLog> for AuditLogRepository {
    async fn create(&self, record: AuditLog) -> StorageResult<AuditLog> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {AUDIT_LOG_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => crate::surreal::audit_log_to_value(&record)?,
                },
            )
            .await?;

        let logs: Vec<AuditLogDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created audit_log: {e}"))
        })?;

        let log_db = logs.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but audit_log not found".to_string())
        })?;

        AuditLog::try_from(log_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<AuditLog>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<AuditLog>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let logs: Vec<AuditLogDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse audit_log: {e}")))?;

        Ok(logs
            .into_iter()
            .next()
            .map(AuditLog::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: AuditLog,
    ) -> StorageResult<AuditLog> {
        let mut normalized = record;
        normalized.base.space_id = space_id.to_string();
        normalized.base.id.clone_from(id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {AUDIT_LOG_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => crate::surreal::audit_log_to_value(&normalized)?,
                },
            )
            .await?;

        let logs: Vec<AuditLogDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated audit_log: {e}"))
        })?;

        let log_db = logs
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Audit log not found in space: {id}")))?;

        AuditLog::try_from(log_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<AuditLog>(space_id, id).await;
        }

        self.client
            .query_bind_checked(
                "UPDATE $id SET deleted_at = time::now(), updated_at = time::now() WHERE space_id = $space_id",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        Ok(())
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<AuditLog>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<AuditLog>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
            )
        };

        let mut response = self
            .client
            .query_bind(
                &query,
                crate::bindings! {
                    "cursor" => cursor.map_or(surrealdb::sql::Value::None, |r| {
                        surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(r))
                    }),
                    "space_id" => space_id
                },
            )
            .await?;

        let logs: Vec<AuditLogDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse audit_logs: {e}")))?;

        logs.into_iter().map(AuditLog::try_from).collect()
    }
}

#[async_trait]
impl SearchRepository<AuditLog> for AuditLogRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<AuditLog>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .search::<AuditLog, _>(criteria, |log, criteria| {
                    criteria.query.as_ref().is_none_or(|query| {
                        let query = query.to_ascii_lowercase();
                        let action_matches = serde_json::to_string(&log.action)
                            .map(|action| action.to_ascii_lowercase().contains(&query))
                            .unwrap_or(false);
                        action_matches || log.actor.to_ascii_lowercase().contains(&query)
                    })
                })
                .await;
        }

        #[derive(Deserialize)]
        struct CountResult {
            total: usize,
        }

        let mut conditions = Vec::new();
        let mut bindings = std::collections::BTreeMap::new();

        if !criteria.include_deleted {
            conditions.push("deleted_at IS NONE".to_string());
        }

        if let Some(ref space_id) = criteria.space_id {
            conditions.push("space_id = $space_id".to_string());
            bindings.insert(
                "space_id".to_string(),
                surrealdb::sql::Value::from(space_id.clone()),
            );
        }

        if let Some(ref query) = criteria.query {
            conditions.push(
                "(string::lowercase(action) CONTAINS string::lowercase($query) OR string::lowercase(actor) CONTAINS string::lowercase($query))"
                    .to_string(),
            );
            bindings.insert(
                "query".to_string(),
                surrealdb::sql::Value::from(query.clone()),
            );
        }

        if let Some(cursor) = criteria.cursor.as_ref() {
            let rid = crate::parse_id(cursor, TableName::AuditLog)
                .map_err(|e| StorageError::QueryError(e.to_string()))?;
            conditions.push("id > $cursor".to_string());
            bindings.insert(
                "cursor".to_string(),
                surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(&rid)),
            );
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log {where_clause} ORDER BY id LIMIT {}",
            criteria.limit + 1
        );

        let mut response = self.client.query_bind(&query, bindings.clone()).await?;

        let logs_db: Vec<AuditLogDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse audit_logs: {e}")))?;

        let mut logs: Vec<AuditLog> = logs_db
            .into_iter()
            .map(AuditLog::try_from)
            .collect::<Result<Vec<_>, _>>()?;

        let has_more = logs.len() > criteria.limit;
        if has_more {
            logs.pop();
        }

        let next_cursor = if has_more {
            logs.last().map(|l| {
                l.base
                    .id
                    .to_string()
                    .split(':')
                    .nth(1)
                    .unwrap_or(&l.base.id.to_string())
                    .to_string()
            })
        } else {
            None
        };

        let count_query =
            format!("SELECT count() as total FROM audit_log {where_clause} GROUP ALL");

        let mut count_response = self.client.query_bind(&count_query, bindings).await?;

        let counts: Vec<CountResult> =
            crate::response::take_first(&mut count_response, "Failed to parse audit log count")?;
        let total = counts.first().map_or(0, |c| c.total);

        Ok(PaginatedResult::new(logs, next_cursor, has_more, total))
    }
}

/// Additional audit-specific operations.
impl AuditLogRepository {
    /// Gets audit logs for a specific record.
    ///
    /// # Errors
    ///
    /// Returns an error if the audit log query fails or deserialization fails.
    pub async fn find_by_record(&self, record_id: &RecordId) -> StorageResult<Vec<AuditLog>> {
        if let Some(store) = self.client.embedded_store() {
            let mut logs = store.scan::<AuditLog>().await?;
            logs.retain(|log| log.base.deleted_at.is_none() && &log.record == record_id);
            logs.sort_by(|lhs, rhs| {
                rhs.timestamp
                    .cmp(&lhs.timestamp)
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            return Ok(logs);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log WHERE record = $record_id AND deleted_at IS NONE ORDER BY timestamp DESC"
                ),
                crate::bindings! { "record_id" => crate::surreal::thing_from_record_id(record_id) },
            )
            .await?;

        let logs: Vec<AuditLogDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse audit_logs: {e}")))?;

        logs.into_iter().map(AuditLog::try_from).collect()
    }

    /// Gets recent audit logs.
    ///
    /// # Errors
    ///
    /// Returns an error if the audit log query fails or deserialization fails.
    pub async fn get_recent(&self, limit: usize) -> StorageResult<Vec<AuditLog>> {
        if let Some(store) = self.client.embedded_store() {
            let mut logs = store.scan::<AuditLog>().await?;
            logs.retain(|log| log.base.deleted_at.is_none());
            logs.sort_by(|lhs, rhs| {
                rhs.timestamp
                    .cmp(&lhs.timestamp)
                    .then_with(|| lhs.base.id.to_string().cmp(&rhs.base.id.to_string()))
            });
            if logs.len() > limit {
                logs.truncate(limit);
            }
            return Ok(logs);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log WHERE deleted_at IS NONE ORDER BY timestamp DESC LIMIT {limit}"
                ),
                serde_json::json!({}),
            )
            .await?;

        let logs: Vec<AuditLogDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse audit_logs: {e}")))?;

        logs.into_iter().map(AuditLog::try_from).collect()
    }
}

impl AuditLogRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<AuditLog>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<AuditLog>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {AUDIT_LOG_PROJECTION} FROM audit_log WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let logs: Vec<AuditLogDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse audit_log: {e}")))?;

        logs.into_iter().next().map(AuditLog::try_from).transpose()
    }
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
    use crate::StorageConfig;

    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::AuditAction;

    use chrono::Utc;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(50000);

    const AUDIT_SCHEMA: &str = r"
        DEFINE TABLE audit_log SCHEMALESS;
    ";

    async fn setup_test_repo() -> Result<AuditLogRepository, Box<dyn std::error::Error>> {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("audit_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await?;

        client.query(AUDIT_SCHEMA).await?;

        Ok(AuditLogRepository::new(client))
    }

    fn create_test_log(action: AuditAction) -> AuditLog {
        let id = RecordId::new_v4(TableName::AuditLog);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let record_id = RecordId::new_v4(TableName::Entity);

        AuditLog {
            base,
            action,
            actor: "test_agent".to_string(),
            record: record_id,
            before: None,
            after: None,
            timestamp: Utc::now(),
            reason: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_audit_log() -> Result<(), Box<dyn std::error::Error>> {
        let repo = setup_test_repo().await?;
        let log = create_test_log(AuditAction::Create);

        let created = repo.create(log.clone()).await?;

        assert_eq!(created.action, AuditAction::Create);
        Ok(())
    }
}
