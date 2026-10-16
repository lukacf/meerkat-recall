//! Work artifact repository implementation for `SurrealDB`.
//!
//! Per spec §5, work artifacts are staged outputs from work runs.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::work::{ArtifactStatus, WorkArtifact};
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

pub(crate) const WORK_ARTIFACT_PROJECTION: &str = r"
    id,
    space_id,
    created_at,
    updated_at,
    created_by,
    deleted_at,
    security,
    IF provenance IS NOT NONE THEN {
        sources: IF provenance.sources IS NOT NONE THEN array::map(provenance.sources, |$s| string::concat(meta::tb($s), ':', meta::id($s))) ELSE NONE END,
        extractor: provenance.extractor,
        run_id: provenance.run_id,
        work_run_id: provenance.work_run_id,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    subjects,
    subjects_state,
    work_run,
    artifact_type,
    data,
    status,
    validation_errors,
    validated_at,
    committed_at
";

/// Database-only proxy for WorkArtifact.
#[derive(Deserialize)]
pub(crate) struct WorkArtifactDb {
    id: surrealdb::sql::Thing,
    space_id: Option<String>,
    created_at: Option<chrono::DateTime<Utc>>,
    updated_at: Option<chrono::DateTime<Utc>>,
    created_by: Option<String>,
    deleted_at: Option<chrono::DateTime<Utc>>,
    provenance: Option<Provenance>,
    security: Option<Security>,
    subjects: Option<Vec<Link<Entity>>>,
    subjects_state: Option<SubjectsState>,
    work_run: Option<surrealdb::sql::Thing>,
    artifact_type: Option<String>,
    data: Option<serde_json::Value>,
    status: Option<ArtifactStatus>,
    validation_errors: Option<Vec<String>>,
    validated_at: Option<chrono::DateTime<Utc>>,
    committed_at: Option<chrono::DateTime<Utc>>,
}

impl TryFrom<WorkArtifactDb> for WorkArtifact {
    type Error = StorageError;

    fn try_from(db: WorkArtifactDb) -> Result<Self, Self::Error> {
        fn required<T>(value: Option<T>, field: &str) -> Result<T, StorageError> {
            value.ok_or_else(|| StorageError::QueryError(format!("Missing {field}")))
        }

        let work_run_thing = db
            .work_run
            .ok_or_else(|| StorageError::QueryError("Missing work_run".to_string()))?;
        let work_run_id = crate::surreal::record_id_from_thing(&work_run_thing)
            .map_err(|e| StorageError::QueryError(e.to_string()))?;

        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: required(db.space_id, "space_id")?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: required(db.created_by, "created_by")?,
                deleted_at: db.deleted_at,
                provenance: required(db.provenance, "provenance")?,
                security: required(db.security, "security")?,
                subjects: required(db.subjects, "subjects")?,
                subjects_state: required(db.subjects_state, "subjects_state")?,
            },
            work_run: Link::new(work_run_id),
            artifact_type: db
                .artifact_type
                .ok_or_else(|| StorageError::QueryError("Missing artifact_type".to_string()))?,
            data: required(db.data, "data")?,
            status: required(db.status, "status")?,
            validation_errors: db.validation_errors,
            validated_at: db.validated_at,
            committed_at: db.committed_at,
        })
    }
}

/// Work artifact repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct WorkArtifactRepository {
    client: StorageClient,
}

impl WorkArtifactRepository {
    /// Creates a new work artifact repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds the most recent artifact for an assertion by type.
    ///
    /// Returns `Ok(None)` if no artifact exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or parsing fails.
    pub async fn find_latest_by_type_and_assertion(
        &self,
        space_id: &str,
        artifact_type: &str,
        assertion_id: &RecordId,
    ) -> StorageResult<Option<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<WorkArtifact>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.artifact_type == artifact_type
                        && item.status != ArtifactStatus::Rejected
                        && item
                            .data
                            .get("assertion_id")
                            .and_then(serde_json::Value::as_str)
                            == Some(assertion_id.to_string().as_str())
                })
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            return Ok(items.into_iter().next());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact \
                     WHERE space_id = $space_id \
                       AND artifact_type = $artifact_type \
                       AND data.assertion_id = $assertion_id \
                       AND status != 'rejected' \
                       AND deleted_at IS NONE \
                     ORDER BY created_at DESC LIMIT 1"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "artifact_type" => artifact_type,
                    "assertion_id" => assertion_id.to_string(),
                },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_artifact: {e}")))?;

        match items.into_iter().next() {
            Some(item) => Ok(Some(WorkArtifact::try_from(item)?)),
            None => Ok(None),
        }
    }

    /// Converts a WorkArtifact to a SurrealDB value for insertion.
    pub(crate) fn work_artifact_to_value(
        item: &WorkArtifact,
    ) -> StorageResult<surrealdb::sql::Value> {
        use std::collections::BTreeMap;
        use surrealdb::sql::{Array, Object, Value};

        let mut obj: BTreeMap<String, Value> = BTreeMap::new();
        obj.insert("space_id".to_string(), item.base.space_id.clone().into());
        obj.insert(
            "created_at".to_string(),
            crate::surreal::datetime_from_chrono(item.base.created_at).into(),
        );
        obj.insert(
            "updated_at".to_string(),
            crate::surreal::datetime_from_chrono(item.base.updated_at).into(),
        );
        obj.insert(
            "created_by".to_string(),
            item.base.created_by.clone().into(),
        );
        obj.insert(
            "deleted_at".to_string(),
            item.base.deleted_at.map_or(Value::None, |dt| {
                crate::surreal::datetime_from_chrono(dt).into()
            }),
        );
        obj.insert(
            "provenance".to_string(),
            crate::surreal::provenance_to_value(&item.base.provenance)?,
        );
        obj.insert(
            "security".to_string(),
            crate::surreal::security_to_value(&item.base.security),
        );
        obj.insert(
            "subjects".to_string(),
            Value::Array(Array::from(
                item.base
                    .subjects
                    .iter()
                    .map(|l| Value::from(crate::surreal::thing_from_record_id(l.id())))
                    .collect::<Vec<Value>>(),
            )),
        );
        obj.insert(
            "subjects_state".to_string(),
            crate::surreal::json_to_surreal(serde_json::json!(item.base.subjects_state))?,
        );
        // Convert Link to native Thing for proper query matching
        obj.insert(
            "work_run".to_string(),
            crate::surreal::thing_from_record_id(item.work_run.id()).into(),
        );
        obj.insert(
            "artifact_type".to_string(),
            item.artifact_type.clone().into(),
        );
        obj.insert(
            "data".to_string(),
            crate::surreal::json_to_surreal(item.data.clone())?,
        );
        obj.insert("status".to_string(), item.status.to_string().into());
        obj.insert(
            "validation_errors".to_string(),
            item.validation_errors.as_ref().map_or(Value::None, |v| {
                Value::Array(Array::from(
                    v.iter()
                        .map(|s| Value::from(s.clone()))
                        .collect::<Vec<Value>>(),
                ))
            }),
        );
        obj.insert(
            "validated_at".to_string(),
            item.validated_at.map_or(Value::None, |dt| {
                crate::surreal::datetime_from_chrono(dt).into()
            }),
        );
        obj.insert(
            "committed_at".to_string(),
            item.committed_at.map_or(Value::None, |dt| {
                crate::surreal::datetime_from_chrono(dt).into()
            }),
        );

        Ok(Value::Object(Object::from(obj)))
    }
}

#[async_trait]
impl Repository<WorkArtifact> for WorkArtifactRepository {
    async fn create(&self, record: WorkArtifact) -> StorageResult<WorkArtifact> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::work_artifact_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {WORK_ARTIFACT_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created work_artifact: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but work_artifact not found".to_string())
        })?;

        WorkArtifact::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<WorkArtifact>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_artifact: {e}")))?;

        Ok(items
            .into_iter()
            .next()
            .map(WorkArtifact::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: WorkArtifact,
    ) -> StorageResult<WorkArtifact> {
        let mut normalized = record;
        normalized.base.space_id = space_id.to_string();
        normalized.base.id.clone_from(id);

        if let Some(store) = self.client.embedded_store() {
            return store.update_existing(space_id, id, normalized).await;
        }

        let record_value = Self::work_artifact_to_value(&normalized)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!(
                    "UPDATE $id CONTENT $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {WORK_ARTIFACT_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated work_artifact: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("WorkArtifact not found in space: {id}"))
        })?;

        WorkArtifact::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<WorkArtifact>(space_id, id).await;
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
    ) -> StorageResult<Vec<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<WorkArtifact>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<WorkArtifactDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_artifacts: {e}"))
        })?;

        items.into_iter().map(WorkArtifact::try_from).collect()
    }
}

/// Additional work-artifact-specific operations.
impl WorkArtifactRepository {
    /// Finds artifacts by status.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_status(
        &self,
        space_id: &str,
        status: ArtifactStatus,
    ) -> StorageResult<Vec<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<WorkArtifact>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.status == status
                })
                .collect();
            items.sort_by_key(|item| item.base.created_at);
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact WHERE space_id = $space_id AND status = $status AND deleted_at IS NONE ORDER BY created_at ASC"
                ),
                crate::bindings! { "space_id" => space_id, "status" => status.to_string() },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_artifacts: {e}"))
        })?;

        items.into_iter().map(WorkArtifact::try_from).collect()
    }

    /// Finds artifacts by work run.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn find_by_work_run(
        &self,
        work_run_id: &RecordId,
    ) -> StorageResult<Vec<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<WorkArtifact>()
                .await?
                .into_iter()
                .filter(|item| item.base.deleted_at.is_none() && item.work_run.id() == work_run_id)
                .collect();
            items.sort_by_key(|item| item.base.created_at);
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact WHERE work_run = $work_run_id AND deleted_at IS NONE ORDER BY created_at ASC"
                ),
                crate::bindings! { "work_run_id" => crate::surreal::thing_from_record_id(work_run_id) },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse work_artifacts: {e}"))
        })?;

        items.into_iter().map(WorkArtifact::try_from).collect()
    }

    /// Updates a work artifact only if it matches the expected status.
    ///
    /// Returns `Ok(None)` when the status does not match, indicating another
    /// worker has already transitioned the artifact.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn update_if_status(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: WorkArtifact,
        expected_status: ArtifactStatus,
    ) -> StorageResult<Option<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let Some(existing) = mutation.get::<WorkArtifact>(space_id, id, false)? else {
                        return Ok(None);
                    };
                    if existing.status != expected_status {
                        return Ok(None);
                    }

                    record.base.id = id.clone();
                    record.base.space_id = space_id.to_string();
                    record.base.updated_at = Utc::now();
                    Ok(Some(mutation.replace(record)?))
                })
                .await;
        }

        record.base.space_id = space_id.to_string();
        record.base.id.clone_from(id);
        record.base.updated_at = Utc::now();

        let mut response = self
            .client
            .query_bind_checked(
                "UPDATE $id CONTENT $record WHERE space_id = $space_id AND status = $status AND deleted_at IS NONE RETURN AFTER",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "status" => expected_status.to_string(),
                    "record" => Self::work_artifact_to_value(&record)?,
                },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated work_artifact: {e}"))
        })?;
        if items.is_empty() {
            return Ok(None);
        }

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Empty result after successful update".to_string())
        })?;

        Ok(Some(WorkArtifact::try_from(item_db)?))
    }
}

impl WorkArtifactRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<WorkArtifact>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<WorkArtifact>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {WORK_ARTIFACT_PROJECTION} FROM work_artifact WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<WorkArtifactDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse work_artifact: {e}")))?;

        items
            .into_iter()
            .next()
            .map(WorkArtifact::try_from)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]
    use super::*;
    use crate::StorageConfig;
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80700);

    const WORK_ARTIFACT_SCHEMA: &str = r"
        DEFINE TABLE work_artifact SCHEMALESS;
    ";

    async fn setup_test_repo() -> WorkArtifactRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("work_artifact_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(WORK_ARTIFACT_SCHEMA).await.unwrap();

        WorkArtifactRepository::new(client)
    }

    fn create_test_work_artifact() -> WorkArtifact {
        let id = RecordId::new_v4(TableName::WorkArtifact);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let work_run_id = RecordId::new_v4(TableName::WorkRun);

        WorkArtifact {
            base,
            work_run: Link::new(work_run_id),
            artifact_type: "knowledge_candidates".to_string(),
            data: serde_json::json!({"assertions": []}),
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_work_artifact() {
        let repo = setup_test_repo().await;
        let item = create_test_work_artifact();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.artifact_type, "knowledge_candidates");
        assert_eq!(created.status, ArtifactStatus::Draft);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_work_artifact() {
        let repo = setup_test_repo().await;
        let item = create_test_work_artifact();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_work_artifact_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_work_artifact();
        item.data = serde_json::json!({"key": "value", "count": 42});

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.data["key"], "value");
        assert_eq!(created.data["count"], 42);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo, sources in the original fixture scope through assertions and cleanup."
    )]
    async fn native_source_provenance_survives_artifact_create_and_replacement() {
        use elephant_types::records::base::{Extractor, ProvenanceStep};
        use elephant_types::records::source::{Source, SourceType};

        let client = StorageClient::new_memory().await.unwrap();
        let repo = WorkArtifactRepository::new(client.clone());
        let sources = crate::SourceRepository::new(client.clone());
        let mut source_ids = Vec::new();
        for name in ["first", "second"] {
            let source = Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "test_space".to_string(),
                    "test_agent".to_string(),
                ),
                SourceType::Manual,
                format!("source-{name}"),
            );
            source_ids.push(sources.create(source).await.unwrap().base.id);
        }
        let mut expected = create_test_work_artifact();
        let id = expected.base.id.clone();
        expected.base.provenance = Provenance {
            sources: source_ids.clone(),
            extractor: Some(Extractor {
                name: Some("native-provenance-regression".to_string()),
                version: Some("1".to_string()),
                model: Some("deterministic".to_string()),
            }),
            work_run_id: Some(expected.work_run.id().clone()),
            run_id: Some("original-run".to_string()),
            prompt_id: Some("original-prompt".to_string()),
            config_id: Some("original-config".to_string()),
            chain: vec![ProvenanceStep {
                step: "extraction".to_string(),
                input: source_ids[0].to_string(),
                output: id.to_string(),
            }],
        };
        expected.base.security = Security {
            policy_id: "default".to_string(),
            level: 2,
            labels: vec!["private".to_string()],
            handling: vec!["restricted".to_string()],
        };
        expected.data = serde_json::json!({"assertions": [{"text": "source-backed output"}]});

        assert_eq!(repo.create(expected.clone()).await.unwrap(), expected);
        assert_eq!(
            repo.get("test_space", &id).await.unwrap(),
            Some(expected.clone())
        );
        assert_native_artifact_sources(&client, &id, &source_ids).await;

        expected.base.provenance.sources.remove(0);
        expected.base.provenance.chain.clear();
        expected.base.provenance.prompt_id = None;
        expected.data = serde_json::json!({"replacement": true});
        assert_eq!(
            repo.update("test_space", &id, expected.clone())
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            repo.get("test_space", &id).await.unwrap(),
            Some(expected.clone())
        );
        assert_native_artifact_sources(&client, &id, &source_ids[1..]).await;

        expected.status = ArtifactStatus::Validated;
        expected.validated_at = Some(Utc::now());
        let conditional = repo
            .update_if_status("test_space", &id, expected.clone(), ArtifactStatus::Draft)
            .await
            .unwrap()
            .expect("the original draft is replaced");
        expected.base.updated_at = conditional.base.updated_at;
        assert_eq!(conditional, expected);
        assert_native_artifact_sources(&client, &id, &source_ids[1..]).await;

        let mut stale = expected.clone();
        stale.base.provenance.sources.clear();
        stale.data = serde_json::json!({"stale": true});
        assert!(repo
            .update_if_status("test_space", &id, stale, ArtifactStatus::Draft)
            .await
            .unwrap()
            .is_none());
        assert_eq!(repo.get("test_space", &id).await.unwrap(), Some(expected));
        assert_native_artifact_sources(&client, &id, &source_ids[1..]).await;
    }

    async fn assert_native_artifact_sources(
        client: &StorageClient,
        id: &RecordId,
        sources: &[RecordId],
    ) {
        let mut response = client
            .query_bind_checked(
                "SELECT VALUE provenance.sources FROM ONLY $id;",
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id) },
            )
            .await
            .unwrap();
        let actual: Vec<surrealdb::sql::Thing> = response.take(0).unwrap();
        let expected = sources
            .iter()
            .map(crate::surreal::thing_from_record_id)
            .collect::<Vec<_>>();
        assert_eq!(
            actual, expected,
            "physical provenance must contain native records"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_work_artifact() {
        let repo = setup_test_repo().await;
        let item = create_test_work_artifact();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ArtifactStatus::Validated;
        updated.validated_at = Some(Utc::now());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.status, ArtifactStatus::Validated);
        assert!(result.validated_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_work_artifact() {
        let repo = setup_test_repo().await;
        let item = create_test_work_artifact();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_matches() {
        let repo = setup_test_repo().await;
        let item = create_test_work_artifact();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ArtifactStatus::Validated;
        updated.validated_at = Some(Utc::now());

        let result = repo
            .update_if_status("test_space", &id, updated, ArtifactStatus::Draft)
            .await
            .unwrap();

        let result = result.expect("status match");
        assert_eq!(result.status, ArtifactStatus::Validated);
        assert!(result.validated_at.is_some());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_mismatch_returns_none() {
        let repo = setup_test_repo().await;
        let item = create_test_work_artifact();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ArtifactStatus::Validated;
        updated.validated_at = Some(Utc::now());

        let _ = repo
            .update_if_status("test_space", &id, updated.clone(), ArtifactStatus::Draft)
            .await
            .unwrap();

        let result = repo
            .update_if_status("test_space", &id, updated, ArtifactStatus::Draft)
            .await
            .unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_if_status_uses_target_id_not_payload_id() {
        let repo = setup_test_repo().await;
        let target = repo.create(create_test_work_artifact()).await.unwrap();
        let other = repo.create(create_test_work_artifact()).await.unwrap();

        let mut payload = other.clone();
        payload.status = ArtifactStatus::Validated;
        payload.validated_at = Some(Utc::now());

        let updated = repo
            .update_if_status(
                "test_space",
                &target.base.id,
                payload,
                ArtifactStatus::Draft,
            )
            .await
            .unwrap()
            .expect("status match");

        assert_eq!(updated.base.id, target.base.id);

        let fetched_target = repo
            .get("test_space", &target.base.id)
            .await
            .unwrap()
            .expect("target artifact");
        assert_eq!(fetched_target.status, ArtifactStatus::Validated);

        let fetched_other = repo
            .get("test_space", &other.base.id)
            .await
            .unwrap()
            .expect("other artifact");
        assert_eq!(fetched_other.status, ArtifactStatus::Draft);
    }

    #[test]
    fn test_work_artifact_missing_space_id_errors() {
        let db = WorkArtifactDb {
            id: surrealdb::sql::Thing::from(("work_artifact", "missing_space_id")),
            space_id: None,
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("test_agent".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            work_run: Some(surrealdb::sql::Thing::from(("work_run", "run_1"))),
            artifact_type: Some("knowledge_candidates".to_string()),
            data: Some(serde_json::json!({})),
            status: Some(ArtifactStatus::Draft),
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };

        let result = WorkArtifact::try_from(db);
        assert!(result.is_err(), "Missing space_id should error");
    }

    #[test]
    fn test_work_artifact_missing_data_errors() {
        let db = WorkArtifactDb {
            id: surrealdb::sql::Thing::from(("work_artifact", "missing_data")),
            space_id: Some("test_space".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("test_agent".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            work_run: Some(surrealdb::sql::Thing::from(("work_run", "run_1"))),
            artifact_type: Some("knowledge_candidates".to_string()),
            data: None,
            status: Some(ArtifactStatus::Draft),
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };

        let result = WorkArtifact::try_from(db);
        assert!(result.is_err(), "Missing data should error");
    }
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn nested_replacement_contract(client: StorageClient) {
        let repo = WorkArtifactRepository::new(client);
        let mut original = create_test_work_artifact();
        original.data = serde_json::json!({"coverage":{"required":["span-a"]},"nested":{"remove":true,"keep":1}});
        original.validation_errors = Some(vec!["previous validation".into()]);
        let original = repo.create(original).await.expect("original artifact");
        let mut changed = original.clone();
        changed.data = serde_json::json!({"nested":{"keep":2}});
        changed.validation_errors = None;
        repo.update("test_space", &original.base.id, changed.clone())
            .await
            .expect("full replacement");
        let stored = repo
            .get("test_space", &original.base.id)
            .await
            .expect("read actual storage")
            .expect("present");
        assert_eq!(stored.data, changed.data);
        assert!(stored.data.get("coverage").is_none());
        assert!(stored.validation_errors.is_none());
        assert_eq!(stored.base.id, original.base.id);
        assert_eq!(stored.base.created_at, original.base.created_at);
        assert_eq!(stored.base.security, original.base.security);
        let mut conditional = stored.clone();
        conditional.data = serde_json::json!({});
        conditional.status = ArtifactStatus::Validated;
        repo.update_if_status(
            "test_space",
            &original.base.id,
            conditional,
            ArtifactStatus::Draft,
        )
        .await
        .expect("conditional replacement")
        .expect("expected status");
        let actual = repo
            .get("test_space", &original.base.id)
            .await
            .expect("read conditional")
            .expect("present");
        assert_eq!(actual.data, serde_json::json!({}));
        assert_eq!(actual.status, ArtifactStatus::Validated);
        let mut forbidden = actual.clone();
        forbidden.data = serde_json::json!({"unexpected":true});
        assert!(repo
            .update_if_status(
                "test_space",
                &original.base.id,
                forbidden,
                ArtifactStatus::Draft
            )
            .await
            .expect("status mismatch")
            .is_none());
        assert_eq!(
            repo.get("test_space", &original.base.id)
                .await
                .expect("unchanged"),
            Some(actual)
        );
    }

    #[tokio::test]
    async fn embedded_artifact_updates_replace_removed_nested_fields() {
        nested_replacement_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    async fn surreal_artifact_updates_replace_removed_nested_fields() {
        nested_replacement_contract(StorageClient::new_memory().await.expect("native memory"))
            .await;
    }
}
