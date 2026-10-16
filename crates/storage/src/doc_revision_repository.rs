//! Document revision repository implementation for `SurrealDB`.
//!
//! Per spec §7.1, doc_revision represents immutable text snapshots from renderer plugins.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::doc::Doc;
use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
use elephant_types::records::entity::Entity;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

pub(crate) const DOC_REVISION_PROJECTION: &str = r"
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
    doc,
    text,
    text_hash,
    renderer,
    manifest,
    supersedes,
    status,
    rendered_at
";

/// Database-only proxy for DocRevision to avoid flatten/native enum issues.
#[derive(Deserialize)]
pub(crate) struct DocRevisionDb {
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
    doc: Option<Link<Doc>>,
    text: Option<String>,
    text_hash: Option<String>,
    renderer: Option<RendererMetadata>,
    manifest: Option<elephant_types::records::RenderManifest>,
    supersedes: Option<Link<DocRevision>>,
    status: Option<RevisionStatus>,
    rendered_at: Option<chrono::DateTime<Utc>>,
}

impl TryFrom<DocRevisionDb> for DocRevision {
    type Error = StorageError;

    fn try_from(db: DocRevisionDb) -> Result<Self, Self::Error> {
        Ok(Self {
            base: BaseFields {
                id: crate::surreal::record_id_from_thing(&db.id)
                    .map_err(|e| StorageError::QueryError(e.to_string()))?,
                space_id: db
                    .space_id
                    .ok_or_else(|| StorageError::QueryError("Missing space_id".to_string()))?,
                created_at: db
                    .created_at
                    .ok_or_else(|| StorageError::QueryError("Missing created_at".to_string()))?,
                updated_at: db
                    .updated_at
                    .ok_or_else(|| StorageError::QueryError("Missing updated_at".to_string()))?,
                created_by: db
                    .created_by
                    .ok_or_else(|| StorageError::QueryError("Missing created_by".to_string()))?,
                deleted_at: db.deleted_at,
                provenance: db
                    .provenance
                    .ok_or_else(|| StorageError::QueryError("Missing provenance".to_string()))?,
                security: db
                    .security
                    .ok_or_else(|| StorageError::QueryError("Missing security".to_string()))?,
                subjects: db
                    .subjects
                    .ok_or_else(|| StorageError::QueryError("Missing subjects".to_string()))?,
                subjects_state: db.subjects_state.ok_or_else(|| {
                    StorageError::QueryError("Missing subjects_state".to_string())
                })?,
            },
            doc: db
                .doc
                .ok_or_else(|| StorageError::QueryError("Missing doc".to_string()))?,
            text: db
                .text
                .ok_or_else(|| StorageError::QueryError("Missing text".to_string()))?,
            text_hash: db
                .text_hash
                .ok_or_else(|| StorageError::QueryError("Missing text_hash".to_string()))?,
            renderer: db
                .renderer
                .ok_or_else(|| StorageError::QueryError("Missing renderer".to_string()))?,
            manifest: db.manifest,
            supersedes: db.supersedes,
            status: db
                .status
                .ok_or_else(|| StorageError::QueryError("Missing status".to_string()))?,
            rendered_at: db
                .rendered_at
                .ok_or_else(|| StorageError::QueryError("Missing rendered_at".to_string()))?,
        })
    }
}

/// Document revision repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct DocRevisionRepository {
    client: StorageClient,
}

impl DocRevisionRepository {
    /// Creates a new document revision repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Shares the client for preparation through the canonical transaction owner.
    #[must_use]
    pub fn storage_client(&self) -> StorageClient {
        self.client.clone()
    }

    /// Gets multiple revisions by ID while preserving the requested order.
    ///
    /// Missing or soft-deleted revisions are skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage lookup fails.
    pub async fn get_many(
        &self,
        space_id: &str,
        ids: &[RecordId],
    ) -> StorageResult<Vec<DocRevision>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(store) = self.client.embedded_store() {
            return store.get_many::<DocRevision>(space_id, ids, false).await;
        }

        let mut revisions = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(revision) = self.get(space_id, id).await? {
                revisions.push(revision);
            }
        }
        Ok(revisions)
    }

    /// Finds revisions by document ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn find_by_doc(&self, doc_id: &RecordId) -> StorageResult<Vec<DocRevision>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<DocRevision>()
                .await?
                .into_iter()
                .filter(|item| item.base.deleted_at.is_none() && item.doc.id() == doc_id)
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            return Ok(items);
        }

        // The doc field is stored as a string "table:uuid" via JSON serialization,
        // so we compare against the string representation
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_REVISION_PROJECTION} FROM doc_revision WHERE doc = $doc_id AND deleted_at IS NONE ORDER BY created_at DESC"
                ),
                crate::bindings! { "doc_id" => crate::surreal::thing_from_record_id(doc_id) },
            )
            .await?;

        let items: Vec<DocRevisionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_revisions: {e}")))?;

        items.into_iter().map(DocRevision::try_from).collect()
    }

    /// Gets a revision by globally unique revision ID.
    ///
    /// This intentionally does not require the caller to know the space first. It is used by
    /// pipeline stages that already hold a concrete revision ID and should not scan/sort all
    /// active revisions merely to recover the row.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn get_by_id(&self, id: &RecordId) -> StorageResult<Option<DocRevision>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get_by_id::<DocRevision>(id, false).await;
        }

        // Record access (`FROM ONLY $id`): the planner has no id-equality
        // fast path, so the `FROM doc_revision WHERE id = $id` form scanned
        // (and deserialized) every revision text on each lookup (issue #34).
        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {DOC_REVISION_PROJECTION} FROM ONLY $id WHERE deleted_at IS NONE"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id) },
            )
            .await?;

        let item: Option<DocRevisionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_revision: {e}")))?;

        item.map(DocRevision::try_from).transpose()
    }

    /// Finds revisions by status.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn find_by_status(&self, status: RevisionStatus) -> StorageResult<Vec<DocRevision>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<DocRevision>()
                .await?
                .into_iter()
                .filter(|item| item.base.deleted_at.is_none() && item.status == status)
                .collect();
            items.sort_by_key(|item| std::cmp::Reverse(item.base.created_at));
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_REVISION_PROJECTION} FROM doc_revision WHERE status = $status AND deleted_at IS NONE ORDER BY created_at DESC"
                ),
                crate::bindings! { "status" => status.to_string() },
            )
            .await?;

        let items: Vec<DocRevisionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_revisions: {e}")))?;

        items.into_iter().map(DocRevision::try_from).collect()
    }

    /// Converts a DocRevision to a SurrealDB value for insertion.
    pub(crate) fn doc_revision_to_value(
        item: &DocRevision,
    ) -> StorageResult<surrealdb::sql::Value> {
        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert(
            "doc".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(item.doc.id())),
        );
        obj.insert(
            "text".to_string(),
            surrealdb::sql::Value::from(item.text.clone()),
        );
        obj.insert(
            "text_hash".to_string(),
            surrealdb::sql::Value::from(item.text_hash.clone()),
        );
        obj.insert(
            "renderer".to_string(),
            crate::surreal::json_to_surreal(serde_json::to_value(&item.renderer).map_err(
                |e| {
                    StorageError::SerializationError(format!(
                        "Failed to serialize doc_revision.renderer: {e}"
                    ))
                },
            )?)?,
        );
        obj.insert(
            "manifest".to_string(),
            item.manifest
                .as_ref()
                .map_or(Ok(surrealdb::sql::Value::None), |value| {
                    crate::surreal::json_to_surreal(serde_json::to_value(value).map_err(|e| {
                        StorageError::SerializationError(format!(
                            "Failed to serialize doc_revision.manifest: {e}"
                        ))
                    })?)
                })?,
        );
        obj.insert(
            "supersedes".to_string(),
            item.supersedes
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |value| {
                    surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(value.id()))
                }),
        );
        obj.insert(
            "status".to_string(),
            crate::surreal::enum_to_surreal(&item.status)?,
        );
        obj.insert(
            "rendered_at".to_string(),
            surrealdb::sql::Value::from(crate::surreal::datetime_from_chrono(item.rendered_at)),
        );
        Ok(surrealdb::sql::Value::Object(obj))
    }
}

#[async_trait]
impl Repository<DocRevision> for DocRevisionRepository {
    async fn create(&self, record: DocRevision) -> StorageResult<DocRevision> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::doc_revision_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {DOC_REVISION_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<DocRevisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created doc_revision: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but doc_revision not found".to_string())
        })?;

        DocRevision::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<DocRevision>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<DocRevision>(space_id, id, false).await;
        }

        // Record access; the space_id/deleted_at WHERE clause preserves space
        // isolation — a cross-space id resolves to NONE.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_REVISION_PROJECTION} FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let item: Option<DocRevisionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_revision: {e}")))?;

        Ok(item.map(DocRevision::try_from).transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: DocRevision,
    ) -> StorageResult<DocRevision> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {DOC_REVISION_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::doc_revision_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<DocRevisionDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated doc_revision: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::NotFound(format!("DocRevision not found in space: {id}"))
        })?;

        DocRevision::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<DocRevision>(space_id, id).await;
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
    ) -> StorageResult<Vec<DocRevision>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<DocRevision>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {DOC_REVISION_PROJECTION} FROM doc_revision WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {DOC_REVISION_PROJECTION} FROM doc_revision WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<DocRevisionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_revisions: {e}")))?;

        items.into_iter().map(DocRevision::try_from).collect()
    }
}

impl DocRevisionRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<DocRevision>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<DocRevision>(space_id, id, true).await;
        }

        // Record access; the space_id/deleted_at WHERE clause preserves space
        // isolation — a cross-space id resolves to NONE.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_REVISION_PROJECTION} FROM ONLY $id WHERE space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let item: Option<DocRevisionDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_revision: {e}")))?;

        item.map(DocRevision::try_from).transpose()
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80100);

    const DOC_REVISION_SCHEMA: &str = r"
        DEFINE TABLE doc_revision SCHEMALESS;
    ";

    async fn setup_test_repo() -> DocRevisionRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("doc_revision_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(DOC_REVISION_SCHEMA).await.unwrap();

        DocRevisionRepository::new(client)
    }

    fn make_doc_revision_db() -> DocRevisionDb {
        DocRevisionDb {
            id: surrealdb::sql::Thing::from(("doc_revision", "rev_1")),
            space_id: Some("test_space".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("test_agent".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            doc: Some(RecordId::new_v4(TableName::Doc).into()),
            text: Some("Hello world".to_string()),
            text_hash: Some("hash".to_string()),
            renderer: Some(RendererMetadata {
                name: "default".to_string(),
                version: "1.0".to_string(),
                config_hash: None,
            }),
            manifest: None,
            supersedes: None,
            status: Some(RevisionStatus::Active),
            rendered_at: Some(Utc::now()),
        }
    }

    #[test]
    fn test_doc_revision_missing_space_id_errors() {
        let mut db = make_doc_revision_db();
        db.space_id = None;

        let result = DocRevision::try_from(db);
        assert!(result.is_err(), "Missing space_id should error");
    }

    #[test]
    fn test_doc_revision_missing_status_errors() {
        let mut db = make_doc_revision_db();
        db.status = None;

        let result = DocRevision::try_from(db);
        assert!(result.is_err(), "Missing status should error");
    }

    fn create_test_doc_revision() -> DocRevision {
        let id = RecordId::new_v4(TableName::DocRevision);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let doc_id = RecordId::new_v4(TableName::Doc);

        DocRevision {
            base,
            doc: Link::new(doc_id),
            text: "Test document content".to_string(),
            text_hash: "abc123hash".to_string(),
            renderer: RendererMetadata {
                name: "default".to_string(),
                version: "1.0.0".to_string(),
                config_hash: None,
            },
            manifest: None,
            supersedes: None,
            status: RevisionStatus::Active,
            rendered_at: Utc::now(),
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_doc_revision() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_revision();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.text, "Test document content");
        assert_eq!(created.status, RevisionStatus::Active);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_doc_revision() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_revision();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().text_hash, "abc123hash");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_doc_revision_by_id() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_revision();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get_by_id(&id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().base.id, id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_doc_revision_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_doc_revision();
        item.text = "Detailed content for roundtrip test".to_string();
        item.renderer.config_hash = Some("config_hash_123".to_string());

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.text, "Detailed content for roundtrip test");
        assert_eq!(created.renderer.name, "default");
        assert_eq!(
            created.renderer.config_hash,
            Some("config_hash_123".to_string())
        );
        assert_eq!(created.status, RevisionStatus::Active);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_doc_revision() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_revision();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = RevisionStatus::Superseded;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.status, RevisionStatus::Superseded);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_doc_revision() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_revision();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_none());
    }

    async fn assert_cross_space_get_is_none(repo: &DocRevisionRepository) {
        let item = create_test_doc_revision();
        let id = item.base.id.clone();
        repo.create(item).await.unwrap();

        // Space isolation: a known id must not resolve from another space.
        let cross_space = repo.get("other_space", &id).await.unwrap();
        assert!(cross_space.is_none(), "cross-space get must return None");

        // The owning space still sees it.
        assert!(repo.get("test_space", &id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_doc_revision_cross_space_get_is_none() {
        let repo = setup_test_repo().await;
        assert_cross_space_get_is_none(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep client in the original fixture scope through assertions and cleanup."
    )]
    async fn test_doc_revision_cross_space_get_is_none_embedded() {
        let client = StorageClient::new_embedded().await.unwrap();
        let repo = DocRevisionRepository::new(client);
        assert_cross_space_get_is_none(&repo).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_by_id_skips_deleted_revision() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_revision();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        assert!(repo.get_by_id(&id).await.unwrap().is_none());
    }
}
