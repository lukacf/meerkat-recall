//! Attribute repository implementation for `SurrealDB`.
//!
//! Per spec §11.2, attributes represent resolved entity-to-literal claims.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;

use elephant_types::records::attribute::{Attribute, Literal};
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::entity::Entity;
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, ValidityWindow};
use elephant_types::records::temporal::TemporalAudit;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

const ATTRIBUTE_PROJECTION: &str = r"
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
        work_run_id: IF provenance.work_run_id IS NOT NONE THEN (IF type::is::record(provenance.work_run_id) THEN string::concat(meta::tb(provenance.work_run_id), ':', meta::id(provenance.work_run_id)) ELSE provenance.work_run_id END) ELSE NONE END,
        prompt_id: provenance.prompt_id,
        config_id: provenance.config_id,
        chain: provenance.chain
    } ELSE NONE END AS provenance,
    IF subjects IS NOT NONE THEN array::map(subjects, |$s| IF type::is::record($s) THEN string::concat(meta::tb($s), ':', meta::id($s)) ELSE $s END) ELSE [] END AS subjects,
    subjects_state,
    IF subject IS NOT NONE THEN (IF type::is::record(subject) THEN string::concat(meta::tb(subject), ':', meta::id(subject)) ELSE subject END) ELSE NONE END AS subject,
    attr_type,
    value,
    status,
    confidence,
    validity_window,
    evidence,
    temporal_audit
";

/// Database-only proxy for Attribute to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct AttributeDb {
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
    subject: Option<Link<Entity>>,
    attr_type: Option<String>,
    value: Option<Literal>,
    status: Option<ClaimStatus>,
    confidence: Option<f32>,
    validity_window: Option<ValidityWindow>,
    evidence: Option<Vec<EvidenceRef>>,
    temporal_audit: Option<TemporalAudit>,
}

impl TryFrom<AttributeDb> for Attribute {
    type Error = StorageError;

    fn try_from(db: AttributeDb) -> Result<Self, Self::Error> {
        fn required<T>(value: Option<T>, field: &str) -> Result<T, StorageError> {
            value.ok_or_else(|| StorageError::QueryError(format!("Missing {field}")))
        }

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
            subject: db
                .subject
                .ok_or_else(|| StorageError::QueryError("Missing subject".to_string()))?,
            attr_type: db
                .attr_type
                .ok_or_else(|| StorageError::QueryError("Missing attr_type".to_string()))?,
            value: db
                .value
                .ok_or_else(|| StorageError::QueryError("Missing value".to_string()))?,
            status: required(db.status, "status")?,
            confidence: required(db.confidence, "confidence")?,
            validity_window: db.validity_window,
            evidence: required(db.evidence, "evidence")?,
            temporal_audit: db.temporal_audit,
        })
    }
}

/// Attribute repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct AttributeRepository {
    client: StorageClient,
}

impl AttributeRepository {
    /// Creates a new attribute repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Shares the client for preparation through the canonical transaction owner.
    #[must_use]
    pub fn storage_client(&self) -> StorageClient {
        self.client.clone()
    }

    /// Finds an attribute by subject, type, and literal value.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_by_subject_type_value(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        attr_type: &str,
        value: &Literal,
    ) -> StorageResult<Option<Attribute>> {
        let candidates = self.find_for_slot(space_id, subject_id, attr_type).await?;
        Ok(candidates.into_iter().find(|item| item.value == *value))
    }

    /// Converts an Attribute to a SurrealDB value for insertion.
    pub(crate) fn attribute_to_value(item: &Attribute) -> StorageResult<surrealdb::sql::Value> {
        use surrealdb::sql::Value;

        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert(
            "subject".to_string(),
            crate::surreal::thing_from_record_id(item.subject.id()).into(),
        );
        obj.insert("attr_type".to_string(), item.attr_type.clone().into());
        let value_json = serde_json::to_value(&item.value).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize attribute value: {e}"))
        })?;
        obj.insert(
            "value".to_string(),
            crate::surreal::json_to_surreal(value_json)?,
        );
        obj.insert(
            "status".to_string(),
            crate::surreal::enum_to_surreal(&item.status)?,
        );
        obj.insert("confidence".to_string(), item.confidence.into());
        let validity = match &item.validity_window {
            Some(window) => {
                crate::surreal::json_to_surreal(serde_json::to_value(window).map_err(|e| {
                    StorageError::SerializationError(format!(
                        "Failed to serialize validity window: {e}"
                    ))
                })?)?
            }
            None => Value::None,
        };
        obj.insert("validity_window".to_string(), validity);
        let evidence_json = serde_json::to_value(&item.evidence).map_err(|e| {
            StorageError::SerializationError(format!("Failed to serialize evidence: {e}"))
        })?;
        obj.insert(
            "evidence".to_string(),
            crate::surreal::json_to_surreal(evidence_json)?,
        );
        let temporal = match &item.temporal_audit {
            Some(audit) => {
                crate::surreal::json_to_surreal(serde_json::to_value(audit).map_err(|e| {
                    StorageError::SerializationError(format!(
                        "Failed to serialize temporal audit: {e}"
                    ))
                })?)?
            }
            None => Value::None,
        };
        obj.insert("temporal_audit".to_string(), temporal);

        Ok(Value::Object(obj))
    }
}

#[async_trait]
impl Repository<Attribute> for AttributeRepository {
    async fn create(&self, record: Attribute) -> StorageResult<Attribute> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::attribute_to_value(&record)?;
        let response = self
            .client
            .query_bind(
                "CREATE $id CONTENT $record;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;
        response.check()?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Attribute>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Attribute>(space_id, id, false).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ATTRIBUTE_PROJECTION} FROM attribute WHERE id = $id AND space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<AttributeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse attribute: {e}")))?;

        Ok(items
            .into_iter()
            .next()
            .map(Attribute::try_from)
            .transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: Attribute,
    ) -> StorageResult<Attribute> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {ATTRIBUTE_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::attribute_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<AttributeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated attribute: {e}"))
        })?;

        let item_db = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("Attribute not found in space: {id}")))?;

        Attribute::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<Attribute>(space_id, id).await;
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
    ) -> StorageResult<Vec<Attribute>> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .list::<Attribute>(space_id, limit, cursor, false)
                .await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {ATTRIBUTE_PROJECTION} FROM attribute WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {ATTRIBUTE_PROJECTION} FROM attribute WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<AttributeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse attributes: {e}")))?;

        items.into_iter().map(Attribute::try_from).collect()
    }
}

impl AttributeRepository {
    /// Finds all active attributes for an entity.
    pub async fn find_by_subject_entity(
        &self,
        space_id: &str,
        subject_id: &RecordId,
    ) -> StorageResult<Vec<Attribute>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Attribute>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.subject.id() == subject_id
                })
                .collect();
            items.sort_by(|left, right| {
                left.attr_type
                    .cmp(&right.attr_type)
                    .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
            });
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ATTRIBUTE_PROJECTION} FROM attribute WHERE space_id = $space_id AND subject = $subject_id AND deleted_at IS NONE ORDER BY attr_type ASC, id ASC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "subject_id" => crate::surreal::thing_from_record_id(subject_id),
                },
            )
            .await?;

        let items: Vec<AttributeDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse attributes: {error}"))
        })?;
        items.into_iter().map(Attribute::try_from).collect()
    }

    /// Gets an attribute including soft-deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Attribute>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<Attribute>(space_id, id, true).await;
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ATTRIBUTE_PROJECTION} FROM attribute WHERE id = $id AND space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let items: Vec<AttributeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse attribute: {e}")))?;

        items
            .into_iter()
            .next()
            .map(Attribute::try_from)
            .transpose()
    }

    /// Finds attributes for a truth slot (subject, attr_type combination).
    ///
    /// Per spec §12.3: truth slots are keyed by subject and predicate (for attributes).
    /// This returns all active claims that could contribute to a slot's truth.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_for_slot(
        &self,
        space_id: &str,
        subject_id: &RecordId,
        attr_type: &str,
    ) -> StorageResult<Vec<Attribute>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<Attribute>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && item.subject.id() == subject_id
                        && item.attr_type == attr_type
                })
                .collect();
            items.sort_by_key(|item| item.base.created_at);
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {ATTRIBUTE_PROJECTION} FROM attribute WHERE space_id = $space_id AND subject = $subject_id AND attr_type = $attr_type AND deleted_at IS NONE ORDER BY created_at ASC"
                ),
                crate::bindings! {
                    "space_id" => space_id,
                    "subject_id" => crate::surreal::thing_from_record_id(subject_id),
                    "attr_type" => attr_type
                },
            )
            .await?;

        let items: Vec<AttributeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse attributes: {e}")))?;

        items.into_iter().map(Attribute::try_from).collect()
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

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80400);

    const ATTRIBUTE_SCHEMA: &str = r"
        DEFINE TABLE attribute SCHEMALESS;
    ";

    async fn setup_test_repo() -> AttributeRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("attribute_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(ATTRIBUTE_SCHEMA).await.unwrap();

        AttributeRepository::new(client)
    }

    fn create_test_attribute() -> Attribute {
        let id = RecordId::new_v4(TableName::Attribute);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let entity_id = RecordId::new_v4(TableName::Entity);

        Attribute {
            base,
            subject: Link::new(entity_id),
            attr_type: "age".to_string(),
            value: Literal::Integer(42),
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn native_optional_temporal_objects_survive_create_replace_and_removal() {
        use elephant_types::records::temporal::{RecencyBasis, TimeRefinement};
        let repo = setup_test_repo().await;
        // These are the actual optional-object contracts from 001_init.surql;
        // a schemaless-only fixture would incorrectly accept JSON NULL.
        repo.client
            .query_checked(
                "DEFINE FIELD validity_window ON TABLE attribute TYPE option<object> FLEXIBLE;
             DEFINE FIELD temporal_audit ON TABLE attribute TYPE option<object> FLEXIBLE;",
            )
            .await
            .unwrap();
        let mut original = create_test_attribute();
        original.base.security.level = 3;
        original.base.security.labels = vec!["private".into()];
        original.base.security.handling = vec!["restricted".into()];
        original.base.subjects_state = SubjectsState::Partial;
        let source = RecordId::new_v4(TableName::Source);
        original.base.provenance.sources.push(source);
        let id = original.base.id.clone();
        let created = repo.create(original.clone()).await.unwrap();
        assert_eq!(created.validity_window, None);
        assert_eq!(created.temporal_audit, None);
        let loaded = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(loaded.base.security, original.base.security);
        assert_eq!(loaded.base.provenance, original.base.provenance);
        assert_eq!(loaded.subject, original.subject);
        let at = chrono::DateTime::from_timestamp(1_800_000_000, 0).unwrap();
        let mut enriched = loaded;
        enriched.validity_window = Some(ValidityWindow::starting_at(at));
        enriched.temporal_audit = Some(TemporalAudit {
            time_basis: RecencyBasis::SourceEventTime,
            time_refinement: Some(TimeRefinement {
                original_basis: RecencyBasis::ObservedAt,
                original_value: at,
                refined_by: Link::new(RecordId::new_v4(TableName::Assertion)),
                refined_at: at,
            }),
        });
        let updated = repo
            .update("test_space", &id, enriched.clone())
            .await
            .unwrap();
        assert_eq!(updated.validity_window, enriched.validity_window);
        assert_eq!(updated.temporal_audit, enriched.temporal_audit);
        let loaded = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(loaded.validity_window, enriched.validity_window);
        assert_eq!(loaded.temporal_audit, enriched.temporal_audit);
        let mut cleared = loaded;
        cleared.validity_window = None;
        cleared.temporal_audit = None;
        repo.update("test_space", &id, cleared).await.unwrap();
        let loaded = repo.get("test_space", &id).await.unwrap().unwrap();
        assert_eq!(loaded.validity_window, None);
        assert_eq!(loaded.temporal_audit, None);
        assert_eq!(loaded.base.security, original.base.security);
        assert_eq!(loaded.base.provenance, original.base.provenance);
        assert_eq!(loaded.base.subjects_state, original.base.subjects_state);
        assert_eq!(loaded.subject, original.subject);
        assert_eq!(loaded.value, original.value);
        let mut response = repo
            .client
            .query_bind_checked(
                "SELECT VALUE validity_window IS NONE AND temporal_audit IS NONE FROM ONLY $id;",
                crate::bindings! {"id" => crate::surreal::thing_from_record_id(&id)},
            )
            .await
            .unwrap();
        let absent: Option<bool> = response.take(0).unwrap();
        assert_eq!(absent, Some(true));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_attribute() {
        let repo = setup_test_repo().await;
        let item = create_test_attribute();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.attr_type, "age");
        assert_eq!(created.value, Literal::Integer(42));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_attribute() {
        let repo = setup_test_repo().await;
        let item = create_test_attribute();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().attr_type, "age");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_attribute_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_attribute();
        item.value = Literal::String("test value".to_string());
        item.confidence = 0.75;

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.value, Literal::String("test value".to_string()));
        assert!((created.confidence - 0.75).abs() < f32::EPSILON);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_attribute() {
        let repo = setup_test_repo().await;
        let item = create_test_attribute();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.status = ClaimStatus::Approved;

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.status, ClaimStatus::Approved);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_attribute() {
        let repo = setup_test_repo().await;
        let item = create_test_attribute();
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
    async fn test_get_attribute_with_deleted_returns_soft_deleted() {
        let repo = setup_test_repo().await;
        let item = create_test_attribute();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();
        repo.delete("test_space", &id).await.unwrap();

        let retrieved = repo.get_with_deleted("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert!(retrieved.unwrap().base.deleted_at.is_some());
    }
}
