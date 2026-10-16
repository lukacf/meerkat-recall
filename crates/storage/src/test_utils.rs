//! Test utilities for `elephant-storage`.
//!
//! These are intended for fast unit/integration tests that should not require a
//! live database or running migrations.

#![allow(missing_docs)]

use async_trait::async_trait;
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::error::{StorageError, StorageResult};
use crate::repository::{PaginatedResult, Repository, SearchCriteria, SearchRepository};
use crate::{EntityWithRelations, MockStorageBackend};
use elephant_types::records::{
    Assertion, AuditLog, Doc, Entity, Event, IdentityCandidate, IdentityCandidateStatus,
    PredicateAlias, PredicateRegistry, Rel, Source, SystemEvent, WorkItem,
};
use elephant_types::RecordId;

fn serialize<T: Serialize>(context: &str, value: &T) -> StorageResult<serde_json::Value> {
    serde_json::to_value(value)
        .map_err(|e| StorageError::SerializationError(format!("Serialize {context}: {e}")))
}

fn deserialize<T: DeserializeOwned>(context: &str, value: serde_json::Value) -> StorageResult<T> {
    serde_json::from_value(value)
        .map_err(|e| StorageError::SerializationError(format!("Deserialize {context}: {e}")))
}

fn serialize_entity(entity: &Entity) -> StorageResult<serde_json::Value> {
    serialize("Entity", entity)
}

fn deserialize_entity(value: serde_json::Value) -> StorageResult<Entity> {
    deserialize("Entity", value)
}

/// In-memory `Entity` repository for tests.
#[derive(Clone, Debug)]
pub struct MockEntityRepository {
    backend: MockStorageBackend,
}

impl MockEntityRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn load(&self, id: &RecordId) -> StorageResult<Option<Entity>> {
        self.backend
            .get_json(id)
            .await
            .map(deserialize_entity)
            .transpose()
    }

    async fn store(&self, entity: &Entity) -> StorageResult<()> {
        let id = entity.base.id.clone();
        let value = serialize_entity(entity)?;
        self.backend.update_json(&id, value).await
    }

    /// Gets an entity including soft-deleted ones.
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Entity>> {
        let entity = self.load(id).await?;
        Ok(entity.filter(|e| e.base.space_id == space_id))
    }

    /// Restores a soft-deleted entity.
    ///
    /// # Errors
    ///
    /// Returns `NotFound` if the entity does not exist.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Entity> {
        let mut entity = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Entity not found: {id}")))?;

        entity.base.deleted_at = None;
        entity.base.updated_at = Utc::now();

        self.store(&entity).await?;
        Ok(entity)
    }

    /// Upsert based on ID.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage fails.
    pub async fn upsert(&self, record: Entity) -> StorageResult<Entity> {
        let space_id = record.base.space_id.clone();
        let id = record.base.id.clone();

        match self.get_with_deleted(&space_id, &id).await? {
            Some(existing) => {
                if existing.base.deleted_at.is_some() {
                    let _ = self.restore(&space_id, &id).await?;
                }
                self.update(&space_id, &id, record).await
            }
            None => self.create(record).await,
        }
    }

    /// Gets entity with all relationships.
    ///
    /// For test speed, this mock returns empty relationship vectors.
    ///
    /// # Errors
    ///
    /// Returns an error if entity retrieval fails.
    pub async fn get_with_relations(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<EntityWithRelations>> {
        let Some(entity) = self.get(space_id, id).await? else {
            return Ok(None);
        };

        Ok(Some(EntityWithRelations {
            entity,
            outgoing_relations: Vec::<Rel>::new(),
            incoming_relations: Vec::<Rel>::new(),
        }))
    }
}

#[async_trait]
impl Repository<Entity> for MockEntityRepository {
    async fn create(&self, record: Entity) -> StorageResult<Entity> {
        let id = record.base.id.clone();
        let value = serialize_entity(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Entity>> {
        let entity = self.get_with_deleted(space_id, id).await?;
        Ok(entity.filter(|e| e.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Entity,
    ) -> StorageResult<Entity> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("Entity not found: {id}")));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("Entity is deleted: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_entity(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut entity = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Entity not found: {id}")))?;

        entity.base.deleted_at = Some(Utc::now());
        entity.base.updated_at = Utc::now();

        self.store(&entity).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Entity>> {
        let mut entities = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_entity)
            .collect::<StorageResult<Vec<_>>>()?;

        entities.retain(|e| e.base.space_id == space_id && e.base.deleted_at.is_none());
        entities.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            entities.retain(|e| e.base.id.to_string() > cursor.to_string());
        }

        if entities.len() > limit {
            entities.truncate(limit);
        }

        Ok(entities)
    }
}

#[async_trait]
impl SearchRepository<Entity> for MockEntityRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Entity>> {
        let mut entities = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_entity)
            .collect::<StorageResult<Vec<_>>>()?;

        if let Some(space_id) = &criteria.space_id {
            entities.retain(|e| &e.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            entities.retain(|e| e.base.deleted_at.is_none());
        }
        if let Some(t) = &criteria.entity_type {
            entities.retain(|e| &e.entity_type == t);
        }
        if let Some(q) = &criteria.query {
            let q = q.to_lowercase();
            entities.retain(|e| e.display_name.to_lowercase().contains(&q));
        }

        entities.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = &criteria.cursor {
            let cursor_full = if cursor.contains(':') {
                cursor.clone()
            } else {
                format!("entity:{cursor}")
            };
            entities.retain(|e| e.base.id.to_string() > cursor_full);
        }

        let total = entities.len();

        let mut page = entities;
        let has_more = page.len() > criteria.limit;
        if has_more {
            page.truncate(criteria.limit + 1);
        }

        let next_cursor = if page.len() > criteria.limit {
            page.pop();
            page.last().map(|e| {
                let id_str = e.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        Ok(PaginatedResult::new(page, next_cursor, has_more, total))
    }
}

fn serialize_rel(rel: &Rel) -> StorageResult<serde_json::Value> {
    serialize("Rel", rel)
}

fn deserialize_rel(value: serde_json::Value) -> StorageResult<Rel> {
    deserialize("Rel", value)
}

/// In-memory `Rel` repository for tests.
#[derive(Clone, Debug)]
pub struct MockRelRepository {
    backend: MockStorageBackend,
}

impl MockRelRepository {
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn load(&self, id: &RecordId) -> StorageResult<Option<Rel>> {
        self.backend
            .get_json(id)
            .await
            .map(deserialize_rel)
            .transpose()
    }

    async fn store(&self, rel: &Rel) -> StorageResult<()> {
        let id = rel.base.id.clone();
        let value = serialize_rel(rel)?;
        self.backend.update_json(&id, value).await
    }

    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Rel>> {
        let rel = self.load(id).await?;
        Ok(rel.filter(|r| r.base.space_id == space_id))
    }

    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Rel> {
        let mut rel = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Rel not found: {id}")))?;

        rel.base.deleted_at = None;
        rel.base.updated_at = Utc::now();

        self.store(&rel).await?;
        Ok(rel)
    }

    pub async fn upsert(&self, record: Rel) -> StorageResult<Rel> {
        let space_id = record.base.space_id.clone();
        let id = record.base.id.clone();

        match self.get_with_deleted(&space_id, &id).await? {
            Some(existing) => {
                if existing.base.deleted_at.is_some() {
                    let _ = self.restore(&space_id, &id).await?;
                }
                self.update(&space_id, &id, record).await
            }
            None => self.create(record).await,
        }
    }
}

#[async_trait]
impl Repository<Rel> for MockRelRepository {
    async fn create(&self, record: Rel) -> StorageResult<Rel> {
        let id = record.base.id.clone();
        let value = serialize_rel(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Rel>> {
        let rel = self.get_with_deleted(space_id, id).await?;
        Ok(rel.filter(|r| r.base.deleted_at.is_none()))
    }

    async fn update(&self, space_id: &str, id: &RecordId, mut record: Rel) -> StorageResult<Rel> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("Rel not found: {id}")));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("Rel is deleted: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_rel(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut rel = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Rel not found: {id}")))?;

        rel.base.deleted_at = Some(Utc::now());
        rel.base.updated_at = Utc::now();

        self.store(&rel).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Rel>> {
        let mut rels = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_rel)
            .collect::<StorageResult<Vec<_>>>()?;

        rels.retain(|r| r.base.space_id == space_id && r.base.deleted_at.is_none());
        rels.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            rels.retain(|r| r.base.id.to_string() > cursor.to_string());
        }

        if rels.len() > limit {
            rels.truncate(limit);
        }

        Ok(rels)
    }
}

#[async_trait]
impl SearchRepository<Rel> for MockRelRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Rel>> {
        let mut rels = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_rel)
            .collect::<StorageResult<Vec<_>>>()?;

        if let Some(space_id) = &criteria.space_id {
            rels.retain(|r| &r.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            rels.retain(|r| r.base.deleted_at.is_none());
        }
        if let Some(q) = &criteria.query {
            let q = q.to_lowercase();
            rels.retain(|r| r.rel_type.to_lowercase().contains(&q));
        }

        rels.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = &criteria.cursor {
            let cursor_full = if cursor.contains(':') {
                cursor.clone()
            } else {
                format!("rel:{cursor}")
            };
            rels.retain(|r| r.base.id.to_string() > cursor_full);
        }

        let total = rels.len();

        let mut page = rels;
        let has_more = page.len() > criteria.limit;
        if has_more {
            page.truncate(criteria.limit + 1);
        }

        let next_cursor = if page.len() > criteria.limit {
            page.pop();
            page.last().map(|r| {
                let id_str = r.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        Ok(PaginatedResult::new(page, next_cursor, has_more, total))
    }
}

fn serialize_doc(doc: &Doc) -> StorageResult<serde_json::Value> {
    serialize("Doc", doc)
}

fn deserialize_doc(value: serde_json::Value) -> StorageResult<Doc> {
    deserialize("Doc", value)
}

/// In-memory `Doc` repository for tests.
#[derive(Clone, Debug)]
pub struct MockDocRepository {
    backend: MockStorageBackend,
}

impl MockDocRepository {
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn load(&self, id: &RecordId) -> StorageResult<Option<Doc>> {
        self.backend
            .get_json(id)
            .await
            .map(deserialize_doc)
            .transpose()
    }

    async fn store(&self, doc: &Doc) -> StorageResult<()> {
        let id = doc.base.id.clone();
        let value = serialize_doc(doc)?;
        self.backend.update_json(&id, value).await
    }

    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Doc>> {
        let doc = self.load(id).await?;
        Ok(doc.filter(|d| d.base.space_id == space_id))
    }

    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Doc> {
        let mut doc = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Doc not found: {id}")))?;

        doc.base.deleted_at = None;
        doc.base.updated_at = Utc::now();

        self.store(&doc).await?;
        Ok(doc)
    }
}

#[async_trait]
impl Repository<Doc> for MockDocRepository {
    async fn create(&self, record: Doc) -> StorageResult<Doc> {
        let id = record.base.id.clone();
        let value = serialize_doc(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Doc>> {
        let doc = self.get_with_deleted(space_id, id).await?;
        Ok(doc.filter(|d| d.base.deleted_at.is_none()))
    }

    async fn update(&self, space_id: &str, id: &RecordId, mut record: Doc) -> StorageResult<Doc> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("Doc not found: {id}")));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("Doc is deleted: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_doc(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut doc = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Doc not found: {id}")))?;

        doc.base.deleted_at = Some(Utc::now());
        doc.base.updated_at = Utc::now();

        self.store(&doc).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Doc>> {
        let mut docs = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_doc)
            .collect::<StorageResult<Vec<_>>>()?;

        docs.retain(|d| d.base.space_id == space_id && d.base.deleted_at.is_none());
        docs.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            docs.retain(|d| d.base.id.to_string() > cursor.to_string());
        }

        if docs.len() > limit {
            docs.truncate(limit);
        }

        Ok(docs)
    }
}

#[async_trait]
impl SearchRepository<Doc> for MockDocRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Doc>> {
        let mut docs = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_doc)
            .collect::<StorageResult<Vec<_>>>()?;

        if let Some(space_id) = &criteria.space_id {
            docs.retain(|d| &d.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            docs.retain(|d| d.base.deleted_at.is_none());
        }
        if let Some(q) = &criteria.query {
            let q = q.to_lowercase();
            docs.retain(|d| d.title.to_lowercase().contains(&q));
        }

        docs.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = &criteria.cursor {
            let cursor_full = if cursor.contains(':') {
                cursor.clone()
            } else {
                format!("doc:{cursor}")
            };
            docs.retain(|d| d.base.id.to_string() > cursor_full);
        }

        let total = docs.len();

        let mut page = docs;
        let has_more = page.len() > criteria.limit;
        if has_more {
            page.truncate(criteria.limit + 1);
        }

        let next_cursor = if page.len() > criteria.limit {
            page.pop();
            page.last().map(|d| {
                let id_str = d.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        Ok(PaginatedResult::new(page, next_cursor, has_more, total))
    }
}

fn serialize_work_item(item: &WorkItem) -> StorageResult<serde_json::Value> {
    serialize("WorkItem", item)
}

fn deserialize_work_item(value: serde_json::Value) -> StorageResult<WorkItem> {
    deserialize("WorkItem", value)
}

/// In-memory `WorkItem` repository for tests.
#[derive(Clone, Debug)]
pub struct MockWorkItemRepository {
    backend: MockStorageBackend,
}

impl MockWorkItemRepository {
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }
}

#[async_trait]
impl Repository<WorkItem> for MockWorkItemRepository {
    async fn create(&self, record: WorkItem) -> StorageResult<WorkItem> {
        let id = record.base.id.clone();
        let value = serialize_work_item(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<WorkItem>> {
        let item = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_work_item)
            .transpose()?;
        Ok(item.filter(|i| i.base.space_id == space_id && i.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: WorkItem,
    ) -> StorageResult<WorkItem> {
        let Some(existing) = self.get(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("WorkItem not found: {id}")));
        };
        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("WorkItem is deleted: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_work_item(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut item = self
            .get(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("WorkItem not found: {id}")))?;
        item.base.deleted_at = Some(Utc::now());
        item.base.updated_at = Utc::now();
        let value = serialize_work_item(&item)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<WorkItem>> {
        let mut items = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_work_item)
            .collect::<StorageResult<Vec<_>>>()?;

        items.retain(|i| i.base.space_id == space_id && i.base.deleted_at.is_none());
        items.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            items.retain(|i| i.base.id.to_string() > cursor.to_string());
        }

        if items.len() > limit {
            items.truncate(limit);
        }

        Ok(items)
    }
}

#[async_trait]
impl SearchRepository<WorkItem> for MockWorkItemRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<WorkItem>> {
        let mut items = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_work_item)
            .collect::<StorageResult<Vec<_>>>()?;

        if let Some(space_id) = &criteria.space_id {
            items.retain(|i| &i.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            items.retain(|i| i.base.deleted_at.is_none());
        }

        items.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        let total = items.len();
        if total > criteria.limit {
            items.truncate(criteria.limit);
        }

        Ok(PaginatedResult::new(items, None, false, total))
    }
}

fn serialize_source(source: &Source) -> StorageResult<serde_json::Value> {
    serialize("Source", source)
}

fn deserialize_source(value: serde_json::Value) -> StorageResult<Source> {
    deserialize("Source", value)
}

/// In-memory `Source` repository for tests.
#[derive(Clone, Debug)]
pub struct MockSourceRepository {
    backend: MockStorageBackend,
}

impl MockSourceRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn load(&self, id: &RecordId) -> StorageResult<Option<Source>> {
        self.backend
            .get_json(id)
            .await
            .map(deserialize_source)
            .transpose()
    }

    async fn store(&self, source: &Source) -> StorageResult<()> {
        let id = source.base.id.clone();
        let value = serialize_source(source)?;
        self.backend.update_json(&id, value).await
    }

    /// Gets a source including soft-deleted ones.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Source>> {
        let source = self.load(id).await?;
        Ok(source.filter(|s| s.base.space_id == space_id))
    }

    /// Restores a soft-deleted source.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Source> {
        let mut source = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Source not found: {id}")))?;

        source.base.deleted_at = None;
        source.base.updated_at = Utc::now();

        self.store(&source).await?;
        Ok(source)
    }

    /// Restores and replaces an existing source in one mock operation.
    pub async fn restore_and_replace_existing(
        &self,
        space_id: &str,
        id: &RecordId,
        mut source: Source,
    ) -> StorageResult<Source> {
        let existing = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Source not found: {id}")))?;

        source.base.id = existing.base.id.clone();
        source.base.space_id.clone_from(&existing.base.space_id);
        source.base.created_at = existing.base.created_at;
        source.base.created_by.clone_from(&existing.base.created_by);
        source.base.deleted_at = None;
        source.base.updated_at = Utc::now();

        self.store(&source).await?;
        Ok(source)
    }

    /// Permanently deletes a source.
    pub async fn hard_delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let source = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Source not found: {id}")))?;

        if source.base.space_id != space_id {
            return Err(StorageError::NotFound(format!("Source not found: {id}")));
        }

        self.backend.delete(id).await
    }
}

#[async_trait]
impl Repository<Source> for MockSourceRepository {
    async fn create(&self, record: Source) -> StorageResult<Source> {
        let id = record.base.id.clone();
        let value = serialize_source(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Source>> {
        let source = self.get_with_deleted(space_id, id).await?;
        Ok(source.filter(|s| s.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Source,
    ) -> StorageResult<Source> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("Source not found: {id}")));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("Source is deleted: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_source(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut source = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Source not found: {id}")))?;

        source.base.deleted_at = Some(Utc::now());
        source.base.updated_at = Utc::now();

        self.store(&source).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Source>> {
        let mut sources = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_source)
            .collect::<StorageResult<Vec<_>>>()?;

        sources.retain(|s| s.base.space_id == space_id && s.base.deleted_at.is_none());
        sources.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            sources.retain(|s| s.base.id.to_string() > cursor.to_string());
        }

        if sources.len() > limit {
            sources.truncate(limit);
        }

        Ok(sources)
    }
}

#[async_trait]
impl SearchRepository<Source> for MockSourceRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Source>> {
        let mut sources = self
            .backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_source)
            .collect::<StorageResult<Vec<_>>>()?;

        if let Some(space_id) = &criteria.space_id {
            sources.retain(|s| &s.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            sources.retain(|s| s.base.deleted_at.is_none());
        }
        if let Some(q) = &criteria.query {
            let q = q.to_lowercase();
            sources.retain(|s| {
                s.uri
                    .as_deref()
                    .is_some_and(|uri| uri.to_lowercase().contains(&q))
                    || s.path
                        .as_deref()
                        .is_some_and(|path| path.to_lowercase().contains(&q))
                    || s.external_id
                        .as_deref()
                        .is_some_and(|external_id| external_id.to_lowercase().contains(&q))
            });
        }

        sources.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        let total = sources.len();
        if total > criteria.limit {
            sources.truncate(criteria.limit);
        }

        Ok(PaginatedResult::new(sources, None, false, total))
    }
}

fn serialize_system_event(event: &SystemEvent) -> StorageResult<serde_json::Value> {
    serialize("SystemEvent", event)
}

fn deserialize_system_event(value: serde_json::Value) -> StorageResult<SystemEvent> {
    deserialize("SystemEvent", value)
}

fn serialize_event(event: &Event) -> StorageResult<serde_json::Value> {
    serialize("Event", event)
}

fn deserialize_event(value: serde_json::Value) -> StorageResult<Event> {
    deserialize("Event", value)
}

/// In-memory `SystemEvent` repository for tests.
#[derive(Clone, Debug)]
pub struct MockSystemEventRepository {
    backend: MockStorageBackend,
}

impl MockSystemEventRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_events(&self) -> StorageResult<Vec<SystemEvent>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_system_event)
            .collect::<StorageResult<Vec<_>>>()
    }

    /// Returns the latest (max) seq for a given space.
    pub async fn get_latest_seq(&self, space_id: &str) -> StorageResult<i64> {
        let max_seq = self
            .list_events()
            .await?
            .into_iter()
            .filter(|e| e.base.space_id == space_id && e.base.deleted_at.is_none())
            .map(|e| e.seq)
            .max()
            .unwrap_or(0);
        Ok(max_seq)
    }

    /// Returns events with seq strictly greater than `after_seq`, ordered by seq asc.
    pub async fn get_after_seq(
        &self,
        space_id: &str,
        after_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|e| e.base.space_id == space_id && e.base.deleted_at.is_none())
            .filter(|e| e.seq > after_seq)
            .collect::<Vec<_>>();
        events.sort_by(|a, b| a.seq.cmp(&b.seq));
        events.truncate(limit);
        Ok(events)
    }

    /// Returns events with seq strictly less than `before_seq`, ordered by seq desc.
    pub async fn get_before_seq(
        &self,
        space_id: &str,
        before_seq: i64,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|e| e.base.space_id == space_id && e.base.deleted_at.is_none())
            .filter(|e| e.seq < before_seq)
            .collect::<Vec<_>>();
        events.sort_by(|a, b| b.seq.cmp(&a.seq));
        events.truncate(limit);
        Ok(events)
    }

    /// Returns the most recent events (by seq desc).
    pub async fn get_recent(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<SystemEvent>> {
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|e| e.base.space_id == space_id && e.base.deleted_at.is_none())
            .collect::<Vec<_>>();
        events.sort_by(|a, b| b.seq.cmp(&a.seq));
        events.truncate(limit);
        Ok(events)
    }

    /// Returns upcoming events for a space.
    ///
    /// Mirrors the production `SystemEventRepository` behavior: system events are not
    /// scheduled in the future, so "upcoming" currently returns recent events.
    pub async fn get_upcoming(&self, space_id: &str, days: u32) -> StorageResult<Vec<SystemEvent>> {
        let _ = days;
        self.get_recent(space_id, 50).await
    }

    /// Gets an event including soft-deleted ones.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<SystemEvent>> {
        let event = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_system_event)
            .transpose()?;
        Ok(event.filter(|e| e.base.space_id == space_id))
    }

    /// Restores a soft-deleted event.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<SystemEvent> {
        let mut event = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("SystemEvent not found: {id}")))?;

        event.base.deleted_at = None;
        event.base.updated_at = Utc::now();

        let value = serialize_system_event(&event)?;
        self.backend.update_json(id, value).await?;
        Ok(event)
    }
}

#[async_trait]
impl Repository<SystemEvent> for MockSystemEventRepository {
    async fn create(&self, record: SystemEvent) -> StorageResult<SystemEvent> {
        let id = record.base.id.clone();
        let value = serialize_system_event(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<SystemEvent>> {
        let event = self.get_with_deleted(space_id, id).await?;
        Ok(event.filter(|e| e.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: SystemEvent,
    ) -> StorageResult<SystemEvent> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!(
                "SystemEvent not found: {id}"
            )));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!(
                "SystemEvent is deleted: {id}"
            )));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_system_event(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut event = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("SystemEvent not found: {id}")))?;
        event.base.deleted_at = Some(Utc::now());
        event.base.updated_at = Utc::now();
        let value = serialize_system_event(&event)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<SystemEvent>> {
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|e| e.base.space_id == space_id && e.base.deleted_at.is_none())
            .collect::<Vec<_>>();
        events.sort_by(|a, b| a.seq.cmp(&b.seq));

        if let Some(cursor) = cursor {
            events.retain(|e| e.base.id.to_string() > cursor.to_string());
        }

        if events.len() > limit {
            events.truncate(limit);
        }

        Ok(events)
    }
}

#[async_trait]
impl SearchRepository<SystemEvent> for MockSystemEventRepository {
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<SystemEvent>> {
        let mut events = self.list_events().await?.into_iter().collect::<Vec<_>>();

        if let Some(space_id) = &criteria.space_id {
            events.retain(|e| &e.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            events.retain(|e| e.base.deleted_at.is_none());
        }
        if let Some(q) = &criteria.query {
            let q = q.to_lowercase();
            events.retain(|e| {
                e.event_type.to_lowercase().contains(&q)
                    || e.payload.to_string().to_lowercase().contains(&q)
            });
        }
        if let Some(entities) = &criteria.entities {
            let want: Vec<String> = entities.clone();
            let mode_all = criteria.mode.as_deref() == Some("all");
            events.retain(|e| {
                let have: std::collections::HashSet<String> =
                    e.base.subjects.iter().map(|l| l.id().to_string()).collect();
                if mode_all {
                    want.iter().all(|id| have.contains(id))
                } else {
                    want.iter().any(|id| have.contains(id))
                }
            });
        }

        events.sort_by(|a, b| a.seq.cmp(&b.seq));

        let total = events.len();
        if total > criteria.limit {
            events.truncate(criteria.limit);
        }

        Ok(PaginatedResult::new(events, None, false, total))
    }
}

/// In-memory `Event` repository for tests.
#[derive(Clone, Debug)]
pub struct MockEventRepository {
    backend: MockStorageBackend,
}

impl MockEventRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_events(&self) -> StorageResult<Vec<Event>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_event)
            .collect::<StorageResult<Vec<_>>>()
    }

    /// Returns recent events ordered by `time_start` descending.
    pub async fn get_recent(&self, space_id: &str, limit: usize) -> StorageResult<Vec<Event>> {
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|event| event.base.space_id == space_id && event.base.deleted_at.is_none())
            .collect::<Vec<_>>();
        events.sort_by(|a, b| {
            b.time_start
                .cmp(&a.time_start)
                .then_with(|| b.base.created_at.cmp(&a.base.created_at))
        });
        events.truncate(limit);
        Ok(events)
    }

    /// Returns upcoming events in the next `days`.
    pub async fn get_upcoming(&self, space_id: &str, days: u32) -> StorageResult<Vec<Event>> {
        let now = Utc::now();
        let horizon = now + chrono::Duration::days(i64::from(days));
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|event| event.base.space_id == space_id && event.base.deleted_at.is_none())
            .filter(|event| {
                event
                    .time_start
                    .is_some_and(|start| start >= now && start <= horizon)
            })
            .collect::<Vec<_>>();
        events.sort_by(|a, b| {
            a.time_start
                .cmp(&b.time_start)
                .then_with(|| a.base.created_at.cmp(&b.base.created_at))
        });
        Ok(events)
    }

    /// Gets an event including soft-deleted ones.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Event>> {
        let event = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_event)
            .transpose()?;
        Ok(event.filter(|item| item.base.space_id == space_id))
    }

    /// Restores a soft-deleted event.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Event> {
        let mut event = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Event not found: {id}")))?;

        event.base.deleted_at = None;
        event.base.updated_at = Utc::now();

        let value = serialize_event(&event)?;
        self.backend.update_json(id, value).await?;
        Ok(event)
    }
}

#[async_trait]
impl Repository<Event> for MockEventRepository {
    async fn create(&self, record: Event) -> StorageResult<Event> {
        let id = record.base.id.clone();
        let value = serialize_event(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Event>> {
        let event = self.get_with_deleted(space_id, id).await?;
        Ok(event.filter(|item| item.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Event,
    ) -> StorageResult<Event> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("Event not found: {id}")));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("Event is deleted: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_event(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut event = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Event not found: {id}")))?;

        event.base.deleted_at = Some(Utc::now());
        event.base.updated_at = Utc::now();
        let value = serialize_event(&event)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Event>> {
        let mut events = self
            .list_events()
            .await?
            .into_iter()
            .filter(|event| event.base.space_id == space_id && event.base.deleted_at.is_none())
            .collect::<Vec<_>>();
        events.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            events.retain(|event| event.base.id.to_string() > cursor.to_string());
        }

        events.truncate(limit);
        Ok(events)
    }
}

#[async_trait]
impl SearchRepository<Event> for MockEventRepository {
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<Event>> {
        let mut events = self.list_events().await?;

        if let Some(space_id) = &criteria.space_id {
            events.retain(|event| &event.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            events.retain(|event| event.base.deleted_at.is_none());
        }
        if let Some(query) = &criteria.query {
            let query = query.to_lowercase();
            events.retain(|event| {
                event.title.to_lowercase().contains(&query)
                    || event
                        .summary
                        .as_deref()
                        .is_some_and(|summary| summary.to_lowercase().contains(&query))
                    || event.metadata.to_string().to_lowercase().contains(&query)
            });
        }
        if let Some(entities) = &criteria.entities {
            let want = entities.clone();
            let mode_all = criteria.mode.as_deref() == Some("all");
            events.retain(|event| {
                let have: std::collections::HashSet<String> = event
                    .base
                    .subjects
                    .iter()
                    .map(|link| link.id().to_string())
                    .collect();
                if mode_all {
                    want.iter().all(|id| have.contains(id))
                } else {
                    want.iter().any(|id| have.contains(id))
                }
            });
        }
        if let Some(cursor) = &criteria.cursor {
            let cursor_full = if cursor.contains(':') {
                cursor.clone()
            } else {
                format!("event:{cursor}")
            };
            events.retain(|event| event.base.id.to_string() > cursor_full);
        }

        events.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        let total = events.len();
        let has_more = events.len() > criteria.limit;
        if has_more {
            events.truncate(criteria.limit);
        }

        let cursor = if has_more {
            events.last().map(|event| {
                let id_str = event.base.id.to_string();
                id_str.split(':').nth(1).unwrap_or(&id_str).to_string()
            })
        } else {
            None
        };

        Ok(PaginatedResult::new(events, cursor, has_more, total))
    }
}

fn serialize_assertion(assertion: &Assertion) -> StorageResult<serde_json::Value> {
    serialize("Assertion", assertion)
}

fn deserialize_assertion(value: serde_json::Value) -> StorageResult<Assertion> {
    deserialize("Assertion", value)
}

/// In-memory `Assertion` repository for tests.
#[derive(Clone, Debug)]
pub struct MockAssertionRepository {
    backend: MockStorageBackend,
}

impl MockAssertionRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_assertions(&self) -> StorageResult<Vec<Assertion>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_assertion)
            .collect::<StorageResult<Vec<_>>>()
    }

    /// Gets an assertion including soft-deleted ones.
    pub async fn get_with_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<Assertion>> {
        let assertion = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_assertion)
            .transpose()?;
        Ok(assertion.filter(|a| a.base.space_id == space_id))
    }

    /// Restores a soft-deleted assertion.
    pub async fn restore(&self, space_id: &str, id: &RecordId) -> StorageResult<Assertion> {
        let mut assertion = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Assertion not found: {id}")))?;

        assertion.base.deleted_at = None;
        assertion.base.updated_at = Utc::now();

        let value = serialize_assertion(&assertion)?;
        self.backend.update_json(id, value).await?;
        Ok(assertion)
    }
}

#[async_trait]
impl Repository<Assertion> for MockAssertionRepository {
    async fn create(&self, record: Assertion) -> StorageResult<Assertion> {
        let id = record.base.id.clone();
        let value = serialize_assertion(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<Assertion>> {
        let assertion = self.get_with_deleted(space_id, id).await?;
        Ok(assertion.filter(|a| a.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: Assertion,
    ) -> StorageResult<Assertion> {
        let Some(existing) = self.get_with_deleted(space_id, id).await? else {
            return Err(StorageError::NotFound(format!("Assertion not found: {id}")));
        };

        if existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!(
                "Assertion is deleted: {id}"
            )));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_assertion(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let mut assertion = self
            .get_with_deleted(space_id, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(format!("Assertion not found: {id}")))?;
        assertion.base.deleted_at = Some(Utc::now());
        assertion.base.updated_at = Utc::now();
        let value = serialize_assertion(&assertion)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<Assertion>> {
        let mut assertions = self
            .list_assertions()
            .await?
            .into_iter()
            .filter(|a| a.base.space_id == space_id && a.base.deleted_at.is_none())
            .collect::<Vec<_>>();

        assertions.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            let cursor_s = cursor.to_string();
            assertions.retain(|a| a.base.id.to_string() > cursor_s);
        }

        assertions.truncate(limit);
        Ok(assertions)
    }
}

fn serialize_predicate_registry(pred: &PredicateRegistry) -> StorageResult<serde_json::Value> {
    serialize("PredicateRegistry", pred)
}

fn deserialize_predicate_registry(value: serde_json::Value) -> StorageResult<PredicateRegistry> {
    deserialize("PredicateRegistry", value)
}

/// In-memory `PredicateRegistry` repository for tests.
#[derive(Clone, Debug)]
pub struct MockPredicateRegistryRepository {
    backend: MockStorageBackend,
}

impl MockPredicateRegistryRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_predicates(&self) -> StorageResult<Vec<PredicateRegistry>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_predicate_registry)
            .collect::<StorageResult<Vec<_>>>()
    }
}

#[async_trait]
impl Repository<PredicateRegistry> for MockPredicateRegistryRepository {
    async fn create(&self, record: PredicateRegistry) -> StorageResult<PredicateRegistry> {
        let id = record.base.id.clone();
        let value = serialize_predicate_registry(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateRegistry>> {
        let pred = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_predicate_registry)
            .transpose()?;
        Ok(pred
            .filter(|p| p.base.space_id == space_id)
            .filter(|p| p.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: PredicateRegistry,
    ) -> StorageResult<PredicateRegistry> {
        let Some(existing) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_predicate_registry)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!(
                "PredicateRegistry not found: {id}"
            )));
        };

        if existing.base.space_id != space_id || existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!(
                "PredicateRegistry not found: {id}"
            )));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_predicate_registry(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let Some(mut pred) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_predicate_registry)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!(
                "PredicateRegistry not found: {id}"
            )));
        };

        if pred.base.space_id != space_id {
            return Err(StorageError::NotFound(format!(
                "PredicateRegistry not found: {id}"
            )));
        }

        pred.base.deleted_at = Some(Utc::now());
        pred.base.updated_at = Utc::now();
        let value = serialize_predicate_registry(&pred)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateRegistry>> {
        let mut preds = self
            .list_predicates()
            .await?
            .into_iter()
            .filter(|p| p.base.space_id == space_id && p.base.deleted_at.is_none())
            .collect::<Vec<_>>();

        preds.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            let cursor_s = cursor.to_string();
            preds.retain(|p| p.base.id.to_string() > cursor_s);
        }

        preds.truncate(limit);
        Ok(preds)
    }
}

fn serialize_predicate_alias(alias: &PredicateAlias) -> StorageResult<serde_json::Value> {
    serialize("PredicateAlias", alias)
}

fn deserialize_predicate_alias(value: serde_json::Value) -> StorageResult<PredicateAlias> {
    deserialize("PredicateAlias", value)
}

/// In-memory `PredicateAlias` repository for tests.
#[derive(Clone, Debug)]
pub struct MockPredicateAliasRepository {
    backend: MockStorageBackend,
}

impl MockPredicateAliasRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_aliases(&self) -> StorageResult<Vec<PredicateAlias>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_predicate_alias)
            .collect::<StorageResult<Vec<_>>>()
    }
}

#[async_trait]
impl Repository<PredicateAlias> for MockPredicateAliasRepository {
    async fn create(&self, record: PredicateAlias) -> StorageResult<PredicateAlias> {
        let id = record.base.id.clone();
        let value = serialize_predicate_alias(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<PredicateAlias>> {
        let alias = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_predicate_alias)
            .transpose()?;
        Ok(alias
            .filter(|a| a.base.space_id == space_id)
            .filter(|a| a.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: PredicateAlias,
    ) -> StorageResult<PredicateAlias> {
        let Some(existing) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_predicate_alias)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!(
                "PredicateAlias not found: {id}"
            )));
        };

        if existing.base.space_id != space_id || existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!(
                "PredicateAlias not found: {id}"
            )));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_predicate_alias(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let Some(mut alias) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_predicate_alias)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!(
                "PredicateAlias not found: {id}"
            )));
        };

        if alias.base.space_id != space_id {
            return Err(StorageError::NotFound(format!(
                "PredicateAlias not found: {id}"
            )));
        }

        alias.base.deleted_at = Some(Utc::now());
        alias.base.updated_at = Utc::now();
        let value = serialize_predicate_alias(&alias)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<PredicateAlias>> {
        let mut aliases = self
            .list_aliases()
            .await?
            .into_iter()
            .filter(|a| a.base.space_id == space_id && a.base.deleted_at.is_none())
            .collect::<Vec<_>>();

        aliases.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            let cursor_s = cursor.to_string();
            aliases.retain(|a| a.base.id.to_string() > cursor_s);
        }

        aliases.truncate(limit);
        Ok(aliases)
    }
}

fn serialize_audit_log(log: &AuditLog) -> StorageResult<serde_json::Value> {
    serialize("AuditLog", log)
}

fn deserialize_audit_log(value: serde_json::Value) -> StorageResult<AuditLog> {
    deserialize("AuditLog", value)
}

/// In-memory `AuditLog` repository for tests.
#[derive(Clone, Debug)]
pub struct MockAuditLogRepository {
    backend: MockStorageBackend,
}

impl MockAuditLogRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_logs(&self) -> StorageResult<Vec<AuditLog>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_audit_log)
            .collect::<StorageResult<Vec<_>>>()
    }
}

#[async_trait]
impl Repository<AuditLog> for MockAuditLogRepository {
    async fn create(&self, record: AuditLog) -> StorageResult<AuditLog> {
        let id = record.base.id.clone();
        let value = serialize_audit_log(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<AuditLog>> {
        let log = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_audit_log)
            .transpose()?;
        Ok(log
            .filter(|l| l.base.space_id == space_id)
            .filter(|l| l.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: AuditLog,
    ) -> StorageResult<AuditLog> {
        let Some(existing) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_audit_log)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!("AuditLog not found: {id}")));
        };

        if existing.base.space_id != space_id || existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!("AuditLog not found: {id}")));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_audit_log(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let Some(mut log) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_audit_log)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!("AuditLog not found: {id}")));
        };

        if log.base.space_id != space_id {
            return Err(StorageError::NotFound(format!("AuditLog not found: {id}")));
        }

        log.base.deleted_at = Some(Utc::now());
        log.base.updated_at = Utc::now();
        let value = serialize_audit_log(&log)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<AuditLog>> {
        let mut logs = self
            .list_logs()
            .await?
            .into_iter()
            .filter(|l| l.base.space_id == space_id && l.base.deleted_at.is_none())
            .collect::<Vec<_>>();

        logs.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            let cursor_s = cursor.to_string();
            logs.retain(|l| l.base.id.to_string() > cursor_s);
        }

        logs.truncate(limit);
        Ok(logs)
    }
}

fn serialize_identity_candidate(candidate: &IdentityCandidate) -> StorageResult<serde_json::Value> {
    serialize("IdentityCandidate", candidate)
}

fn deserialize_identity_candidate(value: serde_json::Value) -> StorageResult<IdentityCandidate> {
    deserialize("IdentityCandidate", value)
}

/// In-memory `IdentityCandidate` repository for tests.
#[derive(Clone, Debug)]
pub struct MockIdentityCandidateRepository {
    backend: MockStorageBackend,
}

impl MockIdentityCandidateRepository {
    /// Create a new mock repo using the provided backend.
    #[must_use]
    pub const fn new(backend: MockStorageBackend) -> Self {
        Self { backend }
    }

    /// Convenience for creating an empty repository.
    #[must_use]
    pub fn empty() -> Self {
        Self::new(MockStorageBackend::new())
    }

    async fn list_candidates(&self) -> StorageResult<Vec<IdentityCandidate>> {
        self.backend
            .list_all_json()
            .await
            .into_iter()
            .map(deserialize_identity_candidate)
            .collect::<StorageResult<Vec<_>>>()
    }

    /// Returns pending candidates for a space.
    pub async fn get_pending(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<IdentityCandidate>> {
        let mut candidates = self
            .list_candidates()
            .await?
            .into_iter()
            .filter(|c| c.base.space_id == space_id && c.base.deleted_at.is_none())
            .filter(|c| c.status == IdentityCandidateStatus::Pending)
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.truncate(limit);
        Ok(candidates)
    }

    /// Returns high-confidence candidates for a space.
    pub async fn get_high_confidence(
        &self,
        space_id: &str,
        threshold: f64,
        limit: usize,
    ) -> StorageResult<Vec<IdentityCandidate>> {
        let mut candidates = self
            .list_candidates()
            .await?
            .into_iter()
            .filter(|c| c.base.space_id == space_id && c.base.deleted_at.is_none())
            .filter(|c| c.confidence >= threshold)
            .collect::<Vec<_>>();
        candidates.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        candidates.truncate(limit);
        Ok(candidates)
    }
}

#[async_trait]
impl Repository<IdentityCandidate> for MockIdentityCandidateRepository {
    async fn create(&self, record: IdentityCandidate) -> StorageResult<IdentityCandidate> {
        let id = record.base.id.clone();
        let value = serialize_identity_candidate(&record)?;
        self.backend.create_json(id, value).await?;
        Ok(record)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<IdentityCandidate>> {
        let candidate = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_identity_candidate)
            .transpose()?;
        Ok(candidate
            .filter(|c| c.base.space_id == space_id)
            .filter(|c| c.base.deleted_at.is_none()))
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        mut record: IdentityCandidate,
    ) -> StorageResult<IdentityCandidate> {
        let Some(existing) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_identity_candidate)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!(
                "IdentityCandidate not found: {id}"
            )));
        };

        if existing.base.space_id != space_id || existing.base.deleted_at.is_some() {
            return Err(StorageError::NotFound(format!(
                "IdentityCandidate not found: {id}"
            )));
        }

        record.base.updated_at = Utc::now();
        let value = serialize_identity_candidate(&record)?;
        self.backend.update_json(id, value).await?;
        Ok(record)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        let Some(mut candidate) = self
            .backend
            .get_json(id)
            .await
            .map(deserialize_identity_candidate)
            .transpose()?
        else {
            return Err(StorageError::NotFound(format!(
                "IdentityCandidate not found: {id}"
            )));
        };

        if candidate.base.space_id != space_id {
            return Err(StorageError::NotFound(format!(
                "IdentityCandidate not found: {id}"
            )));
        }

        candidate.base.deleted_at = Some(Utc::now());
        candidate.base.updated_at = Utc::now();
        let value = serialize_identity_candidate(&candidate)?;
        self.backend.update_json(id, value).await
    }

    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<IdentityCandidate>> {
        let mut candidates = self
            .list_candidates()
            .await?
            .into_iter()
            .filter(|c| c.base.space_id == space_id && c.base.deleted_at.is_none())
            .collect::<Vec<_>>();

        candidates.sort_by(|a, b| a.base.id.to_string().cmp(&b.base.id.to_string()));

        if let Some(cursor) = cursor {
            let cursor_s = cursor.to_string();
            candidates.retain(|c| c.base.id.to_string() > cursor_s);
        }

        candidates.truncate(limit);
        Ok(candidates)
    }
}

#[async_trait]
impl SearchRepository<IdentityCandidate> for MockIdentityCandidateRepository {
    async fn search(
        &self,
        criteria: &SearchCriteria,
    ) -> StorageResult<PaginatedResult<IdentityCandidate>> {
        let mut candidates = self.list_candidates().await?;

        if let Some(space_id) = &criteria.space_id {
            candidates.retain(|c| &c.base.space_id == space_id);
        }
        if !criteria.include_deleted {
            candidates.retain(|c| c.base.deleted_at.is_none());
        }

        candidates.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total = candidates.len();
        candidates.truncate(criteria.limit);
        Ok(PaginatedResult::new(candidates, None, false, total))
    }
}

/// Hold one entered native write on this storage client's real router datastore.
/// The explicit feature enables a fixed vendor control, never arbitrary tasks.
///
/// # Errors
/// Returns an error when no local SDK connection exists, admission is closed,
/// or the fixed write owner fails before entering its transaction.
#[cfg(feature = "test-utils")]
pub async fn enter_local_close_write(
    client: &crate::StorageClient,
) -> StorageResult<(
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<surrealdb::Result<()>>,
)> {
    client
        .db()
        .ok_or_else(|| {
            StorageError::Conflict("native close control requires local SurrealDB".into())
        })?
        .recall_test_enter_local_write()
        .await
        .map_err(Into::into)
}

/// Retain the native SDK handle independently of the storage ownership lock.
/// Available only in the explicit test utility feature.
///
/// # Errors
/// Returns an error for an embedded storage client without an SDK connection.
#[cfg(feature = "test-utils")]
pub fn local_close_sdk_handle(
    client: &crate::StorageClient,
) -> StorageResult<surrealdb::Surreal<surrealdb::engine::any::Any>> {
    client
        .db()
        .cloned()
        .ok_or_else(|| StorageError::Conflict("local close fixture requires SurrealDB".into()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::MockEntityRepository;
    use crate::repository::Repository;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::entity::EntityType;
    use elephant_types::records::Entity;
    use elephant_types::RecordId;

    fn make_entity(name: &str) -> Entity {
        let id = RecordId::new_v4(TableName::Entity);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;
        Entity::new(base, EntityType::new("person"), name.to_string())
    }

    #[tokio::test]
    async fn mock_entity_repository_does_not_require_db() {
        let repo = MockEntityRepository::empty();
        let entity = make_entity("Alice");
        let id = entity.base.id.clone();

        repo.create(entity).await.expect("create");
        let loaded = repo.get("test_space", &id).await.expect("get");
        assert!(loaded.is_some());
    }
}
