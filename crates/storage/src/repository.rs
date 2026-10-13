//! Repository traits for CRUD operations.

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};

use crate::error::StorageResult;
use elephant_types::RecordId;

/// Base repository trait for CRUD operations.
#[async_trait]
pub trait Repository<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    /// Creates a new record.
    async fn create(&self, record: T) -> StorageResult<T>;

    /// Retrieves a record by space ID and ID.
    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<T>>;

    /// Updates an existing record in a space.
    async fn update(&self, space_id: &str, id: &RecordId, record: T) -> StorageResult<T>;

    /// Deletes a record from a space (soft delete by default).
    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()>;

    /// Lists records in a space with pagination.
    async fn list(
        &self,
        space_id: &str,
        limit: usize,
        cursor: Option<&RecordId>,
    ) -> StorageResult<Vec<T>>;
}

use elephant_types::records::entity::EntityType;

/// Search criteria for querying records.
#[derive(Debug, Clone, Default)]
pub struct SearchCriteria {
    /// Full-text search query.
    pub query: Option<String>,
    /// Filter by entity type.
    pub entity_type: Option<EntityType>,
    /// Filter by `space_id`.
    pub space_id: Option<String>,
    /// Filter by entity IDs.
    pub entities: Option<Vec<String>>,
    /// Filter mode for entities (any or all).
    pub mode: Option<String>,
    /// Filter by attribution: records whose `metadata.angel_protectees`
    /// contains this value, plus records with no `angel_protectees` key.
    pub attributed_to: Option<String>,
    /// Include soft-deleted records.
    pub include_deleted: bool,
    /// Maximum number of results.
    pub limit: usize,
    /// Cursor for pagination.
    pub cursor: Option<String>,
}

impl SearchCriteria {
    /// Creates new search criteria with defaults.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            query: None,
            entity_type: None,
            space_id: None,
            entities: None,
            mode: None,
            attributed_to: None,
            include_deleted: false,
            limit: 50,
            cursor: None,
        }
    }

    /// Sets the search query.
    #[must_use]
    pub fn with_query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Sets the entity type filter.
    #[must_use]
    pub fn with_entity_type(mut self, entity_type: EntityType) -> Self {
        self.entity_type = Some(entity_type);
        self
    }

    /// Sets the `space_id` filter.
    #[must_use]
    pub fn with_space_id(mut self, space_id: impl Into<String>) -> Self {
        self.space_id = Some(space_id.into());
        self
    }

    /// Includes deleted records in results.
    #[must_use]
    pub const fn include_deleted(mut self) -> Self {
        self.include_deleted = true;
        self
    }

    /// Sets the result limit.
    #[must_use]
    pub const fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    /// Sets the pagination cursor.
    #[must_use]
    pub fn with_cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    /// Sets the entities filter.
    #[must_use]
    pub fn with_entities(mut self, entities: Vec<String>) -> Self {
        self.entities = Some(entities);
        self
    }

    /// Sets the filter mode.
    #[must_use]
    pub fn with_mode(mut self, mode: impl Into<String>) -> Self {
        self.mode = Some(mode.into());
        self
    }

    /// Sets the attribution filter.
    #[must_use]
    pub fn with_attributed_to(mut self, attributed_to: impl Into<String>) -> Self {
        self.attributed_to = Some(attributed_to.into());
        self
    }
}

/// Search repository trait for full-text and filtered search.
#[async_trait]
pub trait SearchRepository<T>
where
    T: Serialize + DeserializeOwned + Send + Sync,
{
    /// Searches records using the given criteria.
    async fn search(&self, criteria: &SearchCriteria) -> StorageResult<PaginatedResult<T>>;
}

/// Paginated result set.
#[derive(Debug, Clone)]
pub struct PaginatedResult<T> {
    /// The result items.
    pub results: Vec<T>,
    /// Cursor for next page (if any).
    pub cursor: Option<String>,
    /// Whether more results exist.
    pub has_more: bool,
    /// Total count of matching records (post-filter).
    pub total: usize,
}

impl<T> PaginatedResult<T> {
    /// Creates a new paginated result.
    #[must_use]
    pub const fn new(
        results: Vec<T>,
        cursor: Option<String>,
        has_more: bool,
        total: usize,
    ) -> Self {
        Self {
            results,
            cursor,
            has_more,
            total,
        }
    }

    /// Creates an empty result.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            results: Vec::new(),
            cursor: None,
            has_more: false,
            total: 0,
        }
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

    #[test]
    fn test_search_criteria_builder() {
        let criteria = SearchCriteria::new()
            .with_query("test")
            .with_space_id("space_1")
            .with_limit(100)
            .include_deleted();

        assert_eq!(criteria.query, Some("test".to_string()));
        assert_eq!(criteria.space_id, Some("space_1".to_string()));
        assert_eq!(criteria.limit, 100);
        assert!(criteria.include_deleted);
    }

    #[test]
    fn test_paginated_result() {
        let result: PaginatedResult<i32> =
            PaginatedResult::new(vec![1, 2, 3], Some("cursor_123".to_string()), true, 100);

        assert_eq!(result.results.len(), 3);
        assert!(result.has_more);
        assert_eq!(result.total, 100);
    }
}
