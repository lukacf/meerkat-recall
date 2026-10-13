//! Document node repository implementation for `SurrealDB`.
//!
//! Per spec §7.2, doc_node represents structural nodes over a revision.

use async_trait::async_trait;
use chrono::Utc;
use serde::Deserialize;
use std::fmt::Write as _;

use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::doc_node::{DocNode, NodeRole, TextSpan};
use elephant_types::records::doc_revision::DocRevision;
use elephant_types::records::entity::Entity;
use elephant_types::{Link, RecordId};

use crate::client::StorageClient;
use crate::error::{StorageError, StorageResult};
use crate::repository::Repository;

const DOC_NODE_PROJECTION: &str = r"
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
    revision,
    role,
    span,
    node_key,
    text,
    parent,
    position,
    depth,
    embedding_model,
    collection
";

/// Database-only proxy for DocNode to avoid flatten/native enum issues.
#[derive(Deserialize)]
struct DocNodeDb {
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
    revision: Option<Link<DocRevision>>,
    role: Option<NodeRole>,
    span: Option<TextSpan>,
    node_key: Option<String>,
    text: Option<String>,
    parent: Option<Link<DocNode>>,
    position: Option<i32>,
    depth: Option<i32>,
    embedding_model: Option<String>,
    collection: Option<String>,
}

impl TryFrom<DocNodeDb> for DocNode {
    type Error = StorageError;

    fn try_from(db: DocNodeDb) -> Result<Self, Self::Error> {
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
            revision: db
                .revision
                .ok_or_else(|| StorageError::QueryError("Missing revision".to_string()))?,
            role: db
                .role
                .ok_or_else(|| StorageError::QueryError("Missing role".to_string()))?,
            span: db
                .span
                .ok_or_else(|| StorageError::QueryError("Missing span".to_string()))?,
            node_key: db
                .node_key
                .ok_or_else(|| StorageError::QueryError("Missing node_key".to_string()))?,
            text: db.text,
            parent: db.parent,
            position: db
                .position
                .ok_or_else(|| StorageError::QueryError("Missing position".to_string()))?,
            depth: db
                .depth
                .ok_or_else(|| StorageError::QueryError("Missing depth".to_string()))?,
            embedding_model: db.embedding_model,
            collection: db.collection,
        })
    }
}

/// `embedding_model` marker for nodes that can never be embedded.
///
/// The embed worker sets it on nodes with an invalid span or a missing
/// revision: they leave the pending set without a vector. Null the field to
/// send a repaired node back.
pub const UNEMBEDDABLE_EMBEDDING_MODEL: &str = "unembeddable";

/// Embedding progress of one space.
///
/// Produced by [`DocNodeRepository::count_embedding_progress`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EmbeddingProgressCounts {
    /// Embeddable nodes still waiting for an embedding.
    pub pending: usize,
    /// Nodes the worker gave up on.
    ///
    /// Marked [`UNEMBEDDABLE_EMBEDDING_MODEL`]; they have no vector and are
    /// not counted as embedded.
    pub unembeddable: usize,
    /// Nodes with a real embedding, by the model that produced it.
    pub embedded_by_model: std::collections::BTreeMap<String, usize>,
}

impl EmbeddingProgressCounts {
    /// Nodes with a real embedding from any model.
    #[must_use]
    pub fn embedded(&self) -> usize {
        self.embedded_by_model.values().sum()
    }

    fn record(&mut self, model: Option<&str>, count: usize) {
        match model {
            None => self.pending += count,
            Some(UNEMBEDDABLE_EMBEDDING_MODEL) => self.unembeddable += count,
            Some(model) => *self.embedded_by_model.entry(model.to_string()).or_insert(0) += count,
        }
    }
}

/// Document node repository for `SurrealDB`.
#[derive(Clone, Debug)]
pub struct DocNodeRepository {
    client: StorageClient,
}

impl DocNodeRepository {
    /// Creates a new document node repository.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Finds all nodes for a given revision.
    ///
    /// Returns all nodes that belong to the specified revision, regardless of role.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn find_by_revision(&self, revision_id: &RecordId) -> StorageResult<Vec<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            if let Some(revision) = store
                .scan::<DocRevision>()
                .await?
                .into_iter()
                .find(|item| item.base.id == *revision_id)
            {
                return store
                    .doc_nodes_for_revision(&revision.base.space_id, revision_id)
                    .await;
            }

            let mut items: Vec<_> = store
                .scan::<DocNode>()
                .await?
                .into_iter()
                .filter(|item| item.revision.id() == revision_id && item.base.deleted_at.is_none())
                .collect();
            items.sort_by(|left, right| {
                left.depth
                    .cmp(&right.depth)
                    .then_with(|| left.position.cmp(&right.position))
                    .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
            });
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_NODE_PROJECTION} FROM doc_node WHERE revision = $revision_id AND deleted_at IS NONE ORDER BY depth, position"
                ),
                crate::bindings! { "revision_id" => crate::surreal::thing_from_record_id(revision_id) },
            )
            .await?;

        let items: Vec<DocNodeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_nodes: {e}")))?;

        items.into_iter().map(DocNode::try_from).collect()
    }

    /// Finds chunk nodes for a given revision.
    ///
    /// Returns all nodes with `role = Chunk` that belong to the specified revision.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or result parsing fails.
    pub async fn find_chunks_by_revision(
        &self,
        revision_id: &RecordId,
    ) -> StorageResult<Vec<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            if let Some(revision) = store
                .scan::<DocRevision>()
                .await?
                .into_iter()
                .find(|item| item.base.id == *revision_id)
            {
                return store
                    .doc_chunks_for_revision(&revision.base.space_id, revision_id)
                    .await;
            }

            let mut items: Vec<_> = store
                .scan::<DocNode>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.revision.id() == revision_id
                        && item.role == NodeRole::Chunk
                        && item.base.deleted_at.is_none()
                })
                .collect();
            items.sort_by(|left, right| {
                left.position
                    .cmp(&right.position)
                    .then_with(|| left.base.id.to_string().cmp(&right.base.id.to_string()))
            });
            return Ok(items);
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_NODE_PROJECTION} FROM doc_node WHERE revision = $revision_id AND role = 'chunk' AND deleted_at IS NONE ORDER BY position"
                ),
                crate::bindings! { "revision_id" => crate::surreal::thing_from_record_id(revision_id) },
            )
            .await?;

        let items: Vec<DocNodeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_nodes: {e}")))?;

        items.into_iter().map(DocNode::try_from).collect()
    }

    /// Converts a DocNode to a SurrealDB value for insertion.
    pub(crate) fn doc_node_to_value(item: &DocNode) -> StorageResult<surrealdb::sql::Value> {
        let mut obj = crate::surreal::base_fields_to_object(&item.base)?;
        obj.insert(
            "revision".to_string(),
            surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(item.revision.id())),
        );
        obj.insert(
            "role".to_string(),
            crate::surreal::enum_to_surreal(&item.role)?,
        );
        obj.insert(
            "span".to_string(),
            crate::surreal::json_to_surreal(serde_json::to_value(item.span).map_err(|e| {
                StorageError::SerializationError(format!("Failed to serialize doc_node.span: {e}"))
            })?)?,
        );
        obj.insert(
            "node_key".to_string(),
            surrealdb::sql::Value::from(item.node_key.clone()),
        );
        obj.insert(
            "text".to_string(),
            item.text
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |value| {
                    surrealdb::sql::Value::from(value.clone())
                }),
        );
        obj.insert(
            "parent".to_string(),
            item.parent
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |value| {
                    surrealdb::sql::Value::from(crate::surreal::thing_from_record_id(value.id()))
                }),
        );
        obj.insert(
            "position".to_string(),
            surrealdb::sql::Value::from(item.position),
        );
        obj.insert("depth".to_string(), surrealdb::sql::Value::from(item.depth));
        obj.insert(
            "embedding_model".to_string(),
            item.embedding_model
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |value| {
                    surrealdb::sql::Value::from(value.clone())
                }),
        );
        obj.insert(
            "collection".to_string(),
            item.collection
                .as_ref()
                .map_or(surrealdb::sql::Value::None, |value| {
                    surrealdb::sql::Value::from(value.clone())
                }),
        );
        Ok(surrealdb::sql::Value::Object(obj))
    }

    /// Gets multiple nodes by ID while preserving the requested order.
    ///
    /// Missing or soft-deleted nodes are skipped.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying storage lookup fails.
    pub async fn get_many(&self, space_id: &str, ids: &[RecordId]) -> StorageResult<Vec<DocNode>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(store) = self.client.embedded_store() {
            return store.get_many::<DocNode>(space_id, ids, false).await;
        }

        let mut nodes = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(node) = self.get(space_id, id).await? {
                nodes.push(node);
            }
        }
        Ok(nodes)
    }

    /// Updates multiple nodes in one logical operation while preserving order.
    ///
    /// # Errors
    ///
    /// Returns an error if any node is missing, deleted, or the write fails.
    pub async fn update_many(
        &self,
        space_id: &str,
        records: Vec<DocNode>,
    ) -> StorageResult<Vec<DocNode>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }

        if let Some(store) = self.client.embedded_store() {
            let space_id = space_id.to_string();
            return store
                .mutate(|mutation| {
                    let mut updated = Vec::with_capacity(records.len());
                    for record in records {
                        let id = record.base.id.clone();
                        let Some(existing) = mutation.get::<DocNode>(&space_id, &id, true)? else {
                            return Err(StorageError::NotFound(format!(
                                "DocNode not found in space: {id}"
                            )));
                        };
                        if existing.base.deleted_at.is_some() {
                            return Err(StorageError::NotFound(format!(
                                "DocNode is deleted: {id}"
                            )));
                        }
                        updated.push(mutation.replace(record)?);
                    }
                    Ok(updated)
                })
                .await;
        }

        let mut updated = Vec::with_capacity(records.len());
        for record in records {
            let id = record.base.id.clone();
            updated.push(self.update(space_id, &id, record).await?);
        }
        Ok(updated)
    }

    /// Walks parent pointers from `node_id` up to the root.
    ///
    /// Returns the chain ordered from root (document) down to the node's
    /// immediate parent. The node itself is **not** included.
    ///
    /// # Errors
    ///
    /// Returns an error if any lookup fails or data integrity is broken
    /// (e.g. a parent pointer references a non-existent node).
    #[allow(clippy::assigning_clones)]
    pub async fn get_parent_chain(&self, node_id: &RecordId) -> StorageResult<Vec<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            let Some(node) = store
                .scan::<DocNode>()
                .await?
                .into_iter()
                .find(|item| item.base.id == *node_id && item.base.deleted_at.is_none())
            else {
                return Ok(Vec::new());
            };
            return store.doc_parent_chain(&node.base.space_id, node_id).await;
        }

        // First, get the starting node to find its space_id and parent.
        // Record access (`FROM ONLY $id`) — see `get` for why.
        let mut response = self
            .client
            .query_bind(
                &format!("SELECT {DOC_NODE_PROJECTION} FROM ONLY $id WHERE deleted_at IS NONE"),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(node_id) },
            )
            .await?;

        let item: Option<DocNodeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_node: {e}")))?;

        let current = match item {
            Some(db) => DocNode::try_from(db)?,
            None => return Ok(Vec::new()),
        };

        let space_id = current.base.space_id.clone();
        let mut chain = Vec::new();
        let mut parent_link = current.parent.clone();

        // Walk up the tree collecting parents
        while let Some(ref link) = parent_link {
            let parent = self.get(&space_id, link.id()).await?.ok_or_else(|| {
                StorageError::QueryError(format!(
                    "Parent node {} not found for chain walk",
                    link.id()
                ))
            })?;
            let next_parent = parent.parent.clone();
            chain.push(parent);
            parent_link = next_parent;
        }

        // Reverse so it goes root → ... → immediate parent
        chain.reverse();
        Ok(chain)
    }

    /// Counts a space's embeddable nodes by embedding state.
    ///
    /// Embeddable means `chunk`, `paragraph` or `summary` and not deleted. The
    /// split is pending (the [`Self::find_pending_embedding`] predicate), given
    /// up ([`UNEMBEDDABLE_EMBEDDING_MODEL`]) and embedded per model.
    ///
    /// Counts the whole table: call it periodically (the embed loop does),
    /// never per request.
    ///
    /// # Errors
    ///
    /// Returns an error if the count query fails.
    pub async fn count_embedding_progress(
        &self,
        space_id: &str,
    ) -> StorageResult<EmbeddingProgressCounts> {
        #[derive(Deserialize)]
        struct ModelCountRow {
            embedding_model: Option<String>,
            total: usize,
        }

        let mut counts = EmbeddingProgressCounts::default();
        if let Some(store) = self.client.embedded_store() {
            for item in store.scan::<DocNode>().await? {
                if item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && matches!(
                        item.role,
                        NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
                    )
                {
                    counts.record(item.embedding_model.as_deref(), 1);
                }
            }
            return Ok(counts);
        }

        let mut response = self
            .client
            .query_bind(
                "SELECT embedding_model, count() AS total FROM doc_node \
                 WHERE space_id = $space_id \
                   AND role IN ['chunk', 'paragraph', 'summary'] \
                   AND deleted_at IS NONE \
                 GROUP BY embedding_model",
                crate::bindings! { "space_id" => space_id },
            )
            .await?;
        let rows: Vec<ModelCountRow> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse embedding progress counts: {e}"))
        })?;
        for row in rows {
            counts.record(row.embedding_model.as_deref(), row.total);
        }
        Ok(counts)
    }

    /// Finds embeddable nodes that have not yet been embedded.
    ///
    /// Returns nodes with roles suitable for embedding (`chunk`, `paragraph`,
    /// `summary`) that have no `embedding_model` set yet.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails or deserialization fails.
    pub async fn find_pending_embedding(
        &self,
        space_id: &str,
        limit: usize,
    ) -> StorageResult<Vec<DocNode>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut selected = Vec::with_capacity(limit);
        let mut offset = 0usize;
        loop {
            let candidates = self.pending_embedding_page(space_id, limit, offset).await?;
            let count = candidates.len();
            for node in candidates {
                if crate::unmanaged_embedding::is_unmanaged(&self.client, space_id, &node).await? {
                    selected.push(node);
                    if selected.len() == limit {
                        return Ok(selected);
                    }
                }
            }
            if count < limit {
                return Ok(selected);
            }
            offset = offset.checked_add(count).ok_or_else(|| {
                StorageError::DataIntegrityError("pending embedding cursor overflow".into())
            })?;
        }
    }

    /// Recheck generic embedding ownership after text resolution and immediately
    /// before dispatch. Source-managed nodes require their original claimed job.
    pub async fn require_unmanaged_embedding_inputs(
        &self,
        space_id: &str,
        nodes: &[DocNode],
    ) -> StorageResult<()> {
        crate::unmanaged_embedding::require_unmanaged(&self.client, space_id, nodes).await
    }

    /// Mark irrecoverable standalone inputs without changing their text or links.
    /// Source-owned nodes must be repaired by their original publication worker.
    pub async fn mark_unmanaged_embedding_failed(
        &self,
        space_id: &str,
        node_ids: &[RecordId],
        sentinel: &str,
    ) -> StorageResult<()> {
        if node_ids.is_empty() {
            return Ok(());
        }
        let guards = crate::unmanaged_embedding::prepare(&self.client, space_id, node_ids).await?;
        let now = Utc::now();
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    crate::unmanaged_embedding::check_tx(tx, space_id, &guards)?;
                    for id in node_ids {
                        let mut node =
                            tx.get::<DocNode>(space_id, id, false)?.ok_or_else(|| {
                                StorageError::Conflict(
                                    "source_embedding_requires_original_claim".into(),
                                )
                            })?;
                        node.embedding_model = Some(sentinel.to_owned());
                        node.base.updated_at = now;
                        tx.replace(node)?;
                    }
                    Ok(())
                })
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = std::collections::BTreeMap::new();
        crate::unmanaged_embedding::append_sql(&mut sql, &mut bindings, space_id, &guards);
        bindings.insert("failed_embedding_sentinel".into(), sentinel.into());
        bindings.insert(
            "failed_embedding_updated".into(),
            crate::surreal::datetime_from_chrono(now).into(),
        );
        for (index, id) in node_ids.iter().enumerate() {
            let name = format!("failed_embedding_node_{index}");
            bindings.insert(
                name.clone(),
                crate::surreal::thing_from_record_id(id).into(),
            );
            let _ = writeln!(sql, "UPDATE ${name} SET embedding_model=$failed_embedding_sentinel, updated_at=$failed_embedding_updated;");
        }
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map(|_| ())
    }

    async fn pending_embedding_page(
        &self,
        space_id: &str,
        limit: usize,
        offset: usize,
    ) -> StorageResult<Vec<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            let mut items: Vec<_> = store
                .scan::<DocNode>()
                .await?
                .into_iter()
                .filter(|item| {
                    item.base.space_id == space_id
                        && item.base.deleted_at.is_none()
                        && matches!(
                            item.role,
                            NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
                        )
                        && item.embedding_model.is_none()
                })
                .collect();
            items.sort_by(|left, right| {
                left.base
                    .created_at
                    .cmp(&right.base.created_at)
                    .then_with(|| left.base.id.cmp(&right.base.id))
            });
            return Ok(items.into_iter().skip(offset).take(limit).collect());
        }

        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_NODE_PROJECTION} FROM doc_node \
                     WHERE space_id = $space_id \
                       AND role IN ['chunk', 'paragraph', 'summary'] \
                       AND embedding_model IS NONE \
                       AND deleted_at IS NONE \
                     ORDER BY created_at, id \
                     LIMIT {limit} START {offset}"
                ),
                crate::bindings! { "space_id" => space_id },
            )
            .await?;

        let items: Vec<DocNodeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse pending embedding doc_nodes: {e}"))
        })?;

        items.into_iter().map(DocNode::try_from).collect()
    }
}

#[async_trait]
impl Repository<DocNode> for DocNodeRepository {
    async fn create(&self, record: DocNode) -> StorageResult<DocNode> {
        if let Some(store) = self.client.embedded_store() {
            return store.create(record).await;
        }

        let record_value = Self::doc_node_to_value(&record)?;
        let mut response = self
            .client
            .query_bind_checked(
                &format!("CREATE $id CONTENT $record RETURN {DOC_NODE_PROJECTION};"),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "record" => record_value,
                },
            )
            .await?;

        let items: Vec<DocNodeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse created doc_node: {e}"))
        })?;

        let item_db = items.into_iter().next().ok_or_else(|| {
            StorageError::QueryError("Create succeeded but doc_node not found".to_string())
        })?;

        DocNode::try_from(item_db)
    }

    async fn get(&self, space_id: &str, id: &RecordId) -> StorageResult<Option<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<DocNode>(space_id, id, false).await;
        }

        // Record access (`FROM ONLY $id`): the planner has no id-equality
        // fast path, so the `FROM doc_node WHERE id = $id` form scanned the
        // whole table on each point lookup (issue #34). The space_id/
        // deleted_at WHERE clause preserves space isolation.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_NODE_PROJECTION} FROM ONLY $id WHERE space_id = $space_id AND deleted_at IS NONE"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let item: Option<DocNodeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_node: {e}")))?;

        Ok(item.map(DocNode::try_from).transpose()?)
    }

    async fn update(
        &self,
        space_id: &str,
        id: &RecordId,
        record: DocNode,
    ) -> StorageResult<DocNode> {
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
                    "UPDATE $id MERGE $record WHERE space_id = $space_id AND deleted_at IS NONE RETURN {DOC_NODE_PROJECTION};"
                ),
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(id),
                    "space_id" => space_id,
                    "record" => Self::doc_node_to_value(&normalized)?,
                },
            )
            .await?;

        let items: Vec<DocNodeDb> = response.take(0).map_err(|e| {
            StorageError::QueryError(format!("Failed to parse updated doc_node: {e}"))
        })?;

        let item_db = items
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(format!("DocNode not found in space: {id}")))?;

        DocNode::try_from(item_db)
    }

    async fn delete(&self, space_id: &str, id: &RecordId) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store.soft_delete::<DocNode>(space_id, id).await;
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
    ) -> StorageResult<Vec<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            return store.list::<DocNode>(space_id, limit, cursor, false).await;
        }

        let query = if cursor.is_some() {
            format!(
                "SELECT {DOC_NODE_PROJECTION} FROM doc_node WHERE space_id = $space_id AND deleted_at IS NONE AND id > $cursor ORDER BY id LIMIT {limit}"
            )
        } else {
            format!(
                "SELECT {DOC_NODE_PROJECTION} FROM doc_node WHERE space_id = $space_id AND deleted_at IS NONE ORDER BY id LIMIT {limit}"
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

        let items: Vec<DocNodeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_nodes: {e}")))?;

        items.into_iter().map(DocNode::try_from).collect()
    }
}

impl DocNodeRepository {
    /// Reads physical row state for atomic source lifecycle inverse preparation.
    pub(crate) async fn get_including_deleted(
        &self,
        space_id: &str,
        id: &RecordId,
    ) -> StorageResult<Option<DocNode>> {
        if let Some(store) = self.client.embedded_store() {
            return store.get::<DocNode>(space_id, id, true).await;
        }

        // Record access (`FROM ONLY $id`): the planner has no id-equality
        // fast path, so the `FROM doc_node WHERE id = $id` form scanned the
        // whole table on each point lookup (issue #34). The space_id/
        // deleted_at WHERE clause preserves space isolation.
        let mut response = self
            .client
            .query_bind(
                &format!(
                    "SELECT {DOC_NODE_PROJECTION} FROM ONLY $id WHERE space_id = $space_id"
                ),
                crate::bindings! { "id" => crate::surreal::thing_from_record_id(id), "space_id" => space_id },
            )
            .await?;

        let item: Option<DocNodeDb> = response
            .take(0)
            .map_err(|e| StorageError::QueryError(format!("Failed to parse doc_node: {e}")))?;

        item.map(DocNode::try_from).transpose()
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
    use chrono::Utc;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::doc_revision::{DocRevision, RendererMetadata, RevisionStatus};
    use elephant_types::TableName;

    use std::sync::atomic::{AtomicU64, Ordering};

    static TEST_DB_COUNTER: AtomicU64 = AtomicU64::new(80200);

    const DOC_NODE_SCHEMA: &str = r"
        DEFINE TABLE doc_node SCHEMALESS;
    ";

    async fn setup_test_repo() -> DocNodeRepository {
        let db_id = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let config = StorageConfig {
            database: format!("doc_node_repo_test_{db_id}"),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();

        client.query(DOC_NODE_SCHEMA).await.unwrap();

        DocNodeRepository::new(client)
    }

    async fn setup_embedded_repo() -> DocNodeRepository {
        let client = StorageClient::new_embedded().await.unwrap();
        DocNodeRepository::new(client)
    }

    fn create_test_doc_node() -> DocNode {
        let id = RecordId::new_v4(TableName::DocNode);
        let base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        DocNode {
            base,
            revision: Link::new(revision_id),
            role: NodeRole::Chunk,
            span: TextSpan::new(0, 100),
            node_key: "node-key-123".to_string(),
            text: Some("Chunk text content".to_string()),
            parent: None,
            position: 0,
            depth: 1,

            embedding_model: None,
            collection: None,
        }
    }

    fn create_test_doc_revision(revision_id: RecordId) -> DocRevision {
        DocRevision {
            base: BaseFields::new(
                revision_id,
                "test_space".to_string(),
                "test_agent".to_string(),
            ),
            doc: Link::new(RecordId::new_v4(TableName::Doc)),
            text: "Test content".to_string(),
            text_hash: "hash123".to_string(),
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

    fn make_doc_node_db() -> DocNodeDb {
        DocNodeDb {
            id: crate::surreal::thing_from_record_id(&RecordId::new_v4(TableName::DocNode)),
            space_id: Some("space_1".to_string()),
            created_at: Some(Utc::now()),
            updated_at: Some(Utc::now()),
            created_by: Some("tester".to_string()),
            deleted_at: None,
            provenance: Some(Provenance::default()),
            security: Some(Security::default()),
            subjects: Some(Vec::new()),
            subjects_state: Some(SubjectsState::Unknown),
            revision: Some(Link::new(RecordId::new_v4(TableName::DocRevision))),
            role: Some(NodeRole::Section),
            span: Some(TextSpan::new(0, 10)),
            node_key: Some("node-key-1".to_string()),
            text: Some("Some text".to_string()),
            parent: None,
            position: Some(0),
            depth: Some(1),

            embedding_model: None,
            collection: None,
        }
    }

    #[test]
    fn test_doc_node_missing_space_id_errors() {
        let mut db = make_doc_node_db();
        db.space_id = None;
        let err = DocNode::try_from(db).expect_err("missing space_id should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("space_id")));
    }

    #[test]
    fn test_doc_node_missing_role_errors() {
        let mut db = make_doc_node_db();
        db.role = None;
        let err = DocNode::try_from(db).expect_err("missing role should error");
        assert!(matches!(err, StorageError::QueryError(msg) if msg.contains("role")));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_create_doc_node() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_node();

        let created = repo.create(item.clone()).await.unwrap();

        assert_eq!(created.role, NodeRole::Chunk);
        assert_eq!(created.node_key, "node-key-123");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_doc_node() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_node();
        let id = item.base.id.clone();

        repo.create(item).await.unwrap();

        let retrieved = repo.get("test_space", &id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().role, NodeRole::Chunk);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_doc_node_roundtrip() {
        let repo = setup_test_repo().await;
        let mut item = create_test_doc_node();
        item.role = NodeRole::Section;
        item.depth = 2;
        item.position = 5;

        let created = repo.create(item.clone()).await.unwrap();

        // Verify roundtrip
        assert_eq!(created.role, NodeRole::Section);
        assert_eq!(created.depth, 2);
        assert_eq!(created.position, 5);
        assert_eq!(created.span.start, 0);
        assert_eq!(created.span.end, 100);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_update_doc_node() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_node();
        let id = item.base.id.clone();

        let created = repo.create(item).await.unwrap();

        let mut updated = created.clone();
        updated.text = Some("Updated text".to_string());

        let result = repo.update("test_space", &id, updated).await.unwrap();
        assert_eq!(result.text, Some("Updated text".to_string()));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_delete_doc_node() {
        let repo = setup_test_repo().await;
        let item = create_test_doc_node();
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
    async fn test_get_parent_chain_three_levels() {
        let repo = setup_test_repo().await;
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        // Level 0: document root
        let mut root = create_test_doc_node();
        root.role = NodeRole::Document;
        root.depth = 0;
        root.position = 0;
        root.parent = None;
        root.revision = Link::new(revision_id.clone());
        root.node_key = "root".to_string();
        let created_root = repo.create(root).await.unwrap();

        // Level 1: section
        let mut section = create_test_doc_node();
        section.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        section.role = NodeRole::Section;
        section.depth = 1;
        section.position = 0;
        section.parent = Some(Link::new(created_root.base.id.clone()));
        section.revision = Link::new(revision_id.clone());
        section.node_key = "section".to_string();
        let created_section = repo.create(section).await.unwrap();

        // Level 2: paragraph
        let mut para = create_test_doc_node();
        para.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        para.role = NodeRole::Paragraph;
        para.depth = 2;
        para.position = 0;
        para.parent = Some(Link::new(created_section.base.id.clone()));
        para.revision = Link::new(revision_id.clone());
        para.node_key = "paragraph".to_string();
        let created_para = repo.create(para).await.unwrap();

        // Walk parent chain from paragraph
        let chain = repo.get_parent_chain(&created_para.base.id).await.unwrap();

        assert_eq!(chain.len(), 2, "Chain should include root and section");
        assert_eq!(chain[0].role, NodeRole::Document, "First should be root");
        assert_eq!(chain[1].role, NodeRole::Section, "Second should be section");
        assert_eq!(chain[0].base.id, created_root.base.id);
        assert_eq!(chain[1].base.id, created_section.base.id);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_parent_chain_root_node_returns_empty() {
        let repo = setup_test_repo().await;

        let mut root = create_test_doc_node();
        root.role = NodeRole::Document;
        root.depth = 0;
        root.parent = None;
        root.node_key = "root".to_string();
        let created_root = repo.create(root).await.unwrap();

        let chain = repo.get_parent_chain(&created_root.base.id).await.unwrap();
        assert!(chain.is_empty(), "Root node should have empty parent chain");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_get_parent_chain_nonexistent_node() {
        let repo = setup_test_repo().await;
        let fake_id = RecordId::new_v4(TableName::DocNode);

        let chain = repo.get_parent_chain(&fake_id).await.unwrap();
        assert!(
            chain.is_empty(),
            "Non-existent node should return empty chain"
        );
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_find_pending_embedding() {
        let repo = setup_test_repo().await;
        let revision_id = RecordId::new_v4(TableName::DocRevision);

        // Create a chunk node without embedding_model
        let mut chunk = create_test_doc_node();
        chunk.role = NodeRole::Chunk;
        chunk.revision = Link::new(revision_id.clone());
        chunk.node_key = "chunk1".to_string();
        chunk.embedding_model = None;
        repo.create(chunk).await.unwrap();

        // Create a paragraph node without embedding_model
        let mut para = create_test_doc_node();
        para.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        para.role = NodeRole::Paragraph;
        para.revision = Link::new(revision_id.clone());
        para.node_key = "para1".to_string();
        para.embedding_model = None;
        repo.create(para).await.unwrap();

        // Create a document node (should NOT be returned - wrong role)
        let mut doc_node = create_test_doc_node();
        doc_node.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        doc_node.role = NodeRole::Document;
        doc_node.revision = Link::new(revision_id.clone());
        doc_node.node_key = "doc1".to_string();
        doc_node.embedding_model = None;
        repo.create(doc_node).await.unwrap();

        // Create a chunk that already has embedding_model (should NOT be returned)
        let mut embedded_chunk = create_test_doc_node();
        embedded_chunk.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        embedded_chunk.role = NodeRole::Chunk;
        embedded_chunk.revision = Link::new(revision_id.clone());
        embedded_chunk.node_key = "chunk2".to_string();
        embedded_chunk.embedding_model = Some("model-v1".to_string());
        repo.create(embedded_chunk).await.unwrap();

        let pending = repo.find_pending_embedding("test_space", 10).await.unwrap();
        assert_eq!(
            pending.len(),
            2,
            "Should find chunk and paragraph without embeddings"
        );
        assert!(
            pending.iter().all(|n| n.embedding_model.is_none()),
            "All returned nodes should lack embedding_model"
        );
        assert!(
            pending.iter().all(|n| matches!(
                n.role,
                NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
            )),
            "All returned nodes should have embeddable roles"
        );

        // A dead node and a second model, to pin the split.
        let mut dead = create_test_doc_node();
        dead.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        dead.role = NodeRole::Chunk;
        dead.revision = Link::new(revision_id.clone());
        dead.node_key = "chunk-dead".to_string();
        dead.embedding_model = Some(UNEMBEDDABLE_EMBEDDING_MODEL.to_string());
        repo.create(dead).await.unwrap();
        let mut other_model = create_test_doc_node();
        other_model.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        other_model.role = NodeRole::Summary;
        other_model.revision = Link::new(revision_id.clone());
        other_model.node_key = "summary-v2".to_string();
        other_model.embedding_model = Some("model-v2".to_string());
        repo.create(other_model).await.unwrap();

        // Two pending, one given up (not embedded), one per real model, and
        // the document-role node on no side.
        let counts = repo.count_embedding_progress("test_space").await.unwrap();
        assert_eq!(counts.pending, 2);
        assert_eq!(counts.unembeddable, 1);
        assert_eq!(counts.embedded(), 2);
        assert_eq!(counts.embedded_by_model.get("model-v1"), Some(&1));
        assert_eq!(counts.embedded_by_model.get("model-v2"), Some(&1));
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn test_embedded_find_by_revision_and_chunks_do_not_require_active_revision_row() {
        let repo = setup_embedded_repo().await;
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let store = repo.client.embedded_store().expect("embedded store");

        store
            .create(create_test_doc_revision(revision_id.clone()))
            .await
            .expect("create revision");

        let mut chunk = create_test_doc_node();
        chunk.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        chunk.revision = Link::new(revision_id.clone());
        chunk.role = NodeRole::Chunk;
        chunk.node_key = "chunk-node".to_string();
        chunk.depth = 1;
        chunk.position = 1;
        let chunk_id = chunk.base.id.clone();
        repo.create(chunk).await.expect("create chunk node");

        let mut section = create_test_doc_node();
        section.base = BaseFields::new(
            RecordId::new_v4(TableName::DocNode),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        section.revision = Link::new(revision_id.clone());
        section.role = NodeRole::Section;
        section.node_key = "section-node".to_string();
        section.depth = 0;
        section.position = 0;
        let section_id = section.base.id.clone();
        repo.create(section).await.expect("create section node");

        store
            .soft_delete::<DocRevision>("test_space", &revision_id)
            .await
            .expect("soft delete revision");

        let nodes = repo
            .find_by_revision(&revision_id)
            .await
            .expect("find nodes by revision");
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].base.id, section_id);
        assert_eq!(nodes[1].base.id, chunk_id);

        let chunks = repo
            .find_chunks_by_revision(&revision_id)
            .await
            .expect("find chunks by revision");
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].base.id, chunk_id);
    }
}
