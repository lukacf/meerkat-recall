//! DAG-based dependency tracking for work items.
//!
//! Per spec §5.2: The DAG executor tracks dependencies between work items
//! and ensures items only become runnable when all dependencies are satisfied.

use std::collections::{HashMap, HashSet};

use elephant_storage::work_edge_repository::WorkEdgeRepository;
use elephant_storage::work_item_repository::WorkItemRepository;
use elephant_storage::{Repository, StorageClient};
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{WorkEdge, WorkItem, WorkItemStatus};
use elephant_types::{Link, RecordId, TableName};

use crate::error::PipelineError;

/// DAG executor for dependency-ordered work execution.
#[derive(Debug, Clone)]
pub struct DagExecutor {
    item_repo: WorkItemRepository,
    edge_repo: WorkEdgeRepository,
    #[allow(dead_code)]
    storage: StorageClient,
}

impl DagExecutor {
    /// Creates a new DAG executor.
    #[must_use]
    pub fn new(storage: StorageClient) -> Self {
        Self {
            item_repo: WorkItemRepository::new(storage.clone()),
            edge_repo: WorkEdgeRepository::new(storage.clone()),
            storage,
        }
    }

    /// Adds a dependency edge between work items.
    ///
    /// # Errors
    ///
    /// Returns an error if the edge cannot be created.
    pub async fn add_dependency(
        &self,
        from: &RecordId,
        to: &RecordId,
        space_id: &str,
    ) -> Result<WorkEdge, PipelineError> {
        let edge = WorkEdge {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkEdge),
                space_id.to_string(),
                "dag_executor".to_string(),
            ),
            from: Link::new(from.clone()),
            to: Link::new(to.clone()),
            edge_type: "depends_on".to_string(),
        };

        self.edge_repo
            .create(edge)
            .await
            .map_err(PipelineError::StorageError)
    }

    /// Gets all dependencies for a work item (items that must complete first).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_dependencies(
        &self,
        space_id: &str,
        item_id: &RecordId,
    ) -> Result<Vec<WorkItem>, PipelineError> {
        let edges = self
            .edge_repo
            .find_dependencies(item_id)
            .await
            .map_err(PipelineError::StorageError)?;

        let mut deps = Vec::new();
        for edge in edges {
            if let Some(item) = self
                .item_repo
                .get(space_id, edge.from.id())
                .await
                .map_err(PipelineError::StorageError)?
            {
                deps.push(item);
            }
        }

        Ok(deps)
    }

    /// Gets all dependents of a work item (items waiting on this one).
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get_dependents(
        &self,
        space_id: &str,
        item_id: &RecordId,
    ) -> Result<Vec<WorkItem>, PipelineError> {
        let edges = self
            .edge_repo
            .find_dependents(item_id)
            .await
            .map_err(PipelineError::StorageError)?;

        let mut dependents = Vec::new();
        for edge in edges {
            if let Some(item) = self
                .item_repo
                .get(space_id, edge.to.id())
                .await
                .map_err(PipelineError::StorageError)?
            {
                dependents.push(item);
            }
        }

        Ok(dependents)
    }

    /// Checks if all dependencies of an item are satisfied (Done).
    ///
    /// # Errors
    ///
    /// Returns an error if the check fails.
    pub async fn are_dependencies_satisfied(
        &self,
        space_id: &str,
        item_id: &RecordId,
    ) -> Result<bool, PipelineError> {
        let deps = self.get_dependencies(space_id, item_id).await?;

        for dep in deps {
            if dep.status != WorkItemStatus::Done {
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Transitions a work item's status according to the state machine.
    ///
    /// State machine:
    /// - Pending → Runnable (when all deps satisfied)
    /// - Runnable → Running (when claimed)
    /// - Running → Done | Failed (when work completes)
    ///
    /// # Errors
    ///
    /// Returns an error if the transition fails.
    pub async fn transition(
        &self,
        space_id: &str,
        item_id: &RecordId,
        new_status: WorkItemStatus,
    ) -> Result<WorkItem, PipelineError> {
        let item = self
            .item_repo
            .get(space_id, item_id)
            .await
            .map_err(PipelineError::StorageError)?
            .ok_or_else(|| PipelineError::NotFound(item_id.to_string()))?;

        // Validate transition
        self.validate_transition(item.status, new_status)?;

        let expected_status = item.status;
        let mut updated = item;
        updated.status = new_status;

        let result = self
            .item_repo
            .update_if_status(space_id, item_id, updated, expected_status)
            .await
            .map_err(PipelineError::StorageError)?;

        result.ok_or_else(|| {
            PipelineError::InvalidState(format!(
                "Work item {item_id} status changed before transition"
            ))
        })
    }

    async fn promote_if_dependencies_done(
        &self,
        space_id: &str,
        item: &WorkItem,
    ) -> Result<Option<WorkItem>, PipelineError> {
        if item.status != WorkItemStatus::Pending {
            return Ok(None);
        }

        self.validate_transition(item.status, WorkItemStatus::Runnable)?;

        let mut updated = item.clone();
        updated.status = WorkItemStatus::Runnable;

        let result = self
            .item_repo
            .update_if_status_and_dependencies_done(
                space_id,
                &item.base.id,
                updated,
                WorkItemStatus::Pending,
            )
            .await
            .map_err(PipelineError::StorageError)?;

        Ok(result)
    }

    /// Validates that a status transition is legal.
    #[allow(clippy::match_same_arms)] // Keep state transitions explicit for readability
    fn validate_transition(
        &self,
        from: WorkItemStatus,
        to: WorkItemStatus,
    ) -> Result<(), PipelineError> {
        let valid = match (from, to) {
            (WorkItemStatus::Pending, WorkItemStatus::Runnable) => true,
            (WorkItemStatus::Pending, WorkItemStatus::Cancelled) => true,
            (WorkItemStatus::Runnable, WorkItemStatus::Running) => true,
            (WorkItemStatus::Runnable, WorkItemStatus::Cancelled) => true,
            (WorkItemStatus::Running, WorkItemStatus::Done) => true,
            (WorkItemStatus::Running, WorkItemStatus::Failed) => true,
            (WorkItemStatus::Running, WorkItemStatus::Runnable) => true, // Release
            (WorkItemStatus::Running, WorkItemStatus::Cancelled) => true,
            (WorkItemStatus::Failed, WorkItemStatus::Runnable) => true, // Retry
            _ => false,
        };

        if valid {
            Ok(())
        } else {
            Err(PipelineError::InvalidStateTransition {
                from: from.to_string(),
                to: to.to_string(),
            })
        }
    }

    /// Promotes pending items to runnable when their dependencies are satisfied.
    ///
    /// # Errors
    ///
    /// Returns an error if the promotion fails.
    pub async fn promote_ready_items(
        &self,
        space_id: &str,
    ) -> Result<Vec<WorkItem>, PipelineError> {
        let pending = self
            .item_repo
            .find_by_status(space_id, WorkItemStatus::Pending)
            .await
            .map_err(PipelineError::StorageError)?;

        let mut promoted = Vec::new();
        for item in pending {
            if let Some(updated) = self.promote_if_dependencies_done(space_id, &item).await? {
                promoted.push(updated);
            }
        }

        Ok(promoted)
    }

    /// Called when a work item completes - promotes dependents that are now ready.
    ///
    /// # Errors
    ///
    /// Returns an error if promotion fails.
    pub async fn on_item_completed(
        &self,
        space_id: &str,
        item_id: &RecordId,
    ) -> Result<Vec<WorkItem>, PipelineError> {
        let dependents = self.get_dependents(space_id, item_id).await?;

        let mut promoted = Vec::new();
        for dep in dependents {
            if let Some(updated) = self.promote_if_dependencies_done(space_id, &dep).await? {
                promoted.push(updated);
            }
        }

        Ok(promoted)
    }

    /// Computes the topological order of work items.
    ///
    /// # Errors
    ///
    /// Returns an error if there's a cycle or the query fails.
    pub async fn topological_order(&self, space_id: &str) -> Result<Vec<RecordId>, PipelineError> {
        // Get all items and edges
        let items = self
            .item_repo
            .list(space_id, 1000, None)
            .await
            .map_err(PipelineError::StorageError)?;

        let edges = self
            .edge_repo
            .list(space_id, 1000, None)
            .await
            .map_err(PipelineError::StorageError)?;

        // Build adjacency list
        let mut in_degree: HashMap<RecordId, usize> = HashMap::new();
        let mut successors: HashMap<RecordId, Vec<RecordId>> = HashMap::new();

        for item in &items {
            in_degree.entry(item.base.id.clone()).or_insert(0);
            successors.entry(item.base.id.clone()).or_default();
        }

        for edge in &edges {
            *in_degree.entry(edge.to.id().clone()).or_insert(0) += 1;
            successors
                .entry(edge.from.id().clone())
                .or_default()
                .push(edge.to.id().clone());
        }

        // Kahn's algorithm
        let mut queue: Vec<RecordId> = in_degree
            .iter()
            .filter(|(_, &deg)| deg == 0)
            .map(|(id, _)| id.clone())
            .collect();

        let mut result = Vec::new();
        let mut visited = HashSet::new();

        while let Some(node) = queue.pop() {
            if visited.contains(&node) {
                continue;
            }
            visited.insert(node.clone());
            result.push(node.clone());

            if let Some(succs) = successors.get(&node) {
                for succ in succs {
                    if let Some(deg) = in_degree.get_mut(succ) {
                        *deg = deg.saturating_sub(1);
                        if *deg == 0 && !visited.contains(succ) {
                            queue.push(succ.clone());
                        }
                    }
                }
            }
        }

        if result.len() != items.len() {
            return Err(PipelineError::CycleDetected);
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    const TEST_SPACE: &str = "default";

    fn make_work_item(space_id: &str, kind: &str) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space_id.to_string(),
                "test".to_string(),
            ),
            kind: kind.to_string(),
            payload: serde_json::json!({}),
            status: WorkItemStatus::Pending,
            priority: 10,
            idempotency_key: format!("idem-{}", uuid::Uuid::new_v4()),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    #[tokio::test]
    async fn test_dependency_tracking() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let dag = DagExecutor::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        // Create two items: A → B (B depends on A)
        let item_a = make_work_item(TEST_SPACE, "task_a");
        let item_b = make_work_item(TEST_SPACE, "task_b");

        let a = item_repo.create(item_a).await.expect("create A");
        let b = item_repo.create(item_b).await.expect("create B");

        // Add dependency: B depends on A
        dag.add_dependency(&a.base.id, &b.base.id, TEST_SPACE)
            .await
            .expect("add dep");

        // Check dependencies
        let deps = dag
            .get_dependencies(TEST_SPACE, &b.base.id)
            .await
            .expect("get deps");
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0].base.id, a.base.id);

        // Check dependents
        let dependents = dag
            .get_dependents(TEST_SPACE, &a.base.id)
            .await
            .expect("get dependents");
        assert_eq!(dependents.len(), 1);
        assert_eq!(dependents[0].base.id, b.base.id);
    }

    #[tokio::test]
    async fn test_dependencies_not_satisfied() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let dag = DagExecutor::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item_a = make_work_item(TEST_SPACE, "task_a");
        let item_b = make_work_item(TEST_SPACE, "task_b");

        let a = item_repo.create(item_a).await.expect("create A");
        let b = item_repo.create(item_b).await.expect("create B");

        dag.add_dependency(&a.base.id, &b.base.id, TEST_SPACE)
            .await
            .expect("add dep");

        // B's deps are not satisfied (A is still pending)
        let satisfied = dag
            .are_dependencies_satisfied(TEST_SPACE, &b.base.id)
            .await
            .expect("check");
        assert!(!satisfied);
    }

    #[tokio::test]
    async fn test_dependencies_satisfied_after_completion() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let dag = DagExecutor::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item_a = make_work_item(TEST_SPACE, "task_a");
        let item_b = make_work_item(TEST_SPACE, "task_b");

        let a = item_repo.create(item_a).await.expect("create A");
        let b = item_repo.create(item_b).await.expect("create B");

        dag.add_dependency(&a.base.id, &b.base.id, TEST_SPACE)
            .await
            .expect("add dep");

        // Complete A
        let mut a_done = a.clone();
        a_done.status = WorkItemStatus::Done;
        item_repo
            .update(TEST_SPACE, &a.base.id, a_done)
            .await
            .expect("complete A");

        // Now B's deps are satisfied
        let satisfied = dag
            .are_dependencies_satisfied(TEST_SPACE, &b.base.id)
            .await
            .expect("check");
        assert!(satisfied);
    }

    #[tokio::test]
    async fn test_state_transitions() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let dag = DagExecutor::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item = make_work_item(TEST_SPACE, "test");
        let created = item_repo.create(item).await.expect("create");

        // Pending → Runnable
        let runnable = dag
            .transition(TEST_SPACE, &created.base.id, WorkItemStatus::Runnable)
            .await
            .expect("to runnable");
        assert_eq!(runnable.status, WorkItemStatus::Runnable);

        // Runnable → Running
        let running = dag
            .transition(TEST_SPACE, &created.base.id, WorkItemStatus::Running)
            .await
            .expect("to running");
        assert_eq!(running.status, WorkItemStatus::Running);

        // Running → Done
        let done = dag
            .transition(TEST_SPACE, &created.base.id, WorkItemStatus::Done)
            .await
            .expect("to done");
        assert_eq!(done.status, WorkItemStatus::Done);
    }

    #[tokio::test]
    async fn test_invalid_transition_rejected() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let dag = DagExecutor::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        let item = make_work_item(TEST_SPACE, "test");
        let created = item_repo.create(item).await.expect("create");

        // Pending → Done should fail (must go through Runnable, Running)
        let result = dag
            .transition(TEST_SPACE, &created.base.id, WorkItemStatus::Done)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_topological_order() {
        let storage = StorageClient::new_memory().await.expect("storage");
        let dag = DagExecutor::new(storage.clone());
        let item_repo = WorkItemRepository::new(storage);

        // Create A → B → C
        let a = item_repo
            .create(make_work_item(TEST_SPACE, "A"))
            .await
            .expect("A");
        let b = item_repo
            .create(make_work_item(TEST_SPACE, "B"))
            .await
            .expect("B");
        let c = item_repo
            .create(make_work_item(TEST_SPACE, "C"))
            .await
            .expect("C");

        dag.add_dependency(&a.base.id, &b.base.id, TEST_SPACE)
            .await
            .expect("A→B");
        dag.add_dependency(&b.base.id, &c.base.id, TEST_SPACE)
            .await
            .expect("B→C");

        let order = dag.topological_order(TEST_SPACE).await.expect("topo");
        assert_eq!(order.len(), 3);

        // A must come before B, B must come before C
        let a_pos = order.iter().position(|id| *id == a.base.id).unwrap();
        let b_pos = order.iter().position(|id| *id == b.base.id).unwrap();
        let c_pos = order.iter().position(|id| *id == c.base.id).unwrap();

        assert!(a_pos < b_pos);
        assert!(b_pos < c_pos);
    }
}
