//! Extraction planning types.
//!
//! `ExtractionPlan` is the output of the `plan_extraction` work kind,
//! determining which nodes to extract and under what constraints.

use serde::{Deserialize, Serialize};

use elephant_types::records::doc_node::NodeRole;
use elephant_types::RecordId;

/// A selection of document content for extraction.
///
/// Can represent a single node or a batch of adjacent nodes grouped
/// by the planner into a right-sized extraction window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeSelection {
    /// The primary node (first node in the batch).
    pub node_id: RecordId,
    /// All node IDs in this batch (for provenance tracking).
    #[serde(default)]
    pub node_ids: Vec<RecordId>,
    /// Role of the primary node.
    pub role: NodeRole,
    /// Priority for extraction (lower = higher priority).
    pub priority: u32,
    /// Estimated token count for the entire batch.
    pub estimated_tokens: u32,
    /// Byte offset start in the revision text.
    pub span_start: u32,
    /// Byte offset end in the revision text.
    pub span_end: u32,
}

/// Stop conditions for extraction.
///
/// Extraction stops when any of these conditions are met.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)] // Contains f64
pub struct StopConditions {
    /// Coverage threshold (0.0-1.0) - stop when this fraction of nodes is processed.
    pub coverage_threshold: f64,
    /// Maximum budget in euros.
    pub budget_cap_euros: f64,
    /// Maximum number of nodes to process.
    pub max_nodes: u32,
    /// Maximum total tokens to process.
    pub max_tokens: u32,
}

impl Default for StopConditions {
    fn default() -> Self {
        Self {
            coverage_threshold: 0.95,
            budget_cap_euros: 10.0,
            max_nodes: 1000,
            max_tokens: 500_000,
        }
    }
}

/// An extraction plan produced by the planning stage.
///
/// Contains the ordered list of nodes to extract from and
/// the stop conditions for the extraction run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(clippy::derive_partial_eq_without_eq)] // Contains f64
pub struct ExtractionPlan {
    /// The revision being extracted from.
    pub revision_id: RecordId,
    /// Policy version used to create this plan.
    pub policy_version: String,
    /// Ordered list of nodes to extract from.
    pub node_selections: Vec<NodeSelection>,
    /// Stop conditions for this extraction run.
    pub stop_conditions: StopConditions,
    /// Estimated total cost in euros.
    pub estimated_cost_euros: f64,
}

impl ExtractionPlan {
    /// Creates a new extraction plan.
    #[must_use]
    pub fn new(revision_id: RecordId, policy_version: String) -> Self {
        Self {
            revision_id,
            policy_version,
            node_selections: Vec::new(),
            stop_conditions: StopConditions::default(),
            estimated_cost_euros: 0.0,
        }
    }

    /// Adds a node selection to the plan.
    pub fn add_node(&mut self, selection: NodeSelection) {
        self.node_selections.push(selection);
    }

    /// Sets the stop conditions for this plan.
    pub fn with_stop_conditions(mut self, conditions: StopConditions) -> Self {
        self.stop_conditions = conditions;
        self
    }

    /// Sets the estimated cost.
    pub fn with_estimated_cost(mut self, cost_euros: f64) -> Self {
        self.estimated_cost_euros = cost_euros;
        self
    }

    /// Returns the total estimated tokens for all selected nodes.
    #[must_use]
    pub fn total_estimated_tokens(&self) -> u32 {
        self.node_selections
            .iter()
            .map(|s| s.estimated_tokens)
            .sum()
    }

    /// Returns the number of selected nodes.
    #[must_use]
    pub fn node_count(&self) -> usize {
        self.node_selections.len()
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::float_cmp,
        unused_imports,
        dead_code
    )]

    use super::*;
    use elephant_types::ids::TableName;

    #[test]
    fn test_stop_conditions_default() {
        let conditions = StopConditions::default();
        assert_eq!(conditions.coverage_threshold, 0.95);
        assert_eq!(conditions.budget_cap_euros, 10.0);
        assert_eq!(conditions.max_nodes, 1000);
        assert_eq!(conditions.max_tokens, 500_000);
    }

    #[test]
    fn test_extraction_plan_builder() {
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let node_id = RecordId::new_v4(TableName::DocNode);

        let mut plan = ExtractionPlan::new(revision_id.clone(), "v1.0".to_string());
        plan.add_node(NodeSelection {
            node_id: node_id.clone(),
            node_ids: vec![node_id.clone()],
            role: NodeRole::Paragraph,
            priority: 1,
            estimated_tokens: 500,
            span_start: 0,
            span_end: 2000,
        });

        assert_eq!(plan.revision_id, revision_id);
        assert_eq!(plan.policy_version, "v1.0");
        assert_eq!(plan.node_count(), 1);
        assert_eq!(plan.total_estimated_tokens(), 500);
    }

    #[test]
    fn test_extraction_plan_total_tokens() {
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let mut plan = ExtractionPlan::new(revision_id, "v1.0".to_string());

        let n1 = RecordId::new_v4(TableName::DocNode);
        plan.add_node(NodeSelection {
            node_id: n1.clone(),
            node_ids: vec![n1],
            role: NodeRole::Paragraph,
            priority: 1,
            estimated_tokens: 500,
            span_start: 0,
            span_end: 2000,
        });
        let n2 = RecordId::new_v4(TableName::DocNode);
        plan.add_node(NodeSelection {
            node_id: n2.clone(),
            node_ids: vec![n2],
            role: NodeRole::Section,
            priority: 2,
            estimated_tokens: 1000,
            span_start: 2000,
            span_end: 6000,
        });

        assert_eq!(plan.total_estimated_tokens(), 1500);
        assert_eq!(plan.node_count(), 2);
    }

    #[test]
    fn test_extraction_plan_with_methods() {
        let revision_id = RecordId::new_v4(TableName::DocRevision);
        let custom_conditions = StopConditions {
            coverage_threshold: 0.80,
            budget_cap_euros: 0.5,
            max_nodes: 50,
            max_tokens: 50_000,
        };

        let plan = ExtractionPlan::new(revision_id, "v1.0".to_string())
            .with_stop_conditions(custom_conditions.clone())
            .with_estimated_cost(0.25);

        assert_eq!(plan.stop_conditions, custom_conditions);
        assert_eq!(plan.estimated_cost_euros, 0.25);
    }
}
