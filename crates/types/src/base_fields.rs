//! Builder for `BaseFields` with ergonomic defaults.

use chrono::{DateTime, Utc};

use crate::ids::{RecordId, TableName};
use crate::records::base::{BaseFields, Provenance, Security, SubjectsState};

/// Builds `BaseFields` with standard defaults for MCP tools.
#[derive(Debug, Clone)]
pub struct BaseFieldsBuilder {
    space_id: String,
    table: TableName,
    created_by: String,
    policy_id: String,
    now: Option<DateTime<Utc>>,
}

impl BaseFieldsBuilder {
    /// Creates a new builder for a record in the provided space.
    #[must_use]
    pub fn new(space_id: impl Into<String>) -> Self {
        Self {
            space_id: space_id.into(),
            table: TableName::Doc,
            created_by: "mcp_tool".to_string(),
            policy_id: "default".to_string(),
            now: None,
        }
    }

    /// Sets the table name used to generate the record ID.
    #[must_use]
    pub const fn for_table(mut self, table: TableName) -> Self {
        self.table = table;
        self
    }

    /// Overrides the `created_by` value.
    #[must_use]
    pub fn created_by(mut self, created_by: impl Into<String>) -> Self {
        self.created_by = created_by.into();
        self
    }

    /// Overrides the `security.policy_id` value.
    #[must_use]
    pub fn policy_id(mut self, policy_id: impl Into<String>) -> Self {
        self.policy_id = policy_id.into();
        self
    }

    /// Freezes timestamps for deterministic testing.
    #[must_use]
    pub const fn at(mut self, now: DateTime<Utc>) -> Self {
        self.now = Some(now);
        self
    }

    /// Builds a `BaseFields` value with defaults suitable for MCP record creation.
    #[must_use]
    pub fn build(self) -> BaseFields {
        let now = self.now.unwrap_or_else(Utc::now);
        BaseFields {
            id: RecordId::new_v4(self.table),
            space_id: self.space_id,
            created_at: now,
            updated_at: now,
            created_by: self.created_by,
            deleted_at: None,
            provenance: Provenance::default(),
            security: Security {
                policy_id: self.policy_id,
                level: 0,
                labels: Vec::new(),
                handling: Vec::new(),
            },
            subjects: Vec::new(),
            subjects_state: SubjectsState::Unknown,
        }
    }
}
