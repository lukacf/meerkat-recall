//! Outbox event producer.

use elephant_types::RecordId;
use serde::{Deserialize, Serialize};

/// Event types for the outbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventOperation {
    /// Record created.
    Create,
    /// Record updated.
    Update,
    /// Record soft-deleted.
    Delete,
    /// Record restored.
    Restore,
    /// Fact promoted.
    Promote,
    /// Entities merged.
    Merge,
}

/// A record reference in an outbox event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordRef {
    /// Record ID.
    pub id: RecordId,
    /// Table name.
    pub table: String,
    /// Operation performed.
    pub op: EventOperation,
}

/// Outbox event payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutboxPayload {
    /// Space ID.
    pub space_id: String,
    /// Event type (e.g., "ingestion.completed").
    pub event_type: String,
    /// Actor who triggered the event.
    pub actor: String,
    /// Run ID for grouping.
    #[serde(default)]
    pub run_id: Option<String>,
    /// Affected records.
    pub records: Vec<RecordRef>,
}

impl OutboxPayload {
    /// Creates a new outbox payload.
    #[must_use]
    pub const fn new(space_id: String, event_type: String, actor: String) -> Self {
        Self {
            space_id,
            event_type,
            actor,
            run_id: None,
            records: Vec::new(),
        }
    }

    /// Sets the run ID.
    #[must_use]
    pub fn with_run_id(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    /// Adds a record reference (builder pattern).
    #[must_use]
    pub fn with_record(mut self, record: RecordRef) -> Self {
        self.records.push(record);
        self
    }

    /// Adds a record reference (mutating).
    pub fn add_record(&mut self, record: RecordRef) {
        self.records.push(record);
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
    use elephant_types::ids::TableName;

    #[test]
    fn test_outbox_payload() {
        let mut payload = OutboxPayload::new(
            "space_1".to_string(),
            "entity.created".to_string(),
            "test_agent".to_string(),
        );

        let id = RecordId::new_v4(TableName::Entity);
        payload.add_record(RecordRef {
            id,
            table: "entity".to_string(),
            op: EventOperation::Create,
        });

        assert_eq!(payload.records.len(), 1);
    }

    #[test]
    fn test_event_operation_all_variants_serde() {
        let test_cases = [
            (EventOperation::Create, "\"create\""),
            (EventOperation::Update, "\"update\""),
            (EventOperation::Delete, "\"delete\""),
            (EventOperation::Restore, "\"restore\""),
            (EventOperation::Promote, "\"promote\""),
            (EventOperation::Merge, "\"merge\""),
        ];

        for (op, expected) in &test_cases {
            let json = serde_json::to_string(op).unwrap();
            assert_eq!(&json, expected);

            let decoded: EventOperation = serde_json::from_str(&json).unwrap();
            assert_eq!(&decoded, op);
        }
    }

    #[test]
    fn test_event_operation_variants_unique() {
        let ops = [
            EventOperation::Create,
            EventOperation::Update,
            EventOperation::Delete,
            EventOperation::Restore,
            EventOperation::Promote,
            EventOperation::Merge,
        ];

        for op in &ops {
            let count = ops.iter().filter(|o| *o == op).count();
            assert_eq!(count, 1, "Each operation should be unique");
        }
    }

    #[test]
    fn test_record_ref_serde_roundtrip() {
        let record_ref = RecordRef {
            id: RecordId::new_v4(TableName::Entity),
            table: "entity".to_string(),
            op: EventOperation::Update,
        };

        let json = serde_json::to_string(&record_ref).unwrap();
        let decoded: RecordRef = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.table, "entity");
        assert_eq!(decoded.op, EventOperation::Update);
    }

    #[test]
    fn test_outbox_payload_serde_roundtrip() {
        let mut payload = OutboxPayload::new(
            "space_123".to_string(),
            "fact.promoted".to_string(),
            "agent_worker".to_string(),
        );
        payload.run_id = Some("run_456".to_string());
        payload.add_record(RecordRef {
            id: RecordId::new_v4(TableName::Entity),
            table: "entity".to_string(),
            op: EventOperation::Promote,
        });

        let json = serde_json::to_string(&payload).unwrap();
        let decoded: OutboxPayload = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.space_id, "space_123");
        assert_eq!(decoded.event_type, "fact.promoted");
        assert_eq!(decoded.actor, "agent_worker");
        assert_eq!(decoded.run_id, Some("run_456".to_string()));
        assert_eq!(decoded.records.len(), 1);
    }

    #[test]
    fn test_outbox_payload_empty_records() {
        let payload = OutboxPayload::new(
            "space".to_string(),
            "system.heartbeat".to_string(),
            "system".to_string(),
        );

        let json = serde_json::to_string(&payload).unwrap();
        let decoded: OutboxPayload = serde_json::from_str(&json).unwrap();

        assert!(decoded.records.is_empty());
        assert!(decoded.run_id.is_none());
    }

    #[test]
    fn test_outbox_payload_multiple_records() {
        let mut payload = OutboxPayload::new(
            "space".to_string(),
            "batch.completed".to_string(),
            "batch_worker".to_string(),
        );

        for i in 0..5 {
            payload.add_record(RecordRef {
                id: RecordId::new_v4(TableName::Entity),
                table: format!("entity_{i}"),
                op: EventOperation::Create,
            });
        }

        assert_eq!(payload.records.len(), 5);

        let json = serde_json::to_string(&payload).unwrap();
        let decoded: OutboxPayload = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.records.len(), 5);
    }

    #[test]
    fn test_outbox_payload_unicode_fields() {
        let payload = OutboxPayload::new(
            "空间_123".to_string(),
            "事件.创建".to_string(),
            "代理_αβγ".to_string(),
        );

        let json = serde_json::to_string(&payload).unwrap();
        let decoded: OutboxPayload = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.space_id, "空间_123");
        assert_eq!(decoded.event_type, "事件.创建");
        assert_eq!(decoded.actor, "代理_αβγ");
    }

    #[test]
    fn test_record_ref_all_table_types() {
        let tables = [
            "entity",
            "relationship",
            "rel",
            "fact",
            "document",
            "system_event",
        ];

        for table in &tables {
            let record_ref = RecordRef {
                id: RecordId::new_v4(TableName::Entity),
                table: (*table).to_string(),
                op: EventOperation::Create,
            };

            let json = serde_json::to_string(&record_ref).unwrap();
            let decoded: RecordRef = serde_json::from_str(&json).unwrap();
            assert_eq!(decoded.table, *table);
        }
    }

    #[test]
    fn test_outbox_payload_builder_pattern() {
        let record = RecordRef {
            id: RecordId::new_v4(TableName::Entity),
            table: "entity".to_string(),
            op: EventOperation::Create,
        };

        let payload = OutboxPayload::new(
            "space_1".to_string(),
            "entity.created".to_string(),
            "agent".to_string(),
        )
        .with_run_id("run_123")
        .with_record(record);

        assert_eq!(payload.space_id, "space_1");
        assert_eq!(payload.run_id, Some("run_123".to_string()));
        assert_eq!(payload.records.len(), 1);
    }

    #[test]
    fn test_outbox_payload_builder_multiple_records() {
        let payload = OutboxPayload::new(
            "space".to_string(),
            "batch".to_string(),
            "worker".to_string(),
        )
        .with_record(RecordRef {
            id: RecordId::new_v4(TableName::Entity),
            table: "entity".to_string(),
            op: EventOperation::Create,
        })
        .with_record(RecordRef {
            id: RecordId::new_v4(TableName::Rel),
            table: "rel".to_string(),
            op: EventOperation::Create,
        });

        assert_eq!(payload.records.len(), 2);
    }
}
