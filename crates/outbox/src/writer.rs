//! Outbox writer for persisting system events.

use elephant_storage::StorageClient;
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
use elephant_types::records::SystemEvent;
use elephant_types::RecordId;
use std::time::Duration;
use tokio::time::sleep;

use crate::error::{OutboxError, OutboxResult};
use crate::producer::OutboxPayload;

const MAX_WRITE_RETRIES: usize = 20;

/// Writes system events to the outbox.
#[derive(Clone)]
pub struct OutboxWriter {
    client: StorageClient,
}

impl OutboxWriter {
    /// Creates a new outbox writer.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Writes an event to the outbox.
    ///
    /// Reserves its sequence and persists the event in one transaction, above
    /// both current events and the durable floor retained by earlier deletions.
    ///
    /// # Errors
    ///
    /// Returns an error if sequence allocation or persistence fails.
    pub async fn write(&self, payload: OutboxPayload) -> OutboxResult<SystemEvent> {
        let serialized_payload = serde_json::to_value(&payload)
            .map_err(|error| OutboxError::WriteError(error.to_string()))?;
        // Embedded mutations serialize locally. Keep the native writer's
        // existing conflict retry limit and backoff around the whole transaction.
        let attempts = if self.client.embedded_store().is_some() {
            1
        } else {
            MAX_WRITE_RETRIES
        };
        let mut last_error: Option<OutboxError> = None;

        for attempt in 0..attempts {
            let id = RecordId::new_v4(TableName::SystemEvent);
            let mut base = BaseFields::new(id, payload.space_id.clone(), payload.actor.clone());
            base.security = Security {
                policy_id: "default".to_string(),
                level: 0,
                labels: vec![],
                handling: vec![],
            };
            base.provenance = Provenance::default();
            if let Some(run_id) = &payload.run_id {
                base.provenance.run_id = Some(run_id.clone());
            }
            base.subjects_state = SubjectsState::Complete;

            let event = SystemEvent {
                base,
                // Only the atomic storage helper assigns the actual sequence.
                seq: 0,
                event_type: payload.event_type.clone(),
                payload: serialized_payload.clone(),
            };

            match elephant_storage::event_sequence::append_system_event(&self.client, event)
                .await
                .map_err(|error| OutboxError::WriteError(error.to_string()))
            {
                Ok(event) => return Ok(event),
                Err(OutboxError::WriteError(msg)) => {
                    if Self::is_retryable_write_error(&msg) && attempt + 1 < attempts {
                        last_error = Some(OutboxError::WriteError(msg));
                        let backoff_ms = 5u64.saturating_mul((attempt + 1) as u64);
                        sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                    return Err(OutboxError::WriteError(msg));
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            OutboxError::WriteError("Failed to allocate unique sequence".to_string())
        }))
    }

    fn is_retryable_write_error(msg: &str) -> bool {
        let lower = msg.to_lowercase();
        lower.contains("unique")
            || lower.contains("constraint")
            || lower.contains("system_event_space_seq")
            || lower.contains("conflict")
            || lower.contains("retry")
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
    use elephant_storage::StorageConfig;

    const SCHEMA: &str = r"
        DEFINE TABLE system_event SCHEMALESS;
        DEFINE INDEX system_event_space_seq ON TABLE system_event FIELDS space_id, seq UNIQUE;
    ";

    async fn setup_test_writer() -> OutboxWriter {
        let config = StorageConfig {
            database: format!(
                "outbox_writer_test_{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ),
            ..StorageConfig::default()
        };
        let client = StorageClient::new(config).await.unwrap();
        client.query(SCHEMA).await.unwrap();
        OutboxWriter::new(client)
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original writer fixture lifetime through event sequence, type and space assertions."
    )]
    async fn test_write_event() {
        let writer = setup_test_writer().await;
        let payload = OutboxPayload::new(
            "test_space".to_string(),
            "entity.created".to_string(),
            "test_agent".to_string(),
        );

        let event = writer.write(payload).await.unwrap();

        assert_eq!(event.seq, 1);
        assert_eq!(event.event_type, "entity.created");
        assert_eq!(event.base.space_id, "test_space");
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain one original writer fixture across the complete sequence loop and every monotonicity assertion."
    )]
    async fn test_sequence_increments() {
        let writer = setup_test_writer().await;

        for i in 1..=3 {
            let payload = OutboxPayload::new(
                "test_space".to_string(),
                format!("event.{i}"),
                "test_agent".to_string(),
            );
            let event = writer.write(payload).await.unwrap();
            assert_eq!(event.seq, i64::from(i));
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original writer fixture lifetime through all interleaved per-space sequence assertions."
    )]
    async fn test_sequence_per_space() {
        let writer = setup_test_writer().await;

        // Write to space A
        let payload_a = OutboxPayload::new(
            "space_a".to_string(),
            "event.1".to_string(),
            "agent".to_string(),
        );
        let event_a = writer.write(payload_a).await.unwrap();
        assert_eq!(event_a.seq, 1);

        // Write to space B - should start at 1
        let payload_b = OutboxPayload::new(
            "space_b".to_string(),
            "event.1".to_string(),
            "agent".to_string(),
        );
        let event_b = writer.write(payload_b).await.unwrap();
        assert_eq!(event_b.seq, 1);

        // Write to space A again - should be 2
        let payload_a2 = OutboxPayload::new(
            "space_a".to_string(),
            "event.2".to_string(),
            "agent".to_string(),
        );
        let event_a2 = writer.write(payload_a2).await.unwrap();
        assert_eq!(event_a2.seq, 2);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Retain the original writer fixture lifetime through the exact serialized records and run-id assertions."
    )]
    async fn test_payload_serialization() {
        use crate::producer::{EventOperation, RecordRef};

        let writer = setup_test_writer().await;
        let mut payload = OutboxPayload::new(
            "test_space".to_string(),
            "batch.completed".to_string(),
            "test_agent".to_string(),
        );
        payload.run_id = Some("run_123".to_string());
        payload.add_record(RecordRef {
            id: RecordId::new_v4(TableName::Entity),
            table: "entity".to_string(),
            op: EventOperation::Create,
        });

        let event = writer.write(payload).await.unwrap();

        // Check payload was serialized
        assert!(event.payload.get("records").is_some());
        assert!(event.payload.get("run_id").is_some());
    }
}
