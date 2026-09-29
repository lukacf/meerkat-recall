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
    /// Allocates a sequence number and persists the event.
    ///
    /// # Errors
    ///
    /// Returns an error if sequence allocation or persistence fails.
    pub async fn write(&self, payload: OutboxPayload) -> OutboxResult<SystemEvent> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let seq = mutation
                        .scan::<SystemEvent>()?
                        .into_iter()
                        .filter(|event| event.base.space_id == payload.space_id)
                        .map(|event| event.seq)
                        .max()
                        .unwrap_or(0)
                        + 1;

                    let id = RecordId::new_v4(TableName::SystemEvent);
                    let mut base =
                        BaseFields::new(id, payload.space_id.clone(), payload.actor.clone());
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
                        seq,
                        event_type: payload.event_type.clone(),
                        payload: serde_json::to_value(&payload).unwrap_or_default(),
                    };

                    mutation.create(event.clone())?;
                    Ok(event)
                })
                .await
                .map_err(|error| OutboxError::WriteError(error.to_string()));
        }

        let mut last_error: Option<OutboxError> = None;

        for attempt in 0..MAX_WRITE_RETRIES {
            // Get next sequence number for this space
            let seq = self.next_sequence(&payload.space_id).await?;

            // Create the system event
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
                seq,
                event_type: payload.event_type.clone(),
                payload: serde_json::to_value(&payload).unwrap_or_default(),
            };

            // Persist to SurrealDB
            match self.persist(&event).await {
                Ok(()) => return Ok(event),
                Err(OutboxError::WriteError(msg)) => {
                    if Self::is_retryable_write_error(&msg) && attempt + 1 < MAX_WRITE_RETRIES {
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

    /// Gets the next sequence number for a space.
    async fn next_sequence(&self, space_id: &str) -> OutboxResult<i64> {
        #[derive(serde::Deserialize, Default)]
        struct MaxSeq {
            max_seq: Option<i64>,
        }

        let mut result = self
            .client
            .query_bind(
                "SELECT math::max(seq) as max_seq FROM system_event WHERE space_id = $space_id GROUP ALL",
                serde_json::json!({ "space_id": space_id }),
            )
            .await
            .map_err(|e| OutboxError::SequenceError(e.to_string()))?;

        let rows: Vec<MaxSeq> = result
            .take(0)
            .map_err(|e| OutboxError::SequenceError(e.to_string()))?;

        let max = rows.first().and_then(|r| r.max_seq).unwrap_or(0);
        Ok(max + 1)
    }

    fn is_retryable_write_error(msg: &str) -> bool {
        let lower = msg.to_lowercase();
        lower.contains("unique")
            || lower.contains("constraint")
            || lower.contains("system_event_space_seq")
            || lower.contains("conflict")
            || lower.contains("retry")
    }

    /// Persists a system event to storage.
    async fn persist(&self, event: &SystemEvent) -> OutboxResult<()> {
        let id_str = event.base.id.to_string();
        let uuid = id_str.split(':').nth(1).unwrap_or("");
        let bindings = serde_json::json!({
            "id": uuid,
            "space_id": event.base.space_id,
            "created_at": event.base.created_at,
            "updated_at": event.base.updated_at,
            "created_by": event.base.created_by,
            "deleted_at": event.base.deleted_at,
            "security": event.base.security,
            "provenance": event.base.provenance,
            "subjects": event
                .base
                .subjects
                .iter()
                .map(|s| s.id().to_string())
                .collect::<Vec<_>>(),
            "subjects_state": match event.base.subjects_state {
                SubjectsState::Complete => "complete",
                SubjectsState::Partial => "partial",
                SubjectsState::Unknown => "unknown",
            },
            "seq": event.seq,
            "event_type": event.event_type,
            "payload": event.payload,
        });

        let mut response = self
            .client
            .query_bind(
                r"
                CREATE type::thing('system_event', $id) CONTENT {
                    space_id: $space_id,
                    created_at: type::datetime($created_at),
                    updated_at: type::datetime($updated_at),
                    created_by: $created_by,
                    deleted_at: IF $deleted_at IS NONE THEN NONE ELSE type::datetime($deleted_at) END,
                    security: $security,
                    provenance: {
                        sources: array::map($provenance.sources, |$s| type::thing(string::split($s, ':')[0], string::split($s, ':')[1])),
                        extractor: $provenance.extractor,
                        run_id: $provenance.run_id,
                        chain: $provenance.chain
                    },
                    subjects: IF $subjects IS NONE THEN NONE ELSE array::map($subjects, |$s| type::thing(string::split($s, ':')[0], string::split($s, ':')[1])) END,
                    subjects_state: $subjects_state,
                    seq: $seq,
                    event_type: $event_type,
                    payload: $payload
                };
                ",
                bindings,
            )
            .await
            .map_err(|e| OutboxError::WriteError(e.to_string()))?;

        // Ensure statement errors surface (SurrealDB may return per-statement errors in Response).
        let _: surrealdb::Value = response
            .take(0usize)
            .map_err(|e| OutboxError::WriteError(e.to_string()))?;

        Ok(())
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
