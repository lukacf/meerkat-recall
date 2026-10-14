//! Durable, space-local acknowledgment of successfully applied outbox effects.
//!
//! Delivery is at least once: effects must be idempotent. A failed effect or a
//! failed checkpoint write leaves the prior cursor intact for replay.

use serde::Deserialize;

use crate::{StorageClient, StorageError, StorageResult};

const TABLE: &str = "outbox_checkpoint";

/// Checkpoint repository shared by the active outbox consumers.
#[derive(Debug, Clone)]
pub struct OutboxCheckpointRepository {
    client: StorageClient,
}

#[derive(Deserialize)]
struct CheckpointDb {
    space_id: String,
    consumer: String,
    seq: i64,
}

pub(crate) fn key(space_id: &str, consumer: &str) -> String {
    // Length framing prevents collisions between differently split names.
    let input = format!("{}:{space_id}{}:{consumer}", space_id.len(), consumer.len());
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, input.as_bytes()).to_string()
}

impl OutboxCheckpointRepository {
    /// Bind checkpoint operations to one storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Missing checkpoints start at zero, including after a fresh installation.
    ///
    /// # Errors
    /// Rejects malformed stored cursors and propagates storage failures.
    pub async fn load(&self, space_id: &str, consumer: &str) -> StorageResult<i64> {
        if consumer == crate::event_sequence::RESERVED_CONSUMER {
            return Err(StorageError::DataIntegrityError(
                "Producer event floor is not a consumer checkpoint".into(),
            ));
        }
        let row_key = key(space_id, consumer);
        let row: Option<CheckpointDb> = if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    tx.scan_extension_rows(TABLE)?
                        .into_iter()
                        .find(|(_, row)| row["id"] == format!("{TABLE}:{row_key}"))
                        .map(|(_, row)| {
                            serde_json::from_value(row).map_err(|error| {
                                StorageError::DataIntegrityError(error.to_string())
                            })
                        })
                        .transpose()
                })
                .await?
        } else {
            let mut response = self.client.query_bind_checked(
                "SELECT space_id, consumer, seq FROM $id",
                crate::bindings! { "id" => surrealdb::sql::Thing::from((TABLE, row_key.as_str())) },
            ).await?;
            let rows: Vec<CheckpointDb> = response.take(0).map_err(|error| {
                StorageError::DataIntegrityError(format!("Invalid outbox checkpoint: {error}"))
            })?;
            rows.into_iter().next()
        };
        match row {
            None => Ok(0),
            Some(row) if row.space_id == space_id && row.consumer == consumer && row.seq >= 0 => {
                Ok(row.seq)
            }
            Some(_) => Err(StorageError::DataIntegrityError(
                "Outbox checkpoint identity or sequence mismatch".to_string(),
            )),
        }
    }

    /// Acknowledges a complete batch using compare-and-swap. Concurrent
    /// consumers cannot regress the cursor or overwrite another batch's result.
    ///
    /// # Errors
    /// Rejects invalid advancement, changed cursors, and storage failures.
    pub async fn advance(
        &self,
        space_id: &str,
        consumer: &str,
        expected: i64,
        next: i64,
    ) -> StorageResult<()> {
        if space_id.is_empty()
            || consumer.is_empty()
            || consumer == crate::event_sequence::RESERVED_CONSUMER
            || expected < 0
            || next < expected
        {
            return Err(StorageError::DataIntegrityError(
                "Invalid outbox checkpoint advancement".to_string(),
            ));
        }
        let row_key = key(space_id, consumer);
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let id = format!("{TABLE}:{row_key}");
                    let existing = tx
                        .scan_extension_rows(TABLE)?
                        .into_iter()
                        .find(|(_, row)| row["id"] == id);
                    let current = existing
                        .map(|(_, row)| {
                            serde_json::from_value::<CheckpointDb>(row).map_err(|error| {
                                StorageError::DataIntegrityError(error.to_string())
                            })
                        })
                        .transpose()?;
                    if let Some(row) = &current {
                        if row.space_id != space_id || row.consumer != consumer {
                            return Err(StorageError::DataIntegrityError(
                                "Outbox checkpoint identity mismatch".to_string(),
                            ));
                        }
                    }
                    if current.map_or(0, |row| row.seq) != expected {
                        return Err(StorageError::Conflict(
                            "Outbox checkpoint changed".to_string(),
                        ));
                    }
                    tx.upsert_extension_row(
                        TABLE,
                        serde_json::json!({
                            "id": id, "space_id": space_id, "consumer": consumer, "seq": next,
                            "updated_at": chrono::Utc::now(),
                        }),
                    )?;
                    Ok(())
                })
                .await;
        }
        self.client.query_bind_checked(
            "BEGIN TRANSACTION;
             LET $prior = SELECT space_id, consumer, seq FROM ONLY $id;
             IF $prior IS NONE AND $expected != 0 { THROW 'outbox checkpoint conflict'; };
             IF $prior IS NOT NONE AND ($prior.space_id != $space_id OR $prior.consumer != $consumer OR $prior.seq != $expected) { THROW 'outbox checkpoint conflict'; };
             UPSERT $id SET space_id = $space_id, consumer = $consumer, seq = $next, updated_at = time::now();
             COMMIT TRANSACTION;",
            crate::bindings! {
                "id" => surrealdb::sql::Thing::from((TABLE, row_key.as_str())),
                "space_id" => space_id, "consumer" => consumer, "expected" => expected, "next" => next,
            },
        ).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn checkpoint_contract(client: StorageClient) {
        let repo = OutboxCheckpointRepository::new(client.clone());
        assert_eq!(repo.load("quiet", "wiki").await.expect("load"), 0);
        repo.advance("quiet", "wiki", 0, 7).await.expect("ack");
        let restarted = OutboxCheckpointRepository::new(client);
        assert_eq!(restarted.load("quiet", "wiki").await.expect("restart"), 7);
        assert_eq!(restarted.load("other", "wiki").await.expect("space"), 0);
        assert_eq!(restarted.load("quiet", "truth").await.expect("consumer"), 0);
        assert!(repo.advance("quiet", "wiki", 0, 9).await.is_err());
        assert!(repo.advance("quiet", "wiki", 7, 6).await.is_err());
        assert_eq!(repo.load("quiet", "wiki").await.expect("unchanged"), 7);
        // A batch that fails before acknowledgment is replayed from seven.
        repo.advance("quiet", "wiki", 7, 9)
            .await
            .expect("replayed batch");
    }

    #[tokio::test]
    async fn embedded_checkpoint_is_durable_and_space_local() {
        checkpoint_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }

    #[tokio::test]
    async fn surreal_checkpoint_is_durable_and_space_local() {
        checkpoint_contract(StorageClient::new_memory().await.expect("surreal")).await;
    }
}
