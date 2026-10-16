//! Durable, space-local acknowledgment of successfully applied outbox effects.
//!
//! Delivery is at least once: effects must be idempotent. A failed effect or a
//! failed checkpoint write leaves the prior cursor intact for replay.

use serde::{Deserialize, Serialize};
use std::fmt::Write as _;

use crate::{StorageClient, StorageError, StorageResult};

fn historical_event_effects(
    response: &mut surrealdb::Response,
    space: &str,
) -> StorageResult<Vec<HistoricalEventEffect>> {
    let index = response.num_statements().checked_sub(1).ok_or_else(|| {
        StorageError::DataIntegrityError("Historical event effects missing".into())
    })?;
    let effects: Vec<HistoricalEventEffect> = response
        .take(index)
        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
    let expected_ids = std::iter::once(crate::event_sequence::RESERVED_CONSUMER)
        .chain(HISTORICAL_CONSUMERS.iter().copied())
        .map(|consumer| {
            surrealdb::sql::Thing::from((TABLE.to_owned(), key(space, consumer))).to_string()
        })
        .collect::<Vec<_>>();
    if effects.len() != expected_ids.len()
        || effects.iter().zip(expected_ids).any(|(e, id)| {
            e.table != TABLE
                || e.id != id
                || e.after_sha256.len() != 64
                || !e.after_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        })
    {
        return Err(StorageError::DataIntegrityError(
            "Historical event effects differ".into(),
        ));
    }
    Ok(effects)
}

fn archived_event_effects(
    response: &mut surrealdb::Response,
    space: &str,
) -> StorageResult<Vec<HistoricalEventEffect>> {
    let index = response
        .num_statements()
        .checked_sub(1)
        .ok_or_else(|| StorageError::DataIntegrityError("Archived event effects missing".into()))?;
    let native_effects: Vec<HistoricalEventEffect> = response
        .take(index)
        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
    let expected = std::iter::once(crate::event_sequence::RESERVED_CONSUMER)
        .chain(HISTORICAL_CONSUMERS.iter().copied())
        .map(|consumer| {
            surrealdb::sql::Thing::from((TABLE.to_owned(), key(space, consumer))).to_string()
        })
        .collect::<Vec<_>>();
    if native_effects.len() != expected.len()
        || native_effects.iter().zip(expected).any(|(effect, id)| {
            effect.table != TABLE
                || effect.id != id
                || effect.before_sha256.is_some()
                || effect.after_sha256.len() != 64
                || !effect
                    .after_sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
    {
        return Err(StorageError::DataIntegrityError(
            "Archived event effects differ".into(),
        ));
    }
    Ok(native_effects)
}

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
    #[serde(default)]
    historical_baseline: Option<HistoricalEventBaseline>,
}

/// Initial event cursor, explicitly not a claim that imported effects ran.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalEventBaseline {
    /// Version of the serialized historical proof.
    pub version: u32,
    /// SHA-256 of the complete immutable source snapshot file.
    pub snapshot_sha256: String,
    /// Identity of the recovery journal bound to the imported event cursor.
    pub journal_id: String,
    /// Highest original event sequence explicitly retained without delivery.
    pub through_seq: i64,
    /// Explicit `historical_preserved_not_delivered` status, not an effect receipt.
    pub disposition: String,
}

impl HistoricalEventBaseline {
    pub(crate) fn validate(&self, journal_id: &str, seq: i64) -> StorageResult<()> {
        if self.version != 1
            || self.snapshot_sha256.len() != 64
            || !self
                .snapshot_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.journal_id != journal_id
            || self.through_seq < 0
            || self.through_seq > seq
            || self.disposition != "historical_preserved_not_delivered"
        {
            return Err(StorageError::DataIntegrityError(
                "Invalid historical event baseline".into(),
            ));
        }
        Ok(())
    }
}

/// Exact native control changes produced by historical event sealing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalEventEffect {
    /// Physical native table containing this exact inventory effect.
    pub table: String,
    /// Exact native record identity of the affected row.
    pub id: String,
    /// Original full native row digest, or absent if the row did not exist.
    pub before_sha256: Option<String>,
    /// Resulting full native row digest, including its control wrapper.
    pub after_sha256: String,
}

/// Sealed historical event cursors and their exact native inventory effects.
#[derive(Debug)]
pub struct HistoricalEventRegistration {
    /// Historical event cursors sealed for the affected consumers.
    pub baselines: Vec<HistoricalEventBaseline>,
    /// Exact native control changes for final inventory reconciliation.
    pub native_effects: Vec<HistoricalEventEffect>,
}

/// Active consumers whose historical event replay can create new work.
pub const HISTORICAL_CONSUMERS: &[&str] = &["knowledge_views", "truth_maintenance"];

pub(crate) fn key(space_id: &str, consumer: &str) -> String {
    // Length framing prevents collisions between differently split names.
    let input = format!("{}:{space_id}{}:{consumer}", space_id.len(), consumer.len());
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, input.as_bytes()).to_string()
}

fn reserved_consumer(consumer: &str) -> bool {
    consumer == crate::event_sequence::RESERVED_CONSUMER
        || consumer == crate::historical_archive_lifecycle::RESERVED_ARCHIVE_CONSUMER
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
        if reserved_consumer(consumer) {
            return Err(StorageError::DataIntegrityError(
                "Reserved control is not a consumer checkpoint".into(),
            ));
        }
        let row = self.read_row(space_id, consumer).await?;
        self.validate_row(space_id, consumer, row.as_ref())
    }

    fn validate_row(
        &self,
        space_id: &str,
        consumer: &str,
        row: Option<&CheckpointDb>,
    ) -> StorageResult<i64> {
        match row {
            None => Ok(0),
            Some(row) if row.space_id == space_id && row.consumer == consumer && row.seq >= 0 => {
                if let Some(proof) = &row.historical_baseline {
                    proof.validate(self.client.erasure_journal()?.identity(), row.seq)?;
                    if !HISTORICAL_CONSUMERS.contains(&consumer) {
                        return Err(StorageError::DataIntegrityError(
                            "Historical baseline consumer differs".into(),
                        ));
                    }
                }
                Ok(row.seq)
            }
            Some(_) => Err(StorageError::DataIntegrityError(
                "Outbox checkpoint identity or sequence mismatch".to_string(),
            )),
        }
    }

    async fn read_row(
        &self,
        space_id: &str,
        consumer: &str,
    ) -> StorageResult<Option<CheckpointDb>> {
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
                "SELECT space_id, consumer, seq, historical_baseline FROM $id",
                crate::bindings! { "id" => surrealdb::sql::Thing::from((TABLE, row_key.as_str())) },
            ).await?;
            let rows: Vec<CheckpointDb> = response.take(0).map_err(|error| {
                StorageError::DataIntegrityError(format!("Invalid outbox checkpoint: {error}"))
            })?;
            rows.into_iter().next()
        };
        Ok(row)
    }

    /// Initialize active consumer cursors at the exact imported event tail.
    /// Retains the original events and an explicit not-delivered provenance.
    /// Only a fresh target transfer owner can call this; ordinary checkpoint
    /// advance never constructs or removes this proof.
    pub async fn preserve_historical_events(
        &self,
        space: &str,
        snapshot_sha256: &str,
    ) -> StorageResult<Vec<HistoricalEventBaseline>> {
        Ok(self
            .preserve_historical_events_with_effects(space, snapshot_sha256)
            .await?
            .baselines)
    }

    /// Seal original event delivery and return only the guarded native controls.
    pub async fn preserve_historical_events_with_effects(
        &self,
        space: &str,
        snapshot_sha256: &str,
    ) -> StorageResult<HistoricalEventRegistration> {
        self.client.require_transfer_custody()?;
        let marker = self
            .client
            .erasure_journal()?
            .transfer_marker()?
            .ok_or_else(|| StorageError::Conflict("No transfer marker".into()))?;
        if marker.source_sha256 != snapshot_sha256 || space.is_empty() {
            return Err(StorageError::Conflict(
                "Historical event input differs".into(),
            ));
        }
        let journal_id = self.client.erasure_journal()?.identity().to_owned();
        let tail = crate::SystemEventRepository::new(self.client.clone())
            .get_latest_seq(space)
            .await?;
        let proof = HistoricalEventBaseline {
            version: 1,
            snapshot_sha256: snapshot_sha256.into(),
            journal_id,
            through_seq: tail,
            disposition: "historical_preserved_not_delivered".into(),
        };
        proof.validate(self.client.erasure_journal()?.identity(), tail)?;
        let native_effects = if let Some(store) = self.client.embedded_store() {
            let proof = proof.clone();
            store.mutate(|tx| {
                self.client.require_transfer_custody()?;
                let actual=tx.scan::<elephant_types::records::SystemEvent>()?.into_iter().filter(|e|e.base.space_id==space).map(|e|e.seq).max().unwrap_or(0);
                if actual!=tail {return Err(StorageError::Conflict("Historical event tail changed".into()));}
                crate::event_sequence::reserve_tx(tx,space,tail,0)?;
                for consumer in HISTORICAL_CONSUMERS {
                    let id=format!("{TABLE}:{}",key(space,consumer));
                    if tx.scan_extension_rows(TABLE)?.iter().any(|(_,row)|row["id"]==id) {return Err(StorageError::Conflict("Historical consumer already exists".into()));}
                    tx.upsert_extension_row(TABLE,serde_json::json!({"id":id,"space_id":space,"consumer":consumer,"seq":tail,"updated_at":chrono::Utc::now(),"historical_baseline":proof}))?;
                }
                Ok(())
            }).await?;
            Vec::new()
        } else {
            let mut sql = String::from("BEGIN TRANSACTION; LET $floor_before = (SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $floor_id);\n");
            let mut bindings = crate::bindings! {"space"=>space,"tail"=>tail,"proof"=>crate::surreal::json_to_surreal(serde_json::to_value(&proof).map_err(|e|StorageError::SerializationError(e.to_string()))?)?};
            bindings.insert(
                "floor_id".into(),
                surrealdb::sql::Thing::from((
                    TABLE.to_owned(),
                    key(space, crate::event_sequence::RESERVED_CONSUMER),
                ))
                .into(),
            );
            let floor = crate::event_sequence::append_reserve_sql(
                &mut sql,
                &mut bindings,
                space,
                "historical",
                tail,
                0,
            )?;
            writeln!(
                sql,
                "IF {floor} != $tail {{ THROW 'historical_event_tail_changed'; }};"
            )
            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            for (i, consumer) in HISTORICAL_CONSUMERS.iter().enumerate() {
                bindings.insert(format!("consumer_{i}"), (*consumer).into());
                bindings.insert(
                    format!("id_{i}"),
                    surrealdb::sql::Thing::from((TABLE.to_owned(), key(space, consumer))).into(),
                );
                writeln!(sql,"CREATE $id_{i} CONTENT {{space_id:$space,consumer:$consumer_{i},seq:$tail,updated_at:time::now(),historical_baseline:$proof}};").map_err(|e|StorageError::SerializationError(e.to_string()))?;
            }
            sql.push_str("RETURN [{table:'outbox_checkpoint',id:type::string($floor_id),before_sha256:$floor_before,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $floor_id)}");
            for i in 0..HISTORICAL_CONSUMERS.len() {
                write!(sql, ",{{table:'outbox_checkpoint',id:type::string($id_{i}),before_sha256:NONE,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $id_{i})}}").map_err(|e|StorageError::SerializationError(e.to_string()))?;
            }
            sql.push_str("]; COMMIT TRANSACTION;");
            self.client.require_transfer_custody()?;
            let mut response = self.client.query_bind_checked(&sql, bindings).await?;
            historical_event_effects(&mut response, space)?
        };
        for consumer in HISTORICAL_CONSUMERS {
            if self.load(space, consumer).await? != tail {
                return Err(StorageError::DataIntegrityError(
                    "Historical cursor readback differs".into(),
                ));
            }
        }
        Ok(HistoricalEventRegistration {
            baselines: vec![proof; HISTORICAL_CONSUMERS.len()],
            native_effects,
        })
    }

    /// Preserve a verified managed archive's event tail without dispatching it.
    /// Original namespaces remain in the archive; only the selected namespace
    /// is projected. The producer floor and both historical consumers are
    /// committed atomically, with exact native effects for reconciliation.
    ///
    /// # Errors
    /// Refuses absent/stale archive custody, ambiguous namespace projection,
    /// existing native events or checkpoints, and malformed returned effects.
    pub async fn preserve_archived_events_with_effects(
        &self,
        space: &str,
        archive: &crate::historical_archive_lifecycle::SealedOperationalArchive,
    ) -> StorageResult<HistoricalEventRegistration> {
        archive.validate_custody(&self.client)?;
        self.client.require_transfer_custody()?;
        if space.is_empty() || self.client.is_embedded() {
            return Err(StorageError::DataIntegrityError(
                "Invalid archived event scope/backend".into(),
            ));
        }
        let binding = archive.binding();
        let mut tails = std::collections::BTreeMap::new();
        for (original, tail) in &archive.seal().event_tails {
            let projected = if original == &binding.source_space_id {
                &binding.target_space_id
            } else {
                original
            };
            if tail.rows == 0
                || tail.max_seq < 0
                || tails.insert(projected.clone(), tail.max_seq).is_some()
            {
                return Err(StorageError::DataIntegrityError(
                    "Archived event namespace or tail differs".into(),
                ));
            }
        }
        let tail = tails.get(space).copied().unwrap_or(0);
        let proof = HistoricalEventBaseline {
            version: 1,
            snapshot_sha256: binding.snapshot_sha256.clone(),
            journal_id: binding.journal_identity.clone(),
            through_seq: tail,
            disposition: "historical_preserved_not_delivered".into(),
        };
        proof.validate(self.client.erasure_journal()?.identity(), tail)?;
        let mut bindings = crate::bindings! {
            "space"=>space,"tail"=>tail,
            "proof"=>crate::surreal::json_to_surreal(serde_json::to_value(&proof)
                .map_err(|e|StorageError::SerializationError(e.to_string()))?)?,
            "floor_id"=>surrealdb::sql::Thing::from((TABLE.to_owned(),key(space,crate::event_sequence::RESERVED_CONSUMER))),
        };
        let mut sql = String::from(
            "BEGIN TRANSACTION;
            LET $live = SELECT id FROM system_event WHERE space_id=$space LIMIT 1;
            IF array::len($live) != 0 { THROW 'archived_event_native_rows_present'; };
            LET $floor_before = SELECT * FROM ONLY $floor_id;
            IF $floor_before != NONE { THROW 'archived_event_floor_exists'; };
",
        );
        let floor = crate::event_sequence::append_reserve_sql(
            &mut sql,
            &mut bindings,
            space,
            "archived",
            tail,
            0,
        )?;
        writeln!(
            sql,
            "IF {floor} != $tail {{ THROW 'archived_event_tail_changed'; }};"
        )
        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        for (i, consumer) in HISTORICAL_CONSUMERS.iter().enumerate() {
            bindings.insert(format!("consumer_{i}"), (*consumer).into());
            bindings.insert(
                format!("id_{i}"),
                surrealdb::sql::Thing::from((TABLE.to_owned(), key(space, consumer))).into(),
            );
            writeln!(sql,
                "LET $owners_{i}=SELECT id FROM outbox_checkpoint WHERE space_id=$space AND consumer=$consumer_{i};
                 IF array::len($owners_{i}) != 0 {{ THROW 'archived_event_consumer_exists'; }};
                 CREATE $id_{i} CONTENT {{space_id:$space,consumer:$consumer_{i},seq:$tail,updated_at:time::now(),historical_baseline:$proof}} RETURN NONE;")
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
        }
        sql.push_str("RETURN [{table:'outbox_checkpoint',id:type::string($floor_id),before_sha256:NONE,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $floor_id)}");
        for i in 0..HISTORICAL_CONSUMERS.len() {
            write!(sql, ",{{table:'outbox_checkpoint',id:type::string($id_{i}),before_sha256:NONE,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $id_{i})}}")
                .map_err(|e|StorageError::SerializationError(e.to_string()))?;
        }
        sql.push_str("]; COMMIT TRANSACTION;");
        archive.validate_custody(&self.client)?;
        let mut response = self.client.query_bind_checked(&sql, bindings).await?;
        let native_effects = archived_event_effects(&mut response, space)?;
        for consumer in HISTORICAL_CONSUMERS {
            if self.load(space, consumer).await? != tail {
                return Err(StorageError::DataIntegrityError(
                    "Archived event cursor readback differs".into(),
                ));
            }
        }
        Ok(HistoricalEventRegistration {
            baselines: vec![proof; HISTORICAL_CONSUMERS.len()],
            native_effects,
        })
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
            || reserved_consumer(consumer)
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
                    if let Some(proof) = current
                        .as_ref()
                        .and_then(|row| row.historical_baseline.as_ref())
                    {
                        proof.validate(self.client.erasure_journal()?.identity(), expected)?;
                        if !HISTORICAL_CONSUMERS.contains(&consumer) {
                            return Err(StorageError::DataIntegrityError(
                                "Historical baseline consumer differs".into(),
                            ));
                        }
                    }
                    let historical_baseline = current
                        .as_ref()
                        .and_then(|row| row.historical_baseline.clone());
                    if current.map_or(0, |row| row.seq) != expected {
                        return Err(StorageError::Conflict(
                            "Outbox checkpoint changed".to_string(),
                        ));
                    }
                    let mut updated = serde_json::json!({
                        "id": id, "space_id": space_id, "consumer": consumer, "seq": next,
                        "updated_at": chrono::Utc::now(),
                    });
                    if let Some(proof) = historical_baseline {
                        updated["historical_baseline"] = serde_json::to_value(proof)
                            .map_err(|e| StorageError::SerializationError(e.to_string()))?;
                    }
                    tx.upsert_extension_row(TABLE, updated)?;
                    Ok(())
                })
                .await;
        }
        let original = self.read_row(space_id, consumer).await?;
        if self.validate_row(space_id, consumer, original.as_ref())? != expected {
            return Err(StorageError::Conflict("Outbox checkpoint changed".into()));
        }
        let baseline = original
            .and_then(|row| row.historical_baseline)
            .map(|proof| {
                crate::surreal::json_to_surreal(
                    serde_json::to_value(proof)
                        .map_err(|e| StorageError::SerializationError(e.to_string()))?,
                )
            })
            .transpose()?
            .unwrap_or(surrealdb::sql::Value::None);
        self.advance_native(space_id, consumer, expected, next, baseline)
            .await
    }

    async fn advance_native(
        &self,
        space_id: &str,
        consumer: &str,
        expected: i64,
        next: i64,
        baseline: surrealdb::sql::Value,
    ) -> StorageResult<()> {
        let row_key = key(space_id, consumer);
        self.client.query_bind_checked(
            "BEGIN TRANSACTION;
             LET $prior = SELECT space_id, consumer, seq, historical_baseline FROM ONLY $id;
             IF $prior IS NONE AND ($expected != 0 OR $baseline IS NOT NONE) { THROW 'outbox checkpoint conflict'; };
             IF $prior IS NOT NONE AND ($prior.space_id != $space_id OR $prior.consumer != $consumer OR $prior.seq != $expected OR $prior.historical_baseline != $baseline) { THROW 'outbox checkpoint conflict'; };
             UPSERT $id SET space_id = $space_id, consumer = $consumer, seq = $next, updated_at = time::now();
             COMMIT TRANSACTION;",
            crate::bindings! {
                "id" => surrealdb::sql::Thing::from((TABLE, row_key.as_str())),
                "space_id" => space_id, "consumer" => consumer, "expected" => expected, "next" => next, "baseline" => baseline,
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

#[cfg(test)]
#[path = "historical_outbox_tests.rs"]
mod historical_tests;

#[cfg(test)]
#[path = "historical_archived_outbox_tests.rs"]
mod archived_tests;
