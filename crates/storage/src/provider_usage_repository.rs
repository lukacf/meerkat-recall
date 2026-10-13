//! Durable per-physical-request receipts. Reservations and missing receipts are
//! never presented as measured provider consumption.

use chrono::{DateTime, Utc};
use elephant_types::provider_binding::ModelBindingProvenance;
use elephant_types::publication::AttemptFence;
use serde::{Deserialize, Serialize};

use crate::{StorageClient, StorageError, StorageResult};

const TABLE: &str = "provider_call_receipt";

/// Durable accounting for one physical provider request, including retries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCallReceipt {
    /// Unique UUID assigned before this physical request is sent.
    pub call_id: String,
    /// Named space whose approval authorized the request.
    pub space_id: String,
    /// WorkRun ID for queue attempts, or a unique background activity ID.
    pub scope_id: String,
    /// Original queue authority, absent for an independently scoped activity.
    pub attempt: Option<AttemptFence>,
    /// Approved provider, model, and configuration identities; no credentials.
    pub provenance: ModelBindingProvenance,
    /// Cumulative measured input tokens, or unknown when absent.
    pub input_tokens: Option<u64>,
    /// Cumulative measured output tokens, or unknown when absent.
    pub output_tokens: Option<u64>,
    /// A complete provider response was observed. Intermediate usage snapshots
    /// and historical receipts without this marker remain incomplete.
    #[serde(default)]
    pub usage_complete: bool,
    /// Time the dispatch receipt was durably created.
    pub started_at: DateTime<Utc>,
    /// Time of the latest accepted usage observation.
    pub updated_at: DateTime<Utc>,
}

impl ProviderCallReceipt {
    fn verify(&self) -> StorageResult<()> {
        if self.space_id.is_empty()
            || self.scope_id.is_empty()
            || self.provenance.config.space_id != self.space_id
            || self
                .attempt
                .as_ref()
                .is_some_and(|attempt| attempt.space_id != self.space_id)
            || uuid::Uuid::parse_str(&self.call_id).is_err()
        {
            return Err(StorageError::DataIntegrityError(
                "Provider receipt identity mismatch".into(),
            ));
        }
        Ok(())
    }

    fn observe(&mut self, input: Option<u64>, output: Option<u64>) -> StorageResult<()> {
        // Streaming reports cumulative per-call snapshots. Duplicate snapshots
        // replace, never add, and older snapshots cannot reduce known usage.
        for (known, next) in [
            (&mut self.input_tokens, input),
            (&mut self.output_tokens, output),
        ] {
            if let Some(next) = next {
                *known = Some(known.map_or(next, |known| known.max(next)));
            }
        }
        self.input_tokens
            .unwrap_or(0)
            .checked_add(self.output_tokens.unwrap_or(0))
            .ok_or_else(|| StorageError::DataIntegrityError("Provider usage overflow".into()))?;
        self.updated_at = Utc::now();
        Ok(())
    }
}

/// Persist physical dispatches and monotonic per-call usage observations.
#[derive(Debug, Clone)]
pub struct ProviderUsageRepository {
    client: StorageClient,
}

struct ReceiptObservation {
    input: Option<u64>,
    output: Option<u64>,
    complete: bool,
}

#[derive(Deserialize)]
struct ReceiptDb {
    payload: String,
}

impl ProviderUsageRepository {
    /// Bind accounting operations to one storage client.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Must persist before the physical request may leave the process.
    ///
    /// # Errors
    /// Rejects malformed identities, claimed initial consumption, stale attempts,
    /// duplicate call IDs, and storage failures.
    pub async fn start(&self, receipt: ProviderCallReceipt) -> StorageResult<()> {
        receipt.verify()?;
        if receipt.input_tokens.is_some()
            || receipt.output_tokens.is_some()
            || receipt.usage_complete
        {
            return Err(StorageError::DataIntegrityError(
                "Dispatch begins without consumption".into(),
            ));
        }
        let id = format!("{TABLE}:{}", receipt.call_id);
        let payload = serde_json::to_string(&receipt).map_err(invalid)?;
        if let Some(store) = self.client.embedded_store() {
            return store.mutate(|tx| {
                if let Some(attempt) = &receipt.attempt {
                    crate::work_attempt_repository::check_attempt_fence(tx, attempt, Utc::now())?;
                }
                if tx.scan_extension_rows(TABLE)?.iter().any(|(_, row)| row["id"] == id) {
                    return Err(StorageError::Conflict("Provider call ID already dispatched".into()));
                }
                tx.upsert_extension_row(TABLE, serde_json::json!({"id":id,"space_id":receipt.space_id,"scope_id":receipt.scope_id,"payload":payload}))?;
                Ok(())
            }).await;
        }
        let mut sql = "BEGIN TRANSACTION;".to_string();
        let mut bindings = crate::bindings! {
            "receipt_id" => surrealdb::sql::Thing::from((TABLE, receipt.call_id.as_str())),
            "space" => receipt.space_id.as_str(), "scope" => receipt.scope_id.as_str(), "payload" => payload,
        };
        if let Some(attempt) = &receipt.attempt {
            crate::work_attempt_repository::append_attempt_fence(&mut sql, &mut bindings, attempt)?;
        }
        sql.push_str("CREATE $receipt_id SET space_id=$space, scope_id=$scope, payload=$payload; COMMIT TRANSACTION;");
        self.client.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }

    /// A measured late response may update its own receipt after lease expiry;
    /// it grants no right to publish knowledge or complete its old work item.
    ///
    /// # Errors
    /// Rejects missing receipts, changed ownership/provenance, overflow, and
    /// concurrent observation conflicts; propagates storage errors.
    pub async fn observe(
        &self,
        space: &str,
        scope: &str,
        call: &str,
        provenance: &ModelBindingProvenance,
        input: Option<u64>,
        output: Option<u64>,
    ) -> StorageResult<()> {
        self.update_observation(
            space,
            scope,
            call,
            provenance,
            ReceiptObservation {
                input,
                output,
                complete: false,
            },
        )
        .await
    }

    /// Mark an actual complete provider response, independently of its usage
    /// fields. A late terminal response grants no queue/publication authority.
    ///
    /// # Errors
    /// Rejects missing receipts, changed ownership/provenance and storage errors.
    pub async fn finish(
        &self,
        space: &str,
        scope: &str,
        call: &str,
        provenance: &ModelBindingProvenance,
    ) -> StorageResult<()> {
        self.update_observation(
            space,
            scope,
            call,
            provenance,
            ReceiptObservation {
                input: None,
                output: None,
                complete: true,
            },
        )
        .await
    }

    async fn update_observation(
        &self,
        space: &str,
        scope: &str,
        call: &str,
        provenance: &ModelBindingProvenance,
        observation: ReceiptObservation,
    ) -> StorageResult<()> {
        let id = format!("{TABLE}:{call}");
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    let (_, mut row) = tx
                        .scan_extension_rows(TABLE)?
                        .into_iter()
                        .find(|(_, row)| row["id"] == id)
                        .ok_or_else(|| StorageError::NotFound(id.clone()))?;
                    let mut receipt = decode(
                        row["payload"]
                            .as_str()
                            .ok_or_else(|| invalid("Missing receipt payload"))?,
                    )?;
                    verify_owner(&receipt, space, scope, provenance)?;
                    receipt.observe(observation.input, observation.output)?;
                    receipt.usage_complete |= observation.complete;
                    row["payload"] =
                        serde_json::json!(serde_json::to_string(&receipt).map_err(invalid)?);
                    tx.upsert_extension_row(TABLE, row)?;
                    Ok(())
                })
                .await;
        }
        // Serializing this call's observers is done by the pipeline observer.
        // CAS also protects against another process replaying the same receipt.
        let mut response = self.client.query_bind_checked("SELECT payload FROM $receipt_id WHERE space_id=$space AND scope_id=$scope", crate::bindings! {
            "receipt_id" => surrealdb::sql::Thing::from((TABLE,call)), "space" => space, "scope" => scope,
        }).await?;
        let rows: Vec<ReceiptDb> = response.take(0).map_err(invalid)?;
        let row = rows
            .into_iter()
            .next()
            .ok_or_else(|| StorageError::NotFound(id))?;
        let mut receipt = decode(&row.payload)?;
        verify_owner(&receipt, space, scope, provenance)?;
        receipt.observe(observation.input, observation.output)?;
        receipt.usage_complete |= observation.complete;
        self.client.query_bind_checked("BEGIN TRANSACTION; LET $prior = SELECT VALUE payload FROM ONLY $receipt_id; IF $prior != $expected { THROW 'provider receipt changed'; }; UPDATE $receipt_id SET payload=$next; COMMIT TRANSACTION;",crate::bindings! {
            "receipt_id" => surrealdb::sql::Thing::from((TABLE,call)), "expected" => row.payload,
            "next" => serde_json::to_string(&receipt).map_err(invalid)?,
        }).await?;
        Ok(())
    }

    /// Read every physical request receipt for this exact space and run/activity.
    ///
    /// # Errors
    /// Returns storage or malformed receipt errors without truncating results.
    pub async fn for_scope(
        &self,
        space: &str,
        scope: &str,
    ) -> StorageResult<Vec<ProviderCallReceipt>> {
        let payloads = if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    tx.scan_extension_rows(TABLE)?
                        .into_iter()
                        .filter(|(_, row)| row["space_id"] == space && row["scope_id"] == scope)
                        .map(|(_, row)| {
                            row["payload"]
                                .as_str()
                                .map(str::to_string)
                                .ok_or_else(|| invalid("Missing receipt payload"))
                        })
                        .collect::<StorageResult<Vec<_>>>()
                })
                .await?
        } else {
            let mut response = self.client.query_bind_checked("SELECT payload FROM provider_call_receipt WHERE space_id=$space AND scope_id=$scope",crate::bindings! {"space"=>space,"scope"=>scope}).await?;
            let rows: Vec<ReceiptDb> = response.take(0).map_err(invalid)?;
            rows.into_iter().map(|row| row.payload).collect()
        };
        payloads
            .into_iter()
            .map(|payload| {
                let receipt = decode(&payload)?;
                receipt.verify()?;
                if receipt.space_id != space || receipt.scope_id != scope {
                    return Err(invalid("Provider receipt scope mismatch"));
                }
                Ok(receipt)
            })
            .collect()
    }
}

fn invalid(error: impl std::fmt::Display) -> StorageError {
    StorageError::DataIntegrityError(error.to_string())
}
fn decode(payload: &str) -> StorageResult<ProviderCallReceipt> {
    serde_json::from_str(payload).map_err(invalid)
}
fn verify_owner(
    receipt: &ProviderCallReceipt,
    space: &str,
    scope: &str,
    provenance: &ModelBindingProvenance,
) -> StorageResult<()> {
    if receipt.space_id != space || receipt.scope_id != scope || &receipt.provenance != provenance {
        return Err(invalid("Provider receipt owner or binding changed"));
    }
    Ok(())
}
