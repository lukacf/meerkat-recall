//! Bounded retries for the one atomic source-acceptance transaction.
//!
//! Simultaneous independent source PUTs contend on the same space/configuration
//! fence. Reuse the complete original SQL and bindings after a proven rollback;
//! never reprepare source authority or replay a provider operation.

use super::source_acceptance_retry::{proves_retryable_abort, retry_aborted, ErrorEvidence};
use super::{check_query_response, reference_custody, StorageClient, StorageClientInner};
use crate::error::{StorageError, StorageResult};

fn evidence(error: &surrealdb::Error) -> ErrorEvidence<'_> {
    match error {
        surrealdb::Error::Db(surrealdb::error::Db::TxRetryable) => ErrorEvidence::NativeRetryable,
        surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecutedDetail { message }) => {
            ErrorEvidence::NativeFailedDetail(message)
        }
        surrealdb::Error::Db(surrealdb::error::Db::QueryNotExecuted) => {
            ErrorEvidence::RollbackMarker
        }
        surrealdb::Error::Api(surrealdb::error::Api::Query(message)) => {
            ErrorEvidence::RemoteQuery(message)
        }
        // In particular, a Thrown string, timeout, transport error or unknown
        // commit acknowledgement never becomes definite-abort evidence.
        _ => ErrorEvidence::Other,
    }
}

impl StorageClient {
    /// Execute the source repository's single explicit BEGIN..COMMIT batch.
    ///
    /// Only confirmed native/remote transaction aborts permit another attempt.
    /// Every attempt retains the exact original bindings and all in-transaction
    /// source, idempotency, authorization and lifecycle fences. No provider or
    /// preparatory work is included. At most sixteen attempts may start, and no
    /// retry starts after the two-second admission budget. An in-flight query
    /// retains its existing storage deadline and cancellation custody; this is
    /// not a two-second cancellation of an ambiguously committed transaction.
    pub(crate) async fn query_bind_source_acceptance<T: serde::Serialize + Clone + 'static>(
        &self,
        sql: &str,
        bindings: T,
    ) -> StorageResult<surrealdb::Response> {
        let StorageClientInner::Surreal { db } = &self.inner else {
            return Err(StorageError::QueryError(
                "Raw queries are not supported for elephant-db".into(),
            ));
        };
        // Freeze the native binding value once. Each retry also uses the same
        // trusted-query custody path; all transaction predicates recheck it.
        let bindings = reference_custody::binding_value(bindings)?;
        retry_aborted(
            || async {
                let query = self
                    .trusted_query(db, sql, Some(bindings.clone()))
                    .map_err(|error| (error, false))?;
                match self.run_query(query).await {
                    Ok(response) => {
                        let aborted = proves_retryable_abort(
                            response.errors().map(|(_, error)| evidence(error)),
                        );
                        check_query_response(response).map_err(|message| {
                            (StorageError::classify_query_error(message), aborted)
                        })
                    }
                    Err(error) => {
                        let aborted = match &error {
                            StorageError::Surreal(cause) => {
                                proves_retryable_abort(std::iter::once(evidence(cause)))
                            }
                            _ => false,
                        };
                        Err((error, aborted))
                    }
                }
            },
            || uuid::Uuid::new_v4().as_bytes()[0],
        )
        .await
    }
}
