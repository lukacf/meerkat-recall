//! Private task scope captured at native query / synchronous kernel admission.

use super::{RecoveryPhase, StorageClient, StorageError, StorageResult};
use crate::erasure_journal::ErasureIntent;

#[derive(Clone, PartialEq, Eq)]
struct ReservationCustody {
    journal_identity: String,
    space_id: String,
    intent_id: String,
}

tokio::task_local! {
    static ERASURE_RESERVATION: ReservationCustody;
}

/// A runtime callback for another journal cannot borrow this task's custody.
pub fn current_erasure_reservation(journal_identity: &str) -> Option<String> {
    ERASURE_RESERVATION
        .try_with(|context| {
            (context.journal_identity == journal_identity).then(|| context.intent_id.clone())
        })
        .ok()
        .flatten()
}

impl StorageClient {
    /// Scope one original durable intent inside exclusive maintenance or startup
    /// replay. This does not acquire/release durable reservation state itself.
    pub(crate) async fn with_erasure_reservation<T, F>(
        &self,
        intent: &ErasureIntent,
        future: F,
    ) -> StorageResult<T>
    where
        F: std::future::Future<Output = StorageResult<T>>,
    {
        if !self.maintenance
            || (self.maintenance_lease.is_none()
                && self.recovery.status().phase != RecoveryPhase::Replaying)
        {
            return Err(StorageError::Conflict(
                "erasure reservation requires original maintenance custody".into(),
            ));
        }
        if !self
            .journal
            .intents()?
            .iter()
            .any(|recorded| recorded == intent)
        {
            return Err(StorageError::DataIntegrityError(
                "erasure reservation requires its original durable journal intent".into(),
            ));
        }
        let context = ReservationCustody {
            journal_identity: self.journal.identity().to_owned(),
            space_id: intent.space_id.clone(),
            intent_id: intent.intent_id.clone(),
        };
        if ERASURE_RESERVATION
            .try_with(|current| current != &context)
            .unwrap_or(false)
        {
            return Err(StorageError::Conflict(
                "nested erasure reservation cannot change original custody".into(),
            ));
        }
        ERASURE_RESERVATION.scope(context, future).await
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::Repository;
    use elephant_types::records::base::BaseFields;
    use elephant_types::records::source::{Source, SourceType};
    use elephant_types::{RecordId, TableName};
    use std::sync::atomic::{AtomicBool, Ordering};

    fn original_intent(client: &StorageClient) -> ErasureIntent {
        client
            .journal
            .record_intent("reserved-space", &RecordId::new_v4(TableName::Source))
            .unwrap()
    }

    async fn exclusive(client: &StorageClient) -> (super::super::MaintenanceGuard, StorageClient) {
        let guard = client
            .begin_maintenance(RecoveryPhase::Replaying, std::time::Duration::from_secs(5))
            .await
            .unwrap();
        let privileged = client.maintenance_client_with_guard(&guard);
        (guard, privileged)
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn reservation_scope_requires_original_intent_and_maintenance() {
        let client = StorageClient::new_embedded().await.unwrap();
        let intent = original_intent(&client);
        assert!(client
            .with_erasure_reservation(&intent, async { Ok(()) })
            .await
            .is_err());
        assert!(client
            .maintenance_client()
            .with_erasure_reservation(&intent, async { Ok(()) })
            .await
            .is_err());
        let (guard, privileged) = exclusive(&client).await;
        let mut foreign = intent.clone();
        foreign.space_id = "foreign-space".into();
        assert!(privileged
            .with_erasure_reservation(&foreign, async { Ok(()) })
            .await
            .is_err());
        privileged
            .with_erasure_reservation(&intent, async {
                assert_eq!(
                    current_erasure_reservation(client.journal.identity()).as_deref(),
                    Some(intent.intent_id.as_str())
                );
                assert!(current_erasure_reservation("another-journal").is_none());
                Ok(())
            })
            .await
            .unwrap();
        assert!(current_erasure_reservation(client.journal.identity()).is_none());
        drop(privileged);
        guard.finish();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn nested_foreign_scope_is_not_polled_and_errors_restore_previous_context() {
        let client = StorageClient::new_embedded().await.unwrap();
        let intent = original_intent(&client);
        let other = original_intent(&client);
        let (guard, privileged) = exclusive(&client).await;
        let polled = AtomicBool::new(false);
        let result: StorageResult<()> = privileged
            .with_erasure_reservation(&intent, async {
                assert!(privileged
                    .with_erasure_reservation(&other, async {
                        polled.store(true, Ordering::SeqCst);
                        Ok(())
                    })
                    .await
                    .is_err());
                assert_eq!(
                    current_erasure_reservation(client.journal.identity()).as_deref(),
                    Some(intent.intent_id.as_str())
                );
                Err(StorageError::Conflict("fixture interruption".into()))
            })
            .await;
        assert!(result.is_err());
        assert!(!polled.load(Ordering::SeqCst));
        assert!(current_erasure_reservation(client.journal.identity()).is_none());
        drop(privileged);
        guard.finish();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn native_query_captures_owner_before_dispatch_and_other_tasks_cannot_spoof_it() {
        let client = StorageClient::new(super::super::StorageConfig {
            database: format!("reservation_scope_{}", uuid::Uuid::new_v4().simple()),
            ..super::super::StorageConfig::default()
        })
        .await
        .unwrap();
        let intent = original_intent(&client);
        let (guard, privileged) = exclusive(&client).await;
        privileged
            .with_erasure_reservation(&intent, async {
                let mut response = privileged
                    .query_bind_checked(
                        "RETURN $__recall_erasure_reservation;",
                        serde_json::json!({"__recall_erasure_reservation":"caller-forged"}),
                    )
                    .await?;
                assert_eq!(
                    response.take::<Option<String>>(0).unwrap(),
                    Some(intent.intent_id.clone())
                );
                let ordinary_task = privileged.clone();
                let forged = intent.intent_id.clone();
                let actual = tokio::spawn(async move {
                    assert!(
                        current_erasure_reservation(ordinary_task.journal.identity()).is_none()
                    );
                    let mut response = ordinary_task
                        .query_bind_checked(
                            "RETURN $__recall_erasure_reservation;",
                            serde_json::json!({"__recall_erasure_reservation":forged}),
                        )
                        .await
                        .unwrap();
                    response.take::<Option<String>>(0).unwrap()
                })
                .await
                .unwrap();
                assert!(actual.is_none());
                Ok(())
            })
            .await
            .unwrap();
        drop(privileged);
        guard.finish();
        let mut response = client
            .query_checked("RETURN $__recall_erasure_reservation;")
            .await
            .unwrap();
        assert!(response.take::<Option<String>>(0).unwrap().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep fixture resources and guards alive through the original assertions and scope cleanup."
    )]
    async fn actual_kernel_transaction_keeps_owner_and_rejects_unrelated_task() {
        let client = StorageClient::new_embedded().await.unwrap();
        let intent = original_intent(&client);
        let (guard, privileged) = exclusive(&client).await;
        let source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                intent.space_id.clone(),
                "scope-fixture".into(),
            ),
            SourceType::Manual,
            "scope-payload".into(),
        );
        let blocked = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                intent.space_id.clone(),
                "other-task".into(),
            ),
            SourceType::Manual,
            "must-not-persist".into(),
        );
        privileged
            .with_erasure_reservation(&intent, async {
                crate::reference_fence::reserve_reference_guard(
                    &privileged,
                    &intent.space_id,
                    &intent,
                )
                .await?;
                // The real EmbeddedStore path calls tx_with_commit synchronously in
                // this admitted task; no background thread is assumed to inherit it.
                crate::SourceRepository::new(privileged.clone())
                    .create(source.clone())
                    .await?;
                let escaped = privileged.embedded_store().unwrap();
                let attempted = blocked.clone();
                let result = tokio::spawn(async move { escaped.create(attempted).await })
                    .await
                    .unwrap();
                assert!(matches!(result, Err(StorageError::ErasureInProgress)));
                assert!(crate::SourceRepository::new(privileged.clone())
                    .get(&intent.space_id, &blocked.base.id)
                    .await?
                    .is_none());
                privileged
                    .embedded_store()
                    .unwrap()
                    .mutate(|tx| {
                        crate::reference_fence::release_reference_guard_tx(
                            tx,
                            &intent.space_id,
                            &intent.intent_id,
                        )
                    })
                    .await?;
                Ok(())
            })
            .await
            .unwrap();
        drop(privileged);
        guard.finish();
        assert!(crate::SourceRepository::new(client.clone())
            .get(&intent.space_id, &source.base.id)
            .await
            .unwrap()
            .is_some());
        // Release is explicit and durable; unrelated tasks regain ordinary access.
        crate::SourceRepository::new(client)
            .create(blocked)
            .await
            .unwrap();
    }
}
