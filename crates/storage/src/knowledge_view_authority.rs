//! Original managed invocation authority retained through every view successor.
use std::collections::BTreeMap;

use elephant_types::publication::AttemptFence;
use elephant_types::records::{WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};
use serde::Deserialize;
use surrealdb::sql::Value;

use crate::canonical_commit::{append_row_guard, CanonicalRecord};
use crate::{
    EmbeddedMutation, KnowledgeViewRepository, Repository, StorageClient, StorageError,
    StorageResult, WorkItemRepository,
};

/// Prepared authority of the original invocation. No serialized descriptor can
/// mint this proof, and later checks never adopt a replacement claim or grant.
#[derive(Clone)]
pub struct PreparedKnowledgeViewAuthority {
    client: StorageClient,
    space: String,
    attempt: Option<AttemptFence>,
    grant_rows: Vec<WorkItem>,
}
impl std::fmt::Debug for PreparedKnowledgeViewAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedKnowledgeViewAuthority")
            .field("space", &self.space)
            .field("has_original_attempt", &self.attempt.is_some())
            .field("has_managed_grant", &!self.grant_rows.is_empty())
            .finish_non_exhaustive()
    }
}
fn refused() -> StorageError {
    StorageError::Conflict("knowledge_view_original_authority_changed".into())
}

impl KnowledgeViewRepository {
    /// Capture once before any compilation or provider input read.
    ///
    /// Synchronous
    /// managed invocations have a grant; queued invocations carry their original
    /// claim. At least one explicit authority must be provided.
    pub async fn prepare_managed_publication(
        &self,
        space: &str,
        attempt: Option<&AttemptFence>,
        grant: Option<&RecordId>,
    ) -> StorageResult<PreparedKnowledgeViewAuthority> {
        if space.trim().is_empty()
            || (attempt.is_none() && grant.is_none())
            || attempt.is_some_and(|fence| {
                fence.space_id != space
                    || fence.work_item_id.table() != TableName::WorkItem
                    || fence.generation == 0
                    || i64::try_from(fence.generation).is_err()
                    || fence.lease_token.is_empty()
            })
        {
            return Err(refused());
        }
        let grant_rows = if let Some(grant) = grant {
            WorkItemRepository::new(self.client.clone())
                .capture_managed_wiki_grant(space, grant)
                .await?
        } else {
            Vec::new()
        };
        let proof = PreparedKnowledgeViewAuthority {
            client: self.client.clone(),
            space: space.into(),
            attempt: attempt.cloned(),
            grant_rows,
        };
        proof.verify_current().await?;
        Ok(proof)
    }
}

impl PreparedKnowledgeViewAuthority {
    /// Read-only gate for every provider retry and cached-result return. The
    /// actual mutation repeats the same guards inside its write transaction.
    pub async fn verify_current(&self) -> StorageResult<()> {
        self.verify_on(&self.client).await
    }

    /// Verify cached/context access belongs to this original invocation's space.
    pub async fn verify_current_for_space(&self, space: &str) -> StorageResult<()> {
        self.check_space(space)?;
        self.verify_current().await
    }

    /// Exact originally captured lock/grant rows for the definition transaction's
    /// read set. This accessor never recaptures a newer registration.
    #[must_use]
    pub fn original_grant_rows(&self) -> &[WorkItem] {
        &self.grant_rows
    }

    pub(crate) fn check_space(&self, space: &str) -> StorageResult<()> {
        if self.space != space {
            return Err(refused());
        }
        Ok(())
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "The persisted attempt space_id must match the originally captured authority space; work identity, generation and lease are checked separately."
    )]
    pub(crate) async fn verify_on(&self, client: &StorageClient) -> StorageResult<()> {
        let works = WorkItemRepository::new(client.clone());
        for original in &self.grant_rows {
            if works.get(&self.space, &original.base.id).await?.as_ref() != Some(original) {
                return Err(refused());
            }
        }
        if let Some(fence) = &self.attempt {
            let item = works
                .get(&self.space, &fence.work_item_id)
                .await?
                .ok_or_else(refused)?;
            drop(works);
            if item.status != WorkItemStatus::Running
                || item.leased_by.as_deref() != Some(fence.lease_token.as_str())
                || item
                    .lease_expires_at
                    .is_none_or(|expires| expires <= chrono::Utc::now())
                || crate::work_attempt_repository::fence_from_claim(&item)? != *fence
            {
                return Err(refused());
            }
            if let Some(store) = client.embedded_store() {
                // Use the bounded read-only inventory seam: even an empty
                // mutation closure could trigger a managed WAL checkpoint.
                let expected_id = format!("work_attempt:{}", fence.work_item_id.id());
                let mut after = None;
                let current = loop {
                    let page = store
                        .extension_rows_page("work_attempt", &self.space, after.as_deref(), 101)
                        .await?;
                    let Some((last, _)) = page.last() else {
                        return Err(refused());
                    };
                    if let Some((_, value)) = page.iter().find(|(id, _)| id == &expected_id) {
                        break serde_json::from_value::<crate::work_attempt_repository::WorkAttempt>(
                            value.clone(),
                        )
                        .map_err(|_| refused())?;
                    }
                    if last >= &expected_id || page.len() < 101 {
                        return Err(refused());
                    }
                    after = Some(last.clone());
                };
                if current.space_id != self.space
                    || current.work_item_id != fence.work_item_id
                    || current.generation != fence.generation
                    || current.lease_token != fence.lease_token
                    || current.status != "running"
                    || current
                        .lease_expires_at
                        .is_none_or(|expires| expires <= chrono::Utc::now())
                {
                    return Err(refused());
                }
            } else {
                #[derive(Deserialize)]
                struct AttemptRow {
                    space_id: String,
                    work_item_id: surrealdb::sql::Thing,
                    generation: u64,
                    lease_token: String,
                    status: String,
                    lease_expires_at: Option<chrono::DateTime<chrono::Utc>>,
                }
                let mut response = client.query_bind_checked(
                    "SELECT space_id,work_item_id,generation,lease_token,status,lease_expires_at FROM $attempt;",
                    crate::bindings! { "attempt" => crate::work_attempt_repository::attempt_id(&fence.work_item_id) },
                ).await?;
                let rows: Vec<AttemptRow> = response
                    .take(0)
                    .map_err(|error| StorageError::QueryError(error.to_string()))?;
                let [current] = rows.as_slice() else {
                    return Err(refused());
                };
                if current.space_id != self.space
                    || current.work_item_id
                        != crate::surreal::thing_from_record_id(&fence.work_item_id)
                    || current.generation != fence.generation
                    || current.lease_token != fence.lease_token
                    || current.status != "running"
                    || current
                        .lease_expires_at
                        .is_none_or(|expires| expires <= chrono::Utc::now())
                {
                    return Err(refused());
                }
            }
        } else {
            drop(works);
        }
        Ok(())
    }

    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        if let Some(fence) = &self.attempt {
            crate::work_attempt_repository::check_attempt_fence(tx, fence, chrono::Utc::now())?;
        }
        for original in &self.grant_rows {
            if tx
                .get::<WorkItem>(&self.space, &original.base.id, false)?
                .as_ref()
                != Some(original)
            {
                return Err(refused());
            }
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        if let Some(fence) = &self.attempt {
            crate::work_attempt_repository::append_attempt_fence(sql, bindings, fence)?;
        }
        bindings.insert("space".into(), self.space.clone().into());
        for (index, original) in self.grant_rows.iter().enumerate() {
            let row = CanonicalRecord::WorkItem(Box::new(original.clone()));
            // Existing queue guards compare the original full row and touch its
            // commit nonce, conflicting with concurrent grant revocation/rotation.
            append_row_guard(
                sql,
                bindings,
                &format!("view_authority_{index}"),
                &row,
                Some(&row),
            )?;
        }
        Ok(())
    }
}
