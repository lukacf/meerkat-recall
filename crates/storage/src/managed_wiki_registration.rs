//! Durable wiki service grants stored as terminal work registrations.
//!
//! Registrations never enter the runnable queue. Each managed chain references
//! one immutable grant ID, so revoking it also fences stale in-flight schedulers.

use super::{WorkItemRepository, WorkItemStatus};
use crate::{Repository, StorageError, StorageResult};
use chrono::Utc;
use elephant_types::records::BaseFields;
use elephant_types::{RecordId, TableName};

const REGISTRATION_KIND: &str = "managed_wiki_registration";
const RECONCILIATION_KIND: &str = "reconcile_living_wiki";
const REVOKED: &str = "managed_wiki_grant_revoked";
const REGISTRATION_STATE_KIND: &str = "managed_wiki_registration_lock";

impl WorkItemRepository {
    /// Creates a new durable service grant after caller authorization.
    /// Terminal registrations cannot be claimed or retried by queue workers.
    pub async fn create_managed_wiki_grant(
        &self,
        space_id: &str,
        actor: &str,
    ) -> StorageResult<RecordId> {
        if space_id.trim().is_empty() {
            return Err(StorageError::DataIntegrityError(
                "Managed wiki grants require a nonempty space_id".to_string(),
            ));
        }
        let id = RecordId::new_v4(TableName::WorkItem);
        let registration = super::WorkItem {
            base: BaseFields::new(id.clone(), space_id.to_string(), actor.to_string()),
            kind: REGISTRATION_KIND.to_string(),
            payload: serde_json::json!({ "space_id": space_id }),
            status: WorkItemStatus::Done,
            priority: 0,
            idempotency_key: format!("{REGISTRATION_KIND}:{id}"),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|mutation| {
                    for mut item in mutation.scan::<super::WorkItem>()? {
                        if should_revoke(&item, space_id) {
                            item.status = WorkItemStatus::Cancelled;
                            item.last_error = Some(REVOKED.to_string());
                            item.base.updated_at = Utc::now();
                            let _ = mutation.replace(item)?;
                        }
                    }
                    let state = registration_state(space_id, Some(&id));
                    if mutation
                        .get::<super::WorkItem>(space_id, &state.base.id, true)?
                        .is_some()
                    {
                        let _ = mutation.replace(state)?;
                    } else {
                        let _ = mutation.create(state)?;
                    }
                    let _ = mutation.create(registration)?;
                    Ok(())
                })
                .await?;
        } else {
            self.client.query_bind_checked(
                "BEGIN TRANSACTION; \
                 UPSERT $lock_id CONTENT $lock_record; \
                 UPDATE work_item SET status = 'cancelled', last_error = $reason, updated_at = time::now() \
                   WHERE space_id = $space_id AND deleted_at IS NONE AND \
                   ((kind = $registration_kind AND status = 'done') OR \
                    (kind = $reconciliation_kind AND status IN ['runnable', 'pending'])); \
                 CREATE $id CONTENT $record; \
                 COMMIT TRANSACTION;",
                crate::bindings! {
                    "space_id" => space_id,
                    "registration_kind" => REGISTRATION_KIND,
                    "reconciliation_kind" => RECONCILIATION_KIND,
                    "reason" => REVOKED,
                    "id" => crate::surreal::thing_from_record_id(&id),
                    "record" => Self::work_item_to_value(&registration)?,
                    "lock_id" => crate::surreal::thing_from_record_id(&registration_state(space_id, None).base.id),
                    "lock_record" => Self::work_item_to_value(&registration_state(space_id, Some(&id)))?,
                },
            ).await?;
            self.cancel_noncurrent_managed_wiki_grants(space_id).await?;
        }
        Ok(id)
    }

    /// Checks a grant before executing an already queued reconciliation.
    pub async fn managed_wiki_grant_is_active(
        &self,
        space_id: &str,
        grant_id: &RecordId,
    ) -> StorageResult<bool> {
        if !self
            .get(space_id, &registration_state(space_id, None).base.id)
            .await?
            .as_ref()
            .is_some_and(|state| state_authorizes(state, grant_id))
        {
            return Ok(false);
        }
        Ok(self.get(space_id, grant_id).await?.is_some_and(|item| {
            item.kind == REGISTRATION_KIND && item.status == WorkItemStatus::Done
        }))
    }

    /// Capture the exact active registration lock and grant as commit read dependencies.
    ///
    /// Returns `[lock, grant]`. Callers must retain both original rows in the
    /// canonical transaction's read set; this read-only observation alone does
    /// not authorize a later write or survive revocation during planning.
    ///
    /// # Errors
    /// Returns a conflict when either row is missing, revoked or outside the
    /// requested space, and propagates storage read failures.
    pub async fn capture_managed_wiki_grant(
        &self,
        space_id: &str,
        grant_id: &RecordId,
    ) -> StorageResult<Vec<super::WorkItem>> {
        let refused = || StorageError::Conflict(REVOKED.to_string());
        if grant_id.table() != TableName::WorkItem {
            return Err(refused());
        }
        let state = self
            .get(space_id, &registration_state(space_id, None).base.id)
            .await?
            .filter(|state| {
                state.base.space_id == space_id
                    && state.base.deleted_at.is_none()
                    && state_authorizes(state, grant_id)
            })
            .ok_or_else(refused)?;
        let grant = self
            .get(space_id, grant_id)
            .await?
            .filter(|grant| {
                grant.base.id == *grant_id
                    && grant.base.space_id == space_id
                    && grant.base.deleted_at.is_none()
                    && grant.kind == REGISTRATION_KIND
                    && grant.status == WorkItemStatus::Done
            })
            .ok_or_else(refused)?;
        Ok(vec![state, grant])
    }

    /// Finds scheduling configuration only within the current active grant.
    /// A completed job from an older revoked generation cannot shadow it.
    pub async fn find_latest_managed_wiki_work(
        &self,
        space_id: &str,
    ) -> StorageResult<Option<super::WorkItem>> {
        let Some(state) = self
            .get(space_id, &registration_state(space_id, None).base.id)
            .await?
        else {
            return Ok(None);
        };
        let Some(grant_string) = state.payload["active_grant_id"].as_str() else {
            return Ok(None);
        };
        let grant_id = crate::parse_id(grant_string, TableName::WorkItem)
            .map_err(|error| StorageError::DataIntegrityError(error.to_string()))?;
        if !self
            .managed_wiki_grant_is_active(space_id, &grant_id)
            .await?
        {
            return Ok(None);
        }
        if let Some(store) = self.client.embedded_store() {
            let mut items = store.scan::<super::WorkItem>().await?;
            items.extend(
                store
                    .scan::<elephant_types::records::WorkItemArchive>()
                    .await?
                    .into_iter()
                    .map(|archive| archive.record),
            );
            items.retain(|item| {
                item.base.space_id == space_id
                    && item.base.deleted_at.is_none()
                    && item.kind == RECONCILIATION_KIND
                    && !matches!(
                        item.status,
                        WorkItemStatus::Cancelled | WorkItemStatus::Failed
                    )
                    && item.payload["authority"]["grant_id"].as_str() == Some(grant_string)
            });
            items.sort_by(|left, right| {
                right
                    .base
                    .updated_at
                    .cmp(&left.base.updated_at)
                    .then_with(|| right.base.id.cmp(&left.base.id))
            });
            return Ok(items.into_iter().next());
        }
        let mut response = self.client.query_bind(
            &format!("SELECT {} FROM array::concat((SELECT * FROM work_item WHERE space_id = $space_id AND kind = $kind AND payload.authority.grant_id = $grant_string AND status NOT IN ['cancelled', 'failed'] AND deleted_at IS NONE ORDER BY updated_at DESC, id DESC LIMIT 1), (SELECT * FROM work_item_archive WHERE space_id = $space_id AND record.kind = $kind AND record.payload.authority.grant_id = $grant_string AND record.status NOT IN ['cancelled', 'failed'] AND deleted_at IS NONE ORDER BY record.updated_at DESC, record.id DESC LIMIT 1).record) ORDER BY updated_at DESC, id DESC LIMIT 1", super::WORK_ITEM_PROJECTION),
            crate::bindings! {
                "space_id" => space_id, "kind" => RECONCILIATION_KIND, "grant_string" => grant_string.to_string(),
            },
        ).await?;
        let items: Vec<super::WorkItemDb> = response.take(0).map_err(|error| {
            StorageError::QueryError(format!("Failed to parse managed wiki work: {error}"))
        })?;
        items
            .into_iter()
            .next()
            .map(super::WorkItem::try_from)
            .transpose()
    }

    /// Checks the current grant while creating its successor or graph-change job.
    /// A committed-pointer recheck fences overlapping SurrealDB revocation.
    pub async fn create_with_managed_wiki_grant(
        &self,
        record: super::WorkItem,
        grant_id: &RecordId,
    ) -> StorageResult<super::WorkItem> {
        if record.kind != RECONCILIATION_KIND
            || record.payload["authority"]["grant_id"].as_str()
                != Some(grant_id.to_string().as_str())
            || record.payload["authority"]["space_id"].as_str()
                != Some(record.base.space_id.as_str())
        {
            return Err(StorageError::DataIntegrityError(
                "Managed wiki work must reference its registration and space".to_string(),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    if mutation
                        .get_by_id::<elephant_types::records::WorkItemArchive>(
                            &crate::work_retention::archive_id(&record.base.id),
                            true,
                        )?
                        .is_some()
                    {
                        return Err(StorageError::Conflict(
                            "work_identity_already_archived".into(),
                        ));
                    }
                    if !mutation
                        .get::<super::WorkItem>(
                            &record.base.space_id,
                            &registration_state(&record.base.space_id, None).base.id,
                            false,
                        )?
                        .as_ref()
                        .is_some_and(|state| state_authorizes(state, grant_id))
                    {
                        return Err(StorageError::Conflict(REVOKED.to_string()));
                    }
                    let Some(mut grant) =
                        mutation.get::<super::WorkItem>(&record.base.space_id, grant_id, false)?
                    else {
                        return Err(StorageError::Conflict(REVOKED.to_string()));
                    };
                    if grant.kind != REGISTRATION_KIND || grant.status != WorkItemStatus::Done {
                        return Err(StorageError::Conflict(REVOKED.to_string()));
                    }
                    grant.base.updated_at = Utc::now();
                    let _ = mutation.replace(grant)?;
                    mutation.create(record.clone())?;
                    crate::work_producer_repository::initialize_direct_tx(mutation, &record)?;
                    Ok(record)
                })
                .await;
        }

        let mut sql = "BEGIN TRANSACTION; \
                 IF (SELECT VALUE id FROM ONLY $archive) != NONE { THROW 'work_identity_already_archived'; }; \
                 LET $grant = SELECT VALUE id FROM work_item WHERE id = $grant_id \
                   AND space_id = $space_id AND kind = $registration_kind \
                   AND status = 'done' AND deleted_at IS NONE; \
                 LET $state = SELECT VALUE id FROM work_item WHERE id = $state_id \
                   AND space_id = $space_id AND kind = $state_kind AND status = 'done' \
                   AND payload.active_grant_id = $grant_string AND deleted_at IS NONE; \
                 IF array::len($grant) != 1 OR array::len($state) != 1 { THROW 'managed_wiki_grant_revoked'; }; \
                 UPDATE $grant_id SET updated_at = time::now(); \
                 CREATE $id CONTENT $record; ".to_string();
        let mut bindings = crate::bindings! {
            "grant_id" => crate::surreal::thing_from_record_id(grant_id),
            "space_id" => record.base.space_id.clone(),
            "registration_kind" => REGISTRATION_KIND,
            "state_id" => crate::surreal::thing_from_record_id(&registration_state(&record.base.space_id, None).base.id),
            "state_kind" => REGISTRATION_STATE_KIND,
            "grant_string" => grant_id.to_string(),
            "id" => crate::surreal::thing_from_record_id(&record.base.id),
            "archive" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(&record.base.id)),
            "record" => Self::work_item_to_value(&record)?,
        };
        crate::work_producer_repository::append_direct_sql(
            &mut sql,
            &mut bindings,
            "wiki_work",
            &record,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client.query_bind_checked(&sql, bindings).await?;
        // Range cancellation in a concurrent stop can miss a just-inserted
        // row. The committed registration pointer is the final authority.
        if !self
            .managed_wiki_grant_is_active(&record.base.space_id, grant_id)
            .await?
        {
            self.client.query_bind_checked(
                "UPDATE $id SET status = 'cancelled', last_error = $reason, updated_at = time::now() \
                 WHERE space_id = $space_id AND status IN ['runnable', 'pending'];",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&record.base.id),
                    "space_id" => record.base.space_id.clone(),
                    "reason" => REVOKED,
                },
            ).await?;
            return Err(StorageError::Conflict(REVOKED.to_string()));
        }
        Ok(record)
    }

    /// Cancels superseded queued jobs only within one still-active grant.
    /// A graph-change scheduler from an older generation cannot cancel a newer
    /// registration's jobs after an explicit restart.
    pub async fn cancel_queued_managed_wiki_except(
        &self,
        space_id: &str,
        grant_id: &RecordId,
        keep_id: &RecordId,
    ) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    if !mutation
                        .get::<super::WorkItem>(
                            space_id,
                            &registration_state(space_id, None).base.id,
                            false,
                        )?
                        .as_ref()
                        .is_some_and(|state| state_authorizes(state, grant_id))
                    {
                        return Ok(());
                    }
                    let Some(mut grant) =
                        mutation.get::<super::WorkItem>(space_id, grant_id, false)?
                    else {
                        return Ok(());
                    };
                    if grant.kind != REGISTRATION_KIND || grant.status != WorkItemStatus::Done {
                        return Ok(());
                    }
                    let Some(mut replacement) =
                        mutation.get::<super::WorkItem>(space_id, keep_id, false)?
                    else {
                        return Ok(());
                    };
                    if replacement.kind != RECONCILIATION_KIND
                        || replacement.payload["authority"]["grant_id"].as_str()
                            != Some(grant_id.to_string().as_str())
                        || !matches!(
                            replacement.status,
                            WorkItemStatus::Runnable | WorkItemStatus::Pending
                        )
                    {
                        return Ok(());
                    }
                    replacement.base.updated_at = Utc::now();
                    let _ = mutation.replace(replacement)?;
                    grant.base.updated_at = Utc::now();
                    let _ = mutation.replace(grant)?;
                    for mut item in mutation.scan::<super::WorkItem>()? {
                        if item.base.space_id == space_id
                            && item.base.deleted_at.is_none()
                            && item.kind == RECONCILIATION_KIND
                            && item.base.id != *keep_id
                            && item.payload["authority"]["grant_id"].as_str()
                                == Some(grant_id.to_string().as_str())
                            && matches!(
                                item.status,
                                WorkItemStatus::Runnable | WorkItemStatus::Pending
                            )
                        {
                            item.status = WorkItemStatus::Cancelled;
                            item.last_error = Some("superseded_by_graph_change".to_string());
                            item.base.updated_at = Utc::now();
                            let _ = mutation.replace(item)?;
                        }
                    }
                    Ok(())
                })
                .await;
        }
        self.client.query_bind_checked(
            "BEGIN TRANSACTION; \
             LET $grant = SELECT VALUE id FROM work_item WHERE id = $grant_id \
               AND space_id = $space_id AND kind = $registration_kind \
               AND status = 'done' AND deleted_at IS NONE; \
             LET $replacement = SELECT VALUE id FROM work_item WHERE id = $keep_id \
               AND space_id = $space_id AND kind = $reconciliation_kind \
               AND payload.authority.grant_id = $grant_string \
               AND status IN ['runnable', 'pending'] AND deleted_at IS NONE; \
             LET $state = SELECT VALUE id FROM work_item WHERE id = $state_id \
               AND space_id = $space_id AND kind = $state_kind AND status = 'done' \
               AND payload.active_grant_id = $grant_string AND deleted_at IS NONE; \
             IF array::len($grant) = 1 AND array::len($replacement) = 1 AND array::len($state) = 1 { \
               UPDATE $grant_id SET updated_at = time::now(); \
               UPDATE $keep_id SET updated_at = time::now(); \
               UPDATE work_item SET status = 'cancelled', last_error = 'superseded_by_graph_change', updated_at = time::now() \
                 WHERE space_id = $space_id AND kind = $reconciliation_kind \
                 AND payload.authority.grant_id = $grant_string AND id != $keep_id \
                 AND status IN ['runnable', 'pending'] AND deleted_at IS NONE; \
             }; COMMIT TRANSACTION;",
            crate::bindings! {
                "space_id" => space_id,
                "registration_kind" => REGISTRATION_KIND,
                "reconciliation_kind" => RECONCILIATION_KIND,
                "grant_id" => crate::surreal::thing_from_record_id(grant_id),
                "grant_string" => grant_id.to_string(),
                "state_id" => crate::surreal::thing_from_record_id(&registration_state(space_id, None).base.id),
                "state_kind" => REGISTRATION_STATE_KIND,
                "keep_id" => crate::surreal::thing_from_record_id(keep_id),
            },
        ).await?;
        Ok(())
    }

    /// Atomically revokes wiki service authority in one space, then cleans up
    /// queued work. Running jobs may finish but cannot create successors.
    pub async fn revoke_managed_wiki_grants(&self, space_id: &str) -> StorageResult<()> {
        if space_id.trim().is_empty() {
            return Err(StorageError::DataIntegrityError(
                "Managed wiki revocation requires a nonempty space_id".to_string(),
            ));
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|mutation| {
                    let state = registration_state(space_id, None);
                    if mutation
                        .get::<super::WorkItem>(space_id, &state.base.id, true)?
                        .is_some()
                    {
                        let _ = mutation.replace(state)?;
                    } else {
                        let _ = mutation.create(state)?;
                    }
                    for mut item in mutation.scan::<super::WorkItem>()? {
                        if should_revoke(&item, space_id) {
                            item.status = WorkItemStatus::Cancelled;
                            item.last_error = Some(REVOKED.to_string());
                            item.base.updated_at = Utc::now();
                            let _ = mutation.replace(item)?;
                        }
                    }
                    Ok(())
                })
                .await;
        }
        self.client
            .query_bind_checked(
                "BEGIN TRANSACTION; \
                 UPSERT $lock_id CONTENT $lock_record; \
                 UPDATE work_item SET status = 'cancelled', last_error = $reason, updated_at = time::now() \
                   WHERE space_id = $space_id AND kind = $registration_kind \
                   AND status = 'done' AND deleted_at IS NONE; \
                 UPDATE work_item SET status = 'cancelled', last_error = $reason, updated_at = time::now() \
                   WHERE space_id = $space_id AND kind = $reconciliation_kind \
                   AND status IN ['runnable', 'pending'] AND deleted_at IS NONE; \
                 COMMIT TRANSACTION;",
                crate::bindings! {
                    "space_id" => space_id,
                    "registration_kind" => REGISTRATION_KIND,
                    "reconciliation_kind" => RECONCILIATION_KIND,
                    "reason" => REVOKED,
                    "lock_id" => crate::surreal::thing_from_record_id(&registration_state(space_id, None).base.id),
                    "lock_record" => Self::work_item_to_value(&registration_state(space_id, None))?,
                },
            )
            .await?;
        self.cancel_noncurrent_managed_wiki_grants(space_id).await?;
        Ok(())
    }

    // Recheck from a new snapshot after publishing the authority pointer.
    // Capture only noncurrent grant IDs in that snapshot. Concurrent creation
    // publishes its grant and pointer together, so cannot be swept as stale.
    // Enqueues that commit after this sweep check the pointer and clean up
    // themselves; neither path writes or restores the authority pointer.
    async fn cancel_noncurrent_managed_wiki_grants(&self, space_id: &str) -> StorageResult<()> {
        self.client.query_bind_checked(
            "BEGIN TRANSACTION; \
             LET $active = (SELECT VALUE payload.active_grant_id FROM work_item WHERE id = $state_id \
               AND space_id = $space_id AND kind = $state_kind AND deleted_at IS NONE)[0]; \
             LET $old_grants = SELECT VALUE id FROM work_item WHERE space_id = $space_id \
               AND kind = $registration_kind AND deleted_at IS NONE AND string::concat(meta::tb(id), ':', meta::id(id)) != $active; \
             UPDATE work_item SET status = 'cancelled', last_error = $reason, updated_at = time::now() \
               WHERE id IN $old_grants AND status = 'done'; \
             LET $old_strings = array::map($old_grants, |$grant| string::concat(meta::tb($grant), ':', meta::id($grant))); \
             UPDATE work_item SET status = 'cancelled', last_error = $reason, updated_at = time::now() \
               WHERE space_id = $space_id AND kind = $reconciliation_kind \
               AND status IN ['runnable', 'pending'] AND deleted_at IS NONE \
               AND payload.authority.grant_id IN $old_strings; \
             COMMIT TRANSACTION;",
            crate::bindings! {
                "space_id" => space_id,
                "state_id" => crate::surreal::thing_from_record_id(&registration_state(space_id, None).base.id),
                "state_kind" => REGISTRATION_STATE_KIND,
                "reconciliation_kind" => RECONCILIATION_KIND,
                "registration_kind" => REGISTRATION_KIND,
                "reason" => REVOKED,
            },
        ).await?;
        Ok(())
    }
}

// This one per-space record owns the current generation. A scan of historical
// Done grants is insufficient: concurrent SurrealDB range updates can miss a
// grant inserted by another transaction. Only this pointer confers authority.
fn registration_state(space_id: &str, active_grant: Option<&RecordId>) -> super::WorkItem {
    let id = RecordId::new(
        TableName::WorkItem,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_URL,
            format!("elephant:managed-wiki-registration-lock:{space_id}").as_bytes(),
        ),
    );
    super::WorkItem {
        base: BaseFields::new(id.clone(), space_id.to_string(), "wiki_service".to_string()),
        kind: REGISTRATION_STATE_KIND.to_string(),
        payload: serde_json::json!({ "active_grant_id": active_grant }),
        status: WorkItemStatus::Done,
        priority: 0,
        idempotency_key: id.to_string(),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    }
}

fn state_authorizes(state: &super::WorkItem, grant_id: &RecordId) -> bool {
    state.kind == REGISTRATION_STATE_KIND
        && state.status == WorkItemStatus::Done
        && state.payload["active_grant_id"].as_str() == Some(grant_id.to_string().as_str())
}

fn should_revoke(item: &super::WorkItem, space_id: &str) -> bool {
    item.base.space_id == space_id
        && item.base.deleted_at.is_none()
        && ((item.kind == REGISTRATION_KIND && item.status == WorkItemStatus::Done)
            || (item.kind == RECONCILIATION_KIND
                && matches!(
                    item.status,
                    WorkItemStatus::Pending | WorkItemStatus::Runnable
                )))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::{StorageClient, StorageConfig};
    use elephant_types::records::WorkItem;

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn captured_grant_contract(storage: StorageClient) {
        let repo = WorkItemRepository::new(storage);
        let grant = repo
            .create_managed_wiki_grant("captured", "original manager")
            .await
            .expect("original grant");
        let original = repo
            .capture_managed_wiki_grant("captured", &grant)
            .await
            .expect("active original snapshots");
        assert_eq!(original.len(), 2);
        assert_eq!(original[0].kind, REGISTRATION_STATE_KIND);
        assert_eq!(original[1].base.id, grant);
        for row in &original {
            assert_eq!(
                repo.get("captured", &row.base.id)
                    .await
                    .expect("actual row"),
                Some(row.clone())
            );
        }
        assert!(repo
            .capture_managed_wiki_grant("other-space", &grant)
            .await
            .is_err());
        repo.revoke_managed_wiki_grants("captured")
            .await
            .expect("actual revocation");
        assert!(
            matches!(repo.capture_managed_wiki_grant("captured", &grant).await,
            Err(StorageError::Conflict(message)) if message == REVOKED)
        );
        for row in &original {
            assert_ne!(
                repo.get("captured", &row.base.id)
                    .await
                    .expect("revoked row"),
                Some(row.clone()),
                "both original commit observations must detect revocation"
            );
        }
        let replacement = repo
            .create_managed_wiki_grant("captured", "new manager")
            .await
            .expect("new explicit grant");
        assert_ne!(replacement, grant);
        assert!(repo
            .capture_managed_wiki_grant("captured", &grant)
            .await
            .is_err());
        let current = repo
            .capture_managed_wiki_grant("captured", &replacement)
            .await
            .expect("new original snapshots");
        assert_eq!(current[0].base.id, original[0].base.id);
        assert_ne!(current[0], original[0]);
        assert_eq!(current[1].base.id, replacement);
        assert_eq!(
            original[1].base.id, grant,
            "captured authority never adopts a successor"
        );
    }

    #[tokio::test]
    async fn embedded_capture_grant_preserves_original_revocation_dependencies() {
        captured_grant_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }

    #[tokio::test]
    async fn native_capture_grant_preserves_original_revocation_dependencies() {
        captured_grant_contract(StorageClient::new_memory().await.expect("native")).await;
    }

    fn work_item(space_id: &str, grant_id: &RecordId, status: WorkItemStatus) -> WorkItem {
        let id = RecordId::new_v4(TableName::WorkItem);
        WorkItem {
            base: BaseFields::new(id.clone(), space_id.to_string(), "test".to_string()),
            kind: RECONCILIATION_KIND.to_string(),
            payload: serde_json::json!({
                "authority": { "kind": "space", "space_id": space_id, "grant_id": grant_id }
            }),
            status,
            priority: 1,
            idempotency_key: id.to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn archived_identity_contract(storage: StorageClient) {
        let repo = WorkItemRepository::new(storage.clone());
        let grant = repo
            .create_managed_wiki_grant("space", "manager")
            .await
            .expect("actual original grant");
        let original = repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Done),
                &grant,
            )
            .await
            .expect("actual original managed work");
        let archived = crate::work_retention::archive_terminal_work(
            &storage,
            Utc::now() + chrono::Duration::seconds(1),
            16,
        )
        .await
        .expect("actual terminal archival");
        assert_eq!(archived.items_archived, 1);
        let original_authority = repo
            .capture_managed_wiki_grant("space", &grant)
            .await
            .expect("actual unarchived authority");
        let mut replay = original.clone();
        replay.status = WorkItemStatus::Runnable;
        assert!(
            repo.create_with_managed_wiki_grant(replay, &grant)
                .await
                .is_err(),
            "a same-intent proposal cannot create a second hot identity over its archive"
        );
        assert_eq!(
            repo.capture_managed_wiki_grant("space", &grant)
                .await
                .expect("original authority after refusal"),
            original_authority,
            "refused replay cannot partially mutate the grant"
        );
        assert_eq!(
            repo.get("space", &original.base.id)
                .await
                .expect("original history"),
            Some(original.clone())
        );
        let other = repo
            .create_managed_wiki_grant("other", "other manager")
            .await
            .expect("actual foreign-space grant");
        let mut foreign = work_item("other", &other, WorkItemStatus::Runnable);
        foreign.base.id = original.base.id.clone();
        assert!(
            repo.create_with_managed_wiki_grant(foreign, &other)
                .await
                .is_err(),
            "physical archive exclusion is global, not scoped to the new proposal"
        );
        if let Some(store) = storage.embedded_store() {
            assert!(store
                .get_by_id::<WorkItem>(&original.base.id, true)
                .await
                .expect("global hot identity")
                .is_none());
            let cold = store
                .get_by_id::<elephant_types::records::WorkItemArchive>(
                    &crate::work_retention::archive_id(&original.base.id),
                    true,
                )
                .await
                .expect("global original cold identity")
                .expect("retained archive");
            assert_eq!(cold.record, original);
        } else {
            let mut response = storage.query_bind_checked(
                "SELECT VALUE id FROM ONLY $hot; SELECT VALUE original_id FROM ONLY $cold;",
                crate::bindings! {
                    "hot"=>crate::surreal::thing_from_record_id(&original.base.id),
                    "cold"=>crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(&original.base.id)),
                },
            ).await.expect("actual physical identity pair");
            let hot: Option<surrealdb::sql::Thing> = response.take(0).expect("typed hot identity");
            let cold: Option<surrealdb::sql::Thing> =
                response.take(1).expect("typed cold identity");
            assert!(hot.is_none());
            assert_eq!(
                cold,
                Some(crate::surreal::thing_from_record_id(&original.base.id))
            );
        }
        let fresh = repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Runnable),
                &grant,
            )
            .await
            .expect("fresh actual successor still admitted");
        assert_ne!(fresh.base.id, original.base.id);
    }

    #[tokio::test]
    async fn embedded_managed_wiki_create_refuses_existing_global_archive_identity() {
        Box::pin(archived_identity_contract(
            StorageClient::new_embedded().await.expect("embedded"),
        ))
        .await;
    }

    #[tokio::test]
    async fn native_managed_wiki_create_refuses_existing_global_archive_identity() {
        Box::pin(archived_identity_contract(
            StorageClient::new_memory().await.expect("native"),
        ))
        .await;
    }

    #[allow(clippy::too_many_lines)]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn exercise_revocation(storage: StorageClient) {
        let repo = WorkItemRepository::new(storage);
        let grant = repo
            .create_managed_wiki_grant("space", "manager")
            .await
            .unwrap();
        let predecessor = repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Done),
                &grant,
            )
            .await
            .unwrap();
        let successor = repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Runnable),
                &grant,
            )
            .await
            .unwrap();
        let running = repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Running),
                &grant,
            )
            .await
            .unwrap();
        let other = repo
            .create_managed_wiki_grant("other", "manager")
            .await
            .unwrap();
        assert!(repo
            .create_with_managed_wiki_grant(
                work_item("other", &grant, WorkItemStatus::Runnable),
                &grant
            )
            .await
            .is_err());

        repo.cancel_queued_managed_wiki_except("space", &grant, &predecessor.base.id)
            .await
            .unwrap();
        assert_eq!(
            repo.get("space", &successor.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Runnable
        );
        repo.revoke_managed_wiki_grants("space").await.unwrap();
        assert!(repo
            .find_latest_managed_wiki_work("space")
            .await
            .unwrap()
            .is_none());
        assert!(!repo
            .managed_wiki_grant_is_active("space", &grant)
            .await
            .unwrap());
        assert!(repo
            .managed_wiki_grant_is_active("other", &other)
            .await
            .unwrap());
        assert_eq!(
            repo.get("space", &successor.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Cancelled
        );
        assert_eq!(
            repo.get("space", &running.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Running
        );
        assert_eq!(
            repo.get("space", &predecessor.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Done
        );
        // Neither a completed ancestor nor the running job can restart a revoked chain.
        assert!(repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Runnable),
                &grant
            )
            .await
            .is_err());
        let new_grant = repo
            .create_managed_wiki_grant("space", "manager")
            .await
            .unwrap();
        let new_work = repo
            .create_with_managed_wiki_grant(
                work_item("space", &new_grant, WorkItemStatus::Runnable),
                &new_grant,
            )
            .await
            .unwrap();
        let mut completed_old = predecessor.clone();
        completed_old.base.updated_at = Utc::now() + chrono::Duration::hours(1);
        repo.update("space", &predecessor.base.id, completed_old)
            .await
            .unwrap();
        assert_eq!(
            repo.find_latest_managed_wiki_work("space")
                .await
                .unwrap()
                .unwrap()
                .base
                .id,
            new_work.base.id
        );
        repo.cancel_queued_managed_wiki_except("space", &grant, &predecessor.base.id)
            .await
            .unwrap();
        assert_eq!(
            repo.get("space", &new_work.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Runnable
        );
        let new_replacement = repo
            .create_with_managed_wiki_grant(
                work_item("space", &new_grant, WorkItemStatus::Runnable),
                &new_grant,
            )
            .await
            .unwrap();
        repo.cancel_queued_managed_wiki_except("space", &new_grant, &new_replacement.base.id)
            .await
            .unwrap();
        assert_eq!(
            repo.get("space", &new_work.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Cancelled
        );
        assert_eq!(
            repo.get("space", &new_replacement.base.id)
                .await
                .unwrap()
                .unwrap()
                .status,
            WorkItemStatus::Runnable
        );
        assert!(repo
            .create_with_managed_wiki_grant(
                work_item("space", &grant, WorkItemStatus::Runnable),
                &grant
            )
            .await
            .is_err());
        let replacement_grant = repo
            .create_managed_wiki_grant("space", "manager")
            .await
            .unwrap();
        assert!(!repo
            .managed_wiki_grant_is_active("space", &new_grant)
            .await
            .unwrap());
        assert!(repo
            .managed_wiki_grant_is_active("space", &replacement_grant)
            .await
            .unwrap());
        repo.revoke_managed_wiki_grants("space").await.unwrap();
        repo.revoke_managed_wiki_grants("space").await.unwrap();
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn managed_wiki_grant_revocation_racing_enqueue_leaves_no_runnable_work() {
        let repo = WorkItemRepository::new(StorageClient::new_embedded().await.unwrap());
        for _ in 0..8 {
            let grant = repo
                .create_managed_wiki_grant("space", "manager")
                .await
                .unwrap();
            let pending = work_item("space", &grant, WorkItemStatus::Runnable);
            let pending_id = pending.base.id.clone();
            let (enqueue, revoke) = tokio::join!(
                repo.create_with_managed_wiki_grant(pending, &grant),
                repo.revoke_managed_wiki_grants("space")
            );
            revoke.unwrap();
            assert!(!repo
                .managed_wiki_grant_is_active("space", &grant)
                .await
                .unwrap());
            if enqueue.is_ok() {
                assert_eq!(
                    repo.get("space", &pending_id)
                        .await
                        .unwrap()
                        .unwrap()
                        .status,
                    WorkItemStatus::Cancelled
                );
            }
        }
    }

    #[tokio::test]
    async fn managed_wiki_grant_revocation_fences_embedded_schedulers() {
        exercise_revocation(StorageClient::new_embedded().await.unwrap()).await;
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep storage, repo in the original fixture scope through assertions and cleanup."
    )]
    async fn managed_wiki_grant_concurrent_registrations_have_one_active_generation() {
        let storage = StorageClient::new(StorageConfig {
            url: "mem://".to_string(),
            database: format!("wiki_grant_race_{}", uuid::Uuid::new_v4()),
            ..StorageConfig::default()
        })
        .await
        .unwrap();
        let repo = WorkItemRepository::new(storage);
        // Use a fresh space each round to exercise two concurrent first grants,
        // not just replacement of an already initialized registration pointer.
        for round in 0..128 {
            let space = format!("space-{round}");
            let grants = tokio::join!(
                repo.create_managed_wiki_grant(&space, "first"),
                repo.create_managed_wiki_grant(&space, "second")
            );
            let mut active = 0;
            for grant in <[_; 2]>::from(grants).into_iter().flatten() {
                let granted = repo
                    .managed_wiki_grant_is_active(&space, &grant)
                    .await
                    .unwrap();
                active += usize::from(granted);
                let enqueue = repo
                    .create_with_managed_wiki_grant(
                        work_item(&space, &grant, WorkItemStatus::Runnable),
                        &grant,
                    )
                    .await;
                assert_eq!(enqueue.is_ok(), granted, "round {round}");
            }
            assert_eq!(active, 1, "round {round}");
            repo.revoke_managed_wiki_grants(&space).await.unwrap();
            assert!(repo
                .find_latest_managed_wiki_work(&space)
                .await
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_in_scrutinee,
        reason = "Construct both backend fixtures before the loop and retain their original teardown order."
    )]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn managed_wiki_grant_done_history_does_not_confer_authority() {
        for storage in [
            StorageClient::new_embedded().await.unwrap(),
            StorageClient::new(StorageConfig {
                database: format!("wiki_grant_stale_{}", uuid::Uuid::new_v4()),
                ..StorageConfig::default()
            })
            .await
            .unwrap(),
        ] {
            let repo = WorkItemRepository::new(storage);
            let old_grant = repo
                .create_managed_wiki_grant("space", "old")
                .await
                .unwrap();
            let new_grant = repo
                .create_managed_wiki_grant("space", "new")
                .await
                .unwrap();
            let mut historical = repo.get("space", &old_grant).await.unwrap().unwrap();
            historical.status = WorkItemStatus::Done;
            repo.update("space", &old_grant, historical).await.unwrap();
            assert!(!repo
                .managed_wiki_grant_is_active("space", &old_grant)
                .await
                .unwrap());
            assert!(repo
                .managed_wiki_grant_is_active("space", &new_grant)
                .await
                .unwrap());
            assert!(repo
                .create_with_managed_wiki_grant(
                    work_item("space", &old_grant, WorkItemStatus::Runnable),
                    &old_grant,
                )
                .await
                .is_err());
            repo.revoke_managed_wiki_grants("space").await.unwrap();
            let mut historical = repo.get("space", &new_grant).await.unwrap().unwrap();
            historical.status = WorkItemStatus::Done;
            repo.update("space", &new_grant, historical).await.unwrap();
            assert!(!repo
                .managed_wiki_grant_is_active("space", &new_grant)
                .await
                .unwrap());
            assert!(repo
                .find_latest_managed_wiki_work("space")
                .await
                .unwrap()
                .is_none());
        }
    }

    #[tokio::test]
    async fn managed_wiki_grant_surreal_revoke_enqueue_race_cannot_restore_authority() {
        let repo = WorkItemRepository::new(
            StorageClient::new(StorageConfig {
                database: format!("wiki_grant_revoke_race_{}", uuid::Uuid::new_v4()),
                ..StorageConfig::default()
            })
            .await
            .unwrap(),
        );
        for round in 0..32 {
            let space = format!("space-{round}");
            let grant = repo
                .create_managed_wiki_grant(&space, "manager")
                .await
                .unwrap();
            let pending = work_item(&space, &grant, WorkItemStatus::Runnable);
            let pending_id = pending.base.id.clone();
            let (_, stopped) = tokio::join!(
                repo.create_with_managed_wiki_grant(pending, &grant),
                repo.revoke_managed_wiki_grants(&space),
            );
            stopped.unwrap();
            assert!(!repo
                .managed_wiki_grant_is_active(&space, &grant)
                .await
                .unwrap());
            assert!(repo
                .find_latest_managed_wiki_work(&space)
                .await
                .unwrap()
                .is_none());
            assert!(repo
                .create_with_managed_wiki_grant(
                    work_item(&space, &grant, WorkItemStatus::Runnable),
                    &grant,
                )
                .await
                .is_err());
            if let Some(item) = repo.get(&space, &pending_id).await.unwrap() {
                assert_eq!(item.status, WorkItemStatus::Cancelled);
            }
        }
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep repo in the original fixture scope through assertions and cleanup."
    )]
    async fn managed_wiki_grant_surreal_restart_cleans_old_enqueue_race() {
        let repo = WorkItemRepository::new(
            StorageClient::new(StorageConfig {
                database: format!("wiki_grant_restart_race_{}", uuid::Uuid::new_v4()),
                ..StorageConfig::default()
            })
            .await
            .unwrap(),
        );
        for round in 0..32 {
            let space = format!("space-{round}");
            let grant = repo
                .create_managed_wiki_grant(&space, "manager")
                .await
                .unwrap();
            let pending = work_item(&space, &grant, WorkItemStatus::Runnable);
            let pending_id = pending.base.id.clone();
            let (_, replacement) = tokio::join!(
                repo.create_with_managed_wiki_grant(pending, &grant),
                repo.create_managed_wiki_grant(&space, "replacement"),
            );
            let replacement = replacement.unwrap();
            assert!(!repo
                .managed_wiki_grant_is_active(&space, &grant)
                .await
                .unwrap());
            assert!(repo
                .managed_wiki_grant_is_active(&space, &replacement)
                .await
                .unwrap());
            if let Some(item) = repo.get(&space, &pending_id).await.unwrap() {
                assert_eq!(item.status, WorkItemStatus::Cancelled);
            }
            assert!(repo
                .create_with_managed_wiki_grant(
                    work_item(&space, &replacement, WorkItemStatus::Runnable),
                    &replacement,
                )
                .await
                .is_ok());
        }
    }

    #[tokio::test]
    async fn managed_wiki_grant_revocation_fences_surreal_schedulers() {
        let config = StorageConfig {
            url: "mem://".to_string(),
            namespace: "test".to_string(),
            database: format!("wiki_grants_{}", uuid::Uuid::new_v4()),
            ..StorageConfig::default()
        };
        exercise_revocation(StorageClient::new(config).await.unwrap()).await;
    }
}
