//! Atomic scheduling/reopening of one original recurring Watch slot. This is a
//! child of WorkItemRepository so no other owner writes private queue origins.
use super::{RecordId, StorageError, StorageResult, Utc, WorkItem, WorkItemRepository};
use crate::source_watch_registration::PreparedSourceWatchRegistration;
use crate::standing_watch_repository::{
    PreparedStandingWatchAuthority, StandingWatchRepository, PAYLOAD_KEY,
};
use elephant_auth::standing_watch::StandingWatchAdmission;
use elephant_types::records::{WorkItemArchive, WorkItemStatus};
use elephant_types::TableName;
use surrealdb::sql::Value;

impl WorkItemRepository {
    /// Enqueue one durable Watch scheduling slot under its original delegation.
    ///
    /// `invoker` is present for an independently authenticated manual check; its
    /// current admission/grant is checked in addition to the standing owner.
    /// `None` exercises an existing delegation and never creates local trust.
    /// # Errors
    /// Refuses changed Source/Doc/Watch/config/grant, replaced delegation, changed
    /// original work identity, or a concurrent claim/reactivation.
    #[allow(
        clippy::too_many_lines,
        reason = "Watch refresh compares watch authority and publication state before the idempotent enqueue on either backend; retain the complete guarded transaction branches."
    )]
    pub async fn enqueue_source_watch_refresh(
        &self,
        records: &PreparedSourceWatchRegistration,
        authority: &PreparedStandingWatchAuthority,
        invoker: Option<&StandingWatchAdmission>,
        priority: i32,
    ) -> StorageResult<WorkItem> {
        let watch = records.watch();
        if authority.watch() != watch || records.previous() != Some(watch) {
            return Err(StorageError::Conflict(
                "standing_watch_queue_records_changed".into(),
            ));
        }
        records.authorize(authority.principal())?;
        let caller = if let Some(admission) = invoker {
            records.authorize(admission.principal())?;
            Some(
                StandingWatchRepository::new(self.client.clone())
                    .prepare_registration(watch, admission)
                    .await?,
            )
        } else {
            None
        };
        let semantic =
            crate::source_watch_producer::watch_intent_fingerprint(watch, &watch.base.created_by)?;
        let slot = watch.next_check_at.to_rfc3339();
        let identity = authority.identity();
        let key = format!(
            "refresh_source_watch:{}:{semantic}:{}:{}:{slot}",
            watch.base.id, identity.id, identity.digest
        );
        let existing = self
            .find_by_idempotency_key(&watch.base.space_id, &key)
            .await?;
        let mut base = watch.base.clone();
        base.id = RecordId::new(
            TableName::WorkItem,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("{}|{key}", watch.base.space_id).as_bytes(),
            ),
        );
        base.created_at = Utc::now();
        base.updated_at = base.created_at;
        base.deleted_at = None;
        let proposal = WorkItem {
            base,
            kind: "refresh_source_watch".into(),
            payload: serde_json::json!({
                "watch_id":watch.base.id.to_string(),"watch_intent_fingerprint":semantic,
                "watch_scheduling_slot":slot,(PAYLOAD_KEY):identity,
            }),
            status: WorkItemStatus::Runnable,
            priority,
            idempotency_key: key,
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let mut next = existing.clone().unwrap_or_else(|| proposal.clone());
        if let Some(old) = &existing {
            if crate::work_producer_repository::work_intent_fingerprint(old)?
                != crate::work_producer_repository::work_intent_fingerprint(&proposal)?
            {
                return Err(StorageError::Conflict(
                    "standing_watch_queue_intent_changed".into(),
                ));
            }
            if matches!(
                old.status,
                WorkItemStatus::Done | WorkItemStatus::Failed | WorkItemStatus::Cancelled
            ) {
                next.base.updated_at = Utc::now();
                next.status = WorkItemStatus::Runnable;
                next.priority = priority;
                next.attempts = 0;
                next.lease_expires_at = None;
                next.leased_by = None;
                next.run_after = None;
                next.last_error = None;
            }
        }
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    records.check_tx(tx)?;
                    authority.check_tx(tx)?;
                    if let Some(caller) = &caller {
                        caller.check_tx(tx)?;
                    }
                    let hot = tx.get::<WorkItem>(&watch.base.space_id, &next.base.id, false)?;
                    let cold = tx.get::<WorkItemArchive>(
                        &watch.base.space_id,
                        &crate::work_retention::archive_id(&next.base.id),
                        false,
                    )?;
                    if hot
                        .as_ref()
                        .or_else(|| cold.as_ref().map(|row| &row.record))
                        != existing.as_ref()
                    {
                        return Err(StorageError::Conflict(
                            "standing_watch_queue_changed".into(),
                        ));
                    }
                    if hot.is_some() && cold.is_some() {
                        return Err(StorageError::Conflict(
                            "standing_watch_queue_ambiguous_history".into(),
                        ));
                    }
                    if let Some(old) = &existing {
                        crate::work_producer_repository::check_existing_work_origin_tx(tx, old)?;
                    }
                    if existing.as_ref() == Some(&next) {
                        return Ok(next.clone());
                    }
                    if hot.is_some() {
                        tx.replace(next.clone())?;
                    } else {
                        tx.create(next.clone())?;
                    }
                    if cold.is_some() {
                        tx.hard_delete::<WorkItemArchive>(
                            &watch.base.space_id,
                            &crate::work_retention::archive_id(&next.base.id),
                        )?;
                    }
                    if existing.is_none() {
                        crate::work_producer_repository::initialize_direct_tx(tx, &next)?;
                    }
                    Ok(next.clone())
                })
                .await;
        }
        let mut sql = "BEGIN TRANSACTION;\n".to_owned();
        let mut bindings = crate::bindings! {"space"=>watch.base.space_id.as_str(),
        "id"=>crate::surreal::thing_from_record_id(&next.base.id),
        "archive"=>crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(&next.base.id)),
        "record"=>Self::work_item_to_value(&next)?,
        "standing_queue_old"=>existing.as_ref().map(Self::work_item_to_value).transpose()?.map_or(Value::None,crate::canonical_commit::native_expected_fields),};
        records.append_guard_sql(&mut sql, &mut bindings)?;
        authority.append_guard_sql(&mut sql, &mut bindings)?;
        if let Some(caller) = &caller {
            let mut guard = String::new();
            let mut values = std::collections::BTreeMap::new();
            caller.append_guard_sql(&mut guard, &mut values)?;
            crate::standing_watch_repository::append_caller_guard(
                &mut sql,
                &mut bindings,
                &guard,
                values,
            );
        }
        if let Some(old) = &existing {
            crate::work_producer_repository::append_existing_work_origin_guard(
                &self.client,
                &mut sql,
                &mut bindings,
                "standing_queue_prior",
                old,
            )
            .await?;
        }
        sql.push_str("LET $standing_queue_hot=SELECT * FROM ONLY $id; LET $standing_queue_cold=SELECT * FROM ONLY $archive;
            IF $standing_queue_hot!=NONE AND $standing_queue_cold!=NONE { THROW 'standing_watch_queue_ambiguous_history'; };
            LET $standing_queue_actual=IF $standing_queue_hot!=NONE THEN $standing_queue_hot ELSE $standing_queue_cold.record END;
            IF $standing_queue_old=NONE { IF $standing_queue_actual!=NONE { THROW 'standing_watch_queue_changed'; }; }
            ELSE { IF $standing_queue_actual=NONE OR $standing_queue_actual.space_id!=$space { THROW 'standing_watch_queue_changed'; };
                FOR $field IN object::entries($standing_queue_old) { IF $standing_queue_actual[$field[0]]!=$field[1] { THROW 'standing_watch_queue_changed'; }; };
            };\n");
        if existing.as_ref() != Some(&next) {
            sql.push_str("UPSERT $id CONTENT $record;\n");
            if existing.is_some() {
                sql.push_str(crate::work_retention::RETIRE_ARCHIVE_SQL);
            } else {
                crate::work_producer_repository::append_direct_sql(
                    &mut sql,
                    &mut bindings,
                    "standing_queue_new",
                    &next,
                )?;
            }
        }
        sql.push_str("COMMIT TRANSACTION;");
        self.client.query_bind_checked(&sql, bindings).await?;
        Ok(next)
    }
}
