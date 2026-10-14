//! Durable provenance for allocation of a trusted standalone proposal context.
//! Completion describes this allocation only, not a graph mutation or sweep.

use super::{WorkItem, WorkRun, WorkRunMetrics, WorkRunRepository};
use crate::{StorageClient, StorageError, StorageResult, WorkItemRepository};
use chrono::Utc;
use elephant_types::records::base::BaseFields;
use elephant_types::records::work::{WorkItemArchive, WorkItemStatus, WorkRunArchive};
use elephant_types::{Link, RecordId, TableName};

const KIND: &str = "trusted_proposal_context_creation";

fn conflict() -> StorageError {
    StorageError::Conflict("trusted_proposal_context_changed_or_incomplete".into())
}

fn same_envelope(left: &BaseFields, right: &BaseFields) -> bool {
    left.id == right.id
        && left.space_id == right.space_id
        && left.created_by == right.created_by
        && left.deleted_at.is_none()
        && left.security == right.security
        && left.subjects == right.subjects
        && left.subjects_state == right.subjects_state
        && left.provenance == right.provenance
}

fn matches_context(
    item: &WorkItem,
    run: &WorkRun,
    expected_item: &WorkItem,
    expected_run: &WorkRun,
) -> bool {
    same_envelope(&item.base, &expected_item.base)
        && same_envelope(&run.base, &expected_run.base)
        && item.kind == KIND
        && item.status == WorkItemStatus::Done
        && item.payload == expected_item.payload
        && item.idempotency_key == expected_item.idempotency_key
        && item.attempts == 1
        && item.leased_by.is_none()
        && item.lease_expires_at.is_none()
        && item.run_after.is_none()
        && item.last_error.is_none()
        && run.work_item == expected_run.work_item
        && run.attempt == 1
        && run.success
        && run.completed_at.is_some()
        && run.error.is_none()
        && run.worker_id == KIND
        && run.metrics == expected_run.metrics
        && run.model_version.is_none()
        && run.prompt_id.is_none()
        && run.config_hash == expected_run.config_hash
}

/// Allocate real linked records for trusted standalone Draft proposal provenance.
///
/// The caller supplies the authorized union envelope of its prepared proposals
/// and a stable preparation scope. Only a digest of that scope is stored. The
/// terminal run certifies context allocation; it conveys no lease, model usage,
/// source publication, canonical commit, or successful maintenance sweep.
///
/// # Errors
/// Rejects malformed references, changed replay envelopes, partial/deleted
/// contexts and storage failures. Both records are created in one transaction.
#[allow(
    clippy::too_many_lines,
    reason = "Keep the exact replay envelope and source checks with the atomic provenance-only work/run pair creation on both backends."
)]
pub async fn create_trusted_proposal_context(
    storage: &StorageClient,
    envelope: &BaseFields,
    scope: &str,
    sources: &[RecordId],
) -> StorageResult<RecordId> {
    if envelope.space_id.is_empty()
        || envelope.created_by.is_empty()
        || envelope.deleted_at.is_some()
        || scope.is_empty()
        || sources
            .iter()
            .any(|source| source.table() != TableName::Source)
        || envelope
            .provenance
            .sources
            .iter()
            .any(|source| source.table() != TableName::Source)
    {
        return Err(StorageError::DataIntegrityError(
            "Invalid trusted proposal context".into(),
        ));
    }
    let scope_digest = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, scope.as_bytes()).to_string();
    let identity = serde_json::to_vec(&(KIND, &envelope.space_id, &scope_digest))
        .map_err(|error| StorageError::SerializationError(error.to_string()))?;
    let uuid = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &identity);
    let item_id = RecordId::new(TableName::WorkItem, uuid);
    let run_id = RecordId::new(TableName::WorkRun, uuid);
    let mut source_ids = envelope.provenance.sources.clone();
    source_ids.extend_from_slice(sources);
    source_ids.sort();
    source_ids.dedup();
    let now = Utc::now();
    let mut base = envelope.clone();
    base.id = item_id.clone();
    base.created_at = now;
    base.updated_at = now;
    base.provenance.sources.clone_from(&source_ids);
    let item = WorkItem {
        base,
        kind: KIND.into(),
        payload: serde_json::json!({"provenance_only":true,"scope_digest":scope_digest,
            "source_ids":source_ids.iter().map(ToString::to_string).collect::<Vec<_>>()}),
        status: WorkItemStatus::Done,
        priority: 0,
        idempotency_key: format!("{KIND}:{scope_digest}"),
        attempts: 1,
        leased_by: None,
        lease_expires_at: None,
        run_after: None,
        last_error: None,
    };
    let mut base = item.base.clone();
    base.id = run_id.clone();
    let mut metrics = WorkRunMetrics::default();
    metrics.custom.insert("provenance_only".into(), true.into());
    metrics.custom.insert("execution_kind".into(), KIND.into());
    metrics
        .custom
        .insert("model_usage_status".into(), "not_started".into());
    let run = WorkRun {
        base,
        work_item: Link::new(item_id.clone()),
        attempt: 1,
        started_at: now,
        completed_at: Some(now),
        success: true,
        error: None,
        metrics,
        worker_id: KIND.into(),
        worker_version: env!("CARGO_PKG_VERSION").into(),
        model_version: None,
        prompt_id: None,
        config_hash: Some(scope_digest),
    };
    let item_archive_id = crate::work_retention::archive_id(&item_id);
    let run_archive_id = crate::work_retention::archive_id(&run_id);
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                for source in &source_ids {
                    if tx
                        .get::<elephant_types::records::Source>(&envelope.space_id, source, false)?
                        .is_none()
                    {
                        return Err(StorageError::Conflict(
                            "trusted_proposal_source_missing_or_foreign".into(),
                        ));
                    }
                }
                let old_item = match tx.get_by_id::<WorkItem>(&item_id, true)? {
                    Some(item) => Some(item),
                    None => tx
                        .get_by_id::<WorkItemArchive>(&item_archive_id, true)?
                        .map(|archive| {
                            if archive.base.deleted_at.is_some()
                                || archive.original_id != item_id
                                || archive.base.space_id != envelope.space_id
                            {
                                return Err(conflict());
                            }
                            Ok(archive.record)
                        })
                        .transpose()?,
                };
                let old_run = match tx.get_by_id::<WorkRun>(&run_id, true)? {
                    Some(run) => Some(run),
                    None => tx
                        .get_by_id::<WorkRunArchive>(&run_archive_id, true)?
                        .map(|archive| {
                            if archive.base.deleted_at.is_some()
                                || archive.original_id != run_id
                                || archive.base.space_id != envelope.space_id
                            {
                                return Err(conflict());
                            }
                            Ok(archive.record)
                        })
                        .transpose()?,
                };
                match (old_item, old_run) {
                    (None, None) => {
                        tx.create(item.clone())?;
                        tx.create(run.clone())?;
                        crate::work_producer_repository::initialize_direct_tx(tx, &item)?;
                        Ok(())
                    }
                    (Some(old_item), Some(old_run))
                        if matches_context(&old_item, &old_run, &item, &run) =>
                    {
                        Ok(())
                    }
                    _ => Err(conflict()),
                }
            })
            .await?;
    } else {
        let mut origin_sql = String::new();
        let mut origin_bindings = std::collections::BTreeMap::new();
        crate::work_producer_repository::append_direct_sql(
            &mut origin_sql,
            &mut origin_bindings,
            "new_context",
            &item,
        )?;
        let sql = "BEGIN TRANSACTION;
             FOR $context_source IN $context_sources {
                 LET $context_source_row = SELECT * FROM ONLY $context_source;
                 IF $context_source_row = NONE OR $context_source_row.space_id != $context_space OR $context_source_row.deleted_at != NONE { THROW 'trusted_proposal_source_missing_or_foreign'; };
             };
             LET $context_hot_item = SELECT * FROM ONLY $context_item_id;
             LET $context_cold_item = SELECT * FROM ONLY $context_item_archive;
             LET $context_hot_run = SELECT * FROM ONLY $context_run_id;
             LET $context_cold_run = SELECT * FROM ONLY $context_run_archive;
             IF $context_hot_item = NONE AND $context_cold_item != NONE AND ($context_cold_item.deleted_at != NONE OR $context_cold_item.original_id != $context_item_id OR $context_cold_item.space_id != $context_space) { THROW 'trusted_proposal_context_changed_or_incomplete'; };
             IF $context_hot_run = NONE AND $context_cold_run != NONE AND ($context_cold_run.deleted_at != NONE OR $context_cold_run.original_id != $context_run_id OR $context_cold_run.space_id != $context_space) { THROW 'trusted_proposal_context_changed_or_incomplete'; };
             LET $context_old_item = IF $context_hot_item != NONE THEN $context_hot_item ELSE $context_cold_item.record END;
             LET $context_old_run = IF $context_hot_run != NONE THEN $context_hot_run ELSE $context_cold_run.record END;
             IF $context_old_item = NONE AND $context_old_run = NONE {
                 CREATE $context_item_id CONTENT $context_item;
                 CREATE $context_run_id CONTENT $context_run;
                 __PRIVATE_ORIGIN_INSERT__
             } ELSE {
                 IF $context_old_item = NONE OR $context_old_run = NONE
                 OR $context_old_item.id != $context_item_id OR $context_old_run.id != $context_run_id
                 OR $context_old_item.space_id != $context_space OR $context_old_run.space_id != $context_space
                 OR $context_old_item.deleted_at != NONE OR $context_old_run.deleted_at != NONE
                 OR $context_old_item.created_by != $context_item.created_by OR $context_old_run.created_by != $context_run.created_by
                 OR $context_old_item.security != $context_item.security OR $context_old_run.security != $context_run.security
                 OR $context_old_item.subjects != $context_item.subjects OR $context_old_run.subjects != $context_run.subjects
                 OR $context_old_item.subjects_state != $context_item.subjects_state OR $context_old_run.subjects_state != $context_run.subjects_state
                 OR $context_old_item.provenance != $context_item.provenance OR $context_old_run.provenance != $context_run.provenance
                 OR $context_old_item.kind != $context_item.kind OR $context_old_item.status != 'done'
                 OR $context_old_item.payload != $context_item.payload OR $context_old_item.idempotency_key != $context_item.idempotency_key
                 OR $context_old_item.attempts != 1 OR $context_old_item.leased_by != NONE OR $context_old_item.lease_expires_at != NONE
                 OR $context_old_item.run_after != NONE OR $context_old_item.last_error != NONE
                 OR $context_old_run.work_item != $context_item_id OR $context_old_run.attempt != 1
                 OR $context_old_run.success != true OR $context_old_run.completed_at = NONE OR $context_old_run.error != NONE
                 OR $context_old_run.worker_id != $context_run.worker_id OR $context_old_run.metrics != $context_run.metrics
                 OR $context_old_run.model_version != NONE OR $context_old_run.prompt_id != NONE OR $context_old_run.config_hash != $context_run.config_hash
                 { THROW 'trusted_proposal_context_changed_or_incomplete'; };
             };
             COMMIT TRANSACTION;".replace("__PRIVATE_ORIGIN_INSERT__", &origin_sql);
        let mut bindings = crate::bindings! {
            "context_item_id" => crate::surreal::thing_from_record_id(&item_id),
            "context_run_id" => crate::surreal::thing_from_record_id(&run_id),
            "context_item_archive" => crate::surreal::thing_from_record_id(&item_archive_id),
            "context_run_archive" => crate::surreal::thing_from_record_id(&run_archive_id),
            "context_space" => envelope.space_id.clone(),
            "context_sources" => crate::surreal::things_from_record_ids(source_ids.iter().cloned()),
            "context_item" => crate::surreal::stored_native_value(WorkItemRepository::work_item_to_value(&item)?),
            "context_run" => crate::surreal::stored_native_value(WorkRunRepository::work_run_to_value(&run)?),
        };
        bindings.extend(origin_bindings);
        storage.query_bind_checked(&sql, bindings).await?;
    }
    Ok(run_id)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use crate::Repository;

    #[expect(
        clippy::too_many_lines,
        reason = "Keep this ordered conformance scenario and its failure assertions in one function."
    )]
    async fn contract(storage: StorageClient) {
        let mut envelope = BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "named-maintenance".into(),
            "maintenance".into(),
        );
        envelope.security.level = 3;
        let source_id = RecordId::new_v4(TableName::Source);
        let mut source_base = envelope.clone();
        source_base.id = source_id.clone();
        crate::SourceRepository::new(storage.clone())
            .create(elephant_types::records::Source::new(
                source_base,
                elephant_types::records::source::SourceType::Manual,
                "fixture".into(),
            ))
            .await
            .expect("physical source");
        let sources = vec![source_id.clone(), source_id];
        let run_id = create_trusted_proposal_context(
            &storage,
            &envelope,
            "prepared-epoch-and-digest",
            &sources,
        )
        .await
        .expect("allocate actual context");
        let run = WorkRunRepository::new(storage.clone())
            .get(&envelope.space_id, &run_id)
            .await
            .expect("get run")
            .expect("real run");
        let item = WorkItemRepository::new(storage.clone())
            .get(&envelope.space_id, run.work_item.id())
            .await
            .expect("get work")
            .expect("real work");
        assert_eq!(item.kind, KIND);
        assert_eq!(item.status, WorkItemStatus::Done);
        assert!(item.leased_by.is_none());
        assert!(item
            .payload
            .get(crate::work_attempt_repository::ATTEMPT_PAYLOAD_KEY)
            .is_none());
        assert!(run.success && run.completed_at.is_some());
        assert_eq!(run.metrics.custom["provenance_only"], true);
        assert!(!run.metrics.custom.contains_key("attempt_fence"));
        assert_eq!(run.base.security, envelope.security);
        assert_eq!(item.base.provenance.sources.len(), 1);
        assert!(!item
            .payload
            .to_string()
            .contains("prepared-epoch-and-digest"));
        assert_eq!(
            create_trusted_proposal_context(
                &storage,
                &envelope,
                "prepared-epoch-and-digest",
                &sources
            )
            .await
            .expect("exact replay"),
            run_id
        );
        let replay = WorkRunRepository::new(storage.clone())
            .get(&envelope.space_id, &run_id)
            .await
            .expect("get replay")
            .expect("same context");
        assert_eq!(
            run, replay,
            "replay cannot relabel allocation time or outcome"
        );
        let mut foreign = envelope.clone();
        foreign.space_id = "another-space".into();
        assert!(
            create_trusted_proposal_context(
                &storage,
                &foreign,
                "prepared-epoch-and-digest",
                &sources
            )
            .await
            .is_err(),
            "proposal source references cannot cross spaces"
        );
        let mut changed = envelope.clone();
        changed.security.level += 1;
        assert!(create_trusted_proposal_context(
            &storage,
            &changed,
            "prepared-epoch-and-digest",
            &sources
        )
        .await
        .is_err());
        WorkRunRepository::new(storage.clone())
            .delete(&envelope.space_id, &run_id)
            .await
            .expect("delete context run");
        assert!(
            create_trusted_proposal_context(
                &storage,
                &envelope,
                "prepared-epoch-and-digest",
                &sources
            )
            .await
            .is_err(),
            "deleted context cannot be resurrected by replay"
        );
        assert!(WorkItemRepository::new(storage)
            .get(&envelope.space_id, &item.base.id)
            .await
            .expect("other row")
            .is_some());
    }

    #[tokio::test]
    async fn embedded_trusted_proposal_context_is_real_exact_and_not_execution_authority() {
        contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }
    #[tokio::test]
    async fn surreal_trusted_proposal_context_is_real_exact_and_not_execution_authority() {
        contract(StorageClient::new_memory().await.expect("memory")).await;
    }
}
