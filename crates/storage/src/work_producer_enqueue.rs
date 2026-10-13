//! Atomic child creation, original-attempt authority and private producer evidence.
use super::{
    append_insert, append_origin_guard, changed, insert_tx, invalid, origin_id, read_tx,
    source_support_fingerprint, verify_parent_origin, verify_reprocess_execution_parent,
    verify_reprocess_origin, work_intent_fingerprint, BTreeMap, CanonicalRecord, ConflictGroup,
    Deserialize, OriginDb, OriginEnvelope, RecordId, Repository, ReprocessExecutionOrigin,
    StorageError, StorageResult, TableName, Thing, Value, WorkItem, WorkProducerOrigin,
    WorkProducerRepository, WorkRun,
};
use crate::source_watch_producer::PreparedSourceWatchProducer;
use crate::work_attempt_repository::{append_attempt_fence, check_attempt_fence, fence_from_claim};
use crate::work_item_repository::{WorkItemDb, WORK_ITEM_PROJECTION};
use crate::work_run_repository::{WorkRunDb, WORK_RUN_PROJECTION};
use elephant_types::publication::PublicationFence;
use elephant_types::records::Source;
use elephant_types::records::{WorkItemArchive, WorkItemStatus};
use std::fmt::Write as _;

#[path = "work_producer_observation.rs"]
mod observation;
use observation::NativeLineageObservation;

fn same_envelope(
    left: &elephant_types::records::BaseFields,
    right: &elephant_types::records::BaseFields,
) -> bool {
    left.space_id == right.space_id
        && left.created_by == right.created_by
        && left.security == right.security
        && left.subjects == right.subjects
        && left.subjects_state == right.subjects_state
        && left.provenance == right.provenance
}

// Generate guards through the existing canonical predicate. Only consecutive
// identical templates/keys share a loop; original order and multiplicity stay
// intact, including every hot/archive field and conflict nonce write.
fn append_lineage_row_guards(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    rows: &[CanonicalRecord],
) -> StorageResult<()> {
    struct Group {
        template: String,
        names: Vec<String>,
        values: Vec<Value>,
    }
    let mut groups: Vec<Group> = Vec::new();
    for row in rows {
        let mut template = String::new();
        let mut descriptor = BTreeMap::new();
        crate::canonical_commit::append_row_guard(
            &mut template,
            &mut descriptor,
            "producer_row",
            row,
            Some(row),
        )?;
        let names = descriptor.keys().cloned().collect::<Vec<_>>();
        let value = Value::Object(surrealdb::sql::Object::from(descriptor));
        if let Some(group) = groups.last_mut() {
            if group.template == template && group.names == names {
                group.values.push(value);
                continue;
            }
        }
        groups.push(Group {
            template,
            names,
            values: vec![value],
        });
    }
    for (index, group) in groups.into_iter().enumerate() {
        let name = format!("producer_guard_rows_{index}");
        bindings.insert(
            name.clone(),
            Value::Array(surrealdb::sql::Array::from(group.values)),
        );
        let _ = writeln!(sql, "FOR $producer_guard_row IN ${name} {{");
        for key in group.names {
            let _ = writeln!(sql, "LET ${key} = $producer_guard_row.{key};");
        }
        sql.push_str(&group.template);
        sql.push_str("};\n");
    }
    Ok(())
}

// Keep every original private-control guard, but parse its SQL only once per
// admission. The existing guard remains the sole predicate and nonce writer.
fn append_origin_guards(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    origins: &[(WorkItem, OriginEnvelope)],
) -> StorageResult<()> {
    if origins.is_empty() {
        return Ok(());
    }
    let mut template = String::new();
    let mut names = Vec::new();
    let mut values = Vec::with_capacity(origins.len());
    for (_, origin) in origins {
        let mut current = String::new();
        let mut descriptor = BTreeMap::new();
        append_origin_guard(&mut current, &mut descriptor, "producer_original", origin)?;
        let keys = descriptor.keys().cloned().collect::<Vec<_>>();
        if values.is_empty() {
            template = current;
            names = keys;
        } else if current != template || keys != names {
            return Err(invalid("Private producer guard template changed"));
        }
        values.push(Value::Object(surrealdb::sql::Object::from(descriptor)));
    }
    bindings.insert(
        "producer_original_controls".into(),
        Value::Array(surrealdb::sql::Array::from(values)),
    );
    sql.push_str("FOR $producer_control IN $producer_original_controls {\n");
    for name in names {
        let _ = writeln!(sql, "LET ${name} = $producer_control.{name};");
    }
    sql.push_str(&template);
    sql.push_str("};\n");
    Ok(())
}

impl WorkProducerRepository {
    // Read exactly the next original edge. This is an observation, never a
    // cache: the complete rows and envelope are still guarded at child commit.
    async fn lineage_parent(
        &self,
        space: &str,
        work_id: &RecordId,
        run_id: &RecordId,
    ) -> StorageResult<(WorkItem, WorkRun, OriginEnvelope)> {
        if work_id.table() != TableName::WorkItem || run_id.table() != TableName::WorkRun {
            return Err(changed());
        }
        if self.client.is_embedded() {
            let work = crate::WorkItemRepository::new(self.client.clone())
                .get(space, work_id)
                .await?
                .ok_or_else(changed)?;
            let run = crate::WorkRunRepository::new(self.client.clone())
                .get(space, run_id)
                .await?
                .ok_or_else(changed)?;
            let origin = self.read_envelope(&work).await?.ok_or_else(changed)?;
            return Ok((work, run, origin));
        }

        // The retention projections and hot-then-cold, space and deletion
        // predicates match get_item/get_run. Original identities bound as
        // record targets avoid scanning unrelated queue history.
        let query = format!(
            "SELECT {WORK_ITEM_PROJECTION} FROM array::concat(\
                (SELECT * FROM $lineage_work WHERE space_id=$space AND deleted_at IS NONE),\
                (SELECT VALUE record FROM $lineage_work_archive WHERE space_id=$space AND deleted_at IS NONE)) LIMIT 1;\n\
             SELECT {WORK_RUN_PROJECTION} FROM array::concat(\
                (SELECT * FROM $lineage_run WHERE space_id=$space AND deleted_at IS NONE),\
                (SELECT VALUE record FROM $lineage_run_archive WHERE space_id=$space AND deleted_at IS NONE)) LIMIT 1;\n\
             SELECT payload FROM $lineage_origin WHERE space_id=$space AND work_item_id=$lineage_work;"
        );
        let mut response = self
            .client
            .query_bind_checked(
                &query,
                crate::bindings! {
                    "space" => space,
                    "lineage_work" => crate::surreal::thing_from_record_id(work_id),
                    "lineage_work_archive" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(work_id)),
                    "lineage_run" => crate::surreal::thing_from_record_id(run_id),
                    "lineage_run_archive" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(run_id)),
                    "lineage_origin" => origin_id(work_id),
                },
            )
            .await?;
        let work: Vec<WorkItemDb> = response.take(0).map_err(invalid)?;
        let run: Vec<WorkRunDb> = response.take(1).map_err(invalid)?;
        let origin: Vec<OriginDb> = response.take(2).map_err(invalid)?;
        let work = WorkItem::try_from(work.into_iter().next().ok_or_else(changed)?)?;
        let run = WorkRun::try_from(run.into_iter().next().ok_or_else(changed)?)?;
        let origin: OriginEnvelope =
            serde_json::from_str(&origin.into_iter().next().ok_or_else(changed)?.payload)
                .map_err(invalid)?;
        origin.verify(&work)?;
        Ok((work, run, origin))
    }

    /// Create a TM child using the actual conflict group's complete public envelope.
    /// Parent/job identifiers remain in private producer storage, never child payload.
    ///
    /// # Errors
    /// Rejects stale attempts, changed group/run/job/origin evidence or changed replay.
    pub async fn enqueue_group_child(
        &self,
        parent: &WorkItem,
        run: &WorkRun,
        group: &ConflictGroup,
        child: WorkItem,
        wait: bool,
    ) -> StorageResult<WorkItem> {
        if group.base.deleted_at.is_some()
            || group.base.space_id != parent.base.space_id
            || !same_envelope(&child.base, &group.base)
            || child
                .payload
                .get("conflict_group_id")
                .and_then(serde_json::Value::as_str)
                != Some(group.base.id.to_string().as_str())
        {
            return Err(changed());
        }
        self.enqueue_child(parent, run, child, wait, Some(group), None, None)
            .await
    }

    /// Create the next private child under the parent's unchanged public envelope.
    ///
    /// # Errors
    /// Rejects altered public authority or unavailable original producer evidence.
    pub async fn enqueue_successor(
        &self,
        parent: &WorkItem,
        run: &WorkRun,
        child: WorkItem,
        wait: bool,
    ) -> StorageResult<WorkItem> {
        if !same_envelope(&child.base, &parent.base) {
            return Err(changed());
        }
        self.enqueue_child(parent, run, child, wait, None, None, None)
            .await
    }

    /// Bind accepted metadata work to its original triage execution, retaining
    /// only that execution's exact source publication and configuration.
    ///
    /// # Errors
    /// Rejects substituted context, public authority or original attempt proof.
    pub async fn enqueue_source_metadata(
        &self,
        parent: &WorkItem,
        run: &WorkRun,
        child: WorkItem,
    ) -> StorageResult<WorkItem> {
        let publication: PublicationFence = serde_json::from_value(
            parent
                .payload
                .get("_publication")
                .cloned()
                .ok_or_else(changed)?,
        )
        .map_err(invalid)?;
        let configuration: elephant_types::provider_binding::SpaceConfigToken =
            serde_json::from_value(
                parent
                    .payload
                    .get("_source_configuration")
                    .cloned()
                    .ok_or_else(changed)?,
            )
            .map_err(invalid)?;
        let plan = crate::source_publication_manifest::source_plan_id(&publication, &configuration);
        if parent.kind != "triage_source"
            || child.kind != "ingest_metadata"
            || !same_envelope(&child.base, &parent.base)
            || publication.source_id.table() != TableName::Source
            || publication.generation.is_empty()
            || publication.revision_id.is_empty()
            || publication.operation_id.is_empty()
            || configuration.space_id != parent.base.space_id
            || parent
                .payload
                .get("_source_plan")
                .and_then(serde_json::Value::as_str)
                != Some(plan.as_str())
            || parent
                .payload
                .get("source_id")
                .and_then(serde_json::Value::as_str)
                != Some(publication.source_id.to_string().as_str())
            || child.payload.get("source_id") != parent.payload.get("source_id")
            || ["_publication", "_source_configuration", "_source_plan"]
                .iter()
                .any(|key| child.payload.get(*key) != parent.payload.get(*key))
        {
            return Err(changed());
        }
        self.enqueue_child(parent, run, child, true, None, None, Some(&publication))
            .await
    }

    /// Initial accepted-source handoff from the sealed original watch attempt.
    ///
    /// Retains the source fence and the actual private parent accounting chain.
    /// An intake receipt-only retry must not call this mutation API.
    ///
    /// # Errors
    /// Rejects a substituted acceptance/source envelope or obsolete producer.
    pub async fn enqueue_watch_source(
        &self,
        producer: &PreparedSourceWatchProducer,
        source: &Source,
        publication: &PublicationFence,
        child: WorkItem,
    ) -> StorageResult<WorkItem> {
        let parent = producer.original_work();
        let run = producer.original_run();
        let repository =
            crate::source_publication::SourcePublicationRepository::new(self.client.clone());
        let revision = repository
            .revision(
                producer.space_id(),
                &publication.source_id,
                &publication.revision_id,
            )
            .await?
            .ok_or_else(changed)?;
        if source.base.id != publication.source_id
            || source.base.space_id != producer.space_id()
            || source_support_fingerprint(source)? != source_support_fingerprint(&revision.source)?
            || repository
                .source_operation_producer(producer.space_id(), publication)
                .await?
                != producer.origin()
            || child.kind != "triage_source"
            || child.base.space_id != source.base.space_id
            || child.base.created_by != parent.base.created_by
            || child.base.security != source.base.security
            || child.base.subjects != source.base.subjects
            || child.base.subjects_state != source.base.subjects_state
            || child.base.provenance != source.base.provenance
            || child.payload
                != serde_json::json!({
                    "source_id": publication.source_id.to_string(),
                    "policy_version": "source-publication-v1", "_publication": publication,
                })
        {
            return Err(changed());
        }
        drop(repository);
        let current_source = crate::SourceRepository::new(self.client.clone())
            .get(producer.space_id(), &source.base.id)
            .await?
            .ok_or_else(changed)?;
        // This row observation closes erasure/reclassification races around the
        // queue insert. Canonical publication still owns head/currency checks.
        if current_source.hash != source.hash
            || current_source.base.security != source.base.security
            || current_source.base.subjects != source.base.subjects
            || current_source.base.subjects_state != source.base.subjects_state
        {
            return Err(changed());
        }
        self.enqueue_child(
            parent,
            run,
            child,
            false,
            None,
            Some((producer, &current_source)),
            None,
        )
        .await
    }

    async fn observed_lineage_parent(
        &self,
        observation: Option<&NativeLineageObservation>,
        space: &str,
        work_id: &RecordId,
        run_id: &RecordId,
    ) -> StorageResult<(WorkItem, WorkRun, OriginEnvelope)> {
        match observation {
            Some(observed) => observed.parent(space, work_id, run_id),
            None => self.lineage_parent(space, work_id, run_id).await,
        }
    }

    async fn lineage_dependencies(
        &self,
        parent: &WorkItem,
    ) -> StorageResult<(Vec<CanonicalRecord>, Vec<(WorkItem, OriginEnvelope)>)> {
        let mut rows = Vec::new();
        let mut origins = Vec::new();
        let mut current = crate::WorkItemRepository::new(self.client.clone())
            .get(&parent.base.space_id, &parent.base.id)
            .await?
            .ok_or_else(changed)?;
        if work_intent_fingerprint(&current)? != work_intent_fingerprint(parent)? {
            return Err(changed());
        }
        if fence_from_claim(&current).ok().as_ref() != Some(&fence_from_claim(parent)?) {
            return Err(StorageError::Conflict(
                "stale_attempt: original producer claim changed".into(),
            ));
        }
        let native_observation = if self.client.is_embedded() {
            None
        } else {
            Some(NativeLineageObservation::capture(self, &current).await?)
        };
        let mut visited = std::collections::BTreeSet::new();
        let mut observed_origin = None;
        for _ in 0..256 {
            if !visited.insert(current.base.id.clone()) {
                return Err(changed());
            }
            let evidence = match observed_origin.take() {
                Some(origin) => origin,
                None => match &native_observation {
                    Some(observed) => observed.origin(&current)?,
                    None => self.read_envelope(&current).await?.ok_or_else(changed)?,
                },
            };
            rows.push(CanonicalRecord::WorkItem(Box::new(current.clone())));
            origins.push((current.clone(), evidence.clone()));
            match &evidence.origin {
                WorkProducerOrigin::Unknown => return Err(changed()),
                WorkProducerOrigin::Direct => return Ok((rows, origins)),
                WorkProducerOrigin::ReprocessJob {
                    job_id,
                    execution_origin,
                    ..
                } => {
                    let job = crate::reprocess_job_repository::ReprocessJobRepository::new(
                        self.client.clone(),
                    )
                    .get(&current.base.space_id, job_id)
                    .await?
                    .ok_or_else(changed)?;
                    verify_reprocess_origin(&evidence.origin, &job, &current)?;
                    rows.push(CanonicalRecord::ReprocessJob(Box::new(job)));
                    match execution_origin {
                        ReprocessExecutionOrigin::Independent => return Ok((rows, origins)),
                        ReprocessExecutionOrigin::Inherited {
                            parent_work_item_id,
                            original_parent_work_run_id,
                            ..
                        } => {
                            let (prior, run, origin) = self
                                .observed_lineage_parent(
                                    native_observation.as_ref(),
                                    &current.base.space_id,
                                    parent_work_item_id,
                                    original_parent_work_run_id,
                                )
                                .await?;
                            verify_reprocess_execution_parent(execution_origin, &prior, &run)?;
                            rows.push(CanonicalRecord::WorkRun(Box::new(run)));
                            current = prior;
                            observed_origin = Some(origin);
                        }
                    }
                }
                WorkProducerOrigin::Child {
                    parent_work_item_id,
                    original_parent_work_run_id,
                    ..
                } => {
                    let (prior, prior_run, origin) = self
                        .observed_lineage_parent(
                            native_observation.as_ref(),
                            &current.base.space_id,
                            parent_work_item_id,
                            original_parent_work_run_id,
                        )
                        .await?;
                    verify_parent_origin(&evidence.origin, &prior, &prior_run)?;
                    rows.push(CanonicalRecord::WorkRun(Box::new(prior_run)));
                    current = prior;
                    observed_origin = Some(origin);
                }
            }
        }
        Err(changed())
    }

    #[allow(clippy::too_many_arguments)]
    #[allow(
        clippy::too_many_lines,
        reason = "Child enqueue validates parent lineage and origin before one idempotent guarded write on each backend; retain the complete transaction boundary."
    )]
    async fn enqueue_child(
        &self,
        parent: &WorkItem,
        run: &WorkRun,
        mut child: WorkItem,
        wait: bool,
        group: Option<&ConflictGroup>,
        watch: Option<(&PreparedSourceWatchProducer, &Source)>,
        metadata_publication: Option<&PublicationFence>,
    ) -> StorageResult<WorkItem> {
        let attempt = fence_from_claim(parent)?;
        if child.base.space_id != parent.base.space_id
            || child.status != WorkItemStatus::Runnable
            || child.attempts != 0
            || child.leased_by.is_some()
            || child.lease_expires_at.is_some()
            || child.base.deleted_at.is_some()
            || run.completed_at.is_some()
        {
            return Err(changed());
        }
        // The provided semantic key may mention a private coordinator/run. Only
        // its digest enters the public queue. It still identifies this exact run.
        let key = source_support_fingerprint(&serde_json::json!({
            "format": "work-producer-child-key/v1",
            "space_id": parent.base.space_id,
            "parent_work_item_id": parent.base.id,
            "original_parent_work_run_id": run.base.id,
            "kind": child.kind,
            "semantic_key": child.idempotency_key,
        }))?;
        child.idempotency_key = format!("private-child:{key}");
        child.base.id = RecordId::new(
            TableName::WorkItem,
            uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, key.as_bytes()),
        );
        let object = child
            .payload
            .as_object_mut()
            .ok_or_else(|| invalid("Child payload must be an object"))?;
        for private in [
            "_attempt_fence",
            "_predecessor_work_item",
            "_publication",
            "_source_configuration",
            "_source_plan",
            "_metadata_bundle_ids",
        ] {
            if private == "_publication" && watch.is_some() {
                continue;
            }
            if metadata_publication.is_some()
                && matches!(
                    private,
                    "_publication" | "_source_configuration" | "_source_plan"
                )
            {
                continue;
            }
            object.remove(private);
        }
        let origin = WorkProducerOrigin::Child {
            parent_work_item_id: parent.base.id.clone(),
            original_parent_work_run_id: run.base.id.clone(),
            original_parent_attempt: attempt.clone(),
            parent_intent_fingerprint: work_intent_fingerprint(parent)?,
            child_intent_fingerprint: work_intent_fingerprint(&child)?,
            wait_for_parent_completion: wait,
        };
        verify_parent_origin(&origin, parent, run)?;
        let mut source_ids = parent.base.provenance.sources.clone();
        if let Some((_, source)) = watch {
            source_ids.push(source.base.id.clone());
        }
        if let Some(publication) = metadata_publication {
            source_ids.push(publication.source_id.clone());
        }
        let envelope = OriginEnvelope::new(&child, origin, &source_ids)?;
        let (mut rows, origins) = self.lineage_dependencies(parent).await?;
        rows.push(CanonicalRecord::WorkRun(Box::new(run.clone())));
        if let Some(group) = group {
            rows.push(CanonicalRecord::ConflictGroup(Box::new(group.clone())));
        }
        if let Some((_, source)) = watch {
            rows.push(CanonicalRecord::Source(Box::new(source.clone())));
        }
        let existing = crate::WorkItemRepository::new(self.client.clone())
            .get(&child.base.space_id, &child.base.id)
            .await?;
        if let Some(existing) = &existing {
            if work_intent_fingerprint(existing)? != work_intent_fingerprint(&child)? {
                return Err(changed());
            }
            if self.read_envelope(existing).await?.as_ref() != Some(&envelope) {
                return Err(changed());
            }
            rows.push(CanonicalRecord::WorkItem(Box::new(existing.clone())));
        }
        if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    if let Some((producer, _)) = watch {
                        producer.check_tx(tx)?;
                    } else {
                        check_attempt_fence(tx, &attempt, chrono::Utc::now())?;
                    }
                    for row in &rows {
                        let current = row.current(tx)?.ok_or_else(changed)?;
                        if source_support_fingerprint(&current)? != source_support_fingerprint(row)?
                        {
                            return Err(changed());
                        }
                    }
                    for (work, origin) in &origins {
                        if read_tx(tx, work)?.as_ref() != Some(origin) {
                            return Err(changed());
                        }
                    }
                    if let Some(existing) = &existing {
                        if read_tx(tx, existing)?.as_ref() != Some(&envelope) {
                            return Err(changed());
                        }
                    } else {
                        if tx
                            .get_by_id::<WorkItemArchive>(
                                &crate::work_retention::archive_id(&child.base.id),
                                true,
                            )?
                            .is_some()
                        {
                            return Err(changed());
                        }
                        tx.create(child.clone())?;
                        insert_tx(tx, &envelope)?;
                    }
                    Ok(())
                })
                .await?;
        } else {
            let mut sql = "BEGIN TRANSACTION;\n".to_string();
            let mut bindings = crate::bindings! { "space" => child.base.space_id.clone() };
            if let Some((producer, _)) = watch {
                producer.append_guard_sql(&mut sql, &mut bindings)?;
            } else {
                append_attempt_fence(&mut sql, &mut bindings, &attempt)?;
            }
            append_lineage_row_guards(&mut sql, &mut bindings, &rows)?;
            append_origin_guards(&mut sql, &mut bindings, &origins)?;
            if existing.is_some() {
                append_origin_guard(&mut sql, &mut bindings, "producer_child", &envelope)?;
            } else {
                bindings.extend(crate::bindings! {
                    "child_id" => crate::surreal::thing_from_record_id(&child.base.id),
                    "child_archive" => crate::surreal::thing_from_record_id(&crate::work_retention::archive_id(&child.base.id)),
                    "child_record" => crate::WorkItemRepository::work_item_to_value(&child)?,
                });
                sql.push_str("IF (SELECT VALUE id FROM ONLY $child_archive) != NONE { THROW 'work_identity_already_archived'; }; CREATE $child_id CONTENT $child_record;\n");
                append_insert(&mut sql, &mut bindings, "producer_child", &envelope)?;
            }
            sql.push_str("COMMIT TRANSACTION;");
            self.client.query_bind_checked(&sql, bindings).await?;
        }
        Ok(existing.unwrap_or(child))
    }
}

#[cfg(test)]
#[path = "work_producer_enqueue_batch_tests.rs"]
mod batch_tests;

#[cfg(test)]
#[path = "work_producer_observation_tests.rs"]
mod observation_tests;
