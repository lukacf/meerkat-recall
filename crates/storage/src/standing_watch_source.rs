//! Original source-operation custody across queue archival and nested producers.
//!
//! This aggregate has no caller principal: it guards every discovered standing
//! delegation and the exact evidence establishing where those ceilings apply.
use super::{
    invalid, refused, same_configuration_policy, BTreeMap, EmbeddedMutation,
    PreparedStandingWatchAuthority, Principal, PublicationFence, RecordId, Repository,
    StandingWatchIdentity, StandingWatchRepository, StorageClient, StorageResult, Value, WorkItem,
    PAYLOAD_KEY,
};
use crate::canonical_commit::{source_support_fingerprint, CanonicalRecord};
use crate::source_publication::{
    control_thing, read_control_payload_tx, SourcePublicationRepository, StoredOperation,
    OPERATION_TABLE,
};
use crate::work_producer_repository::{
    verify_original_control_payload, verify_parent_origin, verify_reprocess_execution_parent,
    verify_reprocess_origin, ReprocessExecutionOrigin, WorkProducerOrigin,
};
use elephant_types::publication::{AttemptFence, SourceProducerOrigin};
use std::collections::BTreeSet;
use std::fmt::Write as _;

// Same original Work ancestry limit as the budget resolver. Source-operation
// nodes are counted separately because each Work may carry one publication and
// source entry itself adds one; they must not halve the supported Work depth.
const MAX_WORK_NODES: usize = 256;
const MAX_SOURCE_NODES: usize = MAX_WORK_NODES + 1;

#[derive(Clone, PartialEq, Eq)]
struct Control {
    table: &'static str,
    key: String,
    payload: String,
}

/// Exact original source ancestry and all applicable standing ceilings.
///
/// Even a proven Direct operation retains its operation descriptor guard, so a concurrent
/// replacement cannot transform an unguarded no-authority result.
#[derive(Clone)]
pub struct PreparedStandingSourceAuthority {
    space: String,
    authorities: Vec<PreparedStandingWatchAuthority>,
    records: BTreeMap<RecordId, CanonicalRecord>,
    controls: Vec<Control>,
    source_ids: BTreeSet<RecordId>,
}
redacted_debug!(PreparedStandingSourceAuthority);

enum Task {
    Source(PublicationFence),
    Work(Box<WorkItem>),
    Exit(String),
}

impl PreparedStandingSourceAuthority {
    /// Capture this exact original Work and its saved execution ancestry.
    ///
    /// This
    /// shares the source-operation resolver; missing controls never become roots.
    /// # Errors
    /// Refuses changed, missing, cyclic, foreign or unbounded original evidence.
    pub async fn capture_for_work(
        client: &StorageClient,
        original: &WorkItem,
    ) -> StorageResult<Self> {
        let current = crate::WorkItemRepository::new(client.clone())
            .get(&original.base.space_id, &original.base.id)
            .await?
            .ok_or_else(refused)?;
        if source_support_fingerprint(&current)? != source_support_fingerprint(original)? {
            return Err(refused());
        }
        capture_tasks(
            client,
            &original.base.space_id,
            vec![Task::Work(Box::new(original.clone()))],
        )
        .await
    }

    /// Reconcile live execution bookkeeping without adopting new authority.
    ///
    /// Only operation state/error progress may differ from the original payload;
    /// all producer, policy, source and acceptance fields remain exact. The
    /// returned copy still uses the existing whole-payload transaction guard.
    pub(crate) async fn reconcile_source_progress(
        &self,
        client: &StorageClient,
    ) -> StorageResult<Self> {
        let repository = SourcePublicationRepository::new(client.clone());
        let mut reconciled = self.clone();
        for control in &mut reconciled.controls {
            if control.table != OPERATION_TABLE {
                continue;
            }
            let current = read_payload(
                client,
                &repository,
                control.table,
                &control.key,
                &self.space,
            )
            .await?
            .ok_or_else(refused)?;
            if current != control.payload
                && live_operation_authority(&current)?
                    != live_operation_authority(&control.payload)?
            {
                return Err(refused());
            }
            control.payload = current;
        }
        drop(repository);
        Ok(reconciled)
    }

    /// Reconcile a refused transaction only when live receipt progress actually
    /// changed since its last guard. All other original evidence stays captured;
    /// the next transaction must still validate it, including real revocation.
    pub(crate) async fn reconcile_changed_source_progress(
        &self,
        client: &StorageClient,
    ) -> StorageResult<Option<Self>> {
        let reconciled = self.reconcile_source_progress(client).await?;
        Ok((reconciled.controls != self.controls).then_some(reconciled))
    }

    /// Actual source input identities encountered through execution ancestry.
    /// Callers resolve the current affected records without inventing new edges.
    pub fn source_ids(&self) -> impl Iterator<Item = &RecordId> {
        self.source_ids.iter()
    }

    /// Authorize an actual original Source/Doc/render/artifact input under every
    /// applicable ceiling, then retain its exact row for subsequent revalidation.
    ///
    /// Uses the same record policy as Watch registration; no profile bypass.
    /// # Errors
    /// Refuses a denied, foreign, deleted, unsupported or inconsistently read row.
    pub fn authorize_record(&mut self, record: CanonicalRecord) -> StorageResult<()> {
        if record.base().space_id != self.space
            || record.base().deleted_at.is_some()
            || !matches!(
                record,
                CanonicalRecord::Source(_)
                    | CanonicalRecord::Doc(_)
                    | CanonicalRecord::DocRevision(_)
                    | CanonicalRecord::DocNode(_)
                    | CanonicalRecord::WorkArtifact(_)
            )
        {
            return Err(refused());
        }
        for principal in self.principals() {
            crate::source_watch_registration::authorize_standing_source_input(principal, &record)?;
        }
        self.record(record)
    }

    /// Every applicable original standing ceiling, attenuated by its own actual
    /// current grant.
    ///
    /// Callers authorize all relevant Source/Doc effects against
    /// ALL of them; selecting an arbitrary ancestor would widen authority.
    pub fn principals(&self) -> impl Iterator<Item = &Principal> {
        self.authorities
            .iter()
            .map(PreparedStandingWatchAuthority::principal)
    }

    /// Read-only pre-dispatch revalidation.
    ///
    /// No mutation/checkpoint or new grant
    /// adoption occurs. Actual effects still require the same transaction guard.
    /// # Errors
    /// Refuses any changed original control, historical row, grant/config/Watch.
    pub async fn verify_current(&self, client: &StorageClient) -> StorageResult<()> {
        let repository = SourcePublicationRepository::new(client.clone());
        for control in &self.controls {
            if read_payload(
                client,
                &repository,
                control.table,
                &control.key,
                &self.space,
            )
            .await?
            .as_deref()
                != Some(control.payload.as_str())
            {
                return Err(refused());
            }
        }
        for record in self.records.values() {
            let current = CanonicalRecord::load(client, &self.space, &record.base().id)
                .await?
                .ok_or_else(refused)?;
            if source_support_fingerprint(&current)? != source_support_fingerprint(record)? {
                return Err(refused());
            }
        }
        for authority in &self.authorities {
            let current = StandingWatchRepository::new(client.clone())
                .capture(&authority.watch, &authority.stored.identity)
                .await?;
            if !same_configuration_policy(&current.configuration, &authority.configuration)?
                || current.grant != authority.grant
            {
                return Err(refused());
            }
        }
        Ok(())
    }

    fn record(&mut self, record: CanonicalRecord) -> StorageResult<()> {
        let key = record.base().id.clone();
        if let Some(prior) = self.records.get(&key) {
            if source_support_fingerprint(prior)? != source_support_fingerprint(&record)? {
                return Err(refused());
            }
        } else {
            // Actual input populations are set by the admitted source/manifest
            // and physical-call selection. Do not impose an ancestry multiplier
            // on supported document graphs. Ordered dedup avoids quadratic scans.
            self.records.insert(key, record);
        }
        Ok(())
    }

    async fn control(
        &mut self,
        client: &StorageClient,
        repository: &SourcePublicationRepository,
        table: &'static str,
        key: &str,
    ) -> StorageResult<String> {
        let payload = read_payload(client, repository, table, key, &self.space)
            .await?
            .ok_or_else(refused)?;
        if let Some(prior) = self
            .controls
            .iter()
            .find(|row| row.table == table && row.key == key)
        {
            if prior.payload != payload {
                return Err(refused());
            }
        } else {
            self.controls.push(Control {
                table,
                key: key.into(),
                payload: payload.clone(),
            });
        }
        Ok(payload)
    }

    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        for control in &self.controls {
            if read_control_payload_tx(tx, control.table, &control.key, &self.space)?.as_deref()
                != Some(control.payload.as_str())
            {
                return Err(refused());
            }
        }
        for record in self.records.values() {
            let current = CanonicalRecord::load_tx(tx, &self.space, &record.base().id)?
                .ok_or_else(refused)?;
            if source_support_fingerprint(&current)? != source_support_fingerprint(record)? {
                return Err(refused());
            }
        }
        for authority in &self.authorities {
            authority.check_tx(tx)?;
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        if bindings
            .get("space")
            .is_some_and(|value| value != &Value::from(self.space.as_str()))
        {
            return Err(refused());
        }
        bindings.insert("space".into(), self.space.clone().into());
        for (index, control) in self.controls.iter().enumerate() {
            let prefix = format!("standing_source_control_{index}");
            bindings.insert(
                format!("{prefix}_id"),
                control_thing(control.table, &control.key).into(),
            );
            bindings.insert(format!("{prefix}_payload"), control.payload.clone().into());
            // 041 exempts this existing field from semantic epoch rotation.
            // A different nonce name would turn an authority read into an
            // apparent producer change, invalidating prepared canonical work.
            let nonce = match control.table {
                "work_producer_origin" => "read_nonce",
                OPERATION_TABLE => "canonical_read_nonce",
                _ => return Err(refused()),
            };
            let _ = writeln!(sql,"LET ${prefix}_current = SELECT * FROM ONLY ${prefix}_id;\nIF ${prefix}_current = NONE OR ${prefix}_current.space_id != $space OR ${prefix}_current.payload != ${prefix}_payload {{ THROW 'standing_watch_authority_changed_or_missing'; }};\nUPDATE ${prefix}_id SET {nonce}=<string>rand::uuid();");
        }
        for (index, record) in self.records.values().enumerate() {
            crate::canonical_commit::append_row_guard(
                sql,
                bindings,
                &format!("standing_source_record_{index}"),
                record,
                Some(record),
            )?;
        }
        for (index, authority) in self.authorities.iter().enumerate() {
            let mut guard = String::new();
            let mut values = BTreeMap::new();
            authority.append_guard_sql(&mut guard, &mut values)?;
            append_renamed(
                sql,
                bindings,
                &guard,
                values,
                &format!("standing_watch_source_{index}_"),
            );
        }
        Ok(())
    }
}

// Parse the original serialized authority rather than the native `state`
// mirror. A payload-only producer change must still be rejected. Preserve
// unknown fields too: only these two explicit live progress fields may differ.
fn live_operation_authority(payload: &str) -> StorageResult<serde_json::Value> {
    let operation: StoredOperation = serde_json::from_str(payload).map_err(invalid)?;
    if !matches!(
        operation.receipt.state,
        elephant_types::publication::OperationState::Accepted
            | elephant_types::publication::OperationState::Processing
            | elephant_types::publication::OperationState::FailedRetryable
    ) || operation.receipt.completed_at.is_some()
    {
        return Err(refused());
    }
    let mut value: serde_json::Value = serde_json::from_str(payload).map_err(invalid)?;
    let receipt = value
        .get_mut("receipt")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or_else(refused)?;
    receipt.remove("state");
    receipt.remove("error_code");
    Ok(value)
}

pub(super) fn append_renamed(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    guard: &str,
    values: BTreeMap<String, Value>,
    prefix: &str,
) {
    sql.push_str(&guard.replace("$standing_watch_", &format!("${prefix}")));
    bindings.extend(values.into_iter().map(|(key, value)| {
        (
            key.strip_prefix("standing_watch_")
                .map_or_else(|| key.clone(), |suffix| format!("{prefix}{suffix}")),
            value,
        )
    }));
}

pub(super) async fn capture(
    client: &StorageClient,
    space: &str,
    fence: &PublicationFence,
) -> StorageResult<PreparedStandingSourceAuthority> {
    capture_tasks(client, space, vec![Task::Source(fence.clone())]).await
}

#[allow(
    clippy::too_many_lines,
    reason = "The bounded traversal captures dependent tasks, runs, artifacts, and origin guards with one visited set; preserve discovery order and the resulting sealed dependency set."
)]
async fn capture_tasks(
    client: &StorageClient,
    space: &str,
    initial: Vec<Task>,
) -> StorageResult<PreparedStandingSourceAuthority> {
    let repository = SourcePublicationRepository::new(client.clone());
    let mut proof = PreparedStandingSourceAuthority {
        space: space.into(),
        authorities: vec![],
        records: BTreeMap::new(),
        controls: vec![],
        source_ids: BTreeSet::new(),
    };
    let mut tasks = initial;
    let mut source_fences = BTreeMap::<String, PublicationFence>::new();
    let mut path = BTreeSet::new();
    let mut complete = BTreeSet::new();
    let mut work_nodes = 0;
    let mut source_nodes = 0;
    while let Some(task) = tasks.pop() {
        let key = match &task {
            Task::Source(fence) => {
                // Check every reference before completed-node dedup. Reusing an
                // operation ID cannot validate different source/revision/generation.
                if let Some(original) = source_fences.get(&fence.operation_id) {
                    if original != fence {
                        return Err(refused());
                    }
                } else {
                    source_fences.insert(fence.operation_id.clone(), fence.clone());
                }
                format!("source:{}", fence.operation_id)
            }
            Task::Work(work) => {
                // A converging branch must retain the exact same original Work
                // snapshot, including authority and original claim bookkeeping.
                proof.record(CanonicalRecord::WorkItem(work.clone()))?;
                format!("work:{}", work.base.id)
            }
            Task::Exit(key) => {
                path.remove(key);
                complete.insert(key.clone());
                continue;
            }
        };
        if path.contains(&key) {
            return Err(refused());
        }
        if complete.contains(&key) {
            continue;
        }
        match &task {
            Task::Work(_) => work_nodes += 1,
            Task::Source(_) => source_nodes += 1,
            Task::Exit(_) => return Err(refused()),
        }
        if work_nodes > MAX_WORK_NODES || source_nodes > MAX_SOURCE_NODES {
            return Err(refused());
        }
        path.insert(key.clone());
        tasks.push(Task::Exit(key));
        match task {
            Task::Source(fence) => {
                proof.source_ids.insert(fence.source_id.clone());
                let payload = proof
                    .control(client, &repository, OPERATION_TABLE, &fence.operation_id)
                    .await?;
                let operation: StoredOperation = serde_json::from_str(&payload).map_err(invalid)?;
                let receipt = &operation.receipt;
                if receipt.space_id != space
                    || receipt.source_id != fence.source_id
                    || receipt.source_generation != fence.generation
                    || receipt.operation_id != fence.operation_id
                    || receipt.desired_revision.as_deref() != Some(fence.revision_id.as_str())
                {
                    return Err(refused());
                }
                match &operation.producer_origin {
                    SourceProducerOrigin::UnknownOriginal => return Err(refused()),
                    SourceProducerOrigin::Direct => {}
                    SourceProducerOrigin::Watch {
                        watch_id,
                        work_item_id,
                        work_run_id,
                        ..
                    } => {
                        let watch = crate::SourceWatchRepository::new(client.clone())
                            .get(space, watch_id)
                            .await?
                            .ok_or_else(refused)?;
                        let work = crate::WorkItemRepository::new(client.clone())
                            .get(space, work_item_id)
                            .await?
                            .ok_or_else(refused)?;
                        let run = crate::WorkRunRepository::new(client.clone())
                            .get(space, work_run_id)
                            .await?
                            .ok_or_else(refused)?;
                        crate::source_watch_producer::verify_watch_origin(
                            &operation.producer_origin,
                            &work,
                            &run,
                        )?;
                        let authority = PreparedStandingWatchAuthority::capture_for_original_watch(
                            client, &watch, &work, &run,
                        )
                        .await?;
                        proof.source_ids.insert(watch.source.id().clone());
                        proof.authorities.push(authority);
                        proof.record(CanonicalRecord::WorkRun(Box::new(run)))?;
                        tasks.push(Task::Work(Box::new(work)));
                    }
                    SourceProducerOrigin::Reprocess {
                        job_id,
                        selection_artifact_id,
                        source_intent_id,
                        intent_fingerprint,
                        original_attempt,
                        ..
                    } => {
                        let job = crate::ReprocessJobRepository::new(client.clone())
                            .get(space, job_id)
                            .await?
                            .ok_or_else(refused)?;
                        let selection = crate::WorkArtifactRepository::new(client.clone())
                            .get(space, selection_artifact_id)
                            .await?
                            .ok_or_else(refused)?;
                        let intent = crate::WorkArtifactRepository::new(client.clone())
                            .get(space, source_intent_id)
                            .await?
                            .ok_or_else(refused)?;
                        if source_support_fingerprint(&intent)? != *intent_fingerprint {
                            return Err(refused());
                        }
                        // The source producer permits a claimed reprocess root,
                        // not an arbitrary Watch Child masquerading as a job.
                        if let Some(attempt) = original_attempt {
                            let work = crate::WorkItemRepository::new(client.clone())
                                .get(space, &attempt.work_item_id)
                                .await?
                                .ok_or_else(refused)?;
                            let payload = proof
                                .control(
                                    client,
                                    &repository,
                                    "work_producer_origin",
                                    &work.base.id.id().to_string(),
                                )
                                .await?;
                            let origin = verify_original_control_payload(&work, &payload)?;
                            verify_reprocess_origin(&origin, &job, &work)?;
                            let runs = crate::WorkRunRepository::new(client.clone())
                                .find_by_work_item(&work.base.id)
                                .await?;
                            let mut matches = runs.into_iter().filter(|run| {
                                run.base.space_id == space
                                    && run.base.deleted_at.is_none()
                                    && run.work_item.id() == &work.base.id
                                    && run
                                        .metrics
                                        .custom
                                        .get("attempt_fence")
                                        .and_then(|value| {
                                            serde_json::from_value::<AttemptFence>(value.clone())
                                                .ok()
                                        })
                                        .as_ref()
                                        == Some(attempt)
                            });
                            let run = matches.next().ok_or_else(refused)?;
                            if matches.next().is_some() || attempt.space_id != space {
                                return Err(refused());
                            }
                            proof.record(CanonicalRecord::WorkRun(Box::new(run)))?;
                            tasks.push(Task::Work(Box::new(work)));
                        } else {
                            // Explicit trusted direct reprocess still requires
                            // its real immutable job origin; absent is Unknown.
                            let work = crate::reprocess_job_repository::intended_work(&job)?;
                            let payload = proof
                                .control(
                                    client,
                                    &repository,
                                    "work_producer_origin",
                                    &work.base.id.id().to_string(),
                                )
                                .await?;
                            let origin = verify_original_control_payload(&work, &payload)?;
                            verify_reprocess_origin(&origin, &job, &work)?;
                            if let WorkProducerOrigin::ReprocessJob {
                                execution_origin, ..
                            } = &origin
                            {
                                if let Some(parent) =
                                    inherited_parent(client, space, execution_origin, &mut proof)
                                        .await?
                                {
                                    tasks.push(Task::Work(Box::new(parent)));
                                }
                            }
                        }
                        proof.record(CanonicalRecord::ReprocessJob(Box::new(job)))?;
                        proof.record(CanonicalRecord::WorkArtifact(Box::new(selection)))?;
                        proof.record(CanonicalRecord::WorkArtifact(Box::new(intent)))?;
                    }
                }
            }
            Task::Work(work) => {
                let work = *work;
                if work.base.space_id != space || work.base.deleted_at.is_some() {
                    return Err(refused());
                }
                let payload = proof
                    .control(
                        client,
                        &repository,
                        "work_producer_origin",
                        &work.base.id.id().to_string(),
                    )
                    .await?;
                let origin = verify_original_control_payload(&work, &payload)?;
                if let Some(value) = work.payload.get("_publication") {
                    tasks.push(Task::Source(
                        serde_json::from_value(value.clone()).map_err(invalid)?,
                    ));
                }
                // An actual Watch ancestor also restricts nested producers even
                // when encountered through an ordinary private Child edge.
                if work.kind == "refresh_source_watch" {
                    let watch_id: RecordId = work
                        .payload
                        .get("watch_id")
                        .and_then(serde_json::Value::as_str)
                        .ok_or_else(refused)?
                        .parse()
                        .map_err(invalid)?;
                    let watch = crate::SourceWatchRepository::new(client.clone())
                        .get(space, &watch_id)
                        .await?
                        .ok_or_else(refused)?;
                    let identity: StandingWatchIdentity = serde_json::from_value(
                        work.payload.get(PAYLOAD_KEY).cloned().ok_or_else(refused)?,
                    )
                    .map_err(invalid)?;
                    proof.source_ids.insert(watch.source.id().clone());
                    proof.authorities.push(
                        StandingWatchRepository::new(client.clone())
                            .capture(&watch, &identity)
                            .await?,
                    );
                }
                match &origin {
                    WorkProducerOrigin::Unknown => return Err(refused()),
                    WorkProducerOrigin::Direct => {
                        if work.kind == "execute_reprocess_job" {
                            return Err(refused());
                        }
                    }
                    WorkProducerOrigin::ReprocessJob {
                        job_id,
                        execution_origin,
                        ..
                    } => {
                        let job = crate::ReprocessJobRepository::new(client.clone())
                            .get(space, job_id)
                            .await?
                            .ok_or_else(refused)?;
                        verify_reprocess_origin(&origin, &job, &work)?;
                        proof.record(CanonicalRecord::ReprocessJob(Box::new(job)))?;
                        if let Some(parent) =
                            inherited_parent(client, space, execution_origin, &mut proof).await?
                        {
                            tasks.push(Task::Work(Box::new(parent)));
                        }
                    }
                    WorkProducerOrigin::Child {
                        parent_work_item_id,
                        original_parent_work_run_id,
                        ..
                    } => {
                        let parent = crate::WorkItemRepository::new(client.clone())
                            .get(space, parent_work_item_id)
                            .await?
                            .ok_or_else(refused)?;
                        let run = crate::WorkRunRepository::new(client.clone())
                            .get(space, original_parent_work_run_id)
                            .await?
                            .ok_or_else(refused)?;
                        verify_parent_origin(&origin, &parent, &run)?;
                        proof.record(CanonicalRecord::WorkRun(Box::new(run)))?;
                        tasks.push(Task::Work(Box::new(parent)));
                    }
                }
            }
            Task::Exit(_) => return Err(refused()),
        }
    }
    drop(repository);
    Ok(proof)
}

async fn inherited_parent(
    client: &StorageClient,
    space: &str,
    origin: &ReprocessExecutionOrigin,
    proof: &mut PreparedStandingSourceAuthority,
) -> StorageResult<Option<WorkItem>> {
    match origin {
        ReprocessExecutionOrigin::Independent => Ok(None),
        ReprocessExecutionOrigin::Inherited {
            parent_work_item_id,
            original_parent_work_run_id,
            ..
        } => {
            let parent = crate::WorkItemRepository::new(client.clone())
                .get(space, parent_work_item_id)
                .await?
                .ok_or_else(refused)?;
            let run = crate::WorkRunRepository::new(client.clone())
                .get(space, original_parent_work_run_id)
                .await?
                .ok_or_else(refused)?;
            verify_reprocess_execution_parent(origin, &parent, &run)?;
            proof.record(CanonicalRecord::WorkRun(Box::new(run)))?;
            Ok(Some(parent))
        }
    }
}

async fn read_payload(
    client: &StorageClient,
    repository: &SourcePublicationRepository,
    table: &str,
    key: &str,
    space: &str,
) -> StorageResult<Option<String>> {
    if let Some(store) = client.embedded_store() {
        return crate::private_control_read::payload(&store, table, space, key).await;
    }
    repository.read_control_payload(table, key, space).await
}

#[cfg(test)]
#[path = "standing_watch_source_tests.rs"]
mod tests;
