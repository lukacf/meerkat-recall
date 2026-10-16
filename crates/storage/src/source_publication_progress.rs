//! Original published progress, sealed with the existing canonical completion.
//! Current desired Source fields never supply historical publication progress.

use super::{
    conflict, control_thing, decode_payload, incomplete, manifest_key, stable_key, AttemptFence,
    BTreeMap, BTreeSet, Deserialize, EmbeddedMutation, FromStr, IngestMode, PublicationFence,
    RecordId, Repository, Serialize, SourceHead, SourcePublicationCompletion,
    SourcePublicationManifest, SourcePublicationManifestRepository, SourcePublicationRequirements,
    SourceVisibility, StorageClient, StorageResult, TableName, Value, WorkItem, WorkItemStatus,
    WorkRun, COMPLETION_TABLE,
};
use crate::canonical_commit::{
    source_support_fingerprint, CanonicalCommitReceipt, CanonicalRecord,
};
use crate::source_publication::{read_control_payload_tx, SourcePublicationRepository};
use crate::work_producer_repository::{
    verify_original_control_payload, verify_parent_origin, verify_reprocess_execution_parent,
    verify_reprocess_origin, ReprocessExecutionOrigin, WorkProducerOrigin,
};
use elephant_types::publication::{OperationState, SourceRevision};
use elephant_types::records::ingest::IngestDecision;
use elephant_types::records::source::SourceLifecycleStatus;
use elephant_types::records::Source;
use elephant_types::Link;
use std::fmt::Write as _;

/// Progress of the exact completed publication, not another operation state.
/// Explicit None means the original selected ancestry contained no triage.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedSourceProgress {
    /// Achieved mode from the immutable completed manifest.
    pub mode: IngestMode,
    /// Decision from the original successful triage execution, if one existed.
    pub latest_ingest_decision: Option<RecordId>,
    /// Exact original decision content; ID and digest must be present together.
    pub ingest_decision_fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
struct OriginalOrigin {
    key: String,
    payload: String,
}

#[derive(Debug, Clone)]
pub(super) struct PreparedProgress {
    rows: Vec<CanonicalRecord>,
    origins: Vec<OriginalOrigin>,
    decision: Option<IngestDecision>,
    previous_lookup: Option<String>,
}

impl PreparedProgress {
    pub(super) fn certificate(&self, mode: IngestMode) -> StorageResult<PublishedSourceProgress> {
        Ok(PublishedSourceProgress {
            mode,
            latest_ingest_decision: self.decision.as_ref().map(|row| row.base.id.clone()),
            ingest_decision_fingerprint: self
                .decision
                .as_ref()
                .map(source_support_fingerprint)
                .transpose()?,
        })
    }
}

fn changed() -> crate::StorageError {
    conflict("source_original_ingest_decision_changed")
}

/// Derived point-lookup key; the immutable certificate remains the proof.
pub(super) fn published_completion_key(space: &str, source: &RecordId) -> String {
    stable_key(&["published-source-completion/v1", space, &source.to_string()])
}

fn completed_run(work: &WorkItem, run: &WorkRun, space: &str) -> StorageResult<()> {
    if work.base.space_id != space
        || run.base.space_id != space
        || work.base.deleted_at.is_some()
        || run.base.deleted_at.is_some()
        || run.work_item.id() != &work.base.id
    {
        return Err(changed());
    }
    if work.status != WorkItemStatus::Done || !run.success || run.completed_at.is_none() {
        return Err(incomplete("original_triage_ancestry_not_complete"));
    }
    Ok(())
}

/// Capture original selected metadata ancestry; no global latest-decision scan.
#[expect(
    clippy::suspicious_operation_groupings,
    reason = "WorkRun.attempt records the admitting WorkItem.attempts counter; the original AttemptFence remains a separate required identity check."
)]
#[allow(
    clippy::too_many_lines,
    reason = "Progress capture joins work, runs, artifacts, publication controls, and their anomalies from one ordered observation; keep this cross-record projection phase together."
)]
pub(super) async fn capture(
    client: &StorageClient,
    manifest: &SourcePublicationManifest,
    head: &SourceHead,
) -> StorageResult<PreparedProgress> {
    let space = &manifest.space_id;
    let artifacts = crate::WorkArtifactRepository::new(client.clone());
    let items = crate::WorkItemRepository::new(client.clone());
    let runs = crate::WorkRunRepository::new(client.clone());
    let controls = SourcePublicationRepository::new(client.clone());
    let previous_lookup = controls
        .read_control_payload(
            COMPLETION_TABLE,
            &published_completion_key(space, &manifest.publication.source_id),
            space,
        )
        .await?;
    // Retraction clears the published head while retaining the old immutable
    // completion and its derived lookup. An unpublished accepted successor may
    // replace that lookup, but the old certificate must still match exactly.
    // This is a captured CAS preimage, never authority for current visibility;
    // projection below still requires an active published head and its revision.
    if let Some(payload) = &previous_lookup {
        let previous: SourcePublicationCompletion = decode_payload(payload)?;
        if previous.space_id != *space
            || previous.publication.source_id != manifest.publication.source_id
            || head
                .published_revision
                .as_ref()
                .is_some_and(|revision| revision != &previous.publication.revision_id)
        {
            return Err(conflict("source_progress_lookup_changed"));
        }
        let immutable = controls
            .read_control::<SourcePublicationCompletion>(
                COMPLETION_TABLE,
                &manifest_key(space, &previous.publication),
                space,
            )
            .await?;
        if immutable.as_ref() != Some(&previous) {
            return Err(conflict("source_progress_lookup_changed"));
        }
    }
    let mut captured = PreparedProgress {
        rows: Vec::new(),
        origins: Vec::new(),
        decision: None,
        previous_lookup,
    };
    for bundle_id in &manifest.metadata_artifact_ids {
        let bundle = artifacts
            .get(space, bundle_id)
            .await?
            .ok_or_else(|| incomplete("original_metadata_bundle_missing"))?;
        let candidate_id = bundle
            .data
            .get("source_artifact_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(changed)?;
        let candidate = artifacts
            .get(space, &RecordId::from_str(candidate_id)?)
            .await?
            .ok_or_else(|| incomplete("original_metadata_candidate_missing"))?;
        if candidate.base.id == bundle.base.id || candidate.work_run != bundle.work_run {
            return Err(changed());
        }
        let mut run = runs
            .get(space, candidate.work_run.id())
            .await?
            .ok_or_else(|| incomplete("origin_run_missing"))?;
        let mut work = items
            .get(space, run.work_item.id())
            .await?
            .ok_or_else(|| incomplete("predecessor_work_missing"))?;
        captured
            .rows
            .push(CanonicalRecord::WorkArtifact(Box::new(bundle)));
        captured
            .rows
            .push(CanonicalRecord::WorkArtifact(Box::new(candidate)));
        let mut seen = BTreeSet::new();
        let mut original_parent_edge = false;
        for depth in 0..256 {
            if !seen.insert(work.base.id.clone()) {
                return Err(changed());
            }
            completed_run(&work, &run, space)?;
            let key = work.base.id.id().to_string();
            let payload = controls
                .read_control_payload("work_producer_origin", &key, space)
                .await?
                .ok_or_else(changed)?;
            let origin = verify_original_control_payload(&work, &payload)?;
            if matches!(origin, WorkProducerOrigin::Unknown) {
                return Err(changed());
            }
            if matches!(origin, WorkProducerOrigin::Direct)
                && work.payload.get("_predecessor_work_item").is_some()
            {
                // An old public predecessor hint does not retain the original
                // parent run or attempt. It cannot prove independent ancestry
                // or authorize a decision lookup from a current/latest run.
                return Err(conflict("source_original_ancestry_unproven"));
            }
            // A terminal triage still has to prove its own producer. Do not
            // let its successful decision skip an original job/parent check.
            if let WorkProducerOrigin::ReprocessJob { job_id, .. } = &origin {
                let job = crate::ReprocessJobRepository::new(client.clone())
                    .get(space, job_id)
                    .await?
                    .ok_or_else(changed)?;
                verify_reprocess_origin(&origin, &job, &work)?;
                captured
                    .rows
                    .push(CanonicalRecord::ReprocessJob(Box::new(job)));
            }
            captured
                .rows
                .push(CanonicalRecord::WorkItem(Box::new(work.clone())));
            captured
                .rows
                .push(CanonicalRecord::WorkRun(Box::new(run.clone())));
            captured.origins.push(OriginalOrigin { key, payload });
            if work.kind == "triage_source" {
                let producer_parent = match &origin {
                    WorkProducerOrigin::Child {
                        parent_work_item_id,
                        original_parent_work_run_id,
                        ..
                    }
                    | WorkProducerOrigin::ReprocessJob {
                        execution_origin:
                            ReprocessExecutionOrigin::Inherited {
                                parent_work_item_id,
                                original_parent_work_run_id,
                                ..
                            },
                        ..
                    } => Some((parent_work_item_id, original_parent_work_run_id)),
                    _ => None,
                };
                if let Some((parent_id, parent_run_id)) = producer_parent {
                    let parent = items.get(space, parent_id).await?.ok_or_else(changed)?;
                    let parent_run = runs.get(space, parent_run_id).await?.ok_or_else(changed)?;
                    match &origin {
                        WorkProducerOrigin::Child { .. } => {
                            verify_parent_origin(&origin, &parent, &parent_run)?;
                        }
                        WorkProducerOrigin::ReprocessJob {
                            execution_origin, ..
                        } => verify_reprocess_execution_parent(
                            execution_origin,
                            &parent,
                            &parent_run,
                        )?,
                        _ => return Err(changed()),
                    }
                    // The producing coordinator can legitimately still be
                    // running. Its exact original edge is proof, not Done.
                    captured
                        .rows
                        .push(CanonicalRecord::WorkItem(Box::new(parent)));
                    captured
                        .rows
                        .push(CanonicalRecord::WorkRun(Box::new(parent_run)));
                }
                let original_attempt: AttemptFence = serde_json::from_value(
                    run.metrics
                        .custom
                        .get("attempt_fence")
                        .cloned()
                        .ok_or_else(changed)?,
                )
                .map_err(|_| changed())?;
                if original_attempt.space_id != *space
                    || original_attempt.work_item_id != work.base.id
                    || original_attempt.generation == 0
                    || original_attempt.lease_token.is_empty()
                    || run.attempt == 0
                {
                    return Err(changed());
                }
                // A child edge already verifies the retained ORIGINAL attempt.
                // A direct producer retains its historical fence in Done work.
                // This compares original persisted bytes; it neither requires
                // nor creates a current Running lease.
                if !original_parent_edge {
                    let admitted_attempt: AttemptFence = serde_json::from_value(
                        work.payload
                            .get(crate::work_attempt_repository::ATTEMPT_PAYLOAD_KEY)
                            .cloned()
                            .ok_or_else(changed)?,
                    )
                    .map_err(|_| changed())?;
                    if run.attempt != work.attempts || admitted_attempt != original_attempt {
                        return Err(changed());
                    }
                }
                let publication: PublicationFence = serde_json::from_value(
                    work.payload
                        .get("_publication")
                        .cloned()
                        .ok_or_else(changed)?,
                )
                .map_err(|_| changed())?;
                if publication != manifest.publication {
                    return Err(changed());
                }
                let decision_id: RecordId = serde_json::from_value(
                    run.metrics
                        .custom
                        .get("source_ingest_decision_id")
                        .cloned()
                        .ok_or_else(changed)?,
                )
                .map_err(|_| changed())?;
                if decision_id.table() != TableName::IngestDecision {
                    return Err(changed());
                }
                let decision = crate::IngestDecisionRepository::new(client.clone())
                    .get(space, &decision_id)
                    .await?
                    .ok_or_else(changed)?;
                if decision.base.space_id != *space
                    || decision.base.deleted_at.is_some()
                    || decision.source.id() != &manifest.publication.source_id
                    || decision.decision != manifest.mode
                {
                    return Err(changed());
                }
                if captured
                    .decision
                    .as_ref()
                    .is_some_and(|prior| prior != &decision)
                {
                    return Err(changed());
                }
                captured.decision = Some(decision);
                break;
            }
            match &origin {
                WorkProducerOrigin::Direct => break,
                WorkProducerOrigin::ReprocessJob {
                    execution_origin, ..
                } => match execution_origin {
                    ReprocessExecutionOrigin::Independent => break,
                    ReprocessExecutionOrigin::Inherited {
                        parent_work_item_id,
                        original_parent_work_run_id,
                        ..
                    } => {
                        if depth == 255 {
                            return Err(changed());
                        }
                        let parent = items
                            .get(space, parent_work_item_id)
                            .await?
                            .ok_or_else(changed)?;
                        let parent_run = runs
                            .get(space, original_parent_work_run_id)
                            .await?
                            .ok_or_else(changed)?;
                        verify_reprocess_execution_parent(execution_origin, &parent, &parent_run)?;
                        work = parent;
                        run = parent_run;
                        original_parent_edge = true;
                    }
                },
                WorkProducerOrigin::Child {
                    parent_work_item_id,
                    original_parent_work_run_id,
                    ..
                } => {
                    if depth == 255 {
                        return Err(changed());
                    }
                    let parent = items
                        .get(space, parent_work_item_id)
                        .await?
                        .ok_or_else(changed)?;
                    let parent_run = runs
                        .get(space, original_parent_work_run_id)
                        .await?
                        .ok_or_else(changed)?;
                    verify_parent_origin(&origin, &parent, &parent_run)?;
                    work = parent;
                    run = parent_run;
                    original_parent_edge = true;
                }
                WorkProducerOrigin::Unknown => return Err(changed()),
            }
        }
    }
    drop(controls);
    drop(runs);
    drop(items);
    drop(artifacts);
    Ok(captured)
}

pub(super) fn check_tx(
    tx: &EmbeddedMutation<'_>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let Some(progress) = &requirements.progress else {
        return Ok(());
    };
    let space = &requirements.manifest.space_id;
    let lookup_key = format!(
        "{COMPLETION_TABLE}:{}",
        published_completion_key(space, &requirements.manifest.publication.source_id)
    );
    let lookup = tx
        .scan_extension_rows(COMPLETION_TABLE)?
        .into_iter()
        .find(|(key, _)| *key == lookup_key);
    match (lookup, &progress.previous_lookup) {
        (None, None) => {}
        (Some((_, row)), Some(expected))
            if row.get("space_id").and_then(serde_json::Value::as_str) == Some(space.as_str())
                && row.get("source_id").and_then(serde_json::Value::as_str)
                    == Some(
                        requirements
                            .manifest
                            .publication
                            .source_id
                            .to_string()
                            .as_str(),
                    )
                && row.get("payload").and_then(serde_json::Value::as_str)
                    == Some(expected.as_str()) => {}
        _ => return Err(conflict("source_progress_lookup_changed")),
    }
    for expected in &progress.rows {
        let current = expected.current(tx)?.ok_or_else(changed)?;
        if current.fingerprint()? != expected.fingerprint()? {
            return Err(changed());
        }
    }
    for origin in &progress.origins {
        if read_control_payload_tx(tx, "work_producer_origin", &origin.key, space)?.as_ref()
            != Some(&origin.payload)
        {
            return Err(changed());
        }
    }
    if let Some(expected) = &progress.decision {
        if tx
            .get::<IngestDecision>(space, &expected.base.id, false)?
            .as_ref()
            != Some(expected)
        {
            return Err(changed());
        }
    }
    Ok(())
}

pub(super) fn append_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let Some(progress) = &requirements.progress else {
        return Ok(());
    };
    let space = Value::from(requirements.manifest.space_id.clone());
    if bindings.get("space").is_some_and(|bound| bound != &space) {
        return Err(changed());
    }
    bindings.insert("space".into(), space);
    bindings.insert(
        "sf_progress_previous_lookup_id".into(),
        control_thing(
            COMPLETION_TABLE,
            &published_completion_key(
                &requirements.manifest.space_id,
                &requirements.manifest.publication.source_id,
            ),
        )
        .into(),
    );
    bindings.insert(
        "sf_progress_previous_lookup".into(),
        progress
            .previous_lookup
            .as_ref()
            .map_or(Value::None, |payload| payload.clone().into()),
    );
    bindings.insert(
        "sf_progress_lookup_source".into(),
        requirements
            .manifest
            .publication
            .source_id
            .to_string()
            .into(),
    );
    sql.push_str("LET $sf_progress_previous_row = SELECT * FROM ONLY $sf_progress_previous_lookup_id;\n\
        IF $sf_progress_previous_lookup = NONE { IF $sf_progress_previous_row != NONE { THROW 'publication:source_progress_lookup_changed'; }; } ELSE {\n\
            IF $sf_progress_previous_row = NONE OR $sf_progress_previous_row.space_id != $sf_space OR $sf_progress_previous_row.source_id != $sf_progress_lookup_source OR $sf_progress_previous_row.payload != $sf_progress_previous_lookup { THROW 'publication:source_progress_lookup_changed'; };\n\
        };\n");
    for (index, row) in progress.rows.iter().enumerate() {
        crate::canonical_commit::append_row_guard(
            sql,
            bindings,
            &format!("sf_progress_row_{index}"),
            row,
            Some(row),
        )?;
    }
    for (index, origin) in progress.origins.iter().enumerate() {
        let prefix = format!("sf_progress_origin_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            control_thing("work_producer_origin", &origin.key).into(),
        );
        bindings.insert(format!("{prefix}_payload"), origin.payload.clone().into());
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id; IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.payload != ${prefix}_payload {{ THROW 'publication:source_original_ingest_decision_changed'; }}; UPDATE ${prefix}_id SET read_nonce=<string>rand::uuid();");
    }
    if let Some(decision) = &progress.decision {
        bindings.insert(
            "sf_progress_decision_id".into(),
            crate::surreal::thing_from_record_id(&decision.base.id).into(),
        );
        bindings.insert(
            "sf_progress_decision_before".into(),
            crate::canonical_commit::native_expected_fields(
                crate::IngestDecisionRepository::ingest_decision_to_value(decision)?,
            ),
        );
        sql.push_str("LET $sf_progress_decision = SELECT * FROM ONLY $sf_progress_decision_id; IF $sf_progress_decision = NONE OR $sf_progress_decision.space_id != $sf_space { THROW 'publication:source_original_ingest_decision_changed'; }; FOR $sf_progress_field IN object::entries($sf_progress_decision_before) { IF $sf_progress_decision[$sf_progress_field[0]] != $sf_progress_field[1] { THROW 'publication:source_original_ingest_decision_changed'; }; }; UPDATE $sf_progress_decision_id SET canonical_read_nonce=<string>rand::uuid();\n");
    }
    Ok(())
}

pub(super) const fn completed_mode(mode: IngestMode) -> SourceLifecycleStatus {
    match mode {
        IngestMode::Skip => SourceLifecycleStatus::Skipped,
        IngestMode::Metadata => SourceLifecycleStatus::MetadataReady,
        IngestMode::Retrieval => SourceLifecycleStatus::RetrievalReady,
        IngestMode::Graph => SourceLifecycleStatus::GraphReady,
    }
}

impl SourcePublicationRequirements {
    /// Actual original decision bound to the completed publication, or certified absence.
    ///
    /// # Errors
    /// Refuses preparation without a captured original progress proof.
    pub fn published_ingest_decision(&self) -> StorageResult<Option<Link<IngestDecision>>> {
        Ok(self
            .progress
            .as_ref()
            .ok_or_else(changed)?
            .decision
            .as_ref()
            .map(|row| Link::new(row.base.id.clone())))
    }
}

impl SourcePublicationManifestRepository {
    /// Project only progress certified by this published revision's original writer.
    /// Content, base fields and security remain the immutable accepted snapshot.
    #[allow(
        clippy::too_many_lines,
        reason = "Published progress is derived from the current head and exact operation and manifest receipts; preserve all consistency checks before the projection is returned."
    )]
    pub(crate) async fn project_published_source(
        &self,
        head: &SourceHead,
        accepted: &SourceRevision,
    ) -> StorageResult<Option<Source>> {
        let space = &head.space_id;
        if head.lifecycle != SourceVisibility::Active
            || head.source_id != accepted.source_id
            || accepted.space_id != *space
            || accepted.source.base.space_id != *space
            || accepted.source.base.id != head.source_id
            || head.published_revision.as_deref() != Some(accepted.revision_id.as_str())
        {
            return Ok(None);
        }
        let publications = SourcePublicationRepository::new(self.client.clone());
        let Some(lookup) = publications
            .read_control::<SourcePublicationCompletion>(
                COMPLETION_TABLE,
                &published_completion_key(space, &head.source_id),
                space,
            )
            .await?
        else {
            return Ok(None);
        };
        let publication = &lookup.publication;
        if lookup.space_id != *space
            || publication.source_id != head.source_id
            || publication.revision_id != accepted.revision_id
        {
            return Ok(None);
        }
        let Some(manifest) = self.get(space, publication).await? else {
            return Ok(None);
        };
        manifest.validate()?;
        let writer = manifest.historical_adoption.as_ref().map_or_else(
            || format!("source-publication:{}", manifest_key(space, publication)),
            |proof| proof.commit_id(&manifest),
        );
        let Some(receipt) = publications
            .read_control::<CanonicalCommitReceipt>(
                "source_commit_receipt",
                &stable_key(&[space, &writer]),
                space,
            )
            .await?
        else {
            return Ok(None);
        };
        if receipt.space_id != *space
            || receipt.commit_id != writer
            || receipt.publication_authority.as_ref() != Some(publication)
            || publication.source_id != head.source_id
            || publication.revision_id != accepted.revision_id
            || receipt.source_id.as_ref() != Some(&head.source_id)
            || receipt.source_revision.as_ref() != Some(&accepted.revision_id)
            || !receipt.effect_ids.contains(&head.source_id)
        {
            return Ok(None);
        }
        let Some(manifest) = self.get(space, publication).await? else {
            return Ok(None);
        };
        manifest.validate()?;
        let Some(completion) = self.completion(space, publication).await? else {
            return Ok(None);
        };
        // A mutable lookup only identifies an exact original fence. It cannot
        // confer authority independently of the complete immutable certificate.
        if completion != lookup {
            return Ok(None);
        }
        if manifest.historical_adoption.is_some() {
            if completion.manifest_digest != manifest.digest()?
                || completion.commit_id != writer
                || completion.accepted_source_fingerprint.as_ref()
                    != Some(&source_support_fingerprint(accepted)?)
                || receipt.historical_import.as_ref()
                    != Some(
                        &crate::historical_adoption::HistoricalCommitProvenance::for_manifest(
                            &manifest,
                        )?,
                    )
            {
                return Ok(None);
            }
            let operation = publications
                .operation(space, &publication.operation_id)
                .await?;
            if operation.as_ref().is_none_or(|op| {
                !matches!(
                    op.state,
                    OperationState::Processing | OperationState::Completed
                ) || op.source_generation != publication.generation
                    || op.source_id != head.source_id
            }) {
                return Ok(None);
            }
            return Ok(Some(accepted.source.clone()));
        }
        let expected_commit = format!("source-publication:{}", manifest_key(space, publication));
        let Some(progress) = &completion.published_progress else {
            return Ok(None);
        };
        let Some(operation) = publications
            .operation(space, &publication.operation_id)
            .await?
        else {
            return Ok(None);
        };
        drop(publications);
        if manifest.space_id != *space
            || manifest.publication != *publication
            || completion.space_id != *space
            || completion.publication != *publication
            || completion.commit_id != expected_commit
            || receipt.commit_id != expected_commit
            || completion.manifest_digest != manifest.digest()?
            || completion.accepted_source_fingerprint.as_ref()
                != Some(&source_support_fingerprint(accepted)?)
            || progress.mode != manifest.mode
            || operation.space_id != *space
            || operation.source_id != head.source_id
            || operation.operation_id != publication.operation_id
            || operation.source_generation != publication.generation
            || operation.state != OperationState::Completed
            || operation.completed_at.is_none()
            || operation.desired_revision.as_ref() != Some(&accepted.revision_id)
            || operation.published_revision.as_ref() != Some(&accepted.revision_id)
        {
            return Ok(None);
        }
        match (
            &progress.latest_ingest_decision,
            &progress.ingest_decision_fingerprint,
        ) {
            (Some(id), Some(fingerprint)) => {
                if id.table() != TableName::IngestDecision {
                    return Ok(None);
                }
                let Some(decision) = crate::IngestDecisionRepository::new(self.client.clone())
                    .get(space, id)
                    .await?
                else {
                    return Ok(None);
                };
                if decision.base.deleted_at.is_some()
                    || decision.base.space_id != *space
                    || decision.source.id() != &head.source_id
                    || decision.decision != progress.mode
                    || source_support_fingerprint(&decision)? != *fingerprint
                {
                    return Ok(None);
                }
            }
            (None, None) => {}
            _ => return Ok(None),
        }
        let mut source = accepted.source.clone();
        source.current_ingest_mode = Some(progress.mode);
        source.lifecycle_status = completed_mode(progress.mode);
        source.latest_ingest_decision = progress.latest_ingest_decision.clone().map(Link::new);
        Ok(Some(source))
    }
}
