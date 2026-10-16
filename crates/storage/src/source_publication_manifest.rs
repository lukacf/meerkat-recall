//! Immutable, closed expectations for whole-source publication.
//!
//! A manifest is recorded before extraction fanout. Neither an empty queue nor
//! the currently finished subset can extend or complete it. SourceHead remains
//! the only desired/published authority; these rows describe its required work.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use elephant_embeddings::{level_bucket, PartitionKey};
use elephant_types::provider_binding::{
    ModelOperation, ResolvedSpaceConfig, SpaceConfigToken, VectorPartition,
};
use elephant_types::publication::{AttemptFence, PublicationFence, SourceHead, SourceVisibility};
use elephant_types::records::doc_node::NodeRole;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::work::{
    ArtifactStatus, WorkArtifact, WorkItem, WorkItemArchive, WorkItemStatus, WorkRun,
    WorkRunArchive,
};
use elephant_types::records::{Doc, DocNode, DocRevision, NodeEmbedding, SpaceConfig};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surrealdb::sql::Value;
use uuid::Uuid;

use crate::source_publication::{
    conflict, control_thing, control_value, decode_payload, encode_payload, head_key, integrity,
    publication_error, read_control_tx, stable_key, write_control_tx, SourcePublicationRepository,
    HEAD_TABLE,
};
use crate::standing_watch_repository::{
    PreparedStandingSourceAuthority, PreparedStandingWatchAuthority,
};
use crate::{EmbeddedMutation, Repository, StorageClient, StorageResult};

#[path = "source_publication_embedding.rs"]
mod embedding;
pub use embedding::SourceEmbeddingWriteAuthority;
pub(crate) use embedding::{append_embedding_write_sql, check_embedding_write_tx};

#[path = "source_publication_inputs.rs"]
mod inputs;

#[path = "source_publication_render.rs"]
mod render;

#[path = "source_publication_progress.rs"]
mod progress;
pub use progress::PublishedSourceProgress;

#[path = "source_publication_replan.rs"]
mod replan;
pub use replan::{source_bundle_id_for_plan, source_embedding_work_id_for_plan, source_plan_id};

pub(crate) const MANIFEST_TABLE: &str = "source_publication_manifest";
pub(crate) const REGISTRATION_TABLE: &str = "source_bundle_registration";
pub(crate) const COMPLETION_TABLE: &str = "source_publication_completion";
pub(crate) const DOCUMENT_TABLE: &str = "source_document_mapping";
pub(crate) const REVISION_TABLE: &str = "source_revision_mapping";

/// Immutable binding of rendered evidence to the accepted input, never to a
/// subsequently observed source head. Each revision has a separate document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDocumentMapping {
    /// Owning named space.
    pub space_id: String,
    /// Immutable document projection for this source revision.
    pub doc_id: RecordId,
    /// Immutable rendered text revision.
    pub doc_revision_id: RecordId,
    /// Original acceptance authority.
    pub publication: PublicationFence,
}

/// One exact planned extraction output. IDs are chosen before work is enqueued.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedSourceBundle {
    /// Deterministic ID of the future validated commit bundle.
    pub artifact_id: RecordId,
    /// Node used as extraction and evidence identity.
    pub node_id: RecordId,
    /// All nodes covered by a coalesced extraction selection.
    pub node_ids: Vec<RecordId>,
    /// Exact rendered revision, distinct from the external source revision.
    pub revision_id: RecordId,
    /// Selected immutable text range.
    pub span_start: u32,
    /// Exclusive end of the selected text range.
    pub span_end: u32,
}

/// A real indexed vector is required for this exact evidence node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequiredSourceEmbedding {
    /// Embeddable node, including summaries when selected by the embed worker.
    pub node_id: RecordId,
    /// Its immutable rendered revision.
    pub revision_id: RecordId,
    /// Exact security/profile partition; computed from the captured node.
    pub partition_table: String,
}

/// Closed requirements. Persist this once before any extraction child is
/// enqueued. A conflicting retry cannot shrink, grow, or replace the set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePublicationManifest {
    /// Explicit offline import proof; absent for every worker-produced plan.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub historical_adoption: Option<crate::historical_adoption::HistoricalAdoptionProof>,
    /// Owning named space.
    pub space_id: String,
    /// Original acceptance authority.
    pub publication: PublicationFence,
    /// Explicit selected effects; metadata/retrieval do not require extraction.
    pub mode: IngestMode,
    /// Captured approved configuration, excluding mutable coverage counters.
    pub configuration: SpaceConfigToken,
    /// Exact vector identity used by both embedding and search.
    pub embedding_profile: Option<VectorPartition>,
    /// Exact document-to-source revision bindings.
    pub documents: Vec<SourceDocumentMapping>,
    /// Validated source metadata bundles selected before the depth transition.
    pub metadata_artifact_ids: Vec<RecordId>,
    /// Durable extraction plans, including a plan with zero selected nodes.
    pub planning_artifact_ids: Vec<RecordId>,
    /// Exact future validated extraction bundles.
    pub bundles: Vec<ExpectedSourceBundle>,
    /// Exact required index coverage; unembeddable markers do not satisfy it.
    pub embeddings: Vec<RequiredSourceEmbedding>,
    /// Exact embedding job per rendered revision, even when it has no nodes.
    pub embedding_work_item_ids: Vec<RecordId>,
}

/// A durable completion certificate written by the canonical transaction only.
/// This is diagnostic evidence, not a second operation-state authority.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePublicationCompletion {
    /// Named space.
    pub space_id: String,
    /// Original acceptance authority.
    pub publication: PublicationFence,
    /// Identity of the immutable expectation set.
    pub manifest_digest: String,
    /// Atomic canonical receipt identity.
    pub commit_id: String,
    /// Exact rendered domain rows captured and guarded by the original finalizer.
    /// Older certificates without these proofs cannot authorize rendered reads.
    #[serde(default)]
    pub rendered_revision_fingerprints: BTreeMap<RecordId, String>,
    /// Exact document projections captured alongside the rendered revisions.
    #[serde(default)]
    pub document_fingerprints: BTreeMap<RecordId, String>,
    /// Fingerprint of the original immutable accepted source snapshot.
    #[serde(default)]
    pub accepted_source_fingerprint: Option<String>,
    /// Progress certified from this publication's original selected execution.
    /// Missing older proof is not equivalent to certified decision absence.
    #[serde(default)]
    pub published_progress: Option<PublishedSourceProgress>,
    /// Current embedding coverage, separate from the original historical import.
    #[serde(default)]
    pub historical_embedding_coverage:
        Option<crate::historical_embedding_coverage::HistoricalEmbeddingCoverage>,
    /// Time the canonical transaction completed all required effects.
    pub completed_at: DateTime<Utc>,
}

/// Durable acknowledgement of one staged bundle by its original commit claim.
/// Queue records remain the only authority for whether that claim completed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct SourceBundleRegistration {
    space_id: String,
    publication: PublicationFence,
    artifact_id: RecordId,
    attempt: AttemptFence,
}

/// Sealed input to the canonical transaction. Only a persisted manifest can
/// construct this proof; readiness is rechecked within that transaction.
#[derive(Debug, Clone)]
pub struct SourcePublicationRequirements {
    manifest: SourcePublicationManifest,
    head: SourceHead,
    revisions: Vec<DocRevision>,
    documents: Vec<Doc>,
    accepted_source_control: Option<String>,
    original_attempt: Option<AttemptFence>,
    progress: Option<progress::PreparedProgress>,
}

impl SourcePublicationRequirements {
    /// Exact durable expected set.
    #[must_use]
    pub const fn manifest(&self) -> &SourcePublicationManifest {
        &self.manifest
    }

    /// Stable identity for replay lookup before semantic preparation.
    #[must_use]
    pub fn commit_id(&self) -> String {
        format!(
            "source-publication:{}",
            manifest_key(&self.manifest.space_id, &self.manifest.publication)
        )
    }
}

fn set_operation_plan_tx(
    tx: &mut EmbeddedMutation<'_>,
    manifest: SourcePublicationManifest,
    standing_watch: Option<&PreparedStandingSourceAuthority>,
) -> StorageResult<SourcePublicationManifest> {
    if let Some(authority) = standing_watch {
        authority.check_tx(tx)?;
    }
    check_source_tx(tx, &manifest)?;
    check_configuration_tx(tx, &manifest)?;
    let key = manifest_key(&manifest.space_id, &manifest.publication);
    if let Some(existing) =
        read_control_tx::<SourcePublicationManifest>(tx, MANIFEST_TABLE, &key, &manifest.space_id)?
    {
        if existing != manifest {
            return Err(conflict("source_manifest_conflict"));
        }
        return Ok(existing);
    }
    check_plan_tx(tx, &manifest)?;
    for id in &manifest.planning_artifact_ids {
        let mut plan = tx
            .get::<WorkArtifact>(&manifest.space_id, id, false)?
            .ok_or_else(|| incomplete("extraction_plan_missing"))?;
        if plan.status == ArtifactStatus::Draft {
            plan.status = ArtifactStatus::Validated;
            plan.validated_at = Some(Utc::now());
            plan.base.updated_at = Utc::now();
            tx.replace(plan)?;
        }
    }
    for mapping in &manifest.documents {
        write_mapping_tx(tx, mapping)?;
    }
    write_control_tx(
        tx,
        MANIFEST_TABLE,
        &key,
        &manifest.space_id,
        &manifest.publication.source_id,
        &manifest,
    )?;
    tx.advance_canonical_epoch(&manifest.space_id)?;
    Ok(manifest)
}

/// Repository for durable fanout and historical source/evidence mapping.
#[derive(Debug, Clone)]
pub struct SourcePublicationManifestRepository {
    client: StorageClient,
}

impl SourcePublicationManifestRepository {
    /// Bind to either supported storage backend.
    #[must_use]
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Persist a closed plan and all mappings atomically before child enqueue.
    /// Identical retries return the original set; conflicting retries fail.
    pub async fn set_operation_plan(
        &self,
        mut manifest: SourcePublicationManifest,
    ) -> StorageResult<SourcePublicationManifest> {
        manifest.normalize();
        manifest.validate()?;
        if manifest.historical_adoption.is_some() {
            return Err(integrity(
                "historical adoption requires exclusive offline import",
            ));
        }
        let mut standing_watch = Box::pin(
            PreparedStandingWatchAuthority::capture_for_source_operation(
                &self.client,
                &manifest.space_id,
                &manifest.publication,
            ),
        )
        .await?;
        Box::pin(inputs::authorize_inputs(
            self,
            &mut standing_watch,
            &manifest.space_id,
            &manifest.publication,
            Some(&manifest),
            None,
        ))
        .await?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| set_operation_plan_tx(tx, manifest, standing_watch.as_ref()))
                .await;
        }

        // Shared guard builder keeps seal-time and completion-time checks aligned.
        let head = SourcePublicationRepository::new(self.client.clone())
            .head(&manifest.space_id, &manifest.publication.source_id)
            .await?
            .filter(|head| head_matches(head, &manifest))
            .ok_or_else(|| conflict("stale_source_publication"))?;
        let requirements = SourcePublicationRequirements {
            original_attempt: None,
            progress: None,
            revisions: self.load_revisions(&manifest).await?,
            documents: self.load_render_documents(&manifest).await?,
            accepted_source_control: Some(self.load_render_source(&manifest).await?),
            manifest: manifest.clone(),
            head,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        if let Some(authority) = &standing_watch {
            authority.append_guard_sql(&mut sql, &mut bindings)?;
        }
        append_identity_sql(&mut sql, &mut bindings, &requirements)?;
        sql.push_str("LET $sf_existing = SELECT * FROM ONLY $sf_manifest_id;\n\
            IF $sf_existing != NONE AND $sf_existing.payload != $sf_manifest_payload { THROW 'publication:source_manifest_conflict'; };\n");
        append_plan_sql(&mut sql, &mut bindings, &requirements)?;
        sql.push_str("UPDATE $sf_plan_ids SET status = 'validated', validated_at = time::now(), updated_at = time::now() WHERE status = 'draft';\n");
        append_mapping_writes_sql(&mut sql, &mut bindings, &manifest)?;
        sql.push_str("IF $sf_existing = NONE { CREATE $sf_manifest_id SET space_id = $sf_space, source_id = $sf_source_string, payload = $sf_manifest_payload; };\nCOMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(manifest)
    }

    /// Durably register a validated bundle using the original commit-stage claim.
    /// This neither completes the queue item nor publishes any canonical effects.
    #[allow(
        clippy::too_many_lines,
        reason = "Bundle admission compares the sealed manifest and exact artifact selection before its guarded backend write; retain both full transactional branches."
    )]
    pub async fn register_bundle(
        &self,
        space: &str,
        publication: &PublicationFence,
        artifact_id: &RecordId,
        attempt: &AttemptFence,
    ) -> StorageResult<()> {
        if attempt.space_id != space || attempt.generation == 0 || attempt.lease_token.is_empty() {
            return Err(conflict("source_registration_attempt_mismatch"));
        }
        let mut standing_watch = Box::pin(
            PreparedStandingWatchAuthority::capture_for_source_operation(
                &self.client,
                space,
                publication,
            ),
        )
        .await?;
        let context = self
            .registration_context(space, publication, attempt)
            .await?;
        let manifest = context.manifest.clone();
        if let Some(manifest) = &manifest {
            manifest.validate()?;
            if manifest.space_id != space || manifest.publication != *publication {
                return Err(conflict("source_manifest_identity_mismatch"));
            }
        }
        Box::pin(inputs::authorize_inputs(
            self,
            &mut standing_watch,
            space,
            publication,
            manifest.as_ref(),
            Some(artifact_id),
        ))
        .await?;
        let head = SourcePublicationRepository::new(self.client.clone())
            .head(space, &publication.source_id)
            .await?
            .filter(|head| {
                head.space_id == space
                    && head.source_id == publication.source_id
                    && head.generation == publication.generation
                    && head.desired_revision.as_deref() == Some(publication.revision_id.as_str())
                    && head.operation_id == publication.operation_id
                    && head.lifecycle == SourceVisibility::Active
            })
            .ok_or_else(|| conflict("stale_source_publication"))?;
        let registration = SourceBundleRegistration {
            space_id: space.into(),
            publication: publication.clone(),
            artifact_id: artifact_id.clone(),
            attempt: attempt.clone(),
        };
        let key = registration_key(space, publication, artifact_id);
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    if let Some(authority) = &standing_watch {
                        authority.check_tx(tx)?;
                    }
                    crate::work_attempt_repository::check_attempt_fence(tx, attempt, Utc::now())?;
                    if read_control_tx::<SourcePublicationManifest>(
                        tx,
                        MANIFEST_TABLE,
                        &manifest_key(space, publication),
                        space,
                    )? != context.observed
                    {
                        return Err(conflict("source_manifest_conflict"));
                    }
                    if let Some(configuration) = &context.configuration {
                        replan::check_current_configuration_tx(tx, configuration)?;
                        if manifest.is_none()
                            && read_control_tx::<SourcePublicationCompletion>(
                                tx,
                                COMPLETION_TABLE,
                                &manifest_key(space, publication),
                                space,
                            )?
                            .is_some()
                        {
                            return Err(conflict("source_plan_already_completed"));
                        }
                    }
                    let current = read_control_tx::<SourceHead>(
                        tx,
                        HEAD_TABLE,
                        &head_key(space, &publication.source_id),
                        space,
                    )?;
                    if current.as_ref() != Some(&head)
                        || tx
                            .get::<elephant_types::records::Source>(
                                space,
                                &publication.source_id,
                                false,
                            )?
                            .is_none()
                    {
                        return Err(conflict("stale_source_publication"));
                    }
                    if let Some(manifest) = &manifest {
                        check_configuration_tx(tx, manifest)?;
                    }
                    let artifact = tx
                        .get::<WorkArtifact>(space, artifact_id, false)?
                        .ok_or_else(|| incomplete("expected_bundle_missing"))?;
                    check_staging_bundle(manifest.as_ref(), space, publication, &artifact)?;
                    check_origin_tx(tx, space, &artifact)?;
                    let item = tx
                        .get::<WorkItem>(space, &attempt.work_item_id, false)?
                        .ok_or_else(|| incomplete("registration_work_missing"))?;
                    check_registration_work(&item, publication, &artifact)?;
                    if let Some(configuration) = &context.configuration {
                        replan::check_work_plan(&item, publication, configuration, true)?;
                        let origin = origin_run_tx(tx, space, artifact.work_run.id())?;
                        let producer = completed_item_tx(tx, space, origin.work_item.id())?;
                        replan::check_work_plan(&producer, publication, configuration, true)?;
                    }
                    if let Some(old) = read_control_tx::<SourceBundleRegistration>(
                        tx,
                        REGISTRATION_TABLE,
                        &key,
                        space,
                    )? {
                        check_registration_retry(&old, &registration)?;
                    }
                    write_control_tx(
                        tx,
                        REGISTRATION_TABLE,
                        &key,
                        space,
                        &publication.source_id,
                        &registration,
                    )?;
                    tx.advance_canonical_epoch(space)?;
                    Ok(())
                })
                .await;
        }
        // Bind the observed artifact contents. Validation and the receipt write
        // share the same transaction; concurrent artifact replacement conflicts.
        let artifact = crate::WorkArtifactRepository::new(self.client.clone())
            .get(space, artifact_id)
            .await?
            .ok_or_else(|| incomplete("expected_bundle_missing"))?;
        check_staging_bundle(manifest.as_ref(), space, publication, &artifact)?;
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        if let Some(authority) = &standing_watch {
            authority.append_guard_sql(&mut sql, &mut bindings)?;
        }
        if let Some(manifest) = manifest {
            append_identity_sql(
                &mut sql,
                &mut bindings,
                &SourcePublicationRequirements {
                    original_attempt: None,
                    progress: None,
                    manifest,
                    head,
                    revisions: Vec::new(),
                    documents: Vec::new(),
                    accepted_source_control: None,
                },
            )?;
        } else {
            bind_json(&mut bindings, "sf_space", space)?;
            bind_json(
                &mut bindings,
                "sf_source_string",
                publication.source_id.to_string(),
            )?;
            bind_json(&mut bindings, "sf_fence", publication)?;
            bind_json(&mut bindings, "sf_head_payload", encode_payload(&head)?)?;
            bindings.insert(
                "sf_head_id".into(),
                control_thing(HEAD_TABLE, &head_key(space, &publication.source_id)).into(),
            );
            bindings.insert(
                "sf_source_id".into(),
                crate::surreal::thing_from_record_id(&publication.source_id).into(),
            );
            sql.push_str("LET $sf_head_row = SELECT * FROM ONLY $sf_head_id;\n\
                LET $sf_source = SELECT * FROM ONLY $sf_source_id;\n\
                IF $sf_head_row = NONE OR $sf_head_row.space_id != $sf_space OR $sf_head_row.payload != $sf_head_payload OR $sf_source = NONE OR $sf_source.space_id != $sf_space OR $sf_source.deleted_at != NONE { THROW 'publication:stale_source_publication'; };\n\
                fn::recall_physical_read_fence($sf_head_id);\n\
                fn::recall_reference_guard({space_id:$sf_space}, false);\n");
        }
        crate::work_attempt_repository::append_attempt_fence(&mut sql, &mut bindings, attempt)?;
        bindings.insert(
            "sf_registration_manifest_id".into(),
            control_thing(MANIFEST_TABLE, &manifest_key(space, publication)).into(),
        );
        bindings.insert(
            "sf_registration_observed".into(),
            context
                .observed
                .as_ref()
                .map_or(Ok(Value::None), |manifest| {
                    encode_payload(manifest).map(Value::from)
                })?,
        );
        sql.push_str("LET $sf_registration_current = SELECT * FROM ONLY $sf_registration_manifest_id;\n\
            IF $sf_registration_current.payload != $sf_registration_observed { THROW 'publication:source_manifest_conflict'; };\n\
            fn::recall_physical_read_fence($sf_registration_manifest_id);\n");
        if let Some(configuration) = &context.configuration {
            replan::append_current_configuration_sql(&mut sql, &mut bindings, configuration)?;
            bind_json(&mut bindings, "sf_plan_configuration", configuration)?;
            bind_json(
                &mut bindings,
                "sf_plan_version",
                source_plan_id(publication, configuration),
            )?;
            replan::append_work_plan_sql(&mut sql, "fenced_work", "true");
            if context.manifest.is_none() {
                bindings.insert(
                    "sf_early_completion_id".into(),
                    control_thing(COMPLETION_TABLE, &manifest_key(space, publication)).into(),
                );
                sql.push_str("IF (SELECT * FROM ONLY $sf_early_completion_id) != NONE { THROW 'publication:source_plan_already_completed'; };\n");
            }
        }
        bindings.insert(
            "sf_register_artifact_id".into(),
            crate::surreal::thing_from_record_id(artifact_id).into(),
        );
        bind_json(&mut bindings, "sf_register_data", artifact.data.clone())?;
        bind_json(
            &mut bindings,
            "sf_register_run_string",
            artifact.work_run.id().to_string(),
        )?;
        bindings.insert(
            "sf_register_run".into(),
            crate::surreal::thing_from_record_id(artifact.work_run.id()).into(),
        );
        bindings.insert(
            "sf_registration_id".into(),
            control_thing(REGISTRATION_TABLE, &key).into(),
        );
        bindings.insert(
            "sf_registration_value".into(),
            control_value(space, &publication.source_id, &registration)?,
        );
        sql.push_str("LET $sf_register_artifact = SELECT * FROM ONLY $sf_register_artifact_id;\n\
            IF $sf_register_artifact = NONE OR $sf_register_artifact.space_id != $sf_space OR $sf_register_artifact.deleted_at != NONE OR $sf_register_artifact.data != $sf_register_data OR $sf_register_artifact.work_run != $sf_register_run OR $sf_register_artifact.status NOT IN ['validated','committed'] OR $sf_register_artifact.validated_at = NONE OR ($sf_register_artifact.validation_errors != NONE AND array::len($sf_register_artifact.validation_errors) > 0) { THROW 'publication:source_bundle_changed'; };\n\
            IF $fenced_work.kind != 'commit_artifacts' OR $fenced_work.payload._publication != $sf_fence OR $fenced_work.payload.work_run_id != $sf_register_run_string { THROW 'publication:source_registration_work_mismatch'; };\n");
        append_origin_sql(&mut sql, "sf_register_artifact");
        if context.configuration.is_some() {
            replan::append_work_plan_sql(&mut sql, "sf_register_artifact_origin_item", "true");
        }
        sql.push_str("LET $sf_registration_old = SELECT * FROM ONLY $sf_registration_id;\n\
            IF $sf_registration_old != NONE AND ($sf_registration_old.space_id != $sf_space OR $sf_registration_old.state.publication != $sf_fence OR $sf_registration_old.state.artifact_id != $sf_registration_value.state.artifact_id OR $sf_registration_old.state.attempt.work_item_id != $sf_registration_value.state.attempt.work_item_id OR $sf_registration_old.state.attempt.generation > $sf_registration_value.state.attempt.generation OR ($sf_registration_old.state.attempt.generation = $sf_registration_value.state.attempt.generation AND $sf_registration_old.payload != $sf_registration_value.payload)) { THROW 'publication:source_registration_conflict'; };\n\
            UPSERT $sf_registration_id CONTENT $sf_registration_value;\nCOMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    /// Recover a sealed plan after its producing attempt crashed before queue
    /// acknowledgement.
    ///
    /// Only the same planner work item may adopt those exact
    /// durable selections; this does not certify the new run's eventual success.
    #[allow(
        clippy::too_many_lines,
        reason = "Resuming a plan compares the current publication, configuration, and attempt before updating its durable state on either backend; preserve the transaction boundary."
    )]
    pub async fn resume_plan_attempt(
        &self,
        space: &str,
        publication: &PublicationFence,
        attempt: &AttemptFence,
        new_work_run_id: &RecordId,
    ) -> StorageResult<()> {
        if attempt.space_id != space || new_work_run_id.table() != TableName::WorkRun {
            return Err(conflict("source_plan_attempt_mismatch"));
        }
        let mut standing_watch = Box::pin(
            PreparedStandingWatchAuthority::capture_for_source_operation(
                &self.client,
                space,
                publication,
            ),
        )
        .await?;
        let requirements = self
            .requirements_without_progress(space, publication)
            .await?;
        let manifest = requirements.manifest();
        Box::pin(inputs::authorize_inputs(
            self,
            &mut standing_watch,
            space,
            publication,
            Some(manifest),
            None,
        ))
        .await?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    if let Some(authority) = &standing_watch {
                        authority.check_tx(tx)?;
                    }
                    crate::work_attempt_repository::check_attempt_fence(tx, attempt, Utc::now())?;
                    check_source_tx(tx, manifest)?;
                    check_configuration_tx(tx, manifest)?;
                    check_plan_tx(tx, manifest)?;
                    let new_run = tx
                        .get::<WorkRun>(space, new_work_run_id, false)?
                        .ok_or_else(|| incomplete("planner_run_missing"))?;
                    let item = tx
                        .get::<WorkItem>(space, &attempt.work_item_id, false)?
                        .ok_or_else(|| incomplete("planner_work_missing"))?;
                    if new_run.work_item.id() != &attempt.work_item_id
                        || new_run.attempt != item.attempts
                        || item.kind != "plan_extraction"
                        || item.payload.get("_publication")
                            != Some(
                                &serde_json::to_value(publication)
                                    .map_err(|error| integrity(&error.to_string()))?,
                            )
                        || new_run.completed_at.is_some()
                    {
                        return Err(conflict("source_plan_attempt_mismatch"));
                    }
                    replan::check_work_plan(
                        &item,
                        publication,
                        &manifest.configuration,
                        replan::replanned_tx(tx, manifest)?,
                    )?;
                    let mut adopted = false;
                    for id in &manifest.planning_artifact_ids {
                        let mut plan = tx
                            .get::<WorkArtifact>(space, id, false)?
                            .ok_or_else(|| incomplete("extraction_plan_missing"))?;
                        let original = origin_run_tx(tx, space, plan.work_run.id())?;
                        if original.work_item.id() != &attempt.work_item_id {
                            continue;
                        }
                        if plan.status != ArtifactStatus::Validated || plan.validated_at.is_none() {
                            return Err(conflict("source_plan_already_completed"));
                        }
                        adopted = true;
                        plan.work_run = elephant_types::Link::new(new_work_run_id.clone());
                        plan.base.updated_at = Utc::now();
                        tx.replace(plan)?;
                    }
                    if !adopted {
                        return Err(conflict("source_plan_attempt_mismatch"));
                    }
                    tx.advance_canonical_epoch(space)?;
                    Ok(())
                })
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        if let Some(authority) = &standing_watch {
            authority.append_guard_sql(&mut sql, &mut bindings)?;
        }
        append_identity_sql(&mut sql, &mut bindings, &requirements)?;
        append_plan_sql(&mut sql, &mut bindings, &requirements)?;
        crate::work_attempt_repository::append_attempt_fence(&mut sql, &mut bindings, attempt)?;
        bindings.insert(
            "sf_new_plan_run_id".into(),
            crate::surreal::thing_from_record_id(new_work_run_id).into(),
        );
        sql.push_str("LET $sf_new_plan_run = SELECT * FROM ONLY $sf_new_plan_run_id;\n\
            IF $sf_new_plan_run = NONE OR $sf_new_plan_run.space_id != $sf_space OR $sf_new_plan_run.deleted_at != NONE OR $sf_new_plan_run.work_item != $attempt_work_item OR $sf_new_plan_run.attempt != $fenced_work.attempts OR $sf_new_plan_run.completed_at != NONE OR $fenced_work.kind != 'plan_extraction' OR $fenced_work.payload._publication != $sf_fence { THROW 'publication:source_plan_attempt_mismatch'; };\n");
        replan::append_replan_scope_sql(&mut sql, &mut bindings, manifest)?;
        replan::append_work_plan_sql(&mut sql, "fenced_work", "$sf_replanned");
        for (index, id) in manifest.planning_artifact_ids.iter().enumerate() {
            let prefix = format!("sf_resume_plan_{index}");
            bindings.insert(
                format!("{prefix}_id"),
                crate::surreal::thing_from_record_id(id).into(),
            );
            let _ = writeln!(sql, "LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;");
            let run = format!("{prefix}_run");
            append_history_sql(&mut sql, &run, &format!("${prefix}.work_run"), "work_run");
            let _ = writeln!(sql,"IF ${run}.work_item = $attempt_work_item {{\n\
                IF ${prefix}.status != 'validated' OR ${prefix}.validated_at = NONE {{ THROW 'publication:source_plan_already_completed'; }};\n\
                UPDATE ${prefix}_id SET work_run = $sf_new_plan_run_id, updated_at = time::now();\n\
            }};");
        }
        sql.push_str("LET $sf_adopted_plan_ids = SELECT VALUE id FROM $sf_plan_ids WHERE work_run = $sf_new_plan_run_id;\n\
            IF array::len($sf_adopted_plan_ids) = 0 { THROW 'publication:source_plan_attempt_mismatch'; };\nCOMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    /// Read only the exact captured acceptance; never substitute a latest head.
    pub async fn get(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Option<SourcePublicationManifest>> {
        SourcePublicationRepository::new(self.client.clone())
            .read_control(MANIFEST_TABLE, &manifest_key(space, publication), space)
            .await
    }

    /// Load a sealed proof for the canonical commit. Missing plan is incomplete.
    pub async fn requirements(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<SourcePublicationRequirements> {
        let mut requirements = self
            .requirements_without_progress(space, publication)
            .await?;
        requirements.progress = Some(
            Box::pin(progress::capture(
                &self.client,
                &requirements.manifest,
                &requirements.head,
            ))
            .await?,
        );
        Ok(requirements)
    }

    // Planner recovery and embedding can run before the metadata producer has
    // finished. They need immutable input identity, not a completion certificate.
    async fn requirements_without_progress(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<SourcePublicationRequirements> {
        let manifest = self
            .get(space, publication)
            .await?
            .ok_or_else(|| incomplete("manifest_missing"))?;
        manifest.validate()?;
        if manifest.historical_adoption.is_some() {
            return Err(integrity(
                "historical adoption is not a worker finalization plan",
            ));
        }
        if manifest.space_id != space || manifest.publication != *publication {
            return Err(conflict("source_manifest_identity_mismatch"));
        }
        let head = SourcePublicationRepository::new(self.client.clone())
            .head(space, &publication.source_id)
            .await?
            .filter(|head| head_matches(head, &manifest))
            .ok_or_else(|| conflict("stale_source_publication"))?;
        let revisions = self.load_revisions(&manifest).await?;
        Ok(SourcePublicationRequirements {
            original_attempt: None,
            progress: None,
            documents: self.load_render_documents(&manifest).await?,
            accepted_source_control: Some(self.load_render_source(&manifest).await?),
            manifest,
            head,
            revisions,
        })
    }

    async fn load_revisions(
        &self,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<Vec<DocRevision>> {
        let repository = crate::DocRevisionRepository::new(self.client.clone());
        let mut revisions = Vec::with_capacity(manifest.documents.len());
        for mapping in &manifest.documents {
            revisions.push(
                repository
                    .get(&manifest.space_id, &mapping.doc_revision_id)
                    .await?
                    .ok_or_else(|| incomplete("revision_missing"))?,
            );
        }
        drop(repository);
        Ok(revisions)
    }

    /// Recheck all prerequisites without publishing. Canonical commit repeats
    /// the same checks under its transaction to close races after preparation.
    pub async fn check_ready(
        &self,
        requirements: &SourcePublicationRequirements,
    ) -> StorageResult<()> {
        let manifest = requirements.manifest();
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    check_ready_tx(tx, &manifest.space_id, &manifest.publication, requirements)
                })
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        append_ready_sql(
            &mut sql,
            &mut bindings,
            &manifest.space_id,
            &manifest.publication,
            requirements,
        )?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    /// All-space durable recovery, independent of work queue contents. A crash
    /// before any child or finalizer enqueue leaves its immutable plan here.
    pub async fn pending_all(&self) -> StorageResult<Vec<SourcePublicationManifest>> {
        let manifests = if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|tx| {
                    tx.scan_extension_rows(MANIFEST_TABLE)?
                        .into_iter()
                        .map(|(_, row)| {
                            decode_payload::<SourcePublicationManifest>(
                                row.get("payload")
                                    .and_then(serde_json::Value::as_str)
                                    .ok_or_else(|| integrity("manifest payload missing"))?,
                            )
                        })
                        .collect::<StorageResult<Vec<_>>>()
                })
                .await?
        } else {
            #[derive(Deserialize)]
            struct PayloadDb {
                payload: String,
            }
            let mut result = self
                .client
                .query_bind_checked(
                    "SELECT payload FROM source_publication_manifest",
                    BTreeMap::<String, Value>::new(),
                )
                .await?;
            let rows: Vec<PayloadDb> = result.take(0)?;
            rows.into_iter()
                .map(|row| decode_payload(&row.payload))
                .collect::<StorageResult<Vec<_>>>()?
        };
        let publication_repo = SourcePublicationRepository::new(self.client.clone());
        let mut pending = Vec::new();
        for manifest in manifests {
            manifest.validate()?;
            let Some(head) = publication_repo
                .head(&manifest.space_id, &manifest.publication.source_id)
                .await?
            else {
                continue;
            };
            if head_matches(&head, &manifest)
                && self
                    .completion(&manifest.space_id, &manifest.publication)
                    .await?
                    .is_none()
            {
                pending.push(manifest);
            }
        }
        drop(publication_repo);
        pending.sort_by_key(|manifest| {
            (
                manifest.space_id.clone(),
                manifest.publication.operation_id.clone(),
            )
        });
        Ok(pending)
    }

    /// Completion certificate for restart/replay diagnostics.
    pub async fn completion(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Option<SourcePublicationCompletion>> {
        SourcePublicationRepository::new(self.client.clone())
            .read_control(COMPLETION_TABLE, &manifest_key(space, publication), space)
            .await
    }

    /// Immutable source provenance for a document projection.
    pub async fn source_revision_for_document(
        &self,
        space: &str,
        doc_id: &RecordId,
    ) -> StorageResult<Option<SourceDocumentMapping>> {
        if doc_id.table() != TableName::Doc {
            return Err(integrity("document mapping requires a doc ID"));
        }
        SourcePublicationRepository::new(self.client.clone())
            .read_control(DOCUMENT_TABLE, &mapping_key(space, doc_id), space)
            .await
    }

    /// Immutable source provenance for an exact rendered revision. Reads of
    /// old published evidence must use this, never a mutable latest-doc pointer.
    pub async fn source_revision_for_document_revision(
        &self,
        space: &str,
        revision_id: &RecordId,
    ) -> StorageResult<Option<SourceDocumentMapping>> {
        if revision_id.table() != TableName::DocRevision {
            return Err(integrity("revision mapping requires a doc_revision ID"));
        }
        SourcePublicationRepository::new(self.client.clone())
            .read_control(REVISION_TABLE, &mapping_key(space, revision_id), space)
            .await
    }

    /// Resolve node support through its exact immutable rendered revision.
    pub async fn source_revision_for_node(
        &self,
        space: &str,
        node_id: &RecordId,
    ) -> StorageResult<Option<SourceDocumentMapping>> {
        let node = crate::DocNodeRepository::new(self.client.clone())
            .get(space, node_id)
            .await?;
        match node {
            Some(node) => {
                self.source_revision_for_document_revision(space, node.revision.id())
                    .await
            }
            None => Ok(None),
        }
    }
}

impl SourcePublicationManifest {
    fn normalize(&mut self) {
        self.documents
            .sort_by_key(|mapping| mapping.doc_revision_id.to_string());
        self.metadata_artifact_ids.sort_by_key(ToString::to_string);
        self.embedding_work_item_ids
            .sort_by_key(ToString::to_string);
        self.planning_artifact_ids.sort_by_key(ToString::to_string);
        self.bundles
            .sort_by_key(|bundle| bundle.artifact_id.to_string());
        self.embeddings
            .sort_by_key(|embedding| embedding.node_id.to_string());
        for bundle in &mut self.bundles {
            bundle.node_ids.sort_by_key(ToString::to_string);
        }
    }

    /// Validate structural closure before touching storage. An empty graph
    /// selection still needs a durable explicit extraction-plan receipt.
    pub fn validate(&self) -> StorageResult<()> {
        if self.space_id.trim().is_empty()
            || self.configuration.space_id != self.space_id
            || self.publication.source_id.table() != TableName::Source
            || [
                &self.publication.generation,
                &self.publication.revision_id,
                &self.publication.operation_id,
            ]
            .iter()
            .any(|s| s.trim().is_empty())
        {
            return Err(integrity("invalid source manifest identity"));
        }
        if let Some(proof) = &self.historical_adoption {
            return proof.validate_manifest(self);
        }
        self.validate_unique_ids()?;
        self.validate_artifact_document_ids()?;
        self.validate_embedding_work()?;
        self.validate_mode_profile()?;
        self.validate_graph_entries()?;
        Ok(())
    }

    fn validate_unique_ids(&self) -> StorageResult<()> {
        require_unique(
            self.metadata_artifact_ids.iter().map(ToString::to_string),
            "metadata artifacts",
        )?;
        require_unique(
            self.planning_artifact_ids.iter().map(ToString::to_string),
            "planning artifacts",
        )?;
        require_unique(
            self.metadata_artifact_ids
                .iter()
                .chain(&self.planning_artifact_ids)
                .chain(self.bundles.iter().map(|bundle| &bundle.artifact_id))
                .map(ToString::to_string),
            "all planned artifacts",
        )?;
        require_unique(
            self.documents.iter().map(|m| m.doc_id.to_string()),
            "documents",
        )?;
        require_unique(
            self.documents.iter().map(|m| m.doc_revision_id.to_string()),
            "document revisions",
        )?;
        require_unique(
            self.bundles.iter().map(|b| b.artifact_id.to_string()),
            "bundles",
        )?;
        require_unique(
            self.bundles.iter().map(|b| b.node_id.to_string()),
            "extraction nodes",
        )?;
        require_unique(
            self.embeddings.iter().map(|e| e.node_id.to_string()),
            "embedding nodes",
        )?;
        Ok(())
    }

    fn validate_artifact_document_ids(&self) -> StorageResult<()> {
        if self.metadata_artifact_ids.is_empty() {
            return Err(integrity(
                "source manifest requires explicit metadata receipt",
            ));
        }
        for id in self
            .metadata_artifact_ids
            .iter()
            .chain(&self.planning_artifact_ids)
        {
            if id.table() != TableName::WorkArtifact {
                return Err(integrity("invalid expected artifact ID"));
            }
        }
        for mapping in &self.documents {
            if mapping.space_id != self.space_id
                || mapping.publication != self.publication
                || mapping.doc_id.table() != TableName::Doc
                || mapping.doc_revision_id.table() != TableName::DocRevision
            {
                return Err(integrity("source document mapping mismatch"));
            }
        }
        Ok(())
    }

    fn validate_embedding_work(&self) -> StorageResult<()> {
        let expected_work: BTreeSet<_> = self
            .documents
            .iter()
            .map(|mapping| {
                source_embedding_work_id(
                    &self.space_id,
                    &self.publication,
                    &mapping.doc_revision_id,
                )
            })
            .collect();
        let versioned_work: BTreeSet<_> = self
            .documents
            .iter()
            .map(|mapping| {
                source_embedding_work_id_for_plan(
                    &self.space_id,
                    &self.publication,
                    &self.configuration,
                    &mapping.doc_revision_id,
                )
            })
            .collect();
        let actual_work: BTreeSet<_> = self.embedding_work_item_ids.iter().cloned().collect();
        if (expected_work != actual_work && versioned_work != actual_work)
            || actual_work.len() != self.embedding_work_item_ids.len()
        {
            return Err(integrity(
                "embedding work does not cover exact rendered revisions",
            ));
        }
        Ok(())
    }

    fn validate_mode_profile(&self) -> StorageResult<()> {
        match self.mode {
            IngestMode::Skip | IngestMode::Metadata => {
                if !self.documents.is_empty()
                    || !self.bundles.is_empty()
                    || !self.embeddings.is_empty()
                    || !self.planning_artifact_ids.is_empty()
                    || self.embedding_profile.is_some()
                {
                    return Err(integrity(
                        "metadata mode declares unexpected graph or retrieval effects",
                    ));
                }
            }
            IngestMode::Retrieval | IngestMode::Graph => {
                if self.documents.is_empty() || self.embedding_profile.is_none() {
                    return Err(integrity(
                        "retrieval publication needs explicit documents and profile",
                    ));
                }
                if self.mode == IngestMode::Retrieval
                    && (!self.bundles.is_empty() || !self.planning_artifact_ids.is_empty())
                {
                    return Err(integrity("retrieval mode declares graph extraction"));
                }
                if self.mode == IngestMode::Graph
                    && self.planning_artifact_ids.len() != self.documents.len()
                {
                    return Err(integrity(
                        "graph mode requires one explicit plan per rendered revision",
                    ));
                }
            }
        }
        if let Some(profile) = &self.embedding_profile {
            if profile.space_id != self.space_id
                || profile.dimensions == 0
                || profile.profile_id.is_empty()
            {
                return Err(integrity("invalid required embedding profile"));
            }
        }
        Ok(())
    }

    fn validate_graph_entries(&self) -> StorageResult<()> {
        let revisions: BTreeSet<_> = self
            .documents
            .iter()
            .map(|d| d.doc_revision_id.to_string())
            .collect();
        for bundle in &self.bundles {
            if (bundle.artifact_id
                != source_bundle_id(&self.publication, &bundle.revision_id, &bundle.node_id)
                && bundle.artifact_id
                    != source_bundle_id_for_plan(
                        &self.publication,
                        &self.configuration,
                        &bundle.revision_id,
                        &bundle.node_id,
                    ))
                || bundle.node_id.table() != TableName::DocNode
                || !revisions.contains(&bundle.revision_id.to_string())
                || bundle.span_start >= bundle.span_end
            {
                return Err(integrity("invalid expected extraction bundle"));
            }
            require_unique(
                bundle.node_ids.iter().map(ToString::to_string),
                "selection nodes",
            )?;
        }
        for embedding in &self.embeddings {
            if embedding.node_id.table() != TableName::DocNode
                || !revisions.contains(&embedding.revision_id.to_string())
            {
                return Err(integrity("embedding outside expected revision"));
            }
            let key = PartitionKey::from_table_name(&embedding.partition_table)
                .map_err(|error| integrity(&error.to_string()))?;
            if key.model != embedding_model(self)?
                || key.table_name() != embedding.partition_table
                || Some(key.dims)
                    != self
                        .embedding_profile
                        .as_ref()
                        .map(|profile| profile.dimensions)
            {
                return Err(integrity(
                    "embedding partition does not match required profile",
                ));
            }
        }
        Ok(())
    }
}

/// Deterministic queue identity shared with source_queue::enqueue_idempotent.
/// Each rendered revision has an explicit job, including zero-node revisions.
#[must_use]
pub fn source_embedding_work_id(
    space: &str,
    publication: &PublicationFence,
    revision: &RecordId,
) -> RecordId {
    let kind = "embed_source_revision";
    let key = format!(
        "{kind}:{}:{}:{revision}",
        publication.source_id, publication.generation
    );
    let identity = serde_json::json!([space, kind, key]).to_string();
    RecordId::new(
        TableName::WorkItem,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
    )
}

/// Stable future bundle ID. External revision strings are not ordering tokens.
#[must_use]
pub fn source_bundle_id(
    publication: &PublicationFence,
    revision_id: &RecordId,
    node_id: &RecordId,
) -> RecordId {
    let key = format!(
        "{}:{}:{}:{}:{}",
        publication.source_id,
        publication.generation,
        publication.operation_id,
        revision_id,
        node_id
    );
    RecordId::new(
        TableName::WorkArtifact,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()),
    )
}

pub(crate) fn manifest_key(space: &str, publication: &PublicationFence) -> String {
    stable_key(&[
        space,
        &publication.source_id.to_string(),
        &publication.generation,
        &publication.operation_id,
    ])
}

fn mapping_key(space: &str, id: &RecordId) -> String {
    stable_key(&[space, &id.to_string()])
}

fn require_unique(values: impl Iterator<Item = String>, label: &str) -> StorageResult<()> {
    let mut seen = BTreeSet::new();
    if values.into_iter().any(|value| !seen.insert(value)) {
        return Err(integrity(&format!("duplicate {label}")));
    }
    Ok(())
}

fn incomplete(reason: &str) -> crate::StorageError {
    conflict(&format!("source_publication_incomplete:{reason}"))
}

fn head_matches(head: &SourceHead, manifest: &SourcePublicationManifest) -> bool {
    head.space_id == manifest.space_id
        && head.source_id == manifest.publication.source_id
        && head.generation == manifest.publication.generation
        && head.operation_id == manifest.publication.operation_id
        && head.desired_revision.as_deref() == Some(manifest.publication.revision_id.as_str())
        && head.lifecycle == SourceVisibility::Active
}

fn check_source_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    let head = read_control_tx::<SourceHead>(
        tx,
        HEAD_TABLE,
        &head_key(&manifest.space_id, &manifest.publication.source_id),
        &manifest.space_id,
    )?;
    if !head
        .as_ref()
        .is_some_and(|head| head_matches(head, manifest))
    {
        return Err(conflict("stale_source_publication"));
    }
    let source = tx
        .get::<elephant_types::records::Source>(
            &manifest.space_id,
            &manifest.publication.source_id,
            false,
        )?
        .ok_or_else(|| conflict("source_missing"))?;
    if source.base.deleted_at.is_some() {
        return Err(conflict("source_deleted"));
    }
    Ok(())
}

fn check_configuration_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    let id = RecordId::from_str(&manifest.configuration.config_id)?;
    let config = tx
        .get::<SpaceConfig>(&manifest.space_id, &id, false)?
        .ok_or_else(|| conflict("unknown_space"))?;
    let processing = config
        .processing
        .ok_or_else(|| conflict("processing_configuration_missing"))?;
    let current = ResolvedSpaceConfig {
        token: SpaceConfigToken {
            space_id: config.base.space_id,
            config_id: config.base.id.to_string(),
            generation: processing.generation,
            policy_id: processing.policy_id.clone(),
        },
        processing,
        purpose: config.purpose,
        skill: config.skill,
    };
    if current.token != manifest.configuration {
        return Err(conflict("source_configuration_changed"));
    }
    if let Some(expected) = &manifest.embedding_profile {
        for operation in [
            ModelOperation::DocumentEmbedding,
            ModelOperation::QueryEmbedding,
        ] {
            let actual = current
                .authorize(&manifest.space_id, operation, Some(&expected.profile_id))
                .and_then(|binding| binding.vector_partition())
                .map_err(|_| conflict("source_embedding_profile_revoked"))?;
            if actual != *expected {
                return Err(conflict("source_embedding_profile_changed"));
            }
        }
        if current.processing.active_embedding_profile_id.as_deref()
            != Some(expected.profile_id.as_str())
        {
            return Err(conflict("source_embedding_profile_inactive"));
        }
    }
    Ok(())
}

fn write_mapping_tx(
    tx: &mut EmbeddedMutation<'_>,
    mapping: &SourceDocumentMapping,
) -> StorageResult<()> {
    for (table, id) in [
        (DOCUMENT_TABLE, &mapping.doc_id),
        (REVISION_TABLE, &mapping.doc_revision_id),
    ] {
        let key = mapping_key(&mapping.space_id, id);
        if let Some(existing) =
            read_control_tx::<SourceDocumentMapping>(tx, table, &key, &mapping.space_id)?
        {
            if existing != *mapping {
                return Err(conflict("source_document_mapping_conflict"));
            }
        } else {
            write_control_tx(
                tx,
                table,
                &key,
                &mapping.space_id,
                &mapping.publication.source_id,
                mapping,
            )?;
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "All document, coverage, artifact, and configuration checks must use the same borrowed transaction before publication writes; retain this complete read-validation phase."
)]
fn check_plan_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    for mapping in &manifest.documents {
        let doc = tx
            .get::<Doc>(&manifest.space_id, &mapping.doc_id, false)?
            .ok_or_else(|| incomplete("document_missing"))?;
        let revision = tx
            .get::<DocRevision>(&manifest.space_id, &mapping.doc_revision_id, false)?
            .ok_or_else(|| incomplete("revision_missing"))?;
        if doc.source.as_ref().map(elephant_types::Link::id)
            != Some(&manifest.publication.source_id)
            || revision.doc.id() != &mapping.doc_id
            || revision.status != elephant_types::records::doc_revision::RevisionStatus::Active
        {
            return Err(conflict("source_document_lineage_changed"));
        }
        if manifest.mode == IngestMode::Graph
            && !revision.text.trim().is_empty()
            && !manifest
                .bundles
                .iter()
                .any(|bundle| bundle.revision_id == mapping.doc_revision_id)
        {
            return Err(incomplete("nonempty_graph_plan_missing_coverage"));
        }
    }
    let plans = manifest
        .planning_artifact_ids
        .iter()
        .map(|id| {
            tx.get::<WorkArtifact>(&manifest.space_id, id, false)?
                .ok_or_else(|| incomplete("extraction_plan_missing"))
        })
        .collect::<StorageResult<Vec<_>>>()?;
    check_planned_selections(manifest, &plans)?;
    if manifest.mode == IngestMode::Graph {
        for mapping in &manifest.documents {
            let revision = tx
                .get::<DocRevision>(&manifest.space_id, &mapping.doc_revision_id, false)?
                .ok_or_else(|| incomplete("revision_missing"))?;
            let expected = expected_coverage(manifest, &revision)?;
            let plan = plans
                .iter()
                .find(|plan| {
                    plan.data
                        .get("revision_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(mapping.doc_revision_id.to_string().as_str())
                })
                .ok_or_else(|| incomplete("extraction_plan_missing"))?;
            let actual: PlanCoverage = serde_json::from_value(
                plan.data
                    .get("coverage")
                    .cloned()
                    .ok_or_else(|| incomplete("extraction_coverage_missing"))?,
            )
            .map_err(|error| integrity(&error.to_string()))?;
            if actual != expected {
                return Err(conflict("source_extraction_coverage_mismatch"));
            }
        }
    }
    let revisions: BTreeSet<_> = manifest
        .documents
        .iter()
        .map(|mapping| mapping.doc_revision_id.to_string())
        .collect();
    let actual_nodes: BTreeSet<_> = tx
        .scan::<DocNode>()?
        .into_iter()
        .filter(|node| {
            node.base.space_id == manifest.space_id
                && node.base.deleted_at.is_none()
                && revisions.contains(&node.revision.id().to_string())
                && matches!(
                    node.role,
                    NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
                )
        })
        .map(|node| (node.base.id.to_string(), node.revision.id().to_string()))
        .collect();
    let expected_nodes: BTreeSet<_> = manifest
        .embeddings
        .iter()
        .map(|node| (node.node_id.to_string(), node.revision_id.to_string()))
        .collect();
    if actual_nodes != expected_nodes {
        return Err(conflict("source_embedding_expected_set_changed"));
    }
    for bundle in &manifest.bundles {
        for id in std::iter::once(&bundle.node_id).chain(&bundle.node_ids) {
            let node = tx
                .get::<DocNode>(&manifest.space_id, id, false)?
                .ok_or_else(|| incomplete("extraction_node_missing"))?;
            if node.revision.id() != &bundle.revision_id {
                return Err(conflict("source_extraction_node_revision_changed"));
            }
        }
        let revision = tx
            .get::<DocRevision>(&manifest.space_id, &bundle.revision_id, false)?
            .ok_or_else(|| incomplete("revision_missing"))?;
        let start =
            usize::try_from(bundle.span_start).map_err(|_| integrity("invalid extraction span"))?;
        let end =
            usize::try_from(bundle.span_end).map_err(|_| integrity("invalid extraction span"))?;
        if revision.text.get(start..end).is_none() {
            return Err(integrity("extraction selection outside immutable text"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PlanCoverage {
    revision_text_hash: String,
    revision_text_len: u32,
    selected_byte_count: u32,
    complete: bool,
    empty_reason: Option<String>,
}

// Recompute independently from persisted text and the closed selected ranges.
// JSON's `complete` flag is not itself evidence. All coordinates are UTF-8 byte
// offsets, including their boundaries; SQL compares this captured text exactly.
fn expected_coverage(
    manifest: &SourcePublicationManifest,
    revision: &DocRevision,
) -> StorageResult<PlanCoverage> {
    let text_hash = format!("{:x}", Sha256::digest(revision.text.as_bytes()));
    if revision.text_hash != text_hash {
        return Err(conflict("source_revision_hash_mismatch"));
    }
    let length = u32::try_from(revision.text.len())
        .map_err(|_| integrity("rendered revision exceeds supported byte offsets"))?;
    let mut selections: Vec<_> = manifest
        .bundles
        .iter()
        .filter(|bundle| bundle.revision_id == revision.base.id)
        .map(|bundle| (bundle.span_start, bundle.span_end))
        .collect();
    selections.sort_unstable();
    let mut ranges: Vec<(u32, u32)> = Vec::new();
    for (start, end) in selections {
        if start >= end
            || end > length
            || !revision.text.is_char_boundary(start as usize)
            || !revision.text.is_char_boundary(end as usize)
        {
            return Err(conflict("source_selection_invalid_byte_span"));
        }
        if let Some(last) = ranges.last_mut() {
            if start <= last.1 {
                last.1 = last.1.max(end);
                continue;
            }
        }
        ranges.push((start, end));
    }
    if ranges.is_empty() && !revision.text.trim().is_empty() {
        return Err(incomplete("nonempty_graph_plan_missing_coverage"));
    }
    let mut selected_byte_count = 0_u32;
    for (start, end) in &ranges {
        selected_byte_count = selected_byte_count
            .checked_add(end - start)
            .ok_or_else(|| integrity("selection coverage overflow"))?;
    }
    let mut range = 0;
    for (offset, character) in revision.text.char_indices() {
        if character.is_whitespace() {
            continue;
        }
        while range < ranges.len() && (ranges[range].1 as usize) <= offset {
            range += 1;
        }
        if range >= ranges.len()
            || (ranges[range].0 as usize) > offset
            || (ranges[range].1 as usize) < offset + character.len_utf8()
        {
            return Err(incomplete("source_extraction_coverage_incomplete"));
        }
    }
    Ok(PlanCoverage {
        revision_text_hash: text_hash,
        revision_text_len: length,
        selected_byte_count,
        complete: true,
        empty_reason: ranges.is_empty().then(|| "empty_or_whitespace".into()),
    })
}

// Database payload decoding is explicit. These are not custom serde visitors and
// never flatten storage rows through an untyped intermediate representation.
#[derive(Deserialize)]
struct PlannedSelection {
    node_id: RecordId,
    node_ids: Vec<RecordId>,
    span_start: u32,
    span_end: u32,
}

#[derive(Deserialize)]
struct PlannedSelections {
    revision_id: RecordId,
    node_selections: Vec<PlannedSelection>,
}

fn check_planned_selections(
    manifest: &SourcePublicationManifest,
    plans: &[WorkArtifact],
) -> StorageResult<()> {
    let mut found = Vec::new();
    let mut revisions = BTreeSet::new();
    let declared_ids: BTreeSet<_> = manifest
        .bundles
        .iter()
        .map(|bundle| bundle.artifact_id.clone())
        .collect();
    for artifact in plans {
        if artifact.artifact_type != "extraction_plan"
            || !matches!(
                artifact.status,
                ArtifactStatus::Draft | ArtifactStatus::Validated | ArtifactStatus::Committed
            )
            || artifact.base.deleted_at.is_some()
        {
            return Err(integrity("invalid durable extraction plan"));
        }
        let plan: PlannedSelections = serde_json::from_value(artifact.data.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        if !revisions.insert(plan.revision_id.to_string()) {
            return Err(integrity("duplicate rendered revision plan"));
        }
        for selection in plan.node_selections {
            let mut node_ids = selection.node_ids;
            node_ids.sort_by_key(ToString::to_string);
            let versioned_id = source_bundle_id_for_plan(
                &manifest.publication,
                &manifest.configuration,
                &plan.revision_id,
                &selection.node_id,
            );
            // Active fanout uses configuration-scoped IDs. Retain only the
            // original pre-versioned format already accepted by validate();
            // neither format may invent an output outside the declared set.
            let artifact_id = if declared_ids.contains(&versioned_id) {
                versioned_id
            } else {
                source_bundle_id(&manifest.publication, &plan.revision_id, &selection.node_id)
            };
            found.push(ExpectedSourceBundle {
                artifact_id,
                node_id: selection.node_id,
                node_ids,
                revision_id: plan.revision_id.clone(),
                span_start: selection.span_start,
                span_end: selection.span_end,
            });
        }
    }
    found.sort_by_key(|bundle| bundle.artifact_id.to_string());
    if found != manifest.bundles {
        return Err(conflict("source_extraction_expected_set_changed"));
    }
    if manifest.mode == IngestMode::Graph
        && revisions
            != manifest
                .documents
                .iter()
                .map(|mapping| mapping.doc_revision_id.to_string())
                .collect()
    {
        return Err(integrity(
            "graph plan does not cover every rendered revision",
        ));
    }
    Ok(())
}

/// Verify a staged bundle belongs to the original accepted expectation set.
/// Used by the per-node staging path as well as the final transaction.
fn check_bundle_authority(
    space: &str,
    publication: &PublicationFence,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    if artifact.base.space_id != space
        || artifact.base.deleted_at.is_some()
        || artifact.artifact_type != "commit_bundle"
        || artifact.validated_at.is_none()
        || !matches!(
            artifact.status,
            ArtifactStatus::Validated | ArtifactStatus::Committed
        )
        || artifact
            .validation_errors
            .as_ref()
            .is_some_and(|errors| !errors.is_empty())
    {
        return Err(incomplete("bundle_not_validated"));
    }
    let captured: PublicationFence = serde_json::from_value(
        artifact
            .data
            .get("publication")
            .cloned()
            .ok_or_else(|| integrity("source bundle lacks captured publication fence"))?,
    )
    .map_err(|error| integrity(&error.to_string()))?;
    if captured != *publication {
        return Err(conflict("source_bundle_fence_mismatch"));
    }
    let payload = artifact
        .data
        .get("payload")
        .ok_or_else(|| integrity("source bundle payload missing"))?;
    let data = payload
        .get("data")
        .ok_or_else(|| integrity("source bundle data missing"))?;
    if data.get("source_id").and_then(serde_json::Value::as_str)
        != Some(publication.source_id.to_string().as_str())
    {
        return Err(conflict("source_bundle_lineage_mismatch"));
    }
    Ok(())
}

/// Check membership when sealed; only metadata may precede the fanout plan.
pub fn check_staging_bundle(
    manifest: Option<&SourcePublicationManifest>,
    space: &str,
    publication: &PublicationFence,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    if let Some(manifest) = manifest {
        return check_expected_bundle(manifest, artifact);
    }
    check_bundle_authority(space, publication, artifact)?;
    if artifact.data["bundle_kind"] != "source_metadata"
        || artifact.data["payload"]["kind"] != "source_metadata"
    {
        return Err(incomplete("manifest_missing"));
    }
    Ok(())
}

/// Validate a staged bundle against its exact sealed role, revision, and node selection.
pub fn check_expected_bundle(
    manifest: &SourcePublicationManifest,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    check_bundle_authority(&manifest.space_id, &manifest.publication, artifact)?;
    let payload = &artifact.data["payload"];
    let data = &payload["data"];
    if manifest.metadata_artifact_ids.contains(&artifact.base.id) {
        if payload.get("kind").and_then(serde_json::Value::as_str) != Some("source_metadata")
            || artifact
                .data
                .get("bundle_kind")
                .and_then(serde_json::Value::as_str)
                != Some("source_metadata")
        {
            return Err(integrity("expected source metadata bundle"));
        }
        return Ok(());
    }
    let expected = manifest
        .bundles
        .iter()
        .find(|bundle| bundle.artifact_id == artifact.base.id)
        .ok_or_else(|| conflict("source_bundle_not_expected"))?;
    if artifact.status != ArtifactStatus::Validated
        || payload.get("kind").and_then(serde_json::Value::as_str) != Some("knowledge_candidates")
        || artifact
            .data
            .get("bundle_kind")
            .and_then(serde_json::Value::as_str)
            != Some("knowledge_candidates")
        || data.get("node_id").and_then(serde_json::Value::as_str)
            != Some(expected.node_id.to_string().as_str())
        || data.get("revision_id").and_then(serde_json::Value::as_str)
            != Some(expected.revision_id.to_string().as_str())
    {
        return Err(conflict("source_bundle_expected_identity_mismatch"));
    }
    let assertions = data
        .get("assertions")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| integrity("source bundle assertions missing"))?;
    for assertion in assertions {
        let start = assertion
            .get("evidence_start")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| integrity("source assertion evidence start missing"))?;
        let end = assertion
            .get("evidence_end")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| integrity("source assertion evidence end missing"))?;
        if start < u64::from(expected.span_start)
            || end > u64::from(expected.span_end)
            || end <= start
        {
            return Err(conflict("source_assertion_outside_selected_span"));
        }
    }
    Ok(())
}

fn embedding_model(manifest: &SourcePublicationManifest) -> StorageResult<String> {
    let profile = manifest
        .embedding_profile
        .as_ref()
        .ok_or_else(|| integrity("required embedding profile missing"))?;
    Ok(format!("recall:{}", encode_payload(profile)?))
}

fn embedding_partition(
    manifest: &SourcePublicationManifest,
    node: &DocNode,
) -> StorageResult<String> {
    let profile = manifest
        .embedding_profile
        .as_ref()
        .ok_or_else(|| integrity("required embedding profile missing"))?;
    PartitionKey::new(
        node.base.security.policy_id.clone(),
        level_bucket(node.base.security.level),
        embedding_model(manifest)?,
        profile.dimensions,
    )
    .map(|key| key.table_name())
    .map_err(|error| integrity(&error.to_string()))
}

fn check_embedding(
    manifest: &SourcePublicationManifest,
    expected: &RequiredSourceEmbedding,
    node: &DocNode,
    embedding: &NodeEmbedding,
) -> StorageResult<()> {
    let model = embedding_model(manifest)?;
    let profile = manifest
        .embedding_profile
        .as_ref()
        .ok_or_else(|| integrity("required embedding profile missing"))?;
    if node.revision.id() != &expected.revision_id
        || node.base.deleted_at.is_some()
        || node.embedding_model.as_deref() != Some(model.as_str())
        || embedding_partition(manifest, node)? != expected.partition_table
        || embedding.base.space_id != manifest.space_id
        || embedding.base.deleted_at.is_some()
        || embedding.node.id() != &expected.node_id
        || embedding.model != model
        || usize::try_from(embedding.dims).ok() != Some(profile.dimensions)
        || embedding.vector.len() != profile.dimensions
        || embedding.vector.iter().any(|value| !value.is_finite())
        || !embedding.vector.iter().any(|value| *value != 0.0)
        || embedding.base.security != node.base.security
        || embedding.base.subjects != node.base.subjects
        || embedding.base.subjects_state != node.base.subjects_state
    {
        return Err(incomplete("required_embedding_not_indexed"));
    }
    Ok(())
}

/// Capture a node's exact index requirement with the same partition identity
/// used by the approved embed worker and retrieval provider.
pub fn required_embedding(
    profile: &VectorPartition,
    node: &DocNode,
) -> StorageResult<RequiredSourceEmbedding> {
    if profile.space_id != node.base.space_id {
        return Err(integrity("embedding profile belongs to another space"));
    }
    let model = format!("recall:{}", encode_payload(profile)?);
    let partition_table = PartitionKey::new(
        node.base.security.policy_id.clone(),
        level_bucket(node.base.security.level),
        model,
        profile.dimensions,
    )
    .map_err(|error| integrity(&error.to_string()))?
    .table_name();
    Ok(RequiredSourceEmbedding {
        node_id: node.base.id.clone(),
        revision_id: node.revision.id().clone(),
        partition_table,
    })
}

fn bind_json(
    bindings: &mut BTreeMap<String, Value>,
    name: &str,
    value: impl Serialize,
) -> StorageResult<()> {
    let json = serde_json::to_value(value).map_err(|error| integrity(&error.to_string()))?;
    bindings.insert(name.to_owned(), crate::surreal::json_to_surreal(json)?);
    Ok(())
}

fn append_identity_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    bind_json(bindings, "sf_space", &manifest.space_id)?;
    bind_json(
        bindings,
        "sf_source_string",
        manifest.publication.source_id.to_string(),
    )?;
    bind_json(bindings, "sf_fence", &manifest.publication)?;
    bind_json(
        bindings,
        "sf_head_payload",
        encode_payload(&requirements.head)?,
    )?;
    bind_json(bindings, "sf_manifest_payload", encode_payload(manifest)?)?;
    bind_json(
        bindings,
        "sf_config_generation",
        manifest.configuration.generation,
    )?;
    bind_json(bindings, "sf_policy_id", &manifest.configuration.policy_id)?;
    bindings.insert(
        "sf_manifest_id".into(),
        control_thing(
            MANIFEST_TABLE,
            &manifest_key(&manifest.space_id, &manifest.publication),
        )
        .into(),
    );
    bindings.insert(
        "sf_head_id".into(),
        control_thing(
            HEAD_TABLE,
            &head_key(&manifest.space_id, &manifest.publication.source_id),
        )
        .into(),
    );
    bindings.insert(
        "sf_source_id".into(),
        crate::surreal::thing_from_record_id(&manifest.publication.source_id).into(),
    );
    bindings.insert(
        "sf_config_id".into(),
        crate::surreal::thing_from_record_id(&RecordId::from_str(
            &manifest.configuration.config_id,
        )?)
        .into(),
    );
    sql.push_str("LET $sf_head_row = SELECT * FROM ONLY $sf_head_id;\n\
        IF $sf_head_row = NONE OR $sf_head_row.space_id != $sf_space { THROW 'publication:stale_source_publication'; };\n\
        IF $sf_head_row.payload != $sf_head_payload { THROW 'publication:stale_source_publication'; };\n\
        LET $sf_source = SELECT * FROM ONLY $sf_source_id;\n\
        IF $sf_source = NONE OR $sf_source.space_id != $sf_space OR $sf_source.deleted_at != NONE { THROW 'publication:source_missing'; };\n\
        LET $sf_cfg = SELECT * FROM ONLY $sf_config_id;\n\
        IF $sf_cfg = NONE OR $sf_cfg.space_id != $sf_space OR $sf_cfg.deleted_at != NONE OR $sf_cfg.processing = NONE OR $sf_cfg.processing.generation != $sf_config_generation OR $sf_cfg.processing.policy_id != $sf_policy_id { THROW 'publication:source_configuration_changed'; };\n");
    // Same-value UPDATE is optimized away by pinned SurrealDB. The physical
    // head fence and the actual shared reference revision create real writes.
    // Every PRESENT input below has a checked space; 039/043 writer events also
    // touch that original space on deletion or movement to another space.
    sql.push_str("fn::recall_physical_read_fence($sf_head_id);\nfn::recall_reference_guard({space_id:$sf_space}, false);\n");
    if let Some(profile) = &manifest.embedding_profile {
        bind_json(bindings, "sf_profile", profile)?;
        sql.push_str("LET $sf_profiles = object::values($sf_cfg.processing.profiles);\n\
            LET $sf_profile_rows = $sf_profiles[WHERE id = $sf_profile.profile_id];\n\
            IF array::len($sf_profile_rows) != 1 { THROW 'publication:source_embedding_profile_changed'; };\n\
            LET $sf_current_profile = $sf_profile_rows[0];\n\
            LET $sf_connections = object::values($sf_cfg.processing.connections);\n\
            LET $sf_connection_rows = $sf_connections[WHERE id = $sf_current_profile.connection_id];\n\
            IF array::len($sf_connection_rows) != 1 { THROW 'publication:source_embedding_profile_changed'; };\n\
            LET $sf_connection = $sf_connection_rows[0];\n\
            IF $sf_cfg.processing.active_embedding_profile_id != $sf_profile.profile_id OR $sf_cfg.processing.bindings.document_embedding.profile_id != $sf_profile.profile_id OR $sf_cfg.processing.bindings.query_embedding.profile_id != $sf_profile.profile_id OR $sf_cfg.processing.bindings.document_embedding.revoked != false OR $sf_cfg.processing.bindings.query_embedding.revoked != false OR $sf_connection.revoked != false { THROW 'publication:source_embedding_profile_revoked'; };\n\
            IF $sf_current_profile.model != $sf_profile.model OR $sf_current_profile.model_version != $sf_profile.model_version OR $sf_current_profile.preprocessing != $sf_profile.preprocessing OR $sf_current_profile.dimensions != $sf_profile.dimensions OR $sf_connection.provider != $sf_profile.provider OR $sf_connection.account_id != $sf_profile.account_id OR $sf_connection.endpoint != $sf_profile.endpoint { THROW 'publication:source_embedding_profile_changed'; };\n");
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "This SQL fragment validates the exact document and extraction plan under shared transaction bindings; preserve guard emission order and one complete backend fragment."
)]
fn append_plan_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    render::append_render_inputs_sql(sql, bindings, requirements)?;
    let mut revision_things = Vec::new();
    let mut expected_coverages = BTreeMap::new();
    for (index, mapping) in manifest.documents.iter().enumerate() {
        let prefix = format!("sf_doc_{index}");
        let captured_revision = requirements
            .revisions
            .iter()
            .find(|revision| revision.base.id == mapping.doc_revision_id)
            .ok_or_else(|| incomplete("revision_missing"))?;
        bind_json(bindings, &format!("{prefix}_text"), &captured_revision.text)?;
        bind_json(
            bindings,
            &format!("{prefix}_hash"),
            &captured_revision.text_hash,
        )?;
        if manifest.mode == IngestMode::Graph {
            expected_coverages.insert(
                mapping.doc_revision_id.to_string(),
                expected_coverage(manifest, captured_revision)?,
            );
        }
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(&mapping.doc_id).into(),
        );
        bindings.insert(
            format!("{prefix}_revision"),
            crate::surreal::thing_from_record_id(&mapping.doc_revision_id).into(),
        );
        revision_things.push(Value::from(crate::surreal::thing_from_record_id(
            &mapping.doc_revision_id,
        )));
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            LET ${prefix}_rev = SELECT * FROM ONLY ${prefix}_revision;\n\
            IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.deleted_at != NONE OR ${prefix}.source != $sf_source_id OR ${prefix}_rev = NONE OR ${prefix}_rev.space_id != $sf_space OR ${prefix}_rev.deleted_at != NONE OR ${prefix}_rev.doc != ${prefix}_id OR ${prefix}_rev.status != 'active' OR ${prefix}_rev.text != ${prefix}_text OR ${prefix}_rev.text_hash != ${prefix}_hash {{ THROW 'publication:source_document_lineage_changed'; }};");
        if manifest.mode == IngestMode::Graph
            && !manifest
                .bundles
                .iter()
                .any(|bundle| bundle.revision_id == mapping.doc_revision_id)
        {
            let _ = writeln!(sql,"IF string::trim(${prefix}_rev.text) != '' {{ THROW 'publication:source_publication_incomplete:nonempty_graph_plan_missing_coverage'; }};");
        }
    }
    // The persisted JSON object omits NONE members such as empty_reason.
    // Keep byte coverage equality exact in the native stored representation.
    let coverage_json =
        serde_json::to_value(expected_coverages).map_err(|error| integrity(&error.to_string()))?;
    bindings.insert(
        "sf_expected_coverages".into(),
        crate::surreal::stored_native_value(crate::surreal::json_to_surreal(coverage_json)?),
    );
    bindings.insert(
        "sf_revisions".into(),
        Value::Array(surrealdb::sql::Array::from(revision_things)),
    );
    let expected_nodes: Vec<Value> = manifest
        .embeddings
        .iter()
        .map(|node| crate::surreal::thing_from_record_id(&node.node_id).into())
        .collect();
    bindings.insert(
        "sf_embedding_nodes".into(),
        Value::Array(surrealdb::sql::Array::from(expected_nodes)),
    );
    sql.push_str("LET $sf_actual_nodes = SELECT VALUE id FROM doc_node WHERE space_id = $sf_space AND deleted_at = NONE AND revision IN $sf_revisions AND role IN ['chunk','paragraph','summary'];\n\
        IF array::sort($sf_actual_nodes) != array::sort($sf_embedding_nodes) { THROW 'publication:source_embedding_expected_set_changed'; };\n");
    let expected_by_revision: BTreeMap<_, _> = manifest.documents.iter().map(|mapping| {
        let expected = manifest.bundles.iter().filter(|bundle| bundle.revision_id == mapping.doc_revision_id)
            .map(|bundle| serde_json::json!({ "node_id": bundle.node_id, "node_ids": bundle.node_ids, "span_start": bundle.span_start, "span_end": bundle.span_end })).collect::<Vec<_>>();
        (mapping.doc_revision_id.to_string(), expected)
    }).collect();
    bind_json(bindings, "sf_expected_selections", &expected_by_revision)?;
    let plan_ids: Vec<Value> = manifest
        .planning_artifact_ids
        .iter()
        .map(|id| crate::surreal::thing_from_record_id(id).into())
        .collect();
    bindings.insert(
        "sf_plan_ids".into(),
        Value::Array(surrealdb::sql::Array::from(plan_ids)),
    );
    // Compare each persisted selection exactly, including coalesced node IDs and
    // selected span. Count and revision identity prevent duplicate-plan closure.
    sql.push_str("LET $sf_plans = SELECT * FROM $sf_plan_ids;\n\
        IF array::len($sf_plans) != array::len($sf_plan_ids) { THROW 'publication:source_publication_incomplete:extraction_plan_missing'; };\n\
        FOR $sf_plan IN $sf_plans {\n\
            IF $sf_plan.space_id != $sf_space OR $sf_plan.deleted_at != NONE OR $sf_plan.artifact_type != 'extraction_plan' OR $sf_plan.status NOT IN ['draft','validated','committed'] { THROW 'publication:invalid_extraction_plan'; };\n\
            LET $sf_expected = $sf_expected_selections[$sf_plan.data.revision_id];\n\
            IF $sf_plan.data.coverage = NONE OR $sf_plan.data.coverage != $sf_expected_coverages[$sf_plan.data.revision_id] { THROW 'publication:source_extraction_coverage_mismatch'; };\n\
            IF $sf_expected = NONE OR array::len($sf_expected) != array::len($sf_plan.data.node_selections) { THROW 'publication:source_extraction_expected_set_changed'; };\n\
            FOR $sf_selection IN $sf_plan.data.node_selections {\n\
                LET $sf_matches = $sf_expected[WHERE node_id = $sf_selection.node_id AND span_start = $sf_selection.span_start AND span_end = $sf_selection.span_end AND array::sort(node_ids) = array::sort($sf_selection.node_ids)];\n\
                IF array::len($sf_matches) != 1 { THROW 'publication:source_extraction_expected_set_changed'; };\n\
            };\n\
        };\n\
        IF array::len(array::distinct($sf_plans.data.revision_id)) != array::len($sf_plan_ids) { THROW 'publication:duplicate_revision_plan'; };\n");
    for (index, bundle) in manifest.bundles.iter().enumerate() {
        let prefix = format!("sf_selection_{index}");
        let ids: Vec<Value> = std::iter::once(&bundle.node_id)
            .chain(&bundle.node_ids)
            .map(|id| crate::surreal::thing_from_record_id(id).into())
            .collect();
        bindings.insert(
            format!("{prefix}_nodes"),
            Value::Array(surrealdb::sql::Array::from(ids)),
        );
        bindings.insert(
            format!("{prefix}_revision"),
            crate::surreal::thing_from_record_id(&bundle.revision_id).into(),
        );
        let _ = writeln!(sql,"FOR $sf_node_id IN ${prefix}_nodes {{\n\
            LET $sf_node = SELECT * FROM ONLY $sf_node_id;\n\
            IF $sf_node = NONE OR $sf_node.space_id != $sf_space OR $sf_node.deleted_at != NONE OR $sf_node.revision != ${prefix}_revision {{ THROW 'publication:source_extraction_node_revision_changed'; }};\n\
        }};");
    }
    Ok(())
}

fn append_mapping_writes_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    for (index, mapping) in manifest.documents.iter().enumerate() {
        for (kind, table, id) in [
            ("doc", DOCUMENT_TABLE, &mapping.doc_id),
            ("rev", REVISION_TABLE, &mapping.doc_revision_id),
        ] {
            let prefix = format!("sf_map_{kind}_{index}");
            bindings.insert(
                format!("{prefix}_id"),
                control_thing(table, &mapping_key(&manifest.space_id, id)).into(),
            );
            bind_json(
                bindings,
                &format!("{prefix}_payload"),
                encode_payload(mapping)?,
            )?;
            let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
                IF ${prefix} != NONE AND (${prefix}.space_id != $sf_space OR ${prefix}.payload != ${prefix}_payload) {{ THROW 'publication:source_document_mapping_conflict'; }};\n\
                IF ${prefix} = NONE {{ CREATE ${prefix}_id SET space_id = $sf_space, source_id = $sf_source_string, payload = ${prefix}_payload; }};");
        }
    }
    Ok(())
}

/// Add storage preconditions to the caller's canonical transaction. Parameter
/// names are reserved under `sf_`; no source-controlled SQL is interpolated.
#[allow(
    clippy::too_many_lines,
    reason = "Publication readiness guards share a binding namespace and must precede canonical writes in one transaction; retain their complete native validation fragment."
)]
pub(crate) fn append_ready_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    source: &PublicationFence,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    manifest.validate()?;
    if manifest.space_id != space || manifest.publication != *source {
        return Err(conflict("source_finalization_fence_mismatch"));
    }
    append_identity_sql(sql, bindings, requirements)?;
    replan::append_finalizer_sql(sql, bindings, requirements)?;
    progress::append_sql(sql, bindings, requirements)?;
    sql.push_str("LET $sf_stored_manifest = SELECT * FROM ONLY $sf_manifest_id;\n\
        IF $sf_stored_manifest = NONE OR $sf_stored_manifest.space_id != $sf_space OR $sf_stored_manifest.payload != $sf_manifest_payload { THROW 'publication:source_manifest_conflict'; };\n");
    append_plan_sql(sql, bindings, requirements)?;
    for (index, mapping) in manifest.documents.iter().enumerate() {
        for (kind, table, id) in [
            ("doc", DOCUMENT_TABLE, &mapping.doc_id),
            ("rev", REVISION_TABLE, &mapping.doc_revision_id),
        ] {
            let prefix = format!("sf_map_{kind}_{index}");
            bindings.insert(
                format!("{prefix}_id"),
                control_thing(table, &mapping_key(space, id)).into(),
            );
            bind_json(
                bindings,
                &format!("{prefix}_payload"),
                encode_payload(mapping)?,
            )?;
            let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
                IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.payload != ${prefix}_payload {{ THROW 'publication:source_document_mapping_changed'; }};");
        }
    }
    for (index, id) in manifest
        .metadata_artifact_ids
        .iter()
        .chain(manifest.bundles.iter().map(|bundle| &bundle.artifact_id))
        .enumerate()
    {
        let prefix = format!("sf_bundle_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(id).into(),
        );
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            IF ${prefix} = NONE {{ THROW 'publication:source_publication_incomplete:expected_bundle_missing'; }};\n\
            IF ${prefix}.space_id != $sf_space OR ${prefix}.deleted_at != NONE OR ${prefix}.artifact_type != 'commit_bundle' OR ${prefix}.validated_at = NONE OR ${prefix}.status NOT IN ['validated','committed'] OR ( ${prefix}.validation_errors != NONE AND array::len(${prefix}.validation_errors) > 0 ) {{ THROW 'publication:source_publication_incomplete:bundle_not_validated'; }};\n\
            IF ${prefix}.data.publication != $sf_fence OR ${prefix}.data.payload.data.source_id != $sf_source_string {{ THROW 'publication:source_bundle_fence_mismatch'; }};");
        if manifest.metadata_artifact_ids.contains(id) {
            let _ = writeln!(sql,"IF ${prefix}.status NOT IN ['validated','committed'] OR ${prefix}.data.bundle_kind != 'source_metadata' OR ${prefix}.data.payload.kind != 'source_metadata' {{ THROW 'publication:source_publication_incomplete:metadata_not_validated'; }};");
        } else {
            let expected = manifest
                .bundles
                .iter()
                .find(|bundle| &bundle.artifact_id == id)
                .ok_or_else(|| integrity("undeclared bundle"))?;
            bind_json(
                bindings,
                &format!("{prefix}_node"),
                expected.node_id.to_string(),
            )?;
            bind_json(
                bindings,
                &format!("{prefix}_revision"),
                expected.revision_id.to_string(),
            )?;
            bind_json(bindings, &format!("{prefix}_start"), expected.span_start)?;
            bind_json(bindings, &format!("{prefix}_end"), expected.span_end)?;
            let _ = writeln!(sql,"IF ${prefix}.status != 'validated' OR ${prefix}.data.bundle_kind != 'knowledge_candidates' OR ${prefix}.data.payload.kind != 'knowledge_candidates' OR ${prefix}.data.payload.data.node_id != ${prefix}_node OR ${prefix}.data.payload.data.revision_id != ${prefix}_revision {{ THROW 'publication:source_bundle_expected_identity_mismatch'; }};");
        }
        if !manifest.metadata_artifact_ids.contains(id) {
            let _ = writeln!(sql,"IF !type::is::array(${prefix}.data.payload.data.assertions) {{ THROW 'publication:invalid_source_assertions'; }};\n\
                FOR $sf_assertion IN ${prefix}.data.payload.data.assertions {{\n\
                    IF !type::is::int($sf_assertion.evidence_start) OR !type::is::int($sf_assertion.evidence_end) OR $sf_assertion.evidence_start < ${prefix}_start OR $sf_assertion.evidence_end > ${prefix}_end OR $sf_assertion.evidence_end <= $sf_assertion.evidence_start {{ THROW 'publication:source_assertion_outside_selected_span'; }};\n\
                }};");
        }
        append_origin_sql(sql, &prefix);
        replan::append_work_plan_sql(sql, &format!("{prefix}_origin_item"), "$sf_replanned");
        replan::append_candidate_tags_sql(sql, &prefix);
        append_registration_sql(sql, bindings, manifest, id, &prefix)?;
    }
    for (index, id) in manifest.planning_artifact_ids.iter().enumerate() {
        let prefix = format!("sf_plan_origin_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(id).into(),
        );
        let _ = writeln!(sql, "LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;");
        append_origin_sql(sql, &prefix);
        replan::append_work_plan_sql(sql, &format!("{prefix}_origin_item"), "$sf_replanned");
        replan::append_artifact_tags_sql(
            sql,
            &prefix,
            &format!("$sf_replanned OR ${prefix}_origin_item.payload._source_plan != NONE"),
        );
    }
    append_embedding_work_sql(sql, bindings, manifest)?;
    for (index, expected) in manifest.embeddings.iter().enumerate() {
        let prefix = format!("sf_embedding_{index}");
        let partition = PartitionKey::from_table_name(&expected.partition_table)
            .map_err(|error| integrity(&error.to_string()))?;
        let level = partition
            .level_bucket
            .strip_prefix('L')
            .and_then(|s| s.parse::<i32>().ok())
            .ok_or_else(|| integrity("invalid embedding security partition"))?;
        bindings.insert(
            format!("{prefix}_id"),
            surrealdb::sql::Thing::from((
                expected.partition_table.clone(),
                surrealdb::sql::Id::Uuid(expected.node_id.id().into()),
            ))
            .into(),
        );
        bindings.insert(
            format!("{prefix}_node_id"),
            crate::surreal::thing_from_record_id(&expected.node_id).into(),
        );
        bindings.insert(
            format!("{prefix}_revision"),
            crate::surreal::thing_from_record_id(&expected.revision_id).into(),
        );
        bind_json(
            bindings,
            &format!("{prefix}_model"),
            embedding_model(manifest)?,
        )?;
        bind_json(bindings, &format!("{prefix}_dims"), partition.dims)?;
        bind_json(bindings, &format!("{prefix}_policy"), partition.policy_id)?;
        bind_json(bindings, &format!("{prefix}_level"), level)?;
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            LET ${prefix}_node = SELECT * FROM ONLY ${prefix}_node_id;\n\
            IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.deleted_at != NONE OR ${prefix}.node != ${prefix}_node_id OR ${prefix}.model != ${prefix}_model OR ${prefix}.dims != ${prefix}_dims OR array::len(${prefix}.vector) != ${prefix}_dims OR ${prefix}.vector_norm <= 0 OR ${prefix}.vector_norm = NONE {{ THROW 'publication:source_publication_incomplete:required_embedding_missing'; }};\n\
            IF ${prefix}_node = NONE OR ${prefix}_node.space_id != $sf_space OR ${prefix}_node.deleted_at != NONE OR ${prefix}_node.revision != ${prefix}_revision OR ${prefix}_node.embedding_model != ${prefix}_model OR ${prefix}_node.security.policy_id != ${prefix}_policy OR ${prefix}_node.security.level != ${prefix}_level OR ${prefix}.security != ${prefix}_node.security OR ${prefix}.subjects != ${prefix}_node.subjects OR ${prefix}.subjects_state != ${prefix}_node.subjects_state {{ THROW 'publication:source_publication_incomplete:required_embedding_not_indexed'; }};\n\
            FOR $sf_component IN ${prefix}.vector {{ IF $sf_component != $sf_component OR math::abs($sf_component) > 3.4028235e38 {{ THROW 'publication:invalid_embedding_vector'; }}; }};");
        // Approximate KNN requires an HNSW index. Querying the stored vector
        // proves that index is usable, without relying on an embedding marker
        // or unavailable INFO-as-expression syntax in SurrealDB 2.x. Duplicate
        // identical vectors may return a different member; the exact required
        // row and envelope were already checked above in this transaction.
        let _ = writeln!(sql,"LET ${prefix}_visible = SELECT VALUE id FROM {} WHERE vector <|1,100|> ${prefix}.vector AND space_id = $sf_space AND deleted_at = NONE;\n\
            IF array::len(${prefix}_visible) = 0 {{ THROW 'publication:source_publication_incomplete:required_index_missing'; }};", expected.partition_table);
    }
    Ok(())
}

/// Append a diagnostic completion certificate to the same canonical transaction.
pub(crate) fn append_completed_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    requirements: &SourcePublicationRequirements,
    commit_id: &str,
) -> StorageResult<()> {
    let completion = completion_for(requirements, commit_id)?;
    append_committed_effects_sql(sql, bindings, requirements.manifest())?;
    bindings.insert(
        "sf_completed_decision".into(),
        requirements
            .published_ingest_decision()?
            .as_ref()
            .map_or(Value::None, |link| {
                crate::surreal::thing_from_record_id(link.id()).into()
            }),
    );
    sql.push_str("IF $sf_done_source.latest_ingest_decision != $sf_completed_decision { THROW 'publication:source_original_ingest_decision_changed'; };\n");
    bindings.insert(
        "sf_completion_id".into(),
        control_thing(
            COMPLETION_TABLE,
            &manifest_key(&completion.space_id, &completion.publication),
        )
        .into(),
    );
    bind_json(
        bindings,
        "sf_completion_payload",
        encode_payload(&completion)?,
    )?;
    bindings.insert(
        "sf_progress_lookup_id".into(),
        control_thing(
            COMPLETION_TABLE,
            &progress::published_completion_key(
                &completion.space_id,
                &completion.publication.source_id,
            ),
        )
        .into(),
    );
    sql.push_str("CREATE $sf_completion_id SET space_id = $sf_space, source_id = $sf_source_string, payload = $sf_completion_payload;\n\
        UPSERT $sf_progress_lookup_id SET space_id = $sf_space, source_id = $sf_source_string, payload = $sf_completion_payload;\n");
    Ok(())
}

/// Canonical-transaction guard. Call before applying any prepared effects.
pub(crate) fn check_ready_tx(
    tx: &mut EmbeddedMutation<'_>,
    space: &str,
    source: &PublicationFence,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    if manifest.space_id != space || manifest.publication != *source {
        return Err(conflict("source_finalization_fence_mismatch"));
    }
    manifest.validate()?;
    let stored = read_control_tx::<SourcePublicationManifest>(
        tx,
        MANIFEST_TABLE,
        &manifest_key(space, source),
        space,
    )?
    .ok_or_else(|| incomplete("manifest_missing"))?;
    if stored != *manifest {
        return Err(conflict("source_manifest_conflict"));
    }
    check_source_tx(tx, manifest)?;
    check_configuration_tx(tx, manifest)?;
    replan::check_finalizer_tx(tx, requirements)?;
    render::check_render_inputs_tx(tx, requirements)?;
    progress::check_tx(tx, requirements)?;
    for mapping in &manifest.documents {
        let captured = requirements
            .revisions
            .iter()
            .find(|revision| revision.base.id == mapping.doc_revision_id)
            .ok_or_else(|| incomplete("revision_missing"))?;
        let current = tx
            .get::<DocRevision>(space, &mapping.doc_revision_id, false)?
            .ok_or_else(|| incomplete("revision_missing"))?;
        if current.text != captured.text || current.text_hash != captured.text_hash {
            return Err(conflict("source_document_lineage_changed"));
        }
    }
    check_plan_tx(tx, manifest)?;
    for mapping in &manifest.documents {
        for (table, id) in [
            (DOCUMENT_TABLE, &mapping.doc_id),
            (REVISION_TABLE, &mapping.doc_revision_id),
        ] {
            if read_control_tx::<SourceDocumentMapping>(tx, table, &mapping_key(space, id), space)?
                .as_ref()
                != Some(mapping)
            {
                return Err(conflict("source_document_mapping_changed"));
            }
        }
    }
    for id in manifest
        .metadata_artifact_ids
        .iter()
        .chain(manifest.bundles.iter().map(|bundle| &bundle.artifact_id))
    {
        let artifact = tx
            .get::<WorkArtifact>(space, id, false)?
            .ok_or_else(|| incomplete("expected_bundle_missing"))?;
        check_expected_bundle(manifest, &artifact)?;
        check_origin_tx(tx, space, &artifact)?;
        replan::check_artifact_plan_tx(tx, manifest, &artifact)?;
        check_registration_tx(tx, manifest, &artifact)?;
    }
    for id in &manifest.planning_artifact_ids {
        let plan = tx
            .get::<WorkArtifact>(space, id, false)?
            .ok_or_else(|| incomplete("extraction_plan_missing"))?;
        check_origin_tx(tx, space, &plan)?;
        replan::check_artifact_plan_tx(tx, manifest, &plan)?;
    }
    check_embedding_work_tx(tx, manifest)?;
    for expected in &manifest.embeddings {
        let node = tx
            .get::<DocNode>(space, &expected.node_id, false)?
            .ok_or_else(|| incomplete("required_embedding_node_missing"))?;
        let model = embedding_model(manifest)?;
        let embedding_id = crate::chunk_embedding_repository::embedding_record_id_for(
            &expected.node_id,
            space,
            &node.base.security,
            &model,
        );
        let embedding = tx
            .get::<NodeEmbedding>(space, &embedding_id, false)?
            .ok_or_else(|| incomplete("required_embedding_missing"))?;
        check_embedding(manifest, expected, &node, &embedding)?;
    }
    Ok(())
}

fn check_embedding_work_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    if manifest.documents.is_empty() {
        return Ok(());
    }
    let mut runs: BTreeMap<_, _> = tx
        .scan::<WorkRunArchive>()?
        .into_iter()
        .filter(|row| {
            row.base.space_id == manifest.space_id
                && row.base.deleted_at.is_none()
                && row.original_id == row.record.base.id
        })
        .map(|row| (row.original_id, row.record))
        .collect();
    for run in tx.scan::<WorkRun>()? {
        runs.insert(run.base.id.clone(), run);
    }
    for mapping in &manifest.documents {
        let id = manifest.embedding_work_id(&mapping.doc_revision_id)?;
        let item = completed_item_tx(tx, &manifest.space_id, &id)?;
        if item.kind != "embed_source_revision"
            || item.attempts == 0
            || item.payload.get("_publication")
                != Some(
                    &serde_json::to_value(&manifest.publication)
                        .map_err(|error| integrity(&error.to_string()))?,
                )
            || item
                .payload
                .get("revision_id")
                .and_then(serde_json::Value::as_str)
                != Some(mapping.doc_revision_id.to_string().as_str())
        {
            return Err(conflict("source_embedding_work_identity_mismatch"));
        }
        replan::check_work_plan(
            &item,
            &manifest.publication,
            &manifest.configuration,
            replan::replanned_tx(tx, manifest)?,
        )?;
        // Retry allowance is refunded for local dependency/configuration waits.
        // Only the durable completed claim identifies the run that may satisfy
        // publication; earlier runs with the same retry count remain history.
        let authority = crate::work_attempt_repository::read_attempt(tx, &id)?
            .ok_or_else(|| conflict("source_embedding_attempt_missing"))?;
        if authority.space_id != manifest.space_id
            || authority.work_item_id != id
            || authority.status != "done"
            || authority.generation == 0
            || authority.lease_token.is_empty()
        {
            return Err(conflict("source_embedding_attempt_changed"));
        }
        let captured = serde_json::to_value(AttemptFence {
            space_id: authority.space_id,
            work_item_id: authority.work_item_id,
            generation: authority.generation,
            lease_token: authority.lease_token,
        })
        .map_err(|error| integrity(&error.to_string()))?;
        if item
            .payload
            .get(crate::work_attempt_repository::ATTEMPT_PAYLOAD_KEY)
            != Some(&captured)
        {
            return Err(conflict("source_embedding_attempt_changed"));
        }
        let matching: Vec<_> = runs
            .values()
            .filter(|run| {
                run.base.space_id == manifest.space_id
                    && run.base.deleted_at.is_none()
                    && run.work_item.id() == &id
                    && run.metrics.custom.get("attempt_fence") == Some(&captured)
            })
            .collect();
        if matching.len() != 1
            || matching[0].attempt != item.attempts
            || !matching[0].success
            || matching[0].completed_at.is_none()
        {
            return Err(incomplete("embedding_run_not_successful"));
        }
    }
    Ok(())
}

fn append_embedding_work_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    for (index, mapping) in manifest.documents.iter().enumerate() {
        let prefix = format!("sf_embed_work_{index}");
        let id = manifest.embedding_work_id(&mapping.doc_revision_id)?;
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(&id).into(),
        );
        bind_json(
            bindings,
            &format!("{prefix}_revision"),
            mapping.doc_revision_id.to_string(),
        )?;
        bindings.insert(
            format!("{prefix}_attempt_id"),
            crate::work_attempt_repository::attempt_id(&id).into(),
        );
        bind_json(bindings, &format!("{prefix}_id_string"), id.to_string())?;
        append_history_sql(sql, &prefix, &format!("${prefix}_id"), "work_item");
        replan::append_work_plan_sql(sql, &prefix, "$sf_replanned");
        let _ = writeln!(sql,"IF ${prefix}.status != 'done' {{ THROW 'publication:source_publication_incomplete:embedding_work_not_done'; }};\n\
            IF ${prefix}.kind != 'embed_source_revision' OR ${prefix}.attempts <= 0 OR ${prefix}.payload._publication != $sf_fence OR ${prefix}.payload.revision_id != ${prefix}_revision {{ THROW 'publication:source_embedding_work_identity_mismatch'; }};\n\
            LET ${prefix}_authority = SELECT * FROM ONLY ${prefix}_attempt_id;\n\
            IF ${prefix}_authority = NONE OR ${prefix}_authority.space_id != $sf_space OR ${prefix}_authority.work_item_id != ${prefix}_id OR ${prefix}_authority.status != 'done' OR !type::is::int(${prefix}_authority.generation) OR ${prefix}_authority.generation <= 0 OR !type::is::string(${prefix}_authority.lease_token) OR ${prefix}_authority.lease_token = '' {{ THROW 'publication:source_embedding_attempt_changed'; }};\n\
            LET ${prefix}_captured = {{ space_id: $sf_space, work_item_id: ${prefix}_id_string, generation: ${prefix}_authority.generation, lease_token: ${prefix}_authority.lease_token }};\n\
            IF ${prefix}.payload._attempt_fence != ${prefix}_captured {{ THROW 'publication:source_embedding_attempt_changed'; }};\n\
            LET ${prefix}_runs = array::concat((SELECT * FROM work_run WHERE space_id = $sf_space AND deleted_at = NONE AND work_item = ${prefix}_id AND metrics.custom.attempt_fence = ${prefix}_captured), (SELECT VALUE record FROM work_run_archive WHERE space_id = $sf_space AND deleted_at = NONE AND record.space_id = $sf_space AND record.deleted_at = NONE AND record.work_item = ${prefix}_id AND record.metrics.custom.attempt_fence = ${prefix}_captured));\n\
            IF array::len(${prefix}_runs) != 1 OR ${prefix}_runs[0].attempt != ${prefix}.attempts OR ${prefix}_runs[0].success != true OR ${prefix}_runs[0].completed_at = NONE {{ THROW 'publication:source_publication_incomplete:embedding_run_not_successful'; }};");
        append_history_sql(
            sql,
            &format!("{prefix}_run"),
            &format!("${prefix}_runs[0].id"),
            "work_run",
        );
    }
    Ok(())
}

fn registration_key(space: &str, publication: &PublicationFence, artifact: &RecordId) -> String {
    stable_key(&[&manifest_key(space, publication), &artifact.to_string()])
}

fn check_registration_retry(
    old: &SourceBundleRegistration,
    new: &SourceBundleRegistration,
) -> StorageResult<()> {
    if old.space_id != new.space_id
        || old.publication != new.publication
        || old.artifact_id != new.artifact_id
        || old.attempt.work_item_id != new.attempt.work_item_id
        || old.attempt.generation > new.attempt.generation
        || (old.attempt.generation == new.attempt.generation && old != new)
    {
        return Err(conflict("source_registration_conflict"));
    }
    Ok(())
}

fn check_registration_work(
    item: &WorkItem,
    publication: &PublicationFence,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    if item.kind != "commit_artifacts"
        || item.payload.get("_publication")
            != Some(
                &serde_json::to_value(publication)
                    .map_err(|error| integrity(&error.to_string()))?,
            )
        || item
            .payload
            .get("work_run_id")
            .and_then(serde_json::Value::as_str)
            != Some(artifact.work_run.id().to_string().as_str())
    {
        return Err(conflict("source_registration_work_mismatch"));
    }
    Ok(())
}

fn completed_item_tx(
    tx: &EmbeddedMutation<'_>,
    space: &str,
    id: &RecordId,
) -> StorageResult<WorkItem> {
    let item = if let Some(hot) = tx.get::<WorkItem>(space, id, true)? {
        hot
    } else {
        tx.get::<WorkItemArchive>(space, &crate::work_retention::archive_id(id), false)?
            .filter(|row| row.original_id == *id)
            .map(|row| row.record)
            .ok_or_else(|| incomplete("predecessor_work_missing"))?
    };
    if item.base.id != *id
        || item.base.space_id != space
        || item.base.deleted_at.is_some()
        || item.status != WorkItemStatus::Done
    {
        return Err(incomplete("predecessor_work_not_done"));
    }
    Ok(item)
}

fn origin_run_tx(tx: &EmbeddedMutation<'_>, space: &str, id: &RecordId) -> StorageResult<WorkRun> {
    let run = if let Some(hot) = tx.get::<WorkRun>(space, id, true)? {
        hot
    } else {
        tx.get::<WorkRunArchive>(space, &crate::work_retention::archive_id(id), false)?
            .filter(|row| row.original_id == *id)
            .map(|row| row.record)
            .ok_or_else(|| incomplete("origin_run_missing"))?
    };
    if run.base.id != *id || run.base.space_id != space || run.base.deleted_at.is_some() {
        return Err(incomplete("origin_run_missing"));
    }
    Ok(run)
}

fn check_origin_tx(
    tx: &EmbeddedMutation<'_>,
    space: &str,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    let id = artifact.work_run.id();
    let run = origin_run_tx(tx, space, id)?;
    if run.base.id != *id
        || run.base.space_id != space
        || run.base.deleted_at.is_some()
        || !run.success
        || run.completed_at.is_none()
    {
        return Err(incomplete("origin_run_not_successful"));
    }
    completed_item_tx(tx, space, run.work_item.id())?;
    Ok(())
}

fn check_registration_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    let registration = read_control_tx::<SourceBundleRegistration>(
        tx,
        REGISTRATION_TABLE,
        &registration_key(&manifest.space_id, &manifest.publication, &artifact.base.id),
        &manifest.space_id,
    )?
    .ok_or_else(|| incomplete("bundle_registration_missing"))?;
    if registration.space_id != manifest.space_id
        || registration.publication != manifest.publication
        || registration.artifact_id != artifact.base.id
    {
        return Err(conflict("source_registration_identity_mismatch"));
    }
    let item = completed_item_tx(tx, &manifest.space_id, &registration.attempt.work_item_id)?;
    check_registration_work(&item, &manifest.publication, artifact)?;
    replan::check_work_plan(
        &item,
        &manifest.publication,
        &manifest.configuration,
        replan::replanned_tx(tx, manifest)?,
    )?;
    let captured = serde_json::to_value(&registration.attempt)
        .map_err(|error| integrity(&error.to_string()))?;
    if registration.attempt.space_id != manifest.space_id
        || registration.attempt.generation == 0
        || registration.attempt.lease_token.is_empty()
        || item
            .payload
            .get(crate::work_attempt_repository::ATTEMPT_PAYLOAD_KEY)
            != Some(&captured)
    {
        return Err(conflict("source_registration_attempt_changed"));
    }
    Ok(())
}

// Native archive records retain native IDs and dates, just as their hot rows.
// Prefer an existing hot record even when deleted: an older archive must not
// resurrect a deleted predecessor. All nested records are scoped again.
// Pass the full Thing to type::thing: pinned 2.6.5 coerces a standalone UUID
// argument to String, which addresses a different row than an archived UUID ID.
fn append_history_sql(sql: &mut String, prefix: &str, id: &str, table: &str) {
    let _ = writeln!(sql,"LET ${prefix}_hot = SELECT * FROM ONLY {id};\n\
        LET ${prefix}_cold = SELECT * FROM ONLY type::thing('{table}_archive', {id});\n\
        LET ${prefix} = IF ${prefix}_hot != NONE THEN ${prefix}_hot ELSE IF ${prefix}_cold.space_id = $sf_space AND ${prefix}_cold.deleted_at = NONE AND ${prefix}_cold.original_id = {id} THEN ${prefix}_cold.record ELSE NONE END;\n\
        IF ${prefix} = NONE OR ${prefix}.id != {id} OR ${prefix}.space_id != $sf_space OR ${prefix}.deleted_at != NONE {{ THROW 'publication:source_publication_incomplete:predecessor_missing'; }};");
}

fn append_origin_sql(sql: &mut String, artifact: &str) {
    let run = format!("{artifact}_origin_run");
    let item = format!("{artifact}_origin_item");
    append_history_sql(sql, &run, &format!("${artifact}.work_run"), "work_run");
    let _ = writeln!(sql,"IF ${run}.success != true OR ${run}.completed_at = NONE {{ THROW 'publication:source_publication_incomplete:origin_run_not_successful'; }};");
    append_history_sql(sql, &item, &format!("${run}.work_item"), "work_item");
    let _ = writeln!(sql,"IF ${item}.status != 'done' {{ THROW 'publication:source_publication_incomplete:predecessor_work_not_done'; }};");
}

fn append_registration_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    manifest: &SourcePublicationManifest,
    artifact_id: &RecordId,
    artifact: &str,
) -> StorageResult<()> {
    let prefix = format!("{artifact}_registration");
    bindings.insert(
        format!("{prefix}_id"),
        control_thing(
            REGISTRATION_TABLE,
            &registration_key(&manifest.space_id, &manifest.publication, artifact_id),
        )
        .into(),
    );
    bind_json(
        bindings,
        &format!("{prefix}_artifact"),
        artifact_id.to_string(),
    )?;
    let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
        IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.source_id != $sf_source_string OR ${prefix}.state.space_id != $sf_space OR ${prefix}.state.publication != $sf_fence OR ${prefix}.state.artifact_id != ${prefix}_artifact {{ THROW 'publication:source_publication_incomplete:bundle_registration_missing'; }};\n\
        IF ${prefix}.state.attempt.space_id != $sf_space OR ${prefix}.state.attempt.generation <= 0 OR ${prefix}.state.attempt.lease_token = '' OR string::split(${prefix}.state.attempt.work_item_id, ':')[0] != 'work_item' {{ THROW 'publication:source_registration_attempt_changed'; }};\n\
        LET ${prefix}_work_id = type::thing(string::concat(\"work_item:u'\", string::split(${prefix}.state.attempt.work_item_id, ':')[1], \"'\"));");
    let item = format!("{prefix}_work");
    append_history_sql(sql, &item, &format!("${prefix}_work_id"), "work_item");
    replan::append_work_plan_sql(sql, &item, "$sf_replanned");
    let _ = writeln!(sql,"IF ${item}.status != 'done' {{ THROW 'publication:source_publication_incomplete:registration_work_not_done'; }};\n\
        IF ${item}.kind != 'commit_artifacts' OR ${item}.payload._publication != $sf_fence OR ${item}.payload._attempt_fence != ${prefix}.state.attempt OR ${item}.payload.work_run_id != string::concat(meta::tb(${artifact}.work_run), ':', meta::id(${artifact}.work_run)) {{ THROW 'publication:source_registration_attempt_changed'; }};");
    Ok(())
}

fn completion_for(
    requirements: &SourcePublicationRequirements,
    commit_id: &str,
) -> StorageResult<SourcePublicationCompletion> {
    if commit_id != requirements.commit_id() {
        return Err(conflict("source_completion_commit_identity_mismatch"));
    }
    let manifest = requirements.manifest();
    Ok(SourcePublicationCompletion {
        space_id: manifest.space_id.clone(),
        publication: manifest.publication.clone(),
        manifest_digest: stable_key(&[&encode_payload(manifest)?]),
        commit_id: commit_id.to_owned(),
        rendered_revision_fingerprints: render::revision_fingerprints(requirements)?,
        document_fingerprints: render::document_fingerprints(requirements)?,
        accepted_source_fingerprint: Some(render::accepted_source_fingerprint(requirements)?),
        published_progress: Some(
            requirements
                .progress
                .as_ref()
                .ok_or_else(|| incomplete("original_published_progress_missing"))?
                .certificate(manifest.mode)?,
        ),
        historical_embedding_coverage: None,
        completed_at: Utc::now(),
    })
}

/// Write only inside the canonical effects/head/operation/outbox transaction.
#[allow(
    clippy::too_many_lines,
    reason = "Completion verifies every committed artifact and source lifecycle effect inside the canonical transaction before writing the completion receipt; preserve that ordering."
)]
pub(crate) fn mark_completed_tx(
    tx: &mut EmbeddedMutation<'_>,
    requirements: &SourcePublicationRequirements,
    commit_id: &str,
) -> StorageResult<()> {
    let completion = completion_for(requirements, commit_id)?;
    let manifest = requirements.manifest();
    for id in &manifest.planning_artifact_ids {
        let plan = tx
            .get::<WorkArtifact>(&manifest.space_id, id, false)?
            .ok_or_else(|| incomplete("extraction_plan_missing"))?;
        if plan.status != ArtifactStatus::Committed
            || plan.committed_at.is_none()
            || plan.validated_at.is_none()
        {
            return Err(integrity(
                "canonical source plan omitted planning receipt completion",
            ));
        }
    }
    for id in manifest
        .metadata_artifact_ids
        .iter()
        .chain(manifest.bundles.iter().map(|bundle| &bundle.artifact_id))
    {
        let bundle = tx
            .get::<WorkArtifact>(&manifest.space_id, id, false)?
            .ok_or_else(|| incomplete("expected_bundle_missing"))?;
        if bundle.status != ArtifactStatus::Committed || bundle.committed_at.is_none() {
            return Err(integrity(
                "canonical source plan omitted required artifact completion",
            ));
        }
        let candidate_id = bundle
            .data
            .get("source_artifact_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| integrity("bundle candidate identity missing"))?;
        let candidate = tx
            .get::<WorkArtifact>(
                &manifest.space_id,
                &RecordId::from_str(candidate_id)?,
                false,
            )?
            .ok_or_else(|| incomplete("validated_candidate_missing"))?;
        if candidate.status != ArtifactStatus::Committed || candidate.committed_at.is_none() {
            return Err(integrity(
                "canonical source plan omitted candidate completion",
            ));
        }
    }
    let source = tx
        .get::<elephant_types::records::Source>(
            &manifest.space_id,
            &manifest.publication.source_id,
            false,
        )?
        .ok_or_else(|| conflict("source_missing"))?;
    if source.current_ingest_mode != Some(manifest.mode)
        || source.lifecycle_status.to_string() != completed_lifecycle(manifest.mode)
        || source.latest_ingest_decision != requirements.published_ingest_decision()?
    {
        return Err(integrity(
            "canonical source plan omitted source lifecycle completion",
        ));
    }
    let key = manifest_key(&completion.space_id, &completion.publication);
    if let Some(old) = read_control_tx::<SourcePublicationCompletion>(
        tx,
        COMPLETION_TABLE,
        &key,
        &completion.space_id,
    )? {
        if old.commit_id != completion.commit_id
            || old.manifest_digest != completion.manifest_digest
            || old.rendered_revision_fingerprints != completion.rendered_revision_fingerprints
            || old.document_fingerprints != completion.document_fingerprints
            || old.accepted_source_fingerprint != completion.accepted_source_fingerprint
            || old.published_progress != completion.published_progress
        {
            return Err(conflict("source_completion_conflict"));
        }
        if read_control_tx::<SourcePublicationCompletion>(
            tx,
            COMPLETION_TABLE,
            &progress::published_completion_key(
                &completion.space_id,
                &completion.publication.source_id,
            ),
            &completion.space_id,
        )?
        .as_ref()
            != Some(&old)
        {
            return Err(conflict("source_progress_lookup_changed"));
        }
        return Ok(());
    }
    write_control_tx(
        tx,
        COMPLETION_TABLE,
        &key,
        &completion.space_id,
        &completion.publication.source_id,
        &completion,
    )?;
    write_control_tx(
        tx,
        COMPLETION_TABLE,
        &progress::published_completion_key(
            &completion.space_id,
            &completion.publication.source_id,
        ),
        &completion.space_id,
        &completion.publication.source_id,
        &completion,
    )?;
    tx.advance_canonical_epoch(&completion.space_id)?;
    Ok(())
}

const fn completed_lifecycle(mode: IngestMode) -> &'static str {
    match mode {
        IngestMode::Skip => "skipped",
        IngestMode::Metadata => "metadata_ready",
        IngestMode::Retrieval => "retrieval_ready",
        IngestMode::Graph => "graph_ready",
    }
}

fn append_committed_effects_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    sql.push_str("FOR $sf_completed_plan_id IN $sf_plan_ids {\n\
        LET $sf_completed_plan = SELECT * FROM ONLY $sf_completed_plan_id;\n\
        IF $sf_completed_plan = NONE OR $sf_completed_plan.space_id != $sf_space OR $sf_completed_plan.status != 'committed' OR $sf_completed_plan.validated_at = NONE OR $sf_completed_plan.committed_at = NONE { THROW 'publication:source_plan_omitted_planning_receipt_completion'; };\n\
    };\n");
    for (index, id) in manifest
        .metadata_artifact_ids
        .iter()
        .chain(manifest.bundles.iter().map(|bundle| &bundle.artifact_id))
        .enumerate()
    {
        let prefix = format!("sf_done_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(id).into(),
        );
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.status != 'committed' OR ${prefix}.committed_at = NONE {{ THROW 'publication:source_plan_omitted_artifact_completion'; }};\n\
            IF string::split(${prefix}.data.source_artifact_id, ':')[0] != 'work_artifact' {{ THROW 'publication:invalid_candidate_identity'; }};\n\
            LET ${prefix}_candidate_id = type::thing(string::concat(\"work_artifact:u'\", string::split(${prefix}.data.source_artifact_id, ':')[1], \"'\"));\n\
            LET ${prefix}_candidate = SELECT * FROM ONLY ${prefix}_candidate_id;\n\
            IF ${prefix}_candidate = NONE OR ${prefix}_candidate.space_id != $sf_space OR ${prefix}_candidate.status != 'committed' OR ${prefix}_candidate.committed_at = NONE {{ THROW 'publication:source_plan_omitted_candidate_completion'; }};");
    }
    bind_json(bindings, "sf_completed_mode", manifest.mode.to_string())?;
    bind_json(
        bindings,
        "sf_completed_lifecycle",
        completed_lifecycle(manifest.mode),
    )?;
    sql.push_str("LET $sf_done_source = SELECT * FROM ONLY $sf_source_id;\n\
        IF $sf_done_source = NONE OR $sf_done_source.current_ingest_mode != $sf_completed_mode OR $sf_done_source.lifecycle_status != $sf_completed_lifecycle { THROW 'publication:source_plan_omitted_lifecycle_completion'; };\n");
    Ok(())
}

#[cfg(test)]
#[path = "source_publication_manifest_tests.rs"]
mod tests;
