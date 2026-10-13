//! Configuration-version transitions for an unpublished accepted source.
//! SourceHead remains the only acceptance/publication authority.

use super::{
    append_history_sql, append_identity_sql, append_mapping_writes_sql, append_plan_sql, bind_json,
    check_configuration_tx, check_expected_bundle, check_origin_tx, check_plan_tx, check_source_tx,
    completed_item_tx, conflict, control_thing, control_value, encode_payload, head_matches,
    incomplete, inputs, integrity, manifest_key, origin_run_tx, publication_error, read_control_tx,
    source_embedding_work_id, stable_key, write_control_tx, write_mapping_tx, ArtifactStatus,
    AttemptFence, BTreeMap, BTreeSet, DateTime, Deserialize, EmbeddedMutation, FromStr,
    PreparedStandingWatchAuthority, PublicationFence, RecordId, Repository, Serialize,
    SourcePublicationCompletion, SourcePublicationManifest, SourcePublicationManifestRepository,
    SourcePublicationRepository, SourcePublicationRequirements, SpaceConfig, SpaceConfigToken,
    StorageResult, TableName, Utc, Uuid, Value, WorkArtifact, WorkItem, COMPLETION_TABLE,
    MANIFEST_TABLE,
};
use std::fmt::Write as _;

pub(super) const HISTORY_TABLE: &str = "source_publication_manifest_history";
const CONFIGURATION_KEY: &str = "_source_configuration";
const PLAN_KEY: &str = "_source_plan";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct ManifestHistory {
    previous: SourcePublicationManifest,
    successor_configuration: SpaceConfigToken,
    original_attempt: AttemptFence,
    replaced_at: DateTime<Utc>,
}

/// Stable processing-plan identity within the original accepted source fence.
/// Credential rotation changes this work version, never the vector partition.
#[must_use]
pub fn source_plan_id(publication: &PublicationFence, configuration: &SpaceConfigToken) -> String {
    stable_key(&[
        &configuration.space_id,
        &publication.source_id.to_string(),
        &publication.generation,
        &publication.revision_id,
        &publication.operation_id,
        &configuration.config_id,
        &configuration.generation.to_string(),
        &configuration.policy_id,
    ])
}

/// Future bundle identity bound to both accepted input and processing configuration.
#[must_use]
pub fn source_bundle_id_for_plan(
    publication: &PublicationFence,
    configuration: &SpaceConfigToken,
    revision: &RecordId,
    node: &RecordId,
) -> RecordId {
    let key = format!(
        "source-bundle:{}:{revision}:{node}",
        source_plan_id(publication, configuration)
    );
    RecordId::new(
        TableName::WorkArtifact,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()),
    )
}

/// Configuration-specific queue identity, matching source_queue's enqueue key.
#[must_use]
pub fn source_embedding_work_id_for_plan(
    space: &str,
    publication: &PublicationFence,
    configuration: &SpaceConfigToken,
    revision: &RecordId,
) -> RecordId {
    let kind = "embed_source_revision";
    let plan = source_plan_id(publication, configuration);
    let key = format!(
        "{kind}:{}:{}:{revision}:{plan}",
        publication.source_id, publication.generation
    );
    let identity = serde_json::json!([space, kind, key]).to_string();
    RecordId::new(
        TableName::WorkItem,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, identity.as_bytes()),
    )
}

impl SourcePublicationManifest {
    /// Canonical content identity used by the atomic unpublished-replan compare.
    ///
    /// # Errors
    /// Returns an error if the closed manifest cannot be serialized.
    pub fn digest(&self) -> StorageResult<String> {
        Ok(stable_key(&[&encode_payload(self)?]))
    }

    /// Resolve the exact declared embedding job for a rendered revision.
    /// Initial pre-versioned plans remain readable; replacements require versioned jobs.
    ///
    /// # Errors
    /// Rejects an absent or ambiguous job identity in the closed manifest.
    pub fn embedding_work_id(&self, revision: &RecordId) -> StorageResult<RecordId> {
        let versioned = source_embedding_work_id_for_plan(
            &self.space_id,
            &self.publication,
            &self.configuration,
            revision,
        );
        let legacy = source_embedding_work_id(&self.space_id, &self.publication, revision);
        match (
            self.embedding_work_item_ids.contains(&versioned),
            self.embedding_work_item_ids.contains(&legacy),
        ) {
            (true, false) => Ok(versioned),
            (false, true) => Ok(legacy),
            _ => Err(integrity(
                "ambiguous or missing source embedding work identity",
            )),
        }
    }
}

impl SourcePublicationRequirements {
    /// Ensure the commit context carries the same original attempt as the sealed proof.
    /// Unbound initial proofs remain compatible; replanned proofs must bind an attempt.
    #[must_use]
    pub fn matches_attempt(&self, attempt: &AttemptFence) -> bool {
        self.original_attempt
            .as_ref()
            .is_none_or(|original| original == attempt)
    }
}

fn history_key(manifest: &SourcePublicationManifest) -> String {
    source_plan_id(&manifest.publication, &manifest.configuration)
}

pub(super) fn configuration_from_work(item: &WorkItem) -> StorageResult<Option<SpaceConfigToken>> {
    item.payload
        .get(CONFIGURATION_KEY)
        .cloned()
        .map(|value| serde_json::from_value(value).map_err(|error| integrity(&error.to_string())))
        .transpose()
}

pub(super) fn check_work_plan(
    item: &WorkItem,
    publication: &PublicationFence,
    configuration: &SpaceConfigToken,
    required: bool,
) -> StorageResult<()> {
    let captured = configuration_from_work(item)?;
    let plan = item
        .payload
        .get(PLAN_KEY)
        .and_then(serde_json::Value::as_str);
    if !required && captured.is_none() && plan.is_none() {
        return Ok(());
    }
    if item.base.space_id != configuration.space_id
        || captured.as_ref() != Some(configuration)
        || plan != Some(source_plan_id(publication, configuration).as_str())
        || item.payload.get("_publication")
            != Some(
                &serde_json::to_value(publication)
                    .map_err(|error| integrity(&error.to_string()))?,
            )
    {
        return Err(conflict("source_plan_attempt_mismatch"));
    }
    Ok(())
}

pub(super) fn replanned_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<bool> {
    Ok(read_control_tx::<ManifestHistory>(
        tx,
        HISTORY_TABLE,
        &history_key(manifest),
        &manifest.space_id,
    )?
    .is_some())
}

pub(super) fn check_current_configuration_tx(
    tx: &EmbeddedMutation<'_>,
    configuration: &SpaceConfigToken,
) -> StorageResult<()> {
    let id = RecordId::from_str(&configuration.config_id)?;
    let current = tx
        .get::<SpaceConfig>(&configuration.space_id, &id, false)?
        .ok_or_else(|| conflict("unknown_space"))?;
    let processing = current
        .processing
        .ok_or_else(|| conflict("processing_configuration_missing"))?;
    if processing.generation != configuration.generation
        || processing.policy_id != configuration.policy_id
    {
        return Err(conflict("source_configuration_changed"));
    }
    processing
        .validate()
        .map_err(|_| conflict("processing_configuration_invalid"))
}

pub(super) fn append_current_configuration_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    configuration: &SpaceConfigToken,
) -> StorageResult<()> {
    bindings.insert(
        "sf_scope_config_id".into(),
        crate::surreal::thing_from_record_id(&RecordId::from_str(&configuration.config_id)?).into(),
    );
    bind_json(bindings, "sf_scope_configuration", configuration)?;
    sql.push_str("LET $sf_scope_cfg = SELECT * FROM ONLY $sf_scope_config_id;\n\
        IF $sf_scope_cfg = NONE OR $sf_scope_cfg.space_id != $sf_scope_configuration.space_id OR $sf_scope_cfg.deleted_at != NONE OR $sf_scope_cfg.processing = NONE OR $sf_scope_cfg.processing.generation != $sf_scope_configuration.generation OR $sf_scope_cfg.processing.policy_id != $sf_scope_configuration.policy_id { THROW 'publication:source_configuration_changed'; };\n\
        fn::recall_reference_guard({space_id:$sf_scope_configuration.space_id}, false);\n");
    Ok(())
}

pub(super) fn check_artifact_plan_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
    artifact: &WorkArtifact,
) -> StorageResult<()> {
    let run = origin_run_tx(tx, &manifest.space_id, artifact.work_run.id())?;
    let item = completed_item_tx(tx, &manifest.space_id, run.work_item.id())?;
    let required = replanned_tx(tx, manifest)?;
    check_work_plan(
        &item,
        &manifest.publication,
        &manifest.configuration,
        required,
    )?;
    let scoped = required || configuration_from_work(&item)?.is_some();
    if artifact.artifact_type == "extraction_plan" {
        check_artifact_tags(artifact, manifest, scoped)?;
    } else if artifact.artifact_type == "commit_bundle" {
        let id = artifact.data["source_artifact_id"]
            .as_str()
            .ok_or_else(|| integrity("candidate identity missing"))?;
        let candidate = tx
            .get::<WorkArtifact>(&manifest.space_id, &RecordId::from_str(id)?, false)?
            .ok_or_else(|| incomplete("validated_candidate_missing"))?;
        if candidate.work_run != artifact.work_run
            || !matches!(
                candidate.artifact_type.as_str(),
                "knowledge_candidates" | "source_metadata"
            )
            || artifact.data["source_artifact_type"] != candidate.artifact_type
        {
            return Err(conflict("source_candidate_plan_mismatch"));
        }
        check_artifact_tags(&candidate, manifest, scoped)?;
    }
    Ok(())
}

pub(super) fn check_finalizer_tx(
    tx: &mut EmbeddedMutation<'_>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    let required = replanned_tx(tx, manifest)?;
    let Some(attempt) = &requirements.original_attempt else {
        return if required {
            Err(conflict("source_plan_attempt_missing"))
        } else {
            Ok(())
        };
    };
    crate::work_attempt_repository::check_attempt_fence(tx, attempt, Utc::now())?;
    let item = tx
        .get::<WorkItem>(&manifest.space_id, &attempt.work_item_id, false)?
        .ok_or_else(|| incomplete("finalizer_work_missing"))?;
    if item.kind != "finalize_source_revision" {
        return Err(conflict("source_finalizer_work_mismatch"));
    }
    check_work_plan(
        &item,
        &manifest.publication,
        &manifest.configuration,
        required,
    )
}

pub(super) fn append_replan_scope_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    bindings.insert(
        "sf_plan_history_id".into(),
        control_thing(HISTORY_TABLE, &history_key(manifest)).into(),
    );
    bind_json(bindings, "sf_plan_configuration", &manifest.configuration)?;
    bind_json(
        bindings,
        "sf_plan_version",
        source_plan_id(&manifest.publication, &manifest.configuration),
    )?;
    sql.push_str("LET $sf_replanned = (SELECT * FROM ONLY $sf_plan_history_id) != NONE;\n");
    Ok(())
}

pub(super) fn append_work_plan_sql(sql: &mut String, item: &str, required: &str) {
    let _ = writeln!(sql,"IF {required} OR ${item}.payload._source_configuration != NONE OR ${item}.payload._source_plan != NONE {{\n\
        IF ${item}.space_id != $sf_space OR ${item}.payload._publication != $sf_fence OR ${item}.payload._source_configuration != $sf_plan_configuration OR ${item}.payload._source_plan != $sf_plan_version {{ THROW 'publication:source_plan_attempt_mismatch'; }};\n}};");
}

pub(super) fn append_finalizer_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    append_replan_scope_sql(sql, bindings, requirements.manifest())?;
    if let Some(attempt) = &requirements.original_attempt {
        crate::work_attempt_repository::append_attempt_fence(sql, bindings, attempt)?;
        sql.push_str("IF $fenced_work.kind != 'finalize_source_revision' { THROW 'publication:source_finalizer_work_mismatch'; };\n");
        append_work_plan_sql(sql, "fenced_work", "$sf_replanned");
    } else {
        sql.push_str("IF $sf_replanned { THROW 'publication:source_plan_attempt_missing'; };\n");
    }
    Ok(())
}

pub(super) struct RegistrationContext {
    pub(super) observed: Option<SourcePublicationManifest>,
    pub(super) manifest: Option<SourcePublicationManifest>,
    pub(super) configuration: Option<SpaceConfigToken>,
}

impl SourcePublicationManifestRepository {
    /// Seal the finalizer's own original claim into its private completion proof.
    ///
    /// # Errors
    /// Rejects missing expectations, stale claims, or mismatched processing plans.
    pub async fn requirements_for_attempt(
        &self,
        space: &str,
        publication: &PublicationFence,
        original_attempt: &AttemptFence,
    ) -> StorageResult<SourcePublicationRequirements> {
        let mut requirements = self.requirements(space, publication).await?;
        requirements.original_attempt = Some(original_attempt.clone());
        self.check_finalizer_attempt(&requirements).await?;
        Ok(requirements)
    }

    /// Validate original plan authority before even replaying a canonical receipt.
    ///
    /// # Errors
    /// Rejects expired or superseded attempts and propagates storage failures.
    pub async fn check_finalizer_attempt(
        &self,
        requirements: &SourcePublicationRequirements,
    ) -> StorageResult<()> {
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| check_finalizer_tx(tx, requirements))
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        bind_json(&mut bindings, "sf_space", &requirements.manifest.space_id)?;
        bind_json(
            &mut bindings,
            "sf_fence",
            &requirements.manifest.publication,
        )?;
        append_finalizer_sql(&mut sql, &mut bindings, requirements)?;
        sql.push_str("COMMIT TRANSACTION;");
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(())
    }

    /// Resolve only the manifest applicable to this original registration claim.
    /// A newer current configuration may stage metadata before its replacement seal.
    ///
    /// # Errors
    /// Rejects stale attempts, configuration drift, and completed-plan replacement.
    pub async fn staging_manifest_for_attempt(
        &self,
        space: &str,
        publication: &PublicationFence,
        attempt: &AttemptFence,
    ) -> StorageResult<Option<SourcePublicationManifest>> {
        Ok(self
            .registration_context(space, publication, attempt)
            .await?
            .manifest)
    }

    pub(super) async fn registration_context(
        &self,
        space: &str,
        publication: &PublicationFence,
        attempt: &AttemptFence,
    ) -> StorageResult<RegistrationContext> {
        let item = crate::WorkItemRepository::new(self.client.clone())
            .get(space, &attempt.work_item_id)
            .await?
            .ok_or_else(|| incomplete("registration_work_missing"))?;
        if crate::work_attempt_repository::fence_from_claim(&item)? != *attempt {
            return Err(conflict("source_plan_attempt_mismatch"));
        }
        let observed = self.get(space, publication).await?;
        let configuration = configuration_from_work(&item)?;
        let Some(configuration) = configuration else {
            if let Some(manifest) = &observed {
                if self.replan_history(manifest).await?.is_some() {
                    return Err(conflict("source_plan_attempt_missing"));
                }
            }
            return Ok(RegistrationContext {
                manifest: observed.clone(),
                observed,
                configuration: None,
            });
        };
        check_work_plan(&item, publication, &configuration, true)?;
        let current = crate::SpaceConfigRepository::new(self.client.clone())
            .resolve_space(space)
            .await
            .map_err(|error| match error {
                crate::space_config_repository::SpaceResolutionError::Storage(error) => error,
                crate::space_config_repository::SpaceResolutionError::Authorization(error) => {
                    conflict(&format!("source_configuration_invalid:{error}"))
                }
            })?;
        if current.token != configuration {
            return Err(conflict("source_configuration_changed"));
        }
        let manifest = if let Some(manifest) = &observed {
            if manifest.configuration == configuration {
                Some(manifest.clone())
            } else {
                if manifest.configuration.config_id != configuration.config_id
                    || manifest.configuration.generation >= configuration.generation
                    || self.completion(space, publication).await?.is_some()
                {
                    return Err(conflict("source_replan_not_permitted"));
                }
                None
            }
        } else {
            None
        };
        Ok(RegistrationContext {
            observed,
            manifest,
            configuration: Some(configuration),
        })
    }

    async fn replan_history(
        &self,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<Option<ManifestHistory>> {
        SourcePublicationRepository::new(self.client.clone())
            .read_control(HISTORY_TABLE, &history_key(manifest), &manifest.space_id)
            .await
    }
}

fn check_transition(
    previous: &SourcePublicationManifest,
    next: &SourcePublicationManifest,
) -> StorageResult<()> {
    if previous.space_id != next.space_id
        || previous.publication != next.publication
        || previous.configuration.config_id != next.configuration.config_id
        || previous.configuration.generation >= next.configuration.generation
    {
        return Err(conflict("source_replan_identity_mismatch"));
    }
    let previous_artifacts: BTreeSet<_> = previous
        .metadata_artifact_ids
        .iter()
        .chain(&previous.planning_artifact_ids)
        .chain(previous.bundles.iter().map(|bundle| &bundle.artifact_id))
        .collect();
    if next
        .metadata_artifact_ids
        .iter()
        .chain(&next.planning_artifact_ids)
        .chain(next.bundles.iter().map(|bundle| &bundle.artifact_id))
        .any(|id| previous_artifacts.contains(id))
        || next.bundles.iter().any(|bundle| {
            bundle.artifact_id
                != source_bundle_id_for_plan(
                    &next.publication,
                    &next.configuration,
                    &bundle.revision_id,
                    &bundle.node_id,
                )
        })
        || next.documents.iter().any(|mapping| {
            !next
                .embedding_work_item_ids
                .contains(&source_embedding_work_id_for_plan(
                    &next.space_id,
                    &next.publication,
                    &next.configuration,
                    &mapping.doc_revision_id,
                ))
        })
    {
        return Err(conflict("source_replan_reused_old_work"));
    }
    Ok(())
}

fn check_unpublished_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<()> {
    check_source_tx(tx, manifest)?;
    // A fresh acceptance may reuse an already published immutable revision.
    // Completion belongs to the exact generation/operation, not its content key.
    if read_control_tx::<SourcePublicationCompletion>(
        tx,
        COMPLETION_TABLE,
        &manifest_key(&manifest.space_id, &manifest.publication),
        &manifest.space_id,
    )?
    .is_some()
    {
        return Err(conflict("source_plan_already_completed"));
    }
    Ok(())
}

fn check_sealing_artifacts_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
    item: &WorkItem,
) -> StorageResult<()> {
    for id in manifest
        .metadata_artifact_ids
        .iter()
        .chain(&manifest.planning_artifact_ids)
    {
        let artifact = tx
            .get::<WorkArtifact>(&manifest.space_id, id, false)?
            .ok_or_else(|| incomplete("replacement_artifact_missing"))?;
        let run = origin_run_tx(tx, &manifest.space_id, artifact.work_run.id())?;
        if manifest.planning_artifact_ids.contains(id) && run.work_item.id() == &item.base.id {
            if run.attempt != item.attempts || run.completed_at.is_some() {
                return Err(conflict("source_plan_attempt_mismatch"));
            }
            check_work_plan(item, &manifest.publication, &manifest.configuration, true)?;
        } else {
            check_origin_tx(tx, &manifest.space_id, &artifact)?;
            let producer = completed_item_tx(tx, &manifest.space_id, run.work_item.id())?;
            check_work_plan(
                &producer,
                &manifest.publication,
                &manifest.configuration,
                true,
            )?;
        }
        if manifest.planning_artifact_ids.contains(id) {
            check_artifact_tags(&artifact, manifest, true)?;
        }
        if manifest.metadata_artifact_ids.contains(id) {
            check_expected_bundle(manifest, &artifact)?;
            if artifact.status != ArtifactStatus::Validated {
                return Err(conflict("source_replan_reused_old_work"));
            }
        }
    }
    Ok(())
}

impl SourcePublicationManifestRepository {
    /// Atomically replace only unpublished expectations after an approved configuration change.
    ///
    /// The accepted source fence, old manifest digest, new configuration, and original live
    /// sealing attempt are compared in the same transaction. Old model outputs are not relabeled.
    ///
    /// # Errors
    /// Rejects any changed authority, incomplete new plan, or reused old output;
    /// propagates transaction failures without changing the active expectations.
    #[allow(
        clippy::too_many_lines,
        reason = "Replacing a plan atomically checks producer and publication authority, supersedes prior work, and writes the new plan on both backends; retain the complete transaction boundary."
    )]
    pub async fn replace_operation_plan(
        &self,
        space: &str,
        publication: &PublicationFence,
        expected_manifest_digest: &str,
        mut next: SourcePublicationManifest,
        original_attempt: &AttemptFence,
    ) -> StorageResult<SourcePublicationManifest> {
        next.normalize();
        next.validate()?;
        if next.space_id != space
            || next.publication != *publication
            || original_attempt.space_id != space
        {
            return Err(conflict("source_replan_identity_mismatch"));
        }
        let mut standing_watch = Box::pin(
            PreparedStandingWatchAuthority::capture_for_source_operation(
                &self.client,
                space,
                publication,
            ),
        )
        .await?;
        Box::pin(inputs::authorize_inputs(
            self,
            &mut standing_watch,
            space,
            publication,
            Some(&next),
            None,
        ))
        .await?;
        let current = self
            .get(space, publication)
            .await?
            .ok_or_else(|| incomplete("manifest_missing"))?;
        let existing_history = self.replan_history(&next).await?;
        let previous = if current == next {
            existing_history
                .as_ref()
                .map(|history| history.previous.clone())
                .ok_or_else(|| conflict("source_manifest_conflict"))?
        } else {
            current.clone()
        };
        if previous.digest()? != expected_manifest_digest {
            return Err(conflict("source_manifest_conflict"));
        }
        check_transition(&previous, &next)?;
        if existing_history.as_ref().is_some_and(|history| {
            history.previous != previous || history.successor_configuration != next.configuration
        }) {
            return Err(conflict("source_replan_history_conflict"));
        }
        let history = existing_history.unwrap_or_else(|| ManifestHistory {
            previous: previous.clone(),
            successor_configuration: next.configuration.clone(),
            original_attempt: original_attempt.clone(),
            replaced_at: Utc::now(),
        });
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    if let Some(authority) = &standing_watch {
                        authority.check_tx(tx)?;
                    }
                    crate::work_attempt_repository::check_attempt_fence(
                        tx,
                        original_attempt,
                        Utc::now(),
                    )?;
                    check_unpublished_tx(tx, &next)?;
                    check_configuration_tx(tx, &next)?;
                    let item = tx
                        .get::<WorkItem>(space, &original_attempt.work_item_id, false)?
                        .ok_or_else(|| incomplete("planner_work_missing"))?;
                    check_work_plan(&item, publication, &next.configuration, true)?;
                    let stored = read_control_tx::<SourcePublicationManifest>(
                        tx,
                        MANIFEST_TABLE,
                        &manifest_key(space, publication),
                        space,
                    )?
                    .ok_or_else(|| incomplete("manifest_missing"))?;
                    let old_history = read_control_tx::<ManifestHistory>(
                        tx,
                        HISTORY_TABLE,
                        &history_key(&next),
                        space,
                    )?;
                    if stored == next {
                        if old_history.as_ref() == Some(&history) {
                            return Ok(next);
                        }
                        return Err(conflict("source_replan_history_conflict"));
                    }
                    if stored != previous || old_history.is_some() {
                        return Err(conflict("source_manifest_conflict"));
                    }
                    check_plan_tx(tx, &next)?;
                    check_sealing_artifacts_tx(tx, &next, &item)?;
                    for id in &next.planning_artifact_ids {
                        let mut plan = tx
                            .get::<WorkArtifact>(space, id, false)?
                            .ok_or_else(|| incomplete("extraction_plan_missing"))?;
                        if plan.status == ArtifactStatus::Draft {
                            plan.status = ArtifactStatus::Validated;
                            plan.validated_at = Some(Utc::now());
                            plan.base.updated_at = Utc::now();
                            tx.replace(plan)?;
                        }
                    }
                    for mapping in &next.documents {
                        write_mapping_tx(tx, mapping)?;
                    }
                    write_control_tx(
                        tx,
                        HISTORY_TABLE,
                        &history_key(&next),
                        space,
                        &publication.source_id,
                        &history,
                    )?;
                    write_control_tx(
                        tx,
                        MANIFEST_TABLE,
                        &manifest_key(space, publication),
                        space,
                        &publication.source_id,
                        &next,
                    )?;
                    tx.advance_canonical_epoch(space)?;
                    Ok(next)
                })
                .await;
        }
        let head = SourcePublicationRepository::new(self.client.clone())
            .head(space, &publication.source_id)
            .await?
            .filter(|head| head_matches(head, &next))
            .ok_or_else(|| conflict("stale_source_publication"))?;
        let requirements = SourcePublicationRequirements {
            progress: None,
            revisions: self.load_revisions(&next).await?,
            documents: self.load_render_documents(&next).await?,
            accepted_source_control: Some(self.load_render_source(&next).await?),
            manifest: next.clone(),
            head,
            original_attempt: None,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        if let Some(authority) = &standing_watch {
            authority.append_guard_sql(&mut sql, &mut bindings)?;
        }
        append_identity_sql(&mut sql, &mut bindings, &requirements)?;
        append_replan_scope_sql(&mut sql, &mut bindings, &next)?;
        crate::work_attempt_repository::append_attempt_fence(
            &mut sql,
            &mut bindings,
            original_attempt,
        )?;
        append_work_plan_sql(&mut sql, "fenced_work", "true");
        bindings.insert(
            "sf_replan_completion_id".into(),
            control_thing(COMPLETION_TABLE, &manifest_key(space, publication)).into(),
        );
        bind_json(
            &mut bindings,
            "sf_previous_manifest_payload",
            encode_payload(&previous)?,
        )?;
        bind_json(
            &mut bindings,
            "sf_replan_history_payload",
            encode_payload(&history)?,
        )?;
        bindings.insert(
            "sf_replan_history_value".into(),
            control_value(space, &publication.source_id, &history)?,
        );
        sql.push_str("IF (SELECT * FROM ONLY $sf_replan_completion_id) != NONE { THROW 'publication:source_plan_already_completed'; };\n\
            LET $sf_replan_current = SELECT * FROM ONLY $sf_manifest_id;\n\
            LET $sf_replan_history = SELECT * FROM ONLY $sf_plan_history_id;\n\
            IF $sf_replan_current = NONE OR $sf_replan_current.space_id != $sf_space OR $sf_replan_current.payload NOT IN [$sf_previous_manifest_payload, $sf_manifest_payload] { THROW 'publication:source_manifest_conflict'; };\n\
            IF $sf_replan_current.payload = $sf_manifest_payload AND ($sf_replan_history = NONE OR $sf_replan_history.payload != $sf_replan_history_payload) { THROW 'publication:source_replan_history_conflict'; };\n\
            IF $sf_replan_current.payload = $sf_previous_manifest_payload {\n\
                IF $sf_replan_history != NONE { THROW 'publication:source_replan_history_conflict'; };\n");
        append_plan_sql(&mut sql, &mut bindings, &requirements)?;
        for (index, id) in next
            .metadata_artifact_ids
            .iter()
            .chain(&next.planning_artifact_ids)
            .enumerate()
        {
            let artifact = format!("sf_replan_artifact_{index}");
            bindings.insert(
                format!("{artifact}_id"),
                crate::surreal::thing_from_record_id(id).into(),
            );
            let _ = writeln!(sql,"LET ${artifact} = SELECT * FROM ONLY ${artifact}_id;\n\
                IF ${artifact} = NONE OR ${artifact}.space_id != $sf_space OR ${artifact}.deleted_at != NONE {{ THROW 'publication:source_publication_incomplete:replacement_artifact_missing'; }};");
            let run = format!("{artifact}_run");
            let producer = format!("{artifact}_producer");
            append_history_sql(&mut sql, &run, &format!("${artifact}.work_run"), "work_run");
            append_history_sql(
                &mut sql,
                &producer,
                &format!("${run}.work_item"),
                "work_item",
            );
            append_work_plan_sql(&mut sql, &producer, "true");
            if next.metadata_artifact_ids.contains(id) {
                let _ = writeln!(sql,"IF ${artifact}.artifact_type != 'commit_bundle' OR ${artifact}.status != 'validated' OR ${artifact}.validated_at = NONE OR ${artifact}.data.publication != $sf_fence OR ${artifact}.data.bundle_kind != 'source_metadata' OR ${artifact}.data.payload.kind != 'source_metadata' OR ${artifact}.data.payload.data.source_id != $sf_source_string {{ THROW 'publication:source_replan_reused_old_work'; }};");
                let _ = writeln!(sql,"IF ${run}.success != true OR ${run}.completed_at = NONE OR ${producer}.status != 'done' {{ THROW 'publication:source_publication_incomplete:origin_run_not_successful'; }};");
            } else {
                append_artifact_tags_sql(&mut sql, &artifact, "true");
                let _ = writeln!(sql,"IF ${producer}.id = $attempt_work_item {{\n\
                    IF ${run}.attempt != $fenced_work.attempts OR ${run}.completed_at != NONE {{ THROW 'publication:source_plan_attempt_mismatch'; }};\n\
                }} ELSE {{\n\
                    IF ${run}.success != true OR ${run}.completed_at = NONE OR ${producer}.status != 'done' {{ THROW 'publication:source_publication_incomplete:origin_run_not_successful'; }};\n\
                }};");
            }
        }
        sql.push_str("UPDATE $sf_plan_ids SET status = 'validated', validated_at = time::now(), updated_at = time::now() WHERE status = 'draft';\n");
        append_mapping_writes_sql(&mut sql, &mut bindings, &next)?;
        sql.push_str(
            "CREATE $sf_plan_history_id CONTENT $sf_replan_history_value;\n\
            UPDATE $sf_manifest_id SET payload = $sf_manifest_payload;\n\
            };\nCOMMIT TRANSACTION;",
        );
        self.client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(publication_error)?;
        Ok(next)
    }
}

fn check_artifact_tags(
    artifact: &WorkArtifact,
    manifest: &SourcePublicationManifest,
    required: bool,
) -> StorageResult<()> {
    let publication = serde_json::to_value(&manifest.publication)
        .map_err(|error| integrity(&error.to_string()))?;
    if artifact
        .data
        .get("_publication")
        .is_some_and(|value| value != &publication)
    {
        return Err(conflict("source_artifact_plan_mismatch"));
    }
    let configuration = artifact.data.get(CONFIGURATION_KEY);
    let plan = artifact
        .data
        .get(PLAN_KEY)
        .and_then(serde_json::Value::as_str);
    if !required && configuration.is_none() && plan.is_none() {
        return Ok(());
    }
    if configuration
        != Some(
            &serde_json::to_value(&manifest.configuration)
                .map_err(|error| integrity(&error.to_string()))?,
        )
        || plan != Some(source_plan_id(&manifest.publication, &manifest.configuration).as_str())
    {
        return Err(conflict("source_artifact_plan_mismatch"));
    }
    Ok(())
}

pub(super) fn append_artifact_tags_sql(sql: &mut String, artifact: &str, required: &str) {
    let _ = writeln!(sql,"IF ${artifact}.data._publication != NONE AND ${artifact}.data._publication != $sf_fence {{ THROW 'publication:source_artifact_plan_mismatch'; }};\n\
        IF {required} OR ${artifact}.data._source_configuration != NONE OR ${artifact}.data._source_plan != NONE {{\n\
        IF ${artifact}.data._source_configuration != $sf_plan_configuration OR ${artifact}.data._source_plan != $sf_plan_version {{ THROW 'publication:source_artifact_plan_mismatch'; }};\n}};");
}

pub(super) fn append_candidate_tags_sql(sql: &mut String, artifact: &str) {
    let _ = writeln!(sql,"IF ${artifact}.data.payload.kind IN ['knowledge_candidates', 'source_metadata'] {{\n\
        IF string::split(${artifact}.data.source_artifact_id, ':')[0] != 'work_artifact' {{ THROW 'publication:invalid_candidate_identity'; }};\n\
        LET ${artifact}_plan_candidate_id = type::thing(string::concat(\"work_artifact:u'\", string::split(${artifact}.data.source_artifact_id, ':')[1], \"'\"));\n\
        LET ${artifact}_plan_candidate = SELECT * FROM ONLY ${artifact}_plan_candidate_id;\n\
        IF ${artifact}_plan_candidate = NONE OR ${artifact}_plan_candidate.space_id != $sf_space OR ${artifact}_plan_candidate.deleted_at != NONE OR ${artifact}_plan_candidate.work_run != ${artifact}.work_run OR ${artifact}_plan_candidate.artifact_type != ${artifact}.data.source_artifact_type {{ THROW 'publication:source_candidate_plan_mismatch'; }};");
    append_artifact_tags_sql(
        sql,
        &format!("{artifact}_plan_candidate"),
        &format!("$sf_replanned OR ${artifact}_origin_item.payload._source_plan != NONE"),
    );
    sql.push_str("};\n");
}
