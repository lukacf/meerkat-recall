//! Read-only preparation and one canonical commit for public predicate approval.
//!
//! Transports authorize every observed record before preparing changes, then every
//! proposed record before committing. Pending reprocess jobs are durable queue
//! intents consumed by the all-space reprocess recovery scan.

use chrono::Utc;
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::public_mutation::{inherit_public_envelope, PublicMutation};
use elephant_storage::{
    PredicateAliasRepository, PredicateRegistryRepository, Repository, StorageClient,
    WorkRunRepository,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::{Budget, IngestMode};
use elephant_types::records::predicate::{
    AliasScope, AliasStatus, AliasType, PredicateAlias, PredicateRegistry, SemanticsStatus,
};
use elephant_types::records::reprocess::{
    ReprocessJob, ReprocessProgress, ReprocessStatus, ReprocessTargetStage, SelectionCriteria,
};
use elephant_types::records::work::{ArtifactStatus, WorkArtifact, WorkRun};
use elephant_types::{RecordId, TableName};
use uuid::Uuid;

use crate::error::PipelineError;
use crate::predicate::{PredicateProposalData, PREDICATE_PROPOSAL_ARTIFACT_TYPE};
use crate::scheduler::budget::{BudgetCheckResult, BudgetEnforcer};

/// Retained transport workflow choices, independent of record authorization.
#[derive(Clone, Copy)]
pub struct PublicProposalOptions {
    /// Phase 13 approves a draft after performing the validation checks in memory.
    pub allow_draft: bool,
    /// Phase 13 also registers a missing canonical predicate in the same commit.
    pub ensure_registry: bool,
    /// Preserve the workflow's commit-time check against its original work run.
    pub check_work_run_budget: bool,
    /// MCP permits an explicit confidence override; otherwise retain workflow gating.
    pub confidence: Option<f32>,
}

/// Observed approval inputs. Loading this value performs no writes.
pub struct PublicPredicateProposal {
    client: StorageClient,
    mutation: PublicMutation,
    options: PublicProposalOptions,
    proposal: PredicateProposalData,
    /// Exact original artifact; authorize before disclosing its contents or state.
    pub artifact: WorkArtifact,
    /// Existing canonical predicate, if any; requires read and write authorization.
    pub registry: Option<PredicateRegistry>,
    /// Existing aliases relevant to this proposal, including deterministic identities.
    pub aliases: Vec<PredicateAlias>,
    /// Original producer run used by the retained Phase 13 budget check.
    pub work_run: Option<WorkRun>,
}

impl PublicPredicateProposal {
    /// Load dependencies after the transport authorized the artifact. The mutation
    /// must have begun before the artifact was read.
    ///
    /// # Errors
    /// Returns an error for malformed artifacts or failed dependency reads.
    pub async fn load(
        client: StorageClient,
        mutation: PublicMutation,
        artifact: WorkArtifact,
        options: PublicProposalOptions,
    ) -> Result<Self, PipelineError> {
        if artifact.artifact_type != PREDICATE_PROPOSAL_ARTIFACT_TYPE {
            return Err(invalid("Expected a predicate_proposal artifact"));
        }
        if artifact.status != ArtifactStatus::Validated
            && !(options.allow_draft && artifact.status == ArtifactStatus::Draft)
        {
            return Err(invalid("Expected a validated predicate proposal"));
        }
        let proposal: PredicateProposalData = serde_json::from_value(artifact.data.clone())
            .map_err(|error| invalid(&format!("Invalid proposal data: {error}")))?;
        if proposal.space_id != artifact.base.space_id
            || proposal.predicate_id.trim().is_empty()
            || proposal.proposed_aliases.is_empty()
        {
            return Err(invalid(
                "Proposal requires the selected space, predicate and aliases",
            ));
        }
        if options
            .confidence
            .is_some_and(|confidence| !confidence.is_finite() || !(0.0..=1.0).contains(&confidence))
        {
            return Err(invalid(
                "Alias confidence must be finite and between zero and one",
            ));
        }
        let space = &artifact.base.space_id;
        let registry = PredicateRegistryRepository::new(client.clone())
            .find_by_predicate_id(space, &proposal.predicate_id)
            .await?;
        let alias_repo = PredicateAliasRepository::new(client.clone());
        let mut aliases = Vec::new();
        let mut names = std::collections::BTreeSet::new();
        for alias in &proposal.proposed_aliases {
            if alias.alias.trim().is_empty()
                || !alias.confidence.is_finite()
                || !names.insert(alias.alias.to_ascii_lowercase())
            {
                return Err(invalid(
                    "Proposal contains an empty, duplicate or invalid alias",
                ));
            }
            for existing in [
                alias_repo.find_active_by_raw(space, &alias.alias).await?,
                alias_repo
                    .get(
                        space,
                        &alias_id(space, &proposal.predicate_id, &alias.alias, &alias.source),
                    )
                    .await?,
            ]
            .into_iter()
            .flatten()
            {
                if !aliases
                    .iter()
                    .any(|row: &PredicateAlias| row.base.id == existing.base.id)
                {
                    aliases.push(existing);
                }
            }
        }
        let work_run = if options.check_work_run_budget {
            Some(
                WorkRunRepository::new(client.clone())
                    .get(space, artifact.work_run.id())
                    .await?
                    .ok_or_else(|| invalid("Predicate producer work run is absent"))?,
            )
        } else {
            None
        };
        Ok(Self {
            client,
            mutation,
            options,
            proposal,
            artifact,
            registry,
            aliases,
            work_run,
        })
    }

    /// Prepare changes only after the caller has authorized all observed inputs.
    /// Authorize the returned complete after-images before calling `commit`.
    ///
    /// # Errors
    /// Returns an error for conflicting aliases, incompatible envelopes or a
    /// missing/deprecated canonical predicate.
    pub fn prepare(
        mut self,
        actor: &str,
    ) -> Result<PreparedPublicPredicateProposal, PipelineError> {
        if let Some(run) = &self.work_run {
            self.mutation
                .depend_on(CanonicalRecord::WorkRun(Box::new(run.clone())));
        }
        let mut registry_create = None;
        let registry = if let Some(registry) = &self.registry {
            if registry.status != "active" {
                return Err(invalid("Canonical predicate is not active"));
            }
            self.mutation
                .depend_on(CanonicalRecord::PredicateRegistry(Box::new(
                    registry.clone(),
                )));
            registry.clone()
        } else {
            if !self.options.ensure_registry {
                return Err(invalid("Canonical predicate does not exist in registry"));
            }
            let record = PredicateRegistry {
                base: derived_base(
                    &self.artifact.base,
                    stable_id(
                        TableName::PredicateRegistry,
                        &self.artifact.base.space_id,
                        &self.proposal.predicate_id,
                    ),
                    actor,
                ),
                predicate_id: self.proposal.predicate_id.clone(),
                description: None,
                status: "active".into(),
                semantics: None,
                semantics_status: SemanticsStatus::Default,
                first_seen_at: None,
                usage_count: 0,
                last_used_at: None,
            };
            self.mutation
                .stage_create(CanonicalRecord::PredicateRegistry(Box::new(record.clone())))?;
            registry_create = Some(record.clone());
            record
        };
        let mut aliases = Vec::new();
        for proposal in &self.proposal.proposed_aliases {
            let id = alias_id(
                &self.artifact.base.space_id,
                &self.proposal.predicate_id,
                &proposal.alias,
                &proposal.source,
            );
            let mut base = derived_base(&self.artifact.base, id.clone(), actor);
            inherit_public_envelope(&mut base, &registry.base)?;
            let confidence = self
                .options
                .confidence
                .unwrap_or_else(|| proposal.confidence.clamp(0.0, 1.0).max(0.95));
            let mut proposed = PredicateAlias {
                base,
                alias: proposal.alias.clone(),
                alias_type: AliasType::RawString,
                source_plugin: Some(proposal.source.clone()),
                confidence,
                status: AliasStatus::Active,
                scope: AliasScope::Global,
                predicate_id: self.proposal.predicate_id.clone(),
                confidence_version: 1,
                usage_count: 0,
            };
            let relevant: Vec<_> = self
                .aliases
                .iter()
                .filter(|existing| {
                    existing.base.id == id || existing.alias.eq_ignore_ascii_case(&proposal.alias)
                })
                .collect();
            if self.artifact.status == ArtifactStatus::Draft && !relevant.is_empty() {
                return Err(invalid("Active alias already exists for proposal"));
            }
            if let Some(existing) = relevant.first() {
                if relevant.len() != 1
                    || existing.base.id != id
                    || existing.predicate_id != self.proposal.predicate_id
                    || existing.alias != proposal.alias
                    || existing.status != AliasStatus::Active
                    || existing.source_plugin.as_deref() != Some(proposal.source.as_str())
                {
                    return Err(invalid(
                        "Predicate alias conflicts with an existing mapping",
                    ));
                }
                let mut inherited = existing.base.clone();
                inherit_public_envelope(&mut inherited, &proposed.base)?;
                if inherited.security != existing.base.security
                    || inherited.subjects != existing.base.subjects
                    || inherited.subjects_state != existing.base.subjects_state
                {
                    return Err(invalid(
                        "Existing alias cannot inherit the proposal's restrictions",
                    ));
                }
                proposed.base = inherited;
                proposed.usage_count = existing.usage_count;
                proposed.confidence_version = existing.confidence_version;
                self.mutation.stage_replace(*existing, proposed.clone())?;
            } else {
                self.mutation
                    .stage_create(CanonicalRecord::PredicateAlias(Box::new(proposed.clone())))?;
            }
            aliases.push(proposed);
        }
        let mut committed = self.artifact.clone();
        committed.status = ArtifactStatus::Committed;
        committed.validated_at = committed.validated_at.or(Some(Utc::now()));
        committed.validation_errors = None;
        committed.committed_at = Some(Utc::now());
        let mut job_base = derived_base(
            &self.artifact.base,
            stable_id(
                TableName::ReprocessJob,
                &self.artifact.base.space_id,
                &format!("predicate-approval:{}", self.artifact.base.id),
            ),
            actor,
        );
        for alias in &aliases {
            inherit_public_envelope(&mut job_base, &alias.base)?;
        }
        let job = ReprocessJob {
            base: job_base,
            name: format!("predicate-reprocess-{}", self.proposal.predicate_id),
            selection_criteria: SelectionCriteria {
                predicate_ids: std::iter::once(self.proposal.predicate_id.clone())
                    .chain(aliases.iter().map(|alias| alias.alias.clone()))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect(),
                ..SelectionCriteria::default()
            },
            target_stages: vec![
                ReprocessTargetStage::ReResolve,
                ReprocessTargetStage::ReMaintain,
            ],
            status: ReprocessStatus::Pending,
            progress: ReprocessProgress::default(),
            priority: 50,
            budget_limit_euros: None,
            max_sources_per_day: None,
            started_at: None,
            completed_at: None,
            error: None,
            reason: Some(format!(
                "Predicate alias approved; refresh claims for predicate {}",
                self.proposal.predicate_id
            )),
        };
        self.mutation
            .stage_create(CanonicalRecord::ReprocessJob(Box::new(job.clone())))?;
        Ok(PreparedPublicPredicateProposal {
            input: self,
            artifact: committed,
            registry_create,
            aliases,
            reprocess_job: job,
        })
    }
}

/// Authorized before-images and complete proposed changes awaiting one commit.
pub struct PreparedPublicPredicateProposal {
    input: PublicPredicateProposal,
    /// Artifact's complete committed after-image.
    pub artifact: WorkArtifact,
    /// New registry entry, when the Phase 13 workflow needs one.
    pub registry_create: Option<PredicateRegistry>,
    /// Complete alias after-images; inherited restrictions must be authorized.
    pub aliases: Vec<PredicateAlias>,
    /// Durable reprocessing intent committed alongside the aliases and artifact.
    pub reprocess_job: ReprocessJob,
}

impl PreparedPublicPredicateProposal {
    /// Apply the retained budget gate, then commit all changes, audit and outbox.
    /// The caller must have authorized every observed and proposed record.
    ///
    /// # Errors
    /// Returns an error for exhausted budget, stale observations or a rejected
    /// canonical transaction. No alias/artifact/job subset is committed.
    pub async fn commit(self, actor: &str) -> Result<PredicateAlias, PipelineError> {
        if let Some(run) = &self.input.work_run {
            let budget = BudgetEnforcer::new(
                self.input.client.clone(),
                Budget::default(),
                self.artifact.base.space_id.clone(),
            );
            match budget
                .check_budget_at_commit(IngestMode::Graph, run.metrics.estimated_cost_euros)
                .await?
            {
                BudgetCheckResult::Allowed => {}
                BudgetCheckResult::Rejected { reason }
                | BudgetCheckResult::Downgrade { reason, .. } => {
                    return Err(PipelineError::BudgetExceeded(reason))
                }
                BudgetCheckResult::Queued { worker_kind } => {
                    return Err(PipelineError::BudgetExceeded(format!(
                        "Commit blocked due to concurrency limit: {worker_kind}"
                    )))
                }
            }
        }
        self.input
            .mutation
            .replace(&self.input.artifact, self.artifact, false, actor)
            .await?;
        self.aliases
            .into_iter()
            .next()
            .ok_or_else(|| invalid("Proposal has no aliases"))
    }
}

fn derived_base(input: &BaseFields, id: RecordId, actor: &str) -> BaseFields {
    let mut base = input.clone();
    base.id = id;
    base.created_at = Utc::now();
    base.updated_at = base.created_at;
    base.created_by = actor.to_owned();
    base.deleted_at = None;
    base
}

fn stable_id(table: TableName, space: &str, purpose: &str) -> RecordId {
    let key = serde_json::json!(["public-predicate", space, table.as_str(), purpose]).to_string();
    RecordId::new(table, Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes()))
}

fn alias_id(space: &str, predicate: &str, alias: &str, source: &str) -> RecordId {
    // Preserve identities used by the retained predicate workflow.
    RecordId::new(
        TableName::PredicateAlias,
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("{space}|{predicate}|{alias}|{source}").as_bytes(),
        ),
    )
}

fn invalid(message: &str) -> PipelineError {
    PipelineError::InvalidInput(message.to_owned())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]
    use super::*;
    use elephant_storage::{
        AuditLogRepository, ReprocessJobRepository, SystemEventRepository, WorkArtifactRepository,
    };
    use elephant_types::Link;

    async fn storage(embedded: bool) -> StorageClient {
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .expect("storage");
        if !embedded {
            elephant_migrations::apply_schema(&storage)
                .await
                .expect("complete original native schema");
        }
        let config: elephant_types::records::SpaceConfig =
            serde_json::from_value(serde_json::json!({
                "id":RecordId::new_v4(TableName::SpaceConfig),"space_id":"proposal-test",
                "created_at":Utc::now(),"updated_at":Utc::now(),"created_by":"test",
                "security":{"policy_id":"default","level":0,"labels":[],"handling":[]},
                "provenance":{},"subjects":[],"subjects_state":"complete",
                "embedding_provider":"fake","embedding_model":"test","embedding_dims":8
            }))
            .expect("space config");
        elephant_storage::SpaceConfigRepository::new(storage.clone())
            .create(config)
            .await
            .expect("space");
        storage
    }

    fn artifact() -> WorkArtifact {
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::WorkArtifact),
            "proposal-test".into(),
            "producer".into(),
        );
        base.security.level = 4;
        base.security.labels = vec!["restricted".into()];
        WorkArtifact {
            base,
            work_run: Link::new(RecordId::new_v4(TableName::WorkRun)),
            artifact_type: PREDICATE_PROPOSAL_ARTIFACT_TYPE.into(),
            data: serde_json::json!({"space_id":"proposal-test","predicate_id":"works_at","confidence":0.8,
                "proposed_aliases":[{"alias":"employed by","source":"test","confidence":0.8},{"alias":"works for","source":"test","confidence":0.9}]}),
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        }
    }

    async fn prepare(
        storage: &StorageClient,
        artifact: WorkArtifact,
    ) -> Result<PreparedPublicPredicateProposal, PipelineError> {
        let mutation = PublicMutation::begin(storage.clone(), "proposal-test").await?;
        PublicPredicateProposal::load(
            storage.clone(),
            mutation,
            artifact,
            PublicProposalOptions {
                allow_draft: true,
                ensure_registry: true,
                check_work_run_budget: false,
                confidence: None,
            },
        )
        .await?
        .prepare("approver")
    }

    #[tokio::test]
    async fn public_approval_commits_inherited_aliases_registry_job_and_artifact_together() {
        for embedded in [true, false] {
            let storage = storage(embedded).await;
            let observed = artifact();
            let artifacts = WorkArtifactRepository::new(storage.clone());
            artifacts.create(observed.clone()).await.expect("artifact");
            let prepared = prepare(&storage, observed.clone()).await.expect("prepare");
            let registry_id = prepared
                .registry_create
                .as_ref()
                .expect("new registry")
                .base
                .id
                .clone();
            let job_id = prepared.reprocess_job.base.id.clone();
            let aliases = prepared.aliases.clone();
            prepared.commit("approver").await.expect("commit");
            assert_eq!(
                artifacts
                    .get("proposal-test", &observed.base.id)
                    .await
                    .expect("read")
                    .expect("artifact")
                    .status,
                ArtifactStatus::Committed
            );
            let registry = PredicateRegistryRepository::new(storage.clone())
                .get("proposal-test", &registry_id)
                .await
                .expect("read")
                .expect("registry");
            assert_eq!(registry.base.security, observed.base.security);
            for alias in aliases {
                let stored = PredicateAliasRepository::new(storage.clone())
                    .get("proposal-test", &alias.base.id)
                    .await
                    .expect("read")
                    .expect("alias");
                assert_eq!(stored.base.security, observed.base.security);
                assert_eq!(stored.status, AliasStatus::Active);
            }
            let job = ReprocessJobRepository::new(storage.clone())
                .get("proposal-test", &job_id)
                .await
                .expect("read")
                .expect("durable job");
            assert_eq!(job.status, ReprocessStatus::Pending);
            assert_eq!(job.base.security, observed.base.security);
            assert_eq!(
                job.selection_criteria.predicate_ids,
                vec![
                    "employed by".to_owned(),
                    "works for".to_owned(),
                    "works_at".to_owned()
                ]
            );
            assert_eq!(
                job.target_stages,
                vec![
                    ReprocessTargetStage::ReResolve,
                    ReprocessTargetStage::ReMaintain
                ]
            );
            assert_eq!(
                AuditLogRepository::new(storage.clone())
                    .list("proposal-test", 100, None)
                    .await
                    .expect("audits")
                    .len(),
                5
            );
            assert_eq!(
                SystemEventRepository::new(storage)
                    .get_after_seq("proposal-test", 0, 100)
                    .await
                    .expect("outbox")
                    .len(),
                1
            );
        }
    }

    #[tokio::test]
    async fn public_approval_refuses_changed_artifact_without_partial_aliases_or_job() {
        for embedded in [true, false] {
            let storage = storage(embedded).await;
            let observed = artifact();
            let artifacts = WorkArtifactRepository::new(storage.clone());
            artifacts.create(observed.clone()).await.expect("artifact");
            let prepared = prepare(&storage, observed.clone()).await.expect("prepare");
            let mut newer = observed.clone();
            newer.data["proposed_aliases"][0]["alias"] = "changed after authorization".into();
            artifacts
                .update("proposal-test", &observed.base.id, newer.clone())
                .await
                .expect("concurrent update");
            assert!(prepared.commit("approver").await.is_err());
            assert_eq!(
                artifacts
                    .get("proposal-test", &observed.base.id)
                    .await
                    .expect("read")
                    .expect("artifact")
                    .data,
                newer.data
            );
            assert!(PredicateRegistryRepository::new(storage.clone())
                .list("proposal-test", 100, None)
                .await
                .expect("registry")
                .is_empty());
            assert!(PredicateAliasRepository::new(storage.clone())
                .list("proposal-test", 100, None)
                .await
                .expect("aliases")
                .is_empty());
            assert!(ReprocessJobRepository::new(storage.clone())
                .list("proposal-test", 100, None)
                .await
                .expect("jobs")
                .is_empty());
            assert!(AuditLogRepository::new(storage.clone())
                .list("proposal-test", 100, None)
                .await
                .expect("audits")
                .is_empty());
            assert!(SystemEventRepository::new(storage)
                .get_after_seq("proposal-test", 0, 100)
                .await
                .expect("outbox")
                .is_empty());
        }
    }
    #[tokio::test]
    async fn public_approval_fences_original_work_run_after_late_cost_change() {
        for embedded in [true, false] {
            let storage = storage(embedded).await;
            let observed = artifact();
            WorkArtifactRepository::new(storage.clone())
                .create(observed.clone())
                .await
                .expect("artifact");
            let run = WorkRun {
                base: BaseFields::new(
                    observed.work_run.id().clone(),
                    "proposal-test".into(),
                    "producer".into(),
                ),
                work_item: Link::new(RecordId::new_v4(TableName::WorkItem)),
                attempt: 1,
                started_at: Utc::now(),
                completed_at: Some(Utc::now()),
                success: true,
                error: None,
                metrics: Default::default(),
                worker_id: "test".into(),
                worker_version: "test".into(),
                model_version: None,
                prompt_id: None,
                config_hash: None,
            };
            let runs = elephant_storage::WorkRunRepository::new(storage.clone());
            runs.create(run.clone()).await.expect("run");
            let mutation = PublicMutation::begin(storage.clone(), "proposal-test")
                .await
                .expect("begin");
            let prepared = PublicPredicateProposal::load(
                storage.clone(),
                mutation,
                observed,
                PublicProposalOptions {
                    allow_draft: true,
                    ensure_registry: true,
                    check_work_run_budget: true,
                    confidence: None,
                },
            )
            .await
            .expect("load")
            .prepare("approver")
            .expect("prepare");
            let mut changed = run.clone();
            changed.metrics.estimated_cost_euros = 12.0;
            runs.update("proposal-test", &run.base.id, changed)
                .await
                .expect("late cost receipt");
            assert!(prepared.commit("approver").await.is_err());
            assert!(PredicateAliasRepository::new(storage.clone())
                .list("proposal-test", 100, None)
                .await
                .expect("aliases")
                .is_empty());
            assert!(ReprocessJobRepository::new(storage)
                .list("proposal-test", 100, None)
                .await
                .expect("jobs")
                .is_empty());
        }
    }
}
