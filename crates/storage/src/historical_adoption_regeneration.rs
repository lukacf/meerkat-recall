//! S1 publication under the sealed all-batch maintenance capability.
use super::*;
use crate::historical_adoption::canonical_sha256;
use crate::historical_adoption::catchup::{full_control, HistoricalControlImage};
use chrono::DateTime;

async fn verify_resumed_source_receipt(
    client: &StorageClient,
    input: &HistoricalAdoptionInput,
    plan: &HistoricalRegenerationPlan,
    step: &HistoricalRegenerationStep,
    canonical: &CanonicalCommitReceipt,
) -> StorageResult<()> {
    if let Some(external) = &step.external_source_id {
        let imported: HistoricalAdoptionReceipt = serde_json::from_value(
            canonical
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical S1 resumed adoption absent"))?,
        )
        .map_err(|_| integrity("historical S1 resumed adoption invalid"))?;
        if imported.external_source_id != *external
            || imported.original_snapshot_sha256 != input.snapshot_sha256
            || imported.commit_id != canonical.commit_id
        {
            return Err(integrity("historical S1 resumed adoption binding differs"));
        }
        let publication = canonical
            .publication_authority
            .as_ref()
            .ok_or_else(|| integrity("historical S1 resumed publication absent"))?;
        let manifest =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                client.clone(),
            )
            .get(&input.space_id, publication)
            .await?
            .ok_or_else(|| integrity("historical S1 resumed manifest absent"))?;
        manifest.validate()?;
        if canonical.historical_import.as_ref()
            != Some(&HistoricalCommitProvenance::for_manifest(&manifest)?)
            || manifest
                .historical_adoption
                .as_ref()
                .is_none_or(|proof| proof.plan_id != input.plan_id)
        {
            return Err(integrity("historical S1 resumed manifest differs"));
        }
    } else {
        let retired: HistoricalRetirementReceipt = serde_json::from_value(
            canonical
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical S1 retirement absent"))?,
        )
        .map_err(|_| integrity("historical S1 retirement invalid"))?;
        let retirement_snapshot_matches = retired.original_snapshot_sha256 == input.snapshot_sha256;
        if retired.source_id != step.source_id
            || retired.plan_sha256 != plan.plan_sha256
            || !retirement_snapshot_matches
        {
            return Err(integrity("historical S1 retirement binding differs"));
        }
    }
    Ok(())
}

async fn verify_regeneration_prefix(
    client: &StorageClient,
    input: &HistoricalAdoptionInput,
    sealed: &crate::client::SealedHistoricalCatchupAuthority,
    plan: HistoricalRegenerationPlan,
    initial_epoch: String,
) -> StorageResult<RegenerationPosition> {
    let mut previous_epoch = initial_epoch.clone();
    let mut previous = None;
    for (ordinal, receipt) in sealed.recovered_regeneration().iter().enumerate() {
        let state: HistoricalRegenerationProgress = serde_json::from_value(
            receipt
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical S1 resumed state absent"))?,
        )
        .map_err(|_| integrity("historical S1 resumed state invalid"))?;
        let exact_progress =
            read_receipt_exact(client, &input.space_id, &receipt.commit_id).await?;
        if serde_json::to_vec(&exact_progress)
            .map_err(|_| integrity("historical receipt encode"))?
            != serde_json::to_vec(receipt).map_err(|_| integrity("historical receipt encode"))?
        {
            return Err(integrity("historical resumed full receipt differs"));
        }
        let step = plan
            .steps
            .get(ordinal)
            .ok_or_else(|| integrity("historical S1 excess prefix"))?;
        if state.s1_plan_sha256 != plan.plan_sha256
            || state.group_id != step.source_id
            || state.ordinal != ordinal as u64
            || state.previous_regeneration_receipt_sha256 != previous
            || state.before_epoch != previous_epoch
        {
            return Err(integrity("historical S1 resumed group differs"));
        }
        let canonical =
            read_receipt_exact(client, &input.space_id, &state.adoption_commit_id).await?;
        if canonical.source_id.as_ref() != Some(&step.source_id)
            || canonical.space_id != input.space_id
            || canonical.commit_id != state.adoption_commit_id
        {
            return Err(integrity("historical S1 resumed commit identity differs"));
        }
        verify_resumed_source_receipt(client, input, &plan, step, &canonical).await?;
        previous_epoch = state.after_epoch;
        previous = Some(sha256(
            &serde_json::to_vec(receipt).map_err(|_| integrity("historical S1 prefix encode"))?,
        ));
    }
    if previous_epoch != sealed.regeneration_epoch() {
        return Err(integrity("historical S1 resumed epoch differs"));
    }
    let position = RegenerationPosition {
        initial_epoch,
        plan,
        next: sealed.recovered_regeneration().len(),
        previous,
        epoch: sealed.regeneration_epoch().into(),
    };
    Ok(position)
}

type OriginalRegenerationPopulation = (
    BTreeMap<RecordId, RecordId>,
    BTreeMap<RecordId, SourceDocumentMapping>,
    BTreeMap<RecordId, SourcePublicationCompletion>,
);

async fn original_regeneration_population(
    client: &StorageClient,
    input: &HistoricalAdoptionInput,
    sealed: &crate::client::SealedHistoricalCatchupAuthority,
) -> StorageResult<OriginalRegenerationPopulation> {
    let mut documents = BTreeMap::new();
    let mut original_mappings = BTreeMap::new();
    let mut original_completions = BTreeMap::new();
    for binding in sealed.original_sources() {
        let publication = PublicationFence {
            source_id: binding.source_id.clone(),
            generation: binding.head.generation.clone(),
            revision_id: binding
                .head
                .published_revision
                .clone()
                .ok_or_else(|| integrity("historical original publication missing"))?,
            operation_id: binding.head.operation_id.clone(),
        };
        let manifest =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                client.clone(),
            )
            .get(&input.space_id, &publication)
            .await?
            .ok_or_else(|| integrity("historical original manifest missing"))?;
        manifest.validate()?;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical original proof missing"))?;
        if proof.commit_id(&manifest) != binding.commit_id {
            return Err(integrity("historical original commit differs"));
        }
        for mapping in &manifest.documents {
            for id in [&mapping.doc_id, &mapping.doc_revision_id] {
                if original_mappings
                    .insert(id.clone(), mapping.clone())
                    .is_some()
                {
                    return Err(integrity("historical original mapping ambiguous"));
                }
            }
        }
        let completion: SourcePublicationCompletion =
            SourcePublicationRepository::new(client.clone())
                .read_control(
                    COMPLETION_TABLE,
                    &manifest_key(&input.space_id, &publication),
                    &input.space_id,
                )
                .await?
                .ok_or_else(|| integrity("historical original completion absent"))?;
        if completion.publication != publication
            || completion.manifest_digest != manifest.digest()?
            || completion.commit_id != binding.commit_id
        {
            return Err(integrity("historical original completion differs"));
        }
        original_completions.insert(binding.source_id.clone(), completion);
        for id in proof.documents.keys() {
            if documents
                .insert(id.clone(), binding.source_id.clone())
                .is_some()
            {
                return Err(integrity("historical original document group ambiguous"));
            }
        }
    }
    Ok((documents, original_mappings, original_completions))
}

impl HistoricalAdoptionRepository {
    /// Accept only the exact S1 namespace/snapshot after all declared row batches.
    /// The returned owner retains the original nonempty-target maintenance lease.
    pub async fn prepare_catchup_authority(
        &self,
        input: HistoricalAdoptionInput,
        sealed: crate::client::SealedHistoricalCatchupAuthority,
    ) -> StorageResult<PreparedHistoricalAuthority> {
        input.validate_population(true)?;
        sealed.check_custody()?;
        if input.source_space_id != sealed.source_space_id()
            || input.space_id != sealed.space_id()
            || input.snapshot_sha256 != sealed.after_snapshot_sha256()
        {
            return Err(integrity("historical S1 sealed input differs"));
        }
        let client = sealed.client()?.clone();
        if client.embedded_store().is_some() {
            return Err(integrity(
                "historical S1 durable inventory recovery currently requires the native backend",
            ));
        }
        let plan = HistoricalRegenerationPlan::derive(&input, sealed.original_sources())?;
        if sealed.regeneration_plan_binding()
            != (plan.plan_sha256.as_str(), plan.steps.len() as u64)
        {
            return Err(integrity("historical S1 full plan differs from custody"));
        }
        let initial_epoch = sealed.regeneration_start_epoch().to_owned();
        let position =
            verify_regeneration_prefix(&client, &input, &sealed, plan, initial_epoch).await?;
        let (documents, original_mappings, original_completions) =
            original_regeneration_population(&client, &input, &sealed).await?;
        sealed.check_custody()?;
        let authority = PreparedHistoricalAuthority {
            journal_identity: client.erasure_journal()?.identity().to_owned(),
            client,
            snapshot_sha256: input.snapshot_sha256.clone(),
            group_index: input
                .groups
                .iter()
                .enumerate()
                .map(|(index, group)| (group.external_source_id.clone(), index))
                .collect(),
            snapshot_index: input
                .snapshot_groups
                .iter()
                .enumerate()
                .map(|(index, group)| (group.external_source_id.clone(), index))
                .collect(),
            input,
            population: tokio::sync::OnceCell::new(),
            catchup: Some(sealed),
            original_document_groups: documents,
            original_mappings,
            original_completions,
            regeneration: Some(std::sync::Mutex::new(position)),
        };
        let recovered_count = authority
            .catchup
            .as_ref()
            .ok_or_else(|| integrity("historical catchup absent"))?
            .recovered_regeneration()
            .len();
        for ordinal in 0..recovered_count {
            authority
                .recovered_regeneration_effects(ordinal as u64)
                .await?;
        }
        Ok(authority)
    }
}

impl PreparedHistoricalAuthority {
    pub(super) async fn catchup_source_before(
        &self,
        id: &RecordId,
    ) -> StorageResult<Option<Source>> {
        let original_sources = self
            .catchup
            .as_ref()
            .ok_or_else(|| integrity("historical S1 custody absent"))?
            .original_sources();
        let current = crate::SourceRepository::new(self.client.clone())
            .get(&self.input.space_id, id)
            .await?;
        let binding = original_sources
            .iter()
            .find(|binding| binding.source_id == *id);
        match (binding, current) {
            (None, None) => Ok(None),
            (Some(binding), Some(source)) => {
                let repository = SourcePublicationRepository::new(self.client.clone());
                let head = repository
                    .head(&self.input.space_id, id)
                    .await?
                    .ok_or_else(|| integrity("historical S1 original head missing"))?;
                if head.generation != binding.head.generation
                    || head.operation_id != binding.head.operation_id
                    || head.desired_revision != binding.head.desired_revision
                    || head.lifecycle != SourceVisibility::Active
                    || (head.published_revision.is_some() && head != binding.head)
                    || source.base.deleted_at.is_some()
                    || source.type_ != SourceType::Manual
                {
                    return Err(integrity("historical S1 original authority changed"));
                }
                let revision = binding
                    .head
                    .desired_revision
                    .as_deref()
                    .ok_or_else(|| integrity("historical S1 accepted revision missing"))?;
                let accepted: StoredRevision = repository
                    .read_control(
                        "source_revision",
                        &revision_key(&self.input.space_id, id, revision),
                        &self.input.space_id,
                    )
                    .await?
                    .ok_or_else(|| integrity("historical S1 accepted source missing"))?;
                drop(repository);
                if accepted.revision.source != source {
                    return Err(integrity(
                        "historical S1 grouped source changed independently",
                    ));
                }
                Ok(Some(source))
            }
            _ => Err(integrity("historical S1 source identity collision")),
        }
    }

    pub(super) async fn catchup_control_before(
        &self,
        controls: &[Control],
    ) -> StorageResult<BTreeMap<(String, String), HistoricalControlImage>> {
        let mut result = BTreeMap::new();
        if self.catchup.is_none() {
            return Ok(result);
        }
        for control in controls {
            if !crate::private_control_read::global_exists(
                &self.client,
                control.table,
                &control.key,
            )
            .await?
            {
                continue;
            }
            // Immutable publications, revisions, supports, operations and receipts
            // are append-only. Only current pointers can replace exact old images.
            if !matches!(
                control.table,
                "source_head"
                    | "source_identity"
                    | "source_authorization"
                    | "source_document_mapping"
                    | "source_revision_mapping"
                    | "source_publication_completion"
            ) {
                return Err(integrity("historical S1 immutable identity collision"));
            }
            if control.table == COMPLETION_TABLE {
                let payload: SourcePublicationCompletion =
                    serde_json::from_str(&control.payload)
                        .map_err(|_| integrity("historical S1 completion invalid"))?;
                if control.key
                    != stable_key(&[
                        "published-source-completion/v1",
                        &self.input.space_id,
                        &payload.publication.source_id.to_string(),
                    ])
                {
                    return Err(integrity("historical S1 immutable completion collision"));
                }
            }
            let image = full_control(
                &self.client,
                control.table,
                &control.key,
                &self.input.space_id,
            )
            .await?;
            self.validate_original_control(control, &image).await?;
            result.insert((control.table.into(), control.key.clone()), image);
        }
        self.check_custody()?;
        Ok(result)
    }
}

/// Closed publication work derived from all validated S1 groups and all S0 heads.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalRegenerationStep {
    /// Canonical source identity for the historical publication.
    pub source_id: RecordId,
    /// Stable successor group identity, or absent when retiring an original group.
    pub external_source_id: Option<String>,
}
/// Complete ordered successor publication plan, including retirement of absent groups.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalRegenerationPlan {
    /// Version of the serialized historical proof.
    pub version: u32,
    /// Digest of the complete successor adoption input.
    pub source_plan_sha256: String,
    /// Complete ordered regeneration and retirement sequence.
    pub steps: Vec<HistoricalRegenerationStep>,
    /// Digest binding the complete ordered regeneration plan.
    pub plan_sha256: String,
}
/// Written with publication, never a separate success marker.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalRegenerationProgress {
    /// Version of the serialized historical proof.
    pub version: u32,
    /// Digest of the exclusive catch-up input authority.
    pub authority_binding_sha256: String,
    /// Digest of the final original row-batch receipt, or the domain-separated
    /// zero-batch anchor binding the original authority, journal, snapshots,
    /// regeneration plan and epoch when no row batches exist.
    pub original_last_batch_receipt_sha256: String,
    /// Digest of the complete ordered successor publication plan.
    pub s1_plan_sha256: String,
    /// Total steps declared by the complete regeneration plan.
    pub groups: u64,
    /// Zero-based position in the complete regeneration sequence.
    pub ordinal: u64,
    /// Preceding regeneration receipt digest, absent for the first step.
    pub previous_regeneration_receipt_sha256: Option<String>,
    /// Canonical source identity for this regeneration or retirement step.
    pub group_id: RecordId,
    /// Exact adoption or retirement commit ID produced by this step.
    pub adoption_commit_id: String,
    /// Destination epoch required before the atomic step.
    pub before_epoch: String,
    /// Destination epoch durably produced by the atomic step.
    pub after_epoch: String,
}

#[derive(Debug)]
pub(super) struct RegenerationPosition {
    initial_epoch: String,
    plan: HistoricalRegenerationPlan,
    next: usize,
    previous: Option<String>,
    epoch: String,
}

pub fn regeneration_commit_id(binding: &str, ordinal: u64) -> String {
    stable_key(&[
        "historical-catchup-regeneration-v1",
        binding,
        &ordinal.to_string(),
    ])
}

pub(super) fn group_source_id(space: &str, external: &str) -> RecordId {
    RecordId::new(
        TableName::Source,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("historical-source:{space}:{external}").as_bytes(),
        ),
    )
}

impl HistoricalRegenerationPlan {
    /// Derive the complete ordered S1 regeneration and S0 retirement plan from bound populations.
    pub fn derive(
        input: &HistoricalAdoptionInput,
        original: &[crate::client::HistoricalCatchupSource],
    ) -> StorageResult<Self> {
        input.validate_population(true)?;
        let mut original_ids = BTreeSet::new();
        if original.iter().any(|old| {
            old.source_id.table() != TableName::Source
                || old.head.space_id != input.space_id
                || old.head.source_id != old.source_id
                || !original_ids.insert(old.source_id.clone())
        }) {
            return Err(integrity("historical original group population invalid"));
        }
        let mut steps: Vec<_> = input
            .groups
            .iter()
            .map(|group| HistoricalRegenerationStep {
                source_id: group_source_id(&input.space_id, &group.external_source_id),
                external_source_id: Some(group.external_source_id.clone()),
            })
            .collect();
        steps.extend(
            input
                .snapshot_groups
                .iter()
                .map(|group| HistoricalRegenerationStep {
                    source_id: group_source_id(&input.space_id, &group.external_source_id),
                    external_source_id: Some(group.external_source_id.clone()),
                }),
        );
        steps.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        let live: BTreeSet<_> = steps.iter().map(|step| step.source_id.clone()).collect();
        if live.len() != steps.len() {
            return Err(integrity("historical S1 source collision"));
        }
        let mut retired: Vec<_> = original
            .iter()
            .filter(|old| !live.contains(&old.source_id))
            .map(|old| HistoricalRegenerationStep {
                source_id: old.source_id.clone(),
                external_source_id: None,
            })
            .collect();
        retired.sort_by(|a, b| a.source_id.cmp(&b.source_id));
        steps.extend(retired);
        let source_plan_sha256 = canonical_sha256(input)?;
        let plan_sha256 = canonical_sha256(&(1u32, &source_plan_sha256, &steps))?;
        Ok(Self {
            version: 1,
            source_plan_sha256,
            steps,
            plan_sha256,
        })
    }
}

impl PreparedHistoricalAuthority {
    /// Exact complete order; callers cannot omit unchanged or retired groups.
    pub fn regeneration_plan(&self) -> StorageResult<HistoricalRegenerationPlan> {
        self.check_custody()?;
        let position = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 authority absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        Ok(position.plan.clone())
    }

    pub(super) fn prepare_regeneration_control(
        &self,
        source: &RecordId,
        external: Option<&str>,
        commit_id: &str,
        before_epoch: Option<&str>,
    ) -> StorageResult<Option<(Control, CanonicalCommitReceipt)>> {
        let Some(sealed) = &self.catchup else {
            return Ok(None);
        };
        let position = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 position absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        let step = position
            .plan
            .steps
            .get(position.next)
            .ok_or_else(|| integrity("historical S1 order exhausted"))?;
        if &step.source_id != source
            || step.external_source_id.as_deref() != external
            || before_epoch != Some(position.epoch.as_str())
        {
            return Err(integrity("historical S1 group/order/epoch changed"));
        }
        let (binding, last_batch) = sealed.regeneration_binding();
        let ordinal =
            u64::try_from(position.next).map_err(|_| integrity("historical S1 ordinal"))?;
        let state = HistoricalRegenerationProgress {
            version: 1,
            authority_binding_sha256: binding.into(),
            original_last_batch_receipt_sha256: last_batch.into(),
            s1_plan_sha256: position.plan.plan_sha256.clone(),
            groups: position.plan.steps.len() as u64,
            ordinal,
            previous_regeneration_receipt_sha256: position.previous.clone(),
            group_id: source.clone(),
            adoption_commit_id: commit_id.into(),
            before_epoch: position.epoch.clone(),
            after_epoch: uuid::Uuid::new_v4().to_string(),
        };
        let receipt = CanonicalCommitReceipt {
            historical_import: None,
            space_id: self.input.space_id.clone(),
            commit_id: regeneration_commit_id(binding, ordinal),
            input_fingerprint: binding.into(),
            plan_fingerprint: position.plan.plan_sha256.clone(),
            replay_output: Some(
                serde_json::to_value(&state)
                    .map_err(|_| integrity("historical S1 progress encode"))?,
            ),
            source_ids: vec![source.clone()],
            publication_authority: None,
            maintenance_authority: vec![],
            source_id: Some(source.clone()),
            source_revision: None,
            effect_ids: vec![],
            extension_ids: vec![],
            outbox_ids: vec![],
            committed_at: Utc::now(),
        };
        drop(position);
        Ok(Some((
            Control::new(
                "source_commit_receipt",
                stable_key(&[&self.input.space_id, &receipt.commit_id]),
                &receipt,
            )?,
            receipt,
        )))
    }

    pub(super) fn retain_regeneration_commit(
        &self,
        receipt: &CanonicalCommitReceipt,
    ) -> StorageResult<()> {
        let state: HistoricalRegenerationProgress = serde_json::from_value(
            receipt
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical S1 progress absent"))?,
        )
        .map_err(|_| integrity("historical S1 progress decode"))?;
        let mut position = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 position absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        if state.ordinal != position.next as u64
            || state.before_epoch != position.epoch
            || state.previous_regeneration_receipt_sha256 != position.previous
        {
            return Err(integrity("historical S1 concurrent position changed"));
        }
        position.previous = Some(sha256(
            &serde_json::to_vec(receipt).map_err(|_| integrity("historical S1 receipt encode"))?,
        ));
        position.epoch = state.after_epoch;
        position.next += 1;
        drop(position);
        Ok(())
    }
}

/// Explicit withdrawal of an S0 group absent from the complete S1 document plan.
/// Historical payload/support receipts remain retained and no erasure is invented.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalGraphReplacement {
    /// Canonical source identity for the historical publication.
    pub source_id: RecordId,
    /// Published revision identity providing replacement graph support.
    pub source_revision: String,
    /// Exact canonical adoption or retirement commit identifier, not a digest.
    pub commit_id: String,
}
/// Durable withdrawal of an absent original group with exact successor support links.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalRetirementReceipt {
    /// Canonical source identity for the historical publication.
    pub source_id: RecordId,
    /// Exact space configuration identity bound to the retirement receipt.
    pub configuration_id: RecordId,
    /// Digest identifying the successor adoption declaration.
    pub adoption_plan_id: String,
    /// Exact successor support bindings for retained graph records.
    pub graph_replacements: BTreeMap<RecordId, Vec<HistoricalGraphReplacement>>,
    /// SHA-256 of the original snapshot retained by this receipt.
    pub original_snapshot_sha256: String,
    /// Digest binding the complete ordered regeneration plan.
    pub plan_sha256: String,
    /// Exact original adoption commit identifier retained for validation.
    pub original_commit_id: String,
    /// Exact canonical adoption or retirement commit identifier, not a digest.
    pub commit_id: String,
    /// Operation receipt for the bound historical publication.
    pub operation: OperationReceipt,
}

impl PreparedHistoricalAuthority {
    async fn retirement_graph_replacements(
        &self,
        original: &crate::client::HistoricalCatchupSource,
    ) -> StorageResult<BTreeMap<RecordId, Vec<HistoricalGraphReplacement>>> {
        let old =
            read_receipt_exact(&self.client, &self.input.space_id, &original.commit_id).await?;
        let publication = old
            .publication_authority
            .as_ref()
            .ok_or_else(|| integrity("historical retired original publication absent"))?;
        let manifests =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                self.client.clone(),
            );
        let manifest = manifests
            .get(&self.input.space_id, publication)
            .await?
            .ok_or_else(|| integrity("historical retired original manifest absent"))?;
        manifest.validate()?;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical retired original proof absent"))?;
        let repository = SourcePublicationRepository::new(self.client.clone());
        let mut result = BTreeMap::new();
        for record in proof.graph_fingerprints.keys() {
            let mut replacements = Vec::new();
            for support in repository
                .support_for_record(&self.input.space_id, record)
                .await?
            {
                let Some(history) = &support.historical_unknown else {
                    continue;
                };
                if history.plan_id != self.input.plan_id
                    || history.snapshot_sha256 != self.input.snapshot_sha256
                {
                    continue;
                }
                let Some(head) = repository
                    .head(&self.input.space_id, &support.source_id)
                    .await?
                else {
                    continue;
                };
                if head.lifecycle != SourceVisibility::Active
                    || head.published_revision.as_deref() != Some(support.source_revision.as_str())
                {
                    continue;
                }
                let fence = PublicationFence {
                    source_id: support.source_id.clone(),
                    generation: head.generation,
                    revision_id: support.source_revision.clone(),
                    operation_id: head.operation_id,
                };
                let current = manifests
                    .get(&self.input.space_id, &fence)
                    .await?
                    .ok_or_else(|| integrity("historical replacement manifest absent"))?;
                current.validate()?;
                let current_proof = current
                    .historical_adoption
                    .as_ref()
                    .ok_or_else(|| integrity("historical replacement proof absent"))?;
                let commit =
                    read_receipt_exact(&self.client, &self.input.space_id, &history.commit_id)
                        .await?;
                if current_proof.plan_id != self.input.plan_id
                    || current_proof.snapshot_sha256 != self.input.snapshot_sha256
                    || current_proof.graph_fingerprints.get(record)
                        != Some(&support.record_fingerprint)
                    || current_proof.commit_id(&current) != history.commit_id
                    || commit.publication_authority.as_ref() != Some(&fence)
                    || commit.historical_import.as_ref()
                        != Some(&HistoricalCommitProvenance::for_manifest(&current)?)
                    || support.independent
                    || support.last_commit_id.as_deref() != Some(history.commit_id.as_str())
                {
                    return Err(integrity("historical replacement authority differs"));
                }
                self.publication_group(&current_proof.external_source_id)?;
                replacements.push(HistoricalGraphReplacement {
                    source_id: support.source_id,
                    source_revision: support.source_revision,
                    commit_id: history.commit_id.clone(),
                });
            }
            replacements.sort_by(|a, b| a.source_id.cmp(&b.source_id));
            if !replacements.is_empty() {
                result.insert(record.clone(), replacements);
            }
        }
        drop(repository);
        drop(manifests);
        Ok(result)
    }
    fn next_retirement_source(&self) -> StorageResult<RecordId> {
        let position = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 custody absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        let step = position
            .plan
            .steps
            .get(position.next)
            .ok_or_else(|| integrity("historical S1 retirement order exhausted"))?;
        if step.external_source_id.is_some() {
            return Err(integrity("historical S1 next group must be adopted"));
        }
        let source_id = step.source_id.clone();
        drop(position);
        Ok(source_id)
    }

    fn retirement_receipt_controls(
        &self,
        sealed: &crate::client::SealedHistoricalCatchupAuthority,
        source: &Source,
        result: &HistoricalRetirementReceipt,
        operation: OperationReceipt,
        now: DateTime<Utc>,
    ) -> StorageResult<(CanonicalCommitReceipt, StoredOperation)> {
        let canonical = CanonicalCommitReceipt {
            historical_import: None,
            space_id: self.input.space_id.clone(),
            commit_id: result.commit_id.clone(),
            input_fingerprint: sealed.regeneration_binding().0.into(),
            plan_fingerprint: result.plan_sha256.clone(),
            replay_output: Some(
                serde_json::to_value(result)
                    .map_err(|_| integrity("historical retirement encode"))?,
            ),
            source_ids: vec![result.source_id.clone()],
            publication_authority: None,
            maintenance_authority: vec![],
            source_id: Some(result.source_id.clone()),
            source_revision: None,
            effect_ids: vec![],
            extension_ids: vec![],
            outbox_ids: vec![],
            committed_at: now,
        };
        let stored = StoredOperation {
            receipt: operation,
            request_fingerprint: result.commit_id.clone(),
            input_fingerprint: result.commit_id.clone(),
            authorization: SourceAuthorizationEnvelope::from(source),
            contact_digest: None,
            producer_origin: elephant_types::publication::SourceProducerOrigin::UnknownOriginal,
            predecessor_publication: None,
        };
        Ok((canonical, stored))
    }

    async fn retirement_head(
        &self,
        source_id: &RecordId,
        commit_id: &str,
        now: DateTime<Utc>,
    ) -> StorageResult<(SourceHead, String, OperationReceipt)> {
        let mut head = SourcePublicationRepository::new(self.client.clone())
            .head(&self.input.space_id, source_id)
            .await?
            .ok_or_else(|| integrity("historical S1 retired head absent"))?;
        let operation_id = operation_key(&self.input.space_id, commit_id);
        head.generation = stable_key(&["historical-s1-retirement-generation", commit_id]);
        head.published_revision = None;
        head.lifecycle = SourceVisibility::Retracted;
        head.operation_id.clone_from(&operation_id);
        head.updated_at = now;
        let operation = OperationReceipt {
            space_id: self.input.space_id.clone(),
            operation_id: operation_id.clone(),
            source_id: source_id.clone(),
            source_generation: head.generation.clone(),
            desired_revision: head.desired_revision.clone(),
            published_revision: None,
            state: OperationState::Retracted,
            accepted_at: now,
            completed_at: Some(now),
            error_code: Some("historical_group_absent_from_s1".into()),
        };
        Ok((head, operation_id, operation))
    }

    /// Retire the next absent group atomically and return verified native effects for ledger replay.
    /// Requires current custody and the exact original receipt, plan position and epoch.
    pub async fn retire_next_group_with_effects(
        &self,
    ) -> StorageResult<(HistoricalRetirementReceipt, Vec<HistoricalRowEffect>)> {
        self.check_custody()?;
        let source_id = self.next_retirement_source()?;
        let epoch = CanonicalRepository::new(self.client.clone())
            .observe_epoch(&self.input.space_id)
            .await?;
        let source = self
            .catchup_source_before(&source_id)
            .await?
            .ok_or_else(|| integrity("historical S1 retired source absent"))?;
        self.retire_group_with_custody(
            &source_id,
            epoch.as_deref(),
            source,
            self.catchup
                .as_ref()
                .ok_or_else(|| integrity("historical S1 custody absent"))?,
        )
        .await
    }

    async fn retire_group_with_custody(
        &self,
        source_id: &RecordId,
        epoch: Option<&str>,
        source: Source,
        sealed: &crate::client::SealedHistoricalCatchupAuthority,
    ) -> StorageResult<(HistoricalRetirementReceipt, Vec<HistoricalRowEffect>)> {
        let binding = sealed
            .original_sources()
            .iter()
            .find(|old| &old.source_id == source_id)
            .ok_or_else(|| integrity("historical S1 retirement not original"))?;
        let config = crate::SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(&self.input.space_id)
            .await?
            .ok_or_else(|| integrity("historical S1 configuration missing"))?;
        let plan = self.regeneration_plan()?;
        let now = Utc::now();
        let commit_id = stable_key(&[
            "historical-s1-retirement-v1",
            &plan.plan_sha256,
            &source_id.to_string(),
        ]);
        let (head, operation_id, operation) =
            self.retirement_head(source_id, &commit_id, now).await?;
        let graph_replacements = self.retirement_graph_replacements(binding).await?;
        let result = HistoricalRetirementReceipt {
            adoption_plan_id: self.input.plan_id.clone(),
            graph_replacements,
            source_id: source_id.clone(),
            configuration_id: config.base.id.clone(),
            original_snapshot_sha256: self.input.snapshot_sha256.clone(),
            plan_sha256: plan.plan_sha256,
            original_commit_id: binding.commit_id.clone(),
            commit_id: commit_id.clone(),
            operation: operation.clone(),
        };
        let (canonical, stored) =
            self.retirement_receipt_controls(sealed, &source, &result, operation, now)?;
        let (progress, progress_receipt) = self
            .prepare_regeneration_control(source_id, None, &commit_id, epoch)?
            .ok_or_else(|| integrity("historical retirement progress absent"))?;
        let state: HistoricalRegenerationProgress = serde_json::from_value(
            progress_receipt
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical retirement state absent"))?,
        )
        .map_err(|_| integrity("historical retirement state invalid"))?;
        let controls = vec![
            Control::new(
                "source_head",
                head_key(&self.input.space_id, source_id),
                &head,
            )?,
            Control::new("source_operation", operation_id, &stored)?,
            Control::new(
                "source_commit_receipt",
                stable_key(&[&self.input.space_id, &commit_id]),
                &canonical,
            )?,
            progress,
        ];
        let before = self.catchup_control_before(&controls).await?;
        let effects = self
            .commit_group(HistoricalGroupCommit {
                config: &config,
                source: source_id,
                dependencies: &[CanonicalRecord::Source(Box::new(source))],
                effects: &[],
                controls: &controls,
                control_before: &before,
                epoch: epoch.map(str::to_owned),
                after_epoch: Some(&state.after_epoch),
                progress: Some(&progress_receipt),
            })
            .await?;
        if let Some(population) = self.population.get() {
            population.advance_epoch(epoch, &state.after_epoch)?;
        }
        self.retain_regeneration_commit(&progress_receipt)?;
        Ok((result, effects))
    }
}

impl PreparedHistoricalAuthority {
    fn validate_original_mapping(
        &self,
        control: &Control,
        image: &HistoricalControlImage,
    ) -> StorageResult<()> {
        let before: SourceDocumentMapping = image.payload()?;
        let after: SourceDocumentMapping = serde_json::from_str(&control.payload)
            .map_err(|_| integrity("historical S1 mapping invalid"))?;
        let id = if control.table == DOCUMENT_TABLE {
            &before.doc_id
        } else {
            &before.doc_revision_id
        };
        let after_id = if control.table == DOCUMENT_TABLE {
            &after.doc_id
        } else {
            &after.doc_revision_id
        };
        if self.original_mappings.get(id) != Some(&before)
            || id != after_id
            || control.key != stable_key(&[&self.input.space_id, &id.to_string()])
        {
            return Err(integrity(
                "historical S1 original document/revision mapping changed",
            ));
        }
        Ok(())
    }

    async fn validate_original_control(
        &self,
        control: &Control,
        image: &HistoricalControlImage,
    ) -> StorageResult<()> {
        let original_sources = self
            .catchup
            .as_ref()
            .ok_or_else(|| integrity("historical S1 custody absent"))?
            .original_sources();
        let original_source = |id: &RecordId| {
            original_sources
                .iter()
                .find(|old| &old.source_id == id)
                .ok_or_else(|| integrity("historical S1 control not original authority"))
        };
        match control.table {
            "source_head" => {
                let before: SourceHead = image.payload()?;
                let after: SourceHead = serde_json::from_str(&control.payload)
                    .map_err(|_| integrity("historical S1 head invalid"))?;
                let original = original_source(&before.source_id)?;
                if after.source_id != before.source_id
                    || control.key != head_key(&self.input.space_id, &before.source_id)
                    || before.generation != original.head.generation
                    || before.operation_id != original.head.operation_id
                    || before.desired_revision != original.head.desired_revision
                    || before.lifecycle != original.head.lifecycle
                    || (before.published_revision.is_some() && before != original.head)
                {
                    return Err(integrity("historical S1 prior head authority changed"));
                }
            }
            "source_identity" => {
                let before: SourceIdentity = image.payload()?;
                let after: SourceIdentity = serde_json::from_str(&control.payload)
                    .map_err(|_| integrity("historical S1 identity invalid"))?;
                original_source(&before.source_id)?;
                if before.source_id != after.source_id {
                    return Err(integrity("historical S1 identity changed"));
                }
                let source = self
                    .catchup_source_before(&before.source_id)
                    .await?
                    .ok_or_else(|| integrity("historical S1 identity source absent"))?;
                if control.key
                    != external_identity_key(
                        &self.input.space_id,
                        source.type_,
                        source
                            .external_id
                            .as_deref()
                            .ok_or_else(|| integrity("historical S1 external identity absent"))?,
                    )
                {
                    return Err(integrity("historical S1 original identity key changed"));
                }
            }
            "source_authorization" => {
                let before: SourceAuthorizationEnvelope = image.payload()?;
                let after: SourceAuthorizationEnvelope = serde_json::from_str(&control.payload)
                    .map_err(|_| integrity("historical S1 authorization invalid"))?;
                let source_id = original_sources
                    .iter()
                    .find(|old| head_key(&self.input.space_id, &old.source_id) == control.key)
                    .map(|old| old.source_id.clone())
                    .ok_or_else(|| integrity("historical S1 authorization not original"))?;
                let source = self
                    .catchup_source_before(&source_id)
                    .await?
                    .ok_or_else(|| integrity("historical S1 original authorized source absent"))?;
                if before != SourceAuthorizationEnvelope::from(&source)
                    || after.space_id != self.input.space_id
                {
                    return Err(integrity("historical S1 original authorization changed"));
                }
            }
            DOCUMENT_TABLE | REVISION_TABLE => {
                self.validate_original_mapping(control, image)?;
            }
            COMPLETION_TABLE => {
                let before: SourcePublicationCompletion = image.payload()?;
                let after: SourcePublicationCompletion = serde_json::from_str(&control.payload)
                    .map_err(|_| integrity("historical S1 completion invalid"))?;
                if self.original_completions.get(&before.publication.source_id) != Some(&before)
                    || after.publication.source_id != before.publication.source_id
                {
                    return Err(integrity("historical S1 original completion changed"));
                }
            }
            _ => return Err(integrity("historical S1 unsupported replace control")),
        }
        Ok(())
    }
}

/// Source closure is not inventory closure or semantic-index completion. Only
/// the consuming full-ledger verifier can combine it with destination parity.
#[derive(Debug)]
pub struct HistoricalS1SourceClosure {
    pub(crate) source_bindings: Vec<crate::client::HistoricalCatchupSource>,
    pub(crate) configuration_id: RecordId,
    pub(crate) sealed: crate::client::SealedHistoricalCatchupAuthority,
    pub(crate) final_epoch: String,
    pub(crate) source_receipt_sha256: String,
    pub(crate) regeneration_plan_sha256: String,
}

impl PreparedHistoricalAuthority {
    /// Validated durable prefix and complete ordered group count for ledger replay.
    pub fn regeneration_position(&self) -> StorageResult<(u64, u64)> {
        self.check_custody()?;
        let state = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 position absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        Ok((state.next as u64, state.plan.steps.len() as u64))
    }
    /// Reconcile every current native row before a fresh zero-row source cut
    /// writes a publication. The caller must first replay all committed group
    /// effects into the original ledger; no live-state baseline is synthesized.
    pub async fn verify_zero_row_inventory(
        &self,
        ledger: &mut crate::historical_catchup_ledger::HistoricalCatchupLedger,
    ) -> StorageResult<()> {
        self.check_custody()?;
        let (epoch, committed) = {
            let position = self
                .regeneration
                .as_ref()
                .ok_or_else(|| integrity("historical S1 inventory position absent"))?
                .lock()
                .map_err(|_| integrity("historical S1 inventory position unavailable"))?;
            (position.epoch.clone(), position.next as u64)
        };
        ledger
            .verify_regeneration_prefix(
                self.catchup
                    .as_ref()
                    .ok_or_else(|| integrity("historical S1 inventory custody absent"))?,
                &epoch,
                committed,
            )
            .await?;
        self.check_custody()
    }

    /// Read the next declared regeneration step under current custody; none means the sequence is exhausted.
    pub fn next_regeneration_step(&self) -> StorageResult<Option<HistoricalRegenerationStep>> {
        self.check_custody()?;
        let position = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 position absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        Ok(position.plan.steps.get(position.next).cloned())
    }

    async fn closing_progress(
        &self,
        plan: &HistoricalRegenerationPlan,
        step: &HistoricalRegenerationStep,
        ordinal: usize,
        chain_previous: Option<&str>,
        chain_epoch: &str,
    ) -> StorageResult<(CanonicalCommitReceipt, HistoricalRegenerationProgress)> {
        let (authority_binding, last_batch) = self
            .catchup
            .as_ref()
            .ok_or_else(|| integrity("historical S1 custody absent"))?
            .regeneration_binding();
        let key = regeneration_commit_id(authority_binding, ordinal as u64);
        let progress = read_receipt_exact(&self.client, &self.input.space_id, &key).await?;
        let state: HistoricalRegenerationProgress = serde_json::from_value(
            progress
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical S1 closing progress state absent"))?,
        )
        .map_err(|_| integrity("historical S1 closing progress state invalid"))?;
        if progress.commit_id != key
            || progress.space_id != self.input.space_id
            || progress.input_fingerprint != authority_binding
            || progress.plan_fingerprint != plan.plan_sha256
            || state.authority_binding_sha256 != authority_binding
            || state.original_last_batch_receipt_sha256 != last_batch
            || state.s1_plan_sha256 != plan.plan_sha256
            || state.groups != plan.steps.len() as u64
            || state.ordinal != ordinal as u64
            || state.group_id != step.source_id
            || state.previous_regeneration_receipt_sha256.as_deref() != chain_previous
            || chain_epoch != state.before_epoch
            || state.version != 1
            || state.after_epoch == state.before_epoch
            || uuid::Uuid::parse_str(&state.after_epoch).is_err()
            || progress.historical_import.is_some()
            || progress.publication_authority.is_some()
            || !progress.maintenance_authority.is_empty()
            || progress.source_ids != [step.source_id.clone()]
            || progress.source_id.as_ref() != Some(&step.source_id)
            || progress.source_revision.is_some()
            || !progress.effect_ids.is_empty()
            || !progress.extension_ids.is_empty()
            || !progress.outbox_ids.is_empty()
        {
            return Err(integrity("historical S1 complete progress chain changed"));
        }
        Ok((progress, state))
    }

    async fn verify_closing_publication(
        &self,
        external: &str,
        step: &HistoricalRegenerationStep,
        original: &CanonicalCommitReceipt,
        head: &SourceHead,
        publication: &SourcePublicationRepository,
        closing: (
            &mut Option<RecordId>,
            &mut Vec<crate::client::HistoricalCatchupSource>,
        ),
    ) -> StorageResult<()> {
        let (configuration_id, source_bindings) = closing;
        let group = self.publication_group(external)?;
        let operation: StoredOperation = publication
            .read_control("source_operation", &head.operation_id, &self.input.space_id)
            .await?
            .ok_or_else(|| integrity("historical S1 final operation absent"))?;
        let adoption = self
            .verify_existing_group(&group, &operation.receipt)
            .await?;
        let manifest =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                self.client.clone(),
            )
            .get(
                &self.input.space_id,
                original
                    .publication_authority
                    .as_ref()
                    .ok_or_else(|| integrity("historical S1 final publication absent"))?,
            )
            .await?
            .ok_or_else(|| integrity("historical S1 final configuration manifest absent"))?;
        manifest.validate()?;
        if original.historical_import.as_ref()
            != Some(&HistoricalCommitProvenance::for_manifest(&manifest)?)
        {
            return Err(integrity("historical S1 final manifest receipt differs"));
        }
        let current: RecordId = manifest
            .configuration
            .config_id
            .parse()
            .map_err(|_| integrity("historical S1 final configuration identity"))?;
        bind_closing_configuration(configuration_id, &current)?;
        if adoption.commit_id != original.commit_id
            || head.published_revision != head.desired_revision
            || operation.receipt.state != OperationState::Processing
            || operation.receipt.error_code.as_deref()
                != Some(if self.snapshot_index.contains_key(external) {
                    "historical_snapshot_graph_coverage_pending"
                } else {
                    "historical_embedding_coverage_pending"
                })
        {
            return Err(integrity(
                "historical S1 final operation/current coverage differs",
            ));
        }
        source_bindings.push(crate::client::HistoricalCatchupSource {
            source_id: step.source_id.clone(),
            commit_id: original.commit_id.clone(),
            receipt_sha256: sha256(
                &serde_json::to_vec(original)
                    .map_err(|_| integrity("historical S1 binding encode"))?,
            ),
            head: head.clone(),
        });
        Ok(())
    }

    async fn verify_closing_source(
        &self,
        plan: &HistoricalRegenerationPlan,
        step: &HistoricalRegenerationStep,
        state: &HistoricalRegenerationProgress,
        configuration_id: &mut Option<RecordId>,
        source_bindings: &mut Vec<crate::client::HistoricalCatchupSource>,
    ) -> StorageResult<CanonicalCommitReceipt> {
        let publication = SourcePublicationRepository::new(self.client.clone());
        let original = read_receipt_exact(
            &self.client,
            &self.input.space_id,
            &state.adoption_commit_id,
        )
        .await?;
        // A closed primary chain without its same-transaction effect manifest
        // cannot authorize lossless reconstruction of the external ledger.
        self.recovered_regeneration_effects(state.ordinal).await?;
        let head = publication
            .head(&self.input.space_id, &step.source_id)
            .await?
            .ok_or_else(|| integrity("historical S1 final head absent"))?;
        if original.commit_id != state.adoption_commit_id
            || original.space_id != self.input.space_id
            || original.source_id.as_ref() != Some(&step.source_id)
        {
            return Err(integrity("historical S1 final receipt identity differs"));
        }
        if let Some(external) = &step.external_source_id {
            self.verify_closing_publication(
                external,
                step,
                &original,
                &head,
                &publication,
                (configuration_id, source_bindings),
            )
            .await?;
        } else {
            let retirement: HistoricalRetirementReceipt = serde_json::from_value(
                original
                    .replay_output
                    .clone()
                    .ok_or_else(|| integrity("historical S1 final retirement absent"))?,
            )
            .map_err(|_| integrity("historical S1 final retirement invalid"))?;
            bind_closing_configuration(configuration_id, &retirement.configuration_id)?;
            let expected_commit = stable_key(&[
                "historical-s1-retirement-v1",
                &plan.plan_sha256,
                &step.source_id.to_string(),
            ]);
            if retirement.source_id != step.source_id
                || retirement.plan_sha256 != plan.plan_sha256
                || retirement.original_snapshot_sha256 != self.input.snapshot_sha256
                || retirement.commit_id != expected_commit
                || original.commit_id != expected_commit
                || head.lifecycle != SourceVisibility::Retracted
                || head.published_revision.is_some()
                || head.operation_id != retirement.operation.operation_id
            {
                return Err(integrity("historical S1 final retirement differs"));
            }
        }
        drop(publication);
        Ok(original)
    }

    async fn closing_configuration(
        &self,
        mut configuration_id: Option<RecordId>,
        sealed: &crate::client::SealedHistoricalCatchupAuthority,
        canonical: &CanonicalRepository,
        final_epoch: &str,
    ) -> StorageResult<RecordId> {
        if configuration_id.is_none() {
            configuration_id = sealed.certified_empty_baseline_configuration_id()?;
        }
        let configuration_id = configuration_id
            .ok_or_else(|| integrity("historical S1 closing configuration certificate absent"))?;
        let current_config = crate::SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(&self.input.space_id)
            .await?
            .ok_or_else(|| integrity("historical S1 closing configuration absent"))?;
        if current_config.base.id != configuration_id
            || configuration_id.table() != TableName::SpaceConfig
        {
            return Err(integrity("historical S1 closing configuration changed"));
        }
        if canonical
            .observe_epoch(&self.input.space_id)
            .await?
            .as_deref()
            != Some(final_epoch)
        {
            return Err(integrity(
                "historical S1 closing configuration epoch changed",
            ));
        }
        Ok(configuration_id)
    }

    fn completed_regeneration_position(
        &self,
    ) -> StorageResult<(HistoricalRegenerationPlan, String, String, Option<String>)> {
        let position = self
            .regeneration
            .as_ref()
            .ok_or_else(|| integrity("historical S1 position absent"))?
            .lock()
            .map_err(|_| integrity("historical S1 position unavailable"))?;
        if position.next != position.plan.steps.len() {
            return Err(integrity(
                "historical S1 complete group population not committed",
            ));
        }
        Ok((
            position.plan.clone(),
            position.initial_epoch.clone(),
            position.epoch.clone(),
            position.previous.clone(),
        ))
    }

    /// Consume the complete S1 source proof only after the final ordered step.
    /// Never clears custody, marks an operation Completed, or asserts vector parity.
    pub async fn close_regeneration(self) -> StorageResult<HistoricalS1SourceClosure> {
        self.check_custody()?;
        let (plan, initial_epoch, final_epoch, last) = self.completed_regeneration_position()?;
        let canonical = CanonicalRepository::new(self.client.clone());
        if canonical
            .observe_epoch(&self.input.space_id)
            .await?
            .as_deref()
            != Some(final_epoch.as_str())
        {
            return Err(integrity("historical S1 closing epoch changed"));
        }
        let sealed = self
            .catchup
            .as_ref()
            .ok_or_else(|| integrity("historical S1 custody absent"))?;
        let mut source_bindings = Vec::new();
        let mut configuration_id = None;
        let mut chain_previous = None;
        let mut chain_epoch = initial_epoch;
        let mut hash = Sha256::new();
        hash.update(plan.plan_sha256.as_bytes());
        for (ordinal, step) in plan.steps.iter().enumerate() {
            let (progress, state) = self
                .closing_progress(
                    &plan,
                    step,
                    ordinal,
                    chain_previous.as_deref(),
                    &chain_epoch,
                )
                .await?;
            let original = self
                .verify_closing_source(
                    &plan,
                    step,
                    &state,
                    &mut configuration_id,
                    &mut source_bindings,
                )
                .await?;
            let bytes = serde_json::to_vec(&progress)
                .map_err(|_| integrity("historical S1 closing receipt encode"))?;
            chain_previous = Some(sha256(&bytes));
            chain_epoch = state.after_epoch;
            hash.update(bytes);
            hash.update(b"\n");
            hash.update(
                serde_json::to_vec(&original)
                    .map_err(|_| integrity("historical S1 source receipt encode"))?,
            );
            hash.update(b"\n");
        }
        if chain_previous != last
            || chain_epoch != final_epoch
            || canonical
                .observe_epoch(&self.input.space_id)
                .await?
                .as_deref()
                != Some(final_epoch.as_str())
        {
            return Err(integrity("historical S1 final chain/epoch differs"));
        }
        let configuration_id = self
            .closing_configuration(configuration_id, sealed, &canonical, &final_epoch)
            .await?;
        self.client.verify_journal_identity().await?;
        self.check_custody()?;
        let sealed = self
            .catchup
            .ok_or_else(|| integrity("historical S1 closure custody absent"))?;
        Ok(HistoricalS1SourceClosure {
            source_bindings,
            configuration_id,
            sealed,
            final_epoch,
            source_receipt_sha256: hex::encode(hash.finalize()),
            regeneration_plan_sha256: plan.plan_sha256,
        })
    }
}

/// Replay inventory is derived from immutable source proof identities, never
/// from an enumeration of whatever happens to be in the destination today.
impl PreparedHistoricalAuthority {
    async fn inventory_publication_manifest(
        &self,
        original: &CanonicalCommitReceipt,
        publication: &PublicationFence,
    ) -> StorageResult<crate::source_publication_manifest::SourcePublicationManifest> {
        let space = &self.input.space_id;
        let manifest =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                self.client.clone(),
            )
            .get(space, publication)
            .await?
            .ok_or_else(|| integrity("historical replay inventory manifest absent"))?;
        manifest.validate()?;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical replay inventory proof absent"))?;
        if original.historical_import.as_ref()
            != Some(&HistoricalCommitProvenance::for_manifest(&manifest)?)
            || original.commit_id != proof.commit_id(&manifest)
            || proof.plan_id != self.input.plan_id
        {
            return Err(integrity("historical replay inventory authority differs"));
        }
        let group = self.publication_group(&proof.external_source_id)?;
        if proof.document_mapping_sha256 != group.document_mapping_sha256
            || proof.documents.keys().collect::<BTreeSet<_>>()
                != group
                    .documents
                    .iter()
                    .map(|doc| &doc.legacy_doc_id)
                    .collect()
        {
            return Err(integrity(
                "historical replay inventory document population differs",
            ));
        }
        if let Some(snapshot) = &proof.snapshot_graph {
            let declaration = self
                .input
                .snapshot_groups
                .iter()
                .find(|group| group.external_source_id == proof.external_source_id)
                .ok_or_else(|| integrity("historical snapshot inventory undeclared"))?;
            if snapshot.original_rows_sha256 != declaration.original_rows_sha256 {
                return Err(integrity(
                    "historical snapshot inventory population differs",
                ));
            }
        } else if self.snapshot_index.contains_key(&proof.external_source_id) {
            return Err(integrity("historical snapshot inventory proof missing"));
        }
        Ok(manifest)
    }

    async fn publication_regeneration_inventory(
        &self,
        original: &CanonicalCommitReceipt,
        publication: &PublicationFence,
        source: &RecordId,
        ids: &mut BTreeMap<String, surrealdb::sql::Thing>,
    ) -> StorageResult<()> {
        let space = &self.input.space_id;
        let manifest = self
            .inventory_publication_manifest(original, publication)
            .await?;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical replay inventory proof absent"))?;
        let mut add = |table: &str, key: String| {
            let id = control_thing(table, &key);
            ids.insert(id.to_string(), id);
        };
        let key = manifest_key(space, publication);
        add(
            "source_identity",
            external_identity_key(space, SourceType::Manual, &proof.external_source_id),
        );
        add(
            "source_revision",
            revision_key(space, source, &publication.revision_id),
        );
        add("source_operation", publication.operation_id.clone());
        add("source_authorization", head_key(space, source));
        add(MANIFEST_TABLE, key.clone());
        add(COMPLETION_TABLE, key);
        add(
            COMPLETION_TABLE,
            stable_key(&["published-source-completion/v1", space, &source.to_string()]),
        );
        for mapping in &manifest.documents {
            add(
                DOCUMENT_TABLE,
                stable_key(&[space, &mapping.doc_id.to_string()]),
            );
            add(
                REVISION_TABLE,
                stable_key(&[space, &mapping.doc_revision_id.to_string()]),
            );
        }
        for record in proof.graph_fingerprints.keys() {
            add(
                "source_effect_support",
                crate::canonical_commit::support_key(
                    space,
                    source,
                    &publication.revision_id,
                    record,
                ),
            );
        }
        // config_id is a complete typed RecordId, not a control key.
        let config: RecordId = manifest
            .configuration
            .config_id
            .parse()
            .map_err(|_| integrity("historical replay configuration identity"))?;
        if config.table() != TableName::SpaceConfig {
            return Err(integrity("historical replay configuration table"));
        }
        let native = crate::surreal::thing_from_record_id(&config);
        ids.insert(native.to_string(), native);
        let expected_effects: BTreeSet<_> = proof
            .documents
            .keys()
            .cloned()
            .chain(std::iter::once(source.clone()))
            .collect();
        if original.effect_ids.iter().cloned().collect::<BTreeSet<_>>() != expected_effects
            || original.effect_ids.len() != expected_effects.len()
        {
            return Err(integrity("historical replay canonical effects differ"));
        }
        for id in &original.effect_ids {
            let native = crate::surreal::thing_from_record_id(id);
            ids.insert(native.to_string(), native);
        }
        Ok(())
    }

    async fn regeneration_inventory(
        &self,
        progress: &CanonicalCommitReceipt,
        original: &CanonicalCommitReceipt,
    ) -> StorageResult<Vec<(String, surrealdb::sql::Thing)>> {
        let space = &self.input.space_id;
        let source = original
            .source_id
            .as_ref()
            .ok_or_else(|| integrity("historical inventory source absent"))?;
        let mut ids = BTreeMap::<String, surrealdb::sql::Thing>::new();
        let mut add = |table: &str, key: String| {
            let id = control_thing(table, &key);
            ids.insert(id.to_string(), id);
        };
        add("source_head", head_key(space, source));
        add(
            "source_commit_receipt",
            stable_key(&[space, &original.commit_id]),
        );
        add(
            "source_commit_receipt",
            stable_key(&[space, &progress.commit_id]),
        );
        if let Some(publication) = &original.publication_authority {
            self.publication_regeneration_inventory(original, publication, source, &mut ids)
                .await?;
        } else {
            let retirement: HistoricalRetirementReceipt = serde_json::from_value(
                original
                    .replay_output
                    .clone()
                    .ok_or_else(|| integrity("historical retirement output absent"))?,
            )
            .map_err(|_| integrity("historical retirement output invalid"))?;
            add("source_operation", retirement.operation.operation_id);
            if retirement.configuration_id.table() != TableName::SpaceConfig {
                return Err(integrity(
                    "historical retirement configuration table differs",
                ));
            }
            let native = crate::surreal::thing_from_record_id(&retirement.configuration_id);
            ids.insert(native.to_string(), native);
        }
        let fences: BTreeSet<_> = ids
            .values()
            .filter(|id| {
                matches!(
                    id.tb.as_str(),
                    "doc" | "source_head" | "source_publication_manifest"
                )
            })
            .map(|id| format!("{}:{}", id.tb, &sha256(id.to_string().as_bytes())[..3]))
            .collect();
        for key in fences {
            let id = control_thing("native_read_fence", &key);
            ids.insert(id.to_string(), id);
        }
        for table in ["canonical_epoch", "source_erasure_guard"] {
            let id = control_thing(table, space);
            ids.insert(id.to_string(), id);
        }
        let nonce = surrealdb::sql::Thing::from((
            "canonical_epoch",
            surrealdb::sql::Id::Array(surrealdb::sql::Array::from(vec![
                surrealdb::sql::Value::from("recall_source_support_index"),
                surrealdb::sql::Value::from(1i64),
            ])),
        ));
        ids.insert(nonce.to_string(), nonce);
        Ok(ids.into_values().map(|id| (id.tb.clone(), id)).collect())
    }

    /// Recover one committed step's immutable effect manifest. The caller can
    /// replay its expected ledger after a process died after the native COMMIT.
    pub async fn recovered_regeneration_effects(
        &self,
        ordinal: u64,
    ) -> StorageResult<crate::historical_native_effects::VerifiedHistoricalNativeEffects> {
        self.check_custody()?;
        let (step, next, plan_sha) = {
            let position = self
                .regeneration
                .as_ref()
                .ok_or_else(|| integrity("historical regeneration absent"))?
                .lock()
                .map_err(|_| integrity("historical regeneration unavailable"))?;
            (
                position
                    .plan
                    .steps
                    .get(
                        usize::try_from(ordinal)
                            .map_err(|_| integrity("historical ordinal overflow"))?,
                    )
                    .cloned()
                    .ok_or_else(|| integrity("historical ordinal outside plan"))?,
                position.next,
                position.plan.plan_sha256.clone(),
            )
        };
        if ordinal >= next as u64 {
            return Err(integrity("historical step is not committed"));
        }
        let authority_binding = self
            .catchup
            .as_ref()
            .ok_or_else(|| integrity("historical catchup absent"))?
            .regeneration_binding()
            .0;
        let id = regeneration_commit_id(authority_binding, ordinal);
        let progress = read_receipt_exact(&self.client, &self.input.space_id, &id).await?;
        let state: HistoricalRegenerationProgress = serde_json::from_value(
            progress
                .replay_output
                .clone()
                .ok_or_else(|| integrity("historical progress absent"))?,
        )
        .map_err(|_| integrity("historical progress invalid"))?;
        if state.ordinal != ordinal
            || state.group_id != step.source_id
            || state.s1_plan_sha256 != plan_sha
        {
            return Err(integrity("historical effect replay step differs"));
        }
        let original = read_receipt_exact(
            &self.client,
            &self.input.space_id,
            &state.adoption_commit_id,
        )
        .await?;
        if self.client.embedded_store().is_some() {
            return Err(integrity(
                "historical native effect recovery cannot attest embedded inventory",
            ));
        }
        let inventory = self.regeneration_inventory(&progress, &original).await?;
        let binding = crate::historical_native_effects::HistoricalNativeEffectBinding::new(
            authority_binding,
            "regeneration",
            ordinal,
            &progress,
            &inventory,
        )?;
        let effects = crate::historical_native_effects::recover_verified(
            &self.client,
            &step.source_id,
            &binding,
            &progress,
            &inventory,
        )
        .await?;
        self.check_custody()?;
        Ok(effects)
    }
}

pub(super) async fn read_receipt_exact(
    client: &StorageClient,
    space: &str,
    key: &str,
) -> StorageResult<CanonicalCommitReceipt> {
    let physical = stable_key(&[space, key]);
    let image = full_control(client, "source_commit_receipt", &physical, space).await?;
    let receipt: CanonicalCommitReceipt = image.payload()?;
    let source = receipt
        .source_id
        .as_ref()
        .ok_or_else(|| integrity("historical receipt source absent"))?;
    if receipt.space_id != space || receipt.commit_id != key {
        return Err(integrity("historical receipt identity differs"));
    }
    let control = Control::new("source_commit_receipt", physical.clone(), &receipt)?;
    let mut expected = control.value(space, source);
    expected["id"] = format!("source_commit_receipt:{physical}").into();
    let matches = match image {
        HistoricalControlImage::Embedded(actual) => actual == expected,
        HistoricalControlImage::Native(actual) => {
            let surrealdb::sql::Value::Object(mut fields) =
                crate::surreal::json_to_surreal(expected)?
            else {
                return Err(integrity("historical receipt object"));
            };
            fields.insert(
                "id".into(),
                control_thing("source_commit_receipt", &physical).into(),
            );
            actual == crate::surreal::stored_native_value(surrealdb::sql::Value::Object(fields))
        }
    };
    if !matches {
        return Err(integrity("historical full immutable receipt changed"));
    }
    Ok(receipt)
}

fn bind_closing_configuration(
    target: &mut Option<RecordId>,
    current: &RecordId,
) -> StorageResult<()> {
    if current.table() != TableName::SpaceConfig
        || target.as_ref().is_some_and(|previous| previous != current)
    {
        return Err(integrity("historical S1 mixed configuration identities"));
    }
    *target = Some(current.clone());
    Ok(())
}

#[cfg(test)]
mod closing_configuration_tests {
    use super::*;
    #[test]
    fn configuration_closure_requires_one_exact_typed_identity() -> StorageResult<()> {
        let first = RecordId::new_v4(TableName::SpaceConfig);
        let mut identity = None;
        bind_closing_configuration(&mut identity, &first)?;
        bind_closing_configuration(&mut identity, &first)?;
        assert!(bind_closing_configuration(
            &mut identity,
            &RecordId::new_v4(TableName::SpaceConfig)
        )
        .is_err());
        assert!(
            bind_closing_configuration(&mut None, &RecordId::new_v4(TableName::Source)).is_err()
        );
        assert_eq!(identity, Some(first));
        Ok(())
    }
}
