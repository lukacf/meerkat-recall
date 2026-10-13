//! Prepare predicate hygiene under the maintenance owner's captured plan.
use super::*;
use crate::maintenance_commit::{
    merge_authority, request_fingerprint, same_authorization, PlanRecord,
    PlanningTruthMaintenanceStorage,
};
use crate::PipelineResult;
use std::collections::HashSet;

async fn rows<T: PlanRecord + serde::de::DeserializeOwned, R: Repository<T>>(
    planning: &PlanningTruthMaintenanceStorage,
    repo: &R,
    space: &str,
    predicate: impl Fn(&T) -> bool,
) -> PipelineResult<Vec<T>> {
    let mut live = Vec::new();
    let mut cursor = None;
    loop {
        let page = repo.list(space, 256, cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        let next = page.last().map(|row| row.base().id.clone());
        if next == cursor {
            return Err(PipelineError::InvalidState(
                "Predicate hygiene cursor did not advance".into(),
            ));
        }
        live.extend(page.into_iter().filter(&predicate));
        cursor = next;
    }
    planning.preview_rows(live, predicate)
}

fn envelope_key(base: &BaseFields) -> PipelineResult<String> {
    let mut labels = base.security.labels.clone();
    labels.sort();
    labels.dedup();
    let mut handling = base.security.handling.clone();
    handling.sort();
    handling.dedup();
    let mut subjects: Vec<_> = base
        .subjects
        .iter()
        .map(|link| link.id().to_string())
        .collect();
    subjects.sort();
    subjects.dedup();
    request_fingerprint(&(
        &base.space_id,
        &base.security.policy_id,
        base.security.level,
        labels,
        handling,
        subjects,
        base.subjects_state,
    ))
}

fn output_base(input: &BaseFields, table: TableName) -> BaseFields {
    let mut base = input.clone();
    base.id = RecordId::new_v4(table);
    base.created_by = "predicate_hygiene_worker".into();
    base.created_at = chrono::Utc::now();
    base.updated_at = base.created_at;
    base.deleted_at = None;
    base
}

struct Proposal {
    base: BaseFields,
    data: serde_json::Value,
    contributors: Vec<BaseFields>,
}

fn proposal_key(
    alias: &str,
    canonical: &str,
    base: &BaseFields,
) -> PipelineResult<(String, String, String)> {
    Ok((alias.into(), canonical.into(), envelope_key(base)?))
}

#[expect(
    clippy::too_many_arguments,
    reason = "Keep proposal evidence, both predicate identities, authorization inputs and deduplication state explicit within the same atomic plan."
)]
fn add_proposal(
    planning: &PlanningTruthMaintenanceStorage,
    existing_rows: &BTreeMap<(String, String, String), WorkArtifact>,
    proposals: &mut Vec<Proposal>,
    existing: &mut HashSet<(String, String, String)>,
    alias: &str,
    canonical: &str,
    similarity: f64,
    reason: &str,
    inputs: &[&BaseFields],
) -> PipelineResult<bool> {
    let Some(first) = inputs.first() else {
        return Err(PipelineError::InvalidState(
            "Predicate proposal has no authority".into(),
        ));
    };
    let mut base = output_base(first, TableName::WorkArtifact);
    for input in inputs.iter().skip(1) {
        merge_authority(&mut base, input)?;
    }
    let key = proposal_key(alias, canonical, &base)?;
    if !existing.insert(key.clone()) {
        if let Some(row) = existing_rows.get(&key) {
            planning.observe(row)?;
        }
        return Ok(false);
    }
    proposals.push(Proposal {
        base,
        contributors: inputs.iter().map(|base| (**base).clone()).collect(),
        data: serde_json::json!({"type":HYGIENE_PROPOSAL_ARTIFACT_TYPE,"alias_predicate_id":alias,
            "canonical_predicate_id":canonical,"similarity":similarity,"reason":reason}),
    });
    Ok(true)
}

fn has_alias(
    aliases: &[PredicateAlias],
    raw: &str,
    authority: &BaseFields,
) -> PipelineResult<bool> {
    for alias in aliases {
        if alias.status == AliasStatus::Active
            && alias.alias.eq_ignore_ascii_case(raw)
            && same_authorization(&alias.base, authority)?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn stage_alias(
    planning: &PlanningTruthMaintenanceStorage,
    aliases: &mut Vec<PredicateAlias>,
    raw: &PredicateRegistry,
    canonical: &PredicateRegistry,
    confidence: f32,
    plugin: &str,
) -> PipelineResult<()> {
    planning.observe(raw)?;
    planning.observe(canonical)?;
    let mut base = output_base(&raw.base, TableName::PredicateAlias);
    merge_authority(&mut base, &canonical.base)?;
    let alias = PredicateAlias {
        base,
        alias: raw.predicate_id.clone(),
        alias_type: AliasType::RawString,
        source_plugin: Some(plugin.into()),
        confidence,
        status: AliasStatus::Active,
        scope: AliasScope::Global,
        predicate_id: canonical.predicate_id.clone(),
        confidence_version: 1,
        usage_count: 0,
    };
    planning.stage_inherited(
        None,
        alias.clone(),
        &[raw.base.clone(), canonical.base.clone()],
    )?;
    aliases.push(alias);
    Ok(())
}

/// Coalesce transport inputs, preserving every partition's order and boundaries.
/// Only the provider transport is shared: callers still compare predicates and
/// inherit authority within each original partition. Provider batch limits and
/// physical-request admission continue to apply inside the existing adapter.
pub(super) async fn embed_partition_inputs(
    partitions: &[Vec<PredicateRegistry>],
    provider: &dyn EmbeddingProvider,
) -> PipelineResult<Vec<Vec<f32>>> {
    let texts: Vec<_> = partitions
        .iter()
        .filter(|partition| partition.len() >= 2)
        .flat_map(|partition| partition.iter().map(predicate_embed_text))
        .collect();
    if texts.is_empty() {
        return Ok(Vec::new());
    }
    let embeddings = provider.embed(&texts).await?;
    if embeddings.len() != texts.len()
        || embeddings.iter().any(|row| {
            row.len() != provider.dimensions()
                || row.is_empty()
                || row.iter().any(|value| !value.is_finite())
        })
    {
        return Err(PipelineError::ExtractionError(
            "Predicate embedding batch is incomplete or non-finite".into(),
        ));
    }
    Ok(embeddings)
}

/// All aliases and drafts are prepared; the caller owns the sole graph commit.
pub(crate) async fn prepare_predicate_hygiene_sweep(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space: &str,
    config: &PredicateHygieneConfig,
    provider: &dyn EmbeddingProvider,
    work_run: Option<&RecordId>,
) -> PipelineResult<HygieneResult> {
    if !config.auto_alias_threshold.is_finite()
        || !config.proposal_threshold.is_finite()
        || !(0.0..=1.0).contains(&config.auto_alias_threshold)
        || !(0.0..=1.0).contains(&config.proposal_threshold)
    {
        return Err(PipelineError::InvalidInput(
            "Predicate similarity thresholds must be finite probabilities".into(),
        ));
    }
    let mut predicates = rows(
        planning,
        &PredicateRegistryRepository::new(storage.clone()),
        space,
        |row: &PredicateRegistry| row.status == "active",
    )
    .await?;
    predicates.sort_by(|a, b| a.base.id.cmp(&b.base.id));
    predicates.truncate(config.max_predicates);
    let mut aliases = rows(
        planning,
        &PredicateAliasRepository::new(storage.clone()),
        space,
        |_: &PredicateAlias| true,
    )
    .await?;
    let artifacts = rows(
        planning,
        &WorkArtifactRepository::new(storage.clone()),
        space,
        |row: &WorkArtifact| {
            row.artifact_type == HYGIENE_PROPOSAL_ARTIFACT_TYPE
                && row.status != ArtifactStatus::Rejected
        },
    )
    .await?;
    let mut existing = HashSet::new();
    let mut existing_rows = BTreeMap::new();
    for row in artifacts {
        if let (Some(alias), Some(canonical)) = (
            row.data
                .get("alias_predicate_id")
                .and_then(serde_json::Value::as_str),
            row.data
                .get("canonical_predicate_id")
                .and_then(serde_json::Value::as_str),
        ) {
            let key = proposal_key(alias, canonical, &row.base)?;
            existing.insert(key.clone());
            existing_rows.insert(key, row);
        }
    }
    let mut result = HygieneResult {
        aliases_created: 0,
        proposals_created: 0,
        predicates_scanned: predicates.len(),
    };
    let mut proposals = Vec::new();
    if !config.auto_alias_enabled {
        for alias in &mut aliases {
            if alias.status != AliasStatus::Active
                || alias.source_plugin.as_deref() != Some("predicate_hygiene")
            {
                continue;
            }
            planning.observe(alias)?;
            if add_proposal(
                planning,
                &existing_rows,
                &mut proposals,
                &mut existing,
                &alias.alias,
                &alias.predicate_id,
                f64::from(alias.confidence),
                "active predicate_hygiene alias deactivated for review",
                &[&alias.base],
            )? {
                result.proposals_created += 1;
            }
            let before = alias.clone();
            alias.status = AliasStatus::Deprecated;
            alias.base.updated_at = chrono::Utc::now();
            planning.stage(Some(before), alias.clone())?;
        }
    }
    let mut partitions: BTreeMap<String, Vec<PredicateRegistry>> = BTreeMap::new();
    for predicate in predicates {
        partitions
            .entry(envelope_key(&predicate.base)?)
            .or_default()
            .push(predicate);
    }
    let partitions: Vec<_> = partitions.into_values().collect();
    // Capture all actual inputs before any shared provider request. Transport
    // vectors stay local; decisions enter the caller's existing canonical commit.
    for partition in partitions.iter().filter(|partition| partition.len() >= 2) {
        for predicate in partition {
            planning.observe(predicate)?;
        }
    }
    let mut partition_vectors = embed_partition_inputs(&partitions, provider)
        .await?
        .into_iter();
    for partition in partitions {
        let mut syntax: BTreeMap<String, Vec<&PredicateRegistry>> = BTreeMap::new();
        for predicate in &partition {
            if let Some(key) = predicate_syntax_key(&predicate.predicate_id) {
                syntax.entry(key).or_default().push(predicate);
            }
        }
        for group in syntax.values().filter(|group| group.len() > 1) {
            let Some(canonical) = group.iter().max_by(|a, b| {
                a.usage_count
                    .cmp(&b.usage_count)
                    .then_with(|| {
                        predicate_syntax_canonical_score(&a.predicate_id)
                            .cmp(&predicate_syntax_canonical_score(&b.predicate_id))
                    })
                    .then_with(|| b.predicate_id.cmp(&a.predicate_id))
            }) else {
                continue;
            };
            for raw in group {
                if raw.predicate_id == canonical.predicate_id
                    || has_alias(&aliases, &raw.predicate_id, &raw.base)?
                {
                    continue;
                }
                stage_alias(
                    planning,
                    &mut aliases,
                    raw,
                    canonical,
                    1.0,
                    "predicate_syntax",
                )?;
                result.aliases_created += 1;
            }
        }
        if partition.len() < 2 {
            continue;
        }
        // The complete flat response was validated before any grouping. Take
        // exactly this partition's rows; similarity never crosses envelopes.
        let embeddings: Vec<_> = partition_vectors.by_ref().take(partition.len()).collect();
        for group in
            group_predicates_by_similarity(&partition, &embeddings, config.proposal_threshold)
        {
            let canonical = partition
                .iter()
                .find(|row| row.predicate_id == group.canonical)
                .ok_or_else(|| {
                    PipelineError::InvalidState(
                        "Canonical predicate disappeared during preparation".into(),
                    )
                })?;
            for (id, similarity) in group.members {
                let raw = partition
                    .iter()
                    .find(|row| row.predicate_id == id)
                    .ok_or_else(|| {
                        PipelineError::InvalidState(
                            "Predicate disappeared during preparation".into(),
                        )
                    })?;
                if has_alias(&aliases, &id, &raw.base)? {
                    continue;
                }
                if config.auto_alias_enabled && similarity >= config.auto_alias_threshold {
                    stage_alias(
                        planning,
                        &mut aliases,
                        raw,
                        canonical,
                        similarity as f32,
                        "predicate_hygiene",
                    )?;
                    result.aliases_created += 1;
                } else if add_proposal(
                    planning,
                    &existing_rows,
                    &mut proposals,
                    &mut existing,
                    &id,
                    &group.canonical,
                    similarity,
                    "embedding similarity review",
                    &[&raw.base, &canonical.base],
                )? {
                    result.proposals_created += 1;
                }
            }
        }
    }
    for proposal in proposals {
        let run_id = if let Some(run) = work_run {
            run.clone()
        } else {
            let scope = request_fingerprint(&(
                "predicate_hygiene",
                planning.prepared_plan()?.observed_epoch,
                &proposal.data,
                envelope_key(&proposal.base)?,
                &proposal.base.provenance.sources,
            ))?;
            elephant_storage::work_run_repository::create_trusted_proposal_context(
                storage,
                &proposal.base,
                &scope,
                &proposal.base.provenance.sources,
            )
            .await?
        };
        let run = elephant_storage::WorkRunRepository::new(storage.clone())
            .get(space, &run_id)
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState("Predicate proposal work context is missing".into())
            })?;
        let item = elephant_storage::WorkItemRepository::new(storage.clone())
            .get(space, run.work_item.id())
            .await?
            .ok_or_else(|| {
                PipelineError::InvalidState("Predicate proposal work item is missing".into())
            })?;
        planning.observe(&run)?;
        planning.observe(&item)?;
        let artifact = WorkArtifact {
            base: proposal.base,
            work_run: Link::new(run_id),
            artifact_type: HYGIENE_PROPOSAL_ARTIFACT_TYPE.into(),
            data: proposal.data,
            status: ArtifactStatus::Draft,
            validation_errors: None,
            validated_at: None,
            committed_at: None,
        };
        planning.stage_inherited(None, artifact, &proposal.contributors)?;
    }
    Ok(result)
}
