//! Retained deterministic hygiene and dedupe, prepared without live writes.
use super::*;
use crate::maintenance_commit::{
    contains_authorization, request_fingerprint, same_authorization, PlanRecord,
    PlanningTruthMaintenanceStorage,
};
use crate::truth_maintenance::TruthMaintenanceStorage;
use crate::{PipelineError, PipelineResult};
use std::collections::{BTreeMap, HashMap, HashSet};

pub(super) async fn rows<T: PlanRecord + serde::de::DeserializeOwned, R: Repository<T>>(
    planning: &PlanningTruthMaintenanceStorage,
    repository: &R,
    space: &str,
) -> PipelineResult<Vec<T>> {
    let mut selected = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = repository.list(space, 500, cursor.as_ref()).await?;
        if page.is_empty() {
            break;
        }
        let next = page.last().map(|row| row.base().id.clone());
        if next == cursor {
            return Err(PipelineError::InvalidState(
                "Maintenance pagination did not advance".into(),
            ));
        }
        for row in page {
            let id = row.base().id.clone();
            if let Some(row) = planning.preview_row(Some(row), &id, false)? {
                selected.insert(id, row);
            }
        }
        cursor = next;
    }
    // Later phases also see records created by earlier phases in this plan.
    for effect in planning.prepared_plan()?.effects {
        if let Some(row) = effect.after.as_ref().and_then(T::from_record) {
            if row.base().deleted_at.is_none() {
                selected.insert(row.base().id.clone(), row);
            } else {
                selected.remove(&row.base().id);
            }
        }
    }
    Ok(selected.into_values().collect())
}

fn partition(base: &elephant_types::records::BaseFields) -> PipelineResult<String> {
    let mut subjects: Vec<_> = base
        .subjects
        .iter()
        .map(|row| row.id().to_string())
        .collect();
    subjects.sort();
    subjects.dedup();
    let mut labels = base.security.labels.clone();
    labels.sort();
    labels.dedup();
    let mut handling = base.security.handling.clone();
    handling.sort();
    handling.dedup();
    request_fingerprint(&(
        base.space_id.as_str(),
        &base.security.policy_id,
        base.security.level,
        labels,
        handling,
        subjects,
        base.subjects_state,
    ))
}

pub(super) async fn assertions(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &elephant_storage::StorageClient,
    space: &str,
) -> PipelineResult<AssertionSweepResult> {
    let mut result = AssertionSweepResult::default();
    let mut groups: BTreeMap<(AssertionSlot, String), Vec<Assertion>> = BTreeMap::new();
    for row in rows(planning, &AssertionRepository::new(storage.clone()), space).await? {
        if row.resolution_status != ResolutionStatus::Resolved {
            continue;
        }
        result.scanned += 1;
        if let Some(slot) = AssertionSlot::from_assertion(&row) {
            groups
                .entry((slot, partition(&row.base)?))
                .or_default()
                .push(row);
        }
    }
    for mut group in groups.into_values().filter(|group| group.len() > 1) {
        group.sort_by(assertion_winner_order);
        planning.observe(&group.remove(0))?;
        result.duplicate_slots += 1;
        for before in group {
            let mut after = before.clone();
            after.resolution_status = ResolutionStatus::Superseded;
            after.base.updated_at = chrono::Utc::now();
            planning.stage_assertion(before, after)?;
            result.superseded += 1;
        }
    }
    Ok(result)
}

pub(super) async fn relationships(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &elephant_storage::StorageClient,
    space: &str,
) -> PipelineResult<RelationshipSweepResult> {
    let mut result = RelationshipSweepResult::default();
    let mut groups: BTreeMap<(RelationshipSlot, String, String), Vec<Rel>> = BTreeMap::new();
    for row in rows(planning, &RelRepository::new(storage.clone()), space).await? {
        result.scanned += 1;
        groups
            .entry((
                RelationshipSlot::from_rel(&row),
                partition(&row.base)?,
                request_fingerprint(&row.validity_window)?,
            ))
            .or_default()
            .push(row);
    }
    for mut group in groups.into_values().filter(|group| group.len() > 1) {
        group.sort_by(relationship_winner_order);
        let mut winner = group.remove(0);
        let before = winner.clone();
        let contributors = group.clone();
        result.duplicate_slots += 1;
        let mut evidence: HashSet<_> = winner.evidence.iter().map(evidence_identity_key).collect();
        for duplicate in group {
            for support in &duplicate.evidence {
                if evidence.insert(evidence_identity_key(support)) {
                    winner.evidence.push(support.clone());
                    result.evidence_added += 1;
                }
            }
            winner.confidence = winner.confidence.max(duplicate.confidence);
            winner.status = merged_claim_status(winner.status, duplicate.status);
            result.merged += 1;
        }
        winner.base.updated_at = chrono::Utc::now();
        planning
            .merge_relationships(before, winner, &contributors)
            .await?;
        for duplicate in &contributors {
            planning.delete_rel(space, &duplicate.base.id).await?;
        }
    }
    Ok(result)
}

pub(super) async fn hygiene(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &elephant_storage::StorageClient,
    space: &str,
) -> PipelineResult<EntityHygieneSweepResult> {
    let repo = EntityRepository::new(storage.clone());
    let mut entities = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = repo
            .search(&SearchCriteria {
                space_id: Some(space.into()),
                include_deleted: true,
                limit: 500,
                cursor: cursor.clone(),
                ..Default::default()
            })
            .await?;
        if page.results.is_empty() {
            break;
        }
        for row in page.results {
            let id = row.base.id.clone();
            if let Some(current) = planning.preview_row(Some(row), &id, true)? {
                entities.insert(id, current);
            }
        }
        if !page.has_more {
            break;
        }
        if page.cursor == cursor {
            return Err(PipelineError::InvalidState(
                "Entity hygiene pagination did not advance".into(),
            ));
        }
        cursor = page.cursor;
    }
    let mut result = EntityHygieneSweepResult::default();
    let mut retired = HashSet::new();
    let mut redirects = HashMap::new();
    let mut keys = HashMap::new();
    let mut names = HashMap::new();
    let mut deleted = Vec::new();
    for row in entities.values() {
        if row.base.deleted_at.is_some() {
            if row.status == EntityStatus::Merged {
                if let Some(target) = &row.redirect_to {
                    redirects.insert(row.base.id.clone(), target.id().clone());
                }
            } else {
                deleted.push(row);
            }
            continue;
        }
        result.scanned += 1;
        if row.status == EntityStatus::Merged {
            if let Some(target) = &row.redirect_to {
                redirects.insert(row.base.id.clone(), target.id().clone());
            }
            continue;
        }
        if should_retire_entity_label(row) {
            retired.insert(row.base.id.clone());
            let mut after = row.clone();
            after.base.deleted_at = Some(chrono::Utc::now());
            after.base.updated_at = chrono::Utc::now();
            planning.stage(Some(row.clone()), after)?;
            result.retired += 1;
        } else {
            index_active_entity_for_deleted_survivor(row, &mut keys, &mut names);
        }
    }
    for row in deleted {
        if let Some(target) = deleted_entity_survivor(row, &keys, &names) {
            // A same-name row in another security partition grants no redirect.
            if let Some(target_row) = entities.get(&target) {
                if same_authorization(&row.base, &target_row.base)? {
                    redirects.insert(row.base.id.clone(), target);
                    continue;
                }
            }
        }
        retired.insert(row.base.id.clone());
    }
    // Follow actual merged chains, preserving all original subject requirements.
    // Missing/cyclic/cross-partition endpoints cannot authorize graph rewrites.
    let mut checked = HashMap::new();
    for (old, initial) in &redirects {
        let Some(original) = entities.get(old) else {
            continue;
        };
        let mut visited = HashSet::from([old.clone()]);
        let mut current = initial.clone();
        loop {
            if !visited.insert(current.clone()) {
                break;
            }
            let Some(target) = entities.get(&current) else {
                break;
            };
            if !same_authorization(&original.base, &target.base)? {
                break;
            }
            if let Some(next) = redirects.get(&current) {
                current = next.clone();
                continue;
            }
            if target.base.deleted_at.is_none() && !retired.contains(&current) {
                planning.observe(original)?;
                for id in &visited {
                    if let Some(input) = entities.get(id) {
                        planning.observe(input)?;
                    }
                }
                checked.insert(old.clone(), current);
            }
            break;
        }
    }
    if retired.is_empty() && checked.is_empty() {
        return Ok(result);
    }
    for before in rows(planning, &RelRepository::new(storage.clone()), space).await? {
        let mut after = before.clone();
        let mut repointed = false;
        for link in [&mut after.subject, &mut after.object] {
            if let Some(target) = checked.get(link.id()) {
                *link = elephant_types::Link::new(target.clone());
                repointed = true;
            }
        }
        if repointed {
            for link in [&after.subject, &after.object] {
                if !after.base.subjects.contains(link) {
                    after.base.subjects.push(link.clone());
                }
            }
            for link in [&after.subject, &after.object] {
                if let Some(entity) = entities.get(link.id()) {
                    if !contains_authorization(&after.base, &entity.base)? {
                        return Err(PipelineError::InvalidState(
                            "Redirect target exceeds relationship authority".into(),
                        ));
                    }
                    planning.observe(entity)?;
                }
            }
            after.base.updated_at = chrono::Utc::now();
            after.base.id = RecordId::new_v4(TableName::Rel);
            planning.observe(&before)?;
            planning.stage_inherited(None, after, std::slice::from_ref(&before.base))?;
            planning.delete_rel(space, &before.base.id).await?;
            result.relationships_repointed += 1;
        } else if retired.contains(after.subject.id()) || retired.contains(after.object.id()) {
            for link in [&before.subject, &before.object] {
                if retired.contains(link.id()) {
                    if let Some(entity) = entities.get(link.id()) {
                        if !contains_authorization(&before.base, &entity.base)? {
                            return Err(PipelineError::InvalidState(
                                "Retired entity exceeds relationship authority".into(),
                            ));
                        }
                        planning.observe(entity)?;
                    }
                }
            }
            planning.delete_rel(space, &before.base.id).await?;
            result.relationships_deleted += 1;
        }
    }
    for before in rows(planning, &AssertionRepository::new(storage.clone()), space).await? {
        if before.resolution_status == ResolutionStatus::Superseded {
            continue;
        }
        let mut after = before.clone();
        let references_retired = after
            .subject_entity
            .as_ref()
            .is_some_and(|id| retired.contains(id.id()))
            || after
                .object_entity
                .as_ref()
                .is_some_and(|id| retired.contains(id.id()));
        if references_retired {
            after.resolution_status = ResolutionStatus::Superseded;
            result.assertions_superseded += 1;
        } else {
            let mut repointed = false;
            for link in [&mut after.subject_entity, &mut after.object_entity]
                .into_iter()
                .flatten()
            {
                if let Some(target) = checked.get(link.id()) {
                    *link = elephant_types::Link::new(target.clone());
                    if !after.base.subjects.contains(link) {
                        after.base.subjects.push(link.clone());
                    }
                    repointed = true;
                }
            }
            if !repointed {
                continue;
            }
            result.assertions_repointed += 1;
        }
        for link in [
            &before.subject_entity,
            &before.object_entity,
            &after.subject_entity,
            &after.object_entity,
        ]
        .into_iter()
        .flatten()
        {
            if let Some(entity) = entities.get(link.id()) {
                if !contains_authorization(&after.base, &entity.base)? {
                    return Err(PipelineError::InvalidState(
                        "Entity hygiene decision exceeds assertion authority".into(),
                    ));
                }
                planning.observe(entity)?;
            }
        }
        after.base.updated_at = chrono::Utc::now();
        planning.stage_restricted(Some(before), after)?;
    }
    Ok(result)
}

/// Discover every graph predicate without manufacturing a public registry
/// authority. A new unique registry name inherits one actual deterministic
/// contributor; another partition cannot broaden that row's source envelope.
pub(super) async fn predicates(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &elephant_storage::StorageClient,
    space: &str,
) -> PipelineResult<PredicateSweepResult> {
    use elephant_storage::PredicateRegistryRepository;
    use elephant_types::records::{PredicateRegistry, SemanticsStatus};
    let mut known: HashSet<_> = rows(
        planning,
        &PredicateRegistryRepository::new(storage.clone()),
        space,
    )
    .await?
    .into_iter()
    .map(|row| row.predicate_id)
    .collect();
    let mut discovered = BTreeMap::new();
    for row in rows(planning, &AssertionRepository::new(storage.clone()), space).await? {
        let name = row.predicate_raw.trim().to_owned();
        if !name.is_empty() {
            discovered.entry(name).or_insert_with(|| row.record());
        }
    }
    for row in rows(planning, &RelRepository::new(storage.clone()), space).await? {
        let name = row.rel_type.trim().to_owned();
        if !name.is_empty() {
            discovered.entry(name).or_insert_with(|| row.record());
        }
    }
    let mut result = PredicateSweepResult {
        scanned: discovered.len(),
        ..Default::default()
    };
    for (name, input) in discovered {
        if !known.insert(name.clone()) {
            continue;
        }
        match &input {
            elephant_storage::canonical_commit::CanonicalRecord::Assertion(row) => {
                planning.observe(row.as_ref())?
            }
            elephant_storage::canonical_commit::CanonicalRecord::Rel(row) => {
                planning.observe(row.as_ref())?
            }
            _ => {
                return Err(PipelineError::InvalidState(
                    "Invalid predicate discovery contributor".into(),
                ))
            }
        }
        let mut base = input.base().clone();
        base.id = RecordId::new_v4(TableName::PredicateRegistry);
        base.created_at = chrono::Utc::now();
        base.updated_at = base.created_at;
        base.created_by = "maintenance_worker".into();
        base.deleted_at = None;
        let row = PredicateRegistry {
            base,
            predicate_id: name,
            description: None,
            status: "active".into(),
            semantics: None,
            semantics_status: SemanticsStatus::Default,
            first_seen_at: None,
            usage_count: 0,
            last_used_at: None,
        };
        planning.stage_inherited(None, row, std::slice::from_ref(input.base()))?;
        result.registered += 1;
    }
    Ok(result)
}
