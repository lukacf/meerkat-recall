//! Private factual lineage attached to the existing canonical receipt payload.
//! Public receipts never serialize these original graph/support snapshots.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use elephant_types::publication::{OperationState, PublicationFence, SourceHead, SourceVisibility};
use elephant_types::records::Rel;
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use surrealdb::sql::{Thing, Value};

use crate::canonical_commit::{
    CanonicalCommit, CanonicalCommitReceipt, CanonicalEffect, CanonicalRecord, SourceEffectSupport,
};
use crate::source_publication::{
    conflict, control_thing, encode_payload, integrity, read_control_payload_tx, stable_key,
    SourcePublicationRepository,
};
use crate::{EmbeddedMutation, StorageClient, StorageResult};

pub const LINEAGE_FIELD: &str = "_contact_lineage";

/// An actual transaction's relational effects and original factual read set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactLineage {
    pub version: u32,
    pub effects: Vec<CanonicalEffect>,
    pub dependencies: Vec<CanonicalRecord>,
    pub input_support: Vec<SourceEffectSupport>,
    /// Owner-created projection of an independently admitted surviving fact.
    /// No erased before-images or predecessor receipt references are retained.
    #[serde(default)]
    roots: Vec<ProjectedContactRoot>,
    /// Exact persisted bytes only for admission checks; never copied to receipts.
    #[serde(skip)]
    original_payloads: Vec<(String, String)>,
}

const fn relational(row: &CanonicalRecord) -> bool {
    matches!(
        row,
        CanonicalRecord::Rel(_)
            | CanonicalRecord::Entity(_)
            | CanonicalRecord::EntityIdentityKey(_)
    )
}
fn support_key(row: &SourceEffectSupport) -> String {
    stable_key(&[
        &row.space_id,
        &row.source_id.to_string(),
        &row.source_revision,
        &row.record_id.to_string(),
    ])
}

impl ContactLineage {
    pub(crate) async fn prepare(
        client: &StorageClient,
        request: &CanonicalCommit,
    ) -> StorageResult<Option<Self>> {
        if !request.plan.effects.iter().any(|effect| {
            effect
                .before
                .iter()
                .chain(effect.after.iter())
                .any(relational)
        }) {
            return Ok(None);
        }
        let effects: Vec<_> = request
            .plan
            .effects
            .iter()
            .filter(|effect| {
                effect
                    .before
                    .iter()
                    .chain(effect.after.iter())
                    .any(relational)
            })
            .cloned()
            .collect();
        let dependencies: Vec<_> = request
            .plan
            .dependencies
            .iter()
            .chain(
                request
                    .plan
                    .effects
                    .iter()
                    .filter_map(|effect| effect.before.as_ref()),
            )
            .filter(|row| relational(row))
            .cloned()
            .collect();
        let rels: BTreeSet<_> = dependencies
            .iter()
            .filter(|row| relational(row))
            .map(|row| row.base().id.clone())
            .collect();
        let rows: Vec<SourceEffectSupport> = SourcePublicationRepository::new(client.clone())
            .list_controls("source_effect_support", Some(&request.space_id))
            .await?;
        let input_support: Vec<_> = rows
            .into_iter()
            .filter(|support| rels.contains(&support.record_id))
            .collect();
        let repo = SourcePublicationRepository::new(client.clone());
        let mut original_payloads = Vec::new();
        for support in &input_support {
            let key = support_key(support);
            let raw = repo
                .read_control_payload("source_effect_support", &key, &request.space_id)
                .await?
                .ok_or_else(|| conflict("contact_lineage_input_support_changed"))?;
            let observed: SourceEffectSupport = crate::source_publication::decode_payload(&raw)?;
            if encode_payload(&observed)? != encode_payload(support)? {
                return Err(conflict("contact_lineage_input_support_changed"));
            }
            original_payloads.push((key, raw));
        }
        drop(repo);
        Ok(Some(Self {
            original_payloads,
            version: 1,
            effects,
            dependencies,
            input_support,
            roots: Vec::new(),
        }))
    }

    pub(crate) fn check_tx(&self, tx: &EmbeddedMutation<'_>, space: &str) -> StorageResult<()> {
        for (key, payload) in &self.original_payloads {
            if read_control_payload_tx(tx, "source_effect_support", key, space)?.as_ref()
                != Some(payload)
            {
                return Err(conflict("contact_lineage_input_support_changed"));
            }
        }
        Ok(())
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "Preserve the existing fallible guard-composition interface used by atomic SQL transaction callers."
    )]
    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        for (index, (key, payload)) in self.original_payloads.iter().enumerate() {
            bindings.insert(
                format!("contact_lineage_{index}_id"),
                control_thing("source_effect_support", key).into(),
            );
            bindings.insert(
                format!("contact_lineage_{index}_payload"),
                payload.clone().into(),
            );
            let _ = writeln!(sql,"IF (SELECT VALUE space_id FROM ONLY $contact_lineage_{index}_id) != $space OR (SELECT VALUE payload FROM ONLY $contact_lineage_{index}_id) != $contact_lineage_{index}_payload {{ THROW 'publication:contact_lineage_input_support_changed'; }};");
        }
        Ok(())
    }
}

/// Explicit JSON payload construction, not a flattened database proxy. Existing
/// typed public receipt readers ignore this private member on deserialization.
pub fn stored_receipt(
    receipt: &CanonicalCommitReceipt,
    lineage: Option<&ContactLineage>,
) -> StorageResult<serde_json::Value> {
    let mut payload =
        serde_json::to_value(receipt).map_err(|error| integrity(&error.to_string()))?;
    if let Some(lineage) = lineage {
        payload
            .as_object_mut()
            .ok_or_else(|| integrity("canonical receipt is not an object"))?
            .insert(
                LINEAGE_FIELD.into(),
                serde_json::to_value(lineage).map_err(|error| integrity(&error.to_string()))?,
            );
    }
    Ok(payload)
}

impl crate::canonical_commit::CanonicalRepository {
    /// Pure factual comparison shared by preparation and private receipt replay.
    ///
    /// This is not source authority: callers separately prove the original Rel,
    /// exact contributor union, observed entities and original transaction fences.
    pub fn contact_rel_fact_preserved(
        before: &elephant_types::records::Rel,
        after: &elephant_types::records::Rel,
        captured_entities: &[elephant_types::records::Entity],
    ) -> StorageResult<bool> {
        use elephant_types::records::base::SubjectsState;
        let original = &before.base;
        let proposed = &after.base;
        if original.space_id != proposed.space_id
            || original.deleted_at.is_some()
            || before.rel_type != after.rel_type
            || before.validity_window != after.validity_window
            || !after.confidence.is_finite()
            || !(0.0..=1.0).contains(&after.confidence)
            || original.security.policy_id != proposed.security.policy_id
            || original.security.level > proposed.security.level
            || original
                .security
                .labels
                .iter()
                .any(|value| !proposed.security.labels.contains(value))
            || original
                .security
                .handling
                .iter()
                .any(|value| !proposed.security.handling.contains(value))
            || original
                .subjects
                .iter()
                .any(|value| !proposed.subjects.contains(value))
            || matches!(
                (original.subjects_state, proposed.subjects_state),
                (
                    SubjectsState::Unknown,
                    SubjectsState::Partial | SubjectsState::Complete
                ) | (SubjectsState::Partial, SubjectsState::Complete)
            )
            || original
                .provenance
                .sources
                .iter()
                .any(|source| !proposed.provenance.sources.contains(source))
            || before
                .evidence
                .iter()
                .any(|evidence| !after.evidence.contains(evidence))
        {
            return Ok(false);
        }
        Ok(captured_redirect(
            &original.space_id,
            before.subject.id(),
            after.subject.id(),
            captured_entities,
            proposed,
        ) && captured_redirect(
            &original.space_id,
            before.object.id(),
            after.object.id(),
            captured_entities,
            proposed,
        ))
    }
}

fn captured_redirect(
    space: &str,
    from: &elephant_types::RecordId,
    to: &elephant_types::RecordId,
    entities: &[elephant_types::records::Entity],
    proposed: &elephant_types::records::BaseFields,
) -> bool {
    use elephant_types::records::entity::EntityStatus;
    if from == to {
        return true;
    }
    let mut current = from.clone();
    let mut seen = BTreeSet::new();
    for _ in 0..128 {
        if !seen.insert(current.clone()) {
            return false;
        }
        let matches: Vec<_> = entities
            .iter()
            .filter(|row| row.base.id == current)
            .collect();
        let Some(entity) = matches.first() else {
            return false;
        };
        if matches.iter().any(|row| *row != *entity)
            || entity.base.space_id != space
            || entity.base.deleted_at.is_some()
            || !contains_authorization(&entity.base, proposed)
        {
            return false;
        }
        if &current == to {
            return true;
        }
        if entity.status != EntityStatus::Merged {
            return false;
        }
        let Some(redirect) = &entity.redirect_to else {
            return false;
        };
        current = redirect.id().clone();
    }
    false
}

fn contains_authorization(
    original: &elephant_types::records::BaseFields,
    proposed: &elephant_types::records::BaseFields,
) -> bool {
    use elephant_types::records::base::SubjectsState;
    original.space_id == proposed.space_id
        && original.security.policy_id == proposed.security.policy_id
        && original.security.level <= proposed.security.level
        && original
            .security
            .labels
            .iter()
            .all(|value| proposed.security.labels.contains(value))
        && original
            .security
            .handling
            .iter()
            .all(|value| proposed.security.handling.contains(value))
        && original
            .subjects
            .iter()
            .all(|value| proposed.subjects.contains(value))
        && !matches!(
            (original.subjects_state, proposed.subjects_state),
            (
                SubjectsState::Unknown,
                SubjectsState::Partial | SubjectsState::Complete
            ) | (SubjectsState::Partial, SubjectsState::Complete)
        )
}

/// This private fact certificate is only emitted by a guarded inverse transaction
/// after following the actual support path to an independently completed contact
/// admission.
///
/// It is never an admission capability or a substitute contact plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProjectedContactRoot {
    publication: PublicationFence,
    contact_digest: String,
    record_id: RecordId,
    record_fingerprint: String,
}

#[derive(Debug, Clone, Default)]
pub struct OriginalControls {
    values: BTreeMap<(String, String), Option<String>>,
}

impl OriginalControls {
    pub(crate) async fn read<T: serde::de::DeserializeOwned>(
        &mut self,
        client: &StorageClient,
        space: &str,
        table: &str,
        key: &str,
    ) -> StorageResult<Option<T>> {
        // Every payload-table branch below has an actual ordinary writer event
        // on the shared reference revision (039/040). Unknown private tables
        // must not silently borrow a fence their writers never participate in.
        if !matches!(
            table,
            "source_operation"
                | "source_head"
                | "source_revision"
                | "source_effect_support"
                | "source_commit_receipt"
                | "source_decision_receipt"
                | "source_contact_intent"
                | "source_publication_completion"
                | "source_publication_manifest"
        ) {
            return Err(integrity("unsupported original control read fence"));
        }
        let raw = SourcePublicationRepository::new(client.clone())
            .read_control_payload(table, key, space)
            .await?;
        let identity = (table.to_owned(), key.to_owned());
        if let Some(previous) = self.values.get(&identity) {
            if previous != &raw {
                return Err(conflict("contact_lineage_original_input_changed"));
            }
        } else {
            self.values.insert(identity, raw.clone());
        }
        raw.as_deref()
            .map(crate::source_publication::decode_payload)
            .transpose()
    }

    pub(crate) fn check_tx(&self, tx: &EmbeddedMutation<'_>, space: &str) -> StorageResult<()> {
        for ((table, key), original) in &self.values {
            let exists_in_any_space = original.is_none()
                && tx
                    .scan_extension_rows(table)?
                    .iter()
                    .any(|(row_key, _)| row_key == &format!("{table}:{key}"));
            if exists_in_any_space || read_control_payload_tx(tx, table, key, space)? != *original {
                return Err(conflict("contact_lineage_original_input_changed"));
            }
        }
        Ok(())
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "Preserve the existing fallible guard-composition interface used by atomic SQL transaction callers."
    )]
    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
        space: &str,
        prefix: &str,
    ) {
        bindings.insert(format!("{prefix}_space"), space.into());
        for (index, ((table, key), original)) in self.values.iter().enumerate() {
            bindings.insert(
                format!("{prefix}_{index}_id"),
                control_thing(table, key).into(),
            );
            bindings.insert(
                format!("{prefix}_{index}_payload"),
                original.clone().map_or(Value::None, Value::from),
            );
            if original.is_some() {
                let _ = writeln!(sql,"IF (SELECT VALUE space_id FROM ONLY ${prefix}_{index}_id) != ${prefix}_space OR (SELECT VALUE payload FROM ONLY ${prefix}_{index}_id) != ${prefix}_{index}_payload {{ THROW 'publication:contact_lineage_original_input_changed'; }};");
                if matches!(table.as_str(), "source_operation" | "source_head") {
                    // Existing metadata controls are SCHEMALESS. The head's
                    // 043 event is metadata-only, including retained tombstones.
                    let _ = writeln!(sql,"UPDATE ${prefix}_{index}_id SET canonical_read_nonce=<string>rand::uuid();");
                } else {
                    // Never UPDATE historical payload just to lock its read:
                    // some controls are SCHEMAFULL, and restore can already have
                    // projected erased membership while cleanup inspects them.
                    // Every actual payload writer touches this same revision,
                    // including DELETE and both sides of a space move.
                    let _ = writeln!(sql,"fn::recall_reference_guard({{id:${prefix}_{index}_id,space_id:${prefix}_space}},false);");
                }
            } else {
                let _ = writeln!(sql,"IF (SELECT * FROM ONLY ${prefix}_{index}_id) != NONE {{ THROW 'publication:contact_lineage_original_input_changed'; }};");
            }
        }
    }
}

async fn current_publication(
    controls: &mut OriginalControls,
    client: &StorageClient,
    space: &str,
    source: &RecordId,
    revision: &str,
) -> StorageResult<bool> {
    let head: Option<SourceHead> = controls
        .read(
            client,
            space,
            "source_head",
            &crate::source_publication::head_key(space, source),
        )
        .await?;
    Ok(head.is_some_and(|head| {
        head.space_id == space
            && head.source_id == *source
            && head.lifecycle == SourceVisibility::Active
            && head.published_revision.as_deref() == Some(revision)
    }))
}

async fn root_operation(
    controls: &mut OriginalControls,
    client: &StorageClient,
    space: &str,
    publication: &PublicationFence,
) -> StorageResult<Option<String>> {
    let operation: Option<crate::source_publication::StoredOperation> = controls
        .read(client, space, "source_operation", &publication.operation_id)
        .await?;
    let Some(operation) = operation else {
        return Ok(None);
    };
    crate::source_contact_publication::validate_accepted_operation(space, publication, &operation)?;
    if operation.receipt.state != OperationState::Completed {
        return Ok(None);
    }
    Ok(operation.contact_digest.filter(|digest| !digest.is_empty()))
}

/// Walk only the actual pointer path, never a scan for an old matching revision.
///
/// During withdrawal, an invalidated intermediate may be traversed to a distinct
/// surviving independent root; it cannot itself become that root.
#[allow(
    clippy::too_many_lines,
    reason = "The bounded last-writer traversal shares its visited set, fallback projection, and original-control ledger across all proof branches; preserve their fail-closed ordering."
)]
#[allow(
    clippy::too_many_arguments,
    reason = "The traversal keeps publication identity, candidate support, withdrawn source, and mutable original-control ledger as distinct typed proof inputs."
)]
async fn trace_root(
    repository: &crate::canonical_commit::CanonicalRepository,
    space: &str,
    rel: &Rel,
    source_id: &RecordId,
    revision: &str,
    support: &SourceEffectSupport,
    withdrawn: Option<&RecordId>,
    controls: &mut OriginalControls,
) -> StorageResult<Option<(ProjectedContactRoot, Rel, SourceEffectSupport)>> {
    use crate::canonical_commit::{CanonicalRepository, PublishedSourceFence};
    let mut pending = vec![(rel.clone(), support.clone())];
    let mut visited = BTreeSet::new();
    // A projected root proves the current fact, but its support can point to a
    // later writer. During withdrawal, prefer the actual original input ledger
    // so an exact inverse can restore its surviving receipt pointer. Keep the
    // already verified projection if that older path no longer exists.
    let mut projected = None;
    if !current_publication(controls, &repository.client, space, source_id, revision).await? {
        return Ok(None);
    }
    for _ in 0..128 {
        let Some((current, support)) = pending.pop() else {
            return Ok(projected);
        };
        let fingerprint = CanonicalRecord::Rel(Box::new(current.clone())).fingerprint()?;
        if support.space_id != space
            || support.source_id != *source_id
            || support.source_revision != revision
            || support.record_id != current.base.id
            || support.record_fingerprint != fingerprint
            || current.base.space_id != space
        {
            return Ok(projected);
        }
        let Some(commit_id) = &support.last_commit_id else {
            continue;
        };
        if !visited.insert((
            commit_id.clone(),
            current.base.id.clone(),
            fingerprint.clone(),
        )) {
            continue;
        }
        let Some(payload): Option<serde_json::Value> = controls
            .read(
                &repository.client,
                space,
                "source_commit_receipt",
                &stable_key(&[space, commit_id]),
            )
            .await?
        else {
            continue;
        };
        let receipt: CanonicalCommitReceipt = serde_json::from_value(payload.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        let Some(lineage) = payload.get(LINEAGE_FIELD) else {
            continue;
        };
        let lineage: ContactLineage = serde_json::from_value(lineage.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        if receipt.space_id != space
            || receipt.commit_id != *commit_id
            || lineage.version != 1
            || !receipt.effect_ids.contains(&current.base.id)
            || lineage.effects.iter().any(|effect| {
                effect.before.iter().chain(effect.after.iter()).any(|row| {
                    row.base().space_id != space
                        || !receipt.effect_ids.contains(&row.base().id)
                        || !relational(row)
                })
            })
            || lineage
                .dependencies
                .iter()
                .any(|row| row.base().space_id != space || !relational(row))
        {
            return Ok(projected);
        }
        let matching: Vec<_> = lineage.effects.iter().filter(|effect| matches!(&effect.after,Some(CanonicalRecord::Rel(row)) if row.base.id == current.base.id)).collect();
        let [effect] = matching.as_slice() else {
            return Ok(projected);
        };
        if effect
            .after
            .as_ref()
            .map(CanonicalRecord::fingerprint)
            .transpose()?
            != Some(fingerprint.clone())
        {
            return Ok(projected);
        }
        if let Some(original) = &receipt.publication_authority {
            if withdrawn != Some(&original.source_id)
                && !current_publication(
                    controls,
                    &repository.client,
                    space,
                    &original.source_id,
                    &original.revision_id,
                )
                .await?
            {
                continue;
            }
        }
        let mut eligible = true;
        for fence in &receipt.maintenance_authority {
            if withdrawn == Some(fence.source_id()) {
                continue;
            }
            match fence {
                PublishedSourceFence::Head {
                    source_id,
                    published_revision,
                    ..
                } => {
                    eligible &= current_publication(
                        controls,
                        &repository.client,
                        space,
                        source_id,
                        published_revision,
                    )
                    .await?;
                }
                PublishedSourceFence::Legacy { source } => {
                    let head: Option<SourceHead> = controls
                        .read(
                            &repository.client,
                            space,
                            "source_head",
                            &crate::source_publication::head_key(space, &source.base.id),
                        )
                        .await?;
                    eligible &= head.is_none()
                        && matches!(CanonicalRecord::load(&repository.client,space,&source.base.id).await?,Some(CanonicalRecord::Source(current)) if current == *source);
                }
                PublishedSourceFence::PendingControl { .. } => eligible = false,
            }
        }
        if !eligible {
            continue;
        }
        if let Some(original) = receipt
            .publication_authority
            .as_ref()
            .filter(|original| original.source_id == *source_id)
        {
            // A new plain operation of the same revision cannot borrow an older
            // operation's contact seal through its recorded before-image.
            if original.revision_id != revision {
                return Ok(projected);
            }
            let Some(digest) =
                root_operation(controls, &repository.client, space, original).await?
            else {
                return Ok(projected);
            };
            // Capture the exact raw original plan and completion used by the
            // strict verifier; repeated reads below must agree with these bytes.
            let contact_key = crate::source_contact_publication::contact_key(space, original);
            let _: Option<serde_json::Value> = controls
                .read(
                    &repository.client,
                    space,
                    crate::source_contact_publication::CONTACT_TABLE,
                    &contact_key,
                )
                .await?;
            let completion_key = crate::source_publication_manifest::manifest_key(space, original);
            let _: Option<serde_json::Value> = controls
                .read(
                    &repository.client,
                    space,
                    crate::source_publication_manifest::COMPLETION_TABLE,
                    &completion_key,
                )
                .await?;
            if !repository
                .sealed_contact_publication_rel_support(space, original, &current, &receipt)
                .await?
            {
                return Ok(projected);
            }
            // A concurrent rewrite of a private control excluded from the coarse
            // epoch is also detected during preparation, before its TX guard.
            let _: Option<serde_json::Value> = controls
                .read(
                    &repository.client,
                    space,
                    crate::source_contact_publication::CONTACT_TABLE,
                    &contact_key,
                )
                .await?;
            let _: Option<serde_json::Value> = controls
                .read(
                    &repository.client,
                    space,
                    crate::source_publication_manifest::COMPLETION_TABLE,
                    &completion_key,
                )
                .await?;
            if root_operation(controls, &repository.client, space, original)
                .await?
                .as_deref()
                != Some(digest.as_str())
            {
                return Ok(projected);
            }
            return Ok(Some((
                ProjectedContactRoot {
                    publication: original.clone(),
                    contact_digest: digest,
                    record_id: current.base.id.clone(),
                    record_fingerprint: fingerprint,
                },
                current,
                support,
            )));
        }
        for root in &lineage.roots {
            if root.publication.source_id == *source_id
                && root.publication.revision_id == revision
                && root.record_id == current.base.id
                && root.record_fingerprint == fingerprint
                && root_operation(controls, &repository.client, space, &root.publication)
                    .await?
                    .as_deref()
                    == Some(root.contact_digest.as_str())
            {
                if withdrawn.is_none() {
                    return Ok(Some((root.clone(), current, support)));
                }
                if projected.is_none() {
                    projected = Some((root.clone(), current.clone(), support.clone()));
                }
            }
        }
        let mut entities = BTreeMap::new();
        for row in &lineage.dependencies {
            if let CanonicalRecord::Entity(entity) = row {
                if entities
                    .insert(entity.base.id.clone(), (**entity).clone())
                    .is_some_and(|before| before != **entity)
                {
                    return Ok(projected);
                }
            }
        }
        for effect in &lineage.effects {
            if let (Some(CanonicalRecord::Entity(before)), Some(CanonicalRecord::Entity(after))) =
                (&effect.before, &effect.after)
            {
                if before.base.id != after.base.id {
                    return Ok(projected);
                }
                entities.insert(after.base.id.clone(), (**after).clone());
            }
        }
        let entities: Vec<_> = entities.into_values().collect();
        if let Some(CanonicalRecord::Rel(before)) = &effect.before {
            if !CanonicalRepository::contact_rel_fact_preserved(before, &current, &entities)? {
                continue;
            }
        }
        let mut parents = Vec::new();
        for row in &lineage.dependencies {
            let CanonicalRecord::Rel(before) = row else {
                continue;
            };
            if !before.base.provenance.sources.contains(source_id)
                || !CanonicalRepository::contact_rel_fact_preserved(before, &current, &entities)?
            {
                continue;
            }
            let before_fingerprint = row.fingerprint()?;
            for parent in &lineage.input_support {
                if parent.space_id == space
                    && parent.source_id == *source_id
                    && parent.source_revision == revision
                    && parent.record_id == before.base.id
                    && parent.record_fingerprint == before_fingerprint
                {
                    parents.push(((**before).clone(), parent.clone()));
                }
            }
        }
        parents.sort_by(|left, right| left.0.base.id.cmp(&right.0.base.id));
        parents.dedup_by(|left, right| {
            left.0 == right.0 && left.1.last_commit_id == right.1.last_commit_id
        });
        pending.extend(parents);
    }
    Ok(projected)
}

pub async fn proves_rel(
    repository: &crate::canonical_commit::CanonicalRepository,
    space: &str,
    rel: &Rel,
    source_id: &RecordId,
    revision: &str,
    support: &SourceEffectSupport,
) -> StorageResult<bool> {
    Ok(trace_root(
        repository,
        space,
        rel,
        source_id,
        revision,
        support,
        None,
        &mut OriginalControls::default(),
    )
    .await?
    .is_some())
}

struct ContinuationReceipt {
    key: String,
    source_id: RecordId,
    payload: serde_json::Value,
}

/// All fields are storage-owned preparation. Its caller must check the original
/// canonical epoch in the SAME inverse transaction before these control guards.
pub struct PreparedSupportContinuations {
    space: String,
    controls: OriginalControls,
    receipts: Vec<ContinuationReceipt>,
    canonical_roots: Vec<ProjectedContactRoot>,
    pub(crate) support_updates: Vec<SourceEffectSupport>,
}

impl PreparedSupportContinuations {
    pub(crate) fn check_tx(&self, tx: &EmbeddedMutation<'_>) -> StorageResult<()> {
        self.controls.check_tx(tx, &self.space)
    }
    #[expect(
        clippy::unnecessary_wraps,
        reason = "Keep the fallible continuation-guard interface aligned with the lifecycle transaction's other prepared guards."
    )]
    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        self.controls
            .append_guard_sql(sql, bindings, &self.space, "continuation_guard");
        Ok(())
    }
    pub(crate) fn write_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        for receipt in &self.receipts {
            crate::source_publication::write_control_tx(
                tx,
                "source_commit_receipt",
                &receipt.key,
                &self.space,
                &receipt.source_id,
                &receipt.payload,
            )?;
        }
        Ok(())
    }
    pub(crate) fn append_write_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        for (index, receipt) in self.receipts.iter().enumerate() {
            bindings.insert(
                format!("continuation_write_{index}_id"),
                control_thing("source_commit_receipt", &receipt.key).into(),
            );
            bindings.insert(
                format!("continuation_write_{index}_value"),
                crate::source_publication::control_value(
                    &self.space,
                    &receipt.source_id,
                    &receipt.payload,
                )?,
            );
            let _ = writeln!(
                sql,
                "CREATE $continuation_write_{index}_id CONTENT $continuation_write_{index}_value;"
            );
        }
        Ok(())
    }
    pub(crate) fn native_rows(&self) -> StorageResult<Vec<(Thing, Value)>> {
        let mut rows = Vec::new();
        for receipt in &self.receipts {
            rows.push((
                control_thing("source_commit_receipt", &receipt.key),
                crate::source_publication::control_value(
                    &self.space,
                    &receipt.source_id,
                    &receipt.payload,
                )?,
            ));
        }
        for support in &self.support_updates {
            rows.push((control_thing("source_effect_support", &support_key(support)), crate::surreal::json_to_surreal(serde_json::json!({"space_id":support.space_id,"source_id":support.source_id.to_string(),"source_revision":support.source_revision,"record_id":support.record_id.to_string(),"independent":support.independent,"record_fingerprint":support.record_fingerprint,"payload":encode_payload(support)?}))?));
        }
        Ok(rows)
    }
}

fn same_structured_fact(before: &Rel, after: &Rel) -> bool {
    before.base.space_id == after.base.space_id
        && before.subject == after.subject
        && before.object == after.object
        && before.rel_type == after.rel_type
        && before.validity_window == after.validity_window
        && after.base.deleted_at.is_none()
        && after.confidence.is_finite()
        && (0.0..=1.0).contains(&after.confidence)
}

/// Prepares pointer continuations without writing. The lifecycle or canonical
/// retirement writer owns the inverse after-images and physical-closure checks.
#[allow(
    clippy::too_many_lines,
    reason = "Support continuations must be derived from one captured epoch, original receipts, and surviving roots before the final epoch check; retain that complete proof phase."
)]
#[allow(
    clippy::too_many_arguments,
    reason = "Preserve the existing preparation interface separating withdrawn source, effects, survivor supports, and invalidated receipt authority."
)]
pub async fn prepare_support_continuations(
    client: &StorageClient,
    space: &str,
    withdrawn: &RecordId,
    observed_epoch: &str,
    lifecycle_commit_id: &str,
    surviving_records: &[CanonicalRecord],
    original_supports: &[SourceEffectSupport],
    invalidated_receipt_keys: &[String],
) -> StorageResult<PreparedSupportContinuations> {
    use crate::canonical_commit::CanonicalRepository;
    let repository = CanonicalRepository::new(client.clone());
    if lifecycle_commit_id.is_empty() || repository.epoch(space).await? != observed_epoch {
        return Err(conflict("canonical_inputs_changed"));
    }
    let invalidated: BTreeSet<_> = invalidated_receipt_keys.iter().collect();
    let mut prepared = PreparedSupportContinuations {
        space: space.to_owned(),
        controls: OriginalControls::default(),
        receipts: Vec::new(),
        canonical_roots: Vec::new(),
        support_updates: Vec::new(),
    };
    let mut records = BTreeSet::new();
    for after in surviving_records {
        if after.base().space_id != space
            || after.base().deleted_at.is_some()
            || !records.insert(after.base().id.clone())
        {
            return Err(conflict("contact_continuation_survivor_changed"));
        }
        let fingerprint = after.fingerprint()?;
        for original in original_supports.iter().filter(|support| {
            support.record_id == after.base().id && support.source_id != *withdrawn
        }) {
            if original.space_id != space {
                return Err(conflict("contact_continuation_support_scope_changed"));
            }
            let stored: Option<SourceEffectSupport> = prepared
                .controls
                .read(
                    client,
                    space,
                    "source_effect_support",
                    &support_key(original),
                )
                .await?;
            if stored.as_ref() != Some(original) {
                return Err(conflict("contact_lineage_input_support_changed"));
            }
            let mut updated = original.clone();
            updated.record_fingerprint = fingerprint.clone();
            let pointer_deleted = original
                .last_commit_id
                .as_ref()
                .is_some_and(|commit| invalidated.contains(&stable_key(&[space, commit])));
            if !pointer_deleted && original.record_fingerprint == fingerprint {
                prepared.support_updates.push(updated);
                continue;
            }
            let current = CanonicalRecord::load(client, space, &original.record_id)
                .await?
                .ok_or_else(|| conflict("contact_continuation_original_missing"))?;
            if current.fingerprint()? != original.record_fingerprint {
                return Err(conflict("contact_continuation_original_changed"));
            }
            // Unknown or noncontact support remains useful to ordinary lifecycle
            // bookkeeping, but receives no invented factual eligibility.
            updated.last_commit_id = None;
            if let (CanonicalRecord::Rel(before), CanonicalRecord::Rel(after_rel)) =
                (&current, after)
            {
                if !after
                    .base()
                    .provenance
                    .sources
                    .contains(&original.source_id)
                {
                    prepared.support_updates.push(updated);
                    continue;
                }
                if let Some((mut root, original_fact, original_root_support)) = trace_root(
                    &repository,
                    space,
                    before,
                    &original.source_id,
                    &original.source_revision,
                    original,
                    Some(withdrawn),
                    &mut prepared.controls,
                )
                .await?
                {
                    if !same_structured_fact(before, after_rel)
                        && !same_structured_fact(&original_fact, after_rel)
                    {
                        return Err(conflict("contact_continuation_fact_changed"));
                    }
                    root.record_id = after.base().id.clone();
                    root.record_fingerprint = fingerprint.clone();
                    prepared.canonical_roots.push(root.clone());
                    if original_root_support.record_id == after.base().id
                        && original_root_support.record_fingerprint == fingerprint
                        && original_root_support
                            .last_commit_id
                            .as_ref()
                            .is_some_and(|commit| {
                                !invalidated.contains(&stable_key(&[space, commit]))
                            })
                    {
                        prepared.support_updates.push(original_root_support);
                        continue;
                    }
                    root.record_id = after.base().id.clone();
                    root.record_fingerprint = fingerprint.clone();
                    let commit_id = stable_key(&[
                        "contact-continuation-v1",
                        space,
                        lifecycle_commit_id,
                        &original.source_id.to_string(),
                        &after.base().id.to_string(),
                        &fingerprint,
                    ]);
                    let key = stable_key(&[space, &commit_id]);
                    let old: Option<serde_json::Value> = prepared
                        .controls
                        .read(client, space, "source_commit_receipt", &key)
                        .await?;
                    if old.is_some() || invalidated.contains(&key) {
                        return Err(conflict("contact_continuation_identity_exists"));
                    }
                    let lineage = ContactLineage {
                        version: 1,
                        effects: vec![CanonicalEffect {
                            before: None,
                            after: Some(after.clone()),
                        }],
                        dependencies: Vec::new(),
                        input_support: Vec::new(),
                        original_payloads: Vec::new(),
                        roots: vec![root],
                    };
                    let receipt = CanonicalCommitReceipt {
                        historical_import: None,
                        space_id: space.to_owned(),
                        commit_id: commit_id.clone(),
                        input_fingerprint: fingerprint.clone(),
                        plan_fingerprint: fingerprint.clone(),
                        replay_output: None,
                        source_ids: vec![original.source_id.clone()],
                        publication_authority: None,
                        maintenance_authority: Vec::new(),
                        source_id: None,
                        source_revision: None,
                        effect_ids: vec![after.base().id.clone()],
                        extension_ids: Vec::new(),
                        outbox_ids: Vec::new(),
                        committed_at: chrono::Utc::now(),
                    };
                    prepared.receipts.push(ContinuationReceipt {
                        key,
                        source_id: original.source_id.clone(),
                        payload: stored_receipt(&receipt, Some(&lineage))?,
                    });
                    updated.last_commit_id = Some(commit_id);
                }
            }
            prepared.support_updates.push(updated);
        }
    }
    // Detect preparation reads raced by a competing canonical writer. This does
    // not replace the mandatory original-epoch check in the caller transaction.
    if repository.epoch(space).await? != observed_epoch {
        return Err(conflict("canonical_inputs_changed"));
    }
    drop(repository);
    Ok(prepared)
}

impl PreparedSupportContinuations {
    /// Source replacement uses the actual new canonical receipt as the support
    /// pointer.
    ///
    /// Only verified private roots are transferred, never erased input
    /// snapshots; the prepared control guards are checked by that same writer.
    pub(crate) fn attach_canonical_roots(
        &self,
        lineage: &mut Option<ContactLineage>,
    ) -> StorageResult<()> {
        if !self.canonical_roots.is_empty() {
            let target = lineage
                .as_mut()
                .ok_or_else(|| integrity("canonical continuation missing relational effects"))?;
            target.roots.extend(self.canonical_roots.iter().cloned());
        }
        Ok(())
    }
}

/// Called only by commit_inner, never by read-only replacement preparation.
pub async fn prepare_replacement_continuations(
    client: &StorageClient,
    request: &mut CanonicalCommit,
    retired_controls: &[(String, String)],
) -> StorageResult<Option<PreparedSupportContinuations>> {
    let Some(publication) = request
        .publication
        .as_ref()
        .map(|context| context.source.clone())
    else {
        return Ok(None);
    };
    let invalidated: Vec<_> = retired_controls
        .iter()
        .filter(|(table, _)| table == "source_commit_receipt")
        .map(|(_, key)| {
            key.strip_prefix("source_commit_receipt:")
                .unwrap_or(key)
                .to_owned()
        })
        .collect();
    // A same-revision replacement can retain every original receipt while
    // removing the replacing source from a shared row. That inverse changes
    // the row fingerprint and still needs the surviving contributor's exact
    // contact root. Receipt deletion is not the condition for preparing it.
    let supports: Vec<SourceEffectSupport> = SourcePublicationRepository::new(client.clone())
        .list_controls("source_effect_support", Some(&request.space_id))
        .await?;
    // A byte-identical surviving row still needs an actual pointer transition
    // if retirement deletes its old receipt. Add an exact no-op effect so the
    // new canonical receipt and its original CAS both bind this row.
    let mut effect_ids: BTreeSet<_> = request
        .plan
        .effects
        .iter()
        .filter_map(|effect| effect.before.as_ref().or(effect.after.as_ref()))
        .map(|row| row.base().id.clone())
        .collect();
    for support in &supports {
        if support.source_id == publication.source_id
            || effect_ids.contains(&support.record_id)
            || !support.last_commit_id.as_ref().is_some_and(|commit| {
                invalidated.contains(&stable_key(&[&request.space_id, commit]))
            })
        {
            continue;
        }
        let row = CanonicalRecord::load(client, &request.space_id, &support.record_id)
            .await?
            .ok_or_else(|| conflict("contact_continuation_original_missing"))?;
        if row.base().deleted_at.is_some() {
            continue;
        }
        if row.fingerprint()? != support.record_fingerprint {
            return Err(conflict("contact_continuation_original_changed"));
        }
        effect_ids.insert(support.record_id.clone());
        request.plan.effects.push(CanonicalEffect {
            before: Some(row.clone()),
            after: Some(row),
        });
    }
    let survivors: Vec<_> = request
        .plan
        .effects
        .iter()
        .filter_map(|effect| effect.after.as_ref())
        .filter(|row| row.base().deleted_at.is_none())
        .cloned()
        .collect();
    prepare_support_continuations(
        client,
        &request.space_id,
        &publication.source_id,
        &request.plan.observed_epoch,
        &request.commit_id,
        &survivors,
        &supports,
        &invalidated,
    )
    .await
    .map(Some)
}

fn original_lineage_effect<'a>(
    lineage: &'a ContactLineage,
    current: &CanonicalRecord,
    fingerprint: &str,
) -> StorageResult<Option<&'a CanonicalEffect>> {
    let effects: Vec<_> = lineage
        .effects
        .iter()
        .filter(|effect| {
            effect
                .after
                .as_ref()
                .is_some_and(|row| row.base().id == current.base().id)
        })
        .collect();
    let [effect] = effects.as_slice() else {
        return Ok(None);
    };
    if effect
        .after
        .as_ref()
        .map(CanonicalRecord::fingerprint)
        .transpose()?
        .as_deref()
        != Some(fingerprint)
    {
        return Ok(None);
    }
    Ok(Some(*effect))
}

/// Checks original admission identity through the actual last-writer path for
/// inverse composition.
///
/// Factual projection remains separately sealed and the
/// pure composer checks current types/endpoints; this grants no selector remap.
pub async fn proves_original_contact_publication(
    client: &StorageClient,
    space: &str,
    original: &PublicationFence,
    current: &CanonicalRecord,
    support: &SourceEffectSupport,
    controls: &mut OriginalControls,
) -> StorageResult<bool> {
    let mut pending = vec![(current.clone(), support.clone())];
    let mut visited = BTreeSet::new();
    for _ in 0..128 {
        let Some((current, support)) = pending.pop() else {
            return Ok(false);
        };
        let fingerprint = current.fingerprint()?;
        if current.base().space_id != space
            || support.space_id != space
            || support.source_id != original.source_id
            || support.source_revision != original.revision_id
            || support.record_id != current.base().id
            || support.record_fingerprint != fingerprint
        {
            return Ok(false);
        }
        let Some(writer) = &support.last_commit_id else {
            continue;
        };
        if !visited.insert((
            writer.clone(),
            current.base().id.clone(),
            fingerprint.clone(),
        )) {
            continue;
        }
        let payload: Option<serde_json::Value> = controls
            .read(
                client,
                space,
                "source_commit_receipt",
                &stable_key(&[space, writer]),
            )
            .await?;
        let Some(payload) = payload else { continue };
        let receipt: CanonicalCommitReceipt = serde_json::from_value(payload.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        if receipt.space_id != space
            || receipt.commit_id != *writer
            || !receipt.effect_ids.contains(&current.base().id)
        {
            return Ok(false);
        }
        let Some(private) = payload.get(LINEAGE_FIELD) else {
            continue;
        };
        let lineage: ContactLineage = serde_json::from_value(private.clone())
            .map_err(|error| integrity(&error.to_string()))?;
        if lineage.version != 1 {
            return Ok(false);
        }
        let Some(effect) = original_lineage_effect(&lineage, &current, &fingerprint)? else {
            return Ok(false);
        };
        if let Some(publication) = receipt
            .publication_authority
            .as_ref()
            .filter(|publication| publication.source_id == original.source_id)
        {
            return Ok(publication == original);
        }
        for root in &lineage.roots {
            if root.publication == *original
                && root.record_id == current.base().id
                && root.record_fingerprint == fingerprint
                && root_operation(controls, client, space, original)
                    .await?
                    .as_deref()
                    == Some(root.contact_digest.as_str())
            {
                return Ok(true);
            }
        }
        // Same physical identity only. A factual redirect/dedupe needs its own
        // actual lineage transform; this helper never invents one from a lookup.
        for before in lineage
            .dependencies
            .iter()
            .chain(effect.before.iter())
            .filter(|row| row.base().id == current.base().id && row.base().space_id == space)
        {
            let before_fingerprint = before.fingerprint()?;
            for input in lineage.input_support.iter().filter(|input| {
                input.space_id == space
                    && input.source_id == original.source_id
                    && input.source_revision == original.revision_id
                    && input.record_id == before.base().id
                    && input.record_fingerprint == before_fingerprint
            }) {
                pending.push((before.clone(), input.clone()));
            }
        }
    }
    Ok(false)
}

#[cfg(test)]
#[path = "source_contact_lineage_tests.rs"]
mod tests;
