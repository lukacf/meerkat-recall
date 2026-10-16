//! Exact generated native controls and durable transaction inventory for deltas.
use super::*;
use crate::historical_native_effects::{HistoricalNativeEffectBinding, PreparedNativeEffects};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use surrealdb::sql::{Thing, Value};

pub struct Inventory {
    pub effects: PreparedNativeEffects,
    pub binding: HistoricalNativeEffectBinding,
    pub identities: Vec<(String, Thing)>,
}

pub async fn begin(
    authority: &PreparedHistoricalCatchupAuthority,
    effects: &PreparedHistoricalEffects,
    operational: &[crate::historical_delta_operations::OperationalEffect],
    receipt: &CanonicalCommitReceipt,
    source: &elephant_types::RecordId,
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
) -> StorageResult<Inventory> {
    let client = authority.client()?;
    let space = &receipt.space_id;
    let config = crate::SpaceConfigRepository::new(client.clone())
        .get_by_space_id(space)
        .await?
        .ok_or_else(changed)?;
    let config_id = crate::surreal::thing_from_record_id(&config.base.id);
    let ProjectedInventory {
        identities,
        vector_writes,
        fences,
    } = projected_identities(effects, operational, receipt, &config_id)?;
    let (visibility, guards) = generated_write_counts(effects, operational, vector_writes);
    append_generated_control_guards(
        &config,
        (config_id, space),
        &fences,
        (visibility, guards),
        sql,
        bindings,
    )?;
    let (authority_binding, ordinal) = authority.effect_manifest_binding();
    let binding = HistoricalNativeEffectBinding::new(
        authority_binding,
        "batch",
        ordinal,
        receipt,
        &identities,
    )?;
    let manifest = crate::historical_native_effects::begin(
        sql,
        bindings,
        source,
        binding.clone(),
        receipt,
        identities.clone(),
    )?;
    Ok(Inventory {
        effects: manifest,
        binding,
        identities,
    })
}

struct ProjectedInventory {
    identities: Vec<(String, Thing)>,
    vector_writes: u64,
    fences: BTreeSet<String>,
}

fn projected_identities(
    effects: &PreparedHistoricalEffects,
    operational: &[crate::historical_delta_operations::OperationalEffect],
    receipt: &CanonicalCommitReceipt,
    config_id: &Thing,
) -> StorageResult<ProjectedInventory> {
    let space = &receipt.space_id;
    let mut identities = effects
        .projected_rows
        .iter()
        .map(|row| {
            (
                row.id.table().as_str().to_owned(),
                crate::surreal::thing_from_record_id(&row.id),
            )
        })
        .collect::<Vec<_>>();
    let mut vector_writes = 0_u64;
    for row in &effects.projected_embeddings {
        let before = row
            .before
            .as_ref()
            .map(crate::historical_delta_vectors::native)
            .transpose()?;
        let after = row
            .after
            .as_ref()
            .map(crate::historical_delta_vectors::native)
            .transpose()?;
        let moved = before.as_ref().map(|(_, id, _)| id) != after.as_ref().map(|(_, id, _)| id);
        if before.is_some() && (after.is_none() || moved) {
            vector_writes += 1;
        }
        if after.is_some() {
            vector_writes += 1;
        }
        for (_, id, _) in before.into_iter().chain(after) {
            identities.push((id.tb.clone(), id));
        }
    }
    identities.extend(
        effects
            .controls
            .iter()
            .map(|row| (row.table.into(), control_thing(row.table, &row.key))),
    );
    identities.extend(
        operational
            .iter()
            .map(|row| (row.table.into(), control_thing(row.table, &row.key))),
    );
    identities.push((
        "source_commit_receipt".into(),
        control_thing(
            "source_commit_receipt",
            &stable_key(&[space, &receipt.commit_id]),
        ),
    ));
    identities.push((
        "canonical_epoch".into(),
        control_thing("canonical_epoch", space),
    ));
    identities.push(("space_config".into(), config_id.clone()));
    let mut fences = BTreeSet::new();
    for (table, id) in &identities {
        if matches!(
            table.as_str(),
            "doc" | "source_head" | "source_publication_manifest"
        ) {
            let hash = hex::encode(Sha256::digest(id.to_string().as_bytes()));
            fences.insert(format!("{table}:{}", &hash[..3]));
        }
    }
    for key in &fences {
        identities.push((
            "native_read_fence".into(),
            control_thing("native_read_fence", key),
        ));
    }
    identities.sort_by(|a, b| a.1.cmp(&b.1));
    identities.dedup();
    Ok(ProjectedInventory {
        identities,
        vector_writes,
        fences,
    })
}

fn generated_write_counts(
    effects: &PreparedHistoricalEffects,
    operational: &[crate::historical_delta_operations::OperationalEffect],
    vector_writes: u64,
) -> (usize, u64) {
    let visibility_rows = effects
        .projected_rows
        .iter()
        .filter(|row| {
            matches!(
                row.id.table(),
                elephant_types::TableName::Source
                    | elephant_types::TableName::Doc
                    | elephant_types::TableName::DocRevision
                    | elephant_types::TableName::DocNode
                    | elephant_types::TableName::Entity
            )
        })
        .count();
    // Source effects contain only changed controls; operation has no visibility
    // event, while head/completion changes invalidate current coverage.
    let visibility_controls = effects
        .controls
        .iter()
        .filter(|row| {
            matches!(row.table, "source_head" | "source_publication_completion")
                && row.after.as_ref() != Some(&row.before)
        })
        .count();
    let visibility = visibility_rows + visibility_controls;
    // Original schema guards every typed row, each dynamic vector write, changed
    // head/completion, work origin, config invalidation and the primary receipt.
    let guards = effects.projected_rows.len() as u64
        + vector_writes
        + effects
            .controls
            .iter()
            .filter(|row| row.table != "source_operation")
            .count() as u64
        + operational
            .iter()
            .filter(|row| matches!(row.table, "work_producer_origin" | "outbox_checkpoint"))
            .count() as u64
        + visibility as u64
        + 1;
    (visibility, guards)
}

fn append_generated_control_guards(
    config: &elephant_types::records::SpaceConfig,
    identity: (Thing, &str),
    fences: &BTreeSet<String>,
    counts: (usize, u64),
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
) -> StorageResult<()> {
    let (config_id, space) = identity;
    let (visibility, guards) = counts;
    bindings.insert("hdi_config_id".into(), config_id.into());
    bindings.insert(
        "hdi_config_expected".into(),
        crate::canonical_commit::native_expected_fields(crate::surreal::space_config_to_value(
            config,
        )?),
    );
    bindings.insert(
        "hdi_guard_id".into(),
        control_thing("source_erasure_guard", space).into(),
    );
    bindings.insert(
        "hdi_visibility".into(),
        i64::try_from(visibility).map_err(|_| changed())?.into(),
    );
    bindings.insert(
        "hdi_guard_writes".into(),
        i64::try_from(guards).map_err(|_| changed())?.into(),
    );
    sql.push_str("IF !recall::original_row_matches((SELECT * FROM ONLY $hdi_config_id),$space,$hdi_config_expected) { THROW 'historical_delta_config_changed'; }; LET $hdi_config_before=SELECT * FROM ONLY $hdi_config_id; LET $hdi_guard_before=SELECT * FROM ONLY $hdi_guard_id; IF $hdi_guard_before=NONE OR $hdi_guard_before.space_id!=$space OR $hdi_guard_before.journal_id!=$__recall_journal_identity OR !type::is::int($hdi_guard_before.revision) OR $hdi_guard_before.revision<0 { THROW 'historical_delta_guard_changed'; };\n");
    for (index, key) in fences.iter().enumerate() {
        bindings.insert(
            format!("hdi_fence_{index}"),
            control_thing("native_read_fence", key).into(),
        );
        writeln!(sql,"IF (SELECT * FROM ONLY $hdi_fence_{index})!=NONE AND !fn::recall_physical_read_fence_valid((SELECT * FROM ONLY $hdi_fence_{index})) {{ THROW 'historical_delta_fence_shape'; }};").map_err(|_|changed())?;
    }
    bindings.insert(
        "hdi_fences".into(),
        Value::Array(
            fences
                .iter()
                .map(|key| Value::Thing(control_thing("native_read_fence", key)))
                .collect::<Vec<_>>()
                .into(),
        ),
    );
    Ok(())
}

pub(super) fn finish(sql: &mut String, inventory: &Inventory) -> StorageResult<()> {
    sql.push_str("LET $hdi_config_after=SELECT * FROM ONLY $hdi_config_id; IF $hdi_visibility=0 { IF $hdi_config_after!=$hdi_config_before { THROW 'historical_delta_unexpected_config_effect'; }; } ELSE { IF object::keys($hdi_config_after)!=object::keys($hdi_config_before) OR object::keys($hdi_config_after.processing)!=object::keys($hdi_config_before.processing) OR $hdi_config_after.processing.index_generation!=$hdi_config_before.processing.index_generation+$hdi_visibility OR !type::is::datetime($hdi_config_after.updated_at) { THROW 'historical_delta_config_effect'; }; FOR $key IN object::keys($hdi_config_before) { IF $key NOT IN ['processing','updated_at'] AND $hdi_config_after[$key]!=$hdi_config_before[$key] { THROW 'historical_delta_config_payload'; }; }; FOR $key IN object::keys($hdi_config_before.processing) { IF $key!='index_generation' AND $hdi_config_after.processing[$key]!=$hdi_config_before.processing[$key] { THROW 'historical_delta_processing_changed'; }; }; }; LET $hdi_guard_expected=object::from_entries(array::concat(array::filter(object::entries($hdi_guard_before),|$entry|$entry[0]!='revision'),[['revision',$hdi_guard_before.revision+$hdi_guard_writes]])); IF (SELECT * FROM ONLY $hdi_guard_id)!=$hdi_guard_expected { THROW 'historical_delta_guard_effect'; }; FOR $fence IN $hdi_fences { IF !fn::recall_physical_read_fence_valid((SELECT * FROM ONLY $fence)) { THROW 'historical_delta_fence_effect'; }; };\n");
    crate::historical_native_effects::finish(sql, &inventory.effects)
}
