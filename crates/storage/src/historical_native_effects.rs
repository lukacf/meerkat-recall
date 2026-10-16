//! Durable exact native effects for crash-safe historical inventory replay.
//!
//! The effect receipt excludes its own hash; recovery derives that hash only
//! after matching the entire immutable wrapper and its bound primary receipt.
use crate::canonical_commit::CanonicalCommitReceipt;
use crate::source_publication::{control_thing, control_value, stable_key};
use crate::{StorageClient, StorageError, StorageResult};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use surrealdb::sql::{Thing, Value};

const MAX_EFFECTS: usize = 65_536;
const MAX_BYTES: usize = 16 * 1024 * 1024;
/// Non-serializable inventory authority produced only after exact durable
/// wrapper, primary receipt and independently planned identity verification.
#[derive(Debug)]
pub struct VerifiedHistoricalNativeEffects {
    pub(crate) binding: HistoricalNativeEffectBinding,
    pub(crate) effects: Vec<HistoricalNativeEffect>,
}
impl VerifiedHistoricalNativeEffects {
    /// Read the complete verified physical effects without manufacturing ledger authority.
    #[must_use]
    pub fn effects(&self) -> &[HistoricalNativeEffect] {
        &self.effects
    }
}
pub(crate) async fn recover_verified(
    client: &StorageClient,
    source: &RecordId,
    binding: &HistoricalNativeEffectBinding,
    primary: &CanonicalCommitReceipt,
    inventory: &[(String, Thing)],
) -> StorageResult<VerifiedHistoricalNativeEffects> {
    Ok(VerifiedHistoricalNativeEffects {
        effects: recover(client, source, binding, primary, inventory).await?,
        binding: binding.clone(),
    })
}

/// One exact physical row transition recovered from a durable native manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalNativeEffect {
    /// Physical native table containing the affected row.
    pub table: String,
    /// Exact native record identity in its string representation.
    pub id: String,
    /// SHA-256 of the original native row string, or absent for creation.
    pub before_sha256: Option<String>,
    /// SHA-256 of the resulting native row string, or absent for deletion.
    pub after_sha256: Option<String>,
}
/// Immutable authority, ordering and primary receipt binding for native effects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalNativeEffectBinding {
    /// Version of the durable native effect manifest.
    pub version: u32,
    /// Digest of the exclusive catch-up input authority.
    pub authority_binding_sha256: String,
    /// Ordered receipt phase: `batch`, one `archive`, or `regeneration`.
    pub phase: String,
    /// Zero-based position within the declared phase.
    pub ordinal: u64,
    /// Exact canonical commit ID bound to these effects.
    pub primary_commit_id: String,
    /// SHA-256 of the complete typed primary commit receipt.
    pub primary_receipt_sha256: String,
    /// Digest of the independently derived physical table and record identities.
    pub inventory_sha256: String,
    /// Number of independently derived physical identities.
    pub inventory_count: u64,
}
#[derive(Debug)]
pub(crate) struct PreparedNativeEffects {
    inventory: Vec<(String, Thing)>,
}
fn refused() -> StorageError {
    StorageError::Conflict("historical native effect manifest differs".into())
}
fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn receipt_sha(primary: &CanonicalCommitReceipt) -> StorageResult<String> {
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(primary).map_err(crate::durability_io::io_error)?,
    )))
}
impl HistoricalNativeEffectBinding {
    pub(crate) fn new(
        authority: &str,
        phase: &str,
        ordinal: u64,
        primary: &CanonicalCommitReceipt,
        inventory: &[(String, Thing)],
    ) -> StorageResult<Self> {
        let inventory = normalized_inventory(primary, inventory.to_vec())?;
        let value = Self {
            version: 1,
            authority_binding_sha256: authority.into(),
            phase: phase.into(),
            ordinal,
            primary_commit_id: primary.commit_id.clone(),
            primary_receipt_sha256: receipt_sha(primary)?,
            inventory_sha256: inventory_sha(&inventory)?,
            inventory_count: inventory.len() as u64,
        };
        value.validate(primary)?;
        Ok(value)
    }
    fn validate(&self, primary: &CanonicalCommitReceipt) -> StorageResult<()> {
        if self.version != 1
            || !digest(&self.authority_binding_sha256)
            || !matches!(self.phase.as_str(), "batch" | "archive" | "regeneration")
            || (self.phase == "archive" && self.ordinal != 0)
            || self.primary_commit_id != primary.commit_id
            || self.primary_receipt_sha256 != receipt_sha(primary)?
            || !digest(&self.inventory_sha256)
            || self.inventory_count > MAX_EFFECTS as u64
        {
            return Err(refused());
        }
        Ok(())
    }
    fn commit_id(&self) -> String {
        stable_key(&[
            "historical-native-effects-v1",
            &self.authority_binding_sha256,
            &self.phase,
            &self.ordinal.to_string(),
        ])
    }
}
fn normalized_inventory(
    primary: &CanonicalCommitReceipt,
    mut inventory: Vec<(String, Thing)>,
) -> StorageResult<Vec<(String, Thing)>> {
    inventory.push((
        "source_erasure_guard".into(),
        control_thing("source_erasure_guard", &primary.space_id),
    ));
    inventory.sort_by(|a, b| a.1.cmp(&b.1));
    inventory.dedup();
    if inventory.len() > MAX_EFFECTS || inventory.iter().any(|(table, id)| table != &id.tb) {
        return Err(refused());
    }
    let mut previous: Option<&Thing> = None;
    let mut bound = 2_usize;
    for (table, id) in &inventory {
        if previous.is_some_and(|before| before.cmp(id).is_eq()) {
            return Err(refused());
        }
        previous = Some(id);
        // Conservative four UTF-8 bytes per displayed character, plus both
        // 64-byte hashes and field punctuation; bound before SQL construction.
        bound = bound
            .checked_add(
                table
                    .len()
                    .saturating_add(id.to_string().len())
                    .saturating_mul(4)
                    .saturating_add(256),
            )
            .ok_or_else(refused)?;
        if bound > MAX_BYTES {
            return Err(refused());
        }
    }
    Ok(inventory)
}
fn inventory_sha(inventory: &[(String, Thing)]) -> StorageResult<String> {
    let identities = inventory
        .iter()
        .map(|(t, id)| (t, id.to_string()))
        .collect::<Vec<_>>();
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&identities).map_err(crate::durability_io::io_error)?,
    )))
}

fn wrapper(
    binding: &HistoricalNativeEffectBinding,
    primary: &CanonicalCommitReceipt,
    source: &RecordId,
) -> StorageResult<(Thing, Value)> {
    binding.validate(primary)?;
    if !primary.source_ids.contains(source) {
        return Err(refused());
    }
    let receipt = CanonicalCommitReceipt {
        historical_import: None,
        space_id: primary.space_id.clone(),
        commit_id: binding.commit_id(),
        input_fingerprint: binding.authority_binding_sha256.clone(),
        plan_fingerprint: binding.primary_receipt_sha256.clone(),
        replay_output: Some(serde_json::to_value(binding).map_err(crate::durability_io::io_error)?),
        source_ids: primary.source_ids.clone(),
        publication_authority: None,
        maintenance_authority: Vec::new(),
        source_id: Some(source.clone()),
        source_revision: None,
        effect_ids: Vec::new(),
        extension_ids: Vec::new(),
        outbox_ids: Vec::new(),
        committed_at: primary.committed_at,
    };
    let id = control_thing(
        "source_commit_receipt",
        &stable_key(&[&primary.space_id, &receipt.commit_id]),
    );
    let Value::Object(mut fields) = control_value(&primary.space_id, source, &receipt)? else {
        return Err(refused());
    };
    fields.insert("id".into(), id.clone().into());
    Ok((
        id,
        crate::surreal::stored_native_value(Value::Object(fields)),
    ))
}
/// Capture only the caller's planned physical identities before its first write.
/// The caller still validates every canonical and generated after-image.
pub(crate) fn begin(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    source: &RecordId,
    binding: HistoricalNativeEffectBinding,
    primary: &CanonicalCommitReceipt,
    mut inventory: Vec<(String, Thing)>,
) -> StorageResult<PreparedNativeEffects> {
    let (id, wrapper) = wrapper(&binding, primary, source)?;
    inventory = normalized_inventory(primary, inventory)?;
    let HistoricalNativeEffectBinding {
        inventory_sha256,
        inventory_count,
        ..
    } = binding;
    if inventory_sha256 != inventory_sha(&inventory)?
        || inventory_count != inventory.len() as u64
        || inventory.iter().any(|(_, item)| item == &id)
    {
        return Err(refused());
    }
    bindings.insert("hne_receipt_id".into(), id.into());
    bindings.insert("hne_receipt_static".into(), wrapper);
    bindings.insert(
        "hne_guard_id".into(),
        control_thing("source_erasure_guard", &primary.space_id).into(),
    );
    bindings.insert(
        "hne_epoch_id".into(),
        control_thing("canonical_epoch", &primary.space_id).into(),
    );
    sql.push_str("IF (SELECT * FROM ONLY $hne_receipt_id)!=NONE { THROW 'historical_effect_receipt_exists'; };\n");
    for (index, (_, id)) in inventory.iter().enumerate() {
        bindings.insert(format!("hne_{index}_id"), id.clone().into());
        writeln!(sql,"LET $hne_{index}_before=(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $hne_{index}_id);").map_err(|_|refused())?;
    }
    Ok(PreparedNativeEffects { inventory })
}
/// Append the last receipt write, its exact guard effect and the response. The
/// caller appends COMMIT only after this helper; no second transaction is used.
pub(crate) fn finish(sql: &mut String, prepared: &PreparedNativeEffects) -> StorageResult<()> {
    sql.push_str("LET $hne_epoch_before=SELECT * FROM ONLY $hne_epoch_id; LET $hne_guard_before=SELECT * FROM ONLY $hne_guard_id; IF $hne_guard_before=NONE OR !type::is::int($hne_guard_before.revision) OR $hne_guard_before.revision<0 OR $hne_guard_before.revision>=9223372036854775807 { THROW 'historical_effect_guard_invalid'; }; LET $hne_guard_expected=object::from_entries(array::concat(array::filter(object::entries($hne_guard_before),|$entry|$entry[0]!='revision'),[['revision',$hne_guard_before.revision+1]])); LET $hne_manifest=[");
    for (index, (table, _)) in prepared.inventory.iter().enumerate() {
        if index > 0 {
            sql.push(',');
        }
        let after = if table == "source_erasure_guard" {
            "crypto::sha256(type::string($hne_guard_expected))".to_owned()
        } else {
            format!("(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $hne_{index}_id)")
        };
        write!(sql,"{{table:meta::tb($hne_{index}_id),id:type::string($hne_{index}_id),before_sha256:IF $hne_{index}_before=NONE THEN NULL ELSE $hne_{index}_before END,after_sha256:IF {after}=NONE THEN NULL ELSE {after} END}}").map_err(|_|refused())?;
    }
    sql.push_str("]; IF string::len(type::string($hne_manifest))*4>16777216 { THROW 'historical_effect_manifest_byte_bound'; }; LET $hne_record=object::from_entries(array::concat(object::entries($hne_receipt_static),[['historical_native_effects',$hne_manifest]])); CREATE $hne_receipt_id CONTENT $hne_record; IF (SELECT * FROM ONLY $hne_receipt_id)!=$hne_record OR (SELECT * FROM ONLY $hne_guard_id)!=$hne_guard_expected OR (SELECT * FROM ONLY $hne_epoch_id)!=$hne_epoch_before { THROW 'historical_effect_receipt_after_changed'; }; RETURN array::concat($hne_manifest,[{table:'source_commit_receipt',id:type::string($hne_receipt_id),before_sha256:NULL,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $hne_receipt_id)}]);\n");
    Ok(())
}
fn strand(value: Option<Value>) -> StorageResult<String> {
    if let Some(Value::Strand(v)) = value {
        Ok(v.0)
    } else {
        Err(refused())
    }
}
fn optional_digest(value: Option<Value>) -> StorageResult<Option<String>> {
    match value {
        Some(Value::Null) => Ok(None),
        Some(Value::Strand(v)) if digest(&v.0) => Ok(Some(v.0)),
        _ => Err(refused()),
    }
}
fn decode(value: Value) -> StorageResult<Vec<HistoricalNativeEffect>> {
    let Value::Array(values) = value else {
        return Err(refused());
    };
    if values.len() > MAX_EFFECTS || values.to_string().len() > MAX_BYTES {
        return Err(refused());
    }
    let mut result = Vec::with_capacity(values.len());
    let mut seen = BTreeSet::new();
    for value in values.0 {
        let Value::Object(mut row) = value else {
            return Err(refused());
        };
        let table = strand(row.remove("table"))?;
        let id = strand(row.remove("id"))?;
        let before_sha256 = optional_digest(row.remove("before_sha256"))?;
        let after_sha256 = optional_digest(row.remove("after_sha256"))?;
        if !row.is_empty() || !id.starts_with(&format!("{table}:")) || !seen.insert(id.clone()) {
            return Err(refused());
        }
        result.push(HistoricalNativeEffect {
            table,
            id,
            before_sha256,
            after_sha256,
        });
    }
    Ok(result)
}
fn decode_projection(raw: Value) -> StorageResult<(Value, String)> {
    let Value::Object(mut outer) = raw else {
        return Err(refused());
    };
    let record = outer.remove("record").ok_or_else(refused)?;
    let sha = strand(outer.remove("sha256"))?;
    if !outer.is_empty() || !matches!(record, Value::Object(_)) || !digest(&sha) {
        return Err(refused());
    }
    Ok((record, sha))
}

/// Verify the complete immutable manifest wrapper. This is a private transaction
/// recovery primitive; it never accepts arbitrary live rows as expected output.
pub(crate) async fn recover(
    client: &StorageClient,
    source: &RecordId,
    binding: &HistoricalNativeEffectBinding,
    primary: &CanonicalCommitReceipt,
    inventory: &[(String, Thing)],
) -> StorageResult<Vec<HistoricalNativeEffect>> {
    let inventory = normalized_inventory(primary, inventory.to_vec())?;
    if binding.inventory_sha256 != inventory_sha(&inventory)?
        || binding.inventory_count != inventory.len() as u64
    {
        return Err(refused());
    }
    let (id, expected) = wrapper(binding, primary, source)?;
    let mut response = client
        .query_bind_checked(
            "SELECT $this AS record,crypto::sha256(type::string($this)) AS sha256 FROM ONLY $id",
            crate::bindings! {"id"=>id.clone()},
        )
        .await?;
    let raw: surrealdb::Value = response.take(0).map_err(crate::durability_io::io_error)?;
    let (record, full_sha) = decode_projection(raw.into_inner())?;
    let Value::Object(mut fields) = record else {
        return Err(refused());
    };
    let manifest = fields
        .remove("historical_native_effects")
        .ok_or_else(refused)?;
    if Value::Object(fields) != expected {
        return Err(refused());
    }
    let mut effects = decode(manifest)?;
    let actual_ids = effects
        .iter()
        .map(|effect| (effect.table.clone(), effect.id.clone()))
        .collect::<Vec<_>>();
    let expected_ids = inventory
        .iter()
        .map(|(table, id)| (table.clone(), id.to_string()))
        .collect::<Vec<_>>();
    if actual_ids != expected_ids || effects.iter().any(|effect| effect.id == id.to_string()) {
        return Err(refused());
    }
    effects.push(HistoricalNativeEffect {
        table: "source_commit_receipt".into(),
        id: id.to_string(),
        before_sha256: None,
        after_sha256: Some(full_sha),
    });
    Ok(effects)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    fn primary() -> (RecordId, CanonicalCommitReceipt) {
        let source = RecordId::new_v4(elephant_types::TableName::Source);
        let receipt = CanonicalCommitReceipt {
            historical_import: None,
            space_id: "household".into(),
            commit_id: "original-batch".into(),
            input_fingerprint: "a".repeat(64),
            plan_fingerprint: "b".repeat(64),
            replay_output: None,
            source_ids: vec![source.clone()],
            publication_authority: None,
            maintenance_authority: Vec::new(),
            source_id: None,
            source_revision: None,
            effect_ids: Vec::new(),
            extension_ids: Vec::new(),
            outbox_ids: Vec::new(),
            committed_at: chrono::Utc::now(),
        };
        (source, receipt)
    }
    fn manifest(after: &serde_json::Value) -> Value {
        crate::surreal::json_to_surreal(serde_json::json!([{"table":"entity","id":"entity:original","before_sha256":"a".repeat(64),"after_sha256":after}])).expect("native")
    }
    #[test]
    fn native_effect_manifest_requires_explicit_absence_and_exact_fields() {
        let removed = decode(manifest(&serde_json::Value::Null)).expect("explicit deletion");
        assert_eq!(removed[0].after_sha256, None);
        assert_eq!(
            removed[0].before_sha256.as_deref(),
            Some("a".repeat(64).as_str())
        );
        let Value::Array(mut absent) = manifest(&serde_json::Value::Null) else {
            unreachable!()
        };
        let Value::Object(ref mut row) = absent.0[0] else {
            unreachable!()
        };
        row.remove("after_sha256");
        assert!(decode(Value::Array(absent)).is_err());
        let Value::Array(mut unknown) = manifest(&serde_json::Value::Null) else {
            unreachable!()
        };
        let Value::Object(ref mut row) = unknown.0[0] else {
            unreachable!()
        };
        row.insert("new_authority".into(), true.into());
        assert!(decode(Value::Array(unknown)).is_err());
        assert!(decode(manifest(&serde_json::json!("bad"))).is_err());
    }
    #[test]
    fn stored_projection_alias_cannot_hide_unknown_wrapper_field() {
        let (_, primary) = primary();
        let source = &primary.source_ids[0];
        let binding =
            HistoricalNativeEffectBinding::new(&"c".repeat(64), "batch", 0, &primary, &[])
                .expect("binding");
        let (_, expected) = wrapper(&binding, &primary, source).expect("wrapper");
        let Value::Object(mut changed) = expected.clone() else {
            unreachable!()
        };
        changed.insert("__historical_native_sha256".into(), "hidden-field".into());
        let projected = Value::Object(
            BTreeMap::<String, Value>::from([
                ("record".into(), Value::Object(changed)),
                ("sha256".into(), "a".repeat(64).into()),
            ])
            .into(),
        );
        let (actual, _) = decode_projection(projected).expect("outer projection");
        assert_ne!(
            actual, expected,
            "stored alias remains inside exact original record"
        );
        assert!(decode_projection(Value::Object(
            BTreeMap::<String, Value>::from([
                ("record".into(), expected),
                ("sha256".into(), "a".repeat(64).into()),
                ("unexpected".into(), true.into()),
            ])
            .into()
        ))
        .is_err());
    }
    #[test]
    fn effect_binding_requires_exact_primary_and_complete_inventory_before_sql() {
        let (source, primary) = primary();
        let inventory = vec![("entity".into(), control_thing("entity", "original"))];
        let binding =
            HistoricalNativeEffectBinding::new(&"c".repeat(64), "batch", 0, &primary, &inventory)
                .expect("binding");
        let mut changed = primary.clone();
        changed.plan_fingerprint = "d".repeat(64);
        assert!(wrapper(&binding, &changed, &source).is_err());
        assert!(begin(
            &mut String::new(),
            &mut BTreeMap::new(),
            &source,
            binding.clone(),
            &primary,
            Vec::new()
        )
        .is_err());
        let mut excess = inventory.clone();
        excess.push(("entity".into(), control_thing("entity", "excess")));
        assert!(begin(
            &mut String::new(),
            &mut BTreeMap::new(),
            &source,
            binding.clone(),
            &primary,
            excess
        )
        .is_err());
        let mut sql = String::new();
        let mut params = BTreeMap::new();
        let prepared =
            begin(&mut sql, &mut params, &source, binding, &primary, inventory).expect("exact IDs");
        finish(&mut sql, &prepared).expect("finish");
        assert!(
            sql.find("historical_effect_manifest_byte_bound")
                .expect("bound")
                < sql.find("CREATE $hne_receipt_id").expect("create")
        );
        assert!(sql.contains("$hne_guard_before.revision+1"));
        assert!(sql.contains("(SELECT * FROM ONLY $hne_epoch_id)!=$hne_epoch_before"));
        let oversized = vec![(
            "entity".into(),
            control_thing("entity", &"x".repeat(MAX_BYTES / 4)),
        )];
        assert!(HistoricalNativeEffectBinding::new(
            &"c".repeat(64),
            "batch",
            0,
            &primary,
            &oversized
        )
        .is_err());
    }
}
