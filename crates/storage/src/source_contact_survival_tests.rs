//! These are pure composition tests, not stand-ins for admitted source authority.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use chrono::Utc;
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::identity::{EntityIdentityKey, IdentityKeyStatus};
use elephant_types::records::rel::{ClaimStatus, EvidenceRef, Rel, TimeBasis};
use elephant_types::records::EvidenceSpan;
use elephant_types::{Link, RecordId, TableName};

fn entity() -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "space".into(),
            "physical-author".into(),
        ),
        EntityType::new("person"),
        "Casey".into(),
    )
}
fn decision(effect: CanonicalEffect) -> SourceDecisionReceipt {
    SourceDecisionReceipt {
        space_id: "space".into(),
        commit_id: "original-creation".into(),
        sequence: 1,
        source_ids: vec![],
        dependency_ids: vec![],
        effects: vec![effect],
        extension_effects: vec![],
        outbox_ids: vec![],
        committed_at: Utc::now(),
    }
}
fn entity_row(row: Entity) -> CanonicalRecord {
    CanonicalRecord::Entity(Box::new(row))
}
fn compose(
    current: CanonicalRecord,
    original: &CanonicalRecord,
    before: Option<CanonicalRecord>,
    after: CanonicalRecord,
    floors: &[CanonicalRecord],
) -> CanonicalRecord {
    let effect = CanonicalEffect {
        before,
        after: Some(after),
    };
    let decision = decision(effect.clone());
    preserve_contact_contributions(Some(current), original, &decision, &effect, floors)
        .unwrap()
        .unwrap()
}

#[test]
fn creation_inverse_retains_distinct_b_fields_and_removes_a_only_fields() {
    let source_a = RecordId::new_v4(TableName::Source);
    let source_b = RecordId::new_v4(TableName::Source);
    let mut a = entity();
    a.base.provenance.sources = vec![source_a];
    a.base.security.labels = vec!["a-only".into()];
    a.aliases = vec!["A secret alias".into()];
    a.identity_keys = vec!["email".into(), "a-profile".into()];
    a.metadata = serde_json::json!({"source_type":"contact_import","nested":{"secret":"a"}});
    let mut b = entity();
    b.base.id = a.base.id.clone();
    b.base.provenance.sources = vec![source_b.clone()];
    b.base.security.labels = vec!["b-only".into()];
    b.display_name = "Blair".into();
    b.identity_keys = vec!["email".into(), "b-profile".into()];
    let mut original = a.clone();
    original.base.provenance.sources.push(source_b.clone());
    original.aliases.push("Blair".into());
    original.identity_keys.push("b-profile".into());
    // Later ordinary inverses already removed B's delta from this overlay.
    let row = compose(
        entity_row(a.clone()),
        &entity_row(original),
        None,
        entity_row(a.clone()),
        &[entity_row(b)],
    );
    let CanonicalRecord::Entity(row) = row else {
        unreachable!()
    };
    assert_eq!(row.display_name, "Blair");
    assert!(row.aliases.is_empty());
    assert_eq!(row.identity_keys, ["email", "b-profile"]);
    assert_eq!(row.metadata, serde_json::json!({}));
    assert_eq!(row.base.provenance.sources, [source_b]);
    assert_eq!(row.base.security.labels, ["b-only"]);
    assert_eq!(row.base.created_at, a.base.created_at);
    assert_eq!(row.base.created_by, a.base.created_by);
}

#[test]
fn later_manual_nested_and_display_fields_survive_without_restoring_removed_sets() {
    let mut a = entity();
    a.aliases = vec!["a-only".into()];
    a.identity_keys = vec!["email".into()];
    a.metadata = serde_json::json!({"nested":{"secret":"a"}});
    let mut b = entity();
    b.base.id = a.base.id.clone();
    b.display_name = "Blair".into();
    b.identity_keys = vec!["email".into(), "manually-removed-b-profile".into()];
    let mut original = a.clone();
    original.display_name = "Manual name".into();
    original.aliases.push("Blair".into());
    original.metadata["nested"]["manual"] = serde_json::json!("keep");
    original.known_safe = true;
    let mut overlay = original.clone();
    overlay.aliases.retain(|name| name != "Blair");
    let row = compose(
        entity_row(overlay),
        &entity_row(original),
        None,
        entity_row(a),
        &[entity_row(b)],
    );
    let CanonicalRecord::Entity(row) = row else {
        unreachable!()
    };
    assert_eq!(row.display_name, "Manual name");
    assert_eq!(row.aliases, ["Blair"]);
    assert_eq!(row.identity_keys, ["email"]);
    assert_eq!(
        row.metadata,
        serde_json::json!({"nested":{"manual":"keep"}})
    );
    assert!(row.known_safe);
}

#[test]
fn older_independent_baseline_keeps_its_fields_and_adds_b_assertions() {
    let mut baseline = entity();
    baseline.display_name = "Independent".into();
    baseline.summary = Some("older independent summary".into());
    baseline.metadata = serde_json::json!({"independent":true});
    let mut a = baseline.clone();
    a.aliases.push("A-only".into());
    a.metadata["secret"] = serde_json::json!("A-only");
    let mut b = entity();
    b.base.id = a.base.id.clone();
    b.display_name = "Blair".into();
    b.identity_keys = vec!["b-email".into()];
    let mut original = a.clone();
    original.aliases.push("Blair".into());
    original.identity_keys.push("b-email".into());
    let row = compose(
        entity_row(a.clone()),
        &entity_row(original),
        Some(entity_row(baseline.clone())),
        entity_row(a),
        &[entity_row(b)],
    );
    let CanonicalRecord::Entity(row) = row else {
        unreachable!()
    };
    assert_eq!(row.display_name, baseline.display_name);
    assert_eq!(row.summary, baseline.summary);
    assert_eq!(row.metadata, baseline.metadata);
    assert_eq!(row.aliases, ["Blair"]);
    assert_eq!(row.identity_keys, ["b-email"]);
}

fn rel() -> Rel {
    Rel {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Rel),
            "space".into(),
            "contact_ingest".into(),
        ),
        subject: Link::new(RecordId::new_v4(TableName::Entity)),
        object: Link::new(RecordId::new_v4(TableName::Entity)),
        rel_type: "works_at".into(),
        status: ClaimStatus::Approved,
        confidence: 0.95,
        validity_window: None,
        evidence: vec![],
        temporal_audit: None,
    }
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "Verify the exact stored confidence fixture, with no arithmetic or tolerance."
)]
fn relationship_floor_preserves_independent_status_and_drops_a_evidence() {
    let mut a = rel();
    a.evidence = vec![EvidenceRef {
        assertion: Link::new(RecordId::new_v4(TableName::Assertion)),
        span: EvidenceSpan {
            revision: Link::new(RecordId::new_v4(TableName::DocRevision)),
            node: None,
            start: 0,
            end: 1,
            quote_hash: "a-only".into(),
        },
        time_basis: TimeBasis::ObservedAt,
        time_value: Utc::now(),
    }];
    let mut b = a.clone();
    b.evidence.clear();
    let mut current = a.clone();
    current.status = ClaimStatus::Disputed;
    current.confidence = 0.4;
    let row = compose(
        CanonicalRecord::Rel(Box::new(current.clone())),
        &CanonicalRecord::Rel(Box::new(current)),
        None,
        CanonicalRecord::Rel(Box::new(a)),
        &[CanonicalRecord::Rel(Box::new(b))],
    );
    let CanonicalRecord::Rel(row) = row else {
        unreachable!()
    };
    assert_eq!(row.status, ClaimStatus::Disputed);
    assert_eq!(row.confidence, 0.4);
    assert!(row.evidence.is_empty());
}

#[test]
fn identity_floor_keeps_only_surviving_source_evidence() {
    let source_a = RecordId::new_v4(TableName::Source);
    let source_b = RecordId::new_v4(TableName::Source);
    let a = EntityIdentityKey {
        base: BaseFields::new(
            RecordId::new_v4(TableName::EntityIdentityKey),
            "space".into(),
            "contact_ingest".into(),
        ),
        key: "email".into(),
        entity: Link::new(RecordId::new_v4(TableName::Entity)),
        confidence: 1.0,
        first_seen: Some(Utc::now()),
        last_seen: Some(Utc::now()),
        evidence: vec![Link::new(source_a)],
        status: IdentityKeyStatus::Active,
    };
    let mut b = a.clone();
    b.evidence = vec![Link::new(source_b.clone())];
    let mut original = a.clone();
    original.evidence.push(Link::new(source_b.clone()));
    let row = compose(
        CanonicalRecord::EntityIdentityKey(Box::new(a.clone())),
        &CanonicalRecord::EntityIdentityKey(Box::new(original)),
        None,
        CanonicalRecord::EntityIdentityKey(Box::new(a)),
        &[CanonicalRecord::EntityIdentityKey(Box::new(b))],
    );
    let CanonicalRecord::EntityIdentityKey(row) = row else {
        unreachable!()
    };
    assert_eq!(row.evidence, [Link::new(source_b)]);
}

#[test]
fn absent_or_tombstoned_current_cannot_be_revived() {
    let row = entity_row(entity());
    let effect = CanonicalEffect {
        before: None,
        after: Some(row.clone()),
    };
    let decision = decision(effect.clone());
    assert!(preserve_contact_contributions(
        None,
        &row,
        &decision,
        &effect,
        std::slice::from_ref(&row)
    )
    .unwrap()
    .is_none());
    let live_overlay = row.clone();
    let mut deleted = row.clone();
    deleted.base_mut().deleted_at = Some(Utc::now());
    let retained =
        preserve_contact_contributions(Some(deleted.clone()), &deleted, &decision, &effect, &[row])
            .unwrap()
            .unwrap();
    assert_eq!(
        retained.fingerprint().unwrap(),
        deleted.fingerprint().unwrap()
    );
    let retained = preserve_contact_contributions(
        Some(live_overlay.clone()),
        &deleted,
        &decision,
        &effect,
        &[live_overlay],
    )
    .unwrap()
    .unwrap();
    assert_eq!(
        retained.fingerprint().unwrap(),
        deleted.fingerprint().unwrap()
    );
}

#[test]
fn mismatched_decision_effect_space_and_fact_cannot_borrow_contributions() {
    let row = CanonicalRecord::Rel(Box::new(rel()));
    let effect = CanonicalEffect {
        before: None,
        after: Some(row.clone()),
    };
    let mut decision = decision(effect.clone());
    decision.effects.clear();
    assert!(preserve_contact_contributions(
        Some(row.clone()),
        &row,
        &decision,
        &effect,
        std::slice::from_ref(&row)
    )
    .is_err());
    decision.effects.push(effect.clone());
    decision.space_id = "foreign".into();
    assert!(preserve_contact_contributions(
        Some(row.clone()),
        &row,
        &decision,
        &effect,
        std::slice::from_ref(&row)
    )
    .is_err());
    decision.space_id = "space".into();
    let mut other = row.clone();
    if let CanonicalRecord::Rel(rel) = &mut other {
        rel.object = Link::new(RecordId::new_v4(TableName::Entity));
    }
    assert!(
        preserve_contact_contributions(Some(row.clone()), &row, &decision, &effect, &[other])
            .is_err()
    );
}

#[test]
fn original_nonperson_type_and_temporal_fact_cannot_be_replaced_by_defaults() {
    let mut original = entity();
    original.entity_type = EntityType::new("project");
    let mut projected = original.clone();
    projected.entity_type = EntityType::new("person");
    let original = entity_row(original);
    let effect = CanonicalEffect {
        before: None,
        after: Some(original.clone()),
    };
    let selected = decision(effect.clone());
    assert!(preserve_contact_contributions(
        Some(original.clone()),
        &original,
        &selected,
        &effect,
        &[entity_row(projected)]
    )
    .is_err());
    let mut original = rel();
    original.validity_window = Some(elephant_types::records::rel::ValidityWindow {
        start: Some(Utc::now()),
        end: None,
    });
    let mut projected = original.clone();
    projected.validity_window = None;
    let original = CanonicalRecord::Rel(Box::new(original));
    let effect = CanonicalEffect {
        before: None,
        after: Some(original.clone()),
    };
    let selected = decision(effect.clone());
    assert!(preserve_contact_contributions(
        Some(original.clone()),
        &original,
        &selected,
        &effect,
        &[CanonicalRecord::Rel(Box::new(projected))]
    )
    .is_err());
}

#[test]
fn independent_later_scalar_security_cannot_lower_b_required_envelope() {
    let mut a = entity();
    a.base.security.level = 1;
    let mut b = a.clone();
    b.base.security.level = 5;
    let mut current = a.clone();
    current.base.security.level = 2;
    let row = compose(
        entity_row(current.clone()),
        &entity_row(current),
        None,
        entity_row(a),
        &[entity_row(b)],
    );
    assert_eq!(row.base().security.level, 5);
}
