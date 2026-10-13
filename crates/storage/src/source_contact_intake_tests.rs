#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::{EntityIdentityKeyRepository, EntityRepository, Repository, StorageConfig};
use elephant_types::records::{SourceType, SpaceConfig};

async fn client(embedded: bool) -> StorageClient {
    let storage = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        storage
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .unwrap();
    }
    crate::SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                "home".into(),
                "test".into(),
            ),
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    storage
}
fn source() -> Source {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            "home".into(),
            "test".into(),
        ),
        SourceType::Manual,
        "Contact import".into(),
    );
    source.base.subjects_state = SubjectsState::Complete;
    source.metadata = serde_json::json!({"contact":{
        "name":"New Name", "email":"PERSON@example.invalid", "company":"Example Org", "job_title":"Engineer"
    }});
    source
}
fn entity(name: &str) -> Entity {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Entity),
        "home".into(),
        "test".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    Entity::new(base, EntityType::new("person"), name.into())
}
async fn check(storage: &StorageClient, prepared: &PreparedSourceContact) -> StorageResult<()> {
    if let Some(store) = storage.embedded_store() {
        store.mutate(|tx| prepared.check_tx(tx)).await
    } else {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        prepared.append_guard_sql(&mut sql, &mut bindings)?;
        sql.push_str("COMMIT TRANSACTION;\n");
        storage.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn prepared_guards(embedded: bool) {
    let storage = client(embedded).await;
    let mut original = entity("Independent Name");
    original.metadata = serde_json::json!({"independent":"preserve"});
    original.base.security.labels.push("private-label".into());
    original.base.subjects_state = SubjectsState::Partial;
    let original = EntityRepository::new(storage.clone())
        .create(original)
        .await
        .unwrap();
    let mut base = original.base.clone();
    base.id = RecordId::new_v4(TableName::EntityIdentityKey);
    let key = EntityIdentityKeyRepository::new(storage.clone())
        .create(EntityIdentityKey {
            base,
            key: "person@example.invalid".into(),
            entity: Link::new(original.base.id.clone()),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        })
        .await
        .unwrap();
    let mut source = source();
    source.base.security.handling.push("source-handling".into());
    source
        .base
        .subjects
        .push(Link::new(RecordId::new_v4(TableName::Entity)));
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.effects.len(), 4);
    let after = prepared
        .effects
        .iter()
        .find_map(|effect| match &effect.after {
            ContactRecord::Entity(row) if row.base.id == original.base.id => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(after.display_name, original.display_name);
    assert_eq!(after.metadata, original.metadata);
    assert!(after.aliases.contains(&"New Name".into()));
    assert_eq!(after.base.subjects_state, SubjectsState::Partial);
    assert!(after.base.security.labels.contains(&"private-label".into()));
    assert!(after
        .base
        .security
        .handling
        .contains(&"source-handling".into()));
    assert_eq!(after.base.subjects, source.base.subjects);
    assert!(prepared
        .authorizations()
        .iter()
        .any(|row| row.base().id == key.base.id));
    assert_eq!(
        EntityRepository::new(storage.clone())
            .get("home", &original.base.id)
            .await
            .unwrap()
            .unwrap(),
        original
    );
    assert!(crate::SourceRepository::new(storage.clone())
        .list("home", 10, None)
        .await
        .unwrap()
        .is_empty());
    check(&storage, &prepared).await.unwrap();
    // A nonmatching row changes the global epoch but not the sealed selector.
    EntityRepository::new(storage.clone())
        .create(entity("Unrelated"))
        .await
        .unwrap();
    check(&storage, &prepared).await.unwrap();
    // A negative company selector is still a material dependency.
    EntityRepository::new(storage.clone())
        .create(entity("Example Org"))
        .await
        .unwrap();
    assert!(check(&storage, &prepared).await.is_err());
    // A fresh plan passes until an exact originally authorized field changes.
    let fresh = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    check(&storage, &fresh).await.unwrap();
    let mut changed = original.clone();
    changed
        .base
        .security
        .handling
        .push("new-restriction".into());
    EntityRepository::new(storage.clone())
        .update("home", &original.base.id, changed)
        .await
        .unwrap();
    assert!(check(&storage, &fresh).await.is_err());
}
#[tokio::test]
async fn embedded_contact_preparation_preserves_envelopes_and_guards_queries() {
    prepared_guards(true).await;
}
#[tokio::test]
async fn surreal_contact_preparation_preserves_envelopes_and_guards_queries() {
    prepared_guards(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, companies in the original fixture scope through assertions and cleanup."
)]
async fn ambiguity_and_tombstones(embedded: bool) {
    let storage = client(embedded).await;
    let source = source();
    let companies = EntityRepository::new(storage.clone());
    companies.create(entity("Example Org")).await.unwrap();
    companies.create(entity("Example Org")).await.unwrap();
    assert!(
        PreparedSourceContact::prepare(&storage, &source)
            .await
            .is_err(),
        "ambiguous company is never picked arbitrarily"
    );
    let mut plain = source.clone();
    plain.metadata["contact"]
        .as_object_mut()
        .unwrap()
        .remove("company");
    let prepared = PreparedSourceContact::prepare(&storage, &plain)
        .await
        .unwrap()
        .unwrap();
    let key = prepared
        .effects
        .iter()
        .find_map(|effect| match &effect.after {
            ContactRecord::EntityIdentityKey(row) => Some((**row).clone()),
            _ => None,
        })
        .unwrap();
    let mut deleted = key.clone();
    deleted.base.deleted_at = Some(chrono::Utc::now());
    EntityIdentityKeyRepository::new(storage.clone())
        .create(deleted)
        .await
        .unwrap();
    assert!(
        PreparedSourceContact::prepare(&storage, &plain)
            .await
            .is_err(),
        "deterministic key tombstone cannot be revived"
    );
}
#[tokio::test]
async fn embedded_contact_ambiguity_and_key_tombstone_fail_closed() {
    ambiguity_and_tombstones(true).await;
}
#[tokio::test]
async fn surreal_contact_ambiguity_and_key_tombstone_fail_closed() {
    ambiguity_and_tombstones(false).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn sealed_contact_source_and_retirement_baseline_are_exact() {
    let storage = client(true).await;
    let source = source();
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    let restored: PreparedSourceContact =
        serde_json::from_value(serde_json::to_value(&prepared).unwrap()).unwrap();
    restored.validate_source(&source).unwrap();
    let mut changed = source.clone();
    changed.metadata["contact"]["name"] = "Different input".into();
    assert!(restored.validate_source(&changed).is_err());
    changed = source.clone();
    changed.base.id = RecordId::new_v4(TableName::Source);
    assert!(restored.validate_source(&changed).is_err());
    let mut plan = CanonicalPlan::default();
    prepared.append_plan(&mut plan).unwrap();
    assert_eq!(plan.effects.len(), prepared.effects.len());
    assert_eq!(plan.outbox.len(), prepared.effects.len());
    assert!(plan.effects.iter().all(|effect| effect.before.is_none()));

    // An independent baseline survives source retirement; source-only old
    // fields are not copied back into the next accepted contact output.
    let independent = entity("Independent");
    let mut previous = independent.clone();
    previous.aliases.push("retired-source-alias".into());
    previous
        .base
        .provenance
        .sources
        .push(source.base.id.clone());
    let mut retirement = CanonicalPlan::default();
    retirement
        .push(
            Some(CanonicalRecord::Entity(Box::new(previous.clone()))),
            Some(CanonicalRecord::Entity(Box::new(independent.clone()))),
        )
        .unwrap();
    let mut contact = prepared.clone();
    contact.dependencies.clear();
    contact.effects.clear();
    contact.baselines.clear();
    let original = ContactRecord::Entity(Box::new(previous.clone()));
    let baseline = contact
        .capture_baseline(Some(&retirement), &previous.base.id, Some(&original))
        .unwrap()
        .unwrap();
    let ContactRecord::Entity(mut after) = baseline else {
        unreachable!()
    };
    assert!(!after.aliases.contains(&"retired-source-alias".into()));
    after.aliases.push("new-source-alias".into());
    inherit_source(&mut after.base, &source).unwrap();
    contact
        .effect(Some(original), ContactRecord::Entity(after))
        .unwrap();
    contact.digest = contact.compute_digest().unwrap();
    let fresh_retirement = retirement.effects[0].clone();
    contact.append_plan(&mut retirement).unwrap();
    assert!(contact
        .certifies_effect(&retirement.effects[0], Some(&fresh_retirement))
        .unwrap());
    assert!(contact
        .certifies_effect(&retirement.effects[0], None)
        .is_err());
    let mut changed_retirement = fresh_retirement.clone();
    if let Some(CanonicalRecord::Entity(row)) = &mut changed_retirement.after {
        row.summary = Some("different independent baseline".into());
    }
    assert!(contact
        .certifies_effect(&retirement.effects[0], Some(&changed_retirement))
        .is_err());
    changed_retirement = fresh_retirement.clone();
    if let Some(CanonicalRecord::Entity(row)) = &mut changed_retirement.before {
        row.base.security.level += 1;
    }
    assert!(contact
        .certifies_effect(&retirement.effects[0], Some(&changed_retirement))
        .is_err());
    let mut changed_actual = retirement.effects[0].clone();
    if let Some(CanonicalRecord::Entity(row)) = &mut changed_actual.before {
        row.base.security.level += 1;
    }
    assert!(contact
        .certifies_effect(&changed_actual, Some(&fresh_retirement))
        .is_err());
    // The final grounded contribution is covered by the inverse, too.
    if let Some(CanonicalRecord::Entity(row)) = &mut retirement.effects[0].after {
        row.aliases.push("grounded additive name".into());
    }
    let decisions = contact.decision_effects(&retirement).unwrap();
    assert_eq!(decisions.len(), 1);
    assert_eq!(
        serde_json::to_value(&decisions[0].before).unwrap(),
        serde_json::to_value(Some(CanonicalRecord::Entity(Box::new(independent.clone())))).unwrap()
    );
    assert_eq!(
        serde_json::to_value(&decisions[0].after).unwrap(),
        serde_json::to_value(&retirement.effects[0].after).unwrap()
    );
    let inverse = crate::source_decision::inverse_effect(decisions[0].after.clone(), &decisions[0])
        .unwrap()
        .unwrap();
    assert_eq!(
        inverse.fingerprint().unwrap(),
        CanonicalRecord::Entity(Box::new(independent.clone()))
            .fingerprint()
            .unwrap()
    );
    assert_eq!(
        contact.decision_outbox_ids(),
        retirement
            .outbox
            .iter()
            .map(|event| event.base.id.clone())
            .collect::<Vec<_>>()
    );
    let unrelated = CanonicalEffect {
        before: None,
        after: Some(CanonicalRecord::Entity(Box::new(entity("unrelated")))),
    };
    assert!(!contact.certifies_effect(&unrelated, None).unwrap());
    let mut expanded = retirement.clone();
    expanded.effects.push(unrelated);
    assert_eq!(contact.decision_effects(&expanded).unwrap().len(), 1);
    let mut incomplete = retirement.clone();
    incomplete.outbox.clear();
    assert!(contact.decision_effects(&incomplete).is_err());
    assert_eq!(retirement.effects.len(), 1);
    assert_eq!(
        serde_json::to_value(retirement.effects[0].before.as_ref().unwrap()).unwrap(),
        serde_json::to_value(CanonicalRecord::Entity(Box::new(previous))).unwrap()
    );
    let CanonicalRecord::Entity(result) = retirement.effects[0].after.as_ref().unwrap() else {
        unreachable!()
    };
    assert_eq!(result.display_name, independent.display_name);
    assert!(result.aliases.contains(&"new-source-alias".into()));
    assert!(!result.aliases.contains(&"retired-source-alias".into()));
    let mut wrong = CanonicalPlan::default();
    assert!(
        contact.append_plan(&mut wrong).is_err(),
        "required retirement prefix cannot be omitted"
    );
    let mut changed_baseline = independent.clone();
    changed_baseline.base.security.level += 1;
    assert!(!same_retirement_baseline(
        Some(&ContactRecord::Entity(Box::new(independent))),
        Some(&ContactRecord::Entity(Box::new(changed_baseline)))
    )
    .unwrap());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn relation_tombstone(embedded: bool) {
    let storage = client(embedded).await;
    let source = source();
    let seed = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    // Seed existing graph fixtures only; the preparer itself never writes.
    for effect in &seed.effects {
        match &effect.after {
            ContactRecord::Entity(row) => {
                EntityRepository::new(storage.clone())
                    .create((**row).clone())
                    .await
                    .unwrap();
            }
            ContactRecord::EntityIdentityKey(row) => {
                EntityIdentityKeyRepository::new(storage.clone())
                    .create((**row).clone())
                    .await
                    .unwrap();
            }
            ContactRecord::Rel(row) => {
                let mut deleted = (**row).clone();
                deleted.base.deleted_at = Some(chrono::Utc::now());
                crate::RelRepository::new(storage.clone())
                    .create(deleted)
                    .await
                    .unwrap();
            }
        }
    }
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    assert!(!prepared
        .effects
        .iter()
        .any(|effect| matches!(effect.after, ContactRecord::Rel(_))));
    assert!(prepared
        .dependencies
        .iter()
        .any(|row| matches!(row, ContactRecord::Rel(row) if row.base.deleted_at.is_some())));
    check(&storage, &prepared).await.unwrap();
    assert!(!prepared
        .independent_contribution_rows(&source)
        .unwrap()
        .iter()
        .any(|row| matches!(row, CanonicalRecord::Rel(_))));
}
#[tokio::test]
async fn embedded_contact_preserves_deleted_works_at_edge() {
    relation_tombstone(true).await;
}
#[tokio::test]
async fn surreal_contact_preserves_deleted_works_at_edge() {
    relation_tombstone(false).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn identical_contact_value_reacquires_new_generation_support_after_retirement() {
    let storage = client(true).await;
    let source = source();
    let mut prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    let output = prepared.effects[0].after.clone();
    let mut retirement = CanonicalPlan::default();
    retirement.push(Some(output.canonical()), None).unwrap();
    prepared.dependencies.clear();
    prepared.effects.clear();
    prepared.baselines.clear();
    assert!(prepared
        .capture_baseline(Some(&retirement), &output.base().id, Some(&output))
        .unwrap()
        .is_none());
    prepared
        .effect(Some(output.clone()), output.clone())
        .unwrap();
    prepared.digest = prepared.compute_digest().unwrap();
    assert_eq!(
        prepared.effects.len(),
        1,
        "same value is still a new support effect"
    );
    prepared.append_plan(&mut retirement).unwrap();
    assert_eq!(retirement.effects.len(), 1);
    assert_eq!(
        serde_json::to_value(retirement.effects[0].after.as_ref().unwrap()).unwrap(),
        serde_json::to_value(output.canonical()).unwrap()
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn contact_seal_and_overlay_reject_omission_or_overwrite_but_allow_grounded_enrichment() {
    let storage = client(true).await;
    let source = source();
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    prepared
        .validate_seal("home", &source.base.id, prepared.digest())
        .unwrap();
    assert!(prepared
        .validate_seal("other", &source.base.id, prepared.digest())
        .is_err());
    assert!(prepared
        .validate_seal("home", &source.base.id, "different digest")
        .is_err());
    let mut changed = prepared.clone();
    if let ContactRecord::Entity(row) = &mut changed.effects[0].after {
        row.display_name = "tampered".into();
    }
    assert!(changed
        .validate_seal("home", &source.base.id, prepared.digest())
        .is_err());
    let mut plan = CanonicalPlan::default();
    prepared.append_plan(&mut plan).unwrap();
    prepared.validate_plan(&plan).unwrap();
    let mut additive = plan.clone();
    let person = additive
        .effects
        .iter_mut()
        .find_map(|effect| match effect.after.as_mut() {
            Some(CanonicalRecord::Entity(row)) if row.entity_type.as_str() == "person" => Some(row),
            _ => None,
        })
        .unwrap();
    person.aliases.push(person.display_name.clone());
    person.display_name = "New Name Expanded".into();
    person.identity_keys.push("new name expanded".into());
    person.last_seen = Some(chrono::Utc::now());
    person.metadata["angel_protectees"] = serde_json::json!(["additional"]);
    prepared.validate_plan(&additive).unwrap();
    let mut omitted = additive.clone();
    omitted.effects.remove(0);
    assert!(prepared.validate_plan(&omitted).is_err());
    let mut missing_event = additive.clone();
    missing_event.outbox.clear();
    assert!(prepared.validate_plan(&missing_event).is_err());
    let mut changed_event = additive.clone();
    changed_event.outbox[0].payload["actor"] = "forged".into();
    assert!(prepared.validate_plan(&changed_event).is_err());
    let mut overwritten = additive.clone();
    let person = overwritten
        .effects
        .iter_mut()
        .find_map(|effect| match effect.after.as_mut() {
            Some(CanonicalRecord::Entity(row)) if row.entity_type.as_str() == "person" => Some(row),
            _ => None,
        })
        .unwrap();
    person.aliases.clear();
    assert!(
        prepared.validate_plan(&overwritten).is_err(),
        "original contact name cannot disappear"
    );
    let mut unrelated_summary = additive.clone();
    let person = unrelated_summary
        .effects
        .iter_mut()
        .find_map(|effect| match effect.after.as_mut() {
            Some(CanonicalRecord::Entity(row)) if row.entity_type.as_str() == "person" => Some(row),
            _ => None,
        })
        .unwrap();
    person.summary = Some("not an allowed contact overlay mutation".into());
    assert!(prepared.validate_plan(&unrelated_summary).is_err());
    let mut redirected = additive;
    let key = redirected
        .effects
        .iter_mut()
        .find_map(|effect| match effect.after.as_mut() {
            Some(CanonicalRecord::EntityIdentityKey(row)) => Some(row),
            _ => None,
        })
        .unwrap();
    key.entity = Link::new(RecordId::new_v4(TableName::Entity));
    assert!(prepared.validate_plan(&redirected).is_err());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn contact_relationship_proof_keeps_fact_topology_and_envelope_through_maintenance() {
    let storage = client(true).await;
    let mut source = source();
    source.base.security.handling.push("private".into());
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    let original = prepared
        .effects
        .iter()
        .find_map(|effect| match &effect.after {
            ContactRecord::Rel(row) => Some((**row).clone()),
            _ => None,
        })
        .unwrap();
    assert!(prepared.verifies_rel(&original).unwrap());
    let mut maintained = original.clone();
    maintained.status = ClaimStatus::Disputed;
    maintained.confidence = 0.4;
    maintained.base.updated_at = chrono::Utc::now();
    assert!(
        prepared.verifies_rel(&maintained).unwrap(),
        "A separately verifies the current canonical maintenance support"
    );
    maintained.object = Link::new(RecordId::new_v4(TableName::Entity));
    assert!(!prepared.verifies_rel(&maintained).unwrap());
    let mut weaker = original.clone();
    weaker.base.security.handling.clear();
    assert!(!prepared.verifies_rel(&weaker).unwrap());
    let mut withdrawn = original;
    withdrawn.base.deleted_at = Some(chrono::Utc::now());
    assert!(!prepared.verifies_rel(&withdrawn).unwrap());
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn contact_inverse_deletes_source_only_rows_without_reviving_independent_tombstones() {
    let storage = client(true).await;
    let source = source();
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    let output = prepared.effects[0].after.clone();
    let mut original = output.clone();
    let ContactRecord::Entity(row) = &mut original else {
        unreachable!()
    };
    row.aliases.push("old source contribution".into());
    for physical_delete in [true, false] {
        let mut retired = original.clone();
        let ContactRecord::Entity(row) = &mut retired else {
            unreachable!()
        };
        row.base.deleted_at = Some(chrono::Utc::now());
        let mut plan = CanonicalPlan::default();
        plan.push(
            Some(original.canonical()),
            if physical_delete {
                None
            } else {
                Some(retired.canonical())
            },
        )
        .unwrap();
        let fresh_retirement = plan.effects[0].clone();
        let mut contact = prepared.clone();
        contact.dependencies.clear();
        contact.predicates.clear();
        contact.effects.clear();
        contact.baselines.clear();
        assert!(contact
            .capture_baseline(Some(&plan), &original.base().id, Some(&original))
            .unwrap()
            .is_none());
        contact
            .effect(Some(original.clone()), output.clone())
            .unwrap();
        contact.digest = contact.compute_digest().unwrap();
        contact.append_plan(&mut plan).unwrap();
        assert!(contact
            .certifies_effect(&plan.effects[0], Some(&fresh_retirement))
            .unwrap());
        let effects = contact.decision_effects(&plan).unwrap();
        assert!(effects[0].before.is_none());
        assert!(
            crate::source_decision::inverse_effect(effects[0].after.clone(), &effects[0])
                .unwrap()
                .is_none()
        );
        // A deleted original is not a lifecycle-generated retirement of an
        // originally live contact and cannot be normalized into a new create.
        contact.effects[0].before = Some(retired.clone());
        contact.digest = contact.compute_digest().unwrap();
        assert!(contact
            .validate_seal("home", &source.base.id, contact.digest())
            .is_err());
    }
    let mut plan = CanonicalPlan::default();
    prepared.append_plan(&mut plan).unwrap();
    for effect in &plan.effects {
        assert!(prepared.certifies_effect(effect, None).unwrap());
    }
    assert!(prepared
        .decision_effects(&plan)
        .unwrap()
        .iter()
        .all(|effect| effect.before.is_none()));
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn service_contact_preflight_stays_within_original_full_source_envelope() {
    use elephant_types::publication::SourceAuthorizationSnapshot;
    let storage = client(true).await;
    let mut source = source();
    source.base.security.level = 3;
    source.base.security.labels = vec!["z-label".into(), "a-label".into()];
    source.base.security.handling = vec!["no_export".into()];
    source.base.subjects = vec![Link::new(RecordId::new_v4(TableName::Entity))];
    source.base.subjects_state = SubjectsState::Partial;
    let prepared = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    let original = SourceAuthorizationSnapshot::from(&source);
    prepared.validate_service_authority(&original).unwrap();
    let mut changed = original.clone();
    changed.envelope.security.labels.reverse();
    prepared.validate_service_authority(&changed).unwrap();
    for dimension in 0..7 {
        let mut changed = original.clone();
        match dimension {
            0 => changed.envelope.security.level = 0,
            1 => changed.envelope.security.labels.clear(),
            2 => changed.envelope.security.handling.clear(),
            3 => changed.envelope.subjects.clear(),
            4 => changed.envelope.subjects_state = SubjectsState::Complete,
            5 => changed.envelope.security.policy_id = "another-policy".into(),
            _ => changed.envelope.source_id = RecordId::new_v4(TableName::Source),
        }
        assert!(
            prepared.validate_service_authority(&changed).is_err(),
            "dimension {dimension}"
        );
    }
    let mut hidden_company = entity("Example Org");
    hidden_company.base.security.level = 4;
    EntityRepository::new(storage.clone())
        .create(hidden_company)
        .await
        .unwrap();
    let hidden = PreparedSourceContact::prepare(&storage, &source)
        .await
        .unwrap()
        .unwrap();
    assert!(
        hidden.validate_service_authority(&original).is_err(),
        "existing company dependency cannot borrow a lower source's background authority"
    );
    assert_eq!(
        EntityRepository::new(storage)
            .list("home", 10, None)
            .await
            .unwrap()
            .len(),
        1,
        "service preflight never publishes contact records"
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn independent_contribution_projection(embedded: bool) {
    let storage = client(embedded).await;
    let a = RecordId::new_v4(TableName::Source);
    let mut original = entity("A private display");
    original.aliases = vec!["A private alias".into()];
    original.summary = Some("A private summary".into());
    original.metadata =
        serde_json::json!({"source_type":"contact_import","A-private":"annotation"});
    original.known_safe = true;
    original.risk_level = Some(0.8);
    original.trust_level = Some(0.9);
    original.identity_keys = vec![
        "person@example.invalid".into(),
        "https://profile.invalid/A-only".into(),
    ];
    original.base.security.level = 7;
    original.base.security.labels = vec!["A-private".into()];
    original.base.provenance.sources = vec![a.clone()];
    let person = EntityRepository::new(storage.clone())
        .create(original)
        .await
        .unwrap();
    let mut key_base = person.base.clone();
    key_base.id = RecordId::new_v4(TableName::EntityIdentityKey);
    let email_key = EntityIdentityKeyRepository::new(storage.clone())
        .create(EntityIdentityKey {
            base: key_base,
            key: "person@example.invalid".into(),
            entity: Link::new(person.base.id.clone()),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: vec![Link::new(a.clone())],
            status: IdentityKeyStatus::Active,
        })
        .await
        .unwrap();
    let mut company_base = person.base.clone();
    company_base.id = RecordId::new_v4(TableName::Entity);
    let mut company = Entity::new(company_base, EntityType::new("org"), "Example Org".into());
    company.metadata = serde_json::json!({"A-private":"company annotation"});
    company.summary = Some("A private company summary".into());
    let company = EntityRepository::new(storage.clone())
        .create(company)
        .await
        .unwrap();
    let mut rel_base = person.base.clone();
    rel_base.id = RecordId::new_v4(TableName::Rel);
    let rel = crate::RelRepository::new(storage.clone())
        .create(Rel {
            base: rel_base,
            subject: Link::new(person.base.id.clone()),
            object: Link::new(company.base.id.clone()),
            rel_type: "works_at".into(),
            status: ClaimStatus::Approved,
            confidence: 0.95,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        })
        .await
        .unwrap();
    let mut b = source();
    b.metadata["contact"]["profile"] = serde_json::json!("https://profile.invalid/B-only");
    let prepared = Box::pin(PreparedSourceContact::prepare(&storage, &b))
        .await
        .unwrap()
        .unwrap();
    assert!(
        serde_json::to_string(&prepared)
            .unwrap()
            .contains("A private summary"),
        "the private plan really includes the A-bearing baseline being excluded"
    );
    let projected = prepared.independent_contribution_rows(&b).unwrap();
    assert_eq!(projected.len(), 5);
    for row in &projected {
        assert!(prepared
            .effects
            .iter()
            .any(|effect| effect.after.base().id == row.base().id));
        assert_eq!(row.base().security, b.base.security);
        assert_eq!(row.base().subjects, b.base.subjects);
        assert_eq!(row.base().subjects_state, b.base.subjects_state);
        assert_eq!(row.base().provenance.sources, vec![b.base.id.clone()]);
    }
    let rendered = serde_json::to_string(&projected).unwrap();
    assert!(!rendered.contains("A private"));
    assert!(!rendered.contains("A-private"));
    assert!(!rendered.contains("A-only"));
    assert!(!rendered.contains("source_type"));
    assert!(!rendered.contains(&a.to_string()));
    let projected_person = projected
        .iter()
        .find_map(|row| match row {
            CanonicalRecord::Entity(row) if row.base.id == person.base.id => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(projected_person.display_name, "New Name");
    assert!(projected_person.aliases.is_empty());
    assert!(projected_person.summary.is_none());
    assert!(projected_person.handles.is_empty());
    assert!(!projected_person.known_safe);
    assert!(projected_person.risk_level.is_none());
    assert!(projected_person.trust_level.is_none());
    assert_eq!(projected_person.metadata, serde_json::json!({}));
    assert_eq!(
        projected_person.identity_keys,
        vec!["https://profile.invalid/B-only", "person@example.invalid"]
    );
    let projected_key = projected
        .iter()
        .find_map(|row| match row {
            CanonicalRecord::EntityIdentityKey(row) if row.base.id == email_key.base.id => {
                Some(row)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(projected_key.entity.id(), &person.base.id);
    assert_eq!(projected_key.evidence, vec![Link::new(b.base.id.clone())]);
    let projected_relation = projected
        .iter()
        .find_map(|row| match row {
            CanonicalRecord::Rel(row) if row.base.id == rel.base.id => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(projected_relation.subject.id(), &person.base.id);
    assert_eq!(projected_relation.object.id(), &company.base.id);
    assert_eq!(projected_relation.rel_type, "works_at");
    let mut changed_source = b.clone();
    changed_source.metadata["contact"]["name"] = serde_json::json!("Changed source input");
    assert!(prepared
        .independent_contribution_rows(&changed_source)
        .is_err());
    let mut changed_seal = prepared.clone();
    if let ContactRecord::Entity(row) = &mut changed_seal.effects[0].after {
        row.base.id = RecordId::new_v4(TableName::Entity);
    }
    assert!(changed_seal.independent_contribution_rows(&b).is_err());
    // Original input provenance is not laundered away. A's proof acquisition
    // excludes this conjunctive case when the declared predecessor is erased.
    b.base.provenance.sources = vec![a.clone()];
    let derived = Box::pin(PreparedSourceContact::prepare(&storage, &b))
        .await
        .unwrap()
        .unwrap();
    assert!(derived
        .independent_contribution_rows(&b)
        .unwrap()
        .iter()
        .all(|row| row.base().provenance.sources.contains(&a)));
}

#[tokio::test]
async fn embedded_original_contact_projection_excludes_inherited_fields() {
    Box::pin(independent_contribution_projection(true)).await;
}

#[tokio::test]
async fn surreal_original_contact_projection_excludes_inherited_fields() {
    Box::pin(independent_contribution_projection(false)).await;
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
async fn independent_projection_coalesces_only_original_person_company_identity() {
    let storage = client(true).await;
    let person = EntityRepository::new(storage.clone())
        .create(entity("Example Org"))
        .await
        .unwrap();
    let mut base = person.base.clone();
    base.id = RecordId::new_v4(TableName::EntityIdentityKey);
    EntityIdentityKeyRepository::new(storage.clone())
        .create(EntityIdentityKey {
            base,
            key: "person@example.invalid".into(),
            entity: Link::new(person.base.id.clone()),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        })
        .await
        .unwrap();
    let b = source();
    let prepared = Box::pin(PreparedSourceContact::prepare(&storage, &b))
        .await
        .unwrap()
        .unwrap();
    let rows = prepared.independent_contribution_rows(&b).unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| matches!(row, CanonicalRecord::Entity(_)))
            .count(),
        1
    );
    let entity = rows
        .iter()
        .find_map(|row| match row {
            CanonicalRecord::Entity(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(entity.base.id, person.base.id);
    assert_eq!(entity.display_name, "New Name");
    assert_eq!(entity.aliases, vec!["Example Org"]);
    let rel = rows
        .iter()
        .find_map(|row| match row {
            CanonicalRecord::Rel(row) => Some(row),
            _ => None,
        })
        .unwrap();
    assert_eq!(rel.subject.id(), &person.base.id);
    assert_eq!(rel.object.id(), &person.base.id);
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn independent_projection_refuses_inherited_facts(embedded: bool) {
    use elephant_types::records::rel::ValidityWindow;

    // Every case prepares a real contact against persisted inherited facts;
    // neither the seal nor the prepared effects are fabricated or edited.
    for (case, category) in [
        ("person_type", "contact_contribution_person_type_unowned"),
        ("company_type", "contact_contribution_company_type_unowned"),
        (
            "key_confidence",
            "contact_contribution_key_confidence_unowned",
        ),
        (
            "rel_window",
            "contact_contribution_relationship_facts_unowned",
        ),
        (
            "rel_status",
            "contact_contribution_relationship_facts_unowned",
        ),
        (
            "rel_confidence",
            "contact_contribution_relationship_facts_unowned",
        ),
    ] {
        let storage = Box::pin(client(embedded)).await;
        let mut person = entity("Inherited person");
        if case == "person_type" {
            person.entity_type = EntityType::new("org");
        }
        let person = Box::pin(EntityRepository::new(storage.clone()).create(person))
            .await
            .unwrap();
        let mut key_base = person.base.clone();
        key_base.id = RecordId::new_v4(TableName::EntityIdentityKey);
        let key = EntityIdentityKey {
            base: key_base,
            key: "person@example.invalid".into(),
            entity: Link::new(person.base.id.clone()),
            confidence: if case == "key_confidence" { 0.7 } else { 1.0 },
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        };
        let key = Box::pin(EntityIdentityKeyRepository::new(storage.clone()).create(key))
            .await
            .unwrap();
        let mut company = entity("Example Org");
        if case != "company_type" {
            company.entity_type = EntityType::new("org");
        }
        let company = Box::pin(EntityRepository::new(storage.clone()).create(company))
            .await
            .unwrap();
        let mut rel_base = person.base.clone();
        rel_base.id = RecordId::new_v4(TableName::Rel);
        let rel = Rel {
            base: rel_base,
            subject: Link::new(person.base.id.clone()),
            object: Link::new(company.base.id.clone()),
            rel_type: "works_at".into(),
            status: if case == "rel_status" {
                ClaimStatus::Disputed
            } else {
                ClaimStatus::Approved
            },
            confidence: if case == "rel_confidence" { 0.4 } else { 0.95 },
            validity_window: (case == "rel_window")
                .then(|| ValidityWindow::starting_at(person.base.created_at)),
            evidence: vec![],
            temporal_audit: None,
        };
        let rel = Box::pin(crate::RelRepository::new(storage.clone()).create(rel))
            .await
            .unwrap();
        let input = source();
        let prepared = Box::pin(PreparedSourceContact::prepare(&storage, &input))
            .await
            .unwrap()
            .unwrap();
        assert!(
            matches!(
                prepared.independent_contribution_rows(&input),
                Err(StorageError::Conflict(ref reason)) if reason == category
            ),
            "case {case} must refuse rather than invent ownership"
        );
        // The refusal and preparation are read-only, including the inherited
        // facts that cannot yet be represented as independent contributions.
        assert_eq!(
            Box::pin(EntityRepository::new(storage.clone()).get("home", &person.base.id))
                .await
                .unwrap(),
            Some(person)
        );
        assert_eq!(
            Box::pin(EntityRepository::new(storage.clone()).get("home", &company.base.id))
                .await
                .unwrap(),
            Some(company)
        );
        assert_eq!(
            Box::pin(EntityIdentityKeyRepository::new(storage.clone()).get("home", &key.base.id))
                .await
                .unwrap(),
            Some(key)
        );
        assert_eq!(
            Box::pin(crate::RelRepository::new(storage.clone()).get("home", &rel.base.id))
                .await
                .unwrap(),
            Some(rel)
        );
    }
}

#[tokio::test]
async fn embedded_contact_projection_refuses_unowned_inherited_facts() {
    Box::pin(independent_projection_refuses_inherited_facts(true)).await;
}

#[tokio::test]
async fn surreal_contact_projection_refuses_unowned_inherited_facts() {
    Box::pin(independent_projection_refuses_inherited_facts(false)).await;
}
