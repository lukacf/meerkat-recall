//! Codec, original-control CAS and actual canonical receipt boundaries.
//!
//! These
//! tests do not fabricate completed source publications. Full contact worker,
//! two-source inverse and post-erasure maintenance oracles live with finalization.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::*;
use crate::canonical_commit::{CanonicalFault, CanonicalPlan, CanonicalRepository};
use crate::{Repository, StorageConfig};
use elephant_types::records::{
    entity::{EntityStatus, EntityType},
    rel::ClaimStatus,
    BaseFields, Entity, SpaceConfig,
};
use elephant_types::{Link, TableName};

fn base(table: TableName) -> BaseFields {
    BaseFields::new(
        RecordId::new_v4(table),
        "lineage-space".into(),
        "test".into(),
    )
}
fn rel() -> Rel {
    Rel {
        base: base(TableName::Rel),
        subject: Link::new(RecordId::new_v4(TableName::Entity)),
        object: Link::new(RecordId::new_v4(TableName::Entity)),
        rel_type: "works_at".into(),
        status: ClaimStatus::Approved,
        confidence: 0.8,
        validity_window: None,
        evidence: Vec::new(),
        temporal_audit: None,
    }
}
fn receipt() -> CanonicalCommitReceipt {
    CanonicalCommitReceipt {
        historical_import: None,
        space_id: "lineage-space".into(),
        commit_id: "actual-fixture-commit".into(),
        input_fingerprint: "input".into(),
        plan_fingerprint: "plan".into(),
        replay_output: None,
        source_ids: Vec::new(),
        publication_authority: None,
        maintenance_authority: Vec::new(),
        source_id: None,
        source_revision: None,
        effect_ids: Vec::new(),
        extension_ids: Vec::new(),
        outbox_ids: Vec::new(),
        committed_at: chrono::Utc::now(),
    }
}

#[test]
fn public_receipt_roundtrip_does_not_expose_private_original_graph() {
    let row = CanonicalRecord::Rel(Box::new(rel()));
    let lineage = ContactLineage {
        version: 1,
        effects: vec![CanonicalEffect {
            before: Some(row.clone()),
            after: Some(row),
        }],
        dependencies: Vec::new(),
        input_support: Vec::new(),
        roots: Vec::new(),
        original_payloads: vec![("never-persist-key".into(), "never-persist-raw".into())],
    };
    let public = receipt();
    let stored = stored_receipt(&public, Some(&lineage)).unwrap();
    assert!(stored.get(LINEAGE_FIELD).is_some());
    assert!(!serde_json::to_string(&stored)
        .unwrap()
        .contains("never-persist"));
    let decoded: CanonicalCommitReceipt = serde_json::from_value(stored).unwrap();
    assert_eq!(decoded, public);
    assert!(serde_json::to_value(decoded)
        .unwrap()
        .get(LINEAGE_FIELD)
        .is_none());
}

#[test]
fn missing_historical_pointer_and_projection_remain_unknown() {
    let support: SourceEffectSupport = serde_json::from_value(serde_json::json!({
        "space_id":"lineage-space", "source_id":RecordId::new_v4(TableName::Source),
        "source_revision":"same-revision", "record_id":RecordId::new_v4(TableName::Rel),
        "independent":true, "record_fingerprint":"original-fp"
    }))
    .unwrap();
    assert_eq!(support.last_commit_id, None);
    let lineage: ContactLineage = serde_json::from_value(
        serde_json::json!({"version":1,"effects":[],"dependencies":[],"input_support":[support]}),
    )
    .unwrap();
    assert!(lineage.roots.is_empty());
}

#[test]
fn changed_identity_needs_actual_redirect_and_preserves_policy() {
    let before = rel();
    let mut after = before.clone();
    after.base.id = RecordId::new_v4(TableName::Rel);
    after.subject = Link::new(RecordId::new_v4(TableName::Entity));
    after.status = ClaimStatus::Superseded;
    let mut redirect = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "redirect".into(),
    );
    redirect.base.id = before.subject.id().clone();
    redirect.status = EntityStatus::Merged;
    redirect.redirect_to = Some(after.subject.clone());
    let mut target = Entity::new(
        base(TableName::Entity),
        EntityType::new("person"),
        "terminal".into(),
    );
    target.base.id = after.subject.id().clone();
    assert!(!CanonicalRepository::contact_rel_fact_preserved(&before, &after, &[]).unwrap());
    assert!(CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[redirect.clone(), target.clone()]
    )
    .unwrap());
    let mut private_target = target;
    private_target.base.security.level = 9;
    assert!(!CanonicalRepository::contact_rel_fact_preserved(
        &before,
        &after,
        &[redirect.clone(), private_target]
    )
    .unwrap());
    assert!(
        !CanonicalRepository::contact_rel_fact_preserved(&before, &after, &[redirect.clone()])
            .unwrap()
    );
    after.rel_type = "different_fact".into();
    assert!(
        !CanonicalRepository::contact_rel_fact_preserved(&before, &after, &[redirect.clone()])
            .unwrap()
    );
    after.rel_type = before.rel_type.clone();
    redirect.redirect_to = Some(Link::new(redirect.base.id.clone()));
    assert!(
        !CanonicalRepository::contact_rel_fact_preserved(&before, &after, &[redirect]).unwrap()
    );
    let mut restricted = before.clone();
    restricted.base.security.level = 4;
    assert!(!CanonicalRepository::contact_rel_fact_preserved(&restricted, &before, &[]).unwrap());
    let mut foreign = before.clone();
    foreign.base.space_id = "foreign".into();
    assert!(!CanonicalRepository::contact_rel_fact_preserved(&before, &foreign, &[]).unwrap());
}

async fn client(embedded: bool) -> StorageClient {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        client
            .query_checked(concat!(
                include_str!("../../migrations/sql/031_source_publication.surql"),
                "\n",
                include_str!("../../migrations/sql/046_source_support_index_fence.surql")
            ))
            .await
            .unwrap();
        client
            .query_checked(include_str!(
                "../../migrations/sql/039_private_reference_fence.surql"
            ))
            .await
            .unwrap();
    }
    crate::SpaceConfigRepository::new(client.clone())
        .create(SpaceConfig {
            base: base(TableName::SpaceConfig),
            embedding_provider: "fake".into(),
            embedding_model: "fixture".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await
        .unwrap();
    client
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn canonical_receipt_binds_actual_effect_and_fault(embedded: bool) {
    let client = client(embedded).await;
    let mut row = rel();
    for (id, name) in [(row.subject.id(), "subject"), (row.object.id(), "object")] {
        let mut entity = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            name.into(),
        );
        entity.base.id = id.clone();
        crate::EntityRepository::new(client.clone())
            .create(entity)
            .await
            .unwrap();
    }
    row = crate::RelRepository::new(client.clone())
        .create(row)
        .await
        .unwrap();
    let mut after = row.clone();
    after.status = ClaimStatus::Superseded;
    let repository = CanonicalRepository::new(client.clone());
    let request = CanonicalCommit {
        space_id: "lineage-space".into(),
        commit_id: "actual-independent-status".into(),
        input_fingerprint: "original-status-request".into(),
        replay_output: None,
        publication: None,
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: repository.epoch("lineage-space").await.unwrap(),
            effects: vec![CanonicalEffect {
                before: Some(CanonicalRecord::Rel(Box::new(row.clone()))),
                after: Some(CanonicalRecord::Rel(Box::new(after.clone()))),
            }],
            ..CanonicalPlan::default()
        },
    };
    assert!(repository
        .commit_with_fault(request.clone(), CanonicalFault::BeforeReceipt)
        .await
        .is_err());
    assert_eq!(
        crate::RelRepository::new(client.clone())
            .get("lineage-space", &row.base.id)
            .await
            .unwrap(),
        Some(row.clone())
    );
    assert!(repository
        .receipt("lineage-space", &request.commit_id)
        .await
        .unwrap()
        .is_none());
    let committed = repository.commit(request.clone()).await.unwrap();
    assert_eq!(
        crate::RelRepository::new(client.clone())
            .get("lineage-space", &row.base.id)
            .await
            .unwrap(),
        Some(after.clone())
    );
    let raw: serde_json::Value = SourcePublicationRepository::new(client.clone())
        .read_control(
            "source_commit_receipt",
            &stable_key(&["lineage-space", &request.commit_id]),
            "lineage-space",
        )
        .await
        .unwrap()
        .unwrap();
    let private: ContactLineage = serde_json::from_value(raw[LINEAGE_FIELD].clone()).unwrap();
    assert_eq!(private.effects.len(), 1);
    assert_eq!(
        private.effects[0]
            .before
            .as_ref()
            .unwrap()
            .fingerprint()
            .unwrap(),
        CanonicalRecord::Rel(Box::new(row)).fingerprint().unwrap()
    );
    assert_eq!(
        private.effects[0]
            .after
            .as_ref()
            .unwrap()
            .fingerprint()
            .unwrap(),
        CanonicalRecord::Rel(Box::new(after)).fingerprint().unwrap()
    );
    assert!(private.roots.is_empty());
    assert!(serde_json::to_value(committed)
        .unwrap()
        .get(LINEAGE_FIELD)
        .is_none());
}

#[tokio::test]
async fn embedded_canonical_lineage_receipt_is_atomic_and_private() {
    canonical_receipt_binds_actual_effect_and_fault(true).await;
}
#[tokio::test]
async fn surreal_canonical_lineage_receipt_is_atomic_and_private() {
    canonical_receipt_binds_actual_effect_and_fault(false).await;
}

#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn raw_control_guard_is_exact_without_serde_default_reencoding(embedded: bool) {
    let client = client(embedded).await;
    let owner = RecordId::new_v4(TableName::Source);
    // Explicit legacy control fixture, not a source-completion or fact proof.
    let old = serde_json::json!({"space_id":"lineage-space","source_id":owner,"source_revision":"r","record_id":RecordId::new_v4(TableName::Rel),"independent":false,"record_fingerprint":"fp"});
    let key = "legacy-payload-boundary";
    async fn put(client: &StorageClient, key: &str, owner: &RecordId, value: &serde_json::Value) {
        if let Some(embedded) = client.embedded_store() {
            embedded
                .mutate(|tx| {
                    crate::source_publication::write_control_tx(
                        tx,
                        "source_effect_support",
                        key,
                        "lineage-space",
                        owner,
                        value,
                    )
                })
                .await
                .unwrap();
        } else {
            let bindings = crate::bindings! {"id"=>control_thing("source_effect_support",key),"row"=>crate::source_publication::control_value("lineage-space",owner,value).unwrap()};
            client
                .query_bind_checked("UPSERT $id CONTENT $row;", bindings)
                .await
                .unwrap();
        }
    }
    put(&client, key, &owner, &old).await;
    let mut guards = OriginalControls::default();
    let decoded: SourceEffectSupport = guards
        .read(&client, "lineage-space", "source_effect_support", key)
        .await
        .unwrap()
        .unwrap();
    assert!(decoded.last_commit_id.is_none());
    async fn check(client: &StorageClient, guards: &OriginalControls) -> StorageResult<()> {
        if let Some(embedded) = client.embedded_store() {
            return embedded
                .mutate(|tx| guards.check_tx(tx, "lineage-space"))
                .await;
        }
        let mut sql = "BEGIN TRANSACTION;\n".to_owned();
        let mut bindings = BTreeMap::new();
        guards.append_guard_sql(
            &mut sql,
            &mut bindings,
            "lineage-space",
            "continuation_guard",
        );
        sql.push_str("COMMIT TRANSACTION;");
        client.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
    check(&client, &guards).await.unwrap();
    if !embedded {
        // Exercise the real shared writer revision. No payload row UPDATE is
        // needed, including for schemafull or restored historical controls.
        async fn revision(client: &StorageClient) -> u64 {
            let mut response = client.query_bind_checked(
                "SELECT VALUE revision FROM $id;",
                crate::bindings! { "id" => control_thing("source_erasure_guard", "lineage-space") },
            ).await.unwrap();
            let values: Vec<u64> = response.take(0).unwrap();
            assert_eq!(values.len(), 1);
            values[0]
        }
        let first = revision(&client).await;
        check(&client, &guards).await.unwrap();
        assert!(revision(&client).await > first);
        let actual: serde_json::Value = SourcePublicationRepository::new(client.clone())
            .read_control("source_effect_support", key, "lineage-space")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            actual, old,
            "private conflict metadata must not change the payload"
        );
    }
    let mut changed = old;
    changed["record_fingerprint"] = "changed".into();
    put(&client, key, &owner, &changed).await;
    assert!(check(&client, &guards).await.is_err());
}
#[tokio::test]
async fn embedded_contact_continuation_checks_original_raw_controls() {
    raw_control_guard_is_exact_without_serde_default_reencoding(true).await;
}
#[tokio::test]
async fn surreal_contact_continuation_checks_original_raw_controls() {
    raw_control_guard_is_exact_without_serde_default_reencoding(false).await;
}

#[test]
fn survival_and_continuation_original_guards_keep_distinct_bindings() {
    let mut survival = OriginalControls::default();
    survival.values.insert(
        ("source_operation".into(), "surviving-B".into()),
        Some("original-B-payload".into()),
    );
    let mut continuation = OriginalControls::default();
    continuation
        .values
        .insert(("source_commit_receipt".into(), "new-receipt".into()), None);
    let mut bindings = BTreeMap::new();
    let mut sql = String::new();
    survival.append_guard_sql(&mut sql, &mut bindings, "space", "survival_guard");
    continuation.append_guard_sql(&mut sql, &mut bindings, "space", "continuation_guard");
    assert_eq!(
        bindings.get("survival_guard_0_payload"),
        Some(&Value::from("original-B-payload"))
    );
    assert_eq!(
        bindings.get("continuation_guard_0_payload"),
        Some(&Value::None)
    );
    assert!(sql.contains("$survival_guard_space"));
    assert!(sql.contains("SELECT * FROM ONLY $continuation_guard_0_id"));
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn identity_only_commit_captures_actual_lineage(embedded: bool) {
    use elephant_types::records::{identity::IdentityKeyStatus, EntityIdentityKey};
    let client = client(embedded).await;
    let person = crate::EntityRepository::new(client.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "Identity owner".into(),
        ))
        .await
        .unwrap();
    let key = EntityIdentityKey {
        base: base(TableName::EntityIdentityKey),
        key: "owner@example.invalid".into(),
        entity: Link::new(person.base.id.clone()),
        confidence: 1.0,
        first_seen: None,
        last_seen: None,
        evidence: Vec::new(),
        status: IdentityKeyStatus::Active,
    };
    let repo = CanonicalRepository::new(client.clone());
    let request = CanonicalCommit {
        space_id: "lineage-space".into(),
        commit_id: "identity-only-private-lineage".into(),
        input_fingerprint: "original-key-intent".into(),
        replay_output: None,
        publication: None,
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: repo.epoch("lineage-space").await.unwrap(),
            dependencies: vec![CanonicalRecord::Entity(Box::new(person))],
            effects: vec![CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::EntityIdentityKey(Box::new(key.clone()))),
            }],
            ..CanonicalPlan::default()
        },
    };
    assert!(repo
        .commit_with_fault(request.clone(), CanonicalFault::BeforeReceipt)
        .await
        .is_err());
    assert!(
        CanonicalRecord::load(&client, "lineage-space", &key.base.id)
            .await
            .unwrap()
            .is_none()
    );
    repo.commit(request.clone()).await.unwrap();
    let stored: serde_json::Value = SourcePublicationRepository::new(client)
        .read_control(
            "source_commit_receipt",
            &stable_key(&["lineage-space", &request.commit_id]),
            "lineage-space",
        )
        .await
        .unwrap()
        .unwrap();
    let lineage: ContactLineage = serde_json::from_value(stored[LINEAGE_FIELD].clone()).unwrap();
    assert_eq!(lineage.effects.len(), 1);
    assert!(
        matches!(&lineage.effects[0].after,Some(CanonicalRecord::EntityIdentityKey(row)) if row.base.id==key.base.id)
    );
    assert!(
        lineage.roots.is_empty(),
        "ordinary identity creation grants no contact root"
    );
}
#[tokio::test]
async fn embedded_identity_only_receipt_keeps_actual_lineage_and_rollback() {
    identity_only_commit_captures_actual_lineage(true).await;
}
#[tokio::test]
async fn surreal_identity_only_receipt_keeps_actual_lineage_and_rollback() {
    identity_only_commit_captures_actual_lineage(false).await;
}

#[path = "source_contact_replacement_tests.rs"]
mod retained_receipts;
