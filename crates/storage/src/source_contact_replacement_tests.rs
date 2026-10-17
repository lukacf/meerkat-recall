//! Preparation controls for legacy, unproved support.
//!
//! The independent full
//! contact worker oracle tests actual completed A/B publications and plain B
//! replacement; these controls never manufacture a completed contact seal.

use super::*;
use crate::canonical_commit::CanonicalPublicationContext;
use elephant_types::publication::AttemptFence;

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn retained_receipts_do_not_skip_changed_support_preparation(embedded: bool) {
    let storage = client(embedded).await;
    let surviving_source = RecordId::new_v4(TableName::Source);
    let replacing_source = RecordId::new_v4(TableName::Source);
    let mut before = rel();
    before.base.provenance.sources = vec![surviving_source.clone(), replacing_source.clone()];
    for (id, name) in [
        (before.subject.id(), "subject"),
        (before.object.id(), "object"),
    ] {
        let mut entity = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            name.into(),
        );
        entity.base.id = id.clone();
        crate::EntityRepository::new(storage.clone())
            .create(entity)
            .await
            .unwrap();
    }
    before = crate::RelRepository::new(storage.clone())
        .create(before)
        .await
        .unwrap();
    let original = SourceEffectSupport {
        space_id: "lineage-space".into(),
        source_id: surviving_source.clone(),
        source_revision: "original-unproved-legacy-revision".into(),
        record_id: before.base.id.clone(),
        historical_unknown: None,
        independent: false,
        record_fingerprint: CanonicalRecord::Rel(Box::new(before.clone()))
            .fingerprint()
            .unwrap(),
        last_commit_id: Some("unproved-legacy-writer".into()),
    };
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| crate::canonical_commit::write_existing_support_tx(tx, &original))
            .await
            .unwrap();
    } else {
        let mut sql = String::new();
        let mut bindings = BTreeMap::new();
        crate::canonical_commit::append_existing_support_sql(
            &mut sql,
            &mut bindings,
            "legacy",
            &original,
        )
        .unwrap();
        storage.query_bind_checked(&sql, bindings).await.unwrap();
    }
    let canonical = CanonicalRepository::new(storage.clone());
    let original_epoch = canonical.epoch("lineage-space").await.unwrap();
    let mut after = before.clone();
    after.base.provenance.sources = vec![surviving_source];
    assert!(!CanonicalRepository::contact_rel_fact_preserved(&before, &after, &[]).unwrap());
    let after_record = CanonicalRecord::Rel(Box::new(after));
    // This request is ONLY passed to preparation, never to commit. Its synthetic
    // fence grants no authority, and no SourceHead/completion/contact is seeded.
    let mut request = CanonicalCommit {
        space_id: "lineage-space".into(),
        commit_id: "uncommitted-replacement-preparation".into(),
        input_fingerprint: "original-preparation-control".into(),
        replay_output: None,
        publication: Some(CanonicalPublicationContext {
            source: PublicationFence {
                source_id: replacing_source,
                generation: "uncommitted-generation".into(),
                revision_id: "same-revision".into(),
                operation_id: "uncommitted-operation".into(),
            },
            attempt: AttemptFence {
                space_id: "lineage-space".into(),
                work_item_id: RecordId::new_v4(TableName::WorkItem),
                generation: 1,
                lease_token: "unclaimed-preparation-control".into(),
            },
        }),
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: original_epoch.clone(),
            effects: vec![CanonicalEffect {
                before: Some(CanonicalRecord::Rel(Box::new(before.clone()))),
                after: Some(after_record.clone()),
            }],
            ..CanonicalPlan::default()
        },
    };
    let prepared = prepare_replacement_continuations(&storage, &mut request, &[])
        .await
        .unwrap()
        .expect("changed survivor requires preparation even with no retired receipt");
    assert!(
        prepared.canonical_roots.is_empty(),
        "unproved support cannot manufacture a contact root"
    );
    assert!(
        prepared.receipts.is_empty(),
        "preparation cannot fabricate an independent receipt"
    );
    assert_eq!(prepared.support_updates.len(), 1);
    let update = &prepared.support_updates[0];
    assert_eq!(
        update.record_fingerprint,
        after_record.fingerprint().unwrap()
    );
    assert_eq!(update.source_id, original.source_id);
    assert_eq!(update.source_revision, original.source_revision);
    assert_eq!(
        update.last_commit_id, None,
        "unknown original authority must remain unknown"
    );
    assert_eq!(
        request.plan.effects.len(),
        1,
        "retained writers require no invented no-op effects"
    );
    assert_eq!(
        canonical.epoch("lineage-space").await.unwrap(),
        original_epoch
    );
    assert_eq!(
        crate::RelRepository::new(storage.clone())
            .get("lineage-space", &before.base.id)
            .await
            .unwrap(),
        Some(before)
    );
    let persisted: SourceEffectSupport = SourcePublicationRepository::new(storage.clone())
        .read_control(
            "source_effect_support",
            &support_key(&original),
            "lineage-space",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(persisted, original, "preparation is read-only");
    assert!(canonical
        .receipt("lineage-space", &request.commit_id)
        .await
        .unwrap()
        .is_none());

    crate::EntityRepository::new(storage.clone())
        .create(Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            "competing writer".into(),
        ))
        .await
        .unwrap();
    assert!(
        prepare_replacement_continuations(&storage, &mut request, &[])
            .await
            .is_err()
    );
    assert_eq!(
        request.plan.observed_epoch, original_epoch,
        "never recapture a newer authority"
    );
}

#[tokio::test]
async fn embedded_retained_receipts_prepare_changed_support_without_inventing_authority() {
    retained_receipts_do_not_skip_changed_support_preparation(true).await;
}

#[tokio::test]
async fn surreal_retained_receipts_prepare_changed_support_without_inventing_authority() {
    retained_receipts_do_not_skip_changed_support_preparation(false).await;
}
