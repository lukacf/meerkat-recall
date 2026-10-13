//! Deterministic commit-time epoch races, with real canonical transactions.
//! These controls do not require a provider or scheduler timing coincidence.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use super::*;
use crate::scheduler::execution_contract::{
    classify_failure, retry_action, FailureClass, RetryAction,
};
use elephant_storage::{EntityRepository, SpaceConfigRepository};
use elephant_types::records::{entity::EntityType, BaseFields, Entity, SpaceConfig};
use elephant_types::TableName;

const SPACE: &str = "finalizer-commit-contention";

async fn storage(embedded: bool) -> StorageClient {
    let storage = if embedded {
        StorageClient::new_embedded().await.unwrap()
    } else {
        let storage = StorageClient::new_memory().await.unwrap();
        storage
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .unwrap();
        storage
    };
    let config: SpaceConfig = serde_json::from_value(serde_json::json!({
        "id": RecordId::new_v4(TableName::SpaceConfig), "space_id": SPACE,
        "created_at": Utc::now(), "updated_at": Utc::now(), "created_by": "fixture",
        "security": {"policy_id": "default", "level": 0, "labels": [], "handling": []},
        "provenance": {}, "subjects": [], "subjects_state": "complete",
        "embedding_provider": "fake", "embedding_model": "fixture", "embedding_dims": 8
    }))
    .unwrap();
    SpaceConfigRepository::new(storage.clone())
        .create(config)
        .await
        .unwrap();
    storage
}

fn entity(label: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "fixture".into(),
        ),
        EntityType::new("project"),
        label.into(),
    )
}

async fn prepare(
    storage: &StorageClient,
    repository: &CanonicalRepository,
    entity: &Entity,
    commit_id: &str,
) -> CanonicalCommit {
    // Capture the epoch before every input read, including the negative read.
    // Repreparation calls this again; it never refreshes an already-read plan.
    let mut plan = CanonicalPlan {
        observed_epoch: repository.epoch(SPACE).await.unwrap(),
        ..CanonicalPlan::default()
    };
    let before = EntityRepository::new(storage.clone())
        .get(SPACE, &entity.base.id)
        .await
        .unwrap();
    assert!(before.is_none());
    plan.push(
        None,
        Some(CanonicalRecord::Entity(Box::new(entity.clone()))),
    )
    .unwrap();
    CanonicalCommit {
        space_id: SPACE.into(),
        commit_id: commit_id.into(),
        input_fingerprint: format!("fixed-input-{commit_id}"),
        replay_output: None,
        publication: None,
        maintenance: None,
        finalization: None,
        plan,
    }
}

async fn commit_time_contention_reprepares_without_publishing_stale_effects(embedded: bool) {
    let storage = storage(embedded).await;
    let repository = CanonicalRepository::new(storage.clone());
    let pending = entity("pending source effect");
    let competing = entity("independent canonical writer");
    let stale = Box::pin(prepare(&storage, &repository, &pending, "pending")).await;
    let winner = Box::pin(prepare(&storage, &repository, &competing, "competing")).await;
    assert_eq!(stale.plan.observed_epoch, winner.plan.observed_epoch);
    let original_epoch = stale.plan.observed_epoch.clone();

    Box::pin(repository.commit(winner)).await.unwrap();
    let after_competing_commit = repository.epoch(SPACE).await.unwrap();
    assert_ne!(original_epoch, after_competing_commit);
    let refused = Box::pin(repository.commit(stale)).await.unwrap_err();
    assert!(
        matches!(&refused, elephant_storage::StorageError::Conflict(code)
        if code == "canonical_inputs_changed")
    );
    // A late refused plan has no partial effect, receipt or extra epoch write.
    assert!(EntityRepository::new(storage.clone())
        .get(SPACE, &pending.base.id)
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .receipt(SPACE, "pending")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        repository.epoch(SPACE).await.unwrap(),
        after_competing_commit
    );
    assert!(EntityRepository::new(storage.clone())
        .get(SPACE, &competing.base.id)
        .await
        .unwrap()
        .is_some());

    // Use the actual storage refusal, not a synthetic spelling of the error.
    let adapted = execution_error(refused.into(), false);
    assert_eq!(
        classify_failure(&adapted),
        FailureClass::PendingDependencies
    );
    assert_eq!(
        retry_action(classify_failure(&adapted), 3, 3, 10, 120),
        RetryAction::Blocked { delay_secs: 5 }
    );

    let fresh = Box::pin(prepare(&storage, &repository, &pending, "pending")).await;
    assert_eq!(fresh.plan.observed_epoch, after_competing_commit);
    let receipt = Box::pin(repository.commit(fresh)).await.unwrap();
    assert_eq!(receipt.effect_ids, vec![pending.base.id.clone()]);
    assert_eq!(receipt.commit_id, "pending");
    assert!(EntityRepository::new(storage.clone())
        .get(SPACE, &pending.base.id)
        .await
        .unwrap()
        .is_some());
    assert_eq!(
        repository
            .receipt(SPACE, "pending")
            .await
            .unwrap()
            .unwrap()
            .commit_id,
        receipt.commit_id
    );
}

#[tokio::test]
async fn embedded_commit_epoch_conflict_requires_reprepare_without_budget_exhaustion() {
    Box::pin(commit_time_contention_reprepares_without_publishing_stale_effects(true)).await;
}

#[tokio::test]
async fn surreal_commit_epoch_conflict_requires_reprepare_without_budget_exhaustion() {
    Box::pin(commit_time_contention_reprepares_without_publishing_stale_effects(false)).await;
}
