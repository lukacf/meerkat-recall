//! Actual storage mutation between original epoch capture and final dispatch
//! verification. No concurrent timing race, provider call or synthetic epoch.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use super::*;
use crate::scheduler::execution_contract::{
    classify_failure, retry_action, FailureClass, RetryAction,
};
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::{EntityRepository, WorkItemRepository};
use elephant_types::records::entity::{Entity, EntityType};

async fn unrelated_write_refuses_then_revalidates(embedded: bool) {
    let (storage, source, original_work, _, _) =
        Box::pin(super::standing_dispatch_tests::fixture(embedded)).await;
    let space = original_work.base.space_id.as_str();
    let canonical = CanonicalRepository::new(storage.clone());
    Box::pin(guard_physical_dispatch(&storage, &original_work))
        .await
        .expect("original standing authority is valid");
    let captured = canonical.observe_epoch(space).await.unwrap();
    verify_dispatch_epoch(&canonical, space, captured.clone())
        .await
        .expect("unchanged observation permits final verification");

    // The epoch is space-wide: a real unrelated canonical write invalidates
    // this read window even though the original source and work remain intact.
    let unrelated = EntityRepository::new(storage.clone())
        .create(Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                space.into(),
                "unrelated-writer".into(),
            ),
            EntityType::new("place"),
            "Unrelated corpus record".into(),
        ))
        .await
        .expect("real unrelated entity write");
    let after_write = canonical.observe_epoch(space).await.unwrap();
    assert_ne!(
        captured, after_write,
        "real write must advance the original epoch"
    );
    let error = verify_dispatch_epoch(&canonical, space, captured)
        .await
        .expect_err("never dispatch using the obsolete observation");
    assert!(matches!(
        &error,
        PipelineError::StorageError(elephant_storage::StorageError::Conflict(_))
    ));
    let class = classify_failure(&error.into());
    assert_eq!(class, FailureClass::Transient);
    assert_eq!(
        retry_action(class, 1, 3, 5, 120),
        RetryAction::Retry { delay_secs: 5 }
    );
    assert_eq!(retry_action(class, 3, 3, 5, 120), RetryAction::Fail);
    assert_eq!(
        canonical.observe_epoch(space).await.unwrap(),
        after_write,
        "refusal neither advances nor adopts a canonical epoch"
    );
    assert_eq!(
        WorkItemRepository::new(storage.clone())
            .get(space, &original_work.base.id)
            .await
            .unwrap(),
        Some(original_work.clone()),
        "dispatch refusal does not change the original work or attempt"
    );
    assert!(EntityRepository::new(storage.clone())
        .get(space, &unrelated.base.id)
        .await
        .unwrap()
        .is_some());

    // A fresh complete read window can verify the same original authority;
    // this is not a license to accept a changed source or execution ceiling.
    Box::pin(guard_physical_dispatch(&storage, &original_work))
        .await
        .expect("fresh complete validation of unchanged original authority");
    let mut restricted = source.clone();
    restricted.base.security.level = 4;
    SourceRepository::new(storage.clone())
        .update(space, &source.base.id, restricted)
        .await
        .unwrap();
    assert!(
        Box::pin(guard_physical_dispatch(&storage, &original_work))
            .await
            .is_err(),
        "fresh epoch never bypasses the original standing source ceiling"
    );
}

#[tokio::test]
async fn embedded_unrelated_epoch_write_refuses_dispatch_with_bounded_retry() {
    Box::pin(unrelated_write_refuses_then_revalidates(true)).await;
}

#[tokio::test]
async fn native_unrelated_epoch_write_refuses_dispatch_with_bounded_retry() {
    Box::pin(unrelated_write_refuses_then_revalidates(false)).await;
}
