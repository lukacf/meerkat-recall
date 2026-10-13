//! Explicit trusted fixture admission through the actual atomic registration API.
use super::*;
use elephant_types::records::SourceWatch;

#[expect(
    clippy::default_trait_access,
    reason = "Keep the controlled watch registration fixture and inferred purpose type unchanged"
)]
pub(super) async fn register_local(
    storage: &StorageClient,
    watch: &SourceWatch,
) -> elephant_storage::standing_watch_repository::StandingWatchIdentity {
    let principal = elephant_policy::Principal {
        space_id: watch.base.space_id.clone(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        clearances: vec![elephant_policy::Clearance {
            policy_id: watch.base.security.policy_id.clone(),
            max_level: watch.base.security.level,
            labels: watch.base.security.labels.iter().cloned().collect(),
            handling_allow: watch.base.security.handling.iter().cloned().collect(),
        }],
        subject_allowlist: None,
        purpose: Default::default(),
    };
    let admission =
        elephant_auth::standing_watch::StandingWatchAdmission::trusted_local(&principal)
            .expect("explicit fixture trusted-local admission");
    let source = SourceRepository::new(storage.clone())
        .get(&watch.base.space_id, watch.source.id())
        .await
        .expect("actual source read")
        .expect("existing accepted source");
    let records = Box::pin(
        elephant_storage::source_watch_registration::PreparedSourceWatchRegistration::prepare(
            storage,
            &source,
            Some(watch),
            watch.clone(),
        ),
    )
    .await
    .expect("original actual Source/Doc/Watch inputs");
    let standing = Box::pin(
        elephant_storage::standing_watch_repository::StandingWatchRepository::new(storage.clone())
            .prepare_registration(watch, &admission),
    )
    .await
    .expect("explicit bounded standing registration");
    let (_, identity) = Box::pin(records.register(storage, &standing))
        .await
        .expect("atomic original input/delegation registration");
    identity
}
