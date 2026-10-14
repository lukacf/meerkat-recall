#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::retrieval_test_support::QueryFixture;
use elephant_policy::{Clearance, Principal};
use elephant_storage::StorageClient;
use std::collections::HashSet;
use tokio::{sync::Semaphore, time::timeout};

tokio::task_local! {
    static OBSERVED_EPOCHS: Arc<Semaphore>;
}

// Observe actual per-request capture, before cache lookup. This test-only signal
// avoids assuming that a sleeping scheduler has polled both joined requests.
pub(super) fn epoch_observed() {
    let _ = OBSERVED_EPOCHS.try_with(|observed| observed.add_permits(1));
}

fn context(storage: StorageClient, space: &str) -> ToolContext {
    ToolContext::new(
        storage,
        Principal {
            space_id: space.into(),
            scopes: HashSet::from(["read:doc:raw".into()]),
            clearances: vec![Clearance {
                policy_id: "default".into(),
                max_level: 0,
                labels: HashSet::new(),
                handling_allow: HashSet::new(),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        },
    )
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the configuration repository through the complete approval-revocation update."
)]
async fn set_revoked(ctx: &ToolContext, revoked: bool) {
    let repo = SpaceConfigRepository::new(ctx.storage.clone());
    let observed = repo
        .get_by_space_id(&ctx.principal.space_id)
        .await
        .unwrap()
        .unwrap();
    let mut proposed = observed.clone();
    let processing = proposed.processing.as_mut().unwrap();
    processing.generation += 1;
    processing
        .bindings
        .get_mut(&ModelOperation::QueryEmbedding)
        .unwrap()
        .revoked = revoked;
    repo.replace_authorized(&observed, proposed).await.unwrap();
}

#[test]
fn bounded_entries_bytes_ttl_and_store_lifetime() {
    let mut cache = Cache::default();
    let owner = Arc::new(VerifiedLocalModelCache::default());
    let now = Instant::now();
    let mut held = Vec::new();
    for index in 0..MAX_ENTRIES {
        let mut key = [0; 32];
        key[..8].copy_from_slice(&u64::try_from(index).unwrap().to_le_bytes());
        held.push(cache.entry(key, &owner, 3, now).unwrap());
    }
    assert!(cache.entry([255; 32], &owner, 3, now).is_none());
    assert_eq!(cache.entries.len(), MAX_ENTRIES);
    held.pop();
    assert!(cache.entry([255; 32], &owner, 3, now).is_some());
    drop(held);
    assert!(cache.entry([254; 32], &owner, usize::MAX, now).is_none());
    let later = now + TTL;
    let replacement = cache.entry([254; 32], &owner, 3, later).unwrap();
    assert_eq!(cache.entries.len(), 1);
    let filled_at = later + Duration::from_secs(1);
    assert!(replacement
        .value
        .set(Value {
            vector: vec![1.0, 0.0, 0.0].into_boxed_slice(),
            filled_at,
        })
        .is_ok());
    assert!(replacement.fresh(later + TTL));
    assert!(!replacement.fresh(filled_at + TTL));
    drop(replacement);
    drop(owner);
    let other_owner = Arc::new(VerifiedLocalModelCache::default());
    cache.entry([253; 32], &other_owner, 3, later).unwrap();
    assert_eq!(cache.entries.len(), 1);

    let mut cache = Cache::default();
    let dims = (MAX_RESERVED_BYTES - ENTRY_OVERHEAD) / size_of::<f32>();
    let held = cache.entry([0; 32], &other_owner, dims, now).unwrap();
    assert!(cache.entry([1; 32], &other_owner, 3, now).is_none());
    assert_eq!(
        cache
            .entries
            .values()
            .map(|entry| entry.reserved_bytes)
            .sum::<usize>(),
        MAX_RESERVED_BYTES
    );
    drop(held);
    assert!(cache.entry([1; 32], &other_owner, 3, now).is_some());
    assert_eq!(cache.entries.len(), 1);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the shared-fill entry scope through failure, cancellation and cache-state assertions."
)]
async fn failed_and_cancelled_shared_fills_never_install_success() {
    let mut cache = Cache::default();
    let owner = Arc::new(VerifiedLocalModelCache::default());
    let entry = cache.entry([0; 32], &owner, 3, Instant::now()).unwrap();
    let failure = entry
        .value
        .get_or_try_init(|| async { Err::<Value, _>("controlled failure") })
        .await;
    assert!(failure.is_err() && entry.value.get().is_none());
    let started = Arc::new(Semaphore::new(0));
    let signal = started.clone();
    let filling = entry.clone();
    let task = tokio::spawn(async move {
        filling
            .value
            .get_or_try_init(|| async {
                signal.add_permits(1);
                std::future::pending::<Result<Value, &'static str>>().await
            })
            .await
            .map(|value| value.vector.to_vec())
    });
    timeout(Duration::from_secs(1), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let waiting = cache.entry([0; 32], &owner, 3, Instant::now()).unwrap();
    assert!(Arc::ptr_eq(&entry, &waiting));
    let waiter = tokio::spawn(async move {
        waiting
            .value
            .get_or_try_init(|| async {
                Ok::<_, &'static str>(Value {
                    vector: vec![1.0, 0.0, 0.0].into_boxed_slice(),
                    filled_at: Instant::now(),
                })
            })
            .await
            .map(|value| value.vector.to_vec())
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(
        timeout(Duration::from_secs(1), waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        vec![1.0, 0.0, 0.0]
    );
    let value = entry
        .value
        .get_or_try_init(|| async { Err::<Value, _>("ready value must avoid refill") })
        .await
        .unwrap();
    assert_eq!(value.vector.as_ref(), &[1.0, 0.0, 0.0]);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Keep the complete query partition and epoch-reuse control with its original fixture lifetimes."
)]
async fn exact_query_caller_profile_space_partition_and_fresh_epoch_reuse() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let fixture = QueryFixture::install(
        &storage,
        "query-a",
        vec![
            ("original", vec![1.0, 0.0, 0.0]),
            ("original ", vec![0.0, 1.0, 0.0]),
        ],
    )
    .await;
    let mut ctx = context(storage.clone(), "query-a");
    for _ in 0..2 {
        assert_eq!(
            fixture
                .run(embed(&ctx, "query-a", "original"))
                .await
                .unwrap()
                .1,
            vec![1.0, 0.0, 0.0]
        );
    }
    assert_eq!(fixture.physical_requests(), 1);
    fixture
        .run(embed(&ctx, "query-a", "original "))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 2);
    ctx.principal.purpose.insert("changed-purpose".into());
    fixture
        .run(embed(&ctx, "query-a", "original"))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 3);
    ctx.principal.purpose.clear();
    fixture
        .run(embed(&ctx, "query-a", "original"))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 3);
    // Installing SpaceConfig already advances the embedded canonical epoch.
    // Reading that existing token neither changes it nor causes a cache miss.
    let canonical = CanonicalRepository::new(storage.clone());
    let installed_epoch = canonical.observe_epoch("query-a").await.unwrap();
    assert!(installed_epoch.is_some());
    assert_eq!(
        Some(canonical.epoch("query-a").await.unwrap()),
        installed_epoch
    );
    fixture
        .run(embed(&ctx, "query-a", "original"))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 3);
    // Restore rotates read authority. The identical caller vector may survive,
    // but this request must capture and check the new epoch independently.
    elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
        .rotate_canonical_epochs()
        .await
        .unwrap();
    assert_ne!(
        canonical.observe_epoch("query-a").await.unwrap(),
        installed_epoch
    );
    fixture
        .run(embed(&ctx, "query-a", "original"))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 3);
    let repo = SpaceConfigRepository::new(storage.clone());
    let observed = repo.get_by_space_id("query-a").await.unwrap().unwrap();
    let mut proposed = observed.clone();
    let processing = proposed.processing.as_mut().unwrap();
    processing.generation += 1;
    processing
        .connections
        .get_mut("connection")
        .unwrap()
        .credential_generation += 1;
    repo.replace_authorized(&observed, proposed).await.unwrap();
    fixture
        .run(embed(&ctx, "query-a", "original"))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 4);
    set_revoked(&ctx, true).await;
    assert_eq!(
        fixture
            .run(embed(&ctx, "query-a", "original"))
            .await
            .unwrap_err()
            .error_code,
        ErrorCode::ProviderUnapproved
    );
    assert_eq!(fixture.physical_requests(), 4);
    let other =
        QueryFixture::install(&storage, "query-b", vec![("original", vec![0.0, 0.0, 1.0])]).await;
    let other_ctx = context(storage, "query-b");
    assert_eq!(
        other
            .run(embed(&other_ctx, "query-b", "original"))
            .await
            .unwrap()
            .1,
        vec![0.0, 0.0, 1.0]
    );
    assert_eq!(other.physical_requests(), 1);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Keep the complete authority-key control with its original contexts and caller lifetimes."
)]
async fn key_preserves_authority_and_normalizes_only_unordered_sets() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let fixture =
        QueryFixture::install(&storage, "query-key", vec![("query", vec![1.0, 0.0, 0.0])]).await;
    let mut ctx = context(storage.clone(), "query-key");
    let binding = SpaceConfigRepository::new(storage.clone())
        .authorize_model_operation("query-key", ModelOperation::QueryEmbedding, None)
        .await
        .unwrap();
    let owner = storage.local_models();
    ctx.principal.scopes = HashSet::from(["read:doc:raw".into(), "read:entity".into()]);
    let first = key(
        &ctx,
        "query-key",
        "query",
        &fixture.policy,
        &binding.provenance,
        &owner,
    )
    .unwrap();
    ctx.principal.scopes = HashSet::from(["read:entity".into(), "read:doc:raw".into()]);
    assert_eq!(
        first,
        key(
            &ctx,
            "query-key",
            "query",
            &fixture.policy,
            &binding.provenance,
            &owner
        )
        .unwrap()
    );
    let other_owner = Arc::new(VerifiedLocalModelCache::default());
    assert_ne!(
        first,
        key(
            &ctx,
            "query-key",
            "query",
            &fixture.policy,
            &binding.provenance,
            &other_owner
        )
        .unwrap()
    );
    let mut different_profile = binding.provenance.clone();
    different_profile.profile_id = "other-profile".into();
    assert_ne!(
        first,
        key(
            &ctx,
            "query-key",
            "query",
            &fixture.policy,
            &different_profile,
            &owner
        )
        .unwrap()
    );
    let now = chrono::Utc::now().timestamp();
    let mut claims = elephant_auth::Claims {
        sub: "first-caller".into(),
        exp: now + 3600,
        iat: now,
        iss: None,
        aud: None,
        space_id: Some("query-key".into()),
        spaces: None,
        bootstrap_space: None,
        scopes: vec!["read:doc:raw".into()],
        security_level: 0,
        security_labels: vec![],
        clearances: ctx.principal.clearances.clone(),
        subject_allowlist: None,
        purpose: vec![],
    };
    let verified = |claims: &elephant_auth::Claims| {
        let secret = "query-vector-local-fixture-secret";
        let token = jsonwebtoken::encode(
            &jsonwebtoken::Header::default(),
            claims,
            &jsonwebtoken::EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap();
        elephant_auth::LegacyVerifier {
            secret,
            algorithm: jsonwebtoken::Algorithm::HS256,
            audience: None,
            issuer: None,
            mappings: None,
        }
        .verify(&token)
        .unwrap()
    };
    let first_caller = ToolContext::new(storage.clone(), ctx.principal.clone())
        .with_verified_caller(Some(verified(&claims)));
    claims.sub = "second-caller".into();
    let second_caller = ToolContext::new(storage.clone(), ctx.principal.clone())
        .with_verified_caller(Some(verified(&claims)));
    let first_verified_key = key(
        &first_caller,
        "query-key",
        "query",
        &fixture.policy,
        &binding.provenance,
        &owner,
    )
    .unwrap();
    assert_ne!(first, first_verified_key);
    assert_ne!(
        first_verified_key,
        key(
            &second_caller,
            "query-key",
            "query",
            &fixture.policy,
            &binding.provenance,
            &owner
        )
        .unwrap()
    );
    ctx.principal.subject_allowlist = Some(HashSet::new());
    assert_ne!(
        first,
        key(
            &ctx,
            "query-key",
            "query",
            &fixture.policy,
            &binding.provenance,
            &owner
        )
        .unwrap()
    );
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Keep the original unboxed waiter/revocation futures and storage lifetime through all assertions."
)]
async fn shared_fill_waiters_recheck_revocation_and_failed_fill_is_not_reused() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let fixture = QueryFixture::install_with_gate(
        &storage,
        "query-wait",
        vec![("query", vec![1.0, 0.0, 0.0])],
        Some(gate.clone()),
    )
    .await;
    let ctx = context(storage, "query-wait");
    let observed = Arc::new(Semaphore::new(0));
    fixture
        .run(OBSERVED_EPOCHS.scope(observed.clone(), async {
            let first = embed(&ctx, "query-wait", "query");
            let second = embed(&ctx, "query-wait", "query");
            let revoke = async {
                timeout(Duration::from_secs(1), async {
                    while fixture.physical_requests() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                timeout(Duration::from_secs(1), observed.acquire_many(2))
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                assert_eq!(fixture.physical_requests(), 1);
                set_revoked(&ctx, true).await;
                gate.add_permits(1);
            };
            let (a, b, ()) = timeout(Duration::from_secs(2), async {
                tokio::join!(first, second, revoke)
            })
            .await
            .unwrap();
            assert_eq!(a.unwrap_err().error_code, ErrorCode::ProviderUnapproved);
            assert_eq!(b.unwrap_err().error_code, ErrorCode::ProviderUnapproved);
        }))
        .await;
    assert_eq!(fixture.physical_requests(), 1);
    set_revoked(&ctx, false).await;
    gate.add_permits(1);
    timeout(
        Duration::from_secs(1),
        fixture.run(embed(&ctx, "query-wait", "query")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(fixture.physical_requests(), 2);
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Keep the original unboxed epoch-change future and storage lifetime through all waiter assertions."
)]
async fn epoch_change_during_fill_refuses_every_waiter_and_does_not_install_vector() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let fixture = QueryFixture::install_with_gate(
        &storage,
        "query-epoch-race",
        vec![("query", vec![1.0, 0.0, 0.0])],
        Some(gate.clone()),
    )
    .await;
    let ctx = context(storage.clone(), "query-epoch-race");
    let observed = Arc::new(Semaphore::new(0));
    fixture
        .run(OBSERVED_EPOCHS.scope(observed.clone(), async {
            let first = embed(&ctx, "query-epoch-race", "query");
            let second = embed(&ctx, "query-epoch-race", "query");
            let rotate = async {
                timeout(Duration::from_secs(1), async {
                    while fixture.physical_requests() == 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                timeout(Duration::from_secs(1), observed.acquire_many(2))
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                elephant_storage::source_publication::SourcePublicationRepository::new(storage)
                    .rotate_canonical_epochs()
                    .await
                    .unwrap();
                gate.add_permits(1);
            };
            let (first, second, ()) = timeout(Duration::from_secs(2), async {
                tokio::join!(first, second, rotate)
            })
            .await
            .unwrap();
            assert_eq!(first.unwrap_err().error_code, ErrorCode::NotReady);
            assert_eq!(second.unwrap_err().error_code, ErrorCode::NotReady);
        }))
        .await;
    assert_eq!(fixture.physical_requests(), 1);
    gate.add_permits(1);
    fixture
        .run(embed(&ctx, "query-epoch-race", "query"))
        .await
        .unwrap();
    assert_eq!(
        fixture.physical_requests(),
        2,
        "failed old-epoch fill was not retained"
    );
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep the original fixture owners through all authorization, result and state assertions in this control."
)]
async fn memo_hit_has_no_old_epoch_authority() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let fixture = QueryFixture::install(
        &storage,
        "query-hit-epoch",
        vec![("query", vec![1.0, 0.0, 0.0])],
    )
    .await;
    let ctx = context(storage.clone(), "query-hit-epoch");
    let (_, _, provenance) = fixture
        .run(
            ProviderDispatchContext::new(
                SpaceConfigRepository::new(storage.clone()),
                "query-hit-epoch",
            )
            .embedding(ModelOperation::QueryEmbedding),
        )
        .await
        .unwrap();
    let old = CanonicalRepository::new(storage.clone())
        .observe_epoch("query-hit-epoch")
        .await
        .unwrap();
    fixture
        .run(embed(&ctx, "query-hit-epoch", "query"))
        .await
        .unwrap();
    elephant_storage::source_publication::SourcePublicationRepository::new(storage.clone())
        .rotate_canonical_epochs()
        .await
        .unwrap();
    assert_eq!(
        current(&ctx, "query-hit-epoch", &fixture.policy, &provenance, &old)
            .await
            .unwrap_err()
            .error_code,
        ErrorCode::NotReady
    );
    // A new search may reuse the immutable input vector but must adopt and
    // independently validate current authority. The old request remains invalid.
    fixture
        .run(embed(&ctx, "query-hit-epoch", "query"))
        .await
        .unwrap();
    assert_eq!(fixture.physical_requests(), 1);
    assert!(
        current(&ctx, "query-hit-epoch", &fixture.policy, &provenance, &old)
            .await
            .is_err()
    );
}
