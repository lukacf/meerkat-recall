//! Deterministic loopback SPIES through governed transport and real storage.
//! These are retry/authority controls, never provider relevance qualification.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::retrieval_test_support::QueryFixture;
use elephant_storage::{canonical_commit::CanonicalRepository, StorageClient};
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::TableName;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{sync::Semaphore, time::timeout};

tokio::task_local! {
    static PREPARATIONS: Arc<AtomicUsize>;
}
pub(super) fn preparing() {
    let _ = PREPARATIONS.try_with(|count| count.fetch_add(1, Ordering::SeqCst));
}
fn context(storage: StorageClient, space: &str) -> ToolContext {
    ToolContext::new(
        storage,
        elephant_policy::Principal {
            space_id: space.into(),
            scopes: HashSet::from(["read:doc:raw".into()]),
            clearances: vec![elephant_policy::Clearance {
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
fn request(mode: RankingMode) -> SearchRequest {
    SearchRequest {
        query: "query".into(),
        limit: 3,
        filters: SearchFilters::default(),
        source_revision_constraints: BTreeMap::new(),
        ranking_mode: Some(mode),
        use_vector: None,
    }
}
async fn wait_request(fixture: &QueryFixture, expected: usize) {
    timeout(Duration::from_secs(2), async {
        while fixture.physical_requests() < expected {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
async fn unrelated_write(storage: &StorageClient, space: &str) {
    let before = CanonicalRepository::new(storage.clone())
        .observe_epoch(space)
        .await
        .unwrap();
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Entity),
        space.into(),
        "test".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    EntityRepository::new(storage.clone())
        .create(Entity::new(
            base,
            "person".into(),
            "Unrelated maintenance entity".into(),
        ))
        .await
        .unwrap();
    assert_ne!(
        CanonicalRepository::new(storage.clone())
            .observe_epoch(space)
            .await
            .unwrap(),
        before
    );
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Retain the original search, transport gate and storage through joined race controls"
)]
async fn unrelated_epoch_change_reprepares_entire_semantic_and_hybrid_search_once() {
    for mode in [RankingMode::Semantic, RankingMode::Hybrid] {
        let storage = StorageClient::new_embedded().await.unwrap();
        let gate = Arc::new(Semaphore::new(0));
        let fixture = QueryFixture::install_with_gate(
            &storage,
            "reprepare",
            vec![("query", vec![1.0, 0.0, 0.0])],
            Some(gate.clone()),
        )
        .await;
        let ctx = context(storage.clone(), "reprepare");
        let request = request(mode);
        let preparations = Arc::new(AtomicUsize::new(0));
        let race = async {
            wait_request(&fixture, 1).await;
            unrelated_write(&storage, "reprepare").await;
            gate.add_permits(2);
        };
        let (response, ()) = timeout(
            Duration::from_secs(5),
            fixture.run(PREPARATIONS.scope(preparations.clone(), async {
                // Exercise nested HTTP/MCP-style admission as well as search's own lease.
                storage
                    .with_activity(async { tokio::join!(search(&ctx, &request), race) })
                    .await
                    .unwrap()
            })),
        )
        .await
        .unwrap();
        let response = response.unwrap();
        assert!(response.results.is_empty() && response.coverage.is_empty() && !response.partial);
        assert_eq!(
            preparations.load(Ordering::SeqCst),
            2,
            "the whole search, including lexical assembly, was reentered"
        );
        assert_eq!(
            fixture.physical_requests(),
            2,
            "the failed old-epoch fill was not cached"
        );
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Retain the original two-attempt future and physical request controls"
)]
async fn repeated_epoch_churn_stops_after_two_preparations() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let fixture = QueryFixture::install_with_gate(
        &storage,
        "churn",
        vec![("query", vec![1.0, 0.0, 0.0])],
        Some(gate.clone()),
    )
    .await;
    let ctx = context(storage.clone(), "churn");
    let request = request(RankingMode::Semantic);
    let preparations = Arc::new(AtomicUsize::new(0));
    let race = async {
        for count in 1..=2 {
            wait_request(&fixture, count).await;
            unrelated_write(&storage, "churn").await;
            gate.add_permits(1);
        }
    };
    let (response, ()) = timeout(
        Duration::from_secs(5),
        fixture.run(PREPARATIONS.scope(preparations.clone(), async {
            tokio::join!(search(&ctx, &request), race)
        })),
    )
    .await
    .unwrap();
    assert_eq!(response.unwrap_err().error_code, ErrorCode::NotReady);
    assert_eq!(preparations.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.physical_requests(), 2);
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    clippy::significant_drop_tightening,
    reason = "Keep each real binding-mutation race and request accounting in one scope"
)]
async fn revocation_credential_rotation_and_profile_change_are_terminal() {
    for change in ["revoke", "credential", "profile"] {
        let storage = StorageClient::new_embedded().await.unwrap();
        let gate = Arc::new(Semaphore::new(0));
        let fixture = QueryFixture::install_with_gate(
            &storage,
            change,
            vec![("query", vec![1.0, 0.0, 0.0])],
            Some(gate.clone()),
        )
        .await;
        let ctx = context(storage.clone(), change);
        let request = request(RankingMode::Semantic);
        let preparations = Arc::new(AtomicUsize::new(0));
        let race = async {
            wait_request(&fixture, 1).await;
            let repo = SpaceConfigRepository::new(storage.clone());
            let observed = repo.get_by_space_id(change).await.unwrap().unwrap();
            let mut replacement = observed.clone();
            let processing = replacement.processing.as_mut().unwrap();
            processing.generation += 1;
            match change {
                "revoke" => {
                    processing
                        .bindings
                        .get_mut(&ModelOperation::QueryEmbedding)
                        .unwrap()
                        .revoked = true;
                }
                "credential" => {
                    processing
                        .connections
                        .get_mut("connection")
                        .unwrap()
                        .credential_generation += 1;
                }
                "profile" => {
                    let mut profile = processing.profiles["profile"].clone();
                    profile.id = "replacement".into();
                    processing.profiles.insert(profile.id.clone(), profile);
                    processing
                        .bindings
                        .get_mut(&ModelOperation::QueryEmbedding)
                        .unwrap()
                        .profile_id = "replacement".into();
                }
                _ => unreachable!(),
            }
            repo.replace_authorized(&observed, replacement)
                .await
                .unwrap();
            gate.add_permits(1);
        };
        let (response, ()) = timeout(
            Duration::from_secs(5),
            fixture.run(PREPARATIONS.scope(preparations.clone(), async {
                tokio::join!(search(&ctx, &request), race)
            })),
        )
        .await
        .unwrap();
        let error = response.unwrap_err();
        assert!(matches!(
            error.error_code,
            ErrorCode::ProviderUnapproved | ErrorCode::NotReady
        ));
        assert_eq!(
            preparations.load(Ordering::SeqCst),
            1,
            "a public NotReady must not classify a binding change as retryable"
        );
        assert_eq!(fixture.physical_requests(), 1);
    }
}

#[tokio::test]
async fn wrong_space_and_unapproved_search_do_not_dispatch_or_reprepare() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let fixture =
        QueryFixture::install(&storage, "approved", vec![("query", vec![1.0, 0.0, 0.0])]).await;
    let ctx = context(storage.clone(), "foreign");
    drop(storage);
    let mut selected = None;
    let error = Box::pin(fixture.run(query_vectors::embed_for_search(
        &ctx,
        "approved",
        "query",
        &mut selected,
    )))
    .await
    .unwrap_err()
    .into_public();
    assert_eq!(error.error_code, ErrorCode::ForbiddenSpace);
    assert!(selected.is_none());
    let preparations = Arc::new(AtomicUsize::new(0));
    let response = Box::pin(fixture.run(PREPARATIONS.scope(
        preparations.clone(),
        search(&ctx, &request(RankingMode::Semantic)),
    )))
    .await;
    drop(ctx);
    assert_eq!(
        response.unwrap_err().error_code,
        ErrorCode::ProviderUnapproved
    );
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.physical_requests(), 0);
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "The original cancellation scope must cover the blocked second physical attempt"
)]
async fn cancellation_during_reprepare_does_not_detach_or_start_a_third_attempt() {
    let storage = StorageClient::new_embedded().await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let fixture = QueryFixture::install_with_gate(
        &storage,
        "cancel",
        vec![("query", vec![1.0, 0.0, 0.0])],
        Some(gate.clone()),
    )
    .await;
    let ctx = context(storage.clone(), "cancel");
    let request = request(RankingMode::Semantic);
    let race = async {
        wait_request(&fixture, 1).await;
        unrelated_write(&storage, "cancel").await;
        gate.add_permits(1);
        wait_request(&fixture, 2).await;
    };
    let preparations = Arc::new(AtomicUsize::new(0));
    fixture
        .run(PREPARATIONS.scope(preparations.clone(), async {
            let pending = search(&ctx, &request);
            tokio::pin!(pending);
            tokio::select! {
                _ = &mut pending => panic!("second physical response is still gated"),
                () = race => {},
            }
            // Dropping the original future cancels its second preparation in place.
        }))
        .await;
    assert_eq!(preparations.load(Ordering::SeqCst), 2);
    assert_eq!(fixture.physical_requests(), 2);
    assert!(storage.recovery_status().ready);
}

#[expect(
    clippy::significant_drop_tightening,
    clippy::too_many_lines,
    reason = "Keep actual exclusive lifecycle work and its blocked-query observations together"
)]
async fn exclusive_maintenance_is_terminal(restore: bool) {
    use elephant_storage::source_publication::{AcceptSourceRevision, SourceMutation};
    use elephant_types::publication::SourcePrecondition;
    let space = if restore {
        "restore-query"
    } else {
        "erase-query"
    };
    let storage = StorageClient::new_embedded().await.unwrap();
    let gate = Arc::new(Semaphore::new(0));
    let fixture = QueryFixture::install_with_gate(
        &storage,
        space,
        vec![("query", vec![1.0, 0.0, 0.0])],
        Some(gate.clone()),
    )
    .await;
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::Source),
        space.into(),
        "test".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    let mut source = Source::new(base, SourceType::Manual, "original".into());
    source.external_id = Some("maintenance-target".into());
    let source_id = source.base.id.clone();
    let publication = SourcePublicationRepository::new(storage.clone());
    let accepted = publication
        .accept(AcceptSourceRevision {
            source,
            revision_id: "r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "accept".into(),
            request_fingerprint: "accept-original".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .unwrap();
    let mutation = SourceMutation {
        space_id: space.into(),
        source_id: source_id.clone(),
        expected: SourcePrecondition::Generation(accepted.source_generation),
        idempotency_key: "erase".into(),
        request_fingerprint: "erase-original".into(),
        actor: "test".into(),
        authorized: publication
            .authorization_snapshot(space, &source_id)
            .await
            .unwrap()
            .unwrap(),
        authorized_document: None,
    };
    let snapshot =
        std::env::temp_dir().join(format!("recall-query-restore-{}.bin", uuid::Uuid::new_v4()));
    if restore {
        storage.export_snapshot(&snapshot).await.unwrap();
    }
    let ctx = context(storage.clone(), space);
    let request = request(RankingMode::Semantic);
    let preparations = Arc::new(AtomicUsize::new(0));
    let race = async {
        wait_request(&fixture, 1).await;
        let client = storage.clone();
        let path = snapshot.clone();
        // Spawn only test lifecycle work: a separate owner must drain search's
        // activity lease. Production search never spawns or detaches a retry.
        let maintenance = tokio::spawn(async move {
            if restore {
                client.import_snapshot(path).await
            } else {
                SourcePublicationRepository::new(client)
                    .erase_live(mutation)
                    .await
                    .map(|_| ())
            }
        });
        timeout(Duration::from_secs(2), async {
            while storage.recovery_status().ready {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            !maintenance.is_finished(),
            "physical replacement/purge cannot pass the live search lease"
        );
        gate.add_permits(1);
        // Join the maintenance task only after search releases its activity lease.
        (maintenance,)
    };
    let (response, (maintenance,)) = timeout(
        Duration::from_secs(5),
        fixture.run(PREPARATIONS.scope(preparations.clone(), async {
            tokio::join!(search(&ctx, &request), race)
        })),
    )
    .await
    .unwrap();
    assert!(
        response.is_err(),
        "exclusive lifecycle changes terminate this request"
    );
    assert_eq!(preparations.load(Ordering::SeqCst), 1);
    assert_eq!(fixture.physical_requests(), 1);
    timeout(Duration::from_secs(5), maintenance)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(storage.recovery_status().ready);
    if restore {
        std::fs::remove_file(snapshot).unwrap();
    } else {
        assert!(SourceRepository::new(storage)
            .get(space, &source_id)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn direct_search_lease_rejects_pending_restore_without_reprepare() {
    Box::pin(exclusive_maintenance_is_terminal(true)).await;
}
#[tokio::test]
async fn direct_search_lease_rejects_pending_physical_erasure_without_reprepare() {
    Box::pin(exclusive_maintenance_is_terminal(false)).await;
}
