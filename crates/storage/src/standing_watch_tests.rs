#![allow(clippy::expect_used)]
use super::*;
use crate::space_grant_repository::SpaceGrantRepository;
use crate::{SourceWatchRepository, SpaceConfigRepository};
use chrono::Utc;
use elephant_auth::{Claims, LegacyVerifier};
use elephant_policy::Clearance;
use elephant_types::records::{BaseFields, SubjectsState};
use elephant_types::Link;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use std::collections::HashSet;

fn principal() -> Principal {
    Principal {
        space_id: "quiet-watch-space".into(),
        scopes: ["read:source", "write:source", "read:doc", "write:doc"]
            .into_iter()
            .map(str::to_owned)
            .collect(),
        clearances: vec![Clearance {
            policy_id: "household-policy".into(),
            max_level: 3,
            labels: HashSet::from(["private".into()]),
            handling_allow: HashSet::from(["local".into()]),
        }],
        subject_allowlist: Some(HashSet::new()),
        purpose: HashSet::from(["watch-refresh".into()]),
    }
}

fn verified(principal: &Principal, grant: &SpaceGrant) -> StandingWatchAdmission {
    let claims = Claims {
        sub: grant.subject.clone(),
        exp: Utc::now().timestamp() + 3600,
        iat: Utc::now().timestamp(),
        iss: None,
        aud: None,
        space_id: Some(principal.space_id.clone()),
        spaces: Some(vec![principal.space_id.clone()]),
        bootstrap_space: None,
        scopes: principal.scopes.iter().cloned().collect(),
        security_level: 0,
        security_labels: vec![],
        clearances: principal.clearances.clone(),
        subject_allowlist: principal
            .subject_allowlist
            .as_ref()
            .map(|values| values.iter().cloned().collect()),
        purpose: principal.purpose.iter().cloned().collect(),
    };
    let key = b"standing-watch-storage-regression-only";
    let token = encode(&Header::default(), &claims, &EncodingKey::from_secret(key))
        .expect("synthetic signed registration");
    let caller = LegacyVerifier {
        secret: "standing-watch-storage-regression-only",
        algorithm: Algorithm::HS256,
        issuer: None,
        audience: None,
        mappings: None,
    }
    .verify(&token)
    .expect("actual signature verification");
    StandingWatchAdmission::verified(&caller, principal, grant)
        .expect("verified bounded registration")
}

async fn fixture(embedded: bool) -> (StorageClient, SourceWatch, SpaceConfig, SpaceGrant) {
    let storage = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("storage");
    if !embedded {
        storage
            .query_checked(include_str!(
                "../../migrations/sql/031_source_publication.surql"
            ))
            .await
            .expect("actual named space epoch schema");
        storage
            .query_checked("DEFINE TABLE space_grant SCHEMALESS;")
            .await
            .expect("explicit grant fixture schema");
        crate::reference_fence::install_native_reference_guards(&storage, &[])
            .await
            .expect("actual private reference and physical read guard schema");
        storage
            .query_checked(include_str!(
                "../../migrations/sql/042_watch_delegation.surql"
            ))
            .await
            .expect("explicit standing schema");
    }
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "quiet-watch-space".into(),
        "original-owner".into(),
    );
    base.security.policy_id = "household-policy".into();
    base.security.level = 3;
    base.security.labels = vec!["private".into()];
    base.security.handling = vec!["local".into()];
    base.subjects_state = SubjectsState::Complete;
    let config = SpaceConfigRepository::new(storage.clone())
        .create(SpaceConfig {
            base: base.clone(),
            embedding_provider: "unused".into(),
            embedding_model: "unused".into(),
            embedding_dims: 8,
            processing: None,
            purpose: Some("Extraction guidance is not an authorization purpose".into()),
            skill: None,
        })
        .await
        .expect("actual named configuration");
    base.id = RecordId::new_v4(TableName::SourceWatch);
    let watch = SourceWatch {
        base,
        source: Link::new(RecordId::new_v4(TableName::Source)),
        requested_uri: "https://watch.invalid/document".into(),
        status: SourceWatchStatus::Enabled,
        interval_seconds: 3600,
        next_check_at: Utc::now(),
        last_checked_at: None,
        last_changed_at: None,
        etag: None,
        last_modified: None,
        observed_hash: None,
        consecutive_failures: 0,
        last_error: None,
        latest_check: None,
    };
    let grant = SpaceGrantRepository::new(storage.clone())
        .replace(
            SpaceGrant {
                space_id: watch.base.space_id.clone(),
                subject: "original-signed-service".into(),
                generation: String::new(),
                active: true,
                scopes: principal().scopes.into_iter().collect(),
                updated_by: "administrator".into(),
                updated_at: Utc::now(),
            },
            None,
        )
        .await
        .expect("actual current grant");
    (storage, watch, config, grant)
}

// Exercise the same transaction splice used by I's registration consumer. This
// helper cannot construct admission or manufacture a persisted authority row.
async fn commit_registration(
    storage: &StorageClient,
    proof: &PreparedStandingWatchRegistration,
) -> StorageResult<()> {
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                proof.persist_tx(tx)?;
                if proof.previous_watch.is_some() {
                    tx.replace(proof.watch.clone())?;
                } else {
                    tx.create(proof.watch.clone())?;
                }
                Ok(())
            })
            .await
    } else {
        let mut sql = "BEGIN TRANSACTION;".to_owned();
        let mut bindings = BTreeMap::new();
        proof.append_persist_sql(&mut sql, &mut bindings)?;
        bindings.insert(
            "test_watch".into(),
            SourceWatchRepository::watch_to_value(&proof.watch)?,
        );
        sql.push_str("UPSERT $standing_watch_watch CONTENT $test_watch; COMMIT TRANSACTION;");
        storage.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
}

async fn commit_observation(
    storage: &StorageClient,
    proof: &PreparedStandingWatchAuthority,
    hash: &str,
) -> StorageResult<()> {
    let mut updated = proof.watch.clone();
    updated.observed_hash = Some(hash.into());
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                proof.check_tx(tx)?;
                tx.replace(updated)?;
                Ok(())
            })
            .await
    } else {
        let mut sql = "BEGIN TRANSACTION;".to_owned();
        let mut bindings = BTreeMap::new();
        proof.append_guard_sql(&mut sql, &mut bindings)?;
        bindings.insert(
            "test_watch".into(),
            SourceWatchRepository::watch_to_value(&updated)?,
        );
        sql.push_str("UPDATE $standing_watch_watch CONTENT $test_watch; COMMIT TRANSACTION;");
        storage.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn signed_revocation(embedded: bool) {
    let (storage, watch, config, grant) = fixture(embedded).await;
    let repository = StandingWatchRepository::new(storage.clone());
    assert!(
        repository.capture_for_scheduling(&watch).await.is_err(),
        "missing delegation is never local authority"
    );
    let prepared = repository
        .prepare_registration(&watch, &verified(&principal(), &grant))
        .await
        .expect("prepare actual signed input");
    let identity = prepared.identity().clone();
    commit_registration(&storage, &prepared)
        .await
        .expect("atomic original registration");
    let original = repository
        .capture(&watch, &identity)
        .await
        .expect("original current authority");
    assert_eq!(original.principal().subject_allowlist, Some(HashSet::new()));
    assert_eq!(original.principal().purpose, principal().purpose);
    assert!(format!("{original:?}").contains("redacted"));

    if !embedded {
        #[derive(Deserialize)]
        struct GuardRow {
            payload: String,
            digest: String,
            read_nonce: Option<String>,
        }
        let mut nonces = Vec::new();
        for _ in 0..2 {
            let mut sql = "BEGIN TRANSACTION;".to_owned();
            let mut bindings = BTreeMap::new();
            original
                .append_guard_sql(&mut sql, &mut bindings)
                .expect("actual guarded read transaction");
            sql.push_str("COMMIT TRANSACTION;");
            storage
                .query_bind_checked(&sql, bindings)
                .await
                .expect("actual changing guard write");
            let mut response = storage
                .query_bind_checked(
                    "SELECT payload,digest,read_nonce FROM $id",
                    crate::bindings! {"id"=>row_id(&watch.base.id)},
                )
                .await
                .expect("literal original private row");
            let rows: Vec<GuardRow> = response.take(0).expect("typed native proof projection");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].payload, prepared.next.payload);
            assert_eq!(rows[0].digest, identity.digest);
            nonces.push(
                rows[0]
                    .read_nonce
                    .clone()
                    .expect("guard must persist a real write nonce"),
            );
        }
        assert_ne!(
            nonces[0], nonces[1],
            "same-payload no-op cannot provide MVCC exclusion"
        );
        assert_eq!(
            SourceWatchRepository::new(storage.clone())
                .get(&watch.base.space_id, &watch.base.id)
                .await
                .expect("original public Watch unchanged"),
            Some(watch.clone())
        );
    }

    let mut broader = grant.clone();
    broader.scopes.insert("manage:wiki".into());
    let broader = SpaceGrantRepository::new(storage.clone())
        .replace(broader, Some(&grant.generation))
        .await
        .expect("later broader grant");
    assert!(
        commit_observation(&storage, &original, "stale-grant-write")
            .await
            .is_err(),
        "in-flight proof cannot adopt another current grant"
    );
    let attenuated = repository
        .capture(&watch, &identity)
        .await
        .expect("new attempt uses original ceiling");
    assert_eq!(
        attenuated.principal().scopes,
        principal().scopes,
        "broader grant never broadens original delegation"
    );
    assert_eq!(attenuated.principal().clearances[0].max_level, 3);
    assert_eq!(
        attenuated.principal().subject_allowlist,
        Some(HashSet::new())
    );
    assert_eq!(attenuated.principal().purpose, principal().purpose);
    commit_observation(&storage, &attenuated, "original-positive")
        .await
        .expect("same current proof is positive");
    let current = SourceWatchRepository::new(storage.clone())
        .get(&watch.base.space_id, &watch.base.id)
        .await
        .expect("read original watch")
        .expect("watch");
    let proof = repository
        .capture(&current, &identity)
        .await
        .expect("current observation");
    let mut revoked = broader.clone();
    revoked.active = false;
    SpaceGrantRepository::new(storage.clone())
        .replace(revoked, Some(&broader.generation))
        .await
        .expect("concurrent actual revocation");
    assert!(commit_observation(&storage, &proof, "must-not-commit")
        .await
        .is_err());
    assert!(
        repository.capture_for_scheduling(&current).await.is_err(),
        "revoke before fetch/dispatch"
    );
    assert_eq!(
        SourceWatchRepository::new(storage.clone())
            .get(&watch.base.space_id, &watch.base.id)
            .await
            .expect("exact unchanged watch"),
        Some(current)
    );
    assert_eq!(
        SpaceConfigRepository::new(storage)
            .get_by_space_id(&watch.base.space_id)
            .await
            .expect("config unchanged"),
        Some(config)
    );
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository, configurations in the original fixture scope through assertions and cleanup."
)]
async fn registration_races(embedded: bool) {
    let (storage, watch, config, grant) = fixture(embedded).await;
    let repository = StandingWatchRepository::new(storage.clone());
    let admission = verified(&principal(), &grant);
    let stale = repository
        .prepare_registration(&watch, &admission)
        .await
        .expect("original registration proposal");
    let configurations = SpaceConfigRepository::new(storage.clone());
    let mut changed = config.clone();
    changed.purpose = Some("explicit changed configuration after preflight".into());
    let changed = configurations
        .replace_authorized(&config, changed)
        .await
        .expect("supported space policy edit after original preflight");
    assert!(commit_registration(&storage, &stale).await.is_err());
    assert!(SourceWatchRepository::new(storage.clone())
        .get(&watch.base.space_id, &watch.base.id)
        .await
        .expect("no partial watch")
        .is_none());
    assert!(repository
        .stored(&watch)
        .await
        .expect("no partial private authority")
        .is_none());
    // Restore the original policy with the supported original-row CAS before
    // preparing a separate explicit registration. Never refresh the stale proof.
    configurations
        .replace_authorized(&changed, config)
        .await
        .expect("explicit restore original policy");
    let first = repository
        .prepare_registration(&watch, &admission)
        .await
        .expect("original explicit registration");
    commit_registration(&storage, &first)
        .await
        .expect("first authority");
    let original = repository
        .capture(&watch, first.identity())
        .await
        .expect("original current proof");
    let second = repository
        .prepare_registration(&watch, &admission)
        .await
        .expect("explicit newly authorized reconfiguration");
    assert_ne!(first.identity(), second.identity());
    commit_registration(&storage, &second)
        .await
        .expect("replace only through valid explicit admission");
    assert!(
        repository.capture(&watch, first.identity()).await.is_err(),
        "old queued intent cannot borrow replacement delegation"
    );
    assert!(commit_observation(&storage, &original, "stale-delegation")
        .await
        .is_err());
    let current = repository
        .capture(&watch, second.identity())
        .await
        .expect("new explicit identity positive");
    commit_observation(&storage, &current, "replacement-positive")
        .await
        .expect("new original proof positive");
}

#[tokio::test]
async fn embedded_signed_standing_watch_revocation_and_original_ceiling() {
    Box::pin(signed_revocation(true)).await;
}
#[tokio::test]
async fn native_signed_standing_watch_revocation_and_original_ceiling() {
    Box::pin(signed_revocation(false)).await;
}
#[tokio::test]
async fn embedded_standing_watch_config_race_and_reconfiguration_are_atomic() {
    Box::pin(registration_races(true)).await;
}
#[tokio::test]
async fn native_standing_watch_config_race_and_reconfiguration_are_atomic() {
    Box::pin(registration_races(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep authorities, items in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn queued_archive_recovery(embedded: bool) {
    use crate::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
    use crate::source_watch_registration::PreparedSourceWatchRegistration;
    use crate::work_producer_repository::WorkProducerRepository;
    use elephant_types::publication::SourcePrecondition;
    use elephant_types::records::{Source, SourceType, WorkItemStatus};

    let (storage, mut watch, _, grant) = fixture(embedded).await;
    if !embedded {
        for schema in [
            include_str!("../../migrations/sql/032_work_recovery.surql"),
            include_str!("../../migrations/sql/041_work_producer_origin.surql"),
        ] {
            storage
                .query_checked(schema)
                .await
                .expect("explicit queue/publication schema");
        }
    }
    let mut base = watch.base.clone();
    base.id = watch.source.id().clone();
    let source = Source::new(base, SourceType::Web, "original-active-input".into());
    let accepted = SourcePublicationRepository::new(storage.clone())
        .accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "original-independent-admission".into(),
            request_fingerprint: "original-input".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        })
        .await
        .expect("actual original active source");
    let source = crate::SourceRepository::new(storage.clone())
        .get(&watch.base.space_id, &accepted.source_id)
        .await
        .expect("source read")
        .expect("source");
    watch.base.security = source.base.security.clone();
    watch.base.subjects = source.base.subjects.clone();
    watch.base.subjects_state = source.base.subjects_state;
    watch.base.provenance = source.base.provenance.clone();
    let mut ceiling = principal();
    ceiling.subject_allowlist = None;
    let admission = verified(&ceiling, &grant);
    let authorities = StandingWatchRepository::new(storage.clone());
    let initial = PreparedSourceWatchRegistration::prepare(&storage, &source, None, watch.clone())
        .await
        .expect("actual record preflight");
    let registration = authorities
        .prepare_registration(&watch, &admission)
        .await
        .expect("verified admission");
    initial
        .register(&storage, &registration)
        .await
        .expect("actual same-TX record+delegation registration");
    let records =
        PreparedSourceWatchRegistration::prepare(&storage, &source, Some(&watch), watch.clone())
            .await
            .expect("original scheduling readset");
    let authority = authorities
        .capture(&watch, registration.identity())
        .await
        .expect("original delegation");
    let items = crate::WorkItemRepository::new(storage.clone());
    let original = items
        .enqueue_source_watch_refresh(&records, &authority, Some(&admission), 1)
        .await
        .expect("actual authorized queue");
    let origin = WorkProducerRepository::new(storage.clone())
        .origin_for_work(&original)
        .await
        .expect("actual private producer");
    assert_eq!(
        items
            .enqueue_source_watch_refresh(&records, &authority, None, 1)
            .await
            .expect("idempotent same slot")
            .base
            .id,
        original.base.id
    );
    let claimed = items
        .claim_with_lease(
            &watch.base.space_id,
            &original.base.id,
            "original-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("claim")
        .expect("actual original claim");
    let fence =
        crate::work_attempt_repository::fence_from_claim(&claimed).expect("actual original fence");
    let terminal = items
        .finish_authorized(
            &claimed,
            &fence.lease_token,
            WorkItemStatus::Failed,
            Some("failure before observation".into()),
        )
        .await
        .expect("finish original attempt")
        .expect("owned completion");
    let archived = crate::work_retention::archive_terminal_work(
        &storage,
        Utc::now() + chrono::Duration::seconds(1),
        16,
    )
    .await
    .expect("actual original cold archival");
    assert_eq!(archived.items_archived, 1);
    assert_eq!(
        items
            .get(&watch.base.space_id, &original.base.id)
            .await
            .expect("same original cold lookup"),
        Some(terminal.clone())
    );

    let mut original_inputs =
        PreparedStandingSourceAuthority::capture_for_work(&storage, &terminal)
            .await
            .expect("same resolver starts from original archived Work");
    assert_eq!(original_inputs.principals().count(), 1);
    assert_eq!(
        original_inputs.source_ids().collect::<Vec<_>>(),
        vec![&source.base.id]
    );
    original_inputs
        .authorize_record(crate::canonical_commit::CanonicalRecord::Source(Box::new(
            source.clone(),
        )))
        .expect("all original ceilings authorize actual source input");
    let mut denied = source.clone();
    denied.base.security.level = 4;
    assert!(
        original_inputs
            .authorize_record(crate::canonical_commit::CanonicalRecord::Source(Box::new(
                denied
            )),)
            .is_err(),
        "record ABAC cannot be replaced by grant-only checks"
    );
    original_inputs
        .verify_current(&storage)
        .await
        .expect("original read-only preflight");

    let mut revoked = grant.clone();
    revoked.active = false;
    let revoked = SpaceGrantRepository::new(storage.clone())
        .replace(revoked, Some(&grant.generation))
        .await
        .expect("revoke before reactivation");
    assert!(original_inputs.verify_current(&storage).await.is_err());
    assert!(
        PreparedStandingSourceAuthority::capture_for_work(&storage, &terminal)
            .await
            .is_err()
    );
    assert!(items
        .enqueue_source_watch_refresh(&records, &authority, None, 1)
        .await
        .is_err());
    assert_eq!(
        items
            .get(&watch.base.space_id, &original.base.id)
            .await
            .expect("no changed cold work"),
        Some(terminal.clone())
    );
    let mut restored = revoked.clone();
    restored.active = true;
    SpaceGrantRepository::new(storage.clone())
        .replace(restored, Some(&revoked.generation))
        .await
        .expect("explicit restore actual grant");
    let current = authorities
        .capture(&watch, registration.identity())
        .await
        .expect("same delegation current grant");
    let reopened = items
        .enqueue_source_watch_refresh(&records, &current, None, 1)
        .await
        .expect("actual original cold promotion under current authority");
    assert_eq!(reopened.base.id, original.base.id);
    assert_eq!(
        reopened.payload, terminal.payload,
        "original attempt bookkeeping retained without replacing immutable intent"
    );
    assert_eq!(
        WorkProducerRepository::new(storage.clone())
            .origin_for_work(&reopened)
            .await
            .expect("retained original origin"),
        origin
    );
    let next = items
        .claim_with_lease(
            &watch.base.space_id,
            &original.base.id,
            "next-worker",
            Utc::now() + chrono::Duration::minutes(5),
        )
        .await
        .expect("next claim")
        .expect("next actual attempt");
    let next_fence =
        crate::work_attempt_repository::fence_from_claim(&next).expect("new genuine fence");
    assert!(next_fence.generation > fence.generation);
    assert_ne!(next_fence.lease_token, fence.lease_token);
    assert!(items
        .finish_authorized(&next, &fence.lease_token, WorkItemStatus::Done, None)
        .await
        .expect("obsolete token refused")
        .is_none());
}

#[tokio::test]
async fn embedded_standing_watch_reopens_original_archived_slot_without_new_authority() {
    Box::pin(queued_archive_recovery(true)).await;
}
#[tokio::test]
async fn native_standing_watch_reopens_original_archived_slot_without_new_authority() {
    Box::pin(queued_archive_recovery(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep repository in the original fixture scope through assertions and cleanup."
)]
async fn local_authority_boundary(embedded: bool) {
    let (storage, watch, _, grant) = fixture(embedded).await;
    let repository = StandingWatchRepository::new(storage.clone());
    let ceiling = principal();
    let local =
        StandingWatchAdmission::trusted_local(&ceiling).expect("explicit bounded local admission");
    assert!(
        repository
            .capture_for_local_scheduling(&watch, &local)
            .await
            .is_err(),
        "missing does not become local trust"
    );
    let initial = repository
        .prepare_local_registration_if_absent(&watch, &local)
        .await
        .expect("original absence")
        .expect("sealed absent registration");
    let service = repository
        .prepare_registration(&watch, &verified(&ceiling, &grant))
        .await
        .expect("concurrent verified registration");
    commit_registration(&storage, &service)
        .await
        .expect("actual signed registration wins");
    assert!(
        commit_registration(&storage, &initial).await.is_err(),
        "local absence cannot cross concurrent signed registration"
    );
    assert!(
        repository
            .prepare_local_registration_if_absent(&watch, &local)
            .await
            .is_err(),
        "signed registration cannot be adopted locally"
    );
    assert!(repository
        .capture_for_local_scheduling(&watch, &local)
        .await
        .is_err());
    assert_eq!(
        repository
            .stored(&watch)
            .await
            .expect("original signed row")
            .expect("retained")
            .identity,
        *service.identity()
    );

    let mut second = watch.clone();
    second.base.id = RecordId::new_v4(TableName::SourceWatch);
    second.source = Link::new(RecordId::new_v4(TableName::Source));
    let prepared = repository
        .prepare_local_registration_if_absent(&second, &local)
        .await
        .expect("second explicit local absence")
        .expect("new local intent");
    commit_registration(&storage, &prepared)
        .await
        .expect("atomic actual local registration");
    repository
        .capture_for_local_scheduling(&second, &local)
        .await
        .expect("same explicitly selected ceiling positive");
    assert!(repository
        .prepare_local_registration_if_absent(&second, &local)
        .await
        .expect("existing exact local authority")
        .is_none());
    let mut broader = ceiling.clone();
    broader.clearances[0].max_level = 7;
    let changed = StandingWatchAdmission::trusted_local(&broader)
        .expect("separately explicit broader local request");
    assert!(
        repository
            .capture_for_local_scheduling(&second, &changed)
            .await
            .is_err(),
        "cannot silently expand original local registration"
    );
}

#[tokio::test]
async fn embedded_local_watch_cannot_adopt_signed_or_raced_registration() {
    Box::pin(local_authority_boundary(true)).await;
}
#[tokio::test]
async fn native_local_watch_cannot_adopt_signed_or_raced_registration() {
    Box::pin(local_authority_boundary(false)).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, configurations, standing in the original fixture scope through assertions and cleanup."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep this ordered conformance scenario and its failure assertions in one function."
)]
async fn original_policy_survives_content_coverage(embedded: bool) {
    use crate::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
    use crate::{DocRepository, Repository};
    use elephant_types::provider_binding::SpaceProcessingConfig;
    use elephant_types::publication::SourcePrecondition;
    use elephant_types::records::{Doc, Source, SourceType};

    let storage = if embedded {
        Box::pin(StorageClient::new_embedded()).await
    } else {
        Box::pin(StorageClient::new_memory()).await
    }
    .expect("fresh storage");
    if !embedded {
        for schema in [
            include_str!("../../migrations/sql/031_source_publication.surql"),
            include_str!("../../migrations/sql/033_space_processing.surql"),
        ] {
            storage
                .query_checked(schema)
                .await
                .expect("actual publication and content generation schema before records");
        }
        crate::reference_fence::install_native_reference_guards(&storage, &[])
            .await
            .expect("actual physical and private reference guards before records");
        storage
            .query_checked(include_str!(
                "../../migrations/sql/042_watch_delegation.surql"
            ))
            .await
            .expect("actual standing delegation schema before records");
    }
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        "quiet-watch-space".into(),
        "original-owner".into(),
    );
    base.security.policy_id = "household-policy".into();
    base.security.level = 3;
    base.security.labels = vec!["private".into()];
    base.security.handling = vec!["local".into()];
    base.subjects_state = SubjectsState::Complete;
    let configurations = SpaceConfigRepository::new(storage.clone());
    configurations
        .create(SpaceConfig {
            base: base.clone(),
            embedding_provider: "unconfigured".into(),
            embedding_model: "unconfigured".into(),
            embedding_dims: 1,
            processing: Some(SpaceProcessingConfig {
                generation: 1,
                index_generation: 0,
                policy_id: "original-config-policy".into(),
                connections: std::collections::BTreeMap::default(),
                profiles: std::collections::BTreeMap::default(),
                bindings: std::collections::BTreeMap::default(),
                active_embedding_profile_id: None,
                pending_reindex: None,
            }),
            purpose: None,
            skill: None,
        })
        .await
        .expect("original configured space");
    base.id = RecordId::new_v4(TableName::Source);
    let mut source = Source::new(base.clone(), SourceType::Web, "original-source".into());
    source.uri = Some("https://watch.invalid/coverage".into());
    Box::pin(
        SourcePublicationRepository::new(storage.clone()).accept(AcceptSourceRevision {
            source: source.clone(),
            revision_id: "coverage-original".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: "coverage-original".into(),
            request_fingerprint: "coverage-original".into(),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        }),
    )
    .await
    .expect("actual original source admission");
    base.id = RecordId::new_v4(TableName::SourceWatch);
    let watch = SourceWatch {
        base: base.clone(),
        source: Link::new(source.base.id.clone()),
        requested_uri: source.uri.clone().expect("URI"),
        status: SourceWatchStatus::Enabled,
        interval_seconds: 300,
        next_check_at: Utc::now(),
        last_checked_at: None,
        last_changed_at: None,
        etag: None,
        last_modified: None,
        observed_hash: None,
        consecutive_failures: 0,
        last_error: None,
        latest_check: None,
    };
    let standing = StandingWatchRepository::new(storage.clone());
    let admission = StandingWatchAdmission::trusted_local(&principal()).expect("bounded admission");
    let registration = standing
        .prepare_registration(&watch, &admission)
        .await
        .expect("registration");
    commit_registration(&storage, &registration)
        .await
        .expect("actual registration");
    let proof = standing
        .capture(&watch, registration.identity())
        .await
        .expect("original proof");
    let original_configuration = proof.configuration.clone();
    let original_coverage = original_configuration
        .processing
        .as_ref()
        .expect("processing")
        .index_generation;
    base.id = RecordId::new_v4(TableName::Doc);
    DocRepository::new(storage.clone())
        .create(Doc {
            base,
            title: "Watched source".into(),
            text: "Actual newly indexed content".into(),
            sidecar_text: None,
            media_attachments: vec![],
            source: Some(Link::new(source.base.id)),
            rendition_key: "coverage".into(),
            metadata: serde_json::json!({}),
        })
        .await
        .expect("actual content write advances storage coverage");
    let after_content = configurations
        .get_by_space_id(&watch.base.space_id)
        .await
        .expect("configuration")
        .expect("present");
    assert!(
        after_content
            .processing
            .as_ref()
            .expect("processing")
            .index_generation
            > original_coverage
    );
    assert!(
        same_configuration_policy(&original_configuration, &after_content)
            .expect("policy projection")
    );
    commit_observation(&storage, &proof, "content-positive")
        .await
        .expect("same original policy after content write");
    assert_eq!(
        proof.configuration, original_configuration,
        "never recapture or edit the saved proof"
    );

    let current_watch = SourceWatchRepository::new(storage.clone())
        .get(&watch.base.space_id, &watch.base.id)
        .await
        .expect("watch")
        .expect("present");
    let before_policy_edit = standing
        .capture(&current_watch, registration.identity())
        .await
        .expect("new explicit observation");
    let mut changed = after_content.clone();
    changed.purpose = Some("later configuration policy".into());
    configurations
        .replace_authorized(&after_content, changed)
        .await
        .expect("supported policy edit");
    assert!(
        commit_observation(&storage, &before_policy_edit, "must-not-write")
            .await
            .is_err()
    );
    assert_eq!(
        SourceWatchRepository::new(storage)
            .get(&watch.base.space_id, &watch.base.id)
            .await
            .expect("unchanged watch"),
        Some(current_watch)
    );
}

#[tokio::test]
async fn embedded_original_standing_policy_survives_only_content_coverage_changes() {
    Box::pin(original_policy_survives_content_coverage(true)).await;
}

#[tokio::test]
async fn native_original_standing_policy_survives_only_content_coverage_changes() {
    Box::pin(original_policy_survives_content_coverage(false)).await;
}

#[test]
fn standing_configuration_policy_keeps_original_record_identity() {
    let original = SpaceConfig {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SpaceConfig),
            "original-policy-space".into(),
            "original-owner".into(),
        ),
        embedding_provider: "unconfigured".into(),
        embedding_model: "unconfigured".into(),
        embedding_dims: 1,
        processing: None,
        purpose: None,
        skill: None,
    };
    let mut bookkeeping = original.clone();
    bookkeeping.base.updated_at += chrono::Duration::seconds(1);
    assert!(same_configuration_policy(&original, &bookkeeping).expect("bookkeeping"));
    let mut substituted = original.clone();
    substituted.base.id = RecordId::new_v4(TableName::SpaceConfig);
    assert!(
        !same_configuration_policy(&original, &substituted).expect("original identity"),
        "another record with identical policy fields cannot replace original authority"
    );
}
