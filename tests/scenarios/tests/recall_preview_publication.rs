//! Independent transactional oracles using integrity's active repository seams.
//! Requires the coherent integrity + migrations join; no fake commit implementation.

#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_storage::canonical_commit::{
    CanonicalCommit, CanonicalEffect, CanonicalFault, CanonicalPlan, CanonicalPublicationContext,
    CanonicalRecord, CanonicalRepository,
};
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{
    EntityRepository, Repository, SourceRepository, SpaceConfigRepository, StorageClient,
    SystemEventRepository,
};
use elephant_types::publication::{OperationState, SourcePrecondition, SourceVisibility};
use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::work::WorkItemStatus;
use elephant_types::records::{ManualDocTemplate, Source, SourceType, SpaceConfig, SystemEvent};
use elephant_types::{RecordId, TableName};

const SPACE: &str = "publication-business";

async fn register_space(
    client: &StorageClient,
    space: &str,
    dimensions: u32,
) -> anyhow::Result<()> {
    let mut base = BaseFields::new(
        RecordId::new_v4(TableName::SpaceConfig),
        space.into(),
        "qualification".into(),
    );
    base.subjects_state = SubjectsState::Complete;
    // Domain fixture decoding, not a database-boundary deserialization proxy.
    // No model operation is invoked by these transaction tests.
    let mut fields = serde_json::to_value(base)?;
    fields["embedding_provider"] = serde_json::json!(format!("fixture-{space}"));
    fields["embedding_model"] = serde_json::json!(format!("fixture-model-{dimensions}"));
    fields["embedding_dims"] = serde_json::json!(dimensions);
    fields["purpose"] = serde_json::json!(format!("Independent qualification in {space}"));
    fields["skill"] = serde_json::json!(format!("fixture-{space}"));
    fields["processing"] = serde_json::Value::Null;
    let config: SpaceConfig = serde_json::from_value(fields)?;
    SpaceConfigRepository::new(client.clone())
        .create(config)
        .await?;
    Ok(())
}

fn request(space: &str, text: &str) -> anyhow::Result<AcceptSourceRevision> {
    let mut source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            space.into(),
            "qualification".into(),
        ),
        SourceType::Manual,
        support::digest(text),
    );
    source.external_id = Some("shared-external-report".into());
    source.base.subjects_state = SubjectsState::Complete;
    source.set_manual_doc_template(&ManualDocTemplate {
        doc_id: RecordId::new_v4(TableName::Doc).to_string(),
        title: "Independent revision fixture".into(),
        text: text.into(),
        sidecar_text: None,
        media_attachments: vec![],
        rendition_key: "qualification-source".into(),
        metadata: serde_json::json!({"observed_at": "2026-10-06T20:11:00Z"}),
    })?;
    Ok(AcceptSourceRevision {
        source,
        revision_id: "revision-1".into(),
        expected: SourcePrecondition::Absent,
        idempotency_key: "request-1".into(),
        request_fingerprint: "independent-fingerprint-1".into(),
        reactivate: false,
        recreate_after_erasure: false,
        authorized: None,
        authorized_document: None,
    })
}

async fn successor(
    repository: &SourcePublicationRepository,
    original: &AcceptSourceRevision,
    revision: &str,
    generation: &str,
) -> anyhow::Result<AcceptSourceRevision> {
    let mut next = original.clone();
    next.revision_id = revision.into();
    next.idempotency_key = format!("request-{revision}");
    next.request_fingerprint = format!("independent-fingerprint-{revision}");
    next.expected = SourcePrecondition::Generation(generation.into());
    next.authorized = Some(
        support::authorized_source_snapshot(
            repository,
            &original.source.base.space_id,
            &original.source.base.id,
        )
        .await?,
    );
    Ok(next)
}

#[tokio::test]
async fn source_acceptance_is_revision_exact_idempotent_and_desired_head_fenced(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, SPACE, 384).await?;
        let repo = SourcePublicationRepository::new(client);
        let initial = request(SPACE, "The Oriole launch budget is EUR 42000.")?;
        let first = repo.accept(initial.clone()).await?;
        anyhow::ensure!(first.state == OperationState::Accepted);
        anyhow::ensure!(
            first.published_revision.is_none() && first.completed_at.is_none(),
            "acceptance falsely claimed completion"
        );
        anyhow::ensure!(!first.source_generation.is_empty());
        let mut retry = initial.clone();
        retry.expected = SourcePrecondition::Generation(first.source_generation.clone());
        retry.source.metadata["observed_at"] = serde_json::json!("2026-10-06T20:45:00Z");
        anyhow::ensure!(
            repo.accept(retry).await? == first,
            "observation/precondition changed immutable retry identity"
        );

        let mut changed = initial.clone();
        changed.source.hash = support::digest("different source bytes");
        anyhow::ensure!(
            repo.accept(changed).await.is_err(),
            "same idempotency key rebound payload"
        );
        let mut relabeled = initial.clone();
        relabeled.idempotency_key = "new-request-same-revision".into();
        relabeled.request_fingerprint = "different-labels".into();
        relabeled.source.metadata["labels"] =
            serde_json::json!({"initiative_name": ["new meaning"]});
        anyhow::ensure!(
            repo.accept(relabeled).await.is_err(),
            "new request rebound immutable revision semantics"
        );

        let second = repo
            .accept(successor(&repo, &initial, "revision-2", &first.source_generation).await?)
            .await?;
        anyhow::ensure!(second.source_generation != first.source_generation);
        anyhow::ensure!(
            second.published_revision.is_none(),
            "pending source claimed unperformed publication"
        );
        let stale = successor(&repo, &initial, "revision-3", &first.source_generation).await?;
        anyhow::ensure!(
            repo.accept(stale).await.is_err(),
            "stale desired head accepted"
        );
        let head = repo
            .head(SPACE, &initial.source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("head absent"))?;
        anyhow::ensure!(head.desired_revision.as_deref() == Some("revision-2"));
        anyhow::ensure!(head.lifecycle == SourceVisibility::Active);
        let retained = repo
            .revision(SPACE, &initial.source.base.id, "revision-1")
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted immutable input absent"))?;
        anyhow::ensure!(retained.source.hash == initial.source.hash);
    }
    Ok(())
}

#[tokio::test]
async fn changed_policy_after_authorization_cannot_accept_replacement() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, SPACE, 384).await?;
        let publications = SourcePublicationRepository::new(client.clone());
        let initial = request(SPACE, "The Oriole launch budget is EUR 42000.")?;
        let first = publications.accept(initial.clone()).await?;
        let replacement = successor(
            &publications,
            &initial,
            "revision-2",
            &first.source_generation,
        )
        .await?;
        let sources = SourceRepository::new(client.clone());
        let mut changed = sources
            .get(SPACE, &initial.source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("accepted source missing"))?;
        changed.base.security.level = 1; // Fixed principal is cleared only to default level 0.
        sources
            .update(SPACE, &initial.source.base.id, changed)
            .await?;
        let failure = publications
            .accept(replacement)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("replacement used revoked authorization prestate"))?;
        anyhow::ensure!(
            failure.to_string().contains("authorized_source_changed"),
            "{backend:?}: replacement stopped at unrelated guard: {failure}"
        );
        let head = publications
            .head(SPACE, &initial.source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("head lost"))?;
        anyhow::ensure!(
            head.generation == first.source_generation
                && head.desired_revision.as_deref() == Some("revision-1")
        );
        anyhow::ensure!(publications
            .revision(SPACE, &initial.source.base.id, "revision-2")
            .await?
            .is_none());
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_source_replacements_have_one_acceptance_winner() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, SPACE, 384).await?;
        let repo = SourcePublicationRepository::new(client);
        let initial = request(SPACE, "The source owner selects the revision ordering.")?;
        let first = repo.accept(initial.clone()).await?;
        let left = successor(&repo, &initial, "revision-left", &first.source_generation).await?;
        let right = successor(&repo, &initial, "revision-right", &first.source_generation).await?;
        let (left, right) = tokio::join!(repo.accept(left), repo.accept(right));
        anyhow::ensure!(
            usize::from(left.is_ok()) + usize::from(right.is_ok()) == 1,
            "{backend:?}: competing desired-head updates both accepted or both failed"
        );
        let winner = left.or(right)?;
        let head = repo
            .head(SPACE, &initial.source.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("head absent"))?;
        anyhow::ensure!(head.generation == winner.source_generation);
        anyhow::ensure!(head.operation_id == winner.operation_id);
    }
    Ok(())
}

#[tokio::test]
async fn historical_revision_reacceptance_requires_fresh_key_and_current_cas_without_rebinding_input(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, SPACE, 384).await?;
        let repository = SourcePublicationRepository::new(client.clone());
        let original = request(SPACE, "Revision A is the original authoritative text.")?;
        let a = repository.accept(original.clone()).await?;
        let immutable_a = serde_json::to_value(
            repository
                .revision(SPACE, &a.source_id, "revision-1")
                .await?,
        )?;
        let mut next =
            successor(&repository, &original, "revision-2", &a.source_generation).await?;
        let mut template = next
            .source
            .manual_doc_template()?
            .ok_or_else(|| anyhow::anyhow!("fixture template absent"))?;
        template.text = "Revision B is a different authoritative text.".into();
        template.doc_id = RecordId::new_v4(TableName::Doc).to_string();
        next.source.hash = support::digest(&template.text);
        next.source.set_manual_doc_template(&template)?;
        let b = repository.accept(next.clone()).await?;
        let head_b = repository.head(SPACE, &a.source_id).await?;
        let historical_a = repository
            .operation(SPACE, &a.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("historical operation absent"))?;
        let mut replay = original.clone();
        replay.expected = SourcePrecondition::Generation(b.source_generation.clone());
        replay.authorized =
            Some(support::authorized_source_snapshot(&repository, SPACE, &a.source_id).await?);
        anyhow::ensure!(
            repository.accept(replay.clone()).await? == historical_a,
            "old key created fresh intent instead of replaying its historical operation"
        );
        anyhow::ensure!(repository.head(SPACE, &a.source_id).await? == head_b);
        anyhow::ensure!(
            SourceRepository::new(client.clone())
                .get(SPACE, &a.source_id)
                .await?
                .is_some_and(|source| source.hash == next.source.hash),
            "historical replay rewrote current source payload"
        );

        let mut no_cas = replay.clone();
        no_cas.idempotency_key = "fresh-a-without-cas".into();
        no_cas.expected = SourcePrecondition::Absent;
        anyhow::ensure!(
            repository.accept(no_cas).await.is_err(),
            "known immutable revision bypassed required CAS"
        );
        let mut fresh = replay;
        fresh.idempotency_key = "fresh-intent-back-to-a".into();
        let back_to_a = repository.accept(fresh.clone()).await?;
        anyhow::ensure!(
            back_to_a.source_id == a.source_id
                && back_to_a.operation_id != a.operation_id
                && back_to_a.operation_id != b.operation_id
        );
        anyhow::ensure!(
            back_to_a.source_generation != a.source_generation
                && back_to_a.source_generation != b.source_generation
        );
        anyhow::ensure!(
            back_to_a.desired_revision.as_deref() == Some("revision-1")
                && back_to_a.state == OperationState::Accepted
                && back_to_a.completed_at.is_none()
        );
        let head_a = repository
            .head(SPACE, &a.source_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("new desired head absent"))?;
        anyhow::ensure!(
            head_a.operation_id == back_to_a.operation_id
                && head_a.generation == back_to_a.source_generation
                && head_a.desired_revision.as_deref() == Some("revision-1")
        );
        anyhow::ensure!(SourceRepository::new(client.clone())
            .get(SPACE, &a.source_id)
            .await?
            .is_some_and(|source| source.hash == original.source.hash));
        anyhow::ensure!(
            serde_json::to_value(
                repository
                    .revision(SPACE, &a.source_id, "revision-1")
                    .await?
            )? == immutable_a,
            "new acceptance rewrote immutable original input or its acceptance metadata"
        );
        anyhow::ensure!(
            repository.accept(fresh.clone()).await? == back_to_a,
            "fresh key retry made another operation"
        );

        let mut stale = fresh.clone();
        stale.idempotency_key = "different-intent-with-stale-b-cas".into();
        stale.authorized =
            Some(support::authorized_source_snapshot(&repository, SPACE, &a.source_id).await?);
        anyhow::ensure!(
            repository.accept(stale).await.is_err(),
            "known A revision returned an old receipt despite stale new-intent CAS"
        );
        let mut changed = fresh;
        changed.idempotency_key = "attempt-to-rebind-a".into();
        changed.expected = SourcePrecondition::Generation(back_to_a.source_generation.clone());
        changed.authorized =
            Some(support::authorized_source_snapshot(&repository, SPACE, &a.source_id).await?);
        changed.request_fingerprint = "changed-a-payload".into();
        let mut template = changed
            .source
            .manual_doc_template()?
            .ok_or_else(|| anyhow::anyhow!("template absent"))?;
        template.text = "This is not immutable revision A.".into();
        changed.source.hash = support::digest(&template.text);
        changed.source.set_manual_doc_template(&template)?;
        let error = repository
            .accept(changed)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("changed immutable A accepted"))?;
        anyhow::ensure!(
            error.to_string().contains("immutable_revision"),
            "changed A refused for unrelated reason: {error}"
        );
        anyhow::ensure!(repository.head(SPACE, &a.source_id).await?.as_ref() == Some(&head_a));
    }
    Ok(())
}

#[tokio::test]
async fn same_external_source_and_idempotency_keys_are_independent_across_named_spaces(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, "household-qualification", 384).await?;
        register_space(&client, "business-qualification", 768).await?;
        let repo = SourcePublicationRepository::new(client);
        let home = request(
            "household-qualification",
            "Shared handbook: cobalt cabinet.",
        )?;
        let business = request("business-qualification", "Shared handbook: cobalt cabinet.")?;
        let (home_receipt, business_receipt) =
            tokio::join!(repo.accept(home.clone()), repo.accept(business.clone()));
        let (home_receipt, business_receipt) = (home_receipt?, business_receipt?);
        anyhow::ensure!(home_receipt.operation_id != business_receipt.operation_id);
        anyhow::ensure!(home_receipt.source_id != business_receipt.source_id);
        anyhow::ensure!(repo
            .head("household-qualification", &business.source.base.id)
            .await?
            .is_none());
        anyhow::ensure!(repo
            .operation("household-qualification", &business_receipt.operation_id)
            .await?
            .is_none());
        anyhow::ensure!(repo
            .revision("business-qualification", &home.source.base.id, "revision-1")
            .await?
            .is_none());
    }
    Ok(())
}

#[tokio::test]
async fn source_byte_limit_has_no_partial_acceptance_on_oversize() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        for bytes in [1_999_999, 2_000_000, 2_000_001] {
            let client = support::storage(backend).await?;
            register_space(&client, SPACE, 384).await?;
            let repo = SourcePublicationRepository::new(client);
            let text = format!("{}{}", "é".repeat(bytes / 2), "x".repeat(bytes % 2));
            anyhow::ensure!(text.len() == bytes);
            let input = request(SPACE, &text)?;
            let result = repo.accept(input.clone()).await;
            if bytes <= 2_000_000 {
                anyhow::ensure!(
                    result.is_ok(),
                    "{backend:?}: valid UTF-8 byte boundary refused"
                );
            } else {
                anyhow::ensure!(
                    result.is_err(),
                    "{backend:?}: oversized decoded text accepted"
                );
                anyhow::ensure!(
                    repo.head(SPACE, &input.source.base.id).await?.is_none(),
                    "oversized input left a durable accepted head"
                );
                anyhow::ensure!(repo
                    .revision(SPACE, &input.source.base.id, "revision-1")
                    .await?
                    .is_none());
            }
        }
    }
    Ok(())
}

fn entity(name: &str) -> Entity {
    Entity::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            SPACE.into(),
            "qualification".into(),
        ),
        EntityType::new("topic"),
        name.into(),
    )
}

async fn complete_plan(repo: &CanonicalRepository) -> anyhow::Result<CanonicalCommit> {
    let first = entity("Original Oriole record");
    let second = entity("Independent companion record");
    let event = SystemEvent {
        base: BaseFields::new(
            RecordId::new_v4(TableName::SystemEvent),
            SPACE.into(),
            "qualification".into(),
        ),
        seq: 0,
        event_type: "records.changed".into(),
        payload: serde_json::json!({"record_ids": [first.base.id, second.base.id]}),
    };
    Ok(CanonicalCommit {
        space_id: SPACE.into(),
        commit_id: "qualification-complete-plan".into(),
        input_fingerprint: support::digest("qualification-complete-plan-input-v1"),
        replay_output: None,
        publication: None, // Explicit source-less authority tests the storage transaction only.
        maintenance: None,
        finalization: None,
        plan: CanonicalPlan {
            observed_epoch: repo.epoch(SPACE).await?,
            dependencies: vec![],
            effects: vec![first, second]
                .into_iter()
                .map(|row| CanonicalEffect {
                    before: None,
                    after: Some(CanonicalRecord::Entity(Box::new(row))),
                })
                .collect(),
            outbox: vec![event],
            ..CanonicalPlan::default()
        },
    })
}

#[tokio::test]
async fn actual_canonical_faults_roll_back_every_effect_outbox_and_receipt_then_retry(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        for fault in [
            CanonicalFault::AfterFirstEffect,
            CanonicalFault::BeforeOutbox,
            CanonicalFault::BeforeReceipt,
        ] {
            let client = support::storage(backend).await?;
            register_space(&client, SPACE, 384).await?;
            let repo = CanonicalRepository::new(client.clone());
            let plan = complete_plan(&repo).await?;
            anyhow::ensure!(
                repo.commit_with_fault(plan.clone(), fault).await.is_err(),
                "fault was not exercised"
            );
            anyhow::ensure!(
                EntityRepository::new(client.clone())
                    .list(SPACE, 100, None)
                    .await?
                    .is_empty(),
                "{backend:?}/{fault:?}: partial canonical state survived rollback"
            );
            anyhow::ensure!(
                SystemEventRepository::new(client.clone())
                    .list(SPACE, 100, None)
                    .await?
                    .is_empty(),
                "partial outbox survived rollback"
            );
            anyhow::ensure!(
                repo.receipt(SPACE, &plan.commit_id).await?.is_none(),
                "incomplete transaction got receipt"
            );
            anyhow::ensure!(
                repo.epoch(SPACE).await? == plan.plan.observed_epoch,
                "failed transaction changed planning epoch"
            );
            let receipt = repo.commit(plan.clone()).await?;
            anyhow::ensure!(receipt.effect_ids.len() == 2 && receipt.outbox_ids.len() == 1);
            anyhow::ensure!(
                EntityRepository::new(client.clone())
                    .list(SPACE, 100, None)
                    .await?
                    .len()
                    == 2
            );
            anyhow::ensure!(
                SystemEventRepository::new(client)
                    .list(SPACE, 100, None)
                    .await?
                    .len()
                    == 1
            );
            anyhow::ensure!(
                repo.commit(plan).await? == receipt,
                "complete retry was not idempotent"
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn concurrent_new_identity_invalidates_a_plan_even_without_changed_dependencies(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, SPACE, 384).await?;
        let repo = CanonicalRepository::new(client.clone());
        let plan = complete_plan(&repo).await?;
        let concurrent = EntityRepository::new(client.clone())
            .create(entity("New predicate member"))
            .await?;
        anyhow::ensure!(
            repo.commit(plan).await.is_err(),
            "{backend:?}: predicate phantom did not invalidate plan"
        );
        let rows = EntityRepository::new(client).list(SPACE, 100, None).await?;
        anyhow::ensure!(
            rows.len() == 1 && rows[0].base.id == concurrent.base.id,
            "conflict overwrote concurrent state"
        );
    }
    Ok(())
}

#[tokio::test]
async fn canonical_commit_identity_cannot_be_reused_for_changed_semantics() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        register_space(&client, SPACE, 384).await?;
        let repo = CanonicalRepository::new(client.clone());
        let plan = complete_plan(&repo).await?;
        repo.commit(plan.clone()).await?;
        let mut changed = plan;
        let Some(CanonicalRecord::Entity(first)) = changed.plan.effects[0].after.as_mut() else {
            anyhow::bail!("fixture first effect is not entity");
        };
        first.display_name = "Changed semantic payload under old commit identity".into();
        anyhow::ensure!(
            repo.commit(changed).await.is_err(),
            "{backend:?}: changed plan reused completed receipt"
        );
        let rows = EntityRepository::new(client).list(SPACE, 100, None).await?;
        anyhow::ensure!(rows
            .iter()
            .any(|row| row.display_name == "Original Oriole record"));
    }
    Ok(())
}

#[tokio::test]
async fn same_worker_reclaim_fences_old_publication_completion_and_renewal() -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let mut fixture =
            publication_support::MetadataFixture::new(client.clone(), SPACE, 1, 1).await?;
        let queue = fixture.queue.clone();
        let first = fixture.claimed.clone();
        let first_fence = fence_from_claim(&first)?;
        let old_owner = queue.for_attempt(&first)?;
        old_owner.release(SPACE, &first.base.id).await?;
        let second = queue
            .claim()
            .await?
            .ok_or_else(|| anyhow::anyhow!("reclaimed work absent"))?;
        let second_fence = fence_from_claim(&second)?;
        anyhow::ensure!(second_fence.generation > first_fence.generation);
        anyhow::ensure!(second_fence.lease_token != first_fence.lease_token);
        anyhow::ensure!(
            old_owner.complete(SPACE, &first.base.id).await.is_err(),
            "old same-worker claim completed new attempt"
        );
        anyhow::ensure!(
            old_owner
                .renew_lease(SPACE, &first.base.id, 300)
                .await
                .is_err(),
            "old same-worker claim renewed new attempt"
        );

        // Finalizer prepares its own fresh epoch, so stale attempt is isolated
        // from incidental planning-epoch changes and missing manifest failures.
        let failure = fixture
            .finalize()
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("stale attempt published"))?;
        anyhow::ensure!(
            failure.to_string().contains("stale_attempt"),
            "{backend:?}: stale-attempt test stopped at another precondition: {failure}"
        );
        anyhow::ensure!(EntityRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty());
        fixture.claimed = second.clone();
        fixture.finalize().await?;
        let completed = SourcePublicationRepository::new(client.clone())
            .operation(SPACE, &fixture.accepted.operation_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("operation missing"))?;
        anyhow::ensure!(
            completed.state == OperationState::Completed,
            "replacement attempt failed to publish the declared metadata work"
        );
        anyhow::ensure!(
            queue
                .for_attempt(&second)?
                .complete(SPACE, &second.base.id)
                .await?
                .status
                == WorkItemStatus::Done
        );
    }
    Ok(())
}

#[tokio::test]
async fn newer_desired_revision_fences_prior_publication_even_before_it_publishes(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let fixture =
            publication_support::MetadataFixture::new(client.clone(), SPACE, 1, 1).await?;
        let sources = SourcePublicationRepository::new(client.clone());
        let manifests = SourcePublicationManifestRepository::new(client.clone());
        let requirements = manifests.requirements(SPACE, &fixture.publication).await?;
        manifests.check_ready(&requirements).await?;
        let newer = sources
            .accept(
                successor(
                    &sources,
                    &fixture.request,
                    "revision-2",
                    &fixture.accepted.source_generation,
                )
                .await?,
            )
            .await?;
        anyhow::ensure!(newer.published_revision.is_none());
        let canonical = CanonicalRepository::new(client.clone());
        let mut plan = complete_plan(&canonical).await?;
        plan.commit_id = requirements.commit_id();
        plan.finalization = Some(requirements);
        plan.publication = Some(CanonicalPublicationContext {
            source: fixture.publication,
            attempt: fence_from_claim(&fixture.claimed)?,
        });
        let failure = canonical
            .commit(plan)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("older accepted source published"))?;
        anyhow::ensure!(
            failure.to_string().contains("stale_source_generation"),
            "{backend:?}: desired-head oracle stopped at another precondition: {failure}"
        );
        anyhow::ensure!(EntityRepository::new(client)
            .list(SPACE, 100, None)
            .await?
            .is_empty());
    }
    Ok(())
}
