//! Regression for native source progress invalidating another source's plan.
//!
//! These controls use the real acceptance, queue claim and execution-state
//! writers. An actual receipted pre-044 store first demonstrates the old epoch
//! churn, then explicit migration removes only that churn. Transaction nonce
//! writes, physical read fences, original attempt checks and semantic head
//! invalidation must survive. The concurrent Graph dispatcher oracle is owned
//! separately by the pipeline runtime; this module does not claim publication.

#![allow(
    clippy::significant_drop_tightening,
    reason = "Keep fixture clients and repositories alive through their original before/after observations"
)]

use std::collections::BTreeMap;

use chrono::Utc;
use elephant_storage::canonical_commit::CanonicalRepository;
use elephant_storage::source_publication::{AcceptSourceRevision, SourcePublicationRepository};
use elephant_storage::work_attempt_repository::fence_from_claim;
use elephant_storage::{Repository, SpaceConfigRepository, StorageClient, WorkItemRepository};
use elephant_types::publication::{
    AttemptFence, OperationState, PublicationFence, SourcePrecondition,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::{Source, SourceType, SpaceConfig, WorkItem, WorkItemStatus};
use elephant_types::{RecordId, TableName};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

const SPACE: &str = "source-progress-epoch";
const OTHER_SPACE: &str = "source-progress-other";
const MIGRATION: &str = "source_head_semantic_epoch";
const ACTOR: &str = "source-epoch-regression";

struct ClaimedSource {
    publication: PublicationFence,
    attempt: AttemptFence,
}

async fn create_space(client: &StorageClient, space: &str) -> TestResult {
    SpaceConfigRepository::new(client.clone())
        .create(SpaceConfig {
            base: BaseFields::new(
                RecordId::new_v4(TableName::SpaceConfig),
                space.into(),
                ACTOR.into(),
            ),
            embedding_provider: "fake".into(),
            embedding_model: "epoch-fixture-only".into(),
            embedding_dims: 8,
            processing: None,
            purpose: None,
            skill: None,
        })
        .await?;
    Ok(())
}

async fn accept_and_claim(client: &StorageClient, label: &str) -> TestResult<ClaimedSource> {
    let source = Source::new(
        BaseFields::new(
            RecordId::new_v4(TableName::Source),
            SPACE.into(),
            ACTOR.into(),
        ),
        SourceType::Manual,
        format!("epoch-input-{label}"),
    );
    let receipt = Box::pin(SourcePublicationRepository::new(client.clone()).accept(
        AcceptSourceRevision {
            source,
            revision_id: "r1".into(),
            expected: SourcePrecondition::Absent,
            idempotency_key: format!("epoch-accept-{label}"),
            request_fingerprint: format!("epoch-request-{label}"),
            reactivate: false,
            recreate_after_erasure: false,
            authorized: None,
            authorized_document: None,
        },
    ))
    .await?;
    let publication = PublicationFence {
        source_id: receipt.source_id,
        generation: receipt.source_generation,
        revision_id: receipt
            .desired_revision
            .ok_or("accepted revision missing")?,
        operation_id: receipt.operation_id,
    };
    let items = WorkItemRepository::new(client.clone());
    let work = items
        .create(WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                SPACE.into(),
                ACTOR.into(),
            ),
            kind: "finalize_source_revision".into(),
            payload: json!({"_publication": publication}),
            status: WorkItemStatus::Runnable,
            priority: 0,
            idempotency_key: format!("epoch-finalizer-{label}"),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        })
        .await?;
    let claimed = Box::pin(items.claim_with_lease(
        SPACE,
        &work.base.id,
        ACTOR,
        Utc::now() + chrono::Duration::minutes(5),
    ))
    .await?
    .ok_or("original finalizer claim missing")?;
    assert_eq!(claimed.attempts, 1);
    assert_eq!(claimed.status, WorkItemStatus::Running);
    Ok(ClaimedSource {
        publication,
        attempt: fence_from_claim(&claimed)?,
    })
}

#[derive(Debug, Deserialize)]
struct HeadNonceDb {
    physical_id: String,
    fence_nonce: Option<String>,
}

#[derive(Deserialize)]
struct PhysicalNonceDb {
    nonce: String,
}

#[derive(Debug, PartialEq, Eq)]
struct Nonces {
    head: Option<String>,
    physical: String,
}

async fn nonces(client: &StorageClient, source: &ClaimedSource) -> TestResult<Nonces> {
    // The server supplies the same physical identity string that migration 043
    // hashes. Do not reconstruct a head key or serialize a native record via JSON.
    let mut response = client
        .query_bind_checked(
            "SELECT type::string(id) AS physical_id, fence_nonce FROM source_head \
             WHERE space_id=$space AND source_id=$source;",
            json!({"space": SPACE, "source": source.publication.source_id.to_string()}),
        )
        .await?;
    let rows: Vec<HeadNonceDb> = response.take(0)?;
    assert_eq!(rows.len(), 1, "one original accepted source head");
    let row = rows.into_iter().next().ok_or("source head missing")?;
    let digest = format!("{:x}", Sha256::digest(row.physical_id.as_bytes()));
    let bucket = format!("source_head:{}", &digest[..3]);
    let mut response = client
        .query_bind_checked(
            "SELECT nonce FROM type::thing('native_read_fence',$bucket);",
            json!({"bucket": bucket}),
        )
        .await?;
    let rows: Vec<PhysicalNonceDb> = response.take(0)?;
    assert_eq!(rows.len(), 1, "original physical read fence exists");
    Ok(Nonces {
        head: row.fence_nonce,
        physical: rows
            .into_iter()
            .next()
            .ok_or("physical fence missing")?
            .nonce,
    })
}

async fn progress(
    client: &StorageClient,
    source: &ClaimedSource,
    state: OperationState,
    epoch_must_change: bool,
) -> TestResult {
    let canonical = CanonicalRepository::new(client.clone());
    let before_epoch = canonical.current_epoch(SPACE).await?;
    let before_nonces = nonces(client, source).await?;
    let error_code = (state == OperationState::FailedRetryable).then_some("pending_dependencies");
    let publications = SourcePublicationRepository::new(client.clone());
    Box::pin(publications.record_execution_state(
        SPACE,
        &source.publication,
        &source.attempt,
        state,
        error_code,
    ))
    .await?;
    let after_epoch = canonical.current_epoch(SPACE).await?;
    assert_eq!(before_epoch != after_epoch, epoch_must_change);
    let after_nonces = nonces(client, source).await?;
    assert!(after_nonces.head.is_some());
    assert_ne!(
        before_nonces.head, after_nonces.head,
        "keep the row conflict write"
    );
    assert_ne!(
        before_nonces.physical, after_nonces.physical,
        "keep physical read fencing"
    );
    let operation = publications
        .operation(SPACE, &source.publication.operation_id)
        .await?
        .ok_or("original operation missing")?;
    assert_eq!(operation.state, state);
    assert_eq!(operation.published_revision, None);
    Ok(())
}

async fn refuse_stale_progress(client: &StorageClient, source: &ClaimedSource) -> TestResult {
    let canonical = CanonicalRepository::new(client.clone());
    let before_epoch = canonical.current_epoch(SPACE).await?;
    let before_nonces = nonces(client, source).await?;
    let publications = SourcePublicationRepository::new(client.clone());
    let before_operation = publications
        .operation(SPACE, &source.publication.operation_id)
        .await?;
    let mut stale = source.attempt.clone();
    stale.generation = stale
        .generation
        .checked_add(1)
        .ok_or("generation overflow")?;
    assert!(Box::pin(publications.record_execution_state(
        SPACE,
        &source.publication,
        &stale,
        OperationState::Processing,
        None,
    ))
    .await
    .is_err());
    assert!(
        Box::pin(publications.record_execution_state(
            SPACE,
            &source.publication,
            &source.attempt,
            OperationState::Completed,
            None,
        ))
        .await
        .is_err(),
        "bookkeeping cannot certify publication"
    );
    assert_eq!(canonical.current_epoch(SPACE).await?, before_epoch);
    assert_eq!(nonces(client, source).await?, before_nonces);
    assert_eq!(
        publications
            .operation(SPACE, &source.publication.operation_id)
            .await?,
        before_operation
    );
    Ok(())
}

#[tokio::test]
async fn native_progress_epoch_requires_explicit_upgrade_and_keeps_attempt_fences() -> TestResult {
    let client = StorageClient::new_memory().await?;
    let files = super::schema_files();
    let index = files
        .iter()
        .position(|(name, _)| *name == MIGRATION)
        .ok_or("source-head epoch migration missing")?;
    assert_eq!(files[index - 1].0, "native_read_fence");
    Box::pin(super::apply_files(
        &client,
        &files[..index],
        &BTreeMap::new(),
    ))
    .await?;
    client.initialize_journal_identity().await?;
    create_space(&client, SPACE).await?;
    let first = Box::pin(accept_and_claim(&client, "first")).await?;
    let second = Box::pin(accept_and_claim(&client, "second")).await?;

    // This is the old writer, not a synthetic injected Conflict or nonce update:
    // either source's real progress transition invalidates the shared epoch.
    Box::pin(progress(&client, &first, OperationState::Processing, true)).await?;
    Box::pin(progress(&client, &second, OperationState::Processing, true)).await?;
    let old_receipts = super::schema_receipts(&client).await?;
    let mut old_tables = client.list_tables().await?;
    old_tables.sort_unstable();
    assert!(matches!(super::verify_schema(&client).await, Err(error) if error.contains(MIGRATION)));
    assert_eq!(super::schema_receipts(&client).await?, old_receipts);
    let mut refused_tables = client.list_tables().await?;
    refused_tables.sort_unstable();
    // Native table enumeration has no ordering contract. Keep exact membership
    // and multiplicity so read-only refusal cannot add or remove schema.
    assert_eq!(refused_tables, old_tables);

    Box::pin(super::apply_schema(&client)).await?;
    super::verify_schema(&client).await?;
    let current_receipts = super::schema_receipts(&client).await?;
    for (name, checksum) in old_receipts {
        assert_eq!(
            current_receipts.get(&name),
            Some(&checksum),
            "historical SQL stays immutable"
        );
    }
    assert_eq!(
        current_receipts.get(MIGRATION),
        Some(&super::checksum(files[index].1))
    );
    for source in [&first, &second] {
        Box::pin(progress(&client, source, OperationState::Processing, false)).await?;
        Box::pin(progress(
            &client,
            source,
            OperationState::FailedRetryable,
            false,
        ))
        .await?;
        Box::pin(refuse_stale_progress(&client, source)).await?;
    }
    Box::pin(super::apply_schema(&client)).await?;
    assert_eq!(super::schema_receipts(&client).await?, current_receipts);
    client
        .query_bind_checked(
            "UPDATE migration_history SET checksum='wrong' WHERE name=$name;",
            json!({"name": MIGRATION}),
        )
        .await?;
    let damaged = super::schema_receipts(&client).await?;
    assert!(super::verify_schema(&client).await.is_err());
    assert!(Box::pin(super::apply_schema(&client)).await.is_err());
    assert_eq!(super::schema_receipts(&client).await?, damaged);
    Ok(())
}

async fn mutate_probe(client: &StorageClient, sql: &str, spaces: &[&str]) -> TestResult {
    let canonical = CanonicalRepository::new(client.clone());
    let mut before = Vec::new();
    for space in spaces {
        before.push(canonical.current_epoch(space).await?);
    }
    client
        .query_bind_checked(sql, json!({"space": SPACE, "other": OTHER_SPACE}))
        .await?;
    for (space, epoch) in spaces.iter().zip(before) {
        assert_ne!(
            canonical.current_epoch(space).await?,
            epoch,
            "semantic mutation in {space}: {sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn native_source_head_semantic_changes_create_delete_and_space_move_keep_epoch_fences(
) -> TestResult {
    let client = StorageClient::new_memory().await?;
    Box::pin(super::apply_schema(&client)).await?;
    create_space(&client, SPACE).await?;
    create_space(&client, OTHER_SPACE).await?;
    let source = Box::pin(accept_and_claim(&client, "semantic")).await?;
    let canonical = CanonicalRepository::new(client.clone());
    canonical.epoch(OTHER_SPACE).await?;

    // Copy an accepted head's payload/state under a distinct source identity:
    // source_head_space_source forbids two heads for the same space/source pair.
    // Direct writes test the intrinsic event, not public mutation authorization.
    Box::pin(mutate_probe(
        &client,
        "LET $original = (SELECT * FROM source_head WHERE space_id=$space LIMIT 1)[0]; \
         CREATE source_head:epoch_semantic_probe SET space_id=$space, \
         source_id=string::concat('source:',<string>rand::uuid()), \
         payload=$original.payload,state=$original.state;",
        &[SPACE],
    ))
    .await?;
    for sql in [
        "UPDATE source_head:epoch_semantic_probe SET payload=string::concat(payload,' ');",
        "UPDATE source_head:epoch_semantic_probe SET state.generation='different-generation';",
        "UPDATE source_head:epoch_semantic_probe SET state.desired_revision='different-revision';",
        "UPDATE source_head:epoch_semantic_probe SET state.lifecycle='retracted';",
        "UPDATE source_head:epoch_semantic_probe SET source_id='source:different-source';",
        "UPDATE source_head:epoch_semantic_probe SET additional_semantic_field='future-writer';",
        "UPDATE source_head:epoch_semantic_probe UNSET additional_semantic_field;",
    ] {
        Box::pin(mutate_probe(&client, sql, &[SPACE])).await?;
    }
    Box::pin(mutate_probe(
        &client,
        "UPDATE source_head:epoch_semantic_probe SET space_id=$other;",
        &[SPACE, OTHER_SPACE],
    ))
    .await?;
    Box::pin(mutate_probe(
        &client,
        "DELETE source_head:epoch_semantic_probe;",
        &[OTHER_SPACE],
    ))
    .await?;
    // Removing the private nonce is also a semantic no-op, as when an exact
    // control replacement omits that optional transaction-only field.
    Box::pin(progress(
        &client,
        &source,
        OperationState::Processing,
        false,
    ))
    .await?;
    let before_epoch = canonical.current_epoch(SPACE).await?;
    let before_nonces = nonces(&client, &source).await?;
    client
        .query_bind_checked(
            "UPDATE source_head UNSET fence_nonce WHERE space_id=$space AND source_id=$source;",
            json!({"space": SPACE, "source": source.publication.source_id.to_string()}),
        )
        .await?;
    let after_nonces = nonces(&client, &source).await?;
    assert_eq!(after_nonces.head, None);
    assert_ne!(after_nonces.physical, before_nonces.physical);
    assert_eq!(canonical.current_epoch(SPACE).await?, before_epoch);
    Box::pin(progress(
        &client,
        &source,
        OperationState::FailedRetryable,
        false,
    ))
    .await?;
    Ok(())
}
