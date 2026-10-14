//! Accepted-source queue handoff. Source ordering remains owned by publication;
//! this module transports the original fence and idempotently persists work.

use elephant_storage::{Repository, StorageClient, WorkItemRepository};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::PublicationFence;
use elephant_types::records::work::{WorkItem, WorkItemStatus};
use elephant_types::records::Source;
use elephant_types::{RecordId, TableName};

use crate::{PipelineError, PipelineResult};

/// Original accepted source fence retained across queue stages.
pub const PUBLICATION_PAYLOAD_KEY: &str = "_publication";

/// Configuration captured before any semantic source work begins.
pub const SOURCE_CONFIGURATION_KEY: &str = "_source_configuration";
/// Deterministic identity of the original publication and captured configuration.
pub const SOURCE_PLAN_KEY: &str = "_source_plan";

/// Reads and validates a source plan without replacing it with current policy.
/// An unversioned accepted-source bootstrap has neither key.
pub fn configuration_from_work(item: &WorkItem) -> PipelineResult<Option<SpaceConfigToken>> {
    match (
        item.payload.get(SOURCE_CONFIGURATION_KEY),
        item.payload.get(SOURCE_PLAN_KEY),
    ) {
        (None, None) => Ok(None),
        (Some(configuration), Some(plan)) => {
            let configuration: SpaceConfigToken = serde_json::from_value(configuration.clone())
                .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
            let publication = publication_from_work(item)?.ok_or_else(|| {
                PipelineError::InvalidInput("Source plan lacks publication fence".into())
            })?;
            let expected = elephant_storage::source_publication_manifest::source_plan_id(
                &publication,
                &configuration,
            );
            if configuration.space_id != item.base.space_id
                || plan.as_str() != Some(expected.as_str())
            {
                return Err(PipelineError::InvalidInput(
                    "Source plan identity mismatch".into(),
                ));
            }
            Ok(Some(configuration))
        }
        _ => Err(PipelineError::InvalidInput(
            "Incomplete source configuration capture".into(),
        )),
    }
}

/// Pins a new queue item to one configuration. Existing capture cannot change.
pub fn pin_source_configuration(
    item: &mut WorkItem,
    configuration: &SpaceConfigToken,
) -> PipelineResult<()> {
    let original = configuration_from_work(item)?;
    if original
        .as_ref()
        .is_some_and(|original| original != configuration)
        || configuration.space_id != item.base.space_id
    {
        return Err(PipelineError::SecurityViolation(
            "Source configuration cannot be replaced on existing work".into(),
        ));
    }
    let publication = publication_from_work(item)?
        .ok_or_else(|| PipelineError::InvalidInput("Source plan lacks publication fence".into()))?;
    let payload = item
        .payload
        .as_object_mut()
        .ok_or_else(|| PipelineError::InvalidInput("Source payload must be an object".into()))?;
    payload.insert(
        SOURCE_CONFIGURATION_KEY.into(),
        serde_json::to_value(configuration)
            .map_err(|error| PipelineError::InvalidInput(error.to_string()))?,
    );
    payload.insert(
        SOURCE_PLAN_KEY.into(),
        serde_json::json!(
            elephant_storage::source_publication_manifest::source_plan_id(
                &publication,
                configuration
            )
        ),
    );
    Ok(())
}

/// Reads an original source fence. Absence is permitted only for source-less
/// maintenance work; callers decide whether their work kind requires a source.
pub fn publication_from_work(item: &WorkItem) -> PipelineResult<Option<PublicationFence>> {
    item.payload
        .get(PUBLICATION_PAYLOAD_KEY)
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| PipelineError::InvalidInput(format!("Invalid publication fence: {error}")))
}

/// Copies accepted source context across scheduler boundaries. Attempt authority
/// is deliberately not inherited: the successor must claim its own generation.
pub fn inherit_source_context(parent: &WorkItem, child: &mut WorkItem) -> PipelineResult<()> {
    if parent.base.space_id != child.base.space_id {
        return Err(PipelineError::SecurityViolation(
            "Successor crosses source space".to_string(),
        ));
    }
    let publication = publication_from_work(parent)?;
    let configuration = configuration_from_work(parent)?;
    let child_configuration = configuration_from_work(child)?;
    if child_configuration.is_some() && child_configuration != configuration {
        return Err(PipelineError::SecurityViolation(
            "Successor replaced original source plan".into(),
        ));
    }
    let payload = child.payload.as_object_mut().ok_or_else(|| {
        PipelineError::InvalidInput("Successor payload must be an object".to_string())
    })?;
    payload.remove(elephant_storage::work_attempt_repository::ATTEMPT_PAYLOAD_KEY);
    let Some(publication) = publication else {
        return Ok(());
    };
    if let Some(existing) = payload.get(PUBLICATION_PAYLOAD_KEY) {
        let existing: PublicationFence = serde_json::from_value(existing.clone())
            .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
        if existing != publication {
            return Err(PipelineError::SecurityViolation(
                "Successor replaced accepted source fence".to_string(),
            ));
        }
    }
    payload.insert(
        PUBLICATION_PAYLOAD_KEY.to_string(),
        serde_json::to_value(&publication)
            .map_err(|error| PipelineError::InvalidInput(error.to_string()))?,
    );
    // A replacement revision gets distinct work even if its source ID and work
    // kind are unchanged. Replay of the same accepted revision keeps identity.
    let mut suffix = format!(
        ":publication:{}:{}",
        publication.generation, publication.revision_id
    );
    if let Some(configuration) = configuration {
        let plan = elephant_storage::source_publication_manifest::source_plan_id(
            &publication,
            &configuration,
        );
        payload.insert(
            SOURCE_CONFIGURATION_KEY.into(),
            serde_json::to_value(&configuration)
                .map_err(|error| PipelineError::InvalidInput(error.to_string()))?,
        );
        payload.insert(SOURCE_PLAN_KEY.into(), serde_json::json!(plan));
        suffix.push_str(&format!(":plan:{plan}"));
    }
    if !child.idempotency_key.ends_with(&suffix) {
        child.idempotency_key.push_str(&suffix);
    }
    child.base.security.clone_from(&parent.base.security);
    child.base.subjects.clone_from(&parent.base.subjects);
    child.base.subjects_state = parent.base.subjects_state;
    child.base.provenance.clone_from(&parent.base.provenance);
    Ok(())
}

fn comparable_payload(mut payload: serde_json::Value) -> serde_json::Value {
    if let Some(object) = payload.as_object_mut() {
        object.remove(elephant_storage::work_attempt_repository::ATTEMPT_PAYLOAD_KEY);
    }
    payload
}

/// Creates one successor identity. An exact replay returns the durable row;
/// other storage errors or changed semantic payloads are never acknowledged.
pub async fn enqueue_idempotent(
    storage: &StorageClient,
    mut item: WorkItem,
) -> PipelineResult<WorkItem> {
    let identity = serde_json::to_vec(&(
        item.base.space_id.as_str(),
        item.kind.as_str(),
        item.idempotency_key.as_str(),
    ))
    .map_err(|error| PipelineError::InvalidInput(error.to_string()))?;
    item.base.id = RecordId::new(
        TableName::WorkItem,
        uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, &identity),
    );
    let repository = WorkItemRepository::new(storage.clone());
    match repository.create(item.clone()).await {
        Ok(created) => Ok(created),
        Err(error) => {
            let existing = repository.get(&item.base.space_id, &item.base.id).await?;
            match existing {
                Some(existing)
                    if existing.base.deleted_at.is_none()
                        && existing.kind == item.kind
                        && existing.idempotency_key == item.idempotency_key
                        && existing.base.security == item.base.security
                        && existing.base.subjects == item.base.subjects
                        && existing.base.subjects_state == item.base.subjects_state
                        && comparable_payload(existing.payload.clone())
                            == comparable_payload(item.payload) =>
                {
                    Ok(existing)
                }
                _ => Err(error.into()),
            }
        }
    }
}

/// Physical immutable document identity shared by intake staging and recovery.
/// A replacement acceptance must never overwrite the prior revision's document.
pub fn accepted_document_id(
    publication: &PublicationFence,
    original_doc_id: &str,
) -> PipelineResult<RecordId> {
    elephant_storage::source_intake::accepted_document_id(publication, original_doc_id)
        .map_err(Into::into)
}

/// Surface/intake handoff after durable acceptance. A failed enqueue leaves the
/// accepted operation recoverable; it does not roll source ordering backwards.
pub async fn enqueue_accepted_source(
    storage: &StorageClient,
    source: &Source,
    publication: &PublicationFence,
    actor: &str,
) -> PipelineResult<RecordId> {
    let item = accepted_source_work(source, publication, actor)?;
    Ok(enqueue_idempotent(storage, item).await?.base.id)
}

/// Guard the initial queue insert with the same original watch authority that
/// accepted this revision. Read-only receipt retries must not call this method;
/// durable accepted-source recovery owns any acceptance/enqueue crash gap.
pub async fn enqueue_accepted_source_from_attempt(
    storage: &StorageClient,
    source: &Source,
    publication: &PublicationFence,
    actor: &str,
    producer: &elephant_storage::source_watch_producer::PreparedSourceWatchProducer,
) -> PipelineResult<RecordId> {
    let item = accepted_source_work(source, publication, actor)?;
    Ok(
        elephant_storage::work_producer_repository::WorkProducerRepository::new(storage.clone())
            .enqueue_watch_source(producer, source, publication, item)
            .await?
            .base
            .id,
    )
}

/// Enqueues a distinct chain for current policy using the original immutable
/// acceptance. A prior chain cannot acquire this identity through retry.
pub async fn enqueue_source_plan(
    storage: &StorageClient,
    source: &Source,
    publication: &PublicationFence,
    configuration: &SpaceConfigToken,
    actor: &str,
) -> PipelineResult<RecordId> {
    let mut item = accepted_source_work(source, publication, actor)?;
    pin_source_configuration(&mut item, configuration)?;
    item.idempotency_key.push_str(&format!(
        ":plan:{}",
        elephant_storage::source_publication_manifest::source_plan_id(publication, configuration)
    ));
    Ok(enqueue_idempotent(storage, item).await?.base.id)
}

fn accepted_source_work(
    source: &Source,
    publication: &PublicationFence,
    actor: &str,
) -> PipelineResult<WorkItem> {
    if source.base.id != publication.source_id || source.base.space_id.is_empty() {
        return Err(PipelineError::InvalidInput(
            "Accepted source and publication identity differ".to_string(),
        ));
    }
    let mut base = source.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    base.created_by = actor.to_string();
    base.created_at = chrono::Utc::now();
    base.updated_at = base.created_at;
    base.deleted_at = None;
    // Rehydrated accepted input may have an intake lifecycle state. Triage owns
    // the next decision; neither enqueue nor recovery marks it graph-ready.
    let priority = source
        .metadata
        .get("_intake_priority")
        .map(|value| {
            value
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| {
                    PipelineError::InvalidInput("Accepted intake priority must be i32".into())
                })
        })
        .transpose()?
        .unwrap_or(10);
    let item = WorkItem {
        base,
        kind: crate::scheduler::TRIAGE_SOURCE_KIND.to_string(),
        payload: serde_json::json!({ "source_id": publication.source_id.to_string(),
            "policy_version": "source-publication-v1", "_publication": publication }),
        status: WorkItemStatus::Runnable,
        priority,
        idempotency_key: format!(
            "triage_source:{}:{}",
            publication.source_id, publication.generation
        ),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    Ok(item)
}

/// Durable handoff for an explicitly accepted reindex. Configuration remains
/// the sole progress/activation authority; the queue only schedules it.
pub async fn enqueue_space_reindex(
    storage: &StorageClient,
    configuration: &elephant_types::records::SpaceConfig,
    actor: &str,
) -> PipelineResult<RecordId> {
    let pending = configuration
        .processing
        .as_ref()
        .and_then(|processing| processing.pending_reindex.as_ref())
        .ok_or_else(|| PipelineError::InvalidInput("No accepted reindex is pending".into()))?;
    let mut base = configuration.base.clone();
    base.id = RecordId::new_v4(TableName::WorkItem);
    base.created_by = actor.to_string();
    base.created_at = chrono::Utc::now();
    base.updated_at = base.created_at;
    let item = WorkItem {
        base,
        kind: "reindex_space_embeddings".into(),
        payload: serde_json::json!({"reindex_id": pending.id}),
        status: WorkItemStatus::Runnable,
        priority: 10,
        idempotency_key: format!("reindex_space_embeddings:{}", pending.id),
        attempts: 0,
        lease_expires_at: None,
        leased_by: None,
        run_after: None,
        last_error: None,
    };
    Ok(enqueue_idempotent(storage, item).await?.base.id)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_types::records::base::BaseFields;

    fn work(key: &str) -> WorkItem {
        WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                "named".to_string(),
                "actor".to_string(),
            ),
            kind: "render_doc".to_string(),
            payload: serde_json::json!({"doc_id":"doc:fixture"}),
            status: WorkItemStatus::Runnable,
            priority: 10,
            idempotency_key: key.to_string(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        }
    }

    fn fence(generation: &str) -> PublicationFence {
        PublicationFence {
            source_id: RecordId::new_v4(TableName::Source),
            generation: generation.to_string(),
            revision_id: "revision-1".to_string(),
            operation_id: "operation-1".to_string(),
        }
    }

    #[test]
    fn successors_keep_original_publication_but_never_inherit_attempt_authority() {
        let mut parent = work("parent");
        let original = fence("generation-1");
        parent.payload[PUBLICATION_PAYLOAD_KEY] = serde_json::to_value(&original).expect("fence");
        let mut child = work("render");
        child.payload["_attempt_fence"] = serde_json::json!({"lease_token":"old"});
        inherit_source_context(&parent, &mut child).expect("inherit");
        assert_eq!(
            publication_from_work(&child).expect("read"),
            Some(original.clone())
        );
        assert!(child.payload.get("_attempt_fence").is_none());
        let key = child.idempotency_key.clone();
        inherit_source_context(&parent, &mut child).expect("replay");
        assert_eq!(key, child.idempotency_key);
        parent.payload[PUBLICATION_PAYLOAD_KEY] =
            serde_json::to_value(fence("replacement")).expect("fence");
        assert!(inherit_source_context(&parent, &mut child).is_err());
    }

    fn configuration(generation: u64) -> SpaceConfigToken {
        SpaceConfigToken {
            space_id: "named".into(),
            config_id: "space_config:fixture".into(),
            generation,
            policy_id: "explicit-policy".into(),
        }
    }

    #[test]
    fn plan_context_is_inherited_exactly_and_never_recaptured() {
        let mut parent = work("parent");
        parent.payload[PUBLICATION_PAYLOAD_KEY] =
            serde_json::to_value(fence("one")).expect("fence");
        pin_source_configuration(&mut parent, &configuration(1)).expect("capture");
        let mut child = work("extract");
        inherit_source_context(&parent, &mut child).expect("inherit original");
        assert_eq!(
            configuration_from_work(&child).expect("read"),
            Some(configuration(1))
        );
        let identity = child.idempotency_key.clone();
        inherit_source_context(&parent, &mut child).expect("same context replay");
        assert_eq!(identity, child.idempotency_key);
        assert!(pin_source_configuration(&mut child, &configuration(2)).is_err());
        let mut successor_plan = work("extract");
        successor_plan.payload[PUBLICATION_PAYLOAD_KEY] =
            parent.payload[PUBLICATION_PAYLOAD_KEY].clone();
        pin_source_configuration(&mut successor_plan, &configuration(2)).expect("new work capture");
        assert!(inherit_source_context(&successor_plan, &mut child).is_err());
        child.payload[SOURCE_PLAN_KEY] = serde_json::json!("unrelated-plan");
        assert!(configuration_from_work(&child).is_err());
        child
            .payload
            .as_object_mut()
            .expect("object")
            .remove(SOURCE_PLAN_KEY);
        assert!(configuration_from_work(&child).is_err());
    }

    async fn versioned_enqueue_contract(storage: StorageClient) {
        let original = fence("one");
        let mut parent = work("parent");
        parent.payload[PUBLICATION_PAYLOAD_KEY] = serde_json::to_value(&original).expect("fence");
        pin_source_configuration(&mut parent, &configuration(1)).expect("capture");
        let mut first = work("extract");
        inherit_source_context(&parent, &mut first).expect("first context");
        let created = enqueue_idempotent(&storage, first.clone())
            .await
            .expect("create");
        assert_eq!(
            enqueue_idempotent(&storage, first)
                .await
                .expect("replay")
                .base
                .id,
            created.base.id
        );
        let mut next_parent = work("new plan");
        next_parent.payload[PUBLICATION_PAYLOAD_KEY] =
            serde_json::to_value(&original).expect("fence");
        pin_source_configuration(&mut next_parent, &configuration(2)).expect("new context");
        let mut successor = work("extract");
        inherit_source_context(&next_parent, &mut successor).expect("successor context");
        let successor = enqueue_idempotent(&storage, successor)
            .await
            .expect("successor");
        assert_ne!(created.base.id, successor.base.id);
        assert_eq!(
            publication_from_work(&created).expect("original"),
            publication_from_work(&successor).expect("successor")
        );
        let revision = RecordId::new_v4(TableName::DocRevision);
        let mut embedding = work(&format!(
            "embed_source_revision:{}:{}:{}:{}",
            original.source_id,
            original.generation,
            revision,
            elephant_storage::source_publication_manifest::source_plan_id(
                &original,
                &configuration(2)
            )
        ));
        embedding.kind = "embed_source_revision".into();
        embedding.payload[PUBLICATION_PAYLOAD_KEY] =
            serde_json::to_value(&original).expect("fence");
        pin_source_configuration(&mut embedding, &configuration(2)).expect("embedding context");
        let embedding = enqueue_idempotent(&storage, embedding)
            .await
            .expect("embedding job");
        assert_eq!(
            embedding.base.id,
            elephant_storage::source_publication_manifest::source_embedding_work_id_for_plan(
                "named",
                &original,
                &configuration(2),
                &revision
            )
        );
    }

    #[tokio::test]
    async fn embedded_configuration_versions_have_distinct_work_and_exact_embedding_identity() {
        versioned_enqueue_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }

    #[tokio::test]
    async fn surreal_configuration_versions_have_distinct_work_and_exact_embedding_identity() {
        versioned_enqueue_contract(StorageClient::new_memory().await.expect("surreal")).await;
    }

    async fn replay_contract(storage: StorageClient) {
        let item = work("one-accepted-revision");
        let first = enqueue_idempotent(&storage, item.clone())
            .await
            .expect("create");
        let replay = enqueue_idempotent(&storage, item.clone())
            .await
            .expect("replay");
        assert_eq!(first.base.id, replay.base.id);
        let mut changed = item;
        changed.payload["doc_id"] = serde_json::json!("doc:other");
        assert!(enqueue_idempotent(&storage, changed).await.is_err());
        let mut replacement = work("replacement-accepted-revision");
        replacement.payload = first.payload;
        let second = enqueue_idempotent(&storage, replacement)
            .await
            .expect("replacement");
        assert_ne!(first.base.id, second.base.id);
    }

    #[tokio::test]
    async fn embedded_enqueue_replay_is_exact() {
        replay_contract(StorageClient::new_embedded().await.expect("embedded")).await;
    }

    #[tokio::test]
    async fn surreal_enqueue_replay_is_exact() {
        replay_contract(StorageClient::new_memory().await.expect("surreal")).await;
    }
}
