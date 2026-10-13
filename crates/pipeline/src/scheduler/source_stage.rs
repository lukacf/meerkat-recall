//! Configuration-versioned staging identity. These read guards supplement the
//! finalizer's atomic manifest/attempt checks; they grant no publication right.

use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::source_plan_id;
use elephant_storage::space_config_repository::{SpaceConfigRepository, SpaceResolutionError};
use elephant_storage::{
    Repository, StorageClient, WorkArtifactRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::{PublicationFence, SourceVisibility};
use elephant_types::records::work::WorkArtifact;
use elephant_types::{RecordId, TableName};
use serde_json::Value;
use uuid::Uuid;

use crate::PipelineError;

/// Original producer configuration and source intent, never a refreshed head.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceStageContext {
    publication: PublicationFence,
    configuration: SpaceConfigToken,
    plan_id: String,
}

fn invalid(reason: &str) -> PipelineError {
    PipelineError::ValidationError(reason.into())
}

/// Stable attempt-local artifact identity, separated across configuration plans.
/// A new worker run never overwrites an earlier run's model output.
///
/// # Errors
/// Returns an error if identity serialization fails or a required key is empty.
pub fn source_stage_artifact_id(
    space: &str,
    plan_id: &str,
    kind: &str,
    revision: &RecordId,
    node: Option<&RecordId>,
    work_run: &RecordId,
) -> Result<RecordId, PipelineError> {
    if space.is_empty()
        || plan_id.is_empty()
        || kind.is_empty()
        || revision.table() != TableName::DocRevision
        || node.is_some_and(|node| node.table() != TableName::DocNode)
        || work_run.table() != TableName::WorkRun
    {
        return Err(invalid("Source stage identity is incomplete"));
    }
    let identity = serde_json::to_vec(&(
        "source-stage-v1",
        space,
        plan_id,
        kind,
        revision,
        node,
        work_run,
    ))
    .map_err(|error| invalid(&error.to_string()))?;
    Ok(RecordId::new(
        TableName::WorkArtifact,
        Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity),
    ))
}

impl SourceStageContext {
    fn from_payload(space: &str, payload: &Value) -> Result<Option<Self>, PipelineError> {
        let configuration = payload.get("_source_configuration");
        let plan = payload.get("_source_plan");
        if configuration.is_none() && plan.is_none() {
            // Retained unversioned staging is never upgraded here. F permits
            // initial compatibility only while no replanning history exists.
            return Ok(None);
        }
        let configuration: SpaceConfigToken = serde_json::from_value(
            configuration
                .cloned()
                .ok_or_else(|| invalid("Source stage has no captured configuration"))?,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        let publication: PublicationFence = serde_json::from_value(
            payload
                .get("_publication")
                .cloned()
                .ok_or_else(|| invalid("Source stage has no captured publication"))?,
        )
        .map_err(|error| invalid(&error.to_string()))?;
        let plan_id = plan
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("Source stage has no captured plan identity"))?;
        if configuration.space_id != space
            || configuration.config_id.is_empty()
            || configuration.policy_id.is_empty()
            || publication.source_id.table() != TableName::Source
            || publication.generation.is_empty()
            || publication.revision_id.is_empty()
            || publication.operation_id.is_empty()
            || plan_id != source_plan_id(&publication, &configuration)
        {
            return Err(invalid(
                "Source stage plan does not match its captured authority",
            ));
        }
        Ok(Some(Self {
            publication,
            configuration,
            plan_id: plan_id.into(),
        }))
    }

    /// Read only the original work item's immutable plan payload. A replacement
    /// queue item has a different ID and cannot lend its plan to this WorkRun.
    pub(crate) async fn load(
        storage: &StorageClient,
        space: &str,
        work_run: &RecordId,
    ) -> Result<Option<Self>, PipelineError> {
        let run = WorkRunRepository::new(storage.clone())
            .get(space, work_run)
            .await?
            .ok_or_else(|| PipelineError::NotFound(work_run.to_string()))?;
        let item = WorkItemRepository::new(storage.clone())
            .get(space, run.work_item.id())
            .await?;
        let Some(item) = item else {
            // Retained unversioned staging helpers may have only a WorkRun.
            // No finalizer can accept this as a versioned producer attempt.
            return Ok(None);
        };
        let context = Self::from_payload(space, &item.payload)?;
        if let Some(context) = &context {
            context.ensure_current(storage).await?;
        }
        Ok(context)
    }

    pub(crate) fn require_source(&self, source: &RecordId) -> Result<(), PipelineError> {
        if source != &self.publication.source_id {
            return Err(invalid(
                "Source stage producer belongs to another evidence source",
            ));
        }
        Ok(())
    }

    pub(crate) fn artifact_id(
        &self,
        kind: &str,
        revision: &RecordId,
        node: Option<&RecordId>,
        work_run: &RecordId,
    ) -> Result<RecordId, PipelineError> {
        source_stage_artifact_id(
            &self.configuration.space_id,
            &self.plan_id,
            kind,
            revision,
            node,
            work_run,
        )
    }

    pub(crate) fn tag(&self, data: &mut Value) -> Result<(), PipelineError> {
        let object = data
            .as_object_mut()
            .ok_or_else(|| invalid("Source artifact payload must be an object"))?;
        object.insert("_source_plan".into(), Value::String(self.plan_id.clone()));
        object.insert(
            "_source_configuration".into(),
            serde_json::to_value(&self.configuration)
                .map_err(|error| invalid(&error.to_string()))?,
        );
        object.insert(
            "_publication".into(),
            serde_json::to_value(&self.publication).map_err(|error| invalid(&error.to_string()))?,
        );
        Ok(())
    }

    /// Check the same captured context before dispatch, replay or persistence.
    pub(crate) async fn ensure_current(
        &self,
        storage: &StorageClient,
    ) -> Result<(), PipelineError> {
        let current = SpaceConfigRepository::new(storage.clone())
            .resolve_space(&self.configuration.space_id)
            .await
            .map_err(|error| match error {
                SpaceResolutionError::Storage(error) => PipelineError::StorageError(error),
                SpaceResolutionError::Authorization(error) => {
                    PipelineError::ConfigError(format!("provider_authorization: {error}"))
                }
            })?;
        if current.token != self.configuration {
            return Err(invalid(
                "Source stage configuration changed; a new versioned plan is required",
            ));
        }
        let head = SourcePublicationRepository::new(storage.clone())
            .head(&self.configuration.space_id, &self.publication.source_id)
            .await?
            .ok_or_else(|| invalid("Source stage publication disappeared"))?;
        if head.lifecycle != SourceVisibility::Active
            || head.generation != self.publication.generation
            || head.operation_id != self.publication.operation_id
            || head.desired_revision.as_deref() != Some(self.publication.revision_id.as_str())
        {
            return Err(invalid("Source stage captured publication is stale"));
        }
        Ok(())
    }

    pub(crate) async fn replay(
        &self,
        storage: &StorageClient,
        artifact_id: &RecordId,
        kind: &str,
        work_run: &RecordId,
    ) -> Result<Option<WorkArtifact>, PipelineError> {
        self.ensure_current(storage).await?;
        let artifact = WorkArtifactRepository::new(storage.clone())
            .get(&self.configuration.space_id, artifact_id)
            .await?;
        if let Some(artifact) = &artifact {
            if artifact.artifact_type != kind
                || artifact.work_run.id() != work_run
                || Self::from_payload(&self.configuration.space_id, &artifact.data)?.as_ref()
                    != Some(self)
            {
                return Err(invalid(
                    "Persisted source artifact belongs to another producer plan",
                ));
            }
        }
        Ok(artifact)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;

    fn context(generation: u64) -> SourceStageContext {
        let publication = PublicationFence {
            source_id: RecordId::new(TableName::Source, Uuid::from_u128(1)),
            generation: "original-generation".into(),
            revision_id: "immutable-revision".into(),
            operation_id: "operation".into(),
        };
        let configuration = SpaceConfigToken {
            space_id: "named-space".into(),
            config_id: "config".into(),
            generation,
            policy_id: "policy".into(),
        };
        SourceStageContext {
            plan_id: source_plan_id(&publication, &configuration),
            publication,
            configuration,
        }
    }

    #[test]
    fn stage_identity_is_stable_and_separates_plan_kind_revision_node_and_run() {
        let first = context(1);
        let revision = RecordId::new_v4(TableName::DocRevision);
        let node = RecordId::new_v4(TableName::DocNode);
        let run = RecordId::new_v4(TableName::WorkRun);
        let id = first
            .artifact_id("knowledge_candidates", &revision, Some(&node), &run)
            .expect("id");
        assert_eq!(
            id,
            first
                .artifact_id("knowledge_candidates", &revision, Some(&node), &run)
                .expect("same")
        );
        let others = [
            context(2).artifact_id("knowledge_candidates", &revision, Some(&node), &run),
            first.artifact_id("extraction_plan", &revision, Some(&node), &run),
            first.artifact_id(
                "knowledge_candidates",
                &RecordId::new_v4(TableName::DocRevision),
                Some(&node),
                &run,
            ),
            first.artifact_id("knowledge_candidates", &revision, None, &run),
            first.artifact_id(
                "knowledge_candidates",
                &revision,
                Some(&node),
                &RecordId::new_v4(TableName::WorkRun),
            ),
        ];
        for other in others {
            assert_ne!(id, other.expect("separate identity"));
        }
    }

    #[test]
    fn stage_tags_preserve_captured_configuration_and_reject_partial_or_relabelled_context() {
        let original = context(1);
        let mut payload = serde_json::json!({"assertions": []});
        original.tag(&mut payload).expect("tag");
        assert_eq!(
            SourceStageContext::from_payload("named-space", &payload).expect("read"),
            Some(original.clone())
        );
        assert!(SourceStageContext::from_payload("foreign-space", &payload).is_err());
        let mut relabelled = payload.clone();
        relabelled["_source_configuration"]["generation"] = Value::from(2);
        assert!(SourceStageContext::from_payload("named-space", &relabelled).is_err());
        for missing in ["_source_configuration", "_source_plan", "_publication"] {
            let mut partial = payload.clone();
            partial.as_object_mut().expect("object").remove(missing);
            assert!(SourceStageContext::from_payload("named-space", &partial).is_err());
        }
        assert!(
            SourceStageContext::from_payload("named-space", &serde_json::json!({}))
                .expect("legacy staging")
                .is_none()
        );
    }
}
