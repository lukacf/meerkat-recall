//! Retained supersession prepares complete reads/effects before the sole
//! canonical transaction. The public wrappers preserve repository signatures.
use chrono::Utc;
use elephant_storage::{Repository, StorageClient};
use elephant_types::records::assertion::ResolutionStatus;
use elephant_types::records::doc_revision::RevisionStatus;
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::{Assertion, Attribute, AuditAction, AuditLog, DocRevision, Rel};
use elephant_types::{RecordId, TableName};
use serde::{de::DeserializeOwned, Serialize};

use super::{PipelineError, PipelineResult, SupersessionResult};
use crate::maintenance_commit::{same_authorization, PlanRecord, PlanningTruthMaintenanceStorage};
use crate::truth_maintenance::TruthMaintenanceStorage;

pub(super) trait Supersedable: PlanRecord + Serialize + DeserializeOwned {
    fn supersede(&mut self, replacement: &Self) -> PipelineResult<()>;
    fn link_previous(&mut self, _previous: &Self) {}
}

impl Supersedable for DocRevision {
    fn supersede(&mut self, replacement: &Self) -> PipelineResult<()> {
        if self.doc != replacement.doc {
            return Err(PipelineError::InvalidInput(
                "Replacement revision belongs to a different document".into(),
            ));
        }
        self.status = RevisionStatus::Superseded;
        self.base.updated_at = Utc::now();
        Ok(())
    }
    fn link_previous(&mut self, previous: &Self) {
        self.supersedes = Some(elephant_types::Link::new(previous.base.id.clone()));
        self.base.updated_at = Utc::now();
    }
}
impl Supersedable for Assertion {
    fn supersede(&mut self, _replacement: &Self) -> PipelineResult<()> {
        self.resolution_status = ResolutionStatus::Superseded;
        self.base.updated_at = Utc::now();
        Ok(())
    }
}
impl Supersedable for Rel {
    fn supersede(&mut self, _replacement: &Self) -> PipelineResult<()> {
        self.status = ClaimStatus::Superseded;
        self.base.updated_at = Utc::now();
        Ok(())
    }
}
impl Supersedable for Attribute {
    fn supersede(&mut self, _replacement: &Self) -> PipelineResult<()> {
        self.status = ClaimStatus::Superseded;
        self.base.updated_at = Utc::now();
        Ok(())
    }
}

pub(super) async fn supersede<T, R>(
    storage: StorageClient,
    repository: &R,
    old_id: &RecordId,
    new_id: &RecordId,
    space: &str,
) -> PipelineResult<SupersessionResult>
where
    T: Supersedable,
    R: Repository<T> + Sync,
{
    if old_id == new_id || old_id.table() != new_id.table() {
        return Err(PipelineError::InvalidInput(
            "Supersession needs distinct same-kind records".into(),
        ));
    }
    let planning = PlanningTruthMaintenanceStorage::new(storage, space, None).await?;
    let input = serde_json::json!({"old":old_id,"replacement":new_id});
    let scope = old_id.to_string();
    if let Some(previous) = planning.replay("supersession", &scope, &input).await? {
        return Ok(previous);
    }
    let old = repository
        .get(space, old_id)
        .await?
        .ok_or_else(|| PipelineError::NotFound(format!("Superseded record not found: {old_id}")))?;
    let new = repository.get(space, new_id).await?.ok_or_else(|| {
        PipelineError::NotFound(format!("Replacement record not found: {new_id}"))
    })?;
    prepare_supersession(&planning, old, new).await?;
    let result = SupersessionResult {
        old_id: old_id.clone(),
        new_id: new_id.clone(),
        superseded_at: Utc::now(),
    };
    planning
        .commit("supersession", &scope, &input, &result)
        .await?;
    Ok(result)
}

pub(super) async fn prepare_supersession<T: Supersedable>(
    planning: &PlanningTruthMaintenanceStorage,
    old: T,
    new: T,
) -> PipelineResult<()> {
    if !same_authorization(old.base(), new.base())? {
        return Err(PipelineError::InvalidInput(
            "Supersession crosses security or subject authority".into(),
        ));
    }
    planning.observe(&old)?;
    planning.observe(&new)?;
    let mut updated_old = old.clone();
    updated_old.supersede(&new)?;
    let mut updated_new = new.clone();
    updated_new.link_previous(&old);
    stage_with_audit(planning, old, updated_old).await?;
    if serde_json::to_value(&new).map_err(|e| PipelineError::InvalidState(e.to_string()))?
        != serde_json::to_value(&updated_new)
            .map_err(|e| PipelineError::InvalidState(e.to_string()))?
    {
        stage_with_audit(planning, new, updated_new).await?;
    }
    Ok(())
}

async fn stage_with_audit<T: Supersedable>(
    planning: &PlanningTruthMaintenanceStorage,
    before: T,
    after: T,
) -> PipelineResult<()> {
    let mut base = before.base().clone();
    base.id = RecordId::new_v4(TableName::AuditLog);
    base.created_at = Utc::now();
    base.updated_at = base.created_at;
    let before_value =
        serde_json::to_value(&before).map_err(|e| PipelineError::InvalidState(e.to_string()))?;
    let after_value =
        serde_json::to_value(&after).map_err(|e| PipelineError::InvalidState(e.to_string()))?;
    let record = before.base().id.clone();
    planning.stage(Some(before), after)?;
    planning
        .create_audit_log(AuditLog {
            base,
            action: AuditAction::Update,
            actor: "reprocess_supersession".into(),
            record,
            before: Some(before_value),
            after: Some(after_value),
            timestamp: Utc::now(),
            reason: Some("Explicit temporal supersession".into()),
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use elephant_types::records::{BaseFields, Entity, EntityType};
    const SPACE: &str = "supersession-oracle";

    async fn rows(embedded: bool) -> (StorageClient, elephant_storage::RelRepository, Rel, Rel) {
        let client = if embedded {
            StorageClient::new_embedded().await.unwrap()
        } else {
            let client = StorageClient::new_memory().await.unwrap();
            elephant_migrations::apply_schema(&client).await.unwrap();
            client
        };
        elephant_storage::SpaceConfigRepository::new(client.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    SPACE.into(),
                    "oracle".into(),
                ),
                embedding_provider: "unconfigured".into(),
                embedding_model: "unconfigured".into(),
                embedding_dims: 1,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        let entities = elephant_storage::EntityRepository::new(client.clone());
        let base = |table| BaseFields::new(RecordId::new_v4(table), SPACE.into(), "oracle".into());
        let subject = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("person"),
                "Ada".into(),
            ))
            .await
            .unwrap();
        let object = entities
            .create(Entity::new(
                base(TableName::Entity),
                EntityType::new("organization"),
                "Lab".into(),
            ))
            .await
            .unwrap();
        let repo = elephant_storage::RelRepository::new(client.clone());
        let old = repo
            .create(Rel {
                base: base(TableName::Rel),
                subject: elephant_types::Link::new(subject.base.id),
                object: elephant_types::Link::new(object.base.id),
                rel_type: "works_at".into(),
                status: ClaimStatus::Candidate,
                confidence: 0.9,
                validity_window: None,
                evidence: vec![],
                temporal_audit: None,
            })
            .await
            .unwrap();
        let mut new = old.clone();
        new.base = base(TableName::Rel);
        let new = repo.create(new).await.unwrap();
        (client, repo, old, new)
    }

    #[tokio::test]
    async fn missing_replacement_does_not_supersede_old_record() {
        for embedded in [true, false] {
            let (client, repo, old, _) = rows(embedded).await;
            let result = supersede::<Rel, _>(
                client,
                &repo,
                &old.base.id,
                &RecordId::new_v4(TableName::Rel),
                SPACE,
            )
            .await;
            assert!(result.is_err());
            assert_eq!(repo.get(SPACE, &old.base.id).await.unwrap(), Some(old));
        }
    }

    #[tokio::test]
    async fn supersession_replays_exact_output_and_rejects_changed_replacement() {
        for embedded in [true, false] {
            let (client, repo, old, new) = rows(embedded).await;
            let first =
                supersede::<Rel, _>(client.clone(), &repo, &old.base.id, &new.base.id, SPACE)
                    .await
                    .unwrap();
            let replay =
                supersede::<Rel, _>(client.clone(), &repo, &old.base.id, &new.base.id, SPACE)
                    .await
                    .unwrap();
            assert_eq!(first.superseded_at, replay.superseded_at);
            assert_eq!(
                repo.get(SPACE, &old.base.id).await.unwrap().unwrap().status,
                ClaimStatus::Superseded
            );
            assert!(supersede::<Rel, _>(
                client,
                &repo,
                &old.base.id,
                &RecordId::new_v4(TableName::Rel),
                SPACE
            )
            .await
            .is_err());
        }
    }

    #[tokio::test]
    async fn late_transaction_failure_rolls_back_supersession_audit_and_outbox() {
        for embedded in [true, false] {
            let (client, repo, old, new) = rows(embedded).await;
            let planning = PlanningTruthMaintenanceStorage::new(client.clone(), SPACE, None)
                .await
                .unwrap();
            prepare_supersession(&planning, old.clone(), new)
                .await
                .unwrap();
            assert_eq!(
                repo.get(SPACE, &old.base.id).await.unwrap(),
                Some(old.clone())
            );
            let mut request = planning
                .prepare_request(
                    "supersession",
                    &old.base.id.to_string(),
                    &serde_json::json!({"old":old.base.id}),
                    &(),
                )
                .await
                .unwrap();
            let commit_id = request.commit_id.clone();
            // The duplicate create is after all semantic/audit effects and must
            // fail the same actual canonical transaction, not only preparation.
            request
                .plan
                .effects
                .push(elephant_storage::canonical_commit::CanonicalEffect {
                    before: None,
                    after: Some(elephant_storage::canonical_commit::CanonicalRecord::Rel(
                        Box::new(old.clone()),
                    )),
                });
            let canonical =
                elephant_storage::canonical_commit::CanonicalRepository::new(client.clone());
            assert!(canonical.commit(request).await.is_err());
            assert_eq!(repo.get(SPACE, &old.base.id).await.unwrap(), Some(old));
            assert!(canonical
                .receipt(SPACE, &commit_id)
                .await
                .unwrap()
                .is_none());
            assert!(elephant_storage::AuditLogRepository::new(client.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap()
                .is_empty());
            assert!(elephant_storage::SystemEventRepository::new(client)
                .list(SPACE, 100, None)
                .await
                .unwrap()
                .is_empty());
        }
    }
}
