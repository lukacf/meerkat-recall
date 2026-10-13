//! Read-through repositories used only while preparing a canonical promotion.
//! Writes accumulate typed effects; they never reach live storage. The storage
//! owner's canonical epoch fences all observed rows and query ranges at commit.

use elephant_storage::{
    canonical_commit::{CanonicalPlan, CanonicalRecord},
    AssertionRepository, EntityRepository, EventRepository, FreshnessRepository, RelRepository,
    Repository, StorageError, StorageResult,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::EntityStatus;
use elephant_types::{
    records::{Assertion, Entity, Event, Freshness, Rel},
    RecordId,
};
use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(super) struct PlanCapture(pub Arc<Mutex<CanonicalPlan>>);

impl PlanCapture {
    pub fn new(plan: CanonicalPlan) -> Self {
        Self(Arc::new(Mutex::new(plan)))
    }
    fn with<T>(&self, f: impl FnOnce(&mut CanonicalPlan) -> T) -> StorageResult<T> {
        let mut plan = self
            .0
            .lock()
            .map_err(|_| StorageError::QueryError("Promotion plan lock poisoned".into()))?;
        Ok(f(&mut plan))
    }
    pub fn finish(&self) -> StorageResult<CanonicalPlan> {
        self.with(|plan| plan.clone())
    }
    pub fn has_effect(&self, id: &RecordId) -> StorageResult<bool> {
        self.with(|plan| {
            plan.effects
                .iter()
                .filter_map(|effect| effect.after.as_ref())
                .any(|record| &record.base().id == id)
        })
    }
    pub fn dependency(&self, record: CanonicalRecord) -> StorageResult<()> {
        self.with(|plan| plan.dependencies.push(record))
    }
    fn effect(&self, before: Option<CanonicalRecord>, after: CanonicalRecord) -> StorageResult<()> {
        self.with(|plan| plan.push(before, Some(after)))?
    }
    pub fn outbox(&self, event: elephant_types::records::SystemEvent) -> StorageResult<()> {
        self.with(|plan| plan.outbox.push(event))
    }
}

pub(super) trait PlannedRecord: Clone + Serialize + DeserializeOwned + Send + Sync {
    fn base(&self) -> &BaseFields;
    fn canonical(self) -> CanonicalRecord;
    fn from_canonical(value: &CanonicalRecord) -> Option<Self>;
}
macro_rules! planned_record {
    ($record:ty, $variant:ident) => {
        impl PlannedRecord for $record {
            fn base(&self) -> &BaseFields {
                &self.base
            }
            fn canonical(self) -> CanonicalRecord {
                CanonicalRecord::$variant(Box::new(self))
            }
            fn from_canonical(value: &CanonicalRecord) -> Option<Self> {
                if let CanonicalRecord::$variant(record) = value {
                    Some((**record).clone())
                } else {
                    None
                }
            }
        }
    };
}
planned_record!(Entity, Entity);
planned_record!(Assertion, Assertion);
planned_record!(Rel, Rel);
planned_record!(elephant_types::records::Attribute, Attribute);
planned_record!(Event, Event);
planned_record!(Freshness, Freshness);

pub(super) struct PlannedRepository<T, R> {
    repository: R,
    capture: PlanCapture,
    marker: PhantomData<T>,
}
impl<T: PlannedRecord, R: Repository<T>> PlannedRepository<T, R> {
    pub fn new(repository: R, capture: &PlanCapture) -> Self {
        Self {
            repository,
            capture: capture.clone(),
            marker: PhantomData,
        }
    }
    pub async fn get(&self, space: &str, id: &RecordId) -> StorageResult<Option<T>> {
        if let Some(record) = self.capture.with(|plan| {
            plan.effects
                .iter()
                .rev()
                .find(|effect| {
                    effect
                        .after
                        .as_ref()
                        .or(effect.before.as_ref())
                        .is_some_and(|record| {
                            &record.base().id == id && record.base().space_id == space
                        })
                })
                .map(|effect| {
                    effect
                        .after
                        .as_ref()
                        .filter(|record| !record.base().is_deleted())
                        .and_then(T::from_canonical)
                })
        })? {
            // Some(None) is a planned deletion, not permission to reload the
            // still-live pre-transaction row from storage.
            return Ok(record);
        }
        let record = self.repository.get(space, id).await?;
        if let Some(record) = &record {
            self.capture.dependency(record.clone().canonical())?;
        }
        Ok(record)
    }
    pub async fn create(&self, record: T) -> StorageResult<T> {
        if self
            .get(&record.base().space_id, &record.base().id)
            .await?
            .is_some()
        {
            return Err(StorageError::Conflict(
                "Promotion create conflicts with an existing record".into(),
            ));
        }
        self.capture.effect(None, record.clone().canonical())?;
        Ok(record)
    }
    pub async fn update(&self, space: &str, id: &RecordId, record: T) -> StorageResult<T> {
        if record.base().space_id != space || &record.base().id != id {
            return Err(StorageError::Conflict(
                "Promotion update changed record identity".into(),
            ));
        }
        let before = self
            .get(space, id)
            .await?
            .ok_or_else(|| StorageError::NotFound(id.to_string()))?;
        self.capture
            .effect(Some(before.canonical()), record.clone().canonical())?;
        Ok(record)
    }
    fn overlay(&self, records: Vec<T>) -> StorageResult<Vec<T>> {
        for record in &records {
            self.capture.dependency(record.clone().canonical())?;
        }
        self.capture.with(|plan| {
            let mut by_id: std::collections::BTreeMap<String, T> = records
                .into_iter()
                .map(|record| (record.base().id.to_string(), record))
                .collect();
            for effect in &plan.effects {
                if let Some(before) = effect.before.as_ref().and_then(T::from_canonical) {
                    by_id.remove(&before.base().id.to_string());
                }
                if let Some(after) = effect.after.as_ref().and_then(T::from_canonical) {
                    if after.base().is_deleted() {
                        by_id.remove(&after.base().id.to_string());
                    } else {
                        by_id.insert(after.base().id.to_string(), after);
                    }
                }
            }
            by_id
                .into_values()
                .filter(|record| !record.base().is_deleted())
                .collect()
        })
    }
}

pub(super) type PlannedEntities = PlannedRepository<Entity, EntityRepository>;
pub(super) type PlannedAssertions = PlannedRepository<Assertion, AssertionRepository>;
pub(super) type PlannedRels = PlannedRepository<Rel, RelRepository>;
pub(super) type PlannedEvents = PlannedRepository<Event, EventRepository>;
pub(super) type PlannedFreshness = PlannedRepository<Freshness, FreshnessRepository>;

impl PlannedEntities {
    pub async fn find_by_identity_key_all(
        &self,
        space: &str,
        key: &str,
    ) -> StorageResult<Vec<Entity>> {
        Ok(self
            .overlay(self.repository.find_by_identity_key_all(space, key).await?)?
            .into_iter()
            .filter(|entity| {
                entity.base.space_id == space
                    && !entity.base.is_deleted()
                    && entity.status != EntityStatus::Merged
                    && entity.identity_keys.iter().any(|value| value == key)
            })
            .collect())
    }
}
impl PlannedAssertions {
    pub async fn find_by_assertion_key(
        &self,
        space: &str,
        key: &str,
    ) -> StorageResult<Option<Assertion>> {
        Ok(self
            .overlay(
                self.repository
                    .find_by_assertion_key(space, key)
                    .await?
                    .into_iter()
                    .collect(),
            )?
            .into_iter()
            .find(|assertion| assertion.base.space_id == space && assertion.assertion_key == key))
    }
}
impl PlannedRels {
    pub async fn find_for_slot(
        &self,
        space: &str,
        subject: &RecordId,
        object: &RecordId,
        predicate: &str,
    ) -> StorageResult<Vec<Rel>> {
        Ok(self
            .overlay(
                self.repository
                    .find_for_slot(space, subject, object, predicate)
                    .await?,
            )?
            .into_iter()
            .filter(|rel| {
                rel.base.space_id == space
                    && rel.subject.id() == subject
                    && rel.object.id() == object
                    && rel.rel_type == predicate
            })
            .collect())
    }
}
impl PlannedEvents {
    pub async fn find_all_by_lineage_key(
        &self,
        space: &str,
        lineage: &str,
    ) -> StorageResult<Vec<Event>> {
        Ok(self
            .overlay(
                self.repository
                    .find_all_by_lineage_key(space, lineage)
                    .await?,
            )?
            .into_iter()
            .filter(|event| {
                event.base.space_id == space
                    && event
                        .metadata
                        .get("lineage_key")
                        .and_then(serde_json::Value::as_str)
                        == Some(lineage)
            })
            .collect())
    }
}
impl PlannedFreshness {
    pub async fn find_by_target(
        &self,
        space: &str,
        target: &str,
    ) -> StorageResult<Option<Freshness>> {
        Ok(self
            .overlay(
                self.repository
                    .find_by_target(space, target)
                    .await?
                    .into_iter()
                    .collect(),
            )?
            .into_iter()
            .find(|freshness| freshness.base.space_id == space && freshness.target == target))
    }
}

pub(super) async fn record_claim_freshness(
    repository: &PlannedFreshness,
    target_id: &RecordId,
    target_type: &str,
    target_base: &BaseFields,
    observation: crate::freshness::ClaimFreshnessObservation<'_>,
) -> crate::error::PipelineResult<Freshness> {
    let existing = repository
        .find_by_target(&target_base.space_id, &target_id.to_string())
        .await?;
    let freshness = crate::freshness::prepare_claim_freshness(
        existing.as_ref(),
        target_id,
        target_type,
        target_base,
        observation,
    );
    if let Some(existing) = existing {
        Ok(repository
            .update(&target_base.space_id, &existing.base.id, freshness)
            .await?)
    } else {
        Ok(repository.create(freshness).await?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]
    use super::*;
    use elephant_storage::StorageClient;
    use elephant_types::{records::entity::EntityType, TableName};

    async fn entity_fixture() -> (EntityRepository, Entity) {
        let storage = StorageClient::new_embedded().await.expect("storage");
        let repository = EntityRepository::new(storage);
        let mut entity = Entity::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Entity),
                "partition".into(),
                "actor".into(),
            ),
            EntityType::new("person"),
            "Original name".into(),
        );
        entity.identity_keys = vec!["person:original".into()];
        let entity = repository.create(entity).await.expect("entity");
        (repository, entity)
    }

    #[tokio::test]
    async fn retirement_prefix_masks_live_deleted_entity_in_point_and_alias_queries() {
        let (repository, entity) = entity_fixture().await;
        let mut prefix = CanonicalPlan::default();
        prefix
            .push(Some(entity.clone().canonical()), None)
            .expect("retirement");
        let capture = PlanCapture::new(prefix);
        let planned = PlannedRepository::new(repository.clone(), &capture);
        assert!(planned
            .get("partition", &entity.base.id)
            .await
            .expect("point read")
            .is_none());
        assert!(planned
            .find_by_identity_key_all("partition", "person:original")
            .await
            .expect("alias read")
            .is_empty());
        assert_eq!(
            repository
                .get("partition", &entity.base.id)
                .await
                .expect("unchanged live row"),
            Some(entity)
        );
    }

    #[tokio::test]
    async fn retirement_prefix_masks_soft_deleted_entities_without_laundering_virtual_dependencies()
    {
        let (repository, entity) = entity_fixture().await;
        let mut retired = entity.clone();
        retired.base.deleted_at = Some(chrono::Utc::now());
        let mut prefix = CanonicalPlan::default();
        prefix
            .push(
                Some(entity.clone().canonical()),
                Some(retired.clone().canonical()),
            )
            .expect("retirement");
        let capture = PlanCapture::new(prefix);
        let planned = PlannedRepository::new(repository, &capture);
        assert!(planned
            .get("partition", &entity.base.id)
            .await
            .expect("point read")
            .is_none());
        assert!(planned
            .find_by_identity_key_all("partition", "person:original")
            .await
            .expect("alias read")
            .is_empty());
        for dependency in capture.finish().expect("plan").dependencies {
            assert_eq!(Entity::from_canonical(&dependency), Some(entity.clone()));
        }
    }

    #[tokio::test]
    async fn inverse_restoration_is_visible_and_later_effects_coalesce_from_original_before() {
        let (repository, mut entity) = entity_fixture().await;
        entity.base.deleted_at = Some(chrono::Utc::now());
        let entity = repository
            .update("partition", &entity.base.id, entity.clone())
            .await
            .expect("hidden baseline");
        let mut restored = entity.clone();
        restored.base.deleted_at = None;
        restored.display_name = "Independent baseline".into();
        let mut prefix = CanonicalPlan::default();
        prefix
            .push(
                Some(entity.clone().canonical()),
                Some(restored.clone().canonical()),
            )
            .expect("inverse restoration");
        let capture = PlanCapture::new(prefix);
        let planned = PlannedRepository::new(repository.clone(), &capture);
        assert_eq!(
            planned
                .get("partition", &entity.base.id)
                .await
                .expect("restored view"),
            Some(restored.clone())
        );
        let mut enriched = restored;
        enriched
            .aliases
            .push("New independently supported name".into());
        planned
            .update("partition", &entity.base.id, enriched.clone())
            .await
            .expect("sequential effect");
        let result = capture.finish().expect("plan");
        assert_eq!(result.effects.len(), 1);
        assert_eq!(
            result.effects[0]
                .before
                .as_ref()
                .and_then(Entity::from_canonical),
            Some(entity.clone())
        );
        assert_eq!(
            result.effects[0]
                .after
                .as_ref()
                .and_then(Entity::from_canonical),
            Some(enriched)
        );
        assert!(
            result.dependencies.is_empty(),
            "virtual restored row is not a live dependency"
        );
        assert!(repository
            .get("partition", &entity.base.id)
            .await
            .expect("still hidden before commit")
            .is_none());
    }
}
