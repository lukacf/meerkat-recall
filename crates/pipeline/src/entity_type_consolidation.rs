//! Entity type consolidation worker.
//!
//! Periodic worker that finds entities with missing or unknown types
//! and classifies them based on their relationship context.
//! This is a deterministic heuristic classifier, not an LLM call.

use std::collections::HashMap;

use elephant_storage::{
    EntityRepository, RelRepository, SearchCriteria, SearchRepository, StorageClient,
};
use elephant_types::records::entity::EntityType;
use elephant_types::records::rel::Rel;

use crate::error::PipelineResult;
use crate::maintenance_commit::{contains_authorization, PlanningTruthMaintenanceStorage};

/// Configuration for entity type consolidation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EntityTypeConsolidationConfig {
    /// Maximum entities to process per sweep.
    pub max_entities: usize,
    /// Minimum relationship count to attempt classification.
    pub min_rels_for_classification: usize,
}

impl Default for EntityTypeConsolidationConfig {
    fn default() -> Self {
        Self {
            max_entities: 100,
            min_rels_for_classification: 1,
        }
    }
}

/// Result of an entity type consolidation sweep.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConsolidationResult {
    /// Number of entities scanned.
    pub entities_scanned: usize,
    /// Number of entities that were classified and updated.
    pub entities_classified: usize,
    /// Number of entities skipped (insufficient signal or already typed).
    pub entities_skipped: usize,
}

/// Runs entity type consolidation for the given space.
///
/// 1. Finds entities where entity_type is Topic (the default/unknown type).
/// 2. For each entity, loads its relationships (outgoing + incoming).
/// 3. Classifies based on relationship patterns.
/// 4. Commits classifications with their observed relationships atomically.
///
/// # Errors
///
/// Returns an error if storage operations fail.
pub async fn execute_entity_type_sweep(
    entity_repo: &EntityRepository,
    _rel_repo: &RelRepository,
    space_id: &str,
    config: &EntityTypeConsolidationConfig,
) -> PipelineResult<ConsolidationResult> {
    // The retained repository-shaped entry uses the entity's owning store for
    // every read and the one canonical commit, preventing cross-client writes.
    let storage = entity_repo.storage_client();
    let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), space_id, None).await?;
    let result = prepare_entity_type_sweep(&planning, storage, space_id, config).await?;
    planning
        .commit(
            "entity_type_sweep",
            space_id,
            &serde_json::json!(config),
            &result,
        )
        .await?;
    Ok(result)
}

/// Stage classifications in the caller's original maintenance epoch.
/// This helper neither writes live records nor commits the shared plan.
pub(crate) async fn prepare_entity_type_sweep(
    planning: &PlanningTruthMaintenanceStorage,
    storage: &StorageClient,
    space_id: &str,
    config: &EntityTypeConsolidationConfig,
) -> PipelineResult<ConsolidationResult> {
    let criteria = SearchCriteria::new()
        .with_space_id(space_id)
        .with_entity_type(EntityType::new("topic"))
        .with_limit(config.max_entities);
    let live = EntityRepository::new(storage.clone())
        .search(&criteria)
        .await?
        .results;
    let entities = planning.current_rows(live, |row| {
        row.entity_type == EntityType::new("topic")
            && row.status == elephant_types::records::entity::EntityStatus::Active
            && row.redirect_to.is_none()
    })?;
    let rel_repo = RelRepository::new(storage.clone());
    let mut result = ConsolidationResult {
        entities_scanned: 0,
        entities_classified: 0,
        entities_skipped: 0,
    };
    for entity in entities.into_iter().take(config.max_entities) {
        result.entities_scanned += 1;
        let entity_id = &entity.base.id;
        let mut live = rel_repo.find_outgoing(space_id, entity_id).await?;
        live.extend(rel_repo.find_incoming(space_id, entity_id).await?);
        let relationships = planning.current_rows(live, |rel| {
            rel.subject.id() == entity_id || rel.object.id() == entity_id
        })?;
        // A type inferred from a stricter partition must not become visible
        // through a less restricted entity. Classification does not reclassify
        // the entity's opaque policy; only authorized evidence contributes.
        let mut outgoing = Vec::new();
        let mut incoming = Vec::new();
        for rel in relationships {
            if !contains_authorization(&entity.base, &rel.base)? {
                continue;
            }
            if rel.subject.id() == entity_id {
                outgoing.push(rel.clone());
            }
            if rel.object.id() == entity_id {
                incoming.push(rel);
            }
        }
        if outgoing.len() + incoming.len() < config.min_rels_for_classification {
            result.entities_skipped += 1;
            continue;
        }
        match classify_from_relationships(&outgoing, &incoming) {
            Some(new_type) if new_type != entity.entity_type => {
                let mut updated = entity.clone();
                updated.entity_type = new_type;
                updated.base.updated_at = chrono::Utc::now();
                planning.stage(Some(entity), updated)?;
                result.entities_classified += 1;
            }
            _ => result.entities_skipped += 1,
        }
    }
    Ok(result)
}

/// Signal counter for entity type classification.
///
/// Tracks how many relationship-based signals point to each entity type.
/// Uses a HashMap keyed by type string since EntityType is a freeform newtype.
#[derive(Debug, Default)]
struct TypeSignals {
    counts: HashMap<String, usize>,
}

impl TypeSignals {
    fn add(&mut self, entity_type: EntityType) {
        *self
            .counts
            .entry(entity_type.as_str().to_string())
            .or_insert(0) += 1;
    }

    /// Returns the entity type with the most signals, or None if
    /// there are no signals or there is a tie for the top count.
    fn best(&self) -> Option<EntityType> {
        let mut best_type = None;
        let mut best_count = 0;
        let mut is_tie = false;

        for (type_str, &count) in &self.counts {
            if count > best_count {
                best_type = Some(EntityType::new(type_str));
                best_count = count;
                is_tie = false;
            } else if count == best_count && count > 0 {
                is_tie = true;
            }
        }

        if is_tie {
            None
        } else {
            best_type
        }
    }
}

/// Classifies an entity type based on its relationships.
///
/// This is a pure function -- no async, no storage. It counts signals
/// from relationship types and returns the entity type with the most
/// signals, or `None` if there are insufficient or ambiguous signals.
///
/// Classification heuristics:
/// - Subject of "works_at", "employed_by", "member_of" -> Person
/// - Object of "works_at", "employed_by" -> Org
/// - Subject of "located_in", "lives_in" -> Person; "headquartered_in" -> Org
/// - Object of "located_in", "based_in", "headquartered_in" -> Location
/// - Subject of "has_title", "has_role" -> Person
/// - Subject of "has_member", "has_employee" -> Org (the org has members)
/// - Object of "has_member", "has_employee" -> Person (they are a member)
/// - Subject of "founded", "co_founded" -> Person
/// - Object of "founded", "acquired" -> Org
/// - Subject of "runs_on" -> App; Object of "runs_on" -> Device
#[must_use]
pub fn classify_from_relationships(outgoing: &[Rel], incoming: &[Rel]) -> Option<EntityType> {
    let mut signals = TypeSignals::default();

    // Outgoing relationships: this entity is the subject
    for rel in outgoing {
        match rel.rel_type.as_str() {
            // Subject of employment-like rels -> Person
            "works_at" | "employed_by" | "works_for" => signals.add(EntityType::new("person")),
            // Subject of membership -> Person
            "member_of" | "belongs_to" => signals.add(EntityType::new("person")),
            // Subject of title/role -> Person
            "has_title" | "has_role" | "holds_position" => signals.add(EntityType::new("person")),
            // Subject of "has_member" / "has_employee" -> Org (the org has members)
            "has_member" | "has_employee" | "employs" => signals.add(EntityType::new("org")),
            // Subject of personal location rels -> Person
            "located_in" | "lives_in" | "resides_in" => signals.add(EntityType::new("person")),
            // Subject of organizational location rels -> Org
            "based_in" | "headquartered_in" => signals.add(EntityType::new("org")),
            // Subject of founding -> Person
            "founded" | "co_founded" => signals.add(EntityType::new("person")),
            // Subject of corporate actions -> Org
            "acquired" | "merged_with" | "partnered_with" => signals.add(EntityType::new("org")),
            // Subject of creation -> Person
            "developed" | "created" | "built" => signals.add(EntityType::new("person")),
            // Subject of "runs_on" -> App
            "runs_on" | "installed_on" => signals.add(EntityType::new("app")),
            _ => {}
        }
    }

    // Incoming relationships: this entity is the object
    for rel in incoming {
        match rel.rel_type.as_str() {
            // Object of "works_at" / "employed_by" -> Org
            "works_at" | "employed_by" | "works_for" => signals.add(EntityType::new("org")),
            // Object of membership -> Org
            "member_of" | "belongs_to" => signals.add(EntityType::new("org")),
            // Object of location-like rels -> Location
            "located_in" | "based_in" | "headquartered_in" | "lives_in" | "resides_in" => {
                signals.add(EntityType::new("location"));
            }
            // Object of founding/acquisition -> Org
            "founded" | "co_founded" | "acquired" => signals.add(EntityType::new("org")),
            // Object of "runs_on" -> Device
            "runs_on" | "installed_on" => signals.add(EntityType::new("device")),
            // Object of creation -> App
            "developed" | "created" | "built" => signals.add(EntityType::new("app")),
            // Object of "has_member" / "has_employee" -> Person (they are a member)
            "has_member" | "has_employee" | "employs" => signals.add(EntityType::new("person")),
            _ => {}
        }
    }

    signals.best()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;
    use elephant_types::ids::TableName;
    use elephant_types::records::base::{BaseFields, Provenance, Security, SubjectsState};
    use elephant_types::records::rel::ClaimStatus;
    use elephant_types::{Link, RecordId};

    async fn atomic_type_preparation(embedded: bool, stale: bool) {
        use elephant_storage::{Repository, SpaceConfigRepository, SystemEventRepository};
        use elephant_types::records::{Entity, SpaceConfig};
        let storage = if embedded {
            StorageClient::new_embedded().await
        } else {
            StorageClient::new_memory().await
        }
        .unwrap();
        if !embedded {
            storage
                .query_checked(concat!(
                    include_str!("../../migrations/sql/031_source_publication.surql"),
                    "\n",
                    include_str!("../../migrations/sql/046_source_support_index_fence.surql")
                ))
                .await
                .unwrap();
        }
        SpaceConfigRepository::new(storage.clone())
            .create(SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "test_space".into(),
                    "test".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .unwrap();
        let entities = EntityRepository::new(storage.clone());
        let rels = RelRepository::new(storage.clone());
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::Entity),
            "test_space".into(),
            "test".into(),
        );
        base.subjects_state = SubjectsState::Complete;
        let entity = entities
            .create(Entity::new(
                base,
                EntityType::new("topic"),
                "Type target".into(),
            ))
            .await
            .unwrap();
        let mut rel = make_rel("unclassified_relation");
        rel.subject = Link::new(entity.base.id.clone());
        let rel = rels.create(rel).await.unwrap();
        let planning = PlanningTruthMaintenanceStorage::new(storage.clone(), "test_space", None)
            .await
            .unwrap();
        let mut pending = rel.clone();
        pending.rel_type = "works_at".into();
        planning.stage(Some(rel.clone()), pending).unwrap();
        let result = prepare_entity_type_sweep(
            &planning,
            &storage,
            "test_space",
            &EntityTypeConsolidationConfig::default(),
        )
        .await
        .unwrap();
        assert_eq!(
            result.entities_classified, 1,
            "type pass must see earlier relation overlay"
        );
        assert_eq!(
            entities
                .get("test_space", &entity.base.id)
                .await
                .unwrap()
                .unwrap()
                .entity_type,
            EntityType::new("topic")
        );
        if stale {
            let mut changed = rel.clone();
            changed.rel_type = "later_concurrent_relation".into();
            rels.update("test_space", &rel.base.id, changed)
                .await
                .unwrap();
        }
        let committed = planning
            .commit(
                "entity_type_sweep",
                "test_space",
                &serde_json::json!({"fixture":true}),
                &result,
            )
            .await;
        let persisted = entities
            .get("test_space", &entity.base.id)
            .await
            .unwrap()
            .unwrap();
        let events = SystemEventRepository::new(storage)
            .list("test_space", 100, None)
            .await
            .unwrap();
        if stale {
            assert!(committed.is_err());
            assert_eq!(persisted.entity_type, EntityType::new("topic"));
            assert!(events.is_empty());
        } else {
            committed.unwrap();
            assert_eq!(persisted.entity_type, EntityType::new("person"));
            assert!(!events.is_empty());
        }
    }

    #[tokio::test]
    async fn embedded_type_sweep_uses_overlay_and_rejects_stale_commit() {
        atomic_type_preparation(true, false).await;
        atomic_type_preparation(true, true).await;
    }
    #[tokio::test]
    async fn surreal_type_sweep_uses_overlay_and_rejects_stale_commit() {
        atomic_type_preparation(false, false).await;
        atomic_type_preparation(false, true).await;
    }

    fn make_rel(rel_type: &str) -> Rel {
        let id = RecordId::new_v4(TableName::Rel);
        let mut base = BaseFields::new(id, "test_space".to_string(), "test_agent".to_string());
        base.security = Security {
            policy_id: "default".to_string(),
            level: 0,
            labels: vec![],
            handling: vec![],
        };
        base.provenance = Provenance::default();
        base.subjects_state = SubjectsState::Complete;

        let subject_id = RecordId::new_v4(TableName::Entity);
        let object_id = RecordId::new_v4(TableName::Entity);

        Rel {
            base,
            subject: Link::new(subject_id),
            object: Link::new(object_id),
            rel_type: rel_type.to_string(),
            status: ClaimStatus::Candidate,
            confidence: 0.9,
            validity_window: None,
            evidence: vec![],
            temporal_audit: None,
        }
    }

    #[test]
    fn test_classify_entity_with_works_at_rel() {
        // Entity has outgoing "works_at" relationship -> classified as Person
        let outgoing = vec![make_rel("works_at")];
        let incoming = vec![];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("person")));
    }

    #[test]
    fn test_classify_entity_as_org_when_target_of_works_at() {
        // Entity is the target of "works_at" -> classified as Org
        let outgoing = vec![];
        let incoming = vec![make_rel("works_at")];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("org")));
    }

    #[test]
    fn test_classify_entity_with_no_rels() {
        // Entity with no relationships -> stays None (no signal)
        let outgoing = vec![];
        let incoming = vec![];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, None);
    }

    #[test]
    fn test_classify_entity_with_mixed_signals() {
        // 2 person signals vs 1 org signal -> Person wins
        let outgoing = vec![
            make_rel("works_at"),  // person signal
            make_rel("member_of"), // person signal
        ];
        let incoming = vec![
            make_rel("works_at"), // org signal (entity is target)
        ];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("person")));
    }

    #[test]
    fn test_classify_entity_with_tied_signals_returns_none() {
        // Equal person and org signals -> ambiguous, return None
        let outgoing = vec![
            make_rel("works_at"), // person signal
        ];
        let incoming = vec![
            make_rel("works_at"), // org signal
        ];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, None);
    }

    #[test]
    fn test_classify_entity_as_location() {
        // Entity is the target of location rels -> Location
        let outgoing = vec![];
        let incoming = vec![make_rel("located_in"), make_rel("based_in")];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("location")));
    }

    #[test]
    fn test_classify_entity_as_org_with_multiple_signals() {
        // Entity has multiple org signals
        let outgoing = vec![
            make_rel("has_employee"),     // org signal
            make_rel("headquartered_in"), // org signal
        ];
        let incoming = vec![
            make_rel("founded"), // org signal (target of founding)
        ];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("org")));
    }

    #[test]
    fn test_classify_entity_with_unknown_rel_types() {
        // Entity with only unknown relationship types -> no signal
        let outgoing = vec![make_rel("some_custom_rel")];
        let incoming = vec![make_rel("another_custom_rel")];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, None);
    }

    #[test]
    fn test_classify_entity_as_app() {
        // Entity is the subject of "runs_on" -> App
        let outgoing = vec![make_rel("runs_on")];
        let incoming = vec![];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("app")));
    }

    #[test]
    fn test_classify_entity_as_device() {
        // Entity is the target of "runs_on" -> Device
        let outgoing = vec![];
        let incoming = vec![make_rel("runs_on")];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("device")));
    }

    #[test]
    fn test_classify_person_with_employed_by() {
        // Entity has outgoing "employed_by" -> Person
        let outgoing = vec![make_rel("employed_by")];
        let incoming = vec![];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("person")));
    }

    #[test]
    fn test_classify_org_strong_majority() {
        // 3 org signals vs 1 person signal -> Org
        let outgoing = vec![
            make_rel("has_member"),       // org
            make_rel("has_employee"),     // org
            make_rel("headquartered_in"), // org
        ];
        let incoming = vec![
            make_rel("has_employee"), // person (target of has_employee)
        ];

        let result = classify_from_relationships(&outgoing, &incoming);
        assert_eq!(result, Some(EntityType::new("org")));
    }

    #[test]
    fn test_default_config() {
        let config = EntityTypeConsolidationConfig::default();
        assert_eq!(config.max_entities, 100);
        assert_eq!(config.min_rels_for_classification, 1);
    }
}
