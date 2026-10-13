//! Independent actual-planner definition oracles, imported by the existing
//! maintenance test module. Scripted planning is not semantic model evidence.

use super::*;
use crate::wiki_editorial::{
    EditorialDraft, EditorialVerification, PlannedWikiArticle, PlannedWikiSection,
    WikiCreationConfig, WikiEditorialConfig, WikiEditorialLlm, WikiGraphPlanner, WikiOrganization,
    WikiPlan,
};
use crate::PipelineResult;
use elephant_storage::{KnowledgeViewDefinitionRepository, KnowledgeViewRepository};
use elephant_types::records::KnowledgeViewDefinition;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

enum OrganizationMutation {
    ChangeContributor(Entity),
    DeleteDefinition(KnowledgeViewDefinition),
}

struct BoundaryPlanner {
    storage: StorageClient,
    originals: Vec<Entity>,
    mutation: Mutex<Option<OrganizationMutation>>,
    organization_calls: AtomicUsize,
    writer_calls: AtomicUsize,
}

impl BoundaryPlanner {
    fn new(
        storage: StorageClient,
        originals: &[Entity],
        mutation: Option<OrganizationMutation>,
    ) -> Self {
        Self {
            storage,
            originals: originals.to_vec(),
            mutation: Mutex::new(mutation),
            organization_calls: AtomicUsize::new(0),
            writer_calls: AtomicUsize::new(0),
        }
    }

    fn ids(&self) -> Vec<String> {
        self.originals
            .iter()
            .map(|entity| entity.base.id.to_string())
            .collect()
    }
}

#[async_trait::async_trait]
impl WikiEditorialLlm for BoundaryPlanner {
    fn validate_space(&self, space: &str) -> PipelineResult<()> {
        assert_eq!(space, SPACE);
        Ok(())
    }

    async fn plan_wiki(&self, prompt: String) -> PipelineResult<WikiPlan> {
        for original in &self.originals {
            assert!(prompt.contains(&original.base.id.to_string()));
            assert!(prompt.contains(original.summary.as_deref().unwrap()));
        }
        Ok(WikiPlan {
            language: "en".into(),
            articles: self
                .originals
                .iter()
                .map(|original| PlannedWikiArticle {
                    entity_id: original.base.id.to_string(),
                    title: original.display_name.clone(),
                    editorial_reason: "Original independent inventory fixture".into(),
                    supporting_entity_ids: vec![],
                    importance: 3,
                    page_role: "topic".into(),
                    temporal_relevance: "enduring".into(),
                    related_entity_ids: vec![],
                })
                .collect(),
        })
    }

    async fn organize_wiki(&self, prompt: String) -> PipelineResult<WikiOrganization> {
        let call = self.organization_calls.fetch_add(1, Ordering::SeqCst);
        let ids = self.ids();
        assert!(ids.iter().all(|id| prompt.contains(id)));
        // The actual planner has already captured and sent its original
        // inventory. Change storage at this exact boundary, without a sleep,
        // forged receipt, replacement epoch or independently staged plan.
        // The first call produces the taxonomy; the second assigns this one
        // bounded partition. Inject after the final original input is consumed.
        let mutation = if call == 1 {
            self.mutation.lock().unwrap().take()
        } else {
            None
        };
        match mutation {
            Some(OrganizationMutation::ChangeContributor(mut entity)) => {
                entity.summary = Some("Changed after original organization input".into());
                entity.base.updated_at = Utc::now();
                EntityRepository::new(self.storage.clone())
                    .update(SPACE, &entity.base.id.clone(), entity)
                    .await?;
            }
            Some(OrganizationMutation::DeleteDefinition(mut definition)) => {
                definition.base.deleted_at = Some(Utc::now());
                definition.base.updated_at = Utc::now();
                KnowledgeViewDefinitionRepository::new(self.storage.clone())
                    .upsert(definition)
                    .await?;
            }
            None => {}
        }
        Ok(WikiOrganization {
            language: "en".into(),
            title: "Independent definition inventory".into(),
            purpose: "Exercise one exact security partition".into(),
            featured_entity_ids: ids.clone(),
            sections: vec![PlannedWikiSection {
                id: "original-inputs".into(),
                title: "Original inputs".into(),
                description: "Both disconnected original contributors".into(),
                article_entity_ids: ids,
            }],
        })
    }

    async fn write_article(&self, _prompt: String) -> PipelineResult<EditorialDraft> {
        self.writer_calls.fetch_add(1, Ordering::SeqCst);
        Err(PipelineError::ValidationError(
            "D fixture stops after definition publication; no article supplied".into(),
        ))
    }

    async fn verify_article(&self, _prompt: String) -> PipelineResult<EditorialVerification> {
        Err(PipelineError::ValidationError(
            "D definition fixture has no model verification result".into(),
        ))
    }

    fn writer_model(&self) -> &str {
        "qualification-definition-boundary"
    }

    fn verifier_model(&self) -> &str {
        "qualification-no-semantic-verdict"
    }
}

fn config() -> WikiCreationConfig {
    WikiCreationConfig {
        max_articles: 2,
        publish: false,
        ..WikiCreationConfig::default()
    }
}

async fn original_entities(storage: &StorageClient) -> Vec<Entity> {
    let entities = EntityRepository::new(storage.clone());
    let mut result = Vec::new();
    for name in ["Alpha original topic", "Zulu disconnected organizer"] {
        let mut entity = Entity::new(
            base(TableName::Entity),
            EntityType::new("person"),
            name.into(),
        );
        entity.summary = Some(format!(
            "{name} is an independently recorded intrinsic topic."
        ));
        let entity = entities.create(entity).await.unwrap();
        assert!(entity.base.provenance.sources.is_empty());
        result.push(entity);
    }
    result
}

fn planner(storage: &StorageClient, llm: Arc<BoundaryPlanner>) -> WikiGraphPlanner {
    WikiGraphPlanner::new(storage.clone(), llm, WikiEditorialConfig::default())
}

async fn original_commit_identity(storage: &StorageClient, originals: &[Entity]) -> String {
    let mut ids = originals
        .iter()
        .map(|entity| entity.base.id.to_string())
        .collect::<Vec<_>>();
    ids.sort();
    let scope = request_fingerprint(&("explicit_candidate_subset", ids)).unwrap();
    PlanningTruthMaintenanceStorage::new(storage.clone(), SPACE, None)
        .await
        .unwrap()
        .commit_identity("wiki_definition_plan", &scope)
        .unwrap()
}

async fn stale_inventory(embedded: bool) {
    let storage = setup(embedded).await;
    let originals = original_entities(&storage).await;
    let commit_id = original_commit_identity(&storage, &originals).await;
    let llm = Arc::new(BoundaryPlanner::new(
        storage.clone(),
        &originals,
        Some(OrganizationMutation::ChangeContributor(
            originals[1].clone(),
        )),
    ));
    let result = Box::pin(planner(&storage, llm.clone()).create_from_entities(
        SPACE,
        originals.clone(),
        config(),
    ))
    .await;
    assert!(
        result.is_err(),
        "stale whole-inventory publication must refuse"
    );
    assert_eq!(llm.organization_calls.load(Ordering::SeqCst), 2);
    assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 0);
    assert!(KnowledgeViewDefinitionRepository::new(storage.clone())
        .list(SPACE, 10)
        .await
        .unwrap()
        .is_empty());
    assert!(KnowledgeViewRepository::new(storage.clone())
        .list_views(SPACE, 10)
        .await
        .unwrap()
        .is_empty());
    assert!(CanonicalRepository::new(storage.clone())
        .receipt(SPACE, &commit_id)
        .await
        .unwrap()
        .is_none());
    assert!(elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap()
        .is_empty());
    assert!(
        elephant_storage::SystemEventRepository::new(storage.clone())
            .list(SPACE, 100, None)
            .await
            .unwrap()
            .is_empty()
    );
    let entities = EntityRepository::new(storage);
    assert_eq!(
        entities.get(SPACE, &originals[0].base.id).await.unwrap(),
        Some(originals[0].clone())
    );
    assert_eq!(
        entities
            .get(SPACE, &originals[1].base.id)
            .await
            .unwrap()
            .unwrap()
            .summary
            .as_deref(),
        Some("Changed after original organization input")
    );
}

async fn deleted_definition(embedded: bool) {
    let storage = setup(embedded).await;
    let originals = original_entities(&storage).await;
    let llm = Arc::new(BoundaryPlanner::new(storage.clone(), &originals, None));
    let initial =
        Box::pin(planner(&storage, llm).create_from_entities(SPACE, originals.clone(), config()))
            .await
            .unwrap();
    assert_eq!(initial.plan.articles.len(), 2);
    assert!(
        initial.articles.is_empty(),
        "fixture never supplies article output"
    );
    let definitions = KnowledgeViewDefinitionRepository::new(storage.clone());
    let mut before = definitions.list(SPACE, 10).await.unwrap();
    before.sort_by_key(|row| row.base.id.to_string());
    assert_eq!(
        before.len(),
        2,
        "actual planner must publish its complete positive bundle"
    );
    let canonical = CanonicalRepository::new(storage.clone());
    let original_receipt = canonical
        .receipt(SPACE, &initial.definition_commit_id)
        .await
        .unwrap()
        .expect("actual original definition receipt");
    for row in &before {
        assert!(row.base.provenance.sources.is_empty());
        let inputs = canonical
            .definition_decision_inputs(row)
            .await
            .unwrap()
            .expect("source-less intrinsic inputs remain legitimate authority");
        for entity in &originals {
            assert!(inputs.iter().any(|input| input.base().id == entity.base.id));
        }
    }
    let audit_before = elephant_storage::AuditLogRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let events_before = elephant_storage::SystemEventRepository::new(storage.clone())
        .list(SPACE, 100, None)
        .await
        .unwrap();
    let commit_id = original_commit_identity(&storage, &originals).await;
    assert_ne!(commit_id, initial.definition_commit_id);
    let deleted = before
        .iter()
        .find(|row| row.title == originals[1].display_name)
        .expect("original second planned definition")
        .clone();
    let llm = Arc::new(BoundaryPlanner::new(
        storage.clone(),
        &originals,
        Some(OrganizationMutation::DeleteDefinition(deleted.clone())),
    ));
    let result =
        Box::pin(planner(&storage, llm.clone()).create_from_entities(SPACE, originals, config()))
            .await;
    assert!(
        result.is_err(),
        "concurrent definition deletion must refuse the whole replacement"
    );
    assert_eq!(llm.organization_calls.load(Ordering::SeqCst), 2);
    assert_eq!(llm.writer_calls.load(Ordering::SeqCst), 0);
    for original in before {
        let current = definitions
            .get_including_deleted(SPACE, &original.base.id)
            .await
            .unwrap()
            .unwrap();
        if original.base.id == deleted.base.id {
            assert!(
                current.base.deleted_at.is_some(),
                "deleted definition revived"
            );
            let mut without_injected_timestamps = current;
            without_injected_timestamps.base.deleted_at = original.base.deleted_at;
            without_injected_timestamps.base.updated_at = original.base.updated_at;
            assert_eq!(without_injected_timestamps, original);
        } else {
            assert_eq!(current, original, "sibling definition partially replaced");
        }
    }
    assert!(canonical
        .receipt(SPACE, &commit_id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        serde_json::to_value(
            canonical
                .receipt(SPACE, &initial.definition_commit_id)
                .await
                .unwrap()
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(original_receipt).unwrap()
    );
    assert_eq!(
        serde_json::to_value(
            elephant_storage::AuditLogRepository::new(storage.clone())
                .list(SPACE, 100, None)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(audit_before).unwrap()
    );
    assert_eq!(
        serde_json::to_value(
            elephant_storage::SystemEventRepository::new(storage)
                .list(SPACE, 100, None)
                .await
                .unwrap()
        )
        .unwrap(),
        serde_json::to_value(events_before).unwrap()
    );
}

#[tokio::test]
async fn embedded_actual_planner_rejects_changed_disconnected_inventory() {
    Box::pin(stale_inventory(true)).await;
}

#[tokio::test]
async fn surreal_actual_planner_rejects_changed_disconnected_inventory() {
    Box::pin(stale_inventory(false)).await;
}

#[tokio::test]
async fn embedded_actual_planner_never_revives_deleted_definition_or_replaces_sibling() {
    Box::pin(deleted_definition(true)).await;
}

#[tokio::test]
async fn surreal_actual_planner_never_revives_deleted_definition_or_replaces_sibling() {
    Box::pin(deleted_definition(false)).await;
}

#[test]
fn independent_wiki_boundary_futures_stay_bounded() {
    assert!(std::mem::size_of_val(&stale_inventory(true)) < 64 * 1024);
    assert!(std::mem::size_of_val(&deleted_definition(true)) < 64 * 1024);
}
