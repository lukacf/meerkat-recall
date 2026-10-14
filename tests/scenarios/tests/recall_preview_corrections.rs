//! Retained public corrections preserve author input without borrowing old proof.
//! Fixed graph interpretation; no model or installed-transport acceptance claim.

#[path = "../../recall_preview/graph_support.rs"]
mod graph_support;
#[path = "../../recall_preview/publication_support.rs"]
mod publication_support;
#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_mcp::handlers::{
    entities::UpdateEntityHandler,
    relationships::{RelateEntitiesHandler, UpdateRelationshipHandler},
};
use elephant_mcp::{ToolContext, ToolDispatcher};
use elephant_storage::read_eligibility::ReadEligibility;
use elephant_storage::{EntityRepository, RelRepository, Repository};
use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::{Entity, EntityType};
use elephant_types::records::Rel;
use elephant_types::{RecordId, TableName};
use graph_support::GraphFixture;
use serde_json::json;

const SPACE: &str = "correction-qualification";

fn principal() -> elephant_policy::Principal {
    elephant_policy::Principal {
        space_id: SPACE.into(),
        scopes: ["*".into()].into(),
        clearances: vec![
            elephant_policy::Clearance {
                policy_id: "default".into(),
                max_level: 0,
                labels: Default::default(),
                handling_allow: Default::default(),
            },
            elephant_policy::Clearance {
                policy_id: "qualification-policy".into(),
                max_level: 7,
                labels: ["research".into()].into(),
                handling_allow: ["no-external-processing".into()].into(),
            },
        ],
        subject_allowlist: None,
        purpose: Default::default(),
    }
}

#[tokio::test]
async fn public_annotation_survives_but_changed_relationship_cannot_reuse_unchanged_assertion_proof(
) -> anyhow::Result<()> {
    for backend in support::BACKENDS {
        let client = support::storage(backend).await?;
        let graph = GraphFixture::new(client.clone(), SPACE).await?;
        graph.index_fixture_vectors().await?;
        graph.publication.finalize().await?;
        let entities = EntityRepository::new(client.clone());
        let entity = entities
            .list(SPACE, 100, None)
            .await?
            .into_iter()
            .find(|entity| entity.display_name == "Élodie")
            .ok_or_else(|| anyhow::anyhow!("actual published subject absent"))?;
        let mut metadata = entity.metadata.clone();
        metadata["qualification_operator_note"] = json!("Reviewed this existing record.");
        let mut dispatcher = ToolDispatcher::new(ToolContext::new(client.clone(), principal()));
        dispatcher.register(UpdateEntityHandler::new(client.clone()));
        dispatcher.register(UpdateRelationshipHandler::new(client.clone()));
        dispatcher.register(RelateEntitiesHandler::new(client.clone()));
        let response = dispatcher
            .execute(
                "update_entity",
                json!({"id":entity.base.id,"fields":{"metadata":metadata}}),
            )
            .await?;
        anyhow::ensure!(
            response["ok"] == true,
            "retained independent annotation refused: {response}"
        );
        let annotated = entities
            .get(SPACE, &entity.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("annotated entity lost"))?;
        anyhow::ensure!(
            annotated.metadata == metadata && annotated.display_name == entity.display_name
        );
        anyhow::ensure!(
            annotated.base.security == entity.base.security
                && annotated.base.subjects == entity.base.subjects
        );
        let authorize = |base: &BaseFields| {
            base.space_id == SPACE
                && base.security == graph.lineage.source.base.security
                && !base.is_deleted()
        };
        // The actual MCP operation above performs ABAC. This read checks current
        // lineage/fingerprint after the ordinary annotation, without model work.
        anyhow::ensure!(ReadEligibility::new(client.clone())
            .project(annotated, &authorize)
            .await?
            .is_some());

        let relations = RelRepository::new(client.clone());
        let rows = relations.list(SPACE, 100, None).await?;
        anyhow::ensure!(rows.len() == 1 && !rows[0].evidence.is_empty());
        let before = &rows[0];
        anyhow::ensure!(before.rel_type != "destroyed");
        // Positive operation with the unchanged tuple proves permissions and
        // the current evidence path work before the unsupported edit is tried.
        let unchanged = dispatcher
            .execute(
                "update_relationship",
                json!({"id":before.base.id,
            "fields":{"rel_type":before.rel_type,"status":before.status}}),
            )
            .await?;
        anyhow::ensure!(
            unchanged["ok"] == true,
            "valid existing tuple refused: {unchanged}"
        );
        let before = relations
            .get(SPACE, &before.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("relationship absent"))?;
        let result = dispatcher
            .execute(
                "update_relationship",
                json!({"id":before.base.id,
            "fields":{"rel_type":"destroyed","status":"approved"}}),
            )
            .await;
        let reason = match result {
            Err(error) => error.to_string(),
            Ok(response) => {
                anyhow::ensure!(
                    response["ok"] == false,
                    "changed factual relationship borrowed original opened evidence: {response}"
                );
                response.to_string()
            }
        }
        .to_lowercase();
        anyhow::ensure!(
            ["evidence", "assertion", "support", "tuple", "predicate"]
                .iter()
                .any(|word| reason.contains(word)),
            "unsupported edit failed for unrelated reason: {reason}"
        );
        anyhow::ensure!(
            relations.get(SPACE, &before.base.id).await?.as_ref() == Some(&before),
            "refused factual edit left a changed row or partial proof"
        );
        // Explicit independently authored relationships remain usable. These
        // endpoints are actual source-less authoritative records, not published
        // source stand-ins or model-derived entities with stripped provenance.
        let mut endpoints = Vec::new();
        for name in ["Independent author A", "Independent author B"] {
            endpoints.push(
                entities
                    .create(Entity::new(
                        BaseFields::new(
                            RecordId::new_v4(TableName::Entity),
                            SPACE.into(),
                            "qualification".into(),
                        ),
                        EntityType::new("person"),
                        name.into(),
                    ))
                    .await?,
            );
        }
        let response = dispatcher
            .execute(
                "relate_entities",
                json!({
                    "space_id":SPACE,"from_id":endpoints[0].base.id,"to_id":endpoints[1].base.id,
                    "rel_type":"knows","description":"Explicit independent author input"
                }),
            )
            .await?;
        anyhow::ensure!(
            response["ok"] == true,
            "source-less author input was cut: {response}"
        );
        let authored: Rel = serde_json::from_value(response["data"].clone())?;
        anyhow::ensure!(
            authored.evidence.is_empty() && authored.base.provenance.sources.is_empty()
        );
        let response = dispatcher
            .execute(
                "update_relationship",
                json!({
                    "id":authored.base.id,"fields":{"rel_type":"collaborates_with"}
                }),
            )
            .await?;
        anyhow::ensure!(
            response["ok"] == true,
            "independent author correction incorrectly required source proof: {response}"
        );
        let authored = relations
            .get(SPACE, &authored.base.id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("authored relation lost"))?;
        anyhow::ensure!(
            authored.rel_type == "collaborates_with"
                && authored.evidence.is_empty()
                && authored.base.provenance.sources.is_empty()
        );
    }
    Ok(())
}
