//! Exact original after-image and ABA cases for the operational successor.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use crate::{
    KnowledgeViewDefinitionRepository, KnowledgeViewRepository, StorageClient, StorageConfig,
    StorageError,
};
use elephant_types::records::{
    BaseFields, KnowledgeView, KnowledgeViewDefinition, KnowledgeViewKind,
    KnowledgeViewPublicationStatus, KnowledgeViewSelector, KnowledgeViewSurface, ProjectionState,
};
use elephant_types::{Link, RecordId, TableName};

fn base(table: TableName) -> BaseFields {
    BaseFields::new(RecordId::new_v4(table), "withdrawal".into(), "test".into())
}
async fn fixture(
    embedded: bool,
) -> (
    KnowledgeViewRepository,
    KnowledgeViewDefinition,
    KnowledgeView,
) {
    let client = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(StorageConfig::default()).await
    }
    .unwrap();
    if !embedded {
        // These storage-internal fixtures use the same real guard installer as
        // restore; it supplies 039/043 with the client's original journal key.
        crate::reference_fence::install_native_reference_guards(&client, &[])
            .await
            .expect("original native reference and physical guards");
    }
    let definition = KnowledgeViewDefinition {
        base: base(TableName::KnowledgeViewDefinition),
        title: "Original hidden root".into(),
        publication_status: KnowledgeViewPublicationStatus::Hidden,
        related_articles: vec![],
        navigation: None,
        selector: KnowledgeViewSelector::Topic {
            topic: Link::new(RecordId::new_v4(TableName::Entity)),
            relation_types: vec![],
            max_hops: 1,
            max_members: 5,
        },
    };
    let definition = KnowledgeViewDefinitionRepository::new(client.clone())
        .upsert(definition)
        .await
        .unwrap();
    let view = KnowledgeView {
        base: base(TableName::KnowledgeView),
        key: "original-view".into(),
        kind: KnowledgeViewKind::Topic,
        surface: KnowledgeViewSurface::Wiki,
        publication_status: KnowledgeViewPublicationStatus::Published,
        root_record: definition.base.id.to_string(),
        title: "Original view".into(),
        slug: "original".into(),
        security_fingerprint: "fixture".into(),
        current_revision: None,
        revision_count: 0,
        state: ProjectionState::Current,
        state_reason: None,
        published_at: Some(chrono::Utc::now()),
    };
    put_view(&client, &view).await;
    (KnowledgeViewRepository::new(client), definition, view)
}
async fn put_view(client: &StorageClient, view: &KnowledgeView) {
    if let Some(store) = client.embedded_store() {
        store
            .mutate(|tx| {
                if tx
                    .get::<KnowledgeView>(&view.base.space_id, &view.base.id, true)?
                    .is_some()
                {
                    Ok(tx.replace(view.clone())?)
                } else {
                    Ok(tx.create(view.clone())?)
                }
            })
            .await
            .unwrap();
    } else {
        client
            .query_bind_checked(
                "UPSERT $id CONTENT $row;",
                crate::bindings! {
                    "id" => crate::surreal::thing_from_record_id(&view.base.id),
                    "row" => KnowledgeViewRepository::view_to_value(view).unwrap(),
                },
            )
            .await
            .unwrap();
    }
}
#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep definitions in the original fixture scope through assertions and cleanup."
)]
async fn exact_withdrawal_is_idempotent_and_republish_or_hidden_aba_conflicts() {
    for embedded in [true, false] {
        let (repo, original, view) = fixture(embedded).await;
        assert_eq!(
            repo.hide_for_withdrawn_definition(&original).await.unwrap(),
            1
        );
        let hidden = repo
            .get_view(&view.base.space_id, &view.base.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            hidden.publication_status,
            KnowledgeViewPublicationStatus::Hidden
        );
        assert_eq!(hidden.published_at, None);
        assert_eq!(
            repo.hide_for_withdrawn_definition(&original).await.unwrap(),
            0
        );
        assert_eq!(
            repo.get_view(&view.base.space_id, &view.base.id)
                .await
                .unwrap(),
            Some(hidden)
        );
        let definitions = KnowledgeViewDefinitionRepository::new(repo.client.clone());
        let mut next = original.clone();
        next.publication_status = KnowledgeViewPublicationStatus::Published;
        next.base.updated_at += chrono::Duration::seconds(1);
        definitions.upsert(next.clone()).await.unwrap();
        put_view(&repo.client, &view).await;
        assert!(matches!(
            repo.hide_for_withdrawn_definition(&original).await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            repo.get_view(&view.base.space_id, &view.base.id)
                .await
                .unwrap(),
            Some(view.clone())
        );
        next.publication_status = KnowledgeViewPublicationStatus::Hidden;
        next.title = "Newly withdrawn generation".into();
        next.base.updated_at += chrono::Duration::seconds(1);
        let next = definitions.upsert(next).await.unwrap();
        assert!(matches!(
            repo.hide_for_withdrawn_definition(&original).await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            repo.get_view(&view.base.space_id, &view.base.id)
                .await
                .unwrap(),
            Some(view.clone())
        );
        assert_eq!(repo.hide_for_withdrawn_definition(&next).await.unwrap(), 1);
    }
}

#[tokio::test]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep works in the original fixture scope through assertions and cleanup."
)]
async fn managed_withdrawal_retains_original_grant_through_successor() {
    for embedded in [true, false] {
        let (repo, original, view) = fixture(embedded).await;
        let works = crate::WorkItemRepository::new(repo.client.clone());
        let grant = works
            .create_managed_wiki_grant("withdrawal", "manager")
            .await
            .unwrap();
        let proof = repo
            .prepare_managed_publication("withdrawal", None, Some(&grant))
            .await
            .unwrap();
        assert_eq!(
            repo.hide_for_withdrawn_definition_with_authority(&original, &proof)
                .await
                .unwrap(),
            1
        );
        put_view(&repo.client, &view).await;
        works
            .revoke_managed_wiki_grants("withdrawal")
            .await
            .unwrap();
        assert!(matches!(
            repo.hide_for_withdrawn_definition_with_authority(&original, &proof)
                .await,
            Err(StorageError::Conflict(_))
        ));
        assert_eq!(
            repo.get_view("withdrawal", &view.base.id).await.unwrap(),
            Some(view)
        );
    }
}
