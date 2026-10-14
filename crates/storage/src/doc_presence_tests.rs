//! Original physical absence is distinct from scoped visibility.
#![allow(clippy::unwrap_used)]
use super::*;
use elephant_types::records::BaseFields;

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep storage, docs in the original fixture scope through assertions and cleanup."
)]
async fn physical_doc_presence(embedded: bool) {
    let storage = if embedded {
        StorageClient::new_embedded().await
    } else {
        StorageClient::new(crate::StorageConfig::default()).await
    }
    .unwrap();
    let docs = DocRepository::new(storage.clone());
    let id = RecordId::new_v4(TableName::Doc);
    assert!(!docs.exists_globally(&id).await.unwrap());
    assert!(docs
        .exists_globally(&RecordId::new_v4(TableName::Source))
        .await
        .is_err());
    let original = docs
        .create(Doc {
            base: BaseFields::new(id.clone(), "physical-owner".into(), "fixture".into()),
            title: "foreign physical identity".into(),
            text: "private original bytes".into(),
            sidecar_text: None,
            source: None,
            media_attachments: Vec::new(),
            rendition_key: "presence".into(),
            metadata: serde_json::json!({"retained":true}),
        })
        .await
        .unwrap();
    assert!(docs
        .get_with_deleted("different-space", &id)
        .await
        .unwrap()
        .is_none());
    assert!(docs.exists_globally(&id).await.unwrap());
    assert_eq!(
        docs.get_with_deleted("physical-owner", &id).await.unwrap(),
        Some(original)
    );
    docs.delete("physical-owner", &id).await.unwrap();
    let deleted = docs
        .get_with_deleted("physical-owner", &id)
        .await
        .unwrap()
        .unwrap();
    assert!(deleted.base.deleted_at.is_some());
    assert!(docs.exists_globally(&id).await.unwrap());
    assert_eq!(
        docs.get_with_deleted("physical-owner", &id).await.unwrap(),
        Some(deleted)
    );
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                tx.hard_delete::<Doc>("physical-owner", &id)?;
                Ok(())
            })
            .await
            .unwrap();
    } else {
        storage
            .query_bind_checked(
                "DELETE $id;",
                crate::bindings! {"id"=>crate::surreal::thing_from_record_id(&id)},
            )
            .await
            .unwrap();
    }
    assert!(!docs.exists_globally(&id).await.unwrap());
}

#[tokio::test]
async fn embedded_physical_document_presence_never_becomes_scoped_absence() {
    physical_doc_presence(true).await;
}
#[tokio::test]
async fn native_physical_document_presence_never_becomes_scoped_absence() {
    physical_doc_presence(false).await;
}
