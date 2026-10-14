//! Both exposed backends: a caller document cannot replace current-doc authority.
#![allow(clippy::expect_used, clippy::unwrap_used)]
use super::*;
use crate::source_document_guards::PreparedDocGuards;
use crate::{DocRepository, Repository};
use elephant_types::records::{BaseFields, Doc};
use elephant_types::Link;

async fn guard_client(embedded: bool) -> crate::StorageClient {
    let client = tests::client(embedded).await;
    if !embedded {
        client
            .query_checked(include_str!(
                "../../migrations/sql/039_private_reference_fence.surql"
            ))
            .await
            .unwrap();
        client
            .query_checked(include_str!(
                "../../migrations/sql/043_native_read_fence.surql"
            ))
            .await
            .unwrap();
    }
    client
}

async fn check_original_guards(
    storage: &crate::StorageClient,
    guards: &PreparedDocGuards,
) -> StorageResult<()> {
    if let Some(store) = storage.embedded_store() {
        return store.mutate(|tx| guards.check_tx(tx)).await;
    }
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    let mut bindings = crate::bindings! {"space"=>"business-a"};
    guards.append_guard_sql(&mut sql, &mut bindings)?;
    sql.push_str("COMMIT TRANSACTION;");
    storage.query_bind_checked(&sql, bindings).await?;
    Ok(())
}

fn document(source: &RecordId, label: &str) -> Doc {
    Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            "business-a".into(),
            "test".into(),
        ),
        title: label.into(),
        text: format!("original {label}"),
        sidecar_text: None,
        source: Some(Link::new(source.clone())),
        media_attachments: vec![],
        rendition_key: label.into(),
        metadata: serde_json::json!({}),
    }
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
#[expect(
    clippy::too_many_lines,
    reason = "Keep the complete fault and recovery sequence together with its original assertions."
)]
async fn caller_and_current_documents(embedded: bool) {
    let storage = guard_client(embedded).await;
    if !embedded {
        storage
            .query_checked(include_str!(
                "../../migrations/sql/039_private_reference_fence.surql"
            ))
            .await
            .unwrap();
    }
    let repo = SourcePublicationRepository::new(storage.clone());
    let id = RecordId::new_v4(TableName::Source);
    let first = repo
        .accept(tests::request(&id, "doc-r1", SourcePrecondition::Absent))
        .await
        .unwrap();
    let docs = DocRepository::new(storage.clone());
    let caller = docs.create(document(&id, "caller")).await.unwrap();
    let current = docs
        .create(document(&id, "current-physical"))
        .await
        .unwrap();
    let head = repo.head("business-a", &id).await.unwrap().unwrap();
    let guards = PreparedDocGuards::from_authorized_documents(
        "business-a",
        &id,
        Some(&head),
        &[caller.clone(), current.clone(), caller.clone()],
    )
    .unwrap();
    let mut request = tests::request(
        &id,
        "doc-r2",
        SourcePrecondition::Generation(first.source_generation.clone()),
    );
    request.authorized = repo
        .authorization_snapshot("business-a", &id)
        .await
        .unwrap();
    request.authorized_document = Some(caller.clone());
    let mut changed = current.clone();
    changed.text = "changed after authorization".into();
    let changed = docs
        .update("business-a", &current.base.id, changed)
        .await
        .unwrap();
    assert!(repo
        .accept_with_document_guards(request.clone(), &guards, None)
        .await
        .is_err());
    assert_eq!(
        repo.head("business-a", &id).await.unwrap(),
        Some(head.clone())
    );
    assert!(repo
        .operation_by_idempotency_key("business-a", &request.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(repo
        .revision("business-a", &id, "doc-r2")
        .await
        .unwrap()
        .is_none());
    let fresh = PreparedDocGuards::from_authorized_documents(
        "business-a",
        &id,
        Some(&head),
        &[caller.clone(), changed],
    )
    .unwrap();
    let accepted = repo
        .accept_with_document_guards(request.clone(), &fresh, None)
        .await
        .unwrap();
    // Matching receipt lookup does not mutate and must not require old documents
    // or an old head to become current again.
    let mut changed_caller = caller.clone();
    changed_caller.text = "later caller edit".into();
    docs.update("business-a", &caller.base.id, changed_caller)
        .await
        .unwrap();
    assert_eq!(
        repo.accept_with_document_guards(request.clone(), &fresh, None)
            .await
            .unwrap(),
        accepted
    );
    let mut third = tests::request(
        &id,
        "doc-r3",
        SourcePrecondition::Generation(accepted.source_generation),
    );
    third.authorized = repo
        .authorization_snapshot("business-a", &id)
        .await
        .unwrap();
    assert!(
        repo.accept_with_document_guards(third, &fresh, None)
            .await
            .is_err(),
        "a fresh current CAS cannot refresh the old captured head/documents"
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_source_acceptance_guards_both_documents() {
    caller_and_current_documents(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_source_acceptance_guards_both_documents() {
    caller_and_current_documents(false).await;
}

#[test]
fn document_guards_reject_foreign_and_inconsistent_snapshots() {
    let source = RecordId::new_v4(TableName::Source);
    let doc = document(&source, "caller");
    let mut changed = doc.clone();
    changed.text = "different same ID".into();
    assert!(PreparedDocGuards::from_authorized_documents(
        "business-a",
        &source,
        None,
        &[doc.clone(), changed]
    )
    .is_err());
    assert!(PreparedDocGuards::from_authorized_documents(
        "foreign",
        &source,
        None,
        std::slice::from_ref(&doc)
    )
    .is_err());
    assert!(PreparedDocGuards::from_authorized_documents(
        "business-a",
        &RecordId::new_v4(TableName::Source),
        None,
        &[doc]
    )
    .is_err());
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn absent_and_sourceless_documents(embedded: bool) {
    let storage = guard_client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let id = RecordId::new_v4(TableName::Source);
    let first = repo
        .accept(tests::request(&id, "absent-r1", SourcePrecondition::Absent))
        .await
        .unwrap();
    let head = repo.head("business-a", &id).await.unwrap().unwrap();
    let docs = DocRepository::new(storage.clone());
    let mut caller = document(&id, "sourceless-caller");
    caller.source = None;
    let caller = docs.create(caller).await.unwrap();
    let pending = document(&id, "not-yet-staged");
    let guards = PreparedDocGuards::from_authorized_documents(
        "business-a",
        &id,
        Some(&head),
        std::slice::from_ref(&caller),
    )
    .unwrap()
    .with_absent_documents(std::slice::from_ref(&pending.base.id))
    .unwrap();
    assert!(PreparedDocGuards::from_authorized_documents(
        "business-a",
        &id,
        Some(&head),
        std::slice::from_ref(&caller)
    )
    .unwrap()
    .with_absent_documents(std::slice::from_ref(&caller.base.id))
    .is_err());
    let mut request = tests::request(
        &id,
        "absent-r2",
        SourcePrecondition::Generation(first.source_generation),
    );
    request.authorized = repo
        .authorization_snapshot("business-a", &id)
        .await
        .unwrap();
    let pending = docs.create(pending).await.unwrap();
    assert!(repo
        .accept_with_document_guards(request.clone(), &guards, None)
        .await
        .is_err());
    assert_eq!(
        repo.head("business-a", &id).await.unwrap(),
        Some(head.clone())
    );
    assert!(repo
        .operation_by_idempotency_key("business-a", &request.idempotency_key)
        .await
        .unwrap()
        .is_none());
    let guards = PreparedDocGuards::from_authorized_documents(
        "business-a",
        &id,
        Some(&head),
        &[caller, pending],
    )
    .unwrap();
    repo.accept_with_document_guards(request, &guards, None)
        .await
        .unwrap();
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_source_acceptance_guards_absent_and_sourceless_documents() {
    absent_and_sourceless_documents(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_source_acceptance_guards_absent_and_sourceless_documents() {
    absent_and_sourceless_documents(false).await;
}

async fn persist_manifest_observation_fixture(
    storage: &crate::StorageClient,
    manifest: &crate::source_publication_manifest::SourcePublicationManifest,
) {
    let table = crate::source_publication_manifest::MANIFEST_TABLE;
    let key =
        crate::source_publication_manifest::manifest_key(&manifest.space_id, &manifest.publication);
    if let Some(store) = storage.embedded_store() {
        store
            .mutate(|tx| {
                write_control_tx(
                    tx,
                    table,
                    &key,
                    &manifest.space_id,
                    &manifest.publication.source_id,
                    manifest,
                )
            })
            .await
            .unwrap();
    } else {
        storage.query_bind_checked("UPSERT $id CONTENT $record;", crate::bindings! {
            "id"=>control_thing(table,&key),
            "record"=>control_value(&manifest.space_id,&manifest.publication.source_id,manifest).unwrap(),
        }).await.unwrap();
    }
}
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn actual_manifest_observation_guard(embedded: bool) {
    use crate::source_publication_manifest::SourcePublicationManifest;
    let storage = guard_client(embedded).await;
    let repo = SourcePublicationRepository::new(storage.clone());
    let source = RecordId::new_v4(TableName::Source);
    let accepted = repo
        .accept(tests::request(
            &source,
            "manifest-r1",
            SourcePrecondition::Absent,
        ))
        .await
        .unwrap();
    let head = repo.head("business-a", &source).await.unwrap().unwrap();
    let publication = elephant_types::publication::PublicationFence {
        source_id: source.clone(),
        generation: head.generation.clone(),
        revision_id: "manifest-r1".into(),
        operation_id: head.operation_id.clone(),
    };
    let absent =
        PreparedDocGuards::from_authorized_documents("business-a", &source, Some(&head), &[])
            .unwrap()
            .with_manifest_observation(&publication, None)
            .unwrap();
    // This is an actual storage-guard race fixture, not a claim that a worker
    // completed or registered a valid publication plan. The real source head
    // remains unchanged while this private observed payload appears/changes.
    let mut manifest = SourcePublicationManifest {
        space_id: "business-a".into(),
        publication: publication.clone(),
        mode: elephant_types::records::ingest::IngestMode::Metadata,
        configuration: elephant_types::provider_binding::SpaceConfigToken {
            space_id: "business-a".into(),
            config_id: "observed-config".into(),
            generation: 1,
            policy_id: "fixture-policy".into(),
        },
        embedding_profile: None,
        documents: vec![],
        metadata_artifact_ids: vec![],
        planning_artifact_ids: vec![],
        bundles: vec![],
        embeddings: vec![],
        embedding_work_item_ids: vec![],
    };
    persist_manifest_observation_fixture(&storage, &manifest).await;
    let mut request = tests::request(
        &source,
        "manifest-r2",
        SourcePrecondition::Generation(accepted.source_generation),
    );
    request.authorized = repo
        .authorization_snapshot("business-a", &source)
        .await
        .unwrap();
    assert!(repo
        .accept_with_document_guards(request.clone(), &absent, None)
        .await
        .is_err());
    let present =
        PreparedDocGuards::from_authorized_documents("business-a", &source, Some(&head), &[])
            .unwrap()
            .with_manifest_observation(&publication, Some(&manifest))
            .unwrap();
    manifest
        .metadata_artifact_ids
        .push(RecordId::new_v4(TableName::WorkArtifact));
    persist_manifest_observation_fixture(&storage, &manifest).await;
    assert!(repo
        .accept_with_document_guards(request.clone(), &present, None)
        .await
        .is_err());
    assert_eq!(repo.head("business-a", &source).await.unwrap(), Some(head));
    assert!(repo
        .operation_by_idempotency_key("business-a", &request.idempotency_key)
        .await
        .unwrap()
        .is_none());
    assert!(
        present
            .with_manifest_observation(&publication, Some(&manifest))
            .is_err(),
        "captured observations cannot be refreshed inside the same authorization guard"
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_document_guards_fence_original_manifest_presence_and_payload() {
    actual_manifest_observation_guard(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_document_guards_fence_original_manifest_presence_and_payload() {
    actual_manifest_observation_guard(false).await;
}

#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn absent_document_identity_is_global(embedded: bool) {
    let storage = guard_client(embedded).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let source = RecordId::new_v4(TableName::Source);
    publications
        .accept(tests::request(
            &source,
            "physical-absence-r1",
            SourcePrecondition::Absent,
        ))
        .await
        .unwrap();
    let head = publications
        .head("business-a", &source)
        .await
        .unwrap()
        .unwrap();
    let configs = crate::SpaceConfigRepository::new(storage.clone());
    let mut other = configs
        .get_by_space_id("business-a")
        .await
        .unwrap()
        .unwrap();
    other.base.id = RecordId::new_v4(TableName::SpaceConfig);
    other.base.space_id = "another-space".into();
    configs.create(other).await.unwrap();
    let mut foreign = document(&source, "foreign-physical-id");
    foreign.base.space_id = "another-space".into();
    foreign.source = None;
    let guards =
        PreparedDocGuards::from_authorized_documents("business-a", &source, Some(&head), &[])
            .unwrap()
            .with_absent_documents(&[foreign.base.id.clone()])
            .unwrap();
    check_original_guards(&storage, &guards).await.unwrap();
    // Direct trusted repository fixture tests global physical identity rather
    // than request routing: the original key was absent in every named space.
    let foreign = DocRepository::new(storage.clone())
        .create(foreign)
        .await
        .unwrap();
    assert!(check_original_guards(&storage, &guards).await.is_err());
    assert_eq!(
        publications.head("business-a", &source).await.unwrap(),
        Some(head)
    );
    assert_eq!(
        DocRepository::new(storage.clone())
            .get("another-space", &foreign.base.id)
            .await
            .unwrap(),
        Some(foreign)
    );
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn embedded_document_absence_refuses_foreign_physical_identity() {
    absent_document_identity_is_global(true).await;
}
#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
async fn surreal_document_absence_refuses_foreign_physical_identity() {
    absent_document_identity_is_global(false).await;
}

#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn surreal_document_read_guard_changes_only_bounded_fence_metadata() {
    let storage = guard_client(false).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let source = RecordId::new_v4(TableName::Source);
    publications
        .accept(tests::request(
            &source,
            "physical-fence-r1",
            SourcePrecondition::Absent,
        ))
        .await
        .unwrap();
    let head = publications
        .head("business-a", &source)
        .await
        .unwrap()
        .unwrap();
    let doc = DocRepository::new(storage.clone())
        .create(document(&source, "unchanged"))
        .await
        .unwrap();
    let guards = PreparedDocGuards::from_authorized_documents(
        "business-a",
        &source,
        Some(&head),
        std::slice::from_ref(&doc),
    )
    .unwrap();
    async fn fences(storage: &crate::StorageClient) -> Vec<String> {
        let mut response = storage
            .query_checked("SELECT VALUE nonce FROM native_read_fence ORDER BY nonce;")
            .await
            .unwrap();
        response.take(0).unwrap()
    }
    check_original_guards(&storage, &guards).await.unwrap();
    let first = fences(&storage).await;
    check_original_guards(&storage, &guards).await.unwrap();
    let second = fences(&storage).await;
    assert_eq!(first.len(), second.len());
    assert!(first.len() >= 2 && first.len() <= 12288);
    assert_ne!(
        first, second,
        "real native writes must not be optimized away"
    );
    assert_eq!(
        publications.head("business-a", &source).await.unwrap(),
        Some(head)
    );
    assert_eq!(
        DocRepository::new(storage.clone())
            .get("business-a", &doc.base.id)
            .await
            .unwrap(),
        Some(doc)
    );
}

#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn surreal_multiple_original_source_document_guards_keep_both_readsets() {
    let storage = guard_client(false).await;
    let publications = SourcePublicationRepository::new(storage.clone());
    let docs = DocRepository::new(storage.clone());
    let mut originals = Vec::new();
    let mut guards = Vec::new();
    for label in ["left-source-readset", "right-source-readset"] {
        let source = RecordId::new_v4(TableName::Source);
        let mut request = tests::request(&source, label, SourcePrecondition::Absent);
        // This test needs two independent sources, not competing creations for
        // the helper's shared upstream identity.
        request.source.external_id = Some(label.into());
        let accepted = publications.accept(request).await.unwrap();
        assert_eq!(accepted.source_id, source);
        let head = publications
            .head("business-a", &source)
            .await
            .unwrap()
            .unwrap();
        let doc = docs.create(document(&source, label)).await.unwrap();
        guards.push(
            PreparedDocGuards::from_authorized_documents(
                "business-a",
                &source,
                Some(&head),
                std::slice::from_ref(&doc),
            )
            .unwrap(),
        );
        originals.push(doc);
    }
    async fn check(
        storage: &crate::StorageClient,
        guards: &[PreparedDocGuards],
    ) -> StorageResult<()> {
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = crate::bindings! {"space"=>"business-a"};
        for (index, guard) in guards.iter().enumerate() {
            guard.append_guard_sql_scoped(
                &mut sql,
                &mut bindings,
                &format!("original_source_{index}"),
            )?;
        }
        sql.push_str("COMMIT TRANSACTION;");
        storage.query_bind_checked(&sql, bindings).await?;
        Ok(())
    }
    check(&storage, &guards).await.unwrap();
    let mut changed = originals[0].clone();
    changed.text = "first original read changed while the second stayed equal".into();
    let changed = docs
        .update("business-a", &changed.base.id.clone(), changed)
        .await
        .unwrap();
    assert!(
        check(&storage, &guards).await.is_err(),
        "later bindings cannot replace the first original source readset"
    );
    assert_eq!(
        docs.get("business-a", &changed.base.id).await.unwrap(),
        Some(changed)
    );
    assert_eq!(
        docs.get("business-a", &originals[1].base.id).await.unwrap(),
        Some(originals[1].clone())
    );
}

#[path = "../../../tests/recall_preview/native_guard_race_oracles.rs"]
mod independent_native_overlap;
