//! Uses real acceptance/manifest readiness. These tests never mint a completed
//! publication or claim the storage fixture ran an actual renderer.
use super::*;

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn finalizer_captures_every_rendered_field_before_transaction_checks() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "render-proof-guard",
            IngestMode::Graph,
            0,
        )
        .await;
        let space = &fixture.manifest.space_id;
        let manifests = SourcePublicationManifestRepository::new(fixture.storage.clone());
        manifests
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let revisions = crate::DocRevisionRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.documents[0].doc_revision_id;
        for field in ["text", "renderer", "manifest", "recipe", "security"] {
            let original = revisions.get(space, id).await.unwrap().unwrap();
            let captured = manifests
                .requirements(space, &fixture.manifest.publication)
                .await
                .unwrap();
            manifests.check_ready(&captured).await.unwrap();
            let mut changed = original.clone();
            match field {
                "text" => {
                    changed.text = " \n".into();
                    changed.text_hash = format!("{:x}", Sha256::digest(changed.text.as_bytes()));
                }
                "renderer" => changed.renderer.version.push_str("-changed"),
                "manifest" => {
                    changed.manifest = Some(elephant_types::records::RenderManifest::default());
                }
                "recipe" => changed.base.provenance.chain.push(
                    elephant_types::records::base::ProvenanceStep {
                        step: "immutable_render_recipe_v1".into(),
                        input: changed.doc.id().to_string(),
                        output: "different-recipe".into(),
                    },
                ),
                "security" => changed.base.security.level += 1,
                _ => unreachable!(),
            }
            revisions.update(space, id, changed).await.unwrap();
            let error = manifests.check_ready(&captured).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("source_document_lineage_changed"),
                "{field}: {error}"
            );
            assert!(manifests
                .completion(space, &fixture.manifest.publication)
                .await
                .unwrap()
                .is_none());
            revisions.update(space, id, original).await.unwrap();
        }
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn finalizer_captures_raw_document_fields_separately_from_rendered_text() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "raw-doc-proof-guard",
            IngestMode::Graph,
            0,
        )
        .await;
        let space = &fixture.manifest.space_id;
        let manifests = SourcePublicationManifestRepository::new(fixture.storage.clone());
        manifests
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let documents = crate::DocRepository::new(fixture.storage.clone());
        let id = &fixture.manifest.documents[0].doc_id;
        for field in ["text", "title", "metadata", "security"] {
            let original = documents.get(space, id).await.unwrap().unwrap();
            let captured = manifests
                .requirements(space, &fixture.manifest.publication)
                .await
                .unwrap();
            manifests.check_ready(&captured).await.unwrap();
            let mut changed = original.clone();
            match field {
                "text" => changed.text = "changed raw input".into(),
                "title" => changed.title.push_str(" changed"),
                "metadata" => changed.metadata["unpublished"] = serde_json::json!("changed"),
                "security" => changed.base.security.level += 1,
                _ => unreachable!(),
            }
            documents.update(space, id, changed).await.unwrap();
            let error = manifests.check_ready(&captured).await.unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("source_document_lineage_changed"),
                "{field}: {error}"
            );
            assert!(manifests
                .completion(space, &fixture.manifest.publication)
                .await
                .unwrap()
                .is_none());
            documents.update(space, id, original).await.unwrap();
        }
    }
}

#[tokio::test]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn render_proof_uses_exact_original_accepted_control_payload() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "accepted-render-guard",
            IngestMode::Graph,
            0,
        )
        .await;
        let space = &fixture.manifest.space_id;
        let manifests = SourcePublicationManifestRepository::new(fixture.storage.clone());
        manifests
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let captured = manifests
            .requirements(space, &fixture.manifest.publication)
            .await
            .unwrap();
        manifests.check_ready(&captured).await.unwrap();
        let mut changed: serde_json::Value =
            serde_json::from_str(captured.accepted_source_control.as_ref().unwrap()).unwrap();
        changed["revision"]["source"]["metadata"]["unpublished_template"] =
            serde_json::json!("changed immutable input");
        let key = stable_key(&[
            space,
            &fixture.manifest.publication.source_id.to_string(),
            &fixture.manifest.publication.revision_id,
        ]);
        if let Some(store) = fixture.storage.embedded_store() {
            store
                .mutate(|tx| {
                    write_control_tx(
                        tx,
                        crate::source_publication::REVISION_TABLE,
                        &key,
                        space,
                        &fixture.manifest.publication.source_id,
                        &changed,
                    )
                })
                .await
                .unwrap();
        } else {
            fixture.storage.query_bind_checked("UPDATE $id CONTENT $value", crate::bindings! {
                "id" => control_thing(crate::source_publication::REVISION_TABLE, &key),
                "value" => control_value(space, &fixture.manifest.publication.source_id, &changed).unwrap(),
            }).await.unwrap();
        }
        let error = manifests.check_ready(&captured).await.unwrap_err();
        assert!(
            error.to_string().contains("source_render_input_changed"),
            "{error}"
        );
        assert!(manifests
            .completion(space, &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
#[expect(
    clippy::items_after_statements,
    reason = "Keep local fixture and row helpers with the conformance phase whose inputs they describe."
)]
#[expect(
    clippy::large_futures,
    reason = "Keep the conformance helper's original await and allocation shape."
)]
#[expect(
    clippy::significant_drop_tightening,
    reason = "Keep fixture, repository, and mutation owners alive through the original conformance phase."
)]
async fn native_render_guard_changes_private_fences_and_preserves_domain_rows() {
    let fixture = fixture(
        client(false).await,
        "actual-render-conflict-fence",
        IngestMode::Graph,
        0,
    )
    .await;
    let manifests = SourcePublicationManifestRepository::new(fixture.storage.clone());
    manifests
        .set_operation_plan(fixture.manifest.clone())
        .await
        .unwrap();
    let requirements = manifests
        .requirements(&fixture.manifest.space_id, &fixture.manifest.publication)
        .await
        .unwrap();
    let documents = requirements.documents.clone();
    let revisions = requirements.revisions.clone();
    let source_payload = requirements.accepted_source_control.clone().unwrap();
    let mut bindings = crate::bindings! { "sf_space" => fixture.manifest.space_id.clone() };
    let mut sql = String::from("BEGIN TRANSACTION;\n");
    super::super::render::append_render_inputs_sql(&mut sql, &mut bindings, &requirements).unwrap();
    sql.push_str("COMMIT TRANSACTION;");
    fixture
        .storage
        .query_bind_checked(&sql, bindings)
        .await
        .unwrap();
    assert_eq!(
        manifests
            .load_render_documents(&fixture.manifest)
            .await
            .unwrap(),
        documents
    );
    for original in revisions {
        assert_eq!(
            crate::DocRevisionRepository::new(fixture.storage.clone())
                .get(&fixture.manifest.space_id, &original.base.id)
                .await
                .unwrap(),
            Some(original)
        );
    }
    assert_eq!(
        manifests
            .load_render_source(&fixture.manifest)
            .await
            .unwrap(),
        source_payload
    );
    #[derive(serde::Deserialize)]
    struct Nonce {
        canonical_read_nonce: String,
    }
    let key = stable_key(&[
        &fixture.manifest.space_id,
        &fixture.manifest.publication.source_id.to_string(),
        &fixture.manifest.publication.revision_id,
    ]);
    let mut response = fixture.storage.query_bind_checked("SELECT canonical_read_nonce FROM $id;", crate::bindings! {"id" => control_thing(crate::source_publication::REVISION_TABLE, &key)}).await.unwrap();
    let nonce: Vec<Nonce> = response.take(0).unwrap();
    assert_eq!(nonce.len(), 1);
    assert!(uuid::Uuid::parse_str(&nonce[0].canonical_read_nonce).is_ok());
    let mut response = fixture.storage.query_bind_checked("SELECT VALUE revision FROM ONLY $id;", crate::bindings! {"id" => control_thing("source_erasure_guard", &fixture.manifest.space_id)}).await.unwrap();
    let revision: Option<u64> = response.take(0).unwrap();
    assert!(revision.is_some_and(|value| value >= 2));
}
