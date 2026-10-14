//! Actual native guard writes, retaining the original captured embedding proof.
//! Deterministic concurrent writers are covered by the independent race oracle.

use super::*;

async fn reference_revision(storage: &StorageClient, space: &str) -> u64 {
    #[derive(Deserialize)]
    struct Revision {
        revision: u64,
    }
    let mut response = storage
        .query_bind_checked(
            "SELECT revision FROM source_erasure_guard WHERE space_id = $space;",
            crate::bindings! { "space" => space.to_owned() },
        )
        .await
        .unwrap();
    let rows: Vec<Revision> = response.take(0).unwrap();
    assert_eq!(rows.len(), 1, "original journal must own one space guard");
    rows[0].revision
}

async fn original_inputs(fixture: &Fixture, claim: &WorkItem) -> Vec<serde_json::Value> {
    let manifest = &fixture.manifest;
    let mut ids = vec![
        manifest.publication.source_id.clone(),
        claim.base.id.clone(),
    ];
    for mapping in &manifest.documents {
        ids.extend([mapping.doc_id.clone(), mapping.doc_revision_id.clone()]);
    }
    ids.extend(fixture.nodes.iter().map(|node| node.base.id.clone()));
    ids.extend(manifest.metadata_artifact_ids.iter().cloned());
    let mut rows = Vec::new();
    for id in ids {
        let row = crate::canonical_commit::CanonicalRecord::load(
            &fixture.storage,
            &manifest.space_id,
            &id,
        )
        .await
        .unwrap()
        .unwrap();
        rows.push(serde_json::to_value(row).unwrap());
    }
    rows
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
async fn native_embedding_guard_changes_private_revision_without_domain_edits() {
    let (fixture, claim, authority) =
        embedding_write_fixture(false, "native-original-input-fence").await;
    let space = &fixture.manifest.space_id;
    let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
    let publication = SourcePublicationRepository::new(fixture.storage.clone());
    let original = original_inputs(&fixture, &claim).await;
    let head = publication
        .head(space, &fixture.manifest.publication.source_id)
        .await
        .unwrap();
    let manifest = repository
        .get(space, &fixture.manifest.publication)
        .await
        .unwrap();
    let model = embedding_model(&fixture.manifest).unwrap();
    for _ in 0..2 {
        let before = reference_revision(&fixture.storage, space).await;
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = BTreeMap::new();
        // Reuse the very same authority; no recapture, claim renewal, synthetic
        // completion or changed source generation can make the second pass work.
        append_embedding_write_sql(&mut sql, &mut bindings, &authority, &[], &model, &[]).unwrap();
        sql.push_str("COMMIT TRANSACTION;");
        fixture
            .storage
            .query_bind_checked(&sql, bindings)
            .await
            .unwrap();
        assert!(reference_revision(&fixture.storage, space).await > before);
        assert_eq!(original_inputs(&fixture, &claim).await, original);
        assert_eq!(
            publication
                .head(space, &fixture.manifest.publication.source_id)
                .await
                .unwrap(),
            head
        );
        assert_eq!(
            repository
                .get(space, &fixture.manifest.publication)
                .await
                .unwrap(),
            manifest
        );
        assert!(repository
            .completion(space, &fixture.manifest.publication)
            .await
            .unwrap()
            .is_none());
    }
}
