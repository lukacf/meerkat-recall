//! Actual declared membership reads; these are not provider or completion tests.

use super::*;

fn ids(records: &[crate::canonical_commit::CanonicalRecord]) -> BTreeSet<RecordId> {
    records.iter().map(|row| row.base().id.clone()).collect()
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
async fn declared_inputs_include_original_candidates_without_completing_future_bundles() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "declared-inputs",
            IngestMode::Graph,
            2,
        )
        .await;
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        let manifest = repository
            .set_operation_plan(fixture.manifest.clone())
            .await
            .unwrap();
        let publications = SourcePublicationRepository::new(fixture.storage.clone());
        let original_head = publications
            .head(&manifest.space_id, &manifest.publication.source_id)
            .await
            .unwrap();
        let initial = repository.declared_input_records(&manifest).await.unwrap();
        // Doc + revision + two shared extraction/embedding nodes + original
        // metadata bundle/candidate + the durable extraction plan.
        assert_eq!(initial.len(), 7);
        assert_eq!(ids(&initial).len(), initial.len());
        assert!(manifest
            .bundles
            .iter()
            .all(|bundle| !ids(&initial).contains(&bundle.artifact_id)));
        for mapping in &manifest.documents {
            assert!(ids(&initial).contains(&mapping.doc_id));
            assert!(ids(&initial).contains(&mapping.doc_revision_id));
        }
        stage_bundle(&fixture, 0).await;
        let materialized = repository.declared_input_records(&manifest).await.unwrap();
        assert_eq!(materialized.len(), 9);
        let bundle = crate::WorkArtifactRepository::new(fixture.storage.clone())
            .get(&manifest.space_id, &manifest.bundles[0].artifact_id)
            .await
            .unwrap()
            .unwrap();
        let candidate =
            RecordId::from_str(bundle.data["source_artifact_id"].as_str().unwrap()).unwrap();
        assert!(ids(&materialized).contains(&bundle.base.id));
        assert!(ids(&materialized).contains(&candidate));
        assert!(!ids(&materialized).contains(&manifest.bundles[1].artifact_id));
        assert!(repository
            .completion(&manifest.space_id, &manifest.publication)
            .await
            .unwrap()
            .is_none());
        assert_eq!(
            publications
                .head(&manifest.space_id, &manifest.publication.source_id)
                .await
                .unwrap(),
            original_head
        );

        // A materialized bundle cannot hide its deleted original candidate by
        // being treated like an expected future output that has not arrived.
        crate::WorkArtifactRepository::new(fixture.storage.clone())
            .delete(&manifest.space_id, &candidate)
            .await
            .unwrap();
        assert!(repository.declared_input_records(&manifest).await.is_err());
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
async fn declared_inputs_refuse_missing_required_rows_and_deduplicate_coalesced_membership() {
    for embedded in [true, false] {
        let fixture = fixture(
            client(embedded).await,
            "declared-coalesced-inputs",
            IngestMode::Graph,
            2,
        )
        .await;
        let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
        // Exercise the membership reader alone. This is not a new sealed plan:
        // the durable extraction-plan comparison is performed by the sealer.
        let mut coalesced = fixture.manifest.clone();
        coalesced.bundles.truncate(1);
        coalesced.bundles[0].node_ids = fixture
            .nodes
            .iter()
            .map(|node| node.base.id.clone())
            .collect();
        coalesced.bundles[0].span_start = 0;
        coalesced.bundles[0].span_end = 7;
        coalesced.normalize();
        let selected = repository.declared_input_records(&coalesced).await.unwrap();
        assert_eq!(selected.len(), 7);
        for node in &fixture.nodes {
            assert!(ids(&selected).contains(&node.base.id));
        }
        let mut missing = coalesced.clone();
        missing.planning_artifact_ids[0] = RecordId::new_v4(TableName::WorkArtifact);
        assert!(repository.declared_input_records(&missing).await.is_err());
        crate::DocNodeRepository::new(fixture.storage.clone())
            .delete(&coalesced.space_id, &fixture.nodes[1].base.id)
            .await
            .unwrap();
        assert!(repository.declared_input_records(&coalesced).await.is_err());
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
async fn declared_input_population_respects_explicit_metadata_and_retrieval_modes() {
    for embedded in [true, false] {
        for (mode, expected) in [(IngestMode::Metadata, 2), (IngestMode::Retrieval, 6)] {
            let fixture = fixture(client(embedded).await, "declared-mode-inputs", mode, 2).await;
            let repository = SourcePublicationManifestRepository::new(fixture.storage.clone());
            let records = repository
                .declared_input_records(&fixture.manifest)
                .await
                .unwrap();
            assert_eq!(records.len(), expected);
            assert!(fixture.manifest.bundles.is_empty());
            assert!(fixture.manifest.planning_artifact_ids.is_empty());
            assert!(repository
                .completion(&fixture.manifest.space_id, &fixture.manifest.publication)
                .await
                .unwrap()
                .is_none());
        }
    }
}
