//! Retained manual extraction uses explicit source CAS and complete publication.
use elephant_storage::AssertionRepository;

use crate::grounded_source_fixture::reextract_employment_via_mcp;
use crate::Harness;

#[tokio::test]
#[ignore]
async fn scenario_s5_5_manual_extraction() -> anyhow::Result<()> {
    let harness = Harness::new().await?;
    let space = "manual-extraction";
    let (_doc_id, revision_id) = reextract_employment_via_mcp(&harness.client, space).await?;
    let assertions = AssertionRepository::new(harness.client.clone())
        .find_by_revision(space, &revision_id, 50)
        .await?;
    anyhow::ensure!(
        !assertions.is_empty(),
        "explicit extract_doc must publish supported assertions"
    );
    anyhow::ensure!(assertions
        .iter()
        .all(|assertion| assertion.evidence.revision.id() == &revision_id));
    Ok(())
}
