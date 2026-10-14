//! D-owned independent expectations against active promotion on both backends.
//! Deterministic extractor-shaped input is NOT live extraction qualification.

#[path = "../../recall_preview/support.rs"]
mod support;

use elephant_pipeline::scheduler::inline_promotion::{
    prepare_assertion_groups, PreparedPromotion, PromotionGroup, PromotionOptions,
};
use elephant_pipeline::scheduler::promote_assertions_inline;
use elephant_storage::canonical_commit::CanonicalRecord;
use elephant_storage::{
    AssertionRepository, AttributeRepository, DocRepository, EntityRepository, EventRepository,
    RelRepository, Repository, SourceRepository,
};
use elephant_types::{RecordId, TableName};
use support::{BACKENDS, SPACE};

async fn prepare(
    client: &elephant_storage::StorageClient,
    lineage: &support::Lineage,
    raw: elephant_pipeline::extraction::RawAssertion,
) -> Result<PreparedPromotion, elephant_pipeline::error::PipelineError> {
    prepare_assertion_groups(
        client,
        SPACE,
        &[PromotionGroup {
            assertions: vec![raw],
            work_run_id: RecordId::new_v4(TableName::WorkRun),
            doc_node_id: lineage.node.base.id.clone(),
            revision_id: lineage.revision.base.id.clone(),
        }],
        "qualification",
        &PromotionOptions::default(),
    )
    .await
}

async fn no_canonical_effects(client: &elephant_storage::StorageClient) -> anyhow::Result<()> {
    anyhow::ensure!(
        EntityRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty(),
        "invalid evidence created entity labels/identity"
    );
    anyhow::ensure!(
        RelRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty(),
        "invalid evidence created relationship"
    );
    anyhow::ensure!(
        AttributeRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty(),
        "invalid evidence created attribute"
    );
    anyhow::ensure!(
        EventRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty(),
        "invalid evidence created event despite high confidence"
    );
    // Unsupported candidate storage is allowed; canonical assertions are not.
    anyhow::ensure!(
        AssertionRepository::new(client.clone())
            .list(SPACE, 100, None)
            .await?
            .is_empty(),
        "invalid evidence gained a canonical evidence receipt"
    );
    Ok(())
}

#[tokio::test]
async fn exact_grounded_plan_preserves_classified_lineage_without_live_publication(
) -> anyhow::Result<()> {
    for backend in BACKENDS {
        for subjects_state in [
            elephant_types::records::base::SubjectsState::Complete,
            elephant_types::records::base::SubjectsState::Partial,
            elephant_types::records::base::SubjectsState::Unknown,
        ] {
            let client = support::storage(backend).await?;
            let lineage =
                support::lineage_with_subjects_state(&client, SPACE, subjects_state).await?;
            let raw = support::grounded_assertion(&lineage).await?;
            raw.validate_support()?;
            // Even unchanged quote/hash/semantics cannot reuse an independently
            // issued receipt after moving its exact byte coordinates. Validate
            // the receipt itself, before a planner's separate bounds refusal.
            let mut moved = raw.clone();
            moved.evidence_span.start += 1;
            moved.evidence_span.end += 1;
            anyhow::ensure!(
                moved.validate_support().is_err(),
                "{backend:?}: moved coordinates reused the original support receipt"
            );
            let prepared = prepare(&client, &lineage, raw).await?;
            anyhow::ensure!(
                prepared.result.assertions_recorded == 1,
                "{backend:?}: valid evidence was not recorded"
            );
            let rows: Vec<_> = prepared
                .plan
                .effects
                .iter()
                .filter_map(|effect| effect.after.as_ref())
                .collect();
            let entities: Vec<_> = rows
                .iter()
                .filter_map(|row| match row {
                    CanonicalRecord::Entity(entity) => Some(entity),
                    _ => None,
                })
                .collect();
            anyhow::ensure!(
                entities.len() == 2,
                "{backend:?}: expected both newly extracted entities"
            );
            for entity in entities {
                anyhow::ensure!(
                    entity.base.security == lineage.doc.base.security,
                    "{backend:?}: entity classification widened"
                );
                anyhow::ensure!(
                    entity.base.subjects == lineage.doc.base.subjects,
                    "{backend:?}: entity subject scope lost"
                );
                anyhow::ensure!(
                    entity.base.subjects_state == lineage.doc.base.subjects_state,
                    "{backend:?}: entity completeness changed"
                );
            }
            for row in &rows {
                let base = row.base();
                anyhow::ensure!(
                    base.security == lineage.source.base.security,
                    "derived row widened Security"
                );
                anyhow::ensure!(
                    base.subjects_state == lineage.source.base.subjects_state,
                    "derived row widened subjects completeness"
                );
                anyhow::ensure!(
                    lineage
                        .source
                        .base
                        .subjects
                        .iter()
                        .all(|subject| base.subjects.contains(subject)),
                    "derived row lost source subject scope"
                );
                anyhow::ensure!(
                    base.provenance.sources == vec![lineage.source.base.id.clone()],
                    "derived row lost source incarnation"
                );
            }
            let assertions: Vec<_> = rows
                .iter()
                .filter_map(|row| match row {
                    CanonicalRecord::Assertion(assertion) => Some(assertion),
                    _ => None,
                })
                .collect();
            anyhow::ensure!(assertions.len() == 1);
            let evidence = &assertions[0].evidence;
            anyhow::ensure!(evidence.revision.id() == &lineage.revision.base.id);
            anyhow::ensure!(
                evidence.start == 0
                    && usize::try_from(evidence.end)? == lineage.revision.text.len()
            );
            anyhow::ensure!(evidence.quote_hash == support::digest(&lineage.revision.text));
            anyhow::ensure!(assertions[0].base.security == lineage.doc.base.security);
            anyhow::ensure!(
                !prepared.plan.outbox.is_empty(),
                "planned canonical writes omitted propagation"
            );
            no_canonical_effects(&client).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn unsupported_high_confidence_output_never_creates_canonical_knowledge() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        for case in [
            "empty",
            "wrong_hash",
            "derived_hash",
            "wrong_quote",
            "past_end",
            "utf8_boundary",
            "foreign_revision",
            "foreign_node",
            "semantic_predicate",
            "semantic_object",
            "semantic_entity_hint",
            "missing_support_receipt",
        ] {
            let client = support::storage(backend).await?;
            let lineage = support::lineage(&client, SPACE).await?;
            // Mint a legitimate receipt first. Mutating the claim/evidence then
            // tests the actual binding instead of trivially lacking authority.
            let mut raw = support::grounded_assertion(&lineage).await?;
            anyhow::ensure!(
                prepare(&client, &lineage, raw.clone()).await.is_ok(),
                "negative control lacked a valid starting plan"
            );
            match case {
                "empty" => {
                    raw.evidence_span.end = 0;
                    raw.quote = Some(String::new());
                    raw.evidence_span.quote_hash = support::digest("");
                }
                "wrong_hash" => raw.evidence_span.quote_hash = "0".repeat(64),
                "derived_hash" => {
                    raw.evidence_span.quote_hash =
                        format!("derived:{}", support::digest("Élodie|opened|Oriole"))
                }
                "wrong_quote" => raw.quote = Some("Élodie closed the Oriole laboratory.".into()),
                "past_end" => raw.evidence_span.end += 1,
                "utf8_boundary" => raw.evidence_span.start = 1,
                "foreign_revision" => {
                    raw.evidence_span.revision =
                        elephant_types::Link::new(RecordId::new_v4(TableName::DocRevision))
                }
                "foreign_node" => {
                    raw.evidence_span.node = Some(elephant_types::Link::new(RecordId::new_v4(
                        TableName::DocNode,
                    )))
                }
                "semantic_predicate" => raw.predicate_text = "closed".into(),
                "semantic_object" => raw.object_text = Some("another laboratory".into()),
                "semantic_entity_hint" => {
                    raw.subject_entity_id = Some(RecordId::new_v4(TableName::Entity).to_string())
                }
                "missing_support_receipt" => raw.support_verification = None,
                _ => anyhow::bail!("unknown authored counterexample"),
            }
            let outcome = prepare(&client, &lineage, raw).await;
            anyhow::ensure!(
                outcome.is_err(),
                "{backend:?}/{case}: invalid grounding accepted as successful promotion"
            );
            no_canonical_effects(&client).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn deleted_source_or_document_prevents_preparation_from_valid_evidence() -> anyhow::Result<()>
{
    for backend in BACKENDS {
        for delete_source in [false, true] {
            let client = support::storage(backend).await?;
            let lineage = support::lineage(&client, SPACE).await?;
            let raw = support::grounded_assertion(&lineage).await?;
            anyhow::ensure!(prepare(&client, &lineage, raw.clone()).await.is_ok());
            if delete_source {
                SourceRepository::new(client.clone())
                    .delete(SPACE, &lineage.source.base.id)
                    .await?;
            } else {
                DocRepository::new(client.clone())
                    .delete(SPACE, &lineage.doc.base.id)
                    .await?;
            }
            let outcome = prepare(&client, &lineage, raw).await;
            anyhow::ensure!(outcome.is_err(), "{backend:?}: deleted lineage published");
            no_canonical_effects(&client).await?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn direct_node_promotion_cannot_bypass_whole_source_finalization() -> anyhow::Result<()> {
    for backend in BACKENDS {
        let client = support::storage(backend).await?;
        let lineage = support::lineage(&client, SPACE).await?;
        let raw = support::grounded_assertion(&lineage).await?;
        anyhow::ensure!(prepare(&client, &lineage, raw.clone()).await.is_ok());
        anyhow::ensure!(
            promote_assertions_inline(
                &client,
                SPACE,
                &[raw],
                &RecordId::new_v4(TableName::WorkRun),
                &lineage.node.base.id,
                &lineage.revision.base.id,
                "qualification"
            )
            .await
            .is_err(),
            "node-local entrypoint published without sealed source expectations and attempt"
        );
        no_canonical_effects(&client).await?;
    }
    Ok(())
}
