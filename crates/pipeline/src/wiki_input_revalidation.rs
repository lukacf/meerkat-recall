//! Bounded read-only recomputation of standalone editorial inputs.
use super::*;

pub(super) struct WikiInputFence {
    pub(super) epoch: String,
    original: Value,
    remaining: usize,
}

// These are generated graph-projection identities and publication clocks, never
// source/revision IDs or evidence clocks. Everything else, including the entire
// original definition, provenance, dependencies, citations, freshness, validation,
// model-visible catalog and graph contents, participates in exact equality.
fn input_contract(inputs: &WikiArticleInputs) -> PipelineResult<Value> {
    let mut view = serde_json::to_value(&inputs.context.view).map_err(|_| contract_error())?;
    let mut revision =
        serde_json::to_value(&inputs.context.revision).map_err(|_| contract_error())?;
    if !inputs.context.persisted {
        for key in [
            "created_at",
            "updated_at",
            "current_revision",
            "published_at",
        ] {
            view.as_object_mut().ok_or_else(contract_error)?.remove(key);
        }
        for key in ["id", "created_at", "updated_at", "compiled_at"] {
            revision
                .as_object_mut()
                .ok_or_else(contract_error)?
                .remove(key);
        }
    }
    Ok(json!({
        "version": 1,
        "definition": inputs.definition,
        "view": view,
        "revision": revision,
        "persisted": inputs.context.persisted,
        "catalog": inputs.catalog,
        "catalog_json": inputs.catalog_json,
        "source_inputs": inputs.source_inputs,
    }))
}

fn contract_error() -> PipelineError {
    PipelineError::ValidationError("Invalid editorial input contract".into())
}

impl WikiInputFence {
    pub(super) fn new(
        inputs: &WikiArticleInputs,
        epoch: &str,
        allow_revalidation: bool,
    ) -> PipelineResult<Self> {
        Ok(Self {
            epoch: epoch.to_owned(),
            original: input_contract(inputs)?,
            remaining: if allow_revalidation { 2 } else { 0 },
        })
    }
}

impl WikiEditorialCompiler {
    pub(super) async fn capture_article_source_inputs(
        &self,
        space: &str,
        definition: &KnowledgeViewDefinition,
        context: &KnowledgeViewRevision,
    ) -> PipelineResult<
        Vec<(
            elephant_types::records::Source,
            Option<elephant_types::publication::SourceHead>,
        )>,
    > {
        // Direct library calls need the same original-head guarantee as a
        // request-scoped ReadEligibility reader. These are transient snapshots,
        // never an additional persisted source authority.
        let mut ids = definition
            .base
            .provenance
            .sources
            .iter()
            .chain(context.base.provenance.sources.iter())
            .cloned()
            .collect::<BTreeSet<_>>();
        for id in context
            .citations
            .iter()
            .filter_map(|citation| citation.source_id.as_deref())
        {
            ids.insert(id.parse::<RecordId>().map_err(|_| contract_error())?);
        }
        let sources = elephant_storage::SourceRepository::new(self.storage.clone());
        let publications = elephant_storage::source_publication::SourcePublicationRepository::new(
            self.storage.clone(),
        );
        let mut original = Vec::with_capacity(ids.len());
        for id in ids {
            let source = sources.get(space, &id).await?.ok_or_else(|| {
                elephant_storage::StorageError::Conflict("Wiki source is unavailable".into())
            })?;
            let head = publications.head(space, &id).await?;
            original.push((source, head));
        }
        Ok(original)
    }

    pub(super) async fn verify_article_input_fence(
        &self,
        space: &str,
        definition: &RecordId,
        publish: bool,
        fence: &mut WikiInputFence,
    ) -> PipelineResult<()> {
        let current =
            elephant_storage::canonical_commit::CanonicalRepository::new(self.storage.clone())
                .current_epoch(space)
                .await?;
        if current == fence.epoch {
            return Ok(());
        }
        if fence.remaining == 0 {
            return Err(elephant_storage::StorageError::Conflict(
                "Wiki inputs changed during preparation".into(),
            )
            .into());
        }
        fence.remaining -= 1;
        // Recompute the complete bounded selector, including negative membership
        // decisions. Rechecking only the old cited records would miss new facts.
        // The original publication authority is retained; no grant is recaptured.
        let fresh = boxed_wiki_phase(
            "article.revalidate_inputs",
            self.prepare_article_inputs(space, definition, publish, &current),
        )
        .await?;
        if input_contract(&fresh)? != fence.original {
            return Err(elephant_storage::StorageError::Conflict(
                "Selected wiki inputs changed during preparation".into(),
            )
            .into());
        }
        // Catalog augmentation is also inside the newly captured epoch. A writer
        // racing this read cannot be adopted by simply replacing the token.
        elephant_knowledge_views::verify_projection_epoch(&self.storage, space, &current).await?;
        verify_wiki_publication_authority(self.publication_authority.as_ref()).await?;
        fence.epoch = current;
        Ok(())
    }
}
