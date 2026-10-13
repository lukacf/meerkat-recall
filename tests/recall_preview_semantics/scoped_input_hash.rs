//! Public intake, real queued publication and scoped search retain input proof.
//! Loopback vectors are transport spies, never semantic relevance evidence.

use super::{index_fixture, spy};
use anyhow::Context;
use async_trait::async_trait;
use elephant_mcp::handlers::{IngestSourceSimpleHandler, ToolDispatcher};
use elephant_mcp::scoped_search::{ScopedSearchHandler, SearchResponse};
use elephant_pipeline::extraction::{
    DefaultExtractor, ExtractionContext, ExtractionResult, Extractor,
};
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_pipeline::PipelineError;
use elephant_storage::source_publication::SourcePublicationRepository;
use elephant_storage::source_publication_manifest::SourcePublicationManifestRepository;
use elephant_storage::{DocRevisionRepository, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::publication::{OperationState, PublicationFence};
use elephant_types::records::{work::PluginManifest, SourceType};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

// Reuse the existing real dispatcher driver, including original attempt fences,
// failed-run retention and due-time waits. Do not fabricate completed rows.
#[expect(
    dead_code,
    reason = "this inclusion uses drive_pipeline; retain shared drive_item for the independent E2E scenarios"
)]
#[path = "../e2e/src/pipeline_driver.rs"]
mod pipeline_driver;

const EXTERNAL: &str = "public-input-hash";
const REVISION: &str = "public-input-r1";
const RAW: &str = "A small synthetic document for the OB3 preview.";
const RENDERED: &str = "A small synthetic document for the OB3 preview.\n";
const RENDERED_SHA256: &str = "56c0d4337eec82adaf7906ffb905bb9ba6a6f01d67ea774b0e6433e90b147e05";

struct NoExtraction(DefaultExtractor);

#[async_trait]
impl Extractor for NoExtraction {
    fn manifest(&self) -> &PluginManifest {
        self.0.manifest()
    }

    async fn extract(
        &self,
        _context: ExtractionContext,
    ) -> Result<ExtractionResult, PipelineError> {
        Err(PipelineError::InvalidState(
            "Retrieval-only input proof regression must not request extraction".into(),
        ))
    }
}

async fn query(storage: &StorageClient, space: &str, ranking: &str) -> anyhow::Result<Value> {
    let mut dispatcher = ToolDispatcher::new(index_fixture::reader(storage, space));
    dispatcher.register(ScopedSearchHandler);
    Ok(with_dispatch_credentials(
        Arc::new(spy::SpyCredentials),
        dispatcher.execute(
            "search_scoped",
            json!({
                "query":"synthetic", "limit":10, "ranking_mode":ranking,
                "source_revision_constraints":{"public-input-hash":REVISION}
            }),
        ),
    )
    .await?)
}

fn require_original_evidence(response: Value, document_revision: &str) -> anyhow::Result<()> {
    anyhow::ensure!(response["ok"] == true, "scoped search refused: {response}");
    let response: SearchResponse = serde_json::from_value(response["data"].clone())?;
    let evidence: Vec<_> = response
        .results
        .iter()
        .flat_map(|result| &result.evidence)
        .collect();
    anyhow::ensure!(!evidence.is_empty(), "original source evidence is absent");
    for entry in evidence {
        anyhow::ensure!(
            entry.source_id == EXTERNAL
                && entry.revision == REVISION
                && entry.internal_doc_revision_id == document_revision
                && entry.quote == RAW
                && entry.span.start == 0
                && entry.span.end as usize == RAW.len()
                && entry.rendered_span.start == 0
                && entry.rendered_span.end as usize == RAW.len()
                && entry.rendered_quote_sha256 == index_fixture::digest(RAW)
                && entry.rendered_text_sha256 == RENDERED_SHA256,
            "public evidence changed the original identity, bytes or coordinates"
        );
    }
    Ok(())
}

// Deliberate storage corruption after genuine completion. The source control's
// original opaque envelope is preserved, and only its payload is replaced.
// No new acceptance, head, publication receipt or completion is manufactured.
struct AcceptedControl {
    key: String,
    payload: String,
}

async fn accepted_control(
    storage: &StorageClient,
    space: &str,
    publication: &PublicationFence,
) -> anyhow::Result<AcceptedControl> {
    let source = publication.source_id.to_string();
    let tuple = [space, source.as_str(), publication.revision_id.as_str()]
        .iter()
        .map(|part| format!("{}:{part}", part.len()))
        .collect::<String>();
    let key = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, tuple.as_bytes()).to_string();
    let payload = if let Some(store) = storage.embedded_store() {
        let id = format!("source_revision:{key}");
        store
            .mutate(|tx| {
                let rows = tx.scan_extension_rows("source_revision")?;
                let row = rows
                    .iter()
                    .find(|(candidate, row)| candidate == &id && row["space_id"] == space)
                    .ok_or_else(|| {
                        elephant_storage::StorageError::NotFound("accepted fixture input".into())
                    })?;
                row.1["payload"].as_str().map(str::to_owned).ok_or_else(|| {
                    elephant_storage::StorageError::DataIntegrityError(
                        "accepted fixture payload missing".into(),
                    )
                })
            })
            .await?
    } else {
        #[derive(Deserialize)]
        struct PayloadRow {
            payload: String,
        }
        let mut response = storage
            .query_bind_checked(
                "SELECT payload FROM $id WHERE space_id=$space;",
                elephant_storage::bindings! {
                    "id" => surrealdb::sql::Thing::from(("source_revision", key.as_str())),
                    "space" => space.to_owned(),
                },
            )
            .await?;
        let rows: Vec<PayloadRow> = response.take(0)?;
        let [row] = rows.as_slice() else {
            anyhow::bail!("exact accepted source control is missing or duplicated");
        };
        row.payload.clone()
    };
    Ok(AcceptedControl { key, payload })
}

async fn replace_control_payload(
    storage: &StorageClient,
    space: &str,
    control: &AcceptedControl,
    payload: &str,
) -> anyhow::Result<()> {
    if let Some(store) = storage.embedded_store() {
        let id = format!("source_revision:{}", control.key);
        store
            .mutate(|tx| {
                let rows = tx.scan_extension_rows("source_revision")?;
                let (_, mut row) = rows
                    .into_iter()
                    .find(|(candidate, row)| candidate == &id && row["space_id"] == space)
                    .ok_or_else(|| {
                        elephant_storage::StorageError::NotFound("accepted fixture input".into())
                    })?;
                row["payload"] = json!(payload);
                tx.upsert_extension_row("source_revision", row)?;
                Ok(())
            })
            .await?;
    } else {
        storage
            .query_bind_checked(
                "UPDATE $id SET payload=$payload WHERE space_id=$space;",
                elephant_storage::bindings! {
                    "id" => surrealdb::sql::Thing::from(("source_revision", control.key.as_str())),
                    "space" => space.to_owned(), "payload" => payload.to_owned(),
                },
            )
            .await?;
    }
    Ok(())
}

async fn contract(backend: spy::Backend) -> anyhow::Result<()> {
    let storage = Box::pin(spy::storage(backend)).await?;
    let configurations = SpaceConfigRepository::new(storage.clone());
    for (index, space) in spy::SPACES.into_iter().enumerate() {
        let endpoint = spy::Spy::start().await?;
        Box::pin(spy::configure(
            &configurations,
            space,
            &endpoint.endpoint,
            8 + index * 4,
        ))
        .await?;
        let mut context = index_fixture::reader(&storage, space);
        context.principal.scopes.extend(
            ["write:doc", "read:doc:metadata", "read:source"]
                .into_iter()
                .map(str::to_owned),
        );
        let mut dispatcher = ToolDispatcher::new(context);
        dispatcher.register(IngestSourceSimpleHandler::new(storage.clone()));
        let intake = dispatcher
            .execute(
                "ingest_source_simple",
                json!({
                    "name":EXTERNAL, "source_type":"manual", "title":"Original public input",
                    "text":RAW, "ingest_mode":"retrieval",
                    "intake":{"revision_id":REVISION,"idempotency_key":"public-input-hash-1"}
                }),
            )
            .await?;
        anyhow::ensure!(intake["ok"] == true, "public intake refused: {intake}");
        anyhow::ensure!(
            endpoint.count()? == 0,
            "intake unexpectedly called a provider"
        );
        let publications = SourcePublicationRepository::new(storage.clone());
        let head = publications
            .external_head(space, SourceType::Manual, EXTERNAL)
            .await?
            .context("public intake did not create the exact named source")?;
        let publication = PublicationFence {
            source_id: head.source_id.clone(),
            generation: head.generation.clone(),
            revision_id: REVISION.into(),
            operation_id: head.operation_id.clone(),
        };
        let accepted = publications
            .revision(space, &head.source_id, REVISION)
            .await?
            .context("public accepted source missing")?;
        anyhow::ensure!(accepted.content_hash == accepted.source.hash);
        anyhow::ensure!(
            accepted.content_hash.trim_start_matches("sha256:") != index_fixture::digest(RAW),
            "regression must use the actual public producer envelope hash, not a text-hash fixture"
        );
        anyhow::ensure!(
            accepted
                .source
                .manual_doc_template()?
                .context("raw input")?
                .text
                == RAW
        );
        with_dispatch_credentials(
            Arc::new(spy::SpyCredentials),
            Box::pin(pipeline_driver::drive_pipeline(
                &storage,
                space,
                &NoExtraction(DefaultExtractor::new()),
                64,
            )),
        )
        .await?;
        let operation = publications
            .operation(space, &publication.operation_id)
            .await?
            .context("same operation missing")?;
        anyhow::ensure!(operation.state == OperationState::Completed);
        let completed_head = publications
            .head(space, &head.source_id)
            .await?
            .context("head")?;
        anyhow::ensure!(
            completed_head.generation == publication.generation
                && completed_head.operation_id == publication.operation_id
                && completed_head.published_revision.as_deref() == Some(REVISION)
        );
        let manifests = SourcePublicationManifestRepository::new(storage.clone());
        let manifest = manifests
            .get(space, &publication)
            .await?
            .context("original manifest")?;
        anyhow::ensure!(manifest.documents.len() == 1);
        let mapping = &manifest.documents[0];
        let rendered = DocRevisionRepository::new(storage.clone())
            .get(space, &mapping.doc_revision_id)
            .await?
            .context("actual rendered revision")?;
        anyhow::ensure!(
            rendered.text == RENDERED && index_fixture::digest(RENDERED) == RENDERED_SHA256
        );
        let before_query = endpoint.count()?;
        anyhow::ensure!(before_query > 0, "actual document embedding was omitted");
        require_original_evidence(
            query(&storage, space, "lexical").await?,
            &rendered.base.id.to_string(),
        )?;
        anyhow::ensure!(endpoint.count()? == before_query);
        require_original_evidence(
            query(&storage, space, "semantic").await?,
            &rendered.base.id.to_string(),
        )?;
        anyhow::ensure!(
            endpoint.count()? == before_query + 1,
            "query did not reach its governed transport"
        );
        anyhow::ensure!(
            endpoint
                .requests()?
                .iter()
                .all(|request| request.path.ends_with("/embeddings")),
            "Retrieval regression invoked generation"
        );

        let completion = manifests
            .completion(space, &publication)
            .await?
            .context("original canonical completion missing")?;
        anyhow::ensure!(completion.accepted_source_fingerprint.is_some());
        let control = accepted_control(&storage, space, &publication).await?;
        for corruption in ["raw_template", "agreeing_dedup_hashes"] {
            let mut changed: Value = serde_json::from_str(&control.payload)?;
            if corruption == "raw_template" {
                changed["revision"]["source"]["metadata"]["_manual_doc"]["text"] =
                    json!("A small synthetic document for an unaccepted replacement.");
            } else {
                changed["revision"]["content_hash"] = json!("matching-but-uncommitted");
                changed["revision"]["source"]["hash"] = json!("matching-but-uncommitted");
            }
            let changed_payload = serde_json::to_string(&changed)?;
            replace_control_payload(&storage, space, &control, &changed_payload).await?;
            anyhow::ensure!(
                accepted_control(&storage, space, &publication)
                    .await?
                    .payload
                    == changed_payload
            );
            let refused = query(&storage, space, "lexical").await?;
            if refused["ok"] == true {
                let refused: SearchResponse = serde_json::from_value(refused["data"].clone())?;
                anyhow::ensure!(
                    refused.results.is_empty(),
                    "{corruption} borrowed original canonical completion"
                );
            }
            anyhow::ensure!(endpoint.count()? == before_query + 1);
            replace_control_payload(&storage, space, &control, &control.payload).await?;
            anyhow::ensure!(
                manifests.completion(space, &publication).await?.as_ref() == Some(&completion),
                "input corruption must never rewrite original completion authority"
            );
            require_original_evidence(
                query(&storage, space, "lexical").await?,
                &rendered.base.id.to_string(),
            )?;
        }
    }
    Ok(())
}

#[tokio::test]
async fn embedded_public_intake_hash_uses_original_completion_proof() -> anyhow::Result<()> {
    Box::pin(contract(spy::Backend::ElephantDb)).await
}

#[tokio::test]
async fn surreal_public_intake_hash_uses_original_completion_proof() -> anyhow::Result<()> {
    Box::pin(contract(spy::Backend::SurrealMemory)).await
}
