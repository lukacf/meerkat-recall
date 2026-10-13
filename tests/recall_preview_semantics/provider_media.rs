//! Real multimodal renderer and managed-asset reads with controlled HTTP refusal.
//! No test analyzer, rendered-text winner or provider-relevance assertion.
use super::spy;
use anyhow::Context;
use elephant_pipeline::provider_dispatch::with_dispatch_credentials;
use elephant_pipeline::renderer::{multimodal::MultimodalRenderer, RenderContext, Renderer};
use elephant_pipeline::PipelineError;
use elephant_storage::{MediaAssetRepository, Repository, SpaceConfigRepository, StorageClient};
use elephant_types::provider_binding::ModelOperation;
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc::{Doc, DocMediaAttachment, DocMediaKind, DocMediaLocation};
use elephant_types::records::media_asset::{managed_asset_relative_path, MediaAsset};
use elephant_types::{RecordId, TableName};
use std::sync::Arc;

// One RGB pixel (64,128,192), generated as PNG signature + IHDR/IDAT/IEND with
// exact CRCs. This is transport input, never visual-recognition evidence.
const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGNwaDgAAAKEAYEml6crAAAAAElFTkSuQmCC";
const PNG_SHA: &str = "ca9b3f6a5185b96230396dca88491d802b92f983593f1c02f1c483ac67cca1ba";
const PNG_BYTES: u64 = 69;

fn counts(endpoints: &[spy::Spy; 2]) -> anyhow::Result<[usize; 2]> {
    Ok([endpoints[0].count()?, endpoints[1].count()?])
}

async fn media_document(storage: &StorageClient, space: &str) -> anyhow::Result<Doc> {
    let asset_id = RecordId::new_v4(TableName::MediaAsset);
    let assets = MediaAssetRepository::new(storage.clone());
    assets
        .create(MediaAsset {
            base: BaseFields::new(
                asset_id.clone(),
                space.into(),
                "D-media-transport-spy".into(),
            ),
            kind: DocMediaKind::Image,
            media_type: "image/png".into(),
            storage_path: managed_asset_relative_path(&asset_id),
            size_bytes: PNG_BYTES,
            sha256: PNG_SHA.into(),
            payload_base64: Some(PNG.into()),
            duration_ms: None,
            label: Some("one-original-pixel.png".into()),
        })
        .await?;
    let stored = assets
        .get_with_payload(space, &asset_id)
        .await?
        .context("persisted media missing")?;
    anyhow::ensure!(
        stored.payload_base64.as_deref() == Some(PNG)
            && stored.sha256 == PNG_SHA
            && stored.size_bytes == PNG_BYTES
            && stored.base.space_id == space,
        "managed input differs from original fixture"
    );
    Ok(Doc {
        base: BaseFields::new(
            RecordId::new_v4(TableName::Doc),
            space.into(),
            "D-media-transport-spy".into(),
        ),
        title: format!("Original media title for {space}"),
        text: String::new(),
        sidecar_text: Some(format!("Original sidecar for {space}")),
        source: None,
        media_attachments: vec![DocMediaAttachment {
            attachment_id: format!("{space}-original-frame"),
            kind: DocMediaKind::Image,
            media_type: "image/png".into(),
            location: DocMediaLocation::ManagedAsset {
                asset_id: asset_id.to_string(),
            },
            label: Some("one-original-pixel.png".into()),
            size_bytes: Some(PNG_BYTES),
            duration_ms: None,
            sha256: Some(PNG_SHA.into()),
        }],
        rendition_key: format!("{space}-media-transport-input"),
        metadata: serde_json::json!({}),
    })
}

async fn render(
    renderer: &MultimodalRenderer,
    storage: &StorageClient,
    selected: &str,
    doc: Doc,
) -> anyhow::Result<Result<elephant_pipeline::renderer::RenderResult, PipelineError>> {
    Ok(tokio::time::timeout(
        std::time::Duration::from_secs(10),
        with_dispatch_credentials(
            Arc::new(spy::SpyCredentials),
            Box::pin(renderer.render(RenderContext {
                doc,
                storage: Some(storage.clone()),
                space_id: selected.into(),
                config: None,
            })),
        ),
    )
    .await?)
}

fn require_space_refusal(
    result: Result<elephant_pipeline::renderer::RenderResult, PipelineError>,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        matches!(result, Err(PipelineError::ConfigError(ref message))
        if message.starts_with("provider_authorization:")),
        "selected/document mismatch must refuse as provider authorization before media lookup"
    );
    Ok(())
}

#[tokio::test]
async fn actual_media_renderer_checks_document_space_and_current_approval() -> anyhow::Result<()> {
    Box::pin(media_dispatch_contract()).await
}

async fn media_dispatch_contract() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = spy::storage(backend).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        let mut documents = Vec::new();
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            spy::configure(&repo, space, &endpoints[index].endpoint, 8 + index * 4).await?;
            documents.push(media_document(&storage, space).await?);
        }
        // The live analyzer is selected explicitly, without ambient model env.
        // Governance must replace this unapproved caller model with its profile.
        let renderer = MultimodalRenderer::for_live_model("unapproved-caller-media-model");
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            let doc = &documents[index];
            let before = counts(&endpoints)?;
            let mut wrong_document = doc.clone();
            // Its asset really exists in selected space. A missing-asset error
            // therefore cannot masquerade as a document-space authorization gate.
            wrong_document.base.space_id = spy::SPACES[1 - index].into();
            require_space_refusal(
                render(&renderer, &storage, space, wrong_document.clone()).await?,
            )?;
            wrong_document.media_attachments[0].location = DocMediaLocation::ManagedAsset {
                asset_id: RecordId::new_v4(TableName::MediaAsset).to_string(),
            };
            require_space_refusal(render(&renderer, &storage, space, wrong_document).await?)?;
            require_space_refusal(
                render(&renderer, &storage, "semantics-unregistered", doc.clone()).await?,
            )?;
            anyhow::ensure!(
                counts(&endpoints)? == before,
                "{backend:?}/{space}: wrong-space media dispatched"
            );
            check_approval_sequence(&renderer, &storage, &repo, &endpoints, index, doc).await?;
        }
    }
    Ok(())
}

async fn check_approval_sequence(
    renderer: &MultimodalRenderer,
    storage: &StorageClient,
    repo: &SpaceConfigRepository,
    endpoints: &[spy::Spy; 2],
    index: usize,
    doc: &Doc,
) -> anyhow::Result<()> {
    let space = spy::SPACES[index];
    let before = counts(endpoints)?;
    let mut binding = None;
    spy::edit(repo, space, |processing| {
        binding = processing.bindings.remove(&ModelOperation::MediaRender);
        Ok(())
    })
    .await?;
    let missing = render(renderer, storage, space, doc.clone()).await?;
    anyhow::ensure!(
        missing.is_err() && counts(endpoints)? == before,
        "missing media approval dispatched or returned a rendered projection"
    );
    let binding = binding.context("media binding missing before removal")?;
    spy::edit(repo, space, |processing| {
        processing
            .bindings
            .insert(ModelOperation::MediaRender, binding);
        Ok(())
    })
    .await?;
    let approved = render(renderer, storage, space, doc.clone()).await?;
    let mut expected = before;
    expected[index] += 1;
    anyhow::ensure!(
        approved.is_err() && counts(endpoints)? == expected,
        "real media analyzer must fail after exactly one approved physical dispatch"
    );
    let request = endpoints[index]
        .requests()?
        .pop()
        .context("media request missing")?;
    anyhow::ensure!(
        request.body["model"]
            == if index == 0 {
                "claude-spy-a"
            } else {
                "gpt-spy-b"
            }
    );
    let expected_key = format!("spy-only-SEMANTICS_SPY_{}_GENERATE", 8 + index * 4);
    anyhow::ensure!(
        request.credential
            == if index == 0 {
                expected_key
            } else {
                format!("Bearer {expected_key}")
            }
    );
    let body = request.body.to_string();
    anyhow::ensure!(
        body.contains(PNG)
            && body.contains(&doc.title)
            && body.contains(&format!("Original sidecar for {space}")),
        "real media dispatch lost original managed bytes/title/sidecar"
    );
    spy::edit(repo, space, |processing| {
        processing
            .bindings
            .get_mut(&ModelOperation::MediaRender)
            .context("media binding missing")?
            .revoked = true;
        Ok(())
    })
    .await?;
    let revoked = render(renderer, storage, space, doc.clone()).await?;
    anyhow::ensure!(
        revoked.is_err() && counts(endpoints)? == expected,
        "revoked media render dispatched again or returned a projection"
    );
    Ok(())
}

#[derive(Debug, Clone, Copy)]
enum InputChange {
    Unchanged,
    Payload,
    Envelope,
    Subjects,
    Deleted,
    DocAppears,
    ForeignDocAppears,
}

#[derive(Debug)]
struct ChangeDuringAdmission {
    storage: StorageClient,
    doc: Doc,
    change: InputChange,
    calls: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl elephant_embeddings::governed::ProviderDispatchObserver for ChangeDuringAdmission {
    async fn before_dispatch(
        &self,
        _call_id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let changed: anyhow::Result<()> = async {
            anyhow::ensure!(provenance.config.space_id == self.doc.base.space_id);
            anyhow::ensure!(provenance.operation == ModelOperation::MediaRender);
            // This is the inherited durable observer's wait, before the input
            // guard. The mutation is completed before admission returns.
            tokio::task::yield_now().await;
            match self.change {
                InputChange::Unchanged => return Ok(()),
                InputChange::DocAppears | InputChange::ForeignDocAppears => {
                    let mut doc = self.doc.clone();
                    if matches!(self.change, InputChange::ForeignDocAppears) {
                        doc.base.space_id = "semantics-foreign".into();
                    }
                    elephant_storage::DocRepository::new(self.storage.clone())
                        .create(doc)
                        .await?;
                    return Ok(());
                }
                _ => {}
            }
            let DocMediaLocation::ManagedAsset { asset_id } =
                &self.doc.media_attachments[0].location
            else {
                anyhow::bail!("fixture must load original managed bytes");
            };
            let id: RecordId = asset_id.parse()?;
            let repo = MediaAssetRepository::new(self.storage.clone());
            let mut asset = repo
                .get_with_payload(&self.doc.base.space_id, &id)
                .await?
                .context("original asset missing")?;
            match self.change {
                InputChange::Payload => asset.payload_base64 = Some("YWx0ZXJlZCBwYXlsb2Fk".into()),
                InputChange::Envelope => asset.base.security.level += 1,
                InputChange::Subjects => {
                    asset.base.subjects_state = elephant_types::records::SubjectsState::Complete
                }
                InputChange::Deleted => {
                    repo.delete(&self.doc.base.space_id, &id).await?;
                    return Ok(());
                }
                _ => anyhow::bail!("unexpected fixture branch"),
            }
            repo.update(&self.doc.base.space_id, &id, asset).await?;
            Ok(())
        }
        .await;
        changed
            .map_err(|error| elephant_embeddings::EmbeddingError::StorageError(error.to_string()))
    }

    async fn observe_usage(
        &self,
        _call_id: &str,
        _provenance: &elephant_types::provider_binding::ModelBindingProvenance,
        _input: Option<u64>,
        _output: Option<u64>,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        Err(elephant_embeddings::EmbeddingError::ProviderError(
            "refused HTTP spy has no usage".into(),
        ))
    }
}

#[tokio::test]
async fn actual_media_input_guard_runs_after_original_admission() -> anyhow::Result<()> {
    Box::pin(media_input_wait_contract()).await
}

async fn media_input_wait_contract() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = Box::pin(spy::storage(backend)).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            Box::pin(spy::configure(
                &repo,
                space,
                &endpoints[index].endpoint,
                8 + index * 4,
            ))
            .await?;
            for change in [
                InputChange::Unchanged,
                InputChange::Payload,
                InputChange::Envelope,
                InputChange::Subjects,
                InputChange::Deleted,
                InputChange::DocAppears,
                InputChange::ForeignDocAppears,
            ] {
                let doc = Box::pin(media_document(&storage, space)).await?;
                let observer = Arc::new(ChangeDuringAdmission {
                    storage: storage.clone(),
                    doc: doc.clone(),
                    change,
                    calls: std::sync::atomic::AtomicUsize::new(0),
                });
                let before = counts(&endpoints)?;
                let result = elephant_pipeline::provider_dispatch::with_dispatch_observer(
                    observer.clone(),
                    Box::pin(render(
                        &MultimodalRenderer::for_live_model("not-authority"),
                        &storage,
                        space,
                        doc,
                    )),
                )
                .await?;
                anyhow::ensure!(result.is_err(), "spy never supplies a media winner");
                anyhow::ensure!(observer.calls.load(std::sync::atomic::Ordering::SeqCst) == 1,
                    "{backend:?}/{space}/{change:?}: must reach actual inherited physical-send callback");
                let mut expected = before;
                if matches!(change, InputChange::Unchanged) {
                    expected[index] += 1;
                }
                anyhow::ensure!(
                    counts(&endpoints)? == expected,
                    "{backend:?}/{space}/{change:?}: original media mutation reached transport"
                );
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn actual_media_primitive_refuses_managed_or_malformed_fallback() -> anyhow::Result<()> {
    Box::pin(media_no_downgrade_contract()).await
}

async fn media_no_downgrade_contract() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = Box::pin(spy::storage(backend)).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoint = spy::Spy::start().await?;
        let space = spy::SPACES[0];
        Box::pin(spy::configure(&repo, space, &endpoint.endpoint, 8)).await?;
        let doc = Box::pin(media_document(&storage, space)).await?;
        let renderer = MultimodalRenderer::for_live_model("not-authority");
        let mut publication = doc.clone();
        publication.metadata["_publication"] = serde_json::json!({"malformed":true});
        let mut source_linked = doc.clone();
        source_linked.source = Some(elephant_types::Link::new(RecordId::new_v4(
            TableName::Source,
        )));
        let mut wrong_hash = doc.clone();
        wrong_hash.media_attachments[0].sha256 = Some("0".repeat(64));
        let mut wrong_size = doc.clone();
        wrong_size.media_attachments[0].size_bytes = Some(PNG_BYTES + 1);
        for invalid in [publication, source_linked, wrong_hash, wrong_size] {
            anyhow::ensure!(Box::pin(render(&renderer, &storage, space, invalid))
                .await?
                .is_err());
            anyhow::ensure!(
                endpoint.count()? == 0,
                "malformed custody downgraded to unmanaged"
            );
        }
        // An explicit original Work cannot enter the source-less primitive,
        // even if its selected Doc and space look plausible to the caller.
        let work = elephant_types::records::WorkItem {
            base: BaseFields::new(
                RecordId::new_v4(TableName::WorkItem),
                space.into(),
                "spy".into(),
            ),
            kind: "render_doc".into(),
            payload: serde_json::json!({"doc_id":doc.base.id.to_string()}),
            status: elephant_types::records::WorkItemStatus::Pending,
            priority: 0,
            idempotency_key: "unadmitted-media".into(),
            attempts: 0,
            lease_expires_at: None,
            leased_by: None,
            run_after: None,
            last_error: None,
        };
        let result = with_dispatch_credentials(
            Arc::new(spy::SpyCredentials),
            Box::pin(renderer.render_for_work(
                RenderContext {
                    doc: doc.clone(),
                    storage: Some(storage.clone()),
                    space_id: space.into(),
                    config: None,
                },
                &work,
            )),
        )
        .await;
        anyhow::ensure!(
            result.is_err() && endpoint.count()? == 0,
            "missing original custody became Independent"
        );

        let mut foreign = doc.clone();
        foreign.base.space_id = "semantics-foreign".into();
        elephant_storage::DocRepository::new(storage.clone())
            .create(foreign)
            .await?;
        anyhow::ensure!(Box::pin(render(&renderer, &storage, space, doc))
            .await?
            .is_err());
        anyhow::ensure!(
            endpoint.count()? == 0,
            "foreign physical Doc hidden by scoped miss"
        );
    }
    Ok(())
}

#[derive(Debug)]
struct ChangeOnCompletion {
    storage: StorageClient,
    doc: Doc,
    change: bool,
    admitted: std::sync::Mutex<Option<String>>,
    usage: std::sync::atomic::AtomicUsize,
    finished: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl elephant_embeddings::governed::ProviderDispatchObserver for ChangeOnCompletion {
    async fn before_dispatch(
        &self,
        call_id: &str,
        provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        if provenance.config.space_id != self.doc.base.space_id
            || provenance.operation != ModelOperation::MediaRender
        {
            return Err(elephant_embeddings::EmbeddingError::ProviderError(
                "foreign call".into(),
            ));
        }
        let mut admitted = self.admitted.lock().map_err(|_| {
            elephant_embeddings::EmbeddingError::ProviderError("spy lock poisoned".into())
        })?;
        if admitted.replace(call_id.into()).is_some() {
            return Err(elephant_embeddings::EmbeddingError::ProviderError(
                "unexpected extra dispatch".into(),
            ));
        }
        Ok(())
    }
    async fn observe_usage(
        &self,
        call_id: &str,
        _provenance: &elephant_types::provider_binding::ModelBindingProvenance,
        _input: Option<u64>,
        _output: Option<u64>,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        self.check_call(call_id)?;
        self.usage.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
    async fn dispatch_finished(
        &self,
        call_id: &str,
        _provenance: &elephant_types::provider_binding::ModelBindingProvenance,
    ) -> elephant_embeddings::EmbeddingResult<()> {
        self.check_call(call_id)?;
        self.finished
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.change {
            let result: anyhow::Result<()> = async {
                let DocMediaLocation::ManagedAsset { asset_id } =
                    &self.doc.media_attachments[0].location
                else {
                    anyhow::bail!("managed fixture required");
                };
                let id: RecordId = asset_id.parse()?;
                let repo = MediaAssetRepository::new(self.storage.clone());
                let mut asset = repo
                    .get_with_payload(&self.doc.base.space_id, &id)
                    .await?
                    .context("asset missing")?;
                asset.base.security.level += 1;
                repo.update(&self.doc.base.space_id, &id, asset).await?;
                Ok(())
            }
            .await;
            result.map_err(|error| {
                elephant_embeddings::EmbeddingError::StorageError(error.to_string())
            })?;
        }
        Ok(())
    }
}
impl ChangeOnCompletion {
    fn check_call(&self, call_id: &str) -> elephant_embeddings::EmbeddingResult<()> {
        let admitted = self.admitted.lock().map_err(|_| {
            elephant_embeddings::EmbeddingError::ProviderError("spy lock poisoned".into())
        })?;
        if admitted.as_deref() != Some(call_id) {
            return Err(elephant_embeddings::EmbeddingError::ProviderError(
                "receipt identity changed".into(),
            ));
        }
        Ok(())
    }
}

#[tokio::test]
async fn actual_media_postresponse_guard_preserves_original_completion() -> anyhow::Result<()> {
    Box::pin(media_completion_contract()).await
}

async fn media_completion_contract() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = Box::pin(spy::storage(backend)).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            Box::pin(spy::configure(
                &repo,
                space,
                &endpoints[index].endpoint,
                8 + index * 4,
            ))
            .await?;
            endpoints[index].media_projection()?;
            for change in [false, true] {
                let doc = Box::pin(media_document(&storage, space)).await?;
                let observer = Arc::new(ChangeOnCompletion {
                    storage: storage.clone(),
                    doc: doc.clone(),
                    change,
                    admitted: std::sync::Mutex::new(None),
                    usage: std::sync::atomic::AtomicUsize::new(0),
                    finished: std::sync::atomic::AtomicUsize::new(0),
                });
                let before = counts(&endpoints)?;
                let result = elephant_pipeline::provider_dispatch::with_dispatch_observer(
                    observer.clone(),
                    Box::pin(render(
                        &MultimodalRenderer::for_live_model("not-authority"),
                        &storage,
                        space,
                        doc,
                    )),
                )
                .await?;
                let mut expected = before;
                expected[index] += 1;
                anyhow::ensure!(
                    counts(&endpoints)? == expected,
                    "completion control requires exactly one actual physical dispatch"
                );
                anyhow::ensure!(
                    observer.usage.load(std::sync::atomic::Ordering::SeqCst) > 0
                        && observer.finished.load(std::sync::atomic::Ordering::SeqCst) == 1,
                    "added media guard replaced original measured accounting"
                );
                if change {
                    anyhow::ensure!(
                        result.is_err(),
                        "changed media input published a rendered projection"
                    );
                } else {
                    let output = result?;
                    anyhow::ensure!(output.text.contains("CONTROLLED transport marker; not visual evidence."),
                        "unmanaged primitive no longer retains its actual successful transport path");
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn actual_media_retry_refuses_replaced_original_asset() -> anyhow::Result<()> {
    Box::pin(media_retry_contract()).await
}

async fn media_retry_contract() -> anyhow::Result<()> {
    for backend in spy::BACKENDS {
        let storage = Box::pin(spy::storage(backend)).await?;
        let repo = SpaceConfigRepository::new(storage.clone());
        let endpoints = [spy::Spy::start().await?, spy::Spy::start().await?];
        for (index, space) in spy::SPACES.into_iter().enumerate() {
            Box::pin(spy::configure(
                &repo,
                space,
                &endpoints[index].endpoint,
                8 + index * 4,
            ))
            .await?;
            let doc = Box::pin(media_document(&storage, space)).await?;
            let DocMediaLocation::ManagedAsset { asset_id } = &doc.media_attachments[0].location
            else {
                anyhow::bail!("managed fixture required");
            };
            let id: RecordId = asset_id.parse()?;
            let assets = MediaAssetRepository::new(storage.clone());
            let mut replacement = assets
                .get_with_payload(space, &id)
                .await?
                .context("asset missing")?;
            replacement.payload_base64 = Some("cmVwbGFjZWQgYWZ0ZXIgZmlyc3Qgc2VuZA==".into());
            endpoints[index].before_retry(spy::BeforeRetry::ReplaceMedia(assets, replacement))?;
            let observer = Arc::new(ChangeDuringAdmission {
                storage: storage.clone(),
                doc: doc.clone(),
                change: InputChange::Unchanged,
                calls: std::sync::atomic::AtomicUsize::new(0),
            });
            let before = counts(&endpoints)?;
            let result = elephant_pipeline::provider_dispatch::with_dispatch_observer(
                observer.clone(),
                Box::pin(render(
                    &MultimodalRenderer::for_live_model("not-authority"),
                    &storage,
                    space,
                    doc,
                )),
            )
            .await?;
            let mut expected = before;
            expected[index] += 1;
            anyhow::ensure!(
                result.is_err() && counts(&endpoints)? == expected,
                "changed media retry reached outbound transport"
            );
            anyhow::ensure!(
                observer.calls.load(std::sync::atomic::Ordering::SeqCst) == 2,
                "must exercise first SDK send and retry guard, not merely stop on a response error"
            );
        }
    }
    Ok(())
}
