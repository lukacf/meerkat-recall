//! Public Rust facade for embedding Elephant.

#![allow(missing_docs, clippy::missing_errors_doc)]

use std::path::{Path, PathBuf};

use elephant_pipeline::scheduler::ingest_work_kinds::enqueue_triage_source_work_item;
use elephant_pipeline_bin::config::PipelineConfig;
use elephant_storage::{
    deterministic_source_id_for_ingest, persist_manual_doc_for_source,
    update_existing_source_for_reingest, AssertionRepository, AttributeRepository,
    ConflictGroupRepository, DocNodeRepository, DocRepository, DocRevisionRepository,
    EntityRepository, EventRepository, FreshnessRepository, IngestDecisionRepository,
    PredicateAliasRepository, PredicateRegistryRepository, RelRepository, Repository,
    SourceRepository, SpaceConfigRepository, StorageBackendKind, StorageClient, StorageConfig,
    StorageError, SystemEventRepository, TruthSlotRepository, WorkArtifactRepository,
    WorkDeadletterRepository, WorkEdgeRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::error::TypeError;
use elephant_types::records::base::BaseFields;
use elephant_types::records::{Doc, IngestMode, ManualDocTemplate, Source, SourceType};
use elephant_types::{RecordId, TableName};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::watch;
use tokio::task::JoinHandle;

pub use elephant_storage as storage;
pub use elephant_types as types;

pub type ElephantResult<T> = Result<T, ElephantError>;

#[derive(Debug, thiserror::Error)]
pub enum ElephantError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Pipeline(#[from] elephant_pipeline::error::PipelineError),
    #[error(transparent)]
    Type(#[from] TypeError),
    #[error(transparent)]
    Runtime(#[from] anyhow::Error),
}

#[derive(Debug)]
pub struct ElephantBuilder {
    storage_config: StorageConfig,
    worker_config: Option<PipelineConfig>,
}

impl ElephantBuilder {
    #[must_use]
    pub fn with_snapshot_load(mut self, path: impl Into<PathBuf>) -> Self {
        self.storage_config.snapshot_load = Some(path.into());
        self
    }

    #[must_use]
    pub fn with_workers(mut self, config: PipelineConfig) -> Self {
        self.worker_config = Some(config);
        self
    }

    pub async fn open(self) -> ElephantResult<Elephant> {
        let storage = StorageClient::new(self.storage_config).await?;
        let worker = self
            .worker_config
            .map(|config| ManagedWorker::start(config, storage.clone()));
        Ok(Elephant { storage, worker })
    }
}

#[derive(Debug)]
pub struct Elephant {
    storage: StorageClient,
    worker: Option<ManagedWorker>,
}

impl Elephant {
    #[must_use]
    pub fn embedded() -> ElephantBuilder {
        ElephantBuilder {
            storage_config: StorageConfig {
                backend: StorageBackendKind::ElephantDb,
                ..StorageConfig::default()
            },
            worker_config: None,
        }
    }

    #[must_use]
    pub const fn surreal(config: StorageConfig) -> ElephantBuilder {
        ElephantBuilder {
            storage_config: config,
            worker_config: None,
        }
    }

    #[must_use]
    pub const fn storage(&self) -> &StorageClient {
        &self.storage
    }

    #[must_use]
    pub fn repositories(&self) -> Repositories {
        Repositories::new(self.storage.clone())
    }

    pub async fn status(&self) -> ElephantResult<ElephantStatus> {
        let storage_ready = self.storage.health_check().await.is_ok();
        let worker_running = self.worker.as_ref().is_some_and(ManagedWorker::is_running);
        Ok(ElephantStatus {
            backend: self.storage.backend().as_str().to_string(),
            storage_ready,
            worker_running,
        })
    }

    pub async fn ingest_text(&self, request: IngestTextRequest) -> ElephantResult<IngestReceipt> {
        let hash = sha256_hex(request.text.as_bytes());
        let mut source = Source::new(
            BaseFields::new(
                RecordId::new_v4(TableName::Source),
                request.space_id.clone(),
                request.actor.clone(),
            ),
            request.source_type,
            hash.clone(),
        );
        source.external_id = request.external_id.clone();
        source.uri = request.uri.clone();
        source.author = request.author.clone();
        source.metadata = request.source_metadata.clone();
        source.base.id = deterministic_source_id_for_ingest(&source);
        let doc_id = deterministic_doc_id(&request.space_id, &source.base.id, &hash);
        let rendition_key = format!("text:{hash}");
        let doc = Doc {
            base: BaseFields::new(
                doc_id.clone(),
                request.space_id.clone(),
                request.actor.clone(),
            ),
            title: request.title.clone(),
            text: request.text.clone(),
            sidecar_text: request.sidecar_text.clone(),
            source: None,
            media_attachments: Vec::new(),
            rendition_key: rendition_key.clone(),
            metadata: request.doc_metadata.clone(),
        };
        source.set_manual_doc_template(&ManualDocTemplate {
            doc_id: doc_id.to_string(),
            title: request.title,
            text: request.text,
            sidecar_text: request.sidecar_text,
            media_attachments: Vec::new(),
            rendition_key,
            metadata: request.doc_metadata,
        })?;
        source.set_requested_ingest_mode(request.ingest_mode);

        let source_repo = SourceRepository::new(self.storage.clone());
        let persisted_source = match source_repo
            .get_with_deleted(&request.space_id, &source.base.id)
            .await?
        {
            Some(existing) => {
                update_existing_source_for_reingest(&source_repo, &existing, source).await?
            }
            None => source_repo.create(source).await?,
        };
        let persisted_doc =
            persist_manual_doc_for_source(&self.storage, &persisted_source, doc).await?;
        let work_item_id = enqueue_triage_source_work_item(
            &self.storage,
            &persisted_source,
            &request.actor,
            request.priority,
        )
        .await?;

        Ok(IngestReceipt {
            source_id: persisted_source.base.id,
            doc_id: persisted_doc.base.id,
            work_item_id,
        })
    }

    pub async fn snapshot_export(&self, path: impl AsRef<Path>) -> ElephantResult<()> {
        self.storage.export_snapshot(path.as_ref()).await?;
        Ok(())
    }

    pub async fn snapshot_import(&self, path: impl AsRef<Path>) -> ElephantResult<()> {
        self.storage.import_snapshot(path.as_ref()).await?;
        Ok(())
    }

    pub async fn shutdown(mut self) -> ElephantResult<()> {
        if let Some(worker) = self.worker.take() {
            worker.shutdown().await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestTextRequest {
    pub space_id: String,
    pub actor: String,
    pub title: String,
    pub text: String,
    #[serde(default)]
    pub sidecar_text: Option<String>,
    #[serde(default = "default_source_type")]
    pub source_type: SourceType,
    #[serde(default)]
    pub external_id: Option<String>,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub author: Option<String>,
    #[serde(default = "default_ingest_mode")]
    pub ingest_mode: IngestMode,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default = "empty_object")]
    pub source_metadata: serde_json::Value,
    #[serde(default = "empty_object")]
    pub doc_metadata: serde_json::Value,
}

impl Default for IngestTextRequest {
    fn default() -> Self {
        Self {
            space_id: String::new(),
            actor: String::new(),
            title: String::new(),
            text: String::new(),
            sidecar_text: None,
            source_type: default_source_type(),
            external_id: None,
            uri: None,
            author: None,
            ingest_mode: default_ingest_mode(),
            priority: default_priority(),
            source_metadata: empty_object(),
            doc_metadata: empty_object(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestReceipt {
    pub source_id: RecordId,
    pub doc_id: RecordId,
    pub work_item_id: RecordId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElephantStatus {
    pub backend: String,
    pub storage_ready: bool,
    pub worker_running: bool,
}

#[derive(Debug, Clone)]
pub struct Repositories {
    storage: StorageClient,
}

impl Repositories {
    #[must_use]
    pub const fn new(storage: StorageClient) -> Self {
        Self { storage }
    }

    #[must_use]
    pub fn entities(&self) -> EntityRepository {
        EntityRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn relationships(&self) -> RelRepository {
        RelRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn docs(&self) -> DocRepository {
        DocRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn sources(&self) -> SourceRepository {
        SourceRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn events(&self) -> EventRepository {
        EventRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn system_events(&self) -> SystemEventRepository {
        SystemEventRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn assertions(&self) -> AssertionRepository {
        AssertionRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn attributes(&self) -> AttributeRepository {
        AttributeRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn doc_nodes(&self) -> DocNodeRepository {
        DocNodeRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn doc_revisions(&self) -> DocRevisionRepository {
        DocRevisionRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn ingest_decisions(&self) -> IngestDecisionRepository {
        IngestDecisionRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn work_items(&self) -> WorkItemRepository {
        WorkItemRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn work_edges(&self) -> WorkEdgeRepository {
        WorkEdgeRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn work_runs(&self) -> WorkRunRepository {
        WorkRunRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn work_artifacts(&self) -> WorkArtifactRepository {
        WorkArtifactRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn work_deadletters(&self) -> WorkDeadletterRepository {
        WorkDeadletterRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn space_configs(&self) -> SpaceConfigRepository {
        SpaceConfigRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn predicates(&self) -> PredicateRegistryRepository {
        PredicateRegistryRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn predicate_aliases(&self) -> PredicateAliasRepository {
        PredicateAliasRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn conflicts(&self) -> ConflictGroupRepository {
        ConflictGroupRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn truth_slots(&self) -> TruthSlotRepository {
        TruthSlotRepository::new(self.storage.clone())
    }

    #[must_use]
    pub fn freshness(&self) -> FreshnessRepository {
        FreshnessRepository::new(self.storage.clone())
    }
}

#[derive(Debug)]
struct ManagedWorker {
    shutdown_tx: watch::Sender<bool>,
    handle: JoinHandle<anyhow::Result<()>>,
}

impl ManagedWorker {
    fn start(config: PipelineConfig, storage: StorageClient) -> Self {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let handle = tokio::spawn(elephant_pipeline_bin::run_pipeline_with_shutdown(
            config,
            storage,
            shutdown_rx,
        ));
        Self {
            shutdown_tx,
            handle,
        }
    }

    fn is_running(&self) -> bool {
        !self.handle.is_finished()
    }

    async fn shutdown(self) -> ElephantResult<()> {
        let _ = self.shutdown_tx.send(true);
        self.handle.await.map_err(anyhow::Error::from)??;
        Ok(())
    }
}

const fn default_source_type() -> SourceType {
    SourceType::Manual
}

const fn default_ingest_mode() -> IngestMode {
    IngestMode::Graph
}

const fn default_priority() -> i32 {
    5
}

fn empty_object() -> serde_json::Value {
    serde_json::Value::Object(serde_json::Map::new())
}

fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn deterministic_doc_id(space_id: &str, source_id: &RecordId, hash: &str) -> RecordId {
    RecordId::new(
        TableName::Doc,
        uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("{space_id}|{source_id}|text|{hash}").as_bytes(),
        ),
    )
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use super::*;

    #[tokio::test]
    async fn embedded_facade_opens_and_reports_status() {
        let elephant = Elephant::embedded().open().await.expect("open embedded");
        let status = elephant.status().await.expect("status");
        assert_eq!(status.backend, "elephant-db");
        assert!(status.storage_ready);
    }

    #[tokio::test]
    async fn embedded_snapshot_roundtrip_works() {
        let path =
            std::env::temp_dir().join(format!("elephant-facade-{}.json", uuid::Uuid::new_v4()));
        let elephant = Elephant::embedded().open().await.expect("open embedded");
        elephant
            .snapshot_export(&path)
            .await
            .expect("export snapshot");
        elephant
            .snapshot_import(&path)
            .await
            .expect("import snapshot");
        let _ = std::fs::remove_file(path);
    }

    #[tokio::test]
    async fn text_ingest_creates_source_doc_and_work_item() {
        let elephant = Elephant::embedded().open().await.expect("open embedded");
        let receipt = elephant
            .ingest_text(IngestTextRequest {
                space_id: "test-space".to_string(),
                actor: "tester".to_string(),
                title: "Release Notes".to_string(),
                text: "Elephant now has a public facade.".to_string(),
                sidecar_text: None,
                source_type: SourceType::Manual,
                external_id: Some("release-notes".to_string()),
                uri: None,
                author: None,
                ingest_mode: IngestMode::Graph,
                priority: 5,
                source_metadata: empty_object(),
                doc_metadata: empty_object(),
            })
            .await
            .expect("ingest text");

        assert_eq!(receipt.source_id.table(), TableName::Source);
        assert_eq!(receipt.doc_id.table(), TableName::Doc);
        assert_eq!(receipt.work_item_id.table(), TableName::WorkItem);
    }
}
