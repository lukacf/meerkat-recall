//! Public Rust facade for embedding Elephant.

#![allow(missing_docs, clippy::missing_errors_doc)]

use std::path::{Path, PathBuf};

use elephant_pipeline::source_queue::enqueue_accepted_source;
use elephant_pipeline_bin::config::PipelineConfig;
use elephant_storage::source_intake::{IntakeControl, PreparedSourceIntake};
use elephant_storage::{
    deterministic_source_id_for_ingest, AssertionRepository, AttributeRepository,
    ConflictGroupRepository, DocNodeRepository, DocRepository, DocRevisionRepository,
    EntityRepository, EventRepository, FreshnessRepository, IngestDecisionRepository,
    PredicateAliasRepository, PredicateRegistryRepository, RelRepository, SourceRepository,
    SpaceConfigRepository, StorageBackendKind, StorageClient, StorageConfig, StorageError,
    SystemEventRepository, TruthSlotRepository, WorkArtifactRepository, WorkDeadletterRepository,
    WorkEdgeRepository, WorkItemRepository, WorkRunRepository,
};
use elephant_types::error::TypeError;
use elephant_types::publication::OperationReceipt;
use elephant_types::records::base::{BaseFields, Security, SubjectsState};
use elephant_types::records::{IngestMode, ManualDocTemplate, Source, SourceType};
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
            .filter(|_| storage.recovery_status().ready)
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
        let recovery_ready = self.storage.recovery_status().ready;
        let storage_ready = recovery_ready && self.storage.health_check().await.is_ok();
        let worker_running = self.worker.as_ref().is_some_and(ManagedWorker::is_running);
        Ok(ElephantStatus {
            backend: self.storage.backend().as_str().to_string(),
            storage_ready,
            worker_running,
            recovery_ready,
        })
    }

    pub async fn ingest_text(&self, request: IngestTextRequest) -> ElephantResult<IngestReceipt> {
        self.storage
            .with_activity(Box::pin(self.ingest_text_admitted(request)))
            .await?
    }

    #[expect(
        clippy::manual_ok_err,
        clippy::option_if_let_else,
        reason = "Keep the explicit durable-acceptance queue-error branch and original drop order."
    )]
    async fn ingest_text_admitted(
        &self,
        request: IngestTextRequest,
    ) -> ElephantResult<IngestReceipt> {
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
        source.base.security = request.security.clone();
        source.base.subjects = request.subjects.clone();
        source.base.subjects_state = request.subjects_state;
        source.external_id = request.external_id.clone();
        source.uri = request.uri.clone();
        source.author = request.author.clone();
        source.metadata = request.source_metadata.clone();
        source
            .metadata
            .as_object_mut()
            .ok_or_else(|| {
                StorageError::DataIntegrityError("source_metadata must be an object".into())
            })?
            .insert(
                "_intake_priority".into(),
                serde_json::json!(request.priority),
            );
        source.base.id = deterministic_source_id_for_ingest(&source);
        let doc_id = deterministic_doc_id(&request.space_id, &source.base.id, &hash);
        let rendition_key = format!("text:{hash}");
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

        // The facade is a trusted in-process host interface. It preserves the
        // supplied envelope; source acceptance still enforces the same CAS and
        // no-revival contract used by authenticated adapters.
        let prepared = Box::pin(PreparedSourceIntake::prepare(
            &self.storage,
            source,
            request.intake,
        ))
        .await?;
        let accepted = if let Some(receipt) =
            Box::pin(prepared.read_only_contact_retry(&self.storage)).await?
        {
            receipt
        } else {
            let contact = Box::pin(prepared.prepare_replacement_contact(&self.storage)).await?;
            match &contact {
                Some(contact) => {
                    Box::pin(prepared.accept_with_prepared_contact(&self.storage, contact)).await?
                }
                None => Box::pin(prepared.accept(&self.storage)).await?,
            }
        };
        let doc_id = accepted
            .document
            .as_ref()
            .ok_or_else(|| StorageError::DataIntegrityError("text intake has no document".into()))?
            .base
            .id
            .clone();
        let work_item_id = if accepted.needs_processing() {
            match Box::pin(enqueue_accepted_source(
                &self.storage,
                &accepted.source,
                &accepted.publication,
                &request.actor,
            ))
            .await
            {
                Ok(id) => Some(id),
                // The durable operation is the recovery receipt. A queue
                // outage cannot turn accepted input into a failed submission.
                Err(_) => None,
            }
        } else {
            None
        };
        Ok(IngestReceipt {
            source_id: accepted.operation.source_id.clone(),
            doc_id,
            work_item_id,
            operation: accepted.operation,
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

    /// Stop this facade's scheduler, drain real work, and export under the
    /// storage recovery barrier. A drain timeout retains the joining handle.
    pub async fn quiesce_and_export(&mut self, path: impl AsRef<Path>) -> ElephantResult<()> {
        self.drain_worker().await?;
        self.storage.export_snapshot(path.as_ref()).await?;
        Ok(())
    }

    /// Restore only after this facade's workers have drained. Journal replay
    /// and readiness remain owned by `StorageClient`.
    pub async fn quiesce_and_import(&mut self, path: impl AsRef<Path>) -> ElephantResult<()> {
        self.drain_worker().await?;
        self.storage.import_snapshot(path.as_ref()).await?;
        Ok(())
    }

    async fn drain_worker(&mut self) -> ElephantResult<()> {
        if let Some(worker) = self.worker.as_mut() {
            let _ = worker.shutdown_tx.send(true);
            tokio::time::timeout(std::time::Duration::from_secs(60), &mut worker.handle)
                .await
                .map_err(|_| anyhow::anyhow!("worker drain timed out; snapshot was not started"))?
                .map_err(anyhow::Error::from)??;
            self.worker = None;
        }
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
    #[serde(default)]
    pub intake: IntakeControl,
    #[serde(default = "default_security")]
    pub security: Security,
    #[serde(default)]
    pub subjects: Vec<elephant_types::Link<elephant_types::records::Entity>>,
    #[serde(default)]
    pub subjects_state: SubjectsState,
}

fn default_security() -> Security {
    Security {
        policy_id: "default".into(),
        ..Security::default()
    }
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
            intake: IntakeControl::default(),
            security: default_security(),
            subjects: Vec::new(),
            subjects_state: SubjectsState::Unknown,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestReceipt {
    pub source_id: RecordId,
    pub doc_id: RecordId,
    pub work_item_id: Option<RecordId>,
    pub operation: OperationReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElephantStatus {
    pub backend: String,
    pub storage_ready: bool,
    pub worker_running: bool,
    pub recovery_ready: bool,
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
    use elephant_storage::Repository;

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the facade alive through all original status assertions"
    )]
    async fn embedded_facade_opens_and_reports_status() {
        let elephant = Elephant::embedded().open().await.expect("open embedded");
        let status = elephant.status().await.expect("status");
        assert_eq!(status.backend, "elephant-db");
        assert!(status.storage_ready);
    }

    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the facade alive through snapshot import and original file cleanup"
    )]
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
    #[expect(
        clippy::significant_drop_tightening,
        reason = "Keep the facade alive through source intake and all durable receipt assertions"
    )]
    async fn text_ingest_creates_source_doc_and_work_item() {
        let elephant = Elephant::embedded().open().await.expect("open embedded");
        SpaceConfigRepository::new(elephant.storage.clone())
            .create(elephant_types::records::SpaceConfig {
                base: BaseFields::new(
                    RecordId::new_v4(TableName::SpaceConfig),
                    "test-space".into(),
                    "fixture".into(),
                ),
                embedding_provider: "fake".into(),
                embedding_model: "fixture".into(),
                embedding_dims: 8,
                processing: None,
                purpose: None,
                skill: None,
            })
            .await
            .expect("named space");
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
                ..IngestTextRequest::default()
            })
            .await
            .expect("ingest text");

        assert_eq!(receipt.source_id.table(), TableName::Source);
        assert_eq!(receipt.doc_id.table(), TableName::Doc);
        assert_eq!(
            receipt.work_item_id.expect("queued work").table(),
            TableName::WorkItem
        );
    }
    #[tokio::test]
    #[expect(
        clippy::significant_drop_tightening,
        clippy::too_many_lines,
        reason = "Keep the complete contact replacement and original retry oracle with its facade lifetime"
    )]
    async fn text_replacement_preserves_original_contact_class_and_receipt() {
        for sealed in [false, true] {
            let elephant = Elephant::embedded().open().await.expect("open embedded");
            SpaceConfigRepository::new(elephant.storage.clone())
                .create(elephant_types::records::SpaceConfig {
                    base: BaseFields::new(
                        RecordId::new_v4(TableName::SpaceConfig),
                        "contact-facade".into(),
                        "fixture".into(),
                    ),
                    embedding_provider: "fake".into(),
                    embedding_model: "fixture".into(),
                    embedding_dims: 8,
                    processing: None,
                    purpose: None,
                    skill: None,
                })
                .await
                .expect("named space");
            let metadata = serde_json::json!({"contact":{"name":"Facade Person","email":"facade@example.invalid"}});
            let mut source = Source::new(
                BaseFields::new(
                    RecordId::new_v4(TableName::Source),
                    "contact-facade".into(),
                    "fixture".into(),
                ),
                SourceType::Manual,
                "original".into(),
            );
            source.external_id = Some("facade-contact".into());
            source.metadata = metadata.clone();
            source.set_requested_ingest_mode(IngestMode::Metadata);
            let prepared = Box::pin(PreparedSourceIntake::prepare(
                &elephant.storage,
                source,
                IntakeControl::default(),
            ))
            .await
            .expect("prepare original");
            let original = if sealed {
                let contact = Box::pin(prepared.prepare_contact(&elephant.storage))
                    .await
                    .expect("prepare contact")
                    .expect("contact");
                Box::pin(prepared.accept_with_prepared_contact(&elephant.storage, &contact))
                    .await
                    .expect("sealed acceptance")
            } else {
                Box::pin(prepared.accept(&elephant.storage))
                    .await
                    .expect("plain acceptance")
            };
            let request = IngestTextRequest {
                space_id: "contact-facade".into(),
                actor: "host".into(),
                title: "Replacement".into(),
                text: "New immutable input".into(),
                external_id: Some("facade-contact".into()),
                source_metadata: metadata,
                ingest_mode: IngestMode::Metadata,
                intake: IntakeControl {
                    revision_id: Some("facade-r2".into()),
                    idempotency_key: Some("facade-r2".into()),
                    expected_generation: Some(original.publication.generation.clone()),
                },
                ..IngestTextRequest::default()
            };
            let acceptance = elephant.ingest_text(request.clone());
            assert!(
                std::mem::size_of_val(&acceptance) <= 64 * 1024,
                "public intake future must not inline the complete acceptance graph"
            );
            let accepted = Box::pin(acceptance)
                .await
                .expect("replace through public facade");
            assert_eq!(accepted.source_id, original.source.base.id);
            let work = WorkItemRepository::new(elephant.storage.clone())
                .get(
                    "contact-facade",
                    accepted.work_item_id.as_ref().expect("queued work"),
                )
                .await
                .expect("load work")
                .expect("actual work");
            let publication: elephant_types::publication::PublicationFence =
                serde_json::from_value(work.payload["_publication"].clone())
                    .expect("original queued authority");
            assert_eq!(publication.operation_id, accepted.operation.operation_id);
            assert_eq!(
                elephant_storage::SourcePublicationRepository::new(elephant.storage.clone())
                    .accepted_contact_for_publication("contact-facade", &publication)
                    .await
                    .expect("actual admitted capability")
                    .is_some(),
                sealed
            );
            let replay = Box::pin(elephant.ingest_text(request.clone()))
                .await
                .expect("original retry");
            assert_eq!(replay.operation, accepted.operation);
            assert_eq!(replay.doc_id, accepted.doc_id);
            assert_eq!(replay.work_item_id, accepted.work_item_id);
            if sealed {
                let store = elephant.storage.embedded_store().expect("native store");
                store
                    .mutate(|tx| {
                        let rows = tx.scan_extension_rows("source_contact_intent")?;
                        let matching: Vec<_> = rows
                            .into_iter()
                            .filter(|(_, row)| {
                                let payload: serde_json::Value = serde_json::from_str(
                                    row["payload"].as_str().expect("private payload"),
                                )
                                .expect("private intent");
                                payload["publication"]["operation_id"]
                                    == accepted.operation.operation_id
                            })
                            .collect();
                        assert_eq!(matching.len(), 1);
                        tx.hard_delete_extension_row(
                            "source_contact_intent",
                            &matching[0].0,
                            "contact-facade",
                        )
                        .map_err(Into::into)
                    })
                    .await
                    .expect("original plan loss fault");
                let before: serde_json::Value =
                    serde_json::from_slice(&store.export_snapshot_bytes().await.expect("snapshot"))
                        .expect("snapshot JSON");
                let receipt_only = Box::pin(elephant.ingest_text(request))
                    .await
                    .expect("read-only original retry");
                assert_eq!(receipt_only.operation, accepted.operation);
                assert_eq!(receipt_only.doc_id, accepted.doc_id);
                assert!(receipt_only.work_item_id.is_none());
                let after: serde_json::Value =
                    serde_json::from_slice(&store.export_snapshot_bytes().await.expect("snapshot"))
                        .expect("snapshot JSON");
                assert_eq!(before["tables"], after["tables"]);
            }
            assert!(EntityRepository::new(elephant.storage.clone())
                .list("contact-facade", 10, None)
                .await
                .expect("no premature publication")
                .is_empty());
        }
    }
}
