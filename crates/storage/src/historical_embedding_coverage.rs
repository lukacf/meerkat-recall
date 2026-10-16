//!
//! Current retrieval coverage for an imported publication. Import receipt/time
//! remain immutable; only actual current vectors can complete its operation.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use chrono::{DateTime, Utc};
use elephant_embeddings::{level_bucket, PartitionKey};
use elephant_types::provider_binding::{
    ModelOperation, ResolvedSpaceConfig, SpaceConfigToken, VectorPartition,
};
use elephant_types::publication::{
    OperationReceipt, OperationState, PublicationFence, SourceAuthorizationEnvelope, SourceHead,
    SourceVisibility,
};
use elephant_types::records::doc_node::NodeRole;
use elephant_types::records::{DocNode, Entity, NodeEmbedding, SpaceConfig};
use elephant_types::RecordId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use surrealdb::sql::{Thing, Value};

use crate::canonical_commit::{CanonicalRecord, CanonicalRepository};
use crate::historical_adoption::{HistoricalAdoptionReceipt, HistoricalCommitProvenance};
use crate::source_publication::{
    control_thing, encode_payload, head_key, integrity, stable_key, SourcePublicationRepository,
    StoredOperation,
};
use crate::source_publication_manifest::{
    manifest_key, SourcePublicationCompletion, SourcePublicationManifestRepository,
    COMPLETION_TABLE, MANIFEST_TABLE,
};
use crate::{StorageClient, StorageResult};

/// Actual current coverage, separate from the original historical import time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalEmbeddingCoverage {
    /// Immutable source publication and original import receipt.
    pub publication: PublicationFence,
    /// Digest of the immutable historical publication manifest.
    pub manifest_digest: String,
    /// Exact original historical adoption commit ID.
    pub import_commit_id: String,
    /// Exact repository-owned completed reindex, not caller counters.
    pub reindex_id: String,
    /// Content generation of the completed governed reindex.
    pub content_generation: u64,
    /// Processing configuration generation used for current coverage.
    pub config_generation: u64,
    /// Current model and authorization partition whose physical vectors were checked.
    pub partition: VectorPartition,
    /// Complete observed node/entity inputs and actual vector row identities.
    pub node_inputs: BTreeMap<RecordId, String>,
    /// Complete eligible entity identities and input hashes checked for coverage.
    pub entity_inputs: BTreeMap<RecordId, String>,
    /// Digest of the actual current physical vector row inventory.
    pub vector_rows_sha256: String,
    /// Canonical epoch against which the coverage read set was prepared.
    pub observed_epoch: Option<String>,
    /// Actual coverage certification time, separate from historical import time.
    pub completed_at: DateTime<Utc>,
}

/// One source per transaction. Does not call a provider or enqueue extraction.
pub struct HistoricalEmbeddingCoverageRepository {
    client: StorageClient,
}
impl HistoricalEmbeddingCoverageRepository {
    #[must_use]
    /// Bind physical coverage verification to an existing storage client.
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    /// Require the original import plus complete current node/entity coverage.
    /// All dependencies are compared again inside the completion transaction.
    pub async fn certify(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<OperationReceipt> {
        let prepared =
            Box::pin(PreparedCoverage::prepare(&self.client, space, publication)).await?;
        prepared.commit(&self.client).await
    }

    /// Administrative completion bound to an immutable import/catchup population.
    /// Caller declarations never adopt a replacement head, receipt, or activation.
    pub async fn certify_bound(
        &self,
        space: &str,
        original: &crate::client::HistoricalCatchupSource,
        configuration_id: &RecordId,
        reindex_id: &str,
    ) -> StorageResult<OperationReceipt> {
        let publication = PublicationFence {
            source_id: original.source_id.clone(),
            generation: original.head.generation.clone(),
            revision_id: required(
                original.head.published_revision.clone(),
                "coverage bound revision absent",
            )?,
            operation_id: original.head.operation_id.clone(),
        };
        let prepared =
            Box::pin(PreparedCoverage::prepare(&self.client, space, &publication)).await?;
        let head: SourceHead = serde_json::from_str(
            &prepared
                .controls
                .iter()
                .find(|guard| guard.table == "source_head")
                .ok_or_else(|| integrity("coverage bound head absent"))?
                .payload,
        )
        .map_err(|e| integrity(&e.to_string()))?;
        let receipt: crate::canonical_commit::CanonicalCommitReceipt = serde_json::from_str(
            &prepared
                .controls
                .iter()
                .find(|guard| guard.table == "source_commit_receipt")
                .ok_or_else(|| integrity("coverage bound receipt absent"))?
                .payload,
        )
        .map_err(|e| integrity(&e.to_string()))?;
        let certificate = required(
            prepared.completion.historical_embedding_coverage.as_ref(),
            "coverage certificate missing",
        )?;
        if original.head.space_id != space
            || original.source_id != original.head.source_id
            || !same(&original.head, &head)?
            || receipt.commit_id != original.commit_id
            || digest(&serde_json::to_vec(&receipt).map_err(|e| integrity(&e.to_string()))?)
                != original.receipt_sha256
            || prepared.config.base.id != *configuration_id
            || certificate.reindex_id != reindex_id
        {
            return Err(integrity(
                "coverage original population or activation binding differs",
            ));
        }
        prepared.commit(&self.client).await
    }
}

struct ControlGuard {
    table: &'static str,
    key: String,
    payload: String,
    embedded: Option<serde_json::Value>,
}
struct NativeGuard {
    id: Thing,
    row: Value,
}
struct NodeCoverage {
    node: DocNode,
    embedding_id: RecordId,
    table: String,
    native_id: Thing,
    embedded: Option<NodeEmbedding>,
}
struct EntityCoverage {
    entity: Entity,
    key: String,
    native_id: Thing,
    embedded: Option<serde_json::Value>,
}
struct PreparedCoverage {
    space: String,
    publication: PublicationFence,
    epoch: Option<String>,
    config: SpaceConfig,
    model: String,
    partition: VectorPartition,
    controls: Vec<ControlGuard>,
    canonical: Vec<CanonicalRecord>,
    native: Vec<NativeGuard>,
    native_bytes: usize,
    replay: bool,
    nodes: Vec<NodeCoverage>,
    entities: Vec<EntityCoverage>,
    completion: SourcePublicationCompletion,
    operation: StoredOperation,
}
fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn same<T: Serialize>(a: &T, b: &T) -> StorageResult<bool> {
    Ok(encode_payload(a)? == encode_payload(b)?)
}
fn required<T>(value: Option<T>, label: &str) -> StorageResult<T> {
    value.ok_or_else(|| integrity(label))
}
fn vector_valid(vector: &[f32], dims: usize) -> bool {
    vector.len() == dims && vector.iter().all(|v| v.is_finite()) && vector.iter().any(|v| *v != 0.0)
}

async fn native_row(client: &StorageClient, id: &Thing) -> StorageResult<Value> {
    let mut response = client
        .query_bind_checked(
            "SELECT * FROM ONLY $id",
            crate::bindings! {"id"=>id.clone()},
        )
        .await?;
    let row: surrealdb::Value = response.take(0).map_err(|e| integrity(&e.to_string()))?;
    match row.into_inner() {
        row @ Value::Object(_) => Ok(row),
        _ => Err(integrity("coverage native dependency missing")),
    }
}
// Match the exact typed fields using the established native equality contract;
// keep the complete observed row (including unknown fields) for the later CAS.
fn validate_native_typed_image(id: &Thing, actual: &Value, typed: Value) -> StorageResult<()> {
    let Value::Object(actual) = actual else {
        return Err(integrity("coverage native typed image missing"));
    };
    let Value::Object(expected) = crate::canonical_commit::native_expected_fields(typed) else {
        return Err(integrity("coverage typed image is not a record"));
    };
    if actual.get("id") != Some(&Value::Thing(id.clone()))
        || !expected
            .iter()
            .all(|(key, value)| actual.get(key).unwrap_or(&Value::None).equal(value))
    {
        return Err(integrity("coverage typed/native read pair changed"));
    }
    Ok(())
}
impl PreparedCoverage {
    fn capture_native(&mut self, id: Thing, row: Value) -> StorageResult<()> {
        self.native_bytes = self
            .native_bytes
            .checked_add(row.to_string().len())
            .ok_or_else(|| integrity("coverage native bytes overflow"))?;
        if self.native_bytes > 128 * 1024 * 1024 || self.native.len() >= 100_000 {
            return Err(integrity("coverage native dependency bound exceeded"));
        }
        self.native.push(NativeGuard { id, row });
        Ok(())
    }
    async fn control<T: serde::de::DeserializeOwned>(
        &mut self,
        repo: &SourcePublicationRepository,
        client: &StorageClient,
        table: &'static str,
        key: String,
    ) -> StorageResult<T> {
        // Keep exact stored payload bytes for CAS, not a reserialized projection.
        let payload = required(
            repo.read_control_payload(table, &key, &self.space).await?,
            "coverage control missing",
        )?;
        let value = serde_json::from_str(&payload).map_err(|e| integrity(&e.to_string()))?;
        let embedded = if let Some(store) = client.embedded_store() {
            let row = required(
                store
                    .extension_row_by_key(table, &format!("{table}:{key}"))
                    .await?,
                "coverage control row missing",
            )?;
            if row.get("space_id").and_then(serde_json::Value::as_str) != Some(self.space.as_str())
                || row.get("payload").and_then(serde_json::Value::as_str) != Some(payload.as_str())
            {
                return Err(integrity("coverage control wrapper changed"));
            }
            Some(row)
        } else {
            let id = control_thing(table, &key);
            let row = native_row(client, &id).await?;
            let Value::Object(fields) = &row else {
                return Err(integrity("coverage control wrapper missing"));
            };
            if fields.get("space_id") != Some(&Value::from(self.space.clone()))
                || fields.get("payload") != Some(&Value::from(payload.clone()))
            {
                return Err(integrity("coverage control wrapper changed"));
            }
            self.capture_native(id, row)?;
            None
        };
        self.controls.push(ControlGuard {
            table,
            key,
            payload,
            embedded,
        });
        Ok(value)
    }
    async fn canonical(
        &mut self,
        client: &StorageClient,
        id: &RecordId,
    ) -> StorageResult<CanonicalRecord> {
        let row = required(
            CanonicalRecord::load(client, &self.space, id).await?,
            "coverage canonical input missing",
        )?;
        if row.base().id != *id
            || row.base().deleted_at.is_some()
            || row.base().space_id != self.space
        {
            return Err(integrity("coverage canonical input not live"));
        }
        if client.embedded_store().is_none() {
            let id = crate::surreal::thing_from_record_id(id);
            let actual = native_row(client, &id).await?;
            validate_native_typed_image(&id, &actual, row.native()?)?;
            self.capture_native(id, actual)?;
        }
        self.canonical.push(row.clone());
        Ok(row)
    }
    async fn node_vector(
        &mut self,
        client: &StorageClient,
        node: DocNode,
        vector_hashes: &mut BTreeMap<String, String>,
    ) -> StorageResult<()> {
        if let Some(existing) = self
            .nodes
            .iter()
            .find(|existing| existing.node.base.id == node.base.id)
        {
            if !same(&existing.node, &node)? {
                return Err(integrity("coverage duplicate node input differs"));
            }
            // One source can reach the same immutable node through document
            // membership and snapshot evidence. Retain the first full vector
            // guard and submit each physical identity to HNSW exactly once.
            return Ok(());
        }
        let space = self.space.as_str();
        let table = PartitionKey::new(
            node.base.security.policy_id.clone(),
            level_bucket(node.base.security.level),
            self.model.clone(),
            self.partition.dimensions,
        )
        .map_err(|e| integrity(&e.to_string()))?
        .table_name();
        let native_id = Thing::from((
            table.clone(),
            surrealdb::sql::Id::Uuid(node.base.id.id().into()),
        ));
        let embedding_id = crate::chunk_embedding_repository::embedding_record_id_for(
            &node.base.id,
            space,
            &node.base.security,
            &self.model,
        );
        let embedded = if let Some(store) = client.embedded_store() {
            let row = required(
                store
                    .get::<NodeEmbedding>(space, &embedding_id, false)
                    .await?,
                "coverage node vector missing",
            )?;
            validate_node_vector(&row, &node, &self.model, self.partition.dimensions)?;
            vector_hashes.insert(
                embedding_id.to_string(),
                digest(encode_payload(&row)?.as_bytes()),
            );
            Some(row)
        } else {
            let row = native_row(client, &native_id).await?;
            vector_hashes.insert(native_id.to_string(), digest(row.to_string().as_bytes()));
            self.capture_native(native_id.clone(), row)?;
            None
        };
        self.nodes.push(NodeCoverage {
            node,
            embedding_id,
            table,
            native_id,
            embedded,
        });
        Ok(())
    }
    fn new(
        space: &str,
        publication: &PublicationFence,
        epoch: Option<String>,
        config: SpaceConfig,
        vector_identity: (String, VectorPartition),
        publication_state: (SourcePublicationCompletion, StoredOperation),
    ) -> Self {
        let (model, partition) = vector_identity;
        let (completion, operation) = publication_state;
        Self {
            space: space.into(),
            publication: publication.clone(),
            epoch,
            config,
            model,
            partition,
            controls: vec![],
            canonical: vec![],
            native: vec![],
            native_bytes: 0,
            replay: false,
            nodes: vec![],
            entities: vec![],
            completion,
            operation,
        }
    }
    async fn capture_publication(
        &mut self,
        client: &StorageClient,
        repo: &SourcePublicationRepository,
        evidence: PublicationEvidence<'_>,
    ) -> StorageResult<(SourcePublicationCompletion, StoredOperation)> {
        let PublicationEvidence {
            space,
            publication,
            manifest,
            proof,
        } = evidence;
        let head: SourceHead = self
            .control(
                repo,
                client,
                "source_head",
                head_key(space, &publication.source_id),
            )
            .await?;
        if head.lifecycle != SourceVisibility::Active
            || head.generation != publication.generation
            || head.operation_id != publication.operation_id
            || head.desired_revision.as_deref() != Some(&publication.revision_id)
            || head.published_revision != head.desired_revision
        {
            return Err(integrity("coverage publication not current"));
        }
        let stored_manifest: crate::source_publication_manifest::SourcePublicationManifest = self
            .control(
                repo,
                client,
                MANIFEST_TABLE,
                manifest_key(space, publication),
            )
            .await?;
        if !same(manifest, &stored_manifest)? {
            return Err(integrity("coverage manifest changed"));
        }
        let stored_completion: SourcePublicationCompletion = self
            .control(
                repo,
                client,
                COMPLETION_TABLE,
                manifest_key(space, publication),
            )
            .await?;
        let lookup = stable_key(&[
            "published-source-completion/v1",
            space,
            &publication.source_id.to_string(),
        ]);
        let current_completion: SourcePublicationCompletion =
            self.control(repo, client, COMPLETION_TABLE, lookup).await?;
        if !same(&self.completion, &stored_completion)?
            || !same(&stored_completion, &current_completion)?
            || stored_completion.manifest_digest != manifest.digest()?
            || stored_completion.commit_id != proof.commit_id(manifest)
        {
            return Err(integrity("coverage original completion differs"));
        }
        let stored_operation: StoredOperation = self
            .control(
                repo,
                client,
                "source_operation",
                publication.operation_id.clone(),
            )
            .await?;
        if !same(&self.operation, &stored_operation)?
            || stored_operation.receipt.source_id != publication.source_id
            || stored_operation.receipt.source_generation != publication.generation
            || stored_operation.receipt.desired_revision.as_deref()
                != Some(&publication.revision_id)
            || !matches!(
                stored_operation.receipt.state,
                OperationState::Processing | OperationState::Completed
            )
        {
            return Err(integrity("coverage original operation differs"));
        }
        Ok((stored_completion, stored_operation))
    }
    async fn capture_import_authority(
        &mut self,
        client: &StorageClient,
        repo: &SourcePublicationRepository,
        canonical: &CanonicalRepository,
        evidence: PublicationEvidence<'_>,
        stored_completion: &SourcePublicationCompletion,
        stored_operation: &StoredOperation,
    ) -> StorageResult<()> {
        let PublicationEvidence {
            space,
            publication,
            manifest,
            ..
        } = evidence;
        let receipt = required(
            canonical
                .receipt_read_only(space, &stored_completion.commit_id)
                .await?,
            "coverage original import receipt missing",
        )?;
        if receipt.space_id != space
            || receipt.commit_id != stored_completion.commit_id
            || receipt.source_id.as_ref() != Some(&publication.source_id)
            || receipt.source_revision.as_deref() != Some(publication.revision_id.as_str())
            || receipt.historical_import.as_ref()
                != Some(&HistoricalCommitProvenance::for_manifest(manifest)?)
            || receipt.plan_fingerprint != manifest.digest()?
            || receipt.publication_authority.as_ref() != Some(publication)
        {
            return Err(integrity("coverage original import authority differs"));
        }
        let original: HistoricalAdoptionReceipt = serde_json::from_value(required(
            receipt.replay_output.clone(),
            "coverage import output absent",
        )?)
        .map_err(|e| integrity(&e.to_string()))?;
        if original.manifest_digest != manifest.digest()?
            || original.commit_id != stored_completion.commit_id
        {
            return Err(integrity("coverage original import output differs"));
        }
        let captured_receipt: crate::canonical_commit::CanonicalCommitReceipt = self
            .control(
                repo,
                client,
                "source_commit_receipt",
                stable_key(&[space, &stored_completion.commit_id]),
            )
            .await?;
        if !same(&receipt, &captured_receipt)? {
            return Err(integrity("coverage import receipt read pair changed"));
        }
        let source = self.canonical(client, &publication.source_id).await?;
        let CanonicalRecord::Source(source) = source else {
            return Err(integrity("coverage source type"));
        };
        if SourceAuthorizationEnvelope::from(source.as_ref()) != stored_operation.authorization {
            return Err(integrity("coverage source authorization changed"));
        }
        Ok(())
    }
    async fn capture_configuration_and_revision(
        &mut self,
        client: &StorageClient,
        repo: &SourcePublicationRepository,
        evidence: PublicationEvidence<'_>,
        stored_operation: &StoredOperation,
    ) -> StorageResult<elephant_types::publication::SourceRevision> {
        let PublicationEvidence {
            space, publication, ..
        } = evidence;
        if client.embedded_store().is_none() {
            let id = crate::surreal::thing_from_record_id(&self.config.base.id);
            let actual = native_row(client, &id).await?;
            validate_native_typed_image(
                &id,
                &actual,
                crate::surreal::space_config_to_value(&self.config)?,
            )?;
            self.capture_native(id, actual)?;
        }
        let accepted = required(
            repo.revision(space, &publication.source_id, &publication.revision_id)
                .await?,
            "coverage accepted revision missing",
        )?;
        let captured_revision: crate::source_publication::StoredRevision = self
            .control(
                repo,
                client,
                "source_revision",
                crate::source_publication::revision_key(
                    space,
                    &publication.source_id,
                    &publication.revision_id,
                ),
            )
            .await?;
        if !same(&accepted, &captured_revision.revision)?
            || accepted.space_id != space
            || accepted.source_id != publication.source_id
            || accepted.revision_id != publication.revision_id
            || accepted.source.base.id != publication.source_id
            || accepted.source.base.space_id != space
            || captured_revision.operation_id != publication.operation_id
            || captured_revision.input_fingerprint != stored_operation.input_fingerprint
        {
            return Err(integrity("coverage accepted revision read pair changed"));
        }
        Ok(accepted)
    }
    async fn capture_document_coverage(
        &mut self,
        client: &StorageClient,
        evidence: PublicationEvidence<'_>,
        accepted: &elephant_types::publication::SourceRevision,
        inputs: &mut CoverageInputs,
    ) -> StorageResult<()> {
        let PublicationEvidence {
            space,
            manifest,
            proof,
            ..
        } = evidence;
        for mapping in &manifest.documents {
            let CanonicalRecord::Doc(doc) = self.canonical(client, &mapping.doc_id).await? else {
                return Err(integrity("coverage document type"));
            };
            let CanonicalRecord::DocRevision(revision) =
                self.canonical(client, &mapping.doc_revision_id).await?
            else {
                return Err(integrity("coverage revision type"));
            };
            if !proof.document_matches(mapping, &doc, &revision, accepted)? {
                return Err(integrity("coverage original document changed"));
            }
            let nodes = coverage_node_ids(client, space, &mapping.doc_revision_id).await?;
            let expected = required(
                proof.documents.get(&mapping.doc_id),
                "coverage document proof absent",
            )?;
            if nodes.iter().cloned().collect::<BTreeSet<_>>()
                != expected.node_fingerprints.keys().cloned().collect()
            {
                return Err(integrity("coverage node population changed"));
            }
            if self.canonical.len().saturating_add(nodes.len()) > 100_000 {
                return Err(integrity("coverage source dependency bound exceeded"));
            }
            for id in nodes {
                let CanonicalRecord::DocNode(node) = self.canonical(client, &id).await? else {
                    return Err(integrity("coverage node record type"));
                };
                let node = *node;
                if !proof.node_matches(mapping, &node)? {
                    return Err(integrity("coverage original node changed"));
                }
                if !matches!(
                    node.role,
                    NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
                ) {
                    continue;
                }
                let text = coverage_node_text(&node, &revision.text)?;
                inputs.node_inputs.insert(
                    node.base.id.clone(),
                    digest(
                        encode_payload(&(
                            crate::historical_adoption::node_input_fingerprint(&node)?,
                            digest(text.as_bytes()),
                        ))?
                        .as_bytes(),
                    ),
                );
                self.node_vector(client, node, &mut inputs.vector_hashes)
                    .await?;
            }
        }
        Ok(())
    }
    async fn capture_graph_coverage(
        &mut self,
        client: &StorageClient,
        proof: &crate::historical_adoption::HistoricalAdoptionProof,
        inputs: &mut CoverageInputs,
    ) -> StorageResult<()> {
        let mut closure = proof.graph_fingerprints.clone();
        if let Some(snapshot) = &proof.snapshot_graph {
            for (id, fingerprint) in &snapshot.reference_fingerprints {
                if closure
                    .insert(id.clone(), fingerprint.clone())
                    .is_some_and(|before| before != *fingerprint)
                {
                    return Err(integrity("coverage snapshot root/reference proof differs"));
                }
            }
        }
        if self.canonical.len().saturating_add(closure.len()) > 100_000 {
            return Err(integrity("coverage full reference closure exceeds bound"));
        }
        for (id, hash) in &closure {
            let row = self.canonical(client, id).await?;
            if row.fingerprint()? != *hash {
                return Err(integrity("coverage original graph changed"));
            }
            if let CanonicalRecord::DocNode(node) = &row {
                self.capture_snapshot_node(client, node, &closure, inputs)
                    .await?;
            }
            if let CanonicalRecord::Entity(entity) = row {
                self.capture_entity_coverage(client, id, *entity, inputs)
                    .await?;
            }
        }
        Ok(())
    }
    async fn capture_snapshot_node(
        &mut self,
        client: &StorageClient,
        node: &DocNode,
        closure: &BTreeMap<RecordId, String>,
        inputs: &mut CoverageInputs,
    ) -> StorageResult<()> {
        // Snapshot references are original physical dependencies, not a
        // manufactured document mapping. Require the same reindex input
        // and vector for every eligible referenced node.
        if matches!(
            node.role,
            NodeRole::Chunk | NodeRole::Paragraph | NodeRole::Summary
        ) {
            let revision_id = node.revision.id();
            let expected = required(
                closure.get(revision_id),
                "coverage snapshot revision proof absent",
            )?;
            let revision = self.canonical(client, revision_id).await?;
            if revision.fingerprint()? != *expected {
                return Err(integrity("coverage snapshot revision changed"));
            }
            let CanonicalRecord::DocRevision(revision) = revision else {
                return Err(integrity("coverage snapshot revision type"));
            };
            let text = coverage_node_text(node, &revision.text)?;
            inputs.node_inputs.insert(
                node.base.id.clone(),
                digest(
                    encode_payload(&(
                        crate::historical_adoption::node_input_fingerprint(node)?,
                        digest(text.as_bytes()),
                    ))?
                    .as_bytes(),
                ),
            );
            self.node_vector(client, node.clone(), &mut inputs.vector_hashes)
                .await?;
        }
        Ok(())
    }
    async fn capture_entity_coverage(
        &mut self,
        client: &StorageClient,
        id: &RecordId,
        entity: Entity,
        inputs: &mut CoverageInputs,
    ) -> StorageResult<()> {
        // Match the actual reindex provider-input eligibility. Excluded
        // rows remain in the complete original fingerprint/CAS above.
        if entity.status == elephant_types::records::entity::EntityStatus::Merged
            || crate::entity_repository::is_source_inventory_entity(&entity)
        {
            return Ok(());
        }
        let text_hash = crate::entity_profile_embedding_repository::entity_input_hash(&entity);
        inputs.entity_inputs.insert(id.clone(), text_hash.clone());
        let uuid = uuid::Uuid::new_v5(&id.id(), self.model.as_bytes());
        let key = format!("entity_profile_embedding:{uuid}");
        let native_id = Thing::from((
            "entity_profile_embedding",
            surrealdb::sql::Id::Uuid(uuid.into()),
        ));
        let embedded = if let Some(store) = client.embedded_store() {
            let row = required(
                store
                    .extension_row_by_key("entity_profile_embedding", &key)
                    .await?,
                "coverage entity vector missing",
            )?;
            validate_entity_vector(&row, &entity, &self.model, self.partition.dimensions)?;
            inputs
                .vector_hashes
                .insert(key.clone(), digest(encode_payload(&row)?.as_bytes()));
            Some(row)
        } else {
            let row = native_row(client, &native_id).await?;
            inputs
                .vector_hashes
                .insert(key.clone(), digest(row.to_string().as_bytes()));
            self.capture_native(native_id.clone(), row)?;
            None
        };
        self.entities.push(EntityCoverage {
            entity,
            key,
            native_id,
            embedded,
        });
        Ok(())
    }
    fn certify_inventory(
        &mut self,
        evidence: PublicationEvidence<'_>,
        import_commit_id: String,
        processing: &elephant_types::provider_binding::SpaceProcessingConfig,
        activated: &elephant_types::provider_binding::CompletedEmbeddingReindex,
        inputs: CoverageInputs,
    ) -> StorageResult<()> {
        let mut certificate = HistoricalEmbeddingCoverage {
            publication: evidence.publication.clone(),
            manifest_digest: evidence.manifest.digest()?,
            import_commit_id,
            reindex_id: activated.reindex_id.clone(),
            content_generation: activated.content_generation,
            config_generation: processing.generation,
            partition: self.partition.clone(),
            node_inputs: inputs.node_inputs,
            entity_inputs: inputs.entity_inputs,
            vector_rows_sha256: digest(encode_payload(&inputs.vector_hashes)?.as_bytes()),
            observed_epoch: self.epoch.clone(),
            completed_at: Utc::now(),
        };
        if let Some(existing) = &self.completion.historical_embedding_coverage {
            certificate.completed_at = existing.completed_at;
            certificate
                .observed_epoch
                .clone_from(&existing.observed_epoch);
            // This operation can be completed only once, against the same evidence.
            // A superseding profile/publication requires its own explicit lifecycle.
            if existing != &certificate
                || self.operation.receipt.state != OperationState::Completed
                || self.operation.receipt.completed_at != Some(existing.completed_at)
                || self.operation.receipt.error_code.is_some()
            {
                return Err(integrity("coverage existing certificate differs"));
            }
            self.replay = true;
        } else if self.operation.receipt.state == OperationState::Completed {
            return Err(integrity(
                "historical completion lacks coverage certificate",
            ));
        }
        self.completion.historical_embedding_coverage = Some(certificate.clone());
        self.operation.receipt.state = OperationState::Completed;
        self.operation.receipt.completed_at = Some(certificate.completed_at);
        self.operation.receipt.error_code = None;
        Ok(())
    }
    async fn validate_completed_read(
        &self,
        canonical: &CanonicalRepository,
        space: &str,
    ) -> StorageResult<()> {
        if canonical.observe_epoch(space).await? != self.epoch {
            return Err(integrity("coverage dependencies changed during read"));
        }
        if self
            .native
            .iter()
            .map(|r| r.row.to_string().len())
            .sum::<usize>()
            > 128 * 1024 * 1024
        {
            return Err(integrity(
                "coverage source native evidence exceeds transaction bound",
            ));
        }
        Ok(())
    }
    async fn prepare(
        client: &StorageClient,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<Self> {
        let canonical = CanonicalRepository::new(client.clone());
        let epoch = canonical.observe_epoch(space).await?;
        let config = required(
            crate::SpaceConfigRepository::new(client.clone())
                .get_by_space_id(space)
                .await?,
            "coverage config missing",
        )?;
        let (processing, activated) = activated_coverage_configuration(&config, space)?;
        let partition = activated.partition.clone();
        let model = format!("recall:{}", encode_payload(&partition)?);
        let repo = SourcePublicationRepository::new(client.clone());
        let manifests = SourcePublicationManifestRepository::new(client.clone());
        let manifest = required(
            manifests.get(space, publication).await?,
            "coverage manifest missing",
        )?;
        manifest.validate()?;
        let proof = required(
            manifest.historical_adoption.as_ref(),
            "coverage requires historical import",
        )?;
        let completion = required(
            manifests.completion(space, publication).await?,
            "coverage original completion missing",
        )?;
        drop(manifests);
        let operation: StoredOperation = required(
            repo.read_control("source_operation", &publication.operation_id, space)
                .await?,
            "coverage operation missing",
        )?;
        let mut prepared = Self::new(
            space,
            publication,
            epoch,
            config.clone(),
            (model, partition),
            (completion, operation),
        );
        let evidence = PublicationEvidence {
            space,
            publication,
            manifest: &manifest,
            proof,
        };
        let (stored_completion, stored_operation) = prepared
            .capture_publication(client, &repo, evidence)
            .await?;
        prepared
            .capture_import_authority(
                client,
                &repo,
                &canonical,
                evidence,
                &stored_completion,
                &stored_operation,
            )
            .await?;
        let accepted = prepared
            .capture_configuration_and_revision(client, &repo, evidence, &stored_operation)
            .await?;
        drop(repo);
        if manifest.documents.len() > crate::historical_adoption::MAX_HISTORICAL_GROUP_DOCUMENTS
            || proof.graph_fingerprints.len() > 20_000
        {
            return Err(integrity("coverage source group exceeds explicit bound"));
        }
        let mut inputs = CoverageInputs::default();
        prepared
            .capture_document_coverage(client, evidence, &accepted, &mut inputs)
            .await?;
        prepared
            .capture_graph_coverage(client, proof, &mut inputs)
            .await?;
        prepared.validate_completed_read(&canonical, space).await?;
        drop(canonical);
        prepared.certify_inventory(
            evidence,
            stored_completion.commit_id,
            processing,
            activated,
            inputs,
        )?;
        Ok(prepared)
    }
}

#[derive(Clone, Copy)]
struct PublicationEvidence<'a> {
    space: &'a str,
    publication: &'a PublicationFence,
    manifest: &'a crate::source_publication_manifest::SourcePublicationManifest,
    proof: &'a crate::historical_adoption::HistoricalAdoptionProof,
}

#[derive(Default)]
struct CoverageInputs {
    node_inputs: BTreeMap<RecordId, String>,
    entity_inputs: BTreeMap<RecordId, String>,
    vector_hashes: BTreeMap<String, String>,
}

fn activated_coverage_configuration<'a>(
    config: &'a SpaceConfig,
    space: &str,
) -> StorageResult<(
    &'a elephant_types::provider_binding::SpaceProcessingConfig,
    &'a elephant_types::provider_binding::CompletedEmbeddingReindex,
)> {
    let processing = required(config.processing.as_ref(), "coverage processing missing")?;
    let activated = required(
        processing.completed_embedding_reindex.as_ref(),
        "historical coverage requires completed reindex activation",
    )?;
    if processing.pending_reindex.is_some()
        || activated.reindex_id.is_empty()
        || activated.content_generation != processing.index_generation
        || activated.activated_config_generation != processing.generation
        || activated.activated_at > Utc::now()
    {
        return Err(integrity("coverage activation superseded or incomplete"));
    }
    let resolved = ResolvedSpaceConfig {
        token: SpaceConfigToken {
            space_id: space.into(),
            config_id: config.base.id.to_string(),
            generation: processing.generation,
            policy_id: processing.policy_id.clone(),
        },
        processing: processing.clone(),
        purpose: config.purpose.clone(),
        skill: config.skill.clone(),
    };
    for operation in [
        ModelOperation::DocumentEmbedding,
        ModelOperation::QueryEmbedding,
    ] {
        let current = resolved
            .authorize(space, operation, Some(&activated.partition.profile_id))
            .map_err(|e| integrity(&e.to_string()))?
            .vector_partition()
            .map_err(|e| integrity(&e.to_string()))?;
        if current != activated.partition {
            return Err(integrity("coverage full profile changed"));
        }
    }
    let reindex_binding = required(
        processing.bindings.get(&ModelOperation::ReindexEmbedding),
        "coverage reindex approval missing",
    )?;
    if reindex_binding.revoked || reindex_binding.profile_id != activated.partition.profile_id {
        return Err(integrity("coverage reindex approval revoked or changed"));
    }
    if processing.active_embedding_profile_id.as_deref()
        != Some(activated.partition.profile_id.as_str())
    {
        return Err(integrity("coverage target not active"));
    }
    Ok((processing, activated))
}

const COVERAGE_NODE_BOUND: usize = 100_000;
const COVERAGE_NODE_LOOKUP: &str = "SELECT id,space_id,revision FROM doc_node WITH INDEX historical_node_revision_lookup WHERE revision=$revision AND deleted_at IS NONE LIMIT 100001";
#[derive(Deserialize)]
struct CoverageNodeIdentity {
    id: Thing,
    space_id: String,
    revision: Thing,
}
async fn coverage_node_ids(
    client: &StorageClient,
    space: &str,
    revision: &RecordId,
) -> StorageResult<Vec<RecordId>> {
    let mut ids = BTreeSet::new();
    if let Some(store) = client.embedded_store() {
        // Inspect every space bucket without cloning node text or scanning rows.
        for (row_space, id) in store
            .historical_node_ids_for_revision(revision, COVERAGE_NODE_BOUND)
            .await?
        {
            if row_space != space
                || id.table() != elephant_types::TableName::DocNode
                || !ids.insert(id)
            {
                return Err(integrity("coverage indexed node lineage differs"));
            }
        }
    } else {
        let expected = crate::surreal::thing_from_record_id(revision);
        let mut response = client
            .query_bind_checked(
                COVERAGE_NODE_LOOKUP,
                crate::bindings! {"revision"=>expected.clone()},
            )
            .await?;
        let rows: Vec<CoverageNodeIdentity> =
            response.take(0).map_err(|e| integrity(&e.to_string()))?;
        if rows.len() > COVERAGE_NODE_BOUND {
            return Err(integrity("coverage node population exceeds bound"));
        }
        for row in rows {
            let id = crate::surreal::record_id_from_thing(&row.id)
                .map_err(|e| integrity(&e.to_string()))?;
            if row.space_id != space
                || row.revision != expected
                || id.table() != elephant_types::TableName::DocNode
                || !ids.insert(id)
            {
                return Err(integrity("coverage native node lineage differs"));
            }
        }
    }
    Ok(ids.into_iter().collect())
}

fn coverage_node_text<'a>(node: &'a DocNode, revision: &'a str) -> StorageResult<&'a str> {
    if node.role == NodeRole::Summary {
        return node
            .text
            .as_deref()
            .filter(|text| !text.trim().is_empty())
            .ok_or_else(|| integrity("coverage summary text absent"));
    }
    let start = usize::try_from(node.span.start).map_err(|_| integrity("coverage span"))?;
    let end = usize::try_from(node.span.end).map_err(|_| integrity("coverage span"))?;
    if start >= end {
        return Err(integrity("coverage empty node span"));
    }
    revision
        .get(start..end)
        .ok_or_else(|| integrity("coverage node span is not exact UTF-8"))
}

fn validate_node_vector(
    row: &NodeEmbedding,
    node: &DocNode,
    model: &str,
    dims: usize,
) -> StorageResult<()> {
    if row.base.deleted_at.is_some()
        || row.base.space_id != node.base.space_id
        || row.node.id() != &node.base.id
        || row.model != model
        || usize::try_from(row.dims).ok() != Some(dims)
        || row.base.security != node.base.security
        || row.base.subjects != node.base.subjects
        || row.base.subjects_state != node.base.subjects_state
        || !vector_valid(&row.vector, dims)
    {
        return Err(integrity("coverage node vector missing or stale"));
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityVector {
    id: String,
    space_id: String,
    entity_id: String,
    model: String,
    text_hash: String,
    vector: Vec<f32>,
    source_refs: Vec<String>,
    entity_version: DateTime<Utc>,
}
fn validate_entity_vector(
    raw: &serde_json::Value,
    entity: &Entity,
    model: &str,
    dims: usize,
) -> StorageResult<()> {
    let row: EntityVector =
        serde_json::from_value(raw.clone()).map_err(|e| integrity(&e.to_string()))?;
    let expected = format!(
        "entity_profile_embedding:{}",
        uuid::Uuid::new_v5(&entity.base.id.id(), model.as_bytes())
    );
    if row.id != expected
        || row.space_id != entity.base.space_id
        || row.entity_id != entity.base.id.to_string()
        || row.model != model
        || row.text_hash != crate::entity_profile_embedding_repository::entity_input_hash(entity)
        || row.entity_version != entity.base.updated_at
        || row.source_refs
            != entity
                .base
                .provenance
                .sources
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        || !vector_valid(&row.vector, dims)
    {
        return Err(integrity("coverage entity vector missing or stale"));
    }
    Ok(())
}
type CoverageWrite = (&'static str, String, String);

impl PreparedCoverage {
    fn completion_writes(&self) -> StorageResult<Vec<CoverageWrite>> {
        let completion_key = manifest_key(&self.space, &self.publication);
        let lookup_key = stable_key(&[
            "published-source-completion/v1",
            &self.space,
            &self.publication.source_id.to_string(),
        ]);
        let mut writes = Vec::new();
        for (table, key) in [
            (COMPLETION_TABLE, completion_key),
            (COMPLETION_TABLE, lookup_key),
            ("source_operation", self.publication.operation_id.clone()),
        ] {
            let guard = self
                .controls
                .iter()
                .find(|guard| guard.table == table && guard.key == key)
                .ok_or_else(|| integrity("coverage write guard missing"))?;
            let mut payload: serde_json::Value =
                serde_json::from_str(&guard.payload).map_err(|e| integrity(&e.to_string()))?;
            if table == COMPLETION_TABLE {
                let mut retained: BTreeMap<String, Box<serde_json::value::RawValue>> =
                    serde_json::from_str(&guard.payload).map_err(|e| integrity(&e.to_string()))?;
                retained.insert(
                    "historical_embedding_coverage".into(),
                    serde_json::value::to_raw_value(&self.completion.historical_embedding_coverage)
                        .map_err(|e| integrity(&e.to_string()))?,
                );
                writes.push((table, key, encode_payload(&retained)?));
                continue;
            }
            let receipt = payload
                .get_mut("receipt")
                .and_then(serde_json::Value::as_object_mut)
                .ok_or_else(|| integrity("coverage operation receipt malformed"))?;
            receipt.insert(
                "state".into(),
                serde_json::to_value(self.operation.receipt.state)
                    .map_err(|e| integrity(&e.to_string()))?,
            );
            receipt.insert(
                "completed_at".into(),
                serde_json::to_value(self.operation.receipt.completed_at)
                    .map_err(|e| integrity(&e.to_string()))?,
            );
            receipt.insert("error_code".into(), serde_json::Value::Null);
            writes.push((table, key, encode_payload(&payload)?));
        }
        Ok(writes)
    }
    fn validate_embedded_inputs(
        &self,
        tx: &crate::embedded::EmbeddedMutation<'_>,
    ) -> StorageResult<()> {
        if crate::canonical_commit::epoch_tx(tx, &self.space)? != self.epoch {
            return Err(integrity("coverage epoch changed"));
        }
        let config = required(
            tx.get::<SpaceConfig>(&self.space, &self.config.base.id, false)?,
            "coverage config removed",
        )?;
        if !same(&config, &self.config)? {
            return Err(integrity("coverage config changed"));
        }
        for guard in &self.controls {
            let row =
                tx.extension_row_by_key(guard.table, &format!("{}:{}", guard.table, guard.key))?;
            if row.as_ref() != guard.embedded.as_ref() {
                return Err(integrity("coverage exact control changed"));
            }
        }
        for original in &self.canonical {
            let current = required(
                CanonicalRecord::load_tx(tx, &self.space, &original.base().id)?,
                "coverage input removed",
            )?;
            if !same(original, &current)? {
                return Err(integrity("coverage input changed"));
            }
        }
        for node in &self.nodes {
            let current = required(
                tx.get::<NodeEmbedding>(&self.space, &node.embedding_id, false)?,
                "coverage vector removed",
            )?;
            if node.embedded.as_ref() != Some(&current) {
                return Err(integrity("coverage node vector changed"));
            }
            validate_node_vector(&current, &node.node, &self.model, self.partition.dimensions)?;
            if !tx.node_embedding_index_contains(&current)? {
                return Err(integrity("coverage exact vector index member missing"));
            }
        }
        for entity in &self.entities {
            let row = tx.extension_row_by_key("entity_profile_embedding", &entity.key)?;
            if row.as_ref() != entity.embedded.as_ref() {
                return Err(integrity("coverage entity vector changed"));
            }
            validate_entity_vector(
                &required(row, "coverage entity vector removed")?,
                &entity.entity,
                &self.model,
                self.partition.dimensions,
            )?;
        }
        Ok(())
    }
    fn write_embedded_completion(
        &self,
        tx: &mut crate::embedded::EmbeddedMutation<'_>,
        writes: &[CoverageWrite],
    ) -> StorageResult<()> {
        for (table, key, payload) in writes {
            // Completion schema intentionally has no `state` field.
            if self.replay {
                continue;
            }
            let guard = self
                .controls
                .iter()
                .find(|guard| guard.table == *table && guard.key == *key)
                .ok_or_else(|| integrity("coverage write guard absent"))?;
            let mut row = required(guard.embedded.clone(), "coverage embedded wrapper missing")?;
            row["payload"] = serde_json::Value::String(payload.clone());
            if *table == "source_operation" {
                row["state"] = serde_json::to_value(self.operation.receipt.state)
                    .map_err(|e| integrity(&e.to_string()))?;
            }
            tx.upsert_extension_row(*table, row)?;
        }
        Ok(())
    }
    fn native_completion_query(
        &self,
        writes: &[CoverageWrite],
    ) -> StorageResult<(String, BTreeMap<String, Value>)> {
        let mut bindings = crate::bindings! {"space"=>self.space.clone(),"model"=>self.model.clone(),"dims"=>self.partition.dimensions,"epoch_id"=>control_thing("canonical_epoch",&self.space),"epoch"=>self.epoch.clone().map_or(Value::None,Value::from),"config_id"=>crate::surreal::thing_from_record_id(&self.config.base.id)};
        let mut sql=String::from("BEGIN TRANSACTION;\nLET $coverage_epoch = SELECT * FROM ONLY $epoch_id;\nIF ($epoch=NONE AND $coverage_epoch!=NONE) OR ($epoch!=NONE AND ($coverage_epoch=NONE OR $coverage_epoch.space_id!=$space OR $coverage_epoch.epoch!=$epoch)) { THROW 'coverage epoch changed'; };\n");
        for (i, guard) in self.controls.iter().enumerate() {
            bindings.insert(
                format!("control_id_{i}"),
                control_thing(guard.table, &guard.key).into(),
            );
            bindings.insert(format!("control_payload_{i}"), guard.payload.clone().into());
            let _=writeln!(sql,"LET $coverage_control_{i} = SELECT * FROM ONLY $control_id_{i}; IF $coverage_control_{i}=NONE OR $coverage_control_{i}.space_id!=$space OR $coverage_control_{i}.payload!=$control_payload_{i} {{ THROW 'coverage control changed'; }};");
        }
        for (i, guard) in self.native.iter().enumerate() {
            bindings.insert(format!("native_id_{i}"), guard.id.clone().into());
            bindings.insert(format!("native_row_{i}"), guard.row.clone());
            let _=writeln!(sql,"IF (SELECT * FROM ONLY $native_id_{i})!=$native_row_{i} {{ THROW 'coverage exact native dependency changed'; }};");
        }
        // These write locks serialize completion with every config/indexed
        // content change, even when the driver interleaves transaction reads.
        if !self.replay {
            if self.epoch.is_none() {
                return Err(integrity("coverage epoch absent"));
            }
            sql.push_str("UPDATE ONLY $epoch_id SET epoch=<string>rand::uuid();\n");
        }
        self.append_native_node_guards(&mut sql, &mut bindings);
        self.append_native_index_guards(&mut sql, &mut bindings)?;
        self.append_native_entity_guards(&mut sql, &mut bindings);
        self.append_native_completion(&mut sql, &mut bindings, writes)?;
        sql.push_str("COMMIT TRANSACTION;");
        Ok((sql, bindings))
    }
    fn append_native_node_guards(&self, sql: &mut String, bindings: &mut BTreeMap<String, Value>) {
        for (i, node) in self.nodes.iter().enumerate() {
            bindings.insert(format!("node_vector_{i}"), node.native_id.clone().into());
            bindings.insert(
                format!("node_{i}"),
                crate::surreal::thing_from_record_id(&node.node.base.id).into(),
            );
            let _=writeln!(sql,"LET $coverage_node_{i}=SELECT * FROM ONLY $node_{i}; LET $coverage_vector_{i}=SELECT * FROM ONLY $node_vector_{i};\nIF $coverage_vector_{i}=NONE OR $coverage_vector_{i}.space_id!=$space OR $coverage_vector_{i}.deleted_at!=NONE OR $coverage_vector_{i}.node!=$node_{i} OR $coverage_vector_{i}.model!=$model OR $coverage_vector_{i}.dims!=$dims OR array::len($coverage_vector_{i}.vector)!=$dims OR $coverage_vector_{i}.vector_norm=NONE OR $coverage_vector_{i}.vector_norm<=0 OR vector::magnitude($coverage_vector_{i}.vector)<=0 OR $coverage_vector_{i}.security!=$coverage_node_{i}.security OR $coverage_vector_{i}.subjects!=$coverage_node_{i}.subjects OR $coverage_vector_{i}.subjects_state!=$coverage_node_{i}.subjects_state {{ THROW 'coverage current node vector missing'; }};\nFOR $coverage_component IN $coverage_vector_{i}.vector {{ IF $coverage_component!=$coverage_component OR math::abs($coverage_component)>3.4028235e38 {{ THROW 'coverage nonfinite vector'; }}; }};",);
        }
    }
    fn append_native_index_guards(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        let mut partitions: BTreeMap<&str, Vec<Value>> = BTreeMap::new();
        // Native capture already bounds this inventory to 100,000 rows. Stable
        // ordering plus the final equal entry retains BTreeMap collection's
        // last-value rule for duplicate native identities, without mutable keys.
        let mut vector_guards: Vec<&NativeGuard> = self.native.iter().collect();
        vector_guards.sort_by(|left, right| left.id.cmp(&right.id));
        for node in &self.nodes {
            let end = vector_guards.partition_point(|guard| guard.id <= node.native_id);
            let guard = end
                .checked_sub(1)
                .and_then(|index| vector_guards.get(index))
                .filter(|guard| guard.id == node.native_id)
                .ok_or_else(|| integrity("coverage vector guard absent"))?;
            let Value::Object(fields) = &guard.row else {
                return Err(integrity("coverage vector native shape"));
            };
            let vector = required(fields.get("vector").cloned(), "coverage vector absent")?;
            partitions
                .entry(&node.table)
                .or_default()
                .push(Value::Object(surrealdb::sql::Object::from(BTreeMap::<
                    String,
                    Value,
                >::from(
                    [
                    ("id".into(), Value::Thing(node.native_id.clone())),
                    ("vector".into(), vector),
                ]
                ))));
        }
        let per_batch = (16_777_216 / self.partition.dimensions.max(1)).clamp(1, 100_000);
        let mut batch = 0usize;
        for (table, rows) in partitions {
            for rows in rows.chunks(per_batch) {
                bindings.insert(format!("partition_table_{batch}"), Value::from(table));
                bindings.insert(
                    format!("partition_index_{batch}"),
                    Value::from(format!("{table}_hnsw")),
                );
                bindings.insert(
                    format!("partition_rows_{batch}"),
                    Value::Array(surrealdb::sql::Array::from(rows.to_vec())),
                );
                let _ = writeln!(sql, "IF !recall::hnsw_coverage($partition_table_{batch}, $partition_index_{batch}, $partition_rows_{batch}) {{ THROW 'coverage physical index incomplete'; }};");
                batch += 1;
            }
        }
        Ok(())
    }
    fn append_native_entity_guards(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) {
        for (i, entity) in self.entities.iter().enumerate() {
            bindings.insert(
                format!("entity_vector_{i}"),
                entity.native_id.clone().into(),
            );
            bindings.insert(
                format!("entity_id_{i}"),
                entity.entity.base.id.to_string().into(),
            );
            bindings.insert(
                format!("entity_version_{i}"),
                crate::surreal::datetime_from_chrono(entity.entity.base.updated_at).into(),
            );
            bindings.insert(
                format!("entity_hash_{i}"),
                crate::entity_profile_embedding_repository::entity_input_hash(&entity.entity)
                    .into(),
            );
            bindings.insert(
                format!("entity_sources_{i}"),
                Value::from(
                    entity
                        .entity
                        .base
                        .provenance
                        .sources
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                ),
            );
            let _=writeln!(sql,"LET $coverage_entity_{i}=SELECT * FROM ONLY $entity_vector_{i}; IF $coverage_entity_{i}=NONE OR $coverage_entity_{i}.space_id!=$space OR $coverage_entity_{i}.entity_id!=$entity_id_{i} OR $coverage_entity_{i}.model!=$model OR $coverage_entity_{i}.text_hash!=$entity_hash_{i} OR $coverage_entity_{i}.entity_version!=$entity_version_{i} OR $coverage_entity_{i}.source_refs!=$entity_sources_{i} OR array::len($coverage_entity_{i}.vector)!=$dims OR vector::magnitude($coverage_entity_{i}.vector)<=0 {{ THROW 'coverage current entity vector missing'; }}; FOR $coverage_component IN $coverage_entity_{i}.vector {{ IF $coverage_component!=$coverage_component OR math::abs($coverage_component)>3.4028235e38 {{ THROW 'coverage nonfinite entity vector'; }}; }};");
        }
    }
    fn append_native_completion(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
        writes: &[CoverageWrite],
    ) -> StorageResult<()> {
        for (i, (table, key, payload)) in writes.iter().enumerate() {
            bindings.insert(format!("write_id_{i}"), control_thing(table, key).into());
            if self.replay {
                continue;
            }
            let guard = self
                .native
                .iter()
                .find(|guard| guard.id == control_thing(table, key))
                .ok_or_else(|| integrity("coverage native write guard absent"))?;
            let Value::Object(mut fields) = guard.row.clone() else {
                return Err(integrity("coverage control native shape"));
            };
            fields.insert("payload".into(), Value::from(payload.clone()));
            if *table == "source_operation" {
                fields.insert(
                    "state".into(),
                    crate::surreal::json_to_surreal(
                        serde_json::to_value(self.operation.receipt.state)
                            .map_err(|e| integrity(&e.to_string()))?,
                    )?,
                );
            }
            let native = Value::Object(fields);
            bindings.insert(format!("write_row_{i}"), native);
            let _ = writeln!(sql, "UPDATE ONLY $write_id_{i} CONTENT $write_row_{i};");
        }
        Ok(())
    }
    async fn commit(self, client: &StorageClient) -> StorageResult<OperationReceipt> {
        let writes = self.completion_writes()?;
        if let Some(store) = client.embedded_store() {
            store
                .mutate(|tx| {
                    self.validate_embedded_inputs(tx)?;
                    self.write_embedded_completion(tx, &writes)
                })
                .await?;
        } else {
            let (sql, bindings) = self.native_completion_query(&writes)?;
            client.query_bind_checked(&sql, bindings).await?;
        }
        Ok(self.operation.receipt)
    }
}

#[cfg(test)]
#[path = "historical_embedding_coverage_tests.rs"]
mod tests;
