//! Custody-owned historical publication. No worker attempt is synthesized.

use super::{
    integrity, node_input_fingerprint, sha256, source_support_fingerprint, stable_key,
    valid_digest, BTreeMap, BTreeSet, Deserialize, Digest, Doc, DocNode, DocRevision,
    HistoricalAdoptionInput, HistoricalAdoptionProof, HistoricalCommitProvenance,
    HistoricalDocumentInput, HistoricalDocumentProof, HistoricalSourceGroup,
    HistoricalUnknownOwnership, HistoricalVectorState, RecordId, Serialize, Sha256,
    SourceDocumentMapping, SourcePublicationManifest, TableName, Utc,
};
use crate::canonical_commit::{
    append_row_guard, CanonicalCommitReceipt, CanonicalRecord, CanonicalRepository,
};
use crate::source_publication::{
    control_thing, external_identity_key, head_key, operation_key, revision_key, SourceIdentity,
    SourcePublicationRepository, StoredOperation, StoredRevision,
};
use crate::source_publication_manifest::{
    manifest_key, SourcePublicationCompletion, COMPLETION_TABLE, DOCUMENT_TABLE, MANIFEST_TABLE,
    REVISION_TABLE,
};
use crate::{Repository, StorageClient, StorageError, StorageResult};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::{
    OperationReceipt, OperationState, PublicationFence, SourceAuthorizationEnvelope, SourceHead,
    SourceRevision, SourceVisibility,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::doc_revision::RevisionStatus;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::{Source, SourceType, SpaceConfig};
use elephant_types::Link;
use std::fmt::Write as _;

/// One closed original document population, supplied from the verified disk index.
#[derive(Debug, Clone)]
pub struct HistoricalOriginalDocument {
    /// Complete original typed document before explicit source association.
    pub document: Doc,
    /// Exact decoded original row; raw policy/provenance hashes use this representation.
    pub original_document: serde_json::Value,
    /// Immutable original revision with its unchanged text and evidence offsets.
    pub revision: DocRevision,
    /// Complete original node population for this document revision.
    pub nodes: Vec<DocNode>,
}

/// Original rows remain unchanged apart from the explicit Doc.source association.
#[derive(Debug, Clone)]
pub struct HistoricalGroupRows {
    /// Complete original document population for this source group.
    pub documents: Vec<HistoricalOriginalDocument>,
    /// Complete original source records retained by the group.
    pub original_sources: Vec<Source>,
    /// Closed reverse-index graph membership from the same immutable spool.
    pub graph: Vec<CanonicalRecord>,
}

/// Non-serializable capability: only a live transfer owner can prepare it.
#[derive(Debug)]
pub struct PreparedHistoricalAuthority {
    client: StorageClient,
    snapshot_sha256: String,
    journal_identity: String,
    input: HistoricalAdoptionInput,
    group_index: BTreeMap<String, usize>,
    snapshot_index: BTreeMap<String, usize>,
    population: tokio::sync::OnceCell<population::HistoricalPopulation>,
    catchup: Option<crate::client::SealedHistoricalCatchupAuthority>,
    original_document_groups: BTreeMap<RecordId, RecordId>,
    original_mappings: BTreeMap<RecordId, SourceDocumentMapping>,
    original_completions: BTreeMap<RecordId, SourcePublicationCompletion>,
    regeneration: Option<std::sync::Mutex<regeneration::RegenerationPosition>>,
}

struct GroupAssembly<'a> {
    source: PreparedGroupSource<'a>,
    group: &'a HistoricalSourceGroup,
    external_source_id: &'a str,
    now: chrono::DateTime<Utc>,
}
struct GroupReceipts {
    result: HistoricalAdoptionReceipt,
    head: SourceHead,
    completion: SourcePublicationCompletion,
    stored_operation: StoredOperation,
}

struct PreparedGroupSource<'a> {
    original_sources: BTreeMap<RecordId, &'a Source>,
    source_id: RecordId,
    source: Source,
    previous_source: Option<Source>,
    revision_id: String,
    request_fingerprint: String,
    source_input_fingerprint: String,
    operation_id: String,
    generation: String,
    publication: PublicationFence,
    accepted: SourceRevision,
}

struct HistoricalDocumentPreparation<'a> {
    group: &'a HistoricalSourceGroup,
    source: &'a Source,
    publication: &'a PublicationFence,
    original_sources: &'a BTreeMap<RecordId, &'a Source>,
    observed_epoch: Option<&'a str>,
}
#[derive(Default)]
struct HistoricalDocumentEffects {
    documents: BTreeMap<RecordId, HistoricalDocumentProof>,
    mappings: Vec<SourceDocumentMapping>,
    before_after: Vec<(Option<CanonicalRecord>, CanonicalRecord)>,
    dependencies: Vec<CanonicalRecord>,
    seen: BTreeSet<RecordId>,
}

struct HistoricalGroupCommit<'a> {
    config: &'a SpaceConfig,
    source: &'a RecordId,
    dependencies: &'a [CanonicalRecord],
    effects: &'a [(Option<CanonicalRecord>, CanonicalRecord)],
    controls: &'a [Control],
    control_before: &'a BTreeMap<(String, String), super::catchup::HistoricalControlImage>,
    epoch: Option<String>,
    after_epoch: Option<&'a str>,
    progress: Option<&'a CanonicalCommitReceipt>,
}
type NativeGroupInventory = Vec<(String, surrealdb::sql::Thing)>;
type DurableGroupEffects = Option<(
    crate::historical_native_effects::PreparedNativeEffects,
    surrealdb::sql::Thing,
)>;

/// Actual historical adoption result bound to source identity and original snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HistoricalAdoptionReceipt {
    /// Original snapshot namespace before the declared relocation.
    pub source_space_id: String,
    /// Destination namespace containing the adopted population.
    pub space_id: String,
    /// Stable external identity for this complete historical group.
    pub external_source_id: String,
    /// Canonical source identity for the historical publication.
    pub source_id: RecordId,
    /// Operation receipt for the bound historical publication.
    pub operation: OperationReceipt,
    /// Historical adapter revision identity accepted for this source.
    pub accepted_revision: String,
    /// Dedicated historical adapter fingerprint; never a manual PUT fingerprint.
    pub accepted_request_fingerprint: String,
    /// Digest of the complete sorted original row-to-content mapping.
    pub document_mapping_sha256: String,
    /// Exact complete map for the authenticated historical-unchanged request.
    pub document_content_sha256: BTreeMap<RecordId, String>,
    /// Digest of the immutable historical publication manifest.
    pub manifest_digest: String,
    /// SHA-256 of the original snapshot retained by this receipt.
    pub original_snapshot_sha256: String,
    /// Exact canonical adoption or retirement commit identifier, not a digest.
    pub commit_id: String,
}

/// Exact native full-row inventory changes captured by the guarded transaction.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HistoricalRowEffect {
    /// Physical native table containing this exact inventory effect.
    pub table: String,
    /// Exact native record identity of the affected row.
    pub id: String,
    /// Original full native row digest, or absent if the row did not exist.
    pub before_sha256: Option<String>,
    /// Resulting full native row digest, including its control wrapper.
    pub after_sha256: String,
}
/// One atomically adopted group with its durable receipt and inventory effects.
#[derive(Debug)]
pub struct HistoricalCommittedGroup {
    /// Durable historical adoption receipt produced by the group transaction.
    pub receipt: HistoricalAdoptionReceipt,
    /// Native type::string SHA256 domain. Empty only on embedded backends,
    /// whose typed effects are validated by the same atomic closure.
    pub native_effects: Vec<HistoricalRowEffect>,
}

/// Custody-bound adoption and read-only recognition of original historical rows.
#[derive(Debug, Clone)]
pub struct HistoricalAdoptionRepository {
    client: StorageClient,
}

impl HistoricalAdoptionRepository {
    #[must_use]
    /// Bind historical publication to an existing storage client.
    pub const fn new(client: StorageClient) -> Self {
        Self { client }
    }

    pub(super) const fn client(&self) -> &StorageClient {
        &self.client
    }

    /// Read-only recognition of actual historical publication authority.
    /// Recovery uses this to keep adopted content out of extraction planning.
    pub async fn is_historical_publication(
        &self,
        space: &str,
        publication: &PublicationFence,
    ) -> StorageResult<bool> {
        let Some(manifest) =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                self.client.clone(),
            )
            .get(space, publication)
            .await?
        else {
            return Ok(false);
        };
        let Some(proof) = &manifest.historical_adoption else {
            return Ok(false);
        };
        manifest.validate()?;
        let receipt = CanonicalRepository::new(self.client.clone())
            .receipt_read_only(space, &proof.commit_id(&manifest))
            .await?
            .ok_or_else(|| integrity("historical recovery receipt missing"))?;
        if receipt.publication_authority.as_ref() != Some(publication)
            || receipt.historical_import.as_ref()
                != Some(&HistoricalCommitProvenance::for_manifest(&manifest)?)
        {
            return Err(integrity("historical recovery commit differs"));
        }
        Ok(true)
    }

    /// Bootstrap the explicit configuration before content and return only
    /// the exact generated config/epoch inventory effects under transfer custody.
    pub async fn bootstrap_configuration(
        &self,
        snapshot: &str,
        configuration: SpaceConfig,
    ) -> StorageResult<Vec<HistoricalRowEffect>> {
        self.client.require_historical_transfer(snapshot)?;
        let space = configuration.base.space_id.clone();
        let config_id = crate::surreal::thing_from_record_id(&configuration.base.id);
        let epoch_id = control_thing("canonical_epoch", &space);
        let guard_id = control_thing("source_erasure_guard", &space);
        let (before_epoch, before_guard) = if self.client.embedded_store().is_none() {
            let mut response = self
                .client
                .query_bind_checked(
                    "SELECT * FROM ONLY $epoch; SELECT * FROM ONLY $guard;",
                    crate::bindings! {"epoch"=>epoch_id.clone(),"guard"=>guard_id.clone()},
                )
                .await?;
            let before_epoch = response
                .take::<Option<surrealdb::sql::Value>>(0)?
                .unwrap_or(surrealdb::sql::Value::None);
            let before_guard = response
                .take::<Option<surrealdb::sql::Value>>(1)?
                .unwrap_or(surrealdb::sql::Value::None);
            (before_epoch, before_guard)
        } else {
            (surrealdb::sql::Value::None, surrealdb::sql::Value::None)
        };
        crate::SpaceConfigRepository::new(self.client.clone())
            .create(configuration.clone())
            .await?;
        if crate::SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(&configuration.base.space_id)
            .await?
            .as_ref()
            != Some(&configuration)
        {
            return Err(integrity("historical bootstrap typed image differs"));
        }
        if self.client.embedded_store().is_some() {
            return Ok(Vec::new());
        }
        let mut bindings = crate::bindings! {
            "space"=>space.clone(), "config"=>config_id.clone(), "epoch"=>epoch_id.clone(),
            "guard"=>guard_id.clone(), "expected"=>crate::canonical_commit::native_expected_fields(crate::surreal::space_config_to_value(&configuration)?),
            "bootstrap_epoch_before"=>before_epoch, "historical_guard_before"=>before_guard,
            "historical_guard_writes"=>1_i64,
        };
        bindings.insert("historical_guard_id".into(), guard_id.clone().into());
        let mut sql = String::from("BEGIN TRANSACTION; IF !recall::original_row_matches((SELECT * FROM ONLY $config),$space,$expected) { THROW 'historical bootstrap configuration differs'; };\n");
        append_generated_guard_check(&mut sql);
        sql.push_str("RETURN [{table:'space_config',id:type::string($config),before_sha256:NONE,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $config)},{table:'canonical_epoch',id:type::string($epoch),before_sha256:IF $bootstrap_epoch_before=NONE THEN NONE ELSE crypto::sha256(type::string($bootstrap_epoch_before)) END,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $epoch)},{table:'source_erasure_guard',id:type::string($guard),before_sha256:IF $historical_guard_before=NONE THEN NONE ELSE crypto::sha256(type::string($historical_guard_before)) END,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $guard)}]; COMMIT TRANSACTION;");
        let mut response = self.client.query_bind_checked(&sql, bindings).await?;
        let index = response
            .num_statements()
            .checked_sub(1)
            .ok_or_else(|| integrity("historical bootstrap result absent"))?;
        let effects: Vec<HistoricalRowEffect> = response
            .take(index)
            .map_err(|_| integrity("historical bootstrap inventory decode"))?;
        let expected = [
            ("space_config", config_id),
            ("canonical_epoch", epoch_id),
            ("source_erasure_guard", guard_id),
        ];
        if effects.len() != expected.len()
            || effects.iter().zip(&expected).any(|(effect, (table, id))| {
                effect.table != *table
                    || effect.id != id.to_string()
                    || !valid_digest(&effect.after_sha256)
                    || effect
                        .before_sha256
                        .as_ref()
                        .is_some_and(|hash| !valid_digest(hash))
            })
            || effects[0].before_sha256.is_some()
        {
            return Err(integrity("historical bootstrap inventory invalid"));
        }
        Ok(effects)
    }

    /// Validate the complete input while exclusive initial-transfer custody is live.
    /// Declarations alone never grant authority to change an existing destination.
    pub fn prepare_authority(
        &self,
        input: HistoricalAdoptionInput,
    ) -> StorageResult<PreparedHistoricalAuthority> {
        input.validate()?;
        self.client
            .require_historical_transfer(&input.snapshot_sha256)?;
        Ok(PreparedHistoricalAuthority {
            client: self.client.clone(),
            snapshot_sha256: input.snapshot_sha256.clone(),
            journal_identity: self.client.erasure_journal()?.identity().to_owned(),
            group_index: input
                .groups
                .iter()
                .enumerate()
                .map(|(index, group)| (group.external_source_id.clone(), index))
                .collect(),
            snapshot_index: input
                .snapshot_groups
                .iter()
                .enumerate()
                .map(|(index, group)| (group.external_source_id.clone(), index))
                .collect(),
            input,
            population: tokio::sync::OnceCell::new(),
            catchup: None,
            original_document_groups: BTreeMap::new(),
            original_mappings: BTreeMap::new(),
            original_completions: BTreeMap::new(),
            regeneration: None,
        })
    }
}

#[derive(Clone)]
struct Control {
    table: &'static str,
    key: String,
    payload: String,
    state: serde_json::Value,
}

impl Control {
    fn new<T: Serialize>(table: &'static str, key: String, payload: &T) -> StorageResult<Self> {
        Ok(Self {
            table,
            key,
            payload: serde_json::to_string(payload)
                .map_err(|_| integrity("historical control encoding failed"))?,
            state: serde_json::to_value(payload)
                .map_err(|_| integrity("historical control encoding failed"))?,
        })
    }
    fn value(&self, space: &str, source: &RecordId) -> serde_json::Value {
        // 035 deliberately stores only the serialized authority on these
        // SCHEMAFULL tables. Do not propose a state field the schema removes.
        let mut value = serde_json::json!({"space_id":space,"source_id":source.to_string(),"payload":self.payload});
        if !matches!(
            self.table,
            "source_publication_manifest"
                | "source_publication_completion"
                | "source_document_mapping"
                | "source_revision_mapping"
        ) {
            value["state"] = self.state.clone();
        }
        if self.table == "source_effect_support" {
            for key in [
                "record_id",
                "source_revision",
                "independent",
                "record_fingerprint",
                "historical_unknown",
            ] {
                value[key] = self.state[key].clone();
            }
        }
        value
    }
}

impl PreparedHistoricalAuthority {
    fn group(&self, external: &str) -> StorageResult<&HistoricalSourceGroup> {
        self.group_index
            .get(external)
            .and_then(|index| self.input.groups.get(*index))
            .ok_or_else(|| integrity("historical group absent from closed plan"))
    }

    fn publication_group(&self, external: &str) -> StorageResult<HistoricalSourceGroup> {
        if let Some(index) = self.snapshot_index.get(external) {
            let group = self
                .input
                .snapshot_groups
                .get(*index)
                .ok_or_else(|| integrity("historical snapshot group index invalid"))?;
            return Ok(HistoricalSourceGroup {
                external_source_id: group.external_source_id.clone(),
                document_mapping_sha256: group.document_mapping_sha256.clone(),
                documents: vec![],
            });
        }
        Ok(self.group(external)?.clone())
    }

    fn check_custody(&self) -> StorageResult<()> {
        if let Some(catchup) = &self.catchup {
            catchup.check_custody()?;
        } else {
            self.client
                .require_historical_transfer(&self.snapshot_sha256)?;
        }
        if self.client.erasure_journal()?.identity() != self.journal_identity {
            return Err(integrity("historical journal identity changed"));
        }
        Ok(())
    }

    /// Install one fully closed document group atomically. This does not execute
    /// extraction, reindex providers, ordinary queue work, or completion claims.
    pub async fn adopt_group(
        &self,
        external_source_id: &str,
        original: HistoricalGroupRows,
    ) -> StorageResult<HistoricalAdoptionReceipt> {
        Ok(self
            .adopt_group_inner(external_source_id, original, false)
            .await?
            .receipt)
    }

    /// The fresh-target transfer reconciler consumes these exact inventory
    /// changes. It must never recapture an entire mutated original table.
    pub async fn adopt_group_with_effects(
        &self,
        external_source_id: &str,
        original: HistoricalGroupRows,
    ) -> StorageResult<HistoricalCommittedGroup> {
        self.adopt_group_inner(external_source_id, original, true)
            .await
    }

    async fn adopt_group_inner(
        &self,
        external_source_id: &str,
        original: HistoricalGroupRows,
        require_fresh_inventory: bool,
    ) -> StorageResult<HistoricalCommittedGroup> {
        self.check_custody()?;
        let group = self.group(external_source_id)?;
        let space = &self.input.space_id;
        let observed_epoch = CanonicalRepository::new(self.client.clone())
            .observe_epoch(space)
            .await?;
        let replay_key = operation_key(
            space,
            &format!("historical:{}:{external_source_id}", self.input.plan_id),
        );
        if let Some(existing) = SourcePublicationRepository::new(self.client.clone())
            .read_control::<StoredOperation>("source_operation", &replay_key, space)
            .await?
        {
            if require_fresh_inventory {
                return Err(integrity(
                    "historical inventory replay requires original effect receipt",
                ));
            }
            return Ok(HistoricalCommittedGroup {
                receipt: self.verify_existing_group(group, &existing.receipt).await?,
                native_effects: Vec::new(),
            });
        }
        let (config, token) = self.group_configuration().await?;
        let now = Utc::now();
        if original.documents.len() != group.documents.len() {
            return Err(integrity("historical group document population changed"));
        }
        let context = GroupAssembly {
            source: self
                .prepare_group_source(
                    group,
                    &original.documents,
                    &original.original_sources,
                    external_source_id,
                    now,
                )
                .await?,
            group,
            external_source_id,
            now,
        };
        let (mut rows, graph_fingerprints) = self
            .prepare_group_rows(
                &context,
                &original.original_sources,
                original.documents,
                &original.graph,
                observed_epoch.as_deref(),
            )
            .await?;
        let manifest = self.assemble_group_manifest(
            &context,
            &mut rows,
            graph_fingerprints,
            &original.original_sources,
            token,
        )?;
        let receipts = self.assemble_group_receipts(&context, &manifest)?;
        let mut controls =
            self.assemble_group_controls(&context, &manifest, &receipts, &mut rows)?;
        self.append_group_membership_controls(
            &context,
            &manifest,
            &original.graph,
            &receipts.result.commit_id,
            &mut controls,
        )?;
        self.commit_prepared_group(
            &context,
            &config,
            &rows,
            controls,
            observed_epoch,
            receipts.result,
        )
        .await
    }

    async fn group_configuration(&self) -> StorageResult<(SpaceConfig, SpaceConfigToken)> {
        let space = &self.input.space_id;
        let config = crate::SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(space)
            .await?
            .ok_or_else(|| integrity("historical target space configuration missing"))?;
        let processing = config
            .processing
            .as_ref()
            .ok_or_else(|| integrity("historical target processing configuration missing"))?;
        let token = SpaceConfigToken {
            space_id: space.clone(),
            config_id: config.base.id.to_string(),
            generation: processing.generation,
            policy_id: processing.policy_id.clone(),
        };
        processing
            .validate()
            .map_err(|_| integrity("historical configuration invalid"))?;
        Ok((config, token))
    }
    async fn validate_original_document<'a>(
        &self,
        context: &HistoricalDocumentPreparation<'a>,
        state: &mut HistoricalDocumentEffects,
        row: &HistoricalOriginalDocument,
    ) -> StorageResult<&'a HistoricalDocumentInput> {
        let space = &self.input.space_id;
        let declaration = context
            .group
            .documents
            .iter()
            .find(|d| d.legacy_doc_id == row.document.base.id)
            .ok_or_else(|| integrity("historical document not declared"))?;
        let original_doc: Doc = serde_json::from_value(row.original_document.clone())
            .map_err(|_| integrity("historical original document invalid"))?;
        declaration.validate_original_document(&original_doc, &row.original_document)?;
        if original_doc.base.space_id != self.input.source_space_id {
            return Err(integrity("historical original namespace differs"));
        }
        let mut projected_doc = original_doc;
        projected_doc.base.space_id.clone_from(space);
        if projected_doc != row.document {
            return Err(integrity("historical target document projection differs"));
        }
        let population = self
            .population
            .get_or_try_init(|| self.prepare_population())
            .await?;
        population.verify_epoch(context.observed_epoch)?;
        population.verify(row)?;

        if !state.seen.insert(row.document.base.id.clone())
            || row.document.base.space_id != *space
            || row.document.base.deleted_at.is_some()
            || row.document.base.security != context.source.base.security
            || row.document.base.subjects != context.source.base.subjects
            || row.document.base.subjects_state != context.source.base.subjects_state
            || row
                .document
                .source
                .as_ref()
                .is_some_and(|link| !context.original_sources.contains_key(link.id()))
            || row.revision.doc.id() != &row.document.base.id
            || row.revision.base.space_id != *space
            || row.revision.base.deleted_at.is_some()
            || row.revision.status != RevisionStatus::Active
            || row.revision.text != row.document.text
            || row
                .revision
                .text_hash
                .strip_prefix("sha256:")
                .unwrap_or(&row.revision.text_hash)
                != declaration.original_text_sha256
        {
            return Err(integrity(
                "historical document policy, lineage or original text differs",
            ));
        }
        Ok(declaration)
    }
    fn prepare_original_document(
        &self,
        context: &HistoricalDocumentPreparation<'_>,
        state: &mut HistoricalDocumentEffects,
        row: HistoricalOriginalDocument,
        declaration: &HistoricalDocumentInput,
    ) -> StorageResult<()> {
        let space = &self.input.space_id;
        let mut after = row.document.clone();
        after.source = Some(Link::new(context.source.base.id.clone()));
        let mut node_fingerprints = BTreeMap::new();
        for node in row.nodes {
            if node.base.space_id != *space
                || node.base.deleted_at.is_some()
                || node.revision.id() != &row.revision.base.id
                || node.span.start >= node.span.end
                || row
                    .revision
                    .text
                    .get(node.span.start as usize..node.span.end as usize)
                    .is_none()
                || node_fingerprints
                    .insert(node.base.id.clone(), node_input_fingerprint(&node)?)
                    .is_some()
            {
                return Err(integrity(
                    "historical node closure or original UTF-8 span invalid",
                ));
            }
            state
                .dependencies
                .push(CanonicalRecord::DocNode(Box::new(node)));
        }
        state.documents.insert(
            after.base.id.clone(),
            HistoricalDocumentProof {
                original_row_sha256: declaration.original_row_sha256.clone(),
                original_source_id: row.document.source.as_ref().map(|link| link.id().clone()),
                document_fingerprint: source_support_fingerprint(&after)?,
                revision_fingerprint: source_support_fingerprint(&row.revision)?,
                original_text_sha256: declaration.original_text_sha256.clone(),
                doc_content_sha256: declaration.doc_content_sha256.clone(),
                node_fingerprints,
            },
        );
        state.mappings.push(SourceDocumentMapping {
            space_id: space.clone(),
            doc_id: after.base.id.clone(),
            doc_revision_id: row.revision.base.id.clone(),
            publication: context.publication.clone(),
        });
        state
            .dependencies
            .push(CanonicalRecord::DocRevision(Box::new(row.revision)));
        let mut expected_current = row.document;
        if self.catchup.is_some() {
            if let Some(group) = self.original_document_groups.get(&expected_current.base.id) {
                expected_current.source = Some(Link::new(group.clone()));
            }
        }
        state.before_after.push((
            Some(CanonicalRecord::Doc(Box::new(expected_current))),
            CanonicalRecord::Doc(Box::new(after)),
        ));
        Ok(())
    }
    fn prepare_original_graph(
        &self,
        graph: &[CanonicalRecord],
        dependencies: &mut Vec<CanonicalRecord>,
    ) -> StorageResult<BTreeMap<RecordId, String>> {
        let space = &self.input.space_id;
        let mut graph_fingerprints = BTreeMap::new();
        for record in graph {
            if !crate::canonical_commit::is_derived_graph_table(record.base().id.table())
                || record.base().space_id != *space
                || record.base().deleted_at.is_some()
                || graph_fingerprints
                    .insert(record.base().id.clone(), record.fingerprint()?)
                    .is_some()
            {
                return Err(integrity("historical graph closure invalid"));
            }
            dependencies.push(record.clone());
        }
        Ok(graph_fingerprints)
    }
    async fn prepare_group_source<'a>(
        &self,
        group: &HistoricalSourceGroup,
        documents: &[HistoricalOriginalDocument],
        original_sources_input: &'a [Source],
        external_source_id: &str,
        now: chrono::DateTime<Utc>,
    ) -> StorageResult<PreparedGroupSource<'a>> {
        let space = &self.input.space_id;
        let (original_sources, source_id, mut source) = self.original_group_source(
            group,
            documents,
            original_sources_input,
            external_source_id,
            now,
        )?;
        let previous_source = if self.catchup.is_some() {
            let previous = self.catchup_source_before(&source_id).await?;
            if let Some(previous) = &previous {
                source.base.created_at = previous.base.created_at;
            }
            previous
        } else {
            None
        };
        let revision_id = if self.catchup.is_some() {
            format!(
                "historical-snapshot-v1:{}",
                stable_key(&[&self.input.plan_id, &group.document_mapping_sha256])
            )
        } else {
            format!("historical-original-v1:{}", group.document_mapping_sha256)
        };
        let request_fingerprint = sha256(
            &serde_json::to_vec(&(
                "historical-adapter-v1",
                space,
                external_source_id,
                &group.document_mapping_sha256,
                &self.input.snapshot_sha256,
            ))
            .map_err(|_| integrity("historical request encoding failed"))?,
        );
        // The public historical request and canonical import receipt retain
        // their SHA-256 proof. Source operation/revision inputs use the existing
        // UUIDv5 contract required by lifecycle erasure history validation.
        let source_input_fingerprint =
            stable_key(&["historical-source-operation-input-v1", &request_fingerprint]);
        let operation_id = operation_key(
            space,
            &format!("historical:{}:{external_source_id}", self.input.plan_id),
        );
        let generation = stable_key(&[
            space,
            &source_id.to_string(),
            &self.input.plan_id,
            &group.document_mapping_sha256,
        ]);
        let publication = PublicationFence {
            source_id: source_id.clone(),
            generation: generation.clone(),
            revision_id: revision_id.clone(),
            operation_id: operation_id.clone(),
        };
        let accepted = SourceRevision {
            space_id: space.clone(),
            source_id: source_id.clone(),
            revision_id: revision_id.clone(),
            content_hash: source.hash.clone(),
            accepted_at: now,
            source: source.clone(),
        };
        Ok(PreparedGroupSource {
            original_sources,
            source_id,
            source,
            previous_source,
            revision_id,
            request_fingerprint,
            source_input_fingerprint,
            operation_id,
            generation,
            publication,
            accepted,
        })
    }
    async fn prepare_group_rows(
        &self,
        context: &GroupAssembly<'_>,
        original_sources: &[Source],
        original_documents: Vec<HistoricalOriginalDocument>,
        graph: &[CanonicalRecord],
        observed_epoch: Option<&str>,
    ) -> StorageResult<(HistoricalDocumentEffects, BTreeMap<RecordId, String>)> {
        let mut prepared_documents = HistoricalDocumentEffects {
            dependencies: original_sources
                .iter()
                .cloned()
                .map(|source| CanonicalRecord::Source(Box::new(source)))
                .collect(),
            ..HistoricalDocumentEffects::default()
        };
        let document_context = HistoricalDocumentPreparation {
            group: context.group,
            source: &context.source.source,
            publication: &context.source.publication,
            original_sources: &context.source.original_sources,
            observed_epoch,
        };
        for row in original_documents {
            let declaration = self
                .validate_original_document(&document_context, &mut prepared_documents, &row)
                .await?;
            self.prepare_original_document(
                &document_context,
                &mut prepared_documents,
                row,
                declaration,
            )?;
        }
        let graph_fingerprints =
            self.prepare_original_graph(graph, &mut prepared_documents.dependencies)?;
        Ok((prepared_documents, graph_fingerprints))
    }
    fn assemble_group_manifest(
        &self,
        context: &GroupAssembly<'_>,
        rows: &mut HistoricalDocumentEffects,
        graph_fingerprints: BTreeMap<RecordId, String>,
        original_sources: &[Source],
        token: SpaceConfigToken,
    ) -> StorageResult<SourcePublicationManifest> {
        let space = &self.input.space_id;
        let publication = &context.source.publication;
        let group = context.group;
        let external_source_id = context.external_source_id;
        let now = context.now;
        let proof = HistoricalAdoptionProof {
            snapshot_graph: None,
            version: 1,
            source_space_id: self.input.source_space_id.clone(),
            snapshot_sha256: self.input.snapshot_sha256.clone(),
            idmap_sha256: self.input.idmap_sha256.clone(),
            plan_id: self.input.plan_id.clone(),
            documents_sha256: self.input.documents_sha256.clone(),
            groups_sha256: self.input.groups_sha256.clone(),
            document_mapping_sha256: group.document_mapping_sha256.clone(),
            external_source_id: external_source_id.to_owned(),
            imported_at: now,
            documents: std::mem::take(&mut rows.documents),
            graph_fingerprints,
            original_sources: original_sources
                .iter()
                .map(|source| {
                    Ok((
                        source.base.id.clone(),
                        crate::source_publication::encode_payload(source)?,
                    ))
                })
                .collect::<StorageResult<_>>()?,
            vector_state: HistoricalVectorState::PreservedUnverified,
        };
        let manifest = SourcePublicationManifest {
            historical_adoption: Some(proof),
            space_id: space.clone(),
            publication: publication.clone(),
            mode: IngestMode::Graph,
            configuration: token,
            embedding_profile: None,
            documents: std::mem::take(&mut rows.mappings),
            metadata_artifact_ids: vec![],
            planning_artifact_ids: vec![],
            bundles: vec![],
            embeddings: vec![],
            embedding_work_item_ids: vec![],
        };
        manifest.validate()?;
        Ok(manifest)
    }
    fn assemble_group_receipts(
        &self,
        context: &GroupAssembly<'_>,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<GroupReceipts> {
        let space = &self.input.space_id;
        let source_id = &context.source.source_id;
        let revision_id = &context.source.revision_id;
        let request_fingerprint = &context.source.request_fingerprint;
        let operation_id = &context.source.operation_id;
        let generation = &context.source.generation;
        let group = context.group;
        let external_source_id = context.external_source_id;
        let now = context.now;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical proof missing"))?;
        let commit_id = proof.commit_id(manifest);
        let manifest_digest = manifest.digest()?;
        let operation = OperationReceipt {
            space_id: space.clone(),
            operation_id: operation_id.clone(),
            source_id: source_id.clone(),
            source_generation: generation.clone(),
            desired_revision: Some(revision_id.clone()),
            published_revision: Some(revision_id.clone()),
            state: OperationState::Processing,
            accepted_at: now,
            completed_at: None,
            error_code: Some("historical_embedding_coverage_pending".into()),
        };
        let result = HistoricalAdoptionReceipt {
            source_space_id: self.input.source_space_id.clone(),
            space_id: space.clone(),
            external_source_id: external_source_id.to_owned(),
            source_id: source_id.clone(),
            operation: operation.clone(),
            accepted_revision: revision_id.clone(),
            accepted_request_fingerprint: request_fingerprint.clone(),
            document_mapping_sha256: group.document_mapping_sha256.clone(),
            document_content_sha256: group
                .documents
                .iter()
                .map(|doc| (doc.legacy_doc_id.clone(), doc.doc_content_sha256.clone()))
                .collect(),
            manifest_digest: manifest_digest.clone(),
            original_snapshot_sha256: self.input.snapshot_sha256.clone(),
            commit_id: commit_id.clone(),
        };
        let head = SourceHead {
            space_id: space.clone(),
            source_id: source_id.clone(),
            generation: generation.clone(),
            desired_revision: Some(revision_id.clone()),
            published_revision: Some(revision_id.clone()),
            operation_id: operation_id.clone(),
            lifecycle: SourceVisibility::Active,
            updated_at: now,
        };
        let (completion, stored_operation) = self.assemble_group_completion(
            context,
            manifest,
            operation,
            manifest_digest,
            &commit_id,
        )?;
        Ok(GroupReceipts {
            result,
            head,
            completion,
            stored_operation,
        })
    }
    fn assemble_group_completion(
        &self,
        context: &GroupAssembly<'_>,
        manifest: &SourcePublicationManifest,
        operation: OperationReceipt,
        manifest_digest: String,
        commit_id: &str,
    ) -> StorageResult<(SourcePublicationCompletion, StoredOperation)> {
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical proof missing"))?;
        let space = &self.input.space_id;
        let source = &context.source.source;
        let request_fingerprint = &context.source.request_fingerprint;
        let source_input_fingerprint = &context.source.source_input_fingerprint;
        let publication = &context.source.publication;
        let accepted = &context.source.accepted;
        let now = context.now;
        let completion = SourcePublicationCompletion {
            historical_embedding_coverage: None,
            space_id: space.clone(),
            publication: publication.clone(),
            manifest_digest,
            commit_id: commit_id.to_owned(),
            rendered_revision_fingerprints: manifest
                .documents
                .iter()
                .map(|m| {
                    (
                        m.doc_revision_id.clone(),
                        proof.documents[&m.doc_id].revision_fingerprint.clone(),
                    )
                })
                .collect(),
            document_fingerprints: proof
                .documents
                .iter()
                .map(|(id, p)| (id.clone(), p.document_fingerprint.clone()))
                .collect(),
            accepted_source_fingerprint: Some(source_support_fingerprint(accepted)?),
            published_progress: None,
            completed_at: now,
        };
        let stored_operation = StoredOperation {
            receipt: operation,
            request_fingerprint: request_fingerprint.clone(),
            input_fingerprint: source_input_fingerprint.clone(),
            authorization: SourceAuthorizationEnvelope::from(source),
            contact_digest: None,
            producer_origin: elephant_types::publication::SourceProducerOrigin::UnknownOriginal,
            predecessor_publication: None,
        };
        Ok((completion, stored_operation))
    }
    fn assemble_group_controls(
        &self,
        context: &GroupAssembly<'_>,
        manifest: &SourcePublicationManifest,
        receipts: &GroupReceipts,
        rows: &mut HistoricalDocumentEffects,
    ) -> StorageResult<Vec<Control>> {
        let commit_id = receipts.result.commit_id.clone();
        let space = &self.input.space_id;
        let source_id = &context.source.source_id;
        let source = &context.source.source;
        let previous_source = &context.source.previous_source;
        let revision_id = &context.source.revision_id;
        let request_fingerprint = &context.source.request_fingerprint;
        let publication = &context.source.publication;
        let now = context.now;
        rows.before_after.push((
            previous_source
                .clone()
                .map(|source| CanonicalRecord::Source(Box::new(source))),
            CanonicalRecord::Source(Box::new(source.clone())),
        ));
        let canonical = CanonicalCommitReceipt {
            historical_import: Some(HistoricalCommitProvenance::for_manifest(manifest)?),
            space_id: space.clone(),
            commit_id,
            input_fingerprint: request_fingerprint.clone(),
            plan_fingerprint: manifest.digest()?,
            replay_output: Some(
                serde_json::to_value(&receipts.result)
                    .map_err(|_| integrity("historical receipt encoding failed"))?,
            ),
            source_ids: vec![source_id.clone()],
            publication_authority: Some(publication.clone()),
            maintenance_authority: vec![],
            source_id: Some(source_id.clone()),
            source_revision: Some(revision_id.clone()),
            effect_ids: rows
                .before_after
                .iter()
                .map(|(_, after)| after.base().id.clone())
                .collect(),
            extension_ids: vec![],
            outbox_ids: vec![],
            committed_at: now,
        };
        self.assemble_base_controls(context, manifest, receipts, &canonical)
    }
    fn assemble_base_controls(
        &self,
        context: &GroupAssembly<'_>,
        manifest: &SourcePublicationManifest,
        receipts: &GroupReceipts,
        canonical: &CanonicalCommitReceipt,
    ) -> StorageResult<Vec<Control>> {
        let commit_id = &receipts.result.commit_id;
        let space = &self.input.space_id;
        let source_id = &context.source.source_id;
        let source = &context.source.source;
        let revision_id = &context.source.revision_id;
        let source_input_fingerprint = &context.source.source_input_fingerprint;
        let operation_id = &context.source.operation_id;
        let publication = &context.source.publication;
        let accepted = &context.source.accepted;
        let external_source_id = context.external_source_id;
        let key = manifest_key(space, publication);
        let controls = vec![
            Control::new("source_head", head_key(space, source_id), &receipts.head)?,
            Control::new(
                "source_identity",
                external_identity_key(space, source.type_, external_source_id),
                &SourceIdentity {
                    source_id: source_id.clone(),
                },
            )?,
            Control::new(
                "source_revision",
                revision_key(space, source_id, revision_id),
                &StoredRevision {
                    revision: accepted.clone(),
                    operation_id: operation_id.clone(),
                    input_fingerprint: source_input_fingerprint.clone(),
                },
            )?,
            Control::new(
                "source_operation",
                operation_id.clone(),
                &receipts.stored_operation,
            )?,
            Control::new(
                "source_authorization",
                head_key(space, source_id),
                &receipts.stored_operation.authorization,
            )?,
            Control::new(MANIFEST_TABLE, key.clone(), manifest)?,
            Control::new(COMPLETION_TABLE, key, &receipts.completion)?,
            Control::new(
                COMPLETION_TABLE,
                stable_key(&[
                    "published-source-completion/v1",
                    space,
                    &source_id.to_string(),
                ]),
                &receipts.completion,
            )?,
            Control::new(
                "source_commit_receipt",
                stable_key(&[space, commit_id]),
                canonical,
            )?,
        ];
        Ok(controls)
    }
    fn append_group_membership_controls(
        &self,
        context: &GroupAssembly<'_>,
        manifest: &SourcePublicationManifest,
        graph: &[CanonicalRecord],
        commit_id: &str,
        controls: &mut Vec<Control>,
    ) -> StorageResult<()> {
        let space = &self.input.space_id;
        let source_id = &context.source.source_id;
        let revision_id = &context.source.revision_id;
        for record in graph {
            let support = crate::canonical_commit::SourceEffectSupport {
                space_id: space.clone(),
                source_id: source_id.clone(),
                source_revision: revision_id.clone(),
                record_id: record.base().id.clone(),
                independent: false,
                historical_unknown: Some(HistoricalUnknownOwnership {
                    snapshot_sha256: self.input.snapshot_sha256.clone(),
                    plan_id: self.input.plan_id.clone(),
                    commit_id: commit_id.to_owned(),
                }),
                record_fingerprint: record.fingerprint()?,
                last_commit_id: Some(commit_id.to_owned()),
            };
            controls.push(Control::new(
                "source_effect_support",
                crate::canonical_commit::support_key(
                    space,
                    source_id,
                    revision_id,
                    &support.record_id,
                ),
                &support,
            )?);
        }
        for mapping in &manifest.documents {
            controls.push(Control::new(
                DOCUMENT_TABLE,
                stable_key(&[space, &mapping.doc_id.to_string()]),
                mapping,
            )?);
            controls.push(Control::new(
                REVISION_TABLE,
                stable_key(&[space, &mapping.doc_revision_id.to_string()]),
                mapping,
            )?);
        }
        Ok(())
    }
    async fn commit_prepared_group(
        &self,
        context: &GroupAssembly<'_>,
        config: &SpaceConfig,
        rows: &HistoricalDocumentEffects,
        mut controls: Vec<Control>,
        observed_epoch: Option<String>,
        result: HistoricalAdoptionReceipt,
    ) -> StorageResult<HistoricalCommittedGroup> {
        let progress = self.prepare_regeneration_control(
            &context.source.source_id,
            Some(context.external_source_id),
            &result.commit_id,
            observed_epoch.as_deref(),
        )?;
        if let Some((control, _)) = &progress {
            controls.push(control.clone());
        }
        let after_epoch = progress
            .as_ref()
            .map(|(_, receipt)| {
                serde_json::from_value::<regeneration::HistoricalRegenerationProgress>(
                    receipt
                        .replay_output
                        .clone()
                        .ok_or_else(|| integrity("historical S1 progress absent"))?,
                )
                .map(|state| state.after_epoch)
                .map_err(|_| integrity("historical S1 progress invalid"))
            })
            .transpose()?;
        let after_epoch = after_epoch.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        let control_before = self.catchup_control_before(&controls).await?;
        let native_effects = self
            .commit_group(HistoricalGroupCommit {
                config,
                source: &context.source.source_id,
                dependencies: &rows.dependencies,
                effects: &rows.before_after,
                controls: &controls,
                control_before: &control_before,
                epoch: observed_epoch.clone(),
                after_epoch: Some(&after_epoch),
                progress: progress.as_ref().map(|(_, receipt)| receipt),
            })
            .await?;
        if let Some(population) = self.population.get() {
            population.advance_epoch(observed_epoch.as_deref(), &after_epoch)?;
        }
        if let Some((_, receipt)) = &progress {
            self.retain_regeneration_commit(receipt)?;
        }
        Ok(HistoricalCommittedGroup {
            receipt: result,
            native_effects,
        })
    }
    fn original_group_source<'a>(
        &self,
        group: &HistoricalSourceGroup,
        documents: &[HistoricalOriginalDocument],
        original_sources_input: &'a [Source],
        external_source_id: &str,
        now: chrono::DateTime<Utc>,
    ) -> StorageResult<(BTreeMap<RecordId, &'a Source>, RecordId, Source)> {
        let space = &self.input.space_id;
        let first = documents
            .first()
            .ok_or_else(|| integrity("empty historical group"))?;
        let original_sources: BTreeMap<_, _> = original_sources_input
            .iter()
            .map(|source| (source.base.id.clone(), source))
            .collect();
        let expected_sources: BTreeSet<_> = documents
            .iter()
            .filter_map(|row| row.document.source.as_ref().map(|link| link.id().clone()))
            .collect();
        if original_sources.len() != original_sources_input.len()
            || original_sources.keys().cloned().collect::<BTreeSet<_>>() != expected_sources
            || original_sources_input
                .iter()
                .any(|source| source.base.space_id != *space || source.base.deleted_at.is_some())
        {
            return Err(integrity("historical original source closure differs"));
        }
        // The external group is a new explicit identity. Original source rows
        // remain intact even when many originals contribute to one group or one
        // original is shared by multiple external groups.
        let source_id = RecordId::new(
            TableName::Source,
            uuid::Uuid::new_v5(
                &uuid::Uuid::NAMESPACE_OID,
                format!("historical-source:{space}:{external_source_id}").as_bytes(),
            ),
        );
        if original_sources.contains_key(&source_id) {
            return Err(integrity(
                "historical grouped source collides with original",
            ));
        }
        let mut base = BaseFields::new(
            source_id.clone(),
            space.clone(),
            "historical-adoption-v1".into(),
        );
        base.security = first.document.base.security.clone();
        base.subjects.clone_from(&first.document.base.subjects);
        base.subjects_state = first.document.base.subjects_state;
        let mut source = Source::new(
            base,
            SourceType::Manual,
            group.document_mapping_sha256.clone(),
        );
        if source.base.space_id != *space || source.base.deleted_at.is_some() {
            return Err(integrity("historical source scope or type differs"));
        }
        source.external_id = Some(external_source_id.to_owned());
        source.hash.clone_from(&group.document_mapping_sha256);
        source.base.updated_at = now;
        Ok((original_sources, source_id, source))
    }
    fn validate_existing_group_proof(
        &self,
        group: &HistoricalSourceGroup,
        proof: &HistoricalAdoptionProof,
    ) -> StorageResult<()> {
        if proof.plan_id != self.input.plan_id
            || proof.snapshot_sha256 != self.input.snapshot_sha256
            || proof.document_mapping_sha256 != group.document_mapping_sha256
            || proof.external_source_id != group.external_source_id
        {
            return Err(integrity("historical replay input changed"));
        }
        if let Some(snapshot) = &proof.snapshot_graph {
            let declaration = self
                .input
                .snapshot_groups
                .iter()
                .find(|candidate| candidate.external_source_id == group.external_source_id)
                .ok_or_else(|| integrity("historical snapshot replay undeclared"))?;
            if snapshot.original_rows_sha256 != declaration.original_rows_sha256 {
                return Err(integrity(
                    "historical snapshot replay row population differs",
                ));
            }
        } else if self.snapshot_index.contains_key(&group.external_source_id) {
            return Err(integrity("historical snapshot replay proof missing"));
        }
        Ok(())
    }
    async fn verify_existing_group(
        &self,
        group: &HistoricalSourceGroup,
        operation: &OperationReceipt,
    ) -> StorageResult<HistoricalAdoptionReceipt> {
        let publication = PublicationFence {
            source_id: operation.source_id.clone(),
            generation: operation.source_generation.clone(),
            revision_id: operation
                .desired_revision
                .clone()
                .ok_or_else(|| integrity("historical operation revision missing"))?,
            operation_id: operation.operation_id.clone(),
        };
        let manifest =
            crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                self.client.clone(),
            )
            .get(&self.input.space_id, &publication)
            .await?
            .ok_or_else(|| integrity("historical replay manifest missing"))?;
        manifest.validate()?;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical replay proof missing"))?;
        self.validate_existing_group_proof(group, proof)?;
        let commit = regeneration::read_receipt_exact(
            &self.client,
            &self.input.space_id,
            &proof.commit_id(&manifest),
        )
        .await?;
        let expected_effects: BTreeSet<_> = proof
            .documents
            .keys()
            .cloned()
            .chain(std::iter::once(publication.source_id.clone()))
            .collect();
        if commit.historical_import.as_ref()
            != Some(&HistoricalCommitProvenance::for_manifest(&manifest)?)
            || commit.publication_authority.as_ref() != Some(&publication)
            || commit.source_id.as_ref() != Some(&publication.source_id)
            || commit.source_ids != [publication.source_id.clone()]
            || commit.source_revision.as_ref() != Some(&publication.revision_id)
            || commit.plan_fingerprint != manifest.digest()?
            || !commit.maintenance_authority.is_empty()
            || !commit.extension_ids.is_empty()
            || !commit.outbox_ids.is_empty()
            || commit.effect_ids.iter().cloned().collect::<BTreeSet<_>>() != expected_effects
            || commit.effect_ids.len() != expected_effects.len()
            || proof.documents.keys().collect::<BTreeSet<_>>()
                != group.documents.iter().map(|d| &d.legacy_doc_id).collect()
        {
            return Err(integrity("historical replay original commit differs"));
        }
        let head = SourcePublicationRepository::new(self.client.clone())
            .head(&self.input.space_id, &operation.source_id)
            .await?
            .ok_or_else(|| integrity("historical replay head missing"))?;
        if head.generation != publication.generation
            || head.operation_id != publication.operation_id
            || head.lifecycle != SourceVisibility::Active
        {
            return Err(integrity("historical replay superseded"));
        }
        let receipt: HistoricalAdoptionReceipt = serde_json::from_value(
            commit
                .replay_output
                .ok_or_else(|| integrity("historical replay output missing"))?,
        )
        .map_err(|_| integrity("historical replay output invalid"))?;
        let expected_content: BTreeMap<_, _> = group
            .documents
            .iter()
            .map(|doc| (doc.legacy_doc_id.clone(), doc.doc_content_sha256.clone()))
            .collect();
        if receipt.source_space_id != self.input.source_space_id
            || receipt.space_id != self.input.space_id
            || receipt.source_id != publication.source_id
            || receipt.external_source_id != group.external_source_id
            || receipt.accepted_revision != publication.revision_id
            || receipt.accepted_request_fingerprint != commit.input_fingerprint
            || receipt.document_mapping_sha256 != group.document_mapping_sha256
            || receipt.document_content_sha256 != expected_content
            || receipt.manifest_digest != manifest.digest()?
            || receipt.original_snapshot_sha256 != self.input.snapshot_sha256
            || receipt.commit_id != proof.commit_id(&manifest)
            || receipt.operation.source_id != publication.source_id
            || receipt.operation.source_generation != publication.generation
            || receipt.operation.operation_id != publication.operation_id
            || receipt.operation.desired_revision.as_ref() != Some(&publication.revision_id)
        {
            return Err(integrity("historical replay receipt population differs"));
        }
        Ok(receipt)
    }

    async fn commit_group(
        &self,
        request: HistoricalGroupCommit<'_>,
    ) -> StorageResult<Vec<HistoricalRowEffect>> {
        let config = request.config;
        let effects = request.effects;
        self.check_custody()?;
        let space = &self.input.space_id;
        // Keep unchanged rows in the exact guarded inventory, but do not issue
        // a write or count its events. SurrealDB skips every table event when
        // the persisted before/after images are equal (doc/event.rs).
        let effect_writes = effects
            .iter()
            .map(|(before, after)| {
                Ok(match before {
                    Some(before) => {
                        crate::source_publication::encode_payload(before)?
                            != crate::source_publication::encode_payload(after)?
                    }
                    None => true,
                })
            })
            .collect::<StorageResult<Vec<_>>>()?;
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| self.commit_group_tx(tx, &request, &effect_writes))
                .await;
        }
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        let mut bindings = crate::bindings! { "space"=>space.clone(), "historical_epoch_id"=>control_thing("canonical_epoch",space), "historical_epoch"=>request.epoch.clone().map_or(surrealdb::sql::Value::None,Into::into), "historical_config_id"=>crate::surreal::thing_from_record_id(&config.base.id), "historical_config"=>crate::canonical_commit::native_expected_fields(crate::surreal::space_config_to_value(config)?) };
        sql.push_str("IF (SELECT VALUE epoch FROM ONLY $historical_epoch_id) != $historical_epoch { THROW 'historical epoch changed'; }; IF !recall::original_row_matches((SELECT * FROM ONLY $historical_config_id), $space, $historical_config) { THROW 'historical configuration changed'; };\n");
        self.append_group_before_guards(&request, &mut sql, &mut bindings)?;
        self.append_group_event_counts(&request, &mut sql, &mut bindings, &effect_writes)?;
        let inventory = self.append_group_inventory(&request, &mut sql, &mut bindings)?;
        let durable_effects =
            self.append_group_durable_effects(&request, &mut sql, &mut bindings, &inventory)?;
        Self::append_group_writes(&request, &mut sql, &mut bindings, &effect_writes)?;
        self.append_group_after_guards(&request, &mut sql, &mut bindings, &inventory)?;
        if let Some((prepared, _)) = &durable_effects {
            crate::historical_native_effects::finish(&mut sql, prepared)?;
            sql.push_str("COMMIT TRANSACTION;");
        } else {
            sql.push_str("RETURN [");
            for (index, (table, _)) in inventory.iter().enumerate() {
                if index > 0 {
                    sql.push(',');
                }
                bindings.insert(
                    format!("historical_inventory_{index}_table"),
                    table.clone().into(),
                );
                write!(sql,"{{ table:$historical_inventory_{index}_table,id:type::string($historical_inventory_{index}_id),before_sha256:$historical_inventory_{index}_before,after_sha256:(SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $historical_inventory_{index}_id) }}").map_err(|_|integrity("historical inventory SQL"))?;
            }
            sql.push_str("];\nCOMMIT TRANSACTION;");
        }
        self.check_custody()?;
        let mut response = self.client.query_bind_checked(&sql, bindings).await?;
        decode_group_effects(&mut response, &inventory, durable_effects)
    }

    fn commit_group_tx(
        &self,
        tx: &mut crate::EmbeddedMutation<'_>,
        request: &HistoricalGroupCommit<'_>,
        effect_writes: &[bool],
    ) -> StorageResult<Vec<HistoricalRowEffect>> {
        let after_epoch = request.after_epoch;
        let config = request.config;
        let control_before = request.control_before;
        let controls = request.controls;
        let dependencies = request.dependencies;
        let effects = request.effects;
        let epoch = request.epoch.clone();
        let source = request.source;
        let space = &self.input.space_id;
        self.check_custody()?;
        if crate::canonical_commit::epoch_tx(tx, space)? != epoch
            || tx
                .get::<SpaceConfig>(space, &config.base.id, false)?
                .as_ref()
                != Some(config)
        {
            return Err(integrity("historical input epoch/configuration changed"));
        }
        for row in dependencies {
            if row
                .current(tx)?
                .as_ref()
                .map(crate::source_publication::encode_payload)
                .transpose()?
                != Some(crate::source_publication::encode_payload(row)?)
            {
                return Err(integrity("historical dependency changed"));
            }
        }
        for (before, after) in effects {
            if after
                .current(tx)?
                .as_ref()
                .map(crate::source_publication::encode_payload)
                .transpose()?
                != before
                    .as_ref()
                    .map(crate::source_publication::encode_payload)
                    .transpose()?
            {
                return Err(integrity("historical before-image changed"));
            }
        }
        for control in controls {
            let expected = control_before.get(&(control.table.into(), control.key.clone()));
            let actual = tx.extension_row_by_key(
                control.table,
                &format!("{}:{}", control.table, control.key),
            )?;
            match (expected, actual) {
                (None, None) => (),
                (
                    Some(super::catchup::HistoricalControlImage::Embedded(expected)),
                    Some(actual),
                ) if expected == &actual => (),
                _ => return Err(integrity("historical publication full control changed")),
            }
        }
        for ((before, after), write) in effects.iter().zip(effect_writes) {
            if *write {
                after.put(tx, before.is_none())?;
            }
        }
        for control in controls {
            let mut value = control.value(space, source);
            value["id"] = serde_json::Value::String(format!("{}:{}", control.table, control.key));
            tx.upsert_extension_row(control.table, value)?;
        }
        if let Some(after_epoch) = after_epoch {
            tx.upsert_extension_row("canonical_epoch",serde_json::json!({"id":format!("canonical_epoch:{space}"),"space_id":space,"epoch":after_epoch}))?;
        } else {
            tx.advance_canonical_epoch(space)?;
        }
        Ok(Vec::new())
    }
    fn append_group_before_guards(
        &self,
        request: &HistoricalGroupCommit<'_>,
        sql: &mut String,
        bindings: &mut BTreeMap<String, surrealdb::sql::Value>,
    ) -> StorageResult<()> {
        let control_before = request.control_before;
        let controls = request.controls;
        let dependencies = request.dependencies;
        let effects = request.effects;
        let source = request.source;
        let space = &self.input.space_id;
        for (index, row) in dependencies.iter().enumerate() {
            append_row_guard(
                sql,
                bindings,
                &format!("historical_dep_{index}"),
                row,
                Some(row),
            )?;
        }
        for (index, (before, after)) in effects.iter().enumerate() {
            let prefix = format!("historical_effect_{index}");
            append_row_guard(sql, bindings, &prefix, after, before.as_ref())?;
            bindings.insert(format!("{prefix}_after"), after.native()?);
        }
        for (index, control) in controls.iter().enumerate() {
            bindings.insert(
                format!("historical_control_{index}_id"),
                control_thing(control.table, &control.key).into(),
            );
            bindings.insert(
                format!("historical_control_{index}_value"),
                crate::surreal::json_to_surreal(control.value(space, source))?,
            );
            let expected = match control_before.get(&(control.table.into(), control.key.clone())) {
                None => surrealdb::sql::Value::None,
                Some(super::catchup::HistoricalControlImage::Native(value)) => value.clone(),
                Some(_) => return Err(integrity("historical control backend differs")),
            };
            bindings.insert(format!("historical_control_{index}_before"), expected);
            writeln!(sql,"IF (SELECT * FROM ONLY $historical_control_{index}_id) != $historical_control_{index}_before {{ THROW 'historical publication full control changed'; }};").map_err(|_| integrity("historical SQL encoding failed"))?;
        }
        Ok(())
    }
    fn append_group_event_counts(
        &self,
        request: &HistoricalGroupCommit<'_>,
        sql: &mut String,
        bindings: &mut BTreeMap<String, surrealdb::sql::Value>,
        effect_writes: &[bool],
    ) -> StorageResult<()> {
        let controls = request.controls;
        let effects = request.effects;
        let space = &self.input.space_id;
        let visibility_writes = controls
            .iter()
            .filter(|control| {
                matches!(
                    control.table,
                    "source_head"
                        | "source_effect_support"
                        | "source_revision_mapping"
                        | "source_publication_completion"
                        | "source_publication_manifest"
                        | "source_publication_manifest_history"
                )
            })
            .count();
        // 033 content mutations advance configuration and its reference guard.
        let content_visibility_writes = effects
            .iter()
            .zip(effect_writes)
            .filter(|((_, row), write)| {
                **write
                    && matches!(
                        row.base().id.table(),
                        TableName::Source
                            | TableName::Doc
                            | TableName::DocRevision
                            | TableName::DocNode
                            | TableName::Entity
                    )
            })
            .count();
        let visibility_writes = visibility_writes + content_visibility_writes;
        let guarded_controls = controls
            .iter()
            .filter(|control| {
                !matches!(
                    control.table,
                    "source_identity" | "source_authorization" | "source_operation"
                )
            })
            .count();
        bindings.insert(
            "historical_visibility_writes".into(),
            i64::try_from(visibility_writes)
                .map_err(|_| integrity("historical visibility count"))?
                .into(),
        );
        bindings.insert(
            "historical_guard_writes".into(),
            i64::try_from(
                effect_writes.iter().filter(|write| **write).count()
                    + guarded_controls
                    + visibility_writes,
            )
            .map_err(|_| integrity("historical reference count"))?
            .into(),
        );
        bindings.insert(
            "historical_guard_id".into(),
            control_thing("source_erasure_guard", space).into(),
        );
        sql.push_str("LET $historical_guard_before = SELECT * FROM ONLY $historical_guard_id; LET $historical_config_before = SELECT * FROM ONLY $historical_config_id;\n");
        Ok(())
    }
    fn append_group_inventory(
        &self,
        request: &HistoricalGroupCommit<'_>,
        sql: &mut String,
        bindings: &mut BTreeMap<String, surrealdb::sql::Value>,
    ) -> StorageResult<NativeGroupInventory> {
        let config = request.config;
        let controls = request.controls;
        let effects = request.effects;
        let space = &self.input.space_id;
        let mut inventory: Vec<(String, surrealdb::sql::Thing)> = effects
            .iter()
            .map(|(_, after)| {
                (
                    after.base().id.table().as_str().to_owned(),
                    crate::surreal::thing_from_record_id(&after.base().id),
                )
            })
            .chain(controls.iter().map(|control| {
                (
                    control.table.to_owned(),
                    control_thing(control.table, &control.key),
                )
            }))
            .chain(std::iter::once((
                "canonical_epoch".into(),
                control_thing("canonical_epoch", space),
            )))
            .collect();
        let mut physical_fences = BTreeSet::new();
        for (table, id) in &inventory {
            if matches!(
                table.as_str(),
                "doc" | "source_head" | "source_publication_manifest"
            ) {
                let bucket = sha256(id.to_string().as_bytes());
                physical_fences.insert(format!("{table}:{}", &bucket[..3]));
            }
        }
        let support_index_id = surrealdb::sql::Thing::from((
            "canonical_epoch",
            surrealdb::sql::Id::Array(surrealdb::sql::Array::from(vec![
                surrealdb::sql::Value::from("recall_source_support_index"),
                surrealdb::sql::Value::from(1i64),
            ])),
        ));
        inventory.push(("canonical_epoch".into(), support_index_id.clone()));
        bindings.insert(
            "historical_support_index_id".into(),
            support_index_id.into(),
        );
        sql.push_str("IF !fn::recall_support_index_fence_valid((SELECT * FROM ONLY $historical_support_index_id)) { THROW 'historical support index fence invalid'; };\n");
        inventory.push((
            "space_config".into(),
            crate::surreal::thing_from_record_id(&config.base.id),
        ));
        inventory.push((
            "source_erasure_guard".into(),
            control_thing("source_erasure_guard", space),
        ));
        inventory.extend(physical_fences.iter().map(|key| {
            (
                "native_read_fence".into(),
                control_thing("native_read_fence", key),
            )
        }));
        for (index, (_, id)) in inventory.iter().enumerate() {
            bindings.insert(
                format!("historical_inventory_{index}_id"),
                id.clone().into(),
            );
            writeln!(sql,"LET $historical_inventory_{index}_before = (SELECT VALUE crypto::sha256(type::string($this)) FROM ONLY $historical_inventory_{index}_id);").map_err(|_|integrity("historical inventory SQL"))?;
        }
        Ok(inventory)
    }
    fn append_group_durable_effects(
        &self,
        request: &HistoricalGroupCommit<'_>,
        sql: &mut String,
        bindings: &mut BTreeMap<String, surrealdb::sql::Value>,
        inventory: &NativeGroupInventory,
    ) -> StorageResult<DurableGroupEffects> {
        let progress = request.progress;
        let source = request.source;
        let space = &self.input.space_id;
        let durable_effects = progress
            .map(|primary| {
                let state: regeneration::HistoricalRegenerationProgress = serde_json::from_value(
                    primary
                        .replay_output
                        .clone()
                        .ok_or_else(|| integrity("historical effects progress absent"))?,
                )
                .map_err(|_| integrity("historical effects progress invalid"))?;
                let binding = crate::historical_native_effects::HistoricalNativeEffectBinding::new(
                    &state.authority_binding_sha256,
                    "regeneration",
                    state.ordinal,
                    primary,
                    inventory,
                )?;
                let effect_id = control_thing(
                    "source_commit_receipt",
                    &stable_key(&[
                        space,
                        &stable_key(&[
                            "historical-native-effects-v1",
                            &state.authority_binding_sha256,
                            "regeneration",
                            &state.ordinal.to_string(),
                        ]),
                    ]),
                );
                let prepared = crate::historical_native_effects::begin(
                    sql,
                    bindings,
                    source,
                    binding,
                    primary,
                    inventory.clone(),
                )?;
                Ok::<_, StorageError>((prepared, effect_id))
            })
            .transpose()?;
        Ok(durable_effects)
    }
    fn append_group_writes(
        request: &HistoricalGroupCommit<'_>,
        sql: &mut String,
        bindings: &mut BTreeMap<String, surrealdb::sql::Value>,
        effect_writes: &[bool],
    ) -> StorageResult<()> {
        let after_epoch = request.after_epoch;
        let control_before = request.control_before;
        let controls = request.controls;
        let effects = request.effects;
        for (index, (before, _)) in effects.iter().enumerate() {
            if !effect_writes[index] {
                continue;
            }
            let verb = if before.is_some() { "UPDATE" } else { "CREATE" };
            writeln!(
                sql,
                "{verb} $historical_effect_{index}_id CONTENT $historical_effect_{index}_after RETURN NONE;"
            )
            .map_err(|_| integrity("historical SQL encoding failed"))?;
        }
        for (index, control) in controls.iter().enumerate() {
            let verb = if control_before.contains_key(&(control.table.into(), control.key.clone()))
            {
                "UPDATE"
            } else {
                "CREATE"
            };
            writeln!(
                sql,
                "{verb} $historical_control_{index}_id CONTENT $historical_control_{index}_value RETURN NONE;"
            )
            .map_err(|_| integrity("historical SQL encoding failed"))?;
        }
        bindings.insert(
            "historical_after_epoch".into(),
            after_epoch
                .map_or_else(|| uuid::Uuid::new_v4().to_string(), str::to_owned)
                .into(),
        );
        sql.push_str(
            "UPSERT $historical_epoch_id SET space_id=$space, epoch=$historical_after_epoch RETURN NONE;\n",
        );
        Ok(())
    }
    fn append_group_after_guards(
        &self,
        request: &HistoricalGroupCommit<'_>,
        sql: &mut String,
        bindings: &mut BTreeMap<String, surrealdb::sql::Value>,
        inventory: &NativeGroupInventory,
    ) -> StorageResult<()> {
        let controls = request.controls;
        let effects = request.effects;
        let source = request.source;
        let space = &self.input.space_id;
        // Validate exact native after-images inside the same transaction before
        // selecting inventory hashes. Database-generated fields stay represented.
        for (index, (_, after)) in effects.iter().enumerate() {
            bindings.insert(
                format!("historical_inventory_after_{index}"),
                crate::canonical_commit::native_expected_fields(after.native()?),
            );
            writeln!(sql,"IF !recall::original_row_matches((SELECT * FROM ONLY $historical_effect_{index}_id),$space,$historical_inventory_after_{index}) {{ THROW 'historical after-image differs'; }};").map_err(|_|integrity("historical after-image SQL"))?;
        }
        for (index, control) in controls.iter().enumerate() {
            let mut value = control.value(space, source);
            value["id"] = serde_json::Value::Null;
            let mut native = crate::surreal::json_to_surreal(value)?;
            if let surrealdb::sql::Value::Object(ref mut object) = native {
                object.insert(
                    "id".into(),
                    control_thing(control.table, &control.key).into(),
                );
            }
            bindings.insert(
                format!("historical_control_{index}_expected"),
                crate::canonical_commit::native_expected_fields(native),
            );
            writeln!(sql,"IF !recall::original_row_matches((SELECT * FROM ONLY $historical_control_{index}_id),$space,$historical_control_{index}_expected) {{ THROW 'historical control after-image differs: {}'; }};", control.table).map_err(|_|integrity("historical control SQL"))?;
        }
        sql.push_str("IF !fn::recall_support_index_fence_valid((SELECT * FROM ONLY $historical_support_index_id)) { THROW 'historical support index fence invalid'; };\n");
        append_generated_guard_check(sql);
        sql.push_str("LET $historical_config_after=SELECT * FROM ONLY $historical_config_id; IF object::keys($historical_config_after)!=object::keys($historical_config_before) OR object::keys($historical_config_after.processing)!=object::keys($historical_config_before.processing) OR $historical_config_after.processing.index_generation!=$historical_config_before.processing.index_generation+$historical_visibility_writes OR !type::is::datetime($historical_config_after.updated_at) { THROW 'historical generated configuration differs'; }; FOR $key IN object::keys($historical_config_before) { IF $key NOT IN ['processing','updated_at'] AND $historical_config_after[$key]!=$historical_config_before[$key] { THROW 'historical configuration payload changed'; }; }; FOR $key IN object::keys($historical_config_before.processing) { IF $key!='index_generation' AND $historical_config_after.processing[$key]!=$historical_config_before.processing[$key] { THROW 'historical processing authority changed'; }; };\n");
        for (index, (table, _)) in inventory.iter().enumerate() {
            if table == "native_read_fence" {
                writeln!(sql,"IF !fn::recall_physical_read_fence_valid((SELECT * FROM ONLY $historical_inventory_{index}_id)) {{ THROW 'historical generated physical fence invalid'; }};").map_err(|_|integrity("historical fence SQL"))?;
            }
        }
        Ok(())
    }
}

fn decode_group_effects(
    response: &mut surrealdb::Response,
    inventory: &NativeGroupInventory,
    durable_effects: DurableGroupEffects,
) -> StorageResult<Vec<HistoricalRowEffect>> {
    let result_index = response
        .num_statements()
        .checked_sub(1)
        .ok_or_else(|| integrity("historical inventory result absent"))?;
    let result: Vec<crate::historical_native_effects::HistoricalNativeEffect> = response
        .take(result_index)
        .map_err(|_| integrity("historical inventory result decode"))?;
    let mut expected: BTreeMap<String, String> = inventory
        .iter()
        .map(|(table, id)| (id.to_string(), table.clone()))
        .collect();
    if expected.len() != inventory.len() {
        return Err(integrity("historical inventory duplicate identity"));
    }
    if let Some((_, effect_id)) = durable_effects {
        if expected
            .insert(effect_id.to_string(), "source_commit_receipt".into())
            .is_some()
        {
            return Err(integrity("historical effect receipt collision"));
        }
    }
    let mut converted = Vec::with_capacity(result.len());
    for row in result {
        if expected.remove(&row.id).as_ref() != Some(&row.table)
            || row
                .before_sha256
                .as_ref()
                .is_some_and(|value| !valid_digest(value))
        {
            return Err(integrity("historical committed inventory result differs"));
        }
        let after_sha256 = row
            .after_sha256
            .filter(|value| valid_digest(value))
            .ok_or_else(|| integrity("historical committed after-image absent"))?;
        converted.push(HistoricalRowEffect {
            table: row.table,
            id: row.id,
            before_sha256: row.before_sha256,
            after_sha256,
        });
    }
    if !expected.is_empty() {
        return Err(integrity("historical committed inventory incomplete"));
    }
    let result = converted;
    Ok(result)
}

/// 039 changes only the revision of the exact per-space guard. Membership
/// publication is not an adoption effect; all custody and membership fields stay.
fn append_generated_guard_check(sql: &mut String) {
    sql.push_str("LET $historical_guard_after=SELECT * FROM ONLY $historical_guard_id; IF $historical_guard_after=NONE { THROW 'historical generated reference guard absent'; }; IF $historical_guard_before=NONE { LET $fingerprints=recall::reference_tokens($__recall_journal_identity,'__recall_custody__',$__recall_reference_key,[],type::thing('recall_reference_key',$__recall_journal_identity)); IF object::len($historical_guard_after)!=7 OR $historical_guard_after.id!=$historical_guard_id OR $historical_guard_after.space_id!=$space OR $historical_guard_after.journal_id!=$__recall_journal_identity OR $historical_guard_after.key_fingerprint!=$fingerprints[0] OR $historical_guard_after.version!=1 OR $historical_guard_after.membership_count!=0 OR $historical_guard_after.revision!=$historical_guard_writes { THROW 'historical new reference guard differs'; }; } ELSE { IF object::keys($historical_guard_after)!=object::keys($historical_guard_before) OR $historical_guard_after.revision!=$historical_guard_before.revision+$historical_guard_writes { THROW 'historical reference revision differs'; }; FOR $key IN object::keys($historical_guard_before) { IF $key!='revision' AND $historical_guard_after[$key]!=$historical_guard_before[$key] { THROW 'historical reference authority changed'; }; }; };\n");
}

#[path = "historical_adoption_regeneration.rs"]
mod regeneration;

pub use regeneration::{
    HistoricalGraphReplacement, HistoricalRegenerationPlan, HistoricalRegenerationProgress,
    HistoricalRegenerationStep, HistoricalRetirementReceipt, HistoricalS1SourceClosure,
};

#[path = "historical_adoption_population.rs"]
mod population;

#[path = "historical_snapshot_graph.rs"]
mod snapshot_graph;
pub use snapshot_graph::HistoricalSnapshotGraphRow;
