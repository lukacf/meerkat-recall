//! Source publication invalidation inside the distinct historical catch-up lease.
//! This never turns a partial row batch into a complete S1 publication.
use super::{historical_evidence_links, sha256, HistoricalCommitProvenance};
use crate::canonical_commit::{CanonicalCommitReceipt, CanonicalRecord, CanonicalRepository};
use crate::client::{HistoricalCatchupSource, PreparedHistoricalCatchupAuthority};
use crate::historical_delta::PreparedDeltaBatch;
use crate::source_publication::{control_thing, head_key, integrity, stable_key, StoredOperation};
use crate::source_publication_manifest::SourcePublicationManifestRepository;
use crate::{StorageClient, StorageError, StorageResult};
use chrono::Utc;
use elephant_types::publication::{OperationState, PublicationFence, SourceVisibility};
use elephant_types::{RecordId, TableName};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// Preserve the entire backend control row, including extra projections. Native
/// values must not round-trip through JSON and lose Thing/time/decimal types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoricalControlImage {
    Embedded(serde_json::Value),
    Native(surrealdb::sql::Value),
}
#[derive(Debug, Clone)]
pub struct HistoricalControlEffect {
    pub(crate) table: &'static str,
    pub(crate) key: String,
    pub(crate) before: HistoricalControlImage,
    pub(crate) after: Option<HistoricalControlImage>,
}
#[derive(Debug)]
pub struct PreparedHistoricalEffects {
    pub(crate) controls: Vec<HistoricalControlEffect>,
    /// Cached exact immutable originals for affected groups; compare only, never rewrite.
    pub(crate) original_receipts: Vec<HistoricalControlEffect>,
    /// Exact declared S0/S1 rows projected into the current imported namespace.
    pub(crate) projected_rows: Vec<crate::historical_delta::HistoricalDeltaRow>,
    /// Legacy vectors stay under their original model/identity; only namespace projects.
    pub(crate) projected_embeddings: Vec<crate::historical_delta::HistoricalEmbeddingDelta>,
    pub(crate) batch_digest: String,
    pub(crate) expected_epoch: String,
}

/// Constructed once under original catch-up custody. Every original manifest is
/// read once; batches use the reverse map rather than rescanning the corpus.
#[derive(Debug)]
pub struct PreparedHistoricalEffectIndex {
    space: String,
    before_snapshot: String,
    source_space: String,
    documents: BTreeMap<RecordId, (RecordId, Option<RecordId>)>,
    memberships: BTreeMap<RecordId, BTreeSet<RecordId>>,
    bindings: BTreeMap<RecordId, HistoricalCatchupSource>,
    original_receipts: BTreeMap<RecordId, HistoricalControlEffect>,
    /// Identity bound by every original manifest and its immutable commit receipt.
    original_configuration_id: RecordId,
}

pub async fn full_control(
    client: &StorageClient,
    table: &'static str,
    key: &str,
    space: &str,
) -> StorageResult<HistoricalControlImage> {
    if let Some(store) = client.embedded_store() {
        let physical = format!("{table}:{key}");
        let row = store
            .extension_row_by_key(table, &physical)
            .await?
            .ok_or_else(|| integrity("historical catch-up original control absent"))?;
        if row.get("id").and_then(serde_json::Value::as_str) != Some(physical.as_str())
            || row.get("space_id").and_then(serde_json::Value::as_str) != Some(space)
        {
            return Err(integrity("historical control identity differs"));
        }
        return Ok(HistoricalControlImage::Embedded(row));
    }
    let mut result = client
        .query_bind_checked(
            "SELECT * FROM ONLY $row",
            crate::bindings! {"row"=>control_thing(table,key),"space"=>space},
        )
        .await?;
    let raw: surrealdb::Value = result
        .take(0)
        .map_err(|_| integrity("historical catch-up control decode"))?;
    let value = raw.into_inner();
    let surrealdb::sql::Value::Object(fields) = &value else {
        return Err(integrity("historical control not an object"));
    };
    if fields.get("id") != Some(&surrealdb::sql::Value::Thing(control_thing(table, key)))
        || fields.get("space_id") != Some(&surrealdb::sql::Value::from(space))
    {
        return Err(integrity("historical control identity differs"));
    }
    Ok(HistoricalControlImage::Native(value))
}
impl HistoricalControlImage {
    pub(crate) fn payload<T: serde::de::DeserializeOwned>(&self) -> StorageResult<T> {
        let payload = match self {
            Self::Embedded(value) => value
                .get("payload")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            Self::Native(surrealdb::sql::Value::Object(fields)) => match fields.get("payload") {
                Some(surrealdb::sql::Value::Strand(value)) => Some(value.as_str().to_owned()),
                _ => None,
            },
            Self::Native(_) => None,
        }
        .ok_or_else(|| integrity("historical control payload absent"))?;
        serde_json::from_str(&payload).map_err(|_| integrity("historical control payload invalid"))
    }
    pub(crate) fn replace<T: Serialize>(&self, payload: &T) -> StorageResult<Self> {
        let text =
            serde_json::to_string(payload).map_err(|_| integrity("historical control encode"))?;
        let state = serde_json::to_value(payload)
            .map_err(|_| integrity("historical control state encode"))?;
        Ok(match self {
            Self::Embedded(value) => {
                let mut value = value.clone();
                value["payload"] = text.into();
                value["state"] = state;
                Self::Embedded(value)
            }
            Self::Native(surrealdb::sql::Value::Object(fields)) => {
                let mut fields = fields.clone();
                fields.insert("payload".into(), text.into());
                fields.insert("state".into(), crate::surreal::json_to_surreal(state)?);
                Self::Native(surrealdb::sql::Value::Object(fields))
            }
            Self::Native(_) => return Err(integrity("historical control not an object")),
        })
    }
}
struct InvalidationBefore {
    key: String,
    before: HistoricalControlImage,
    head: elephant_types::publication::SourceHead,
    already_invalidated: bool,
    operation_before: HistoricalControlImage,
    operation: StoredOperation,
}

#[derive(Default)]
struct EffectIndexAssembly {
    documents: BTreeMap<RecordId, (RecordId, Option<RecordId>)>,
    source_space: Option<String>,
    memberships: BTreeMap<RecordId, BTreeSet<RecordId>>,
    exact: BTreeMap<RecordId, HistoricalCatchupSource>,
    snapshot: Option<String>,
    original_receipts: BTreeMap<RecordId, HistoricalControlEffect>,
    original_configuration_id: Option<RecordId>,
    cached_bytes: usize,
    membership_count: usize,
}

impl PreparedHistoricalEffectIndex {
    /// Bind original manifests and full evidence populations once. The caller's
    /// binding list must be the exact original authority input, not a subset.
    pub async fn prepare(
        authority: &PreparedHistoricalCatchupAuthority,
        bindings: &[HistoricalCatchupSource],
    ) -> StorageResult<Self> {
        authority.verify_source_bindings(bindings)?;
        Self::prepare_with_client(authority, bindings, authority.client()?).await
    }

    async fn prepare_with_client(
        authority: &PreparedHistoricalCatchupAuthority,
        bindings: &[HistoricalCatchupSource],
        client: &StorageClient,
    ) -> StorageResult<Self> {
        if bindings.is_empty() {
            let configuration_id = authority
                .certified_empty_baseline_configuration_id()?
                .ok_or_else(|| integrity("historical empty baseline certificate absent"))?;
            if configuration_id.table() != TableName::SpaceConfig {
                return Err(integrity(
                    "historical empty baseline configuration identity",
                ));
            }
            let (source_space, space, before_snapshot) = authority.original_population_scope()?;
            return Ok(Self {
                space: space.to_owned(),
                source_space: source_space.to_owned(),
                before_snapshot: before_snapshot.to_owned(),
                documents: BTreeMap::new(),
                memberships: BTreeMap::new(),
                bindings: BTreeMap::new(),
                original_receipts: BTreeMap::new(),
                original_configuration_id: configuration_id,
            });
        }
        if bindings.len() > 100_000 {
            return Err(integrity("historical catch-up source bound"));
        }
        let space = bindings[0].head.space_id.clone();
        let mut state = EffectIndexAssembly::default();
        for binding in bindings {
            let manifest = Self::original_binding_manifest(
                client,
                &SourcePublicationManifestRepository::new(client.clone()),
                &space,
                binding,
                &mut state,
            )
            .await?;
            Self::retain_binding_population(binding, &manifest, &mut state)?;
        }
        authority.check_custody()?;
        let EffectIndexAssembly {
            documents,
            source_space,
            memberships,
            exact,
            snapshot,
            original_receipts,
            original_configuration_id,
            ..
        } = state;
        Ok(Self {
            space,
            before_snapshot: snapshot.ok_or_else(|| integrity("historical snapshot absent"))?,
            memberships,
            documents,
            source_space: source_space
                .ok_or_else(|| integrity("historical original space absent"))?,
            bindings: exact,
            original_receipts,
            original_configuration_id: original_configuration_id
                .ok_or_else(|| integrity("historical original configuration absent"))?,
        })
    }

    async fn original_binding_manifest(
        client: &StorageClient,
        publications: &SourcePublicationManifestRepository,
        space: &str,
        binding: &HistoricalCatchupSource,
        state: &mut EffectIndexAssembly,
    ) -> StorageResult<crate::source_publication_manifest::SourcePublicationManifest> {
        let EffectIndexAssembly {
            exact,
            original_receipts,
            cached_bytes,
            snapshot,
            ..
        } = state;
        let head = &binding.head;
        if head.space_id != space
            || head.source_id != binding.source_id
            || head.lifecycle != SourceVisibility::Active
            || exact.contains_key(&binding.source_id)
        {
            return Err(integrity("historical catch-up source binding invalid"));
        }
        // The immutable receipt remains valid after a previous delta batch
        // has unpublished the head. Current-head reads cannot resume that state.
        let receipt_key = stable_key(&[space, &binding.commit_id]);
        let image = full_control(client, "source_commit_receipt", &receipt_key, space).await?;
        let receipt: CanonicalCommitReceipt = image.payload()?;
        let image_bytes = match &image {
            HistoricalControlImage::Embedded(value) => serde_json::to_vec(value),
            HistoricalControlImage::Native(value) => serde_json::to_vec(value),
        }
        .map_err(|_| integrity("historical receipt cache encode"))?
        .len();
        *cached_bytes = cached_bytes
            .checked_add(image_bytes)
            .ok_or_else(|| integrity("historical receipt cache size overflow"))?;
        if *cached_bytes > 512 * 1024 * 1024
            || receipt.space_id != space
            || receipt.commit_id != binding.commit_id
        {
            return Err(integrity("historical receipt identity or cache bound"));
        }
        original_receipts.insert(
            binding.source_id.clone(),
            HistoricalControlEffect {
                table: "source_commit_receipt",
                key: receipt_key,
                before: image.clone(),
                after: Some(image),
            },
        );
        if sha256(
            &serde_json::to_vec(&receipt).map_err(|_| integrity("historical receipt encode"))?,
        ) != binding.receipt_sha256
        {
            return Err(integrity("historical receipt hash differs"));
        }
        let provenance = receipt
            .historical_import
            .as_ref()
            .ok_or_else(|| integrity("historical original receipt required"))?;
        if snapshot
            .as_ref()
            .is_some_and(|previous| previous != &provenance.snapshot_sha256)
        {
            return Err(integrity("historical catch-up mixed snapshots"));
        }
        *snapshot = Some(provenance.snapshot_sha256.clone());
        let publication = PublicationFence {
            source_id: binding.source_id.clone(),
            generation: head.generation.clone(),
            revision_id: head
                .published_revision
                .clone()
                .ok_or_else(|| integrity("historical original publication absent"))?,
            operation_id: head.operation_id.clone(),
        };
        let manifest = publications
            .get(space, &publication)
            .await?
            .ok_or_else(|| integrity("historical original manifest absent"))?;
        manifest.validate()?;
        if HistoricalCommitProvenance::for_manifest(&manifest)? != *provenance
            || receipt.publication_authority.as_ref() != Some(&publication)
        {
            return Err(integrity("historical manifest receipt differs"));
        }
        Ok(manifest)
    }

    fn retain_binding_population(
        binding: &HistoricalCatchupSource,
        manifest: &crate::source_publication_manifest::SourcePublicationManifest,
        state: &mut EffectIndexAssembly,
    ) -> StorageResult<()> {
        let EffectIndexAssembly {
            original_configuration_id,
            source_space,
            documents,
            memberships,
            membership_count,
            exact,
            ..
        } = state;
        let configuration_id: RecordId = manifest
            .configuration
            .config_id
            .parse()
            .map_err(|_| integrity("historical original configuration identity"))?;
        if configuration_id.table() != TableName::SpaceConfig
            || original_configuration_id
                .as_ref()
                .is_some_and(|original| original != &configuration_id)
        {
            return Err(integrity("historical original configuration differs"));
        }
        *original_configuration_id = Some(configuration_id);
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("historical original proof absent"))?;
        if source_space
            .as_ref()
            .is_some_and(|space| space != &proof.source_space_id)
        {
            return Err(integrity("historical catch-up original spaces differ"));
        }
        *source_space = Some(proof.source_space_id.clone());
        for (id, original) in &proof.documents {
            if documents
                .insert(
                    id.clone(),
                    (
                        binding.source_id.clone(),
                        original.original_source_id.clone(),
                    ),
                )
                .is_some()
            {
                return Err(integrity(
                    "historical catch-up document belongs to multiple groups",
                ));
            }
        }
        let ids = std::iter::once(&binding.source_id)
            .chain(proof.original_sources.keys())
            .chain(proof.graph_fingerprints.keys())
            .chain(
                manifest
                    .documents
                    .iter()
                    .flat_map(|doc| [&doc.doc_id, &doc.doc_revision_id]),
            )
            .chain(
                proof
                    .documents
                    .values()
                    .flat_map(|doc| doc.node_fingerprints.keys()),
            );
        for id in ids {
            if memberships
                .entry(id.clone())
                .or_default()
                .insert(binding.source_id.clone())
            {
                *membership_count += 1;
                if *membership_count > 4_000_000 {
                    return Err(integrity("historical dependency membership cache bound"));
                }
            }
        }
        exact.insert(binding.source_id.clone(), binding.clone());
        Ok(())
    }

    fn replay_typed_identities(
        &self,
        batch: &PreparedDeltaBatch,
        affected: &mut BTreeSet<RecordId>,
        ids: &mut Vec<surrealdb::sql::Thing>,
        semantic_ids: &mut BTreeSet<RecordId>,
    ) -> StorageResult<()> {
        for row in &batch.input.rows {
            semantic_ids.insert(row.id.clone());
            ids.push(crate::surreal::thing_from_record_id(&row.id));
            if let Some(sources) = self.memberships.get(&row.id) {
                affected.extend(sources.iter().cloned());
            }
            for image in row.before.iter().chain(row.after.iter()) {
                if matches!(
                    image,
                    crate::transfer_batch::TransferRecord::MediaAsset(_)
                        | crate::transfer_batch::TransferRecord::PredicateRegistry(_)
                        | crate::transfer_batch::TransferRecord::PredicateAlias(_)
                ) {
                    affected.extend(self.bindings.keys().cloned());
                }
                for reference in delta_evidence_links(image)? {
                    if let Some(sources) = self.memberships.get(&reference) {
                        affected.extend(sources.iter().cloned());
                    }
                }
            }
        }
        for row in &batch.input.embeddings {
            semantic_ids.insert(row.id.clone());
            for image in row.before.iter().chain(row.after.iter()) {
                let mut projected = image.clone();
                if projected.base.space_id != self.source_space {
                    return Err(integrity("historical replay vector namespace"));
                }
                projected.base.space_id.clone_from(&self.space);
                let (_, id, _) = crate::historical_delta_vectors::native(&projected)?;
                ids.push(id);
                let mut references = provenance_links(&projected.base)?;
                references.insert(projected.node.id().clone());
                for reference in references {
                    if let Some(sources) = self.memberships.get(&reference) {
                        affected.extend(sources.iter().cloned());
                    }
                }
            }
        }
        Ok(())
    }

    /// Reconstruct only identities justified by the immutable typed batch and
    /// original source index. Current destination heads are not replay input.
    pub(crate) async fn replay_inventory(
        &self,
        authority: &PreparedHistoricalCatchupAuthority,
        batch: &PreparedDeltaBatch,
        primary: &CanonicalCommitReceipt,
    ) -> StorageResult<Vec<(String, surrealdb::sql::Thing)>> {
        authority.check_custody()?;
        if batch.input.space_id != self.space
            || batch.input.source_space_id != self.source_space
            || batch.input.before_snapshot_sha256 != self.before_snapshot
            || primary.space_id != self.space
            || primary.plan_fingerprint != batch.digest
        {
            return Err(integrity("historical replay original index differs"));
        }
        let mut affected = BTreeSet::new();
        let mut ids = Vec::new();
        let mut semantic_ids = BTreeSet::new();
        self.replay_typed_identities(batch, &mut affected, &mut ids, &mut semantic_ids)?;
        if primary.effect_ids.len() != semantic_ids.len()
            || primary.effect_ids.iter().cloned().collect::<BTreeSet<_>>() != semantic_ids
        {
            return Err(integrity(
                "historical replay typed effect identities differ",
            ));
        }
        let operational = ordered_native_identities(
            crate::historical_delta_operations::planned_identities(batch)
                .into_iter()
                .map(|(_, id)| id)
                .collect(),
        );
        let declared = ordered_native_identities(primary.extension_ids.clone());
        if declared.len() != primary.extension_ids.len() {
            return Err(integrity("historical replay duplicate controls"));
        }
        let mut required = operational.clone();
        let mut optional = Vec::new();
        for source in affected {
            let binding = self
                .bindings
                .get(&source)
                .ok_or_else(|| integrity("historical replay source missing"))?;
            required.push(control_thing(
                "source_head",
                &head_key(&self.space, &source),
            ));
            required.push(control_thing(
                "source_operation",
                &binding.head.operation_id,
            ));
            optional.push(control_thing(
                "source_publication_completion",
                &stable_key(&[
                    "published-source-completion/v1",
                    &self.space,
                    &source.to_string(),
                ]),
            ));
            let cached = self
                .original_receipts
                .get(&source)
                .ok_or_else(|| integrity("historical replay original receipt missing"))?;
            if full_control(authority.client()?, cached.table, &cached.key, &self.space).await?
                != cached.before
            {
                return Err(integrity("historical replay original authority changed"));
            }
        }
        // Completion is removed only by the first invalidating batch; the
        // exact primary receipt plus its durable manifest binds that choice.
        let required = ordered_native_identities(required);
        let optional = ordered_native_identities(optional);
        if required
            .iter()
            .any(|id| declared.binary_search(id).is_err())
            || declared.iter().any(|id| {
                required.binary_search(id).is_err() && optional.binary_search(id).is_err()
            })
        {
            return Err(integrity(
                "historical replay source control identities differ",
            ));
        }
        ids.extend(
            declared
                .into_iter()
                .filter(|id| operational.binary_search(id).is_err()),
        );
        // Do not rediscover configuration from mutable destination projections
        // during replay: the original manifest fixes its physical identity.
        ids.push(crate::surreal::thing_from_record_id(
            &self.original_configuration_id,
        ));
        Ok(ordered_native_identities(ids)
            .into_iter()
            .map(|id| (id.tb.clone(), id))
            .collect())
    }

    pub(crate) async fn prepare_effects(
        &self,
        authority: &PreparedHistoricalCatchupAuthority,
        batch: &PreparedDeltaBatch,
    ) -> StorageResult<PreparedHistoricalEffects> {
        authority.bind_batch(batch)?;
        if batch.input.space_id != self.space
            || batch.input.before_snapshot_sha256 != self.before_snapshot
        {
            return Err(integrity("historical catch-up index differs"));
        }
        self.prepare_effects_with_client(authority, batch, authority.client()?)
            .await
    }

    async fn prepare_effects_with_client(
        &self,
        authority: &PreparedHistoricalCatchupAuthority,
        batch: &PreparedDeltaBatch,
        client: &StorageClient,
    ) -> StorageResult<PreparedHistoricalEffects> {
        if batch.input.source_space_id != self.source_space {
            return Err(integrity("historical catch-up source namespace differs"));
        }
        let mut affected = BTreeSet::new();
        let projected_rows = self.project_delta_rows(batch, &mut affected)?;
        let projected_embeddings = self.project_delta_embeddings(batch, &mut affected)?;
        let mut controls = Vec::new();
        let mut original_receipts = Vec::new();
        for id in &affected {
            self.invalidate_source_controls(client, id, &mut controls, &mut original_receipts)
                .await?;
        }
        if CanonicalRepository::new(client.clone())
            .observe_epoch(&self.space)
            .await?
            .as_deref()
            != Some(batch.input.expected_epoch.as_str())
        {
            return Err(StorageError::Conflict(
                "historical catch-up preparation epoch changed".into(),
            ));
        }
        authority.check_custody()?;
        Ok(PreparedHistoricalEffects {
            controls,
            original_receipts,
            projected_rows,
            projected_embeddings,
            batch_digest: batch.digest.clone(),
            expected_epoch: batch.input.expected_epoch.clone(),
        })
    }
    fn project_delta_rows(
        &self,
        batch: &PreparedDeltaBatch,
        affected: &mut BTreeSet<RecordId>,
    ) -> StorageResult<Vec<crate::historical_delta::HistoricalDeltaRow>> {
        let mut projected_rows = Vec::new();
        for row in &batch.input.rows {
            let mut projected = row.clone();
            for (is_before, image) in [(true, &mut projected.before), (false, &mut projected.after)]
            {
                if let Some(image) = image {
                    image.relocate_space(&self.source_space, &self.space)?;
                    if let crate::transfer_batch::TransferRecord::Doc(doc) = image {
                        if let Some((group, original_source)) = self.documents.get(&doc.base.id) {
                            if is_before
                                && doc.source.as_ref().map(elephant_types::Link::id)
                                    != original_source.as_ref()
                            {
                                return Err(integrity(
                                    "historical raw document original association differs",
                                ));
                            }
                            // Provisional current-group association remains hidden
                            // after invalidation. Whole S1 regrouping happens once
                            // after every exact raw row batch has committed.
                            doc.source = Some(elephant_types::Link::new(group.clone()));
                        }
                    }
                }
            }
            projected.before_fingerprint = projected
                .before
                .as_ref()
                .map(crate::historical_delta::runtime_row_fingerprint)
                .transpose()?;
            projected.after_fingerprint = projected
                .after
                .as_ref()
                .map(crate::historical_delta::runtime_row_fingerprint)
                .transpose()?;
            projected_rows.push(projected);
            if let Some(sources) = self.memberships.get(&row.id) {
                affected.extend(sources.iter().cloned());
            }
            // Predicate semantics and media payloads can affect groups through
            // non-record semantic keys or attachment references not present in
            // old proofs. Conservatively invalidate the complete original group
            // set; never guess an empty dependency set from an incomplete index.
            if row.before.iter().chain(row.after.iter()).any(|image| {
                matches!(
                    image,
                    crate::transfer_batch::TransferRecord::MediaAsset(_)
                        | crate::transfer_batch::TransferRecord::PredicateRegistry(_)
                        | crate::transfer_batch::TransferRecord::PredicateAlias(_)
                )
            }) {
                affected.extend(self.bindings.keys().cloned());
            }
            // New typed dependencies also invalidate their whole original group.
            for image in row.before.iter().chain(row.after.iter()) {
                for id in delta_evidence_links(image)? {
                    if let Some(sources) = self.memberships.get(&id) {
                        affected.extend(sources.iter().cloned());
                    }
                }
            }
        }
        Ok(projected_rows)
    }

    fn project_delta_embeddings(
        &self,
        batch: &PreparedDeltaBatch,
        affected: &mut BTreeSet<RecordId>,
    ) -> StorageResult<Vec<crate::historical_delta::HistoricalEmbeddingDelta>> {
        let mut projected_embeddings = Vec::new();
        for row in &batch.input.embeddings {
            let mut projected = row.clone();
            for image in [&mut projected.before, &mut projected.after]
                .into_iter()
                .flatten()
            {
                if image.base.space_id != self.source_space {
                    return Err(integrity("historical vector original namespace differs"));
                }
                image.base.space_id.clone_from(&self.space);
                let mut references = provenance_links(&image.base)?;
                references.insert(image.node.id().clone());
                for id in references {
                    if let Some(sources) = self.memberships.get(&id) {
                        affected.extend(sources.iter().cloned());
                    }
                }
            }
            projected.before_fingerprint = projected
                .before
                .as_ref()
                .map(crate::historical_delta::runtime_embedding_fingerprint)
                .transpose()?;
            projected.after_fingerprint = projected
                .after
                .as_ref()
                .map(crate::historical_delta::runtime_embedding_fingerprint)
                .transpose()?;
            // Typed validation retains finite f32 values, dimensions, identity,
            // explicit destructive intent and original model, never active-profile approval.
            projected.validate(&self.space)?;
            projected_embeddings.push(projected);
        }
        Ok(projected_embeddings)
    }

    async fn invalidation_before(
        &self,
        client: &StorageClient,
        id: &RecordId,
    ) -> StorageResult<InvalidationBefore> {
        let bound = self
            .bindings
            .get(id)
            .ok_or_else(|| integrity("historical affected source absent"))?;
        let key = head_key(&self.space, id);
        let before = full_control(client, "source_head", &key, &self.space).await?;
        let head: elephant_types::publication::SourceHead = before.payload()?;
        let already_invalidated = head.published_revision.is_none();
        if head.generation != bound.head.generation
            || head.operation_id != bound.head.operation_id
            || head.desired_revision != bound.head.desired_revision
            || head.lifecycle != bound.head.lifecycle
            || (!already_invalidated && head != bound.head)
        {
            return Err(StorageError::Conflict(
                "historical catch-up head changed".into(),
            ));
        }
        let operation_before =
            full_control(client, "source_operation", &head.operation_id, &self.space).await?;
        let operation: StoredOperation = operation_before.payload()?;
        if operation.receipt.source_id != *id
            || operation.receipt.source_generation != head.generation
            || operation.receipt.operation_id != head.operation_id
        {
            return Err(integrity("historical catch-up operation differs"));
        }
        Ok(InvalidationBefore {
            key,
            before,
            head,
            already_invalidated,
            operation_before,
            operation,
        })
    }

    async fn invalidate_source_controls(
        &self,
        client: &StorageClient,
        id: &RecordId,
        controls: &mut Vec<HistoricalControlEffect>,
        original_receipts: &mut Vec<HistoricalControlEffect>,
    ) -> StorageResult<()> {
        original_receipts.push(
            self.original_receipts
                .get(id)
                .ok_or_else(|| integrity("historical original receipt cache incomplete"))?
                .clone(),
        );
        let InvalidationBefore {
            key,
            before,
            mut head,
            already_invalidated,
            operation_before,
            mut operation,
        } = self.invalidation_before(client, id).await?;
        if already_invalidated {
            if operation.receipt.state != OperationState::Processing
                || operation.receipt.error_code.as_deref()
                    != Some("historical_delta_adoption_pending")
            {
                return Err(integrity("historical catch-up invalidation proof absent"));
            }
            // Still compare exact controls in each subsequent batch.
            controls.push(HistoricalControlEffect {
                table: "source_head",
                key,
                before: before.clone(),
                after: Some(before),
            });
            controls.push(HistoricalControlEffect {
                table: "source_operation",
                key: head.operation_id,
                before: operation_before.clone(),
                after: Some(operation_before),
            });
            return Ok(());
        }
        head.published_revision = None;
        head.updated_at = Utc::now();
        operation.receipt.state = OperationState::Processing;
        operation.receipt.published_revision = None;
        operation.receipt.error_code = Some("historical_delta_adoption_pending".into());
        operation.receipt.completed_at = None;
        controls.push(HistoricalControlEffect {
            table: "source_head",
            key,
            after: Some(before.replace(&head)?),
            before,
        });
        controls.push(HistoricalControlEffect {
            table: "source_operation",
            key: head.operation_id,
            after: Some(operation_before.replace(&operation)?),
            before: operation_before,
        });
        let lookup = stable_key(&[
            "published-source-completion/v1",
            &self.space,
            &id.to_string(),
        ]);
        let before = full_control(
            client,
            "source_publication_completion",
            &lookup,
            &self.space,
        )
        .await?;
        controls.push(HistoricalControlEffect {
            table: "source_publication_completion",
            key: lookup,
            before,
            after: None,
        });
        Ok(())
    }
}

// Match BTreeSet's native Ord equivalence and first-inserted representative.
// Stable sort retains insertion order among equal keys; dedup removes later
// equivalents. Native Thing values never pass through string or JSON coercion.
fn ordered_native_identities(mut ids: Vec<surrealdb::sql::Thing>) -> Vec<surrealdb::sql::Thing> {
    ids.sort();
    ids.dedup_by(|later, earlier| (*later).cmp(earlier).is_eq());
    ids
}

/// Operational rows cannot introduce adopted graph evidence. Supported graph
/// variants are converted explicitly; malformed typed links always fail closed.
fn delta_evidence_links(
    image: &crate::transfer_batch::TransferRecord,
) -> StorageResult<BTreeSet<RecordId>> {
    use crate::transfer_batch::TransferRecord as T;
    let canonical = match image {
        T::Doc(row) => CanonicalRecord::Doc(Box::new(row.clone())),
        T::DocRevision(row) => CanonicalRecord::DocRevision(Box::new(row.clone())),
        T::DocNode(row) => CanonicalRecord::DocNode(Box::new(row.clone())),
        T::Source(row) => CanonicalRecord::Source(Box::new(row.clone())),
        T::Entity(row) => CanonicalRecord::Entity(Box::new(row.clone())),
        T::EntityIdentityKey(row) => CanonicalRecord::EntityIdentityKey(Box::new(row.clone())),
        T::Assertion(row) => CanonicalRecord::Assertion(Box::new(row.clone())),
        T::Attribute(row) => CanonicalRecord::Attribute(Box::new(row.clone())),
        T::Rel(row) => CanonicalRecord::Rel(Box::new(row.clone())),
        T::ConflictGroup(row) => CanonicalRecord::ConflictGroup(Box::new(row.clone())),
        T::TruthSlot(row) => CanonicalRecord::TruthSlot(Box::new(row.clone())),
        T::Event(row) => CanonicalRecord::Event(Box::new(row.clone())),
        T::Freshness(row) => CanonicalRecord::Freshness(Box::new(row.clone())),
        T::AuditLog(_)
        | T::CostLedgerDaily(_)
        | T::ReprocessJob(_)
        | T::WorkItem(_)
        | T::WorkEdge(_)
        | T::WorkRun(_)
        | T::WorkArtifact(_)
        | T::WorkDeadletter(_)
        | T::SystemEvent(_)
        | T::IngestDecision(_)
        | T::WorkItemArchive(_)
        | T::WorkRunArchive(_) => return Ok(BTreeSet::new()),
        T::IdentityCandidate(row) => {
            let mut refs = provenance_links(&row.base)?;
            for entity in [&row.entity_a, &row.entity_b] {
                if entity.id().table() != TableName::Entity {
                    return Err(integrity(
                        "historical identity candidate entity type differs",
                    ));
                }
                refs.insert(entity.id().clone());
            }
            return Ok(refs);
        }
        T::EntityMerge(row) => {
            let mut refs = provenance_links(&row.base)?;
            for entity in [&row.from, &row.into] {
                if entity.id().table() != TableName::Entity {
                    return Err(integrity("historical merge entity type differs"));
                }
                refs.insert(entity.id().clone());
            }
            return Ok(refs);
        }
        T::MediaAsset(row) => return provenance_links(&row.base),
        T::PredicateRegistry(row) => return provenance_links(&row.base),
        T::PredicateAlias(row) => return provenance_links(&row.base),
    };
    historical_evidence_links(&canonical)
}

fn provenance_links(
    base: &elephant_types::records::base::BaseFields,
) -> StorageResult<BTreeSet<RecordId>> {
    if base
        .provenance
        .sources
        .iter()
        .any(|id| id.table() != TableName::Source)
    {
        return Err(integrity("historical provenance source has wrong table"));
    }
    Ok(base.provenance.sources.iter().cloned().collect())
}

#[cfg(test)]
mod tests {
    use super::delta_evidence_links;
    #[test]
    fn malformed_supported_dependency_refuses_instead_of_empty_set() {
        let mut doc = crate::historical_adoption::tests::document();
        doc.base.provenance.sources.push(doc.base.id.clone());
        assert!(delta_evidence_links(&crate::transfer_batch::TransferRecord::Doc(doc)).is_err());
    }
    #[test]
    fn source_less_document_has_explicit_empty_dependency_set() {
        let mut doc = crate::historical_adoption::tests::document();
        doc.source = None;
        doc.base.provenance.sources.clear();
        assert!(
            matches!(delta_evidence_links(&crate::transfer_batch::TransferRecord::Doc(doc)),Ok(ref links) if links.is_empty())
        );
    }
}
