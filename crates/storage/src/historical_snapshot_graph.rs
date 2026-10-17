//! Snapshot evidence authority for original source-less graph records. The
//! anchor identifies the import, never an author, model run or document quote.
use super::{
    regeneration, Control, HistoricalAdoptionReceipt, HistoricalCommittedGroup,
    HistoricalGroupCommit, PreparedHistoricalAuthority,
};
use crate::canonical_commit::{
    source_support_fingerprint, CanonicalCommitReceipt, CanonicalRecord, CanonicalRepository,
};
use crate::historical_adoption::{
    canonical_sha256, document_mapping_sha256, historical_snapshot_links, HistoricalAdoptionProof,
    HistoricalCommitProvenance, HistoricalSnapshotGraphProof, HistoricalSourceGroup,
    HistoricalUnknownOwnership, HistoricalVectorState,
};
use crate::source_publication::{
    external_identity_key, head_key, integrity, operation_key, revision_key, stable_key,
    SourceIdentity, SourcePublicationRepository, StoredOperation, StoredRevision,
};
use crate::source_publication_manifest::{
    manifest_key, SourcePublicationCompletion, SourcePublicationManifest, COMPLETION_TABLE,
    MANIFEST_TABLE,
};
use crate::StorageResult;
use chrono::{DateTime, Utc};
use elephant_types::provider_binding::SpaceConfigToken;
use elephant_types::publication::{
    OperationReceipt, OperationState, PublicationFence, SourceAuthorizationEnvelope, SourceHead,
    SourceRevision, SourceVisibility,
};
use elephant_types::records::base::BaseFields;
use elephant_types::records::ingest::IngestMode;
use elephant_types::records::{Source, SourceType, SpaceConfig};
use elephant_types::RecordId;
use std::collections::{BTreeMap, BTreeSet};

/// One original graph row and its explicit projected typed representation.
#[derive(Debug, Clone)]
pub struct HistoricalSnapshotGraphRow {
    /// Typed graph row with historical literal decoding and declared namespace relocation.
    pub record: CanonicalRecord,
    /// Complete original graph JSON before relocation or support installation.
    pub original_row: serde_json::Value,
    /// SHA-256 of canonical original JSON, distinct from the lexical spool row hash.
    pub original_row_sha256: String,
}
struct SnapshotGraphPopulation {
    raw_hashes: BTreeMap<RecordId, String>,
    fingerprints: BTreeMap<RecordId, String>,
    references: BTreeMap<RecordId, BTreeSet<RecordId>>,
    reference_fingerprints: BTreeMap<RecordId, String>,
    dependencies: BTreeMap<RecordId, CanonicalRecord>,
    bytes: usize,
}

struct SnapshotGraphPublication {
    source: Source,
    accepted: SourceRevision,
    publication: PublicationFence,
    mapping: String,
    external: String,
    now: DateTime<Utc>,
}

struct SnapshotGraphReceipts {
    receipt: HistoricalAdoptionReceipt,
    canonical: CanonicalCommitReceipt,
    operation: OperationReceipt,
    source_input: String,
}

struct SnapshotGraphCommit {
    config: SpaceConfig,
    publication: SnapshotGraphPublication,
    previous_source: Option<Source>,
    dependencies: BTreeMap<RecordId, CanonicalRecord>,
    receipts: SnapshotGraphReceipts,
    controls: Vec<Control>,
    epoch: Option<String>,
}

impl PreparedHistoricalAuthority {
    /// Adopt at most 256 same-envelope graph rows under original transfer/S1
    /// custody. Original rows and provenance are immutable transaction inputs.
    pub async fn adopt_snapshot_graph(
        &self,
        rows: Vec<HistoricalSnapshotGraphRow>,
    ) -> StorageResult<HistoricalCommittedGroup> {
        self.check_custody()?;
        if rows.is_empty() || rows.len() > 256 {
            return Err(integrity(
                "snapshot graph requires bounded transfer custody",
            ));
        }
        let space = &self.input.space_id;
        let epoch = CanonicalRepository::new(self.client.clone())
            .observe_epoch(space)
            .await?;
        let config = crate::SpaceConfigRepository::new(self.client.clone())
            .get_by_space_id(space)
            .await?
            .ok_or_else(|| integrity("snapshot graph configuration absent"))?;
        let processing = config
            .processing
            .as_ref()
            .ok_or_else(|| integrity("snapshot graph processing absent"))?;
        processing
            .validate()
            .map_err(|_| integrity("snapshot graph processing invalid"))?;
        let mut population = self.snapshot_population(&rows)?;
        self.close_snapshot_references(&mut population).await?;
        let mapping = document_mapping_sha256(
            population
                .raw_hashes
                .iter()
                .map(|(id, hash)| (id, hash.as_str())),
        )?;
        let external = format!("historical-snapshot-graph:{mapping}");
        let declaration = self
            .input
            .snapshot_groups
            .iter()
            .find(|group| group.external_source_id == external)
            .ok_or_else(|| integrity("snapshot graph is absent from the complete original plan"))?;
        if declaration.original_rows_sha256 != population.raw_hashes
            || declaration.document_mapping_sha256 != mapping
        {
            return Err(integrity("snapshot graph declared population differs"));
        }
        let source_id = regeneration::group_source_id(space, &external);
        let previous_source = if self.catchup.is_some() {
            self.catchup_source_before(&source_id).await?
        } else {
            None
        };
        let group = self.publication_group(&external)?;
        if let Some(replayed) = self
            .replay_snapshot_group(&source_id, &group, &population)
            .await?
        {
            return Ok(replayed);
        }
        let publication = self.snapshot_publication(
            rows[0].record.base(),
            mapping,
            external,
            source_id,
            previous_source.as_ref(),
        );
        let (manifest, dependencies) = self.snapshot_manifest(&config, &publication, population)?;
        manifest.validate()?;
        let receipts = self.snapshot_receipts(&publication, &manifest)?;
        let mut controls = self.snapshot_controls(&publication, &manifest, &receipts)?;
        self.append_snapshot_supports(&rows, &publication, &receipts, &mut controls)?;
        self.commit_snapshot_group(SnapshotGraphCommit {
            config,
            publication,
            previous_source,
            dependencies,
            receipts,
            controls,
            epoch,
        })
        .await
    }

    fn snapshot_population(
        &self,
        rows: &[HistoricalSnapshotGraphRow],
    ) -> StorageResult<SnapshotGraphPopulation> {
        let space = &self.input.space_id;
        let first = rows[0].record.base();
        let mut raw_hashes = BTreeMap::new();
        let mut fingerprints = BTreeMap::new();
        let mut references = BTreeMap::new();
        let mut dependencies = BTreeMap::new();
        let mut bytes = 0_usize;
        for row in rows {
            let base = row.record.base();
            bytes = bytes
                .checked_add(
                    serde_json::to_vec(&row.original_row)
                        .map_err(|_| integrity("snapshot graph encoding"))?
                        .len(),
                )
                .ok_or_else(|| integrity("snapshot graph size overflow"))?;
            if bytes > 128 * 1024 * 1024
                || !crate::canonical_commit::is_derived_graph_table(base.id.table())
                || base.space_id != *space
                || base.deleted_at.is_some()
                || !base.provenance.sources.is_empty()
                || base.security != first.security
                || base.subjects != first.subjects
                || base.subjects_state != first.subjects_state
                || canonical_sha256(&row.original_row)? != row.original_row_sha256
            {
                return Err(integrity(
                    "snapshot graph original scope/policy/hash differs",
                ));
            }
            let projected = crate::transfer_batch::historical_record_projection(
                base.id.table().as_str(),
                &row.original_row,
            )?;
            let mut original: CanonicalRecord = serde_json::from_value(
                serde_json::json!({"table":base.id.table().as_str(),"record":projected}),
            )
            .map_err(|_| integrity("snapshot graph typed original invalid"))?;
            if original.base().space_id != self.input.source_space_id {
                return Err(integrity("snapshot graph original namespace differs"));
            }
            original.base_mut().space_id.clone_from(space);
            if serde_json::to_value(&original).map_err(|_| integrity("snapshot graph encoding"))?
                != serde_json::to_value(&row.record)
                    .map_err(|_| integrity("snapshot graph encoding"))?
                || raw_hashes
                    .insert(base.id.clone(), row.original_row_sha256.clone())
                    .is_some()
            {
                return Err(integrity("snapshot graph exact projection differs"));
            }
            let links = historical_snapshot_links(&row.record)?;
            fingerprints.insert(base.id.clone(), row.record.fingerprint()?);
            references.insert(base.id.clone(), links);
            dependencies.insert(base.id.clone(), row.record.clone());
        }
        Ok(SnapshotGraphPopulation {
            raw_hashes,
            fingerprints,
            references,
            dependencies,
            reference_fingerprints: BTreeMap::new(),
            bytes,
        })
    }

    async fn close_snapshot_references(
        &self,
        population: &mut SnapshotGraphPopulation,
    ) -> StorageResult<()> {
        let space = &self.input.space_id;
        let SnapshotGraphPopulation {
            dependencies,
            references,
            reference_fingerprints,
            bytes,
            ..
        } = population;
        // Close the complete typed reference graph once under the captured
        // epoch. Every dependency becomes a CAS input and immutable fingerprint;
        // read-time authorization still applies independently to each row.
        let mut pending: std::collections::VecDeque<_> = dependencies.keys().cloned().collect();
        let mut visited = BTreeSet::new();
        while let Some(id) = pending.pop_front() {
            if !visited.insert(id.clone()) {
                continue;
            }
            if visited.len() > 8192 || pending.len() > 32768 {
                return Err(integrity("snapshot graph reference closure bound"));
            }
            let record = dependencies
                .get(&id)
                .cloned()
                .ok_or_else(|| integrity("snapshot graph closure dependency missing"))?;
            let links = historical_snapshot_links(&record)?;
            for reference in &links {
                if !dependencies.contains_key(reference) {
                    // Every first insertion, including a root's first-hop
                    // reference, passes the same count and byte budget.
                    if dependencies.len() >= 8192 {
                        return Err(integrity("snapshot graph reference closure bound"));
                    }
                    let row = CanonicalRecord::load(&self.client, space, reference)
                        .await?
                        .ok_or_else(|| integrity("snapshot graph transitive reference absent"))?;
                    if row.base().space_id != *space || row.base().deleted_at.is_some() {
                        return Err(integrity("snapshot graph transitive reference unavailable"));
                    }
                    *bytes = bytes.saturating_add(
                        serde_json::to_vec(&row)
                            .map_err(|_| integrity("snapshot graph closure encode"))?
                            .len(),
                    );
                    if *bytes > 128 * 1024 * 1024 {
                        return Err(integrity("snapshot graph closure bytes bound"));
                    }
                    dependencies.insert(reference.clone(), row);
                }
                if pending.len() >= 32768 {
                    return Err(integrity("snapshot graph pending closure bound"));
                }
                pending.push_back(reference.clone());
            }
            reference_fingerprints.insert(id.clone(), record.fingerprint()?);
            references.insert(id, links);
        }
        Ok(())
    }

    async fn replay_snapshot_group(
        &self,
        source_id: &RecordId,
        group: &HistoricalSourceGroup,
        population: &SnapshotGraphPopulation,
    ) -> StorageResult<Option<HistoricalCommittedGroup>> {
        let space = &self.input.space_id;
        let existing = SourcePublicationRepository::new(self.client.clone())
            .head(space, source_id)
            .await?;
        if let Some(head) = existing {
            if self.catchup.is_none() {
                let operation: StoredOperation =
                    SourcePublicationRepository::new(self.client.clone())
                        .read_control("source_operation", &head.operation_id, space)
                        .await?
                        .ok_or_else(|| integrity("snapshot graph replay operation absent"))?;
                let publication = PublicationFence {
                    source_id: source_id.clone(),
                    generation: head.generation,
                    revision_id: head
                        .published_revision
                        .ok_or_else(|| integrity("snapshot replay publication missing"))?,
                    operation_id: head.operation_id,
                };
                let manifest =
                    crate::source_publication_manifest::SourcePublicationManifestRepository::new(
                        self.client.clone(),
                    )
                    .get(space, &publication)
                    .await?
                    .ok_or_else(|| integrity("snapshot replay manifest missing"))?;
                let proof = manifest
                    .historical_adoption
                    .as_ref()
                    .ok_or_else(|| integrity("snapshot replay proof missing"))?;
                let snapshot = proof
                    .snapshot_graph
                    .as_ref()
                    .ok_or_else(|| integrity("snapshot replay graph proof missing"))?;
                if proof.graph_fingerprints != population.fingerprints
                    || snapshot.typed_references != population.references
                    || snapshot.reference_fingerprints != population.reference_fingerprints
                {
                    return Err(integrity("snapshot replay exact reference image changed"));
                }
                let receipt = self
                    .verify_existing_group(group, &operation.receipt)
                    .await?;
                return Ok(Some(HistoricalCommittedGroup {
                    receipt,
                    native_effects: vec![],
                }));
            }
        }
        Ok(None)
    }

    fn snapshot_publication(
        &self,
        first: &BaseFields,
        mapping: String,
        external: String,
        source_id: RecordId,
        previous_source: Option<&Source>,
    ) -> SnapshotGraphPublication {
        let space = &self.input.space_id;
        let now = Utc::now();
        let mut base = BaseFields::new(
            source_id.clone(),
            space.clone(),
            "historical-snapshot-anchor-v1".into(),
        );
        base.security = first.security.clone();
        base.subjects.clone_from(&first.subjects);
        base.subjects_state = first.subjects_state;
        let mut source = Source::new(base, SourceType::Manual, mapping.clone());
        source.external_id = Some(external.clone());
        source.metadata = serde_json::json!({"historical_snapshot_evidence":{"snapshot_sha256":self.input.snapshot_sha256,
            "original_authorship":"unknown_preserved","document_quotes":false}});
        if let Some(previous) = previous_source {
            source.base.created_at = previous.base.created_at;
        }
        // Identical original graph bytes in a fresh snapshot still have a new
        // import authority. Append a plan-bound revision and support rows;
        // never overwrite the immutable prior snapshot proof.
        let revision = if self.catchup.is_some() {
            format!(
                "historical-snapshot-graph-v1:{}",
                stable_key(&[&self.input.plan_id, &mapping])
            )
        } else {
            format!("historical-snapshot-graph-v1:{mapping}")
        };
        let operation_id = operation_key(space, &format!("{}:{external}", self.input.plan_id));
        let publication = PublicationFence {
            source_id: source_id.clone(),
            generation: stable_key(&[space, &external, &self.input.plan_id]),
            revision_id: revision.clone(),
            operation_id,
        };
        let accepted = SourceRevision {
            space_id: space.clone(),
            source_id,
            revision_id: revision,
            content_hash: mapping.clone(),
            accepted_at: now,
            source: source.clone(),
        };
        SnapshotGraphPublication {
            source,
            accepted,
            publication,
            mapping,
            external,
            now,
        }
    }

    fn snapshot_manifest(
        &self,
        config: &SpaceConfig,
        prepared: &SnapshotGraphPublication,
        population: SnapshotGraphPopulation,
    ) -> StorageResult<(
        SourcePublicationManifest,
        BTreeMap<RecordId, CanonicalRecord>,
    )> {
        let space = &self.input.space_id;
        let processing = config
            .processing
            .as_ref()
            .ok_or_else(|| integrity("snapshot graph processing absent"))?;
        let SnapshotGraphPublication {
            publication,
            mapping,
            external,
            now,
            ..
        } = prepared;
        let SnapshotGraphPopulation {
            raw_hashes,
            fingerprints,
            references,
            reference_fingerprints,
            dependencies,
            ..
        } = population;
        let manifest = SourcePublicationManifest {
            space_id: space.clone(),
            publication: publication.clone(),
            mode: IngestMode::Graph,
            configuration: SpaceConfigToken {
                space_id: space.clone(),
                config_id: config.base.id.to_string(),
                generation: processing.generation,
                policy_id: processing.policy_id.clone(),
            },
            embedding_profile: None,
            documents: vec![],
            metadata_artifact_ids: vec![],
            planning_artifact_ids: vec![],
            bundles: vec![],
            embeddings: vec![],
            embedding_work_item_ids: vec![],
            historical_adoption: Some(HistoricalAdoptionProof {
                snapshot_graph: Some(HistoricalSnapshotGraphProof {
                    version: 1,
                    original_rows_sha256: raw_hashes,
                    typed_references: references,
                    reference_fingerprints,
                }),
                version: 1,
                source_space_id: self.input.source_space_id.clone(),
                snapshot_sha256: self.input.snapshot_sha256.clone(),
                idmap_sha256: self.input.idmap_sha256.clone(),
                plan_id: self.input.plan_id.clone(),
                documents_sha256: self.input.documents_sha256.clone(),
                groups_sha256: self.input.groups_sha256.clone(),
                document_mapping_sha256: mapping.clone(),
                external_source_id: external.clone(),
                imported_at: *now,
                documents: BTreeMap::new(),
                graph_fingerprints: fingerprints,
                original_sources: BTreeMap::new(),
                vector_state: HistoricalVectorState::PreservedUnverified,
            }),
        };
        Ok((manifest, dependencies))
    }

    fn snapshot_receipts(
        &self,
        prepared: &SnapshotGraphPublication,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<SnapshotGraphReceipts> {
        let space = &self.input.space_id;
        let SnapshotGraphPublication {
            publication,
            mapping,
            external,
            now,
            ..
        } = prepared;
        let source_id = &publication.source_id;
        let revision = &publication.revision_id;
        let operation_id = &publication.operation_id;
        let proof = manifest
            .historical_adoption
            .as_ref()
            .ok_or_else(|| integrity("snapshot graph proof absent"))?;
        let commit_id = proof.commit_id(manifest);
        let digest = manifest.digest()?;
        let request = canonical_sha256(&(space, &external, &self.input.snapshot_sha256, &digest))?;
        // Public snapshot/canonical proofs retain their SHA-256 domain. The
        // source lifecycle input uses the same UUIDv5 domain as document adoption.
        let source_input = stable_key(&["historical-source-operation-input-v1", &request]);
        let operation = OperationReceipt {
            space_id: space.clone(),
            operation_id: operation_id.clone(),
            source_id: source_id.clone(),
            source_generation: publication.generation.clone(),
            desired_revision: Some(revision.clone()),
            published_revision: Some(revision.clone()),
            state: OperationState::Processing,
            accepted_at: *now,
            completed_at: None,
            error_code: Some("historical_snapshot_graph_coverage_pending".into()),
        };
        let receipt = HistoricalAdoptionReceipt {
            source_space_id: self.input.source_space_id.clone(),
            space_id: space.clone(),
            external_source_id: external.clone(),
            source_id: source_id.clone(),
            operation: operation.clone(),
            accepted_revision: revision.clone(),
            accepted_request_fingerprint: request.clone(),
            document_mapping_sha256: mapping.clone(),
            document_content_sha256: BTreeMap::new(),
            manifest_digest: digest.clone(),
            original_snapshot_sha256: self.input.snapshot_sha256.clone(),
            commit_id: commit_id.clone(),
        };
        let canonical = CanonicalCommitReceipt {
            historical_import: Some(HistoricalCommitProvenance::for_manifest(manifest)?),
            space_id: space.clone(),
            commit_id,
            input_fingerprint: request,
            plan_fingerprint: digest,
            replay_output: Some(
                serde_json::to_value(&receipt)
                    .map_err(|_| integrity("snapshot graph receipt encoding"))?,
            ),
            source_ids: vec![source_id.clone()],
            publication_authority: Some(publication.clone()),
            maintenance_authority: vec![],
            source_id: Some(source_id.clone()),
            source_revision: Some(revision.clone()),
            effect_ids: vec![source_id.clone()],
            extension_ids: vec![],
            outbox_ids: vec![],
            committed_at: *now,
        };
        Ok(SnapshotGraphReceipts {
            receipt,
            canonical,
            operation,
            source_input,
        })
    }

    fn snapshot_control_payloads(
        &self,
        prepared: &SnapshotGraphPublication,
        receipts: &SnapshotGraphReceipts,
    ) -> StorageResult<(SourcePublicationCompletion, SourceHead, StoredOperation)> {
        let space = &self.input.space_id;
        let SnapshotGraphPublication {
            source,
            accepted,
            publication,
            now,
            ..
        } = prepared;
        let source_id = &publication.source_id;
        let revision = &publication.revision_id;
        let operation_id = &publication.operation_id;
        let completion = SourcePublicationCompletion {
            space_id: space.clone(),
            publication: publication.clone(),
            manifest_digest: receipts.receipt.manifest_digest.clone(),
            commit_id: receipts.receipt.commit_id.clone(),
            rendered_revision_fingerprints: BTreeMap::new(),
            document_fingerprints: BTreeMap::new(),
            accepted_source_fingerprint: Some(source_support_fingerprint(accepted)?),
            published_progress: None,
            historical_embedding_coverage: None,
            completed_at: *now,
        };
        let head = SourceHead {
            space_id: space.clone(),
            source_id: source_id.clone(),
            generation: publication.generation.clone(),
            desired_revision: Some(revision.clone()),
            published_revision: Some(revision.clone()),
            operation_id: operation_id.clone(),
            lifecycle: SourceVisibility::Active,
            updated_at: *now,
        };
        let stored = StoredOperation {
            receipt: receipts.operation.clone(),
            request_fingerprint: receipts.receipt.accepted_request_fingerprint.clone(),
            input_fingerprint: receipts.source_input.clone(),
            authorization: SourceAuthorizationEnvelope::from(source),
            contact_digest: None,
            producer_origin: elephant_types::publication::SourceProducerOrigin::UnknownOriginal,
            predecessor_publication: None,
        };
        Ok((completion, head, stored))
    }

    fn snapshot_controls(
        &self,
        prepared: &SnapshotGraphPublication,
        manifest: &SourcePublicationManifest,
        receipts: &SnapshotGraphReceipts,
    ) -> StorageResult<Vec<Control>> {
        let space = &self.input.space_id;
        let SnapshotGraphPublication {
            publication,
            external,
            ..
        } = prepared;
        let source_id = &publication.source_id;
        let revision = &publication.revision_id;
        let operation_id = &publication.operation_id;
        let (completion, head, stored) = self.snapshot_control_payloads(prepared, receipts)?;
        let key = manifest_key(space, publication);
        let controls = vec![
            Control::new("source_head", head_key(space, source_id), &head)?,
            Control::new(
                "source_identity",
                external_identity_key(space, SourceType::Manual, external),
                &SourceIdentity {
                    source_id: source_id.clone(),
                },
            )?,
            Control::new(
                "source_revision",
                revision_key(space, source_id, revision),
                &StoredRevision {
                    revision: prepared.accepted.clone(),
                    operation_id: operation_id.clone(),
                    input_fingerprint: receipts.source_input.clone(),
                },
            )?,
            Control::new("source_operation", operation_id.clone(), &stored)?,
            Control::new(
                "source_authorization",
                head_key(space, source_id),
                &stored.authorization,
            )?,
            Control::new(MANIFEST_TABLE, key.clone(), manifest)?,
            Control::new(COMPLETION_TABLE, key, &completion)?,
            Control::new(
                COMPLETION_TABLE,
                stable_key(&[
                    "published-source-completion/v1",
                    space,
                    &source_id.to_string(),
                ]),
                &completion,
            )?,
            Control::new(
                "source_commit_receipt",
                stable_key(&[space, &receipts.receipt.commit_id]),
                &receipts.canonical,
            )?,
        ];
        Ok(controls)
    }

    fn append_snapshot_supports(
        &self,
        rows: &[HistoricalSnapshotGraphRow],
        prepared: &SnapshotGraphPublication,
        receipts: &SnapshotGraphReceipts,
        controls: &mut Vec<Control>,
    ) -> StorageResult<()> {
        let space = &self.input.space_id;
        let source_id = &prepared.publication.source_id;
        let revision = &prepared.publication.revision_id;
        let commit_id = &receipts.receipt.commit_id;
        for row in rows {
            let support = crate::canonical_commit::SourceEffectSupport {
                space_id: space.clone(),
                source_id: source_id.clone(),
                source_revision: revision.clone(),
                record_id: row.record.base().id.clone(),
                independent: false,
                record_fingerprint: row.record.fingerprint()?,
                last_commit_id: Some(commit_id.clone()),
                historical_unknown: Some(HistoricalUnknownOwnership {
                    snapshot_sha256: self.input.snapshot_sha256.clone(),
                    plan_id: self.input.plan_id.clone(),
                    commit_id: commit_id.clone(),
                }),
            };
            controls.push(Control::new(
                "source_effect_support",
                crate::canonical_commit::support_key(
                    space,
                    source_id,
                    revision,
                    &support.record_id,
                ),
                &support,
            )?);
        }
        Ok(())
    }

    async fn commit_snapshot_group(
        &self,
        input: SnapshotGraphCommit,
    ) -> StorageResult<HistoricalCommittedGroup> {
        let SnapshotGraphCommit {
            config,
            publication,
            previous_source,
            dependencies,
            receipts,
            mut controls,
            epoch,
        } = input;
        let SnapshotGraphPublication {
            source,
            publication,
            external,
            ..
        } = publication;
        let source_id = publication.source_id;
        let commit_id = &receipts.receipt.commit_id;
        let progress = self.prepare_regeneration_control(
            &source_id,
            Some(&external),
            commit_id,
            epoch.as_deref(),
        )?;
        if let Some((control, _)) = &progress {
            controls.push(control.clone());
        }
        let after = match &progress {
            Some((_, receipt)) => {
                serde_json::from_value::<regeneration::HistoricalRegenerationProgress>(
                    receipt
                        .replay_output
                        .clone()
                        .ok_or_else(|| integrity("snapshot graph progress missing"))?,
                )
                .map_err(|_| integrity("snapshot graph progress invalid"))?
                .after_epoch
            }
            None => uuid::Uuid::new_v4().to_string(),
        };
        let control_before = self.catchup_control_before(&controls).await?;
        let effects = self
            .commit_group(HistoricalGroupCommit {
                config: &config,
                source: &source_id,
                dependencies: &dependencies.into_values().collect::<Vec<_>>(),
                effects: &[(
                    previous_source.map(|row| CanonicalRecord::Source(Box::new(row))),
                    CanonicalRecord::Source(Box::new(source)),
                )],
                controls: &controls,
                control_before: &control_before,
                epoch: epoch.clone(),
                after_epoch: Some(&after),
                progress: progress.as_ref().map(|(_, receipt)| receipt),
            })
            .await?;
        if let Some(population) = self.population.get() {
            population.advance_epoch(epoch.as_deref(), &after)?;
        }
        if let Some((_, receipt)) = &progress {
            self.retain_regeneration_commit(receipt)?;
        }
        Ok(HistoricalCommittedGroup {
            receipt: receipts.receipt,
            native_effects: effects,
        })
    }
}
