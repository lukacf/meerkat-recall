//! Exact input point reads from one already captured desired manifest.
//! Source/head selection and original execution ancestry stay with their owners.

use crate::canonical_commit::CanonicalRecord;

use super::{
    conflict, incomplete, integrity, BTreeMap, DocNode, FromStr, PreparedStandingSourceAuthority,
    PublicationFence, RecordId, SourcePublicationManifest, SourcePublicationManifestRepository,
    SourcePublicationRequirements, StorageClient, StorageResult, TableName,
};

fn input_bound(manifest: &SourcePublicationManifest) -> StorageResult<usize> {
    let mut bound = manifest.documents.len().checked_mul(2);
    for count in [
        Some(manifest.embeddings.len()),
        Some(manifest.planning_artifact_ids.len()),
        manifest.metadata_artifact_ids.len().checked_mul(2),
        manifest.bundles.len().checked_mul(2),
    ] {
        bound = bound.and_then(|total| count.and_then(|count| total.checked_add(count)));
    }
    for bundle in &manifest.bundles {
        bound = bound.and_then(|total| total.checked_add(bundle.node_ids.len())?.checked_add(1));
    }
    bound.ok_or_else(|| integrity("source input population overflow"))
}

fn declared_ids(manifest: &SourcePublicationManifest) -> StorageResult<BTreeMap<RecordId, bool>> {
    manifest.validate()?;
    let mut ids = BTreeMap::new();
    let mut insert = |id: &RecordId, table: TableName, required: bool| -> StorageResult<()> {
        if id.table() != table {
            return Err(integrity("source input record kind mismatch"));
        }
        ids.entry(id.clone())
            .and_modify(|present| *present |= required)
            .or_insert(required);
        Ok(())
    };
    for mapping in &manifest.documents {
        insert(&mapping.doc_id, TableName::Doc, true)?;
        insert(&mapping.doc_revision_id, TableName::DocRevision, true)?;
    }
    for bundle in &manifest.bundles {
        insert(&bundle.node_id, TableName::DocNode, true)?;
        for node in &bundle.node_ids {
            insert(node, TableName::DocNode, true)?;
        }
        // This ID is an expectation fixed before fanout, not a completion.
        insert(&bundle.artifact_id, TableName::WorkArtifact, false)?;
    }
    for embedding in &manifest.embeddings {
        insert(&embedding.node_id, TableName::DocNode, true)?;
    }
    for artifact in manifest
        .metadata_artifact_ids
        .iter()
        .chain(&manifest.planning_artifact_ids)
    {
        insert(artifact, TableName::WorkArtifact, true)?;
    }
    Ok(ids)
}

async fn load_input(
    client: &StorageClient,
    space: &str,
    id: &RecordId,
) -> StorageResult<Option<CanonicalRecord>> {
    let row = CanonicalRecord::load(client, space, id).await?;
    if row.as_ref().is_some_and(|row| {
        row.base().id != *id || row.base().space_id != space || row.base().deleted_at.is_some()
    }) {
        return Err(conflict("source_publication_input_changed"));
    }
    Ok(row)
}

async fn add_candidate(
    client: &StorageClient,
    space: &str,
    row: &CanonicalRecord,
    records: &mut BTreeMap<RecordId, CanonicalRecord>,
) -> StorageResult<()> {
    let CanonicalRecord::WorkArtifact(artifact) = row else {
        return Ok(());
    };
    if artifact.artifact_type != "commit_bundle" {
        return Ok(());
    }
    let candidate = RecordId::from_str(
        artifact
            .data
            .get("source_artifact_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| integrity("source bundle input identity missing"))?,
    )?;
    if candidate.table() != TableName::WorkArtifact || candidate == artifact.base.id {
        return Err(integrity("source bundle input identity invalid"));
    }
    let original = load_input(client, space, &candidate)
        .await?
        .ok_or_else(|| incomplete("source_candidate_missing"))?;
    if let Some(previous) = records.get(&candidate) {
        if previous.fingerprint()? != original.fingerprint()? {
            return Err(conflict("source_publication_input_changed"));
        }
    } else {
        records.insert(candidate, original);
    }
    Ok(())
}

impl SourcePublicationManifestRepository {
    /// Point-read the complete input population declared by this captured plan.
    ///
    /// The caller retains the original desired-head/manifest and membership
    /// guards. This does not choose a head, scan historical documents, or prove
    /// readiness. Future knowledge bundles may not exist yet; every required
    /// document/node/planning/metadata input and every materialized bundle's
    /// original candidate must exist. Physical dispatch also checks any input
    /// it consumes before a manifest exists or outside this declared population.
    ///
    /// # Errors
    /// Refuses invalid, missing required, deleted, foreign or changed inputs.
    pub async fn declared_input_records(
        &self,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<Vec<CanonicalRecord>> {
        let bound = input_bound(manifest)?;
        let mut records = BTreeMap::new();
        for (id, required) in declared_ids(manifest)? {
            let Some(row) = load_input(&self.client, &manifest.space_id, &id).await? else {
                if required {
                    return Err(incomplete("declared_input_missing"));
                }
                continue;
            };
            add_candidate(&self.client, &manifest.space_id, &row, &mut records).await?;
            if let Some(previous) = records.get(&id) {
                if previous.fingerprint()? != row.fingerprint()? {
                    return Err(conflict("source_publication_input_changed"));
                }
            } else {
                records.insert(id, row);
            }
        }
        if records.len() > bound {
            return Err(integrity("source inputs exceed their declared population"));
        }
        Ok(records.into_values().collect())
    }
}

pub(super) async fn authorize_inputs(
    repository: &SourcePublicationManifestRepository,
    standing: &mut Option<PreparedStandingSourceAuthority>,
    space: &str,
    publication: &PublicationFence,
    manifest: Option<&SourcePublicationManifest>,
    artifact: Option<&RecordId>,
) -> StorageResult<()> {
    let Some(proof) = standing else { return Ok(()) };
    if proof.principals().next().is_none() {
        // Direct/independent admission still retains B's original private
        // ancestry proof. It has no standing Watch ceiling to add here.
        return Ok(());
    }
    proof.authorize_record(
        load_input(&repository.client, space, &publication.source_id)
            .await?
            .ok_or_else(|| incomplete("source_missing"))?,
    )?;
    if let Some(manifest) = manifest {
        for row in repository.declared_input_records(manifest).await? {
            proof.authorize_record(row)?;
        }
    }
    if let Some(id) = artifact {
        let row = load_input(&repository.client, space, id)
            .await?
            .ok_or_else(|| incomplete("expected_bundle_missing"))?;
        let mut candidates = BTreeMap::new();
        add_candidate(&repository.client, space, &row, &mut candidates).await?;
        proof.authorize_record(row)?;
        for candidate in candidates.into_values() {
            proof.authorize_record(candidate)?;
        }
    }
    Ok(())
}

pub(super) async fn authorize_embedding_inputs(
    repository: &SourcePublicationManifestRepository,
    standing: &mut Option<PreparedStandingSourceAuthority>,
    requirements: &SourcePublicationRequirements,
    nodes: &[DocNode],
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    authorize_inputs(
        repository,
        standing,
        &manifest.space_id,
        &manifest.publication,
        None,
        None,
    )
    .await?;
    let Some(proof) = standing else { return Ok(()) };
    if proof.principals().next().is_none() {
        return Ok(());
    }
    for mapping in &manifest.documents {
        proof.authorize_record(
            load_input(&repository.client, &manifest.space_id, &mapping.doc_id)
                .await?
                .ok_or_else(|| incomplete("doc_missing"))?,
        )?;
    }
    for revision in &requirements.revisions {
        proof.authorize_record(CanonicalRecord::DocRevision(Box::new(revision.clone())))?;
    }
    for node in nodes {
        // The provider consumes these original node snapshots. Subsequent
        // batches legitimately change embedding_model, so their full input and
        // policy guard stays in same_node_input rather than B's immutable rows.
        let row = CanonicalRecord::DocNode(Box::new(node.clone()));
        for principal in proof.principals() {
            crate::source_watch_registration::authorize_standing_source_input(principal, &row)?;
        }
    }
    Ok(())
}
