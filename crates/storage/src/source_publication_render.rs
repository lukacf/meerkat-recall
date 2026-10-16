//! Original finalizer proof for raw document inputs and rendered revisions.
//!
//! Rendered text may differ from an accepted template. Only exact domain rows
//! captured and checked by the publication transaction certify that output;
//! neither a renderer name, a recipe marker, nor a recomputed text hash does.

use super::{
    conflict, control_thing, incomplete, integrity, manifest_key, stable_key, BTreeMap, BTreeSet,
    Deserialize, Doc, DocRevision, EmbeddedMutation, RecordId, Repository, SourceDocumentMapping,
    SourcePublicationManifest, SourcePublicationManifestRepository, SourcePublicationRepository,
    SourcePublicationRequirements, StorageResult, Value,
};
use crate::canonical_commit::{source_support_fingerprint, CanonicalCommitReceipt};
use elephant_types::publication::SourceRevision;
use std::fmt::Write as _;

fn render_identity_matches(
    mapping: &SourceDocumentMapping,
    document: &Doc,
    revision: &DocRevision,
) -> bool {
    let space = &mapping.space_id;
    !(document.base.space_id != *space
        || revision.base.space_id != *space
        || document.base.id != mapping.doc_id
        || revision.base.id != mapping.doc_revision_id
        || revision.doc.id() != &mapping.doc_id
        || document.source.as_ref().map(elephant_types::Link::id)
            != Some(&mapping.publication.source_id))
}

fn source_control_key(manifest: &SourcePublicationManifest) -> String {
    stable_key(&[
        &manifest.space_id,
        &manifest.publication.source_id.to_string(),
        &manifest.publication.revision_id,
    ])
}

fn accepted_source(control: &str) -> StorageResult<SourceRevision> {
    #[derive(Deserialize)]
    struct AcceptedControl {
        revision: SourceRevision,
    }
    serde_json::from_str::<AcceptedControl>(control)
        .map(|control| control.revision)
        .map_err(|_| integrity("invalid accepted source snapshot"))
}

fn source_payload_tx(
    tx: &EmbeddedMutation<'_>,
    manifest: &SourcePublicationManifest,
) -> StorageResult<String> {
    let key = format!(
        "{}:{}",
        crate::source_publication::REVISION_TABLE,
        source_control_key(manifest)
    );
    tx.scan_extension_rows(crate::source_publication::REVISION_TABLE)?
        .into_iter()
        .find(|(id, row)| {
            id == &key
                && row.get("space_id").and_then(serde_json::Value::as_str)
                    == Some(manifest.space_id.as_str())
        })
        .and_then(|(_, row)| {
            row.get("payload")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
        .ok_or_else(|| incomplete("accepted_source_missing"))
}

fn check_source_identity(
    manifest: &SourcePublicationManifest,
    source: &SourceRevision,
) -> StorageResult<()> {
    if source.space_id != manifest.space_id
        || source.source_id != manifest.publication.source_id
        || source.revision_id != manifest.publication.revision_id
        || source.source.base.space_id != manifest.space_id
        || source.source.base.id != manifest.publication.source_id
        || source.source.base.deleted_at.is_some()
        || source.content_hash != source.source.hash
    {
        return Err(conflict("source_render_input_changed"));
    }
    Ok(())
}

impl SourcePublicationManifestRepository {
    pub(super) async fn load_render_documents(
        &self,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<Vec<Doc>> {
        let repository = crate::DocRepository::new(self.client.clone());
        let mut documents = Vec::with_capacity(manifest.documents.len());
        for mapping in &manifest.documents {
            documents.push(
                repository
                    .get(&manifest.space_id, &mapping.doc_id)
                    .await?
                    .ok_or_else(|| incomplete("document_missing"))?,
            );
        }
        drop(repository);
        Ok(documents)
    }

    pub(super) async fn load_render_source(
        &self,
        manifest: &SourcePublicationManifest,
    ) -> StorageResult<String> {
        let payload = if let Some(store) = self.client.embedded_store() {
            store.mutate(|tx| source_payload_tx(tx, manifest)).await?
        } else {
            #[derive(Deserialize)]
            struct PayloadRow {
                payload: String,
            }
            let mut response = self.client.query_bind_checked(
                "SELECT payload FROM $render_source_id WHERE space_id = $render_source_space",
                crate::bindings! {
                    "render_source_id" => control_thing(crate::source_publication::REVISION_TABLE, &source_control_key(manifest)),
                    "render_source_space" => manifest.space_id.clone(),
                },
            ).await?;
            let mut rows: Vec<PayloadRow> = response.take(0)?;
            if rows.len() != 1 {
                return Err(incomplete("accepted_source_missing"));
            }
            rows.remove(0).payload
        };
        check_source_identity(manifest, &accepted_source(&payload)?)?;
        Ok(payload)
    }

    /// Read original completed authority without treating the latest desired
    /// generation as the historical publication.
    ///
    /// The caller fences visibility
    /// and authorizes both the current and accepted source envelopes.
    ///
    /// This checks the full accepted snapshot, including the original staged
    /// text and producer-specific content fingerprint. A source deduplication
    /// fingerprint need not equal a digest of the raw text alone.
    ///
    /// # Errors
    /// Returns storage errors or an explicit source-input identity conflict.
    /// Missing or changed completion, row fingerprints, or receipt return false.
    pub async fn completed_document_revision_is_current(
        &self,
        mapping: &SourceDocumentMapping,
        document: &Doc,
        revision: &DocRevision,
        accepted: &SourceRevision,
    ) -> StorageResult<bool> {
        let space = &mapping.space_id;
        if !render_identity_matches(mapping, document, revision) {
            return Ok(false);
        }
        let Some(manifest) = self.get(space, &mapping.publication).await? else {
            return Ok(false);
        };
        manifest.validate()?;
        if manifest.space_id != *space
            || manifest.publication != mapping.publication
            || !manifest.documents.contains(mapping)
            || self
                .source_revision_for_document(space, &document.base.id)
                .await?
                .as_ref()
                != Some(mapping)
        {
            return Ok(false);
        }
        check_source_identity(&manifest, accepted)?;
        let Some(completion) = self.completion(space, &mapping.publication).await? else {
            return Ok(false);
        };
        let expected_commit = manifest.historical_adoption.as_ref().map_or_else(
            || {
                format!(
                    "source-publication:{}",
                    manifest_key(space, &mapping.publication)
                )
            },
            |proof| proof.commit_id(&manifest),
        );
        let expected_documents: BTreeSet<_> =
            manifest.documents.iter().map(|item| &item.doc_id).collect();
        let expected_revisions: BTreeSet<_> = manifest
            .documents
            .iter()
            .map(|item| &item.doc_revision_id)
            .collect();
        if completion.space_id != *space
            || completion.publication != mapping.publication
            || completion.commit_id != expected_commit
            || completion.manifest_digest != manifest.digest()?
            || completion
                .document_fingerprints
                .keys()
                .collect::<BTreeSet<_>>()
                != expected_documents
            || completion
                .rendered_revision_fingerprints
                .keys()
                .collect::<BTreeSet<_>>()
                != expected_revisions
            || completion.document_fingerprints.get(&document.base.id)
                != Some(&source_support_fingerprint(document)?)
            || completion
                .rendered_revision_fingerprints
                .get(&revision.base.id)
                != Some(&source_support_fingerprint(revision)?)
            || completion.accepted_source_fingerprint.as_ref()
                != Some(&source_support_fingerprint(accepted)?)
        {
            return Ok(false);
        }
        // The normal receipt accessor intentionally requires current generation
        // authority for mutation replay. A read of the old published revision
        // during a newer pending acceptance uses its exact original receipt.
        let Some(receipt) = SourcePublicationRepository::new(self.client.clone())
            .read_control::<CanonicalCommitReceipt>(
                "source_commit_receipt",
                &stable_key(&[space, &expected_commit]),
                space,
            )
            .await?
        else {
            return Ok(false);
        };
        match &manifest.historical_adoption {
            Some(proof) => {
                if receipt.historical_import.as_ref()
                    != Some(
                        &crate::historical_adoption::HistoricalCommitProvenance::for_manifest(
                            &manifest,
                        )?,
                    )
                    || !proof.document_matches(mapping, document, revision, accepted)?
                {
                    return Ok(false);
                }
            }
            None if receipt.historical_import.is_some() => return Ok(false),
            None => {}
        }
        Ok(receipt.space_id == *space
            && receipt.commit_id == completion.commit_id
            && receipt.publication_authority.as_ref() == Some(&mapping.publication)
            && receipt.source_id.as_ref() == Some(&mapping.publication.source_id)
            && receipt.source_revision.as_ref() == Some(&mapping.publication.revision_id))
    }
    /// Return original input only after verifying the exact publication proof.
    /// Historical groups bind original input separately for each document.
    pub async fn verified_original_document_text(
        &self,
        mapping: &SourceDocumentMapping,
        document: &Doc,
        revision: &DocRevision,
        accepted: &SourceRevision,
    ) -> StorageResult<Option<String>> {
        if !self
            .completed_document_revision_is_current(mapping, document, revision, accepted)
            .await?
        {
            return Ok(None);
        }
        let Some(manifest) = self.get(&mapping.space_id, &mapping.publication).await? else {
            return Ok(None);
        };
        if manifest.historical_adoption.is_some() {
            return Ok(Some(document.text.clone()));
        }
        accepted
            .source
            .manual_doc_template()
            .map(|template| template.map(|input| input.text))
            .map_err(|_| integrity("invalid accepted original document input"))
    }
}

pub(super) fn revision_fingerprints(
    requirements: &SourcePublicationRequirements,
) -> StorageResult<BTreeMap<RecordId, String>> {
    requirements
        .revisions
        .iter()
        .map(|row| Ok((row.base.id.clone(), source_support_fingerprint(row)?)))
        .collect()
}

pub(super) fn document_fingerprints(
    requirements: &SourcePublicationRequirements,
) -> StorageResult<BTreeMap<RecordId, String>> {
    requirements
        .documents
        .iter()
        .map(|row| Ok((row.base.id.clone(), source_support_fingerprint(row)?)))
        .collect()
}

pub(super) fn accepted_source_fingerprint(
    requirements: &SourcePublicationRequirements,
) -> StorageResult<String> {
    let source = accepted_source(
        requirements
            .accepted_source_control
            .as_ref()
            .ok_or_else(|| incomplete("accepted_source_missing"))?,
    )?;
    check_source_identity(requirements.manifest(), &source)?;
    source_support_fingerprint(&source)
}

pub(super) fn check_render_inputs_tx(
    tx: &EmbeddedMutation<'_>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    let captured = requirements
        .accepted_source_control
        .as_ref()
        .ok_or_else(|| incomplete("accepted_source_missing"))?;
    let current = source_payload_tx(tx, manifest)?;
    if current != *captured {
        return Err(conflict("source_render_input_changed"));
    }
    for document in &requirements.documents {
        let current = tx
            .get::<Doc>(&manifest.space_id, &document.base.id, false)?
            .ok_or_else(|| incomplete("document_missing"))?;
        if current != *document {
            return Err(conflict("source_document_lineage_changed"));
        }
    }
    for revision in &requirements.revisions {
        let current = tx
            .get::<DocRevision>(&manifest.space_id, &revision.base.id, false)?
            .ok_or_else(|| incomplete("revision_missing"))?;
        if current != *revision {
            return Err(conflict("source_document_lineage_changed"));
        }
    }
    Ok(())
}

pub(super) fn append_render_inputs_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    requirements: &SourcePublicationRequirements,
) -> StorageResult<()> {
    let manifest = requirements.manifest();
    let captured = requirements
        .accepted_source_control
        .as_ref()
        .ok_or_else(|| incomplete("accepted_source_missing"))?;
    bindings.insert(
        "sf_render_source_id".into(),
        control_thing(
            crate::source_publication::REVISION_TABLE,
            &source_control_key(manifest),
        )
        .into(),
    );
    bindings.insert("sf_render_source_payload".into(), captured.clone().into());
    sql.push_str("LET $sf_render_source = SELECT * FROM ONLY $sf_render_source_id;\nIF $sf_render_source = NONE OR $sf_render_source.space_id != $sf_space OR $sf_render_source.payload != $sf_render_source_payload { THROW 'publication:source_render_input_changed'; };\nUPDATE $sf_render_source_id SET canonical_read_nonce = <string>rand::uuid();\n");
    for (index, document) in requirements.documents.iter().enumerate() {
        append_domain_guard(
            sql,
            bindings,
            &format!("sf_render_doc_{index}"),
            &document.base.id,
            crate::surreal::doc_to_value(document)?,
        );
    }
    for (index, revision) in requirements.revisions.iter().enumerate() {
        append_domain_guard(
            sql,
            bindings,
            &format!("sf_render_rev_{index}"),
            &revision.base.id,
            crate::DocRevisionRepository::doc_revision_to_value(revision)?,
        );
    }
    Ok(())
}

fn append_domain_guard(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    id: &RecordId,
    value: Value,
) {
    bindings.insert(
        format!("{prefix}_id"),
        crate::surreal::thing_from_record_id(id).into(),
    );
    bindings.insert(
        format!("{prefix}_expected"),
        crate::canonical_commit::native_expected_fields(value),
    );
    // Ordinary Doc/DocRevision mutations write the same space reference guard.
    // A real revision increment creates a storage conflict; same-value domain
    // UPDATE is optimized away by the pinned engine and is not a write fence.
    let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\nIF ${prefix} = NONE OR ${prefix}.space_id != $sf_space {{ THROW 'publication:source_document_lineage_changed'; }};\nFOR $field IN object::entries(${prefix}_expected) {{ IF ${prefix}[$field[0]] != $field[1] {{ THROW 'publication:source_document_lineage_changed'; }}; }};\nfn::recall_reference_guard({{id:${prefix}_id,space_id:$sf_space}},false);");
}
