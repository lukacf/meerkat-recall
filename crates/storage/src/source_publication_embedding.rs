//! Original embedding-claim authority checked inside the vector transaction.

use super::{
    append_identity_sql, append_plan_sql, bind_json, check_configuration_tx, check_embedding,
    check_plan_tx, check_source_tx, conflict, control_thing, embedding_model, encode_payload,
    incomplete, inputs, integrity, level_bucket, manifest_key, mapping_key, publication_error,
    read_control_tx, replan, require_unique, AttemptFence, BTreeMap, DocNode, DocRevision,
    EmbeddedMutation, NodeEmbedding, PartitionKey, PreparedStandingSourceAuthority,
    PreparedStandingWatchAuthority, PublicationFence, RecordId, Repository, SourceDocumentMapping,
    SourcePublicationCompletion, SourcePublicationManifest, SourcePublicationManifestRepository,
    SourcePublicationRequirements, SpaceConfigToken, StorageClient, StorageResult, Utc, Value,
    WorkItem, COMPLETION_TABLE, DOCUMENT_TABLE, MANIFEST_TABLE, REVISION_TABLE,
};
use std::fmt::Write as _;

// Local, provider-free commit retries. This does not reset or refund a queue
// attempt, and continuing contention still returns the original refusal.
const MAX_SOURCE_PROGRESS_RETRIES: usize = 3;

/// Captured authority for one source revision's embedding job.
///
/// This is not a dispatch permit by itself. The vector repository must recheck
/// it inside the same transaction that changes vectors and node markers.
#[derive(Debug, Clone)]
pub struct SourceEmbeddingWriteAuthority {
    requirements: SourcePublicationRequirements,
    attempt: AttemptFence,
    revision_id: RecordId,
    nodes: Vec<DocNode>,
    // Original delegation plus the grant/configuration observed before provider
    // dispatch. A completed provider call cannot refresh this authority.
    standing_watch: Option<PreparedStandingSourceAuthority>,
}

impl SourceEmbeddingWriteAuthority {
    /// Keep the original inference inputs and authority while admitting only
    /// unrelated live operation-progress updates before the vector transaction.
    pub(crate) async fn reconcile_source_progress(
        &self,
        client: &StorageClient,
    ) -> StorageResult<Self> {
        let mut reconciled = self.clone();
        if let Some(standing_watch) = &self.standing_watch {
            reconciled.standing_watch =
                Some(standing_watch.reconcile_source_progress(client).await?);
        }
        Ok(reconciled)
    }

    /// Run an atomic storage transaction against the original authority. A live
    /// sibling may change receipt bookkeeping after reconciliation (including
    /// while native vector partitions are prepared). Retry only a known guard
    /// refusal with a proven, narrowly reconcilable progress change, at most
    /// four guarded commit invocations, each retaining the backend's existing
    /// transaction-conflict retry bound. The closure must never dispatch a
    /// provider or commit partial vector/node writes on error.
    pub(crate) async fn commit_with_source_progress<F, Fut>(
        &self,
        client: &StorageClient,
        mut transaction: F,
    ) -> StorageResult<Self>
    where
        F: FnMut(Self) -> Fut,
        Fut: std::future::Future<Output = StorageResult<()>>,
    {
        let mut authority = self.reconcile_source_progress(client).await?;
        let mut retries = 0;
        loop {
            match transaction(authority.clone()).await {
                Ok(()) => return Ok(authority),
                Err(error) => {
                    if retries == MAX_SOURCE_PROGRESS_RETRIES || !is_standing_source_refusal(&error)
                    {
                        return Err(error);
                    }
                    let Some(original) = &authority.standing_watch else {
                        return Err(error);
                    };
                    let Some(reconciled) =
                        original.reconcile_changed_source_progress(client).await?
                    else {
                        // A guard name is not evidence of benign contention.
                        return Err(error);
                    };
                    authority.standing_watch = Some(reconciled);
                    retries += 1;
                }
            }
        }
    }

    /// Finish preparation after the original ancestry and immutable inputs have
    /// been loaded.
    ///
    /// A sibling may record live progress during those reads, before
    /// any provider dispatch. Reconcile only that progress, then run the original
    /// transaction checks against the same captured authority and input values.
    pub(crate) async fn validate_preparation(
        &self,
        client: &StorageClient,
        model: &str,
    ) -> StorageResult<Self> {
        self.commit_with_source_progress(client, |authority| async move {
            if let Some(store) = client.embedded_store() {
                store
                    .mutate(|tx| check_embedding_write_tx(tx, &authority, &[], model, &[]))
                    .await?;
            } else {
                let mut sql = String::from("BEGIN TRANSACTION;\n");
                let mut bindings = BTreeMap::new();
                append_embedding_write_sql(&mut sql, &mut bindings, &authority, &[], model, &[])?;
                sql.push_str("COMMIT TRANSACTION;");
                client
                    .query_bind_checked(&sql, bindings)
                    .await
                    .map_err(publication_error)?;
            }
            Ok(())
        })
        .await
    }

    /// Named space captured before provider work began.
    #[must_use]
    pub fn space_id(&self) -> &str {
        &self.requirements.manifest.space_id
    }

    /// Validate the owned node/revision snapshots that will supply provider input.
    /// Call before extracting text, then use those same snapshots for inference.
    ///
    /// # Errors
    /// Rejects a reread that differs from the captured immutable input, including
    /// a transient change that could return to its old value before commit.
    pub fn validate_embedding_input(
        &self,
        node: &DocNode,
        revision: &DocRevision,
    ) -> StorageResult<()> {
        let captured_node = self
            .nodes
            .iter()
            .find(|captured| captured.base.id == node.base.id)
            .ok_or_else(|| conflict("source_embedding_node_not_planned"))?;
        let captured_revision = self
            .requirements
            .revisions
            .iter()
            .find(|captured| captured.base.id == self.revision_id)
            .ok_or_else(|| incomplete("revision_missing"))?;
        if !same_node_input(node, captured_node)
            || revision.base.id != self.revision_id
            || revision.base.space_id != self.space_id()
            || revision.base.deleted_at.is_some()
            || !same_revision_input(revision, captured_revision)
        {
            return Err(conflict("source_embedding_input_changed"));
        }
        Ok(())
    }
}

// Exact shapes produced by the embedded guard, checked native preparation and
// native vector transaction. Never match incidental provider/error text.
fn is_standing_source_refusal(error: &crate::StorageError) -> bool {
    match error {
        crate::StorageError::Conflict(reason) => {
            reason == "standing_watch_authority_changed_or_missing"
        }
        crate::StorageError::QueryError(reason) => matches!(
            reason.as_str(),
            "An error occurred: standing_watch_authority_changed_or_missing"
                | "DocNode embedding transaction failed: An error occurred: standing_watch_authority_changed_or_missing"
        ),
        _ => false,
    }
}

fn same_node_input(current: &DocNode, captured: &DocNode) -> bool {
    let mut normalized = current.clone();
    normalized.base.updated_at = captured.base.updated_at;
    normalized
        .embedding_model
        .clone_from(&captured.embedding_model);
    normalized == *captured
}

fn same_revision_input(current: &DocRevision, captured: &DocRevision) -> bool {
    let mut normalized = current.clone();
    normalized.base.updated_at = captured.base.updated_at;
    normalized == *captured
}

impl SourcePublicationManifestRepository {
    /// Capture the closed plan and original live embedding claim before dispatch.
    ///
    /// # Errors
    /// Rejects stale source/configuration/attempt authority, undeclared revisions,
    /// completed publications, and missing immutable source evidence.
    pub async fn prepare_embedding_write(
        &self,
        space: &str,
        publication: &PublicationFence,
        configuration: &SpaceConfigToken,
        attempt: &AttemptFence,
        revision_id: &RecordId,
    ) -> StorageResult<SourceEmbeddingWriteAuthority> {
        let mut standing_watch = Box::pin(
            PreparedStandingWatchAuthority::capture_for_source_operation(
                &self.client,
                space,
                publication,
            ),
        )
        .await?;
        let requirements = self
            .requirements_without_progress(space, publication)
            .await?;
        let manifest = requirements.manifest();
        if manifest.configuration != *configuration
            || attempt.space_id != space
            || manifest.embedding_work_id(revision_id)? != attempt.work_item_id
            || !manifest
                .documents
                .iter()
                .any(|mapping| mapping.doc_revision_id == *revision_id)
        {
            return Err(conflict("source_embedding_attempt_mismatch"));
        }
        let model = embedding_model(manifest)?;
        let repository = crate::DocNodeRepository::new(self.client.clone());
        let mut nodes = Vec::new();
        for expected in manifest
            .embeddings
            .iter()
            .filter(|expected| expected.revision_id == *revision_id)
        {
            nodes.push(
                repository
                    .get(space, &expected.node_id)
                    .await?
                    .ok_or_else(|| incomplete("embedding_node_missing"))?,
            );
        }
        drop(repository);
        Box::pin(inputs::authorize_embedding_inputs(
            self,
            &mut standing_watch,
            &requirements,
            &nodes,
        ))
        .await?;
        let authority = SourceEmbeddingWriteAuthority {
            requirements,
            attempt: attempt.clone(),
            revision_id: revision_id.clone(),
            nodes,
            standing_watch,
        };
        Box::pin(authority.validate_preparation(&self.client, &model)).await
    }
}

fn check_batch(
    authority: &SourceEmbeddingWriteAuthority,
    node_ids: &[RecordId],
    model: &str,
    embeddings: &[NodeEmbedding],
) -> StorageResult<()> {
    let manifest = authority.requirements.manifest();
    if model != embedding_model(manifest)? || node_ids.len() != embeddings.len() {
        return Err(conflict("source_embedding_batch_mismatch"));
    }
    require_unique(
        node_ids.iter().map(ToString::to_string),
        "embedding batch node",
    )?;
    let profile = manifest
        .embedding_profile
        .as_ref()
        .ok_or_else(|| integrity("required embedding profile missing"))?;
    for (id, embedding) in node_ids.iter().zip(embeddings) {
        let expected = manifest
            .embeddings
            .iter()
            .find(|expected| expected.node_id == *id)
            .filter(|expected| expected.revision_id == authority.revision_id)
            .ok_or_else(|| conflict("source_embedding_node_not_planned"))?;
        let partition = PartitionKey::new(
            embedding.base.security.policy_id.clone(),
            level_bucket(embedding.base.security.level),
            model.into(),
            profile.dimensions,
        )
        .map_err(|error| integrity(&error.to_string()))?;
        if embedding.node.id() != id
            || embedding.base.space_id != manifest.space_id
            || embedding.base.deleted_at.is_some()
            || embedding.model != model
            || usize::try_from(embedding.dims).ok() != Some(profile.dimensions)
            || embedding.vector.len() != profile.dimensions
            || embedding.vector.iter().any(|value| !value.is_finite())
            || !embedding.vector.iter().any(|value| *value != 0.0)
            || partition.table_name() != expected.partition_table
        {
            return Err(conflict("source_embedding_batch_mismatch"));
        }
    }
    Ok(())
}

/// Check the original claim and all vector inputs under the embedded write lock.
pub fn check_embedding_write_tx(
    tx: &mut EmbeddedMutation<'_>,
    authority: &SourceEmbeddingWriteAuthority,
    node_ids: &[RecordId],
    model: &str,
    embeddings: &[NodeEmbedding],
) -> StorageResult<()> {
    check_batch(authority, node_ids, model, embeddings)?;
    if let Some(standing_watch) = &authority.standing_watch {
        standing_watch.check_tx(tx)?;
    }
    let requirements = &authority.requirements;
    let manifest = requirements.manifest();
    let space = &manifest.space_id;
    let key = manifest_key(space, &manifest.publication);
    if read_control_tx::<SourcePublicationManifest>(tx, MANIFEST_TABLE, &key, space)?.as_ref()
        != Some(manifest)
    {
        return Err(conflict("source_manifest_conflict"));
    }
    if read_control_tx::<SourcePublicationCompletion>(tx, COMPLETION_TABLE, &key, space)?.is_some()
    {
        return Err(conflict("source_plan_already_completed"));
    }
    check_source_tx(tx, manifest)?;
    check_configuration_tx(tx, manifest)?;
    crate::work_attempt_repository::check_attempt_fence(tx, &authority.attempt, Utc::now())?;
    let item = tx
        .get::<WorkItem>(space, &authority.attempt.work_item_id, false)?
        .ok_or_else(|| incomplete("embedding_work_missing"))?;
    if item.kind != "embed_source_revision"
        || item.base.id != manifest.embedding_work_id(&authority.revision_id)?
        || item.payload["revision_id"].as_str() != Some(authority.revision_id.to_string().as_str())
        || item.payload["_publication"]
            != serde_json::to_value(&manifest.publication)
                .map_err(|error| integrity(&error.to_string()))?
    {
        return Err(conflict("source_embedding_attempt_mismatch"));
    }
    replan::check_work_plan(
        &item,
        &manifest.publication,
        &manifest.configuration,
        replan::replanned_tx(tx, manifest)?,
    )?;
    check_plan_tx(tx, manifest)?;
    for mapping in &manifest.documents {
        let captured = requirements
            .revisions
            .iter()
            .find(|revision| revision.base.id == mapping.doc_revision_id)
            .ok_or_else(|| incomplete("revision_missing"))?;
        let current = tx
            .get::<DocRevision>(space, &mapping.doc_revision_id, false)?
            .ok_or_else(|| incomplete("revision_missing"))?;
        if !same_revision_input(&current, captured) {
            return Err(conflict("source_document_lineage_changed"));
        }
        for (table, id) in [
            (DOCUMENT_TABLE, &mapping.doc_id),
            (REVISION_TABLE, &mapping.doc_revision_id),
        ] {
            if read_control_tx::<SourceDocumentMapping>(tx, table, &mapping_key(space, id), space)?
                .as_ref()
                != Some(mapping)
            {
                return Err(conflict("source_document_mapping_changed"));
            }
        }
    }
    for captured in &authority.nodes {
        let current = tx
            .get::<DocNode>(space, &captured.base.id, false)?
            .ok_or_else(|| incomplete("embedding_node_missing"))?;
        if !same_node_input(&current, captured) {
            return Err(conflict("source_embedding_input_changed"));
        }
    }
    for (id, embedding) in node_ids.iter().zip(embeddings) {
        let mut node = tx
            .get::<DocNode>(space, id, false)?
            .ok_or_else(|| incomplete("embedding_node_missing"))?;
        // Validate the intended post-write marker without making it visible.
        node.embedding_model = Some(model.into());
        let expected = manifest
            .embeddings
            .iter()
            .find(|expected| expected.node_id == *id)
            .ok_or_else(|| conflict("source_embedding_node_not_planned"))?;
        check_embedding(manifest, expected, &node, embedding)?;
    }
    Ok(())
}

/// Append equivalent native guards before the vector/marker mutation statements.
#[allow(
    clippy::too_many_lines,
    reason = "Partition guards and embedding writes share one ordered transaction fragment and binding namespace; keep their complete native write protocol together."
)]
pub fn append_embedding_write_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    authority: &SourceEmbeddingWriteAuthority,
    node_ids: &[RecordId],
    model: &str,
    embeddings: &[NodeEmbedding],
) -> StorageResult<()> {
    check_batch(authority, node_ids, model, embeddings)?;
    if let Some(standing_watch) = &authority.standing_watch {
        standing_watch.append_guard_sql(sql, bindings)?;
    }
    let requirements = &authority.requirements;
    let manifest = requirements.manifest();
    append_identity_sql(sql, bindings, requirements)?;
    replan::append_replan_scope_sql(sql, bindings, manifest)?;
    crate::work_attempt_repository::append_attempt_fence(sql, bindings, &authority.attempt)?;
    bind_json(
        bindings,
        "sf_write_revision",
        authority.revision_id.to_string(),
    )?;
    bindings.insert(
        "sf_write_job".into(),
        crate::surreal::thing_from_record_id(&manifest.embedding_work_id(&authority.revision_id)?)
            .into(),
    );
    bindings.insert(
        "sf_write_completion".into(),
        control_thing(
            COMPLETION_TABLE,
            &manifest_key(&manifest.space_id, &manifest.publication),
        )
        .into(),
    );
    sql.push_str("IF $fenced_work.kind != 'embed_source_revision' OR $fenced_work.id != $sf_write_job OR $fenced_work.payload.revision_id != $sf_write_revision OR $fenced_work.payload._publication != $sf_fence { THROW 'publication:source_embedding_attempt_mismatch'; };\n\
        LET $sf_write_manifest = SELECT * FROM ONLY $sf_manifest_id;\n\
        IF $sf_write_manifest = NONE OR $sf_write_manifest.space_id != $sf_space OR $sf_write_manifest.payload != $sf_manifest_payload { THROW 'publication:source_manifest_conflict'; };\n\
        IF (SELECT * FROM ONLY $sf_write_completion) != NONE { THROW 'publication:source_plan_already_completed'; };\n\
        fn::recall_physical_read_fence($sf_manifest_id);\n");
    replan::append_work_plan_sql(sql, "fenced_work", "$sf_replanned");
    append_plan_sql(sql, bindings, requirements)?;
    for (index, revision) in requirements.revisions.iter().enumerate() {
        let prefix = format!("sf_write_revision_input_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(&revision.base.id).into(),
        );
        bindings.insert(
            format!("{prefix}_expected"),
            crate::canonical_commit::native_expected_fields(
                crate::canonical_commit::CanonicalRecord::DocRevision(Box::new(revision.clone()))
                    .native()?,
            ),
        );
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            IF ${prefix} = NONE {{ THROW 'publication:source_document_lineage_changed'; }};\n\
            FOR $entry IN object::entries(${prefix}_expected) {{ IF $entry[0] != 'updated_at' AND ${prefix}[$entry[0]] != $entry[1] {{ THROW 'publication:source_document_lineage_changed'; }}; }};");
    }
    for (index, mapping) in manifest.documents.iter().enumerate() {
        for (kind, table, id) in [
            ("doc", DOCUMENT_TABLE, &mapping.doc_id),
            ("rev", REVISION_TABLE, &mapping.doc_revision_id),
        ] {
            let prefix = format!("sf_write_map_{kind}_{index}");
            bindings.insert(
                format!("{prefix}_id"),
                control_thing(table, &mapping_key(&manifest.space_id, id)).into(),
            );
            bind_json(
                bindings,
                &format!("{prefix}_payload"),
                encode_payload(mapping)?,
            )?;
            let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
                IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.payload != ${prefix}_payload {{ THROW 'publication:source_document_mapping_changed'; }};");
        }
    }
    for (index, captured) in authority.nodes.iter().enumerate() {
        let prefix = format!("sf_write_input_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(&captured.base.id).into(),
        );
        bindings.insert(
            format!("{prefix}_expected"),
            crate::surreal::stored_native_value(crate::DocNodeRepository::doc_node_to_value(
                captured,
            )?),
        );
        let _ = writeln!(
            sql,
            "LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            IF ${prefix} = NONE {{ THROW 'publication:source_embedding_input_changed'; }};"
        );
        // Only the mutable index marker and update timestamp are excluded.
        // The fixed field list still compares top-level NONE after object
        // normalization; absence never removes a required comparison.
        for field in [
            "space_id",
            "created_at",
            "created_by",
            "deleted_at",
            "security",
            "provenance",
            "subjects",
            "subjects_state",
            "revision",
            "role",
            "span",
            "node_key",
            "text",
            "parent",
            "position",
            "depth",
            "collection",
        ] {
            let _ = writeln!(sql,"IF ${prefix}.{field} != ${prefix}_expected.{field} {{ THROW 'publication:source_embedding_input_changed'; }};");
        }
    }
    for (index, (id, embedding)) in node_ids.iter().zip(embeddings).enumerate() {
        let prefix = format!("sf_write_node_{index}");
        bindings.insert(
            format!("{prefix}_id"),
            crate::surreal::thing_from_record_id(id).into(),
        );
        bindings.insert(
            format!("{prefix}_revision"),
            crate::surreal::thing_from_record_id(&authority.revision_id).into(),
        );
        bindings.insert(
            format!("{prefix}_security"),
            crate::surreal::security_to_value(&embedding.base.security),
        );
        bindings.insert(
            format!("{prefix}_subjects"),
            Value::Array(crate::surreal::things_from_links(
                embedding.base.subjects.clone(),
            )),
        );
        bindings.insert(
            format!("{prefix}_subjects_state"),
            crate::surreal::enum_to_surreal(&embedding.base.subjects_state)?,
        );
        let _ = writeln!(sql,"LET ${prefix} = SELECT * FROM ONLY ${prefix}_id;\n\
            IF ${prefix} = NONE OR ${prefix}.space_id != $sf_space OR ${prefix}.deleted_at != NONE OR ${prefix}.revision != ${prefix}_revision OR ${prefix}.security != ${prefix}_security OR ${prefix}.subjects != ${prefix}_subjects OR ${prefix}.subjects_state != ${prefix}_subjects_state {{ THROW 'publication:source_embedding_envelope_changed'; }};");
    }
    Ok(())
}
