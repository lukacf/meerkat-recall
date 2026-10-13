//! One atomic publication of parallel-prepared siblings in one security partition.
use super::{
    publication, BaseFields, KnowledgeView, KnowledgeViewDependencyIndex, KnowledgeViewRepository,
    KnowledgeViewRevision, StorageError, StorageResult, TableName, Value,
};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

struct PreparedPublication {
    view: KnowledgeView,
    revision: KnowledgeViewRevision,
    dependencies: Vec<BaseFields>,
    indexes: Vec<KnowledgeViewDependencyIndex>,
    event: elephant_types::records::SystemEvent,
}

impl KnowledgeViewRepository {
    /// Publish all successfully prepared siblings in one transaction under the
    /// original input epoch and the same original managed invocation authority.
    ///
    /// Every input is revalidated before any write; one failed guard or effect
    /// rolls back all revisions, views, dependency indexes and outbox events.
    /// The batch belongs to one space/security partition. Subject sets may differ
    /// per article and retain each article's existing complete envelope checks.
    /// Empty batches perform no writes but still verify supplied authority.
    #[allow(
        clippy::too_many_lines,
        reason = "Keep the shared original epoch and authority, all sibling guards and the all-or-none publication transaction visible together."
    )]
    pub async fn publish_batch_at_epoch(
        &self,
        publications: Vec<(KnowledgeView, KnowledgeViewRevision)>,
        observed_epoch: &str,
        authority: Option<&crate::PreparedKnowledgeViewAuthority>,
    ) -> StorageResult<Vec<(KnowledgeView, KnowledgeViewRevision)>> {
        if let Some(authority) = authority {
            authority.verify_on(&self.client).await?;
        }
        let Some((first, _)) = publications.first() else {
            return Ok(Vec::new());
        };
        let space = first.base.space_id.clone();
        let security = first.base.security.clone();
        let fingerprint = first.security_fingerprint.clone();
        if let Some(authority) = authority {
            authority.check_space(&space)?;
        }
        let mut views = BTreeSet::new();
        let mut revisions = BTreeSet::new();
        let mut keys = BTreeSet::new();
        let mut prepared = Vec::with_capacity(publications.len());
        for (view, revision) in publications {
            validate_pair(&view, &revision)?;
            if view.base.space_id != space
                || view.base.security != security
                || view.security_fingerprint != fingerprint
                || !views.insert(view.base.id.clone())
                || !revisions.insert(revision.base.id.clone())
                || !keys.insert(view.key.clone())
            {
                return Err(publication::invalid_input(
                    "knowledge_view_batch_partition_or_duplicate",
                ));
            }
            let dependencies = self
                .validate_publication_inputs(&revision, observed_epoch)
                .await?;
            let event = publication::publication_event(&view, &revision);
            let indexes = Self::dependency_indexes(&view, &revision);
            prepared.push(PreparedPublication {
                view,
                revision,
                dependencies,
                indexes,
                event,
            });
        }
        if let Some(store) = self.client.embedded_store() {
            store
                .mutate(|mutation| {
                    if let Some(authority) = authority {
                        authority.check_tx(mutation)?;
                    }
                    if crate::canonical_commit::epoch_tx(mutation, &space)?.as_deref()
                        != Some(observed_epoch)
                    {
                        return Err(publication::input_changed());
                    }
                    crate::source_publication::require_space(mutation, &space)?;
                    // Guard ALL original inputs and generations before the first
                    // sibling write. Own earlier writes never refresh a fence.
                    for row in &prepared {
                        guard_embedded(mutation, row)?;
                    }
                    let mut sequence =
                        crate::event_sequence::reserve_tx(mutation, &space, 0, prepared.len())?;
                    mutation.advance_canonical_epoch(&space)?;
                    for row in &prepared {
                        sequence = sequence.checked_add(1).ok_or_else(|| {
                            publication::invalid_input("outbox sequence exhausted")
                        })?;
                        write_embedded(mutation, row, sequence)?;
                    }
                    Ok(())
                })
                .await?;
        } else {
            let mut bindings = crate::bindings! {
                "space_id" => space.clone(),
                "epoch_id" => crate::source_publication::control_thing("canonical_epoch", &space),
                "observed_epoch" => observed_epoch,
            };
            let mut sql = String::from(
                "BEGIN TRANSACTION;\n\
                 IF (SELECT VALUE version FROM ONLY publication_schema:canonical) != 1 { THROW 'publication:canonical_schema_missing'; };\n\
                 IF (SELECT VALUE epoch FROM ONLY $epoch_id) != $observed_epoch { THROW 'publication:knowledge_view_inputs_changed'; };\n\
                 LET $spaces = SELECT id FROM space_config WHERE space_id = $space_id AND deleted_at IS NONE;\n\
                 IF array::len($spaces) != 1 { THROW 'publication:unknown_space'; };\n",
            );
            if let Some(authority) = authority {
                authority.append_guard_sql(&mut sql, &mut bindings)?;
            }
            for (position, row) in prepared.iter().enumerate() {
                guard_native(&mut sql, &mut bindings, &format!("batch_{position}"), row)?;
            }
            let sequence = crate::event_sequence::append_reserve_sql(
                &mut sql,
                &mut bindings,
                &space,
                "knowledge_view_events",
                0,
                prepared.len(),
            )?;
            sql.push_str("UPDATE $epoch_id SET epoch = <string>rand::uuid();\n");
            for (position, row) in prepared.iter().enumerate() {
                write_native(
                    &mut sql,
                    &mut bindings,
                    &format!("batch_{position}"),
                    row,
                    &format!("{sequence} + {}", position + 1),
                )?;
            }
            sql.push_str("COMMIT TRANSACTION;");
            self.client
                .query_bind_checked(&sql, bindings)
                .await
                .map_err(crate::source_publication::publication_error)?;
        }
        Ok(prepared
            .into_iter()
            .map(|row| (row.view, row.revision))
            .collect())
    }
}

fn validate_pair(view: &KnowledgeView, revision: &KnowledgeViewRevision) -> StorageResult<()> {
    let revision_id = revision.base.id.to_string();
    if view.base.id.table() != TableName::KnowledgeView
        || revision.base.id.table() != TableName::KnowledgeViewRevision
        || view.base.space_id != revision.base.space_id
        || view.base.security != revision.base.security
        || view.base.subjects != revision.base.subjects
        || view.base.subjects_state != revision.base.subjects_state
        || view.base.deleted_at.is_some()
        || revision.base.deleted_at.is_some()
        || view.root_record != revision.root_record
        || view.kind != revision.kind
        || view.security_fingerprint != revision.compiler.policy_fingerprint
        || revision.view_id != view.base.id.to_string()
        || view.current_revision.as_deref() != Some(revision_id.as_str())
        || view.revision_count != revision.revision
        || !revision
            .dependencies
            .iter()
            .any(|dependency| dependency.record_id == view.root_record)
    {
        return Err(StorageError::Conflict(
            "Knowledge-view publication receipt does not match its view".into(),
        ));
    }
    Ok(())
}

#[allow(
    clippy::needless_pass_by_ref_mut,
    reason = "Require exclusive access to the same mutation context used for subsequent sibling writes; guard checks must stay inside that transaction."
)]
fn guard_embedded(
    mutation: &mut crate::EmbeddedMutation<'_>,
    row: &PreparedPublication,
) -> StorageResult<()> {
    for expected in &row.dependencies {
        let current = mutation
            .scan_raw_rows(expected.id.table())?
            .into_iter()
            .find(|(id, _)| id == &expected.id)
            .map(|(_, value)| {
                serde_json::from_value::<BaseFields>(value).map_err(publication::invalid_input)
            })
            .transpose()?;
        if current.as_ref() != Some(expected) {
            return Err(publication::input_changed());
        }
    }
    let previous =
        mutation.get::<KnowledgeView>(&row.view.base.space_id, &row.view.base.id, true)?;
    if previous
        .as_ref()
        .is_none_or(|old| old.base.deleted_at.is_some())
    {
        if row.view.revision_count != 1 || row.revision.supersedes.is_some() {
            return Err(publication::input_changed());
        }
    } else if previous.as_ref().is_some_and(|old| {
        old.revision_count.checked_add(1) != Some(row.view.revision_count)
            || old.current_revision != row.revision.supersedes
            || old.root_record != row.view.root_record
            || old.key != row.view.key
    }) {
        return Err(publication::input_changed());
    }
    if mutation
        .get::<KnowledgeViewRevision>(&row.revision.base.space_id, &row.revision.base.id, true)?
        .is_some()
    {
        return Err(publication::input_changed());
    }
    Ok(())
}

fn write_embedded(
    mutation: &mut crate::EmbeddedMutation<'_>,
    row: &PreparedPublication,
    sequence: i64,
) -> StorageResult<()> {
    mutation.create(row.revision.clone())?;
    if mutation
        .get::<KnowledgeView>(&row.view.base.space_id, &row.view.base.id, true)?
        .is_some()
    {
        mutation.replace(row.view.clone())?;
    } else {
        mutation.create(row.view.clone())?;
    }
    for index in mutation
        .scan::<KnowledgeViewDependencyIndex>()?
        .into_iter()
        .filter(|index| {
            index.base.space_id == row.revision.base.space_id
                && index.view.id() == &row.view.base.id
        })
    {
        mutation.hard_delete::<KnowledgeViewDependencyIndex>(
            &row.revision.base.space_id,
            &index.base.id,
        )?;
    }
    for index in &row.indexes {
        mutation.create(index.clone())?;
    }
    let mut event = row.event.clone();
    event.seq = sequence;
    mutation.create(event)?;
    Ok(())
}

fn guard_native(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    row: &PreparedPublication,
) -> StorageResult<()> {
    bindings.extend(crate::bindings! {
        format!("{prefix}_view_id") => crate::surreal::thing_from_record_id(&row.view.base.id),
        format!("{prefix}_view") => KnowledgeViewRepository::view_to_value(&row.view)?,
        format!("{prefix}_revision_id") => crate::surreal::thing_from_record_id(&row.revision.base.id),
        format!("{prefix}_revision") => KnowledgeViewRepository::revision_to_value(&row.revision)?,
    });
    write!(sql, "LET ${prefix}_previous = SELECT * FROM ONLY ${prefix}_view_id;\n\
        IF ${prefix}_previous = NONE OR ${prefix}_previous.deleted_at != NONE {{\n\
        IF ${prefix}_view.revision_count != 1 OR ${prefix}_revision.supersedes != NONE {{ THROW 'publication:knowledge_view_inputs_changed'; }};\n\
        }} ELSE {{\n\
        IF ${prefix}_previous.space_id != $space_id OR ${prefix}_previous.revision_count + 1 != ${prefix}_view.revision_count OR ${prefix}_previous.current_revision != ${prefix}_revision.supersedes OR ${prefix}_previous.root_record != ${prefix}_view.root_record OR ${prefix}_previous.key != ${prefix}_view.key {{ THROW 'publication:knowledge_view_inputs_changed'; }};\n\
        }};\n\
        IF (SELECT VALUE id FROM ONLY ${prefix}_revision_id) IS NOT NONE {{ THROW 'publication:knowledge_view_inputs_changed'; }};\n")
        .map_err(publication::invalid_input)?;
    for (position, dependency) in row.dependencies.iter().enumerate() {
        let dependency_prefix = format!("{prefix}_input_{position}");
        bindings.insert(
            format!("{dependency_prefix}_id"),
            crate::surreal::thing_from_record_id(&dependency.id).into(),
        );
        bindings.insert(
            format!("{dependency_prefix}_expected"),
            crate::canonical_commit::native_expected_fields(Value::Object(
                crate::surreal::base_fields_to_object(dependency)?,
            )),
        );
        write!(sql, "LET ${dependency_prefix}_current = SELECT * FROM ONLY ${dependency_prefix}_id;\nIF ${dependency_prefix}_current = NONE {{ THROW 'publication:knowledge_view_inputs_changed'; }};\nFOR ${dependency_prefix}_entry IN object::entries(${dependency_prefix}_expected) {{ IF ${dependency_prefix}_current[${dependency_prefix}_entry[0]] != ${dependency_prefix}_entry[1] {{ THROW 'publication:knowledge_view_inputs_changed'; }}; }};\n")
            .map_err(publication::invalid_input)?;
    }
    Ok(())
}

fn write_native(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    prefix: &str,
    row: &PreparedPublication,
    sequence: &str,
) -> StorageResult<()> {
    writeln!(sql, "CREATE ${prefix}_revision_id CONTENT ${prefix}_revision; UPSERT ${prefix}_view_id CONTENT ${prefix}_view; DELETE knowledge_view_dependency WHERE space_id = $space_id AND view = ${prefix}_view_id;")
        .map_err(publication::invalid_input)?;
    for (position, index) in row.indexes.iter().enumerate() {
        let id = format!("{prefix}_dependency_id_{position}");
        let value = format!("{prefix}_dependency_value_{position}");
        writeln!(sql, "CREATE ${id} CONTENT ${value};").map_err(publication::invalid_input)?;
        bindings.insert(
            id,
            crate::surreal::thing_from_record_id(&index.base.id).into(),
        );
        bindings.insert(
            value,
            KnowledgeViewRepository::dependency_index_to_value(index)?,
        );
    }
    bindings.extend(crate::bindings! {
        format!("{prefix}_event_id") => crate::surreal::thing_from_record_id(&row.event.base.id),
        format!("{prefix}_event") => crate::SystemEventRepository::system_event_to_value(&row.event)?,
    });
    writeln!(sql, "CREATE ${prefix}_event_id CONTENT object::from_entries(array::concat(object::entries(${prefix}_event), [['seq', {sequence}]]));")
        .map_err(publication::invalid_input)?;
    Ok(())
}
