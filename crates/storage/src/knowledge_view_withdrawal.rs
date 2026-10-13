//! Idempotent visibility successor of an exact canonical definition withdrawal.
use super::{
    KnowledgeView, KnowledgeViewPublicationStatus, KnowledgeViewRepository, StorageError,
    StorageResult, TableName,
};
use crate::canonical_commit::{append_row_guard, CanonicalRecord};
use crate::source_publication::{conflict, integrity};
use elephant_types::records::KnowledgeViewDefinition;

impl KnowledgeViewRepository {
    /// Hide views only while the exact original withdrawn definition is current.
    ///
    /// Pass the after-image saved in the canonical withdrawal receipt. Re-reading
    /// a newer definition before calling this method would adopt new authority.
    /// An exact retry returns zero; stale, deleted, replaced or republished roots
    /// conflict without changing any view.
    pub async fn hide_for_withdrawn_definition(
        &self,
        definition: &KnowledgeViewDefinition,
    ) -> StorageResult<usize> {
        self.hide_with_original_authority(definition, None).await
    }

    /// Carry the same original managed invocation authority through the
    /// operational visibility successor of a canonical withdrawal.
    pub async fn hide_for_withdrawn_definition_with_authority(
        &self,
        definition: &KnowledgeViewDefinition,
        authority: &crate::PreparedKnowledgeViewAuthority,
    ) -> StorageResult<usize> {
        self.hide_with_original_authority(definition, Some(authority))
            .await
    }

    async fn hide_with_original_authority(
        &self,
        definition: &KnowledgeViewDefinition,
        authority: Option<&crate::PreparedKnowledgeViewAuthority>,
    ) -> StorageResult<usize> {
        if let Some(authority) = authority {
            authority.check_space(&definition.base.space_id)?;
            authority.verify_on(&self.client).await?;
        }
        if definition.base.deleted_at.is_some()
            || definition.base.id.table() != TableName::KnowledgeViewDefinition
            || definition.publication_status != KnowledgeViewPublicationStatus::Hidden
            || definition.base.space_id.is_empty()
        {
            return Err(integrity(
                "knowledge_view_withdrawal_requires_hidden_definition",
            ));
        }
        let root = definition.base.id.to_string();
        if let Some(store) = self.client.embedded_store() {
            return store
                .mutate(|tx| {
                    if let Some(authority) = authority {
                        authority.check_tx(tx)?;
                    }
                    let current = tx.get::<KnowledgeViewDefinition>(
                        &definition.base.space_id,
                        &definition.base.id,
                        true,
                    )?;
                    if current.as_ref() != Some(definition) {
                        return Err(conflict("knowledge_view_withdrawal_definition_changed"));
                    }
                    let mut count = 0usize;
                    for mut view in tx.scan::<KnowledgeView>()?.into_iter().filter(|view| {
                        view.base.space_id == definition.base.space_id
                            && view.root_record == root
                            && view.base.deleted_at.is_none()
                            && view.publication_status != KnowledgeViewPublicationStatus::Hidden
                    }) {
                        view.publication_status = KnowledgeViewPublicationStatus::Hidden;
                        view.published_at = None;
                        view.base.updated_at = chrono::Utc::now();
                        let _: KnowledgeView = tx.replace(view)?;
                        count += 1;
                    }
                    Ok(count)
                })
                .await;
        }
        let row = CanonicalRecord::KnowledgeViewDefinition(Box::new(definition.clone()));
        let mut bindings = crate::bindings! {
            "space" => definition.base.space_id.clone(),
            "root" => root,
        };
        let mut sql = String::from("BEGIN TRANSACTION;\n");
        if let Some(authority) = authority {
            authority.append_guard_sql(&mut sql, &mut bindings)?;
        }
        append_row_guard(&mut sql, &mut bindings, "withdrawal", &row, Some(&row))?;
        // The pinned engine skips unchanged domain writes. Every actual
        // definition mutation increments this same private per-space guard,
        // yielding a real conflict without changing user fields or timestamps.
        sql.push_str("fn::recall_reference_guard({id:$withdrawal_id,space_id:$space},false);\n");
        sql.push_str("LET $hidden = UPDATE knowledge_view SET publication_status = 'hidden', published_at = NONE, updated_at = time::now() WHERE space_id = $space AND root_record = $root AND deleted_at IS NONE AND publication_status != 'hidden' RETURN id;\nRETURN array::len($hidden);\nCOMMIT TRANSACTION;");
        let mut response = self
            .client
            .query_bind_checked(&sql, bindings)
            .await
            .map_err(crate::source_publication::publication_error)?;
        let result = response
            .num_statements()
            .checked_sub(1)
            .ok_or_else(|| integrity("knowledge_view_withdrawal_missing_result"))?;
        let changed: Option<usize> = response.take(result).map_err(|error| {
            StorageError::QueryError(format!("Failed to decode withdrawal count: {error}"))
        })?;
        changed.ok_or_else(|| integrity("knowledge_view_withdrawal_missing_result"))
    }
}

#[cfg(test)]
#[path = "knowledge_view_withdrawal_tests.rs"]
mod tests;
