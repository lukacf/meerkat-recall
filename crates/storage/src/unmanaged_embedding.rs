//! Generic document embedding must not compete with an accepted source claim.
//! These are temporary ownership observations, never a publication authority.
use std::collections::BTreeMap;
use std::fmt::Write as _;

use elephant_types::records::{Doc, DocNode, DocRevision};
use elephant_types::RecordId;
use surrealdb::sql::Value;

use crate::embedded::EmbeddedMutation;
use crate::{
    DocNodeRepository, DocRepository, DocRevisionRepository, Repository,
    SourcePublicationRepository, StorageClient, StorageError, StorageResult,
};

fn changed() -> StorageError {
    StorageError::Conflict("source_embedding_requires_original_claim".into())
}

/// Only the links which determine ownership. No source text, vector, or grant.
#[derive(Clone, Debug)]
pub struct Ownership {
    node: RecordId,
    revision: RecordId,
    document: Option<RecordId>,
    document_present: bool,
    source: Option<RecordId>,
}

impl Ownership {
    async fn capture(client: &StorageClient, space: &str, node: &DocNode) -> StorageResult<Self> {
        if node.base.space_id != space || node.base.deleted_at.is_some() {
            return Err(changed());
        }
        let revision = DocRevisionRepository::new(client.clone())
            .get_including_deleted(space, node.revision.id())
            .await?;
        let mut result = Self {
            node: node.base.id.clone(),
            revision: node.revision.id().clone(),
            document: None,
            document_present: false,
            source: None,
        };
        // Preserve existing missing-revision/dead-letter handling. The vector
        // transaction still checks this exact absence, including foreign rows.
        let Some(revision) = revision else {
            return Ok(result);
        };
        if revision.base.deleted_at.is_some() {
            return Err(changed());
        }
        result.document = Some(revision.doc.id().clone());
        let repository = DocRepository::new(client.clone());
        let document = repository
            .get_with_deleted(space, revision.doc.id())
            .await?;
        if let Some(document) = document {
            if document.base.deleted_at.is_some() {
                return Err(changed());
            }
            result.document_present = true;
            result.source = document.source.as_ref().map(|link| link.id().clone());
        } else if repository.exists_globally(revision.doc.id()).await? {
            return Err(changed());
        }
        drop(repository);
        Ok(result)
    }

    async fn is_unmanaged(&self, client: &StorageClient, space: &str) -> StorageResult<bool> {
        match &self.source {
            Some(source) => Ok(!SourcePublicationRepository::new(client.clone())
                .head_exists_globally(space, source)
                .await?),
            None => Ok(true),
        }
    }
}

pub async fn is_unmanaged(
    client: &StorageClient,
    space: &str,
    node: &DocNode,
) -> StorageResult<bool> {
    match Ownership::capture(client, space, node).await {
        Ok(guard) => guard.is_unmanaged(client, space).await,
        Err(StorageError::Conflict(code)) if code == "source_embedding_requires_original_claim" => {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

/// Recheck immediately before a generic provider call, without adopting a claim.
pub async fn require_unmanaged(
    client: &StorageClient,
    space: &str,
    nodes: &[DocNode],
) -> StorageResult<()> {
    let repository = DocNodeRepository::new(client.clone());
    for node in nodes {
        let current = repository
            .get(space, &node.base.id)
            .await?
            .ok_or_else(changed)?;
        if current.revision != node.revision || !is_unmanaged(client, space, &current).await? {
            return Err(changed());
        }
    }
    drop(repository);
    Ok(())
}

pub async fn prepare(
    client: &StorageClient,
    space: &str,
    ids: &[RecordId],
) -> StorageResult<Vec<Ownership>> {
    let repository = DocNodeRepository::new(client.clone());
    let mut guards = Vec::with_capacity(ids.len());
    for id in ids {
        let node = repository.get(space, id).await?.ok_or_else(changed)?;
        let guard = Ownership::capture(client, space, &node).await?;
        if !guard.is_unmanaged(client, space).await? {
            return Err(changed());
        }
        guards.push(guard);
    }
    drop(repository);
    Ok(guards)
}

pub fn check_tx(tx: &EmbeddedMutation<'_>, space: &str, guards: &[Ownership]) -> StorageResult<()> {
    for guard in guards {
        let node = tx
            .get_by_id::<DocNode>(&guard.node, true)?
            .ok_or_else(changed)?;
        if node.base.space_id != space
            || node.base.deleted_at.is_some()
            || node.revision.id() != &guard.revision
        {
            return Err(changed());
        }
        let revision = tx.get_by_id::<DocRevision>(&guard.revision, true)?;
        match (&guard.document, revision) {
            (None, None) => {}
            (Some(id), Some(row))
                if row.base.space_id == space
                    && row.base.deleted_at.is_none()
                    && row.doc.id() == id => {}
            _ => return Err(changed()),
        }
        if let Some(id) = &guard.document {
            let document = tx.get_by_id::<Doc>(id, true)?;
            match (guard.document_present, document) {
                (false, None) => {}
                (true, Some(row))
                    if row.base.space_id == space
                        && row.base.deleted_at.is_none()
                        && row.source.as_ref().map(elephant_types::Link::id)
                            == guard.source.as_ref() => {}
                _ => return Err(changed()),
            }
        }
        if let Some(source) = &guard.source {
            let key = format!(
                "source_head:{}",
                crate::source_publication::head_key(space, source)
            );
            // Global identity, not a space/payload filter: malformed authority
            // must never be mistaken for a standalone document.
            if tx
                .scan_extension_rows("source_head")?
                .iter()
                .any(|(id, _)| id == &key)
            {
                return Err(changed());
            }
        }
    }
    Ok(())
}

pub fn append_sql(
    sql: &mut String,
    bindings: &mut BTreeMap<String, Value>,
    space: &str,
    guards: &[Ownership],
) {
    if guards.is_empty() {
        return;
    }
    bindings.insert("unmanaged_space".into(), space.into());
    for (index, guard) in guards.iter().enumerate() {
        let p = format!("unmanaged_{index}");
        bindings.insert(
            format!("{p}_node"),
            crate::surreal::thing_from_record_id(&guard.node).into(),
        );
        bindings.insert(
            format!("{p}_revision"),
            crate::surreal::thing_from_record_id(&guard.revision).into(),
        );
        let _ = writeln!(sql, "LET ${p}_n = SELECT * FROM ONLY ${p}_node;");
        let _ = writeln!(sql, "IF ${p}_n = NONE OR ${p}_n.space_id != $unmanaged_space OR ${p}_n.deleted_at != NONE OR ${p}_n.revision != ${p}_revision {{ THROW 'source_embedding_requires_original_claim'; }};");
        let _ = writeln!(sql, "LET ${p}_r = SELECT * FROM ONLY ${p}_revision;");
        if let Some(document) = &guard.document {
            bindings.insert(
                format!("{p}_document"),
                crate::surreal::thing_from_record_id(document).into(),
            );
            let _ = writeln!(sql, "IF ${p}_r = NONE OR ${p}_r.space_id != $unmanaged_space OR ${p}_r.deleted_at != NONE OR ${p}_r.doc != ${p}_document {{ THROW 'source_embedding_requires_original_claim'; }};");
            let _ = writeln!(sql, "LET ${p}_d = SELECT * FROM ONLY ${p}_document;");
            if guard.document_present {
                bindings.insert(
                    format!("{p}_source"),
                    guard.source.as_ref().map_or(Value::None, |id| {
                        crate::surreal::thing_from_record_id(id).into()
                    }),
                );
                let _ = writeln!(sql, "IF ${p}_d = NONE OR ${p}_d.space_id != $unmanaged_space OR ${p}_d.deleted_at != NONE OR ${p}_d.source != ${p}_source {{ THROW 'source_embedding_requires_original_claim'; }};");
            } else {
                let _ = writeln!(
                    sql,
                    "IF ${p}_d != NONE {{ THROW 'source_embedding_requires_original_claim'; }};"
                );
            }
        } else {
            let _ = writeln!(
                sql,
                "IF ${p}_r != NONE {{ THROW 'source_embedding_requires_original_claim'; }};"
            );
        }
        if let Some(source) = &guard.source {
            bindings.insert(
                format!("{p}_head"),
                crate::source_publication::control_thing(
                    "source_head",
                    &crate::source_publication::head_key(space, source),
                )
                .into(),
            );
            let _ = writeln!(sql, "LET ${p}_h = SELECT id FROM ONLY ${p}_head; IF ${p}_h != NONE {{ THROW 'source_embedding_requires_original_claim'; }};");
        }
    }
    // Existing migration031 makes Doc/DocRevision/source_head changes write
    // this same epoch. A real write fences absent rows and changes occurring
    // after the checks, even when an embedding marker update would be a no-op.
    sql.push_str("UPSERT type::thing('canonical_epoch', $unmanaged_space) SET space_id=$unmanaged_space, epoch=<string>rand::uuid();\n");
}

#[cfg(test)]
#[path = "unmanaged_embedding_tests.rs"]
mod tests;
