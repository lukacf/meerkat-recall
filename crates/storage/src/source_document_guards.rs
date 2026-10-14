//! Exact caller/current document authorization observations for source mutation.
//! These ephemeral guards add no durable authority or historical payload copy.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use elephant_types::publication::{PublicationFence, SourceHead};
use elephant_types::records::Doc;
use elephant_types::{RecordId, TableName};
use surrealdb::sql::Value;

use crate::canonical_commit::{append_row_guard, CanonicalRecord};
use crate::source_publication::{
    conflict, control_thing, encode_payload, head_key, integrity, read_control_tx,
    AcceptSourceRevision, SourcePublicationRepository, HEAD_TABLE,
};
use crate::{EmbeddedMutation, StorageResult};

/// Original document snapshots which have all passed adapter authorization.
///
/// The captured head is the same observation used to locate the current physical document.
/// It is never refreshed to make an old authorization pass.
#[derive(Debug, Clone)]
pub struct PreparedDocGuards {
    space: String,
    source_id: RecordId,
    head: Option<SourceHead>,
    documents: Vec<Doc>,
    absent_documents: Vec<RecordId>,
    manifest: Option<(
        PublicationFence,
        Option<crate::source_publication_manifest::SourcePublicationManifest>,
    )>,
}

impl PreparedDocGuards {
    /// Seals the caller's document and the current accepted physical document.
    /// `None` means an observed absent head, for an actual legacy source.
    ///
    /// # Errors
    /// Rejects foreign identity/scope or inconsistent duplicates. Empty present
    /// rows permit an expected physical document which has not been staged yet.
    pub fn from_authorized_documents(
        space: &str,
        source_id: &RecordId,
        head: Option<&SourceHead>,
        documents: &[Doc],
    ) -> StorageResult<Self> {
        if space.trim().is_empty()
            || source_id.table() != TableName::Source
            || head.is_some_and(|head| head.space_id != space || head.source_id != *source_id)
        {
            return Err(integrity("invalid document authorization scope"));
        }
        let mut unique = BTreeMap::new();
        for document in documents {
            if document.base.id.table() != TableName::Doc
                || document.base.space_id != space
                || document
                    .source
                    .as_ref()
                    .is_some_and(|source| source.id() != source_id)
            {
                return Err(integrity("authorized document belongs to another source"));
            }
            if unique
                .insert(document.base.id.clone(), document.clone())
                .is_some_and(|before| before != *document)
            {
                return Err(integrity("inconsistent document authorization snapshots"));
            }
        }
        Ok(Self {
            space: space.into(),
            source_id: source_id.clone(),
            head: head.cloned(),
            documents: unique.into_values().collect(),
            absent_documents: Vec::new(),
            manifest: None,
        })
    }

    /// Adds original absent-document observations, including accepted input
    /// whose physical document staging has not succeeded.
    ///
    /// A source-less caller
    /// document may instead appear in the present snapshot set above.
    pub fn with_absent_documents(mut self, ids: &[RecordId]) -> StorageResult<Self> {
        for id in ids {
            if id.table() != TableName::Doc
                || self
                    .documents
                    .iter()
                    .any(|document| document.base.id == *id)
            {
                return Err(integrity("inconsistent document absence observation"));
            }
            if !self.absent_documents.contains(id) {
                self.absent_documents.push(id.clone());
            }
        }
        self.absent_documents.sort();
        Ok(self)
    }

    /// Retains the exact manifest read used to enumerate current physical Docs.
    ///
    /// In particular, absent staging must not become a new unobserved manifest
    /// under the same processing head between authorization and acceptance.
    pub fn with_manifest_observation(
        mut self,
        original: &PublicationFence,
        observed: Option<&crate::source_publication_manifest::SourcePublicationManifest>,
    ) -> StorageResult<Self> {
        if original.source_id != self.source_id
            || observed.is_some_and(|manifest| {
                manifest.space_id != self.space || manifest.publication != *original
            })
        {
            return Err(integrity("foreign document manifest observation"));
        }
        let observation = (original.clone(), observed.cloned());
        if let Some(before) = &self.manifest {
            if encode_payload(before)? != encode_payload(&observation)? {
                return Err(conflict(
                    "document manifest observation cannot be refreshed",
                ));
            }
        }
        self.manifest = Some(observation);
        Ok(self)
    }

    pub(crate) fn validate_scope(&self, space: &str, source_id: &RecordId) -> StorageResult<()> {
        if self.space != space || self.source_id != *source_id {
            return Err(conflict("authorized_document_source_changed"));
        }
        Ok(())
    }

    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "This guard is called within the exclusive mutation protocol; preserve its existing mutable transaction-borrow interface and lifetime through validation."
    )]
    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        let head: Option<SourceHead> = read_control_tx(
            tx,
            HEAD_TABLE,
            &head_key(&self.space, &self.source_id),
            &self.space,
        )?;
        let head_physical_id = format!("{HEAD_TABLE}:{}", head_key(&self.space, &self.source_id));
        let foreign_or_malformed_head = self.head.is_none()
            && tx
                .scan_extension_rows(HEAD_TABLE)?
                .iter()
                .any(|(key, _)| key == &head_physical_id);
        if foreign_or_malformed_head
            || head.as_ref().map(encode_payload).transpose()?
                != self.head.as_ref().map(encode_payload).transpose()?
        {
            return Err(conflict("authorized_document_head_changed"));
        }
        for document in &self.documents {
            if tx
                .get::<Doc>(&self.space, &document.base.id, true)?
                .as_ref()
                != Some(document)
            {
                return Err(conflict("authorized_document_changed"));
            }
        }
        for id in &self.absent_documents {
            if tx.get_by_id::<Doc>(id, true)?.is_some() {
                return Err(conflict("authorized_document_appeared"));
            }
        }
        if let Some((publication, observed)) = &self.manifest {
            let current: Option<crate::source_publication_manifest::SourcePublicationManifest> =
                read_control_tx(
                    tx,
                    crate::source_publication_manifest::MANIFEST_TABLE,
                    &crate::source_publication_manifest::manifest_key(&self.space, publication),
                    &self.space,
                )?;
            let table = crate::source_publication_manifest::MANIFEST_TABLE;
            let physical_id = format!(
                "{table}:{}",
                crate::source_publication_manifest::manifest_key(&self.space, publication)
            );
            let foreign_or_malformed_manifest = observed.is_none()
                && tx
                    .scan_extension_rows(table)?
                    .iter()
                    .any(|(key, _)| key == &physical_id);
            if foreign_or_malformed_manifest
                || current.as_ref().map(encode_payload).transpose()?
                    != observed.as_ref().map(encode_payload).transpose()?
            {
                return Err(conflict("authorized_document_manifest_changed"));
            }
        }
        Ok(())
    }

    /// Compose several original source/document observations without replacing
    /// one another's bindings.
    ///
    /// `$space` remains the caller's single named space.
    /// An empty prefix preserves the original single-source binding names.
    pub(crate) fn append_guard_sql_scoped(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
        prefix: &str,
    ) -> StorageResult<()> {
        if !prefix
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            return Err(integrity("invalid document guard namespace"));
        }
        let mut guarded_sql = String::new();
        let mut guarded_bindings = BTreeMap::new();
        self.append_guard_sql_unscoped(&mut guarded_sql, &mut guarded_bindings)?;
        let namespace = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}_")
        };
        sql.push_str(&guarded_sql.replace("$authorized_", &format!("${namespace}authorized_")));
        for (key, value) in guarded_bindings {
            let suffix = key
                .strip_prefix("authorized_")
                .ok_or_else(|| integrity("unexpected document guard binding"))?;
            let scoped = format!("{namespace}authorized_{suffix}");
            if bindings.insert(scoped, value).is_some() {
                return Err(integrity("duplicate document guard namespace"));
            }
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        self.append_guard_sql_scoped(sql, bindings, "")
    }

    fn append_guard_sql_unscoped(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        bindings.insert("authorized_docs_space".into(), self.space.clone().into());
        bindings.insert(
            "authorized_docs_head".into(),
            control_thing(HEAD_TABLE, &head_key(&self.space, &self.source_id)).into(),
        );
        bindings.insert(
            "authorized_docs_head_payload".into(),
            self.head.as_ref().map_or(Ok(Value::None), |head| {
                encode_payload(head).map(Value::from)
            })?,
        );
        if self.head.is_some() {
            sql.push_str("IF (SELECT VALUE space_id FROM ONLY $authorized_docs_head) != $authorized_docs_space OR (SELECT VALUE payload FROM ONLY $authorized_docs_head) != $authorized_docs_head_payload { THROW 'publication:authorized_document_head_changed'; };\n");
        } else {
            sql.push_str("IF (SELECT * FROM ONLY $authorized_docs_head) != NONE { THROW 'publication:authorized_document_head_changed'; };\n");
        }
        // Migration 043 installs this exact same typed-identity function on all
        // actual writers. It performs a real bounded metadata write, including
        // absent keys and a foreign-space writer at the same physical identity.
        sql.push_str("fn::recall_physical_read_fence($authorized_docs_head);\n");
        for (index, document) in self.documents.iter().enumerate() {
            let row = CanonicalRecord::Doc(Box::new(document.clone()));
            append_row_guard(
                sql,
                bindings,
                &format!("authorized_docs_{index}"),
                &row,
                Some(&row),
            )?;
            let _ = writeln!(
                sql,
                "fn::recall_physical_read_fence($authorized_docs_{index}_id);"
            );
        }
        for (index, id) in self.absent_documents.iter().enumerate() {
            bindings.insert(
                format!("authorized_absent_doc_{index}"),
                crate::surreal::thing_from_record_id(id).into(),
            );
            let _ = writeln!(sql,"IF (SELECT * FROM ONLY $authorized_absent_doc_{index}) != NONE {{ THROW 'publication:authorized_document_appeared'; }};\nfn::recall_physical_read_fence($authorized_absent_doc_{index});");
        }
        if let Some((publication, observed)) = &self.manifest {
            bindings.insert(
                "authorized_docs_manifest".into(),
                control_thing(
                    crate::source_publication_manifest::MANIFEST_TABLE,
                    &crate::source_publication_manifest::manifest_key(&self.space, publication),
                )
                .into(),
            );
            bindings.insert(
                "authorized_docs_manifest_payload".into(),
                observed.as_ref().map_or(Ok(Value::None), |manifest| {
                    encode_payload(manifest).map(Value::from)
                })?,
            );
            if observed.is_some() {
                sql.push_str("IF (SELECT VALUE space_id FROM ONLY $authorized_docs_manifest) != $authorized_docs_space OR (SELECT VALUE payload FROM ONLY $authorized_docs_manifest) != $authorized_docs_manifest_payload { THROW 'publication:authorized_document_manifest_changed'; };\n");
            } else {
                sql.push_str("IF (SELECT * FROM ONLY $authorized_docs_manifest) != NONE { THROW 'publication:authorized_document_manifest_changed'; };\n");
            }
            sql.push_str("fn::recall_physical_read_fence($authorized_docs_manifest);\n");
        }
        Ok(())
    }
}

impl SourcePublicationRepository {
    /// Accepts only if every original authorized document and head still match.
    /// A matching original operation is a read-only replay before these guards.
    pub async fn accept_with_document_guards(
        &self,
        request: AcceptSourceRevision,
        guards: &PreparedDocGuards,
        contact: Option<&crate::source_contact_intake::PreparedSourceContact>,
    ) -> StorageResult<elephant_types::publication::OperationReceipt> {
        self.accept_inner(request, None, None, contact, None, Some(guards), None)
            .await
    }
}

// The maintenance lane owns this read-only intake selector consumer. Keep it a
// child so it can verify original private observations without exposing fields.
#[path = "standing_dispatch_inputs.rs"]
pub mod standing_dispatch_inputs;
