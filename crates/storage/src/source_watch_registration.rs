//! Original Source/Doc/Watch read sets for explicit recurring-watch authority.
//!
//! Registration writes one Watch and its private delegation in one transaction. This helper
//! never accepts a source revision or creates publication authority.

use std::collections::BTreeMap;

use elephant_policy::{AbacEngine, AccessDecision, Principal, RecordAttributes};
use elephant_types::publication::SourceVisibility;
use elephant_types::records::{Doc, HasSecurity, Source, SourceType, SourceWatch};
use elephant_types::TableName;
use surrealdb::sql::Value;

use crate::canonical_commit::{append_row_guard, native_expected_fields, CanonicalRecord};
use crate::source_document_guards::PreparedDocGuards;
use crate::source_publication::SourcePublicationRepository;
use crate::standing_watch_repository::{PreparedStandingWatchRegistration, StandingWatchIdentity};
use crate::{EmbeddedMutation, SourceWatchRepository, StorageClient, StorageError, StorageResult};

/// Exact original records which must pass preflight and remain current at write.
#[derive(Debug, Clone)]
pub struct PreparedSourceWatchRegistration {
    source: Source,
    previous: Option<SourceWatch>,
    proposed: SourceWatch,
    documents: Vec<Doc>,
    document_guards: PreparedDocGuards,
}

impl PreparedSourceWatchRegistration {
    /// Capture affected current document identities under the original active
    /// head, retaining the Source/Watch observations supplied by the caller.
    pub async fn prepare(
        storage: &StorageClient,
        source: &Source,
        previous: Option<&SourceWatch>,
        proposed: SourceWatch,
    ) -> StorageResult<Self> {
        if source.type_ != SourceType::Web
            || source.base.deleted_at.is_some()
            || proposed.base.id.table() != TableName::SourceWatch
            || proposed.base.deleted_at.is_some()
            || proposed.base.space_id != source.base.space_id
            || proposed.source.id() != &source.base.id
            || proposed.interval_seconds <= 0
            || previous.is_some_and(|old| {
                old.base.deleted_at.is_some()
                    || old.base.id != proposed.base.id
                    || old.source != proposed.source
                    || old.base.space_id != proposed.base.space_id
            })
        {
            return Err(StorageError::DataIntegrityError(
                "standing_watch_registration_scope".into(),
            ));
        }
        let head = SourcePublicationRepository::new(storage.clone())
            .head(&source.base.space_id, &source.base.id)
            .await?
            .filter(|head| head.lifecycle == SourceVisibility::Active)
            .ok_or_else(|| StorageError::Conflict("standing_watch_source_not_active".into()))?;
        let (documents, document_guards) =
            crate::source_intake::current_watch_document_guards(storage, &head).await?;
        Ok(Self {
            source: source.clone(),
            previous: previous.cloned(),
            proposed,
            documents,
            document_guards,
        })
    }

    /// Original live Source, also fenced when no current Doc has been staged.
    #[must_use]
    pub const fn source(&self) -> &Source {
        &self.source
    }

    /// Every currently materialized Doc in the original desired input/manifest.
    #[must_use]
    pub fn documents(&self) -> &[Doc] {
        &self.documents
    }

    /// Existing Watch before any envelope or schedule changes.
    #[must_use]
    pub const fn previous(&self) -> Option<&SourceWatch> {
        self.previous.as_ref()
    }

    /// Complete proposed Watch for registration or the unchanged enqueue input.
    #[must_use]
    pub const fn watch(&self) -> &SourceWatch {
        &self.proposed
    }

    /// Full old/new ABAC under the caller's actual bounded principal. A tool
    /// profile or development skip-auth switch cannot grant standing authority.
    pub fn authorize(&self, principal: &Principal) -> StorageResult<()> {
        let mut principal = principal.clone();
        for scope in ["read:source", "write:source", "read:doc", "write:doc"] {
            if principal.scopes.contains("*") {
                principal.scopes.insert(scope.into());
            }
            if !principal.scopes.contains(scope) {
                return Err(StorageError::Conflict(format!(
                    "standing_watch_missing_scope:{scope}"
                )));
            }
        }
        authorize_standing_source_input(
            &principal,
            &CanonicalRecord::Source(Box::new(self.source.clone())),
        )?;
        if let Some(previous) = &self.previous {
            authorize_standing_watch_record(&principal, previous, ["read:source", "write:source"])?;
        }
        authorize_standing_watch_record(
            &principal,
            &self.proposed,
            ["read:source", "write:source"],
        )?;
        for doc in &self.documents {
            authorize_standing_source_input(
                &principal,
                &CanonicalRecord::Doc(Box::new(doc.clone())),
            )?;
        }
        Ok(())
    }

    /// Commit the Watch and its explicit fresh delegation together.
    ///
    /// The proof
    /// must be prepared for this exact proposed Watch; it cannot be substituted
    /// for a different registration or inferred from an existing record marker.
    pub async fn register(
        self,
        storage: &StorageClient,
        authority: &PreparedStandingWatchRegistration,
    ) -> StorageResult<(SourceWatch, StandingWatchIdentity)> {
        if authority.watch() != &self.proposed {
            return Err(StorageError::Conflict(
                "standing_watch_registration_changed".into(),
            ));
        }
        self.authorize(authority.principal())?;
        if let Some(store) = storage.embedded_store() {
            store
                .mutate(|tx| {
                    self.check_tx(tx)?;
                    authority.persist_tx(tx)?;
                    if self.previous.is_some() {
                        tx.replace(self.proposed.clone())?;
                    } else {
                        tx.create(self.proposed.clone())?;
                    }
                    Ok(())
                })
                .await?;
        } else {
            let mut bindings = BTreeMap::new();
            let mut sql = String::from("BEGIN TRANSACTION;\n");
            self.append_guard_sql(&mut sql, &mut bindings)?;
            authority.append_persist_sql(&mut sql, &mut bindings)?;
            bindings.insert(
                "standing_registration_content".into(),
                SourceWatchRepository::watch_to_value(&self.proposed)?,
            );
            if self.previous.is_some() {
                sql.push_str(
                    "UPDATE $standing_registration_watch CONTENT $standing_registration_content;\n",
                );
            } else {
                sql.push_str(
                    "CREATE $standing_registration_watch CONTENT $standing_registration_content;\n",
                );
            }
            sql.push_str("COMMIT TRANSACTION;");
            storage
                .query_bind_checked(&sql, bindings)
                .await
                .map_err(crate::source_publication::publication_error)?;
        }
        Ok((self.proposed, authority.identity().clone()))
    }

    /// Same original record guard is reused inside the queue owner's mutation.
    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        let space = &self.source.base.space_id;
        if tx
            .get::<Source>(space, &self.source.base.id, false)?
            .as_ref()
            != Some(&self.source)
            || tx
                .get_by_id::<SourceWatch>(&self.proposed.base.id, true)?
                .as_ref()
                != self.previous.as_ref()
            || tx.scan::<SourceWatch>()?.iter().any(|watch| {
                watch.base.space_id == *space
                    && watch.base.deleted_at.is_none()
                    && watch.source.id() == &self.source.base.id
                    && self.previous.as_ref().map(|old| &old.base.id) != Some(&watch.base.id)
            })
        {
            return Err(StorageError::Conflict(
                "standing_watch_records_changed".into(),
            ));
        }
        self.document_guards.check_tx(tx)
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        bindings.extend(crate::bindings! {
            "space" => self.source.base.space_id.clone(),
            "standing_registration_watch" => crate::surreal::thing_from_record_id(&self.proposed.base.id),
            "standing_registration_source" => crate::surreal::thing_from_record_id(&self.source.base.id),
        });
        sql.push_str("LET $standing_registration_current = SELECT * FROM ONLY $standing_registration_watch;\n");
        if let Some(previous) = &self.previous {
            bindings.insert(
                "standing_registration_previous".into(),
                native_expected_fields(SourceWatchRepository::watch_to_value(previous)?),
            );
            sql.push_str("IF $standing_registration_current = NONE { THROW 'publication:standing_watch_records_changed'; };\nFOR $entry IN object::entries($standing_registration_previous) { IF $standing_registration_current[$entry[0]] != $entry[1] { THROW 'publication:standing_watch_records_changed'; }; };\n");
        } else {
            sql.push_str("IF $standing_registration_current != NONE { THROW 'publication:standing_watch_records_changed'; };\n");
        }
        sql.push_str("IF array::len(SELECT id FROM source_watch WHERE space_id=$space AND source=$standing_registration_source AND deleted_at IS NONE AND id!=$standing_registration_watch) != 0 { THROW 'publication:standing_watch_records_changed'; };\n");
        let source = CanonicalRecord::Source(Box::new(self.source.clone()));
        append_row_guard(
            sql,
            bindings,
            "standing_registration_source_row",
            &source,
            Some(&source),
        )?;
        // Source is SCHEMAFULL. Its actual writers advance this existing
        // private revision; a same-value Source UPDATE would be optimized away.
        sql.push_str("fn::recall_reference_guard({ id:$standing_registration_source, space_id:$space },false);\n");
        self.document_guards.append_guard_sql(sql, bindings)
    }
}

/// Evaluate one old or proposed affected record under a preserved standing
/// ceiling. Callers still retain and check the exact record in their mutation.
pub fn authorize_standing_watch_record(
    principal: &Principal,
    record: &impl HasSecurity,
    scopes: [&str; 2],
) -> StorageResult<()> {
    let mut principal = principal.clone();
    if principal.scopes.contains("*") {
        principal
            .scopes
            .extend(scopes.iter().map(|scope| (*scope).to_owned()));
    }
    let attributes = RecordAttributes {
        space_id: record.space_id().into(),
        security: record.security().clone(),
        subjects: record.subjects(),
        subjects_state: record.subjects_state(),
    };
    for scope in scopes {
        if let AccessDecision::Deny(reason) =
            AbacEngine::new().evaluate(&principal, &attributes, scope)
        {
            return Err(StorageError::Conflict(format!(
                "standing_watch_access_denied:{reason}"
            )));
        }
    }
    Ok(())
}

/// Shared policy for actual source-processing inputs and proposed images.
///
/// This function is pure: it grants no execution authority and captures no row. The caller
/// retains original rows and checks lifecycle in its own transaction.
///
/// # Errors
/// Refuses unsupported record kinds, missing required capabilities, or a denied
/// full security/subject envelope. Artifacts use the existing document scopes.
pub fn authorize_standing_source_input(
    principal: &Principal,
    record: &CanonicalRecord,
) -> StorageResult<()> {
    let scopes = match record {
        CanonicalRecord::Source(_) => ["read:source", "write:source"],
        CanonicalRecord::Doc(_)
        | CanonicalRecord::DocRevision(_)
        | CanonicalRecord::DocNode(_)
        | CanonicalRecord::WorkArtifact(_) => ["read:doc", "write:doc"],
        _ => {
            return Err(StorageError::Conflict(
                "standing_watch_unsupported_input".into(),
            ))
        }
    };
    authorize_standing_watch_record(principal, &InputEnvelope(record.base()), scopes)
}

/// Apply the existing document capabilities to one managed media input.
/// This is pure policy; callers retain and compare the actual original asset.
///
/// # Errors
/// Refuses denied scope, security or subject access without inventing an asset
/// capability or treating the caller's actor string as authority.
pub fn authorize_standing_media_asset(
    principal: &Principal,
    asset: &elephant_types::records::MediaAsset,
) -> StorageResult<()> {
    authorize_standing_watch_record(
        principal,
        &InputEnvelope(&asset.base),
        ["read:doc", "write:doc"],
    )
}

// Input artifacts and managed assets have no HasSecurity implementation.
// This private projection preserves their complete base-field envelope.
struct InputEnvelope<'a>(&'a elephant_types::records::BaseFields);
impl HasSecurity for InputEnvelope<'_> {
    fn space_id(&self) -> &str {
        &self.0.space_id
    }
    fn security(&self) -> &elephant_types::records::Security {
        &self.0.security
    }
    fn subjects(&self) -> Vec<elephant_types::Link<elephant_types::records::Entity>> {
        self.0.subjects.clone()
    }
    fn subjects_state(&self) -> elephant_types::records::SubjectsState {
        self.0.subjects_state
    }
}

#[cfg(test)]
mod input_policy_tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use elephant_types::records::{BaseFields, SubjectsState};
    use elephant_types::{Link, RecordId};
    use std::collections::HashSet;

    fn principal() -> Principal {
        Principal {
            space_id: "input-policy".into(),
            scopes: ["read:source", "write:source", "read:doc", "write:doc"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            clearances: vec![elephant_policy::Clearance {
                policy_id: "default".into(),
                max_level: 3,
                labels: HashSet::from(["personal".into()]),
                handling_allow: HashSet::from(["restricted".into()]),
            }],
            subject_allowlist: None,
            purpose: HashSet::new(),
        }
    }

    fn input(table: TableName) -> Option<CanonicalRecord> {
        let mut base = BaseFields::new(
            RecordId::new_v4(table),
            "input-policy".into(),
            "fixture".into(),
        );
        base.security.level = 3;
        base.security.labels.push("personal".into());
        base.security.handling.push("restricted".into());
        base.subjects
            .push(Link::new(RecordId::new_v4(TableName::Entity)));
        base.subjects_state = SubjectsState::Complete;
        let mut row = serde_json::to_value(base).unwrap();
        let fields = match table {
            TableName::Source => serde_json::json!({"type":"manual", "hash":"input-hash"}),
            TableName::Doc => {
                serde_json::json!({"title":"Input", "text":"actual input", "rendition_key":"input"})
            }
            TableName::DocRevision => {
                serde_json::json!({"doc":RecordId::new_v4(TableName::Doc), "text":"actual input", "text_hash":"input-hash", "renderer":{"name":"fixture","version":"1"}, "rendered_at":chrono::Utc::now()})
            }
            TableName::DocNode => {
                serde_json::json!({"revision":RecordId::new_v4(TableName::DocRevision), "role":"document", "span":{"start":0,"end":12}, "node_key":"node"})
            }
            TableName::WorkArtifact => {
                serde_json::json!({"work_run":RecordId::new_v4(TableName::WorkRun), "artifact_type":"knowledge_candidates", "data":{"assertions":[]}})
            }
            _ => return None,
        };
        row.as_object_mut()
            .unwrap()
            .extend(fields.as_object().unwrap().clone());
        Some(match table {
            TableName::Source => {
                CanonicalRecord::Source(Box::new(serde_json::from_value(row).unwrap()))
            }
            TableName::Doc => CanonicalRecord::Doc(Box::new(serde_json::from_value(row).unwrap())),
            TableName::DocRevision => {
                CanonicalRecord::DocRevision(Box::new(serde_json::from_value(row).unwrap()))
            }
            TableName::DocNode => {
                CanonicalRecord::DocNode(Box::new(serde_json::from_value(row).unwrap()))
            }
            TableName::WorkArtifact => {
                CanonicalRecord::WorkArtifact(Box::new(serde_json::from_value(row).unwrap()))
            }
            _ => return None,
        })
    }

    #[test]
    fn managed_media_keeps_document_scope_and_full_subject_restrictions() {
        let row = input(TableName::Doc).unwrap();
        let mut base = row.base().clone();
        base.id = RecordId::new_v4(TableName::MediaAsset);
        let mut value = serde_json::to_value(base).unwrap();
        value.as_object_mut().unwrap().extend(
            serde_json::json!({
                "kind":"image", "media_type":"image/png", "size_bytes":3,
                "sha256":"fixture", "storage_path":"fixture", "payload_base64":"YWJj",
            })
            .as_object()
            .unwrap()
            .clone(),
        );
        let asset: elephant_types::records::MediaAsset = serde_json::from_value(value).unwrap();
        let allowed = principal();
        authorize_standing_media_asset(&allowed, &asset).unwrap();
        for missing in ["read:doc", "write:doc"] {
            let mut denied = allowed.clone();
            denied.scopes.remove(missing);
            assert!(authorize_standing_media_asset(&denied, &asset).is_err());
        }
        let mut denied = allowed.clone();
        denied.clearances[0].handling_allow.clear();
        assert!(authorize_standing_media_asset(&denied, &asset).is_err());
        let mut denied = allowed;
        denied.subject_allowlist = Some(HashSet::new());
        assert!(authorize_standing_media_asset(&denied, &asset).is_err());
    }

    #[test]
    fn all_physical_inputs_require_their_existing_capabilities_and_full_envelope() {
        for table in [
            TableName::Source,
            TableName::Doc,
            TableName::DocRevision,
            TableName::DocNode,
            TableName::WorkArtifact,
        ] {
            let row = input(table).unwrap();
            let full = principal();
            authorize_standing_source_input(&full, &row).unwrap();
            let capabilities = if table == TableName::Source {
                ["read:source", "write:source"]
            } else {
                ["read:doc", "write:doc"]
            };
            for missing in capabilities {
                let mut denied = full.clone();
                denied.scopes.remove(missing);
                // Invented artifact authority never substitutes for document access.
                denied
                    .scopes
                    .extend(["read:work_artifact".into(), "write:work_artifact".into()]);
                assert!(authorize_standing_source_input(&denied, &row).is_err());
            }
            let mut denied = full.clone();
            denied.clearances[0].handling_allow.clear();
            assert!(authorize_standing_source_input(&denied, &row).is_err());
            let mut denied = full.clone();
            denied.clearances[0].max_level = 2;
            assert!(authorize_standing_source_input(&denied, &row).is_err());
            let mut denied = full.clone();
            denied.subject_allowlist = Some(HashSet::new());
            assert!(authorize_standing_source_input(&denied, &row).is_err());
            let mut denied = full.clone();
            denied.space_id = "foreign".into();
            assert!(authorize_standing_source_input(&denied, &row).is_err());
            let mut unknown = row.clone();
            unknown.base_mut().subjects_state = SubjectsState::Unknown;
            let mut scoped = full;
            scoped.subject_allowlist = Some(
                row.base()
                    .subjects
                    .iter()
                    .map(|link| link.id().clone())
                    .collect(),
            );
            assert!(authorize_standing_source_input(&scoped, &unknown).is_err());
        }
    }
}
