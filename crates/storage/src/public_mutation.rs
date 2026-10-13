//! Public guarded mutations through the sole canonical transaction boundary.
//!
//! Capture the epoch before reading or authorizing records. The exact authorized
//! before-image, derived support, audit record, semantic outbox event and receipt
//! are committed together. No mutation succeeds with warning-only propagation.

use chrono::Utc;
use elephant_types::records::base::BaseFields;
use elephant_types::records::{
    Assertion, Attribute, AuditAction, AuditLog, Doc, Entity, EntityIdentityKey, Event,
    IdentityCandidate, PredicateAlias, PredicateRegistry, Rel, ReprocessJob, SystemEvent,
    WorkArtifact,
};
use elephant_types::{RecordId, TableName};
use serde::{de::DeserializeOwned, Serialize};
use sha2::{Digest, Sha256};

use crate::canonical_commit::{
    CanonicalCommit, CanonicalEffect, CanonicalMaintenanceContext, CanonicalMaintenanceOrigin,
    CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use crate::{StorageClient, StorageError, StorageResult};

/// Join an input record's restrictions into a newly derived public record.
/// Different policy domains cannot be collapsed into one ordinary envelope.
pub fn inherit_public_envelope(output: &mut BaseFields, input: &BaseFields) -> StorageResult<()> {
    use elephant_types::records::base::SubjectsState;
    if output.space_id != input.space_id || output.security.policy_id != input.security.policy_id {
        return Err(conflict(
            "derived records require one selected space and policy domain",
        ));
    }
    output.security.level = output.security.level.max(input.security.level);
    for label in &input.security.labels {
        if !output.security.labels.contains(label) {
            output.security.labels.push(label.clone());
        }
    }
    for handling in &input.security.handling {
        if !output.security.handling.contains(handling) {
            output.security.handling.push(handling.clone());
        }
    }
    for subject in &input.subjects {
        if !output.subjects.contains(subject) {
            output.subjects.push(subject.clone());
        }
    }
    output.subjects_state = match (output.subjects_state, input.subjects_state) {
        (SubjectsState::Complete, SubjectsState::Complete) => SubjectsState::Complete,
        (SubjectsState::Unknown, _) | (_, SubjectsState::Unknown) => SubjectsState::Unknown,
        _ => SubjectsState::Partial,
    };
    for source in &input.provenance.sources {
        if !output.provenance.sources.contains(source) {
            output.provenance.sources.push(source.clone());
        }
    }
    Ok(())
}

async fn relationship_assertion_dependencies(
    client: &StorageClient,
    space: &str,
    rel: &Rel,
    assertion: &Assertion,
) -> StorageResult<Vec<CanonicalRecord>> {
    if rel.base.space_id != space
        || assertion.base.space_id != space
        || assertion.base.deleted_at.is_some()
        || assertion
            .subject_entity
            .as_ref()
            .map(elephant_types::Link::id)
            != Some(rel.subject.id())
        || assertion
            .object_entity
            .as_ref()
            .map(elephant_types::Link::id)
            != Some(rel.object.id())
        || assertion.object_literal.is_some()
    {
        return Err(conflict(
            "relationship endpoints differ from referenced assertion",
        ));
    }
    predicate_assertion_dependencies(client, space, &rel.rel_type, assertion).await
}

async fn predicate_assertion_dependencies(
    client: &StorageClient,
    space: &str,
    predicate: &str,
    assertion: &Assertion,
) -> StorageResult<Vec<CanonicalRecord>> {
    let aliases = crate::PredicateAliasRepository::new(client.clone());
    let mut dependencies = Vec::new();
    let mut predicates = Vec::new();
    for predicate in [
        predicate,
        assertion
            .predicate_id
            .as_deref()
            .unwrap_or(&assertion.predicate_raw),
    ] {
        let predicate = predicate.trim();
        if predicate.is_empty() {
            return Err(conflict("claim assertion predicate is empty"));
        }
        if let Some(alias) = aliases.find_active_by_raw(space, predicate).await? {
            predicates.push(alias.predicate_id.clone());
            dependencies.push(CanonicalRecord::PredicateAlias(Box::new(alias)));
        } else {
            predicates.push(predicate.to_owned());
        }
    }
    if predicates[0] != predicates[1] {
        return Err(conflict(
            "claim predicate differs from referenced assertion",
        ));
    }
    Ok(dependencies)
}

async fn attribute_assertion_dependencies(
    client: &StorageClient,
    space: &str,
    attribute: &Attribute,
    assertion: &Assertion,
) -> StorageResult<Vec<CanonicalRecord>> {
    use elephant_types::records::attribute::Literal;
    let value = match &attribute.value {
        Literal::String(value)
        | Literal::Date(value)
        | Literal::DateTime(value)
        | Literal::Duration(value) => serde_json::Value::String(value.clone()),
        Literal::Integer(value) => serde_json::Value::Number((*value).into()),
        Literal::Float(value) => serde_json::Value::Number(
            serde_json::Number::from_f64(*value)
                .ok_or_else(|| conflict("claim literal is not finite"))?,
        ),
        Literal::Boolean(value) => serde_json::Value::Bool(*value),
        Literal::Json(value) => value.clone(),
    };
    if attribute.base.space_id != space
        || assertion.base.space_id != space
        || assertion.base.deleted_at.is_some()
        || assertion
            .subject_entity
            .as_ref()
            .map(elephant_types::Link::id)
            != Some(attribute.subject.id())
        || assertion.object_entity.is_some()
        || assertion.object_literal.as_ref() != Some(&value)
    {
        return Err(conflict(
            "attribute subject or literal differs from referenced assertion",
        ));
    }
    predicate_assertion_dependencies(client, space, &attribute.attr_type, assertion).await
}

/// Domain records accepted by the public guarded mutation adapter.
pub trait PublicRecord: Clone + Serialize + DeserializeOwned + Send + Sync {
    /// Complete authorization and lifecycle envelope.
    fn public_base(&self) -> &BaseFields;
    /// Complete authorization and lifecycle envelope, for guarded preparation.
    fn public_base_mut(&mut self) -> &mut BaseFields;
    /// Preserve the typed storage representation.
    fn canonical(self) -> CanonicalRecord;
}

macro_rules! public_record {
    ($($record:ident),+ $(,)?) => {$ (
        impl PublicRecord for $record {
            fn public_base(&self) -> &BaseFields { &self.base }
            fn public_base_mut(&mut self) -> &mut BaseFields { &mut self.base }
            fn canonical(self) -> CanonicalRecord { CanonicalRecord::$record(Box::new(self)) }
        }
    )+ };
}
public_record!(
    Entity,
    Rel,
    Assertion,
    Event,
    EntityIdentityKey,
    Doc,
    IdentityCandidate,
    PredicateRegistry,
    PredicateAlias,
    WorkArtifact,
    ReprocessJob
);

/// One request's read fence, captured before preparation and authorization.
#[derive(Clone)]
pub struct PublicMutation {
    canonical: CanonicalRepository,
    client: StorageClient,
    space_id: String,
    observed_epoch: String,
    dependencies: Vec<CanonicalRecord>,
    additional_effects: Vec<CanonicalEffect>,
    #[cfg(test)]
    fault: Option<crate::canonical_commit::CanonicalFault>,
}

impl PublicMutation {
    /// Begin a public mutation before its first storage read.
    pub async fn begin(client: StorageClient, space_id: &str) -> StorageResult<Self> {
        let canonical = CanonicalRepository::new(client.clone());
        let observed_epoch = canonical.epoch(space_id).await?;
        Ok(Self {
            canonical,
            client,
            space_id: space_id.to_owned(),
            observed_epoch,
            dependencies: Vec::new(),
            additional_effects: Vec::new(),
            #[cfg(test)]
            fault: None,
        })
    }

    /// Include an additional exact record used for authorization or preparation.
    pub fn depend_on(&mut self, record: CanonicalRecord) {
        self.dependencies.push(record);
    }

    /// Inspect an assertion staged by this request without treating it as a
    /// stored dependency. Its caller must have authorized it before staging.
    #[must_use]
    pub fn staged_assertion(&self, id: &RecordId) -> Option<&Assertion> {
        self.additional_effects
            .iter()
            .find_map(|effect| match &effect.after {
                Some(CanonicalRecord::Assertion(assertion)) if &assertion.base.id == id => {
                    Some(assertion.as_ref())
                }
                _ => None,
            })
    }

    /// Require a cited assertion to describe this exact resolved relationship.
    ///
    /// Existing predicate aliases may establish equivalence; raw quote equality
    /// alone never establishes a new endpoint or predicate claim.
    ///
    /// # Errors
    /// Rejects incompatible endpoints, predicates or space identity and
    /// propagates failures reading the current predicate alias.
    pub async fn observe_relationship_assertion(
        &mut self,
        rel: &Rel,
        assertion: &Assertion,
    ) -> StorageResult<()> {
        let aliases =
            relationship_assertion_dependencies(&self.client, &self.space_id, rel, assertion)
                .await?;
        self.dependencies.extend(aliases);
        Ok(())
    }

    /// Require an attribute citation to describe its resolved subject, predicate
    /// and exact literal value, preserving the same stored alias dependencies.
    ///
    /// # Errors
    /// Rejects incompatible subject/predicate/literal claims, nonfinite numbers
    /// and failures reading the current predicate alias.
    pub async fn observe_attribute_assertion(
        &mut self,
        attribute: &Attribute,
        assertion: &Assertion,
    ) -> StorageResult<()> {
        let aliases =
            attribute_assertion_dependencies(&self.client, &self.space_id, attribute, assertion)
                .await?;
        self.dependencies.extend(aliases);
        Ok(())
    }

    /// Observe exact immutable evidence bytes and capture their live dependencies.
    ///
    /// This checks integrity only; the public caller still authorizes every row
    /// returned here before deriving or returning evidence from it.
    pub async fn observe_evidence(
        &mut self,
        span: &elephant_types::records::assertion::EvidenceSpan,
    ) -> StorageResult<(
        Doc,
        elephant_types::records::DocRevision,
        Option<elephant_types::records::DocNode>,
    )> {
        use crate::Repository;
        let revision = crate::DocRevisionRepository::new(self.client.clone())
            .get(&self.space_id, span.revision.id())
            .await?
            .ok_or_else(|| conflict("evidence revision is not live in selected space"))?;
        let doc = crate::DocRepository::new(self.client.clone())
            .get(&self.space_id, revision.doc.id())
            .await?
            .ok_or_else(|| conflict("evidence document is not live in selected space"))?;
        let quote = revision
            .text
            .get(span.start as usize..span.end as usize)
            .filter(|quote| !quote.is_empty())
            .ok_or_else(|| conflict("evidence span is not a nonempty UTF-8 range"))?;
        if !crate::read_eligibility::hash_matches(quote, &span.quote_hash) {
            return Err(conflict(
                "evidence quote hash does not match immutable revision",
            ));
        }
        let node = if let Some(id) = &span.node {
            let node = crate::DocNodeRepository::new(self.client.clone())
                .get(&self.space_id, id.id())
                .await?
                .ok_or_else(|| conflict("evidence node is not live in selected space"))?;
            if node.revision.id() != &revision.base.id
                || span.start < node.span.start
                || span.end > node.span.end
            {
                return Err(conflict("evidence span does not belong to referenced node"));
            }
            self.depend_on(CanonicalRecord::DocNode(Box::new(node.clone())));
            Some(node)
        } else {
            None
        };
        self.depend_on(CanonicalRecord::Doc(Box::new(doc.clone())));
        self.depend_on(CanonicalRecord::DocRevision(Box::new(revision.clone())));
        Ok((doc, revision, node))
    }

    /// Stage an additional prepared record in the same commit. Callers must
    /// authorize this record with the primary effect; no storage mutation occurs.
    pub fn stage_create(&mut self, record: CanonicalRecord) -> StorageResult<()> {
        if record.base().space_id != self.space_id || record.base().deleted_at.is_some() {
            return Err(conflict("invalid staged public record"));
        }
        if let Some(existing) = self
            .additional_effects
            .iter()
            .filter_map(|effect| effect.after.as_ref())
            .find(|old| old.base().id == record.base().id)
        {
            if record_value(existing)? != record_value(&record)? {
                return Err(conflict("staged create identity has conflicting payloads"));
            }
            return Ok(());
        }
        self.additional_effects.push(CanonicalEffect {
            before: None,
            after: Some(record),
        });
        Ok(())
    }

    /// Stage another authorized replacement for a multi-record public action.
    /// Its exact before-image and source support share the primary commit fence.
    pub fn stage_replace<T: PublicRecord>(
        &mut self,
        observed: &T,
        mut proposed: T,
    ) -> StorageResult<()> {
        preserve_envelope(observed.public_base(), proposed.public_base())?;
        if observed.public_base().space_id != self.space_id {
            return Err(conflict("staged replacement belongs to another space"));
        }
        if proposed.public_base().deleted_at != observed.public_base().deleted_at {
            return Err(conflict(
                "staged ordinary replacement cannot change lifecycle",
            ));
        }
        proposed.public_base_mut().created_at = observed.public_base().created_at;
        proposed
            .public_base_mut()
            .created_by
            .clone_from(&observed.public_base().created_by);
        proposed.public_base_mut().updated_at = Utc::now();
        let before = observed.clone().canonical();
        let after = proposed.canonical();
        if let Some(effect) = self.additional_effects.iter_mut().find(|effect| {
            effect
                .after
                .as_ref()
                .is_some_and(|row| row.base().id == after.base().id)
        }) {
            if effect.after.as_ref().map(record_value).transpose()? != Some(record_value(&before)?)
            {
                return Err(conflict(
                    "staged replacement does not follow previous effect",
                ));
            }
            effect.after = Some(after);
        } else {
            self.additional_effects.push(CanonicalEffect {
                before: Some(before),
                after: Some(after),
            });
        }
        Ok(())
    }

    /// Create only if this identity was absent at the transaction boundary.
    pub async fn create<T: PublicRecord>(&self, proposed: T, actor: &str) -> StorageResult<T> {
        if proposed.public_base().deleted_at.is_some() {
            return Err(conflict("new records must be live"));
        }
        self.commit(None, proposed, AuditAction::Create, actor)
            .await
    }

    /// Replace only the exact authorized row, preserving its complete envelope.
    pub async fn replace<T: PublicRecord>(
        &self,
        observed: &T,
        mut proposed: T,
        restore: bool,
        actor: &str,
    ) -> StorageResult<T> {
        preserve_envelope(observed.public_base(), proposed.public_base())?;
        if observed.public_base().deleted_at.is_some() && !restore {
            return Err(conflict("record is deleted; explicit restoration required"));
        }
        proposed.public_base_mut().created_at = observed.public_base().created_at;
        proposed
            .public_base_mut()
            .created_by
            .clone_from(&observed.public_base().created_by);
        proposed.public_base_mut().deleted_at = None;
        let action = if observed.public_base().deleted_at.is_some() {
            AuditAction::Restore
        } else {
            AuditAction::Update
        };
        self.commit(Some(observed.clone()), proposed, action, actor)
            .await
    }

    /// Soft-delete only the exact live row authorized by the caller.
    pub async fn delete<T: PublicRecord>(&self, observed: &T, actor: &str) -> StorageResult<T> {
        if observed.public_base().deleted_at.is_some() {
            return Err(conflict("record already deleted"));
        }
        let mut proposed = observed.clone();
        proposed.public_base_mut().deleted_at = Some(Utc::now());
        self.commit(Some(observed.clone()), proposed, AuditAction::Delete, actor)
            .await
    }

    /// Restore without changing classification, content or source authority.
    pub async fn restore<T: PublicRecord>(&self, observed: &T, actor: &str) -> StorageResult<T> {
        let mut proposed = observed.clone();
        proposed.public_base_mut().deleted_at = None;
        self.commit(
            Some(observed.clone()),
            proposed,
            AuditAction::Restore,
            actor,
        )
        .await
    }

    /// Commit an authorized audit reversal. An audit snapshot never authorizes
    /// replacement of a newer classification, identity or subject envelope.
    pub async fn reverse<T: PublicRecord>(
        &self,
        observed: &T,
        proposed: T,
        actor: &str,
    ) -> StorageResult<T> {
        preserve_envelope(observed.public_base(), proposed.public_base())?;
        let action = match (
            observed.public_base().deleted_at.is_some(),
            proposed.public_base().deleted_at.is_some(),
        ) {
            (false, true) => AuditAction::Delete,
            (true, false) => AuditAction::Restore,
            _ => AuditAction::Update,
        };
        self.commit(Some(observed.clone()), proposed, action, actor)
            .await
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Keep authorization-preserving pre-state, dependencies, audit and canonical effects in the same ordered mutation preparation."
    )]
    async fn commit<T: PublicRecord>(
        &self,
        observed: Option<T>,
        mut proposed: T,
        action: AuditAction,
        actor: &str,
    ) -> StorageResult<T> {
        if proposed.public_base().space_id != self.space_id
            || observed
                .as_ref()
                .is_some_and(|row| row.public_base().space_id != self.space_id)
        {
            return Err(conflict("record belongs to another space"));
        }
        let input = serde_json::json!({"space": self.space_id, "actor": actor, "action": action, "before": &observed, "proposed": semantic_record(&proposed)?, "additional_effects": self.additional_effects.iter().map(|effect| Ok(serde_json::json!({"before":&effect.before,"after":effect.after.as_ref().map(record_value).transpose()?.as_ref().map(semantic_record).transpose()?}))).collect::<StorageResult<Vec<_>>>()?});
        let fingerprint = fingerprint(&input)?;
        let commit_id = format!("public-mutation:{fingerprint}");
        // The support ledger binds observed rows, never proposed replacements.
        // A new output's source authority comes from actual lineage dependencies.
        let mut supporting = self.dependencies.clone();
        supporting.extend(observed.as_ref().map(|row| row.clone().canonical()));
        for effect in &self.additional_effects {
            supporting.extend(effect.before.iter().cloned());
        }
        let mut output_records = vec![proposed.clone().canonical()];
        output_records.extend(
            self.additional_effects
                .iter()
                .filter_map(|effect| effect.after.clone()),
        );
        for output in &output_records {
            let evidence = match output {
                CanonicalRecord::Rel(rel) => Some(&rel.evidence),
                CanonicalRecord::Attribute(attribute) => Some(&attribute.evidence),
                _ => None,
            };
            if let Some(evidence) = evidence.filter(|_| output.base().deleted_at.is_none()) {
                for evidence in evidence {
                    let assertion = if let Some(CanonicalRecord::Assertion(assertion)) =
                        output_records
                            .iter()
                            .find(|record| record.base().id == *evidence.assertion.id())
                    {
                        assertion.as_ref().clone()
                    } else {
                        use crate::Repository;
                        let assertion = crate::AssertionRepository::new(self.client.clone())
                            .get(&self.space_id, evidence.assertion.id())
                            .await?
                            .ok_or_else(|| {
                                conflict("claim assertion is not live in selected space")
                            })?;
                        supporting.push(CanonicalRecord::Assertion(Box::new(assertion.clone())));
                        assertion
                    };
                    if assertion.evidence != evidence.span {
                        return Err(conflict("claim span differs from referenced assertion"));
                    }
                    let aliases = match output {
                        CanonicalRecord::Rel(rel) => {
                            relationship_assertion_dependencies(
                                &self.client,
                                &self.space_id,
                                rel,
                                &assertion,
                            )
                            .await?
                        }
                        CanonicalRecord::Attribute(attribute) => {
                            attribute_assertion_dependencies(
                                &self.client,
                                &self.space_id,
                                attribute,
                                &assertion,
                            )
                            .await?
                        }
                        _ => Vec::new(),
                    };
                    supporting.extend(aliases);
                }
            }
            let mut lineage = output.base().provenance.sources.clone();
            match output {
                CanonicalRecord::Doc(doc) => {
                    lineage.extend(doc.source.iter().map(|source| source.id().clone()));
                }
                CanonicalRecord::Assertion(assertion) => {
                    lineage.push(assertion.evidence.revision.id().clone());
                }
                CanonicalRecord::Rel(rel) => lineage.extend(
                    rel.evidence
                        .iter()
                        .map(|evidence| evidence.span.revision.id().clone()),
                ),
                CanonicalRecord::Attribute(attribute) => lineage.extend(
                    attribute
                        .evidence
                        .iter()
                        .map(|evidence| evidence.span.revision.id().clone()),
                ),
                CanonicalRecord::Event(event) => {
                    lineage.extend(event.source_refs.iter().map(|source| source.id().clone()));
                    lineage.extend(
                        event
                            .assertion_refs
                            .iter()
                            .map(|assertion| assertion.id().clone()),
                    );
                }
                _ => {}
            }
            for id in lineage {
                if supporting.iter().any(|record| record.base().id == id) {
                    continue;
                }
                if let Some(staged) = output_records.iter().find(|record| record.base().id == id) {
                    // Its own lineage is traversed in this same loop. Staging is
                    // never treated as an observed stored row.
                    if matches!(staged, CanonicalRecord::Assertion(_)) {
                        continue;
                    }
                }
                let record = CanonicalRecord::load(&self.client, &self.space_id, &id)
                    .await?
                    .ok_or_else(|| {
                        conflict("public mutation lineage is absent in selected space")
                    })?;
                if record.base().deleted_at.is_some() {
                    return Err(conflict("public mutation lineage has been withdrawn"));
                }
                supporting.push(record);
            }
        }
        let mut dependencies = self.dependencies.clone();
        for record in &supporting {
            if !dependencies
                .iter()
                .any(|dependency| dependency.base().id == record.base().id)
            {
                dependencies.push(record.clone());
            }
        }
        let sources = self
            .canonical
            .capture_maintenance_support(&self.space_id, &supporting)
            .await?;
        if let Some(receipt) = self.canonical.receipt(&self.space_id, &commit_id).await? {
            if receipt.input_fingerprint != fingerprint {
                return Err(conflict("public mutation idempotency conflict"));
            }
            let result: T = serde_json::from_value(
                receipt
                    .replay_output
                    .ok_or_else(|| conflict("public mutation receipt has no result"))?,
            )
            .map_err(encoding_error)?;
            let current =
                CanonicalRecord::load(&self.client, &self.space_id, &result.public_base().id)
                    .await?
                    .ok_or_else(|| conflict("replayed public result no longer exists"))?;
            if record_value(&current)? != serde_json::to_value(&result).map_err(encoding_error)? {
                return Err(conflict(
                    "replayed public result changed; authorize its current version",
                ));
            }
            return Ok(result);
        }
        let now = Utc::now();
        proposed.public_base_mut().updated_at = now;
        if matches!(action, AuditAction::Delete) {
            proposed.public_base_mut().deleted_at = Some(now);
        }
        let before = observed.as_ref().map(|record| record.clone().canonical());
        let after = proposed.clone().canonical();
        let mut audit_base = proposed.public_base().clone();
        audit_base.id = stable_id(TableName::AuditLog, &commit_id, "audit");
        audit_base.created_at = now;
        audit_base.updated_at = now;
        audit_base.created_by = actor.to_owned();
        audit_base.deleted_at = None;
        let audit = AuditLog {
            base: audit_base,
            action,
            actor: actor.to_owned(),
            record: proposed.public_base().id.clone(),
            before: observed
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(encoding_error)?,
            after: Some(serde_json::to_value(&proposed).map_err(encoding_error)?),
            timestamp: now,
            reason: Some(commit_id.clone()),
        };
        let operation = match action {
            AuditAction::Create => "create",
            AuditAction::Delete => "delete",
            AuditAction::Restore => "restore",
            _ => "update",
        };
        let suffix = match action {
            AuditAction::Create => "created",
            AuditAction::Delete => "deleted",
            AuditAction::Restore => "restored",
            _ => "updated",
        };
        let event_type = format!("{}.{suffix}", proposed.public_base().id.table().as_str());
        // A multi-record correction may span policy domains. Each semantic
        // event carries only the records governed by its own joined envelope.
        let primary_change = serde_json::json!({"id": proposed.public_base().id, "table": proposed.public_base().id.table().as_str(), "op": operation});
        let mut event_groups = vec![(proposed.public_base().clone(), vec![primary_change])];
        for effect in &self.additional_effects {
            let Some(record) = &effect.after else {
                continue;
            };
            let change = serde_json::json!({"id":record.base().id,"table":record.base().id.table().as_str(),"op":if effect.before.is_some() {"update"} else {"create"}});
            if let Some((base, records)) = event_groups
                .iter_mut()
                .find(|(base, _)| base.security.policy_id == record.base().security.policy_id)
            {
                inherit_public_envelope(base, record.base())?;
                records.push(change);
            } else {
                event_groups.push((record.base().clone(), vec![change]));
            }
        }
        let mut outbox = Vec::with_capacity(event_groups.len());
        for (index, (mut base, records)) in event_groups.into_iter().enumerate() {
            let kind = if index == 0 {
                "event".to_owned()
            } else {
                format!("event:{}", base.security.policy_id)
            };
            base.id = stable_id(TableName::SystemEvent, &commit_id, &kind);
            base.created_at = now;
            base.updated_at = now;
            actor.clone_into(&mut base.created_by);
            base.deleted_at = None;
            outbox.push(SystemEvent {
                base, seq: 0, event_type: event_type.clone(),
                payload: serde_json::json!({"space_id":self.space_id,"event_type":event_type,"actor":actor,"run_id":null,"records":records}),
            });
        }
        let replay_output = Some(serde_json::to_value(&proposed).map_err(encoding_error)?);
        let mut effects = self.additional_effects.clone();
        for effect in &self.additional_effects {
            let Some(after) = &effect.after else {
                continue;
            };
            let mut base = after.base().clone();
            base.id = stable_id(
                TableName::AuditLog,
                &commit_id,
                &format!("audit:{}", after.base().id),
            );
            base.created_at = now;
            base.updated_at = now;
            actor.clone_into(&mut base.created_by);
            base.deleted_at = None;
            let auxiliary_audit = AuditLog {
                base,
                action: if effect.before.is_some() {
                    AuditAction::Update
                } else {
                    AuditAction::Create
                },
                actor: actor.to_owned(),
                record: after.base().id.clone(),
                before: effect.before.as_ref().map(record_value).transpose()?,
                after: Some(record_value(after)?),
                timestamp: now,
                reason: Some(commit_id.clone()),
            };
            effects.push(CanonicalEffect {
                before: None,
                after: Some(CanonicalRecord::AuditLog(Box::new(auxiliary_audit))),
            });
        }
        effects.push(CanonicalEffect {
            before,
            after: Some(after),
        });
        effects.push(CanonicalEffect {
            before: None,
            after: Some(CanonicalRecord::AuditLog(Box::new(audit))),
        });
        let request = CanonicalCommit {
            space_id: self.space_id.clone(),
            commit_id,
            input_fingerprint: fingerprint,
            replay_output,
            publication: None,
            maintenance: Some(CanonicalMaintenanceContext {
                origin: CanonicalMaintenanceOrigin::IndependentMutation,
                attempt: None,
                sources,
            }),
            finalization: None,
            plan: CanonicalPlan {
                extension_effects: Vec::new(),
                observed_epoch: self.observed_epoch.clone(),
                dependencies,
                effects,
                outbox,
            },
        };
        #[cfg(test)]
        if let Some(fault) = self.fault {
            self.canonical.commit_with_fault(request, fault).await?;
            return Ok(proposed);
        }
        Box::pin(self.canonical.commit(request)).await?;
        Ok(proposed)
    }
}

/// Read-only audit reversal preparation. Public handlers authorize both complete
/// envelopes before calling `commit`; the same before-image remains fenced.
pub struct PreparedAuditRevert {
    mutation: PublicMutation,
    /// Exact current row, including its current classification and tombstone.
    pub current: CanonicalRecord,
    /// Desired previous content, preserving the current authorization envelope.
    pub proposed: CanonicalRecord,
}

impl PreparedAuditRevert {
    /// Prepare retained record reversal without writing any record or outbox.
    pub async fn prepare(
        client: StorageClient,
        space: &str,
        audit: &AuditLog,
    ) -> StorageResult<Self> {
        if audit.base.space_id != space
            || matches!(
                audit.action,
                AuditAction::Promote | AuditAction::Merge | AuditAction::Purge
            )
        {
            return Err(conflict("audit action cannot be reversed"));
        }
        let mut mutation = PublicMutation::begin(client.clone(), space).await?;
        mutation.depend_on(CanonicalRecord::AuditLog(Box::new(audit.clone())));
        let current = CanonicalRecord::load(&client, space, &audit.record)
            .await?
            .ok_or_else(|| conflict("audit target no longer exists"))?;
        if matches!(&current, CanonicalRecord::Source(_))
            || matches!(&current, CanonicalRecord::Doc(doc) if doc.source.is_some())
        {
            return Err(conflict(
                "source-backed reversal requires source revision authority",
            ));
        }
        let value = match audit.action {
            AuditAction::Create => audit.after.as_ref(),
            AuditAction::Update | AuditAction::Delete | AuditAction::Restore => {
                audit.before.as_ref()
            }
            _ => None,
        }
        .ok_or_else(|| conflict("audit has no reversible snapshot"))?;
        let mut proposed = decode_snapshot(&audit.record, value.clone())?;
        preserve_envelope(current.base(), proposed.base())?;
        if let Some(after) = &audit.after {
            let after = decode_snapshot(&audit.record, after.clone())?;
            if record_value(&current)? != record_value(&after)? {
                return Err(conflict(
                    "audit target changed after the recorded operation",
                ));
            }
        } else if audit.action == AuditAction::Delete {
            // Old delete receipts omitted the after-image. Only unchanged content
            // under the current exact tombstone can qualify for restoration.
            let mut old = record_value(&proposed)?;
            let mut live = record_value(&current)?;
            for value in [&mut old, &mut live] {
                if let Some(object) = value.as_object_mut() {
                    object.remove("updated_at");
                    object.remove("deleted_at");
                }
            }
            if current.base().deleted_at.is_none() || old != live {
                return Err(conflict("legacy delete snapshot no longer matches target"));
            }
        } else {
            return Err(conflict("audit is missing the exact resulting snapshot"));
        }
        if audit.action == AuditAction::Create {
            proposed.base_mut().deleted_at = Some(Utc::now());
        }
        Ok(Self {
            mutation,
            current,
            proposed,
        })
    }

    /// Commit only after the current and proposed rows have both been authorized.
    pub async fn commit(self, actor: &str) -> StorageResult<()> {
        macro_rules! reverse { ($($variant:ident),+ $(,)?) => { match (self.current, self.proposed) {
            $((CanonicalRecord::$variant(before), CanonicalRecord::$variant(after)) => reverse_owned_record(&self.mutation, before, after, actor).await,)+
            _ => Err(conflict("unsupported audit target")),
        } }; }
        reverse!(
            Entity,
            Rel,
            Assertion,
            Event,
            EntityIdentityKey,
            IdentityCandidate,
            Doc
        )
    }
}

// Keep each typed reversal future behind the same allocation boundary. The public
// async commit retains its original owned rows, task and cancellation lifetime,
// without reserving stack space for every concrete record variant at once.
#[inline(never)]
fn reverse_owned_record<'a, T: PublicRecord + 'a>(
    mutation: &'a PublicMutation,
    before: Box<T>,
    after: Box<T>,
    actor: &'a str,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = StorageResult<()>> + Send + 'a>> {
    Box::pin(async move {
        Box::pin(mutation.reverse(before.as_ref(), *after, actor))
            .await
            .map(|_| ())
    })
}

fn decode_snapshot(id: &RecordId, mut value: serde_json::Value) -> StorageResult<CanonicalRecord> {
    // Older domain snapshots omitted id because the audit already names its target.
    // Fill only that omission; a conflicting embedded id remains invalid.
    let object = value
        .as_object_mut()
        .ok_or_else(|| conflict("invalid audit record snapshot"))?;
    let expected_id = serde_json::to_value(id).map_err(encoding_error)?;
    match object.get("id") {
        Some(existing) if existing != &expected_id => {
            return Err(conflict("audit snapshot names a different target"));
        }
        None => {
            object.insert("id".into(), expected_id);
        }
        _ => {}
    }
    // Snapshots are ordinary API serialization, never a database proxy boundary.
    macro_rules! decode { ($($variant:ident),+ $(,)?) => { match id.table() {
        $(TableName::$variant => Ok(CanonicalRecord::$variant(Box::new(serde_json::from_value(value).map_err(encoding_error)?))),)+
        _ => Err(conflict("audit record type is not reversible")),
    } }; }
    decode!(
        Entity,
        Rel,
        Assertion,
        Event,
        EntityIdentityKey,
        IdentityCandidate,
        Doc,
        Source
    )
}

fn record_value(record: &CanonicalRecord) -> StorageResult<serde_json::Value> {
    let value = serde_json::to_value(record).map_err(encoding_error)?;
    value
        .get("record")
        .cloned()
        .ok_or_else(|| conflict("canonical record has no typed payload"))
}

fn preserve_envelope(before: &BaseFields, after: &BaseFields) -> StorageResult<()> {
    if before.id != after.id
        || before.space_id != after.space_id
        || before.security != after.security
        || before.subjects != after.subjects
        || before.subjects_state != after.subjects_state
    {
        return Err(conflict(
            "ordinary mutation cannot change identity, security or subjects",
        ));
    }
    Ok(())
}

fn semantic_record<T: Serialize>(record: &T) -> StorageResult<serde_json::Value> {
    let mut value = serde_json::to_value(record).map_err(encoding_error)?;
    if let Some(object) = value.as_object_mut() {
        object.remove("updated_at");
        if object
            .get("deleted_at")
            .is_some_and(|value| !value.is_null())
        {
            object.insert("deleted_at".into(), serde_json::Value::Bool(true));
        }
    }
    Ok(value)
}

fn fingerprint(value: &serde_json::Value) -> StorageResult<String> {
    let bytes = serde_json::to_vec(value).map_err(encoding_error)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn stable_id(table: TableName, commit_id: &str, kind: &str) -> RecordId {
    let digest = Sha256::digest(format!("{commit_id}:{kind}"));
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    RecordId::new(table, uuid::Uuid::from_bytes(bytes))
}
fn conflict(message: &str) -> StorageError {
    StorageError::Conflict(message.to_owned())
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "Consumes serde errors through the existing Result::map_err callback signature."
)]
fn encoding_error(error: serde_json::Error) -> StorageError {
    StorageError::SerializationError(error.to_string())
}

#[cfg(test)]
#[path = "public_mutation_tests.rs"]
mod tests;
