//! Read-only preparation of retained structured contact intake.
//!
//! SourcePublicationRepository owns acceptance and private intent persistence.
//! This module never writes records, advances a source head, or publishes alone.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use elephant_types::records::base::{BaseFields, SubjectsState};
use elephant_types::records::entity::{EntityStatus, EntityType};
use elephant_types::records::identity::IdentityKeyStatus;
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::HasSecurity;
use elephant_types::records::{Entity, EntityIdentityKey, Rel, Source};
use elephant_types::{Link, RecordId, TableName};
use serde::{Deserialize, Serialize};
use surrealdb::sql::Value;
use uuid::Uuid;

use crate::canonical_commit::{
    CanonicalEffect, CanonicalPlan, CanonicalRecord, CanonicalRepository,
};
use crate::public_mutation::inherit_public_envelope;
use crate::{EmbeddedMutation, StorageClient, StorageError, StorageResult};

#[path = "source_contact_contribution.rs"]
mod contribution;

/// Only these graph record kinds can be derived by structured contact intake.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContactRecord {
    /// Person or organization, retaining existing independent content.
    Entity(Box<Entity>),
    /// Explicit email/profile identity association.
    EntityIdentityKey(Box<EntityIdentityKey>),
    /// The retained works_at relationship.
    Rel(Box<Rel>),
}

impl ContactRecord {
    fn base(&self) -> &BaseFields {
        match self {
            Self::Entity(row) => &row.base,
            Self::EntityIdentityKey(row) => &row.base,
            Self::Rel(row) => &row.base,
        }
    }
    pub(crate) fn canonical(&self) -> CanonicalRecord {
        match self {
            Self::Entity(row) => CanonicalRecord::Entity(row.clone()),
            Self::EntityIdentityKey(row) => CanonicalRecord::EntityIdentityKey(row.clone()),
            Self::Rel(row) => CanonicalRecord::Rel(row.clone()),
        }
    }
    fn from_canonical(row: CanonicalRecord) -> StorageResult<Self> {
        match row {
            CanonicalRecord::Entity(row) => Ok(Self::Entity(row)),
            CanonicalRecord::EntityIdentityKey(row) => Ok(Self::EntityIdentityKey(row)),
            CanonicalRecord::Rel(row) => Ok(Self::Rel(row)),
            _ => Err(invalid("unsupported contact record")),
        }
    }
    /// Existing and proposed contact records use ordinary entity/relationship scopes.
    #[must_use]
    pub const fn read_scope(&self) -> &'static str {
        match self {
            Self::Rel(_) => "read:relationship",
            _ => "read:entity",
        }
    }
    /// Write scope paired with the complete old/new envelope.
    #[must_use]
    pub const fn write_scope(&self) -> &'static str {
        match self {
            Self::Rel(_) => "write:relationship",
            _ => "write:entity",
        }
    }
}

impl HasSecurity for ContactRecord {
    fn space_id(&self) -> &str {
        &self.base().space_id
    }
    fn security(&self) -> &elephant_types::records::base::Security {
        &self.base().security
    }
    fn subjects(&self) -> Vec<Link<Entity>> {
        match self {
            Self::Entity(row) => row.subjects(),
            Self::EntityIdentityKey(row) => row.subjects(),
            Self::Rel(row) => row.subjects(),
        }
    }
    fn subjects_state(&self) -> SubjectsState {
        self.base().subjects_state
    }
}

/// A complete create or replacement; contacts never delete arbitrary records.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContactEffect {
    /// Exact originally observed row; none requires physical absence.
    pub before: Option<ContactRecord>,
    /// Complete authorized output.
    pub after: ContactRecord,
}

/// Predicate results captured before authorization and checked inside mutations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContactPredicate {
    /// All live identity records for an exact normalized key.
    LiveIdentityKey {
        /// Exact normalized email or profile selector.
        key: String,
        /// Complete matching physical identities observed during preparation.
        matched: Vec<RecordId>,
    },
    /// All live active unredirected non-inventory entities with this exact name.
    LiveCompany {
        /// Exact organization display-name selector.
        display_name: String,
        /// All live active matches, including ambiguity that admission rejects.
        matched: Vec<RecordId>,
    },
    /// All works_at relationships for the endpoints, including tombstones.
    WorksAt {
        /// Person identity at the relationship's source endpoint.
        subject: RecordId,
        /// Organization identity at the relationship's destination endpoint.
        object: RecordId,
        /// All exact endpoint/type matches, including lifecycle tombstones.
        matched: Vec<RecordId>,
    },
}

/// Private accepted attachment. Public transports never deserialize this type
/// from a request or source metadata. Construction captures actual storage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedSourceContact {
    source_id: RecordId,
    space_id: String,
    observed_epoch: String,
    source_fingerprint: String,
    digest: String,
    dependencies: Vec<ContactRecord>,
    effects: Vec<ContactEffect>,
    predicates: Vec<ContactPredicate>,
    baselines: Vec<(RecordId, Option<ContactRecord>)>,
}

#[derive(Deserialize)]
struct ContactInput {
    name: Option<String>,
    email: Option<String>,
    company: Option<String>,
    job_title: Option<String>,
    profile: Option<String>,
}

impl PreparedSourceContact {
    /// Capture the original query results and derive the retained contact plan.
    /// Returns none when the source contains no contact input.
    ///
    /// # Errors
    /// Rejects malformed input, ambiguous/revoked identity, incompatible policy,
    /// lifecycle resurrection, oversized scans, or failed storage reads.
    #[allow(
        clippy::too_many_lines,
        reason = "Contact admission captures identity, relationship, and support dependencies in their original read order before one sealed proposal; keep this protocol phase together."
    )]
    pub async fn prepare(storage: &StorageClient, source: &Source) -> StorageResult<Option<Self>> {
        let Some(input) = source.metadata.get("contact") else {
            return Ok(None);
        };
        let contact: ContactInput = serde_json::from_value(input.clone()).map_err(encoding)?;
        let mut prepared = Self {
            source_id: source.base.id.clone(),
            space_id: source.base.space_id.clone(),
            observed_epoch: CanonicalRepository::new(storage.clone())
                .epoch(&source.base.space_id)
                .await?,
            source_fingerprint: crate::canonical_commit::source_support_fingerprint(source)?,
            digest: String::new(),
            dependencies: vec![],
            effects: vec![],
            predicates: vec![],
            baselines: vec![],
        };
        // This is an original-state retirement preview, never an authority used
        // to publish the new input. Acceptance captures its own returned fence.
        let publication = crate::SourcePublicationRepository::new(storage.clone());
        let original_head = publication
            .head(&prepared.space_id, &prepared.source_id)
            .await?;
        drop(publication);
        let retirement = if let Some(head) = original_head {
            if head.lifecycle == elephant_types::publication::SourceVisibility::Active {
                let revision_id = head
                    .desired_revision
                    .ok_or_else(|| invalid("active contact source has no desired revision"))?;
                let fence = elephant_types::publication::PublicationFence {
                    source_id: head.source_id,
                    generation: head.generation,
                    revision_id,
                    operation_id: head.operation_id,
                };
                Some(
                    Box::pin(
                        CanonicalRepository::new(storage.clone())
                            .prepare_source_replacement(&prepared.space_id, &fence),
                    )
                    .await?,
                )
            } else {
                None
            }
        } else {
            None
        };
        let keys = contact_identity_keys(&contact);
        let key_rows = if keys.is_empty() {
            vec![]
        } else {
            scan(storage, &prepared.space_id, TableName::EntityIdentityKey).await?
        };
        let mut matches = Vec::new();
        for key in &keys {
            let selected: Vec<_> = key_rows.iter().filter(|row| matches!(row, ContactRecord::EntityIdentityKey(row) if row.base.deleted_at.is_none() && row.key == *key)).cloned().collect();
            prepared.predicates.push(ContactPredicate::LiveIdentityKey {
                key: key.clone(),
                matched: ids(&selected),
            });
            for row in selected {
                prepared.depend(&row)?;
                let ContactRecord::EntityIdentityKey(row) = row else {
                    return Err(invalid("contact key type"));
                };
                if row.status != IdentityKeyStatus::Active {
                    return Err(conflict("contact identity key is not active"));
                }
                matches.push(*row);
            }
        }
        let identities: BTreeSet<_> = matches.iter().map(|row| row.entity.id().clone()).collect();
        if identities.len() > 1 {
            return Err(conflict(
                "contact keys name different identities; explicit merge required",
            ));
        }
        let person_id = identities
            .into_iter()
            .next()
            .unwrap_or_else(|| stable_id(TableName::Entity, source, &["person"]));
        let old_person = load(storage, &prepared.space_id, &person_id).await?;
        if old_person.is_none() && !matches.is_empty() {
            return Err(conflict("contact identity key has no live target"));
        }
        if let Some(ContactRecord::Entity(row)) = &old_person {
            active_entity(row)?;
        }
        let person_baseline =
            prepared.capture_baseline(retirement.as_ref(), &person_id, old_person.as_ref())?;
        let mut person = if let Some(ContactRecord::Entity(row)) = &person_baseline {
            active_entity(row)?;
            (**row).clone()
        } else if person_baseline.is_some() {
            return Err(invalid("contact person identity type"));
        } else {
            Entity::new(
                derived_base(source, person_id.clone()),
                EntityType::new("person"),
                contact
                    .name
                    .clone()
                    .or_else(|| contact.email.clone())
                    .unwrap_or_else(|| "Unknown".into()),
            )
        };
        inherit_source(&mut person.base, source)?;
        for row in &matches {
            inherit_public_envelope(&mut person.base, &row.base)?;
        }
        for key in &keys {
            if !person.identity_keys.contains(key) {
                person.identity_keys.push(key.clone());
            }
        }
        if let Some(name) = contact.name {
            if person.display_name != name && !person.aliases.contains(&name) {
                person.aliases.push(name);
            }
        }
        if person_baseline.is_none() {
            person.metadata = serde_json::json!({"source_type":"contact_import"});
        }
        prepared.effect(old_person, ContactRecord::Entity(Box::new(person.clone())))?;
        for key in keys {
            let existing: Vec<_> = matches
                .iter()
                .filter(|row| row.key == key)
                .cloned()
                .collect();
            if existing.is_empty() {
                let id = identity_key_id(&prepared.space_id, &key)?;
                if load(storage, &prepared.space_id, &id).await?.is_some() {
                    return Err(conflict(
                        "contact key identity already exists; no implicit revival",
                    ));
                }
                prepared.capture_baseline(retirement.as_ref(), &id, None)?;
                let mut base = person.base.clone();
                base.id = id;
                prepared.effect(
                    None,
                    ContactRecord::EntityIdentityKey(Box::new(EntityIdentityKey {
                        base,
                        key,
                        entity: Link::new(person_id.clone()),
                        confidence: 1.0,
                        first_seen: Some(source.base.created_at),
                        last_seen: Some(source.base.updated_at),
                        evidence: vec![Link::new(source.base.id.clone())],
                        status: IdentityKeyStatus::Active,
                    })),
                )?;
            } else {
                for before in existing {
                    let baseline = prepared.capture_baseline(
                        retirement.as_ref(),
                        &before.base.id,
                        Some(&ContactRecord::EntityIdentityKey(Box::new(before.clone()))),
                    )?;
                    let mut after = match baseline {
                        Some(ContactRecord::EntityIdentityKey(row)) => *row,
                        None => {
                            let mut base = person.base.clone();
                            base.id = before.base.id.clone();
                            EntityIdentityKey {
                                base,
                                key: before.key.clone(),
                                entity: Link::new(person_id.clone()),
                                confidence: 1.0,
                                first_seen: Some(source.base.created_at),
                                last_seen: Some(source.base.updated_at),
                                evidence: vec![],
                                status: IdentityKeyStatus::Active,
                            }
                        }
                        _ => return Err(invalid("contact key retirement type")),
                    };
                    if after.entity.id() != &person_id {
                        return Err(conflict("contact identity changes under source retirement; explicit identity resolution required"));
                    }
                    inherit_public_envelope(&mut after.base, &person.base)?;
                    inherit_source(&mut after.base, source)?;
                    if !after
                        .evidence
                        .iter()
                        .any(|reference| reference.id() == &source.base.id)
                    {
                        after.evidence.push(Link::new(source.base.id.clone()));
                    }
                    prepared.effect(
                        Some(ContactRecord::EntityIdentityKey(Box::new(before))),
                        ContactRecord::EntityIdentityKey(Box::new(after)),
                    )?;
                }
            }
        }
        if let (Some(company), Some(_job_title)) = (contact.company, contact.job_title) {
            let selected: Vec<_> = scan(storage, &prepared.space_id, TableName::Entity)
                .await?
                .into_iter()
                .filter(|row| company_matches(row, &company))
                .collect();
            prepared.predicates.push(ContactPredicate::LiveCompany {
                display_name: company.clone(),
                matched: ids(&selected),
            });
            if selected.len() > 1 {
                return Err(conflict(
                    "contact company name is ambiguous; explicit identity required",
                ));
            }
            let old_company = if let Some(row) = selected.into_iter().next() {
                Some(row)
            } else {
                load(
                    storage,
                    &prepared.space_id,
                    &stable_id(TableName::Entity, source, &["company", &company]),
                )
                .await?
            };
            if let Some(ContactRecord::Entity(row)) = &old_company {
                active_entity(row)?;
            }
            let company_id = old_company.as_ref().map_or_else(
                || stable_id(TableName::Entity, source, &["company", &company]),
                |row| row.base().id.clone(),
            );
            let company_baseline = prepared.capture_baseline(
                retirement.as_ref(),
                &company_id,
                old_company.as_ref(),
            )?;
            let company_current = prepared
                .effects
                .iter()
                .find(|effect| effect.after.base().id == company_id)
                .map(|effect| effect.after.clone())
                .or(company_baseline);
            let mut company_entity = if let Some(ContactRecord::Entity(row)) = &company_current {
                active_entity(row)?;
                (**row).clone()
            } else if company_current.is_some() {
                return Err(invalid("contact company identity type"));
            } else {
                Entity::new(
                    derived_base(source, company_id.clone()),
                    EntityType::new("org"),
                    company.clone(),
                )
            };
            if company_entity.display_name != company && !company_entity.aliases.contains(&company)
            {
                company_entity.aliases.push(company);
            }
            inherit_source(&mut company_entity.base, source)?;
            prepared.effect(
                old_company,
                ContactRecord::Entity(Box::new(company_entity.clone())),
            )?;
            let selected: Vec<_> = scan(storage, &prepared.space_id, TableName::Rel)
                .await?
                .into_iter()
                .filter(|row| relation_matches(row, &person_id, &company_entity.base.id))
                .collect();
            prepared.predicates.push(ContactPredicate::WorksAt {
                subject: person_id.clone(),
                object: company_entity.base.id.clone(),
                matched: ids(&selected),
            });
            if selected.len() > 1 {
                return Err(conflict("contact relationship is ambiguous"));
            }
            let old = selected.into_iter().next();
            if let Some(row) = &old {
                prepared.depend(row)?;
            }
            // Retain tombstones. Contact intake never restores a deleted edge.
            if old
                .as_ref()
                .is_none_or(|row| row.base().deleted_at.is_none())
            {
                let relation_id = old.as_ref().map_or_else(
                    || {
                        stable_id(
                            TableName::Rel,
                            source,
                            &[
                                "works_at",
                                &person_id.to_string(),
                                &company_entity.base.id.to_string(),
                            ],
                        )
                    },
                    |row| row.base().id.clone(),
                );
                let baseline =
                    prepared.capture_baseline(retirement.as_ref(), &relation_id, old.as_ref())?;
                let mut relation = if let Some(ContactRecord::Rel(row)) = &baseline {
                    if row.subject.id() != &person_id || row.object.id() != &company_entity.base.id
                    {
                        return Err(conflict(
                            "contact relationship endpoints change under source retirement",
                        ));
                    }
                    (**row).clone()
                } else {
                    Rel {
                        base: derived_base(source, relation_id),
                        subject: Link::new(person_id),
                        object: Link::new(company_entity.base.id.clone()),
                        rel_type: "works_at".into(),
                        status: ClaimStatus::Approved,
                        confidence: 0.95,
                        validity_window: None,
                        evidence: vec![],
                        temporal_audit: None,
                    }
                };
                inherit_public_envelope(&mut relation.base, &person.base)?;
                inherit_public_envelope(&mut relation.base, &company_entity.base)?;
                inherit_source(&mut relation.base, source)?;
                prepared.effect(old, ContactRecord::Rel(Box::new(relation)))?;
            }
        }
        prepared.digest = prepared.compute_digest()?;
        prepared.validate_source(source)?;
        Ok(Some(prepared))
    }

    /// Source incarnation the private attachment belongs to.
    #[must_use]
    pub(crate) const fn source_id(&self) -> &RecordId {
        &self.source_id
    }
    /// Selected owning space.
    #[must_use]
    pub(crate) fn space_id(&self) -> &str {
        &self.space_id
    }
    /// Epoch captured before the first contact selector read.
    #[must_use]
    pub(crate) fn observed_epoch(&self) -> &str {
        &self.observed_epoch
    }
    /// Complete private plan integrity digest, never raw payload.
    #[must_use]
    pub(crate) fn digest(&self) -> &str {
        &self.digest
    }
    /// Full old/new envelopes the transport must authorize before acceptance.
    #[must_use]
    pub fn authorizations(&self) -> Vec<&ContactRecord> {
        self.dependencies
            .iter()
            .chain(
                self.effects
                    .iter()
                    .flat_map(|effect| effect.before.iter().chain(std::iter::once(&effect.after))),
            )
            .collect()
    }

    fn compute_digest(&self) -> StorageResult<String> {
        let mut value = serde_json::to_value((
            &self.source_id,
            &self.space_id,
            &self.observed_epoch,
            &self.source_fingerprint,
            &self.dependencies,
            &self.effects,
            &self.predicates,
            &self.baselines,
        ))
        .map_err(encoding)?;
        value.sort_all_objects();
        let bytes = serde_json::to_vec(&value).map_err(encoding)?;
        Ok(Uuid::new_v5(&Uuid::NAMESPACE_OID, &bytes).to_string())
    }
    fn depend(&mut self, row: &ContactRecord) -> StorageResult<()> {
        if let Some(old) = self
            .dependencies
            .iter()
            .find(|old| old.base().id == row.base().id)
        {
            if !same(old, row)? {
                return Err(conflict("contact input changed during preparation"));
            }
        } else {
            self.dependencies.push(row.clone());
        }
        Ok(())
    }
    fn capture_baseline(
        &mut self,
        retirement: Option<&CanonicalPlan>,
        id: &RecordId,
        original: Option<&ContactRecord>,
    ) -> StorageResult<Option<ContactRecord>> {
        if let Some((_, baseline)) = self.baselines.iter().find(|(known, _)| known == id) {
            return Ok(baseline
                .clone()
                .filter(|row| row.base().deleted_at.is_none()));
        }
        let baseline = if let Some(effect) = retirement.and_then(|plan| {
            plan.effects.iter().find(|effect| {
                effect
                    .after
                    .as_ref()
                    .or(effect.before.as_ref())
                    .is_some_and(|row| &row.base().id == id)
            })
        }) {
            let observed = effect
                .before
                .clone()
                .map(ContactRecord::from_canonical)
                .transpose()?;
            if !same_optional(observed.as_ref(), original)? {
                return Err(conflict(
                    "contact retirement preview changed its original snapshot",
                ));
            }
            effect
                .after
                .clone()
                .map(ContactRecord::from_canonical)
                .transpose()?
        } else {
            original.cloned()
        };
        self.baselines.push((id.clone(), baseline.clone()));
        Ok(baseline.filter(|row| row.base().deleted_at.is_none()))
    }

    fn effect(&mut self, before: Option<ContactRecord>, after: ContactRecord) -> StorageResult<()> {
        if let Some(row) = &before {
            self.depend(row)?;
        }
        // An identical final value still needs this generation's support after
        // retirement removes the previous contribution. Operation replay is
        // handled at acceptance, never by skipping an individual graph effect.
        if let Some(existing) = self
            .effects
            .iter_mut()
            .find(|effect| effect.after.base().id == after.base().id)
        {
            if !same_optional(existing.before.as_ref(), before.as_ref())? {
                return Err(conflict("nonsequential contact effect"));
            }
            existing.after = after;
        } else {
            self.effects.push(ContactEffect { before, after });
        }
        Ok(())
    }

    pub(crate) fn validate_seal(
        &self,
        space: &str,
        source_id: &RecordId,
        digest: &str,
    ) -> StorageResult<()> {
        if self.space_id != space
            || &self.source_id != source_id
            || self.source_id.table() != TableName::Source
            || self.space_id.trim().is_empty()
            || self.digest != digest
            || self.digest != self.compute_digest()?
        {
            return Err(conflict("contact seal identity or digest changed"));
        }
        let mut seen = BTreeSet::new();
        for row in self.dependencies.iter().chain(
            self.effects
                .iter()
                .flat_map(|effect| effect.before.iter().chain(std::iter::once(&effect.after))),
        ) {
            let table = match row {
                ContactRecord::Entity(_) => TableName::Entity,
                ContactRecord::EntityIdentityKey(_) => TableName::EntityIdentityKey,
                ContactRecord::Rel(_) => TableName::Rel,
            };
            if row.base().space_id != self.space_id || row.base().id.table() != table {
                return Err(invalid("contact seal contains an invalid record identity"));
            }
        }
        for effect in &self.effects {
            let id = &effect.after.base().id;
            if !seen.insert(id.clone())
                || effect.after.base().deleted_at.is_some()
                || !effect
                    .after
                    .base()
                    .provenance
                    .sources
                    .contains(&self.source_id)
                || effect
                    .before
                    .as_ref()
                    .is_some_and(|row| &row.base().id != id || row.base().deleted_at.is_some())
                || self
                    .baselines
                    .iter()
                    .filter(|(known, _)| known == id)
                    .count()
                    != 1
            {
                return Err(invalid(
                    "contact seal contains invalid effects or baselines",
                ));
            }
        }
        for predicate in &self.predicates {
            for id in predicate.matched() {
                if !self
                    .dependencies
                    .iter()
                    .any(|row| row.base().id == id && predicate.matches(row))
                {
                    return Err(invalid("contact predicate lacks its original row"));
                }
            }
        }
        Ok(())
    }

    pub(crate) fn validate_source(&self, source: &Source) -> StorageResult<()> {
        self.validate_seal(&source.base.space_id, &source.base.id, &self.digest)?;
        if self.source_fingerprint != crate::canonical_commit::source_support_fingerprint(source)? {
            return Err(conflict(
                "contact attachment does not match accepted source",
            ));
        }
        for effect in &self.effects {
            let after = effect.after.base();
            if after.space_id != self.space_id
                || after.deleted_at.is_some()
                || !after.provenance.sources.contains(&self.source_id)
                || effect.before.as_ref().is_some_and(|row| {
                    row.base().id != after.id || row.base().space_id != self.space_id
                })
            {
                return Err(invalid("invalid contact effect identity or ownership"));
            }
            let mut required = after.clone();
            inherit_public_envelope(&mut required, &source.base)?;
            if required.security != after.security
                || required.subjects != after.subjects
                || required.subjects_state != after.subjects_state
            {
                return Err(invalid("contact effect weakened source authorization"));
            }
        }
        Ok(())
    }

    /// A promotion overlay may enrich a contact, but may not discard or replace
    /// the accepted contribution, its original read set, or its semantic event.
    pub(crate) fn validate_plan(&self, plan: &CanonicalPlan) -> StorageResult<()> {
        self.validate_seal(&self.space_id, &self.source_id, &self.digest)?;
        for row in &self.dependencies {
            let matching: Vec<_> = plan
                .dependencies
                .iter()
                .filter(|record| record.base().id == row.base().id)
                .collect();
            if matching.is_empty() {
                return Err(conflict("publication omitted a contact dependency"));
            }
            let expected = serde_json::to_value(row.canonical()).map_err(encoding)?;
            for actual in matching {
                if serde_json::to_value(actual).map_err(encoding)? != expected {
                    return Err(conflict("publication changed a contact dependency"));
                }
            }
        }
        for effect in &self.effects {
            let id = &effect.after.base().id;
            let matching: Vec<_> = plan
                .effects
                .iter()
                .filter(|effect| {
                    effect
                        .after
                        .as_ref()
                        .or(effect.before.as_ref())
                        .is_some_and(|row| &row.base().id == id)
                })
                .collect();
            if matching.len() != 1 {
                return Err(conflict(
                    "publication omitted or duplicated a contact effect",
                ));
            }
            let actual = matching[0];
            let before = actual
                .before
                .clone()
                .map(ContactRecord::from_canonical)
                .transpose()?;
            let Some(after) = actual
                .after
                .clone()
                .map(ContactRecord::from_canonical)
                .transpose()?
            else {
                return Err(conflict("publication deleted an accepted contact effect"));
            };
            if !same_optional(before.as_ref(), effect.before.as_ref())?
                || !retains_contact(&effect.after, &after)?
            {
                return Err(conflict(
                    "publication replaced an accepted contact contribution",
                ));
            }
            let expected_event = self.event(effect);
            let events: Vec<_> = plan
                .outbox
                .iter()
                .filter(|event| event.base.id == expected_event.base.id)
                .collect();
            if events.len() != 1
                || serde_json::to_value(events[0]).map_err(encoding)?
                    != serde_json::to_value(expected_event).map_err(encoding)?
            {
                return Err(conflict("publication omitted or changed a contact event"));
            }
        }
        Ok(())
    }

    /// Bounds background preflight to the original authorized source envelope.
    ///
    /// This checks every old/new/dependency row; it does not grant contact
    /// capability or replace the caller's original publication/source CAS.
    /// Callers separately preserve the authorized source's own restrictions.
    ///
    /// # Errors
    /// Rejects another source/space, an invalid seal, or any contact dependency
    /// whose policy, classification, handling, or subjects exceed that envelope.
    pub fn validate_service_authority(
        &self,
        authorized: &elephant_types::publication::SourceAuthorizationSnapshot,
    ) -> StorageResult<()> {
        self.validate_seal(&self.space_id, &self.source_id, &self.digest)?;
        let envelope = &authorized.envelope;
        if &envelope.source_id != self.source_id() || envelope.space_id != self.space_id {
            return Err(conflict(
                "contact service authority belongs to another source",
            ));
        }
        for record in self.authorizations() {
            let mut allowed = record.base().clone();
            allowed.space_id.clone_from(&envelope.space_id);
            allowed.security.clone_from(&envelope.security);
            allowed.subjects.clone_from(&envelope.subjects);
            allowed.subjects_state = envelope.subjects_state;
            // Conjunction adds only missing restrictions and preserves the
            // original list order; set ordering grants no additional permission.
            let mut joined = allowed.clone();
            inherit_public_envelope(&mut joined, record.base())?;
            if joined.security != allowed.security
                || joined.subjects != allowed.subjects
                || joined.subjects_state != allowed.subjects_state
            {
                return Err(conflict(
                    "contact preflight exceeds original service authority",
                ));
            }
        }
        Ok(())
    }

    /// Certifies only this sealed contact contribution against freshly prepared
    /// retirement.
    ///
    /// The actual first-before remains the canonical CAS input;
    /// the independent retirement baseline is never substituted for that guard.
    pub(crate) fn certifies_effect(
        &self,
        actual: &CanonicalEffect,
        retirement: Option<&CanonicalEffect>,
    ) -> StorageResult<bool> {
        self.validate_seal(&self.space_id, &self.source_id, &self.digest)?;
        let Some(id) = actual
            .after
            .as_ref()
            .or(actual.before.as_ref())
            .map(|row| &row.base().id)
        else {
            return Ok(false);
        };
        let Some(sealed) = self
            .effects
            .iter()
            .find(|effect| &effect.after.base().id == id)
        else {
            return Ok(false);
        };
        let before = actual
            .before
            .clone()
            .map(ContactRecord::from_canonical)
            .transpose()?;
        let after = actual
            .after
            .clone()
            .map(ContactRecord::from_canonical)
            .transpose()?;
        if !same_optional(before.as_ref(), sealed.before.as_ref())?
            || !after
                .as_ref()
                .map(|after| retains_contact(&sealed.after, after))
                .transpose()?
                .unwrap_or(false)
        {
            return Err(conflict(
                "contact effect no longer matches its accepted contribution",
            ));
        }
        let baseline = self
            .baselines
            .iter()
            .find(|(known, _)| known == id)
            .ok_or_else(|| invalid("contact effect has no captured retirement baseline"))?;
        if let Some(retirement) = retirement {
            let before = retirement
                .before
                .clone()
                .map(ContactRecord::from_canonical)
                .transpose()?;
            let after = retirement
                .after
                .clone()
                .map(ContactRecord::from_canonical)
                .transpose()?;
            if !same_optional(before.as_ref(), sealed.before.as_ref())?
                || !same_retirement_baseline(after.as_ref(), baseline.1.as_ref())?
            {
                return Err(conflict(
                    "fresh contact retirement changed its captured baseline",
                ));
            }
        } else if !same_retirement_baseline(baseline.1.as_ref(), sealed.before.as_ref())? {
            return Err(conflict(
                "fresh contact retirement omitted a required effect",
            ));
        }
        Ok(true)
    }

    /// Builds the contact-only inverse for A's existing source decision receipt.
    ///
    /// A replacement's old published contribution is the CAS before, never the
    /// inverse baseline: erasing r2 must not resurrect r1. A freshly certified
    /// source-exclusive retirement tombstone has no surviving independent row.
    pub(crate) fn decision_effects(
        &self,
        plan: &CanonicalPlan,
    ) -> StorageResult<Vec<CanonicalEffect>> {
        self.validate_plan(plan)?;
        self.effects
            .iter()
            .map(|sealed| {
                let id = &sealed.after.base().id;
                let baseline = self
                    .baselines
                    .iter()
                    .find(|(known, _)| known == id)
                    .ok_or_else(|| invalid("contact effect has no captured retirement baseline"))?;
                let actual = plan
                    .effects
                    .iter()
                    .find(|effect| {
                        effect
                            .after
                            .as_ref()
                            .is_some_and(|row| &row.base().id == id)
                    })
                    .ok_or_else(|| conflict("publication omitted a contact effect"))?;
                Ok(CanonicalEffect {
                    before: baseline
                        .1
                        .as_ref()
                        .filter(|row| row.base().deleted_at.is_none())
                        .map(ContactRecord::canonical),
                    after: actual.after.clone(),
                })
            })
            .collect()
    }

    /// Exact contact event identities, excluding unrelated promotion events.
    pub(crate) fn decision_outbox_ids(&self) -> Vec<RecordId> {
        self.effects
            .iter()
            .map(|effect| self.event(effect).base.id)
            .collect()
    }

    /// Attests the structured relationship fact, not a fabricated passage.
    ///
    /// A additionally checks the current published fence and support/decision
    /// fingerprint before exposing this proof to maintenance. Status/confidence
    /// may have changed through that separate canonical lifecycle authority.
    pub(crate) fn verifies_rel(&self, current: &Rel) -> StorageResult<bool> {
        self.validate_seal(&self.space_id, &self.source_id, &self.digest)?;
        let Some(original) = self.effects.iter().find_map(|effect| match &effect.after {
            ContactRecord::Rel(row) if row.base.id == current.base.id => Some(row),
            _ => None,
        }) else {
            return Ok(false);
        };
        Ok(current.base.deleted_at.is_none()
            && current.confidence.is_finite()
            && (0.0..=1.0).contains(&current.confidence)
            && original.subject == current.subject
            && original.object == current.object
            && original.rel_type == current.rel_type
            && original.validity_window == current.validity_window
            && base_extends(&original.base, &current.base)?
            && original
                .evidence
                .iter()
                .all(|row| current.evidence.contains(row)))
    }

    fn event(&self, effect: &ContactEffect) -> elephant_types::records::SystemEvent {
        let id = &effect.after.base().id;
        let mut base = effect.after.base().clone();
        base.id = RecordId::new(
            TableName::SystemEvent,
            Uuid::new_v5(
                &Uuid::NAMESPACE_URL,
                format!("contact:{}:{}", self.digest, id).as_bytes(),
            ),
        );
        base.created_by = "contact_ingest".into();
        let operation = if effect.before.is_some() {
            "update"
        } else {
            "create"
        };
        elephant_types::records::SystemEvent {
            base,
            seq: 0,
            event_type: "contact.ingested".into(),
            payload: serde_json::json!({"space_id":self.space_id,"event_type":"contact.ingested","actor":"contact_ingest","source_id":self.source_id,
                "records":[{"id":id,"table":id.table().as_str(),"op":operation}]}),
        }
    }

    #[allow(
        clippy::needless_pass_by_ref_mut,
        reason = "This guard is called within the exclusive mutation protocol; preserve its existing mutable transaction-borrow interface and lifetime through validation."
    )]
    pub(crate) fn check_tx(&self, tx: &mut EmbeddedMutation<'_>) -> StorageResult<()> {
        for (expected, before) in self.dependencies.iter().map(|row| (row, Some(row))).chain(
            self.effects
                .iter()
                .map(|effect| (&effect.after, effect.before.as_ref())),
        ) {
            let current = CanonicalRecord::load_tx(tx, &self.space_id, &expected.base().id)?
                .map(ContactRecord::from_canonical)
                .transpose()?;
            match (before, current.as_ref()) {
                (None, None) => {}
                (Some(old), Some(current)) if same(old, current)? => {}
                _ => return Err(conflict("accepted contact record changed")),
            }
        }
        for predicate in &self.predicates {
            let rows: Vec<ContactRecord> = match predicate {
                ContactPredicate::LiveIdentityKey { .. } => tx
                    .scan::<EntityIdentityKey>()?
                    .into_iter()
                    .map(|row| ContactRecord::EntityIdentityKey(Box::new(row)))
                    .collect(),
                ContactPredicate::LiveCompany { .. } => tx
                    .scan::<Entity>()?
                    .into_iter()
                    .map(|row| ContactRecord::Entity(Box::new(row)))
                    .collect(),
                ContactPredicate::WorksAt { .. } => tx
                    .scan::<Rel>()?
                    .into_iter()
                    .map(|row| ContactRecord::Rel(Box::new(row)))
                    .collect(),
            };
            let actual: Vec<_> = rows
                .into_iter()
                .filter(|row: &ContactRecord| {
                    row.base().space_id == self.space_id && predicate.matches(row)
                })
                .collect();
            if ids(&actual) != predicate.matched() {
                return Err(conflict("accepted contact predicate changed"));
            }
        }
        Ok(())
    }

    pub(crate) fn append_guard_sql(
        &self,
        sql: &mut String,
        bindings: &mut BTreeMap<String, Value>,
    ) -> StorageResult<()> {
        bindings.insert("space".into(), Value::from(self.space_id.clone()));
        for (index, (row, before)) in self
            .dependencies
            .iter()
            .map(|row| (row, Some(row)))
            .chain(
                self.effects
                    .iter()
                    .map(|effect| (&effect.after, effect.before.as_ref())),
            )
            .enumerate()
        {
            let before = before.map(ContactRecord::canonical);
            crate::canonical_commit::append_row_guard(
                sql,
                bindings,
                &format!("contact_row_{index}"),
                &row.canonical(),
                before.as_ref(),
            )?;
        }
        for (index, predicate) in self.predicates.iter().enumerate() {
            let prefix = format!("contact_predicate_{index}");
            let query = match predicate {
                ContactPredicate::LiveIdentityKey { key, .. } => {
                    bindings.insert(format!("{prefix}_key"), Value::from(key.clone()));
                    format!("SELECT VALUE id FROM entity_identity_key WHERE space_id = $space AND key = ${prefix}_key AND deleted_at IS NONE")
                }
                ContactPredicate::LiveCompany { display_name, .. } => {
                    bindings.insert(format!("{prefix}_name"), Value::from(display_name.clone()));
                    if crate::entity_repository::is_source_inventory_label(display_name) {
                        "SELECT VALUE id FROM entity WHERE false".into()
                    } else {
                        format!("SELECT VALUE id FROM entity WHERE space_id = $space AND display_name = ${prefix}_name AND deleted_at IS NONE AND status = 'active' AND redirect_to IS NONE")
                    }
                }
                ContactPredicate::WorksAt {
                    subject, object, ..
                } => {
                    bindings.insert(
                        format!("{prefix}_subject"),
                        crate::surreal::thing_from_record_id(subject).into(),
                    );
                    bindings.insert(
                        format!("{prefix}_object"),
                        crate::surreal::thing_from_record_id(object).into(),
                    );
                    format!("SELECT VALUE id FROM rel WHERE space_id = $space AND `in` = ${prefix}_subject AND `out` = ${prefix}_object AND rel_type = 'works_at'")
                }
            };
            bindings.insert(
                format!("{prefix}_expected"),
                Value::Array(
                    predicate
                        .matched()
                        .iter()
                        .map(|id| Value::Thing(crate::surreal::thing_from_record_id(id)))
                        .collect::<Vec<_>>()
                        .into(),
                ),
            );
            let _ = writeln!(sql,"LET ${prefix}_actual = ({query});\nIF array::sort(${prefix}_actual) != array::sort(${prefix}_expected) {{ THROW 'publication:accepted_contact_predicate_changed'; }};");
        }
        Ok(())
    }

    pub(crate) fn append_plan(&self, plan: &mut CanonicalPlan) -> StorageResult<()> {
        for row in &self.dependencies {
            let record = row.canonical();
            if let Some(existing) = plan
                .dependencies
                .iter()
                .find(|existing| existing.base().id == record.base().id)
            {
                if serde_json::to_value(existing).map_err(encoding)?
                    != serde_json::to_value(&record).map_err(encoding)?
                {
                    return Err(conflict("contact dependency changed in publication plan"));
                }
            } else {
                plan.dependencies.push(record);
            }
        }
        for effect in &self.effects {
            let id = &effect.after.base().id;
            let prefix = plan.effects.iter().find(|prior| {
                prior
                    .after
                    .as_ref()
                    .or(prior.before.as_ref())
                    .is_some_and(|row| &row.base().id == id)
            });
            let baseline = self
                .baselines
                .iter()
                .find(|(known, _)| known == id)
                .ok_or_else(|| invalid("contact effect has no captured retirement baseline"))?;
            let before = if let Some(prefix) = prefix {
                let original = prefix
                    .before
                    .clone()
                    .map(ContactRecord::from_canonical)
                    .transpose()?;
                if !same_optional(original.as_ref(), effect.before.as_ref())? {
                    return Err(conflict(
                        "contact publication prefix has different original input",
                    ));
                }
                let current = prefix
                    .after
                    .clone()
                    .map(ContactRecord::from_canonical)
                    .transpose()?;
                if !same_retirement_baseline(baseline.1.as_ref(), current.as_ref())? {
                    return Err(conflict("contact retirement baseline changed"));
                }
                prefix.after.clone()
            } else {
                if !same_retirement_baseline(baseline.1.as_ref(), effect.before.as_ref())? {
                    return Err(conflict("contact retirement prefix is missing"));
                }
                effect.before.as_ref().map(ContactRecord::canonical)
            };
            plan.push(before, Some(effect.after.canonical()))?;
            plan.outbox.push(self.event(effect));
        }
        Ok(())
    }
}

impl ContactPredicate {
    fn matched(&self) -> Vec<RecordId> {
        let values = match self {
            Self::LiveIdentityKey { matched, .. }
            | Self::LiveCompany { matched, .. }
            | Self::WorksAt { matched, .. } => matched,
        };
        let mut ids = values.clone();
        ids.sort();
        ids.dedup();
        ids
    }
    fn matches(&self, row: &ContactRecord) -> bool {
        match self {
            Self::LiveIdentityKey { key, .. } => {
                matches!(row, ContactRecord::EntityIdentityKey(row) if row.base.deleted_at.is_none() && row.key == *key)
            }
            Self::LiveCompany { display_name, .. } => company_matches(row, display_name),
            Self::WorksAt {
                subject, object, ..
            } => relation_matches(row, subject, object),
        }
    }
}
fn company_matches(row: &ContactRecord, name: &str) -> bool {
    matches!(row, ContactRecord::Entity(row) if row.base.deleted_at.is_none() && row.status == EntityStatus::Active && row.redirect_to.is_none() && row.display_name == name && !crate::entity_repository::is_source_inventory_entity(row))
}
fn relation_matches(row: &ContactRecord, subject: &RecordId, object: &RecordId) -> bool {
    matches!(row, ContactRecord::Rel(row) if row.subject.id() == subject && row.object.id() == object && row.rel_type == "works_at")
}
fn ids(rows: &[ContactRecord]) -> Vec<RecordId> {
    let mut result: Vec<_> = rows.iter().map(|row| row.base().id.clone()).collect();
    result.sort();
    result.dedup();
    result
}
async fn scan(
    storage: &StorageClient,
    space: &str,
    table: TableName,
) -> StorageResult<Vec<ContactRecord>> {
    crate::entity_merge_repository::merge_scan(storage, space, table)
        .await?
        .into_iter()
        .map(ContactRecord::from_canonical)
        .collect()
}
async fn load(
    storage: &StorageClient,
    space: &str,
    id: &RecordId,
) -> StorageResult<Option<ContactRecord>> {
    CanonicalRecord::load(storage, space, id)
        .await?
        .map(ContactRecord::from_canonical)
        .transpose()
}
fn active_entity(entity: &Entity) -> StorageResult<()> {
    if entity.base.deleted_at.is_some()
        || entity.status != EntityStatus::Active
        || entity.redirect_to.is_some()
    {
        return Err(conflict(
            "contact identity is not active; explicit lifecycle or merge required",
        ));
    }
    Ok(())
}
fn stable_id(table: TableName, source: &Source, parts: &[&str]) -> RecordId {
    let mut key = format!("source-contact:{}:{}", source.base.space_id, source.base.id);
    for part in parts {
        let _ = write!(key, ":{}:{part}", part.len());
    }
    RecordId::new(table, Uuid::new_v5(&Uuid::NAMESPACE_URL, key.as_bytes()))
}
fn identity_key_id(space: &str, key: &str) -> StorageResult<RecordId> {
    let bytes = serde_json::to_vec(&(space, key)).map_err(encoding)?;
    Ok(RecordId::new(
        TableName::EntityIdentityKey,
        Uuid::new_v5(&Uuid::NAMESPACE_URL, &bytes),
    ))
}
fn contact_identity_keys(contact: &ContactInput) -> Vec<String> {
    let mut keys: Vec<_> = contact
        .email
        .iter()
        .map(|email| email.to_lowercase())
        .chain(contact.profile.iter().cloned())
        .collect();
    keys.sort();
    keys.dedup();
    keys
}

fn derived_base(source: &Source, id: RecordId) -> BaseFields {
    let mut base = source.base.clone();
    base.id = id;
    base.created_by = "contact_ingest".into();
    base
}
fn inherit_source(base: &mut BaseFields, source: &Source) -> StorageResult<()> {
    inherit_public_envelope(base, &source.base)?;
    if !base.provenance.sources.contains(&source.base.id) {
        base.provenance.sources.push(source.base.id.clone());
    }
    Ok(())
}
fn base_extends(original: &BaseFields, current: &BaseFields) -> StorageResult<bool> {
    if original.id != current.id
        || original.space_id != current.space_id
        || original.created_at != current.created_at
        || original.created_by != current.created_by
        || original.deleted_at != current.deleted_at
    {
        return Ok(false);
    }
    let mut required = current.clone();
    if inherit_public_envelope(&mut required, original).is_err()
        || required.security != current.security
        || required.subjects != current.subjects
        || required.subjects_state != current.subjects_state
        || required.provenance.sources != current.provenance.sources
    {
        return Ok(false);
    }
    let mut old = serde_json::to_value(&original.provenance).map_err(encoding)?;
    let mut new = serde_json::to_value(&current.provenance).map_err(encoding)?;
    for value in [&mut old, &mut new] {
        if let Some(object) = value.as_object_mut() {
            object.remove("sources");
        }
    }
    Ok(json_extends(&old, &new, "provenance"))
}

fn json_extends(original: &serde_json::Value, current: &serde_json::Value, field: &str) -> bool {
    match (original, current) {
        (serde_json::Value::Object(old), serde_json::Value::Object(new)) => {
            old.iter().all(|(key, value)| {
                new.get(key)
                    .is_some_and(|actual| json_extends(value, actual, key))
            })
        }
        (serde_json::Value::Array(old), serde_json::Value::Array(new))
            if field == "angel_protectees" =>
        {
            old.iter().all(|value| new.contains(value))
        }
        (serde_json::Value::Array(old), serde_json::Value::Array(new)) if field == "chain" => {
            new.starts_with(old)
        }
        _ => original == current,
    }
}

fn retains_contact(original: &ContactRecord, current: &ContactRecord) -> StorageResult<bool> {
    if !base_extends(original.base(), current.base())? {
        return Ok(false);
    }
    match (original, current) {
        (ContactRecord::Entity(old), ContactRecord::Entity(new)) => {
            if !old.aliases.iter().all(|value| new.aliases.contains(value))
                || !old
                    .identity_keys
                    .iter()
                    .all(|value| new.identity_keys.contains(value))
                || (old.display_name != new.display_name
                    && !new.aliases.contains(&old.display_name))
                || !json_extends(&old.metadata, &new.metadata, "metadata")
                || (old.entity_type != new.entity_type && old.entity_type.as_str() != "topic")
                || (old.last_seen.is_some() && new.last_seen != old.last_seen)
            {
                return Ok(false);
            }
            let mut expected = (**old).clone();
            expected.base = new.base.clone();
            expected.aliases.clone_from(&new.aliases);
            expected.identity_keys.clone_from(&new.identity_keys);
            expected.display_name.clone_from(&new.display_name);
            expected.entity_type.clone_from(&new.entity_type);
            expected.metadata.clone_from(&new.metadata);
            expected.last_seen = new.last_seen;
            Ok(expected == **new)
        }
        (ContactRecord::EntityIdentityKey(old), ContactRecord::EntityIdentityKey(new)) => {
            if !new.confidence.is_finite()
                || new.confidence < old.confidence
                || new.confidence > 1.0
                || !old.evidence.iter().all(|row| new.evidence.contains(row))
            {
                return Ok(false);
            }
            let mut expected = (**old).clone();
            expected.base = new.base.clone();
            expected.confidence = new.confidence;
            expected.evidence.clone_from(&new.evidence);
            Ok(expected == **new)
        }
        (ContactRecord::Rel(old), ContactRecord::Rel(new)) => {
            if !new.confidence.is_finite()
                || new.confidence < old.confidence
                || new.confidence > 1.0
                || !old.evidence.iter().all(|row| new.evidence.contains(row))
                || (old.status != new.status
                    && !(old.status == ClaimStatus::Candidate
                        && new.status == ClaimStatus::Approved
                        && !new.evidence.is_empty()))
            {
                return Ok(false);
            }
            let mut expected = (**old).clone();
            expected.base = new.base.clone();
            expected.confidence = new.confidence;
            expected.status = new.status;
            expected.evidence.clone_from(&new.evidence);
            Ok(expected == **new)
        }
        _ => Ok(false),
    }
}

fn same_optional(a: Option<&ContactRecord>, b: Option<&ContactRecord>) -> StorageResult<bool> {
    match (a, b) {
        (None, None) => Ok(true),
        (Some(a), Some(b)) => same(a, b),
        _ => Ok(false),
    }
}
fn same_retirement_baseline(
    a: Option<&ContactRecord>,
    b: Option<&ContactRecord>,
) -> StorageResult<bool> {
    let normalize = |row: Option<&ContactRecord>| -> StorageResult<serde_json::Value> {
        let mut value = serde_json::to_value(row).map_err(encoding)?;
        if let Some(record) = value
            .as_object_mut()
            .and_then(|object| object.values_mut().next())
            .and_then(serde_json::Value::as_object_mut)
        {
            record.remove("updated_at");
            if record
                .get("deleted_at")
                .is_some_and(|value| !value.is_null())
            {
                record.insert("deleted_at".into(), serde_json::Value::Bool(true));
            }
        }
        Ok(value)
    };
    Ok(normalize(a)? == normalize(b)?)
}
fn same(a: &ContactRecord, b: &ContactRecord) -> StorageResult<bool> {
    Ok(serde_json::to_value(a).map_err(encoding)? == serde_json::to_value(b).map_err(encoding)?)
}
#[allow(
    clippy::needless_pass_by_value,
    reason = "This error adapter is passed directly to Result::map_err, whose callback receives the owned serialization error."
)]
fn encoding(error: serde_json::Error) -> StorageError {
    StorageError::SerializationError(error.to_string())
}
fn conflict(message: &str) -> StorageError {
    StorageError::Conflict(message.into())
}
fn invalid(message: &str) -> StorageError {
    StorageError::DataIntegrityError(message.into())
}

#[cfg(test)]
#[path = "source_contact_intake_tests.rs"]
mod tests;
