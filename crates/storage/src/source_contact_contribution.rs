//! Positive contribution baselines from the exact original contact admission.
//!
//! These rows are not replacement instructions: the inverse composer preserves
//! independently supported current fields before source-exclusive deletion.

use std::collections::{BTreeMap, BTreeSet};

use elephant_types::records::base::BaseFields;
use elephant_types::records::entity::EntityType;
use elephant_types::records::identity::IdentityKeyStatus;
use elephant_types::records::rel::ClaimStatus;
use elephant_types::records::{Entity, EntityIdentityKey, Rel, Source};
use elephant_types::{Link, RecordId};

use crate::canonical_commit::CanonicalRecord;
use crate::StorageResult;

use super::{
    active_entity, conflict, contact_identity_keys, derived_base, encoding, inherit_source,
    invalid, ContactEffect, ContactInput, ContactPredicate, ContactRecord, PreparedSourceContact,
};

impl PreparedSourceContact {
    /// Derive only the claims independently present in the original sealed input.
    /// IDs come from actual admitted effects, never from a new selector lookup.
    ///
    /// The result is a minimal standalone contribution, not a field-deletion mask.
    /// A's inverse composer must combine it with independent current baselines,
    /// check original completion/lineage, and guard every original raw control.
    /// Source-declared provenance remains conjunctive; this method never makes
    /// a genuinely derived source independent of its declared predecessors.
    pub(crate) fn independent_contribution_rows(
        &self,
        original_source: &Source,
    ) -> StorageResult<Vec<CanonicalRecord>> {
        self.validate_source(original_source)?;
        if original_source.base.deleted_at.is_some() {
            return Err(invalid("original contact source is deleted"));
        }
        let input: ContactInput = serde_json::from_value(
            original_source
                .metadata
                .get("contact")
                .cloned()
                .ok_or_else(|| invalid("original sealed contact input missing"))?,
        )
        .map_err(encoding)?;
        // Preparation always records the selected person first, even when a
        // subsequent company effect coalesces onto that same physical entity.
        let Some(ContactEffect {
            after: ContactRecord::Entity(selected_person),
            ..
        }) = self.effects.first()
        else {
            return Err(invalid("original contact person effect missing"));
        };
        active_entity(selected_person)?;
        if selected_person.entity_type != EntityType::new("person") {
            return Err(conflict("contact_contribution_person_type_unowned"));
        }
        let person_id = selected_person.base.id.clone();
        if input.name.as_ref().is_some_and(|name| {
            selected_person.display_name != *name && !selected_person.aliases.contains(name)
        }) {
            return Err(invalid(
                "original person effect does not retain the contact name",
            ));
        }
        let keys = contact_identity_keys(&input);
        if keys
            .iter()
            .any(|key| !selected_person.identity_keys.contains(key))
        {
            return Err(invalid(
                "original person effect does not retain contact keys",
            ));
        }
        let mut person = Entity::new(
            contribution_base(original_source, person_id.clone())?,
            EntityType::new("person"),
            input
                .name
                .clone()
                .or_else(|| input.email.clone())
                .unwrap_or_else(|| "Unknown".into()),
        );
        person.identity_keys.clone_from(&keys);
        let mut rows = BTreeMap::new();
        rows.insert(person_id.clone(), ContactRecord::Entity(Box::new(person)));
        let mut covered_keys = BTreeSet::new();
        for effect in &self.effects {
            if let ContactRecord::EntityIdentityKey(admitted) = &effect.after {
                if !keys.contains(&admitted.key)
                    || admitted.entity.id() != &person_id
                    || admitted.status != IdentityKeyStatus::Active
                    || !admitted
                        .evidence
                        .iter()
                        .any(|source| source.id() == &self.source_id)
                {
                    return Err(invalid(
                        "original contact key effect does not match its input",
                    ));
                }
                if admitted.confidence.to_bits() != 1.0_f64.to_bits() {
                    return Err(conflict("contact_contribution_key_confidence_unowned"));
                }
                covered_keys.insert(admitted.key.clone());
                rows.insert(
                    admitted.base.id.clone(),
                    ContactRecord::EntityIdentityKey(Box::new(contribution_identity_key(
                        original_source,
                        &self.source_id,
                        &person_id,
                        admitted,
                    )?)),
                );
            }
        }
        if covered_keys != keys.into_iter().collect() {
            return Err(invalid("original contact identity effect is incomplete"));
        }
        if let Some(company) = input.company.as_ref().filter(|_| input.job_title.is_some()) {
            self.project_company_contribution(original_source, &person_id, company, &mut rows)?;
        }
        // No omitted effect may turn into an unconstrained fallback to its
        // original complete afterimage. Tombstones were dependencies, not effects.
        if self
            .effects
            .iter()
            .any(|effect| !rows.contains_key(&effect.after.base().id))
        {
            return Err(invalid(
                "original contact has an unsupported independent effect",
            ));
        }
        Ok(rows.into_values().map(|row| row.canonical()).collect())
    }

    fn project_company_contribution(
        &self,
        source: &Source,
        person: &RecordId,
        company_name: &str,
        rows: &mut BTreeMap<RecordId, ContactRecord>,
    ) -> StorageResult<()> {
        let endpoints: Vec<_> = self
            .predicates
            .iter()
            .filter_map(|predicate| match predicate {
                ContactPredicate::WorksAt {
                    subject, object, ..
                } if subject == person => Some(object.clone()),
                _ => None,
            })
            .collect();
        let [company_id] = endpoints.as_slice() else {
            return Err(invalid(
                "original contact company endpoint is ambiguous or missing",
            ));
        };
        let Some(ContactEffect {
            after: ContactRecord::Entity(admitted_company),
            ..
        }) = self
            .effects
            .iter()
            .find(|effect| &effect.after.base().id == company_id)
        else {
            return Err(invalid("original contact company effect missing"));
        };
        active_entity(admitted_company)?;
        if company_id != person && admitted_company.entity_type != EntityType::new("org") {
            return Err(conflict("contact_contribution_company_type_unowned"));
        }
        if admitted_company.display_name != company_name
            && !admitted_company
                .aliases
                .iter()
                .any(|alias| alias == company_name)
        {
            return Err(invalid(
                "original company effect does not retain contact company",
            ));
        }
        if company_id == person {
            let Some(ContactRecord::Entity(projected)) = rows.get_mut(person) else {
                return Err(invalid("original coalesced contact entity missing"));
            };
            if projected.display_name != company_name {
                projected.aliases.push(company_name.to_owned());
            }
        } else {
            rows.insert(
                company_id.clone(),
                ContactRecord::Entity(Box::new(Entity::new(
                    contribution_base(source, company_id.clone())?,
                    EntityType::new("org"),
                    company_name.to_owned(),
                ))),
            );
        }
        for effect in &self.effects {
            if let ContactRecord::Rel(admitted) = &effect.after {
                if admitted.subject.id() != person
                    || admitted.object.id() != company_id
                    || admitted.rel_type != "works_at"
                {
                    return Err(invalid("original contact relationship endpoints changed"));
                }
                // The contact input cannot express these inherited facts. A
                // standalone default would silently rewrite their meaning.
                if admitted.validity_window.is_some()
                    || admitted.status != ClaimStatus::Approved
                    || admitted.confidence.to_bits() != 0.95_f32.to_bits()
                {
                    return Err(conflict("contact_contribution_relationship_facts_unowned"));
                }
                rows.insert(
                    admitted.base.id.clone(),
                    ContactRecord::Rel(Box::new(Rel {
                        base: contribution_base(source, admitted.base.id.clone())?,
                        subject: Link::new(person.clone()),
                        object: Link::new(company_id.clone()),
                        rel_type: "works_at".into(),
                        status: ClaimStatus::Approved,
                        confidence: 0.95,
                        validity_window: None,
                        evidence: vec![],
                        temporal_audit: None,
                    })),
                );
            }
        }
        Ok(())
    }
}

fn contribution_identity_key(
    source: &Source,
    source_id: &RecordId,
    person_id: &RecordId,
    admitted: &EntityIdentityKey,
) -> StorageResult<EntityIdentityKey> {
    Ok(EntityIdentityKey {
        base: contribution_base(source, admitted.base.id.clone())?,
        key: admitted.key.clone(),
        entity: Link::new(person_id.clone()),
        confidence: 1.0,
        first_seen: Some(source.base.created_at),
        last_seen: Some(source.base.updated_at),
        evidence: vec![Link::new(source_id.clone())],
        status: IdentityKeyStatus::Active,
    })
}

fn contribution_base(source: &Source, id: RecordId) -> StorageResult<BaseFields> {
    let mut base = derived_base(source, id);
    inherit_source(&mut base, source)?;
    Ok(base)
}
