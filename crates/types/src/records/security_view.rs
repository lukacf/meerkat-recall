//! Shared security view helpers for record-level authorization.

use std::collections::HashSet;

use crate::{Link, RecordId};

use super::assertion::Assertion;
use super::attribute::Attribute;
use super::base::{Security, SubjectsState};
use super::doc::Doc;
use super::doc_node::DocNode;
use super::doc_revision::DocRevision;
use super::entity::Entity;
use super::event::Event;
use super::freshness::Freshness;
use super::identity::{EntityIdentityKey, EntityMerge, IdentityCandidate};
use super::knowledge_view::{
    KnowledgeView, KnowledgeViewDefinition, KnowledgeViewDependencyIndex, KnowledgeViewRevision,
};
use super::predicate::{PredicateAlias, PredicateRegistry};
use super::rel::Rel;
use super::source::Source;
use super::source_watch::{SourceWatch, SourceWatchCheck};
use super::space::SpaceConfig;
use super::system::{AuditLog, SystemEvent};
use super::truth::TruthSlot;
use super::work::WorkItem;

/// Trait for records with security attributes.
pub trait HasSecurity {
    /// Returns the space ID.
    fn space_id(&self) -> &str;
    /// Returns the security configuration.
    fn security(&self) -> &Security;
    /// Returns the subjects associated with the record.
    fn subjects(&self) -> Vec<Link<Entity>> {
        vec![]
    }
    /// Returns the state of the subjects.
    fn subjects_state(&self) -> SubjectsState {
        SubjectsState::Unknown
    }
}

impl HasSecurity for super::base::BaseFields {
    fn space_id(&self) -> &str {
        &self.space_id
    }
    fn security(&self) -> &Security {
        &self.security
    }
    fn subjects(&self) -> Vec<Link<Entity>> {
        self.subjects.clone()
    }
    fn subjects_state(&self) -> SubjectsState {
        self.subjects_state
    }
}

fn dedupe_subject_links(links: impl IntoIterator<Item = Link<Entity>>) -> Vec<Link<Entity>> {
    let mut seen: HashSet<RecordId> = HashSet::new();
    let mut result = Vec::new();
    for link in links {
        if seen.insert(link.id().clone()) {
            result.push(link);
        }
    }
    result
}

impl HasSecurity for EntityIdentityKey {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }
    fn security(&self) -> &Security {
        &self.base.security
    }
    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        subjects.push(self.entity.clone());
        dedupe_subject_links(subjects)
    }
    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for Entity {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        subjects.push(Link::new(self.base.id.clone()));
        dedupe_subject_links(subjects)
    }

    fn subjects_state(&self) -> SubjectsState {
        if self.base.subjects_state == SubjectsState::Unknown {
            SubjectsState::Complete
        } else {
            self.base.subjects_state
        }
    }
}

impl HasSecurity for KnowledgeView {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for KnowledgeViewDefinition {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for KnowledgeViewRevision {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for KnowledgeViewDependencyIndex {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for SourceWatch {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for SourceWatchCheck {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for SystemEvent {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for Event {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for Attribute {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        subjects.push(self.subject.clone());
        dedupe_subject_links(subjects)
    }

    fn subjects_state(&self) -> SubjectsState {
        if self.base.subjects_state == SubjectsState::Unknown {
            SubjectsState::Complete
        } else {
            self.base.subjects_state
        }
    }
}

impl HasSecurity for Freshness {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for Rel {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        subjects.push(self.subject.clone());
        subjects.push(self.object.clone());
        dedupe_subject_links(subjects)
    }

    fn subjects_state(&self) -> SubjectsState {
        if self.base.subjects_state == SubjectsState::Unknown {
            SubjectsState::Complete
        } else {
            self.base.subjects_state
        }
    }
}

impl HasSecurity for Doc {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for Source {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

// A revision is independently classified. Parent-document authorization cannot
// substitute for this immutable envelope, or vice versa. In particular, do not
// upgrade unknown/partial subject metadata while exposing historical text.
impl HasSecurity for DocRevision {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for IdentityCandidate {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        subjects.push(self.entity_a.clone());
        subjects.push(self.entity_b.clone());
        dedupe_subject_links(subjects)
    }

    fn subjects_state(&self) -> SubjectsState {
        if self.base.subjects_state == SubjectsState::Unknown {
            SubjectsState::Complete
        } else {
            self.base.subjects_state
        }
    }
}

impl HasSecurity for EntityMerge {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        subjects.push(self.from.clone());
        subjects.push(self.into.clone());
        dedupe_subject_links(subjects)
    }

    fn subjects_state(&self) -> SubjectsState {
        if self.base.subjects_state == SubjectsState::Unknown {
            SubjectsState::Complete
        } else {
            self.base.subjects_state
        }
    }
}

impl HasSecurity for AuditLog {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for Assertion {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        let mut subjects = self.base.subjects.clone();
        if let Some(ref subject_entity) = self.subject_entity {
            subjects.push(subject_entity.clone());
        }
        if let Some(ref object_entity) = self.object_entity {
            subjects.push(object_entity.clone());
        }
        dedupe_subject_links(subjects)
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for PredicateRegistry {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for PredicateAlias {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for TruthSlot {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for WorkItem {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for SpaceConfig {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

impl HasSecurity for DocNode {
    fn space_id(&self) -> &str {
        &self.base.space_id
    }

    fn security(&self) -> &Security {
        &self.base.security
    }

    fn subjects(&self) -> Vec<Link<Entity>> {
        self.base.subjects.clone()
    }

    fn subjects_state(&self) -> SubjectsState {
        self.base.subjects_state
    }
}

// Status and mutation guards retain only the policy metadata needed after erasure.
// Incomplete subject scoping remains incomplete; no adapter may upgrade it.
impl HasSecurity for crate::publication::SourceAuthorizationEnvelope {
    fn space_id(&self) -> &str {
        &self.space_id
    }
    fn security(&self) -> &Security {
        &self.security
    }
    fn subjects(&self) -> Vec<Link<Entity>> {
        self.subjects.clone()
    }
    fn subjects_state(&self) -> SubjectsState {
        self.subjects_state
    }
}

#[cfg(test)]
mod source_authorization_tests {
    use super::*;
    #[test]
    fn retained_source_policy_never_upgrades_subject_completeness() {
        for state in [
            SubjectsState::Unknown,
            SubjectsState::Partial,
            SubjectsState::Complete,
        ] {
            let envelope = crate::publication::SourceAuthorizationEnvelope {
                space_id: "alpha".into(),
                source_id: RecordId::new_v4(crate::TableName::Source),
                security: Security {
                    policy_id: "default".into(),
                    level: 3,
                    labels: vec![],
                    handling: vec![],
                },
                subjects: vec![],
                subjects_state: state,
            };
            assert_eq!(envelope.subjects_state(), state);
            assert_eq!(envelope.space_id(), "alpha");
            assert_eq!(envelope.security().level, 3);
        }
    }
}

#[cfg(test)]
mod identity_key_security_tests {
    use super::*;
    use crate::records::base::BaseFields;
    use crate::records::identity::IdentityKeyStatus;
    use crate::TableName;

    #[test]
    fn identity_key_requires_linked_subject_and_preserves_unknown_coverage() {
        let entity = Link::new(RecordId::new_v4(TableName::Entity));
        let mut base = BaseFields::new(
            RecordId::new_v4(TableName::EntityIdentityKey),
            "home".into(),
            "test".into(),
        );
        base.subjects.push(entity.clone());
        base.security.handling.push("restricted".into());
        let row = EntityIdentityKey {
            base,
            key: "email:person@example.invalid".into(),
            entity: entity.clone(),
            confidence: 1.0,
            first_seen: None,
            last_seen: None,
            evidence: vec![],
            status: IdentityKeyStatus::Active,
        };
        assert_eq!(row.subjects(), vec![entity]);
        assert_eq!(row.subjects_state(), SubjectsState::Unknown);
        assert_eq!(row.security().handling, ["restricted"]);
    }
}
