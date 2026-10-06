//! Protected, unverified V1 dependency claims and conservative derivation.
//!
//! This module preserves what an authority claims influenced work. It neither
//! authenticates that authority nor proves its inventory complete or current.
//! Elephant policy, current source access, recipient checks and protected entry
//! remain separate. Serialized envelopes belong on protected owner channels;
//! prompt, UI, log and payload projections require their own authorization.

use crate::restrictions::{AudienceRef, ProcessorRef, ResourceDomain, SetRestriction};
use crate::PrincipalClaim;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Exact resource identity within a claimed authority domain.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceRef {
    /// Claimed resource owner and namespace.
    pub domain: ResourceDomain,
    /// Exact owner-local resource identity.
    pub resource_id: String,
}

/// Content observation and separate historical security observation.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceObservation {
    /// Observed resource.
    pub resource: ResourceRef,
    /// Exact immutable content revision.
    pub content_revision: String,
    /// Historical epoch, never a current-access proof.
    pub observed_security_epoch: u64,
}

/// Each type of influence retains the source dependency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Influence {
    /// Content reached the derivation.
    Content,
    /// A control choice influenced the derivation.
    Control,
    /// A provider retained context influencing the derivation.
    ProviderHeldContext,
}

/// One source observation and its influence on work.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dependency {
    /// Exact content and historical security observation.
    pub source: SourceObservation,
    /// Why it belongs in the complete dependency domain.
    pub influence: Influence,
}

/// Resource-bearing details for an unresolved contributor.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDetails {
    /// Referenced protected resource.
    pub resource: ResourceRef,
}

/// Authority-domain details for an unresolved contributor.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainDetails {
    /// Unavailable source domain.
    pub domain: ResourceDomain,
}

/// Provider-context details for an unresolved contributor.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProcessorDetails {
    /// Processor whose retained context is not inventoried.
    pub processor: ProcessorRef,
}

/// Legacy-bundle details for an unresolved contributor.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BundleDetails {
    /// Bundle whose dependencies are not established.
    pub bundle: ResourceRef,
}

/// Unresolved contributors persist alongside known dependencies.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum UnknownContributor {
    /// A contributor cannot be identified.
    UnidentifiedSource {},
    /// No immutable content revision is known.
    MissingContentRevision {
        /// Protected resource details.
        details: ResourceDetails,
    },
    /// A resource has incomplete contributing-source provenance.
    IncompleteTransitiveProvenance {
        /// Protected resource details.
        details: ResourceDetails,
    },
    /// Its current source authority is unavailable.
    UnavailableSourceAuthority {
        /// Protected domain details.
        details: DomainDetails,
    },
    /// A provider's retained context has not been inventoried.
    UninventoriedProviderContext {
        /// Protected processor details.
        details: ProcessorDetails,
    },
    /// A legacy bundle has no accepted complete provenance.
    UnknownLegacyProvenance {
        /// Protected bundle details.
        details: BundleDetails,
    },
}

/// Unauthenticated inventory that retains every distinct source observation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DependencyInventory {
    records: BTreeSet<Dependency>,
    unknown: BTreeSet<UnknownContributor>,
}

impl DependencyInventory {
    /// Construct an unverified claim. Empty input is an explicit union identity,
    /// never proof that a provider or legacy bundle has no contributors.
    #[must_use]
    pub fn new(
        records: impl IntoIterator<Item = Dependency>,
        unknown: impl IntoIterator<Item = UnknownContributor>,
    ) -> Self {
        Self {
            records: records.into_iter().collect(),
            unknown: unknown.into_iter().collect(),
        }
    }

    /// Preserve all known and unknown contributors from both observations.
    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        Self {
            records: self.records.union(&other.records).cloned().collect(),
            unknown: self.unknown.union(&other.unknown).cloned().collect(),
        }
    }

    /// Whether this claim lists no unresolved contributor. This does not prove
    /// truthful inventory, subject completeness, current permission or release.
    #[must_use]
    pub fn declares_complete_inventory(&self) -> bool {
        self.unknown.is_empty()
    }

    /// Protected observations for the enforcing owner's current source checks.
    #[must_use]
    pub const fn records(&self) -> &BTreeSet<Dependency> {
        &self.records
    }

    /// Protected unresolved contributors; public refusals need another projection.
    #[must_use]
    pub const fn unknown_contributors(&self) -> &BTreeSet<UnknownContributor> {
        &self.unknown
    }
}

/// Knowledge entity the source concerns, not an authenticated principal.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubjectEntityRef {
    /// Entity's source authority domain.
    pub domain: ResourceDomain,
    /// Exact knowledge entity identifier.
    pub entity_id: String,
}

/// Subject completeness remains distinct from inventory completeness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubjectCompleteness {
    /// Source authority claims its subjects complete, including an empty set.
    Complete,
    /// Only some subjects are identified.
    Partial,
    /// Subject coverage is unknown.
    Unknown,
    /// Subject authority is unavailable.
    Unavailable,
}

/// Source-local subject claims whose meaning belongs to the source authority.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceSubjects {
    /// Claimed source-local subjects.
    pub entities: BTreeSet<SubjectEntityRef>,
    /// Claimed coverage of that subject inventory.
    pub completeness: SubjectCompleteness,
}

/// Attribute meaning declared by its source policy domain.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicyAttribute {
    /// Classification fact.
    Classification {
        /// Exact policy-owned name.
        name: String,
    },
    /// Restrictive handling requirement.
    RestrictiveHandling {
        /// Exact policy-owned name.
        name: String,
    },
    /// Exception belonging only to this source observation.
    PermissionException {
        /// Exact policy-owned name.
        name: String,
    },
}

/// Historical policy evidence remains attached to one exact source observation.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourcePolicySnapshot {
    /// Source whose policy was observed.
    pub source: SourceObservation,
    /// Exact policy-domain identity within that source authority.
    pub policy_domain: String,
    /// Historical policy content revision, not current authority.
    pub policy_revision: String,
    /// Source-local attributes, never flattened across sources.
    pub attributes: BTreeSet<PolicyAttribute>,
    /// Source-local subject claims.
    pub subjects: SourceSubjects,
}

/// Correlation with claimed owner evidence, not a replayable authorization.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceReference {
    /// Claimed evidence owner.
    pub authority: PrincipalClaim,
    /// Exact owner-local reference.
    pub reference_id: String,
}

/// Ordinary derivations preserve all observed input restrictions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DerivationKind {
    /// Model-produced answer.
    ModelAnswer,
    /// Summary of contributing content.
    Summary,
    /// Compacted context.
    Compaction,
    /// Embedding of contributing content.
    Embedding,
    /// Extracted memory.
    MemoryExtraction,
    /// Cached copy.
    CacheCopy,
    /// Generated tool arguments.
    ToolArguments,
    /// Produced artifact.
    Artifact,
}

/// Claimed envelope origin with protected owner correlation.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "origin", rename_all = "snake_case", deny_unknown_fields)]
pub enum EnvelopeOrigin {
    /// Source authority's original claim.
    Source {
        /// Claimed owner evidence.
        evidence: EvidenceReference,
    },
    /// Ordinary derivation from input envelopes.
    Derived {
        /// Derivation category, never a declassification instruction.
        kind: DerivationKind,
        /// Claimed derivation-owner evidence.
        evidence: EvidenceReference,
    },
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EnvelopeFields {
    dependencies: DependencyInventory,
    processors: SetRestriction<ProcessorRef>,
    audiences: SetRestriction<AudienceRef>,
    source_policies: BTreeSet<SourcePolicySnapshot>,
    origin: EnvelopeOrigin,
}

/// Protected unverified envelope, structurally validated on construction/decode.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "EnvelopeFields", into = "EnvelopeFields")]
pub struct ProtectedEnvelope(EnvelopeFields);

/// Structural refusal independent of resource-access policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeRefusal {
    /// Policy observations do not exactly cover the known source observations.
    #[error("source and policy observations have different coverage")]
    SourceCoverageMismatch,
    /// An ordinary derivation must have at least one contributing envelope.
    #[error("ordinary derivation has no inputs")]
    NoInputs,
}

impl TryFrom<EnvelopeFields> for ProtectedEnvelope {
    type Error = EnvelopeRefusal;
    fn try_from(fields: EnvelopeFields) -> Result<Self, Self::Error> {
        let known: BTreeSet<_> = fields
            .dependencies
            .records
            .iter()
            .map(|d| &d.source)
            .collect();
        let governed: BTreeSet<_> = fields.source_policies.iter().map(|p| &p.source).collect();
        if known != governed {
            return Err(EnvelopeRefusal::SourceCoverageMismatch);
        }
        Ok(Self(fields))
    }
}

impl From<ProtectedEnvelope> for EnvelopeFields {
    fn from(envelope: ProtectedEnvelope) -> Self {
        envelope.0
    }
}

impl ProtectedEnvelope {
    /// Construct a source claim with exact source-policy coverage.
    ///
    /// # Errors
    /// Missing or extra source-policy observations refuse. This does not verify
    /// the claimant, complete dependencies, current policy or source access.
    pub fn source_claim(
        dependencies: DependencyInventory,
        processors: SetRestriction<ProcessorRef>,
        audiences: SetRestriction<AudienceRef>,
        source_policies: impl IntoIterator<Item = SourcePolicySnapshot>,
        evidence: EvidenceReference,
    ) -> Result<Self, EnvelopeRefusal> {
        Self::try_from(EnvelopeFields {
            dependencies,
            processors,
            audiences,
            source_policies: source_policies.into_iter().collect(),
            origin: EnvelopeOrigin::Source { evidence },
        })
    }

    /// Preserve every observed input, including uncited control/provider input.
    ///
    /// # Errors
    /// An empty input set refuses. Inputs are structurally validated carriers,
    /// not authenticated inventories. No ordinary derivation removes unknown
    /// contributors, transfers a source's exceptions or declassifies content.
    pub fn derive(
        inputs: &[Self],
        kind: DerivationKind,
        evidence: EvidenceReference,
    ) -> Result<Self, EnvelopeRefusal> {
        let (first, rest) = inputs.split_first().ok_or(EnvelopeRefusal::NoInputs)?;
        let mut fields = first.0.clone();
        for input in rest {
            fields.dependencies = fields.dependencies.union(&input.0.dependencies);
            fields.processors = fields.processors.intersect(&input.0.processors);
            fields.audiences = fields.audiences.intersect(&input.0.audiences);
            fields
                .source_policies
                .extend(input.0.source_policies.iter().cloned());
        }
        fields.origin = EnvelopeOrigin::Derived { kind, evidence };
        Self::try_from(fields)
    }

    /// Protected dependency inventory for current source-owner checks.
    #[must_use]
    pub const fn dependencies(&self) -> &DependencyInventory {
        &self.0.dependencies
    }

    /// Claimed processing ceiling, not permission to send data.
    #[must_use]
    pub const fn processors(&self) -> &SetRestriction<ProcessorRef> {
        &self.0.processors
    }

    /// Claimed audience ceiling, not current release authorization.
    #[must_use]
    pub const fn audiences(&self) -> &SetRestriction<AudienceRef> {
        &self.0.audiences
    }

    /// Source-local historical policy observations.
    #[must_use]
    pub const fn source_policies(&self) -> &BTreeSet<SourcePolicySnapshot> {
        &self.0.source_policies
    }
}

redacted_debug!(
    ResourceRef,
    SourceObservation,
    Dependency,
    ResourceDetails,
    DomainDetails,
    ProcessorDetails,
    BundleDetails,
    UnknownContributor,
    DependencyInventory,
    SubjectEntityRef,
    SourceSubjects,
    PolicyAttribute,
    SourcePolicySnapshot,
    EvidenceReference,
    EnvelopeOrigin,
    ProtectedEnvelope,
);
