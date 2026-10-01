//! Independent implementation of the normalized Meerkat/Elephant contract.
//!
//! This module compares untrusted capability claims. Compatibility authenticates
//! neither a peer nor its implementation and grants no resource access. Active
//! HTTP/MCP routes still refuse governed exchange tokens. A future composition
//! must derive its advertisement from its actual owners and retain the selected
//! requirements with work; it must not advertise support merely from this enum.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Security protocol version, independent of the Elephant package version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractVersion {
    /// Major contract version.
    pub major: u16,
    /// Minor contract version. Compatibility is exact, including this field.
    pub minor: u16,
}

impl ContractVersion {
    /// The only implemented semantic version.
    pub const V1: Self = Self { major: 1, minor: 0 };
}

/// Selected security behavior. Unknown profiles have no fallback.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractProfile {
    /// Explicit trusted embedded behavior, never an implicit downgrade.
    TrustedEmbedded,
    /// Buffered governed behavior, requiring every baseline duty below.
    GovernedBufferedV1,
}

/// Common semantics required of participating owners, not tool permissions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Semantic {
    /// Principal identity includes its trusted domain.
    QualifiedIdentity,
    /// Admitted work retains its authoritative requester association.
    RetainedWorkAssociation,
    /// Child grants cannot widen their ancestors.
    AttenuatingDelegation,
    /// Every applicable decision must permit the operation.
    ConjunctiveDecisions,
    /// Decisions bind the exact operation and arguments.
    ExactOperationBinding,
    /// Actual entry fences every participating authority.
    ParticipatingAuthorityFence,
    /// Mandatory evidence is durably coupled to its operation owner.
    DurableSecurityEvidence,
    /// Derivations retain all observed dependency restrictions.
    ConservativeDependencies,
    /// Release checks current recipient authority.
    CurrentAudienceRelease,
    /// Recovery refuses stale authority snapshots.
    RollbackResistance,
}

/// Mandatory duties, each realized by its canonical participating owner.
///
/// Each participant enforces its own operation or verifies exact evidence from
/// its canonical owner under a declared handoff. A composed receipt need not be
/// duplicated. Commissioning a service does not give its caller direct read
/// access to the service's private inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Obligation {
    /// Entry has a mandatory durable receipt.
    DurableEntryReceipt,
    /// Settlement has a mandatory durable receipt.
    DurableSettlementReceipt,
    /// Resource use checks its current authority.
    CurrentResourceUse,
    /// Release checks its current intended recipient.
    CurrentRecipientRelease,
    /// Information retains its dependency restrictions.
    PreserveDependencies,
    /// Credential owners confine secret material to authorized sinks.
    ConfineSecrets,
}

const BASELINE_SEMANTICS: [Semantic; 10] = [
    Semantic::QualifiedIdentity,
    Semantic::RetainedWorkAssociation,
    Semantic::AttenuatingDelegation,
    Semantic::ConjunctiveDecisions,
    Semantic::ExactOperationBinding,
    Semantic::ParticipatingAuthorityFence,
    Semantic::DurableSecurityEvidence,
    Semantic::ConservativeDependencies,
    Semantic::CurrentAudienceRelease,
    Semantic::RollbackResistance,
];
const BASELINE_OBLIGATIONS: [Obligation; 6] = [
    Obligation::DurableEntryReceipt,
    Obligation::DurableSettlementReceipt,
    Obligation::CurrentResourceUse,
    Obligation::CurrentRecipientRelease,
    Obligation::PreserveDependencies,
    Obligation::ConfineSecrets,
];

/// Exact retained requirements. No absent field or empty set chooses defaults.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractRequirements {
    /// Independently versioned wire semantics.
    pub version: ContractVersion,
    /// Explicitly selected behavior.
    pub profile: ContractProfile,
    /// Additional required semantics; governed baseline is always enforced.
    pub semantics: BTreeSet<Semantic>,
    /// Additional required duties; governed baseline is always enforced.
    pub obligations: BTreeSet<Obligation>,
}

/// Claimed support for a single exact version/profile pair.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractSupport {
    /// Exact version supported by this entry.
    pub version: ContractVersion,
    /// Exact profile supported by this entry.
    pub profile: ContractProfile,
    /// Claimed semantic support, not proof of implementation.
    pub semantics: BTreeSet<Semantic>,
    /// Claimed obligation support, not fulfillment for an operation.
    pub obligations: BTreeSet<Obligation>,
}

/// Untrusted advertisement. There is deliberately no all-supported default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContractAdvertisement {
    /// Support entries; duplicate selected version/profile entries refuse.
    pub contracts: Vec<ContractSupport>,
}

/// Participant whose advertisement cannot satisfy the requirements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Participant {
    /// This composition.
    Local,
    /// Its authenticated remote counterpart, once separately verified.
    Remote,
}

/// Classified incompatibility. A refusal must not trigger an ungoverned retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ContractRefusal {
    /// The evaluator has no semantics for the requested version.
    #[error("security contract version is not implemented")]
    UnknownVersion,
    /// Participant omitted the exact version.
    #[error("{0:?} does not support the security contract version")]
    UnsupportedVersion(Participant),
    /// Participant omitted the selected profile within the exact version.
    #[error("{0:?} does not support the selected security profile")]
    UnsupportedProfile(Participant),
    /// Duplicate entries would introduce implicit rule ordering.
    #[error("{0:?} advertises ambiguous contract support")]
    AmbiguousSupport(Participant),
    /// Participant lacks a required semantic in this exact entry.
    #[error("{0:?} does not support a required security semantic")]
    UnsupportedSemantic(Participant),
    /// Participant lacks a required obligation in this exact entry.
    #[error("{0:?} cannot fulfill a required security obligation")]
    UnsupportedObligation(Participant),
}

/// Checked claim compatibility only. This is not serializable or a permit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibleContract(ContractRequirements);

impl CompatibleContract {
    /// Effective requirements, including all implicit governed baseline duties.
    #[must_use]
    pub const fn requirements(&self) -> &ContractRequirements {
        &self.0
    }
}

fn check_participant(
    required: &ContractRequirements,
    advertisement: &ContractAdvertisement,
    participant: Participant,
) -> Result<(), ContractRefusal> {
    let matching_version: Vec<_> = advertisement
        .contracts
        .iter()
        .filter(|entry| entry.version == required.version)
        .collect();
    if matching_version.is_empty() {
        return Err(ContractRefusal::UnsupportedVersion(participant));
    }
    let matching_profile: Vec<_> = matching_version
        .into_iter()
        .filter(|entry| entry.profile == required.profile)
        .collect();
    let support = match matching_profile.as_slice() {
        [] => return Err(ContractRefusal::UnsupportedProfile(participant)),
        [only] => *only,
        _ => return Err(ContractRefusal::AmbiguousSupport(participant)),
    };
    if !required.semantics.is_subset(&support.semantics) {
        return Err(ContractRefusal::UnsupportedSemantic(participant));
    }
    if !required.obligations.is_subset(&support.obligations) {
        return Err(ContractRefusal::UnsupportedObligation(participant));
    }
    Ok(())
}

/// Check both advertisements against the retained exact contract.
///
/// The advertisements must be bound to actual owners and authenticated peers
/// by a separate trusted composition. A historical successful result confers
/// no current access and cannot replace negotiation after restore or handoff.
///
/// # Errors
/// Refuses unknown versions, missing or ambiguous exact support, and any
/// missing semantic or obligation. It never chooses another version/profile.
pub fn check_compatibility(
    required: &ContractRequirements,
    local: &ContractAdvertisement,
    remote: &ContractAdvertisement,
) -> Result<CompatibleContract, ContractRefusal> {
    if required.version != ContractVersion::V1 {
        return Err(ContractRefusal::UnknownVersion);
    }
    let mut effective = required.clone();
    if effective.profile == ContractProfile::GovernedBufferedV1 {
        effective.semantics.extend(BASELINE_SEMANTICS);
        effective.obligations.extend(BASELINE_OBLIGATIONS);
    }
    check_participant(&effective, local, Participant::Local)?;
    check_participant(&effective, remote, Participant::Remote)?;
    Ok(CompatibleContract(effective))
}
