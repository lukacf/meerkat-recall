//! Pure V1 restriction algebra for independent contract consumption.
//!
//! These are untrusted ceilings, not authenticated policy or grants. No bound
//! authorizes a resource operation, discovers authority from identifier text or
//! replaces Elephant's resource policy. Correlated policy alternatives cannot be
//! reduced to these independent Cartesian dimensions.

use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// A required fact that has not been established.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Unresolved {
    /// No contributing fact was supplied.
    Absent,
    /// Its value or meaning is unknown.
    Unknown,
    /// Its authority is unavailable.
    Unavailable,
}

/// Result of mathematical matching, never an access decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Match {
    /// All known bounds include the value.
    Matches,
    /// A known bound excludes the value.
    OutsideBounds,
    /// At least one contributing fact remains unresolved.
    Unresolved(BTreeSet<Unresolved>),
}

/// Exact set restriction with explicit unrestricted identity.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "bound", rename_all = "snake_case", deny_unknown_fields)]
pub enum SetBound<T: Ord> {
    /// Explicit identity for intersection; missing data cannot select it.
    Unrestricted {},
    /// Empty values permit nothing. Text such as `*` is literal.
    Exact {
        /// Exact, owner-declared values.
        values: BTreeSet<T>,
    },
}

/// Known set ceiling and all unresolved contributors.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SetRestriction<T: Ord> {
    bound: SetBound<T>,
    unresolved: BTreeSet<Unresolved>,
}

impl<T: Ord> std::fmt::Debug for SetRestriction<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SetRestriction")
            .field("protected_metadata", &"redacted")
            .finish_non_exhaustive()
    }
}

impl<T: Ord + Clone> SetRestriction<T> {
    /// Explicit mathematical identity, never a missing-authority fallback.
    #[must_use]
    pub const fn unrestricted() -> Self {
        Self {
            bound: SetBound::Unrestricted {},
            unresolved: BTreeSet::new(),
        }
    }

    /// Claim a finite ceiling, without authenticating its source.
    #[must_use]
    pub fn exact(values: impl IntoIterator<Item = T>) -> Self {
        Self {
            bound: SetBound::Exact {
                values: values.into_iter().collect(),
            },
            unresolved: BTreeSet::new(),
        }
    }

    /// Preserve an unresolved contributing fact.
    #[must_use]
    pub fn unresolved(fact: Unresolved) -> Self {
        Self {
            bound: SetBound::Unrestricted {},
            unresolved: BTreeSet::from([fact]),
        }
    }

    /// Intersect known ceilings while retaining every unresolved contributor.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let bound = match (&self.bound, &other.bound) {
            (SetBound::Exact { values: a }, SetBound::Exact { values: b }) => SetBound::Exact {
                values: a.intersection(b).cloned().collect(),
            },
            (SetBound::Unrestricted {}, value) | (value, SetBound::Unrestricted {}) => {
                value.clone()
            }
        };
        Self {
            bound,
            unresolved: self.unresolved.union(&other.unresolved).copied().collect(),
        }
    }

    /// Match only this claim's bounds; no current access is established.
    #[must_use]
    pub fn check(&self, value: &T) -> Match {
        if !self.unresolved.is_empty() {
            return Match::Unresolved(self.unresolved.clone());
        }
        match &self.bound {
            SetBound::Exact { values } if !values.contains(value) => Match::OutsideBounds,
            _ => Match::Matches,
        }
    }
}

/// Claimed time window, inclusive at start and exclusive at expiry.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimeWindow {
    /// Unix milliseconds at first permitted matching instant.
    pub not_before_ms: u64,
    /// Unix milliseconds at first excluded instant.
    pub expires_at_ms: u64,
}

/// Time bound. Inverted windows match no instant.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "bound", rename_all = "snake_case", deny_unknown_fields)]
pub enum TimeBound {
    /// Explicit mathematical identity.
    Unrestricted {},
    /// Start-inclusive, end-exclusive interval.
    Window {
        /// Exact interval claim.
        window: TimeWindow,
    },
    /// No instant is included.
    Empty {},
}

/// Claimed lifetime with unresolved contributing facts retained.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Lifetime {
    bound: TimeBound,
    unresolved: BTreeSet<Unresolved>,
}

impl Lifetime {
    /// Explicit identity for time intersection.
    #[must_use]
    pub const fn unrestricted() -> Self {
        Self {
            bound: TimeBound::Unrestricted {},
            unresolved: BTreeSet::new(),
        }
    }

    /// Retain the intersection of both windows and every unresolved fact.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let bound = match (self.bound, other.bound) {
            (TimeBound::Empty {}, _) | (_, TimeBound::Empty {}) => TimeBound::Empty {},
            (TimeBound::Unrestricted {}, b) | (b, TimeBound::Unrestricted {}) => b,
            (TimeBound::Window { window: a }, TimeBound::Window { window: b }) => {
                let start = a.not_before_ms.max(b.not_before_ms);
                let end = a.expires_at_ms.min(b.expires_at_ms);
                if start < end {
                    TimeBound::Window {
                        window: TimeWindow {
                            not_before_ms: start,
                            expires_at_ms: end,
                        },
                    }
                } else {
                    TimeBound::Empty {}
                }
            }
        };
        Self {
            bound,
            unresolved: self.unresolved.union(&other.unresolved).copied().collect(),
        }
    }

    /// Match an observed instant without authenticating the clock or claim.
    #[must_use]
    pub fn check(&self, now_ms: u64) -> Match {
        if !self.unresolved.is_empty() {
            return Match::Unresolved(self.unresolved.clone());
        }
        let includes = match self.bound {
            TimeBound::Unrestricted {} => true,
            TimeBound::Empty {} => false,
            TimeBound::Window { window } => {
                window.not_before_ms <= now_ms && now_ms < window.expires_at_ms
            }
        };
        if includes {
            Match::Matches
        } else {
            Match::OutsideBounds
        }
    }
}

/// Remaining child edges, independent of current-use permission.
#[derive(Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "bound", rename_all = "snake_case", deny_unknown_fields)]
pub enum DepthBound {
    /// Explicit unbounded edge count.
    Unrestricted {},
    /// Zero permits no additional child edge.
    Remaining {
        /// Remaining child edges.
        edges: u32,
    },
}

/// Claimed depth ceiling with required unresolved facts retained.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationDepth {
    bound: DepthBound,
    unresolved: BTreeSet<Unresolved>,
}

/// Refused depth attenuation, without exposing resource identities.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DepthRefusal {
    /// Parent has no remaining child edge.
    #[error("delegation depth is exhausted")]
    Exhausted,
    /// Parent ceiling contains unresolved facts.
    #[error("delegation depth is unresolved")]
    Unresolved,
}

impl DelegationDepth {
    /// Explicit identity for same-level depth intersection.
    #[must_use]
    pub const fn unrestricted() -> Self {
        Self {
            bound: DepthBound::Unrestricted {},
            unresolved: BTreeSet::new(),
        }
    }

    /// Same-level intersection, without consuming an edge.
    #[must_use]
    pub fn intersect(&self, other: &Self) -> Self {
        let bound = match (self.bound, other.bound) {
            (DepthBound::Remaining { edges: a }, DepthBound::Remaining { edges: b }) => {
                DepthBound::Remaining { edges: a.min(b) }
            }
            (DepthBound::Unrestricted {}, b) | (b, DepthBound::Unrestricted {}) => b,
        };
        Self {
            bound,
            unresolved: self.unresolved.union(&other.unresolved).copied().collect(),
        }
    }

    /// Consume one parent edge and intersect the requested child ceiling.
    ///
    /// # Errors
    /// Refuses an exhausted or unresolved parent. Requested unresolved facts are
    /// retained in the result and still prevent use. Grant issuance and all
    /// other constraints remain separate obligations.
    pub fn child(&self, requested: &Self) -> Result<Self, DepthRefusal> {
        if !self.unresolved.is_empty() {
            return Err(DepthRefusal::Unresolved);
        }
        let inherited = match self.bound {
            DepthBound::Unrestricted {} => Self::unrestricted(),
            DepthBound::Remaining { edges: 0 } => return Err(DepthRefusal::Exhausted),
            DepthBound::Remaining { edges } => Self {
                bound: DepthBound::Remaining { edges: edges - 1 },
                unresolved: BTreeSet::new(),
            },
        };
        Ok(inherited.intersect(requested))
    }
}

/// Feature-owned exact action name, without wildcard interpretation.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ActionRef {
    /// Owning feature.
    pub feature: String,
    /// Exact action.
    pub action: String,
}

/// Claimed resource namespace, scoped by its exact qualified owner.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceDomain {
    /// Claimed owner; wire data does not authenticate it.
    pub authority: crate::PrincipalClaim,
    /// Exact owner-local namespace.
    pub namespace: String,
}

/// Claimed processing principal or owner-declared route.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "processor", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProcessorRef {
    /// Exact principal.
    Principal {
        /// Claimed principal.
        principal: crate::PrincipalClaim,
    },
    /// Exact route under an authority.
    Route {
        /// Claimed route authority.
        authority: crate::PrincipalClaim,
        /// Exact route.
        route_id: String,
    },
}

/// Claimed recipient or separately governed destination.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "audience", rename_all = "snake_case", deny_unknown_fields)]
pub enum AudienceRef {
    /// Exact principal.
    Principal {
        /// Claimed recipient.
        principal: crate::PrincipalClaim,
    },
    /// Destination whose current audience remains separately authoritative.
    Destination {
        /// Claimed destination authority.
        authority: crate::PrincipalClaim,
        /// Exact destination.
        destination_id: String,
    },
}

redacted_debug!(
    ResourceDomain,
    ProcessorRef,
    AudienceRef,
    ActionRef,
    TimeWindow,
    TimeBound,
    Lifetime,
    DepthBound,
    DelegationDepth,
    ExecutionRestrictions,
);

/// Independent Cartesian ceilings; this is not an ABAC expression language.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionRestrictions {
    /// Exact action ceiling.
    pub actions: SetRestriction<ActionRef>,
    /// Exact resource-domain ceiling.
    pub resource_domains: SetRestriction<ResourceDomain>,
    /// Exact processor ceiling.
    pub processors: SetRestriction<ProcessorRef>,
    /// Exact audience ceiling.
    pub audiences: SetRestriction<AudienceRef>,
    /// Claimed temporal ceiling.
    pub lifetime: Lifetime,
    /// Claimed delegation ceiling.
    pub delegation_depth: DelegationDepth,
}

impl ExecutionRestrictions {
    /// Apply every parent ceiling and consume one child edge.
    ///
    /// # Errors
    /// Refuses exhausted or unresolved depth. Other unresolved dimensions are
    /// retained and still prevent use; success is not an issued grant.
    pub fn child(&self, requested: &Self) -> Result<Self, DepthRefusal> {
        Ok(Self {
            actions: self.actions.intersect(&requested.actions),
            resource_domains: self.resource_domains.intersect(&requested.resource_domains),
            processors: self.processors.intersect(&requested.processors),
            audiences: self.audiences.intersect(&requested.audiences),
            lifetime: self.lifetime.intersect(&requested.lifetime),
            delegation_depth: self.delegation_depth.child(&requested.delegation_depth)?,
        })
    }
}
