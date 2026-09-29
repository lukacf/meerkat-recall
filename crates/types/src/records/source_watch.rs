//! Durable refresh policy and observation receipts for remote sources.

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::TypeError;
use crate::{Link, RecordId};

use super::base::BaseFields;
use super::source::Source;

/// Operational state of a source watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceWatchStatus {
    /// Periodic checks are enabled.
    Enabled,
    /// Checks were deliberately paused.
    Paused,
    /// The most recent check failed and will be retried.
    Error,
    /// The source kind or representation cannot be watched safely.
    Unsupported,
}

impl fmt::Display for SourceWatchStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Enabled => formatter.write_str("enabled"),
            Self::Paused => formatter.write_str("paused"),
            Self::Error => formatter.write_str("error"),
            Self::Unsupported => formatter.write_str("unsupported"),
        }
    }
}

impl FromStr for SourceWatchStatus {
    type Err = TypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "enabled" => Ok(Self::Enabled),
            "paused" => Ok(Self::Paused),
            "error" => Ok(Self::Error),
            "unsupported" => Ok(Self::Unsupported),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "source_watch_status".to_string(),
                value: value.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for SourceWatchStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_string_enum(deserializer, "SourceWatchStatus", Self::from_str)
    }
}

/// Outcome recorded by one immutable source-watch check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceWatchOutcome {
    /// Remote validators confirmed that content did not change.
    NotModified,
    /// Content bytes matched the previously observed hash.
    HashUnchanged,
    /// Content changed and a new source ingest was staged.
    Changed,
    /// The check failed before a conclusive observation.
    Failed,
    /// The source cannot currently be refreshed by this watcher.
    Unsupported,
}

impl fmt::Display for SourceWatchOutcome {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotModified => formatter.write_str("not_modified"),
            Self::HashUnchanged => formatter.write_str("hash_unchanged"),
            Self::Changed => formatter.write_str("changed"),
            Self::Failed => formatter.write_str("failed"),
            Self::Unsupported => formatter.write_str("unsupported"),
        }
    }
}

impl FromStr for SourceWatchOutcome {
    type Err = TypeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "not_modified" => Ok(Self::NotModified),
            "hash_unchanged" => Ok(Self::HashUnchanged),
            "changed" => Ok(Self::Changed),
            "failed" => Ok(Self::Failed),
            "unsupported" => Ok(Self::Unsupported),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "source_watch_outcome".to_string(),
                value: value.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for SourceWatchOutcome {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserialize_string_enum(deserializer, "SourceWatchOutcome", Self::from_str)
    }
}

fn deserialize_string_enum<'de, D, T, F>(
    deserializer: D,
    name: &'static str,
    parse: F,
) -> Result<T, D::Error>
where
    D: serde::Deserializer<'de>,
    F: Fn(&str) -> Result<T, TypeError> + Copy,
{
    use serde::de::{self, VariantAccess, Visitor};

    struct EnumVisitor<T, F> {
        name: &'static str,
        parse: F,
        marker: std::marker::PhantomData<T>,
    }

    impl<'de, T, F> Visitor<'de> for EnumVisitor<T, F>
    where
        F: Fn(&str) -> Result<T, TypeError> + Copy,
    {
        type Value = T;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            write!(formatter, "a string or native enum for {}", self.name)
        }

        fn visit_str<E>(self, value: &str) -> Result<T, E>
        where
            E: de::Error,
        {
            (self.parse)(value).map_err(de::Error::custom)
        }

        fn visit_enum<A>(self, data: A) -> Result<T, A::Error>
        where
            A: de::EnumAccess<'de>,
        {
            let (variant, visitor): (String, A::Variant) = data.variant()?;
            if let Ok(value) = visitor.newtype_variant::<String>() {
                return (self.parse)(&value).map_err(de::Error::custom);
            }
            (self.parse)(&variant).map_err(de::Error::custom)
        }
    }

    deserializer.deserialize_any(EnumVisitor {
        name,
        parse,
        marker: std::marker::PhantomData,
    })
}

/// Mutable schedule and latest observation state for one source.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceWatch {
    /// Shared record fields. Security must match the watched source.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Source whose remote representation is checked.
    pub source: Link<Source>,
    /// Canonical requested URI. Redirect destinations are observation data.
    pub requested_uri: String,
    /// Current operational state.
    pub status: SourceWatchStatus,
    /// Normal interval between successful checks.
    pub interval_seconds: i64,
    /// Earliest time at which the next check should be scheduled.
    pub next_check_at: DateTime<Utc>,
    /// Most recent completed check, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_checked_at: Option<DateTime<Utc>>,
    /// Most recent conclusive content change, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_changed_at: Option<DateTime<Utc>>,
    /// Current remote HTTP entity tag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Current remote HTTP Last-Modified value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    /// Hash of the latest successfully observed representation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_hash: Option<String>,
    /// Consecutive check failures since the last successful observation.
    #[serde(default)]
    pub consecutive_failures: u32,
    /// Most recent operational error. This is never treated as source content.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Latest immutable observation receipt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_check: Option<RecordId>,
}

/// Immutable receipt for one source-watch attempt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceWatchCheck {
    /// Shared record fields copied from the watch security envelope.
    #[serde(flatten)]
    pub base: BaseFields,
    /// Watch that produced this receipt.
    pub watch: Link<SourceWatch>,
    /// Source observed by this receipt.
    pub source: Link<Source>,
    /// Conclusive outcome.
    pub outcome: SourceWatchOutcome,
    /// Time at which network work began.
    pub started_at: DateTime<Utc>,
    /// Time at which the receipt was finalized.
    pub completed_at: DateTime<Utc>,
    /// Requested URI.
    pub requested_uri: String,
    /// Final URI after validated redirects, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_uri: Option<String>,
    /// HTTP media type, when available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// HTTP entity tag observed by this check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// HTTP Last-Modified value observed by this check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    /// Hash before this observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prior_hash: Option<String>,
    /// Hash observed by this check.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_hash: Option<String>,
    /// Number of body bytes read, when a representation was returned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_length: Option<u64>,
    /// Failure or unsupported rationale. This is never source evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_and_outcome_roundtrip_as_strings() -> serde_json::Result<()> {
        for status in [
            SourceWatchStatus::Enabled,
            SourceWatchStatus::Paused,
            SourceWatchStatus::Error,
            SourceWatchStatus::Unsupported,
        ] {
            let encoded = serde_json::to_string(&status)?;
            let decoded: SourceWatchStatus = serde_json::from_str(&encoded)?;
            assert_eq!(decoded, status);
        }
        for outcome in [
            SourceWatchOutcome::NotModified,
            SourceWatchOutcome::HashUnchanged,
            SourceWatchOutcome::Changed,
            SourceWatchOutcome::Failed,
            SourceWatchOutcome::Unsupported,
        ] {
            let encoded = serde_json::to_string(&outcome)?;
            let decoded: SourceWatchOutcome = serde_json::from_str(&encoded)?;
            assert_eq!(decoded, outcome);
        }
        Ok(())
    }
}
