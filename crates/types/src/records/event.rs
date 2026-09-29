//! Domain event type for timeline/event records.
//!
//! These events are first-class domain records and are distinct from
//! `SystemEvent` outbox/ops records.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::error::{TypeError, TypeResult};
use crate::records::assertion::Assertion;
use crate::records::base::BaseFields;
use crate::records::source::Source;
use crate::validation::{validate_base_fields, validate_confidence, Validate};
use crate::Link;

/// Lifecycle status for a domain event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventStatus {
    /// Proposed event candidate pending promotion/review.
    #[default]
    Candidate,
    /// Approved event shown in primary timeline surfaces.
    Approved,
    /// Replaced by newer/better evidence.
    Superseded,
    /// Rejected event (low-confidence loser or invalidated by maintenance policy).
    Rejected,
}

impl std::fmt::Display for EventStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Candidate => write!(f, "candidate"),
            Self::Approved => write!(f, "approved"),
            Self::Superseded => write!(f, "superseded"),
            Self::Rejected => write!(f, "rejected"),
        }
    }
}

impl FromStr for EventStatus {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "candidate" => Ok(Self::Candidate),
            "approved" => Ok(Self::Approved),
            "superseded" => Ok(Self::Superseded),
            "rejected" => Ok(Self::Rejected),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "event_status".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for EventStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct EventStatusVisitor;

        impl<'de> Visitor<'de> for EventStatusVisitor {
            type Value = EventStatus;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for EventStatus")
            }

            fn visit_str<E>(self, value: &str) -> Result<EventStatus, E>
            where
                E: de::Error,
            {
                EventStatus::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return EventStatus::from_str(&s).map_err(de::Error::custom);
                }

                EventStatus::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(EventStatusVisitor)
    }
}

/// Precision attached to event time values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimePrecision {
    /// Exact timestamp.
    Exact,
    /// Rounded/known to minute precision.
    Minute,
    /// Rounded/known to hour precision.
    Hour,
    /// Rounded/known to day precision.
    Day,
    /// Rounded/known to month precision.
    Month,
    /// Rounded/known to year precision.
    Year,
    /// Unknown precision.
    #[default]
    Unknown,
}

impl std::fmt::Display for TimePrecision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Exact => write!(f, "exact"),
            Self::Minute => write!(f, "minute"),
            Self::Hour => write!(f, "hour"),
            Self::Day => write!(f, "day"),
            Self::Month => write!(f, "month"),
            Self::Year => write!(f, "year"),
            Self::Unknown => write!(f, "unknown"),
        }
    }
}

impl FromStr for TimePrecision {
    type Err = TypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "exact" => Ok(Self::Exact),
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "month" => Ok(Self::Month),
            "year" => Ok(Self::Year),
            "unknown" => Ok(Self::Unknown),
            _ => Err(TypeError::InvalidEnumVariant {
                field: "time_precision".to_string(),
                value: s.to_string(),
            }),
        }
    }
}

impl<'de> Deserialize<'de> for TimePrecision {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{self, Visitor};
        use std::fmt;

        struct TimePrecisionVisitor;

        impl<'de> Visitor<'de> for TimePrecisionVisitor {
            type Value = TimePrecision;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a string or native enum for TimePrecision")
            }

            fn visit_str<E>(self, value: &str) -> Result<TimePrecision, E>
            where
                E: de::Error,
            {
                TimePrecision::from_str(value).map_err(|e| de::Error::custom(e.to_string()))
            }

            fn visit_enum<A>(self, data: A) -> Result<Self::Value, A::Error>
            where
                A: de::EnumAccess<'de>,
            {
                use serde::de::VariantAccess;
                let (variant, visitor): (String, A::Variant) = data.variant()?;

                if let Ok(s) = visitor.newtype_variant::<String>() {
                    return TimePrecision::from_str(&s).map_err(de::Error::custom);
                }

                TimePrecision::from_str(&variant).map_err(de::Error::custom)
            }
        }

        deserializer.deserialize_any(TimePrecisionVisitor)
    }
}

const fn default_confidence() -> f32 {
    1.0
}

fn default_metadata() -> serde_json::Value {
    serde_json::json!({})
}

/// Domain event persisted in the `event` table.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Base fields common to all records.
    ///
    /// Event participants are stored in `base.subjects` to reuse ABAC subject
    /// inheritance and avoid duplicating link fields.
    #[serde(flatten)]
    pub base: BaseFields,

    /// Human-readable event title.
    pub title: String,

    /// Optional summary/description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,

    /// Lifecycle status.
    #[serde(default)]
    pub status: EventStatus,

    /// Event start time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_start: Option<DateTime<Utc>>,

    /// Event end time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub time_end: Option<DateTime<Utc>>,

    /// Precision attached to event time fields.
    #[serde(default)]
    pub time_precision: TimePrecision,

    /// Overall confidence in this event claim (`0.0..=1.0`).
    #[serde(default = "default_confidence")]
    pub confidence: f32,

    /// Confidence in the time values (`0.0..=1.0`).
    #[serde(default = "default_confidence")]
    pub time_confidence: f32,

    /// Evidence assertion links.
    #[serde(default)]
    pub assertion_refs: Vec<Link<Assertion>>,

    /// Evidence source links.
    #[serde(default)]
    pub source_refs: Vec<Link<Source>>,

    /// Flexible metadata payload.
    #[serde(default = "default_metadata")]
    pub metadata: serde_json::Value,
}

impl Event {
    /// Creates a new event with default status/time metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if event invariants fail.
    pub fn new(
        base: BaseFields,
        title: impl Into<String>,
        assertion_refs: Vec<Link<Assertion>>,
        source_refs: Vec<Link<Source>>,
    ) -> TypeResult<Self> {
        let item = Self {
            base,
            title: title.into(),
            summary: None,
            status: EventStatus::Candidate,
            time_start: None,
            time_end: None,
            time_precision: TimePrecision::Unknown,
            confidence: default_confidence(),
            time_confidence: default_confidence(),
            assertion_refs,
            source_refs,
            metadata: default_metadata(),
        };
        item.validate()?;
        Ok(item)
    }

    /// Validates event invariants (INV-001..INV-004).
    ///
    /// # Errors
    ///
    /// Returns an error if any invariant is violated.
    pub fn validate(&self) -> TypeResult<()> {
        validate_base_fields(&self.base)?;

        // INV-001
        if self.title.trim().is_empty() {
            return Err(TypeError::ValidationError(
                "event title cannot be empty".to_string(),
            ));
        }

        // INV-002
        validate_confidence(f64::from(self.confidence))?;
        validate_confidence(f64::from(self.time_confidence))?;

        // INV-003
        if let (Some(start), Some(end)) = (self.time_start, self.time_end) {
            if end < start {
                return Err(TypeError::ValidationError(
                    "event time_end must be >= time_start".to_string(),
                ));
            }
        }

        // INV-004
        if self.assertion_refs.is_empty() && self.source_refs.is_empty() {
            return Err(TypeError::ValidationError(
                "event must include at least one evidence reference".to_string(),
            ));
        }

        Ok(())
    }
}

impl Validate for Event {
    fn validate(&self) -> TypeResult<()> {
        Self::validate(self)
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code
    )]
    use super::*;
    use crate::ids::TableName;
    use crate::RecordId;

    fn make_valid_event() -> Event {
        let base = BaseFields::new(
            RecordId::new_v4(TableName::Event),
            "test_space".to_string(),
            "test_agent".to_string(),
        );
        let assertion_id = RecordId::new_v4(TableName::Assertion);

        let mut event = Event::new(
            base,
            "Test event",
            vec![Link::new(assertion_id)],
            Vec::new(),
        )
        .expect("valid event");
        event.time_start = Some(Utc::now());
        event.time_end = event.time_start.map(|t| t + chrono::Duration::hours(2));
        event.time_precision = TimePrecision::Hour;
        event.confidence = 0.9;
        event.time_confidence = 0.8;
        event
    }

    #[test]
    fn test_event_status_roundtrip() {
        for status in [
            EventStatus::Candidate,
            EventStatus::Approved,
            EventStatus::Superseded,
            EventStatus::Rejected,
        ] {
            let s = status.to_string();
            let parsed = EventStatus::from_str(&s).unwrap();
            assert_eq!(status, parsed);
        }
    }

    #[test]
    fn test_event_status_serde_roundtrip() {
        let status = EventStatus::Approved;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"approved\"");

        let parsed: EventStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, status);
    }

    #[test]
    fn test_time_precision_roundtrip() {
        for precision in [
            TimePrecision::Exact,
            TimePrecision::Minute,
            TimePrecision::Hour,
            TimePrecision::Day,
            TimePrecision::Month,
            TimePrecision::Year,
            TimePrecision::Unknown,
        ] {
            let s = precision.to_string();
            let parsed = TimePrecision::from_str(&s).unwrap();
            assert_eq!(precision, parsed);
        }
    }

    #[test]
    fn test_time_precision_serde_roundtrip() {
        let precision = TimePrecision::Day;
        let json = serde_json::to_string(&precision).unwrap();
        assert_eq!(json, "\"day\"");

        let parsed: TimePrecision = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, precision);
    }

    #[test]
    fn test_event_validate_ok() {
        let event = make_valid_event();
        assert!(event.validate().is_ok());
    }

    #[test]
    fn test_event_validate_rejects_empty_title() {
        let mut event = make_valid_event();
        event.title = "   ".to_string();
        assert!(event.validate().is_err());
    }

    #[test]
    fn test_event_validate_rejects_confidence_out_of_range() {
        let mut event = make_valid_event();
        event.confidence = 1.1;
        assert!(event.validate().is_err());

        let mut event = make_valid_event();
        event.time_confidence = -0.1;
        assert!(event.validate().is_err());
    }

    #[test]
    fn test_event_validate_rejects_invalid_time_range() {
        let mut event = make_valid_event();
        let now = Utc::now();
        event.time_start = Some(now);
        event.time_end = Some(now - chrono::Duration::hours(1));
        assert!(event.validate().is_err());
    }

    #[test]
    fn test_event_validate_rejects_missing_evidence() {
        let mut event = make_valid_event();
        event.assertion_refs.clear();
        event.source_refs.clear();
        assert!(event.validate().is_err());
    }
}
