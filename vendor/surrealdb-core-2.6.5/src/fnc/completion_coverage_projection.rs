//! Exact non-authoritative comparison of completion payloads for index invalidation.
use serde::de::{MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::value::RawValue;
use std::collections::BTreeMap;

const FIELD: &str = "historical_embedding_coverage";
const MAX_BYTES: usize = 16 * 1024 * 1024;

// Validation is recursive even where later comparison uses borrowed raw bytes.
// This refuses duplicate nested keys rather than normalizing malformed JSON.
struct Strict;
impl<'de> Deserialize<'de> for Strict {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = Strict;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("JSON without duplicate keys")
            }
            fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Strict, E> {
                Ok(Strict)
            }
            fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Strict, E> {
                Ok(Strict)
            }
            fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Strict, E> {
                Ok(Strict)
            }
            fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Strict, E> {
                Ok(Strict)
            }
            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Strict, E> {
                Ok(Strict)
            }
            fn visit_unit<E: serde::de::Error>(self) -> Result<Strict, E> {
                Ok(Strict)
            }
            fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<Strict, S::Error> {
                while seq.next_element::<Strict>()?.is_some() {}
                Ok(Strict)
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Strict, M::Error> {
                let mut keys = std::collections::BTreeSet::new();
                while let Some(key) = map.next_key::<String>()? {
                    if !keys.insert(key) {
                        return Err(serde::de::Error::custom("duplicate completion key"));
                    }
                    map.next_value::<Strict>()?;
                }
                Ok(Strict)
            }
        }
        d.deserialize_any(StrictVisitor)
    }
}

struct Fields<'a>(BTreeMap<String, &'a RawValue>);
impl<'de> Deserialize<'de> for Fields<'de> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldsVisitor;
        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = Fields<'de>;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a completion object with unique top-level fields")
            }
            fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<Self::Value, M::Error> {
                let mut fields = BTreeMap::new();
                while let Some((key, value)) = map.next_entry::<String, &'de RawValue>()? {
                    if fields.insert(key, value).is_some() {
                        return Err(serde::de::Error::custom("duplicate completion field"));
                    }
                    if fields.len() > 64 {
                        return Err(serde::de::Error::custom("completion field bound"));
                    }
                }
                Ok(Fields(fields))
            }
        }
        deserializer.deserialize_map(FieldsVisitor)
    }
}

/// Compare retained payload fields byte-for-byte, excluding only coverage evidence.
/// This is an invalidation predicate, never authority to read or write a completion.
/// Duplicate keys, malformed envelopes and bounds cannot claim equivalence.
pub(crate) fn coverage_only(before: &str, after: &str) -> bool {
    if before.len() > MAX_BYTES || after.len() > MAX_BYTES {
        return false;
    }
    if serde_json::from_str::<Strict>(before).is_err()
        || serde_json::from_str::<Strict>(after).is_err()
    {
        return false;
    }
    let (Ok(Fields(mut before)), Ok(Fields(mut after))) =
        (serde_json::from_str(before), serde_json::from_str(after))
    else {
        return false;
    };
    for key in [
        "space_id",
        "publication",
        "manifest_digest",
        "commit_id",
        "completed_at",
    ] {
        if !before.contains_key(key) || !after.contains_key(key) {
            return false;
        }
    }
    let Some(coverage) = after.remove(FIELD) else {
        return false;
    };
    if !coverage.get().starts_with('{') {
        return false;
    }
    before.remove(FIELD);
    before.len() == after.len()
        && before.iter().all(|(key, value)| {
            after
                .get(key)
                .is_some_and(|other| value.get() == other.get())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_coverage_projection_refuses_original_changes_and_malformed_payloads() {
        let before = r#"{"space_id":"s","publication":{"generation":"1"},"manifest_digest":"m","commit_id":"c","completed_at":"t","historical_embedding_coverage":null}"#;
        let after = before.replace("null}", r#"{"reindex_id":"r"}}"#);
        assert!(coverage_only(before, &after));
        assert!(coverage_only(&after, &after));
        for changed in [
            after.replace(r#""commit_id":"c""#, r#""commit_id":"d""#),
            after.replace(r#""generation":"1""#, r#""generation":"2""#),
            after.replace(r#""space_id":"s""#, r#""space_id":"other""#),
            after.replace(r#""manifest_digest":"m""#, r#""manifest_digest":1"#),
            after.replace(
                r#""reindex_id":"r""#,
                r#""reindex_id":"r","reindex_id":"r""#,
            ),
            after.replace(
                r#""generation":"1""#,
                r#""generation":"1","generation":"1""#,
            ),
            after.replace(r#""commit_id":"c""#, r#""commit_id":"c","commit_id":"c""#),
            "[]".into(),
            "{".into(),
        ] {
            assert!(!coverage_only(before, &changed));
        }
        assert!(!coverage_only(&after, before));
        assert!(!coverage_only(
            r#"{"x":1}"#,
            r#"{"x":1,"historical_embedding_coverage":{}}"#
        ));
    }
}
