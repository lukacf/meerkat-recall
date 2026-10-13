//! Duplicate-aware bounded JSON decoding of the supported embedded export.
use super::{
    incomplete, BTreeMap, Budget, Inventory, NativeInspectionValue, SnapshotInspectionLimits,
    StorageResult, Utc,
};
use serde::de::{DeserializeSeed, Error as _, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use std::fmt;

pub(super) fn parse(bytes: &[u8], budget: &mut Budget<'_>) -> StorageResult<serde_json::Value> {
    parse_at(bytes, budget, Location::Value)
}

#[derive(Clone, Copy)]
enum Location {
    Value,
    Export,
    Tables,
    Rows,
    Row,
}

fn parse_at(
    bytes: &[u8],
    budget: &mut Budget<'_>,
    location: Location,
) -> StorageResult<serde_json::Value> {
    // Check lexical sizes before serde allocates decoded strings. Escaping can
    // shorten an exported string; conservatively bounding encoded bytes is safe.
    lexical_bounds(bytes, budget.limits)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let value = Seed {
        budget,
        depth: 0,
        location,
    }
    .deserialize(&mut decoder)
    .map_err(|_| incomplete("json_invalid_duplicate_or_limit"))?;
    decoder
        .end()
        .map_err(|_| incomplete("json_trailing_input"))?;
    Ok(value)
}

fn lexical_bounds(bytes: &[u8], limits: &SnapshotInspectionLimits) -> StorageResult<()> {
    let mut quote_start = None;
    let mut escaped = false;
    let mut depth = 0usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if let Some(start) = quote_start {
            if index - start > limits.string_bytes {
                return Err(incomplete("string_limit"));
            }
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                quote_start = None;
            }
        } else {
            match byte {
                b'"' => quote_start = Some(index + 1),
                b'{' | b'[' => {
                    depth += 1;
                    if depth > limits.depth {
                        return Err(incomplete("value_depth_limit"));
                    }
                }
                b'}' | b']' => {
                    depth = depth
                        .checked_sub(1)
                        .ok_or_else(|| incomplete("json_delimiters"))?;
                }
                _ => {}
            }
        }
    }
    if quote_start.is_some() || depth != 0 {
        return Err(incomplete("json_delimiters"));
    }
    Ok(())
}

struct Seed<'a, 'b> {
    budget: &'a mut Budget<'b>,
    depth: usize,
    location: Location,
}
impl<'de> DeserializeSeed<'de> for Seed<'_, '_> {
    type Value = serde_json::Value;
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<Self::Value, D::Error> {
        self.budget.node(self.depth).map_err(D::Error::custom)?;
        if matches!(self.location, Location::Row) {
            self.budget.parsed_rows += 1;
            if self.budget.parsed_rows > self.budget.limits.rows {
                return Err(D::Error::custom("row limit"));
            }
        }
        decoder.deserialize_any(self)
    }
}
impl<'de> Visitor<'de> for Seed<'_, '_> {
    type Value = serde_json::Value;
    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bounded unique-key JSON")
    }
    fn visit_bool<E: serde::de::Error>(self, value: bool) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<Self::Value, E> {
        Ok(value.into())
    }
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<Self::Value, E> {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .ok_or_else(|| E::custom("nonfinite JSON"))
    }
    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        self.budget.string(value).map_err(E::custom)?;
        Ok(value.into())
    }
    fn visit_string<E: serde::de::Error>(self, value: String) -> Result<Self::Value, E> {
        self.budget.string(&value).map_err(E::custom)?;
        Ok(value.into())
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }
    fn visit_none<E: serde::de::Error>(self) -> Result<Self::Value, E> {
        Ok(serde_json::Value::Null)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut values = Vec::new();
        while let Some(value) = sequence.next_element_seed(Seed {
            budget: self.budget,
            depth: self.depth + 1,
            location: if matches!(self.location, Location::Rows) {
                Location::Row
            } else {
                Location::Value
            },
        })? {
            values.push(value);
        }
        Ok(serde_json::Value::Array(values))
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut values = serde_json::Map::new();
        while let Some(key) = map.next_key::<String>()? {
            self.budget.string(&key).map_err(A::Error::custom)?;
            if values.contains_key(&key) {
                return Err(A::Error::custom("duplicate map key"));
            }
            let location = match self.location {
                Location::Export if key == "tables" => Location::Tables,
                Location::Tables => {
                    self.budget.parsed_tables += 1;
                    if self.budget.parsed_tables > self.budget.limits.tables {
                        return Err(A::Error::custom("table limit"));
                    }
                    Location::Rows
                }
                _ => Location::Value,
            };
            let value = map.next_value_seed(Seed {
                budget: self.budget,
                depth: self.depth + 1,
                location,
            })?;
            values.insert(key, value);
        }
        Ok(serde_json::Value::Object(values))
    }
}

pub(super) fn inspect(bytes: &[u8], inventory: &mut Inventory<'_>) -> StorageResult<()> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Export {
        backend: String,
        version: u32,
        exported_at: chrono::DateTime<Utc>,
        tables: BTreeMap<String, Vec<serde_json::Value>>,
    }
    let value = parse_at(bytes, &mut inventory.budget, Location::Export)?;
    let export: Export =
        serde_json::from_value(value).map_err(|_| incomplete("embedded_export_shape"))?;
    if export.backend != "elephant-db" || export.version != 1 || export.exported_at > Utc::now() {
        return Err(incomplete("embedded_export_version"));
    }
    if export.tables.len() > inventory.budget.limits.tables {
        return Err(incomplete("table_limit"));
    }
    for (table, rows) in export.tables {
        inventory.table(&table)?;
        if rows.len()
            > inventory
                .budget
                .limits
                .rows
                .saturating_sub(inventory.report.rows.len())
        {
            return Err(incomplete("row_limit"));
        }
        for row in rows {
            let identity = crate::canonical_extension::embedded_physical_identity(&table, &row)
                .map_err(|_| incomplete("embedded_identity_shape"))?;
            let record = tagged(row)?;
            // Preserve embedded JSON strings exactly. The separately decoded
            // physical identity describes storage identity without guessing that
            // an arbitrary string in a payload is a native reference.
            inventory.row(identity, record)?;
        }
    }
    Ok(())
}

fn tagged(value: serde_json::Value) -> StorageResult<NativeInspectionValue> {
    Ok(match value {
        serde_json::Value::Null => NativeInspectionValue::Null,
        serde_json::Value::Bool(value) => NativeInspectionValue::Bool(value),
        serde_json::Value::String(value) => NativeInspectionValue::String(value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                NativeInspectionValue::Int(value)
            } else if let Some(value) = value.as_u64() {
                NativeInspectionValue::Uint(value.to_string())
            } else {
                NativeInspectionValue::FloatBits(format!(
                    "{:016x}",
                    value
                        .as_f64()
                        .ok_or_else(|| incomplete("embedded_number"))?
                        .to_bits()
                ))
            }
        }
        serde_json::Value::Array(values) => NativeInspectionValue::Array(
            values
                .into_iter()
                .map(tagged)
                .collect::<StorageResult<_>>()?,
        ),
        serde_json::Value::Object(values) => NativeInspectionValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, tagged(value)?)))
                .collect::<StorageResult<_>>()?,
        ),
    })
}
