use crate::error::{StorageError, StorageResult};
use chrono::{DateTime, Utc};
use elephant_types::ids::TableName;
use elephant_types::records::base::{BaseFields, Provenance, Security};
use elephant_types::records::{
    AuditLog, Doc, Entity, EntityIdentityKey, EntityMerge, IdentityCandidate, Rel, RelEvidence,
    Source, SpaceConfig,
};
use elephant_types::{Link, RecordId};
use num_traits::ToPrimitive;
use serde::Serialize;
use std::collections::BTreeMap;
use surrealdb::sql::{Array, Datetime, Id, Object, Thing, Value};

/// Converts an Elephant `RecordId` into a `SurrealDB` `Thing`.
#[must_use]
pub fn thing_from_record_id(id: &RecordId) -> Thing {
    let tb = id.table().as_str().to_string();
    // Use native Uuid variant if available, or simple string for relationships
    let id_part = match id.table() {
        TableName::Rel => Id::from(id.id().simple().to_string()),
        _ => Id::Uuid(id.id().into()), // Use explicit variant to avoid From ambiguity
    };
    Thing::from((tb, id_part))
}

/// Converts a `SurrealDB` `Thing` back into an Elephant `RecordId`.
///
/// # Errors
///
/// Returns a `TypeError` if the table name is invalid or the ID part is not a valid UUID.
pub fn record_id_from_thing(thing: &Thing) -> Result<RecordId, elephant_types::error::TypeError> {
    use std::str::FromStr;
    let table = if thing.tb.starts_with("emb__") {
        TableName::ChunkEmbedding
    } else {
        TableName::from_str(&thing.tb)?
    };

    match &thing.id {
        Id::Uuid(u) => {
            // Convert surrealdb::sql::Uuid to uuid::Uuid via bytes
            let uuid = uuid::Uuid::from_bytes(u.as_bytes().to_owned());
            Ok(RecordId::new(table, uuid))
        }
        Id::String(s) => {
            // Still handle string fallback (e.g. for relationships or older persisted data)
            let id_part = s
                .strip_prefix('⟨')
                .and_then(|inner| inner.strip_suffix('⟩'))
                .unwrap_or(s);
            let id_part = id_part
                .strip_prefix("u'")
                .and_then(|inner| inner.strip_suffix('\''))
                .unwrap_or(id_part);
            let id = uuid::Uuid::parse_str(id_part)?;
            Ok(RecordId::new(table, id))
        }
        _ => {
            // Fallback for Number or other types by parsing the string representation
            let id = uuid::Uuid::parse_str(&thing.id.to_string())?;
            Ok(RecordId::new(table, id))
        }
    }
}

/// Converts a `chrono::DateTime<Utc>` into a `SurrealDB` `Datetime`.
#[must_use]
pub fn datetime_from_chrono(dt: DateTime<Utc>) -> Datetime {
    Datetime::from(dt)
}

/// Converts a collection of `RecordId`s into a `SurrealDB` `Array`.
#[must_use]
pub fn things_from_record_ids(ids: impl IntoIterator<Item = RecordId>) -> Array {
    Array::from(
        ids.into_iter()
            .map(|id| Value::from(thing_from_record_id(&id)))
            .collect::<Vec<Value>>(),
    )
}

/// Converts a collection of `Link`s into a `SurrealDB` `Array`.
#[must_use]
pub fn things_from_links<T>(links: impl IntoIterator<Item = Link<T>>) -> Array {
    Array::from(
        links
            .into_iter()
            .map(|l| Value::from(thing_from_record_id(l.id())))
            .collect::<Vec<Value>>(),
    )
}

/// Helper to convert a Security struct into a native Surreal Value (Object).
pub(crate) fn security_to_value(s: &Security) -> Value {
    let mut map: BTreeMap<String, Value> = BTreeMap::new();
    map.insert("policy_id".to_string(), s.policy_id.clone().into());
    map.insert("level".to_string(), s.level.into());
    map.insert(
        "labels".to_string(),
        Value::Array(Array::from(s.labels.clone())),
    );
    map.insert(
        "handling".to_string(),
        Value::Array(Array::from(s.handling.clone())),
    );
    Value::Object(Object::from(map))
}

/// Helper to convert a `serde_json::Value` to a `SurrealDB` native Value.
pub fn json_to_surreal(val: serde_json::Value) -> StorageResult<Value> {
    json_to_surreal_checked(val, "json_to_surreal")
}

fn json_to_surreal_fallible<T: Serialize>(value: &T, context: &str) -> StorageResult<Value> {
    value
        .serialize(FiniteCheckSerializer { context })
        .map_err(|e| StorageError::SerializationError(e.to_string()))?;
    let json = serde_json::to_value(value).map_err(|e| {
        StorageError::SerializationError(format!("Failed to serialize {context}: {e}"))
    })?;
    json_to_surreal_checked(json, context)
}

#[derive(Debug)]
struct FiniteCheckError(String);

impl std::fmt::Display for FiniteCheckError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FiniteCheckError {}

impl serde::ser::Error for FiniteCheckError {
    fn custom<T: std::fmt::Display>(msg: T) -> Self {
        Self(msg.to_string())
    }
}

struct FiniteCheckSerializer<'a> {
    context: &'a str,
}

struct FiniteCheckCompound<'a> {
    context: &'a str,
}

impl serde::ser::SerializeSeq for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl serde::ser::SerializeTuple for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl serde::ser::SerializeTupleStruct for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl serde::ser::SerializeTupleVariant for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl serde::ser::SerializeMap for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), Self::Error> {
        key.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl serde::ser::SerializeStruct for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl serde::ser::SerializeStructVariant for FiniteCheckCompound<'_> {
    type Ok = ();
    type Error = FiniteCheckError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        _key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error> {
        value.serialize(FiniteCheckSerializer {
            context: self.context,
        })?;
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> serde::Serializer for FiniteCheckSerializer<'a> {
    type Ok = ();
    type Error = FiniteCheckError;

    type SerializeSeq = FiniteCheckCompound<'a>;
    type SerializeTuple = FiniteCheckCompound<'a>;
    type SerializeTupleStruct = FiniteCheckCompound<'a>;
    type SerializeTupleVariant = FiniteCheckCompound<'a>;
    type SerializeMap = FiniteCheckCompound<'a>;
    type SerializeStruct = FiniteCheckCompound<'a>;
    type SerializeStructVariant = FiniteCheckCompound<'a>;

    fn serialize_bool(self, _value: bool) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i8(self, _value: i8) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i16(self, _value: i16) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i32(self, _value: i32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i64(self, _value: i64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_i128(self, _value: i128) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u8(self, _value: u8) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u16(self, _value: u16) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u32(self, _value: u32) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u64(self, _value: u64) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_u128(self, _value: u128) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<Self::Ok, Self::Error> {
        if value.is_finite() {
            Ok(())
        } else {
            Err(FiniteCheckError(format!(
                "Non-finite number in {}",
                self.context
            )))
        }
    }

    fn serialize_f64(self, value: f64) -> Result<Self::Ok, Self::Error> {
        if value.is_finite() {
            Ok(())
        } else {
            Err(FiniteCheckError(format!(
                "Non-finite number in {}",
                self.context
            )))
        }
    }

    fn serialize_char(self, _value: char) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_str(self, _value: &str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_bytes(self, _value: &[u8]) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }

    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error> {
        value.serialize(self)
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        Ok(FiniteCheckCompound {
            context: self.context,
        })
    }
}

fn json_to_surreal_checked(val: serde_json::Value, context: &str) -> StorageResult<Value> {
    match val {
        serde_json::Value::Null => Ok(Value::None),
        serde_json::Value::Bool(b) => Ok(Value::from(b)),
        serde_json::Value::Number(n) => number_to_surreal_checked(&n, context),
        serde_json::Value::String(s) => Ok(Value::from(s)),
        serde_json::Value::Array(arr) => {
            let mut values = Vec::with_capacity(arr.len());
            for entry in arr {
                values.push(json_to_surreal_checked(entry, context)?);
            }
            Ok(Value::Array(Array::from(values)))
        }
        serde_json::Value::Object(obj) => {
            let mut map: BTreeMap<String, Value> = BTreeMap::new();
            for (k, v) in obj {
                map.insert(k, json_to_surreal_checked(v, context)?);
            }
            Ok(Value::Object(Object::from(map)))
        }
    }
}

#[allow(clippy::option_if_let_else)]
fn number_to_surreal_checked(n: &serde_json::Number, context: &str) -> StorageResult<Value> {
    if let Some(i) = n.as_i64() {
        Ok(Value::from(i))
    } else if let Some(u) = n.as_u64() {
        Ok(Value::from(u))
    } else if let Some(f) = n.as_f64() {
        if f.is_finite() {
            Ok(Value::from(f))
        } else {
            Err(StorageError::SerializationError(format!(
                "Non-finite number in {context}"
            )))
        }
    } else {
        Err(StorageError::SerializationError(format!(
            "Unsupported number in {context}"
        )))
    }
}

/// Helper to convert a `SurrealDB` native Value back to `serde_json::Value`.
pub fn surreal_to_json(val: Value) -> StorageResult<serde_json::Value> {
    match val {
        Value::None | Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(b)),
        Value::Number(n) => match n {
            surrealdb::sql::Number::Int(i) => {
                Ok(serde_json::Value::Number(serde_json::Number::from(i)))
            }
            surrealdb::sql::Number::Float(f) => float_to_json_value(f, "surreal_to_json.float"),
            surrealdb::sql::Number::Decimal(d) => {
                let f = d.to_f64().ok_or_else(|| {
                    StorageError::SerializationError(
                        "surreal_to_json.decimal: non-representable decimal".to_string(),
                    )
                })?;
                float_to_json_value(f, "surreal_to_json.decimal")
            }
            _ => Err(StorageError::SerializationError(
                "surreal_to_json.number: unsupported number variant".to_string(),
            )),
        },
        Value::Strand(s) => Ok(serde_json::Value::String(s.to_string())),
        Value::Uuid(u) => Ok(serde_json::Value::String(u.to_string())),
        Value::Array(arr) => {
            let mut values = Vec::with_capacity(arr.len());
            for entry in arr {
                values.push(surreal_to_json(entry)?);
            }
            Ok(serde_json::Value::Array(values))
        }
        Value::Object(obj) => {
            // Check if this is a SurrealDB unit enum: { VariantName: None } or { VariantName: Null }
            if obj.len() == 1 {
                if let Some(key) = obj.keys().next() {
                    if let Some(val) = obj.get(key) {
                        if matches!(val, Value::None | Value::Null) {
                            return Ok(serde_json::Value::String(key.clone()));
                        }
                    }
                }
            }

            let mut map = serde_json::Map::new();
            for (k, v) in obj {
                let key = k.clone();
                // Special case for "type" field which is a reserved word in Rust
                let mapped_key = if key == "type" {
                    "entity_type".to_string()
                } else {
                    key
                };
                map.insert(mapped_key, surreal_to_json(v)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::Thing(t) => Ok(serde_json::Value::String(t.to_string())),
        Value::Datetime(dt) => Ok(serde_json::Value::String(dt.to_string())),
        other => Err(StorageError::SerializationError(format!(
            "surreal_to_json: unsupported value {other:?}"
        ))),
    }
}

fn float_to_json_value(f: f64, context: &str) -> StorageResult<serde_json::Value> {
    if !f.is_finite() {
        return Err(StorageError::SerializationError(format!(
            "Non-finite number in {context}"
        )));
    }
    if (f - f.floor()).abs() < f64::EPSILON {
        #[allow(clippy::cast_possible_truncation)]
        return Ok(serde_json::Value::Number(serde_json::Number::from(
            f as i64,
        )));
    }
    serde_json::Number::from_f64(f)
        .map(serde_json::Value::Number)
        .ok_or_else(|| StorageError::SerializationError(format!("Invalid float in {context}")))
}

/// Robustly deserializes a domain record from a `SurrealDB` result.
/// Bridges the gap between native driver tokens and domain serde expectations.
///
/// # Errors
///
/// Returns a `TypeError` if deserialization fails.
pub fn robust_deserialize<T>(
    id: Thing,
    extra: Object,
) -> Result<T, elephant_types::error::TypeError>
where
    T: serde::de::DeserializeOwned,
{
    let mut obj = extra;
    obj.insert("id".to_string(), id.into());
    let json = surreal_to_json(Value::Object(obj)).map_err(|e| {
        elephant_types::error::TypeError::InvalidEnumVariant {
            field: "serialization".to_string(),
            value: e.to_string(),
        }
    })?;
    serde_json::from_value(json).map_err(|e| elephant_types::error::TypeError::InvalidEnumVariant {
        field: "serialization".to_string(),
        value: e.to_string(),
    })
}

/// Helper to convert an enum to a `SurrealDB` native string Value.
pub fn enum_to_surreal<T: serde::Serialize>(val: &T) -> StorageResult<Value> {
    let s =
        serde_json::to_string(val).map_err(|e| StorageError::SerializationError(e.to_string()))?;
    Ok(Value::from(s.trim_matches('"').to_string()))
}

/// Helper to convert a `Provenance` struct into a native Surreal Value (Object).
pub(crate) fn provenance_to_value(p: &Provenance) -> StorageResult<Value> {
    let mut map: BTreeMap<String, Value> = BTreeMap::new();
    map.insert(
        "sources".to_string(),
        Value::Array(things_from_record_ids(p.sources.clone())),
    );

    map.insert(
        "extractor".to_string(),
        match p.extractor.as_ref() {
            Some(ext) => json_to_surreal_fallible(ext, "provenance.extractor")?,
            None => Value::None,
        },
    );
    map.insert(
        "run_id".to_string(),
        p.run_id
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    map.insert(
        "work_run_id".to_string(),
        p.work_run_id
            .as_ref()
            .map_or(Value::None, |id| Value::from(id.to_string())),
    );
    map.insert(
        "prompt_id".to_string(),
        p.prompt_id
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    map.insert(
        "config_id".to_string(),
        p.config_id
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    map.insert(
        "chain".to_string(),
        json_to_surreal_fallible(&p.chain, "provenance.chain")?,
    );

    Ok(Value::Object(Object::from(map)))
}

/// Constructs a Surreal Object with the common `BaseFields`.
pub(crate) fn base_fields_to_object(base: &BaseFields) -> StorageResult<Object> {
    let mut map: BTreeMap<String, Value> = BTreeMap::new();
    map.insert("space_id".to_string(), base.space_id.clone().into());
    map.insert(
        "created_at".to_string(),
        datetime_from_chrono(base.created_at).into(),
    );
    map.insert(
        "updated_at".to_string(),
        datetime_from_chrono(base.updated_at).into(),
    );
    map.insert("created_by".to_string(), base.created_by.clone().into());
    map.insert(
        "deleted_at".to_string(),
        base.deleted_at
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );

    map.insert("security".to_string(), security_to_value(&base.security));
    map.insert(
        "provenance".to_string(),
        provenance_to_value(&base.provenance)?,
    );
    map.insert(
        "subjects".to_string(),
        Value::Array(things_from_links(base.subjects.clone())),
    );
    map.insert(
        "subjects_state".to_string(),
        enum_to_surreal(&base.subjects_state)?,
    );

    Ok(Object::from(map))
}

/// Converts an `Entity` record into a `SurrealDB` `Value`.
pub fn entity_to_value(r: &Entity) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("type".to_string(), enum_to_surreal(&r.entity_type)?);
    obj.insert("display_name".to_string(), r.display_name.clone().into());
    obj.insert(
        "aliases".to_string(),
        Value::Array(Array::from(r.aliases.clone())),
    );
    obj.insert(
        "summary".to_string(),
        r.summary
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "handles".to_string(),
        json_to_surreal_fallible(&r.handles, "entity.handles")?,
    );
    obj.insert(
        "identity_keys".to_string(),
        Value::Array(Array::from(r.identity_keys.clone())),
    );
    obj.insert(
        "risk_level".to_string(),
        r.risk_level.map_or(Value::None, Value::from),
    );
    obj.insert(
        "trust_level".to_string(),
        r.trust_level.map_or(Value::None, Value::from),
    );
    obj.insert("known_safe".to_string(), r.known_safe.into());
    obj.insert(
        "first_seen".to_string(),
        r.first_seen
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );
    obj.insert(
        "last_seen".to_string(),
        r.last_seen
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );
    obj.insert("status".to_string(), enum_to_surreal(&r.status)?);
    obj.insert(
        "redirect_to".to_string(),
        r.redirect_to
            .as_ref()
            .map_or(Value::None, |l| Value::from(thing_from_record_id(l.id()))),
    );
    obj.insert("metadata".to_string(), json_to_surreal(r.metadata.clone())?);
    Ok(Value::Object(obj))
}

/// Converts a `Rel` record into a `SurrealDB` `Value`.
///
/// # Errors
///
/// Returns `StorageError::SerializationError` if evidence or temporal audit
/// data fails to serialize.
pub fn rel_to_value(r: &Rel) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("rel_type".to_string(), r.rel_type.clone().into());
    obj.insert("status".to_string(), r.status.to_string().into());
    obj.insert("confidence".to_string(), Value::from(r.confidence));
    obj.insert(
        "validity_window".to_string(),
        r.validity_window.as_ref().map_or(Ok(Value::None), |vw| {
            json_to_surreal(serde_json::json!({
                "start": vw.start,
                "end": vw.end,
            }))
        })?,
    );
    obj.insert(
        "evidence".to_string(),
        json_to_surreal_fallible(&r.evidence, "rel.evidence")?,
    );
    obj.insert(
        "temporal_audit".to_string(),
        r.temporal_audit.as_ref().map_or(Ok(Value::None), |ta| {
            json_to_surreal_fallible(ta, "rel.temporal_audit")
        })?,
    );
    Ok(Value::Object(obj))
}

/// Converts a `Doc` record into a `SurrealDB` `Value`.
pub fn doc_to_value(r: &Doc) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("title".to_string(), r.title.clone().into());
    obj.insert("text".to_string(), r.text.clone().into());
    obj.insert(
        "sidecar_text".to_string(),
        r.sidecar_text
            .as_ref()
            .map_or(Value::None, |value| Value::from(value.clone())),
    );
    obj.insert(
        "source".to_string(),
        r.source
            .as_ref()
            .map_or(Value::None, |l| Value::from(thing_from_record_id(l.id()))),
    );
    obj.insert(
        "media_attachments".to_string(),
        json_to_surreal_fallible(&r.media_attachments, "doc.media_attachments")?,
    );
    obj.insert("rendition_key".to_string(), r.rendition_key.clone().into());
    obj.insert("metadata".to_string(), json_to_surreal(r.metadata.clone())?);
    Ok(Value::Object(obj))
}

/// Converts a `Source` record into a `SurrealDB` `Value`.
pub fn source_to_value(r: &Source) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("type".to_string(), enum_to_surreal(&r.type_)?);
    obj.insert(
        "lifecycle_status".to_string(),
        enum_to_surreal(&r.lifecycle_status)?,
    );
    obj.insert(
        "current_ingest_mode".to_string(),
        match r.current_ingest_mode.as_ref() {
            Some(mode) => enum_to_surreal(mode)?,
            None => Value::None,
        },
    );
    obj.insert(
        "latest_ingest_decision".to_string(),
        r.latest_ingest_decision
            .as_ref()
            .map_or(Value::None, |l| Value::from(thing_from_record_id(l.id()))),
    );
    obj.insert(
        "uri".to_string(),
        r.uri
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "path".to_string(),
        r.path
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "external_id".to_string(),
        r.external_id
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert("hash".to_string(), r.hash.clone().into());
    obj.insert(
        "author".to_string(),
        r.author
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "channel".to_string(),
        r.channel
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "timestamp".to_string(),
        r.timestamp
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );
    obj.insert(
        "parent".to_string(),
        r.parent
            .as_ref()
            .map_or(Value::None, |l| Value::from(thing_from_record_id(l.id()))),
    );
    obj.insert(
        "range".to_string(),
        match r.range.as_ref() {
            Some(range) => json_to_surreal_fallible(range, "source.range")?,
            None => Value::None,
        },
    );
    obj.insert("metadata".to_string(), json_to_surreal(r.metadata.clone())?);
    Ok(Value::Object(obj))
}

/// Converts an `AuditLog` record into a `SurrealDB` `Value`.
pub fn audit_log_to_value(r: &AuditLog) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("action".to_string(), enum_to_surreal(&r.action)?);
    obj.insert("actor".to_string(), r.actor.clone().into());
    obj.insert("record".to_string(), thing_from_record_id(&r.record).into());
    obj.insert(
        "before".to_string(),
        match r.before.as_ref() {
            Some(v) => json_to_surreal(v.clone())?,
            None => Value::None,
        },
    );
    obj.insert(
        "after".to_string(),
        match r.after.as_ref() {
            Some(v) => json_to_surreal(v.clone())?,
            None => Value::None,
        },
    );
    obj.insert(
        "timestamp".to_string(),
        datetime_from_chrono(r.timestamp).into(),
    );
    obj.insert(
        "reason".to_string(),
        r.reason
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    Ok(Value::Object(obj))
}

/// Converts an `EntityIdentityKey` record into a `SurrealDB` `Value`.
pub fn entity_identity_key_to_value(r: &EntityIdentityKey) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("key".to_string(), r.key.clone().into());
    obj.insert(
        "entity".to_string(),
        thing_from_record_id(r.entity.id()).into(),
    );
    obj.insert("confidence".to_string(), r.confidence.into());
    obj.insert(
        "first_seen".to_string(),
        r.first_seen
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );
    obj.insert(
        "last_seen".to_string(),
        r.last_seen
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );
    obj.insert(
        "evidence".to_string(),
        Value::Array(things_from_links(r.evidence.clone())),
    );
    obj.insert("status".to_string(), enum_to_surreal(&r.status)?);
    Ok(Value::Object(obj))
}

/// Converts an `IdentityCandidate` record into a `SurrealDB` `Value`.
pub fn identity_candidate_to_value(r: &IdentityCandidate) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert(
        "entity_a".to_string(),
        thing_from_record_id(r.entity_a.id()).into(),
    );
    obj.insert(
        "entity_b".to_string(),
        thing_from_record_id(r.entity_b.id()).into(),
    );
    obj.insert("signals".to_string(), json_to_surreal(r.signals.clone())?);
    obj.insert("confidence".to_string(), r.confidence.into());
    obj.insert("status".to_string(), enum_to_surreal(&r.status)?);
    obj.insert("proposed_by".to_string(), r.proposed_by.clone().into());
    obj.insert(
        "decided_by".to_string(),
        r.decided_by
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "decided_at".to_string(),
        r.decided_at
            .map_or(Value::None, |dt| Value::from(datetime_from_chrono(dt))),
    );
    Ok(Value::Object(obj))
}

/// Converts an `EntityMerge` record into a `SurrealDB` `Value`.
pub fn entity_merge_to_value(r: &EntityMerge) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("from".to_string(), thing_from_record_id(r.from.id()).into());
    obj.insert("into".to_string(), thing_from_record_id(r.into.id()).into());
    obj.insert("strategy".to_string(), enum_to_surreal(&r.strategy)?);
    obj.insert("decided_by".to_string(), r.decided_by.clone().into());
    obj.insert(
        "decided_at".to_string(),
        datetime_from_chrono(r.decided_at).into(),
    );
    obj.insert(
        "run_id".to_string(),
        r.run_id
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    Ok(Value::Object(obj))
}

/// Shorthand macro for creating `SurrealDB` query bindings.
#[macro_export]
macro_rules! bindings {
    ($($key:expr => $value:expr),* $(,)?) => {{
        let mut map = std::collections::BTreeMap::<String, surrealdb::sql::Value>::new();
        $(
            map.insert($key.to_string(), $value.into());
        )*
        map
    }};
}

/// Converts a `RelEvidence` record into a `SurrealDB` `Value`.
pub fn rel_evidence_to_value(r: &RelEvidence) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert("rel".to_string(), thing_from_record_id(r.rel.id()).into());
    obj.insert("fact".to_string(), thing_from_record_id(r.fact.id()).into());
    obj.insert(
        "source".to_string(),
        thing_from_record_id(r.source.id()).into(),
    );
    obj.insert("confidence".to_string(), r.confidence.into());
    obj.insert(
        "observed_at".to_string(),
        datetime_from_chrono(r.observed_at).into(),
    );
    obj.insert(
        "run_id".to_string(),
        r.run_id
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    Ok(Value::Object(obj))
}

/// Converts a `SpaceConfig` record into a `SurrealDB` `Value`.
pub fn space_config_to_value(r: &SpaceConfig) -> StorageResult<Value> {
    let mut obj = base_fields_to_object(&r.base)?;
    obj.insert(
        "embedding_provider".to_string(),
        Value::from(r.embedding_provider.clone()),
    );
    obj.insert(
        "embedding_model".to_string(),
        Value::from(r.embedding_model.clone()),
    );
    obj.insert("embedding_dims".to_string(), Value::from(r.embedding_dims));
    obj.insert(
        "purpose".to_string(),
        r.purpose
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    obj.insert(
        "skill".to_string(),
        r.skill
            .as_ref()
            .map_or(Value::None, |s| Value::from(s.clone())),
    );
    Ok(Value::Object(obj))
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        clippy::mixed_attributes_style,
        clippy::similar_names,
        clippy::type_complexity,
        clippy::float_cmp,
        clippy::const_is_empty,
        clippy::expect_used,
        clippy::panic,
        unused_imports,
        dead_code,
        clippy::too_many_lines,
        clippy::cognitive_complexity
    )]
    use super::*;

    fn normalize_thing_id_part(s: &str) -> &str {
        let normalized = s
            .strip_prefix('⟨')
            .and_then(|inner| inner.strip_suffix('⟩'))
            .unwrap_or(s);
        normalized
            .strip_prefix("u'")
            .and_then(|inner| inner.strip_suffix('\''))
            .unwrap_or(normalized)
    }

    #[derive(Serialize)]
    struct BadFloatPayload {
        value: f64,
    }

    #[test]
    fn json_to_surreal_fallible_rejects_non_finite_numbers() {
        let payload = BadFloatPayload { value: f64::NAN };
        let err = json_to_surreal_fallible(&payload, "bad").unwrap_err();
        assert!(matches!(err, StorageError::SerializationError(_)));
    }

    #[test]
    fn thing_from_record_id_preserves_entity_uuid_format() {
        let id = RecordId::new_v4(TableName::Entity);
        let thing = thing_from_record_id(&id);
        let thing_str = thing.to_string();
        assert!(thing_str.starts_with("entity:"));

        let thing_id_part = thing_str.strip_prefix("entity:").expect("entity prefix");
        let thing_id_part = normalize_thing_id_part(thing_id_part);
        assert_eq!(thing_id_part, id.id().to_string());
        assert!(thing_id_part.contains('-'));
    }

    #[test]
    fn thing_from_record_id_normalizes_rel_uuid_format() {
        let id = RecordId::new_v4(TableName::Rel);
        let thing = thing_from_record_id(&id);

        let expected = format!("rel:{}", id.id().simple());
        assert_eq!(thing.to_string(), expected);
        assert!(!thing.to_string().contains('-'));
    }
}
