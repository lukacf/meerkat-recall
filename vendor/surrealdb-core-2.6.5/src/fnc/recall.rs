//! Recall's versioned, pure erasure-reference normalizer. No database or key custody.
//! The byte format and deployment contract are described in `../RECALL_SURREALDB_PATCH.md`.

use crate::err::Error;
use crate::sql::{Array, Id, Number, Object, Thing, Value};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::BTreeSet;

/// Version of the exact typed reference encoding and HMAC domain.
pub const RECALL_REFERENCE_TOKEN_VERSION: u32 = 1;
const DOMAIN: &[u8] = b"meerkat-recall/reference/v1\0";
const MAX_DEPTH: usize = 128;
const MAX_NODES: usize = 1_000_000;
const MAX_DECODED_BYTES: usize = 64 * 1024 * 1024;

fn invalid(message: &'static str) -> Error {
	Error::InvalidArguments {
		name: "recall::reference_tokens".into(),
		message: message.into(),
	}
}

fn field(output: &mut Vec<u8>, value: &[u8]) {
	output.extend_from_slice(&(value.len() as u64).to_be_bytes());
	output.extend_from_slice(value);
}

fn depth_guard(depth: usize) -> Result<(), Error> {
	if depth > MAX_DEPTH {
		return Err(invalid("reference nesting limit exceeded"));
	}
	Ok(())
}

fn encode_id(output: &mut Vec<u8>, id: &Id, depth: usize) -> Result<(), Error> {
	depth_guard(depth)?;
	match id {
		Id::String(value) => {
			output.push(0);
			field(output, value.as_bytes());
		}
		Id::Number(value) => {
			output.push(1);
			output.extend_from_slice(&value.to_be_bytes());
		}
		Id::Uuid(value) => {
			output.push(2);
			output.extend_from_slice(value.as_bytes());
		}
		Id::Array(value) => {
			output.push(3);
			encode_array(output, value, depth + 1)?;
		}
		Id::Object(value) => {
			output.push(4);
			encode_object(output, value, depth + 1)?;
		}
		Id::Generate(_) | Id::Range(_) => {
			return Err(invalid("unevaluated record identity is unsupported"));
		}
	}
	Ok(())
}

fn encode_array(output: &mut Vec<u8>, values: &Array, depth: usize) -> Result<(), Error> {
	depth_guard(depth)?;
	output.extend_from_slice(&(values.len() as u64).to_be_bytes());
	for value in values.iter() {
		encode_value(output, value, depth + 1)?;
	}
	Ok(())
}

fn encode_object(output: &mut Vec<u8>, values: &Object, depth: usize) -> Result<(), Error> {
	depth_guard(depth)?;
	output.extend_from_slice(&(values.len() as u64).to_be_bytes());
	for (key, value) in values.iter() {
		field(output, key.as_bytes());
		encode_value(output, value, depth + 1)?;
	}
	Ok(())
}

fn encode_value(output: &mut Vec<u8>, value: &Value, depth: usize) -> Result<(), Error> {
	depth_guard(depth)?;
	match value {
		Value::None => output.push(0),
		Value::Null => output.push(1),
		Value::Bool(false) => output.push(2),
		Value::Bool(true) => output.push(3),
		Value::Number(Number::Int(value)) => {
			output.push(4);
			output.extend_from_slice(&value.to_be_bytes());
		}
		Value::Number(Number::Float(value)) => {
			if !value.is_finite() {
				return Err(invalid(
					"non-finite record identity component is unsupported",
				));
			}
			output.push(5);
			output.extend_from_slice(&value.to_bits().to_be_bytes());
		}
		Value::Number(Number::Decimal(value)) => {
			output.push(6);
			field(output, value.normalize().to_string().as_bytes());
		}
		Value::Strand(value) => {
			output.push(7);
			field(output, value.as_str().as_bytes());
		}
		Value::Duration(value) => {
			output.push(8);
			output.extend_from_slice(&value.nanos().to_be_bytes());
		}
		Value::Datetime(value) => {
			output.push(9);
			output.extend_from_slice(&value.0.timestamp().to_be_bytes());
			output.extend_from_slice(&value.0.timestamp_subsec_nanos().to_be_bytes());
		}
		Value::Uuid(value) => {
			output.push(10);
			output.extend_from_slice(value.as_bytes());
		}
		Value::Array(value) => {
			output.push(11);
			encode_array(output, value, depth + 1)?;
		}
		Value::Object(value) => {
			output.push(12);
			encode_object(output, value, depth + 1)?;
		}
		Value::Geometry(value) => {
			output.push(13);
			// The pinned native Geometry serializer preserves variant and ordered
			// coordinates. It does not parse or execute a display representation.
			field(
				output,
				&revision::to_vec(value).map_err(|_| invalid("invalid geometry identity"))?,
			);
		}
		Value::Bytes(value) => {
			output.push(14);
			field(output, value);
		}
		Value::Thing(value) => {
			output.push(15);
			field(output, value.tb.as_bytes());
			encode_id(output, &value.id, depth + 1)?;
		}
		_ => {
			return Err(invalid(
				"unevaluated record identity component is unsupported",
			))
		}
	}
	Ok(())
}

// A negative filter only: accepted strings still pass through the retained parser
// and exact native-display comparison. Neither JSON prefixes nor an unquoted
// UUID-shaped legacy key can be a canonical Thing display. Native string UUID
// keys require backticks; native UUID keys require u-quotes. In particular, do
// not exclude other hyphenated keys: `table:-123` is a native numeric identity.
fn may_be_canonical_reference(value: &str) -> bool {
	if matches!(
		value.trim_start().as_bytes().first(),
		Some(b'{' | b'[' | b'"')
	) {
		return false;
	}
	if let Some((table, key)) = value.split_once(':') {
		if !table.is_empty()
			&& table
				.bytes()
				.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
			&& key.len() == 36
			&& key.bytes().enumerate().all(|(index, byte)| {
				if matches!(index, 8 | 13 | 18 | 23) {
					byte == b'-'
				} else {
					byte.is_ascii_hexdigit()
				}
			})
		{
			return false;
		}
	}
	true
}

struct Extractor<'a> {
	// Owned, invocation-local state after the exact v1 domain/store prefix.
	// Clone it for each identity; never advance it while emitting a token.
	prefixed_mac: Hmac<Sha256>,
	typed_tables: BTreeSet<&'a str>,
	tokens: BTreeSet<String>,
	remaining_nodes: usize,
	decoded_bytes: usize,
}

impl Extractor<'_> {
	fn consume(&mut self, depth: usize) -> Result<(), Error> {
		depth_guard(depth)?;
		self.remaining_nodes = self
			.remaining_nodes
			.checked_sub(1)
			.ok_or_else(|| invalid("reference node limit exceeded"))?;
		Ok(())
	}

	fn identity_token(&self, reference: &Thing) -> Result<String, Error> {
		if reference.tb.is_empty() {
			return Err(invalid("empty record table is unsupported"));
		}
		let mut message = Vec::new();
		field(&mut message, reference.tb.as_bytes());
		let normalized = match &reference.id {
			Id::String(value) if self.typed_tables.contains(reference.tb.as_str()) => {
				uuid::Uuid::parse_str(value)
					.ok()
					.map(|uuid| Id::Uuid(uuid.into()))
			}
			_ => None,
		};
		encode_id(
			&mut message,
			normalized.as_ref().unwrap_or(&reference.id),
			0,
		)?;
		let mut mac = self.prefixed_mac.clone();
		mac.update(&message);
		Ok(hex::encode(mac.finalize().into_bytes()))
	}

	fn reference(&mut self, reference: &Thing, depth: usize) -> Result<(), Error> {
		self.consume(depth)?;
		self.tokens.insert(self.identity_token(reference)?);
		// A composite native key may itself contain a native reference.
		match &reference.id {
			Id::Array(values) => {
				for value in values.iter() {
					self.native(value, depth + 1)?;
				}
			}
			Id::Object(values) => {
				for (key, value) in values.iter() {
					self.string(key, depth + 1)?;
					self.native(value, depth + 1)?;
				}
			}
			_ => {}
		}
		Ok(())
	}

	fn string(&mut self, value: &str, depth: usize) -> Result<(), Error> {
		self.consume(depth)?;
		if value.contains(':') {
			if may_be_canonical_reference(value) {
				if let Ok(reference) = crate::syn::thing(value) {
					// A parser accepts comments and surrounding whitespace too. Only
					// complete canonical spellings count as serialized references.
					if reference.to_string() == value {
						self.reference(&reference, depth + 1)?;
						return Ok(());
					}
				}
			}
			if let Some((table, key)) = value.split_once(':') {
				// Retained domain RecordId spellings and simple legacy extension
				// keys precede native quoted Thing display. Require the entire
				// string to be this grammar; never search inside prose.
				let table_valid = !table.is_empty()
					&& table
						.bytes()
						.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_');
				let key_valid = !key.is_empty()
					&& key
						.bytes()
						.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-');
				if table_valid && key_valid {
					self.reference(&Thing::from((table, key)), depth + 1)?;
				}
			}
		}
		let first = value.trim_start().as_bytes().first().copied();
		if matches!(first, Some(b'{' | b'[' | b'"')) {
			self.decoded_bytes = self
				.decoded_bytes
				.checked_add(value.len())
				.filter(|bytes| *bytes <= MAX_DECODED_BYTES)
				.ok_or_else(|| invalid("encoded reference byte limit exceeded"))?;
			match serde_json::from_str::<serde_json::Value>(value) {
				Ok(decoded) => self.json(&decoded, depth + 1)?,
				Err(error) if error.to_string().starts_with("recursion limit exceeded") => {
					return Err(invalid("encoded reference nesting limit exceeded"));
				}
				Err(_) => {}
			}
		}
		Ok(())
	}

	fn native(&mut self, value: &Value, depth: usize) -> Result<(), Error> {
		self.consume(depth)?;
		match value {
			Value::Thing(reference) => self.reference(reference, depth + 1)?,
			Value::Strand(value) => self.string(value.as_str(), depth + 1)?,
			Value::Array(values) => {
				for value in values.iter() {
					self.native(value, depth + 1)?;
				}
			}
			Value::Object(values) => {
				if let Some(reference) = native_reference_object(values) {
					self.reference(&reference, depth + 1)?;
				}
				for (key, value) in values.iter() {
					self.string(key, depth + 1)?;
					self.native(value, depth + 1)?;
				}
			}
			Value::None
			| Value::Null
			| Value::Bool(_)
			| Value::Number(_)
			| Value::Duration(_)
			| Value::Datetime(_)
			| Value::Uuid(_)
			| Value::Geometry(_)
			| Value::Bytes(_) => {}
			_ => return Err(invalid("unevaluated reference value is unsupported")),
		}
		Ok(())
	}

	fn json(&mut self, value: &serde_json::Value, depth: usize) -> Result<(), Error> {
		self.consume(depth)?;
		match value {
			serde_json::Value::String(value) => self.string(value, depth + 1)?,
			serde_json::Value::Array(values) => {
				for value in values {
					self.json(value, depth + 1)?;
				}
			}
			serde_json::Value::Object(values) => {
				if let Some(table) = values
					.get("tb")
					.or_else(|| values.get("table"))
					.and_then(serde_json::Value::as_str)
				{
					if let Some(id) = values.get("id").and_then(json_reference_id) {
						self.reference(&Thing::from((table, id)), depth + 1)?;
					}
				}
				for (key, value) in values {
					self.string(key, depth + 1)?;
					self.json(value, depth + 1)?;
				}
			}
			_ => {}
		}
		Ok(())
	}
}

fn native_reference_object(values: &Object) -> Option<Thing> {
	let Value::Strand(table) = values.get("tb").or_else(|| values.get("table"))? else {
		return None;
	};
	let id = match values.get("id")? {
		Value::Strand(value) => Id::String(value.as_str().to_owned()),
		Value::Number(Number::Int(value)) => Id::Number(*value),
		Value::Uuid(value) => Id::Uuid(*value),
		Value::Array(value) => Id::Array(value.clone()),
		Value::Object(value) => Id::Object(value.clone()),
		Value::Thing(value) if value.tb == table.as_str() => value.id.clone(),
		_ => return None,
	};
	Some(Thing::from((table.as_str(), id)))
}

fn json_reference_id(value: &serde_json::Value) -> Option<Id> {
	match value {
		serde_json::Value::String(value) => Some(Id::String(value.clone())),
		serde_json::Value::Number(value) => value.as_i64().map(Id::Number),
		serde_json::Value::Array(values) => values
			.iter()
			.map(json_native_value)
			.collect::<Option<Vec<_>>>()
			.map(|values| Id::Array(values.into())),
		serde_json::Value::Object(values) => values
			.iter()
			.map(|(key, value)| Some((key.clone(), json_native_value(value)?)))
			.collect::<Option<std::collections::BTreeMap<_, _>>>()
			.map(|values| Id::Object(values.into())),
		_ => None,
	}
}

fn json_native_value(value: &serde_json::Value) -> Option<Value> {
	Some(match value {
		serde_json::Value::Null => Value::Null,
		serde_json::Value::Bool(value) => Value::Bool(*value),
		serde_json::Value::Number(value) => {
			if let Some(value) = value.as_i64() {
				Value::from(value)
			} else if let Some(value) = value.as_u64() {
				// Never round a large integer through f64: different native
				// composite identities must retain different token messages.
				Value::Number(Number::Decimal(rust_decimal::Decimal::from(value)))
			} else {
				Value::from(value.as_f64()?)
			}
		}
		serde_json::Value::String(value) => Value::from(value.clone()),
		serde_json::Value::Array(values) => Value::Array(
			values
				.iter()
				.map(json_native_value)
				.collect::<Option<Vec<_>>>()?
				.into(),
		),
		serde_json::Value::Object(values) => Value::Object(
			values
				.iter()
				.map(|(key, value)| Some((key.clone(), json_native_value(value)?)))
				.collect::<Option<std::collections::BTreeMap<_, _>>>()?
				.into(),
		),
	})
}

fn extractor<'a>(
	journal_identity: &str,
	space: &str,
	key: &'a [u8; 32],
	typed_uuid_tables: &'a [String],
) -> Result<Extractor<'a>, Error> {
	let journal =
		uuid::Uuid::parse_str(journal_identity).map_err(|_| invalid("invalid journal identity"))?;
	if journal.to_string() != journal_identity || space.is_empty() {
		return Err(invalid("invalid journal identity or space"));
	}
	if typed_uuid_tables.iter().any(|table| {
		table.is_empty()
			|| !table
				.bytes()
				.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
	}) {
		return Err(invalid("invalid typed table configuration"));
	}
	let mut prefix = DOMAIN.to_vec();
	field(&mut prefix, journal_identity.as_bytes());
	field(&mut prefix, space.as_bytes());
	let mut prefixed_mac =
		Hmac::<Sha256>::new_from_slice(key).map_err(|_| invalid("invalid reference key"))?;
	prefixed_mac.update(&prefix);
	Ok(Extractor {
		prefixed_mac,
		typed_tables: typed_uuid_tables.iter().map(String::as_str).collect(),
		tokens: BTreeSet::new(),
		remaining_nodes: MAX_NODES,
		decoded_bytes: 0,
	})
}

/// Tokenize one physical identity without collecting references it contains.
/// Erasing a composite-ID row must not fence the independent rows its ID cites.
pub fn recall_reference_identity_token(
	journal_identity: &str,
	space: &str,
	key: &[u8; 32],
	typed_uuid_tables: &[String],
	identity: &Thing,
) -> Result<String, Error> {
	extractor(journal_identity, space, key, typed_uuid_tables)?.identity_token(identity)
}

/// Produce exact, store-bound opaque tokens from a fully evaluated native value.
/// The key and all record identities are excluded from error messages.
pub fn recall_reference_tokens(
	journal_identity: &str,
	space: &str,
	key: &[u8; 32],
	typed_uuid_tables: &[String],
	value: &Value,
) -> Result<Vec<String>, Error> {
	let mut extractor = extractor(journal_identity, space, key, typed_uuid_tables)?;
	extractor.native(value, 0)?;
	Ok(extractor.tokens.into_iter().collect())
}

// Manual, non-coercing argument inspection avoids upstream type-conversion
// errors formatting the supplied private key as part of an error message.
pub fn reference_tokens(arguments: Vec<Value>) -> Result<Value, Error> {
	let [Value::Strand(journal), Value::Strand(space), Value::Bytes(key), Value::Array(tables), value]: [Value; 5] = arguments.try_into()
        .map_err(|_| invalid("expected journal, space, private bytes, typed tables and value"))?
    else { return Err(invalid("invalid reference token argument types")); };
	let key: &[u8; 32] = key
		.as_slice()
		.try_into()
		.map_err(|_| invalid("invalid reference key length"))?;
	let tables = tables
		.0
		.into_iter()
		.map(|table| match table {
			Value::Strand(table) => Ok(table.0),
			_ => Err(invalid("invalid typed table configuration")),
		})
		.collect::<Result<Vec<_>, _>>()?;
	Ok(Value::Array(
		recall_reference_tokens(journal.as_str(), space.as_str(), key, &tables, &value)?.into(),
	))
}

pub fn reference_token_version((): ()) -> Result<Value, Error> {
	Ok(Value::from(i64::from(RECALL_REFERENCE_TOKEN_VERSION)))
}

/// Compare every original top-level field using the native SQL equality rule.
///
/// Missing keys behave exactly like `$current[$key]`: they yield `NONE`.
/// Unobserved extra fields are ignored. The caller owns existence, space,
/// conflict writes and transaction guards; this function only compares values.
pub fn original_fields_match(arguments: Vec<Value>) -> Result<Value, Error> {
	let invalid = || Error::InvalidArguments {
		name: "recall::original_fields_match".into(),
		message: "expected current and original field objects".into(),
	};
	let [Value::Object(current), Value::Object(expected)]: [Value; 2] =
		arguments.try_into().map_err(|_| invalid())?
	else {
		return Err(invalid());
	};
	Ok(Value::Bool(expected.iter().all(|(key, value)| {
		current.get(key).unwrap_or(&Value::None).equal(value)
	})))
}

/// Fuse the retained presence, space and original-field predicates.
///
/// Callers supply stored native data, never executable expressions. Absence or
/// a malformed current row cannot match; malformed guard arguments fail closed.
/// Database reads, conflict writes and their events remain with the caller.
pub fn original_row_matches(arguments: Vec<Value>) -> Result<Value, Error> {
	let invalid = || Error::InvalidArguments {
		name: "recall::original_row_matches".into(),
		message: "expected current value, space string and original field object".into(),
	};
	let [current, space @ Value::Strand(_), Value::Object(expected)]: [Value; 3] =
		arguments.try_into().map_err(|_| invalid())?
	else {
		return Err(invalid());
	};
	let Value::Object(current) = current else {
		return Ok(Value::Bool(false));
	};
	Ok(Value::Bool(
		current
			.get("space_id")
			.unwrap_or(&Value::None)
			.equal(&space)
			&& expected
				.iter()
				.all(|(key, value)| current.get(key).unwrap_or(&Value::None).equal(value)),
	))
}

#[cfg(test)]
#[path = "recall_hmac_tests.rs"]
mod hmac_tests;
