use super::{
	decode_history_key, decode_native, invalid, valid_digest, NativeHistoryKey, MAX_HISTORY_BYTES,
	MAX_HISTORY_RECORDS,
};
use crate::err::Error;
use crate::key;
use crate::kvs::KeyEncode;
use crate::sql::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;

/// A raw native version from a bounded, scoped, privileged inspection.
#[derive(Clone, Eq, PartialEq)]
pub struct RecallHistoryRecord {
	pub key: Vec<u8>,
	pub value: Vec<u8>,
	pub version: u64,
	pub timestamp: u64,
	pub tombstone: bool,
	pub current: bool,
}

impl fmt::Debug for RecallHistoryRecord {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RecallHistoryRecord")
			.field("key_bytes", &self.key.len())
			.field("value_bytes", &self.value.len())
			.field("version", &self.version)
			.field("timestamp", &self.timestamp)
			.field("tombstone", &self.tombstone)
			.field("current", &self.current)
			.finish_non_exhaustive()
	}
}

impl RecallHistoryRecord {
	/// Encode a typed native value with the same pinned codec used by the store.
	pub fn encode_value(value: &Value) -> Result<Vec<u8>, Error> {
		super::encode_native_history_value(value)
	}

	pub fn decoded_key(&self) -> Result<NativeHistoryKey, Error> {
		decode_history_key(&self.key)
	}

	pub fn decoded_value(&self) -> Result<Value, Error> {
		if self.tombstone {
			return Err(invalid("tombstone has no native document value"));
		}
		decode_native(&self.value)
	}

	pub(super) fn visit_commitment_fields(&self, mut visit: impl FnMut(&[u8])) {
		visit(&(self.key.len() as u64).to_be_bytes());
		visit(&self.key);
		visit(&self.version.to_be_bytes());
		visit(&self.timestamp.to_be_bytes());
		visit(&[u8::from(self.tombstone), u8::from(self.current)]);
		visit(&(self.value.len() as u64).to_be_bytes());
		visit(&self.value);
	}

	pub fn to_rpc_value(&self) -> Value {
		Value::Object(
			[
				("key".into(), Value::Bytes(self.key.clone().into())),
				("value".into(), Value::Bytes(self.value.clone().into())),
				(
					"version".into(),
					Value::Bytes(self.version.to_be_bytes().to_vec().into()),
				),
				(
					"timestamp".into(),
					Value::Bytes(self.timestamp.to_be_bytes().to_vec().into()),
				),
				("tombstone".into(), Value::Bool(self.tombstone)),
				("current".into(), Value::Bool(self.current)),
			]
			.into_iter()
			.collect(),
		)
	}

	pub fn from_rpc_value(value: Value) -> Result<Self, Error> {
		let Value::Object(mut fields) = value else {
			return Err(invalid("native history row is not an object"));
		};
		if fields.len() != 6 {
			return Err(invalid("native history row field set differs"));
		}
		let bytes = |value| match value {
			Some(Value::Bytes(bytes)) if bytes.len() <= MAX_HISTORY_BYTES => Ok(bytes.into_inner()),
			_ => Err(invalid("native history byte field differs")),
		};
		let number = |value| -> Result<u64, Error> {
			let bytes: [u8; 8] = bytes(value)?
				.try_into()
				.map_err(|_| invalid("native history integer width differs"))?;
			Ok(u64::from_be_bytes(bytes))
		};
		let boolean = |value| match value {
			Some(Value::Bool(value)) => Ok(value),
			_ => Err(invalid("native history boolean differs")),
		};
		let row = Self {
			key: bytes(fields.remove("key"))?,
			value: bytes(fields.remove("value"))?,
			version: number(fields.remove("version"))?,
			timestamp: number(fields.remove("timestamp"))?,
			tombstone: boolean(fields.remove("tombstone"))?,
			current: boolean(fields.remove("current"))?,
		};
		if !fields.is_empty() || row.key.is_empty() || (row.tombstone && !row.value.is_empty()) {
			return Err(invalid("native history row is malformed"));
		}
		Ok(row)
	}
}

/// A complete native scope. Mutation after inspection is detected before copying.
#[derive(Clone)]
pub struct RecallHistorySnapshot {
	pub namespace: String,
	pub database: String,
	pub fingerprint: String,
	pub records: Vec<RecallHistoryRecord>,
}

impl fmt::Debug for RecallHistorySnapshot {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RecallHistorySnapshot")
			.field("namespace", &self.namespace)
			.field("database", &self.database)
			.field("fingerprint", &self.fingerprint)
			.field("records", &self.records.len())
			.finish_non_exhaustive()
	}
}

impl RecallHistorySnapshot {
	/// Input order is irrelevant; the commitment uses exact sorted native versions.
	pub fn new(
		namespace: String,
		database: String,
		mut records: Vec<RecallHistoryRecord>,
	) -> Result<Self, Error> {
		if records.len() > MAX_HISTORY_RECORDS {
			return Err(invalid("native history exceeds record bound"));
		}
		records.sort_by(|a, b| {
			(&a.key, a.version, a.timestamp).cmp(&(&b.key, b.version, b.timestamp))
		});
		let mut result = Self {
			namespace,
			database,
			fingerprint: String::new(),
			records,
		};
		result.fingerprint = result.calculate_fingerprint()?;
		Ok(result)
	}

	pub fn verify(&self) -> Result<(), Error> {
		if !valid_digest(&self.fingerprint) || self.calculate_fingerprint()? != self.fingerprint {
			return Err(invalid("original scoped native history changed"));
		}
		Ok(())
	}

	pub fn contains_key(&self, key: &[u8]) -> Result<bool, Error> {
		Ok(key.starts_with(&key::database::all::new(&self.namespace, &self.database).encode()?))
	}

	fn calculate_fingerprint(&self) -> Result<String, Error> {
		if self.namespace.is_empty()
			|| self.database.is_empty()
			|| self.namespace.len() > 4096
			|| self.database.len() > 4096
			|| self.records.len() > MAX_HISTORY_RECORDS
		{
			return Err(invalid(
				"native history scope or record count exceeds bound",
			));
		}
		let prefix = key::database::all::new(&self.namespace, &self.database).encode()?;
		let mut digest = Sha256::new();
		digest.update(b"meerkat-recall/native-history-snapshot/v1\0");
		digest.update((prefix.len() as u64).to_be_bytes());
		digest.update(&prefix);
		digest.update((self.records.len() as u64).to_be_bytes());
		let mut bytes = 0usize;
		let mut prior: Option<(&[u8], u64, u64)> = None;
		let mut current_counts: BTreeMap<&[u8], usize> = BTreeMap::new();
		for row in &self.records {
			bytes = bytes
				.checked_add(row.key.len())
				.and_then(|n| n.checked_add(row.value.len()))
				.and_then(|n| n.checked_add(64))
				.ok_or_else(|| invalid("native history input size overflow"))?;
			let identity = (row.key.as_slice(), row.version, row.timestamp);
			if bytes > MAX_HISTORY_BYTES
				|| !row.key.starts_with(&prefix)
				|| (row.tombstone && !row.value.is_empty())
				|| prior.is_some_and(|p| p >= identity)
			{
				return Err(invalid(
					"native history bound, scope, ordering or version differs",
				));
			}
			prior = Some(identity);
			let count = current_counts.entry(&row.key).or_default();
			*count += usize::from(row.current);
			if *count > 1 {
				return Err(invalid("native key has multiple current versions"));
			}
			row.visit_commitment_fields(|part| digest.update(part));
		}
		if current_counts.values().any(|count| *count != 1) {
			return Err(invalid("native key has no inspected current version"));
		}
		Ok(hex::encode(digest.finalize()))
	}

	pub fn to_rpc_value(&self) -> Value {
		Value::Object(
			[
				("version".into(), Value::from(1_i64)),
				("namespace".into(), Value::from(self.namespace.clone())),
				("database".into(), Value::from(self.database.clone())),
				("fingerprint".into(), Value::from(self.fingerprint.clone())),
				(
					"records".into(),
					Value::Array(
						self.records
							.iter()
							.map(RecallHistoryRecord::to_rpc_value)
							.collect::<Vec<_>>()
							.into(),
					),
				),
			]
			.into_iter()
			.collect(),
		)
	}

	pub fn from_rpc_value(value: Value) -> Result<Self, Error> {
		let Value::Object(mut fields) = value else {
			return Err(invalid("native history snapshot is not an object"));
		};
		if fields.len() != 5 || fields.remove("version") != Some(Value::from(1_i64)) {
			return Err(invalid(
				"native history snapshot version or field set differs",
			));
		}
		let string = |v| match v {
			Some(Value::Strand(v)) => Ok(v.0),
			_ => Err(invalid("native history snapshot string differs")),
		};
		let namespace = string(fields.remove("namespace"))?;
		let database = string(fields.remove("database"))?;
		let fingerprint = string(fields.remove("fingerprint"))?;
		let Some(Value::Array(records)) = fields.remove("records") else {
			return Err(invalid("native history record list differs"));
		};
		if records.len() > MAX_HISTORY_RECORDS || !fields.is_empty() {
			return Err(invalid("native history snapshot exceeds record bound"));
		}
		let records = records
			.0
			.into_iter()
			.map(RecallHistoryRecord::from_rpc_value)
			.collect::<Result<Vec<_>, _>>()?;
		let snapshot = Self {
			namespace,
			database,
			fingerprint,
			records,
		};
		snapshot.verify()?;
		Ok(snapshot)
	}
}
