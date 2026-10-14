//! Typed source ownership for native historical versions.
//!
//! This is called by the exclusive physical-generation copier, never registered
//! as a SQL function. The caller owns original-journal custody, complete history
//! enumeration, causal support proofs and derived-index reconstruction.

use crate::err::Error;
use crate::fnc::recall::{recall_reference_identity_token, recall_reference_tokens};
use crate::key;
use crate::kvs::{KeyDecode, KeyEncode};
use crate::sql::{Id, Thing, Value};
use hmac::{Hmac, Mac};
use revision::Revisioned;
use sha2::Sha256;
use std::collections::BTreeSet;
use std::fmt;
use std::io::Cursor;

#[path = "recall_history_inventory.rs"]
mod inventory;
pub use inventory::{RecallHistoryRecord, RecallHistorySnapshot};
#[path = "recall_history_selection.rs"]
mod selection;
pub use selection::{HistoryDecision, HistorySelection};

pub const MAX_HISTORY_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_HISTORY_RECORDS: usize = 1_000_000;

fn valid_digest(value: &str) -> bool {
	value.len() == 64
		&& value
			.bytes()
			.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn invalid(message: &'static str) -> Error {
	Error::InvalidArguments {
		name: "recall history authority".into(),
		message: message.into(),
	}
}

/// A journal-bound capability supplied only through the trusted native command.
/// No persisted receipt, Debug representation or error contains the HMAC key.
#[derive(Clone)]
pub struct RecallHistoryAuthority {
	namespace: String,
	database: String,
	journal_identity: String,
	space: String,
	key: [u8; 32],
	typed_uuid_tables: Vec<String>,
	membership: BTreeSet<String>,
	history_fingerprint: Option<String>,
	erased_versions: BTreeSet<String>,
	retained_controls: BTreeSet<String>,
}

impl fmt::Debug for RecallHistoryAuthority {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("RecallHistoryAuthority")
			.field("namespace", &self.namespace)
			.field("database", &self.database)
			.field("space", &self.space)
			.field("membership_count", &self.membership.len())
			.finish_non_exhaustive()
	}
}

impl RecallHistoryAuthority {
	/// Construct from the original journal under its maintenance reservation.
	/// The caller must independently authenticate the journal anchor and key.
	pub fn new(
		namespace: String,
		database: String,
		journal_identity: String,
		space: String,
		key: [u8; 32],
		typed_uuid_tables: Vec<String>,
		membership: BTreeSet<String>,
	) -> Result<Self, Error> {
		if membership.is_empty()
			|| membership.len() > MAX_HISTORY_RECORDS
			|| membership.iter().any(|token| !valid_digest(token))
		{
			return Err(invalid("missing or invalid original membership"));
		}
		let mut authority = Self::for_backup_inspection(
			namespace,
			database,
			journal_identity,
			space,
			key,
			typed_uuid_tables,
		)?;
		authority.membership = membership;
		Ok(authority)
	}

	/// Inspect backup origins before any erasure intent exists. This context can
	/// compute exact version commitments but cannot bind or cross the physical
	/// authority boundary because it has no original erased membership.
	pub fn for_backup_inspection(
		namespace: String,
		database: String,
		journal_identity: String,
		space: String,
		key: [u8; 32],
		typed_uuid_tables: Vec<String>,
	) -> Result<Self, Error> {
		if namespace.is_empty()
			|| database.is_empty()
			|| [&namespace, &database, &space]
				.iter()
				.any(|s| s.len() > 4096)
			|| journal_identity.len() != 36
			|| typed_uuid_tables.len() > 1024
			|| typed_uuid_tables.iter().any(|t| t.len() > 256)
		{
			return Err(invalid(
				"history inspection scope exceeds bounded native input",
			));
		}
		// Reuse the actual typed normalizer's journal, space and table validation.
		recall_reference_tokens(
			&journal_identity,
			&space,
			&key,
			&typed_uuid_tables,
			&Value::None,
		)?;
		Ok(Self {
			namespace,
			database,
			journal_identity,
			space,
			key,
			typed_uuid_tables,
			membership: BTreeSet::new(),
			history_fingerprint: None,
			erased_versions: BTreeSet::new(),
			retained_controls: BTreeSet::new(),
		})
	}

	pub fn namespace(&self) -> &str {
		&self.namespace
	}

	pub fn database(&self) -> &str {
		&self.database
	}

	pub fn space(&self) -> &str {
		&self.space
	}

	/// Bind opaque exact-version proofs durably recorded by the original journal.
	pub fn bind_history(
		mut self,
		fingerprint: String,
		erased_versions: BTreeSet<String>,
		retained_controls: BTreeSet<String>,
	) -> Result<Self, Error> {
		if self.membership.is_empty()
			|| !valid_digest(&fingerprint)
			|| erased_versions
				.len()
				.saturating_add(retained_controls.len())
				> MAX_HISTORY_RECORDS
			|| erased_versions
				.iter()
				.chain(&retained_controls)
				.any(|token| !valid_digest(token))
			|| !erased_versions.is_disjoint(&retained_controls)
		{
			return Err(invalid(
				"invalid or contradictory historical version commitments",
			));
		}
		if self.history_fingerprint.is_some() {
			return Err(invalid("historical authority is already bound"));
		}
		self.history_fingerprint = Some(fingerprint);
		self.erased_versions = erased_versions;
		self.retained_controls = retained_controls;
		Ok(self)
	}

	pub fn validate_bound(&self) -> Result<(), Error> {
		if self.membership.is_empty()
			|| self
				.history_fingerprint
				.as_ref()
				.is_none_or(|v| !valid_digest(v))
		{
			return Err(invalid(
				"history authority has no original inspected stream",
			));
		}
		Ok(())
	}

	/// Opaque proof of this exact native version; contains no recoverable payload.
	pub fn version_commitment(&self, record: &RecallHistoryRecord) -> Result<String, Error> {
		let mut mac = Hmac::<Sha256>::new_from_slice(&self.key)
			.map_err(|_| invalid("invalid historical commitment key"))?;
		mac.update(b"meerkat-recall/native-history-version/v1\0");
		for field in [
			&self.namespace,
			&self.database,
			&self.journal_identity,
			&self.space,
		] {
			mac.update(&(field.len() as u64).to_be_bytes());
			mac.update(field.as_bytes());
		}
		record.visit_commitment_fields(|part| mac.update(part));
		Ok(hex::encode(mac.finalize().into_bytes()))
	}

	/// Sensitive native transport value. The trusted command must redact it.
	pub fn to_rpc_value(&self) -> Value {
		Value::Object(
			[
				("version".into(), Value::from(1_i64)),
				("namespace".into(), Value::from(self.namespace.clone())),
				("database".into(), Value::from(self.database.clone())),
				(
					"journal_identity".into(),
					Value::from(self.journal_identity.clone()),
				),
				("space".into(), Value::from(self.space.clone())),
				("key".into(), Value::Bytes(self.key.to_vec().into())),
				(
					"typed_uuid_tables".into(),
					Value::Array(
						self.typed_uuid_tables
							.iter()
							.cloned()
							.map(Value::from)
							.collect::<Vec<_>>()
							.into(),
					),
				),
				(
					"membership".into(),
					Value::Array(
						self.membership
							.iter()
							.cloned()
							.map(Value::from)
							.collect::<Vec<_>>()
							.into(),
					),
				),
				(
					"history_fingerprint".into(),
					self.history_fingerprint
						.as_ref()
						.map_or(Value::None, |v| Value::from(v.clone())),
				),
				(
					"erased_versions".into(),
					Value::Array(
						self.erased_versions
							.iter()
							.cloned()
							.map(Value::from)
							.collect::<Vec<_>>()
							.into(),
					),
				),
				(
					"retained_controls".into(),
					Value::Array(
						self.retained_controls
							.iter()
							.cloned()
							.map(Value::from)
							.collect::<Vec<_>>()
							.into(),
					),
				),
			]
			.into_iter()
			.collect(),
		)
	}

	/// Strict, non-coercing native decoding, with no secret-bearing errors.
	pub fn from_rpc_value(value: Value) -> Result<Self, Error> {
		let Value::Object(mut fields) = value else {
			return Err(invalid("history authority must be a native object"));
		};
		if fields.len() != 11 || fields.remove("version") != Some(Value::from(1_i64)) {
			return Err(invalid("history authority version or field set differs"));
		}
		let string = |value| match value {
			Some(Value::Strand(value)) => Ok(value.0),
			_ => Err(invalid("history authority string field differs")),
		};
		let strings = |value| -> Result<Vec<String>, Error> {
			let Some(Value::Array(values)) = value else {
				return Err(invalid("history authority list field differs"));
			};
			if values.len() > MAX_HISTORY_RECORDS {
				return Err(invalid("history authority list exceeds input bound"));
			}
			values
				.0
				.into_iter()
				.map(|value| string(Some(value)))
				.collect()
		};
		let namespace = string(fields.remove("namespace"))?;
		let database = string(fields.remove("database"))?;
		let journal = string(fields.remove("journal_identity"))?;
		let space = string(fields.remove("space"))?;
		let Some(Value::Bytes(bytes)) = fields.remove("key") else {
			return Err(invalid("history authority key shape differs"));
		};
		let key: [u8; 32] = bytes
			.as_slice()
			.try_into()
			.map_err(|_| invalid("history authority key length differs"))?;
		let tables = strings(fields.remove("typed_uuid_tables"))?;
		let tokens = strings(fields.remove("membership"))?;
		let membership: BTreeSet<String> = tokens.iter().cloned().collect();
		let fingerprint = string(fields.remove("history_fingerprint"))?;
		let erased = strings(fields.remove("erased_versions"))?;
		let retained = strings(fields.remove("retained_controls"))?;
		let erased_set: BTreeSet<String> = erased.iter().cloned().collect();
		let retained_set: BTreeSet<String> = retained.iter().cloned().collect();
		if !fields.is_empty()
			|| membership.len() != tokens.len()
			|| erased_set.len() != erased.len()
			|| retained_set.len() != retained.len()
		{
			return Err(invalid(
				"history authority fields or membership duplicates differ",
			));
		}
		Self::new(namespace, database, journal, space, key, tables, membership)?.bind_history(
			fingerprint,
			erased_set,
			retained_set,
		)
	}

	/// Pure matching only; the reference normalizer never evaluates stored SQL.
	pub fn references_erased(&self, value: &Value) -> Result<bool, Error> {
		Ok(recall_reference_tokens(
			&self.journal_identity,
			&self.space,
			&self.key,
			&self.typed_uuid_tables,
			value,
		)?
		.iter()
		.any(|token| self.membership.contains(token)))
	}

	pub fn erased_identity(&self, identity: &Thing) -> Result<bool, Error> {
		let token = recall_reference_identity_token(
			&self.journal_identity,
			&self.space,
			&self.key,
			&self.typed_uuid_tables,
			identity,
		)?;
		Ok(self.membership.contains(&token))
	}

	/// Classify one actual native record version, including currently absent rows.
	///
	/// `retained_control` is an exact-value verdict from the existing lifecycle
	/// authority, not a table-name allowance. Historical system metadata needs its
	/// own exact inspected-version proof. A contaminated value becomes a same-version tombstone
	/// when independent versions of its identity survive; blindly omitting it can
	/// reveal an older version at that historical timestamp.
	pub fn classify_record_version(
		&self,
		native_key: &[u8],
		native_value: &[u8],
		tombstone: bool,
		current: bool,
		retained_control: bool,
	) -> Result<RecordVersionDecision, Error> {
		let NativeHistoryKey::Record {
			namespace,
			database,
			identity,
		} = decode_history_key(native_key)?
		else {
			return Ok(RecordVersionDecision::NotRecord);
		};
		if namespace != self.namespace || database != self.database {
			return Ok(RecordVersionDecision::Keep);
		}
		// Tombstones contain no record value. Their keys may still be removed by
		// the whole-identity decision, after all independent versions are checked.
		if tombstone {
			if !native_value.is_empty() {
				return Err(invalid("native record tombstone contains a value"));
			}
			return Ok(RecordVersionDecision::Keep);
		}
		let value: Value = decode_native(native_value)?;
		let Value::Object(fields) = &value else {
			return Err(invalid("native historical record is not an object"));
		};
		let Some(Value::Strand(space)) = fields.get("space_id") else {
			// Unscoped system records are handled by the caller's explicit control
			// policy. No missing scope may be guessed from a UUID or byte substring.
			return Ok(RecordVersionDecision::UnscopedRecord);
		};
		if space.as_str() != self.space {
			return Ok(RecordVersionDecision::Keep);
		}
		let probe = Value::Array(vec![Value::Thing(identity), value].into());
		if !self.references_erased(&probe)? {
			return Ok(RecordVersionDecision::Keep);
		}
		// The selector obtains this verdict only from an exact matching inspected
		// version commitment. The lifecycle owner may certify historical system
		// metadata using its complete writer-derived shape and identity policy.
		if retained_control {
			return Ok(RecordVersionDecision::Keep);
		}
		if current {
			return Err(invalid(
				"current source-bearing record survived the original purge",
			));
		}
		Ok(RecordVersionDecision::EraseValue)
	}
}

/// The pinned native value encoder, bounded without a second unbounded buffer.
pub fn encode_native_history_value(value: &Value) -> Result<Vec<u8>, Error> {
	struct Bounded(Vec<u8>);
	impl std::io::Write for Bounded {
		fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
			if self
				.0
				.len()
				.checked_add(bytes.len())
				.is_none_or(|len| len > MAX_HISTORY_BYTES)
			{
				return Err(std::io::Error::other("native history value exceeds bound"));
			}
			self.0.extend_from_slice(bytes);
			Ok(bytes.len())
		}
		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}
	let mut output = Bounded(Vec::new());
	value
		.serialize_revisioned(&mut output)
		.map_err(|_| invalid("native historical value cannot be encoded within bound"))?;
	Ok(output.0)
}

fn origin_mac(
	domain: &[u8],
	journal_identity: &str,
	namespace: &str,
	database: &str,
	key: &[u8; 32],
	fields: &[&[u8]],
) -> Result<String, Error> {
	let journal = uuid::Uuid::parse_str(journal_identity)
		.map_err(|_| invalid("invalid backup origin journal"))?;
	if journal.to_string() != journal_identity
		|| namespace.is_empty()
		|| database.is_empty()
		|| namespace.len() > 4096
		|| database.len() > 4096
		|| fields
			.iter()
			.try_fold(0usize, |size, field| size.checked_add(field.len()))
			.is_none_or(|size| size > MAX_HISTORY_BYTES)
	{
		return Err(invalid("backup origin scope or input bound differs"));
	}
	let mut mac =
		Hmac::<Sha256>::new_from_slice(key).map_err(|_| invalid("invalid backup origin key"))?;
	mac.update(domain);
	for field in [
		journal_identity.as_bytes(),
		namespace.as_bytes(),
		database.as_bytes(),
	]
	.into_iter()
	.chain(fields.iter().copied())
	{
		mac.update(&(field.len() as u64).to_be_bytes());
		mac.update(field);
	}
	Ok(hex::encode(mac.finalize().into_bytes()))
}

/// Opaque lookup of an exact backup record's native key and complete value.
/// It is deliberately distinct from an erasure version commitment.
pub fn backup_origin_content_commitment(
	journal_identity: &str,
	namespace: &str,
	database: &str,
	key: &[u8; 32],
	native_key: &[u8],
	native_value: &[u8],
) -> Result<String, Error> {
	if !native_key.starts_with(&key::database::all::new(namespace, database).encode()?) {
		return Err(invalid("backup origin native key is outside its scope"));
	}
	origin_mac(
		b"meerkat-recall/backup-origin-content/v1\0",
		journal_identity,
		namespace,
		database,
		key,
		&[native_key, native_value],
	)
}

/// Opaque source identity in an authenticated backup causal map.
/// The caller supplies the domain RecordId spelling `source:<canonical UUID>`.
/// This token is distinct from content, metadata and native-version commitments.
pub fn backup_origin_source_commitment(
	journal_identity: &str,
	namespace: &str,
	database: &str,
	key: &[u8; 32],
	space: &str,
	source_id: &str,
) -> Result<String, Error> {
	if space.is_empty() || space.len() > 4096 || source_id.len() != 43 {
		return Err(invalid("backup causal source scope differs"));
	}
	let id = source_id
		.strip_prefix("source:")
		.ok_or_else(|| invalid("backup causal source table differs"))?;
	let parsed =
		uuid::Uuid::parse_str(id).map_err(|_| invalid("backup causal source identity differs"))?;
	if parsed.to_string() != id {
		return Err(invalid("backup causal source identity is noncanonical"));
	}
	recall_reference_tokens(journal_identity, space, key, &[], &Value::None)?;
	origin_mac(
		b"meerkat-recall/backup-origin-source/v1\0",
		journal_identity,
		namespace,
		database,
		key,
		&[space.as_bytes(), source_id.as_bytes()],
	)
}

/// Authenticate the caller's exact canonical sidecar bytes under original custody.
pub fn backup_origin_authenticator(
	journal_identity: &str,
	namespace: &str,
	database: &str,
	key: &[u8; 32],
	canonical_bytes: &[u8],
) -> Result<String, Error> {
	origin_mac(
		b"meerkat-recall/backup-origin-authenticator/v1\0",
		journal_identity,
		namespace,
		database,
		key,
		&[canonical_bytes],
	)
}

/// Authenticate the complete multi-scope imported-origin union in one journal.
/// The owner holds its original writer lock across read, merge, fsync and ACK.
pub fn backup_origin_journal_authenticator(
	journal_identity: &str,
	key: &[u8; 32],
	canonical_bytes: &[u8],
) -> Result<String, Error> {
	if journal_identity.len() != 36 || canonical_bytes.len() > MAX_HISTORY_BYTES {
		return Err(invalid("imported origin journal or input bound differs"));
	}
	let journal = uuid::Uuid::parse_str(journal_identity)
		.map_err(|_| invalid("invalid imported origin journal"))?;
	if journal.to_string() != journal_identity {
		return Err(invalid("imported origin journal is noncanonical"));
	}
	let mut mac = Hmac::<Sha256>::new_from_slice(key)
		.map_err(|_| invalid("invalid imported origin journal key"))?;
	mac.update(b"meerkat-recall/backup-origin-journal/v1\0");
	for field in [journal_identity.as_bytes(), canonical_bytes] {
		mac.update(&(field.len() as u64).to_be_bytes());
		mac.update(field);
	}
	Ok(hex::encode(mac.finalize().into_bytes()))
}

/// Decisions concern a single version, never all history of a modified key.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecordVersionDecision {
	Keep,
	EraseValue,
	NotRecord,
	UnscopedRecord,
}

/// Exact native identities for the caller's historical owner/version join.
#[derive(Clone, Debug, PartialEq)]
pub enum NativeHistoryKey {
	Record {
		namespace: String,
		database: String,
		identity: Thing,
	},
	Index {
		namespace: String,
		database: String,
		table: String,
		index: String,
		fields: crate::sql::Array,
		identity: Option<Thing>,
	},
	Graph {
		namespace: String,
		database: String,
		from: Thing,
		to: Thing,
	},
	Reference {
		namespace: String,
		database: String,
		from: Thing,
		to: Thing,
	},
	/// Aggregated full-text/vector/index-building state needs native rebuilding.
	IndexInternal {
		namespace: String,
		database: String,
		table: String,
		index: String,
	},
	/// Metadata and change-feed data require their own exact codec/policy.
	Other,
}

/// Decode native key classes without SQL/display parsing or UUID substring tests.
/// All recognized keys must round-trip through constructors with fixed markers.
pub fn decode_history_key(bytes: &[u8]) -> Result<NativeHistoryKey, Error> {
	if !bytes.starts_with(b"/*") {
		return Ok(NativeHistoryKey::Other);
	}
	// Namespace/database/table prefixes are native zero-terminated strings.
	// This dispatch only identifies the codec; the complete key is independently
	// decoded and reconstructed below before any ownership decision is possible.
	let mut rest = &bytes[2..];
	for (level, marker) in [b'*', b'*'].into_iter().enumerate() {
		let end = rest
			.iter()
			.position(|b| *b == 0)
			.ok_or_else(|| invalid("truncated native scope key"))?;
		rest = &rest[end + 1..];
		if rest.first() != Some(&marker) {
			if level == 1 {
				validate_database_metadata(bytes, rest)?;
			}
			return Ok(NativeHistoryKey::Other);
		}
		rest = &rest[1..];
	}
	let end = rest
		.iter()
		.position(|b| *b == 0)
		.ok_or_else(|| invalid("truncated native table key"))?;
	let tail = &rest[end + 1..];
	match tail.first() {
		Some(b'*') => {
			let decoded = key::thing::Thing::decode(bytes)
				.map_err(|_| invalid("invalid native record key"))?;
			let canonical =
				key::thing::Thing::new(decoded.ns, decoded.db, decoded.tb, decoded.id.clone());
			if canonical.encode()? != bytes {
				return Err(invalid("noncanonical native record key"));
			}
			Ok(NativeHistoryKey::Record {
				namespace: decoded.ns.into(),
				database: decoded.db.into(),
				identity: Thing::from((decoded.tb, decoded.id)),
			})
		}
		Some(b'+') => {
			let index_end = tail[1..]
				.iter()
				.position(|b| *b == 0)
				.ok_or_else(|| invalid("truncated native index key"))?
				+ 1;
			if tail.get(index_end + 1) != Some(&b'*') {
				let prefix_len = bytes.len() - tail.len() + index_end + 1;
				let prefix = key::index::all::All::decode(&bytes[..prefix_len])
					.map_err(|_| invalid("invalid native index prefix"))?;
				if key::index::all::new(prefix.ns, prefix.db, prefix.tb, prefix.ix).encode()?
					!= bytes[..prefix_len]
				{
					return Err(invalid("noncanonical native index prefix"));
				}
				// The deferred native builder stores an initial marker at the
				// exact All key, without a tagged suffix (kvs::index). A suffix
				// still requires its complete, canonical internal-key codec.
				if prefix_len != bytes.len() {
					validate_index_internal(
						bytes,
						tail.get(index_end + 1..index_end + 4).unwrap_or(&[]),
					)?;
				}
				return Ok(NativeHistoryKey::IndexInternal {
					namespace: prefix.ns.into(),
					database: prefix.db.into(),
					table: prefix.tb.into(),
					index: prefix.ix.into(),
				});
			}
			let decoded = key::index::Index::decode(bytes)
				.map_err(|_| invalid("invalid native index key"))?;
			if key::index::Index::new(
				decoded.ns,
				decoded.db,
				decoded.tb,
				decoded.ix,
				&decoded.fd,
				decoded.id.as_deref(),
			)
			.encode()? != bytes
			{
				return Err(invalid("noncanonical native index key"));
			}
			Ok(NativeHistoryKey::Index {
				namespace: decoded.ns.into(),
				database: decoded.db.into(),
				table: decoded.tb.into(),
				index: decoded.ix.into(),
				fields: decoded.fd.into_owned(),
				identity: decoded
					.id
					.map(|id| Thing::from((decoded.tb, id.into_owned()))),
			})
		}
		Some(b'~') => {
			let decoded = key::graph::Graph::decode(bytes)
				.map_err(|_| invalid("invalid native graph key"))?;
			let to = Thing::from((decoded.ft, decoded.fk.clone()));
			if key::graph::Graph::new(
				decoded.ns,
				decoded.db,
				decoded.tb,
				decoded.id.clone(),
				decoded.eg,
				&to,
			)
			.encode()? != bytes
			{
				return Err(invalid("noncanonical native graph key"));
			}
			Ok(NativeHistoryKey::Graph {
				namespace: decoded.ns.into(),
				database: decoded.db.into(),
				from: Thing::from((decoded.tb, decoded.id)),
				to,
			})
		}
		Some(b'&') => {
			let decoded = key::r#ref::Ref::decode(bytes)
				.map_err(|_| invalid("invalid native reference key"))?;
			if key::r#ref::Ref::new(
				decoded.ns,
				decoded.db,
				decoded.tb,
				decoded.id.clone(),
				decoded.ft,
				decoded.ff,
				decoded.fk.clone(),
			)
			.encode()? != bytes
			{
				return Err(invalid("noncanonical native reference key"));
			}
			Ok(NativeHistoryKey::Reference {
				namespace: decoded.ns.into(),
				database: decoded.db.into(),
				from: Thing::from((decoded.tb, decoded.id)),
				to: Thing::from((decoded.ft, decoded.fk)),
			})
		}
		Some(b'!') if tail.starts_with(b"!bs") => {
			// Full-text global state uses table-local !bs{index}, unlike the
			// +{index}!tag layout of the other index internals. It contains
			// aggregate state and must be omitted and rebuilt, not retained
			// as table metadata. Match the actual Bs writer codec exactly.
			let decoded = key::index::bs::Bs::decode(bytes)
				.map_err(|_| invalid("invalid native full-text state key"))?;
			if key::index::bs::Bs::new(decoded.ns, decoded.db, decoded.tb, decoded.ix).encode()?
				!= bytes
			{
				return Err(invalid("noncanonical native full-text state key"));
			}
			Ok(NativeHistoryKey::IndexInternal {
				namespace: decoded.ns.into(),
				database: decoded.db.into(),
				table: decoded.tb.into(),
				index: decoded.ix.into(),
			})
		}
		_ => {
			validate_table_metadata(bytes, tail)?;
			Ok(NativeHistoryKey::Other)
		}
	}
}

// Only known native metadata classes can bypass document ownership. A new or
// malformed in-scope key is not an implicit permission to copy opaque payload.
macro_rules! metadata_codec {
	($bytes:expr, $scope:ident, $module:ident, $name:ident) => {{
		let decoded = key::$scope::$module::$name::decode($bytes)
			.map_err(|_| invalid("invalid native metadata key"))?;
		if decoded.encode()? != $bytes {
			return Err(invalid("noncanonical native metadata key"));
		}
	}};
}

fn validate_table_metadata(bytes: &[u8], tail: &[u8]) -> Result<(), Error> {
	match tail.get(..3) {
		Some(b"!ev") => metadata_codec!(bytes, table, ev, Ev),
		Some(b"!fd") => metadata_codec!(bytes, table, fd, Fd),
		Some(b"!ft") => metadata_codec!(bytes, table, ft, Ft),
		Some(b"!ix") => metadata_codec!(bytes, table, ix, Ix),
		Some(b"!lq") => metadata_codec!(bytes, table, lq, Lq),
		_ if tail.is_empty() => metadata_codec!(bytes, table, all, Table),
		_ => return Err(invalid("unknown native table metadata key")),
	}
	Ok(())
}

fn validate_database_metadata(bytes: &[u8], tail: &[u8]) -> Result<(), Error> {
	match tail.get(..3) {
		Some(b"!ac") => metadata_codec!(bytes, database, ac, Ac),
		Some(b"!ap") => metadata_codec!(bytes, database, ap, Ap),
		Some(b"!az") => metadata_codec!(bytes, database, az, Az),
		Some(b"!cg") => metadata_codec!(bytes, database, cg, Cg),
		Some(b"!fn") => metadata_codec!(bytes, database, fc, Fc),
		Some(b"!ml") => metadata_codec!(bytes, database, ml, Ml),
		Some(b"!pa") => metadata_codec!(bytes, database, pa, Pa),
		Some(b"!tb") => metadata_codec!(bytes, database, tb, Tb),
		Some(b"!ts") => metadata_codec!(bytes, database, ts, Ts),
		Some(b"!us") => metadata_codec!(bytes, database, us, Us),
		Some(b"!vs") => metadata_codec!(bytes, database, vs, Vs),
		_ if tail.is_empty() => metadata_codec!(bytes, database, all, All),
		_ if tail.first() == Some(&b'&') => {
			let grant = key::database::access::gr::Gr::decode(bytes)
				.map_err(|_| invalid("invalid native database access grant key"))?;
			if key::database::access::gr::new(grant.ns, grant.db, grant.ac, grant.gr).encode()?
				!= bytes
			{
				return Err(invalid("noncanonical native database access grant key"));
			}
		}
		_ if tail.first() == Some(&b'#') => {
			let feed = key::change::Cf::decode(bytes)
				.map_err(|_| invalid("invalid native change-feed key"))?;
			if key::change::Cf::new(feed.ns, feed.db, feed.vs, feed.tb).encode()? != bytes {
				return Err(invalid("noncanonical native change-feed key"));
			}
			// Recognized, but selection still requires a source-aware value codec.
		}
		_ => return Err(invalid("unknown native database metadata key")),
	}
	Ok(())
}

fn validate_index_internal(bytes: &[u8], tag: &[u8]) -> Result<(), Error> {
	macro_rules! native {
		($module:ident, $name:ident) => {{
			let decoded = key::index::$module::$name::decode(bytes)
				.map_err(|_| invalid("invalid native index-internal key"))?;
			if decoded.encode()? != bytes {
				return Err(invalid("noncanonical native index-internal key"));
			}
		}};
	}
	match tag {
		b"!bc" => native!(bc, Bc),
		b"!bd" => native!(bd, Bd),
		b"!bf" => native!(bf, Bf),
		b"!bi" => native!(bi, Bi),
		b"!bk" => native!(bk, Bk),
		b"!bl" => native!(bl, Bl),
		b"!bo" => native!(bo, Bo),
		b"!bp" => native!(bp, Bp),
		b"!bt" => native!(bt, Bt),
		b"!bu" => native!(bu, Bu),
		b"!df" => native!(df, Df),
		b"!hd" => native!(hd, Hd),
		b"!he" => native!(he, He),
		b"!hi" => native!(hi, Hi),
		b"!hl" => native!(hl, Hl),
		b"!hn" => native!(hn, HnswNode),
		b"!hs" => native!(hs, Hs),
		b"!hv" => native!(hv, Hv),
		b"!ib" => native!(ib, Ib),
		b"!ip" => native!(ip, Ip),
		b"!iu" => native!(iu, IndexCountKey),
		b"!vm" => native!(vm, Vm),
		_ => return Err(invalid("unrecognized native index-internal key class")),
	}
	Ok(())
}

/// Decode the owner stored as a native unique-index value.
pub fn unique_index_owner(bytes: &[u8]) -> Result<Thing, Error> {
	decode_native(bytes)
}

fn decode_native<T: Revisioned>(bytes: &[u8]) -> Result<T, Error> {
	let mut reader = Cursor::new(bytes);
	let value = T::deserialize_revisioned(&mut reader)
		.map_err(|_| invalid("native historical value cannot be decoded"))?;
	if reader.position() != bytes.len() as u64 {
		return Err(invalid("native historical value has trailing bytes"));
	}
	Ok(value)
}

/// Construct the exact native record key for a derived-key ownership join.
pub fn record_key(namespace: &str, database: &str, table: &str, id: &Id) -> Result<Vec<u8>, Error> {
	key::thing::new(namespace, database, table, id).encode()
}

/// Encode a native ordinary index tuple, using the same codec as IndexOperation.
/// This does not evaluate fields or authorize their retention.
pub fn ordinary_index_key(
	namespace: &str,
	database: &str,
	table: &str,
	index: &str,
	fields: &crate::sql::Array,
	identity: Option<&Id>,
) -> Result<Vec<u8>, Error> {
	key::index::Index::new(namespace, database, table, index, fields, identity).encode()
}

#[cfg(test)]
#[path = "recall_history_tests.rs"]
mod tests;
