use super::{
	decode_native, invalid, record_key, unique_index_owner, NativeHistoryKey,
	RecallHistoryAuthority, RecallHistoryRecord, RecallHistorySnapshot, RecordVersionDecision,
};
use crate::err::Error;
use crate::kvs::KeyEncode;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Index, Thing};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

fn remaining(deadline: Option<Instant>) -> Result<(), Error> {
	if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
		return Err(invalid("original native history deadline expired"));
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryDecision {
	Keep,
	Tombstone,
	Drop,
}

/// Immutable decisions over one exact inspected native stream.
/// No raw historical value or HMAC key is retained in this plan.
pub struct HistorySelection {
	decisions: BTreeMap<(Vec<u8>, u64, u64), HistoryDecision>,
	aggregate_indexes: BTreeSet<(String, String)>,
}

impl HistorySelection {
	/// Prepare while the original engine's exclusive maintenance owner is held.
	/// The snapshot must have just been re-read from that engine; its fingerprint
	/// must equal every authority's earlier journal-bound inspection.
	pub fn prepare(
		authorities: &[RecallHistoryAuthority],
		snapshot: &RecallHistorySnapshot,
	) -> Result<Self, Error> {
		Self::prepare_inner(authorities, snapshot, false, None)
	}

	/// Runtime entrypoint: propagate the owning operation's original deadline.
	pub fn prepare_with_deadline(
		authorities: &[RecallHistoryAuthority],
		snapshot: &RecallHistorySnapshot,
		deadline: Instant,
	) -> Result<Self, Error> {
		Self::prepare_inner(authorities, snapshot, false, Some(deadline))
	}

	/// Versionless engines have no independently exposed historical owner values.
	pub fn prepare_versionless(
		authorities: &[RecallHistoryAuthority],
		snapshot: &RecallHistorySnapshot,
	) -> Result<Self, Error> {
		if snapshot.records.iter().any(|row| !row.current) {
			return Err(invalid("versionless selection received historical records"));
		}
		Self::prepare_inner(authorities, snapshot, true, None)
	}

	pub fn prepare_versionless_with_deadline(
		authorities: &[RecallHistoryAuthority],
		snapshot: &RecallHistorySnapshot,
		deadline: Instant,
	) -> Result<Self, Error> {
		remaining(Some(deadline))?;
		if snapshot.records.iter().any(|row| !row.current) {
			return Err(invalid("versionless selection received historical records"));
		}
		Self::prepare_inner(authorities, snapshot, true, Some(deadline))
	}

	fn prepare_inner(
		authorities: &[RecallHistoryAuthority],
		snapshot: &RecallHistorySnapshot,
		versionless: bool,
		deadline: Option<Instant>,
	) -> Result<Self, Error> {
		remaining(deadline)?;
		snapshot.verify()?;
		if authorities.is_empty() {
			return Err(invalid("historical erasure authority is absent"));
		}
		for authority in authorities {
			remaining(deadline)?;
			authority.validate_bound()?;
			if authority.namespace != snapshot.namespace
				|| authority.database != snapshot.database
				|| authority.history_fingerprint.as_ref() != Some(&snapshot.fingerprint)
			{
				return Err(invalid(
					"historical authority and original scoped stream differ",
				));
			}
		}
		let mut result = Self {
			decisions: BTreeMap::new(),
			aggregate_indexes: BTreeSet::new(),
		};
		let mut records: BTreeMap<Vec<u8>, Vec<&RecallHistoryRecord>> = BTreeMap::new();
		let mut all_versions: BTreeMap<Vec<u8>, Vec<&RecallHistoryRecord>> = BTreeMap::new();
		let mut seen_controls = vec![BTreeSet::new(); authorities.len()];
		// First decide documents; derived keys join these historical versions.
		for row in &snapshot.records {
			remaining(deadline)?;
			all_versions.entry(row.key.clone()).or_default().push(row);
			if !matches!(row.decoded_key()?, NativeHistoryKey::Record { .. }) {
				continue;
			}
			records.entry(row.key.clone()).or_default().push(row);
			let mut erase = false;
			let mut retain_control = false;
			for (index, authority) in authorities.iter().enumerate() {
				remaining(deadline)?;
				let commitment = authority.version_commitment(row)?;
				if authority.erased_versions.contains(&commitment) {
					erase = true;
				}
				if authority.retained_controls.contains(&commitment) {
					retain_control = true;
					seen_controls[index].insert(commitment);
				}
			}
			if retain_control && (row.tombstone || erase) {
				return Err(invalid("historical control retention is contradictory"));
			}
			for authority in authorities {
				remaining(deadline)?;
				match authority.classify_record_version(
					&row.key,
					&row.value,
					row.tombstone,
					row.current,
					retain_control,
				)? {
					RecordVersionDecision::Keep => {}
					RecordVersionDecision::EraseValue => erase = true,
					RecordVersionDecision::UnscopedRecord if retain_control => {}
					RecordVersionDecision::UnscopedRecord => {
						return Err(invalid(
							"unscoped historical record lacks exact control policy",
						))
					}
					RecordVersionDecision::NotRecord => {
						return Err(invalid("native history classification changed"))
					}
				}
			}
			if erase && (row.current || row.tombstone) {
				return Err(invalid(
					"causal erasure proof targets current state or tombstone",
				));
			}
			result.insert(
				row,
				if erase {
					HistoryDecision::Tombstone
				} else {
					HistoryDecision::Keep
				},
			);
		}
		for (index, authority) in authorities.iter().enumerate() {
			if seen_controls[index] != authority.retained_controls {
				return Err(invalid(
					"original exact-control commitment is absent from native history",
				));
			}
		}
		// A key disappears only if no independent value at any version survives.
		for history in records.values() {
			remaining(deadline)?;
			if history.iter().all(|row| row.tombstone) {
				let NativeHistoryKey::Record { identity, .. } = history[0].decoded_key()? else {
					return Err(invalid("native record group classification changed"));
				};
				for authority in authorities {
					if authority.erased_identity(&identity)? {
						for row in history {
							result.insert(row, HistoryDecision::Drop);
						}
						break;
					}
				}
			}
			result.finish_identity(history)?;
		}
		let mut derived: BTreeMap<Vec<u8>, Vec<&RecallHistoryRecord>> = BTreeMap::new();
		for row in &snapshot.records {
			remaining(deadline)?;
			let decoded = row.decoded_key()?;
			let decision = match decoded {
				NativeHistoryKey::Record { .. } => continue,
				NativeHistoryKey::IndexInternal { table, index, .. } => {
					// G rebuilds this complete aggregate prefix from current
					// retained documents. Copying even the latest old node can
					// retain retired terms or stale HNSW elements.
					result.aggregate_indexes.insert((table, index));
					HistoryDecision::Drop
				}
				NativeHistoryKey::Index { table, index, .. } if versionless => {
					// No historical scalar state is exposed by this engine.
					// Omit the complete old index before writing the stage:
					// writing then deleting stale entries would preserve their
					// payload in the new WAL and native file metadata.
					result.aggregate_indexes.insert((table, index));
					HistoryDecision::Drop
				}
				NativeHistoryKey::Index {
					namespace,
					database,
					table,
					index,
					identity,
					..
				} => {
					let unique = identity.is_none();
					let owner = if !row.tombstone {
						let stored = unique_index_owner(&row.value)?;
						if stored.tb != table || identity.as_ref().is_some_and(|id| id != &stored) {
							return Err(invalid("native index key and owner value differ"));
						}
						stored
					} else if let Some(identity) = identity {
						identity
					} else {
						// A unique-index deletion stores no owner value. The
						// preceding actual version supplies its exact owner.
						let prior = all_versions
							.get(&row.key)
							.ok_or_else(|| invalid("native index history is absent"))?
							.iter()
							.copied()
							.filter(|candidate| {
								candidate.key == row.key
									&& !candidate.tombstone && candidate.version <= row.version
							})
							.max_by_key(|candidate| (candidate.version, candidate.timestamp))
							.ok_or_else(|| {
								invalid("historical unique-index tombstone has no owner")
							})?;
						unique_index_owner(&prior.value)?
					};
					if owner.tb != table {
						return Err(invalid("native index deletion owner table differs"));
					}
					let decision = result.scalar_decision(
						&records,
						&all_versions,
						row,
						&namespace,
						&database,
						&index,
						&owner,
						unique,
						deadline,
					)?;
					if decision != HistoryDecision::Keep {
						result.aggregate_indexes.insert((table, index));
					}
					decision
				}
				NativeHistoryKey::Graph {
					namespace,
					database,
					from,
					to,
				}
				| NativeHistoryKey::Reference {
					namespace,
					database,
					from,
					to,
				} => {
					let first = result.owner_decision(
						&records,
						row,
						&namespace,
						&database,
						&from,
						authorities,
						versionless,
					)?;
					let second = result.owner_decision(
						&records,
						row,
						&namespace,
						&database,
						&to,
						authorities,
						versionless,
					)?;
					if first == HistoryDecision::Keep && second == HistoryDecision::Keep {
						HistoryDecision::Keep
					} else {
						HistoryDecision::Tombstone
					}
				}
				NativeHistoryKey::Other => {
					let prefix =
						crate::key::database::all::new(&snapshot.namespace, &snapshot.database);
					use crate::kvs::KeyEncode;
					if row.key.get(prefix.encode()?.len()) == Some(&b'#') {
						return Err(invalid(
							"native change-feed history requires its source-aware codec",
						));
					}
					// Database/table definitions, clocks and independent global
					// bookkeeping are copied exactly; no modified-key collapse.
					HistoryDecision::Keep
				}
			};
			result.insert(row, decision);
			if !matches!(
				row.decoded_key()?,
				NativeHistoryKey::IndexInternal { .. } | NativeHistoryKey::Other
			) {
				derived.entry(row.key.clone()).or_default().push(row);
			}
		}
		for history in derived.values() {
			remaining(deadline)?;
			result.finish_identity(history)?;
		}
		remaining(deadline)?;
		Ok(result)
	}

	fn insert(&mut self, row: &RecallHistoryRecord, decision: HistoryDecision) {
		self.decisions
			.insert((row.key.clone(), row.version, row.timestamp), decision);
	}

	/// Deferred builders commit captured field tuples in a later transaction.
	/// A temporal join to the most recent owner therefore cannot prove retention.
	/// Require exact native key evidence from inspected document/schema intervals.
	#[allow(clippy::too_many_arguments)]
	fn scalar_decision(
		&self,
		records: &BTreeMap<Vec<u8>, Vec<&RecallHistoryRecord>>,
		all_versions: &BTreeMap<Vec<u8>, Vec<&RecallHistoryRecord>>,
		derived: &RecallHistoryRecord,
		namespace: &str,
		database: &str,
		index: &str,
		owner: &Thing,
		unique: bool,
		deadline: Option<Instant>,
	) -> Result<HistoryDecision, Error> {
		let owner_key = record_key(namespace, database, &owner.tb, &owner.id)?;
		let history = records
			.get(&owner_key)
			.ok_or_else(|| invalid("native scalar history has no inspected owner"))?;
		let definition_key =
			crate::key::table::ix::new(namespace, database, &owner.tb, index).encode()?;
		let definitions = all_versions
			.get(&definition_key)
			.ok_or_else(|| invalid("native scalar history has no inspected definition"))?;
		let mut independent = false;
		let mut erased = false;
		for (definition_position, definition_row) in definitions.iter().enumerate() {
			remaining(deadline)?;
			if definition_row.tombstone || definition_row.version > derived.version {
				continue;
			}
			let definition: DefineIndexStatement = decode_native(&definition_row.value)?;
			if definition.what.as_str() != owner.tb || definition.name.as_str() != index {
				return Err(invalid("native scalar definition identity differs"));
			}
			if !matches!(
				(&definition.index, unique),
				(Index::Uniq, true) | (Index::Idx, false)
			) {
				continue;
			}
			let definition_end = definitions
				.get(definition_position + 1)
				.map(|row| row.version);
			for (record_position, record) in history.iter().enumerate() {
				remaining(deadline)?;
				if record.tombstone || record.version > derived.version {
					continue;
				}
				// The record value and scalar definition must actually coexist.
				// A value superseded before this index existed is not a witness.
				let start = record.version.max(definition_row.version);
				let record_end = history.get(record_position + 1).map(|row| row.version);
				if definition_end.is_some_and(|end| start >= end)
					|| record_end.is_some_and(|end| start >= end)
				{
					continue;
				}
				let document = record.decoded_value()?;
				let keys = crate::kvs::recall_scalar_projection::scalar_record_keys(
					namespace,
					database,
					&definition,
					owner,
					&document,
					deadline,
				)?;
				remaining(deadline)?;
				if keys.contains(&derived.key) {
					match self.decision(&record.key, record.version, record.timestamp)? {
						HistoryDecision::Keep => independent = true,
						HistoryDecision::Tombstone | HistoryDecision::Drop => erased = true,
					}
				}
			}
		}
		match (independent, erased) {
			(true, _) => Ok(HistoryDecision::Keep),
			(false, true) => Ok(HistoryDecision::Tombstone),
			(false, false) => Err(invalid(
				"native scalar tuple has no exact inspected field evidence",
			)),
		}
	}

	fn finish_identity(&mut self, history: &[&RecallHistoryRecord]) -> Result<(), Error> {
		let mut affected = false;
		let mut independent = false;
		for row in history {
			let decision = self.decision(&row.key, row.version, row.timestamp)?;
			affected |= decision != HistoryDecision::Keep;
			independent |= !row.tombstone && decision == HistoryDecision::Keep;
		}
		if affected && !independent {
			for row in history {
				self.insert(row, HistoryDecision::Drop);
			}
		}
		Ok(())
	}

	fn owner_decision(
		&self,
		histories: &BTreeMap<Vec<u8>, Vec<&RecallHistoryRecord>>,
		derived: &RecallHistoryRecord,
		namespace: &str,
		database: &str,
		owner: &Thing,
		authorities: &[RecallHistoryAuthority],
		versionless: bool,
	) -> Result<HistoryDecision, Error> {
		let key = record_key(namespace, database, &owner.tb, &owner.id)?;
		let Some(history) = histories.get(&key) else {
			if versionless {
				for authority in authorities {
					if authority.namespace == namespace
						&& authority.database == database
						&& authority.erased_identity(owner)?
					{
						return Ok(HistoryDecision::Drop);
					}
				}
			}
			return Err(invalid("native derived history has no record owner"));
		};
		// Document and its index effects share a native transaction version.
		// For a deletion, its empty record tombstone cannot authorize payload;
		// use the last actual value that the deletion retired.
		let origin = history
			.iter()
			.copied()
			.filter(|row| {
				!row.tombstone
					&& if derived.tombstone {
						row.version < derived.version
					} else {
						row.version <= derived.version
					}
			})
			.max_by_key(|row| (row.version, row.timestamp));
		let Some(origin) = origin else {
			if versionless {
				for authority in authorities {
					if authority.erased_identity(owner)? {
						return Ok(HistoryDecision::Drop);
					}
				}
			}
			return Err(invalid("native derived history precedes its record owner"));
		};
		Ok(
			match self.decision(&origin.key, origin.version, origin.timestamp)? {
				HistoryDecision::Keep => HistoryDecision::Keep,
				HistoryDecision::Tombstone | HistoryDecision::Drop => HistoryDecision::Tombstone,
			},
		)
	}

	/// Missing keys/versions are an error, never an implicit retention decision.
	pub fn decision(
		&self,
		key: &[u8],
		version: u64,
		timestamp: u64,
	) -> Result<HistoryDecision, Error> {
		self.decisions
			.get(&(key.to_vec(), version, timestamp))
			.copied()
			.ok_or_else(|| invalid("native version was not in the original inspected selection"))
	}

	/// Mandatory current-index reconstruction in the isolated staging database.
	/// Includes every decoded ordinary index on genuinely versionless engines;
	/// versioned engines preserve independently retained ordinary index history.
	pub fn aggregate_indexes(&self) -> &BTreeSet<(String, String)> {
		&self.aggregate_indexes
	}
}
