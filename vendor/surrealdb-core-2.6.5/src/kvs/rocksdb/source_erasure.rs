//! Native versionless inventory and shared typed index reconstruction.
use super::physical_checkpoint::{failure, remaining};
use super::OptimisticTransactionDB;
use crate::err::Error;
use crate::kvs::recall_history::{
	HistoryDecision, HistorySelection, MAX_HISTORY_BYTES, MAX_HISTORY_RECORDS,
};
use crate::kvs::recall_index_erasure::{self, RebuildTarget};
use crate::kvs::{
	Datastore, KeyEncode, RecallHistoryAuthority, RecallHistoryRecord, RecallHistorySnapshot,
};
use rocksdb::{Direction, IteratorMode};
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Instant;

pub(super) fn capture(
	db: &OptimisticTransactionDB,
	namespace: &str,
	database: &str,
	deadline: Instant,
) -> Result<RecallHistorySnapshot, Error> {
	// Validate scope before constructing a potentially broad key range.
	RecallHistorySnapshot::new(namespace.to_owned(), database.to_owned(), Vec::new())?;
	let prefix = crate::key::database::all::new(namespace, database).encode()?;
	let mut records = Vec::new();
	let mut bytes = 0usize;
	for item in db.iterator(IteratorMode::From(&prefix, Direction::Forward)) {
		remaining(deadline)?;
		let (key, value) = item?;
		if !key.starts_with(&prefix) {
			break;
		}
		bytes = bytes
			.checked_add(key.len())
			.and_then(|n| n.checked_add(value.len()))
			.and_then(|n| n.checked_add(64))
			.ok_or_else(|| failure("native scoped inventory overflow"))?;
		if bytes > MAX_HISTORY_BYTES || records.len() >= MAX_HISTORY_RECORDS {
			return Err(failure("native scoped inventory exceeds finite bound"));
		}
		records.push(RecallHistoryRecord {
			key: key.to_vec(),
			value: value.to_vec(),
			version: 0,
			timestamp: 0,
			tombstone: false,
			current: true,
		});
	}
	remaining(deadline)?;
	RecallHistorySnapshot::new(namespace.to_owned(), database.to_owned(), records)
}

pub(super) struct PreparedSelection {
	dropped: BTreeSet<Vec<u8>>,
	dropped_prefixes: Vec<Vec<u8>>,
	targets: Vec<RebuildTarget>,
	rebuilt_prefixes: Vec<Vec<u8>>,
	rebuilt_exact_keys: BTreeSet<Vec<u8>>,
}

impl PreparedSelection {
	pub(super) fn prepare(
		db: &OptimisticTransactionDB,
		authorities: &[RecallHistoryAuthority],
		deadline: Instant,
	) -> Result<Self, Error> {
		if authorities.is_empty() {
			return Err(failure("physical history authorities are absent"));
		}
		let mut scopes = BTreeMap::<(String, String), Vec<RecallHistoryAuthority>>::new();
		for authority in authorities {
			authority.validate_bound()?;
			scopes
				.entry((authority.namespace().into(), authority.database().into()))
				.or_default()
				.push(authority.clone());
		}
		let mut result = Self {
			dropped: BTreeSet::new(),
			dropped_prefixes: Vec::new(),
			targets: Vec::new(),
			rebuilt_prefixes: Vec::new(),
			rebuilt_exact_keys: BTreeSet::new(),
		};
		for ((ns, db_name), authorities) in scopes {
			let snapshot = capture(db, &ns, &db_name, deadline)?;
			let selection = HistorySelection::prepare_versionless_with_deadline(
				&authorities,
				&snapshot,
				deadline,
			)?;
			result
				.targets
				.extend(recall_index_erasure::collect_targets_with_deadline(
					&snapshot,
					selection.aggregate_indexes(),
					deadline,
				)?);
			for (table, index) in selection.aggregate_indexes() {
				result
					.dropped_prefixes
					.push(crate::key::index::all::new(&ns, &db_name, table, index).encode()?);
			}
			for row in &snapshot.records {
				remaining(deadline)?;
				match selection.decision(&row.key, 0, 0)? {
					HistoryDecision::Keep => {}
					HistoryDecision::Drop => {
						result.dropped.insert(row.key.clone());
					}
					HistoryDecision::Tombstone => {
						return Err(failure(
							"versionless source selection requested a historical tombstone",
						))
					}
				}
			}
		}
		for (ns, db, definition) in &result.targets {
			result.rebuilt_prefixes.push(
				crate::key::index::all::new(ns, db, &definition.what, &definition.name).encode()?,
			);
		}
		result.rebuilt_exact_keys = recall_index_erasure::rebuild_extra_keys(&result.targets)?;
		Ok(result)
	}

	pub(super) fn drops(&self, key: &[u8]) -> bool {
		self.dropped.contains(key)
			|| self
				.dropped_prefixes
				.iter()
				.any(|prefix| key.starts_with(prefix))
	}

	pub(super) fn rebuilt_key(&self, key: &[u8]) -> Result<bool, Error> {
		if self.rebuilt_exact_keys.contains(key)
			|| self
				.rebuilt_prefixes
				.iter()
				.any(|prefix| key.starts_with(prefix))
		{
			return Ok(true);
		}

		Ok(false)
	}

	pub(super) fn rebuild(
		&self,
		path: &Path,
		deadline: Instant,
		capabilities: &crate::dbs::Capabilities,
	) -> Result<(), Error> {
		if self.targets.is_empty() {
			return Ok(());
		}
		remaining(deadline)?;
		let path = path
			.to_str()
			.ok_or_else(|| failure("staging native path is not UTF8"))?;
		// Called only by the owned blocking copier after closing its stage writer.
		tokio::runtime::Handle::current().block_on(async {
			let stage = Datastore::recall_open_staging(&format!("rocksdb:{path}"), deadline)
				.await?
				.with_capabilities(capabilities.clone());
			let rebuilt =
				recall_index_erasure::rebuild_current(&stage, &self.targets, deadline).await;
			let closed = stage.recall_shutdown_staging(deadline).await;
			drop(stage);
			rebuilt?;
			closed?;
			remaining(deadline)?;
			Ok(())
		})
	}
}
