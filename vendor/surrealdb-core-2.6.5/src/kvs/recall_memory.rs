//! Versionless erasure within the original memory datastore's exclusive owner.
//! The private view shares the native store, but no ordinary caller can acquire it.
use super::recall_history::{HistoryDecision, HistorySelection};
use super::recall_index_erasure;
use super::{Datastore, KeyEncode, LockType, RecallHistoryAuthority, TransactionType};
use crate::err::Error;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

fn remaining(deadline: Instant) -> Result<(), Error> {
	if Instant::now() >= deadline {
		return Err(Error::Ds("original memory erasure deadline expired".into()));
	}
	Ok(())
}

async fn protected(
	view: &Datastore,
	prefixes: &[Vec<u8>],
	exact_keys: &BTreeSet<Vec<u8>>,
	deadline: Instant,
) -> Result<String, Error> {
	let tx = view
		.transaction(TransactionType::Read, LockType::Optimistic)
		.await?;
	let result = async {
		let mut next = Some(Vec::<u8>::new()..vec![0xff]);
		let mut digest = Sha256::new();
		digest.update(b"recall-memory-protected-native/v1\0");
		while let Some(range) = next {
			remaining(deadline)?;
			let batch = tx.batch_keys_vals(range, 256, None).await?;
			next = batch.next;
			for (key, value) in batch.result {
				if exact_keys.contains(&key)
					|| prefixes.iter().any(|prefix| key.starts_with(prefix))
				{
					continue;
				}
				digest.update((key.len() as u64).to_be_bytes());
				digest.update(key);
				digest.update((value.len() as u64).to_be_bytes());
				digest.update(value);
			}
		}
		Ok(format!("{:x}", digest.finalize()))
	}
	.await;
	tx.cancel().await?;
	result
}

pub(super) async fn erase(
	view: &Datastore,
	authorities: &[RecallHistoryAuthority],
	deadline: Instant,
) -> Result<(), Error> {
	remaining(deadline)?;
	let mut scopes = BTreeMap::<(String, String), Vec<RecallHistoryAuthority>>::new();
	for authority in authorities {
		scopes
			.entry((authority.namespace().into(), authority.database().into()))
			.or_default()
			.push(authority.clone());
	}
	let mut dropped = BTreeSet::new();
	let mut prefixes = Vec::new();
	let mut targets = Vec::new();
	// Complete every policy check against the original memory state before mutation.
	for ((ns, db), authorities) in scopes {
		let snapshot = view.recall_memory_snapshot(deadline, &ns, &db).await?;
		let selection =
			HistorySelection::prepare_versionless_with_deadline(&authorities, &snapshot, deadline)?;
		targets.extend(recall_index_erasure::collect_targets_with_deadline(
			&snapshot,
			selection.aggregate_indexes(),
			deadline,
		)?);
		let scope_prefixes = selection
			.aggregate_indexes()
			.iter()
			.map(|(table, index)| crate::key::index::all::new(&ns, &db, table, index).encode())
			.collect::<Result<Vec<_>, _>>()?;
		for row in &snapshot.records {
			remaining(deadline)?;
			match selection.decision(&row.key, 0, 0)? {
				HistoryDecision::Drop => {
					dropped.insert(row.key.clone());
				}
				HistoryDecision::Keep => {}
				HistoryDecision::Tombstone => {
					return Err(Error::Ds(
						"memory erasure cannot invent a historical tombstone".into(),
					))
				}
			}
			if scope_prefixes
				.iter()
				.any(|prefix| row.key.starts_with(prefix))
			{
				dropped.insert(row.key.clone());
			}
		}
		prefixes.extend(scope_prefixes);
	}
	let exact_keys = recall_index_erasure::rebuild_extra_keys(&targets)?;
	remaining(deadline)?;
	let tx = view
		.transaction(TransactionType::Write, LockType::Optimistic)
		.await?;
	let removed = async {
		for key in dropped {
			remaining(deadline)?;
			tx.clr(key).await?;
		}
		Ok::<(), Error>(())
	}
	.await;
	if let Err(error) = removed {
		tx.cancel().await?;
		return Err(error);
	}
	tx.commit().await?;
	drop(tx);
	let before = protected(view, &prefixes, &exact_keys, deadline).await?;
	recall_index_erasure::rebuild_current(view, &targets, deadline).await?;
	if protected(view, &prefixes, &exact_keys, deadline).await? != before {
		return Err(Error::Ds(
			"memory index reconstruction changed unrelated native data".into(),
		));
	}
	remaining(deadline)?;
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::time::Duration;

	#[tokio::test]
	async fn protected_fingerprint_exempts_only_exact_selected_full_text_state() {
		let view = Datastore::new("memory").await.unwrap();
		let deadline = Instant::now() + Duration::from_secs(10);
		let prefix = b"/*ns\0*db\0*entity\0+search\0".to_vec();
		let state = b"/*ns\0*db\0*entity\0!bssearch\0".to_vec();
		let prefixes = vec![prefix];
		let exact_keys = BTreeSet::from([state.clone()]);
		let original = protected(&view, &prefixes, &exact_keys, deadline)
			.await
			.unwrap();
		let tx = view
			.transaction(TransactionType::Write, LockType::Optimistic)
			.await
			.unwrap();
		tx.set(state, vec![1, 2, 3], None).await.unwrap();
		tx.commit().await.unwrap();
		drop(tx);
		assert_eq!(
			protected(&view, &prefixes, &exact_keys, deadline)
				.await
				.unwrap(),
			original
		);
		// Every scope component, suffix and ordinary record/schema key stays
		// protected. An exact Bs exception must never become a table prefix.
		for key in [
			b"/*ns\0*db\0*entity\0!bssearch\0extra".as_slice(),
			b"/*other\0*db\0*entity\0!bssearch\0",
			b"/*ns\0*other\0*entity\0!bssearch\0",
			b"/*ns\0*db\0*other\0!bssearch\0",
			b"/*ns\0*db\0*entity\0!bsother\0",
			b"/*ns\0*db\0*entity\0!ixsearch\0",
			b"/*ns\0*db\0*entity\0*independent\0",
		] {
			let before = protected(&view, &prefixes, &exact_keys, deadline)
				.await
				.unwrap();
			let tx = view
				.transaction(TransactionType::Write, LockType::Optimistic)
				.await
				.unwrap();
			tx.set(key.to_vec(), vec![4, 5, 6], None).await.unwrap();
			tx.commit().await.unwrap();
			drop(tx);
			assert_ne!(
				protected(&view, &prefixes, &exact_keys, deadline)
					.await
					.unwrap(),
				before
			);
		}
	}
}
