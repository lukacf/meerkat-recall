//! Reconstruct current derived indexes in an unpublished erasure generation.
//!
//! Historical record selection belongs to the erasure authority. The generation
//! owner must first exclude every old aggregate and builder-internal version,
//! retain authorized record history, and keep this datastore inaccessible to
//! ordinary readers and writers until verification and durable promotion finish.
//! This module neither authorizes erasure nor publishes a storage generation.

use super::recall_history::{decode_history_key, NativeHistoryKey, RecallHistorySnapshot};
use crate::ctx::MutableContext;
use crate::dbs::{Options, Session};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::index::IndexOperation;
use crate::key::index::{all, bs::Bs, df::Df};
use crate::key::root::ic::IndexCompactionKey;
use crate::key::thing;
use crate::kvs::{Datastore, KeyDecode, KeyEncode, LockType, Transaction, TransactionType};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Index, Thing, Value};
use reblessive::TreeStack;
use revision::Revisioned;
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Namespace, database, and the exact retained current index definition.
pub(super) type RebuildTarget = (String, String, Arc<DefineIndexStatement>);

/// Exact native builder keys outside the selected `+index` prefix. Only a
/// retained current Search definition writes table-local full-text state.
/// Generation guards must use equality with these keys, never prefix matching.
pub(super) fn rebuild_extra_keys(targets: &[RebuildTarget]) -> Result<BTreeSet<Vec<u8>>, Error> {
    targets
        .iter()
        .filter(|(_, _, definition)| matches!(&definition.index, Index::Search(_)))
        .map(|(ns, db, definition)| Bs::new(ns, db, &definition.what, &definition.name).encode())
        .collect()
}

/// Resolve reconstruction obligations against the same complete, verified
/// native inventory used by the source-version selector. Removed definitions
/// have no current index to rebuild; their old internal keys are still excluded
/// by the selector. Scalar definitions are reconciled without discarding their
/// retained ordinary index-entry history.
#[cfg(test)]
pub(super) fn collect_targets(
    snapshot: &RecallHistorySnapshot,
    requested: &BTreeSet<(String, String)>,
) -> Result<Vec<RebuildTarget>, Error> {
    collect_targets_inner(snapshot, requested, None)
}

/// Runtime discovery consumes the original physical-checkpoint deadline.
pub(super) fn collect_targets_with_deadline(
    snapshot: &RecallHistorySnapshot,
    requested: &BTreeSet<(String, String)>,
    deadline: Instant,
) -> Result<Vec<RebuildTarget>, Error> {
    collect_targets_inner(snapshot, requested, Some(deadline))
}

fn collect_targets_inner(
    snapshot: &RecallHistorySnapshot,
    requested: &BTreeSet<(String, String)>,
    deadline: Option<Instant>,
) -> Result<Vec<RebuildTarget>, Error> {
    let check_deadline = || match deadline {
        Some(deadline) => remaining(deadline).map(|_| ()),
        None => Ok(()),
    };
    check_deadline()?;
    snapshot.verify()?;
    check_deadline()?;
    let mut expected = BTreeMap::new();
    for (table, name) in requested {
        check_deadline()?;
        expected.insert(
            crate::key::table::ix::new(&snapshot.namespace, &snapshot.database, table, name)
                .encode()?,
            (table, name),
        );
    }
    let mut current = BTreeMap::new();
    for row in &snapshot.records {
        check_deadline()?;
        if row.current
            && expected.contains_key(&row.key)
            && current.insert(row.key.as_slice(), row).is_some()
        {
            return Err(failure("multiple current index definitions"));
        }
    }
    let mut targets = Vec::new();
    for (key, (table, name)) in expected {
        check_deadline()?;
        let Some(row) = current.get(key.as_slice()) else {
            continue;
        };
        if row.tombstone {
            continue;
        }
        let mut bytes = Cursor::new(row.value.as_slice());
        let definition = DefineIndexStatement::deserialize_revisioned(&mut bytes)
            .map_err(|_| failure("invalid current index definition"))?;
        if bytes.position() != row.value.len() as u64
            || definition.what.as_str() != table
            || definition.name.as_str() != name
        {
            return Err(failure(
                "index definition does not match its native identity",
            ));
        }
        targets.push((
            snapshot.namespace.clone(),
            snapshot.database.clone(),
            Arc::new(definition),
        ));
    }
    check_deadline()?;
    Ok(targets)
}

fn failure(message: &str) -> Error {
    Error::Ds(format!("Recall derived-index reconstruction: {message}"))
}

fn remaining(deadline: Instant) -> Result<Duration, Error> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or_else(|| failure("original checkpoint deadline expired"))
}

fn aggregate(index: &Index) -> bool {
    matches!(
        index,
        Index::Search(_) | Index::Hnsw(_) | Index::MTree(_) | Index::Count
    )
}

fn prefix_range(ns: &str, db: &str, index: &DefineIndexStatement) -> Result<Range<Vec<u8>>, Error> {
    let start = all::new(ns, db, &index.what, &index.name).encode()?;
    let mut end = start.clone();
    end.push(0xff);
    Ok(start..end)
}

async fn check_filtered_prefix(
    staging: &Datastore,
    tx: &Transaction,
    ns: &str,
    db: &str,
    index: &DefineIndexStatement,
    deadline: Instant,
) -> Result<(), Error> {
    let mut next = Some(prefix_range(ns, db, index)?);
    while let Some(range) = next {
        remaining(deadline)?;
        let keys = match tx.batch_keys_vals_versions(range.clone(), 256).await {
            Ok(batch) => {
                next = batch.next;
                batch
                    .result
                    .into_iter()
                    .map(|(key, _, _, _)| key)
                    .collect::<Vec<_>>()
            }
            // RocksDB has no version stream. This sealed datastore identity is
            // not a user-supplied label. A failed SurrealKV history read must
            // never become a current-only absence check.
            Err(Error::UnsupportedVersionedQueries) if staging.to_string() == "rocksdb" => {
                let batch = tx.batch_keys_vals(range, 256, None).await?;
                next = batch.next;
                batch.result.into_iter().map(|(key, _)| key).collect()
            }
            Err(error) => return Err(error),
        };
        for key in keys {
            remaining(deadline)?;
            // A current aggregate definition may have retained scalar history
            // under this name. Native ordinary entries use `*`; every aggregate
            // or builder-internal codec uses `!`. Only the exact typed ordinary
            // key is permitted, after the caller's source-authorized selection.
            match decode_history_key(&key)? {
                NativeHistoryKey::Index {
                    namespace,
                    database,
                    table,
                    index: name,
                    ..
                } if namespace == ns
                    && database == db
                    && table == index.what.as_str()
                    && name == index.name.as_str() => {}
                _ => {
                    return Err(failure(
                        "original aggregate or builder history remains in staging",
                    ))
                }
            }
        }
    }
    Ok(())
}

/// Rebuild affected aggregates using the same field evaluation and index
/// operations as `REBUILD INDEX`, without spawning builders or deferred daemons.
///
/// The caller supplies a private staging datastore after source-authorized
/// history filtering. Every old aggregate or builder-internal version must be
/// absent, including historical tombstones and orphan keys. Exact ordinary index
/// entries may remain even when the current definition changed from scalar to
/// aggregate; these disjoint key classes cannot feed aggregate reads. Unknown
/// key shapes always fail. Existing ordinary scalar versions survive
/// reconciliation; current coverage is refreshed. Record history is never
/// rewritten here. All targets are validated before writes;
/// all reconstructed indexes commit together. The generation owner must still
/// close, independently verify, and physically retire its original generation.
pub(super) async fn rebuild_current(
    staging: &Datastore,
    targets: &[RebuildTarget],
    deadline: Instant,
) -> Result<(), Error> {
    remaining(deadline)?;
    if targets.is_empty() {
        return Ok(());
    }
    let tx = Arc::new(
        staging
            .transaction(TransactionType::Write, LockType::Optimistic)
            .await?,
    );
    let result = rebuild_transaction(staging, &tx, targets, deadline).await;
    if let Err(error) = result {
        // Cancellation must drain even if the original work deadline has expired.
        // A failed cancellation also prevents the owner from promoting this stage.
        tx.cancel().await?;
        return Err(error);
    }
    if let Err(error) = remaining(deadline) {
        tx.cancel().await?;
        return Err(error);
    }
    // Do not abandon a commit and claim cancellation. The native owner retains
    // custody until it finishes; a late acknowledgement cannot qualify the stage.
    tx.commit().await?;
    remaining(deadline)?;
    Ok(())
}

async fn rebuild_transaction(
    staging: &Datastore,
    tx: &Arc<Transaction>,
    targets: &[RebuildTarget],
    deadline: Instant,
) -> Result<(), Error> {
    let mut seen = BTreeSet::new();
    for (ns, db, index) in targets {
        remaining(deadline)?;
        if ns.is_empty() || db.is_empty() {
            return Err(failure("target is not a scoped index"));
        }
        let range = prefix_range(ns, db, index)?;
        if !seen.insert(range.start.clone()) {
            return Err(failure("duplicate aggregate target"));
        }
        let current = tx.get_tb_index(ns, db, &index.what, &index.name).await?;
        if current.as_ref() != index.as_ref() {
            return Err(failure("current index definition changed"));
        }
        check_filtered_prefix(staging, tx, ns, db, index, deadline).await?;
    }

    for (ns, db, index) in targets {
        remaining(deadline)?;
        if !aggregate(&index.index) {
            // Clear only current scalar entries. SurrealKV `del` creates a
            // soft tombstone; it does not erase any retained older version.
            // This also removes stale keys left by deferred field changes.
            let mut next = Some(prefix_range(ns, db, index)?);
            while let Some(range) = next {
                remaining(deadline)?;
                let batch = tx.batch_keys_vals(range, 256, None).await?;
                next = batch.next;
                for (key, _) in batch.result {
                    remaining(deadline)?;
                    tx.del(key).await?;
                }
            }
        }
        let node_id = staging.id();
        let opt = Options::new()
            .with_id(node_id)
            .with_ns(Some(ns.as_str().into()))
            .with_db(Some(db.as_str().into()))
            .with_auth(Session::owner().au)
            .with_perms(false);
        // Fresh caches are essential: a live HNSW cache may still hold elements
        // from the retired generation even after its on-disk keys were removed.
        let mut context = MutableContext::background();
        context.set_transaction(tx.clone());
        context.add_capabilities(Arc::new(staging.get_capabilities().clone()));
        context.add_timeout(remaining(deadline)?)?;
        let context = context.freeze();
        let mut next =
            Some(thing::prefix(ns, db, &index.what)?..thing::suffix(ns, db, &index.what)?);
        let mut stack = TreeStack::new();
        let mut compact_count = false;
        while let Some(range) = next {
            remaining(deadline)?;
            let batch = tx.batch_keys_vals(range, 256, None).await?;
            next = batch.next;
            for (key, value) in batch.result {
                remaining(deadline)?;
                let record = thing::Thing::decode(&key)?;
                if record.ns != ns.as_str()
                    || record.db != db.as_str()
                    || record.tb != index.what.as_str()
                    || record.encode()? != key
                {
                    return Err(failure("record key escaped the selected table"));
                }
                let value: Value = revision::from_slice(&value)?;
                let rid: Arc<Thing> = Thing::from((record.tb, record.id)).into();
                let doc = CursorDoc::new(Some(rid.clone()), None, value);
                let values = stack
                    .enter(|stk| Document::build_opt_values(stk, &context, &opt, index, &doc))
                    .finish()
                    .await?;
                let mut operation = IndexOperation::new(&context, &opt, index, None, values, &rid);
                stack
                    .enter(|stk| operation.compute(stk, &mut compact_count))
                    .finish()
                    .await?;
            }
        }
        remaining(deadline)?;
        if compact_count {
            let key = IndexCompactionKey::new(
                Cow::Borrowed(ns),
                Cow::Borrowed(db),
                Cow::Borrowed(&index.what),
                Cow::Borrowed(&index.name),
                node_id,
                Uuid::new_v4(),
            );
            IndexOperation::index_count_compaction(&key, tx).await?;
        }
        if index.defer {
            tx.set(
                Df::new(ns, db, &index.what, &index.name),
                revision::to_vec(&true)?,
                None,
            )
            .await?;
        }
    }
    remaining(deadline)?;
    Ok(())
}

#[cfg(test)]
#[path = "recall_index_erasure_tests.rs"]
mod tests;
