# SurrealKV 0.9.3 Recall storage backport

Elephant pins this backport through the repository-relative `[patch.crates-io]`
entry. It retains the snapshot-publication repair and adds an owned native
checkpoint writer that preserves selected historical versions. No dependency
version is upgraded. This code does not establish source ownership, serializable
resource fencing, or complete physical erasure by itself.

## Provenance

- Crate: `surrealkv` 0.9.3 from the crates.io registry archive.
- Archive SHA-256: `08a5041979bdff8599a1d5f6cb7365acb9a79664e2a84e5c4fddac2b3969f7d1`.
- Upstream Git revision recorded in `.cargo_vcs_info.json`:
  `8cc4ca4f09a6e42195af9c40f4a0581a445fe7bf`.
- Upstream repository: <https://github.com/surrealdb/surrealkv>.
- Upstream license: Apache-2.0, preserved unchanged at
  [LICENSE](surrealkv-0.9.3/LICENSE).
- All 41 original archive files are preserved. The original source-byte
  checksums are in [surrealkv-0.9.3-upstream-sha256.json](surrealkv-0.9.3-upstream-sha256.json).
- Source-checksum manifest SHA-256: `1009301d25dd9c0c4344d9c2c61a9169c110ebe35cc36863eb333490b837a11a`.
- Five original files differ from the registry archive: `src/snapshot.rs`,
  `src/transaction.rs`, `src/indexer.rs`, `src/lib.rs` and `src/store.rs`.
  Two files are added: `src/checkpoint.rs` and `src/checkpoint_tests.rs`.
  Their complete change is
  [surrealkv-0.9.3-backport.patch](surrealkv-0.9.3-backport.patch).
- The final native component is pinned to Recall source commit
  `627af9d1e67f1ecc63a060844875199577cb382c` (maintenance chain
  `790ab3e0` → `f9da94a4` → `627af9d1`). The prior snapshot repair is retained.
  The paired server's `packaging/recall-surreal/source.lock.json` binds every
  changed or added file and all three provenance files by SHA-256.

## Engine change

`src/snapshot.rs` tags a cloned index with that index's published version,
observed under the same index read lock. Previously it used the oracle version,
which advances before the new index is published. A transaction could therefore
read old bytes while claiming the in-flight writer's newer version. After that
writer committed, the stale transaction would miss the equal-version conflict
and overwrite or archive its acknowledged update.

The snapshot iterator also explicitly names its elided borrow lifetime (`Iter<'_, ...>`). This annotation resolves Rust 1.94
`mismatched_lifetime_syntaxes`; it changes no runtime behavior.

`src/transaction.rs` adds one deterministic regression. It pauses between the
actual commit phases of version assignment and index publication, creates the
stale reader in that gap, publishes the writer, and requires the stale archive
to conflict. It exercises both versioned and unversioned indexes without timing
or scheduling assumptions. Reverting the snapshot fix makes the regression fail.
The original snapshot regression and its expectations remain. The native
checkpoint additions below account for the remaining changed files. All other
upstream file bytes are unchanged, including Cargo manifests, the standalone
lockfile, archive metadata and license.

## Native checkpoint writer

`src/checkpoint.rs` exposes a bounded raw version visitor and a selected checkpoint
writer. The caller supplies `Keep`, `Tombstone` or `Drop` for each original native
version. The writer preserves retained keys, values, transaction versions,
timestamps and soft-tombstone absence boundaries; it does not infer source
ownership. New generations receive a checked durable high-water clock so erasing
the newest or every key cannot reuse transaction versions after reopen.

`src/indexer.rs` exposes the exact underlying version iteration used by this
visitor. `src/store.rs` loads the checkpoint clock, exposes the immutable version
mode and provides checked clock publication. `src/lib.rs` registers and exports
this narrow native interface. The writer requires drained native snapshots and
compaction, an empty destination and the original absolute deadline. It never
promotes or retires a database directory. Recall's core adapter owns admission,
source-authorized selection, private index reconstruction, independent reopened
stream verification and complete old-generation retirement.

Twelve new private vendor test functions cover selected copying, retained history,
soft tombstones, erased gaps, physical log canaries, high-water preservation,
active snapshot refusal, destination/deadline errors and observer failures. They
are authored source and have not been compiled or executed in this checkpoint.
They are not automatically run by Elephant's workspace tests. The separate core
adapter's nineteen authored private tests and public workspace controls also
require their own recorded validation; this provenance update claims no runtime
or physical-erasure qualification.

Cargo validation remains centrally scheduled. When selected, it must use this
worktree's `scripts/repo-cargo`, the original resource budget and the coordinator's
idle scheduling and concurrency limits. The original source archive and all
prior validation evidence remain unchanged.

## Separate Elephant change

The engine repair alone does not fix deleted-key conflict coverage. In the
unversioned memory backend, an archive deletes the hot key. A stale retry's
SurrealQL DELETE of an archive absent in its snapshot can emit no cold-key write,
so a retry can miss the archive's surviving cold-key conflict.

Elephant's native WorkItem/WorkRun update transactions now write both existing
hot/cold identities: the temporary archive UPSERT followed by DELETE in the
same transaction forces an actual cold-key write without leaving a second
committed history record. This covers ordinary updates, status-conditional
updates and the public dependency-conditional updater. The transaction owner
and existing conflict retry behavior remain unchanged. The embedded backend
continues to use its existing single mutation owner.

That native change is outside this vendored patch. It requires the actual
repository retention races and hot/cold sink assertions in addition to the
engine unit test. Neither repair establishes general predicate serializability,
current authorization, independent recovery witnesses or a new execution journal.
