# SurrealKV 0.9.3 snapshot-publication backport

Elephant pins this narrow backport through the repository-relative
`[patch.crates-io]` entry. No dependency version is upgraded. This corrects an
acknowledged-update loss in the existing retention/retry race; it does not
claim serializable resource or grant fencing.

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
- Exactly two files differ from the registry archive. Their complete change is
  [surrealkv-0.9.3-backport.patch](surrealkv-0.9.3-backport.patch).

## Engine change

`src/snapshot.rs` tags a cloned index with that index's published version,
observed under the same index read lock. Previously it used the oracle version,
which advances before the new index is published. A transaction could therefore
read old bytes while claiming the in-flight writer's newer version. After that
writer committed, the stale transaction would miss the equal-version conflict
and overwrite or archive its acknowledged update.

`src/transaction.rs` adds one deterministic regression. It pauses between the
actual commit phases of version assignment and index publication, creates the
stale reader in that gap, publishes the writer, and requires the stale archive
to conflict. It exercises both versioned and unversioned indexes without timing
or scheduling assumptions. Reverting the snapshot fix makes the regression fail.
All other upstream file bytes are unchanged, including its standalone lockfile.

Run the upstream library suite separately from Elephant's workspace:

```bash
RUST_LANE_ID=retention-vendor-tests ./scripts/repo-cargo test --manifest-path vendor/surrealkv-0.9.3/Cargo.toml --locked --lib
```

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
