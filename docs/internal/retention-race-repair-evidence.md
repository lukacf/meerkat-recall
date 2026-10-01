# Retention retry/archive race repair evidence

This is a separate repair of an existing storage race reproduced on clean main
`1aefaad2de68535ca5b588f58e8ed1ae1bf5b56a`. It is not completion of ADR-001,
Elephant federation, governed entry, or all workspace/CI gates.

## Baseline and failed sink

The frozen authorization prerequisite at
`feae4da94ab5d2ed5a1714b0bea2a446bb6763b4` failed its real full-workspace test gate:

```bash
RUST_LANE_ID=adr-prerequisite-tests ./scripts/repo-cargo test --workspace --lib --bins
```

`/tmp/adr-001-elephant-workspace-tests.log` records 360 storage tests passed,
1 failed and 7 ignored. The failed retention race acknowledged a Runnable retry
but read Failed afterward. The preserved prerequisite executable
`/tmp/adr-001-elephant-storage-feae4da` has SHA-256
`896775628cbf2c3284e9439920a2146791f96163b6933d7b36770d0dd405187b`;
its direct repetition failed on run 2.

A clean-main rebuild used the identical workspace feature graph:

```bash
RUST_LANE_ID=adr-prerequisite-tests ./scripts/repo-cargo clean -p elephant-storage
RUST_LANE_ID=adr-prerequisite-tests ./scripts/repo-cargo test --workspace --lib --bins work_retention::tests::retention_retry_race_surreal -- --exact --nocapture
```

Fresh storage compilation is recorded in
`/tmp/adr-001-elephant-retention-baseline.log`. The preserved executable
`/tmp/adr-001-elephant-storage-1aefaad` has SHA-256
`8a59e6012a81ad32d9be36df265ec64d6e14a024a8e6737d96f091291c14f25c`.
Its direct repetition failed on run 13 in
`/tmp/adr-001-elephant-retention-baseline-repeat.log`. Remaining unrelated
baseline package builds were stopped after that fresh executable reproduced
the failure. This is not a passed baseline workspace gate.

Instrumentation in this separate worktree later captured
`archived=1 retried=Runnable hot=[] cold=["failed"]` on run 5 in
`/tmp/elephant-retention-pair-repeat.log`. The assertion reads both actual
repository identities, exposing an acknowledged-update loss rather than a
projection-only duplicate. That diagnostic instrumentation is not in the final
production patch.

## Two independent causes and repairs

1. SurrealKV 0.9.3 advances its oracle before publishing the new index.
   A concurrent snapshot could clone old Failed bytes while claiming the
   Runnable writer's new version. Equal versions evade the subsequent strict
   greater-than conflict check. The pinned vendor backport tags the snapshot
   with the version of the index cloned under its read lock. Its deterministic
   regression splits the actual commit phases, with no sleeps, and runs both
   versioned and unversioned modes. Original failure and repaired results are
   in `/tmp/elephant-retention-kv-publication.log` and
   `/tmp/elephant-retention-kv-publication-fixed.log`.
2. In the reverse order, the unversioned backend can remove the old hot key,
   and SurrealQL DELETE of an archive absent in the retry snapshot emits no
   cold-key write. The retry then misses the surviving cold-key conflict.
   The deterministic engine probe in
   `/tmp/elephant-retention-kv-both-causes.log` demonstrates both no-write
   success with duplicate hot/cold state and forced-write conflict. Native
   update transactions now write both existing identities, temporarily UPSERTing
   then DELETEing the archive in the same transaction. No temporary archive
   survives commit and no new execution journal or mutex is introduced.

The engine patch is isolated in two upstream source files. All 41 original
archive files are preserved, with exactly those two changed. Registry archive,
original file checksums, license and exact patch are in the
[backport provenance](../../vendor/surrealkv-0.9.3-backport.md).
`Cargo.lock` changes only the SurrealKV source/checksum for the repository-relative
path override, with no dependency upgrades.

Native sibling scope includes WorkItem ordinary and status-conditional updates,
WorkRun ordinary updates, and WorkItem's public dependency-conditional updater.
The latter intentionally refuses after archival wins because it updates hot
items only. The repair preserves that behavior. Queue and lease mutation paths
require nonterminal states and cannot satisfy the retention candidate predicate.
Embedded storage retains its existing single mutation owner. Offline archive
import is not an online execution/update authority.

## Verification

Completed focused commands:

```bash
./scripts/repo-cargo fmt --all
./scripts/repo-cargo test -p elephant-storage --lib work_retention::tests -- --nocapture
RUST_LANE_ID=retention-vendor-tests ./scripts/repo-cargo test --manifest-path vendor/surrealkv-0.9.3/Cargo.toml --locked --lib
```

- Retention: 14 passed, 0 failed. Log:
  `/tmp/elephant-retention-repair-final-tests.log`.
- Upstream vendor library: 185 passed, 0 failed, 1 existing ignored test.
  Log: `/tmp/elephant-retention-vendor-tests.log`.
- Tests assert actual hot-only storage after acknowledged updates, restore
  and retry outcomes in both ordered commit cases, concurrent WorkItem retries,
  concurrent WorkRun updates, and the dependency-conditional sibling. Concurrent
  start order is alternated for run updates; this is scheduler stress, not a
  deterministic claim about commit order. Deterministic publication ordering
  is established separately by the engine regression.

Broader verification completed:

```bash
./scripts/repo-cargo test -p elephant-storage --lib
./scripts/repo-cargo clippy -p elephant-storage --all-targets --all-features -- -D warnings
./scripts/repo-cargo fmt --all -- --check
bash scripts/verify_active_surface.sh
bash scripts/verify_legacy_terms.sh
npm ci --prefix scripts/docs --ignore-scripts
make docs-check
actionlint .github/workflows/release.yml .github/workflows/build-api-binary.yml
git diff --check
```

- Storage library: 365 passed, 0 failed, 7 existing ignored tests. Log:
  `/tmp/elephant-retention-storage-suite.log`.
- Storage Clippy: passed in 5m30s. Log:
  `/tmp/elephant-retention-storage-clippy.log`. The subsequent vendor change
  only sharpened the regression assertion to the exact conflict error; it did
  not change compiled dependency or native production behavior.
- The exact linked storage test executable ran all 14 retention tests 100 times,
  passing all 1,400 test executions. Each suite includes 32 iterations per race
  and backend. Log and executable hash:
  `/tmp/elephant-retention-repair-repeat.log`.
- The final vendor suite, including exact TransactionWriteConflict matching,
  passed 185 tests with 1 existing ignored test. Log:
  `/tmp/elephant-retention-vendor-tests-final.log`.
- The final regression copied into an isolated control tree with the original
  snapshot implementation fails as expected. Log:
  `/tmp/elephant-retention-vendor-control-final.log`. This is deliberate RED
  sensitivity evidence, not a failed candidate gate.
- Format, surface, legacy-term, whitespace and changed-workflow actionlint
  checks passed. Docs check passed with 55 files, 43 pages, 282 local links and
  no errors; log `/tmp/elephant-retention-docs-check.log`.

Independent review found the initial Docker build inputs omitted the new local
path patch. The repair copies vendor bytes into the planner, before chef cook,
and again after chef cook before the final build. `.dockerignore` preserves the
entire pinned vendor input. The runtime image and both binary artifact workflows
include the unchanged upstream license, backport notice, exact patch and source
checksums. Dockerfile.hooks installs tools only and mounts the checkout, so it
has no source-copy omission. Release and API binary workflows use full checkouts.

Host-side cargo-chef 0.1.77 prepared the exact planner input subset, reconstructed
the cook skeleton with `--no-build`, resolved locked metadata, and verified that
the final vendor copy restores every original patched byte. Logs:
`/tmp/elephant-retention-docker-planner.log`,
`/tmp/elephant-retention-docker-cook.log`, and
`/tmp/elephant-retention-docker-cook-metadata.json`. No Docker image build was
run: the local Docker daemon is unavailable. These checks establish path/input
availability, not a complete release-build result. Full workspace checks and
CI remain open until actually completed on the combined release candidate.

## Boundaries

The in-process pinned SurrealKV repair does not upgrade a remote SurrealDB
server. Such a server requires its own equivalent engine correction. SQL pair
writes alone cannot correct the snapshot-publication bug. Neither patch proves
general predicate serializability, authorization fencing, source policy epochs,
independent recovery witnesses or audit-to-effect atomicity.

The authorization prerequisite and frozen auth commit f233966 were not edited
by this repair. No production configuration, deployment, upstream issue or
publication was performed.
