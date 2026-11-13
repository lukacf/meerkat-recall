# Reblessive 0.4.3 cancellation cleanup and driver borrowing

Recall pins this copy through the root `[patch.crates-io]` table. It preserves
the cleanup owner when converting a tree `Runner` into its finishing future.

## Provenance

- Registry crate: `reblessive` 0.4.3.
- Archive SHA-256: `bbc4a4ea2a66a41a1152c4b3d86e8954dc087bdf33af35446e6e176db4e73c8c`.
- Upstream revision from `.cargo_vcs_info.json`:
  `71f35e46741f367ec4db9e4c3ec0af2e888e4967`.
- Repository: <https://github.com/DelSkayn/reblessive>.
- The original MIT [LICENSE](reblessive-0.4.3/LICENSE) and all 42 archive files
  are retained. Original file digests are recorded in
  [the checksum manifest](reblessive-0.4.3-upstream-sha256.json).
- Changed upstream files: `src/tree/runner.rs`, `src/tree/mod.rs` and
  `src/stack/runner.rs`.
  Added modules: `src/tree/cancellation_tests.rs` and
  `src/tree/cancellation_diagnostics.rs`.
- The complete source delta is recorded in
  [the backport patch](reblessive-0.4.3-backport.patch).

## Failure and repair

Upstream `Runner::finish` forgets the runner and returns a future containing
only a stack reference. Dropping that future before completion therefore does
not execute the runner's cleanup. An attempted next entry panics because tasks
remain on the stack. More seriously, pending futures can contain borrowed input
whose lifetime ends before the stack is destroyed; their deferred destructors
can then access freed data.

The finishing future now owns the original runner. Its existing destructor
clears scheduled children and root tasks, including typed result cleanup. No
new unsafe block, detached task, timeout, retry or allocator is introduced. The
existing result marker is retained so this change does not broaden the finishing
future's automatic thread-safety traits.

Before clearing a runner with pending root tasks, the destructor writes one
payload-free line to stderr: `event=recall_tree_cleanup phase=begin`, followed
by `unix_ms`, `clock_ok`, `pid`, `seq`, `pending_root_tasks`, and `unwinding`.
The sequence is process-local and wraps at the platform's `usize` limit. Root
counts exclude the result slot and do not count scheduler children. Completed
runs with no pending root tasks emit no line. This records cleanup beginning,
not completion or the cause of cancellation. Failed writes are ignored so an
I/O error cannot panic during cleanup; stderr backpressure can delay the drop.
Clock failures produce `unix_ms=0 clock_ok=false`. No query, record, tenant or
input content is logged. Missing records alone cannot exclude cancellation:
capture coverage, process identity, I/O failures and bypassed destructors matter.

## Validation

The regression module covers unpolled cancellation, nested tasks, scoped
scheduler children, borrowed-input lifetime, completed result ownership,
stack reuse, panic unwinding, cross-thread cancellation with stale scheduler
wakers, and diagnostic output failure. A Miri-only regression allows the input
to die before the stack, exposing the original dangling destructor safely under
the interpreter. Run the dependency tests explicitly; excluded
vendor packages are not tested by the workspace unit gate:

```sh
./scripts/repo-cargo test --manifest-path vendor/reblessive-0.4.3/Cargo.toml --locked --features tree --lib
MIRIFLAGS=-Zmiri-disable-isolation ./scripts/repo-cargo +nightly miri test --manifest-path vendor/reblessive-0.4.3/Cargo.toml --locked --features tree tree::cancellation
```

Miri isolation is fully disabled to permit the diagnostic's wall-clock read;
these tests use synthetic inputs and do not contact a database or provider.
The stale-waker test wakes after cancellation, when the scheduler has already
marked the task queued; this is a harmless no-op. It does not exercise a wake
that concurrently enqueues a task during cancellation.

A separate minimal safe-Rust reproduction against the original registry copy
failed immediate destruction and stack-reuse assertions. Miri reported a dangling
borrow in a deferred destructor after its input left scope. All three checks
pass with the cleanup repair. The combined cleanup and diagnostic candidate also
passes all 46 native library tests and the 10 focused cancellation/diagnostic
tests under Miri. Full workspace and target-platform release checks remain
separate gates.

## Ordinary stack driver borrowing

`Runner::step_async` and `Runner::finish_async` now return futures tied to the
exclusive borrow of the runner, rather than its longer-lived stack reference.
This prevents a driver from surviving runner cleanup or coexisting with another
driver (including a synchronous step). Four compile-negative examples covering
both methods compiled on the baseline and are rejected after this change; all
46 existing native tests still pass. Run them with the vendor command above
without `--lib` so Rustdoc checks are included.

The synchronous parser's `finish()` call does not use these async methods. This
repair therefore does not establish the cause of an allocator abort during
reference normalization. It also does not make non-Send captured state safe to
move between threads.

## Limits

These repairs address ordinary cancellation cleanup and exclusive async-driver
borrowing. They do **not** resolve the independent unsound `Send`/`Sync` defect described in
[RUSTSEC-2026-0454](https://rustsec.org/advisories/RUSTSEC-2026-0454) and the
[upstream advisory](https://github.com/surrealdb/surrealdb/security/advisories/GHSA-m9qr-4rcq-3jhc).
Do not suppress that advisory on the strength of this patch. The related
[upstream timeout proposal](https://github.com/surrealdb/surrealdb/pull/7342)
describes stack poisoning after cancellation; this patch repairs the dependency's
cleanup ownership rather than resetting a caller's stack afterward.

Explicitly forgetting a `Runner` or `FinishFuture` bypasses its destructor and
the diagnostic. Pending borrowed tasks can still reach the existing `Stack`
destructor after their inputs die. This residual leak-path lifetime defect is
outside the ordinary-drop repair; the dependency is not generally memory-safe.

Dependency tests establish the reproduced lifetime defect and its correction.
They do not establish the origin of an observed production allocator abort,
general heap stability, or a memory-growth fix. In particular, dropping a Recall
read deadline does not by itself prove that an embedded database runner was
cancelled: the SDK retains ownership of accepted requests. Runtime integration
and production qualification are separate evidence.
