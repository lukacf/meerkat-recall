# Native overlapping transaction oracle source

`native_guard_race_oracles.rs` is included by the storage owner's
`source_document_guards_tests.rs` with:

```rust
#[path = "../../../tests/recall_preview/native_guard_race_oracles.rs"]
mod independent_native_overlap;
```

It consumes the root-authorized test-only native constructor from F59a179c and
Afa929c's pinned `surrealdb/http` dev-dependency, plus the actual 042/043 guard
source. Production constructors and network capabilities are unchanged.

Five authored cases use an actual accepted source/head and persisted named-space
configuration. One positive commits with no competing writer. Four cases overlap
the original `PreparedDocGuards::append_guard_sql` transaction with:

- A foreign-space CREATE at the exact originally absent physical Doc key.
- An actual repository update that restricts the original Doc policy.
- An actual second source acceptance advancing the original head generation.
- An actual update of the original observed private manifest payload.

The manifest case is a storage observation fixture, matching A's existing
manifest guard fixture. It does not claim a completed/validated pipeline plan.

The original transaction reaches its uniquely addressed loopback GET only after
the original exact row predicates and real changing bucket writes. The owned
listener commits the competing writer before returning `true`. There is no
timed sleep or pre-transaction mutation standing in for concurrency. The original
transaction then tries to create one Doc in a **third space**, with no source
reference, preventing an incidental same-space canonical-epoch conflict from
substituting for the prepared read guard. Only the pinned native commit-conflict
diagnostic qualifies. A raw physical read must find no effect row, including a
soft-deleted row; the competing write must remain visible.

Both futures run directly under the same absolute nine-second race deadline;
there are no spawned test tasks. The original engine query and transaction
deadlines are eight seconds, and the entire case is bounded at 45 seconds.
Listener/stream ownership provides cancellation cleanup. The barrier releases
the first query even when the competing writer fails, then propagates that
original error. A timeout, denied capability, malformed HTTP request, HTTP error
or another query error cannot become a passing conflict result. HTTP request
headers are capped at 4096 bytes and the endpoint is one owned ephemeral IPv4
loopback address/port with a unique path.

The observer binds the original global `$space` required by present-Doc guards.
It uses `StorageClient::query_bind` exactly once and checks that original response
directly. The repository checked wrapper automatically retries native conflicts
and therefore cannot be used to observe this first-commit failure. Competing
ordinary writers retain their normal repository paths.

The observer inspects every original `take_errors()` entry in statement order.
At least one entry must equal the pinned native commit-conflict message (bare or
wrapped in the engine's exact failed-transaction detail). Other entries may only
equal the plain rollback marker. A generic rollback alone, another error, or a
message merely containing conflict words cannot pass. The positive requires no
errors. This avoids first-error ordering hiding the original conflict.

This source checkpoint has direct rustfmt and diff checks only. No Cargo,
compiler, Rust test, listener, service or provider was executed by D. Actual
overlap, backend rollback and cleanup acceptance await the parent's central
offline run; sequential nonce/event tests alone do not establish that gate.
