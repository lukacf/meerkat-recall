# Source-history inspection and physical checkpoint

The original `recall_physical_checkpoint(timeout_ms)` API remains available for
versionless engine reclamation. Source-history erasure uses two additional
administrative methods under one original lifecycle maintenance reservation:

```rust,ignore
let snapshot = db.recall_source_history_inventory(namespace, database, remaining_ms)
    .into_owned().await?;
snapshot.verify()?;
// The existing domain authority projects exact causal effects, acknowledges
// payload-free commitments in the original journal, and binds every authority
// to this snapshot's fingerprint. It never persists the snapshot's raw values.
let authorities = prepare_original_journal_authorities(&snapshot)?;
db.recall_physical_checkpoint_with_history(
    namespace, database, remaining_ms, authorities,
).into_owned().await?;
```

`remaining_ms` is recomputed from the controller's **same original deadline** at
each step; inspection, projection, durable acknowledgement and physical apply
must not each receive a fresh budget. Each SDK call also enforces its own supplied
remaining budget through response validation. Timeout or disconnect never proves
that a physical checkpoint finished. The lifecycle owner retains its barrier on
an uncertain result.

Both RPC v1 and v2 require a nonexpired root owner, the exact originally selected
nonempty namespace/database, and the specific method capability. Namespace and
database owners cannot administer the whole engine. Remote anonymous requests
are refused even with authentication disabled; trusted embedded anonymous use
remains available with authentication disabled. Unsupported methods or engines
fail explicitly. Neither method falls back to SQL, retries, or reconnect replay.

The native wire requests are:

```text
recall_source_history_inventory:
  ["meerkat-recall-source-history-inventory/v1", namespace, database, timeout_ms]
recall_physical_history_checkpoint:
  ["meerkat-recall-physical-history-checkpoint/v1", namespace, database,
   timeout_ms, original_journal_authorities]
```

Inventory returns a strict complete-response envelope and the source-authority
owner's `RecallHistorySnapshot`. Its native key/value bytes, full-width versions
and timestamps, current/tombstone markers and scoped fingerprint are validated by
that owner's codec. The engine captures all versions needed for the scope,
including records absent from current queries. Oversized or incomplete inventory
is refused; no success silently omits rows or pages. Inventory is sensitive data
held for projection, not an operation receipt or export.

Physical apply receives all relevant authorities as native objects using the
source-authority owner's strict codec. Each authority must be bound to the
inspected history and contain the exact original journal membership and approved
version commitments. The engine checks the original scoped history before
applying its immutable selection. A changed history remains an error requiring
fresh preparation. The transport neither computes causal ownership nor removes
all versions of a modified key. Independent versions and historical-only rows
are handled by the engine/source-authority selection contract.

The checkpoint response contains exactly `protocol`, `namespace`, `database`,
`max_timeout_ms`, `authority_count`, and `complete: true`. The SDK checks all six
native fields. Inventory's five response fields are `protocol`, `namespace`,
`database`, `snapshot`, and `complete: true`; the nested snapshot is verified too.
A boolean, missing field, wrong scope, wrong count, malformed native value or
wrong response variant cannot become unit success.

The RPC transport bounds authority vectors to 4096 entries and exact native
revision-encoded payloads to 64 MiB. The source codec applies its own record and
field bounds as well. Exceeding a bound is an explicit refusal, not permission to
drop intents or apply a partial set. Larger workloads require an agreed complete
inventory protocol or a proven equivalent authority grouping. No implicit
coalescing is performed here.

Command, request and response Debug paths redact authority keys, memberships and
raw inventory. The existing HTTP/WS authentication and native serializers carry
the data; no JSON intermediate, ordinary query variable or second public source
state authority is introduced.

Eleven native Mem workspace controls cover v1/v2 completion, retained independent
source-less Recall entities, refusal of unprovable unscoped data, complete authority
vectors, root/scope/capability enforcement, malformed native requests, real SDK
owned futures, inventory read-only behavior (including the full unscoped native
survivor row and foreign-scope exclusion), and request/response redaction. Seven private SDK controls cover exact wire preservation,
strict completion decoding, missing methods, wrong response variants, original
deadlines, incomplete inventory and trace redaction. These are authored source;
engine physical deletion, exact historical selection and crash behavior require
the separate engine and source-authority controls. No Cargo or test execution was
performed by this source checkpoint.
