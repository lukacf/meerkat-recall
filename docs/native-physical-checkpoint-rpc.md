# Native physical checkpoint transport

Recall's patched SurrealDB SDK exposes
`db.recall_physical_checkpoint(timeout_ms).await`. It returns `Ok(())` only after
the selected engine reports a completed physical checkpoint and the SDK validates
the versioned completion response. This is an administrative operation outside
an ordinary query transaction. It accepts no SQL, filesystem path, source text or
caller-supplied file list.

The local SDK and remote HTTP/WebSocket SDK use the same contract. The remote
server must be built from the matching patched core. An upstream server that does
not implement the methods returns an error; there is no SQL fallback. The paired
server build copies the complete reviewed vendor source tree, including these
RPC changes and the engine checkpoint implementation.

## Authority and capability

Physical reclamation affects the whole datastore. Both remote methods require an
authenticated **root owner**, an unexpired session, and a nonempty selected
namespace and database. Namespace owners, database owners, record users, root
editors/viewers and anonymous remote callers are refused, including when server
authentication is disabled. Restricted root credentials remain subject to the
existing authorization check. Each method also obeys the existing RPC capability
allow/deny configuration. In-process embedded callers retain trusted anonymous
access when authentication is disabled; authenticated embedded callers require
the same root-owner authority.

First the SDK sends this request through the existing RPC transport:

```json
{
  "id": 1,
  "method": "recall_physical_checkpoint_capabilities",
  "params": ["meerkat-recall-physical-checkpoint/v1"]
}
```

For a supported engine and an authorized session, the result is:

```json
{
  "protocol": "meerkat-recall-physical-checkpoint/v1",
  "namespace": "recall",
  "database": "knowledge",
  "max_timeout_ms": 300000
}
```

The result advertises protocol support. It does not mean that erasure completed,
the engine is ready, or any files were retired. An engine without an implemented
physical checkpoint returns an explicit error. Native memory has no persistent
files to retire; persistent engines must prove their own reclamation before
reporting completion.

## Execution and uncertainty

The SDK uses one original deadline, between 1 and 300,000 milliseconds, for the
capability exchange, checkpoint dispatch and completion validation. It passes
only the remaining budget to the engine. For example, this request is valid when
59,000 milliseconds remain:

```json
{
  "id": 2,
  "method": "recall_physical_checkpoint",
  "params": ["meerkat-recall-physical-checkpoint/v1", "recall", "knowledge", 59000]
}
```

The original namespace and database must still match the selected session at
checkpoint admission. Both RPC protocol versions use the same handler. Integers
outside the range, floats, strings, missing/extra arguments, changed scopes and
unknown protocol versions are refused before entering the engine operation.

Only successful engine completion returns:

```json
{
  "protocol": "meerkat-recall-physical-checkpoint/v1",
  "namespace": "recall",
  "database": "knowledge",
  "max_timeout_ms": 300000,
  "complete": true
}
```

The SDK checks the exact field set, native value types, original scope and
deadline. `NONE`, `null`, a bare boolean, missing fields and another scope cannot
be mistaken for completion. There is no automatic retry or reconnect replay of
the checkpoint command.

A deadline or lost connection can leave engine-owned work running. The SDK
returns an error and makes no cancellation or completion claim. The storage
maintenance owner must retain the original erasure/restore barrier and journal
custody while the engine drains, and must refuse readiness after an uncertain
outcome. Transport capability is not a substitute for that engine/storage
ownership. A completion acknowledgment is also not independent qualification of
physical file retirement or backup retention.

## Regression coverage

`crates/storage/src/native_physical_rpc_tests.rs` exercises real native-memory
RPC V1/V2 dispatch, survivor preservation, root-owner authorization, expired and
changed scopes, protocol/type/timeout rejection, method capability denial and the
local SDK exchange. The SDK module also contains strict-response and transport
encoding controls. These memory controls establish transport behavior; the
separate persistent-engine tests and paired-server qualification must establish
physical reclamation on disk and actual installed remote behavior.
