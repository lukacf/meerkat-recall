# Pristine native schema profile capture

The migrations test `native_schema_profile_tests::pristine_native_schema_profile_matches_independent_instances`
initializes two independent, task-owned native in-memory databases using the
actual ordered migration registry. It requires exactly the final 001–043
registry, captures fresh schema, then exports and ordinarily restores the same
pristine store with its original journal before capturing the standard refreshed
schema. No Source, document, space configuration or provider fixture is inserted.
Each independent instance has a 60-second harness bound.

This source depends on the final migration registry and the independently owned
public offline snapshot inspector. It is intentionally not executable against a
partial source join. No new product API, copied migration order or DDL interpreter
is introduced. Native `INFO` supplies canonical definition identity and formatter
text; the existing inspector separately retains every original exported schema
statement and SHA256. The profile records both pre-refresh and post-refresh
stages because their equivalent guard definitions can have different text.

The normal test compares both complete stage inventories without creating an
output artifact. Central orchestration may explicitly set
`RECALL_PRISTINE_SCHEMA_PROFILE_OUTPUT` to a new absolute file outside the checkout.
Its parent must already exist and be canonical. Output uses exclusive creation,
mode 0600, file and parent-directory synchronization, and a 16 MiB bound. Existing
files and symlinks are refused. Raw snapshots and original journals remain in
private test-owned temporary directories and are removed with those directories;
their rows, anchors, UUIDs and keys are never emitted in the profile.

An emitted profile binds the clean actual Git commit/tree, compiled source
digests, Cargo.lock, pinned engine metadata/formatter/export sources and every
ordered migration's name and SHA256. Runtime source bytes must match the compiled
inputs. The membership table is the explicit normal-export exclusion. Unknown
database/table categories, live queries, sensitive database definitions or
unexpected non-schema rows refuse capture.

The result is always an **unreviewed proposal**. S/E must map every captured
definition to pinned migrations or a known engine/standard-refresh transformation;
D must independently freeze the exact allowed profiles afterward. New candidate
definitions cannot pass because they repeat across pre/post exports or lack a
canary. Any later source/schema/engine change invalidates the reviewed profile.
This capture establishes no erasure, restore-recovery, semantic or product gate.

This worker has authored source only. Rust execution and the explicit artifact
path are owned by central orchestration; no capture result is claimed here.
