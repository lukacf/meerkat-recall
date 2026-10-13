# Pinned Recall native reference capability

The two source directories `surrealdb-core-2.6.5` and `surrealdb-2.6.5` come from
exact crates.io archives already pinned by this workspace's lockfile. Their
`RECALL_PROVENANCE.json` files record the archive SHA-256 and upstream revision.
The adjacent `*-upstream-sha256.json` manifests record every original file. All
upstream license files and normalized/original manifests are retained. The only
new production dependency is `hmac = 0.12.1`, already present in the lockfile.
Integration owns the minimal root lockfile path-source adjustment.

Run `python3 vendor/verify_recall_surrealdb.py` to check unchanged upstream files,
reviewed patch locations, required additions, and independent known vectors.
This source audit does not compile or execute Rust. Runtime qualification uses
`elephant-storage`'s `native_reference_tokens` integration test target, alongside
storage lifecycle, journal, and actual native write-event tests. Those gates are
centrally scheduled to share the machine's build cache and CPU budget.

The reviewed patch locations include `src/dbs/options.rs` in the core crate.
Commit `0e738553a1888d626b9caafa86c6fdebf1494660` extracted the original
depth-budget comparison into `Options::check_dive`; `Options::dive` calls that
check before creating derived options. Native scalar evaluation and inert-value
depth checks use the same comparison without copying unused options. The audit
allowlist records this existing source change; the upstream hash manifests and
the exact changed-file and added-file checks remain unchanged.

## Surface and custody

The SQL builtins are:

- `recall::reference_token_version()` returns integer `1`.
- `recall::reference_tokens(journal, space, key, typed_uuid_tables, value)` returns
  sorted unique lowercase 64-character hexadecimal HMAC-SHA256 tokens.

The SDK reexports `RECALL_REFERENCE_TOKEN_VERSION` and
`recall_reference_tokens(&str, &str, &[u8; 32], &[String], &sql::Value)`, returning
`Result<Vec<String>, surrealdb::error::Db>`. The Rust and SQL entry points call
the same pure normalizer. There is no database lookup, mutable global state,
scripting, network access, secret generation, or secret persistence here.

Journal must be a canonical lowercase UUID, space must be nonempty, and the SQL
key argument must be native bytes of exactly length 32. Fixed argument errors
exclude caller values, keys, table names, and record IDs. Key custody belongs to
the independently durable Recall erasure journal, not these crates.

The narrowly scoped `Context::value` change lets `__recall_reference_key` and
`__recall_journal_identity`, plus `__recall_erasure_reservation`, inherit through isolated custom-function and closure
contexts. They remain valid trusted SDK query bindings. They are deliberately
**not** in `PROTECTED_PARAM_NAMES`, which rejects bound variables. Recall's
client appends private bindings after user parameters. Raw database and host
administrators are trusted; these are not public SQL capabilities. The names do
not become persisted database parameters or remain bound after a query.

Remote deployments require a server built with this same pinned core patch and
must pass the fixed-vector/version probe. An unmodified 2.6.5 server does not
implement this capability. Storage owns import admission, latest native event
installation, per-space transaction guard conflicts, private journal replay,
and readiness; the pure builtins cannot provide those guarantees alone.

## Version 1 reference and byte rules

Recognized references are native `Thing` values, complete canonical native Thing
strings, complete legacy `table:key` spellings with ASCII alphanumeric/underscore
tables and alphanumeric/underscore/hyphen keys, `{tb: table, id: ...}` or
`{table: table, id: ...}` objects, object keys, and recursively decoded JSON
strings containing those forms. A native string `"42"` ID and numeric `42` ID
remain different. Plain UUIDs have no table and produce no reference token.
There is no substring, suffix, or prose matching; noncanonical parse results
including trailing comments do not become references. Native composite array
and object IDs are retained, and their contents are also visited for references.
For explicitly configured Recall UUID tables only, a string ID parsing as UUID
normalizes to the native UUID identity. Extension tables retain native ID type.

The HMAC message begins with ASCII `meerkat-recall/reference/v1` plus NUL,
followed by length-prefixed UTF-8 journal, space, and table. Every length/count is
an unsigned 64-bit big-endian integer. The record-ID encoding follows:

| ID kind | Tag | Content |
| --- | --- | --- |
| String | 0 | Length-prefixed UTF-8 |
| Number | 1 | Signed 64-bit big-endian |
| UUID | 2 | Raw 16 bytes |
| Array | 3 | Count followed by typed native values |
| Object | 4 | Count, then sorted UTF-8 key/value pairs |

Composite native values use these distinct tags: NONE 0, NULL 1, false 2, true 3,
integer 4, finite float 5, decimal 6, string 7, duration 8, datetime 9, UUID 10,
array 11, object 12, geometry 13, bytes 14, and Thing 15. Integers use signed
64-bit big-endian; floats use exact 64-bit IEEE bits, decimals use normalized
length-prefixed decimal text, durations use unsigned 128-bit nanoseconds, and
datetimes use signed 64-bit seconds plus unsigned 32-bit nanoseconds. Strings
and bytes use length-prefixes. Arrays/objects use the preceding count/pair
rules. UUID uses 16 bytes. Geometry uses length-prefixed pinned `revision`
serialization. A Thing is length-prefixed table plus typed record ID. Large
unsigned JSON integers within composite IDs use exact Decimal, never rounded
floating point. Future upstream encoding changes require a new token version.

Generated or range IDs, nonfinite identity components, and unevaluated
expression/Future/closure values return fixed errors. This prevents uncomputed
references from being silently omitted. Depth is bounded at 128, visited nodes
at 1,000,000, and cumulatively decoded JSON text at 64 MiB; limit violations fail
closed with fixed errors. Invalid JSON text remains ordinary text.

## Independently derived capability vectors

These constants are generated using Python's standard `hmac`/`hashlib`, not by
calling either Rust entry point. Journal is
`00000000-0000-0000-0000-000000000001`, space is `__recall_capability__`, key bytes
are integers `0..31`, typed table configuration is empty, and table is
`recall_probe`:

| Native ID | Expected token |
| --- | --- |
| String `42` | `40e9322203b5e4d741f086599f851ac1b7c4e8a2e0bad1398da4ffde5911874c` |
| Number `42` | `03d2d2962853eaa5410909793d65f296968ae5e6024ab522121d575cf1108454` |

The mixed/deduplicated output sorts Number's token before String's token. The
SDK integration target also covers typed UUID normalization, recursive encoded
objects, native composite identity, prose neighbors, journal/space/key binding,
actual SDK private binding admission, custom functions, isolated closures,
write events, fresh-query loss of private bindings, and redacted errors.

## Identity membership and debugging

`recall_reference_identity_token` is an additional pure Rust API with the same
journal/space/key/typed-table arguments and one `&sql::Thing`, returning one
`Result<String, surrealdb::error::Db>`. It applies exactly the same identity
encoding but does not extract references from inside a composite key. Erasure
membership uses this API for the physically removed identity; ordinary payload
checks use the full recursive extractor. Thus removing a composite-ID record
does not also erase an independent record merely cited in that ID.

The known custody bindings are redacted in context, SDK query, internal query
command, router query request, and native RPC query request `Debug` formatting.
Other values keep their existing formatting. This does not change generic
`Value` serialization or raw wire bodies. Deployments must not enable query
body/binding capture at HTTP proxies, RPC transport logging, or custom tracing.
A trusted SQL administrator can intentionally return a private binding; the
application must not expose its privileged raw SQL connection publicly.

The third inherited/redacted custody name, `__recall_erasure_reservation`, binds
only an admitted maintenance scope to its durable erasure intent. The Recall
client explicitly binds NONE outside that scope, after all caller bindings.
The engine only preserves query context; storage validates authority and the
shared reservation row. It never retains the owner binding between queries.

`Response::errors()` adds a borrowed iterator over `(statement_index, &Error)`.
Recall can identify the exact retryable reservation rejection on every query
path without consuming or reordering unrelated unchecked response errors.

## Pinned upstream visibility correction

The upstream transaction `stream` and `stream_keys` helpers accept the
crate-private planner `ScanDirection`; all callers are inside the core crate.
Their visibility is now `pub(crate)` to match that existing type boundary.
This removes two `private_interfaces` warnings exposed by the path dependency
without changing scan behavior or making planner internals public.

## Original-field comparison capability

`recall::original_fields_match(current, expected)` accepts exactly two native
objects. It compares each expected top-level field using the same directional
`Value::equal` implementation as the retained SQL expression
`$current[$entry[0]] != $entry[1]`. A missing current key yields native `NONE`;
`NULL` remains distinct, and additional unobserved current keys are ignored.
Native nested values, record identity types, numbers, and regex equality retain
the engine's existing semantics. Invalid arguments return fixed diagnostics
without formatting either object.

The native function evaluates each argument once and compares the resulting
fields directly. This avoids recomputing the whole current object for every
field in the canonical before-image loop. It has no database access, cache,
mutation, authority or new reference-token encoding. The canonical caller still
checks existence, space and archive selection, and performs every original
nonce update, reference event and attempt guard in the same transaction and
order. The shared comparison is also used by grouped producer guards through
the same canonical helper.

Readiness requires both the unchanged token-version-1 known vectors and explicit
positive/negative original-field comparison results from the same native server.
A server missing this function refuses admission. The storage native guard tests
compare the function against the actual former SQL loop for typed values and
for every field of genuine persisted Work and WorkRun records; existing late
refusal controls retain physical nonce and sentinel rollback checks. The actual
256-Work / 257-Source constructor and its original 240-second deadline remain
the performance gate. Source review or a short sampling run does not pass it.


## Complete original-row predicate

`recall::original_row_matches(current, space, expected)` fuses the existing
presence, namespace and original-field checks. It requires a native space string
and expected-field object. An absent or non-object current value returns false;
an object must have a matching `space_id` and satisfy every expected field using
the unchanged directional native `Value::equal` rule. Missing keys yield `NONE`,
not `NULL`; extra unobserved keys remain irrelevant. The older two-argument
`original_fields_match` function and its invalid-argument behavior are unchanged.

Canonical and private producer callers pass stored native data through bindings.
The function evaluates these arguments once, avoiding the repeated recursive
object evaluation in separate SQL presence, namespace and payload predicates.
It performs no database access or mutation. Both original hot and cold reads,
archive guards, absent-row branches, every nonce update and native event, all
attempt checks, private-origin authority and transaction ordering remain intact.
The SQL callers retain their original refusal tags.

Fusion evaluates the expected argument even when the current value is absent;
this has no side effects for the callers' stored data. Invalid space/expected
argument shapes always produce a fixed, non-payload error rather than a match.
Malformed non-object current values are refused rather than interpreted as a
record expression or traversed. This is a stored-row predicate, not a promise to
preserve arbitrary executable SQL expressions or malformed-input error timing.
Neither distinction grants acceptance to a previously refused stored row.

The same required native readiness response now includes a positive row probe
and absent, foreign-space and changed-payload negative probes, without defaults.
Actual SQL tests retain the original 28 native vectors and every physical Work
and WorkRun field mutation, and additionally compare the fused predicate against
the original presence/namespace IF and native field loop with correct, foreign
and missing spaces. A separate actual private-origin fixture compares the exact
old origin IF, including missing and malformed current rows, payload refusal and
fixed invalid-argument diagnostics. Existing transaction rollback/nonce controls
and the original 256/257 boundary test remain unchanged. These are test sources;
only the central runtime gate can establish equivalence and performance.


## Native data container evaluation

`Object::compute` and `Array::compute` can clone a container only after a bounded,
side-effect-free probe establishes that its entire value tree evaluates to itself.
The original evaluator bodies remain the complete fallback. The probe accepts
NONE, NULL, booleans, native numbers, strings, durations, datetimes, UUIDs,
geometry, bytes, regexes, and Things with numeric, string, or UUID IDs, recursively
inside arrays and objects. It preserves native types, literal keys, and NONE
entries; it does not serialize, normalize, hash, cache, or compare authority.

Every other value, including deferred expressions, parameters, functions,
constants, casts, futures, and generated/compound/range record IDs, causes the
original evaluator to run. The probe visits at most 4,096 child values and 32 child
levels using an iterative worklist. These are optimization eligibility limits;
exceeding either limit falls back and does not reject the query or alter its depth
limit. Even when a large outer container falls back, an eligible child container
can still use the same identity rule when reached in original evaluation order.

Each original child `Value::compute` consumes one `Options::dive` level. Siblings
share the same options, so the probe checks the maximum descendant depth against
the original remaining budget. The outer container's own dive has already
occurred when entered through `Value::compute`; direct Object/Array callers have
no extra outer dive in either path. Scalar Thing IDs add no child dive. A failed
probe, including a failed depth check, returns to the original evaluator without
returning a probe error or executing any member. The original first error,
deferred side effects, function and field permission checks, and evaluation order
therefore remain authoritative.

This change does not alter SQL, schema, reads, nonce writes, events, statement
outputs, transaction boundaries, reference HMACs, or the original 256/257 ancestry
boundary and 240-second test deadline. Real workspace native-value regressions
exercise the result and fallback contracts. Source review and the bounded event
diagnostic do not establish a performance improvement: the unchanged original
native boundary test must complete before a performance claim is made. Paired
server artifacts must include these exact reviewed core source changes; the
dynamic reference source receipt binds the complete vendor tree.

## Local native container projections

The native value dispatcher also avoids copying `Options` for inert scalars and
Things with numeric, string or UUID IDs. It performs the same one-level depth
check through the helper used by `Options::dive`, then clones the original value.
These branches have no descendants that consume derived options. The container
identity probe uses that same depth-only check instead of constructing and
discarding an options copy. Deferred values, generated and compound IDs, and
every other dispatcher branch retain their full original evaluation.

Frozen-dispatcher comparisons and explicit zero/one-level assertions cover the
native scalar variants and fixed record IDs. This removes repeated handle copies
and scalar-ID stack dispatch from actual guard evaluation; it does not eliminate
database reads, update outputs, stored-future effects, or erasure checks. Runtime
improvement remains unmeasured until the unchanged boundary oracle completes.

For an idiom beginning with an ordinary locally bound parameter, the evaluator
can borrow an Object or Array instead of first cloning its entire value before
field lookup. Eligibility uses the same complete, bounded identity proof above,
including unselected siblings. The original parameter Value dive is checked
first; probing the bound root as one child counts its own Value dive and every
descendant. A failed proof returns to the original evaluation body, preserving
unselected deferred effects, first errors, generated IDs and depth refusals.
The proof's existing node/depth caps are conservative optimization limits only.

Special `$this`/`$self`, absent local bindings, global parameter resolution and
permissions, and noncontainer starts retain their original paths. Successful
proof changes only the preliminary whole-container copy: the original `get`
and result `compute` still receive the original path, context, options and
document. Record traversal, dynamic path expressions, methods and their errors
therefore retain their existing evaluation. No parameter implementation, SQL,
schema, authority cache, guard, event, nonce, output or transaction is changed.

Actual SQL regression sources compare local projections with deferred-leaf
original evaluation and fixed native expected values, and cover unselected
effects/errors/depth, optimization caps, special/global bindings and live record
traversal. This removes a source-visible repeated copy in ancestry observations
and guards; it is not a measured performance claim. The original native
256/257 boundary and 240-second deadline remain the performance gate.

## Plain point SET update dispatch

`UpdateStatement::compute` selects the existing `prepare_thing` future directly
for a computed native Thing, retaining target evaluation, table permission
preparation, range handling and original UPDATE target errors. Other target
values retain general preparation. A single prepared Thing with SET data and no
WITH, WHERE, explicit output, timeout, EXPLAIN, parallel flag or inherited query
planner can use the existing point collector. All iterator counters, limits,
errors and results must still be pristine. SELECT eligibility is unchanged.

The shared point collector retains the original four-level depth charge,
cancellation checks, record load, `Iterator::process` and complete
`Document::update` pipeline. Field and table permissions, current-record checks,
indexes, views, live notifications, events, changefeeds and default postimage
evaluation all remain in that pipeline. Stored futures therefore still execute
at the original UPDATE output boundary. Stored document errors and ONLY result
cardinality keep their original handling. Every excluded shape retains the
general output path, including nested statements with an inherited planner.

This removes general async iterator dispatch around repeated point updates; it
does not eliminate scans that were already absent or claim expensive planner
allocation savings. Each original nonce update and each nested shared erasure
revision update remains independent. Both original SELECT-star computations,
all schema events, reference checks, ordering and transaction boundaries remain
unchanged. There is no authority cache or schema/query rewrite.

Existing native point regression cases additionally cover SET postimages with
exact native values, target/event order, rollback, missing/invalid targets,
fallback modifiers and cardinality, original depth errors, table and field
permissions, revocation, stored-future effects and failures, zero timeouts and
inherited index plans. These additions are source-only until the central runtime
gate runs. No speedup is claimed; the original 256-Work / 257-Source whole-case
240-second oracle and the three stored-future reference-guard effects are intact.

## Immutable subquery parent snapshots

`Subquery::compute_unbordered` keeps the same parent context and all original
evaluation calls. Its parent value now uses `CursorValue::snapshot_arc`: an
already immutable `Arc<Value>` is retained, while a mutable value is independently
cloned exactly as before. Snapshot capture does not mutate or freeze the cursor.
The existing `to_mut` copy-on-write path protects every retained snapshot from
subsequent changes. There is no shared mutable value or evaluated-result cache.

Events already freeze before/after/current documents before their subqueries.
Sharing those immutable documents avoids a sampled whole-object clone for each
subquery. Futures and other executable values remain in the captured value and
are evaluated by the original code at every original boundary. Context, options,
depth, permissions, SQL, guard reads, nonce writes, events and transaction ordering
are unchanged. Workspace native parent controls cover nested writes, distinct
before/after values, repeated future effects and rollback; a private vendor cursor
test covers mutation after capture. Source review is not a performance result.

## Immutable array closure bodies and owned arguments

`array::fold` and the closure branch of `array::filter` borrow the immutable
closure they already own, instead of constructing a temporary anonymous function
and repeatedly copying its syntax tree and computed arguments. Both retain the
original anonymous function's `futures=true` option and exactly one `stk.run`
boundary into `Closure::compute`. Fold passes the existing accumulator, member and
index without re-evaluating them. Filter still computes each input member with
the original incoming options and keeps its copy for the surviving result. The
nonclosure equality-filter path is unchanged.

`Closure::compute` consumes its already owned argument vector instead of cloning
each argument into the isolated context. Optional and missing arguments, ignored
extra arguments, coercion and its errors, body evaluation, and return coercion
retain their original order and behavior. The generic anonymous-function
dispatcher is unchanged. There is no closure-definition cache, function-name
specialization, new evaluation shortcut, or changed capability/depth boundary.

Native workspace regression sources cover accumulator/index/effect order,
filter evaluation and retained values, missing/optional/extra arguments,
argument/return coercion, future-bearing body effects and rollback, current closure
bindings, and isolated context behavior. These ownership changes address copies
observed in the bounded native stack sample; neither that observation nor these
source controls establishes a speedup. The original ancestry test is unchanged.

## Owned builtin table names and object entries

The reference-token builtin consumes its owned array of typed-table arguments
and moves each string into the existing fallible collection. Argument shape,
32-byte key validation, eager table-element validation, canonical journal and
space checks, and table-name validation retain their original order and fixed
errors. The public token APIs, extractor, HMAC encoding and traversal limits
remain unchanged. No table configuration survives the call.

`object::entries` consumes its owned `BTreeMap` and moves each key and native
value into the existing two-element entry arrays. Lexical key order and exact
native value variants remain intact. Argument evaluation and coercion still
precede the builtin, with their original effects and errors; the function does
not recompute values or alter any other object builtin.

Four workspace SDK controls cover these ownership changes:

| Integration target | Control | Contract |
| --- | --- | --- |
| `native_reference_tokens` | `native_reference_owned_table_arguments_preserve_public_token_parity` | SQL/public API parity, typed UUIDs, empty/duplicate/changing table lists, fixed vectors and sorted unique tokens. |
| `native_reference_tokens` | `native_reference_owned_table_arguments_preserve_error_precedence_and_redaction` | Mixed-invalid argument precedence, fixed private diagnostics, and a valid call after errors. |
| `native_value_navigation` | `native_object_entries_preserves_order_and_nested_native_values` | Empty and ordered keys, including Unicode and literal dotted keys, with exact nested native types. |
| `native_value_navigation` | `native_object_entries_evaluates_argument_once_and_keeps_transaction_errors` | Single argument evaluation, original thrown error and transaction rollback. |

These source changes remove redundant string and value copies. The four new
controls are authored but have not been compiled or executed at this checkpoint;
no measured speedup is claimed. All original reads, nonce writes, native events,
guard checks and outputs remain required. The original 256-Work / 257-Source
constructor and its 240-second deadline still require runtime qualification.

## Native physical generations and authenticated history

The administrative SDK/RPC methods carry explicit namespace/database scope,
finite remaining deadlines and typed, redacted original-journal authorities.
Root authorization and exact native protocol responses are required. History
inventory reports actual scoped native versions and binds the physical copy to
its complete fingerprint. There is no SQL execution flag or alternate source
state authority. The Recall client durably acknowledges opaque history proofs
before submitting every original erasure intent together.

The core owns ordinary transaction admission, active and removed index workers,
and cache invalidation through generation replacement and worker restart. The
actual transaction destructor releases admission after native snapshots drop;
requester cancellation does not cancel the unexposed owner. A temporary restart
view reads only index metadata while restarted workers keep the original gate.
Failure or panic leaves ordinary admission closed until restart reconciliation.

RocksDB creates a fresh verified native directory and retires the complete old
generation, including SST, WAL, MANIFEST, LOG and OPTIONS files. SurrealKV copies
the selected actual versions with their original clocks into a fresh generation.
Both use the checked directory-promotion protocol and stable OS ownership. The
memory adapter reports its actual versionless keys and hard-clears selected
native keys under exclusive ownership. Unsupported engines return a failure;
they cannot certify physical erasure.

Selection validates typed records, original controls and derived native keys.
Versionless index prefixes are omitted before the first new-generation write.
Persistent historical scalar entries require exact field evidence under
compatible inspected index definitions; deferred worker commit time alone is
not document provenance. A pure local proof uses the native document and index
operators with no transaction or function capability. Current SEARCH, HNSW,
MTREE, COUNT and scalar indexes are rebuilt through the shared native operators
under the original datastore capabilities. Protected record/version digests are
checked before promotion. Unknown provenance, native formats and stale inventory
are explicit refusals, not implicit authorization to keep or delete bytes.

The normalized patched manifests declare Rust 1.89, required by the stable
`File::try_lock` ownership API. Original upstream manifests remain unchanged;
the Recall workspace already requires Rust 1.94. No package version or dependency
resolution is changed by this declaration.

Implementation and limitations are documented in
[`native-physical-erasure.md`](../docs/native-physical-erasure.md),
[`native-source-history-erasure.md`](../docs/native-source-history-erasure.md),
[`native-derived-index-erasure.md`](../docs/native-derived-index-erasure.md), and
[`native-backup-history-origins.md`](../docs/native-backup-history-origins.md).
These source changes and authored controls require the coordinated runtime gate.
Private vendor tests do not count as executed storage workspace tests.

## Conservative parsing and NONE-field cleanup

Reference extraction skips the native parser only for JSON prefixes and exact
unquoted UUID-shaped legacy keys that cannot equal a canonical native Thing
display. All other candidates still use the original parser and exact display
comparison. Legacy recognition, JSON decoding, HMAC bytes, error ordering and
node, depth and decoded-byte budgets are unchanged. Negative integer IDs retain
native numeric precedence.

Ordinary NONE-field cleanup first borrows the postimage to prove that its
original field walk cannot remove anything. Direct array NONE slots and opaque
native values remain untouched; object NONE fields at any nested array level
still take the full original path. A 4096-node or 32-level proof limit also falls
back to the complete existing cleanup. The check runs after all table and
SCHEMAFULL/strict field rules; it changes no nonce, event, shared revision write
or transaction ordering. These source optimizations require the original native
256/257 boundary test to pass within its unchanged 240-second limit.

## Owned FOR container values

`FOR` consumes the array returned by its original range evaluation. At the
original per-element compute location, it now moves an owned Object or Array
when the existing bounded identity proof certifies that evaluating the entire
value, including its root and descendant computation depth, is an identity.
Scalar and range handling stay on the original evaluator. Deferred values,
generated or composite record IDs, proof-cap exhaustion and insufficient depth
also use the complete original evaluator. The proof and its limits are unchanged.

A FOREACH-private operation holds only that choice and is called at the same
location with the original stack, context, Options and document. It adds no
stack boundary, depth charge, public API, test switch or cache. Original range
conversion, timeout checks, per-iteration contexts, body order, loop control,
yield, permission/event effects and transaction rollback remain unchanged.

Five workspace `native_value_compute_tests::foreach_ownership` controls cover
actual native FOR values, the retained fallback, caps and nested control flow,
deferred range effects, duplicate events/rollback and record/global permissions.
Four vendor `sql::statements::foreach::ownership_tests` controls independently compare
the actual private per-element operation with the frozen unconditional b028
value evaluator; materialized input avoids range evaluation masking root depth,
caps, deferred errors and effect counts. The vendor controls are a separate
execution target and do not count as executed workspace tests. All controls are
source-only until separately run. No measured speedup or ancestry PASS follows;
the original 256-Work / 257-Source whole-case 240-second oracle is unchanged.

## Physical source checkpoint hashes

The explicit allowlist covers 866 original upstream files. The following
SHA-256 values bind every patched or added core/SDK file and the separately
owned final SurrealKV packaging metadata at this source checkpoint. Original
archive manifests remain the authority for unchanged upstream bytes. This is
source provenance; it does not claim compilation or runtime qualification.

| Path | SHA-256 |
| --- | --- |
| `packaging/recall-surreal/source.lock.json` | `2145adbc99bd9b92f6ebb752bd6297e25dc0b4b613e278af4f8ff01dc48d39b7` |
| `vendor/surrealdb-2.6.5/Cargo.toml` | `cf9d919b73102154286523f7db08f4a6111b1071a762677b5ebc8f07f1c8ee52` |
| `vendor/surrealdb-2.6.5/RECALL_PROVENANCE.json` | `f9600d67467ad6e56230e74de68b9980626a4098ba8d9850d4bf4ccd089644a9` |
| `vendor/surrealdb-2.6.5/src/api/conn/cmd.rs` | `2899ab6bb8785af2e6d94a66b13538b90272b6c53e80ee8c5aa3593cf0b7bbc7` |
| `vendor/surrealdb-2.6.5/src/api/engine/local/mod.rs` | `2fb383c594248e75d541f9864d0379cc282986d8e2ce4af2d33b91e94489a58b` |
| `vendor/surrealdb-2.6.5/src/api/engine/remote/mod.rs` | `5f9ca74cda88a57b1b9208a06ca0005f603437f17711f57071220e52960b95ae` |
| `vendor/surrealdb-2.6.5/src/api/method/mod.rs` | `494ce4da24f74dc334f462205c40133a99de817120ce98598fba26e22f400c2f` |
| `vendor/surrealdb-2.6.5/src/api/method/query.rs` | `e16cda25dc1d3454df5e74db8a11050f6cf91be00dbc70437c9f4884d33cac22` |
| `vendor/surrealdb-2.6.5/src/api/method/recall_history_inventory.rs` | `87d7ed9b2e51793d4119cb471f4cf973c5c07a58d646b8b57d9a88580b1fc4ad` |
| `vendor/surrealdb-2.6.5/src/api/method/recall_physical_checkpoint.rs` | `a92d14b4ebde2412df95668eec70196c20dd957430ce98a060732182b3972450` |
| `vendor/surrealdb-2.6.5/src/api/method/recall_physical_history_checkpoint.rs` | `fbf8a475ba0b326c9c2af45726eb36e609f0e7693c6166351e2d50b9a739d362` |
| `vendor/surrealdb-2.6.5/src/api/method/tests/server.rs` | `a37d0b7be81de61dbbdee99f8e43182c47566765c724080c4326590c5940fa48` |
| `vendor/surrealdb-2.6.5/src/lib.rs` | `256b5cb692026cb2f46563be225ae6d45678335803b62ae177aad12f98a4d4b6` |
| `vendor/surrealdb-core-2.6.5/Cargo.toml` | `4b0658836dfc1b4da1f3c4156fae87ded574e665e18a2d65e1a35664c67c37a6` |
| `vendor/surrealdb-core-2.6.5/RECALL_PROVENANCE.json` | `ef2b80584e11a9925959cdd94703eabf3e979e56bdc5e349b7eec190f271e11a` |
| `vendor/surrealdb-core-2.6.5/src/ctx/context.rs` | `e286a123d31375dc1e091f74b15b7f6ae389d8304332c0a2988d902b8bd8321e` |
| `vendor/surrealdb-core-2.6.5/src/dbs/iterator.rs` | `f33ad12ade2fd0eac9e0ccdf3230654431b2b2789c1e6c3638ae6fc2a9d1233d` |
| `vendor/surrealdb-core-2.6.5/src/doc/document.rs` | `fa5aabd0225f7d51f3ba596c10e93dc719dc3379bd266a060fb3eaecbd46d209` |
| `vendor/surrealdb-core-2.6.5/src/doc/field.rs` | `d684c77809b55dbf71383fd94983117c47916419cc72d13ba5198338c0546fce` |
| `vendor/surrealdb-core-2.6.5/src/doc/process.rs` | `df6f13823cb110984add3d7ee27eb0893427ecfaf44796f05c1d0f2159cd6be9` |
| `vendor/surrealdb-core-2.6.5/src/fnc/array.rs` | `2a7674beb8afd260b34265c78029e75ea938ef65f651310f7ab98c5fbd94d105` |
| `vendor/surrealdb-core-2.6.5/src/fnc/mod.rs` | `5cfbb39b5c8e74060cf349c14a27a658280ea712b9966ac0b3bbf043a9887fc1` |
| `vendor/surrealdb-core-2.6.5/src/fnc/object.rs` | `c3e8faaf1c8a3ec3cf322e64d893512213eee5c3a42022d1fd2426ae29a8c3e4` |
| `vendor/surrealdb-core-2.6.5/src/fnc/recall.rs` | `9491071b934b425a73ada8a3c86d603ad65dc42a37ee5b2d31570fbc59af07c0` |
| `vendor/surrealdb-core-2.6.5/src/fnc/recall_hmac_tests.rs` | `6887f5b2aee1cf124d6245fd1e54ac78afce905419a14df7e3151ec1453d484b` |
| `vendor/surrealdb-core-2.6.5/src/idx/index.rs` | `a180c9479bdda4add4c6cfefb73e97b042065fd6c2d8fea997447bc1bf8429e3` |
| `vendor/surrealdb-core-2.6.5/src/idx/trees/store/hnsw.rs` | `f02f77cf036fffaabf4e48f55e47fa4b4981bf4f6f18c28fd89d76df72ab85e4` |
| `vendor/surrealdb-core-2.6.5/src/idx/trees/store/mod.rs` | `a3f760d9b5eb4917d3813d0ca7e2016aa6f8f9ee0b9356924a5115e034d1464d` |
| `vendor/surrealdb-core-2.6.5/src/key/debug.rs` | `23c3efcd76d8dd56473b517c858ed53b225acf22c875644942612bea024469f3` |
| `vendor/surrealdb-core-2.6.5/src/kvs/ds.rs` | `432b264087e374c50ea1294f1ab5af6ee58891b81594c75603d47ad414c7f9d5` |
| `vendor/surrealdb-core-2.6.5/src/kvs/index.rs` | `42fb8e0fd0e94f69cbfd1ed857ce22fc59597db9f2e18f6456183e7f8c87e660` |
| `vendor/surrealdb-core-2.6.5/src/kvs/mod.rs` | `dc2e46c74f4b012748a6be745f391dcdf7381b92ae1477cc0e248e06c3ca96be` |
| `vendor/surrealdb-core-2.6.5/src/kvs/physical_generation.rs` | `8f9695ab0cbb6774e8cf9be5c925c0decfc68c6c59cf55ce72ac0bad2f4c2a84` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_history.rs` | `a98d012e81edd5fb377f1748a66eefa903e659ae5d1ef2173f2f39f94e984d8a` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_history_inventory.rs` | `e4261a7ec6bf7578fd4251a0eced4d65a706507edc95fdef3abf5acf48fcb316` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_history_selection.rs` | `a944085c45ddf4b2e22faee9b875aab1807ce47c994cec9c86e936e1fc749cd3` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_history_tests.rs` | `103456659abeeb057af05e24df8d9d8f47749e7a28f99e0712d677813d044436` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_index_erasure.rs` | `7c7da303ae6d98028fd5bcefd65136cb1fb445bc029f57ed0e2b371d8ae56466` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_index_erasure_tests.rs` | `6c5be8665747fb35dc7415c38d2cd188b541f52801d60a96cc73f81c1118fe0b` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_memory.rs` | `c39881c58712d0987b31398effefbf78b7d2e88e4bac635225e89e610f5e5604` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_physical_tests.rs` | `c1452a882ea4a59fc4d670c3e6ff89cd0306c19122e6700b53c036119c7a7a02` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_scalar_projection.rs` | `04d486d9825cd3480915c324764550c63f05a55cfb2515b326b5030bfbd19ad4` |
| `vendor/surrealdb-core-2.6.5/src/kvs/recall_scalar_projection_tests.rs` | `3a6ed847170bb455e8f5eeb976ff36e9aa5424d9ce76b289b32f4bbfde841fdf` |
| `vendor/surrealdb-core-2.6.5/src/kvs/rocksdb/mod.rs` | `a929c7777205a652adf1aee963ad816ff3bd4f4b58cae859bc02aa00871ab2f3` |
| `vendor/surrealdb-core-2.6.5/src/kvs/rocksdb/physical_checkpoint.rs` | `778dae3de7f51c474d2035fa884956fe78b01762d32bb38d95c524547d6d1610` |
| `vendor/surrealdb-core-2.6.5/src/kvs/rocksdb/physical_checkpoint_tests.rs` | `ddf7e6a6206b9a552127c04d79cbc79e750fb80b8193c2720727873d2a4bae11` |
| `vendor/surrealdb-core-2.6.5/src/kvs/rocksdb/source_erasure.rs` | `89634b7930bb10d819f6924602881a4bf9a2c8e59d775eb52eb4f6417f32d2bc` |
| `vendor/surrealdb-core-2.6.5/src/kvs/surrealkv/mod.rs` | `242c70d736317c662bb26ec95a3ba7800786f481698265f054d8b4f407436bb6` |
| `vendor/surrealdb-core-2.6.5/src/kvs/surrealkv/physical_checkpoint.rs` | `f8958f0133964181b75a1dbb71b314a77aaf0b7170919cdffc20dea7ea03d8e6` |
| `vendor/surrealdb-core-2.6.5/src/kvs/surrealkv/physical_checkpoint_tests.rs` | `7660b6d5fd9b8d735226f6da2f30b0a4a5b08d45ce3f3c66d4ced952ce03bb7f` |
| `vendor/surrealdb-core-2.6.5/src/kvs/tr.rs` | `b2dbe09b9c04ccf3a0ed6f25c9e2c77ff651ed6557138fdf5c464d87f149adf3` |
| `vendor/surrealdb-core-2.6.5/src/kvs/tx.rs` | `c88abf17f22a95b7895be3da901c98f477f0213bd20d301ab575eed763fa9025` |
| `vendor/surrealdb-core-2.6.5/src/lib.rs` | `ea0f550f9e028e5336ee8f78e73b8dfc6ac2be2ba4f4432d812673e904ddc826` |
| `vendor/surrealdb-core-2.6.5/src/rpc/method.rs` | `d0b3e045d659ef12d2169f709dbd101d89ca47247107ad0edce9f310f9aac823` |
| `vendor/surrealdb-core-2.6.5/src/rpc/mod.rs` | `9d26e6c6cc4d0483cc0d496200e9e7519fe3ddbed57c53819085587498390d13` |
| `vendor/surrealdb-core-2.6.5/src/rpc/protocol/v1.rs` | `24f1d997dc3a556aa17b116156ce94c94201833d79540789a40309a03f102ba0` |
| `vendor/surrealdb-core-2.6.5/src/rpc/protocol/v2.rs` | `b5c6858b3e91e6f76778935811bdf4253ed48ef06751f63c2ebf903610bf334b` |
| `vendor/surrealdb-core-2.6.5/src/rpc/recall_history_inventory.rs` | `e6eb697fc4a8062ff0ea7395fdf60fc041c7c61e4cccad33c1d27bd12e994978` |
| `vendor/surrealdb-core-2.6.5/src/rpc/recall_physical_checkpoint.rs` | `55904edc5faabf1206a963f655a686648a3e34563a5b46210187b9fbe892dc55` |
| `vendor/surrealdb-core-2.6.5/src/rpc/recall_physical_history_checkpoint.rs` | `9a3e9fb03ebeedd39dcd9a83e7a5353a82ea717b66c24ff50a95a1bff7c68080` |
| `vendor/surrealdb-core-2.6.5/src/rpc/request.rs` | `a5d63dbb39d1552d8929ca2db5601e82074fcce14fc1cce7864c88aa4d276cc6` |
| `vendor/surrealdb-core-2.6.5/src/rpc/response.rs` | `32f4cda5c8922c5da05b3ca8604bc783455474ae47a4bb522fdd627f33df882e` |
| `vendor/surrealdb-core-2.6.5/src/sql/array.rs` | `5667fc31d4482443ded2de98dc4b6559f6aec0e5ed9199f19351dd25e1783f7a` |
| `vendor/surrealdb-core-2.6.5/src/sql/closure.rs` | `62b4d56be4acbcb195f29fc8cdd25242543f0a1bc517a5e3b7c437e012879d16` |
| `vendor/surrealdb-core-2.6.5/src/sql/idiom.rs` | `f9d33c97a4c7ed56c4399400b313c08dac0844b35cb2ecd93a867ba13876e94e` |
| `vendor/surrealdb-core-2.6.5/src/sql/object.rs` | `cadcb7ba4f573c97a24f3366f26ee67dc8a978299a2ae507ea83d6c20033a052` |
| `vendor/surrealdb-core-2.6.5/src/sql/statements/foreach.rs` | `407cb4e448f65e05773b0c91310c7e812e084a566d167310c9674b4f3a90991e` |
| `vendor/surrealdb-core-2.6.5/src/sql/statements/foreach_ownership_tests.rs` | `3cd2edd98289fc41bb093d1ea6b452df14111402b48a422d4b4d3448b25bf35f` |
| `vendor/surrealdb-core-2.6.5/src/sql/statements/select.rs` | `c92495170d815c15c33ea5f731e258364be1b7fe4c8909060563cbf015e7f3c9` |
| `vendor/surrealdb-core-2.6.5/src/sql/statements/update.rs` | `17ddfa1b63f2c46baeb19ea6187a9e29151584df39057681618bf1de7ae1dbc7` |
| `vendor/surrealdb-core-2.6.5/src/sql/subquery.rs` | `c6e5447abbbe2c389c9b9ed2cdcd875470b785cf32616650b2c0283e3fc6c050` |
| `vendor/surrealdb-core-2.6.5/src/sql/value/dispatch_tests.rs` | `3402507eac4dd1061199ce9807803ed6759a0afb40fd1ad7771e75962650b065` |
| `vendor/surrealdb-core-2.6.5/src/sql/value/every.rs` | `ce3adac13a88fe6fb94c38bec4a41b2ac488342ccf6be25fb8d97a9b26da6a98` |
| `vendor/surrealdb-core-2.6.5/src/sql/value/get.rs` | `e52e335b638606d592bbbcd720b0091451af9bd154187cf4f74a8d5e7c2c9843` |
| `vendor/surrealdb-core-2.6.5/src/sql/value/missing_field_tests.rs` | `f69755602db74f9b5ec41f8be6c0b175398f57cbc22246c9183d75050c871ff7` |
| `vendor/surrealdb-core-2.6.5/src/sql/value/value.rs` | `381fc36701d5b934f41257275df30e266eb3d74ee706e81a17a2165f7f784c48` |
| `vendor/surrealdb-core-2.6.5/src/syn/parser/builtin.rs` | `b3597a5ef162bbea9ef8a5f9f7375109ea4f08b6ae0a7a9bcd86f7e5005fab20` |
| `vendor/surrealkv-0.9.3-backport.md` | `13504c8694478246abc92a8789a37b593831b72ac1e51372f2efbe92efd5a9bb` |
| `vendor/surrealkv-0.9.3-backport.patch` | `45d6c66b1fc6664902fb376bde47e0bb1d18d19b73e7ad69b3816aa556647464` |
