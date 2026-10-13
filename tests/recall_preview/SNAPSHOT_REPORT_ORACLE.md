# Independent exported-inventory component

`scripts/qualification/snapshot_report_oracle.py` is a pure independent predicate
over F's actual three captured reports and original fixture/journal evidence.
It imports no product reference selector, database, subprocess or network code.
`evaluate_export_bytes(pre, post, replay, fixture)` is the preferred collector
entry point: exact raw byte lengths/SHA-256 are checked against the fixture and
duplicate-aware JSON parsing rejects duplicate keys and nonfinite numbers.
`evaluate_exports` also exists for previously validated parsed objects; its
result explicitly leaves raw capture custody to its caller.

The fixture contract is `recall-physical-fixture/v1`, with uppercase `A`/`B`,
distinct named spaces/source incarnations, an actual URI canary in each original
Metadata source, original accepted and completed public DTOs, and the original
durable erasure intent. Intent timing uses the collector's real
`erase_completed_observed_at`; the public operation DTO has no server completion
timestamp. All previous intent files must retain their original bytes, and the
one added intent must belong to A within the observed erasure interval.

Both original physical Source and immutable SourceRevision must be present.
The oracle independently follows exact typed/domain references from complete
pre-erasure rows and requires every source-exclusive physical identity to be
absent after erasure and after replay. Soft deletion does not qualify. The
original A URI canary cannot survive in any row/key/payload/schema. Every B-owned
row retains its exact typed identity and independently recomputed full-record
digest, including timestamps and deleted rows. An arbitrary UUID substring in
ordinary prose and a different table sharing the same UUID are not source
references. The shared external key is intentional and cannot be globally banned.

Retained source heads, identities, authorization envelopes, sanitized operations,
tombstone references and the known payload-free erasure event are checked by
explicit field schemas and original operation/intent bindings. The mirror
`state` and serialized `payload` must agree. Unknown retained A-bearing structures
return `unmet`. New native read-fence buckets permit only the exact bounded
table/three-hex-digit key plus version 1 and UUID nonce; unexpected payload fields
are never exempted merely because of their table name. These exported checks do
not replace runtime restore admission checks.

Physical record IDs must match their outer typed inventory identities. Boolean
`true` cannot satisfy a native integer version. Original private operation
generations are bound to the actual typed ingest acceptance operation's
`source_generation` field. Later head/operation DTOs use `generation` instead.
F retains the original typed ingest receipt unchanged. Erasure operations retain the
product's payload-free `unknown_original` producer tag and four-field original
`PublicationFence`, which has no invented `space_id` member.

Every native schema statement must match a source-reviewed exact `(SHA, SQL)`
entry. The reviewed set is currently empty pending E/L's closed schema contract:
**actual native schema inventories remain UNMET**. Identical definitions across
three reports and absence of the URI canary do not establish schema cleanliness.
No runtime can train this expected set from its own export.

V1 remains unchanged. Explicit v2 permits only the frozen lossless dictionary:
`blobs[raw_sha256] = {bytes, base64}` with canonical standard unpadded base64,
and `string_ref`/`bytes_ref` in row values. A shared raw blob can serve both types;
the String occurrence must decode as UTF-8 and the Bytes occurrence stays Bytes.
References cannot appear inside physical identities, reference inventories or
Thing keys. Every original expanded tagged row hash is recomputed one row at a
time; the expanded full inventory is not retained. Dangling/orphan references,
wrong lengths/hashes, unknown keys/tags and noncanonical encoding refuse.

Limits remain 32 MiB plus the CLI newline per report, 64 MiB unique dictionary
raw bytes, 64 MiB aggregate logical String/Bytes leaf bytes counting every row
occurrence, 14,065,536 bytes per leaf, 200,000 rows, two million tagged nodes,
depth 128 and 100,000 native reference occurrences. F retains the original whole
capture/process limits. Real 2 MB source/Graph export size feasibility remains
unmeasured; a bound failure is `unmet`, never truncation or a smaller product
source limit.

Every result has `scope: exported_inventory` and `product_acceptance: false`.
Even a component `pass` does not establish live database/PV/WAL/media removal,
registered-backup retention/expiry/removal, installed artifacts, continuous
restore barriers, model semantics or full OB3 acceptance. OB3's physical deletion
and bounded backup retention remain distinct from Recall's restore replay gate.

`snapshot_report_oracle_test.py` authors synthetic closed-world controls for both
backend representations, full-record preservation, soft-deleted survivors,
unknown payload exemptions, journal replacement, malformed/incomplete evidence,
typed identity distinctions, raw capture custody and strict v2 decoding and
expansion bounds. These fixtures are parser/predicate test source, not captured
product exports. No helper, test, database, socket, Cargo or provider execution
was performed at this source checkpoint; the parent schedules validation.
