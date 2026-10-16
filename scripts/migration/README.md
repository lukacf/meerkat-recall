# Offline preservation planner

`preservation_plan.py` prepares local evidence for a preservation migration. It
does not connect to Recall, restore a target, accept sources, execute CAS, grant
erase authority, call a provider, or certify searchable publication. The released
`data-migrate` command remains a bounded legacy transfer; this planner does not
establish a supported application cutover by itself.

The input contract follows HomeCore's `recall_transition` source at
`0bfba844203c71d8d5b07594179404b2f74f04bc`. An inventory needs the original
`elephant-db` version 1 JSON snapshot and either its matching
`inventory.idmap.jsonl` or an explicitly bound conveyor ledger backup.
Full-corpus T2/T2g exports are not required for this inventory. A later delta
validation needs an actual S1 snapshot and its complete
`reconcile.changes.jsonl`; neither is manufactured from the initial snapshot.

## Prepare an inventory

Supply hashes obtained independently from the input producer. The output
directory must not exist. Commands below illustrate explicit local inputs; they
do not authorize a consumer export or migration run.

```sh
python3 scripts/migration/preservation_plan.py inventory \
  --snapshot /offline/s0.json \
  --snapshot-sha256 "$S0_SHA256" \
  --idmap /offline/inventory.idmap.jsonl \
  --idmap-sha256 "$IDMAP_SHA256" \
  --space default \
  --target-space household \
  --output /offline/plan-s0
```

For a fresh acceptance run, take the store cut first, then create the ledger
backup using HomeCore's read-only SQLite backup procedure. HomeCore's conveyor
records KEEP before ingest and does not revoke those decisions. Extra KEEP keys
without a document in the cut do not create sources. The planner reads only the
explicit offline backup; it does not acquire the store cut or copy a live ledger.

```sh
python3 scripts/migration/preservation_plan.py inventory \
  --snapshot /offline/fresh-s0.json \
  --snapshot-sha256 "$S0_SHA256" \
  --ledger-backup /offline/conveyor-backup.sqlite \
  --ledger-sha256 "$LEDGER_SHA256" \
  --ledger-cut-order store_cut_then_ledger_backup \
  --space default \
  --target-space household \
  --output /offline/fresh-plan-s0
```

This mode generates `inventory.idmap.jsonl` in the same snapshot pass, hashes
its exact bytes and binds the ledger hash in the plan. It implements the pinned
HomeCore single-key/KEEP classification. It refuses WAL-format headers, ledger sidecars, duplicate
KEEP keys, hash mismatches and malformed keys. The cut-order argument records the
operator's declaration; acquisition receipts must prove the actual ordering.
The original HomeCore documents use space `default`. This input scope does not
rename records to `household`; target mapping requires an explicit runtime
projection and receipt. Original lexical bytes and hashes keep their real space. `--target-space` records
that explicit mapping in the plan and refuses an existing population in the
target space. If omitted, it records identity mapping. Runtime conversion must
validate every typed relocation and cross-space reference; the planner does not
perform that conversion.

In the default `native` disposition, imported hot and archived work adds a
`historical_work_hold_v1` runtime
requirement with exact counts and snapshot binding. Running rows are preserved
unchanged. The planner cannot verify a target hold: runtime must register and
reconcile every private Unknown historical marker inside transfer custody before
readiness. A plan with that requirement is not permission to release copied work.

The explicit `managed_archive_v1` disposition preserves the complete fixed
operational class in a journal-managed archive instead. Those rows retain exact
original bytes and remain outside native work and ordinary history queries. The
report distinguishes archive-inert history from native historical holds and
accounts for every original row. Use the same disposition across all source
cuts; see [the managed archive workflow](MANAGED-OPERATIONAL-ARCHIVE.md).

The native exporter at `e40da5088940f58d30511f39531a4cd8ae7348b8` adds an
`export_notes` object. The planner validates its exact field set, source-table
counts/ranges and embedding partition mappings. It streams the potentially large
`name_embedding_metadata_shadows` array one entry at a time, retains original
lexical parts and hashes in `export_note_parts`, and checks shadow entity IDs
against their original output ordinals. Native precision-loss and geometry-review
counts remain explicit blockers. All bytes remain bound by the snapshot hash;
unknown headers or note fields are refused. This preserves exporter evidence;
it does not undo normalization or certify legacy vectors as current profiles.

The reader consumes one JSON row at a time and uses a SQLite disk index. The
default limit is 64 MiB of UTF-8 bytes per JSON value or JSONL line, configurable
for inventory with `--max-value-bytes`. This is an input-size bound, not a process
RSS guarantee: decoding, canonicalization and indexing require additional memory.
The parser can read ahead by up to one chunk. It rejects malformed envelopes,
duplicate JSON keys, invalid UTF-8, nonfinite numbers and duplicate row identities.
Table counts include explicitly declared empty tables. All rows are inventoried;
modern authority and unknown tables remain visible as runtime blockers.

Successful output includes:

- `plan.json`: input hashes, counts, explicit hash contract and output hashes.
- `inventory.sqlite`: all table/key/row hashes, original lexical row bytes and
  document identity joins.
- `documents.jsonl`: each original document's identity and exact text evidence.
- `groups.jsonl`: every stable external source ID and its document mapping hash.

`hc-src-` and `hc-legacy-` identities follow the pinned HomeCore mapping. Several
derived documents can share one external source ID; the planner preserves every
member instead of selecting a representative. A conflicting identity mapping or
an idmap whose title/text hash disagrees with the snapshot is refused. An idmap
entry for a deleted, missing or foreign-space document is also refused.

No `Subject`/`From` headers or truncation are applied. Three distinct values must
remain distinct:

1. `doc_content_sha256` hashes canonical JSON containing original title and text.
2. `original_text_sha256` hashes the exact original UTF-8 text bytes.
3. An accepted request fingerprint depends on the agreed runtime adoption/client
   envelope. It is `null` here, as is the accepted revision. Neither content hash
   authorizes a no-op source resend.

The explicit `homecore-python-json-v1` hash domain uses Python's sorted-key,
compact, unescaped-Unicode JSON encoding. Integers retain arbitrary precision;
finite floats use Python binary64 parsing/rendering. Thus `1`, `1.0`, `true`, and
negative zero can have distinct hashes. This matches the pinned HomeCore
reconciler; it is not an assertion that Rust's JSON serializer produces the same
bytes. Runtime integration must use an explicit compatible adapter. Whole-file
SHA-256 also binds the original lexical input bytes.

The `recall-original-lexical-row-spool-v1` disk index retains each row in
`raw_rows(t,k,lexical,lexical_sha256)`. The BLOB contains the exact original UTF-8
JSON value, including whitespace and numeric spelling. Its lexical hash differs
from `rows.h`, which uses the Python canonical domain above. A Rust adapter must
parse the lexical bytes, verify typed lossless conversion independently, and
keep its runtime fingerprint domain distinct. The final plan binds the closed
SQLite file hash, original backend/version/exported_at header and the snapshot
hash consumed during the same inventory pass.
Readers must verify those bindings, row identities, counts and raw hashes; the
index is untrusted evidence until validated. Original bytes add disk usage
roughly proportional to the snapshot, which must be admitted before a real run.

The additive `reference_candidates` table indexes exact record-shaped strings
and their JSON-pointer paths during the same pass. The `reverse_references`
index supports bounded lookups by referenced table and ID. These are lexical
candidates: typed consumers must validate the field and its semantics. Text,
metadata and dictionary keys can contain similar strings; the index grants no
ownership and does not infer membership through a shared entity.

`fixtures/python-raw-documents-v1.json` contains synthetic Python-produced raw
documents and exact declaration hashes for omitted and explicit-null optional
fields. Rust integration must check these actual bytes, rather than reconstruct
raw policy/provenance from typed defaults.

## Validate a complete delta

```sh
python3 scripts/migration/preservation_plan.py delta \
  --baseline /offline/plan-s0/inventory.sqlite \
  --baseline-sha256 "$INVENTORY_SHA256" \
  --before-snapshot-sha256 "$S0_SHA256" \
  --after-snapshot /offline/s1.json \
  --after-snapshot-sha256 "$S1_SHA256" \
  --changes /offline/reconcile.changes.jsonl \
  --changes-sha256 "$CHANGES_SHA256" \
  --output /offline/plan-delta
```

The planner validates before/after hashes against S0, checks every S1 row, and
requires an explicit delta for every removed row. Missing graph-only changes,
duplicate identities, forged before-images, changed snapshot bindings and
unsupported authority changes fail. It records added/removed empty table
declarations. Change class, flags and dependency rank are retained as advisory
metadata, never as replay authorization or a sufficient execution order.

`changes.jsonl` retains complete before/after rows, including graph-only edits,
soft deletes, restores and removals. These files may contain private text and
must remain private. `delta.json` is written only after complete validation and
hashes that output. The read-only baseline file hash is checked before and after
validation. A failure leaves a partial directory for diagnosis; without the final
manifest it is not a valid plan. Choose a fresh output directory for a correction.

If S1 is a fresh export with no row differences, retain its own snapshot hash and
prepare its S1 inventory normally. An empty changes file is valid only after the
planner verifies every S1 row against S0 and all table declarations. The runtime
still closes a new source-bound baseline: zero changed rows do not make two
different snapshot files interchangeable. Only an exact same-file declaration
with matching identity plan and configuration can use the verified no-op path.

The output captures an exact before-image guard or an absence guard and a
deterministic operation ID. It does not execute that guard. Runtime replay still
must bind current source heads, generations and epochs; validate ownership,
policy and graph support; and atomically apply rows with recovery/idempotency
receipts. A delete or removal requires explicit destructive intent and never
implicitly authorizes physical erasure. Independent and agent-authored graph
support must survive source replacement according to the runtime contract.

## Remaining migration qualification

Historical adoption must preserve physical document/revision/node/span identity,
record the actual new migration execution, and prove publication without inventing
old extraction or worker receipts. Copied pending work must remain inert. Existing
vectors remain legacy payload until their profile lineage and coverage are proven;
model name and 3072 dimensions alone do not establish compatibility.

After inventory, the source implementation accepts `plan.json` through
`data-migrate --preservation-plan ... --historical-config ... --report ...`.
Its v2 report binds the closed `.transfer.sqlite` and
`.catchup-bindings.jsonl` companions. `data-migrate-delta` consumes those exact
files and complete S0/S1 plans; its completed v2 report emits `.catchup.sqlite`
and `.catchup-bindings.jsonl` for a later delta. Preserve earlier baselines and
the original journal. See [the CLI contract](../../docs/cli/overview.mdx#adopt-a-prepared-historical-snapshot)
for command arguments, explicit deletion intent and interrupted-operation recovery.
These source changes remain subject to runtime qualification; planner success is
not transfer, publication or cutover success.

In native mode, unchanged historical work holds and event checkpoints keep
their original snapshot provenance through a fresh zero-row cut. A later operational change
must join that exact control row to the predecessor's storage-retained completion
certificate and closed native inventory. The runtime retains full before-image
checks and rejects missing, foreign or changed evidence. It never treats an old
snapshot hash supplied by the caller as authority, releases historical work, or
replays the retained event tail. Preserve the original journal and all completed
baseline companions for this chain, including a certified empty population.
Managed-archive mode instead publishes an exact successor archive receipt for
each fresh cut, including a zero-row cut. Its native event sequence floor retains
the earlier maximum when an event is removed; archived work stays outside native
scheduling. Both paths retain the same complete S1 and recovery obligations.

Finish the final original-store cut and all historical deltas before governed
embedding-only reindex, activation and imported-publication certification.
Those destination-owned changes invalidate the old historical baseline; the
implementation does not recapture a new baseline from arbitrary live state.
HomeCore keeps Elephant stopped from final S1 through switch and retains the
store-file manifest as freeze evidence. Its new-document semantic backlog stays
outside the historical population and enters ordinary ingestion after activation
and certification. Consumer writes remain stopped until certification.

The required full import duration is at most one hour. The clock includes fresh
live export, transfer, planning and identity-map generation, load and adoption,
space and grant setup, any embedding-only reindex, and reconciliation. No dataset
preparation may happen before that clock. Frozen snapshots and earlier T1 maps
are diagnostic fixtures only. This planner has no whole-corpus throughput result;
report each phase and the total. A short final delta after unbounded preparation
does not meet the requirement.

Fresh identity generation must bind the snapshot and the matching conveyor
ledger KEEP-key decisions. HomeCore classifies a single metadata key as derived
only when the ledger kept that key; snapshot metadata alone is insufficient.
Do not silently reuse an older T1 map or change existing external source keys.

Synthetic checks run without product services or Cargo:

```sh
python3 -m unittest discover -s scripts/migration -p 'test_*.py' -v
```

Inline media is checked during the same bounded snapshot pass: strict canonical
base64, decoded `size_bytes` and SHA-256 must agree. `media_preservation` reports
verified inline bytes and external references. The planner never opens a
`storage_path`. External media in either snapshot requires separately verified
preservation availability; such a plan remains blocked and is not self-contained.

A validated delta retains the known global `migration_history` maintenance
receipt as source provenance, with exact S0/S1 hashes. It has no tenant space and
never becomes a target schema migration or an instruction to execute one.
Unknown receipt identities, extra fields and malformed timestamps are refused.
The runtime adapter must join the retained S0 value to the original transfer
report and snapshot, then retain both values in the catch-up receipt chain.
