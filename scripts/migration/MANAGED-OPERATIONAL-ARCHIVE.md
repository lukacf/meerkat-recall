# Managed operational history: private migration workflow

This describes the migration source under development. It is not a release or
cutover qualification. The archive, lifecycle, catchup and administrative command
changes must pass the combined runtime controls before a consumer run. The
released 0.9.1 documentation and its acceptance evidence remain separate.

## Explicit disposition

An inventory defaults to `native`. Select the optional archive disposition when
creating each original inventory, using the producer's independently supplied
snapshot and identity-map hashes:

```sh
python3 scripts/migration/preservation_plan.py inventory \
  --snapshot /offline/s0.json \
  --snapshot-sha256 "$S0_SHA256" \
  --idmap /offline/inventory.idmap.jsonl \
  --idmap-sha256 "$IDMAP_SHA256" \
  --space default \
  --target-space household \
  --operational-disposition managed_archive_v1 \
  --output /offline/plan-s0
```

Use the same explicit disposition for S1 and subsequent inventories. A completed
native baseline cannot silently become an archive baseline. Mode, original
namespaces, source hash, spool hash and source inventory participate in the
immutable plan and report bindings. A missing mode on an older plan means native;
an unknown mode is refused. Do not edit an existing plan or receipt to switch.

The entire fixed operational class is archived: `audit_log`,
`cost_ledger_daily`, `work_item`, `work_run`, `work_item_archive`,
`work_run_archive`, `work_artifact`, `work_edge`, `work_deadletter`, and
`system_event`. Declared empty tables remain distinct from absent tables.
Knowledge, document, revision, node, graph, media and vector rows continue through
their typed native transfer. Unknown tables and unresolved preservation blockers
remain refusals; there is no general table-skipping option.

The archive retains exact original JSON bytes, including whitespace, numeric
spelling and source space. The source-to-target space projection is separate
metadata. Independently computed table counts, lexical byte counts and ordered
row digests must agree with the plan and the sealed archive. The global source
inventory still covers every original row. Native row counts and hashes report
actual native rows only; the report separately identifies the archived rows and
recognized retained global controls.

## Inactive historical rows

Archived work retains its original status and payload but is excluded from
ordinary work lookup, recovery, claiming and dispatch. It has no newly fabricated
worker completion or native historical-hold rows. The report identifies this as
`managed_archive_inert`; it does not claim the rows were copied into native work
tables. Historical audit and cost rows are also outside ordinary live queries.

Archived events are preserved history, not delivered effects. Initial transfer
reserves the verified original sequence floor and both historical consumer
positions in each applicable namespace. Future native events allocate above
that floor. Catchup must preserve the previous maximum when a later source cut
removes an old event; it cannot acknowledge intervening destination-owned events.
No historical event replay is implied by a checkpoint.

## Required custody and cut sequence

The archive belongs to the original destination journal. Its intended private
path is registered before any payload is written; its closed file, exact seal
and native anchor are bound to the completed migration. These generations are
non-expiring preservation dependencies, not disposable files under the ordinary
backup retention window. Preserve the original journal, archive generations,
completed reports and their closed ledger/binding companions together.

The supported order remains S0 transfer, final original-store cut, all historical
catchups, governed embedding-only reindex, explicit activation, imported
publication certification, and then ordinary consumer writes. Destination-owned
reindex or live writes do not grant permission to recapture a new historical
baseline. An archive seal alone does not certify current embedding coverage or
readiness.

A fresh zero-row source cut still needs its own source-bound completion. An
archive-only change still needs exact predecessor evidence and a completed
native/archive transition. Deletions require the existing explicit delta intent;
they do not authorize physical erasure. The delta staging index contains only
source row keys, hashes and byte lengths; bounded batches read their original
lexical bytes from the immutable input spools and verify those hashes. It does
not create an unmanaged second copy of archive payloads. Preserve earlier immutable generations
and baseline companions for S1-to-S2 replay.

Initial transfer failure retains the existing incomplete-transfer marker and
fails closed. This source slice does **not** provide automatic initial report
reconstruction or a general retry that adopts partial destination state. Keep the
failed target and receipts for diagnosis. The archive-aware explicit discard
implementation binds the original failed-transfer marker, records archive
removal intent before deleting payloads, clears the native target, and then
clears the exact registered archive state. The command wrong-marker refusal,
registered payload removal and fresh reimport control have passed. Do not
substitute a generic native-only discard. Catchup uses its separate exact
pending-operation protocol; its persistent interrupted-candidate, native/journal
commit-gap, ledger-gap and three-cut sequence-floor control has also passed.
Later selected controls also passed actual disk erasure/reopen and the original
combined backup/erasure/restore path; earlier failed receipts remain retained.
It can discard only the exact registered pending candidate under existing catchup
custody, reuse an already sealed candidate, recover a committed native archive
phase before its journal acknowledgment, and replay its immutable effect manifest
into the predecessor ledger. A missing or changed original input, pending marker,
receipt or artifact remains a refusal; retry does not capture arbitrary live state.

## Command completion and connection ownership

The composed connection-close implementation has passed six storage controls:
entered-write drain and clone admission, timeout/cancelled waiters, rejected
queries, backpressured export descendants, immediate RocksDB reopen, and failed
recovery initialization followed by reopen. On that composed source, eight CLI
controls passed: discard/reimport, native initial transfer and its inventory
fence, interrupted persistent delta, and four certification cases. Later exact
source successors passed real disk erasure/reopen and the original combined
backup/erasure/restore control. These results support the tested caller and
lifecycle paths; they do not prove deployment-profile process exit, the final
repository gates, or complete household recovery. The caller wrappers cover the planner-backed initial transfer, delta
and historical certification commands when they open their own local connection.
Injected library/test clients stay with their caller. Do not infer equivalent
coverage for other commands or a remote server shutdown.

With that implementation, command success requires both the operation result and
an awaited local connection close. Close rejects new requests, drains admitted
native owners and reports a timeout or shutdown failure as an error. It neither
clears an incomplete migration marker nor rolls back a completed operation. If
both operation and close fail, the command retains the original operation error
and adds the close failure. A completed report followed by a close failure still
records the completed operation; it does not prove successful process teardown.
Preserve the report and original journal, and use the exact operation-specific
recovery protocol. Never delete a target or rerun against a new baseline merely
because the process returned nonzero. The passed constructor control preserves
the original Recovery diagnostic phase and journal-mismatch cause; it does not
establish that every initialization failure or command teardown has been tested.

## Inspect and export

These are trusted local administrative commands. They require an explicit
existing local native target and the exact SHA-256 of a completed original
report. They do not accept an archive filesystem path and do not expose an HTTP
or ordinary search endpoint. Storage verifies the journal and native anchor,
resolves authorized surviving generations after redaction, and holds read
admission throughout the administrative callback.

```sh
meerkat-recall migration-archive inspect \
  --url rocksdb:///absolute/recall-store \
  --namespace elephant --database elephant \
  --report /offline/completed-report.json \
  --report-sha256 "$REPORT_SHA256"

meerkat-recall migration-archive export \
  --url rocksdb:///absolute/recall-store \
  --namespace elephant --database elephant \
  --report /offline/completed-report.json \
  --report-sha256 "$REPORT_SHA256" \
  --table work_item \
  --output /offline/new-work-item-export
```

Inspection returns original and resolved receipt identities, surviving table
counts and original declarations. It does not return row payloads. Export accepts
one declared table in the fixed class, including an explicitly empty table.
Unknown or undeclared tables and stale, altered or uncommitted receipts fail.
After a new source cut, supply that cut's completed report; an older cut is not
automatically adopted as current authority.

Export creates a new private directory and never overwrites or resumes one:

- `rows.bin` concatenates the exact original UTF-8 row bytes. Rows can contain
  newlines and trailing whitespace; this file is not JSONL.
- `rows.jsonl` contains one index entry per row: `table`, `id`, byte `offset`,
  byte `bytes` length and `lexical_sha256`. Read the specified byte slice from
  `rows.bin` to reconstruct each original row.
- `manifest.json` is published last, after the data, index and pending manifest
  are fsynced. It binds original/resolved receipts, source report SHA, row count,
  data byte count and file hashes. A partial directory without a valid complete
  manifest is not a completed export. A pending manifest is not completion.

The manifest labels the export `operator_controlled_external_copy` and
`managed_erasure_applies: false`. This new copy leaves managed archive custody.
The operator is responsible for its access, retention and subsequent deletion;
Recall cannot claim remote physical erasure of that external copy. Export does
not restore rows into live work tables or make them dispatchable.

## Restore and physical erasure boundaries

A database-only backup cannot establish preservation-complete recovery. Restore
must retain and verify the original journal and its referenced archive artifacts;
a copied native receipt cannot authorize missing or altered bytes. Do not rename,
replace or manually prune registered archive directories. Selected actual disk
reopen and combined backup/erasure/restore controls have passed, including the
original authority failures after exact writer corrections. Retain their source,
binary and result receipts; final release qualification and actual household
backup completeness remain separate requirements.

Source erasure must deny access before changing payloads, account for every
managed generation and recognized dependency, publish verified survivors, remove
the affected old bytes, and durably finish the original journal intent. A pending
redaction remains a refusal. A pre-erasure checkpoint cannot bypass the latest
original journal. Unknown ownership and shared aggregate reconciliation may
refuse; read denial alone is not proof of completed physical removal.

Original offline source snapshots remain under their original custody. Newly
created destination-managed spools, archives or export copies cannot evade the
lifecycle by being called diagnostic files. Separately declared external exports
have the operator-controlled boundary described above. The full combined
lifecycle controls remain necessary. Selected storage controls have passed for
redaction restart, foreign-namespace survivors and stale pre-redaction registry
refusal; those results do not qualify the complete administrative and catchup
command path.

## Timing and qualification

Record archive source paging, writes, inventory verification and sealing in the
same full import clock as fresh export, identity-map generation, planning, native
copy, adoption, all final cuts, reindex, activation and reconciliation. The limit
remains 3,600 seconds with no dataset preparation excluded. Existing small dev
profile timings do not predict the full workload or intended deployment profile.
The large native vector population remains a separate cost; operational archive
mode does not remove it.

The source-only synthetic driver selects this disposition with
`synthetic_scale.py run --operational-disposition managed_archive_v1`; its default
remains `native`. Each run generates fresh input inside its own clock and binds
the mode through input, plan, report and baseline. The complete household profile
is unchanged. It reports source rows, native source rows, archived rows, retained
global rows and the actual closed native inventory separately. Archive paging
and seal timings are subphases; do not add them again to elapsed CLI wall time.
This synthetic clock excludes live export, transport, final catchup, embedding,
activation/certification and switch, so a pass alone cannot satisfy the full
3,600-second requirement.

Before a consumer run, retain actual combined results for initial native/archive
inventory parity, event floors, S0/S1/S2 and fresh zero-row cuts, interrupted
recovery, exact administrative export, restore and all-generation erasure. Run
the final repository checks on the exact coherent source. No whole-corpus timing
or completed migration is established by planner success or static review.

## Operator handoff evidence

Keep these distinct in the handoff:

| Evidence | Required interpretation |
| --- | --- |
| Closed source cut, ledger backup and producer hashes | Prove the actual cut order and fresh matching identity map; an old diagnostic T1 map is insufficient. |
| Initial and successive completed reports | Bind every source table and row to native, managed archive, retained global or declared-empty accounting; preserve each original journal and baseline companion. |
| Recovery, archive export and erasure controls | Require actual selected tests on the composed source, including persistent reopen and interrupted native/journal/ledger boundaries. Static review and earlier passes do not supersede a later failure. |
| Synthetic initial-transfer timing | Measures only its stated generation, planning and initial-transfer stages. It does not measure live legacy export, final catchup, providers, activation, certification or consumer switch. |
| Fresh end-to-end acceptance | Includes every required stage within 3,600 seconds, with no dataset preparation outside the clock. It remains unmeasured. |

The retained legacy exporter and its phase telemetry still need qualification on
the exact implementation used for the fresh cut; source compatibility alone is
not an export duration or preservation result. Existing vector model labels and
dimensions do not prove current coverage. Any embedding-only provider work needs
its actual profile, grants, cost/window limits, completion and certification
evidence. These commands do not authorize bulk extraction or bulk LLM replay.
Deterministic local provider controls are not evidence of real provider throughput.

HomeCore keeps Elephant stopped from final S1 through switch and proves that
freeze with an unchanged store-file manifest. W3–W6 remain stopped; W1 write
failures stay closed. W2 retains only new documents in `semantic_backlog.jsonl`,
outside the historical inventory, until ordinary ingestion after activation and
certification. No consumer writes may race the historical baseline. A successful
synthetic initial transfer cannot establish these consumer conditions or authorize
a cutover; retain the separately coordinated consumer read, write and preservation
results before claiming the full migration complete.
