# Private original-run evidence exchange

The service can capture qualification evidence into a private local directory.
It is disabled by default and adds no HTTP/MCP route. Set both
`RECALL_PRIVATE_PRIMARY_EVIDENCE` (absolute capability path) and
`RECALL_PRIVATE_PRIMARY_EVIDENCE_SHA256` (SHA-256 of the original capability
bytes) before service startup to enable it. A partial or invalid activation
fails startup. The capability path is operator-owned, absolute and hash-bound;
requests cannot choose paths. The retained qualification bootstrap separately
restricts its exchange to the original `/work` run directory. The transport is supported on Linux; explicit activation on an
unsupported platform fails, while ordinary portable service startup is unchanged.

The capability binds the original run nonce, backend, expiry, canonical corpus
digest, two synthetic spaces and all 28 exact original input hashes. Its exact
fields are `format`, `run_nonce`, `backend`, `corpus_sha256`, `expires_at`,
`directory`, and `source_revisions`. Each selected revision contains `space`,
`source_key`, `revision`, and `input_sha256`. The selection is compiled into the
service; supplying another corpus or extra source does not widen access.

Only `original_publication`, `original_maintenance`, and `current_canonical`
requests exist. The collector captures each original publication before accepting
the next revision, including visit-r1 before visit-r2. The private receipt reader
shares the existing canonical replay source-authority predicate and the exporter
additionally requires the exact desired/published head and original operation.
No old supplied publication fence authorizes a historical receipt payload.

There are 32 single-use logical captures per backend: 28 publications, two
maintenance jobs, and two final canonical inventories. Every capture runs under
storage activity admission, reads its current source heads and canonical epoch,
and compares them after collection. Restore and erasure admission remain shared
with ordinary storage. Replacement storage clients invalidate the worker.
A capture has a 30-second local deadline, bounded by the original capability
expiry at publication. It makes no provider calls and never retries its selected
storage collection. Missing, changed, oversized or timed-out evidence produces
an incomplete reply and ends the private worker; it never advances a success
checkpoint or permits later captures to rely on a failed in-memory marker.
The service drops that failed worker and its capability while continuing the
original HTTP/MCP server future. It does not restart the worker, reuse the
exchange, or turn the incomplete capture into success. Server completion still
cancels a running exporter and retains the server's own result.

Requests and replies use the `recall-private-primary-evidence-*/v1` formats.
The request ID hashes canonical compact JSON before adding `request_id`; the
reply also hashes the complete original request bytes. Requests are published by
the collector after fsync using a persistent exclusive `.ID.json.claim`
directory. The worker ignores those bounded claim entries without traversing
them. It reads only exact `ID.json` files. Descriptor-relative opens reject
symlinks, traversal, nonregular files, multiple hard links, nonowner files and
permissions allowing group/other access. Replies and pages use fsync followed by
Linux `renameat2(NOREPLACE)` and directory fsync. Existing evidence is never
replaced. A nonempty exchange cannot be reused after process restart.

A reply is at most 128 KiB. Row-array pages are at most 128 rows and 256 KiB,
with names `ID.0000.json` onward and exact `name`, `sha256`, `bytes`, `rows`,
`table` references. Pages exist before the final reply is published. The complete
exchange allows 16 MiB per backend, 4096 newly written nonprovider rows, 432 distinct provider
call IDs and at most 64 pages per request. Overflow is incomplete, not truncation.
The reply distinguishes `capture_reply_byte_limit`, `capture_total_byte_limit`,
`capture_row_limit`, and `capture_provider_receipt_limit`. The corresponding
warning records only the fixed error code and numeric candidate reply bytes,
cumulative bytes, cumulative rows and distinct provider receipt count. Previous
captures remain charged. Each logical snapshot independently allows at most
4096 nonprovider rows. Exact unchanged current rows can select an earlier
complete same-space maintenance row without another physical write or charge;
new or changed row bytes remain charged. Request and reply bytes always count.

Publication evidence contains the exact manifest, completion certificate,
operation, original source/input identity, and current-authorized canonical
receipt. The completion certificate must contain fingerprints for both the
selected document and its exact rendered revision; empty or unrelated maps do
not certify those inputs. The receipt projection omits `replay_output` and records its presence,
byte count and fingerprint separately from the fingerprint of the original typed
receipt. Canonical inventories include all physical IDs in the selected tables,
including deletion/supersession markers and unattributed rows, with exhausted
page boundaries, counts, row hashes and table hashes. Causal decision payloads
require their receipt's current source authority; obsolete decisions retain an
explicit payload omission marker. These records support independent review;
`complete` describes evidence collection and never asserts semantic correctness.

For each table, `evidence.inventory[table]` retains `count`, `table_sha256`,
`exhausted`, and `selection_scope`. Its `rows` member is an explicit commitment
to the complete ordered identity/hash list:

```json
{
  "format": "recall-private-primary-row-manifest/v1",
  "source": "original_table_pages",
  "encoding": "canonical_json_utf8",
  "sha256": "<SHA-256 of the complete canonical identity/hash array>"
}
```

Reconstruct the array from original page rows. Legacy descriptors use descriptor
and row order; selected current descriptors use explicit current table positions. Each entry is exactly `{"id": identity, "sha256": row_hash}`. The
identity uses the existing field priority `id`, then `call_id`, then `commit_id`;
identities are unique and lexicographically ordered. Hash each original canonical
row's UTF-8 bytes, preserving number spellings, escaped strings and Unicode.
Hash the canonical identity/hash array to check the `rows.sha256` commitment.
Joining those same original row bytes with `[` / `,` / `]` reconstructs the full
table encoding and must match `table_sha256` and `count`. Empty tables commit to
the empty array and have no row pages. Missing, duplicated, changed or reordered
rows cannot satisfy the original commitments.

All physical rows remain in the existing hash-bound pages, including deleted,
superseded and unattributed rows. This representation removes duplicate inline
identity lists from the bounded reply; it does not remove evidence, create a
new page kind, widen a ceiling or reinterpret older captures whose `rows`
member contains the original inline array. Cross-language consumers must use
the original row-token bytes when checking hashes, rather than reserializing
decoded floating-point numbers with a different JSON encoder.

Maintenance evidence joins the exact completed job to its original hot/cold
work item, all retained attempts, original seed and artifacts, canonical decision
lineage and per-physical-request usage receipts. Missing token values remain
unknown. No recorded adjudicator dispatch is reported as such; public job counts
or zero calls never establish provider conformance. The independent collector
must inspect the original input/decision records to classify deterministic,
no-conflict, memoized, or unexercised behavior.

Source tests cover capability expiry and scope, strict nested JSON, immutable
publication, path/symlink/hardlink custody, complete paging, overflow, unknown
usage, and current source/supersession/erasure/barrier checks on both backends.
The compact-inventory controls reconstruct all 4096 original rows and 432
distinct receipt rows, retain empty tables and marker-bearing rows, detect
changed/missing/duplicate/reordered content, and exercise each unchanged
allocation ceiling. API lifetime controls preserve serving after the one failed
exporter and cancel the original exporter when the server finishes.
These are authored source controls. This document makes no claim that the new
batch has been compiled or executed.


Current snapshots retain the five physical page descriptor fields and add
`selection`: a bounded array of `[source_row_index, current_table_position]`
pairs. Every current descriptor uses this form together. New delta pages select
all their physical rows; retained pages can select an exact subset. Insertions,
physical removals and changed security/support/tombstone fields therefore do not
force unchanged rows across page boundaries into duplicate physical writes.

The exporter always rereads full current tables and enforces the original epoch,
head, source, restore and erasure checks. Its process-local index binds canonical
row SHA256 to space, table, original page and row index. An original maintenance
page becomes reference authority only after all its pages and complete response
are successfully published. Current and failed captures cannot grant authority.

The client accepts a retained page only from an earlier fully validated complete
same-client/run/backend/space maintenance response and with identical original
name/hash/bytes/rows/table. It rereads the whole original page, preserves raw row
JSON tokens and reconstructs explicit current positions. It rejects duplicate
page names, duplicate or invalid source indices/positions, holes, unselected
fresh rows, mixed formats and detached/foreign/changed origins. Existing complete
inventory, row-order and row/table hash checks then apply unchanged. Cache
eligibility follows the successful deadline check as well as validation. All
original page and response files remain unchanged; physical and logical row
limits, byte limits and descriptor counts remain independent and enforced.
