# Private native membership inspection

`native_private_membership.py` supplies one fixed read-only `QUERY` and the pure
`validate(raw, tokens, canary)` decoder. It covers the original native physical
qualification requirement for the table deliberately omitted from ordinary
snapshots. It does not add a public product endpoint or change backup contents.

The existing boundary owner stops Recall, positively confirms its exit, then
uses the existing Recall image's `curl` on the original owned paired network.
The paired database stays running only for this inspection. Use the existing
synthetic root username/password, selected namespace `offline` and database
`fixture`, and fixed URL `http://paired-store:8000/sql`. `Surreal-NS`/`Surreal-DB`
select the database; omit authentication-namespace/database headers so Basic
authentication retains its existing root authority. No data, journal, auth, or
host mounts are needed in this unprivileged helper container.

The driver owns HTTP framing/status/Content-Type checks, disables proxies and
redirects, and retains the complete raw response in its existing 64 MiB tmpfs
before bounded Docker copy. A 30-second curl cap is clamped to the original
remaining allocation; there is no new clock or retry. The 32 MiB response cap
and 100,000 row cap are within the existing 96 MiB combined evidence allowance.
Hitting a cap refuses complete coverage. After capture the driver stops the
paired database and takes the original read-only volume archives.

The query reads DB schema, full membership table schema, count, every complete
row with native type witnesses, and count again. `object::keys($this)` detects
fields omitted by JSON simplification. Native record/String and string-valued
space/token checks prevent JSON from treating other native types as strings.
The string record key must match the existing UUIDv5 OID stable key built from
UTF-8 byte-length-prefixed space and token. The whole original row is retained;
an extra field, canary, duplicate, wrong identity, or membership absent from the
original nonsecret journal token inventory is a violation.

The exact schema is pinned to migration 039 and the existing native formatter:
SCHEMALESS, TYPE ANY, PERMISSIONS NONE, one unique space/token index, and no
fields, events, live queries, or views. Unknown schema is incomplete evidence.
The fixed DB schema query never reads session-bound private journal keys.
Unexpected DB users, access definitions, or stored parameters are refused and
excluded from the derived report; raw evidence stays in the private allocation.
Diagnostics contain fixed reason codes and never echo response values.
A decoded canary anywhere in the complete response is checked first, so an
unrelated unknown schema or error response cannot turn positive leakage into
an incomplete-evidence result.

The decoder returns complete rows, native witnesses, schema, counts, and raw
hash/length without asserting physical acceptance. The independent boundary
oracle must also compare per-space membership counts with the complete ordinary
export's `source_erasure_guard`, verify original nonsecret journal file custody,
and bind this inspection to the same stopped Recall and paired database before
the physical archive. A valid private-table report alone proves none of those
other conditions. `Incomplete` means coverage is absent or unreviewed;
`Violation` means actual observed state contradicts the private-row contract.

Nine synthetic wire controls cover valid/empty/UTF-8 inventories, hidden fields,
native type collapse, identity/token substitution, duplicates, canaries, counts,
errors, schema changes, malformed JSON and bounds. They do not execute SQL or
qualify the native wire; the fixed query still requires central native validation.
