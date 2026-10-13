# Lossless private evidence row fragments

A private qualification snapshot may contain an immutable JSON row larger than
a single 256 KiB page. The exporter preserves its exact canonical JSON UTF-8
bytes in a contiguous group of fresh physical pages. Each page body is exactly
`{"encoding":"base64","data":"..."}`. Its ordinary descriptor gains
`row_fragment` with `row_sha256`, `row_bytes`, `index`, `count`, and `offset`.
Indices start at zero, offsets cover every original byte without overlap or a
gap, and the group shares one table, digest, byte count and fragment count.

Every descriptor has `rows: 1`, describing the single grouped logical row.
Consumers count that row once after reconstructing and verifying its whole
original byte sequence. The original numeric spelling and Unicode bytes feed
the existing row and ordered-table fingerprints; parsing and reserialization
cannot substitute for this proof. Missing, repeated, reordered, interleaved,
inconsistent or tampered fragments must refuse the snapshot.

Current snapshots repeat the same `selection: [[0, current_table_position]]`
on every group descriptor and apply it once after reconstruction. Every group
uses the current request's page names; fragments cannot refer to a previous
capture. Ordinary exact maintenance-row reuse remains unchanged. A fragmented
row does not populate that reuse cache and is fully emitted and charged again
in a later snapshot.

The exporter slices at 128 KiB of raw row bytes. Encoded physical pages still
must fit 256 KiB. The existing 64 physical pages, 16 MiB total capture, 128 KiB
reply, 4,096 non-provider row and 432 provider receipt limits remain unchanged.
An oversized group or exhausted allocation still fails explicitly. There is no
truncation, payload omission, corpus reduction or automatic retry.

Producer controls cover byte-identical reconstruction across a split Unicode
scalar and a negative-zero value, ordinary/fragment page ordering, exact page
boundaries, current positions and ordinary-row reuse, and unchanged page/byte/row
limits. Consumer controls independently enforce fragment rejection and complete
inventory/custody joins. These are qualification transport controls, not provider
or semantic acceptance. Original incomplete captures remain failed evidence.

The first producer regression optionally writes one fresh private fixture when
`RECALL_PRIVATE_FRAGMENT_FIXTURE_DIR` names an unused absolute directory. It
retains the actual `prepare_packet` request, response and physical page bytes,
the original ordered table bytes, and a hash/length manifest. The selected
consumer regression reads those files through the production reconstruction
path and compares the complete original byte sequence. This is one roundtrip
within the affected regressions, not a provider capture or a general export API.
No fixture is written unless explicitly selected; existing paths are refused.
