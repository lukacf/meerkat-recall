# Corrected search examples, coordinate successor

[The successor packet](examples-coordinate-v2.json) preserves the full original
examples and corrects the search citation for `The trial began in June.`: this
quote is **24 UTF-8 bytes**, so its raw interval is `[0,24)`. The original
`examples.json` interval `[0,23)` is invalid. The original file and the independently
delivered fourteen-case `ob3-consumer-fixtures.json` packet remain unchanged;
the successor records the exact original examples hash in `supersedes`.

The successor adds all three required rendered citation fields using complete,
explicit synthetic input/output literals. The pinned text renderer
`elephant.renderer.text` version `1.0.0`, with no configuration hash, renders the
24-byte input to the same sentence followed by one LF, a **25-byte full text**.
The selected support excludes that generated LF: `rendered_span` is `[0,24)`.
`rendered_quote_sha256` hashes the 24-byte support, while `rendered_text_sha256`
hashes the complete 25-byte rendered text. The packet includes both full texts
and literal hashes for independent review; it does not supply a future runtime
receipt or imply that this illustrative score and record identity were observed.

A static recursive audit of every literal `span` plus `quote` pair in this
contract directory found only the original 23/24 mismatch. Both source and
rendered slices in the successor match their explicit complete text. Source
review and installed HTTP/MCP execution remain distinct; independent review of
this corrected successor is required before it replaces the delivered example.

OB3's current reader validates coverage and trusts Recall's current server proof.
It does not call the immutable revision GET in its v1 integration. Its local
hash/span helpers therefore do not establish actual consumer verification of a
full returned revision. Equal quote bytes cannot establish the correct repeated
occurrence; Recall's original transform mapping remains a required service
invariant. The independent seven-case coordinate fixture and actual HTTP/MCP
regressions cover this stronger mapping requirement separately.

Future external outage acceptance uses the independently reviewed
`ob3-recall-outage-2` producer, original commit
`d2e45817117d67d23bcdc8e33a021b6c0c56f184`. Its schema is 1,546 bytes with SHA-256
`101dac0cd5ce984df7987cf85708a6c5bb63004bc6a856dc8a463b97ef2c4121`;
`receipt.rs` is 26,101 bytes with SHA-256
`9bd02e63ba5a7487da688af16d6690ce066da2d730ff50e34f56821b642a2320`.
The version adds checked cross-worker totals, exact positive-read participant
matching and duplicate-participant refusal. Original v1 artifacts remain
historical, unqualified evidence; no actual external outage run is claimed here.
