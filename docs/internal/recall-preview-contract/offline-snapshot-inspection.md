# Offline snapshot inspection contract

`elephant_storage::snapshot_inspection::inspect_snapshot` validates one existing
named snapshot bundle without creating a client, importing SQL, accessing the
network, or opening the external erasure journal. The ordinary snapshot manifest
validator checks version, backend, opaque snapshot/journal identities and finite
unexpired retention. Payload length and SHA-256 must match before parsing.

Input is an operator-owned directory. The directory and input leaf files must
not be symlinks; files must be regular and retain the observed size and Unix
inode/device identity. Unix opens use `O_NOFOLLOW | O_NONBLOCK` before descriptor
validation so a replaced FIFO cannot block inspection. Ancestor directory
components remain the operator's trusted filesystem boundary.

The fixed maximum limits are 64 KiB manifest, 64 MiB payload, 200,000 rows,
100,000 statements, 256 tables, 128 levels, 2,000,000 value nodes/tokens,
14,065,536 bytes per encoded string token/decoded string/byte value,
100,000 typed reference occurrences and 32 MiB serialized report. Callers may lower these limits, never raise them. Lexical
string/depth/token checks run before native parsing; the pinned statement parser
also enforces explicit recursion limits. Embedded row/table/node limits are
charged during duplicate-aware JSON decoding. Native statements are decoded one
at a time; a statement still consumes bounded parser memory before its actual
row count is checked. Output size is charged per row/schema statement and again
for the complete serialized report. No partial result is returned.

The string limit is derived from the actual 2,000,000-byte source text maximum.
`source_publication::encode_payload` serializes the immutable revision control
as JSON once; embedded JSON export or the pinned native `QuoteStr` then quotes
that payload. A raw ASCII control byte can become six JSON bytes (`\u0001`)
and seven exported bytes (`\\u0001`). Seven times the source maximum plus
64 KiB for the ordinary control envelope is 14,065,536 bytes. Native direct
backspace spelling needs five bytes per input byte; native unpadded base64
needs `ceil(4*n/3)`, both below this envelope. Quotes are excluded from the token
cap; escape/base64 bytes count. Larger arbitrary metadata or repeated content
can still exceed this finite per-record envelope. The unchanged aggregate
payload and report bounds can also refuse a large complete database: repeated
worst-case control copies must return `incomplete`, never omit physical rows.
The ordinary maximum-size source and its actual immutable control export are
covered by authored acceptance/export tests; independent offline fixtures cover
the worst escaped control representation, exact bounds and native bytes.

The versioned report contains manifest identity, table counts, row counts,
lossless physical identities, SHA-256 row fingerprints and all native typed
record reference occurrences. A string resembling a record reference remains a
string. Integer, string, UUID and composite native record identities remain
distinct. Decimal scale, float bits, bytes, durations, timestamps and native
`NONE` retain tagged representations. Embedded payload values preserve their
actual JSON storage representation; its physical identity is decoded separately
using the same storage identity helper used by erasure inventory.

Full tagged records are available only with `include_records = true` (CLI
`--json --include-records`). Native schema definitions, which can contain
payload in events/defaults, are always counted/hashed; their original SQL bytes
are retained only with the same explicit option. Schema SQL is opaque original
text, never evaluated or rewritten through an AST formatter. The inspector
accepts the pinned ordinary export's `OPTION IMPORT`, `DEFINE` and literal
`INSERT` / `INSERT RELATION` shapes. Record object duplicate keys and duplicate
physical identities are refused. Unsupported statements, value expressions,
versioned inserts, formats, limits and malformed inputs produce only
`snapshot_inspection_incomplete:*` errors without raw payload diagnostics.
The export spelling `encoding::base64::decode` is accepted only with one literal
string argument and decoded by a local bounded decoder, never executed.

A valid report is evidence about **this one bundle**, not an erasure completion
certificate. Private full reports must be kept with restrictive permissions.
Live persistent volumes, media/cold files, all registered older snapshots and
expiry state, and the independently retained original journal require separate
complete inspection. Hash-only/default reports cannot prove removed content.
The authored storage tests include actual named exports from both backends,
lossless native values/references, duplicates, unsupported statements, file
integrity, expiry, format and all resource bounds. Central integration owns
compilation and execution; this source checkpoint alone is not qualification.
