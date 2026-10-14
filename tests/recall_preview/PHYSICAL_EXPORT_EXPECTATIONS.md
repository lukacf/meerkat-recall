# Independent installed physical-erasure expectations

This freezes the interpretation of F's existing Metadata A/B fixture for E's
offline typed snapshot inspector. It supplements the container boundary contract;
it authorizes no execution. A and B use distinct named spaces and actual returned
source identities. Their shared external key does not make them the same source.
Same-space independent Graph support has separate D/F oracles.

Before erasure, capture an actual registered export and its manifest, the full
typed inspection with `--include-records`, actual public A/B source/document
identities, exact submitted text and unique source-A canaries. Require the real A
source and immutable accepted input to occur, and actual B source/input to occur.
An empty inventory or Metadata-only empty search cannot satisfy this precondition.
Keep the complete original B-space physical rows as the independent baseline.

After acknowledged erase and after restoring the original older backup with its
original journal, require all of the following:

- The inspector reports a complete verified bundle with exact manifest/backend,
  byte length/hash, journal identity, row/statement/table totals, all typed rows
  and full schema statements. Missing raw records, truncation, unsupported native
  values, duplicate identities/keys or parser refusal is **UNMET**, never absence.
- The physical A Source and SourceRevision records are absent, including
  tombstones. So are the actual A Doc/revision/node/media/vector rows and all
  source-exclusive hot/cold work, artifacts, decisions, support, contact plans,
  canonical before-images, audit/outbox bodies and view histories identified by
  the original captured lineage. A public 404 is insufficient.
- No data row, extension payload, nested encoded JSON or schema literal retains
  the unique original A text/canaries. Independently traverse exact native record
  identities, typed links and encoded JSON keys/values; preserve integer versus
  string identities, Unicode keys, UUID and table distinctions. Do not use a UUID
  substring search as the ownership rule. Ordinary prose or a prefix-neighbor
  belonging to B is not an A reference.
- All B-space data and control rows remain exactly equal to the original typed
  baseline, including timestamps, policy, fields, vectors and histories. Compare
  complete records by typed physical identity. Do not strip `updated_at` to make
  an unexpected B write pass. Any documented global snapshot envelope is outside
  the per-row baseline and must still match its own contract.

Opaque lifecycle controls are allowed only through an explicit, strict projection
of their purpose. A table name is not a blanket exemption. Decode stored `payload`
JSON and reject extra fields or source body/title/metadata/external-key/template,
artifact text, before-images or credential values even in a retained control.

| Permitted control purpose | Allowed A-bearing content and required checks |
| --- | --- |
| `source_head` | Exact public SourceHead fields only: space/source identity, opaque generation and operation, declared revision labels, lifecycle and timestamp. A must be Erased and `published_revision` absent. Revision labels must equal the fixture's original declared tokens, not arbitrary strings. |
| `source_authorization` | Exact SourceAuthorizationEnvelope: space/source identity, original policy/security and subject ID/completeness fields. Match the original authorization envelope. No original Source or authorization snapshot carrying data fields. |
| `source_operation` | Exact OperationReceipt plus known request/input digests, the exact minimal authorization envelope, optional contact digest, typed producer identity and predecessor publication identity. IDs/revision labels must come from actual original observations; digests must have the reviewed bounded digest representation. Only terminal receipts may remain after completed erase. No request body or historical contact plan. |
| `source_identity` | Physical source-incarnation pointer only. It must not grant an active incarnation or recreate A. No external identity in plaintext payload; the known external key remains prohibited. |
| `source_tombstone_ref` | Space/source identity, original erasure intent ID and opaque erased-reference token only. Bind each to the actual original journal/erasure observations. It is not source content and need not disappear merely because the source ID appears. |
| Journal anchor/reference guard | Exact known journal ID, key fingerprint, version, membership count, revision and optional reservation identity. No original payload or membership key material. Completed erasure must have no active reservation. Validate retained counts/identity against actual journal custody; export omission of private HMAC memberships is not proof they were inspected. |
| Erasure file work | A completed erase must have finished its file work. No outstanding A `source_erasure_files` rows or extant original owned asset bytes may be accepted as complete. |
| Payload-free `source.erased` notification | Exact typed event envelope and known erased-ID summary only. No old source, document, contact, artifact, evidence or before/after payload. An arbitrary event or arbitrary `data` map is not exempt. |

Source-free space grants, epochs, counters and checkpoints are not A payload just
because they share a space. Their identity/shape still must be valid; a control
containing an A data reference outside the explicit projections above requires
independent review and remains UNMET. F must implement this policy from the
original public identities and captured typed rows, not call E's erasure selector
or copy its retained-table list as an oracle.

The registered post-erasure export proves only that exported physical state.
Before the installed physical gate can pass, separately inventory the owned
persistent volume, current and previous live snapshots, WAL, managed media,
registered backup files and manifests. Older backups may exist only within the
original finite retention deadline; observe actual expiry and physical removal of
both manifest and data. Preserve evidence of original journal custody throughout.
Offline parsing must open no DB, import no rows, read no journal key and mount only
the owned backup read-only. Full records are sensitive private captures, bounded
by the inspector's 32 MiB complete output limit.

OB3's gate remains physical source deletion plus bounded backup retention.
Continuous restore/readiness observation and original-journal replay are
additional Recall requirements, not substitutions for physical deletion or a
new replay condition imposed on OB3.
