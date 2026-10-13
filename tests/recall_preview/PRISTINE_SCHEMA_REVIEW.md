# Independent pristine schema source review

`scripts/qualification/pristine_schema_review.py` checks the original Runtime16
proposal against the exact pinned source. It imports no product or S attribution
code. Its separate scanner retains identifier case, string values, numeric
bounds, field clauses, permissions and complete event/function tokens. It also
checks all source-file and fragment hashes, exact original INFO/export bodies,
and complete stage inventories. The transformation rules were reviewed against
the pinned SurrealDB field/table/index parser, persistence and display source.

The reviewed inputs are the original `a7e46c17...` capture, S's exact v5 source
attribution, and E's independently retained original reference-event and 043
receipts. E's separate review establishes the exact Rust refresh-template
origins; this script does not substitute a candidate-created template for it.

The six test methods include changed numeric thresholds, literal case,
identifier case, table/field permissions, extra event statements, changed
reference guards, explicit closure types and array subtype flexibility. They
are test source until an authorized run records their result.

This static review neither runs a database nor installs an allowed profile.
Any subsequent physical-export oracle needs complete exact fresh/restored
profiles and a narrow grammar for original exporter section headers. It must
refuse unknown schema, partial profiles and mixtures, and preserve every
physical erasure and independent-support expectation. Source/body agreement
does not establish live erasure, restore replay, media/WAL deletion or backup
retention.


D completed the separate static body comparison at source `6c4d1e9dd3cc7e4a23bbffe96f41e12682df75e9`:
all 73 pinned source files, all 2,374 original INFO/export bodies, and all
source fragments matched. The retained static receipt is
`qualification-pristine-source-review-runtime16.json`, SHA256
`ff60d131b293ac01767544275157fcdec2928c6158c7d7c9bad23b9f6a0d6895`.
D separately cross-checked every one of E's 120 original reference-function/event
body, source-file and source-fragment identities against S's final v5 map.

`native-schema-profile-runtime16.json` freezes 1,193 distinct definition bodies
as two complete profiles. The oracle's constants are SHA256 over each sorted
complete list of definition-body hashes: 1,186 fresh and 1,188 refreshed. The
catalog is test/source-review material; the pure runtime oracle reads no files
and has no caller-supplied profile or success flag. An empty native schema,
missing/duplicate definition, mixture, unknown body or changed literal refuses.
The embedded backend requires no native schema statements.

The permitted leading export headers are only the original three-line exporter
sections for FUNCTIONS, ANALYZERS, and the 72 exact reviewed table names. At
most a previous table's DATA header and the current table's structure header
may precede a definition. The current header must match its definition; arbitrary
comments and unknown tables refuse. Body bytes, case and literals remain exact;
the one terminal semicolon and surrounding ASCII whitespace are exporter syntax.
The erased canary is checked against the entire raw statement before any header
removal. This accounts for header attachment changing when the previous table
contains rows, without ignoring arbitrary schema/comment content.

Post-erasure schema must equal the original complete profile. Restored schema
must equal the complete standard-refresh profile. The only fresh-to-restored
changes are two added reference events on `outbox_checkpoint` and `space_grant`,
and five standard-template replacements on `source_bundle_registration`,
`source_document_mapping`, `source_contact_intent`, `watch_delegation`, and
`work_producer_origin`. The original false reference scan on `source_head` and
original reservation exception on `system_event` remain unchanged. This replaces
the earlier impossible raw-schema-equality requirement with the exact independently
reviewed restoration behavior, without accepting a union of schema statements.

The original 19 export-oracle controls now use the frozen complete schema for
the synthetic native fixtures. Seven additional methods cover omission,
duplication, mixed profiles, mandatory refresh, post-erasure schema stability,
valid populated-export headers, unknown/comment payloads and consistent malicious
permission changes. These 26 controls and the six source-review methods all passed at exact source
`3ac8350765f3ffba4fa94d9935eb6c15fa09ae3d`: 32 tests, exit 0, unittest 0.262 s
(process 0.328 s), under root's explicit one-run authorization. The sealed receipt
SHA256 is `14466dc17f26ae9dbd22031e76da583590c540ed2166e057e684136c9aae00fa`. No physical
storage or product acceptance follows from this result.
