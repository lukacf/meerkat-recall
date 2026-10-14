# Pinned schema source attribution

`scripts/qualification/semantic_schema_map.py` reads an original unreviewed
pristine profile and source blobs at its exact Git commit. It verifies the
recorded source tree, engine tree, twelve compiled-source hashes, ordered
migration hashes, and all original INFO/export identities and statement hashes.
It preserves every original definition and associates its migration history,
source line and statement hash. Array subtype fields, relation endpoint fields,
implicit tables and the actual standard restore event template have separate
attribution categories. Unknown identities remain explicitly unrecognized.

This is a source review aid. Name attribution does not prove body equivalence.
The intentionally narrow lexical hint strips comments, whitespace and the
stored-definition creation modifiers; it keeps quoted content intact but folds
other token case. Every formatter or engine transformation it cannot compare
remains `NEEDS_FORMATTER_OR_TRANSFORMATION_REVIEW`. It does not execute SQL,
model an entire migration engine, install an allowed schema, or emit acceptance.
D owns the independent review and allowed-profile freeze.

The output is exclusively created and always marks `product_acceptance=false`
and `allowed_profile=false`. Original capture and source files are never edited.
The offline controls exercise only lexical boundaries and refusal behavior.

The additional body review hint compares every field clause, table property and
index property with narrowly enumerated pinned parser defaults. It preserves
identifier case and literal values, permits field-clause formatter order, and
shows expected and observed field properties. Engine-generated array fields
use the parent type and the first subtype creation's flexible flag; relation
endpoints use the declared relation table type. This remains a review aid, not
an allowed-schema predicate. Function and event comparison is limited to pinned
operator aliases, string quote representation, the formatter's single `fn::`
value block, parentheses around SELECT on a LET right-hand side, and the parser's
default single closure parameter type `any`. Additional statements, changed
arguments or explicit other parameter types remain different. Every transform
still requires D's independent review; E separately reviews reference guard and
physical fence bodies against the original pinned source.

The Runtime16 source review also accounts for two historical engine effects:
`doc_chunk` was explicitly removed in migration 007, then a later index/event
definition implicitly recreated its default schemaless table. The initial
untyped `rel.evidence` and `attribute.evidence` arrays generated no subtype;
their first typed definitions in migration 003 therefore created flexible
object subfields. Other existing generated subfields retain their initial
flexible flags when their parent definition changes. These are observations
about the pinned source, not a request to alter or remove any runtime surface.
