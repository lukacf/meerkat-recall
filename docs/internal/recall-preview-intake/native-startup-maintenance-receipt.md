# Native startup after explicit migration

The retained `migrate` command applies the schema, then runs the fake-embedding
repair when a real embedding provider is selected. The repair records its
completion in `migration_history`. Previously, subsequent `serve` rejected that
same marker as an unknown schema receipt, so a freshly migrated native database
could not start. The installed basic and dispatch observations both captured
this schema-verification error after the migrate command had succeeded.

The repair writer now exports its existing receipt name and checksum as the
single constants used by startup verification and native historical control
retention. The persisted name, record key and checksum are unchanged. Schema
verification accepts only this exact optional pair alongside the required DDL
receipts. A marker alone cannot initialize an existing database, satisfy a
missing DDL receipt or permit noncontiguous history. Unknown names and changed
repair or schema checksums still refuse both verification and migration.

The regression exercises the real schema initializer, repair writer, read-only
startup verifier and repeated migration in sequence. Additional controls reject
a damaged repair receipt without rewriting it, a marker-only database, unknown
receipt names, missing migrations and altered DDL checksums. These tests are
authored for central execution; this source checkpoint does not claim they ran.

The same startup sequence is used by the retained native FastEmbed observation.
This fix addresses its schema admission prerequisite; it does not establish an
inference, installed-case, source-erasure or broader qualification result.
