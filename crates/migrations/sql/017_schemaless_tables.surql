-- 017: schemaless parity with the embedded elephant-db engine (issue #28).
--
-- The SCHEMAFULL definitions in 001..016 drifted from the Rust structs:
-- fields the runtime writes but the schema never DEFINEd (e.g.
-- source.lifecycle_status, assertion.significance) were silently stripped
-- by SurrealDB, stalling the ingest pipeline with no errors anywhere.
-- The embedded engine — the reference implementation the test suite runs
-- against — has no schema, so schemaless is the de-facto storage contract.
-- Converting the tables keeps every existing DEFINE FIELD constraint
-- (typed validation still applies where defined) while letting undefined
-- fields through instead of dropping them.

DEFINE TABLE OVERWRITE assertion SCHEMALESS;
DEFINE TABLE OVERWRITE attribute SCHEMALESS;
DEFINE TABLE OVERWRITE audit_log SCHEMALESS;
DEFINE TABLE OVERWRITE conflict_group SCHEMALESS;
DEFINE TABLE OVERWRITE cost_ledger_daily SCHEMALESS;
DEFINE TABLE OVERWRITE doc SCHEMALESS;
DEFINE TABLE OVERWRITE entity SCHEMALESS;
DEFINE TABLE OVERWRITE entity_identity_key SCHEMALESS;
DEFINE TABLE OVERWRITE entity_merge SCHEMALESS;
DEFINE TABLE OVERWRITE event SCHEMALESS;
DEFINE TABLE OVERWRITE freshness SCHEMALESS;
DEFINE TABLE OVERWRITE id_map SCHEMALESS;
DEFINE TABLE OVERWRITE identity_candidate SCHEMALESS;
DEFINE TABLE OVERWRITE predicate_alias SCHEMALESS;
DEFINE TABLE OVERWRITE predicate_registry SCHEMALESS;
DEFINE TABLE OVERWRITE rel_evidence SCHEMALESS;
DEFINE TABLE OVERWRITE reprocess_job SCHEMALESS;
DEFINE TABLE OVERWRITE source SCHEMALESS;
DEFINE TABLE OVERWRITE space_config SCHEMALESS;
DEFINE TABLE OVERWRITE system_event SCHEMALESS;
DEFINE TABLE OVERWRITE truth_slot SCHEMALESS;

-- Nested-key stripping survives a table-level SCHEMALESS conversion: a
-- non-FLEXIBLE `TYPE object` field still drops undeclared nested keys.
-- Migration 003 made provenance.extractor/.chain FLEXIBLE for some tables
-- only; these were missed.
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE assertion TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE assertion TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE attribute TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE attribute TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE truth_slot TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE truth_slot TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE conflict_group TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE conflict_group TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE predicate_registry TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE predicate_registry TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE predicate_alias TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE predicate_alias TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE freshness TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE freshness TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE reprocess_job TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE reprocess_job TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE cost_ledger_daily TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE cost_ledger_daily TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE event TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE event TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.extractor ON TABLE space_config TYPE option<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance.chain ON TABLE space_config TYPE array<object> FLEXIBLE;
DEFINE FIELD OVERWRITE provenance ON TABLE space_config TYPE object FLEXIBLE;

-- audit_log.action's ASSERT predates AuditAction::Restore and ::Purge —
-- audit entries for restores/purges were rejected (and, with the response
-- checking of issue #29 previously absent, silently lost).
DEFINE FIELD OVERWRITE action ON TABLE audit_log TYPE string ASSERT $value INSIDE ['create', 'update', 'delete', 'restore', 'purge', 'promote', 'merge'];
