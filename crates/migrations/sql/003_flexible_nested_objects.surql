-- Elephant Core Schema Updates
-- Migration: 003_flexible_nested_objects
--
-- The initial schema defined several `object` / `array<object>` fields without
-- marking them `FLEXIBLE`. In SurrealDB SCHEMAFULL mode, non-flexible objects
-- will drop/strip nested keys that are not explicitly defined, which breaks
-- provenance, audit snapshots, outbox payloads, and identity candidate signals.
--
-- This migration updates those fields to `FLEXIBLE` so nested keys are preserved.

-- Provenance extractor + chain must preserve nested keys across all tables.
REMOVE FIELD provenance.extractor ON TABLE entity;
REMOVE FIELD provenance.chain ON TABLE entity;
DEFINE FIELD provenance.extractor ON TABLE entity TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE entity TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE rel;
REMOVE FIELD provenance.chain ON TABLE rel;
DEFINE FIELD provenance.extractor ON TABLE rel TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE rel TYPE array<object> FLEXIBLE;

-- Rel/Attribute evidence arrays store structured EvidenceRef objects.
-- If not typed as array<object> FLEXIBLE, SurrealDB will not persist nested keys.
REMOVE FIELD evidence ON TABLE rel;
DEFINE FIELD evidence ON TABLE rel TYPE array<object> FLEXIBLE DEFAULT [];

REMOVE FIELD evidence ON TABLE attribute;
DEFINE FIELD evidence ON TABLE attribute TYPE array<object> FLEXIBLE DEFAULT [];


REMOVE FIELD provenance.extractor ON TABLE doc;
REMOVE FIELD provenance.chain ON TABLE doc;
DEFINE FIELD provenance.extractor ON TABLE doc TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE doc TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE doc_chunk;
REMOVE FIELD provenance.chain ON TABLE doc_chunk;
DEFINE FIELD provenance.extractor ON TABLE doc_chunk TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE doc_chunk TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE chunk_embedding;
REMOVE FIELD provenance.chain ON TABLE chunk_embedding;
DEFINE FIELD provenance.extractor ON TABLE chunk_embedding TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE chunk_embedding TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE entity_identity_key;
REMOVE FIELD provenance.chain ON TABLE entity_identity_key;
DEFINE FIELD provenance.extractor ON TABLE entity_identity_key TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE entity_identity_key TYPE array<object> FLEXIBLE;

-- Fact table field redefinitions removed (fact table deprecated).

REMOVE FIELD provenance.extractor ON TABLE system_event;
REMOVE FIELD provenance.chain ON TABLE system_event;
DEFINE FIELD provenance.extractor ON TABLE system_event TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE system_event TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE source;
REMOVE FIELD provenance.chain ON TABLE source;
DEFINE FIELD provenance.extractor ON TABLE source TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE source TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE audit_log;
REMOVE FIELD provenance.chain ON TABLE audit_log;
DEFINE FIELD provenance.extractor ON TABLE audit_log TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE audit_log TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE id_map;
REMOVE FIELD provenance.chain ON TABLE id_map;
DEFINE FIELD provenance.extractor ON TABLE id_map TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE id_map TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE identity_candidate;
REMOVE FIELD provenance.chain ON TABLE identity_candidate;
DEFINE FIELD provenance.extractor ON TABLE identity_candidate TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE identity_candidate TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE rel_evidence;
REMOVE FIELD provenance.chain ON TABLE rel_evidence;
DEFINE FIELD provenance.extractor ON TABLE rel_evidence TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE rel_evidence TYPE array<object> FLEXIBLE;

REMOVE FIELD provenance.extractor ON TABLE entity_merge;
REMOVE FIELD provenance.chain ON TABLE entity_merge;
DEFINE FIELD provenance.extractor ON TABLE entity_merge TYPE option<object> FLEXIBLE;
DEFINE FIELD provenance.chain ON TABLE entity_merge TYPE array<object> FLEXIBLE;

-- Entity handles are stored as an array of structured objects.
REMOVE FIELD handles ON TABLE entity;
DEFINE FIELD handles ON TABLE entity TYPE array<object> FLEXIBLE;

-- Source range is a structured object (start_ms/end_ms/index).
REMOVE FIELD range ON TABLE source;
DEFINE FIELD range ON TABLE source TYPE option<object> FLEXIBLE;

-- Outbox payload is a reference-oriented object.
REMOVE FIELD payload ON TABLE system_event;
DEFINE FIELD payload ON TABLE system_event TYPE object FLEXIBLE;

-- Identity candidate signals are a flexible object.
REMOVE FIELD signals ON TABLE identity_candidate;
DEFINE FIELD signals ON TABLE identity_candidate TYPE object FLEXIBLE;

-- Audit snapshots must preserve full record JSON for reverts.
REMOVE FIELD before ON TABLE audit_log;
REMOVE FIELD after ON TABLE audit_log;
DEFINE FIELD before ON TABLE audit_log TYPE option<object> FLEXIBLE;
DEFINE FIELD after ON TABLE audit_log TYPE option<object> FLEXIBLE;
