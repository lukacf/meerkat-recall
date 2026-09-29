-- Elephant Core Schema Initialization
-- Migration: 001_init
-- This creates all core tables with their schemaful field definitions.

-- =============================================================================
-- Analyzers for Full-Text Search
-- =============================================================================

DEFINE ANALYZER simple TOKENIZERS blank, class FILTERS lowercase, ascii;

-- =============================================================================
-- Migration History Table (for idempotency tracking)
-- =============================================================================

DEFINE TABLE migration_history SCHEMAFULL;
DEFINE FIELD name ON TABLE migration_history TYPE string;
DEFINE FIELD applied_at ON TABLE migration_history TYPE datetime;
DEFINE FIELD checksum ON TABLE migration_history TYPE string;
DEFINE INDEX migration_name ON TABLE migration_history FIELDS name UNIQUE;

-- =============================================================================
-- Core Tables
-- =============================================================================

DEFINE TABLE entity SCHEMAFULL;
DEFINE TABLE rel TYPE RELATION IN entity OUT entity ENFORCED;
DEFINE TABLE doc SCHEMAFULL;
DEFINE TABLE doc_chunk SCHEMAFULL;
DEFINE TABLE chunk_embedding SCHEMALESS;

DEFINE TABLE entity_identity_key SCHEMAFULL;
-- DEFINE TABLE fact SCHEMAFULL; -- DEPRECATED: Fact type removed in favor of Rel + Attribute
DEFINE TABLE system_event SCHEMAFULL;
DEFINE TABLE source SCHEMAFULL;
DEFINE TABLE audit_log SCHEMAFULL;
DEFINE TABLE id_map SCHEMAFULL;
DEFINE TABLE identity_candidate SCHEMAFULL;
DEFINE TABLE rel_evidence SCHEMAFULL;
DEFINE TABLE entity_merge SCHEMAFULL;
DEFINE TABLE freshness SCHEMAFULL;
DEFINE TABLE reprocess_job SCHEMAFULL;

-- =============================================================================
-- Entity Table Fields
-- =============================================================================

-- Base fields (common to all core tables)
DEFINE FIELD space_id ON TABLE entity TYPE string;
DEFINE FIELD created_at ON TABLE entity TYPE datetime;
DEFINE FIELD updated_at ON TABLE entity TYPE datetime;
DEFINE FIELD created_by ON TABLE entity TYPE string;
DEFINE FIELD deleted_at ON TABLE entity TYPE option<datetime>;

-- Security fields
DEFINE FIELD security ON TABLE entity TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE entity TYPE string;
DEFINE FIELD security.level ON TABLE entity TYPE number;
DEFINE FIELD security.labels ON TABLE entity TYPE array<string>;
DEFINE FIELD security.handling ON TABLE entity TYPE array<string>;

-- Provenance fields
DEFINE FIELD provenance ON TABLE entity TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE entity TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE entity TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE entity TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE entity TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE entity TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE entity TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE entity TYPE array<object>;

-- Subject fields
DEFINE FIELD subjects ON TABLE entity TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE entity TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Entity-specific fields
DEFINE FIELD type ON TABLE entity TYPE string ASSERT $value INSIDE ['person', 'app', 'group', 'topic', 'platform', 'org', 'location', 'device'];
DEFINE FIELD display_name ON TABLE entity TYPE string;
DEFINE FIELD aliases ON TABLE entity TYPE array<string>;
DEFINE FIELD summary ON TABLE entity TYPE option<string>;
DEFINE FIELD handles ON TABLE entity TYPE array<object>;
DEFINE FIELD identity_keys ON TABLE entity TYPE array<string>;
DEFINE FIELD risk_level ON TABLE entity TYPE option<number>;
DEFINE FIELD trust_level ON TABLE entity TYPE option<number>;
DEFINE FIELD known_safe ON TABLE entity TYPE bool DEFAULT false;
DEFINE FIELD first_seen ON TABLE entity TYPE option<datetime>;
DEFINE FIELD last_seen ON TABLE entity TYPE option<datetime>;
DEFINE FIELD status ON TABLE entity TYPE string ASSERT $value INSIDE ['active', 'merged'] DEFAULT 'active';
DEFINE FIELD redirect_to ON TABLE entity TYPE option<record>;
DEFINE FIELD metadata ON TABLE entity TYPE object FLEXIBLE;

-- =============================================================================
-- Rel (Relationship) Table Fields
-- =============================================================================

DEFINE FIELD space_id ON TABLE rel TYPE string;
DEFINE FIELD created_at ON TABLE rel TYPE datetime;
DEFINE FIELD updated_at ON TABLE rel TYPE datetime;
DEFINE FIELD created_by ON TABLE rel TYPE string;
DEFINE FIELD deleted_at ON TABLE rel TYPE option<datetime>;
DEFINE FIELD security ON TABLE rel TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE rel TYPE string;
DEFINE FIELD security.level ON TABLE rel TYPE number;
DEFINE FIELD security.labels ON TABLE rel TYPE array<string>;
DEFINE FIELD security.handling ON TABLE rel TYPE array<string>;
DEFINE FIELD provenance ON TABLE rel TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE rel TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE rel TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE rel TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE rel TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE rel TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE rel TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE rel TYPE array<object>;
DEFINE FIELD subjects ON TABLE rel TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE rel TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

DEFINE FIELD rel_type ON TABLE rel TYPE string;
DEFINE FIELD status ON TABLE rel TYPE string DEFAULT 'candidate';
DEFINE FIELD confidence ON TABLE rel TYPE number DEFAULT 0.0;
DEFINE FIELD validity_window ON TABLE rel TYPE option<object> FLEXIBLE;
DEFINE FIELD evidence ON TABLE rel TYPE array DEFAULT [];
DEFINE FIELD temporal_audit ON TABLE rel TYPE option<object> FLEXIBLE;

-- =============================================================================
-- Event Table Fields
-- =============================================================================


-- =============================================================================
-- Doc Table Fields
-- =============================================================================

DEFINE FIELD space_id ON TABLE doc TYPE string;
DEFINE FIELD created_at ON TABLE doc TYPE datetime;
DEFINE FIELD updated_at ON TABLE doc TYPE datetime;
DEFINE FIELD created_by ON TABLE doc TYPE string;
DEFINE FIELD deleted_at ON TABLE doc TYPE option<datetime>;
DEFINE FIELD security ON TABLE doc TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE doc TYPE string;
DEFINE FIELD security.level ON TABLE doc TYPE number;
DEFINE FIELD security.labels ON TABLE doc TYPE array<string>;
DEFINE FIELD security.handling ON TABLE doc TYPE array<string>;
DEFINE FIELD provenance ON TABLE doc TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE doc TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE doc TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE doc TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE doc TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE doc TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE doc TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE doc TYPE array<object>;
DEFINE FIELD subjects ON TABLE doc TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE doc TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

DEFINE FIELD title ON TABLE doc TYPE string;
DEFINE FIELD text ON TABLE doc TYPE string;
DEFINE FIELD source ON TABLE doc TYPE option<record>;
DEFINE FIELD rendition_key ON TABLE doc TYPE string;
DEFINE FIELD metadata ON TABLE doc TYPE object FLEXIBLE;

-- =============================================================================
-- Doc Chunk Table Fields
-- =============================================================================

DEFINE FIELD space_id ON TABLE doc_chunk TYPE string;
DEFINE FIELD created_at ON TABLE doc_chunk TYPE datetime;
DEFINE FIELD updated_at ON TABLE doc_chunk TYPE datetime;
DEFINE FIELD created_by ON TABLE doc_chunk TYPE string;
DEFINE FIELD deleted_at ON TABLE doc_chunk TYPE option<datetime>;
DEFINE FIELD security ON TABLE doc_chunk TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE doc_chunk TYPE string;
DEFINE FIELD security.level ON TABLE doc_chunk TYPE number;
DEFINE FIELD security.labels ON TABLE doc_chunk TYPE array<string>;
DEFINE FIELD security.handling ON TABLE doc_chunk TYPE array<string>;
DEFINE FIELD provenance ON TABLE doc_chunk TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE doc_chunk TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE doc_chunk TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE doc_chunk TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE doc_chunk TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE doc_chunk TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE doc_chunk TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE doc_chunk TYPE array<object>;
DEFINE FIELD subjects ON TABLE doc_chunk TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE doc_chunk TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

DEFINE FIELD doc ON TABLE doc_chunk TYPE record;
DEFINE FIELD position ON TABLE doc_chunk TYPE number;
DEFINE FIELD text ON TABLE doc_chunk TYPE string;
DEFINE FIELD embedding ON TABLE doc_chunk TYPE option<array<float>> ASSERT $value IS NONE OR array::len($value) = 3072;
DEFINE FIELD embedding_model ON TABLE doc_chunk TYPE option<string>;
DEFINE FIELD collection ON TABLE doc_chunk TYPE string;
DEFINE FIELD chunk_key ON TABLE doc_chunk TYPE string;
DEFINE FIELD metadata ON TABLE doc_chunk TYPE object FLEXIBLE;

-- =============================================================================
-- Entity Identity Key Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE entity_identity_key TYPE string;
DEFINE FIELD created_at ON TABLE entity_identity_key TYPE datetime;
DEFINE FIELD updated_at ON TABLE entity_identity_key TYPE datetime;
DEFINE FIELD created_by ON TABLE entity_identity_key TYPE string;
DEFINE FIELD deleted_at ON TABLE entity_identity_key TYPE option<datetime>;
DEFINE FIELD security ON TABLE entity_identity_key TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE entity_identity_key TYPE string;
DEFINE FIELD security.level ON TABLE entity_identity_key TYPE number;
DEFINE FIELD security.labels ON TABLE entity_identity_key TYPE array<string>;
DEFINE FIELD security.handling ON TABLE entity_identity_key TYPE array<string>;
DEFINE FIELD provenance ON TABLE entity_identity_key TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE entity_identity_key TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE entity_identity_key TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE entity_identity_key TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE entity_identity_key TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE entity_identity_key TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE entity_identity_key TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE entity_identity_key TYPE array<object>;
DEFINE FIELD subjects ON TABLE entity_identity_key TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE entity_identity_key TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Entity identity key-specific fields
DEFINE FIELD key ON TABLE entity_identity_key TYPE string;
DEFINE FIELD entity ON TABLE entity_identity_key TYPE record;
DEFINE FIELD confidence ON TABLE entity_identity_key TYPE number;
DEFINE FIELD first_seen ON TABLE entity_identity_key TYPE option<datetime>;
DEFINE FIELD last_seen ON TABLE entity_identity_key TYPE option<datetime>;
DEFINE FIELD evidence ON TABLE entity_identity_key TYPE array<record>;
DEFINE FIELD status ON TABLE entity_identity_key TYPE string ASSERT $value INSIDE ['active', 'revoked'] DEFAULT 'active';

-- =============================================================================
-- Fact Table Fields (DEPRECATED: removed in favor of Rel + Attribute)
-- =============================================================================
-- All fact table field definitions have been removed.

-- =============================================================================
-- System Event (Outbox) Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE system_event TYPE string;
DEFINE FIELD created_at ON TABLE system_event TYPE datetime;
DEFINE FIELD updated_at ON TABLE system_event TYPE datetime;
DEFINE FIELD created_by ON TABLE system_event TYPE string;
DEFINE FIELD deleted_at ON TABLE system_event TYPE option<datetime>;
DEFINE FIELD security ON TABLE system_event TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE system_event TYPE string;
DEFINE FIELD security.level ON TABLE system_event TYPE number;
DEFINE FIELD security.labels ON TABLE system_event TYPE array<string>;
DEFINE FIELD security.handling ON TABLE system_event TYPE array<string>;
DEFINE FIELD provenance ON TABLE system_event TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE system_event TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE system_event TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE system_event TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE system_event TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE system_event TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE system_event TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE system_event TYPE array<object>;
DEFINE FIELD subjects ON TABLE system_event TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE system_event TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- System event-specific fields
DEFINE FIELD seq ON TABLE system_event TYPE number;
DEFINE FIELD event_type ON TABLE system_event TYPE string;
DEFINE FIELD payload ON TABLE system_event TYPE object;

-- =============================================================================
-- Source Table Fields
-- =============================================================================

DEFINE FIELD space_id ON TABLE source TYPE string;
DEFINE FIELD created_at ON TABLE source TYPE datetime;
DEFINE FIELD updated_at ON TABLE source TYPE datetime;
DEFINE FIELD created_by ON TABLE source TYPE string;
DEFINE FIELD deleted_at ON TABLE source TYPE option<datetime>;
DEFINE FIELD security ON TABLE source TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE source TYPE string;
DEFINE FIELD security.level ON TABLE source TYPE number;
DEFINE FIELD security.labels ON TABLE source TYPE array<string>;
DEFINE FIELD security.handling ON TABLE source TYPE array<string>;
DEFINE FIELD provenance ON TABLE source TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE source TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE source TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE source TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE source TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE source TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE source TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE source TYPE array<object>;
DEFINE FIELD subjects ON TABLE source TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE source TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

DEFINE FIELD type ON TABLE source TYPE string;
DEFINE FIELD external_id ON TABLE source TYPE option<string>;
DEFINE FIELD uri ON TABLE source TYPE option<string>;
DEFINE FIELD path ON TABLE source TYPE option<string>;
DEFINE FIELD parent ON TABLE source TYPE option<record>;
DEFINE FIELD range ON TABLE source TYPE option<object>;
DEFINE FIELD timestamp ON TABLE source TYPE option<datetime>;
DEFINE FIELD author ON TABLE source TYPE option<string>;
DEFINE FIELD channel ON TABLE source TYPE option<string>;
DEFINE FIELD hash ON TABLE source TYPE string;
DEFINE FIELD metadata ON TABLE source TYPE object FLEXIBLE;

-- =============================================================================
-- Audit Log Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE audit_log TYPE string;
DEFINE FIELD created_at ON TABLE audit_log TYPE datetime;
DEFINE FIELD updated_at ON TABLE audit_log TYPE datetime;
DEFINE FIELD created_by ON TABLE audit_log TYPE string;
DEFINE FIELD deleted_at ON TABLE audit_log TYPE option<datetime>;
DEFINE FIELD security ON TABLE audit_log TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE audit_log TYPE string;
DEFINE FIELD security.level ON TABLE audit_log TYPE number;
DEFINE FIELD security.labels ON TABLE audit_log TYPE array<string>;
DEFINE FIELD security.handling ON TABLE audit_log TYPE array<string>;
DEFINE FIELD provenance ON TABLE audit_log TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE audit_log TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE audit_log TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE audit_log TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE audit_log TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE audit_log TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE audit_log TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE audit_log TYPE array<object>;
DEFINE FIELD subjects ON TABLE audit_log TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE audit_log TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Audit log-specific fields
DEFINE FIELD action ON TABLE audit_log TYPE string ASSERT $value INSIDE ['create', 'update', 'delete', 'promote', 'merge'];
DEFINE FIELD actor ON TABLE audit_log TYPE string;
DEFINE FIELD record ON TABLE audit_log TYPE record;
DEFINE FIELD before ON TABLE audit_log TYPE option<object>;
DEFINE FIELD after ON TABLE audit_log TYPE option<object>;
DEFINE FIELD timestamp ON TABLE audit_log TYPE datetime;
DEFINE FIELD reason ON TABLE audit_log TYPE option<string>;

-- =============================================================================
-- ID Map Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE id_map TYPE string;
DEFINE FIELD created_at ON TABLE id_map TYPE datetime;
DEFINE FIELD updated_at ON TABLE id_map TYPE datetime;
DEFINE FIELD created_by ON TABLE id_map TYPE string;
DEFINE FIELD deleted_at ON TABLE id_map TYPE option<datetime>;
DEFINE FIELD security ON TABLE id_map TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE id_map TYPE string;
DEFINE FIELD security.level ON TABLE id_map TYPE number;
DEFINE FIELD security.labels ON TABLE id_map TYPE array<string>;
DEFINE FIELD security.handling ON TABLE id_map TYPE array<string>;
DEFINE FIELD provenance ON TABLE id_map TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE id_map TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE id_map TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE id_map TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE id_map TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE id_map TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE id_map TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE id_map TYPE array<object>;
DEFINE FIELD subjects ON TABLE id_map TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE id_map TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- ID map-specific fields
DEFINE FIELD legacy_id ON TABLE id_map TYPE string;
DEFINE FIELD record ON TABLE id_map TYPE record;
DEFINE FIELD source_system ON TABLE id_map TYPE string;

-- =============================================================================
-- Identity Candidate Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE identity_candidate TYPE string;
DEFINE FIELD created_at ON TABLE identity_candidate TYPE datetime;
DEFINE FIELD updated_at ON TABLE identity_candidate TYPE datetime;
DEFINE FIELD created_by ON TABLE identity_candidate TYPE string;
DEFINE FIELD deleted_at ON TABLE identity_candidate TYPE option<datetime>;
DEFINE FIELD security ON TABLE identity_candidate TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE identity_candidate TYPE string;
DEFINE FIELD security.level ON TABLE identity_candidate TYPE number;
DEFINE FIELD security.labels ON TABLE identity_candidate TYPE array<string>;
DEFINE FIELD security.handling ON TABLE identity_candidate TYPE array<string>;
DEFINE FIELD provenance ON TABLE identity_candidate TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE identity_candidate TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE identity_candidate TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE identity_candidate TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE identity_candidate TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE identity_candidate TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE identity_candidate TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE identity_candidate TYPE array<object>;
DEFINE FIELD subjects ON TABLE identity_candidate TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE identity_candidate TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Identity candidate-specific fields
DEFINE FIELD entity_a ON TABLE identity_candidate TYPE record;
DEFINE FIELD entity_b ON TABLE identity_candidate TYPE record;
DEFINE FIELD signals ON TABLE identity_candidate TYPE object;
DEFINE FIELD confidence ON TABLE identity_candidate TYPE number;
DEFINE FIELD status ON TABLE identity_candidate TYPE string ASSERT $value INSIDE ['pending', 'approved', 'rejected'] DEFAULT 'pending';
DEFINE FIELD proposed_by ON TABLE identity_candidate TYPE string;
DEFINE FIELD decided_by ON TABLE identity_candidate TYPE option<string>;
DEFINE FIELD decided_at ON TABLE identity_candidate TYPE option<datetime>;

-- =============================================================================
-- Rel Evidence Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE rel_evidence TYPE string;
DEFINE FIELD created_at ON TABLE rel_evidence TYPE datetime;
DEFINE FIELD updated_at ON TABLE rel_evidence TYPE datetime;
DEFINE FIELD created_by ON TABLE rel_evidence TYPE string;
DEFINE FIELD deleted_at ON TABLE rel_evidence TYPE option<datetime>;
DEFINE FIELD security ON TABLE rel_evidence TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE rel_evidence TYPE string;
DEFINE FIELD security.level ON TABLE rel_evidence TYPE number;
DEFINE FIELD security.labels ON TABLE rel_evidence TYPE array<string>;
DEFINE FIELD security.handling ON TABLE rel_evidence TYPE array<string>;
DEFINE FIELD provenance ON TABLE rel_evidence TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE rel_evidence TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE rel_evidence TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE rel_evidence TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE rel_evidence TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE rel_evidence TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE rel_evidence TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE rel_evidence TYPE array<object>;
DEFINE FIELD subjects ON TABLE rel_evidence TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE rel_evidence TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Rel evidence-specific fields
DEFINE FIELD rel ON TABLE rel_evidence TYPE record;
DEFINE FIELD fact ON TABLE rel_evidence TYPE record;
DEFINE FIELD source ON TABLE rel_evidence TYPE record;
DEFINE FIELD confidence ON TABLE rel_evidence TYPE number;
DEFINE FIELD observed_at ON TABLE rel_evidence TYPE datetime;
DEFINE FIELD run_id ON TABLE rel_evidence TYPE option<string>;

-- =============================================================================
-- Entity Merge Table Fields
-- =============================================================================

-- Base fields
DEFINE FIELD space_id ON TABLE entity_merge TYPE string;
DEFINE FIELD created_at ON TABLE entity_merge TYPE datetime;
DEFINE FIELD updated_at ON TABLE entity_merge TYPE datetime;
DEFINE FIELD created_by ON TABLE entity_merge TYPE string;
DEFINE FIELD deleted_at ON TABLE entity_merge TYPE option<datetime>;
DEFINE FIELD security ON TABLE entity_merge TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE entity_merge TYPE string;
DEFINE FIELD security.level ON TABLE entity_merge TYPE number;
DEFINE FIELD security.labels ON TABLE entity_merge TYPE array<string>;
DEFINE FIELD security.handling ON TABLE entity_merge TYPE array<string>;
DEFINE FIELD provenance ON TABLE entity_merge TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE entity_merge TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE entity_merge TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE entity_merge TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE entity_merge TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE entity_merge TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE entity_merge TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE entity_merge TYPE array<object>;
DEFINE FIELD subjects ON TABLE entity_merge TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE entity_merge TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Entity merge-specific fields
DEFINE FIELD from ON TABLE entity_merge TYPE record;
DEFINE FIELD into ON TABLE entity_merge TYPE record;
DEFINE FIELD strategy ON TABLE entity_merge TYPE string ASSERT $value INSIDE ['keep_a', 'keep_b', 'combine'];
DEFINE FIELD decided_by ON TABLE entity_merge TYPE string;
DEFINE FIELD decided_at ON TABLE entity_merge TYPE datetime;
DEFINE FIELD run_id ON TABLE entity_merge TYPE option<string>;

-- =============================================================================
-- Assertion Table Fields (v0.8 Smart Ingestion Pipeline)
-- =============================================================================

DEFINE TABLE assertion SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE assertion TYPE string;
DEFINE FIELD created_at ON TABLE assertion TYPE datetime;
DEFINE FIELD updated_at ON TABLE assertion TYPE datetime;
DEFINE FIELD created_by ON TABLE assertion TYPE string;
DEFINE FIELD deleted_at ON TABLE assertion TYPE option<datetime>;
DEFINE FIELD security ON TABLE assertion TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE assertion TYPE string;
DEFINE FIELD security.level ON TABLE assertion TYPE number;
DEFINE FIELD security.labels ON TABLE assertion TYPE array<string>;
DEFINE FIELD security.handling ON TABLE assertion TYPE array<string>;
DEFINE FIELD provenance ON TABLE assertion TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE assertion TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE assertion TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE assertion TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE assertion TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE assertion TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE assertion TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE assertion TYPE array<object>;
DEFINE FIELD subjects ON TABLE assertion TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE assertion TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Assertion-specific fields
DEFINE FIELD subject_text ON TABLE assertion TYPE string;
DEFINE FIELD predicate_raw ON TABLE assertion TYPE string;
DEFINE FIELD predicate_id ON TABLE assertion TYPE option<string>;
DEFINE FIELD object_text ON TABLE assertion TYPE option<string>;
DEFINE FIELD object_literal ON TABLE assertion TYPE option<object> FLEXIBLE;
DEFINE FIELD evidence ON TABLE assertion TYPE object FLEXIBLE;
DEFINE FIELD source_event_time ON TABLE assertion TYPE option<datetime>;
DEFINE FIELD observed_at ON TABLE assertion TYPE datetime;
DEFINE FIELD asserted_at ON TABLE assertion TYPE datetime;
DEFINE FIELD assertion_key ON TABLE assertion TYPE string;
DEFINE FIELD resolution_status ON TABLE assertion TYPE string ASSERT $value INSIDE ['pending', 'resolved', 'partial', 'abstained', 'failed', 'superseded'] DEFAULT 'pending';
DEFINE FIELD resolution_method ON TABLE assertion TYPE option<string>;
DEFINE FIELD resolution_confidence ON TABLE assertion TYPE option<number>;
DEFINE FIELD retry_state ON TABLE assertion TYPE object DEFAULT {};
DEFINE FIELD retry_state.resolution_attempts ON TABLE assertion TYPE number DEFAULT 0;
DEFINE FIELD retry_state.last_resolution_at ON TABLE assertion TYPE option<datetime>;
DEFINE FIELD retry_state.abstention_reason ON TABLE assertion TYPE option<string>;
DEFINE FIELD subject_entity ON TABLE assertion TYPE option<record>;
DEFINE FIELD object_entity ON TABLE assertion TYPE option<record>;
DEFINE FIELD confidence ON TABLE assertion TYPE number DEFAULT 0.0;

-- =============================================================================
-- Attribute Table Fields (v0.8 Smart Ingestion Pipeline)
-- =============================================================================

DEFINE TABLE attribute SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE attribute TYPE string;
DEFINE FIELD created_at ON TABLE attribute TYPE datetime;
DEFINE FIELD updated_at ON TABLE attribute TYPE datetime;
DEFINE FIELD created_by ON TABLE attribute TYPE string;
DEFINE FIELD deleted_at ON TABLE attribute TYPE option<datetime>;
DEFINE FIELD security ON TABLE attribute TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE attribute TYPE string;
DEFINE FIELD security.level ON TABLE attribute TYPE number;
DEFINE FIELD security.labels ON TABLE attribute TYPE array<string>;
DEFINE FIELD security.handling ON TABLE attribute TYPE array<string>;
DEFINE FIELD provenance ON TABLE attribute TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE attribute TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE attribute TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE attribute TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE attribute TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE attribute TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE attribute TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE attribute TYPE array<object>;
DEFINE FIELD subjects ON TABLE attribute TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE attribute TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Attribute-specific fields
DEFINE FIELD subject ON TABLE attribute TYPE record;
DEFINE FIELD attr_type ON TABLE attribute TYPE string;
DEFINE FIELD value ON TABLE attribute TYPE object FLEXIBLE;
DEFINE FIELD status ON TABLE attribute TYPE string ASSERT $value INSIDE ['candidate', 'approved', 'superseded', 'disputed'] DEFAULT 'candidate';
DEFINE FIELD confidence ON TABLE attribute TYPE number DEFAULT 0.0;
DEFINE FIELD validity_window ON TABLE attribute TYPE option<object> FLEXIBLE;
DEFINE FIELD evidence ON TABLE attribute TYPE array DEFAULT [];
DEFINE FIELD temporal_audit ON TABLE attribute TYPE option<object> FLEXIBLE;

-- =============================================================================
-- Truth Slot Table Fields (v0.8 Smart Ingestion Pipeline)
-- =============================================================================

DEFINE TABLE truth_slot SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE truth_slot TYPE string;
DEFINE FIELD created_at ON TABLE truth_slot TYPE datetime;
DEFINE FIELD updated_at ON TABLE truth_slot TYPE datetime;
DEFINE FIELD created_by ON TABLE truth_slot TYPE string;
DEFINE FIELD deleted_at ON TABLE truth_slot TYPE option<datetime>;
DEFINE FIELD security ON TABLE truth_slot TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE truth_slot TYPE string;
DEFINE FIELD security.level ON TABLE truth_slot TYPE number;
DEFINE FIELD security.labels ON TABLE truth_slot TYPE array<string>;
DEFINE FIELD security.handling ON TABLE truth_slot TYPE array<string>;
DEFINE FIELD provenance ON TABLE truth_slot TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE truth_slot TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE truth_slot TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE truth_slot TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE truth_slot TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE truth_slot TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE truth_slot TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE truth_slot TYPE array<object>;
DEFINE FIELD subjects ON TABLE truth_slot TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE truth_slot TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Truth slot-specific fields
DEFINE FIELD slot_key ON TABLE truth_slot TYPE string;
DEFINE FIELD slot_type ON TABLE truth_slot TYPE string ASSERT $value INSIDE ['rel', 'attribute'];
DEFINE FIELD subject_entity ON TABLE truth_slot TYPE string;
DEFINE FIELD predicate_id ON TABLE truth_slot TYPE string;
DEFINE FIELD current_values ON TABLE truth_slot TYPE array<object> FLEXIBLE DEFAULT [];
DEFINE FIELD conflict_group ON TABLE truth_slot TYPE option<record>;
DEFINE FIELD confidence ON TABLE truth_slot TYPE number DEFAULT 0.0;
DEFINE FIELD rationale ON TABLE truth_slot TYPE option<string>;
DEFINE FIELD semantics_version ON TABLE truth_slot TYPE number DEFAULT 0;
DEFINE FIELD alias_version ON TABLE truth_slot TYPE number DEFAULT 0;
DEFINE FIELD stale ON TABLE truth_slot TYPE bool DEFAULT false;
DEFINE FIELD stale_reason ON TABLE truth_slot TYPE option<string>;
DEFINE FIELD computed_at ON TABLE truth_slot TYPE datetime;
DEFINE FIELD security_hash ON TABLE truth_slot TYPE string;

-- =============================================================================
-- Conflict Group Table Fields (v0.8 Smart Ingestion Pipeline)
-- =============================================================================

DEFINE TABLE conflict_group SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE conflict_group TYPE string;
DEFINE FIELD created_at ON TABLE conflict_group TYPE datetime;
DEFINE FIELD updated_at ON TABLE conflict_group TYPE datetime;
DEFINE FIELD created_by ON TABLE conflict_group TYPE string;
DEFINE FIELD deleted_at ON TABLE conflict_group TYPE option<datetime>;
DEFINE FIELD security ON TABLE conflict_group TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE conflict_group TYPE string;
DEFINE FIELD security.level ON TABLE conflict_group TYPE number;
DEFINE FIELD security.labels ON TABLE conflict_group TYPE array<string>;
DEFINE FIELD security.handling ON TABLE conflict_group TYPE array<string>;
DEFINE FIELD provenance ON TABLE conflict_group TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE conflict_group TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE conflict_group TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE conflict_group TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE conflict_group TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE conflict_group TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE conflict_group TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE conflict_group TYPE array<object>;
DEFINE FIELD subjects ON TABLE conflict_group TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE conflict_group TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Conflict group-specific fields
DEFINE FIELD slot_key ON TABLE conflict_group TYPE string;
DEFINE FIELD slot_type ON TABLE conflict_group TYPE string ASSERT $value INSIDE ['rel', 'attribute'];
DEFINE FIELD claim_ids ON TABLE conflict_group TYPE array<string> DEFAULT [];
DEFINE FIELD resolved ON TABLE conflict_group TYPE bool DEFAULT false;
DEFINE FIELD resolution_rationale ON TABLE conflict_group TYPE option<string>;
DEFINE FIELD detected_at ON TABLE conflict_group TYPE datetime;
DEFINE FIELD resolved_at ON TABLE conflict_group TYPE option<datetime>;

-- =============================================================================
-- Predicate Registry Table Fields (v0.8 Smart Ingestion Pipeline)
-- =============================================================================

DEFINE TABLE predicate_registry SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE predicate_registry TYPE string;
DEFINE FIELD created_at ON TABLE predicate_registry TYPE datetime;
DEFINE FIELD updated_at ON TABLE predicate_registry TYPE datetime;
DEFINE FIELD created_by ON TABLE predicate_registry TYPE string;
DEFINE FIELD deleted_at ON TABLE predicate_registry TYPE option<datetime>;
DEFINE FIELD security ON TABLE predicate_registry TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE predicate_registry TYPE string;
DEFINE FIELD security.level ON TABLE predicate_registry TYPE number;
DEFINE FIELD security.labels ON TABLE predicate_registry TYPE array<string>;
DEFINE FIELD security.handling ON TABLE predicate_registry TYPE array<string>;
DEFINE FIELD provenance ON TABLE predicate_registry TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE predicate_registry TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE predicate_registry TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE predicate_registry TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE predicate_registry TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE predicate_registry TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE predicate_registry TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE predicate_registry TYPE array<object>;
DEFINE FIELD subjects ON TABLE predicate_registry TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE predicate_registry TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Predicate registry-specific fields
DEFINE FIELD predicate_id ON TABLE predicate_registry TYPE string;
DEFINE FIELD description ON TABLE predicate_registry TYPE option<string>;
DEFINE FIELD status ON TABLE predicate_registry TYPE string DEFAULT 'active';
DEFINE FIELD semantics ON TABLE predicate_registry TYPE option<object> FLEXIBLE;
DEFINE FIELD semantics_status ON TABLE predicate_registry TYPE string ASSERT $value INSIDE ['configured', 'default', 'needs_review'] DEFAULT 'default';
DEFINE FIELD first_seen_at ON TABLE predicate_registry TYPE option<datetime>;
DEFINE FIELD usage_count ON TABLE predicate_registry TYPE number DEFAULT 0;
DEFINE FIELD last_used_at ON TABLE predicate_registry TYPE option<datetime>;

-- =============================================================================
-- Predicate Alias Table Fields (v0.8 Smart Ingestion Pipeline)
-- =============================================================================

DEFINE TABLE predicate_alias SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE predicate_alias TYPE string;
DEFINE FIELD created_at ON TABLE predicate_alias TYPE datetime;
DEFINE FIELD updated_at ON TABLE predicate_alias TYPE datetime;
DEFINE FIELD created_by ON TABLE predicate_alias TYPE string;
DEFINE FIELD deleted_at ON TABLE predicate_alias TYPE option<datetime>;
DEFINE FIELD security ON TABLE predicate_alias TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE predicate_alias TYPE string;
DEFINE FIELD security.level ON TABLE predicate_alias TYPE number;
DEFINE FIELD security.labels ON TABLE predicate_alias TYPE array<string>;
DEFINE FIELD security.handling ON TABLE predicate_alias TYPE array<string>;
DEFINE FIELD provenance ON TABLE predicate_alias TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE predicate_alias TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE predicate_alias TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE predicate_alias TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE predicate_alias TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE predicate_alias TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE predicate_alias TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE predicate_alias TYPE array<object>;
DEFINE FIELD subjects ON TABLE predicate_alias TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE predicate_alias TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Predicate alias-specific fields
DEFINE FIELD alias ON TABLE predicate_alias TYPE string;
DEFINE FIELD alias_type ON TABLE predicate_alias TYPE string ASSERT $value INSIDE ['raw_string', 'plugin_code', 'schema_field'] DEFAULT 'raw_string';
DEFINE FIELD source_plugin ON TABLE predicate_alias TYPE option<string>;
DEFINE FIELD confidence ON TABLE predicate_alias TYPE number DEFAULT 0.0;
DEFINE FIELD status ON TABLE predicate_alias TYPE string ASSERT $value INSIDE ['proposed', 'active', 'deprecated'] DEFAULT 'proposed';
DEFINE FIELD scope ON TABLE predicate_alias TYPE string ASSERT $value INSIDE ['global', 'plugin_only', 'source_type'] DEFAULT 'global';
DEFINE FIELD predicate_id ON TABLE predicate_alias TYPE string;
DEFINE FIELD confidence_version ON TABLE predicate_alias TYPE number DEFAULT 0;
DEFINE FIELD usage_count ON TABLE predicate_alias TYPE number DEFAULT 0;

-- =============================================================================
-- Freshness Table Fields (v0.8 Ranking)
-- =============================================================================

DEFINE TABLE freshness SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE freshness TYPE string;
DEFINE FIELD created_at ON TABLE freshness TYPE datetime;
DEFINE FIELD updated_at ON TABLE freshness TYPE datetime;
DEFINE FIELD created_by ON TABLE freshness TYPE string;
DEFINE FIELD deleted_at ON TABLE freshness TYPE option<datetime>;
DEFINE FIELD security ON TABLE freshness TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE freshness TYPE string;
DEFINE FIELD security.level ON TABLE freshness TYPE number;
DEFINE FIELD security.labels ON TABLE freshness TYPE array<string>;
DEFINE FIELD security.handling ON TABLE freshness TYPE array<string>;
DEFINE FIELD provenance ON TABLE freshness TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE freshness TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE freshness TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE freshness TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE freshness TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE freshness TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE freshness TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE freshness TYPE array<object>;
DEFINE FIELD subjects ON TABLE freshness TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE freshness TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Freshness-specific fields
DEFINE FIELD target ON TABLE freshness TYPE string;
DEFINE FIELD target_type ON TABLE freshness TYPE string;
DEFINE FIELD decay_model ON TABLE freshness TYPE string ASSERT $value INSIDE ['linear', 'exponential', 'step_function'];
DEFINE FIELD score ON TABLE freshness TYPE number;
DEFINE FIELD confidence ON TABLE freshness TYPE number;
DEFINE FIELD rationale ON TABLE freshness TYPE option<string>;
DEFINE FIELD factors ON TABLE freshness TYPE object FLEXIBLE;
DEFINE FIELD computed_at ON TABLE freshness TYPE datetime;
DEFINE FIELD next_review_at ON TABLE freshness TYPE option<datetime>;
DEFINE FIELD demote ON TABLE freshness TYPE bool DEFAULT false;
DEFINE FIELD demotion_threshold ON TABLE freshness TYPE option<number>;

-- =============================================================================
-- Reprocess Job Table Fields (v0.8 Backfill)
-- =============================================================================

DEFINE TABLE reprocess_job SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE reprocess_job TYPE string;
DEFINE FIELD created_at ON TABLE reprocess_job TYPE datetime;
DEFINE FIELD updated_at ON TABLE reprocess_job TYPE datetime;
DEFINE FIELD created_by ON TABLE reprocess_job TYPE string;
DEFINE FIELD deleted_at ON TABLE reprocess_job TYPE option<datetime>;
DEFINE FIELD security ON TABLE reprocess_job TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE reprocess_job TYPE string;
DEFINE FIELD security.level ON TABLE reprocess_job TYPE number;
DEFINE FIELD security.labels ON TABLE reprocess_job TYPE array<string>;
DEFINE FIELD security.handling ON TABLE reprocess_job TYPE array<string>;
DEFINE FIELD provenance ON TABLE reprocess_job TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE reprocess_job TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE reprocess_job TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE reprocess_job TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE reprocess_job TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE reprocess_job TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE reprocess_job TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE reprocess_job TYPE array<object>;
DEFINE FIELD subjects ON TABLE reprocess_job TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE reprocess_job TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Reprocess job-specific fields
DEFINE FIELD name ON TABLE reprocess_job TYPE string;
DEFINE FIELD selection_criteria ON TABLE reprocess_job TYPE object FLEXIBLE;
DEFINE FIELD target_stages ON TABLE reprocess_job TYPE array<string>;
DEFINE FIELD status ON TABLE reprocess_job TYPE string ASSERT $value INSIDE ['pending', 'running', 'paused', 'completed', 'failed', 'cancelled'] DEFAULT 'pending';
DEFINE FIELD progress ON TABLE reprocess_job TYPE object FLEXIBLE;
DEFINE FIELD priority ON TABLE reprocess_job TYPE number DEFAULT 0;
DEFINE FIELD budget_limit_euros ON TABLE reprocess_job TYPE option<number>;
DEFINE FIELD max_sources_per_day ON TABLE reprocess_job TYPE option<number>;
DEFINE FIELD started_at ON TABLE reprocess_job TYPE option<datetime>;
DEFINE FIELD completed_at ON TABLE reprocess_job TYPE option<datetime>;
DEFINE FIELD error ON TABLE reprocess_job TYPE option<string>;
DEFINE FIELD reason ON TABLE reprocess_job TYPE option<string>;

-- =============================================================================
-- Cost Ledger Daily Table Fields (v0.8 Budget Tracking)
-- =============================================================================

DEFINE TABLE cost_ledger_daily SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE cost_ledger_daily TYPE string;
DEFINE FIELD created_at ON TABLE cost_ledger_daily TYPE datetime;
DEFINE FIELD updated_at ON TABLE cost_ledger_daily TYPE datetime;
DEFINE FIELD created_by ON TABLE cost_ledger_daily TYPE string;
DEFINE FIELD deleted_at ON TABLE cost_ledger_daily TYPE option<datetime>;
DEFINE FIELD security ON TABLE cost_ledger_daily TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE cost_ledger_daily TYPE string;
DEFINE FIELD security.level ON TABLE cost_ledger_daily TYPE number;
DEFINE FIELD security.labels ON TABLE cost_ledger_daily TYPE array<string>;
DEFINE FIELD security.handling ON TABLE cost_ledger_daily TYPE array<string>;
DEFINE FIELD provenance ON TABLE cost_ledger_daily TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE cost_ledger_daily TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE cost_ledger_daily TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE cost_ledger_daily TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE cost_ledger_daily TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE cost_ledger_daily TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE cost_ledger_daily TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE cost_ledger_daily TYPE array<object>;
DEFINE FIELD subjects ON TABLE cost_ledger_daily TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE cost_ledger_daily TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Cost ledger-specific fields
DEFINE FIELD date ON TABLE cost_ledger_daily TYPE datetime;
DEFINE FIELD mode ON TABLE cost_ledger_daily TYPE string ASSERT $value IS NOT NONE AND $value INSIDE ['skip', 'metadata', 'retrieval', 'graph'];
DEFINE FIELD worker_kind ON TABLE cost_ledger_daily TYPE string;
DEFINE FIELD total_euros ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
DEFINE FIELD total_items ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
DEFINE FIELD total_tokens ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
DEFINE FIELD total_runs ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
DEFINE FIELD total_failures ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
DEFINE FIELD reserved_euros ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
DEFINE FIELD reconciled_euros ON TABLE cost_ledger_daily TYPE number ASSERT $value IS NOT NONE;
