-- Elephant Core Schema Indexes
-- Migration: 002_indexes
-- This creates all indexes for efficient querying.

-- =============================================================================
-- Entity Indexes
-- =============================================================================

DEFINE INDEX entity_name ON TABLE entity FIELDS display_name SEARCH ANALYZER simple BM25;
DEFINE INDEX entity_summary ON TABLE entity FIELDS summary SEARCH ANALYZER simple BM25;
DEFINE INDEX entity_alias ON TABLE entity FIELDS aliases SEARCH ANALYZER simple BM25;
DEFINE INDEX entity_type ON TABLE entity FIELDS type;
DEFINE INDEX entity_space ON TABLE entity FIELDS space_id;
DEFINE INDEX entity_status ON TABLE entity FIELDS status;
DEFINE INDEX entity_deleted_space ON TABLE entity FIELDS space_id, deleted_at;
DEFINE INDEX entity_redirect ON TABLE entity FIELDS redirect_to;
DEFINE INDEX security_policy_level ON TABLE entity FIELDS security.policy_id, security.level;

-- =============================================================================
-- Event Indexes
-- =============================================================================


-- =============================================================================
-- Doc Indexes
-- =============================================================================

DEFINE INDEX doc_space ON TABLE doc FIELDS space_id;
DEFINE INDEX doc_deleted_space ON TABLE doc FIELDS space_id, deleted_at;
DEFINE INDEX doc_rendition_unique ON TABLE doc FIELDS space_id, rendition_key UNIQUE;
DEFINE INDEX security_policy_level_doc ON TABLE doc FIELDS security.policy_id, security.level;

-- =============================================================================
-- Doc Chunk Indexes
-- =============================================================================

DEFINE INDEX doc_chunk_space ON TABLE doc_chunk FIELDS space_id;
DEFINE INDEX doc_chunk_deleted_space ON TABLE doc_chunk FIELDS space_id, deleted_at;
DEFINE INDEX doc_chunk_text ON TABLE doc_chunk FIELDS text SEARCH ANALYZER simple BM25;
DEFINE INDEX doc_chunk_vec ON TABLE doc_chunk FIELDS embedding HNSW DIMENSION 3072 DIST COSINE;
DEFINE INDEX chunk_key_unique ON TABLE doc_chunk FIELDS doc, chunk_key UNIQUE;
DEFINE INDEX doc_chunk_doc ON TABLE doc_chunk FIELDS doc;

-- =============================================================================
-- Chunk Embedding Indexes
-- =============================================================================

DEFINE INDEX chunk_embedding_space ON TABLE chunk_embedding FIELDS space_id;
DEFINE INDEX chunk_embedding_deleted_space ON TABLE chunk_embedding FIELDS space_id, deleted_at;
DEFINE INDEX chunk_embedding_model ON TABLE chunk_embedding FIELDS model;
DEFINE INDEX chunk_embedding_chunk ON TABLE chunk_embedding FIELDS chunk;

-- =============================================================================
-- Identity Indexes
-- =============================================================================

DEFINE INDEX entity_identity_key_space ON TABLE entity_identity_key FIELDS space_id;
DEFINE INDEX entity_identity_key_deleted_space ON TABLE entity_identity_key FIELDS space_id, deleted_at;
DEFINE INDEX entity_identity_key_unique ON TABLE entity_identity_key FIELDS space_id, key UNIQUE;
DEFINE INDEX entity_identity_key_entity ON TABLE entity_identity_key FIELDS entity;
DEFINE INDEX identity_candidate_space ON TABLE identity_candidate FIELDS space_id;
DEFINE INDEX identity_candidate_deleted_space ON TABLE identity_candidate FIELDS space_id, deleted_at;
DEFINE INDEX identity_candidate_status ON TABLE identity_candidate FIELDS status;
DEFINE INDEX identity_candidate_entities ON TABLE identity_candidate FIELDS entity_a, entity_b;

-- =============================================================================
-- Fact Indexes (DEPRECATED: fact table removed)
-- =============================================================================

-- =============================================================================
-- Source Indexes
-- =============================================================================

DEFINE INDEX source_space ON TABLE source FIELDS space_id;
DEFINE INDEX source_deleted_space ON TABLE source FIELDS space_id, deleted_at;
DEFINE INDEX source_external ON TABLE source FIELDS type, external_id;
DEFINE INDEX source_dedup ON TABLE source FIELDS type, external_id, hash;

-- =============================================================================
-- System Event (Outbox) Indexes
-- =============================================================================

DEFINE INDEX system_event_space ON TABLE system_event FIELDS space_id;
DEFINE INDEX system_event_deleted_space ON TABLE system_event FIELDS space_id, deleted_at;
DEFINE INDEX system_event_space_seq ON TABLE system_event FIELDS space_id, seq UNIQUE;

-- =============================================================================
-- Relationship Indexes
-- =============================================================================

DEFINE INDEX rel_space ON TABLE rel FIELDS space_id;
DEFINE INDEX rel_deleted_space ON TABLE rel FIELDS space_id, deleted_at;
DEFINE INDEX rel_type ON TABLE rel FIELDS rel_type;
DEFINE INDEX rel_status ON TABLE rel FIELDS status;
DEFINE INDEX rel_evidence_space ON TABLE rel_evidence FIELDS space_id;
DEFINE INDEX rel_evidence_deleted_space ON TABLE rel_evidence FIELDS space_id, deleted_at;
DEFINE INDEX rel_evidence_rel ON TABLE rel_evidence FIELDS rel;

-- =============================================================================
-- Entity Merge Indexes
-- =============================================================================

DEFINE INDEX entity_merge_space ON TABLE entity_merge FIELDS space_id;
DEFINE INDEX entity_merge_deleted_space ON TABLE entity_merge FIELDS space_id, deleted_at;
DEFINE INDEX entity_merge_into ON TABLE entity_merge FIELDS into;
DEFINE INDEX entity_merge_from ON TABLE entity_merge FIELDS from;

-- =============================================================================
-- Audit Log Indexes
-- =============================================================================

DEFINE INDEX audit_log_space ON TABLE audit_log FIELDS space_id;
DEFINE INDEX audit_log_deleted_space ON TABLE audit_log FIELDS space_id, deleted_at;
DEFINE INDEX audit_log_record ON TABLE audit_log FIELDS record;
DEFINE INDEX audit_log_timestamp ON TABLE audit_log FIELDS timestamp;
DEFINE INDEX audit_log_actor ON TABLE audit_log FIELDS actor;
DEFINE INDEX audit_log_action ON TABLE audit_log FIELDS action;

-- =============================================================================
-- ID Map Indexes
-- =============================================================================

DEFINE INDEX id_map_space ON TABLE id_map FIELDS space_id;
DEFINE INDEX id_map_deleted_space ON TABLE id_map FIELDS space_id, deleted_at;
DEFINE INDEX id_map_lookup ON TABLE id_map FIELDS space_id, source_system, legacy_id UNIQUE;

-- =============================================================================
-- Assertion Indexes
-- =============================================================================

DEFINE INDEX assertion_space ON TABLE assertion FIELDS space_id;
DEFINE INDEX assertion_deleted_space ON TABLE assertion FIELDS space_id, deleted_at;
DEFINE INDEX assertion_key_unique ON TABLE assertion FIELDS space_id, assertion_key UNIQUE;
DEFINE INDEX assertion_resolution_status ON TABLE assertion FIELDS resolution_status;
DEFINE INDEX assertion_subject_entity ON TABLE assertion FIELDS subject_entity;
DEFINE INDEX assertion_predicate_raw ON TABLE assertion FIELDS predicate_raw;

-- =============================================================================
-- Attribute Indexes
-- =============================================================================

DEFINE INDEX attribute_space ON TABLE attribute FIELDS space_id;
DEFINE INDEX attribute_deleted_space ON TABLE attribute FIELDS space_id, deleted_at;
DEFINE INDEX attribute_subject ON TABLE attribute FIELDS subject;
DEFINE INDEX attribute_subject_type ON TABLE attribute FIELDS subject, attr_type;
DEFINE INDEX attribute_status ON TABLE attribute FIELDS status;

-- =============================================================================
-- Truth Slot Indexes
-- =============================================================================

DEFINE INDEX truth_slot_space ON TABLE truth_slot FIELDS space_id;
DEFINE INDEX truth_slot_deleted_space ON TABLE truth_slot FIELDS space_id, deleted_at;
DEFINE INDEX truth_slot_key_unique ON TABLE truth_slot FIELDS space_id, slot_key UNIQUE;
DEFINE INDEX truth_slot_subject_entity ON TABLE truth_slot FIELDS subject_entity;
DEFINE INDEX truth_slot_predicate ON TABLE truth_slot FIELDS predicate_id;
DEFINE INDEX truth_slot_stale ON TABLE truth_slot FIELDS stale;

-- =============================================================================
-- Conflict Group Indexes
-- =============================================================================

DEFINE INDEX conflict_group_space ON TABLE conflict_group FIELDS space_id;
DEFINE INDEX conflict_group_deleted_space ON TABLE conflict_group FIELDS space_id, deleted_at;
DEFINE INDEX conflict_group_slot_key ON TABLE conflict_group FIELDS slot_key;
DEFINE INDEX conflict_group_resolved ON TABLE conflict_group FIELDS resolved;

-- =============================================================================
-- Predicate Registry Indexes
-- =============================================================================

DEFINE INDEX predicate_registry_space ON TABLE predicate_registry FIELDS space_id;
DEFINE INDEX predicate_registry_deleted_space ON TABLE predicate_registry FIELDS space_id, deleted_at;
DEFINE INDEX predicate_registry_id_unique ON TABLE predicate_registry FIELDS space_id, predicate_id UNIQUE;
DEFINE INDEX predicate_registry_status ON TABLE predicate_registry FIELDS status;

-- =============================================================================
-- Predicate Alias Indexes
-- =============================================================================

DEFINE INDEX predicate_alias_space ON TABLE predicate_alias FIELDS space_id;
DEFINE INDEX predicate_alias_deleted_space ON TABLE predicate_alias FIELDS space_id, deleted_at;
DEFINE INDEX predicate_alias_lookup ON TABLE predicate_alias FIELDS space_id, alias, status;
DEFINE INDEX predicate_alias_predicate ON TABLE predicate_alias FIELDS predicate_id;
DEFINE INDEX predicate_alias_scope ON TABLE predicate_alias FIELDS scope;

-- =============================================================================
-- Freshness Indexes
-- =============================================================================

DEFINE INDEX freshness_space ON TABLE freshness FIELDS space_id;
DEFINE INDEX freshness_deleted_space ON TABLE freshness FIELDS space_id, deleted_at;
DEFINE INDEX freshness_target ON TABLE freshness FIELDS target;
DEFINE INDEX freshness_target_type ON TABLE freshness FIELDS target_type;

-- =============================================================================
-- Reprocess Job Indexes
-- =============================================================================

DEFINE INDEX reprocess_job_space ON TABLE reprocess_job FIELDS space_id;
DEFINE INDEX reprocess_job_deleted_space ON TABLE reprocess_job FIELDS space_id, deleted_at;
DEFINE INDEX reprocess_job_status ON TABLE reprocess_job FIELDS status;
DEFINE INDEX reprocess_job_priority ON TABLE reprocess_job FIELDS priority;
