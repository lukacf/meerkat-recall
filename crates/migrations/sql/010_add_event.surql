-- Add first-class domain event table.
-- Migration: 010_add_event

DEFINE TABLE event SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE event TYPE string;
DEFINE FIELD created_at ON TABLE event TYPE datetime;
DEFINE FIELD updated_at ON TABLE event TYPE datetime;
DEFINE FIELD created_by ON TABLE event TYPE string;
DEFINE FIELD deleted_at ON TABLE event TYPE option<datetime>;
DEFINE FIELD security ON TABLE event TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE event TYPE string;
DEFINE FIELD security.level ON TABLE event TYPE number;
DEFINE FIELD security.labels ON TABLE event TYPE array<string>;
DEFINE FIELD security.handling ON TABLE event TYPE array<string>;
DEFINE FIELD provenance ON TABLE event TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE event TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE event TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE event TYPE option<string>;
DEFINE FIELD provenance.work_run_id ON TABLE event TYPE option<string>;
DEFINE FIELD provenance.prompt_id ON TABLE event TYPE option<string>;
DEFINE FIELD provenance.config_id ON TABLE event TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE event TYPE array<object>;
DEFINE FIELD subjects ON TABLE event TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE event TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Event-specific fields
DEFINE FIELD title ON TABLE event TYPE string ASSERT string::len(string::trim($value)) > 0;
DEFINE FIELD summary ON TABLE event TYPE option<string>;
DEFINE FIELD status ON TABLE event TYPE string ASSERT $value INSIDE ['candidate', 'approved', 'superseded', 'rejected'] DEFAULT 'candidate';
DEFINE FIELD time_start ON TABLE event TYPE option<datetime>;
DEFINE FIELD time_end ON TABLE event TYPE option<datetime> ASSERT $value IS NONE OR time_start IS NONE OR $value >= time_start;
DEFINE FIELD time_precision ON TABLE event TYPE string ASSERT $value INSIDE ['exact', 'minute', 'hour', 'day', 'month', 'year', 'unknown'] DEFAULT 'unknown';
DEFINE FIELD confidence ON TABLE event TYPE number ASSERT $value >= 0 AND $value <= 1 DEFAULT 1;
DEFINE FIELD time_confidence ON TABLE event TYPE number ASSERT $value >= 0 AND $value <= 1 DEFAULT 1;
DEFINE FIELD assertion_refs ON TABLE event TYPE array<record> DEFAULT [];
DEFINE FIELD source_refs ON TABLE event TYPE array<record> DEFAULT [];
DEFINE FIELD metadata ON TABLE event TYPE object FLEXIBLE;

-- Event indexes
DEFINE INDEX event_space ON TABLE event FIELDS space_id;
DEFINE INDEX event_space_status_deleted ON TABLE event FIELDS space_id, status, deleted_at;
DEFINE INDEX event_space_time_start ON TABLE event FIELDS space_id, time_start;
DEFINE INDEX event_space_time_end ON TABLE event FIELDS space_id, time_end;
DEFINE INDEX event_title ON TABLE event FIELDS title SEARCH ANALYZER simple BM25;
DEFINE INDEX event_summary ON TABLE event FIELDS summary SEARCH ANALYZER simple BM25;
