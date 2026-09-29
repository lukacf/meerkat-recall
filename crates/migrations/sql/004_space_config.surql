-- =============================================================================
-- Space Config Table
-- =============================================================================

DEFINE TABLE space_config SCHEMAFULL;

-- Base fields
DEFINE FIELD space_id ON TABLE space_config TYPE string;
DEFINE FIELD created_at ON TABLE space_config TYPE datetime;
DEFINE FIELD updated_at ON TABLE space_config TYPE datetime;
DEFINE FIELD created_by ON TABLE space_config TYPE string;
DEFINE FIELD deleted_at ON TABLE space_config TYPE option<datetime>;
DEFINE FIELD security ON TABLE space_config TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD security.policy_id ON TABLE space_config TYPE string;
DEFINE FIELD security.level ON TABLE space_config TYPE number;
DEFINE FIELD security.labels ON TABLE space_config TYPE array<string>;
DEFINE FIELD security.handling ON TABLE space_config TYPE array<string>;
DEFINE FIELD provenance ON TABLE space_config TYPE object ASSERT $value IS NOT NONE;
DEFINE FIELD provenance.sources ON TABLE space_config TYPE array<record>;
DEFINE FIELD provenance.extractor ON TABLE space_config TYPE option<object>;
DEFINE FIELD provenance.run_id ON TABLE space_config TYPE option<string>;
DEFINE FIELD provenance.chain ON TABLE space_config TYPE array<object>;
DEFINE FIELD subjects ON TABLE space_config TYPE option<array<record>>;
DEFINE FIELD subjects_state ON TABLE space_config TYPE string ASSERT $value INSIDE ['complete', 'partial', 'unknown'];

-- Embedding policy fields
DEFINE FIELD embedding_provider ON TABLE space_config TYPE string;
DEFINE FIELD embedding_model ON TABLE space_config TYPE string;
DEFINE FIELD embedding_dims ON TABLE space_config TYPE number;

DEFINE INDEX space_config_space_unique ON TABLE space_config FIELDS space_id UNIQUE;
