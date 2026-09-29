-- Truth maintenance LLM adjudication support.
-- Migration: 016_tm_adjudication

-- Widen slot_type vocabularies for event and rel_set slots.
REMOVE FIELD slot_type ON truth_slot;
DEFINE FIELD slot_type ON TABLE truth_slot TYPE string ASSERT $value INSIDE ['rel', 'attribute', 'event', 'rel_set'];
REMOVE FIELD slot_type ON conflict_group;
DEFINE FIELD slot_type ON TABLE conflict_group TYPE string ASSERT $value INSIDE ['rel', 'attribute', 'event', 'rel_set'];

-- Adjudication memoization fields on conflict_group.
DEFINE FIELD adjudicated_claim_set_hash ON TABLE conflict_group TYPE option<string>;
DEFINE FIELD adjudicated_at ON TABLE conflict_group TYPE option<datetime>;

-- Allow LLM-proposed predicate semantics provenance.
REMOVE FIELD semantics_status ON predicate_registry;
DEFINE FIELD semantics_status ON TABLE predicate_registry TYPE string ASSERT $value INSIDE ['configured', 'default', 'needs_review', 'llm_proposed'] DEFAULT 'default';
