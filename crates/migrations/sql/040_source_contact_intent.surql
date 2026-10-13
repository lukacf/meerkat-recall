-- Original authorized retained-contact effects, never supplied as a wire plan.
-- Payload belongs to its physical source and is erased with that incarnation.
DEFINE TABLE IF NOT EXISTS source_contact_intent SCHEMALESS;
DEFINE INDEX IF NOT EXISTS source_contact_space_source ON source_contact_intent FIELDS space_id,source_id;
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_contact_intent
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    LET $space = IF $event = 'DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing('canonical_epoch', $space) SET space_id=$space, epoch=<string>rand::uuid(); };
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPSERT type::thing('canonical_epoch', $before.space_id) SET space_id=$before.space_id, epoch=<string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_contact_intent
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_reference_guard($before, false); } ELSE {
        fn::recall_reference_guard($after, true);
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before, false); };
    };
};

UPSERT publication_schema:source_contact SET version=1;
