-- Original private queue producer. This is accounting provenance, not an
-- attempt, work item, authorization grant or duplicate monetary ledger.
-- Missing rows are Unknown; erasure may remove reference-bearing provenance.
DEFINE TABLE IF NOT EXISTS work_producer_origin SCHEMALESS;
DEFINE INDEX IF NOT EXISTS work_producer_space_work ON work_producer_origin FIELDS space_id,work_item_id UNIQUE;
-- New queue-origin registration and exact-read nonces cannot invalidate the
-- parent's already prepared decision. Missing origin never proves authority.
-- Semantic changes and removals remain canonical planning conflicts.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE work_producer_origin
WHEN $event IN ['UPDATE','DELETE'] THEN {
    IF $event='DELETE' {
        IF $before.space_id!=NONE { UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid(); };
    } ELSE {
        LET $origin_before = array::filter(object::entries($before), |$entry| $entry[0]!='read_nonce');
        LET $origin_after = array::filter(object::entries($after), |$entry| $entry[0]!='read_nonce');
        IF $origin_before != $origin_after {
            IF $after.space_id!=NONE { UPSERT type::thing('canonical_epoch',$after.space_id) SET space_id=$after.space_id,epoch=<string>rand::uuid(); };
            IF $before.space_id!=$after.space_id AND $before.space_id!=NONE {
                UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid();
            };
        };
    };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE work_producer_origin
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_reference_guard($before,false); } ELSE {
        fn::recall_reference_guard($after,true);
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
UPSERT publication_schema:work_producer SET version=1;

-- Closed saved selections participate in the shared predicate epoch. Checking
-- before OR after also catches changing the kind to bypass membership and moves.
DEFINE EVENT OVERWRITE reprocess_selection_predicate_fence ON TABLE work_artifact
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND
    ($before.artifact_type IN ['reprocess_selection/v1','reprocess_source_intent/v1'] OR
     $after.artifact_type IN ['reprocess_selection/v1','reprocess_source_intent/v1']) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing('canonical_epoch', $space) SET space_id=$space, epoch=<string>rand::uuid(); };
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPSERT type::thing('canonical_epoch', $before.space_id) SET space_id=$before.space_id, epoch=<string>rand::uuid();
    };
};
