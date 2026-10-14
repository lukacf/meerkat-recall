-- Finalizer readiness and accepted-input recovery keep private row writes for
-- native transaction conflict detection. Those writes are not new canonical
-- inputs. Exclude only these exact fields; compare every other field, including
-- unknown future fields and absent versus explicit-null values. Keep all
-- physical/read-reference events and the original guarded nonce writes.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE ingest_decision
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    LET $semantic_before = array::filter(object::entries($before ?? {}), |$entry| $entry[0]!='canonical_read_nonce');
    LET $semantic_after = array::filter(object::entries($after ?? {}), |$entry| $entry[0]!='canonical_read_nonce');
    IF $event!='UPDATE' OR $semantic_before!=$semantic_after {
        LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
        IF $space!=NONE {
            UPSERT type::thing('canonical_epoch',$space) SET space_id=$space,epoch=<string>rand::uuid();
        };
        IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
            UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid();
        };
    };
};

DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_revision
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    LET $semantic_before = array::filter(object::entries($before ?? {}), |$entry| $entry[0]!='canonical_read_nonce');
    LET $semantic_after = array::filter(object::entries($after ?? {}), |$entry| $entry[0]!='canonical_read_nonce');
    IF $event!='UPDATE' OR $semantic_before!=$semantic_after {
        LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
        IF $space!=NONE {
            UPSERT type::thing('canonical_epoch',$space) SET space_id=$space,epoch=<string>rand::uuid();
        };
        IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
            UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid();
        };
    };
};

DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_head
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    LET $semantic_before = array::filter(object::entries($before ?? {}), |$entry| $entry[0] NOT IN ['fence_nonce','canonical_read_nonce']);
    LET $semantic_after = array::filter(object::entries($after ?? {}), |$entry| $entry[0] NOT IN ['fence_nonce','canonical_read_nonce']);
    IF $event!='UPDATE' OR $semantic_before!=$semantic_after {
        LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
        IF $space!=NONE {
            UPSERT type::thing('canonical_epoch',$space) SET space_id=$space,epoch=<string>rand::uuid();
        };
        IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
            UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid();
        };
    };
};
