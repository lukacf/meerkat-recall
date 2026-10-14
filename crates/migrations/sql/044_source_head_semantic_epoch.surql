-- Worker begin/failure transitions write a private nonce on the original head
-- to participate in native row conflicts. They change no publication input.
-- Keep that write and the physical/reference fence events; only semantic head
-- changes invalidate unrelated canonical preparation in the same space.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_head
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    LET $head_before = array::filter(object::entries($before ?? {}), |$entry| $entry[0]!='fence_nonce');
    LET $head_after = array::filter(object::entries($after ?? {}), |$entry| $entry[0]!='fence_nonce');
    IF $event!='UPDATE' OR $head_before!=$head_after {
        LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
        IF $space!=NONE {
            UPSERT type::thing('canonical_epoch',$space) SET space_id=$space,epoch=<string>rand::uuid();
        };
        IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
            UPSERT type::thing('canonical_epoch',$before.space_id) SET space_id=$before.space_id,epoch=<string>rand::uuid();
        };
    };
};
