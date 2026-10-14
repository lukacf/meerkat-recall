-- Source eligibility changes invalidate the existing staged index coverage.
-- A source-head transaction fence nonce alone changes no published input.

DEFINE EVENT OVERWRITE source_visibility_index_generation ON TABLE source_head
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND ($event != 'UPDATE' OR $before.space_id != $after.space_id OR $before.payload != $after.payload OR $before.state != $after.state OR $before.payload = NONE) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$space AND deleted_at IS NONE AND processing IS NOT NONE;
};

DEFINE EVENT OVERWRITE source_visibility_index_generation ON TABLE source_effect_support
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND ($event != 'UPDATE' OR $before.space_id != $after.space_id OR $before.payload != $after.payload OR $before.state != $after.state OR $before.payload = NONE) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$space AND deleted_at IS NONE AND processing IS NOT NONE;
};

DEFINE EVENT OVERWRITE source_visibility_index_generation ON TABLE source_revision_mapping
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND ($event != 'UPDATE' OR $before.space_id != $after.space_id OR $before.payload != $after.payload OR $before.state != $after.state OR $before.payload = NONE) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$space AND deleted_at IS NONE AND processing IS NOT NONE;
};

DEFINE EVENT OVERWRITE source_visibility_index_generation ON TABLE source_publication_completion
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND ($event != 'UPDATE' OR $before.space_id != $after.space_id OR $before.payload != $after.payload OR $before.state != $after.state OR $before.payload = NONE) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$space AND deleted_at IS NONE AND processing IS NOT NONE;
};

DEFINE EVENT OVERWRITE source_visibility_index_generation ON TABLE source_publication_manifest
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND ($event != 'UPDATE' OR $before.space_id != $after.space_id OR $before.payload != $after.payload OR $before.state != $after.state OR $before.payload = NONE) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$space AND deleted_at IS NONE AND processing IS NOT NONE;
};

DEFINE EVENT OVERWRITE source_visibility_index_generation ON TABLE source_publication_manifest_history
WHEN $event IN ['CREATE','UPDATE','DELETE'] AND ($event != 'UPDATE' OR $before.space_id != $after.space_id OR $before.payload != $after.payload OR $before.state != $after.state OR $before.payload = NONE) THEN {
    LET $space = IF $event='DELETE' THEN $before.space_id ELSE $after.space_id END;
    IF $event='UPDATE' AND $before.space_id!=$after.space_id AND $before.space_id!=NONE {
        UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$before.space_id AND deleted_at IS NONE AND processing IS NOT NONE;
    };
    UPDATE space_config SET processing.index_generation+=1,updated_at=time::now() WHERE space_id=$space AND deleted_at IS NONE AND processing IS NOT NONE;
};
