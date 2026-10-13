-- Explicit durable standing delegation for recurring source watches. No token,
-- source/document content, or inferred legacy authority is stored here.
DEFINE TABLE IF NOT EXISTS watch_delegation SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON watch_delegation TYPE string;
DEFINE FIELD IF NOT EXISTS watch_id ON watch_delegation TYPE record<source_watch>;
DEFINE FIELD IF NOT EXISTS source_id ON watch_delegation TYPE record<source>;
DEFINE FIELD IF NOT EXISTS payload ON watch_delegation TYPE string;
DEFINE FIELD IF NOT EXISTS digest ON watch_delegation TYPE string;
DEFINE FIELD IF NOT EXISTS read_nonce ON watch_delegation TYPE option<string>;
DEFINE INDEX IF NOT EXISTS watch_delegation_identity ON watch_delegation FIELDS space_id,watch_id UNIQUE;
DEFINE INDEX IF NOT EXISTS watch_delegation_source ON watch_delegation FIELDS space_id,source_id;

-- Explicit replacement/removal invalidates in-flight canonical observations.
-- A no-change MVCC touch must not invalidate its own transaction's epoch.
DEFINE EVENT OVERWRITE watch_delegation_epoch ON TABLE watch_delegation
WHEN $event IN ["CREATE", "DELETE"] OR ($event = "UPDATE" AND
    ($before.space_id != $after.space_id OR $before.watch_id != $after.watch_id OR
     $before.source_id != $after.source_id OR $before.payload != $after.payload OR
     $before.digest != $after.digest)) THEN {
    LET $space = IF $event = "DELETE" { $before.space_id } ELSE { $after.space_id };
    UPSERT type::thing('canonical_epoch', $space) SET space_id=$space,epoch=<string>rand::uuid();
};

-- Every late private write remains subject to physical source erasure and the
-- closed reference inventory, including source/space replacement and deletion.
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE watch_delegation
WHEN $event IN ['CREATE','UPDATE','DELETE'] THEN {
    IF $event='DELETE' { fn::recall_reference_guard($before,false); } ELSE {
        fn::recall_reference_guard($after,true);
        IF $event='UPDATE' AND $before.space_id!=$after.space_id { fn::recall_reference_guard($before,false); };
    };
};
