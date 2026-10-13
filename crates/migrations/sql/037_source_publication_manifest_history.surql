-- Immutable superseded expectations for unpublished configuration replans.
-- The existing source_publication_manifest row remains the sole active plan.
DEFINE TABLE IF NOT EXISTS source_publication_manifest_history SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON source_publication_manifest_history TYPE string;
DEFINE FIELD IF NOT EXISTS source_id ON source_publication_manifest_history TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON source_publication_manifest_history TYPE string;
DEFINE FIELD IF NOT EXISTS state ON source_publication_manifest_history FLEXIBLE TYPE object;
DEFINE INDEX IF NOT EXISTS source_manifest_history_by_source ON source_publication_manifest_history FIELDS space_id, source_id;
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_publication_manifest_history
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
    IF $event = "UPDATE" AND $before.space_id != $after.space_id AND $before.space_id != NONE {
        UPSERT type::thing("canonical_epoch", $before.space_id) SET space_id = $before.space_id, epoch = <string>rand::uuid();
    };
};
DEFINE EVENT OVERWRITE erased_reference_fence ON TABLE source_publication_manifest_history
WHEN $event IN ["CREATE", "UPDATE"] THEN {
    LET $matches = SELECT id FROM source_tombstone_ref WHERE space_id = $after.space_id
        AND (string::contains(<string>$after, token) OR string::contains(<string>$after, string::replace(token, '-', '')));
    IF array::len($matches) > 0 { THROW 'publication:source_erased'; };
};
