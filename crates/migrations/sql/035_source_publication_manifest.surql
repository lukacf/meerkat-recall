-- Closed whole-source expectations and immutable evidence/source mappings.
-- Payload is a canonical JSON string; native/flatten decoding is not used.
DEFINE TABLE IF NOT EXISTS source_publication_manifest SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON source_publication_manifest TYPE string;
DEFINE FIELD IF NOT EXISTS source_id ON source_publication_manifest TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON source_publication_manifest TYPE string;
DEFINE INDEX IF NOT EXISTS source_manifest_by_source ON source_publication_manifest FIELDS space_id, source_id;

DEFINE TABLE IF NOT EXISTS source_publication_completion SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON source_publication_completion TYPE string;
DEFINE FIELD IF NOT EXISTS source_id ON source_publication_completion TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON source_publication_completion TYPE string;
DEFINE INDEX IF NOT EXISTS source_completion_by_source ON source_publication_completion FIELDS space_id, source_id;

DEFINE TABLE IF NOT EXISTS source_document_mapping SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON source_document_mapping TYPE string;
DEFINE FIELD IF NOT EXISTS source_id ON source_document_mapping TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON source_document_mapping TYPE string;
DEFINE INDEX IF NOT EXISTS source_document_mapping_by_source ON source_document_mapping FIELDS space_id, source_id;

DEFINE TABLE IF NOT EXISTS source_revision_mapping SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON source_revision_mapping TYPE string;
DEFINE FIELD IF NOT EXISTS source_id ON source_revision_mapping TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON source_revision_mapping TYPE string;
DEFINE INDEX IF NOT EXISTS source_revision_mapping_by_source ON source_revision_mapping FIELDS space_id, source_id;

-- Every control mutation participates in the canonical predicate fence.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_publication_manifest
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};

-- Every control mutation participates in the canonical predicate fence.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_publication_completion
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};

-- Every control mutation participates in the canonical predicate fence.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_document_mapping
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};

-- Every control mutation participates in the canonical predicate fence.
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_revision_mapping
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};

-- Acknowledgement of a staged bundle by the original commit-stage attempt.
-- Queue hot/archive records remain completion authority.
DEFINE TABLE IF NOT EXISTS source_bundle_registration SCHEMAFULL;
DEFINE FIELD IF NOT EXISTS space_id ON source_bundle_registration TYPE string;
DEFINE FIELD IF NOT EXISTS source_id ON source_bundle_registration TYPE string;
DEFINE FIELD IF NOT EXISTS payload ON source_bundle_registration TYPE string;
DEFINE FIELD IF NOT EXISTS state ON source_bundle_registration FLEXIBLE TYPE object;
DEFINE INDEX IF NOT EXISTS source_bundle_registration_by_source ON source_bundle_registration FIELDS space_id, source_id;
DEFINE EVENT OVERWRITE canonical_predicate_fence ON TABLE source_bundle_registration
WHEN $event IN ["CREATE", "UPDATE", "DELETE"] THEN {
    LET $space = IF $event = "DELETE" THEN $before.space_id ELSE $after.space_id END;
    IF $space != NONE { UPSERT type::thing("canonical_epoch", $space) SET space_id = $space, epoch = <string>rand::uuid(); };
};
