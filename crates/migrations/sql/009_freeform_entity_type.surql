-- Remove fixed entity type constraint to allow freeform strings.
-- Entity types are now defined by extraction skills, not a hardcoded enum.
REMOVE FIELD type ON entity;
DEFINE FIELD type ON TABLE entity TYPE string;
