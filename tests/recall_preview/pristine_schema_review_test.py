"""Closed-world negative controls for independent source-to-schema comparison."""
import copy
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "independent_pristine_review", ROOT / "scripts/qualification/pristine_schema_review.py")
REVIEW = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(REVIEW)


def field(statement, actual):
    return {"category": "fields", "origin_category": "explicit-migration-history",
            "statement": actual, "source_history": [{"operation": "DEFINE", "statement": statement}]}


class SourceBodyControls(unittest.TestCase):
    def test_field_defaults_and_clause_reordering_preserve_exact_values(self):
        record = field("DEFINE FIELD generation ON source_head TYPE int ASSERT $value >= 0 DEFAULT 0;",
                       "DEFINE FIELD generation ON source_head TYPE int DEFAULT 0 ASSERT $value >= 0 PERMISSIONS FULL")
        REVIEW.compare_body(record)
        for before, after in ((">= 0", ">= 1"), ("DEFAULT 0", "DEFAULT 1"),
                              ("$value", "$Value"), ("PERMISSIONS FULL", "PERMISSIONS NONE")):
            changed = copy.deepcopy(record)
            changed["statement"] = changed["statement"].replace(before, after)
            with self.assertRaises(ValueError):
                REVIEW.compare_body(changed)

    def test_string_quote_display_does_not_fold_literal_contents(self):
        record = field("DEFINE FIELD label ON source TYPE string DEFAULT 'Original';",
                       'DEFINE FIELD label ON source TYPE string DEFAULT "Original" PERMISSIONS FULL')
        REVIEW.compare_body(record)
        record["statement"] = record["statement"].replace("Original", "original")
        with self.assertRaises(ValueError):
            REVIEW.compare_body(record)

    def test_event_formatter_does_not_remove_extra_statement_or_guard(self):
        original = "DEFINE EVENT fence ON source WHEN $event != 'DELETE' THEN { fn::guard($after, true); };"
        record = {"category": "events", "origin_category": "explicit-migration-history",
                  "source_history": [{"operation": "DEFINE", "statement": original}],
                  "statement": "DEFINE EVENT fence ON source WHEN $event != 'DELETE' THEN { fn::guard($after, true) }"}
        REVIEW.compare_body(record)
        for changed in (record["statement"].replace("true", "false"),
                        record["statement"].replace(") }", "); RETURN true; }"),
                        record["statement"].replace("$after", "$After")):
            with self.assertRaises(ValueError):
                REVIEW.compare_body({**record, "statement": changed})

    def test_explicit_closure_type_is_not_erased(self):
        original = "DEFINE FUNCTION fn::f() { RETURN array::map([], |$x| $x); };"
        record = {"category": "functions", "origin_category": "explicit-migration-history",
                  "source_history": [{"operation": "DEFINE", "statement": original}],
                  "statement": "DEFINE FUNCTION fn::f() { RETURN array::map([], |$x: any| $x); } PERMISSIONS FULL"}
        REVIEW.compare_body(record)
        record["statement"] = record["statement"].replace("$x: any", "$x: int")
        with self.assertRaises(ValueError):
            REVIEW.compare_body(record)

    def test_table_permission_does_not_default_over_explicit_change(self):
        record = {"category": "tables", "name": "source", "origin_category": "explicit-migration-history",
                  "source_history": [{"operation": "DEFINE", "statement": "DEFINE TABLE source SCHEMALESS;"}],
                  "statement": "DEFINE TABLE source TYPE ANY SCHEMALESS PERMISSIONS FULL"}
        with self.assertRaises(ValueError):
            REVIEW.compare_body(record)

    def test_array_child_preserves_first_typed_flexible_definition(self):
        record = {"category": "fields", "origin_category": "engine-array-subfield",
                  "source_history": [
                      {"operation": "DEFINE", "statement": "DEFINE FIELD evidence ON rel TYPE array;"},
                      {"operation": "DEFINE", "statement": "DEFINE FIELD evidence ON rel FLEXIBLE TYPE array<object>;"},
                      {"operation": "DEFINE", "statement": "DEFINE FIELD evidence ON rel TYPE array<object>;"}],
                  "statement": "DEFINE FIELD evidence[*] ON rel FLEXIBLE TYPE object PERMISSIONS FULL"}
        REVIEW.compare_body(record)
        record["statement"] = record["statement"].replace(" FLEXIBLE", "")
        with self.assertRaises(ValueError):
            REVIEW.compare_body(record)


if __name__ == "__main__":
    unittest.main()
