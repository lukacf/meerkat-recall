"""Offline controls for source attribution; no database/schema acceptance."""
import importlib.util
from pathlib import Path
import unittest

PATH = Path(__file__).resolve().parents[2] / "scripts/qualification/semantic_schema_map.py"
SPEC = importlib.util.spec_from_file_location("semantic_schema_map", PATH)
mapper = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(mapper)


class SourceMapControls(unittest.TestCase):
    def test_nested_blocks_strings_regex_and_comments_preserve_boundaries(self):
        source = """-- DEFINE TABLE counterfeit;
DEFINE EVENT guard ON TABLE doc WHEN true THEN {
  IF $value = 'x;--not comment' { THROW 'no'; };
  RETURN string::matches($value, /^[a-z]{3};$/);
};
DEFINE FIELD body ON TABLE doc TYPE string;
"""
        statements = list(mapper.statements(source))
        self.assertEqual(len(statements), 2)
        self.assertEqual(mapper.identity(statements[0][2]), ("doc", "events", "guard"))
        self.assertIn("'x;--not comment'", statements[0][2])
        self.assertIn("/^[a-z]{3};$/", statements[0][2])

    def test_same_identity_different_predicate_remains_different(self):
        source = "DEFINE FIELD count ON doc TYPE int ASSERT $value = 3;"
        changed = source.replace("= 3", "= 4")
        self.assertEqual(mapper.identity(source), mapper.identity(changed))
        self.assertNotEqual(mapper.lexical(source), mapper.lexical(changed))

    def test_literal_case_and_payload_not_normalized(self):
        self.assertNotEqual(mapper.lexical("THROW 'Secret'"), mapper.lexical("THROW 'secret'"))
        self.assertNotEqual(mapper.lexical("THROW 'a b'"), mapper.lexical("THROW 'ab'"))

    def test_unknown_category_not_attributed(self):
        self.assertIsNone(mapper.identity("DEFINE USER unknown ON ROOT PASSWORD 'x';"))

    def test_nested_array_identity_and_function_identity(self):
        self.assertEqual(mapper.identity("DEFINE FIELD a[*][*] ON TABLE doc TYPE int;"),
                         ("doc", "fields", "a[*][*]"))
        self.assertEqual(mapper.identity("DEFINE FUNCTION OVERWRITE fn::guard($a: object) { RETURN true; };"),
                         (None, "functions", "guard"))

    def test_unclosed_block_refuses(self):
        with self.assertRaises(ValueError):
            list(mapper.statements("DEFINE EVENT a ON b WHEN true THEN { RETURN true;"))

    def test_formatter_quoted_reserved_field_has_same_source_identity(self):
        self.assertEqual(mapper.identity("DEFINE FIELD `value` ON attribute TYPE object;"),
                         mapper.identity("DEFINE FIELD value ON TABLE attribute TYPE object;"))

    def test_field_properties_keep_assertion_and_literal_values(self):
        source = "DEFINE FIELD status ON TABLE entity TYPE string ASSERT $value INSIDE ['active'] DEFAULT 'active';"
        rendered = "DEFINE FIELD status ON entity TYPE string DEFAULT 'active' ASSERT $value INSIDE ['active'] PERMISSIONS FULL"
        self.assertEqual(mapper.field_properties(source), mapper.field_properties(rendered))
        self.assertNotEqual(mapper.field_properties(source), mapper.field_properties(rendered.replace("['active']", "['merged']")))

    def test_table_permissions_change_remains_visible(self):
        definition = {"category": "tables", "name": "doc", "origin_category": "explicit-migration-history",
                      "statement": "DEFINE TABLE doc TYPE ANY SCHEMALESS PERMISSIONS FULL"}
        origins = [{"operation": "DEFINE", "statement": "DEFINE TABLE doc SCHEMALESS;"}]
        self.assertFalse(mapper.body_review_hint(definition, origins)["matched"])

    def test_identifier_case_is_not_folded_in_body_review(self):
        self.assertNotEqual(mapper.field_properties("DEFINE FIELD a ON t TYPE string ASSERT $value = metadata.x;"),
                            mapper.field_properties("DEFINE FIELD a ON t TYPE string ASSERT $value = Metadata.x;"))

    def test_single_call_formatter_does_not_erase_additional_statement(self):
        source = "DEFINE EVENT e ON t WHEN true THEN { fn::guard($after, false); };"
        canonical = "DEFINE EVENT e ON t WHEN true THEN { fn::guard($after, false) }"
        self.assertEqual(mapper.block_review_tokens(source), mapper.block_review_tokens(canonical))
        mutated = canonical.replace("false)", "true)")
        self.assertNotEqual(mapper.block_review_tokens(source), mapper.block_review_tokens(mutated))
        injected = canonical.replace("false)", "false); fn::unknown()")
        self.assertNotEqual(mapper.block_review_tokens(source), mapper.block_review_tokens(injected))

    def test_default_closure_any_does_not_accept_explicit_changed_type(self):
        omitted = "RETURN array::map($items, |$item| $item);"
        explicit = "RETURN array::map($items, |$item: any| $item);"
        self.assertEqual(mapper.block_review_tokens(omitted), mapper.block_review_tokens(explicit))
        self.assertNotEqual(mapper.block_review_tokens(omitted), mapper.block_review_tokens(explicit.replace(": any", ": int")))


if __name__ == "__main__":
    unittest.main()
