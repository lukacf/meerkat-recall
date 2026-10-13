"""Synthetic wire refusal controls; no transport, engine, or credentials."""
import copy
import json
import unittest
from unittest.mock import patch

import native_private_membership as api


def row(space="space-a", token="1" * 64, key="e360a4bf-fe85-5ff8-9871-3f7dcaa1851f"):
    return {"row": {"id": "source_erasure_membership:`" + key + "`",
                    "space_id": space, "token": token}, "keys": ["id", "space_id", "token"],
            "id_is_record": True, "id_is_string": True, "id_inner": key,
            "space_is_string": True, "token_is_string": True}


def wire(rows=None):
    rows = [row()] if rows is None else rows
    # Exact nine keys emitted by pinned native InfoStatement::Db(false).
    # This fixture must not mirror the decoder's category set.
    database = {name: {} for name in ("accesses", "analyzers", "apis", "configs", "functions",
                                     "models", "params", "tables", "users")}
    database["tables"] = {api.TABLE: api.TABLE_DEFINITION, "ordinary": "ordinary schema"}
    counted = [{"count": len(rows)}] if rows else []
    return [{"time": "1ms", "status": "OK", "result": result} for result in
            (database, copy.deepcopy(api.TABLE_SCHEMA), counted, rows, copy.deepcopy(counted))]


def raw(value):
    return json.dumps(value, ensure_ascii=True).encode()


class PrivateMembership(unittest.TestCase):
    def test_complete_native_rows_and_empty_original_inventory(self):
        evidence = api.validate(raw(wire()), {"space-a": {"1" * 64}}, "ERASED-CANARY")
        self.assertEqual(evidence["rows"], [row()])
        self.assertEqual(evidence["space_counts"], {"space-a": 1})
        self.assertFalse(evidence["product_acceptance"])
        self.assertEqual(api.validate(raw(wire([])), {}, "ERASED-CANARY")["row_count"], 0)

    def test_utf8_space_uses_original_byte_length_native_string_id(self):
        value = row("å-space", "3" * 64, "f4afdcd3-dfbd-51af-956c-8249c1819a80")
        result = api.validate(raw(wire([value])), {"å-space": {"3" * 64}}, "ERASED-CANARY")
        self.assertEqual(result["rows"], [value])

    def test_fullrow_extra_payload_and_native_hidden_field_fail(self):
        for mutation in ("payload", "hidden"):
            with self.subTest(mutation=mutation):
                value = wire()
                if mutation == "payload":
                    value[3]["result"][0]["row"]["text"] = "unrelated retained text"
                else:
                    value[3]["result"][0]["keys"].append("omitted_native_none")
                with self.assertRaises(api.Violation):
                    api.validate(raw(value), {"space-a": {"1" * 64}}, "ERASED-CANARY")

    def test_native_record_and_field_types_cannot_hide_in_json_strings(self):
        for field in ("id_is_record", "id_is_string", "space_is_string", "token_is_string"):
            for invalid in (False, 1, "true"):
                with self.subTest(field=field, invalid=invalid):
                    value = wire()
                    value[3]["result"][0][field] = invalid
                    with self.assertRaises(api.Violation):
                        api.validate(raw(value), {"space-a": {"1" * 64}}, "ERASED-CANARY")

    def test_native_id_and_original_token_inventory_cannot_be_substituted(self):
        for mutation in ("id", "inner", "space", "token"):
            with self.subTest(mutation=mutation):
                value = wire()
                item = value[3]["result"][0]
                if mutation == "inner":
                    item["id_inner"] = "other"
                else:
                    field = "space_id" if mutation == "space" else mutation
                    item["row"][field] = "2" * 64 if field == "token" else "other"
                with self.assertRaises(api.Violation):
                    api.validate(raw(value), {"space-a": {"1" * 64}}, "ERASED-CANARY")
        with self.assertRaises(api.Violation):
            api.validate(raw(wire()), {"space-a": {"2" * 64}}, "ERASED-CANARY")
        with self.assertRaises(api.Violation):
            api.validate(raw(wire([])), {"space-a": {"1" * 64}}, "ERASED-CANARY")

    def test_duplicate_membership_and_canary_are_positive_failures(self):
        with self.assertRaises(api.Violation):
            api.validate(raw(wire([row(), row()])), {"space-a": {"1" * 64}}, "ERASED-CANARY")
        with self.assertRaises(api.Violation):
            api.validate(raw(wire()), {"space-a": {"1" * 64}}, "space-a")
        for index in (0, 1, 3):
            value = wire()
            value[index]["result"] = {"unknown": {"nested": ["ERASED-CANARY"]}}
            value[index]["status"] = "ERR"
            with self.assertRaises(api.Violation):
                api.validate(raw(value), {"space-a": {"1" * 64}}, "ERASED-CANARY")
            with self.assertRaises(api.Violation):
                api.validate(raw(value), None, "ERASED-CANARY")

    def test_counts_results_and_errors_never_accept_truncated_capture(self):
        values = []
        for index in (2, 4):
            value = wire()
            value[index]["result"] = [{"count": 2}]
            values.append(value)
        values.append(wire()[:-1])
        for result in (True, -1, 100_001, "1"):
            value = wire()
            value[2]["result"] = [{"count": result}]
            values.append(value)
        value = wire()
        value[3]["status"] = "ERR"
        value[3]["result"] = "secret-like error must not appear in diagnostic"
        values.append(value)
        for value in values:
            with self.subTest(value=values.index(value)), self.assertRaises(api.Incomplete) as error:
                api.validate(raw(value), {"space-a": {"1" * 64}}, "ERASED-CANARY")
            self.assertNotIn("secret-like", str(error.exception))

    def test_schema_permissions_extra_fields_indexes_and_secret_schema_refuse(self):
        values = []
        value = wire()
        del value[0]["result"]["apis"]
        values.append(value)
        value = wire()
        value[0]["result"]["tables"][api.TABLE] = api.TABLE_DEFINITION.replace("NONE", "FULL")
        values.append(value)
        for category in ("fields", "events", "indexes", "lives", "tables"):
            value = wire()
            value[1]["result"][category]["unexpected"] = "unreviewed declaration"
            values.append(value)
        for category in ("users", "accesses", "params"):
            value = wire()
            value[0]["result"][category]["unexpected"] = "private schema"
            values.append(value)
        for value in values:
            with self.assertRaises(api.Incomplete):
                api.validate(raw(value), {"space-a": {"1" * 64}}, "ERASED-CANARY")

    def test_duplicate_nonfinite_deep_malformed_and_bounded_json_refuse(self):
        for value in (b'[{"time":"1s","time":"2s"}]', b'[1e999]', b'[NaN]',
                      b'[' * 1500 + b']' * 1500, b'\xff', raw(wire())[:-1]):
            with self.assertRaises(api.Incomplete):
                api.validate(value, {}, "ERASED-CANARY")
        value = raw(wire())
        with patch.object(api, "RAW_CAP", len(value) - 1), self.assertRaises(api.Incomplete):
            api.validate(value, {"space-a": {"1" * 64}}, "ERASED-CANARY")
        with patch.object(api, "ENTRY_CAP", 1), self.assertRaises(api.Incomplete):
            api.validate(raw(wire([row(), row()])), {"space-a": {"1" * 64}}, "ERASED-CANARY")


if __name__ == "__main__":
    unittest.main()
