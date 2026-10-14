"""Independent replay predicate controls; synthetic rows, never product evidence."""
import copy
import unittest

import installed_evidence_validator as validator


class InstalledMcpReplayTests(unittest.TestCase):
    def setUp(self):
        self.arguments = {"query": "original known query", "ranking_mode": "lexical", "limit": 1}
        base = {"method": "POST", "path": "/mcp/full", "credential_role": None,
                "space": "home", "status": 200}
        self.discovery = {**base, "index": 17, "request": {"jsonrpc": "2.0", "id": 17,
            "method": "tools/list", "params": {}}, "body": {"tools": [{"name": "get_runtime_status",
            "description": "Current runtime status", "inputSchema": {}}]}}
        self.denied = {**base, "index": 18, "request": {"jsonrpc": "2.0", "id": 18,
            "method": "tools/call", "params": {"name": "search_scoped", "arguments": self.arguments}},
            "body": {"isError": True, "content": [{"type": "text", "text": validator.encode(
                {"error_code": "unauthorized", "message": "Authentication required", "retryable": False}).decode()}]}}

    def test_original_public_discovery_and_data_denial_pass(self):
        validator.mcp_anonymous_case([self.discovery, self.denied], "home", "original known query", ["PRIVATE"])

    def test_discovery_alone_or_wrong_original_request_fails(self):
        with self.assertRaises(ValueError):
            validator.mcp_anonymous_case([self.discovery], "home", "original known query", [])
        altered = copy.deepcopy(self.denied)
        altered["request"]["params"]["arguments"]["query"] = "different query"
        with self.assertRaises(ValueError):
            validator.mcp_anonymous_case([self.discovery, altered], "home", "original known query", [])

    def test_anonymous_tool_or_source_disclosure_fails(self):
        for change in ["extra-tool", "description-canary"]:
            row = copy.deepcopy(self.discovery)
            if change == "extra-tool":
                row["body"]["tools"].append({"name": "search_scoped", "description": "Search", "inputSchema": {}})
            else:
                row["body"]["tools"][0]["description"] = "PRIVATE"
            with self.subTest(change=change), self.assertRaises(ValueError):
                validator.mcp_anonymous_case([row, self.denied], "home", "original known query", ["PRIVATE"])

    def test_http_error_or_tool_success_does_not_replace_typed_denial(self):
        for change in ["http403", "tool-success", "extra-result", "extra-block"]:
            row = copy.deepcopy(self.denied)
            if change == "http403": row["status"] = 403
            elif change == "tool-success": row["body"]["isError"] = False
            elif change == "extra-result": row["body"]["data"] = []
            else: row["body"]["content"].append({"type": "text", "text": "extra"})
            with self.subTest(change=change), self.assertRaises(ValueError):
                validator.mcp_data_denial(row, "unauthorized", [], self.arguments)

    def test_wrong_code_retryability_or_leak_fails(self):
        for payload in [{"error_code": "forbidden_space", "message": "Denied", "retryable": False},
                        {"error_code": "unauthorized", "message": "Denied", "retryable": True},
                        {"error_code": "unauthorized", "message": "PRIVATE", "retryable": False}]:
            row = copy.deepcopy(self.denied)
            row["body"]["content"][0]["text"] = validator.encode(payload).decode()
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                validator.mcp_data_denial(row, "unauthorized", ["PRIVATE"], self.arguments)

    def test_foreign_source_original_revision_is_required(self):
        row = copy.deepcopy(self.denied)
        args = {"query": "business query", "ranking_mode": "lexical", "source_revision_constraints": {"source": "r1"}}
        row["request"]["params"]["arguments"] = copy.deepcopy(args)
        row["body"]["content"][0]["text"] = validator.encode(
            {"error_code": "forbidden_space", "message": "Denied", "retryable": False}).decode()
        validator.mcp_data_denial(row, "forbidden_space", [], args)
        row["request"]["params"]["arguments"]["source_revision_constraints"]["source"] = "r2"
        with self.assertRaises(ValueError): validator.mcp_data_denial(row, "forbidden_space", [], args)

    def test_rpc_id_boolean_extra_payload_or_envelope_error_fails(self):
        base = {"jsonrpc": "2.0", "id": 18, "result": self.denied["body"]}
        self.assertEqual(validator.rpc_body(validator.encode(base), self.denied["request"]), self.denied["body"])
        for change in [{"id": 19}, {"id": True}, {"unexpected": "payload"}, {"error": {"code": -32600}}]:
            with self.subTest(change=change), self.assertRaises(ValueError):
                validator.rpc_body(validator.encode({**base, **change}), self.denied["request"])


if __name__ == "__main__":
    unittest.main()
