"""Pure controls for actual qualification HTTP/MCP methods, not product auth proof.

Loads only selected source definitions. Controlled in-memory HTTP responses do
not start a server, initialize a driver, import product modules or contact a model.
"""
import argparse
import ast
import base64
import contextlib
from email.message import Message
import hashlib
import http.client
import json
import math
from pathlib import Path
import time
import types
import unittest
from unittest.mock import Mock, patch

SOURCE_ROOT = Path(__file__).resolve().parents[2]
DRIVER = None
DISPATCH = None
SECRET = "private-synthetic-wire-token"
SOURCE_CANARY = "private-source-canary-never-returned"
HOME_QUERY = "Original household qualification query"
BUSINESS_QUERY = "Original business qualification query"


def selected_functions(path, names):
    tree = ast.parse(path.read_bytes(), filename=str(path))
    nodes = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in nodes} != set(names):
        raise ValueError("selected actual function missing")
    return nodes


def selected_class(path, name, methods, bases=()):
    tree = ast.parse(path.read_bytes(), filename=str(path))
    original = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)
    selected = [node for node in original.body if isinstance(node, ast.FunctionDef) and node.name in methods]
    if {node.name for node in selected} != set(methods):
        raise ValueError("selected actual method missing")
    return ast.ClassDef(name=name, bases=[ast.Name(id=base, ctx=ast.Load()) for base in bases],
                        keywords=[], body=selected, decorator_list=[])


def load_actual_methods():
    parser_path = SOURCE_ROOT / "scripts/qualification/recall_preview.py"
    parser_scope = {"json": json, "math": math}
    exec(compile(ast.Module(body=selected_functions(parser_path, {"strict_json_body"}),
                            type_ignores=[]), str(parser_path), "exec"), parser_scope)
    driver_path = DRIVER or SOURCE_ROOT / "scripts/qualification/installed_linux_offline.py"
    nodes = selected_functions(driver_path, {"require", "digest", "encoded", "mcp_value", "typed_denial",
        "mcp_response_result", "mcp_typed_denial", "mcp_public_status_only"})
    nodes.append(selected_class(driver_path, "Driver", {"http", "denied_tool", "check_mcp_auth"}))
    fixtures = types.SimpleNamespace(corpus=lambda: {
        "sources": [{"text": SOURCE_CANARY}],
        "queries": [{"id": "home-service", "query": HOME_QUERY},
                    {"id": "business-review", "query": BUSINESS_QUERY}]})
    scope = {"base64": base64, "hashlib": hashlib, "http": http, "json": json, "time": time,
             "fixtures": fixtures,
             "capture": types.SimpleNamespace(strict_json_body=parser_scope["strict_json_body"],
                                               total_deadline=lambda _seconds: contextlib.nullcontext())}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])),
                 str(driver_path), "exec"), scope)
    dispatch_path = DISPATCH or SOURCE_ROOT / "scripts/qualification/installed_linux_dispatch.py"
    dispatch_scope = {"Driver": scope["Driver"], "core": types.SimpleNamespace(**scope)}
    dispatch = selected_class(dispatch_path, "DispatchDriver", {"query"}, ("Driver",))
    exec(compile(ast.fix_missing_locations(ast.Module(body=[dispatch], type_ignores=[])),
                 str(dispatch_path), "exec"), dispatch_scope)
    return scope["Driver"], dispatch_scope["DispatchDriver"]


def denial(code="forbidden_space", **changes):
    body = {"error_code": code, "message": "The selected authority is not granted", "retryable": False}
    body.update(changes)
    return {"content": [{"type": "text", "text": json.dumps(body)}], "isError": True}


def discovery():
    return {"tools": [{"name": "get_runtime_status", "description": "Get runtime readiness.", "inputSchema": {}}]}


class InstalledMcpAuthWire(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver_type, cls.dispatch_type = load_actual_methods()

    def owner(self, *, dispatch=False):
        owner = (self.dispatch_type if dispatch else self.driver_type).__new__(
            self.dispatch_type if dispatch else self.driver_type)
        owner.requests = 20
        owner.started = time.monotonic()
        owner.port = 1
        owner.tokens = {"household": SECRET}
        owner.cases = {"spaces": ["household", "business"], "queries": ["home-service", "business-review"],
                       "limits": {"http_calls": 128, "http_seconds": 5, "total_seconds": 300,
                                  "response_bytes": 262144}}
        owner.remaining = lambda: 30
        owner.saved = []
        owner.save = lambda name, value: owner.saved.append((name, json.loads(value)))
        owner.records = []
        owner.record = owner.records.append
        return owner

    def wire(self, owner, responses, action):
        connections = []
        for status, body in responses:
            raw = body if isinstance(body, bytes) else b"data: " + json.dumps(body).encode() + b"\n\n"
            headers = Message()
            headers["Content-Length"] = str(len(raw))
            response = Mock(status=status, headers=headers)
            response.read1.side_effect = [raw, b""] if raw else [b""]
            connection = Mock()
            connection.getresponse.return_value = response
            connections.append(connection)
        with patch.object(http.client, "HTTPConnection", side_effect=connections):
            try:
                value, error = action(), None
            except ValueError as caught:
                value, error = None, caught
        for connection in connections[:len(owner.saved)]:
            connection.request.assert_called_once()
            connection.close.assert_called_once()
        self.assertEqual(len(owner.saved), len(connections))
        return value, error, connections

    def reply(self, result, request_id=21):
        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def denied(self, owner):
        return lambda: owner.denied_tool("search_entities", {
            "query": BUSINESS_QUERY, "use_vector": True, "limit": 1},
            token="household", space="business", code="forbidden_space")

    def test_actual_http_and_denial_preserve_rpc_id_token_space_and_query(self):
        owner = self.owner()
        value, error, connections = self.wire(owner, [(200, self.reply(denial()))], self.denied(owner))
        self.assertIsNone(error)
        self.assertEqual(value["error_code"], "forbidden_space")
        request = connections[0].request.call_args
        self.assertEqual(request.args[:2], ("POST", "/mcp/full"))
        self.assertEqual(request.args[3]["Authorization"], "Bearer " + SECRET)
        self.assertEqual(request.args[3]["X-Space-Id"], "business")
        self.assertEqual(json.loads(request.args[2]), {"jsonrpc": "2.0", "id": 21,
            "method": "tools/call", "params": {"name": "search_entities",
            "arguments": {"query": BUSINESS_QUERY, "use_vector": True, "limit": 1}}})
        self.assertEqual(owner.requests, 21)
        self.assertTrue(owner.saved[0][1]["raw_body_complete"])
        self.assertEqual(owner.records, [])

    def test_success_or_non_boolean_error_never_counts_as_denial(self):
        for marker in (False, None, 1, "true", "absent"):
            with self.subTest(marker=marker):
                result = denial()
                if marker == "absent":
                    result.pop("isError")
                else:
                    result["isError"] = marker
                owner = self.owner()
                _, error, _ = self.wire(owner, [(200, self.reply(result))], self.denied(owner))
                self.assertIsInstance(error, ValueError)

    def test_wrong_or_ambiguous_rpc_envelope_never_counts_as_denial(self):
        variants = []
        for field, value in (("id", 22), ("id", True), ("jsonrpc", "1.0"),
                             ("error", {"code": -32000}), ("private_data", "unexpected")):
            body = self.reply(denial())
            body[field] = value
            variants.append(body)
        body = self.reply(denial())
        body.pop("result")
        variants.append(body)
        for body in variants:
            with self.subTest(body=body):
                owner = self.owner()
                _, error, _ = self.wire(owner, [(200, body)], self.denied(owner))
                self.assertIsInstance(error, ValueError)

    def test_extra_content_metadata_or_structured_data_never_counts_as_denial(self):
        variants = []
        for field in ("structuredContent", "_meta", "unexpected"):
            value = denial()
            value[field] = {"data": "unauthorized"}
            variants.append(value)
        for content in ([], denial()["content"] * 2, [{"type": "image", "data": "x"}],
                        [{**denial()["content"][0], "annotations": {}}]):
            value = denial()
            value["content"] = content
            variants.append(value)
        for result in variants:
            with self.subTest(result=result):
                owner = self.owner()
                _, error, _ = self.wire(owner, [(200, self.reply(result))], self.denied(owner))
                self.assertIsInstance(error, ValueError)

    def test_wrong_typed_classification_retry_or_private_payload_fails(self):
        variants = [denial("unauthorized"), denial(retryable=True), denial(message="x" * 513),
                    denial(message=SECRET), denial(message=SOURCE_CANARY), denial(data=[])]
        for result in variants:
            with self.subTest(kind=result["content"][0]["text"][:25]):
                owner = self.owner()
                _, error, _ = self.wire(owner, [(200, self.reply(result))], self.denied(owner))
                self.assertIsInstance(error, ValueError)
                self.assertNotIn(SECRET, str(error))
                self.assertNotIn(SOURCE_CANARY, str(error))

    def test_malformed_or_duplicate_key_inner_error_fails_strictly(self):
        for text in ('{"error_code":"forbidden_space","error_code":"unauthorized"}',
                     '{"retryable":NaN}', "not json", "[]"):
            with self.subTest(text=text):
                owner = self.owner()
                result = {"isError": True, "content": [{"type": "text", "text": text}]}
                _, error, _ = self.wire(owner, [(200, self.reply(result))], self.denied(owner))
                self.assertIsInstance(error, ValueError)

    def test_http_error_is_not_silently_accepted_as_the_tool_contract(self):
        owner = self.owner()
        raw = json.dumps({"error_code": "forbidden_space", "message": "Denied", "retryable": False}).encode()
        _, error, _ = self.wire(owner, [(403, raw)], self.denied(owner))
        self.assertIsInstance(error, ValueError)
        self.assertEqual(owner.saved[0][1]["status"], 403)

    def test_public_discovery_requires_a_separate_unauthenticated_data_denial(self):
        owner = self.owner()
        _, error, connections = self.wire(owner, [(200, self.reply(discovery())),
            (200, self.reply(denial("unauthorized"), 22))],
            lambda: owner.check_mcp_auth("household", HOME_QUERY))
        self.assertIsNone(error)
        self.assertEqual(owner.records, ["mcp_auth_denied"])
        self.assertEqual(owner.requests, 22)
        for connection in connections:
            self.assertNotIn("Authorization", connection.request.call_args.args[3])
            self.assertEqual(connection.request.call_args.args[3]["X-Space-Id"], "household")
        request = json.loads(connections[1].request.call_args.args[2])
        self.assertEqual(request["id"], 22)
        self.assertEqual(request["params"], {"name": "search_scoped", "arguments": {
            "query": HOME_QUERY, "ranking_mode": "lexical", "limit": 1}})

    def test_discovery_with_data_tool_or_extra_payload_stops_before_auth_record(self):
        variants = []
        for names in ([], ["search_scoped"], ["get_runtime_status", "search_scoped"],
                      ["get_runtime_status", "get_runtime_status"]):
            variants.append({"tools": [{**discovery()["tools"][0], "name": name} for name in names]})
        variants.extend([{**discovery(), "data": "private"}, {**discovery(), "nextCursor": "x"}])
        for result in variants:
            with self.subTest(result=result):
                owner = self.owner()
                _, error, _ = self.wire(owner, [(200, self.reply(result))],
                                       lambda: owner.check_mcp_auth("household", HOME_QUERY))
                self.assertIsInstance(error, ValueError)
                self.assertEqual(owner.records, [])
                self.assertEqual(owner.requests, 21)

    def test_public_discovery_alone_cannot_pass_when_data_call_succeeds(self):
        owner = self.owner()
        result = {"isError": False, "content": [{"type": "text", "text": '{"results":[]}'}]}
        _, error, _ = self.wire(owner, [(200, self.reply(discovery())), (200, self.reply(result, 22))],
                               lambda: owner.check_mcp_auth("household", HOME_QUERY))
        self.assertIsInstance(error, ValueError)
        self.assertEqual(owner.records, [])

    def test_discovery_descriptions_cannot_leak_token_or_source(self):
        for canary in (SECRET, SOURCE_CANARY):
            with self.subTest(kind="credential" if canary == SECRET else "source"):
                result = discovery()
                result["tools"][0]["description"] = canary
                owner = self.owner()
                _, error, _ = self.wire(owner, [(200, self.reply(result))],
                                       lambda: owner.check_mcp_auth("household", HOME_QUERY))
                self.assertIsInstance(error, ValueError)
                self.assertNotIn(canary, str(error))

    def test_dispatch_rejection_uses_exact_original_query_and_provider_code(self):
        for code, accepted in (("provider_unapproved", True), ("forbidden_space", False)):
            with self.subTest(code=code):
                owner = self.owner(dispatch=True)
                _, error, connections = self.wire(owner, [(200, self.reply(denial(code)))],
                    lambda: owner.query("household", "household", rejected=True))
                self.assertEqual(error is None, accepted)
                request = json.loads(connections[0].request.call_args.args[2])
                self.assertEqual(request["params"], {"name": "search_entities", "arguments": {
                    "query": HOME_QUERY, "use_vector": True, "limit": 1}})
                self.assertEqual(owner.requests, 21)
                self.assertEqual(owner.records, [])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT)
    parser.add_argument("--driver", type=Path)
    parser.add_argument("--dispatch", type=Path)
    args, remaining = parser.parse_known_args()
    SOURCE_ROOT, DRIVER, DISPATCH = args.source_root, args.driver, args.dispatch
    unittest.main(argv=[__file__, *remaining])
