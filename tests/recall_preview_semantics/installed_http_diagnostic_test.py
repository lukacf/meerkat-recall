"""Pure HTTP-capture controls using actual source methods and a controlled wire.

No driver initialization, imports of product modules, socket, child, or model.
These controls qualify diagnostics only; they cannot establish a working route.
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


def source_functions(path, names):
    tree = ast.parse(path.read_bytes(), filename=str(path))
    selected = [node for node in tree.body
                if isinstance(node, ast.FunctionDef) and node.name in names]
    if {node.name for node in selected} != set(names):
        raise ValueError("requested original source definitions are absent")
    return selected


def load_original_http():
    """Compile only the actual HTTP method and its pure parsing dependencies."""
    parser_path = SOURCE_ROOT / "scripts/qualification/recall_preview.py"
    parser_scope = {"json": json, "math": math}
    parser_nodes = source_functions(parser_path, {"strict_json_body"})
    exec(compile(ast.Module(body=parser_nodes, type_ignores=[]), str(parser_path), "exec"), parser_scope)
    path = DRIVER or SOURCE_ROOT / "scripts/qualification/installed_linux_offline.py"
    nodes = source_functions(path, {"require", "digest", "encoded", "mcp_value"})
    tree = ast.parse(path.read_bytes(), filename=str(path))
    driver = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "Driver")
    method = next(node for node in driver.body if isinstance(node, ast.FunctionDef) and node.name == "http")
    # No Driver constructor, module top-level loader or fixture execution.
    nodes.append(ast.ClassDef(name="Driver", bases=[], keywords=[], body=[method], decorator_list=[]))
    scope = {"base64": base64, "hashlib": hashlib, "http": http, "json": json, "time": time,
             "capture": types.SimpleNamespace(strict_json_body=parser_scope["strict_json_body"],
                                               total_deadline=lambda _seconds: contextlib.nullcontext())}
    exec(compile(ast.fix_missing_locations(ast.Module(body=nodes, type_ignores=[])), str(path), "exec"), scope)
    return scope["Driver"]


class InstalledHttpDiagnostics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.driver_type = load_original_http()

    def response(self, status, raw, *, mcp=False, content_length=None):
        owner = self.driver_type.__new__(self.driver_type)
        owner.requests = 0
        owner.started = time.monotonic()
        owner.port = 1  # No connection is constructed: HTTPConnection is patched.
        owner.tokens = {"bootstrap": "private-synthetic-token-never-in-errors"}
        owner.cases = {"limits": {"http_calls": 2, "http_seconds": 5,
                                  "total_seconds": 30, "response_bytes": 4096}}
        owner.remaining = lambda: 30
        owner.saved = []
        owner.save = lambda name, value: owner.saved.append((name, json.loads(value)))
        headers = Message()
        headers["Content-Length"] = str(len(raw) if content_length is None else content_length)
        response = Mock(status=status, headers=headers)
        response.read1.side_effect = [raw, b""] if raw else [b""]
        connection = Mock()
        connection.getresponse.return_value = response
        path = "/mcp/full" if mcp else "/v1/spaces"
        with patch.object(http.client, "HTTPConnection", return_value=connection):
            try:
                value = owner.http("POST", path, "bootstrap", "household",
                                   {"id": 7, "private": "private-request-canary"}, mcp=mcp)
                result = (value, None)
            except ValueError as error:
                result = (None, error)
        connection.request.assert_called_once()
        connection.close.assert_called_once()
        self.assertEqual(owner.requests, 1)
        self.assertEqual(len(owner.saved), 1)
        name, saved = owner.saved[0]
        self.assertEqual(name, "http-001.json")
        self.assertEqual(saved["status"], status)
        self.assertEqual(base64.b64decode(saved["raw_body_base64"]), raw)
        self.assertEqual(saved["body_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertNotIn(owner.tokens["bootstrap"], json.dumps(saved))
        return result[0], result[1], saved

    def test_complete_empty_404_preserves_status_and_capture_and_fails(self):
        value, error, saved = self.response(404, b"")
        self.assertIsNone(value)
        self.assertEqual(str(error), "HTTP 404 POST /v1/spaces: invalid response body (0 bytes; capture http-001.json)")
        self.assertIsInstance(error.__cause__, json.JSONDecodeError)
        self.assertTrue(saved["raw_body_complete"])

    def test_empty_success_is_still_rejected(self):
        value, error, _ = self.response(201, b"")
        self.assertIsNone(value)
        self.assertIn("HTTP 201", str(error))

    def test_invalid_bodies_never_become_defaults_or_disclose_body(self):
        for raw in (b"private-response-canary", b'{"a":1,"a":2}', b'{"n":NaN}',
                    b'{"n":1e999}', b"\xff", b'{"incomplete":'):
            with self.subTest(raw=raw):
                value, error, saved = self.response(403, raw)
                self.assertIsNone(value)
                self.assertIsInstance(error, ValueError)
                self.assertIn("HTTP 403 POST /v1/spaces", str(error))
                self.assertNotIn("private-response-canary", str(error))
                self.assertNotIn("private-request-canary", str(error))
                self.assertNotIn("private-synthetic-token", str(error))
                self.assertTrue(saved["raw_body_complete"])

    def test_valid_success_and_typed_denial_remain_exact(self):
        for status, body in ((201, {"space_id": "household"}),
                             (403, {"error_code": "forbidden", "message": "Denied", "retryable": False})):
            with self.subTest(status=status):
                value, error, _ = self.response(status, json.dumps(body).encode())
                self.assertIsNone(error)
                self.assertEqual(value, {"status": status, "body": body})

    def test_valid_mcp_sse_retains_exact_response_identity(self):
        body = {"jsonrpc": "2.0", "id": 7, "result": {"configured": True}}
        value, error, _ = self.response(200, b"data: " + json.dumps(body).encode() + b"\n\n", mcp=True)
        self.assertIsNone(error)
        self.assertEqual(value, {"status": 200, "body": body})

    def test_mcp_wrong_identity_or_incomplete_event_still_fails(self):
        for raw in (b'data: {"jsonrpc":"2.0","id":8,"result":{}}\n\n',
                    b'data: {"jsonrpc":"2.0","id":7,"result":{}}\n'):
            with self.subTest(raw=raw):
                value, error, _ = self.response(200, raw, mcp=True)
                self.assertIsNone(value)
                self.assertIn("HTTP 200 POST /mcp/full", str(error))
                self.assertIsInstance(error.__cause__, ValueError)

    def test_framing_failure_precedes_parser_and_remains_incomplete(self):
        value, error, saved = self.response(404, b"", content_length=1)
        self.assertIsNone(value)
        self.assertEqual(str(error), "invalid response length")
        self.assertFalse(saved["raw_body_complete"])
        self.assertEqual(saved["capture_error"], "invalid response length")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=SOURCE_ROOT)
    parser.add_argument("--driver", type=Path)
    args, remaining = parser.parse_known_args()
    SOURCE_ROOT, DRIVER = args.source_root, args.driver
    unittest.main(argv=[__file__, *remaining])
