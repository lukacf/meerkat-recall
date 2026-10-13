"""Offline protocol/custody controls; these are not installed product results."""
import asyncio
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from full_workload_transport import Capture, Client, Unmet, decode, encode, endpoint, mcp_response, response_frame


LIMITS = {"public_http_calls_per_backend": 8000, "request_body_bytes": 3145728,
          "raw_source_response_bytes": 3145728, "request_seconds": 10}


class Writer:
    def __init__(self):
        self.data, self.closed = [], False

    def write(self, data):
        self.data.append(data)

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    async def wait_closed(self):
        pass


def reader(data):
    value = asyncio.StreamReader()
    value.feed_data(data)
    value.feed_eof()
    return value


def runtime_body(identifier=1, *, initialize=False):
    return {"jsonrpc": "2.0", "id": identifier,
            "method": "initialize" if initialize else "tools/call",
            "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                       "clientInfo": {"name": "full-workload-runtime-inspector", "version": "1"}}
                      if initialize else {"name": "get_runtime_config", "arguments": {}}}


class TransportTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for target in ("asyncio.open_connection", "socket.socket", "subprocess.Popen"):
            guard = patch(target, side_effect=AssertionError("offline transport controls only"))
            guard.start()
            self.addCleanup(guard.stop)

    def setUp(self):
        for name in ("asyncio.start_server", "asyncio.open_connection", "subprocess.Popen", "socket.create_connection"):
            guard = patch(name, side_effect=AssertionError("closed-world transport controls cannot execute runtime"))
            guard.start()
            self.addCleanup(guard.stop)

    async def frame(self, wire, limit=1024):
        observed = {"raw_body": b"", "framing_complete": False}
        await response_frame(reader(wire), observed, limit)
        return observed

    async def test_complete_length_and_chunked_preserve_actual_body(self):
        for wire in (b'HTTP/1.1 200 OK\r\nContent-Length: 7\r\n\r\n{"n":1}',
                     b'HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\n{"n\r\n4\r\n":1}\r\n0\r\n\r\n'):
            with self.subTest(wire=wire):
                value = await self.frame(wire)
                self.assertEqual(value["raw_body"], b'{"n":1}')
                self.assertEqual(value["body"], {"n": 1})
                self.assertTrue(value["framing_complete"])

    async def test_ambiguous_unbounded_and_trailing_responses_refuse(self):
        for headers, suffix in [(b"Content-Length: 2\r\nContent-Length: 2", b"{}"),
                                (b"Content-Length: 2\r\nTransfer-Encoding: chunked", b"{}"),
                                (b"Content-Length: 1025", b"{}"),
                                (b"Content-Length: 2", b"{}extra"),
                                (b"Content-Type: application/json", b"{}")]:
            with self.subTest(headers=headers), self.assertRaises(Unmet):
                await self.frame(b"HTTP/1.1 200 OK\r\n" + headers + b"\r\n\r\n" + suffix)

    async def test_truncated_body_retains_exact_received_prefix(self):
        observed = {"raw_body": b"", "framing_complete": False}
        with self.assertRaises(Unmet):
            await response_frame(reader(b'HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n{"n":'), observed, 1024)
        self.assertEqual(observed["raw_body"], b'{"n":')
        self.assertFalse(observed["framing_complete"])

    async def test_original_request_is_sent_once_and_raw_error_is_captured(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Capture(Path(directory) / "capture", 16 * 1024 * 1024)
            client = Client("http://127.0.0.1:43120", {"data": "synthetic.signed.jwt"}, capture,
                            LIMITS, time.monotonic() + 20, time.time() + 20)
            writer = Writer()
            with patch("asyncio.open_connection", return_value=(
                    reader(b'HTTP/1.1 503 Unavailable\r\nContent-Length: 16\r\n\r\n{"error":"busy"}'), writer)) as connect:
                result = await client.request("POST", "/v1/sources/ingest", "qualification-household",
                                              {"original": True}, role="data")
            self.assertEqual(connect.call_count, 1)
            self.assertEqual(len(writer.data), 1)
            self.assertTrue(writer.closed)
            self.assertEqual(result["status"], 503)
            self.assertEqual(client.count, 1)
            self.assertIsNone(result.get("provider_attempt_ids"))
            self.assertEqual((capture.directory / "000001.response.bin").read_bytes(), b'{"error":"busy"}')
            self.assertEqual(capture.held, 0)

    async def test_expired_or_exhausted_run_refuses_before_connect(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120", {"data": "synthetic.signed.jwt"},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024),
                            LIMITS, time.monotonic() - 1, time.time() + 20)
            with patch("asyncio.open_connection", side_effect=AssertionError("no network")) as connect:
                with self.assertRaises(Unmet):
                    await client.request("GET", "/v1/work-items", "qualification-household", role="data")
                client.deadline = time.monotonic() + 10
                client.count = 8000
                with self.assertRaises(Unmet):
                    await client.request("GET", "/v1/work-items", "qualification-household", role="data")
            connect.assert_not_called()

    async def test_cancelled_request_is_captured_and_not_resubmitted(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Capture(Path(directory) / "capture", 16 * 1024 * 1024)
            client = Client("http://127.0.0.1:43120", {"data": "synthetic.signed.jwt"}, capture,
                            LIMITS, time.monotonic() + 20, time.time() + 20)
            with patch("asyncio.open_connection", side_effect=asyncio.CancelledError) as connect:
                with self.assertRaises(asyncio.CancelledError):
                    await client.request("PUT", "/v1/spaces/qualification-household/sources/key",
                                         "qualification-household", {"revision": "original"}, role="data")
            self.assertEqual(connect.call_count, 1)
            record = decode((capture.directory / "000001.exchange.json").read_bytes())
            self.assertEqual(record["error_type"], "CancelledError")
            self.assertFalse(record["framing_complete"])

    async def test_runtime_and_named_requests_share_capture_count_and_exact_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Capture(Path(directory) / "capture", 16 * 1024 * 1024)
            tokens = {"data": "named.data.jwt", "manage": "named.manage.jwt",
                      "runtime:inspect": "original.runtime.jwt"}
            client = Client("http://127.0.0.1:43120", tokens, capture, LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            for identifier, initialize in ((1, True), (2, False)):
                request = runtime_body(identifier, initialize=initialize)
                result = {"jsonrpc": "2.0", "id": identifier, "result": {"original": identifier}}
                media = b"application/json" if initialize else b"text/event-stream"
                raw = encode(result) if initialize else b"event: message\ndata: " + encode(result) + b"\n\n"
                wire = (b"HTTP/1.1 200 OK\r\nContent-Type: " + media + b"\r\nContent-Length: "
                        + str(len(raw)).encode() + b"\r\n\r\n" + raw)
                writer = Writer()
                with patch("asyncio.open_connection", return_value=(reader(wire), writer)) as connect:
                    observed = await client.runtime_request(request, response_limit=262144)
                connect.assert_awaited_once()
                self.assertEqual(observed["id"], identifier)
                self.assertEqual(observed["body"], result)
                self.assertEqual(observed["path"], "/mcp/full")
                self.assertEqual(observed["credential_role"], "runtime:inspect")
                self.assertIsNone(observed["space"])
                self.assertNotIn(b"X-Space-Id:", writer.data[0])
                self.assertIn(b"Authorization: Bearer original.runtime.jwt\r\n", writer.data[0])
                self.assertIn(b"MCP-Protocol-Version: 2025-03-26\r\n", writer.data[0])
                self.assertIn(b"Accept: application/json, text/event-stream\r\n", writer.data[0])
                self.assertEqual((capture.directory / f"{identifier:06}.request.json").read_bytes(), encode(request))
                self.assertEqual((capture.directory / f"{identifier:06}.response.bin").read_bytes(), raw)
            writer = Writer()
            with patch("asyncio.open_connection", return_value=(
                    reader(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"), writer)):
                observed = await client.request("GET", "/v1/spaces/qualification-household",
                                                "qualification-household", role="manage", response_limit=1024)
            self.assertEqual(observed["id"], 3)
            self.assertIn(b"X-Space-Id: qualification-household\r\n", writer.data[0])
            self.assertIn(b"Authorization: Bearer named.manage.jwt\r\n", writer.data[0])
            self.assertEqual(client.count, 3)
            self.assertEqual([row["id"] for row in client.rows], [1, 2, 3])
            self.assertEqual(capture.held, 0)
            for path in capture.directory.iterdir():
                for token in tokens.values():
                    self.assertNotIn(token.encode(), path.read_bytes())

    async def test_runtime_global_other_tools_arguments_and_named_credentials_refuse_before_io(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120", {"data": "named.data.jwt", "runtime:inspect": "original.runtime.jwt"},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            requests = [None, {}, runtime_body() | {"method": "notifications/initialized"},
                        runtime_body() | {"id": True}, runtime_body() | {"id": 8001},
                        runtime_body() | {"path": "/v1/runtime"},
                        runtime_body() | {"params": {"name": "initialize_runtime", "arguments": {}}},
                        runtime_body() | {"params": {"name": "get_runtime_config", "arguments": {"space_id": "foreign"}}},
                        runtime_body() | {"params": {"name": "get_runtime_config"}},
                        runtime_body(initialize=True) | {"params": {"protocolVersion": "2025-03-26"}}]
            with patch("asyncio.open_connection", side_effect=AssertionError("no connection")) as connect:
                for request in requests:
                    with self.subTest(request=request), self.assertRaises(Unmet):
                        await client.runtime_request(request)
                for space in (None, "qualification-household"):
                    with self.assertRaises(Unmet):
                        await client.request("POST", "/mcp/full", space, runtime_body(), role="runtime:inspect")
                del client.tokens["runtime:inspect"]
                with self.assertRaises(Unmet):
                    await client.runtime_request(runtime_body())
                connect.assert_not_called()
            self.assertEqual(client.count, 0)
            self.assertEqual(client.capture.held, 0)
            self.assertEqual(list(client.capture.directory.iterdir()), [])

    async def test_runtime_obeys_original_shared_expiry_request_and_capture_caps(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120", {"runtime:inspect": "original.runtime.jwt"},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() - 1, time.time() + 20)
            with patch("asyncio.open_connection", side_effect=AssertionError("no connection")) as connect:
                with self.assertRaisesRegex(Unmet, "deadline"):
                    await client.runtime_request(runtime_body())
                client.deadline = time.monotonic() + 20
                client.expires = time.time() - 1
                with self.assertRaisesRegex(Unmet, "deadline"):
                    await client.runtime_request(runtime_body())
                client.expires = time.time() + 20
                client.count = LIMITS["public_http_calls_per_backend"]
                with self.assertRaisesRegex(Unmet, "request cap"):
                    await client.runtime_request(runtime_body())
                client.count = 0
                client.capture.maximum = 1
                with self.assertRaisesRegex(Unmet, "capture budget"):
                    await client.runtime_request(runtime_body())
                connect.assert_not_called()
            self.assertEqual(client.count, 0)
            self.assertEqual(client.capture.held, 0)

    async def test_runtime_cancelled_request_keeps_original_failure_without_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120", {"runtime:inspect": "original.runtime.jwt"},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            with patch("asyncio.open_connection", side_effect=asyncio.CancelledError) as connect:
                with self.assertRaises(asyncio.CancelledError):
                    await client.runtime_request(runtime_body())
                connect.assert_awaited_once()
            self.assertEqual(client.count, 1)
            self.assertEqual(client.rows[0]["error_type"], "CancelledError")
            self.assertEqual(client.rows[0]["credential_role"], "runtime:inspect")
            self.assertEqual(client.capture.held, 0)

    async def test_paired_health_uses_only_bound_endpoint_without_auth_on_same_client(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Capture(Path(directory) / "capture", 16 * 1024 * 1024)
            client = Client("http://127.0.0.1:43120", {"data": "named.data.jwt"}, capture, LIMITS,
                            time.monotonic() + 60, time.time() + 60)
            paired = "http://127.0.0.1:43121"
            client.bind_paired_health(paired)
            writer = Writer()
            with patch("asyncio.open_connection", return_value=(
                    reader(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"), writer)) as connect:
                observed = await client.paired_health(paired)
            connect.assert_awaited_once_with("127.0.0.1", 43121, limit=65536)
            self.assertEqual(observed["endpoint"], paired)
            self.assertEqual(observed["id"], 1)
            self.assertEqual(observed["path"], "/health")
            self.assertEqual(observed["status"], 200)
            self.assertTrue(observed["framing_complete"])
            self.assertIsNone(observed["body"])
            self.assertIsNone(observed["credential_role"])
            self.assertIsNone(observed["credential_sha256"])
            self.assertNotIn(b"Authorization:", writer.data[0])
            self.assertNotIn(b"X-Space-Id:", writer.data[0])
            self.assertTrue(writer.data[0].startswith(b"GET /health HTTP/1.1\r\nHost: 127.0.0.1:43121\r\n"))
            writer = Writer()
            with patch("asyncio.open_connection", return_value=(
                    reader(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"), writer)) as connect:
                await client.request("GET", "/v1/work-items", "qualification-household", role="data", response_limit=1024)
            connect.assert_awaited_once_with("127.0.0.1", 43120, limit=65536)
            self.assertEqual(client.count, 2)
            self.assertEqual([row["id"] for row in client.rows], [1, 2])
            self.assertEqual(capture.held, 0)
            self.assertEqual((capture.directory / "000001.response.bin").read_bytes(), b"")
            self.assertEqual(decode((capture.directory / "000001.exchange.json").read_bytes())["endpoint"], paired)

    async def test_paired_health_cannot_rebind_extend_window_or_bypass_shared_caps(self):
        with tempfile.TemporaryDirectory() as directory:
            ticks = [10.0]
            client = Client("http://127.0.0.1:43120", {},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            100, time.time() + 60, clock=lambda: ticks[0])
            paired = "http://127.0.0.1:43121"
            with patch("asyncio.open_connection", side_effect=AssertionError("no connection")) as connect:
                with self.assertRaisesRegex(Unmet, "originally bound"):
                    await client.paired_health(paired)
                for address in ("http://localhost:43121", paired + "/health", "http://example.invalid:80"):
                    with self.assertRaises(Unmet):
                        client.bind_paired_health(address)
                client.bind_paired_health(paired)
                self.assertEqual(client._paired_deadline, 40)
                for address in (paired, "http://127.0.0.1:43122"):
                    with self.assertRaisesRegex(Unmet, "already bound"):
                        client.bind_paired_health(address)
                with self.assertRaisesRegex(Unmet, "originally bound"):
                    await client.paired_health(paired + "/")
                client.count = LIMITS["public_http_calls_per_backend"]
                with self.assertRaisesRegex(Unmet, "request cap"):
                    await client.paired_health(paired)
                client.count = 0
                client.capture.maximum = 1
                with self.assertRaisesRegex(Unmet, "capture budget"):
                    await client.paired_health(paired)
                client.capture.maximum = 16 * 1024 * 1024
                ticks[0] = 40
                with self.assertRaisesRegex(Unmet, "deadline"):
                    await client.paired_health(paired)
                self.assertEqual(client._paired_deadline, 40)
                connect.assert_not_called()
            self.assertEqual(client.count, 0)
            self.assertEqual(client.capture.held, 0)

    async def test_paired_health_preserves_failed_framing_and_never_follows_redirect(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120", {},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            paired = "http://127.0.0.1:43121"
            client.bind_paired_health(paired)
            self.assertEqual(client._paired_deadline, client.deadline)
            for wire, expected_status in ((b"HTTP/1.1 302 Found\r\nLocation: http://example.invalid/\r\nContent-Length: 0\r\n\r\n", 302),
                                           (b"HTTP/1.1 200 OK\r\nContent-Length: 4097\r\n\r\n", 200)):
                with patch("asyncio.open_connection", return_value=(reader(wire), Writer())) as connect:
                    observed = await client.paired_health(paired)
                connect.assert_awaited_once()
                self.assertEqual(observed["status"], expected_status)
                if expected_status == 200:
                    self.assertEqual(observed["error_type"], "Unmet")
                    self.assertFalse(observed["framing_complete"])
            self.assertEqual(client.count, 2)

    async def test_owner_fixed_startup_and_export_use_separate_operator_on_same_capture(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120",
                            {"runtime:inspect": "original.inspect.jwt", "runtime:operator": "original.operator.jwt"},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            initialize = runtime_body(initialize=True)
            initialize["params"]["clientInfo"]["name"] = "full-workload-runtime"
            def tool(name, arguments):
                return runtime_body() | {"params": {"name": name, "arguments": arguments}}
            requests = [("GET", "/v1/health", None), ("GET", "/v1/readiness", None),
                        ("POST", "/mcp/full", initialize),
                        ("POST", "/mcp/full", tool("initialize_runtime", {
                            "ingest_enabled": False, "extractor_required": False, "backend": "elephant-db",
                            "multimodal_asset_root": "/original/media", "snapshot_path": "/original/state.snapshot"})),
                        ("POST", "/mcp/full", tool("configure_runtime", {
                            "ingest_enabled": False, "extractor_required": False, "multimodal_asset_root": "/original/media"})),
                        ("POST", "/mcp/full", tool("configure_runtime", {"ingest_enabled": True, "extractor_required": False})),
                        ("POST", "/v1/system/snapshot/export", {"path": "original-1", "name": "original-1",
                                                               "retention_seconds": 3600, "quiesce_timeout_seconds": 30})]
            for identifier, (method, path, request) in enumerate(requests, 1):
                if path == "/mcp/full":
                    request["id"] = identifier
                    result = {"jsonrpc": "2.0", "id": identifier, "result": {}}
                else:
                    result = {"original": identifier}
                raw = encode(result)
                wire = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                        + str(len(raw)).encode() + b"\r\n\r\n" + raw)
                writer = Writer()
                with patch("asyncio.open_connection", return_value=(reader(wire), writer)) as connect:
                    row = await client.owner_request(method, path, request, response_limit=262144)
                connect.assert_awaited_once()
                self.assertEqual(row["body"], result)
                self.assertEqual(row["credential_role"], "runtime:operator")
                self.assertIsNone(row["space"])
                self.assertNotIn(b"X-Space-Id:", writer.data[0])
                self.assertIn(b"Authorization: Bearer original.operator.jwt\r\n", writer.data[0])
                self.assertNotIn(b"original.inspect.jwt", writer.data[0])
                self.assertEqual(row["id"], identifier)
            self.assertEqual(client.count, 7)
            self.assertEqual(client.capture.held, 0)

    async def test_operator_denies_other_tools_routes_and_unbounded_export_before_io(self):
        with tempfile.TemporaryDirectory() as directory:
            client = Client("http://127.0.0.1:43120", {"runtime:operator": "original.operator.jwt"},
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            export = {"path": "original", "name": "original", "retention_seconds": 3600, "quiesce_timeout_seconds": 30}
            bad = [("GET", "/v1/health", {}), ("GET", "/v1/runtime/status", None),
                   ("POST", "/v1/system/snapshot/restore", export),
                   ("POST", "/v1/system/snapshot/export?extra=1", export),
                   ("POST", "/mcp/full", runtime_body()),
                   ("POST", "/mcp/full", runtime_body(initialize=True)),
                   ("POST", "/mcp/full", runtime_body() | {"params": {"name": "configure_runtime", "arguments": {"ingest_enabled": True}}}),
                   ("POST", "/mcp/full", runtime_body() | {"params": {"name": "configure_runtime", "arguments": {"ingest_enabled": 1, "extractor_required": False}}})]
            for mutation in ({"retention_seconds": 3601}, {"quiesce_timeout_seconds": 31},
                             {"quiesce_timeout_seconds": True}, {"path": "/foreign"},
                             {"name": "../foreign", "path": "../foreign"}, {"extra": True}):
                bad.append(("POST", "/v1/system/snapshot/export", export | mutation))
            with patch("asyncio.open_connection", side_effect=AssertionError("no connection")) as connect:
                for method, path, body in bad:
                    with self.subTest(method=method, path=path, body=body), self.assertRaises(Unmet):
                        await client.owner_request(method, path, body)
                with self.assertRaises(Unmet):
                    await client.runtime_request(runtime_body())
                client.tokens = {"runtime:inspect": "original.inspect.jwt"}
                with self.assertRaises(Unmet):
                    await client.owner_request("GET", "/v1/health")
                connect.assert_not_called()
            self.assertEqual(client.count, 0)
            self.assertEqual(client.capture.held, 0)

    async def test_existing_named_adapter_keeps_one_prefix_and_global_methods_bypass_it(self):
        # This is the exact adapter shape used by full_workload_driver.NamedClient.
        # The base transport must not add a second prefix or call this override
        # for operator, inspector or paired requests.
        class NamedClient(Client):
            async def request(self, method, path, space, body=None, *, role, **options):
                selected = f"{space}:{role}"
                if selected not in self.tokens:
                    raise Unmet("original named-space credential required")
                return await super().request(method, path, space, body, role=selected, **options)

        with tempfile.TemporaryDirectory() as directory:
            tokens = {"household:data": "household.data.jwt", "business:data": "business.data.jwt",
                      "household:revocable": "household.revocable.jwt", "data": "legacy.data.jwt",
                      "runtime:inspect": "original.inspect.jwt", "runtime:operator": "original.operator.jwt"}
            client = NamedClient("http://127.0.0.1:43120", tokens,
                            Capture(Path(directory) / "capture", 16 * 1024 * 1024), LIMITS,
                            time.monotonic() + 20, time.time() + 20)
            for space, role in (("household", "data"), ("business", "data"), ("household", "revocable")):
                writer = Writer()
                with patch("asyncio.open_connection", return_value=(
                        reader(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}"), writer)):
                    row = await client.request("GET", "/v1/work-items", space, role=role, response_limit=1024)
                self.assertIn(f"Authorization: Bearer {tokens[space + ':' + role]}\r\n".encode(), writer.data[0])
                self.assertIn(f"X-Space-Id: {space}\r\n".encode(), writer.data[0])
                self.assertEqual(row["credential_role"], space + ":" + role)
                self.assertEqual(row["space"], space)
            with patch("asyncio.open_connection", side_effect=AssertionError("no connection")) as connect:
                for space, role in (("foreign", "data"), ("business", "revocable"), ("household", "runtime:operator")):
                    with self.assertRaises(Unmet):
                        await client.request("GET", "/v1/work-items", space, role=role)
                del client.tokens["household:data"]
                with self.assertRaises(Unmet):
                    await client.request("GET", "/v1/work-items", "household", role="data")
                connect.assert_not_called()
            self.assertEqual(client.count, 3)
            for request_kind in ("inspector", "operator", "paired"):
                identifier = client.count + 1
                raw = encode({"jsonrpc": "2.0", "id": identifier, "result": {}}) if request_kind == "inspector" else b"{}"
                wire = (b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                        + str(len(raw)).encode() + b"\r\n\r\n" + raw)
                writer = Writer()
                with patch("asyncio.open_connection", return_value=(reader(wire), writer)):
                    if request_kind == "inspector":
                        row = await client.runtime_request(runtime_body(identifier), response_limit=1024)
                    elif request_kind == "operator":
                        row = await client.owner_request("GET", "/v1/health", response_limit=1024)
                    else:
                        client.bind_paired_health("http://127.0.0.1:43121")
                        row = await client.paired_health("http://127.0.0.1:43121")
                self.assertEqual(row["id"], identifier)
                self.assertIsNone(row["space"])
                self.assertNotIn(b"X-Space-Id:", writer.data[0])
                self.assertEqual(row["credential_role"], {"inspector": "runtime:inspect",
                                 "operator": "runtime:operator", "paired": None}[request_kind])
            self.assertEqual([row["id"] for row in client.rows], list(range(1, 7)))
            self.assertEqual(client.count, 6)
            self.assertEqual(client.capture.held, 0)

    def test_no_redirect_proxy_or_nonloopback_target(self):
        for value in ("https://127.0.0.1:80", "http://localhost:80", "http://example.invalid:80",
                      "http://127.0.0.1:80/path", "http://user@127.0.0.1:80", "http://127.0.0.1:80?x"):
            with self.subTest(value=value), self.assertRaises(Unmet):
                endpoint(value)

    def test_json_duplicates_nonfinite_and_invalid_utf8_refuse(self):
        for raw in (b'{"a":1,"a":2}', b'{"n":NaN}', b'{"n":1e999}', b'"\xff"'):
            with self.subTest(raw=raw), self.assertRaises(Unmet):
                decode(raw)

    def test_mcp_complete_json_and_sse_keep_one_exact_original_identity(self):
        value = {"jsonrpc": "2.0", "id": 7, "result": {"isError": True, "content": []}}
        raw = encode(value)
        self.assertEqual(mcp_response(raw, "application/json; charset=utf-8", 7), value)
        self.assertEqual(mcp_response(b": bounded comment\nevent: message\nid: event-7\ndata: " + raw + b"\n\n", "text/event-stream", 7), value)
        self.assertEqual(mcp_response(b"event: message\r\ndata: " + raw + b"\r\n\r\n", "text/event-stream", 7), value)

    def test_mcp_extra_foreign_or_truncated_frames_never_pass(self):
        raw = encode({"jsonrpc": "2.0", "id": 7, "result": {}})
        for body, media, identity in ((raw, "application/json", 8),
                (raw, "text/plain", 7),
                (b"data: " + raw + b"\n", "text/event-stream", 7),
                ((b"data: " + raw + b"\n\n") * 2, "text/event-stream", 7),
                (b"event: other\ndata: " + raw + b"\n\n", "text/event-stream", 7),
                (b"retry: 10\ndata: " + raw + b"\n\n", "text/event-stream", 7),
                (b"data: {\"jsonrpc\":\"2.0\",\"id\":7,\"id\":7,\"result\":{}}\n\n", "text/event-stream", 7),
                (encode({"jsonrpc": "2.0", "id": True, "result": {}}), "application/json", 1),
                (encode({"jsonrpc": "2.0", "id": 7, "result": {}, "error": {}}), "application/json", 7)):
            with self.subTest(body=body), self.assertRaises(Unmet):
                mcp_response(body, media, identity)

    async def test_mcp_exact_post_uses_original_capture_budget_and_protocol_headers(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Capture(Path(directory) / "capture", 16 * 1024 * 1024)
            client = Client("http://127.0.0.1:43120", {"data": "synthetic.signed.jwt"}, capture,
                            LIMITS, time.monotonic() + 20, time.time() + 20)
            request = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "get_source_head", "arguments": {"external_source_id": "original"}}}
            result = {"jsonrpc": "2.0", "id": 1, "result": {"isError": True, "content": [{"type": "text", "text": "{\"error_code\":\"forbidden_space\"}"}]}}
            raw = b"event: message\ndata: " + encode(result) + b"\n\n"
            wire = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw
            writer = Writer()
            with patch("asyncio.open_connection", return_value=(reader(wire), writer)) as connect:
                observed = await client.request("POST", "/mcp/full", "qualification-household", request, role="data", response_limit=262144)
            self.assertEqual(observed["body"], result)
            self.assertEqual(connect.call_count, 1)
            self.assertEqual(client.count, 1)
            self.assertIn(b"Accept: application/json, text/event-stream\r\n", writer.data[0])
            self.assertIn(b"MCP-Protocol-Version: 2025-03-26\r\n", writer.data[0])
            self.assertEqual((capture.directory / "000001.response.bin").read_bytes(), raw)
            self.assertEqual(capture.held, 0)
            for method, path in (("GET", "/mcp/full"), ("POST", "/mcp/full?query=1"),
                                 ("POST", "/mcp/other"), ("POST", "/mcp/full/")):
                with self.subTest(method=method, path=path), self.assertRaises(Unmet):
                    await client.request(method, path, "qualification-household", request, role="data")
            self.assertEqual(client.count, 1)

    async def test_mcp_incomplete_or_wrong_original_id_is_captured_as_failure_once(self):
        with tempfile.TemporaryDirectory() as directory:
            capture = Capture(Path(directory) / "capture", 16 * 1024 * 1024)
            client = Client("http://127.0.0.1:43120", {"data": "synthetic.signed.jwt"}, capture,
                            LIMITS, time.monotonic() + 20, time.time() + 20)
            raw = encode({"jsonrpc": "2.0", "id": 99, "result": {}})
            wire = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw
            with patch("asyncio.open_connection", return_value=(reader(wire), Writer())) as connect:
                result = await client.request("POST", "/mcp/full", "qualification-household", {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}, role="data")
            self.assertEqual(result["error_type"], "Unmet")
            self.assertIsNone(result["body"])
            self.assertEqual(client.count, 1)
            self.assertEqual(connect.call_count, 1)
            self.assertEqual((capture.directory / "000001.response.bin").read_bytes(), raw)


if __name__ == "__main__":
    unittest.main()
