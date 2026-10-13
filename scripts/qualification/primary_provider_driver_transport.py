"""One-shot public transport using the independently reviewed bounded HTTP reader."""
from __future__ import annotations

import asyncio
import base64
import importlib.util
import os
from pathlib import Path
import re
import time

from primary_provider_driver_contract import LIMITS, encode, require, sha


def load_wire(root):
    path = root / "scripts/qualification/recall_workload.py"
    spec = importlib.util.spec_from_file_location("primary_reviewed_workload_wire", path)
    require(spec is not None and spec.loader is not None, "reviewed HTTP helper unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def private_new(path, raw):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def sse_rpc(raw, parse_json, request_id):
    """Decode only a complete bounded stream, retaining the sole original reply."""
    text = raw.decode("utf-8").replace("\r\n", "\n")
    require(text.endswith("\n\n"), "incomplete MCP event stream")
    replies = []
    for block in text.split("\n\n"):
        data = []
        for line in block.splitlines():
            if not line or line.startswith(":"):
                continue
            name, separator, value = line.partition(":")
            require(separator and name in ("event", "data", "id", "retry"), "unknown MCP SSE field")
            if name == "data":
                data.append(value.removeprefix(" "))
        if data:
            event = parse_json("\n".join(data).encode())
            require(isinstance(event, dict) and event.get("jsonrpc") == "2.0", "invalid MCP event")
            # Notifications cannot substitute the original requested result.
            if "id" in event:
                require(event["id"] == request_id, "MCP response identity changed")
                replies.append(event)
    require(len(replies) == 1, "missing or duplicate original MCP result")
    return replies[0]


class PublicTransport:
    def __init__(self, wire, endpoint, tokens, output, deadline):
        self.wire = wire
        self.host, self.port = wire.endpoint(endpoint)
        self.tokens, self.output, self.deadline = tokens, output, deadline
        self.count = self.capture_bytes = 0
        self.captures = []
        require(output.is_absolute() and output == output.resolve() and not output.exists(),
                "new canonical private capture directory required")
        output.mkdir(mode=0o700)
        (output / "requests").mkdir(mode=0o700)

    async def request(self, method, path, space, body=None, timeout=15, mcp_id=None):
        require(method in ("GET", "POST", "PUT") and path.startswith("/")
                and re.fullmatch(r"[\x21-\x7e]+", path) and "#" not in path, "invalid fixed public route")
        require(space in self.tokens and re.fullmatch(r"[a-zA-Z0-9_-]+", space), "unknown explicit space")
        remaining = self.deadline - time.monotonic()
        require(remaining > 0 and self.count < LIMITS["http_requests"], "primary request allocation exhausted")
        require(self.capture_bytes + LIMITS["response_bytes"] * 3 < LIMITS["capture_bytes"], "capture allocation exhausted")
        timeout = min(timeout, remaining)
        payload = b"" if body is None else encode(body)
        require(len(payload) <= LIMITS["request_bytes"], "original request exceeds bound")
        self.count += 1
        key = f"{self.count:05}"
        # Persist the original request before its only send. A crash leaves an
        # explicit unsettled attempt, never an automatic re-submission.
        intent = {"method": method, "path": path, "space": space, "request_body": body,
                  "request_body_base64": base64.b64encode(payload).decode(), "request_sha256": sha(payload),
                  "timeout_seconds": timeout, "harness_attempt": 1}
        raw_intent = encode(intent)
        private_new(self.output / "requests" / (key + ".request.json"), raw_intent)
        self.capture_bytes += len(raw_intent)
        host = f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"
        accept = "application/json, text/event-stream" if mcp_id is not None else "application/json"
        extra = "MCP-Protocol-Version: 2025-03-26\r\n" if mcp_id is not None else ""
        request = (f"{method} {path} HTTP/1.1\r\nHost: {host}\r\nAuthorization: Bearer {self.tokens[space]}\r\n"
                   f"X-Space-Id: {space}\r\nContent-Type: application/json\r\nAccept: {accept}\r\n{extra}"
                   f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode() + payload
        observed = {"raw_body": b"", "framing_complete": False, "status": None, "body": None}
        started, writer, error_type = time.monotonic_ns(), None, None
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_connection(self.host, self.port, limit=self.wire.HEADER_CAP)
                writer.write(request)
                await writer.drain()
                try:
                    await self.wire.read_frame(reader, observed)
                except (ValueError, UnicodeError):
                    content_type = observed.get("response_headers", {}).get("content-type", [])
                    if (mcp_id is None or not observed["framing_complete"] or len(content_type) != 1
                            or content_type[0].split(";", 1)[0].strip() != "text/event-stream"):
                        raise
                    observed["body"] = sse_rpc(observed["raw_body"], self.wire.helpers.capture.strict_json_body, mcp_id)
        except (Exception, asyncio.CancelledError) as error:
            error_type = type(error).__name__
        finally:
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(0.1):
                        await writer.wait_closed()
                except (Exception, asyncio.CancelledError) as error:
                    error_type = error_type or type(error).__name__
        raw = observed.pop("raw_body")[:LIMITS["response_bytes"]]
        response = {**observed, "raw_body_base64": base64.b64encode(raw).decode(), "body_sha256": sha(raw),
                    "raw_body_complete": observed["framing_complete"], "duration_ms": (time.monotonic_ns() - started) / 1e6,
                    "error_type": error_type, "request_index": self.count}
        encoded = encode(response)
        self.capture_bytes += len(encoded)
        require(self.capture_bytes <= LIMITS["capture_bytes"], "capture byte bound exceeded")
        private_new(self.output / "requests" / (key + ".response.json"), encoded)
        self.captures.append({"request_index": self.count, "request_sha256": sha(raw_intent), "response_sha256": sha(encoded)})
        return response

    @staticmethod
    def successful(response, status=200):
        require(response.get("error_type") is None and response.get("framing_complete") is True
                and response.get("body") is not None and response.get("status") == status,
                "original public request failed or was incompletely captured")
        return response["body"]

    async def get(self, path, space):
        return self.successful(await self.request("GET", path, space))
