"""Finite public HTTP capture for the independently reviewed full workload.

No process execution, database access, ambient proxy, redirect or request retry.
Actual product qualification requires the installed service and isolation receipt.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import time
from urllib.parse import urlsplit


class Unmet(RuntimeError):
    """An original prerequisite or observable protocol contract was not met."""


def require(condition, reason):
    if not condition:
        raise Unmet(reason)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def sha(data):
    return hashlib.sha256(data).hexdigest()


def decode(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
        encode(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise Unmet("invalid finite UTF-8 JSON") from error


def pinned(path, expected, maximum=8 * 1024 * 1024):
    path = Path(path)
    require(path.is_absolute() and path == path.resolve(), "original canonical input path required")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum, "bounded regular input required")
        raw = os.read(fd, maximum + 1)
    finally:
        os.close(fd)
    require(len(raw) <= maximum and sha(raw) == expected, "original input hash differs")
    return decode(raw)


def private_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, "wb", closefd=False) as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        os.close(fd)


def endpoint(value):
    parsed = urlsplit(value)
    require(parsed.scheme == "http" and parsed.hostname == "127.0.0.1"
            and parsed.port is not None and 0 < parsed.port <= 65535
            and not parsed.username and not parsed.password and not parsed.query
            and not parsed.fragment and parsed.path in ("", "/"),
            "explicit owned IPv4 loopback HTTP endpoint required")
    return parsed.hostname, parsed.port


def mcp_response(raw, content_type, request_id):
    """One exact JSON-RPC response; bounded SSE is only this MCP envelope."""
    media = content_type.split(";", 1)[0].strip().lower()
    if media == "application/json":
        value = decode(raw)
    elif media == "text/event-stream":
        require(raw and b"\r" not in raw.replace(b"\r\n", b""), "invalid MCP SSE line endings")
        normalized = raw.replace(b"\r\n", b"\n")
        require(normalized.endswith(b"\n\n"), "truncated MCP SSE final event")
        values, fields, events = [], [], 0
        for line in normalized.split(b"\n"):
            require(len(line) <= 262144, "MCP SSE line exceeds original search response bound")
            if not line:
                if fields:
                    events += 1
                    require(events <= 16, "MCP SSE event count exceeded")
                    data, names = [], set()
                    for field, payload in fields:
                        if field == b"data":
                            data.append(payload)
                        elif field == b"event":
                            require(field not in names and payload == b"message", "unexpected MCP SSE event")
                            names.add(field)
                        elif field == b"id":
                            require(field not in names and 0 < len(payload) <= 256
                                    and all(32 <= byte < 127 for byte in payload), "invalid MCP SSE event identity")
                            names.add(field)
                    require(data, "MCP SSE event has no original JSON response")
                    values.append(decode(b"\n".join(data)))
                    fields = []
                continue
            if line.startswith(b":"):
                continue
            require(b":" in line and len(fields) < 128, "invalid bounded MCP SSE field")
            key, payload = line.split(b":", 1)
            require(key in (b"data", b"event", b"id"), "unsupported MCP SSE field")
            fields.append((key, payload[1:] if payload.startswith(b" ") else payload))
        require(not fields and len(values) == 1, "MCP requires exactly one complete original response")
        value = values[0]
    else:
        raise Unmet("MCP response must be explicit JSON or bounded SSE")
    require(type(value) is dict and value.get("jsonrpc") == "2.0"
            and type(value.get("id")) is type(request_id) and value.get("id") == request_id
            and set(value) in ({"jsonrpc", "id", "result"}, {"jsonrpc", "id", "error"}),
            "MCP response identity or exact result/error envelope differs")
    return value


async def response_frame(reader, observed, maximum, *, mcp_request_id=None):
    """Preserve bounded actual framing/body even on incomplete reads."""
    raw = bytearray()
    header_bytes = 0

    async def line(bound):
        nonlocal header_bytes
        data = await reader.readline()
        header_bytes += len(data)
        require(data.endswith(b"\r\n") and len(data) <= bound and header_bytes <= 65536,
                "invalid or incomplete HTTP line")
        return data

    async def exact(count):
        require(0 <= count <= maximum - len(raw), "response body exceeds bound")
        remaining = count
        while remaining:
            data = await reader.read(min(65536, remaining))
            require(bool(data), "truncated response body")
            raw.extend(data)
            observed["raw_body"] = bytes(raw)
            remaining -= len(data)

    first = await line(8192)
    observed["first_response_ns"] = time.monotonic_ns()
    require(re.fullmatch(rb"HTTP/1\.[01] [1-5][0-9]{2} [^\r\n]*\r\n", first) is not None,
            "invalid HTTP status line")
    observed["status"] = int(first.split(b" ", 2)[1])
    headers = []
    while True:
        row = await line(8192)
        if row == b"\r\n":
            break
        require(len(headers) < 128 and not row.startswith((b" ", b"\t")) and b":" in row,
                "invalid bounded HTTP headers")
        key, value = row[:-2].split(b":", 1)
        require(re.fullmatch(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+", key) is not None,
                "invalid HTTP field name")
        value = value.strip(b" \t")
        require(all(byte == 9 or 32 <= byte < 127 for byte in value), "invalid HTTP field value")
        headers.append((key.decode("ascii").lower(), value.decode("ascii")))
    observed["headers"] = headers
    lengths = [value for key, value in headers if key == "content-length"]
    encodings = [value.lower() for key, value in headers if key == "transfer-encoding"]
    require(len(lengths) <= 1 and len(encodings) <= 1 and not (lengths and encodings),
            "ambiguous response framing")
    if lengths:
        value = lengths[0]
        require(value.isascii() and value.isdecimal() and len(value) <= 8, "invalid body length")
        await exact(int(value))
    elif encodings == ["chunked"]:
        chunks = 0
        while True:
            size = (await line(128))[:-2]
            require(re.fullmatch(rb"[0-9a-fA-F]{1,8}", size) is not None, "invalid chunk size")
            count = int(size, 16)
            if count == 0:
                require(await line(8192) == b"\r\n", "unexpected response trailers")
                break
            chunks += 1
            require(chunks <= 65536, "response chunk count limit")
            await exact(count)
            require(await reader.readexactly(2) == b"\r\n", "incomplete chunk delimiter")
    elif observed["status"] in (204, 304):
        pass
    else:
        raise Unmet("response has no bounded framing")
    require(await reader.read(1) == b"", "trailing HTTP bytes on close-only connection")
    observed["framing_complete"] = True
    if mcp_request_id is not None and observed["status"] == 200:
        types = [value for key, value in headers if key == "content-type"]
        require(len(types) == 1, "MCP requires one explicit response media type")
        observed["body"] = mcp_response(bytes(raw), types[0], mcp_request_id)
    else:
        observed["body"] = decode(bytes(raw)) if raw else None


class Capture:
    def __init__(self, directory, maximum):
        self.directory = Path(directory)
        require(self.directory.is_absolute() and self.directory == self.directory.resolve()
                and not self.directory.exists(), "new canonical private capture directory required")
        self.directory.mkdir(mode=0o700)
        self.maximum, self.written, self.held = maximum, 0, 0

    def reserve(self, maximum):
        require(maximum > 0 and self.written + self.held + maximum <= self.maximum,
                "original capture budget exhausted")
        self.held += maximum

    def save(self, identifier, request, observed, reservation):
        raw = observed.pop("raw_body", b"")
        observed["response_body_sha256"] = sha(raw)
        observed["response_body_bytes"] = len(raw)
        metadata = encode(observed)
        size = len(request) + len(raw) + len(metadata)
        require(size <= reservation, "reserved capture capacity exceeded")
        private_new(self.directory / f"{identifier:06}.request.json", request)
        private_new(self.directory / f"{identifier:06}.response.bin", raw)
        private_new(self.directory / f"{identifier:06}.exchange.json", metadata)
        self.held -= reservation
        self.written += size


def validate_owner_request(method, path, body):
    """Fixed original startup/export operations; no arbitrary global route/tool."""
    if method == "GET" and path in ("/v1/health", "/v1/readiness"):
        require(body is None, "owner status request cannot contain a body")
        return
    if method == "POST" and path == "/v1/system/snapshot/export":
        require(type(body) is dict
                and set(body) == {"path", "name", "retention_seconds", "quiesce_timeout_seconds"}
                and type(body["name"]) is str
                and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", body["name"]) is not None
                and body["path"] == body["name"]
                and type(body["retention_seconds"]) is int and body["retention_seconds"] == 3600
                and type(body["quiesce_timeout_seconds"]) is int
                and 1 <= body["quiesce_timeout_seconds"] <= 30,
                "exact bounded original owner snapshot export required")
        return
    require(method == "POST" and path == "/mcp/full" and type(body) is dict,
            "owner operation is outside fixed startup/export routes")
    params = body.get("params")
    if body.get("method") == "initialize":
        require(params == {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "full-workload-runtime", "version": "1"}},
                "exact original runtime owner initialization required")
        return
    require(body.get("method") == "tools/call" and type(params) is dict
            and set(params) == {"name", "arguments"} and type(params["arguments"]) is dict,
            "exact original owner runtime tool required")
    arguments = params["arguments"]
    if params["name"] == "initialize_runtime":
        require(set(arguments) == {"ingest_enabled", "extractor_required", "multimodal_asset_root",
                                   "backend", "snapshot_path"}
                and arguments["ingest_enabled"] is False and arguments["extractor_required"] is False
                and arguments["backend"] == "elephant-db",
                "exact original embedded initialization required")
        paths = (arguments["multimodal_asset_root"], arguments["snapshot_path"])
    else:
        require(params["name"] == "configure_runtime", "owner runtime tool is not allowed")
        if set(arguments) == {"ingest_enabled", "extractor_required"}:
            require(arguments["ingest_enabled"] is True and arguments["extractor_required"] is False,
                    "exact original worker activation required")
            paths = ()
        else:
            require(set(arguments) == {"ingest_enabled", "extractor_required", "multimodal_asset_root"}
                    and arguments["ingest_enabled"] is False and arguments["extractor_required"] is False,
                    "exact original owner runtime configuration required")
            paths = (arguments["multimodal_asset_root"],)
    require(all(type(value) is str and 0 < len(value.encode()) <= 4096
                and value.startswith("/") and str(Path(value)) == value
                and ".." not in Path(value).parts and all(ord(char) >= 32 for char in value)
                for value in paths), "original absolute runtime paths required")


class Client:
    def __init__(self, address, tokens, capture, limits, deadline, expires, clock=time.monotonic):
        self.host, self.port = endpoint(address)
        self.tokens, self.capture, self.limits = tokens, capture, limits
        require(all(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token)
                    for token in tokens.values()), "signed synthetic JWTs required")
        self.deadline, self.expires, self.clock = deadline, expires, clock
        self.count, self.sent_active, self.peak_sent = 0, 0, 0
        self.rows = []
        self._paired_address, self._paired_target, self._paired_deadline = None, None, None

    def bind_paired_health(self, address):
        """Bind the owner's reviewed endpoint once, before its original startup."""
        require(self._paired_target is None, "original paired health endpoint already bound")
        require(self.clock() < self.deadline and time.time() < self.expires, "original run deadline expired")
        target = endpoint(address)
        self._paired_address, self._paired_target = address, target
        self._paired_deadline = min(self.deadline, self.clock() + 30)

    async def paired_health(self, address):
        require(self._paired_target is not None and address == self._paired_address,
                "exact originally bound paired health endpoint required")
        return await self._request("GET", "/health", None, None, role=None,
                                   response_limit=4096, paired_health=True)

    async def request(self, method, path, space, body=None, *, role, response_limit=None, idempotency=None):
        require(type(space) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", space) is not None
                and role not in ("runtime:inspect", "runtime:operator") and role in self.tokens,
                "explicit named-space credential required")
        return await self._request(method, path, space, body, role=role,
                                   response_limit=response_limit, idempotency=idempotency)

    async def runtime_request(self, body, response_limit=None):
        """Only the original inspector's initialization and read-only runtime tool."""
        return await self._request("POST", "/mcp/full", None, body,
                                   role="runtime:inspect", response_limit=response_limit)

    async def owner_request(self, method, path, body=None, *, response_limit=None):
        return await self._request(method, path, None, body, role="runtime:operator",
                                   response_limit=response_limit, owner=True)

    async def _request(self, method, path, space, body, *, role, response_limit=None, idempotency=None,
                       paired_health=False, owner=False):
        mcp = method == "POST" and path == "/mcp/full"
        require(not (paired_health and owner), "distinct original owner and paired request required")
        if owner:
            require(space is None and role == "runtime:operator" and role in self.tokens and idempotency is None,
                    "original explicit runtime operator credential required")
            validate_owner_request(method, path, body)
        if paired_health:
            require(method == "GET" and path == "/health" and space is None and body is None
                    and role is None and idempotency is None and self._paired_target is not None,
                    "only original paired health inspection is permitted")
        require(method in ("GET", "POST", "PUT", "DELETE") and (path.startswith("/v1/") or mcp or paired_health)
                and re.fullmatch(r"[\x21-\x7e]+", path) and not path.startswith("//"), "invalid public request target")
        if mcp:
            require(type(body) is dict and body.get("jsonrpc") == "2.0"
                    and type(body.get("id")) is int and 0 < body["id"] <= 8000
                    and body.get("method") in ("initialize", "tools/call")
                    and set(body) == {"jsonrpc", "id", "method", "params"}, "explicit bounded original MCP request required")
        if paired_health:
            target_host, target_port = self._paired_target
            request_deadline = min(self.deadline, self._paired_deadline)
        else:
            target_host, target_port = self.host, self.port
            request_deadline = self.deadline
        if space is None and not paired_health and not owner:
            require(mcp and role == "runtime:inspect" and role in self.tokens and idempotency is None,
                    "original explicit runtime inspection credential required")
            params = body["params"]
            if body["method"] == "initialize":
                require(params == {"protocolVersion": "2025-03-26", "capabilities": {},
                                   "clientInfo": {"name": "full-workload-runtime-inspector", "version": "1"}},
                        "exact original runtime inspector initialization required")
            else:
                require(params == {"name": "get_runtime_config", "arguments": {}},
                        "only argument-free get_runtime_config is permitted globally")
        elif not paired_health and not owner:
            require(type(space) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", space) is not None
                    and role not in ("runtime:inspect", "runtime:operator") and role in self.tokens,
                    "explicit named-space credential required")
        require(self.clock() < request_deadline and time.time() < self.expires, "original run deadline expired")
        require(self.count < self.limits["public_http_calls_per_backend"], "original HTTP request cap exhausted")
        payload = encode(body) if body is not None else b""
        require(len(payload) <= self.limits["request_body_bytes"], "request body exceeds reviewed bound")
        maximum = response_limit or self.limits["raw_source_response_bytes"]
        require(0 < maximum <= self.limits["raw_source_response_bytes"], "response limit exceeds original cap")
        if idempotency is not None:
            require(re.fullmatch(r"[A-Za-z0-9_-]{1,128}", idempotency) is not None,
                    "invalid original idempotency key")
        reservation = len(payload) + 3 * maximum + 131072
        self.capture.reserve(reservation)
        self.count += 1
        identifier = self.count
        observed = {"id": identifier, "method": method, "path": path, "space": space,
                    "credential_role": role,
                    "credential_sha256": None if paired_health else sha(self.tokens[role].encode()),
                    "request_sha256": sha(payload), "started_ns": time.monotonic_ns(),
                    "raw_body": b"", "framing_complete": False, "body": None, "status": None}
        if paired_health:
            observed["endpoint"] = self._paired_address
        extra = ""
        if idempotency is not None:
            extra = f"Idempotency-Key: {idempotency}\r\n"
        accept = "application/json, text/event-stream" if mcp else "application/json"
        if mcp:
            extra += "MCP-Protocol-Version: 2025-03-26\r\n"
        space_header = f"X-Space-Id: {space}\r\n" if space is not None else ""
        authorization = "" if paired_health else f"Authorization: Bearer {self.tokens[role]}\r\n"
        wire = (f"{method} {path} HTTP/1.1\r\nHost: {target_host}:{target_port}\r\n"
                f"{authorization}{space_header}"
                f"Content-Type: application/json\r\nAccept: {accept}\r\n{extra}"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n").encode() + payload
        writer, sent = None, False
        cancelled = False
        try:
            async with asyncio.timeout(min(self.limits["request_seconds"], request_deadline - self.clock())):
                reader, writer = await asyncio.open_connection(target_host, target_port, limit=65536)
                observed["connected_ns"] = time.monotonic_ns()
                writer.write(wire)
                await writer.drain()
                observed["client_write_complete_ns"] = time.monotonic_ns()
                self.sent_active += 1
                self.peak_sent = max(self.peak_sent, self.sent_active)
                sent = True
                await response_frame(reader, observed, maximum, mcp_request_id=body["id"] if mcp else None)
        except (Exception, asyncio.CancelledError) as error:
            observed["error_type"] = type(error).__name__
            cancelled = isinstance(error, asyncio.CancelledError)
        finally:
            if sent:
                self.sent_active -= 1
            if writer is not None:
                writer.close()
                try:
                    async with asyncio.timeout(0.1):
                        await writer.wait_closed()
                except (Exception, asyncio.CancelledError):
                    observed["connection_cleanup_failed"] = True
            observed["ended_ns"] = time.monotonic_ns()
            self.capture.save(identifier, payload, observed, reservation)
            self.rows.append(observed)
        if cancelled:
            raise asyncio.CancelledError
        return observed
