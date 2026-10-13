"""Owned startup processes and one-attempt bounded public captures.

Callers must validate the independent launch context before constructing these
objects. Importing this module does not execute a candidate or open a socket.
"""
from __future__ import annotations

import asyncio
import base64
import datetime as dt
import importlib.util
import os
from pathlib import Path
import re
import signal
import socket
import stat
import sys
import threading
import time

from runtime_package_bootstrap_contract import D28_WHOLE_SECONDS, PRIMARY_BACKEND_SECONDS, LIMITS, encode, fields, private_path, process_identity, require, sha, strict_json, utc

ROOT = Path(__file__).resolve().parents[2]
DEPENDENCIES = {
    "scripts/qualification/runtime_package_bootstrap.py",
    "scripts/qualification/runtime_package_bootstrap_contract.py",
    "scripts/qualification/runtime_package_bootstrap_io.py",
    "scripts/qualification/runtime_package_bootstrap_custody.py",
    "scripts/qualification/runtime_package_bootstrap_evidence.py",
    "scripts/qualification/primary_provider_driver.py",
    "scripts/qualification/primary_provider_driver_contract.py",
    "scripts/qualification/primary_provider_driver_transport.py",
    "scripts/qualification/primary_provider_driver_startup.py",
    "scripts/qualification/primary_provider_driver_runtime.py",
    "tests/recall_preview_semantics/gateway/launch_context.py",
    "tests/recall_preview_semantics/workload_profiles.py",
    "scripts/qualification/recall_workload.py", "scripts/qualification/installed_linux_offline.py",
    "scripts/qualification/installed_process_capture.py",
    "scripts/qualification/recall_preview.py", "scripts/release/artifact.py",
    "tests/recall_preview/fixtures.py", "tests/recall_preview/matrix.py", "tests/recall_preview/oracle.py",
}


def read_bound(path, maximum=16777216, *, private=False):
    path = Path(path)
    require(path.is_absolute() and path.resolve() == path, "canonical absolute file required")
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK
    descriptor = os.open(path, flags)
    with os.fdopen(descriptor, "rb") as stream:
        info = os.fstat(stream.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_size <= maximum
                and (not private or (info.st_uid == os.getuid() and info.st_mode & 0o077 == 0)), "regular bounded private file required")
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "input grew beyond bound")
    return raw


def file_hash(path):
    # Runtime is an immutable read-only mount. Do not load an entire executable.
    import hashlib
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1048576), b""):
            result.update(chunk)
    return result.hexdigest()


def private_new(path, raw):
    private_path(str(path))
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def read_reference(reference, maximum=16777216):
    raw = read_bound(reference["path"], maximum, private=True)
    require(sha(raw) == reference["sha256"], "reviewed handoff bytes changed")
    return strict_json(raw)


def reviewed_module(name, path, expected):
    """Execute the exact reviewed source bytes, without a second loader read."""
    raw = read_bound(path, 8 * 1024**2)
    require(sha(raw) == expected, "reviewed packaged helper source differs")
    spec = importlib.util.spec_from_file_location(name, path)
    require(spec is not None and spec.loader is not None, "reviewed source module metadata unavailable")
    module = importlib.util.module_from_spec(spec)
    exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


def load_helpers(plan):
    require(DEPENDENCIES <= set(plan["dependencies"]), "required transitive source identity is absent")
    for name, expected in plan["dependencies"].items():
        path = ROOT / name
        require(path.resolve() == path and not path.is_symlink() and file_hash(path) == expected,
                "reviewed packaged helper differs: " + name)
    path = ROOT / "scripts/qualification/recall_workload.py"
    module = reviewed_module("bootstrap_reviewed_wire", path,
                             plan["dependencies"]["scripts/qualification/recall_workload.py"])
    # The actual S loader owns OwnedProcess and the D framing/parser dependency.
    # Check every loaded source module under /opt/recall against the reviewed map.
    for value in tuple(sys.modules.values()) + (module, module.helpers):
        source = getattr(value, "__file__", None)
        if source and Path(source).is_relative_to(ROOT):
            name = Path(source).relative_to(ROOT).as_posix()
            require(name in plan["dependencies"] and file_hash(source) == plan["dependencies"][name],
                    "unreviewed transitive startup helper")
    return module


def verify_isolation(proof):
    fields(proof, {"format", "network_namespace", "parent_network_namespace", "interfaces",
                   "ipv4_routes", "provider_credentials_present"})
    require(proof["format"] == "recall-gateway-isolation/v1"
            and proof["network_namespace"] != proof["parent_network_namespace"]
            and proof["network_namespace"] == os.readlink("/proc/self/ns/net")
            and proof["interfaces"] == ["lo"] and proof["ipv4_routes"] == 0
            and proof["provider_credentials_present"] is False, "actual gateway namespace evidence differs")
    interfaces = {line.split(":", 1)[0].strip() for line in read_bound(Path("/proc/self/net/dev").resolve(), 32768).decode().splitlines() if ":" in line}
    require(interfaces == {"lo"} and len(read_bound(Path("/proc/self/net/route").resolve(), 32768).decode().splitlines()) <= 1,
            "current private namespace gained an interface or route")
    return proof


class Allocation:
    def __init__(self, started_ns, deadline_ns, whole_deadline_ns, expires_at, *, maximum_seconds=PRIMARY_BACKEND_SECONDS):
        require(type(maximum_seconds) is int and maximum_seconds in (PRIMARY_BACKEND_SECONDS, D28_WHOLE_SECONDS),
                "unreviewed startup duration ceiling")
        now = time.monotonic_ns()
        require(all(type(v) is int for v in (started_ns, deadline_ns, whole_deadline_ns))
                and 0 < started_ns <= now < deadline_ns <= whole_deadline_ns
                and deadline_ns - started_ns <= maximum_seconds * 10**9, "original backend allocation is invalid")
        self.started_ns, self.deadline_ns, self.whole_deadline_ns = started_ns, deadline_ns, whole_deadline_ns
        self.expiry = utc(expires_at)
        self.maximum_seconds = maximum_seconds
        self.bootstrap_deadline_ns = min(deadline_ns - LIMITS["cleanup_seconds"] * 10**9,
                                         now + LIMITS["bootstrap_seconds"] * 10**9)
        self.bootstrap = True

    def remaining(self, *, cleanup=False):
        now = time.monotonic_ns()
        wall = (self.expiry - dt.datetime.now(dt.timezone.utc)).total_seconds()
        deadline = min(self.deadline_ns, self.whole_deadline_ns)
        if not cleanup:
            deadline -= LIMITS["cleanup_seconds"] * 10**9
            if self.bootstrap:
                deadline = min(deadline, self.bootstrap_deadline_ns)
        remaining = min((deadline - now) / 10**9, wall)
        require(remaining > 0, "original startup/run allocation expired")
        return remaining


class Processes:
    def __init__(self, helper, root, allocation, *, capture_module=None, reserve_capture=None):
        self.helper, self.root, self.allocation = helper, root, allocation
        self.items, self.records = [], []
        self.capture_module = capture_module
        self.capture_journal = None if capture_module is None else capture_module.CaptureJournal(
            root, reserve=reserve_capture)

    def launch(self, binary, arguments, environment, *, purpose, cwd):
        self.allocation.remaining()
        require(len(self.items) < LIMITS["processes"], "startup process count exhausted")
        require(str(binary) in ("/opt/recall/bin/elephant", "/opt/recall/bin/recall-surreal", "/usr/bin/python3"),
                "unreviewed executable path")
        number = len(self.items) + 1
        logs = [self.root / "logs" / f"{number:02}.{suffix}" for suffix in ("stdout", "stderr")]
        record = {"index": number, "purpose": purpose, "command": [str(binary), *arguments],
                  "cwd": str(cwd), "environment_names": sorted(environment),
                  "logs": [str(path) for path in logs], "started_monotonic_ns": time.monotonic_ns(),
                  "exit_code": None}
        # Post-spawn initialization failure must return the original owned
        # handle even if the helper already reaped it. Keep failure cleanup
        # inside both the original deadline and a bounded five-second ceiling.
        proof = None
        if self.capture_module is not None:
            proof = self.capture_module.ProcessProof(binary, arguments, cwd, environment, logs,
                self.allocation.deadline_ns, journal=self.capture_journal, index=number)
        self.allocation.remaining()
        deadline = time.monotonic() + min(5, self.allocation.remaining(cleanup=True))
        if self.capture_module is not None:
            deadline = min(deadline, self.capture_module.deadline_seconds(self.allocation.deadline_ns))
        try:
            capture = {} if proof is None else {"capture_proof": proof}
            child = self.helper.OwnedProcess(binary, arguments, cwd, environment, logs,
                LIMITS["log_bytes_per_stream"], initialization_deadline=deadline, **capture)
        except BaseException as error:
            child = getattr(error, "owned_process", None)
            if child is not None:
                record.update(pid=child.pid, initialization_error_type=type(error).__name__,
                              initialization_cleanup_failed=bool(getattr(error, "owned_cleanup_errors", ())))
                self.items.append(child)
                self.records.append(record)
            raise
        record["pid"] = child.pid
        self.items.append(child)
        self.records.append(record)
        return child

    async def wait(self, child, seconds):
        bound = min(seconds, self.allocation.remaining())
        code = await asyncio.to_thread(child.wait, bound)
        self.records[self.items.index(child)]["exit_code"] = code
        require(code == 0, "owned startup command failed")
        return self.items.index(child) + 1

    def identity(self, child, executable, expected_sha):
        require(child.poll() is None, "owned service exited before handoff")
        actual = os.readlink(f"/proc/{child.pid}/exe")
        require(actual == executable and file_hash(Path(actual)) == expected_sha,
                "running process executable differs from reviewed binary")
        raw = read_bound(Path(f"/proc/{child.pid}/stat"), 8192).decode()
        return process_identity(raw, child.pid, actual, expected_sha)

    def command_receipt(self):
        records = []
        for child, original in zip(self.items, self.records):
            record = dict(original)
            if child.reaped:
                record["log_sha256"] = {path: file_hash(Path(path)) for path in record["logs"]}
            else:
                record["log_sha256"] = None  # Live logs are not final evidence.
            records.append(record)
        value = {"format": "recall-runtime-process-commands/v1", "processes": records}
        raw = encode(value)
        require(len(raw) <= 262144, "process command capture exceeds bound")
        path = self.root / "process-commands.json"
        private_new(path, raw)
        return {"path": str(path), "sha256": sha(raw)}

    async def cleanup(self):
        failures = []

        def failed(record, stage, error_type):
            failures.append({"index": record["index"], "stage": stage, "error_type": error_type})

        def remaining(record, maximum):
            try:
                return min(maximum, self.allocation.remaining(cleanup=True))
            except ValueError:
                failed(record, "original-cleanup-deadline", "DeadlineExpired")
                # Signals and a nonblocking reap may still release an owned
                # child. An expired deadline never grants a fresh wait period.
                return 0

        def signal_owned(child, record, kind):
            try:
                child.signal(kind)
                return True
            except Exception as error:
                failed(record, "owned-signal-" + str(kind), type(error).__name__)
                return False

        for child, record in reversed(list(zip(self.items, self.records))):
            if record.get("initialization_error_type"):
                failed(record, "owned-process-initialization", record["initialization_error_type"])
            if child.reaped:
                continue
            if signal_owned(child, record, signal.SIGTERM):
                try:
                    # Keep the final owned reap inside the same remaining
                    # allowance, including the shared helper's log draining.
                    grace = max(0, min(62, remaining(record, 67) - 5))
                    record["exit_code"] = await asyncio.to_thread(child.wait, grace)
                except (Exception, asyncio.CancelledError) as error:
                    failed(record, "graceful-owned-wait", type(error).__name__)
            if not child.reaped:
                failed(record, "forced-owned-termination", "IncompleteGracefulShutdown")
                if signal_owned(child, record, signal.SIGKILL):
                    try:
                        record["exit_code"] = await asyncio.to_thread(child.wait, remaining(record, 5))
                    except (Exception, asyncio.CancelledError) as error:
                        failed(record, "owned-final-reap", type(error).__name__)
                # Failed group cleanup must retain the leader's reserved PID.
                # Reaping it here could release a still-live descendant group
                # from our original process identity. Continue other handles.
            if not child.reaped:
                failed(record, "owned-final-reap", "UnreapedOwnedChild")
            if record["exit_code"] is not None and record["exit_code"] != 0:
                failed(record, "owned-service-exit", "NonzeroExit")
        for child, record in zip(self.items, self.records):
            record["log_sha256"] = None
            if not child.reaped or any(thread.is_alive() for thread in child.threads):
                failed(record, "owned-final-logs", "UnfinishedCapture")
                continue
            try:
                if remaining(record, 1) > 0:
                    record["log_sha256"] = {path: file_hash(Path(path)) for path in record["logs"]}
            except Exception as error:
                failed(record, "owned-final-logs", type(error).__name__)
        return failures


class PublicCapture:
    def __init__(self, wire, root, allocation):
        self.wire, self.root, self.allocation = wire, root, allocation
        self.tokens = {}
        self.captures = []
        self.count = self.total_bytes = 0
        self.budget_lock = threading.RLock()

    def reserve_capture(self, amount):
        """Charge original HTTP and shared process-proof bytes to one allocation."""
        with self.budget_lock:
            require(type(amount) is int and amount >= 0
                    and self.total_bytes + amount <= LIMITS["capture_bytes"], "startup capture budget exhausted")
            self.total_bytes += amount

    async def request(self, method, path, *, role=None, space=None, body=None, port=43130, mcp=False):
        require(method in ("GET", "POST", "PUT") and re.fullmatch(r"/[\x21-\x7e]*", path)
                and "#" not in path and port in (43130, 43131), "fixed bounded public route required")
        require(port == 43130 or (method, path, role, space, body) == ("GET", "/health", None, None, None),
                "paired server access is health-only")
        require(space is None or re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", space), "explicit space header invalid")
        require(role is None or role in self.tokens, "credential role is unknown")
        payload = b"" if body is None else encode(body)
        require(len(payload) <= LIMITS["request_bytes"] and self.count < LIMITS["requests"]
                and self.total_bytes + LIMITS["response_bytes"] * 3 < LIMITS["capture_bytes"], "startup capture budget exhausted")
        timeout = min(LIMITS["request_seconds"], self.allocation.remaining())
        self.count += 1
        number = self.count
        intent = {"request_index": number, "method": method, "path": path, "port": port,
                  "credential_role": role, "space": space, "body": body,
                  "request_body_base64": base64.b64encode(payload).decode(), "body_sha256": sha(payload),
                  "harness_attempt": 1, "timeout_seconds": timeout, "started_monotonic_ns": time.monotonic_ns()}
        raw_intent = encode(intent)
        request_path = self.root / "captures" / f"{number:03}.request.json"
        self.reserve_capture(len(raw_intent))
        private_new(request_path, raw_intent)
        headers = [f"{method} {path} HTTP/1.1", f"Host: 127.0.0.1:{port}", "Content-Type: application/json",
                   "Accept: application/json, text/event-stream" if mcp else "Accept: application/json",
                   f"Content-Length: {len(payload)}", "Connection: close"]
        if role is not None:
            headers.append("Authorization: Bearer " + self.tokens[role])
        if space is not None:
            headers.append("X-Space-Id: " + space)
        if mcp:
            headers.append("MCP-Protocol-Version: 2025-03-26")
        request = ("\r\n".join(headers) + "\r\n\r\n").encode() + payload
        observed = {"raw_body": b"", "framing_complete": False, "status": None, "body": None}
        writer, error_type = None, None
        try:
            async with asyncio.timeout(timeout):
                reader, writer = await asyncio.open_connection("127.0.0.1", port, limit=self.wire.HEADER_CAP)
                writer.write(request)
                await writer.drain()
                try:
                    await self.wire.read_frame(reader, observed)
                except (ValueError, UnicodeError):
                    require(observed["framing_complete"], "incomplete response frame")
                    media = observed.get("response_headers", {}).get("content-type", [])
                    if mcp and len(media) == 1 and media[0].split(";", 1)[0].strip() == "text/event-stream":
                        observed["body"] = self.wire.helpers.mcp_value(observed["raw_body"], body["id"])
                    elif port == 43131 and observed["status"] == 200 and observed["raw_body"] in (b"", b"Ok", b"OK"):
                        observed["body"] = {"paired_health": "actual-success-empty-or-ok-body"}
                    else:
                        raise
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
        result = {**observed, "error_type": error_type, "request_index": number,
                  "raw_body_base64": base64.b64encode(raw).decode(), "body_sha256": sha(raw),
                  "finished_monotonic_ns": time.monotonic_ns()}
        encoded = encode(result)
        self.reserve_capture(len(encoded))
        response_path = self.root / "captures" / f"{number:03}.response.json"
        private_new(response_path, encoded)
        self.captures.append({"request_index": number, "request_path": str(request_path),
                              "response_path": str(response_path), "request_sha256": sha(raw_intent),
                              "response_sha256": sha(encoded)})
        return result

    @staticmethod
    def success(response, status=200):
        require(response["status"] == status and response["framing_complete"] is True
                and response["error_type"] is None and response["body"] is not None,
                "actual startup request failed or capture is incomplete")
        return response["body"]

    async def tool(self, name, arguments):
        identifier = self.count + 1
        response = await self.request("POST", "/mcp/full", role="operator", body={
            "jsonrpc": "2.0", "id": identifier, "method": "tools/call", "params": {
                "name": name, "arguments": arguments}}, mcp=True)
        body = self.success(response)
        require(body.get("jsonrpc") == "2.0" and body.get("id") == identifier and "error" not in body,
                "actual MCP response identity or result differs")
        result = body["result"]
        require(result.get("isError") is not True, "actual runtime tool failed")
        texts = [item["text"] for item in result.get("content", []) if item.get("type") == "text"]
        require(len(texts) == 1, "one actual runtime tool payload required")
        value = strict_json(texts[0].encode())
        if type(value) is dict and value.get("ok") is True:
            value = value["data"]
        return value, response["request_index"]


def ensure_ports_absent():
    for port in (43130, 43131):
        with socket.socket() as reserve:
            reserve.bind(("127.0.0.1", port))
    # Binding is not reserved across process launch. A race fails subsequent
    # owned-PID liveness/readiness checks; no pre-existing service is adopted.
