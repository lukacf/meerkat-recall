#!/usr/bin/env python3
"""Installed Linux qualification driver. Defaults to an inert reviewed plan.

Execution is a separate, explicit allocation. No build, provider, model, Docker,
deployment, or credential discovery is performed. Candidate children run with a
new synthetic signing key and an explicit environment inside an offline network
namespace. Success qualifies only the independently reviewed case inventory.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import hashlib
import http.client
import importlib.util
import json
import math
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
import time
import threading
import types
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
CASES = ROOT / "tests/recall_preview_semantics/installed_linux_cases.json"
D_ROOT = ROOT / "tests/recall_preview"
LOADED_SOURCE_HASHES = {}
MAX_HELPER_SOURCE = 8 * 1024**2
# Only the enclosing reviewed loader may inject this registry before exec. Its
# entries contain the original already-executed modules and reviewed raw bytes.
VERIFIED_DEPENDENCIES = globals().get("VERIFIED_DEPENDENCIES")
DEPENDENCY_NAMES = frozenset(("fixtures", "matrix", "oracle", "installed_d_capture",
                              "installed_release_artifact", "installed_original_process_capture"))


def verified_dependency(name, path, expected_sha256=None):
    registry = VERIFIED_DEPENDENCIES
    if type(registry) is not dict or set(registry) != DEPENDENCY_NAMES or name not in registry:
        raise ValueError("complete original qualification dependency registry required")
    entry = registry[name]
    if type(entry) is not dict or set(entry) != {"module", "path", "source_bytes", "sha256"}:
        raise ValueError("original qualification dependency entry differs")
    result, raw, original_sha = entry["module"], entry["source_bytes"], entry["sha256"]
    exact_path = str(path)
    if (not Path(exact_path).is_absolute() or os.path.normpath(exact_path) != exact_path
            or entry["path"] != exact_path or type(raw) is not bytes or not 0 < len(raw) <= MAX_HELPER_SOURCE
            or type(original_sha) is not str or not re.fullmatch(r"[0-9a-f]{64}", original_sha)
            or hashlib.sha256(raw).hexdigest() != original_sha
            or (expected_sha256 is not None and original_sha != expected_sha256)
            or type(result) is not types.ModuleType or result.__name__ != name
            or getattr(result, "__file__", None) != exact_path
            or getattr(getattr(result, "__spec__", None), "name", None) != name
            or getattr(getattr(result, "__spec__", None), "origin", None) != exact_path):
        raise ValueError("original verified qualification dependency identity differs")
    return result, original_sha


def original_module_source(path: Path):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("canonical qualification dependency path required")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or not 0 < before.st_size <= MAX_HELPER_SOURCE:
            raise ValueError("bounded regular qualification dependency required")
        raw = stream.read(MAX_HELPER_SOURCE + 1)
        after = os.fstat(stream.fileno())
    observed = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    if len(raw) != before.st_size or observed(before) != observed(after):
        raise ValueError("qualification dependency changed during original read")
    return raw, hashlib.sha256(raw).hexdigest()


def module(name: str, path: Path, expected_sha256=None):
    if VERIFIED_DEPENDENCIES is not None:
        result, original_sha = verified_dependency(name, path, expected_sha256)
        if result in LOADED_SOURCE_HASHES and LOADED_SOURCE_HASHES[result] != original_sha:
            raise ValueError("original qualification module bytes cannot be replaced")
        LOADED_SOURCE_HASHES[result] = original_sha
        return result
    raw, original_sha = original_module_source(path)
    if expected_sha256 is not None and original_sha != expected_sha256:
        raise ValueError("reviewed qualification dependency source differs")
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise ValueError("qualification dependency is unavailable")
    result = importlib.util.module_from_spec(spec)
    prior = sys.modules.get(name)
    sys.modules[name] = result
    try:
        exec(compile(raw, str(path), "exec", dont_inherit=True), result.__dict__)
    except BaseException:
        if prior is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = prior
        raise
    LOADED_SOURCE_HASHES[result] = original_sha
    return result


# Consume the actual D parser/deadline and frozen input generator. Their source
# hashes are captured; the driver's observations do not replace D's verdict.
fixtures = module("fixtures", D_ROOT / "fixtures.py")
matrix = module("matrix", D_ROOT / "matrix.py")
oracle = module("oracle", D_ROOT / "oracle.py")

capture = module("installed_d_capture", ROOT / "scripts/qualification/recall_preview.py")
artifact = module("installed_release_artifact", ROOT / "scripts/release/artifact.py")
process_capture = module("installed_original_process_capture", ROOT / "scripts/qualification/installed_process_capture.py")


def require(condition, reason: str):
    if not condition:
        raise ValueError(reason)


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def loaded_digest(value) -> str:
    if VERIFIED_DEPENDENCIES is not None:
        original_sha = LOADED_SOURCE_HASHES.get(value)
        result, reviewed_sha = verified_dependency(value.__name__, Path(value.__file__), original_sha)
        require(result is value and original_sha is not None and original_sha == reviewed_sha,
                "original verified qualification module instance changed")
        return original_sha
    """A plan cannot review current path bytes while running an older import."""
    path = Path(value.__file__)
    _raw, current = original_module_source(path)
    require(LOADED_SOURCE_HASHES.get(value) == current,
            "executed qualification dependency differs from current source")
    return current


def read_json(path: Path):
    return capture.strict_json_body(path.read_bytes())


def utc(value: str) -> dt.datetime:
    result = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(result.tzinfo is not None, "authorization deadline needs a timezone")
    return result


def private_new(path: Path, data: bytes):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def encoded(value) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False).encode()


def plan() -> dict:
    for dependency in (fixtures, matrix, oracle):
        loaded_digest(dependency)
    cases = read_json(CASES)
    corpus = fixtures.corpus()
    require(cases["spaces"] == [row["id"] for row in corpus["spaces"]], "frozen space mapping changed")
    for row in cases["sources"]:
        for field in ("baseline_revision", "replacement_revision"):
            require(sum(source["space"] == row["space"] and source["key"] == row["key"]
                        and source["revision"] == row[field] for source in corpus["sources"]) == 1,
                    "frozen source mapping is absent or ambiguous")
        require(sum(query["id"] == row["query_id"] and query["space"] == row["space"]
                    for query in corpus["queries"]) == 1, "frozen original query is absent")
    return {"format": "recall-installed-linux-plan/v1", "expectations": cases,
            "expectations_sha256": file_digest(CASES), "corpus_sha256": fixtures.corpus_sha256(),
            "driver_sha256": file_digest(Path(__file__)),
            "collector_sha256": loaded_digest(capture),
            "installer_sha256": loaded_digest(artifact),
            "process_capture_sha256": loaded_digest(process_capture),
            "execution": "not authorized by a plan", "product_acceptance": False}


def validate_manifest(path: Path, now: dt.datetime | None = None) -> tuple[dict, dict]:
    run = read_json(path)
    require(run.get("format") == "recall-installed-linux-run/v1", "unknown run manifest")
    require(run.get("execution_authorized") is True, "execution has not been allocated")
    require(run.get("backend") in ("elephant-db", "surreal-db"), "explicit supported native backend required")
    for key, size in (("source_commit", 40), ("archive_sha256", 64), ("binary_sha256", 64),
                      ("expectation_review_sha256", 64)):
        require(isinstance(run.get(key), str) and re.fullmatch(f"[0-9a-f]{{{size}}}", run[key]),
                f"exact trusted {key} required")
    now = now or dt.datetime.now(dt.timezone.utc)
    require(now < utc(run["expires_at"]), "execution allocation expired")
    for name in ("archive", "run_directory", "expectation_review"):
        require(Path(run[name]).is_absolute(), f"{name} must be absolute")
    review_path = Path(run["expectation_review"])
    require(file_digest(review_path) == run["expectation_review_sha256"], "independent review digest mismatch")
    reviewed = read_json(review_path)
    current = plan()
    require(reviewed.get("format") == "recall-installed-linux-review/v1"
            and reviewed.get("approved") is True, "independent expectations are not approved")
    for key in ("expectations_sha256", "corpus_sha256", "driver_sha256", "collector_sha256", "installer_sha256", "process_capture_sha256"):
        require(reviewed.get(key) == current[key], f"reviewed {key} differs")
    require(reviewed.get("reviewer") == "qualification", "independent D reviewer required")
    # These are custody-bound operator records, not cryptographic signatures.
    return run, current


def offline_namespace(parent: str) -> dict:
    require(sys.platform == "linux", "this driver requires Linux")
    current = os.readlink("/proc/self/ns/net")
    require(parent != current and re.fullmatch(r"net:\[\d+\]", parent), "fresh network namespace required")
    interfaces = {name for _, name in socket.if_nameindex()}
    require(interfaces == {"lo"}, "only the isolated loopback interface is allowed")
    routes = Path("/proc/net/route").read_text().splitlines()
    require(len(routes) == 1, "namespace has an IPv4 route")
    # Linux keeps default unreachable IPv6 entries even in a fresh namespace.
    ipv6 = Path("/proc/net/ipv6_route")
    if ipv6.exists():
        require(all(line.split()[-1] == "lo" for line in ipv6.read_text().splitlines()),
                "namespace has an external IPv6 interface")
    return {"network_namespace": current, "parent_network_namespace": parent,
            "interfaces": ["lo"], "external_routes": False,
            "limit": "local kernel observation; not a proof against a compromised host"}


def enable_loopback():
    # unshare --user --map-root-user --net grants only namespace-local NET_ADMIN.
    import fcntl
    import struct
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
        flags = fcntl.ioctl(control.fileno(), 0x8913, struct.pack("16sh", b"lo", 0))
        current = struct.unpack("16sh", flags[:18])[1]
        fcntl.ioctl(control.fileno(), 0x8914, struct.pack("16sh", b"lo", current | 1))


def mcp_value(raw: bytes, request_id: int) -> dict:
    try:
        value = capture.strict_json_body(raw)
    except (ValueError, UnicodeError):
        # A completed transport may contain SSE instead of a JSON RPC body.
        require(raw.replace(b"\r\n", b"\n").endswith(b"\n\n"), "MCP SSE event is incomplete")
        values = []
        for event in raw.replace(b"\r\n", b"\n").split(b"\n\n"):
            lines = [line[5:].lstrip(b" ") for line in event.split(b"\n") if line.startswith(b"data:")]
            if lines:
                values.append(capture.strict_json_body(b"\n".join(lines)))
        matches = [value for value in values if isinstance(value, dict) and value.get("id") == request_id]
        require(len(matches) == 1, "MCP SSE response identity absent or ambiguous")
        value = matches[0]
    require(isinstance(value, dict) and type(value.get("id")) is int and value["id"] == request_id,
            "MCP response identity mismatch")
    require(value.get("jsonrpc") == "2.0", "MCP response protocol version missing")
    return value


def typed_denial(body, code, secrets=()):
    require(isinstance(body, dict) and set(body) == {"error_code", "message", "retryable"},
            "denied response contains an unexpected payload")
    require(body["error_code"] == code and body["retryable"] is False,
            "denied response classification differs")
    require(isinstance(body["message"], str) and len(body["message"]) <= 512,
            "denied response message is unbounded")
    wire = encoded(body).decode()
    canaries = [source["text"] for source in fixtures.corpus()["sources"]]
    require(all(value not in wire for value in [*canaries, *secrets] if value),
            "denied response disclosed private fixture input")


def mcp_response_result(response, request_id):
    """Require one successful transport response for this exact RPC request."""
    require(type(response) is dict and set(response) == {"status", "body"}
            and type(response["status"]) is int and response["status"] == 200,
            "MCP response transport differs")
    body = response["body"]
    require(type(body) is dict and set(body) == {"jsonrpc", "id", "result"}
            and body["jsonrpc"] == "2.0" and type(body["id"]) is int
            and type(request_id) is int and body["id"] == request_id
            and type(body["result"]) is dict, "MCP response envelope differs")
    return body["result"]


def mcp_typed_denial(response, request_id, code, secrets=()):
    """An HTTP 200 tool error is a denial only with this exact safe payload."""
    result = mcp_response_result(response, request_id)
    require(set(result) == {"content", "isError"} and result["isError"] is True,
            "MCP denied tool returned success or unexpected data")
    content = result["content"]
    require(type(content) is list and len(content) == 1 and type(content[0]) is dict
            and set(content[0]) == {"type", "text"} and content[0]["type"] == "text"
            and type(content[0]["text"]) is str, "MCP denial must contain one typed text block")
    body = capture.strict_json_body(content[0]["text"].encode())
    typed_denial(body, code, secrets)
    return body


def mcp_public_status_only(response, request_id, secrets=()):
    """Anonymous discovery may describe only the existing public status tool."""
    result = mcp_response_result(response, request_id)
    require(set(result) == {"tools"} and type(result["tools"]) is list
            and len(result["tools"]) == 1, "anonymous MCP discovery exposed other tools or data")
    tool = result["tools"][0]
    require(type(tool) is dict and set(tool) == {"name", "description", "inputSchema"}
            and tool["name"] == "get_runtime_status" and type(tool["description"]) is str
            and 0 < len(tool["description"]) <= 512 and tool["inputSchema"] == {},
            "anonymous MCP discovery differs from public status-only schema")
    wire = encoded(result).decode()
    canaries = [source["text"] for source in fixtures.corpus()["sources"]]
    require(all(value not in wire for value in [*canaries, *secrets] if value),
            "anonymous MCP discovery disclosed private fixture input")


class OwnedProcess:
    """Drain bounded private logs without imposing a file-size cap on storage."""
    def __init__(self, binary, arguments, cwd, environment, log_paths, cap, *, initialization_deadline=None,
                 capture_proof=None, stream_limits=None):
        if initialization_deadline is None:
            initialization_deadline = time.monotonic() + 5
        require(type(initialization_deadline) in (int, float) and math.isfinite(initialization_deadline)
                and initialization_deadline > time.monotonic(), "process initialization deadline expired")
        self.logs, self.threads = [], []
        self.failed = threading.Event()
        self.group_lock = threading.Lock()
        self.reaped = False
        self.process = None
        self.capture_proof = capture_proof
        self.log_paths = dict()
        try:
            require(stream_limits is None or capture_proof is not None,
                    "explicit output bounds require original process capture")
            output_limits = ((cap, cap) if capture_proof is None
                             else capture_proof.validate_stream_limits(stream_limits, cap))
            if capture_proof is not None:
                require(capture_proof.intent["argv"] == [str(binary), *arguments]
                        and capture_proof.intent["cwd"] == str(cwd)
                        and capture_proof.intent["environment_names"] == sorted(environment)
                        and capture_proof.intent["log_paths"] == [str(path) for path in log_paths]
                        and int(initialization_deadline * 1e9) <= capture_proof.deadline_ns,
                        "original process capture cannot adopt another invocation")
            for path in log_paths:
                self.logs.append(os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"))
                self.log_paths[id(self.logs[-1])] = str(path)
            if capture_proof is not None:
                capture_proof.emit("popen-requested", {"initialization_deadline_ns": int(initialization_deadline * 1e9)})
            self.process = subprocess.Popen([str(binary), *arguments], cwd=cwd, env=environment,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                close_fds=True, start_new_session=True)
            self.pid = self.process.pid
            if capture_proof is not None:
                capture_proof.launched(self.pid)
            for stream, log, maximum in zip((self.process.stdout, self.process.stderr), self.logs, output_limits):
                thread = threading.Thread(target=self.drain, args=(stream, log, maximum), daemon=True)
                # Keep even a partially started thread visible to failure cleanup.
                self.threads.append(thread)
                thread.start()
        except BaseException as error:
            self._record_proof("failed", "initialization", error)
            if self.process is None:
                for log in self.logs:
                    log.close()
            else:
                self.failed.set()
                # Preserve original exception type and custody if bounded cleanup
                # itself fails. The caller can register this original handle.
                error.owned_process = self
                error.owned_cleanup_errors = self._initialization_failure(initialization_deadline)
            raise

    def _record_proof(self, action, *arguments):
        proof = getattr(self, "capture_proof", None)
        if proof is not None:
            try:
                getattr(proof, action)(*arguments)
            except Exception as error:
                proof.failed(action, error)

    @property
    def launch_stat_raw(self):
        return self.capture_proof.launch_stat_raw if self.capture_proof is not None else None

    @property
    def launch_stat_after_raw(self):
        return self.capture_proof.launch_stat_after_raw if self.capture_proof is not None else None

    @property
    def launch_executable(self):
        return self.capture_proof.launch_executable if self.capture_proof is not None else None

    @property
    def launch_binary(self):
        return self.capture_proof.launch_binary if self.capture_proof is not None else None

    def _initialization_failure(self, deadline):
        failures = []
        def attempt(label, action):
            try:
                action()
            except BaseException as error:
                failures.append(label + ":" + type(error).__name__)
        attempt("kill-owned-group", lambda: self.signal(signal.SIGKILL))
        for thread in self.threads:
            if thread.ident is not None:
                attempt("join-log", lambda thread=thread: thread.join(timeout=max(0, deadline - time.monotonic())))
        def reap():
            with self.group_lock:
                # The leader PID is still reserved until this exact wait.
                # A failed signal must retain the leader reservation even when
                # the leader has already exited. Its descendants may still live.
                self._signal_owned_locked(signal.SIGKILL)
                self.process.wait(timeout=max(0, deadline - time.monotonic()))
                self.reaped = True
                self._record_proof("reaped_child", self.process.returncode)
        attempt("reap-owned-leader", reap)
        for index, (stream, log) in enumerate(zip((self.process.stdout, self.process.stderr), self.logs)):
            if index < len(self.threads) and self.threads[index].is_alive():
                failures.append("log-thread-still-running")
                continue  # The drainer owns these streams; avoid a blocking close.
            attempt("close-pipe", stream.close)
            attempt("close-log", log.close)
        return tuple(failures)

    def drain(self, stream, log, cap):
        remaining = cap
        eof, error_type = False, None
        try:
            while chunk := stream.read1(8192):
                log.write(chunk[:remaining])
                if len(chunk) > remaining:
                    self.failed.set()
                    error_type = "LogBoundExceeded"
                    self.signal(signal.SIGKILL)
                    break
                remaining -= len(chunk)
            else:
                eof = True
        except (OSError, ValueError):
            error_type = "LogCaptureError"
            self.failed.set()
            self.signal(signal.SIGKILL)
        finally:
            stream.close()
            log.close()
            path = getattr(self, "log_paths", {}).get(id(log))
            if path is not None:
                self._record_proof("log_closed", path, eof, error_type)

    def _signal_owned_locked(self, kind):
        # Keep the leader unreaped until group cleanup. Its reserved PID makes
        # this process-group number unavailable for reuse by a foreign group.
        if not self.reaped:
            proof = getattr(self, "capture_proof", None)
            phase = "post-exit-descendant-cleanup" if proof is not None and proof.observed_exit is not None else "requested-before-exit-observation"
            self._record_proof("signal", kind, phase, "requested")
            try:
                os.killpg(self.pid, kind)
            except ProcessLookupError:
                self._record_proof("signal", kind, phase, "group-absent")
            except BaseException as error:
                self._record_proof("failed", "signal", error)
                raise
            else:
                self._record_proof("signal", kind, phase, "sent")

    def signal(self, kind):
        with self.group_lock:
            self._signal_owned_locked(kind)

    def poll(self):
        with self.group_lock:
            if self.reaped:
                return self.process.returncode
            event = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            if event is None:
                return None
            code = event.si_status if event.si_code == os.CLD_EXITED else -event.si_status
            self._record_proof("exit_observed", code, event.si_status, event.si_code)
            return code

    def wait(self, timeout):
        require(type(timeout) in (int, float) and math.isfinite(timeout) and timeout >= 0,
                "finite nonnegative process wait bound required")
        deadline = time.monotonic() + timeout
        while self.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired("owned installed child", timeout)
            time.sleep(min(0.05, remaining))
        if self.reaped:
            return self.process.returncode
        for thread in self.threads:
            if thread.ident is not None:
                thread.join(timeout=min(1, max(0, deadline - time.monotonic())))
        if any(thread.is_alive() for thread in self.threads):
            self.failed.set()
            self.signal(signal.SIGKILL)
            for thread in self.threads:
                if thread.ident is not None:
                    thread.join(timeout=max(0, deadline - time.monotonic()))
        # Also terminate owned descendants that closed their log descriptors.
        # A lock excludes a drain thread signaling between reap and PID reuse.
        with self.group_lock:
            self._signal_owned_locked(signal.SIGKILL)
            code = self.process.wait(timeout=max(0, deadline - time.monotonic()))
            self.reaped = True
            self._record_proof("reaped_child", code)
        require(not any(thread.is_alive() for thread in self.threads), "owned process log pipe did not close")
        require(not self.failed.is_set(), "owned process exceeded log bound or capture failed")
        return code


class Driver:
    expected_target = "x86_64-unknown-linux-gnu"
    observation_format = "recall-installed-linux-observation/v1"
    process_capture_manifest_format = "recall-installed-linux-run/v1"
    process_capture_cases = CASES

    def __init__(self, manifest: dict, inventory: dict, isolation: dict, *, original_manifest_raw=None):
        self.manifest, self.inventory = manifest, inventory
        self.original_manifest_raw = original_manifest_raw
        self.cases = inventory["expectations"]
        self.root = Path(manifest["run_directory"])
        require(not self.root.exists() and not self.root.is_symlink(), "run directory must be new")
        require(self.root == self.root.resolve(), "run directory must have canonical unsymlinked parents")
        for parent in self.root.parents:
            require(not (parent / "Cargo.toml").exists() and not (parent / ".elephant").exists(),
                    "run directory must be outside an existing runtime/project")
        self.root.mkdir(mode=0o700)
        for name in ("private", "captures", "service", "backups", "media", "recovery"):
            (self.root / name).mkdir(mode=0o700)
        self.report = {"format": self.observation_format, "product_acceptance": False,
                       "source_commit": manifest["source_commit"], "archive_sha256": manifest["archive_sha256"],
                       "backend": manifest["backend"],
                       "binary_sha256": manifest["binary_sha256"], "plan": inventory,
                       "isolation": isolation, "cases": [], "unqualified_gates": self.cases["unqualified_gates"]}
        self.report["run_manifest_sha256"] = digest(encoded(manifest))
        expiry_delta = utc(manifest["expires_at"]) - dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)
        expires_wall_ns = ((expiry_delta.days * 86400 + expiry_delta.seconds) * 10**9
                           + expiry_delta.microseconds * 1000)
        self.allocation_clock, self.allocation_deadline_ns = process_capture.pin_allocation_clock(
            self.cases["limits"]["total_seconds"], expires_wall_ns)
        self.started = self.allocation_clock["mono_before_ns"] / 10**9
        self.allocation_deadline = process_capture.deadline_seconds(self.allocation_deadline_ns)
        self.process_journal = None
        self.case_proofs = []
        self.case_request_start = self.case_process_start = 0
        self.child = None
        self.owned_commands = []
        self.serial = 0
        self.requests = 0
        self.capture_bytes = 0
        self.capture_budget_lock = threading.RLock()
        self.environment = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "RUST_LOG": "warn",
                            "HOME": str(self.root / "private"),
                            "ELEPHANT_SNAPSHOT_DIR": str(self.root / "backups"),
                            "ELEPHANT_DURABILITY_PATH": str(self.root / "recovery")}
        self.tokens = {}
        self.binary = self.root / "installed/meerkat-recall"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            self.port = reservation.getsockname()[1]

    def begin_process_capture(self):
        # Darwin has a separately owned custody primitive. Do not silently
        # attribute Linux /proc evidence to that platform's observations.
        if self.expected_target != "x86_64-unknown-linux-gnu" or self.manifest.get("format") != self.process_capture_manifest_format:
            return
        require(self.process_journal is None, "original process journal already initialized")
        self.process_journal = process_capture.CaptureJournal(self.root, reserve=self.reserve_capture)
        deadline_ns = self.allocation_deadline_ns
        self.controller_capture = process_capture.capture_controller(self.process_journal, deadline_ns,
            allocation_clock=self.allocation_clock)
        require(type(self.original_manifest_raw) is bytes and len(self.original_manifest_raw) <= 1024 * 1024
                and capture.strict_json_body(self.original_manifest_raw) == self.manifest,
                "original validated manifest bytes required")
        self.manifest_capture = self.process_journal.save("captures/original-manifest.json", self.original_manifest_raw, "installed-manifest")
        review_raw = process_capture.read_bounded(self.manifest["expectation_review"], 1024 * 1024)
        require(digest(review_raw) == self.manifest["expectation_review_sha256"], "original installed review changed")
        self.review_capture = self.process_journal.save("captures/original-review.json", review_raw, "installed-review")
        case_raw = process_capture.read_bounded(self.process_capture_cases, 1024 * 1024)
        corpus_raw = process_capture.read_bounded(fixtures.CORPUS_PATH, 1024 * 1024)
        require(digest(case_raw) == self.inventory["expectations_sha256"]
                and digest(corpus_raw) == self.inventory["corpus_sha256"], "original installed literal inputs changed")
        case_ref = self.process_journal.save("captures/original-cases.json", case_raw, "installed-case-file")
        corpus_ref = self.process_journal.save("captures/original-corpus.json", corpus_raw, "installed-corpus")
        self.reviewed_inputs_capture = self.process_journal.save("captures/reviewed-inputs.json", process_capture.encode({
            "format": "recall-installed-reviewed-inputs/v2", "plan": self.inventory,
            "review": self.review_capture, "case_file": case_ref, "corpus": corpus_ref}), "installed-reviewed-plan")

    def remaining(self) -> float:
        require(dt.datetime.now(dt.timezone.utc) < utc(self.manifest["expires_at"]), "execution allocation expired")
        remaining = min(getattr(self, "allocation_deadline", self.started + self.cases["limits"]["total_seconds"]) - time.monotonic(),
            (utc(self.manifest["expires_at"]) - dt.datetime.now(dt.timezone.utc)).total_seconds())
        require(remaining > 0, "run deadline expired")
        return remaining

    def record(self, case: str, **observed):
        require(case in self.cases["required_cases"], "undeclared installed case")
        self.report["cases"].append({"case": case, "observation": "passed", **observed})
        if getattr(self, "process_journal", None) is not None:
            proof = {"case": case, "observed_ns": time.monotonic_ns(),
                     "request_indices": list(range(self.case_request_start + 1, self.requests + 1)),
                     "process_indices": list(range(self.case_process_start + 1, self.serial + 1))}
            self.case_proofs.append(proof)
            self.process_journal.event(None, "case-observed", proof)
            self.case_request_start, self.case_process_start = self.requests, self.serial

    def reserve_capture(self, amount):
        # Actual driver construction installs this one lock before any child or
        # HTTP call; test-only bare objects retain their original single caller.
        lock = getattr(self, "capture_budget_lock", None)
        if lock is None:
            self.capture_budget_lock = lock = threading.RLock()
        with lock:
            require(type(amount) is int and amount >= 0
                    and self.capture_bytes + amount <= self.cases["limits"]["http_capture_bytes"],
                    "HTTP capture storage limit reached")
            self.capture_bytes += amount

    def save(self, name: str, raw: bytes):
        self.reserve_capture(len(raw))
        private_new(self.root / "captures" / name, raw)

    def create_process(self, arguments: list[str], log_paths: list[Path]):
        """Platform custody seam; callers cannot select it through a manifest."""
        remaining = min(self.remaining(), (utc(self.manifest["expires_at"]) - dt.datetime.now(dt.timezone.utc)).total_seconds())
        deadline = min(getattr(self, "allocation_deadline", self.started + self.cases["limits"]["total_seconds"]),
                       time.monotonic() + remaining)
        extra = {}
        if getattr(self, "process_journal", None) is not None:
            proof = process_capture.ProcessProof(self.binary, arguments, self.root / "service", self.environment,
                log_paths, self.allocation_deadline_ns, journal=self.process_journal, index=self.serial)
            require(proof.launch_binary["sha256"] == self.manifest["binary_sha256"], "original installed launch binary changed")
            extra["capture_proof"] = proof
        return OwnedProcess(self.binary, arguments, self.root / "service", self.environment,
                            log_paths, self.cases["limits"]["process_log_bytes"],
                            initialization_deadline=deadline, **extra)

    def process(self, arguments: list[str], *, serve=False):
        self.remaining()
        require(not serve or self.child is None, "owned service handle already present")
        self.serial += 1
        require(self.serial <= self.cases["limits"]["processes"], "owned process count limit reached")
        prefix = f"process-{self.serial:03d}"
        try:
            process = self.create_process(arguments,
                [self.root / "captures" / f"{prefix}.{suffix}" for suffix in ("stdout", "stderr")])
        except BaseException as error:
            owned = getattr(error, "owned_process", None)
            if owned is not None:
                self.owned_commands.append(owned)
            raise
        if serve:
            self.child = process
            return prefix
        self.owned_commands.append(process)
        try:
            code = process.wait(timeout=min(15, self.remaining()))
        except BaseException:
            process.signal(signal.SIGKILL)
            process.wait(timeout=min(5, self.cleanup_remaining()))
            raise
        finally:
            if process.reaped:
                self.owned_commands.remove(process)
        require(code == 0, f"installed CLI command failed: {prefix}")
        return prefix

    def cleanup_remaining(self):
        """Cleanup consumes the original run and wall-clock allocation."""
        return max(0, min(getattr(self, "allocation_deadline", self.started + self.cases["limits"]["total_seconds"]) - time.monotonic(),
                          (utc(self.manifest["expires_at"]) - dt.datetime.now(dt.timezone.utc)).total_seconds()))

    def _stop_owned(self, process, *, require_clean, deadline):
        if process.poll() is None:
            process.signal(signal.SIGTERM)
        try:
            code = process.wait(timeout=min(65, max(0, deadline - time.monotonic())))
        except subprocess.TimeoutExpired:
            process.signal(signal.SIGKILL)
            process.wait(timeout=min(5, max(0, deadline - time.monotonic())))
            require(not require_clean, "owned service did not drain before deadline")
            return
        require(not require_clean or code == 0, "owned service failed graceful shutdown")

    def stop(self, *, require_clean=True):
        deadline = min(getattr(self, "allocation_deadline", self.started + self.cases["limits"]["total_seconds"]),
                       time.monotonic() + self.cleanup_remaining())
        failures = []
        handles = ([self.child] if self.child is not None else []) + list(self.owned_commands)
        for process in handles:
            try:
                self._stop_owned(process, require_clean=require_clean, deadline=deadline)
            except BaseException as error:
                failures.append(error)
            finally:
                # Keep every original handle whose group cleanup did not reap.
                if process.reaped:
                    if process is self.child:
                        self.child = None
                    if process in self.owned_commands:
                        self.owned_commands.remove(process)
        if failures:
            raise failures[0]

    def http(self, method: str, path: str, token: str | None = None, space: str | None = None,
             body=None, *, mcp=False) -> dict:
        self.requests += 1
        require(self.requests <= self.cases["limits"]["http_calls"], "HTTP attempt limit reached")
        request_limit = (self.cases["limits"]["snapshot_http_seconds"] if path.startswith("/v1/system/snapshot/")
                         else self.cases["limits"]["http_seconds"])
        timeout = min(self.remaining(), request_limit)
        headers = {"Accept": "application/json, text/event-stream" if mcp else "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + self.tokens[token]
        if space:
            headers["X-Space-Id"] = space
        payload = None if body is None else encoded(body)
        if payload is not None:
            headers["Content-Type"] = "application/json"
        limit = self.cases["limits"]["response_bytes"]
        started = time.monotonic()
        deadline = min(getattr(self, "allocation_deadline", self.started + self.cases["limits"]["total_seconds"]),
                       started + timeout)
        timeout = deadline - started
        require(timeout > 0, "original HTTP allocation deadline expired")
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        raw = b""
        framing_error = None
        status = None
        try:
            with capture.total_deadline(timeout):
                connection.request(method, path, payload, headers)
                response = connection.getresponse()
                status = response.status
                try:
                    while len(raw) <= limit:
                        chunk = response.read1(min(8192, limit + 1 - len(raw)))
                        if not chunk:
                            break
                        raw += chunk
                except http.client.IncompleteRead as incomplete:
                    raw += incomplete.partial
                    framing_error = "incomplete response framing"
                lengths = response.headers.get_all("Content-Length", [])
                transfers = response.headers.get_all("Transfer-Encoding", [])
                if lengths and (len(lengths) != 1 or not lengths[0].isdigit() or int(lengths[0]) != len(raw)):
                    framing_error = "invalid response length"
                if transfers and (lengths or transfers != ["chunked"]):
                    framing_error = "ambiguous response framing"
                if len(raw) > limit:
                    framing_error = "response exceeds bound"
        except (OSError, TimeoutError, http.client.HTTPException) as error:
            self.save(f"http-{self.requests:03d}.json", encoded({
                "method": method, "path": path, "space": space, "credential_role": token,
                "request": body, "status": status, "transport_error_type": type(error).__name__,
                "raw_body_base64": base64.b64encode(raw[:limit]).decode(),
                "body_sha256": digest(raw[:limit]), "raw_body_complete": False,
                "started_monotonic_ns": int(started * 1e9), "finished_monotonic_ns": time.monotonic_ns(),
                "deadline_monotonic_ns": int(deadline * 1e9),
                "duration_ms": (time.monotonic() - started) * 1000}))
            raise
        finally:
            connection.close()
        item = {"method": method, "path": path, "space": space, "credential_role": token,
                "request": body, "status": status, "raw_body_base64": base64.b64encode(raw[:limit]).decode(),
                "body_sha256": digest(raw[:limit]), "raw_body_complete": framing_error is None,
                "started_monotonic_ns": int(started * 1e9), "finished_monotonic_ns": time.monotonic_ns(),
                "deadline_monotonic_ns": int(deadline * 1e9),
                "duration_ms": (time.monotonic() - started) * 1000}
        if framing_error:
            item["capture_error"] = framing_error
        self.save(f"http-{self.requests:03d}.json", encoded(item))
        require(framing_error is None, framing_error or "incomplete response")
        try:
            value = mcp_value(raw, body["id"]) if mcp and status == 200 else capture.strict_json_body(raw)
        except (ValueError, UnicodeError) as error:
            # Keep the complete original capture and refuse invalid responses.
            # Do not echo response/request bodies or credentials in diagnostics.
            raise ValueError(
                f"HTTP {status} {method} {path}: invalid response body "
                f"({len(raw)} bytes; capture http-{self.requests:03d}.json)"
            ) from error
        return {"status": status, "body": value}

    def rpc(self, token: str, space: str | None, method: str, params: dict) -> dict:
        result = self.http("POST", "/mcp/full", token, space,
                           {"jsonrpc": "2.0", "id": self.requests + 1, "method": method, "params": params}, mcp=True)
        require(result["status"] == 200, "MCP transport denied positive request")
        require("error" not in result["body"], "MCP protocol error")
        return result["body"]["result"]

    def tool(self, name: str, arguments: dict, *, token="operator", space=None, error=False):
        result = self.rpc(token, space, "tools/call", {"name": name, "arguments": arguments})
        require((result.get("isError") is True) == error, f"unexpected MCP tool outcome: {name}")
        texts = [block["text"] for block in result.get("content", []) if block.get("type") == "text"]
        require(len(texts) == 1, "single typed MCP result required")
        value = capture.strict_json_body(texts[0].encode())
        if isinstance(value, dict) and value.get("ok") is True:
            return value["data"]
        return value

    def denied_tool(self, name: str, arguments: dict, *, token, space, code):
        request_id = self.requests + 1
        response = self.http("POST", "/mcp/full", token, space,
            {"jsonrpc": "2.0", "id": request_id, "method": "tools/call",
             "params": {"name": name, "arguments": arguments}}, mcp=True)
        return mcp_typed_denial(response, request_id, code, self.tokens.values())

    def check_mcp_auth(self, space: str, query: str):
        request_id = self.requests + 1
        response = self.http("POST", "/mcp/full", space=space,
            body={"jsonrpc": "2.0", "id": request_id, "method": "tools/list", "params": {}}, mcp=True)
        mcp_public_status_only(response, request_id, self.tokens.values())
        self.denied_tool("search_scoped", {"query": query, "ranking_mode": "lexical", "limit": 1},
                         token=None, space=space, code="unauthorized")
        self.record("mcp_auth_denied")

    def launch(self):
        self.process(["serve", "--server-only", "--shutdown-timeout-seconds", "60"], serve=True)
        deadline = time.monotonic() + min(20, self.remaining())
        while time.monotonic() < deadline:
            require(self.child is not None and self.child.poll() is None, "owned service exited during startup")
            try:
                health = self.http("GET", "/v1/health")
                require(health["status"] == 200, "liveness endpoint failed")
                return
            except (ConnectionRefusedError, ConnectionResetError):
                time.sleep(min(1, self.remaining()))
        raise ValueError("owned service failed bounded startup")

    def credentials(self):
        key = self.root / "private/signing.key"
        self.process(["service-token", "bootstrap", "--secret-file", str(key)])
        clearances = [{"policy_id": row["policy_id"], "max_level": row["level"], "labels": [], "handling_allow": []}
                      for row in fixtures.corpus()["spaces"]]
        clearances.append({"policy_id": "default", "max_level": 0, "labels": [], "handling_allow": []})
        clearance_path = self.root / "private/clearances.json"
        private_new(clearance_path, encoded(clearances))
        scopes = ["read:status", "read:space_config", "write:space_config", "read:source", "write:source",
                  "read:doc:raw", "read:doc:metadata", "read:entity", "manage:grant", "manage:provider"]
        self.data_scopes = scopes
        management_scopes = ["manage:grant", "manage:provider", "read:space_config", "write:space_config",
                             "read:status", "read:system", "write:system"]
        for role in ("operator", "service", "household"):
            path = self.root / "private" / f"{role}.token"
            arguments = ["service-token", "issue", "--secret-file", str(key), "--subject", f"installed-{role}",
                         "--expires-in", "900", "--issuer", "installed-offline", "--audience", "recall-installed",
                         "--output", str(path)]
            if role == "operator":
                arguments += ["--global-admin"]
                selected_scopes = management_scopes
            else:
                arguments += ["--clearances-file", str(clearance_path)]
                for space in self.cases["spaces"][:1 if role == "household" else 2]:
                    arguments += ["--space", space]
                selected_scopes = scopes
            for scope in selected_scopes:
                arguments += ["--scope", scope]
            self.process(arguments)
            require(path.stat().st_mode & 0o077 == 0 and not path.is_symlink(), "token file is not private")
            self.tokens[role] = path.read_text().strip()
            require(bool(self.tokens[role]), "CLI issued no credential")
        for space in fixtures.corpus()["spaces"]:
            name = space["id"]
            role = "bootstrap-" + name
            clearance_path = self.root / "private" / f"{role}-clearances.json"
            private_new(clearance_path, encoded([{"policy_id": space["policy_id"], "max_level": space["level"],
                                                 "labels": [], "handling_allow": []}]))
            path = self.root / "private" / f"{role}.token"
            arguments = ["service-token", "issue", "--secret-file", str(key), "--subject", f"installed-{role}",
                         "--bootstrap-space", name, "--clearances-file", str(clearance_path),
                         "--expires-in", "900", "--issuer", "installed-offline", "--audience", "recall-installed",
                         "--output", str(path)]
            for scope in ("read:space_config", "write:space_config", "manage:grant", "manage:provider"):
                arguments += ["--scope", scope]
            self.process(arguments)
            require(path.stat().st_mode & 0o077 == 0 and not path.is_symlink(), "bootstrap token file is not private")
            self.tokens[role] = path.read_text().strip()
            require(bool(self.tokens[role]), "CLI issued no bootstrap credential")
        self.environment.update({"JWT_SECRET_FILE": str(key), "JWT_ALGORITHM": "HS256",
                                 "JWT_ISSUER": "installed-offline", "JWT_AUDIENCE": "recall-installed",
                                 "ELEPHANT_HOST": "127.0.0.1", "ELEPHANT_PORT": str(self.port)})
        self.record("fresh_cli_auth", roles=sorted(self.tokens), skip_auth=False)

    def bootstrap_role(self, space: str) -> str:
        require(space in self.cases["spaces"], "bootstrap target is outside the frozen named spaces")
        role = "bootstrap-" + space
        require(bool(self.tokens.get(role)), "explicit named bootstrap credential is absent")
        return role

    @staticmethod
    def source_path(space: str, key: str) -> str:
        return f"/v1/spaces/{urllib.parse.quote(space, safe='')}/sources/{urllib.parse.quote(key, safe='')}"

    def head(self, row: dict):
        result = self.http("GET", self.source_path(row["space"], row["key"]) + "/head", "service", row["space"])
        require(result["status"] == 200, "accepted source head unavailable")
        return result["body"]

    def accept(self, row: dict, revision: str, generation=None):
        source = next(source for source in fixtures.corpus()["sources"] if source["space"] == row["space"]
                      and source["key"] == row["key"] and source["revision"] == revision)
        result = self.http("PUT", self.source_path(row["space"], row["key"]), "service", row["space"],
                           fixtures.source_request(source, generation))
        require(result["status"] == 202, "source acceptance failed")
        body = result["body"]
        require(body["desired_revision"] == revision and body["published_revision"] is None,
                "acceptance invented published coverage")
        require(all(isinstance(body.get(key), str) and body[key] for key in
                    ("operation_id", "internal_source_id", "generation")), "acceptance identity missing")
        return body

    def configuration(self, space):
        name = space["id"]
        response = self.http("GET", f"/v1/spaces/{name}", "service", name)
        require(response["status"] == 200, "stored named-space configuration unavailable")
        body = response["body"]
        require(body["space_id"] == name and isinstance(body["id"], str), "stored named-space identity differs")
        require(body["security"] == {"policy_id": space["policy_id"], "level": space["level"],
                                    "labels": [], "handling": []}, "stored security envelope differs")
        for key, expected in {"embedding_provider": "openai", "embedding_model": f"installed-unapproved-{name}",
                              "embedding_dims": space["dimensions"], "purpose": space["extraction_purpose"],
                              "skill": space["extraction_skill"]}.items():
            require(body.get(key) == expected, f"stored named-space {key} differs")
        processing = dict(body["processing"])
        index_epoch = processing.pop("index_generation")
        require(type(index_epoch) is int and index_epoch >= 0, "stored index epoch is invalid")
        require(processing == {"generation": 1, "policy_id": space["policy_id"],
                    "connections": {}, "profiles": {}, "bindings": {},
                    "active_embedding_profile_id": None, "pending_reindex": None},
                "stored empty processing authority differs")
        # Source-head writes advance the content epoch and updated_at (schema
        # 033/038); configuration identity and admin generation must stay fixed.
        identity = dict(body)
        identity.pop("updated_at", None)
        identity["processing"] = processing
        return identity

    def verify_configurations(self, original):
        for space in fixtures.corpus()["spaces"]:
            actual = self.configuration(space)
            require(actual == original[space["id"]], "restored or reopened stored configuration changed")

    def verify_operation(self, space, expected):
        result = self.http("GET", "/v1/operations/" + urllib.parse.quote(expected["operation_id"], safe=""),
                           "service", space)
        require(result["status"] == 200 and result["body"] == expected,
                "original durable operation identity or state changed")

    def execute(self):
        with capture.total_deadline(self.remaining()):
            manifest = artifact.verify(Path(self.manifest["archive"]), self.manifest["archive_sha256"], self.root / "installed")
        self.report["artifact_manifest"] = manifest
        require(manifest["target"] == self.expected_target, "wrong installed target")
        require(manifest["source_commit"] == self.manifest["source_commit"], "artifact source differs")
        require(manifest["binary_sha256"] == self.manifest["binary_sha256"] == file_digest(self.binary), "binary identity differs")
        prefix = self.process(["--version"])
        version = (self.root / "captures" / f"{prefix}.stdout").read_text().strip()
        require(version == f"meerkat-recall {manifest['version']}", "installed version differs")
        self.record("verified_installed_binary", version=version)
        try:
            self.http("GET", "/v1/health")
        except ConnectionRefusedError:
            self.record("absent_service", observation_kind="connection_refused")
        else:
            raise ValueError("fresh selected service address is already occupied")
        self.credentials()
        self.launch()
        unready = self.http("GET", "/v1/readiness", "operator")
        require(unready["status"] == 503 and unready["body"]["configured"] is False
                and unready["body"]["ready"] is False, "bootstrap liveness became readiness")
        self.record("bootstrap_unready")
        self.rpc("operator", None, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                 "clientInfo": {"name": "installed-offline-qualification", "version": "1"}})
        if self.manifest["backend"] == "elephant-db":
            status = self.tool("initialize_runtime", {"backend": "elephant-db", "ingest_enabled": False,
                "extractor_required": False, "snapshot_path": str(self.root / "service/state.snapshot"),
                "multimodal_asset_root": str(self.root / "media")})
        else:
            # Explicit retained migrate command, never connect to a remote server
            # or treat a vanilla SurrealDB version string as patched capability.
            self.stop()
            self.environment.update({"ELEPHANT_DB": "surreal-db",
                "SURREALDB_URL": "rocksdb://" + str(self.root / "service/native-store"),
                "SURREALDB_NAMESPACE": "installed_offline", "SURREALDB_DATABASE": "isolated_fixture",
                "SURREALDB_USER": "root", "SURREALDB_PASS": "synthetic-offline-only"})
            self.process(["migrate"])
            self.launch()
            status = self.tool("configure_runtime", {"ingest_enabled": False, "extractor_required": False,
                "multimodal_asset_root": str(self.root / "media")})
        require(status["configured"] is True and status["storage_ready"] is True, "runtime initialization failed")
        require(status["backend"] == self.manifest["backend"], "runtime backend differs from allocated backend")
        self.record("runtime_initialized_without_workers")
        grants = {}
        for space in fixtures.corpus()["spaces"]:
            name = space["id"]
            processing = {"generation": 1, "index_generation": 0, "policy_id": space["policy_id"],
                          "connections": {}, "profiles": {}, "bindings": {},
                          "active_embedding_profile_id": None, "pending_reindex": None}
            result = self.http("POST", "/v1/spaces", self.bootstrap_role(name), name, {"name": name,
                "security": {"policy_id": space["policy_id"], "level": space["level"], "labels": [], "handling": []},
                "embedding_provider": "openai", "embedding_model": f"installed-unapproved-{name}",
                "embedding_dims": space["dimensions"], "purpose": space["extraction_purpose"],
                "skill": space["extraction_skill"], "processing": processing})
            require(result["status"] == 201, "named space creation failed")
            grant_path = f"/v1/spaces/{name}/grants/installed-service"
            result = self.http("PUT", grant_path, self.bootstrap_role(name), name,
                               {"active": True, "scopes": self.data_scopes, "expected_generation": None})
            require(result["status"] == 200, "current service grant creation failed")
            grants[name] = result["body"]["generation"]
        configurations = {space["id"]: self.configuration(space) for space in fixtures.corpus()["spaces"]}
        self.record("two_named_spaces", configurations=configurations)
        home, business = self.cases["spaces"]
        household_grant = self.http("PUT", f"/v1/spaces/{home}/grants/installed-household", self.bootstrap_role(home), home,
            {"active": True, "scopes": self.data_scopes, "expected_generation": None})
        require(household_grant["status"] == 200, "household positive grant creation failed")
        positive = self.http("GET", f"/v1/spaces/{home}/status", "household", home)
        require(positive["status"] == 200, "single-space credential positive control denied")
        denied = self.http("GET", f"/v1/spaces/{home}/status")
        require(denied["status"] == 401, "unauthenticated HTTP admitted")
        typed_denial(denied["body"], "unauthorized", self.tokens.values())
        self.record("http_auth_denied")
        home_query = next(query["query"] for query in fixtures.corpus()["queries"]
                          if query["id"] == self.cases["sources"][0]["query_id"])
        self.check_mcp_auth(home, home_query)
        for token, selected in (("household", business), ("service", home)):
            denied = self.http("GET", f"/v1/spaces/{business}/status", token, selected)
            require(denied["status"] == 403, "wrong named-space selection admitted")
            typed_denial(denied["body"], "forbidden_space", self.tokens.values())
        self.record("wrong_space_denied")
        for space in self.cases["spaces"]:
            self.rpc("service", space, "initialize", {"protocolVersion": "2025-03-26", "capabilities": {},
                     "clientInfo": {"name": "installed-two-space-client", "version": "1"}})
            listed = self.rpc("service", space, "tools/list", {})
            require(any(tool["name"] == "search_scoped" for tool in listed["tools"]), "scoped MCP read missing")
        self.record("mcp_positive")
        grant_path = f"/v1/spaces/{home}/grants/installed-service"
        revoked = self.http("PUT", grant_path, self.bootstrap_role(home), home,
            {"active": False, "scopes": self.data_scopes, "expected_generation": grants[home]})
        require(revoked["status"] == 200, "grant revocation failed")
        denied = self.http("GET", f"/v1/spaces/{home}/status", "service", home)
        require(denied["status"] == 403, "next request ignored current grant revocation")
        typed_denial(denied["body"], "forbidden_space", self.tokens.values())
        restored = self.http("PUT", grant_path, self.bootstrap_role(home), home,
            {"active": True, "scopes": self.data_scopes, "expected_generation": revoked["body"]["generation"]})
        require(restored["status"] == 200, "grant restoration CAS failed")
        self.record("grant_revocation")
        baseline = {}
        operations = {}
        for row in self.cases["sources"]:
            accepted = self.accept(row, row["baseline_revision"])
            replay = self.accept(row, row["baseline_revision"])
            require(accepted == replay, "idempotent acceptance changed receipt")
            head = self.head(row)
            require(head["desired_revision"] == row["baseline_revision"] and head["state"] == "pending", "disabled workers claimed completion")
            operation = self.http("GET", "/v1/operations/" + urllib.parse.quote(accepted["operation_id"], safe=""), "service", row["space"])
            require(operation["status"] == 200 and operation["body"]["state"] == "pending", "operation is not pending")
            query = next(query for query in fixtures.corpus()["queries"] if query["id"] == row["query_id"])
            request = {"query": query["query"], "limit": query["k"], "ranking_mode": "lexical",
                       "source_revision_constraints": {row["key"]: row["baseline_revision"]}}
            result = self.http("POST", f"/v1/spaces/{row['space']}/search", "service", row["space"], request)
            require(result["status"] == 200 and result["body"]["partial"] is True and result["body"]["results"] == [], "pending source became a retrieval winner")
            require(result["body"]["coverage"][row["key"]]["state"] == "pending", "pending coverage absent")
            mcp_result = self.tool("search_scoped", request, token="service", space=row["space"])
            require(mcp_result == result["body"], "HTTP/MCP pending coverage differs")
            baseline[row["space"]] = head
            operations[row["space"]] = operation["body"]
        self.record("source_acceptance_and_idempotence")
        self.record("pending_is_not_complete")
        business_row = self.cases["sources"][1]
        forbidden = self.http("GET", self.source_path(business, business_row["key"]) + "/head", "household", business)
        require(forbidden["status"] == 403, "foreign accepted source became visible")
        typed_denial(forbidden["body"], "forbidden_space", self.tokens.values())
        self.denied_tool("search_scoped", {
            "query": next(query["query"] for query in fixtures.corpus()["queries"]
                          if query["id"] == business_row["query_id"]),
            "ranking_mode": "lexical", "source_revision_constraints": {
                business_row["key"]: business_row["baseline_revision"]}},
            token="household", space=business, code="forbidden_space")
        bounded = self.http("POST", f"/v1/spaces/{home}/search", "service", home,
                            {"query": "original invalid-limit control", "limit": 51, "ranking_mode": "lexical"})
        require(bounded["status"] == 400 and bounded["body"]["error_code"] == "invalid_request", "query limit was silently repaired")
        typed_denial(bounded["body"], "invalid_request", self.tokens.values())
        self.record("input_bounds")
        backup_path = "baseline"
        backup = self.http("POST", "/v1/system/snapshot/export", "operator", body={"path": backup_path,
            "name": "installed-baseline", "retention_seconds": 3600, "quiesce_timeout_seconds": 30})
        require(backup["status"] == 200 and backup["body"]["success"] is True and isinstance(backup["body"].get("manifest"), dict), "registered named export failed")
        self.record("named_export", manifest=backup["body"]["manifest"])
        for row in self.cases["sources"]:
            self.accept(row, row["replacement_revision"], baseline[row["space"]]["generation"])
        self.record("replacement_accepted")
        restored = self.http("POST", "/v1/system/snapshot/restore", "operator", body={"path": backup_path})
        require(restored["status"] == 200 and restored["body"]["success"] is True, "registered backup restore failed")
        for row in self.cases["sources"]:
            require(self.head(row) == baseline[row["space"]], "restore did not recover original acknowledged head")
            self.verify_operation(row["space"], operations[row["space"]])
        self.verify_configurations(configurations)
        self.record("restore_original_heads")
        self.stop()
        self.launch()
        for row in self.cases["sources"]:
            require(self.head(row) == baseline[row["space"]], "graceful reopen changed persisted head")
            self.verify_operation(row["space"], operations[row["space"]])
        self.verify_configurations(configurations)
        self.record("graceful_restart")
        self.tool("configure_runtime", {"runtime_mode": "read_only"})
        listed = self.rpc("service", home, "tools/list", {})
        names = {tool["name"] for tool in listed["tools"]}
        require("search_scoped" in names and "retract_source" not in names, "read-only tool inventory is false")
        refused = self.tool("retract_source", {}, token="service", space=home, error=True)
        typed_denial(refused, "capability_disabled", self.tokens.values())
        require(self.head(self.cases["sources"][0])["desired_revision"] == self.cases["sources"][0]["baseline_revision"], "read-only metadata read failed")
        self.record("read_only_capabilities")
        self.stop()
        require(file_digest(self.binary) == self.manifest["binary_sha256"], "installed executable changed during run")
        config = self.root / "service/.elephant/elephant_config.toml"
        self.report["runtime_configuration_sha256"] = file_digest(config)
        self.report["processes"] = self.serial
        require({row["case"] for row in self.report["cases"]} == set(self.cases["required_cases"]), "required observations missing")
        self.report["subset_observation"] = "passed; requires independent D verdict"

    def run(self) -> int:
        code = 0
        try:
            self.begin_process_capture()
            self.execute()
        except Exception as error:
            code = 1
            self.report["subset_observation"] = "failed"
            # Known errors contain case labels, never bearer values or headers.
            self.report["failure_type"] = type(error).__name__
            self.report["failure"] = str(error) if isinstance(error, ValueError) else "transport or process prerequisite failed"
        finally:
            try:
                self.stop(require_clean=False)
            except Exception as error:
                code = 1
                self.report["subset_observation"] = "failed"
                self.report["cleanup_error_type"] = type(error).__name__
            self.report["duration_seconds"] = time.monotonic() - self.started
            self.report["http_attempts"] = self.requests
            self.report["processes"] = self.serial
            try:
                self.finish_process_capture()
            except Exception as error:
                code = 1
                self.report["subset_observation"] = "failed"
                self.report["process_capture_error_type"] = type(error).__name__
            raw_observation = encoded(self.report)
            require(len(raw_observation) <= 2 * 1024 * 1024, "original observation exceeds bounded evidence envelope")
            self.reserve_capture(len(raw_observation))
            private_new(self.root / "observation.json", raw_observation)
        return code

    def finish_process_capture(self):
        journal = getattr(self, "process_journal", None)
        if journal is None:
            return
        rows = [proof.snapshot() for proof in journal.processes]
        complete = len(rows) == self.serial and all(row["reaped"] and not row["errors"]
            and all(log["complete"] for log in row["logs"]) for row in rows)
        journal_capture = journal.save("captures/process-journal.json", process_capture.encode({
            "format": "recall-installed-process-journal/v2", "processes": rows,
            "attempted_processes": self.serial, "case_proofs": self.case_proofs,
            "terminal_observed_ns": time.monotonic_ns(), "complete": complete}), "installed-process-journal")
        cleanup = journal.save("captures/process-cleanup.json", process_capture.encode({
            "format": "recall-installed-process-cleanup/v2", "observed_ns": time.monotonic_ns(),
            "remaining_child_pid": self.child.pid if self.child is not None else None,
            "remaining_command_pids": [process.pid for process in self.owned_commands],
            "reaped_indices": [row["index"] for row in rows if row["reaped"]],
            "capture_complete": complete, "original_deadline_ns": getattr(self, "allocation_deadline_ns",
                int((self.started + self.cases["limits"]["total_seconds"]) * 1e9)),
            "original_expires_at": self.manifest["expires_at"]}), "installed-cleanup")
        additional = [(Path(log["path"]), "process-log", self.cases["limits"]["process_log_bytes"])
                      for row in rows for log in row["logs"] if log["complete"]]
        additional += [(self.root / "captures" / f"http-{index:03d}.json", "installed-http", 2 * 1024 * 1024)
                       for index in range(1, self.requests + 1)]
        configuration = self.root / "service/.elephant/elephant_config.toml"
        configuration_capture = None
        if configuration.is_file():
            configuration_capture = journal.save("captures/runtime-configuration.toml",
                process_capture.read_bounded(configuration, 1024 * 1024), "installed-runtime-configuration")
        artifact_capture = None
        artifact_path = self.root / "installed/provenance.json"
        if artifact_path.is_file():
            artifact_capture = journal.save("captures/artifact-provenance.json",
                process_capture.read_bounded(artifact_path, 1024 * 1024), "installed-artifact-manifest")
        inventory = journal.inventory(additional)
        inventory_capture = journal.save("captures/capture-inventory.json", process_capture.encode(inventory), "capture-inventory")
        evidence = {"format": "recall-installed-process-evidence/v2",
            "controller": self.controller_capture, "review": self.review_capture, "manifest": self.manifest_capture,
            "reviewed_plan": getattr(self, "reviewed_inputs_capture", None),
            "process_journal": journal_capture, "cleanup": cleanup, "capture_inventory": inventory_capture,
            "runtime_configuration": configuration_capture, "artifact_manifest": artifact_capture}
        journal.save("captures/process-evidence.json", process_capture.encode(evidence), "installed-process-evidence")
        require(complete and self.child is None and not self.owned_commands,
                "original installed process capture or cleanup is incomplete")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("plan", help="show exact inert inventory; no candidate or credential access")
    run = sub.add_parser("run", help="requires later explicit allocation and independent reviewed source")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    run.add_argument("--parent-network-namespace", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps(plan(), indent=2))
        return 0
    require(sys.version_info >= (3, 12), "artifact installation requires Python 3.12+")
    manifest, inventory = validate_manifest(args.manifest)
    isolation = offline_namespace(args.parent_network_namespace)
    enable_loopback()
    return Driver(manifest, inventory, isolation,
                  original_manifest_raw=process_capture.read_bounded(args.manifest, 1024 * 1024)).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({"status": "unmet-prerequisite", "error_type": type(error).__name__,
                          "product_acceptance": False}), file=sys.stderr)
        sys.exit(78)
