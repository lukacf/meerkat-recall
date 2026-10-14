"""Actual installed-process lifetime for the original controlled full workload.

Importing this module is inert. The reviewed caller supplies the original
allocation, transport, isolation observation and source-verified helper objects.
There is no PID adoption, shell callback, environment inheritance or new clock.
Receipts are observations; independent qualification remains a separate gate.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import copy
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import stat
import threading
import time
from urllib.parse import quote, urlsplit
import uuid


MIB = 1024 * 1024
LOG_CAP = MIB
INSPECTION_CAP = 32 * MIB + 1
PAYLOAD_CAP = 64 * MIB
MANIFEST_CAP = 65536
MAX_PROCESSES = 16
BOOTSTRAP_SCOPES = ("read:space_config", "write:space_config", "manage:grant", "manage:provider")
SPACES = {"qualification-household", "qualification-business"}
PLAN_FIELDS = {"format", "backend", "run_nonce", "endpoint", "spy_endpoint", "run_directory",
               "archive", "artifact_sha256", "binary_sha256", "source_commit", "target", "paired",
               "spaces", "principals", "worker_runtime_expectations", "spy_credential"}


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def spy_authorities(plan):
    """Only fixed synthetic credentials; never consume parent provider keys."""
    if "spy_credentials" not in plan:
        require(plan.get("spy_credential") == "synthetic-controlled-provider-key",
                "explicit controlled fake spy credential required")
        return {space: {"credential_ref": "FULL_WORKLOAD_SPY_KEY", "value": plan["spy_credential"]}
                for space in SPACES}
    expected = {space: {"credential_ref": "FULL_WORKLOAD_SPY_KEY_" + space.upper().replace("-", "_"),
                        "value": "synthetic-controlled-provider-key-" + space} for space in SPACES}
    require(plan.get("spy_credential") == "synthetic-controlled-provider-key"
            and encode(plan["spy_credentials"]) == encode(expected),
            "exact distinct per-space synthetic spy credentials required")
    return copy.deepcopy(expected)


async def retained_thread(function, /, *args, **kwargs):
    """Finish the same bounded worker before cancellation releases its owner."""
    task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
    return await retained_task(task)


async def retained_task(task):
    """Keep the original worker attached even through repeated cancellation."""
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                break
        if not task.cancelled():
            task.exception()
        raise


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False).encode("utf-8")


def decode(raw):
    def unique(pairs):
        value = {}
        for key, item in pairs:
            require(key not in value, "duplicate original JSON field")
            value[key] = item
        return value
    result = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
    encode(result)
    return result


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical(value):
    path = Path(value)
    require(path.is_absolute() and path.resolve() == path and not path.is_symlink(),
            "canonical original path required")
    return path


def loopback(value, *, api=False):
    parsed = urlsplit(value)
    require(parsed.scheme == "http" and parsed.hostname == "127.0.0.1"
            and parsed.port is not None and 0 < parsed.port <= 65535
            and not parsed.username and not parsed.password and not parsed.query
            and not parsed.fragment and parsed.path == ("/v1" if api else ""),
            "explicit original IPv4 loopback endpoint required")
    return parsed.port


def bounded(path, maximum, *, private=False):
    path = canonical(path)
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        before = os.fstat(stream.fileno())
        require(stat.S_ISREG(before.st_mode) and before.st_size <= maximum
                and (not private or (before.st_uid == os.getuid() and before.st_mode & 0o077 == 0)),
                "bounded original regular private input required")
        raw = stream.read(maximum + 1)
        after = os.fstat(stream.fileno())
    identity = lambda value: (value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns)
    require(identity(before) == identity(after) and len(raw) == before.st_size <= maximum,
            "original file changed or exceeded its bound")
    return raw


def new_private(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def worker_environment(expectations):
    """Set every configurable observed input; immutable tree defaults stay checked."""
    require(expectations.get("format") == "recall-full-workload-worker-expectations/v1",
            "original explicit worker recipe required")
    queue, embed = expectations["work_queue"], expectations["embed_worker"]
    require(queue == {"enabled": True, "max_concurrent": 1, "max_extract_concurrent": 1,
        "work_item_timeout_secs": 420, "transient_retry_max_attempts": 3,
        "transient_retry_base_delay_secs": 10, "transient_retry_max_delay_secs": 120,
        "allowed_kinds": [], "disabled_kinds": [], "doc_tree": {
            "max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
            "detect_sections": False, "semantic_chunking": True, "create_summaries": False,
            "summary_threshold": 1000, "summarizer_model": "claude-sonnet-4-5"}},
        "full-workload queue/tree expectation differs")
    require(embed == {"enabled": True, "batch_size": 100, "provider": "unconfigured",
                     "model": "text-embedding-3-large", "dimensions": 3072},
            "explicit global embedding template differs")
    return {"PIPELINE_WORK_QUEUE_ENABLED": "true", "PIPELINE_MAX_CONCURRENT": "1",
            "PIPELINE_MAX_EXTRACT_CONCURRENT": "1", "PIPELINE_WORK_ITEM_TIMEOUT_SECS": "420",
            "PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS": "3",
            "PIPELINE_WORK_TRANSIENT_RETRY_BASE_DELAY_SECS": "10",
            "PIPELINE_WORK_TRANSIENT_RETRY_MAX_DELAY_SECS": "120",
            "PIPELINE_WORK_QUEUE_ALLOWED_KINDS": "", "PIPELINE_WORK_QUEUE_DISABLED_KINDS": "",
            "PIPELINE_SUMMARIZER_MODEL": "claude-sonnet-4-5", "PIPELINE_EMBED_ENABLED": "true",
            "PIPELINE_EMBED_BATCH_SIZE": "100", "PIPELINE_EMBED_PROVIDER": "unconfigured",
            "PIPELINE_EMBED_MODEL": "text-embedding-3-large", "PIPELINE_EMBED_DIMS": "3072"}


class FullWorkloadOwnedRuntime:
    spy_authorities = staticmethod(spy_authorities)

    def __init__(self, *, allocation, runtime_plan, client, capture, deadline_ns,
                 whole_deadline_ns, allocation_clock, isolation, installed_helpers,
                 process_capture, artifact_helpers, inspection_helpers):
        require(set(runtime_plan) == PLAN_FIELDS | ({"spy_credentials"} if "spy_credentials" in runtime_plan else set())
                and runtime_plan["format"] == "recall-full-workload-runtime-plan/v1",
                "exact original runtime plan required")
        self.plan = copy.deepcopy(runtime_plan)
        self.allocation = copy.deepcopy(allocation)
        self.client, self.capture = client, capture
        self.deadline_ns, self.whole_deadline_ns = deadline_ns, whole_deadline_ns
        self.active_deadline_ns = min(deadline_ns, whole_deadline_ns) - 75 * 10**9
        self.allocation_clock, self.isolation = copy.deepcopy(allocation_clock), copy.deepcopy(isolation)
        self.helpers, self.pc, self.artifact, self.inspection = (
            installed_helpers, process_capture, artifact_helpers, inspection_helpers)
        require(installed_helpers.process_capture is process_capture
                and installed_helpers.artifact is artifact_helpers,
                "original source-verified helper objects differ")
        self.backend, self.nonce = self.plan["backend"], self.plan["run_nonce"]
        require(self.backend in ("elephant-db", "surreal-db")
                and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.nonce), "original backend/nonce required")
        for key in ("source_commit", "artifact_sha256", "binary_sha256"):
            length = 40 if key == "source_commit" else 64
            require(re.fullmatch("[a-f0-9]{" + str(length) + "}", self.plan[key])
                    and self.plan[key] == allocation.get(key), "original allocation identity differs: " + key)
        require(allocation.get("run_nonce") == self.nonce
                and allocation.get("backend") == self.backend
                and allocation.get("endpoint") == self.plan["endpoint"]
                and allocation.get("execution_authorized") is True
                and allocation.get("scope") == "controlled-owned-spy-only"
                and allocation.get("external_provider_sends") == 0
                and allocation.get("paid_model_calls") == 0, "original controlled allocation required")
        require(client.capture is capture and capture.maximum == 1024 * MIB and not client.tokens,
                "same original capture and initially empty signed-token map required")
        require(all(type(value) is int for value in (deadline_ns, whole_deadline_ns))
                and 0 < allocation_clock["mono_before_ns"] <= allocation_clock["mono_after_ns"] < deadline_ns
                and deadline_ns <= whole_deadline_ns
                and whole_deadline_ns - allocation_clock["mono_before_ns"] <= 3600 * 10**9
                and deadline_ns - allocation_clock["mono_before_ns"] <= 1800 * 10**9,
                "original integer backend clock differs")
        require(client.deadline <= process_capture.deadline_seconds(deadline_ns)
                and client.expires <= allocation_clock["expires_wall_ns"] / 10**9,
                "transport extends original allocation")
        self.root = canonical(self.plan["run_directory"])
        require(self.root.parent == canonical(capture.directory) and not self.root.exists(),
                "new runtime directory inside original Capture required")
        self.port = loopback(self.plan["endpoint"])
        require((client.host, client.port) == ("127.0.0.1", self.port), "original client endpoint differs")
        spy_port = loopback(self.plan["spy_endpoint"], api=True)
        require(spy_port != self.port, "owned spy and service ports must differ")
        authorities = spy_authorities(self.plan)
        self.spy_environment = {row["credential_ref"]: row["value"] for row in authorities.values()}
        spaces = self.plan["spaces"]
        require(type(spaces) is list and len(spaces) == 2 and {row["name"] for row in spaces} == SPACES
                and set(self.plan["principals"]) == SPACES, "both original named spaces required")
        for space in spaces:
            name = space["name"]
            require(set(self.plan["principals"][name]) == {"data", "manage", "revocable"}, "exact named principal roles required")
            for role in self.plan["principals"][name].values():
                require(set(role) == {"subject", "scopes"}
                        and re.fullmatch(r"[A-Za-z0-9_-]{1,128}", role["subject"])
                        and type(role["scopes"]) is list and role["scopes"]
                        and len(set(role["scopes"])) == len(role["scopes"])
                        and all(re.fullmatch(r"[a-z_:]+", scope) for scope in role["scopes"]),
                        "original explicit principal/scopes required")
            connections = space["processing"]["connections"]
            require(connections and all(value["endpoint"] == self.plan["spy_endpoint"]
                    and value["credential_ref"] == authorities[name]["credential_ref"] for value in connections.values()),
                    "original connections must target only the owned controlled spy")
        self.worker_env = worker_environment(self.plan["worker_runtime_expectations"])
        paired = self.plan["paired"]
        if self.backend == "elephant-db":
            require(paired is None, "embedded backend has no paired process")
        else:
            require(type(paired) is dict and set(paired) == {"binary", "binary_sha256", "endpoint"}
                    and re.fullmatch(r"[a-f0-9]{64}", paired["binary_sha256"]), "original paired artifact required")
            canonical(paired["binary"])
            require(loopback(paired["endpoint"]) not in (self.port, spy_port), "original service ports overlap")
        require(self.isolation.get("format") == "recall-owned-network-isolation/v1"
                and self.isolation.get("backend") == self.backend and self.isolation.get("run_nonce") == self.nonce,
                "original isolation carrier identity differs")
        self.state, self.recall, self.paired = "new", None, None
        self.processes, self.failures, self.configurations = [], [], {}
        self.journal, self.loop, self.loop_thread = None, None, None
        self.sequence, self.exports = 0, 0
        self.exported_publications = set()
        self.startup_receipt, self.cleanup_receipt = None, None
        self.lifetime_lock = asyncio.Lock()
        self.active_remaining()

    def active_remaining(self):
        seconds = min(self.remaining(), (self.active_deadline_ns - time.monotonic_ns()) / 10**9)
        require(seconds > 0, "original active allocation expired; cleanup reserve remains")
        return seconds

    def remaining(self):
        seconds = min((min(self.deadline_ns, self.whole_deadline_ns) - time.monotonic_ns()) / 10**9,
                      (self.allocation_clock["expires_wall_ns"] - time.time_ns()) / 10**9)
        require(seconds > 0, "original runtime deadline expired")
        return seconds

    def charge(self, amount):
        """Conservative retained reservation, serialized with the original Client."""
        def reserve():
            self.remaining()
            self.capture.reserve(amount)
            self.capture.held -= amount
            self.capture.written += amount
        if threading.get_ident() == self.loop_thread:
            reserve()
            return
        done = concurrent.futures.Future()
        def complete():
            try:
                reserve()
                done.set_result(None)
            except BaseException as error:
                done.set_exception(error)
        self.loop.call_soon_threadsafe(complete)
        done.result(timeout=self.remaining())

    async def save(self, label, value, kind="full-workload-runtime"):
        self.sequence += 1
        return await retained_thread(self.journal.save,
            f"captures/runtime-{self.sequence:04d}-{label}.json", encode(value), kind)

    def check_request(self, backend, nonce):
        require(backend == self.backend and nonce == self.nonce, "original backend/run nonce differs")
        require(self.state == "active" and self.recall is not None, "original runtime is not active")
        self.active_remaining()

    def ensure_ports_absent(self):
        # Bind tests send no request and never adopt a pre-existing listener.
        ports = [self.port]
        if self.plan["paired"] is not None:
            ports.append(loopback(self.plan["paired"]["endpoint"]))
        held = []
        try:
            for port in ports:
                self.remaining()
                probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                held.append(probe)
                probe.bind(("127.0.0.1", port))
        finally:
            for probe in held:
                probe.close()

    async def __aenter__(self):
        require(self.state == "new", "original runtime lifetime cannot be restarted")
        self.state = "starting"
        self.loop, self.loop_thread = asyncio.get_running_loop(), threading.get_ident()
        try:
            async with asyncio.timeout_at(self.pc.deadline_seconds(self.active_deadline_ns)):
                return await self.startup()
        except BaseException as error:
            self.state = "failed"
            self.failures.append({"phase": "startup", "error_type": type(error).__name__})
            await self.cleanup()
            raise

    async def startup(self):
        self.remaining()
        actual = self.helpers.offline_namespace(self.isolation["original_host_network_namespace"])
        require(actual["network_namespace"] == self.isolation["current_network_namespace"],
                "original isolation namespace changed")
        self.ensure_ports_absent()
        self.root.mkdir(mode=0o700)
        for name in ("captures", "logs", "private", "service", "paired", "media", "recovery", "backups"):
            (self.root / name).mkdir(mode=0o700)
        self.journal = self.pc.CaptureJournal(self.root, reserve=self.charge)
        self.controller = await retained_thread(self.pc.capture_controller, self.journal,
            self.deadline_ns, allocation_clock=self.allocation_clock)
        await self.save("isolation", actual)
        # Existing release verification checks every archive member, native
        # provenance and executable digest before installing a fresh tree.
        self.installed = self.root / "installed"
        manifest = await retained_thread(self.artifact.verify,
            canonical(self.plan["archive"]), self.plan["artifact_sha256"], self.installed)
        self.remaining()
        require(all(manifest.get(key) == self.plan[key] for key in ("source_commit", "binary_sha256", "target")),
                "actual verified artifact identity differs")
        self.binary = canonical(self.installed / "meerkat-recall")
        await self.save("artifact", manifest)
        self.env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "RUST_LOG": "warn",
            "HOME": str(self.root / "private"), "ELEPHANT_HOST": "127.0.0.1", "ELEPHANT_PORT": str(self.port),
            "JWT_ALGORITHM": "HS256", "JWT_ISSUER": "recall-full-workload", "JWT_AUDIENCE": "recall-full-workload",
            "ELEPHANT_DURABILITY_PATH": str(self.root / "recovery"),
            "ELEPHANT_SNAPSHOT_DIR": str(self.root / "backups"),
            **self.spy_environment, **self.worker_env}
        self.key = self.root / "private/signing.key"
        self.charge(16384)
        await self.command(["service-token", "bootstrap", "--secret-file", str(self.key)], "signing-key")
        bounded(self.key, 16384, private=True)
        self.env["JWT_SECRET_FILE"] = str(self.key)
        await self.credentials()
        await self.services()
        await self.spaces()
        self.state = "active"
        identity = await self.revalidate_original()
        self.startup_receipt = await self.save("startup", {
            "format": "recall-full-workload-owned-startup/v1", "backend": self.backend, "run_nonce": self.nonce,
            "source_commit": self.plan["source_commit"], "artifact_sha256": self.plan["artifact_sha256"],
            "binary_sha256": self.plan["binary_sha256"], "endpoint": self.plan["endpoint"],
            "controller": self.controller, "processes": identity, "schema_operation": self.schema_operation,
            "configurations": self.configurations, "ready_request_index": self.ready_index,
            "fresh_owned_store": True, "workers_enabled": True, "qualified": False,
            "product_acceptance": False, "deadline_ns": self.deadline_ns})
        return self

    async def __aexit__(self, kind, error, traceback):
        if error is not None:
            self.failures.append({"phase": "workload", "error_type": type(error).__name__})
        await self.cleanup()
        if error is None:
            require(not self.failures, "original runtime cleanup/capture failed")
        return False

    async def launch(self, binary, arguments, purpose, *, environment=None, cwd=None, inspection=False):
        self.active_remaining()
        require(len(self.processes) < MAX_PROCESSES, "original installed process subcap exhausted")
        number = len(self.processes) + 1
        logs = [self.root / "logs" / f"{number:02d}.{name}" for name in ("stdout", "stderr")]
        limits = (INSPECTION_CAP, LOG_CAP) if inspection else (LOG_CAP, LOG_CAP)
        kinds = ("snapshot-inspection", "process-log") if inspection else ("process-log", "process-log")
        if not inspection:
            self.charge(sum(limits))
        environment, cwd = environment or self.env, cwd or self.root / "service"
        entry = {"index": number, "purpose": purpose, "binary": Path(binary), "child": None,
                 "proof": None, "logs": logs, "limits": limits, "kinds": kinds}
        self.processes.append(entry)
        def create():
            options = {"stream_limits": limits, "stream_kinds": kinds} if inspection else {}
            proof = self.pc.ProcessProof(binary, arguments, cwd, environment, logs, self.active_deadline_ns,
                                         journal=self.journal, index=number, **options)
            entry["proof"] = proof
            expected = self.plan["binary_sha256"] if Path(binary) == self.binary else self.plan["paired"]["binary_sha256"]
            require(proof.launch_binary["sha256"] == expected, "original executable differs before launch")
            try:
                child = self.helpers.OwnedProcess(binary, arguments, cwd, environment, logs, LOG_CAP,
                    initialization_deadline=self.pc.deadline_seconds(min(self.active_deadline_ns, time.monotonic_ns()+5*10**9)),
                    capture_proof=proof,
                    **({"stream_limits": limits} if inspection else {}))
                entry["child"] = child
            except BaseException as error:
                entry["child"] = getattr(error, "owned_process", None)
                raise
            return child
        # Shield only the constructor: on cancellation obtain its genuine handle
        # before cleanup. No second launch or PID-based recovery is allowed.
        await retained_thread(create)
        return entry

    async def owned_wait(self, entry, timeout):
        require(entry.get("wait_task") is None, "original child already has a retained wait")
        task = asyncio.create_task(asyncio.to_thread(entry["child"].wait, timeout))
        entry["wait_task"] = task
        try:
            return await retained_task(task)
        finally:
            if task.done():
                entry["wait_task"] = None

    async def wait(self, entry, maximum):
        child = entry["child"]
        end = min(self.deadline_ns, self.whole_deadline_ns, time.monotonic_ns() + int(maximum * 10**9))
        while await retained_thread(child.poll) is None:
            self.remaining()
            require(time.monotonic_ns() < end, "owned command did not exit within original bound")
            require(not child.failed.is_set(), "owned process output capture failed")
            await asyncio.sleep(min(0.05, self.remaining()))
        code = await self.owned_wait(entry, min(self.remaining(), max(0.001, (end-time.monotonic_ns()) / 10**9)))
        proof = await retained_thread(entry["proof"].snapshot)
        await self.save("process-terminal", proof, "installed-process-terminal")
        require(code == 0 and proof["exit_code"] == 0 and proof["reaped"]
                and not proof["errors"] and not child.failed.is_set()
                and len(proof["logs"]) == 2 and all(row["complete"] and row["ref"] for row in proof["logs"]),
                "original process did not exit cleanly with complete output")
        return proof

    async def command(self, arguments, purpose, *, maximum=15, inspection=False):
        entry = await self.launch(self.binary, arguments, purpose, inspection=inspection)
        return entry, await self.wait(entry, maximum)

    async def credentials(self):
        await self.issue("runtime:operator", "fw-runtime-operator", ["read:status", "read:system", "write:system"])
        await self.issue("runtime:inspect", "fw-runtime-inspector", ["read:system"])
        for space in self.plan["spaces"]:
            name = space["name"]
            await self.issue(name + ":bootstrap", "fw-bootstrap-" + name, list(BOOTSTRAP_SCOPES), space, bootstrap=True)
            for role in ("data", "manage", "revocable"):
                principal = self.plan["principals"][name][role]
                await self.issue(name + ":" + role, principal["subject"], principal["scopes"], space)

    async def issue(self, role, subject, scopes, space=None, *, bootstrap=False):
        stem = role.replace(":", "-")
        path = self.root / "private" / (stem + ".token")
        # All tokens expire inside the original allocation. CLI management
        # tokens also respect the existing one-hour product ceiling.
        seconds = min(int(self.remaining()), 1800)
        require(seconds > 0, "no original token lifetime remains")
        args = ["service-token", "issue", "--secret-file", str(self.key), "--subject", subject,
                "--expires-in", str(seconds), "--issuer", self.env["JWT_ISSUER"],
                "--audience", self.env["JWT_AUDIENCE"], "--output", str(path)]
        if space is None:
            args.append("--global-admin")
        else:
            clearances = [{"policy_id": space["security"]["policy_id"], "max_level": space["security"]["level"],
                           "labels": [], "handling_allow": []}]
            clearance_path = self.root / "private" / (stem + ".clearances.json")
            raw = encode(clearances)
            self.charge(len(raw))
            new_private(clearance_path, raw)
            args += ["--bootstrap-space" if bootstrap else "--space", space["name"],
                     "--clearances-file", str(clearance_path)]
        for scope in scopes:
            args += ["--scope", scope]
        self.charge(16384)
        await self.command(args, "issue-" + stem)
        token = bounded(path, 16384, private=True).decode("ascii").strip()
        require(re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", token)
                and role not in self.client.tokens, "actual original CLI token missing or replaced")
        self.client.tokens[role] = token

    @staticmethod
    def successful(row, status=200):
        require(row.get("error_type") is None and row.get("framing_complete") is True
                and row.get("status") == status and type(row.get("body")) is dict,
                "complete original successful HTTP response required")
        return row["body"]

    async def runtime_request(self, method, path, body=None):
        self.remaining()
        return await self.client.owner_request(method, path, body, response_limit=262144)

    async def tool(self, name, arguments):
        identifier = self.client.count + 1
        row = await self.runtime_request("POST", "/mcp/full", {"jsonrpc": "2.0", "id": identifier,
            "method": "tools/call", "params": {"name": name, "arguments": arguments}})
        envelope = self.successful(row)
        require(set(envelope) == {"jsonrpc", "id", "result"} and envelope["id"] == identifier
                and envelope["jsonrpc"] == "2.0", "actual original MCP response differs")
        result = envelope["result"]
        require(result.get("isError") is not True and type(result.get("content")) is list
                and len(result["content"]) == 1 and result["content"][0].get("type") == "text",
                "actual successful runtime tool result required")
        return decode(result["content"][0]["text"].encode()), row["id"]

    async def readiness(self, entry, *, paired=False, ready=False):
        end = min(self.deadline_ns, time.monotonic_ns() + 30 * 10**9)
        while time.monotonic_ns() < end:
            self.remaining()
            require(await retained_thread(entry["child"].poll) is None and not entry["child"].failed.is_set(),
                    "original service exited or output capture failed during startup")
            row = await self.client.paired_health(self.plan["paired"]["endpoint"]) if paired else await self.runtime_request(
                "GET", "/v1/readiness" if ready else "/v1/health")
            if row.get("error_type") is None and row.get("framing_complete") and row.get("status") == 200:
                if not ready or all(row["body"].get(key) is True for key in
                        ("ready", "configured", "storage_ready", "recovery_ready")) and row["body"].get("draining") is False:
                    return row
            await asyncio.sleep(min(0.25, self.remaining()))
        raise ValueError("original service readiness window expired")

    async def services(self):
        if self.backend == "surreal-db":
            paired = self.plan["paired"]
            password = "synthetic-" + secrets.token_urlsafe(32)
            database_env = {"PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "RUST_LOG": "warn",
                            "HOME": str(self.root / "private"), "SURREAL_USER": "qualification", "SURREAL_PASS": password}
            self.client.bind_paired_health(paired["endpoint"])
            self.paired = await self.launch(canonical(paired["binary"]),
                ["start", "--bind", "127.0.0.1:" + str(loopback(paired["endpoint"])),
                 "file:" + str(self.root / "paired/database.db")], "paired-persistent-server",
                environment=database_env, cwd=self.root / "paired")
            await self.readiness(self.paired, paired=True)
            self.env.update(ELEPHANT_DB="surreal-db", SURREALDB_URL=paired["endpoint"].replace("http://", "ws://", 1),
                SURREALDB_NAMESPACE="full-workload", SURREALDB_DATABASE="fixture",
                SURREALDB_USER="qualification", SURREALDB_PASS=password)
            entry, proof = await self.command(["migrate"], "explicit-fresh-schema", maximum=90)
            self.schema_operation = {"kind": "migrate", "process_index": entry["index"], "exit_code": proof["exit_code"]}
        self.recall = await self.launch(self.binary, ["serve", "--server-only", "--shutdown-timeout-seconds", "60"],
                                        "recall-authenticated-service")
        await self.readiness(self.recall)
        identifier = self.client.count + 1
        initialized = self.successful(await self.runtime_request("POST", "/mcp/full", {"jsonrpc": "2.0",
            "id": identifier, "method": "initialize", "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                "clientInfo": {"name": "full-workload-runtime", "version": "1"}}}))
        require(initialized.get("id") == identifier and "result" in initialized and "error" not in initialized,
                "original runtime MCP initialization failed")
        patch = {"ingest_enabled": False, "extractor_required": False, "multimodal_asset_root": str(self.root / "media")}
        if self.backend == "elephant-db":
            row = await self.runtime_request("GET", "/v1/readiness")
            require(row.get("status") == 503 and row.get("framing_complete") is True
                    and row.get("error_type") is None and row["body"].get("configured") is False,
                    "fresh serve silently initialized storage")
            value, index = await self.tool("initialize_runtime", {**patch, "backend": "elephant-db",
                                          "snapshot_path": str(self.root / "service/state.snapshot")})
            self.schema_operation = {"kind": "initialize_runtime", "request_index": index}
        else:
            value, _ = await self.tool("configure_runtime", patch)
        require(value.get("configured") is True and value.get("storage_ready") is True
                and value.get("backend") == self.backend, "explicit original schema initialization failed")
        await self.readiness(self.recall, ready=True)

    async def spaces(self):
        for request in self.plan["spaces"]:
            name = request["name"]
            row = await self.client.request("POST", "/v1/spaces", name, request, role="bootstrap")
            actual = self.successful(row, 201)
            require(actual.get("space_id") == name and actual.get("created_by") == "fw-bootstrap-" + name
                    and type(actual.get("id")) is str and actual["id"].startswith("space_config:")
                    and all(actual.get(key) == value for key, value in request.items() if key != "name"),
                    "actual named configuration differs from original create intent")
            path = "/v1/spaces/" + quote(name, safe="")
            require(self.successful(await self.client.request("GET", path, name, role="bootstrap")) == actual,
                    "actual configuration readback differs")
            self.configurations[name] = actual
            for role, principal in self.plan["principals"][name].items():
                grant_path = path + "/grants/" + quote(principal["subject"], safe="")
                body = {"active": True, "scopes": principal["scopes"], "expected_generation": None}
                grant = self.successful(await self.client.request("PUT", grant_path, name, body, role="bootstrap"))
                require(grant.get("space_id") == name and grant.get("subject") == principal["subject"]
                        and grant.get("active") is True and set(grant.get("scopes", [])) == set(principal["scopes"])
                        and type(grant.get("generation")) is str and grant["generation"], "actual original grant differs")
                require(self.successful(await self.client.request("GET", grant_path, name, role="bootstrap")) == grant,
                        "actual original grant readback differs")
            empty = self.successful(await self.client.request("GET", path + "/sources?limit=100", name, role="data"))
            require(empty.get("sources") == [] and empty.get("next_cursor") is None, "new owned space is not empty")
        await self.tool("configure_runtime", {"ingest_enabled": True, "extractor_required": False})
        row = await self.readiness(self.recall, ready=True)
        require(row["body"].get("worker_ready") is True and row["body"].get("semantic_ingest_ready") is True,
                "actual original workers are not ready")
        self.ready_index = row["id"]

    async def current_identity(self, entry):
        self.active_remaining()
        child, proof = entry["child"], entry["proof"]
        require(child is not None and child.capture_proof is proof and await retained_thread(child.poll) is None
                and not child.reaped and not child.failed.is_set(), "original owned process is no longer live")
        def observe():
            identity = proof.identity
            path = "/proc/" + str(identity["pid"])
            before = self.pc.read_bounded(path + "/stat", 8192)
            require(self.pc.process_identity(before) == identity, "original process identity changed")
            executable = self.pc.binary_identity(path + "/exe", self.active_deadline_ns, proc_link=True)
            target = os.readlink(path + "/exe")
            after = self.pc.read_bounded(path + "/stat", 8192)
            require(self.pc.process_identity(after) == identity
                    and target == str(entry["binary"])
                    and all(executable[key] == proof.launch_binary[key]
                            for key in ("sha256", "device", "inode", "bytes", "mtime_ns", "ctime_ns")),
                    "original executable or process identity changed during capture")
            refs = [self.journal.save(f"captures/current-{self.sequence:04d}-{entry['index']:02d}-{suffix}.stat",
                                    raw, "process-stat") for suffix, raw in (("before", before), ("after", after))]
            return ({"executable_sha256": executable["sha256"], "process": copy.deepcopy(identity)},
                    {"process_index": entry["index"], "identity": identity,
                     "executable": executable, "target": target, "stat": refs})
        self.sequence += 1
        identity, evidence = await retained_thread(observe)
        await self.save("current-executable", evidence)
        return identity

    async def revalidate_original(self):
        self.check_request(self.backend, self.nonce)
        recall = await self.current_identity(self.recall)
        paired = await self.current_identity(self.paired) if self.paired is not None else None
        self.check_request(self.backend, self.nonce)
        return {"format": "recall-full-workload-process-identity/v1", "backend": self.backend,
                "run_nonce": self.nonce, "processes": {"recall": recall, "paired": paired}}

    async def stop_original(self, backend, nonce):
        async with self.lifetime_lock:
            return await self._stop_original(backend, nonce)

    async def _stop_original(self, backend, nonce):
        self.check_request(backend, nonce)
        original = await self.revalidate_original()
        self.remaining()
        self.state = "stopping"
        try:
            await retained_thread(self.recall["child"].signal, signal.SIGTERM)
            proof = await self.wait(self.recall, 60)
            self.state = "stopped"
            return await self.save("original-stop", {"format": "recall-full-workload-original-stop/v1",
                "backend": self.backend, "run_nonce": self.nonce, "original": original,
                "process_index": self.recall["index"], "terminal": proof,
                "paired_retained": self.paired is not None, "qualified": False, "product_acceptance": False})
        except BaseException as error:
            self.state = "failed"
            self.failures.append({"phase": "original-stop", "error_type": type(error).__name__})
            raise

    @staticmethod
    def publication(observed):
        require(type(observed) is dict, "actual completed Graph observation required")
        accepted, operation, source, doc, revision, head = (observed[key] for key in
            ("accepted", "operation", "source", "doc", "immutable_revision", "head"))
        require(operation.get("state") == head.get("state") == "complete"
                and source.get("lifecycle_status") == "graph_ready" and source.get("current_ingest_mode") == "graph"
                and source.get("id") == accepted.get("source_id") == doc.get("source")
                and doc.get("id") == accepted.get("doc_id") == revision.get("doc")
                and source.get("space_id") in SPACES and doc.get("space_id") == revision.get("space_id") == source["space_id"]
                and doc.get("text") == revision.get("text") and type(doc.get("text")) is str
                and sha(doc["text"].encode()) == revision.get("text_hash"),
                "actual completed source/document/revision binding differs")
        accepted_operation = accepted.get("operation", {})
        for key, original_key in (("operation_id", "operation_id"), ("generation", "source_generation"),
                                  ("desired_revision", "desired_revision"), ("internal_source_id", "source_id")):
            require(type(accepted_operation.get(original_key)) is str and accepted_operation[original_key]
                    and operation.get(key) == head.get(key) == accepted_operation[original_key],
                    "original publication identity differs: " + key)
        require(accepted_operation["source_id"] == source["id"]
                and re.fullmatch(r"[a-f0-9]{64}", source.get("hash", ""))
                and operation.get("published_revision") == head.get("published_revision") == accepted_operation["desired_revision"]
                and operation.get("lifecycle") == head.get("lifecycle") == "active"
                and operation.get("error_code") is None, "original full source content identity missing")
        return {"space_id": source["space_id"], "source_id": source["id"], "doc_id": doc["id"],
                "operation_id": accepted_operation["operation_id"], "generation": accepted_operation["source_generation"],
                "source_revision_id": accepted_operation["desired_revision"], "immutable_revision_id": revision["id"],
                "raw_sha256": source["hash"], "rendered_sha256": revision["text_hash"],
                "rendered_bytes": len(doc["text"].encode())}

    async def capture_original_export(self, backend, nonce, observed):
        async with self.lifetime_lock:
            before = self.exports
            try:
                return await self._capture_original_export(backend, nonce, observed)
            except BaseException as error:
                if self.exports != before:
                    self.failures.append({"phase": "capacity-export", "error_type": type(error).__name__})
                    self.state = "failed"
                raise

    async def _capture_original_export(self, backend, nonce, observed):
        self.check_request(backend, nonce)
        publication = self.publication(observed)
        key = (publication["source_id"], publication["generation"], publication["operation_id"])
        require(key not in self.exported_publications and self.exports < 2, "original capacity export duplicated or exceeds two sources")
        original = await self.revalidate_original()
        self.charge(PAYLOAD_CAP + MANIFEST_CAP)
        self.exports += 1
        self.exported_publications.add(key)
        name = f"full-workload-{self.nonce}-{self.exports}"
        bundle = self.root / "backups" / name
        require(not bundle.exists() and not bundle.is_symlink(), "original export destination already exists")
        row = await self.runtime_request("POST", "/v1/system/snapshot/export", {
            "path": name, "name": name, "retention_seconds": 3600,
            "quiesce_timeout_seconds": min(30, max(1, int(self.remaining())))})
        body = self.successful(row)
        require(body.get("success") is True and type(body.get("manifest")) is dict, "actual quiesced export did not succeed")
        manifest = body["manifest"]
        require(canonical(bundle).parent == self.root / "backups"
                and {path.name for path in bundle.iterdir()} == {"manifest.json", "data.snapshot"},
                "original immutable export bundle shape differs")
        manifest_raw = bounded(bundle / "manifest.json", MANIFEST_CAP)
        payload = await retained_thread(bounded, bundle / "data.snapshot", PAYLOAD_CAP)
        require(decode(manifest_raw) == manifest and manifest.get("version") == 1
                and manifest.get("format") == "recall-snapshot-v1" and manifest.get("backend") == self.backend
                and manifest.get("name") == name and manifest.get("bytes") == len(payload)
                and manifest.get("sha256") == sha(payload), "actual export payload/manifest differs")
        for field in ("snapshot_id", "journal_id"):
            require(type(manifest.get(field)) is str and str(uuid.UUID(manifest[field])) == manifest[field],
                    "actual snapshot/journal identity missing")
        self.remaining()
        entry, terminal = await self.command(["snapshot", "inspect", "--input", str(bundle), "--json",
            "--include-records", "--compact-records"], "original-capacity-inspection", maximum=60, inspection=True)
        report_raw = await retained_thread(bounded, entry["logs"][0], INSPECTION_CAP, private=True)
        report_ref = terminal["logs"][0]["ref"]
        require(report_ref == {"sha256": sha(report_raw), "bytes": len(report_raw), "kind": "snapshot-inspection"},
                "complete original report proof differs")
        report = self.inspection.checked_inspection(report_raw, manifest, expected_format="recall-snapshot-inspection-v2")
        require(bounded(bundle / "manifest.json", MANIFEST_CAP) == manifest_raw
                and sha(await retained_thread(bounded, bundle / "data.snapshot", PAYLOAD_CAP)) == manifest["sha256"], "retained original bundle changed after inspection")
        require(report["manifest"] == manifest and await self.revalidate_original() == original,
                "original runtime changed during export inspection")
        return await self.save("capacity-export", {"format": "recall-full-workload-capacity-export/v1",
            "backend": self.backend, "run_nonce": self.nonce, "publication": publication, "original": original,
            "request_index": row["id"], "manifest": manifest,
            "manifest_capture": {"path": str(bundle / "manifest.json"), "ref": {
                "sha256": sha(manifest_raw), "bytes": len(manifest_raw), "kind": "snapshot-manifest"}},
            "payload_capture": {"path": str(bundle / "data.snapshot"), "ref": {
                "sha256": sha(payload), "bytes": len(payload), "kind": "snapshot-payload"}},
            "report_capture": {"path": str(entry["logs"][0]), "ref": report_ref}, "process_terminal": terminal,
            "qualified": False, "product_acceptance": False,
            "physical_acceptance": "unmet: independent original physical-report oracle required"})

    async def cleanup(self):
        # Reverse launch order stops Recall/inspection before the paired server.
        # Retain every original handle even when construction, wait or proof fails.
        for entry in reversed(self.processes):
            child = entry["child"]
            if child is None or child.reaped:
                continue
            try:
                self.remaining()
                if await retained_thread(child.poll) is None:
                    await retained_thread(child.signal, signal.SIGTERM)
                try:
                    await self.wait(entry, min(60 if entry["purpose"] == "recall-authenticated-service" else 5, self.remaining()))
                except BaseException as error:
                    self.failures.append({"phase": "cleanup-graceful", "process_index": entry["index"],
                                          "error_type": type(error).__name__})
                    self.remaining()
                    if await retained_thread(child.poll) is None:
                        await retained_thread(child.signal, signal.SIGKILL)
                    await self.owned_wait(entry, min(5, self.remaining()))
            except BaseException as error:
                self.failures.append({"phase": "cleanup", "process_index": entry["index"],
                                      "error_type": type(error).__name__})
        if self.journal is not None:
            try:
                proofs = [await retained_thread(entry["proof"].snapshot) for entry in self.processes if entry["proof"]]
                self.cleanup_receipt = await self.save("cleanup", {"format": "recall-full-workload-runtime-cleanup/v1", "backend": self.backend,
                    "run_nonce": self.nonce, "processes": proofs, "errors": self.failures,
                    "all_original_children_reaped": all(entry["child"] is None or entry["child"].reaped for entry in self.processes),
                    "qualified": False, "product_acceptance": False})
            except BaseException as error:
                self.failures.append({"phase": "cleanup-capture", "error_type": type(error).__name__})
        self.state = "failed" if self.failures else "stopped"
