"""Closed-world controls: no child, socket, provider, database or archive runs."""
import asyncio
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch
import uuid


PATH = Path(__file__).with_name("full_workload_owned_runtime.py")
SPEC = importlib.util.spec_from_file_location("full_workload_owned_runtime", PATH)
owner_module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(owner_module)
Owner = owner_module.FullWorkloadOwnedRuntime


def worker_expectations():
    return {"format": "recall-full-workload-worker-expectations/v1", "work_queue": {
        "enabled": True, "max_concurrent": 1, "max_extract_concurrent": 1, "work_item_timeout_secs": 420,
        "transient_retry_max_attempts": 3, "transient_retry_base_delay_secs": 10,
        "transient_retry_max_delay_secs": 120, "allowed_kinds": [], "disabled_kinds": [],
        "doc_tree": {"max_chunk_size": 512, "chunk_overlap": 50, "create_paragraphs": True,
                     "detect_sections": False, "semantic_chunking": True, "create_summaries": False,
                     "summary_threshold": 1000, "summarizer_model": "claude-sonnet-4-5"}},
        "embed_worker": {"enabled": True, "batch_size": 100, "provider": "unconfigured",
                         "model": "text-embedding-3-large", "dimensions": 3072},
        "embedding_execution": {name: {kind: {"max_total_http_attempts": 3, "max_inputs_per_call": 100,
            "initial_segment_bytes": 2048, "max_split_depth": 16} for kind in ("query_embedding", "document_embedding")}
            for name in sorted(owner_module.SPACES)}}


class Capture:
    def __init__(self, directory):
        self.directory, self.maximum = directory, 1024 * owner_module.MIB
        self.held = self.written = 0
    def reserve(self, amount):
        if amount <= 0 or self.written + self.held + amount > self.maximum:
            raise ValueError("original capture budget exhausted")
        self.held += amount


class Journal:
    def __init__(self, root, *, reserve):
        self.root, self.reserve, self.processes = root, reserve, []
        self.lock = threading.Lock()
    def save(self, name, raw, kind):
        with self.lock:
            self.reserve(len(raw))
            path = self.root / name
            owner_module.new_private(path, raw)
            return {"path": str(path), "ref": {"sha256": owner_module.sha(raw), "bytes": len(raw), "kind": kind}}


class Proof:
    def __init__(self, fixture, binary, arguments, cwd, environment, logs, deadline_ns,
                 *, journal, index, stream_limits=None, stream_kinds=None):
        self.fixture, self.index, self.journal = fixture, index, journal
        self.logs, self.arguments = logs, arguments
        self.identity = {"pid": 1000+index, "ppid": 1, "pgrp": 1000+index, "session": 1000+index, "start_ticks": index}
        self.launch_binary = fixture.binary_identity(str(binary), deadline_ns)
        self.kinds = stream_kinds or ("process-log", "process-log")
        self.limits = stream_limits or (owner_module.LOG_CAP, owner_module.LOG_CAP)
        self.reaped, self.exit_code = False, None
        self.errors, self.complete = [], True
        if stream_limits:
            self.journal.reserve(sum(stream_limits))
        self.intent = {"argv": [str(binary), *arguments], "cwd": str(cwd),
                       "environment_names": sorted(environment), "deadline_ns": deadline_ns}
        journal.processes.append(self)
    def snapshot(self):
        return {"index": self.index, "intent": self.intent, "identity": self.identity,
                "reaped": self.reaped, "exit_code": self.exit_code, "errors": self.errors,
                "logs": [{"path": str(path), "complete": self.reaped and self.complete,
                          "ref": {"sha256": owner_module.sha(path.read_bytes()), "bytes": path.stat().st_size,
                                  "kind": kind} if self.reaped and self.complete else None}
                         for path, kind in zip(self.logs, self.kinds)]}


class Process:
    def __init__(self, fixture, binary, arguments, cwd, environment, logs, cap,
                 *, initialization_deadline, capture_proof, stream_limits=None):
        self.capture_proof, self.reaped, self.failed = capture_proof, False, threading.Event()
        self.arguments, self.signals, self.environment = arguments, [], dict(environment)
        self.alive = arguments[0] in ("serve", "start")
        self.code = None if self.alive else 0
        fixture.launches.append(self)
        for path in logs:
            owner_module.new_private(path, b"")
        if arguments[:2] == ["service-token", "bootstrap"]:
            owner_module.new_private(Path(arguments[arguments.index("--secret-file")+1]), b"controlled-fake-key\n")
        if arguments[:2] == ["service-token", "issue"]:
            owner_module.new_private(Path(arguments[arguments.index("--output")+1]), b"actual.mock.token")
        if arguments[:2] == ["snapshot", "inspect"]:
            bundle = Path(arguments[arguments.index("--input")+1])
            manifest = json.loads((bundle / "manifest.json").read_bytes())
            report = {"format": "recall-snapshot-inspection-v2", "manifest": manifest, "rows": []}
            logs[0].write_bytes(owner_module.encode(report) + b"\n")
            if fixture.mutate_report:
                capture_proof.complete = False
            if fixture.mutate_bundle_after_inspection:
                (bundle / "data.snapshot").write_bytes(b"changed-after-inspection")
    def poll(self):
        return self.code
    def signal(self, number):
        self.signals.append(number)
        self.alive = False
        self.code = 0 if number == signal.SIGTERM else -9
    def wait(self, timeout):
        if self.code is None:
            raise TimeoutError("mock child is still live")
        self.reaped = self.capture_proof.reaped = True
        self.capture_proof.exit_code = self.code
        return self.code


class Client:
    def __init__(self, fixture, capture, deadline, expires):
        self.fixture, self.capture = fixture, capture
        self.host, self.port, self.tokens = "127.0.0.1", 43130, {}
        self.deadline, self.expires, self.clock = deadline, expires, time.monotonic
        self.count, self.configured, self.workers = 0, False, False
        self.rows, self.configs, self.grants = [], {}, {}
        self.paired_endpoint = None
    def row(self, method, path, body, status=200):
        self.count += 1
        row = {"id": self.count, "method": method, "path": path, "body": body,
               "status": status, "framing_complete": True, "error_type": None}
        self.rows.append(row)
        return row
    def bind_paired_health(self, endpoint):
        assert self.paired_endpoint is None
        self.paired_endpoint = endpoint
    async def paired_health(self, endpoint):
        assert endpoint == self.paired_endpoint
        return self.row("GET", "/health", None)
    async def runtime_request(self, body):
        raise AssertionError("owner must not use the read-only inspection transport for mutations")
    async def owner_request(self, method, path, body=None, response_limit=None):
        assert "runtime:operator" in self.tokens
        if path == "/v1/health":
            value, status = {}, 200
        elif path == "/v1/readiness":
            value = {"configured": self.configured, "ready": self.configured,
                     "storage_ready": self.configured, "recovery_ready": True, "draining": False,
                     "worker_ready": self.workers, "semantic_ingest_ready": self.workers}
            status = 200 if self.configured else 503
        elif path == "/mcp/full":
            if body["method"] == "initialize":
                result = {"protocolVersion": "2025-03-26"}
            else:
                args = body["params"]["arguments"]
                self.configured = True
                self.workers = args.get("ingest_enabled", self.workers)
                result = {"content": [{"type": "text", "text": json.dumps({"configured": True,
                    "storage_ready": True, "backend": self.fixture.plan["backend"]})}], "isError": False}
            value, status = {"jsonrpc": "2.0", "id": body["id"], "result": result}, 200
        elif path == "/v1/system/snapshot/export":
            bundle = self.fixture.owner.root / "backups" / body["path"]
            bundle.mkdir(mode=0o700)
            payload = b"original mock persisted payload"
            manifest = {"version": 1, "format": "recall-snapshot-v1", "snapshot_id": str(uuid.uuid4()),
                        "journal_id": str(uuid.uuid4()), "backend": self.fixture.plan["backend"],
                        "name": body["name"], "bytes": len(payload), "sha256": owner_module.sha(payload),
                        "created_at": "2026-10-07T04:00:00Z", "expires_at": "2026-10-07T05:00:00Z"}
            owner_module.new_private(bundle / "manifest.json", owner_module.encode(manifest))
            owner_module.new_private(bundle / "data.snapshot", payload if not self.fixture.mutate_payload else b"foreign")
            value, status = {"success": True, "manifest": manifest}, 200
        else:
            raise AssertionError((method, path))
        return self.row(method, path, value, status)
    async def request(self, method, path, space, body=None, *, role):
        assert space+":"+role in self.tokens
        if method == "POST" and path == "/v1/spaces":
            actual = {key: value for key, value in body.items() if key != "name"}
            actual.update(space_id=space, id="space_config:"+str(uuid.uuid4()), created_by="fw-bootstrap-"+space)
            self.configs[space] = actual
            return self.row(method, path, actual, 201)
        if "/grants/" in path:
            if method == "PUT":
                self.grants[path] = {"space_id": space, "subject": path.rsplit("/", 1)[1],
                    "active": body["active"], "scopes": body["scopes"], "generation": str(uuid.uuid4())}
            value = self.grants[path]
        elif "/sources?" in path:
            value = {"sources": [], "next_cursor": None}
        else:
            value = self.configs[space]
        return self.row(method, path, value)


class Fixture:
    def __init__(self, root, backend="elephant-db"):
        self.launches, self.mutate_payload, self.mutate_report, self.mutate_bundle_after_inspection = [], False, False, False
        self.plan = {"format": "recall-full-workload-runtime-plan/v1", "backend": backend, "run_nonce": "original-run",
            "endpoint": "http://127.0.0.1:43130", "spy_endpoint": "http://127.0.0.1:43129/v1",
            "run_directory": str(root / "owned"), "archive": str(root / "candidate.tar.gz"),
            "artifact_sha256": "a"*64, "binary_sha256": "b"*64, "source_commit": "c"*40,
            "target": "x86_64-unknown-linux-gnu", "paired": None, "spaces": [], "principals": {},
            "worker_runtime_expectations": worker_expectations(), "spy_credential": "synthetic-controlled-provider-key"}
        if backend == "surreal-db":
            self.plan["paired"] = {"binary": str(root / "paired-binary"), "binary_sha256": "d"*64,
                                   "endpoint": "http://127.0.0.1:43131"}
        for name in sorted(owner_module.SPACES):
            self.plan["spaces"].append({"name": name, "security": {"policy_id": name, "level": 3},
                "processing": {"connections": {"owned-spy": {"endpoint": self.plan["spy_endpoint"],
                                                           "credential_ref": "FULL_WORKLOAD_SPY_KEY"}}}})
            self.plan["principals"][name] = {role: {"subject": "fw-"+role+"-"+name,
                "scopes": ["read:space_config", "read:source"]} for role in ("data", "manage", "revocable")}
        self.allocation = {key: self.plan[key] for key in ("backend", "run_nonce", "endpoint", "source_commit", "artifact_sha256", "binary_sha256")}
        self.allocation.update(execution_authorized=True, scope="controlled-owned-spy-only", external_provider_sends=0, paid_model_calls=0)
        now = time.monotonic_ns()
        self.deadline = now + 1700*10**9
        self.clock = {"mono_before_ns": now, "mono_after_ns": now, "wall_ns": time.time_ns(),
                      "expires_wall_ns": time.time_ns()+1700*10**9, "total_seconds": 1800}
        self.capture = Capture(root)
        self.client = Client(self, self.capture, self.deadline / 10**9, self.clock["expires_wall_ns"] / 10**9)
        self.pc = SimpleNamespace(CaptureJournal=Journal, deadline_seconds=lambda ns: ns/10**9,
            ProcessProof=lambda *args, **kw: Proof(self, *args, **kw),
            capture_controller=lambda journal, deadline, **kw: journal.save("captures/controller.json", b"{}", "installed-controller"),
            binary_identity=self.binary_identity, process_identity=json.loads, read_bounded=self.process_stat)
        self.artifact = SimpleNamespace(verify=self.verify_artifact)
        self.helpers = SimpleNamespace(process_capture=self.pc, artifact=self.artifact,
            offline_namespace=lambda parent: {"parent_network_namespace": parent, "network_namespace": "net:[2]"},
            OwnedProcess=lambda *args, **kw: Process(self, *args, **kw))
        self.inspection = SimpleNamespace(checked_inspection=self.checked_inspection)
        self.arguments = dict(allocation=self.allocation, runtime_plan=self.plan, client=self.client,
            capture=self.capture, deadline_ns=self.deadline, whole_deadline_ns=self.deadline,
            allocation_clock=self.clock, isolation={"format": "recall-owned-network-isolation/v1", "backend": backend, "run_nonce": "original-run",
                "original_host_network_namespace": "net:[1]", "current_network_namespace": "net:[2]",
                "verdict": "passed", "outbound_denial_observed": True, "external_provider_sends": 0,
                "original_probe_capture_sha256": "e"*64},
            installed_helpers=self.helpers, process_capture=self.pc, artifact_helpers=self.artifact,
            inspection_helpers=self.inspection)
        self.owner = Owner(**self.arguments)
        self.owner.ensure_ports_absent = lambda: None
    def verify_artifact(self, archive, expected, destination):
        destination.mkdir(mode=0o700)
        (destination / "meerkat-recall").write_bytes(b"mock bytes never executed")
        return {key: self.plan[key] for key in ("source_commit", "binary_sha256", "target")}
    def binary_identity(self, path, deadline, **kw):
        paired = self.plan["paired"]
        pair = paired is not None and str(path) == paired["binary"]
        if str(path).startswith("/proc/"):
            index = int(str(path).split("/")[2])-1000
            proof = self.owner.processes[index-1]["proof"]
            return dict(proof.launch_binary)
        return {"sha256": "d"*64 if pair else "b"*64, "device": 1, "inode": 2 if pair else 1,
                "bytes": 25, "mtime_ns": 1, "ctime_ns": 1, "path": str(path)}
    def process_stat(self, path, maximum):
        index = int(str(path).split("/")[2])-1000
        return owner_module.encode(self.owner.processes[index-1]["proof"].identity)
    def executable_link(self, path):
        index = int(str(path).split("/")[2])-1000
        return str(self.owner.processes[index-1]["binary"])
    @staticmethod
    def checked_inspection(raw, manifest, *, expected_format):
        value = json.loads(raw)
        if value["format"] != expected_format or value["manifest"] != manifest:
            raise ValueError("original report manifest differs")
        return value


def publication(label="one"):
    source, doc, revision = "source:"+label, "doc:"+label, "doc_revision:"+label
    text = "Original full rendered content "+label
    operation = {"operation_id": "op-"+label, "internal_source_id": source, "generation": "generation-"+label,
                 "desired_revision": "revision-"+label, "published_revision": "revision-"+label,
                 "lifecycle": "active", "state": "complete", "error_code": None}
    accepted = {"source_id": source, "doc_id": doc, "operation": {"operation_id": operation["operation_id"],
                "source_id": source, "source_generation": operation["generation"], "desired_revision": operation["desired_revision"]}}
    return {"accepted": accepted, "operation": operation, "head": dict(operation),
        "source": {"id": source, "space_id": "qualification-household", "lifecycle_status": "graph_ready",
                   "current_ingest_mode": "graph", "hash": hashlib.sha256(b"original raw").hexdigest()},
        "doc": {"id": doc, "source": source, "text": text, "space_id": "qualification-household"},
        "immutable_revision": {"id": revision, "doc": doc, "text": text,
                               "text_hash": owner_module.sha(text.encode()), "space_id": "qualification-household"}}


class Controls(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.forbidden = [patch.object(subprocess, "Popen", side_effect=AssertionError("no real process")),
                          patch.object(socket, "create_connection", side_effect=AssertionError("no real connection"))]
        for guard in self.forbidden:
            guard.start()
    def tearDown(self):
        for guard in reversed(self.forbidden):
            guard.stop()
        self.temporary.cleanup()

    async def enter(self, fixture):
        # Only /proc/exe reads are replaced; filesystem path canonicalization
        # continues to use the real no-follow regular temporary files.
        original = os.readlink
        patcher = patch.object(owner_module.os, "readlink", side_effect=lambda path, *a, **kw:
            fixture.executable_link(path) if str(path).startswith("/proc/") else original(path, *a, **kw))
        patcher.start()
        self.addCleanup(patcher.stop)
        return await fixture.owner.__aenter__()

    async def test_native_actual_lifetime_two_exports_uses_original_sixteen_processes(self):
        fixture = Fixture(self.root, "surreal-db")
        owner = await self.enter(fixture)
        self.assertEqual(owner.state, "active")
        self.assertEqual(set(fixture.client.tokens), {"runtime:inspect", "runtime:operator"} |
            {name+":"+role for name in owner_module.SPACES for role in ("bootstrap", "data", "manage", "revocable")})
        first = await owner.capture_original_export("surreal-db", owner.nonce, publication())
        await owner.capture_original_export("surreal-db", owner.nonce, publication("two"))
        self.assertEqual(len(fixture.launches), 16)
        self.assertFalse(json.loads(Path(first["path"]).read_bytes())["qualified"])
        paired = owner.paired["child"]
        await owner.stop_original("surreal-db", owner.nonce)
        self.assertTrue(owner.recall["child"].reaped)
        self.assertEqual(paired.signals, [])
        self.assertIsNone(paired.poll())
        await owner.__aexit__(None, None, None)
        self.assertTrue(paired.reaped)
        self.assertIsNotNone(owner.cleanup_receipt)
        self.assertEqual(owner.state, "stopped")
        self.assertFalse(owner.failures)
        self.assertEqual(fixture.capture.held, 0)

    async def test_embedded_explicit_initialize_and_workers_after_both_spaces(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        self.assertEqual(owner.schema_operation["kind"], "initialize_runtime")
        self.assertEqual(len(owner.configurations), 2)
        self.assertEqual(owner.active_deadline_ns, owner.deadline_ns-75*10**9)
        self.assertEqual(fixture.launches[-1].environment["PIPELINE_EMBED_PROVIDER"], "unconfigured")
        self.assertFalse(any(item.arguments == ["migrate"] for item in fixture.launches))
        await owner.__aexit__(None, None, None)

    async def test_wrong_backend_nonce_and_stopped_owner_cannot_signal_or_export(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        calls = fixture.client.count
        for backend, nonce in (("surreal-db", owner.nonce), (owner.backend, "other")):
            with self.assertRaises(ValueError):
                await owner.stop_original(backend, nonce)
            with self.assertRaises(ValueError):
                await owner.capture_original_export(backend, nonce, publication())
        self.assertEqual(fixture.client.count, calls)
        self.assertEqual(owner.recall["child"].signals, [])
        await owner.stop_original(owner.backend, owner.nonce)
        with self.assertRaises(ValueError):
            await owner.stop_original(owner.backend, owner.nonce)
        with self.assertRaises(ValueError):
            await owner.revalidate_original()
        await owner.__aexit__(None, None, None)

    async def test_expired_active_clock_preserves_cleanup_budget(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        owner.active_deadline_ns = time.monotonic_ns()-1
        with self.assertRaisesRegex(ValueError, "active allocation"):
            await owner.stop_original(owner.backend, owner.nonce)
        self.assertEqual(owner.recall["child"].signals, [])
        await owner.__aexit__(None, None, None)
        self.assertTrue(owner.recall["child"].reaped)

    async def test_capture_exhaustion_refuses_before_export_http_or_new_process(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        count, processes = fixture.client.count, len(fixture.launches)
        maximum = fixture.capture.maximum
        fixture.capture.maximum = fixture.capture.written+owner_module.MIB
        with self.assertRaisesRegex(ValueError, "budget"):
            await owner.capture_original_export(owner.backend, owner.nonce, publication())
        self.assertEqual((fixture.client.count, len(fixture.launches)), (count, processes))
        fixture.capture.maximum = maximum
        await owner.__aexit__(None, None, None)

    async def test_payload_digest_failure_cannot_launch_inspector_or_gain_export_retry(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        fixture.mutate_payload = True
        count = len(fixture.launches)
        with self.assertRaisesRegex(ValueError, "payload/manifest"):
            await owner.capture_original_export(owner.backend, owner.nonce, publication())
        self.assertEqual(len(fixture.launches), count)
        with self.assertRaisesRegex(ValueError, "not active"):
            await owner.capture_original_export(owner.backend, owner.nonce, publication())
        await owner.__aexit__(ValueError, ValueError("export refused"), None)
        self.assertTrue(owner.failures)

    async def test_incomplete_report_is_not_a_successful_capture(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        fixture.mutate_report = True
        with self.assertRaisesRegex(ValueError, "complete output"):
            await owner.capture_original_export(owner.backend, owner.nonce, publication())
        self.assertFalse(list((owner.root / "captures").glob("*capacity-export*")))
        await owner.__aexit__(ValueError, ValueError("report refused"), None)
        self.assertTrue(owner.failures)

    async def test_bundle_mutation_after_inspection_is_not_a_complete_capture(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        fixture.mutate_bundle_after_inspection = True
        with self.assertRaisesRegex(ValueError, "bundle changed"):
            await owner.capture_original_export(owner.backend, owner.nonce, publication())
        self.assertFalse(list((owner.root / "captures").glob("*capacity-export*")))
        await owner.__aexit__(ValueError, ValueError("bundle changed"), None)
        self.assertEqual(owner.state, "failed")

    async def test_changed_actual_process_stat_refuses_before_signal(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        original = fixture.pc.process_identity
        fixture.pc.process_identity = lambda raw: {**original(raw), "start_ticks": 99999}
        with self.assertRaisesRegex(ValueError, "identity changed"):
            await owner.stop_original(owner.backend, owner.nonce)
        self.assertEqual(owner.recall["child"].signals, [])
        fixture.pc.process_identity = original
        await owner.__aexit__(None, None, None)

    async def test_foreign_handle_cannot_reuse_original_proof(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        child = owner.recall["child"]
        child.capture_proof = object()
        with self.assertRaisesRegex(ValueError, "no longer live"):
            await owner.stop_original(owner.backend, owner.nonce)
        self.assertEqual(child.signals, [])
        child.capture_proof = owner.recall["proof"]
        await owner.__aexit__(None, None, None)

    async def test_failed_startup_retains_original_handle_and_cleanup_receipt(self):
        fixture = Fixture(self.root)
        fixture.owner.spaces = AsyncMock(side_effect=ValueError("original grant refused"))
        with self.assertRaisesRegex(ValueError, "grant refused"):
            await self.enter(fixture)
        self.assertIsNone(fixture.owner.startup_receipt)
        self.assertIsNotNone(fixture.owner.cleanup_receipt)
        self.assertEqual(fixture.owner.state, "failed")
        self.assertTrue(all(process.reaped for process in fixture.launches))

    async def test_cancelled_wait_is_joined_before_cleanup_can_reap_again(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        child = owner.recall["child"]
        original_wait = child.wait
        started, release = threading.Event(), threading.Event()
        calls = []
        def held_wait(timeout):
            calls.append(timeout)
            started.set()
            if not release.wait(timeout):
                raise TimeoutError("test wait not released")
            return original_wait(timeout)
        child.wait = held_wait
        stop = asyncio.create_task(owner.stop_original(owner.backend, owner.nonce))
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        stop.cancel()
        await asyncio.sleep(0)
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await stop
        await owner.__aexit__(asyncio.CancelledError, asyncio.CancelledError(), None)
        self.assertEqual(len(calls), 1)
        self.assertTrue(child.reaped)
        self.assertIsNone(owner.recall.get("wait_task"))

    async def test_launch_proof_and_initialization_keep_the_original_cleanup_reserve(self):
        fixture = Fixture(self.root)
        original = fixture.helpers.OwnedProcess
        observations = []
        def create(*args, **kwargs):
            observations.append((kwargs["capture_proof"].intent["deadline_ns"], kwargs["initialization_deadline"]))
            return original(*args, **kwargs)
        fixture.helpers.OwnedProcess = create
        owner = await self.enter(fixture)
        for deadline, initialization in observations:
            self.assertEqual(deadline, owner.active_deadline_ns)
            self.assertLessEqual(initialization, fixture.pc.deadline_seconds(owner.active_deadline_ns))
            self.assertLess(initialization, fixture.pc.deadline_seconds(owner.deadline_ns)-70)
        await owner.__aexit__(None, None, None)

    async def test_cancelled_identity_capture_finishes_before_cleanup_even_when_cancelled_twice(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        original = fixture.pc.binary_identity
        started, release = threading.Event(), threading.Event()
        deadlines, saved = [], []
        original_save = owner.journal.save
        def held_identity(path, deadline, **kwargs):
            deadlines.append(deadline)
            started.set()
            if not release.wait(2):
                raise TimeoutError("test original identity capture not released")
            return original(path, deadline, **kwargs)
        def track_save(name, raw, kind):
            result = original_save(name, raw, kind)
            if "/current-" in name:
                saved.append(result)
            return result
        fixture.pc.binary_identity = held_identity
        owner.journal.save = track_save
        observation = asyncio.create_task(owner.revalidate_original())
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        for _ in range(2):
            observation.cancel()
            await asyncio.sleep(0)
            self.assertFalse(observation.done())
        self.assertEqual(owner.recall["child"].signals, [])
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await observation
        self.assertEqual(deadlines, [owner.active_deadline_ns])
        self.assertEqual(len(saved), 2)
        self.assertTrue(all(Path(row["path"]).is_file() for row in saved))
        await owner.__aexit__(asyncio.CancelledError, asyncio.CancelledError(), None)
        self.assertTrue(owner.recall["child"].reaped)

    async def test_cancelled_capture_save_finishes_before_owner_unwinds(self):
        fixture = Fixture(self.root)
        owner = await self.enter(fixture)
        original = owner.journal.save
        started, release = threading.Event(), threading.Event()
        completed = []
        def held_save(name, raw, kind):
            if name.endswith("-held-original.json"):
                started.set()
                if not release.wait(2):
                    raise TimeoutError("test original capture save not released")
            result = original(name, raw, kind)
            if name.endswith("-held-original.json"):
                completed.append(result)
            return result
        owner.journal.save = held_save
        saving = asyncio.create_task(owner.save("held-original", {"original": True}))
        self.assertTrue(await asyncio.to_thread(started.wait, 2))
        saving.cancel()
        await asyncio.sleep(0)
        self.assertFalse(saving.done())
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await saving
        self.assertEqual(len(completed), 1)
        self.assertEqual(json.loads(Path(completed[0]["path"]).read_bytes()), {"original": True})
        await owner.__aexit__(asyncio.CancelledError, asyncio.CancelledError(), None)
        self.assertTrue(owner.recall["child"].reaped)

    async def test_actual_original_isolation_carrier_cannot_adopt_another_namespace(self):
        fixture = Fixture(self.root)
        fixture.helpers.offline_namespace = lambda parent: {"parent_network_namespace": parent,
                                                            "network_namespace": "net:[999]"}
        with self.assertRaisesRegex(ValueError, "namespace changed"):
            await fixture.owner.__aenter__()
        self.assertEqual(fixture.launches, [])
        self.assertFalse(fixture.owner.root.exists())
        self.assertIsNone(fixture.owner.startup_receipt)

    def test_original_graph_identity_and_full_digest_are_required(self):
        value = publication()
        self.assertEqual(Owner.publication(value)["rendered_bytes"], len(value["doc"]["text"].encode()))
        for area, field, replacement in (("operation", "state", "running"), ("head", "generation", "new"),
                ("doc", "text", "truncated"), ("immutable_revision", "text_hash", "0"*64)):
            changed = copy.deepcopy(value)
            changed[area][field] = replacement
            with self.assertRaises(ValueError):
                Owner.publication(changed)

    def test_constructor_rejects_future_tokens_foreign_backend_and_changed_worker_recipe(self):
        fixture = Fixture(self.root)
        fixture.client.tokens["runtime:operator"] = "future.supplied.token"
        with self.assertRaises(ValueError):
            Owner(**fixture.arguments)
        fixture.client.tokens.clear()
        fixture.allocation["backend"] = "surreal-db"
        with self.assertRaises(ValueError):
            Owner(**fixture.arguments)
        fixture.allocation["backend"] = "elephant-db"
        fixture.plan["worker_runtime_expectations"]["work_queue"]["max_concurrent"] = 2
        with self.assertRaises(ValueError):
            Owner(**fixture.arguments)

    async def test_distinct_space_credentials_reach_owned_child_environment_without_shared_fallback(self):
        for backend in ("elephant-db", "surreal-db"):
            root = self.root / backend
            root.mkdir()
            fixture = Fixture(root, backend)
            credentials = {space: {"credential_ref": "FULL_WORKLOAD_SPY_KEY_" + space.upper().replace("-", "_"),
                                   "value": "synthetic-controlled-provider-key-" + space}
                           for space in ("qualification-household", "qualification-business")}
            fixture.plan["spy_credentials"] = copy.deepcopy(credentials)
            for row in fixture.plan["spaces"]:
                row["processing"]["connections"]["owned-spy"]["credential_ref"] = credentials[row["name"]]["credential_ref"]
            fixture.owner = Owner(**fixture.arguments)
            fixture.owner.ensure_ports_absent = lambda: None
            owner = await self.enter(fixture)
            try:
                self.assertNotIn("FULL_WORKLOAD_SPY_KEY", owner.env)
                for row in credentials.values():
                    self.assertEqual(owner.env[row["credential_ref"]], row["value"])
                servers = [child for child in fixture.launches if child.arguments[0] == "serve"]
                self.assertEqual(len(servers), 1)
                for row in credentials.values():
                    self.assertEqual(servers[0].environment[row["credential_ref"]], row["value"])
                self.assertNotIn("FULL_WORKLOAD_SPY_KEY", servers[0].environment)
            finally:
                await owner.cleanup()
            self.assertEqual(owner.state, "stopped")

    def test_credential_map_and_connection_substitutions_refuse_before_owned_startup(self):
        fixture = Fixture(self.root)
        credentials = {space: {"credential_ref": "FULL_WORKLOAD_SPY_KEY_" + space.upper().replace("-", "_"),
                               "value": "synthetic-controlled-provider-key-" + space}
                       for space in ("qualification-household", "qualification-business")}
        fixture.plan["spy_credentials"] = credentials
        for row in fixture.plan["spaces"]:
            row["processing"]["connections"]["owned-spy"]["credential_ref"] = credentials[row["name"]]["credential_ref"]
        valid = copy.deepcopy(fixture.plan)
        household, business = "qualification-household", "qualification-business"
        changes = [lambda plan: plan["spy_credentials"].pop(household),
                   lambda plan: plan["spy_credentials"].update(foreign=credentials[household]),
                   lambda plan: plan["spy_credentials"][household].update(value=credentials[business]["value"]),
                   lambda plan: plan["spy_credentials"][household].update(credential_ref=credentials[business]["credential_ref"]),
                   lambda plan: plan["spy_credentials"][household].update(value="arbitrary-provider-secret"),
                   lambda plan: plan["spy_credentials"][household].update(extra="unused"),
                   lambda plan: plan["spaces"][0]["processing"]["connections"]["owned-spy"].update(credential_ref="FULL_WORKLOAD_SPY_KEY")]
        for change in changes:
            plan = copy.deepcopy(valid)
            change(plan)
            with self.subTest(change=change), self.assertRaises(ValueError):
                Owner(**{**fixture.arguments, "runtime_plan": plan})
        self.assertEqual(fixture.launches, [])
        self.assertFalse(fixture.owner.root.exists())


if __name__ == "__main__":
    unittest.main()
