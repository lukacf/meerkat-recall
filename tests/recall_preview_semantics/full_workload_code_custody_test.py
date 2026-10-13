"""Actual CLI bootstrap with synthetic tempfile modules only; no product calls."""
import asyncio
from contextlib import contextmanager, redirect_stdout
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import time
from types import ModuleType
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "scripts/qualification/full_workload.py"
SPEC = importlib.util.spec_from_file_location("full_workload_code_controls", SOURCE)
cli = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cli)

TRANSPORT = '''import hashlib, json
from pathlib import Path
class Capture:
    def __init__(self, directory, maximum):
        self.directory, self.maximum = Path(directory), maximum
        self.directory.mkdir()
        self.held, self.written, self.events = 0, 0, []
    def reserve(self, amount):
        assert self.written + self.held + amount <= self.maximum
        self.held += amount
def require(condition, reason):
    if not condition: raise ValueError(reason)
def encode(value): return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
def decode(raw): return json.loads(raw)
def sha(raw): return hashlib.sha256(raw).hexdigest()
def endpoint(value): return value
def annotation(value: int): return "original-transport"
'''
SPY = '''class ControlledSpy:
    def __init__(self, *args):
        self.tasks, self.server, self.capture = set(), None, args[2]
    async def start(self, port): self.capture.events.append("spy-start")
    async def close(self): self.capture.events.append("spy-close")
'''
DRIVER = '''import asyncio
from full_workload_transport import annotation
CONTRACT_SHA = "contract"
CORPUS_SHA = "corpus"
CASES, SPACES = (), ()
def validate_inputs(*args): pass
class NamedClient:
    def __init__(self, address, tokens, capture, limits, deadline, expires):
        self.tokens, self.capture, self.deadline, self.expires = tokens, capture, deadline, expires
class Driver:
    observed_oracle = None
    original_owner = original_callback = None
    stall = False
    def __init__(self, *args, ob3_oracle=None, **kwargs):
        self.client = args[0]
        assert self.client.capture.events[-1] == "inspect-initial"
        self.client.capture.events.append("driver-created")
        type(self).observed_oracle = ob3_oracle
        type(self).original_owner = kwargs["service_control"]
        assert kwargs["capacity_capture"] is type(self).original_owner
        type(self).original_callback = kwargs["runtime_inspection"]
        self.callback = kwargs["runtime_inspection"]
    def event(self, *args): pass
    async def run(self):
        self.client.capture.events.append("driver-run")
        if self.stall: await asyncio.Future()
        for boundary in ("before_fw09_first_space", "before_fw09_second_space", "after_fw09_cleanup"):
            assert await self.callback(boundary) is None
        return {"fixture_only": True, "events": self.client.capture.events}
'''
ORACLE = '''def evaluate_receipt_bytes(receipt, expectations):
    return ("original-oracle", receipt, expectations)
'''
INSTALLED = '''import hashlib
LOADED_SOURCE_HASHES = {}
expected = ("fixtures", "matrix", "oracle", "installed_d_capture", "installed_release_artifact", "installed_original_process_capture")
assert set(VERIFIED_DEPENDENCIES) == set(expected)
for name, entry in VERIFIED_DEPENDENCIES.items():
    assert set(entry) == {"module", "path", "source_bytes", "sha256"}
    module = entry["module"]
    assert module.__name__ == module.__spec__.name == name
    assert module.__file__ == module.__spec__.origin == entry["path"]
    assert module.ORIGINAL == name
    assert hashlib.sha256(entry["source_bytes"]).hexdigest() == entry["sha256"]
    LOADED_SOURCE_HASHES[module] = entry["sha256"]
artifact = VERIFIED_DEPENDENCIES["installed_release_artifact"]["module"]
process_capture = VERIFIED_DEPENDENCIES["installed_original_process_capture"]["module"]
'''
OWNER = '''import asyncio
class FullWorkloadOwnedRuntime:
    cleanup_fails = False
    cleanup_delay = 0
    last = None
    def __init__(self, **values):
        type(self).last = self
        self.client, self.capture = values["client"], values["capture"]
        assert self.client.capture is self.capture and self.client.tokens == {}
        assert values["installed_helpers"].process_capture is values["process_capture"]
        assert values["installed_helpers"].artifact is values["artifact_helpers"]
        self.active_deadline_ns = min(values["deadline_ns"], values["whole_deadline_ns"]) - 75_000_000_000
        self.state, self.failures = "new", []
        self.startup_receipt = self.cleanup_receipt = None
    async def __aenter__(self):
        self.capture.events.append("owner-enter")
        self.state = "active"
        self.client.tokens["runtime:inspect"] = "fixture-only-not-a-real-token"
        self.startup_receipt = {"fixture_only": "original-startup"}
        return self
    async def __aexit__(self, *args):
        self.capture.events.append("owner-exit")
        if self.cleanup_delay: await asyncio.sleep(self.cleanup_delay)
        self.cleanup_receipt = {"fixture_only": "original-cleanup"}
        self.state = "failed" if self.cleanup_fails else "stopped"
        self.failures = ["fixture-cleanup-error"] if self.cleanup_fails else []
'''
INSPECTOR = '''from runtime_package_bootstrap_contract import ORIGINAL
class RuntimeRecipeInspector:
    reject = False
    substitute = False
    def __init__(self, client, plan, owner):
        self.client, self.owner = client, owner
        assert owner.state == "active" and "runtime:inspect" in client.tokens
    async def capture(self, boundary):
        if self.reject: raise ValueError("fixture-inspection-refused")
        self.client.capture.events.append("inspect-" + boundary)
        if self.substitute: return True
'''


class OriginalCodeControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = [self.root / path.relative_to(ROOT) for path in cli.FILES]
        contents = {"full_workload.py": SOURCE.read_bytes(), "full_workload_transport.py": TRANSPORT.encode(),
                    "full_workload_spy.py": SPY.encode(), "full_workload_driver.py": DRIVER.encode(),
                    "full_workload_graph.py": b'ORIGINAL = "graph"\n', "full_workload_load.py": b'ORIGINAL = "load"\n',
                    "ob3_outage_oracle.py": ORACLE.encode(),
                    "installed_linux_offline.py": INSTALLED.encode(),
                    "runtime_package_bootstrap_contract.py": b'ORIGINAL = "contract"\n',
                    "full_workload_runtime_inspection.py": INSPECTOR.encode(),
                    "full_workload_owned_runtime.py": OWNER.encode(),
                    "snapshot_inspection_capture.py": b'ORIGINAL = "inspection"\n'}
        for name, relative in cli.MODULES:
            if name in cli.INSTALLED_DEPENDENCIES:
                contents[Path(relative).name] = ('ORIGINAL = ' + repr(name) + '\n').encode()
        contents["recall_preview.py"] += b'import sys\nsys.path.insert(0, "fixture-candidate-import-path")\n'
        contents["installed_process_capture.py"] += b'def deadline_seconds(value): return value / 10**9\n'
        for path in self.files:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(contents[path.name])
        self.addCleanup(patch.stopall)
        patch.object(cli, "ROOT", self.root).start()
        patch.object(cli, "FILES", self.files).start()

    def snapshot(self):
        expected = {str(path.relative_to(self.root)): cli.sha(path.read_bytes()) for path in self.files}
        return cli.snapshot_sources(expected)

    def test_executes_original_snapshot_after_files_change_without_pyc_or_reopen(self):
        originals, hashes = self.snapshot()
        original_path = list(sys.path)
        for path in self.files:
            path.write_bytes(b'raise AssertionError("later source must not execute")\n')
        with cli.reviewed_code(originals, hashes) as loaded:
            self.assertEqual(loaded.Driver.__module__, "full_workload_driver")
            transport = sys.modules["full_workload_transport"]
            self.assertEqual(transport.annotation(1), "original-transport")
            self.assertIs(transport.annotation.__annotations__["value"], int)
            self.assertEqual(loaded._OB3_ORACLE(b"original receipt", b"independent expectation"),
                             ("original-oracle", b"original receipt", b"independent expectation"))
            self.assertEqual(loaded.source_hashes(), hashes)
            self.assertEqual(loaded.__file__, str(self.files[0]))
            self.assertEqual(loaded.__spec__.origin, str(self.files[0]))
            self.assertEqual(len(loaded.source_hashes()), 18)
            self.assertIs(loaded._INSTALLED_HELPERS.artifact, loaded._ARTIFACT_HELPERS)
            self.assertIs(loaded._INSTALLED_HELPERS.process_capture, loaded._PROCESS_CAPTURE)
            self.assertEqual(set(loaded._INSTALLED_HELPERS.VERIFIED_DEPENDENCIES), set(cli.INSTALLED_DEPENDENCIES))
            self.assertEqual(loaded._RUNTIME_INSPECTOR.__module__, "full_workload_runtime_inspection")
            self.assertEqual(loaded._RUNTIME_OWNER.__module__, "full_workload_owned_runtime")
        self.assertEqual(sys.path, original_path)
        self.assertEqual(list(self.root.rglob("*.pyc")), [])

    def test_cached_module_is_never_adopted_and_namespace_restored_on_refusal(self):
        original = ModuleType("full_workload_transport")
        original.annotation = lambda _: "unreviewed-cache"
        originals, hashes = self.snapshot()
        with patch.dict(sys.modules, {"full_workload_transport": original}):
            with self.assertRaisesRegex(ValueError, "controlled-exit"):
                with cli.reviewed_code(originals, hashes) as loaded:
                    self.assertIsNot(sys.modules["full_workload_transport"], original)
                    self.assertEqual(sys.modules["full_workload_transport"].annotation(1), "original-transport")
                    self.assertFalse(loaded.inventory()["product_acceptance"])
                    raise ValueError("controlled-exit")
            self.assertIs(sys.modules["full_workload_transport"], original)

    def test_all_pins_must_match_before_any_candidate_module_executes(self):
        for path in self.files[1:]:
            path.write_bytes(b'raise AssertionError("candidate ran before all source pins matched")\n')
        manifest = self.root / "manifest.json"
        allocation = self.root / "allocation.json"
        allocation.write_bytes(cli.encode({"execution_authorized": True, "scope": "controlled-owned-spy-only",
            "external_provider_sends": 0, "paid_model_calls": 0, "driver_source_sha256": {}}))
        manifest.write_bytes(cli.encode({"allocation": {"path": str(allocation), "sha256": cli.sha(allocation.read_bytes())}}))
        argv = [str(self.files[0]), "run", "--manifest", str(manifest), "--manifest-sha256", cli.sha(manifest.read_bytes()), "--execute-reviewed"]
        with patch.object(sys, "argv", argv), self.assertRaisesRegex(ValueError, "source pins differ"):
            cli.main()

    def test_substituted_snapshot_cannot_be_executed(self):
        originals, hashes = self.snapshot()
        originals["scripts/qualification/ob3_outage_oracle.py"] = b'raise AssertionError("substitution")\n'
        with self.assertRaisesRegex(ValueError, "snapshot differs"):
            with cli.reviewed_code(originals, hashes):
                self.fail("substituted code reached consumer")

    def test_missing_transitive_pin_refuses_before_any_module_executes(self):
        originals, hashes = self.snapshot()
        del hashes["scripts/qualification/installed_process_capture.py"]
        with self.assertRaisesRegex(ValueError, "snapshot differs"):
            with cli.reviewed_code(originals, hashes):
                self.fail("missing original process helper pin admitted")

    def test_nested_dependency_substitution_and_import_path_leak_are_refused(self):
        installed = self.root / "scripts/qualification/installed_linux_offline.py"
        installed.write_bytes(INSTALLED.encode() + b'process_capture = object()\n')
        originals, hashes = self.snapshot()
        original_path = list(sys.path)
        prior = ModuleType("installed_original_process_capture")
        with patch.dict(sys.modules, {"installed_original_process_capture": prior}):
            with self.assertRaisesRegex(ValueError, "installed dependency identity differs"):
                with cli.reviewed_code(originals, hashes):
                    self.fail("substituted nested module admitted")
            self.assertIs(sys.modules["installed_original_process_capture"], prior)
        self.assertEqual(sys.path, original_path)

    def test_actual_cli_passes_exact_verified_oracle_callable_to_driver(self):
        originals, hashes = self.snapshot()
        with cli.reviewed_code(originals, hashes) as loaded:
            receipt = asyncio.run(loaded.execute(self.execution_inputs()))
            self.assertIs(loaded.Driver.observed_oracle, loaded._OB3_ORACLE)
            self.assertFalse(receipt["qualified"])
            self.assertFalse(receipt["product_acceptance"])
            self.assertTrue(receipt["owned_spy_closed"])
            self.assertTrue(receipt["owned_runtime_closed"])
            self.assertIs(loaded.Driver.original_owner, loaded._RUNTIME_OWNER.last)
            self.assertIs(loaded.Driver.original_callback.__self__.owner, loaded._RUNTIME_OWNER.last)
            self.assertEqual(receipt["runtime_cleanup"], {"fixture_only": "original-cleanup"})
            self.assertEqual(loaded._RUNTIME_OWNER.last.capture.events,
                ["spy-start", "owner-enter", "inspect-initial", "driver-created", "driver-run",
                 "inspect-before_fw09_first_space", "inspect-before_fw09_second_space",
                 "inspect-after_fw09_cleanup", "owner-exit", "spy-close"])

    def execution_inputs(self, active_seconds=5):
        deadline_ns = time.monotonic_ns() + int((75 + active_seconds) * 10**9)
        return {"manifest": {"output": str(self.root / "capture"), "endpoint": "unused-fixture",
                    "backend": "elephant-db", "run_nonce": "fixture", "spy_port": 1,
                    "allocation": {"sha256": "fixture-allocation"}},
                "allocation": {"source_commit": "fixture", "artifact_sha256": "fixture", "binary_sha256": "fixture"},
                "contract": {"controlled_fixture_limits": {"capture_bytes_per_backend": 1024**3}},
                "corpus": {}, "plan": {"spy_recipe": {}, "spy_recipe_sha256": "fixture"}, "plan_hash": "fixture",
                "runtime_plan": {}, "isolation": {}, "external": {}, "deadline": deadline_ns / 10**9,
                "expires": time.time() + 75 + active_seconds, "deadline_ns": deadline_ns,
                "whole_deadline_ns": deadline_ns, "allocation_clock": {"fixture_only": True}, "input_hashes": {}}

    def test_initial_inspection_refusal_cleans_owner_without_constructing_driver(self):
        originals, hashes = self.snapshot()
        with cli.reviewed_code(originals, hashes) as loaded:
            loaded._RUNTIME_INSPECTOR.reject = True
            receipt = asyncio.run(loaded.execute(self.execution_inputs()))
            self.assertEqual(receipt["error_type"], "ValueError")
            self.assertIsNone(receipt["observation"])
            self.assertIsNone(loaded.Driver.observed_oracle)
            self.assertTrue(receipt["owned_runtime_closed"])
            self.assertEqual(loaded._RUNTIME_OWNER.last.capture.events,
                             ["spy-start", "owner-enter", "owner-exit", "spy-close"])
            saved = json.loads((self.root / "capture/cli-terminal-receipt.json").read_bytes())
            self.assertEqual(saved, receipt)

    def test_boolean_inspection_cannot_substitute_for_capture(self):
        originals, hashes = self.snapshot()
        with cli.reviewed_code(originals, hashes) as loaded:
            loaded._RUNTIME_INSPECTOR.substitute = True
            receipt = asyncio.run(loaded.execute(self.execution_inputs()))
            self.assertEqual(receipt["error_type"], "ValueError")
            self.assertIsNone(loaded.Driver.observed_oracle)
            self.assertTrue(receipt["owned_runtime_closed"])

    def test_original_active_timeout_unwinds_owner_inside_reserved_cleanup_time(self):
        originals, hashes = self.snapshot()
        with cli.reviewed_code(originals, hashes) as loaded:
            loaded.Driver.stall = True
            inputs = self.execution_inputs(active_seconds=0.02)
            receipt = asyncio.run(loaded.execute(inputs))
            self.assertEqual(receipt["error_type"], "TimeoutError")
            self.assertTrue(receipt["owned_runtime_closed"])
            self.assertTrue(receipt["owned_spy_closed"])
            self.assertLess(time.monotonic_ns(), inputs["deadline_ns"])

    def test_cleanup_failure_never_sets_closed_runtime(self):
        originals, hashes = self.snapshot()
        with cli.reviewed_code(originals, hashes) as loaded:
            loaded._RUNTIME_OWNER.cleanup_fails = True
            receipt = asyncio.run(loaded.execute(self.execution_inputs()))
            self.assertEqual(receipt["error_type"], "ValueError")
            self.assertFalse(receipt["owned_runtime_closed"])
            self.assertFalse(receipt["qualified"])

    def test_normal_cleanup_can_cross_active_boundary_within_original_reserve(self):
        originals, hashes = self.snapshot()
        with cli.reviewed_code(originals, hashes) as loaded:
            loaded._RUNTIME_OWNER.cleanup_delay = 0.06
            inputs = self.execution_inputs(active_seconds=0.02)
            receipt = asyncio.run(loaded.execute(inputs))
            self.assertIsNone(receipt["error_type"])
            self.assertTrue(receipt["owned_runtime_closed"])
            self.assertTrue(receipt["owned_spy_closed"])
            self.assertGreater(time.monotonic_ns(), loaded._RUNTIME_OWNER.last.active_deadline_ns)
            self.assertLess(time.monotonic_ns(), inputs["deadline_ns"])

    def test_main_samples_original_integer_clock_before_any_input_read(self):
        _, hashes = self.snapshot()
        allocation = self.root / "allocation.json"
        allocation.write_bytes(cli.encode({"execution_authorized": True, "scope": "controlled-owned-spy-only",
            "external_provider_sends": 0, "paid_model_calls": 0, "driver_source_sha256": hashes}))
        manifest = self.root / "manifest.json"
        manifest.write_bytes(cli.encode({"allocation": {"path": str(allocation), "sha256": cli.sha(allocation.read_bytes())}}))
        reads, received = [], []
        original_read, original_scope = cli.Inputs._read, cli.reviewed_code
        def read(instance, *args, **kwargs):
            reads.append(time.monotonic_ns())
            return original_read(instance, *args, **kwargs)
        @contextmanager
        def scope(*args):
            with original_scope(*args) as loaded:
                def inputs(path, digest, raws, clock):
                    received.append(clock)
                    self.assertEqual(raws, (manifest.read_bytes(), allocation.read_bytes()))
                    return {"fixture_only": True}
                async def execute(value):
                    self.assertEqual(value, {"fixture_only": True})
                    return {"format": "fixture", "backend": "fixture", "error_type": None,
                            "qualified": False, "product_acceptance": False}
                loaded.reviewed_inputs, loaded.execute = inputs, execute
                yield loaded
        argv = [str(SOURCE), "run", "--manifest", str(manifest), "--manifest-sha256", cli.sha(manifest.read_bytes()), "--execute-reviewed"]
        with patch.object(sys, "argv", argv), patch.object(cli.Inputs, "_read", read), \
                patch.object(cli, "reviewed_code", scope), redirect_stdout(io.StringIO()):
            self.assertEqual(cli.main(), 1)
        self.assertEqual(len(received), 1)
        self.assertEqual(set(received[0]), {"mono_before_ns", "wall_ns", "mono_after_ns"})
        self.assertTrue(all(type(value) is int and value > 0 for value in received[0].values()))
        self.assertLessEqual(received[0]["mono_before_ns"], received[0]["mono_after_ns"])
        self.assertLessEqual(received[0]["mono_after_ns"], reads[0])

    def test_source_size_and_symlink_refuse_before_import(self):
        with patch.object(cli, "SOURCE_LIMIT", 8), self.assertRaisesRegex(ValueError, "source byte bound"):
            cli.snapshot_sources()
        target = self.root / "other.py"
        target.write_bytes(b"pass\n")
        self.files[-1].unlink()
        self.files[-1].symlink_to(target)
        with self.assertRaisesRegex(ValueError, "canonical original source"):
            cli.snapshot_sources()


class RuntimeInputControls(unittest.TestCase):
    def setUp(self):
        self.spaces = ("qualification-household", "qualification-business")
        self.manifest = {"backend": "elephant-db", "run_nonce": "fixture", "endpoint": "http://127.0.0.1:43130",
                         "spy_port": 43131, "output": "/work/fixture"}
        self.allocation = {**self.manifest, "source_commit": "1" * 40, "artifact_sha256": "2" * 64, "binary_sha256": "3" * 64}
        self.plan = {"configurations": {}, "worker_runtime_expectations": {"fixture_only": True},
                     "runtime_principal_templates": {},
                     "cases": {"FW10": {"spaces": []}},
                     "spy_recipe": {"rules": [{"credential_sha256": cli.sha(b"fixture-only-credential")}]}}
        self.runtime = {**self.allocation, "format": "recall-full-workload-runtime-plan/v1",
                        "run_directory": "/work/fixture/runtime", "spy_endpoint": "http://127.0.0.1:43131/v1",
                        "worker_runtime_expectations": self.plan["worker_runtime_expectations"],
                        "spy_credential": "fixture-only-credential", "spaces": [], "principals": {}}
        for index, space in enumerate(self.spaces):
            subject = f"fixture-data-{index}"
            configuration = {"space_id": space, "subjects": [], "subjects_state": "unknown",
                             "security": {"level": 3 + index * 4}, "embedding_dims": 3,
                             "processing": {"generation": 1, "index_generation": 0}}
            self.plan["configurations"][space] = {"configuration": configuration, "subject": subject,
                                                  "grant": {"scopes": ["read:source"]}}
            self.runtime["spaces"].append({"name": space, **{k: v for k, v in configuration.items()
                                                         if k not in ("space_id", "subjects", "subjects_state")}})
            self.runtime["principals"][space] = {"data": {"subject": subject, "scopes": ["read:source"]},
                "manage": {"subject": f"fixture-manage-{index}", "scopes": ["manage:grant"]},
                "revocable": {"subject": f"fixture-revocable-{index}", "scopes": ["read:source"]}}
            self.plan["runtime_principal_templates"][space] = copy.deepcopy({role: self.runtime["principals"][space][role]
                                                                           for role in ("manage", "revocable")})
            self.plan["cases"]["FW10"]["spaces"].append({"space": space, "role": "revocable", "subject": f"fixture-revocable-{index}"})

    def bind(self, runtime):
        with patch.object(cli, "SPACES", self.spaces, create=True):
            return cli.bind_runtime_inputs(runtime, self.plan, self.manifest, self.allocation)

    def test_original_preoutput_inputs_match_without_creating_runtime_facts(self):
        before = copy.deepcopy(self.runtime)
        self.assertIs(self.bind(self.runtime), self.runtime)
        self.assertEqual(self.runtime, before)
        self.assertNotIn("startup_receipt", self.runtime)
        self.assertNotIn("tokens", self.runtime)

    def test_wrong_space_recipe_principal_artifact_or_spy_input_refuses(self):
        changes = [lambda x: x.update(backend="surreal-db"),
                   lambda x: x.update(binary_sha256="4" * 64),
                   lambda x: x.update(spy_endpoint="http://127.0.0.1:43132/v1"),
                   lambda x: x.update(run_directory="/work/foreign/runtime"),
                   lambda x: x.update(worker_runtime_expectations={"fixture_only": False}),
                   lambda x: x["spaces"].reverse(),
                   lambda x: x["spaces"][0]["security"].update(level=True),
                   lambda x: x["spaces"][0]["processing"].update(generation=True),
                   lambda x: x["principals"][self.spaces[0]]["data"].update(subject="foreign"),
                   lambda x: x["principals"][self.spaces[0]]["manage"].update(subject="foreign"),
                   lambda x: x["principals"][self.spaces[0]]["manage"].update(scopes=["manage:provider"]),
                   lambda x: x["principals"][self.spaces[0]]["revocable"].update(scopes=["write:source"]),
                   lambda x: x["principals"][self.spaces[0]].pop("revocable"),
                   lambda x: x["principals"][self.spaces[0]]["revocable"].update(subject="foreign"),
                   lambda x: x.update(spy_credential="other-controlled-credential")]
        for change in changes:
            runtime = copy.deepcopy(self.runtime)
            change(runtime)
            with self.subTest(change=change), self.assertRaises(ValueError):
                self.bind(runtime)

    def test_absent_approved_principal_templates_refuse_preoutput(self):
        for templates in (None, {}, {self.spaces[0]: self.plan["runtime_principal_templates"][self.spaces[0]]}):
            with self.subTest(templates=templates), patch.dict(self.plan, {"runtime_principal_templates": templates}), \
                    self.assertRaisesRegex(ValueError, "approved principal template maps"):
                self.bind(self.runtime)

    def scoped_credentials(self):
        self.runtime["spy_credential"] = "synthetic-controlled-provider-key"
        credentials = {space: {"credential_ref": "FULL_WORKLOAD_SPY_KEY_" + space.upper().replace("-", "_"),
                               "value": "synthetic-controlled-provider-key-" + space} for space in self.spaces}
        self.runtime["spy_credentials"] = credentials
        self.plan["spy_recipe"]["rules"] = []
        for space, request in zip(self.spaces, self.runtime["spaces"]):
            connection = {"credential_ref": credentials[space]["credential_ref"], "endpoint": self.runtime["spy_endpoint"]}
            self.plan["configurations"][space]["configuration"]["processing"]["connections"] = {"owned": copy.deepcopy(connection)}
            request["processing"]["connections"] = {"owned": connection}
            self.plan["spy_recipe"]["rules"].append({"space": space, "model": "same-model",
                "path": "/v1/embeddings", "credential_sha256": cli.sha(credentials[space]["value"].encode())})
        path = ROOT / "scripts/qualification/full_workload_owned_runtime.py"
        spec = importlib.util.spec_from_file_location("controlled_credential_owner", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module.FullWorkloadOwnedRuntime

    def test_preoutput_scoped_credentials_bind_actual_runtime_validator_connections_and_rules(self):
        owner = self.scoped_credentials()
        before = cli.encode([self.runtime, self.plan])
        with patch.object(cli, "_RUNTIME_OWNER", owner):
            self.assertIs(self.bind(self.runtime), self.runtime)
        self.assertEqual(cli.encode([self.runtime, self.plan]), before)
        self.assertNotEqual(*[row["credential_sha256"] for row in self.plan["spy_recipe"]["rules"]])

    def test_preoutput_scoped_credentials_refuse_foreign_shared_or_unbound_authority(self):
        owner = self.scoped_credentials()
        original_runtime, original_plan = copy.deepcopy(self.runtime), copy.deepcopy(self.plan)
        changes = [lambda: self.runtime["spy_credentials"].pop(self.spaces[0]),
                   lambda: self.runtime["spy_credentials"][self.spaces[0]].update(value="unreviewed-key"),
                   lambda: self.plan["spy_recipe"]["rules"][0].update(credential_sha256="0" * 64),
                   lambda: self.plan["spy_recipe"]["rules"][0].update(space="foreign-space"),
                   lambda: self.plan["spy_recipe"]["rules"].append(copy.deepcopy(self.plan["spy_recipe"]["rules"][0])),
                   lambda: self.runtime["spaces"][0]["processing"]["connections"]["owned"].update(credential_ref="FULL_WORKLOAD_SPY_KEY")]
        with patch.object(cli, "_RUNTIME_OWNER", owner):
            for change in changes:
                self.runtime, self.plan = copy.deepcopy(original_runtime), copy.deepcopy(original_plan)
                change()
                with self.subTest(change=change), self.assertRaises(ValueError):
                    self.bind(self.runtime)
        self.runtime, self.plan = copy.deepcopy(original_runtime), copy.deepcopy(original_plan)
        self.runtime.pop("spy_credentials")
        for rule in self.plan["spy_recipe"]["rules"]:
            rule.update(path="/v1/responses", credential_sha256=cli.sha(self.runtime["spy_credential"].encode()))
        with self.assertRaisesRegex(ValueError, "ambiguous controlled provider space authority"):
            self.bind(self.runtime)


if __name__ == "__main__":
    unittest.main()
