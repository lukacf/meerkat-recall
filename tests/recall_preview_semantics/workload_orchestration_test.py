"""Offline custody controls only: mocked L predicates and C owned lifecycle.

These tests do not execute a process, listener, provider, or product oracle.
A mocked component is a control-flow token, never qualification evidence.
"""
import asyncio
from contextlib import asynccontextmanager
from collections.abc import Mapping
import copy
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

spec = importlib.util.spec_from_file_location("workload_orchestration_controls", Path(__file__).with_name("workload_orchestration.py"))
o = importlib.util.module_from_spec(spec)
spec.loader.exec_module(o)


class FileControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = o.OriginalFiles(roots=(self.root,))

    def test_exact_original_bytes_survive_import(self):
        raw = b'{ "original":  1 }\n'
        path = self.root / "original.json"
        o.private_new(path, raw)
        ref = o.reference(raw, "original")
        self.files.import_files([{"path": str(path), "ref": ref}])
        self.assertEqual(self.files.get(ref), raw)
        self.assertEqual(self.files.original_path(ref), str(path))
        self.assertNotEqual(raw, o.encode(self.files.json(ref)))
        self.assertEqual(self.files.inline, {})
        self.assertEqual(self.files.inline_bytes, 0)
        with patch.object(o, "read_original") as read:
            self.assertIn(ref["sha256"], self.files.raw)
            self.assertEqual(list(self.files.raw), [ref["sha256"]])
        read.assert_not_called()
        path.write_bytes(b"replacement")
        with self.assertRaises(ValueError):
            self.files.get(ref)

    def test_symlink_or_foreign_path_is_refused(self):
        path = self.root / "original"
        path.write_bytes(b"same")
        link = self.root / "alias"
        link.symlink_to(path)
        with self.assertRaisesRegex(ValueError, "original_path"):
            o.read_original(link, 4)
        with self.assertRaisesRegex(ValueError, "capture_path"):
            self.files.path("/unreviewed/input")

    def test_original_replacement_and_wrong_kind_are_refused(self):
        first, second = o.reference(b"first", "original"), o.reference(b"second", "original")
        self.files.retain(first, b"first", str(self.root / "same"))
        with self.assertRaisesRegex(ValueError, "path_replaced"):
            self.files.retain(second, b"second", str(self.root / "same"))
        with self.assertRaisesRegex(ValueError, "kind_alias"):
            self.files.retain(o.reference(b"first", "another"), b"first")
        with self.assertRaisesRegex(ValueError, "hash_size"):
            self.files.retain(first, b"later")

    def test_strict_parse_and_reference_shape(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e999}', b'\xff'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                o.strict_json(raw)
        ref = o.reference(b"x", "original")
        ref["bytes"] = True
        with self.assertRaisesRegex(ValueError, "reference_identity"):
            o.checked_reference(ref)

    def test_private_new_cannot_overwrite_original(self):
        path = self.root / "original"
        o.private_new(path, b"first")
        with self.assertRaises(FileExistsError):
            o.private_new(path, b"second")
        self.assertEqual(path.read_bytes(), b"first")
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_capture_rejects_uninventoried_blob_and_token_path(self):
        raw = b'{"capture":1}'
        ref = o.reference(raw, "capture")
        path = str(self.root / "capture")
        inventory = o.encode({"format": "recall-dynamic-capture-inventory/v2", "files": [{"path": path, "ref": ref}]})
        index = o.reference(inventory, "capture-inventory")
        bundle = {"capture_inventory": index, "raw_evidence": {ref["sha256"]: raw, index["sha256"]: inventory}}
        other = copy.deepcopy(bundle)
        other["raw_evidence"][o.digest(b"extra")] = b"extra"
        with self.assertRaisesRegex(ValueError, "uninventoried"):
            self.files.import_capture(other)
        files = o.OriginalFiles(roots=(self.root,))
        files.forbidden_paths.add(path)
        with self.assertRaisesRegex(ValueError, "capture_path"):
            files.import_capture(bundle)

    def test_runtime_aggregate_is_checked_before_read(self):
        self.files.total = o.MAX_TOTAL
        with patch.object(o, "read_original") as read, self.assertRaisesRegex(ValueError, "inventory_bound"):
            self.files.read(str(self.root / "unread"), o.reference(b"x", "original"))
        read.assert_not_called()

    def test_only_two_typed_bounded_gateway_mounts_can_enter_original_inventory(self):
        files = o.OriginalFiles()
        for path, kind in o.GATEWAY_ORIGINALS.items():
            with self.subTest(path=path):
                raw = ('{ "original_mount": "' + kind + '" }\n').encode()
                ref = o.reference(raw, kind)
                with patch.object(o, "read_original", return_value=raw) as read:
                    files.read(path, ref)
                    self.assertEqual(files.get(ref, kind), raw)
                self.assertEqual(read.call_count, 2)
                for call in read.call_args_list:
                    self.assertEqual(call.args, (path, len(raw)))
                    self.assertEqual(call.kwargs, {"private": True})
                for invalid in ({**ref, "kind": "input-inventory"}, {**ref, "bytes": 131073}):
                    with patch.object(o, "read_original") as read, self.assertRaisesRegex(ValueError, "gateway_original_kind_bound"):
                        files.read(path, invalid)
                    read.assert_not_called()
                with self.assertRaisesRegex(ValueError, "reference_fields"):
                    files.path(path)
                with patch.object(o, "read_original", return_value=b"changed"), self.assertRaisesRegex(ValueError, "original_hash_size"):
                    files.get(ref)
        with patch.object(o, "read_original") as read, self.assertRaisesRegex(ValueError, "capture_path"):
            files.read("/another-gateway-original.json", o.reference(b"{}", "gateway-manifest"))
        read.assert_not_called()


class LazyMappingControls(unittest.TestCase):
    def test_union_never_reads_provider_contents_during_construction(self):
        accesses = []
        raw = b"bounded one-record provider original"
        key = o.digest(raw)

        class OriginalResolver(Mapping):
            def __iter__(self):
                return iter([key])
            def __len__(self):
                return 1
            def __getitem__(self, asked):
                accesses.append(asked)
                if asked != key:
                    raise KeyError(asked)
                return raw

        combined = o.EvidenceUnion({o.digest(b"runtime"): b"runtime"}, OriginalResolver())
        self.assertEqual(len(combined), 2)
        self.assertEqual(accesses, [])
        self.assertEqual(combined[key], raw)
        self.assertEqual(accesses, [key])
        self.assertEqual(combined[key], raw)
        self.assertEqual(accesses, [key, key])  # No hidden eager/cache substitute.

    def test_lazy_changed_original_hash_is_rejected_at_each_lookup(self):
        raw = {o.digest(b"original"): b"original"}
        combined = o.EvidenceUnion(raw)
        key = next(iter(raw))
        self.assertEqual(combined[key], b"original")
        raw[key] = b"replacement"
        with self.assertRaisesRegex(ValueError, "mapping_bytes"):
            combined[key]
        with self.assertRaises(KeyError):
            combined["0" * 64]

    def test_duplicate_identity_is_checked_without_losing_either_original(self):
        key = o.digest(b"same")
        second = {key: b"same"}
        combined = o.EvidenceUnion({key: b"same"}, second)
        self.assertEqual(combined[key], b"same")
        second[key] = b"different"
        with self.assertRaisesRegex(ValueError, "mapping_bytes"):
            combined[key]


class PredicateImportControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.review = {"helper_sha256": {}}
        for name, relative in o.PREDICATES.items():
            names = ["validate_installed", "validate_process_proof"] if name == "installed_evidence_validator" else ["validate_provider_accounting"]
            raw = "\n".join(f'def {function}(*args):\n    return "reviewed test control"\n' for function in names).encode()
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            self.review["helper_sha256"][relative] = o.digest(raw)

    def test_cached_predicate_is_replaced_only_inside_reviewed_scope(self):
        cached = ModuleType("installed_evidence_validator")
        cached.validate_installed = lambda *_: "unreviewed cached control"
        with patch.dict(sys.modules, {"installed_evidence_validator": cached}):
            with o.reviewed_predicates(self.review, self.root):
                self.assertEqual(sys.modules["installed_evidence_validator"].validate_installed(), "reviewed test control")
                self.assertEqual(sys.modules["provider_evidence_validator"].validate_provider_accounting(), "reviewed test control")
            self.assertIs(sys.modules["installed_evidence_validator"], cached)

    def test_hash_refusal_restores_already_bound_original_modules(self):
        installed = ModuleType("installed_evidence_validator")
        provider = ModuleType("provider_evidence_validator")
        self.review["helper_sha256"][o.PREDICATES["provider_evidence_validator"]] = "0" * 64
        with patch.dict(sys.modules, {"installed_evidence_validator": installed, "provider_evidence_validator": provider}):
            with self.assertRaisesRegex(ValueError, "helper_changed_before_import"):
                with o.reviewed_predicates(self.review, self.root):
                    self.fail("changed provider predicate activated")
            self.assertIs(sys.modules["installed_evidence_validator"], installed)
            self.assertIs(sys.modules["provider_evidence_validator"], provider)

    def test_refused_component_restores_both_original_imports(self):
        installed = ModuleType("installed_evidence_validator")
        provider = ModuleType("provider_evidence_validator")
        with patch.dict(sys.modules, {"installed_evidence_validator": installed, "provider_evidence_validator": provider}):
            with self.assertRaisesRegex(ValueError, "component_refused"):
                with o.reviewed_predicates(self.review, self.root):
                    raise ValueError("component_refused")
            self.assertIs(sys.modules["installed_evidence_validator"], installed)
            self.assertIs(sys.modules["provider_evidence_validator"], provider)

    def test_executed_helper_is_the_exact_verified_source_snapshot(self):
        relative = o.PREDICATES["installed_evidence_validator"]
        path = self.root / relative
        original = path.read_bytes()
        read = o.read_original
        def replace_after_read(filename, maximum, **kwargs):
            raw = read(filename, maximum, **kwargs)
            if Path(filename) == path:
                path.write_text('raise RuntimeError("unreviewed second path read")\n')
            return raw
        with patch.object(o, "read_original", side_effect=replace_after_read):
            with o.reviewed_predicates(self.review, self.root):
                self.assertEqual(sys.modules["installed_evidence_validator"].validate_installed(), "reviewed test control")
        self.assertEqual(o.digest(original), self.review["helper_sha256"][relative])
class RuntimeControls(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = o.OriginalFiles(roots=(self.root,))
        self.events = []
        self.plan = {"backend": "elephant-db", "run_nonce": "original-control-nonce"}
        self.identity = {"backend": "elephant-db", "run_nonce": "original-control-nonce"}
        self.plan_ref = self.save("plan", self.plan, "bootstrap-plan")
        self.installed = self.save("installed", {"control": "original installed evidence"}, "installed-offline-evidence")
        review = {"expires_at": "2099-01-01T00:00:00Z", "identities": {"elephant-db": self.identity},
                  "bootstrap_plans": {"elephant-db": self.plan_ref}, "controller_policy": {"expected_anchor": {"original": 1}}}
        raw = o.encode(review)
        self.reviewed = {"review": review, "raw_bytes": raw, "raw_sha256": o.digest(raw)}
        self.fail_runtime = self.fail_live = False
        self.cleanup_status = "complete"
        self.capture_count = 0
        self.live = SimpleNamespace(token_files={"fixture": str(self.root / "credential.jwt")},
            cleanup_reference=None, revalidate_custody=self.revalidate,
            capture_original_custody=self.capture)
        self.validator = SimpleNamespace(validate_static=Mock(return_value=self.plan), validate_runtime=self.validate)
        self.startup = SimpleNamespace(reviewed_d28_startup=self.start)

    def save(self, name, value, kind):
        return self.files.save(self.root / (name + ".json"), value, kind)

    def revalidate(self):
        self.events.append("live")
        if self.fail_live:
            raise ValueError("original_live_identity_changed")

    def capture(self):
        self.capture_count += 1
        bundle, raw, rows = {}, {}, []
        context = {"anchor": {"configuration_sha256": "a" * 64}}
        def original(name, data, kind):
            ref = o.reference(data, kind)
            raw[ref["sha256"]] = data
            path = self.root / (name + ".original")
            if path.exists():
                self.assertEqual(path.read_bytes(), data)
            else:
                o.private_new(path, data)
            rows.append({"path": str(path), "ref": ref})
            return ref
        # Deliberately noncanonical raw bytes exercise custody only. These
        # mocked launch inputs are never submitted to L's actual predicates.
        manifest = original("launch-manifest", b'{ "fixture": "manifest" }\n', "gateway-manifest")
        spec = original("launch-spec", b'{ "fixture": "launch" }\n', "gateway-launch-spec")
        inputs = original("input-inventory", b'{ "fixture": "inputs" }\n', "input-inventory")
        for name in o.RUNTIME_REFERENCES:
            if name == "capture_inventory":
                continue
            value = context if name == "controller" else {"original": name}
            kind = "gateway-context" if name == "controller" else name.replace("_", "-")
            if name == "current_processes":
                value["input_inventory"] = inputs
            if name == "outer_launch":
                kind = "gateway-launch-inputs"
                value = {"format": "recall-gateway-launch-inputs/v2", "manifest": manifest,
                         "spec": spec, "input_inventory": inputs}
            bundle[name] = original(name, o.encode(value), kind)
        inventory_raw = o.encode({"format": "recall-dynamic-capture-inventory/v2", "files": rows})
        inventory_ref = o.reference(inventory_raw, "capture-inventory")
        raw[inventory_ref["sha256"]] = inventory_raw
        bundle["capture_inventory"] = inventory_ref
        bundle["raw_evidence"] = raw
        bundle["controller_context"] = {"context": context, "raw_bytes": o.encode(context)}
        return bundle

    def validate(self, reviewed, envelope, raw, context):
        self.events.append("predicate")
        if self.fail_runtime:
            raise ValueError("independent_predicate_refused")
        self.assertEqual(envelope["installed_offline"], self.installed)
        self.assertEqual(raw[envelope["controller"]["sha256"]], context["raw_bytes"])
        return {"format": "recall-installed-runtime-component/v2", "verdict": "passed", "product_acceptance": False,
                "identity": envelope["identity"], "static_review_sha256": reviewed["raw_sha256"],
                "contract_sha256": "b" * 64, "actual_runtime_configuration_sha256": "c" * 64,
                "actual_space_configurations_sha256": "d" * 64}

    @asynccontextmanager
    async def start(self, original, expected):
        self.events.append("start")
        self.assertEqual(original, {"path": self.files.original_path(self.plan_ref), "sha256": self.plan_ref["sha256"]})
        try:
            yield self.live
        finally:
            self.events.append("cleanup")
            terminal = {"format": "recall-runtime-bootstrap-cleanup/v1", **self.plan,
                "status": self.cleanup_status, "cleanup_complete": self.cleanup_status == "complete",
                "cleanup_failures": [], "error_type": None}
            raw = o.encode(terminal)
            path = self.root / "cleanup.json"
            o.private_new(path, raw)
            self.live.cleanup_reference = {"path": str(path), "sha256": o.digest(raw)}

    def runtime(self):
        return o.current_runtime(self.files, self.reviewed, self.validator, self.startup,
                                 "elephant-db", self.installed, self.root / "derived")

    async def test_phase_runs_only_after_original_predicate_and_cleanup_is_retained(self):
        async with self.runtime() as custody:
            self.events.append("phase")
            self.assertEqual(custody.component["product_acceptance"], False)
        self.assertLess(self.events.index("predicate"), self.events.index("phase"))
        self.assertEqual(self.events[-1], "cleanup")
        self.assertIn(str(self.root / "cleanup.json"), self.files.files)
        saved = o.strict_json((self.root / "derived/runtime-001.component.json").read_bytes())
        self.assertNotIn("qualified", saved)

    async def test_original_outer_launch_members_and_same_input_inventory_reach_predicate(self):
        async with self.runtime() as custody:
            envelope = custody.envelope
            wrapper = self.files.json(envelope["outer_launch"], "gateway-launch-inputs")
            current = self.files.json(envelope["current_processes"])
            self.assertEqual(set(wrapper), {"format", "manifest", "spec", "input_inventory"})
            self.assertEqual(wrapper["format"], "recall-gateway-launch-inputs/v2")
            self.assertEqual(wrapper["input_inventory"], current["input_inventory"])
            for field, kind, raw in (("manifest", "gateway-manifest", b'{ "fixture": "manifest" }\n'),
                                     ("spec", "gateway-launch-spec", b'{ "fixture": "launch" }\n')):
                self.assertEqual(self.files.get(wrapper[field], kind), raw)
                self.assertNotEqual(raw, o.encode(o.strict_json(raw)))
            stored = self.files.json(custody.evidence_reference, "runtime-evidence")
            self.assertEqual(stored["outer_launch"], envelope["outer_launch"])

    async def test_missing_original_outer_launch_never_reaches_predicate_or_phase(self):
        original_capture = self.capture
        def missing():
            bundle = original_capture()
            del bundle["outer_launch"]
            return bundle
        self.live.capture_original_custody = missing
        with self.assertRaises(KeyError):
            async with self.runtime():
                self.fail("missing original launch entered phase")
        self.assertNotIn("predicate", self.events)
        self.assertEqual(self.events[-1], "cleanup")

    async def test_runtime_refusal_cleans_original_processes_without_phase(self):
        self.fail_runtime = True
        with self.assertRaisesRegex(ValueError, "independent_predicate_refused"):
            async with self.runtime():
                self.fail("phase entered after independent refusal")
        self.assertEqual(self.events[-1], "cleanup")

    async def test_cleanup_failure_suppresses_following_backend(self):
        self.cleanup_status = "incomplete"
        reached_second = False
        with self.assertRaisesRegex(ValueError, "terminal_cleanup_incomplete"):
            async with self.runtime():
                pass
            reached_second = True
        self.assertFalse(reached_second)

    async def test_live_custody_change_after_phase_cleans_and_refuses(self):
        with self.assertRaisesRegex(ValueError, "original_live_identity_changed"):
            async with self.runtime():
                self.fail_live = True
        self.assertEqual(self.events[-1], "cleanup")

    async def test_expired_review_never_starts(self):
        self.reviewed["review"]["expires_at"] = "2000-01-01T00:00:00Z"
        with self.assertRaisesRegex(ValueError, "review_expired"):
            async with self.runtime():
                self.fail("expired phase entered")
        self.assertEqual(self.events, [])
        self.validator.validate_static.assert_not_called()

    async def test_static_original_plan_mismatch_never_starts(self):
        self.validator.validate_static.return_value = {**self.plan, "run_nonce": "replacement"}
        with self.assertRaisesRegex(ValueError, "original_plan_changed"):
            async with self.runtime():
                self.fail("changed plan entered")
        self.assertEqual(self.events, [])

    async def test_original_byte_wrapper_cannot_be_replaced_with_byte_count(self):
        original_capture = self.capture
        def invalid():
            bundle = original_capture()
            bundle["controller_context"]["raw_bytes"] = len(bundle["controller_context"]["raw_bytes"])
            return bundle
        self.live.capture_original_custody = invalid
        with self.assertRaisesRegex(ValueError, "original_controller_bytes"):
            async with self.runtime():
                self.fail("integer accepted as original controller bytes")
        self.assertNotIn("predicate", self.events)
        self.assertEqual(self.events[-1], "cleanup")

    async def test_successful_initial_runtime_admission_cannot_be_refreshed(self):
        async with self.runtime() as custody:
            original_time = custody.validated_monotonic_ns
            self.assertGreater(original_time, 0)
            with self.assertRaisesRegex(ValueError, "already_validated"):
                custody.capture()
            self.assertEqual(custody.validated_monotonic_ns, original_time)
            self.assertEqual(self.events.count("predicate"), 1)

    async def test_current_chain_keeps_original_admission_and_separate_actual_capture(self):
        async with self.runtime() as custody:
            original_time = custody.validated_monotonic_ns
            original_evidence = custody.evidence_reference
            def current_capture():
                bundle = self.capture()
                # This is a plumbing control only. L is not called with this
                # deliberately incomplete current-custody object.
                raw = o.encode({"format": "recall-dynamic-current-runtime-custody/v2", "control": "later observation",
                                "outer_launch": bundle["outer_launch"]})
                ref = o.reference(raw, "runtime-current-custody")
                path = self.root / "later-current-custody.json"
                o.private_new(path, raw)
                bundle["raw_evidence"][ref["sha256"]] = raw
                bundle["current_custody"] = ref
                bundle["references"] = {"current_custody": {"path": str(path), "sha256": ref["sha256"]}}
                return bundle
            self.live.revalidate_worker_recipe = AsyncMock()
            self.live.capture_original_custody = current_capture
            chain_ref, _, _ = await custody.current_chain("prepared")
            chain = self.files.json(chain_ref, "runtime-validation-chain")
            self.assertEqual(chain["validated_monotonic_ns"], original_time)
            self.assertEqual(chain["evidence"], original_evidence)
            self.assertEqual(chain["component"], custody.component_reference)
            self.assertEqual(chain["current_custody"]["kind"], "runtime-current-custody")
            current = self.files.json(chain["current_custody"], "runtime-current-custody")
            self.assertEqual(current["outer_launch"], custody.envelope["outer_launch"])
            self.assertEqual(self.events.count("predicate"), 1)


if __name__ == "__main__":
    unittest.main()
