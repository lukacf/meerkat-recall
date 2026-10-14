"""Offline v2 collector plumbing controls, never runtime/provider qualification."""
import asyncio
from collections.abc import Mapping
from contextlib import asynccontextmanager, nullcontext
import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import workload_orchestration as o
import workload_phase_capture as phases


def component():
    return {"identity": {"source_commit": "a" * 40, "artifact_sha256": "b" * 64, "binary_sha256": "c" * 64,
                         "backend": "elephant-db", "endpoint": "http://127.0.0.1:43130"}}


class PhaseInputControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.files = o.OriginalFiles(roots=(self.root,))
        self.names = ["household", "business"]
        refs = {}
        corpus = {"spaces": [{"id": name, "provider": "openai", "model": "declared", "dimensions": 8}
                             for name in self.names]}
        for name in phases.STATIC.values():
            refs[name] = self.save(name, corpus if name == "corpus" else {"static": name}, "static-" + name)
        refs["workload_intents"] = self.save("intents", {"format": "recall-workload-intents/v2", "backends": {
            "elephant-db": {"fixture_timestamp": "2026-10-07T00:00:00Z", "preparation_nonce": "original-preparation-123",
                            "measurement_nonce": "original-measurement-123"}}}, "workload-intents")
        refs["expected_configurations"] = self.save("blueprint", {name: {"record_security": {},
            "embedding_partition_model": "recall:declared", "input_envelope": {}} for name in self.names}, "expected-configurations")
        actual = {row["id"]: {"space_id": row["id"], "id": "space_config:actual-" + row["id"],
            "embedding_provider": row["provider"], "embedding_model": row["model"], "embedding_dims": row["dimensions"]}
            for row in corpus["spaces"]}
        actual_ref = self.save("actual", actual, "space-configurations")
        self.custody = SimpleNamespace(files=self.files, output=self.root, backend="elephant-db", component=component(),
            envelope={"actual_space_configurations": actual_ref},
            reviewed={"review": {"input_refs": refs, "helper_sha256": {phases.BRIDGE: "d" * 64}}},
            live=SimpleNamespace(phase={"plan": {"expires_at": "2099-01-01T00:00:00Z"}},
                                 token_files={name: str(self.root / (name + ".jwt")) for name in self.names}))

    def save(self, name, value, kind):
        return self.files.save(self.root / (name + ".json"), value, kind)

    def test_actual_configuration_ids_are_bound_after_runtime_only(self):
        manifest, loaded = phases.phase_inputs(self.custody, phase="preparation",
            authorization={"execution_authorized": True, "provider_dispatch_authorized": True})
        self.assertEqual(manifest["nonce"], "original-preparation-123")
        for name in self.names:
            item = loaded["expected_configurations"][name]
            self.assertEqual(item["configuration"]["id"], "space_config:actual-" + name)
            self.assertNotIn("configuration", self.files.json(self.custody.reviewed["review"]["input_refs"]["expected_configurations"])[name])
            self.assertEqual(manifest["space_credentials"][name], "service-" + name)
        self.assertNotIn("installed_runtime_evidence", loaded)
        self.assertNotIn("qualified", manifest)

    def test_measurement_local_role_labels_retain_same_original_signed_files(self):
        manifest, _ = phases.phase_inputs(self.custody, phase="measurement",
            authorization={"execution_authorized": True, "provider_dispatch_authorized": True})
        self.assertEqual(manifest["nonce"], "original-measurement-123")
        for index, name in enumerate(self.names):
            self.assertEqual(manifest["space_credentials"][name], f"service_{index}")
            self.assertEqual(manifest["credential_files"][f"service_{index}"], self.custody.live.token_files[name])

    def test_missing_original_allocation_cannot_become_execution_permission(self):
        for allocation in ({}, {"execution_authorized": True, "provider_dispatch_authorized": False}):
            with self.subTest(allocation=allocation), self.assertRaisesRegex(ValueError, "execution_allocation"):
                phases.phase_inputs(self.custody, phase="preparation", authorization=allocation)


class CohortControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "preparation").mkdir()
        self.files = o.OriginalFiles(roots=(self.root,))
        self.manifest = {"original": "manifest", "phase": "preparation"}
        self.caller = self.files.save(self.root / "preparation.manifest.json", self.manifest, "workload-phase-manifest")
        self.live = SimpleNamespace(revalidate_custody=Mock(), instance=SimpleNamespace(allocation=SimpleNamespace(remaining=Mock(return_value=1000))))
        self.custody = SimpleNamespace(component=component(), live=self.live, files=self.files, output=self.root,
                                      context={"context": {"anchor_sha256": "a" * 64}},
                                      reviewed={"review": {"expires_at": "2099-01-01T00:00:00Z"}})
        self.begin, self.close = {"original": "begin"}, {"original": "close"}
        self.bridge = SimpleNamespace(authorize_provider_allocation=Mock(return_value=self.begin),
            provider_accounting_with_evidence=Mock(return_value={"format": "recall-exclusive-cohort-close-envelope/v2",
                "close": self.close, "export": {"path": "/gateway-accounting/original-index", "ref": {}}}),
            export_cohort_evidence=Mock())
        self.cohort = phases.OriginalCohort(self.custody, self.bridge, self.caller)

    def test_same_one_close_request_and_no_legacy_fallback(self):
        manifest = self.manifest
        self.assertEqual(self.cohort.authorize_provider_allocation(manifest, {}, {}), self.begin)
        self.assertEqual(self.cohort.provider_accounting(manifest, {}, {}), self.close)
        self.bridge.provider_accounting_with_evidence.assert_called_once()
        with self.assertRaisesRegex(ValueError, "begin_once"):
            self.cohort.authorize_provider_allocation(manifest, {}, {})
        with self.assertRaisesRegex(ValueError, "close_once"):
            self.cohort.provider_accounting(manifest, {}, {})
        self.bridge.provider_accounting_with_evidence.assert_called_once()

    def test_original_clock_reserve_refuses_before_outbound_control(self):
        self.live.instance.allocation.remaining.return_value = 9
        with self.assertRaisesRegex(ValueError, "clock_reserve"):
            self.cohort.authorize_provider_allocation({}, {}, {})
        self.bridge.authorize_provider_allocation.assert_not_called()
        self.live.instance.allocation.remaining.return_value = 129
        with self.assertRaisesRegex(ValueError, "clock_reserve"):
            self.cohort.provider_accounting({}, {}, {})
        self.bridge.provider_accounting_with_evidence.assert_not_called()

    def test_export_reads_only_bundle_and_original_receipts(self):
        raw, refs = {}, {}
        for name, value, kind in (("begin", self.begin, "provider-cohort-begin"), ("close", self.close, "provider-cohort-close")):
            data = o.encode(value)
            refs[name] = o.reference(data, kind)
            raw[refs[name]["sha256"]] = data
        bundle = {"format": "recall-dynamic-provider-accounting/v2", **refs,
                  **{name: {"retained": name} for name in ("baseline", "phase_clock", "sealed", "capture_inventory", "dispatch_records")}}
        encoded = o.encode(bundle)
        ref = o.reference(encoded, "provider-accounting")
        raw[ref["sha256"]] = encoded
        forbidden = "f" * 64
        accesses = []
        class Lazy(Mapping):
            def __iter__(self):
                return iter([*raw, forbidden])
            def __len__(self):
                return len(raw) + 1
            def __getitem__(self, key):
                accesses.append(key)
                if key == forbidden:
                    raise AssertionError("must not eagerly read provider body")
                return raw[key]
        original_channel = {"sidecar": b' { "original" : "sidecar" }\n',
                            "mountinfo": b"original  mountinfo\n", "index": b'{ "original": "index" }',
                            "directory": b'{ "original" : "directory" }\n'}
        exported = {"reference": ref, "raw_evidence": Lazy(), "files": [], "channel": original_channel}
        self.bridge.export_cohort_evidence.return_value = exported
        self.cohort.authorize_provider_allocation(self.manifest, {}, {})
        self.cohort.provider_accounting(self.manifest, {}, {})
        actual, observed = self.cohort.export()
        self.assertIs(actual, exported)
        self.assertEqual(observed, bundle)
        self.assertEqual(len(accesses), 3)
        self.assertNotIn(forbidden, accesses)
        channel = self.files.json(self.cohort.channel_reference, "provider-accounting-channel")
        self.assertEqual(set(channel), {"format", "anchor_sha256", "sidecar", "mountinfo", "index", "directory", "caller"})
        self.assertEqual(channel["caller"], self.caller)
        self.assertEqual(channel["anchor_sha256"], "a" * 64)
        for name, raw in original_channel.items():
            self.assertEqual(self.files.get(channel[name]), raw)
        self.assertEqual(self.files.get(channel["caller"]), o.encode(self.manifest))
        self.assertEqual(set(actual), {"reference", "raw_evidence", "files", "channel"})
        with self.assertRaisesRegex(ValueError, "closed_export_missing"):
            self.cohort.export()
        self.bridge.export_cohort_evidence.assert_called_once()

    def test_changed_caller_refuses_before_original_cohort_begin(self):
        with self.assertRaisesRegex(ValueError, "caller_changed"):
            self.cohort.authorize_provider_allocation({**self.manifest, "original": "replacement"}, {}, {})
        self.bridge.authorize_provider_allocation.assert_not_called()

    def test_changed_original_caller_file_refuses_before_original_cohort_begin(self):
        (self.root / "preparation.manifest.json").write_bytes(o.encode({**self.manifest, "original": "replacement"}))
        with self.assertRaises(ValueError):
            self.cohort.authorize_provider_allocation(self.manifest, {}, {})
        self.bridge.authorize_provider_allocation.assert_not_called()

    def test_oversized_caller_reference_refuses_without_export_or_control(self):
        with self.assertRaisesRegex(ValueError, "caller_reference"):
            phases.OriginalCohort(self.custody, self.bridge, {**self.caller, "bytes": 131073})
        self.bridge.authorize_provider_allocation.assert_not_called()
        self.bridge.export_cohort_evidence.assert_not_called()

    def test_original_raw_inventory_ceiling_refuses_before_file_creation(self):
        with patch.object(o, "MAX_TOTAL", self.files.total):
            with self.assertRaisesRegex(ValueError, "inventory_bound"):
                self.files.save_raw(self.root / "unwritten.raw", b"original extra bytes", "provider-mountinfo")
        self.assertFalse((self.root / "unwritten.raw").exists())


class PreparationCaptureControls(unittest.TestCase):
    def test_reference_inventory_preserves_full_original_response_files(self):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            (root / "captures").mkdir()
            files = o.OriginalFiles(roots=(root,))
            published = [{"space": space, "key": "original-" + space, "revision": "r1",
                          "doc_id": "doc:" + space, "internal_doc_revision_id": "doc_revision:" + space}
                         for space in ("household", "business")]
            calls = [(row["space"], "/v1/spaces/" + row["space"]) for row in published]
            calls += [(row["space"], "/v1/docs/doc%3A" + row["space"] + "/revisions/doc_revision%3A" + row["space"]) for row in published]
            calls += calls[:2]
            originals = {}
            for index, (space, path) in enumerate(calls, 1):
                o.private_new(root / "captures" / f"{index:04}.request.json", o.encode({"original_intent_index": index}))
                raw = b"  " + o.encode({"space": space, "method": "GET", "path": path,
                    "response": {"body": {"original": index}}}) + b"\n"
                o.private_new(root / "captures" / f"{index:04}.json", raw)
                originals[index] = raw
            o.private_new(root / "observation.json", b'{ "original": "observation" }\n')
            preparation = SimpleNamespace(root=root, http_count=6, published=published,
                loaded={"expected_configurations": {space: {} for space in ("household", "business")}})
            refs, _ = phases.preparation_references(SimpleNamespace(files=files), preparation)
            before = files.json(refs["configuration_before"])
            self.assertEqual(files.get(before["household"]), originals[1])
            after = files.json(refs["configuration_after"])
            self.assertEqual(files.get(after["business"]), originals[6])
            immutable = files.json(refs["immutable_revisions"])
            self.assertEqual(files.get(immutable[0]["response"]), originals[3])
            self.assertEqual(files.get(refs["observation"]), b'{ "original": "observation" }\n')
            self.assertEqual(files.inline, {})


class EntrypointControls(unittest.IsolatedAsyncioTestCase):
    async def test_original_first_backend_cleanup_failure_prevents_second_start(self):
        spec = importlib.util.spec_from_file_location("d28_entry_controls", o.ROOT / "scripts/qualification/semantic_workload.py")
        entry = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(entry)
        events = []
        @asynccontextmanager
        async def runtime(files, reviewed, validator, startup, backend, installed, output):
            events.append("start:" + backend)
            yield SimpleNamespace()
            events.append("cleanup:" + backend)
            raise ValueError("original_cleanup_failed")
        with tempfile.TemporaryDirectory() as name:
            root = Path(name) / "new-run"
            reviewed = {"review": {}}
            prepared = {backend: (SimpleNamespace(files={}), reviewed, {}) for backend in entry.BACKENDS}
            phase_module = SimpleNamespace(prepare_and_measure=AsyncMock(return_value={"control": "observed"}))
            def load(_, relative, review):
                return phase_module if relative == entry.PHASES else SimpleNamespace()
            with patch.object(entry, "verify_helpers"), patch.object(entry, "load_helper", side_effect=load), patch.object(entry, "current_runtime", runtime), patch.object(entry, "reviewed_predicates", return_value=nullcontext()):
                with self.assertRaisesRegex(ValueError, "original_cleanup_failed"):
                    await entry.run({"output_directory": str(root)}, prepared)
            self.assertEqual(events, ["start:elephant-db", "cleanup:elephant-db"])
            report = o.strict_json((root / "observation.json").read_bytes())
            self.assertEqual(report["collector_verdict"], "failed")
            self.assertFalse(report["product_acceptance"])
            self.assertEqual(len(report["backends"]), 1)


if __name__ == "__main__":
    unittest.main()
