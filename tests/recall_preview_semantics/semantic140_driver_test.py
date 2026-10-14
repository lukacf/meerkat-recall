"""Closed successor collection/custody controls; never provider qualification.

Only controlled in-memory scheduling and temporary evidence files are used.
Original D28 tests remain separate and unchanged.
"""
import asyncio
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import time
from types import ModuleType
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(Path(__file__).resolve().parent))
import workload_orchestration as custody

spec = importlib.util.spec_from_file_location("semantic140_transport_controls", ROOT / "scripts/qualification/recall_workload.py")
transport = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transport)
profiles = transport.profiles

NAMES = {"contract": "contract", "corpus": "corpus", "provider_allocation": "allocation-proposal",
         "preparation_contract": "preparation-contract", "typed_delta": "allocation-typed-delta",
         "rendered_map": "rendered-map"}


def document(stem):
    return json.loads((ROOT / "tests/recall_preview" / (stem + ".json")).read_bytes())


def manifest():
    return {"profile_id": "semantic140-v1",
            **{name + "_sha256": value for name, value in profiles.SEMANTIC140.hashes.items()}}


class ClosedProfileControls(unittest.TestCase):
    def test_original_files_keep_exact_bytes_and_successor_keeps_all_known_answers(self):
        for name, suffix in NAMES.items():
            for prefix, selected in (("workload-", profiles.LEGACY), ("semantic140-", profiles.SEMANTIC140)):
                raw = (ROOT / "tests/recall_preview" / (prefix + suffix + ".json")).read_bytes()
                self.assertEqual(hashlib.sha256(raw).hexdigest(), selected.hashes[name])
        original, successor = document("workload-corpus"), document("semantic140-corpus")
        for field in ("sources", "queries", "spaces", "metric"):
            self.assertEqual(transport.canonical(successor[field]), transport.canonical(original[field]))
        old = {(row["caller_id"], row["ordinal"]): row for row in original["schedule"]}
        for row in successor["schedule"]:
            caller = int(row["caller_id"].removeprefix("caller-"))
            space, local = divmod(caller, 70)
            inherited = old[(f"caller-{space * 25 + local % 25:02}", row["ordinal"])]
            for field in ("query_id", "space", "ordinal"):
                self.assertEqual(row[field], inherited[field])
        self.assertEqual(len(successor["schedule"]), 560)

    def test_only_explicit_profile_and_all_six_exact_hashes_select_successor(self):
        self.assertIs(profiles.profile_for_manifest(manifest()), profiles.SEMANTIC140)
        self.assertIs(profiles.profile_for_allocation(profiles.SEMANTIC140.hashes["provider_allocation"]), profiles.SEMANTIC140)
        self.assertIs(profiles.profile_for_manifest({}), profiles.LEGACY)
        changes = [("profile_id", value) for value in (None, True, "140", "d28-v1")]
        changes += [(name + "_sha256", profiles.LEGACY.hashes[name]) for name in NAMES]
        for key, value in changes:
            candidate = manifest()
            candidate[key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                profiles.profile_for_manifest(candidate)
        for name in NAMES:
            candidate = manifest()
            del candidate[name + "_sha256"]
            with self.subTest(missing=name), self.assertRaises(ValueError):
                profiles.profile_for_manifest(candidate)
        with self.assertRaises(ValueError):
            profiles.profile_for_allocation("0" * 64)

    def test_closed_schedule_rejects_missing_duplicate_or_rewritten_evidence(self):
        contract, corpus = document("semantic140-contract"), document("semantic140-corpus")
        self.assertEqual(len(transport.validate_schedule(contract, corpus)), 100)
        mutations = [lambda x: x["schedule"].pop(),
                     lambda x: x["schedule"].__setitem__(1, x["schedule"][0]),
                     lambda x: x["queries"][0].update(query="substituted question"),
                     lambda x: x["queries"][0]["required"][0].update(quote_fragment="forged answer"),
                     lambda x: x["sources"][-1].update(text="unrelated source was silently replaced")]
        for mutate in mutations:
            candidate = copy.deepcopy(corpus)
            mutate(candidate)
            with self.assertRaises(ValueError):
                transport.validate_schedule(contract, candidate)
        candidate = dict(contract, concurrent_callers=139)
        with self.assertRaises(ValueError):
            transport.validate_schedule(candidate, corpus)
        with self.assertRaises(ValueError):
            transport.validate_schedule(document("workload-contract"), corpus)

    def test_successor_caps_never_leak_into_old_measurement_close(self):
        begin = {"cohort_id": 2, "baseline_sha256": "a" * 64}
        accounting = {"format": "recall-exclusive-cohort-accounting/v1", "verdict": "passed", "cohort_id": 2,
            "before_sha256": "a" * 64, "admission_closed": True, "unsettled_dispatches": 0,
            "per_request_association": "unmet", "after_sha256": "b" * 64,
            "capture_inventory_sha256": "c" * 64, "sealed_receipt_sha256": "d" * 64,
            "physical_dispatch_reservations": 1680, "reserved_nano_usd": 1680 * 1_064_960}
        transport.measurement_close(begin, accounting, profile_id="semantic140-v1")
        with self.assertRaises(ValueError):
            transport.measurement_close(begin, accounting)
        for updates in ({"physical_dispatch_reservations": 1681, "reserved_nano_usd": 1681 * 1_064_960},
                        {"unsettled_dispatches": 1}, {"reserved_nano_usd": 1},
                        {"before_sha256": "0" * 64}, {"per_request_association": "inferred"}):
            with self.subTest(updates=updates), self.assertRaises(ValueError):
                transport.measurement_close(begin, dict(accounting, **updates), profile_id="semantic140-v1")

    def test_observation_bound_is_profile_specific_without_large_allocation(self):
        ref = {"sha256": "a" * 64, "kind": "measurement-observation", "bytes": 64 * 1024**2 + 1}
        with self.assertRaises(ValueError):
            custody.checked_reference(ref)
        custody.checked_reference(ref, profile_id="semantic140-v1")
        with self.assertRaises(ValueError):
            custody.checked_reference(dict(ref, bytes=512 * 1024**2 + 1), profile_id="semantic140-v1")
        with self.assertRaises(ValueError):
            custody.checked_reference(ref, profile_id="arbitrary-quota")

    def test_distinct_physical_copies_count_even_when_hashes_match(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            files = custody.OriginalFiles(roots=(root,), profile_id="semantic140-v1")
            raw = b"identical"
            with patch.object(custody, "runtime_bound", return_value=len(raw) * 2):
                first = files.save_raw(root / "first", raw, "capture")
                second = files.save_raw(root / "second", raw, "capture")
                self.assertEqual(first, second)
                self.assertEqual(files.total, len(raw))
                self.assertEqual(files.physical_bytes, len(raw) * 2)
                with self.assertRaises(ValueError):
                    files.save_raw(root / "third", raw, "capture")
                self.assertFalse((root / "third").exists())

    def test_old_review_never_loads_new_expectations_and_new_review_restores_poisoned_cache(self):
        poisoned = ModuleType("semantic140_expectations")
        poisoned.marker = "unreviewed cached source"
        installed = ModuleType("installed_evidence_validator")
        installed.validate_installed = lambda: None
        installed.validate_process_proof = lambda: None
        provider = ModuleType("provider_evidence_validator")
        provider.validate_provider_accounting = lambda: None
        verified = ModuleType("semantic140_expectations")
        verified.marker = "exact reviewed source snapshot"
        modules = {"installed_evidence_validator": installed,
                   "provider_evidence_validator": provider,
                   "semantic140_expectations": verified}
        calls = []

        def load(name, path, review, root):
            calls.append((name, path))
            sys.modules[name] = modules[name]
            return modules[name]

        with patch.dict(sys.modules, {"semantic140_expectations": poisoned}), \
                patch.object(custody, "load_helper", side_effect=load):
            with custody.reviewed_predicates({}):
                self.assertIs(sys.modules["semantic140_expectations"], poisoned)
            self.assertEqual([name for name, _ in calls], list(custody.PREDICATES))
            calls.clear()
            with self.assertRaisesRegex(RuntimeError, "reviewed body failed"):
                with custody.reviewed_predicates({"profile_id": "semantic140-v1"}):
                    self.assertIs(sys.modules["semantic140_expectations"], verified)
                    self.assertEqual(calls[0], ("semantic140_expectations", custody.EXPECTATIONS_SOURCE))
                    raise RuntimeError("reviewed body failed")
            self.assertIs(sys.modules["semantic140_expectations"], poisoned)


class ScheduledCallerControls(unittest.IsolatedAsyncioTestCase):
    async def test_exact_140_callers_issue_all_560_original_rows_in_order(self):
        workload = transport.Workload.__new__(transport.Workload)
        workload.loaded = {"contract": document("semantic140-contract"), "corpus": document("semantic140-corpus")}
        workload.callers = {}
        workload.started_ns = time.monotonic_ns()
        active = peak = 0
        calls = []

        async def request(row):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            calls.append(dict(row))
            await asyncio.sleep(0)
            active -= 1

        workload.request = request
        with patch.object(transport.asyncio, "open_connection", side_effect=AssertionError("no network")):
            await workload.measure()
        self.assertEqual(len(calls), 560)
        self.assertEqual(peak, 140)
        self.assertEqual(active, 0)
        self.assertEqual({row["id"] for row in calls}, {row["id"] for row in workload.loaded["corpus"]["schedule"]})
        for caller in profiles.SEMANTIC140.caller_ids():
            self.assertEqual([row["ordinal"] for row in calls if row["caller_id"] == caller], [0, 1, 2, 3])
            self.assertLessEqual(workload.callers[caller]["ready_ms"], workload.barrier_released_ms)


if __name__ == "__main__":
    unittest.main()
