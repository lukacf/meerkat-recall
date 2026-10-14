"""Finite host-only controls; no product, resource probes or provider calls."""
import copy
from pathlib import Path
import subprocess
import sys
import unittest
from unittest import mock

import runtime_package_capacity as capacity


class CurrentPackagingCapacityTests(unittest.TestCase):
    def state(self):
        return {"format": "recall-actual-capacity-observation/v1", "capacity_ok": True,
                "nominal_cumulative_limit_enforced": False, "limit_bytes": None,
                "checks": 2007, "consumed_bytes": 1800 * capacity.GIB,
                "historical_snapshot": {"path": "/retained/history", "sha256": "a" * 64, "bytes": 1},
                "capacity_receipt": {"path": "/retained/capacity", "sha256": "b" * 64, "bytes": 1},
                "filesystems": {
                    "root": {"path": "/", "total": 1000 * capacity.GIB,
                             "used": 800 * capacity.GIB, "free": 200 * capacity.GIB,
                             "minimum_free_bytes": capacity.FLOOR},
                    "target": {"path": "/mnt/recall-release", "total": 1000 * capacity.GIB,
                               "used": 400 * capacity.GIB, "free": 600 * capacity.GIB,
                               "minimum_free_bytes": capacity.FLOOR}}}

    def samples(self, state):
        return {key: {name: item[name] for name in ("path", "total", "used", "free")}
                for key, item in state["filesystems"].items()}

    def test_isolated_cli_resolves_exact_siblings(self):
        script = Path(__file__).resolve().with_name("prepare_gateway_runtime.py")
        result = subprocess.run([sys.executable, "-I", "-B", str(script), "--help"],
                                capture_output=True, timeout=5, check=False)
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertIn(b"--capacity-binding", result.stdout)

    def test_original_observer_state_and_counters_are_returned_unchanged(self):
        state = self.state()
        original = copy.deepcopy(state)
        observer = mock.Mock(receipt=Path("/retained/observation"))
        observer.check.return_value = state
        adapter = capacity.PackagingBudget(observer)
        self.assertIs(adapter.check(), state)
        self.assertEqual(state, original)
        self.assertEqual(adapter.receipt, observer.receipt)
        observer.check.assert_called_once_with()

    def test_packaging_stops_on_recorded_low_capacity(self):
        state = self.state()
        state["capacity_ok"] = False
        observer = mock.Mock(receipt=Path("/retained/observation"))
        observer.check.return_value = state
        with self.assertRaises(ValueError):
            capacity.PackagingBudget(observer).check()
        self.assertFalse(state["capacity_ok"])
        observer.check.assert_called_once_with()

    def test_static_read_preserves_history_above_old_nominal_cap(self):
        state = self.state()
        original = copy.deepcopy(state)
        result = capacity.validate_samples(state, self.samples(state))
        self.assertEqual(state, original)
        self.assertEqual(result["original_checks"], 2007)
        self.assertEqual(result["original_consumed_bytes"], 1800 * capacity.GIB)
        self.assertFalse(result["counters_modified"])
        self.assertTrue(result["capacity_ok"])

    def test_static_refuses_either_filesystem_below_original_floor(self):
        for device in ("root", "target"):
            with self.subTest(device=device):
                state = self.state()
                samples = self.samples(state)
                samples[device]["free"] = capacity.FLOOR - 1
                with self.assertRaises(ValueError):
                    capacity.validate_samples(state, samples)

    def test_static_refuses_changed_device_total_path_or_floor(self):
        for mutation in ("device", "total", "path", "floor"):
            with self.subTest(mutation=mutation):
                state = self.state()
                samples = self.samples(state)
                if mutation == "device":
                    samples["foreign"] = samples.pop("target")
                elif mutation == "total":
                    samples["target"]["total"] += 1
                elif mutation == "path":
                    samples["target"]["path"] = "/other"
                else:
                    state["filesystems"]["target"]["minimum_free_bytes"] -= 1
                with self.assertRaises(ValueError):
                    capacity.validate_samples(state, samples)

    def test_unselected_adapter_binding_is_rejected_before_import(self):
        record = {"format": "recall-runtime-package-current-capacity/v1",
                  "adapter": {"path": "/unselected.py", "sha256": "0" * 64,
                              "bytes": capacity.ADAPTER_BYTES},
                  "capacity_receipt": {"path": "/retained/capacity", "sha256": capacity.CAPACITY_SHA256,
                                       "bytes": capacity.CAPACITY_BYTES},
                  "historical_ledger": str(capacity.HISTORY), "observation": "/new/observation",
                  "history_carry": {"path": "/retained/carry", "sha256": capacity.CARRY_SHA256,
                                    "bytes": capacity.CARRY_BYTES},
                  "root_minimum_free_bytes": capacity.FLOOR,
                  "target_minimum_free_bytes": capacity.FLOOR}
        with mock.patch.object(capacity, "checked_file", return_value=Path("/binding")), \
                mock.patch.object(capacity, "read_json", return_value=record), \
                mock.patch.object(capacity.importlib.util, "spec_from_file_location") as loader:
            with self.assertRaises(ValueError):
                capacity.binding({}, capacity.HISTORY)
            loader.assert_not_called()

    def test_static_lineage_requires_selected_latest_seed_before_sampling(self):
        state = self.state()
        state.update(historical_checks=9733, historical_consumed_bytes=1317796118528,
                     checks=9740, consumed_bytes=1317796118628)
        original = copy.deepcopy(state)
        carry = {"sealed_exact_copy": {**state["historical_snapshot"], "path": "/sealed/latest"},
                 "inherited_counters": {"checks": 9733, "consumed_bytes": 1317796118528}}
        self.assertIsNone(capacity.validate_lineage(state, original, carry, state["capacity_receipt"]))
        for mutation in ("sha256", "bytes", "historical_checks", "historical_consumed_bytes",
                         "checks", "consumed_bytes"):
            with self.subTest(mutation=mutation):
                changed = copy.deepcopy(state)
                if mutation == "sha256":
                    changed["historical_snapshot"]["sha256"] = "c" * 64
                elif mutation == "bytes":
                    changed["historical_snapshot"]["bytes"] += 1
                else:
                    changed[mutation] -= 1
                # Even mutually agreeing receipt and state cannot select a
                # different seed or inherited counters. Last counters still
                # compare against the actual earlier packaged receipt.
                claimed_original = copy.deepcopy(original)
                if mutation in ("sha256", "bytes", "historical_checks", "historical_consumed_bytes"):
                    claimed_original = copy.deepcopy(changed)
                value = {"observation": "/new/observation", "_carry": carry,
                         "capacity_receipt": state["capacity_receipt"]}
                receipt = {"resource_budget_binding": {}, "resource_budget_file": "/new/observation",
                           "resource_budget": claimed_original}
                with mock.patch.object(capacity, "binding", return_value=value), \
                        mock.patch.object(capacity, "read_json", return_value=changed), \
                        mock.patch.object(capacity.shutil, "disk_usage") as sample:
                    with self.assertRaises(ValueError):
                        capacity.validate_current_capacity({}, receipt, (), capacity.HISTORY)
                    sample.assert_not_called()


if __name__ == "__main__":
    unittest.main()
