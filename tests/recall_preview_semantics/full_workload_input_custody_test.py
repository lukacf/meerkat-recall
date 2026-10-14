"""Offline tests of the actual CLI input loader, with execution imports inert.

Driver, spy and transport entry objects are stubs. Only original-file custody is
under test; these controls cannot establish an external consumer verdict.
"""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def decode(raw):
    # Inert strict parser substitute; actual CLI imports the existing reviewed
    # transport decoder. These tests exercise its caller's original-byte path.
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate key")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    encode(value)
    return value


def load_cli():
    driver = ModuleType("full_workload_driver")
    for name in ("CONTRACT_SHA", "CORPUS_SHA", "CASES", "SPACES", "Driver", "NamedClient", "validate_inputs"):
        setattr(driver, name, Mock(name=name))
    spy = ModuleType("full_workload_spy")
    spy.ControlledSpy = Mock(name="no spy execution")
    transport = ModuleType("full_workload_transport")
    for name, value in {"Capture": Mock(), "encode": encode, "decode": decode, "endpoint": Mock(),
                        "require": require, "sha": lambda raw: hashlib.sha256(raw).hexdigest()}.items():
        setattr(transport, name, value)
    spec = importlib.util.spec_from_file_location("full_workload_input_controls", ROOT / "scripts/qualification/full_workload.py")
    module = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {driver.__name__: driver, spy.__name__: spy, transport.__name__: transport}):
        spec.loader.exec_module(module)
    return module


cli = load_cli()


class OriginalInputControls(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.inputs = cli.Inputs()

    def original(self, name, raw):
        path = self.root / name
        path.write_bytes(raw)
        path.chmod(0o600)
        return {"path": str(path), "sha256": hashlib.sha256(raw).hexdigest()}

    def descriptors(self):
        original = '{ "observation" : "original café" }\n'.encode()
        expected = b'{\n "expectation": "independent pre-output fixture"\n}\n'
        descriptor = {"owner": "OB3", "original": self.original("receipt.json", original),
                      "expectations": self.original("expected.json", expected)}
        return {"FW13": descriptor}, original, expected

    def test_both_exact_raw_documents_reach_carrier_without_reencoding(self):
        descriptors, original, expected = self.descriptors()
        result = cli.external_inputs(self.inputs, descriptors, copy.deepcopy(descriptors), "elephant-db", "original-nonce")
        carrier = result["FW13"]
        self.assertEqual(carrier["original_raw"], original)
        self.assertEqual(carrier["expectations_raw"], expected)
        self.assertEqual(carrier["original"], decode(original))
        self.assertNotEqual(carrier["original_raw"], encode(carrier["original"]))
        self.assertEqual(carrier["original_sha256"], hashlib.sha256(original).hexdigest())
        self.assertEqual(carrier["expectations_sha256"], hashlib.sha256(expected).hexdigest())
        self.assertEqual(self.inputs.bytes, len(original) + len(expected))
        self.assertNotIn("qualified", carrier)

    def test_legacy_descriptor_remains_missing_expectations(self):
        descriptors, original, _ = self.descriptors()
        del descriptors["FW13"]["expectations"]
        result = cli.external_inputs(self.inputs, descriptors, descriptors, "elephant-db", "original-nonce")["FW13"]
        self.assertEqual(result["original_raw"], original)
        self.assertNotIn("expectations_raw", result)
        self.assertNotIn("expectations_sha256", result)

    def test_changed_independent_descriptor_refuses_before_any_read(self):
        descriptors, _, _ = self.descriptors()
        allocation = copy.deepcopy(descriptors)
        allocation["FW13"]["expectations"]["sha256"] = "0" * 64
        with patch.object(self.inputs, "read_with_raw") as read, self.assertRaisesRegex(ValueError, "unreviewed"):
            cli.external_inputs(self.inputs, descriptors, allocation, "elephant-db", "original-nonce")
        read.assert_not_called()

    def test_both_inputs_share_original_aggregate_ceiling(self):
        descriptors, original, expected = self.descriptors()
        self.inputs.bytes = cli.INPUT_CAP - len(original) - len(expected) + 1
        with self.assertRaisesRegex(ValueError, "aggregate"):
            cli.external_inputs(self.inputs, descriptors, descriptors, "elephant-db", "original-nonce")
        self.assertEqual(self.inputs.bytes, cli.INPUT_CAP - len(expected) + 1)

    def test_aggregate_bound_is_checked_before_reading_contents(self):
        descriptor = self.original("bounded.json", b'{"bounded":true}')
        self.inputs.bytes = cli.INPUT_CAP
        with patch.object(cli.os, "fdopen") as reader, self.assertRaisesRegex(ValueError, "aggregate"):
            self.inputs.read_with_raw(descriptor)
        reader.assert_not_called()

    def test_four_mib_original_and_expectation_limit(self):
        descriptor = self.original("large.json", b" " * (4 * 1024**2 + 1))
        with self.assertRaisesRegex(ValueError, "bounded regular"):
            self.inputs.read_with_raw(descriptor)
        self.assertEqual(self.inputs.bytes, 0)

    def test_private_owner_mode_and_symlink_requirements(self):
        descriptor = self.original("private.json", b"{}")
        Path(descriptor["path"]).chmod(0o644)
        with self.assertRaisesRegex(ValueError, "private"):
            self.inputs.read_with_raw(descriptor)
        # Ordinary nonsecret static fixtures retain their existing readable mode.
        self.assertEqual(self.inputs.read(descriptor), {})
        Path(descriptor["path"]).chmod(0o600)
        alias = self.root / "alias.json"
        alias.symlink_to(descriptor["path"])
        with self.assertRaisesRegex(ValueError, "canonical"):
            self.inputs.read_with_raw({**descriptor, "path": str(alias)})

    def test_wrong_hash_and_reused_path_with_changed_bytes_refuse(self):
        descriptor = self.original("fixed.json", b'{"a":1}')
        with self.assertRaisesRegex(ValueError, "hash differs"):
            self.inputs.read_with_raw({**descriptor, "sha256": "0" * 64})
        self.inputs.read_with_raw(descriptor)
        changed = b'{"a":2}'
        Path(descriptor["path"]).write_bytes(changed)
        with self.assertRaisesRegex(ValueError, "path was replaced"):
            self.inputs.read_with_raw({**descriptor, "sha256": hashlib.sha256(changed).hexdigest()})

    def test_strict_transport_parser_receives_original_once(self):
        descriptor = self.original("spacing.json", b'{ "a": 1 }\n')
        with patch.object(cli, "decode", wraps=decode) as parser:
            value, raw = self.inputs.read_with_raw(descriptor)
        parser.assert_called_once_with(raw)
        self.assertEqual(value, {"a": 1})

    def test_homecore_cannot_adopt_ob3_expectation_descriptor(self):
        descriptors, _, _ = self.descriptors()
        descriptors["FW13"]["owner"] = "HomeCore"
        with patch.object(self.inputs, "read_with_raw") as read, self.assertRaisesRegex(ValueError, "only for OB3"):
            cli.external_inputs(self.inputs, descriptors, descriptors, "elephant-db", "original-nonce")
        read.assert_not_called()
        del descriptors["FW13"]["expectations"]
        result = cli.external_inputs(self.inputs, descriptors, descriptors, "elephant-db", "original-nonce")["FW13"]
        self.assertEqual(result["owner"], "HomeCore")
        self.assertNotIn("expectations_raw", result)

    def test_complete_eighteen_source_pins_retain_original_seven_and_fixed_oracle(self):
        original_seven = {
            "scripts/qualification/full_workload.py", "scripts/qualification/ob3_outage_oracle.py",
            *{"tests/recall_preview_semantics/full_workload_" + name + ".py"
              for name in ("driver", "transport", "spy", "graph", "load")},
        }
        installed_and_runtime = {
            "tests/recall_preview/fixtures.py",
            "tests/recall_preview/matrix.py",
            "tests/recall_preview/oracle.py",
            "scripts/qualification/recall_preview.py",
            "scripts/release/artifact.py",
            "scripts/qualification/installed_process_capture.py",
            "scripts/qualification/installed_linux_offline.py",
            "scripts/qualification/snapshot_inspection_capture.py",
            "scripts/qualification/runtime_package_bootstrap_contract.py",
            "scripts/qualification/full_workload_runtime_inspection.py",
            "scripts/qualification/full_workload_owned_runtime.py",
        }
        actual = [str(path.relative_to(ROOT)) for path in cli.FILES]
        self.assertEqual(len(actual), 18)
        self.assertTrue(original_seven.isdisjoint(installed_and_runtime))
        self.assertEqual(set(actual), original_seven | installed_and_runtime)
        self.assertEqual(
            [path for name, path in cli.MODULES if name == "full_workload_ob3_oracle"],
            ["scripts/qualification/ob3_outage_oracle.py"],
        )


if __name__ == "__main__":
    unittest.main()
