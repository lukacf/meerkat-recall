"""Original-byte custody controls with all OS process/socket probes mocked."""
from __future__ import annotations

from collections.abc import Mapping
from contextlib import ExitStack
import copy
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

import runtime_package_bootstrap_evidence as evidence
import runtime_package_bootstrap_contract as contract
import runtime_package_bootstrap_test as fixtures


def stat(pid, parent, group, session, start, state="S"):
    fields = [state, str(parent), str(group), str(session)] + ["0"] * 15 + [str(start)]
    return f'{pid} (name ) with spaces) {" ".join(fields)}\n'.encode()


class Controls(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        for target in ("subprocess.Popen", "socket.create_connection", "asyncio.open_connection"):
            self.stack.enter_context(mock.patch(target, side_effect=AssertionError("execution forbidden")))
        self.plan = fixtures.plan()
        self.root = Path(self.plan["run_directory"])
        self.receipt = fixtures.receipt(self.plan)
        self.files, self.written, self.reads = {}, {}, []
        def put(path, raw):
            self.files[str(path)] = raw
            return {"path": str(path), "sha256": contract.sha(raw)}
        self.put = put
        self.inventory = {"format": "recall-gateway-inputs/v1", "files": [
            {"path": "original-plan", "bytes": 2, "sha256": contract.sha(b"{}")}]}
        self.outer_manifest = b'{ "format": "original-outer-manifest", "original": true }\n'
        self.outer_spec = b'{ "format": "original-outer-spec", "original": true }\n'
        manifest = put("/gateway-manifest.json", self.outer_manifest)
        spec = put("/gateway-launch.json", self.outer_spec)
        self.context = {"anchor": {"input_inventory_sha256": contract.sha(contract.encode(self.inventory)),
            "manifest_sha256": manifest["sha256"], "configuration_sha256": spec["sha256"],
            "clock": {"boot_id": "original-boot"}},
            "process": {"pid": 20, "ppid": 10, "pgrp": 20, "session": 20, "start_ticks": 200},
            "network": {"namespace": "net:[123]"}}
        self.context["anchor_sha256"] = contract.sha(contract.encode(self.context["anchor"]))
        put("/launch-anchor.json", contract.encode(self.context["anchor"]))
        self.context["captures"] = {name: put(f"/work/gateway.{name}", (name + " original\n").encode())
                                    for name in ("dev", "ipv4", "ipv6")}
        self.context_ref = put("/work/context.json", contract.encode(self.context))
        self.receipt["runtime_config_sha256"] = put(self.root / "service/.elephant/elephant_config.toml", b"original = 1\n")["sha256"]
        put(self.root / "plan.json", contract.encode(self.plan))
        self.receipt["isolation_sha256"] = put("/work/isolation.json", b'{ "original-isolation": true }\n')["sha256"]
        logs = [str(self.root / "logs/01.stdout"), str(self.root / "logs/01.stderr")]
        self.log_raw = b"original bounded CLI diagnostic\n"
        hashes = {logs[0]: put(logs[0], b"")["sha256"], logs[1]: put(logs[1], self.log_raw)["sha256"]}
        journal = {"format": "recall-runtime-process-commands/v1", "processes": [
            {"index": 1, "pid": 50, "exit_code": 0, "logs": logs, "log_sha256": hashes},
            {"index": 2, "pid": 40, "exit_code": None, "logs": ["/work/unfinished"], "log_sha256": None}]}
        self.receipt["process_commands"] = put(self.root / "process-commands.json", contract.encode(journal))
        request = put(self.root / "captures/001.request.json", b'{ "actual-request": true }\n')
        response = put(self.root / "captures/001.response.json", b'{ "actual-response": true }\n')
        self.receipt["captures"] = [{"request_index": 1, "request_path": request["path"],
            "request_sha256": request["sha256"], "response_path": response["path"], "response_sha256": response["sha256"]}]
        for index in (2, 3):
            req = put(self.root / f"captures/{index:03}.request.json", contract.encode({"original request": index}))
            resp = put(self.root / f"captures/{index:03}.response.json", contract.encode({"original response": index}))
            self.receipt["captures"].append({"request_index": index, "request_path": req["path"],
                "request_sha256": req["sha256"], "response_path": resp["path"], "response_sha256": resp["sha256"]})
        self.receipt_ref = put(self.root / "runtime-bootstrap.json", contract.encode(self.receipt))
        self.files.update({"/proc/20/stat": stat(20, 10, 20, 20, 200),
            "/proc/30/stat": stat(30, 20, 20, 20, 300), "/proc/40/stat": stat(40, 30, 40, 40, 400),
            "/proc/sys/kernel/random/boot_id": b"original-boot\n"})
        self.cli = SimpleNamespace(pid=50, reaped=True, threads=[SimpleNamespace(is_alive=lambda: False)])
        self.service = SimpleNamespace(pid=40, reaped=False, threads=[SimpleNamespace(is_alive=lambda: True)])
        for index, child in enumerate((self.cli, self.service), 1):
            snapshot = {"index": index, "identity": {"pid": child.pid}, "original invocation": index}
            child.capture_proof = SimpleNamespace(snapshot=lambda value=snapshot: copy.deepcopy(value))
        original_event = b'{ "original process event": true }\n'
        event = put(self.root / "captures/original-event.json", original_event)
        self.instance = SimpleNamespace(root=self.root, allocation=SimpleNamespace(remaining=mock.Mock(return_value=100)),
            processes=SimpleNamespace(items=[self.cli, self.service], capture_journal=SimpleNamespace(files={event["path"]:
                {"sha256": event["sha256"], "bytes": len(original_event), "kind": "installed-process-event"}})),
            http=SimpleNamespace(captures=copy.deepcopy(self.receipt["captures"])),
            worker_recipe_observation={"request_index": 1},
            embedding_recipe_observations={"household": {"request_index": 2}, "business": {"request_index": 3}})
        self.gateway = SimpleNamespace(read_context=mock.Mock(return_value=self.context),
            input_inventory=mock.Mock(return_value=self.inventory))
        def read(path, maximum, **_):
            path = str(path)
            self.reads.append(path)
            raw = self.files[path]
            if len(raw) > maximum:
                raise ValueError("mocked original read bound")
            return raw
        self.reader = self.stack.enter_context(mock.patch.object(evidence, "read_bound", side_effect=read))
        def write(path, raw):
            path = str(path)
            if path in self.files:
                raise FileExistsError("original is immutable")
            self.files[path] = self.written[path] = raw
        self.stack.enter_context(mock.patch.object(evidence, "private_new", side_effect=write))
        self.stack.enter_context(mock.patch.object(evidence.os, "getpid", return_value=30))
        self.stack.enter_context(mock.patch.object(evidence, "file_hash", side_effect=lambda path:
            "b" * 64 if str(path) == "/opt/recall/bin/elephant" else "e" * 64))
        self.stack.enter_context(mock.patch.object(evidence.os, "readlink", side_effect=lambda path:
            "net:[123]" if path == "/proc/self/ns/net" else
            "/opt/recall/bin/elephant" if path == "/proc/40/exe" else "/usr/bin/python3"))

    def capture(self, sequence=1):
        return evidence.capture_live_startup(self.instance, self.receipt_ref, self.context_ref,
            self.gateway, {"original": "G expectation"}, sequence=sequence)

    def test_original_logs_network_and_stat_are_hash_bound_without_token_reads(self):
        result = self.capture()
        self.assertEqual(result["raw_evidence"][contract.sha(self.log_raw)], self.log_raw)
        current = contract.strict_json(result["raw_evidence"][result["current_processes"]["sha256"]])
        self.assertEqual(set(current["processes"]), {"controller", "bootstrap", "recall"})
        self.assertEqual(current["processes"]["recall"]["identity"], self.receipt["processes"]["recall"])
        original_stat = self.files["/proc/40/stat"]
        self.assertEqual(result["raw_evidence"][current["processes"]["recall"]["stat"]["sha256"]], original_stat)
        self.assertEqual(result["raw_evidence"][current["processes"]["recall"]["stat_after"]["sha256"]], original_stat)
        self.assertGreaterEqual(result["controller_context"]["observed_monotonic_ns"], current["observed_monotonic_ns"])
        self.assertNotIn("/work/unfinished", self.reads)
        self.assertFalse(any(path.endswith(".token") or path.endswith("signing.key") for path in self.reads))
        inventory = contract.strict_json(result["raw_evidence"][result["capture_inventory"]["sha256"]])
        self.assertEqual(len(inventory["files"]), len({row["path"] for row in inventory["files"]}))
        self.assertIn("process-log", {row["ref"]["kind"] for row in inventory["files"]})
        self.assertIn("gateway-anchor", {row["ref"]["kind"] for row in inventory["files"]})
        self.assertEqual(self.files[self.receipt_ref["path"]], contract.encode(self.receipt))
        self.assertEqual(result["controller_context"]["raw_bytes"], self.files[self.context_ref["path"]])
        commands = contract.strict_json(result["raw_evidence"][result["command_inventory"]["sha256"]])
        self.assertEqual(commands["format"], "recall-dynamic-startup-command-inventory/v2")
        self.assertEqual(len(commands["process_proofs"]), 2)
        self.assertEqual(commands["commands"]["sha256"], self.receipt["process_commands"]["sha256"])
        self.assertIn("installed-process-event", {row["ref"]["kind"] for row in inventory["files"]})
        self.assertEqual(self.gateway.read_context.call_count, 2)
        for call in self.gateway.read_context.call_args_list:
            self.assertTrue(call.kwargs["immediate_parent"])

    def test_later_recipe_reads_retain_original_requests_without_rewriting_startup(self):
        first = self.capture()
        for index in (4, 5, 6):
            req = self.put(self.root / f"captures/{index:03}.request.json", contract.encode({"later request": index}))
            resp = self.put(self.root / f"captures/{index:03}.response.json", contract.encode({"later response": index}))
            self.instance.http.captures.append({"request_index": index, "request_path": req["path"],
                "request_sha256": req["sha256"], "response_path": resp["path"], "response_sha256": resp["sha256"]})
        self.instance.worker_recipe_observation = {"request_index": 4}
        self.instance.embedding_recipe_observations = {"household": {"request_index": 5}, "business": {"request_index": 6}}
        result = self.capture(sequence=2)
        current = contract.strict_json(result["raw_evidence"][result["current_custody"]["sha256"]])
        self.assertEqual(current["worker_recipe_capture"]["request_index"], 4)
        self.assertEqual(current["provider_recipe_captures"]["business"]["request_index"], 6)
        self.assertEqual(current["capture_inventory"], result["capture_inventory"])
        self.assertNotEqual(first["current_custody"], result["current_custody"])
        self.assertEqual(self.files[self.receipt_ref["path"]], contract.encode(self.receipt))
        self.assertIn(current["provider_recipe_captures"]["household"]["response"]["sha256"], result["raw_evidence"])

    def test_original_outer_files_are_unmodified_and_share_current_input_inventory(self):
        original_outer = None
        for sequence in (1, 2):
            with self.subTest(sequence=sequence):
                result = self.capture(sequence=sequence)
                outer = contract.strict_json(result["raw_evidence"][result["outer_launch"]["sha256"]])
                self.assertEqual(set(outer), {"format", "manifest", "spec", "input_inventory"})
                self.assertEqual(outer["format"], "recall-gateway-launch-inputs/v2")
                self.assertLessEqual(result["outer_launch"]["bytes"], 16384)
                self.assertEqual(result["raw_evidence"][outer["manifest"]["sha256"]], self.outer_manifest)
                self.assertEqual(result["raw_evidence"][outer["spec"]["sha256"]], self.outer_spec)
                current = contract.strict_json(result["raw_evidence"][result["current_processes"]["sha256"]])
                self.assertEqual(outer["input_inventory"], current["input_inventory"])
                custody = contract.strict_json(result["raw_evidence"][result["current_custody"]["sha256"]])
                self.assertEqual(custody["outer_launch"], result["outer_launch"])
                inventory = contract.strict_json(result["raw_evidence"][result["capture_inventory"]["sha256"]])
                paths = {row["path"]: row["ref"] for row in inventory["files"]}
                self.assertEqual(paths["/gateway-manifest.json"], outer["manifest"])
                self.assertEqual(paths["/gateway-launch.json"], outer["spec"])
                self.assertIn(result["outer_launch"], paths.values())
                if original_outer is None:
                    original_outer = result["outer_launch"]
                self.assertEqual(result["outer_launch"], original_outer)
        self.assertEqual(self.files[self.receipt_ref["path"]], contract.encode(self.receipt))
        self.assertFalse(any(path.endswith(".token") or path.endswith("signing.key") for path in self.reads))

    def test_outer_file_reserialization_missing_file_and_oversize_refuse(self):
        for sequence, change in enumerate(("normalize", "missing", "oversize"), 1):
            for key, original in (("/gateway-manifest.json", self.outer_manifest),
                                  ("/gateway-launch.json", self.outer_spec)):
                self.files[key] = original
            path = "/gateway-launch.json"
            if change == "normalize":
                self.files[path] = contract.encode(contract.strict_json(self.outer_spec))
            elif change == "missing":
                del self.files[path]
            else:
                self.files[path] = b" " * 131073
            with self.subTest(change=change), self.assertRaises((ValueError, KeyError)):
                self.capture(sequence=sequence)

    def test_outer_file_change_during_process_observation_refuses_before_current_packet(self):
        original = self.reader.side_effect
        def changed(path, maximum, **kwargs):
            if str(path) == "/proc/40/stat":
                self.files["/gateway-manifest.json"] = b"changed after original capture"
            return original(path, maximum, **kwargs)
        self.reader.side_effect = changed
        with self.assertRaisesRegex(ValueError, "changed after capture"):
            self.capture()
        self.assertFalse(any(path.endswith("-current-runtime.json") for path in self.written))

    def test_missing_prelaunch_proof_or_changed_original_capture_prefix_refuses(self):
        self.cli.capture_proof = None
        with self.assertRaisesRegex(ValueError, "prelaunch process proof"):
            self.capture()

    def test_frozen_startup_requests_cannot_be_replaced_by_later_reads(self):
        self.instance.http.captures[0]["request_sha256"] = "9" * 64
        with self.assertRaisesRegex(ValueError, "startup capture prefix changed"):
            self.capture()

    def test_changed_original_setup_log_refuses(self):
        self.files[str(self.root / "logs/01.stderr")] = b"rewritten"
        with self.assertRaises(ValueError):
            self.capture()

    def test_shared_journal_uses_original_four_mib_log_limit_in_both_passes(self):
        path = str(self.root / "logs/01.stderr")
        for sequence, size in enumerate((2 * 1024**2 + 1, 4 * 1024**2, 4 * 1024**2 + 1), 1):
            with self.subTest(size=size):
                raw = b"x" * size
                reference = self.put(path, raw)
                journal = contract.strict_json(self.files[self.receipt["process_commands"]["path"]])
                journal["processes"][0]["log_sha256"][path] = reference["sha256"]
                self.receipt["process_commands"] = self.put(self.receipt["process_commands"]["path"],
                                                            contract.encode(journal))
                self.receipt_ref = self.put(self.receipt_ref["path"], contract.encode(self.receipt))
                self.instance.processes.capture_journal.files[path] = {
                    "sha256": reference["sha256"], "bytes": size, "kind": "process-log"}
                if size <= contract.LIMITS["log_bytes_per_stream"]:
                    result = self.capture(sequence=sequence)
                    self.assertEqual(result["raw_evidence"][reference["sha256"]], raw)
                    self.assertGreaterEqual(self.reads.count(path), 2)
                else:
                    with self.assertRaises(ValueError):
                        self.capture(sequence=sequence)

    def test_shared_journal_metadata_keeps_two_mib_limit(self):
        raw = b"x" * (2 * 1024**2 + 1)
        reference = self.put(self.root / "captures/oversized-event.json", raw)
        self.instance.processes.capture_journal.files[reference["path"]] = {
            "sha256": reference["sha256"], "bytes": len(raw), "kind": "installed-process-event"}
        with self.assertRaises(ValueError):
            self.capture()

    def test_lazy_index_membership_reads_no_body_and_lookup_rechecks_original(self):
        result = self.capture()
        originals = result["raw_evidence"]
        self.assertIsInstance(originals, Mapping)
        self.assertNotIsInstance(originals, dict)
        checksum = contract.sha(self.log_raw)
        self.reads.clear()
        self.assertIn(checksum, originals)
        self.assertNotIn("0" * 64, originals)
        self.assertIn(checksum, list(originals))
        self.assertEqual(len(originals), len(list(originals)))
        self.assertEqual(self.reads, [])
        self.assertEqual(originals[checksum], self.log_raw)
        self.assertEqual(self.reads, [str(self.root / "logs/01.stderr")])
        self.files[str(self.root / "logs/01.stderr")] = b"z" * len(self.log_raw)
        with self.assertRaisesRegex(ValueError, "changed after capture"):
            originals[checksum]

    def test_lazy_lookup_rechecks_every_original_alias_without_cache(self):
        capture = evidence.Capture(self.root, 1)
        first = self.put(self.root / "first", b"same original")
        second = self.put(self.root / "second", b"same original")
        for reference in (first, second):
            capture.original(reference, "same-kind", 100)
        self.reads.clear()
        self.assertEqual(capture.raw_evidence[first["sha256"]], b"same original")
        self.assertEqual(self.reads, [first["path"], second["path"]])
        self.files[second["path"]] = b"changed alias"
        with self.assertRaisesRegex(ValueError, "changed after capture"):
            capture.raw_evidence[first["sha256"]]

    def test_setup_exit_is_not_accepted_without_actual_reaping_and_drained_logs(self):
        self.cli.reaped = False
        with self.assertRaises(ValueError):
            self.capture()
        self.cli.reaped = True
        self.cli.threads = [SimpleNamespace(is_alive=lambda: True)]
        with self.assertRaises(ValueError):
            self.capture(sequence=2)

    def test_changed_process_start_ticks_refuses(self):
        self.files["/proc/40/stat"] = stat(40, 30, 40, 40, 401)
        with self.assertRaises(ValueError):
            self.capture()

    def test_process_replacement_between_original_stat_captures_refuses(self):
        original = self.reader.side_effect
        seen = 0
        def changed(path, maximum, **kwargs):
            nonlocal seen
            if str(path) == "/proc/40/stat":
                seen += 1
                if seen == 2:
                    return stat(40, 30, 40, 40, 401)
            return original(path, maximum, **kwargs)
        self.reader.side_effect = changed
        with self.assertRaises(ValueError):
            self.capture()

    def test_foreign_bootstrap_process_cannot_supply_current_carrier(self):
        with mock.patch.object(evidence.os, "getpid", return_value=99), self.assertRaises(ValueError):
            self.capture()

    def test_changed_current_executable_hash_refuses(self):
        with mock.patch.object(evidence, "file_hash", return_value="f" * 64), self.assertRaises(ValueError):
            self.capture()

    def test_changed_original_input_inventory_refuses(self):
        self.gateway.input_inventory.return_value = {"format": "changed"}
        with self.assertRaises(ValueError):
            self.capture()

    def test_stat_parser_does_not_accept_zombie_or_recycled_identity(self):
        for raw, pid in ((stat(40, 30, 40, 40, 400, "Z"), 40),
                         (stat(40, 30, 40, 40, 400), 41), (b"broken", 40)):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                evidence.stat_identity(raw, pid)

    def test_same_original_path_cannot_change_kind_and_existing_channels_stay_bounded(self):
        capture = evidence.Capture(self.root, 1)
        capture.retain("/work/original", b"original", "kind")
        with self.assertRaises(ValueError):
            capture.retain("/work/original", b"original", "other")
        with mock.patch.object(evidence, "MAX_BYTES", 8), self.assertRaises(ValueError):
            capture.retain("/work/another", b"more", "kind")
        with self.assertRaises(ValueError):
            evidence.Capture(self.root, 17)


if __name__ == "__main__":
    unittest.main()
