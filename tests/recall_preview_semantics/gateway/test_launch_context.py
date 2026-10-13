"""Offline custody controls. No processes, sockets, namespaces or providers."""
from contextlib import nullcontext
import os
from types import SimpleNamespace
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import launch_context as context

BOOT = "12345678-1234-1234-1234-123456789abc"
BRIDGE = {"pid": 2, "ppid": 1, "pgrp": 1, "session": 1, "start_ticks": 90}
RAW = {"dev": b"Inter-| Receive\n lo: 0 0 0 0\n", "ipv4": b"Iface Destination Gateway\n", "ipv6": b""}


def anchor():
    value = {key: "a" * 64 for key in context.HASH_FIELDS}
    value.update(format=context.ANCHOR_FORMAT, run_nonce="original-reviewed-run-001",
                 source_commit="b" * 40, allocation_sha256=None, input_inventory_sha256=None,
                 clock={"wall_start": 1000.0, "mono_start": 500.0, "boot_id": BOOT,
                        "deadline_wall": 6400.0, "deadline_mono": 5900.0, "time_cap_seconds": 5400},
                 caps={"physical_dispatches": 432, "maximum_nano_usd": 75_000_000_000},
                 parent={"process": {"pid": 100, "ppid": 99, "pgrp": 100, "session": 100, "start_ticks": 10},
                         "network_namespace": "net:[1000]"})
    value["bridge_sha256"] = context.digest(b"bridge source")
    value["context_helper_sha256"] = context.digest(b"context source")
    return value


def observation():
    original = anchor()
    checksum = context.digest(context.encode(original))
    return {"format": context.CONTEXT_FORMAT, "anchor": original, "anchor_sha256": checksum,
            "process": BRIDGE.copy(), "network": {"namespace": "net:[2000]", "parent_namespace": "net:[1000]",
            "interfaces": ["lo"], "ipv4_routes": 0, "ipv6_loopback_routes": 0},
            "observed_wall": 1001.0, "observed_mono": 501.0,
            "captures": {name: {"path": f"/work/gateway-context-{checksum}.{name}.txt", "sha256": context.digest(raw)}
                         for name, raw in RAW.items()}}


class ContextTests(unittest.TestCase):
    def setUp(self):
        for name in ("socket.socket", "subprocess.Popen"):
            guard = patch(name, side_effect=AssertionError("no network or processes in offline context controls"))
            guard.start()
            self.addCleanup(guard.stop)

    def validate(self, value, **changes):
        args = dict(expected={"run_nonce": "original-reviewed-run-001", "artifact_sha256": "a" * 64},
                    now_wall=1002.0, now_mono=502.0)
        args.update(changes)
        return context.validate_context(value, **args)

    def test_exact_original_context_retains_absolute_deadlines_and_caps(self):
        value = self.validate(observation())
        self.assertEqual(value["anchor"]["clock"]["deadline_mono"], 5900.0)
        self.assertEqual(value["anchor"]["caps"]["physical_dispatches"], 432)
        self.assertNotIn("qualified", value)

    def test_relabelled_identity_expiration_and_clock_rollback_refuse(self):
        value = observation()
        for changes in ({"expected": {"run_nonce": "replacement"}}, {"now_wall": 6400.0},
                        {"now_mono": 5900.0}, {"now_mono": 500.0}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                self.validate(value, **changes)

    def test_anchor_does_not_allow_budget_or_start_deadline_reset(self):
        for modify in (lambda a: a["caps"].update(physical_dispatches=433),
                       lambda a: a["clock"].update(deadline_mono=5901.0),
                       lambda a: a["caps"].update(maximum_nano_usd=True),
                       lambda a: a.update(allocation_sha256="c" * 64)):
            value = anchor()
            modify(value)
            with self.assertRaises(ValueError):
                context.validate_anchor(value)

    def test_cohort_has_only_its_original_separate_allocation(self):
        value = anchor()
        value["allocation_sha256"] = "c" * 64
        value["clock"].update(time_cap_seconds=7800, deadline_wall=8800.0, deadline_mono=8300.0)
        value["caps"].update(physical_dispatches=2100, maximum_nano_usd=3_000_000_000)
        context.validate_anchor(value)
        value["caps"]["maximum_nano_usd"] = 75_000_000_000
        with self.assertRaises(ValueError):
            context.validate_anchor(value)

    def test_copied_isolation_flag_or_same_namespace_refuses(self):
        value = observation()
        value["network"]["namespace"] = "net:[1000]"
        with self.assertRaises(ValueError):
            self.validate(value)
        value = observation()
        value["network_isolation_verified"] = True
        with self.assertRaises(ValueError):
            self.validate(value)

    def test_external_interfaces_and_routes_refuse_from_original_bytes(self):
        for values in ((RAW["dev"] + b"eth0: 0\n", RAW["ipv4"], b""),
                       (RAW["dev"], RAW["ipv4"] + b"eth0 0 0\n", b""),
                       (RAW["dev"], RAW["ipv4"], b"0 0 0 0 0 0 0 0 0 eth0\n")):
            with self.assertRaises(ValueError):
                context.parse_network(*values)

    def test_process_start_ticks_are_parsed_after_command_with_parentheses(self):
        # stat fields 3..22: state, ppid, pgrp, session, fifteen intervening
        # fields, then the original process start time.
        raw = ("2 (name with (nested) chars) " + " ".join(["S", "1", "2", "3"] + ["0"] * 15 + ["777", "0"]) + "\n").encode()
        with patch.object(context, "read_bounded", return_value=raw):
            self.assertEqual(context.process_identity(2), {"pid": 2, "ppid": 1, "pgrp": 2, "session": 3, "start_ticks": 777})

    def test_original_private_files_cannot_be_overwritten_or_follow_symlinks(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "original.json"
            ref = context.write_original(path, b"original")
            self.assertEqual(context.read_bounded(path, private=True), b"original")
            self.assertEqual(ref["sha256"], context.digest(b"original"))
            with self.assertRaises(FileExistsError):
                context.write_original(path, b"replacement")
            alias = Path(directory) / "alias.json"
            alias.symlink_to(path)
            with self.assertRaises(OSError):
                context.read_bounded(alias, private=True)

    def test_duplicate_nonfinite_and_unknown_fields_refuse(self):
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":1e999}', b'\xff'):
            with self.assertRaises((ValueError, UnicodeError)):
                context.strict_json(raw)
        value = observation()
        value["approved"] = True
        with self.assertRaises(ValueError):
            self.validate(value)

    def test_producer_seals_original_raw_observations_before_context(self):
        original = anchor()
        files = {"/bridge.py": b"bridge source", "/launch_context.py": b"context source",
                 "/proc/sys/kernel/random/boot_id": BOOT.encode()}
        for name, filename in (("dev", "dev"), ("ipv4", "route"), ("ipv6", "ipv6_route")):
            files[f"/proc/{os.getpid()}/net/{filename}"] = RAW[name]
        writes = []
        def write(path, raw):
            writes.append((str(path), raw))
            return {"path": str(path), "sha256": context.digest(raw)}
        with patch.object(context, "read_bounded", side_effect=lambda path, **_: files[str(path)]), \
             patch.object(context, "process_identity", return_value=BRIDGE.copy()), \
             patch.object(context.os, "readlink", return_value="net:[2000]"), \
             patch.object(context.time, "time", return_value=1002.0), \
             patch.object(context.time, "monotonic", return_value=502.0), \
             patch.object(context, "write_original", side_effect=write):
            reference = context.observe(context.encode(original))
            self.assertEqual(len(writes), 4)
            self.assertEqual(reference["path"], writes[-1][0])
            captured = context.strict_json(writes[-1][1])
            self.assertEqual(captured["anchor"], original)
            self.assertEqual(captured["process"], BRIDGE)
            for name, ref in captured["captures"].items():
                self.assertIn((ref["path"], RAW[name]), writes[:-1])
            files["/bridge.py"] = b"changed producer"
            with self.assertRaisesRegex(ValueError, "producer_source"):
                context.observe(context.encode(original))
            self.assertEqual(len(writes), 4, "changed source must fail before any new observation")

    def test_parent_anchor_uses_original_ledger_clock_and_actual_paired_bytes(self):
        import launcher
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            runtime, source = root / "runtime", root / "source"
            (runtime / "opt/recall/bin").mkdir(parents=True)
            (runtime / "opt/recall/share").mkdir(parents=True)
            source.mkdir()
            (runtime / "opt/recall/bin/recall-surreal").write_bytes(b"paired binary")
            (source / "bridge.py").write_bytes(b"bridge source")
            (source / "launch_context.py").write_bytes(b"context source")
            manifest = {key: "a" * 64 for key in context.HASH_FIELDS}
            manifest.update(run_id="original-reviewed-run-001", bindings=[])
            package = {"format": "recall-gateway-runtime-provenance/v1", "status": "packaged_unqualified",
                       "artifact_source_commit": "b" * 40,
                       "binaries": {"recall": "a" * 64, "paired": context.digest(b"paired binary")}}
            (runtime / "opt/recall/share/runtime-package.json").write_bytes(context.encode(package))
            queries = []
            def query(sql):
                queries.append(sql)
                return SimpleNamespace(fetchone=lambda: (1000.0, 500.0, BOOT))
            ledger = SimpleNamespace(transaction=nullcontext, check_time=lambda: 5200.0,
                                     db=SimpleNamespace(execute=query))
            policy = SimpleNamespace(manifest=manifest, manifest_hash="a" * 64)
            with patch.object(context, "process_identity", return_value=BRIDGE.copy()), \
                 patch.object(launcher.os, "readlink", return_value="net:[1000]"):
                original = context.strict_json(launcher.original_launch_anchor(policy, ledger, runtime,
                    {"command": ["/usr/bin/python3", "-I", "/reviewed.py"]}, source, cohort=False))
            self.assertEqual(original["clock"], anchor()["clock"])
            self.assertEqual(original["paired_artifact_sha256"], context.digest(b"paired binary"))
            self.assertEqual(queries, ["SELECT wall_start,mono_start,boot FROM run"])

    def test_launcher_mounts_parent_anchor_and_helper_readonly(self):
        import launcher
        with patch.object(launcher.shutil, "which", return_value="/usr/bin/bwrap"), \
             patch.object(launcher.os, "readlink", return_value="net:[1000]"):
            command = launcher.launch_command(Path("/runtime"), Path("/owned/work"), Path("/owned/gateway.sock"),
                Path("/reviewed/bridge.py"), ["/usr/bin/python3", "-I", "/reviewed.py"], {}, Path("/owned/anchor.json"))
        for source, target in (("/owned/anchor.json", "/launch-anchor.json"),
                               ("/reviewed/launch_context.py", "/launch_context.py"),
                               ("/owned/manifest.json", "/gateway-manifest.json"),
                               ("/owned/launch.json", "/gateway-launch.json")):
            at = command.index(source)
            self.assertEqual(command[at - 1:at + 2], ["--ro-bind", source, target])
        self.assertIn("--unshare-all", command)
        self.assertIn("--clearenv", command)

    def test_saved_outer_launch_inputs_preserve_exact_bytes_and_refuse_replacements(self):
        import launcher
        from wire import Denied
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = b'{ "original": true }\n'
            spec = b'{ "command": ["/reviewed.py"] }\n'
            context.write_original(root / "manifest.json", manifest)
            context.write_original(root / "launch.json", spec)
            launcher.verify_original_launch_inputs(root, context.digest(manifest), spec)
            self.assertEqual((root / "manifest.json").read_bytes(), manifest)
            self.assertEqual((root / "launch.json").read_bytes(), spec)
            # Semantically equal but re-serialized files are not the originals.
            (root / "manifest.json").write_bytes(context.encode(context.strict_json(manifest)))
            with self.assertRaisesRegex(Denied, "run_anchor_mismatch"):
                launcher.verify_original_launch_inputs(root, context.digest(manifest), spec)
            (root / "manifest.json").write_bytes(manifest)
            (root / "launch.json").write_bytes(context.encode(context.strict_json(spec)))
            with self.assertRaisesRegex(Denied, "run_launch_mismatch"):
                launcher.verify_original_launch_inputs(root, context.digest(manifest), spec)
            (root / "launch.json").unlink()
            (root / "launch.json").symlink_to(root / "manifest.json")
            with self.assertRaises(OSError):
                launcher.verify_original_launch_inputs(root, context.digest(manifest), spec)
            (root / "launch.json").unlink()
            context.write_original(root / "launch.json", b"x" * 131072)
            (root / "launch.json").write_bytes(b"x" * 131073)
            with self.assertRaisesRegex(Denied, "unsafe_private_file"):
                launcher.verify_original_launch_inputs(root, context.digest(manifest), spec)

    def test_static_inputs_are_pinned_copied_and_mounted_separately(self):
        import launcher
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, destination = root / "approved", root / "sealed"
            source.mkdir(mode=0o700)
            (source / "nested").mkdir(mode=0o700)
            context.write_original(source / "nested/plan.json", b'{"runtime_sha256":"original"}')
            original = context.input_inventory(source)
            checksum = context.digest(context.encode(original))
            self.assertEqual(context.stage_inputs(source, destination, checksum), original)
            self.assertEqual(context.input_inventory(destination), original)
            self.assertEqual((destination / "nested/plan.json").stat().st_mode & 0o777, 0o400)
            self.assertEqual(destination.stat().st_mode & 0o777, 0o500)
            self.assertEqual(context.stage_inputs(source, destination, checksum), original)
            with patch.object(launcher.shutil, "which", return_value="/usr/bin/bwrap"), \
                 patch.object(launcher.os, "readlink", return_value="net:[1000]"):
                args = launcher.launch_command(Path("/runtime"), Path("/work-host"), Path("/socket"), Path("/bridge.py"),
                    ["/reviewed.py"], {}, Path("/anchor"), destination)
            at = args.index(str(destination))
            self.assertEqual(args[at - 1:at + 2], ["--ro-bind", str(destination), "/inputs"])
            # TemporaryDirectory cleanup needs original owner's write access;
            # this is test-owned cleanup, never mutation of production captures.
            destination.chmod(0o700)
            (destination / "nested").chmod(0o700)
            (source / "nested/plan.json").write_bytes(b"changed input")
            with self.assertRaisesRegex(ValueError, "original_inventory_mismatch"):
                context.stage_inputs(source, root / "replacement", checksum)
            self.assertFalse((root / "replacement").exists())

    def test_static_input_symlinks_public_permissions_and_bounds_refuse(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "plan.json"
            context.write_original(path, b"original")
            path.chmod(0o644)
            with self.assertRaises(ValueError):
                context.input_inventory(root)
            path.chmod(0o600)
            alias = root / "alias.json"
            alias.symlink_to(path)
            with self.assertRaises(ValueError):
                context.input_inventory(root)
            alias.unlink()
            with patch.object(context, "INPUT_FILE_CAP", 3), self.assertRaises(ValueError):
                context.input_inventory(root)
            with patch.object(context, "INPUT_TREE_CAP", 3), self.assertRaises(ValueError):
                context.input_inventory(root)
            with patch.object(context, "INPUT_COUNT_CAP", 0), self.assertRaises(ValueError):
                context.input_inventory(root)

    def test_changed_mounted_inputs_refuse_despite_unchanged_claimed_context(self):
        value = anchor()
        value["input_inventory_sha256"] = "c" * 64
        with patch.object(context, "input_inventory", return_value={"format": "recall-gateway-inputs/v1", "files": []}), \
             self.assertRaisesRegex(ValueError, "mounted_inventory_changed"):
            context.verify_inputs(value)

    def test_consumer_verifies_original_mount_raw_captures_and_live_bridge(self):
        value = observation()
        raw = context.encode(value)
        path = f'/work/gateway-context-{value["anchor_sha256"]}.json'
        files = {path: raw, "/launch-anchor.json": context.encode(value["anchor"]),
                 "/bridge.py": b"bridge source", "/launch_context.py": b"context source",
                 "/proc/sys/kernel/random/boot_id": BOOT.encode()}
        files.update({value["captures"][name]["path"]: data for name, data in RAW.items()})
        for name, filename in (("dev", "dev"), ("ipv4", "route"), ("ipv6", "ipv6_route")):
            files[f"/proc/{os.getpid()}/net/{filename}"] = RAW[name]
        with patch.object(context, "read_bounded", side_effect=lambda path, **_: files[str(path)]), \
             patch.object(context, "process_identity", return_value=BRIDGE.copy()), \
             patch.object(context.os, "getppid", return_value=2), \
             patch.object(context.os, "readlink", return_value="net:[2000]"), \
             patch.object(context.time, "time", return_value=1002.0), \
             patch.object(context.time, "monotonic", return_value=502.0):
            args = dict(expected={"run_nonce": value["anchor"]["run_nonce"]}, immediate_parent=True)
            self.assertEqual(context.read_context(path, context.digest(raw), **args), value)
            files[value["captures"]["dev"]["path"]] += b"eth0: 0\n"
            with self.assertRaisesRegex(ValueError, "raw_capture_changed"):
                context.read_context(path, context.digest(raw), **args)
            files[value["captures"]["dev"]["path"]] = RAW["dev"]
            with patch.object(context, "process_identity", return_value={**BRIDGE, "start_ticks": 91}), self.assertRaisesRegex(ValueError, "bridge_replaced"):
                context.read_context(path, context.digest(raw), **args)


if __name__ == "__main__":
    unittest.main()
