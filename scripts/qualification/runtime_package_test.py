"""Offline controls only: virtual package bytes and mocked ELF inspection.

No candidate executable, package manager, gateway, socket or service is started.
"""
from __future__ import annotations

import io
from pathlib import Path
import stat
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import prepare_gateway_runtime as package
import runtime_package_io as files


class Budget:
    def __init__(self):
        self.checks = 0
    def check(self):
        self.checks += 1
        return {"mock_budget": True}


def archive(entries):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as tar:
        for name, kind, content in entries:
            info = tarfile.TarInfo(name)
            info.mode = 0o755 if kind == "executable" else 0o644
            if kind in ("file", "executable", "setuid"):
                info.size = len(content)
                if kind == "setuid":
                    info.mode |= stat.S_ISUID
                tar.addfile(info, io.BytesIO(content))
            elif kind == "directory":
                info.type, info.mode = tarfile.DIRTYPE, 0o755
                tar.addfile(info)
            else:
                info.type = {"link": tarfile.SYMTYPE, "hardlink": tarfile.LNKTYPE,
                             "fifo": tarfile.FIFOTYPE}[kind]
                info.linkname = content
                tar.addfile(info)
    stream.seek(0)
    return stream


class Controls(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.budget = Budget()
        self.limits = files.Limits(60, self.budget)
        self.pool = files.PackagePool(self.root / "pool", self.limits)
        self.output = files.RuntimeTree(self.root / "runtime", self.limits)
        self.process_guard = mock.patch("subprocess.Popen", side_effect=AssertionError("process launch forbidden"))
        self.process_guard.start()

    def tearDown(self):
        self.process_guard.stop()
        package.clean_tree(self.root / "runtime")
        self.temporary.cleanup()

    def test_virtual_links_materialize_only_supplied_regular_bytes(self):
        self.pool.read_tar(archive([
            ("./usr/bin/python3.11", "executable", b"reviewed-interpreter"),
            ("./usr/bin/python3", "link", "python3.11"),
            ("./bin/python3", "link", "/usr/bin/python3"),
            ("./usr/bin/second", "hardlink", "./usr/bin/python3.11"),
        ]), "python-fixture")
        for name in ("usr/bin/python3", "bin/python3", "usr/bin/second"):
            self.pool.materialize(name, name, self.output)
            path = self.output.root / name
            self.assertFalse(path.is_symlink())
            self.assertEqual(path.read_bytes(), b"reviewed-interpreter")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o555)
        self.assertEqual(self.limits.output_bytes, 3 * len(b"reviewed-interpreter"))

    def test_directory_alias_is_copied_without_a_symlink(self):
        self.pool.read_tar(archive([
            ("usr/lib/value", "file", b"value"), ("lib", "link", "usr/lib")]), "library")
        self.pool.materialize("lib", "lib", self.output)
        self.assertEqual((self.output.root / "lib/value").read_bytes(), b"value")
        self.assertFalse((self.output.root / "lib").is_symlink())

    def test_escape_special_and_setid_package_entries_are_refused(self):
        for entries in [[("../escape", "file", b"x")], [("/absolute", "file", b"x")],
                        [("bad", "fifo", "")], [("bad", "setuid", b"x")],
                        [("usr/link", "link", "../../outside")]]:
            with self.subTest(entries=entries), self.assertRaises(ValueError):
                self.pool.read_tar(archive(entries), "invalid")
        self.assertFalse((self.root / "escape").exists())

    def test_missing_target_never_borrows_host_files(self):
        self.pool.read_tar(archive([("usr/bin/host", "link", "/usr/bin/true")]), "missing")
        with self.assertRaises(ValueError):
            self.pool.materialize("usr/bin/host", "usr/bin/host", self.output)
        self.assertFalse((self.output.root / "usr/bin/host").exists())

    def test_link_cycle_and_ancestor_directory_cycle_are_refused(self):
        self.pool.read_tar(archive([("a", "link", "b"), ("b", "link", "a")]), "cycle")
        with self.assertRaises(ValueError):
            self.pool.materialize("a", "a", self.output)
        self.pool.read_tar(archive([("usr/lib/value", "file", b"x"),
                                    ("usr/lib/cycle", "link", "/usr/lib")]), "directory-cycle")
        with self.assertRaises(ValueError):
            self.pool.materialize("usr/lib", "usr/lib", self.output)

    def test_different_package_payload_collision_is_refused(self):
        self.pool.read_tar(archive([("usr/lib/same", "file", b"one")]), "one")
        with self.assertRaises(ValueError):
            self.pool.read_tar(archive([("usr/lib/same", "file", b"two")]), "two")

    def test_identical_package_payload_can_be_shared(self):
        for identity in ("one", "two"):
            self.pool.read_tar(archive([("usr/lib/same", "file", b"one")]), identity)
        self.pool.materialize("usr/lib/same", "usr/lib/same", self.output)
        self.assertEqual((self.output.root / "usr/lib/same").read_bytes(), b"one")

    def test_copy_refuses_truncation_and_wrong_identity(self):
        for index, (raw, size, checksum) in enumerate([(b"x", 2, None), (b"wrong", 5, "0" * 64)]):
            with self.assertRaises(ValueError):
                files.copy_stream(io.BytesIO(raw), self.root / str(index), size, checksum, self.limits)

    def test_size_entry_deadline_and_original_budget_are_checked(self):
        for kwargs in ({"pool": files.MAX_POOL + 1}, {"output": files.MAX_TREE + 1},
                       {"entries": files.MAX_ENTRIES + 1}):
            with self.assertRaises(ValueError):
                files.Limits(60, self.budget).check(**kwargs)
        self.limits.deadline = 0
        with self.assertRaises(ValueError):
            self.limits.check()
        budget = Budget()
        with mock.patch.object(budget, "check", side_effect=ValueError("original allocation exhausted")):
            with self.assertRaises(ValueError):
                files.Limits(60, budget)

    def test_output_entry_limit_includes_all_directories(self):
        with mock.patch.object(files, "MAX_ENTRIES", 2):
            with self.assertRaises(ValueError):
                self.output.file("a/b/c", io.BytesIO(b"x"), 1, None, False, {})
        self.assertFalse((self.output.root / "a/b/c").exists())

    def test_final_tree_is_read_only_and_never_replaces_a_file(self):
        self.output.file("usr/lib/value", io.BytesIO(b"value"), 5, None, False, {})
        with self.assertRaises(ValueError):
            self.output.file("usr/lib/value", io.BytesIO(b"new"), 3, None, False, {})
        self.output.seal()
        for path in (self.output.root, *self.output.root.rglob("*")):
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o555 if path.is_dir() else 0o444)
            self.assertFalse(path.is_symlink())

    def test_input_hash_mode_and_symlink_custody(self):
        path = self.root / "input"
        path.write_bytes(b"input")
        path.chmod(0o600)
        spec = {"path": str(path), "bytes": 5, "sha256": files.digest(path)}
        self.assertEqual(files.checked_file(spec), path)
        for change in ({"sha256": "0" * 64}, {"bytes": 4}):
            with self.assertRaises(ValueError):
                files.checked_file({**spec, **change})
        path.chmod(0o666)
        with self.assertRaises(ValueError):
            files.checked_file(spec)
        path.chmod(0o600)
        link = self.root / "link"
        link.symlink_to(path)
        with self.assertRaises(ValueError):
            files.checked_file({**spec, "path": str(link)})

    def test_json_rejects_ambiguity_overflow_and_changed_reviewed_bytes(self):
        path = self.root / "manifest.json"
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":1e400}'):
            path.write_bytes(raw)
            with self.assertRaises(ValueError):
                package.read_json(path)
        path.write_text('{"a":1}')
        with self.assertRaises(ValueError):
            package.read_json(path, expected="0" * 64)
        with self.assertRaises(ValueError):
            package.read_json(path, maximum=3)

    def test_launch_contract_rejects_ambient_credentials_and_unreviewed_driver(self):
        driver = "opt/recall/scripts/qualification/primary_provider_driver.py"
        inventory = {driver: {}, "opt/recall/share/native/skills/example/SKILL.md": {}}
        spec = {"format": "recall-gateway-launch/v1",
                "command": ["/usr/bin/python3", "-I", "/" + driver],
                "environment": {"PIPELINE_SKILLS_DIR": "/opt/recall/share/native/skills"}, "bindings": []}
        package.validate_launch(spec, inventory)
        for key in ("JWT_SECRET", "OPENAI_API_KEY", "PYTHONPATH", "SKIP_AUTH", "HOME"):
            with self.assertRaises(ValueError):
                package.validate_launch({**spec, "environment": {key: "ambient"}}, inventory)
        for command in (["/bin/sh", "-c", "anything"], ["/usr/bin/python3", "/" + driver],
                        ["/usr/bin/python3", "-I", "/work/unreviewed.py"]):
            with self.assertRaises(ValueError):
                package.validate_launch({**spec, "command": command}, inventory)
        with self.assertRaises(ValueError):
            package.validate_launch({**spec, "environment": {"PIPELINE_SKILLS_DIR": "/work/skills"}}, inventory)

    def test_future_launch_is_a_separate_phase_and_not_a_runtime_input(self):
        manifest = self.root / "inputs.json"
        manifest.write_bytes(files.canonical_json({"format": "recall-gateway-runtime-inputs/v2",
            "artifact_source_commit": "a" * 40, "recall_bundle": {}, "paired_bundle": {},
            "packages": [], "package_paths": [], "harness_files": [],
            "launch_spec": {"path": "/future/launch.json", "sha256": "b" * 64}}))
        with self.assertRaisesRegex(ValueError, "contract"):
            package.prepare(package.HERE.parents[2], manifest, self.root / "unallocated")
        self.assertFalse((self.root / "unallocated").exists())

    def test_packager_passes_the_explicit_allocation_before_materializing_inputs(self):
        class ReachedBudget(Exception):
            pass

        manifest = self.root / "inputs.json"
        manifest.write_bytes(files.canonical_json({"format": "recall-gateway-runtime-inputs/v2",
            "artifact_source_commit": "a" * 40, "recall_bundle": {"root": str(self.root)},
            "paired_bundle": {"root": str(self.root)},
            "packages": [{"package": "python3.11-minimal", "version": "3.11.0",
                          "file": {"path": str(self.root / "package.deb")}}],
            "package_paths": ["usr/bin/python3.11"], "harness_files": ["fixture"]}))
        destination = self.root / "unallocated"
        for gib in (150, 200, 300, 400, 600, 800):
            constructor = mock.Mock(side_effect=ReachedBudget)
            limits = mock.Mock(return_value=(8, 250 * files.GIB, gib * files.GIB))
            release = SimpleNamespace(ResourceBudget=constructor, build_limits=limits)
            with self.subTest(gib=gib), \
                 mock.patch.object(package, "helpers", return_value=(release, None)), \
                 mock.patch.object(package, "original_budget_receipt", return_value=self.root / "original.json"), \
                 mock.patch.object(package, "bundle_root", return_value=self.root), \
                 mock.patch.dict(package.os.environ, {"RECALL_ARTIFACT_DOCKER_ROOT": str(self.root)}):
                with self.assertRaises(ReachedBudget):
                    package.prepare(package.HERE.parents[2], manifest, destination)
            limits.assert_called_once_with()
            self.assertEqual(constructor.call_args.kwargs, {"allocation": gib * files.GIB})
            self.assertEqual(constructor.call_args.args[0], self.root / "original.json")
            self.assertFalse(destination.exists())

    def test_all_readonly_gateway_mount_targets_exist_before_tree_seal(self):
        package.mount_targets(self.output)
        self.output.seal()
        for name in ("inputs", "work", "proc", "dev", "tmp", "home"):
            path = self.output.root / name
            self.assertTrue(path.is_dir())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o555)
        for name in ("bridge.py", "launch_context.py", "launch-anchor.json", "gateway.sock"):
            path = self.output.root / name
            self.assertTrue(path.is_file())
            self.assertEqual(path.read_bytes(), b"")
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o444)

    def test_later_launch_accepts_only_explicit_hashed_input_tree_reference(self):
        driver = "opt/recall/scripts/qualification/runtime_package_bootstrap.py"
        spec = {"format": "recall-gateway-launch/v1", "command": ["/usr/bin/python3", "-I", "/" + driver],
                "environment": {}, "bindings": [], "inputs": {"directory": str(self.root), "inventory_sha256": "a" * 64}}
        package.validate_launch(spec, {driver: {}})
        for inputs in ({"directory": "relative", "inventory_sha256": "a" * 64},
                       {"directory": str(self.root), "inventory_sha256": "invalid"},
                       {"directory": str(self.root), "inventory_sha256": "a" * 64, "approved": True}):
            with self.assertRaises(ValueError):
                package.validate_launch({**spec, "inputs": inputs}, {driver: {}})

    def test_original_budget_receipt_cannot_be_created_or_aliased(self):
        path = self.root / "original-budget.json"
        with self.assertRaises(ValueError):
            package.original_budget_receipt(str(path))
        self.assertFalse(path.exists())
        path.write_text('{"remaining":123}')
        path.chmod(0o600)
        self.assertEqual(package.original_budget_receipt(str(path)), path)
        link = self.root / "budget-alias"
        link.symlink_to(path)
        for value in (str(link), "relative-budget", str(self.root), ""):
            with self.subTest(value=value), self.assertRaises(ValueError):
                package.original_budget_receipt(value)
        path.chmod(0o666)
        with self.assertRaises(ValueError):
            package.original_budget_receipt(str(path))

    def launch_budget(self, minimum):
        device = str(self.root.stat().st_dev)
        return {"format": "meerkat-recall-artifact-allocation/v1", "checks": 17,
                "consumed_bytes": 123456, "minimum_free_bytes": minimum,
                "limit_bytes": 600 * files.GIB, "refused": False,
                "filesystems": {device: {"path": str(self.root), "total": 10**12,
                                         "used": 500_000_000_000, "free": 500_000_000_000}}}

    def test_static_launch_honors_decimal_and_stricter_retained_floor_without_writing_ledger(self):
        class ReachedHelpers(Exception):
            pass

        ledger = self.root / "original-budget.json"
        receipt = self.root / "launch-check.json"
        for minimum, available, accepted in (
                (250 * files.GIB, 250 * files.GIB, True),
                (300_000_000_000, 300_000_000_000 - 1, False),
                (300_000_000_000, 300_000_000_000, True),
                (350_000_000_000, 350_000_000_000 - 1, False),
                (350_000_000_000, 350_000_000_000, True)):
            with self.subTest(minimum=minimum, available=available):
                state = self.launch_budget(minimum)
                if minimum == 250 * files.GIB:
                    state["limit_bytes"] = 400 * files.GIB
                original = files.canonical_json(state)
                ledger.write_bytes(original)
                ledger.chmod(0o600)
                with mock.patch.dict(package.os.environ, {"RECALL_ARTIFACT_BUDGET_FILE": str(ledger)}), \
                     mock.patch.object(package, "HERE", self.root / "scripts/qualification/prepare_gateway_runtime.py"), \
                     mock.patch.object(package.shutil, "disk_usage", return_value=SimpleNamespace(total=10**12, free=available)), \
                     mock.patch.object(package, "helpers", side_effect=ReachedHelpers) as helpers:
                    if accepted:
                        with self.assertRaises(ReachedHelpers):
                            package.validate_packaged_launch(self.root, self.output.root,
                                                             self.root / "launch.json", receipt)
                        helpers.assert_called_once_with(self.root)
                    else:
                        self.assertGreater(available, 250 * files.GIB)
                        with self.assertRaisesRegex(ValueError, "retained free-space floor"):
                            package.validate_packaged_launch(self.root, self.output.root,
                                                             self.root / "launch.json", receipt)
                        helpers.assert_not_called()
                self.assertEqual(ledger.read_bytes(), original)
                self.assertFalse(receipt.exists())

    def test_static_launch_refuses_missing_invalid_refused_or_foreign_allocation(self):
        ledger = self.root / "original-budget.json"
        receipt = self.root / "launch-check.json"
        valid = self.launch_budget(300_000_000_000)
        invalid = [None, [], {}, {**valid, "format": "foreign"},
                   {**valid, "minimum_free_bytes": True},
                   {**valid, "minimum_free_bytes": 300_000_000_000.0},
                   {**valid, "minimum_free_bytes": 250 * files.GIB - 1},
                   {**valid, "checks": -1}, {**valid, "consumed_bytes": True},
                   {**valid, "consumed_bytes": valid["limit_bytes"]},
                   {**valid, "refused": True}, {**valid, "filesystems": {}},
                   {**valid, "filesystems": {"foreign-device": {"total": 10**12}}},
                   {**valid, "filesystems": {str(self.root.stat().st_dev): {"total": 10**12 + 1}}}]
        for state in invalid:
            with self.subTest(state=state):
                if ledger.exists():
                    ledger.unlink()
                original = None if state is None else files.canonical_json(state)
                if original is not None:
                    ledger.write_bytes(original)
                    ledger.chmod(0o600)
                with mock.patch.dict(package.os.environ, {"RECALL_ARTIFACT_BUDGET_FILE": str(ledger)}), \
                     mock.patch.object(package, "HERE", self.root / "scripts/qualification/prepare_gateway_runtime.py"), \
                     mock.patch.object(package.shutil, "disk_usage", return_value=SimpleNamespace(total=10**12, free=500_000_000_000)), \
                     mock.patch.object(package, "helpers") as helpers:
                    with self.assertRaises(ValueError):
                        package.validate_packaged_launch(self.root, self.output.root,
                                                         self.root / "launch.json", receipt)
                    helpers.assert_not_called()
                self.assertEqual(ledger.read_bytes() if ledger.exists() else None, original)
                self.assertFalse(receipt.exists())

    def test_bundle_copy_uses_frozen_identity_and_refuses_missing_or_added_files(self):
        bundle = self.root / "bundle"
        bundle.mkdir()
        binary = bundle / "binary"
        binary.write_bytes(b"reviewed")
        binary.chmod(0o600)
        frozen = {"binary": {"bytes": 8, "sha256": files.digest(binary)}}
        binary.write_bytes(b"modified")
        with self.assertRaisesRegex(ValueError, "reviewed identity"):
            package.copy_bundle(bundle, self.output, "share", "binary", "bin/candidate",
                                self.limits, frozen)
        # No digest taken after the mutation may become the expected identity.
        binary.unlink()
        with self.assertRaisesRegex(ValueError, "lost a reviewed file"):
            package.copy_bundle(bundle, self.output, "share", "binary", "bin/candidate",
                                self.limits, frozen)
        (bundle / "surprise").write_bytes(b"unreviewed")
        with self.assertRaisesRegex(ValueError, "unreviewed file"):
            package.copy_bundle(bundle, self.output, "share", "binary", "bin/candidate",
                                self.limits, frozen)

    def test_inspection_timeout_retains_leader_until_timer_join_and_group_cleanup(self):
        events = []
        raw = SimpleNamespace(pid=7654321, stdout=io.BytesIO(b"inspection"))
        raw.wait = mock.Mock(side_effect=lambda **_: events.append("reap") or 0)
        class Timer:
            def __init__(self, delay, callback):
                self.callback, self.ident = callback, None
            def start(self):
                self.ident = 1
                events.append("timer-start")
            def cancel(self):
                events.append("timer-cancel")
            def join(self):
                # A callback already running when canceled can still signal.
                self.callback()
                events.append("timer-joined")
        exited = SimpleNamespace(si_status=0, si_code=files.os.CLD_EXITED)
        with mock.patch.object(files.subprocess, "Popen", return_value=raw), \
             mock.patch.object(files.threading, "Timer", Timer), \
             mock.patch.object(files.os, "waitid", return_value=exited) as observe, \
             mock.patch.object(files.os, "killpg", side_effect=lambda *_: events.append("kill")):
            with files.inspected_process(["/usr/bin/readelf", "--version"], self.limits) as child:
                self.assertEqual(child.stdout.read(), b"inspection")
            observe.assert_called_once_with(files.os.P_PID, raw.pid,
                files.os.WEXITED | files.os.WNOHANG | files.os.WNOWAIT)
            self.assertEqual(events, ["timer-start", "timer-cancel", "kill", "timer-joined", "kill", "reap"])
            child.kill()
            self.assertEqual(events[-1], "reap")
            self.assertEqual(events.count("kill"), 2, "a reaped PID must never be signaled")
            raw.wait.assert_called_once_with(timeout=5)
            self.assertTrue(raw.stdout.closed)

    def test_inspection_body_failure_still_joins_timer_before_reap(self):
        events = []
        raw = SimpleNamespace(pid=7654322, stdout=io.BytesIO())
        raw.wait = mock.Mock(side_effect=lambda **_: events.append("reap") or 0)
        timer = mock.Mock(ident=1)
        timer.join.side_effect = lambda: events.append("joined")
        with mock.patch.object(files.subprocess, "Popen", return_value=raw), \
             mock.patch.object(files.threading, "Timer", return_value=timer), \
             mock.patch.object(files.os, "killpg", side_effect=lambda *_: events.append("kill")), \
             self.assertRaisesRegex(ValueError, "parse failure"):
            with files.inspected_process(["/usr/bin/git", "archive", "HEAD"], self.limits):
                raise ValueError("parse failure")
        self.assertEqual(events, ["joined", "kill", "reap"])
        self.assertTrue(raw.stdout.closed)

    def test_source_change_before_publication_is_refused(self):
        (self.root / "Cargo.lock").write_bytes(b"lock")
        source = {"source_commit": "a" * 40, "cargo_lock_sha256": files.digest(self.root / "Cargo.lock")}
        with mock.patch.object(package, "tool_text", side_effect=["a" * 40, ""]):
            package.verify_source_unchanged(self.root, source, self.limits)
        for answers in (["b" * 40, ""], ["a" * 40, " M changed.py"]):
            with mock.patch.object(package, "tool_text", side_effect=answers), self.assertRaises(ValueError):
                package.verify_source_unchanged(self.root, source, self.limits)
        (self.root / "Cargo.lock").write_bytes(b"mutated")
        with mock.patch.object(package, "tool_text", side_effect=["a" * 40, ""]), self.assertRaises(ValueError):
            package.verify_source_unchanged(self.root, source, self.limits)

    def test_package_control_refuses_duplicate_identity_fields(self):
        self.assertEqual(files.package_control("Package: python3.11-minimal\nVersion: 3.11.2\nArchitecture: amd64\n")["Architecture"], "amd64")
        with self.assertRaises(ValueError):
            files.package_control("Package: one\nPackage: two\n")

    def python_runtime_fixture(self):
        elf = b"\x7fELFstatic-inspection-fixture"
        required = ("usr/bin/python3", "opt/recall/bin/elephant", "opt/recall/bin/recall-surreal",
                    "lib64/ld-linux-x86-64.so.2", "lib/x86_64-linux-gnu/libc.so.6")
        for name in required:
            self.output.file(name, io.BytesIO(elf), len(elf), None, True, {})
        for name in ("encodings/__init__.py", "ssl.py", "json/__init__.py", "urllib/request.py"):
            self.output.file("usr/lib/python3.11/" + name, io.BytesIO(b"pass"), 4, None, False, {})
        for name in ("_ssl", "_hashlib"):
            self.output.file("usr/lib/python3.11/lib-dynload/" + name + ".cpython-311-x86_64-linux-gnu.so",
                             io.BytesIO(elf), len(elf), None, False, {})
        header = "Class: ELF64\nMachine: Advanced Micro Devices X86-64\n"
        state = {"dynamic": "(NEEDED) Shared library: [libc.so.6]", "versions": "Name: GLIBC_2.36",
                 "symbols": "\n".join("  %d: 0000000000410000 123 FUNC GLOBAL DEFAULT 14 PyInit_%s" % (i, module)
                                      for i, module in enumerate(("_socket", "select", "_random", "_struct"), 1))}
        def inspect(command, _limits):
            return {"--file-header": header, "--dynamic": state["dynamic"],
                    "--program-headers": "[Requesting program interpreter: /lib64/ld-linux-x86-64.so.2]",
                    "--version-info": state["versions"], "--dyn-syms": state["symbols"]}[command[2]]
        return state, inspect

    def test_elf_inspection_is_static_and_missing_unsafe_closure_is_refused(self):
        state, inspect = self.python_runtime_fixture()
        with mock.patch.object(package, "tool_text", side_effect=inspect):
            observed = package.inspect_dependencies(self.output, self.limits)
            self.assertEqual(observed["usr/bin/python3"]["needed"]["libc.so.6"], "lib/x86_64-linux-gnu/libc.so.6")
            modules = observed["usr/bin/python3"]["python_modules"]
            self.assertEqual(modules["builtin_initializers"], {"_socket": "PyInit__socket", "select": "PyInit_select",
                                                             "_random": "PyInit__random", "_struct": "PyInit__struct"})
            self.assertEqual(set(modules["dynamic_extensions"]), {"_ssl", "_hashlib"})
            for dynamic in ("(NEEDED) Shared library: [missing.so]", "(RUNPATH) Library runpath: [/host]"):
                state["dynamic"] = dynamic
                with self.assertRaises(ValueError):
                    package.inspect_dependencies(self.output, self.limits)
            state["dynamic"], state["versions"] = "", "Name: GLIBC_2.39"
            with self.assertRaises(ValueError):
                package.inspect_dependencies(self.output, self.limits)
        self.assertGreater(self.budget.checks, 0)

    def test_builtin_initializers_require_defined_global_functions_in_interpreter(self):
        state, inspect = self.python_runtime_fixture()
        original = state["symbols"]
        with mock.patch.object(package, "tool_text", side_effect=inspect):
            for module in ("_socket", "select", "_random", "_struct"):
                lines = original.splitlines()
                index = next(i for i, line in enumerate(lines) if line.endswith("PyInit_" + module))
                for replacement in ("", lines[index].replace("DEFAULT 14", "DEFAULT UND"),
                                    lines[index].replace("FUNC", "OBJECT"),
                                    lines[index].replace("GLOBAL", "LOCAL"), lines[index] + "_unrelated"):
                    with self.subTest(module=module, replacement=replacement):
                        changed = lines.copy()
                        changed[index] = replacement
                        state["symbols"] = "\n".join(changed)
                        with self.assertRaisesRegex(ValueError, "built-in module"):
                            package.inspect_dependencies(self.output, self.limits)

    def test_openssl_extensions_require_inspected_elf_at_actual_python_import_paths(self):
        _state, inspect = self.python_runtime_fixture()
        with mock.patch.object(package, "tool_text", side_effect=inspect):
            for module in ("_ssl", "_hashlib"):
                name = "usr/lib/python3.11/lib-dynload/" + module + ".cpython-311-x86_64-linux-gnu.so"
                entry = self.output.inventory.pop(name)
                with self.subTest(module=module, defect="missing"):
                    with self.assertRaisesRegex(ValueError, "extension module"):
                        package.inspect_dependencies(self.output, self.limits)
                wrong_path = "opt/recall/" + Path(name).name
                self.output.file(wrong_path, io.BytesIO(b"\x7fELFdecoy"), 9, None, False, {})
                with self.subTest(module=module, defect="outside import path"):
                    with self.assertRaisesRegex(ValueError, "extension module"):
                        package.inspect_dependencies(self.output, self.limits)
                self.output.inventory[name] = entry
                path = self.output.root / name
                original = path.read_bytes()
                path.chmod(0o644)
                path.write_bytes(b"not-ELF")
                with self.subTest(module=module, defect="not ELF"):
                    with self.assertRaisesRegex(ValueError, "extension module"):
                        package.inspect_dependencies(self.output, self.limits)
                path.write_bytes(original)
                path.chmod(0o444)


if __name__ == "__main__":
    unittest.main()
