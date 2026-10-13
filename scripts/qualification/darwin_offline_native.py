#!/usr/bin/env python3
"""Reviewed, bounded native Apple Silicon acceptance; the default plan is inert.

Reuse the installed shared case implementation unchanged. Darwin supplies only
process custody, a restrictive Seatbelt profile and measured resource controls.
An unavailable isolation prerequisite is UNMET, never evidence of acceptance.
"""
from __future__ import annotations

import argparse
import ctypes
import datetime as dt
import errno
import importlib.util
import json
import math
import os
from pathlib import Path
import platform
import re
import select
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
CASES = ROOT / "tests/recall_preview_semantics/darwin_offline_cases.json"
CHILD = ROOT / "scripts/qualification/darwin_offline_child.py"
DEPENDENCIES = (
    "scripts/qualification/darwin_offline_child.py",
    "scripts/qualification/installed_linux_offline.py",
    "scripts/qualification/recall_preview.py", "scripts/release/artifact.py",
    "tests/recall_preview_semantics/installed_linux_cases.json",
    "tests/recall_preview/fixtures.py", "tests/recall_preview/corpus.json",
    "tests/recall_preview/matrix.py", "tests/recall_preview/oracle.py",
)
spec = importlib.util.spec_from_file_location("darwin_shared_installed", ROOT / DEPENDENCIES[1])
if spec is None or spec.loader is None:
    raise ValueError("reviewed shared case implementation is absent")
core = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = core
spec.loader.exec_module(core)
require = core.require


class UnmetPrerequisite(Exception):
    """A required platform observation was not obtained."""


def plan():
    shared = core.plan()
    cases = core.read_json(CASES)
    require(cases["shared_cases"] == "tests/recall_preview_semantics/installed_linux_cases.json",
            "shared case identity changed")
    # Preserve the original case oracle including its explicit remaining gates.
    # The additional platform observation never rewrites Linux or semantic claims.
    return {"format": "recall-darwin-native-plan/v1", "product_acceptance": False,
            "execution": "not allocated by a plan", "expectations": shared["expectations"],
            "platform_expectations": cases, "shared_plan": shared,
            "platform_expectations_sha256": core.file_digest(CASES),
            "driver_sha256": core.file_digest(Path(__file__)),
            "dependency_sha256": {name: core.file_digest(ROOT / name) for name in DEPENDENCIES}}


def validate_allocation(path, now=None):
    manifest = core.read_json(Path(path))
    require(manifest.get("format") == "recall-darwin-native-run/v1"
            and manifest.get("execution_authorized") is True, "Darwin execution is not allocated")
    require((now or dt.datetime.now(dt.timezone.utc)) < core.utc(manifest["expires_at"]),
            "execution allocation expired")
    require(manifest.get("backend") in ("elephant-db", "surreal-db"), "explicit backend required")
    for name, length in (("source_commit", 40), ("archive_sha256", 64), ("binary_sha256", 64),
                         ("review_sha256", 64), ("python_sha256", 64), ("compatibility_sha256", 64)):
        require(isinstance(manifest.get(name), str) and
                re.fullmatch("[0-9a-f]{" + str(length) + "}", manifest[name]), "exact " + name + " required")
    for name in ("archive", "run_directory", "review", "python", "compatibility"):
        require(Path(manifest[name]).is_absolute(), name + " must be absolute")
    review_path = Path(manifest["review"])
    require(core.file_digest(review_path) == manifest["review_sha256"], "independent review digest differs")
    review, inventory = core.read_json(review_path), plan()
    require(review.get("format") == "recall-darwin-native-review/v1" and
            review.get("approved") is True and review.get("reviewer") == "qualification",
            "independent Darwin review is absent")
    for name in ("platform_expectations_sha256", "driver_sha256", "dependency_sha256"):
        require(review.get(name) == inventory[name], "reviewed " + name + " differs")
    return manifest, inventory


def version(value):
    require(isinstance(value, str) and re.fullmatch(r"\d+(?:\.\d+){1,2}", value), "invalid OS version")
    fields = tuple(map(int, value.split(".")))
    return fields + (0,) * (3 - len(fields))


def host_prerequisites(manifest):
    if sys.platform != "darwin" or platform.machine() != "arm64":
        raise UnmetPrerequisite("native macOS arm64 is required")
    if sys.version_info < (3, 12) or not hasattr(select, "kqueue"):
        raise UnmetPrerequisite("Python 3.12+ with native kqueue is required")
    if not Path("/usr/bin/sandbox-exec").is_file():
        raise UnmetPrerequisite("Seatbelt launcher is unavailable")
    interpreter = Path(manifest["python"])
    require(interpreter == interpreter.resolve() == Path(sys.executable).resolve(),
            "allocated interpreter is not the current canonical interpreter")
    require(core.file_digest(interpreter) == manifest["python_sha256"], "interpreter identity differs")
    compatibility_path = Path(manifest["compatibility"])
    require(core.file_digest(compatibility_path) == manifest["compatibility_sha256"],
            "trusted compatibility receipt differs")
    receipt = core.read_json(compatibility_path)
    require(receipt.get("format") == "recall-darwin-compatibility/v1", "missing compatibility evidence")
    for key in ("source_commit", "archive_sha256", "binary_sha256"):
        require(receipt.get(key) == manifest[key], "compatibility " + key + " differs")
    require(receipt.get("architecture") == "arm64", "compatibility architecture differs")
    actual = platform.mac_ver()[0]
    require(actual == manifest.get("host_os_version") == receipt.get("receiver_os_version"),
            "allocated macOS version differs")
    require(version(receipt["binary_min_os"]) <= version(actual) and
            version(receipt["onnx_min_os"]) <= version(actual) and
            version(receipt["binary_min_os"]) <= version("26.5") and
            version(receipt["onnx_min_os"]) <= version("26.5"),
            "artifact exceeds the receiving allocation's macOS minimum ceiling")
    for name in ("binary_load_commands_sha256", "onnx_archive_sha256", "onnx_load_commands_sha256"):
        require(re.fullmatch("[0-9a-f]{64}", receipt.get(name, "")), "missing " + name + " evidence")
    return {"platform": "darwin", "architecture": "arm64", "macos": actual,
            "compatibility": receipt, "python_sha256": manifest["python_sha256"],
            "sandbox": "Seatbelt; actual denial preflight required"}


def archive_bounds(manifest, limits):
    """Bound declared expansion before shared verification creates any files."""
    archive = Path(manifest["archive"])
    require(archive.is_file() and not archive.is_symlink() and archive.stat().st_size <= limits["archive_bytes"],
            "archive exceeds the supplied input bound")
    require(core.file_digest(archive) == manifest["archive_sha256"], "trusted archive digest differs")
    entries, size = 0, 0
    with core.capture.total_deadline(30):
        with tarfile.open(archive, "r|gz") as stream:
            for member in stream:
                entries += 1
                size += member.size
                require(entries <= limits["filesystem_entries"] and
                        0 <= member.size <= limits["archive_expanded_bytes"] and
                        size <= limits["archive_expanded_bytes"], "archive expansion exceeds allocation")
    return {"archive_entries": entries, "declared_expanded_bytes": size}


def quoted(value):
    value = str(value)
    require(not any(ord(character) < 32 for character in value), "control character in sandbox path")
    return json.dumps(value, ensure_ascii=True)


def sandbox_profile(root, port, executable, *, interpreter=None):
    require(type(port) is int and 1 <= port <= 65535, "invalid owned endpoint")
    root = Path(root)
    mutable = [root / name for name in ("private", "service", "backups", "media", "recovery", "tmp")]
    readable = [Path("/System/Library"), Path("/usr/lib"), Path("/Library/Apple"),
                Path("/usr/share/zoneinfo"), Path("/private/var/db/dyld"), root / "installed", *mutable]
    literals = [Path("/dev/null"), Path("/dev/urandom"), Path("/dev/random"),
                Path("/private/etc/localtime"), Path(executable)]
    if interpreter is not None:
        # The preflight uses a separate explicit interpreter/runtime allowance.
        # This never expands the candidate's filesystem policy.
        readable += [Path(sys.base_prefix).resolve() / "lib"]
        literals += [Path(interpreter), CHILD.resolve()]
    def paths(values, kind="subpath"):
        return " ".join("(" + kind + " " + quoted(path) + ")" for path in values)
    ancestors = sorted({parent for path in [*readable, *literals] for parent in path.parents}, key=str)
    return "\n".join([
        "(version 1)", "(deny default)",
        "(allow process-exec (literal " + quoted(executable) + "))",
        "(allow signal (target same-sandbox))",
        "(allow process-info* (target same-sandbox))",
        "(allow sysctl-read)",
        "(allow file-read-metadata " + paths(ancestors, "literal") + ")",
        "(allow file-read* " + paths(readable) + " " + paths(literals, "literal") + ")",
        "(allow file-map-executable " + paths(readable) + " (literal " + quoted(executable) + "))",
        "(allow file-write* " + paths(mutable) + " (literal \"/dev/null\"))",
        "(allow network-bind network-inbound (local ip \"localhost:" + str(port) + "\"))",
        # No process-fork, outbound network, Mach services, home, keychain,
        # installed-tree writes, evidence writes or ambient filesystem extension.
        "",
    ])


class DarwinProcess(core.OwnedProcess):
    """Retain the original session leader until every group signal is finished.

    XNU NOTE_EXIT is edge triggered: register BEFORE releasing a waiting trusted
    child. Popen.poll/wait must never run before final locked group cleanup.
    """
    def __init__(self, command, cwd, environment, log_paths, cap, interpreter, *, initialization_deadline):
        require(type(initialization_deadline) in (int, float) and math.isfinite(initialization_deadline)
                and initialization_deadline > time.monotonic(), "original child initialization deadline expired")
        self.logs = []
        self.threads = []
        self.failed = threading.Event()
        self.group_lock = threading.Lock()
        self.reaped = False
        self.exited = False
        self.process = None
        self.events = select.kqueue()
        read_fd, write_fd = os.pipe()
        try:
            for path in log_paths:
                self.logs.append(os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb"))
            self.process = subprocess.Popen([str(interpreter), "-I", "-S", str(CHILD),
                "--await-parent", str(read_fd), *command], cwd=cwd, env=environment,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                close_fds=True, pass_fds=(read_fd,), start_new_session=True)
            self.pid = self.process.pid
            # Only the child may keep a reader. Otherwise an interpreter that
            # exits before registration could leave the parent's write falsely
            # succeeding against its own read descriptor, losing NOTE_EXIT.
            os.close(read_fd)
            read_fd = None
            registration = select.kevent(self.pid, filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_ONESHOT,
                fflags=select.KQ_NOTE_EXIT)
            self.events.control([registration], 0, 0)
            for stream, log in zip((self.process.stdout, self.process.stderr), self.logs):
                thread = threading.Thread(target=self.drain, args=(stream, log, cap), daemon=True)
                self.threads.append(thread)
                thread.start()
            require(os.write(write_fd, b"G") == 1, "child release handshake failed")
        except BaseException as error:
            if self.process is not None:
                self.failed.set()
                error.owned_process = self
                error.owned_cleanup_errors = self._initialization_failure(initialization_deadline)
            else:
                for log in self.logs:
                    log.close()
            if self.process is None or self.reaped:
                self.events.close()
            raise
        finally:
            if read_fd is not None:
                os.close(read_fd)
            os.close(write_fd)

    def _poll_locked(self):
        if self.reaped:
            return self.process.returncode
        if not self.exited:
            for event in self.events.control(None, 1, 0):
                require(not (event.flags & select.KQ_EV_ERROR), "child event observation failed")
                require(event.ident == self.pid and event.fflags & select.KQ_NOTE_EXIT,
                        "unexpected child event identity")
                self.exited = True
        # Shared callers only distinguish None from completed. The true code is
        # obtained by shared wait() after group cleanup, never invented here.
        return 0 if self.exited else None

    def poll(self):
        with self.group_lock:
            return self._poll_locked()

    def wait(self, timeout):
        try:
            return super().wait(timeout)
        finally:
            if self.reaped:
                self.events.close()


class TaskInfo(ctypes.Structure):
    # Stable PROC_PIDTASKINFO ABI: six u64 counters followed by twelve i32
    # counters. Only resident bytes and total user/system time are consumed.
    _fields_ = [(name, ctypes.c_uint64) for name in
                ("virtual", "resident", "user", "system", "threads_user", "threads_system")] + [
                ("other_counters", ctypes.c_int32 * 12)]


class ResourceMonitor:
    def __init__(self, driver):
        self.driver = driver
        self.limits = driver.inventory["platform_expectations"]["limits"]
        self.stopped = threading.Event()
        self.failure = None
        self.lock = threading.Lock()
        self.children = []
        self.peak_bytes = 0
        self.peak_rss = 0
        self.minimum_free = None
        self.samples = 0
        self.proc = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        self.proc.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                          ctypes.c_void_p, ctypes.c_int]
        self.proc.proc_pidinfo.restype = ctypes.c_int
        self.thread = threading.Thread(target=self.watch, daemon=True)

    def register(self, process):
        with self.lock:
            self.children.append(process)
            if self.failure:
                process.signal(signal.SIGKILL)

    def sample(self):
        info = os.statvfs(self.driver.root)
        free = info.f_bavail * info.f_frsize
        self.minimum_free = free if self.minimum_free is None else min(free, self.minimum_free)
        require(free >= self.limits["minimum_free_bytes"], "Darwin run free-space floor reached")
        count, total = 0, 0
        for current, directories, files in os.walk(self.driver.root, followlinks=False):
            for name in directories + files:
                count += 1
                require(count <= self.limits["filesystem_entries"], "Darwin run entry bound reached")
                try:
                    item = (Path(current) / name).lstat()
                except FileNotFoundError:
                    continue
                total += max(item.st_size, item.st_blocks * 512)
                require(total <= self.limits["run_bytes"], "Darwin run disk allocation reached")
        self.peak_bytes = max(self.peak_bytes, total)
        with self.lock:
            for child in self.children:
                with child.group_lock:
                    if child._poll_locked() is not None:
                        continue
                    task = TaskInfo()
                    size = self.proc.proc_pidinfo(child.pid, 4, 0, ctypes.byref(task), ctypes.sizeof(task))
                    if size != ctypes.sizeof(task):
                        # Exit can occur between the non-reaping poll and query.
                        require(child._poll_locked() is not None, "owned child resource sample unavailable")
                        continue
                    self.peak_rss = max(self.peak_rss, task.resident)
                    require(task.resident <= self.limits["resident_bytes"], "Darwin child RSS allocation reached")
        require(time.monotonic() - self.driver.started <= self.driver.cases["limits"]["total_seconds"],
                "Darwin run deadline reached")
        require(dt.datetime.now(dt.timezone.utc) < core.utc(self.driver.manifest["expires_at"]),
                "Darwin allocation expired")
        self.samples += 1

    def check(self):
        if self.failure:
            raise ValueError(self.failure)

    def watch(self):
        while not self.stopped.wait(self.limits["sample_seconds"]):
            try:
                self.sample()
            except Exception as error:
                self.failure = str(error) if isinstance(error, ValueError) else "resource monitor failed"
                with self.lock:
                    for child in self.children:
                        child.signal(signal.SIGKILL)
                return

    def start(self):
        self.sample()
        self.thread.start()

    def close(self):
        self.stopped.set()
        self.thread.join(timeout=min(5, self.driver.cleanup_remaining()))
        require(not self.thread.is_alive(), "resource monitor failed to stop")
        self.check()

    def receipt(self):
        return {"samples": self.samples, "peak_run_bytes": self.peak_bytes,
                "peak_child_rss_bytes": self.peak_rss, "minimum_free_bytes": self.minimum_free,
                "failure": self.failure, "limits": self.limits,
                "limitation": "sampled bounds can overshoot between observations; no cgroup or CPU affinity"}


class DarwinDriver(core.Driver):
    expected_target = "aarch64-apple-darwin"
    observation_format = "recall-darwin-native-observation/v1"

    def __init__(self, manifest, inventory, isolation):
        super().__init__(manifest, inventory, isolation)
        (self.root / "tmp").mkdir(mode=0o700)
        self.interpreter = Path(manifest["python"])
        self.environment.update({"TMPDIR": str(self.root / "tmp"), "PYTHONDONTWRITEBYTECODE": "1",
                                 "TOKIO_WORKER_THREADS": "2", "RAYON_NUM_THREADS": "2",
                                 "PIPELINE_SKILLS_DIR": str(self.root / "installed/skills")})
        self.profile = self.root / "candidate.sb"
        core.private_new(self.profile, sandbox_profile(self.root, self.port, self.binary).encode())
        self.monitor = ResourceMonitor(self)
        self.report["platform_cases"] = []
        self.report["platform_scope"] = inventory["platform_expectations"]["scope"]

    def remaining(self):
        self.monitor.check()
        return super().remaining()

    def spawn(self, command, log_paths):
        self.monitor.check()
        deadline = time.monotonic() + min(5, self.cleanup_remaining())
        try:
            child = DarwinProcess(command, self.root / "service", self.environment, log_paths,
                                  self.cases["limits"]["process_log_bytes"], self.interpreter,
                                  initialization_deadline=deadline)
        except BaseException as error:
            owned = getattr(error, "owned_process", None)
            if owned is not None:
                # Register even direct preflight spawns, which do not pass
                # through the shared Driver.process command registry.
                try:
                    self.monitor.register(owned)
                except BaseException:
                    # register appends before acting on a failed monitor. Keep
                    # the original initialization exception and attached child.
                    pass
            raise
        self.monitor.register(child)
        return child

    def create_process(self, arguments, log_paths):
        self.monitor.check()
        return self.spawn(["/usr/bin/sandbox-exec", "-f", str(self.profile), str(self.binary), *arguments], log_paths)

    def isolation_preflight(self):
        forbidden = self.root / "denied-control"
        forbidden.mkdir(mode=0o700)
        canary = os.urandom(32)
        core.private_new(forbidden / "read-control", canary)
        profile = self.root / "probe.sb"
        core.private_new(profile, sandbox_profile(self.root, self.port, self.interpreter,
                                                  interpreter=self.interpreter).encode())
        logs = [self.root / "captures" / ("isolation." + suffix) for suffix in ("stdout", "stderr")]
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            listener.setblocking(False)
            child = self.spawn(["/usr/bin/sandbox-exec", "-f", str(profile), str(self.interpreter),
                "-I", "-S", str(CHILD), "--probe", str(self.root / "private"), str(forbidden),
                str(listener.getsockname()[1]), str(self.port)], logs)
            try:
                require(child.wait(min(10, self.remaining())) == 0, "sandbox denial preflight failed")
            finally:
                if not child.reaped:
                    child.signal(signal.SIGKILL)
                    child.wait(min(5, self.cleanup_remaining()))
            try:
                accepted, _ = listener.accept()
            except BlockingIOError:
                pass
            else:
                accepted.close()
                raise UnmetPrerequisite("outbound isolation did not hold")
        observed = core.read_json(logs[0])
        require(set(observed) == {"outside_read_errno", "outside_write_errno", "outbound_errno",
                                  "owned_loopback_bind", "owned_write"}, "sandbox controls incomplete")
        for key in ("outside_read_errno", "outside_write_errno", "outbound_errno"):
            require(observed[key] in (errno.EACCES, errno.EPERM), "sandbox denial was not observed")
        require(observed["owned_loopback_bind"] is True and observed["owned_write"] is True,
                "sandbox positive controls failed")
        require((forbidden / "read-control").read_bytes() == canary and
                not (forbidden / "write-control").exists(), "denied synthetic data changed")
        self.report["isolation"].update({"denial_preflight": observed,
            "candidate_policy_sha256": core.file_digest(self.profile), "probe_policy_sha256": core.file_digest(profile),
            "external_network_probe": False, "ambient_credentials": False,
            "network_scope": "candidate has no outbound rule; sole inbound/bind endpoint is owned loopback",
            "write_scope": "owned mutable directories only; artifact and evidence are not writable"})
        self.report["platform_cases"].append({"case": "actual_sandbox_controls", "observation": "passed"})

    def execute(self):
        try:
            self.isolation_preflight()
        except Exception as error:
            raise UnmetPrerequisite("Darwin sandbox preflight was not satisfied") from error
        super().execute()
        self.monitor.check()
        self.report["platform_cases"].append({"case": "shared_installed_cases_on_arm64", "observation": "passed"})
        require({case["case"] for case in self.report["platform_cases"]} ==
                set(self.inventory["platform_expectations"]["platform_cases"]), "Darwin platform observations missing")

    def run(self):
        code, started = 0, False
        try:
            self.monitor.start()
            started = True
            self.execute()
        except UnmetPrerequisite:
            code = 78
            self.report.update({"subset_observation": "unmet-prerequisite", "failure": "Darwin isolation prerequisite unavailable"})
        except Exception as error:
            code = 1
            self.report.update({"subset_observation": "failed", "failure_type": type(error).__name__,
                "failure": str(error) if isinstance(error, ValueError) else "transport or platform prerequisite failed"})
        finally:
            cleanups = [lambda: self.stop(require_clean=False)]
            # Attempt EVERY owned handle even if an earlier cleanup reports an
            # error. No loss of custody due to a failed log or graceful drain.
            for child in self.monitor.children:
                def cleanup(process=child):
                    if not process.reaped:
                        process.signal(signal.SIGKILL)
                        process.wait(min(5, self.cleanup_remaining()))
                cleanups.append(cleanup)
            if started:
                cleanups.append(self.monitor.close)
            for cleanup in cleanups:
                try:
                    cleanup()
                except Exception as error:
                    code = 1
                    self.report["subset_observation"] = "failed"
                    self.report.setdefault("cleanup_error_types", []).append(type(error).__name__)
            self.report.update({"duration_seconds": time.monotonic() - self.started,
                "http_attempts": self.requests, "processes": self.serial,
                "resources": self.monitor.receipt(), "product_acceptance": False})
            core.private_new(self.root / "observation.json", core.encoded(self.report))
        return code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("plan")
    run = sub.add_parser("run")
    run.add_argument("--manifest", type=Path, required=True)
    run.add_argument("--execute-reviewed", action="store_true", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        print(json.dumps(plan(), indent=2))
        return 0
    manifest, inventory = validate_allocation(args.manifest)
    isolation = host_prerequisites(manifest)
    # Initial floor before any extraction; the continuous monitor includes the
    # verifier's temporary tree and final installed tree under the same root.
    limits = inventory["platform_expectations"]["limits"]
    require(shutil.disk_usage(Path(manifest["run_directory"]).parent).free >= limits["minimum_free_bytes"],
            "Darwin run free-space floor unavailable")
    isolation["archive_bound"] = archive_bounds(manifest, limits)
    return DarwinDriver(manifest, inventory, isolation).run()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as error:
        print(json.dumps({"status": "unmet-prerequisite", "error_type": type(error).__name__,
                          "product_acceptance": False}), file=sys.stderr)
        sys.exit(78)
