#!/usr/bin/env python3
"""Allocated single-lane native build; --plan reads inputs without execution."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import json
import os
from pathlib import Path
import platform
import pwd
import re
import signal
import stat
import sys
import time

import artifact
from container_artifact import GIB, DEFAULT_ALLOCATION_BYTES as MAX_ALLOCATION_BYTES, MIN_FREE_BYTES, ResourceBudget
from darwin_build_process import BuildProcess, GroupObserver

FORMAT = "recall-darwin-build-allocation/v1"
MAX_SECONDS = 12 * 60 * 60
CLEANUP_SECONDS = 15
LOG_BYTES = 256 * 1024**2
IDENTITY_KEYS = {"source_commit", "source_tree", "source_tar_sha256", "cargo_lock_sha256"}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def canonical(value, directory=True):
    path = Path(value)
    require(path.is_absolute() and path.resolve(strict=True) == path, "path must exist and be canonical")
    require(path.is_dir() if directory else stat.S_ISREG(path.stat().st_mode), "wrong path type")
    require(not any(character in str(path) for character in "\n\r:\x00"), "unsupported path character")
    return path


def read_json(path, limit=65536):
    path = canonical(path, False)
    require(path.stat().st_size <= limit, "JSON input exceeds bound")
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    return json.loads(path.read_bytes(), object_pairs_hook=pairs)


def private_file(path):
    path = canonical(path, False)
    require(path.stat().st_uid == os.getuid() and not path.stat().st_mode & 0o077,
            "allocation, ledger and lane files must be private and owned")
    return path


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(parsed.tzinfo is not None, "allocation timestamp requires timezone")
    return parsed


def write_receipt(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def validate_identity(identity):
    require(set(identity) == IDENTITY_KEYS, "complete trusted source identity required")
    for key, value in identity.items():
        require(isinstance(value, str) and re.fullmatch("[a-f0-9]{40}" if key in
                ("source_commit", "source_tree") else "[a-f0-9]{64}", value), "invalid source identity")
    return identity


def validate_limits(allocation):
    require(allocation.get("target") == "aarch64-apple-darwin", "native ARM64 target required")
    require(allocation.get("cargo_jobs") == 4, "exactly four Cargo jobs required")
    seconds = allocation.get("maximum_seconds")
    require(type(seconds) is int and 0 < seconds <= MAX_SECONDS, "build ceiling must be at most 12 hours")
    require(allocation.get("minimum_free_bytes", 0) >= MIN_FREE_BYTES, "250 GiB free floor required")
    require(0 < allocation.get("allocation_bytes", 0) <= MAX_ALLOCATION_BYTES, "150 GiB maximum required")
    rss = allocation.get("maximum_group_resident_bytes")
    require(type(rss) is int and 0 < rss <= 96 * GIB, "explicit group RSS ceiling at most 96 GiB required")
    window = (timestamp(allocation["expires_at"]) - timestamp(allocation["started_at"])).total_seconds()
    require(0 < window <= seconds, "lane window must fit the original elapsed ceiling")
    lane = allocation.get("lane", {})
    require(lane.get("coordinator") == "codex-security-adr-mac" and lane.get("state") == "START"
            and isinstance(lane.get("grant_id"), str) and 0 < len(lane["grant_id"]) <= 256,
            "original Mac START grant required")
    return None


def validate(allocation):
    require(allocation.get("format") == FORMAT, "wrong Darwin build allocation format")
    identity = validate_identity(allocation.get("source_identity", {}))
    validate_limits(allocation)
    return identity


def prepare(allocation):
    identity = validate(allocation)
    tools_expected = allocation.get("tool_sha256", {})
    require(isinstance(allocation.get("python_sha256"), str)
            and re.fullmatch("[a-f0-9]{64}", allocation["python_sha256"]), "pinned Python digest required")
    require(all(isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value)
                for value in tools_expected.values()), "tool digest must be SHA256")
    task = canonical(allocation["task_directory"])
    require(task.stat().st_uid == os.getuid() and not task.stat().st_mode & 0o077, "private task directory required")
    source = canonical(allocation["source_directory"])
    require(source == task / "source" and (source / ".git").is_dir()
            and (source / ".git").resolve(strict=True) == source / ".git", "reviewed detached bundle clone required")
    paths = {name: canonical(allocation[name]) for name in
             ("cargo_home", "cargo_target", "ort_cache", "rustup_home", "tmp_directory", "output_directory",
              "receipt_directory", "developer_directory", "sdk_root", "libclang_directory")}
    for name in ("ort_cache", "rustup_home", "tmp_directory", "output_directory", "receipt_directory"):
        require(paths[name].is_relative_to(task) and not paths[name].is_relative_to(source), "mutable task path outside allocation")
    require(paths["ort_cache"] == task / "ort-cache", "task-local ORT cache required")
    require(not any(paths["output_directory"].iterdir()) and not any(paths["receipt_directory"].iterdir()),
            "output and receipt directories must be empty for this attempt")
    home = canonical(pwd.getpwuid(os.getuid()).pw_dir)
    key = source.name + "-" + hashlib.sha1(str(source / ".git").encode()).hexdigest()[:10]
    lane = "recall-darwin-" + identity["source_commit"][:12]
    base = home / "Library/Caches/rust-workspaces" / key
    require(paths["cargo_home"] == base / "cargo-home" and paths["cargo_target"] == base / "targets" / lane,
            "cache paths differ from actual repository wrapper")
    require(not (paths["cargo_home"] / "bin").exists(), "repository Cargo home must not override pinned toolchain")
    require(not any((paths["cargo_home"] / name).exists() for name in ("config", "config.toml", "credentials", "credentials.toml")),
            "task Cargo home must not contain ambient configuration or credentials")
    toolchain = paths["rustup_home"] / "toolchains/1.94.0-aarch64-apple-darwin/bin"
    python = canonical(allocation["python"], False)
    require(python.is_relative_to(task / "tools") and artifact.digest(python) == allocation["python_sha256"],
            "pinned task-local interpreter differs")
    tools = {"rustc": canonical(toolchain / "rustc", False), "cargo": canonical(toolchain / "cargo", False),
             "clang": Path(allocation["clang"]), "clang++": Path(allocation["clangxx"])}
    for name in ("clang", "clang++"):
        require(tools[name].is_absolute() and tools[name].parent.resolve(strict=True) == tools[name].parent
                and tools[name].is_file() and tools[name].resolve(strict=True).is_relative_to(paths["developer_directory"]),
                "compiler outside selected CLT")
        # Preserve clang++ argv[0] even when it is a symlink to clang: the name
        # selects the C++ driver. Hash the resolved bytes, not a different tool.
        require(tools[name].name == name, "compiler driver name differs")
    require((paths["libclang_directory"] / "libclang.dylib").is_file(), "CLT libclang unavailable")
    require(paths["sdk_root"].is_relative_to(paths["developer_directory"])
            and paths["libclang_directory"].is_relative_to(paths["developer_directory"]), "SDK/libclang outside selected CLT")
    expected_hashes = allocation.get("tool_sha256", {})
    require(set(expected_hashes) == set(tools), "all actual compiler and Cargo hashes required")
    for name, executable in tools.items():
        require(artifact.digest(executable) == expected_hashes[name], "allocated tool digest differs")
    ledger = private_file(allocation["budget_receipt"])
    original = read_json(ledger)
    require(original.get("format") == "meerkat-recall-artifact-allocation/v1", "original shared ledger required")
    monitored = tuple(canonical(value) for value in allocation["monitored_paths"])
    devices = {str(path.stat().st_dev) for path in monitored}
    require(devices == set(original["filesystems"]), "original shared ledger filesystem set must not change")
    for path in (task, source, ledger.parent, *paths.values(), python, *tools.values()):
        require(str(path.stat().st_dev) in devices, "allocation omits an involved filesystem")
    lane_file = private_file(allocation["lane_lock"])
    require(str(lane_file.stat().st_dev) in devices, "allocation omits lane-lock filesystem")
    bootstrap = canonical(task / "cargo-bootstrap")
    require(not (bootstrap / "bin").exists(), "bootstrap Cargo home must not override pinned toolchain")
    environment = {"HOME": str(home), "PATH": f"{python.parent}:{toolchain}:/usr/bin:/bin:/usr/sbin:/sbin",
        "LANG": "C", "LC_ALL": "C", "PYTHONDONTWRITEBYTECODE": "1",
        "RUSTUP_HOME": str(paths["rustup_home"]), "RUSTUP_TOOLCHAIN": "1.94.0-aarch64-apple-darwin",
        "CARGO_HOME": str(bootstrap), "CARGO_BUILD_JOBS": "4", "CARGO_INCREMENTAL": "0",
        "RECALL_BUILD_MIN_FREE_GIB": str((allocation["minimum_free_bytes"] + GIB - 1) // GIB),
        "LZMA_API_STATIC": "1", "TMPDIR": str(paths["tmp_directory"]), "RUST_LANE_ID": lane,
        "ORT_CACHE_DIR": str(paths["ort_cache"]), "DEVELOPER_DIR": str(paths["developer_directory"]),
        "SDKROOT": str(paths["sdk_root"]), "CC": str(tools["clang"]), "CXX": str(tools["clang++"]),
        "LIBCLANG_PATH": str(paths["libclang_directory"]), "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": "/dev/null", "GIT_TERMINAL_PROMPT": "0"}
    return {"identity": identity, "task": task, "source": source, "paths": paths, "python": python,
            "tools": tools, "environment": environment, "ledger": ledger, "monitored": monitored,
            "lane_file": lane_file, "original_ledger_sha256": artifact.digest(ledger)}


class Supervisor:
    def __init__(self, allocation, prepared, observer):
        self.allocation, self.prepared, self.observer = allocation, prepared, observer
        self.started = time.monotonic()
        self.wall_deadline = min(timestamp(allocation["expires_at"]),
                                 timestamp(allocation["started_at"]) + dt.timedelta(seconds=allocation["maximum_seconds"]))
        self.deadline = self.started + max(0, (self.wall_deadline - dt.datetime.now(dt.timezone.utc)).total_seconds())
        if prepared.get("bootstrap_clock") is not None:
            original = prepared["bootstrap_clock"]
            self.deadline = min(self.deadline, original["monotonic"] + max(0,
                (self.wall_deadline - original["wall"]).total_seconds()))
        self.cancelled = False
        self.children = []
        self.samples = 0
        self.peak_rss = 0
        self.minimum_free = None
        self.last_budget = None
        self.commands = []
        self.budget = ResourceBudget(prepared["ledger"], prepared["monitored"],
                                     allocation["minimum_free_bytes"], allocation["allocation_bytes"])

    def cleanup_deadline(self, child=None):
        # Clamp wall-clock changes without extending the original monotonic
        # ceiling. Each command retains this same deadline for every retry.
        deadline = min(self.deadline, time.monotonic() + max(0,
            (self.wall_deadline - dt.datetime.now(dt.timezone.utc)).total_seconds()))
        return min(deadline, child.cleanup_deadline) if child is not None else deadline

    def check(self, child=None):
        require(not self.cancelled, "build cancelled")
        require(time.monotonic() < self.cleanup_deadline() - CLEANUP_SECONDS,
                "build original elapsed ceiling reached; cleanup reserve retained")
        now = dt.datetime.now(dt.timezone.utc)
        require(timestamp(self.allocation["started_at"]) <= now < timestamp(self.allocation["expires_at"]),
                "original Mac lane grant is not current")
        # Never allow ResourceBudget's initial-allocation behavior during a run.
        private_file(self.prepared["ledger"])
        self.last_budget = self.budget.check(blocking=False)
        free = min(item["free"] for item in self.last_budget["filesystems"].values())
        self.minimum_free = free if self.minimum_free is None else min(self.minimum_free, free)
        if self.prepared.get("bootstrap"):
            import darwin_bootstrap
            darwin_bootstrap.check_download(sys.modules[__name__], self.prepared)
        if child is not None:
            sample = self.observer.sample(child.process.pid)
            require(not sample["uncertain_pids"], "owned build resource observation incomplete")
            self.peak_rss = max(self.peak_rss, sample["resident_bytes"])
            require(sample["resident_bytes"] < self.allocation["maximum_group_resident_bytes"],
                    "owned build resident allocation reached")
        self.samples += 1

    def command(self, name, arguments, seconds, cap=4 * 1024**2):
        path = self.prepared["paths"]["receipt_directory"] / (name + ".log")
        child = BuildProcess(self.observer)
        started = time.monotonic()
        command_deadline = min(started + seconds, self.cleanup_deadline() - CLEANUP_SECONDS)
        child.cleanup_deadline = min(command_deadline + CLEANUP_SECONDS, self.cleanup_deadline())
        self.children.append(child)  # Keep custody even if launch only partly succeeds.
        record = {"name": name, "argv": list(map(str, arguments)), "log": str(path), "cap_bytes": cap}
        child.command_record = record
        self.commands.append(record)
        self.check()
        try:
            def registered(handle):
                custody = handle.custody()
                record.update(custody)
                write_receipt(path.with_suffix(".custody.json"), {
                    "format": "recall-darwin-command-custody/v1", "name": name,
                    "supervisor_pid": os.getpid(), "grant_id": self.allocation["lane"]["grant_id"],
                    "argv": record["argv"], "child_released": False, **custody})
            child.launch(record["argv"], self.prepared["source"], self.prepared["environment"],
                         path, self.prepared["python"], cap, registered=registered)
            next_sample = 0
            while not child.done():
                require(not child.failure.is_set(), "build log failed or exceeded bound")
                require(time.monotonic() < command_deadline, "command deadline reached")
                require(not self.cancelled, "build cancelled")
                if time.monotonic() >= next_sample:
                    self.check(child)
                    next_sample = time.monotonic() + 5
                time.sleep(min(0.1, max(0, command_deadline - time.monotonic())))
        except BaseException as error:
            record["failure"] = {"type": type(error).__name__, "message": str(error),
                                 "errno": getattr(error, "errno", None), "custody": child.custody()}
            raise
        finally:
            try:
                record["returncode"] = child.cleanup(deadline=self.cleanup_deadline(child))
            except BaseException as error:
                record["cleanup_failure"] = {"type": type(error).__name__,
                    "message": str(error), "errno": getattr(error, "errno", None),
                    "custody": child.custody()}
                raise
            finally:
                record.update(child.custody())
                record["elapsed_seconds"] = time.monotonic() - started
                record["captured_bytes"] = min(child.bytes, cap)
        require(not child.failure.is_set(), "build log failed or exceeded bound")
        require(record["returncode"] == 0, "owned command failed: " + name)
        self.check()
        return path

    def run(self):
        if self.prepared.get("bootstrap"):
            import darwin_bootstrap
            return darwin_bootstrap.run(sys.modules[__name__], self)
        p = self.prepared
        python = str(p["python"])
        source = p["source"]
        identity_command = [python, str(source / "scripts/release/artifact.py"), "source-identity", "--root", str(source)]
        before = self.command("source-before", identity_command, 120)
        require(read_json(before) == p["identity"], "source differs from trusted transfer identity")
        for name, command in (
            ("python", [python, "--version"]),
            ("rustc", [str(p["tools"]["rustc"]), "-vV"]),
            ("cargo", [str(p["tools"]["cargo"]), "-V"]),
            ("clang", [str(p["tools"]["clang"]), "--version"]),
            ("builder-os", ["/usr/bin/sw_vers"]),
            ("sdk-version", ["/usr/bin/xcrun", "--sdk", "macosx", "--show-sdk-version"]),
            ("cargo-env", [str(source / "scripts/repo-cargo"), "--print-env"]),
        ):
            self.command(name, command, 30)
        rustc = (p["paths"]["receipt_directory"] / "rustc.log").read_text()
        require("release: 1.94.0\n" in rustc and "host: aarch64-apple-darwin\n" in rustc,
                "actual compiler is not pinned Rust ARM64")
        actual = dict(line.split("=", 1) for line in
                      (p["paths"]["receipt_directory"] / "cargo-env.log").read_text().splitlines())
        require(actual["CARGO_HOME"] == str(p["paths"]["cargo_home"])
                and actual["CARGO_TARGET_DIR"] == str(p["paths"]["cargo_target"]), "actual cache paths changed")
        self.command("build", [str(source / "scripts/release/build-native"), "aarch64-apple-darwin",
                     str(p["paths"]["output_directory"])], self.allocation["maximum_seconds"], LOG_BYTES)
        after = self.command("source-after", identity_command, 120)
        require(read_json(after) == p["identity"], "source changed during build")


def execute(allocation, prepared, allocation_sha256):
    require(platform.system() == "Darwin" and platform.machine() == "arm64", "native ARM64 Darwin required")
    bootstrap = prepared.get("bootstrap") is True
    require((sys.version_info[:2] >= (3, 11) if bootstrap else sys.version_info[:3] == (3, 12, 12))
            and Path(sys.executable).resolve() == prepared["python"],
            "run the supervisor with the exact pinned Python")
    require(allocation.get("execution_authorized") is True, "coordinator build authorization required")
    lock_path = prepared["lane_file"]
    lock = os.open(lock_path, os.O_RDWR | os.O_NOFOLLOW)
    handlers = {}
    supervisor = None
    report = {"format": "recall-darwin-build-supervision/v1", "allocation_sha256": allocation_sha256,
              "source_identity": prepared["identity"], "environment": prepared["environment"],
              "tool_sha256": ({name: row["sha256"] for name, row in allocation["tools"].items()}
                              if bootstrap else allocation["tool_sha256"]),
              "python_sha256": (allocation["bootstrap_python"]["sha256"] if bootstrap else allocation["python_sha256"]),
              "original_ledger_sha256": prepared["original_ledger_sha256"],
              "lane": allocation["lane"], "product_acceptance": False, "build_succeeded": False,
              "lane_release_ready": False}
    if bootstrap:
        report.update(format="recall-darwin-bootstrap-supervision/v1", bootstrap_succeeded=False,
                      operation=allocation["operation"])
    acquired = False
    marker_written = False
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        acquired = True
        require(os.fstat(lock).st_size == 0, "Mac lane contains an unreconciled prior build; coordinator review required")
        marker = json.dumps({"allocation_sha256": allocation_sha256, "supervisor_pid": os.getpid(),
                             "grant_id": allocation["lane"]["grant_id"], "release_ready": False}).encode()
        os.write(lock, marker)
        os.fsync(lock)
        marker_written = True
        supervisor = Supervisor(allocation, prepared, GroupObserver())
        for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP):
            handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, lambda number, frame: setattr(supervisor, "cancelled", True))
        supervisor.run()
        report["bootstrap_succeeded" if bootstrap else "build_succeeded"] = True
    except BaseException as error:
        report["failure"] = type(error).__name__ + ": " + str(error)
    finally:
        cleanup_errors = []
        if supervisor is not None:
            for child in reversed(supervisor.children):
                try:
                    result = child.cleanup(deadline=supervisor.cleanup_deadline(child))
                    if isinstance(getattr(child, "command_record", None), dict):
                        child.command_record["returncode"] = result
                except BaseException as error:
                    cleanup_errors.append(str(error))
                finally:
                    if isinstance(getattr(child, "command_record", None), dict):
                        child.command_record.update(child.custody())
            report.update(commands=supervisor.commands, disk_samples=supervisor.samples,
                          elapsed_seconds=time.monotonic() - supervisor.started,
                          peak_group_resident_bytes=supervisor.peak_rss, minimum_observed_free_bytes=supervisor.minimum_free,
                          final_budget=supervisor.last_budget, cleanup_errors=cleanup_errors)
            report["lane_release_ready"] = not cleanup_errors and all(
                child.group_empty and (child.process is None or child.reaped) for child in supervisor.children)
        if bootstrap and supervisor is not None:
            try:
                supervisor.check()
                report["final_budget"] = supervisor.last_budget
                require(type(report["final_budget"]) is dict
                        and report["final_budget"].get("refused") is False,
                        "actual final nonrefused bootstrap budget required")
            except BaseException as error:
                report["bootstrap_succeeded"] = False
                report["terminal_failure"] = type(error).__name__ + ": " + str(error)
        # A nonempty persistent marker survives crashes and incomplete cleanup.
        # Do not clear a previous owner's marker when acquiring its blocked lane.
        if marker_written and supervisor is None:
            # Observer construction failed before any child could exist.
            report["lane_release_ready"] = True
        if acquired and marker_written and report["lane_release_ready"]:
            # Commit the cleanup receipt before clearing the durable marker.
            write_receipt(prepared["paths"]["receipt_directory"] / "supervision.json", report)
            os.ftruncate(lock, 0)
            os.fsync(lock)
        for signum, handler in handlers.items():
            signal.signal(signum, handler)
        os.close(lock)
        if not (prepared["paths"]["receipt_directory"] / "supervision.json").exists():
            write_receipt(prepared["paths"]["receipt_directory"] / "supervision.json", report)
    if bootstrap and supervisor is not None and time.monotonic() >= supervisor.cleanup_deadline():
        raise ValueError("original bootstrap allocation expired during terminal receipt")
    require(report["bootstrap_succeeded" if bootstrap else "build_succeeded"] and report["lane_release_ready"],
            "Darwin build failed; inspect supervision.json and retain lane if cleanup is unconfirmed")
    return report


def main():
    entry_clock = {"monotonic": time.monotonic(), "wall": dt.datetime.now(dt.timezone.utc)}
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--allocation", type=Path, required=True)
    parser.add_argument("--sha256", required=True, help="independently reviewed allocation digest")
    parser.add_argument("--bootstrap", action="store_true", help="acquire exact source/tools within the original Mac allocation")
    parser.add_argument("--plan", action="store_true", help="validate allocation paths; execute nothing")
    args = parser.parse_args()
    allocation_path = private_file(args.allocation)
    require(artifact.digest(allocation_path) == args.sha256, "allocation bytes differ from reviewed digest")
    allocation = read_json(allocation_path)
    if args.bootstrap:
        import darwin_bootstrap
        prepared = darwin_bootstrap.prepare(sys.modules[__name__], allocation)
        prepared["bootstrap_clock"] = entry_clock
    else:
        prepared = prepare(allocation)
    if args.plan and args.bootstrap:
        print(json.dumps({"format": "recall-darwin-bootstrap-plan/v1", "allocation_sha256": args.sha256,
            "operation": allocation["operation"], "source_identity": prepared["identity"],
            "environment": prepared["environment"], "execution_authorized": False}, sort_keys=True))
    elif args.plan:
        print(json.dumps({"format": "recall-darwin-build-plan/v1", "allocation_sha256": args.sha256,
            "source_identity": prepared["identity"], "command": [str(prepared["source"] / "scripts/release/build-native"),
            "aarch64-apple-darwin", str(prepared["paths"]["output_directory"])],
            "environment": prepared["environment"], "execution_authorized": False}, sort_keys=True))
    else:
        execute(allocation, prepared, args.sha256)


if __name__ == "__main__":
    main()
