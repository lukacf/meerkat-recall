"""Original bounded process evidence for separately allocated Linux runs.

This module never launches, signals, waits for, or adopts a process. The existing
OwnedProcess custody primitive reports its actual actions at their boundaries.
Captures are private observations, not independently accepted proof or authority.
"""
from __future__ import annotations

import copy
import hashlib
import json
import math
import os
from pathlib import Path
import stat
import threading
import time

MAX_METADATA = 2 * 1024 * 1024
MAX_EVENTS = 512
MAX_BINARY = 512 * 1024 * 1024
INSPECTION_STREAM_LIMITS = (32 * 1024 * 1024 + 1, 1024 * 1024)
INSPECTION_STREAM_KINDS = ("snapshot-inspection", "process-log")


def require(value, reason):
    if not value:
        raise ValueError(reason)


def encode(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def pin_allocation_clock(total_seconds, expires_wall_ns, *, original_sample=None):
    require(type(total_seconds) is int and 0 < total_seconds <= 7800
            and type(expires_wall_ns) is int, "invalid original allocation clock")
    if original_sample is None:
        before = time.monotonic_ns()
        wall = time.time_ns()
        after = time.monotonic_ns()
    else:
        require(type(original_sample) is dict
                and set(original_sample) == {"mono_before_ns", "wall_ns", "mono_after_ns"}
                and all(type(value) is int and value > 0 for value in original_sample.values()),
                "invalid original pre-input clock sample")
        before, wall, after = (original_sample[key] for key in ("mono_before_ns", "wall_ns", "mono_after_ns"))
    deadline = before + min(total_seconds * 10**9, expires_wall_ns - wall)
    require(before <= after < deadline, "original allocation expired during clock sampling")
    return {"mono_before_ns": before, "wall_ns": wall, "mono_after_ns": after,
            "expires_wall_ns": expires_wall_ns, "total_seconds": total_seconds}, deadline


def deadline_seconds(deadline_ns):
    """Old float timeout interfaces must round DOWN, never renew a nanosecond."""
    value = deadline_ns / 10**9
    while int(value * 10**9) > deadline_ns:
        value = math.nextafter(value, -math.inf)
    return value


def read_bounded(path, maximum):
    with Path(path).open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum, "original capture exceeds bound")
    return raw


def process_identity(raw):
    text = raw.decode("ascii")
    left, right = text.find(" ("), text.rfind(") ")
    require(left > 0 and right > left, "invalid original process stat")
    fields = text[right + 2:].split()
    require(len(fields) >= 20, "incomplete original process stat")
    value = {"pid": int(text[:left]), "ppid": int(fields[1]), "pgrp": int(fields[2]),
             "session": int(fields[3]), "start_ticks": int(fields[19])}
    require(all(type(part) is int and part >= 0 for part in value.values()) and value["pid"] > 0,
            "invalid original process identity")
    return value


def file_identity(value):
    return {"device": value.st_dev, "inode": value.st_ino, "bytes": value.st_size,
            "mode": value.st_mode, "mtime_ns": value.st_mtime_ns, "ctime_ns": value.st_ctime_ns}


def binary_identity(path, deadline_ns, *, proc_link=False):
    path = Path(path)
    require(path.is_absolute(), "absolute original executable required")
    descriptor = os.open(path, os.O_RDONLY | (0 if proc_link else os.O_NOFOLLOW))
    try:
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= MAX_BINARY,
                "bounded regular original executable required")
        result = hashlib.sha256()
        count = 0
        while True:
            require(time.monotonic_ns() < deadline_ns, "original executable capture deadline expired")
            block = os.read(descriptor, 1024 * 1024)
            if not block:
                break
            count += len(block)
            require(count <= MAX_BINARY, "original executable grew beyond bound")
            result.update(block)
        after = os.fstat(descriptor)
        require(file_identity(before) == file_identity(after) and count == before.st_size,
                "original executable changed during capture")
        return {"path": str(path), "sha256": result.hexdigest(), **file_identity(before)}
    finally:
        os.close(descriptor)


class CaptureJournal:
    def __init__(self, root, *, reserve=None):
        self.root = Path(root)
        require(self.root.is_absolute() and self.root.resolve() == self.root, "canonical original capture root required")
        self.lock = threading.RLock()
        self.files, self.written, self.sequence = {}, 0, 0
        self.processes = []
        self.capture_errors = []
        self.has_shared_reservation = reserve is not None
        self.reserve = reserve or (lambda _amount: None)

    def save(self, relative, raw, kind):
        require(type(raw) is bytes and type(kind) is str and kind, "original typed bytes required")
        path = self.root / relative
        require(path.resolve() == path and path.parent == self.root / "captures",
                "process evidence escaped original capture directory")
        with self.lock:
            require(str(path) not in self.files and self.written + len(raw) <= MAX_METADATA,
                    "original process metadata budget exhausted or capture replaced")
            self.reserve(len(raw))
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            ref = {"sha256": sha(raw), "bytes": len(raw), "kind": kind}
            self.files[str(path)] = ref
            self.written += len(raw)
            return {"path": str(path), "ref": ref}

    def event(self, process, action, data):
        with self.lock:
            self.sequence += 1
            require(self.sequence <= MAX_EVENTS, "original process event budget exhausted")
            return self.save(f"captures/process-event-{self.sequence:04d}.json", encode({
                "format": "recall-installed-process-event/v2", "sequence": self.sequence,
                "process": process, "action": action, "observed_ns": time.monotonic_ns(), "data": data}),
                "installed-process-event")

    def inventory(self, additional=()):
        files = dict(self.files)
        for path, kind, maximum in additional:
            path = Path(path)
            require(path.resolve() == path and path.is_file() and not path.is_symlink(),
                    "original capture file missing or replaced")
            raw = read_bounded(path, maximum)
            ref = {"sha256": sha(raw), "bytes": len(raw), "kind": kind}
            require(str(path) not in files or files[str(path)] == ref, "original capture file identity changed")
            files[str(path)] = ref
        return {"format": "recall-dynamic-capture-inventory/v2",
                "files": [{"path": path, "ref": ref} for path, ref in sorted(files.items())]}


class ProcessProof:
    """One immutable invocation, prepared before the actual Popen call."""
    def __init__(self, binary, arguments, cwd, environment, log_paths, deadline_ns, *, journal=None, index=1,
                 stream_limits=None, stream_kinds=None):
        self.journal, self.index = journal, index
        self.lock = threading.RLock()
        self.raw = {}
        self.events, self.errors = [], []
        self.started_ns, self.deadline_ns = time.monotonic_ns(), deadline_ns
        self.launch_stat_raw = self.launch_stat_after_raw = None
        self.launch_executable = self.launch_binary = None
        self.identity = None
        self.observed_exit = self.reaped_ns = self.exit_code = None
        self.reaped = False
        self.logs = [str(Path(path)) for path in log_paths]
        self.log_state = {}
        self._original_streams = None
        require(type(deadline_ns) is int and self.started_ns < deadline_ns, "original process allocation expired")
        self.intent = {"argv": [str(binary), *arguments], "cwd": str(cwd),
                       "environment_names": sorted(environment), "started_ns": self.started_ns,
                       "deadline_ns": deadline_ns, "log_paths": self.logs}
        if stream_limits is not None or stream_kinds is not None:
            require(type(stream_limits) is tuple and all(type(value) is int for value in stream_limits)
                    and stream_limits == INSPECTION_STREAM_LIMITS
                    and type(stream_kinds) is tuple and stream_kinds == INSPECTION_STREAM_KINDS,
                    "unsupported original output stream roles or bounds")
            require(type(arguments) is list and len(arguments) == 7
                    and arguments[:3] == ["snapshot", "inspect", "--input"]
                    and arguments[4:] == ["--json", "--include-records", "--compact-records"]
                    and type(arguments[3]) is str and Path(arguments[3]).is_absolute()
                    and str(Path(arguments[3]).resolve()) == arguments[3],
                    "report output requires the fixed original snapshot inspection command")
            require(len(self.logs) == 2 and len(set(self.logs)) == 2
                    and journal is not None and journal.has_shared_reservation,
                    "report output requires two original streams and the shared capture reservation")
            self._original_streams = tuple(zip(self.logs, stream_kinds, stream_limits))
            self.intent["streams"] = self._stream_rows()
        require(len(encode(self.intent)) <= 65536, "original process intent exceeds bound")
        if journal is not None:
            journal.processes.append(self)
        if self._original_streams is not None:
            # Hold the original retained-output ceiling before any output file
            # or process exists. A failure/short output never refunds this hold.
            try:
                journal.reserve(sum(stream_limits))
            except BaseException as error:
                self.failed("output-reservation", error)
                raise
        self.emit("intent", self.intent)
        try:
            self.launch_binary = binary_identity(binary, deadline_ns)
            self.emit("launch-binary", self.launch_binary)
        except BaseException as error:
            self.failed("launch-binary", error)
            raise

    def _stream_rows(self):
        return [{"path": path, "kind": kind, "maximum_bytes": maximum, "reserved_bytes": maximum}
                for path, kind, maximum in self._original_streams or ()]

    def output_streams(self):
        """Return only this invocation's original stream roles, never an override."""
        if self._original_streams is None:
            require("streams" not in self.intent, "original output stream intent changed")
            return [(path, "process-log", 4 * 1024 * 1024) for path in self.logs]
        require(self.intent.get("streams") == self._stream_rows()
                and tuple(self.logs) == tuple(row[0] for row in self._original_streams),
                "original output stream intent changed")
        return list(self._original_streams)

    def validate_stream_limits(self, limits, ordinary_cap):
        streams = self.output_streams()
        if self._original_streams is None:
            require(limits is None, "original output streams were not reserved")
            return (ordinary_cap, ordinary_cap)
        require(type(limits) is tuple and all(type(value) is int for value in limits)
                and limits == tuple(row[2] for row in streams)
                and type(ordinary_cap) is int and ordinary_cap == INSPECTION_STREAM_LIMITS[1],
                "actual output streams differ from original reserved bounds")
        return limits

    def emit(self, action, data):
        with self.lock:
            record = {"action": action, "observed_ns": time.monotonic_ns(), "data": copy.deepcopy(data)}
            if self.journal is not None:
                record["capture"] = self.journal.event(self.index, action, data)
            require(len(self.events) < 128, "original process event count exceeded")
            self.events.append(record)

    def failed(self, action, error):
        with self.lock:
            self.errors.append({"action": action, "error_type": type(error).__name__, "observed_ns": time.monotonic_ns()})
            try:
                self.emit("capture-error", self.errors[-1])
            except Exception:
                # Cleanup must retain custody even when persistence itself fails.
                pass

    def original_raw(self, label, raw):
        require(label not in self.raw, "original process bytes cannot be replaced")
        self.raw[label] = raw
        if self.journal is not None:
            return self.journal.save(f"captures/process-{self.index:03d}-{label}", raw, "process-stat")
        return {"ref": {"sha256": sha(raw), "bytes": len(raw), "kind": "process-stat"}}

    def launched(self, pid):
        before = read_bounded(f"/proc/{pid}/stat", 8192)
        self.launch_stat_raw = before
        self.identity = process_identity(before)
        require(self.identity["pid"] == pid and self.identity["ppid"] == os.getpid()
                and self.identity["pgrp"] == pid and self.identity["session"] == pid,
                "original child birth or new-session identity differs")
        refs = {"before": self.original_raw("launch-before.stat", before)}
        exe_error = None
        try:
            target = os.readlink(f"/proc/{pid}/exe")
            actual = binary_identity(f"/proc/{pid}/exe", self.deadline_ns, proc_link=True)
            require(target == self.intent["argv"][0] and all(actual[key] == self.launch_binary[key]
                    for key in ("sha256", "device", "inode", "bytes", "mode", "mtime_ns", "ctime_ns")),
                    "actual child executable differs from original launch input")
            self.launch_executable = {**actual, "target": target}
        except FileNotFoundError as error:
            # /proc/PID/exe disappears for an already exited, still unreaped CLI.
            # Keep its original raw zombie stat and launch input, never claim a
            # live executable observation that did not occur.
            exe_error = type(error).__name__
        after = read_bounded(f"/proc/{pid}/stat", 8192)
        self.launch_stat_after_raw = after
        require(process_identity(after) == self.identity, "original child identity changed during launch capture")
        if exe_error is not None:
            # Exit can release /proc/PID/exe before stat reports Z. Observe that
            # transition without reaping or renewing the original deadline.
            # Every sample remains original evidence; only an observed Z with
            # the exact unreaped birth identity can complete this fallback.
            transition_deadline = min(self.deadline_ns, time.monotonic_ns() + 100_000_000)
            for attempt in range(16):
                require(time.monotonic_ns() < transition_deadline,
                        "original exit observation deadline expired")
                observed = self.original_raw(f"launch-exit-{attempt:02d}.stat", after)
                self.emit("launch-exit-observation", {"stat": observed})
                self.launch_stat_after_raw = after
                require(process_identity(after) == self.identity,
                        "original child identity changed during launch capture")
                require(time.monotonic_ns() < transition_deadline,
                        "original exit observation deadline expired")
                text = after.decode("ascii")
                if text[text.rfind(") ") + 2:].split()[0] == "Z":
                    break
                require(attempt < 15, "missing executable is not an observed original zombie")
                remaining = (transition_deadline - time.monotonic_ns()) / 1e9
                require(remaining > 0, "original exit observation deadline expired")
                time.sleep(min(0.005, remaining))
                require(time.monotonic_ns() < transition_deadline,
                        "original exit observation deadline expired")
                after = read_bounded(f"/proc/{pid}/stat", 8192)
        refs["after"] = self.original_raw("launch-after.stat", after)
        self.emit("launched", {"identity": self.identity, "stat": refs,
                               "executable": self.launch_executable, "executable_error": exe_error})

    def signal(self, number, phase, outcome):
        self.emit("signal", {"signal": int(number), "phase": phase, "outcome": outcome})

    def exit_observed(self, code, status, kind):
        if self.observed_exit is None:
            self.observed_exit = {"code": code, "waitid_status": status, "waitid_kind": kind,
                                  "observed_ns": time.monotonic_ns()}
            self.emit("exit-observed", self.observed_exit)

    def reaped_child(self, code):
        require(not self.reaped, "original child was already reaped")
        self.reaped, self.exit_code, self.reaped_ns = True, code, time.monotonic_ns()
        self.emit("reaped", {"code": code, "reaped_ns": self.reaped_ns})

    def log_closed(self, path, eof, error):
        with self.lock:
            require(path in self.logs and path not in self.log_state, "original log closure identity differs")
            self.log_state[path] = {"eof": eof, "error_type": error, "closed_ns": time.monotonic_ns()}
            self.emit("log-closed", {"path": path, **self.log_state[path]})

    def snapshot(self):
        with self.lock:
            logs = []
            for path, kind, maximum in self.output_streams():
                ref = None
                state = self.log_state.get(path)
                logs_complete = self.reaped and state is not None and state["eof"] and state["error_type"] is None
                if logs_complete:
                    raw = read_bounded(path, maximum)
                    ref = {"sha256": sha(raw), "bytes": len(raw), "kind": kind}
                logs.append({"path": path, "ref": ref, "complete": logs_complete, "closure": copy.deepcopy(state)})
            return {"index": self.index, "intent": copy.deepcopy(self.intent),
                    "launch_binary": copy.deepcopy(self.launch_binary), "identity": copy.deepcopy(self.identity),
                    "events": copy.deepcopy(self.events), "errors": copy.deepcopy(self.errors),
                    "exit_observed": copy.deepcopy(self.observed_exit), "exit_code": self.exit_code,
                    "reaped": self.reaped, "reaped_ns": self.reaped_ns, "logs": logs}


def capture_controller(journal, deadline_ns, *, allocation_clock):
    """Capture this original controller's identity and isolation, never its env."""
    before = read_bounded('/proc/self/stat', 8192)
    identity = process_identity(before)
    executable = binary_identity('/proc/self/exe', deadline_ns, proc_link=True)
    executable['target'] = os.readlink('/proc/self/exe')
    after = read_bounded('/proc/self/stat', 8192)
    require(process_identity(after) == identity and identity['pid'] == os.getpid(),
            'original controller identity changed during capture')
    raw = {'before': ('controller-before.stat', before, 'process-stat'),
           'after': ('controller-after.stat', after, 'process-stat'),
           'network_dev': ('controller-net-dev.txt', read_bounded('/proc/net/dev', 65536), 'network-dev'),
           'network_ipv4': ('controller-net-route.txt', read_bounded('/proc/net/route', 65536), 'network-route'),
           'network_ipv6': ('controller-net-ipv6.txt', read_bounded('/proc/net/ipv6_route', 65536), 'network-ipv6-route'),
           'boot_id': ('controller-boot-id.txt', read_bounded('/proc/sys/kernel/random/boot_id', 128), 'boot-id')}
    refs = {key: journal.save('captures/' + name, content, kind) for key, (name, content, kind) in raw.items()}
    wall_before = time.monotonic_ns()
    wall = time.time_ns()
    wall_after = time.monotonic_ns()
    value = {'format': 'recall-installed-controller/v2', 'identity': identity, 'executable': executable,
             'network_namespace': os.readlink('/proc/self/ns/net'), 'captures': refs,
             'observed_ns': wall_after, 'deadline_ns': deadline_ns, 'wall_time_ns': wall,
             'wall_sample_before_ns': wall_before, 'wall_sample_after_ns': wall_after,
             'allocation_clock': allocation_clock}
    return journal.save('captures/controller.json', encode(value), 'installed-controller')
