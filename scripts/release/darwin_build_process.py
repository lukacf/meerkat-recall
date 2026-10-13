"""Darwin build group custody; imports and mocked controls are portable.

ABI references: Apple xnu bsd/sys/proc_info.h and
libsyscall/wrappers/libproc/libproc.c. proc_listpgrppids returns PID count,
not bytes. Only the original, unreaped child group receives signals.
"""
from __future__ import annotations

import ctypes
import errno
import os
from pathlib import Path
import select
import signal
import subprocess
import threading
import time


class TaskInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint64) for name in
                ("virtual", "resident", "user", "system", "threads_user", "threads_system")] + [
                ("counters", ctypes.c_int32 * 12)]


class ShortInfo(ctypes.Structure):
    _fields_ = [(name, ctypes.c_uint32) for name in ("pid", "ppid", "pgid", "status")] + [
        ("comm", ctypes.c_char * 16), ("rest", ctypes.c_uint32 * 8)]


class GroupObserver:
    def __init__(self):
        self.lib = ctypes.CDLL("/usr/lib/libproc.dylib", use_errno=True)
        self.lib.proc_listpgrppids.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        self.lib.proc_listpgrppids.restype = ctypes.c_int
        self.lib.proc_pidinfo.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
                                         ctypes.c_void_p, ctypes.c_int]
        self.lib.proc_pidinfo.restype = ctypes.c_int

    def sample(self, group):
        # Fixed bound includes spare slots; a full result is not a complete list.
        array = (ctypes.c_int * 2048)()
        ctypes.set_errno(0)
        count = self.lib.proc_listpgrppids(group, array, ctypes.sizeof(array))
        if not 0 <= count < len(array) or (count == 0 and ctypes.get_errno()):
            raise ValueError("owned process-group inventory unavailable or over bound")
        live, uncertain, resident = [], [], 0
        for pid in array[:count]:
            if pid <= 0:
                raise ValueError("invalid owned process-group inventory")
            info = ShortInfo()
            ctypes.set_errno(0)
            size = self.lib.proc_pidinfo(pid, 13, 0, ctypes.byref(info), ctypes.sizeof(info))
            if size != ctypes.sizeof(info):
                if ctypes.get_errno() == errno.ESRCH:
                    continue
                uncertain.append(pid)
                continue
            # An exiting member may leave and its PID may be recycled. Never
            # signal individual sampled PIDs, or count a now-foreign process.
            if info.pid != pid or info.pgid != group:
                continue
            if info.status == 5:  # SZOMB: exited, waiting for parent reaping.
                continue
            live.append(pid)
            task = TaskInfo()
            size = self.lib.proc_pidinfo(pid, 4, 0, ctypes.byref(task), ctypes.sizeof(task))
            if size == ctypes.sizeof(task):
                resident += task.resident
            else:
                again = ShortInfo()
                ctypes.set_errno(0)
                size = self.lib.proc_pidinfo(pid, 13, 0, ctypes.byref(again), ctypes.sizeof(again))
                gone = (size == 0 and ctypes.get_errno() == errno.ESRCH)
                departed = size == ctypes.sizeof(again) and (again.pgid != group or again.status == 5)
                if gone or departed:
                    live.remove(pid)
                else:
                    uncertain.append(pid)
        return {"live_pids": live, "uncertain_pids": uncertain, "resident_bytes": resident}


class BuildProcess:
    """Popen.poll/wait are forbidden until final group cleanup and observation.

    A registered handle is retained even when launch, log draining or observation
    fails. Cleanup never sends a signal after reaping the group leader.
    """
    def __init__(self, observer):
        self.observer = observer
        self.process = None
        self.events = None
        self.reaped = False
        self.exited = False
        self.group_empty = False
        self.failure = threading.Event()
        self.thread = None
        self.log = None
        self.bytes = 0
        self.last_sample = None
        self.stage = "not-launched"
        self.signal_error = None
        self.first_cleanup_sample = None
        self.successful_group_signal = None

    def launch(self, command, cwd, environment, log, interpreter, cap, *, registered=None):
        self.stage = "launch"
        self.events = select.kqueue()
        read_fd, write_fd = os.pipe()
        try:
            descriptor = os.open(log, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            self.log = os.fdopen(descriptor, "wb")
            child = Path(__file__).with_name("darwin_build_child.py")
            self.process = subprocess.Popen([str(interpreter), "-I", "-S", str(child),
                "--await-parent", str(read_fd), *command], cwd=cwd, env=environment,
                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                close_fds=True, pass_fds=(read_fd,), start_new_session=True)
            # The child remains blocked on its pipe until its exact original
            # PID/expected PGID is durably recorded by the existing supervisor.
            self.stage = "register-custody"
            if registered is not None:
                registered(self)
            # Close before registration/release: an early child exit must make
            # the release fail with EPIPE, not succeed against our own reader.
            os.close(read_fd)
            read_fd = None
            event = select.kevent(self.process.pid, filter=select.KQ_FILTER_PROC,
                flags=select.KQ_EV_ADD | select.KQ_EV_ENABLE | select.KQ_EV_ONESHOT,
                fflags=select.KQ_NOTE_EXIT)
            self.events.control([event], 0, 0)
            self.thread = threading.Thread(target=self._drain, args=(cap,), daemon=True)
            self.thread.start()
            if os.write(write_fd, b"G") != 1:
                raise ValueError("build child release failed")
            self.stage = "running"
        finally:
            if read_fd is not None:
                os.close(read_fd)
            os.close(write_fd)

    def _drain(self, cap):
        try:
            while data := self.process.stdout.read(65536):
                available = max(0, cap - self.bytes)
                self.log.write(data[:available])
                self.bytes += len(data)
                if self.bytes > cap:
                    self.failure.set()
            self.log.flush()
            os.fsync(self.log.fileno())
        except BaseException:
            self.failure.set()
        finally:
            self.process.stdout.close()
            self.log.close()

    def done(self):
        if self.reaped:
            return True
        if not self.exited:
            for event in self.events.control(None, 1, 0):
                if (event.flags & select.KQ_EV_ERROR or event.ident != self.process.pid
                        or not event.fflags & select.KQ_NOTE_EXIT):
                    raise ValueError("original build child exit observation failed")
                self.exited = True
        return self.exited

    def kill_owned(self):
        if self.process is not None and not self.reaped:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.successful_group_signal = {"pgid": self.process.pid, "signal": int(signal.SIGKILL),
                                                "leader_reaped": False}
            except ProcessLookupError:
                pass

    def cleanup(self, *, deadline):
        if self.reaped:
            self._finish_capture(deadline)
            return self.process.returncode
        if self.process is None:
            if self.log is not None:
                self.log.close()
            if self.events is not None:
                self.events.close()
            self.group_empty = True
            return None
        # Keep the leader unreaped while observing and, if necessary, repeating
        # a group signal. A live group member or uncertain observation holds lane.
        while True:
            self.stage = "cleanup-observation"
            sample = self.last_sample = self.observer.sample(self.process.pid)
            if self.first_cleanup_sample is None:
                self.first_cleanup_sample = sample
            if not sample["live_pids"] and not sample["uncertain_pids"]:
                self.group_empty = True
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("owned build group cleanup unconfirmed; retain Mac lane")
            self.stage = "cleanup-signal"
            try:
                self.kill_owned()
            except PermissionError as error:
                # Darwin may return EPERM for an already exited zombie-only
                # group. A fresh complete native sample is the only basis for
                # accepting that race; permission failure itself proves nothing.
                self.signal_error = {"type": type(error).__name__, "errno": error.errno}
                if error.errno != errno.EPERM:
                    raise
                self.stage = "cleanup-signal-recheck"
                sample = self.last_sample = self.observer.sample(self.process.pid)
                if sample["live_pids"] or sample["uncertain_pids"]:
                    raise
                self.group_empty = True
                break
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
        self.stage = "cleanup-reap"
        result = self.process.wait(timeout=max(0, deadline - time.monotonic()))
        self.reaped = True
        self.events.close()
        self._finish_capture(deadline)
        return result

    def _finish_capture(self, deadline):
        self.stage = "cleanup-capture"
        # A failed Thread.start leaves a handle that cannot be joined. The
        # original child still belongs to cleanup, as do its idle pipe and log.
        if self.thread is not None and self.thread.ident is not None:
            self.thread.join(timeout=min(2, max(0, deadline - time.monotonic())))
            if self.thread.is_alive():
                self.group_empty = False
                raise RuntimeError("build log pipe still open; retain Mac lane")
        # No drainer can hold these streams now. Retry this after a reaped-child
        # cleanup failure too; successful leader reaping alone is not closure.
        self.process.stdout.close()
        if self.log is not None:
            self.log.close()
        self.group_empty = True
        self.stage = "closed"

    def custody(self):
        return {"child_pid": self.process.pid if self.process is not None else None,
                "expected_pgid": self.process.pid if self.process is not None else None,
                "exit_observed": self.exited, "leader_reaped": self.reaped,
                "group_cleanup_confirmed": self.group_empty and self.reaped,
                "stage": self.stage, "last_group_sample": self.last_sample,
                "signal_error": self.signal_error, "first_cleanup_sample": self.first_cleanup_sample,
                "successful_group_signal": self.successful_group_signal}
