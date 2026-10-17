"""Linux-only supervision for the sequential, synthetic qualification driver.

The phase leader is retained (including as a zombie) until all owned descendants
are gone, preventing its PID/process-group identity from being recycled during
cleanup. The launcher cannot execute the command before its fsynced identity
receipt exists. This module does not supervise arbitrary concurrent children.
"""
from contextlib import contextmanager
import ctypes
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time


def identity(pid):
    try:
        raw = Path(f'/proc/{pid}/stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    fields = raw[raw.rfind(')') + 2:].split()
    return {'pid': int(pid), 'state': fields[0], 'ppid': int(fields[1]),
            'pgid': int(fields[2]), 'sid': int(fields[3]), 'starttime': int(fields[19])}


def same_process(left, right):
    return left is not None and right is not None and all(
        left[k] == right[k] for k in ('pid', 'pgid', 'sid', 'starttime'))


def processes():
    result = {}
    for path in Path('/proc').iterdir():
        if path.name.isdecimal():
            item = identity(int(path.name))
            if item is not None:
                result[item['pid']] = item
    return result


def durable_json(path, value, replace=False):
    path = Path(path)
    temporary = path.with_name(path.name + '.new') if replace else path
    with temporary.open('x') as out:
        json.dump(value, out, sort_keys=True)
        out.write('\n')
        out.flush()
        os.fsync(out.fileno())
    if replace:
        os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def interruptible():
    def stop(signum, _frame):
        raise InterruptedError(f'synthetic driver signal {signum}')
    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


@contextmanager
def defer_launch_signals():
    # Caller assigns the launched Phase while TERM/INT are blocked, then enters
    # its cleanup-owning try/finally before a pending signal can be delivered.
    previous = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGTERM, signal.SIGINT})
    try:
        yield
    finally:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous)


@contextmanager
def cleanup_signals():
    # Repeated TERM/INT cannot interrupt owned-child cleanup. SIGKILL remains an
    # external-controller responsibility, using the durable phase identities.
    previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGTERM, signal.SIGINT)}
    try:
        yield
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


def enable_subreaper():
    if sys.platform != 'linux' or not hasattr(os, 'pidfd_open'):
        raise RuntimeError('synthetic process custody requires Linux pidfds')
    if any(row['ppid'] == os.getpid() for row in processes().values()):
        raise RuntimeError('synthetic phase requires exclusive child ownership')
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), 'cannot enable child subreaper')


class Phase:
    def __init__(self, command, root, name, env, log):
        enable_subreaper()
        self.root = Path(root)
        self.name = name
        self.members = {}
        self.boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
        self.supervisor = identity(os.getpid())
        self.process = None
        read_fd, write_fd = os.pipe()
        try:
            self.process = subprocess.Popen(
                [sys.executable, __file__, '_launch', str(read_fd), *map(str, command)],
                pass_fds=(read_fd,), stdout=log, stderr=subprocess.STDOUT,
                env=env, start_new_session=True)
            os.close(read_fd)
            read_fd = None
            self.leader = identity(self.process.pid)
            if self.leader is None or self.leader['pgid'] != self.process.pid or self.leader['sid'] != self.process.pid:
                raise RuntimeError('synthetic phase leader identity unavailable')
            self.members[self.leader['pid']] = self.leader
            durable_json(self.root / f'{name}.process.json', {
                'format': 'recall-synthetic-phase-process-v1', 'boot_id': self.boot_id,
                'supervisor': self.supervisor, 'leader': self.leader,
                'command': list(map(str, command)), 'receipt_before_command_exec': True})
            self.observe()
            os.write(write_fd, b'G')
        except BaseException as original:
            if self.process is not None:
                try:
                    if hasattr(self, 'leader') and self.leader is not None:
                        self.close(terminate=True)
                    else:
                        self.process.kill()
                        self.process.wait()
                except BaseException as cleanup_error:
                    original.add_note(f'owned launch cleanup also failed: {cleanup_error!r}')
            raise
        finally:
            if read_fd is not None:
                os.close(read_fd)
            os.close(write_fd)

    def discover(self):
        # Discovery and signalling must remain usable after ENOSPC/fsync failure.
        rows = processes()
        current = rows.get(self.leader['pid'])
        if not same_process(current, self.leader):
            raise RuntimeError('phase leader identity changed before owned cleanup')
        owned = {pid: row for pid, row in rows.items()
                 if same_process(self.members.get(pid), row)}
        # A sequential subreaper has no unrelated child processes. Newly adopted
        # descendants remain attributable even after their parents exit/setsid.
        owned.update({pid: row for pid, row in rows.items()
                      if row['ppid'] == self.supervisor['pid']
                      and row['starttime'] >= self.leader['starttime']})
        while True:
            extra = {pid: row for pid, row in rows.items() if row['ppid'] in owned}
            if not (extra.keys() - owned.keys()):
                break
            owned.update(extra)
        self.members.update(owned)
        return owned

    def observe(self):
        owned = self.discover()
        durable_json(self.root / f'{self.name}.members.json', {
            'boot_id': self.boot_id, 'supervisor': self.supervisor,
            'leader': self.leader, 'members': list(self.members.values())}, replace=True)
        return owned

    def finished(self):
        return self.observe()[self.leader['pid']]['state'] == 'Z'

    def close(self, terminate=False, grace=10.0):
        with cleanup_signals():
            return self._close(terminate, grace)

    def record_closure(self, closure):
        try:
            durable_json(self.root / f'{self.name}.closure.json', closure)
        except BaseException as error:
            # Cleanup already completed or exhausted its bound; never let an
            # evidence-write failure prevent signalling or reaping.
            closure['receipt_error'] = repr(error)
        return closure

    def _close(self, terminate, grace):
        owned = self.discover()
        descendant_at_exit = any(pid != self.leader['pid'] and row['state'] != 'Z'
                                 for pid, row in owned.items())
        needs_stop = terminate or descendant_at_exit
        term_until = time.monotonic() + grace
        kill_until = term_until + grace
        while True:
            owned = self.discover()
            for pid, row in owned.items():
                if pid == self.leader['pid']:
                    continue
                if row['state'] == 'Z' and row['ppid'] == os.getpid():
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except ChildProcessError:
                        pass
            owned = self.discover()
            descendants = {pid: row for pid, row in owned.items() if pid != self.leader['pid']}
            leader_done = owned[self.leader['pid']]['state'] == 'Z'
            if leader_done and not descendants:
                break
            if needs_stop:
                signum = signal.SIGTERM if time.monotonic() < term_until else signal.SIGKILL
                for row in owned.values():
                    signal_exact(row, signum)
            if time.monotonic() >= kill_until:
                self.record_closure({
                    'state': 'OWNED_PROCESSES_REMAIN', 'leader': self.leader,
                    'remaining': list(owned.values()), 'all_owned_processes_absent': False})
                raise RuntimeError('owned phase processes remain after cleanup')
            time.sleep(0.02)
        code = self.process.wait()  # Reap the leader last; no early poll/wait.
        remaining = [row for pid, old in self.members.items()
                     if (row := identity(pid)) is not None and same_process(row, old)]
        closure = {'state': 'CLOSED' if not remaining else 'OWNED_PROCESSES_REMAIN',
                   'leader': self.leader, 'exit_code': code, 'remaining': remaining,
                   'all_owned_processes_absent': not remaining,
                   'unexpected_live_descendants_at_exit': descendant_at_exit and not terminate}
        self.record_closure(closure)
        if remaining:
            raise RuntimeError('owned phase process survives wait')
        return closure


def signal_exact(expected, signum):
    """Signal an exact current identity through a pidfd, never an unbound PGID."""
    if not same_process(identity(expected['pid']), expected):
        return False
    try:
        fd = os.pidfd_open(expected['pid'])
    except ProcessLookupError:
        return False
    try:
        if not same_process(identity(expected['pid']), expected):
            return False
        try:
            signal.pidfd_send_signal(fd, signum)
        except ProcessLookupError:
            return False
        return True
    finally:
        os.close(fd)


def finish_result(root, payload, started, deadline, save):
    """Write semantic evidence first, then time the final verification/fsync.

    A timed result never treats an over-budget semantic success as performance
    qualification. The caller/controller must also require successful exit and
    complete process closure.
    """
    save(Path(root) / 'verified-output.json', payload)
    finished = time.monotonic()
    if finished > deadline:
        raise RuntimeError('overall admission deadline exceeded by final verification')
    payload = dict(payload, seconds_including_generation=finished - started,
                   verification_and_evidence_fsync_within_deadline=True)
    save(Path(root) / 'result.json', payload)
    if time.monotonic() > deadline:
        raise RuntimeError('overall admission deadline exceeded by terminal receipt')


if __name__ == '__main__':
    if len(sys.argv) < 4 or sys.argv[1] != '_launch':
        raise SystemExit('private synthetic process launcher')
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGTERM, signal.SIGINT})
    fd = int(sys.argv[2])
    token = os.read(fd, 1)
    os.close(fd)
    if token != b'G':
        raise SystemExit(125)
    os.execvpe(sys.argv[3], sys.argv[3:], os.environ)
