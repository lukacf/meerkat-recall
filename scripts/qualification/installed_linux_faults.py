#!/usr/bin/env python3
"""Bounded installed durability faults; default invocation only prints a plan.

The reviewed allocation, artifact verification and private namespace are required
before execution. Library fault injection and canonical migration rollback remain
separate evidence; this driver never upgrades those into installed observations.
"""
from __future__ import annotations

import argparse
import base64
import datetime as dt
import errno
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import re
import signal
import socket
import stat
import subprocess
import sys
import threading
import time
import urllib.parse

ROOT = Path(__file__).resolve().parents[2]
CASES = ROOT / 'tests/recall_preview_semantics/installed_linux_faults_cases.json'
DEPENDENCIES = ('scripts/qualification/installed_linux_offline.py',
                'scripts/qualification/recall_preview.py', 'scripts/release/artifact.py',
                'tests/recall_preview/fixtures.py', 'tests/recall_preview/corpus.json',
                'tests/recall_preview/matrix.py', 'tests/recall_preview/oracle.py')
QUOTA = 64 * 1024 * 1024
MAX_LOG = 2 * 1024 * 1024
MAX_TRACE = 8 * 1024 * 1024
SPACE = 'qualification-durability'


class UnmetPrerequisite(Exception):
    """An unexecuted case is never a passing observation."""


def require(condition, message):
    if not condition:
        raise ValueError(message)


def encoded(value):
    return json.dumps(value, sort_keys=True, allow_nan=False).encode()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, 'duplicate JSON field')
            result[key] = value
        return result
    def invalid(_value):
        raise ValueError('nonfinite JSON number')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b''):
            result.update(chunk)
    return result.hexdigest()


def private_new(path, data):
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') as output:
        output.write(data)
        output.flush()
        os.fsync(output.fileno())


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    require(spec is not None and spec.loader is not None, 'reviewed dependency unavailable')
    result = importlib.util.module_from_spec(spec)
    sys.modules[name] = result
    spec.loader.exec_module(result)
    return result


def utilities():
    # Only the frozen public MCP decoder, deadline/parser and installer APIs are
    # consumed. No coupling to another qualification driver's private processes.
    core = module('fault_installed_public', ROOT / DEPENDENCIES[0])
    return core.mcp_value, core.capture, core.artifact


def plan():
    return {'format': 'recall-installed-fault-plan/v1', 'product_acceptance': False,
            'execution': 'not allocated by a plan', 'expectations': strict_json(CASES.read_bytes()),
            'expectations_sha256': digest(CASES), 'driver_sha256': digest(__file__),
            'dependency_sha256': {path: digest(ROOT / path) for path in DEPENDENCIES}}


def utc(text):
    value = dt.datetime.fromisoformat(text.replace('Z', '+00:00'))
    require(value.tzinfo is not None, 'deadline must have timezone')
    return value


def validate_allocation(path, now=None):
    allocation = strict_json(Path(path).read_bytes())
    require(allocation.get('format') == 'recall-installed-fault-run/v1'
            and allocation.get('execution_authorized') is True, 'execution is not allocated')
    require((now or dt.datetime.now(dt.timezone.utc)) < utc(allocation['expires_at']), 'allocation expired')
    for field, size in [('source_commit', 40), ('archive_sha256', 64), ('binary_sha256', 64),
                        ('review_sha256', 64)]:
        require(isinstance(allocation.get(field), str)
                and re.fullmatch('[a-f0-9]{' + str(size) + '}', allocation[field]), 'exact ' + field + ' required')
    for field in ('archive', 'run_directory', 'review'):
        require(Path(allocation[field]).is_absolute(), field + ' must be absolute')
    review_path = Path(allocation['review'])
    require(digest(review_path) == allocation['review_sha256'], 'independent review digest differs')
    review, inventory = strict_json(review_path.read_bytes()), plan()
    require(review.get('format') == 'recall-installed-fault-review/v1'
            and review.get('reviewer') == 'qualification' and review.get('approved') is True,
            'independent fault expectations not reviewed')
    for field in ('expectations_sha256', 'driver_sha256', 'dependency_sha256'):
        require(review.get(field) == inventory[field], 'reviewed ' + field + ' differs')
    if allocation.get('paired_bundle') is not None:
        require(Path(allocation['paired_bundle']).is_absolute(), 'paired bundle must be absolute')
        for field in ('paired_binary_sha256', 'paired_source_sha256', 'paired_build_sha256'):
            require(re.fullmatch('[a-f0-9]{64}', allocation.get(field, '')), 'trusted ' + field + ' required')
    return allocation, inventory


def namespace_command(root, run, manifest, paired=None):
    arguments = ['/usr/bin/bwrap', '--unshare-user', '--unshare-net', '--unshare-pid',
                 '--unshare-ipc', '--unshare-uts', '--die-with-parent', '--new-session',
                 '--clearenv', '--cap-add', 'CAP_NET_ADMIN', '--proc', '/proc', '--dev', '/dev']
    for path in ('/usr', '/bin', '/lib', '/lib64'):
        if Path(path).exists():
            arguments += ['--ro-bind', path, path]
    if Path('/etc/ld.so.cache').exists():
        arguments += ['--ro-bind', '/etc/ld.so.cache', '/etc/ld.so.cache']
    arguments += ['--ro-bind', str(root), '/reviewed', '--bind', str(run), '/qualification',
                  '--size', str(QUOTA), '--tmpfs', '/fault-live',
                  '--size', str(8 * 1024 * 1024), '--tmpfs', '/tmp',
                  '--setenv', 'PATH', '/usr/bin:/bin', '--setenv', 'LANG', 'C.UTF-8',
                  '--setenv', 'PYTHONDONTWRITEBYTECODE', '1', '--chdir', '/qualification']
    if paired is not None:
        arguments += ['--ro-bind', str(paired), '/paired']
    return arguments + ['--', '/usr/bin/python3', '/reviewed/scripts/qualification/installed_linux_faults.py',
                        '--inside', str(manifest)]


def isolation(parent):
    require(sys.platform == 'linux', 'Linux namespace required')
    current = {kind: os.readlink('/proc/self/ns/' + kind) for kind in ('user', 'mnt', 'net')}
    require(all(current[key] != parent[key] for key in current), 'namespace was not independently created')
    # This capability belongs only to the newly created network namespace.
    import fcntl
    import struct
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as control:
        flags = fcntl.ioctl(control.fileno(), 0x8913, struct.pack('16sh', b'lo', 0))
        old = struct.unpack('16sh', flags[:18])[1]
        fcntl.ioctl(control.fileno(), 0x8914, struct.pack('16sh', b'lo', old | 1))
    require({name for _, name in socket.if_nameindex()} == {'lo'}, 'external network interface present')
    require(len(Path('/proc/net/route').read_text().splitlines()) == 1, 'external IPv4 route present')
    ipv6 = Path('/proc/net/ipv6_route')
    require(not ipv6.exists() or all(row.split()[-1] == 'lo' for row in ipv6.read_text().splitlines()),
            'external IPv6 route present')
    mounts = [row.split() for row in Path('/proc/self/mountinfo').read_text().splitlines()]
    matches = [row for row in mounts if row[4] == '/fault-live']
    require(len(matches) == 1 and matches[0][matches[0].index('-') + 1] == 'tmpfs', 'quota path is not a tmpfs mount')
    info = os.statvfs('/fault-live')
    require(0 < info.f_blocks * info.f_frsize <= QUOTA, 'tmpfs exceeds allocated quota')
    require(os.stat('/qualification').st_dev != os.stat('/fault-live').st_dev,
            'original journal and evidence must be outside the full filesystem')
    import resource
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    resource.setrlimit(resource.RLIMIT_AS, (4 * 1024**3, 4 * 1024**3))
    allowed = sorted(os.sched_getaffinity(0))[:2]
    os.sched_setaffinity(0, allowed)
    os.nice(10)
    return {'cpu_affinity': allowed, 'address_space_bytes': 4 * 1024**3,
            'namespaces': current, 'parent_namespaces': parent, 'tmpfs_bytes': info.f_blocks * info.f_frsize,
            'mount': '/fault-live', 'network_interfaces': ['lo']}


def fill_quota(path, limit=QUOTA):
    """Write only our new filler; never truncate an existing file or host volume."""
    total = 0
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        while True:
            require(total < limit, 'filler reached allocation without observed ENOSPC')
            try:
                count = os.write(descriptor, b'F' * min(65536, limit - total))
                require(count > 0, 'filler stopped without ENOSPC')
                total += count
            except OSError as error:
                if error.errno != errno.ENOSPC:
                    raise
                os.fsync(descriptor)
                return {'filler_bytes': total, 'errno': error.errno,
                        'free_bytes': os.statvfs(Path(path).parent).f_bavail * os.statvfs(Path(path).parent).f_frsize}
    finally:
        os.close(descriptor)


def enospc_writes(raw, directory):
    """Require kernel ENOSPC on the candidate's actual bounded save destination."""
    prefix = re.escape(str(directory)) + r'/[^>\n]*'
    expression = r'\b(?:write|pwrite64)\(\d+<' + prefix + r'>.*= -1 ENOSPC\b'
    return [row for row in raw.decode('utf-8', errors='replace').splitlines() if re.search(expression, row)]


MANAGEMENT_SCOPES = ['manage:grant', 'read:system', 'write:system', 'read:space_config',
                     'write:space_config', 'read:status']
SERVICE_SCOPES = ['read:entity', 'write:entity', 'read:status']


def credential_arguments(key, clearance, output, role):
    require(role in ('operator', 'service'), 'unknown synthetic credential role')
    arguments = ['service-token', 'issue', '--secret-file', str(key), '--subject', 'fault-' + role,
                 '--expires-in', '1800', '--issuer', 'fault-driver', '--audience', 'fault-driver',
                 '--output', str(output)]
    if role == 'operator':
        arguments += ['--global-admin']
        scopes = MANAGEMENT_SCOPES
    else:
        arguments += ['--space', SPACE, '--clearances-file', str(clearance)]
        scopes = SERVICE_SCOPES
    for scope in scopes:
        arguments += ['--scope', scope]
    return arguments


class Child:
    """Own an unreaped process group and bounded private stdout/stderr drains."""
    def __init__(self, command, cwd, environment, directory, serial, trace=None):
        self.paths = [directory / f'child-{serial:03d}.{name}' for name in ('stdout', 'stderr')]
        self.logs = [os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'wb') for path in self.paths]
        self.failure, self.lock, self.reaped = threading.Event(), threading.Lock(), False
        self.trace, self.done = trace, threading.Event()
        try:
            self.process = subprocess.Popen(command, cwd=cwd, env=environment, stdin=subprocess.DEVNULL,
                                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
        except BaseException:
            for log in self.logs:
                log.close()
            raise
        self.pid = self.process.pid
        self.threads = []
        for source, output in zip((self.process.stdout, self.process.stderr), self.logs):
            thread = threading.Thread(target=self.drain, args=(source, output), daemon=True)
            thread.start()
            self.threads.append(thread)
        self.monitor = threading.Thread(target=self.monitor_trace, daemon=True)
        self.monitor.start()

    def monitor_trace(self):
        while not self.done.wait(0.025):
            try:
                if self.trace is not None and self.trace.exists() and self.trace.stat().st_size > MAX_TRACE:
                    self.failure.set()
                    self.signal(signal.SIGKILL)
                    return
            except OSError:
                self.failure.set()
                self.signal(signal.SIGKILL)
                return

    def drain(self, source, output):
        remaining = MAX_LOG
        try:
            while chunk := source.read1(8192):
                output.write(chunk[:remaining])
                if len(chunk) > remaining:
                    self.failure.set()
                    self.signal(signal.SIGKILL)
                    break
                remaining -= len(chunk)
        except (OSError, ValueError):
            self.failure.set()
            self.signal(signal.SIGKILL)
        finally:
            source.close()
            output.close()

    def signal(self, kind):
        with self.lock:
            if not self.reaped:
                try:
                    os.killpg(self.pid, kind)
                except ProcessLookupError:
                    pass

    def poll(self):
        with self.lock:
            if self.reaped:
                return self.process.returncode
            event = os.waitid(os.P_PID, self.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
            return None if event is None else event.si_status if event.si_code == os.CLD_EXITED else -event.si_status

    def wait(self, seconds):
        end = time.monotonic() + seconds
        while self.poll() is None:
            if time.monotonic() >= end:
                raise subprocess.TimeoutExpired('owned fault child', seconds)
            time.sleep(0.025)
        self.signal(signal.SIGKILL)
        self.done.set()
        self.monitor.join(timeout=1)
        for thread in self.threads:
            thread.join(timeout=2)
        with self.lock:
            code = self.process.wait(timeout=3)
            self.reaped = True
        require(not self.failure.is_set() and not any(thread.is_alive() for thread in self.threads),
                'bounded child output failed')
        return code


class Driver:
    def __init__(self, allocation, inventory, isolated):
        self.allocation, self.inventory = allocation, inventory
        self.root, self.live = Path('/qualification'), Path('/fault-live')
        self.binary = self.root / 'installed/meerkat-recall'
        self.cwd = self.live / 'service'
        self.cwd.mkdir(mode=0o700)
        for name in ('journal', 'backups', 'captures', 'private', 'negative'):
            (self.root / name).mkdir(mode=0o700)
        self.checkpoint = self.live / 'state.snapshot'
        self.wal = self.live / 'state.snapshot.durable/commits.wal'
        self.config = self.cwd / '.elephant/elephant_config.toml'
        self.started = time.monotonic()
        self.serial, self.calls, self.saved = 0, 0, 0
        self.children, self.service, self.trace = [], None, None
        self.candidate_pidfd = None
        self.tokens = {}
        self.mcp_value, self.capture, _artifact = utilities()
        self.environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'RUST_LOG': 'warn',
                            'HOME': str(self.root / 'private'), 'TOKIO_WORKER_THREADS': '4', 'RAYON_NUM_THREADS': '2', 'ELEPHANT_DURABILITY_PATH': str(self.root / 'journal'),
                            'ELEPHANT_SNAPSHOT_DIR': str(self.root / 'backups')}
        self.report = {'format': 'recall-installed-fault-observations/v1', 'product_acceptance': False,
                       'source_commit': allocation['source_commit'], 'binary_sha256': allocation['binary_sha256'],
                       'plan': inventory, 'isolation': isolated, 'cases': [],
                       'separate_evidence_required': inventory['expectations']['separate_evidence_required']}
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            self.port = reservation.getsockname()[1]
        self.entities = []

    def remaining(self):
        require(dt.datetime.now(dt.timezone.utc) < utc(self.allocation['expires_at']), 'allocation expired')
        value = self.inventory['expectations']['limits']['total_seconds'] - (time.monotonic() - self.started)
        require(value > 0, 'fault allocation deadline exceeded')
        if self.trace and self.trace.exists():
            require(self.trace.stat().st_size <= MAX_TRACE, 'private syscall trace exceeded bound')
        return value

    def save(self, name, value):
        raw = encoded(value)
        self.saved += len(raw)
        require(self.saved <= 16 * 1024 * 1024, 'observation storage limit exceeded')
        private_new(self.root / 'captures' / name, raw)

    def case(self, name, action):
        require(name in self.inventory['expectations']['required_cases'], 'unreviewed case')
        row = {'case': name, 'started_monotonic': time.monotonic()}
        self.report['cases'].append(row)
        try:
            row['observed'] = action()
            row['verdict'] = 'pass'
        except UnmetPrerequisite as error:
            row.update(verdict='unmet_prerequisite', reason=str(error))
        except BaseException as error:
            row.update(verdict='fail', error_type=type(error).__name__, reason=str(error)[:512])
            raise
        finally:
            row['ended_monotonic'] = time.monotonic()
            self.save('case-' + name + '.json', row)

    def process(self, arguments, *, binary=None, cwd=None, environment=None, serve=False, trace=False, expected=0):
        self.remaining()
        self.serial += 1
        require(self.serial <= 32, 'owned process count exceeded')
        command = ['/usr/bin/setpriv', '--no-new-privs', '--bounding-set=-all', '--', str(binary or self.binary), *arguments]
        trace_path = None
        if trace:
            self.trace = self.root / 'captures' / f'syscalls-{self.serial:03d}.log'
            private_new(self.trace, b'')
            trace_path = self.trace
            command = ['/usr/bin/strace', '-f', '-qq', '-yy', '-s', '80', '-e',
                       'trace=write,pwrite64,fsync,fdatasync,rename,renameat,renameat2', '-o', str(self.trace), *command]
        child = Child(command, cwd or self.cwd, environment or self.environment, self.root / 'captures', self.serial, trace_path)
        self.children.append(child)
        if serve:
            return child
        try:
            code = child.wait(min(30, self.remaining()))
        except BaseException:
            child.signal(signal.SIGKILL)
            child.wait(5)
            raise
        require(expected is None or code == expected, 'CLI exit differs from required outcome')
        return {'exit_code': code, 'stdout': child.paths[0].read_bytes(), 'stderr': child.paths[1].read_bytes()}

    def http(self, method, path, body=None, space=None, role='operator'):
        self.calls += 1
        require(self.calls <= 150, 'HTTP call bound exceeded')
        headers = {'Accept': 'application/json, text/event-stream'}
        if role in self.tokens:
            headers['Authorization'] = 'Bearer ' + self.tokens[role]
        if space:
            headers['X-Space-Id'] = space
        payload = None if body is None else encoded(body)
        if payload is not None:
            headers['Content-Type'] = 'application/json'
        timeout = min(10, self.remaining())
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=timeout)
        try:
            with self.capture.total_deadline(timeout):
                connection.request(method, path, payload, headers)
                response = connection.getresponse()
                raw = response.read(1024 * 1024 + 1)
                require(len(raw) <= 1024 * 1024, 'HTTP body exceeded bound')
                lengths = response.headers.get_all('Content-Length', [])
                transfers = response.headers.get_all('Transfer-Encoding', [])
                require(not lengths or len(lengths) == 1 and lengths[0].isdigit() and int(lengths[0]) == len(raw),
                        'incomplete HTTP framing')
                require(not transfers or not lengths and transfers == ['chunked'], 'ambiguous HTTP framing')
                status = response.status
        finally:
            connection.close()
        value = self.mcp_value(raw, body['id']) if path == '/mcp/full' else self.capture.strict_json_body(raw)
        self.save(f'http-{self.calls:03d}.json', {'method': method, 'path': path, 'space': space,
                  'request': body, 'status': status, 'raw_body_base64': base64.b64encode(raw).decode()})
        return status, value

    def tool(self, name, arguments, *, space=None, failure=False, role='operator'):
        status, value = self.http('POST', '/mcp/full', {'jsonrpc': '2.0', 'id': self.calls + 1,
            'method': 'tools/call', 'params': {'name': name, 'arguments': arguments}}, space, role=role)
        require(status == 200, 'MCP tool transport failed')
        content = value.get('result', {})
        texts = [block['text'] for block in content.get('content', []) if block.get('type') == 'text']
        parsed = strict_json(texts[0]) if len(texts) == 1 else None
        failed = 'error' in value or content.get('isError') is True or isinstance(parsed, dict) and parsed.get('ok') is False
        require(failed == failure, 'MCP tool result differs from expected success/failure')
        if failure:
            return value
        require(parsed is not None, 'typed tool result absent')
        return parsed['data'] if isinstance(parsed, dict) and parsed.get('ok') is True else parsed

    def launch(self):
        self.service = self.process(['serve', '--server-only', '--shutdown-timeout-seconds', '15'], serve=True, trace=True)
        self.candidate_pidfd = None
        end = time.monotonic() + min(30, self.remaining())
        while time.monotonic() < end:
            if self.service.poll() is not None:
                wire = b''.join(path.read_bytes() for path in self.service.paths)
                if b'ptrace' in wire.lower() or b'operation not permitted' in wire.lower():
                    raise UnmetPrerequisite('owned syscall tracing unavailable; no OS-ENOSPC case executed')
                raise ValueError('installed service exited before liveness')
            if self.candidate_pidfd is None:
                children = Path(f'/proc/{self.service.pid}/task/{self.service.pid}/children').read_text().split()
                for pid in children:
                    try:
                        if Path(f'/proc/{pid}/exe').resolve() == self.binary.resolve():
                            self.candidate_pidfd = os.pidfd_open(int(pid))
                            break
                    except ProcessLookupError:
                        pass
            try:
                if self.http('GET', '/v1/health')[0] == 200:
                    require(self.candidate_pidfd is not None, 'owned traced candidate identity absent')
                    return
            except (ConnectionRefusedError, ConnectionResetError):
                pass
            time.sleep(0.1)
        raise ValueError('installed service failed to become live')

    def ready(self):
        status, body = self.http('GET', '/v1/readiness')
        require(status == 200 and body.get('ready') is True and body.get('storage_ready') is True
                and body.get('recovery_ready') is True and body.get('draining') is False, 'recovery barrier is not ready')
        return body

    def stop(self, kill=False, clean=True):
        require(self.service is not None, 'no owned service')
        child = self.service
        require(self.candidate_pidfd is not None, 'owned candidate process identity absent')
        # Signal the exact installed process, allowing its parent tracer to drain
        # and report its exit. Killing the tracer group would hide graceful save.
        signal.pidfd_send_signal(self.candidate_pidfd, signal.SIGKILL if kill else signal.SIGTERM)
        code = child.wait(min(22, self.remaining()))
        os.close(self.candidate_pidfd)
        self.candidate_pidfd = None
        self.service = None
        require(not clean or code == 0, 'installed graceful checkpoint did not succeed')
        return {'exit_code': code, 'stdout': child.paths[0].read_text(), 'stderr': child.paths[1].read_text()}

    def journal(self):
        values = {name: (self.root / 'journal' / name).read_text().strip()
                  for name in ('journal-id', 'reference-key-fingerprint')}
        require(re.fullmatch('[0-9a-f-]{36}', values['journal-id'])
                and re.fullmatch('[0-9a-f]{64}', values['reference-key-fingerprint']), 'original journal identity invalid')
        return values

    def witness(self):
        checkpoint = strict_json(self.checkpoint.read_bytes())
        require(checkpoint['format'] == 'recall-embedded-checkpoint-v1', 'not a private live checkpoint')
        return {'checkpoint_path': str(self.checkpoint), 'checkpoint_sha256': digest(self.checkpoint),
                'checkpoint_sequence': checkpoint['sequence'], 'checkpoint_store_id': checkpoint['store_id'],
                'checkpoint_journal_id': checkpoint['journal_id'], 'wal_path': str(self.wal),
                'wal_sha256': digest(self.wal), 'wal_bytes': self.wal.stat().st_size,
                'configuration_path': str(self.config), 'configuration_sha256': digest(self.config),
                'journal': self.journal(), 'acknowledged_entities': self.entities.copy()}

    def entity(self, number):
        value = self.tool('create_entity_simple', {'entity_type': 'topic',
            'display_name': 'Installed durability witness ' + str(number),
            'summary': 'Exact acknowledged synthetic bytes ' + str(number)}, space=SPACE, role='service')
        require(isinstance(value, dict) and isinstance(value.get('id'), str), 'actual acknowledged entity ID absent')
        self.entities.append(value)
        return value

    def verify_entities(self):
        for entity in self.entities:
            status, value = self.http('GET', '/v1/entities/' + urllib.parse.quote(entity['id'], safe=''), space=SPACE, role='service')
            require(status == 200 and value == entity, 'reopened acknowledged entity differs')
        return self.entities.copy()

    def bootstrap(self):
        key = self.root / 'private/signing.key'
        self.process(['service-token', 'bootstrap', '--secret-file', str(key)])
        clearance = self.root / 'private/clearance.json'
        private_new(clearance, encoded([{'policy_id': 'default', 'max_level': 0, 'labels': [], 'handling_allow': []}]))
        for role in ('operator', 'service'):
            output = self.root / 'private' / (role + '.token')
            self.process(credential_arguments(key, clearance, output, role))
            self.tokens[role] = output.read_text().strip()
            require(self.tokens[role].count('.') == 2 and len(self.tokens[role]) <= 16384,
                    'candidate did not issue a bounded synthetic JWT')
        self.environment.update(JWT_SECRET_FILE=str(key), JWT_ALGORITHM='HS256', JWT_ISSUER='fault-driver',
                                JWT_AUDIENCE='fault-driver', ELEPHANT_HOST='127.0.0.1', ELEPHANT_PORT=str(self.port))
        self.launch()
        status, before = self.http('GET', '/v1/readiness')
        require(status == 503 and before.get('configured') is False, 'fresh bootstrap incorrectly ready')
        self.http('POST', '/mcp/full', {'jsonrpc': '2.0', 'id': self.calls + 1, 'method': 'initialize',
            'params': {'protocolVersion': '2025-03-26', 'capabilities': {},
                       'clientInfo': {'name': 'fault-driver', 'version': '1'}}})
        initialized = self.tool('initialize_runtime', {'backend': 'elephant-db', 'snapshot_path': str(self.checkpoint),
                                                      'ingest_enabled': False, 'extractor_required': False})
        require(initialized.get('configured') is True, 'runtime initialization failed')
        self.ready()
        status, _space = self.http('POST', '/v1/spaces', {'name': SPACE,
            'security': {'policy_id': 'default', 'level': 0, 'labels': [], 'handling': []},
            'embedding_provider': 'openai', 'embedding_model': 'unapproved-offline', 'embedding_dims': 8,
            'purpose': 'synthetic durability', 'skill': 'offline', 'processing': {
                'generation': 1, 'index_generation': 0, 'policy_id': 'default', 'connections': {}, 'profiles': {},
                'bindings': {}, 'active_embedding_profile_id': None, 'pending_reindex': None}}, space=SPACE)
        require(status == 201, 'named fixture space not created')
        status, _grant = self.http('PUT', f'/v1/spaces/{SPACE}/grants/fault-service',
            {'active': True, 'scopes': SERVICE_SCOPES, 'expected_generation': None}, space=SPACE)
        require(status == 200, 'current named-space service grant missing')
        self.entity(1)
        self.stop()
        self.baseline = self.witness()
        self.launch()
        self.ready()
        self.verify_entities()
        before_wal = self.witness()
        self.entity(2)
        self.before_fault = self.witness()
        require(self.before_fault['checkpoint_sha256'] == before_wal['checkpoint_sha256']
                and self.before_fault['wal_bytes'] > before_wal['wal_bytes'], 'acknowledged WAL-only fixture absent')
        self.save('prior-good-witness.json', self.before_fault)
        return self.before_fault

    def full_configuration(self):
        self.filler = self.live / 'owned-enospc-filler'
        self.full = fill_quota(self.filler)
        require(self.full['errno'] == 28 and self.full['free_bytes'] == 0, 'filesystem was not actually full')
        offset = self.trace.stat().st_size
        failure = self.tool('configure_runtime', {'runtime_mode': 'read_only'}, failure=True)
        writes = enospc_writes(self.trace.read_bytes()[offset:], self.config.parent)
        require(writes, 'candidate configuration write had no observed kernel ENOSPC')
        require(digest(self.config) == self.before_fault['configuration_sha256'], 'failed configuration changed prior bytes')
        require(digest(self.checkpoint) == self.before_fault['checkpoint_sha256'], 'configuration failure changed checkpoint')
        require(self.journal() == self.before_fault['journal'], 'original journal changed during full configuration')
        return {'filesystem': self.full, 'save_syscalls': writes, 'tool_failure': failure,
                'configuration_sha256': digest(self.config), 'checkpoint_sha256': digest(self.checkpoint)}

    def full_checkpoint(self):
        offset = self.trace.stat().st_size
        result = self.stop(clean=False)
        writes = enospc_writes(self.trace.read_bytes()[offset:], Path(str(self.checkpoint) + '.durable'))
        require(result['exit_code'] != 0 and writes, 'shutdown checkpoint failure lacks actual kernel ENOSPC/nonzero')
        require(digest(self.checkpoint) == self.before_fault['checkpoint_sha256'], 'failed checkpoint replaced prior committed bytes')
        require(digest(self.wal) == self.before_fault['wal_sha256'], 'failed checkpoint changed acknowledged WAL')
        require(self.journal() == self.before_fault['journal'], 'failed checkpoint changed original journal')
        self.filler.unlink()  # Only the exact file created by fill_quota.
        self.launch()
        ready = self.ready()
        acknowledged = self.verify_entities()
        require(self.journal() == self.before_fault['journal'], 'reopen replaced original journal')
        return {'exit_code': result['exit_code'], 'save_syscalls': writes, 'reopened_same_destination': str(self.checkpoint),
                'ready': ready, 'acknowledged_entities': acknowledged, 'journal': self.journal()}

    def kill_and_reopen(self):
        before = self.witness()
        self.entity(3)
        acknowledged = self.witness()
        require(before['checkpoint_sha256'] == acknowledged['checkpoint_sha256']
                and before['wal_sha256'] != acknowledged['wal_sha256'], 'new acknowledgment was not WAL-only')
        killed = self.stop(kill=True, clean=False)
        require(killed['exit_code'] != 0, 'SIGKILL did not interrupt the owned installed process')
        require(digest(self.checkpoint) == acknowledged['checkpoint_sha256'], 'kill unexpectedly advanced checkpoint')
        self.launch()
        ready = self.ready()
        entities = self.verify_entities()
        require(self.journal() == acknowledged['journal'], 'kill/reopen changed original journal')
        self.stop()
        return {'phase_observed': 'ready after public acknowledgment and before checkpoint publication',
                'phase_not_established': 'interruption inside a particular atomic-save syscall',
                'before_kill': acknowledged, 'exit_code': killed['exit_code'], 'reopened_ready': ready,
                'acknowledged_entities': entities}

    def negative_cli(self, name, arguments, extra, reason):
        cwd = self.root / 'negative' / name
        cwd.mkdir(mode=0o700)
        environment = {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8', 'RUST_LOG': 'info',
                       'HOME': str(cwd), 'ELEPHANT_DURABILITY_PATH': str(cwd / 'journal'), **extra}
        result = self.process(arguments, cwd=cwd, environment=environment, expected=None)
        wire = result['stdout'] + result['stderr']
        require(result['exit_code'] != 0 and re.search(reason, wire, re.I), 'negative CLI lacks its specific failure reason')
        require(b'Schema applied successfully' not in wire and b'transfer completed' not in wire,
                'failed initialization reported success')
        require(not (cwd / '.elephant/elephant_config.toml').exists(), 'failed initialization published configuration')
        return {'exit_code': result['exit_code'], 'diagnostic_sha256': hashlib.sha256(wire).hexdigest(),
                'config_absent': True}

    def backend_unavailable(self):
        with socket.socket() as owned_port:
            owned_port.bind(('127.0.0.1', 0))  # Bound, never listening; no foreign endpoint can claim it.
            return self.negative_cli('unavailable', ['migrate'], {'ELEPHANT_DB': 'surreal-db',
                'SURREALDB_URL': 'ws://127.0.0.1:' + str(owned_port.getsockname()[1]),
                'SURREALDB_USER': 'synthetic', 'SURREALDB_PASS': 'synthetic'}, rb'connect|refused')

    def unsupported_transfer(self):
        before = digest(self.checkpoint)
        target = self.root / 'negative/never-created-target'
        result = self.negative_cli('transfer', ['data-migrate', '--snapshot', str(self.checkpoint),
            '--url', 'rocksdb://' + str(target)], {}, rb'preview live/named snapshot envelopes.*backend transfer refused')
        require(not target.exists() and digest(self.checkpoint) == before, 'refused transfer mutated source or target')
        return {**result, 'source_sha256': before, 'target_absent': True}

    def bad_credentials(self):
        if self.allocation.get('paired_bundle') is None:
            raise UnmetPrerequisite('matching reviewed paired native bundle was not allocated')
        bundle = Path('/paired')
        for name, field in [('recall-surreal', 'paired_binary_sha256'),
                            ('RECALL_SOURCE_PROVENANCE.json', 'paired_source_sha256'),
                            ('BUILD_PROVENANCE.json', 'paired_build_sha256')]:
            require(digest(bundle / name) == self.allocation[field], 'paired artifact identity differs')
        source = strict_json((bundle / 'RECALL_SOURCE_PROVENANCE.json').read_bytes())
        build = strict_json((bundle / 'BUILD_PROVENANCE.json').read_bytes())
        require(source['recall_commit'] == self.allocation['source_commit']
                and build['binary_sha256'] == self.allocation['paired_binary_sha256'], 'paired source does not match Recall')
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1', 0))
            port = reserve.getsockname()[1]
        server = self.process(['start', '--bind', '127.0.0.1:' + str(port), 'memory'],
            binary=bundle / 'recall-surreal', environment={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8',
            'SURREAL_USER': 'fault-fixture', 'SURREAL_PASS': 'synthetic-correct-password'}, serve=True)
        try:
            end = time.monotonic() + min(20, self.remaining())
            while True:
                require(server.poll() is None, 'paired authentication control failed startup')
                try:
                    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
                    connection.request('GET', '/health')
                    response = connection.getresponse()
                    okay = response.status == 200 and len(response.read(1025)) <= 1024
                    connection.close()
                    if okay:
                        break
                except OSError:
                    pass
                require(time.monotonic() < end, 'paired authentication control not healthy')
                time.sleep(0.1)
            # Establish the server's credential boundary with the same native
            # installed CLI before the deliberate wrong-password attempt.
            positive = self.root / 'negative' / 'correct-credentials'
            positive.mkdir(mode=0o700)
            self.process(['migrate'], cwd=positive, environment={'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8',
                'RUST_LOG': 'warn', 'HOME': str(positive), 'ELEPHANT_DB': 'surreal-db',
                'ELEPHANT_DURABILITY_PATH': str(positive / 'journal'),
                'SURREALDB_URL': 'ws://127.0.0.1:' + str(port), 'SURREALDB_NAMESPACE': 'fault_positive',
                'SURREALDB_DATABASE': 'fixture', 'SURREALDB_USER': 'fault-fixture',
                'SURREALDB_PASS': 'synthetic-correct-password'})
            return self.negative_cli('bad-credentials', ['migrate'], {'ELEPHANT_DB': 'surreal-db',
                'SURREALDB_URL': 'ws://127.0.0.1:' + str(port), 'SURREALDB_USER': 'fault-fixture',
                'SURREALDB_PASS': 'synthetic-wrong-password'}, rb'authenticat|signin|credentials|permission')
        finally:
            server.signal(signal.SIGKILL)
            server.wait(5)

    def execute(self):
        for name, action in [('prior_good_checkpoint_and_wal', self.bootstrap),
                             ('configuration_os_enospc', self.full_configuration),
                             ('checkpoint_os_enospc_and_reopen', self.full_checkpoint),
                             ('installed_sigkill_acknowledged_wal', self.kill_and_reopen),
                             ('backend_unavailable', self.backend_unavailable),
                             ('unsupported_preview_transfer', self.unsupported_transfer),
                             ('bad_backend_credentials', self.bad_credentials)]:
            self.case(name, action)
            if self.report['cases'][-1]['verdict'] != 'pass' and name != 'bad_backend_credentials':
                break

    def finish(self):
        failures = []
        if self.candidate_pidfd is not None:
            os.close(self.candidate_pidfd)
            self.candidate_pidfd = None
        for child in reversed(self.children):
            if not child.reaped:
                try:
                    child.signal(signal.SIGKILL)
                    child.wait(5)
                except BaseException as error:
                    failures.append(type(error).__name__)
        self.report['cleanup_failures'] = failures
        executed = {row['case'] for row in self.report['cases']}
        for name in self.inventory['expectations']['required_cases']:
            if name not in executed:
                self.report['cases'].append({'case': name, 'verdict': 'unmet_prerequisite',
                    'reason': 'an earlier required fixture or execution step did not complete'})
        observed = {row['case']: row['verdict'] for row in self.report['cases']}
        self.report['installed_cases_passed'] = not failures and all(
            observed.get(name) == 'pass' for name in self.inventory['expectations']['required_cases'])
        private_new(self.root / 'observations.json', encoded(self.report))


def outside(manifest):
    allocation, inventory = validate_allocation(manifest)
    for path in ('/usr/bin/bwrap', '/usr/bin/strace', '/usr/bin/setpriv', '/usr/bin/python3'):
        if not os.access(path, os.X_OK):
            raise UnmetPrerequisite('required owned-namespace tool unavailable: ' + path)
    run = Path(allocation['run_directory'])
    require(run == run.resolve() and not run.exists(), 'new canonical run directory required')
    require(not any((parent / '.elephant').exists() or (parent / 'Cargo.toml').exists() for parent in run.parents),
            'run directory must be outside live runtime and checkout')
    run.mkdir(mode=0o700)
    _mcp, capture, artifact = utilities()
    with capture.total_deadline(60):
        installed = artifact.verify(Path(allocation['archive']), allocation['archive_sha256'], run / 'installed')
    require(installed['source_commit'] == allocation['source_commit']
            and installed['binary_sha256'] == allocation['binary_sha256'] == digest(run / 'installed/meerkat-recall')
            and installed['target'] == 'x86_64-unknown-linux-gnu', 'installed candidate identity differs')
    parent = {name: os.readlink('/proc/self/ns/' + name) for name in ('user', 'mnt', 'net')}
    packet = {'allocation': allocation, 'inventory': inventory, 'parent_namespaces': parent,
              'installed_manifest': installed}
    private_new(run / 'namespace-input.json', encoded(packet))
    command = namespace_command(ROOT.resolve(), run, '/qualification/namespace-input.json',
                                allocation.get('paired_bundle'))
    private_new(run / 'namespace-command.json', encoded(command))
    # The namespace keeper is the Python driver itself; the capped filesystem
    # survives every candidate stop/kill/reopen until this one child completes.
    runner = Child(command, run, {'PATH': '/usr/bin:/bin', 'LANG': 'C.UTF-8'}, run, 0)
    try:
        code = runner.wait(inventory['expectations']['limits']['total_seconds'] + 45)
    except BaseException:
        runner.signal(signal.SIGKILL)
        runner.wait(5)
        raise
    require((run / 'observations.json').exists(), 'namespace prerequisite/startup failed; no installed verdict available')
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--execute', type=Path)
    group.add_argument('--inside', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    if args.inside:
        require(args.inside == Path('/qualification/namespace-input.json'), 'unexpected namespace packet path')
        packet = strict_json(args.inside.read_bytes())
        require(packet['inventory'] == plan(), 'namespace source differs from reviewed launcher')
        isolated = isolation(packet['parent_namespaces'])
        driver = Driver(packet['allocation'], packet['inventory'], isolated)
        failure = None
        try:
            driver.execute()
        except BaseException as error:
            failure = type(error).__name__
            driver.report['failure_type'] = failure
        finally:
            driver.finish()
        return 0 if failure is None and driver.report['installed_cases_passed'] else 1
    if args.execute:
        return outside(args.execute)
    print(json.dumps(plan(), indent=2))
    return 0


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except UnmetPrerequisite as error:
        print(json.dumps({'verdict': 'unmet_prerequisite', 'reason': str(error), 'product_acceptance': False}), file=sys.stderr)
        raise SystemExit(2)
    except (ValueError, OSError, KeyError, subprocess.SubprocessError) as error:
        print(json.dumps({'verdict': 'fail', 'error_type': type(error).__name__, 'reason': str(error)[:512],
                          'product_acceptance': False}), file=sys.stderr)
        raise SystemExit(1)
