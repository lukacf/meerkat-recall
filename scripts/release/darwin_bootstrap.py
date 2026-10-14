"""Closed source/tool acquisition steps under the existing Darwin supervisor.

Import is inert. The original START, ledger, lane marker, command process-group
custody and absolute deadline remain owned by supervise_darwin_build. No final
source or future task interpreter is pretended to exist during bootstrap.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import re
import shutil
from urllib.parse import urlsplit

FORMAT = 'recall-darwin-bootstrap-allocation/v1'
FIELDS = {'format', 'execution_authorized', 'operation', 'target', 'cargo_jobs',
          'maximum_seconds', 'minimum_free_bytes', 'allocation_bytes',
          'maximum_group_resident_bytes', 'started_at', 'expires_at', 'lane',
          'task_directory', 'budget_receipt', 'lane_lock', 'monitored_paths',
          'bootstrap_python', 'tools', 'paths', 'source'}
PATHS = {'tmp_directory', 'receipt_directory', 'uv_cache', 'python_install',
         'rustup_home', 'cargo_bootstrap', 'acquisition_directory'}
PYTHON_REQUEST = 'cpython-3.12.12-macos-aarch64-none'
RUST_REQUEST = '1.94.0-aarch64-apple-darwin'
BUNDLE_BYTES = 128 * 1024**2


def prepare(api, allocation):
    api.require(set(allocation) == FIELDS and allocation['format'] == FORMAT,
                'complete closed bootstrap allocation required')
    api.require(allocation['operation'] in {'tools', 'source', 'tools-and-source', 'native-controls'},
                'bootstrap operation must be tools, source, tools-and-source or native-controls')
    # Validate only the shared original grant. This does not invent final source.
    api.validate_limits(allocation)
    task = api.canonical(allocation['task_directory'])
    api.require(task.stat().st_uid == api.os.getuid() and not task.stat().st_mode & 0o077,
                'private original task directory required')
    paths = allocation['paths']
    api.require(type(paths) is dict and set(paths) == PATHS, 'complete bootstrap path set required')
    paths = {key: api.canonical(value) for key, value in paths.items()}
    api.require(len(set(paths.values())) == len(PATHS), 'bootstrap paths must be distinct')
    for name, path in paths.items():
        api.require(path.is_relative_to(task) and path != task
                    and path.stat().st_uid == api.os.getuid() and not path.stat().st_mode & 0o077,
                    'bootstrap writes must be in private task directories')
        api.require(not any(path.is_relative_to(other) for other in paths.values() if other != path),
                    'bootstrap writable roots overlap')
    api.require(not any(paths['receipt_directory'].iterdir()), 'fresh bootstrap receipts required')
    api.require(paths['rustup_home'] == task / 'rustup'
                and paths['cargo_bootstrap'] == task / 'cargo-bootstrap'
                and paths['python_install'] == task / 'tools/python', 'task-local tool destinations differ')
    api.require(not (paths['cargo_bootstrap'] / 'bin').exists(), 'bootstrap Cargo bin override forbidden')
    api.require(not any((paths['cargo_bootstrap'] / name).exists()
                       for name in ('config', 'config.toml', 'credentials', 'credentials.toml')),
                'bootstrap Cargo configuration forbidden')
    ledger = api.private_file(allocation['budget_receipt'])
    original = api.read_json(ledger)
    api.require(original.get('format') == 'meerkat-recall-artifact-allocation/v1', 'original Mac ledger required')
    api.require(original.get('limit_bytes') == allocation['allocation_bytes']
                and original.get('minimum_free_bytes') == allocation['minimum_free_bytes'],
                'bootstrap cannot change original ledger allocation or free floor')
    monitored = tuple(api.canonical(value) for value in allocation['monitored_paths'])
    devices = {str(path.stat().st_dev) for path in monitored}
    api.require(devices == set(original['filesystems']), 'original monitored devices differ')
    lane_file = api.private_file(allocation['lane_lock'])
    for path in (task, ledger.parent, lane_file, *paths.values()):
        api.require(str(path.stat().st_dev) in devices, 'bootstrap filesystem is not allocated')
    source = allocation['source']
    transport = None
    cloud_config = None
    if allocation['operation'] in {'tools', 'native-controls'}:
        api.require(source is None, 'tools-only bootstrap must not imply source acceptance')
    else:
        api.require(type(source) is dict and set(source) == {
            'url', 'bundle_sha256', 'source_identity', 'cloudsdk_config_directory'},
            'closed exact final source acquisition required')
        api.require(isinstance(source['url'], str) and len(source['url']) <= 8192
                    and not any(c.isspace() or ord(c) < 32 for c in source['url']),
                    'literal source bundle location required')
        url = urlsplit(source['url'])
        api.require(not url.username and not url.password and not url.query and not url.fragment,
                    'source location cannot contain credentials, queries or fragments')
        if url.scheme == 'gs':
            api.require(re.fullmatch(r'[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]', url.netloc)
                        and re.fullmatch(r'/[A-Za-z0-9._/-]+', url.path)
                        and all(part not in {'', '.', '..'} for part in url.path[1:].split('/')),
                        'one literal gs:// object required; wildcard and generation selectors forbidden')
            cloud_config = api.canonical(source['cloudsdk_config_directory'])
            api.require(cloud_config.stat().st_uid == api.os.getuid()
                        and not cloud_config.stat().st_mode & 0o077
                        and str(cloud_config.stat().st_dev) in devices,
                        'existing private allocated gcloud configuration required')
            transport = 'gcloud'
        else:
            api.require(url.scheme == 'https' and bool(url.hostname)
                        and source['cloudsdk_config_directory'] is None,
                        'HTTPS source location or literal gs:// object required')
            transport = 'curl'
        api.require(re.fullmatch('[a-f0-9]{64}', source['bundle_sha256']), 'source bundle checksum required')
        api.validate_identity(source['source_identity'])
        api.require(not (task / 'source').exists() and not (task / 'source').is_symlink(),
                    'source acquisition requires a fresh source directory')
        bundle = paths['acquisition_directory'] / 'source.bundle'
        api.require(not bundle.exists() and not bundle.is_symlink(), 'source bundle destination exists')
    expected_tools = allocation['tools']
    required = ({'uv', 'rustup'} if allocation['operation'] in {'tools', 'tools-and-source'} else set())
    if allocation['operation'] == 'native-controls':
        required = {'rustup'}
    if allocation['operation'] in {'source', 'tools-and-source'}:
        required |= {'git', transport}
    api.require(type(expected_tools) is dict and set(expected_tools) == required,
                'exact operation tool set required')
    origins = {}

    def trusted_executable(label, value):
        api.require(type(value) is dict and set(value) == {'path', 'sha256', 'origin_receipt'},
                    'exact executable and trusted origin receipt required')
        executable = Path(value['path'])
        api.require(executable.is_absolute() and executable.parent.resolve(strict=True) == executable.parent
                    and executable.is_file() and api.os.access(executable, api.os.X_OK)
                    and not any(c in str(executable) for c in '\r\n:\x00'),
                    'canonical invocation parent and existing executable required')
        resolved = api.canonical(executable.resolve(strict=True), False)
        api.require(re.fullmatch('[a-f0-9]{64}', value['sha256'])
                    and api.artifact.digest(resolved) == value['sha256'], 'bootstrap tool bytes differ')
        origin = value['origin_receipt']
        api.require(type(origin) is dict and set(origin) == {'path', 'sha256'}, 'trusted origin reference required')
        record = api.private_file(origin['path'])
        api.require(record.stat().st_size <= 65536 and re.fullmatch('[a-f0-9]{64}', origin['sha256'])
                    and api.artifact.digest(record) == origin['sha256'], 'trusted origin receipt changed')
        proof = api.read_json(record)
        api.require(proof.get('format') == 'recall-darwin-trusted-tool/v1'
                    and proof.get('reviewed') is True and proof.get('executable') == str(executable)
                    and proof.get('canonical_executable') == str(resolved)
                    and proof.get('sha256') == value['sha256']
                    and isinstance(proof.get('origin'), str) and 0 < len(proof['origin']) <= 4096,
                    'trusted tool origin does not bind actual executable')
        for path in (resolved, record):
            api.require(str(path.stat().st_dev) in devices, 'trusted tool filesystem omitted from original ledger')
        origins[label] = {'reference': origin, 'receipt': proof}
        # rustup is a rustup-init symlink: argv[0] selects behavior. Preserve
        # manager invocation while hashing/recording its canonical target.
        return resolved if label == 'bootstrap_python' else executable

    python = trusted_executable('bootstrap_python', allocation['bootstrap_python'])
    tools = {name: trusted_executable(name, value) for name, value in expected_tools.items()}
    environment = {
        'HOME': str(task), 'PATH': f'{python.parent}:/usr/bin:/bin:/usr/sbin:/sbin',
        'LANG': 'C', 'LC_ALL': 'C', 'PYTHONDONTWRITEBYTECODE': '1',
        'TMPDIR': str(paths['tmp_directory']), 'UV_PYTHON_INSTALL_DIR': str(paths['python_install']),
        'UV_CACHE_DIR': str(paths['uv_cache']), 'UV_NO_PROGRESS': '1',
        'RUSTUP_HOME': str(paths['rustup_home']), 'CARGO_HOME': str(paths['cargo_bootstrap']),
        'RUSTUP_TOOLCHAIN': RUST_REQUEST, 'RUSTUP_AUTO_INSTALL': '0', 'CARGO_BUILD_JOBS': '4',
        'GIT_CONFIG_NOSYSTEM': '1', 'GIT_CONFIG_GLOBAL': '/dev/null', 'GIT_TERMINAL_PROMPT': '0',
    }
    if source is not None:
        api.require(tools['git'].name == 'git', 'reviewed Git invocation must be named git')
        environment['PATH'] = str(tools['git'].parent) + ':' + environment['PATH']
        api.require(shutil.which('git', path=environment['PATH']) == str(tools['git']),
                    'artifact identity Git resolution differs from reviewed invocation')
    if transport == 'gcloud':
        # Reuse only the explicitly reviewed existing authentication directory.
        # No key contents, token environment or credential-creation command is read.
        environment.update(CLOUDSDK_CONFIG=str(cloud_config), CLOUDSDK_CORE_DISABLE_PROMPTS='1',
            CLOUDSDK_CORE_DISABLE_FILE_LOGGING='true', CLOUDSDK_CORE_DISABLE_USAGE_REPORTING='true',
            CLOUDSDK_CORE_LOG_HTTP='false', CLOUDSDK_CORE_VERBOSITY='warning',
            CLOUDSDK_COMPONENT_MANAGER_DISABLE_UPDATE_CHECK='true',
            CLOUDSDK_STORAGE_PROCESS_COUNT='1', CLOUDSDK_STORAGE_THREAD_COUNT='1',
            CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD='0',
            CLOUDSDK_STORAGE_TRACKER_FILES_DIRECTORY=str(paths['acquisition_directory']))
    # `source` here is explicitly the bootstrap command working directory. It
    # is not an invented Git checkout or a final-source identity.
    return {'identity': source['source_identity'] if source else None,
            'task': task, 'source': task, 'paths': paths, 'python': python, 'tools': tools,
            'environment': environment, 'ledger': ledger, 'monitored': monitored,
            'lane_file': lane_file, 'original_ledger_sha256': api.artifact.digest(ledger),
            'bootstrap': True, 'tool_origins': origins}


def check_download(api, prepared):
    """Sample the exact acquisition output through the original observer loop."""
    bundle = prepared['paths']['acquisition_directory'] / 'source.bundle'
    if bundle.exists() or bundle.is_symlink():
        api.require(not bundle.is_symlink() and bundle.is_file()
                    and bundle.stat().st_size <= BUNDLE_BYTES,
                    'source bundle sampled size/type bound exceeded')


def run(api, supervisor):
    """Only original Supervisor.command may create an acquisition process."""
    allocation, prepared = supervisor.allocation, supervisor.prepared
    tools, paths, task = prepared['tools'], prepared['paths'], prepared['task']
    supervisor.check()
    if allocation['operation'] == 'native-controls':
        return native_controls(api, supervisor)
    observations = {'format': 'recall-darwin-bootstrap-observation/v1',
                    'operation': allocation['operation'], 'source_identity': None,
                    'python': None, 'rustc': None, 'cargo': None,
                    'tool_origins': prepared['tool_origins'], 'product_acceptance': False}
    for name in sorted(tools):
        executable = tools[name]
        if name == 'rustup':
            # The pinned Homebrew multicall binary has a version-only setup
            # branch. Manager-mode --version also queries the active compiler.
            origin = prepared['tool_origins'][name]['receipt']
            executable = api.canonical(origin['canonical_executable'], False)
            api.require(executable.name == 'rustup-init'
                        and api.artifact.digest(executable) == origin['sha256'],
                        'version-only rustup-init must match the trusted manager bytes')
        supervisor.command(name + '-version', [executable, '--version'], 30)
    if allocation['operation'] in {'tools', 'tools-and-source'}:
        # Fixed reviewed manager commands. No default toolchain, profile files,
        # self-update, global bin link, Homebrew or Apple developer selection.
        supervisor.command('python-install', [tools['uv'], '--no-config', 'python', 'install',
            '--no-bin', PYTHON_REQUEST], 1800, 16 * 1024**2)
        found = supervisor.command('python-find', [tools['uv'], '--no-config', 'python', 'find',
            '--managed-python', '--no-python-downloads', '--no-project', '3.12.12'], 30)
        located = found.read_text().strip()
        api.require('\n' not in located and Path(located).is_absolute(), 'uv did not return one Python path')
        python = Path(located).resolve(strict=True)
        api.require(python.is_relative_to(paths['python_install']) and python.is_file(), 'managed Python escaped task')
        supervisor.command('python-identity', [python, '-I', '-S', '-c',
            'import platform,sys; assert sys.version_info[:3] == (3,12,12); assert platform.machine() == "arm64"; print(sys.version)'], 30)
        supervisor.command('rust-install', [tools['rustup'], 'toolchain', 'install', RUST_REQUEST,
            '--profile', 'minimal', '--no-self-update'], 2400, 16 * 1024**2)
        toolchain = paths['rustup_home'] / 'toolchains' / RUST_REQUEST / 'bin'
        rustc = api.canonical(toolchain / 'rustc', False)
        cargo = api.canonical(toolchain / 'cargo', False)
        rust_log = supervisor.command('rustc-identity', [rustc, '-vV'], 30)
        api.require('release: 1.94.0\n' in rust_log.read_text()
                    and 'host: aarch64-apple-darwin\n' in rust_log.read_text(), 'installed Rust target/version differs')
        supervisor.command('cargo-identity', [cargo, '-V'], 30)
        for name, executable in [('python', python), ('rustc', rustc), ('cargo', cargo)]:
            supervisor.check()
            observations[name] = {'path': str(executable), 'sha256': api.artifact.digest(executable)}
    if allocation['operation'] in {'source', 'tools-and-source'}:
        source = allocation['source']
        bundle = paths['acquisition_directory'] / 'source.bundle'
        if 'gcloud' in tools:
            supervisor.command('source-download', [tools['gcloud'], 'storage', 'cp',
                source['url'], bundle], 1210, 1024**2)
        else:
            supervisor.command('source-download', [tools['curl'], '--fail', '--silent', '--show-error',
                '--proto', '=https', '--max-redirs', '0', '--max-time', '1200', '--max-filesize', str(BUNDLE_BYTES),
                '--output', bundle, '--url', source['url']], 1210, 1024**2)
        supervisor.check()
        api.require(0 < bundle.stat().st_size <= BUNDLE_BYTES
                    and api.artifact.digest(bundle) == source['bundle_sha256'], 'source bundle size/checksum differs')
        git = tools['git']
        destination = task / 'source'
        supervisor.command('source-empty-init', [git, 'init', '--quiet', destination], 30)
        supervisor.command('source-bundle-verify', [git, '-C', destination, 'bundle', 'verify', bundle], 120)
        heads = supervisor.command('source-bundle-heads', [git, 'bundle', 'list-heads', bundle], 30)
        api.require(heads.read_text().splitlines() == [source['source_identity']['source_commit'] + ' HEAD'],
                    'bundle HEAD differs from reviewed final source')
        supervisor.command('source-fetch', [git, '-C', destination, '-c', 'pack.threads=2',
            'fetch', '--no-tags', bundle, 'HEAD'], 600, 16 * 1024**2)
        supervisor.command('source-checkout', [git, '-C', destination, 'checkout', '--detach',
            source['source_identity']['source_commit']], 120)
        api.require(not (destination / '.git/objects/info/alternates').exists(), 'source has alternate object store')
        supervisor.command('source-fsck', [git, '-C', destination, 'fsck', '--full', '--strict'], 180, 16 * 1024**2)
        # Use the already reviewed bootstrap artifact helper, not downloaded code.
        identity = supervisor.command('source-identity', [prepared['python'], '-I', '-B',
            Path(api.artifact.__file__), 'source-identity', '--root', destination], 180)
        api.require(api.read_json(identity) == source['source_identity'], 'acquired source identity differs')
        observations['source_identity'] = source['source_identity']
        observations['bundle'] = {'path': str(bundle), 'sha256': source['bundle_sha256'], 'bytes': bundle.stat().st_size}
    supervisor.check()
    api.write_receipt(paths['receipt_directory'] / 'bootstrap-observation.json', observations)


def native_controls(api, supervisor):
    """Three fixed commands through the original supervisor; no nested owner."""
    prepared = supervisor.prepared
    python = prepared['python']
    # Retain the original expiry and cleanup reserve while applying a shorter
    # finite control ceiling. This cannot extend or reset the original clock.
    supervisor.deadline = min(supervisor.deadline, api.time.monotonic() + 120)
    supervisor.check()
    supervisor.command('native-immediate-exit', [python, '-I', '-S', '-c', 'pass'], 5, 65536)
    # The leader waits for a ready byte before exit. Its same-group descendant
    # closes that handshake and waits; cleanup must observe/signal it before
    # reaping the original leader. No child creates a new session or group.
    descendant = ('import os,time,json\nr,w=os.pipe()\npid=os.fork()\n'
        'if pid==0:\n os.close(r)\n os.write(w,b"R")\n os.close(w)\n time.sleep(30)\n'
        'else:\n os.close(w)\n assert os.read(r,1)==b"R"\n os.close(r)\n'
        ' print(json.dumps({"leader":os.getpid(),"pgid":os.getpgrp(),"descendant":pid}),flush=True)\n'
        ' os._exit(0)\n')
    descendant_log = supervisor.command('native-owned-descendant', [python, '-I', '-S', '-c', descendant], 5, 65536)
    descendant_identity = api.read_json(descendant_log)
    origin = prepared['tool_origins']['rustup']['receipt']
    version = api.canonical(origin['canonical_executable'], False)
    api.require(version.name == 'rustup-init' and api.artifact.digest(version) == origin['sha256'],
                'version-only rustup-init must match the trusted manager bytes')
    log = supervisor.command('native-rustup-version', [version, '--version'], 30, 65536)
    api.require(log.read_text().splitlines() == ['rustup-init 1.28.2 (2025-04-28)'],
                'expected pinned rustup-init version-only banner')
    supervisor.check()
    api.require(len(supervisor.commands) == 3 and all(
        row.get('returncode') == 0 and row.get('group_cleanup_confirmed') is True
        and type(row.get('child_pid')) is int and row['expected_pgid'] == row['child_pid']
        for row in supervisor.commands), 'all three actual native command custody receipts required')
    api.require(set(descendant_identity) == {'leader', 'pgid', 'descendant'}
        and descendant_identity['leader'] == descendant_identity['pgid'] == supervisor.commands[1]['child_pid']
        and type(descendant_identity['descendant']) is int and descendant_identity['descendant'] > 1
        and descendant_identity['descendant'] != descendant_identity['leader'],
        'actual same-group descendant identity differs from owned command')
    immediate, owned, _ = supervisor.commands
    api.require(immediate.get('exit_observed') is True,
                'actual immediate-exit NOTE_EXIT observation required')
    first = owned.get('first_cleanup_sample') or {}
    api.require(owned.get('exit_observed') is True
        and descendant_identity['descendant'] in first.get('live_pids', [])
        and not first.get('uncertain_pids')
        and owned.get('successful_group_signal') == {
            'pgid': descendant_identity['pgid'], 'signal': int(api.signal.SIGKILL), 'leader_reaped': False}
        and owned.get('last_group_sample', {}).get('live_pids') == []
        and owned.get('last_group_sample', {}).get('uncertain_pids') == [],
        'actual live-descendant sample, original group signal and final empty observation required')
    api.write_receipt(prepared['paths']['receipt_directory'] / 'native-control-observation.json', {
        'format': 'recall-darwin-native-control-observation/v1',
        'operation': 'native-controls', 'commands': supervisor.commands,
        'native_controls_succeeded': True, 'mock_EPERM_or_custody_faults_run': False,
        'owned_descendant_identity': descendant_identity,
        'historical_group_cleanup_proven': False, 'product_acceptance': False,
        'original_allocation_retained': True})
