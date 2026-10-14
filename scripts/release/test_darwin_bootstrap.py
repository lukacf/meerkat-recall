"""Closed mocked bootstrap controls; no network, tools, libproc or child process."""
import copy
import datetime as dt
import json
import os
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

import darwin_bootstrap as bootstrap
import supervise_darwin_build as api


class BootstrapTests(unittest.TestCase):
    def setUp(self):
        for target in ('subprocess.Popen', 'subprocess.run', 'subprocess.check_output', 'socket.socket', 'ctypes.CDLL'):
            block = patch(target, side_effect=AssertionError('no actual process/network/platform call'))
            block.start()
            self.addCleanup(block.stop)
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.task = self.root / 'task'
        self.task.mkdir(mode=0o700)
        paths = {}
        for key, relative in [('tmp_directory', 'tmp'), ('receipt_directory', 'receipts'),
                              ('uv_cache', 'uv-cache'), ('python_install', 'tools/python'),
                              ('rustup_home', 'rustup'), ('cargo_bootstrap', 'cargo-bootstrap'),
                              ('acquisition_directory', 'acquisition')]:
            path = self.task / relative
            path.mkdir(mode=0o700, parents=True)
            paths[key] = str(path)
        self.ledger = self.task / 'budget.json'
        self.ledger.write_text(json.dumps({'format': 'meerkat-recall-artifact-allocation/v1',
            'filesystems': {str(self.task.stat().st_dev): {'free': 900 * api.GIB}},
            'limit_bytes': 150 * api.GIB, 'minimum_free_bytes': 250 * api.GIB,
            'consumed_bytes': 12345, 'checks': 7}))
        self.ledger.chmod(0o600)
        lane = self.task / 'lane.lock'
        lane.touch(mode=0o600)
        now = dt.datetime.now(dt.timezone.utc)
        self.allocation = {'format': bootstrap.FORMAT, 'execution_authorized': False,
            'operation': 'tools', 'target': 'aarch64-apple-darwin', 'cargo_jobs': 4,
            'maximum_seconds': 43200, 'minimum_free_bytes': 250 * api.GIB,
            'allocation_bytes': 150 * api.GIB, 'maximum_group_resident_bytes': 96 * api.GIB,
            'started_at': (now - dt.timedelta(minutes=10)).isoformat(),
            'expires_at': (now + dt.timedelta(hours=10)).isoformat(),
            'lane': {'coordinator': 'codex-security-adr-mac', 'state': 'START', 'grant_id': 'original-start'},
            'task_directory': str(self.task), 'budget_receipt': str(self.ledger), 'lane_lock': str(lane),
            'monitored_paths': [str(self.task)], 'paths': paths, 'source': None,
            'bootstrap_python': self.trusted('python'),
            'tools': {name: self.trusted(name) for name in ('uv', 'rustup')}}

    def trusted(self, name):
        executable = self.root / name
        executable.write_text('synthetic exact tool; never executed: ' + name)
        executable.chmod(0o700)
        checksum = api.artifact.digest(executable)
        origin = self.task / (name + '-origin.json')
        origin.write_text(json.dumps({'format': 'recall-darwin-trusted-tool/v1', 'reviewed': True,
                                     'executable': str(executable), 'canonical_executable': str(executable), 'sha256': checksum,
                                     'origin': 'synthetic reviewed tool fixture'}))
        origin.chmod(0o600)
        return {'path': str(executable), 'sha256': checksum,
                'origin_receipt': {'path': str(origin), 'sha256': api.artifact.digest(origin)}}

    def prepare(self):
        return bootstrap.prepare(api, self.allocation)

    def rustup_multicall(self):
        tool = self.allocation['tools']['rustup']
        invocation = Path(tool['path'])
        target = invocation.with_name('rustup-init')
        invocation.rename(target)
        invocation.symlink_to(target)
        origin = Path(tool['origin_receipt']['path'])
        proof = json.loads(origin.read_text())
        proof['canonical_executable'] = str(target)
        origin.write_text(json.dumps(proof))
        tool['origin_receipt']['sha256'] = api.artifact.digest(origin)
        return invocation, target

    def source_case(self):
        self.allocation['operation'] = 'source'
        self.allocation['tools'] = {name: self.trusted(name) for name in ('git', 'curl')}
        self.allocation['source'] = {'url': 'https://example.invalid/final.bundle', 'bundle_sha256': 'f' * 64,
            'cloudsdk_config_directory': None,
            'source_identity': {'source_commit': '1' * 40, 'source_tree': '2' * 40,
                                'source_tar_sha256': '3' * 64, 'cargo_lock_sha256': '4' * 64}}

    def test_bootstrap_prepares_without_source_or_future_python_and_preserves_ledger(self):
        before = self.ledger.read_bytes()
        with patch.dict(os.environ, {'AWS_SECRET_ACCESS_KEY': 'not-forwarded', 'RUSTFLAGS': 'not-forwarded'}):
            prepared = self.prepare()
        self.assertIsNone(prepared['identity'])
        self.assertEqual(prepared['source'], self.task)
        self.assertEqual(prepared['python'], Path(self.allocation['bootstrap_python']['path']))
        self.assertEqual(prepared['environment']['HOME'], str(self.task))
        self.assertNotIn('AWS_SECRET_ACCESS_KEY', prepared['environment'])
        self.assertNotIn('RUSTFLAGS', prepared['environment'])
        self.assertFalse((self.task / 'source').exists())
        self.assertFalse(any((self.task / 'tools/python').iterdir()))
        self.assertEqual(self.ledger.read_bytes(), before)

    def test_closed_schema_rejects_arbitrary_commands_or_wrong_operation(self):
        for key, value in [('commands', ['/bin/sh', '-c', 'arbitrary']), ('operation', 'build'),
                           ('cargo_jobs', 8), ('allocation_bytes', 200 * api.GIB)]:
            allocation = copy.deepcopy(self.allocation)
            allocation[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                bootstrap.prepare(api, allocation)

    def test_original_ledger_limit_and_floor_cannot_change(self):
        for key, value in [('limit_bytes', 149 * api.GIB), ('minimum_free_bytes', 251 * api.GIB)]:
            original = json.loads(self.ledger.read_text())
            self.ledger.write_text(json.dumps({**original, key: value}))
            before = self.ledger.read_bytes()
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, 'original ledger'):
                self.prepare()
            self.assertEqual(self.ledger.read_bytes(), before)

    def test_origin_receipt_and_actual_manager_bytes_must_match(self):
        tool = self.allocation['tools']['uv']
        Path(tool['path']).write_text('changed')
        with self.assertRaisesRegex(ValueError, 'tool bytes differ'):
            self.prepare()
        tool['sha256'] = api.artifact.digest(Path(tool['path']))
        with self.assertRaisesRegex(ValueError, 'origin does not bind'):
            self.prepare()

    def test_rustup_symlink_keeps_invocation_name_and_hashes_canonical_target(self):
        tool = self.allocation['tools']['rustup']
        invocation = Path(tool['path'])
        target = invocation.with_name('rustup-init')
        invocation.rename(target)
        invocation.symlink_to(target)
        origin = Path(tool['origin_receipt']['path'])
        proof = json.loads(origin.read_text())
        proof['canonical_executable'] = str(target)
        origin.write_text(json.dumps(proof))
        tool['origin_receipt']['sha256'] = api.artifact.digest(origin)
        prepared = self.prepare()
        self.assertEqual(prepared['tools']['rustup'], invocation)
        self.assertEqual(prepared['tool_origins']['rustup']['receipt']['canonical_executable'], str(target))
        self.assertEqual(api.artifact.digest(target), tool['sha256'])
        target.write_text('changed canonical target')
        with self.assertRaisesRegex(ValueError, 'tool bytes differ'):
            self.prepare()
        other = invocation.with_name('different-target')
        other.write_text('synthetic exact tool; never executed: rustup')
        other.chmod(0o700)
        invocation.unlink()
        invocation.symlink_to(other)
        with self.assertRaisesRegex(ValueError, 'origin does not bind'):
            self.prepare()

    def test_bootstrap_paths_reject_escape_overlap_and_ambient_config(self):
        paths = self.allocation['paths']
        original = dict(paths)
        for key, value in [('uv_cache', str(self.root)), ('uv_cache', paths['tmp_directory'])]:
            paths.update(original)
            paths[key] = value
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.prepare()
        paths.update(original)
        (Path(paths['cargo_bootstrap']) / 'config.toml').write_text('[build]\n')
        with self.assertRaisesRegex(ValueError, 'configuration'):
            self.prepare()

    def test_source_requires_https_exact_identity_and_fresh_destination(self):
        self.source_case()
        self.assertEqual(self.prepare()['identity'], self.allocation['source']['source_identity'])
        for url in ('http://example.invalid/bundle', 'https://user:password@example.invalid/bundle',
                    'file:///tmp/bundle', 'https://example.invalid/bundle#fragment',
                    'https://example.invalid/bundle?token=secret'):
            self.allocation['source']['url'] = url
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.prepare()
        self.allocation['source']['url'] = 'https://example.invalid/bundle'
        (self.task / 'source').mkdir()
        with self.assertRaisesRegex(ValueError, 'fresh source'):
            self.prepare()

    def test_gcloud_source_uses_one_literal_object_and_existing_explicit_auth(self):
        self.source_case()
        config = self.root / 'existing-cloud-config'
        config.mkdir(mode=0o700)
        self.allocation['tools'].pop('curl')
        self.allocation['tools']['gcloud'] = self.trusted('gcloud')
        source = self.allocation['source']
        source.update(url='gs://reviewed-private-bucket/exact/final.bundle',
                      cloudsdk_config_directory=str(config))
        with patch.dict(os.environ, {'GOOGLE_APPLICATION_CREDENTIALS': 'not-forwarded'}):
            prepared = self.prepare()
        environment = prepared['environment']
        self.assertEqual(environment['CLOUDSDK_CONFIG'], str(config))
        self.assertNotIn('CLOUDSDK_PYTHON', environment)
        self.assertEqual(environment['CLOUDSDK_STORAGE_PROCESS_COUNT'], '1')
        self.assertEqual(environment['CLOUDSDK_STORAGE_THREAD_COUNT'], '1')
        self.assertEqual(environment['CLOUDSDK_STORAGE_SLICED_OBJECT_DOWNLOAD_THRESHOLD'], '0')
        self.assertEqual(environment['CLOUDSDK_STORAGE_TRACKER_FILES_DIRECTORY'],
                         str(prepared['paths']['acquisition_directory']))
        self.assertEqual(environment['CLOUDSDK_CORE_DISABLE_USAGE_REPORTING'], 'true')
        self.assertNotIn('GOOGLE_APPLICATION_CREDENTIALS', environment)
        instance = Mock(allocation=self.allocation, prepared=prepared)

        def command(name, argv, seconds, cap=None):
            if name == 'source-download':
                self.assertEqual(argv, [prepared['tools']['gcloud'], 'storage', 'cp', source['url'],
                    prepared['paths']['acquisition_directory'] / 'source.bundle'])
                self.assertEqual(seconds, 1210)
                (prepared['paths']['acquisition_directory'] / 'source.bundle').write_bytes(b'wrong')
            return self.root / 'unused'

        instance.command.side_effect = command
        with self.assertRaisesRegex(ValueError, 'size/checksum'):
            bootstrap.run(api, instance)
        self.assertEqual([call.args[0] for call in instance.command.call_args_list],
                         ['gcloud-version', 'git-version', 'source-download'])
        for url in ('gs://reviewed-bucket/*', 'gs://reviewed-bucket/../object',
                    'gs://reviewed-bucket/object#17', 'gs://reviewed-bucket/object?token=secret',
                    'gs://reviewed-bucket/object\nnext', 'gs://reviewed-bucket/'):
            source['url'] = url
            with self.subTest(url=url), self.assertRaises(ValueError):
                self.prepare()

    def test_source_identity_resolves_reviewed_git_from_different_parent(self):
        self.source_case()
        tool = self.allocation['tools']['git']
        original = Path(tool['path'])
        chosen = self.root / 'reviewed-git/bin/git'
        chosen.parent.mkdir(parents=True)
        original.rename(chosen)
        origin = Path(tool['origin_receipt']['path'])
        proof = json.loads(origin.read_text())
        proof.update(executable=str(chosen), canonical_executable=str(chosen))
        origin.write_text(json.dumps(proof))
        tool.update(path=str(chosen), origin_receipt={'path': str(origin), 'sha256': api.artifact.digest(origin)})
        prepared = self.prepare()
        self.assertEqual(prepared['tools']['git'], chosen)
        self.assertEqual(bootstrap.shutil.which('git', path=prepared['environment']['PATH']), str(chosen))
        self.assertEqual(prepared['environment']['PATH'].split(':')[0], str(chosen.parent))
        chosen.chmod(0o600)
        with self.assertRaisesRegex(ValueError, 'existing executable'):
            self.prepare()

    def test_original_observer_rejects_oversize_or_symlink_download(self):
        prepared = self.prepare()
        bundle = prepared['paths']['acquisition_directory'] / 'source.bundle'
        bundle.write_bytes(b'123456789')
        with patch.object(bootstrap, 'BUNDLE_BYTES', 8), self.assertRaisesRegex(ValueError, 'sampled size/type'):
            bootstrap.check_download(api, prepared)
        bundle.unlink()
        bundle.symlink_to(self.ledger)
        with self.assertRaisesRegex(ValueError, 'sampled size/type'):
            bootstrap.check_download(api, prepared)

    def test_tools_only_cannot_claim_source_or_extra_download_tool(self):
        self.allocation['source'] = {'invented': True}
        with self.assertRaisesRegex(ValueError, 'tools-only'):
            self.prepare()
        self.allocation['source'] = None
        self.allocation['tools']['curl'] = self.trusted('curl')
        with self.assertRaisesRegex(ValueError, 'exact operation tool set'):
            self.prepare()

    def test_tool_steps_use_original_command_and_fixed_task_local_versions(self):
        invocation, version_binary = self.rustup_multicall()
        prepared = self.prepare()
        instance = Mock(allocation=self.allocation, prepared=prepared)
        python = prepared['paths']['python_install'] / 'cpython/bin/python3.12'
        rustbin = prepared['paths']['rustup_home'] / ('toolchains/' + bootstrap.RUST_REQUEST + '/bin')
        for executable in (python, rustbin / 'rustc', rustbin / 'cargo'):
            executable.parent.mkdir(parents=True, exist_ok=True)
            executable.write_text('synthetic installed executable')

        def command(name, argv, seconds, cap=None):
            path = prepared['paths']['receipt_directory'] / (name + '.log')
            value = str(python) + '\n' if name == 'python-find' else 'release: 1.94.0\nhost: aarch64-apple-darwin\n'
            path.write_text(value)
            return path

        instance.command.side_effect = command
        bootstrap.run(api, instance)
        calls = {call.args[0]: call for call in instance.command.call_args_list}
        self.assertEqual(calls['python-install'].args[1], [prepared['tools']['uv'], '--no-config', 'python',
                         'install', '--no-bin', bootstrap.PYTHON_REQUEST])
        self.assertEqual(calls['rust-install'].args[1], [prepared['tools']['rustup'], 'toolchain', 'install',
                         bootstrap.RUST_REQUEST, '--profile', 'minimal', '--no-self-update'])
        self.assertEqual(calls['rustup-version'].args[1], [version_binary, '--version'])
        self.assertEqual(calls['rust-install'].args[1][0], invocation)
        self.assertEqual(prepared['environment']['RUSTUP_AUTO_INSTALL'], '0')
        observation = json.loads((prepared['paths']['receipt_directory'] / 'bootstrap-observation.json').read_text())
        self.assertIsNone(observation['source_identity'])
        self.assertFalse(observation['product_acceptance'])
        self.assertEqual(observation['python']['sha256'], api.artifact.digest(python))
        self.assertGreaterEqual(instance.check.call_count, 5)

    def test_version_only_uses_pinned_setup_mode_with_empty_toolchain(self):
        _, target = self.rustup_multicall()
        prepared = self.prepare()
        instance = Mock(allocation=self.allocation, prepared=prepared)
        def command(name, argv, seconds, cap=None):
            if name == 'python-install':
                raise RuntimeError('stop before any explicit installation')
            self.assertEqual(name, 'rustup-version' if argv[0] == target else 'uv-version')
            self.assertEqual(argv[1:], ['--version'])
            self.assertFalse(any(prepared['paths']['rustup_home'].iterdir()))
            self.assertEqual(prepared['environment']['RUSTUP_AUTO_INSTALL'], '0')
        instance.command.side_effect = command
        with self.assertRaisesRegex(RuntimeError, 'stop before'):
            bootstrap.run(api, instance)
        self.assertEqual([call.args[0] for call in instance.command.call_args_list],
                         ['rustup-version', 'uv-version', 'python-install'])

    def test_version_only_rejects_wrong_basename_or_changed_canonical_bytes(self):
        prepared = self.prepare()
        instance = Mock(allocation=self.allocation, prepared=prepared)
        with self.assertRaisesRegex(ValueError, 'version-only'):
            bootstrap.run(api, instance)
        instance.command.assert_not_called()
        _, target = self.rustup_multicall()
        prepared = self.prepare()
        target.write_text('changed after preparation')
        instance = Mock(allocation=self.allocation, prepared=prepared)
        with self.assertRaisesRegex(ValueError, 'version-only'):
            bootstrap.run(api, instance)
        instance.command.assert_not_called()

    def native_prepared(self):
        self.rustup_multicall()
        self.allocation.update(operation='native-controls', source=None)
        self.allocation['tools'].pop('uv')
        return self.prepare()

    def test_native_controls_accept_only_exact_rustup_and_no_source(self):
        prepared = self.native_prepared()
        self.assertEqual(set(prepared['tools']), {'rustup'})
        self.assertIsNone(prepared['identity'])
        self.allocation['source'] = {'invented': 'source'}
        with self.assertRaisesRegex(ValueError, 'must not imply source'):
            self.prepare()
        self.allocation['source'] = None
        self.allocation['tools']['uv'] = self.trusted('uv-extra')
        with self.assertRaisesRegex(ValueError, 'exact operation tool set'):
            self.prepare()

    def test_native_control_mode_uses_original_command_three_times_without_install(self):
        prepared = self.native_prepared()
        instance = Mock(allocation=self.allocation, prepared=prepared, deadline=5000, commands=[])
        def command(name, argv, seconds, cap):
            self.assertEqual((seconds, cap), (30 if name == 'native-rustup-version' else 5, 65536))
            self.assertNotIn('install', argv)
            path = prepared['paths']['receipt_directory'] / (name + '.log')
            value = 'rustup-init 1.28.2 (2025-04-28)\n' if name == 'native-rustup-version' else ''
            if name == 'native-owned-descendant':
                value = json.dumps({'leader': 302, 'pgid': 302, 'descendant': 401})
            path.write_text(value)
            instance.commands.append({'name': name, 'argv': list(map(str, argv)), 'returncode': 0,
                'group_cleanup_confirmed': True, 'child_pid': 301+len(instance.commands),
                'expected_pgid': 301+len(instance.commands), 'exit_observed': True,
                'first_cleanup_sample': {'live_pids': [401], 'uncertain_pids': []},
                'successful_group_signal': {'pgid': 302, 'signal': int(api.signal.SIGKILL), 'leader_reaped': False},
                'last_group_sample': {'live_pids': [], 'uncertain_pids': []}})
            return path
        instance.command.side_effect = command
        with patch.object(api.time, 'monotonic', return_value=100):
            bootstrap.run(api, instance)
        self.assertEqual(instance.deadline, 220)
        self.assertEqual([c.args[0] for c in instance.command.call_args_list],
                         ['native-immediate-exit', 'native-owned-descendant', 'native-rustup-version'])
        self.assertIn('os.fork()', instance.command.call_args_list[1].args[1][-1])
        result = json.loads((prepared['paths']['receipt_directory'] / 'native-control-observation.json').read_text())
        self.assertTrue(result['native_controls_succeeded'])
        self.assertFalse(result['product_acceptance'] or result['historical_group_cleanup_proven'])

    def test_native_control_first_failure_stops_before_other_commands(self):
        prepared = self.native_prepared()
        instance = Mock(allocation=self.allocation, prepared=prepared, deadline=150, commands=[])
        instance.command.side_effect = RuntimeError('original cleanup failed')
        with patch.object(api.time, 'monotonic', return_value=100), self.assertRaisesRegex(RuntimeError, 'cleanup failed'):
            bootstrap.run(api, instance)
        self.assertEqual(instance.deadline, 150)
        self.assertEqual(instance.command.call_count, 1)
        self.assertFalse((prepared['paths']['receipt_directory'] / 'native-control-observation.json').exists())

    def test_native_control_refuses_unconfirmed_command_custody(self):
        prepared = self.native_prepared()
        instance = Mock(allocation=self.allocation, prepared=prepared, deadline=5000, commands=[])
        def command(name, argv, seconds, cap):
            path = prepared['paths']['receipt_directory'] / (name + '.log')
            path.write_text(json.dumps({'leader': 301, 'pgid': 301, 'descendant': 401})
                if name == 'native-owned-descendant' else 'rustup-init 1.28.2 (2025-04-28)\n')
            instance.commands.append({'returncode': 0, 'group_cleanup_confirmed': False,
                                      'child_pid': 301, 'expected_pgid': 301})
            return path
        instance.command.side_effect = command
        with self.assertRaisesRegex(ValueError, 'custody receipts'):
            bootstrap.run(api, instance)
        self.assertFalse((prepared['paths']['receipt_directory'] / 'native-control-observation.json').exists())
        for row in instance.commands:
            row.update(group_cleanup_confirmed=True, exit_observed=True)
        instance.command.reset_mock()
        instance.commands.clear()
        def missing_live_evidence(name, argv, seconds, cap):
            path = command(name, argv, seconds, cap)
            row = instance.commands[-1]
            row.update(group_cleanup_confirmed=True, exit_observed=True)
            return path
        instance.command.side_effect = missing_live_evidence
        with self.assertRaisesRegex(ValueError, 'actual live-descendant sample'):
            bootstrap.run(api, instance)

    def test_wrong_bundle_digest_stops_before_git_or_downloaded_python(self):
        self.source_case()
        prepared = self.prepare()
        instance = Mock(allocation=self.allocation, prepared=prepared)

        def command(name, argv, seconds, cap=None):
            if name == 'source-download':
                (prepared['paths']['acquisition_directory'] / 'source.bundle').write_bytes(b'wrong')
            return self.root / 'unused'

        instance.command.side_effect = command
        with self.assertRaisesRegex(ValueError, 'size/checksum'):
            bootstrap.run(api, instance)
        names = [call.args[0] for call in instance.command.call_args_list]
        self.assertEqual(names, ['curl-version', 'git-version', 'source-download'])

    def test_source_steps_validate_bundle_and_final_identity_under_same_command(self):
        self.source_case()
        raw = b'synthetic exact bundle never executed'
        self.allocation['source']['bundle_sha256'] = bootstrap.hashlib.sha256(raw).hexdigest()
        prepared = self.prepare()
        instance = Mock(allocation=self.allocation, prepared=prepared)

        def command(name, argv, seconds, cap=None):
            path = prepared['paths']['receipt_directory'] / (name + '.log')
            if name == 'source-download':
                (prepared['paths']['acquisition_directory'] / 'source.bundle').write_bytes(raw)
            if name == 'source-empty-init':
                (self.task / 'source/.git/objects/info').mkdir(parents=True)
            value = ''
            if name == 'source-bundle-heads':
                value = prepared['identity']['source_commit'] + ' HEAD\n'
            if name == 'source-identity':
                value = json.dumps(prepared['identity'])
                self.assertEqual(argv[0], prepared['python'])
                self.assertEqual(argv[3], Path(api.artifact.__file__))
            path.write_text(value)
            return path

        instance.command.side_effect = command
        bootstrap.run(api, instance)
        names = [call.args[0] for call in instance.command.call_args_list]
        self.assertLess(names.index('source-bundle-verify'), names.index('source-fetch'))
        self.assertLess(names.index('source-fsck'), names.index('source-identity'))
        observation = json.loads((prepared['paths']['receipt_directory'] / 'bootstrap-observation.json').read_text())
        self.assertEqual(observation['source_identity'], prepared['identity'])

    def test_bootstrap_uses_same_execute_marker_cleanup_without_build_claim(self):
        prepared = self.prepare()
        allocation = {**self.allocation, 'execution_authorized': True}
        instance = Mock(started=api.time.monotonic(), commands=[], samples=1, peak_rss=0,
                        minimum_free=900 * api.GIB, last_budget={'refused': False}, children=[])
        instance.cleanup_deadline.return_value = api.time.monotonic() + 3600
        with (patch.object(api.platform, 'system', return_value='Darwin'),
              patch.object(api.platform, 'machine', return_value='arm64'),
              patch.object(api.sys, 'version_info', (3, 11, 9)),
              patch.object(api.sys, 'executable', str(prepared['python'])),
              patch.object(api, 'GroupObserver', return_value=Mock()),
              patch.object(api, 'Supervisor', return_value=instance)):
            report = api.execute(allocation, prepared, 'a' * 64)
        instance.run.assert_called_once()
        self.assertTrue(report['bootstrap_succeeded'] and report['lane_release_ready'])
        self.assertFalse(report['build_succeeded'])
        self.assertEqual(prepared['lane_file'].read_bytes(), b'')
        self.assertTrue((prepared['paths']['receipt_directory'] / 'supervision.json').exists())

    def test_bootstrap_entry_clock_cannot_refresh_after_slow_preparation(self):
        prepared = self.prepare()
        original = dt.datetime(2026, 10, 7, 9, tzinfo=dt.timezone.utc)
        allocation = {**self.allocation, 'started_at': original.isoformat(),
                      'expires_at': (original + dt.timedelta(hours=1)).isoformat()}
        prepared['bootstrap_clock'] = {'monotonic': 100, 'wall': original}
        with (patch.object(api.time, 'monotonic', return_value=700),
              patch.object(api.dt, 'datetime', wraps=dt.datetime) as date):
            # Even a wall-clock rollback after preparation cannot renew time.
            date.now.return_value = original
            instance = api.Supervisor(allocation, prepared, Mock())
        self.assertEqual(instance.deadline, 3700)

    def test_bootstrap_failure_uses_original_cleanup_and_never_claims_success(self):
        prepared = self.prepare()
        allocation = {**self.allocation, 'execution_authorized': True}
        child = Mock(group_empty=False, reaped=False, cleanup_deadline=123)
        child.cleanup.side_effect = RuntimeError('original group unresolved')
        instance = Mock(started=api.time.monotonic(), commands=[], samples=1, peak_rss=0,
                        minimum_free=900 * api.GIB, last_budget={}, children=[child])
        instance.run.side_effect = ValueError('original deadline')
        instance.cleanup_deadline.return_value = 123
        with (patch.object(api.platform, 'system', return_value='Darwin'),
              patch.object(api.platform, 'machine', return_value='arm64'),
              patch.object(api.sys, 'version_info', (3, 11, 9)),
              patch.object(api.sys, 'executable', str(prepared['python'])),
              patch.object(api, 'GroupObserver', return_value=Mock()),
              patch.object(api, 'Supervisor', return_value=instance), self.assertRaises(ValueError)):
            api.execute(allocation, prepared, 'a' * 64)
        child.cleanup.assert_called_once_with(deadline=123)
        self.assertTrue(prepared['lane_file'].read_text())
        report = json.loads((prepared['paths']['receipt_directory'] / 'supervision.json').read_text())
        self.assertFalse(report['bootstrap_succeeded'] or report['build_succeeded'] or report['lane_release_ready'])


if __name__ == '__main__':
    unittest.main()
