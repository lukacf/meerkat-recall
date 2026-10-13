"""Offline tests only: no process, mount, network or candidate execution."""
import datetime as dt
import errno
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('faults_under_test', ROOT / 'scripts/qualification/installed_linux_faults.py')
faults = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(faults)


class FaultDriverTests(unittest.TestCase):
    def setUp(self):
        self.no_process = mock.patch.object(faults.subprocess, 'Popen', side_effect=AssertionError('no child permitted in offline tests'))
        self.no_process.start()
        self.no_socket = mock.patch.object(faults.socket, 'socket', side_effect=AssertionError('no listener permitted in offline tests'))
        self.no_socket.start()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()

    def tearDown(self):
        self.temp.cleanup()
        self.no_socket.stop()
        self.no_process.stop()

    def test_plan_has_distinct_composed_evidence(self):
        result = faults.plan()
        self.assertFalse(result['product_acceptance'])
        self.assertEqual(7, len(result['expectations']['required_cases']))
        self.assertEqual(7, len(result['dependency_sha256']))
        self.assertTrue(any('canonical apply_files' in row for row in result['expectations']['separate_evidence_required']))

    def test_duplicate_and_nonfinite_json_refused(self):
        for raw in (b'{"a":1,"a":2}', b'{"a":NaN}', b'{"a":Infinity}'):
            with self.assertRaises(ValueError):
                faults.strict_json(raw)

    def test_namespace_mounts_only_allocated_writable_roots(self):
        command = faults.namespace_command(Path('/review-source'), Path('/owned-new'), '/qualification/namespace-input.json')
        self.assertIn('--unshare-user', command)
        self.assertIn('--unshare-net', command)
        self.assertIn('--unshare-pid', command)
        self.assertNotIn('--share-net', command)
        self.assertEqual(1, command.count('--bind'))
        index = command.index('--bind')
        self.assertEqual(['/owned-new', '/qualification'], command[index + 1:index + 3])
        index = command.index('--size')
        self.assertEqual([str(64 * 1024**2), '--tmpfs', '/fault-live'], command[index + 1:index + 4])
        self.assertNotIn('--ro-bind', command[command.index('--'):])

    def test_filler_requires_actual_enospc(self):
        path = self.root / 'filler'
        with mock.patch.object(faults.os, 'write', side_effect=[4, OSError(errno.ENOSPC, 'full')]) as write:
            observed = faults.fill_quota(path, 8)
        self.assertEqual(28, observed['errno'])
        self.assertEqual(4, observed['filler_bytes'])
        self.assertEqual(2, write.call_count)
        self.assertEqual(0o600, path.stat().st_mode & 0o777)

    def test_filler_never_overwrites_existing_file(self):
        path = self.root / 'filler'
        path.write_bytes(b'prior bytes')
        with self.assertRaises(FileExistsError):
            faults.fill_quota(path, 8)
        self.assertEqual(b'prior bytes', path.read_bytes())

    def test_permission_error_is_not_disk_full(self):
        with mock.patch.object(faults.os, 'write', side_effect=OSError(errno.EACCES, 'denied')):
            with self.assertRaises(PermissionError):
                faults.fill_quota(self.root / 'filler', 8)

    def test_missing_enospc_does_not_fill_beyond_limit(self):
        with mock.patch.object(faults.os, 'write', return_value=8) as write:
            with self.assertRaisesRegex(ValueError, 'without observed ENOSPC'):
                faults.fill_quota(self.root / 'filler', 8)
        self.assertEqual(1, write.call_count)

    def test_syscall_evidence_requires_candidate_save_path_and_errno(self):
        prefix = b'12 write(9</fault-live/service/.elephant/.next>, "bytes", 5) = -1 ENOSPC (No space left on device)\n'
        self.assertEqual(1, len(faults.enospc_writes(prefix, '/fault-live/service/.elephant')))
        self.assertEqual([], faults.enospc_writes(prefix, '/fault-live/state.snapshot.durable'))
        self.assertEqual([], faults.enospc_writes(prefix.replace(b'ENOSPC', b'EACCES'), '/fault-live/service/.elephant'))
        self.assertEqual([], faults.enospc_writes(b'No space left on device (os error 28)', '/fault-live/service/.elephant'))
        self.assertEqual([], faults.enospc_writes(prefix.replace(b'write(', b'read('), '/fault-live/service/.elephant'))

    def test_management_and_data_credentials_follow_actual_cli_boundary(self):
        operator = faults.credential_arguments('/key', '/clearance', '/operator', 'operator')
        service = faults.credential_arguments('/key', '/clearance', '/service', 'service')
        self.assertIn('--global-admin', operator)
        self.assertNotIn('--space', operator)
        self.assertNotIn('--clearances-file', operator)
        self.assertEqual('/clearance', service[service.index('--clearances-file') + 1])
        self.assertNotIn('--global-admin', service)
        self.assertEqual(faults.SPACE, service[service.index('--space') + 1])
        scopes = lambda argv: {argv[index + 1] for index, item in enumerate(argv) if item == '--scope'}
        actual_source = (ROOT / 'crates/auth/src/service_token.rs').read_text()
        block = actual_source.split('pub const GLOBAL_ADMIN_SCOPES: &[&str] = &[', 1)[1].split('];', 1)[0]
        allowed = set(faults.re.findall(r'"([a-z_:]+)"', block))
        self.assertTrue(scopes(operator) <= allowed)
        self.assertEqual(set(faults.SERVICE_SCOPES), scopes(service))
        self.assertNotIn('write:entity', scopes(operator))
        self.assertIn('write:entity', scopes(service))

    def allocation(self):
        inventory = faults.plan()
        review = {'format': 'recall-installed-fault-review/v1', 'reviewer': 'qualification', 'approved': True,
                  **{key: inventory[key] for key in ('expectations_sha256', 'driver_sha256', 'dependency_sha256')}}
        path = self.root / 'review.json'
        path.write_bytes(faults.encoded(review))
        manifest = {'format': 'recall-installed-fault-run/v1', 'execution_authorized': True,
                    'expires_at': '2030-01-01T00:00:00Z', 'source_commit': 'a' * 40,
                    'archive_sha256': 'b' * 64, 'binary_sha256': 'c' * 64, 'review_sha256': faults.digest(path),
                    'archive': str(self.root / 'archive.tar.gz'), 'run_directory': str(self.root / 'new-run'),
                    'review': str(path), 'paired_bundle': None}
        packet = self.root / 'allocation.json'
        packet.write_bytes(faults.encoded(manifest))
        return packet, manifest, path, review

    def test_exact_independent_review_required(self):
        packet, manifest, path, review = self.allocation()
        validated, _inventory = faults.validate_allocation(packet, dt.datetime(2026, 10, 7, tzinfo=dt.timezone.utc))
        self.assertEqual(manifest, validated)
        review['driver_sha256'] = 'f' * 64
        path.write_bytes(faults.encoded(review))
        manifest['review_sha256'] = faults.digest(path)
        packet.write_bytes(faults.encoded(manifest))
        with self.assertRaisesRegex(ValueError, 'driver_sha256 differs'):
            faults.validate_allocation(packet)

    def test_expired_or_unallocated_refused(self):
        packet, manifest, _path, _review = self.allocation()
        with self.assertRaisesRegex(ValueError, 'expired'):
            faults.validate_allocation(packet, dt.datetime(2031, 1, 1, tzinfo=dt.timezone.utc))
        manifest['execution_authorized'] = False
        packet.write_bytes(faults.encoded(manifest))
        with self.assertRaisesRegex(ValueError, 'not allocated'):
            faults.validate_allocation(packet)

    def test_review_dependency_change_refused(self):
        packet, manifest, path, review = self.allocation()
        review['dependency_sha256'][faults.DEPENDENCIES[0]] = 'e' * 64
        path.write_bytes(faults.encoded(review))
        manifest['review_sha256'] = faults.digest(path)
        packet.write_bytes(faults.encoded(manifest))
        with self.assertRaisesRegex(ValueError, 'dependency_sha256 differs'):
            faults.validate_allocation(packet)

    def bare_driver(self):
        driver = faults.Driver.__new__(faults.Driver)
        driver.inventory = faults.plan()
        driver.report = {'cases': []}
        driver.root = self.root
        driver.saved = 0
        driver.children = []
        driver.candidate_pidfd = None
        (self.root / 'captures').mkdir()
        return driver

    def test_unmet_case_never_becomes_pass(self):
        driver = self.bare_driver()
        def missing():
            raise faults.UnmetPrerequisite('matching native binary absent')
        driver.case('bad_backend_credentials', missing)
        driver.finish()
        self.assertFalse(driver.report['installed_cases_passed'])
        self.assertEqual(7, len(driver.report['cases']))
        self.assertTrue(all(row['verdict'] == 'unmet_prerequisite' for row in driver.report['cases']))

    def test_failed_case_preserves_failure_and_marks_unexecuted(self):
        driver = self.bare_driver()
        with self.assertRaises(ValueError):
            driver.case('configuration_os_enospc', lambda: faults.require(False, 'different old bytes'))
        driver.finish()
        self.assertEqual('fail', driver.report['cases'][0]['verdict'])
        self.assertFalse(driver.report['installed_cases_passed'])
        self.assertEqual(6, sum(row['verdict'] == 'unmet_prerequisite' for row in driver.report['cases']))

    def test_only_complete_inventory_can_pass(self):
        driver = self.bare_driver()
        for name in driver.inventory['expectations']['required_cases']:
            driver.case(name, lambda: {'mocked_only': True})
        driver.finish()
        self.assertTrue(driver.report['installed_cases_passed'])
        # This is a driver reducer assertion, never real installed qualification.

    def test_preview_transfer_checks_source_and_absent_target(self):
        driver = self.bare_driver()
        driver.checkpoint = self.root / 'live.snapshot'
        driver.checkpoint.write_bytes(b'private source fixture')
        driver.negative_cli = mock.Mock(return_value={'exit_code': 1})
        result = driver.unsupported_transfer()
        self.assertTrue(result['target_absent'])
        self.assertEqual(faults.digest(driver.checkpoint), result['source_sha256'])
        self.assertIn(str(driver.checkpoint), driver.negative_cli.call_args.args[1])
        (self.root / 'negative').mkdir()
        (self.root / 'negative/never-created-target').mkdir()
        with self.assertRaisesRegex(ValueError, 'mutated source or target'):
            driver.unsupported_transfer()

    def test_wrong_credentials_missing_bundle_is_unmet(self):
        driver = self.bare_driver()
        driver.allocation = {'paired_bundle': None}
        with self.assertRaises(faults.UnmetPrerequisite):
            driver.bad_credentials()

    def test_actual_acknowledged_entity_equality_is_required(self):
        driver = self.bare_driver()
        driver.entities = [{'id': 'entity:one', 'summary': 'exact bytes', 'security': {'level': 0}}]
        driver.http = mock.Mock(return_value=(200, driver.entities[0]))
        self.assertEqual(driver.entities, driver.verify_entities())
        driver.http.return_value = (200, {**driver.entities[0], 'summary': 'changed bytes'})
        with self.assertRaisesRegex(ValueError, 'differs'):
            driver.verify_entities()

    def test_checkpoint_failure_requires_same_wal_and_original_journal(self):
        driver = self.bare_driver()
        driver.checkpoint = self.root / 'state.snapshot'
        driver.checkpoint.write_bytes(b'old good checkpoint')
        driver.wal = self.root / 'commits.wal'
        driver.wal.write_bytes(b'acknowledged WAL')
        driver.trace = self.root / 'trace'
        driver.trace.write_bytes(b'')
        driver.before_fault = {'checkpoint_sha256': faults.digest(driver.checkpoint),
                               'wal_sha256': faults.digest(driver.wal), 'journal': {'journal-id': 'original'}}
        driver.filler = self.root / 'filler'
        driver.filler.write_bytes(b'owned')
        driver.journal = mock.Mock(return_value={'journal-id': 'original'})
        driver.launch, driver.ready, driver.verify_entities = mock.Mock(), mock.Mock(return_value={}), mock.Mock(return_value=[])
        def stop(**_kwargs):
            driver.trace.write_text('12 write(9<' + str(driver.checkpoint) + '.durable/checkpoint.next>, "x", 1) = -1 ENOSPC\n')
            driver.wal.write_bytes(b'changed WAL')
            return {'exit_code': 1}
        driver.stop = stop
        with self.assertRaisesRegex(ValueError, 'changed acknowledged WAL'):
            driver.full_checkpoint()
        self.assertTrue(driver.filler.exists())
        driver.launch.assert_not_called()


if __name__ == '__main__':
    unittest.main()
