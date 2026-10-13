"""F-owned mock controls; no process, signal, listener, provider or artifact run."""
from __future__ import annotations

import datetime as dt
import importlib.util
import importlib.machinery
import io
import json
import os
from pathlib import Path
import signal
import sys
import tempfile
import threading
import time
import types
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('installed_capture_driver_test', ROOT / 'scripts/qualification/installed_linux_offline.py')
driver = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(driver)
producer = driver.process_capture


def stat_bytes(pid=41001, parent=42001, *, start=123, state='S'):
    fields = [state, str(parent), str(pid), str(pid), *(['0'] * 15), str(start)]
    return (str(pid) + ' (original (name)) ' + ' '.join(fields) + '\n').encode()


class ClosedWorld(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / 'captures').mkdir()
        (self.root / 'service').mkdir()
        self.binary = self.root / 'never-execute'
        self.binary.write_bytes(b'original mock-only executable bytes')
        self.logs = [self.root / 'captures' / ('original.' + suffix) for suffix in ('stdout', 'stderr')]
        self.deadline = time.monotonic() + 30
        self.journal = producer.CaptureJournal(self.root)
        for target, name in ((driver.subprocess, 'Popen'), (driver.os, 'killpg'), (driver.os, 'waitid'),
                             (driver.socket, 'socket'), (driver.threading, 'Thread')):
            guard = patch.object(target, name, side_effect=AssertionError('unmocked process/network operation'))
            guard.start()
            self.addCleanup(guard.stop)

    def proof(self, arguments=None):
        return producer.ProcessProof(self.binary, arguments or ['--version'], self.root / 'service',
            {'PATH': '/reviewed', 'SYNTHETIC_KEY': 'must-not-be-recorded'}, self.logs,
            int(self.deadline * 1e9), journal=self.journal)

    def launch_patches(self, proof, *, zombie=False, changed=False):
        actual = {**proof.launch_binary, 'path': '/proc/41001/exe'}
        original_read = producer.read_bounded
        stats = iter([stat_bytes(state='Z' if zombie else 'S'),
                      stat_bytes(start=124 if changed else 123, state='Z' if zombie else 'S')])
        def read(path, maximum):
            return next(stats) if str(path) == '/proc/41001/stat' else original_read(path, maximum)
        return [patch.object(producer, 'read_bounded', side_effect=read),
                patch.object(producer, 'binary_identity', return_value=actual),
                patch.object(producer.os, 'getpid', return_value=42001),
                patch.object(producer.os, 'readlink', side_effect=FileNotFoundError() if zombie else None,
                             return_value=str(self.binary))]

    def enter_patches(self, patches):
        for item in patches:
            item.start()
            self.addCleanup(item.stop)


class OriginalProcessTests(ClosedWorld):
    def test_prelaunch_original_intent_and_binary_are_durable_before_any_child(self):
        proof = self.proof()
        self.assertEqual([item['action'] for item in proof.events], ['intent', 'launch-binary'])
        self.assertEqual(proof.intent['argv'], [str(self.binary), '--version'])
        self.assertEqual(proof.intent['environment_names'], ['PATH', 'SYNTHETIC_KEY'])
        self.assertNotIn('must-not-be-recorded', json.dumps(proof.snapshot()))
        self.assertEqual(proof.launch_binary['sha256'], producer.sha(self.binary.read_bytes()))
        for item in self.journal.files:
            self.assertEqual(Path(item).stat().st_mode & 0o777, 0o600)
        self.assertFalse(proof.snapshot()['reaped'])
        self.assertTrue(all(row['ref'] is None for row in proof.snapshot()['logs']))

    def test_exact_unreaped_birth_brackets_actual_executable(self):
        proof = self.proof()
        self.enter_patches(self.launch_patches(proof))
        proof.launched(41001)
        self.assertEqual(proof.identity, {'pid': 41001, 'ppid': 42001, 'pgrp': 41001, 'session': 41001, 'start_ticks': 123})
        self.assertEqual(proof.launch_stat_raw, stat_bytes())
        self.assertEqual(proof.launch_stat_after_raw, stat_bytes())
        self.assertEqual(proof.launch_executable['sha256'], proof.launch_binary['sha256'])
        self.assertEqual(len([ref for ref in self.journal.files.values() if ref['kind'] == 'process-stat']), 2)

    def test_already_zombie_retains_original_stat_without_fabricated_live_executable(self):
        proof = self.proof()
        self.enter_patches(self.launch_patches(proof, zombie=True))
        proof.launched(41001)
        self.assertIsNone(proof.launch_executable)
        self.assertEqual(proof.events[-1]['data']['executable_error'], 'FileNotFoundError')
        self.assertEqual(proof.launch_stat_after_raw, stat_bytes(state='Z'))
        self.assertEqual(proof.errors, [])

    def test_changed_process_identity_does_not_gain_a_new_birth(self):
        proof = self.proof()
        self.enter_patches(self.launch_patches(proof, changed=True))
        with self.assertRaisesRegex(ValueError, 'identity changed'):
            proof.launched(41001)
        self.assertEqual(proof.identity['start_ticks'], 123)
        self.assertFalse(any(item['action'] == 'launched' for item in proof.events))

    def test_missing_executable_for_non_zombie_is_not_a_completed_launch(self):
        proof = self.proof()
        patches = self.launch_patches(proof)
        patches[-1] = patch.object(producer.os, 'readlink', side_effect=FileNotFoundError())
        # Keep returning the same live identity through the bounded fallback;
        # neither a finite mock iterator nor wall-clock scheduling is the oracle.
        original_read = producer.read_bounded
        patches[0] = patch.object(producer, 'read_bounded', side_effect=lambda path, maximum:
            stat_bytes() if str(path) == '/proc/41001/stat' else original_read(path, maximum))
        patches += [patch.object(producer.time, 'sleep'),
                    patch.object(producer.time, 'monotonic_ns', return_value=time.monotonic_ns())]
        self.enter_patches(patches)
        with self.assertRaisesRegex(ValueError, 'not an observed original zombie'):
            proof.launched(41001)
        self.assertEqual(len([event for event in proof.events
                              if event['action'] == 'launch-exit-observation']), 16)
        self.assertFalse(any(event['action'] == 'launched' for event in proof.events))

    def test_original_log_bytes_require_exit_reap_and_actual_eof_closure(self):
        proof = self.proof()
        for path in self.logs:
            path.write_bytes(b'original log bytes')
        proof.log_closed(str(self.logs[0]), True, None)
        proof.log_closed(str(self.logs[1]), False, 'LogBoundExceeded')
        self.assertFalse(proof.snapshot()['logs'][0]['complete'])
        proof.exit_observed(0, 0, os.CLD_EXITED)
        proof.reaped_child(0)
        rows = proof.snapshot()['logs']
        self.assertTrue(rows[0]['complete'])
        self.assertEqual(rows[0]['ref']['sha256'], producer.sha(b'original log bytes'))
        self.assertFalse(rows[1]['complete'])
        self.assertIsNone(rows[1]['ref'])
        with self.assertRaisesRegex(ValueError, 'already reaped'):
            proof.reaped_child(0)

    def test_failed_prelaunch_hash_preserves_original_attempt_in_journal(self):
        with patch.object(producer, 'binary_identity', side_effect=OSError('mock original failure')):
            with self.assertRaises(OSError):
                self.proof()
        self.assertEqual(len(self.journal.processes), 1)
        row = self.journal.processes[0].snapshot()
        self.assertIsNone(row['identity'])
        self.assertFalse(row['reaped'])
        self.assertEqual(row['errors'][0]['action'], 'launch-binary')
        self.assertEqual(row['intent']['argv'], [str(self.binary), '--version'])

    def test_expired_binary_capture_does_not_replace_original_input(self):
        with self.assertRaisesRegex(ValueError, 'deadline'):
            producer.binary_identity(self.binary, time.monotonic_ns() - 1)
        alias = self.root / 'alias'
        alias.symlink_to(self.binary)
        with self.assertRaises(OSError):
            producer.binary_identity(alias, int(self.deadline * 1e9))

    def test_inventory_is_raw_content_addressed_and_never_replaces_a_path(self):
        self.proof()
        inventory = self.journal.inventory()
        self.assertEqual(inventory['format'], 'recall-dynamic-capture-inventory/v2')
        for item in inventory['files']:
            self.assertEqual(item['ref']['sha256'], producer.sha(Path(item['path']).read_bytes()))
            self.assertEqual(item['ref']['bytes'], Path(item['path']).stat().st_size)
        with self.assertRaisesRegex(ValueError, 'replaced'):
            self.journal.save('captures/process-event-0001.json', b'new', 'installed-process-event')
        with self.assertRaisesRegex(ValueError, 'escaped'):
            self.journal.save('private/token', b'never', 'forbidden')

    def test_metadata_and_http_share_original_capture_capacity(self):
        value = driver.Driver.__new__(driver.Driver)
        value.capture_bytes = 80
        value.capture_budget_lock = threading.RLock()
        value.cases = {'limits': {'http_capture_bytes': 100}}
        journal = producer.CaptureJournal(self.root, reserve=value.reserve_capture)
        journal.save('captures/original.bin', b'1234567890', 'mock-original')
        self.assertEqual(value.capture_bytes, 90)
        value.reserve_capture(10)
        with self.assertRaisesRegex(ValueError, 'storage limit'):
            journal.save('captures/overflow.bin', b'1', 'mock-original')
        self.assertFalse((self.root / 'captures/overflow.bin').exists())
        self.assertEqual(value.capture_bytes, 100)


class ExitTransitionTests(ClosedWorld):
    def observe(self, proof, samples, *, exe_error=None):
        pending = iter(samples)
        original_read = producer.read_bounded
        self.clock = [time.monotonic_ns()]
        def read(path, maximum):
            if str(path) != '/proc/41001/stat':
                return original_read(path, maximum)
            value = next(pending)
            if isinstance(value, BaseException):
                raise value
            return value
        def sleep(seconds):
            self.clock[0] += int(seconds * 1e9)
        self.reader = Mock(side_effect=read)
        self.sleeper = Mock(side_effect=sleep)
        self.enter_patches([
            patch.object(producer, 'read_bounded', self.reader),
            patch.object(producer.os, 'getpid', return_value=42001),
            patch.object(producer.os, 'readlink', side_effect=exe_error or FileNotFoundError()),
            patch.object(producer.time, 'monotonic_ns', side_effect=lambda: self.clock[0]),
            patch.object(producer.time, 'sleep', self.sleeper),
        ])

    def test_exit_transition_requires_captured_same_birth_zombie(self):
        proof = self.proof()
        self.observe(proof, [stat_bytes(state='R'), stat_bytes(state='R'),
                             stat_bytes(state='S'), stat_bytes(state='Z')])
        proof.launched(41001)
        self.assertEqual(proof.launch_stat_raw, stat_bytes(state='R'))
        self.assertEqual(proof.launch_stat_after_raw, stat_bytes(state='Z'))
        self.assertIsNone(proof.launch_executable)
        self.assertFalse(proof.reaped)
        self.assertIsNone(proof.observed_exit)
        self.assertTrue(all(not row['complete'] for row in proof.snapshot()['logs']))
        observed = [row for row in proof.events if row['action'] == 'launch-exit-observation']
        self.assertEqual(len(observed), 3)
        for event, state in zip(observed, ['R', 'S', 'Z']):
            ref = event['data']['stat']
            raw = Path(ref['path']).read_bytes()
            self.assertEqual(raw, stat_bytes(state=state))
            self.assertEqual(ref['ref']['sha256'], producer.sha(raw))
        self.assertEqual(proof.events[-1]['action'], 'launched')
        self.assertEqual(proof.events[-1]['data']['executable_error'], 'FileNotFoundError')
        self.assertEqual(self.sleeper.call_count, 2)

    def test_exit_transition_rejects_changed_identity_even_if_zombie(self):
        proof = self.proof()
        changed = stat_bytes(start=124, state='Z')
        self.observe(proof, [stat_bytes(), stat_bytes(), changed])
        with self.assertRaisesRegex(ValueError, 'identity changed'):
            proof.launched(41001)
        self.assertEqual(proof.identity['start_ticks'], 123)
        self.assertFalse(any(row['action'] == 'launched' for row in proof.events))
        self.assertEqual(Path(proof.events[-1]['data']['stat']['path']).read_bytes(), changed)
        self.assertFalse(proof.reaped)

    def test_exit_transition_disappearance_cannot_supply_zombie_evidence(self):
        proof = self.proof()
        self.observe(proof, [stat_bytes(), stat_bytes(), FileNotFoundError()])
        with self.assertRaises(FileNotFoundError):
            proof.launched(41001)
        self.assertFalse(any(row['action'] == 'launched' for row in proof.events))
        self.assertEqual(len([row for row in proof.events if row['action'] == 'launch-exit-observation']), 1)
        self.assertFalse(proof.reaped)

    def test_exit_transition_permission_error_does_not_enter_missing_exe_fallback(self):
        proof = self.proof()
        self.observe(proof, [stat_bytes()], exe_error=PermissionError())
        with self.assertRaises(PermissionError):
            proof.launched(41001)
        self.assertFalse(any(row['action'] in {'launched', 'launch-exit-observation'} for row in proof.events))
        self.sleeper.assert_not_called()
        self.assertFalse(proof.reaped)

    def test_exit_transition_uses_original_deadline_without_late_zombie_read(self):
        proof = self.proof()
        self.observe(proof, [stat_bytes(), stat_bytes(), stat_bytes(state='Z')])
        proof.deadline_ns = self.clock[0] + 2_000_000
        deadline = proof.deadline_ns
        with self.assertRaisesRegex(ValueError, 'deadline expired'):
            proof.launched(41001)
        self.assertEqual(proof.deadline_ns, deadline)
        self.assertEqual(self.reader.call_count, 2)
        self.sleeper.assert_called_once_with(0.002)
        self.assertFalse(any(row['action'] == 'launched' for row in proof.events))

    def test_exit_transition_capture_failure_cannot_complete_launch(self):
        proof = self.proof()
        self.observe(proof, [stat_bytes(), stat_bytes(), stat_bytes(state='Z')])
        with patch.object(self.journal, 'reserve', side_effect=[None, ValueError('original metadata budget exhausted')]):
            with self.assertRaisesRegex(ValueError, 'metadata budget'):
                proof.launched(41001)
        self.assertFalse(any(row['action'] == 'launched' for row in proof.events))
        self.sleeper.assert_not_called()


class SnapshotOutputTests(ClosedWorld):
    def arguments(self):
        return ['snapshot', 'inspect', '--input', str(self.root / 'original-bundle'),
                '--json', '--include-records', '--compact-records']

    def report_proof(self, **changes):
        values = {'stream_limits': (33554433, 1048576),
                  'stream_kinds': ('snapshot-inspection', 'process-log')}
        values.update(changes)
        return producer.ProcessProof(self.binary, self.arguments(), self.root / 'service',
            {'PATH': '/reviewed'}, self.logs, int(self.deadline * 1e9), journal=self.journal, **values)

    def shared_journal(self, reserve=None):
        self.reservations = []
        self.journal = producer.CaptureJournal(self.root, reserve=reserve or self.reservations.append)

    def test_fixed_roles_and_whole_output_hold_are_original_before_launch(self):
        self.shared_journal()
        proof = self.report_proof()
        self.assertEqual(self.reservations[0], 34603009)
        self.assertEqual(sum(self.reservations[1:]), self.journal.written)
        self.assertTrue(all(amount > 0 for amount in self.reservations))
        expected = [{'path': str(path), 'kind': kind, 'maximum_bytes': maximum, 'reserved_bytes': maximum}
                    for path, kind, maximum in zip(self.logs,
                        ('snapshot-inspection', 'process-log'), (33554433, 1048576))]
        self.assertEqual(proof.intent['streams'], expected)
        original = json.loads(Path(proof.events[0]['capture']['path']).read_bytes())
        self.assertEqual(original['data']['streams'], expected)
        self.assertEqual(proof.validate_stream_limits((33554433, 1048576), 1048576), (33554433, 1048576))
        self.assertTrue(all(not path.exists() for path in self.logs))

    def test_report_exception_refuses_unreserved_or_generic_commands_and_caps(self):
        with self.assertRaisesRegex(ValueError, 'shared capture reservation'):
            self.report_proof()
        self.shared_journal()
        cases = [({'stream_limits': (33554434, 1048576)}, self.arguments()),
                 ({'stream_limits': (33554433, 1048577)}, self.arguments()),
                 ({'stream_limits': [33554433, 1048576]}, self.arguments()),
                 ({'stream_kinds': ('process-log', 'process-log')}, self.arguments()),
                 ({'stream_kinds': ('snapshot-inspection-report', 'process-log')}, self.arguments()),
                 ({}, ['serve']), ({}, self.arguments() + ['--other']),
                 ({}, ['snapshot', 'inspect', '--input', 'relative', '--json', '--include-records', '--compact-records'])]
        for changes, arguments in cases:
            with self.subTest(changes=changes, arguments=arguments), patch.object(self, 'arguments', return_value=arguments):
                with self.assertRaises(ValueError):
                    self.report_proof(**changes)
        self.assertEqual(self.reservations, [])
        self.assertFalse(self.journal.processes)

    def test_refused_original_output_hold_has_no_child_or_output_file(self):
        reserve = Mock(side_effect=ValueError('original capture exhausted'))
        self.shared_journal(reserve)
        with self.assertRaisesRegex(ValueError, 'original capture exhausted'):
            self.report_proof()
        proof = self.journal.processes[0]
        self.assertEqual(proof.errors[0]['action'], 'output-reservation')
        self.assertFalse(any(event['action'] == 'intent' for event in proof.events))
        self.assertTrue(all(not path.exists() for path in self.logs))
        self.assertIsNone(proof.identity)
        self.assertFalse(proof.reaped)

    def launch_report(self, proof, raw):
        child = Mock(pid=41001, stdout=io.BytesIO(raw), stderr=io.BytesIO(b'original stderr'), returncode=0)
        child.wait.return_value = 0
        thread = Mock(ident=1, is_alive=Mock(return_value=False))
        self.enter_patches(self.launch_patches(proof))
        with patch.object(driver.subprocess, 'Popen', return_value=child), \
             patch.object(driver.threading, 'Thread', return_value=thread) as threads:
            owned = driver.OwnedProcess(self.binary, self.arguments(), self.root / 'service',
                {'PATH': '/reviewed'}, self.logs, 1048576, initialization_deadline=self.deadline,
                capture_proof=proof, stream_limits=(33554433, 1048576))
        self.assertEqual([call.kwargs['args'][2] for call in threads.call_args_list], [33554433, 1048576])
        return owned, child

    def test_original_report_over_four_megabytes_keeps_its_single_kind_and_complete_bytes(self):
        self.shared_journal()
        proof = self.report_proof()
        raw = b'original report\n' + b'x' * (4 * 1024 * 1024)
        owned, child = self.launch_report(proof, raw)
        owned.drain(child.stdout, owned.logs[0], 33554433)
        owned.drain(child.stderr, owned.logs[1], 1048576)
        event = types.SimpleNamespace(si_status=0, si_code=os.CLD_EXITED)
        with patch.object(driver.os, 'waitid', return_value=event), patch.object(driver.os, 'killpg'):
            self.assertEqual(owned.wait(5), 0)
        snapshot = proof.snapshot()
        self.assertTrue(all(row['complete'] for row in snapshot['logs']))
        self.assertEqual(snapshot['logs'][0]['ref'],
                         {'sha256': producer.sha(raw), 'bytes': len(raw), 'kind': 'snapshot-inspection'})
        inventory = self.journal.inventory([(self.logs[0], 'snapshot-inspection', 33554433),
                                            (self.logs[1], 'process-log', 1048576)])
        ref = next(row['ref'] for row in inventory['files'] if row['path'] == str(self.logs[0]))
        self.assertEqual(ref, snapshot['logs'][0]['ref'])
        self.assertEqual(self.reservations.count(34603009), 1)

    def test_one_byte_over_original_report_cap_never_yields_complete_prefix(self):
        self.shared_journal()
        proof = self.report_proof()
        owned, child = self.launch_report(proof, b'x' * (33554433 + 1))
        with patch.object(driver.os, 'killpg'):
            owned.drain(child.stdout, owned.logs[0], 33554433)
        owned.drain(child.stderr, owned.logs[1], 1048576)
        proof.exit_observed(-signal.SIGKILL, signal.SIGKILL, os.CLD_KILLED)
        proof.reaped_child(-signal.SIGKILL)
        self.assertTrue(owned.failed.is_set())
        self.assertEqual(self.logs[0].stat().st_size, 33554433)
        self.assertEqual(proof.snapshot()['logs'][0]['closure']['error_type'], 'LogBoundExceeded')
        self.assertFalse(proof.snapshot()['logs'][0]['complete'])
        self.assertIsNone(proof.snapshot()['logs'][0]['ref'])

    def test_owned_process_cannot_omit_or_change_original_stream_limits(self):
        self.shared_journal()
        proof = self.report_proof()
        with patch.object(driver.subprocess, 'Popen') as popen:
            for limits in (None, (33554433, 4194304), (4194304, 1048576)):
                with self.subTest(limits=limits), self.assertRaises(ValueError):
                    driver.OwnedProcess(self.binary, self.arguments(), self.root / 'service',
                        {'PATH': '/reviewed'}, self.logs, 1048576, initialization_deadline=self.deadline,
                        capture_proof=proof, stream_limits=limits)
        popen.assert_not_called()
        self.assertTrue(all(not path.exists() for path in self.logs))

    def test_late_report_role_or_reservation_rewrite_is_refused(self):
        self.shared_journal()
        proof = self.report_proof()
        original = json.loads(Path(proof.events[0]['capture']['path']).read_bytes())
        proof.intent['streams'][0]['reserved_bytes'] = 0
        with self.assertRaisesRegex(ValueError, 'intent changed'):
            proof.snapshot()
        with self.assertRaisesRegex(ValueError, 'intent changed'):
            proof.validate_stream_limits((33554433, 1048576), 1048576)
        self.assertEqual(original['data']['streams'][0]['reserved_bytes'], 33554433)

    def test_ordinary_proof_has_no_exception_and_keeps_four_megabyte_read_bound(self):
        proof = self.proof()
        self.assertNotIn('streams', proof.intent)
        self.assertEqual(proof.validate_stream_limits(None, 1048576), (1048576, 1048576))
        with self.assertRaisesRegex(ValueError, 'not reserved'):
            proof.validate_stream_limits((33554433, 1048576), 1048576)
        for path in self.logs:
            path.write_bytes(b'x' * (4 * 1024 * 1024 + 1))
            proof.log_closed(str(path), True, None)
        proof.exit_observed(0, 0, os.CLD_EXITED)
        proof.reaped_child(0)
        with self.assertRaisesRegex(ValueError, 'exceeds bound'):
            proof.snapshot()


class OwnedBoundaryTests(ClosedWorld):
    def owned(self, proof):
        child = Mock(pid=41001, stdout=io.BytesIO(b'original stdout'), stderr=io.BytesIO(b'original stderr'), returncode=0)
        child.wait.return_value = 0
        thread = Mock(ident=1, is_alive=Mock(return_value=False))
        self.enter_patches(self.launch_patches(proof))
        with patch.object(driver.subprocess, 'Popen', return_value=child), \
             patch.object(driver.threading, 'Thread', return_value=thread):
            owned = driver.OwnedProcess(self.binary, ['--version'], self.root / 'service',
                {'PATH': '/reviewed', 'SYNTHETIC_KEY': 'must-not-be-recorded'}, self.logs, 1024,
                initialization_deadline=self.deadline, capture_proof=proof)
        # Explicit synchronous doubles exercise the actual drain body without
        # creating a thread or child. The bytes come from this original mock.
        owned.drain(child.stdout, owned.logs[0], 1024)
        owned.drain(child.stderr, owned.logs[1], 1024)
        return owned, child

    def test_actual_custody_wait_signals_descendants_then_records_single_reap(self):
        proof = self.proof()
        owned, child = self.owned(proof)
        event = types.SimpleNamespace(si_status=0, si_code=os.CLD_EXITED)
        with patch.object(driver.os, 'waitid', return_value=event), patch.object(driver.os, 'killpg') as kill:
            self.assertEqual(owned.wait(5), 0)
        kill.assert_called_once_with(41001, signal.SIGKILL)
        child.wait.assert_called_once()
        snapshot = proof.snapshot()
        self.assertTrue(snapshot['reaped'])
        self.assertTrue(all(row['complete'] for row in snapshot['logs']))
        signals = [event['data'] for event in proof.events if event['action'] == 'signal']
        self.assertEqual([event['outcome'] for event in signals], ['requested', 'sent'])
        self.assertTrue(all(event['phase'] == 'post-exit-descendant-cleanup' for event in signals))
        self.assertEqual(owned.launch_stat_raw, stat_bytes())

    def test_failed_group_cleanup_retains_reserved_original_leader(self):
        proof = self.proof()
        owned, child = self.owned(proof)
        event = types.SimpleNamespace(si_status=0, si_code=os.CLD_EXITED)
        with patch.object(driver.os, 'waitid', return_value=event), \
             patch.object(driver.os, 'killpg', side_effect=PermissionError('mock original group failure')):
            with self.assertRaises(PermissionError):
                owned.wait(5)
        child.wait.assert_not_called()
        self.assertFalse(owned.reaped)
        self.assertFalse(proof.snapshot()['reaped'])
        self.assertEqual(proof.errors[-1]['action'], 'signal')

    def test_capture_cannot_be_attached_to_replacement_arguments(self):
        proof = self.proof()
        with patch.object(driver.subprocess, 'Popen') as popen:
            with self.assertRaisesRegex(ValueError, 'another invocation'):
                driver.OwnedProcess(self.binary, ['different'], self.root / 'service', {}, self.logs, 1024,
                    initialization_deadline=self.deadline, capture_proof=proof)
        popen.assert_not_called()
        self.assertEqual(proof.intent['argv'], [str(self.binary), '--version'])

    def test_popen_failure_retains_intent_without_manufactured_pid(self):
        proof = self.proof()
        with patch.object(driver.subprocess, 'Popen', side_effect=OSError('mock launch refused')):
            with self.assertRaises(OSError):
                driver.OwnedProcess(self.binary, ['--version'], self.root / 'service',
                    {'PATH': '/reviewed', 'SYNTHETIC_KEY': 'must-not-be-recorded'}, self.logs, 1024,
                    initialization_deadline=self.deadline, capture_proof=proof)
        self.assertIsNone(proof.identity)
        self.assertFalse(proof.reaped)
        self.assertEqual(proof.events[2]['action'], 'popen-requested')
        self.assertEqual(proof.errors[-1]['action'], 'initialization')

    def test_terminal_sidecar_contains_original_logs_without_rewriting_case_outcomes(self):
        proof = self.proof()
        owned, _ = self.owned(proof)
        with patch.object(driver.os, 'waitid', return_value=types.SimpleNamespace(si_status=0, si_code=os.CLD_EXITED)), \
             patch.object(driver.os, 'killpg'):
            owned.wait(5)
        value = driver.Driver.__new__(driver.Driver)
        value.root, value.process_journal = self.root, self.journal
        value.serial, value.requests, value.child, value.owned_commands = 1, 0, None, []
        value.started = time.monotonic()
        value.cases = {'limits': {'process_log_bytes': 1024, 'total_seconds': 300}}
        value.manifest = {'expires_at': '2999-01-01T00:00:00Z'}
        value.case_proofs = [{'case': 'original-case', 'process_indices': [1], 'request_indices': []}]
        value.report = {'cases': [{'case': 'original-case', 'observation': 'passed'}], 'product_acceptance': False}
        for name in ('controller_capture', 'review_capture', 'manifest_capture'):
            setattr(value, name, self.journal.save('captures/' + name + '.json', b'{"original":true}', 'mock-original'))
        before = json.dumps(value.report, sort_keys=True)
        value.finish_process_capture()
        self.assertEqual(json.dumps(value.report, sort_keys=True), before)
        journal = json.loads((self.root / 'captures/process-journal.json').read_bytes())
        self.assertTrue(journal['complete'])
        self.assertEqual(journal['processes'][0]['intent']['argv'], proof.intent['argv'])
        inventory = json.loads((self.root / 'captures/capture-inventory.json').read_bytes())
        logs = [row for row in inventory['files'] if row['ref']['kind'] == 'process-log']
        self.assertEqual(len(logs), 2)
        for row in logs:
            self.assertEqual(row['ref']['sha256'], producer.sha(Path(row['path']).read_bytes()))


class OriginalHelperSourceTests(ClosedWorld):
    def registry(self):
        registry = {}
        for name in driver.DEPENDENCY_NAMES:
            path = self.root / (name + '.py')
            raw = b'VALUE = 23\n'
            spec = importlib.util.spec_from_file_location(name, path)
            value = importlib.util.module_from_spec(spec)
            exec(compile(raw, str(path), 'exec', dont_inherit=True), value.__dict__)
            registry[name] = {'module': value, 'path': str(path), 'source_bytes': raw,
                              'sha256': producer.sha(raw)}
            self.addCleanup(driver.LOADED_SOURCE_HASHES.pop, value, None)
        return registry

    def test_injected_original_modules_are_reused_without_source_read_or_execution(self):
        registry = self.registry()
        entry = registry['fixtures']
        foreign = types.ModuleType('fixtures')
        with patch.object(driver, 'VERIFIED_DEPENDENCIES', registry), \
             patch.object(driver, 'original_module_source', side_effect=AssertionError('dependency reread')), \
             patch.object(importlib.machinery.SourceFileLoader, 'exec_module', side_effect=AssertionError('dependency reexec')), \
             patch.dict(sys.modules, {'fixtures': foreign}):
            actual = driver.module('fixtures', Path(entry['path']), entry['sha256'])
            self.assertIs(actual, entry['module'])
            self.assertEqual(actual.VALUE, 23)
            self.assertEqual(driver.loaded_digest(actual), entry['sha256'])
            self.assertIs(sys.modules['fixtures'], foreign)
        self.assertFalse(Path(entry['path']).exists())

    def test_missing_or_mismatched_injected_dependency_never_falls_back(self):
        registry = self.registry()
        entry = registry['fixtures']
        cases = [({}, 'fixtures', Path(entry['path']), entry['sha256']),
                 (registry, 'foreign', Path(entry['path']), entry['sha256']),
                 (registry, 'fixtures', self.root / 'different.py', entry['sha256']),
                 (registry, 'fixtures', Path(entry['path']), '0' * 64)]
        for values, name, path, expected in cases:
            with self.subTest(name=name, path=path, expected=expected), \
                 patch.object(driver, 'VERIFIED_DEPENDENCIES', values), \
                 patch.object(driver, 'original_module_source', side_effect=AssertionError('unreviewed fallback')):
                with self.assertRaises(ValueError):
                    driver.module(name, path, expected)
        for field, replacement in [('source_bytes', b'VALUE = 99\n'), ('path', '/foreign.py'), ('module', object())]:
            changed = {**registry, 'fixtures': {**entry, field: replacement}}
            with self.subTest(field=field), patch.object(driver, 'VERIFIED_DEPENDENCIES', changed):
                with self.assertRaises(ValueError):
                    driver.module('fixtures', Path(entry['path']))

    def test_injected_registry_cannot_recertify_old_module_with_replacement_bytes(self):
        registry = self.registry()
        entry = registry['fixtures']
        with patch.object(driver, 'VERIFIED_DEPENDENCIES', registry):
            actual = driver.module('fixtures', Path(entry['path']))
            entry.update(source_bytes=b'VALUE = 99\n', sha256=producer.sha(b'VALUE = 99\n'))
            with self.assertRaisesRegex(ValueError, 'cannot be replaced'):
                driver.module('fixtures', Path(entry['path']))
            with self.assertRaises(ValueError):
                driver.loaded_digest(actual)
        self.assertEqual(actual.VALUE, 23)

    def test_injected_module_metadata_must_match_original_name_and_path(self):
        registry = self.registry()
        entry = registry['fixtures']
        for attribute, replacement in [('__name__', 'foreign'), ('__file__', '/foreign.py'), ('__spec__', None)]:
            with self.subTest(attribute=attribute), patch.object(driver, 'VERIFIED_DEPENDENCIES', registry), \
                 patch.object(entry['module'], attribute, replacement):
                with self.assertRaises(ValueError):
                    driver.module('fixtures', Path(entry['path']))

    def load(self, path, expected=None):
        name = 'f_original_helper_' + str(len(driver.LOADED_SOURCE_HASHES))
        self.addCleanup(sys.modules.pop, name, None)
        value = driver.module(name, path, expected)
        self.addCleanup(driver.LOADED_SOURCE_HASHES.pop, value, None)
        return value

    def test_exact_original_bytes_execute_even_if_path_changes_after_read(self):
        path = self.root / 'helper.py'
        source = b'VALUE = 23\n'
        path.write_bytes(source)
        original_factory = driver.importlib.util.module_from_spec
        def changed_after_read(spec):
            path.write_bytes(b'VALUE = 42\n')
            return original_factory(spec)
        with patch.object(importlib.machinery.SourceFileLoader, 'exec_module',
                          side_effect=AssertionError('source loader or pyc executed')), \
             patch.object(driver.importlib.util, 'module_from_spec', side_effect=changed_after_read):
            value = self.load(path, producer.sha(source))
        self.assertEqual(value.VALUE, 23)
        self.assertEqual(value.__file__, str(path))
        self.assertEqual(value.__spec__.origin, str(path))
        self.assertIs(sys.modules[value.__name__], value)
        with self.assertRaisesRegex(ValueError, 'executed qualification dependency differs'):
            driver.loaded_digest(value)

    def test_original_hash_is_bound_to_module_instance_not_latest_path_import(self):
        path = self.root / 'helper.py'
        path.write_bytes(b'VALUE = 1\n')
        old = self.load(path)
        path.write_bytes(b'VALUE = 2\n')
        current = self.load(path)
        self.assertEqual(driver.loaded_digest(current), producer.sha(path.read_bytes()))
        with self.assertRaises(ValueError):
            driver.loaded_digest(old)

    def test_wrong_hash_refuses_before_execution_and_failure_restores_prior_module(self):
        path = self.root / 'helper.py'
        path.write_bytes(b'raise RuntimeError("original helper failure")\n')
        with self.assertRaisesRegex(ValueError, 'reviewed qualification dependency source differs'):
            self.load(path, '0' * 64)
        name = 'f_original_helper_prior'
        prior = types.ModuleType(name)
        with patch.dict(sys.modules, {name: prior}):
            with self.assertRaisesRegex(RuntimeError, 'original helper failure'):
                driver.module(name, path)
            self.assertIs(sys.modules[name], prior)

    def test_helper_source_is_bounded_regular_and_unsymlinked(self):
        path = self.root / 'helper.py'
        path.write_bytes(b'VALUE = 1\n')
        link = self.root / 'link.py'
        link.symlink_to(path)
        with self.assertRaises(ValueError):
            self.load(link)
        with patch.object(driver, 'MAX_HELPER_SOURCE', 3):
            with self.assertRaisesRegex(ValueError, 'bounded regular'):
                self.load(path)
        path.write_bytes(b'')
        with self.assertRaisesRegex(ValueError, 'bounded regular'):
            self.load(path)


class OriginalDeadlineTests(ClosedWorld):
    def test_original_preinput_sample_is_used_without_resampling_or_mutation(self):
        sample = {'mono_before_ns': 1000000000, 'wall_ns': 2000000000, 'mono_after_ns': 1000000007}
        with patch.object(producer.time, 'monotonic_ns', side_effect=AssertionError('renewed original sample')), \
             patch.object(producer.time, 'time_ns', side_effect=AssertionError('renewed original wall')):
            clock, deadline = producer.pin_allocation_clock(1800, 4000000000, original_sample=sample)
        self.assertEqual(deadline, 3000000000)
        self.assertEqual(clock, {**sample, 'expires_wall_ns': 4000000000, 'total_seconds': 1800})
        sample['mono_before_ns'] = 123
        self.assertEqual(clock['mono_before_ns'], 1000000000)

    def test_invalid_or_already_expired_original_sample_never_gets_replacement_clock(self):
        original = {'mono_before_ns': 10, 'wall_ns': 20, 'mono_after_ns': 12}
        cases = [None, {}, {**original, 'extra': 1}, {**original, 'wall_ns': True},
                 {**original, 'mono_before_ns': 0}, {**original, 'mono_after_ns': 9},
                 {**original, 'wall_ns': 20.0}]
        # None is the ordinary real-clock mode; test malformed provided values
        # and sample-time expiry without entering that mode.
        for sample in cases[1:]:
            with self.subTest(sample=sample), \
                 patch.object(producer.time, 'monotonic_ns', side_effect=AssertionError('clock fallback')), \
                 patch.object(producer.time, 'time_ns', side_effect=AssertionError('clock fallback')):
                with self.assertRaises(ValueError):
                    producer.pin_allocation_clock(1800, 40, original_sample=sample)
        with patch.object(producer.time, 'monotonic_ns', side_effect=AssertionError('clock fallback')), \
             patch.object(producer.time, 'time_ns', side_effect=AssertionError('clock fallback')):
            with self.assertRaisesRegex(ValueError, 'expired'):
                producer.pin_allocation_clock(1800, 22, original_sample=original)

    def test_original_clock_bracket_conservatively_pins_one_integer_deadline(self):
        with patch.object(producer.time, 'monotonic_ns', side_effect=[1000, 1200]), \
             patch.object(producer.time, 'time_ns', return_value=10000):
            clock, deadline = producer.pin_allocation_clock(300, 20000)
        self.assertEqual(clock, {'mono_before_ns': 1000, 'wall_ns': 10000, 'mono_after_ns': 1200,
                                'expires_wall_ns': 20000, 'total_seconds': 300})
        self.assertEqual(deadline, 11000)
        self.assertLessEqual(int(producer.deadline_seconds(deadline) * 10**9), deadline)
        with patch.object(producer.time, 'monotonic_ns', side_effect=[1000, 1200]), \
             patch.object(producer.time, 'time_ns', return_value=10000):
            with self.assertRaisesRegex(ValueError, 'expired during clock'):
                producer.pin_allocation_clock(300, 10100)

    def test_process_deadline_clamps_to_original_pin_after_preparation_time(self):
        value = driver.Driver.__new__(driver.Driver)
        value.manifest = {'expires_at': '2999-01-01T00:00:00Z'}
        value.cases = {'limits': {'total_seconds': 300, 'process_log_bytes': 1024}}
        value.started, value.allocation_deadline = 100, 110
        value.root, value.binary, value.environment = self.root, self.binary, {}
        value.remaining = Mock(return_value=10)
        value.process_journal = None
        with patch.object(driver.time, 'monotonic', return_value=101), patch.object(driver, 'OwnedProcess') as factory:
            value.create_process(['--version'], self.logs)
        self.assertEqual(factory.call_args.kwargs['initialization_deadline'], 110)

    def test_final_observation_is_charged_inside_original_capture_budget(self):
        value = driver.Driver.__new__(driver.Driver)
        value.root, value.started, value.requests, value.serial = self.root, time.monotonic(), 0, 0
        value.capture_bytes = 50
        value.capture_budget_lock = threading.RLock()
        value.cases = {'limits': {'http_capture_bytes': 4096}}
        value.report = {'cases': [], 'product_acceptance': False}
        value.begin_process_capture = value.execute = value.stop = value.finish_process_capture = Mock()
        self.assertEqual(value.run(), 0)
        self.assertEqual(value.capture_bytes, 50 + (self.root / 'observation.json').stat().st_size)

    def test_wall_expiry_and_pinned_monotonic_expiry_never_gain_new_allowance(self):
        value = driver.Driver.__new__(driver.Driver)
        value.manifest = {'expires_at': '2026-10-07T03:10:00Z'}
        value.cases = {'limits': {'total_seconds': 300}}
        value.started, value.allocation_deadline = 10, 110
        datetime_type = dt.datetime
        fake = Mock()
        fake.fromisoformat.side_effect = datetime_type.fromisoformat
        fake.now.return_value = datetime_type(2026, 10, 7, 3, 9, 59, tzinfo=dt.timezone.utc)
        with patch.object(driver.dt, 'datetime', fake), patch.object(driver.time, 'monotonic', return_value=20):
            self.assertEqual(value.remaining(), 1)
            self.assertEqual(value.cleanup_remaining(), 1)
            fake.now.return_value = datetime_type(2026, 10, 7, 3, 0, tzinfo=dt.timezone.utc)
            self.assertEqual(value.remaining(), 90)
            self.assertEqual(value.cleanup_remaining(), 90)
        with patch.object(driver.dt, 'datetime', fake), patch.object(driver.time, 'monotonic', return_value=111):
            with self.assertRaisesRegex(ValueError, 'deadline'):
                value.remaining()
            self.assertEqual(value.cleanup_remaining(), 0)


if __name__ == '__main__':
    unittest.main()
