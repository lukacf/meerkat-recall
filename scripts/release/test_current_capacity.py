"""Current host safeguards and compiler-cache compatibility; no heavy processes."""
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

import container_artifact as artifact


class CurrentCapacityTests(unittest.TestCase):
    def test_current_mode_ignores_obsolete_allocation_but_retains_jobs_and_floor(self):
        with patch.dict(os.environ, {'RECALL_BUILD_RESOURCE_MODE': 'current-capacity',
                                    'RECALL_ARTIFACT_MAX_GIB': 'obsolete', 'CARGO_BUILD_JOBS': '32'}, clear=True):
            self.assertEqual(artifact.build_limits(), (32, 250 * artifact.GIB, 0))
            for key, value in [('CARGO_BUILD_JOBS', '33'), ('RECALL_BUILD_MIN_FREE_GIB', '249')]:
                with patch.dict(os.environ, {key: value}), self.assertRaises(ValueError): artifact.build_limits()

    def test_actual_memory_root_and_target_floors_and_legacy_refusal(self):
        gib = artifact.GIB
        original = Path.read_text
        memory = [121 * 1024**2]
        free = {'root': 266 * gib, 'target': 251 * gib}
        def read(path, *args, **kwargs):
            return 'MemAvailable: ' + str(memory[0]) + ' kB\n' if str(path) == '/proc/meminfo' else original(path, *args, **kwargs)
        def usage(path):
            available = free['root' if Path(path) == Path('/') else 'target']
            return shutil._ntuple_diskusage(1000*gib, 1000*gib-available, available)
        with tempfile.TemporaryDirectory() as temp, patch.object(Path, 'read_text', read), patch.object(artifact.shutil, 'disk_usage', side_effect=usage):
            root = Path(temp)
            for kind in ('memory', 'root', 'target'):
                receipt = root / (kind + '.json'); monitor = artifact.CurrentCapacity(receipt, (root,))
                self.assertFalse(monitor.check()['refused'])
                if kind == 'memory': memory[0] = 119 * 1024**2
                else: free[kind] -= 2 * gib
                with self.assertRaises(RuntimeError): monitor.check()
                self.assertTrue(json.loads(receipt.read_text())['refused'])
                memory[0] = 121 * 1024**2; free.update(root=266*gib, target=251*gib)
                with self.assertRaises(ValueError): monitor.check()
            old = root / 'legacy.json'; old.write_text('{"format":"meerkat-recall-artifact-allocation/v1","consumed_bytes":999999}')
            before = old.read_bytes()
            with self.assertRaises(ValueError): artifact.CurrentCapacity(old, (root,)).check()
            self.assertEqual(before, old.read_bytes())

    def test_receipt_path_device_and_floor_changes_do_not_replace_record(self):
        gib = artifact.GIB
        original = Path.read_text
        def read(path, *args, **kwargs):
            return 'MemAvailable: 200000000 kB\n' if str(path) == '/proc/meminfo' else original(path, *args, **kwargs)
        with tempfile.TemporaryDirectory() as temp, patch.object(Path, 'read_text', read), patch.object(artifact.shutil, 'disk_usage', return_value=shutil._ntuple_diskusage(1000*gib, 500*gib, 500*gib)):
            root = Path(temp); receipt = root/'capacity.json'
            artifact.CurrentCapacity(receipt, (root,), 300*gib).check()
            before = receipt.read_bytes()
            with self.assertRaises(ValueError): artifact.CurrentCapacity(receipt, (root,), 250*gib).check()
            with self.assertRaises(ValueError): artifact.CurrentCapacity(receipt, (Path('/'),), 300*gib).check()
            self.assertEqual(before, receipt.read_bytes())
            value = json.loads(before); value['filesystems'][str(root)]['device'] += 1
            receipt.write_text(json.dumps(value)); altered = receipt.read_bytes()
            with self.assertRaises(ValueError): artifact.CurrentCapacity(receipt, (root,), 300*gib).check()
            self.assertEqual(altered, receipt.read_bytes())

    def test_sequential_builds_keep_independent_capacity_path_sets(self):
        gib = artifact.GIB
        original = Path.read_text
        def read(path, *args, **kwargs):
            return 'MemAvailable: 200000000 kB\n' if str(path) == '/proc/meminfo' else original(path, *args, **kwargs)
        with tempfile.TemporaryDirectory() as temp, patch.object(Path, 'read_text', read), patch.object(artifact.shutil, 'disk_usage', return_value=shutil._ntuple_diskusage(1000*gib, 500*gib, 500*gib)):
            root = Path(temp); recall = root/'recall'; paired = root/'paired'; recall.mkdir(); paired.mkdir()
            first = root/'recall.json'; second = root/'paired.json'
            artifact.CurrentCapacity(first, (recall,)).check(); before = first.read_bytes()
            state = artifact.CurrentCapacity(second, (paired,)).check()
            self.assertFalse(state['refused']); self.assertEqual(first.read_bytes(), before)
            self.assertIn(str(paired), state['filesystems']); self.assertNotIn(str(recall), state['filesystems'])

    def test_compatible_cache_ignores_checkout_source_but_not_native_inputs(self):
        with patch.dict(os.environ, {'RECALL_BUILD_RESOURCE_MODE': 'current-capacity'}), patch.object(artifact, 'cache_contract', return_value={'compiler': '1.94', 'native': 'a'}) as contract:
            first = artifact.cache_identity(Path('/first'), {'source_commit': 'a'*40})
            self.assertEqual(first, artifact.cache_identity(Path('/second'), {'source_commit': 'b'*40}))
            contract.return_value = {'compiler': '1.94', 'native': 'b'}
            self.assertNotEqual(first, artifact.cache_identity(Path('/first'), {'source_commit': 'a'*40}))

    def test_daemon_history_requires_owned_source_and_actual_terminal_state(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); monitor = artifact.CurrentCapacity(root/'capacity.json', (root,))
            monitor.build_environment(); ref = 'a'*24
            directory = monitor.buildx_directory/'refs/default/default'; directory.mkdir(parents=True)
            (directory/ref).write_text('{"LocalPath":"/owned/source"}')
            value = {'Ref': ref, 'BuildArgs': [{'Name': 'SOURCE_COMMIT', 'Value': 'b'*40}], 'Status': 'completed'}
            with patch.object(artifact.subprocess, 'check_output', return_value=json.dumps(value).encode()) as command:
                monitor.confirm_build_terminal('b'*40)
                self.assertTrue(json.loads((root/'capacity.buildkit-cleanup.json').read_text())['all_owned_history_terminal'])
                self.assertIn('history', command.call_args.args[0])
                with self.assertRaises(ValueError): monitor.confirm_build_terminal('c'*40)
            self.assertFalse(json.loads((root/'capacity.buildkit-cleanup.json').read_text())['all_owned_history_terminal'])


if __name__ == '__main__': unittest.main()
