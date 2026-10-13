"""Build command/cache provenance controls; all external operations are mocked."""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock

import container_artifact as container


class ContainerCacheTests(unittest.TestCase):
    def test_invalid_explicit_cache_refused_before_any_source_or_docker_operation(self):
        for value in ('', '../cache', 'recall-bookworm-rust194-' + 'a' * 63,
                      'recall-bookworm-rust194-' + 'A' * 64, 123):
            with self.subTest(value=value), mock.patch.object(container.artifact, 'source_identity') as source, \
                    mock.patch.object(container.subprocess, 'Popen') as docker:
                with self.assertRaisesRegex(ValueError, 'cache identity'):
                    container.build_image(Path('/unused'), 'fixture', Path('/unused'), cache_id=value)
                source.assert_not_called()
                docker.assert_not_called()

    def build(self, explicit):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / 'out'
            archive_bytes = b'fixture source tar bytes; never extracted'
            identity = {'source_commit': '1' * 40, 'source_tree': '2' * 40,
                        'source_tar_sha256': hashlib.sha256(archive_bytes).hexdigest(),
                        'cargo_lock_sha256': '3' * 64}
            image = 'sha256:' + '4' * 64
            inputs = {'builder': {'image': 'rust@sha256:' + '5' * 64},
                      'runtime': {'image': 'debian@sha256:' + '6' * 64}}
            budget = mock.Mock(receipt=root / 'budget.json')
            budget.check.return_value = {'checks': 1}

            def run(command, **kwargs):
                if command[:2] == ['git', '-C']:
                    kwargs['stdout'].write(archive_bytes)
                else:
                    self.assertEqual(command, ['docker', 'buildx', 'version'])

            def check_output(command, **kwargs):
                if command[:2] == ['docker', 'info']:
                    return str(root)
                self.assertEqual(command[:3], ['docker', 'image', 'inspect'])
                return image

            def popen(command, **kwargs):
                Path(command[command.index('--iidfile') + 1]).write_text(image)
                Path(command[command.index('--metadata-file') + 1]).write_text('{"fixture":true}')
                return mock.Mock(returncode=0, poll=mock.Mock(return_value=0))

            with mock.patch.object(container.artifact, 'source_identity', return_value=identity), \
                    mock.patch.object(container, 'load_inputs', return_value=inputs), \
                    mock.patch.object(container, 'build_limits', return_value=(8, 250 * 1024**3, 150 * 1024**3)), \
                    mock.patch.dict(os.environ, {'RECALL_ARTIFACT_BUDGET_FILE': str(root / 'budget.json')}), \
                    mock.patch.object(container, 'ResourceBudget', return_value=budget), \
                    mock.patch.object(container.subprocess, 'run', side_effect=run), \
                    mock.patch.object(container.subprocess, 'check_output', side_effect=check_output), \
                    mock.patch.object(container.subprocess, 'Popen', side_effect=popen), \
                    mock.patch.object(container, 'package_image', return_value={'fixture': True}) as package:
                kwargs = {} if explicit is None else {'cache_id': explicit}
                self.assertEqual(container.build_image(root, 'fixture', output, **kwargs), {'fixture': True})
                package.assert_called_once_with(root, image, identity['source_commit'], output)
            record = json.loads((output / 'container-build-metadata.json').read_text())
            expected = container.cache_identity(root, identity) if explicit is None else explicit
            self.assertEqual(record['cache_id'], expected)
            self.assertIn('RECALL_BUILD_CACHE_ID=' + expected, record['command'])
            self.assertIn('SOURCE_COMMIT=' + identity['source_commit'], record['command'])
            self.assertIn('SOURCE_TREE=' + identity['source_tree'], record['command'])
            self.assertIn('SOURCE_SHA256=' + identity['source_tar_sha256'], record['command'])
            self.assertEqual(record['input_lock'], inputs)

    def test_default_cache_preserves_current_source_graph_identity(self):
        self.build(None)

    def test_explicit_cache_keeps_current_source_and_records_actual_build_selection(self):
        self.build('recall-bookworm-rust194-' + 'a' * 64)


if __name__ == '__main__':
    unittest.main()
