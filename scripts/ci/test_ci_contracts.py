"""Cheap CI contracts: fail-closed gates, exact ignored-test lanes and fixture pins."""
import hashlib
import io
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib
import unittest
from unittest import mock

import cargo_cache_paths
import provision_model_fixture as model

ROOT = Path(__file__).resolve().parents[2]


def ignored_inventory():
    found = set()
    for directory in ['crates', 'tests', 'bin']:
        for path in (ROOT / directory).rglob('*.rs'):
            text = path.read_text()
            for ignored in re.finditer(r'^\s*#\[ignore(?:\s*=\s*"([^"\n]*)")?\]', text, re.M):
                function = re.search(r'\b(?:async\s+)?fn\s+(\w+)\s*\(', text[ignored.end():])
                if function is None:
                    raise AssertionError(f'ignored attribute without a function: {path}')
                found.add((str(path.relative_to(ROOT)), function[1]))
    return found


def job_blocks():
    text = (ROOT / '.github/workflows/ci.yml').read_text()
    pairs = re.split(r'^  ([a-z][a-z0-9-]*):\n', text.split('jobs:\n', 1)[1], flags=re.M)[1:]
    return dict(zip(pairs[::2], pairs[1::2]))


class WorkflowContracts(unittest.TestCase):
    def test_required_contexts_and_failed_classifier_fail_closed(self):
        jobs = job_blocks()
        expected = {'Container configuration', 'Secret scan (full Git history)',
                    'Format, lint, and static checks', 'Workspace unit gate',
                    'Full test suite (all features)', 'Security audit', 'Documentation', 'WASM check'}
        names = {re.search(r'^    name: (.+)$', b, re.M)[1] for b in jobs.values()}
        self.assertTrue(expected <= names)
        for name in ['fmt-lint', 'test-unit', 'test-all', 'docs', 'wasm-check', 'e2e']:
            block = jobs[name]
            self.assertIn('needs: changes', block)
            self.assertIn('always()', block)
            guard = re.search(r'      - name: Require successful classification\n.*?        run: \|\n((?:          [^\n]+\n)+)', block, re.S)[1]
            shell = '\n'.join(line[10:] for line in guard.splitlines())
            for result, output, succeeds in [('success','true',True),('success','false',True),('success','',False),('failure','true',False),('skipped','true',False),('cancelled','false',False)]:
                with self.subTest(job=name,result=result,output=output):
                    process = subprocess.run(['bash','-e','-c',shell],env=dict(os.environ,CLASSIFICATION_RESULT=result,DOCS_ONLY=output),capture_output=True)
                    self.assertEqual(process.returncode == 0,succeeds)
        for name in ['secrets','audit','container-config']:
            self.assertNotIn('docs_only',jobs[name])
        self.assertRegex(jobs['docs'], r'name: Validate reader documentation\n        run:')
        self.assertNotIn('paths-ignore:', (ROOT/'.github/workflows/ci.yml').read_text())

    def test_rust_steps_gate_and_cache_uses_actual_wrapper_paths(self):
        for name, block in job_blocks().items():
            if name not in {'fmt-lint','test-unit','test-all','docs','wasm-check','e2e'}:
                continue
            for step in block.split('      - ')[1:]:
                if ('uses: dtolnay/' in step or 'uses: ./.github/actions/cargo-cache' in step
                        or re.search(r'run: (make (?:test|lint|doc\b|fmt-check|verify)|rustup |./scripts/repo-cargo)',step)):
                    self.assertIn("if: needs.changes.outputs.docs_only != 'true'",step)
        action=(ROOT/'.github/actions/cargo-cache/action.yml').read_text()
        self.assertIn('workspaces: . -> ${{ steps.paths.outputs.target }}',action)
        self.assertIn('prefix-key: wrapper-v2-${{ inputs.lane }}',action)
        paths={'CARGO_HOME':'/tmp/ci-cache/cargo-home','CARGO_TARGET_DIR':'/tmp/ci-cache/targets/job'}
        with mock.patch.object(cargo_cache_paths.subprocess,'check_output',return_value='\n'.join(f'{k}={v}' for k,v in paths.items())) as run:
            self.assertEqual(cargo_cache_paths.resolve(ROOT),paths)
            self.assertEqual(run.call_args.args[0], [str(ROOT/'scripts/repo-cargo'),'--print-env'])
        for invalid in ['CARGO_HOME=/tmp/home\n','CARGO_HOME=relative\nCARGO_TARGET_DIR=/tmp/target\n']:
            with mock.patch.object(cargo_cache_paths.subprocess,'check_output',return_value=invalid),self.assertRaises(ValueError):
                cargo_cache_paths.resolve(ROOT)

    def test_every_ignored_test_has_an_explicit_lane_and_selection(self):
        inventory=json.loads((ROOT/'scripts/ci/ignored_test_lanes.json').read_text())['tests']
        entries={(entry['path'],entry['test']) for entry in inventory}
        self.assertEqual(len(entries),len(inventory))
        self.assertEqual(entries,ignored_inventory(), 'New ignored tests need an explicit reviewed resource lane')
        aliases=tomllib.loads((ROOT/'.cargo/config.toml').read_text())['alias']
        deterministic=aliases['test-e2e']
        self.assertIn('test --workspace --all-features -- --ignored',deterministic)
        skipped=re.findall(r'--skip (\S+)',deterministic)
        for entry in inventory:
            # The local-resource module supplies the fully-qualified test prefix.
            qualified=('provider_dispatch::local_resource_tests::' if entry['lane']=='provisioned-model' else '')+entry['test']
            excluded=any(pattern in qualified for pattern in skipped)
            with self.subTest(test=qualified):
                if entry['lane'] in {'deterministic-e2e','deterministic-scale'}:
                    self.assertFalse(excluded)
                elif entry['lane']=='provisioned-model':
                    self.assertTrue(excluded)
                    self.assertIn(qualified,aliases['test-e2e-model'])
                    self.assertIn('--ignored --exact',aliases['test-e2e-model'])
                    self.assertIn('--features elephant-embeddings/fastembed',aliases['test-e2e-model'])
                else:
                    self.assertEqual(entry['lane'],'live-provider-opt-in')
                    self.assertTrue(excluded)
        e2e=job_blocks()['e2e']
        self.assertIn('make test-e2e-model',e2e)
        self.assertIn('provision_model_fixture.py',e2e)
        self.assertLess(e2e.index('provision_model_fixture.py'),e2e.index('make test-e2e-model'))


class ModelFixtureTests(unittest.TestCase):
    def test_exact_source_inventory(self):
        spec=json.loads((ROOT/'scripts/ci/model_fixture.json').read_text())
        model.validate(spec)
        for mutate in [lambda s:s.update(revision='main'),lambda s:s['files'].pop(),lambda s:s['files'].append(s['files'][0]),lambda s:s['files'][0].update(path='../escape'),lambda s:s['files'][0].update(bytes=0),lambda s:s.update(repository='other/model')]:
            altered=json.loads(json.dumps(spec));mutate(altered)
            with self.assertRaises(ValueError):model.validate(altered)

    def test_bytes_are_bounded_and_verified_before_provisioning(self):
        payload=b'fixture bytes';item={'bytes':len(payload),'sha256':hashlib.sha256(payload).hexdigest()}
        for value,valid in [(payload,True),(b'wrong content',False),(payload[:-1],False),(payload+b'excess',False)]:
            with self.subTest(value=value),tempfile.TemporaryDirectory() as temporary:
                output=Path(temporary)/'asset'
                if valid:
                    model.receive(io.BytesIO(value),output,item);self.assertEqual(output.read_bytes(),payload)
                else:
                    with self.assertRaises(ValueError):model.receive(io.BytesIO(value),output,item)
        with tempfile.TemporaryDirectory() as temporary:
            spec=json.loads((ROOT/'scripts/ci/model_fixture.json').read_text())
            with self.assertRaises(FileExistsError):model.provision(Path(temporary),spec)


if __name__=='__main__':unittest.main()
