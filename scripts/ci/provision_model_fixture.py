#!/usr/bin/env python3
"""Fetch only the checked-in CI fixture pins, verify bytes, then provision locally.

This is test setup, not the production provisioner. No credentials, provider
approval changes, inference or unpinned fallback downloads occur here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tempfile
import urllib.request

FILES = {'onnx/model.onnx', 'tokenizer.json', 'config.json',
         'special_tokens_map.json', 'tokenizer_config.json'}


def validate(spec: dict) -> None:
    if (spec['format'] != 'recall-ci-model-fixture/v1'
            or spec['repository'] != 'BAAI/bge-small-en-v1.5'
            or not re.fullmatch(r'[0-9a-f]{40}', spec['revision'])
            or spec['license'] != 'MIT'
            or spec['model'] != 'bge-small-en-v1.5'
            or spec['preprocessing'] != 'text-v1'):
        raise ValueError('unexpected CI model source or identity')
    if {f['path'] for f in spec['files']} != FILES or len(spec['files']) != len(FILES):
        raise ValueError('fixture file inventory must be complete and exact')
    for item in spec['files']:
        if (not isinstance(item['bytes'], int) or not 0 < item['bytes'] <= 160 * 1024**2
                or not re.fullmatch(r'[0-9a-f]{64}', item['sha256'])):
            raise ValueError('unbounded or unpinned fixture asset')


def receive(stream, destination: Path, item: dict) -> None:
    digest = hashlib.sha256()
    size = 0
    with destination.open('xb') as out:
        while chunk := stream.read(1024**2):
            size += len(chunk)
            if size > item['bytes']:
                raise ValueError('fixture asset exceeds pinned size')
            digest.update(chunk)
            out.write(chunk)
    if size != item['bytes'] or digest.hexdigest() != item['sha256']:
        raise ValueError('fixture asset checksum/size mismatch')


def provision(root: Path, spec: dict) -> dict:
    validate(spec)
    root.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory(prefix='selected-', dir=root) as temp:
        selected = Path(temp)
        for item in spec['files']:
            destination = selected / PurePosixPath(item['path'])
            destination.parent.mkdir(exist_ok=True, parents=True)
            url = f"https://huggingface.co/{spec['repository']}/resolve/{spec['revision']}/{item['path']}"
            with urllib.request.urlopen(url, timeout=60) as response:
                if not response.url.startswith('https://'):
                    raise ValueError('model asset redirect must remain HTTPS')
                receive(response, destination, item)
        repo = Path(__file__).resolve().parents[2]
        result = subprocess.check_output([
            sys.executable, str(repo / 'scripts/release/provision-fastembed'),
            '--root', str(root / 'installed'), '--model', spec['model'],
            '--preprocessing', spec['preprocessing'],
            '--onnx', str(selected / 'onnx/model.onnx'), '--tokenizer-dir', str(selected),
        ], text=True)
    return json.loads(result)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--github-env', type=Path, required=True)
    args = parser.parse_args()
    spec = json.loads(Path(__file__).with_name('model_fixture.json').read_text())
    result = provision(args.root.resolve(), spec)
    version = result['model_version'].removeprefix('sha256:')
    if not re.fullmatch(r'[0-9a-f]{64}', version) or not result['endpoint'].startswith('file://'):
        raise ValueError('invalid provisioned identity')
    with args.github_env.open('a') as env:
        env.write(f"RECALL_TEST_FASTEMBED_ENDPOINT={result['endpoint']}\n")
        env.write(f'RECALL_TEST_FASTEMBED_MANIFEST_SHA256={version}\n')
    print(json.dumps({'source': spec['source'], 'model_version': result['model_version'],
                      'qualification': 'Verified fixture bytes only; inference test runs next'}))


if __name__ == '__main__':
    main()
