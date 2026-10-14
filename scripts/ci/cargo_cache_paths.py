#!/usr/bin/env python3
"""Expose the repo-cargo paths without invoking Cargo or changing cache identity."""
import os
from pathlib import Path
import subprocess


def resolve(root: Path) -> dict[str, str]:
    output = subprocess.check_output([str(root / 'scripts/repo-cargo'), '--print-env'], text=True)
    paths = dict(line.split('=', 1) for line in output.splitlines()
                 if line.startswith(('CARGO_HOME=', 'CARGO_TARGET_DIR=')))
    if set(paths) != {'CARGO_HOME', 'CARGO_TARGET_DIR'}:
        raise ValueError('repository wrapper did not provide both cache paths')
    for value in paths.values():
        if not Path(value).is_absolute() or any(c in value for c in '\r\n'):
            raise ValueError('invalid repository cache path')
    return paths


def main() -> None:
    root = Path(os.environ['GITHUB_WORKSPACE']).resolve()
    paths = resolve(root)
    with open(os.environ['GITHUB_ENV'], 'a', encoding='utf-8') as env:
        for key, value in paths.items():
            env.write(f'{key}={value}\n')
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as output:
        output.write('target=' + os.path.relpath(paths['CARGO_TARGET_DIR'], root) + '\n')


if __name__ == '__main__':
    main()
