#!/usr/bin/env python3
"""Prepare the release's pinned Linux CPU static ORT distribution, without Cargo.

Only the repository lock selects an origin. A completed cache is verified on
every use; a corrupt cache is refused, never silently repaired or substituted.
Darwin continues to use the locked ort-sys distribution.
"""
from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
import tempfile
import time
import urllib.request
import zipfile

TARGET = 'x86_64-unknown-linux-gnu'
LOCK = Path(__file__).with_name('linux-static-ort.lock.json')
SHA256 = re.compile(r'[a-f0-9]{64}')
MAX_FILE = 128 * 1024**2


def require(condition, message):
    if not condition:
        raise ValueError(message)


def regular_receipt(path, maximum=MAX_FILE):
    path = Path(path)
    require(path.absolute() == path.resolve(strict=True), f'aliased build input: {path}')
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1
            and 0 < before.st_size <= maximum, f'invalid regular build input: {path}')
    with path.open('rb') as stream:
        digest = hashlib.file_digest(stream, 'sha256').hexdigest()
    after = path.lstat()
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    require(identity(before) == identity(after), f'build input changed: {path}')
    return {'path': str(path), 'sha256': digest, 'bytes': before.st_size}


def load_lock(path=LOCK):
    receipt = regular_receipt(path, 64 * 1024)
    lock = json.loads(Path(path).read_text())
    require(lock.get('format') == 'meerkat-recall-linux-static-ort/v1'
            and (lock.get('target'), lock.get('version'), lock.get('api'),
                 lock.get('link_mode'), lock.get('provider')) ==
            (TARGET, '1.24.2', 24, 'static', 'CPU'), 'unsupported Linux ORT lock')
    archive = lock['archive']
    require(archive['url'].startswith('https://github.com/csukuangfj/onnxruntime-libs/releases/download/v1.24.2/')
            and re.fullmatch(r'[a-zA-Z0-9_.-]+', archive['root']), 'invalid pinned archive origin')
    for item in [archive, *lock['files'].values()]:
        require(SHA256.fullmatch(item['sha256']) and type(item['bytes']) is int
                and 0 < item['bytes'] <= MAX_FILE, 'invalid pinned file digest or size')
    require({'lib/libonnxruntime.a', 'include/onnxruntime_c_api.h'} <= lock['files'].keys()
            and len(lock['files']) <= 64, 'missing static library or API header')
    for name in lock['files']:
        safe_member(name)
    require(regular_receipt(path, 64 * 1024) == receipt, 'Linux ORT lock changed')
    for name, pin in lock.get('notices', {}).items():
        require(name in {'LICENSE', 'ThirdPartyNotices.txt'}, 'unexpected runtime notice')
        matches(Path(path).parent / 'onnxruntime-notices' / name, pin)
    return lock, receipt


def safe_member(name):
    pure = PurePosixPath(name)
    require(not pure.is_absolute() and pure.parts and '\\' not in name
            and all(part not in ('.', '..') for part in pure.parts)
            and str(pure) == name, 'unsafe archive member')
    return pure


def cache_path(home, lock):
    return Path(home).resolve(strict=True) / 'recall-static-ort' / TARGET / lock['archive']['sha256']


def matches(path, expected):
    value = regular_receipt(path)
    require(all(value[key] == expected[key] for key in ('bytes', 'sha256')),
            f'pinned input checksum or size differs: {path}')
    return value


def verify(home, lock_path=LOCK):
    lock, lock_record = load_lock(lock_path)
    cache = cache_path(home, lock)
    require(cache.is_dir() and cache == cache.resolve(strict=True), 'missing or aliased ORT cache')
    archive = matches(cache / 'upstream.zip', lock['archive'])
    distribution = cache / 'distribution'
    observed = set()
    for path in distribution.rglob('*'):
        require(not path.is_symlink(), 'symlink in ORT cache')
        if not path.is_dir():
            observed.add(str(path.relative_to(distribution)))
    require(observed == set(lock['files']), 'ORT cache member inventory differs')
    records = {name: matches(distribution / name, pin) for name, pin in lock['files'].items()}
    with (distribution / 'lib/libonnxruntime.a').open('rb') as stream:
        require(stream.read(8) == b'!<arch>\n', 'not a regular static archive')
    header = (distribution / 'include/onnxruntime_c_api.h').read_text()
    require(re.findall(r'^#define ORT_API_VERSION (\d+)\s*$', header, re.M) == ['24'],
            'pinned runtime does not declare API 24')
    return {'origin': 'repository-pinned-linux-static-distribution', 'lock': lock_record,
            'target': TARGET, 'version': lock['version'], 'api': 24, 'feature_set': 'CPU',
            'url': lock['archive']['url'], 'sha256': lock['archive']['sha256'],
            'archive': archive, 'members': records,
            'publisher': lock['publisher'], 'publisher_source_commit': lock['publisher_source_commit'],
            'publisher_build_url': lock['publisher_build_url'], 'build_platform': lock['build_platform'],
            'qualification': 'pinned build input; final ELF baseline and inference require separate checks'}


def download(archive, destination):
    """One bounded HTTPS GET. No retry, fallback URL, or unchecked cache input."""
    started = time.monotonic()
    request = urllib.request.Request(archive['url'], headers={'User-Agent': 'meerkat-recall-release'})
    digest = hashlib.sha256()
    count = 0
    with urllib.request.urlopen(request, timeout=30) as response, destination.open('xb') as output:
        require(response.url.startswith('https://'), 'archive redirect must retain HTTPS')
        while chunk := response.read(min(1024**2, archive['bytes'] + 1 - count)):
            count += len(chunk)
            require(count <= archive['bytes'] and time.monotonic() - started < 120,
                    'archive download exceeded size or time limit')
            output.write(chunk)
            digest.update(chunk)
    require(count == archive['bytes'] and digest.hexdigest() == archive['sha256'],
            'download checksum or size differs')


def unpack(archive_path, destination, lock):
    """Validate the complete ZIP inventory before writing only regular files."""
    expected = {lock['archive']['root'] + '/' + name: pin for name, pin in lock['files'].items()}
    allowed_dirs = {str(parent) + '/' for name in expected for parent in PurePosixPath(name).parents
                    if str(parent) != '.'}
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()
        names = [member.filename for member in members]
        require(len(members) <= 128 and len(names) == len(set(names)), 'duplicate or excessive ZIP members')
        require(set(names) - allowed_dirs == set(expected), 'ZIP member inventory differs')
        for member in members:
            safe_member(member.filename.rstrip('/'))
            mode = member.external_attr >> 16
            if member.is_dir():
                require(stat.S_ISDIR(mode) and member.file_size == 0, 'invalid ZIP directory')
                continue
            require(stat.S_ISREG(mode) and not member.flag_bits & 1
                    and member.file_size == expected[member.filename]['bytes'], 'invalid ZIP regular member')
        destination.mkdir()
        for member in members:
            if member.is_dir():
                continue
            relative = PurePosixPath(member.filename).relative_to(lock['archive']['root'])
            output = destination / str(relative)
            output.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, output.open('xb') as stream:
                remaining = member.file_size
                while remaining:
                    chunk = source.read(min(1024**2, remaining))
                    require(bool(chunk), 'truncated ZIP member')
                    stream.write(chunk)
                    remaining -= len(chunk)
                require(not source.read(1), 'oversized ZIP member')
            matches(output, expected[member.filename])


def prepare(home, lock_path=LOCK):
    lock, _ = load_lock(lock_path)
    cache = cache_path(home, lock)
    cache.parent.mkdir(parents=True, exist_ok=True)
    require(cache.parent == cache.parent.resolve(strict=True), 'aliased ORT cache parent')
    descriptor = os.open(cache.parent / '.prepare.lock', os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(descriptor, 'r+') as guard:
        require(stat.S_ISREG(os.fstat(guard.fileno()).st_mode)
                and os.fstat(guard.fileno()).st_nlink == 1, 'aliased ORT cache lock')
        fcntl.flock(guard, fcntl.LOCK_EX)
        if not cache.exists() and not cache.is_symlink():
            with tempfile.TemporaryDirectory(prefix='.prepare-', dir=cache.parent) as directory:
                staging = Path(directory) / 'completed'
                staging.mkdir()
                download(lock['archive'], staging / 'upstream.zip')
                unpack(staging / 'upstream.zip', staging / 'distribution', lock)
                staging.rename(cache)
        result = verify(home, lock_path)
    return Path(result['members']['lib/libonnxruntime.a']['path']).parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cargo-env', type=Path, required=True)
    args = parser.parse_args()
    homes = [line.removeprefix('CARGO_HOME=') for line in args.cargo_env.read_text().splitlines()
             if line.startswith('CARGO_HOME=')]
    require(len(homes) == 1 and Path(homes[0]).is_absolute(), 'missing unique absolute Cargo home')
    print(prepare(Path(homes[0])))


if __name__ == '__main__':
    main()
