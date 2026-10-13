#!/usr/bin/env python3
"""Retain static ONNX Runtime inputs from the completed native Cargo build.

Run inside the original build/cache mount, before copying the executable. No
Cargo command, native library import, download, or executable is run by this helper.
Cargo's dependency fingerprints identify the selected instances, including when
unrelated old instances remain in the same cache. This is build provenance, not
an inference or artifact acceptance result.
"""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import stat
import tarfile
import tomllib

TARGETS = {'x86_64-unknown-linux-gnu', 'aarch64-apple-darwin'}
MAX_TEXT = 2 * 1024**2
MAX_ARCHIVE = 512 * 1024**2
SHA256 = re.compile(r'[a-f0-9]{64}')
FINGERPRINT = re.compile(r'[a-f0-9]{16}')


def require(condition, message):
    if not condition:
        raise ValueError(message)


def file_receipt(path: Path, *, maximum=MAX_TEXT, text=False):
    """Bind stable regular bytes; large archives are streamed, never embedded."""
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and 0 < info.st_size <= maximum,
            f'not a bounded regular build input: {path}')
    digest = hashlib.sha256()
    pieces = []
    with path.open('rb') as stream:
        while chunk := stream.read(1024**2):
            digest.update(chunk)
            if text:
                pieces.append(chunk)
    after = path.lstat()
    identity = lambda s: (s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns, s.st_ctime_ns)
    require(identity(info) == identity(after), f'build input changed: {path}')
    result = {'path': str(path), 'sha256': digest.hexdigest(), 'bytes': info.st_size}
    if text:
        result['text'] = b''.join(pieces).decode('utf-8')
    return result


def unique(values, description):
    values = list(values)
    require(len(values) == 1, f'expected one {description}, found {len(values)}')
    return values[0]


def fingerprint_record(path):
    record = file_receipt(path, text=True)
    value = json.loads(record['text'])
    signature = file_receipt(path.with_suffix(''), text=True)
    raw = signature['text']
    require(FINGERPRINT.fullmatch(raw), f'unsupported Cargo fingerprint encoding: {path}')
    # Cargo stores the u64 fingerprint as its eight little-endian bytes in hex.
    return value, {**record, 'fingerprint': signature}, int.from_bytes(bytes.fromhex(raw), 'little')


def dependency(fingerprints, parent, dependency_name, crate, filename):
    edge = unique((row for row in parent['deps'] if row[1] == dependency_name),
                  f'{dependency_name} dependency')
    require(len(edge) == 4 and type(edge[3]) is int, 'unsupported Cargo dependency encoding')
    matches = []
    for path in sorted(fingerprints.glob(f'{crate}-*/{filename}.json')):
        value, record, number = fingerprint_record(path)
        if number == edge[3]:
            matches.append((value, record))
    return unique(matches, f'selected {crate} fingerprint')


def features(value, crate):
    selected = json.loads(value['features'])
    require(isinstance(selected, list) and all(isinstance(item, str) for item in selected)
            and len(selected) == len(set(selected)), f'invalid {crate} features')
    require({'api-24', 'download-binaries'} <= set(selected), f'{crate} requires API 24 and the locked download-capable feature graph')
    require(not {'load-dynamic', 'disable-linking', 'alternative-backend', 'pkg-config'} & set(selected),
            f'{crate} is not the release static runtime')
    return selected


def collect(root: Path, cargo_env: Path, target: str):
    require(target in TARGETS, 'unsupported release target')
    root = root.resolve(strict=True)
    environment = {}
    for line in cargo_env.read_text().splitlines():
        key, separator, value = line.partition('=')
        if separator and key in {'CARGO_HOME', 'CARGO_TARGET_DIR'}:
            require(key not in environment and Path(value).is_absolute(), 'invalid Cargo environment receipt')
            environment[key] = Path(value).resolve(strict=True)
    require(set(environment) == {'CARGO_HOME', 'CARGO_TARGET_DIR'}, 'missing Cargo environment receipt')
    deploy = environment['CARGO_TARGET_DIR'] / 'deploy'
    binary = deploy / 'meerkat-recall'
    require(binary.is_file() and not binary.is_symlink(), 'completed native deploy executable required')
    fingerprints = deploy / '.fingerprint'
    # Cargo may hardlink, copy, or clone the selected deps executable.
    # Match stable bytes and refuse multiple matching units; timestamps and
    # inode identity do not identify the selected dependency fingerprint.
    binary_record = file_receipt(binary, maximum=1024**3)
    matching_binaries = []
    for path in sorted((deploy / 'deps').glob('meerkat_recall-*')):
        unit = path.name.removeprefix('meerkat_recall-')
        if (not FINGERPRINT.fullmatch(unit) or not path.is_file() or path.is_symlink()
                or path.stat().st_size != binary_record['bytes']):
            continue
        candidate = file_receipt(path, maximum=1024**3)
        if candidate['sha256'] == binary_record['sha256']:
            matching_binaries.append(path)
    selected_binary = unique(matching_binaries, 'selected deploy executable')
    unit = selected_binary.name.removeprefix('meerkat_recall-')
    require(FINGERPRINT.fullmatch(unit), 'unsupported Cargo executable name')
    current, record, _ = fingerprint_record(fingerprints / f'meerkat-recall-bin-{unit}' / 'bin-meerkat-recall.json')
    require({'default', 'rocksdb', 'fastembed'} <= set(json.loads(current['features'])),
            'selected executable lacks release features')
    selected_binary_fingerprint = current
    compiler_runtime = None
    chain = [{'crate': 'meerkat-recall-bin', **record}]
    selected = {}
    for name, crate, filename in (
        ('elephant_embeddings', 'elephant-embeddings', 'lib-elephant_embeddings'),
        ('fastembed', 'fastembed', 'lib-fastembed'),
        ('ort', 'ort', 'lib-ort'),
        ('ort_sys', 'ort-sys', 'lib-ort_sys'),
        ('build_script_main', 'ort-sys', 'run-build-script-build-script-main'),
    ):
        current, record = dependency(fingerprints, current, name, crate, filename)
        chain.append({'crate': crate, **record})
        if filename in {'lib-ort', 'lib-ort_sys'}:
            selected[crate] = features(current, crate)
    output_path = deploy / 'build' / Path(record['path']).parent.name / 'output'
    output = file_receipt(output_path, text=True)
    lines = output['text'].splitlines()
    directive = 'cargo:rustc-link-lib=static=onnxruntime'
    require(lines.count(directive) == 1, 'selected build did not statically link ONNX Runtime')
    require(not any('rustc-link-lib=' in line and 'onnxruntime' in line and line != directive for line in lines),
            'ambiguous ONNX Runtime link mode')
    libraries = []
    for line in lines:
        prefix = 'cargo:rustc-link-search=native='
        if line.startswith(prefix):
            path = Path(line.removeprefix(prefix))
            require(path.is_absolute(), 'relative native library search directory')
            archive = path / 'libonnxruntime.a'
            if archive.is_file():
                libraries.append(archive)
    library = unique(libraries, 'selected ONNX Runtime static archive')
    distribution_hash = library.parent.name
    if target != 'x86_64-unknown-linux-gnu':
        require(SHA256.fullmatch(distribution_hash) and library.parent.parent.name == target,
                'static library is not the target checksum-addressed distribution')
    static_library = file_receipt(library, maximum=MAX_ARCHIVE)
    with library.open('rb') as stream:
        require(stream.read(8) == b'!<arch>\n', 'ONNX Runtime input is not a regular static archive')
    lock = file_receipt(root / 'Cargo.lock', text=True)
    packages = tomllib.loads(lock['text'])['package']
    locked = {}
    for name in ('fastembed', 'ort', 'ort-sys'):
        package = unique((item for item in packages if item['name'] == name), f'locked {name}')
        require(package.get('source') == 'registry+https://github.com/rust-lang/crates.io-index'
                and SHA256.fullmatch(package.get('checksum', '')), f'{name} is not a locked registry package')
        locked[name] = {key: package[key] for key in ('version', 'source', 'checksum')}
    require(locked['ort']['version'] == locked['ort-sys']['version'] == '2.0.0-rc.12',
            'release runtime receipt requires the reviewed ort version')
    registry = environment['CARGO_HOME'] / 'registry' / 'src'
    source = unique(registry.glob(f"*/ort-sys-{locked['ort-sys']['version']}"), 'locked ort-sys source directory')
    package_path = (environment['CARGO_HOME'] / 'registry/cache' / source.parent.name
                    / (source.name + '.crate'))
    package_receipt = file_receipt(package_path)
    require(package_receipt['sha256'] == locked['ort-sys']['checksum'],
            'ort-sys registry archive checksum differs from lock')
    table = file_receipt(source / 'build/download/dist.txt', text=True)
    with tarfile.open(package_path, 'r:gz') as archive:
        member = unique((member for member in archive.getmembers()
                         if member.name == source.name + '/build/download/dist.txt'),
                        'registry distribution table member')
        require(member.isfile() and 0 < member.size <= MAX_TEXT, 'invalid registry distribution table')
        with archive.extractfile(member) as stream:
            original_table = stream.read(MAX_TEXT + 1)
    require(hashlib.sha256(original_table).hexdigest() == table['sha256'],
            'distribution table differs from the locked registry archive')
    if target == 'x86_64-unknown-linux-gnu':
        # Linux's upstream ort-sys archive requires newer glibc than Bookworm.
        # Only our explicit pinned CPU distribution may replace that archive.
        # Keep registry provenance, but do not claim its dist.txt selected Linux.
        spec = importlib.util.spec_from_file_location(
            'recall_linux_static_ort', Path(__file__).with_name('linux_static_ort.py'))
        linux = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(linux)
        distribution = linux.verify(environment['CARGO_HOME'],
                                    root / 'scripts/release/linux-static-ort.lock.json')
        require(str(library) == distribution['members']['lib/libonnxruntime.a']['path']
                and static_library == distribution['members']['lib/libonnxruntime.a'],
                'selected Linux library differs from the pinned distribution')
        _, gcc_fingerprint = dependency(
            fingerprints, selected_binary_fingerprint, 'build_script_build',
            'meerkat-recall-bin', 'run-build-script-build-script-build')
        gcc_output = file_receipt(deploy / 'build' / Path(gcc_fingerprint['path']).parent.name / 'output', text=True)
        gcc_lines = gcc_output['text'].splitlines()
        prefix = 'cargo:rustc-link-arg='
        gcc_arg = unique((line.removeprefix(prefix) for line in gcc_lines if line.startswith(prefix)),
                         'selected Linux compiler runtime link argument')
        gcc_path = Path(gcc_arg)
        require(gcc_path.is_absolute() and gcc_path.name == 'libgcc.a'
                and gcc_path == gcc_path.resolve(strict=True), 'invalid compiler runtime archive path')
        gcc_archive = file_receipt(gcc_path, maximum=MAX_ARCHIVE)
        with gcc_path.open('rb') as stream:
            require(stream.read(8) == b'!<arch>\n', 'compiler runtime is not a static archive')
        require('cargo:rerun-if-changed=' + gcc_arg in gcc_lines,
                'compiler runtime archive must invalidate changed build inputs')
        compiler_runtime = {'selected_build_fingerprint': gcc_fingerprint, 'link_output': gcc_output,
                            'static_library': gcc_archive, 'link_argument': gcc_arg}
        distribution['ort_sys_registry_archive'] = package_receipt
        distribution['ort_sys_distribution_table_not_selected'] = table
    else:
        # ort-sys verifies this compressed download; its digest is not the .a digest.
        rows = [line.split('\t') for line in table['text'].splitlines()]
        row = unique((row for row in rows if len(row) == 4 and row[1] == target
                      and row[3] == distribution_hash), 'selected upstream distribution')
        require(row[2].startswith('https://'), 'distribution URL must use HTTPS')
        distribution = {'feature_set': row[0], 'target': row[1], 'url': row[2], 'sha256': row[3],
                        'record': '\t'.join(row), 'table': table, 'registry_archive': package_receipt}
    require(file_receipt(binary, maximum=1024**3) == binary_record,
            'deploy executable changed during provenance collection')
    return {
        'format': 'meerkat-recall-static-ort-build/v1', 'target': target,
        'build_profile': 'deploy', 'link_mode': 'static', 'api': 24, 'load_dynamic': False,
        'binary_sha256': binary_record['sha256'],
        'cargo_lock_sha256': lock['sha256'], 'locked_packages': locked,
        'features': selected, 'dependency_fingerprints': chain,
        'link_output': output, 'static_link_directive': directive,
        'distribution': distribution,
        **({'compiler_runtime': compiler_runtime} if compiler_runtime else {}),
        'static_library': static_library,
        'qualification': 'actual completed build inputs; inference and acceptance remain separate',
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path.cwd())
    parser.add_argument('--cargo-env', type=Path, required=True)
    parser.add_argument('--target', choices=sorted(TARGETS), required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    receipt = collect(args.root, args.cargo_env, args.target)
    with args.output.open('x') as stream:
        json.dump(receipt, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write('\n')


if __name__ == '__main__':
    main()
