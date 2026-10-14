#!/usr/bin/env python3
"""Export/promote already qualified images. No Cargo build or service launch.

Export is explicit and scheduled; verify is offline. Promotion is reserved for
the canonical GitHub Actions workflow with packages:write, not user credentials.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile

REPOSITORY = 'https://github.com/lukacf/meerkat-recall'
ROLES = {'recall': 'meerkat-recall', 'paired': 'recall-surreal'}
SHA = re.compile(r'[a-f0-9]{64}')
COMMIT = re.compile(r'[a-f0-9]{40}')
VERSION = re.compile(r'0\.9\.[1-9][0-9]*')
# Each GitHub release asset must be smaller than 2 GiB.
MAX_ARCHIVE_BYTES = 2 * 1024**3 - 1


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def read_json(path, limit=1024**2):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('bounded regular JSON file required')
    return json.loads(path.read_bytes())


def validate_manifest(value):
    if (value.get('format') != 'meerkat-recall-container-delivery/v1'
            or not VERSION.fullmatch(value.get('version', ''))
            or not COMMIT.fullmatch(value.get('source_commit', ''))
            or value.get('repository') != REPOSITORY
            or set(value.get('images', {})) != set(ROLES)):
        raise ValueError('exact canonical release version/source and both images required')
    for role, name in ROLES.items():
        item = value['images'][role]
        if (item.get('repository') != 'ghcr.io/lukacf/' + name
                or item.get('archive') != name + '.docker.tar'
                or not SHA.fullmatch(item.get('archive_sha256', ''))
                or not re.fullmatch('sha256:[a-f0-9]{64}', item.get('image_id', ''))
                or not SHA.fullmatch(item.get('binary_sha256', ''))
                or not 0 < item.get('archive_bytes', 0) <= MAX_ARCHIVE_BYTES):
            raise ValueError('invalid image identity or archive')
    return value


def validate_image(image, source, role):
    labels = image.get('Config', {}).get('Labels', {}) or {}
    if ((image.get('Os'), image.get('Architecture')) != ('linux', 'amd64')
            or labels.get('org.opencontainers.image.source') != REPOSITORY
            or labels.get('org.opencontainers.image.revision') != source):
        raise ValueError('image platform or canonical source labels differ')
    expected = 'BUSL-1.1' if role == 'paired' else '(MIT OR Apache-2.0) AND BUSL-1.1'
    if labels.get('org.opencontainers.image.licenses') != expected:
        raise ValueError('upstream BSL license distinction missing')
    if role == 'paired' and (labels.get('org.meerkat.recall.reference-token-version') != '1'
            or labels.get('org.meerkat.recall.paired-upstream') != '8afd2ba0b6c0e232f19f42070e8811b323324c4c'):
        raise ValueError('paired upstream or reference capability identity differs')


def verify_archive(path, item, source, role):
    path = Path(path)
    if (path.is_symlink() or not path.is_file() or path.stat().st_size != item['archive_bytes']
            or digest(path) != item['archive_sha256']):
        raise ValueError('exported image archive identity differs')
    # Read metadata only; never extract arbitrary tar members or execute images.
    with tarfile.open(path, 'r:') as archive:
        names = set()
        for index, member in enumerate(archive):
            pure = Path(member.name)
            if (index > 10000 or pure.is_absolute() or '..' in pure.parts
                    or member.name in names or not (member.isfile() or member.isdir())):
                raise ValueError('unsafe or duplicate image archive member')
            names.add(member.name)
        def small(name):
            member = archive.getmember(name)
            if not member.isfile() or member.size > 1024**2:
                raise ValueError('oversize image metadata')
            return archive.extractfile(member).read()
        manifests = json.loads(small('manifest.json'))
        if len(manifests) != 1:
            raise ValueError('one exact exported image required')
        config = small(manifests[0]['Config'])
        if 'sha256:' + hashlib.sha256(config).hexdigest() != item['image_id']:
            raise ValueError('Docker config digest does not match qualified local image ID')
        cfg = json.loads(config)
        validate_image({'Os': cfg['os'], 'Architecture': cfg['architecture'], 'Config': cfg['config']}, source, role)


def verify(directory, expected_sha):
    directory = Path(directory)
    path = directory / 'container-delivery.json'
    if not SHA.fullmatch(expected_sha) or digest(path) != expected_sha:
        raise ValueError('explicit reviewed delivery manifest digest required')
    manifest = validate_manifest(read_json(path))
    for role, item in manifest['images'].items():
        verify_archive(directory / item['archive'], item, manifest['source_commit'], role)
    return manifest


def export_images(selection, output):
    """Selection is a private release-owner receipt, never copied into public assets."""
    selected = read_json(selection)
    if (selected.get('approved') is not True or selected.get('reviewer') != 'release-owner'
            or selected.get('host_port_smoke_passed') is not True
            or selected.get('compose_installation_passed') is not True
            or selected.get('paired_capability_passed') is not True):
        raise ValueError('exact image qualification and actual host-port smoke required')
    source, version = selected['source_commit'], selected['version']
    if not COMMIT.fullmatch(source) or not VERSION.fullmatch(version):
        raise ValueError('invalid source/version')
    output = Path(output).absolute()
    if output.exists() or output.is_symlink() or output.resolve() != output:
        raise ValueError('fresh canonical output required')
    # Same real free-space safety as the scheduled release host. No stale nominal
    # cumulative cap or new allocation is introduced by an image export.
    for path, floor in ((Path('/'), 265), (output.parent, 250)):
        if shutil.disk_usage(path).free < floor * 1024**3 + 2 * MAX_ARCHIVE_BYTES:
            raise ValueError('insufficient actual free space for bounded exports')
    images = {}
    for role, name in ROLES.items():
        item = selected['images'][role]
        build_receipt = Path(item['build_receipt'])
        if digest(build_receipt) != item['build_receipt_sha256']:
            raise ValueError('qualified build receipt changed')
        receipt = read_json(build_receipt, limit=4 * 1024**2)
        if any(receipt.get(key) != expected for key, expected in (
                ('source_commit', source), ('image_id', item['image_id']),
                ('binary_sha256', item['binary_sha256']))):
            raise ValueError('qualified build receipt source/image/binary join differs')
        image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', item['image_id']]))[0]
        if image['Id'] != item['image_id'] or not SHA.fullmatch(item['binary_sha256']):
            raise ValueError('qualified image or binary identity differs')
        validate_image(image, source, role)
        images[role] = {'repository': 'ghcr.io/lukacf/' + name, 'image_id': item['image_id'],
                        'binary_sha256': item['binary_sha256'], 'archive': name + '.docker.tar'}
    output.mkdir(mode=0o700)
    for item in images.values():
        archive = output / item['archive']
        subprocess.run(['docker', 'image', 'save', '--output', str(archive), item['image_id']], check=True, timeout=300)
        item.update(archive_bytes=archive.stat().st_size, archive_sha256=digest(archive))
        if item['archive_bytes'] > MAX_ARCHIVE_BYTES:
            raise ValueError('image export exceeds bound; preserve partial output for diagnosis')
    manifest = {'format': 'meerkat-recall-container-delivery/v1', 'repository': REPOSITORY,
                'source_commit': source, 'version': version, 'images': images,
                'qualification': 'Exact images accepted by release owner; raw operational evidence retained privately.'}
    validate_manifest(manifest)
    path = output / 'container-delivery.json'
    path.write_text(json.dumps(manifest, indent=2) + '\n')
    verify(output, digest(path))
    return {'manifest': str(path), 'sha256': digest(path), 'published': False}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest='command', required=True)
    e = sub.add_parser('export'); e.add_argument('--selection', required=True, type=Path); e.add_argument('--output', required=True, type=Path)
    v = sub.add_parser('verify'); v.add_argument('--directory', required=True, type=Path); v.add_argument('--sha256', required=True)
    a = p.parse_args()
    try:
        result = export_images(a.selection, a.output) if a.command == 'export' else verify(a.directory, a.sha256)
        print(json.dumps(result))
    except (ValueError, OSError, KeyError, subprocess.SubprocessError):
        p.exit(1, 'container delivery refused; inspect retained private evidence, no credentials printed\n')
