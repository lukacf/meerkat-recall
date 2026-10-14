#!/usr/bin/env python3
"""Canonical Actions-only promotion of qualified images; no builds or services."""
import base64
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import urllib.parse

import container_delivery as delivery


def response(path, headers):
    connection = http.client.HTTPSConnection('ghcr.io', timeout=30)
    try:
        connection.request('GET', path, headers=headers)
        reply = connection.getresponse()
        raw = reply.read(1024**2 + 1)
        if len(raw) > 1024**2:
            raise ValueError('registry response exceeds bound')
        return reply.status, dict(reply.getheaders()), raw
    finally:
        connection.close()


def registry_manifest(repository, version, expected_config):
    if repository not in {'ghcr.io/lukacf/' + name for name in delivery.ROLES.values()}:
        raise ValueError('canonical repository required')
    name = repository.removeprefix('ghcr.io/')
    credential = base64.b64encode((os.environ['GITHUB_ACTOR'] + ':' + os.environ['GH_TOKEN']).encode()).decode()
    status, _, raw = response('/token?' + urllib.parse.urlencode({
        'service': 'ghcr.io', 'scope': 'repository:' + name + ':pull,push'}),
        {'Authorization': 'Basic ' + credential})
    if status != 200:
        raise ValueError('registry authentication failed; absence not established')
    token = json.loads(raw).get('token')
    if not token:
        raise ValueError('registry authorization token missing')
    status, headers, raw = response('/v2/' + name + '/manifests/' + version, {
        'Authorization': 'Bearer ' + token,
        'Accept': 'application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'})
    return classify_manifest(status, headers, raw, expected_config)


def classify_manifest(status, headers, raw, expected_config):
    value = json.loads(raw)
    if status == 404:
        codes = [item.get('code') for item in value.get('errors', [])]
        if codes and all(code in ('MANIFEST_UNKNOWN', 'NAME_UNKNOWN') for code in codes):
            return None
        raise ValueError('unrecognized registry absence response')
    if status != 200 or value.get('schemaVersion') != 2:
        raise ValueError('registry lookup failed; absence not established')
    if value.get('config', {}).get('digest') != expected_config:
        raise ValueError('release tag already names different image; refusing overwrite')
    digest = 'sha256:' + hashlib.sha256(raw).hexdigest()
    if {k.lower(): v for k, v in headers.items()}.get('docker-content-digest') != digest:
        raise ValueError('registry manifest digest differs')
    return digest


def draft_release(tag):
    # The REST tag route only returns published releases. gh resolves drafts
    # through GraphQL, then we bind the result to the numeric REST release ID.
    found = json.loads(subprocess.check_output([
        'gh', 'release', 'view', tag, '--repo', 'lukacf/meerkat-recall',
        '--json', 'databaseId'], timeout=30))
    release_id = found.get('databaseId')
    if type(release_id) is not int or release_id <= 0:
        raise ValueError('positive draft release ID required')
    release = json.loads(subprocess.check_output([
        'gh', 'api', f'repos/lukacf/meerkat-recall/releases/{release_id}'], timeout=30))
    if type(release.get('id')) is not int or release['id'] != release_id:
        raise ValueError('selected release ID differs')
    return release


def verify_dispatch_source(source, source_directory):
    if (os.environ.get('GITHUB_ACTIONS') != 'true'
            or os.environ.get('GITHUB_REPOSITORY') != 'lukacf/meerkat-recall'
            or os.environ.get('GITHUB_REF') != 'refs/heads/main'):
        raise ValueError('canonical main GitHub Actions publisher required')
    tooling_sha = os.environ.get('GITHUB_SHA', '')
    if not delivery.COMMIT.fullmatch(source) or not delivery.COMMIT.fullmatch(tooling_sha):
        raise ValueError('exact qualified source and tooling commits required')
    tooling_root = Path(__file__).resolve().parents[2]
    for directory, expected in ((tooling_root, tooling_sha), (Path(source_directory), source)):
        actual = subprocess.check_output(
            ['git', '-C', str(directory), 'rev-parse', 'HEAD'], text=True, timeout=30).strip()
        if actual != expected:
            raise ValueError('checked-out tooling or qualified source differs')
    return tooling_sha


def verify_release_binding(release, tag, source):
    if (release.get('draft') is not True or release.get('tag_name') != tag
            or release.get('prerelease') is not False):
        raise ValueError('selected normal draft release required')
    def api(path):
        return json.loads(subprocess.check_output(['gh', 'api', 'repos/lukacf/meerkat-recall/' + path], timeout=30))
    obj = api('git/ref/tags/' + tag)['object']
    for _ in range(4):
        if obj.get('type') == 'commit':
            if obj.get('sha') != source:
                raise ValueError('release tag does not resolve to qualified source')
            return
        if obj.get('type') != 'tag' or not delivery.COMMIT.fullmatch(obj.get('sha', '')):
            break
        obj = api('git/tags/' + obj['sha'])['object']
    raise ValueError('release tag cannot be resolved within bound')


def promote(directory, expected_sha, release_tag, source_commit, source_directory):
    tooling_sha = verify_dispatch_source(source_commit, source_directory)
    root = Path(directory)
    value = delivery.verify(root, expected_sha)
    if 'v' + value['version'] != release_tag or value['source_commit'] != source_commit:
        raise ValueError('dispatch, release and qualified source differ')
    release = draft_release(release_tag)
    verify_release_binding(release, release_tag, value['source_commit'])
    (root / 'publication-binding.json').write_text(json.dumps({
        'tooling_commit': tooling_sha, 'qualified_source_commit': source_commit,
        'release_id': release['id'], 'release_tag': release_tag,
        'delivery_manifest_sha256': expected_sha}, indent=2) + '\n')
    # Check both destinations before any mutation. Errors never imply absence.
    for item in value['images'].values():
        registry_manifest(item['repository'], value['version'], item['image_id'])
    published = {}
    record = root / 'published-images.json'
    for role, item in value['images'].items():
        tag = item['repository'] + ':' + value['version']
        # Recheck immediately before push. The workflow serializes canonical
        # publishers; GHCR provides no compare-and-swap against outside writers.
        remote = registry_manifest(item['repository'], value['version'], item['image_id'])
        if remote is None:
            subprocess.run(['docker', 'load', '--input', str(root / item['archive'])], check=True, timeout=300)
            image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', item['image_id']], timeout=30))[0]
            if image['Id'] != item['image_id']:
                raise ValueError('loaded image differs')
            subprocess.run(['docker', 'tag', item['image_id'], tag], check=True, timeout=30)
            subprocess.run(['docker', 'push', tag], check=True, timeout=600)
            remote = registry_manifest(item['repository'], value['version'], item['image_id'])
            if remote is None:
                raise ValueError('published manifest absent')
        published[role] = {'image': item['repository'] + '@' + remote,
                           'tag': tag, 'config_digest': item['image_id']}
        # Retain the first successful publication even if the second fails.
        record.write_text(json.dumps(published, indent=2) + '\n')


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', required=True)
    parser.add_argument('--sha256', required=True)
    parser.add_argument('--release-tag', required=True)
    parser.add_argument('--source-commit', required=True)
    parser.add_argument('--source-directory', required=True)
    args = parser.parse_args()
    try:
        promote(args.directory, args.sha256, args.release_tag, args.source_commit, args.source_directory)
    except Exception as error:
        parser.exit(1, 'container promotion refused (' + type(error).__name__ + '); no credentials printed\n')
