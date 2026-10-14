#!/usr/bin/env python3
"""Explicit bounded smoke of a running owned container's actual Docker host port.

No build, service start, provider call, relay or network mutation. This checks
host publishing and auth/readiness only, not semantic ingestion qualification.
"""
import argparse
import hashlib
import http.client
import json
from pathlib import Path
import re
import stat
import subprocess


def validate_mapping(container, container_id, port):
    if (container.get('Id') != container_id or not container.get('State', {}).get('Running')
            or container.get('HostConfig', {}).get('NetworkMode') == 'host'):
        raise ValueError('explicit running owned container with real port publishing required')
    ports = container.get('NetworkSettings', {}).get('Ports', {}).get('3000/tcp')
    if ports != [{'HostIp': '127.0.0.1', 'HostPort': str(port)}]:
        raise ValueError('actual Docker loopback publication differs or is absent')


def request(port, route, token=None, space=None):
    connection = http.client.HTTPConnection('127.0.0.1', port, timeout=5)
    headers = {'Accept': 'application/json'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    if space:
        headers['X-Space-Id'] = space
    try:
        connection.request('GET', route, headers=headers)
        response = connection.getresponse()
        raw = response.read(65537)
        if len(raw) > 65536:
            raise ValueError('response exceeds bound')
        # http.client does not follow redirects or ambient HTTP proxies.
        return response.status, json.loads(raw)
    finally:
        connection.close()


def smoke(container_id, port, token_file, space, require_ready):
    if not re.fullmatch('[a-f0-9]{64}', container_id) or not 1024 <= port <= 65535:
        raise ValueError('exact container ID and unprivileged host port required')
    if not re.fullmatch('[A-Za-z0-9_.:-]{1,128}', space):
        raise ValueError('explicit named space required')
    path = Path(token_file)
    metadata = path.lstat()
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077 or metadata.st_size > 65536:
        raise ValueError('private regular bearer file required')
    token = path.read_text().rstrip('\r\n')
    if not token or '\r' in token or '\n' in token:
        raise ValueError('invalid bearer shape')
    before = json.loads(subprocess.check_output(['docker', 'inspect', container_id], timeout=10))[0]
    validate_mapping(before, container_id, port)
    health, _ = request(port, '/v1/health')
    denied, _ = request(port, '/v1/readiness')
    authorized, readiness = request(port, '/v1/readiness', token, space)
    if health != 200 or denied not in (401, 403):
        raise ValueError('liveness or unauthenticated refusal failed')
    if (authorized, readiness.get('ready')) not in ((200, True), (503, False)):
        raise ValueError('authenticated readiness contract failed')
    if require_ready and (authorized != 200 or readiness.get('ready') is not True):
        raise ValueError('configured service is not ready')
    after = json.loads(subprocess.check_output(['docker', 'inspect', container_id], timeout=10))[0]
    validate_mapping(after, container_id, port)
    if before['State']['StartedAt'] != after['State']['StartedAt'] or before['Image'] != after['Image']:
        raise ValueError('container restarted or image changed during smoke')
    return {'state': 'PASS_HOST_PORT_AND_AUTH_SMOKE', 'container_id': container_id,
            'image_id': before['Image'], 'host': '127.0.0.1', 'port': port,
            'health_status': health, 'unauthenticated_readiness_status': denied,
            'authenticated_readiness_status': authorized, 'ready': readiness['ready'],
            'space': space, 'http_calls': 3, 'relay_used': False,
            'semantic_qualification': False, 'provider_calls': 0}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--container-id', required=True); p.add_argument('--port', type=int, required=True)
    p.add_argument('--token-file', type=Path, required=True); p.add_argument('--space', required=True)
    p.add_argument('--require-ready', action='store_true'); p.add_argument('--output', type=Path, required=True)
    a = p.parse_args()
    try:
        if a.output.exists() or a.output.is_symlink():
            raise ValueError('fresh receipt output required')
        result = smoke(a.container_id, a.port, a.token_file, a.space, a.require_ready)
        with a.output.open('x') as stream:
            json.dump(result, stream, indent=2); stream.write('\n')
        a.output.chmod(0o600)
        print(json.dumps({'state': result['state'], 'receipt': str(a.output)}))
    except Exception as error:
        p.exit(1, 'host-port smoke failed (' + type(error).__name__ + '); no readiness or publication qualification granted\n')
