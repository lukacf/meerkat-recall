#!/usr/bin/env python3
"""Explicit read-only remote version/capability probe; does not qualify storage recovery."""
import argparse
import base64
import json
import os
import re
import urllib.parse
import urllib.request

from prepare import read_lock

MAX_RESPONSE_BYTES = 65536


def capability_sql(lock: dict) -> str:
    vector = lock["capability"]
    return (
        'RETURN recall::reference_token_version();\n'
        f'RETURN recall::reference_tokens("{vector["journal"]}", "{vector["space"]}", '
        f'encoding::hex::decode("{vector["key_hex"]}"), [], [type::thing("recall_probe", 42), '
        'type::thing("recall_probe", "42")]);\n'
    )


def validate_capability(body: bytes, lock: dict) -> None:
    if len(body) > MAX_RESPONSE_BYTES:
        raise ValueError("capability response exceeds bound")
    data = json.loads(body)
    if not isinstance(data, list) or len(data) != 2 or any(not isinstance(row, dict) or row.get("status") != "OK" for row in data):
        raise ValueError("server lacks the required Recall native capability")
    if type(data[0].get("result")) is not int or data[0]["result"] != lock["reference_token_version"]:
        raise ValueError("native reference version differs from paired source")
    expected = sorted([lock["capability"]["number_token"], lock["capability"]["string_token"]])
    if data[1].get("result") != expected:
        raise ValueError("native reference fixed vector/type distinction failed")


def probe(endpoint: str, namespace: str, database: str, expected_metadata: str, username: str, password: str) -> dict:
    parsed = urllib.parse.urlsplit(endpoint)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("supply an HTTP(S) endpoint without embedded credentials/query/fragment")
    if not re.fullmatch(r"recall\.reference-v1\.[0-9a-f]{12}", expected_metadata):
        raise ValueError("exact paired build metadata required")
    for name in (namespace, database, username):
        if not name or any(ord(c) < 32 or ord(c) == 127 for c in name):
            raise ValueError("invalid explicit namespace/database/username")
    if not password:
        raise ValueError("explicit authenticated capability credentials required")
    lock = read_lock()
    headers = {"Accept": "application/json", "Content-Type": "text/plain", "Surreal-NS": namespace, "Surreal-DB": database,
               "Authorization": "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()}
    # Never follow a redirect that could forward authentication to another host.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, response_headers, newurl):
            return None
    opener = urllib.request.build_opener(NoRedirect)
    def request(path: str, data: bytes | None = None) -> bytes:
        req = urllib.request.Request(endpoint.rstrip("/") + path, data=data, headers=headers)
        with opener.open(req, timeout=15) as response:
            body = response.read(MAX_RESPONSE_BYTES + 1)
        if len(body) > MAX_RESPONSE_BYTES:
            raise ValueError("remote capability response exceeds bound")
        return body
    version = request("/version").decode().strip()
    if version != f'surrealdb-{lock["upstream_version"]}+{expected_metadata}':
        raise ValueError("remote server version/build identity is not the paired artifact")
    validate_capability(request("/sql", capability_sql(lock).encode()), lock)
    return {"status": "version_and_pure_capability_pass", "version": version,
            "reference_token_version": lock["reference_token_version"],
            "limits": "Not a HTTP/WS/import/erasure/restart/concurrency qualification receipt"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--expected-build-metadata", required=True)
    args = parser.parse_args()
    # Credentials are deployment inputs, never command-line arguments or output.
    try:
        result = probe(args.endpoint, args.namespace, args.database, args.expected_build_metadata,
                       os.environ.get("RECALL_SURREAL_PROBE_USER", ""), os.environ.get("RECALL_SURREAL_PROBE_PASSWORD", ""))
        print(json.dumps(result))
    except Exception:
        raise SystemExit("paired remote capability check failed; no readiness/qualification granted") from None
