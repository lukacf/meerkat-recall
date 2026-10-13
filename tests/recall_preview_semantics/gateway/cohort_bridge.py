"""Public qualification-only D28 bridge; no provider key or ledger access.

Authority is the running isolated gateway supervisor and its original allocation.
This module cannot manufacture accounting from a public request/time/body hash.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import os
from pathlib import Path
import stat


class CohortUnavailable(RuntimeError):
    pass


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(value).hexdigest()


def _payload(manifest, allocation, action):
    if not isinstance(manifest, dict) or not isinstance(allocation, dict):
        raise CohortUnavailable("cohort inputs missing")
    if manifest.get("execution_authorized") is not True or manifest.get("provider_dispatch_authorized") is not True:
        raise CohortUnavailable("cohort execution not authorized")
    gateway = os.environ.get("RECALL_GATEWAY_MANIFEST_SHA256", "")
    if len(gateway) != 64:
        raise CohortUnavailable("isolated cohort supervisor unavailable")
    payload = {"action": action, "gateway_manifest_sha256": gateway,
            "allocation_sha256": manifest["provider_allocation_sha256"],
            "contract_sha256": manifest["contract_sha256"], "corpus_sha256": manifest["corpus_sha256"],
            "preparation_contract_sha256": manifest["preparation_contract_sha256"],
            "typed_delta_sha256": manifest["typed_delta_sha256"],
            "backend": manifest["backend"], "phase": manifest.get("provider_phase", "measurement"),
            "caller_sha256": digest(canonical(manifest)), "binary_sha256": manifest["binary_sha256"]}
    if "profile_id" in manifest:
        payload["profile_id"] = manifest["profile_id"]
    return payload


def _strict_json(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise CohortUnavailable("cohort JSON duplicate key")
            result[key] = value
        return result

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs)
        # Reject constants and overflowing exponents recursively, not just NaN
        # tokens; the reviewed control contract has finite JSON values only.
        canonical(value)
        return value
    except (ValueError, UnicodeError, RecursionError) as error:
        raise CohortUnavailable("cohort JSON malformed or nonfinite") from error


def _call(payload):
    token = os.environ.get("RECALL_GATEWAY_COHORT_CONTROL_TOKEN", "")
    if not token or len(token) > 128 or not all(32 < ord(c) < 127 for c in token):
        raise CohortUnavailable("isolated cohort capability unavailable")
    body = canonical(payload)
    if len(body) > 16384:
        raise CohortUnavailable("cohort control body limit")
    connection = http.client.HTTPConnection("127.0.0.1", 43129, timeout=10)
    try:
        connection.request("POST", "/v1/_qualification/cohort", body=body,
                           headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        response = connection.getresponse()
        lengths = response.headers.get_all("Content-Length", [])
        types = response.headers.get_all("Content-Type", [])
        if (response.status != 200 or len(lengths) != 1
                or response.headers.get_all("Transfer-Encoding", [])
                or types != ["application/json"]
                or not lengths[0].isascii() or not lengths[0].isdecimal()
                or len(lengths[0]) > 5 or not 0 < int(lengths[0]) <= 65536):
            raise CohortUnavailable("cohort supervisor framing invalid")
        data = response.read(65537)
        if len(data) != int(lengths[0]) or response.length != 0:
            raise CohortUnavailable("cohort supervisor refused or incomplete")
        value = _strict_json(data)
        if not isinstance(value, dict):
            raise CohortUnavailable("cohort supervisor response malformed")
        return value
    except (OSError, http.client.HTTPException) as error:
        raise CohortUnavailable("cohort supervisor transport incomplete") from error
    finally:
        connection.close()


def _verify_file(manifest, name, parsed=None):
    path = Path(manifest[name + "_path"])
    if not path.is_absolute() or path != path.resolve():
        raise CohortUnavailable("cohort input path is not canonical")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > 131072:
            raise CohortUnavailable("cohort input file limit")
        data = os.read(fd, 131073)
    finally:
        os.close(fd)
    if digest(data) != manifest[name + "_sha256"] or (parsed is not None and _strict_json(data) != parsed):
        raise CohortUnavailable("cohort parsed input differs from pinned file")


def authorize_provider_allocation(manifest, contract, allocation):
    payload = _payload(manifest, allocation, "begin")
    # The independent collector already verifies the exact contract file hash.
    # Its parsed content must also agree with the reviewed allocation dimensions.
    if not isinstance(contract, dict):
        raise CohortUnavailable("independent contract missing")
    _verify_file(manifest, "contract", contract)
    _verify_file(manifest, "provider_allocation", allocation)
    _verify_file(manifest, "preparation_contract")
    _verify_file(manifest, "typed_delta")
    return _call(payload)


def provider_accounting(manifest, observation, allocation):
    payload = _payload(manifest, allocation, "close")
    if not isinstance(observation, dict):
        raise CohortUnavailable("cohort observation missing")
    payload["observation_sha256"] = digest(canonical(observation))
    return _call(payload)


def provider_accounting_with_evidence(manifest, observation, allocation):
    """Explicit v2 carrier; keep the original v1 close available to old callers."""
    payload = _payload(manifest, allocation, "close_with_evidence")
    if not isinstance(observation, dict):
        raise CohortUnavailable("cohort observation missing")
    payload["observation_sha256"] = digest(canonical(observation))
    envelope = _call(payload)
    if (set(envelope) != {"format", "close", "export"}
            or envelope["format"] != "recall-exclusive-cohort-close-envelope/v2"
            or not isinstance(envelope["close"], dict)):
        raise CohortUnavailable("original accounting envelope differs")
    return envelope


def export_cohort_evidence(manifest, begin, close, exported):
    """No new control/provider call: read the original read-only capture channel."""
    import importlib.util
    specification = importlib.util.spec_from_file_location("recall_accounting_channel", "/accounting_channel.py")
    if specification is None or specification.loader is None:
        raise CohortUnavailable("original accounting reader unavailable")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module.open_export(manifest, begin, close, exported)
