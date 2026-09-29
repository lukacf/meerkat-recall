import base64
import datetime
import hashlib
import hmac
import json
import os
import shutil
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Optional

import requests

BASE_URL = "http://127.0.0.1:3000"
SQL_URL = "http://localhost:8000/sql"
SQL_AUTH = ("root", "root")
SURREAL_NS = os.getenv("SURREALDB_NAMESPACE", "elephant")
SURREAL_DB = os.getenv("SURREALDB_DATABASE", "test")
SQL_HEADERS = {
    "surreal-ns": SURREAL_NS,
    "surreal-db": SURREAL_DB,
    "Accept": "application/json",
    "Content-Type": "text/plain",
}
JWT_SECRET = os.getenv("JWT_SECRET", "development-secret-key")
HTTP_TIMEOUT_SECS = float(os.getenv("HTTP_TIMEOUT_SECS", "10"))
HTTP_RETRIES = int(os.getenv("HTTP_RETRIES", "30"))
HTTP_RETRY_BACKOFF_SECS = float(os.getenv("HTTP_RETRY_BACKOFF_SECS", "0.25"))
HTTP_MAX_BACKOFF_SECS = float(os.getenv("HTTP_MAX_BACKOFF_SECS", "2.0"))

PHASE13_LOCK_ENV = "ELEPHANT_PHASE13_LOCK_DIR"


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except OSError:
        return False
    else:
        return True


@contextmanager
def scenario_lock(name: str = "phase13_scenarios"):
    repo_root = Path(__file__).resolve().parent.parent
    lock_root = Path(
        os.getenv("RCT_LOCK_ROOT", str(repo_root / ".rct" / "outputs" / "tool-locks"))
    )
    lock_dir = lock_root / f"{name}.lock.d"
    pid_path = lock_dir / "pid"

    held = os.getenv(PHASE13_LOCK_ENV)
    if held == str(lock_dir):
        yield
        return

    lock_root.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            lock_dir.mkdir()
            pid_path.write_text(str(os.getpid()), encoding="utf-8")
            break
        except FileExistsError:
            pid = None
            try:
                pid = int(pid_path.read_text(encoding="utf-8").strip())
            except Exception:
                pid = None

            if pid is not None and _pid_is_alive(pid):
                time.sleep(1)
                continue

            try:
                shutil.rmtree(lock_dir, ignore_errors=True)
            except Exception:
                time.sleep(1)

    os.environ[PHASE13_LOCK_ENV] = str(lock_dir)
    try:
        yield
    finally:
        os.environ.pop(PHASE13_LOCK_ENV, None)
        shutil.rmtree(lock_dir, ignore_errors=True)


def iso_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def record_id(table: str) -> str:
    return f"{table}:{uuid.uuid4()}"


def base_fields(table: str, space: str = "default", created_by: str = "tester") -> Dict[str, Any]:
    now = iso_now()
    return {
        "id": record_id(table),
        "space_id": space,
        "created_at": now,
        "updated_at": now,
        "created_by": created_by,
        "deleted_at": None,
        "provenance": {
            "sources": [],
            "extractor": None,
            "work_run_id": None,
            "run_id": None,
            "prompt_id": None,
            "config_id": None,
            "chain": [],
        },
        "security": {
            "policy_id": "default",
            "level": 0,
            "labels": [],
            "handling": [],
        },
        "subjects": [],
        "subjects_state": "unknown",
    }


def make_entity(
    display_name: str,
    entity_type: str = "person",
    space: str = "default",
    identity_keys=None,
    aliases=None,
    metadata=None,
) -> Dict[str, Any]:
    if identity_keys is None:
        identity_keys = []
    if aliases is None:
        aliases = []
    if metadata is None:
        metadata = {}
    base = base_fields("entity", space=space)
    return {
        **base,
        "type": entity_type,
        "display_name": display_name,
        "aliases": aliases,
        "summary": None,
        "handles": [],
        "identity_keys": identity_keys,
        "risk_level": None,
        "trust_level": None,
        "known_safe": False,
        "first_seen": None,
        "last_seen": None,
        "status": "active",
        "redirect_to": None,
        "metadata": metadata,
    }


def make_doc(
    title: str,
    text: str,
    rendition_key: str,
    space: str = "default",
    source_id: Optional[str] = None,
    metadata=None,
) -> Dict[str, Any]:
    if metadata is None:
        metadata = {}
    base = base_fields("doc", space=space)
    return {
        **base,
        "title": title,
        "text": text,
        "source": source_id,
        "rendition_key": rendition_key,
        "metadata": metadata,
    }


def make_source(
    source_type: str,
    content: str,
    space: str = "default",
    metadata=None,
) -> Dict[str, Any]:
    if metadata is None:
        metadata = {}
    h = hashlib.sha256(content.encode("utf-8")).hexdigest()
    base = base_fields("source", space=space)
    return {
        **base,
        "type": source_type,
        "external_id": None,
        "uri": None,
        "path": None,
        "parent": None,
        "range": None,
        "timestamp": None,
        "author": None,
        "channel": None,
        "hash": h,
        "metadata": metadata,
    }

def _request_with_retries(call, retries: int = HTTP_RETRIES):
    backoff = HTTP_RETRY_BACKOFF_SECS
    for attempt in range(retries):
        try:
            return call()
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
            if attempt + 1 >= retries:
                raise
            time.sleep(backoff)
            backoff = min(backoff * 1.5, HTTP_MAX_BACKOFF_SECS)


def post(path: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None):
    url = f"{BASE_URL}{path}"
    resp = _request_with_retries(
        lambda: requests.post(
            url, json=payload, headers=headers or {}, timeout=HTTP_TIMEOUT_SECS
        )
    )
    resp.raise_for_status()
    return resp


def put(path: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None):
    url = f"{BASE_URL}{path}"
    resp = _request_with_retries(
        lambda: requests.put(
            url, json=payload, headers=headers or {}, timeout=HTTP_TIMEOUT_SECS
        )
    )
    resp.raise_for_status()
    return resp


def get(path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None):
    url = f"{BASE_URL}{path}"
    resp = _request_with_retries(
        lambda: requests.get(
            url, params=params, headers=headers or {}, timeout=HTTP_TIMEOUT_SECS
        )
    )
    resp.raise_for_status()
    return resp


def patch(path: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None):
    url = f"{BASE_URL}{path}"
    resp = _request_with_retries(
        lambda: requests.patch(
            url, json=payload, headers=headers or {}, timeout=HTTP_TIMEOUT_SECS
        )
    )
    resp.raise_for_status()
    return resp


def sql(query: str):
    resp = _request_with_retries(
        lambda: requests.post(
            SQL_URL,
            data=query,
            headers=SQL_HEADERS,
            auth=SQL_AUTH,
            timeout=HTTP_TIMEOUT_SECS,
        )
    )
    resp.raise_for_status()
    return resp.json()


def normalize_record_id(value):
    if isinstance(value, dict):
        value = value.get("id")
    if not isinstance(value, str):
        return value
    if value.startswith('"') and value.endswith('"'):
        value = value[1:-1]
    if ":u'" in value and value.endswith("'"):
        prefix, rest = value.split(":u'", 1)
        return f"{prefix}:{rest[:-1]}"
    if ":'" in value and value.endswith("'"):
        prefix, rest = value.split(":'", 1)
        return f"{prefix}:{rest[:-1]}"
    return value


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def jwt_token(claims: Dict[str, Any], secret: str = JWT_SECRET) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = _b64url(json.dumps(header, separators=(",", ":"), sort_keys=True).encode("utf-8"))
    payload_b64 = _b64url(
        json.dumps(claims, separators=(",", ":"), sort_keys=True).encode("utf-8")
    )
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{header_b64}.{payload_b64}.{_b64url(signature)}"


def make_claims(
    subject: str,
    space_id: str,
    scopes: Optional[list[str]] = None,
    security_level: int = 10,
    security_labels: Optional[list[str]] = None,
    exp_seconds: int = 3600,
) -> Dict[str, Any]:
    now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
    return {
        "sub": subject,
        "exp": now + exp_seconds,
        "iat": now,
        "space_id": space_id,
        "scopes": scopes or ["*"],
        "security_level": security_level,
        "security_labels": security_labels or [],
        "clearances": [],
        "purpose": [],
    }


def auth_headers(claims: Dict[str, Any]) -> Dict[str, str]:
    return {"Authorization": f"Bearer {jwt_token(claims)}"}


def wait_for(predicate, timeout: float = 60.0, interval: float = 1.0, error_message: Optional[str] = None):
    start = time.time()
    while time.time() - start < timeout:
        result = predicate()
        if result is not None:
            return result
        time.sleep(interval)
    raise TimeoutError(error_message or "Timed out waiting for condition")


def wait_for_api_ready(timeout: float = 60.0):
    def healthy():
        try:
            resp = requests.get(f"{BASE_URL}/health", timeout=2)
        except requests.exceptions.RequestException:
            return None
        if resp.status_code == 200:
            return True
        return None

    wait_for(healthy, timeout=timeout, interval=0.5)


def ensure_predicate_alias(
    predicate_id: str,
    headers: Dict[str, str],
    description: Optional[str] = None,
    confidence: float = 0.95,
    alias: Optional[str] = None,
) -> Dict[str, Any]:
    alias_value = alias or predicate_id
    preds = get("/v1/predicates?limit=200", headers=headers).json()
    existing = next(
        (
            item
            for item in preds.get("results", [])
            if item.get("predicate", {}).get("predicate_id") == predicate_id
        ),
        None,
    )
    if existing is None:
        predicate = post(
            "/v1/predicates",
            {"predicate_id": predicate_id, "description": description},
            headers=headers,
        ).json()
        existing = {"predicate": predicate, "aliases": []}

    predicate = existing["predicate"]
    aliases = existing.get("aliases") or []
    active = next(
        (
            alias
            for alias in aliases
            if alias.get("alias") == alias_value and alias.get("status") == "active"
        ),
        None,
    )
    if active is not None:
        return active

    created = post(
        f"/v1/predicates/{predicate['id']}/aliases",
        {"alias": alias_value, "confidence": confidence},
        headers=headers,
    ).json()
    if created.get("status") == "active":
        return created

    updated = patch(
        f"/v1/predicates/{predicate['id']}/aliases/{created['id']}",
        {"status": "active"},
        headers=headers,
    ).json()
    return updated
