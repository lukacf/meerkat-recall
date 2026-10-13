"""Original D28 evidence custody; D/L alone define component predicates.

This in-process adapter keeps C's actual owned services alive while a caller
collects preparation and measurement. It never turns a v1 observation into a
qualified receipt, and never creates or replaces G's allocation or clock.
"""
from __future__ import annotations

from contextlib import asynccontextmanager, contextmanager
from collections.abc import Mapping
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import stat
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
SELF = "tests/recall_preview_semantics/workload_orchestration.py"
VALIDATOR = "tests/recall_preview_semantics/dynamic_component_validator.py"
STARTUP = "scripts/qualification/d28_runtime_bootstrap.py"
PREDICATES = {
    "installed_evidence_validator": "tests/recall_preview_semantics/installed_evidence_validator.py",
    "provider_evidence_validator": "tests/recall_preview_semantics/provider_evidence_validator.py",
}
MAX_FILE = 64 * 1024**2
# Runtime originals only. G's 20 GiB channel is disk-backed and stays outside
# this pool; EvidenceUnion never converts its lazy resolver into a dictionary.
MAX_TOTAL = 352 * 1024**2
MAX_FILES = 16384
MAX_INLINE_METADATA = 8 * 1024**2
REF_FIELDS = {"sha256", "bytes", "kind"}
PROFILE_SOURCE = "tests/recall_preview_semantics/workload_profiles.py"
EXPECTATIONS_SOURCE = "tests/recall_preview_semantics/semantic140_expectations.py"
PROFILE_SHA256 = "aa55b0a29cc0663425c889ef7206f17d48613eb6263e39e100919857babe4f13"
_profile_path = ROOT / PROFILE_SOURCE
_profile_raw = _profile_path.read_bytes()
if hashlib.sha256(_profile_raw).hexdigest() != PROFILE_SHA256:
    raise ValueError("orchestration_closed_profile_source_changed")
_profile_spec = importlib.util.spec_from_file_location("recall_orchestration_profiles", _profile_path)
profiles = importlib.util.module_from_spec(_profile_spec)
sys.modules[_profile_spec.name] = profiles
exec(compile(_profile_raw, str(_profile_path), "exec", dont_inherit=True), profiles.__dict__)
del _profile_raw
GATEWAY_ORIGINALS = {"/gateway-manifest.json": "gateway-manifest", "/gateway-launch.json": "gateway-launch-spec"}
RUNTIME_REFERENCES = ("bootstrap", "actual_runtime_configuration", "actual_space_configurations",
                      "controller", "current_processes", "outer_launch", "capture_inventory", "command_inventory",
                      "cleanup_obligations")


def require(condition, code):
    if not condition:
        raise ValueError(code)


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "orchestration_duplicate_json_key")
            result[key] = value
        return result
    require(type(raw) is bytes, "orchestration_original_bytes")
    try:
        result = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs)
        encode(result)
        return result
    except (ValueError, UnicodeError, TypeError, RecursionError) as error:
        raise ValueError("orchestration_invalid_json") from error


def reference(raw, kind):
    require(type(raw) is bytes and type(kind) is str and 0 < len(kind) <= 128,
            "orchestration_reference_input")
    return {"sha256": digest(raw), "bytes": len(raw), "kind": kind}


def file_bound(profile_id=profiles.LEGACY_ID):
    selected = profiles.profile(profile_id)
    return MAX_FILE if selected.id == profiles.LEGACY_ID else selected.observation_bytes


def runtime_bound(profile_id=profiles.LEGACY_ID):
    selected = profiles.profile(profile_id)
    return MAX_TOTAL if selected.id == profiles.LEGACY_ID else selected.runtime_bytes


def checked_reference(value, *, profile_id=profiles.LEGACY_ID):
    maximum = file_bound(profile_id)
    require(type(value) is dict and set(value) == REF_FIELDS, "orchestration_reference_fields")
    require(type(value["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", value["sha256"])
            and type(value["bytes"]) is int and 0 <= value["bytes"] <= maximum
            and type(value["kind"]) is str and 0 < len(value["kind"]) <= 128,
            "orchestration_reference_identity")
    return value


def read_original(path, maximum, *, private=False, profile_id=profiles.LEGACY_ID):
    """Read one canonical regular inode, checking its identity across the read."""
    path = Path(path)
    require(type(maximum) is int and 0 <= maximum <= file_bound(profile_id) and path.is_absolute()
            and path == path.resolve() and not path.is_symlink(), "orchestration_original_path")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        before = os.fstat(descriptor)
        require(stat.S_ISREG(before.st_mode) and before.st_size <= maximum
                and (not private or before.st_mode & 0o077 == 0), "orchestration_original_file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(maximum + 1)
        after = os.fstat(descriptor)
        visible = path.stat(follow_symlinks=False)
        keys = ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_mode")
        require(all(getattr(before, key) == getattr(after, key) == getattr(visible, key) for key in keys)
                and len(raw) == before.st_size <= maximum, "orchestration_original_file_changed")
        return raw
    finally:
        os.close(descriptor)


def private_new(path, raw, *, profile_id=profiles.LEGACY_ID):
    """Persist a new derived record without overwriting any original evidence."""
    path = Path(path)
    require(type(raw) is bytes and len(raw) <= file_bound(profile_id) and path.is_absolute()
            and path.parent == path.parent.resolve(), "orchestration_output_path")
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


class OriginalByteMap(Mapping):
    """Load a single indexed original without retaining file contents."""
    def __init__(self, owner):
        self.owner = owner

    def __iter__(self):
        return iter(self.owner.references)

    def __len__(self):
        return len(self.owner.references)

    def __contains__(self, key):
        return key in self.owner.references

    def __getitem__(self, key):
        ref = self.owner.references[key]
        paths = self.owner.locations.get(key)
        if paths:
            path = min(paths)
            self.owner.path(path, ref)
            options = {} if self.owner.profile.id == profiles.LEGACY_ID else {"profile_id": self.owner.profile.id}
            raw = read_original(path, ref["bytes"], private=path.startswith("/work/") or path in GATEWAY_ORIGINALS,
                                **options)
        else:
            raw = self.owner.inline[key]
        require(type(raw) is bytes and len(raw) == ref["bytes"] and digest(raw) == key,
                "orchestration_original_hash_size")
        return raw


class OriginalFiles:
    """Typed original bytes and their actual paths, with no authority inference."""
    def __init__(self, *, roots=(Path("/work"), Path("/inputs")), profile_id=profiles.LEGACY_ID):
        self.profile = profiles.profile(profile_id)
        self.roots = tuple(Path(root) for root in roots)
        self.references, self.kinds, self.files, self.locations, self.inline = {}, {}, {}, {}, {}
        self.raw = OriginalByteMap(self)
        self.inline_bytes = 0
        self.total = 0
        self.physical_bytes = 0
        self.forbidden_paths = set()

    def path(self, value, ref=None):
        require(type(value) is str and 0 < len(value) <= 1024, "orchestration_capture_path")
        fixed_original = value in GATEWAY_ORIGINALS
        if fixed_original:
            checked_reference(ref, profile_id=self.profile.id)
            require(ref["kind"] == GATEWAY_ORIGINALS[value] and ref["bytes"] <= 131072,
                    "orchestration_gateway_original_kind_bound")
        path = Path(value)
        require(path.is_absolute() and str(path) == value and ".." not in path.parts
                and (fixed_original or path == Path("/launch-anchor.json") or any(path.is_relative_to(root) for root in self.roots))
                and value not in self.forbidden_paths, "orchestration_capture_path")
        return path

    def retain(self, ref, raw, path=None):
        checked_reference(ref, profile_id=self.profile.id)
        require(type(raw) is bytes and len(raw) == ref["bytes"] and digest(raw) == ref["sha256"],
                "orchestration_original_hash_size")
        sha = ref["sha256"]
        require(sha not in self.kinds or self.kinds[sha] == ref["kind"], "orchestration_kind_alias")
        if sha not in self.references:
            require(len(self.raw) < MAX_FILES and self.total + len(raw) <= runtime_bound(self.profile.id),
                    "orchestration_original_inventory_bound")
            self.references[sha], self.kinds[sha] = dict(ref), ref["kind"]
            self.total += len(raw)
        if path is not None:
            self.path(path, ref)
            require(path not in self.files or self.files[path] == ref, "orchestration_original_path_replaced")
            require(path in self.files or len(self.files) < MAX_FILES, "orchestration_capture_count")
            if self.profile.id == profiles.SEMANTIC140_ID and path not in self.files:
                require(self.physical_bytes + len(raw) <= runtime_bound(self.profile.id),
                        "orchestration_physical_capture_bound")
                self.physical_bytes += len(raw)
            self.files[path] = dict(ref)
            self.locations.setdefault(sha, set()).add(path)
            if sha in self.inline:
                self.inline_bytes -= len(self.inline.pop(sha))
        elif sha not in self.locations and sha not in self.inline:
            # C's capture inventory is briefly pathless while being decoded;
            # its original path is attached immediately below. Large originals
            # always need an actual producer path, never an eager memory pool.
            require(self.inline_bytes + len(raw) <= MAX_INLINE_METADATA,
                    "orchestration_pathless_metadata_bound")
            self.inline[sha] = raw
            self.inline_bytes += len(raw)
        return dict(ref)

    def read(self, path, ref):
        self.path(path, ref)
        checked_reference(ref, profile_id=self.profile.id)
        require(ref["sha256"] in self.raw or self.total + ref["bytes"] <= runtime_bound(self.profile.id),
                "orchestration_original_inventory_bound")
        options = {} if self.profile.id == profiles.LEGACY_ID else {"profile_id": self.profile.id}
        raw = read_original(path, ref["bytes"], private=path.startswith("/work/") or path in GATEWAY_ORIGINALS,
                            **options)
        return self.retain(ref, raw, path)

    def get(self, ref, kind=None):
        checked_reference(ref, profile_id=self.profile.id)
        require(kind is None or ref["kind"] == kind, "orchestration_reference_kind")
        require(ref["sha256"] in self.raw, "orchestration_original_missing")
        raw = self.raw[ref["sha256"]]
        self.retain(ref, raw)
        return raw

    def json(self, ref, kind=None):
        return strict_json(self.get(ref, kind))

    def import_files(self, rows):
        require(type(rows) is list and 0 < len(rows) <= MAX_FILES, "orchestration_file_rows")
        paths = set()
        for row in rows:
            require(type(row) is dict and set(row) == {"path", "ref"}
                    and row["path"] not in paths, "orchestration_duplicate_file_row")
            paths.add(row["path"])
            self.read(row["path"], row["ref"])

    def import_capture(self, bundle):
        """Consume C's actual in-process raw map and original file inventory."""
        require(type(bundle) is dict and isinstance(bundle.get("raw_evidence"), Mapping),
                "orchestration_capture_bundle")
        raw = bundle["raw_evidence"]
        inventory_ref = checked_reference(bundle["capture_inventory"], profile_id=self.profile.id)
        require(inventory_ref["sha256"] in raw, "orchestration_capture_inventory_missing")
        self.retain(inventory_ref, raw[inventory_ref["sha256"]])
        inventory = self.json(inventory_ref, "capture-inventory")
        require(type(inventory) is dict and set(inventory) == {"format", "files"}
                and inventory["format"] == "recall-dynamic-capture-inventory/v2"
                and type(inventory["files"]) is list and 0 < len(inventory["files"]) <= MAX_FILES,
                "orchestration_capture_inventory")
        paths = set()
        for row in inventory["files"]:
            require(type(row) is dict and set(row) == {"path", "ref"}
                    and row["path"] not in paths, "orchestration_duplicate_capture_path")
            paths.add(row["path"])
            ref = checked_reference(row["ref"], profile_id=self.profile.id)
            require(ref["sha256"] in raw, "orchestration_capture_original_missing")
            self.retain(ref, raw[ref["sha256"]], row["path"])
        # Unknown extra raw blobs must not become a pathless escape around the
        # reviewed inventory or a means of copying token material into output.
        admitted = {inventory_ref["sha256"], *(row["ref"]["sha256"] for row in inventory["files"])}
        # This new derived object references the current inventory, so it must
        # sit outside that inventory to keep the byte-reference graph acyclic.
        current_ref = bundle.get("current_custody")
        if current_ref is not None:
            checked_reference(current_ref, profile_id=self.profile.id)
            require(current_ref["kind"] == "runtime-current-custody", "orchestration_current_custody_kind")
            location = bundle.get("references", {}).get("current_custody")
            require(type(location) is dict and set(location) == {"path", "sha256"}
                    and location["sha256"] == current_ref["sha256"], "orchestration_current_custody_path")
            self.retain(current_ref, raw[current_ref["sha256"]], location["path"])
            admitted.add(current_ref["sha256"])
        require(set(raw) == admitted,
                "orchestration_uninventoried_capture_bytes")
        index_path = bundle.get("references", {}).get("capture_inventory")
        if index_path is not None:
            require(type(index_path) is dict and set(index_path) == {"path", "sha256"}
                    and index_path["sha256"] == inventory_ref["sha256"], "orchestration_original_inventory_path")
            self.retain(inventory_ref, raw[inventory_ref["sha256"]], index_path["path"])

    def original_path(self, ref):
        self.get(ref)
        paths = [path for path, value in self.files.items() if value == ref]
        require(len(paths) == 1, "orchestration_ambiguous_original_path")
        return paths[0]

    def save(self, path, value, kind):
        return self.save_raw(path, encode(value), kind)

    def save_raw(self, path, raw, kind):
        """Persist exact original producer bytes without parsing/re-encoding."""
        require(type(raw) is bytes and len(raw) <= file_bound(self.profile.id), "orchestration_original_raw_bound")
        ref = reference(raw, kind)
        checked_reference(ref, profile_id=self.profile.id)
        require(ref["sha256"] in self.references or (len(self.raw) < MAX_FILES and self.total + len(raw) <= runtime_bound(self.profile.id)),
                "orchestration_original_inventory_bound")
        self.path(str(path))
        if self.profile.id == profiles.SEMANTIC140_ID:
            require(str(path) not in self.files
                    and self.physical_bytes + len(raw) <= runtime_bound(self.profile.id),
                    "orchestration_physical_capture_bound")
        private_new(path, raw, profile_id=self.profile.id)
        return self.retain(ref, raw, str(path))


def verify_helpers(review, root=ROOT):
    hashes = review.get("helper_sha256")
    if review.get("profile_id") == profiles.SEMANTIC140_ID:
        require(type(hashes) is dict and hashes.get(PROFILE_SOURCE) == PROFILE_SHA256
                and EXPECTATIONS_SOURCE in hashes,
                "orchestration_closed_profile_unreviewed")
    require(type(hashes) is dict and {SELF, VALIDATOR, STARTUP} <= set(hashes),
            "orchestration_required_helper_inventory")
    for name, wanted in hashes.items():
        require(type(name) is str and name == str(Path(name)) and not Path(name).is_absolute()
                and ".." not in Path(name).parts and type(wanted) is str
                and re.fullmatch(r"[0-9a-f]{64}", wanted), "orchestration_helper_identity")
        path = root / name
        require(path.resolve().is_relative_to(root.resolve()), "orchestration_helper_escape")
        require(digest(read_original(path, MAX_FILE)) == wanted, "orchestration_helper_changed")


def load_helper(name, relative, review, root=ROOT):
    require(relative in review["helper_sha256"], "orchestration_unreviewed_helper")
    path = root / relative
    raw = read_original(path, MAX_FILE)
    require(digest(raw) == review["helper_sha256"][relative],
            "orchestration_helper_changed_before_import")
    spec = importlib.util.spec_from_file_location(name, path)
    require(spec is not None and spec.loader is not None, "orchestration_helper_unavailable")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    # Execute precisely the verified source snapshot, without a second path
    # read or a timestamp/size-validated bytecode cache substituting its code.
    exec(compile(raw, str(path), "exec", dont_inherit=True), module.__dict__)
    return module


@contextmanager
def reviewed_predicates(review, root=ROOT):
    """Bind L's canonical pure imports to their exact reviewed source files.

    A previously cached module or another sys.path entry cannot supply the
    installed/provider predicate. Restore the original module namespace on
    every normal, refused or cancelled exit; no permanent global replacement.
    """
    missing = object()
    predicates = dict(PREDICATES)
    if review.get("profile_id") == profiles.SEMANTIC140_ID:
        predicates = {"semantic140_expectations": EXPECTATIONS_SOURCE, **predicates}
    previous = {name: sys.modules.get(name, missing) for name in predicates}
    try:
        for name, path in predicates.items():
            module = load_helper(name, path, review, root)
            required = (() if name == "semantic140_expectations" else
                        ("validate_installed", "validate_process_proof") if name == "installed_evidence_validator"
                        else ("validate_provider_accounting",))
            require(all(callable(getattr(module, item, None)) for item in required),
                    "orchestration_reviewed_predicate_api")
        yield
    finally:
        for name, original in previous.items():
            if original is missing:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def current_expiry(review):
    try:
        expiry = dt.datetime.fromisoformat(review["expires_at"].replace("Z", "+00:00"))
        require(expiry.tzinfo is not None and time.time() < expiry.timestamp(), "orchestration_review_expired")
    except (KeyError, TypeError, AttributeError) as error:
        raise ValueError("orchestration_review_expiry") from error


def reviewed_static(files, ref):
    raw = files.get(ref, "runtime-review")
    return {"review": strict_json(raw), "raw_sha256": ref["sha256"], "raw_bytes": raw}


class EvidenceUnion(Mapping):
    """Read one original from bounded runtime/local/G maps on demand.

    G owns its read-only export mount, directory proof, original per-record
    limits and aggregate disk ceiling. The caller supplies that actual resolver;
    this union cannot reopen a private ledger or expand any producer budget.
    L owns reference-kind/size and semantic predicates at each lookup.
    """
    def __init__(self, *sources, profile_id=profiles.LEGACY_ID):
        self.profile = profiles.profile(profile_id)
        require(sources and all(isinstance(source, Mapping) for source in sources),
                "orchestration_original_mapping")
        self.sources = sources
        owners = {}
        for index, source in enumerate(sources):
            require(len(source) <= MAX_FILES, "orchestration_original_mapping_count")
            for key in source:
                require(type(key) is str and re.fullmatch(r"[0-9a-f]{64}", key),
                        "orchestration_original_mapping_key")
                owners.setdefault(key, []).append(index)
                require(len(owners) <= MAX_FILES, "orchestration_original_mapping_count")
        self.owners = owners

    def __iter__(self):
        return iter(self.owners)

    def __len__(self):
        return len(self.owners)

    def __contains__(self, key):
        return key in self.owners

    def __getitem__(self, key):
        if key not in self.owners:
            raise KeyError(key)
        original = None
        for index in self.owners[key]:
            value = self.sources[index][key]
            require(type(value) is bytes and len(value) <= file_bound(self.profile.id) and digest(value) == key,
                    "orchestration_original_mapping_bytes")
            require(original is None or original == value, "orchestration_original_mapping_conflict")
            original = value
        return original


class RuntimeCustody:
    """One current C-owned backend; L validates every admitted boundary."""
    def __init__(self, files, reviewed, validator, live, backend, installed, output):
        self.files, self.reviewed, self.validator, self.live = files, reviewed, validator, live
        self.backend, self.installed, self.output = backend, installed, Path(output)
        self.component = self.envelope = self.context = None
        self.component_reference = self.evidence_reference = None
        self.validated_wall = self.validated_monotonic_ns = None
        self.sequence = 0
        tokens = set(live.token_files.values())
        require(not tokens.intersection(files.files), "orchestration_token_in_evidence")
        files.forbidden_paths.update(tokens)

    def capture(self):
        require(self.component is None, "orchestration_original_runtime_already_validated")
        current_expiry(self.reviewed["review"])
        self.live.revalidate_custody()
        original = self.live.capture_original_custody()
        self.files.import_capture(original)
        context = original["controller_context"]
        # This is an in-process byte wrapper, never a serialized approval flag.
        require(type(context) is dict and type(context.get("raw_bytes")) is bytes
                and context["raw_bytes"] == self.files.get(original["controller"], "gateway-context"),
                "orchestration_original_controller_bytes")
        identity = dict(self.reviewed["review"]["identities"][self.backend])
        identity["outer_configuration_sha256"] = context["context"]["anchor"]["configuration_sha256"]
        envelope = {"format": "recall-dynamic-runtime-evidence/v2", "identity": identity,
                    "installed_offline": self.installed,
                    **{name: original[name] for name in RUNTIME_REFERENCES}}
        component = self.validator.validate_runtime(self.reviewed, envelope, self.files.raw, context)
        validated_wall, validated_ns = time.time(), time.monotonic_ns()
        require(type(component) is dict and component.get("format") == "recall-installed-runtime-component/v2"
                and component.get("verdict") == "passed" and component.get("product_acceptance") is False
                and component.get("identity") == identity, "orchestration_runtime_component_refused")
        self.live.revalidate_custody()
        current_expiry(self.reviewed["review"])
        self.sequence += 1
        self.evidence_reference = self.files.save(
            self.output / f"runtime-{self.sequence:03}.evidence.json", envelope, "runtime-evidence")
        self.component_reference = self.files.save(
            self.output / f"runtime-{self.sequence:03}.component.json", component, "runtime-component")
        self.validated_wall, self.validated_monotonic_ns = validated_wall, validated_ns
        self.component, self.envelope, self.context = component, envelope, context
        return component

    async def current_chain(self, label):
        """Bind a fresh actual C inspection to the unchanged original admission."""
        require(self.component is not None and re.fullmatch(r"[a-z][a-z0-9-]{0,63}", label),
                "orchestration_original_runtime_required")
        current_expiry(self.reviewed["review"])
        await self.live.revalidate_worker_recipe()
        original = self.live.capture_original_custody()
        self.files.import_capture(original)
        current = original["current_custody"]
        # C produces the pair indices and exact original current captures. S
        # does not infer them from the earlier immutable startup receipt.
        self.files.get(current, "runtime-current-custody")
        chain = {"format": "recall-dynamic-runtime-chain/v2", "component": self.component_reference,
                 "evidence": self.evidence_reference, "validated_monotonic_ns": self.validated_monotonic_ns,
                 "validated_wall": self.validated_wall, "current_custody": current}
        result = self.files.save(self.output / (label + ".runtime-chain.json"), chain, "runtime-validation-chain")
        self.live.revalidate_custody()
        return result, original["controller"], original["controller_context"]


@asynccontextmanager
async def current_runtime(files, reviewed, validator, startup, backend, installed, output):
    """Use C's real context manager; refusal still reaches original cleanup."""
    review = reviewed["review"]
    current_expiry(review)
    plan = validator.validate_static(reviewed, files.raw, backend)
    require(type(plan) is dict and plan.get("backend") == backend, "orchestration_static_plan_refused")
    plan_ref = review["bootstrap_plans"][backend]
    require(files.json(plan_ref, "bootstrap-plan") == plan, "orchestration_original_plan_changed")
    files.get(installed, "installed-offline-evidence")
    output = Path(output)
    files.path(str(output))
    require(output.is_absolute() and output == output.resolve() and not output.exists(), "orchestration_new_output")
    output.mkdir(mode=0o700)
    expected = review["controller_policy"]["expected_anchor"]
    original = {"path": files.original_path(plan_ref), "sha256": plan_ref["sha256"]}
    live = None
    try:
        async with startup.reviewed_d28_startup(original, expected) as live:
            custody = RuntimeCustody(files, reviewed, validator, live, backend, installed, output)
            custody.capture()
            yield custody
            # Validate the existing live identity; preparation legitimately
            # changes the public inventory, so do not rerun empty-startup checks.
            live.revalidate_custody()
            current_expiry(review)
    finally:
        # Original C cleanup is immutable and independent of any earlier pass.
        # Missing/incomplete cleanup propagates and prevents the next backend.
        if live is not None:
            cleanup = live.cleanup_reference
            require(type(cleanup) is dict and set(cleanup) == {"path", "sha256"},
                    "orchestration_terminal_cleanup_missing")
            raw = read_original(cleanup["path"], MAX_FILE, private=True)
            require(digest(raw) == cleanup["sha256"], "orchestration_terminal_cleanup_changed")
            files.retain(reference(raw, "runtime-terminal-cleanup"), raw, cleanup["path"])
            terminal = strict_json(raw)
            require(terminal.get("format") == "recall-runtime-bootstrap-cleanup/v1"
                    and terminal.get("backend") == backend and terminal.get("run_nonce") == plan["run_nonce"]
                    and terminal.get("cleanup_complete") is True and terminal.get("status") == "complete"
                    and terminal.get("cleanup_failures") == [] and terminal.get("error_type") is None,
                    "orchestration_terminal_cleanup_incomplete")
