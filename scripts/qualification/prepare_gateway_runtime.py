#!/usr/bin/env python3
"""Materialize a reviewed isolated runtime; never build, install or launch it."""
from __future__ import annotations

import argparse
import hashlib
import importlib
import importlib.util
import io
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import sys
import tempfile

HERE = Path(__file__).resolve()
# -I removes the script directory: admit only this explicit reviewed sibling path.
sys.path.insert(0, str(HERE.parent))
from runtime_package_io import (
    GIB, Limits, PackagePool, RuntimeTree, SHA256, add_deb,
    canonical_json, checked_file, digest, inspected_process, object_fields, relative, require, tool_text,
)

from runtime_package_capacity import current_budget, validate_current_capacity
sys.path.pop(0)
require(all(Path(sys.modules[name].__file__).resolve() == HERE.parent / (name + ".py")
            for name in ("runtime_package_io", "runtime_package_capacity")),
        "runtime packaging dependency came from another checkout")
PACKAGE_PREFIXES = ("usr/bin/", "usr/lib/", "lib/", "lib64/", "etc/ssl/certs/",
                    "usr/share/zoneinfo/")
HARNESS_PREFIXES = ("scripts/qualification/", "tests/recall_preview/",
                    "tests/recall_preview_semantics/")
ENVIRONMENT = {"RUST_LOG", "SURREALDB_URL", "SURREALDB_NAMESPACE", "SURREALDB_DATABASE",
               "ELEPHANT_HOST", "ELEPHANT_PORT", "RECALL_BACKEND", "JWT_ISSUER", "JWT_AUDIENCE",
               "PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS", "PIPELINE_SKILLS_DIR"}
TARGET = "x86_64-unknown-linux-gnu"
# Debian's Python 3.11 minimal build links these four modules into the
# interpreter. The two OpenSSL modules remain loadable extensions. The exact
# official source recipe and its signed-metadata custody are documented in
# runtime_package_README.md; this is an explicit contract, not a missing-.so
# fallback. Actual packaged symbols and extension ELF files must still exist.
PYTHON_BUILTIN_MODULES = ("_socket", "select", "_random", "_struct")
PYTHON_DYNAMIC_MODULES = ("_ssl", "_hashlib")

DIAGNOSTIC_STAGE = {"stage": "arguments", "item_index": None}


def diagnostic_stage(stage, item_index=None):
    # These are source-owned labels/indices, never input paths or payload text.
    require(re.fullmatch(r"[a-z_]{1,48}", stage) is not None
            and (item_index is None or type(item_index) is int and 0 <= item_index <= 100000),
            "invalid internal packaging diagnostic stage")
    DIAGNOSTIC_STAGE.update(stage=stage, item_index=item_index)


def failure_diagnostic(error, output):
    frames = []
    trace = error.__traceback__
    while trace is not None:
        code = trace.tb_frame.f_code
        path = Path(code.co_filename)
        if path.is_absolute() and path.is_relative_to(HERE.parents[2]):
            frames.append({"file": path.relative_to(HERE.parents[2]).as_posix(),
                           "function": code.co_name[:96], "line": trace.tb_lineno})
        trace = trace.tb_next
    name = type(error).__name__
    record = {"format": "recall-runtime-package-failure/v1", "state": "failed",
              **DIAGNOSTIC_STAGE, "exception_type": name if re.fullmatch(r"[A-Za-z_]{1,64}", name) else "Error",
              "source_frames": frames[-16:], "exception_message_recorded": False,
              "locals_or_input_payloads_recorded": False, "runtime_acceptance": False}
    raw = canonical_json(record) + b"\n"
    require(len(raw) <= 16384, "failure diagnostic exceeded bound")
    target = output.with_name(output.name + ".failure.json")
    require(target.is_absolute() and target.parent.resolve() == target.parent,
            "failure diagnostic requires a canonical output parent")
    descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(target.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return {"path": str(target), "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


def read_json(path, maximum=4 * 1024**2, expected=None):
    require(path.stat().st_size <= maximum, "JSON input size limit")
    with path.open("rb") as stream:
        raw = stream.read(maximum + 1)
    require(len(raw) <= maximum and (expected is None or hashlib.sha256(raw).hexdigest() == expected),
            "JSON input size or identity differs")
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result
    def number(value):
        result = float(value)
        require(math.isfinite(result), "nonfinite JSON number")
        return result
    return json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_float=number,
                      parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))


def source_identity(root, limits):
    """Same committed tar identity as release artifacts, streamed under bounds."""
    def git(*args):
        return tool_text(["/usr/bin/git", "-C", str(root), *args], limits).strip()
    require(not git("status", "--porcelain", "--untracked-files=normal"),
            "reviewed source checkout must be clean")
    commit = git("rev-parse", "HEAD")
    checksum = hashlib.sha256()
    total = 0
    with inspected_process(["/usr/bin/git", "-C", str(root), "archive", "--format=tar", "HEAD"], limits) as process:
        for block in iter(lambda: process.stdout.read(1024 * 1024), b""):
            total += len(block)
            require(total <= 2 * GIB, "source archive provenance size limit")
            limits.check()
            checksum.update(block)
    require(git("rev-parse", "HEAD") == commit
            and not git("status", "--porcelain", "--untracked-files=normal"),
            "source changed while recording provenance")
    return {"source_commit": commit,
            "source_tree": git("rev-parse", "HEAD^{tree}"),
            "source_tar_sha256": checksum.hexdigest(),
            "cargo_lock_sha256": digest(root / "Cargo.lock")}


def helpers(root):
    release = root / "scripts/release"
    sys.path.insert(0, str(release))
    try:
        module = importlib.import_module("container_artifact")
    finally:
        sys.path.pop(0)
    require(Path(module.__file__).resolve() == release / "container_artifact.py"
            and Path(module.artifact.__file__).resolve() == release / "artifact.py",
            "release helpers came from another checkout")
    gateway = root / "tests/recall_preview_semantics/gateway"
    source = gateway / "launcher.py"
    spec = importlib.util.spec_from_file_location("_runtime_package_gateway", source)
    launcher = importlib.util.module_from_spec(spec)
    sys.path.insert(0, str(gateway))
    try:
        spec.loader.exec_module(launcher)
    finally:
        sys.path.pop(0)
    # Gateway modules imported above must come from this reviewed source tree.
    for name in ("capture", "ledger", "policy", "server", "transport", "wire", "launch_context"):
        require(Path(sys.modules[name].__file__).resolve().parent == gateway,
                "gateway dependency came from another checkout")
    return module, launcher


def bundle_root(value):
    path = Path(value)
    require(path.is_absolute() and path.resolve() == path and path.is_dir()
            and not path.is_symlink(), "bundle root must be a canonical directory")
    return path


def actual_native_elf(path, artifact, limits):
    texts = [tool_text(["/usr/bin/readelf", "--wide", option, str(path)], limits)
             for option in ("--file-header", "--dynamic", "--version-info")]
    return {**artifact.elf_dependencies(*texts), "binary_sha256": digest(path)}


def copy_bundle(root, output, prefix, binary_name, binary_target, limits, expected,
                permitted_alias=None, selected=None):
    seen = set()
    for path in sorted(root.rglob("*")):
        limits.check()
        name = path.relative_to(root).as_posix()
        if selected is not None and name not in selected:
            continue
        if path.is_dir() and not path.is_symlink():
            continue
        require(name in expected, "artifact acquired an unreviewed file")
        identity = expected[name]
        seen.add(name)
        if path.is_symlink():
            require(name == permitted_alias and os.readlink(path) == binary_name
                    and identity == {"symlink": binary_name}, "unsupported artifact symlink")
            continue
        require(path.is_file() and "sha256" in identity and "bytes" in identity,
                "artifact entry differs from reviewed type")
        info = path.stat()
        require(not info.st_mode & 0o7022 and info.st_size == identity["bytes"],
                "unsafe artifact file mode or changed length")
        destination = binary_target if name == binary_name else prefix + "/" + name
        # Never re-baseline the expected digest after validation. Copying must
        # match the exact frozen manifest identity even if input bytes change.
        with path.open("rb") as stream:
            output.file(destination, stream, identity["bytes"], identity["sha256"],
                        name == binary_name, {"artifact_path": name})
    require(seen == set(expected), "artifact lost a reviewed file")


def original_budget_receipt(value):
    path = Path(value)
    require(path.is_absolute() and path.resolve() == path and not path.is_symlink(),
            "original shared artifact budget receipt must be canonical")
    require(path.is_file() and not path.stat().st_mode & 0o7022,
            "original shared artifact budget receipt must already exist as a regular private file")
    return path


def verify_source_unchanged(root, source, limits):
    head = tool_text(["/usr/bin/git", "-C", str(root), "rev-parse", "HEAD"], limits).strip()
    status = tool_text(["/usr/bin/git", "-C", str(root), "status", "--porcelain",
                        "--untracked-files=normal"], limits).strip()
    require(head == source["source_commit"] and not status
            and digest(root / "Cargo.lock") == source["cargo_lock_sha256"],
            "reviewed source changed before runtime publication")


def retained_paired_source(inputs, paired, source, limits):
    """Pin an original paired artifact without relabeling its build provenance.

    A separately declared older origin is usable only when its complete patched
    storage trees and pinned upstream recipe equal the current Recall sources.
    The existing binary, build provenance, ELF and builder checks still apply.
    """
    origin = paired.get("source_commit", inputs["artifact_source_commit"])
    require(type(origin) is str and re.fullmatch(r"[a-f0-9]{40}", origin)
            and source.get("recall_commit") == origin, "paired source/build identity differs")
    if origin == inputs["artifact_source_commit"]:
        return origin
    root = inputs["_root"]
    lock_path = root / "packaging/recall-surreal/source.lock.json"
    lock = read_json(lock_path, 256 * 1024)
    require(source.get("source_lock_sha256") == digest(lock_path)
            and source.get("upstream_commit") == lock["upstream_commit"]
            and source.get("archive_sha256") == lock["archive_sha256"]
            and source.get("rust_toolchain") == lock["rust_toolchain"]
            and source.get("capability_version") == lock["reference_token_version"]
            and source.get("server_package") == lock["server_package"],
            "retained paired upstream recipe differs from current Recall")
    paths = {**lock["vendor_paths"], "surrealkv": lock["surrealkv"]["path"]}
    recorded = source.get("vendor_files")
    require(type(recorded) is dict and set(recorded) == set(paths),
            "retained paired storage source inventory differs")
    for name, value in sorted(paths.items()):
        base = root / relative(value)
        require(base.resolve() == base and base.is_dir() and not base.is_symlink(),
                "retained paired storage root is not canonical")
        actual = {}
        for path in sorted(base.rglob("*")):
            limits.check()
            require(not path.is_symlink(), "retained paired storage source contains symlink")
            if path.is_dir():
                continue
            require(path.is_file() and len(actual) < 8192,
                    "retained paired storage source type or count differs")
            actual[path.relative_to(base).as_posix()] = digest(path)
        require(actual == recorded[name], "retained paired storage bytes differ from current Recall")
    return origin


def install_artifacts(inputs, output, release, limits):
    diagnostic_stage("native_manifest")
    native = object_fields(inputs["recall_bundle"], ("root", "provenance_sha256"))
    root = bundle_root(native["root"])
    require(SHA256.fullmatch(native["provenance_sha256"] or "") is not None
            and digest(root / "provenance.json") == native["provenance_sha256"],
            "Recall provenance differs from reviewed input")
    manifest = read_json(root / "provenance.json", expected=native["provenance_sha256"])
    artifact = release.artifact
    artifact.validate_manifest(manifest)
    require(manifest["source_commit"] == inputs["artifact_source_commit"]
            and manifest["target"] == TARGET, "Recall source or target differs")
    diagnostic_stage("native_inventory")
    inventory = artifact.inventory(root)
    inventory.pop("provenance.json", None)
    require(inventory == manifest["files"], "Recall bundle inventory differs")
    require(manifest["build"]["native_reference_capability"]
            == artifact.reference_source_receipt(root / "third-party"),
            "Recall bundled reference capability differs")
    diagnostic_stage("native_elf")
    require(actual_native_elf(root / "meerkat-recall", artifact, limits)
            == manifest["build"]["runtime_dependencies"]["elf"],
            "Recall actual ELF differs from build receipt")
    diagnostic_stage("native_copy")
    copy_bundle(root, output, "opt/recall/share/native", "meerkat-recall",
                "opt/recall/bin/elephant", limits,
                {**manifest["files"], "provenance.json": {"sha256": native["provenance_sha256"],
                    "bytes": (root / "provenance.json").stat().st_size}}, permitted_alias="elephant")

    diagnostic_stage("paired_provenance")
    paired = object_fields(inputs["paired_bundle"],
                           ("root", "build_provenance_sha256", "source_provenance_sha256"),
                           ("source_commit",))
    root = bundle_root(paired["root"])
    required = {"LICENSE.upstream", "LICENSE.core", "LICENSE.sdk", "LICENSE.surrealkv",
                "surrealkv-0.9.3-backport.patch", "surrealkv-0.9.3-backport.md",
                "surrealkv-0.9.3-upstream-sha256.json"}
    require(all((root / name).is_file() for name in required), "paired component custody is incomplete")
    selected = required | {"recall-surreal", "BUILD_PROVENANCE.json",
                           "RECALL_SOURCE_PROVENANCE.json", "CONTAINER_INPUTS.json"}
    expected = {name: {"sha256": digest(root / name), "bytes": (root / name).stat().st_size}
                for name in selected}
    for key, filename in (("build_provenance_sha256", "BUILD_PROVENANCE.json"),
                          ("source_provenance_sha256", "RECALL_SOURCE_PROVENANCE.json")):
        require(SHA256.fullmatch(paired[key] or "") is not None
                and digest(root / filename) == paired[key], "paired provenance differs")
    build = read_json(root / "BUILD_PROVENANCE.json", expected=paired["build_provenance_sha256"])
    source = read_json(root / "RECALL_SOURCE_PROVENANCE.json", expected=paired["source_provenance_sha256"])
    require(build["status"] == "built_unqualified"
            and build["source_provenance_sha256"] == paired["source_provenance_sha256"],
            "paired source/build identity differs")
    diagnostic_stage("paired_sources")
    retained_paired_source(inputs, paired, source, limits)
    diagnostic_stage("paired_builder_inputs")
    lock = release.load_inputs(inputs["_root"])
    require(build["container_inputs"] == lock
            and read_json(root / "CONTAINER_INPUTS.json",
                          expected=expected["CONTAINER_INPUTS.json"]["sha256"]) == lock
            and build["actual_builder_image"] == lock["builder"]["image"],
            "paired runtime must use the reviewed Bookworm builder inputs")
    diagnostic_stage("paired_elf")
    require(build["binary_sha256"] == digest(root / "recall-surreal")
            and actual_native_elf(root / "recall-surreal", artifact, limits) == build["elf"],
            "paired actual executable differs from build receipt")
    expected["recall-surreal"]["sha256"] = build["binary_sha256"]
    expected["BUILD_PROVENANCE.json"]["sha256"] = paired["build_provenance_sha256"]
    expected["RECALL_SOURCE_PROVENANCE.json"]["sha256"] = paired["source_provenance_sha256"]
    diagnostic_stage("paired_copy")
    copy_bundle(root, output, "opt/recall/share/paired", "recall-surreal",
                "opt/recall/bin/recall-surreal", limits, expected, selected=selected)
    require(output.inventory["opt/recall/bin/elephant"]["sha256"] == manifest["binary_sha256"]
            and output.inventory["opt/recall/bin/recall-surreal"]["sha256"] == build["binary_sha256"],
            "copied runtime binaries differ from the declared build identities")
    return {"recall": manifest["binary_sha256"], "paired": build["binary_sha256"]}


def inspect_dependencies(output, limits):
    """Inspect every packaged ELF without executing it or consulting host libraries."""
    paths = set(output.inventory)
    records = {}
    for diagnostic_index, name in enumerate(sorted(paths)):
        diagnostic_stage("runtime_elf_dependencies", diagnostic_index)
        path = output.root / name
        with path.open("rb") as stream:
            if stream.read(4) != b"\x7fELF":
                continue
        header = tool_text(["/usr/bin/readelf", "--wide", "--file-header", str(path)], limits)
        require(re.search(r"Class:\s+ELF64\b", header)
                and re.search(r"Machine:\s+Advanced Micro Devices X86-64\b", header),
                "runtime contains a non-amd64 ELF")
        dynamic = tool_text(["/usr/bin/readelf", "--wide", "--dynamic", str(path)], limits)
        program = tool_text(["/usr/bin/readelf", "--wide", "--program-headers", str(path)], limits)
        versions = tool_text(["/usr/bin/readelf", "--wide", "--version-info", str(path)], limits)
        require(all(tuple(map(int, version.split("."))) <= (2, 36)
                    for version in re.findall(r"\bGLIBC_(\d+\.\d+)\b", versions)),
                "runtime ELF exceeds the reviewed Bookworm glibc baseline")
        needed = re.findall(r"\(NEEDED\).*Shared library: \[([^\]]+)\]", dynamic)
        interpreter = re.findall(r"Requesting program interpreter:\s*([^\]]+)\]", program)
        # Keep linker search deterministic. A separate reviewed extension can
        # support RPATH; do not guess or borrow an ambient loader search path.
        require(not re.search(r"\((?:RPATH|RUNPATH)\)", dynamic), "runtime ELF has unsupported linker search paths")
        for loader in interpreter:
            require(loader.startswith("/") and loader[1:] in paths, "packaged ELF loader is missing")
        resolved = {}
        for library in needed:
            require("/" not in library, "ELF dependency has a non-library path")
            choices = [directory + "/" + library for directory in
                       ("lib/x86_64-linux-gnu", "usr/lib/x86_64-linux-gnu", "lib64", "usr/lib64", "lib", "usr/lib")]
            found = [choice for choice in choices if choice in paths]
            require(found, "ELF dependency is missing from the packaged runtime")
            require(len({output.inventory[choice]["sha256"] for choice in found}) == 1,
                    "ambiguous packaged ELF dependency")
            resolved[library] = found[0]
        records[name] = {"sha256": output.inventory[name]["sha256"],
                         "interpreter": interpreter, "needed": resolved}
    for name in ("usr/bin/python3", "opt/recall/bin/elephant", "opt/recall/bin/recall-surreal"):
        require(name in records, "required runtime executable is not an inspected ELF")
    for name in ("usr/lib/python3.11/encodings/__init__.py", "usr/lib/python3.11/ssl.py",
                 "usr/lib/python3.11/json/__init__.py", "usr/lib/python3.11/urllib/request.py"):
        require(name in paths, "required Python 3.11 standard library is missing")
    extensions = {}
    for module in PYTHON_DYNAMIC_MODULES:
        name = "usr/lib/python3.11/lib-dynload/" + module + ".cpython-311-x86_64-linux-gnu.so"
        require(name in records, "required Python extension module is not an inspected ELF")
        extensions[module] = name
    diagnostic_stage("python_builtin_symbols")
    symbols = tool_text(["/usr/bin/readelf", "--wide", "--dyn-syms",
                         str(output.root / "usr/bin/python3")], limits)
    # Only a defined global function in the supplied interpreter is evidence.
    # An undefined import, symbol mention, object or other module's .so cannot
    # satisfy a built-in module. No candidate interpreter is executed here.
    defined = set(re.findall(
        r"^\s*\d+:\s+[0-9a-fA-F]+\s+\d+\s+FUNC\s+GLOBAL\s+DEFAULT\s+[1-9]\d*\s+(PyInit_\w+)\s*$",
        symbols, re.M))
    builtins = {module: "PyInit_" + module for module in PYTHON_BUILTIN_MODULES}
    require(set(builtins.values()) <= defined,
            "required Python built-in module has no defined interpreter initialization function")
    records["usr/bin/python3"]["python_modules"] = {
        "builtin_initializers": builtins, "dynamic_extensions": extensions}
    return records


def validate_launch(spec, inventory):
    object_fields(spec, ("format", "command", "environment", "bindings"), ("inputs",))
    require(spec["format"] == "recall-gateway-launch/v1", "unsupported gateway launch specification")
    if "inputs" in spec:
        inputs = object_fields(spec["inputs"], ("directory", "inventory_sha256"))
        directory = Path(inputs["directory"])
        require(directory.is_absolute() and directory.resolve() == directory and directory.is_dir()
                and not directory.is_symlink() and SHA256.fullmatch(inputs["inventory_sha256"] or ""),
                "launch inputs require an original canonical directory and exact inventory hash")
    command = spec["command"]
    require(type(command) is list and 3 <= len(command) <= 64
            and command[:2] == ["/usr/bin/python3", "-I"], "driver must use isolated Python")
    require(all(type(arg) is str and arg and len(arg) <= 4096 and "\0" not in arg for arg in command),
            "invalid launch argument")
    require(command[2].startswith("/opt/recall/scripts/qualification/")
            and command[2][1:] in inventory, "reviewed primary driver is not packaged")
    relative(command[2][1:])
    environment = object_fields(spec["environment"], (), ENVIRONMENT)
    require(all(type(value) is str and len(value) <= 1024 and "\0" not in value
                for value in environment.values()), "invalid launch environment")
    if "PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS" in environment:
        require(environment["PIPELINE_WORK_TRANSIENT_RETRY_MAX_ATTEMPTS"] == "3",
                "unexpected cohort retry policy")
    if "PIPELINE_SKILLS_DIR" in environment:
        directory = environment["PIPELINE_SKILLS_DIR"]
        require(directory.startswith("/opt/recall/"), "skills directory must be in the reviewed runtime")
        relative(directory[1:])
        require(any(name.startswith(directory[1:] + "/") for name in inventory),
                "reviewed skills directory is not packaged")
    require(type(spec["bindings"]) is list, "gateway bindings must be an explicit reviewed list")


def clean_tree(path):
    if path.exists():
        for entry in path.rglob("*"):
            if entry.is_dir():
                os.chmod(entry, 0o700)
        os.chmod(path, 0o700)
        shutil.rmtree(path)


def mount_targets(output):
    for name in ("proc", "dev", "tmp", "home", "work", "inputs"):
        output.directory(name)
    for name in ("bridge.py", "launch_context.py", "launch-anchor.json", "gateway.sock",
                 "gateway-manifest.json", "gateway-launch.json"):
        output.file(name, io.BytesIO(b""), 0, hashlib.sha256(b"").hexdigest(), False,
                    {"generated": "empty read-only gateway mount target"})


def prepare(root, manifest_path, destination, *, seconds=300, capacity_binding=None):
    require(root.resolve() == root and HERE == root / "scripts/qualification/prepare_gateway_runtime.py",
            "run the packager from the reviewed checkout")
    require(destination.is_absolute() and destination.parent.resolve() == destination.parent
            and not destination.exists() and not destination.is_symlink(),
            "runtime destination must be a new canonical absolute directory")
    require(not destination.is_relative_to(root), "place runtime outputs outside the checkout")
    require(manifest_path.is_absolute() and not manifest_path.is_symlink(), "explicit manifest required")
    require(manifest_path.resolve() == manifest_path and manifest_path.stat().st_size <= 256 * 1024,
            "runtime input manifest is noncanonical or oversized")
    diagnostic_stage("runtime_inputs")
    manifest_sha256 = digest(manifest_path)
    inputs = read_json(manifest_path, 256 * 1024, expected=manifest_sha256)
    object_fields(inputs, ("format", "artifact_source_commit", "recall_bundle", "paired_bundle",
                          "packages", "package_paths", "harness_files"))
    require(inputs["format"] == "recall-gateway-runtime-inputs/v2"
            and re.fullmatch(r"[a-f0-9]{40}", inputs["artifact_source_commit"] or ""),
            "unsupported runtime input identity")
    require(type(inputs["packages"]) is list and 1 <= len(inputs["packages"]) <= 32,
            "package count limit")
    require(type(inputs["package_paths"]) is list and 1 <= len(inputs["package_paths"]) <= 64,
            "package path count limit")
    require(type(inputs["harness_files"]) is list and 1 <= len(inputs["harness_files"]) <= 256,
            "harness file count limit")
    require(any(item["package"] == "python3.11-minimal" and
                item["version"].split(":")[-1].startswith("3.11.") for item in inputs["packages"]),
            "explicit Bookworm Python 3.11 package is required")
    diagnostic_stage("load_helpers")
    release, gateway = helpers(root)
    inputs["_root"] = root
    budget_file = original_budget_receipt(os.environ.get("RECALL_ARTIFACT_BUDGET_FILE", ""))
    destination.parent.mkdir(parents=True, exist_ok=True)
    # Match the original allocation's complete filesystem set, including Docker
    # without contacting its daemon. The scheduler supplies that exact path.
    monitored = [root, destination.parent, Path(tempfile.gettempdir())]
    if capacity_binding is None:
        docker_root = Path(os.environ.get("RECALL_ARTIFACT_DOCKER_ROOT", ""))
        require(docker_root.is_absolute() and docker_root.is_dir(),
                "original Docker storage filesystem path required")
        monitored.append(docker_root)
    monitored.extend(Path(item["file"]["path"]).parent for item in inputs["packages"])
    monitored.extend(bundle_root(inputs[key]["root"]) for key in ("recall_bundle", "paired_bundle"))
    diagnostic_stage("actual_capacity")
    budget = (current_budget(capacity_binding, destination, monitored, budget_file)
              if capacity_binding is not None else
              release.ResourceBudget(budget_file, tuple(monitored), allocation=release.build_limits()[2]))
    limits = Limits(seconds, budget)
    diagnostic_stage("helper_source_identity")
    source = source_identity(root, limits)
    receipt_path = destination.with_name(destination.name + ".receipt.json")
    require(not receipt_path.exists() and not receipt_path.is_symlink(), "runtime receipt already exists")
    published = False
    with tempfile.TemporaryDirectory(prefix=".recall-runtime-package-", dir=destination.parent) as temporary:
        scratch = Path(temporary)
        private_inputs = scratch / "inputs"
        private_inputs.mkdir(mode=0o700)
        pool = PackagePool(scratch / "pool", limits)
        # A sealed directory cannot change its parent without write permission
        # on that directory. Keep the output beside the final destination so
        # publication preserves the 0555 seal through a same-parent rename.
        # RuntimeTree.mkdir is exclusive; the existing finally owns cleanup.
        output = RuntimeTree(scratch.with_name(scratch.name + "-runtime"), limits)
        try:
            packages = []
            for diagnostic_index, item in enumerate(inputs["packages"]):
                diagnostic_stage("debian_package", diagnostic_index)
                packages.append(add_deb(item, pool, private_inputs, limits))
            require(len({item["package"] for item in packages}) == len(packages),
                    "duplicate Debian package identities")
            for diagnostic_index, name in enumerate(inputs["package_paths"]):
                diagnostic_stage("selected_package_path", diagnostic_index)
                relative(name)
                require((name + "/").startswith(PACKAGE_PREFIXES), "package destination is outside runtime libraries")
                pool.materialize(name, name, output)
            for diagnostic_index, package in enumerate(packages):
                diagnostic_stage("package_license", diagnostic_index)
                name = "usr/share/doc/" + package["package"] + "/copyright"
                pool.materialize(name, name, output)
            binaries = install_artifacts(inputs, output, release, limits)
            for diagnostic_index, item in enumerate(inputs["harness_files"]):
                diagnostic_stage("harness_input", diagnostic_index)
                object_fields(item, ("repository_path", "sha256", "bytes"))
                name = relative(item["repository_path"])
                require((name.startswith(HARNESS_PREFIXES) or name == "scripts/release/artifact.py"), "harness file is outside reviewed source paths")
                path = checked_file({"path": str(root / name), "sha256": item["sha256"], "bytes": item["bytes"]})
                with path.open("rb") as stream:
                    output.file("opt/recall/" + name, stream, item["bytes"], item["sha256"], False,
                                {"repository_path": name, "source_commit": source["source_commit"]})
            dependencies = inspect_dependencies(output, limits)
            diagnostic_stage("runtime_mount_targets")
            mount_targets(output)
            provenance = {"format": "recall-gateway-runtime-provenance/v1",
                          "status": "packaged_unqualified",
                          "packager_source": source,
                          "artifact_source_commit": inputs["artifact_source_commit"],
                          "paired_source_commit": inputs["paired_bundle"].get(
                              "source_commit", inputs["artifact_source_commit"]),
                          "binaries": binaries, "packages": packages,
                          "files": output.inventory.copy(), "elf_dependencies": dependencies}
            raw = canonical_json(provenance) + b"\n"
            output.file("opt/recall/share/runtime-package.json", io.BytesIO(raw), len(raw),
                        hashlib.sha256(raw).hexdigest(), False, {"generated": "packaging provenance"})
            diagnostic_stage("runtime_seal")
            output.seal()
            # Use the actual gateway's authority, not a second tree-hash format.
            diagnostic_stage("runtime_tree_identity")
            runtime_hash = gateway.tree_hash(output.root)
            gateway_directory = root / "tests/recall_preview_semantics/gateway"
            gateway_hash = gateway.digest(gateway.encode(
                [[path.name, digest(path)] for path in sorted(gateway_directory.glob("*.py"))]))
            receipt = {"format": "recall-gateway-runtime-package/v1", "status": "packaged_unqualified",
                       "runtime_sha256": runtime_hash, "gateway_sha256": gateway_hash,
                       "artifact_sha256": binaries["recall"], "paired_artifact_sha256": binaries["paired"],
                       "input_manifest_sha256": manifest_sha256, "runtime_path": str(destination),
                       "bytes": limits.output_bytes,
                       "source": source, "resource_budget_file": str(budget.receipt),
                       "resource_budget": budget.check(),
                       **({"resource_budget_binding": capacity_binding}
                          if capacity_binding is not None else {}),
                       "qualification": "No candidate executable, Python runtime, service, gateway or provider was executed"}
            limits.check(force=True)
            require(digest(manifest_path) == manifest_sha256, "input manifest changed during packaging")
            diagnostic_stage("final_source_identity")
            verify_source_unchanged(root, source, limits)
            diagnostic_stage("publish_runtime")
            os.rename(output.root, destination)
            published = True
            with receipt_path.open("x", encoding="utf-8") as stream:
                json.dump(receipt, stream, indent=2, sort_keys=True)
                stream.write("\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(receipt_path, 0o600)
            directory = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            return receipt
        finally:
            # Only the private staging tree is removed. Once published, never
            # silently delete a runtime if its outer receipt write was uncertain.
            if not published:
                clean_tree(output.root)


def validate_packaged_launch(root, runtime, specification, output, *, capacity_binding=None):
    """A later static phase, after runtime -> plans/review -> inputs -> launch.

    This reads an existing sealed runtime and writes only a small new receipt.
    It does not allocate another package tree, execute code from the runtime,
    manufacture gateway approval, or change the original runtime receipt.
    """
    require(root.resolve() == root and HERE == root / "scripts/qualification/prepare_gateway_runtime.py",
            "run static launch validation from the reviewed checkout")
    runtime = bundle_root(str(runtime))
    require(output.is_absolute() and output.parent.resolve() == output.parent
            and not output.exists() and not output.is_symlink() and not output.is_relative_to(runtime),
            "launch receipt must be a new canonical external path")
    # Static validation consumes the original allocation without creating an
    # observer, resetting counters or lowering a scheduler-retained floor.
    budget_path = original_budget_receipt(os.environ.get("RECALL_ARTIFACT_BUDGET_FILE", ""))
    receipt_path = runtime.with_name(runtime.name + ".receipt.json")
    receipt = read_json(receipt_path)
    capacity_observation = None
    if capacity_binding is not None:
        capacity_observation = validate_current_capacity(
            capacity_binding, receipt, (root, runtime, output.parent), budget_path)
    else:
        require("resource_budget_binding" not in receipt, "current capacity binding required")
        budget = read_json(budget_path, maximum=65536)
        require(type(budget) is dict and budget.get("format") == "meerkat-recall-artifact-allocation/v1",
                "original allocation identity differs")
        for field in ("checks", "consumed_bytes", "minimum_free_bytes", "limit_bytes"):
            require(type(budget.get(field)) is int and budget[field] >= 0,
                    "original allocation integer invalid")
        require(budget["minimum_free_bytes"] >= 250 * GIB
                and budget["consumed_bytes"] < budget["limit_bytes"]
                and budget.get("refused") is False, "original allocation refused or limits invalid")
        filesystems = budget.get("filesystems")
        require(type(filesystems) is dict and filesystems, "original allocation filesystem identity missing")
        minimum_free_bytes = max(250 * GIB, budget["minimum_free_bytes"])
        for path in (root, runtime, output.parent):
            sample = filesystems.get(str(path.stat().st_dev))
            require(type(sample) is dict and type(sample.get("total")) is int and sample["total"] > 0,
                    "original allocation filesystem identity differs")
            usage = shutil.disk_usage(path)
            require(usage.total == sample["total"], "original allocation filesystem size changed")
            require(usage.free >= minimum_free_bytes, "original retained free-space floor not met")
    _, gateway = helpers(root)
    receipt_path = runtime.with_name(runtime.name + ".receipt.json")
    receipt = read_json(receipt_path)
    require(receipt["format"] == "recall-gateway-runtime-package/v1"
            and receipt["status"] == "packaged_unqualified" and receipt["runtime_path"] == str(runtime)
            and gateway.tree_hash(runtime) == receipt["runtime_sha256"], "original sealed runtime differs")
    provenance = read_json(runtime / "opt/recall/share/runtime-package.json", maximum=32 * 1024**2)
    checksum = digest(specification)
    launch = read_json(specification, maximum=256 * 1024, expected=checksum)
    validate_launch(launch, provenance["files"])
    source = root / "tests/recall_preview_semantics/gateway"
    gateway_hash = gateway.digest(gateway.encode([[path.name, digest(path)] for path in sorted(source.glob("*.py"))]))
    require(gateway_hash == receipt["gateway_sha256"], "reviewed gateway source changed after packaging")
    result = {"format": "recall-gateway-packaged-launch/v1", "status": "static_checked_unqualified",
              "runtime_sha256": receipt["runtime_sha256"], "runtime_receipt_sha256": digest(receipt_path),
              "configuration_sha256": checksum, "command": launch["command"], "gateway_sha256": gateway_hash,
              **({"current_capacity": capacity_observation} if capacity_observation is not None else {}),
              "qualification": "Static command and runtime identities only; actual gateway validates input bytes and approval"}
    descriptor = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(canonical_json(result) + b"\n")
        stream.flush()
        os.fsync(stream.fileno())
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recall-root", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--inputs", type=Path)
    mode.add_argument("--validate-launch", type=Path)
    parser.add_argument("--runtime", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--deadline-seconds", type=int, default=300)
    parser.add_argument("--capacity-binding", type=Path)
    parser.add_argument("--capacity-binding-sha256")
    args = parser.parse_args()
    def deadline(_signal, _frame):
        raise TimeoutError("runtime packaging deadline")
    try:
        require(sys.platform.startswith("linux") and 1 <= args.deadline_seconds <= 600,
                "Linux packaging and a 1..600 second deadline are required")
        require((args.capacity_binding is None) == (args.capacity_binding_sha256 is None),
                "both current capacity binding arguments are required")
        capacity_binding = None
        if args.capacity_binding is not None:
            capacity_binding = {"path": str(args.capacity_binding),
                                "sha256": args.capacity_binding_sha256,
                                "bytes": args.capacity_binding.stat().st_size}
        signal.signal(signal.SIGALRM, deadline)
        signal.alarm(args.deadline_seconds)
        if args.validate_launch is not None:
            require(args.runtime is not None, "existing runtime required for later launch validation")
            result = validate_packaged_launch(args.recall_root.resolve(), args.runtime.absolute(),
                                              args.validate_launch.absolute(), args.output.absolute(),
                                              capacity_binding=capacity_binding)
        else:
            require(args.runtime is None, "runtime path belongs only to launch validation")
            result = prepare(args.recall_root.resolve(), args.inputs.absolute(), args.output.absolute(),
                             seconds=args.deadline_seconds, capacity_binding=capacity_binding)
        print(json.dumps(result, sort_keys=True))
        return 0
    except Exception as error:
        # No exception message, traceback source text, locals, argv or payloads.
        try:
            diagnostic = failure_diagnostic(error, args.output.absolute())
            sys.stderr.write(json.dumps({"runtime_packaging_failed": True, "diagnostic": diagnostic}) + "\n")
        except Exception:
            sys.stderr.write(json.dumps({"runtime_packaging_failed": True,
                                         "stage": DIAGNOSTIC_STAGE["stage"],
                                         "diagnostic_not_written": True}) + "\n")
        return 78
    finally:
        signal.alarm(0)


if __name__ == "__main__":
    sys.exit(main())
