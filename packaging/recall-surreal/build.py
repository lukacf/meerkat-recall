#!/usr/bin/env python3
"""Explicit scheduled build of a verified prepared tree; never called by preparation."""
import argparse
import importlib.util
import platform
import re
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from prepare import digest, file_manifest

MIN_FREE_BYTES = 250 * 1024**3

MAX_GROWTH_BYTES = 150 * 1024**3


def portable_inputs(source: Path, builder_image: str | None) -> dict:
    lock = json.loads((source / ".recall/container-inputs.lock.json").read_text())
    if lock.get("format") != "meerkat-recall-container-inputs/v1" or lock.get("platform") != "linux/amd64":
        raise ValueError("paired builder requires the reviewed Linux amd64 image lock")
    for name in ("builder", "runtime", "frontend"):
        entry = lock[name]
        if not re.fullmatch(r"[a-z0-9./:_-]+@sha256:[a-f0-9]{64}", entry["image"]):
            raise ValueError("paired container input is not an immutable image")
        if entry["image"].split("@", 1)[1] != entry["manifest_digest"]:
            raise ValueError("paired container input digest disagrees")
        if (entry["os"], entry["architecture"]) != ("linux", "amd64"):
            raise ValueError("paired container input has another platform")
    if builder_image is not None and builder_image != lock["builder"]["image"]:
        raise ValueError("paired builder differs from the shared reviewed image")
    return lock


def inspect_binary(source: Path, binary: Path) -> dict:
    inspector = source / ".recall/artifact.py"
    spec = importlib.util.spec_from_file_location("recall_paired_release_inspector", inspector)
    if spec is None or spec.loader is None:
        raise ValueError("reviewed ELF inspector missing")
    module = importlib.util.module_from_spec(spec)
    previous_bytecode = sys.dont_write_bytecode
    try:
        sys.dont_write_bytecode = True
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous_bytecode
    # readelf only: this does not execute or load the produced server.
    return module.inspect_elf(binary)


def check_disk_budget(initial: dict[Path, int], maximum_growth: int) -> None:
    for path, baseline in initial.items():
        free = shutil.disk_usage(path).free
        if free < MIN_FREE_BYTES:
            raise RuntimeError("less than 250 GiB free; stopping owned paired build")
        if baseline - free > maximum_growth:
            raise RuntimeError("assigned incremental disk allocation exceeded; stopping owned paired build")


def check_prepared(source: Path) -> dict:
    receipt = json.loads((source / "RECALL_SOURCE_PROVENANCE.json").read_text())
    actual = file_manifest(source)
    actual.pop("RECALL_SOURCE_PROVENANCE.json", None)
    if actual != receipt["prepared_files"]:
        raise ValueError("prepared source changed after provenance capture")
    return receipt


def build(source: Path, bundle: Path) -> None:
    receipt = check_prepared(source)
    inputs = portable_inputs(source, os.environ.get("RECALL_PAIRED_BUILDER_IMAGE"))
    libc_name, libc_version = platform.libc_ver()
    if platform.machine() != "x86_64" or libc_name != "glibc" or not re.fullmatch(r"\d+\.\d+", libc_version):
        raise ValueError("paired artifact requires an identified x86-64 glibc host")
    if tuple(map(int, libc_version.split("."))) > (2, 36):
        raise ValueError("host glibc exceeds Debian 12; use the pinned portable builder")
    if not shutil.which("readelf"):
        raise ValueError("readelf is required for the actual binary ABI receipt")
    maximum_growth = int(os.environ.get("RECALL_PAIRED_MAX_GROWTH_GIB", "150")) * 1024**3
    if not 0 < maximum_growth <= MAX_GROWTH_BYTES:
        raise ValueError("paired build allocation must be 1..150 GiB within the shared artifact budget")
    if bundle.exists() or bundle.is_symlink():
        raise ValueError("refusing to replace an existing artifact bundle")
    if shutil.disk_usage(source).free < MIN_FREE_BYTES:
        raise ValueError("less than 250 GiB free; heavy build refused")
    jobs = int(os.environ.get("CARGO_BUILD_JOBS", "8"))
    if not 1 <= jobs <= 16:
        raise ValueError("paired server build requires 1..16 centrally scheduled jobs")
    env = os.environ.copy()
    env.update({
        "CARGO_BUILD_JOBS": str(jobs), "CARGO_PROFILE_DEV_DEBUG": "0",
        "CARGO_INCREMENTAL": "0", "BINARYEN_CORES": "8", "TLC_WORKERS": "8",
        "LZMA_API_STATIC": "1", "PYTHONDONTWRITEBYTECODE": "1",
        "SURREAL_BUILD_METADATA": receipt["build_metadata"], "RUSTUP_TOOLCHAIN": receipt["rust_toolchain"],
        "REPO_CARGO_REPO_ROOT": str(source), "REPO_CARGO_WORKSPACE_ROOT": str(source),
        "REPO_CARGO_WORKTREE_ROOT": str(source), "REPO_CARGO_GIT_COMMON_DIR": str(source / ".git"),
    })
    wrapper = source / ".recall/repo-cargo"
    if sys.platform.startswith("linux"):
        prefix = ["chrt", "--idle", "0", "ionice", "-c3"]
    else:
        raise ValueError("no qualified build scheduling recipe for this host")
    values = subprocess.check_output([str(wrapper), "--print-env"], cwd=source, env=env, text=True)
    target = Path(next(line.split("=", 1)[1] for line in values.splitlines() if line.startswith("CARGO_TARGET_DIR=")))
    monitored = (source, target)
    if any(shutil.disk_usage(path).free < MIN_FREE_BYTES for path in monitored):
        raise ValueError("less than 250 GiB free on source/build cache; heavy build refused")
    if env.get("CARGO_BUILD_TARGET"):
        raise ValueError("this recipe builds a native host artifact; cross targets require a separate receipt")
    initial_free = {path: shutil.disk_usage(path).free for path in monitored}
    check_disk_budget(initial_free, maximum_growth)
    compiler = subprocess.check_output(["rustc", "--version", "--verbose"], cwd=source, env=env, text=True)
    if f"release: {receipt['rust_toolchain']}" not in compiler.splitlines():
        raise ValueError("actual compiler differs from the pinned paired toolchain")
    packages = subprocess.check_output(["dpkg-query", "-W", "-f=${binary:Package}\\t${Version}\\n"], text=True)
    command = prefix + [str(wrapper), "build", "--locked", "--release", "-p", "surreal", "--bin", "surreal"]
    process = subprocess.Popen(command, cwd=source, env=env, start_new_session=True)
    try:
        while process.poll() is None:
            check_disk_budget(initial_free, maximum_growth)
            time.sleep(2)
        if process.returncode:
            raise RuntimeError(f"paired server build failed: exit {process.returncode}")
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
    binary = target / "release/surreal"
    if not binary.is_file():
        raise ValueError("scheduled build produced no server binary")
    elf = inspect_binary(source, binary)
    # This check follows codegen too: linker growth must obey the same allocation.
    check_disk_budget(initial_free, maximum_growth)
    bundle.mkdir(parents=True)
    shutil.copy2(binary, bundle / "recall-surreal")
    shutil.copy2(source / "LICENSE", bundle / "LICENSE.upstream")
    shutil.copy2(source / "crates/core/LICENSE", bundle / "LICENSE.core")
    shutil.copy2(source / "crates/sdk/LICENSE", bundle / "LICENSE.sdk")
    shutil.copy2(source / "vendor/surrealkv-0.9.3/LICENSE", bundle / "LICENSE.surrealkv")
    for name in ("surrealkv-0.9.3-backport.patch", "surrealkv-0.9.3-backport.md", "surrealkv-0.9.3-upstream-sha256.json"):
        shutil.copy2(source / "vendor" / name, bundle / name)
    shutil.copy2(source / "RECALL_SOURCE_PROVENANCE.json", bundle / "RECALL_SOURCE_PROVENANCE.json")
    shutil.copy2(source / ".recall/container-inputs.lock.json", bundle / "CONTAINER_INPUTS.json")
    receipt = {
        "status": "built_unqualified", "server_package": "surreal",
        "binary_sha256": digest(bundle / "recall-surreal"),
        "source_provenance_sha256": digest(source / "RECALL_SOURCE_PROVENANCE.json"),
        "prepared_lock_sha256": digest(source / "Cargo.lock"),
        "build_metadata": receipt["build_metadata"], "rust_toolchain": receipt["rust_toolchain"],
        "compiler": compiler, "host": {"system": platform.system(), "machine": platform.machine(), "libc": [libc_name, libc_version]},
        "elf": elf, "builder_packages": sorted(packages.splitlines()),
        "container_inputs": inputs, "actual_builder_image": os.environ.get("RECALL_PAIRED_BUILDER_IMAGE"),
        "elf_inspector_sha256": digest(source / ".recall/artifact.py"),
        "maximum_incremental_bytes": maximum_growth,
        "command": command, "features": "upstream defaults, no default feature removed",
        "qualification": "Version/capability, HTTP/WS/query/import, recovery and actual packaged-host checks pending",
    }
    (bundle / "BUILD_PROVENANCE.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({"status": receipt["status"], "binary_sha256": receipt["binary_sha256"]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    options = parser.parse_args()
    build(options.prepared.resolve(), options.bundle.absolute())
