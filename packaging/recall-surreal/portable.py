#!/usr/bin/env python3
"""Centrally scheduled paired build using Recall's shared host resource receipt."""
import argparse
import hashlib
import importlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

import build
import prepare

HERE = Path(__file__).resolve().parent


def release_helpers(root: Path):
    """Load the reviewed release helper; never fork its allocation accounting."""
    path = root / "scripts/release"
    sys.path.insert(0, str(path))
    try:
        helper = importlib.import_module("container_artifact")
    finally:
        sys.path.pop(0)
    if (Path(helper.__file__).resolve() != path / "container_artifact.py"
            or Path(helper.artifact.__file__).resolve() != path / "artifact.py"):
        raise ValueError("resource or ELF helper came from another checkout")
    return helper


def cache_identity(source: Path, provenance: dict) -> str:
    graph = {
        "prepared_path": str(source.resolve()),
        "prepared_provenance_sha256": prepare.digest(source / "RECALL_SOURCE_PROVENANCE.json"),
        "recall_commit": provenance["recall_commit"],
        "rust_toolchain": provenance["rust_toolchain"],
        "target": "x86_64-unknown-linux-gnu", "profile": "release",
        "package": "surreal", "default_features": True,
    }
    if os.environ.get('RECALL_BUILD_RESOURCE_MODE') == 'current-capacity':
        graph = {'rust_toolchain': provenance['rust_toolchain'], 'target': 'x86_64-unknown-linux-gnu',
                 'profile': 'release', 'package': 'surreal', 'default_features': True,
                 'inputs': json.loads((source / '.recall/container-inputs.lock.json').read_text()),
                 'wrapper_sha256': prepare.digest(source / '.recall/repo-cargo'),
                 'native_environment': {'LZMA_API_STATIC': '1'},
                 'profile_config': __import__('tomllib').loads((source / 'Cargo.toml').read_text()).get('profile', {})}
    return "recall-paired-" + hashlib.sha256(json.dumps(graph, sort_keys=True).encode()).hexdigest()


def run_build(command: list[str], budget) -> None:
    budget.check()
    current = os.environ.get('RECALL_BUILD_RESOURCE_MODE') == 'current-capacity'
    environment = budget.build_environment() if current else None
    process = subprocess.Popen(command, start_new_session=True, env=environment)
    try:
        while process.poll() is None:
            budget.check()
            time.sleep(2)
        if process.returncode:
            raise RuntimeError(f"paired image build failed: exit {process.returncode}")
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        if current:
            source = next(value.split('=', 1)[1] for value in command if value.startswith('RECALL_SOURCE_COMMIT='))
            budget.confirm_build_terminal(source)
    budget.check()


def validate_bundle(source: Path, bundle: Path, inputs: dict, artifact) -> dict:
    receipt = json.loads((bundle / "BUILD_PROVENANCE.json").read_text())
    expected_source = prepare.digest(source / "RECALL_SOURCE_PROVENANCE.json")
    if (receipt.get("status") != "built_unqualified"
            or receipt.get("source_provenance_sha256") != expected_source
            or prepare.digest(bundle / "RECALL_SOURCE_PROVENANCE.json") != expected_source):
        raise ValueError("paired bundle differs from the prepared source identity")
    if receipt.get("container_inputs") != inputs or receipt.get("actual_builder_image") != inputs["builder"]["image"]:
        raise ValueError("paired bundle used other container inputs")
    binary = bundle / "recall-surreal"
    if receipt.get("binary_sha256") != prepare.digest(binary):
        raise ValueError("paired executable differs from its build receipt")
    if artifact.inspect_elf(binary) != receipt.get("elf"):
        raise ValueError("paired executable ELF differs from its build receipt")
    return receipt


def build_portable(root: Path, source: Path, bundle: Path, tag: str) -> dict:
    if HERE != (root / "packaging/recall-surreal").resolve():
        raise ValueError("run the paired launcher from the same reviewed Recall checkout")
    helper = release_helpers(root)
    identity = helper.artifact.source_identity(root)
    provenance = build.check_prepared(source)
    if provenance["recall_commit"] != identity["source_commit"]:
        raise ValueError("prepared tree and launcher belong to different reviewed commits")
    inputs = helper.load_inputs(root)
    if inputs != build.portable_inputs(source, inputs["builder"]["image"]):
        raise ValueError("prepared image lock differs from the reviewed checkout")
    if bundle.exists() or bundle.is_symlink():
        raise ValueError("paired output must be a new bundle directory")
    budget_path = os.environ.get("RECALL_ARTIFACT_BUDGET_FILE")
    if not budget_path or not Path(budget_path).is_absolute():
        raise ValueError("set absolute RECALL_ARTIFACT_BUDGET_FILE: fresh paired receipt in current-capacity; shared with Recall in legacy mode")
    jobs, minimum, allocation = helper.build_limits()
    docker_root = Path(subprocess.check_output(
        ["docker", "info", "--format", "{{.DockerRootDir}}"], text=True).strip())
    bundle.parent.mkdir(parents=True, exist_ok=True)
    budget = helper.resource_monitor(Path(budget_path),
        (root, source, docker_root, bundle.parent, Path(tempfile.gettempdir())), minimum, allocation)
    budget.check()
    cache = cache_identity(source, provenance)
    # The host ledger covers both artifacts. Keep the paired compiler's
    # independent, narrower growth ceiling when the shared allowance is larger.
    paired_growth = (build.MAX_GROWTH_BYTES if helper.resource_mode() == 'current-capacity'
                     else min(allocation, build.MAX_GROWTH_BYTES))
    with tempfile.TemporaryDirectory(prefix="recall-paired-build-") as temporary:
        scratch = Path(temporary)
        command = ["docker", "buildx", "build", "--platform", "linux/amd64",
            "--file", str(source / ".recall/Dockerfile.build"), "--target", "artifact",
            "--output", f"type=local,dest={bundle}", "--metadata-file", str(scratch / "builder.json")]
        for key, value in [("BUILDER_IMAGE", inputs["builder"]["image"]),
                           ("RECALL_SOURCE_COMMIT", identity["source_commit"]),
                           ("RECALL_PAIRED_CACHE_ID", cache), ("CARGO_BUILD_JOBS", str(jobs)),
                           ("RECALL_PAIRED_MAX_GROWTH_GIB", str(paired_growth // 1024**3)),
                           ("RECALL_BUILD_RESOURCE_MODE", helper.resource_mode())]:
            command.extend(["--build-arg", f"{key}={value}"])
        command.append(str(source))
        run_build(command, budget)
        receipt = validate_bundle(source, bundle, inputs, helper.artifact)
        runtime_command = ["docker", "buildx", "build", "--platform", "linux/amd64", "--load",
            "--file", str(HERE / "Dockerfile"), "--build-arg", f"RUNTIME_IMAGE={inputs['runtime']['image']}",
            "--build-arg", f"RECALL_SOURCE_COMMIT={identity['source_commit']}",
            "--tag", tag, "--iidfile", str(scratch / "image-id"),
            "--metadata-file", str(scratch / "runtime.json"), str(bundle)]
        run_build(runtime_command, budget)
        image_id = (scratch / "image-id").read_text().strip()
        if not helper.IMAGE_ID.fullmatch(image_id):
            raise ValueError("paired runtime did not return an immutable image identity")
        image = json.loads(subprocess.check_output(["docker", "image", "inspect", image_id], text=True))[0]
        if image.get("Id") != image_id or (image.get("Os"), image.get("Architecture")) != ("linux", "amd64"):
            raise ValueError("paired runtime image identity or platform changed")
        resources = budget.check()
        result = {
            "format": "recall-paired-container-build/v1", "status": "built_unqualified",
            "source_commit": identity["source_commit"], "image_id": image_id,
            "repo_digests": image.get("RepoDigests", []), "binary_sha256": receipt["binary_sha256"],
            "cache_id": cache, "commands": [command, runtime_command],
            "builder_metadata": json.loads((scratch / "builder.json").read_text()),
            "runtime_metadata": json.loads((scratch / "runtime.json").read_text()),
            "resource_helper_sha256": prepare.digest(root / "scripts/release/container_artifact.py"),
            "resource_budget_file": str(budget.receipt), "resource_budget": resources,
            "qualification": "Build only; no paired server, provider, transport or recovery acceptance run",
        }
        with (bundle / "CONTAINER_BUILD_PROVENANCE.json").open("x") as stream:
            json.dump(result, stream, indent=2)
            stream.write("\n")
        budget.check()
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recall-root", type=Path, required=True)
    parser.add_argument("--prepared", type=Path, required=True)
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--tag", required=True)
    options = parser.parse_args()
    try:
        print(json.dumps(build_portable(options.recall_root.resolve(), options.prepared.resolve(),
                                      options.bundle.absolute(), options.tag), indent=2))
    except (ValueError, OSError, KeyError, RuntimeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"paired build: {error}\n")
