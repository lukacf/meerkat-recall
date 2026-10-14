#!/usr/bin/env python3
"""Package one exact container-built executable without another Cargo build."""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import tomllib

import artifact

TARGET = "x86_64-unknown-linux-gnu"
IMAGE_ID = re.compile(r"sha256:[a-f0-9]{64}")
IMAGE_REFERENCE = re.compile(r"(?:[a-z0-9._:-]+/)*[a-z0-9._-]+@sha256:[a-f0-9]{64}")
GIB = 1024**3
MIN_FREE_BYTES = 250 * GIB
DEFAULT_ALLOCATION_BYTES = 150 * GIB
MAX_ALLOCATION_BYTES = 1280 * GIB


def build_limits() -> tuple[int, int, int]:
    """Use 150 GiB by default; a scheduler-approved caller may request up to 1280."""
    jobs = int(os.environ.get("CARGO_BUILD_JOBS", "8"))
    minimum = int(os.environ.get("RECALL_BUILD_MIN_FREE_GIB", "250")) * GIB
    if resource_mode() == 'current-capacity':
        if not 1 <= jobs <= 32 or minimum < MIN_FREE_BYTES:
            raise ValueError('require 1..32 jobs and >=250 GiB target free floor')
        return jobs, minimum, 0  # No cumulative limit in this mode.
    allocation = int(os.environ.get("RECALL_ARTIFACT_MAX_GIB", "150")) * GIB
    if not 1 <= jobs <= 16 or minimum < MIN_FREE_BYTES or not 0 < allocation <= MAX_ALLOCATION_BYTES:
        raise ValueError("require 1..16 build jobs, >=250 GiB free floor and <=1280 GiB artifact allocation")
    return jobs, minimum, allocation


def resource_mode() -> str:
    mode = os.environ.get('RECALL_BUILD_RESOURCE_MODE', 'legacy-allocation')
    if mode not in ('legacy-allocation', 'current-capacity'):
        raise ValueError('unknown build resource mode')
    return mode


def cache_contract(root: Path) -> dict:
    """Compatibility only; Cargo fingerprints source and dependency changes."""
    dockerfile = (root / 'Dockerfile').read_text()
    # Compiler installation and native environment only: later source COPY/RUN
    # and runtime image metadata are not compiler compatibility identities.
    compiler_recipe = dockerfile.split('FROM ${BUILDER_IMAGE} AS builder\n', 1)[1].split('COPY Cargo.toml Cargo.lock', 1)[0]
    compiler_recipe = re.sub(r'ARG SOURCE_[A-Z0-9_]+=.*\n|ARG CARGO_BUILD_JOBS=.*\n|ARG RECALL_BUILD_CACHE_ID=.*\n', '', compiler_recipe)
    profiles = tomllib.loads((root / 'Cargo.toml').read_text())['profile']
    return {'format': 'recall-build-cache-compatibility/v1', 'rustc': '1.94.0',
            'target': TARGET, 'profile': 'deploy', 'features': artifact.FEATURES,
            'default_features': True, 'package': 'meerkat-recall-bin',
            'inputs': load_inputs(root), 'native_static_ort': artifact.digest(root / 'scripts/release/linux-static-ort.lock.json'),
            'profile_config': {key: profiles[key] for key in ('release', 'deploy')},
            'native_environment': {'LZMA_API_STATIC': '1', 'ORT_PREFER_DYNAMIC_LINK': '0'},
            'wrapper_sha256': artifact.digest(root / 'scripts/repo-cargo'),
            'native_helper_sha256': artifact.digest(root / 'scripts/release/linux_static_ort.py'),
            'compiler_recipe_sha256': hashlib.sha256(compiler_recipe.encode()).hexdigest()}


def cache_identity(root: Path, identity: dict) -> str:
    """Reuse retries of one reviewed build, isolating other checkout/feature graphs."""
    graph = cache_contract(root) if resource_mode() == 'current-capacity' else {**identity, "checkout": str(root.resolve()), "rustc": "1.94.0",
             "target": TARGET, "profile": "deploy", "features": artifact.FEATURES,
             "default_features": True}
    digest = hashlib.sha256(json.dumps(graph, sort_keys=True).encode()).hexdigest()
    return "recall-bookworm-rust194-" + digest


class CurrentCapacity:
    """Fresh v2 receipts preserve actual observations; no legacy ledger mutation."""
    def __init__(self, receipt: Path, paths: tuple[Path, ...], minimum: int = MIN_FREE_BYTES):
        if not receipt.is_absolute() or minimum < MIN_FREE_BYTES or receipt.is_symlink():
            raise ValueError('absolute current-capacity receipt and target floor required')
        self.receipt, self.minimum = receipt, minimum
        self.paths = tuple(dict.fromkeys((Path('/'), *(path.resolve() for path in paths))))

    def build_environment(self) -> dict:
        directory = self.receipt.parent / (self.receipt.stem + '-buildx')
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.is_symlink():
            raise ValueError('private BuildKit attribution directory required')
        self.buildx_directory = directory
        env = os.environ.copy()
        env.update(BUILDX_CONFIG=str(directory), BUILDX_BUILDER='default')
        return env

    def confirm_build_terminal(self, source_commit: str) -> None:
        """Retained observer pattern: private refs plus actual daemon history.

        Client exit alone is not cleanup evidence. Never signal the daemon or a
        foreign compiler. An unresolved owned job blocks this build's closure.
        """
        deadline = time.monotonic() + 90
        record = {'source_commit': source_commit, 'all_owned_history_terminal': False, 'refs': {}}
        try:
            while time.monotonic() < deadline:
                refs = []
                root = self.buildx_directory / 'refs'
                for path in root.rglob('*') if root.exists() else []:
                    if path.is_symlink(): raise ValueError('aliased private build ref')
                    if path.is_dir(): continue
                    rel = path.relative_to(root)
                    if (len(rel.parts) != 3 or rel.parts[:2] != ('default', 'default')
                            or not re.fullmatch('[a-z0-9]{20,64}', rel.parts[2])
                            or path.stat().st_size > 16384 or len(refs) >= 2):
                        raise ValueError('unexpected private build reference')
                    refs.append(rel.parts[2])
                for ref in refs:
                    raw = subprocess.check_output(['docker', 'buildx', 'history', 'inspect',
                        '--builder', 'default', '--format', 'json', ref],
                        env=self.build_environment(), timeout=10)
                    if len(raw) > 1024**2: raise ValueError('oversize history record')
                    value = json.loads(raw)
                    args = {item['Name']: item['Value'] for item in value.get('BuildArgs', [])}
                    if value.get('Ref') != ref or source_commit not in (args.get('SOURCE_COMMIT'), args.get('RECALL_SOURCE_COMMIT')):
                        raise ValueError('owned history source identity differs')
                    status = value.get('Status')
                    if status not in ('running', 'completed', 'failed', 'canceled'):
                        raise ValueError('unknown owned build status')
                    record['refs'][ref] = status
                if refs and all(record['refs'][ref] != 'running' for ref in refs):
                    record['all_owned_history_terminal'] = True
                    return
                time.sleep(min(2, max(0, deadline-time.monotonic())))
            raise RuntimeError('owned BuildKit closure unresolved; do not begin the next build')
        finally:
            destination = self.receipt.with_suffix('.buildkit-cleanup.json')
            destination.write_text(json.dumps(record, indent=2) + '\n')
            destination.chmod(0o600)

    def check(self) -> dict:
        self.receipt.parent.mkdir(parents=True, exist_ok=True)
        with self.receipt.with_suffix(self.receipt.suffix + '.lock').open('a+') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            previous = json.loads(self.receipt.read_text()) if self.receipt.exists() else None
            if previous and (previous.get('format') != 'meerkat-recall-current-capacity/v1'
                             or previous.get('refused')):
                raise ValueError('use a fresh current-capacity receipt; never reuse a legacy or refused record')
            memory = next(int(line.split()[1]) * 1024 for line in Path('/proc/meminfo').read_text().splitlines()
                          if line.startswith('MemAvailable:'))
            filesystems = {str(path): {'device': path.stat().st_dev, 'free_bytes': shutil.disk_usage(path).free,
                                      'floor_bytes': max(self.minimum, 265 * GIB) if path == Path('/') else self.minimum}
                           for path in self.paths}
            if previous:
                prior = previous['filesystems']
                if (set(prior) != set(filesystems) or previous['mem_floor_bytes'] > 120 * GIB
                        or any(prior[path]['device'] != value['device']
                               or prior[path]['floor_bytes'] > value['floor_bytes']
                               for path, value in filesystems.items())):
                    raise ValueError('capacity paths/devices changed or floors lowered; use a fresh record')
            state = {'format': 'meerkat-recall-current-capacity/v1', 'sample_unix': time.time(),
                     'checks': (previous['checks'] if previous else 0) + 1, 'filesystems': filesystems,
                     'mem_available_bytes': memory, 'mem_floor_bytes': 120 * GIB,
                     'refused': memory < 120 * GIB or any(v['free_bytes'] < v['floor_bytes'] for v in filesystems.values())}
            descriptor, name = tempfile.mkstemp(prefix=self.receipt.name + '.', dir=self.receipt.parent)
            try:
                with os.fdopen(descriptor, 'w') as stream:
                    json.dump(state, stream, indent=2); stream.write('\n'); stream.flush(); os.fsync(stream.fileno())
                os.replace(name, self.receipt)
                directory = os.open(self.receipt.parent, os.O_RDONLY | os.O_DIRECTORY)
                try: os.fsync(directory)
                finally: os.close(directory)
            finally:
                if os.path.exists(name): os.unlink(name)
            if state['refused']:
                raise RuntimeError('actual free disk or MemAvailable floor reached; stop owned build')
            return state


def resource_monitor(receipt: Path, paths: tuple[Path, ...], minimum: int, allocation: int):
    if resource_mode() == 'current-capacity':
        return CurrentCapacity(receipt, paths, minimum)
    return ResourceBudget(receipt, paths, minimum, allocation)


class ResourceBudget:
    """Persistent aggregate disk checkpoints shared by sequential artifact builders.

    Positive filesystem usage increments consume the allocation; later deletion
    never refunds it. Foreign growth counts too. These sampled observations do
    not attribute bytes to processes or grant another simultaneous Cargo slot.
    """
    def __init__(self, receipt: Path, paths: tuple[Path, ...], minimum: int = MIN_FREE_BYTES,
                 allocation: int = DEFAULT_ALLOCATION_BYTES):
        if not receipt.is_absolute() or minimum < MIN_FREE_BYTES or not 0 < allocation <= MAX_ALLOCATION_BYTES:
            raise ValueError("shared artifact receipt must be absolute and preserve hard resource limits")
        self.receipt, self.minimum, self.allocation = receipt, minimum, allocation
        self.paths = {str(path.stat().st_dev): path.resolve() for path in paths}
        if not self.paths:
            raise ValueError("artifact budget requires monitored filesystems")

    def check(self, *, blocking: bool = True) -> dict:
        self.receipt.parent.mkdir(parents=True, exist_ok=True)
        with self.receipt.with_suffix(self.receipt.suffix + ".lock").open("a+") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
            current = {device: {"path": str(path), "total": usage.total, "used": usage.used,
                                "free": usage.free}
                       for device, path in self.paths.items() for usage in [shutil.disk_usage(path)]}
            if self.receipt.exists():
                state = json.loads(self.receipt.read_text())
                if state.get("format") != "meerkat-recall-artifact-allocation/v1" or set(state["filesystems"]) != set(current):
                    raise ValueError("shared artifact allocation filesystem set changed; scheduler review required")
                if state["minimum_free_bytes"] < MIN_FREE_BYTES or not 0 < state["limit_bytes"] <= MAX_ALLOCATION_BYTES:
                    raise ValueError("shared artifact allocation contains invalid resource limits")
                if state.get("refused"):
                    raise ValueError("shared artifact allocation was refused; scheduler review required")
            else:
                state = {"format": "meerkat-recall-artifact-allocation/v1", "filesystems": current,
                         "minimum_free_bytes": self.minimum, "limit_bytes": self.allocation,
                         "consumed_bytes": 0, "checks": 0}
            state["minimum_free_bytes"] = max(state["minimum_free_bytes"], self.minimum)
            state["limit_bytes"] = min(state["limit_bytes"], self.allocation)
            for device, sample in current.items():
                prior = state["filesystems"][device]
                if sample["total"] != prior["total"]:
                    raise ValueError("monitored filesystem size changed; scheduler review required")
                state["consumed_bytes"] += max(0, sample["used"] - prior["used"])
            state["filesystems"] = current
            state["checks"] += 1
            state["refused"] = (state["consumed_bytes"] >= state["limit_bytes"] or
                                any(sample["free"] < state["minimum_free_bytes"] for sample in current.values()))
            descriptor, filename = tempfile.mkstemp(prefix=self.receipt.name + ".", dir=self.receipt.parent)
            try:
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(state, stream, indent=2); stream.write("\n")
                    stream.flush(); os.fsync(stream.fileno())
                os.replace(filename, self.receipt)
                directory = os.open(self.receipt.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
            finally:
                if os.path.exists(filename):
                    os.unlink(filename)
            if state["refused"]:
                raise RuntimeError("artifact allocation or free-space floor reached; cancel owned build and review")
            return state


def load_inputs(root: Path) -> dict:
    lock = json.loads((root / "scripts/release/container-inputs.lock.json").read_text())
    if lock.get("format") != "meerkat-recall-container-inputs/v1" or lock.get("platform") != "linux/amd64":
        raise ValueError("unsupported container input lock")
    for role in ("builder", "runtime", "frontend"):
        item = lock[role]
        if not IMAGE_REFERENCE.fullmatch(item["image"]) or item["image"].split("@", 1)[1] != item["manifest_digest"]:
            raise ValueError("container inputs require exact immutable image manifests")
        if (item.get("os"), item.get("architecture")) != ("linux", "amd64"):
            raise ValueError("container input platform differs from linux/amd64")
    return lock


def package_inventory(text: str, required: set[str]) -> dict[str, str]:
    packages = {}
    for line in text.splitlines():
        name, separator, version = line.partition("\t")
        if not separator or not re.fullmatch(r"[a-z0-9][a-z0-9+.:_-]*", name) or not version or name in packages:
            raise ValueError("invalid installed package inventory")
        packages[name] = version
    if not required.issubset({name.split(":", 1)[0] for name in packages}):
        raise ValueError("installed package inventory is missing required build/runtime libraries")
    libc = next((version for name, version in packages.items() if name.split(":", 1)[0] == "libc6"), "")
    if not libc.startswith("2.36-"):
        raise ValueError("container must use the Debian 12 libc6 baseline")
    return packages


def native_receipt(root: Path, identity: dict, image: dict, provenance: dict,
                   binary: Path, runtime_links: str, runtime_packages: str) -> dict:
    """Validate all joins before turning container provenance into a native receipt."""
    lock = load_inputs(root)
    if not IMAGE_ID.fullmatch(image.get("Id", "")) or (image.get("Os"), image.get("Architecture")) != ("linux", "amd64"):
        raise ValueError("container artifact requires an exact linux/amd64 image ID")
    labels = image.get("Config", {}).get("Labels", {})
    for label, field in (("org.opencontainers.image.revision", "source_commit"),
                         ("org.meerkat.recall.source-tree", "source_tree"),
                         ("org.meerkat.recall.source-tar-sha256", "source_tar_sha256")):
        if labels.get(label) != identity[field]:
            raise ValueError(f"image label disagrees with reviewed source: {field}")
    if provenance.get("format") != "meerkat-recall-container/v1":
        raise ValueError("container build provenance is absent")
    for field, value in identity.items():
        if provenance.get(field) != value:
            raise ValueError(f"image build provenance disagrees with reviewed source: {field}")
    expected_inputs = {"platform": "linux/amd64", "builder_image": lock["builder"]["image"],
                       "runtime_image": lock["runtime"]["image"]}
    if provenance.get("container_inputs") != expected_inputs:
        raise ValueError("container build used different immutable image inputs")
    if provenance.get("binary_sha256") != artifact.digest(binary):
        raise ValueError("image executable differs from build provenance")
    version = artifact.tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    if provenance.get("version") != version or provenance.get("version_output", "").split()[-1:] != [version]:
        raise ValueError("container executable version differs from reviewed workspace")
    if not re.match(r"rustc 1\.94\.0(?:\s|$)", provenance.get("rustc", "")):
        raise ValueError("container compiler differs from the pinned Rust toolchain")
    artifact.validate_build(provenance, TARGET)
    actual_elf = artifact.inspect_elf(binary)
    recorded_elf = provenance["runtime_dependencies"]["elf"]
    for field in ("binary_sha256", "needed", "imported_versions", "baseline"):
        if actual_elf[field] != recorded_elf[field]:
            raise ValueError("image executable ELF differs from build receipt")
    if provenance["native_reference_capability"] != artifact.reference_source_receipt(root / "vendor"):
        raise ValueError("image native reference sources differ from reviewed source")
    builder_packages = package_inventory(provenance.get("builder_packages", ""),
                                        {"libc6", "libssl-dev", "clang", "cmake", "python3", "binutils"})
    packages = package_inventory(runtime_packages, {"libc6", "libssl3", "libstdc++6", "libgomp1", "ca-certificates", "curl"})
    links = artifact.runtime_dependencies(TARGET, runtime_links)
    links.update(binary_sha256=provenance["binary_sha256"], elf=actual_elf)
    result = {**provenance, "target": TARGET, "runtime_dependencies": links,
              "builder_runtime_dependencies": provenance["runtime_dependencies"],
              "container_image_id": image["Id"], "container_repo_digests": image.get("RepoDigests", []),
              "builder_package_versions": builder_packages, "runtime_package_versions": packages,
              "container_input_lock_sha256": artifact.digest(root / "scripts/release/container-inputs.lock.json"),
              "extraction": "same executable copied from stopped image; no recompilation or execution"}
    artifact.validate_build(result, TARGET)
    return result


def package_image(root: Path, image_id: str, expected_source: str, output: Path) -> dict:
    if not IMAGE_ID.fullmatch(image_id):
        raise ValueError("provide the immutable local image ID, not a mutable tag")
    identity = artifact.source_identity(root)
    if identity["source_commit"] != expected_source:
        raise ValueError("checkout differs from the explicitly reviewed source commit")
    if (output / "container-receipt.json").exists():
        raise ValueError("container receipt already exists; use a fresh output directory")
    image = json.loads(subprocess.check_output(["docker", "image", "inspect", image_id], text=True))[0]
    if image["Id"] != image_id:
        raise ValueError("Docker returned another image identity")
    with tempfile.TemporaryDirectory(prefix="recall-image-package-") as temporary:
        scratch = Path(temporary)
        container = subprocess.check_output(["docker", "create", image_id], text=True).strip()
        try:
            for remote, local in (("/usr/local/bin/meerkat-recall", "meerkat-recall"),
                                  ("/usr/share/meerkat-recall/provenance.json", "provenance.json"),
                                  ("/usr/share/meerkat-recall/runtime-dependencies.txt", "runtime-dependencies.txt"),
                                  ("/usr/share/meerkat-recall/runtime-packages.txt", "runtime-packages.txt")):
                subprocess.run(["docker", "cp", f"{container}:{remote}", str(scratch / local)], check=True)
        finally:
            subprocess.run(["docker", "container", "rm", container], check=True, stdout=subprocess.DEVNULL)
        build = native_receipt(root, identity, image, json.loads((scratch / "provenance.json").read_text()),
                               scratch / "meerkat-recall", (scratch / "runtime-dependencies.txt").read_text(),
                               (scratch / "runtime-packages.txt").read_text())
        receipt = scratch / "native-build.json"
        receipt.write_text(json.dumps(build, indent=2) + "\n")
        archive = artifact.package(root, scratch / "meerkat-recall", TARGET, output, receipt)
        result = {"format": "meerkat-recall-image-receipt/v1", **identity,
                  "image_id": image_id, "architecture": "amd64", "os": "linux",
                  "repo_digests": image.get("RepoDigests", []), "binary_sha256": build["binary_sha256"],
                  "native_archive": str(archive), "native_archive_sha256": artifact.digest(archive),
                  "build_provenance": build,
                  "qualification": "build/package only; installed native/container acceptance not executed"}
        with (output / "container-receipt.json").open("x") as stream:
            json.dump(result, stream, indent=2); stream.write("\n")
        return result


def build_image(root: Path, tag: str, output: Path, *, cache_id: str | None = None, cache_evidence: Path | None = None) -> dict:
    """One scheduled build; an explicit reviewed cache may carry warm compiler outputs.

    The cache is an optimization, never the source identity or acceptance record.
    Cargo and the native-input fingerprints still determine required rebuilds.
    """
    if cache_id is not None and (not isinstance(cache_id, str) or
                                 not re.fullmatch(r"recall-bookworm-rust194-[a-f0-9]{64}", cache_id)):
        raise ValueError("invalid reviewed Recall BuildKit cache identity")
    if cache_id is not None and resource_mode() == 'current-capacity':
        evidence = json.loads(cache_evidence.read_text()) if cache_evidence else {}
        if evidence.get('cache_id') != cache_id or evidence.get('compatibility') != cache_contract(root):
            raise ValueError('existing cache requires exact current compatibility evidence')
        retained = Path(evidence['retained_build_receipt'])
        if artifact.digest(retained) != evidence['retained_build_receipt_sha256']:
            raise ValueError('retained cache compiler receipt changed')
        retained_receipt = json.loads(retained.read_text())
        retained_build = retained_receipt['build_provenance']
        if (retained_build['build_resources']['cache_id'] != cache_id
                or retained_build['target'] != TARGET
                or retained_build['features'] != artifact.FEATURES
                or retained_build['build_profile'] != 'deploy'
                or retained_build['default_features'] is not True
                or not retained_build['rustc'].startswith('rustc 1.94.0 ')):
            raise ValueError('retained cache target/profile/compiler identity differs')
        packages = retained_build['builder_packages']
        package_inventory(packages, {'libc6', 'gcc', 'g++', 'clang', 'cmake', 'libssl-dev', 'liblzma-dev'})
        if hashlib.sha256(packages.encode()).hexdigest() != evidence['retained_builder_packages_sha256']:
            raise ValueError('retained native compiler package inventory differs')
    identity = artifact.source_identity(root)
    inputs = load_inputs(root)
    jobs, minimum, allocation = build_limits()
    budget_path = os.environ.get("RECALL_ARTIFACT_BUDGET_FILE")
    if not budget_path:
        raise ValueError("set an absolute RECALL_ARTIFACT_BUDGET_FILE: fresh per-build in current-capacity; shared in legacy mode")
    docker_root = Path(subprocess.check_output(["docker", "info", "--format", "{{.DockerRootDir}}"], text=True).strip())
    if output.exists():
        raise ValueError('fresh output directory required')
    output.mkdir(parents=True)
    budget = resource_monitor(Path(budget_path), (root, docker_root, output, Path(tempfile.gettempdir())), minimum, allocation)
    budget.check()
    cache = cache_identity(root, identity) if cache_id is None else cache_id
    subprocess.run(["docker", "buildx", "version"], check=True, stdout=subprocess.DEVNULL)
    with tempfile.TemporaryDirectory(prefix="recall-container-build-") as temporary:
        scratch = Path(temporary)
        source = scratch / "source.tar"
        with source.open("xb") as stream:
            subprocess.run(["git", "-C", str(root), "archive", "--format=tar", identity["source_commit"]], stdout=stream, check=True)
        if artifact.digest(source) != identity["source_tar_sha256"]:
            raise ValueError("container context differs from reviewed committed source")
        command = ["docker", "buildx", "build", "--platform", "linux/amd64", "--load",
                   "--tag", tag, "--iidfile", str(scratch / "image-id"),
                   "--metadata-file", str(scratch / "build-metadata.json")]
        for name, value in (("SOURCE_COMMIT", identity["source_commit"]),
                            ("SOURCE_TREE", identity["source_tree"]),
                            ("SOURCE_SHA256", identity["source_tar_sha256"]),
                            ("BUILDER_IMAGE", inputs["builder"]["image"]),
                            ("RUNTIME_IMAGE", inputs["runtime"]["image"]), ("CARGO_BUILD_JOBS", str(jobs)),
                            ("RECALL_BUILD_CACHE_ID", cache)):
            command.extend(["--build-arg", f"{name}={value}"])
        command.append("-")
        with source.open("rb") as stream:
            environment = budget.build_environment() if isinstance(budget, CurrentCapacity) else None
            process = subprocess.Popen(command, cwd=root, stdin=stream, start_new_session=True, env=environment)
            try:
                while process.poll() is None:
                    budget.check()
                    time.sleep(2)
                if process.returncode:
                    raise RuntimeError(f"container build failed: exit {process.returncode}")
            finally:
                if process.poll() is None:
                    os.killpg(process.pid, signal.SIGTERM)
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait()
                if isinstance(budget, CurrentCapacity):
                    budget.confirm_build_terminal(identity['source_commit'])
        # Resolve the immutable local image ID, never trust the caller's tag at
        # the later extraction boundary. BuildKit metadata records its output.
        image_id = subprocess.check_output(["docker", "image", "inspect", "--format", "{{.Id}}",
                                           (scratch / "image-id").read_text().strip()], text=True).strip()
        budget.check()
        result = package_image(root, image_id, identity["source_commit"], output)
        resources = budget.check()
        with (output / "container-build-metadata.json").open("x") as stream:
            json.dump({"command": command, "metadata": json.loads((scratch / "build-metadata.json").read_text()),
                       "input_lock": inputs, "image_id": image_id, "cache_id": cache,
                       "resource_budget_file": str(budget.receipt), "resource_budget": resources,
                       "resource_mode": resource_mode(), "cache_compatibility": cache_contract(root)}, stream, indent=2)
            stream.write("\n")
        return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    commands = parser.add_subparsers(dest="command", required=True)
    pack = commands.add_parser("package")
    pack.add_argument("--image-id", required=True)
    pack.add_argument("--expected-source", required=True)
    pack.add_argument("--output-dir", type=Path, required=True)
    build = commands.add_parser("build")
    build.add_argument("--tag", required=True)
    build.add_argument("--output-dir", type=Path, required=True)
    build.add_argument('--cache-id')
    build.add_argument('--cache-evidence', type=Path)
    args = parser.parse_args()
    try:
        if args.command == "build":
            result = build_image(args.root.resolve(), args.tag, args.output_dir, cache_id=args.cache_id, cache_evidence=args.cache_evidence)
        else:
            result = package_image(args.root.resolve(), args.image_id, args.expected_source, args.output_dir)
        print(json.dumps(result, indent=2))
    except (ValueError, OSError, KeyError, RuntimeError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"container artifact: {error}\n")
