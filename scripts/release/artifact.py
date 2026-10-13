#!/usr/bin/env python3
"""Versioned offline native archive packaging, verification and installation.

This records source/build provenance, not target acceptance or an attestation.
Only the build recipe should supply a binary for distribution.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tarfile
import tempfile
import tomllib

FORMAT = "meerkat-recall-native/v1"
TARGETS = {"x86_64-unknown-linux-gnu", "aarch64-apple-darwin"}
FEATURES = ["rocksdb", "fastembed"]
PATCHED_SOURCE_TREES = (
    "pdf-inspector", "surrealkv-0.9.3", "meerkat-llm-core-0.8.51",
    "meerkat-openai-0.8.51", "meerkat-gemini-0.8.51",
    "surrealdb-core-2.6.5", "surrealdb-2.6.5",
)
OPERATIONAL_DOCUMENTS = (
    "docs/snapshot-durability.md", "docs/embedded-durable-commits.md",
    "scripts/release/README.md", "docs/operations.md", "docs/installation.md",
)
REFERENCE_SOURCE_TREES = ("surrealdb-core-2.6.5", "surrealdb-2.6.5")
REFERENCE_SOURCE_RECORDS = (
    "surrealdb-core-2.6.5-upstream-sha256.json",
    "surrealdb-2.6.5-upstream-sha256.json", "RECALL_SURREALDB_PATCH.md",
)
REFERENCE_UPSTREAM_COMMIT = "8afd2ba0b6c0e232f19f42070e8811b323324c4c"
REFERENCE_CAPABILITY_FORMAT = "meerkat-recall-reference-capability-source/v1"
BUILD_COMMAND = "./scripts/repo-cargo build --locked --profile deploy -p meerkat-recall-bin --features rocksdb,fastembed"
LINUX_LIBRARIES = {
    "libssl.so.3", "libcrypto.so.3", "libstdc++.so.6", "libgcc_s.so.1",
    "libc.so.6", "libm.so.6", "libpthread.so.0", "libdl.so.2", "librt.so.1",
    "libz.so.1", "liblzma.so.5", "libgomp.so.1", "ld-linux-x86-64.so.2",
}
LINUX_SYMBOL_BASELINE = {"GLIBC": (2, 36), "GLIBCXX": (3, 4, 30), "CXXABI": (1, 3, 13), "GCC": (12, 0, 0)}


def elf_dependencies(header: str, dynamic: str, versions: str) -> dict:
    """Check actual ELF identity and imported versions against Debian 12."""
    if not re.search(r"Class:\s+ELF64\b", header) or not re.search(r"Machine:\s+Advanced Micro Devices X86-64\b", header):
        raise ValueError("native Linux artifact must be x86-64 ELF64")
    needed = sorted(set(re.findall(r"\(NEEDED\).*Shared library: \[([^\]]+)\]", dynamic)))
    if not needed or set(needed) - LINUX_LIBRARIES:
        raise ValueError("unsupported or absent ELF shared-library dependencies")
    # Only version needs describe imported ABI requirements; definitions may be
    # exported by a binary and must not be confused with runtime requirements.
    needs = versions.split("Version needs section", 1)
    if len(needs) != 2:
        raise ValueError("ELF version requirements are absent")
    imported = sorted(set(re.findall(r"Name:\s+([A-Za-z0-9_.]+)", needs[1])))
    if not imported:
        raise ValueError("ELF version requirements are empty")
    for symbol in imported:
        for prefix, maximum in LINUX_SYMBOL_BASELINE.items():
            if symbol.startswith(prefix + "_"):
                suffix = symbol[len(prefix) + 1:]
                if not re.fullmatch(r"\d+(?:\.\d+)*", suffix) or tuple(map(int, suffix.split("."))) > maximum:
                    raise ValueError(f"ELF ABI requirement exceeds Debian 12 baseline: {symbol}")
                break
        else:
            if symbol != "OPENSSL_3.0.0":
                raise ValueError(f"unrecognized imported ELF ABI version: {symbol}")
    return {"format": "meerkat-recall-elf/v1", "baseline": "debian-12-amd64",
            "needed": needed, "imported_versions": imported,
            "header": header, "dynamic": dynamic, "versions": versions}


def inspect_elf(binary: Path) -> dict:
    """Inspect a binary without loading or executing its instructions."""
    outputs = [subprocess.check_output(["readelf", "--wide", option, str(binary)], text=True)
               for option in ("--file-header", "--dynamic", "--version-info")]
    return {**elf_dependencies(*outputs), "binary_sha256": digest(binary)}


def validate_elf(receipt: dict, binary_sha256: str) -> None:
    if receipt.get("binary_sha256") != binary_sha256:
        raise ValueError("ELF receipt names another binary")
    checked = elf_dependencies(receipt.get("header", ""), receipt.get("dynamic", ""), receipt.get("versions", ""))
    if receipt != {**checked, "binary_sha256": binary_sha256}:
        raise ValueError("ELF receipt disagrees with its inspection")


def runtime_dependencies(target: str, output: str) -> dict:
    """Validate dynamic links against libraries in the supported target image/OS."""
    if not isinstance(output, str):
        raise ValueError("runtime dependency inspection must be text")
    libraries = []
    if target == "aarch64-apple-darwin":
        lines = output.splitlines()[1:]  # otool first line is the inspected executable.
        for line in lines:
            path = line.strip().split(" (", 1)[0]
            if not path:
                continue
            if not path.startswith(("/usr/lib/", "/System/Library/")):
                raise ValueError(f"non-system Darwin dynamic library: {path}")
            libraries.append(path)
    elif target == "x86_64-unknown-linux-gnu":
        for line in output.splitlines():
            line = line.strip()
            if not line or line.startswith("linux-vdso.so."):
                continue
            if "not found" in line:
                raise ValueError(f"unresolved Linux dynamic library: {line}")
            path = line.split("=>", 1)[-1].strip().split(" ", 1)[0]
            if not path.startswith(("/lib/", "/lib64/", "/usr/lib/")) or Path(path).name not in LINUX_LIBRARIES:
                raise ValueError(f"unsupported Linux dynamic library: {path}")
            libraries.append(path)
    else:
        raise ValueError("unsupported native dependency target")
    if not libraries:
        raise ValueError("dynamic dependency inspection returned no libraries")
    return {"format": "meerkat-recall-runtime-links/v1", "target": target,
            "libraries": sorted(set(libraries)), "inspection": output}


def inspect_runtime(binary: Path, target: str) -> dict:
    command = ["otool", "-L", str(binary)] if target == "aarch64-apple-darwin" else ["ldd", str(binary)]
    result = runtime_dependencies(target, subprocess.check_output(command, text=True))
    result["binary_sha256"] = digest(binary)
    if target == "x86_64-unknown-linux-gnu":
        result["elf"] = inspect_elf(binary)
    return result


def model_cache_receipt(cache: Path, binary_sha256: str) -> dict:
    """Fingerprint already provisioned model bytes; never resolve or download models."""
    if not re.fullmatch("[a-f0-9]{64}", binary_sha256):
        raise ValueError("model cache receipt requires the exact executable SHA256")
    cache = cache.resolve(strict=True)
    if not cache.is_dir():
        raise ValueError("model cache must be a directory")
    files = {}
    for path in sorted(cache.rglob("*")):
        relative = path.relative_to(cache).as_posix()
        if path.is_symlink():
            resolved = path.resolve(strict=True)
            if not resolved.is_relative_to(cache) or not resolved.is_file():
                raise ValueError(f"model cache link escapes cache or is not a file: {relative}")
            files[relative] = {"symlink": os.readlink(path), "sha256": digest(resolved), "bytes": resolved.stat().st_size}
        elif path.is_file():
            files[relative] = {"sha256": digest(path), "bytes": path.stat().st_size}
        elif not path.is_dir():
            raise ValueError(f"unsupported model cache entry: {relative}")
    if not files:
        raise ValueError("model cache has no model assets")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":")).encode()
    return {"format": "meerkat-recall-model-cache/v1", "binary_sha256": binary_sha256,
            "files": files, "cache_sha256": hashlib.sha256(canonical).hexdigest(),
            "qualification": "observed local bytes only; capture before and after exact-artifact provider acceptance"}



def digest(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def git(root: Path, *args: str) -> str:
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def source_identity(root: Path) -> dict:
    if git(root, "status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("source checkout must be clean, including untracked files; put output outside the checkout")
    # Git's tar stream records the complete committed tree, including vendored
    # source and executable modes. No wall-clock archive timestamp is involved.
    with tempfile.TemporaryFile() as stream:
        subprocess.run(["git", "-C", str(root), "archive", "--format=tar", "HEAD"], stdout=stream, check=True)
        stream.seek(0)
        source_sha256 = hashlib.file_digest(stream, "sha256").hexdigest()
    return {
        "source_commit": git(root, "rev-parse", "HEAD"),
        "source_tree": git(root, "rev-parse", "HEAD^{tree}"),
        "source_tar_sha256": source_sha256,
        "cargo_lock_sha256": digest(root / "Cargo.lock"),
    }


def inventory(bundle: Path) -> dict:
    files = {}
    for path in sorted(bundle.rglob("*")):
        name = path.relative_to(bundle).as_posix()
        if path.is_symlink():
            files[name] = {"symlink": os.readlink(path)}
        elif path.is_file():
            files[name] = {"sha256": digest(path), "bytes": path.stat().st_size,
                           "mode": path.stat().st_mode & 0o777}
    return files


def reference_source_receipt(vendor: Path) -> dict:
    """Bind the exact patched core/SDK sources; no runtime capability claim."""
    trees = {}
    for name in REFERENCE_SOURCE_TREES:
        base = vendor / name
        provenance = json.loads((base / "RECALL_PROVENANCE.json").read_text())
        if (provenance.get("version") != "2.6.5"
                or provenance.get("source_revision") != REFERENCE_UPSTREAM_COMMIT
                or provenance.get("package") != name.removesuffix("-2.6.5")):
            raise ValueError(f"unexpected reference capability source: {name}")
        if "Business Source License 1.1" not in (base / "LICENSE").read_text():
            raise ValueError(f"missing pinned upstream license: {name}")
        files = {}
        for path in sorted(base.rglob("*")):
            if path.is_symlink():
                raise ValueError("reference capability sources cannot contain symlinks")
            if path.is_file():
                files[path.relative_to(base).as_posix()] = digest(path)
        trees[name] = hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    records = {name: digest(vendor / name) for name in REFERENCE_SOURCE_RECORDS}
    return {"format": REFERENCE_CAPABILITY_FORMAT, "version": 1, "upstream_version": "2.6.5",
            "upstream_commit": REFERENCE_UPSTREAM_COMMIT, "source_trees_sha256": trees,
            "source_records_sha256": records, "remote_server": "corresponding patched server and runtime capability probe required"}


def validate_reference_receipt(receipt: dict) -> None:
    if (receipt.get("format") != REFERENCE_CAPABILITY_FORMAT or receipt.get("version") != 1
            or receipt.get("upstream_version") != "2.6.5"
            or receipt.get("upstream_commit") != REFERENCE_UPSTREAM_COMMIT):
        raise ValueError("missing or incompatible native reference capability source receipt")
    for key, expected in (("source_trees_sha256", REFERENCE_SOURCE_TREES),
                          ("source_records_sha256", REFERENCE_SOURCE_RECORDS)):
        values = receipt.get(key, {})
        if set(values) != set(expected) or any(not isinstance(value, str) or not re.fullmatch("[a-f0-9]{64}", value) for value in values.values()):
            raise ValueError("invalid native reference capability source hashes")


def archive_tree(bundle: Path, archive: Path, epoch: int) -> None:
    # Stable archive metadata makes packaging the same inputs reproducible.
    with archive.open("xb") as output, gzip.GzipFile(filename="", fileobj=output, mode="wb", mtime=epoch) as compressed:
        with tarfile.open(fileobj=compressed, mode="w", format=tarfile.PAX_FORMAT) as tar:
            for path in sorted(bundle.rglob("*")):
                info = tar.gettarinfo(str(path), arcname=path.relative_to(bundle).as_posix())
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = epoch
                if info.isfile():
                    with path.open("rb") as source:
                        tar.addfile(info, source)
                else:
                    tar.addfile(info)


def package(root: Path, binary: Path, target: str, output: Path, receipt: Path) -> Path:
    if target not in TARGETS:
        raise ValueError("unsupported native target")
    identity = source_identity(root)
    build = json.loads(receipt.read_text())
    for key, value in identity.items():
        if build.get(key) != value:
            raise ValueError(f"build receipt does not match {key}")
    if build.get("target") != target or build.get("build_command") != BUILD_COMMAND:
        raise ValueError("build receipt target/command mismatch")
    if build.get("binary_sha256") != digest(binary):
        raise ValueError("binary differs from build receipt")
    validate_build(build, target)
    if build["native_reference_capability"] != reference_source_receipt(root / "vendor"):
        raise ValueError("reference capability sources differ from build receipt")
    version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    output.mkdir(parents=True, exist_ok=True)
    archive = output / f"meerkat-recall-{version}-{target}.tar.gz"
    with tempfile.TemporaryDirectory(prefix="recall-package-") as temporary:
        bundle = Path(temporary)
        shutil.copyfile(binary, bundle / "meerkat-recall")
        (bundle / "meerkat-recall").chmod(0o755)
        (bundle / "elephant").symlink_to("meerkat-recall")
        for name in ("LICENSE", "LICENSE-MIT", "LICENSE-APACHE", "THIRD_PARTY_NOTICES.md"):
            shutil.copyfile(root / name, bundle / name)
        shutil.copytree(root / "skills", bundle / "skills")
        shutil.copyfile(root / "scripts/release/ARTIFACT_README.md", bundle / "README.md")
        for name in OPERATIONAL_DOCUMENTS:
            destination = bundle / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / name, destination)
        # Ship the exact patched sources, including PDF parser tables and notices.
        for name in PATCHED_SOURCE_TREES:
            shutil.copytree(root / "vendor" / name, bundle / "third-party" / name)
        for path in sorted((root / "vendor").glob("surrealkv-0.9.3-*")):
            if path.is_file():
                shutil.copyfile(path, bundle / "third-party" / path.name)
        shutil.copyfile(root / "vendor" / "provider-http-backport.json", bundle / "third-party" / "provider-http-backport.json")
        for name in REFERENCE_SOURCE_RECORDS:
            shutil.copyfile(root / "vendor" / name, bundle / "third-party" / name)
        (bundle / "licenses").mkdir()
        if target == "x86_64-unknown-linux-gnu":
            shutil.copytree(root / "scripts/release/onnxruntime-notices",
                            bundle / "licenses/onnxruntime-1.24.2")
        for name in REFERENCE_SOURCE_TREES:
            shutil.copyfile(root / "vendor" / name / "LICENSE", bundle / "licenses" / f"{name}-LICENSE")
        shutil.copyfile(root / "scripts" / "release" / "THIRD_PARTY_SOURCE_NOTICES.txt", bundle / "third-party" / "SOURCE_NOTICES.txt")
        shutil.copytree(root / "deploy" / "native", bundle / "service")
        shutil.copyfile(root / "scripts" / "release" / "artifact.py", bundle / "artifact.py")
        shutil.copyfile(root / "scripts" / "release" / "provision-fastembed", bundle / "provision-fastembed")
        (bundle / "provision-fastembed").chmod(0o755)
        # Safe extraction removes writable group/other bits. Normalize before
        # hashing so checked-out umasks cannot produce self-invalidating bundles.
        for path in bundle.rglob("*"):
            if path.is_symlink():
                continue
            path.chmod(0o755 if path.is_dir() or path.stat().st_mode & 0o111 else 0o644)
        manifest = {
            "format": FORMAT, "version": version, "target": target, **identity,
            "license": "MIT OR Apache-2.0", "build": build,
            "component_licenses": {name: {"license": "BUSL-1.1", "text": f"licenses/{name}-LICENSE"} for name in REFERENCE_SOURCE_TREES},
            "license_scope": "Recall project license; bundled components retain their accompanying licenses",
            "binary_sha256": digest(binary), "files": inventory(bundle),
            "qualification": "not established by packaging; see exact-artifact acceptance receipt",
        }
        (bundle / "provenance.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        archive_tree(bundle, archive, int(git(root, "show", "-s", "--format=%ct", "HEAD")))
    archive.with_suffix("").with_suffix(".sha256").write_text(f"{digest(archive)}  {archive.name}\n")
    return archive


def safe_members(tar: tarfile.TarFile) -> list[tarfile.TarInfo]:
    members = tar.getmembers()
    names = set()
    for member in members:
        name = PurePosixPath(member.name)
        if name.is_absolute() or ".." in name.parts or str(name) != member.name or member.name in names:
            raise ValueError("archive contains an invalid or duplicate path")
        names.add(member.name)
        if member.issym():
            if member.name != "elephant" or member.linkname != "meerkat-recall":
                raise ValueError("archive contains an unsupported symbolic link")
        elif not (member.isfile() or member.isdir()):
            raise ValueError("archive contains an unsupported member type")
        if member.mode & 0o7000:
            raise ValueError("archive contains special file permissions")
    # No archive member may pass through a file/symlink parent.
    nondirectories = {member.name for member in members if not member.isdir()}
    for member in members:
        if any(str(parent) in nondirectories for parent in PurePosixPath(member.name).parents):
            raise ValueError("archive member has a non-directory parent")
    return members


def validate_build(build: dict, target: str) -> None:
    validate_reference_receipt(build.get("native_reference_capability", {}))
    if build.get("build_command") != BUILD_COMMAND or build.get("build_profile") != "deploy":
        raise ValueError("unsupported native build recipe")
    if build.get("features") != FEATURES or build.get("default_features") is not True:
        raise ValueError("artifact must include the exact supported feature set")
    links = build.get("runtime_dependencies", {})
    if links.get("format") != "meerkat-recall-runtime-links/v1" or links.get("target") != target:
        raise ValueError("runtime dependency receipt is missing or targets another platform")
    if links.get("binary_sha256") != build.get("binary_sha256"):
        raise ValueError("runtime dependency receipt names another binary")
    checked = runtime_dependencies(target, links.get("inspection", ""))
    if links.get("libraries") != checked["libraries"]:
        raise ValueError("runtime dependency receipt does not match inspection")
    if target == "x86_64-unknown-linux-gnu":
        validate_elf(links.get("elf", {}), build.get("binary_sha256"))


def validate_manifest(manifest: dict) -> None:
    if manifest.get("format") != FORMAT or manifest.get("target") not in TARGETS:
        raise ValueError("unsupported artifact manifest format or target")
    for field, length in (("source_commit", 40), ("source_tree", 40),
                          ("source_tar_sha256", 64), ("cargo_lock_sha256", 64),
                          ("binary_sha256", 64)):
        value = manifest.get(field)
        if not isinstance(value, str) or len(value) != length or any(c not in "0123456789abcdef" for c in value):
            raise ValueError(f"invalid or absent manifest {field}")
    build = manifest.get("build", {})
    for field in ("source_commit", "source_tree", "source_tar_sha256", "cargo_lock_sha256", "binary_sha256", "target"):
        if build.get(field) != manifest[field]:
            raise ValueError(f"build receipt and manifest disagree on {field}")
    validate_build(build, manifest["target"])
    if not isinstance(manifest.get("version"), str) or not manifest["version"]:
        raise ValueError("missing artifact version")


def verify(archive: Path, expected_sha256: str, destination: Path | None = None) -> dict:
    if len(expected_sha256) != 64 or digest(archive) != expected_sha256.lower():
        raise ValueError("archive SHA256 does not match the trusted release record")
    if destination is not None and (destination.exists() or destination.is_symlink()):
        raise ValueError("install destination already exists; use a fresh version directory")
    parent = destination.parent if destination else None
    if parent:
        parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".recall-verify-", dir=parent) as temporary:
        bundle = Path(temporary) / "bundle"
        bundle.mkdir()
        with tarfile.open(archive, "r:gz") as tar:
            members = safe_members(tar)
            tar.extractall(bundle, members=members, filter="data")
        manifest = json.loads((bundle / "provenance.json").read_text())
        validate_manifest(manifest)
        observed = inventory(bundle)
        observed.pop("provenance.json", None)
        if manifest.get("files") != observed:
            raise ValueError("artifact contents differ from manifest")
        if manifest["build"]["native_reference_capability"] != reference_source_receipt(bundle / "third-party"):
            raise ValueError("bundled reference capability sources differ from build receipt")
        expected_licenses = {name: {"license": "BUSL-1.1", "text": f"licenses/{name}-LICENSE"} for name in REFERENCE_SOURCE_TREES}
        if manifest.get("component_licenses") != expected_licenses:
            raise ValueError("bundled native reference license metadata is absent")
        for name in REFERENCE_SOURCE_TREES:
            if digest(bundle / "licenses" / f"{name}-LICENSE") != digest(bundle / "third-party" / name / "LICENSE"):
                raise ValueError("bundled native reference license text differs")
        if manifest.get("binary_sha256") != digest(bundle / "meerkat-recall"):
            raise ValueError("binary digest differs from manifest")
        if observed.get("elephant") != {"symlink": "meerkat-recall"}:
            raise ValueError("compatibility alias is missing")
        if destination:
            # Rename a verified complete tree on one filesystem. Never replace
            # an existing installation, service config, or running executable.
            bundle.rename(destination)
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    identity = sub.add_parser("source-identity")
    identity.add_argument("--root", type=Path, default=Path.cwd())
    pack = sub.add_parser("package")
    pack.add_argument("--root", type=Path, default=Path.cwd())
    pack.add_argument("--binary", type=Path, required=True)
    pack.add_argument("--target", choices=sorted(TARGETS), required=True)
    pack.add_argument("--output-dir", type=Path, required=True)
    pack.add_argument("--build-receipt", type=Path, required=True)
    reference = sub.add_parser("reference-source-receipt")
    reference.add_argument("--vendor", type=Path, default=Path.cwd() / "vendor")
    runtime = sub.add_parser("runtime-dependencies")
    runtime.add_argument("--binary", type=Path, required=True)
    runtime.add_argument("--target", choices=sorted(TARGETS), required=True)
    cache = sub.add_parser("model-cache-receipt")
    cache.add_argument("--cache", type=Path, required=True)
    cache.add_argument("--binary-sha256", required=True)
    for command in ("verify", "install"):
        check = sub.add_parser(command)
        check.add_argument("--archive", type=Path, required=True)
        check.add_argument("--sha256", required=True, help="SHA256 from a trusted release record")
        if command == "install":
            check.add_argument("--destination", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "source-identity":
            result = source_identity(args.root)
        elif args.command == "reference-source-receipt":
            result = reference_source_receipt(args.vendor)
        elif args.command == "runtime-dependencies":
            result = inspect_runtime(args.binary, args.target)
        elif args.command == "model-cache-receipt":
            result = model_cache_receipt(args.cache, args.binary_sha256)
        elif args.command == "package":
            path = package(args.root, args.binary, args.target, args.output_dir, args.build_receipt)
            result = {"archive": str(path), "sha256": digest(path)}
        else:
            result = verify(args.archive, args.sha256, getattr(args, "destination", None))
        print(json.dumps(result, indent=2, sort_keys=True))
    except (ValueError, OSError, KeyError, tarfile.TarError, subprocess.CalledProcessError) as error:
        parser.exit(1, f"artifact: {error}\n")


if __name__ == "__main__":
    main()
