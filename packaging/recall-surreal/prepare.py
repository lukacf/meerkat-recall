#!/usr/bin/env python3
"""Prepare the pinned paired server source. Never invokes Cargo or a server."""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
import urllib.request

HERE = Path(__file__).resolve().parent


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def file_manifest(root: Path) -> dict[str, str]:
    result = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise ValueError(f"symlink not permitted in reviewed source: {path}")
        if path.is_file():
            result[path.relative_to(root).as_posix()] = digest(path)
    return result


def read_lock() -> dict:
    return json.loads((HERE / "source.lock.json").read_text())


def acquire(lock: dict, archive: Path, download: bool) -> None:
    if not archive.exists():
        if not download:
            raise ValueError("source archive absent; supply it or explicitly use --download")
        archive.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary = tempfile.mkstemp(prefix=".recall-source-", dir=archive.parent)
        partial = Path(temporary)
        try:
            with os.fdopen(descriptor, "wb") as target:
                with urllib.request.urlopen(lock["archive_url"], timeout=60) as response:
                    total = 0
                    while block := response.read(1024 * 1024):
                        total += len(block)
                        if total > lock["archive_max_bytes"]:
                            raise ValueError("source archive exceeds pinned size bound")
                        target.write(block)
            if digest(partial) != lock["archive_sha256"]:
                raise ValueError("upstream source archive digest mismatch")
            os.link(partial, archive)
        finally:
            partial.unlink(missing_ok=True)
    if archive.stat().st_size > lock["archive_max_bytes"] or digest(archive) != lock["archive_sha256"]:
        raise ValueError("upstream source archive digest mismatch")


def extract_source(archive: Path, destination: Path, lock: dict) -> None:
    prefix = f"surrealdb-{lock['upstream_commit']}"
    total = 0
    seen = set()
    with tarfile.open(archive, "r:gz") as source:
        for member in source:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or not path.parts or path.parts[0] != prefix:
                raise ValueError("source archive contains an invalid path")
            relative = Path(*path.parts[1:])
            if not path.parts[1:]:
                if not member.isdir():
                    raise ValueError("invalid source archive root")
                continue
            if relative in seen or not (member.isdir() or member.isfile()):
                raise ValueError("duplicate path or special file in source archive")
            seen.add(relative)
            target = destination / relative
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            total += member.size
            if total > lock["unpacked_max_bytes"]:
                raise ValueError("expanded source archive exceeds bound")
            target.parent.mkdir(parents=True, exist_ok=True)
            with source.extractfile(member) as stream, target.open("xb") as output:
                shutil.copyfileobj(stream, output)
            target.chmod(0o755 if member.mode & 0o111 else 0o644)
    for name, expected in lock["upstream_files"].items():
        if digest(destination / name) != expected:
            raise ValueError(f"pinned upstream file mismatch: {name}")
    manifest = tomllib.loads((destination / "Cargo.toml").read_text())
    if manifest["package"]["name"] != "surreal" or manifest["package"]["version"] != lock["upstream_version"]:
        raise ValueError("not the pinned Surreal server root package")


def add_hmac_lock_edge(text: str) -> str:
    """Add only the new direct dependency; retain every upstream resolved version."""
    packages = tomllib.loads(text)["package"]
    hmac = [p for p in packages if p["name"] == "hmac"]
    if len(hmac) != 1 or hmac[0]["version"] != "0.12.1":
        raise ValueError("unexpected upstream HMAC lock identity")
    pattern = r'(\[\[package\]\]\nname = "surrealdb-core"\nversion = "2\.6\.5"\n.*?dependencies = \[\n)(.*?)(\n\])'
    match = re.search(pattern, text, re.S)
    if not match:
        raise ValueError("missing original core lock entry")
    lines = match[2].splitlines()
    if ' "hmac",' in lines:
        raise ValueError("unexpected preexisting core HMAC lock edge")
    lines.append(' "hmac",')
    replacement = match[1] + "\n".join(sorted(lines)) + match[3]
    result = text[:match.start()] + replacement + text[match.end():]
    after = tomllib.loads(result)["package"]
    before_by_name = {(p["name"], p["version"]): p for p in packages}
    after_by_name = {(p["name"], p["version"]): p for p in after}
    for key, package in before_by_name.items():
        expected = dict(package)
        if key == ("surrealdb-core", "2.6.5"):
            expected["dependencies"] = sorted(package["dependencies"] + ["hmac"])
        if after_by_name.get(key) != expected:
            raise ValueError("unrelated dependency lock change")
    return result


def paired_lock(text: str, lock: dict) -> str:
    """Make only the reviewed HMAC edge and KV/vart parity substitutions."""
    text = add_hmac_lock_edge(text)
    spec = lock["surrealkv"]
    before = tomllib.loads(text)["package"]
    result = text
    expected = []
    for package in before:
        updated = dict(package)
        if package["name"] == "surrealkv":
            if (package["version"], package.get("checksum")) != (
                spec["upstream_lock_version"], spec["upstream_lock_checksum"]
            ):
                raise ValueError("unexpected upstream SurrealKV lock identity")
            updated.pop("source")
            updated.pop("checksum")
            updated["version"] = spec["version"]
            updated["dependencies"] = [
                "vart " + spec["vart"]["version"] if dep == "vart " + spec["vart"]["upstream_version"] else dep
                for dep in package["dependencies"]
            ]
        elif package["name"] == "vart" and package["version"] == spec["vart"]["upstream_version"]:
            if package.get("checksum") != spec["vart"]["upstream_checksum"] or package.get("dependencies"):
                raise ValueError("unexpected upstream vart lock identity")
            updated["version"] = spec["vart"]["version"]
            updated["checksum"] = spec["vart"]["checksum"]
        elif "vart " + spec["vart"]["upstream_version"] in package.get("dependencies", []):
            raise ValueError("unreviewed second consumer of replaced vart identity")
        if updated != package:
            pattern = r'(?m)^\[\[package\]\]\nname = "' + re.escape(package["name"]) + r'"\nversion = "' + re.escape(package["version"]) + r'"\n.*?(?=^\[\[package\]\]|\Z)'
            match = re.search(pattern, result, re.S)
            if not match:
                raise ValueError("missing reviewed package block")
            lines = ["[[package]]"]
            for key, value in updated.items():
                if isinstance(value, list):
                    lines += [key + " = ["] + [" " + json.dumps(item) + "," for item in value] + ["]"]
                else:
                    lines.append(key + " = " + json.dumps(value))
            result = result[:match.start()] + "\n".join(lines) + "\n\n" + result[match.end():]
        expected.append(updated)
    if sum(p["name"] == "surrealkv" for p in before) != 1 or sum(
        p["name"] == "vart" and p["version"] == spec["vart"]["upstream_version"] for p in before
    ) != 1:
        raise ValueError("missing pinned KV/vart lock closure")
    if tomllib.loads(result)["package"] != expected:
        raise ValueError("unrelated dependency lock change")
    return result


def overlay_surrealkv(recall: Path, source: Path, lock: dict) -> dict:
    spec = lock["surrealkv"]
    vendor = recall / spec["path"]
    parent = vendor.parent
    for name, expected in spec["provenance_files"].items():
        if digest(parent / name) != expected:
            raise ValueError("SurrealKV provenance differs from reviewed checkpoint")
    original = json.loads((parent / "surrealkv-0.9.3-upstream-sha256.json").read_text())
    expected = original | spec["patched_files"]
    manifest = file_manifest(vendor)
    if manifest != expected:
        raise ValueError("SurrealKV differs from reviewed retention/lifetime repair")
    package = tomllib.loads((vendor / "Cargo.toml").read_text())["package"]
    if (package["name"], package["version"]) != ("surrealkv", spec["version"]):
        raise ValueError("incorrect SurrealKV package identity")
    target = source / spec["path"]
    shutil.copytree(vendor, target)
    if file_manifest(target) != manifest:
        raise ValueError("SurrealKV copy was not byte-exact")
    for name in spec["provenance_files"]:
        shutil.copy2(parent / name, target.parent / name)
    return manifest


def overlay_vendor(recall: Path, source: Path, lock: dict) -> dict:
    manifests = {}
    for package, relative in lock["vendor_paths"].items():
        vendor = recall / relative
        provenance = json.loads((vendor / "RECALL_PROVENANCE.json").read_text())
        if (provenance["package"], provenance["version"], provenance["source_revision"]) != (
            package, lock["upstream_version"], lock["upstream_commit"]
        ):
            raise ValueError(f"paired vendor provenance mismatch: {package}")
        manifest = file_manifest(vendor)
        target = source / "crates" / ("core" if package == "surrealdb-core" else "sdk")
        # This target exists only in our fresh, verified staging tree.
        shutil.rmtree(target)
        shutil.copytree(vendor, target)
        if file_manifest(target) != manifest:
            raise ValueError("vendor copy was not byte-exact")
        manifests[package] = manifest
    manifests["surrealkv"] = overlay_surrealkv(recall, source, lock)
    core = source / "crates/core"
    if "hmac" not in tomllib.loads((core / "Cargo.toml").read_text()).get("dependencies", {}):
        raise ValueError("reviewed reference capability dependency is absent")
    if not (core / "src/fnc/recall.rs").is_file():
        raise ValueError("reviewed reference builtin is absent")
    cargo = source / "Cargo.toml"
    original = cargo.read_text()
    if "[patch.crates-io]" in original:
        raise ValueError("unexpected upstream patch section")
    workspace = tomllib.loads(original)["workspace"]
    if "exclude" in workspace or original.count("[workspace]\n") != 1:
        raise ValueError("unexpected upstream workspace membership")
    original = original.replace("[workspace]\n", '[workspace]\nexclude = ["vendor/surrealkv-0.9.3"]\n', 1)
    cargo.write_text(original + '\n# Recall: preserve identical reviewed storage forks.\n[patch.crates-io]\nsurrealdb-core = { path = "crates/core" }\nsurrealkv = { path = "vendor/surrealkv-0.9.3" }\n')
    path = source / "Cargo.lock"
    path.write_text(paired_lock(path.read_text(), lock))
    return manifests


def prepare(recall: Path, expected_commit: str, archive: Path, output: Path, download: bool) -> dict:
    lock = read_lock()
    if HERE != (recall / "packaging/recall-surreal").resolve():
        raise ValueError("run the preparation recipe from the same reviewed Recall checkout")
    if not re.fullmatch(r"[0-9a-f]{40}", expected_commit):
        raise ValueError("explicit full reviewed Recall commit required")
    actual = subprocess.check_output(["git", "-C", str(recall), "rev-parse", "HEAD"], text=True).strip()
    if actual != expected_commit:
        raise ValueError("Recall checkout differs from reviewed commit")
    dirty = subprocess.check_output(["git", "-C", str(recall), "status", "--porcelain", "--untracked-files=all", "--", "vendor", "packaging/recall-surreal", "scripts/repo-cargo", "scripts/release/artifact.py", "scripts/release/container-inputs.lock.json", "Cargo.toml", "Cargo.lock"], text=True)
    if dirty:
        raise ValueError("paired source inputs must be committed and unchanged")
    verifier = recall / "vendor/verify_recall_surrealdb.py"
    if not verifier.is_file():
        raise ValueError("reviewed paired vendor verifier absent; join E core/SDK checkpoint first")
    subprocess.run(["python3", str(verifier)], check=True)
    if output.exists() or output.is_symlink():
        raise ValueError("refusing to replace an existing prepared source directory")
    acquire(lock, archive, download)
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".recall-surreal-", dir=output.parent) as temporary:
        stage = Path(temporary) / "source"
        stage.mkdir()
        extract_source(archive, stage, lock)
        vendors = overlay_vendor(recall, stage, lock)
        (stage / "rust-toolchain.toml").write_text(f'[toolchain]\nchannel = "{lock["rust_toolchain"]}"\nprofile = "minimal"\n')
        (stage / ".recall").mkdir()
        shutil.copy2(recall / "scripts/repo-cargo", stage / ".recall/repo-cargo")
        # Reuse the release lane's reviewed ELF/ABI authority, not a second parser.
        for name in ("artifact.py", "container-inputs.lock.json"):
            shutil.copy2(recall / "scripts/release" / name, stage / ".recall" / name)
        for name in ("build.py", "prepare.py", "Dockerfile.build", "entrypoint.sh"):
            shutil.copy2(HERE / name, stage / ".recall" / name)
        receipt = {
            "format": 1, "status": "source_prepared_unbuilt_unqualified",
            "recall_commit": actual, "upstream_commit": lock["upstream_commit"],
            "archive_sha256": lock["archive_sha256"], "source_lock_sha256": digest(HERE / "source.lock.json"),
            "rust_toolchain": lock["rust_toolchain"], "server_package": "surreal",
            "capability_version": lock["reference_token_version"],
            "build_metadata": f"recall.reference-v1.{actual[:12]}",
            "vendor_files": vendors, "prepared_files": file_manifest(stage),
            "lock_change": "Core direct hmac 0.12.1 edge; registry SurrealKV 0.9.1 replaced by reviewed path 0.9.3; its sole vart edge 0.9.2 -> 0.9.3; all unrelated packages unchanged",
        }
        (stage / "RECALL_SOURCE_PROVENANCE.json").write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
        os.rename(stage, output)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--recall-root", type=Path, required=True)
    parser.add_argument("--expected-recall-commit", required=True)
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--download", action="store_true", help="explicitly download the pinned source archive if absent")
    args = parser.parse_args()
    receipt = prepare(args.recall_root.resolve(), args.expected_recall_commit, args.archive.resolve(), args.output.absolute(), args.download)
    print(json.dumps({"status": receipt["status"], "recall_commit": receipt["recall_commit"], "output": str(args.output)}))


if __name__ == "__main__":
    main()
