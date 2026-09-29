#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CARGO="${CARGO:-$ROOT/scripts/repo-cargo}"

python3 - "$ROOT" "$CARGO" <<'PY'
import json
import pathlib
import subprocess
import sys
import tomllib

root = pathlib.Path(sys.argv[1])
cargo_bin = sys.argv[2]
with (root / "Cargo.toml").open("rb") as fh:
    cargo = tomllib.load(fh)

workspace_version = cargo["workspace"]["package"]["version"]
metadata = subprocess.check_output(
    [cargo_bin, "metadata", "--format-version", "1", "--no-deps"],
    cwd=root,
    text=True,
)
meta = json.loads(metadata)
workspace_members = set(meta["workspace_members"])
packages = [pkg for pkg in meta["packages"] if pkg["id"] in workspace_members]

mismatches: list[str] = []
for pkg in sorted(packages, key=lambda item: item["name"]):
    if pkg["version"] != workspace_version:
        mismatches.append(
            f"{pkg['name']}: version {pkg['version']} != workspace {workspace_version}"
        )

if mismatches:
    print("Version parity check failed:", file=sys.stderr)
    for mismatch in mismatches:
        print(f"  - {mismatch}", file=sys.stderr)
    sys.exit(1)

print(f"version-parity=ok workspace_version={workspace_version}")
PY
