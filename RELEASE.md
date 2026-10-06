# Release process

Releases promote reviewed, verified artifacts built from a specific source
commit. Tags do not trigger a build or publish a release. The workspace version
and changelog can describe a candidate before any public release exists.

## Validate the source

Start from a clean checkout of the intended release commit:

```bash
make release-preflight
make test-e2e
```

`make ci-smoke` is a smaller local gate, not a substitute for the release gate.
Confirm CI passed for the selected commit, version parity holds, and release
notes describe the actual changes and supported platforms.

Before public distribution, review tracked files, package contents, all refs
and history that will become public, and repository metadata for credentials,
private data, logs, snapshots, and internal deployment details. A clean working
tree and a secret-pattern scan alone do not establish publication safety.
Keep private audit evidence outside the repository. If anything sensitive was
committed, removing it from the latest tree does not remove it from history.

## Candidate contract

The current source declares Meerkat Recall `0.1.0` and pins its six direct Meerkat
family dependencies to `0.8.51`. It has no direct MobKit dependency. These
source versions do not imply acceptance of an external MobKit or HomeCore
runtime, or compatibility with the planned 0.9 platform. Record that
integration's exact versions and live acceptance separately.

The initial distribution includes source and native binary downloads, without
a registry-published Rust library. Native targets are Linux x86-64
(`x86_64-unknown-linux-gnu`) and macOS ARM64 (`aarch64-apple-darwin`). Public
support for a target requires its own artifact and clean-machine receipt.
The checked-in [installation instructions](docs/installation.mdx) use the
workflow's flat archive layout and require a model provider for the
default-feature demo.

The public source repository is `lukacf/meerkat-recall`; candidate work remains
private until publication is authorized. Source downloads and native archives
share the selected `v0.1.0` source identity. Internal crates, configuration paths,
environment names, database identities and HTTP/MCP wire names retain their
existing spelling. No automatic data relocation is part of this rename.

## Build candidates

Manually run `.github/workflows/release.yml` (**Build release candidates**) for
the reviewed ref. It validates the source, builds Linux x86-64 and macOS ARM64
binaries with RocksDB and the default Meerkat extraction feature, and uploads
candidate archives. It does not create a GitHub release or publish crates.

Each archive contains `meerkat-recall`, an `elephant` symlink for the 0.1
compatibility line, license texts, and `provenance.json`
with the source commit/tree, build command, workflow run, and binary checksum.
The workflow creates archive checksums, uploads the candidate artifacts, then
requires a GitHub build attestation for each archive. A later attestation failure
leaves the run failed while preserving the uploaded bytes for private testing.

For private testing only, the owner may select those artifacts when source
validation and the target's build, packaging and upload steps all succeeded.
Record the exact run and attempt, artifact identity, source and checksums, and
the attestation failure as pending evidence. Bundled `provenance.json` is build
metadata, not a verified GitHub attestation. A downloadable artifact or a
successful private test does not qualify it for public promotion.

Record the selected build profile as well as its features. This workflow uses
`--profile deploy`; a locally accepted `--release` executable is a separate
artifact until its exact profile, source and bytes are explicitly selected for
distribution. Do not transfer an executable's acceptance receipt to a newly
built archive solely because the source commit matches.

## Accept the actual archives

A successful build, `--help`, or source-built no-default-features demo is not
clean-install acceptance of a distributed default-feature executable. For
each target, preserve a receipt with:

- The exact archive and binary SHA256, source commit and tree, `Cargo.lock`
  SHA256, build profile and command, enabled/default features, license files,
  and provenance source.
- Target-specific notices for the Rust and native components actually linked
  into the executable, plus provenance and redistribution treatment for bundled
  fixtures. The workspace's own license does not establish complete dependency
  or fixture compatibility.
- A clean host's OS version and architecture, required dynamic libraries, and
  the commands used to verify, unpack, and install the archive without Cargo.
- `meerkat-recall --help` and `meerkat-recall --version` output, plus identical
  version output from the `elephant` compatibility alias. Bind the reported
  version to provenance and the verified digest.
- A configured model provider with access to the selected writer and verifier
  models. Record provider/model names and success or failure, never credentials.
  Missing credentials are an unmet prerequisite, not a semantic demo pass.
- A run of the installed `meerkat-recall demo-wiki`: inspect its context projection,
  compilation receipt, source evidence, article-generation results and any
  verifier rejections. Record the returned evidence, not just HTTP status.
- Separate persistent-runtime checks for migration, restart persistence,
  authenticated HTTP/MCP reads and writes, isolation, and live ingestion with
  pending/embedded counts. The in-memory demo cannot establish these.

Also verify the documented download path anonymously after public assets
exist. A private candidate supplied directly to a tester cannot prove public
discoverability or download authorization. Record unavailable target, archive,
provider, persistence or live-isolation evidence as pending.

## Promote verified artifacts

1. Require a successful candidate workflow and verify each archive's GitHub
   build attestation against the selected source and workflow identity. Check
   its checksum and bundled provenance. Smoke test the executable on each
   supported target host and record the exact binary checksum. Missing or
   failed attestation blocks public promotion even if private testing passed.
2. Confirm the intended `vX.Y.Z` tag matches the Cargo workspace version. Tag
   the reviewed source commit after the checks pass.
3. Create a draft GitHub release containing those verified archives, a flat
   `checksums.sha256` manifest, and accurate release notes. Verify the uploaded
   bytes and provenance before publishing the draft.
4. Move the relevant `Unreleased` changelog entries into the published version
   section with the actual publication date.

If a build comes from a different commit, verify identical source trees or
review and document every build-excluded difference. A locally authored
provenance file does not constitute a GitHub build attestation.

## Deployment and rollback

Rehearse migrations on a consistent database backup before activation. Retain
the previous executable with its matching data and configuration, and verify
runtime readiness plus representative application requests after switching.
See [Deployment](docs/operations/deployment.mdx) for the operational procedure.

## Package publishing

Publishing binary archives does not publish Rust crates or the local-helper
npm package. Crate publishing is disabled in workspace release metadata and
is not an input to the candidate workflow. Use `make publish-dry-run` to
inspect package eligibility and contents; enabling package publication is a
separate release change.
