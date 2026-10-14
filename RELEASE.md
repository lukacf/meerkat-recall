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

## Distribution contract

The current source declares Meerkat Recall `0.9.0` and exact-pins its six direct
Meerkat family dependencies to the published `0.9.0` release. It has no direct
Meta dependency. Record this integration's exact versions and live acceptance
separately; matching versions do not establish acceptance of external Meta or
HomeCore runtimes. Existing Recall `v0.1.0` tags and artifacts remain immutable.
Publish `v0.9.0` only after the corresponding source and artifacts qualify.

The initial distribution includes source and native binary downloads, without
a registry-published Rust library. Native targets are Linux x86-64
(`x86_64-unknown-linux-gnu`) and macOS ARM64 (`aarch64-apple-darwin`). Public
support for a target requires its own artifact and clean-machine receipt.
The checked-in [installation instructions](docs/installation.mdx) use the
workflow's flat archive layout and require a model provider for the
default-feature demo.

The public source repository and release destination are both
`lukacf/meerkat-recall`. Publish versioned releases as normal releases, with
GitHub's prerelease flag disabled. Build candidates remain intermediate artifacts
until qualified for promotion. Source downloads and native provenance retain
their exact selected identities; when a distribution snapshot differs from the
build commit, preserve its explicit mapping rather than relabeling the binary. Internal crates, configuration paths,
environment names, database identities and HTTP/MCP wire names retain their
existing spelling. No automatic data relocation is part of this rename.

## Build candidates

Manually run `.github/workflows/release.yml` (**Build release candidates**) for
the reviewed ref. It validates the source, builds Linux x86-64 and macOS ARM64
binaries with RocksDB and the default Meerkat extraction feature, and uploads
candidate archives. It does not create a GitHub release or publish crates.

The native build and packaging implementation is versioned in
`scripts/release/`; the workflow invokes the same recipe used by allocated native
builders. The recipe compares committed source identities before and after build.
The archive uses `meerkat-recall-native/v1`, records the complete source tar
SHA256 and hashes every runtime file, including exact patched vendor source.

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

1. Select the exact qualified artifacts and their retained build/install
   evidence. For workflow-built candidates, verify the successful workflow and
   its GitHub attestation against the selected source and workflow identity.
   Independently built releases retain their actual native build receipts and
   must not claim a GitHub attestation. Check checksums, bundled provenance and
   the exact executable acceptance for each supported target.
2. Confirm the intended `vX.Y.Z` tag matches the Cargo workspace version. Tag
   the reviewed source commit after the checks pass.
3. Create a draft normal GitHub release in `lukacf/meerkat-recall`, with
   `prerelease=false`, containing those verified archives, a flat
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

## Container candidate

`scripts/release/build-container LOCAL_TAG OUTPUT_DIRECTORY` builds from the exact
committed Git archive and writes an image receipt with source commit/tree/tar
digest, local image ID and any assigned repository digests. It never pushes or
deploys. The image embeds source/build provenance, licensed patched dependency
source, and the native binary. A local image ID is not a registry digest.
Qualify the published bytes by digest separately before using the rendered
StatefulSet/PV recipe; no native receipt qualifies a different image build.
