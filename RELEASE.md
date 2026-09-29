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

## Build candidates

Manually run `.github/workflows/release.yml` (**Build release candidates**) for
the reviewed ref. It validates the source, builds Linux x86-64 and macOS ARM64
binaries with RocksDB and the default Meerkat extraction feature, and uploads
candidate archives. It does not create a GitHub release or publish crates.

Each archive contains the executable, license texts, and `provenance.json`
with the source commit/tree, build command, workflow run, and binary checksum.
The workflow also creates archive checksums and build attestations. Verify the
successful run and download the artifacts before promotion.

## Promote verified artifacts

1. Check the archive checksum and bundled provenance. Smoke test the executable
   on each supported target host and record the exact binary checksum.
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
