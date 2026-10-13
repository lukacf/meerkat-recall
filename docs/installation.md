# Native installation

The distribution repository is
[lukacf/meerkat-recall-releases](https://github.com/lukacf/meerkat-recall-releases).
Select a published release's exact assets and trusted `checksums.sha256` record.
The target names are `x86_64-unknown-linux-gnu` (Linux x86-64) and
`aarch64-apple-darwin` (Apple Silicon). Availability and supported OS versions are
stated by each release's actual target receipts; this guide itself is not a
qualification result.

For version 0.1.0, archive URLs have this form:

```text
https://github.com/lukacf/meerkat-recall-releases/releases/download/v0.1.0/meerkat-recall-0.1.0-TARGET.tar.gz
```

Use the matching reviewed source snapshot's `scripts/release/artifact.py` with
Python 3.12+ and the independently trusted archive SHA256:

```sh
python3 scripts/release/artifact.py install \
  --archive meerkat-recall-0.1.0-TARGET.tar.gz \
  --sha256 TRUSTED_ARCHIVE_SHA256 \
  --destination /absolute/releases/recall-0.1.0-DIGEST
```

The verifier checks the complete archive before publishing a new directory. It
refuses an existing destination and does not start a service. Preserve the entire
installation: executable, `elephant` compatibility symlink, skills, licenses,
patched dependency sources, service templates and provenance. Set
`PIPELINE_SKILLS_DIR` to its absolute installed `skills` directory. Copying only
the executable does not preserve all runtime inputs.

The public archive's provenance is an explicitly named derivative of the original
build record. Its source commit/tree identify the actual build. A separate
`public_derivation` binds the parentless public source snapshot, the original
archive/provenance hashes, and the unchanged binary digest. Documentation and
metadata projection change the archive digest; they do not rebuild the binary.
Do not treat the public snapshot commit as the original build commit, or this
metadata as a third-party build attestation.

Model weights and provider secrets are supplied separately. Read
[operations](operations.md) before provisioning named spaces and approvals.
Archive installation, service activation and upgrades are separate operator
actions. An exact executable's acceptance does not automatically qualify a
different build, container image or runtime configuration.
