# Meerkat Recall native archive

Retain this entire verified installation. `meerkat-recall` is the executable;
`elephant` is a compatibility symlink. The artifact includes its skills, patched
dependency sources, notices, licenses, service templates and offline verifier.
Use `PIPELINE_SKILLS_DIR` with the absolute installed `skills` directory.

Read [operations](docs/operations.md) for named spaces, signing keys, grants,
processing approvals, source reconciliation and recovery. Read
[installation](docs/installation.md) for archive verification and the public
download location. Templates do not start services or provision secrets.

`provenance.json` records actual build and executable identity plus an explicit
public source/documentation derivation. Its build commit is distinct from the
parentless public snapshot. This derivative metadata is not a raw original build
receipt or an attestation. Packaging alone does not establish acceptance.

Model weights and provider credentials are separate. The offline
`provision-fastembed` helper requires Python 3.12+ and selected local model files.
The [snapshot guide](docs/snapshot-durability.md) and
[embedded commit guide](docs/embedded-durable-commits.md) explain recovery and
the independent journal that must survive replacement of data and backups.
