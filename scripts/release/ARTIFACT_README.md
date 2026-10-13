# Meerkat Recall native archive

This bundle records exact source and executable provenance. Packaging does not
establish runtime, provider or consumer acceptance. Keep the complete installed
tree and compare its archive digest with the separately trusted release record.

Native archives put `meerkat-recall` beside this file; `elephant` is a compatibility
symlink to that same executable. The Linux container installs both names under
`/usr/local/bin` and puts this documentation under `/usr/share/meerkat-recall`.
Use the archive's actual executable path when preparing a service or qualification
namespace. The binary is built with default features and `rocksdb,fastembed`.

Read the [operations guide](docs/internal/recall-preview-contract/homecore-operations.md)
for signing-key provisioning, separate management and named-space credentials,
current grants, processing approvals, readiness, and recovery. Its linked
[wire examples](docs/internal/recall-preview-contract/examples.json),
[operator request templates](docs/internal/recall-preview-contract/operator-template.json)
and [public contract](docs/internal/recall-preview-contract/README.md) are included.
These examples contain no operational credentials or provider approval.

The [snapshot guide](docs/snapshot-durability.md) and
[embedded commit guide](docs/embedded-durable-commits.md) describe durable storage
and restore boundaries. Keep the original independent recovery journal outside
replaced data and backup trees. Do not infer successful recovery from liveness.
Native archives also include `service/README.md`, systemd and launchd templates;
installing the archive does not load or start them. The complete source-checkout
[build and verification recipe](scripts/release/README.md) is included for reference;
its source-build commands require the separately reviewed Git checkout.

The complete `skills` tree belongs to this artifact. Set `PIPELINE_SKILLS_DIR`
to its absolute installed path. The supplied native templates and container
already specify their respective paths. A copied executable alone can fall back
to an unavailable build-checkout path and cannot establish an operational install.
PDF parser CMaps are embedded in the executable; no external CMap download is
required. Leave `PDF_INSPECTOR_BCMAPS_DIR` unset unless deliberately provisioning
an independently reviewed override.

Model weights and provider secrets are separate. Governed FastEmbed uses explicitly
approved `file://` asset roots and checksum-pinned manifests. The bundled
`provision-fastembed` helper requires Python 3.12+ on the operator host; the runtime
container does not include Python. Provision and verify selected model assets
outside the container, retain their origin and license evidence, and mount the
complete immutable version directory read-only for the service. A model cache or
model name is not approval, and missing assets are not a passing provider check.

Native archives include `artifact.py` for offline verification and installation
with Python 3.12+. Obtain a trusted copy and archive digest independently before
running a verifier. Container images retain their manifest at `provenance.json`;
record the exact immutable image identity separately. The `third-party` directory
contains all seven patched dependency source trees, original license texts and
patch provenance. Native component licenses also appear under `licenses`;
container licenses are under `/usr/share/licenses/meerkat-recall`.

Remote database mode requires the separately built compatible patched SurrealDB
server. Its binary is not included here; retain the exact matching server source,
build and capability receipts. Ordinary upstream server version equality alone
does not establish compatibility.
