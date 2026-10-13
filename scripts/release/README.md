# Build and provenance

Use Rust 1.94.0 and Python 3.12+. The workspace pins its Meerkat dependencies and
contains the exact patched vendor sources and licenses. Cargo commands use the
repository wrapper so worktrees have separate build outputs:

```sh
./scripts/repo-cargo build --locked --profile deploy -p meerkat-recall-bin --features rocksdb,fastembed
```

Build in a separately provisioned environment with the native development
dependencies required by the chosen target. Running this command creates a new
artifact; it does not recreate an existing release's qualification. The supplied
`build-native` recipe captures committed source identity, compiler, features and
actual runtime dependencies. Linux binaries must satisfy the recorded Debian 12
ABI baseline. Apple Silicon builds must resolve dynamic dependencies to system
libraries and carry the actual linked static ONNX Runtime provenance. Do not
substitute another machine's build cache or assume an untested OS minimum.

The `artifact.py` tool verifies and installs native archives offline. Its
`meerkat-recall-native/v1` manifest binds the build commit/tree, full original
source tar SHA256, lockfile, executable, features and file inventory. A trusted
archive hash authenticates the complete manifest and payload. Verification does
not start the executable or provision model weights or secrets.

Public distribution uses a separate selected source snapshot and reviewed
operator documentation. `PUBLIC_SOURCE.json` maps the unchanged code inputs and
distribution-only changes to the original build tree. Native public provenance
adds `public_derivation`: original archive and raw provenance SHA256, selected
public snapshot commit/tree/archive, and unchanged binary identity. Original
build-host paths and private operational records are not distributed. This is
an explicit metadata projection, not a raw build receipt or build attestation.
Release qualification must select the final public archive, while preserving
the original executable/build evidence.

The seven patched runtime dependency trees and accompanying notices/licenses
remain in native archives. The full source snapshot also retains the other
workspace patches needed to build. Component licenses apply independently of
the project's MIT OR Apache-2.0 license. Remote SurrealDB mode requires a
separately supplied matching patched server and its capability evidence.

See [installation](../../docs/installation.md) and
[operations](../../docs/operations.md). Model assets are independently selected,
licensed and checksum-pinned; `provision-fastembed` works only from local files.
