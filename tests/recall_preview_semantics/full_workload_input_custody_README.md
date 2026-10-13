The full-workload CLI retains exact external receipt bytes and independent
expectation bytes from the original allocation's descriptors. A descriptor is
`{owner, original, expectations}`, where each input is the existing exact
`{path, sha256}` form. The complete descriptor must match the allocation.
The earlier `{owner, original}` form remains readable with its expectation
prerequisite absent; the loader does not derive expectations from observations.

`Inputs.read_with_raw` opens one canonical task-owned private regular file,
checks its inode and metadata across the bounded read, verifies the original
hash, and passes those same bytes to the existing strict transport decoder.
Each external file is bounded to 4 MiB; both count against the existing 32 MiB
aggregate before reading. The in-memory carrier adds `original_raw`, and when
provided `expectations_raw` and `expectations_sha256`. Raw bytes are never added
to the on-disk JSON descriptor or serialized by the CLI as a new receipt.

A owns the consumer and D's pure oracle owns the verdict. The new loader controls
import the actual CLI with inert driver/spy/transport execution objects; they
exercise file custody only. They do not execute the CLI, inspect a namespace,
start a service, read credentials, contact a provider, or qualify a consumer.

Only OB3 has D's frozen optional expectation descriptor. HomeCore's earlier
`{owner, original}` input remains readable with unmet independent expectations;
HomeCore cannot adopt OB3's descriptor by changing the owner label. The original
six CLI/driver helper pins now also include the fixed seventh source dependency
`scripts/qualification/ob3_outage_oracle.py`; no runtime-selected oracle path is
accepted.

The complete owned-runtime source closure extends those original seven files to
18 fixed files, including the installed release/process helpers, their D source
dependencies, and the runtime owner and inspector. The input-custody control
checks that exact independent path set, retaining every original seven-file
member and the fixed OB3 module-to-path identity. It does not derive its expected
inventory from the CLI's declared modules. This corrects the stale seven-file
assertion exposed by the first joined helper batch; it changes no runtime file,
source pin, oracle predicate or acceptance threshold.
