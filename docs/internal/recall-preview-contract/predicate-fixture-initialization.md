# Native predicate and audit fixture initialization

The Phase 13 approval adapter, MCP predicate fixtures and MCP audit-revert fixture
initialize a fresh native in-memory store with `elephant_migrations::apply_schema`
before creating records. Applying migration 031 alone leaves later canonical
writer events without their required `fn::recall_reference_guard`; Runtime16
reported this concrete native failure in the pipeline and MCP approval paths.
The API's matching approval fixture returned its intentionally redacted storage
error. The corrected fixtures retain the actual migration receipts, journal/key
initialization and all current native fences. They do not redefine schema tables
or substitute guard functions after initialization.

The classified MCP reprocess fixture creates its selected configuration with the
original classified policy, level, labels, handling and subjects. The prior
fixture tried to change that envelope through a repository update, which correctly
refuses changes to the authorized envelope before reaching the handler. Creating
the original classified record preserves the intended admission/read regression
without relaxing that guard or recapturing authority after a failed mutation.

All original refusal, full inherited-envelope and atomic artifact/alias/job
assertions remain. These are source repairs following the original Runtime16
failures; no successful Rust execution is claimed by this checkpoint. MCP uses
the intake owner's explicit development dependency on the existing workspace
migrations crate for these fixtures.
The storage-level `public_mutation` native fixture also installs the actual
current reference and physical read guards before creating its first record,
using `reference_fence::install_native_reference_guards` after its canonical
schema setup. Storage cannot depend on the migrations crate without a dependency
cycle. This is the existing production guard installer, including the current
039/043 definitions; no test stub or weakened guard is introduced. The original
audit inverse and atomic predicate approval assertions remain unchanged.
