# Native intake fixture initialization

The Runtime16 MCP intake failures reached `fn::recall_physical_read_fence` in
the real publication guard, but the document/source fixtures had only created
bare record tables and applied migration 031. They omitted the current native
fence function and other active schema dependencies.

All four native fixture constructors for document intake, document extraction,
document restamping and source intake now call the authoritative
`elephant_migrations::apply_schema` on their fresh, empty database. Partial DDL
and the obsolete `doc_chunk` fixture definition are removed. The original
named-space configuration and every handler assertion remain unchanged. The
embedded fixture continues to use its actual embedded storage kernel.

This adds one existing workspace dev dependency to `elephant-mcp` and its lock
edge. It introduces no new package, resolver operation, schema definition or
production guard bypass. The established migration future is boxed at the
fixture boundary, as in the existing mounted API MCP setup.

The production source audit found complete initialization already present:

- `elephant_migrations::apply_schema` includes all 43 current migrations and
  validates the resulting schema receipts and journal identity.
- Explicit runtime initialization and the native `migrate` command call that
  function; ordinary runtime startup calls read-only `verify_schema`.
- Native snapshot replacement calls `refresh_native_reference_guards`, whose
  shared installer reapplies migrations 039 and 043 before restore admission
  opens. It validates the existing private rows before installing these guards.

The recorded missing-function failures establish this fixture defect. Batch
count and missing-result failures in the same constructors remain subject to
the next central run; this source change does not claim they have passed.
The retained handler tests remain the regression oracles. Only direct
formatting, diff and manifest/lock syntax checks were run locally; no Cargo or
Rust test execution was performed.
