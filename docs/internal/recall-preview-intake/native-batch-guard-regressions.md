# Native producer guard batching regressions

The independent tests in
`crates/storage/src/work_producer_enqueue_batch_tests.rs` exercise recovery's
private enqueue batching helpers against the current canonical guard from
`bf9d4366c41cb4dbd02fc981eda9c7f54ac0dcc0`. Recovery owns the helper implementation
and its test-module declaration. This commit adds only the regression module
and this explanation; it must be composed with that implementation before
validation.

The existing canonical and private-origin predicates remain authoritative.
Consecutive equal SQL templates and equal binding-name sets may share a loop,
but every original descriptor must retain its position and multiplicity. The
same transaction performs all comparisons and conflict writes before the child
and its private origin can be created. No additional source or producer
authority is introduced.

The five native controls cover:

- Actual typed Work creation, claim, original-fence WorkRun and private origin;
  successful hot and cold tuple reads and duplicate guard descriptors. The cold
  path uses actual attempt completion and `archive_terminal_work`.
- Originally absent Work/Run fields becoming present and an integer becoming a
  string after observation. A valid earlier descriptor must not leave its nonce
  committed when a later original before-image refuses.
- A later private origin with a changed payload, foreign space or missing row.
  Earlier Work/Run and origin nonce writes must roll back.
- Archived Work with a changed envelope space, deletion timestamp, nested
  originally absent field or missing physical archive. The earlier archived
  Run's nonce must roll back. A foreign or deleted hot row beside a valid cold
  original must refuse instead of falling back past the physical hot row.
- Missing or wrong-table Work/Run, foreign-space tuple lookup and invalid or missing private
  evidence. The caller cannot substitute an inferred direct origin.

Every successful guard batch writes a sentinel after the actual guards.
Negative controls require the original guard's refusal, no sentinel, and exact
pre-call physical nonce values. Low-level mutations are deliberately invalid
negative inputs applied after real original observations. They do not fabricate
successful producer authority.

Source review uses the pinned native `Value` binding path and the vendored
SurrealDB 2.6.5 object/loop implementation: objects retain `NONE` entries and each
loop iteration receives a fresh context. Those source facts do not replace the
native execution controls. Direct formatting and diff checks are permitted;
these new tests have not been compiled or executed locally. The parent owns the
coherent validation run, including the unchanged original 256-Work/257-Source
boundary and its existing 240-second deadline. No performance result is claimed.

The central Runtime20 compile found that the pinned SDK does not implement
`QueryResult<u64>` for `Response::take`; no selected test bodies executed in that
run. The physical-count reader now uses the supported `Option<u64>` result and
explicitly requires its actual value. Missing or wrongly typed output fails the
test instead of becoming a zero count. The original count query, nonce reader,
five controls, guard predicates and physical-presence assertions are unchanged.
This correction was reviewed against the vendored SDK; it was not compiled or
executed locally.
