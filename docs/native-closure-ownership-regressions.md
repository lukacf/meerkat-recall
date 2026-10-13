# Native closure ownership controls

The native SurrealDB evaluator borrows the existing closure body in `array::fold`
and the closure branch of `array::filter`. These paths previously cloned the
whole closure into an anonymous function, whose evaluator cloned it again.
Arguments already owned by a closure call now move through its original
coercion loop instead of being copied once more.

Both array operations retain the original anonymous-function options with
futures enabled and its single stack entry into `Closure::compute`. Filter
still computes each member with the incoming options and retains a separate
copy of the argument for the result. Its non-closure equality branch is
unchanged. Closure argument requirements, optional arguments, ignored extra
arguments, isolated scope, coercion errors, body evaluation and return-type
checks retain the original evaluator. Database reads, writes, event execution,
reference normalization, authorization and evaluation depth are unchanged.

Closure parameters bind by their raw identifier, matching native parameter
lookup and named-function argument binding. SQL display formatting can add
backticks to reserved words such as `value`, or quote names containing spaces.
Using that display text as a context key previously made valid closure parameters
resolve to `NONE`. Binding now uses `Ident::to_raw`; error messages retain their
existing SQL display formatting.

The seven native storage controls in
`crates/storage/src/native_closure_ownership_tests.rs` use real memory-backed
SurrealDB queries, fixed native results and durable trace rows:

- Fold checks accumulator contents, element order, ordinal arguments and body
  effects.
- Filter checks ordered input effects, one predicate call per element, exact
  surviving values, non-closure equality and the empty case.
- Argument controls check optional and required arguments, ignored extras,
  argument coercion before body execution, return coercion and native decimals.
- Future-bearing bodies retain one effect per invocation; an original later
  transaction error must roll back those effects.
- Rebound closure bodies are observed on each query, and outer local variables
  remain outside the closure's isolated context.
- Reserved and quoted parameter names work through direct closure calls, fold
  and filter, with exact native results. The existing `$value` cases remain
  unchanged so renaming a fixture cannot conceal the binding defect.

These controls establish behavior only when actually executed. They do not
measure allocations or prove a speedup. The original timed 256-work/257-source
ancestry test remains the performance gate with its original timeout.
