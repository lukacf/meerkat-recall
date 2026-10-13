# Native value evaluation regressions

The storage workspace tests exercise the actual pinned native evaluator through
`StorageClient`, including the SDK's raw native result path. They do not invoke
the computed-data eligibility helper or reproduce it as an expected-result rule.

One independent input replaces native leaves with deferred parameters bound to
the same values. Every nonempty enclosing object or array must consequently run
the original evaluator. Both results are compared with the complete expected
native structure, including concrete numeric and record-ID variants, NONE, bytes
and regex values; no JSON roundtrip is used.

The controls also execute real expressions, casts, constants, futures, generated
and compound record IDs, ordered durable function effects, and an original UPDATE
event with rollback on refusal. A large pure prefix cannot suppress or repeat its
deferred suffix. Earlier deferred errors retain priority over later depth errors,
and arrays retain the opposite order when the overdeep value comes first.
The bound-function effect case uses an explicit original transaction: a bare
parameter does not declare the writeability of functions in its bound value.
RETURN retains its native result while the original COMMIT makes effects durable.

Depth checks use the original public native computation budget without changing
the environment or thread stack. Values immediately around its actual remaining
budget must retain success and refusal. Inputs above the optimization's depth or
node eligibility bounds still use the original evaluator; these bounds are not
new query limits.

These seven tests are source-authored for central workspace validation. They are
not runtime or performance evidence until executed. The original ancestry
256/257 boundary and 240-second deadline, prior guard controls, native comparator
oracles, reference guards and readiness checks remain unchanged. Full vendor
impact also requires the centrally scheduled broader suite.

Five additional local-projection controls exercise the actual idiom evaluator.
They compare native Object/Array projections with deferred-leaf original
evaluation, including exact numeric/Thing variants, missing values and literal
keys. Actual durable function traces verify that unselected deferred siblings
run once, in order, before a dynamic path expression; an eligible pure container
still executes the path expression. Large prefixes cannot hide those effects.

Selecting a shallow field must still reject an unselected sibling beyond the
original depth budget, with success tested at its exact boundary. Eligibility
caps remain fallback conditions. Deferred errors and compound-ID evaluation
retain their original priority; generated IDs are fresh per query. Special
document parameters ignore ordinary local shadows, global parameters retain
their original resolution, and a projected Thing observes an intervening actual
row update. These controls are source-complete, not executed performance proof.
