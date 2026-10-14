# Missing optional native fields

Native queue observations project optional Work and Run columns. Absent fields
previously left the small local object lookup and constructed the full async
fallback evaluator. Its object-field branch then continued from `NONE`, even
when the field was the final path component.

The local lookup now performs that same missing-field continuation directly. An
empty tail returns `NONE`; every nonempty tail still calls `NONE.get` with the
original stack, context, options and document. Path-depth validation still occurs
before lookup. Methods, their arguments, optional parts and recursion are not
discarded. Present fields, complex record IDs, arrays, remote records and the
entire fallback body retain their previous behavior.

This removes unnecessary fallback construction at a frequent optional-field
boundary. It changes no source acceptance, ancestry limits, row predicates,
hot/archive reads, nonce writes, stored Futures or event ordering. The original
256-Work/257-Source workload and its 240-second deadline remain unchanged.

Two workspace controls exercise actual native document projections, `NONE`
method continuations, method-argument errors and unselected sibling evaluation.
Two private dependency controls compare the retained fallback across thirteen
tail shapes and preserve the original path-depth refusal. Private controls are
not counted as workspace execution. Source inspection and formatting do not
establish a performance result; the original complete workload must pass.
