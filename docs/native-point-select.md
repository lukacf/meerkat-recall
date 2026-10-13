# Native point SELECT dispatch

Plain `SELECT *` reads of one prepared record use a small output future. The
selected branch is chosen before constructing either the point or general
output future on the existing reblessive stack. Record target preparation also
selects the original `prepare_thing` future before constructing the generic
preparation future. Neither change reduces the number or order of required
database reads, guard writes, nonce changes or events.

The enclosing subquery dispatcher selects each original statement future on the
same stack as well. This keeps a point SELECT from inheriting frame storage for
unselected mutation and scan statements. Its context, parent document, options,
branch selection and outer Return conversion stay unchanged.

Admission requires exactly one `*` field, without the VALUE flag, one original
target, one prepared `Iterable::Thing`, no inherited query planner, and a fresh
iterator. Every modifier except ONLY is excluded: conditions, WITH, omissions,
split/group/order, limit/start, fetch, historical version, timeout, explain,
parallel and temporary-file output all retain general output. Target evaluation
and table permission checks occur before admission. An outer planner can survive
setup of an empty local planner, so admission checks the resulting context.

The point branch preserves the statement trace, child cancellation context,
memory result collector and original `opt.dive(4)`. It checks cancellation before
collection, uses the original `Collected::Thing` processor for strict table checks
and versioned record access, then uses the original iterator/document processor.
That processor retains record, table, view and field permissions, futures,
metadata removal and Ignore behavior. Its document check after fetching and the
iterator's post-entry check remain in their original order. Stored document
errors propagate only after the post-entry check. The caller still applies its
original timeout check and ONLY zero/one/many result conversion.

The complete general preparation and output implementations remain unchanged.
No query permission, transaction boundary, depth limit, deadline or ancestry
boundary is relaxed. In particular, the original 256/257 ancestry controls and
240-second deadline remain the performance acceptance criteria.

Workspace controls use the actual native database and compare native results
with explicit expected values and ineligible general queries. They cover native
numeric and record-ID representations, missing rows, ONLY cardinality, target
and projection side effects, transactional event rollback, modifiers and targets,
and adjacent values at the original computation depth boundary. Separate native
permission controls exercise the shared document path. Source review and authored
tests do not establish runtime or performance success; central validation must
execute them and the unchanged ancestry controls.
