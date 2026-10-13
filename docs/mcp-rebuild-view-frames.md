# MCP rebuild dispatch frames

Runtime24's original mixed-projection erasure fixture still aborted on the
default stack. Its actual compiled `RebuildKnowledgeViewHandler::execute` poll
function reserved 266296 bytes (prologue `0x41000 + 0x38`) while dispatching the
real `entity_id` request. A boxed trait future does not by itself bound the poll
function's native stack frame.

The handler now selects one private entity, source or definition rebuild method.
Each branch retains its original scoped lookup, write authorization, limits,
forced compiler call and authorized response conversion, in that order. The
selected branch and its repository/compiler/response child futures are boxed at
their actual awaits. Private non-inlined method boundaries keep branch
construction out of the shared dispatch body. No spawned task, thread or new
execution context is introduced.

Input coercion and schema, missing-space refusal, limit validation, root parsing,
permission verbs, error conversion and response shape remain unchanged. The
existing Entity/Source/Graph compiler APIs and their publication/read semantics
are unchanged. This patch touches only the rebuild handler; current reads and
wiki compilation remain separately owned.

The new pure regression checks the actual three private future types against a
32 KiB storage bound without constructing or polling a fake handler. It does not
measure generated poll frames or prove stack safety. The original real mixed
erasure fixture, all existing rebuild authorization/reservation controls and the
unchanged default stack remain the runtime acceptance tests. Source review and
formatting are the only checks performed for this checkpoint.
