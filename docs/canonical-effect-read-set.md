# Canonical effect read set

Both canonical backend transactions validate every typed effect's original
before-image before applying the first typed effect. Dependencies, original
publication and attempt authority, the canonical epoch, and all receipt guards
remain part of that same transaction. Effect order is preserved.

This matters for native graph storage: the pinned SurrealDB `Document::purge`
deletes incident edges when it deletes an endpoint. Checking a later edge's
before-image after that endpoint write would compare against this transaction's
own cascade and reject an otherwise valid plan. An explicit later edge deletion
is safe after its original row has already been validated.

The regression source uses endpoint-before-edge effect ordering on both exposed
backends. It verifies rejection of a stale edge with a current epoch, rollback
after the first endpoint deletion (including native graph adjacency), atomic
completion with its outbox and receipt, exact receipt replay, and preservation
of an independent edge and both surviving endpoints. The existing actual source
replacement regression remains necessary to qualify the full publication path.
