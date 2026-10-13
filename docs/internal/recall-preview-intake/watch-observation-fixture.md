# Original Watch observation fixture

The accepted-byte crash-gap regression captures the actual live Source immediately after initial acceptance, before the original producer claim and first manual replacement. Acceptance can stamp the live row differently from its immutable revision, so the authorization snapshot must describe the actual old row. The proposed replacement still derives from the original immutable input and carries its original publication fence.

This fixes fixture setup only. Original retries continue to use their original snapshots; production authorization is unchanged. The existing regression still requires a fresh actual claim to observe already accepted bytes without accepting another generation, staging another document, changing the original receipt, or adding another queue item.

The corresponding full Standing fixture already uses the actual live Source. Apply this isolated correction only to the standalone extension, retaining that full Standing behavior when composing. Source review and formatting were performed; Cargo and Rust tests were not run in the intake worktree.
