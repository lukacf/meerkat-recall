# Unqualified F01 source checkpoint

This preserves already-started authorization repair work for the agent assigned on the new machine. It is not merged, qualified or applied by this documentation publication.

- Base commit: `f59e6f5c99ed095a015656626f4d32c106c13075`.
- Source owner's staged tree: `bb4d3d563301f489871825daf1097b20ff4e3d87`.
- Changed files: 20, listed in [manifest.json](manifest.json).
- Patch: [f01-unqualified.patch](f01-unqualified.patch), 114,057 bytes.
- Patch SHA256: `dcf393c5047fb7f85347229207233c8fda7855ef8bbc5e9eaed2ba3bdbaaff76`.
- New source commit/push/PR: none at checkpoint.
- Owner state: stopped for transfer on Luka's instruction; destination agent/machine pending assignment.

## Known incomplete validation

Qualification exited 101 during compilation. `DocRevision` lacks the `HasSecurity` trait implementation required at `crates/mcp/src/handlers/system.rs:1990`. The API chunk handler also uses that trait requirement and needs assessment. **No tests executed.** Further compiler, test or lint failures may remain.

The source owner reports independent source review and fixes for four issues, followed by a clear re-review. It also reports formatting, active-surface, version-parity and documentation checks passing. Those limited checks do not supersede the compiler failure. No subsequent fix/build/test was started after the ownership correction, and the owned qualification process exited naturally.

A final question about REST create-space read-scope preflight before duplicate lookup was raised to the reviewer but was not implemented or re-reviewed. It remains a review note, not an additional confirmed finding.

## Scope carried by the patch

The intended draft scope is space operation/record authorization and guarded updates; authorized provenance lineage; relationship old/new permission checks and guarded replacement; raw REST document redaction; work reads/retry with hot/cold guards; read-only REST middleware; accompanying regression-source additions and documentation. These are intended outcomes, not qualified guarantees. F02-F11 are not implemented by this patch.

## Recovery by the assigned owner

First verify repository instructions, the patch hash and the exact base. Use a fresh isolated worktree/branch; preserve any unrelated existing work. The patch is a draft for inspection, not an instruction to merge it.

On the exact base, after the new owner is authorized to begin implementation, the recorded recovery procedure is:

```bash
git apply --check /path/to/f01-unqualified.patch
git apply --index /path/to/f01-unqualified.patch
git write-tree
```

The resulting staged tree should equal `bb4d3d563301f489871825daf1097b20ff4e3d87`. If the chosen base has advanced, do not expect this equality after adapting the patch. Preserve the original patch, reconcile conflicts deliberately and record the successor revision. The documentation branch's additional handoff files also change the full tree hash, so the exact equality check belongs in a worktree at the stated base.

Resolve the known compile failure, inspect the review note, and perform the repository's appropriate validation under the receiving owner's execution mandate. Do not count the stopped qualification as a pass or apply a blanket visibility/permission workaround merely to compile.
