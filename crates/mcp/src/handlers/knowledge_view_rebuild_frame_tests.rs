use super::{RebuildKnowledgeViewHandler, ToolContext};
use elephant_types::RecordId;
use std::future::Future;

// The factory is never called. Its actual return type identifies the private
// active-path future without initializing storage or inventing fixture rows.
fn phase_size<F: Future>(
    _factory: impl FnOnce(&'static RebuildKnowledgeViewHandler, &'static ToolContext, RecordId) -> F,
) -> usize {
    std::mem::size_of::<F>()
}

#[test]
#[expect(
    clippy::redundant_closure_for_method_calls,
    reason = "Keep the original future-type factory and measured regression unchanged."
)]
fn rebuild_branch_futures_keep_large_children_out_of_dispatch_state() {
    let sizes = [
        (
            "entity",
            phase_size(|handler, ctx, id| handler.rebuild_entity(ctx, id, 25)),
        ),
        (
            "source",
            phase_size(|handler, ctx, id| handler.rebuild_source(ctx, id, 25)),
        ),
        (
            "definition",
            phase_size(|handler, ctx, id| handler.rebuild_definition(ctx, id)),
        ),
    ];
    for (phase, bytes) in sizes {
        assert!(
            bytes <= 32 * 1024,
            "actual rebuild {phase} future retains {bytes} bytes"
        );
    }
}
