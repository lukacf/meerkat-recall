//! Payload-free evidence that a runner began cleaning up pending root tasks.
#![forbid(unsafe_code)]

use std::{
    io::{self, Write},
    sync::atomic::{AtomicUsize, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static CLEANUP_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

pub(super) fn record_pending_drop(pending_root_tasks: usize) {
    if pending_root_tasks == 0 {
        return;
    }
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH);
    let unix_ms = elapsed.as_ref().map_or(0, |time| time.as_millis());
    record(
        &mut io::stderr().lock(),
        &CLEANUP_SEQUENCE,
        pending_root_tasks,
        unix_ms,
        elapsed.is_ok(),
        std::process::id(),
        std::thread::panicking(),
    );
}

// A failed diagnostic write must not turn cancellation into a panic. This
// record precedes cleanup; it does not claim that cleanup completed.
fn record(
    output: &mut impl Write,
    sequence: &AtomicUsize,
    pending_root_tasks: usize,
    unix_ms: u128,
    clock_ok: bool,
    pid: u32,
    unwinding: bool,
) {
    if pending_root_tasks == 0 {
        return;
    }
    let seq = sequence.fetch_add(1, Ordering::Relaxed).wrapping_add(1);
    let _ = writeln!(output, "event=recall_tree_cleanup phase=begin unix_ms={unix_ms} clock_ok={clock_ok} pid={pid} seq={seq} pending_root_tasks={pending_root_tasks} unwinding={unwinding}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_pending_cleanup_and_unwinding_but_not_completed_runs() {
        let sequence = AtomicUsize::new(0);
        let mut output = Vec::new();
        record(&mut output, &sequence, 0, 123, true, 7, false);
        assert!(output.is_empty());
        assert_eq!(sequence.load(Ordering::Relaxed), 0);
        record(&mut output, &sequence, 2, 123, true, 7, false);
        record(&mut output, &sequence, 1, 0, false, 7, true);
        assert_eq!(String::from_utf8(output).unwrap(), concat!(
            "event=recall_tree_cleanup phase=begin unix_ms=123 clock_ok=true pid=7 seq=1 pending_root_tasks=2 unwinding=false\n",
            "event=recall_tree_cleanup phase=begin unix_ms=0 clock_ok=false pid=7 seq=2 pending_root_tasks=1 unwinding=true\n",
        ));
    }

    #[test]
    fn failed_output_and_counter_wrap_do_not_panic() {
        // A full fixed buffer returns WriteZero. Diagnostics are best effort.
        let mut full: &mut [u8] = &mut [];
        let sequence = AtomicUsize::new(usize::MAX);
        record(&mut full, &sequence, 1, 123, true, 7, false);
        assert_eq!(sequence.load(Ordering::Relaxed), 0);
    }
}
