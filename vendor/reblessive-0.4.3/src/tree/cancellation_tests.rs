//! Regression tests for the cleanup ownership transferred by `Runner::finish`.
#![forbid(unsafe_code)]

use super::TreeStack;
use std::{
    future::{pending, Future},
    panic::{catch_unwind, AssertUnwindSafe},
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Wake, Waker},
};

struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}

fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    let waker = Waker::from(Arc::new(Noop));
    future.poll(&mut Context::from_waker(&waker))
}

struct Probe(Arc<AtomicUsize>);
impl Drop for Probe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn assert_reusable(stack: &mut TreeStack) {
    let mut next = Box::pin(
        stack
            .enter(|_| async { String::from("next query") })
            .finish(),
    );
    assert_eq!(poll(next.as_mut()), Poll::Ready(String::from("next query")));
}

#[test]
fn dropping_unpolled_finish_releases_captures() {
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Probe(drops.clone());
    let mut stack = TreeStack::new();
    drop(stack.enter(|_| async move { drop(probe) }).finish());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_reusable(&mut stack);
    drop(stack);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn cancelling_nested_finish_drops_each_task_once_and_allows_reuse() {
    let drops = Arc::new(AtomicUsize::new(0));
    let root = Probe(drops.clone());
    let child = Probe(drops.clone());
    let mut stack = TreeStack::new();
    let mut finish = Box::pin(
        stack
            .enter(|stk| async move {
                let _root = root;
                stk.run(|_| async move {
                    let _child = child;
                    pending::<()>().await;
                })
                .await;
            })
            .finish(),
    );
    assert!(poll(finish.as_mut()).is_pending());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(finish);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_reusable(&mut stack);
    drop(stack);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn cancelling_scoped_finish_releases_scheduler_children() {
    let drops = Arc::new(AtomicUsize::new(0));
    let first = Probe(drops.clone());
    let second = Probe(drops.clone());
    let mut stack = TreeStack::new();
    let mut finish = Box::pin(
        stack
            .enter(|stk| async move {
                stk.scope(|scope| async move {
                    let a = scope.run(|_| async move {
                        let _first = first;
                        pending::<()>().await;
                    });
                    let b = scope.run(|_| async move {
                        let _second = second;
                        pending::<()>().await;
                    });
                    futures_util::future::join(a, b).await;
                })
                .await;
            })
            .finish(),
    );
    assert!(poll(finish.as_mut()).is_pending());
    drop(finish);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_reusable(&mut stack);
    drop(stack);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

#[test]
fn cancelled_borrowed_future_drops_before_its_input() {
    struct Borrowed<'a>(&'a [String], Arc<AtomicUsize>);
    impl Drop for Borrowed<'_> {
        fn drop(&mut self) {
            assert_eq!(self.0[0], "live until cancelled");
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    // On assertion failure the stack is destroyed before input, keeping the
    // native red regression safe. The Miri-only test below checks UAF.
    let input = vec![String::from("live until cancelled")];
    let mut stack = TreeStack::new();
    {
        let borrowed = Borrowed(&input, drops.clone());
        let mut finish = Box::pin(
            stack
                .enter(|_| async move {
                    let _borrowed = borrowed;
                    pending::<()>().await;
                })
                .finish(),
        );
        assert!(poll(finish.as_mut()).is_pending());
        drop(finish);
        // Assert while input is still live, so an unfixed native test fails
        // without deliberately executing the later dangling destructor.
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    drop(input);
    drop(stack);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn finished_result_has_one_owner_and_stack_is_reusable() {
    let drops = Arc::new(AtomicUsize::new(0));
    let result = Probe(drops.clone());
    let mut stack = TreeStack::new();
    let mut finish = Box::pin(stack.enter(|_| async { result }).finish());
    let Poll::Ready(result) = poll(finish.as_mut()) else {
        panic!("result was not ready")
    };
    drop(finish);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_reusable(&mut stack);
    drop(stack);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(result);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn panicking_finish_releases_nested_tasks() {
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Probe(drops.clone());
    let mut stack = TreeStack::new();
    assert!(catch_unwind(AssertUnwindSafe(|| {
        let mut finish = Box::pin(
            stack
                .enter(|stk| async move {
                    let _probe = probe;
                    stk.run(|_| async { panic!("synthetic query panic") }).await;
                })
                .finish(),
        );
        poll(finish.as_mut())
    }))
    .is_err());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_reusable(&mut stack);
}

// With the upstream implementation this safe-Rust input produces a dangling
// destructor when the stack is dropped. Keep it Miri-only so a native red run
// does not deliberately execute undefined behavior.
#[cfg(miri)]
#[test]
fn cancelled_finish_cannot_outlive_borrowed_data() {
    struct Borrowed<'a>(&'a [String]);
    impl Drop for Borrowed<'_> {
        fn drop(&mut self) {
            assert_eq!(self.0[0], "live until cancelled");
        }
    }
    let mut stack = TreeStack::new();
    {
        let source = vec![String::from("live until cancelled")];
        let borrowed = Borrowed(&source);
        let mut finish = Box::pin(
            stack
                .enter(|_| async move {
                    let _borrowed = borrowed;
                    pending::<()>().await;
                })
                .finish(),
        );
        assert!(poll(finish.as_mut()).is_pending());
        drop(finish);
    }
    drop(stack);
}

#[test]
fn cross_thread_cancellation_tolerates_stale_scheduler_wakers() {
    struct CaptureWaker(Arc<Mutex<Option<Waker>>>);
    impl Future for CaptureWaker {
        type Output = ();
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            *self.0.lock().unwrap() = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    let mut stack = TreeStack::new();
    let rounds = if cfg!(miri) { 8 } else { 200 };
    for _ in 0..rounds {
        let drops = Arc::new(AtomicUsize::new(0));
        let root = Probe(drops.clone());
        let child = Probe(drops.clone());
        let captured = Arc::new(Mutex::new(None));
        let capture = CaptureWaker(captured.clone());
        let mut finish = Box::pin(
            stack
                .enter(|stk| async move {
                    let _root = root;
                    stk.scope(|scope| async move {
                        scope
                            .run(|_| async move {
                                let _child = child;
                                capture.await;
                            })
                            .await;
                    })
                    .await;
                })
                .finish(),
        );
        assert!(poll(finish.as_mut()).is_pending());
        let stale = captured.lock().unwrap().take().expect("child was polled");
        std::thread::scope(|scope| {
            scope.spawn(move || drop(finish)).join().unwrap();
        });
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        stale.wake_by_ref();
        stale.wake();
        assert_reusable(&mut stack);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }
}
