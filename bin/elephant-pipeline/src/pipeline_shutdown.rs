//! Classify worker completion against the actual requested shutdown state, then
//! drain all workers while preserving failures from the cleanup phase.
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};

type WorkerExit = Option<Result<anyhow::Result<()>, JoinError>>;

fn classify_exit(result: WorkerExit, requested: bool) -> anyhow::Result<()> {
    match result {
        Some(Ok(Ok(()))) if requested => Ok(()),
        Some(Ok(Ok(()))) => anyhow::bail!("pipeline worker exited unexpectedly"),
        Some(Ok(Err(error))) => Err(error.context("pipeline worker stopped with error")),
        Some(Err(error)) => anyhow::bail!("pipeline worker task failed: {error}"),
        None => anyhow::bail!("all pipeline workers exited unexpectedly"),
    }
}

pub(super) async fn wait(
    tasks: &mut JoinSet<anyhow::Result<()>>,
    shutdown: &mut watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let initial = loop {
        if *shutdown.borrow() {
            break Ok(());
        }
        tokio::select! {
            changed = shutdown.changed() => {
                if let Err(error) = changed {
                    break Err(anyhow::Error::new(error).context("pipeline shutdown channel closed"));
                }
                // A false notification is not a request to stop the runtime.
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                // The task and notification may both be ready. Read the actual
                // shared state here rather than interpreting select's winner.
                break classify_exit(result, *shutdown.borrow());
            }
        }
    };
    let aborting = initial.is_err();
    let mut failures: Vec<String> = initial
        .err()
        .map(|error| format!("{error:#}"))
        .into_iter()
        .collect();
    if aborting {
        // Remaining workers cannot be left running after a failed supervisor.
        tasks.abort_all();
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => failures.push(format!("pipeline worker cleanup: {error:#}")),
            Err(error) if aborting && error.is_cancelled() => {}
            Err(error) => failures.push(format!("pipeline worker cleanup task failed: {error}")),
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        anyhow::bail!("{}", failures.join("; "))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::panic)]
    use super::*;

    #[tokio::test]
    async fn both_ready_join_winner_observes_actual_shutdown_request() {
        let (send, mut shutdown) = watch::channel(false);
        send.send(true).expect("actual requested shutdown");
        let completed = std::future::ready(Some(Ok(Ok(()))));
        // Deterministically select the completed-worker branch while BOTH it
        // and the actual watch notification are ready, matching the race.
        let result = tokio::select! {
            biased;
            result = completed => classify_exit(result, *shutdown.borrow()),
            _ = shutdown.changed() => panic!("join branch must win this deterministic oracle"),
        };
        result.expect("requested clean completion is successful");
        assert!(classify_exit(Some(Ok(Ok(()))), false).is_err());
        assert!(classify_exit(Some(Ok(Err(anyhow::anyhow!("real failure")))), true).is_err());
    }

    #[tokio::test]
    async fn requested_shutdown_retains_every_cleanup_error_and_panic() {
        let (_send, mut shutdown) = watch::channel(true);
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Err(anyhow::anyhow!("first worker error")) });
        tasks.spawn(async { Err(anyhow::anyhow!("second worker error")) });
        tasks.spawn(async { panic!("actual worker panic") });
        let error = wait(&mut tasks, &mut shutdown)
            .await
            .expect_err("cleanup failure")
            .to_string();
        assert!(error.contains("first worker error"));
        assert!(error.contains("second worker error"));
        assert!(error.contains("actual worker panic"));
        assert!(tasks.is_empty(), "every worker was joined");
    }

    #[tokio::test]
    async fn unexpected_clean_exit_aborts_and_joins_remaining_workers() {
        let (_send, mut shutdown) = watch::channel(false);
        let mut tasks = JoinSet::new();
        tasks.spawn(async { Ok(()) });
        tasks.spawn(std::future::pending::<anyhow::Result<()>>());
        let error = wait(&mut tasks, &mut shutdown)
            .await
            .expect_err("unexpected exit")
            .to_string();
        assert!(error.contains("pipeline worker exited unexpectedly"));
        assert!(
            tasks.is_empty(),
            "failed supervisor retains no orphan tasks"
        );
    }
}
