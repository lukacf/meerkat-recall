//! Admission at the executing blocking thread, after both possible queue waits.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use super::{full_input, EmbeddingResult};
use crate::governed::EmbeddingDispatch;

struct CancelOnDrop(Arc<AtomicBool>);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

pub(super) async fn run<T: Send + 'static>(
    dispatch: Option<EmbeddingDispatch>,
    model: String,
    dimensions: usize,
    preprocessing: full_input::Preprocessing,
    tokens: u64,
    infer: impl FnOnce() -> EmbeddingResult<T> + Send + 'static,
) -> EmbeddingResult<T> {
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| full_input::invalid("local inference requires an async runtime"))?;
    let cancelled = Arc::new(AtomicBool::new(false));
    let _cancel_on_drop = CancelOnDrop(cancelled.clone());
    tokio::task::spawn_blocking(move || {
        let require_live = || {
            if cancelled.load(Ordering::Acquire) {
                Err(full_input::invalid("local embedding request was cancelled"))
            } else {
                Ok(())
            }
        };
        require_live()?;
        // This closure is already executing. Neither a model mutex nor the
        // blocking pool can queue inference after its last approval check.
        let observation = runtime.block_on(async {
            if let Some(dispatch) = &dispatch {
                let (binding, _) = dispatch.authorize().await?;
                if binding.profile.model != model
                    || binding.profile.dimensions != Some(dimensions)
                    || binding.profile.preprocessing != preprocessing.as_str()
                {
                    return Err(full_input::invalid(
                        "approved local embedding profile changed",
                    ));
                }
                require_live()?;
                let observation = dispatch.dispatch_started(&binding).await?;
                dispatch.authorize().await?;
                require_live()?;
                Ok(observation)
            } else {
                Ok(None)
            }
        })?;
        require_live()?;
        let result = infer()?;
        // An admitted inference cannot be interrupted safely. If its caller
        // drops meanwhile, still finish its actual measured usage receipt.
        if let Some(dispatch) = &dispatch {
            runtime.block_on(dispatch.usage_snapshot(observation.as_ref(), tokens))?;
        }
        Ok(result)
    })
    .await
    .map_err(|error| {
        super::EmbeddingError::ProviderError(format!("fastembed join error: {error}"))
    })?
}
