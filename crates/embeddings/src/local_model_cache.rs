//! Service-owned, bounded reuse of verified local model resources.
//!
//! The slot stores a model without request dispatch state. A replacement releases
//! the retained model and waits for its executing owners before loading another.
//! Initialization owns the slot through the blocking task, even if its requester
//! is cancelled. Nothing here caches authorization or provider usage receipts.

#[cfg(any(feature = "fastembed", test))]
use std::sync::Arc;

#[cfg(any(feature = "fastembed", test))]
use crate::{EmbeddingError, EmbeddingResult};

/// One retained verified local model per storage-client lifetime. Fresh contexts
/// and repositories share this resource owner; independent clients do not.
#[derive(Default)]
pub struct VerifiedLocalModelCache {
    #[cfg(feature = "fastembed")]
    slot: ResourceSlot<LocalModelIdentity, crate::FastEmbedEmbeddingProvider>,
}

impl VerifiedLocalModelCache {
    /// Successful model constructions during this owner's lifetime. Reused
    /// request handles do not increment this counter; failed loads are excluded.
    #[must_use]
    pub fn successful_initializations(&self) -> usize {
        #[cfg(feature = "fastembed")]
        {
            self.slot
                .initializations
                .load(std::sync::atomic::Ordering::Relaxed)
        }
        #[cfg(not(feature = "fastembed"))]
        {
            0
        }
    }
}

impl std::fmt::Debug for VerifiedLocalModelCache {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifiedLocalModelCache")
            .finish_non_exhaustive()
    }
}

#[cfg(feature = "fastembed")]
#[derive(Clone, PartialEq, Eq)]
struct LocalModelIdentity {
    endpoint: String,
    model: String,
    manifest: String,
    dimensions: Option<usize>,
    preprocessing: String,
}

#[cfg(feature = "fastembed")]
impl From<&elephant_types::provider_binding::ResolvedModelBinding> for LocalModelIdentity {
    fn from(binding: &elephant_types::provider_binding::ResolvedModelBinding) -> Self {
        Self {
            endpoint: binding.connection.endpoint.clone(),
            model: binding.profile.model.clone(),
            manifest: binding.profile.model_version.clone(),
            dimensions: binding.profile.dimensions,
            preprocessing: binding.profile.preprocessing.clone(),
        }
    }
}

#[cfg(feature = "fastembed")]
impl VerifiedLocalModelCache {
    pub(crate) async fn provider(
        &self,
        binding: &elephant_types::provider_binding::ResolvedModelBinding,
    ) -> EmbeddingResult<crate::FastEmbedEmbeddingProvider> {
        crate::pinned_fastembed::validate_binding_identity(binding)?;
        let identity = LocalModelIdentity::from(binding);
        let binding = binding.clone();
        self.slot
            .get_or_try_init(identity, move |capacity| {
                crate::pinned_fastembed::load_provider(&binding, capacity)
            })
            .await
    }
}

// The generic slot permits deterministic lifetime tests without loading a model
// or adding a production constructor override. Values must retain their capacity
// permit until all actual executing owners have finished with the resource.
#[cfg(any(feature = "fastembed", test))]
pub(crate) struct ResourceSlot<K, V> {
    entry: Arc<tokio::sync::Mutex<Option<(K, V)>>>,
    capacity: Arc<tokio::sync::Semaphore>,
    initializations: Arc<std::sync::atomic::AtomicUsize>,
}

#[cfg(any(feature = "fastembed", test))]
impl<K, V> Default for ResourceSlot<K, V> {
    fn default() -> Self {
        Self {
            entry: Arc::default(),
            capacity: Arc::new(tokio::sync::Semaphore::new(1)),
            initializations: Arc::default(),
        }
    }
}

#[cfg(any(feature = "fastembed", test))]
impl<K: PartialEq + Send + 'static, V: Clone + Send + 'static> ResourceSlot<K, V> {
    #[allow(
        clippy::significant_drop_tightening,
        reason = "The owned slot guard must enter the blocking constructor so cancellation cannot permit duplicate initialization."
    )]
    pub(crate) async fn get_or_try_init(
        &self,
        identity: K,
        load: impl FnOnce(tokio::sync::OwnedSemaphorePermit) -> EmbeddingResult<V> + Send + 'static,
    ) -> EmbeddingResult<V> {
        let mut entry = self.entry.clone().lock_owned().await;
        if let Some((current, resource)) = &*entry {
            if current == &identity {
                return Ok(resource.clone());
            }
        }
        // Do not count only cache entries: an evicted model may still execute
        // on a blocking thread after its async requester has been cancelled.
        entry.take();
        let capacity = self.capacity.clone().acquire_owned().await.map_err(|_| {
            EmbeddingError::ProviderError("local model resource owner closed".into())
        })?;
        let initializations = self.initializations.clone();
        tokio::task::spawn_blocking(move || {
            let resource = load(capacity)?;
            initializations.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            *entry = Some((identity, resource.clone()));
            Ok(resource)
        })
        .await
        .map_err(|_| EmbeddingError::ProviderError("local model initialization failed".into()))?
    }
}

#[cfg(test)]
#[path = "local_model_cache_tests.rs"]
mod tests;
