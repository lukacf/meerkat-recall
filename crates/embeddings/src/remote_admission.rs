//! Process-local remote transport backpressure shared by generation and embeddings.
//!
//! This gate is a resource throttle, never provider approval or a spending ledger.
//! Independent Recall processes and other applications need their own admission.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use sha2::{Digest, Sha256};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{EmbeddingError, EmbeddingResult};

const MAX_IN_FLIGHT: usize = 2;
type EndpointGates = Mutex<HashMap<[u8; 32], Weak<Semaphore>>>;
static GATES: OnceLock<EndpointGates> = OnceLock::new();

fn invalid() -> EmbeddingError {
    EmbeddingError::ProviderError("Remote request admission unavailable".into())
}

fn endpoint_gate(endpoint: &str) -> EmbeddingResult<Arc<Semaphore>> {
    let mut endpoint = reqwest::Url::parse(endpoint).map_err(|_| invalid())?;
    if !matches!(endpoint.scheme(), "http" | "https")
        || endpoint.host_str().is_none()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.query().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(invalid());
    }
    let path = endpoint.path().trim_end_matches('/').to_owned();
    endpoint.set_path(&path);
    let key: [u8; 32] = Sha256::digest(endpoint.as_str().as_bytes()).into();
    let mut gates = GATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .map_err(|_| invalid())?;
    gates.retain(|_, gate| gate.strong_count() > 0);
    if let Some(gate) = gates.get(&key).and_then(Weak::upgrade) {
        return Ok(gate);
    }
    let gate = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
    gates.insert(key, Arc::downgrade(&gate));
    drop(gates);
    Ok(gate)
}

/// Wait for one endpoint's shared remote-operation slot in this process.
///
/// Hold through the complete response/stream, including decoding, and release
/// by dropping on completion, cancellation or error. Recheck current provider
/// authority after this wait. Waiting does not create a physical-call receipt or
/// change the caller's original deadline. Account aliases and models sharing the
/// endpoint share capacity; different processes are deliberately not coordinated.
///
/// # Errors
/// Returns a bounded error if the endpoint or admission state is invalid.
pub async fn acquire(endpoint: &str) -> EmbeddingResult<OwnedSemaphorePermit> {
    endpoint_gate(endpoint)?
        .acquire_owned()
        .await
        .map_err(|_| invalid())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    #[test]
    fn endpoint_aliases_share_capacity_and_unrelated_endpoints_do_not() {
        let first = endpoint_gate("http://NORMALIZED.invalid:80/v1/").unwrap();
        let same = endpoint_gate("http://normalized.invalid/v1").unwrap();
        let other = endpoint_gate("http://unrelated.invalid/v1").unwrap();
        assert!(Arc::ptr_eq(&first, &same));
        assert!(!Arc::ptr_eq(&first, &other));
        for invalid in [
            "file:///tmp/local",
            "https://secret@host.invalid/v1",
            "https://host.invalid/v1?secret=1",
        ] {
            assert!(endpoint_gate(invalid).is_err());
        }
    }

    #[tokio::test]
    async fn queued_admission_and_cancellation_preserve_exact_capacity() {
        let endpoint = "http://cancel-control.invalid/v1";
        let first = acquire(endpoint).await.unwrap();
        let second = acquire(endpoint).await.unwrap();
        let mut pending = Box::pin(acquire(endpoint));
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(5), &mut pending)
                .await
                .is_err()
        );
        drop(pending);
        drop(first);
        let replacement =
            tokio::time::timeout(std::time::Duration::from_secs(1), acquire(endpoint))
                .await
                .unwrap()
                .unwrap();
        let gate = endpoint_gate(endpoint).unwrap();
        assert_eq!(gate.available_permits(), 0);
        drop(second);
        drop(replacement);
        assert_eq!(gate.available_permits(), MAX_IN_FLIGHT);
    }
}
